package resilience

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// DependencyConfig 单个依赖实例的容错策略。
type DependencyConfig struct {
	Name string
	// Breaker 熔断策略（零值字段用 DefaultBreakerConfig 补齐）。
	Breaker BreakerConfig
	// Retry 重试策略（零值字段用 DefaultRetryConfig 补齐；MaxAttempts 显式给 1 表示不重试）。
	Retry RetryConfig
	// PerAttemptTimeout 单次尝试超时；0 表示只受外层 ctx 总预算约束。
	PerAttemptTimeout time.Duration
	// Fallback 可选降级函数；主流程失败时调用。
	Fallback FallbackFunc
}

// Client 容错调用层入口；按依赖实例名隔离熔断器，一路持续失败不影响其他实例。
type Client struct {
	mu       sync.Mutex
	breakers map[string]*Breaker
	configs  map[string]DependencyConfig
	logf     func(format string, args ...any)
}

// NewClient 创建空的调用层客户端。
func NewClient() *Client {
	return &Client{
		breakers: map[string]*Breaker{},
		configs:  map[string]DependencyConfig{},
		logf:     log.Printf,
	}
}

// SetLogger 替换全局判定日志出口（测试中传 t.Logf），并同步到已注册熔断器。
func (c *Client) SetLogger(f func(format string, args ...any)) {
	c.mu.Lock()
	if f != nil {
		c.logf = f
		for _, b := range c.breakers {
			b.SetLogger(f)
		}
	}
	c.mu.Unlock()
}

// Register 注册（或替换）一个依赖实例的配置与对应熔断器。
func (c *Client) Register(cfg DependencyConfig) *Breaker {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cfg.Name == "" {
		cfg.Name = "default"
	}
	cfg.Breaker = cfg.Breaker.withDefaults()
	cfg.Retry = withDefaultRetry(cfg.Retry)
	b := NewBreaker(cfg.Name, cfg.Breaker)
	b.SetLogger(c.logf)
	c.breakers[cfg.Name] = b
	c.configs[cfg.Name] = cfg
	return b
}

func withDefaultRetry(r RetryConfig) RetryConfig {
	d := DefaultRetryConfig()
	if r.MaxAttempts == 0 {
		r.MaxAttempts = d.MaxAttempts
	}
	if r.InitialBackoff == 0 {
		r.InitialBackoff = d.InitialBackoff
	}
	if r.MaxBackoff == 0 {
		r.MaxBackoff = d.MaxBackoff
	}
	if r.Multiplier == 0 {
		r.Multiplier = d.Multiplier
	}
	if r.JitterFactor == 0 {
		r.JitterFactor = d.JitterFactor
	}
	return r
}

// Breaker 返回某依赖实例的熔断器（不存在返回 nil）。
func (c *Client) Breaker(name string) *Breaker {
	c.mu.Lock()
	b := c.breakers[name]
	c.mu.Unlock()
	return b
}

// CallResult 是一次完整调用（可能经历多次尝试/降级）的可解释结果。
type CallResult struct {
	Dependency string
	// Attempts 实际打到下游的尝试次数（熔断拒绝、纯退避等待都不计入）。
	Attempts int
	// FinalKind 最终判定：timeout / circuit_open / retry_exhausted / business_error / fallback。
	FinalKind FailureKind
	// Value 真实成功时为下游值；降级时为 *FallbackResult。
	Value any
	// Fallback 为 true 表示 Value 是降级结果而非真实成功，调用方可一眼识别。
	Fallback bool
	Err      error
	// Decisions 按时间顺序记录每一步判定依据，便于解释与测试断言。
	Decisions []string
}

// Call 按 熔断 -> 尝试 -> 退避重试 -> 降级 的路径执行一次调用，
// 全程遵守 ctx 携带的总时间预算；req.Dependency 决定使用哪套隔离策略。
func (c *Client) Call(ctx context.Context, t Transport, req *Request) *CallResult {
	if req == nil {
		req = &Request{}
	}
	name := req.Dependency
	if name == "" {
		name = "default"
	}

	c.mu.Lock()
	cfg, ok := c.configs[name]
	logf := c.logf
	if !ok {
		// 未注册依赖：用默认策略即时创建一个独立熔断器。
		cfg = DependencyConfig{
			Name:    name,
			Breaker: DefaultBreakerConfig(),
			Retry:   DefaultRetryConfig(),
		}
		b := NewBreaker(name, cfg.Breaker)
		b.SetLogger(logf)
		c.breakers[name] = b
		c.configs[name] = cfg
	}
	breaker := c.breakers[name]
	c.mu.Unlock()

	res := &CallResult{Dependency: name}
	add := func(f string, args ...any) {
		msg := sprintf(f, args...)
		res.Decisions = append(res.Decisions, msg)
		logf("[call dep=%s op=%s] %s", name, req.Operation, msg)
	}

	add("开始调用 retryable=%v maxAttempts=%d perAttemptTimeout=%s",
		req.Retryable, cfg.Retry.MaxAttempts, durationOrNone(cfg.PerAttemptTimeout))

	var lastErr error
	var lastKind = KindUnknown

	for attempt := 1; attempt <= cfg.Retry.MaxAttempts; attempt++ {
		// 每轮先看总预算：预算已耗尽就不能再发起新尝试。
		if err := ctx.Err(); err != nil {
			lastErr = err
			lastKind = KindTimeout
			add("尝试#%d 前发现总预算已耗尽（%v），停止重试", attempt, err)
			break
		}

		// 1) 熔断许可。
		perm, err := breaker.Allow(ctx)
		if err != nil {
			lastErr = err
			lastKind = KindCircuitOpen
			add("尝试#%d 被熔断器拒绝：%v", attempt, err)
			break
		}

		// 2) 单次尝试预算 = 外层剩余预算 ∩ PerAttemptTimeout。
		attemptCtx := ctx
		cancel := func() {}
		if cfg.PerAttemptTimeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, cfg.PerAttemptTimeout)
		}

		// 3) 真正打到下游（副作用只发生在这里）。
		resp := t.Do(attemptCtx, req)
		res.Attempts = attempt
		cancel()

		// 4) 解释本次尝试结果。
		if resp != nil && resp.Err == nil {
			perm.Complete(true)
			add("尝试#%d 成功", attempt)
			res.FinalKind = KindUnknown // 真实成功不使用失败分类
			res.Value = resp.Value
			res.Err = nil
			return res
		}

		// 失败：先判定失败性质，再上报熔断（同一尝试只 Complete 一次）。
		rawErr := ErrNoResult
		if resp != nil && resp.Err != nil {
			rawErr = resp.Err
		}
		attemptTimeout := errors.Is(rawErr, context.DeadlineExceeded) ||
			errors.Is(rawErr, context.Canceled)
		// 注意：父 ctx 取消也归为超时/取消类，同样不该再重试。
		parentDone := ctx.Err() != nil
		retryableDown := IsRetryable(rawErr)

		var attemptKind FailureKind
		switch {
		case attemptTimeout:
			attemptKind = KindTimeout
		case retryableDown:
			attemptKind = KindRetryExhausted // 过程性分类，真正耗尽时才定为最终结论
		default:
			attemptKind = KindBusiness
		}
		lastErr = rawErr
		lastKind = attemptKind
		// 熔断统计：超时与可重试错误都算一次失败；业务失败也算失败。
		perm.Complete(false)
		add("尝试#%d 失败：%v（超时=%v 可重试下游错误=%v 父预算结束=%v）",
			attempt, rawErr, attemptTimeout, retryableDown, parentDone)

		// 5) 是否允许再来一次：
		//    a. 操作本身必须可安全重试；
		//    b. 错误必须是可重试性质（超时/可重试下游错误），业务失败立即拒绝；
		//    c. 还有剩余尝试次数；
		//    d. 总时间预算还够睡完下一次退避。
		if !req.Retryable {
			// 不可安全重试的操作明确拒绝再次尝试。对调用方这是确定性结论：
			// 超时根因保留 timeout，其余（哪怕下游自称可重试）一律按业务失败。
			if !attemptTimeout {
				lastKind = KindBusiness
			}
			add("操作不可安全重试（非幂等），明确拒绝重试，结论=%s", lastKind)
			break
		}
		if !attemptTimeout && !retryableDown {
			add("错误为不可重试业务失败，明确拒绝重试，结论=business_error")
			lastKind = KindBusiness
			break
		}
		if parentDone {
			add("外层时间预算已结束，不再重试，结论=timeout")
			lastKind = KindTimeout
			break
		}
		if attempt == cfg.Retry.MaxAttempts {
			add("已达最大尝试次数 %d，重试耗尽", cfg.Retry.MaxAttempts)
			if attemptTimeout {
				lastKind = KindTimeout
			} else {
				lastKind = KindRetryExhausted
			}
			break
		}

		bo := nextBackoff(cfg.Retry, attempt)
		add("尝试#%d 后等待退避 %s（含抖动），再进行下一次尝试", attempt, bo)
		if werr := waitForBackoff(ctx, bo); werr != nil {
			add("剩余时间预算不足以完成退避（%v），放弃后续尝试，结论=timeout", werr)
			lastErr = werr
			lastKind = KindTimeout
			break
		}
	}

	// 6) 降级路径。
	res.FinalKind = lastKind
	res.Err = &CallError{Kind: lastKind, Err: lastErr}
	if cfg.Fallback != nil {
		add("主流程失败（%s），执行降级函数", lastKind)
		v, ferr := cfg.Fallback(ctx, req, res.Err)
		if ferr != nil {
			add("降级函数自身失败：%v", ferr)
			res.Err = &CallError{Kind: lastKind, Err: ferr}
			return res
		}
		fv, ok := v.(*FallbackResult)
		if !ok {
			// 统一包装，保证调用方一定能识别“这是降级值”。
			fv = &FallbackResult{Value: v, Cause: res.Err}
		} else if fv.Cause == nil {
			fv.Cause = res.Err
		}
		res.Value = fv
		res.Fallback = true
		res.FinalKind = KindFallback
		res.Err = nil
		add("已返回降级结果（cause=%v），与真实成功明确区分", fv.Cause)
		return res
	}

	add("最终结论=%s 根因=%v 实际下游尝试次数=%d", lastKind, lastErr, res.Attempts)
	return res
}

func sprintf(f string, args ...any) string {
	return fmt.Sprintf(f, args...)
}

func durationOrNone(d time.Duration) string {
	if d <= 0 {
		return "无(仅受外层预算约束)"
	}
	return d.String()
}
