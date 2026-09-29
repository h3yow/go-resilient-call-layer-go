package resilience

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/h3yow/go-resilient-call-layer-go/pkg/downstream"
	"github.com/h3yow/go-resilient-call-layer-go/pkg/relerr"
)

// Operation 描述一次调用的安全属性。只有可安全重试的操作才允许重试。
type Operation struct {
	// Key 业务操作标识，用于日志。
	Key string
	// Retryable 为 false 时，失败一律不重试（首次失败即终局）。
	Retryable bool
}

// CallResult 让调用方一眼区分真实成功与降级，并解释失败归类。
type CallResult struct {
	// Outcome 最终归类。
	Outcome Outcome
	// Attempts 真实发起下游调用的尝试次数（熔断拒绝不计数）。
	Attempts int
	// Degraded 为 true 表示 Response 来自 Fallback 而非下游真实应答。
	Degraded bool
	// Dependency 本次调用命中的下游实例名。
	Dependency string
	// Reason 人类可读的判定依据（含状态迁移、预算判定、每轮重试决策）。
	Reason string
}

func (r *CallResult) explainf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	if r.Reason == "" {
		r.Reason = line
	} else {
		r.Reason += " | " + line
	}
}

// Config 是 Caller 的配置。
type Config struct {
	Breaker BreakerConfig
	Retry   RetryConfig
	// Budget 单次调用的总时间预算；<=0 表示仅受 ctx 截止时间限制。
	Budget time.Duration
}

// DefaultConfig 返回默认配置。
func DefaultConfig() Config {
	return Config{
		Breaker: DefaultBreakerConfig(),
		Retry:   DefaultRetryConfig(),
		Budget:  500 * time.Millisecond,
	}
}

func (c *Config) normalize() {
	c.Breaker.normalize()
	c.Retry.normalize()
}

// FallbackFunc 在真实调用失败时产出降级应答。
// cause 为导致降级的原始错误（熔断打开时为 ErrCircuitOpen）。
type FallbackFunc func(dep string, op Operation, cause error) (*downstream.Response, error)

// Caller 按依赖维度编排熔断、重试、时间预算与降级。
type Caller struct {
	cfg    Config
	policy RetryPolicy
	log    LogFunc
	now    func() time.Time

	breakersMu sync.Mutex
	breakers   map[string]*breaker
}

// NewCaller 创建容错调用编排器。
func NewCaller(cfg Config, log LogFunc) *Caller {
	return newCallerWithClock(cfg, log, time.Now)
}

func newCallerWithClock(cfg Config, log LogFunc, nowFn func() time.Time) *Caller {
	cfg.normalize()
	if log == nil {
		log = func(string, ...any) {}
	}
	if nowFn == nil {
		nowFn = time.Now
	}
	return &Caller{
		cfg:      cfg,
		policy:   NewRetryPolicy(cfg.Retry),
		log:      log,
		now:      nowFn,
		breakers: make(map[string]*breaker),
	}
}

// BreakerFor 返回（或惰性创建）某个依赖实例独立的熔断器。
// 不同实例持有不同 Breaker，一路持续失败不会拖垮其他实例。
func (c *Caller) BreakerFor(dep *downstream.Instance) Breaker {
	name := dep.Name()
	c.breakersMu.Lock()
	defer c.breakersMu.Unlock()
	b, ok := c.breakers[name]
	if !ok {
		b = newBreakerWithClock(name, c.cfg.Breaker, c.log, c.now)
		c.breakers[name] = b
	}
	return b
}

// Invoke 针对指定下游实例执行一次带完整容错保护的调用。
func (c *Caller) Invoke(ctx context.Context, dep *downstream.Instance, op Operation,
	req downstream.Request, fb FallbackFunc) (*downstream.Response, *CallResult) {

	result := &CallResult{Dependency: dep.Name()}
	start := c.now()

	// 1) 确定本次调用的时间预算：ctx 与 Config.Budget 取更早的截止时间。
	callCtx := ctx
	var cancel context.CancelFunc
	if c.cfg.Budget > 0 {
		callCtx, cancel = context.WithDeadline(ctx, start.Add(c.cfg.Budget))
		defer cancel()
	}
	budgetDesc := "仅受 ctx 限制"
	if dl, ok := callCtx.Deadline(); ok {
		budgetDesc = "截止剩余 " + dl.Sub(start).String()
	}
	c.log("call[%s] op=%s retryable=%v 开始调用: 预算=%s, 最大尝试=%d",
		dep.Name(), op.Key, op.Retryable, budgetDesc, c.cfg.Retry.MaxAttempts)
	result.explainf("开始调用(op=%s, retryable=%v, %s)", op.Key, op.Retryable, budgetDesc)

	br := c.BreakerFor(dep).(*breaker)

	var (
		terminalOutcome Outcome
		cause           error
	)

	for attempt := 1; attempt <= c.cfg.Retry.MaxAttempts; attempt++ {
		now := c.now()

		// 2) 先看剩余时间预算：预算已耗尽就立即终止，绝不把超时调用继续拖着跑。
		if err := callCtx.Err(); err != nil {
			terminalOutcome = OutcomeTimeout
			cause = relerr.NewTimeoutError("调用[%s]剩余时间预算已耗尽，第 %d 次尝试未发起: %v",
				dep.Name(), attempt, err)
			result.explainf("第%d次尝试前预算已耗尽，判定 TIMEOUT，不发起调用", attempt)
			c.log("call[%s] 第%d次尝试前检测到预算耗尽(%v)，终止重试", dep.Name(), attempt, err)
			break
		}

		// 3) 熔断放行判定（按依赖隔离；半开期间并发放行有上限）。
		decision := br.Allow(now)
		if !decision.Allowed {
			terminalOutcome = OutcomeCircuitOpen
			cause = fmt.Errorf("%w: %s", relerr.ErrCircuitOpen, decision.Reason)
			result.explainf("第%d次尝试被熔断拒绝: %s", attempt, decision.Reason)
			c.log("call[%s] 第%d次尝试被熔断拒绝: %s", dep.Name(), attempt, decision.Reason)
			break
		}

		// 4) 真实发起一次下游调用（副作用在传输层恰好计数一次）。
		result.Attempts++
		c.log("call[%s] 第%d/%d次尝试已放行(gen=%d): %s",
			dep.Name(), attempt, c.cfg.Retry.MaxAttempts, decision.Generation, decision.Reason)
		resp, err := dep.Do(callCtx, req)
		finishAt := c.now()

		if err == nil {
			// 成功：仅向当前代际上报一次。
			br.OnSuccess(finishAt, decision.Generation)
			result.Outcome = OutcomeSuccess
			result.explainf("第%d次尝试真实成功", attempt)
			c.log("call[%s] 第%d次尝试成功，上报熔断成功(gen=%d)", dep.Name(), attempt, decision.Generation)
			return resp, result
		}

		// 5) 失败：仅向当前代际上报一次（熔断拒绝的尝试不会走到这里，杜绝重复累加）。
		br.OnFailure(finishAt, decision.Generation)
		kind := relerr.KindOf(err)
		c.log("call[%s] 第%d次尝试失败: kind=%s err=%v，已上报熔断失败(gen=%d)",
			dep.Name(), attempt, kind, err, decision.Generation)

		// 6) 终局归类与“是否还能重试”的判定。
		budgetExhausted := callCtx.Err() != nil

		switch {
		case kind == relerr.KindBusiness:
			// 业务错误：语义明确失败，无论操作是否可重试都不重试。
			terminalOutcome = OutcomeBusinessError
			cause = err
			result.explainf("第%d次尝试返回业务错误(不可重试)，判定 BUSINESS_ERROR", attempt)
			c.log("call[%s] 业务错误，明确拒绝重试", dep.Name())
		case budgetExhausted && (kind == relerr.KindTimeout || relerr.IsTimeout(err)):
			terminalOutcome = OutcomeTimeout
			cause = err
			result.explainf("第%d次尝试超时且时间预算已耗尽，判定 TIMEOUT", attempt)
			c.log("call[%s] 时间预算耗尽，判定 TIMEOUT，不再重试", dep.Name())
		case !op.Retryable:
			// 操作声明不可安全重试：首次失败即终局，明确拒绝重试。
			terminalOutcome = OutcomeNotRetryable
			cause = err
			result.explainf("第%d次尝试失败(kind=%s)但操作不可重试，判定 NOT_RETRYABLE", attempt, kind)
			c.log("call[%s] 操作不可安全重试，拒绝重试", dep.Name())
		case attempt >= c.cfg.Retry.MaxAttempts:
			terminalOutcome = OutcomeRetryExhausted
			cause = err
			result.explainf("第%d次尝试失败(kind=%s)，重试次数已耗尽，判定 RETRY_EXHAUSTED",
				attempt, kind)
			c.log("call[%s] 重试次数耗尽(%d次)，判定 RETRY_EXHAUSTED", dep.Name(), attempt)
		default:
			// 7) 可重试：计算带抖动的退避，并用剩余预算约束。
			backoff := c.policy.NextBackoff(attempt)
			var remaining time.Duration
			if dl, ok := callCtx.Deadline(); ok {
				remaining = dl.Sub(c.now())
			} else {
				remaining = 1<<63 - 1 // ctx 无截止时间：预算无限
			}
			if remaining <= backoff {
				terminalOutcome = OutcomeTimeout
				cause = relerr.NewTimeoutError(
					"调用[%s]剩余预算 %s 不足以覆盖退避 %s，放弃第 %d 次重试",
					dep.Name(), remaining, backoff, attempt+1)
				result.explainf(
					"第%d次失败后剩余预算 %s <= 退避 %s，不再硬拖，判定 TIMEOUT",
					attempt, remaining.Round(time.Microsecond), backoff)
				c.log("call[%s] 剩余预算 %s 不足以覆盖退避 %s，放弃重试",
					dep.Name(), remaining, backoff)
				break
			}
			c.log("call[%s] 第%d次失败后进入退避 %s（剩余预算 %s），准备第 %d 次重试",
				dep.Name(), attempt, backoff, remaining.Round(time.Microsecond), attempt+1)
			result.explainf("第%d次失败(kind=%s)，退避 %s 后重试", attempt, kind, backoff)

			timer := time.NewTimer(backoff)
			select {
			case <-callCtx.Done():
				timer.Stop()
				terminalOutcome = OutcomeTimeout
				cause = relerr.NewTimeoutError("调用[%s]退避等待期间预算耗尽: %v",
					dep.Name(), callCtx.Err())
				result.explainf("退避等待期间预算耗尽，判定 TIMEOUT")
			case <-timer.C:
				continue
			}
		}
		if terminalOutcome != OutcomeUnknown {
			break
		}
	}

	// 8) 降级：真实路径终局失败时，若提供降级函数则返回降级结果。
	if fb != nil {
		fbResp, fbErr := fb(dep.Name(), op, cause)
		if fbErr == nil && fbResp != nil {
			result.Outcome = OutcomeFallback
			result.Degraded = true
			result.explainf("真实调用终局=%s，已使用降级结果(FALLBACK)，非真实成功", terminalOutcome)
			c.log("call[%s] 终局=%s，返回降级结果（明确标记 DEGRADED）", dep.Name(), terminalOutcome)
			return fbResp, result
		}
		if fbErr != nil {
			result.explainf("降级函数也失败: %v，沿用终局 %s", fbErr, terminalOutcome)
			c.log("call[%s] 降级函数失败: %v", dep.Name(), fbErr)
		}
	}

	result.Outcome = terminalOutcome
	result.explainf("终局判定 %s（真实尝试 %d 次）", terminalOutcome, result.Attempts)
	c.log("call[%s] 结束: outcome=%s attempts=%d", dep.Name(), terminalOutcome, result.Attempts)
	return nil, result
}
