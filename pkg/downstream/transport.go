// Package downstream 提供可替换的本地传输实现，用于模拟下游服务实例：
// 可配置延迟、超时、瞬时错误与业务错误，并精确记录副作用次数，
// 供容错层在不依赖真实网络的情况下验证超时/重试/熔断/降级行为。
package downstream

import (
	"context"
	"sync"
	"time"

	"github.com/h3yow/go-resilient-call-layer-go/pkg/relerr"
)

// Request 是发往下游的请求。
type Request struct {
	// Key 请求标识；幂等/只读操作应在调用方的 Operation 中声明。
	Key string
	// Payload 仅用于日志展示。
	Payload string
}

// Response 是下游的成功/业务应答。
type Response struct {
	Instance string
	Key      string
	Payload  string
}

// Behavior 描述实例在下一次（或持续）调用中的行为。
type Behavior struct {
	// Latency 每次调用的处理延迟。
	Latency time.Duration
	// Timeout 若 >0 且 Latency 超过它，则处理到该时长即返回超时错误。
	Timeout time.Duration
	// FailTransient 若为 true，返回瞬时错误（可重试类别）。
	FailTransient bool
	// BusinessError 若非空，返回业务错误（不可重试类别）。
	BusinessError string
	// FailNTimes 仅前 N 次按失败规则响应，之后恢复成功；0 表示失败规则持续生效。
	FailNTimes int
}

func (b Behavior) failing() bool { return b.FailTransient || b.BusinessError != "" }

// Instance 是一个可配置行为的模拟下游实例。
type Instance struct {
	name string

	mu                sync.Mutex
	behavior          Behavior
	callCount         int
	remainingFailures int // -1 表示失败规则持续生效；>=0 为剩余失败次数
}

// NewInstance 创建一个模拟下游实例（name 作为依赖隔离维度）。
func NewInstance(name string) *Instance {
	return &Instance{name: name, remainingFailures: -1}
}

// Name 返回实例名。
func (i *Instance) Name() string { return i.name }

// Configure 替换该实例的行为配置。
func (i *Instance) Configure(b Behavior) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.behavior = b
	if b.FailNTimes > 0 {
		i.remainingFailures = b.FailNTimes
	} else {
		i.remainingFailures = -1
	}
}

// Do 执行一次本地“传输”。
//
// 副作用计数只在调用真正开始处理时累加一次：若进入时 ctx 已结束
// （预算已耗尽），则不会触碰计数，直接返回超时类错误。
// 处理期间 ctx 先到期同样返回超时类错误，且不会产生“成功”副作用。
func (i *Instance) Do(ctx context.Context, req Request) (*Response, error) {
	// 调用前预算已耗尽：不开始处理，不计数，避免对已超时调用硬拖。
	if err := ctx.Err(); err != nil {
		return nil, relerr.NewTimeoutError("downstream[%s] skipped: budget already exhausted: %v", i.name, err)
	}

	i.mu.Lock()
	cfg := i.behavior
	shouldFail := cfg.failing()
	if shouldFail {
		switch {
		case i.remainingFailures > 0:
			i.remainingFailures--
		case i.remainingFailures == 0:
			// 配额耗尽，本次起恢复成功。
			shouldFail = false
		default:
			// -1：失败规则持续生效。
		}
	}
	// 真正开始处理：副作用恰好累加一次。
	i.callCount++
	count := i.callCount
	i.mu.Unlock()

	// 计算本次处理时长与“下游自身处理超时”。
	wait := cfg.Latency
	selfTimeout := cfg.Timeout > 0 && cfg.Latency > cfg.Timeout
	if selfTimeout && cfg.Timeout < wait {
		wait = cfg.Timeout
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		// 调用方时间预算先到期：归类为超时，绝不继续拖着跑。
		return nil, relerr.NewTimeoutError("downstream[%s] attempt#%d canceled after budget: %v",
			i.name, count, ctx.Err())
	case <-timer.C:
	}

	if selfTimeout {
		return nil, relerr.NewTimeoutError("downstream[%s] attempt#%d processing timeout after %s",
			i.name, count, cfg.Timeout)
	}
	if shouldFail {
		if cfg.BusinessError != "" {
			return nil, relerr.NewBusinessError("downstream[%s] attempt#%d business error: %s",
				i.name, count, cfg.BusinessError)
		}
		return nil, relerr.NewTransientError("downstream[%s] attempt#%d transient failure",
			i.name, count)
	}
	return &Response{Instance: i.name, Key: req.Key, Payload: req.Payload}, nil
}

// CallCount 返回该实例真实开始处理的调用次数（副作用次数）。
func (i *Instance) CallCount() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.callCount
}

// ResetCounters 清零调用计数（不改变行为配置）。
func (i *Instance) ResetCounters() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.callCount = 0
}
