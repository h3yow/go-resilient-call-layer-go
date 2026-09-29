package resilience_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/h3yow/go-resilient-call-layer-go/resilience"
)

// ExampleClient_overview 演示同一次调用在不同路径下得到可区分的结论，
// 以及熔断按依赖隔离、降级结果可识别。运行：
//
//	go test ./resilience/ -run ExampleClient_overview -v
func ExampleClient_overview() {
	SetRandSeedForExample()
	c := resilience.NewClient()
	c.SetLogger(func(string, ...any) {}) // 示例输出保持干净；实际使用可接 log.Printf

	c.Register(resilience.DependencyConfig{
		Name: "user-service",
		Breaker: resilience.BreakerConfig{
			FailureThreshold:      3,
			OpenCooldown:          80 * time.Millisecond,
			HalfOpenMaxConcurrent: 1,
			HalfOpenSuccessNeeded: 1,
		},
		Retry: resilience.RetryConfig{
			MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: 4 * time.Millisecond,
			Multiplier: 2, JitterFactor: 0,
		},
		Fallback: func(ctx context.Context, req *resilience.Request, cause error) (any, error) {
			return &resilience.FallbackResult{Value: "兜底用户信息"}, nil
		},
	})

	// 路径 1：可重试错误，重试后成功。
	tr1 := resilience.NewLocalTransport("user-service").
		Script(resilience.Fail(true, "瞬时抖动"), resilience.OK("真实用户"))
	r1 := c.Call(context.Background(), tr1,
		&resilience.Request{Dependency: "user-service", Operation: "GetUser", Retryable: true})
	fmt.Printf("路径1: 尝试=%d 值=%v err=%v\n", r1.Attempts, r1.Value, r1.Err)

	// 路径 2：不可重试的业务失败 -> 不重试，走降级。
	tr2 := resilience.NewLocalTransport("user-service").
		Script(resilience.Fail(false, "用户不存在"))
	r2 := c.Call(context.Background(), tr2,
		&resilience.Request{Dependency: "user-service", Operation: "GetUser", Retryable: false})
	fv := r2.Value.(*resilience.FallbackResult)
	var ce *resilience.CallError
	errors.As(fv.Cause, &ce)
	fmt.Printf("路径2: fallback=%v 降级值=%v 触发原因=%s\n", r2.Fallback, fv.Value, ce.Kind)

	// 路径 3：总预算不足导致超时（退避睡不完），结论 timeout。
	tr3 := resilience.NewLocalTransport("user-service").
		Script(resilience.Fail(true, "慢"))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
	defer cancel()
	r3 := c.Call(ctx, tr3,
		&resilience.Request{Dependency: "user-service", Operation: "GetUser", Retryable: true})
	// 即使走了降级，FallbackResult.Cause 仍保留真实根因 timeout。
	r3fv := r3.Value.(*resilience.FallbackResult)
	var r3ce *resilience.CallError
	errors.As(r3fv.Cause, &r3ce)
	fmt.Printf("路径3: fallback=%v 根因=%s\n", r3.Fallback, r3ce.Kind)

	// 路径 4：熔断打开期间拒绝调用（不产生下游副作用）。
	tr4 := resilience.NewLocalTransport("user-service").
		Script(resilience.Fail(false, "挂"), resilience.Fail(false, "挂"),
			resilience.Fail(false, "挂"))
	_ = c.Call(context.Background(), tr4,
		&resilience.Request{Dependency: "user-service", Operation: "GetUser", Retryable: false})
	if c.Breaker("user-service").State() == resilience.StateOpen {
		r4 := c.Call(context.Background(), tr4,
			&resilience.Request{Dependency: "user-service", Operation: "GetUser", Retryable: true})
		fmt.Printf("路径4: kind=%s 实际下游尝试=%d fallback=%v\n",
			r4.FinalKind, r4.Attempts, r4.Fallback)
	}

	// Output:
	// 路径1: 尝试=2 值=真实用户 err=<nil>
	// 路径2: fallback=true 降级值=兜底用户信息 触发原因=business_error
	// 路径3: fallback=true 根因=timeout
	// 路径4: kind=fallback 实际下游尝试=0 fallback=true
}

// SetRandSeedForExample 让示例中的抖动确定化（演示用）。
func SetRandSeedForExample() { resilience.SetRandSeed(42) }
