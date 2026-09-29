// demo 用可替换的本地传输实例演示容错调用层在不同路径下的一致结论：
// 重试后成功、业务错误不重试、不可重试操作拒绝、预算超时、熔断隔离与降级。
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/h3yow/go-resilient-call-layer-go/pkg/downstream"
	"github.com/h3yow/go-resilient-call-layer-go/pkg/resilience"
)

func main() {
	log := func(format string, args ...any) {
		fmt.Printf("  [日志] "+format+"\n", args...)
	}
	caller := resilience.NewCaller(resilience.Config{
		Breaker: resilience.BreakerConfig{
			FailureThreshold:         3,
			OpenDuration:             200 * time.Millisecond,
			HalfOpenMaxConcurrent:    2,
			HalfOpenSuccessThreshold: 2,
		},
		Retry: resilience.RetryConfig{
			MaxAttempts:    3,
			InitialBackoff: 20 * time.Millisecond,
			MaxBackoff:     200 * time.Millisecond,
			Multiplier:     2,
			JitterFraction: 0.3,
		},
		Budget: 300 * time.Millisecond,
	}, log)

	cache := func(dep string, op resilience.Operation, cause error) (*downstream.Response, error) {
		return &downstream.Response{Instance: dep, Key: op.Key, Payload: "缓存兜底数据"}, nil
	}

	scenario := func(title string, dep *downstream.Instance, op resilience.Operation,
		fb resilience.FallbackFunc) {
		fmt.Printf("\n=== %s ===\n", title)
		resp, r := caller.Invoke(context.Background(), dep, op,
			downstream.Request{Key: op.Key}, fb)
		marker := "真实结果"
		payload := ""
		if r.Degraded {
			marker = "降级结果"
		}
		if resp != nil {
			payload = resp.Payload
		}
		fmt.Printf("  [结论] outcome=%s attempts=%d degraded=%v (%s) payload=%q\n",
			r.Outcome, r.Attempts, r.Degraded, marker, payload)
		fmt.Printf("  [判定链] %s\n", r.Reason)
	}

	// 1) 前两次瞬时失败、第三次成功 -> SUCCESS（重试 + 退避抖动生效）。
	recoverable := downstream.NewInstance("profile-svc")
	recoverable.Configure(downstream.Behavior{FailTransient: true, FailNTimes: 2})
	scenario("瞬时失败后重试恢复", recoverable,
		resilience.Operation{Key: "GetProfile", Retryable: true}, nil)

	// 2) 业务错误：永远不重试，即使声明了可重试。
	bizErr := downstream.NewInstance("payment-svc")
	bizErr.Configure(downstream.Behavior{BusinessError: "余额不足"})
	scenario("业务错误（不可重试）", bizErr,
		resilience.Operation{Key: "Pay", Retryable: true}, nil)

	// 3) 不可安全重试的写操作：首次瞬时失败即终局 NOT_RETRYABLE。
	writeOnly := downstream.NewInstance("order-svc")
	writeOnly.Configure(downstream.Behavior{FailTransient: true})
	scenario("不可重试写操作", writeOnly,
		resilience.Operation{Key: "CreateOrder", Retryable: false}, nil)

	// 4) 下游慢于预算 -> TIMEOUT，并走降级返回缓存。
	slow := downstream.NewInstance("search-svc")
	slow.Configure(downstream.Behavior{Latency: 2 * time.Second})
	scenario("下游超时 -> 降级", slow,
		resilience.Operation{Key: "Search", Retryable: true}, cache)

	// 5) 依赖隔离：inventory 持续失败直至熔断；catalog 同进程仍真实成功。
	inventory := downstream.NewInstance("inventory-svc")
	inventory.Configure(downstream.Behavior{FailTransient: true})
	catalog := downstream.NewInstance("catalog-svc")
	catalog.Configure(downstream.Behavior{})
	for i := 0; i < 6; i++ {
		caller.Invoke(context.Background(), inventory,
			resilience.Operation{Key: "Stock", Retryable: true},
			downstream.Request{}, cache)
	}
	scenario("熔断打开后直接拒绝 -> 降级", inventory,
		resilience.Operation{Key: "Stock", Retryable: true}, cache)
	scenario("健康实例不受其他实例熔断影响", catalog,
		resilience.Operation{Key: "Catalog", Retryable: true}, nil)
}
