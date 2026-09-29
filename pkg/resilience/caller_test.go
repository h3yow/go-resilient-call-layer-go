package resilience

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/go-resilient-call-layer-go/pkg/downstream"
)

func fastCfg(attempts int, budget time.Duration) Config {
	return Config{
		Breaker: BreakerConfig{
			FailureThreshold:         5,
			OpenDuration:             500 * time.Millisecond,
			HalfOpenMaxConcurrent:    2,
			HalfOpenSuccessThreshold: 2,
		},
		Retry: RetryConfig{
			MaxAttempts:    attempts,
			InitialBackoff: 5 * time.Millisecond,
			MaxBackoff:     50 * time.Millisecond,
			Multiplier:     2,
			JitterFraction: 0,
		},
		Budget: budget,
	}
}

func degraded(dep string, op Operation, cause error) (*downstream.Response, error) {
	return &downstream.Response{Instance: dep, Key: op.Key, Payload: "CACHED"}, nil
}

func TestCaller_SuccessFirstTry(t *testing.T) {
	c := NewCaller(fastCfg(3, time.Second), testLog(t))
	ins := downstream.NewInstance("svc-A")
	ins.Configure(downstream.Behavior{Latency: time.Millisecond})

	resp, r := c.Invoke(context.Background(), ins, Operation{Key: "get", Retryable: true},
		downstream.Request{Key: "get"}, nil)
	if r.Outcome != OutcomeSuccess || r.Degraded || r.Attempts != 1 {
		t.Fatalf("期望真实成功/非降级/1次尝试，实际 %+v", r)
	}
	if resp == nil || resp.Instance != "svc-A" {
		t.Fatalf("应返回真实下游应答，实际 %+v", resp)
	}
	if ins.CallCount() != 1 {
		t.Fatalf("副作用应为1，实际 %d", ins.CallCount())
	}
}

func TestCaller_RetryThenSuccess(t *testing.T) {
	c := NewCaller(fastCfg(4, time.Second), testLog(t))
	ins := downstream.NewInstance("svc-A")
	// 前2次瞬时失败，第3次成功。
	ins.Configure(downstream.Behavior{FailTransient: true, FailNTimes: 2})

	resp, r := c.Invoke(context.Background(), ins, Operation{Key: "get", Retryable: true},
		downstream.Request{Key: "get"}, nil)
	if r.Outcome != OutcomeSuccess {
		t.Fatalf("期望重试后成功，实际 %s: %s", r.Outcome, r.Reason)
	}
	if r.Attempts != 3 {
		t.Fatalf("期望3次尝试，实际 %d", r.Attempts)
	}
	if resp == nil || resp.Key != "get" {
		t.Fatalf("应答内容不匹配: %+v", resp)
	}
	if ins.CallCount() != 3 {
		t.Fatalf("副作用次数必须与尝试次数一致=3，实际 %d", ins.CallCount())
	}
}

func TestCaller_BusinessErrorNeverRetried(t *testing.T) {
	c := NewCaller(fastCfg(5, time.Second), testLog(t))
	ins := downstream.NewInstance("svc-A")
	ins.Configure(downstream.Behavior{BusinessError: "余额不足"})

	_, r := c.Invoke(context.Background(), ins, Operation{Key: "pay", Retryable: true},
		downstream.Request{}, nil)
	if r.Outcome != OutcomeBusinessError {
		t.Fatalf("业务错误必须归类 BUSINESS_ERROR，实际 %s", r.Outcome)
	}
	if r.Attempts != 1 || ins.CallCount() != 1 {
		t.Fatalf("业务错误不得重试：attempts=%d count=%d", r.Attempts, ins.CallCount())
	}
}

func TestCaller_NonRetryableOperationRejected(t *testing.T) {
	c := NewCaller(fastCfg(5, time.Second), testLog(t))
	ins := downstream.NewInstance("svc-A")
	ins.Configure(downstream.Behavior{FailTransient: true})

	_, r := c.Invoke(context.Background(), ins, Operation{Key: "createOrder", Retryable: false},
		downstream.Request{}, nil)
	if r.Outcome != OutcomeNotRetryable {
		t.Fatalf("不可重试操作首次失败应归类 NOT_RETRYABLE，实际 %s", r.Outcome)
	}
	if r.Attempts != 1 || ins.CallCount() != 1 {
		t.Fatalf("不得重试：attempts=%d count=%d", r.Attempts, ins.CallCount())
	}
}

func TestCaller_RetryExhausted(t *testing.T) {
	c := NewCaller(fastCfg(3, time.Second), testLog(t))
	ins := downstream.NewInstance("svc-A")
	ins.Configure(downstream.Behavior{FailTransient: true})

	_, r := c.Invoke(context.Background(), ins, Operation{Key: "get", Retryable: true},
		downstream.Request{}, nil)
	if r.Outcome != OutcomeRetryExhausted {
		t.Fatalf("持续瞬时失败应收敛为 RETRY_EXHAUSTED，实际 %s: %s", r.Outcome, r.Reason)
	}
	if r.Attempts != 3 {
		t.Fatalf("期望恰好3次尝试，实际 %d", r.Attempts)
	}
	if ins.CallCount() != 3 {
		t.Fatalf("副作用不得重复/遗漏，期望3实际 %d", ins.CallCount())
	}
}

func TestCaller_FallbackIsExplicitlyMarked(t *testing.T) {
	c := NewCaller(fastCfg(2, time.Second), testLog(t))
	ins := downstream.NewInstance("svc-A")
	ins.Configure(downstream.Behavior{FailTransient: true})

	var fallbackCalls int32
	fb := func(dep string, op Operation, cause error) (*downstream.Response, error) {
		atomic.AddInt32(&fallbackCalls, 1)
		return degraded(dep, op, cause)
	}
	resp, r := c.Invoke(context.Background(), ins, Operation{Key: "get", Retryable: true},
		downstream.Request{}, fb)
	if r.Outcome != OutcomeFallback || !r.Degraded {
		t.Fatalf("降级路径必须归类 FALLBACK 且 Degraded=true，实际 %+v", r)
	}
	if resp == nil || resp.Payload != "CACHED" {
		t.Fatalf("应返回降级内容，实际 %+v", resp)
	}
	if atomic.LoadInt32(&fallbackCalls) != 1 {
		t.Fatalf("降级函数对一次调用只能执行一次，实际 %d", fallbackCalls)
	}
}
