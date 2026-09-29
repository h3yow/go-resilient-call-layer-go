package resilience

import (
	"context"
	"testing"
	"time"

	"github.com/h3yow/go-resilient-call-layer-go/pkg/downstream"
)

func breakerTripCfg() Config {
	return Config{
		Breaker: BreakerConfig{
			FailureThreshold:         3,
			OpenDuration:             100 * time.Millisecond,
			HalfOpenMaxConcurrent:    2,
			HalfOpenSuccessThreshold: 2,
		},
		Retry: RetryConfig{
			MaxAttempts:    1, // 不重试，让每次调用恰好对应1次熔断失败
			InitialBackoff: time.Millisecond,
		},
		Budget: time.Second,
	}
}

func TestCaller_DependencyIsolation(t *testing.T) {
	c := NewCaller(breakerTripCfg(), testLog(t))
	bad := downstream.NewInstance("svc-BAD")
	bad.Configure(downstream.Behavior{FailTransient: true})
	good := downstream.NewInstance("svc-GOOD")
	good.Configure(downstream.Behavior{})

	// 打坏 svc-BAD 直到其熔断打开。
	var sawOpen bool
	for i := 0; i < 10; i++ {
		_, r := c.Invoke(context.Background(), bad, Operation{Key: "k", Retryable: true},
			downstream.Request{}, nil)
		if r.Outcome == OutcomeCircuitOpen {
			sawOpen = true
			break
		}
	}
	if !sawOpen {
		t.Fatalf("svc-BAD 应在持续失败后熔断打开并返回 CIRCUIT_OPEN")
	}

	// svc-GOOD 必须完全不受影响：真实成功，使用的是独立熔断器。
	resp, r := c.Invoke(context.Background(), good, Operation{Key: "k", Retryable: true},
		downstream.Request{Key: "k"}, nil)
	if r.Outcome != OutcomeSuccess || resp == nil {
		t.Fatalf("一路持续失败不能拖垮其他实例，svc-GOOD 应真实成功，实际 %s", r.Outcome)
	}
	snapBad := c.BreakerFor(bad).Snapshot(time.Now())
	snapGood := c.BreakerFor(good).Snapshot(time.Now())
	if snapBad.State != StateOpen {
		t.Fatalf("svc-BAD 应处于 OPEN，实际 %s", snapBad.State)
	}
	if snapGood.State != StateClosed {
		t.Fatalf("svc-GOOD 应保持 CLOSED，实际 %s", snapGood.State)
	}
}

func TestCaller_CircuitOpenThenHalfOpenRecover(t *testing.T) {
	c := NewCaller(breakerTripCfg(), testLog(t))
	ins := downstream.NewInstance("svc-Rec")
	// 先持续失败触发打开。
	ins.Configure(downstream.Behavior{FailTransient: true})
	for i := 0; i < 5; i++ {
		c.Invoke(context.Background(), ins, Operation{Key: "k", Retryable: true},
			downstream.Request{}, nil)
	}
	if snap := c.BreakerFor(ins).Snapshot(time.Now()); snap.State != StateOpen {
		t.Fatalf("前置: 应 OPEN，实际 %s", snap.State)
	}

	// OPEN 期间调用：不触碰下游，归类 CIRCUIT_OPEN，Attempts=0。
	_, r := c.Invoke(context.Background(), ins, Operation{Key: "k", Retryable: true},
		downstream.Request{}, nil)
	if r.Outcome != OutcomeCircuitOpen || r.Attempts != 0 {
		t.Fatalf("OPEN 期间应直接拒绝 CIRCUIT_OPEN 且 0 次尝试，实际 %+v", r)
	}
	beforeCount := ins.CallCount()

	// 等待冷却结束，下游恢复成功：半开探测连续成功后回到 CLOSED。
	time.Sleep(130 * time.Millisecond)
	ins.Configure(downstream.Behavior{})

	// 半开恢复阈值=2：前两次探测成功即恢复。
	var recovered bool
	for i := 0; i < 3; i++ {
		_, rr := c.Invoke(context.Background(), ins, Operation{Key: "k", Retryable: true},
			downstream.Request{}, nil)
		if rr.Outcome == OutcomeSuccess &&
			c.BreakerFor(ins).Snapshot(time.Now()).State == StateClosed {
			recovered = true
			break
		}
	}
	if !recovered {
		t.Fatalf("冷却后探测成功应恢复 CLOSED")
	}
	if ins.CallCount() == beforeCount {
		t.Fatalf("半开期间应真实发起过探测调用")
	}
}

func TestCaller_HighConcurrencyCircuitCountsConsistent(t *testing.T) {
	c := NewCaller(breakerTripCfg(), testLog(t))
	ins := downstream.NewInstance("svc-Flaky")
	ins.Configure(downstream.Behavior{Latency: time.Millisecond, FailTransient: true})

	const n = 100
	done := make(chan *CallResult, n)
	for i := 0; i < n; i++ {
		go func() {
			_, r := c.Invoke(context.Background(), ins, Operation{Key: "k", Retryable: true},
				downstream.Request{}, nil)
			done <- r
		}()
	}

	var openRejected, transientFailed int
	for i := 0; i < n; i++ {
		r := <-done
		switch r.Outcome {
		case OutcomeCircuitOpen:
			openRejected++
			if r.Attempts != 0 {
				t.Fatalf("熔断拒绝的调用 Attempts 必须为0，实际 %d", r.Attempts)
			}
		case OutcomeRetryExhausted, OutcomeBusinessError, OutcomeNotRetryable:
			transientFailed++
		default:
			t.Fatalf("持续失败场景不应出现 %s", r.Outcome)
		}
	}
	// 真实打到下游的次数（副作用）必须与“尝试过”的调用结果数一致：
	// 熔断打开后拒绝的调用不计入。
	allowedCalls := transientFailed
	if ins.CallCount() != allowedCalls {
		t.Fatalf("副作用计数 %d 必须等于真实放行调用数 %d（拒绝不计数、不重复累加）",
			ins.CallCount(), allowedCalls)
	}
	if openRejected == 0 {
		t.Fatalf("100 并发持续失败，必然有大量调用在熔断打开后被拒绝，实际0")
	}
	snap := c.BreakerFor(ins).Snapshot(time.Now())
	if snap.State != StateOpen {
		t.Fatalf("最终应停留在 OPEN，实际 %s", snap.State)
	}
}

func TestCaller_MixedRetryableAndNonRetryableOperations(t *testing.T) {
	c := NewCaller(fastCfg(3, time.Second), testLog(t))
	ins := downstream.NewInstance("svc-Mix")
	ins.Configure(downstream.Behavior{FailTransient: true})

	// 可重试（只读）操作：耗尽3次 -> RETRY_EXHAUSTED，副作用3次。
	_, rReadOnly := c.Invoke(context.Background(), ins,
		Operation{Key: "query", Retryable: true}, downstream.Request{}, nil)
	if rReadOnly.Outcome != OutcomeRetryExhausted {
		t.Fatalf("可重试操作应收敛 RETRY_EXHAUSTED，实际 %s", rReadOnly.Outcome)
	}
	if rReadOnly.Attempts != 3 {
		t.Fatalf("可重试操作应尝试3次，实际 %d", rReadOnly.Attempts)
	}

	// 不可重试（写）操作：首次失败即终局 -> NOT_RETRYABLE，仅1次副作用。
	_, rWrite := c.Invoke(context.Background(), ins,
		Operation{Key: "create", Retryable: false}, downstream.Request{}, nil)
	if rWrite.Outcome != OutcomeNotRetryable {
		t.Fatalf("不可重试操作必须明确拒绝为 NOT_RETRYABLE，实际 %s", rWrite.Outcome)
	}
	if rWrite.Attempts != 1 {
		t.Fatalf("不可重试操作只能尝试1次，实际 %d", rWrite.Attempts)
	}
	if ins.CallCount() != 4 {
		t.Fatalf("混合场景副作用总计应为 3+1=4，实际 %d", ins.CallCount())
	}
}
