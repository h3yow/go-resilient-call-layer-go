package resilience

import (
	"context"
	"testing"
	"time"

	"github.com/h3yow/go-resilient-call-layer-go/pkg/downstream"
)

func TestCaller_DownstreamLatencyExceedsBudget(t *testing.T) {
	c := NewCaller(fastCfg(3, 80*time.Millisecond), testLog(t))
	ins := downstream.NewInstance("svc-T")
	// 下游处理 300ms，远超 80ms 预算：第1次尝试即超时，不得重试硬拖。
	ins.Configure(downstream.Behavior{Latency: 300 * time.Millisecond})

	start := time.Now()
	_, r := c.Invoke(context.Background(), ins, Operation{Key: "get", Retryable: true},
		downstream.Request{}, nil)
	elapsed := time.Since(start)

	if r.Outcome != OutcomeTimeout {
		t.Fatalf("预算内未完成应归类 TIMEOUT，实际 %s: %s", r.Outcome, r.Reason)
	}
	if r.Attempts != 1 {
		t.Fatalf("超时耗尽预算后不得再发起尝试，实际 %d", r.Attempts)
	}
	if ins.CallCount() != 1 {
		t.Fatalf("副作用恰好1次，实际 %d", ins.CallCount())
	}
	// 给退避(5ms)和调度留余量，但不能拖到第2次下游调用(300ms)。
	if elapsed > 200*time.Millisecond {
		t.Fatalf("超时后不应硬拖，总耗时 %s 异常", elapsed)
	}
}

func TestCaller_RemainingBudgetLessThanBackoff(t *testing.T) {
	// 预算 60ms，退避 5ms，下游每次 40ms。
	// 第1次尝试在 ~40ms 失败（ctx 未到期，因为 40<60）；剩余 ~20ms 足够退避 5ms，
	// 第2次尝试开始后在 ~60ms 预算点超时 -> TIMEOUT。
	c := NewCaller(fastCfg(4, 60*time.Millisecond), testLog(t))
	ins := downstream.NewInstance("svc-T")
	ins.Configure(downstream.Behavior{Latency: 40 * time.Millisecond, FailTransient: true})

	_, r := c.Invoke(context.Background(), ins, Operation{Key: "get", Retryable: true},
		downstream.Request{}, nil)
	if r.Outcome != OutcomeTimeout {
		t.Fatalf("重试中途预算耗尽应归类 TIMEOUT 而非 RETRY_EXHAUSTED，实际 %s: %s",
			r.Outcome, r.Reason)
	}
	if r.Attempts != 2 {
		t.Fatalf("应恰好发起2次尝试，实际 %d", r.Attempts)
	}
	if ins.CallCount() != 2 {
		t.Fatalf("副作用应恰好2次，实际 %d", ins.CallCount())
	}
}

func TestCaller_BudgetInsufficientBeforeFirstBackoff(t *testing.T) {
	// 预算 42ms：第1次尝试耗时 40ms 失败后，剩余约 2ms < 退避 5ms，
	// 必须立即判定 TIMEOUT，绝不发起第2次调用。
	c := NewCaller(fastCfg(4, 42*time.Millisecond), testLog(t))
	ins := downstream.NewInstance("svc-T")
	ins.Configure(downstream.Behavior{Latency: 40 * time.Millisecond, FailTransient: true})

	_, r := c.Invoke(context.Background(), ins, Operation{Key: "get", Retryable: true},
		downstream.Request{}, nil)
	if r.Outcome != OutcomeTimeout {
		t.Fatalf("剩余预算不足以覆盖退避应归类 TIMEOUT，实际 %s: %s", r.Outcome, r.Reason)
	}
	if r.Attempts != 1 {
		t.Fatalf("预算不够退避时不得发起第2次尝试，实际 %d", r.Attempts)
	}
	if ins.CallCount() != 1 {
		t.Fatalf("副作用应恰好1次，实际 %d", ins.CallCount())
	}
}

func TestCaller_PreCanceledContextNoSideEffect(t *testing.T) {
	c := NewCaller(fastCfg(3, time.Second), testLog(t))
	ins := downstream.NewInstance("svc-T")
	ins.Configure(downstream.Behavior{Latency: time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, r := c.Invoke(ctx, ins, Operation{Key: "get", Retryable: true},
		downstream.Request{}, degraded)
	if r.Outcome != OutcomeFallback {
		t.Fatalf("预取消场景提供降级后应返回 FALLBACK，实际 %s", r.Outcome)
	}
	if !r.Degraded {
		t.Fatalf("必须标记为降级")
	}
	if r.Attempts != 0 || ins.CallCount() != 0 {
		t.Fatalf("预算已耗尽不得发起下游调用：attempts=%d count=%d", r.Attempts, ins.CallCount())
	}
}

func TestCaller_ContextDeadlineTighterThanConfigBudget(t *testing.T) {
	// ctx 截止 30ms，配置预算 5s：取更早者，30ms 即超时。
	c := NewCaller(fastCfg(3, 5*time.Second), testLog(t))
	ins := downstream.NewInstance("svc-T")
	ins.Configure(downstream.Behavior{Latency: 200 * time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_, r := c.Invoke(ctx, ins, Operation{Key: "get", Retryable: true},
		downstream.Request{}, nil)
	if r.Outcome != OutcomeTimeout || r.Attempts != 1 {
		t.Fatalf("ctx 更紧时应按 ctx 判定 TIMEOUT 且仅1次尝试，实际 %+v", r)
	}
}
