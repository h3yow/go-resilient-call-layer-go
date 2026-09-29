package resilience

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newTestClient 构造带 t.Logf 日志出口的客户端（日志即判定依据）。
func newTestClient(t *testing.T) *Client {
	t.Helper()
	c := NewClient()
	c.SetLogger(t.Logf)
	return c
}

func mustKind(t *testing.T, r *CallResult, want FailureKind) {
	t.Helper()
	if r.FinalKind != want {
		t.Fatalf("FinalKind=%s(%v) want=%s; err=%v; decisions=%v",
			r.FinalKind, r.FinalKind, want, r.Err, r.Decisions)
	}
}

// 可重试错误：前两次失败，第三次成功 -> 只重试，最终真实成功，副作用 3 次。
func TestCallRetryThenSuccess(t *testing.T) {
	c := newTestClient(t)
	c.Register(DependencyConfig{
		Name: "svc",
		Breaker: BreakerConfig{FailureThreshold: 10, OpenCooldown: time.Second,
			HalfOpenMaxConcurrent: 1, HalfOpenSuccessNeeded: 1},
		Retry: RetryConfig{MaxAttempts: 3, InitialBackoff: time.Millisecond,
			MaxBackoff: 5 * time.Millisecond, Multiplier: 2, JitterFactor: 0},
	})
	tr := NewLocalTransport("svc").Script(
		Fail(true, "抖动1"), Fail(true, "抖动2"), OK("real-data"),
	)
	r := c.Call(context.Background(), tr, &Request{
		Dependency: "svc", Operation: "query", Retryable: true,
	})
	if r.Err != nil || r.Fallback {
		t.Fatalf("应真实成功, err=%v fallback=%v", r.Err, r.Fallback)
	}
	if r.Value != "real-data" {
		t.Fatalf("value=%v", r.Value)
	}
	if r.Attempts != 3 || tr.Calls() != 3 {
		t.Fatalf("尝试次数=%d 副作用次数=%d, want 3/3", r.Attempts, tr.Calls())
	}
	t.Logf("重试后真实成功：尝试=%d 下游调用=%d 决策链=%v", r.Attempts, tr.Calls(), r.Decisions)
}

// 可重试错误一直失败 -> 重试耗尽，结论可区分。
func TestCallRetryExhausted(t *testing.T) {
	c := newTestClient(t)
	c.Register(DependencyConfig{
		Name: "svc",
		Breaker: BreakerConfig{FailureThreshold: 100, OpenCooldown: time.Second,
			HalfOpenMaxConcurrent: 1, HalfOpenSuccessNeeded: 1},
		Retry: RetryConfig{MaxAttempts: 4, InitialBackoff: time.Millisecond,
			MaxBackoff: 2 * time.Millisecond, Multiplier: 2, JitterFactor: 0},
	})
	tr := NewLocalTransport("svc").Script(Fail(true, "一直抖动"))
	r := c.Call(context.Background(), tr, &Request{
		Dependency: "svc", Operation: "query", Retryable: true,
	})
	mustKind(t, r, KindRetryExhausted)
	var ce *CallError
	if !errors.As(r.Err, &ce) || ce.Kind != KindRetryExhausted {
		t.Fatalf("错误应携带 KindRetryExhausted: %v", r.Err)
	}
	if r.Attempts != 4 || tr.Calls() != 4 {
		t.Fatalf("尝试=%d 副作用=%d want 4/4", r.Attempts, tr.Calls())
	}
	t.Logf("重试耗尽：尝试=%d 结论=%s", r.Attempts, r.FinalKind)
}

// 不可重试操作：即使下游标了 retryable，也绝不发起第二次调用。
func TestCallNonRetryableRejectsRetry(t *testing.T) {
	c := newTestClient(t)
	c.Register(DependencyConfig{
		Name: "svc",
		Breaker: BreakerConfig{FailureThreshold: 100, OpenCooldown: time.Second,
			HalfOpenMaxConcurrent: 1, HalfOpenSuccessNeeded: 1},
		Retry: DefaultRetryConfig(),
	})
	tr := NewLocalTransport("svc").Script(Fail(true, "我自己说能重试也没用"))
	r := c.Call(context.Background(), tr, &Request{
		Dependency: "svc", Operation: "charge-money", Retryable: false,
	})
	mustKind(t, r, KindBusiness)
	if tr.Calls() != 1 {
		t.Fatalf("不可重试操作副作用=%d, 必须只调用 1 次", tr.Calls())
	}
	t.Logf("不可重试操作只调用一次：副作用=%d 结论=%s", tr.Calls(), r.FinalKind)
}

// 可重试与不可重试错误混合：第一次可重试失败 -> 重试；第二次不可重试业务失败 -> 立即停止。
func TestCallMixedRetryableAndBusiness(t *testing.T) {
	c := newTestClient(t)
	c.Register(DependencyConfig{
		Name: "svc",
		Breaker: BreakerConfig{FailureThreshold: 100, OpenCooldown: time.Second,
			HalfOpenMaxConcurrent: 1, HalfOpenSuccessNeeded: 1},
		Retry: RetryConfig{MaxAttempts: 5, InitialBackoff: time.Millisecond,
			MaxBackoff: 2 * time.Millisecond, Multiplier: 2, JitterFactor: 0},
	})
	tr := NewLocalTransport("svc").Script(
		Fail(true, "瞬时错误"),
		Fail(false, "余额不足"), // 明确不可重试
		OK("永远到不了"),
	)
	r := c.Call(context.Background(), tr, &Request{
		Dependency: "svc", Operation: "create-order", Retryable: true,
	})
	mustKind(t, r, KindBusiness)
	if r.Attempts != 2 || tr.Calls() != 2 {
		t.Fatalf("业务失败应在第2次尝试停止, attempts=%d calls=%d", r.Attempts, tr.Calls())
	}
	t.Logf("混合错误：第1次重试、第2次业务失败立即停止，尝试=%d", r.Attempts)
}

// 外层总预算不足以覆盖首次尝试：结论必须是 timeout，且不硬拖。
func TestCallTimeoutByBudget(t *testing.T) {
	c := newTestClient(t)
	c.Register(DependencyConfig{
		Name: "svc",
		Breaker: BreakerConfig{FailureThreshold: 100, OpenCooldown: time.Second,
			HalfOpenMaxConcurrent: 1, HalfOpenSuccessNeeded: 1},
		Retry: RetryConfig{MaxAttempts: 5, InitialBackoff: 100 * time.Millisecond,
			MaxBackoff: 200 * time.Millisecond, Multiplier: 2, JitterFactor: 0},
	})
	tr := NewLocalTransport("svc").Script(Timeout())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	r := c.Call(ctx, tr, &Request{Dependency: "svc", Operation: "slow", Retryable: true})
	elapsed := time.Since(start)
	mustKind(t, r, KindTimeout)
	if elapsed > 100*time.Millisecond {
		t.Fatalf("超时后不应硬拖，实际耗时 %s", elapsed)
	}
	t.Logf("总预算 20ms 内超时：耗时=%s 尝试=%d 结论=%s", elapsed, r.Attempts, r.FinalKind)
}

// 剩余时间不足以完成退避：不发起多余尝试，结论 timeout。
func TestCallInsufficientBudgetForBackoff(t *testing.T) {
	c := newTestClient(t)
	c.Register(DependencyConfig{
		Name: "svc",
		Breaker: BreakerConfig{FailureThreshold: 100, OpenCooldown: time.Second,
			HalfOpenMaxConcurrent: 1, HalfOpenSuccessNeeded: 1},
		Retry: RetryConfig{MaxAttempts: 5, InitialBackoff: 300 * time.Millisecond,
			MaxBackoff: 300 * time.Millisecond, Multiplier: 2, JitterFactor: 0},
	})
	tr := NewLocalTransport("svc").Script(Fail(true, "先失败一次"))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	r := c.Call(ctx, tr, &Request{Dependency: "svc", Operation: "query", Retryable: true})
	mustKind(t, r, KindTimeout)
	if tr.Calls() != 1 {
		t.Fatalf("预算不够退避时不应发起第二次下游调用, calls=%d", tr.Calls())
	}
	t.Logf("剩余预算<退避：只调用=%d 次后判定 timeout，决策=%v", tr.Calls(), r.Decisions)
}

// 单次尝试超时 PerAttemptTimeout 短于下游处理时间。
func TestCallPerAttemptTimeout(t *testing.T) {
	c := newTestClient(t)
	c.Register(DependencyConfig{
		Name: "svc",
		Breaker: BreakerConfig{FailureThreshold: 100, OpenCooldown: time.Second,
			HalfOpenMaxConcurrent: 1, HalfOpenSuccessNeeded: 1},
		Retry:             RetryConfig{MaxAttempts: 2, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, Multiplier: 2, JitterFactor: 0},
		PerAttemptTimeout: 10 * time.Millisecond,
	})
	tr := NewLocalTransport("svc").Script(OKAfter(50*time.Millisecond, "data"))
	r := c.Call(context.Background(), tr, &Request{
		Dependency: "svc", Operation: "slow", Retryable: true,
	})
	mustKind(t, r, KindTimeout) // 两次尝试都是超时，最终根因保留 timeout
	t.Logf("单次尝试超时：尝试=%d calls=%d", r.Attempts, tr.Calls())
}

// 降级：主流程业务失败 -> 返回 FallbackResult，标记清晰，且能看到触发原因。
func TestCallFallbackOnFailure(t *testing.T) {
	c := newTestClient(t)
	var fallbackCalls int64
	c.Register(DependencyConfig{
		Name: "svc",
		Breaker: BreakerConfig{FailureThreshold: 100, OpenCooldown: time.Second,
			HalfOpenMaxConcurrent: 1, HalfOpenSuccessNeeded: 1},
		Retry: RetryConfig{MaxAttempts: 1},
		Fallback: func(ctx context.Context, req *Request, cause error) (any, error) {
			fallbackCalls++
			return &FallbackResult{Value: "cached-value"}, nil
		},
	})
	tr := NewLocalTransport("svc").Script(Fail(false, "库存系统挂了"))
	r := c.Call(context.Background(), tr, &Request{
		Dependency: "svc", Operation: "stock", Retryable: false,
	})
	if r.Err != nil {
		t.Fatalf("降级成功时 Err 应为 nil, got %v", r.Err)
	}
	if !r.Fallback || r.FinalKind != KindFallback {
		t.Fatalf("应明确标记降级, fallback=%v kind=%s", r.Fallback, r.FinalKind)
	}
	fv, ok := r.Value.(*FallbackResult)
	if !ok {
		t.Fatalf("降级值必须是 *FallbackResult, got %T", r.Value)
	}
	if fv.Value != "cached-value" {
		t.Fatalf("降级值=%v", fv.Value)
	}
	var ce *CallError
	if !errors.As(fv.Cause, &ce) || ce.Kind != KindBusiness {
		t.Fatalf("降级 Cause 应保留原始失败分类 business_error, got %v", fv.Cause)
	}
	if fallbackCalls != 1 {
		t.Fatalf("降级函数只应调用 1 次, got %d", fallbackCalls)
	}
	t.Logf("降级返回：fallback=%v value=%v cause=%v", r.Fallback, fv.Value, fv.Cause)
}

// 熔断打开时：不产生任何下游调用，直接走降级，结论可解释。
func TestCallFallbackWhenCircuitOpen(t *testing.T) {
	c := newTestClient(t)
	c.Register(DependencyConfig{
		Name: "svc",
		Breaker: BreakerConfig{FailureThreshold: 1, OpenCooldown: time.Hour,
			HalfOpenMaxConcurrent: 1, HalfOpenSuccessNeeded: 1},
		Retry: RetryConfig{MaxAttempts: 1},
		Fallback: func(ctx context.Context, req *Request, cause error) (any, error) {
			return "degraded", nil // 返回裸值也应被统一包装成 FallbackResult
		},
	})
	tr := NewLocalTransport("svc").Script(Fail(true, "boom"))
	// 第一次调用失败（阈值 1 立即熔断），其结果走降级，但熔断必须已经打开。
	r1 := c.Call(context.Background(), tr, &Request{Dependency: "svc", Retryable: false})
	if !r1.Fallback {
		t.Fatalf("第一次失败也应走降级, kind=%s", r1.FinalKind)
	}
	if c.Breaker("svc").State() != StateOpen {
		t.Fatal("第一次失败后熔断应打开")
	}
	callsAfterFirst := tr.Calls()
	// 第二次调用应被熔断拒绝、零下游副作用、走降级。
	r2 := c.Call(context.Background(), tr, &Request{Dependency: "svc", Retryable: true})
	if !r2.Fallback || r2.FinalKind != KindFallback {
		t.Fatalf("熔断拒绝后应走降级, kind=%s fallback=%v", r2.FinalKind, r2.Fallback)
	}
	fv := r2.Value.(*FallbackResult)
	var ce *CallError
	if !errors.As(fv.Cause, &ce) || ce.Kind != KindCircuitOpen {
		t.Fatalf("降级 Cause 应为 circuit_open, got %v", fv.Cause)
	}
	if tr.Calls() != callsAfterFirst {
		t.Fatalf("熔断拒绝不得产生下游调用, before=%d after=%d", callsAfterFirst, tr.Calls())
	}
	if r2.Attempts != 0 {
		t.Fatalf("熔断拒绝时实际尝试次数应为0, got %d", r2.Attempts)
	}
	t.Logf("熔断打开走降级：零下游调用，cause=%v，降级值=%v", fv.Cause, fv.Value)
}

// 依赖隔离：实例 A 熔断打开期间，实例 B 的调用完全正常，且 B 自己的策略独立。
func TestCallDependencyIsolation(t *testing.T) {
	c := newTestClient(t)
	brkCfg := BreakerConfig{FailureThreshold: 1, OpenCooldown: time.Hour,
		HalfOpenMaxConcurrent: 1, HalfOpenSuccessNeeded: 1}
	c.Register(DependencyConfig{Name: "A", Breaker: brkCfg, Retry: RetryConfig{MaxAttempts: 1}})
	c.Register(DependencyConfig{Name: "B", Breaker: brkCfg, Retry: RetryConfig{MaxAttempts: 1}})
	trA := NewLocalTransport("A").Script(Fail(false, "A 挂了"))
	trB := NewLocalTransport("B").Script(OK("B-ok"))

	for i := 0; i < 10; i++ {
		ra := c.Call(context.Background(), trA, &Request{Dependency: "A", Retryable: false})
		if i == 0 {
			mustKind(t, ra, KindBusiness)
		} else {
			mustKind(t, ra, KindCircuitOpen)
		}
	}
	if c.Breaker("A").State() != StateOpen || c.Breaker("B").State() != StateClosed {
		t.Fatalf("隔离失败 A=%s B=%s", c.Breaker("A").State(), c.Breaker("B").State())
	}
	for i := 0; i < 10; i++ {
		rb := c.Call(context.Background(), trB, &Request{Dependency: "B", Retryable: true})
		if rb.Err != nil || rb.Value != "B-ok" {
			t.Fatalf("B 不应受 A 影响: err=%v value=%v", rb.Err, rb.Value)
		}
	}
	sa, sb := c.Breaker("A").Stats(), c.Breaker("B").Stats()
	t.Logf("隔离验证：A state=%s rejected=%d calls(A)=%d | B state=%s failures=%d calls(B)=%d",
		sa.State, sa.TotalRejected, trA.Calls(), sb.State, sb.ConsecutiveFailures, trB.Calls())
	if trA.Calls() != 1 {
		t.Fatalf("A 熔断后不应再有副作用, calls=%d", trA.Calls())
	}
	if trB.Calls() != 10 {
		t.Fatalf("B 10 次都应真实调用, calls=%d", trB.Calls())
	}
}

// 高并发持续失败：熔断器必然在某个时刻打开；
// 打开后的请求必须零下游副作用、全部得到 circuit_open 的可解释结论；
// 并发结束后统计自洽：下游调用次数 == 各次调用尝试次数之和，失败计数不重复。
func TestCallConcurrentFailuresTripsBreaker(t *testing.T) {
	c := newTestClient(t)
	c.Register(DependencyConfig{
		Name: "svc",
		Breaker: BreakerConfig{FailureThreshold: 8, OpenCooldown: time.Hour,
			HalfOpenMaxConcurrent: 1, HalfOpenSuccessNeeded: 1},
		Retry: RetryConfig{MaxAttempts: 1},
	})
	var calls int64
	tr := NewLocalTransport("svc").Dynamic(func(ctx context.Context, req *Request, seq int) *Response {
		atomic.AddInt64(&calls, 1)
		return &Response{Err: &DownstreamError{Op: req.Operation, Msg: "boom", Retryable: true}}
	})

	const goroutines = 200
	var wg sync.WaitGroup
	var businessKinds, openKinds, sumAttempts int64
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r := c.Call(context.Background(), tr, &Request{Dependency: "svc", Retryable: false})
			switch r.FinalKind {
			case KindBusiness:
				atomic.AddInt64(&businessKinds, 1)
			case KindCircuitOpen:
				atomic.AddInt64(&openKinds, 1)
			default:
				t.Errorf("并发失败场景出现意外结论: %s", r.FinalKind)
			}
			atomic.AddInt64(&sumAttempts, int64(r.Attempts))
		}()
	}
	close(start)
	wg.Wait()

	if got := tr.Calls(); got != calls || got != sumAttempts {
		t.Fatalf("计数不自洽：transport.Calls=%d 动态计数=%d 结果尝试之和=%d",
			tr.Calls(), calls, sumAttempts)
	}
	if c.Breaker("svc").State() != StateOpen {
		t.Fatalf("熔断器应已打开, got %s", c.Breaker("svc").State())
	}
	st := c.Breaker("svc").Stats()
	// 阈值 8：切换瞬间可能已有一批请求在熔断打开前被放行并跑完，
	// 因此穿透失败数 >= 8 且 <= 200 都是合法的；但连续失败计数本身必须精确等于阈值，
	// 因为熔断打开后的请求根本拿不到 permit，不可能再上报失败。
	if businessKinds < 8 || businessKinds > goroutines {
		t.Fatalf("穿透失败数=%d 不在合法区间 [8,%d]", businessKinds, goroutines)
	}
	if businessKinds+openKinds != goroutines {
		t.Fatalf("结论之和=%d want %d", businessKinds+openKinds, goroutines)
	}
	if st.ConsecutiveFailures != 8 {
		t.Fatalf("连续失败计数=%d want 8（不得重复累加）", st.ConsecutiveFailures)
	}
	if openKinds != st.TotalRejected {
		t.Fatalf("熔断拒绝结果数=%d 与 Breaker 拒绝计数=%d 不一致", openKinds, st.TotalRejected)
	}
	t.Logf("200 并发：穿透失败=%d 熔断拒绝=%d 总熔断次数=%d 下游副作用=%d 最终状态=%s",
		businessKinds, openKinds, st.TotalTrips, tr.Calls(), st.State)
}

// 熔断切换窗口的高并发：从 OPEN 冷却到期进入 HALF_OPEN 的瞬间，
// 放行探测数严格等于 HalfOpenMaxConcurrent，且每个成功探测只计一次；
// 全部探测成功后恢复 CLOSED，所有并发请求都拿到明确结论。
func TestCallConcurrentHalfOpenTransition(t *testing.T) {
	c := newTestClient(t)
	const maxConcurrent = 2
	c.Register(DependencyConfig{
		Name: "svc",
		Breaker: BreakerConfig{FailureThreshold: 1, OpenCooldown: 40 * time.Millisecond,
			HalfOpenMaxConcurrent: maxConcurrent, HalfOpenSuccessNeeded: maxConcurrent},
		Retry: RetryConfig{MaxAttempts: 1},
	})
	var calls int64
	tr := NewLocalTransport("svc").Dynamic(func(ctx context.Context, req *Request, seq int) *Response {
		atomic.AddInt64(&calls, 1)
		// 首个调用（seq=0）失败以打开熔断；之后全部成功用于半开探测。
		if seq == 0 {
			return &Response{Err: &DownstreamError{Op: req.Operation, Msg: "init", Retryable: true}}
		}
		return &Response{Value: "ok"}
	})

	// 打开熔断。
	r0 := c.Call(context.Background(), tr, &Request{Dependency: "svc", Retryable: false})
	if r0.FinalKind != KindBusiness {
		t.Fatalf("首次应业务失败, got %s", r0.FinalKind)
	}

	// 冷却期内并发打一波：全部 circuit_open，且零新增副作用。
	hitDuringOpen := hammer(t, c, tr, 64, false)
	if hitDuringOpen[KindCircuitOpen] != 64 {
		t.Fatalf("OPEN 期间 64 并发应全部 circuit_open, got=%v", hitDuringOpen)
	}

	// 等待冷却到期，再用高并发争抢半开探测名额。
	// 用屏障把拿到探测名额的请求钉在下游处理中，确保 burst 个请求全部在
	// “半开满额”窗口内完成 Allow 判定，而不是被快速恢复 CLOSED 后放行。
	time.Sleep(45 * time.Millisecond)
	before := tr.Calls()
	const burst = 128
	var probes int64
	probesIn := make(chan struct{}, maxConcurrent)
	releaseProbes := make(chan struct{})
	tr.Dynamic(func(ctx context.Context, req *Request, seq int) *Response {
		if seq == 0 {
			return &Response{Err: &DownstreamError{Op: req.Operation, Msg: "init", Retryable: true}}
		}
		// 只有抢到半开名额的请求会真正进入传输；它们在此挂住直到统一释放。
		atomic.AddInt64(&probes, 1)
		probesIn <- struct{}{}
		<-releaseProbes
		return &Response{Value: "ok"}
	})

	// 两阶段闸门：先让全部 goroutine 就位，再统一起跑，保证争抢真实并发。
	ready := make(chan struct{}, burst)
	start := make(chan struct{})
	var mu sync.Mutex
	dist := map[FailureKind]int64{}
	var wg sync.WaitGroup
	for i := 0; i < burst; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready <- struct{}{}
			<-start
			r := c.Call(context.Background(), tr, &Request{Dependency: "svc", Retryable: true})
			mu.Lock()
			dist[r.FinalKind]++
			mu.Unlock()
		}()
	}
	for i := 0; i < burst; i++ {
		<-ready
	}
	close(start)

	// 等两个探测槽被占满；此时其余请求应被半开上限拒绝。
	waitUntil(func() bool { return atomic.LoadInt64(&probes) == maxConcurrent }, time.Second)
	waitUntil(func() bool {
		mu.Lock()
		done := dist[KindCircuitOpen] == burst-maxConcurrent
		mu.Unlock()
		return done
	}, time.Second)

	// 此时状态必须仍是 HALF_OPEN（探测被挂住，尚未上报结果）。
	if c.Breaker("svc").State() != StateHalfOpen {
		t.Fatalf("探测在途时应保持 HALF_OPEN, got %s", c.Breaker("svc").State())
	}
	st := c.Breaker("svc").Stats()
	if st.HalfOpenInFlight != maxConcurrent {
		t.Fatalf("半开在途=%d want %d", st.HalfOpenInFlight, maxConcurrent)
	}
	if tr.Calls()-before != int64(maxConcurrent) {
		t.Fatalf("满额窗口内新增下游副作用=%d want %d", tr.Calls()-before, maxConcurrent)
	}

	// 统一释放探测：两次成功 -> CLOSED。
	for i := 0; i < maxConcurrent; i++ {
		<-probesIn // 排空，避免屏障 channel 干扰
	}
	close(releaseProbes)
	wg.Wait()

	st = c.Breaker("svc").Stats()
	t.Logf("冷却到期后 %d 并发争抢探测：结论分布=%v, 新增下游调用=%d, 最终状态=%s, 拒绝累计=%d",
		burst, dist, tr.Calls()-before, c.Breaker("svc").State(), st.TotalRejected)

	// 恰好 maxConcurrent 个请求穿透为半开探测（成功），其余被拒绝；
	// 不允许“一股脑全打过去”。
	if dist[KindUnknown] != maxConcurrent { // 真实成功 FinalKind=KindUnknown(0)
		t.Fatalf("半开探测穿透数=%d want %d, 分布=%v", dist[KindUnknown], maxConcurrent, dist)
	}
	if dist[KindCircuitOpen] != burst-maxConcurrent {
		t.Fatalf("半开满额拒绝数=%d want %d", dist[KindCircuitOpen], burst-maxConcurrent)
	}
	if tr.Calls()-before != int64(maxConcurrent) {
		t.Fatalf("新增下游副作用=%d want %d", tr.Calls()-before, maxConcurrent)
	}
	if c.Breaker("svc").State() != StateClosed {
		t.Fatalf("探测全部成功后应 CLOSED, got %s", c.Breaker("svc").State())
	}
	if st.HalfOpenInFlight != 0 || st.ConsecutiveFailures != 0 {
		t.Fatalf("恢复后计数应清零: inFlight=%d failures=%d",
			st.HalfOpenInFlight, st.ConsecutiveFailures)
	}
}

// waitUntil 轮询条件直到成立或超时，超时即 fatal。
func waitUntil(cond func() bool, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	panic("waitUntil: 条件在超时内未成立")
}

// hammer 发起 n 个并发 Call，返回按 FinalKind 聚合的数量分布。
func hammer(t *testing.T, c *Client, tr *LocalTransport, n int, retryable bool) map[FailureKind]int64 {
	t.Helper()
	var mu sync.Mutex
	dist := map[FailureKind]int64{}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r := c.Call(context.Background(), tr, &Request{Dependency: "svc", Retryable: retryable})
			mu.Lock()
			dist[r.FinalKind]++
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	return dist
}

// 高并发下“同一次调用不重复统计”：每个请求只打一次下游（MaxAttempts=1），
// transport 记录数必须与穿透请求数严格相等，重复 Complete 也不影响计数。
func TestNoDuplicateSideEffectsUnderConcurrency(t *testing.T) {
	c := newTestClient(t)
	c.Register(DependencyConfig{
		Name: "svc",
		Breaker: BreakerConfig{FailureThreshold: 100000, OpenCooldown: time.Hour,
			HalfOpenMaxConcurrent: 1, HalfOpenSuccessNeeded: 1},
		Retry: RetryConfig{MaxAttempts: 1},
	})
	tr := NewLocalTransport("svc").Dynamic(func(ctx context.Context, req *Request, seq int) *Response {
		return &Response{Value: seq}
	})
	const n = 300
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r := c.Call(context.Background(), tr, &Request{Dependency: "svc", Retryable: true})
			if r.Err != nil {
				t.Errorf("意外失败: %v", r.Err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if tr.Calls() != n {
		t.Fatalf("下游副作用=%d want %d，存在重复/遗漏", tr.Calls(), n)
	}
	if len(tr.Records()) != n {
		t.Fatalf("调用记录数=%d want %d", len(tr.Records()), n)
	}
	// 记录序号应严格 0..n-1 无重复。
	seen := map[int]bool{}
	for _, rec := range tr.Records() {
		if seen[rec.Seq] {
			t.Fatalf("序号 %d 重复，并发计数错乱", rec.Seq)
		}
		seen[rec.Seq] = true
	}
	t.Logf("300 并发成功调用：副作用=%d，序号无重复", tr.Calls())
}
