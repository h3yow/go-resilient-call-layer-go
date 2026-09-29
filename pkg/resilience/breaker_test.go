package resilience

import (
	"sync"
	"testing"
	"time"
)

func newTestBreaker(t *testing.T) (*breaker, *clock) {
	t.Helper()
	clk := &clock{}
	cfg := BreakerConfig{
		FailureThreshold:         3,
		OpenDuration:             100 * time.Millisecond,
		HalfOpenMaxConcurrent:    2,
		HalfOpenSuccessThreshold: 2,
	}
	b := newBreakerWithClock("dep-A", cfg, testLog(t), clk.now)
	return b, clk
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func TestBreaker_ClosedFailureThresholdOpens(t *testing.T) {
	b, clk := newTestBreaker(t)

	// 未达阈值的失败累计，成功会清零。
	a := b.Allow(clk.now())
	if !a.Allowed {
		t.Fatalf("首次调用应放行")
	}
	b.OnFailure(clk.now(), a.Generation)
	if snap := b.Snapshot(clk.now()); snap.ConsecutiveFail != 1 {
		t.Fatalf("连续失败应为1，实际 %d", snap.ConsecutiveFail)
	}
	// 成功清零。
	a = b.Allow(clk.now())
	b.OnSuccess(clk.now(), a.Generation)
	if snap := b.Snapshot(clk.now()); snap.ConsecutiveFail != 0 {
		t.Fatalf("成功后连续失败应清零，实际 %d", snap.ConsecutiveFail)
	}

	// 连续失败达到阈值 -> OPEN。
	var openGen int64
	for i := 1; i <= 3; i++ {
		d := b.Allow(clk.now())
		b.OnFailure(clk.now(), d.Generation)
		openGen = d.Generation
	}
	snap := b.Snapshot(clk.now())
	if snap.State != StateOpen {
		t.Fatalf("达到失败阈值后应为 OPEN，实际 %s", snap.State)
	}
	// OPEN 期间一律拒绝，且拒绝不产生任何失败计数累加。
	d := b.Allow(clk.now())
	if d.Allowed {
		t.Fatalf("OPEN 状态必须拒绝")
	}
	snap2 := b.Snapshot(clk.now())
	if snap2.Generation != openGen+1 {
		t.Fatalf("OPEN 迁移应开启新代际，期望 %d 实际 %d", openGen+1, snap2.Generation)
	}
}

func TestBreaker_OpenToHalfOpenAndRecovery(t *testing.T) {
	b, clk := newTestBreaker(t)
	tripOpen(t, b, clk)

	// 冷却未到，仍拒绝。
	clk.advance(50 * time.Millisecond)
	if d := b.Allow(clk.now()); d.Allowed {
		t.Fatalf("冷却未结束应保持 OPEN 拒绝")
	}

	// 冷却结束，惰性进入 HALF_OPEN，探测放行有并发上限。
	clk.advance(60 * time.Millisecond)
	p1 := b.Allow(clk.now())
	if !p1.Allowed {
		t.Fatalf("半开探测1应放行")
	}
	p2 := b.Allow(clk.now())
	if !p2.Allowed {
		t.Fatalf("半开探测2应放行（达到上限）")
	}
	if extra := b.Allow(clk.now()); extra.Allowed {
		t.Fatalf("半开探测名额已满，第3个并发必须拒绝，不能一股脑全打过去")
	}

	// 探测1成功：释放一个名额，连续成功 1/2，仍 HALF_OPEN。
	b.OnSuccess(clk.now(), p1.Generation)
	if snap := b.Snapshot(clk.now()); snap.State != StateHalfOpen || snap.HalfOpenSuccess != 1 {
		t.Fatalf("首次探测成功后应保持 HALF_OPEN 且成功数1，实际 %+v", snap)
	}
	// 名额释放后可以再放一个探测。
	p3 := b.Allow(clk.now())
	if !p3.Allowed {
		t.Fatalf("探测完成释放名额后应能再次放行")
	}
	b.OnSuccess(clk.now(), p3.Generation)
	if snap := b.Snapshot(clk.now()); snap.State != StateClosed {
		t.Fatalf("连续探测成功达到阈值应恢复 CLOSED，实际 %s", snap.State)
	}
	// 旧代际（半开代际）的迟到上报必须被忽略，不影响已恢复的状态。
	b.OnSuccess(clk.now(), p2.Generation)
	b.OnFailure(clk.now(), p2.Generation)
	if snap := b.Snapshot(clk.now()); snap.State != StateClosed || snap.ConsecutiveFail != 0 {
		t.Fatalf("过期代际上报不得重复计数，实际 %+v", snap)
	}
}

func TestBreaker_HalfOpenFailureReopens(t *testing.T) {
	b, clk := newTestBreaker(t)
	tripOpen(t, b, clk)
	clk.advance(101 * time.Millisecond)

	p1 := b.Allow(clk.now())
	halfOpenGen := p1.Generation
	p2 := b.Allow(clk.now())
	// 探测1失败 -> 立即重新 OPEN。
	b.OnFailure(clk.now(), p1.Generation)
	if snap := b.Snapshot(clk.now()); snap.State != StateOpen {
		t.Fatalf("半开探测失败应立即重新 OPEN，实际 %s", snap.State)
	}
	// 探测2的迟到结果属于旧代际，必须忽略：计数/状态不再变化。
	b.OnSuccess(clk.now(), p2.Generation)
	snap := b.Snapshot(clk.now())
	if snap.State != StateOpen {
		t.Fatalf("过期代际成功不应让熔断恢复，实际 %s", snap.State)
	}
	if snap.Generation == halfOpenGen {
		t.Fatalf("重新 OPEN 应开启新代际")
	}
}

func TestBreaker_HighConcurrentTransitionsConsistent(t *testing.T) {
	b, clk := newTestBreaker(t)
	tripOpen(t, b, clk)
	clk.advance(101 * time.Millisecond)

	// 高并发申请半开探测：放行数必须恰好等于上限，拒绝数 = 其余。
	const goroutines = 200
	var wg sync.WaitGroup
	var allowedMu sync.Mutex
	allowed := 0
	rejected := 0
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			d := b.Allow(clk.now())
			allowedMu.Lock()
			if d.Allowed {
				allowed++
			} else {
				rejected++
			}
			allowedMu.Unlock()
		}()
	}
	wg.Wait()
	if allowed != 2 {
		t.Fatalf("半开放行数必须恰好为并发上限2，实际 %d", allowed)
	}
	if rejected != goroutines-2 {
		t.Fatalf("拒绝数应为 %d，实际 %d", goroutines-2, rejected)
	}
	snap := b.Snapshot(clk.now())
	if snap.HalfOpenActive != 2 {
		t.Fatalf("半开在途计数必须为2，实际 %d", snap.HalfOpenActive)
	}

	// 两个在途探测恰好各上报一次（成功），计数必须恰好归零，不重复累加。
	gens := activeGenerations(b)
	for _, g := range gens {
		b.OnSuccess(clk.now(), g)
	}
	snap = b.Snapshot(clk.now())
	if snap.HalfOpenActive != 0 {
		t.Fatalf("在途探测各上报一次后 HalfOpenActive 应为0，实际 %d", snap.HalfOpenActive)
	}
	if snap.State != StateClosed {
		t.Fatalf("两个探测均成功(2/2)应恢复 CLOSED，实际 %s", snap.State)
	}
	// 重复上报同一批探测结果必须被代际机制忽略，计数不得错乱。
	for _, g := range gens {
		b.OnSuccess(clk.now(), g)
		b.OnFailure(clk.now(), g)
	}
	snap = b.Snapshot(clk.now())
	if snap.State != StateClosed || snap.ConsecutiveFail != 0 {
		t.Fatalf("重复/过期上报不得重复累加，实际 %+v", snap)
	}
}

func tripOpen(t *testing.T, b *breaker, clk *clock) {
	t.Helper()
	for i := 0; i < 3; i++ {
		d := b.Allow(clk.now())
		if !d.Allowed {
			t.Fatalf("CLOSED 阶段应放行")
		}
		b.OnFailure(clk.now(), d.Generation)
	}
	if snap := b.Snapshot(clk.now()); snap.State != StateOpen {
		t.Fatalf("前置条件: 应已 OPEN，实际 %s", snap.State)
	}
}

func activeGenerations(b *breaker) []int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	gens := make([]int64, 0, b.halfOpenActive)
	for i := 0; i < b.halfOpenActive; i++ {
		gens = append(gens, b.generation)
	}
	return gens
}

func testLog(t *testing.T) LogFunc {
	return func(format string, args ...any) {
		t.Logf(format, args...)
	}
}
