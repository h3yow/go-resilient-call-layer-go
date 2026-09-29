package resilience

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testBreaker(t *testing.T, cfg BreakerConfig) *Breaker {
	b := NewBreaker("svc-a", cfg)
	b.SetLogger(t.Logf)
	return b
}

func TestBreakerTripsAndRecovers(t *testing.T) {
	cfg := BreakerConfig{
		FailureThreshold:      3,
		OpenCooldown:          30 * time.Millisecond,
		HalfOpenMaxConcurrent: 1,
		HalfOpenSuccessNeeded: 2,
	}
	b := testBreaker(t, cfg)
	ctx := context.Background()

	// 3 次连续失败 -> OPEN
	for i := 1; i <= 3; i++ {
		p, err := b.Allow(ctx)
		if err != nil {
			t.Fatalf("第%d次失败前应放行: %v", i, err)
		}
		p.Complete(false)
	}
	if got := b.State(); got != StateOpen {
		t.Fatalf("连续3次失败后应 OPEN, got %s", got)
	}
	if b.Stats().TotalTrips != 1 {
		t.Fatalf("TotalTrips=%d want 1", b.Stats().TotalTrips)
	}

	// OPEN 期间一律拒绝，且不产生 permit
	if _, err := b.Allow(ctx); err != ErrCircuitOpen {
		t.Fatalf("OPEN 期间应拒绝, got %v", err)
	}

	// 冷却到期后第 1 个请求进入 HALF_OPEN 并探测
	time.Sleep(35 * time.Millisecond)
	p1, err := b.Allow(ctx)
	if err != nil {
		t.Fatalf("冷却到期后探测应放行: %v", err)
	}
	if b.State() != StateHalfOpen {
		t.Fatal("应已迁入 HALF_OPEN")
	}
	// 半开上限 1，第二个并发探测必须被拒绝
	if _, err := b.Allow(ctx); err != ErrCircuitOpen {
		t.Fatalf("半开在途已满应拒绝, got %v", err)
	}
	p1.Complete(true)
	// 成功 1/2，仍 HALF_OPEN
	if b.State() != StateHalfOpen {
		t.Fatalf("1/2 成功应仍 HALF_OPEN, got %s", b.State())
	}
	p2, err := b.Allow(ctx)
	if err != nil {
		t.Fatalf("槽位释放后应再放行探测: %v", err)
	}
	p2.Complete(true)
	if b.State() != StateClosed {
		t.Fatalf("2/2 成功后应 CLOSED, got %s", b.State())
	}
	if st := b.Stats(); st.ConsecutiveFailures != 0 {
		t.Fatalf("恢复后连续失败计数应清零, got %d", st.ConsecutiveFailures)
	}
}

func TestBreakerHalfOpenFailureReopens(t *testing.T) {
	cfg := BreakerConfig{
		FailureThreshold:      1,
		OpenCooldown:          20 * time.Millisecond,
		HalfOpenMaxConcurrent: 2,
		HalfOpenSuccessNeeded: 2,
	}
	b := testBreaker(t, cfg)
	ctx := context.Background()

	p, _ := b.Allow(ctx)
	p.Complete(false)
	if b.State() != StateOpen {
		t.Fatal("阈值1，一次失败即 OPEN")
	}
	time.Sleep(25 * time.Millisecond)
	p, err := b.Allow(ctx) // 触发 OPEN -> HALF_OPEN
	if err != nil {
		t.Fatalf("探测应放行: %v", err)
	}
	p.Complete(false) // 半开探测失败 -> 重新 OPEN
	if b.State() != StateOpen {
		t.Fatalf("半开探测失败应重新 OPEN, got %s", b.State())
	}
	if b.Stats().TotalTrips != 2 {
		t.Fatalf("TotalTrips=%d want 2", b.Stats().TotalTrips)
	}
}

func TestBreakerSuccessResetsConsecutiveFailures(t *testing.T) {
	cfg := BreakerConfig{FailureThreshold: 3, OpenCooldown: time.Second,
		HalfOpenMaxConcurrent: 1, HalfOpenSuccessNeeded: 1}
	b := testBreaker(t, cfg)
	ctx := context.Background()
	p, _ := b.Allow(ctx)
	p.Complete(false)
	p, _ = b.Allow(ctx)
	p.Complete(false)
	p, _ = b.Allow(ctx)
	p.Complete(true) // 成功应把连续失败清零
	if st := b.Stats(); st.ConsecutiveFailures != 0 {
		t.Fatalf("成功后连续失败应清零, got %d", st.ConsecutiveFailures)
	}
	// 再来 2 次失败不应熔断（因为计数被重置了）
	for i := 0; i < 2; i++ {
		p, _ := b.Allow(ctx)
		p.Complete(false)
	}
	if b.State() != StateClosed {
		t.Fatalf("连续失败未重新达到阈值应仍 CLOSED, got %s", b.State())
	}
}

func TestBreakerIsolation(t *testing.T) {
	// 两个实例熔断器互不影响：A 一路失败打开，B 始终可放行。
	cfg := BreakerConfig{FailureThreshold: 2, OpenCooldown: time.Second,
		HalfOpenMaxConcurrent: 1, HalfOpenSuccessNeeded: 1}
	a := NewBreaker("inst-A", cfg)
	bk := NewBreaker("inst-B", cfg)
	a.SetLogger(t.Logf)
	bk.SetLogger(t.Logf)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		p, err := a.Allow(ctx)
		if err != nil {
			if err != ErrCircuitOpen || a.State() != StateOpen {
				t.Fatalf("A 第%d次请求应因 OPEN 被拒, got err=%v state=%s", i+1, err, a.State())
			}
			continue
		}
		p.Complete(false)
	}
	if a.State() != StateOpen {
		t.Fatal("A 应 OPEN")
	}
	for i := 0; i < 5; i++ {
		p, err := bk.Allow(ctx)
		if err != nil {
			t.Fatalf("B 不应受 A 影响, 第%d次被拒: %v", i+1, err)
		}
		p.Complete(true)
	}
	if bk.State() != StateClosed {
		t.Fatal("B 应保持 CLOSED")
	}
	t.Log("实例 A=OPEN，实例 B=CLOSED：按依赖维度隔离生效")
}

func TestPermitCompleteIdempotent(t *testing.T) {
	cfg := BreakerConfig{FailureThreshold: 3, OpenCooldown: time.Second,
		HalfOpenMaxConcurrent: 1, HalfOpenSuccessNeeded: 1}
	b := testBreaker(t, cfg)
	ctx := context.Background()
	p, _ := b.Allow(ctx)
	// 即使调用方重复 Complete（模拟 defer + 显式调用），也只能计一次失败。
	p.Complete(false)
	p.Complete(false)
	p.Complete(false)
	if st := b.Stats(); st.ConsecutiveFailures != 1 {
		t.Fatalf("重复 Complete 不应重复计数, got %d", st.ConsecutiveFailures)
	}
	t.Log("permit.Complete 幂等：同一尝试失败计数只累加一次")
}

func TestHalfOpenConcurrentCapUnderHighConcurrency(t *testing.T) {
	// 高并发场景：半开期间放行数严格有上限，其余全部快速拒绝；计数不错乱。
	cfg := BreakerConfig{FailureThreshold: 1, OpenCooldown: 20 * time.Millisecond,
		HalfOpenMaxConcurrent: 3, HalfOpenSuccessNeeded: 3}
	b := testBreaker(t, cfg)
	ctx := context.Background()
	p, _ := b.Allow(ctx)
	p.Complete(false)
	time.Sleep(25 * time.Millisecond)

	const n = 100
	var wg sync.WaitGroup
	var allowed, rejected int64
	var mu sync.Mutex
	var permits []*permit
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			pp, err := b.Allow(ctx)
			if err != nil {
				atomic.AddInt64(&rejected, 1)
				return
			}
			atomic.AddInt64(&allowed, 1)
			mu.Lock()
			permits = append(permits, pp)
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()

	if allowed != 3 {
		t.Fatalf("半开放行=%d want 3（上限），拒绝=%d", allowed, rejected)
	}
	if allowed+rejected != n {
		t.Fatalf("放行+拒绝=%d want %d，每个请求都必须有明确结论", allowed+rejected, n)
	}
	// 3 个探测全部成功 -> CLOSED；每个 permit 只 Complete 一次。
	for _, pp := range permits {
		pp.Complete(true)
	}
	if b.State() != StateClosed {
		t.Fatalf("3 个探测成功后应 CLOSED, got %s", b.State())
	}
	st := b.Stats()
	if st.HalfOpenInFlight != 0 {
		t.Fatalf("恢复后在途计数应为0, got %d", st.HalfOpenInFlight)
	}
	t.Logf("100 并发抢半开探测：放行=%d 拒绝=%d；全部成功后状态=%s",
		allowed, rejected, b.State())
}
