package resilience

import (
	"context"
	"testing"
	"time"
)

func TestNextBackoffExponentialAndCapped(t *testing.T) {
	SetRandSeed(1) // 抖动确定化（factor=0 时无影响）
	cfg := RetryConfig{
		MaxAttempts:    5,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		Multiplier:     2,
		JitterFactor:   0,
	}
	got := []time.Duration{
		nextBackoff(cfg, 1),
		nextBackoff(cfg, 2),
		nextBackoff(cfg, 3),
		nextBackoff(cfg, 4),
	}
	want := []time.Duration{10, 20, 40, 80}
	for i := range got {
		if got[i] != want[i]*time.Millisecond {
			t.Fatalf("attempt=%d backoff=%s want=%s (无抖动时应严格指数)", i+1, got[i], want[i]*time.Millisecond)
		}
	}
	// 超过 MaxBackoff 要被夹住。
	if d := nextBackoff(cfg, 10); d != 100*time.Millisecond {
		t.Fatalf("backoff cap = %s, want 100ms", d)
	}
	t.Logf("退避序列(无抖动): %v, 封顶后=%s", got, nextBackoff(cfg, 10))
}

func TestNextBackoffJitterRange(t *testing.T) {
	cfg := RetryConfig{
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     time.Second,
		Multiplier:     2,
		JitterFactor:   0.3,
	}
	for seed := int64(0); seed < 50; seed++ {
		SetRandSeed(seed)
		d := nextBackoff(cfg, 1)
		if d < 70*time.Millisecond || d > 130*time.Millisecond {
			t.Fatalf("seed=%d jitter backoff=%s 超出 [70ms,130ms]", seed, d)
		}
	}
	t.Log("抖动退避始终落在 base*(1±0.3) 区间内")
}

func TestWaitForBackoffRespectsDeadline(t *testing.T) {
	// 剩余预算比退避短：必须立即放弃，绝不硬拖。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := waitForBackoff(ctx, time.Second)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("预算不足时应返回错误，而不是睡满退避")
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("预算不足仍等待了 %s，硬拖超时调用", elapsed)
	}
	t.Logf("预算 5ms < 退避 1s：立即返回 err=%v，实际等待 %s", err, elapsed)

	// 预算充足：睡满短退避。
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := waitForBackoff(ctx2, 2*time.Millisecond); err != nil {
		t.Fatalf("预算充足不应报错: %v", err)
	}
}

func TestWaitForBackoffWithoutDeadline(t *testing.T) {
	if err := waitForBackoff(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("无 deadline 的 ctx 应正常睡完: %v", err)
	}
}
