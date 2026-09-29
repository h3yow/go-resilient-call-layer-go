package resilience

import (
	"math/rand"
	"testing"
	"time"
)

func TestRetryBackoff_GrowsAndCaps(t *testing.T) {
	// 固定随机源为 0（Float64=0 -> factor=1-j，取下界），便于精确断言指数增长。
	cfg := RetryConfig{
		MaxAttempts:    5,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		Multiplier:     2,
		JitterFraction: 0, // 无抖动：纯指数
	}
	p := newRetryPolicyWithRand(cfg, rand.New(rand.NewSource(1)))

	want := []time.Duration{10, 20, 40, 80, 100, 100}
	for attempt, w := range want {
		if got := p.NextBackoff(attempt + 1); got != w*time.Millisecond {
			t.Fatalf("第%d次失败退避期望 %s，实际 %s", attempt+1, w*time.Millisecond, got)
		}
	}
}

func TestRetryBackoff_JitterBounds(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     time.Second,
		Multiplier:     2,
		JitterFraction: 0.5,
	}
	p := newRetryPolicyWithRand(cfg, rand.New(rand.NewSource(7)))

	// failedAttempt=1 -> base=100ms，抖动范围 [50ms,150ms]。
	for i := 0; i < 1000; i++ {
		got := p.NextBackoff(1)
		if got < 50*time.Millisecond || got > 150*time.Millisecond {
			t.Fatalf("抖动超出 [50ms,150ms]: %s", got)
		}
	}
}

func TestRetryBackoff_NoNegative(t *testing.T) {
	cfg := RetryConfig{
		InitialBackoff: 1 * time.Nanosecond,
		MaxBackoff:     1 * time.Second,
		Multiplier:     2,
		JitterFraction: 1,
	}
	p := newRetryPolicyWithRand(cfg, rand.New(rand.NewSource(3)))
	for i := 1; i <= 10; i++ {
		if got := p.NextBackoff(i); got < 0 {
			t.Fatalf("退避不得为负，第%d次为 %s", i, got)
		}
	}
}
