package resilience

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// RetryConfig 重试配置。
type RetryConfig struct {
	// MaxAttempts 最大尝试次数（含首次），<=1 表示不重试。
	MaxAttempts int
	// InitialBackoff 首次重试前的基础退避。
	InitialBackoff time.Duration
	// MaxBackoff 退避上限。
	MaxBackoff time.Duration
	// Multiplier 指数退避乘数（<=0 时按 2 处理）。
	Multiplier float64
	// JitterFactor 抖动比例 [0,1]；实际退避在 base*(1±factor) 之间均匀随机。
	JitterFactor float64
}

// DefaultRetryConfig 演示用默认值：3 次尝试、指数退避、±20% 抖动。
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		Multiplier:     2,
		JitterFactor:   0.2,
	}
}

var (
	// randMu 保护 randSource 并发使用（math/rand 顶层函数在 1.20+ 已安全，
	// 这里保留独立 source 以便测试注入确定性随机）。
	randMu sync.Mutex
	// randSource 可替换，便于测试确定性。
	randSource = rand.New(rand.NewSource(1))
)

// SetRandSeed 重置抖动随机源种子（测试用）。
func SetRandSeed(seed int64) {
	randMu.Lock()
	randSource = rand.New(rand.NewSource(seed))
	randMu.Unlock()
}

// nextBackoff 计算第 attempt 次重试前（attempt 从 1 开始，即首次尝试失败后）
// 的退避时长：指数增长并夹到 MaxBackoff，再叠加对称抖动。
func nextBackoff(cfg RetryConfig, attempt int) time.Duration {
	mult := cfg.Multiplier
	if mult <= 0 {
		mult = 2
	}
	base := float64(cfg.InitialBackoff)
	for i := 1; i < attempt; i++ {
		base *= mult
	}
	if max := float64(cfg.MaxBackoff); max > 0 && base > max {
		base = max
	}
	factor := cfg.JitterFactor
	if factor < 0 {
		factor = 0
	}
	if factor > 1 {
		factor = 1
	}
	randMu.Lock()
	// [1-factor, 1+factor) 上均匀分布。
	jitter := 1 + (randSource.Float64()*2-1)*factor
	randMu.Unlock()
	d := time.Duration(float64(base) * jitter)
	if d < 0 {
		d = 0
	}
	return d
}

// waitForBackoff 在遵守 ctx 剩余时间预算的前提下等待退避。
// 预算不足以睡满退避时：立即返回 ctx.Err()，绝不把已经超时的调用硬拖着跑；
// ctx 没有 deadline 时正常睡满。
func waitForBackoff(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	// 若剩余预算比退避还短，直接判定预算不足，不做无意义等待。
	if dl, ok := ctx.Deadline(); ok {
		remain := time.Until(dl)
		if remain <= 0 {
			return context.DeadlineExceeded
		}
		if d > remain {
			return context.DeadlineExceeded
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
