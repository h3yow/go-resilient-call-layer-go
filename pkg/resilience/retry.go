package resilience

import (
	"math/rand"
	"time"
)

// RetryConfig 是重试与退避配置。
type RetryConfig struct {
	// MaxAttempts 最大尝试次数（含首次），<=1 表示不重试。
	MaxAttempts int
	// InitialBackoff 首次重试前的基础退避。
	InitialBackoff time.Duration
	// MaxBackoff 单次退避上限（指数增长封顶）。
	MaxBackoff time.Duration
	// Multiplier 退避指数倍率（<1 时按 2 处理）。
	Multiplier float64
	// JitterFraction 抖动比例，取值 [0,1]：
	// 实际退避在 base*(1-JitterFraction) 与 base*(1+JitterFraction) 之间对称抖动。
	JitterFraction float64
}

// DefaultRetryConfig 返回默认重试配置。
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		Multiplier:     2,
		JitterFraction: 0.2,
	}
}

func (c *RetryConfig) normalize() {
	d := DefaultRetryConfig()
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = d.MaxAttempts
	}
	if c.InitialBackoff <= 0 {
		c.InitialBackoff = d.InitialBackoff
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = d.MaxBackoff
	}
	if c.Multiplier < 1 {
		c.Multiplier = d.Multiplier
	}
	if c.JitterFraction < 0 {
		c.JitterFraction = 0
	}
	if c.JitterFraction > 1 {
		c.JitterFraction = 1
	}
}

// RetryPolicy 决定“最多尝试几次”以及“失败后等待多久”。
// 是否真的重试还必须同时满足：操作声明可安全重试、错误类别可重试、
// 本次调用剩余时间预算足以覆盖退避与下一次尝试。
type RetryPolicy interface {
	// Config 返回归一化后的配置。
	Config() RetryConfig
	// NextBackoff 计算第 failedAttempt 次失败后的退避时长（failedAttempt 从 1 开始）。
	NextBackoff(failedAttempt int) time.Duration
}

type retryPolicy struct {
	config RetryConfig
	rng    *rand.Rand
}

// NewRetryPolicy 创建重试策略，使用独立随机源避免全局锁竞争。
func NewRetryPolicy(cfg RetryConfig) RetryPolicy {
	cfg.normalize()
	return &retryPolicy{
		config: cfg,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// newRetryPolicyWithRand 注入随机源，供确定性测试使用。
func newRetryPolicyWithRand(cfg RetryConfig, rng *rand.Rand) *retryPolicy {
	cfg.normalize()
	return &retryPolicy{config: cfg, rng: rng}
}

func (p *retryPolicy) Config() RetryConfig { return p.config }

func (p *retryPolicy) NextBackoff(failedAttempt int) time.Duration {
	cfg := p.config
	// failedAttempt=1 表示首次尝试失败，退避基数为 InitialBackoff。
	base := float64(cfg.InitialBackoff)
	for i := 1; i < failedAttempt; i++ {
		base *= cfg.Multiplier
	}
	max := float64(cfg.MaxBackoff)
	if base > max {
		base = max
	}
	// 对称抖动：[base*(1-j), base*(1+j)]。
	j := cfg.JitterFraction
	factor := 1 + (p.rng.Float64()*2-1)*j
	wait := time.Duration(base * factor)
	if wait < 0 {
		wait = 0
	}
	return wait
}
