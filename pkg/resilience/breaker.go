package resilience

import (
	"sync"
	"time"
)

// BreakerState 熔断器状态。
type BreakerState int

const (
	// StateClosed 关闭：正常放行，统计连续失败。
	StateClosed BreakerState = iota
	// StateOpen 打开：直接拒绝所有调用。
	StateOpen
	// StateHalfOpen 半开：限流放行探测调用，依据探测结果恢复或重新打开。
	StateHalfOpen
)

// String 返回状态名。
func (s BreakerState) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateOpen:
		return "OPEN"
	case StateHalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}

// BreakerConfig 是单个依赖实例的熔断配置。
type BreakerConfig struct {
	// FailureThreshold Closed 状态下连续失败达到该阈值即打开熔断。
	FailureThreshold int
	// OpenDuration 打开状态的持续时间；超时后下一次 Allow 惰性进入半开。
	OpenDuration time.Duration
	// HalfOpenMaxConcurrent 半开状态允许同时放行的探测调用上限。
	HalfOpenMaxConcurrent int
	// HalfOpenSuccessThreshold 半开期间连续成功达到该阈值则恢复为 Closed。
	HalfOpenSuccessThreshold int
}

// DefaultBreakerConfig 返回默认熔断配置。
func DefaultBreakerConfig() BreakerConfig {
	return BreakerConfig{
		FailureThreshold:         5,
		OpenDuration:             5 * time.Second,
		HalfOpenMaxConcurrent:    2,
		HalfOpenSuccessThreshold: 2,
	}
}

func (c *BreakerConfig) normalize() {
	d := DefaultBreakerConfig()
	if c.FailureThreshold <= 0 {
		c.FailureThreshold = d.FailureThreshold
	}
	if c.OpenDuration <= 0 {
		c.OpenDuration = d.OpenDuration
	}
	if c.HalfOpenMaxConcurrent <= 0 {
		c.HalfOpenMaxConcurrent = d.HalfOpenMaxConcurrent
	}
	if c.HalfOpenSuccessThreshold <= 0 {
		c.HalfOpenSuccessThreshold = d.HalfOpenSuccessThreshold
	}
}

// StateSnapshot 是某一时刻熔断器状态的可观测快照（供测试与日志断言）。
type StateSnapshot struct {
	State           BreakerState
	Generation      int64
	ConsecutiveFail int
	HalfOpenActive  int
	HalfOpenSuccess int
	OpenedAt        time.Time
}

// Allowed 表示一次放行的判定结果。
type Allowed struct {
	Allowed    bool
	Generation int64
	// Reason 记录放行/拒绝的判定依据。
	Reason string
}

// Breaker 是按依赖实例隔离的熔断器。
// 每个下游实例持有独立的 Breaker，状态互不影响。
type Breaker interface {
	// Allow 在调用前申请放行；打开或半开名额耗尽时返回 Allowed=false。
	Allow(now time.Time) Allowed
	// OnSuccess 上报一次成功。generation 与当前代际不一致的上报视为过期，直接忽略，
	// 保证一次物理调用的结果在其所属代际内恰好计数一次，绝不跨状态重复累加。
	OnSuccess(now time.Time, generation int64)
	// OnFailure 上报一次失败，过期上报同样忽略。
	OnFailure(now time.Time, generation int64)
	// Snapshot 返回当前状态快照（会惰性推动 Open->HalfOpen 迁移）。
	Snapshot(now time.Time) StateSnapshot
}

type breaker struct {
	dependency string
	config     BreakerConfig
	log        LogFunc

	mu sync.Mutex
	// 以下字段均在 mu 保护下访问。
	state           BreakerState
	generation      int64
	closedFailures  int // Closed 下连续失败数，成功归零
	openedAt        time.Time
	halfOpenActive  int // HalfOpen 下在途探测调用数
	halfOpenSuccess int // HalfOpen 下连续探测成功数，失败归零
}

// NewBreaker 创建一个熔断器。
func NewBreaker(dependency string, cfg BreakerConfig, log LogFunc) Breaker {
	return newBreakerWithClock(dependency, cfg, log, time.Now)
}

// newBreakerWithClock 注入时钟，供确定性测试使用。
func newBreakerWithClock(dependency string, cfg BreakerConfig, log LogFunc, nowFn func() time.Time) *breaker {
	cfg.normalize()
	if log == nil {
		log = func(string, ...any) {}
	}
	return &breaker{
		dependency: dependency,
		config:     cfg,
		log:        log,
		state:      StateClosed,
	}
}

// maybeOpenToHalfOpenLocked 若 Open 已持续足够久，则惰性迁移到 HalfOpen。调用方持锁。
func (b *breaker) maybeOpenToHalfOpenLocked(now time.Time) {
	if b.state != StateOpen {
		return
	}
	if now.Sub(b.openedAt) < b.config.OpenDuration {
		return
	}
	old := b.state
	b.generation++ // 新的半开探测代际：此前在途调用的结果将被视为过期
	b.state = StateHalfOpen
	b.halfOpenActive = 0
	b.halfOpenSuccess = 0
	b.closedFailures = 0
	b.log("breaker[%s] 状态迁移 %s->%s: 打开冷却 %s 结束，开启探测代际 gen=%d（半开并发上限=%d，恢复阈值=%d）",
		b.dependency, old, StateHalfOpen, b.config.OpenDuration, b.generation,
		b.config.HalfOpenMaxConcurrent, b.config.HalfOpenSuccessThreshold)
}

func (b *breaker) Allow(now time.Time) Allowed {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.maybeOpenToHalfOpenLocked(now)

	switch b.state {
	case StateClosed:
		return Allowed{Allowed: true, Generation: b.generation,
			Reason: "熔断器关闭，直接放行"}
	case StateOpen:
		remain := b.config.OpenDuration - now.Sub(b.openedAt)
		return Allowed{Allowed: false, Generation: b.generation,
			Reason: "熔断器打开，拒绝调用（冷却剩余 " + remain.String() + "）"}
	default: // StateHalfOpen
		if b.halfOpenActive >= b.config.HalfOpenMaxConcurrent {
			return Allowed{Allowed: false, Generation: b.generation,
				Reason: "半开探测名额已满（" +
					itoa(b.halfOpenActive) + "/" + itoa(b.config.HalfOpenMaxConcurrent) +
					"），拒绝多余并发，避免一股脑全打过去"}
		}
		b.halfOpenActive++
		return Allowed{Allowed: true, Generation: b.generation,
			Reason: "半开放行探测（在途 " + itoa(b.halfOpenActive) +
				"/" + itoa(b.config.HalfOpenMaxConcurrent) + "）"}
	}
}

func (b *breaker) OnSuccess(now time.Time, generation int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if generation != b.generation {
		b.log("breaker[%s] 成功上报忽略: 代际过期 gen=%d != 当前 gen=%d（状态=%s），不重复计数",
			b.dependency, generation, b.generation, b.state)
		return
	}

	switch b.state {
	case StateClosed:
		if b.closedFailures != 0 {
			b.log("breaker[%s] CLOSED 成功: 连续失败 %d 清零", b.dependency, b.closedFailures)
		}
		b.closedFailures = 0
	case StateHalfOpen:
		if b.halfOpenActive > 0 {
			b.halfOpenActive--
		}
		b.halfOpenSuccess++
		b.log("breaker[%s] HALF_OPEN 探测成功: 连续成功 %d/%d，在途探测=%d，gen=%d",
			b.dependency, b.halfOpenSuccess, b.config.HalfOpenSuccessThreshold,
			b.halfOpenActive, generation)
		if b.halfOpenSuccess >= b.config.HalfOpenSuccessThreshold {
			b.transitionToLocked(StateClosed, now,
				"半开探测连续成功达到恢复阈值，熔断恢复关闭")
		}
	default:
		// Open 状态不应有在途调用；防御性忽略。
		b.log("breaker[%s] 成功上报异常: 当前状态 %s 无在途探测，忽略", b.dependency, b.state)
	}
}

func (b *breaker) OnFailure(now time.Time, generation int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if generation != b.generation {
		b.log("breaker[%s] 失败上报忽略: 代际过期 gen=%d != 当前 gen=%d（状态=%s），不重复计数",
			b.dependency, generation, b.generation, b.state)
		return
	}

	switch b.state {
	case StateClosed:
		b.closedFailures++
		if b.closedFailures >= b.config.FailureThreshold {
			b.transitionToLocked(StateOpen, now,
				"连续失败达到阈值 "+itoa(b.config.FailureThreshold)+"，熔断打开")
			return
		}
		b.log("breaker[%s] CLOSED 失败: 连续失败 %d/%d，gen=%d",
			b.dependency, b.closedFailures, b.config.FailureThreshold, generation)
	case StateHalfOpen:
		if b.halfOpenActive > 0 {
			b.halfOpenActive--
		}
		b.transitionToLocked(StateOpen, now,
			"半开探测失败，判定下游未恢复，立即重新打开；其余同代际在途探测结果将按过期忽略")
	default:
		b.log("breaker[%s] 失败上报异常: 当前状态 %s，忽略", b.dependency, b.state)
	}
}

// transitionToLocked 执行状态迁移并开启新代际。调用方持锁。
func (b *breaker) transitionToLocked(target BreakerState, now time.Time, reason string) {
	old := b.state
	b.state = target
	b.generation++
	switch target {
	case StateOpen:
		b.openedAt = now
		b.halfOpenActive = 0
		b.halfOpenSuccess = 0
	case StateClosed:
		b.closedFailures = 0
		b.halfOpenActive = 0
		b.halfOpenSuccess = 0
	case StateHalfOpen:
		b.halfOpenActive = 0
		b.halfOpenSuccess = 0
	}
	b.log("breaker[%s] 状态迁移 %s->%s: %s（新代际 gen=%d）",
		b.dependency, old, target, reason, b.generation)
}

func (b *breaker) Snapshot(now time.Time) StateSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maybeOpenToHalfOpenLocked(now)
	return StateSnapshot{
		State:           b.state,
		Generation:      b.generation,
		ConsecutiveFail: b.closedFailures,
		HalfOpenActive:  b.halfOpenActive,
		HalfOpenSuccess: b.halfOpenSuccess,
		OpenedAt:        b.openedAt,
	}
}

// itoa 避免为一条日志引入 strconv 的小型辅助。
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
