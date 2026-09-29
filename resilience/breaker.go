package resilience

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// BreakerState 熔断器状态：关闭 -> 打开 -> 半开 -> 关闭/打开。
type BreakerState int

const (
	StateClosed BreakerState = iota
	StateOpen
	StateHalfOpen
)

// String 返回状态中文名，便于日志解释。
func (s BreakerState) String() string {
	switch s {
	case StateOpen:
		return "OPEN(打开)"
	case StateHalfOpen:
		return "HALF_OPEN(半开)"
	default:
		return "CLOSED(关闭)"
	}
}

// BreakerConfig 熔断器配置。
type BreakerConfig struct {
	// FailureThreshold 关闭态下连续失败达到该次数即打开。
	FailureThreshold int
	// OpenCooldown 打开态持续时间，之后允许探测进入半开。
	OpenCooldown time.Duration
	// HalfOpenMaxConcurrent 半开态允许同时放行的探测请求上限。
	HalfOpenMaxConcurrent int
	// HalfOpenSuccessNeeded 半开态累计多少次成功后恢复关闭。
	HalfOpenSuccessNeeded int
}

// DefaultBreakerConfig 一组偏演示用的默认值。
func DefaultBreakerConfig() BreakerConfig {
	return BreakerConfig{
		FailureThreshold:      5,
		OpenCooldown:          500 * time.Millisecond,
		HalfOpenMaxConcurrent: 2,
		HalfOpenSuccessNeeded: 2,
	}
}

// withDefaults 用默认值补齐零值字段。
func (c BreakerConfig) withDefaults() BreakerConfig {
	d := DefaultBreakerConfig()
	if c.FailureThreshold <= 0 {
		c.FailureThreshold = d.FailureThreshold
	}
	if c.OpenCooldown <= 0 {
		c.OpenCooldown = d.OpenCooldown
	}
	if c.HalfOpenMaxConcurrent <= 0 {
		c.HalfOpenMaxConcurrent = d.HalfOpenMaxConcurrent
	}
	if c.HalfOpenSuccessNeeded <= 0 {
		c.HalfOpenSuccessNeeded = d.HalfOpenSuccessNeeded
	}
	return c
}

type permitPhase int

const (
	phaseClosed permitPhase = iota
	phaseHalfOpen
)

// permit 表示一次被放行的调用许可；Complete 用 sync.Once 保证幂等：
// 同一次尝试无论 Complete 被调用几次，失败统计/半开放行计数只生效一次。
type permit struct {
	breaker *Breaker
	phase   permitPhase
	// epoch 是半开“代”编号：状态从半开迁走后，迟到的旧 permit 自动失效。
	epoch uint64
	once  sync.Once
}

// Complete 上报本次被放行调用的结果。幂等。
func (p *permit) Complete(success bool) {
	p.once.Do(func() { p.breaker.report(p, success) })
}

// Breaker 单个下游实例的熔断器，按依赖维度隔离，互不影响。
type Breaker struct {
	name string
	cfg  BreakerConfig

	logMu sync.RWMutex
	logf  func(format string, args ...any)

	mu sync.Mutex
	// 以下字段均在 mu 保护下读写。
	state               BreakerState
	consecutiveFailures int
	openedAt            time.Time
	halfOpenInFlight    int
	halfOpenSuccesses   int
	// epoch 每次进入半开 +1；半开迁出时也 +1，使在途 permit 失效。
	epoch uint64

	// 计数用原子量，快照读取无需抢锁，且并发下不错乱。
	totalRejected int64
	totalTrips    int64
}

// NewBreaker 创建命名熔断器。
func NewBreaker(name string, cfg BreakerConfig) *Breaker {
	return &Breaker{name: name, cfg: cfg.withDefaults(), logf: func(string, ...any) {}}
}

func (b *Breaker) doLog(f string, args ...any) {
	b.logMu.RLock()
	lf := b.logf
	b.logMu.RUnlock()
	lf(f, args...)
}

// SetLogger 注入判定/迁移日志函数（如 t.Logf），并发安全。
func (b *Breaker) SetLogger(f func(format string, args ...any)) {
	if f == nil {
		return
	}
	b.logMu.Lock()
	b.logf = f
	b.logMu.Unlock()
}

// Name 返回熔断器所属依赖实例名。
func (b *Breaker) Name() string { return b.name }

// State 返回当前状态（加锁读取；打开态可能顺带完成冷却到期迁移）。
func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	b.maybeCooldownElapsedLocked()
	s := b.state
	b.mu.Unlock()
	return s
}

// BreakerStats 当前用于解释状态机的计数快照。
type BreakerStats struct {
	State               BreakerState
	ConsecutiveFailures int
	HalfOpenInFlight    int
	HalfOpenSuccesses   int
	OpenedAt            time.Time
	TotalRejected       int64
	TotalTrips          int64
}

// Stats 返回计数快照（互斥字段加锁，计数字段原子读）。
func (b *Breaker) Stats() BreakerStats {
	b.mu.Lock()
	b.maybeCooldownElapsedLocked()
	s := BreakerStats{
		State:               b.state,
		ConsecutiveFailures: b.consecutiveFailures,
		HalfOpenInFlight:    b.halfOpenInFlight,
		HalfOpenSuccesses:   b.halfOpenSuccesses,
		OpenedAt:            b.openedAt,
	}
	b.mu.Unlock()
	s.TotalRejected = atomic.LoadInt64(&b.totalRejected)
	s.TotalTrips = atomic.LoadInt64(&b.totalTrips)
	return s
}

// Allow 尝试获取一次调用许可；熔断打开（含半开放行已满）时返回 ErrCircuitOpen。
// 调用方必须在结束时调用 permit.Complete（permit 自身保证只生效一次）。
func (b *Breaker) Allow(_ context.Context) (*permit, error) {
	b.mu.Lock()
	ep := b.epoch
	switch b.state {
	case StateClosed:
		b.mu.Unlock()
		b.doLog("[breaker=%s] 放行(CLOSED)", b.name)
		// epoch 必须在持锁时快照，防止在途期间状态迁移后读到新代编号。
		return &permit{breaker: b, phase: phaseClosed, epoch: ep}, nil

	case StateOpen:
		b.maybeCooldownElapsedLocked()
		if b.state == StateOpen {
			b.mu.Unlock()
			atomic.AddInt64(&b.totalRejected, 1)
			b.doLog("[breaker=%s] 拒绝(OPEN 冷却未到，openedAt=%s 前)", b.name, b.cfg.OpenCooldown)
			return nil, ErrCircuitOpen
		}
		// 已迁入半开，继续走半开分派。
	}

	// HALF_OPEN（含刚从 OPEN 自动迁入）。
	if b.halfOpenInFlight >= b.cfg.HalfOpenMaxConcurrent {
		b.mu.Unlock()
		atomic.AddInt64(&b.totalRejected, 1)
		b.doLog("[breaker=%s] 拒绝(HALF_OPEN 探测并发已达上限 %d)", b.name, b.cfg.HalfOpenMaxConcurrent)
		return nil, ErrCircuitOpen
	}
	b.halfOpenInFlight++
	ep = b.epoch
	b.mu.Unlock()
	b.doLog("[breaker=%s] 放行(HALF_OPEN 探测，在途=%d/%d)",
		b.name, b.unsafeInFlight(), b.cfg.HalfOpenMaxConcurrent)
	return &permit{breaker: b, phase: phaseHalfOpen, epoch: ep}, nil
}

// unsafeInFlight 仅用于日志，调用时未持锁（读到的是近似值，日志可接受）。
func (b *Breaker) unsafeInFlight() int {
	b.mu.Lock()
	n := b.halfOpenInFlight
	b.mu.Unlock()
	return n
}

// maybeCooldownElapsedLocked 打开态冷却到期则迁入半开（调用方持锁）。
func (b *Breaker) maybeCooldownElapsedLocked() {
	if b.state == StateOpen && time.Since(b.openedAt) >= b.cfg.OpenCooldown {
		b.toHalfOpenLocked()
	}
}

func (b *Breaker) toHalfOpenLocked() {
	b.state = StateHalfOpen
	b.halfOpenInFlight = 0
	b.halfOpenSuccesses = 0
	b.epoch++
	b.doLog("[breaker=%s] 状态迁移 OPEN -> HALF_OPEN（冷却 %s 到期，探测并发上限=%d，需连续成功=%d）",
		b.name, b.cfg.OpenCooldown, b.cfg.HalfOpenMaxConcurrent, b.cfg.HalfOpenSuccessNeeded)
}

func (b *Breaker) toOpenLocked(reason string) {
	b.state = StateOpen
	b.openedAt = time.Now()
	b.halfOpenInFlight = 0
	b.halfOpenSuccesses = 0
	b.epoch++
	atomic.AddInt64(&b.totalTrips, 1)
	b.doLog("[breaker=%s] 状态迁移 -> OPEN（原因=%s，第 %d 次熔断，冷却 %s）",
		b.name, reason, atomic.LoadInt64(&b.totalTrips), b.cfg.OpenCooldown)
}

func (b *Breaker) toClosedLocked(reason string) {
	b.state = StateClosed
	b.consecutiveFailures = 0
	b.halfOpenInFlight = 0
	b.halfOpenSuccesses = 0
	b.epoch++
	b.doLog("[breaker=%s] 状态迁移 HALF_OPEN -> CLOSED（原因=%s）", b.name, reason)
}

// report 处理一次 permit 的最终结果，所有状态迁移集中在这里，持锁完成。
func (b *Breaker) report(p *permit, success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 迟到的旧代 permit：其槽位在状态迁出时已统一清算，这里绝不能重复计数。
	if p.epoch != b.epoch {
		b.doLog("[breaker=%s] 忽略过期探测结果(success=%v，状态已是 %s)", b.name, success, b.state)
		return
	}

	switch p.phase {
	case phaseClosed:
		// 双重保险：代编号不一致，或状态已不是 CLOSED（本 permit 在途期间
		// 熔断已被其他请求打开），本次迟到结果都不得再触碰计数/状态。
		if b.state != StateClosed {
			b.doLog("[breaker=%s] 忽略 CLOSED 阶段迟到结果(success=%v，状态已迁移为 %s)",
				b.name, success, b.state)
			return
		}
		if success {
			if b.consecutiveFailures != 0 {
				b.doLog("[breaker=%s] CLOSED 成功，连续失败计数 %d -> 0", b.name, b.consecutiveFailures)
			}
			b.consecutiveFailures = 0
			return
		}
		b.consecutiveFailures++
		b.doLog("[breaker=%s] CLOSED 失败，连续失败=%d/%d",
			b.name, b.consecutiveFailures, b.cfg.FailureThreshold)
		if b.consecutiveFailures >= b.cfg.FailureThreshold {
			b.toOpenLocked("CLOSED 连续失败达阈值")
		}

	case phaseHalfOpen:
		// 只有同代 permit 才会计入在途释放，保证不会重复累加。
		b.halfOpenInFlight--
		if b.halfOpenInFlight < 0 {
			b.halfOpenInFlight = 0
		}
		if !success {
			b.doLog("[breaker=%s] HALF_OPEN 探测失败，立即重新 OPEN", b.name)
			b.toOpenLocked("HALF_OPEN 探测失败")
			return
		}
		b.halfOpenSuccesses++
		b.doLog("[breaker=%s] HALF_OPEN 探测成功，成功数=%d/%d，剩余在途=%d",
			b.name, b.halfOpenSuccesses, b.cfg.HalfOpenSuccessNeeded, b.halfOpenInFlight)
		if b.halfOpenSuccesses >= b.cfg.HalfOpenSuccessNeeded {
			b.toClosedLocked("HALF_OPEN 探测成功数达标")
		}
	}
}
