package resilience

import "errors"

// FailureKind 对一次调用最终失败的原因进行分类，
// 让调用方可以明确区分：重试耗尽 / 业务失败 / 熔断拒绝 / 超时 / 降级。
type FailureKind int

const (
	// KindUnknown 未分类。
	KindUnknown FailureKind = iota
	// KindTimeout 在时间预算内没有拿到结果（含每次尝试的超时）。
	KindTimeout
	// KindCircuitOpen 熔断器打开，本次调用被直接拒绝。
	KindCircuitOpen
	// KindRetryExhausted 可重试错误反复出现，重试次数耗尽。
	KindRetryExhausted
	// KindBusiness 下游返回了明确的、不可重试的业务失败。
	KindBusiness
	// KindFallback 主流程失败，返回了降级结果（注意：这是可识别的“降级成功”）。
	KindFallback
)

// String 返回失败分类的可读名称，便于日志解释。
func (k FailureKind) String() string {
	switch k {
	case KindTimeout:
		return "timeout"
	case KindCircuitOpen:
		return "circuit_open"
	case KindRetryExhausted:
		return "retry_exhausted"
	case KindBusiness:
		return "business_error"
	case KindFallback:
		return "fallback"
	default:
		return "unknown"
	}
}

// CallError 是调用层返回的可解释错误：携带失败分类与根因。
type CallError struct {
	Kind FailureKind
	Err  error
}

func (e *CallError) Error() string {
	if e.Err != nil {
		return e.Kind.String() + ": " + e.Err.Error()
	}
	return e.Kind.String()
}

// Unwrap 支持 errors.Is/As 透传到底层根因。
func (e *CallError) Unwrap() error { return e.Err }

// 预置哨兵错误。
var (
	// ErrCircuitOpen 熔断打开，调用被拒绝（根因为 context 之外的本地哨兵）。
	ErrCircuitOpen = errors.New("circuit breaker open: request rejected")
	// ErrNoResult 下游没有返回任何结果。
	ErrNoResult = errors.New("downstream returned no result")
	// ErrNonRetryableRejected 操作不可重试，重试策略明确拒绝（首次就失败）。
	ErrNonRetryableRejected = errors.New("operation is not retryable")
)
