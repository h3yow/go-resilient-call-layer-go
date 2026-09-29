package resilience

import "github.com/h3yow/go-resilient-call-layer-go/pkg/relerr"

// LogFunc 用于在关键判定点输出可解释的判定依据与状态迁移。
type LogFunc func(format string, args ...any)

// Outcome 是一次容错调用的最终归类结果。
type Outcome int

const (
	// OutcomeUnknown 未设置（占位）。
	OutcomeUnknown Outcome = iota
	// OutcomeSuccess 下游真实成功。
	OutcomeSuccess
	// OutcomeBusinessError 下游明确返回的业务失败（不可重试）。
	OutcomeBusinessError
	// OutcomeTimeout 时间预算耗尽（含等待退避时预算已用尽）。
	OutcomeTimeout
	// OutcomeRetryExhausted 可重试错误反复出现，重试次数耗尽。
	OutcomeRetryExhausted
	// OutcomeCircuitOpen 熔断器打开（含半开探测名额已满）直接拒绝。
	OutcomeCircuitOpen
	// OutcomeNotRetryable 操作不允许重试，首次尝试即失败。
	OutcomeNotRetryable
	// OutcomeFallback 真实调用失败，使用了降级结果。
	OutcomeFallback
)

// String 返回结果的可读名称。
func (o Outcome) String() string {
	switch o {
	case OutcomeSuccess:
		return "SUCCESS"
	case OutcomeBusinessError:
		return "BUSINESS_ERROR"
	case OutcomeTimeout:
		return "TIMEOUT"
	case OutcomeRetryExhausted:
		return "RETRY_EXHAUSTED"
	case OutcomeCircuitOpen:
		return "CIRCUIT_OPEN"
	case OutcomeNotRetryable:
		return "NOT_RETRYABLE"
	case OutcomeFallback:
		return "FALLBACK"
	default:
		return "UNKNOWN"
	}
}

// 错误分类原语统一收敛在 relerr 包，这里以别名/透传方式对外暴露，
// 调用方只依赖 resilience 即可。

// ErrorKind 描述下游错误的性质。
type ErrorKind = relerr.ErrorKind

// 错误类别常量。
const (
	KindBusiness  = relerr.KindBusiness
	KindTransient = relerr.KindTransient
	KindTimeout   = relerr.KindTimeout
)

// ClassifiedError 是带错误类别的错误类型。
type ClassifiedError = relerr.ClassifiedError

// NewBusinessError 构造业务错误：任何情况下都不重试。
var NewBusinessError = relerr.NewBusinessError

// NewTransientError 构造瞬时错误：对可安全重试的操作允许重试。
var NewTransientError = relerr.NewTransientError

// NewTimeoutError 构造超时错误：对可安全重试的操作允许重试。
var NewTimeoutError = relerr.NewTimeoutError

// ErrCircuitOpen 是熔断器打开（或半开探测名额已满）时的哨兵错误。
var ErrCircuitOpen = relerr.ErrCircuitOpen

// IsTimeout 判断 error 是否为超时类错误。
var IsTimeout = relerr.IsTimeout

// ErrorKindOf 提取错误类别；未知错误按瞬时错误处理。
var ErrorKindOf = relerr.KindOf

// isRetryableKind 判断错误类别是否属于“可安全重试”的类别。
func isRetryableKind(kind ErrorKind) bool { return relerr.IsRetryableKind(kind) }
