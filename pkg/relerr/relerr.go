// Package relerr 提供容错层与本地传输层共用的错误分类原语，
// 独立成包以避免 resilience 与 downstream 之间的循环依赖。
package relerr

import "fmt"

// ErrorKind 描述下游错误的性质。
type ErrorKind int

const (
	// KindBusiness 业务错误：下游正常应答但语义失败，不可重试。
	KindBusiness ErrorKind = iota
	// KindTransient 瞬时错误：可安全重试（仅限幂等/只读操作）。
	KindTransient
	// KindTimeout 下游超时：可安全重试（仅限幂等/只读操作）。
	KindTimeout
)

func (k ErrorKind) String() string {
	switch k {
	case KindBusiness:
		return "BUSINESS"
	case KindTransient:
		return "TRANSIENT"
	case KindTimeout:
		return "TIMEOUT"
	default:
		return "UNKNOWN"
	}
}

// ClassifiedError 是带错误类别的错误类型。
type ClassifiedError struct {
	kind    ErrorKind
	message string
}

// Error 实现 error。
func (e *ClassifiedError) Error() string { return e.message }

// Kind 返回错误类别。
func (e *ClassifiedError) Kind() ErrorKind { return e.kind }

// NewBusinessError 构造业务错误：语义明确失败，任何情况下都不重试。
func NewBusinessError(format string, args ...any) error {
	return &ClassifiedError{kind: KindBusiness, message: fmt.Sprintf(format, args...)}
}

// NewTransientError 构造瞬时错误：对可安全重试的操作允许重试。
func NewTransientError(format string, args ...any) error {
	return &ClassifiedError{kind: KindTransient, message: fmt.Sprintf(format, args...)}
}

// NewTimeoutError 构造超时错误：对可安全重试的操作允许重试。
func NewTimeoutError(format string, args ...any) error {
	return &ClassifiedError{kind: KindTimeout, message: fmt.Sprintf(format, args...)}
}

// ErrCircuitOpen 是熔断器打开（或半开探测名额已满）时的哨兵错误。
var ErrCircuitOpen = &ClassifiedError{kind: KindTransient, message: "circuit breaker open"}

// IsTimeout 判断 error 是否为超时类错误。
func IsTimeout(err error) bool {
	if err == nil {
		return false
	}
	if ce, ok := err.(*ClassifiedError); ok {
		return ce.kind == KindTimeout
	}
	return false
}

// KindOf 提取错误类别；未知错误按瞬时错误处理。
func KindOf(err error) ErrorKind {
	if err == nil {
		return KindTransient
	}
	if ce, ok := err.(*ClassifiedError); ok {
		return ce.kind
	}
	return KindTransient
}

// IsRetryableKind 判断错误类别是否属于“可安全重试”的类别。
func IsRetryableKind(kind ErrorKind) bool {
	return kind == KindTransient || kind == KindTimeout
}
