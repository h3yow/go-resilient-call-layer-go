package resilience

import "context"

// Request 是发往下游的请求。Retryable 由业务/传输层声明：
// 只有幂等、可安全重试的操作才允许重试。
type Request struct {
	Dependency string
	Operation  string
	Payload    any
	// Retryable 声明该操作是否可安全重试（如查询/幂等更新为 true，
	// 扣费、下单等非幂等操作必须为 false）。
	Retryable bool
}

// Response 是下游返回。Err 为下游错误（可通过 *DownstreamError 判断是否可重试）。
type Response struct {
	Value any
	Err   error
}

// DownstreamError 让下游错误显式声明自己是否可重试。
type DownstreamError struct {
	Op        string
	Msg       string
	Retryable bool
}

func (e *DownstreamError) Error() string {
	kind := "non-retryable"
	if e.Retryable {
		kind = "retryable"
	}
	return "downstream " + e.Op + " failed (" + kind + "): " + e.Msg
}

// IsRetryable 判断一个下游错误是否允许重试：
// 仅当错误是 *DownstreamError 且显式标记 Retryable 时才成立。
// context 超时/取消在调用层单独按超时处理，也属于可重试（时间预算允许时）。
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	var de *DownstreamError
	if asDownstream(err, &de) {
		return de.Retryable
	}
	return false
}

func asDownstream(err error, target **DownstreamError) bool {
	for err != nil {
		if de, ok := err.(*DownstreamError); ok {
			*target = de
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// Transport 是可替换的本地传输抽象，用本地实现模拟下游服务。
type Transport interface {
	Do(ctx context.Context, req *Request) *Response
}
