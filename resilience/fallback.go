package resilience

import "context"

// FallbackFunc 降级函数：根据原请求与触发降级的错误返回一个兜底值。
type FallbackFunc func(ctx context.Context, req *Request, cause error) (any, error)

// FallbackResult 标记这是一个降级结果，调用方可一眼识别。
type FallbackResult struct {
	Value any
	Cause error
}
