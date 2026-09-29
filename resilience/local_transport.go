package resilience

import (
	"context"
	"sync"
	"time"
)

// Behavior 描述一次模拟下游调用的行为。seq 从 0 开始，表示这是该实例第几次被调。
// 实现内部应尊重 ctx（超时/取消），以便真实模拟超时路径。
type Behavior func(ctx context.Context, req *Request, seq int) *Response

// CallRecord 记录一次实际打到“下游”的调用，用于断言副作用次数。
type CallRecord struct {
	Seq       int
	Operation string
	Payload   any
	At        time.Time
}

// LocalTransport 用预置脚本/行为函数在本地模拟单个下游实例。
// 每次 Do 都会真实计数（模拟一次下游副作用），因此测试中可以用 Calls()
// 断言：不可重试操作绝不会被第二次调用、熔断拒绝时根本不会产生调用。
type LocalTransport struct {
	name string

	mu        sync.Mutex
	calls     int64
	records   []CallRecord
	behaviors []Behavior
	// dynamic：若设置，则优先于脚本，每次调用实时决定结果。
	dynamic Behavior
}

// NewLocalTransport 创建一个命名的本地模拟传输（name 即依赖实例标识）。
func NewLocalTransport(name string) *LocalTransport {
	return &LocalTransport{name: name}
}

// Name 返回模拟的依赖实例名。
func (t *LocalTransport) Name() string { return t.name }

// Script 预置一段顺序脚本：第 1 次调用用第 1 个行为，依次消耗；
// 脚本耗尽后重复最后一个行为（没有脚本时返回 nil，调用层会按 NoResult 处理）。
func (t *LocalTransport) Script(behaviors ...Behavior) *LocalTransport {
	t.mu.Lock()
	t.behaviors = append([]Behavior(nil), behaviors...)
	t.mu.Unlock()
	return t
}

// Dynamic 设置动态行为函数（优先级高于脚本），便于按请求内容分支。
func (t *LocalTransport) Dynamic(b Behavior) *LocalTransport {
	t.mu.Lock()
	t.dynamic = b
	t.mu.Unlock()
	return t
}

// Calls 返回实际发生的下游调用次数（即副作用次数）。
func (t *LocalTransport) Calls() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.calls
}

// Records 返回调用记录的副本。
func (t *LocalTransport) Records() []CallRecord {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]CallRecord, len(t.records))
	copy(out, t.records)
	return out
}

func (t *LocalTransport) Do(ctx context.Context, req *Request) *Response {
	t.mu.Lock()
	seq := int(t.calls)
	t.calls++
	t.records = append(t.records, CallRecord{
		Seq: seq, Operation: req.Operation, Payload: req.Payload, At: time.Now(),
	})
	var b Behavior
	if t.dynamic != nil {
		b = t.dynamic
	} else if len(t.behaviors) > 0 {
		idx := seq
		if idx >= len(t.behaviors) {
			idx = len(t.behaviors) - 1
		}
		b = t.behaviors[idx]
	}
	t.mu.Unlock()

	if b == nil {
		return &Response{Err: ErrNoResult}
	}
	return b(ctx, req, seq)
}

// ---------- 内置行为构造器 ----------

// OK 立即返回成功值。
func OK(value any) Behavior {
	return func(ctx context.Context, req *Request, seq int) *Response {
		return &Response{Value: value}
	}
}

// OKAfter 延迟 delay 后成功；期间 ctx 取消则返回超时响应（不写成功值）。
func OKAfter(delay time.Duration, value any) Behavior {
	return func(ctx context.Context, req *Request, seq int) *Response {
		if !sleep(ctx, delay) {
			return timeoutResponse()
		}
		return &Response{Value: value}
	}
}

// Fail 立即返回一个下游错误。
func Fail(retryable bool, msg string) Behavior {
	return func(ctx context.Context, req *Request, seq int) *Response {
		return &Response{Err: &DownstreamError{Op: req.Operation, Msg: msg, Retryable: retryable}}
	}
}

// FailAfter 延迟后返回错误（ctx 先取消则变为超时）。
func FailAfter(delay time.Duration, retryable bool, msg string) Behavior {
	return func(ctx context.Context, req *Request, seq int) *Response {
		if !sleep(ctx, delay) {
			return timeoutResponse()
		}
		return &Response{Err: &DownstreamError{Op: req.Operation, Msg: msg, Retryable: retryable}}
	}
}

// Timeout 模拟下游卡住，直到 ctx 超时。
func Timeout() Behavior {
	return func(ctx context.Context, req *Request, seq int) *Response {
		<-ctx.Done()
		return timeoutResponse()
	}
}

func timeoutResponse() *Response {
	return &Response{Err: context.DeadlineExceeded}
}

// sleep 在 ctx 预算内等待；被取消/超时返回 false。
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return ctx.Err() == nil
	case <-ctx.Done():
		return false
	}
}
