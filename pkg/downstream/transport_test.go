package downstream

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/go-resilient-call-layer-go/pkg/relerr"
)

func TestTransport_AlreadyCanceledContextNoSideEffect(t *testing.T) {
	ins := NewInstance("dep-X")
	ins.Configure(Behavior{Latency: time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 进入时预算已耗尽

	_, err := ins.Do(ctx, Request{Key: "k"})
	if err == nil {
		t.Fatalf("已取消 ctx 必须返回错误")
	}
	if relerr.KindOf(err) != relerr.KindTimeout {
		t.Fatalf("应归类为超时，实际 %s", relerr.KindOf(err))
	}
	if ins.CallCount() != 0 {
		t.Fatalf("预算已耗尽的调用不得产生副作用计数，实际 %d", ins.CallCount())
	}
}

func TestTransport_DeadlineDuringLatencyTimesOut(t *testing.T) {
	ins := NewInstance("dep-X")
	ins.Configure(Behavior{Latency: 200 * time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := ins.Do(ctx, Request{Key: "k"})
	elapsed := time.Since(start)
	if relerr.KindOf(err) != relerr.KindTimeout {
		t.Fatalf("处理期间预算到期应返回超时，实际 %v", err)
	}
	if elapsed >= 150*time.Millisecond {
		t.Fatalf("超时后不得硬拖，应在预算附近返回，实际耗时 %s", elapsed)
	}
	// 真正开始处理过，所以副作用恰好1次（但不是成功）。
	if ins.CallCount() != 1 {
		t.Fatalf("开始处理后计数应为1，实际 %d", ins.CallCount())
	}
}

func TestTransport_SelfTimeoutConfig(t *testing.T) {
	ins := NewInstance("dep-X")
	ins.Configure(Behavior{Latency: 100 * time.Millisecond, Timeout: 10 * time.Millisecond})
	start := time.Now()
	_, err := ins.Do(context.Background(), Request{})
	if relerr.KindOf(err) != relerr.KindTimeout {
		t.Fatalf("自身处理超时应返回 KindTimeout，实际 %v", err)
	}
	if time.Since(start) >= 80*time.Millisecond {
		t.Fatalf("应在 Timeout 附近返回，不应等满 Latency")
	}
}

func TestTransport_FailNTimesThenRecover(t *testing.T) {
	ins := NewInstance("dep-X")
	ins.Configure(Behavior{FailTransient: true, FailNTimes: 2})

	for i := 1; i <= 2; i++ {
		if _, err := ins.Do(context.Background(), Request{}); relerr.KindOf(err) != relerr.KindTransient {
			t.Fatalf("第%d次应为瞬时错误", i)
		}
	}
	resp, err := ins.Do(context.Background(), Request{Key: "k3"})
	if err != nil || resp == nil || resp.Key != "k3" {
		t.Fatalf("失败配额耗尽后应恢复成功，resp=%v err=%v", resp, err)
	}
	if ins.CallCount() != 3 {
		t.Fatalf("副作用计数应为3，实际 %d", ins.CallCount())
	}
}

func TestTransport_BusinessErrorKind(t *testing.T) {
	ins := NewInstance("dep-X")
	ins.Configure(Behavior{BusinessError: "余额不足"})
	_, err := ins.Do(context.Background(), Request{})
	if relerr.KindOf(err) != relerr.KindBusiness {
		t.Fatalf("应归类业务错误，实际 %v", err)
	}
}

func TestTransport_ConcurrentCallCountConsistent(t *testing.T) {
	ins := NewInstance("dep-X")
	ins.Configure(Behavior{Latency: time.Millisecond})

	const n = 300
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, _ = ins.Do(context.Background(), Request{})
		}()
	}
	wg.Wait()
	if ins.CallCount() != n {
		t.Fatalf("并发下副作用计数必须恰好为 %d，实际 %d", n, ins.CallCount())
	}
}
