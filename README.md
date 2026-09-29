# 多依赖容错调用层（Go）

面向多下游依赖调用的容错层：用**可替换的本地传输**模拟下游服务，在统一的
调用路径上提供 **超时控制、退避+抖动重试、按实例隔离的熔断器、可识别的降级**，
并让每一次判定都“可解释”（日志 + 决策链）。

- 模块路径：`github.com/h3yow/go-resilient-call-layer-go`
- 代码包：`resilience/`，无第三方依赖，仅用标准库。

## 快速开始

```go
c := resilience.NewClient()
c.Register(resilience.DependencyConfig{
    Name: "user-service",
    Breaker: resilience.BreakerConfig{
        FailureThreshold:      5,   // 连续 5 次失败打开
        OpenCooldown:          500 * time.Millisecond,
        HalfOpenMaxConcurrent: 2,   // 半开最多同时放 2 个探测
        HalfOpenSuccessNeeded: 2,   // 2 次探测成功后恢复关闭
    },
    Retry: resilience.RetryConfig{
        MaxAttempts:    3,
        InitialBackoff: 10 * time.Millisecond,
        MaxBackoff:     200 * time.Millisecond,
        Multiplier:     2,
        JitterFactor:   0.2, // 实际退避在 base*(1±0.2) 随机
    },
    PerAttemptTimeout: 200 * time.Millisecond, // 单次尝试超时
    Fallback: func(ctx context.Context, req *resilience.Request, cause error) (any, error) {
        return &resilience.FallbackResult{Value: "缓存兜底"}, nil
    },
})

tr := resilience.NewLocalTransport("user-service").
    Script(resilience.Fail(true, "抖动"), resilience.OK("真实数据"))

r := c.Call(ctx, tr, &resilience.Request{
    Dependency: "user-service", Operation: "GetUser", Retryable: true,
})
switch {
case r.Fallback:
    fv := r.Value.(*resilience.FallbackResult) // 一眼看出是降级，不是真实成功
    _ = fv.Cause                               // 触发降级的原始失败（带分类）
default:
    _ = r.Value // 真实成功
}
```

本地传输提供脚本化行为（无需起真实服务）：

- `OK(v)` / `OKAfter(d, v)`：立即/延迟成功；
- `Fail(retryable, msg)` / `FailAfter(...)`：返回可重试或不可重试的下游错误；
- `Timeout()`：一直卡到 ctx 超时；
- `Dynamic(fn)`：按请求/序号动态决定；
- `tr.Calls()` / `tr.Records()`：读取真实打到下游的次数与记录（用于断言副作用次数）。

## 熔断器状态机

熔断器**按依赖实例名隔离**（`Client` 内部每个 name 一个独立 `Breaker`），
实例 A 持续失败打开，绝不影响实例 B。

```
                连续失败 >= FailureThreshold
        CLOSED ───────────────────────────────► OPEN
          ▲                                        │
          │ 半开累计成功 >= HalfOpenSuccessNeeded   │ OpenCooldown 到期
          │                                        ▼
          └────────────────────────────────── HALF_OPEN
                  任一探测失败立即回到 OPEN
```

- **CLOSED（关闭）**：正常放行；连续失败计数在成功时清零，达到
  `FailureThreshold` 时迁到 OPEN。
- **OPEN（打开）**：`Allow` 立即返回 `ErrCircuitOpen`，**不会产生下游调用**；
  经过 `OpenCooldown` 后，下一个 `Allow` 触发迁移到 HALF_OPEN。
- **HALF_OPEN（半开）**：最多同时放行 `HalfOpenMaxConcurrent` 个探测请求，
  额外来的请求同样被快速拒绝（“不会一股脑全打过去”）；
  - 探测成功：成功数 +1，达到 `HalfOpenSuccessNeeded` 恢复 CLOSED；
  - 任一探测失败：立即回到 OPEN，重新计时冷却。

### 并发一致性保证

- 所有状态迁移在互斥锁内完成；`TotalRejected`/`TotalTrips` 等用原子计数。
- 每次放行返回一个 `permit`，其 `Complete(success)` 用 `sync.Once` 保证
  **同一次尝试无论被上报几次，失败统计/半开在途计数只生效一次**。
- permit 携带“代（epoch）”编号并在锁内快照：状态迁出半开时统一清算在途槽位，
  迟到的旧代结果被直接忽略；CLOSED 阶段在途期间若熔断已被其他请求打开，
  其迟到结果也不再触碰计数——因此切换瞬间的高并发不会出现计数错乱或重复累加。

## 重试与时间预算规则

调用主循环：`熔断 Allow → 发起一次尝试 → 解释结果 →（必要时）退避 → 重试 → 降级`。

1. **只有可安全重试的操作才重试**：请求必须显式 `Retryable=true`（查询/幂等更新），
   非幂等操作（扣费、下单等）置 `false`，即使下游错误自称可重试也只调用一次。
2. **只有可重试性质的失败才重试**：超时类（`context.DeadlineExceeded/Canceled`）
   或显式标记 `Retryable` 的 `*DownstreamError`；不可重试的业务失败立即终止。
3. **退避**：`min(InitialBackoff * Multiplier^(n-1), MaxBackoff)`，再叠加
   `±JitterFactor` 的均匀抖动。
4. **严格遵守剩余时间预算**：
   - 每轮尝试前先检查外层 ctx，预算耗尽不再发起新尝试；
   - 退避前若剩余预算不足以睡完退避，**立即**返回超时结论，不把已超时的调用硬拖着跑；
   - `PerAttemptTimeout > 0` 时，单次尝试预算 = 外层剩余预算 ∩ 单次超时。

## 结果分类（失败原因可区分）

`CallResult.FinalKind`（`CallError.Kind`）取值：

| 分类 | 含义 |
| --- | --- |
| `KindTimeout` | 时间预算内没有拿到结果（尝试超时/预算耗尽/退避时间不够） |
| `KindCircuitOpen` | 熔断器打开（含半开满额），请求被直接拒绝、零下游副作用 |
| `KindRetryExhausted` | 可重试错误反复出现，达到 `MaxAttempts` 仍失败 |
| `KindBusiness` | 不可重试的业务失败，或不可安全重试操作的确定性失败 |
| `KindFallback` | 主流程失败后返回了降级结果 |

降级约定：`CallResult.Fallback == true` 且 `Value` 一定是 `*FallbackResult`
（即使降级函数返回裸值也会被统一包装）；`FallbackResult.Cause` 保留触发降级的
原始 `*CallError`，调用方既能一眼识别降级，又能知道真实根因。

## 配置项

### `BreakerConfig`

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `FailureThreshold` | 5 | CLOSED 下连续失败多少次打开 |
| `OpenCooldown` | 500ms | OPEN 持续时间，到期后允许探测进入 HALF_OPEN |
| `HalfOpenMaxConcurrent` | 2 | 半开探测最大并发，超出即拒绝 |
| `HalfOpenSuccessNeeded` | 2 | 半开累计多少次成功恢复 CLOSED |

### `RetryConfig`

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `MaxAttempts` | 3 | 最大尝试次数（含首次），1 表示不重试 |
| `InitialBackoff` | 10ms | 首次重试基础退避 |
| `MaxBackoff` | 200ms | 退避上限 |
| `Multiplier` | 2 | 指数乘数（<=0 按 2 处理） |
| `JitterFactor` | 0.2 | 抖动比例，区间 [0,1] |

### `DependencyConfig`

`Name`（依赖实例标识）、`Breaker`、`Retry`、`PerAttemptTimeout`、`Fallback`。
零值字段用默认值补齐；未注册的依赖在首次调用时按默认策略即时创建独立熔断器。

## 可解释性

- `Client.SetLogger(t.Logf)` / `NewBreaker(...).SetLogger(...)` 可接入测试日志，
  日志会打印每次 Allow 判定、每次尝试成败、退避决策、每一次状态迁移及原因。
- `CallResult.Decisions` 按时间顺序保存判定依据字符串，测试可直接断言。
- `Breaker.Stats()` 返回状态、连续失败数、半开在途/成功数、累计拒绝/熔断次数快照。

## 验证方法

```bash
# 全量单测（含 -race 竞态检测）
go test ./... -race -count=1 -v

# 端到端示例（四种调用路径）
go test ./resilience/ -run ExampleClient_overview -v

# 静态检查
go vet ./...
```

测试覆盖：

- 熔断：打开→半开探测→恢复关闭、探测失败重开、成功清零、实例隔离、
  permit 幂等、切换窗口 100/128/200 并发下的放行上限与计数一致性；
- 重试/预算：指数退避与封顶、抖动区间、预算不足立即放弃、不硬拖、
  可重试与不可重试错误混合、不可重试操作零重复副作用；
- 降级：业务失败/熔断拒绝/超时触发降级，降级结果可识别、原因可区分；
- 副作用：用 `LocalTransport.Calls()/Records()` 断言调用次数精确、序号无重复。
