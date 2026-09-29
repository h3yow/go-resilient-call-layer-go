# Go 容错调用层（Resilient Call Layer）

面向多依赖调用的统一容错层，配合**可替换的本地传输实现**模拟下游服务，
让超时、重试、熔断、降级在不同调用路径下都有一致、可解释的结论。

## 1. 包结构

| 包 | 职责 |
| --- | --- |
| `pkg/relerr` | 下游错误分类原语（业务 / 瞬时 / 超时），独立成包以避免 `resilience` 与 `downstream` 循环依赖 |
| `pkg/downstream` | 可配置的本地传输实现：延迟、自身处理超时、瞬时错误、业务错误、前 N 次失败后恢复；精确记录副作用次数 |
| `pkg/resilience` | 容错编排：按依赖隔离的熔断器、指数退避+抖动重试、时间预算、降级，以及最终结果归类 |
| `cmd/demo` | 可运行的端到端演示，打印每条路径的结论与完整判定链 |

## 2. 熔断状态机（按依赖实例隔离）

每个下游实例（以 `Instance.Name()` 为键）持有**独立**的熔断器，
一路持续失败只会打开它自己的熔断，不能拖垮其他实例（见
`TestCaller_DependencyIsolation`）。

```text
                 连续失败 >= FailureThreshold
        CLOSED ───────────────────────────────► OPEN
          ▲                                        │
          │ 半开探测连续成功                        │ OpenDuration 冷却结束
          │ >= HalfOpenSuccessThreshold            │（下一次 Allow 惰性迁移，无后台协程）
          │                                        ▼
          └────────────────────────────────── HALF_OPEN
                  任一次探测失败立即回到 OPEN
```

- **CLOSED**：正常放行，累计连续失败数；任意成功清零。
- **OPEN**：一律快速拒绝（`OutcomeCircuitOpen`，`Attempts=0`，不触碰下游、不计副作用）。
- **HALF_OPEN**：只允许最多 `HalfOpenMaxConcurrent` 个探测调用**同时**在途，
  名额满后的并发请求直接拒绝，不会一股脑全打到刚恢复的下游。
  - 探测连续成功达到 `HalfOpenSuccessThreshold` → CLOSED；
  - 任一次探测失败 → 立即重新 OPEN。

### 代际（generation）机制：计数不重复、不乱串

每次状态迁移都会开启新的 `generation`。`Allow` 返回调用所属代际，
`OnSuccess/OnFailure` 只接受当前代际的上报；迁移前在途调用的迟到结果
一律按“代际过期”忽略。因此：

- 同一次物理调用的成功/失败在其所属代际内**恰好计数一次**；
- 半开失败重新 OPEN 后，另一个同批探测的迟到成功不会把熔断“救回”CLOSED；
- 高并发下放行计数、在途计数、失败计数保持一致
  （见 `TestBreaker_HighConcurrentTransitionsConsistent`、
  `TestCaller_HighConcurrencyCircuitCountsConsistent`）。

## 3. 重试与时间预算规则

### 是否重试（三个条件同时满足才重试）

1. 操作声明 `Operation.Retryable == true`——只有可安全重试（幂等/只读）的操作允许重试；
   不可重试操作首次失败即终局，归类 `NOT_RETRYABLE`。
2. 错误类别可重试：`TRANSIENT` / `TIMEOUT`；`BUSINESS`（业务错误）永不重试。
3. 重试次数未超过 `Retry.MaxAttempts`（含首次）。

### 退避与抖动

- 指数退避：`InitialBackoff * Multiplier^(failedAttempt-1)`，以 `MaxBackoff` 封顶；
- 对称抖动：实际等待在 `base*(1±JitterFraction)` 之间；
- 退避与抖动均为测试可注入（随机源/时钟），退避时长确定性可断言。

### 剩余时间预算（绝不硬拖已超时的调用）

- 本次调用预算取 `ctx` 截止时间与 `Config.Budget` 中的**更早者**。
- 每次尝试前先检查预算：已耗尽则立即终止（`Attempts` 不增加，下游不计数）。
- 失败后若要重试：先比较**剩余预算与退避时长**，剩余预算不足以覆盖退避时，
  立即归类 `TIMEOUT`，不再发起下一次调用。
- 退避等待期间预算到期，同样立即终止。
- 下游处理中预算到期：本地传输通过 `ctx` 立即返回超时错误，不会等满它配置的延迟。

### 结果归类（`Outcome`，六类失败/成功清晰区分）

| Outcome | 含义 |
| --- | --- |
| `SUCCESS` | 下游真实成功，`Degraded=false` |
| `BUSINESS_ERROR` | 下游业务失败，任何情况下不重试 |
| `NOT_RETRYABLE` | 操作不可安全重试，首次失败即终局 |
| `RETRY_EXHAUSTED` | 可重试错误反复出现，尝试次数耗尽（此时预算仍有余量） |
| `TIMEOUT` | 时间预算耗尽（尝试前、尝试中、退避前、退避中均可能） |
| `CIRCUIT_OPEN` | 熔断打开或半开探测名额已满，直接拒绝 |
| `FALLBACK` | 提供了降级函数且降级成功；**`Degraded=true`**，调用方一眼可辨非真实成功 |

`CallResult.Reason` 以 ` | ` 串联每一步判定依据；日志同步打印每次
Allow 判定、失败类别、预算比较与状态迁移。

## 4. 关键配置项

```go
resilience.Config{
    Breaker: resilience.BreakerConfig{
        FailureThreshold:         5,              // CLOSED 连续失败阈值
        OpenDuration:             5 * time.Second,// OPEN 冷却时长
        HalfOpenMaxConcurrent:    2,              // 半开同时在途探测上限
        HalfOpenSuccessThreshold: 2,              // 半开连续成功恢复阈值
    },
    Retry: resilience.RetryConfig{
        MaxAttempts:    3,                 // 最大尝试次数（含首次）
        InitialBackoff: 10 * time.Millisecond,
        MaxBackoff:     200 * time.Millisecond,
        Multiplier:     2.0,
        JitterFraction: 0.2,               // [0,1] 对称抖动
    },
    Budget: 500 * time.Millisecond,        // 单次调用总预算；<=0 仅受 ctx 限制
}
```

下游实例行为按需替换：

```go
ins := downstream.NewInstance("profile-svc")
ins.Configure(downstream.Behavior{
    Latency:       20 * time.Millisecond, // 处理延迟
    Timeout:       10 * time.Millisecond, // 自身处理超时（>0 且 Latency 更长时生效）
    FailTransient: true,                  // 瞬时错误（可重试类别）
    BusinessError: "",                    // 业务错误（不可重试类别）
    FailNTimes:    2,                     // 仅前 N 次失败，之后恢复；0 表示持续失败
})
```

## 5. 验证方法

```bash
# 全部单测（含每次调用的判定依据与状态迁移日志，用 -v 查看）
go test ./... -count=1 -v

# 并发安全：竞态检测
go test ./... -race -count=1

# 端到端演示：六条路径的结论与判定链
go run ./cmd/demo
```

测试覆盖的重点场景：

- 熔断 CLOSED→OPEN→HALF_OPEN→CLOSED 全迁移、半开失败立即重开；
- 熔断切换期 200 并发：半开放行恰好等于上限、其余拒绝、计数一致；
- 编排层 100 并发持续失败：副作用次数 == 真实放行数，拒绝零副作用；
- 依赖隔离：坏实例 OPEN 时健康实例仍 SUCCESS；
- 预算边界：下游超时、重试中途耗尽、剩余预算 < 退避、ctx 预取消、ctx 比配置更紧；
- 可重试/不可重试操作混合、业务错误、降级路径；
- 断言副作用不重复（`Instance.CallCount()`）、状态计数不重复累加、四类失败原因可区分。
