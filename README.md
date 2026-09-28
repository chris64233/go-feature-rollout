# go-feature-rollout

功能灰度规则服务：支持**多功能版本整组原子发布**、确定性分桶判定、前置依赖、幂等发布、快照回滚，以及**根据线上结果自动暂停放量（exposure guard）**。

开发环境：Go 1.23.0。

运行测试：

    go test ./...

## 核心概念

- **规则版本（RuleVersion）**：某个功能的一次规则定义，包含灰度百分比、明确包含/排除名单、对其他功能版本的前置依赖。发布后**不可修改**；同一功能只能有一个当前版本，版本号必须单调递增且不可复用。
- **发布组**：一次发布可包含多个功能的新版本，校验通过后**整组原子生效**——要么全部成功，要么全部失败，在线判定永远看不到只发布了一半的中间态。
- **快照（Snapshot）**：每次发布/回滚/自动暂停都产生一份不可变的整组快照，序列号单调递增。在线判定沿依赖链读取的始终是**同一份快照**，不会混用新旧版本。
- **回滚**：重新激活一份历史一致快照（当前指针切回），历史本身不被修改，发布序列号继续单调递增。
- **自动暂停守卫（Exposure Guard）**：每个发布版本可独立配置 `MinObservations`（最少观察数量）与 `MaxFailureRate`（失败比例上限，[0,1]）。线上结果按版本归集，观察量足够且失败比例越线时自动停止放量，切回上一稳定版本，并永久保存触发时的统计依据。

## 判定语义

对 `Evaluate(feature, userID)`：

1. 沿依赖链递归判定：任一依赖功能不存在、当前版本低于依赖要求、或对该用户不放行，则整体拒绝；
2. 排除名单优先于一切；
3. 包含名单优先于百分比；
4. 最后按确定性分桶判定百分比。

分桶只依赖 `feature + userID`（FNV-1a → 0-99 号桶），与规则版本无关，因此：

- 同一用户面对同一规则版本，结果始终稳定；
- 百分比上调只扩大受众、下调只收缩受众，不会因版本切换重新洗牌。

## 结果上报与自动暂停

### 1. 配置守卫

守卫参数是**版本级**配置，随版本发布后不可修改；`MinObservations == 0` 表示该版本不启用自动暂停：

```go
s.Publish("chg-guard", []featurerollout.RuleInput{{
    Feature: "search-v2", Version: 2, Percentage: 20,
    MinObservations: 100,   // 至少观察 100 个结果
    MaxFailureRate:  0.05,  // 失败比例上限 5%
}})
```

### 2. 上报必须关联实际采用的版本

上报时必须带回判定时实际拿到的 `Version` 与 `Seq`（即 `Evaluate` 的返回值）：

```go
r := s.Evaluate("search-v2", userID)
// 业务执行后，把结果关联到本次判定实际采用的版本/快照：
res, err := s.Report(featurerollout.ResultReport{
    ResultID:        "req-abc-123",   // 结果唯一 ID，用于去重
    Feature:         "search-v2",
    ExpectedVersion: r.Version,
    ExpectedSeq:     r.Seq,           // 0 表示只校验"当前"版本
    Failure:         failed,
})
```

归属与计数规则：

- **只计入实际服务该版本的快照**：`ExpectedSeq` 指向的快照中，该功能必须恰好服务 `ExpectedVersion`，否则返回 `KindStale`；快照不存在同样拒绝；
- **旧版本结果不会混入新版本**：统计严格按 `(feature, version)` 隔离，每个版本独立计数，新版本永远从 0 个观察开始；
- **重复上报不重复计数**：同一 `ResultID` 只计一次（重复提交返回 `Accepted=false` 的幂等结果）；同一 ID 改挂别的版本返回 `KindConflict`；
- 暂停后晚到的、属于旧快照的结果仍可正常计入对应旧版本，但不会再触发任何状态变化。

### 3. 越线与暂停

越线条件（两者同时满足）：

- 去重后观察数 `Observed >= MinObservations`（观察量足够）；
- 失败比例 `Failures / Observed` **严格大于** `MaxFailureRate`（恰好等于上限不算越线）。

越线时自动暂停：

- 产生新的当前快照（历史记录 `Kind = "auto-pause"`，序列号继续单调递增），把该功能指针切回**上一稳定版本**——版本号更小、且自身未曾被自动暂停过的最高已发布版本；不存在上一稳定版本时该功能整体下线（依赖它的功能判定随之 fail-closed）；
- **暂停后的新判定**立即使用稳定版本；
- **既有判定不被追溯修改**：历史快照、`EvalAt` 回放结果保持原样，暂停前计入的结果统计也原样保留。

### 4. 只暂停一次

- 同一版本**至多自动暂停一次**：暂停成功后，重复上报、重复 `CheckPause` 都只返回/留存既有事件，不再产生状态变化；
- 通知回调 `SetNotifier` 与实际暂停严格一一对应——只在暂停首次提交成功后、服务锁外调用一次，重复上报/重复检查/过期拒绝均不通知。

### 5. 与人工操作并发：拒绝过期操作

自动暂停采用乐观并发：提交时只有**当前快照仍服务于被暂停版本**才生效。若暂停提交前人工回滚或重新发布已经改变当前版本，这次暂停作为过期操作被拒绝（返回 `KindStale`，不产生事件、不通知），但合法归属的晚到上报仍计入正确版本的统计。

### 6. 暂停依据留存

每次暂停保存不可变的 `PauseEvent`，可通过 `PauseEvents()` / `PauseEventForVersion(feature, version)` 查询，包含：

- 触发瞬间的统计快照（观察数、失败数、失败比例、版本的守卫阈值）；
- `Reason`（人类可读的越线原因）与触发暂停的那次上报；
- `ResultIDs` / `FailureIDs`：**被计入统计的全部结果 ID 及其中失败的 ID**，计数与 ID 集合严格一致，可逐条说明哪些结果被计入；
- 暂停前/后快照序列号、回退到的稳定版本、发生时间。

也可通过 `Stats(feature, version)` 查询任意已发布版本当前的去重统计，通过 `CheckPause(feature)` 对当前版本执行一次显式复查。

## 接口

```go
s := featurerollout.NewService()

// 草拟 → 校验 → 原子发布（也可 Publish 一步完成）
id, _ := s.CreateDraft("chg-1001", []featurerollout.RuleInput{
    {Feature: "search-v2", Version: 1, Percentage: 20,
        Include: []string{"alice"},
        Deps:    []featurerollout.Dependency{{Feature: "user-service", Version: 3}}},
})
err := s.ValidateDraft(id)          // 发布前校验：依赖真实存在且不成环
res, err := s.PublishDraft(id)      // 整组原子生效，按 chg-1001 幂等

// 在线判定
r := s.Evaluate("search-v2", "alice")   // r.Allowed / r.Version / r.Seq

// 结果上报（关联判定时实际采用的版本与快照）
rr, err := s.Report(featurerollout.ResultReport{
    ResultID: "req-1", Feature: "search-v2",
    ExpectedVersion: r.Version, ExpectedSeq: r.Seq, Failure: true,
}) // rr.Paused / rr.Stats / rr.PauseEvent

// 回滚到历史快照（重新激活，不修改历史）
rb, err := s.Rollback("chg-1002", snapshotSeq)

// 查询
hist := s.History()                 // 发布历史（含回滚与自动暂停），序列号单调
cur := s.CurrentRule("search-v2")   // 当前版本
old, err := s.EvalAt(seq, "search-v2", "alice") // 历史快照回放
st, err := s.Stats("search-v2", 2)  // 某版本去重统计
events := s.PauseEvents()           // 所有自动暂停事件（含触发时统计依据）
```

## 幂等与冲突

发布/回滚按**外部变更号（changeID）幂等**：

- 同一 changeID 重复提交且内容一致 → 返回首次发布的结果，不产生新序列号；
- 同一 changeID 提交不同内容 → 返回 `KindConflict` 冲突错误。

## 错误分类

所有错误可通过 `KindOf(err)` 分类：

| Kind | 含义 |
|---|---|
| `KindParam` | 参数非法（空功能名、百分比越界、版本号 < 1、同组重复功能等） |
| `KindDependency` | 依赖的功能未发布、或依赖版本高于候选当前版本 |
| `KindCycle` | 依赖成环（含自依赖，可跨多次发布形成） |
| `KindVersion` | 版本已发布/未单调递增、回滚目标快照不存在等 |
| `KindConflict` | 同一变更号提交了不同内容，或同一 ResultID 改挂别的版本 |
| `KindNotFound` | 草拟、功能等对象不存在 |
| `KindStale` | 过期操作：上报归属的快照/版本已不服务、或暂停被并发的人工回滚/重新发布抢先 |

## 并发保证

- 发布、回滚与自动暂停在写锁内串行执行，序列号严格单调；
- 校验与状态切换在同一临界区内完成，失败不留下任何部分状态；
- 判定在锁内取出当前不可变快照后在锁外求值，与并发发布/回滚互不干扰；
- 结果上报的计数、越线判断与暂停提交在同一临界区内完成：同一版本至多暂停一次，
  暂停通知恰好发送一次；自动暂停与人工回滚/重新发布并发时，过期暂停被拒绝。

以上性质由 `service_test.go` 与 `guard_test.go` 中的并发测试（`go test -race`）覆盖。
