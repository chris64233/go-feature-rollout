# go-feature-rollout

功能灰度规则服务：支持**多功能版本整组原子发布**、确定性分桶判定、前置依赖、幂等发布、快照回滚，以及自动保护暂停后的**分批恢复**（分批放量 + 观察窗口）。

开发环境：Go 1.23.0。

运行测试：

    go test ./...

## 核心概念

- **规则版本（RuleVersion）**：某个功能的一次规则定义，包含灰度百分比、明确包含/排除名单、对其他功能版本的前置依赖。发布后**不可修改**；同一功能只能有一个当前版本，版本号必须单调递增且不可复用。
- **发布组**：一次发布可包含多个功能的新版本，校验通过后**整组原子生效**——要么全部成功，要么全部失败，在线判定永远看不到只发布了一半的中间态。
- **快照（Snapshot）**：每次发布/回滚产生一份不可变的整组快照（单调递增的序列号）。在线判定沿依赖链读取的始终是**同一份快照**，不会混用新旧版本。
- **回滚**：重新激活一份历史一致快照（当前指针切回），历史本身不被修改，发布序列号继续单调递增。

## 判定语义

对 `Evaluate(feature, userID)`：

1. 沿依赖链递归判定：任一依赖功能不存在、当前版本低于依赖要求、或对该用户不放行，则整体拒绝；
2. 排除名单优先于一切；
3. 包含名单优先于百分比；
4. 最后按确定性分桶判定百分比。

分桶只依赖 `feature + userID`（FNV-1a → 0-99 号桶），与规则版本无关，因此：

- 同一用户面对同一规则版本，结果始终稳定；
- 百分比上调只扩大受众、下调只收缩受众，不会因版本切换重新洗牌。

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

// 回滚到历史快照（重新激活，不修改历史）
rb, err := s.Rollback("chg-1002", snapshotSeq)

// 查询
hist := s.History()                 // 发布历史（含回滚），序列号单调
cur := s.CurrentRule("search-v2")   // 当前版本
old, err := s.EvalAt(seq, "search-v2", "alice") // 历史快照回放

// 自动保护暂停后的分批恢复（详见下文）
plan, _ := s.StartRecovery("rcv-1", "search-v2", cur.Version,
    []featurerollout.RecoveryBatchInput{{Percentage: 10, ObserveCount: 100, FailureThreshold: 2}})
s.ReportObservation("rcv-1", featurerollout.ObservationInput{Batch: 0, Attempt: 0, Success: true})
s.AdvanceRecovery("rcv-1") // 只有上一批观察通过后才放量下一批
```

## 自动保护后的分批恢复

自动保护机制把某个版本拦下之后，恢复**不允许一次性把流量全部打开**，只能按恢复计划一批一批放量，每批都经过一个全新的观察窗口。

### 恢复计划

`StartRecovery(changeID, feature, version, batches)` 创建并立即启动计划，计划写明：

- **版本**：恢复针对的功能版本；恢复期间该功能发布/回滚到其他版本后，旧计划标记为 `superseded`，对旧计划的一切操作返回 `KindConflict`，旧闸门让位给新版本；
- **每批比例**：各批放量百分比必须严格递增，且第一批必须高于暂停时的安全比例；
- **观察数量**（`ObserveCount`）：该批放量后必须收齐多少个结果；
- **失败阈值**（`FailureThreshold`）：满足 `0 <= 阈值 < 观察数量`，窗口内失败数**超过**阈值即阻断（恰好等于阈值仍算通过）。

第一批在创建后立即放量并开窗；之后必须**显式推进**：

```go
plan, _ := s.StartRecovery("rcv-1001", "search-v2", 3, []featurerollout.RecoveryBatchInput{
    {Percentage: 10, ObserveCount: 100, FailureThreshold: 2},
    {Percentage: 30, ObserveCount: 200, FailureThreshold: 4},
    {Percentage: 100, ObserveCount: 500, FailureThreshold: 5},
})

// 观察窗口内逐个上报当批结果；窗口没收齐之前比例保持不变，不能开下一批
r, _ := s.ReportObservation("rcv-1001", featurerollout.ObservationInput{
    Batch: plan.CurrentBatch, Attempt: 0, Success: true,
})
// r.WindowClosed / r.WindowPassed / r.Blocked 反映本批结论

if r.WindowPassed {
    s.AdvanceRecovery("rcv-1001") // 只有上一批观察完成后才允许提高比例
}
```

### 状态机与并发语义

状态：`observing`（观察中）→ `ready`（本批通过、可推进）→ … → `completed`；
旁路状态：`paused`（人工暂停）、`blocked`（观察失败）、`cancelled`（取消）、`superseded`（版本漂移/被新计划取代）。

- **上一批没观察完不能开下一批**：`observing`/`paused` 状态下 `AdvanceRecovery` 返回 `KindConflict`；观察通过只进入 `ready`，比例停在当前安全值，必须显式推进才放量下一批。
- **暂停后从安全比例恢复**：`PauseRecovery` 立即作废未收齐的当前窗口（标记 `interrupted` 并记录原因）；`ResumeRecovery` 在同一批开一个**全新窗口**（`Attempt` 加 1，计数归零），从暂停时的安全比例重新放量。在 `ready` 状态暂停时，恢复会重新观察当前安全比例，而不是跳到下一批。中间再次暂停，下一次恢复始终从暂停时的安全比例开始。
- **旧批次结果不能推进新批次**：上报必须同时匹配"当前批次 + 当前开窗轮次（Attempt）"。旧批次、旧 Attempt、窗口关闭后才送达的结果一律返回 `KindConflict` 并计入该窗口的 `LateResults`，既不能推进新批次，也不会重复计数、重复放量。
- **失败归属于当批次**：窗口内失败数只计入该批该次开窗，不会被下一批恢复误用；观察失败时安全比例不前进，计划进入 `blocked`，本批实际比例记为观察前的安全比例。
- **取消保留已完成批次**：`CancelRecovery` 后已通过批次的证据完整保留，未收齐窗口标记 `interrupted`，剩余流量停在最后一个安全比例；之后可以从该安全比例重新提恢复计划（首批比例必须更高）。

### 放量闸门与判定

- 恢复期间 `Evaluate` 的百分比分桶受恢复闸门约束：`observing` 用当批比例，`ready/paused/blocked/cancelled/completed` 用安全比例；`EvalResult.Percentage` 返回实际生效比例。包含/排除名单与依赖判定不受闸门影响。
- 每次判定在同一读锁内取出"快照 + 全部恢复闸门"，沿依赖链求值期间看不到恢复状态交错迁移的中间态。
- 历史回放 `EvalAt` 不施加恢复闸门，仍按当时规则判定。
- 上报、暂停、恢复、推进、创建全部在同一把写锁内串行迁移，与发布/回滚/在线判定并发安全（`go test -race`）。

### 证据留存

`GetRecovery(changeID)` 返回完整审计证据，每批包含：计划比例/观察数量/失败阈值、**实际生效比例**（`ActualPercentage`，未通过的批次为观察前安全比例）、每个观察窗口的轮次、计入结果数、失败数、迟到结果数、结局（`open/passed/blocked/interrupted`）与**阻断原因**；计划级 `BlockReason` 记录暂停/阻断/取消/取代原因。

```go
s.PauseRecovery("rcv-1001", "operator hold")  // 人工暂停（幂等）
s.ResumeRecovery("rcv-1001")                 // 从安全比例重新开窗
s.CancelRecovery("rcv-1001", "rollback")     // 取消：停在最后安全比例（幂等）
pct, ok := s.EffectivePercentage("search-v2") // 当前实际放量比例
```

恢复操作按 `changeID` 幂等：同一请求重复提交（包括推进到任意阶段后重放）返回原计划当前状态，不会重新开窗或重复放量；同号提交不同批次内容或不同版本返回 `KindConflict`。

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
| `KindConflict` | 同一变更号提交了不同内容 |
| `KindNotFound` | 草拟等对象不存在 |

分批恢复复用同一套分类：恢复号不存在 → `KindNotFound`；批次参数非法（比例越界/非严格递增、观察数量 < 1、阈值越界、首批不高于安全比例）→ `KindParam`；旧批次/旧窗口结果、未观察完就推进、版本漂移、同功能已有进行中的恢复、同号异内容/异版本 → `KindConflict`。

## 并发保证

- 发布与回滚在写锁内串行执行，序列号严格单调；
- 校验与状态切换在同一临界区内完成，失败不留下任何部分状态；
- 判定在锁内取出当前不可变快照（及当前恢复闸门）后在锁外求值，与并发发布/回滚/恢复操作互不干扰；
- 分批恢复的上报、暂停、恢复、推进在同一写锁内串行迁移，旧批次结果不能推进新批次，也不会重复放量。

以上性质由 `service_test.go` / `recovery_test.go` 中的并发测试（`go test -race`）覆盖。
