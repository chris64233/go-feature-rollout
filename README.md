# go-feature-rollout

功能灰度规则服务：支持**多功能版本整组原子发布**、确定性分桶判定、前置依赖、幂等发布与快照回滚。

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

## 有期限的人工受众覆盖

用于紧急放行或临时屏蔽**少量用户**。覆盖是带时间窗的人工决定（生效时间、
到期时间、原因、操作人），与灰度规则分离存储，按外部操作号幂等管理。

### 判定优先级

在线判定按以下顺序取第一个命中的结论，`EvalResult.Source` 说明最终命中来源：

1. `block_override`：屏蔽覆盖——最高优先级，连依赖失败也可以被显式确认；
2. `allow_override`：放行覆盖——优先于前置依赖与灰度规则；
3. `dependency`：前置依赖不存在、版本不足或对该用户拒绝；
4. `exclude_list`：规则排除名单；
5. `include_list`：规则包含名单；
6. `bucket`：确定性百分比分桶；
7. `feature_missing`：功能尚未发布。

命中覆盖时 `EvalResult.OverrideID` 为覆盖 ID，否则为空。

### 版本边界（覆盖不跨版本沿用）

- 创建覆盖时必须指定**当时的当前规则版本**（必须已经发布且与当前指针一致），
  同时记录该功能的“发布代际”；
- 该功能一旦**发布新版本或回滚导致当前版本变化**，旧覆盖在同一写临界区内
  立即作废（状态变为 `superseded`），新版本判定不再放行或屏蔽；
- 回滚后旧覆盖**不会复活**；如需在重新激活的版本上放行，必须重新创建覆盖；
- 覆盖只作用于在线判定（`Evaluate`），历史快照回放（`EvalAt`）不套用覆盖，
  审计看到的始终是规则本身。

### 时间窗与撤销

- 时间窗为 `[StartAt, EndAt)`：生效时刻（含）起命中，到期时刻（不含）立即失效，
  到期点放行绝不拖到下一次扫描；
- `SweepExpiredOverrides` 只是把已到期覆盖物化为 `expired` 状态，在线判定本身
  按同一时间视图检查到期，扫描/发布/判定并发时过期覆盖也不可能继续放行；
- 撤销（`revoked`）只影响**后续**判定；每次在线判定追加一条不可变 `Decision`
  记录（`Decisions()` 可查），撤销、过期、版本切换都不会改写历史判定；
- 同一功能+用户在同一发布版本（同一发布代际）至多有一条有效覆盖。

时间通过 `NewServiceWithClock(nowFunc)` 注入，测试可完全固定时间、无需真实等待。

覆盖接口：

```go
s := featurerollout.NewServiceWithClock(myClock) // 也可继续用 NewService()

// 创建（opID 幂等）
r, err := s.CreateOverride("ops-1001", featurerollout.OverrideInput{
    Feature: "search-v2", UserID: "alice", Version: 3,
    Kind: featurerollout.OverrideAllow, // 或 OverrideBlock
    StartAt: start, EndAt: start.Add(time.Hour),
    Reason: "紧急放行", Operator: "admin",
})

// 撤销（只影响后续判定）
s.RevokeOverride("ops-1002", featurerollout.RevokeOverrideInput{
    Feature: "search-v2", UserID: "alice", Version: 3})

// 延长到期时间（NewEnd 必须严格晚于当前 EndAt）
s.ExtendOverride("ops-1003", featurerollout.ExtendOverrideInput{
    Feature: "search-v2", UserID: "alice", Version: 3,
    NewEnd: start.Add(3 * time.Hour), Reason: "继续观察"})

s.SweepExpiredOverrides()                    // 物化过期状态
ov, _ := s.GetOverride(r.Override.ID)        // 单条查询（含历史覆盖）
s.ListOverrides("search-v2", "alice", 0)     // 某用户的覆盖历史
s.ActiveOverrides()                          // 当前生效窗口内的覆盖
for _, d := range s.Decisions() { ... }      // 不可变判定记录（含命中来源）
```

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
| `KindConflict` | 同一变更号提交了不同内容 |
| `KindNotFound` | 草拟等对象不存在 |

覆盖操作沿用同一错误分类：参数/时间窗非法为 `KindParam`，目标版本不是当前版本
为 `KindVersion`，功能未发布或覆盖不存在为 `KindNotFound`，同版本已有有效覆盖、
同一操作号重放内容变化为 `KindConflict`。

覆盖的创建/撤销/延长均按**外部操作号（opID）幂等**：

- 同一 opID 以相同内容重放（含相同操作类型）→ 返回首次结果，
  `OverrideResult.Replayed == true`，不产生新覆盖；
- 用户、功能、版本、时间、原因或操作人任一变化，或换了操作类型 → `KindConflict`。

## 并发保证

- 发布与回滚在写锁内串行执行，序列号严格单调；
- 校验与状态切换在同一临界区内完成，失败不留下任何部分状态；
- 判定在锁内取出当前不可变快照后在锁外求值，与并发发布/回滚互不干扰。
- 覆盖集合采用 copy-on-write 不可变快照，判定取出快照指针与统一时间视图后
  在锁外求值；覆盖到期扫描、版本切换与在线判定并发时，过期覆盖不会继续放行，
  迟到的旧版本操作（目标版本已不是当前指针）直接失败，不会覆盖当前发布指针。

以上性质由 `service_test.go` 中的并发测试（`go test -race`）覆盖。
