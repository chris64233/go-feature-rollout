# go-feature-rollout

功能灰度规则服务：支持**多功能版本整组原子发布**、确定性分桶判定、前置依赖、幂等发布与快照回滚。

此外支持**有期限的人工受众覆盖**（紧急放行 / 临时屏蔽少量用户），覆盖严格受发布版本约束，不会把旧版本的临时决定带到新版本。

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

1. **屏蔽覆盖**：命中绑定当前发布版本且在有效期内的屏蔽覆盖，直接拒绝（最高优先级，kill switch 即使依赖异常也生效）；
2. **放行覆盖**：命中有效放行覆盖则直接放行（紧急放行可越过依赖链、本功能名单与百分比）；
3. **前置依赖**：沿依赖链递归判定，任一依赖功能不存在、当前版本低于依赖要求、或对该用户不放行，则整体拒绝；
4. 排除名单；
5. 包含名单；
6. 最后按确定性分桶判定百分比。

结果 `EvalResult.Reason` 说明最终命中的规则来源：

| Reason | 含义 |
|---|---|
| `override_deny` / `override_allow` | 命中人工覆盖（同时在 `OverrideID` 给出覆盖 ID） |
| `dependency` | 前置依赖未满足 |
| `exclude` / `include` | 命中规则排除 / 包含名单 |
| `percentage` | 由确定性百分比分桶决定（放行或拒绝） |
| `no_rule` | 功能在当前快照中没有任何已发布规则 |

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
```

## 人工受众覆盖

覆盖包含：功能、用户、绑定的规则版本、效果（allow/deny）、生效时间、到期时间（排他边界）、原因和操作人。

```go
// 时间源可在测试中固定（默认 time.Now，UTC），判定与到期扫描共用同一时钟。
s.SetClock(func() time.Time { return fixedNow })

// 创建：Version 必须是该功能当前已发布版本；传 0 表示绑定创建时刻的当前版本。
r, err := s.CreateOverride("ops-1001", featurerollout.OverrideInput{
    Feature: "search-v2", UserID: "alice", Version: 1,
    Effect: featurerollout.OverrideAllow,
    Effective: start, Expires: start.Add(time.Hour),
    Reason: "incident hotfix", Operator: "admin-a",
})

s.RevokeOverride("ops-1002", r.OverrideID)         // 撤销，只影响后续判定
s.ExtendOverride("ops-1003", r.OverrideID, later)  // 延长到期时间（只能更晚）
s.SweepExpired()                                   // 到期扫描，返回本次停用的覆盖 ID
s.GetOverride(r.OverrideID)                        // 查询单条状态
s.ListOverrides("search-v2")                       // 该功能全部覆盖（含历史）
```

**版本边界（关键）**：

- 覆盖绑定"规则版本号 + 发布版本代际"。新版本发布后旧覆盖立即作废；**回滚也不会让旧覆盖复活**——回滚重新激活旧版本号时会产生新的代际，旧覆盖的代际已不匹配。
- 被版本切换作废的覆盖状态为 `superseded`，不沿用、不能延长；迟到的、针对旧版本的创建请求一律被拒绝，不能影响当前发布指针。
- 同一功能、同一用户、同一发布版本最多有一条有效覆盖；到期（`expired`）或撤销（`revoked`）后释放槽位，方可重建。
- 到期时间为排他边界：`now >= Expires` 即不再生效。判定路径本身即时检查到期，与扫描、发布并发时也不会放行过期覆盖；扫描只负责状态落库与槽位释放。
- 撤销只影响撤销之后的判定；`EvalAt` 历史回放永不套用任何覆盖，历史判定记录不可能被改写。

**操作幂等**：创建、撤销、延长都按外部操作号（opID）幂等。相同操作重放返回首次结果；创建时用户、功能、版本、效果或时间变化、撤销/延长作用于不同覆盖、延长到不同到期时间，均返回 `KindConflict`。失败的调用不占用操作号。

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

## 并发保证

- 发布与回滚在写锁内串行执行，序列号严格单调；
- 校验与状态切换在同一临界区内完成，失败不留下任何部分状态；
- 判定在锁内取出当前不可变快照后在锁外求值，与并发发布/回滚互不干扰。

- 在线判定在读锁内同时读取当前快照、覆盖索引与服务时钟，发布/回滚、到期扫描、撤销/延长与判定并发时不会读到错位状态；过期覆盖在临界时刻即失效，迟到的旧版本操作不能影响当前发布指针。

以上性质由 `service_test.go`、`override_test.go` 中的并发测试（`go test -race`）覆盖。
