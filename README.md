# go-feature-rollout

功能灰度规则服务：支持**多功能版本整组原子发布**、确定性分桶判定、前置依赖、幂等发布与快照回滚，以及基于线上结果的**自动暂停保护**。

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
```

## 结果上报与自动暂停

发布版本可携带保护配置 `GuardConfig`（随版本不可变）：

```go
res, _ := s.Publish("chg-2001", []featurerollout.RuleInput{
    {Feature: "search-v2", Version: 3, Percentage: 20,
        Guard: &featurerollout.GuardConfig{
            MinObservations: 100,  // 至少累计 100 次观察才做判定
            MaxFailureRatio: 0.05, // 失败比例严格超过 5% 即暂停
        }},
})
```

**上报规则**（`ReportResult(feature, version, reportID, failed)`）：

- 上报必须关联**实际生效的版本号**；统计按 `(feature, version)` 隔离，旧版本的结果不会混入新版本，新版本的统计从零开始；
- `reportID` 由上报方生成并保证唯一：同一 `reportID` 重复上报**不重复计数**，幂等返回已有统计；
- 上报给从未发布过的功能/版本会报错（`KindNotFound` / `KindVersion`）。

**暂停规则**：

- 每次计入新结果后检查：去重后的观察数 `>= MinObservations` 且失败比例**严格大于** `MaxFailureRatio` 时，自动暂停该版本；
- 暂停 = 生成一份新快照，该功能的当前版本回退到**上一稳定版本**（最高的未暂停旧版本；没有则功能整体下线），新判定立即按旧版本放行；历史快照不可变，既有判定可通过 `EvalAt` 原样回放；
- 每个版本**至多暂停一次**：暂停后的重复上报、重复 `CheckGuard` 检查不会再产生状态变化，通知回调（`SetPauseNotifier`）也至多触发一次；
- 与人工操作并发时按发布版本拒绝过期操作：被暂停版本必须仍是当前版本，否则（已人工回滚或重新发布）暂停被放弃；回滚到含已暂停版本的历史快照会被拒绝（`KindVersion`）。恢复放量需发布更新的版本；
- 每次暂停保存 `PauseRecord`：触发原因、统计快照（观察数、失败数、失败比例、上限配置）以及**全部被计入的 reportID 列表**，可通过 `PauseOf(feature)` / `Pauses()` 查询；`StatsOf(feature, version)` 可查看任意版本的实时统计。

```go
out, _ := s.ReportResult("search-v2", 3, "req-9f2c", true) // out.Paused 表示本次触发了暂停
pause, ok := s.PauseOf("search-v2") // pause.Reason / pause.Stats / pause.Seq
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

## 并发保证

- 发布与回滚在写锁内串行执行，序列号严格单调；
- 校验与状态切换在同一临界区内完成，失败不留下任何部分状态；
- 结果上报的计数、越线检查与自动暂停在同一临界区内原子完成，与并发的人工回滚/重新发布串行化，过期操作按发布版本被拒绝；
- 判定在锁内取出当前不可变快照后在锁外求值，与并发发布/回滚互不干扰。

以上性质由 `service_test.go` 中的并发测试（`go test -race`）覆盖。
