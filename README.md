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

## 并发保证

- 发布与回滚在写锁内串行执行，序列号严格单调；
- 校验与状态切换在同一临界区内完成，失败不留下任何部分状态；
- 判定在锁内取出当前不可变快照后在锁外求值，与并发发布/回滚互不干扰。

以上性质由 `service_test.go` 中的并发测试（`go test -race`）覆盖。
