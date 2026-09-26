# go-feature-rollout

功能灰度发布服务：以**不可变规则版本**为单位管理功能开关，支持整组原子发布、
前置依赖、确定性百分比分桶、显式受众包含/排除、幂等发布与快照式回滚。

开发环境：Go 1.23.0。

运行测试：

    go test ./... -race

## 核心概念

| 概念 | 说明 |
|---|---|
| `VersionSpec` / `RuleVersion` | 一条功能规则版本。发布后不可修改；一个功能同一时刻只有一个当前版本。 |
| `Draft` | 待发布草稿，可反复修改（`UpdateDraft`）与校验（`ValidateDraft`），再由 `Publish` 整组发布。 |
| `ChangeID` | 外部变更号。发布/回滚请求按它幂等：同号同内容重放返回首次记录，同号异内容报 `ErrChangeConflict`。 |
| `ChangeRecord` | 一次成功发布/回滚的历史记录，只追加、永不修改，`Seq` 为严格单调的发布序号。 |
| 快照（snapshot） | 每次发布产生的全量"功能 -> 当前版本"不可变视图，是在线判定的读取单位。 |

## 规则语义

一条规则版本包含：

- **百分比** `Percentage`（0–100）：以 `(功能, 用户)` 的 SHA-256 哈希做确定性分桶。
  同一用户面对同一版本结果永远稳定；桶号与版本无关，因此百分比调大时新受众
  严格包含旧受众、调小时严格收缩（嵌套桶），放量不会把已有用户抖掉。
- **显式受众** `Audience.Include / Exclude`：排除优先于包含，包含优先于百分比。
- **前置依赖** `DependsOn`：本功能只在所列功能的当前版本对该用户也放行时才放行。

发布前校验（`ValidateDraft`，`Publish` 时以最新状态复检）：

- 参数：功能键/版本号非空、百分比在 `[0,100]`、同组功能不重复 —— `ErrInvalidParam`；
- 版本：版本号未被占用（不可变）—— `ErrVersionConflict`；
- 依赖：依赖的功能已发布或同组发布 —— `ErrDependencyNotFound`；
- 环：与已发布规则合图后依赖无环（三色 DFS）—— `ErrDependencyCycle`。

## 原子性与并发

- **整组原子**：一次 `Publish` 的所有版本要么全部生效、要么全部失败；
  失败不留任何历史记录，线上不可见中间态。
- **单快照判定**：`Evaluate` 进入时加载一次当前快照指针，整次判定
  （含沿依赖链的递归）只读这一份快照，绝不混用新旧版本。
- **并发模型**：写路径互斥锁串行化，序号严格单调；读路径无锁加载原子指针，
  发布与判定并发时判定要么看到旧快照整体、要么看到新快照整体。
- **回滚**：`Rollback(changeID, targetChangeID)` 不修改历史，而是把目标记录
  对应的整份历史一致快照重新激活为当前快照，并追加一条 `ChangeRollback` 记录，
  发布序号照常单调递增。

## API 一览

```go
s := featurerollout.NewService()

d := s.NewDraft([]featurerollout.VersionSpec{
    {Feature: "search", Version: "v2", Percentage: 20,
     Audience:  featurerollout.Audience{Include: []featurerollout.UserID{"vip"}},
     DependsOn: []featurerollout.FeatureKey{"login"}},
    {Feature: "login", Version: "v3", Percentage: 100},
})
if err := s.ValidateDraft(d.ID); err != nil { /* 分类错误 */ }

rec, err := s.Publish("chg-20260927-01", d.ID)   // 整组原子发布，按变更号幂等
ok, err := s.Evaluate("search", "user-42")        // 单快照在线判定
rb, err := s.Rollback("chg-20260927-02", rec.ChangeID) // 回滚 = 重新激活历史快照

hist := s.History()          // 发布历史（含回滚），按 Seq 升序
cur := s.CurrentVersions()   // 当前各功能的当前版本
```

错误分类（用 `errors.Is` 判定）：`ErrInvalidParam`（参数）、
`ErrDependencyNotFound`（依赖不存在）、`ErrDependencyCycle`（依赖成环）、
`ErrVersionConflict`（版本冲突）、`ErrChangeConflict`（幂等冲突）、
`ErrNotFound`（草稿/功能/记录不存在）。

## 代码结构

- `types.go` — 规则版本、草稿、发布记录等数据类型
- `bucket.go` — 确定性嵌套分桶
- `service.go` — 草拟、校验、原子发布、回滚、判定与历史
- `atomic.go` — 当前快照的原子指针
- `errors.go` — 分类错误哨兵
- `service_test.go` — 上述全部语义的自动化测试（含并发单调性、`-race` 通过）
