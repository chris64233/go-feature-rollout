package featurerollout

import "time"

// Dependency 表达对另一个功能某个已发布版本的前置依赖：
// 要求该功能已发布过 Version，且判定当时其当前版本不低于 Version 并对用户放行。
type Dependency struct {
	Feature string
	Version int
}

// RuleInput 是草拟/发布接口接收的规则描述。
type RuleInput struct {
	Feature    string       // 功能标识
	Version    int          // 规则版本号，同一功能内必须单调递增且不可复用
	Percentage int          // 灰度百分比 0-100，基于确定性分桶
	Include    []string     // 明确放行的用户（优先于百分比）
	Exclude    []string     // 明确排除的用户（优先于 Include）
	Deps       []Dependency // 前置依赖
}

// RuleVersion 是已发布的规则版本。发布后不可修改，任何字段都不应被改写。
type RuleVersion struct {
	Feature    string
	Version    int
	Percentage int
	Include    map[string]struct{}
	Exclude    map[string]struct{}
	Deps       []Dependency
}

// Snapshot 是一份整组一致的已发布快照：每个功能至多一个当前版本。
// 在线判定沿依赖链读取时只使用同一份 Snapshot，绝不混用新旧版本。
type Snapshot struct {
	Seq   uint64
	Rules map[string]*RuleVersion // feature -> 当前版本
}

// PublishRecord 是发布历史中的一条记录，序列号单调递增。
type PublishRecord struct {
	Seq        uint64
	ChangeID   string
	Kind       string // "publish" 或 "rollback"
	Features   []string
	RollbackTo uint64 // 仅回滚记录：重新激活的历史快照序列号
	Time       time.Time
}

// PublishResult 是发布/回滚成功后的结果，同一变更号重复发布返回相同结果。
type PublishResult struct {
	Seq      uint64
	ChangeID string
	Features []string
}

// Draft 是一次尚未发布的草拟。
type Draft struct {
	ID       string
	ChangeID string
	Rules    []RuleInput
	Hash     string // 规则内容的规范化哈希，用于幂等校验
}

// 分批恢复状态机：
//
//   - RecoveryObserving：某一批已放量，观察窗口尚未收齐；
//   - RecoveryReady：本批观察通过，允许（且仅允许）推进下一批；
//   - RecoveryPaused：人工暂停；恢复时以"已完成观察的安全比例"重新开窗；
//   - RecoveryBlocked：某批观察失败（失败数超阈值），永久停在当前安全比例；
//   - RecoveryCompleted：全部批次观察通过，放量到计划最终比例；
//   - RecoveryCancelled：计划被取消，保留已完成批次，剩余流量停在最后安全比例；
//   - RecoverySuperseded：版本漂移或被同功能同版本的新计划取代，不再控制放量。
const (
	RecoveryObserving  = "observing"
	RecoveryReady      = "ready"
	RecoveryPaused     = "paused"
	RecoveryBlocked    = "blocked"
	RecoveryCompleted  = "completed"
	RecoveryCancelled  = "cancelled"
	RecoverySuperseded = "superseded"
)

// 单个观察窗口的结局。
const (
	WindowOpen        = "open"        // 观察中
	WindowPassed      = "passed"      // 观察收齐且失败数未超阈值
	WindowBlocked     = "blocked"     // 观察收齐且失败数超过阈值
	WindowInterrupted = "interrupted" // 观察未收齐即被暂停/取消
)

// RecoveryBatchInput 描述恢复计划中的一批：放量到 Percentage，
// 收齐 ObserveCount 个结果且失败数不超过 FailureThreshold 才算观察通过。
type RecoveryBatchInput struct {
	Percentage       int
	ObserveCount     int
	FailureThreshold int
}

// ObservationInput 是一次恢复观察结果上报。Attempt 唯一标识一次"开窗放量"，
// 旧批次/旧尝试的结果会被拒绝，不能推进新批次，也不能重复计数。
type ObservationInput struct {
	Batch   int  // 批次下标，从 0 开始
	Attempt int  // 开窗轮次：每批首次放量为 0，暂停后恢复为 1、2……
	Success bool // true=成功结果，false=失败结果
}

// BatchWindow 保留一次开窗放量的全部证据。
type BatchWindow struct {
	Attempt     int
	StartedAt   time.Time
	ClosedAt    time.Time // 零值表示尚未关闭
	Result      string    // Window* 状态
	Observed    int       // 本次开窗计入的结果总数
	Failures    int       // 本次开窗计入的失败数
	LateResults int       // 窗口关闭/失效后才送达、被拒绝的旧结果数
	BlockReason string    // 阻断原因（观察失败 / 暂停 / 取消）
}

// RecoveryBatch 保留一批的计划值与全部观察轮次。
type RecoveryBatch struct {
	Percentage       int
	ObserveCount     int
	FailureThreshold int
	ActualPercentage int // 本批观察通过后实际生效的比例；未通过为 0
	Windows          []*BatchWindow
}

// RecoveryPlan 是一次分批恢复计划（按 ChangeID 幂等）。
type RecoveryPlan struct {
	ID             string
	ChangeID       string
	Feature        string
	Version        int    // 启动时该功能的当前规则版本；版本变化后一切操作报冲突
	PausedSafePct  int    // 启动前（被保护机制拦下时）的安全比例
	SafePercentage int    // 最近一个已完成观察批次的安全比例
	CurrentBatch   int    // 当前批次下标；终态时为最后处理过的批次
	Status         string // Recovery* 状态
	Batches        []*RecoveryBatch
	BlockReason    string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// ObservationReceipt 是一次观察结果上报的处理回执。
type ObservationReceipt struct {
	PlanID         string
	Feature        string
	Batch          int
	Attempt        int
	Observed       int // 当前开窗已计入的结果数
	Failures       int // 当前开窗已计入的失败数
	ObserveTarget  int // 本批要求的观察数量
	WindowClosed   bool
	WindowPassed   bool
	SafePercentage int // 观察通过后的新安全比例；未关闭为当前安全比例
	Blocked        bool
	BlockReason    string
	PlanStatus     string
}
