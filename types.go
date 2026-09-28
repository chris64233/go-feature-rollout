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

	// 自动暂停守卫（仅对本版本生效，统计按版本隔离）。
	// MinObservations 为 0 表示不启用自动暂停；否则当观察数 >= MinObservations
	// 且失败比例严格大于 MaxFailureRate（取值 [0,1]）时自动暂停本版本。
	MinObservations int
	MaxFailureRate  float64
}

// RuleVersion 是已发布的规则版本。发布后不可修改，任何字段都不应被改写。
type RuleVersion struct {
	Feature    string
	Version    int
	Percentage int
	Include    map[string]struct{}
	Exclude    map[string]struct{}
	Deps       []Dependency

	// 自动暂停守卫配置。MinObservations == 0 表示不启用。
	MinObservations int
	MaxFailureRate  float64
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
	Kind       string // "publish"、"rollback" 或 "auto-pause"
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

// ResultReport 是一次线上结果上报。
//
// 结果必须关联调用方实际采用的判定版本：ExpectedSeq/ExpectedVersion 取自
// Evaluate 返回值。服务端只在"该快照中该功能确实是该版本"时才接受计数，
// 从而保证旧版本结果不会混入新版本，过期上报也不会污染当前版本统计。
type ResultReport struct {
	ResultID string // 上报唯一标识，重复上报不重复计数
	Feature  string
	// ExpectedVersion 是上报方判定时实际命中的规则版本（Evaluate.Version）。
	ExpectedVersion int
	// ExpectedSeq 是上报方判定时使用的快照序列号（Evaluate.Seq）；0 表示不校验。
	ExpectedSeq uint64
	Failure     bool // 本次结果是否失败
}

// ReportResult 是一次上报的处理结果。
type ReportResult struct {
	// Accepted 表示本次上报是否被计入（重复上报为 false）。
	Accepted bool
	// Paused 表示本次上报是否触发了自动暂停。
	Paused bool
	// Stats 是计入（或重复上报命中的已有记录）后该版本的最新累计统计。
	Stats StatsSnapshot
	// PauseEvent 在 Paused 为 true 时返回触发的暂停事件；其余情况为 nil。
	PauseEvent *PauseEvent
}

// StatsSnapshot 是某个功能某个版本在某一时刻的统计快照。
// 计数结果由 ResultIDs/FailureIDs 两个集合严格决定（Observed == 两集合大小之和）。
type StatsSnapshot struct {
	Feature     string
	Version     int
	Observed    int     // 去重后的累计观察数
	Failures    int     // 去重后的累计失败数
	FailureRate float64 // Failures / Observed；Observed 为 0 时为 0

	MinObservations int     // 该版本配置的最少观察数量
	MaxFailureRate  float64 // 该版本配置的失败比例上限
}

// PauseEvent 记录一次自动暂停的完整依据，永久保留、不可修改。
type PauseEvent struct {
	Feature       string
	Version       int           // 被暂停的版本
	PausedAtSeq   uint64        // 暂停后新当前快照的序列号
	PreviousSeq   uint64        // 触发暂停时当前快照的序列号（乐观并发基准）
	RevertedTo    int           // 暂停后回退到的稳定版本（0 表示该功能下线）
	Reason        string        // 人类可读的越线原因
	Snapshot      StatsSnapshot // 触发瞬间的统计快照
	TriggerResult ResultReport  // 触发本次暂停的那次上报
	ResultIDs     []string      // 被计入统计的全部结果 ID（成功+失败），排序
	FailureIDs    []string      // 其中失败结果的 ID，排序
	Time          time.Time
}
