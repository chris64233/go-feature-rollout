package featurerollout

import (
	"fmt"
	"sort"
	"time"
)

// GuardConfig 是发布版本上的自动暂停保护配置：
// 当该版本累计观察数达到 MinObservations 且失败比例超过 MaxFailureRatio 时，
// 自动暂停该版本，新判定回退到同一功能的上一稳定（未暂停）版本。
type GuardConfig struct {
	MinObservations int     // 触发判定所需的最少观察数量，>= 1
	MaxFailureRatio float64 // 失败比例上限，(0,1]；严格超过才触发
}

// versionStats 是某个已发布版本的线上结果统计（内部状态）。
// 统计按 (feature, version) 隔离：旧版本的结果不会混入新版本。
type versionStats struct {
	total     int
	failures  int
	reportIDs map[string]struct{} // 已计入的上报号，用于去重
}

// StatsSnapshot 是触发暂停时刻的统计快照，说明哪些结果被计入了判定。
type StatsSnapshot struct {
	Total           int      // 已计入的观察总数（去重后）
	Failures        int      // 其中失败数
	FailureRatio    float64  // Failures / Total
	MinObservations int      // 触发时版本上配置的最少观察数
	MaxFailureRatio float64  // 触发时版本上配置的失败比例上限
	ReportIDs       []string // 全部被计入的上报号，升序
}

// PauseRecord 是一次自动暂停的完整记录。
type PauseRecord struct {
	Feature string
	Version int           // 被暂停的版本
	Seq     uint64        // 暂停产生的新快照序列号
	Reason  string        // 人可读的触发原因
	Stats   StatsSnapshot // 触发时刻的统计快照
	Time    time.Time
}

// ReportOutcome 是一次结果上报的结果。
type ReportOutcome struct {
	Counted  bool   // false 表示重复上报，未重复计数
	Total    int    // 该版本当前累计观察数
	Failures int    // 该版本当前累计失败数
	Paused   bool   // 本次上报触发了自动暂停
	PauseSeq uint64 // 暂停产生的新快照序列号（未触发时为 0）
}

// VersionStatsView 是某版本统计的只读视图。
type VersionStatsView struct {
	Feature      string
	Version      int
	Total        int
	Failures     int
	FailureRatio float64
	Paused       bool
}

// SetPauseNotifier 设置自动暂停通知回调。每次暂停至多调用一次，
// 在锁外调用；传 nil 清除。回调内不应再调用本服务的写方法。
func (s *Service) SetPauseNotifier(fn func(PauseRecord)) {
	s.mu.Lock()
	s.notifier = fn
	s.mu.Unlock()
}

// ReportResult 上报一次线上结果，关联到实际生效的版本 (feature, version)。
//
//   - reportID 是上报方生成的唯一标识：同一 reportID 重复上报不会重复计数，
//     幂等返回首次计入后的统计；
//   - 统计按版本隔离：上报给旧版本的结果只计入旧版本，不会混入新版本；
//   - 计入后若该版本统计越过保护上限，则在同一临界区内完成自动暂停
//     （每个版本至多暂停一次，重复上报/重复检查不会再次产生状态变化或通知）。
func (s *Service) ReportResult(feature string, version int, reportID string, failed bool) (ReportOutcome, error) {
	if reportID == "" {
		return ReportOutcome{}, paramErr("reportID is empty")
	}

	s.mu.Lock()
	vm, ok := s.versions[feature]
	if !ok {
		s.mu.Unlock()
		return ReportOutcome{}, notFoundErr("feature %q has no published version", feature)
	}
	if _, ok := vm[version]; !ok {
		s.mu.Unlock()
		return ReportOutcome{}, versionErr("feature %q version %d was never published", feature, version)
	}

	st := s.statsForLocked(feature, version)
	if _, dup := st.reportIDs[reportID]; dup {
		// 重复上报：不重复计数，也不会再次触发暂停。
		out := ReportOutcome{Counted: false, Total: st.total, Failures: st.failures}
		s.mu.Unlock()
		return out, nil
	}
	st.reportIDs[reportID] = struct{}{}
	st.total++
	if failed {
		st.failures++
	}

	out := ReportOutcome{Counted: true, Total: st.total, Failures: st.failures}
	pause := s.maybePauseLocked(feature, version)
	notifier := s.notifier
	if pause != nil {
		out.Paused = true
		out.PauseSeq = pause.Seq
	}
	s.mu.Unlock()

	if pause != nil && notifier != nil {
		notifier(*pause)
	}
	return out, nil
}

// CheckGuard 按当前统计对指定版本执行一次保护检查，越线则暂停。
// 与 ReportResult 中的检查共用同一幂等逻辑：已暂停的版本不会再次暂停。
func (s *Service) CheckGuard(feature string, version int) (*PauseRecord, error) {
	s.mu.Lock()
	vm, ok := s.versions[feature]
	if !ok {
		s.mu.Unlock()
		return nil, notFoundErr("feature %q has no published version", feature)
	}
	if _, ok := vm[version]; !ok {
		s.mu.Unlock()
		return nil, versionErr("feature %q version %d was never published", feature, version)
	}
	pause := s.maybePauseLocked(feature, version)
	notifier := s.notifier
	s.mu.Unlock()

	if pause != nil && notifier != nil {
		notifier(*pause)
	}
	return pause, nil
}

// maybePauseLocked 检查指定版本是否越过保护上限，越线则原子暂停。
// 返回 nil 表示未发生暂停。调用方必须持有写锁。
//
// 暂停的生效条件（全部满足，且每个版本至多生效一次）：
//   - 版本配置了 Guard，去重后的观察数 >= MinObservations；
//   - 失败比例严格大于 MaxFailureRatio；
//   - 该版本未被暂停过；
//   - 该版本仍是功能的当前版本——否则说明已发生人工回滚或重新发布，
//     本次暂停属于过期操作，直接放弃（通过发布版本拒绝过期操作）。
func (s *Service) maybePauseLocked(feature string, version int) *PauseRecord {
	rv := s.versions[feature][version]
	if rv == nil || rv.Guard == nil {
		return nil
	}
	st := s.stats[feature][version]
	if st == nil || st.total < rv.Guard.MinObservations {
		return nil
	}
	ratio := float64(st.failures) / float64(st.total)
	if ratio <= rv.Guard.MaxFailureRatio {
		return nil
	}
	if s.paused[feature][version] {
		return nil // 只能成功暂停一次
	}
	if s.current[feature] != version {
		return nil // 已非当前版本：过期操作，拒绝
	}

	// 回退目标：同一功能中小于被暂停版本的、最高的未暂停版本；没有则该功能整体下线。
	fallback := 0
	for v := range s.versions[feature] {
		if v < version && !s.paused[feature][v] && v > fallback {
			fallback = v
		}
	}

	nextRules := make(map[string]*RuleVersion, len(s.currentSnap.Rules))
	for f, r := range s.currentSnap.Rules {
		nextRules[f] = r
	}
	if fallback > 0 {
		nextRules[feature] = s.versions[feature][fallback]
	} else {
		delete(nextRules, feature)
	}

	s.seq++
	newSeq := s.seq
	snap := &Snapshot{Seq: newSeq, Rules: nextRules}

	rec := PublishRecord{
		Seq:      newSeq,
		ChangeID: fmt.Sprintf("auto-pause:%s@%d", feature, version),
		Kind:     "pause",
		Features: []string{feature},
		Time:     time.Now().UTC(),
	}

	s.currentSnap = snap
	s.snapshots[newSeq] = snap
	if fallback > 0 {
		s.current[feature] = fallback
	} else {
		delete(s.current, feature)
	}
	s.history = append(s.history, rec)
	if s.paused[feature] == nil {
		s.paused[feature] = map[int]bool{}
	}
	s.paused[feature][version] = true

	// 统计快照：保存触发时刻的完整依据，含全部被计入的上报号。
	reportIDs := make([]string, 0, len(st.reportIDs))
	for id := range st.reportIDs {
		reportIDs = append(reportIDs, id)
	}
	sort.Strings(reportIDs)
	statsSnap := StatsSnapshot{
		Total:           st.total,
		Failures:        st.failures,
		FailureRatio:    ratio,
		MinObservations: rv.Guard.MinObservations,
		MaxFailureRatio: rv.Guard.MaxFailureRatio,
		ReportIDs:       reportIDs,
	}

	pause := &PauseRecord{
		Feature: feature,
		Version: version,
		Seq:     newSeq,
		Reason: fmt.Sprintf("feature %q version %d: failure ratio %.4f (%d/%d) exceeded limit %.4f after %d observations (min %d)",
			feature, version, ratio, st.failures, st.total,
			rv.Guard.MaxFailureRatio, st.total, rv.Guard.MinObservations),
		Stats: statsSnap,
		Time:  rec.Time,
	}
	s.pauses = append(s.pauses, *pause)
	return pause
}

func (s *Service) statsForLocked(feature string, version int) *versionStats {
	m := s.stats[feature]
	if m == nil {
		m = map[int]*versionStats{}
		s.stats[feature] = m
	}
	st := m[version]
	if st == nil {
		st = &versionStats{reportIDs: map[string]struct{}{}}
		m[version] = st
	}
	return st
}

// StatsOf 返回某功能指定版本的统计视图；版本从未发布时 ok 为 false。
func (s *Service) StatsOf(feature string, version int) (VersionStatsView, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	vm, ok := s.versions[feature]
	if !ok {
		return VersionStatsView{}, false
	}
	if _, ok := vm[version]; !ok {
		return VersionStatsView{}, false
	}
	view := VersionStatsView{
		Feature: feature,
		Version: version,
		Paused:  s.paused[feature][version],
	}
	if st := s.stats[feature][version]; st != nil {
		view.Total = st.total
		view.Failures = st.failures
		if st.total > 0 {
			view.FailureRatio = float64(st.failures) / float64(st.total)
		}
	}
	return view, true
}

// PauseOf 返回某功能最近一次自动暂停的记录；未发生过暂停时 ok 为 false。
func (s *Service) PauseOf(feature string) (PauseRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := len(s.pauses) - 1; i >= 0; i-- {
		if s.pauses[i].Feature == feature {
			return s.pauses[i], true
		}
	}
	return PauseRecord{}, false
}

// Pauses 返回全部自动暂停记录，按发生顺序（序列号升序）。
func (s *Service) Pauses() []PauseRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]PauseRecord, len(s.pauses))
	copy(out, s.pauses)
	return out
}
