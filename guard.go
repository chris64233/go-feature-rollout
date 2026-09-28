package featurerollout

import (
	"fmt"
	"sort"
	"time"
)

// 本文件实现线上结果守卫：
//
//   - 结果上报必须携带判定时实际采用的版本（及快照序列号），服务端校验归属，
//     统计严格按 (feature, version) 隔离；
//   - ResultID 全局去重，重复上报不重复计数；
//   - 观察数达到版本配置的下限且失败比例严格越线时，自动把该功能切回上一稳定版本；
//   - 暂停整体在写锁内提交，同一版本至多暂停一次，重复上报/重复检查不再产生
//     状态变化与通知；
//   - 暂停采用乐观并发：提交时若当前快照已被手工回滚/重新发布改变，则拒绝过期暂停。

// pausedVersions 返回某功能所有"已暂停过"的版本集合（延迟创建）。
// 调用方持有写锁。
func (s *Service) pausedVersions(feature string) map[int]*PauseEvent {
	m, ok := s.pauseEventsByVersion[feature]
	if !ok {
		m = map[int]*PauseEvent{}
		s.pauseEventsByVersion[feature] = m
	}
	return m
}

// ---------------- 结果上报 ----------------

// Report 上报一次线上结果。
//
// 归属规则：
//   - r.ExpectedVersion 必须是已经发布的版本；
//   - r.ExpectedSeq 非 0 时，该快照中该功能必须恰好服务于 ExpectedVersion
//     （快照不存在或服务的是别的版本，按过期操作 KindStale 拒绝）；
//   - r.ExpectedSeq 为 0 时，只接受当前正在服务的版本，否则按 KindStale 拒绝。
//
// 重复上报：同一 ResultID 且归属相同 -> Accepted=false 幂等返回，不计数、
// 不触发暂停、不通知；同一 ResultID 改挂别的版本 -> KindConflict。
func (s *Service) Report(r ResultReport) (*ReportResult, error) {
	if r.ResultID == "" {
		return nil, paramErr("ResultID is empty")
	}
	if r.Feature == "" {
		return nil, paramErr("feature name is empty")
	}
	if r.ExpectedVersion < 1 {
		return nil, paramErr("ExpectedVersion must be >= 1, got %d", r.ExpectedVersion)
	}

	s.mu.Lock()
	res, ev, err := s.reportLocked(r)
	notifier := s.notifier
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	// 通知只可能伴随一次"新提交"的暂停；锁外调用，保证不重入、不重复。
	// ev 与 res.PauseEvent 同为锁内制作的独立副本，回调无法改写内部状态。
	if ev != nil && notifier != nil {
		notifier(*res.PauseEvent)
	}
	return res, nil
}

func (s *Service) reportLocked(r ResultReport) (*ReportResult, *PauseEvent, error) {
	key := statsKey(r.Feature, r.ExpectedVersion)

	// 去重：同一结果只能计数一次。
	if prevKey, seen := s.resultIndex[r.ResultID]; seen {
		if prevKey != key {
			return nil, nil, conflictErr("ResultID %q already reported against %q", r.ResultID, prevKey)
		}
		return &ReportResult{
			Accepted: false,
			Paused:   false,
			Stats:    s.statsSnapshotLocked(s.stats[key]),
		}, nil, nil
	}

	// 版本必须真实发布过（发布后不可变，守卫配置也随之固定）。
	rv := s.versions[r.Feature][r.ExpectedVersion]
	if rv == nil {
		return nil, nil, versionErr("feature %q version %d has never been published",
			r.Feature, r.ExpectedVersion)
	}

	// 校验结果与实际采用版本的归属。
	if r.ExpectedSeq != 0 {
		snap, ok := s.snapshots[r.ExpectedSeq]
		if !ok {
			return nil, nil, staleErr("snapshot seq %d no longer exists for report %q",
				r.ExpectedSeq, r.ResultID)
		}
		served, ok := snap.Rules[r.Feature]
		if !ok || served.Version != r.ExpectedVersion {
			servedV := 0
			if served != nil {
				servedV = served.Version
			}
			return nil, nil, staleErr("stale report %q: snapshot seq %d serves feature %q version %d, not %d",
				r.ResultID, r.ExpectedSeq, r.Feature, servedV, r.ExpectedVersion)
		}
	} else {
		cur, ok := s.currentSnap.Rules[r.Feature]
		if !ok || cur.Version != r.ExpectedVersion {
			servedV := 0
			if cur != nil {
				servedV = cur.Version
			}
			return nil, nil, staleErr("stale report %q: feature %q currently serves version %d, not %d",
				r.ResultID, r.Feature, servedV, r.ExpectedVersion)
		}
	}

	// 计入对应版本的统计，版本之间天然隔离，旧版本结果进不了新版本的集合。
	st := s.stats[key]
	if st == nil {
		st = &versionStats{
			feature:    r.Feature,
			version:    r.ExpectedVersion,
			resultIDs:  map[string]struct{}{},
			failureIDs: map[string]struct{}{},
		}
		s.stats[key] = st
	}
	st.resultIDs[r.ResultID] = struct{}{}
	if r.Failure {
		st.failureIDs[r.ResultID] = struct{}{}
	}
	s.resultIndex[r.ResultID] = key

	snap := s.statsSnapshotLocked(st)
	res := &ReportResult{Accepted: true, Stats: snap}

	// 该版本已经暂停过：继续允许计数（晚到的真实结果），但绝不再次暂停/通知。
	if _, alreadyPaused := s.pausedVersions(r.Feature)[r.ExpectedVersion]; alreadyPaused {
		return res, nil, nil
	}
	if !guardCrossed(snap) {
		return res, nil, nil
	}

	// 越线：尝试提交自动暂停。若期间已被手工回滚/重新发布抢先，暂停作为过期操作被拒绝，
	// 本次上报依然保留在正确版本的统计中。
	ev, err := s.autoPauseLocked(rv, st, snap, r)
	if err != nil {
		if k, ok := KindOf(err); ok && k == KindStale {
			return res, nil, nil
		}
		return nil, nil, err
	}
	res.Paused = true
	res.PauseEvent = clonePauseEvent(ev)
	return res, ev, nil
}

// ---------------- 显式检查 ----------------

// CheckPause 对某功能"当前正在服务"的版本重新执行一次越线检查。
//
// 返回值：事件非 nil 表示该版本已处于暂停状态（本次新触发或此前已暂停）；
// nil 表示未越线、不暂停。此前已暂停时原样返回既有事件，不产生状态变化、不通知。
func (s *Service) CheckPause(feature string) (*PauseEvent, error) {
	s.mu.Lock()
	cur := s.currentSnap.Rules[feature]
	if cur == nil {
		s.mu.Unlock()
		return nil, notFoundErr("feature %q has no current version", feature)
	}
	if existing := s.pausedVersions(feature)[cur.Version]; existing != nil {
		s.mu.Unlock()
		return clonePauseEvent(existing), nil
	}
	st := s.stats[statsKey(feature, cur.Version)]
	snap := s.statsSnapshotLocked(st)
	var ev *PauseEvent
	var err error
	if guardCrossed(snap) {
		trigger := ResultReport{Feature: feature, ExpectedVersion: cur.Version, ExpectedSeq: s.currentSnap.Seq}
		ev, err = s.autoPauseLocked(cur, st, snap, trigger)
	}
	notifier := s.notifier
	s.mu.Unlock()
	if err != nil {
		if k, ok := KindOf(err); ok && k == KindStale {
			// 并发的手工操作已改变当前版本：本次检查过期，什么也不做。
			return nil, nil
		}
		return nil, err
	}
	if ev != nil {
		out := clonePauseEvent(ev)
		if notifier != nil {
			notifier(*out)
		}
		return out, nil
	}
	return nil, nil
}

// ---------------- 自动暂停提交 ----------------

// autoPauseLocked 在写锁内原子提交一次自动暂停。调用方须持有写锁。
//
// 乐观并发：只有当前快照仍然服务于被暂停版本时才提交；手工回滚或重新发布
// 已经改变当前版本时返回 KindStale，拒绝这次过期暂停。
func (s *Service) autoPauseLocked(rv *RuleVersion, st *versionStats, snap StatsSnapshot, trigger ResultReport) (*PauseEvent, error) {
	feature := rv.Feature
	cur := s.currentSnap.Rules[feature]
	if cur == nil || cur.Version != rv.Version {
		servedV := 0
		if cur != nil {
			servedV = cur.Version
		}
		return nil, staleErr("auto-pause rejected: feature %q current version is %d, report was for %d",
			feature, servedV, rv.Version)
	}
	if _, alreadyPaused := s.pausedVersions(feature)[rv.Version]; alreadyPaused {
		// 防御性兜底：正常调用方已提前判断。
		return nil, nil
	}
	prevSeq := s.currentSnap.Seq
	stable := s.stableVersionLocked(feature, rv.Version)

	nextRules := make(map[string]*RuleVersion, len(s.currentSnap.Rules))
	for f, r := range s.currentSnap.Rules {
		nextRules[f] = r
	}
	if stable == 0 {
		// 没有上一稳定版本：该功能整体下线（依赖它的功能在判定时随之 fail-closed）。
		delete(nextRules, feature)
	} else {
		nextRules[feature] = s.versions[feature][stable]
	}

	s.seq++
	newSeq := s.seq
	newSnap := &Snapshot{Seq: newSeq, Rules: nextRules}

	rec := PublishRecord{
		Seq:      newSeq,
		Kind:     "auto-pause",
		Features: []string{feature},
		Time:     time.Now().UTC(),
	}

	ev := &PauseEvent{
		Feature:     feature,
		Version:     rv.Version,
		PausedAtSeq: newSeq,
		PreviousSeq: prevSeq,
		RevertedTo:  stable,
		Reason: fmt.Sprintf("failure rate %.4f (%d/%d) exceeds max %.4f with min observations %d",
			snap.FailureRate, snap.Failures, snap.Observed, snap.MaxFailureRate, snap.MinObservations),
		Snapshot:      snap,
		TriggerResult: trigger,
		ResultIDs:     sortedKeys(st.resultIDs),
		FailureIDs:    sortedKeys(st.failureIDs),
		Time:          rec.Time,
	}

	// 单点切换：只动这一个功能的指针，其余功能沿用同一份快照。
	s.currentSnap = newSnap
	s.snapshots[newSeq] = newSnap
	if stable == 0 {
		delete(s.current, feature)
	} else {
		s.current[feature] = stable
	}
	s.history = append(s.history, rec)
	s.pausedVersions(feature)[rv.Version] = ev
	s.pauseEvents = append(s.pauseEvents, ev)
	return ev, nil
}

// stableVersionLocked 查找上一稳定版本：版本号严格小于 badVersion、
// 且自身未曾被自动暂停过的最高已发布版本；不存在返回 0（表示整体下线）。
func (s *Service) stableVersionLocked(feature string, badVersion int) int {
	best := 0
	for v := range s.versions[feature] {
		if v >= badVersion || v <= best {
			continue
		}
		if _, paused := s.pausedVersions(feature)[v]; paused {
			continue
		}
		best = v
	}
	return best
}

// ---------------- 查询 ----------------

// Stats 返回某功能某版本当前的去重统计；版本必须已发布。
func (s *Service) Stats(feature string, version int) (StatsSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if rv := s.versions[feature][version]; rv == nil {
		return StatsSnapshot{}, versionErr("feature %q version %d has never been published", feature, version)
	}
	return s.statsSnapshotLocked(s.stats[statsKey(feature, version)]), nil
}

// PauseEvents 返回所有自动暂停事件，按发生顺序升序，内容为不可变副本。
func (s *Service) PauseEvents() []*PauseEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*PauseEvent, len(s.pauseEvents))
	for i, ev := range s.pauseEvents {
		out[i] = clonePauseEvent(ev)
	}
	return out
}

// PauseEventForVersion 返回某功能某版本的暂停事件；未暂停过返回 nil。
func (s *Service) PauseEventForVersion(feature string, version int) *PauseEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if ev := s.pauseEventsByVersion[feature][version]; ev != nil {
		return clonePauseEvent(ev)
	}
	return nil
}

// ---------------- 内部工具 ----------------

// statsSnapshotLocked 由结果集合严格推导统计快照，计数不可能与集合不一致。
func (s *Service) statsSnapshotLocked(st *versionStats) StatsSnapshot {
	if st == nil {
		return StatsSnapshot{}
	}
	obs := len(st.resultIDs)
	fails := len(st.failureIDs)
	rate := 0.0
	if obs > 0 {
		rate = float64(fails) / float64(obs)
	}
	out := StatsSnapshot{
		Feature:     st.feature,
		Version:     st.version,
		Observed:    obs,
		Failures:    fails,
		FailureRate: rate,
	}
	if rv := s.versions[st.feature][st.version]; rv != nil {
		out.MinObservations = rv.MinObservations
		out.MaxFailureRate = rv.MaxFailureRate
	}
	return out
}

// guardCrossed 判断是否越线：观察量足够，且失败比例"严格"大于上限。
// MinObservations == 0 表示该版本未启用守卫，永不越线。
func guardCrossed(st StatsSnapshot) bool {
	return st.MinObservations > 0 &&
		st.Observed >= st.MinObservations &&
		st.FailureRate > st.MaxFailureRate
}

func sortedKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func clonePauseEvent(ev *PauseEvent) *PauseEvent {
	if ev == nil {
		return nil
	}
	cp := *ev
	cp.ResultIDs = append([]string(nil), ev.ResultIDs...)
	cp.FailureIDs = append([]string(nil), ev.FailureIDs...)
	return &cp
}
