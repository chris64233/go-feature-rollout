package featurerollout

import (
	"fmt"
	"sync"
	"testing"
)

// guardPublish 发布一个带守卫配置的新版本，返回发布结果。
func guardPublish(t *testing.T, s *Service, changeID, feature string, version, pct, minObs int, maxRate float64) *PublishResult {
	t.Helper()
	return mustPublish(t, s, changeID, []RuleInput{{
		Feature:         feature,
		Version:         version,
		Percentage:      pct,
		MinObservations: minObs,
		MaxFailureRate:  maxRate,
	}})
}

func mustReport(t *testing.T, s *Service, r ResultReport) *ReportResult {
	t.Helper()
	res, err := s.Report(r)
	if err != nil {
		t.Fatalf("Report(%+v) failed: %v", r, err)
	}
	return res
}

// ---------- 守卫参数校验 ----------

func TestGuardConfigValidation(t *testing.T) {
	s := NewService()

	_, err := s.Publish("chg-g-1", []RuleInput{{
		Feature: "a", Version: 1, MinObservations: -1, MaxFailureRate: 0.5,
	}})
	expectKind(t, err, KindParam)

	_, err = s.Publish("chg-g-2", []RuleInput{{
		Feature: "a", Version: 1, MinObservations: 10, MaxFailureRate: 1.1,
	}})
	expectKind(t, err, KindParam)

	_, err = s.Publish("chg-g-3", []RuleInput{{
		Feature: "a", Version: 1, MinObservations: 10, MaxFailureRate: -0.1,
	}})
	expectKind(t, err, KindParam)

	// 上报参数。
	_, err = s.Report(ResultReport{Feature: "a", ExpectedVersion: 1})
	expectKind(t, err, KindParam) // 缺 ResultID
	_, err = s.Report(ResultReport{ResultID: "r1", ExpectedVersion: 1})
	expectKind(t, err, KindParam) // 缺 Feature
	_, err = s.Report(ResultReport{ResultID: "r1", Feature: "a", ExpectedVersion: 0})
	expectKind(t, err, KindParam)
}

// ---------- 版本归属与统计隔离 ----------

func TestReportAttribution(t *testing.T) {
	s := NewService()
	r1 := mustPublish(t, s, "chg-att-1", []RuleInput{{Feature: "a", Version: 1, Percentage: 100}})
	r2 := guardPublish(t, s, "chg-att-2", "a", 2, 100, 10, 0.2)

	// v1 的结果计入 v1，绝不混入 v2。
	for i := 0; i < 5; i++ {
		mustReport(t, s, ResultReport{
			ResultID: fmt.Sprintf("old-%d", i), Feature: "a",
			ExpectedVersion: 1, ExpectedSeq: r1.Seq, Failure: true,
		})
	}
	st1, err := s.Stats("a", 1)
	if err != nil || st1.Observed != 5 || st1.Failures != 5 {
		t.Fatalf("v1 stats = %+v, %v", st1, err)
	}
	st2, err := s.Stats("a", 2)
	if err != nil || st2.Observed != 0 {
		t.Fatalf("v2 stats must stay empty, got %+v, %v", st2, err)
	}

	// 快照中实际服务的是 v1，却声称来自 v2：过期上报被拒绝。
	_, err = s.Report(ResultReport{
		ResultID: "stale-1", Feature: "a",
		ExpectedVersion: 2, ExpectedSeq: r1.Seq, Failure: true,
	})
	expectKind(t, err, KindStale)

	// 不存在的快照序列号：拒绝。
	_, err = s.Report(ResultReport{
		ResultID: "stale-2", Feature: "a",
		ExpectedVersion: 2, ExpectedSeq: 999, Failure: true,
	})
	expectKind(t, err, KindStale)

	// 不携带快照号时只接受"当前"版本：当前是 v2，报 v2 接受，报 v1 拒绝。
	mustReport(t, s, ResultReport{ResultID: "cur-1", Feature: "a", ExpectedVersion: 2, Failure: false})
	_, err = s.Report(ResultReport{ResultID: "cur-2", Feature: "a", ExpectedVersion: 1})
	expectKind(t, err, KindStale)

	// 快照与版本一致（r2 服务 v2）：接受。
	mustReport(t, s, ResultReport{
		ResultID: "att-v2-1", Feature: "a",
		ExpectedVersion: 2, ExpectedSeq: r2.Seq, Failure: true,
	})
	st2, _ = s.Stats("a", 2)
	if st2.Observed != 2 || st2.Failures != 1 {
		t.Fatalf("v2 stats = %+v, want obs=2 fails=1", st2)
	}

	// 版本从未发布：版本错误。
	_, err = s.Stats("a", 99)
	expectKind(t, err, KindVersion)
	_, err = s.Report(ResultReport{ResultID: "ghost", Feature: "a", ExpectedVersion: 99, ExpectedSeq: r2.Seq})
	expectKind(t, err, KindVersion)
}

// ---------- 重复上报去重 ----------

func TestReportDedup(t *testing.T) {
	s := NewService()
	r := guardPublish(t, s, "chg-dedup-1", "a", 1, 100, 100, 0.5)

	first := mustReport(t, s, ResultReport{
		ResultID: "dup-1", Feature: "a", ExpectedVersion: 1, ExpectedSeq: r.Seq, Failure: true,
	})
	if !first.Accepted {
		t.Fatal("first report must be accepted")
	}
	// 完全相同的重复上报：不重复计数。
	second := mustReport(t, s, ResultReport{
		ResultID: "dup-1", Feature: "a", ExpectedVersion: 1, ExpectedSeq: r.Seq, Failure: false,
	})
	if second.Accepted {
		t.Fatal("duplicate report must not be accepted again")
	}
	st, _ := s.Stats("a", 1)
	if st.Observed != 1 || st.Failures != 1 {
		t.Fatalf("duplicate report changed counts: %+v", st)
	}

	// 发布 v2 后，同一 ResultID 改挂 v2：冲突，计数不变。
	guardPublish(t, s, "chg-dedup-2", "a", 2, 100, 100, 0.5)
	_, err := s.Report(ResultReport{ResultID: "dup-1", Feature: "a", ExpectedVersion: 2})
	expectKind(t, err, KindConflict)
}

// ---------- 越线阈值语义 ----------

func TestGuardThreshold(t *testing.T) {
	// 观察量不足：即使失败率 100% 也不暂停；量够后才暂停。
	s1 := NewService()
	r1 := guardPublish(t, s1, "chg-th-1", "a", 1, 100, 5, 0.2)
	report1 := func(id string, fail bool) *ReportResult {
		return mustReport(t, s1, ResultReport{
			ResultID: id, Feature: "a", ExpectedVersion: 1, ExpectedSeq: r1.Seq, Failure: fail,
		})
	}
	for i := 0; i < 4; i++ {
		if res := report1(fmt.Sprintf("r%d", i), true); res.Paused {
			t.Fatalf("must not pause before min observations (at %d)", i+1)
		}
	}
	if res := report1("r4", false); !res.Paused { // 4/5 = 0.8 > 0.2
		t.Fatal("must pause once min observations met and rate crossed")
	}

	// 等号不算越线：5 个观察中 1 失败（恰好 0.2）不暂停，再来一个失败才暂停。
	s2 := NewService()
	r2 := guardPublish(t, s2, "chg-th-2", "a", 1, 100, 5, 0.2)
	report2 := func(id string, fail bool) *ReportResult {
		return mustReport(t, s2, ResultReport{
			ResultID: id, Feature: "a", ExpectedVersion: 1, ExpectedSeq: r2.Seq, Failure: fail,
		})
	}
	report2("a1", true)
	for i := 0; i < 4; i++ {
		report2(fmt.Sprintf("b%d", i), false)
	}
	if ev := s2.PauseEventForVersion("a", 1); ev != nil {
		t.Fatalf("failure rate exactly at max must not pause: %+v", ev)
	}
	if res := report2("c1", true); !res.Paused { // 2/6 > 0.2
		t.Fatal("expected pause when 2/6 failures exceeds 0.2")
	}

	// 上限为 0 时，首个失败即暂停（min=1）。
	s3 := NewService()
	r3 := guardPublish(t, s3, "chg-th-3", "x", 1, 100, 1, 0.0)
	if res := mustReport(t, s3, ResultReport{
		ResultID: "ok", Feature: "x", ExpectedVersion: 1, ExpectedSeq: r3.Seq, Failure: false,
	}); res.Paused {
		t.Fatal("success must not pause when max rate is 0")
	}
	if res := mustReport(t, s3, ResultReport{
		ResultID: "bad", Feature: "x", ExpectedVersion: 1, ExpectedSeq: r3.Seq, Failure: true,
	}); !res.Paused {
		t.Fatal("first failure must pause when max rate is 0 and min is 1")
	}
}

// ---------- 自动暂停：切回上一稳定版本、事件留存 ----------

func TestAutoPauseRevertsAndRecords(t *testing.T) {
	s := NewService()
	// v1 全量稳定版本（不启用守卫），v2 灰度版本启用守卫。
	r1 := guardPublish(t, s, "chg-ap-1", "a", 1, 100, 0, 0)
	r2 := guardPublish(t, s, "chg-ap-2", "a", 2, 10, 10, 0.2)

	if got := s.Evaluate("a", "u1").Version; got != 2 {
		t.Fatalf("expected current version 2, got %d", got)
	}

	// 9 失败 1 成功，凑满 10 个观察，失败率 0.9。
	var trigger *ReportResult
	for i := 0; i < 9; i++ {
		trigger = mustReport(t, s, ResultReport{
			ResultID: fmt.Sprintf("f%d", i), Feature: "a",
			ExpectedVersion: 2, ExpectedSeq: r2.Seq, Failure: true,
		})
	}
	if trigger.Paused {
		t.Fatal("must not pause with only 9 observations")
	}
	trigger = mustReport(t, s, ResultReport{
		ResultID: "ok1", Feature: "a",
		ExpectedVersion: 2, ExpectedSeq: r2.Seq, Failure: false,
	})
	if !trigger.Paused {
		t.Fatal("10th report crossing threshold must pause")
	}

	// 暂停后新判定回到 v1。
	cur := s.Evaluate("a", "u1")
	if cur.Version != 1 {
		t.Fatalf("new decisions must use stable v1, got version %d", cur.Version)
	}
	if cur.Seq != trigger.PauseEvent.PausedAtSeq {
		t.Fatalf("eval seq %d != pause snapshot seq %d", cur.Seq, trigger.PauseEvent.PausedAtSeq)
	}
	if got := s.CurrentRule("a").Version; got != 1 {
		t.Fatalf("current rule version = %d, want 1", got)
	}

	// 历史追加 auto-pause 记录，序列号单调。
	h := s.History()
	last := h[len(h)-1]
	if last.Kind != "auto-pause" || last.Seq != trigger.PauseEvent.PausedAtSeq {
		t.Fatalf("unexpected last history record: %+v", last)
	}
	for i := 1; i < len(h); i++ {
		if h[i].Seq <= h[i-1].Seq {
			t.Fatalf("history seq not monotonic: %+v", h)
		}
	}

	// 事件记录了完整统计依据，且计数与 ID 集合严格一致。
	ev := trigger.PauseEvent
	if ev.Feature != "a" || ev.Version != 2 || ev.RevertedTo != 1 || ev.PreviousSeq != r2.Seq {
		t.Fatalf("unexpected pause event: %+v", ev)
	}
	if ev.Snapshot.Observed != 10 || ev.Snapshot.Failures != 9 {
		t.Fatalf("pause snapshot stats wrong: %+v", ev.Snapshot)
	}
	if len(ev.ResultIDs) != 10 || len(ev.FailureIDs) != 9 {
		t.Fatalf("pause event must explain counted results: %d/%d", len(ev.ResultIDs), len(ev.FailureIDs))
	}
	if ev.TriggerResult.ResultID != "ok1" {
		t.Fatalf("trigger result = %+v", ev.TriggerResult)
	}
	if ev.Reason == "" {
		t.Fatal("pause reason must be recorded")
	}

	// 查询接口返回同一事件；事件总数为 1。
	if got := s.PauseEventForVersion("a", 2); got == nil || got.PausedAtSeq != ev.PausedAtSeq {
		t.Fatalf("PauseEventForVersion mismatch: %+v", got)
	}
	if events := s.PauseEvents(); len(events) != 1 || events[0].PausedAtSeq != ev.PausedAtSeq {
		t.Fatalf("PauseEvents = %+v", events)
	}

	// r1 快照回放仍然是 v1，历史判定语义不变。
	old, err := s.EvalAt(r1.Seq, "a", "u1")
	if err != nil || old.Version != 1 || !old.Allowed {
		t.Fatalf("historical replay changed: %+v, %v", old, err)
	}
}

// ---------- 暂停只生效一次 ----------

func TestPauseHappensOnce(t *testing.T) {
	s := NewService()
	var mu sync.Mutex
	notifies := 0
	s.SetNotifier(func(ev PauseEvent) {
		mu.Lock()
		notifies++
		mu.Unlock()
	})

	r1 := guardPublish(t, s, "chg-once-1", "a", 1, 100, 0, 0)
	r2 := guardPublish(t, s, "chg-once-2", "a", 2, 100, 1, 0.0)

	cross := func(id string) *ReportResult {
		return mustReport(t, s, ResultReport{
			ResultID: id, Feature: "a",
			ExpectedVersion: 2, ExpectedSeq: r2.Seq, Failure: true,
		})
	}
	first := cross("x1")
	if !first.Paused {
		t.Fatal("first crossing report must pause")
	}
	// 暂停后继续有失败结果到达：照常计数，但不再暂停、不再通知。
	for _, id := range []string{"x2", "x3", "x4"} {
		res := cross(id)
		if !res.Accepted || res.Paused || res.PauseEvent != nil {
			t.Fatalf("post-pause report %s must be counted without pausing: %+v", id, res)
		}
	}
	st, _ := s.Stats("a", 2)
	if st.Observed != 4 {
		t.Fatalf("post-pause reports must still count, got %+v", st)
	}

	// 重复上报同样不再产生状态变化。
	dup := mustReport(t, s, ResultReport{
		ResultID: "x1", Feature: "a", ExpectedVersion: 2, ExpectedSeq: r2.Seq, Failure: true,
	})
	if dup.Accepted || dup.Paused {
		t.Fatalf("duplicate trigger report must stay inert: %+v", dup)
	}

	if len(s.PauseEvents()) != 1 || len(s.History()) != 3 { // publish v1, publish v2, auto-pause
		t.Fatalf("state changed after pause: events=%d history=%d", len(s.PauseEvents()), len(s.History()))
	}
	if notifies != 1 {
		t.Fatalf("notifier must fire exactly once, got %d", notifies)
	}

	// 当前已是 v1：对当前 v1 重复检查不会做任何事（v1 未启用守卫）。
	if ev, err := s.CheckPause("a"); err != nil || ev != nil {
		t.Fatalf("CheckPause on stable v1 must do nothing: %+v %v", ev, err)
	}

	// 人工把当前切回"曾经服务 v2"的历史快照后重复检查 v2：
	// 只返回既有事件，不新增状态、不重复通知。
	mustRollback(t, s, "chg-once-rb", r2.Seq)
	ev1, err := s.CheckPause("a")
	if err != nil || ev1 == nil || ev1.Version != 2 {
		t.Fatalf("CheckPause should return the existing paused event: %+v, %v", ev1, err)
	}
	ev2, _ := s.CheckPause("a")
	if ev2.PausedAtSeq != ev1.PausedAtSeq {
		t.Fatalf("repeated CheckPause must return the same event: %d vs %d", ev1.PausedAtSeq, ev2.PausedAtSeq)
	}
	if len(s.PauseEvents()) != 1 {
		t.Fatalf("recheck must not add events, got %d", len(s.PauseEvents()))
	}
	if notifies != 1 {
		t.Fatalf("recheck must not notify again, got %d", notifies)
	}
	_ = r1
}

// ---------- 无上一稳定版本时整体下线，依赖者 fail-closed ----------

func TestPauseWithoutStableVersion(t *testing.T) {
	s := NewService()
	// base v1 直接带守卫，没有更早的稳定版本。
	mustPublish(t, s, "chg-nostable-1", []RuleInput{
		{Feature: "base", Version: 1, Percentage: 100, MinObservations: 1, MaxFailureRate: 0},
		{Feature: "top", Version: 1, Percentage: 100, Deps: []Dependency{{Feature: "base", Version: 1}}},
	})
	res := mustReport(t, s, ResultReport{
		ResultID: "boom", Feature: "base", ExpectedVersion: 1, Failure: true,
	})
	if !res.Paused {
		t.Fatal("base v1 must pause")
	}
	ev := res.PauseEvent
	if ev.RevertedTo != 0 {
		t.Fatalf("expected feature removal (RevertedTo=0), got %d", ev.RevertedTo)
	}
	if s.CurrentRule("base") != nil {
		t.Fatal("base must have no current version after pause")
	}
	if s.Evaluate("base", "u1").Allowed {
		t.Fatal("base must be denied after removal")
	}
	// 依赖 base 的功能随之 fail-closed，且不会 panic。
	if s.Evaluate("top", "u1").Allowed {
		t.Fatal("dependent feature must fail closed when dependency is paused away")
	}
}

// ---------- 暂停版本在寻找稳定版本时被跳过 ----------

func TestStableVersionSkipsPaused(t *testing.T) {
	s := NewService()
	guardPublish(t, s, "chg-sk-1", "a", 1, 100, 0, 0)
	r2 := guardPublish(t, s, "chg-sk-2", "a", 2, 100, 1, 0.0)
	mustReport(t, s, ResultReport{ResultID: "p2", Feature: "a", ExpectedVersion: 2, ExpectedSeq: r2.Seq, Failure: true})
	if s.CurrentRule("a").Version != 1 {
		t.Fatal("v2 should pause back to v1")
	}

	// 在 v1 之上发布 v3（v2 已暂停），v3 再越线时应跳过 v2 回到 v1。
	r3 := guardPublish(t, s, "chg-sk-3", "a", 3, 100, 1, 0.0)
	res := mustReport(t, s, ResultReport{ResultID: "p3", Feature: "a", ExpectedVersion: 3, ExpectedSeq: r3.Seq, Failure: true})
	if !res.Paused || res.PauseEvent.RevertedTo != 1 {
		t.Fatalf("v3 pause must revert to v1 skipping paused v2: %+v", res.PauseEvent)
	}
	if len(s.PauseEvents()) != 2 {
		t.Fatalf("v2 and v3 each pause exactly once, got %d", len(s.PauseEvents()))
	}
}

// ---------- 自动暂停与手工回滚 / 重新发布并发：过期操作被拒绝 ----------

func TestPauseRejectsStaleOperations(t *testing.T) {
	s := NewService()
	r1 := guardPublish(t, s, "chg-st-1", "a", 1, 100, 0, 0)
	r2 := guardPublish(t, s, "chg-st-2", "a", 2, 100, 10, 0.2)

	// 给 v2 攒到临界（9 个失败），但先不越线。
	for i := 0; i < 9; i++ {
		mustReport(t, s, ResultReport{
			ResultID: fmt.Sprintf("v2-%d", i), Feature: "a",
			ExpectedVersion: 2, ExpectedSeq: r2.Seq, Failure: true,
		})
	}

	// 手工回滚抢先：当前回到 v1。
	mustRollback(t, s, "chg-st-rb", r1.Seq)

	// 晚到的 v2 越线上报：归属合法（快照 r2 当时确实服务 v2），计数保留，
	// 但暂停作为过期操作被拒绝——不产生暂停事件、不通知、v1 当前状态不变。
	res := mustReport(t, s, ResultReport{
		ResultID: "v2-late", Feature: "a",
		ExpectedVersion: 2, ExpectedSeq: r2.Seq, Failure: true,
	})
	if !res.Accepted || res.Paused {
		t.Fatalf("late report must be counted but not pause after rollback: %+v", res)
	}
	if s.PauseEventForVersion("a", 2) != nil {
		t.Fatal("stale auto-pause must not create an event")
	}
	if s.CurrentRule("a").Version != 1 {
		t.Fatal("manual rollback state must survive the rejected pause")
	}
	st, _ := s.Stats("a", 2)
	if st.Observed != 10 || st.Failures != 10 {
		t.Fatalf("late report must still be attributed to v2: %+v", st)
	}

	// 重新发布 v3：v2 的统计绝不混入 v3，v3 从 0 开始观察。
	r3 := guardPublish(t, s, "chg-st-3", "a", 3, 100, 10, 0.5)
	st3, _ := s.Stats("a", 3)
	if st3.Observed != 0 {
		t.Fatalf("new version stats must start empty: %+v", st3)
	}
	// 不带快照号的 v2 上报：当前已是 v3，按过期拒绝。
	_, err := s.Report(ResultReport{ResultID: "stale-current", Feature: "a", ExpectedVersion: 2})
	expectKind(t, err, KindStale)
	// 对 v2 的显式检查也不能影响当前 v3。
	if ev, err := s.CheckPause("a"); err != nil || ev != nil {
		t.Fatalf("CheckPause on current v3 with empty stats must do nothing: %+v %v", ev, err)
	}
	if s.CurrentSnapshotSeq() != r3.Seq {
		t.Fatal("current snapshot must remain v3's publish")
	}
}

func mustRollback(t *testing.T, s *Service, changeID string, seq uint64) *PublishResult {
	t.Helper()
	res, err := s.Rollback(changeID, seq)
	if err != nil {
		t.Fatalf("Rollback(%q, %d) failed: %v", changeID, seq, err)
	}
	return res
}

// ---------- CheckPause 未配置 / 未越线 / 不存在 ----------

func TestCheckPauseGuards(t *testing.T) {
	s := NewService()
	r := mustPublish(t, s, "chg-cp-1", []RuleInput{{Feature: "a", Version: 1, Percentage: 100}})

	// 未启用守卫：失败再多也不检查暂停。
	for i := 0; i < 20; i++ {
		mustReport(t, s, ResultReport{
			ResultID: fmt.Sprintf("r%d", i), Feature: "a",
			ExpectedVersion: 1, ExpectedSeq: r.Seq, Failure: true,
		})
	}
	if ev, err := s.CheckPause("a"); err != nil || ev != nil {
		t.Fatalf("unguarded version must never pause: %+v %v", ev, err)
	}
	_, err := s.CheckPause("ghost")
	expectKind(t, err, KindNotFound)
}

// ---------- 并发：重复上报只暂停、只通知一次 ----------

func TestConcurrentReportsPauseOnce(t *testing.T) {
	s := NewService()
	var mu sync.Mutex
	notifies := 0
	s.SetNotifier(func(PauseEvent) {
		mu.Lock()
		notifies++
		mu.Unlock()
	})

	guardPublish(t, s, "chg-cr-1", "a", 1, 100, 0, 0)
	r2 := guardPublish(t, s, "chg-cr-2", "a", 2, 100, 50, 0.1)

	const workers = 8
	const perWorker = 20
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				// 全部携带判定时的历史快照号：暂停后晚到的上报依然归属合法。
				_, _ = s.Report(ResultReport{
					ResultID:        fmt.Sprintf("w%d-%d", w, i),
					Feature:         "a",
					ExpectedVersion: 2,
					ExpectedSeq:     r2.Seq,
					Failure:         true,
				})
			}
		}(w)
	}
	wg.Wait()

	if events := s.PauseEvents(); len(events) != 1 {
		t.Fatalf("exactly one pause event expected, got %d", len(events))
	}
	autoPauses := 0
	for _, h := range s.History() {
		if h.Kind == "auto-pause" {
			autoPauses++
		}
	}
	if autoPauses != 1 {
		t.Fatalf("exactly one auto-pause history record expected, got %d", autoPauses)
	}
	if notifies != 1 {
		t.Fatalf("notifier must fire exactly once under concurrency, got %d", notifies)
	}
	st, _ := s.Stats("a", 2)
	if st.Observed != workers*perWorker {
		t.Fatalf("all unique reports must be counted, got %d", st.Observed)
	}
	if got := s.CurrentRule("a").Version; got != 1 {
		t.Fatalf("current version must be stable v1, got %d", got)
	}
}

// 上报与手工回滚并发：任何执行交错下都至多暂停一次、序列号保持单调。
func TestConcurrentReportsAndRollback(t *testing.T) {
	s := NewService()
	r1 := guardPublish(t, s, "chg-cm-1", "a", 1, 100, 0, 0)
	r2 := guardPublish(t, s, "chg-cm-2", "a", 2, 100, 20, 0.0)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = s.Report(ResultReport{
					ResultID:        fmt.Sprintf("w%d-%d", w, i),
					Feature:         "a",
					ExpectedVersion: 2,
					ExpectedSeq:     r2.Seq,
					Failure:         true,
				})
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			// 回滚到 v1 快照；重复目标/过期等错误都属于合法交错，忽略。
			_, _ = s.Rollback(fmt.Sprintf("chg-cm-rb-%d", i), r1.Seq)
		}
	}()

	// 等上报协程把量打够后收尾。
	for s.CurrentRule("a") != nil && s.CurrentRule("a").Version == 2 {
		if st, _ := s.Stats("a", 2); st.Observed >= 40 {
			break
		}
	}
	close(stop)
	wg.Wait()

	if events := s.PauseEvents(); len(events) > 1 {
		t.Fatalf("at most one pause event under interleaving, got %d", len(events))
	}
	h := s.History()
	for i := 1; i < len(h); i++ {
		if h[i].Seq <= h[i-1].Seq {
			t.Fatalf("history seq not monotonic: %+v", h)
		}
	}
}
