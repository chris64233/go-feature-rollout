package featurerollout

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

// 发布一个 100% 放行的版本，可选保护配置。
func mustPublishGuarded(t *testing.T, s *Service, changeID, feature string, version int, guard *GuardConfig) *PublishResult {
	t.Helper()
	return mustPublish(t, s, changeID, []RuleInput{
		{Feature: feature, Version: version, Percentage: 100, Guard: guard},
	})
}

// 连续上报 n 条失败结果，返回最后一次的 outcome。
func reportFailures(t *testing.T, s *Service, feature string, version, n int, idPrefix string) ReportOutcome {
	t.Helper()
	var out ReportOutcome
	for i := 0; i < n; i++ {
		var err error
		out, err = s.ReportResult(feature, version, fmt.Sprintf("%s-%d", idPrefix, i), true)
		if err != nil {
			t.Fatalf("ReportResult failed: %v", err)
		}
		if !out.Counted {
			t.Fatalf("report %s-%d should be counted", idPrefix, i)
		}
	}
	return out
}

// ---------- 参数与版本归属 ----------

func TestGuardConfigValidation(t *testing.T) {
	s := NewService()

	cases := []struct {
		name  string
		guard *GuardConfig
	}{
		{"min observations zero", &GuardConfig{MinObservations: 0, MaxFailureRatio: 0.5}},
		{"min observations negative", &GuardConfig{MinObservations: -1, MaxFailureRatio: 0.5}},
		{"ratio zero", &GuardConfig{MinObservations: 1, MaxFailureRatio: 0}},
		{"ratio negative", &GuardConfig{MinObservations: 1, MaxFailureRatio: -0.1}},
		{"ratio over one", &GuardConfig{MinObservations: 1, MaxFailureRatio: 1.01}},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.Publish(fmt.Sprintf("chg-guard-%d", i), []RuleInput{
				{Feature: "a", Version: i + 1, Percentage: 50, Guard: tc.guard},
			})
			expectKind(t, err, KindParam)
		})
	}

	// 合法边界：min=1, ratio=1。
	mustPublishGuarded(t, s, "chg-guard-ok", "b", 1, &GuardConfig{MinObservations: 1, MaxFailureRatio: 1})
}

func TestReportUnknownFeatureOrVersion(t *testing.T) {
	s := NewService()
	mustPublishGuarded(t, s, "chg-1", "a", 1, nil)

	_, err := s.ReportResult("ghost", 1, "r-1", true)
	expectKind(t, err, KindNotFound)

	_, err = s.ReportResult("a", 99, "r-2", true)
	expectKind(t, err, KindVersion)

	_, err = s.ReportResult("a", 1, "", true)
	expectKind(t, err, KindParam)
}

func TestReportDeduplication(t *testing.T) {
	s := NewService()
	mustPublishGuarded(t, s, "chg-1", "a", 1, &GuardConfig{MinObservations: 2, MaxFailureRatio: 0.5})

	out, err := s.ReportResult("a", 1, "r-1", true)
	if err != nil || !out.Counted || out.Total != 1 || out.Failures != 1 {
		t.Fatalf("first report: %+v, err=%v", out, err)
	}

	// 同一 reportID 重复上报：不重复计数。
	out, err = s.ReportResult("a", 1, "r-1", true)
	if err != nil {
		t.Fatalf("duplicate report: %v", err)
	}
	if out.Counted {
		t.Fatal("duplicate report should not be counted")
	}
	if out.Total != 1 || out.Failures != 1 {
		t.Fatalf("duplicate report changed stats: %+v", out)
	}

	view, ok := s.StatsOf("a", 1)
	if !ok || view.Total != 1 || view.Failures != 1 || view.FailureRatio != 1 {
		t.Fatalf("stats view: %+v, ok=%v", view, ok)
	}
}

func TestStatsIsolatedByVersion(t *testing.T) {
	s := NewService()
	mustPublishGuarded(t, s, "chg-1", "a", 1, &GuardConfig{MinObservations: 2, MaxFailureRatio: 0.5})
	mustPublishGuarded(t, s, "chg-2", "a", 2, &GuardConfig{MinObservations: 2, MaxFailureRatio: 0.5})

	// 旧版本 v1 的失败结果只计入 v1。
	reportFailures(t, s, "a", 1, 2, "old")

	v1, _ := s.StatsOf("a", 1)
	v2, _ := s.StatsOf("a", 2)
	if v1.Total != 2 || v1.Failures != 2 {
		t.Fatalf("v1 stats: %+v", v1)
	}
	if v2.Total != 0 || v2.Failures != 0 {
		t.Fatalf("old version results leaked into v2: %+v", v2)
	}
	// v1 已非当前版本，越线也不应触发暂停（过期操作）。
	if len(s.Pauses()) != 0 {
		t.Fatalf("stale version must not be paused: %+v", s.Pauses())
	}
}

// ---------- 自动暂停 ----------

func TestAutoPauseFallbackToPreviousStable(t *testing.T) {
	s := NewService()
	mustPublishGuarded(t, s, "chg-1", "a", 1, nil) // 稳定版本，无保护
	v2res := mustPublishGuarded(t, s, "chg-2", "a", 2, &GuardConfig{MinObservations: 3, MaxFailureRatio: 0.5})

	var notifications []PauseRecord
	s.SetPauseNotifier(func(p PauseRecord) { notifications = append(notifications, p) })

	// 前两次失败：观察数不足，不暂停。
	reportFailures(t, s, "a", 2, 2, "f")
	if r := s.Evaluate("a", "anyone"); r.Version != 2 || !r.Allowed {
		t.Fatalf("before threshold: %+v", r)
	}

	// 第三次失败：3/3 > 0.5，触发暂停。
	out := reportFailures(t, s, "a", 2, 1, "f3rd")
	if !out.Paused || out.PauseSeq == 0 {
		t.Fatalf("expected pause, got %+v", out)
	}

	// 新判定回到上一稳定版本 v1。
	if r := s.Evaluate("a", "anyone"); r.Version != 1 || !r.Allowed {
		t.Fatalf("after pause should fall back to v1: %+v", r)
	}
	if cur := s.CurrentRule("a"); cur == nil || cur.Version != 1 {
		t.Fatalf("current rule after pause: %+v", cur)
	}

	// 通知恰好一次，内容完整。
	if len(notifications) != 1 {
		t.Fatalf("expected exactly 1 notification, got %d", len(notifications))
	}
	n := notifications[0]
	if n.Feature != "a" || n.Version != 2 || n.Seq != out.PauseSeq {
		t.Fatalf("notification: %+v", n)
	}
	if n.Stats.Total != 3 || n.Stats.Failures != 3 || n.Stats.FailureRatio != 1 {
		t.Fatalf("stats snapshot: %+v", n.Stats)
	}
	if len(n.Stats.ReportIDs) != 3 {
		t.Fatalf("counted report ids: %v", n.Stats.ReportIDs)
	}
	if !strings.Contains(n.Reason, "exceeded limit") {
		t.Fatalf("reason: %q", n.Reason)
	}

	// 暂停产生一条新的发布记录，序列号单调递增。
	if out.PauseSeq <= v2res.Seq {
		t.Fatalf("pause seq %d should be greater than publish seq %d", out.PauseSeq, v2res.Seq)
	}
	hist := s.History()
	last := hist[len(hist)-1]
	if last.Kind != "pause" || last.Seq != out.PauseSeq {
		t.Fatalf("history tail: %+v", last)
	}
}

func TestPauseOnlyOnce(t *testing.T) {
	s := NewService()
	mustPublishGuarded(t, s, "chg-1", "a", 1, &GuardConfig{MinObservations: 1, MaxFailureRatio: 0.5})

	notifyCount := 0
	s.SetPauseNotifier(func(PauseRecord) { notifyCount++ })

	out := reportFailures(t, s, "a", 1, 1, "f")
	if !out.Paused {
		t.Fatal("expected pause")
	}
	seqAfterPause := s.CurrentSnapshotSeq()

	// 重复上报（新 reportID）：计数但不再暂停。
	out, err := s.ReportResult("a", 1, "f-extra", true)
	if err != nil || !out.Counted || out.Paused {
		t.Fatalf("report after pause: %+v, err=%v", out, err)
	}
	// 重复上报（旧 reportID）：不计数也不暂停。
	out, err = s.ReportResult("a", 1, "f-0", true)
	if err != nil || out.Counted || out.Paused {
		t.Fatalf("duplicate report after pause: %+v, err=%v", out, err)
	}
	// 重复检查：不再产生状态变化。
	pause, err := s.CheckGuard("a", 1)
	if err != nil || pause != nil {
		t.Fatalf("re-check should be a no-op: %+v, err=%v", pause, err)
	}

	if notifyCount != 1 {
		t.Fatalf("notifier called %d times, want 1", notifyCount)
	}
	if len(s.Pauses()) != 1 {
		t.Fatalf("pauses: %+v", s.Pauses())
	}
	if s.CurrentSnapshotSeq() != seqAfterPause {
		t.Fatal("repeated reports/checks changed state")
	}
}

func TestMinObservationsGate(t *testing.T) {
	s := NewService()
	mustPublishGuarded(t, s, "chg-1", "a", 1, &GuardConfig{MinObservations: 3, MaxFailureRatio: 0.5})

	// 观察数不足时，即使 100% 失败也不暂停。
	reportFailures(t, s, "a", 1, 2, "f")
	if len(s.Pauses()) != 0 {
		t.Fatal("paused before min observations")
	}

	// 达到观察数但比例未越线（2/3 ≈ 0.667 > 0.5 越线，改用 1/3）。
	s2 := NewService()
	mustPublishGuarded(t, s2, "chg-1", "a", 1, &GuardConfig{MinObservations: 3, MaxFailureRatio: 0.5})
	reportFailures(t, s2, "a", 1, 1, "f")
	if _, err := s2.ReportResult("a", 1, "ok-1", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.ReportResult("a", 1, "ok-2", false); err != nil {
		t.Fatal(err)
	}
	if len(s2.Pauses()) != 0 {
		t.Fatal("paused while ratio within limit")
	}
}

func TestPauseWithoutPreviousVersion(t *testing.T) {
	s := NewService()
	mustPublishGuarded(t, s, "chg-1", "a", 1, &GuardConfig{MinObservations: 1, MaxFailureRatio: 0.5})
	reportFailures(t, s, "a", 1, 1, "f")

	// 没有可回退的旧版本：功能整体下线。
	if r := s.Evaluate("a", "anyone"); r.Allowed || r.Version != 0 {
		t.Fatalf("feature should be fully off: %+v", r)
	}
	if cur := s.CurrentRule("a"); cur != nil {
		t.Fatalf("current rule should be nil: %+v", cur)
	}
}

func TestPauseSkipsPausedFallback(t *testing.T) {
	s := NewService()
	guard := &GuardConfig{MinObservations: 1, MaxFailureRatio: 0.5}
	mustPublishGuarded(t, s, "chg-1", "a", 1, nil)
	mustPublishGuarded(t, s, "chg-2", "a", 2, guard)
	mustPublishGuarded(t, s, "chg-3", "a", 3, guard)

	// 先暂停 v2（此时当前版本是 v3，v2 已过期，暂停被拒绝）——
	// 改为直接暂停当前版本 v3，回退应跳过已暂停版本。
	reportFailures(t, s, "a", 3, 1, "f3")
	if r := s.Evaluate("a", "anyone"); r.Version != 2 {
		t.Fatalf("after pausing v3 should fall back to v2: %+v", r)
	}
	// v2 成为当前版本后越线，暂停 v2，回退到 v1。
	reportFailures(t, s, "a", 2, 1, "f2")
	if r := s.Evaluate("a", "anyone"); r.Version != 1 {
		t.Fatalf("after pausing v2 should fall back to v1: %+v", r)
	}
	if len(s.Pauses()) != 2 {
		t.Fatalf("pauses: %+v", s.Pauses())
	}
}

// ---------- 既有判定保留 ----------

func TestPauseKeepsExistingDecisions(t *testing.T) {
	s := NewService()
	mustPublishGuarded(t, s, "chg-1", "a", 1, nil)
	v2 := mustPublishGuarded(t, s, "chg-2", "a", 2, &GuardConfig{MinObservations: 1, MaxFailureRatio: 0.5})

	// 暂停前在 v2 快照上的判定。
	before := s.Evaluate("a", "anyone")
	if before.Version != 2 || !before.Allowed {
		t.Fatalf("before pause: %+v", before)
	}

	reportFailures(t, s, "a", 2, 1, "f")

	// 既有判定可原样回放：历史快照不可变。
	replay, err := s.EvalAt(v2.Seq, "a", "anyone")
	if err != nil {
		t.Fatal(err)
	}
	if replay != before {
		t.Fatalf("replay %+v != original %+v", replay, before)
	}
	// 新判定回到 v1。
	if r := s.Evaluate("a", "anyone"); r.Version != 1 {
		t.Fatalf("after pause: %+v", r)
	}
}

// ---------- 与人工操作的并发/过期 ----------

func TestStalePauseRejectedAfterRepublish(t *testing.T) {
	s := NewService()
	mustPublishGuarded(t, s, "chg-1", "a", 1, &GuardConfig{MinObservations: 2, MaxFailureRatio: 0.5})
	// 人工重新发布 v2 之后，v1 的越线统计不再触发暂停。
	mustPublishGuarded(t, s, "chg-2", "a", 2, nil)

	reportFailures(t, s, "a", 1, 3, "f")
	if len(s.Pauses()) != 0 {
		t.Fatalf("stale pause should be rejected: %+v", s.Pauses())
	}
	if r := s.Evaluate("a", "anyone"); r.Version != 2 {
		t.Fatalf("current version should stay v2: %+v", r)
	}
}

func TestStalePauseRejectedAfterRollback(t *testing.T) {
	s := NewService()
	v1 := mustPublishGuarded(t, s, "chg-1", "a", 1, nil)
	mustPublishGuarded(t, s, "chg-2", "a", 2, &GuardConfig{MinObservations: 2, MaxFailureRatio: 0.5})
	// 人工回滚到 v1 之后，v2 的越线统计不再触发暂停。
	if _, err := s.Rollback("chg-3", v1.Seq); err != nil {
		t.Fatal(err)
	}

	reportFailures(t, s, "a", 2, 3, "f")
	if len(s.Pauses()) != 0 {
		t.Fatalf("stale pause should be rejected: %+v", s.Pauses())
	}
	if r := s.Evaluate("a", "anyone"); r.Version != 1 {
		t.Fatalf("current version should stay v1: %+v", r)
	}
}

func TestRollbackToPausedVersionRejected(t *testing.T) {
	s := NewService()
	mustPublishGuarded(t, s, "chg-1", "a", 1, nil)
	v2 := mustPublishGuarded(t, s, "chg-2", "a", 2, &GuardConfig{MinObservations: 1, MaxFailureRatio: 0.5})

	reportFailures(t, s, "a", 2, 1, "f") // 暂停 v2，回到 v1

	// 回滚到 v2 生效的快照属于过期操作：v2 已被暂停。
	_, err := s.Rollback("chg-3", v2.Seq)
	expectKind(t, err, KindVersion)

	// 但发布更新的版本（v3）恢复放量是允许的，统计重新计数。
	mustPublishGuarded(t, s, "chg-4", "a", 3, nil)
	if r := s.Evaluate("a", "anyone"); r.Version != 3 || !r.Allowed {
		t.Fatalf("republish should resume rollout: %+v", r)
	}
	if view, _ := s.StatsOf("a", 3); view.Total != 0 {
		t.Fatalf("v3 stats should start fresh: %+v", view)
	}
}

// ---------- 并发 ----------

func TestConcurrentReportsPauseOnce(t *testing.T) {
	s := NewService()
	mustPublishGuarded(t, s, "chg-1", "a", 1, nil)
	mustPublishGuarded(t, s, "chg-2", "a", 2, &GuardConfig{MinObservations: 50, MaxFailureRatio: 0.5})

	var mu sync.Mutex
	notifyCount := 0
	s.SetPauseNotifier(func(PauseRecord) {
		mu.Lock()
		notifyCount++
		mu.Unlock()
	})

	const reporters = 8
	const perReporter = 25 // 共 200 条唯一上报，全部失败
	var wg sync.WaitGroup
	for w := 0; w < reporters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perReporter; i++ {
				id := fmt.Sprintf("w%d-%d", w, i)
				if _, err := s.ReportResult("a", 2, id, true); err != nil {
					t.Errorf("report: %v", err)
					return
				}
				// 并发重复上报与检查：不得产生额外暂停。
				_, _ = s.ReportResult("a", 2, id, true)
				_, _ = s.CheckGuard("a", 2)
			}
		}(w)
	}
	wg.Wait()

	view, _ := s.StatsOf("a", 2)
	if view.Total != reporters*perReporter || view.Failures != reporters*perReporter {
		t.Fatalf("stats: %+v", view)
	}
	if !view.Paused {
		t.Fatal("expected pause")
	}
	if pauses := s.Pauses(); len(pauses) != 1 {
		t.Fatalf("expected exactly 1 pause, got %d", len(pauses))
	}
	mu.Lock()
	defer mu.Unlock()
	if notifyCount != 1 {
		t.Fatalf("notifier called %d times, want 1", notifyCount)
	}
}

func TestConcurrentPauseAndRollback(t *testing.T) {
	// 自动暂停与人工回滚/重新发布并发：状态始终一致，
	// 暂停只在版本仍为当前版本时生效，过期操作被拒绝。
	for trial := 0; trial < 20; trial++ {
		s := NewService()
		v1 := mustPublishGuarded(t, s, "chg-1", "a", 1, nil)
		mustPublishGuarded(t, s, "chg-2", "a", 2, &GuardConfig{MinObservations: 1, MaxFailureRatio: 0.5})

		var wg sync.WaitGroup
		wg.Add(3)
		go func() { defer wg.Done(); _, _ = s.ReportResult("a", 2, "f-1", true) }()
		go func() { defer wg.Done(); _, _ = s.Rollback("chg-3", v1.Seq) }()
		go func() {
			defer wg.Done()
			_, _ = s.Publish("chg-4", []RuleInput{{Feature: "a", Version: 3, Percentage: 100}})
		}()
		wg.Wait()

		pauses := s.Pauses()
		if len(pauses) > 1 {
			t.Fatalf("trial %d: at most one pause, got %d", trial, len(pauses))
		}
		// 无论时序如何，当前版本绝不能是已暂停的 v2。
		if cur := s.CurrentRule("a"); cur != nil && cur.Version == 2 && len(pauses) == 1 {
			t.Fatalf("trial %d: paused version is still current", trial)
		}
	}
}
