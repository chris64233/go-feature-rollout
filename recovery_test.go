package featurerollout

import (
	"fmt"
	"sync"
	"testing"
)

// publishAtZero 制造"规则目标被保护机制拦下"的状态：
// 当前规则比例 0%，恢复只能一批一批放量，不能直接恢复到 100%。
func publishAtZero(t *testing.T, s *Service, changeID, feature string, version int) {
	t.Helper()
	mustPublish(t, s, changeID, []RuleInput{{Feature: feature, Version: version, Percentage: 0}})
	if pct, ok := s.EffectivePercentage(feature); !ok || pct != 0 {
		t.Fatalf("setup: effective pct=(%d,%v), want 0", pct, ok)
	}
}

// fillWindow 向当前窗口收齐 ObserveCount 个结果，前 failures 个为失败。
func fillWindow(t *testing.T, s *Service, changeID string, plan *RecoveryPlan, batch, failures int) *RecoveryPlan {
	t.Helper()
	b := plan.Batches[batch]
	attempt := b.Windows[len(b.Windows)-1].Attempt
	for i := 0; i < b.ObserveCount; i++ {
		r, err := s.ReportObservation(changeID, ObservationInput{
			Batch: batch, Attempt: attempt, Success: i >= failures,
		})
		if err != nil {
			t.Fatalf("ReportObservation(batch=%d i=%d): %v", batch, i, err)
		}
		last := i == b.ObserveCount-1
		if last != r.WindowClosed {
			t.Fatalf("batch=%d i=%d: WindowClosed=%v want %v", batch, i, r.WindowClosed, last)
		}
	}
	got, err := s.GetRecovery(changeID)
	if err != nil {
		t.Fatalf("GetRecovery: %v", err)
	}
	return got
}

func allowedUsers(t *testing.T, s *Service, feature string, n int, wantPct int) int {
	t.Helper()
	count := 0
	for i := 0; i < n; i++ {
		r := s.Evaluate(feature, fmt.Sprintf("user-%d", i))
		if r.Percentage != wantPct {
			t.Fatalf("Evaluate pct=%d, want %d", r.Percentage, wantPct)
		}
		if r.Allowed {
			count++
		}
	}
	return count
}

func sampleBatches() []RecoveryBatchInput {
	return []RecoveryBatchInput{
		{Percentage: 10, ObserveCount: 3, FailureThreshold: 1},
		{Percentage: 50, ObserveCount: 2, FailureThreshold: 0},
		{Percentage: 100, ObserveCount: 1, FailureThreshold: 0},
	}
}

// ---------- 需求 1：计划写明版本/比例/观察数量/失败阈值；上一批没观察完不能开下一批 ----------

func TestRecoveryPlanValidation(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-v-1", []RuleInput{{Feature: "a", Version: 1, Percentage: 0}})

	cases := []struct {
		name     string
		changeID string
		feature  string
		version  int
		batches  []RecoveryBatchInput
		kind     ErrorKind
	}{
		{"empty change id", "", "a", 1, []RecoveryBatchInput{{10, 1, 0}}, KindParam},
		{"empty feature", "c", "", 1, []RecoveryBatchInput{{10, 1, 0}}, KindParam},
		{"no batches", "c", "a", 1, nil, KindParam},
		{"negative pct", "c", "a", 1, []RecoveryBatchInput{{-1, 1, 0}}, KindParam},
		{"pct over 100", "c", "a", 1, []RecoveryBatchInput{{101, 1, 0}}, KindParam},
		{"observe count zero", "c", "a", 1, []RecoveryBatchInput{{10, 0, 0}}, KindParam},
		{"negative threshold", "c", "a", 1, []RecoveryBatchInput{{10, 2, -1}}, KindParam},
		{"threshold equals observe", "c", "a", 1, []RecoveryBatchInput{{10, 2, 2}}, KindParam},
		{"pct not increasing", "c", "a", 1,
			[]RecoveryBatchInput{{20, 1, 0}, {20, 1, 0}}, KindParam},
		{"first pct not above safe", "c", "a", 1,
			[]RecoveryBatchInput{{0, 1, 0}}, KindParam},
		{"feature missing", "c", "ghost", 1,
			[]RecoveryBatchInput{{10, 1, 0}}, KindNotFound},
		{"version mismatch", "c", "a", 2,
			[]RecoveryBatchInput{{10, 1, 0}}, KindConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.StartRecovery(tc.changeID, tc.feature, tc.version, tc.batches)
			expectKind(t, err, tc.kind)
		})
	}
}

func TestRecoveryBatchLifecycle(t *testing.T) {
	s := NewService()
	publishAtZero(t, s, "chg-bl-1", "a", 1)

	plan, err := s.StartRecovery("rcv-bl", "a", 1, sampleBatches())
	if err != nil {
		t.Fatalf("StartRecovery: %v", err)
	}
	if plan.Version != 1 || plan.Status != RecoveryObserving ||
		plan.SafePercentage != 0 || plan.PausedSafePct != 0 || plan.CurrentBatch != 0 {
		t.Fatalf("unexpected initial plan: %+v", plan)
	}
	for i, b := range plan.Batches {
		want := sampleBatches()[i]
		if b.Percentage != want.Percentage || b.ObserveCount != want.ObserveCount ||
			b.FailureThreshold != want.FailureThreshold {
			t.Fatalf("batch %d plan values lost: %+v", i, b)
		}
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 10 {
		t.Fatalf("first batch pct = %d, want 10", pct)
	}

	// 上一批没观察完不能开下一批。
	if _, err := s.AdvanceRecovery("rcv-bl"); err == nil {
		t.Fatal("advance during observing must fail")
	} else {
		expectKind(t, err, KindConflict)
	}

	// 观察中途比例保持不变。
	if _, err := s.ReportObservation("rcv-bl",
		ObservationInput{Batch: 0, Attempt: 0, Success: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReportObservation("rcv-bl",
		ObservationInput{Batch: 0, Attempt: 0, Success: false}); err != nil {
		t.Fatal(err)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 10 {
		t.Fatalf("mid-observation pct changed: %d", pct)
	}

	// 收齐后进入 ready，但比例不自动提高，必须显式推进。
	r, err := s.ReportObservation("rcv-bl",
		ObservationInput{Batch: 0, Attempt: 0, Success: true})
	if err != nil {
		t.Fatal(err)
	}
	if !r.WindowClosed || !r.WindowPassed {
		t.Fatalf("receipt = %+v", r)
	}
	plan, _ = s.GetRecovery("rcv-bl")
	if plan.Status != RecoveryReady || plan.SafePercentage != 10 {
		t.Fatalf("ready plan = %+v", plan)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 10 {
		t.Fatalf("ready pct = %d, want 10", pct)
	}

	plan, err = s.AdvanceRecovery("rcv-bl")
	if err != nil {
		t.Fatalf("AdvanceRecovery: %v", err)
	}
	if plan.CurrentBatch != 1 || plan.Status != RecoveryObserving {
		t.Fatalf("after advance: %+v", plan)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 50 {
		t.Fatalf("second batch pct = %d, want 50", pct)
	}
	plan = fillWindow(t, s, "rcv-bl", plan, 1, 0)

	plan, _ = s.AdvanceRecovery("rcv-bl")
	if pct, _ := s.EffectivePercentage("a"); pct != 100 {
		t.Fatalf("final batch pct = %d, want 100", pct)
	}
	plan = fillWindow(t, s, "rcv-bl", plan, 2, 0)
	if plan.Status != RecoveryCompleted || plan.SafePercentage != 100 {
		t.Fatalf("completed plan = %+v", plan)
	}

	// 终态不能再推进/暂停；每批证据完整。
	if _, err := s.AdvanceRecovery("rcv-bl"); err == nil {
		t.Fatal("advance after completion must fail")
	}
	if _, err := s.PauseRecovery("rcv-bl", ""); err == nil {
		t.Fatal("pause after completion must fail")
	}
	for i, b := range plan.Batches {
		if b.ActualPercentage != b.Percentage || len(b.Windows) != 1 ||
			b.Windows[0].Result != WindowPassed ||
			b.Windows[0].Observed != b.ObserveCount {
			t.Fatalf("batch %d evidence broken: %+v", i, b)
		}
	}
}

// ---------- 需求 3：同一恢复请求返回原计划；内容/版本变化报冲突 ----------

func TestRecoveryIdempotentReplay(t *testing.T) {
	s := NewService()
	publishAtZero(t, s, "chg-id-1", "a", 1)
	batches := []RecoveryBatchInput{{Percentage: 10, ObserveCount: 2, FailureThreshold: 0}}

	p1, err := s.StartRecovery("rcv-id", "a", 1, batches)
	if err != nil {
		t.Fatal(err)
	}
	// 推进计划到终态，再用同一请求重放：返回的是原计划当前状态，不会重新开窗/放量。
	p1 = fillWindow(t, s, "rcv-id", p1, 0, 0)
	if p1.Status != RecoveryCompleted {
		t.Fatalf("precondition status=%s", p1.Status)
	}
	p2, err := s.StartRecovery("rcv-id", "a", 1, batches)
	if err != nil {
		t.Fatalf("replay must be idempotent: %v", err)
	}
	if p2.ID != p1.ID || p2.Status != RecoveryCompleted ||
		p2.SafePercentage != 10 || p2.CurrentBatch != 0 ||
		len(p2.Batches[0].Windows) != 1 {
		t.Fatalf("replay changed plan: %+v vs %+v", p1, p2)
	}

	// 同号改批次内容 -> 冲突。
	if _, err := s.StartRecovery("rcv-id", "a", 1,
		[]RecoveryBatchInput{{Percentage: 20, ObserveCount: 2, FailureThreshold: 0}}); err == nil {
		t.Fatal("different batches under same changeID must conflict")
	} else {
		expectKind(t, err, KindConflict)
	}
	if _, err := s.StartRecovery("rcv-id", "a", 1,
		[]RecoveryBatchInput{{Percentage: 10, ObserveCount: 3, FailureThreshold: 0}}); err == nil {
		t.Fatal("different observe count under same changeID must conflict")
	} else {
		expectKind(t, err, KindConflict)
	}

	// 同功能进行中：换新 changeID 也不能再起一个。
	publishAtZero(t, s, "chg-id-2", "b", 1)
	_, _ = s.StartRecovery("rcv-id-b1", "b", 1,
		[]RecoveryBatchInput{{Percentage: 10, ObserveCount: 1, FailureThreshold: 0}})
	_, err = s.StartRecovery("rcv-id-b2", "b", 1,
		[]RecoveryBatchInput{{Percentage: 20, ObserveCount: 1, FailureThreshold: 0}})
	expectKind(t, err, KindConflict)

	// 未知恢复号 -> not_found。
	for _, op := range []func() error{
		func() error { _, e := s.PauseRecovery("missing", ""); return e },
		func() error { _, e := s.ResumeRecovery("missing"); return e },
		func() error { _, e := s.AdvanceRecovery("missing"); return e },
		func() error { _, e := s.CancelRecovery("missing", ""); return e },
		func() error {
			_, e := s.ReportObservation("missing", ObservationInput{Batch: 0})
			return e
		},
	} {
		expectKind(t, op(), KindNotFound)
	}
	if _, err := s.GetRecovery("missing"); err == nil {
		t.Fatal("GetRecovery missing must fail")
	} else {
		expectKind(t, err, KindNotFound)
	}
}

// 暂停/取消操作自身幂等，不重复产生状态。
func TestPauseCancelIdempotent(t *testing.T) {
	s := NewService()
	publishAtZero(t, s, "chg-pi-1", "a", 1)
	_, _ = s.StartRecovery("rcv-pi", "a", 1,
		[]RecoveryBatchInput{{Percentage: 10, ObserveCount: 4, FailureThreshold: 0}})

	p1, err := s.PauseRecovery("rcv-pi", "hold")
	if err != nil {
		t.Fatal(err)
	}
	p2, err := s.PauseRecovery("rcv-pi", "hold again")
	if err != nil || p2.Status != RecoveryPaused {
		t.Fatalf("pause idempotent: %v %v", p2, err)
	}
	if p1.UpdatedAt != p2.UpdatedAt || len(p2.Batches[0].Windows) != 1 {
		t.Fatalf("repeated pause must not alter state: %+v vs %+v", p1, p2)
	}

	_, _ = s.ResumeRecovery("rcv-pi")
	_, _ = s.CancelRecovery("rcv-cx", "") // 不存在的计划
	c1, err := s.CancelRecovery("rcv-pi", "stop")
	if err != nil {
		t.Fatal(err)
	}
	c2, err := s.CancelRecovery("rcv-pi", "stop again")
	if err != nil || c2.Status != RecoveryCancelled {
		t.Fatalf("cancel idempotent: %v %v", c2, err)
	}
	if c1.UpdatedAt != c2.UpdatedAt {
		t.Fatal("repeated cancel must not alter state")
	}
}

// ---------- 需求 2、6：暂停后重新开窗；旧批次/旧窗口结果不能推进新批次、不能重复计数 ----------

func TestPauseResumeFreshWindow(t *testing.T) {
	s := NewService()
	publishAtZero(t, s, "chg-pr-1", "a", 1)
	plan, _ := s.StartRecovery("rcv-pr", "a", 1, []RecoveryBatchInput{
		{Percentage: 30, ObserveCount: 4, FailureThreshold: 0},
		{Percentage: 80, ObserveCount: 1, FailureThreshold: 0},
	})

	// 观察到一半（2/4，含 1 失败）时人工暂停。
	_, _ = s.ReportObservation("rcv-pr",
		ObservationInput{Batch: 0, Attempt: 0, Success: true})
	_, _ = s.ReportObservation("rcv-pr",
		ObservationInput{Batch: 0, Attempt: 0, Success: false})
	plan, err := s.PauseRecovery("rcv-pr", "operator hold")
	if err != nil || plan.Status != RecoveryPaused {
		t.Fatalf("pause: %v %v", plan, err)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 0 {
		t.Fatalf("paused pct = %d, want safe 0", pct)
	}
	win0 := plan.Batches[0].Windows[0]
	if win0.Result != WindowInterrupted || win0.Observed != 2 || win0.Failures != 1 ||
		win0.ClosedAt.IsZero() || win0.BlockReason == "" {
		t.Fatalf("interrupted window evidence: %+v", win0)
	}

	// 暂停态不能推进/重复恢复。
	if _, err := s.AdvanceRecovery("rcv-pr"); err == nil {
		t.Fatal("advance while paused must fail")
	}

	// 恢复：同批次开全新窗口 attempt=1，计数归零。
	plan, err = s.ResumeRecovery("rcv-pr")
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	win1 := plan.Batches[0].Windows[1]
	if win1.Attempt != 1 || win1.Observed != 0 || win1.Failures != 0 || win1.Result != WindowOpen {
		t.Fatalf("fresh window: %+v", win1)
	}

	// 旧窗口 attempt=0 迟到的失败结果必须被拒绝，只计入 LateResults，
	// 不影响新窗口（需求 6：失败结果归属于当批次当次开窗）。
	_, err = s.ReportObservation("rcv-pr",
		ObservationInput{Batch: 0, Attempt: 0, Success: false})
	expectKind(t, err, KindConflict)
	plan, _ = s.GetRecovery("rcv-pr")
	if plan.Batches[0].Windows[0].LateResults != 1 {
		t.Fatalf("late result not recorded: %+v", plan.Batches[0].Windows[0])
	}

	// 新窗口必须自己收齐 4 个结果，旧结果一个都不算。
	for i := 0; i < 4; i++ {
		r, err := s.ReportObservation("rcv-pr",
			ObservationInput{Batch: 0, Attempt: 1, Success: true})
		if err != nil {
			t.Fatalf("fresh report %d: %v", i, err)
		}
		if i == 3 && (!r.WindowClosed || !r.WindowPassed) {
			t.Fatalf("fresh window should pass: %+v", r)
		}
	}
	plan, _ = s.GetRecovery("rcv-pr")
	if plan.Status != RecoveryReady || plan.SafePercentage != 30 {
		t.Fatalf("plan after fresh window: %+v", plan)
	}
	win1 = plan.Batches[0].Windows[1]
	if win1.Observed != 4 || win1.Failures != 0 {
		t.Fatalf("fresh window counts: %+v", win1)
	}
}

// ready 状态暂停再恢复：已通过的安全比例保留，但恢复后必须重新开窗验证，
// 不会直接跳到下一批。
func TestPauseAtReadyResumeReopensSafeBatch(t *testing.T) {
	s := NewService()
	publishAtZero(t, s, "chg-prr-1", "a", 1)
	plan, _ := s.StartRecovery("rcv-prr", "a", 1, []RecoveryBatchInput{
		{Percentage: 20, ObserveCount: 1, FailureThreshold: 0},
		{Percentage: 60, ObserveCount: 1, FailureThreshold: 0},
	})
	plan = fillWindow(t, s, "rcv-prr", plan, 0, 0)

	paused, err := s.PauseRecovery("rcv-prr", "hold")
	if err != nil || paused.Status != RecoveryPaused {
		t.Fatalf("pause at ready: %v %v", paused, err)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 20 {
		t.Fatalf("paused safe pct = %d, want 20", pct)
	}
	plan, err = s.ResumeRecovery("rcv-prr")
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if plan.CurrentBatch != 0 || plan.Status != RecoveryObserving {
		t.Fatalf("resumed plan = %+v", plan)
	}
	if wins := plan.Batches[0].Windows; len(wins) != 2 || wins[1].Attempt != 1 {
		t.Fatalf("expected re-observation window: %+v", wins)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 20 {
		t.Fatalf("resumed pct = %d, want safe 20 re-observed", pct)
	}
}

// 旧批次/错轮次/关闭后的结果全部拒绝，不能推进新批次、不能重复放量。
func TestStaleObservationsRejected(t *testing.T) {
	s := NewService()
	publishAtZero(t, s, "chg-so-1", "a", 1)
	plan, _ := s.StartRecovery("rcv-so", "a", 1, []RecoveryBatchInput{
		{Percentage: 10, ObserveCount: 2, FailureThreshold: 0},
		{Percentage: 50, ObserveCount: 2, FailureThreshold: 0},
		{Percentage: 90, ObserveCount: 2, FailureThreshold: 0},
	})
	plan = fillWindow(t, s, "rcv-so", plan, 0, 0)
	plan, _ = s.AdvanceRecovery("rcv-so")

	// 旧批次迟到结果 -> 冲突，留痕到旧批次窗口。
	_, err := s.ReportObservation("rcv-so",
		ObservationInput{Batch: 0, Attempt: 0, Success: false})
	expectKind(t, err, KindConflict)
	// 越界批次 -> 参数错误。
	_, err = s.ReportObservation("rcv-so",
		ObservationInput{Batch: 9, Attempt: 0, Success: true})
	expectKind(t, err, KindParam)
	// 当前批次错轮次 -> 冲突。
	_, err = s.ReportObservation("rcv-so",
		ObservationInput{Batch: 1, Attempt: 7, Success: true})
	expectKind(t, err, KindConflict)

	// 窗口收齐后再上报：冲突，不计入 Observed，只加 LateResults。
	plan = fillWindow(t, s, "rcv-so", plan, 1, 0)
	if plan.Status != RecoveryReady {
		t.Fatalf("status = %s, want ready", plan.Status)
	}
	r, err := s.ReportObservation("rcv-so",
		ObservationInput{Batch: 1, Attempt: 0, Success: true})
	if err == nil || r != nil {
		t.Fatalf("post-close report must fail: %+v %v", r, err)
	}
	plan, _ = s.GetRecovery("rcv-so")
	win := plan.Batches[1].Windows[0]
	if win.Observed != 2 || win.LateResults != 2 {
		t.Fatalf("closed window altered: observed=%d late=%d", win.Observed, win.LateResults)
	}
	// 旧批次证据未被迟到结果污染。
	if w0 := plan.Batches[0].Windows[0]; w0.Observed != 2 || w0.Failures != 0 {
		t.Fatalf("old batch polluted: %+v", w0)
	}
}

// ---------- 需求 3、4：恢复失败保持当前安全比例；实际比例/观察结果/阻断原因留痕 ----------

func TestRecoveryFailureHoldsSafePercentage(t *testing.T) {
	s := NewService()
	publishAtZero(t, s, "chg-fl-1", "a", 1)
	plan, _ := s.StartRecovery("rcv-fl", "a", 1, []RecoveryBatchInput{
		{Percentage: 10, ObserveCount: 1, FailureThreshold: 0},
		{Percentage: 40, ObserveCount: 2, FailureThreshold: 0},
	})
	plan = fillWindow(t, s, "rcv-fl", plan, 0, 0)
	plan, _ = s.AdvanceRecovery("rcv-fl")
	allowedUsers(t, s, "a", 500, 40) // 当前已放量 40%

	// 第二批窗口出现失败：阻断，比例退回安全比例 10%。
	_, err := s.ReportObservation("rcv-fl",
		ObservationInput{Batch: 1, Attempt: 0, Success: false})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReportObservation("rcv-fl",
		ObservationInput{Batch: 1, Attempt: 0, Success: true}); err != nil {
		t.Fatal(err)
	}
	plan, _ = s.GetRecovery("rcv-fl")
	if plan.Status != RecoveryBlocked || plan.SafePercentage != 10 {
		t.Fatalf("blocked plan = %+v", plan)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 10 {
		t.Fatalf("blocked pct = %d, want safe 10", pct)
	}

	// 证据留存：第二批实际比例=安全比例，窗口 blocked，窗口级与计划级阻断原因都在；
	// 第一批通过证据保留。
	b1 := plan.Batches[1]
	if b1.ActualPercentage != 10 || b1.Windows[0].Result != WindowBlocked ||
		b1.Windows[0].Observed != 2 || b1.Windows[0].Failures != 1 ||
		b1.Windows[0].BlockReason == "" || plan.BlockReason == "" {
		t.Fatalf("block evidence missing: batch=%+v planReason=%q", b1, plan.BlockReason)
	}
	if b0 := plan.Batches[0]; b0.ActualPercentage != 10 ||
		b0.Windows[0].Result != WindowPassed {
		t.Fatalf("passed batch evidence lost: %+v", b0)
	}

	// 阻断后上报/推进/恢复/取消全部被拒。
	if _, err := s.ReportObservation("rcv-fl",
		ObservationInput{Batch: 1, Attempt: 0, Success: true}); err == nil {
		t.Fatal("report after block must fail")
	}
	_, err = s.AdvanceRecovery("rcv-fl")
	expectKind(t, err, KindConflict)
	_, err = s.ResumeRecovery("rcv-fl")
	expectKind(t, err, KindConflict)
	_, err = s.CancelRecovery("rcv-fl", "")
	expectKind(t, err, KindConflict)

	// 阈值边界严格：阈值 1 时恰好 1 个失败通过，第 2 个失败才阻断。
	s2 := NewService()
	publishAtZero(t, s2, "chg-fl-2", "a", 1)
	_, _ = s2.StartRecovery("rcv-fl2", "a", 1, []RecoveryBatchInput{
		{Percentage: 30, ObserveCount: 3, FailureThreshold: 1},
	})
	for i, ok := range []bool{true, false, false} {
		rec, err := s2.ReportObservation("rcv-fl2",
			ObservationInput{Batch: 0, Attempt: 0, Success: ok})
		if err != nil {
			t.Fatalf("report %d: %v", i, err)
		}
		if i == 2 && (!rec.WindowClosed || rec.WindowPassed || !rec.Blocked) {
			t.Fatalf("2 failures vs threshold 1 must block: %+v", rec)
		}
	}
	p, _ := s2.GetRecovery("rcv-fl2")
	if p.SafePercentage != 0 || p.Batches[0].ActualPercentage != 0 {
		t.Fatalf("blocked baseline must stay 0: %+v", p)
	}

	// 阻断后可以从安全比例重新提恢复，新计划首批高于安全比例即可。
	retry, err := s.StartRecovery("rcv-fl-retry", "a", 1,
		[]RecoveryBatchInput{{Percentage: 20, ObserveCount: 1, FailureThreshold: 0}})
	if err != nil {
		t.Fatalf("retry after block: %v", err)
	}
	if retry.PausedSafePct != 10 || retry.SafePercentage != 10 {
		t.Fatalf("retry baseline = %d, want 10", retry.PausedSafePct)
	}
	// 旧计划被取代但证据保留。
	old, _ := s.GetRecovery("rcv-fl")
	if old.Status != RecoverySuperseded || old.BlockReason == "" {
		t.Fatalf("superseded old plan: %+v", old)
	}
}

// ---------- 需求 5：取消保留已完成批次，剩余流量停在最后安全比例 ----------

func TestCancelKeepsCompletedBatches(t *testing.T) {
	s := NewService()
	publishAtZero(t, s, "chg-cx-1", "a", 1)
	plan, _ := s.StartRecovery("rcv-cx", "a", 1, []RecoveryBatchInput{
		{Percentage: 10, ObserveCount: 1, FailureThreshold: 0},
		{Percentage: 40, ObserveCount: 2, FailureThreshold: 0},
		{Percentage: 100, ObserveCount: 1, FailureThreshold: 0},
	})
	plan = fillWindow(t, s, "rcv-cx", plan, 0, 0)
	plan, _ = s.AdvanceRecovery("rcv-cx")
	// 第二批只观察到一半（1/2）即取消。
	if _, err := s.ReportObservation("rcv-cx",
		ObservationInput{Batch: 1, Attempt: 0, Success: true}); err != nil {
		t.Fatal(err)
	}
	plan, err := s.CancelRecovery("rcv-cx", "rollback decision")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if plan.Status != RecoveryCancelled || plan.SafePercentage != 10 {
		t.Fatalf("cancelled plan = %+v", plan)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 10 {
		t.Fatalf("after cancel pct = %d, want last safe 10", pct)
	}
	// 第一批通过证据保留；第二批中断窗口与部分结果保留；第三批从未打开。
	if plan.Batches[0].ActualPercentage != 10 ||
		plan.Batches[0].Windows[0].Result != WindowPassed {
		t.Fatalf("completed batch lost: %+v", plan.Batches[0])
	}
	w := plan.Batches[1].Windows[0]
	if w.Result != WindowInterrupted || w.Observed != 1 || w.BlockReason == "" {
		t.Fatalf("interrupted evidence: %+v", w)
	}
	if plan.Batches[2].ActualPercentage != 0 || len(plan.Batches[2].Windows) != 0 {
		t.Fatalf("unopened batch must stay empty: %+v", plan.Batches[2])
	}

	// 取消后推进/恢复/上报被拒。
	if _, err := s.AdvanceRecovery("rcv-cx"); err == nil {
		t.Fatal("advance after cancel must fail")
	}
	if _, err := s.ResumeRecovery("rcv-cx"); err == nil {
		t.Fatal("resume after cancel must fail")
	}
	if _, err := s.ReportObservation("rcv-cx",
		ObservationInput{Batch: 1, Attempt: 0, Success: true}); err == nil {
		t.Fatal("report after cancel must fail")
	}

	// 可以从最后安全比例 10% 重新提恢复。
	p2, err := s.StartRecovery("rcv-cx2", "a", 1,
		[]RecoveryBatchInput{{Percentage: 30, ObserveCount: 1, FailureThreshold: 0}})
	if err != nil {
		t.Fatalf("new recovery after cancel: %v", err)
	}
	if p2.PausedSafePct != 10 {
		t.Fatalf("new baseline = %d, want 10", p2.PausedSafePct)
	}

	// 尚未观察就取消的计划：停在起点安全比例。
	publishAtZero(t, s, "chg-cx-b", "b", 1)
	pb, _ := s.StartRecovery("rcv-cxb", "b", 1,
		[]RecoveryBatchInput{{Percentage: 20, ObserveCount: 1, FailureThreshold: 0}})
	pb, err = s.CancelRecovery("rcv-cxb", "")
	if err != nil || pb.SafePercentage != 0 || pb.Status != RecoveryCancelled {
		t.Fatalf("cancel fresh plan: %+v %v", pb, err)
	}
	if pct, _ := s.EffectivePercentage("b"); pct != 0 {
		t.Fatalf("fresh cancel pct = %d, want 0", pct)
	}
}

// ---------- 放量闸门：被保护拦下的流量不会被一次性放开 ----------

func TestGateWidensBatchByBatch(t *testing.T) {
	s := NewService()
	publishAtZero(t, s, "chg-gate-1", "a", 1)

	// 恢复前全部拦截。
	if n := allowedUsers(t, s, "a", 500, 0); n != 0 {
		t.Fatalf("protected traffic must be blocked before recovery, got %d", n)
	}

	_, err := s.StartRecovery("rcv-gate", "a", 1, []RecoveryBatchInput{
		{Percentage: 10, ObserveCount: 5, FailureThreshold: 0},
		{Percentage: 30, ObserveCount: 1, FailureThreshold: 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	n10 := allowedUsers(t, s, "a", 500, 10)
	if n10 == 0 || n10 > 60 {
		t.Fatalf("first batch ~10%%, got %d", n10)
	}

	// 观察未收齐期间无论上报多少，比例都不变。
	for i := 0; i < 4; i++ {
		if _, err := s.ReportObservation("rcv-gate",
			ObservationInput{Batch: 0, Attempt: 0, Success: true}); err != nil {
			t.Fatal(err)
		}
		if n := allowedUsers(t, s, "a", 500, 10); n != n10 {
			t.Fatalf("exposure changed mid-observation: %d vs %d", n, n10)
		}
	}
	if _, err := s.ReportObservation("rcv-gate",
		ObservationInput{Batch: 0, Attempt: 0, Success: true}); err != nil {
		t.Fatal(err)
	}
	// ready 未推进：仍然只有第一批流量。
	if n := allowedUsers(t, s, "a", 500, 10); n != n10 {
		t.Fatalf("ready must not auto-advance: %d vs %d", n, n10)
	}
	_, _ = s.AdvanceRecovery("rcv-gate")
	n30 := allowedUsers(t, s, "a", 500, 30)
	if n30 <= n10 {
		t.Fatalf("second batch must widen exposure: %d -> %d", n10, n30)
	}
}

// 闸门只约束百分比分桶，包含/排除名单语义不变。
func TestGateKeepsIncludeExclude(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-ie-1", []RuleInput{{
		Feature: "a", Version: 1, Percentage: 0,
		Include: []string{"vip"}, Exclude: []string{"banned"},
	}})
	_, _ = s.StartRecovery("rcv-ie", "a", 1, []RecoveryBatchInput{
		{Percentage: 50, ObserveCount: 1, FailureThreshold: 0},
	})
	r := s.Evaluate("a", "vip")
	if !r.Allowed || r.Percentage != 50 {
		t.Fatalf("include user during recovery: %+v", r)
	}
	if s.Evaluate("a", "banned").Allowed {
		t.Fatal("exclude user must still be denied")
	}
}

// 历史回放不施加恢复闸门。
func TestEvalAtIgnoresGate(t *testing.T) {
	s := NewService()
	publishAtZero(t, s, "chg-ea-1", "a", 1)
	seq := s.CurrentSnapshotSeq()
	_, _ = s.StartRecovery("rcv-ea", "a", 1, []RecoveryBatchInput{
		{Percentage: 50, ObserveCount: 1, FailureThreshold: 0},
	})
	r, err := s.EvalAt(seq, "a", "user-1")
	if err != nil {
		t.Fatal(err)
	}
	if r.Percentage != 0 || r.Allowed {
		t.Fatalf("historical replay must use rule pct 0: %+v", r)
	}
}

// ---------- 需求 3：版本变化报冲突，闸门让位 ----------

func TestVersionDriftConflictAndGateRelease(t *testing.T) {
	s := NewService()
	publishAtZero(t, s, "chg-vd-1", "a", 1)
	_, _ = s.StartRecovery("rcv-vd", "a", 1, []RecoveryBatchInput{
		{Percentage: 20, ObserveCount: 3, FailureThreshold: 0},
	})
	// 观察中途发布 v2 100%。
	if _, err := s.ReportObservation("rcv-vd",
		ObservationInput{Batch: 0, Attempt: 0, Success: true}); err != nil {
		t.Fatal(err)
	}
	mustPublish(t, s, "chg-vd-2", []RuleInput{{Feature: "a", Version: 2, Percentage: 100}})

	for _, op := range []func() error{
		func() error {
			_, e := s.ReportObservation("rcv-vd",
				ObservationInput{Batch: 0, Attempt: 0, Success: true})
			return e
		},
		func() error { _, e := s.PauseRecovery("rcv-vd", ""); return e },
		func() error { _, e := s.ResumeRecovery("rcv-vd"); return e },
		func() error { _, e := s.AdvanceRecovery("rcv-vd"); return e },
		func() error { _, e := s.CancelRecovery("rcv-vd", ""); return e },
	} {
		expectKind(t, op(), KindConflict)
	}

	plan, _ := s.GetRecovery("rcv-vd")
	if plan.Status != RecoverySuperseded || plan.BlockReason == "" {
		t.Fatalf("old plan = %+v", plan)
	}
	// 未收齐窗口证据原样保留。
	if w := plan.Batches[0].Windows; len(w) != 1 ||
		w[0].Result != WindowOpen || w[0].Observed != 1 {
		t.Fatalf("partial evidence changed: %+v", w)
	}
	// 闸门让位，新版本 100% 生效。
	if pct, _ := s.EffectivePercentage("a"); pct != 100 {
		t.Fatalf("gate not released, pct = %d", pct)
	}
	if !s.Evaluate("a", "anyone").Allowed {
		t.Fatal("v2 at 100% must allow")
	}

	// 回滚回 v1 后旧计划不复活；新计划以当前规则比例（0%）为起点。
	r1 := s.History()[0]
	if _, err := s.Rollback("chg-vd-rb", r1.Seq); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	p2, err := s.StartRecovery("rcv-vd2", "a", 1,
		[]RecoveryBatchInput{{Percentage: 30, ObserveCount: 1, FailureThreshold: 0}})
	if err != nil {
		t.Fatalf("new recovery on rolled-back v1: %v", err)
	}
	if p2.PausedSafePct != 0 {
		t.Fatalf("baseline = %d, want 0", p2.PausedSafePct)
	}
}

// 多个功能的恢复互相独立。
func TestMultipleFeaturesIndependent(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-mf-1", []RuleInput{
		{Feature: "a", Version: 1, Percentage: 0},
		{Feature: "b", Version: 1, Percentage: 0},
	})
	_, _ = s.StartRecovery("rcv-mf-a", "a", 1,
		[]RecoveryBatchInput{{Percentage: 10, ObserveCount: 1, FailureThreshold: 0}})
	pb, _ := s.StartRecovery("rcv-mf-b", "b", 1,
		[]RecoveryBatchInput{{Percentage: 50, ObserveCount: 1, FailureThreshold: 0}})
	if _, err := s.PauseRecovery("rcv-mf-a", "hold a"); err != nil {
		t.Fatal(err)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 0 {
		t.Fatalf("a paused pct = %d, want 0", pct)
	}
	if pct, _ := s.EffectivePercentage("b"); pct != 50 {
		t.Fatalf("b must stay at 50, got %d", pct)
	}
	if pb.Status != RecoveryObserving {
		t.Fatalf("b plan changed: %s", pb.Status)
	}
}

// 同一批可以经历多轮暂停/恢复，每轮都是全新窗口、attempt 递增，
// 任一轮的失败都不污染其他轮；通过后安全比例才前进一步。
func TestRepeatedPauseResumeCycles(t *testing.T) {
	s := NewService()
	publishAtZero(t, s, "chg-rp-1", "a", 1)
	_, _ = s.StartRecovery("rcv-rp", "a", 1, []RecoveryBatchInput{
		{Percentage: 30, ObserveCount: 2, FailureThreshold: 0},
		{Percentage: 80, ObserveCount: 1, FailureThreshold: 0},
	})

	// 第 0 轮开窗，收一个成功后暂停。
	_, _ = s.ReportObservation("rcv-rp",
		ObservationInput{Batch: 0, Attempt: 0, Success: true})
	if _, err := s.PauseRecovery("rcv-rp", "hold 1"); err != nil {
		t.Fatal(err)
	}
	// 第 1 轮开窗，收一个失败后又暂停（该失败属于第 1 轮）。
	if _, err := s.ResumeRecovery("rcv-rp"); err != nil {
		t.Fatal(err)
	}
	_, _ = s.ReportObservation("rcv-rp",
		ObservationInput{Batch: 0, Attempt: 1, Success: false})
	if _, err := s.PauseRecovery("rcv-rp", "hold 2"); err != nil {
		t.Fatal(err)
	}
	// 第 2 轮开窗，全成功收齐 2 个 -> 通过；前两轮结果都不复用。
	if _, err := s.ResumeRecovery("rcv-rp"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		r, err := s.ReportObservation("rcv-rp",
			ObservationInput{Batch: 0, Attempt: 2, Success: true})
		if err != nil {
			t.Fatalf("report %d: %v", i, err)
		}
		if i == 1 && (!r.WindowPassed || r.SafePercentage != 30) {
			t.Fatalf("attempt 2 window must pass at safe 30: %+v", r)
		}
	}
	plan, _ := s.GetRecovery("rcv-rp")
	if plan.Status != RecoveryReady || plan.SafePercentage != 30 {
		t.Fatalf("plan = %+v", plan)
	}
	for i, w := range plan.Batches[0].Windows {
		if w.Attempt != i {
			t.Fatalf("window %d attempt = %d", i, w.Attempt)
		}
		if i < 2 && w.Result != WindowInterrupted {
			t.Fatalf("window %d result = %s, want interrupted", i, w.Result)
		}
	}
	if w := plan.Batches[0].Windows; w[0].Observed != 1 || w[1].Observed != 1 ||
		w[1].Failures != 1 || w[2].Observed != 2 || w[2].Failures != 0 {
		t.Fatalf("per-window attribution broken: %+v", w)
	}
}

// 同批次比例严格高于当前安全比例：阻断/取消后再提恢复，首批不能回退比例。
func TestRetryMustAdvanceFromSafe(t *testing.T) {
	s := NewService()
	publishAtZero(t, s, "chg-rt-1", "a", 1)
	_, _ = s.StartRecovery("rcv-rt", "a", 1, []RecoveryBatchInput{
		{Percentage: 10, ObserveCount: 1, FailureThreshold: 0},
		{Percentage: 40, ObserveCount: 1, FailureThreshold: 0},
	})
	plan, _ := s.GetRecovery("rcv-rt")
	plan = fillWindow(t, s, "rcv-rt", plan, 0, 0)
	plan, _ = s.AdvanceRecovery("rcv-rt")
	// 第二批阻断，停在 10%。
	_, _ = s.ReportObservation("rcv-rt",
		ObservationInput{Batch: 1, Attempt: 0, Success: false})

	// 首批 10% 不高于安全比例 10% -> 参数错误；必须从更高比例开始。
	_, err := s.StartRecovery("rcv-rt-retry-low", "a", 1,
		[]RecoveryBatchInput{{Percentage: 10, ObserveCount: 1, FailureThreshold: 0}})
	expectKind(t, err, KindParam)
	p, err := s.StartRecovery("rcv-rt-retry", "a", 1,
		[]RecoveryBatchInput{{Percentage: 20, ObserveCount: 1, FailureThreshold: 0}})
	if err != nil {
		t.Fatalf("retry from safe pct: %v", err)
	}
	if p.PausedSafePct != 10 {
		t.Fatalf("baseline = %d, want 10", p.PausedSafePct)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 20 {
		t.Fatalf("retry pct = %d, want 20", pct)
	}
}

// ---------- 需求 2：上报 / 暂停 / 恢复 / 推进并发交错 ----------
//
// 不变量：
//  1. 实际放量比例只能是某批比例或某条安全比例，绝不越过未完成观察的批次；
//  2. 每个开窗窗口计入结果不超过 ObserveCount，不重复计数/重复放量；
//  3. 暂停后新窗口 attempt 严格递增，旧 attempt 结果一律被拒绝。
func TestConcurrentRecoveryInterleaving(t *testing.T) {
	s := NewService()
	publishAtZero(t, s, "chg-ci-0", "a", 1)
	const observe = 50
	_, err := s.StartRecovery("rcv-ci", "a", 1, []RecoveryBatchInput{
		{Percentage: 10, ObserveCount: observe, FailureThreshold: observe - 1},
		{Percentage: 30, ObserveCount: observe, FailureThreshold: observe - 1},
		{Percentage: 60, ObserveCount: observe, FailureThreshold: observe - 1},
	})
	if err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// 上报协程：跟随当前批次/轮次上报，偶尔故意发旧轮次结果（必须被拒）。
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			i := seed
			for {
				select {
				case <-stop:
					return
				default:
				}
				p, err := s.GetRecovery("rcv-ci")
				if err != nil {
					t.Errorf("get: %v", err)
					return
				}
				if p.Status == RecoveryCompleted || p.Status == RecoveryBlocked {
					return
				}
				batch := p.CurrentBatch
				var attempt int
				if wins := p.Batches[batch].Windows; len(wins) > 0 {
					attempt = wins[len(wins)-1].Attempt
				}
				in := ObservationInput{Batch: batch, Attempt: attempt, Success: true}
				if i%7 == 0 && attempt > 0 {
					in.Attempt = attempt - 1
				}
				_, _ = s.ReportObservation("rcv-ci", in)
				i++
			}
		}(g)
	}

	// 推进协程。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			p, _ := s.GetRecovery("rcv-ci")
			switch p.Status {
			case RecoveryReady:
				_, _ = s.AdvanceRecovery("rcv-ci")
			case RecoveryCompleted, RecoveryBlocked:
				return
			}
		}
	}()

	// 暂停/恢复协程。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 40; i++ {
			p, _ := s.GetRecovery("rcv-ci")
			if p.Status == RecoveryCompleted || p.Status == RecoveryBlocked {
				return
			}
			if p.Status == RecoveryObserving {
				if _, err := s.PauseRecovery("rcv-ci", "concurrent hold"); err == nil {
					_, _ = s.ResumeRecovery("rcv-ci")
				}
			}
		}
	}()

	// 判定协程：实际比例只能落在安全集合里。
	allowedPct := map[int]bool{0: true, 10: true, 30: true, 60: true}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			r := s.Evaluate("a", "probe-user")
			if !allowedPct[r.Percentage] {
				t.Errorf("unexpected effective pct %d", r.Percentage)
				return
			}
		}
	}()

	// 主协程辅助推进直到完成。
	for i := 0; i < 200000; i++ {
		p, _ := s.GetRecovery("rcv-ci")
		if p.Status == RecoveryCompleted {
			break
		}
		if p.Status == RecoveryBlocked {
			t.Fatalf("unexpected block with all-success reports: %q", p.BlockReason)
		}
		if p.Status == RecoveryReady {
			_, _ = s.AdvanceRecovery("rcv-ci")
		}
		if i == 199999 {
			t.Fatalf("recovery did not finish: status=%s batch=%d", p.Status, p.CurrentBatch)
		}
	}
	close(stop)
	wg.Wait()

	plan, _ := s.GetRecovery("rcv-ci")
	if plan.SafePercentage != 60 {
		t.Fatalf("final safe pct = %d, want 60", plan.SafePercentage)
	}
	for bi, b := range plan.Batches {
		var passed, interrupted int
		for wi, w := range b.Windows {
			if w.Attempt != wi {
				t.Fatalf("batch %d window %d attempt=%d", bi, wi, w.Attempt)
			}
			if w.Observed > b.ObserveCount {
				t.Fatalf("batch %d window over-observed: %d", bi, w.Observed)
			}
			switch w.Result {
			case WindowPassed:
				passed++
				if w.Observed != observe || w.Failures != 0 {
					t.Fatalf("passed window bad counts: %+v", w)
				}
			case WindowInterrupted:
				interrupted++
				if w.Observed >= observe {
					t.Fatalf("interrupted window over-observed: %d", w.Observed)
				}
			default:
				t.Fatalf("unexpected final window result %q", w.Result)
			}
		}
		if passed < 1 {
			t.Fatalf("batch %d has no passed window", bi)
		}
		if b.ActualPercentage != b.Percentage {
			t.Fatalf("batch %d actual pct=%d want %d", bi, b.ActualPercentage, b.Percentage)
		}
		if passed > 1 && interrupted == 0 {
			t.Fatalf("batch %d passed %d times without interruption", bi, passed)
		}
	}
}

// ready 状态并发推进：恰好一个成功，窗口只打开一次，不跳批、不重复放量。
func TestConcurrentAdvanceOnlyOnce(t *testing.T) {
	s := NewService()
	publishAtZero(t, s, "chg-ca-0", "a", 1)
	plan, _ := s.StartRecovery("rcv-ca", "a", 1, []RecoveryBatchInput{
		{Percentage: 10, ObserveCount: 1, FailureThreshold: 0},
		{Percentage: 40, ObserveCount: 1, FailureThreshold: 0},
	})
	plan = fillWindow(t, s, "rcv-ca", plan, 0, 0)

	const n = 32
	var wg sync.WaitGroup
	ok, rejected := 0, 0
	start := make(chan struct{})
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := s.AdvanceRecovery("rcv-ca")
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				ok++
			} else {
				rejected++
			}
		}()
	}
	close(start)
	wg.Wait()
	if ok != 1 || rejected != n-1 {
		t.Fatalf("advance ok=%d rejected=%d", ok, rejected)
	}
	plan, _ = s.GetRecovery("rcv-ca")
	if plan.CurrentBatch != 1 || plan.Status != RecoveryObserving {
		t.Fatalf("plan after concurrent advance: %+v", plan)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 40 {
		t.Fatalf("pct = %d, want 40", pct)
	}
	if wins := plan.Batches[1].Windows; len(wins) != 1 ||
		wins[0].Attempt != 0 || wins[0].Observed != 0 {
		t.Fatalf("batch1 window opened more than once: %+v", wins)
	}
}

// 并发重复创建同一恢复：只有一个进行中计划，其余要么幂等返回、要么冲突。
func TestConcurrentStartRecovery(t *testing.T) {
	s := NewService()
	publishAtZero(t, s, "chg-cs-0", "a", 1)
	batches := []RecoveryBatchInput{{Percentage: 10, ObserveCount: 1, FailureThreshold: 0}}
	const n = 16
	var wg sync.WaitGroup
	start := make(chan struct{})
	var mu sync.Mutex
	success := map[string]string{} // changeID -> planID
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			changeID := "rcv-cs"
			if i%3 == 0 {
				changeID = fmt.Sprintf("rcv-cs-%d", i) // 异号，应因已有进行中计划而冲突
			}
			p, err := s.StartRecovery(changeID, "a", 1, batches)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				success[changeID] = p.ID
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if len(success) != 1 {
		t.Fatalf("expected exactly one successful changeID, got %v", success)
	}
	var changeID string
	for c := range success {
		changeID = c
	}
	plan, _ := s.GetRecovery(changeID)
	if wins := plan.Batches[0].Windows; len(wins) != 1 || wins[0].Observed != 0 {
		t.Fatalf("first window opened more than once: %+v", wins)
	}
}
