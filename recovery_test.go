package featurerollout

import (
	"fmt"
	"sync"
	"testing"
)

func passBatch(t *testing.T, s *Service, plan *RecoveryPlan, batch, failures int) *RecoveryPlan {
	t.Helper()
	b := plan.Batches[batch]
	attempt := b.Windows[len(b.Windows)-1].Attempt
	for i := 0; i < b.ObserveCount; i++ {
		ok := i >= failures
		r, err := s.ReportObservation(plan.ChangeID, ObservationInput{Batch: batch, Attempt: attempt, Success: ok})
		if err != nil {
			t.Fatalf("ReportObservation(batch=%d, i=%d) failed: %v", batch, i, err)
		}
		if i < b.ObserveCount-1 {
			if r.WindowClosed {
				t.Fatalf("window closed early at result %d", i)
			}
			continue
		}
		if !r.WindowClosed || !r.WindowPassed {
			t.Fatalf("last receipt: closed=%v passed=%v", r.WindowClosed, r.WindowPassed)
		}
	}
	got, err := s.GetRecovery(plan.ChangeID)
	if err != nil {
		t.Fatalf("GetRecovery: %v", err)
	}
	return got
}

// 制造"规则目标 100% 但被保护机制拦下"的状态：
// 直接发布一条 0% 规则表示当前安全比例就是 0%（保护动作），
// 后续恢复必须一批一批放量，不能直接恢复到 100%。
func setupGuardedAtZero(t *testing.T, s *Service, feature string, version int) {
	t.Helper()
	mustPublish(t, s, fmt.Sprintf("chg-setup-%s", feature), []RuleInput{
		{Feature: feature, Version: version, Percentage: 0},
	})
	if pct, ok := s.EffectivePercentage(feature); !ok || pct != 0 {
		t.Fatalf("setup: effective pct = (%d,%v), want 0", pct, ok)
	}
}

// ---------- 计划参数校验 ----------

func TestStartRecoveryValidation(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-rv-1", []RuleInput{{Feature: "a", Version: 1, Percentage: 0}})

	cases := []struct {
		name    string
		change  string
		feature string
		version int
		batches []RecoveryBatchInput
		kind    ErrorKind
	}{
		{"empty change", "", "a", 1, []RecoveryBatchInput{{10, 1, 0}}, KindParam},
		{"empty feature", "c", "", 1, []RecoveryBatchInput{{10, 1, 0}}, KindParam},
		{"no batches", "c", "a", 1, nil, KindParam},
		{"pct negative", "c", "a", 1, []RecoveryBatchInput{{-1, 1, 0}}, KindParam},
		{"pct over 100", "c", "a", 1, []RecoveryBatchInput{{101, 1, 0}}, KindParam},
		{"observe zero", "c", "a", 1, []RecoveryBatchInput{{10, 0, 0}}, KindParam},
		{"threshold negative", "c", "a", 1, []RecoveryBatchInput{{10, 2, -1}}, KindParam},
		{"threshold equals observe", "c", "a", 1, []RecoveryBatchInput{{10, 2, 2}}, KindParam},
		{"non increasing pct", "c", "a", 1,
			[]RecoveryBatchInput{{20, 1, 0}, {20, 1, 0}}, KindParam},
		{"first pct not above safe", "c", "a", 1,
			[]RecoveryBatchInput{{0, 1, 0}}, KindParam},
		{"unknown feature", "c", "ghost", 1,
			[]RecoveryBatchInput{{10, 1, 0}}, KindNotFound},
		{"version mismatch", "c", "a", 2,
			[]RecoveryBatchInput{{10, 1, 0}}, KindConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.StartRecovery(tc.change, tc.feature, tc.version, tc.batches)
			expectKind(t, err, tc.kind)
		})
	}
}

// ---------- 幂等与同号冲突 ----------

func TestRecoveryIdempotentAndConflict(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-ri-1", []RuleInput{{Feature: "a", Version: 1, Percentage: 0}})

	batches := []RecoveryBatchInput{{Percentage: 10, ObserveCount: 2, FailureThreshold: 0}}
	p1, err := s.StartRecovery("rcv-i", "a", 1, batches)
	if err != nil {
		t.Fatalf("StartRecovery: %v", err)
	}
	p2, err := s.StartRecovery("rcv-i", "a", 1, batches)
	if err != nil {
		t.Fatalf("repeated StartRecovery: %v", err)
	}
	if p1.ID != p2.ID || p1.Status != p2.Status || p1.CurrentBatch != p2.CurrentBatch {
		t.Fatalf("same request must return original plan: %+v vs %+v", p1, p2)
	}

	// 同号改批次内容 / 版本 -> 冲突。
	if _, err := s.StartRecovery("rcv-i", "a", 1,
		[]RecoveryBatchInput{{Percentage: 20, ObserveCount: 2, FailureThreshold: 0}}); err == nil {
		t.Fatal("different batches with same changeID must conflict")
	} else {
		expectKind(t, err, KindConflict)
	}
	if _, err := s.StartRecovery("rcv-i", "a", 1,
		[]RecoveryBatchInput{{Percentage: 10, ObserveCount: 3, FailureThreshold: 0}}); err == nil {
		t.Fatal("different observe count with same changeID must conflict")
	}

	// 同一功能已有进行中的恢复时，新 changeID 也不能再起一个。
	_, err = s.StartRecovery("rcv-other", "a", 1,
		[]RecoveryBatchInput{{Percentage: 10, ObserveCount: 1, FailureThreshold: 0}})
	expectKind(t, err, KindConflict)

	// 未知恢复号。
	if _, err := s.PauseRecovery("nope", ""); err == nil {
		t.Fatal("pause unknown plan must fail")
	} else {
		expectKind(t, err, KindNotFound)
	}
	_, err = s.AdvanceRecovery("nope")
	expectKind(t, err, KindNotFound)
	_, err = s.CancelRecovery("nope", "")
	expectKind(t, err, KindNotFound)
	_, err = s.ReportObservation("nope", ObservationInput{Batch: 0})
	expectKind(t, err, KindNotFound)
}

// ---------- 完整的分批恢复 ----------

func TestRecoveryBatchLifecycle(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-rbl-1", []RuleInput{{Feature: "a", Version: 1, Percentage: 0}})

	plan, err := s.StartRecovery("rcv-1", "a", 1, []RecoveryBatchInput{
		{Percentage: 10, ObserveCount: 3, FailureThreshold: 1},
		{Percentage: 50, ObserveCount: 2, FailureThreshold: 0},
		{Percentage: 100, ObserveCount: 1, FailureThreshold: 0},
	})
	if err != nil {
		t.Fatalf("StartRecovery: %v", err)
	}
	if plan.Status != RecoveryObserving || plan.SafePercentage != 0 || plan.CurrentBatch != 0 {
		t.Fatalf("unexpected initial plan: %+v", plan)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 10 {
		t.Fatalf("effective pct after start = %d, want 10", pct)
	}

	// 上一批没观察完，不能开下一批。
	if _, err := s.AdvanceRecovery("rcv-1"); err == nil {
		t.Fatal("advance must be rejected while observing")
	}

	// 第一批：3 个结果 1 个失败，阈值 1 -> 通过。
	plan = passBatch(t, s, plan, 0, 1)
	if plan.Status != RecoveryReady || plan.SafePercentage != 10 {
		t.Fatalf("batch0: status=%s safe=%d", plan.Status, plan.SafePercentage)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 10 {
		t.Fatalf("ready state must hold at safe pct 10, got %d", pct)
	}
	b0 := plan.Batches[0]
	if b0.ActualPercentage != 10 || b0.Windows[0].Result != WindowPassed ||
		b0.Windows[0].Observed != 3 || b0.Windows[0].Failures != 1 {
		t.Fatalf("batch0 evidence not retained: %+v", b0)
	}

	// 显式推进第二批：比例升到 50，新窗口 attempt=0。
	plan, err = s.AdvanceRecovery("rcv-1")
	if err != nil {
		t.Fatalf("AdvanceRecovery: %v", err)
	}
	if plan.Status != RecoveryObserving || plan.CurrentBatch != 1 {
		t.Fatalf("after advance: %+v", plan)
	}
	if plan.Batches[1].Windows[0].Attempt != 0 {
		t.Fatalf("new batch window attempt = %d, want 0", plan.Batches[1].Windows[0].Attempt)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 50 {
		t.Fatalf("effective pct = %d, want 50", pct)
	}

	plan = passBatch(t, s, plan, 1, 0)
	if plan.Status != RecoveryReady || plan.SafePercentage != 50 {
		t.Fatalf("batch1: status=%s safe=%d", plan.Status, plan.SafePercentage)
	}

	// 推进并完成最后一批。
	plan, _ = s.AdvanceRecovery("rcv-1")
	if pct, _ := s.EffectivePercentage("a"); pct != 100 {
		t.Fatalf("effective pct = %d, want 100", pct)
	}
	plan = passBatch(t, s, plan, 2, 0)
	if plan.Status != RecoveryCompleted || plan.SafePercentage != 100 {
		t.Fatalf("final plan = %+v", plan)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 100 {
		t.Fatalf("completed effective pct = %d, want 100", pct)
	}

	// 完成后不能再推进/暂停。
	if _, err := s.AdvanceRecovery("rcv-1"); err == nil {
		t.Fatal("advance on completed plan must fail")
	}
	if _, err := s.PauseRecovery("rcv-1", ""); err == nil {
		t.Fatal("pause on completed plan must fail")
	}

	// 每批实际比例、观察结果都留痕。
	for i, b := range plan.Batches {
		if len(b.Windows) != 1 || b.Windows[0].Observed != b.ObserveCount ||
			b.Windows[0].Result != WindowPassed || b.ActualPercentage != b.Percentage {
			t.Fatalf("batch %d evidence broken: %+v", i, b)
		}
	}
}

// 判定放量随批次扩大；观察失败后收回安全比例。
func TestRecoveryGateAffectsEvaluation(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-gate-1", []RuleInput{{Feature: "a", Version: 1, Percentage: 0}})

	users := make([]string, 500)
	for i := range users {
		users[i] = fmt.Sprintf("u-%d", i)
	}
	allowedCount := func(pct int) int {
		n := 0
		for _, u := range users {
			r := s.Evaluate("a", u)
			if r.Allowed {
				n++
			}
			if r.Percentage != pct {
				t.Fatalf("Evaluate reported pct %d, want %d", r.Percentage, pct)
			}
		}
		return n
	}

	plan, err := s.StartRecovery("rcv-gate", "a", 1, []RecoveryBatchInput{
		{Percentage: 10, ObserveCount: 1, FailureThreshold: 0},
		{Percentage: 40, ObserveCount: 1, FailureThreshold: 0},
	})
	if err != nil {
		t.Fatalf("StartRecovery: %v", err)
	}
	n10 := allowedCount(10)
	if n10 == 0 || n10 > 60 {
		t.Fatalf("~10%% expected, got %d", n10)
	}

	plan = passBatch(t, s, plan, 0, 0)
	if n := allowedCount(10); n != n10 {
		t.Fatalf("ready-before-advance must not change exposure: %d vs %d", n, n10)
	}

	plan, _ = s.AdvanceRecovery("rcv-gate")
	if n := allowedCount(40); n <= n10 {
		t.Fatalf("exposure must grow after advance: %d -> %d", n10, n)
	}

	// 第二批观察失败：放量退回安全比例 10%，阻断原因留痕。
	if _, err := s.ReportObservation("rcv-gate",
		ObservationInput{Batch: 1, Attempt: 0, Success: false}); err != nil {
		t.Fatalf("report failure: %v", err)
	}
	plan, _ = s.GetRecovery("rcv-gate")
	if plan.Status != RecoveryBlocked || plan.SafePercentage != 10 {
		t.Fatalf("plan = %+v", plan)
	}
	if n := allowedCount(10); n != n10 {
		t.Fatalf("blocked exposure must fall back to safe pct: %d vs %d", n, n10)
	}
	if plan.Batches[1].ActualPercentage != 10 ||
		plan.Batches[1].Windows[0].Result != WindowBlocked ||
		plan.Batches[1].Windows[0].BlockReason == "" || plan.BlockReason == "" {
		t.Fatalf("block evidence not retained: batch=%+v plan=%q", plan.Batches[1], plan.BlockReason)
	}

	// 阻断后保持安全比例，且可以从安全比例重新提一份新恢复计划。
	p2, err := s.StartRecovery("rcv-gate-retry", "a", 1,
		[]RecoveryBatchInput{{Percentage: 20, ObserveCount: 1, FailureThreshold: 0}})
	if err != nil {
		t.Fatalf("retry after block: %v", err)
	}
	if p2.PausedSafePct != 10 {
		t.Fatalf("retry baseline = %d, want 10", p2.PausedSafePct)
	}
}

// ---------- 暂停后恢复：旧窗口失败归属旧尝试，新窗口重新计数 ----------

func TestPauseResumeFreshWindow(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-pr-1", []RuleInput{{Feature: "a", Version: 1, Percentage: 0}})

	plan, _ := s.StartRecovery("rcv-pr", "a", 1, []RecoveryBatchInput{
		{Percentage: 30, ObserveCount: 4, FailureThreshold: 0},
		{Percentage: 80, ObserveCount: 1, FailureThreshold: 0},
	})
	// 观察到一半（2/4，含 1 失败）时人工暂停。
	for i := 0; i < 2; i++ {
		if _, err := s.ReportObservation("rcv-pr",
			ObservationInput{Batch: 0, Attempt: 0, Success: i != 0}); err != nil {
			t.Fatalf("report: %v", err)
		}
	}
	plan, err := s.PauseRecovery("rcv-pr", "operator hold")
	if err != nil || plan.Status != RecoveryPaused {
		t.Fatalf("pause: %+v %v", plan, err)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 0 {
		t.Fatalf("paused effective pct = %d, want 0", pct)
	}
	win0 := plan.Batches[0].Windows[0]
	if win0.Result != WindowInterrupted || win0.Observed != 2 || win0.Failures != 1 ||
		win0.BlockReason == "" {
		t.Fatalf("interrupted window evidence: %+v", win0)
	}

	// 暂停幂等；暂停态推进/恢复开窗之外的推进被拒。
	if p, err := s.PauseRecovery("rcv-pr", "again"); err != nil || p.Status != RecoveryPaused {
		t.Fatalf("pause idempotency: %+v %v", p, err)
	}
	if _, err := s.AdvanceRecovery("rcv-pr"); err == nil {
		t.Fatal("advance while paused must fail")
	}

	// 恢复：同批次开全新窗口 attempt=1，从安全比例 0% 重新放量 30%。
	plan, err = s.ResumeRecovery("rcv-pr")
	if err != nil {
		t.Fatalf("ResumeRecovery: %v", err)
	}
	if plan.Status != RecoveryObserving {
		t.Fatalf("status after resume = %s", plan.Status)
	}
	win1 := plan.Batches[0].Windows[1]
	if win1.Attempt != 1 || win1.Observed != 0 || win1.Failures != 0 || win1.Result != WindowOpen {
		t.Fatalf("fresh window after resume: %+v", win1)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 30 {
		t.Fatalf("resumed effective pct = %d, want 30", pct)
	}

	// 旧窗口（attempt=0）迟到的失败结果不能计入新窗口。
	_, err = s.ReportObservation("rcv-pr",
		ObservationInput{Batch: 0, Attempt: 0, Success: false})
	expectKind(t, err, KindConflict)
	plan, _ = s.GetRecovery("rcv-pr")
	if got := plan.Batches[0].Windows[0].LateResults; got != 1 {
		t.Fatalf("late result not recorded on old window: %d", got)
	}

	// 新窗口即使全成功，也必须自己收齐 4 个结果（旧结果不算）。
	for i := 0; i < 4; i++ {
		r, err := s.ReportObservation("rcv-pr",
			ObservationInput{Batch: 0, Attempt: 1, Success: true})
		if err != nil {
			t.Fatalf("report into fresh window: %v", err)
		}
		if i == 3 && (!r.WindowClosed || !r.WindowPassed) {
			t.Fatalf("fresh window should pass: %+v", r)
		}
	}
	plan, _ = s.GetRecovery("rcv-pr")
	if plan.Status != RecoveryReady || plan.SafePercentage != 30 {
		t.Fatalf("plan after fresh window: %+v", plan)
	}
}

// ready 状态暂停再恢复：观察已完成，恢复后仍需重新开窗验证当前安全比例。
func TestPauseAtReadyAndResume(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-prr-1", []RuleInput{{Feature: "a", Version: 1, Percentage: 0}})
	plan, _ := s.StartRecovery("rcv-prr", "a", 1, []RecoveryBatchInput{
		{Percentage: 20, ObserveCount: 1, FailureThreshold: 0},
		{Percentage: 60, ObserveCount: 1, FailureThreshold: 0},
	})
	plan = passBatch(t, s, plan, 0, 0)

	// 第一批已通过（安全比例 20%），未推进第二批时暂停。
	paused, err := s.PauseRecovery("rcv-prr", "hold")
	if err != nil || paused.Status != RecoveryPaused {
		t.Fatalf("pause at ready: %v", err)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 20 {
		t.Fatalf("paused safe pct = %d, want 20", pct)
	}
	// 恢复后从安全比例 20% 重新观察第一批，而不是直接开 60% 的第二批。
	plan, err = s.ResumeRecovery("rcv-prr")
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if plan.CurrentBatch != 0 || plan.Status != RecoveryObserving {
		t.Fatalf("resumed plan = %+v", plan)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 20 {
		t.Fatalf("resumed pct = %d, want 20 (safe pct re-observed)", pct)
	}
	if win := plan.Batches[0].Windows; len(win) != 2 || win[1].Attempt != 1 {
		t.Fatalf("expected new reobservation window: %+v", win)
	}
}

// ---------- 旧批次结果不能推进新批次，也不能重复放量 ----------

func TestStaleObservationsRejected(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-so-1", []RuleInput{{Feature: "a", Version: 1, Percentage: 0}})
	plan, _ := s.StartRecovery("rcv-so", "a", 1, []RecoveryBatchInput{
		{Percentage: 10, ObserveCount: 2, FailureThreshold: 0},
		{Percentage: 50, ObserveCount: 2, FailureThreshold: 0},
		{Percentage: 90, ObserveCount: 2, FailureThreshold: 0},
	})

	// 第一批观察通过并推进到第二批。
	plan = passBatch(t, s, plan, 0, 0)
	plan, _ = s.AdvanceRecovery("rcv-so")

	// 旧批次（batch=0）的迟到结果：拒绝，不能再影响已完成批次。
	_, err := s.ReportObservation("rcv-so",
		ObservationInput{Batch: 0, Attempt: 0, Success: false})
	expectKind(t, err, KindConflict)
	// 越界批次：参数错误。
	_, err = s.ReportObservation("rcv-so",
		ObservationInput{Batch: 5, Attempt: 0, Success: true})
	expectKind(t, err, KindParam)

	// 当前批次错误的 attempt：拒绝。
	_, err = s.ReportObservation("rcv-so",
		ObservationInput{Batch: 1, Attempt: 9, Success: true})
	expectKind(t, err, KindConflict)

	// 窗口收齐后再上报：拒绝且不重复计数。
	plan = passBatch(t, s, plan, 1, 0)
	if plan.Status != RecoveryReady {
		t.Fatalf("batch1 should be ready: %s", plan.Status)
	}
	r, err := s.ReportObservation("rcv-so",
		ObservationInput{Batch: 1, Attempt: 0, Success: true})
	if err == nil || r != nil {
		t.Fatalf("report after window closed must fail, got %+v %v", r, err)
	}
	win := plan.Batches[1].Windows[0]
	if win.Observed != 2 || win.LateResults != 1 {
		t.Fatalf("closed window altered: observed=%d late=%d", win.Observed, win.LateResults)
	}
}

// ---------- 观察失败保持安全比例，阈值边界严格 ----------

func TestRecoveryFailureThreshold(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-ft-1", []RuleInput{{Feature: "a", Version: 1, Percentage: 0}})

	// 阈值 1：恰好 1 个失败通过；第 2 个失败才阻断。
	plan, _ := s.StartRecovery("rcv-ft1", "a", 1, []RecoveryBatchInput{
		{Percentage: 30, ObserveCount: 3, FailureThreshold: 1},
	})
	reports := []bool{true, false, false}
	for i, ok := range reports {
		r, err := s.ReportObservation("rcv-ft1",
			ObservationInput{Batch: 0, Attempt: 0, Success: ok})
		if err != nil {
			t.Fatalf("report %d: %v", i, err)
		}
		if i == 2 {
			if !r.WindowClosed || r.WindowPassed || !r.Blocked {
				t.Fatalf("2 failures against threshold 1 must block: %+v", r)
			}
		}
	}
	plan, _ = s.GetRecovery("rcv-ft1")
	if plan.Status != RecoveryBlocked || plan.SafePercentage != 0 {
		t.Fatalf("blocked plan = %+v", plan)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 0 {
		t.Fatalf("blocked pct = %d, want 0", pct)
	}
	// 阻断后上报、推进、恢复都被拒。
	_, err := s.AdvanceRecovery("rcv-ft1")
	expectKind(t, err, KindConflict)
	_, err = s.ResumeRecovery("rcv-ft1")
	expectKind(t, err, KindConflict)

	// 阈值 0：出现第一个失败立即（随窗口收齐）阻断。
	s2 := NewService()
	mustPublish(t, s2, "chg-ft-2", []RuleInput{{Feature: "a", Version: 1, Percentage: 0}})
	_, _ = s2.StartRecovery("rcv-ft2", "a", 1, []RecoveryBatchInput{
		{Percentage: 30, ObserveCount: 2, FailureThreshold: 0},
	})
	if _, err := s2.ReportObservation("rcv-ft2",
		ObservationInput{Batch: 0, Attempt: 0, Success: false}); err != nil {
		t.Fatalf("report: %v", err)
	}
	if _, err := s2.ReportObservation("rcv-ft2",
		ObservationInput{Batch: 0, Attempt: 0, Success: true}); err != nil {
		t.Fatalf("report: %v", err)
	}
	p, _ := s2.GetRecovery("rcv-ft2")
	if p.Status != RecoveryBlocked {
		t.Fatalf("status = %s, want blocked", p.Status)
	}
}

// ---------- 取消：保留已完成批次，停在最后安全比例 ----------

func TestCancelRecoveryKeepsSafeBatches(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-cx-1", []RuleInput{{Feature: "a", Version: 1, Percentage: 0}})
	plan, _ := s.StartRecovery("rcv-cx", "a", 1, []RecoveryBatchInput{
		{Percentage: 10, ObserveCount: 1, FailureThreshold: 0},
		{Percentage: 40, ObserveCount: 2, FailureThreshold: 0},
		{Percentage: 100, ObserveCount: 1, FailureThreshold: 0},
	})
	plan = passBatch(t, s, plan, 0, 0)
	plan, _ = s.AdvanceRecovery("rcv-cx")
	// 第二批只观察到一半（1/2）就取消。
	if _, err := s.ReportObservation("rcv-cx",
		ObservationInput{Batch: 1, Attempt: 0, Success: true}); err != nil {
		t.Fatalf("report: %v", err)
	}
	plan, err := s.CancelRecovery("rcv-cx", "rollback decision")
	if err != nil {
		t.Fatalf("CancelRecovery: %v", err)
	}
	if plan.Status != RecoveryCancelled || plan.SafePercentage != 10 {
		t.Fatalf("cancelled plan = %+v", plan)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 10 {
		t.Fatalf("after cancel pct = %d, want last safe 10", pct)
	}
	// 第一批证据保留；第二批窗口标记为中断且证据保留。
	if plan.Batches[0].ActualPercentage != 10 ||
		plan.Batches[0].Windows[0].Result != WindowPassed {
		t.Fatalf("completed batch lost: %+v", plan.Batches[0])
	}
	w := plan.Batches[1].Windows[0]
	if w.Result != WindowInterrupted || w.Observed != 1 || w.BlockReason == "" {
		t.Fatalf("interrupted batch evidence: %+v", w)
	}
	if plan.Batches[2].ActualPercentage != 0 || len(plan.Batches[2].Windows) != 0 {
		t.Fatalf("unopened batch must stay empty: %+v", plan.Batches[2])
	}

	// 取消幂等；取消后推进/恢复/上报被拒。
	if p, err := s.CancelRecovery("rcv-cx", "again"); err != nil || p.Status != RecoveryCancelled {
		t.Fatalf("cancel idempotency: %v %v", p, err)
	}
	if _, err := s.AdvanceRecovery("rcv-cx"); err == nil {
		t.Fatal("advance after cancel must fail")
	}
	if _, err := s.ReportObservation("rcv-cx",
		(ObservationInput{Batch: 1, Attempt: 0, Success: true})); err == nil {
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

	// 取消一个尚未观察的初始计划：停在起点安全比例。
	setupGuardedAtZero(t, s, "b", 1)
	pb, _ := s.StartRecovery("rcv-cxb", "b", 1,
		[]RecoveryBatchInput{{Percentage: 20, ObserveCount: 1, FailureThreshold: 0}})
	pb, err = s.CancelRecovery("rcv-cxb", "")
	if err != nil || pb.Status != RecoveryCancelled || pb.SafePercentage != 0 {
		t.Fatalf("cancel fresh plan: %+v %v", pb, err)
	}
	if pct, _ := s.EffectivePercentage("b"); pct != 0 {
		t.Fatalf("b pct = %d, want 0", pct)
	}
}

// ---------- 被保护拦下的流量不会被一次性放开 ----------

func TestProtectedTrafficNotFullyReopened(t *testing.T) {
	s := NewService()
	setupGuardedAtZero(t, s, "a", 1)

	users := make([]string, 500)
	for i := range users {
		users[i] = fmt.Sprintf("u-%d", i)
	}
	allowed := func() int {
		n := 0
		for _, u := range users {
			if s.Evaluate("a", u).Allowed {
				n++
			}
		}
		return n
	}
	if n := allowed(); n != 0 {
		t.Fatalf("protected traffic must stay blocked before recovery, got %d", n)
	}

	plan, err := s.StartRecovery("rcv-guarded", "a", 1, []RecoveryBatchInput{
		{Percentage: 10, ObserveCount: 5, FailureThreshold: 0},
		{Percentage: 30, ObserveCount: 1, FailureThreshold: 0},
	})
	if err != nil {
		t.Fatalf("StartRecovery: %v", err)
	}
	if plan.PausedSafePct != 0 {
		t.Fatalf("baseline = %d, want 0", plan.PausedSafePct)
	}
	n10 := allowed()
	if n10 == 0 || n10 > 60 {
		t.Fatalf("first batch should expose ~10%% only, got %d", n10)
	}

	// 观察未完成期间，无论怎样都不能进一步放量。
	for i := 0; i < 4; i++ {
		if _, err := s.ReportObservation("rcv-guarded",
			ObservationInput{Batch: 0, Attempt: 0, Success: true}); err != nil {
			t.Fatalf("report: %v", err)
		}
		if n := allowed(); n != n10 {
			t.Fatalf("exposure changed mid-observation: %d vs %d", n, n10)
		}
	}
	if _, err := s.ReportObservation("rcv-guarded",
		ObservationInput{Batch: 0, Attempt: 0, Success: true}); err != nil {
		t.Fatalf("report: %v", err)
	}
	// ready 但未推进：仍然只有第一批流量。
	if n := allowed(); n != n10 {
		t.Fatalf("ready must not auto-advance: %d vs %d", n, n10)
	}
	plan, _ = s.AdvanceRecovery("rcv-guarded")
	n30 := allowed()
	if n30 <= n10 {
		t.Fatalf("second batch must widen exposure: %d -> %d", n10, n30)
	}
}

// ---------- 版本变化：旧恢复操作报冲突，闸门让位给新版本 ----------

func TestRecoveryVersionDriftConflict(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-vd-1", []RuleInput{{Feature: "a", Version: 1, Percentage: 0}})
	_, _ = s.StartRecovery("rcv-vd", "a", 1, []RecoveryBatchInput{
		{Percentage: 20, ObserveCount: 3, FailureThreshold: 0},
	})

	// 恢复进行中发布了新版本。
	mustPublish(t, s, "chg-vd-2", []RuleInput{{Feature: "a", Version: 2, Percentage: 100}})

	for _, op := range []string{"report", "pause", "resume", "advance", "cancel"} {
		var err error
		switch op {
		case "report":
			_, err = s.ReportObservation("rcv-vd",
				ObservationInput{Batch: 0, Attempt: 0, Success: true})
		case "pause":
			_, err = s.PauseRecovery("rcv-vd", "")
		case "resume":
			_, err = s.ResumeRecovery("rcv-vd")
		case "advance":
			_, err = s.AdvanceRecovery("rcv-vd")
		case "cancel":
			_, err = s.CancelRecovery("rcv-vd", "")
		}
		expectKind(t, err, KindConflict)
	}

	plan, _ := s.GetRecovery("rcv-vd")
	if plan.Status != RecoverySuperseded || plan.BlockReason == "" {
		t.Fatalf("old plan should be superseded with reason: %+v", plan)
	}
	// 旧批次窗口证据仍保留。
	if w := plan.Batches[0].Windows; len(w) != 1 || w[0].Result != WindowOpen {
		t.Fatalf("evidence changed: %+v", w)
	}
	// 旧闸门不再约束新版本：100% 生效。
	if pct, ok := s.EffectivePercentage("a"); !ok || pct != 100 {
		t.Fatalf("version drift should release gate, pct=(%d,%v)", pct, ok)
	}
	if !s.Evaluate("a", "anyone").Allowed {
		t.Fatal("v2 at 100% must allow after supersede")
	}

	// 回滚回 v1 后，旧计划依然已终结，不能复活控制放量；
	// 需要针对当前版本重新提恢复（其起点是当前规则的比例）。
	r1 := s.History()[0]
	_, _ = s.Rollback("chg-vd-rb", r1.Seq)
	plan2, err := s.StartRecovery("rcv-vd2", "a", 1,
		[]RecoveryBatchInput{{Percentage: 30, ObserveCount: 1, FailureThreshold: 0}})
	if err != nil {
		t.Fatalf("new recovery on rolled-back v1: %v", err)
	}
	if plan2.PausedSafePct != 0 {
		t.Fatalf("rollback to v1 pct 0 baseline = %d", plan2.PausedSafePct)
	}
}

// 名单用户不受恢复闸门影响：闸门只约束百分比分桶。
func TestRecoveryGateKeepsIncludeExclude(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-ie-1", []RuleInput{{
		Feature: "a", Version: 1, Percentage: 0,
		Include: []string{"vip"}, Exclude: []string{"banned"},
	}})
	_, _ = s.StartRecovery("rcv-ie", "a", 1, []RecoveryBatchInput{
		{Percentage: 50, ObserveCount: 1, FailureThreshold: 0},
	})
	if !s.Evaluate("a", "vip").Allowed {
		t.Fatal("include list must still allow during recovery")
	}
	if s.Evaluate("a", "banned").Allowed {
		t.Fatal("exclude list must still deny during recovery")
	}
}

// ---------- 并发：上报 / 暂停 / 恢复 / 推进交错 ----------

// 并发交错下不变量：
//  1. 任一时刻实际放量比例只能等于某个已打开批次的比例或某条安全比例，
//     绝不会越过未完成观察的批次；
//  2. 每个开窗窗口计入的结果数不超过 ObserveCount，不重复放量计数；
//  3. 暂停后新窗口的 attempt 严格递增，旧 attempt 结果一律被拒绝。
func TestConcurrentRecoveryInterleaving(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-conc-r-0", []RuleInput{{Feature: "a", Version: 1, Percentage: 0}})
	const observe = 50
	_, err := s.StartRecovery("rcv-conc", "a", 1, []RecoveryBatchInput{
		{Percentage: 10, ObserveCount: observe, FailureThreshold: observe - 1}, // 全成功才通过
		{Percentage: 30, ObserveCount: observe, FailureThreshold: observe - 1},
		{Percentage: 60, ObserveCount: observe, FailureThreshold: observe - 1},
	})
	if err != nil {
		t.Fatalf("StartRecovery: %v", err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// 上报协程：按计划返回的当前批次/轮次上报，也故意混入旧轮次结果（必须被拒）。
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				p, err := s.GetRecovery("rcv-conc")
				if err != nil {
					t.Errorf("get: %v", err)
					return
				}
				if p.Status == RecoveryCompleted {
					return
				}
				batch := p.CurrentBatch
				var attempt int
				if wins := p.Batches[batch].Windows; len(wins) > 0 {
					attempt = wins[len(wins)-1].Attempt
				}
				in := ObservationInput{Batch: batch, Attempt: attempt, Success: true}
				if i%7 == 0 && attempt > 0 {
					in.Attempt = attempt - 1 // 旧轮次结果，必须被拒绝
				}
				_, _ = s.ReportObservation("rcv-conc", in)
				i++
			}
		}(g)
	}

	// 推进协程：ready 立即推进下一批。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			p, _ := s.GetRecovery("rcv-conc")
			switch p.Status {
			case RecoveryReady:
				// 可能与暂停协程交错：ready 被暂停后推进返回冲突，属预期。
				_, _ = s.AdvanceRecovery("rcv-conc")
			case RecoveryCompleted:
				return
			}
		}
	}()

	// 暂停/恢复协程：在观察中随机打断，迫使窗口重新计数。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			p, _ := s.GetRecovery("rcv-conc")
			if p.Status == RecoveryCompleted {
				return
			}
			if p.Status == RecoveryObserving {
				if _, err := s.PauseRecovery("rcv-conc", "concurrent hold"); err == nil {
					// 仅在自己暂停成功时恢复；窗口可能恰在暂停瞬间收齐变 ready。
					_, _ = s.ResumeRecovery("rcv-conc")
				}
			}
		}
	}()

	// 判定协程：实际比例只能是 {0,10,30,60} 中的安全值，且必须单调推进（暂停时回落）。
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
				t.Errorf("unexpected effective pct %d (status %d)", r.Percentage, r.Seq)
				return
			}
		}
	}()

	// 等待最终完成（上报量充足），最多辅助推进。
	deadline := 0
	for {
		p, _ := s.GetRecovery("rcv-conc")
		if p.Status == RecoveryCompleted {
			break
		}
		if p.Status == RecoveryReady {
			_, _ = s.AdvanceRecovery("rcv-conc")
		}
		deadline++
		if deadline > 100000 {
			t.Fatalf("recovery did not complete, status=%s batch=%d", p.Status, p.CurrentBatch)
		}
	}
	close(stop)
	wg.Wait()

	plan, _ := s.GetRecovery("rcv-conc")
	if plan.SafePercentage != 60 {
		t.Fatalf("final safe pct = %d, want 60", plan.SafePercentage)
	}
	// 每个通过的窗口计数精确等于 ObserveCount；其余窗口为中断窗口（严格欠收）。
	// 每个批次的开窗轮次 attempt 严格从 0 递增。
	for bi, b := range plan.Batches {
		var passed, interrupted int
		for wi, w := range b.Windows {
			if w.Attempt != wi {
				t.Fatalf("batch %d window %d attempt = %d", bi, wi, w.Attempt)
			}
			switch w.Result {
			case WindowPassed:
				passed++
				if w.Observed != observe {
					t.Fatalf("passed window observed %d, want %d", w.Observed, observe)
				}
				if w.Failures != 0 {
					t.Fatalf("all reports were success, got failures %d", w.Failures)
				}
			case WindowInterrupted:
				interrupted++
				if w.Observed >= observe {
					t.Fatalf("interrupted window over-observed: %d", w.Observed)
				}
			case WindowOpen, WindowBlocked:
				t.Fatalf("unexpected final window result %q", w.Result)
			}
		}
		if passed < 1 {
			t.Fatalf("batch %d has %d passed windows, want at least 1", bi, passed)
		}
		if b.ActualPercentage != b.Percentage {
			t.Fatalf("batch %d actual pct = %d, want %d", bi, b.ActualPercentage, b.Percentage)
		}
		// 多个 passed 窗口只可能来自 ready 状态被暂停后重新观察（仍是同一批、同一安全比例）。
		if passed > 1 && interrupted == 0 && len(b.Windows) == passed {
			t.Fatalf("batch %d passed %d times without any intervening interruption", bi, passed)
		}
	}
}

// 并发重复推进：ready 状态下只有一个推进成功，不会重复放量/跳批。
func TestConcurrentAdvanceOnlyOnce(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-conc-a-0", []RuleInput{{Feature: "a", Version: 1, Percentage: 0}})
	plan, _ := s.StartRecovery("rcv-conc-a", "a", 1, []RecoveryBatchInput{
		{Percentage: 10, ObserveCount: 1, FailureThreshold: 0},
		{Percentage: 40, ObserveCount: 1, FailureThreshold: 0},
	})
	plan = passBatch(t, s, plan, 0, 0)
	plan, _ = s.GetRecovery("rcv-conc-a")
	if plan.Status != RecoveryReady {
		t.Fatalf("precondition: status=%s", plan.Status)
	}

	const n = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, rejected := 0, 0
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := s.AdvanceRecovery("rcv-conc-a")
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
		t.Fatalf("advance: ok=%d rejected=%d", ok, rejected)
	}
	plan, _ = s.GetRecovery("rcv-conc-a")
	if plan.CurrentBatch != 1 || plan.Status != RecoveryObserving {
		t.Fatalf("plan after concurrent advance: %+v", plan)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 40 {
		t.Fatalf("pct = %d, want 40", pct)
	}
	if wins := plan.Batches[1].Windows; len(wins) != 1 || wins[0].Observed != 0 {
		t.Fatalf("batch1 window must be opened exactly once: %+v", wins)
	}
}

// 多个功能的恢复互不干扰：各自的闸门、批次、状态独立。
func TestMultipleFeaturesIndependentRecovery(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-mf-1", []RuleInput{
		{Feature: "a", Version: 1, Percentage: 0},
		{Feature: "b", Version: 1, Percentage: 0},
	})
	pa, _ := s.StartRecovery("rcv-mf-a", "a", 1,
		[]RecoveryBatchInput{{Percentage: 10, ObserveCount: 1, FailureThreshold: 0}})
	pb, _ := s.StartRecovery("rcv-mf-b", "b", 1,
		[]RecoveryBatchInput{{Percentage: 50, ObserveCount: 1, FailureThreshold: 0}})
	if pa.ID == pb.ID {
		t.Fatal("distinct plans")
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 10 {
		t.Fatalf("a pct = %d", pct)
	}
	if pct, _ := s.EffectivePercentage("b"); pct != 50 {
		t.Fatalf("b pct = %d", pct)
	}
	// 暂停 a 不影响 b。
	if _, err := s.PauseRecovery("rcv-mf-a", "hold a"); err != nil {
		t.Fatalf("pause a: %v", err)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 0 {
		t.Fatalf("a paused pct = %d, want 0", pct)
	}
	if pct, _ := s.EffectivePercentage("b"); pct != 50 {
		t.Fatalf("b must be unaffected, pct = %d", pct)
	}
	if st := pb.Status; st != RecoveryObserving {
		t.Fatalf("b status changed: %s", st)
	}
}

// 观察中发布新版本：旧计划被取代，未收齐窗口保留 open 证据，闸门让位。
func TestPublishDuringObservationSupersedes(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-pd-1", []RuleInput{{Feature: "a", Version: 1, Percentage: 0}})
	_, _ = s.StartRecovery("rcv-pd", "a", 1, []RecoveryBatchInput{
		{Percentage: 20, ObserveCount: 5, FailureThreshold: 0},
	})
	if _, err := s.ReportObservation("rcv-pd",
		ObservationInput{Batch: 0, Attempt: 0, Success: true}); err != nil {
		t.Fatalf("report: %v", err)
	}
	// 观察中途发布 v2（50%）。
	mustPublish(t, s, "chg-pd-2", []RuleInput{{Feature: "a", Version: 2, Percentage: 50}})

	plan, _ := s.GetRecovery("rcv-pd")
	if plan.Status != RecoverySuperseded {
		t.Fatalf("status = %s", plan.Status)
	}
	w := plan.Batches[0].Windows[0]
	if w.Observed != 1 || w.Result != WindowOpen {
		t.Fatalf("partial evidence must remain: %+v", w)
	}
	if pct, _ := s.EffectivePercentage("a"); pct != 50 {
		t.Fatalf("gate released, pct = %d, want 50", pct)
	}
	// v2 上可以正常起新恢复，旧 changeID 仍然冲突。
	p2, err := s.StartRecovery("rcv-pd2", "a", 2,
		[]RecoveryBatchInput{{Percentage: 80, ObserveCount: 1, FailureThreshold: 0}})
	if err != nil {
		t.Fatalf("new recovery on v2: %v", err)
	}
	if p2.PausedSafePct != 50 {
		t.Fatalf("baseline = %d, want 50", p2.PausedSafePct)
	}
}

// 完成后从 100% 无法再提恢复（首批比例必须高于安全比例），但不再有进行中计划。
func TestNoRecoveryAfterFullHundred(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-full-1", []RuleInput{{Feature: "a", Version: 1, Percentage: 0}})
	plan, _ := s.StartRecovery("rcv-full", "a", 1,
		[]RecoveryBatchInput{{Percentage: 100, ObserveCount: 1, FailureThreshold: 0}})
	plan = passBatch(t, s, plan, 0, 0)
	if plan.Status != RecoveryCompleted {
		t.Fatalf("status = %s", plan.Status)
	}
	_, err := s.StartRecovery("rcv-full-2", "a", 1,
		[]RecoveryBatchInput{{Percentage: 100, ObserveCount: 1, FailureThreshold: 0}})
	expectKind(t, err, KindParam)
}

// 同号请求在计划推进、暂停、阻断等任意阶段重放，都返回同一份计划现状。
func TestStartRecoveryIdempotentAcrossStates(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-ir-1", []RuleInput{{Feature: "a", Version: 1, Percentage: 0}})
	batches := []RecoveryBatchInput{
		{Percentage: 10, ObserveCount: 1, FailureThreshold: 0},
		{Percentage: 50, ObserveCount: 1, FailureThreshold: 0},
	}
	p1, err := s.StartRecovery("rcv-ir", "a", 1, batches)
	if err != nil {
		t.Fatal(err)
	}
	// 推进到第二批完成。
	p1 = passBatch(t, s, p1, 0, 0)
	p1, _ = s.AdvanceRecovery("rcv-ir")
	p1 = passBatch(t, s, p1, 1, 0)

	p2, err := s.StartRecovery("rcv-ir", "a", 1, batches)
	if err != nil {
		t.Fatalf("idempotent replay after completion: %v", err)
	}
	if p2.Status != RecoveryCompleted || p2.SafePercentage != 50 ||
		len(p2.Batches[1].Windows) != 1 || p2.CurrentBatch != 1 {
		t.Fatalf("replay must return current plan state: %+v", p2)
	}
}
