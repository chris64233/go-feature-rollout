package featurerollout

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// 本文件实现自动保护暂停发布后的分批恢复：
//
//   - 被保护机制拦下的流量不能一次性全部打开，只能按恢复计划一批一批放量；
//   - 计划写明每个批次的版本、比例、观察数量、失败阈值；
//   - 一批放量后必须开一个全新的观察窗口，收齐 ObserveCount 个结果，
//     窗口内失败数超过 FailureThreshold 即阻断，比例退回观察前的安全比例；
//   - 观察通过只是"允许推进"，必须显式 AdvanceRecovery 才开下一批，
//     上一批没观察完绝不能开下一批；
//   - 中途人工暂停后再次恢复，从上一个已完成观察的安全比例重新开窗，
//     旧窗口结果归属于旧批次/旧开窗轮次，不能被新窗口复用；
//   - 计划取消时保留已完成批次，剩余流量停在最后一个安全比例。
//
// 所有恢复操作与发布共用同一把写锁：上报、暂停、恢复、推进并发交错时
// 状态机串行迁移，旧批次结果既不能推进新批次，也不会重复放量。

func nowUTC() time.Time { return time.Now().UTC() }

func openWindow(attempt int) *BatchWindow {
	return &BatchWindow{Attempt: attempt, StartedAt: nowUTC(), Result: WindowOpen}
}

func manualPauseReason(reason string) string {
	if reason == "" {
		return "manual pause before observation completed"
	}
	return "manual pause: " + reason
}

// recoveryHash 计算恢复请求内容的规范化哈希，用于"同号异内容/版本"冲突检测。
func recoveryHash(feature string, version, safePct int, batches []RecoveryBatchInput) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s@%d from=%d\n", feature, version, safePct)
	for _, b := range batches {
		fmt.Fprintf(h, "pct=%d observe=%d maxfail=%d\n", b.Percentage, b.ObserveCount, b.FailureThreshold)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// recoveryGatePctLocked 返回某功能当前规则受恢复闸门约束后的实际放量比例。
// 没有恢复计划、计划不控制该功能或版本已漂移时第二个返回值为 false。
// 调用方持有读锁或写锁。
func (s *Service) recoveryGatePctLocked(feature string, rulePct int) (int, bool) {
	planID, ok := s.gateRecovery[feature]
	if !ok {
		return rulePct, false
	}
	plan := s.recoveryPlans[planID]
	if plan == nil {
		return rulePct, false
	}
	// 计划启动后该功能发布了其他版本：恢复闸门不再适用（对旧计划的操作会报冲突）。
	if r, ok := s.currentSnap.Rules[feature]; !ok || r.Version != plan.Version {
		return rulePct, false
	}
	switch plan.Status {
	case RecoveryObserving:
		return plan.Batches[plan.CurrentBatch].Percentage, true
	case RecoveryReady, RecoveryPaused, RecoveryBlocked, RecoveryCancelled, RecoveryCompleted:
		// 观察通过等待推进 / 暂停 / 阻断 / 取消 / 完成：停在安全比例。
		return plan.SafePercentage, true
	}
	return rulePct, false
}

// checkPlanVersionLocked 校验计划所针对的功能版本仍然是当前规则版本。
func (s *Service) checkPlanVersionLocked(plan *RecoveryPlan) error {
	r, ok := s.currentSnap.Rules[plan.Feature]
	if !ok {
		return conflictErr("recovery %q: feature %q no longer has a current rule", plan.ChangeID, plan.Feature)
	}
	if r.Version != plan.Version {
		return conflictErr("recovery %q targets version %d of feature %q, but current version is %d",
			plan.ChangeID, plan.Version, plan.Feature, r.Version)
	}
	return nil
}

func (s *Service) planByChangeLocked(changeID string) (*RecoveryPlan, error) {
	planID, ok := s.recoveryChange[changeID]
	if !ok {
		return nil, notFoundErr("recovery %q not found", changeID)
	}
	return s.recoveryPlans[planID], nil
}

// ---------------- 创建/启动 ----------------

// StartRecovery 为某个已发布功能版本创建并启动分批恢复计划。
//
// 创建成功后第一批立即放量并打开观察窗口。按 changeID 幂等：
// 同一 changeID 重复提交且内容（功能、版本、起点安全比例、批次）一致时返回原计划；
// 内容或版本变化返回 KindConflict。
func (s *Service) StartRecovery(changeID, feature string, version int, batches []RecoveryBatchInput) (*RecoveryPlan, error) {
	if changeID == "" {
		return nil, paramErr("changeID is empty")
	}
	if feature == "" {
		return nil, paramErr("feature name is empty")
	}
	if len(batches) == 0 {
		return nil, paramErr("recovery plan for %q has no batches", feature)
	}
	lastPct := -1
	for i, b := range batches {
		if b.Percentage < 0 || b.Percentage > 100 {
			return nil, paramErr("recovery batch %d percentage must be in [0,100], got %d", i, b.Percentage)
		}
		if b.ObserveCount < 1 {
			return nil, paramErr("recovery batch %d observe count must be >= 1, got %d", i, b.ObserveCount)
		}
		if b.FailureThreshold < 0 || b.FailureThreshold >= b.ObserveCount {
			return nil, paramErr("recovery batch %d failure threshold must satisfy 0 <= threshold < observe count (%d), got %d",
				i, b.ObserveCount, b.FailureThreshold)
		}
		if i > 0 && b.Percentage <= lastPct {
			return nil, paramErr("recovery batch percentages must be strictly increasing, batch %d pct %d <= %d",
				i, b.Percentage, lastPct)
		}
		lastPct = b.Percentage
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// 幂等：同号成功创建过，比对内容（含起点安全比例）。
	if prevID, seen := s.recoveryChange[changeID]; seen {
		prev := s.recoveryPlans[prevID]
		hash := recoveryHash(feature, version, prev.PausedSafePct, batches)
		if prev.Feature == feature && prev.Version == version && s.recoveryChangeHash[changeID] == hash {
			return cloneRecoveryPlan(prev), nil
		}
		return nil, conflictErr("recovery changeID %q already used with different content or version", changeID)
	}

	rule, ok := s.currentSnap.Rules[feature]
	if !ok {
		return nil, notFoundErr("feature %q has no current rule", feature)
	}
	if rule.Version != version {
		return nil, conflictErr("recovery targets version %d of feature %q, but current version is %d",
			version, feature, rule.Version)
	}

	// 同一功能只能有一个进行中的恢复。
	if activeID := s.activeRecovery[feature]; activeID != "" {
		active := s.recoveryPlans[activeID]
		return nil, conflictErr("feature %q already has an %s recovery %q",
			feature, active.Status, active.ChangeID)
	}

	// 恢复起点默认为规则比例；若上一份恢复已终结（blocked/cancelled/completed）
	// 且仍把流量停在安全比例，新计划从该安全比例开始，并取代旧闸门
	//（旧计划保留审计证据，标记 superseded）。
	safePct := rule.Percentage
	if prevGateID := s.gateRecovery[feature]; prevGateID != "" {
		if prev := s.recoveryPlans[prevGateID]; prev != nil && prev.Version == version {
			safePct = prev.SafePercentage
			prev.Status = RecoverySuperseded
			prev.BlockReason = "replaced by a new recovery plan"
			prev.UpdatedAt = nowUTC()
		}
	}
	if batches[0].Percentage <= safePct {
		return nil, paramErr("first recovery batch percentage %d must be greater than paused safe percentage %d",
			batches[0].Percentage, safePct)
	}

	plan := &RecoveryPlan{
		ID:             "recovery-" + changeID,
		ChangeID:       changeID,
		Feature:        feature,
		Version:        version,
		PausedSafePct:  safePct,
		SafePercentage: safePct,
		CurrentBatch:   0,
		Status:         RecoveryObserving,
		CreatedAt:      nowUTC(),
		UpdatedAt:      nowUTC(),
	}
	for _, b := range batches {
		plan.Batches = append(plan.Batches, &RecoveryBatch{
			Percentage:       b.Percentage,
			ObserveCount:     b.ObserveCount,
			FailureThreshold: b.FailureThreshold,
		})
	}
	// 第一批立即放量并打开第一个观察窗口。
	plan.Batches[0].Windows = append(plan.Batches[0].Windows, openWindow(0))

	s.recoveryPlans[plan.ID] = plan
	s.recoveryChange[changeID] = plan.ID
	s.recoveryChangeHash[changeID] = recoveryHash(feature, version, safePct, batches)
	s.activeRecovery[feature] = plan.ID
	s.gateRecovery[feature] = plan.ID
	return cloneRecoveryPlan(plan), nil
}

// ---------------- 结果上报 ----------------

// ReportObservation 上报当前观察窗口内的一个结果（成功或失败）。
//
// 只有"当前批次、当前开窗轮次"的结果才被接受；旧批次、暂停前旧窗口、
// 已关闭窗口的结果一律返回 KindConflict（并计入窗口 LateResults 留痕），
// 不能推进新批次，也不会重复计数、重复放量。
func (s *Service) ReportObservation(changeID string, in ObservationInput) (*ObservationReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	plan, err := s.planByChangeLocked(changeID)
	if err != nil {
		return nil, err
	}
	if in.Batch < 0 || in.Batch >= len(plan.Batches) {
		return nil, paramErr("batch index %d out of range [0,%d)", in.Batch, len(plan.Batches))
	}
	if err := s.checkPlanVersionLocked(plan); err != nil {
		return nil, err
	}
	batch := plan.Batches[in.Batch]

	// 非观察中状态 / 非当前批次：旧批次结果不能推进新批次。
	if plan.Status != RecoveryObserving || in.Batch != plan.CurrentBatch {
		recordLateResult(batch, in.Attempt)
		return nil, conflictErr("stale observation for recovery %q: batch %d attempt %d is not the active window (status=%s, current batch=%d)",
			changeID, in.Batch, in.Attempt, plan.Status, plan.CurrentBatch)
	}
	win := batch.Windows[len(batch.Windows)-1]
	// 旧开窗轮次（暂停前的窗口）的结果不能计入新窗口。
	if in.Attempt != win.Attempt {
		recordLateResult(batch, in.Attempt)
		return nil, conflictErr("stale observation for recovery %q batch %d: attempt %d != current attempt %d",
			changeID, in.Batch, in.Attempt, win.Attempt)
	}
	if win.Result != WindowOpen {
		win.LateResults++
		return nil, conflictErr("observation window of recovery %q batch %d already %s",
			changeID, in.Batch, win.Result)
	}

	win.Observed++
	if !in.Success {
		win.Failures++
	}
	plan.UpdatedAt = nowUTC()

	receipt := &ObservationReceipt{
		PlanID:         plan.ID,
		Feature:        plan.Feature,
		Batch:          in.Batch,
		Attempt:        win.Attempt,
		Observed:       win.Observed,
		Failures:       win.Failures,
		ObserveTarget:  batch.ObserveCount,
		SafePercentage: plan.SafePercentage,
		PlanStatus:     plan.Status,
	}
	if win.Observed < batch.ObserveCount {
		return receipt, nil
	}

	// 观察窗口收齐：失败数严格归属于当批次当次开窗，决定本批通过还是阻断。
	win.ClosedAt = nowUTC()
	receipt.WindowClosed = true
	if win.Failures > batch.FailureThreshold {
		// 恢复失败：保持观察前的安全比例，本批不放行，阻断原因留痕。
		win.Result = WindowBlocked
		win.BlockReason = fmt.Sprintf("failures %d exceeded threshold %d within %d observations",
			win.Failures, batch.FailureThreshold, win.Observed)
		plan.Status = RecoveryBlocked
		plan.BlockReason = fmt.Sprintf("batch %d (pct %d) blocked: %s",
			in.Batch, batch.Percentage, win.BlockReason)
		batch.ActualPercentage = plan.SafePercentage
		delete(s.activeRecovery, plan.Feature) // 闸门保留在 gateRecovery：停在安全比例
		receipt.Blocked = true
		receipt.BlockReason = plan.BlockReason
	} else {
		// 观察通过：本批比例成为新安全比例；下一批必须显式推进才打开。
		win.Result = WindowPassed
		plan.SafePercentage = batch.Percentage
		batch.ActualPercentage = batch.Percentage
		if in.Batch == len(plan.Batches)-1 {
			plan.Status = RecoveryCompleted
			delete(s.activeRecovery, plan.Feature) // 全部放量，闸门留在最终安全比例
		} else {
			plan.Status = RecoveryReady
		}
		receipt.WindowPassed = true
	}
	receipt.SafePercentage = plan.SafePercentage
	receipt.PlanStatus = plan.Status
	plan.UpdatedAt = nowUTC()
	return receipt, nil
}

// recordLateResult 把被拒绝的旧结果记到对应批次匹配轮次的窗口上，便于审计；
// 找不到匹配窗口时记到最近一个窗口（至少存在一个已打开窗口）。
func recordLateResult(batch *RecoveryBatch, attempt int) {
	var win *BatchWindow
	for i := len(batch.Windows) - 1; i >= 0; i-- {
		if batch.Windows[i].Attempt == attempt {
			win = batch.Windows[i]
			break
		}
		if win == nil {
			win = batch.Windows[i]
		}
	}
	if win == nil && len(batch.Windows) > 0 {
		win = batch.Windows[len(batch.Windows)-1]
	}
	if win != nil {
		win.LateResults++
	}
}

// ---------------- 人工暂停 / 恢复 / 推进 / 取消 ----------------

// PauseRecovery 人工暂停恢复。观察未收齐的当前窗口立即作废（interrupted），
// 已收齐批次的结果不受影响；再次恢复时从当前安全比例重新开窗。
// 暂停操作幂等：重复暂停返回当前计划，不产生新状态。
func (s *Service) PauseRecovery(changeID, reason string) (*RecoveryPlan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	plan, err := s.planByChangeLocked(changeID)
	if err != nil {
		return nil, err
	}
	if err := s.checkPlanVersionLocked(plan); err != nil {
		return nil, err
	}
	switch plan.Status {
	case RecoveryPaused:
		return cloneRecoveryPlan(plan), nil
	case RecoveryObserving, RecoveryReady:
		if plan.Status == RecoveryObserving {
			windows := plan.Batches[plan.CurrentBatch].Windows
			win := windows[len(windows)-1]
			win.ClosedAt = nowUTC()
			win.Result = WindowInterrupted
			win.BlockReason = manualPauseReason(reason)
		}
		plan.Status = RecoveryPaused
		plan.BlockReason = manualPauseReason(reason)
		plan.UpdatedAt = nowUTC()
		return cloneRecoveryPlan(plan), nil
	default:
		return nil, conflictErr("recovery %q is %s and cannot be paused", changeID, plan.Status)
	}
}

// ResumeRecovery 从暂停中恢复：在当前批次以当前安全比例重新打开一个全新观察窗口
// （Attempt 加 1）。未观察完的旧窗口结果作废，不能计入新窗口。
//
// 暂停发生在 ready（上一批已通过、下一批未开）时，恢复会在当前安全比例上
// 重新开窗观察，而不是跳到下一批。
func (s *Service) ResumeRecovery(changeID string) (*RecoveryPlan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	plan, err := s.planByChangeLocked(changeID)
	if err != nil {
		return nil, err
	}
	if err := s.checkPlanVersionLocked(plan); err != nil {
		return nil, err
	}
	if plan.Status != RecoveryPaused {
		return nil, conflictErr("recovery %q is %s, not paused", changeID, plan.Status)
	}
	batch := plan.Batches[plan.CurrentBatch]
	nextAttempt := 0
	if len(batch.Windows) > 0 {
		nextAttempt = batch.Windows[len(batch.Windows)-1].Attempt + 1
	}
	batch.Windows = append(batch.Windows, openWindow(nextAttempt))
	plan.Status = RecoveryObserving
	plan.BlockReason = ""
	plan.UpdatedAt = nowUTC()
	return cloneRecoveryPlan(plan), nil
}

// AdvanceRecovery 在一批观察通过（status=ready）后打开下一批：
// 提高比例并打开新的观察窗口。未完成观察（observing/paused）时返回 KindConflict，
// 保证上一批没观察完就不能开下一批，也不会跳批、重复放量。
func (s *Service) AdvanceRecovery(changeID string) (*RecoveryPlan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	plan, err := s.planByChangeLocked(changeID)
	if err != nil {
		return nil, err
	}
	if err := s.checkPlanVersionLocked(plan); err != nil {
		return nil, err
	}
	if plan.Status != RecoveryReady {
		return nil, conflictErr("recovery %q is %s; next batch can only open after current batch observation passed",
			changeID, plan.Status)
	}
	next := plan.CurrentBatch + 1
	if next >= len(plan.Batches) {
		return nil, conflictErr("recovery %q has no next batch", changeID)
	}
	plan.CurrentBatch = next
	plan.Status = RecoveryObserving
	plan.Batches[next].Windows = append(plan.Batches[next].Windows, openWindow(0))
	plan.UpdatedAt = nowUTC()
	return cloneRecoveryPlan(plan), nil
}

// CancelRecovery 取消恢复计划：已经完成观察的批次全部保留，
// 未收齐的当前窗口作废（interrupted），剩余流量停在最后一个安全比例。
// 取消操作幂等。
func (s *Service) CancelRecovery(changeID, reason string) (*RecoveryPlan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	plan, err := s.planByChangeLocked(changeID)
	if err != nil {
		return nil, err
	}
	if err := s.checkPlanVersionLocked(plan); err != nil {
		return nil, err
	}
	switch plan.Status {
	case RecoveryCancelled:
		return cloneRecoveryPlan(plan), nil
	case RecoveryCompleted, RecoveryBlocked, RecoverySuperseded:
		return nil, conflictErr("recovery %q is %s and cannot be cancelled", changeID, plan.Status)
	}
	if plan.Status == RecoveryObserving {
		windows := plan.Batches[plan.CurrentBatch].Windows
		win := windows[len(windows)-1]
		win.ClosedAt = nowUTC()
		win.Result = WindowInterrupted
		win.BlockReason = "recovery cancelled before observation completed"
	}
	plan.Status = RecoveryCancelled
	if reason != "" {
		plan.BlockReason = "recovery cancelled: " + reason
	} else {
		plan.BlockReason = "recovery cancelled"
	}
	plan.UpdatedAt = nowUTC()
	delete(s.activeRecovery, plan.Feature) // 闸门保留：停在最后安全比例
	return cloneRecoveryPlan(plan), nil
}

// ---------------- 查询 ----------------

// GetRecovery 按恢复变更号读取计划：包含每批实际比例、观察结果与阻断原因。
func (s *Service) GetRecovery(changeID string) (*RecoveryPlan, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	planID, ok := s.recoveryChange[changeID]
	if !ok {
		return nil, notFoundErr("recovery %q not found", changeID)
	}
	return cloneRecoveryPlan(s.recoveryPlans[planID]), nil
}

// EffectivePercentage 返回某功能当前实际生效的放量比例：
// 处于恢复流程中时受恢复闸门约束（暂停/阻断/取消时为最后安全比例）。
// 功能不存在时第二个返回值为 false。
func (s *Service) EffectivePercentage(feature string) (int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rule, ok := s.currentSnap.Rules[feature]
	if !ok {
		return 0, false
	}
	pct, _ := s.recoveryGatePctLocked(feature, rule.Percentage)
	return pct, true
}

// cloneRecoveryPlan 返回计划的深拷贝，避免调用方持有内部可变状态。
func cloneRecoveryPlan(p *RecoveryPlan) *RecoveryPlan {
	cp := *p
	cp.Batches = make([]*RecoveryBatch, len(p.Batches))
	for i, b := range p.Batches {
		bc := *b
		bc.Windows = make([]*BatchWindow, len(b.Windows))
		for j, w := range b.Windows {
			wc := *w
			bc.Windows[j] = &wc
		}
		cp.Batches[i] = &bc
	}
	return &cp
}
