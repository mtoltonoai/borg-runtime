use super::*;

// Traceability scope: ExecutionUncertain and ProcessTreeStopped preserve selected_checkpoint independently of result/uncertainty.
//= spec/impl/v2/implementation.md#loss-returns-to-the-last-checkpoint
//# The task record MUST distinguish preserved work from work whose outcome is unknown after worker loss.
// Traceability scope: AcceptedResult is committed to the task independently of worker/executor process lifetime.
//= spec/impl/v2/implementation.md#the-supplied-executor-owns-its-inner-loop
//# The platform MUST keep accepted task results independently of the supplied executor's process.
// Traceability scope: Positive browser root-result navigation is a separate test gap; validation strings are not protected-check proof. The load oracle asserts exact selected checkpoint and three returned check outcomes; it does not provide protected checker provenance.
//= spec/impl/v2/implementation.md#observable-end-to-end-result
//# The initial system MUST return references to the source changes and validation results produced by a successful task.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# Every transition into a retry-related blocked state MUST publish its reason as a durable event.
pub(super) fn observe(
    tx: &Connection,
    r: &Report,
    cfg: &ControllerConfig,
    now: u64,
    validated: &Context<'_>,
) -> Result<(Data, bool)> {
    let limits = tx.limits;
    if !valid_id(limits, &r.worker_id) || !valid_id(limits, &r.incarnation) || !valid_id(limits, &r.attempt_id) {
        return refuse("invalid_input", "invalid report identity");
    }
    if matches!(r.observation, Observation::NativeRequestDiagnostic { .. }) {
        // The immutable charge supplies authority. A late observation cannot
        // change current task ownership, health, usage or retry permission.
        return super::super::native_request_diagnostics::observe(tx, r);
    }
    let mut t = owned(tx, &r.task_id, &r.attempt_id, r.generation, false)?;
    let a = t
        .task
        .attempt
        .as_ref()
        .ok_or_else(|| Fault::Store("current attempt missing".into()))?;
    if a.worker_id != r.worker_id || a.worker_incarnation != r.incarnation {
        return refuse(
            "stale_incarnation",
            "report does not identify the assigned worker incarnation",
        );
    }
    let body: Option<String> = tx
        .query_row(
            "SELECT body FROM workers WHERE id=?1",
            [&r.worker_id],
            |row| row.get(0),
        )
        .optional()?;
    let current: Worker = decode(
        &body.ok_or_else(|| Fault::Refused("unknown_worker", "worker not registered".into()))?,
    )?;
    if current.incarnation != r.incarnation {
        return refuse("stale_incarnation", "worker incarnation was superseded");
    }
    super::task_tool::progress(tx, &t.task, r, now)?;
    let mut supplied_wait=None;
    match &r.observation {
        Observation::SuppliedOuter { observation } => supplied_wait=super::supplied_outer::observe(tx, &mut t, r, observation, now)?,
        Observation::NativeRequestDiagnostic { .. } =>
            return super::super::native_request_diagnostics::observe(tx, r),
        Observation::NativeOutputCapped { .. } => super::native_output_cap::validate_report(tx, &t.task, r)?,
        Observation::NativeFormatFailure { .. } => super::native_format::validate_report(tx, &t.task, r)?,
        Observation::NativeRequestRejected { .. } => super::native_retry::validate_report(tx, &t.task, r)?,
        Observation::NativeRetryBlocked { .. } => super::native_retry::block(tx, &mut t, r, now)?,
        Observation::NativeLoopCompleted {
            model_requests,
            calls,
            requests,
            final_response_index,
        } => {
            super::native_loop::completed(tx, &t.task, r, *model_requests, calls, requests, *final_response_index, now)?;
        }
        Observation::Started { thread_id, turn_id } => {
            if terminal(t.task.status) || t.task.status == TaskStatus::Blocked
                || t.task.native_retry_block.is_some() || a.stopped || a.phase == AttemptPhase::StopRequested {
                return refuse("invalid_transition", "cannot start this attempt");
            }
            if thread_id.as_ref().is_some_and(|x| !valid_id(limits, x))
                || turn_id.as_ref().is_some_and(|x| !valid_id(limits, x))
            {
                return refuse("invalid_input", "invalid executor association");
            }
            let previous: (Option<String>, Option<String>) = tx.query_row(
                "SELECT thread_id,turn_id FROM attempts WHERE id=?1",
                [&r.attempt_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            if previous
                .0
                .as_ref()
                .is_some_and(|s| Some(s) != thread_id.as_ref())
                || previous
                    .1
                    .as_ref()
                    .is_some_and(|s| Some(s) != turn_id.as_ref())
            {
                return refuse(
                    "operation_conflict",
                    "attempt already has another executor association",
                );
            }
            tx.execute(
                "UPDATE attempts SET thread_id=?2,turn_id=?3 WHERE id=?1",
                params![r.attempt_id, thread_id, turn_id],
            )?;
            t.task.attempt.as_mut().unwrap().phase = AttemptPhase::Running;
        }
        Observation::Progress {
            text,
            output,
            truncated,
        } => {
            if text.len() > limits.controller.max_progress_bytes {
                return refuse("capacity", "progress/output exceeds task bound: controller.max_progress_bytes");
            }
            if output.len() > t.task.input.limits.output_bytes {
                return refuse("capacity", "progress/output exceeds task bound: task.input.limits.output_bytes");
            }
            // Snapshot semantics: repeated/deduped transport cannot append output
            // twice. The producer reports whether its retained snapshot was cut.
            t.task.progress = text.clone();
            t.task.recent_output = output.clone();
            t.task.output_truncated = *truncated;
        }
        //= spec/impl/v2/implementation.md#task-state-separates-execution-from-acceptance
        //# An executor outcome MUST remain distinct from the task's accepted outcome.
        Observation::ExecutorOutcome { success, summary } => {
            if summary.len() > t.task.input.limits.output_bytes {
                return refuse("capacity", "executor summary exceeds task bound: task.input.limits.output_bytes");
            }
            if let Some(old) = t.executor_success {
                if old != *success {
                    return refuse(
                        "operation_conflict",
                        "executor terminal outcome already differs",
                    );
                }
            }
            t.executor_success = Some(*success);
            tx.execute(
                "UPDATE attempts SET executor_outcome=?2 WHERE id=?1",
                params![r.attempt_id, encode(&r.observation)?],
            )?;
        }
        //= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
        //# A cancellation acknowledgment MUST distinguish requested cancellation from verified execution termination.
        Observation::ProcessTreeStopped => {
            // Capture actual pre-stop eligibility in this original receipt.
            // Typed unavailable preserves the physical stop and blocks later success.
            super::super::governance_completion::capture_before_stop(tx, &t, r, now)?;
            tx.execute(
                "UPDATE commands SET acknowledged=1 WHERE attempt_id=?1",
                [&r.attempt_id],
            )?;
            tx.execute(
                "UPDATE attempts SET control_remaining=0 WHERE id=?1",
                [&r.attempt_id],
            )?;
            let uncertain = t.unresolved_execution || pending_effects(tx, &t.task.id)? > 0;
            let a = t.task.attempt.as_mut().unwrap();
            //= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
            //# A task waiting for delegated work MUST release its execution slot.
            a.stopped = true;
            a.phase = AttemptPhase::Stopped;
            a.uncertain = uncertain;
            let settled:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM waits WHERE parent=?1 AND parent_attempt=?2 AND state='settled')",params![t.task.id,r.attempt_id],|row|row.get(0))?;
            if !terminal(t.task.status) && t.task.status != TaskStatus::Blocked
                && t.task.native_retry_block.is_none() && t.task.waiting_child_id.is_none() && settled {
                t.task.status = TaskStatus::Queued;
                t.task.blocked_reason = Some("child_outcome_ready".into());
            }
            super::super::governance_completion::mark_stopped_hold(&mut t);
        }
        Observation::ExecutionUncertain { message } => {
            if message.len() > limits.controller.max_progress_bytes {
                return refuse("capacity", "uncertainty description exceeds bound: controller.max_progress_bytes");
            }
            t.unresolved_execution = true;
            t.task.attempt.as_mut().unwrap().uncertain = true;
            if !terminal(t.task.status) {
                t.task.status = if t.task.pending_assignment.is_some() {TaskStatus::Blocked} else {TaskStatus::Failed};
                t.task.blocked_reason = Some("execution_uncertain".into());
                stop(tx, &mut t)?;
            }
        }
        // Traceability scope: Same-attempt cumulative snapshots cannot decrease; exact old receipts replay before this update, and successor admission sums retained attempts.
        //= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
        //# Reconnecting or resuming an attempt MUST NOT reset its recorded model-request accounting.
        Observation::Usage {
            model_requests,
            input_tokens,
            output_tokens,
            ..
        } => {
            let stored: (u32, Option<i64>, Option<i64>) = tx.query_row(
                "SELECT model_requests,input_tokens,output_tokens FROM attempts WHERE id=?1",
                [&r.attempt_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
            let old = (
                stored.0,
                stored.1.map(unsigned).transpose()?,
                stored.2.map(unsigned).transpose()?,
            );
            if *model_requests < old.0
                || old.1.is_some_and(|n| input_tokens.is_none_or(|v| v < n))
                || old.2.is_some_and(|n| output_tokens.is_none_or(|v| v < n))
            {
                return refuse(
                    "invalid_input",
                    "cumulative usage cannot decrease or become unknown",
                );
            }
            let input = input_tokens.map(integer).transpose()?;
            let output = output_tokens.map(integer).transpose()?;
            super::super::native_request_diagnostics::reserve(tx, &t.task, r, old.0, cfg)?;
            tx.execute("UPDATE attempts SET model_requests=?2,input_tokens=?3,output_tokens=?4 WHERE id=?1",params![r.attempt_id,model_requests,input,output])?;
            if used_model_requests(tx, &t.task.id)? + u64::from(t.delegated_model_requests)
                > u64::from(t.task.input.limits.model_requests)
                && !terminal(t.task.status)
            {
                t.task.status = TaskStatus::Failed;
                t.task.blocked_reason = Some("model_request_budget_exceeded".into());
                stop(tx, &mut t)?;
            }
        }
        Observation::ResultCandidate {
            checks_passed,
            summary,
            checkpoint,
            validation,
            output,
        } => {
            //= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
            //# Successful completion and cancellation MUST be ordered by the controller's committed task revision.
            if t.task.status != TaskStatus::Active || a.phase == AttemptPhase::StopRequested {
                return refuse(
                    "invalid_transition",
                    "this execution phase cannot accept a task result",
                );
            }
            if t.task.waiting_child_id.is_some() {
                return refuse("invalid_transition", "parent still waits for child");
            }
            if now >= t.task.input.limits.deadline_unix_ms {
                return refuse("deadline", "result arrived after original deadline");
            }
            if t.unresolved_execution || pending_effects(tx, &t.task.id)? > 0 {
                return refuse(
                    "reconciliation_required",
                    "execution/effect outcome remains uncertain",
                );
            }
            // Received-only recovery can fail platform acceptance after actual
            // cleanup without inventing an executor that ran or exited. Keep
            // the stored executor outcome absent, and never use this path for
            // success, live execution, or an uncertain attempt.
            let success = match t.executor_success {
                Some(executor_success) => executor_success && *checks_passed,
                None if !*checks_passed && a.stopped && !a.uncertain => false,
                None => {
                    return refuse(
                        "invalid_transition",
                        "executor outcome has not been observed",
                    );
                }
            };
            if summary.len() > t.task.input.limits.output_bytes {
                return refuse("capacity", "candidate output exceeds task bounds: task.input.limits.output_bytes");
            }
            if validation.len() > limits.controller.max_validation_strings {
                return refuse("capacity", "candidate output exceeds task bounds: controller.max_validation_strings");
            }
            if validation.iter().any(|s| s.len() > limits.controller.max_validation_string_bytes) {
                return refuse("capacity", "candidate output exceeds task bounds: controller.max_validation_string_bytes");
            }
            if bounded_json_size(output, t.task.input.limits.output_bytes).is_err() {
                return refuse("capacity", "candidate output exceeds task bounds: task.input.limits.output_bytes");
            }
            //= spec/impl/v2/implementation.md#observable-end-to-end-result
            //# A task MUST NOT be reported successful solely because its executor exited successfully.
            let pull_request = if success {
                super::super::pull_request::accepted_reference(tx, &t.task)?
            } else { None };
            let accepted = success.then(|| AcceptedResult {
                pull_request,
                attempt_id: r.attempt_id.clone(),
                summary: summary.clone(),
                checkpoint: checkpoint.clone(),
                validation: validation.clone(),
                output: output.clone(),
            });
            if accepted.as_ref().is_some_and(|result| {
                !crate::delegation::accepted_result_fits(&t.task.input.limits, result)
            }) {
                return refuse(
                    "capacity",
                    "complete accepted result exceeds configured result bound: task.input.limits.result_bytes",
                );
            }
            if success {
                // The schema helper validates this exact small candidate outside
                // the transaction. Obtain its one-use proof before scanning the
                // potentially large original provider output: the first pass
                // rolls back for this ticket and must not repeat that scan.
                // Final dispatch still checks every occurrence below in its own
                // transaction, with all owner/input and final clock guards.
                if t.task.input.definition.executor == ExecutorKind::Native || (t.task.input.output_schema.is_some() && super::super::sessions::is_successor(tx,&t.task.session_id)?) {
                    let output = output.as_ref().ok_or_else(|| {
                        Fault::Refused("invalid_input", "candidate lacks its original required output".into())
                    })?;
                    validated.require(&t.task.input, Some(output), Some(&t), now)?;
                    if t.task.input.definition.executor==ExecutorKind::Native { super::super::native_request_diagnostics::require_attempt_terminals(tx, &t.task)?; }
                }
                super::task_tool::acceptance(tx, &t.task, validation)?;
                super::native_loop::acceptance(tx, &t.task, validation)?;
                match t.task.input.definition.executor {
                    ExecutorKind::Coding => {
                        if validation.len() < t.task.input.acceptance_commands.len() {
                            return refuse("invalid_input", "missing declared validation outcomes");
                        }
                        if t.task.selected_checkpoint.is_none()
                            || checkpoint.as_ref() != t.task.selected_checkpoint.as_ref()
                        {
                            return refuse(
                                "checkpoint_unsettled",
                                "candidate must identify its acknowledged selected checkpoint",
                            );
                        }
                    }
                    ExecutorKind::Native => {
                        let requests: u32 = tx.query_row(
                            "SELECT model_requests FROM attempts WHERE id=?1",
                            [&r.attempt_id],
                            |row| row.get(0),
                        )?;
                        if requests == 0 && !direct_child_continuation(&t) {
                            return refuse(
                                "usage_unavailable",
                                "native request usage has not been observed",
                            );
                        }
                    }
                }
            }
            let final_now = (cfg.clock)();
            if final_now < now {
                return refuse(
                    "clock_unavailable",
                    "clock moved backwards during result validation",
                );
            }
            if final_now >= t.task.input.limits.deadline_unix_ms {
                return refuse(
                    "deadline",
                    "result validation crossed the original deadline",
                );
            }
            // All original result, usage, acceptance and final-clock guards above
            // remain mandatory. Historical reconciliation cannot mint a new decision.
            super::super::governance_completion::before_result(tx, &t, r, *checks_passed, final_now)?;
            t.task.status = if success {
                TaskStatus::Succeeded
            } else if t.task.pending_assignment.is_some() {
                TaskStatus::Blocked
            } else {
                TaskStatus::Failed
            };
            t.task.blocked_reason = if success {
                None
            } else {
                Some("acceptance_failed".into())
            };
            if success {
                t.task.result = accepted;
            }
        }
    }
    save(tx, &mut t, "worker_observation", &r.observation)?;
    settle_parent(tx, &t)?;
    Ok((supplied_wait.map(Data::SuppliedOuterWait).unwrap_or_else(||Data::Task(Box::new(t.task))), true))
}
