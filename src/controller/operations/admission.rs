//! One current predicate for queued diagnostics and actual worker admission.
//! A read does not reserve a slot, prove worker connectivity or create an owner.
use super::*;

pub(super) enum Decision {
    Blocked(String),
    BudgetExhausted,
    Ready {
        attempt: Attempt,
        command: WorkerCommand,
        next_counter: u64,
    },
}

fn common_reason(
    tx: &Connection,
    t: &StoredTask,
    cfg: &ControllerConfig,
    now: u64,
) -> Result<Option<String>> {
    //= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
    //# An automatic retry MUST wait until prior execution is known unable to continue.
    let reason = if t.task.status == TaskStatus::Blocked || t.task.native_retry_block.is_some() {
        Some("native_retry_blocked")
    } else if now >= t.task.input.limits.deadline_unix_ms {
        Some("execution_deadline")
    } else if t.task.waiting_child_id.is_some() {
        Some("waiting_child")
    } else if active(&t.task) {
        Some("prior_execution_active")
    } else {
        None
    };
    if let Some(r) = reason {
        return Ok(Some(r.into()));
    }
    if let Some(reason) = super::super::governance_control::admission_reason(tx, &t.task.id, now)? {
        return Ok(Some(reason));
    }

    // Traceability scope: Ordinary admission checks explicit open waits; collaboration adds no unconditional human-approval gate.
    //= spec/impl/v2/implementation.md#collaboration-surfaces-preserve-existing-authority
    //# The collaboration expansion MUST NOT require human approval of ordinary work that has no explicitly requested product wait.
    //= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
    //# A task with an open collaboration wait MUST be ineligible for a new execution attempt.

    if super::super::collaboration_work::admission_blocked(tx, &t.task.id)? {
        return Ok(Some("workflow_wait".into()));
    }
    if super::super::collaboration_notifications::delivery_blocks_admission(tx, &t.task)? {
        return Ok(Some("workflow_delivery_unresolved".into()));
    }
    if super::super::governance_completion::admission_blocked(tx, t, now)? {
        return Ok(Some("governance_completion_blocked".into()));
    }
    if let Err(reason) = t.task.input.definition.required_inference() {
        return Ok(Some(reason.into()));
    }
    if t.task.input.definition.execution_profile.is_none() {
        return Ok(Some("template_profile_unbound".into()));
    }
    //= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
    //# A task MUST start only after its prerequisites have succeeded.
    for dependency in &t.task.input.dependencies {
        let d = load(tx, dependency)?;
        if d.task.status != TaskStatus::Succeeded {
            return Ok(Some(format!(
                "prerequisite:{dependency}:{:?}",
                d.task.status
            )));
        }
    }
    if !regular_receipt_room(tx, cfg)? {
        return Ok(Some("receipt_capacity".into()));
    }
    //= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
    //# An automatic retry MUST wait until relevant uncertain external effects are reconciled.
    // Traceability scope: The selected provider interface has no outcome lookup for an unknown request; unresolved execution or effects therefore block admission instead of authorizing a resend.
    //= spec/impl/v2/implementation.md#platform-owned-model-retries
    //# A platform-owned retry of an uncertain provider effect MUST use the receiving interface's supported outcome reconciliation or deduplication before resending that effect.
    // Traceability scope: This executor-independent uncertainty fence also prevents a supplied-executor successor admission from creating a session or repeating an uncertain start merely for a provider retry.
    //= spec/impl/v2/implementation.md#platform-owned-model-retries
    //# Platform recovery of a supplied executor MUST NOT create another session or repeat an uncertain start merely to implement a provider retry.
    if t.unresolved_execution
        || t.task.attempt.as_ref().is_some_and(|a| a.uncertain)
        || pending_effects(tx, &t.task.id)? > 0
    {
        return Ok(Some("reconciliation_required".into()));
    }
    if t.task.completed_child.is_some() && !descendants_settled(tx, &t.task.id, cfg.max_tasks)? {
        return Ok(Some("child_reconciliation_required".into()));
    }
    //= spec/impl/v2/implementation.md#definitions-select-execution
    //# A session MUST have at most one active execution attempt at a time.
    // Traceability scope: Active-attempt exclusion is keyed by session, while executor capacity is checked separately; it is not an agent-wide singleton.
    //= spec/impl/v2/implementation.md#definitions-select-execution
    //# An agent identity MUST support multiple concurrent sessions.
    let busy: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM tasks WHERE session=?1 AND active=1)",
        [&t.task.session_id],
        |r| r.get(0),
    )?;
    if busy {
        return Ok(Some("session_busy".into()));
    }
    if !cleanup_available(
        tx,
        cfg,
        5u32.saturating_sub(t.cleanup_remaining).saturating_add(3),
    )? {
        return Ok(Some("cleanup_capacity".into()));
    }
    if t.task.input.native_workflow.is_some() && t.task.completed_child.is_none() {
        match configured_sources(tx, &t.task.input, cfg, now, Some(&t.task.id), t.depth) {
            Ok(()) => {}
            Err(Fault::Refused(code, _)) => {
                return Ok(Some(format!("configured_child_unavailable:{code}")));
            }
            Err(error) => return Err(error),
        }
    }
    Ok(None)
}

// Traceability scope: Each successor uses all prior attempt charges plus retained child reservations; accepted input limits are not rewritten.
//= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
//# Reconnecting or resuming an attempt MUST NOT reset its recorded model-request accounting.
fn remaining(tx: &Connection, t: &StoredTask) -> Result<u32> {
    let used = used_model_requests(tx, &t.task.id)?;
    Ok(u64::from(t.task.input.limits.model_requests)
        .saturating_sub(used.saturating_add(u64::from(t.delegated_model_requests))) as u32)
}
// Traceability scope: Executor-specific active attempts are compared with the registered worker capacity before a RunAttempt can be committed; this does not install host resource controls.
//= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
//# Admission MUST enforce configured queue and resource bounds.
fn worker_room(tx: &Connection, worker: &Worker, kind: ExecutorKind) -> Result<bool> {
    let capacity = match kind {
        ExecutorKind::Coding => worker.coding_capacity,
        ExecutorKind::Native => worker.native_capacity,
    };
    let used: i64 = tx.query_row("SELECT count(*) FROM tasks WHERE worker=?1 AND incarnation=?2 AND executor=?3 AND active=1",
        params![worker.id, worker.incarnation, executor(kind)], |r| r.get(0))?;
    Ok(unsigned(used)? < capacity as u64)
}

// These ordinary data values become a launch only when await_commands commits
// the exact attempt and command. Read paths merely measure the same bounded
// representation; they cannot expose a RunAttempt or persist these values.
// Traceability scope: Allocates a successor attempt/generation for the same task.
//= spec/impl/v2/implementation.md#interrupted-work-is-reconciled
//# Recovery that cannot resume available saved executor history MUST create a new attempt.
fn proposal(tx: &Connection, t: &StoredTask, worker: &Worker, remaining: u32, now: u64) -> Result<Decision> {
    if crate::task_tool::enabled(&t.task.input) && remaining < 2 {
        return Ok(Decision::BudgetExhausted);
    }
    if remaining == 0 && !direct_child_continuation(t) {
        return Ok(Decision::BudgetExhausted);
    }
    let generation = t
        .generation
        .checked_add(1)
        .ok_or_else(|| Fault::Store("generation exhausted".into()))?;
    let next_counter = t
        .attempt_counter
        .checked_add(1)
        .ok_or_else(|| Fault::Store("attempt counter exhausted".into()))?;
    // Traceability scope: Successor admission copies the accepted task deadline; reconnecting a controller does not assign a new clock origin.
    //= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
    //# Reconnecting or resuming an attempt MUST NOT reset its execution deadline.
    // Traceability scope: Successor attempts copy the retained task session identity instead of allocating another instantiation.
    //= spec/impl/v2/implementation.md#definitions-select-execution
    //# Recovery of a session MUST retain its session identity.
    let attempt = Attempt {
        session_id: t.task.session_id.clone(),
        id: format!("attempt-{}-{next_counter}", t.task.id),
        task_id: t.task.id.clone(),
        generation,
        worker_id: worker.id.clone(),
        worker_incarnation: worker.incarnation.clone(),
        phase: AttemptPhase::Assigned,
        deadline_unix_ms: t.task.input.limits.deadline_unix_ms,
        stopped: false,
        uncertain: false,
    };
    //= spec/impl/v2/implementation.md#definitions-select-execution
    //# An execution attempt MUST bind the definition revision it uses.
    // Traceability scope: Dispatch copies the immutable accepted definition; only the remaining task request allowance is adjusted.
    //= spec/impl/v2/implementation.md#definitions-select-execution
    //# An execution attempt MUST use the inference settings of its accepted definition revision.
    let mut input = t.task.input.clone();
    input.limits.model_requests = remaining;
    let session = super::super::sessions::get(tx, &t.task.session_id)?;
    session
        .validate_for(&attempt, &input)
        .map_err(|_| Fault::Store("session launch binding mismatch".into()))?;

    // Traceability scope: Resolved wait references join the ordinary durable RunAttempt command for the same task and session.
    //= spec/impl/v2/implementation.md#committed-resolutions-reach-their-consumers
    //# A committed resolution that makes a waiting task eligible MUST wake it through the existing platform command mechanism.

    let workflow_continuation = if let Some(limits) = tx.limits.workflow.as_ref() {
        let resolved_waits = super::super::collaboration_work::pending_resolution_refs(tx, &t.task.id, limits)?;
        if resolved_waits.is_empty() { None } else {
            if !matches!(t.task.input.definition.execution_profile, Some(ExecutionProfile::CollaborationV2 { .. }))
                || t.task.input.resolved_limits.as_ref().is_none_or(|accepted| accepted.global_metadata.workflow.is_none()) {
                return Ok(Decision::Blocked("workflow_delivery_profile_unavailable".into()));
            }
            Some(crate::collaboration::workflow::WorkflowContinuation {
                task_id: t.task.id.clone(), session_id: t.task.session_id.clone(), resolved_waits,
            })
        }
    } else { None };
    // Read the old stopped owner before await_commands replaces its attempt.
    let completion_continuation = super::super::governance_completion::resume_for_admission(tx, t, now)?;
    let command = WorkerCommand::RunAttempt {
        session: (session).into(),
        command_id: format!("run-{}", attempt.id),
        attempt: attempt.clone(),
        task: (input).into(),
        // Executor conversation associations do not select recovery source.
        //= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
        //# A recorded executor conversation identifier MUST NOT be treated as a durable checkpoint by itself.
        checkpoint: t.task.selected_checkpoint.clone(),
        continuation: (t.task.completed_child.clone()).map(Into::into),
        workflow_continuation,
        completion_continuation: (completion_continuation).map(Into::into),
    };
    if bounded_json_size(&command, tx.limits.controller.run_command_bytes).is_err() {
        return Ok(Decision::Blocked(
            "continuation_exceeds_command_bound".into(),
        ));
    }
    Ok(Decision::Ready {
        attempt,
        command,
        next_counter,
    })
}

pub(super) fn check(
    tx: &Connection,
    t: &StoredTask,
    worker: &Worker,
    cfg: &ControllerConfig,
    now: u64,
) -> Result<Decision> {
    if let Some(reason) = common_reason(tx, t, cfg, now)? {
        return Ok(Decision::Blocked(reason));
    }
    if !worker_room(tx, worker, t.task.input.definition.executor)? {
        return Ok(Decision::Blocked("executor_capacity".into()));
    }
    proposal(tx, t, worker, remaining(tx, t)?, now)
}

//= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
//# A rejected or deferred admission MUST expose its reason.
pub(super) fn current_reason(
    tx: &Connection,
    t: &StoredTask,
    cfg: &ControllerConfig,
    now: u64,
) -> Result<Option<String>> {
    if let Some(reason) = common_reason(tx, t, cfg, now)? {
        return Ok(Some(reason));
    }
    let remaining = remaining(tx, t)?;
    let mut q = tx.prepare("SELECT body FROM workers ORDER BY id")?;
    let mut rows = q.query([])?;
    let mut count = 0;
    let mut capacity = false;
    let mut delivery_blocked = false;
    while let Some(row) = rows.next()? {
        count += 1;
        if count > cfg.max_workers {
            return refuse("capacity", "worker registry exceeds view bound: controller.max_workers");
        }
        let worker: Worker = decode(&row.get::<_, String>(0)?)?;
        if !worker_room(tx, &worker, t.task.input.definition.executor)? {
            continue;
        }
        capacity = true;
        match proposal(tx, t, &worker, remaining, now)? {
            Decision::Ready { command, .. } => {
                let existing =
                    super::delivery_snapshot(tx, &worker.id, &worker.incarnation, cfg, true)?;
                // A diagnostic uses the smallest empty-ACK exchange. It is
                // not a reservation for a later caller's larger ACK/input frame.
                let view = Operation::AwaitCommands {
                    operation_id: "_".into(),
                    worker_id: worker.id.clone(),
                    incarnation: worker.incarnation.clone(),
                    acknowledged: Vec::new(),
                };
                if super::delivery_fits(&existing, &command, cfg, &view) {
                    return Ok(None);
                }
                delivery_blocked = true;
            }
            Decision::BudgetExhausted => return Ok(Some("model_request_budget_exhausted".into())),
            Decision::Blocked(_) => {}
        }
    }
    Ok(Some(
        if delivery_blocked {
            "command_delivery_capacity"
        } else if capacity {
            "continuation_exceeds_command_bound"
        } else {
            "executor_capacity"
        }
        .into(),
    ))
}
