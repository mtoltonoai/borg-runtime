//! Canonical supplied outer facts and fresh dispatch admission, in the original
//! ObserveWorker transaction. Receipt replay is history, never a new grant.
use super::*;
use crate::supplied_outer::{self as contract, Cause, Observation as Outer};

fn caller(r: &Report) -> crate::mcp::Caller {
    crate::mcp::Caller {
        task_id: r.task_id.clone(),
        attempt_id: r.attempt_id.clone(),
        generation: r.generation,
        worker_id: r.worker_id.clone(),
        incarnation: r.incarnation.clone(),
    }
}
fn owns(r: &Report, c: &crate::mcp::Caller) -> bool {
    r.task_id == c.task_id
        && r.attempt_id == c.attempt_id
        && r.generation == c.generation
        && r.worker_id == c.worker_id
        && r.incarnation == c.incarnation
}
pub(crate) fn original(
    tx: &Connection<'_>,
    c: &crate::mcp::Caller,
    index: u32,
    phase: &str,
) -> Result<Outer> {
    let id = contract::id(&c.attempt_id, index, phase);
    let record = super::super::metadata::read(tx, &id)?.ok_or_else(|| {
        Fault::Refused(
            "reconciliation_required",
            "supplied outer original receipt absent".into(),
        )
    })?;
    let Operation::ObserveWorker { report } = record.request else {
        return refuse(
            "source_mismatch",
            "supplied original is not a worker report",
        );
    };
    let accepted = match &record.outcome {
        Outcome::Ok {
            data: Data::Task(t),
        } => {
            t.id == c.task_id
                && t.input.definition.executor == ExecutorKind::Coding
                && t.attempt.as_ref().is_some_and(|a| {
                    a.id == c.attempt_id
                        && a.generation == c.generation
                        && a.worker_id == c.worker_id
                        && a.worker_incarnation == c.incarnation
                })
        }
        Outcome::Ok {
            data: Data::SuppliedOuterWait(wait),
        } => phase == "wait" && wait.index == index,
        _ => false,
    };
    if !owns(&report, c) || report.report_id != id || !accepted {
        return refuse(
            "source_mismatch",
            "supplied original owner or outcome differs",
        );
    }
    match report.observation {
        Observation::SuppliedOuter { observation }
            if observation.index() == index && observation.phase() == phase =>
        {
            Ok(observation)
        }
        _ => refuse("source_mismatch", "supplied original phase differs"),
    }
}
fn wait(tx: &Connection<'_>, c: &crate::mcp::Caller, index: u32) -> Result<contract::Wait> {
    original(tx, c, index, "wait")?;
    let record = super::super::metadata::read(tx, &contract::id(&c.attempt_id, index, "wait"))?
        .ok_or_else(|| Fault::Store("supplied wait disappeared".into()))?;
    match record.outcome {
        Outcome::Ok {
            data: Data::SuppliedOuterWait(wait),
        } => Ok(wait),
        _ => refuse("source_mismatch", "supplied wait outcome differs"),
    }
}

fn active(tx: &Connection<'_>, t: &StoredTask, now: u64) -> Result<()> {
    let a = t
        .task
        .attempt
        .as_ref()
        .ok_or_else(|| Fault::Store("supplied attempt absent".into()))?;
    if t.task.status != TaskStatus::Active
        || a.stopped
        || a.uncertain
        || a.phase == AttemptPhase::StopRequested
        || t.unresolved_execution
        || t.task.pending_assignment.is_some()
        || t.task.waiting_child_id.is_some()
        || now >= a.deadline_unix_ms
        || now >= t.task.input.limits.deadline_unix_ms
    {
        return refuse(
            "invalid_transition",
            "supplied continuation no longer current",
        );
    }
    let ended: bool = tx.query_row(
        "SELECT executor_outcome IS NOT NULL FROM attempts WHERE id=?1",
        [&a.id],
        |r| r.get(0),
    )?;
    if ended || pending_effects(tx, &t.task.id)? > 0 {
        return refuse(
            "reconciliation_required",
            "supplied continuation has unresolved effects or final outcome",
        );
    }
    Ok(())
}
fn previous(
    tx: &Connection<'_>,
    c: &crate::mcp::Caller,
    index: u32,
    thread: &str,
    turn: &str,
    cause: &Cause,
) -> Result<()> {
    let prior = index.checked_sub(1).filter(|n| *n > 0).ok_or_else(|| {
        Fault::Refused(
            "invalid_input",
            "continuation index is not additional".into(),
        )
    })?;
    let Outer::Terminal {
        thread_id,
        turn_id,
        terminal,
        ..
    } = original(tx, c, prior, "terminal")?
    else {
        return refuse("source_mismatch", "supplied terminal phase differs");
    };
    if thread_id != thread
        || turn_id != turn
        || match cause {
            Cause::RateLimit { .. } => !contract::rate_limited(&terminal),
            Cause::Completion { .. } => terminal["status"] != "completed",
        }
    {
        return refuse(
            "reconciliation_required",
            "continuation lacks exact qualifying terminal",
        );
    }
    Ok(())
}
pub(crate) fn observe(
    tx: &Connection<'_>,
    t: &mut StoredTask,
    r: &Report,
    o: &Outer,
    now: u64,
) -> Result<Option<contract::Wait>> {
    let c = caller(r);
    let index = o.index();
    if t.task.input.definition.executor != ExecutorKind::Coding
        || index == 0
        || r.report_id != contract::id(&r.attempt_id, index, o.phase())
        || !valid_id(tx.limits, o.thread())
        || !valid_id(tx.limits, o.turn())
    {
        return refuse("source_mismatch", "supplied outer identity differs");
    }
    let execution = t
        .task
        .input
        .execution_limits()
        .map_err(|e| Fault::Refused("invalid_limits", e))?;
    let policy =
        execution.coding.rate_limit_retry.as_ref().ok_or_else(|| {
            Fault::Refused("not_configured", "supplied retry policy absent".into())
        })?;
    policy
        .validate()
        .map_err(|e| Fault::Refused("invalid_limits", e))?;
    match o {
        Outer::Acknowledged {
            thread_id, turn_id, ..
        } => {
            let association: (Option<String>, Option<String>) = tx.query_row(
                "SELECT thread_id,turn_id FROM attempts WHERE id=?1",
                [&r.attempt_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            if association.0.as_deref() != Some(thread_id)
                || (index == 1 && association.1.as_deref() != Some(turn_id))
            {
                return refuse("source_mismatch", "supplied original association differs");
            }
            if index > 1 {
                let grant = original(tx, &c, index, "dispatch")?;
                if grant.thread() != thread_id {
                    return refuse("source_mismatch", "supplied ACK thread differs");
                }
                if u64::from(index) > tx.limits.controller.max_receipts as u64 {
                    return refuse(
                        "capacity",
                        "supplied occurrence count exceeds receipt inventory",
                    );
                }
                for previous in 1..index {
                    if original(tx, &c, previous, "ack")?.turn() == turn_id {
                        return refuse(
                            "source_mismatch",
                            "supplied ACK reused an earlier public turn identity",
                        );
                    }
                }
            }
        }
        Outer::Terminal {
            thread_id,
            turn_id,
            terminal,
            ..
        } => {
            let ack = original(tx, &c, index, "ack")?;
            if ack.thread() != thread_id
                || ack.turn() != turn_id
                || terminal["id"] != *turn_id
                || !matches!(
                    terminal["status"].as_str(),
                    Some("completed" | "failed" | "interrupted")
                )
                || terminal.as_object().is_none_or(|m| {
                    m.keys()
                        .any(|k| !matches!(k.as_str(), "id" | "status" | "error"))
                })
                || serde_json::to_vec(terminal)
                    .map_err(|e| Fault::Store(e.to_string()))?
                    .len()
                    > execution.coding.frame_bytes
            {
                return refuse(
                    "source_mismatch",
                    "supplied terminal differs from acknowledged occurrence",
                );
            }
        }
        Outer::Waiting {
            thread_id,
            previous_turn_id,
            cause,
            ..
        }
        | Outer::Dispatch {
            thread_id,
            previous_turn_id,
            cause,
            ..
        } => {
            active(tx, t, now)?;
            previous(tx, &c, index, thread_id, previous_turn_id, cause)?;
            if matches!(o, Outer::Waiting { .. }) {
                let prior_count = if index == 2 {
                    0
                } else {
                    wait(tx, &c, index - 1)?.rate_failures
                };
                let (rate_failures, delay) = match cause {
                    Cause::RateLimit { retry_index } => {
                        let next = prior_count.checked_add(1).ok_or_else(|| {
                            Fault::Refused("capacity", "supplied retry counter exhausted".into())
                        })?;
                        if *retry_index != next {
                            return refuse(
                                "source_mismatch",
                                "supplied retry counter differs from canonical predecessor",
                            );
                        }
                        (
                            next,
                            policy
                                .delay_ms(
                                    next,
                                    contract::jitter_sample(thread_id, index),
                                    now,
                                    t.task.input.limits.deadline_unix_ms.min(
                                        t.task
                                            .attempt
                                            .as_ref()
                                            .ok_or_else(|| {
                                                Fault::Store("supplied attempt absent".into())
                                            })?
                                            .deadline_unix_ms,
                                    ),
                                )
                                .map_err(|e| Fault::Refused("invalid_transition", e))?,
                        )
                    }
                    Cause::Completion { begin_operation_id } => {
                        let begin = super::super::metadata::read(tx, begin_operation_id)?
                            .ok_or_else(|| {
                                Fault::Refused("source_mismatch", "completion Begin absent".into())
                            })?;
                        if !matches!(begin.request,Operation::BeginCompletionContinuation(ref input) if input.caller==c
                            &&matches!(&input.target,crate::governance_completion::CompletionTarget::CodexTurn{thread_id:t,previous_turn_id:p} if t==thread_id&&p==previous_turn_id))
                            || !matches!(
                                begin.outcome,
                                Outcome::Ok {
                                    data: Data::Completion(
                                        crate::governance_completion::CompletionOutcome::Begun(_)
                                    )
                                }
                            )
                        {
                            return refuse(
                                "source_mismatch",
                                "completion continuation differs from original Begin",
                            );
                        }
                        (prior_count, 0)
                    }
                };
                return Ok(Some(contract::Wait {
                    index,
                    rate_failures,
                    admitted_at_ms: now,
                    not_before_ms: now
                        .checked_add(delay)
                        .ok_or_else(|| Fault::Refused("capacity", "retry timer overflow".into()))?,
                }));
            }
            if matches!(o, Outer::Dispatch { .. }) {
                if now < wait(tx, &c, index)?.not_before_ms {
                    return refuse("invalid_transition", "supplied retry timer has not elapsed");
                }
                let waiting = original(tx, &c, index, "wait")?;
                if waiting
                    != (Outer::Waiting {
                        index,
                        thread_id: thread_id.clone(),
                        previous_turn_id: previous_turn_id.clone(),
                        cause: cause.clone(),
                    })
                {
                    return refuse(
                        "source_mismatch",
                        "supplied continuation differs from original wait",
                    );
                }
                let old: u32 = tx.query_row(
                    "SELECT model_requests FROM attempts WHERE id=?1",
                    [&r.attempt_id],
                    |row| row.get(0),
                )?;
                let next = index - 1;
                if old.checked_add(1) != Some(next)
                    || used_model_requests(tx, &t.task.id)?
                        .checked_add(u64::from(t.delegated_model_requests))
                        .and_then(|n| n.checked_add(1))
                        .is_none_or(|n| n > u64::from(t.task.input.limits.model_requests))
                {
                    return refuse(
                        "capacity",
                        "supplied outer continuation exhausted original allowance",
                    );
                }
                // Preserve known cumulative token totals; only visible outer debit changes.
                tx.execute(
                    "UPDATE attempts SET model_requests=?2 WHERE id=?1",
                    params![r.attempt_id, next],
                )?;
            }
        }
    }
    Ok(None)
}

/// The charter owner still validates its original ACK/result. This receipt is
/// the additional dispatch permission for the exact typed rate terminal only.
pub(crate) fn rate_dispatch(
    tx: &Connection<'_>,
    c: &crate::mcp::Caller,
    index: u32,
    thread: &str,
    turn: &str,
) -> Result<bool> {
    Ok(
        matches!(original(tx,c,index,"dispatch")?,Outer::Dispatch{thread_id,previous_turn_id,cause:Cause::RateLimit{..},..} if thread_id==thread&&previous_turn_id==turn),
    )
}

pub(crate) fn completed_boundary(
    tx: &Connection<'_>,
    c: &crate::mcp::Caller,
    thread: &str,
    turn: &str,
) -> Result<()> {
    let charged: u32 = tx.query_row(
        "SELECT model_requests FROM attempts WHERE id=?1",
        [&c.attempt_id],
        |r| r.get(0),
    )?;
    let index = charged
        .checked_add(1)
        .ok_or_else(|| Fault::Store("supplied index overflow".into()))?;
    if matches!(original(tx,c,index,"terminal")?,Outer::Terminal{thread_id,turn_id,terminal,..}
        if thread_id==thread&&turn_id==turn&&terminal["status"]=="completed")
    {
        Ok(())
    } else {
        refuse(
            "source_mismatch",
            "completion boundary differs from latest acknowledged supplied terminal",
        )
    }
}

pub(crate) fn completion_consumed(
    tx: &Connection<'_>,
    task: &Task,
    value: &crate::governance_completion::CompletionContinuation,
    thread: &str,
    turn: &str,
) -> Result<()> {
    let c = &value.caller;
    let charged: u32 = tx.query_row(
        "SELECT model_requests FROM attempts WHERE id=?1",
        [&c.attempt_id],
        |r| r.get(0),
    )?;
    let mut index = charged
        .checked_add(1)
        .ok_or_else(|| Fault::Store("supplied occurrence overflow".into()))?;
    let bound = task
        .input
        .resolved_limits
        .as_ref()
        .ok_or_else(|| Fault::Store("accepted metadata absent".into()))?
        .global_metadata
        .governance
        .as_ref()
        .ok_or_else(|| Fault::Store("completion policy absent".into()))?
        .scan_rows;
    if index as usize > bound {
        return refuse(
            "capacity",
            "completion supplied lineage exceeds selected scan bound",
        );
    }
    let Outer::Terminal {
        thread_id,
        turn_id,
        terminal,
        ..
    } = original(tx, c, index, "terminal")?
    else {
        return refuse("source_mismatch", "consuming supplied terminal absent");
    };
    if thread_id != thread || turn_id != turn || terminal["status"] != "completed" {
        return refuse(
            "source_mismatch",
            "completion consumption differs from actual successful supplied terminal",
        );
    }
    loop {
        if index == 1 {
            if !matches!(&value.target,crate::governance_completion::CompletionTarget::InitialCodexTurn{thread_id:t} if t==thread)
            {
                return refuse(
                    "source_mismatch",
                    "completion lineage lacks its original Begin dispatch",
                );
            }
            return Ok(());
        }
        match original(tx, c, index, "dispatch")? {
            Outer::Dispatch {
                thread_id,
                cause: Cause::Completion { begin_operation_id },
                ..
            } if thread_id == thread && begin_operation_id == value.begin_operation_id => {
                return Ok(());
            }
            Outer::Dispatch {
                thread_id,
                previous_turn_id,
                cause: Cause::RateLimit { .. },
                ..
            } if thread_id == thread => {
                let prior = original(tx, c, index - 1, "terminal")?;
                if !matches!(prior,Outer::Terminal{thread_id:t,turn_id:p,terminal,..} if t==thread&&p==previous_turn_id&&contract::rate_limited(&terminal))
                {
                    return refuse(
                        "source_mismatch",
                        "completion retry lineage lacks exact rate terminal",
                    );
                }
                index -= 1;
            }
            _ => {
                return refuse(
                    "source_mismatch",
                    "completion input changed across supplied continuation",
                );
            }
        }
    }
}

// Validate historical causal joins without applying today's live-owner guard.
// Original receipt order and accepted session policy remain authoritative after
// cancellation, reassignment, retirement and successful completion.
fn validate_retained(tx: &Connection<'_>, report: &Report, o: &Outer, revision: u64) -> Result<()> {
    let c = caller(report);
    let index = o.index();
    if index == 0 || index as usize > tx.limits.controller.max_receipts {
        return Err(Fault::Store("supplied occurrence inventory invalid".into()));
    }
    original(tx, &c, index, o.phase())?;
    let before = |n: u32, phase: &str| -> Result<Outer> {
        let value = original(tx, &c, n, phase)?;
        let receipt = super::super::metadata::read(tx, &contract::id(&c.attempt_id, n, phase))?
            .ok_or_else(|| Fault::Store("supplied predecessor disappeared".into()))?;
        if receipt.commit.controller_revision >= revision {
            return Err(Fault::Store("supplied receipt causal order differs".into()));
        }
        Ok(value)
    };
    let (raw, task_id, thread, initial): (String, String, Option<String>, Option<String>) = tx
        .query_row(
            "SELECT body,task_id,thread_id,turn_id FROM attempts WHERE id=?1",
            [&c.attempt_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )?;
    if raw.len() > tx.limits.transport.body_bytes {
        return Err(Fault::Store("supplied attempt body exceeds bound".into()));
    }
    let attempt: Attempt = decode(&raw)?;
    if task_id != c.task_id
        || attempt.task_id != c.task_id
        || attempt.id != c.attempt_id
        || attempt.generation != c.generation
        || attempt.worker_id != c.worker_id
        || attempt.worker_incarnation != c.incarnation
        || thread.as_deref() != Some(o.thread())
    {
        return Err(Fault::Store("supplied retained owner differs".into()));
    }
    let session = super::super::sessions::get(tx, &attempt.session_id)?;
    let input = super::super::sessions::input(tx, &session)?;
    let execution = input.execution_limits().map_err(Fault::Store)?;
    let policy = execution
        .coding
        .rate_limit_retry
        .as_ref()
        .ok_or_else(|| Fault::Store("supplied retained policy absent".into()))?;
    policy.validate().map_err(Fault::Store)?;
    match o {
        Outer::Acknowledged { turn_id, .. } => {
            if index == 1 {
                if initial.as_deref() != Some(turn_id) {
                    return Err(Fault::Store("supplied initial ACK differs".into()));
                }
            } else {
                if before(index, "dispatch")?.thread() != o.thread() {
                    return Err(Fault::Store("supplied ACK dispatch differs".into()));
                }
                for previous in 1..index {
                    if before(previous, "ack")?.turn() == turn_id {
                        return Err(Fault::Store("supplied ACK reuses original turn".into()));
                    }
                }
            }
        }
        Outer::Terminal {
            thread_id,
            turn_id,
            terminal,
            ..
        } => {
            let ack = before(index, "ack")?;
            if ack.thread() != thread_id
                || ack.turn() != turn_id
                || terminal["id"] != *turn_id
                || !matches!(
                    terminal["status"].as_str(),
                    Some("completed" | "failed" | "interrupted")
                )
                || terminal.as_object().is_none_or(|m| {
                    m.keys()
                        .any(|k| !matches!(k.as_str(), "id" | "status" | "error"))
                })
                || serde_json::to_vec(terminal)?.len() > execution.coding.frame_bytes
            {
                return Err(Fault::Store(
                    "supplied retained terminal/ACK differs".into(),
                ));
            }
        }
        Outer::Waiting {
            thread_id,
            previous_turn_id,
            cause,
            ..
        } => {
            if index < 2 {
                return Err(Fault::Store("supplied wait has no predecessor".into()));
            }
            before(index - 1, "terminal")?;
            previous(tx, &c, index, thread_id, previous_turn_id, cause)?;
            let prior_count = if index == 2 {
                0
            } else {
                before(index - 1, "wait")?;
                wait(tx, &c, index - 1)?.rate_failures
            };
            let retained = wait(tx, &c, index)?;
            let (counter, delay) = match cause {
                Cause::RateLimit { retry_index } => {
                    let next = prior_count
                        .checked_add(1)
                        .ok_or_else(|| Fault::Store("supplied retry count overflow".into()))?;
                    if *retry_index != next {
                        return Err(Fault::Store("supplied retry ordinal differs".into()));
                    }
                    (
                        next,
                        policy
                            .delay_ms(
                                next,
                                contract::jitter_sample(thread_id, index),
                                retained.admitted_at_ms,
                                input.limits.deadline_unix_ms.min(attempt.deadline_unix_ms),
                            )
                            .map_err(Fault::Store)?,
                    )
                }
                Cause::Completion { begin_operation_id } => {
                    let begin = super::super::metadata::read(tx, begin_operation_id)?
                        .ok_or_else(|| Fault::Store("supplied completion Begin absent".into()))?;
                    if begin.commit.controller_revision >= revision
                        || !matches!(begin.request,Operation::BeginCompletionContinuation(ref b) if b.caller==c&&matches!(&b.target,crate::governance_completion::CompletionTarget::CodexTurn{thread_id:t,previous_turn_id:p} if t==thread_id&&p==previous_turn_id))
                        || !matches!(
                            begin.outcome,
                            Outcome::Ok {
                                data: Data::Completion(
                                    crate::governance_completion::CompletionOutcome::Begun(_)
                                )
                            }
                        )
                    {
                        return Err(Fault::Store("supplied completion original differs".into()));
                    }
                    (prior_count, 0)
                }
            };
            if retained.rate_failures != counter
                || retained.admitted_at_ms.checked_add(delay) != Some(retained.not_before_ms)
            {
                return Err(Fault::Store(
                    "supplied canonical wait differs from accepted policy".into(),
                ));
            }
        }
        Outer::Dispatch {
            thread_id,
            previous_turn_id,
            cause,
            ..
        } => {
            if before(index, "wait")?
                != (Outer::Waiting {
                    index,
                    thread_id: thread_id.clone(),
                    previous_turn_id: previous_turn_id.clone(),
                    cause: cause.clone(),
                })
            {
                return Err(Fault::Store(
                    "supplied dispatch differs from original wait".into(),
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_existing(
    tx: &Connection<'_>,
    cfg: &ControllerConfig,
    version: i64,
) -> Result<()> {
    let supported = version >= super::super::application_schema::SUPPLIED_RETRY_VERSION;
    let bound = cfg
        .max_receipts
        .checked_add(cfg.max_cleanup_receipts)
        .ok_or_else(|| Fault::Store("supplied receipt bound overflow".into()))?;
    let mut statement = tx.prepare("SELECT id FROM receipts ORDER BY controller_revision")?;
    let mut rows = statement.query([])?;
    let mut count = 0;
    while let Some(row) = rows.next()? {
        count += 1;
        if count > bound {
            return Err(Fault::Store(
                "supplied receipt inventory exceeds bound".into(),
            ));
        }
        let id: String = row.get(0)?;
        let r = super::super::metadata::read(tx, &id)?
            .ok_or_else(|| Fault::Store("supplied original receipt absent".into()))?;
        if !supported
            && (contract::operation_uses(&r.request) || contract::outcome_uses(&r.outcome))
        {
            return Err(Fault::Store(
                "supplied outer recovery vocabulary requires schema25".into(),
            ));
        }
        if let (Operation::ObserveWorker { report }, Outcome::Ok { .. }) = (&r.request, &r.outcome)
        {
            if let Observation::SuppliedOuter { observation } = &report.observation {
                validate_retained(tx, report, observation, r.commit.controller_revision)?;
            }
        }
    }
    if !supported {
        let mut statement = tx.prepare("SELECT body FROM definitions")?;
        let mut rows = statement.query([])?;
        let mut count = 0usize;
        while let Some(row) = rows.next()? {
            count += 1;
            if count > bound {
                return Err(Fault::Store("definition inventory exceeds bound".into()));
            }
            let raw: String = row.get(0)?;
            if raw.len() > tx.limits.transport.body_bytes {
                return Err(Fault::Store("definition body bound".into()));
            }
            let d: Definition = decode(&raw)?;
            if contract::definition_uses(&d) {
                return Err(Fault::Store(
                    "supplied retry definition requires schema25".into(),
                ));
            }
        }
        let mut statement = tx.prepare("SELECT id FROM tasks ORDER BY seq")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            if contract::task_uses(&load(tx, &id)?.task) {
                return Err(Fault::Store(
                    "supplied retry input requires schema25".into(),
                ));
            }
        }
        if version >= super::super::application_schema::DEFAULT_ASSIGNMENT_VERSION {
            let mut statement = tx.prepare("SELECT id FROM sessions ORDER BY seq")?;
            let mut rows = statement.query([])?;
            let mut count = 0usize;
            while let Some(row) = rows.next()? {
                count += 1;
                if count > super::super::sessions::bound(cfg)? {
                    return Err(Fault::Store(
                        "supplied session inventory exceeds bound".into(),
                    ));
                }
                let id: String = row.get(0)?;
                let session = super::super::sessions::get(tx, &id)?;
                if contract::input_uses(&super::super::sessions::input(tx, &session)?) {
                    return Err(Fault::Store(
                        "supplied retry historical input requires schema25".into(),
                    ));
                }
            }
            super::super::task_handoffs::admit_prior_supplied_retry(tx, bound)?;
        }
        let mut statement = tx.prepare("SELECT body FROM commands ORDER BY seq")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let raw: String = row.get(0)?;
            if raw.len() > tx.limits.transport.body_bytes {
                return Err(Fault::Store("command body bound".into()));
            }
            let command: WorkerCommand = decode(&raw)?;
            if contract::command_uses(&command) {
                return Err(Fault::Store(
                    "supplied retry command requires schema25".into(),
                ));
            }
        }
    }
    Ok(())
}
