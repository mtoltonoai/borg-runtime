//! Initial supplied-context delivery only. Zero is a storage discriminator in
//! the existing request table, never a native charge, model index or model turn.
//! A missing acknowledgment remains an original uncertain delivery; it cannot
//! authorize a second thread/start or a replacement-context workaround.
use super::*;

pub(super) fn initial_binding(s: &mut CharterSession) -> Result<()> {
    if s.last_turn_id.is_some() || s.compaction_due.is_some() || s.compaction_in_flight.is_some() {
        return refuse("unsupported_charter_boundary", "supplied delivery requires a fresh outer context");
    }
    if let Some(source) = s.pending.take() { s.effective = source; }
    Ok(())
}

// Initial supplied context is admitted only before executor association or
// outcome; unsupported running delivery stays refused. This is boundary
// safety, not support for adopting later changes inside a running executor.
//= spec/impl/v2/implementation.md#charter-changes-apply-at-safe-boundaries
//# A charter change MUST reach the supplied executor only at a supported step boundary.
fn current_coding(ctx: &Context<'_, '_>, caller: &Caller, session_id: &str) -> Result<Task> {
    bounded_task(ctx.tx, &caller.task_id)?;
    let task = super::super::collaboration_calls::caller(ctx.tx, caller, ctx.now_ms)?.task;
    if task.session_id != session_id { return refuse("caller_mismatch", "supplied context belongs to another session"); }
    if task.input.definition.executor != ExecutorKind::Coding {
        return refuse("unsupported_charter_boundary", "supplied context requires the coding executor");
    }
    if task.attempt.as_ref().is_none_or(|a| !matches!(a.phase, AttemptPhase::Assigned | AttemptPhase::Running)) {
        return refuse("invalid_transition", "initial supplied context must precede executor association");
    }
    // Worker admission reports Started(None, None) before any executor work and
    // moves the attempt to Running. The canonical association, not that phase
    // alone, distinguishes admission from an actual executor thread or turn.
    // A recorded executor outcome also closes this initial delivery boundary,
    // including failures before an executor could return either identity.
    let executor_observed: bool = ctx.tx.query_row(
        "SELECT thread_id IS NOT NULL OR turn_id IS NOT NULL OR executor_outcome IS NOT NULL FROM attempts WHERE id=?1",
        [&caller.attempt_id], |row| row.get(0),
    )?;
    if executor_observed {
        return refuse("invalid_transition", "initial supplied context must precede executor association or outcome");
    }
    Ok(task)
}

pub(super) fn validate_intent(tx: &StoreContext<'_>, v: &CharterSuppliedContext, l: &CharterLimits) -> Result<()> {
    let task = historical(tx, &v.caller, &v.session_id)?;
    let coding = &task.input.execution_limits().map_err(|e| corrupt(&e))?.coding;
    if task.input.definition.executor != ExecutorKind::Coding || v.request_id == 0
        || v.binding_revision < 2 || v.acknowledgment.is_some()
        || supplied_context_instructions(&v.effective).map_err(|_| corrupt("source rendering failed"))? != v.developer_instructions {
        return Err(corrupt("supplied context original intent differs"));
    }
    bounded(&v.developer_instructions, coding.frame_bytes)?;
    bounded(v, l.record_bytes)?;
    Ok(())
}

fn reconciled_prior_contexts(ctx:&Context<'_, '_>,caller:&Caller,session_id:&str)->Result<()> {
    let mut query=ctx.tx.prepare("SELECT id FROM charter_requests WHERE session_id=?1 AND kind='supplied_context' ORDER BY id LIMIT ?2")?;
    let ids=query.query_map(params![session_id,super::super::database::sql_bound(ctx.limits.records.saturating_add(1))?],|r|r.get::<_,String>(0))?
        .collect::<std::result::Result<Vec<_>,_>>()?;
    if ids.len()>ctx.limits.records{return refuse("capacity","supplied context inventory exceeds charter.records");}
    for id in ids {
        let original=self::query(ctx.tx,&id,ctx.limits)?;
        if original.caller.task_id!=caller.task_id || original.caller.attempt_id==caller.attempt_id
            ||original.caller.generation>=caller.generation ||original.acknowledgment.is_none() {
            return refuse("reconciliation_required","original supplied context has no distinct closed acknowledged attempt");
        }
        closed_context(ctx.tx,&original,ctx.limits)?;
    }
    Ok(())
}

fn closed_context(tx:&StoreContext<'_>,original:&CharterSuppliedContext,limits:&CharterLimits)->Result<()> {
    if original.acknowledgment.is_none(){return refuse("reconciliation_required","original supplied context lacks acknowledged delivery")}
        let raw=raw_body(tx,"SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM attempts WHERE id=?1",&original.caller.attempt_id,tx.limits.transport.body_bytes)?
            .ok_or_else(||corrupt("original supplied attempt absent"))?;
        let prior:Attempt=stored(&raw,tx.limits.transport.body_bytes)?;
        if !prior.stopped||prior.phase!=AttemptPhase::Stopped||prior.uncertain {
            return refuse("reconciliation_required","original supplied process termination is not settled");
        }
        let last:Option<String>=tx.query_row("SELECT id FROM charter_requests WHERE attempt_id=?1 AND kind='supplied_turn' ORDER BY request_index DESC LIMIT 1",[&prior.id],|r|r.get(0)).optional()?;
        if let Some(last)=last {
            // New vocabulary retains the exact public terminal. Missing start
            // ACK/result stays unresolved even if a generic task failure exists.
            if super::supplied_turns::query(tx,&last,limits)?.terminal.is_none() {
                return refuse("reconciliation_required","original public outer turn has no terminal evidence");
            }
        } else {
            // Historical delivery can only use an actually recorded executor
            // outcome. ProcessTreeStopped + failed ResultCandidate alone is not
            // a provider terminal (received-only recovery also produces those).
            let raw=raw_body(tx,"SELECT CAST(substr(CAST(executor_outcome AS BLOB),1,?2) AS TEXT) FROM attempts WHERE id=?1 AND executor_outcome IS NOT NULL",&prior.id,tx.limits.transport.body_bytes)?;
            let terminal=raw.as_deref().map(|v|stored::<Observation>(v,tx.limits.transport.body_bytes)).transpose()?;
            if !matches!(terminal,Some(Observation::ExecutorOutcome{..})) {
                return refuse("reconciliation_required","historical supplied context lacks original executor terminal evidence");
            }
        }
    Ok(())
}
/// Read-only predecessor proof: no invented caller, new context or execution.
pub(super) fn closed_session(tx:&StoreContext<'_>,session_id:&str,task_id:&str,limits:&CharterLimits)->Result<()> {
    let mut q=tx.prepare("SELECT id FROM charter_requests WHERE session_id=?1 AND kind='supplied_context' ORDER BY id LIMIT ?2")?;
    let ids=q.query_map(params![session_id,super::super::database::sql_bound(limits.records.saturating_add(1))?],|r|r.get::<_,String>(0))?.collect::<std::result::Result<Vec<_>,_>>()?;
    if ids.len()>limits.records{return refuse("capacity","supplied context inventory exceeds charter.records")}
    for id in ids {
        let original=query(tx,&id,limits)?;
        if original.session_id!=session_id || original.caller.task_id!=task_id {return Err(corrupt("predecessor supplied context differs"))}
        closed_context(tx,&original,limits)?;
    }
    Ok(())
}

pub(super) fn begin(ctx: &Context<'_, '_>, caller: &Caller, session_id: &str,
    expected_revision: u64, source: &CharterSourceRef, request_id: u64,
    developer_instructions: &str) -> Result<CharterOutcome> {
    let task = current_coding(ctx, caller, session_id)?;
    let mut s = require_session(ctx.tx, session_id, ctx.limits)?;
    if s.revision != expected_revision { return refuse("stale_revision", "charter binding differs"); }
    // A canonical retry is a new attempt, not a second dispatch of an unknown
    // context. Every prior context must have its genuine ACK and terminal/stop
    // evidence; a failed task status or a newer pending charter cannot supply it.
    reconciled_prior_contexts(ctx,caller,session_id)?;
    initial_binding(&mut s)?;
    if request_id == 0 { return refuse("invalid_input", "supplied request identity must be nonzero"); }
    if source != &s.effective { return refuse("source_mismatch", "supplied context source differs from selected binding"); }
    let effective = resolve(ctx.tx, source, ctx.limits)?;
    let coding = &task.input.execution_limits().map_err(|e| corrupt(&e))?.coding;
    bounded(&developer_instructions, coding.frame_bytes)?;
    if supplied_context_instructions(&effective).map_err(|_| corrupt("source rendering failed"))? != developer_instructions {
        return refuse("source_mismatch", "supplied context instructions differ from exact source projection");
    }
    let old = advance(&mut s)?;
    let v = CharterSuppliedContext { delivery_id: ctx.operation_id.into(), session_id: session_id.into(),
        caller: caller.clone(), binding_revision: s.revision, effective, request_id,
        developer_instructions: developer_instructions.into(), acknowledgment: None };
    insert_request(ctx, &RequestRecord::SuppliedContext(v.clone()))?;
    save_session(ctx.tx, &s, ctx.limits, Some(old), Some(ctx.operation_id))?;
    Ok(CharterOutcome::SuppliedContext(v))
}

fn validate_ack(tx: &StoreContext<'_>, original: &CharterSuppliedContext,
    ack: &CharterSuppliedAcknowledgment, l: &CharterLimits) -> Result<()> {
    identity(tx, &ack.operation_id)?;
    identity(tx, &ack.thread_id)?;
    let task = historical(tx, &original.caller, &original.session_id)?;
    let coding = &task.input.execution_limits().map_err(|e| corrupt(&e))?.coding;
    bounded(&ack.response, coding.frame_bytes)?;
    bounded(ack, l.record_bytes)?;
    let inference = task.input.definition.required_inference().map_err(corrupt)?;
    if ack.request_id != original.request_id || ack.response.get("thread").and_then(|v|v.get("id")).and_then(|v|v.as_str()) != Some(ack.thread_id.as_str())
        || ack.response.get("model").and_then(|v|v.as_str()) != Some(inference.model.as_str())
        || ack.response.get("reasoningEffort").and_then(|v|v.as_str()) != Some(inference.reasoning_effort.as_str()) {
        return refuse("source_mismatch", "supplied acknowledgment identity or effective configuration differs");
    }
    Ok(())
}

pub(super) fn get(tx: &StoreContext<'_>, id: &str, l: &CharterLimits) -> Result<CharterSuppliedContext> {
    let RequestRecord::SuppliedContext(mut v) = request(tx, id, l)? else {
        return refuse("invalid_input", "charter occurrence is not a supplied context");
    };
    if let Some(raw) = raw_body(tx, "SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM charter_closures WHERE request_id=?1", id, l.record_bytes)? {
        let ack: CharterSuppliedAcknowledgment = stored(&raw, l.record_bytes)?;
        validate_ack(tx, &v, &ack, l)?;
        v.acknowledgment = Some(ack);
    }
    bounded(&v, l.record_bytes)?;
    Ok(v)
}

pub(super) fn ack(ctx: &Context<'_, '_>, caller: &Caller, id: &str, request_id: u64,
    response: &serde_json::Value) -> Result<CharterOutcome> {
    let original = query(ctx.tx, id, ctx.limits)?;
    historical(ctx.tx, caller, &original.session_id)?;
    if caller != &original.caller { return refuse("caller_mismatch", "supplied acknowledgment belongs to another original caller"); }
    if original.acknowledgment.is_some() { return refuse("operation_conflict", "supplied context already has its original acknowledgment"); }
    let thread_id = response.get("thread").and_then(|v|v.get("id")).and_then(|v|v.as_str())
        .ok_or_else(|| Fault::Refused("invalid_input", "supplied acknowledgment lacks actual thread identity".into()))?;
    let ack = CharterSuppliedAcknowledgment { operation_id: ctx.operation_id.into(), request_id,
        thread_id: thread_id.into(), response: response.clone() };
    validate_ack(ctx.tx, &original, &ack, ctx.limits)?;
    let mut result = original;
    result.acknowledgment = Some(ack.clone());
    bounded(&result, ctx.limits.record_bytes)?;
    capacity(ctx.tx, ctx.limits, 1)?;
    ctx.tx.execute("INSERT INTO charter_closures(request_id,body) VALUES(?1,?2)", params![id,encode(&ack)?])?;
    // A late ACK is historical evidence only. It neither clears a newer pending
    // source nor makes a stopped caller current, and it never creates a turn.
    Ok(CharterOutcome::SuppliedContext(result))
}

pub(super) fn validate_ack_receipt(tx: &StoreContext<'_>, id: &str, l: &CharterLimits) -> Result<()> {
    let v = get(tx, id, l)?;
    let ack = v.acknowledgment.as_ref().ok_or_else(|| corrupt("supplied acknowledgment row is absent"))?;
    let original = receipt(tx, &ack.operation_id)?;
    let begun = request(tx, id, l)?;
    validate_request_receipt(tx, &begun)?;
    if original.commit.controller_revision <= receipt(tx, id)?.commit.controller_revision {
        return Err(corrupt("supplied acknowledgment precedes its original intent"));
    }
    if !matches!((&original.request,charter_outcome(&original)?),
        (Operation::ObserveCharter{caller,observation:CharterObservation::AckSuppliedContext{delivery_id,request_id,response},..},CharterOutcome::SuppliedContext(record))
        if caller==&v.caller&&delivery_id==id&&*request_id==ack.request_id&&response==&ack.response&&record==&v) {
        return Err(corrupt("supplied acknowledgment differs from its original committed response"));
    }
    Ok(())
}

pub(super) fn query(tx: &StoreContext<'_>, id: &str, l: &CharterLimits) -> Result<CharterSuppliedContext> {
    let v = get(tx, id, l)?;
    validate_request_receipt(tx, &request(tx, id, l)?)?;
    if v.acknowledgment.is_some() { validate_ack_receipt(tx, id, l)?; }
    Ok(v)
}
