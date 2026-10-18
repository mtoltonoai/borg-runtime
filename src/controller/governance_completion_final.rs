//! Final stopped proof and ordinary same-task admission. No process restart,
//! recipient-policy override, new attempt allocation or receipt ownership here.
use super::*;
use crate::governance::CompletionCheck;
use serde::{Serialize,Deserialize};
use crate::governance_completion::{CompletionBoundary,CompletionResume};
use crate::protocol::{ExecutorKind,Observation,Report};

#[derive(Debug,Clone,Serialize,Deserialize,PartialEq)]
#[serde(deny_unknown_fields)]
struct StopInput {event:WorkflowActivity,consumed:bool}
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq)]
#[serde(tag="kind",rename_all="snake_case",deny_unknown_fields)]
enum StopProjection {Complete {items:Vec<StopInput>,pending_begin:bool},Unavailable {code:CompletionFailure}}
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq)]
#[serde(deny_unknown_fields)]
struct StopProof {
    report_id:String,caller:crate::mcp::Caller,task_revision:u64,
    decision:Option<DecisionRef>,observed_product_revision:u64,projection:StopProjection,
}
/// Admission reserves the actual private unavailable-proof shape. No SQL or
/// state write occurs here; the accepted/root bounds both must represent it.
pub(in crate::controller) fn validate_stop_failure_envelope(tx:&StoreContext<'_>,stored:&StoredTask)->Result<()> {
    let task=&stored.task;let Some(limits)=selected(task)? else{return Ok(())};
    let accepted=&task.input.resolved_limits.as_ref().unwrap().global_metadata;
    let root=tx.governance_limits;
    let record_bound=root.governance.as_ref().map_or(limits.record_bytes,|v|v.record_bytes).min(limits.record_bytes);
    let width=root.transport.identity_bytes.max(accepted.transport.identity_bytes);
    if width>record_bound {return refuse("invalid_limits","governance stop proof cannot retain accepted identity width")}
    let wide="x".repeat(width);
    let proof=StopProof {report_id:wide.clone(),caller:crate::mcp::Caller {task_id:task.id.clone(),attempt_id:wide.clone(),generation:u64::MAX,worker_id:wide.clone(),incarnation:wide.clone()},
        task_revision:u64::MAX,decision:Some(DecisionRef {operation_id:wide,point:DecisionPoint::Completion}),
        observed_product_revision:u64::MAX,projection:StopProjection::Unavailable {code:CompletionFailure::InvalidCanonicalState}};
    let bound=record_bound.min(root.transport.body_bytes).min(accepted.transport.body_bytes);
    if super::super::bounded_json_size(&proof,bound).is_err() {
        return refuse("invalid_limits","accepted governance cannot retain the minimum physical-stop proof");
    }
    Ok(())
}
fn projection_failure(error:Fault)->StopProjection {
    StopProjection::Unavailable {code:match error {
        Fault::Refused("capacity",_)=>CompletionFailure::BoundExceeded,
        Fault::Store(_)=>CompletionFailure::InvalidCanonicalState,
        _=>CompletionFailure::Unavailable,
    }}
}
fn stop_rows(tx:&StoreContext<'_>,task_id:&str,limits:&GovernanceLimits)->Result<Vec<(i64,StopProof)>> {
    let mut query=tx.prepare("SELECT revision,CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM history WHERE task_id=?1 AND kind='governance_stop_inputs' ORDER BY seq LIMIT ?3")?;
    let rows=query.query_map(params![task_id,database::sql_bound(limits.record_bytes.saturating_add(1))?,database::sql_bound(limits.scan_rows.saturating_add(1))?],
        |r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?)))?.collect::<std::result::Result<Vec<_>,_>>()?;
    if rows.len()>limits.scan_rows {return refuse("capacity","physical-stop proof inventory exceeds governance.scan_rows")}
    let mut out=Vec::with_capacity(rows.len());
    for (revision,raw) in rows {
        if raw.len()>limits.record_bytes {return refuse("capacity","physical-stop proof exceeds governance.record_bytes")}
        let proof:StopProof=decode(&raw)?;
        if proof.caller.task_id!=task_id||integer(proof.task_revision)?!=revision {return Err(Fault::Store("physical-stop history identity differs".into()))}
        out.push((revision,proof));
    }
    Ok(out)
}
/// Invoke before modifying the genuinely current attempt for its first physical
/// stop. Projection failure never refuses that stop: it is retained as typed
/// unavailable for later voluntary acceptance. SQLite/commit failure retains
/// the ordinary original-report retry semantics; it is never called success.
pub(in crate::controller) fn capture_before_stop(tx:&StoreContext<'_>,stored:&StoredTask,r:&Report,now:u64)->Result<()> {
    let task=&stored.task;let Some(limits)=selected(task)? else{return Ok(())};
    let a=task.attempt.as_ref().ok_or_else(||Fault::Store("stop proof attempt absent".into()))?;
    if a.stopped||database::terminal(task.status)||!super::super::governance::completion_enforced(tx)? {return Ok(())}
    if !matches!(r.observation,Observation::ProcessTreeStopped)||r.task_id!=task.id||r.attempt_id!=a.id
        ||r.generation!=a.generation||r.worker_id!=a.worker_id||r.incarnation!=a.worker_incarnation {
        return Err(Fault::Store("stop proof differs from validated original report".into()));
    }
    let projected=(||->Result<StopProjection>{
        if task.status!=TaskStatus::Active||a.phase!=AttemptPhase::Running||a.uncertain||stored.unresolved_execution {
            return refuse("invalid_transition","stop input view was not a current active safe boundary");
        }
        let input=if supports_delivery(task) {
            super::super::collaboration_notifications::completion_inputs(tx,task,now,limits.scan_rows)?
        } else {
            if let Some(policy)=tx.workflow_policy {
                if !super::super::collaboration_work::pending_resolution_refs(tx,&task.id,policy)?.is_empty() {
                    return refuse("not_configured","stop input view cannot consume pending workflow refs");
                }
            }
            super::super::collaboration_notifications::CompletionInputs {items:Vec::new(),
                pending_begin:super::super::collaboration_notifications::delivery_blocks_admission(tx,task)?}
        };
        Ok(StopProjection::Complete {items:input.items.into_iter().map(|i|StopInput {event:i.event,consumed:i.consumed}).collect(),pending_begin:input.pending_begin})
    })();
    let mut proof=StopProof {report_id:r.report_id.clone(),caller:crate::mcp::Caller {task_id:task.id.clone(),attempt_id:a.id.clone(),generation:a.generation,worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone()},
        task_revision:task.revision.checked_add(1).ok_or_else(||Fault::Store("stop task revision exhausted".into()))?,
        decision:stored.completion_hold.as_ref().map(|h|h.decision.clone()),
        observed_product_revision:workflow::product_revision(tx)?,projection:projected.unwrap_or_else(projection_failure)};
    if super::super::bounded_json_size(&proof,limits.record_bytes).is_err() {
        proof.projection=StopProjection::Unavailable {code:CompletionFailure::BoundExceeded};
    }
    // Accepted policy admission requires room for this minimal typed failure.
    super::super::bounded_json_size(&proof,limits.record_bytes)
        .map_err(|_|Fault::Store("accepted governance policy cannot retain minimum stop proof".into()))?;
    // The validated original receipt and !attempt.stopped fence select this
    // first occurrence. A later reader refuses duplicate/corrupt history; a
    // bounded projection failure must never discard the actual physical stop.
    tx.execute("INSERT INTO history(task_id,revision,kind,body) VALUES(?1,?2,'governance_stop_inputs',?3)",params![task.id,integer(proof.task_revision)?,database::encode(&proof)?])?;
    Ok(())
}
fn read_stop(tx:&StoreContext<'_>,task:&Task,hold:&CompletionHold,limits:&GovernanceLimits)->Result<StopProof> {
    let a=task.attempt.as_ref().unwrap();
    let rows=stop_rows(tx,&task.id,limits)?.into_iter().filter(|(_,p)|p.caller.attempt_id==a.id).collect::<Vec<_>>();
    if rows.len()!=1 {return refuse("source_mismatch","original stopped input proof absent or duplicated")}
    let proof=rows[0].1.clone();
    if integer(proof.task_revision)?!=rows[0].0||proof.task_revision>task.revision||proof.decision.as_ref()!=Some(&hold.decision)
        ||proof.caller.task_id!=task.id||proof.caller.attempt_id!=a.id||proof.caller.generation!=a.generation
        ||proof.caller.worker_id!=a.worker_id||proof.caller.incarnation!=a.worker_incarnation
        ||proof.observed_product_revision<hold.observed_product_revision||proof.observed_product_revision>workflow::product_revision(tx)? {
        return refuse("source_mismatch","stopped input proof differs from original task/admission");
    }
    let original=super::super::governance::original_receipt(tx,&proof.report_id)?.ok_or_else(||Fault::Refused("source_mismatch","original stopped report receipt absent".into()))?;
    if !matches!(original.request,crate::protocol::Operation::ObserveWorker {report}
        if report.report_id==proof.report_id&&report.task_id==task.id&&report.attempt_id==a.id&&report.generation==a.generation
            &&report.worker_id==a.worker_id&&report.incarnation==a.worker_incarnation&&matches!(report.observation,Observation::ProcessTreeStopped))
        ||!matches!(original.outcome,crate::protocol::Outcome::Ok {data:crate::protocol::Data::Task(t)}
            if t.id==task.id&&t.revision==proof.task_revision&&t.attempt.as_ref().is_some_and(|a|a.id==proof.caller.attempt_id&&a.stopped&&a.phase==AttemptPhase::Stopped)) {
        return refuse("source_mismatch","stop projection has no exact committed physical stop");
    }
    if let StopProjection::Complete {items,..}=&proof.projection {
        let policy=tx.workflow_policy;
        let mut seen=std::collections::BTreeSet::new();
        for item in items {
            let policy=policy.ok_or_else(||Fault::Store("recorded stop event lacks workflow policy".into()))?;
            if !seen.insert(item.event.id.clone())||item.event.id.product_revision>proof.observed_product_revision {return Err(Fault::Store("duplicate or future stopped input event".into()))}
            let raw:String=tx.query_row("SELECT CAST(substr(CAST(body AS BLOB),1,?3) AS TEXT) FROM workflow_events WHERE product_revision=?1 AND ordinal=?2",params![integer(item.event.id.product_revision)?,item.event.id.ordinal,database::sql_bound(policy.event_bytes.saturating_add(1))?],|r|r.get(0))?;
            if raw.len()>policy.event_bytes||decode::<WorkflowActivity>(&raw)?!=item.event {return Err(Fault::Store("stopped input body differs from immutable original event".into()))}
        }
    }
    Ok(proof)
}

fn supports_delivery(task:&Task)->bool {
    matches!(task.input.definition.execution_profile,Some(crate::protocol::ExecutionProfile::CollaborationV2 {..}))
        &&task.input.resolved_limits.as_ref().is_some_and(|a|a.global_metadata.workflow.is_some())
}
fn stopped_inputs(tx:&StoreContext<'_>,task:&Task,now:u64,limits:&GovernanceLimits)
    ->Result<super::super::collaboration_notifications::CompletionInputs> {
    if supports_delivery(task) {
        super::super::collaboration_notifications::completion_stopped_inputs(tx,task,now,limits.scan_rows)
    } else {
        if let Some(policy)=tx.workflow_policy {
            if !super::super::collaboration_work::pending_resolution_refs(tx,&task.id,policy)?.is_empty() {
                return refuse("not_configured","stopped required input has no accepted delivery capability");
            }
        }
        Ok(super::super::collaboration_notifications::CompletionInputs {items:Vec::new(),
            pending_begin:super::super::collaboration_notifications::delivery_blocks_admission(tx,task)?})
    }
}
fn waits(tx:&StoreContext<'_>,task:&Task,limits:&GovernanceLimits)->Result<Vec<WorkflowWait>> {
    match tx.workflow_policy {
        Some(policy)=>super::super::collaboration_work::completion_waits(tx,&task.id,policy,limits.scan_rows),
        None=>{
            if super::super::collaboration_work::admission_blocked(tx,&task.id)? {return refuse("workflow_disabled","required wait lacks selected policy");}
            Ok(Vec::new())
        },
    }
}
fn actor(task:&Task,r:&Report)->RecordedActor {
    RecordedActor {principal:PrincipalRef::Agent {agent_id:task.assigned_agent_id.clone()},
        provenance:ActorProvenance::BoundAgentSession {agent_id:task.assigned_agent_id.clone(),
            session_id:task.session_id.clone(),task_id:r.task_id.clone(),attempt_id:r.attempt_id.clone(),
            generation:r.generation,worker_id:r.worker_id.clone(),incarnation:r.incarnation.clone()}}
}
/// Root calls this immediately before the original ResultCandidate assigns a
/// terminal status, after its existing owner, output/acceptance and deadline
/// checks. Non-voluntary executor failure and cancellation keep old semantics.
pub(in crate::controller) fn before_result(tx:&StoreContext<'_>,stored:&StoredTask,r:&Report,checks_passed:bool,now:u64)->Result<()> {
    if stored.executor_success!=Some(true)||!super::super::governance::completion_enforced(tx)? {return Ok(())}
    let task=&stored.task;let Some(limits)=selected(task)? else {return Ok(())};
    let a=task.attempt.as_ref().ok_or_else(||Fault::Store("final completion attempt absent".into()))?;
    if !matches!(r.observation,Observation::ResultCandidate {..}) || r.task_id!=task.id
        ||r.attempt_id!=a.id||r.generation!=a.generation||r.worker_id!=a.worker_id||r.incarnation!=a.worker_incarnation
        ||task.status!=TaskStatus::Active||task.native_retry_block.is_some()||a.phase!=AttemptPhase::Stopped||!a.stopped||a.uncertain
        ||stored.unresolved_execution||now>=task.input.limits.deadline_unix_ms
        ||super::super::operations::pending_effects(tx,&task.id)?!=0 {
        return refuse("invalid_transition","final completion requires its exact verified stopped original owner");
    }
    // Original replay is handled before dispatch. ReconcileResult cannot mint a
    // new rule decision under a historical report's identity.
    if super::super::governance::original_receipt(tx,&r.report_id)?.is_some() {
        return refuse("governance_completion_blocked","historical final candidate requires its original decision, not reevaluation");
    }
    // A failed/lost ACK never becomes a fresh decision that erases the original
    // input intent. Only its original Begin/ACK reconciliation may discharge it.
    if stored.completion_hold.as_ref().and_then(|h|h.continuation.as_ref()).is_some_and(|c|c.acknowledged.is_none()) {
        return refuse("delivery_pending","original completion input remains unacknowledged");
    }
    let current=workflow::product_revision(tx)?;
    let previous=stored.completion_hold.as_ref().map_or(0,|h|h.state.consecutive_count);
    let assessed=(||->Result<(Vec<CompletionReason>,Option<EventId>,bool)>{
        let hold=stored.completion_hold.as_ref().ok_or_else(||Fault::Refused("not_found","original active completion admission missing".into()))?;
        validate_hold(tx,task,hold)?;
        if hold.state.subject.attempt_id!=a.id||!matches!(hold.state.disposition,CompletionDisposition::Admit)
            ||hold.continuation.is_some() {
            return refuse("source_mismatch","final candidate lacks the exact original active Admit");
        }
        let (original,evaluation)=actions::recorded_evaluation(tx,&hold.decision)?;
        if original.caller.task_id!=task.id||original.caller.attempt_id!=a.id||original.caller.generation!=a.generation
            ||original.caller.worker_id!=a.worker_id||original.caller.incarnation!=a.worker_incarnation
            ||evaluation.state!=hold.state||matches!(evaluation.boundary,CompletionBoundary::StoppedResult {..}) {
            return refuse("source_mismatch","final candidate admission belongs to another boundary");
        }
        let stop=read_stop(tx,task,hold,limits)?;
        let waits=waits(tx,task,limits)?;
        let mut input=stopped_inputs(tx,task,now,limits)?;
        match stop.projection {
            StopProjection::Unavailable {code}=>return Ok((vec![CompletionReason::EvaluationUnavailable {code}],None,false)),
            StopProjection::Complete {items,pending_begin}=>{
                input.pending_begin|=pending_begin;
                for original in items {
                    if let Some(current)=input.items.iter_mut().find(|i|i.event.id==original.event.id) {
                        if current.event!=original.event {return Err(Fault::Store("stopped input occurrence changed".into()))}
                        // A stopped caller cannot append a new input. Never
                        // manufacture consumption from vanished eligibility.
                        current.consumed&=original.consumed;
                    } else {input.items.push(super::super::collaboration_notifications::CompletionInput {event:original.event,consumed:original.consumed});}
                }
            },
        }
        let mut reasons=waits.iter().filter(|w|matches!(w.status,WaitStatus::Open))
            .map(|w|CompletionReason::OpenWait {wait:w.reference.clone()}).collect::<Vec<_>>();
        if let Some(child)=&task.waiting_child_id {reasons.push(CompletionReason::DelegatedTask {task_id:child.clone()})}
        reasons.extend(input.items.iter().filter(|i|!i.consumed)
            .map(|i|CompletionReason::UnconsumedInput {event_id:i.event.id.clone()}));
        if !checks_passed {
            match task.input.definition.executor {
                ExecutorKind::Native=>reasons.push(CompletionReason::AcceptanceCheck {check:CompletionCheck::NativeOutput,index:0}),
                ExecutorKind::Coding=>{
                    // A negative aggregate proves no individual command's
                    // positive result. Name the actual accepted obligations.
                    for index in 0..task.input.acceptance_commands.len() {
                        reasons.push(CompletionReason::AcceptanceCheck {check:CompletionCheck::CodingValidation,index});
                        if reasons.len()>limits.reason_items {break}
                    }
                    if task.input.acceptance_commands.is_empty() {reasons.push(CompletionReason::EvaluationUnavailable {code:CompletionFailure::Unavailable})}
                },
            }
        }
        if input.pending_begin {reasons.push(CompletionReason::EvaluationUnavailable {code:CompletionFailure::Unavailable})}
        let reset=match tx.workflow_policy {
            Some(policy)=>reset_event(tx,task,&waits,&input.items,Some(hold),current,limits,policy)?,
            None=>None,
        };
        let used=super::super::operations::used_model_requests(tx,&task.id)?.saturating_add(u64::from(stored.delegated_model_requests));
        let next=if reset.is_some(){1}else{previous.saturating_add(1)};
        let accepted=&task.input.resolved_limits.as_ref().unwrap().global_metadata;
        let continuable=used<u64::from(task.input.limits.model_requests)
            &&next<=super::super::governance::continuation_limit(tx,accepted)?&&!input.pending_begin
            &&!reasons.iter().any(|r|matches!(r,CompletionReason::OpenWait {..}|CompletionReason::DelegatedTask {..}));
        Ok((reasons,reset,continuable))
    })();
    let (reasons,reset,continuable)=match assessed {
        Ok(v)=>v,
        Err(Fault::Refused("capacity",_))=>(vec![CompletionReason::EvaluationUnavailable {code:CompletionFailure::BoundExceeded}],None,false),
        Err(Fault::Refused(_,_))=>(vec![CompletionReason::EvaluationUnavailable {code:CompletionFailure::Unavailable}],None,false),
        Err(Fault::Store(_))=>(vec![CompletionReason::EvaluationUnavailable {code:CompletionFailure::InvalidCanonicalState}],None,false),
        Err(e)=>return Err(e),
    };
    let decision=decide(subject(task)?,reasons,previous,reset,continuable,limits);
    let blocked=matches!(decision.disposition,CompletionDisposition::Blocked {..});
    super::super::governance::stage_completion(tx,&r.report_id,&actor(task,r),decision,current)?;
    if blocked {return refuse("governance_completion_blocked",format!("remaining work is recorded in completion decision {}",r.report_id))}
    Ok(())
}

/// The physical stop is always retained. Only our exact typed held state may
/// become queued; retry-blocked/failed/cancelled tasks and every foreign marker
/// remain untouched. Ordinary admission still owns all eligibility checks.
pub(in crate::controller) fn mark_stopped_hold(stored:&mut StoredTask) {
    let Some(hold)=&stored.completion_hold else{return};
    if matches!(stored.task.status,TaskStatus::Active|TaskStatus::Queued)
        &&stored.task.native_retry_block.is_none()
        &&matches!(hold.state.disposition,CompletionDisposition::Blocked {..})
        &&stored.task.blocked_reason.as_deref()==Some(format!("governance_completion_blocked:{}",hold.decision.operation_id).as_str())
        &&stored.task.attempt.as_ref().is_some_and(|a|a.stopped&&a.phase==AttemptPhase::Stopped&&!a.uncertain)
        &&!stored.unresolved_execution {
        stored.task.status=TaskStatus::Queued;
    }
}

/// Exact independent reset witness selected under the actual current task view.
/// The original count/audit is unchanged; only a later actual Evaluate records
/// a reset. A prior admitted event is not itself input acknowledgement.
pub(super) fn resume_reset(tx:&StoreContext<'_>,task:&Task,hold:&CompletionHold,now:u64,
    expected:Option<&EventId>)->Result<Option<EventId>> {
    let Some(limits)=selected(task)? else{return Ok(None)};
    let waits=waits(tx,task,limits)?;
    if waits.iter().any(|w|matches!(w.status,WaitStatus::Open))||task.waiting_child_id.is_some() {return Ok(None)}
    let input=if task.attempt.as_ref().is_some_and(|a|!a.stopped&&a.phase==AttemptPhase::Running) {
        if supports_delivery(task) {super::super::collaboration_notifications::completion_inputs(tx,task,now,limits.scan_rows)?}
        else {super::super::collaboration_notifications::CompletionInputs {items:Vec::new(),pending_begin:super::super::collaboration_notifications::delivery_blocks_admission(tx,task)?}}
    }else {stopped_inputs(tx,task,now,limits)?};
    if input.pending_begin {return Ok(None)}
    let current=workflow::product_revision(tx)?;
    let through=match expected {
        Some(id) if id.product_revision>hold.observed_product_revision&&id.product_revision<=current=>id.product_revision,
        Some(_)=>return Ok(None),
        None=>current,
    };
    let reset=match tx.workflow_policy {
        Some(policy)=>reset_event(tx,task,&waits,&input.items,Some(hold),through,limits,policy)?,
        None=>None,
    };
    Ok(match expected {Some(id) if reset.as_ref()!=Some(id)=>None,_=>reset})
}

/// Called by the existing ordinary admission calculation, before allocating its
/// attempt/RunAttempt. The caller still performs every existing capacity,
/// deadline, dependency, stop, uncertainty and remaining-accounting check.
pub(in crate::controller) fn resume_for_admission(tx:&StoreContext<'_>,stored:&StoredTask,now:u64)->Result<Option<CompletionResume>> {
    let Some(hold)=&stored.completion_hold else{return Ok(None)};
    validate_hold(tx,&stored.task,hold)?;
    if !matches!(hold.state.disposition,CompletionDisposition::Blocked {..}) {return Ok(None)}
    let task=&stored.task;let Some(limits)=selected(task)? else{return Ok(None)};
    let own_reason=format!("governance_completion_blocked:{}",hold.decision.operation_id);
    let reason_ok=matches!(task.blocked_reason.as_deref(),Some("workflow_waits_ended"|"child_outcome_ready"))
        ||task.blocked_reason.as_deref()==Some(own_reason.as_str());
    if task.status!=TaskStatus::Queued||task.native_retry_block.is_some()||!reason_ok
        ||hold.continuation.as_ref().is_some_and(|c|c.acknowledged.is_none())
        ||task.attempt.as_ref().is_none_or(|a|!a.stopped||a.phase!=AttemptPhase::Stopped||a.uncertain)
        ||stored.unresolved_execution||task.waiting_child_id.is_some()||now>=task.input.limits.deadline_unix_ms
        ||super::super::operations::pending_effects(tx,&task.id)?!=0
        ||super::super::collaboration_work::admission_blocked(tx,&task.id)? {return Ok(None)}
    let accepted=&task.input.resolved_limits.as_ref().unwrap().global_metadata;
    let cap=super::super::governance::continuation_limit(tx,accepted)?;
    if !super::super::governance::completion_enforced(tx)?
        ||super::super::operations::used_model_requests(tx,&task.id)?.saturating_add(u64::from(stored.delegated_model_requests))>=u64::from(task.input.limits.model_requests)
        ||stopped_inputs(tx,task,now,limits)?.pending_begin {return Ok(None)}
    let reset=resume_reset(tx,task,hold,now,None)?;
    let eligible=if reset.is_some() {cap>0} else {
        hold.continuation.is_none()&&matches!(hold.state.disposition,CompletionDisposition::Blocked {continuation_eligible:true,exhausted:false})
            &&hold.state.consecutive_count<=cap
    };
    if !eligible {return Ok(None)}
    let (_,evaluation)=actions::recorded_evaluation(tx,&hold.decision)?;
    Ok(Some(CompletionResume {evaluation,reset_event:reset}))
}
