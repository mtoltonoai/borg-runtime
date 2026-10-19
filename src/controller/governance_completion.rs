//! Bounded completion assessment in the original controller transaction.
//! This module never dispatches a model request, acknowledges input, retries an
//! operation, or commits a receipt. The producer must first retain the recorded
//! reason through its supported continuation boundary.
use super::database::{self, decode, integer, load, refuse, Fault, Result, StoreContext, StoredTask};
use super::collaboration_workflow as workflow;
use crate::collaboration::{work::{WaitStatus, WorkflowWait}, workflow::*};
use crate::governance::{CompletionDecision, CompletionDisposition, CompletionFailure,
    CompletionHold, CompletionReason, CompletionSubject, DecisionPoint, DecisionRef};
use crate::limits::{GovernanceLimits, WorkflowLimits};
use crate::protocol::{AttemptPhase, Task, TaskStatus};
use rusqlite::params;
use std::collections::BTreeMap;

#[path="governance_completion_actions.rs"]
mod actions;
pub(super) use actions::{apply, before_delivery, validate_native_request, validate_native_final};
#[path="governance_completion_final.rs"]
mod final_acceptance;
pub(super) use final_acceptance::{before_result, resume_for_admission, capture_before_stop, validate_stop_failure_envelope, mark_stopped_hold};

pub(super) struct Assessment {
    pub decision: CompletionDecision,
    pub observed_product_revision: u64,
}

fn selected(task: &Task) -> Result<Option<&GovernanceLimits>> {
    task.input.execution_limits().map_err(Fault::Store)?;
    Ok(task.input.resolved_limits.as_ref().ok_or_else(||
        Fault::Store("completion task has no accepted metadata".into()))?
        .global_metadata.governance.as_ref())
}

fn subject(task: &Task) -> Result<CompletionSubject> {
    let attempt = task.attempt.as_ref().ok_or_else(||
        Fault::Store("completion has no accepted attempt".into()))?;
    if attempt.task_id != task.id || attempt.session_id != task.session_id {
        return Err(Fault::Store("completion task and attempt identity differ".into()));
    }
    Ok(CompletionSubject { task_id: task.id.clone(), session_id: task.session_id.clone(),
        attempt_id: attempt.id.clone(), generation: attempt.generation,
        task_revision: task.revision, accepted_global_revision: task.input.resolved_limits.as_ref()
            .ok_or_else(||Fault::Store("completion accepted metadata absent".into()))?.global_revision.clone() })
}

/// Retained state is not authority. Validate its exact task, historical attempt,
/// accepted policy and bounded decision before using its count or watermark.
fn validate_hold_value(tx: &StoreContext<'_>, task: &Task, hold: &CompletionHold) -> Result<()> {
    let limits = selected(task)?.ok_or_else(|| Fault::Store("completion state without accepted policy".into()))?;
    let s = &hold.state.subject;
    let current = subject(task)?;
    if hold.decision.point != DecisionPoint::Completion
        || !database::valid_id(tx.limits, &hold.decision.operation_id)
        || s.task_id != current.task_id || s.session_id != current.session_id
        || s.accepted_global_revision != current.accepted_global_revision
        || s.task_revision > task.revision || s.generation > current.generation
        || hold.observed_product_revision > workflow::product_revision(tx)?
        || hold.state.consecutive_count > limits.completion_continuations.saturating_add(1)
        || hold.state.reasons.len() > limits.reason_items
        || super::bounded_json_size(&hold.state.reasons, limits.reason_bytes).is_err()
        || super::bounded_json_size(hold, limits.record_bytes).is_err() {
        return Err(Fault::Store("invalid retained completion state".into()));
    }
    let raw: String = tx.query_row(
        "SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM attempts WHERE id=?1",
        params![s.attempt_id, super::database::sql_bound(tx.limits.transport.body_bytes.saturating_add(1))?], |r|r.get(0))?;
    if raw.len() > tx.limits.transport.body_bytes { return Err(Fault::Store("completion attempt exceeds bound".into())); }
    let attempt: crate::protocol::Attempt = decode(&raw)?;
    if attempt.id != s.attempt_id || attempt.task_id != s.task_id
        || attempt.session_id != s.session_id || attempt.generation != s.generation {
        return Err(Fault::Store("retained completion attempt binding differs".into()));
    }
    if let Some(event) = &hold.state.independent_reset_event {
        let database_id:String=tx.query_row("SELECT value FROM settings WHERE name='database_id'",[],|r|r.get(0))?;
        if event.database_id != database_id || event.product_revision == 0
            || event.product_revision > hold.observed_product_revision {
            return Err(Fault::Store("completion reset witness is outside its evaluated view".into()));
        }
    }
    match &hold.state.disposition {
        CompletionDisposition::Admit if hold.state.reasons.is_empty() => {},
        CompletionDisposition::Blocked { continuation_eligible, exhausted }
            if !hold.state.reasons.is_empty()
                && *exhausted == (hold.state.consecutive_count > limits.completion_continuations)
                && !(*continuation_eligible && (*exhausted || hold.state.reasons.iter().any(|r|
                    matches!(r,CompletionReason::EvaluationUnavailable {..})))) => {},
        _ => return Err(Fault::Store("completion disposition and bounded evidence differ".into())),
    }
    Ok(())
}

pub(super) fn validate_hold(tx: &StoreContext<'_>, task: &Task, hold: &CompletionHold) -> Result<()> {
    validate_hold_value(tx,task,hold)?;
    let record=super::governance::read_decision(tx,&hold.decision)?;
    let outcome=match hold.state.disposition {
        CompletionDisposition::Admit=>crate::governance::ActualDecisionOutcome::Applied,
        CompletionDisposition::Blocked {..}=>crate::governance::ActualDecisionOutcome::GovernanceBlocked,
    };
    if record.id!=hold.decision || record.detail!=crate::governance::DecisionDetail::Completion(hold.state.clone())
        || record.observed_product_revision!=Some(hold.observed_product_revision)
        || record.mode!=crate::governance::DecisionMode::Enforcing
        || record.actual_outcome!=outcome {
        return Err(Fault::Store("completion hold differs from its original enforcing receipt".into()));
    }
    actions::validate_continuation(tx,task,hold)?;
    Ok(())
}

/// Existing admission reads this retained state. Admit keeps the count; merely
/// clearing the block is never an independent-event reset.
pub(super) fn admission_blocked(tx: &StoreContext<'_>, stored: &StoredTask, now:u64) -> Result<bool> {
    let Some(hold) = &stored.completion_hold else { return Ok(false); };
    validate_hold(tx, &stored.task, hold)?;
    Ok(matches!(hold.state.disposition, CompletionDisposition::Blocked {..})
        && final_acceptance::resume_for_admission(tx,stored,now)?.is_none())
}

fn independent(task: &Task, event: &WorkflowActivity) -> bool {
    match &event.actor.provenance {
        ActorProvenance::BoundAgentSession { task_id, session_id, .. } =>
            task_id != &task.id && session_id != &task.session_id,
        ActorProvenance::TrustedHost { path: TrustedHostPath::Browser | TrustedHostPath::OperatorApi } => true,
        // Attribution alone is insufficient. Only an original external Task
        // terminal occurrence can represent independent lifecycle resolution.
        ActorProvenance::TrustedHost { path: TrustedHostPath::ControllerMaintenance } =>
            matches!(&event.detail, ActivityDetail::Task(TaskChange::Closed {resolution})
                if resolution.task_id != task.id),
    }
}

/// Only actual required-work changes are candidates. Owner changes, comments,
/// self progress, repeated scans and the decision/reason itself are not resets.
fn wait_change(event: &WorkflowActivity, task: &Task, waits: &[WorkflowWait]) -> bool {
    // An explicit resolved wait remains a required-work association even before
    // a new attempt has admitted its input refs. Use the original terminal
    // occurrence, never the separate WaitResolved audit sibling.
    let outcome=match &event.detail {
        ActivityDetail::Task(TaskChange::Closed {resolution})=>Some(ResolverOutcome::Task(resolution.clone())),
        ActivityDetail::Question(QuestionChange::Closed {resolution})=>Some(ResolverOutcome::Question(resolution.clone())),
        ActivityDetail::Review(ReviewChange::Closed {resolution})=>Some(ResolverOutcome::Review(resolution.clone())),
        _=>None,
    };
    if let Some(outcome)=outcome {
        if waits.iter().any(|w|w.reference.task_id==task.id&&matches!(&w.status,
            WaitStatus::Resolved {resolution,actor,..} if resolution.event_id==event.id
                &&resolution.outcome==outcome&&actor==&event.actor)) {return true}
    }
    if event.subject != (RecordRef::Task { task_id: task.id.clone() }) { return false; }
    match &event.detail {
        ActivityDetail::Wait(WaitChange::Added {wait,resolver}) => waits.iter().any(|w|
            &w.reference==wait && &w.resolver==resolver && w.product_revision==event.id.product_revision
                && w.created_by==event.actor),
        ActivityDetail::Wait(WaitChange::Removed {wait,resolver,reason}) => waits.iter().any(|w|
            &w.reference==wait && &w.resolver==resolver && matches!(&w.status,
                WaitStatus::Removed {reason:original,actor,product_revision,..}
                    if original==&reason.text && !reason.truncated && actor==&event.actor
                        && *product_revision==event.id.product_revision)),
        // Resolved waits use the original terminal event, supplied by DELIVERY;
        // the queryable WaitResolved audit sibling is not a second input.
        _ => false,
    }
}


// Traceability scope: Only a later independent event changing this task required work or applicable input resets the retained count.
//= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
//# The consecutive blocked-completion count MUST reset only on an independently originated event that changes the required work or applicable inputs.

fn reset_event(tx: &StoreContext<'_>, task: &Task, waits: &[WorkflowWait],
    inputs: &[super::collaboration_notifications::CompletionInput], prior: Option<&CompletionHold>,
    current: u64, limits: &GovernanceLimits, workflow_limits: &WorkflowLimits) -> Result<Option<EventId>> {
    let Some(prior) = prior else { return Ok(None); };
    let after = prior.observed_product_revision;
    let mut candidates = BTreeMap::new();
    for input in inputs {
        if input.event.id.product_revision > after && input.event.id.product_revision <= current
            && independent(task, &input.event) {
            candidates.insert(input.event.id.clone(), ());
        }
    }
    let mut statement = tx.prepare(
        "SELECT product_revision,ordinal,CAST(substr(CAST(body AS BLOB),1,?3) AS TEXT) FROM workflow_events WHERE product_revision>?1 AND product_revision<=?2 ORDER BY product_revision,ordinal LIMIT ?4")?;
    let rows = statement.query_map(params![integer(after)?,integer(current)?,
        super::database::sql_bound(workflow_limits.event_bytes.saturating_add(1))?,super::database::sql_bound(limits.scan_rows.saturating_add(1))?],
        |r|Ok((r.get::<_,i64>(0)?,r.get::<_,u32>(1)?,r.get::<_,String>(2)?)))?
        .collect::<std::result::Result<Vec<_>,_>>()?;
    if rows.len()>limits.scan_rows { return refuse("capacity","completion reset scan exceeds governance.scan_rows"); }
    let database_id:String=tx.query_row("SELECT value FROM settings WHERE name='database_id'",[],|r|r.get(0))?;
    for (revision,ordinal,raw) in rows {
        if raw.len()>workflow_limits.event_bytes { return Err(Fault::Store("oversized completion reset event".into())); }
        let event:WorkflowActivity=decode(&raw)?;
        if event.id.database_id!=database_id || integer(event.id.product_revision)?!=revision || event.id.ordinal!=ordinal {
            return Err(Fault::Store("completion reset event identity differs".into()));
        }
        let child_change=match &event.detail {
            ActivityDetail::Task(TaskChange::Closed {resolution}) if task.completed_child.as_ref()
                .is_some_and(|child| child.task_id==resolution.task_id) => {
                super::collaboration_finalizer::validate_task_resolution(tx,resolution)?;
                let child=task.completed_child.as_ref().unwrap();
                let status=match resolution.status {
                    TerminalTaskStatus::Succeeded=>TaskStatus::Succeeded,
                    TerminalTaskStatus::Failed=>TaskStatus::Failed,
                    TerminalTaskStatus::Cancelled=>TaskStatus::Cancelled,
                };
                child.status==status && child.summary==resolution.summary
                    && child.result.is_some()==resolution.accepted_result.is_some()
            },
            _=>false,
        };
        if (wait_change(&event,task,waits) || child_change) && independent(task,&event) {
            workflow::validate_recorded_actor(tx,&event.actor)?;
            candidates.insert(event.id,());
        }
    }
    Ok(candidates.pop_last().map(|(id,_)|id))
}

fn decide(subject: CompletionSubject, mut reasons: Vec<CompletionReason>,
    previous_count: usize, reset: Option<EventId>, continuable: bool,
    limits: &GovernanceLimits) -> CompletionDecision {
    let failed = reasons.iter().any(|r|matches!(r,CompletionReason::EvaluationUnavailable {..}));
    if reasons.len()>limits.reason_items || super::bounded_json_size(&reasons,limits.reason_bytes).is_err() {
        reasons=vec![CompletionReason::EvaluationUnavailable {code:CompletionFailure::BoundExceeded}];
    }

    // Traceability scope: Exhaustion or evaluation failure disables continuation; persist_hold retains the visible typed block and original reason.
    //= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
    //# Exhausting the blocked-completion bound MUST leave the task visibly blocked without authorizing another model turn.
    //= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
    //# Failure of the completion check MUST leave the task visibly blocked without authorizing another model turn.

    let failure = failed || reasons.iter().any(|r|matches!(r,CompletionReason::EvaluationUnavailable {..}));
    let before=if reset.is_some() {0} else {previous_count};
    let consecutive_count=if reasons.is_empty() {before} else {
        before.saturating_add(1).min(limits.completion_continuations.saturating_add(1))
    };
    let exhausted=consecutive_count>limits.completion_continuations;

    // Traceability scope: Only an empty canonical remaining-work reason set admits voluntary completion.
    //= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
    //# A voluntary completion decision MUST refuse completion while required work remains unfinished.

    let disposition=if reasons.is_empty() {CompletionDisposition::Admit} else {
        CompletionDisposition::Blocked {continuation_eligible:continuable && !failure && !exhausted,exhausted}
    };
    CompletionDecision {subject,disposition,reasons,consecutive_count,independent_reset_event:reset}
}

/// This is the active supported boundary, before final completion evidence or a
/// physical stop. A stopped scope cannot prove the absence of an owed route.
/// Caller ownership and the concrete boundary proof are validated by dispatch.
pub(super) fn assess_running(tx: &StoreContext<'_>, stored: &StoredTask, now: u64) -> Result<Option<Assessment>> {
    if !super::governance::completion_enforced(tx)? {return Ok(None)};
    assess_at(tx,stored,now,None)
}


// Traceability scope: Selected completion enforcement reads canonical task, waits, child work and owed input at its supported boundary.
//= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
//# A voluntary completion decision MUST evaluate the current canonical required work for its task.


// Traceability scope: Continuation eligibility also retains unresolved-effect, current-owner, deadline and remaining-accounting fences.
//= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
//# A continuation after refused voluntary completion MUST preserve the task's original identity, accepted settings, remaining deadline, accounting and unresolved effects.

fn assess_at(tx:&StoreContext<'_>,stored:&StoredTask,now:u64,candidate_bound:Option<usize>)->Result<Option<Assessment>> {
    let task=&stored.task;
    let Some(limits)=selected(task)? else {return Ok(None)};
    let s=subject(task)?;
    let attempt=task.attempt.as_ref().unwrap();
    if task.status!=TaskStatus::Active || task.native_retry_block.is_some() || attempt.phase!=AttemptPhase::Running || attempt.stopped
        || attempt.uncertain || stored.unresolved_execution || now>=task.input.limits.deadline_unix_ms {
        return refuse("invalid_transition","completion requires its current active safe boundary");
    }
    if super::operations::pending_effects(tx,&task.id)?!=0 {
        return refuse("reconciliation_required","completion cannot pass unresolved original effects");
    }
    if let Some(hold)=&stored.completion_hold {
        validate_hold(tx,task,hold)?;
        if hold.continuation.as_ref().is_some_and(|c|c.acknowledged.is_none()) {
            return refuse("delivery_pending","original completion input remains unacknowledged");
        }
    }
    let current=workflow::product_revision(tx)?;
    let previous=stored.completion_hold.as_ref().map_or(0,|h|h.state.consecutive_count);
    let evaluated=(||->Result<(Vec<CompletionReason>,Option<EventId>,bool)> {
        let workflow_limits=tx.workflow_policy;
        let waits=match workflow_limits {
            Some(policy)=>super::collaboration_work::completion_waits(tx,&task.id,policy,limits.scan_rows)?,
            None=>{
                if super::collaboration_work::admission_blocked(tx,&task.id)? {
                    return refuse("workflow_disabled","canonical required waits lack their selected policy");
                }
                Vec::new()
            },
        };
        let supports_delivery=matches!(task.input.definition.execution_profile,Some(crate::protocol::ExecutionProfile::CollaborationV2 {..}))
            && task.input.resolved_limits.as_ref().is_some_and(|accepted|accepted.global_metadata.workflow.is_some());
        let input=if supports_delivery {
            super::collaboration_notifications::completion_inputs(tx,task,now,limits.scan_rows)?
        } else {
            if let Some(policy)=workflow_limits {
                if !super::collaboration_work::pending_resolution_refs(tx,&task.id,policy)?.is_empty() {
                    return refuse("not_configured","required resolution input has no accepted delivery capability");
                }
            }
            // Concrete and dynamic routes only select accepted V2 scopes.
            // A retained unknown input remains a fence regardless of profile.
            super::collaboration_notifications::CompletionInputs {items:Vec::new(),
                pending_begin:super::collaboration_notifications::delivery_blocks_admission(tx,task)?}
        };
        let mut reasons=waits.iter().filter(|w|matches!(w.status,WaitStatus::Open))
            .map(|w|CompletionReason::OpenWait {wait:w.reference.clone()}).collect::<Vec<_>>();
        if let Some(child)=&task.waiting_child_id {
            let actual=load(tx,child)?.task;
            if actual.parent_task_id.as_deref()!=Some(task.id.as_str()) || database::terminal(actual.status) {
                return Err(Fault::Store("completion child wait differs from canonical child".into()));
            }
            reasons.push(CompletionReason::DelegatedTask {task_id:child.clone()});
        }
        for item in &input.items {

            // Traceability scope: Observed or prepared input stays a blocking reason until exact consumption is acknowledged.
            //= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
            //# A voluntary completion decision MUST refuse completion while an applicable required input remains unconsumed.

            if !item.consumed { reasons.push(CompletionReason::UnconsumedInput {event_id:item.event.id.clone()}); }
        }
        let reset=match workflow_limits {
            Some(policy)=>reset_event(tx,task,&waits,&input.items,stored.completion_hold.as_ref(),current,limits,policy)?,
            None=>None,
        };
        if input.pending_begin {
            reasons.push(CompletionReason::EvaluationUnavailable {code:CompletionFailure::Unavailable});
        }
        let used=super::operations::used_model_requests(tx,&task.id)?
            .saturating_add(u64::from(stored.delegated_model_requests));
        let accepted=&task.input.resolved_limits.as_ref().unwrap().global_metadata;

        // Traceability scope: The continuation bound is selected from versioned policy and accepted task metadata, within its existing allowance.
        //= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
        //# Consecutive continuations caused by refused voluntary completion MUST be bounded by versioned metadata.

        let selected_bound=match candidate_bound {
            Some(value)=>value.min(limits.completion_continuations),
            None=>super::governance::continuation_limit(tx,accepted)?,
        };
        let next_count=if reset.is_some() {1} else {previous.saturating_add(1)};
        let continuable=used<u64::from(task.input.limits.model_requests)
            && next_count<=selected_bound
            && !input.pending_begin && !reasons.iter().any(|r|matches!(r,
            CompletionReason::OpenWait {..}|CompletionReason::DelegatedTask {..}));
        Ok((reasons,reset,continuable))
    })();
    let (reasons,reset,continuable)=match evaluated {
        Ok(value)=>value,
        Err(Fault::Refused("capacity",_))=>(vec![CompletionReason::EvaluationUnavailable {code:CompletionFailure::BoundExceeded}],None,false),
        Err(Fault::Refused(_,_))=>(vec![CompletionReason::EvaluationUnavailable {code:CompletionFailure::Unavailable}],None,false),
        Err(Fault::Store(_))=>(vec![CompletionReason::EvaluationUnavailable {code:CompletionFailure::InvalidCanonicalState}],None,false),
        Err(other)=>return Err(other),
    };
    let decision=decide(s,reasons,previous,reset,continuable,limits);
    if super::bounded_json_size(&decision.reasons,limits.reason_bytes).is_err()
        || super::bounded_json_size(&decision,limits.record_bytes).is_err() {
        return refuse("capacity","minimum completion failure record exceeds accepted governance bounds");
    }
    Ok(Some(Assessment {decision,observed_product_revision:current}))
}

/// Separate operator shadow input: only the still-current original NotEnforced
/// boundary may be evaluated. It creates no hold, counter, event or model work.
/// R validates the exact candidate rule/config and records the shadow outcome.
pub(super) fn shadow_assess_original(tx:&StoreContext<'_>,original:&crate::governance_completion::EvaluateCompletion,
    candidate_continuations:usize,now:u64)->Result<Option<Assessment>> {
    let receipt=super::governance::original_receipt(tx,&original.operation_id)?.ok_or_else(||Fault::Refused("not_found","shadow original completion receipt absent".into()))?;
    if receipt.commit.controller_revision!=super::metadata::current(tx)?
        ||!matches!(receipt.request,crate::protocol::Operation::EvaluateCompletion(ref value) if value==original)
        ||!matches!(receipt.outcome,crate::protocol::Outcome::Ok {data:crate::protocol::Data::Completion(completion)} if matches!(completion.as_ref(),crate::governance_completion::CompletionOutcome::NotEnforced)) {
        return refuse("stale_revision","shadow cannot reconstruct a moved completion input view");
    }
    let stored=super::collaboration_calls::caller(tx,&original.caller,now)?;
    actions::original_boundary(tx,&stored,&original.boundary,&original.intervening_requests)?;
    assess_at(tx,&stored,now,Some(candidate_continuations))
}

/// Called solely by the original receipt finalizer after the final response and
/// mandatory bounds. On refusal the inner action has rolled back; this typed
/// hold and its audit are committed together in the existing outer transaction.

// Traceability scope: The bounded decision and original audit are committed with the hold in the existing receipt transaction.
//= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
//# A refused voluntary completion MUST record a bounded reason identifying the remaining required work or input.

pub(super) fn persist_hold(tx:&StoreContext<'_>, id:&DecisionRef, decision:&CompletionDecision,
    observed_product_revision:u64, _now:u64) -> Result<()> {
    let mut stored=load(tx,&decision.subject.task_id)?;
    if database::terminal(stored.task.status) {
        // A successful final acceptance may advance the task exactly once. Its
        // original audit retains the decision; terminal records never relaunch.
        if matches!(decision.disposition,CompletionDisposition::Admit)
            && stored.task.revision==decision.subject.task_revision.saturating_add(1) {
            return Ok(());
        }
        return Err(Fault::Store("completion hold cannot replace a terminal decision".into()));
    }
    if subject(&stored.task)?!=decision.subject {
        return Err(Fault::Store("completion decision view changed before original receipt commit".into()));
    }
    let hold=CompletionHold {decision:id.clone(),state:decision.clone(),observed_product_revision,continuation:None};
    // The finalizer inserts the exact enclosing receipt after this hook in the
    // same transaction. It cannot be queried before it exists; every later read
    // uses validate_hold and joins the immutable original audit.
    validate_hold_value(tx,&stored.task,&hold)?;
    stored.task.blocked_reason=match decision.disposition {
        CompletionDisposition::Admit=>None,
        CompletionDisposition::Blocked {..}=>Some(format!("governance_completion_blocked:{}",id.operation_id)),
    };
    // Verified stop is retained. Ordinary admission may select a new attempt
    // only through resume_for_admission; no old process/attempt is revived.
    if matches!(decision.disposition,CompletionDisposition::Blocked {..})
        && stored.task.attempt.as_ref().is_some_and(|a|a.stopped&&!a.uncertain) {
        stored.task.status=TaskStatus::Queued;
    }
    stored.completion_hold=Some(hold);
    database::save(tx,&mut stored,"completion_decision",id)?;
    Ok(())
}
