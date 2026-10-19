//! The three closed worker exchanges share the original operation receipt.
use super::*;
use crate::governance_completion::*;
use crate::{mcp::Caller, protocol::{Data,Operation,Outcome,ExecutorKind,Observation}};
use super::super::{metadata, collaboration_calls, collaboration_notifications, governance};

fn actor(task:&Task,caller:&Caller)->RecordedActor {
    RecordedActor {principal:PrincipalRef::Agent {agent_id:task.assigned_agent_id.clone()},
        provenance:ActorProvenance::BoundAgentSession {agent_id:task.assigned_agent_id.clone(),
            session_id:task.session_id.clone(),task_id:caller.task_id.clone(),attempt_id:caller.attempt_id.clone(),
            generation:caller.generation,worker_id:caller.worker_id.clone(),incarnation:caller.incarnation.clone()}}
}
fn identity(tx:&StoreContext<'_>,actual:&str,expected:String)->Result<()> {
    if !database::valid_id(tx.limits,actual) || actual!=expected {
        return refuse("operation_conflict","completion operation is not its original boundary identity");
    }
    Ok(())
}
pub(super) fn recorded_evaluation(tx:&StoreContext<'_>,id:&DecisionRef)->Result<(EvaluateCompletion,CompletionEvaluation)> {
    let original=governance::original_receipt(tx,&id.operation_id)?.ok_or_else(||Fault::Refused("not_found","completion decision receipt absent".into()))?;
    let input=match &original.request {
        Operation::EvaluateCompletion(input)=>{
            identity(tx,&input.operation_id,evaluation_id(&input.caller,&input.boundary)?)?;
            if matches!(input.boundary,CompletionBoundary::StoppedResult {..}) {return refuse("source_mismatch","Evaluate cannot supply a stopped result boundary");}
            input.clone()
        },
        Operation::ObserveWorker {report} if report.report_id==id.operation_id&&matches!(report.observation,Observation::ResultCandidate {..})=>
            EvaluateCompletion {operation_id:report.report_id.clone(),caller:Caller {task_id:report.task_id.clone(),attempt_id:report.attempt_id.clone(),generation:report.generation,worker_id:report.worker_id.clone(),incarnation:report.incarnation.clone()},boundary:CompletionBoundary::StoppedResult {report_id:report.report_id.clone()},intervening_requests:Vec::new()},
        _=>return refuse("source_mismatch","completion decision is not an original completion boundary"),
    };
    let record=governance::read_decision(tx,id)?;
    let crate::governance::DecisionDetail::Completion(state)=record.detail else {
        return refuse("source_mismatch","receipt does not contain a completion decision");
    };
    if record.id!=*id || record.mode!=crate::governance::DecisionMode::Enforcing
        || state.subject.task_id!=input.caller.task_id || state.subject.attempt_id!=input.caller.attempt_id
        || state.subject.generation!=input.caller.generation {
        return refuse("source_mismatch","completion decision owner or enforcing mode differs");
    }
    let value=CompletionEvaluation {decision:id.clone(),state,boundary:input.boundary.clone()};
    let valid=match (&value.state.disposition,&record.actual_outcome,&original.outcome) {
        (CompletionDisposition::Admit,crate::governance::ActualDecisionOutcome::Applied,
            Outcome::Ok {data:Data::Completion(found)})=>matches!(found.as_ref(),CompletionOutcome::Evaluated(found) if found==&value),
        (CompletionDisposition::Blocked {..},crate::governance::ActualDecisionOutcome::GovernanceBlocked,
            Outcome::Refused {code,..})=>code=="governance_completion_blocked",
        _=>false,
    };
    if !valid {return refuse("source_mismatch","completion original outcome differs from its decision");}
    Ok((input,value))
}

pub(super) fn original_boundary(tx:&StoreContext<'_>,stored:&StoredTask,boundary:&CompletionBoundary,requests:&[crate::native_loop_contract::NativeRequestEvidence])->Result<()> {
    let task=&stored.task;let attempt=task.attempt.as_ref().unwrap();
    match (task.input.definition.executor,boundary) {
        (ExecutorKind::Native,CompletionBoundary::NativeResponse {request_index})=>{
            let used:u32=tx.query_row("SELECT model_requests FROM attempts WHERE id=?1",[&attempt.id],|r|r.get(0))?;
            let caller=Caller {task_id:task.id.clone(),attempt_id:attempt.id.clone(),generation:attempt.generation,worker_id:attempt.worker_id.clone(),incarnation:attempt.worker_incarnation.clone()};
            let (charged,_)=super::super::operations::validate_completion_gap(tx,&caller,*request_index,requests)?;
            if *request_index==0 || charged!=used {return refuse("source_mismatch","native completion does not bind every charged intervening request");}
            let id=crate::native_loop_contract::reservation_id(&attempt.id,*request_index);
            let reservation=governance::original_receipt(tx,&id)?.ok_or_else(||Fault::Refused("source_mismatch","native completion reservation absent".into()))?;
            if !matches!(reservation.request,Operation::ObserveWorker {report} if report.task_id==task.id
                &&report.attempt_id==attempt.id&&report.generation==attempt.generation
                &&report.worker_id==attempt.worker_id&&report.incarnation==attempt.worker_incarnation
                &&matches!(report.observation,Observation::Usage {model_requests,..} if model_requests==*request_index))
                || !matches!(reservation.outcome,Outcome::Ok {..}) {
                return refuse("source_mismatch","native completion reservation has another body or outcome");
            }
        },
        (ExecutorKind::Coding,CompletionBoundary::CodexTurn {thread_id,turn_id})=>{
            if !requests.is_empty()||!database::valid_id(tx.limits,thread_id)||!database::valid_id(tx.limits,turn_id) {
                return refuse("invalid_input","invalid completed Codex boundary");
            }
            let initial:(Option<String>,Option<String>)=tx.query_row("SELECT thread_id,turn_id FROM attempts WHERE id=?1",[&attempt.id],|r|Ok((r.get(0)?,r.get(1)?)))?;
            let supported=stored.completion_hold.as_ref().and_then(|h|h.continuation.as_ref())
                .and_then(|c|c.acknowledged.as_ref()).map(|ack|&ack.consumed_at);
            let supplied_retry=task.input.execution_limits().map_err(|e|Fault::Store(e))?.coding.rate_limit_retry.is_some();
            if supplied_retry {
                let caller=Caller {task_id:task.id.clone(),attempt_id:attempt.id.clone(),generation:attempt.generation,
                    worker_id:attempt.worker_id.clone(),incarnation:attempt.worker_incarnation.clone()};
                super::super::operations::supplied_outer::completed_boundary(tx,&caller,thread_id,turn_id)?;
            } else if supported.map_or_else(||initial.0.as_ref()==Some(thread_id)&&initial.1.as_ref()==Some(turn_id),|actual|actual==boundary)==false {
                return refuse("source_mismatch","Codex completion is not the initial or acknowledged continuation turn");
            }
        },
        _=>return refuse("source_mismatch","completion boundary differs from accepted executor"),
    }
    Ok(())
}

fn target_matches(tx:&StoreContext<'_>,original:&EvaluateCompletion,target:&CompletionTarget)->Result<bool> {
    Ok(match (&original.boundary,target) {
        (CompletionBoundary::NativeResponse {request_index},CompletionTarget::NativeRequest {request_index:new})=>{
            let (charged,_)=super::super::operations::validate_completion_gap(tx,&original.caller,*request_index,&original.intervening_requests)?;
            charged.checked_add(1)==Some(*new)
        },
        (CompletionBoundary::CodexTurn {thread_id,turn_id},CompletionTarget::CodexTurn {thread_id:next,previous_turn_id})=>thread_id==next&&turn_id==previous_turn_id&&original.intervening_requests.is_empty(),
        _=>false,
    })
}
fn acknowledged_target(target:&CompletionTarget,boundary:&CompletionBoundary)->bool {
    match (target,boundary) {
        (CompletionTarget::NativeRequest {request_index:a}|CompletionTarget::InitialNativeRequest {request_index:a},CompletionBoundary::NativeResponse {request_index:b})=>b>=a,
        (CompletionTarget::InitialCodexTurn {thread_id},CompletionBoundary::CodexTurn {thread_id:actual,..})=>thread_id==actual,
        (CompletionTarget::CodexTurn {thread_id,previous_turn_id},CompletionBoundary::CodexTurn {thread_id:actual,turn_id})=>thread_id==actual&&turn_id!=previous_turn_id,
        _=>false,
    }
}

/// The old decision cannot grant a new caller by itself. Only the exact
/// ordinarily admitted RunAttempt can associate its reason with a fresh owner.

// Traceability scope: A resumed continuation must match its original same-task command, accepted settings, session, deadline and remaining allowance.
//= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
//# A continuation after refused voluntary completion MUST preserve the task's original identity, accepted settings, remaining deadline, accounting and unresolved effects.

fn validate_initial_command(tx:&StoreContext<'_>,task:&Task,caller:&Caller,evaluation:&CompletionEvaluation,target:&CompletionTarget)->Result<Box<CompletionResume>> {
    // Historical input ACKs stay valid after another ordinary attempt. Read
    // their exact command attempt; live Begin separately validated current caller.
    let raw_attempt:String=tx.query_row("SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM attempts WHERE id=?1",
        params![caller.attempt_id,database::sql_bound(tx.limits.transport.body_bytes.saturating_add(1))?],|r|r.get(0))?;
    if raw_attempt.len()>tx.limits.transport.body_bytes {return refuse("capacity","completion input attempt exceeds bound")}
    let a:crate::protocol::Attempt=decode(&raw_attempt)?;
    if a.task_id!=task.id||a.session_id!=task.session_id||caller.task_id!=task.id||caller.attempt_id!=a.id||caller.generation!=a.generation
        ||caller.worker_id!=a.worker_id||caller.incarnation!=a.worker_incarnation
        ||evaluation.state.subject.task_id!=task.id||evaluation.state.subject.session_id!=task.session_id
        ||evaluation.state.subject.attempt_id==a.id||evaluation.state.subject.generation>=a.generation
        ||task.input.resolved_limits.as_ref().is_none_or(|s|s.global_revision!=evaluation.state.subject.accepted_global_revision)
        {
        return refuse("source_mismatch","completion initial input has no ordinary same-task successor binding");
    }
    let command_id=format!("run-{}",a.id);
    let probe=database::sql_bound(tx.limits.controller.run_command_bytes.saturating_add(1))?;
    let raw:String=tx.query_row("SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM commands WHERE id=?1",params![command_id,probe],|r|r.get(0))?;
    if raw.len()>tx.limits.controller.run_command_bytes {return Err(Fault::Store("completion original RunAttempt exceeds bound".into()))}
    let command:crate::protocol::WorkerCommand=decode(&raw)?;
    let crate::protocol::WorkerCommand::RunAttempt {command_id:original,attempt,session,task:accepted,completion_continuation:Some(resume),..}=command else {
        return refuse("source_mismatch","original RunAttempt carries no completion reason");
    };
    let mut expected=task.input.clone();expected.limits.model_requests=accepted.limits.model_requests;
    if original!=command_id||attempt.id!=a.id||attempt.task_id!=task.id||attempt.session_id!=task.session_id
        ||attempt.generation!=a.generation||attempt.worker_id!=a.worker_id||attempt.worker_incarnation!=a.worker_incarnation
        ||attempt.phase!=AttemptPhase::Assigned||attempt.stopped||attempt.uncertain
        ||attempt.deadline_unix_ms!=task.input.limits.deadline_unix_ms||session.id!=task.session_id
        ||session.agent_id!=task.assigned_agent_id||accepted!=expected||resume.evaluation!=*evaluation
        ||accepted.limits.model_requests==0||accepted.limits.model_requests>task.input.limits.model_requests {
        return refuse("source_mismatch","completion reason differs from exact original admitted command");
    }
    let target_ok=match (task.input.definition.executor,target) {
        (ExecutorKind::Native,CompletionTarget::InitialNativeRequest {request_index})=>*request_index==1,
        (ExecutorKind::Coding,CompletionTarget::InitialCodexTurn {thread_id})=>database::valid_id(tx.limits,thread_id),
        _=>false,
    };
    if !target_ok {return refuse("source_mismatch","completion initial target differs from accepted executor")}
    let old:String=tx.query_row("SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM attempts WHERE id=?1",
        params![evaluation.state.subject.attempt_id,database::sql_bound(tx.limits.transport.body_bytes.saturating_add(1))?],|r|r.get(0))?;
    if old.len()>tx.limits.transport.body_bytes {return refuse("capacity","prior completion attempt exceeds bound")}
    let old:crate::protocol::Attempt=decode(&old)?;
    if old.id!=evaluation.state.subject.attempt_id||old.task_id!=task.id||old.session_id!=task.session_id||old.generation!=evaluation.state.subject.generation
        ||!old.stopped||old.phase!=AttemptPhase::Stopped||old.uncertain {
        return refuse("source_mismatch","completion successor lacks exact stopped nonuncertain prior attempt");
    }
    Ok(resume)
}

fn validate_delivery_ack(tx:&StoreContext<'_>,value:&CompletionContinuation,ack:&CompletionInputAck)->Result<()> {
    match (&value.delivery_batch_id,&ack.delivery_ack_operation_id) {
        (None,None)=>Ok(()),
        (Some(batch),Some(operation_id))=>{
            if !database::valid_id(tx.limits,operation_id) {return refuse("source_mismatch","paired input ACK identity is invalid");}
            let original=governance::original_receipt(tx,operation_id)?.ok_or_else(||Fault::Refused("delivery_pending","paired workflow ACK receipt is absent".into()))?;
            if !matches!(original.request,Operation::AcknowledgeCollaborationDelivery {operation_id:ref id,ref caller,ref batch_id,ref context_binding}
                if id==operation_id&&caller==&value.caller&&batch_id==batch&&context_binding==&value.context_binding)
                || !matches!(original.outcome,Outcome::Ok {data:Data::CollaborationDelivery(ref observation)} if observation.pending.is_none()) {
                return refuse("source_mismatch","paired workflow ACK differs from the original input context");
            }
            Ok(())
        },
        _=>refuse("source_mismatch","paired input requires exactly its original workflow ACK"),
    }
}

/// The original fixed Begin target remains the first POST. A later response
/// consumes that same input only with every intervening confirmed rejection and
/// an identical host-captured serialized request binding. No range walk or
/// rejected response is counted as input consumption.
fn validate_native_ack(tx:&StoreContext<'_>,task:&Task,value:&CompletionContinuation,ack:&CompletionInputAck)->Result<()> {
    let first=match value.target {
        CompletionTarget::NativeRequest {request_index}|CompletionTarget::InitialNativeRequest {request_index}=>request_index,
        _=>{
            if ack.native_input.is_some() {return refuse("source_mismatch","Codex ACK contains native request evidence")}
            if task.input.execution_limits().map_err(Fault::Store)?.coding.rate_limit_retry.is_some() {
                let CompletionBoundary::CodexTurn{thread_id,turn_id}=&ack.consumed_at else{return refuse("source_mismatch","supplied completion boundary absent")};
                super::super::operations::supplied_outer::completion_consumed(tx,task,value,thread_id,turn_id)?;
            }
            return Ok(());
        },
    };
    let CompletionBoundary::NativeResponse {request_index:last}=ack.consumed_at else {return refuse("source_mismatch","native input has no actual response boundary")};
    let input=ack.native_input.as_ref().ok_or_else(||Fault::Refused("source_mismatch","native input binding absent".into()))?;
    let accepted=task.input.resolved_limits.as_ref().ok_or_else(||Fault::Store("native input accepted policy absent".into()))?;
    let limits=selected(task)?.ok_or_else(||Fault::Store("native completion policy absent".into()))?;
    let count=u32::try_from(input.rejections.len()).map_err(|_|Fault::Refused("capacity","native rejection evidence count exceeds bound".into()))?;
    if first==0||first.checked_add(count)!=Some(last)||input.rejections.len()>limits.scan_rows
        ||input.rejections.len()>task.input.limits.model_requests as usize
        ||super::super::bounded_json_size(input,limits.record_bytes.min(accepted.global_metadata.transport.body_bytes)).is_err() {
        return refuse("capacity","native input rejection evidence is incomplete or exceeds its explicit bound");
    }
    input.request_binding.validate(task.input.execution_limits().map_err(Fault::Store)?.native.request_bytes)
        .map_err(|e|Fault::Refused("source_mismatch",e))?;
    let begin=governance::original_receipt(tx,&value.begin_operation_id)?.ok_or_else(||Fault::Refused("source_mismatch","native input Begin absent".into()))?;
    let mut revision=begin.commit.controller_revision;
    for (offset,item) in input.rejections.iter().enumerate() {
        let index=first.checked_add(u32::try_from(offset).map_err(|_|Fault::Store("native rejection ordinal overflow".into()))?)
            .ok_or_else(||Fault::Store("native rejection index overflow".into()))?;
        if item.request_index!=index||item.operation_id!=crate::native_loop_contract::rejection_id(&value.caller.attempt_id,index) {
            return refuse("source_mismatch","native input rejection is not the exact next charged request");
        }
        let reservation_id=crate::native_loop_contract::reservation_id(&value.caller.attempt_id,index);
        governance::original_receipt(tx,&reservation_id)?.ok_or_else(||Fault::Refused("source_mismatch","native retry reservation absent".into()))?;
        governance::original_receipt(tx,&item.operation_id)?.ok_or_else(||Fault::Refused("source_mismatch","native rejection receipt absent".into()))?;
        let reserved=super::super::operations::native_retry::reservation(tx,&value.caller,index)?;
        let proof=super::super::operations::native_retry::confirmed_rejection(tx,&value.caller,index)?
            .ok_or_else(||Fault::Refused("source_mismatch","native rejection proof absent".into()))?;
        if reserved<=revision||proof.controller_revision<=reserved||proof.operation_id!=item.operation_id
            ||!proof.rejection.kind.same_input_retryable()
            ||proof.request_binding!=input.request_binding {
            return refuse("source_mismatch","native retry class, original input or receipt order differs");
        }
        revision=proof.controller_revision;
    }
    let final_reservation=crate::native_loop_contract::reservation_id(&value.caller.attempt_id,last);
    governance::original_receipt(tx,&final_reservation)?.ok_or_else(||Fault::Refused("source_mismatch","native response reservation absent".into()))?;
    if super::super::operations::native_retry::reservation(tx,&value.caller,last)?<=revision {
        return refuse("source_mismatch","native actual response charge precedes original input proof");
    }
    let rejected=crate::native_loop_contract::rejection_id(&value.caller.attempt_id,last);
    if governance::original_receipt(tx,&rejected)?.is_some() {
        return refuse("source_mismatch","a confirmed rejection cannot acknowledge model input consumption");
    }
    Ok(())
}

pub(super) fn validate_continuation(tx:&StoreContext<'_>,task:&Task,hold:&CompletionHold)->Result<()> {
    let Some(value)=&hold.continuation else {return Ok(())};
    let (original,evaluation)=recorded_evaluation(tx,&value.decision)?;
    let begin=governance::original_receipt(tx,&value.begin_operation_id)?.ok_or_else(||Fault::Store("completion Begin receipt missing".into()))?;
    let Operation::BeginCompletionContinuation(input)=begin.request else {return Err(Fault::Store("completion Begin receipt kind differs".into()))};
    if value.decision!=hold.decision||evaluation.state!=hold.state||value.caller.task_id!=task.id
        || input.caller!=value.caller||input.decision!=value.decision||input.context_binding!=value.context_binding
        ||input.target!=value.target||input.delivery_batch_id!=value.delivery_batch_id
        ||!(if matches!(value.target,CompletionTarget::InitialNativeRequest {..}|CompletionTarget::InitialCodexTurn {..}) {validate_initial_command(tx,task,&value.caller,&evaluation,&value.target)?;true} else {original.caller==value.caller&&target_matches(tx,&original,&value.target)?}) {
        return Err(Fault::Store("retained completion input differs from original decision or Begin".into()));
    }
    let mut first=value.clone();first.acknowledged=None;
    if !matches!(begin.outcome,Outcome::Ok {data:Data::Completion(feedback)}
        if matches!(feedback.as_ref(),CompletionOutcome::Begun(feedback) if feedback.evaluation==evaluation&&feedback.continuation==first)) {
        return Err(Fault::Store("completion Begin original outcome differs".into()));
    }
    identity(tx,&input.operation_id,begin_id(&value.caller,&value.decision)?)?;
    if let Some(ack)=&value.acknowledged {
        let original=governance::original_receipt(tx,&ack.operation_id)?.ok_or_else(||Fault::Store("completion input ACK receipt missing".into()))?;
        if !matches!(original.request,Operation::AcknowledgeCompletionInput(input)
            if input.caller==value.caller&&input.begin_operation_id==value.begin_operation_id&&input.consumed_at==ack.consumed_at&&input.delivery_ack_operation_id==ack.delivery_ack_operation_id&&input.native_input==ack.native_input)
            ||!matches!(original.outcome,Outcome::Ok {data:Data::Completion(actual)} if matches!(actual.as_ref(),CompletionOutcome::InputAcknowledged(actual) if actual==value))
            ||!acknowledged_target(&value.target,&ack.consumed_at) {
            return Err(Fault::Store("completion input ACK differs from its original supported boundary".into()));
        }
        identity(tx,&ack.operation_id,acknowledgement_id(&value.caller,&value.begin_operation_id)?)?;
        validate_delivery_ack(tx,value,ack)?;
        validate_native_ack(tx,task,value,ack)?;
    }
    Ok(())
}

pub(in crate::controller) fn apply(tx:&StoreContext<'_>,op:&Operation,cfg:&super::super::ControllerConfig,now:u64)->Result<(Data,bool)> {
    let caller=match op {
        Operation::EvaluateCompletion(input)=>&input.caller,
        Operation::BeginCompletionContinuation(input)=>&input.caller,
        Operation::AcknowledgeCompletionInput(input)=>&input.caller,
        _=>return Err(Fault::Store("wrong closed completion operation".into())),
    };
    let mut stored=collaboration_calls::caller(tx,caller,now)?;
    let result=match op {
        Operation::EvaluateCompletion(input)=>{
            identity(tx,&input.operation_id,evaluation_id(caller,&input.boundary)?)?;
            original_boundary(tx,&stored,&input.boundary,&input.intervening_requests)?;
            let Some(assessment)=assess_running(tx,&stored,now)? else {
                return Ok((Data::Completion(Box::new(CompletionOutcome::NotEnforced)),false));
            };
            let decision=DecisionRef {operation_id:input.operation_id.clone(),point:DecisionPoint::Completion};
            governance::stage_completion(tx,&input.operation_id,&actor(&stored.task,caller),assessment.decision.clone(),assessment.observed_product_revision)?;
            let after=(cfg.clock)();
            if after<now {return refuse("clock_unavailable","clock moved backwards during completion evaluation");}
            collaboration_calls::caller(tx,caller,after)?;
            if matches!(assessment.decision.disposition,CompletionDisposition::Blocked {..}) {
                return refuse("governance_completion_blocked",format!("remaining work is recorded in completion decision {}",input.operation_id));
            }
            (CompletionOutcome::Evaluated(CompletionEvaluation {decision,state:assessment.decision,boundary:input.boundary.clone()}),true)
        },
        Operation::BeginCompletionContinuation(input)=>{
            identity(tx,&input.operation_id,begin_id(caller,&input.decision)?)?;
            if !database::valid_id(tx.limits,&input.context_binding) {return refuse("invalid_input","invalid completion input context");}
            let (original,evaluation)=recorded_evaluation(tx,&input.decision)?;
            let initial=matches!(input.target,CompletionTarget::InitialNativeRequest {..}|CompletionTarget::InitialCodexTurn {..});
            let resume=if initial {Some(validate_initial_command(tx,&stored.task,caller,&evaluation,&input.target)?)}else{None};
            let target_valid=initial||(original.caller==*caller&&target_matches(tx,&original,&input.target)?);
            if !target_valid {
                return refuse("source_mismatch","completion continuation is not the original caller and next boundary");
            }
            if initial {
                let used:u32=tx.query_row("SELECT model_requests FROM attempts WHERE id=?1",[&caller.attempt_id],|r|r.get(0))?;
                let association:(Option<String>,Option<String>)=tx.query_row("SELECT thread_id,turn_id FROM attempts WHERE id=?1",[&caller.attempt_id],|r|Ok((r.get(0)?,r.get(1)?)))?;
                if used!=0||association.1.is_some() {return refuse("source_mismatch","initial completion intent follows an already dispatched input")}
            }
            let hold=stored.completion_hold.as_ref().ok_or_else(||Fault::Refused("not_found","completion hold absent".into()))?;
            validate_hold(tx,&stored.task,hold)?;
            let reset=resume.as_ref().and_then(|r|r.reset_event.as_ref());
            let reset_valid=if let Some(event)=reset {
                final_acceptance::resume_reset(tx,&stored.task,hold,now,Some(event))?.as_ref()==Some(event)
            }else{false};
            let original_eligible=matches!(hold.state.disposition,CompletionDisposition::Blocked {continuation_eligible:true,exhausted:false});
            if hold.decision!=input.decision||hold.state!=evaluation.state
                ||hold.continuation.as_ref().is_some_and(|c|c.acknowledged.is_none()||!initial||!reset_valid)
                ||(!original_eligible&&!reset_valid)||reset.is_some()&&!reset_valid {
                return refuse("governance_completion_blocked","recorded completion does not authorize another input");
            }
            // Historical reasons identify the owed occurrence; current assessment
            // establishes that this caller may still consume it now. Neither the
            // old receipt nor a supplied batch can grant new recipient authority.
            let Some(current)=assess_running(tx,&stored,now)? else {return refuse("governance_completion_blocked","completion enforcement changed before continuation");};
            if current.decision.reasons.iter().any(|r|matches!(r,CompletionReason::EvaluationUnavailable {..}|CompletionReason::OpenWait {..}|CompletionReason::DelegatedTask {..})) {
                return refuse("governance_completion_blocked","completion continuation has a current work or input hold");
            }
            let current_required:Vec<_>=current.decision.reasons.iter().filter_map(|r|match r {
                CompletionReason::UnconsumedInput {event_id}=>Some(event_id),_=>None,
            }).collect();
            let original_required:Vec<_>=evaluation.state.reasons.iter().filter_map(|r|match r {
                CompletionReason::UnconsumedInput {event_id} if current_required.contains(&event_id)=>Some(event_id),_=>None,
            }).collect();
            let reset_required:Vec<_>=reset.into_iter().collect();
            let required=if reset.is_some() {&reset_required} else if original_required.is_empty() {&current_required}else{&original_required};
            if stored.task.native_retry_block.is_some()||!governance::completion_enforced(tx)?||super::super::operations::pending_effects(tx,&stored.task.id)?!=0
                ||super::super::collaboration_work::admission_blocked(tx,&stored.task.id)? {
                return refuse("governance_completion_blocked","completion continuation has a current hold or unknown effect");
            }
            let accepted=&stored.task.input.resolved_limits.as_ref().unwrap().global_metadata;
            if (if reset_valid {1}else{hold.state.consecutive_count})>governance::continuation_limit(tx,accepted)? {
                return refuse("governance_completion_blocked","current configured continuation allowance is exhausted");
            }
            let used=super::super::operations::used_model_requests(tx,&stored.task.id)?.saturating_add(u64::from(stored.delegated_model_requests));
            if used>=u64::from(stored.task.input.limits.model_requests) {return refuse("model_request_budget_exceeded","completion continuation preserves remaining accounting");}
            let supports=matches!(stored.task.input.definition.execution_profile,Some(crate::protocol::ExecutionProfile::CollaborationV2 {..}))&&accepted.workflow.is_some();
            if supports {
                let (data,changed)=collaboration_notifications::apply(tx,&Operation::ObserveCollaborationDelivery {caller:caller.clone(),limit:1},cfg,now)?;
                let Data::CollaborationDelivery(delivery)=data else {return Err(Fault::Store("workflow observation kind differs".into()))};
                if changed || delivery.pending.as_ref().is_some_and(|b|b.delivery.is_some()) {
                    return refuse("delivery_pending","original workflow input remains unknown");
                }
                let selected=delivery.pending.as_ref().filter(|b|input.delivery_batch_id.as_ref()==Some(&b.batch_id)
                    && b.prepared_by==*caller && b.page.has_input());
                if input.delivery_batch_id.is_some() && selected.is_none() {
                    return refuse("delivery_pending","completion input does not bind its exact prepared workflow batch");
                }
                if !required.is_empty() && selected.is_none_or(|b|!required.iter().any(|event|b.page.contains_event(event)))
                    || reset.is_some_and(|event|selected.is_none_or(|b|!b.page.contains_event(event))) {
                    return refuse("delivery_pending","completion input omits its exact required workflow occurrence");
                }
            } else if !required.is_empty()||reset.is_some()||input.delivery_batch_id.is_some()||collaboration_notifications::delivery_blocks_admission(tx,&stored.task)? {
                return refuse("delivery_pending","completion has no accepted workflow batch capability");
            }
            let continuation=CompletionContinuation {begin_operation_id:input.operation_id.clone(),caller:caller.clone(),decision:input.decision.clone(),context_binding:input.context_binding.clone(),target:input.target.clone(),delivery_batch_id:input.delivery_batch_id.clone(),acknowledged:None};
            stored.completion_hold.as_mut().unwrap().continuation=Some(continuation.clone());
            database::save(tx,&mut stored,"completion_input_begin",&input.operation_id)?;
            (CompletionOutcome::Begun(CompletionFeedback {evaluation,continuation}),true)
        },
        Operation::AcknowledgeCompletionInput(input)=>{
            identity(tx,&input.operation_id,acknowledgement_id(caller,&input.begin_operation_id)?)?;
            let hold=stored.completion_hold.as_ref().ok_or_else(||Fault::Refused("not_found","completion hold absent".into()))?;
            validate_hold(tx,&stored.task,hold)?;
            let mut continuation=hold.continuation.clone().ok_or_else(||Fault::Refused("not_found","completion input Begin absent".into()))?;
            if continuation.caller!=*caller||continuation.begin_operation_id!=input.begin_operation_id||continuation.acknowledged.is_some()
                ||!acknowledged_target(&continuation.target,&input.consumed_at) {
                return refuse("delivery_pending","completion ACK differs from the original input and supported response");
            }
            if let CompletionBoundary::NativeResponse {..}=&input.consumed_at {original_boundary(tx,&stored,&input.consumed_at,&[])?;}
            if let CompletionBoundary::CodexTurn {thread_id,turn_id}=&input.consumed_at {
                if !database::valid_id(tx.limits,thread_id)||!database::valid_id(tx.limits,turn_id) {return refuse("invalid_input","invalid actual Codex input-consumption boundary");}
            }
            let acknowledgement=CompletionInputAck {operation_id:input.operation_id.clone(),consumed_at:input.consumed_at.clone(),delivery_ack_operation_id:input.delivery_ack_operation_id.clone(),native_input:input.native_input.clone()};
            validate_delivery_ack(tx,&continuation,&acknowledgement)?;
            validate_native_ack(tx,&stored.task,&continuation,&acknowledgement)?;
            continuation.acknowledged=Some(acknowledgement);
            stored.completion_hold.as_mut().unwrap().continuation=Some(continuation.clone());
            database::save(tx,&mut stored,"completion_input_ack",&input.operation_id)?;
            (CompletionOutcome::InputAcknowledged(continuation),true)
        },
        _=>unreachable!(),
    };
    let after=(cfg.clock)();
    if after<now {return refuse("clock_unavailable","clock moved backwards during completion transition");}
    collaboration_calls::caller(tx,caller,after)?;
    Ok((Data::Completion(Box::new(result.0)),result.1))
}

/// Root invokes this at the existing delivery dispatch. A governance Begin may
/// share its one exact input with one preselected C batch, never another append.
pub(in crate::controller) fn before_delivery(tx:&StoreContext<'_>,op:&Operation)->Result<()> {
    let caller=match op {
        Operation::ObserveCollaborationDelivery {..}=>return Ok(()),
        Operation::PrepareCollaborationDelivery {caller,..}|Operation::BeginCollaborationDelivery {caller,..}|Operation::AcknowledgeCollaborationDelivery {caller,..}=>caller,
        _=>return Err(Fault::Store("wrong delivery completion fence operation".into())),
    };
    let task=load(tx,&caller.task_id)?;
    let Some(hold)=&task.completion_hold else {return Ok(())};
    validate_hold(tx,&task.task,hold)?;
    let Some(value)=hold.continuation.as_ref().filter(|v|v.acknowledged.is_none()) else {return Ok(())};
    match op {
        Operation::BeginCollaborationDelivery {caller,batch_id,context_binding,..}|
        Operation::AcknowledgeCollaborationDelivery {caller,batch_id,context_binding,..}
            if caller==&value.caller&&value.delivery_batch_id.as_ref()==Some(batch_id)&&context_binding==&value.context_binding=>Ok(()),
        _=>refuse("delivery_pending","original completion input intent permits only its exact paired append"),
    }
}

/// Leaf validator for the root's one ordered native request coverage merge.
/// The leaf covers the original rejected no-tool response, not the replacement
/// response or a fabricated collaboration call. All grants remain original.
pub(in crate::controller) fn validate_native_request(tx:&StoreContext<'_>,task:&Task,
    evidence:&NativeCompletionContinuationEvidence)->Result<()> {
    let attempt=task.attempt.as_ref().ok_or_else(||Fault::Store("native completion attempt absent".into()))?;
    let caller=Caller {task_id:task.id.clone(),attempt_id:attempt.id.clone(),generation:attempt.generation,
        worker_id:attempt.worker_id.clone(),incarnation:attempt.worker_incarnation.clone()};
    let (original,evaluation)=recorded_evaluation(tx,&evidence.decision)?;
    if original.caller!=caller || evaluation.boundary!=(CompletionBoundary::NativeResponse {request_index:evidence.request_index})
        ||!matches!(evaluation.state.disposition,CompletionDisposition::Blocked {continuation_eligible:true,exhausted:false}) {
        return refuse("source_mismatch","native continuation evidence differs from original eligible rejection");
    }
    let begin=governance::original_receipt(tx,&evidence.begin_operation_id)?.ok_or_else(||Fault::Refused("source_mismatch","native continuation Begin absent".into()))?;
    let Outcome::Ok {data:Data::Completion(feedback)}=&begin.outcome else {
        return refuse("source_mismatch","native continuation Begin was not accepted");
    };
    let CompletionOutcome::Begun(feedback)=feedback.as_ref() else {
        return refuse("source_mismatch","native continuation Begin was not accepted");
    };
    let ack=governance::original_receipt(tx,&evidence.acknowledgement_operation_id)?.ok_or_else(||Fault::Refused("source_mismatch","native continuation ACK absent".into()))?;
    let Outcome::Ok {data:Data::Completion(continuation)}=&ack.outcome else {
        return refuse("source_mismatch","native continuation input has no actual ACK");
    };
    let CompletionOutcome::InputAcknowledged(continuation)=continuation.as_ref() else {
        return refuse("source_mismatch","native continuation input has no actual ACK");
    };
    if feedback.evaluation!=evaluation || feedback.continuation.begin_operation_id!=evidence.begin_operation_id
        ||continuation.caller!=caller||continuation.begin_operation_id!=evidence.begin_operation_id
        ||continuation.acknowledged.as_ref().is_none_or(|a|a.operation_id!=evidence.acknowledgement_operation_id) {
        return refuse("source_mismatch","native continuation leaf contains another original input");
    }
    let record=governance::read_decision(tx,&evidence.decision)?;
    let hold=CompletionHold {decision:evidence.decision.clone(),state:evaluation.state,
        observed_product_revision:record.observed_product_revision.ok_or_else(||Fault::Store("completion watermark absent".into()))?,
        continuation:Some(continuation.clone())};
    validate_continuation(tx,task,&hold)?;
    let (charged,gap_revision)=super::super::operations::validate_completion_gap(tx,&caller,evidence.request_index,&original.intervening_requests)?;
    let next=charged.checked_add(1).ok_or_else(||Fault::Refused("source_mismatch","native continuation index overflows".into()))?;
    let reservation=|index:u32|->Result<u64>{
        let id=crate::native_loop_contract::reservation_id(&attempt.id,index);
        let receipt=governance::original_receipt(tx,&id)?.ok_or_else(||Fault::Refused("source_mismatch","native continuation reservation absent".into()))?;
        if !matches!(receipt.request,Operation::ObserveWorker {ref report} if report.report_id==id
            &&report.task_id==caller.task_id&&report.attempt_id==caller.attempt_id&&report.generation==caller.generation
            &&report.worker_id==caller.worker_id&&report.incarnation==caller.incarnation
            &&matches!(report.observation,Observation::Usage {model_requests,..} if model_requests==index))
            ||!matches!(receipt.outcome,Outcome::Ok {data:Data::Task(ref t)} if t.id==task.id
                &&t.attempt.as_ref().is_some_and(|a|a.id==attempt.id&&!a.stopped&&!a.uncertain&&a.phase==AttemptPhase::Running)) {
            return refuse("source_mismatch","native continuation reservation owner/body differs");
        }
        Ok(receipt.commit.controller_revision)
    };
    let first=reservation(evidence.request_index)?;let second=reservation(next)?;
    if !(first<=gap_revision&&gap_revision<record.controller_revision&&record.controller_revision<begin.commit.controller_revision
        &&begin.commit.controller_revision<second&&second<ack.commit.controller_revision) {
        return refuse("source_mismatch","native continuation does not preserve original request/input ordering");
    }
    Ok(())
}

/// Root's unified native coverage calls this when the final ordinary response
/// precedes charged compaction/rejection work. The proposed index alone proves
/// nothing: read the exact original boundary and its actual disposition.
pub(in crate::controller) fn validate_native_final(tx:&StoreContext<'_>,task:&Task,final_response_index:u32,total:u32)->Result<()> {
    let a=task.attempt.as_ref().ok_or_else(||Fault::Store("native final attempt absent".into()))?;
    let caller=Caller {task_id:task.id.clone(),attempt_id:a.id.clone(),generation:a.generation,worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone()};
    let boundary=CompletionBoundary::NativeResponse {request_index:final_response_index};
    let id=evaluation_id(&caller,&boundary)?;
    let original=governance::original_receipt(tx,&id)?.ok_or_else(||Fault::Refused("source_mismatch","native final completion boundary receipt absent".into()))?;
    let Operation::EvaluateCompletion(ref input)=original.request else {return refuse("source_mismatch","native final receipt is not original EvaluateCompletion")};
    if input.operation_id!=id||input.caller!=caller||input.boundary!=boundary {
        return refuse("source_mismatch","native final boundary differs from original caller");
    }
    let (charged,revision)=super::super::operations::validate_completion_gap(tx,&caller,final_response_index,&input.intervening_requests)?;
    if charged!=total||revision>=original.commit.controller_revision {return refuse("source_mismatch","native final gap accounting or original ordering differs")}
    match original.outcome {
        Outcome::Ok {data:Data::Completion(completion)}=>match *completion {
            CompletionOutcome::NotEnforced=>Ok(()),
            CompletionOutcome::Evaluated(ref evaluation)
                if matches!(evaluation.state.disposition,CompletionDisposition::Admit)=>{
                let (_,validated)=recorded_evaluation(tx,&evaluation.decision)?;
                if validated!=*evaluation||evaluation.boundary!=boundary||evaluation.decision.operation_id!=id {
                    return refuse("source_mismatch","native final admission does not bind original evidence");
                }
                Ok(())
            },
            _=>refuse("source_mismatch","native final boundary has no original admission"),
        },
        _=>refuse("source_mismatch","native final boundary has no original admission"),
    }
}
