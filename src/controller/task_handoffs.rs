//! Same-task reassignment is a durable intent followed by physical/effect closure.
//! The original and every successor's resolved inputs remain immutable session data.
use super::{ControllerConfig, database::*, database::StoreContext as Connection, validation::Context};
use crate::protocol::*;
use rusqlite::{OptionalExtension,params};
use serde::{Deserialize,Serialize};

#[derive(Debug,Clone,Serialize,Deserialize,PartialEq)]
#[serde(deny_unknown_fields)]
struct Intent {
    operation_id:String,
    task_id:String,
    from_session_id:String,
    expected_revision:u64,
    session_sequence:u64,
    session:Session,
    input:TaskInput,
}
fn enabled(tx:&Connection<'_>)->Result<bool>{
    let version:i64=tx.query_row("PRAGMA user_version",[],|r|r.get(0))?;
    Ok(version>=super::application_schema::DEFAULT_ASSIGNMENT_VERSION)
}
pub(super) fn pending(tx:&Connection<'_>,id:&str)->Result<bool>{
    if !enabled(tx)?{return Ok(false)}
    Ok(tx.query_row("SELECT EXISTS(SELECT 1 FROM task_assignments WHERE task_id=?1 AND state='pending')",[id],|r|r.get(0))?)
}
pub(super) fn cancel_pending(tx:&Connection<'_>,id:&str)->Result<()> {
    if enabled(tx)?{tx.execute("UPDATE task_assignments SET state='cancelled' WHERE task_id=?1 AND state='pending'",[id])?;}
    Ok(())
}
fn read(tx:&Connection<'_>,id:&str)->Result<Option<Intent>>{
    let raw:Option<String>=tx.query_row("SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM task_assignments WHERE operation_id=?1",params![id,sql_bound(tx.limits.transport.body_bytes.saturating_add(1))?],|r|r.get(0)).optional()?;
    raw.map(|raw|{
        if raw.len()>tx.limits.transport.body_bytes{return Err(Fault::Store("handoff intent exceeds bound".into()))}
        let value:Intent=decode(&raw)?;
        if value.operation_id!=id{return Err(Fault::Store("handoff intent identity differs".into()))}
        Ok(value)
    }).transpose()
}
/// Construct only a compatible execution projection. Task-wide instruction,
/// completion schema, prerequisites and cumulative budget never become a child.
pub(super) fn successor(tx:&Connection<'_>,old:&TaskInput,target:&Definition,cfg:&ControllerConfig,now:u64)->Result<TaskInput>{
    let mut next=old.clone();
    if old.definition.executor!=target.executor {
        let template=crate::delegation::handoff_template(old).ok_or_else(||Fault::Refused("handoff_not_configured","cross-executor assignment requires explicit accepted handoff configuration".into()))?;
        if old.definition.executor!=ExecutorKind::Native || target.executor!=ExecutorKind::Coding || template.definition!=*target {
            return refuse("handoff_configuration_mismatch","target differs from the accepted same-task handoff definition");
        }
        // These are accepted writable operands, never promoted read-only scope.
        next.repository=Some(template.repository.clone());
        next.repository_read=None;
        next.instruction_source=template.instruction_source.clone();
        next.pull_request=template.pull_request.clone();
        for command in &template.acceptance_commands {
            if !next.acceptance_commands.contains(command){next.acceptance_commands.push(command.clone());}
        }
        next.native_workflow=None;
    }
    next.definition=target.clone();
    let policy=old.resolved_limits.as_ref().ok_or_else(||Fault::Store("handoff original policy absent".into()))?.global_metadata.as_ref();
    next.resolved_limits=Some(policy.resolve_task(&next).map_err(|e|Fault::Refused("invalid_limits",e))?);
    let view=tx.with_limits(policy);
    super::repositories::resolve_input(&view,&mut next,true)?;
    super::operations::validate(&next,cfg,now)?;
    super::charters::validate_definition(&view,target)?;
    if next.instruction!=old.instruction || next.dependencies!=old.dependencies || next.output_schema!=old.output_schema || next.limits!=old.limits {
        return Err(Fault::Store("handoff changed original task obligations".into()));
    }
    Ok(next)
}

//= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
//# An accepted reassignment to a different agent MUST create a distinct session for the same task identity.
//= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
//# A reassigned session MUST bind the selected target agent's accepted definition revision.
pub(super) fn request(tx:&Connection<'_>,operation_id:&str,task_id:&str,expected_revision:u64,agent_id:&str,cfg:&ControllerConfig,now:u64,validation:&Context<'_>)->Result<(Data,bool)>{
    let mut task=load(tx,task_id)?;
    super::operations::revision(&task,expected_revision)?;
    if agent_id==task.task.assigned_agent_id {return Ok((Data::Task(Box::new(task.task)),false))}
    if terminal(task.task.status){return refuse("invalid_transition","accepted terminal tasks cannot be reassigned")}
    if pending(tx,task_id)?{return refuse("handoff_pending","task already has an accepted reassignment")}
    if task.task.waiting_child_id.is_some() || task.task.completed_child.is_some() || !super::operations::descendants_settled(tx,task_id,cfg.max_tasks)? {
        return refuse("reconciliation_required","delegated task obligations must settle before reassignment");
    }
    if task.task.native_retry_block.is_some(){return refuse("reconciliation_required","native correction must resolve before reassignment")}
    if task.completion_hold.is_some(){return refuse("reconciliation_required","existing completion hold must resolve before reassignment")}
    if super::collaboration_work::handoff_blocked(tx,&task.task)? || super::collaboration_notifications::delivery_blocks_admission(tx,&task.task)? {return refuse("reconciliation_required","existing workflow obligations must settle before reassignment")}
    let target=super::agents::selected_active_definition(tx,agent_id)?;
    let input=successor(tx,&task.task.input,&target,cfg,now)?;
    // A validation ticket reenters the same receipt/CAS transaction; no intent
    // or stop is committed if a concurrent head/task change invalidates it.
    if input.output_schema.is_some(){validation.require(&input,None,None,now)?;}
    let count:i64=tx.query_row("SELECT count(*) FROM sessions",[],|r|r.get(0))?;
    let reserved:i64=tx.query_row("SELECT count(*) FROM task_assignments WHERE state='pending'",[],|r|r.get(0))?;
    let bound=super::sessions::bound(cfg)? as u64;
    if unsigned(count)?.checked_add(unsigned(reserved)?).is_none_or(|n|n>=bound){return refuse("capacity","session retention bound is exhausted")}
    if task.cleanup_remaining==0 {
        if !cleanup_available(tx,cfg,1)?{return refuse("capacity","reassignment has no existing cleanup reserve")}
        task.cleanup_remaining=1;
    }
    let database_id:String=tx.query_row("SELECT value FROM settings WHERE name='database_id'",[],|r|r.get(0))?;
    let session_sequence=next(tx,"session")?;
    let session=Session{id:format!("session-{database_id}-{session_sequence:020}"),agent_id:target.id.clone(),definition:target.clone(),task_id:task_id.into()};
    let intent=Intent{operation_id:operation_id.into(),task_id:task_id.into(),from_session_id:task.task.session_id.clone(),expected_revision,session_sequence,session,input};
    let mut projected=load(tx,task_id)?;
    projected.task.input=intent.input.clone();
    projected.task.session_id=intent.session.id.clone();
    projected.task.assigned_agent_id=intent.session.agent_id.clone();
    projected.task.attempt=None;
    projected.task.pending_assignment=None;
    super::governance::validate_failure_envelope(tx,&projected)?;
    let body=encode(&intent)?;
    if body.len()>tx.limits.transport.body_bytes{return refuse("capacity","handoff intent exceeds transport.body_bytes")}
    tx.execute("INSERT INTO task_assignments(operation_id,task_id,from_session_id,state,body) VALUES(?1,?2,?3,'pending',?4)",params![operation_id,task_id,intent.from_session_id,body])?;
    task.task.pending_assignment=Some(PendingAssignment{operation_id:intent.operation_id.clone(),from_session_id:intent.from_session_id.clone(),session:intent.session.clone()});
    task.task.status=TaskStatus::Blocked;
    task.task.blocked_reason=Some("handoff_waiting_for_stop".into());
    stop(tx,&mut task)?;
    save(tx,&mut task,"assignment_requested",&serde_json::json!({"operation_id":operation_id,"from_session_id":intent.from_session_id,"target_agent_id":agent_id,"target_revision":target.revision}))?;
    activate(tx,&intent,cfg,now)?;
    Ok((Data::Task(Box::new(load(tx,task_id)?.task)),true))
}

//= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
//# A reassigned session MUST NOT execute until the prior execution is known unable to continue.
//= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
//# A reassigned session MUST NOT execute until relevant uncertain external effects are reconciled.
//= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
//# A task reassignment MUST preserve the task's accepted deadline.
//= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
//# A task reassignment MUST preserve its recorded usage accounting.
fn activate(tx:&Connection<'_>,intent:&Intent,cfg:&ControllerConfig,now:u64)->Result<bool>{
    let mut task=load(tx,&intent.task_id)?;
    if task.task.session_id!=intent.from_session_id {return Err(Fault::Store("pending handoff no longer owns original session".into()))}
    if task.task.status==TaskStatus::Cancelled || task.task.status==TaskStatus::Succeeded {
        cancel_pending(tx,&intent.task_id)?;
        task.task.pending_assignment=None;
        save(tx,&mut task,"assignment_cancelled",&intent.operation_id)?;
        return Ok(true)
    }
    let retained_policy=task.task.input.resolved_limits.as_ref().ok_or_else(||Fault::Store("handoff original metadata absent".into()))?.global_metadata.as_ref();
    let retained=tx.with_limits(retained_policy);
    let reason=if active(&task.task){Some("handoff_waiting_for_stop")}
        else if task.unresolved_execution || task.task.attempt.as_ref().is_some_and(|a|a.uncertain) || super::operations::pending_effects(tx,&intent.task_id)?>0 {Some("handoff_reconciliation_required")}
        else if !super::charters::handoff_closed(&retained,&intent.from_session_id)? {Some("handoff_original_context_unresolved")}
        else if task.task.native_retry_block.is_some() {Some("handoff_native_correction_required")}
        else if super::collaboration_work::handoff_blocked(tx,&task.task)? || super::collaboration_notifications::delivery_blocks_admission(tx,&task.task)? {Some("handoff_obligations_unsettled")}
        else if task.completion_hold.is_some() || task.task.waiting_child_id.is_some() || !super::operations::descendants_settled(tx,&intent.task_id,cfg.max_tasks)? {Some("handoff_obligations_unsettled")}
        else if task.task.input.limits.deadline_unix_ms<=now {Some("handoff_original_deadline_expired")}
        else if super::operations::used_model_requests(tx,&intent.task_id)?.saturating_add(u64::from(task.delegated_model_requests))>=u64::from(task.task.input.limits.model_requests){Some("handoff_original_budget_exhausted")}
        else {None};
    if let Some(reason)=reason {
        if task.task.status!=TaskStatus::Blocked || task.task.blocked_reason.as_deref()!=Some(reason){task.task.status=TaskStatus::Blocked;task.task.blocked_reason=Some(reason.into());save(tx,&mut task,"assignment_waiting",&reason)?;return Ok(true)}
        return Ok(false)
    }
    let session_before=super::sessions::get(tx,&intent.from_session_id)?;
    if super::sessions::input(tx,&session_before)?!=task.task.input {return Err(Fault::Store("handoff original input changed".into()))}
    // Retain a selected checkpoint only under the exact same accepted writer.
    if let Some(checkpoint)=&task.task.selected_checkpoint {
        let repository=intent.input.repository.as_ref().ok_or_else(||Fault::Store("handoff checkpoint has no retained repository".into()))?;
        if Some(&checkpoint.registration)!=repository.registration.as_ref() || checkpoint.repository!=repository.checkpoint_remote {return Err(Fault::Store("handoff checkpoint source changed".into()))}
    }
    let session=&intent.session;
    super::sessions::insert(tx,session,intent.session_sequence)?;
    super::sessions::bind_input(tx,&session,&intent.input,Some(&intent.from_session_id),Some(&intent.operation_id))?;
    task.task.pending_assignment=None;
    task.task.session_id=session.id.clone();
    task.task.assigned_agent_id=session.agent_id.clone();
    task.task.input=intent.input.clone();
    task.task.attempt=None; // All old attempts and their aggregate debit remain.
    task.task.status=TaskStatus::Queued;
    task.task.blocked_reason=None;
    task.executor_success=None;
    // Existing correction-required and completion holds are not erased by routing.
    let policy=intent.input.resolved_limits.as_ref().unwrap().global_metadata.as_ref();
    super::charters::initialize(&tx.with_limits(policy),&session.id)?;
    tx.execute("UPDATE task_assignments SET state='activated',to_session_id=?2 WHERE operation_id=?1 AND state='pending'",params![intent.operation_id,session.id])?;
    save(tx,&mut task,"assignment_activated",&serde_json::json!({"operation_id":intent.operation_id,"from_session_id":intent.from_session_id,"session_id":session.id,"agent_id":session.agent_id}))?;
    Ok(true)
}

/// Called by the existing owner only after a successful durable mutation. No
/// timer/poller, transport call or replacement receipt is introduced.
pub(super) fn progress(tx:&Connection<'_>,operation:&Operation,cfg:&ControllerConfig,now:u64)->Result<bool>{
    let task_id=match operation {
        Operation::ObserveWorker{report}=>Some(report.task_id.as_str()),
        Operation::SettleOperation{task_id,..}=>Some(task_id.as_str()),
        Operation::ObserveCharter{caller,..}=>Some(caller.task_id.as_str()),
        _=>None,
    };
    let Some(task_id)=task_id else{return Ok(false)};
    let id:Option<String>=tx.query_row("SELECT operation_id FROM task_assignments WHERE task_id=?1 AND state='pending'",[task_id],|r|r.get(0)).optional()?;
    let Some(id)=id else{return Ok(false)};
    activate(tx,&read(tx,&id)?.ok_or_else(||Fault::Store("pending handoff disappeared".into()))?,cfg,now)
}

//= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
//# A task reassignment MUST retain the task's prior session and execution-input history.
pub(super) fn validate_existing(tx:&Connection<'_>,cfg:&ControllerConfig,version:i64)->Result<()> {
    if version<super::application_schema::DEFAULT_ASSIGNMENT_VERSION{return Ok(())}
    let count:i64=tx.query_row("SELECT count(*) FROM task_assignments",[],|r|r.get(0))?;
    if unsigned(count)?>cfg.max_receipts as u64{return Err(Fault::Store("handoff inventory exceeds receipt bound".into()))}
    let orphan:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM session_inputs i LEFT JOIN sessions s ON s.id=i.session_id WHERE s.id IS NULL OR s.task_id!=i.task_id)",[],|r|r.get(0))?;
    if orphan{return Err(Fault::Store("orphan immutable session input".into()))}
    let mut reserved=std::collections::BTreeSet::new();
    let mut q=tx.prepare("SELECT operation_id,task_id,from_session_id,state,to_session_id FROM task_assignments ORDER BY operation_id")?;
    let mut rows=q.query([])?;
    while let Some(row)=rows.next()? {
        let id:String=row.get(0)?;let task_id:String=row.get(1)?;let from:String=row.get(2)?;let state:String=row.get(3)?;let to:Option<String>=row.get(4)?;
        let intent=read(tx,&id)?.ok_or_else(||Fault::Store("handoff missing".into()))?;
        if intent.task_id!=task_id||intent.from_session_id!=from||intent.expected_revision==0
            ||intent.session.task_id!=task_id||intent.session.agent_id!=intent.input.definition.id
            ||intent.session.definition!=intent.input.definition||intent.session_sequence==0
            ||!valid_id(tx.limits,&intent.session.id)||!reserved.insert(intent.session.id.clone()) {
            return Err(Fault::Store("handoff index/reserved identity differs".into()))
        }
        let database_id:String=tx.query_row("SELECT value FROM settings WHERE name='database_id'",[],|r|r.get(0))?;
        let counter:i64=tx.query_row("SELECT value FROM counters WHERE kind='session'",[],|r|r.get(0))?;
        if intent.session.id!=format!("session-{database_id}-{:020}",intent.session_sequence) || unsigned(counter)?<intent.session_sequence {return Err(Fault::Store("reserved session sequence differs".into()))}
        let previous=super::sessions::get(tx,&from)?;
        let old=super::sessions::input(tx,&previous)?;
        if previous.task_id!=task_id || old.instruction!=intent.input.instruction || old.dependencies!=intent.input.dependencies || old.output_schema!=intent.input.output_schema || old.limits!=intent.input.limits || old.resolved_limits.as_ref().map(|s|&s.global_metadata)!=intent.input.resolved_limits.as_ref().map(|s|&s.global_metadata){return Err(Fault::Store("handoff changed original obligations".into()))}
        intent.input.execution_limits().map_err(Fault::Store)?;
        let mut expected=old.clone();
        if old.definition.executor!=intent.input.definition.executor {
            let target=crate::delegation::handoff_template(&old).ok_or_else(||Fault::Store("handoff has no accepted execution operands".into()))?;
            if old.definition.executor!=ExecutorKind::Native||intent.input.definition.executor!=ExecutorKind::Coding||target.definition!=intent.input.definition {return Err(Fault::Store("handoff accepted target differs".into()))}
            expected.repository=Some(target.repository.clone());expected.repository_read=None;
            expected.instruction_source=target.instruction_source.clone();expected.pull_request=target.pull_request.clone();
            for check in &target.acceptance_commands {if !expected.acceptance_commands.contains(check){expected.acceptance_commands.push(check.clone())}}
            expected.native_workflow=None;
        }
        expected.definition=intent.input.definition.clone();
        expected.resolved_limits=Some(old.resolved_limits.as_ref().unwrap().global_metadata.resolve_task(&expected).map_err(Fault::Store)?);
        if expected!=intent.input || old.definition.id==intent.input.definition.id {return Err(Fault::Store("handoff immutable execution projection differs".into()))}
        let receipt=super::metadata::read(tx,&id)?.ok_or_else(||Fault::Store("handoff original receipt absent".into()))?;
        let matched=match &receipt.request {
            Operation::AssignTask{task_id,expected_revision,agent_id,..}=>task_id==&intent.task_id&&*expected_revision==intent.expected_revision&&agent_id==&intent.input.definition.id,
            Operation::CallCollaboration{call} if call.tool=="collab_task_assign"=>call.arguments.get("task_id").and_then(serde_json::Value::as_str)==Some(intent.task_id.as_str())&&call.arguments.get("expected_revision").and_then(serde_json::Value::as_u64)==Some(intent.expected_revision)&&call.arguments.get("agent_id").and_then(serde_json::Value::as_str)==Some(intent.input.definition.id.as_str()),
            _=>false,
        };
        let expected_pending=PendingAssignment{operation_id:id.clone(),from_session_id:from.clone(),session:intent.session.clone()};
        let recorded=match &receipt.outcome {Outcome::Ok{data:Data::Task(task)}=>task,_=>return Err(Fault::Store("handoff original admission differs".into()))};
        let accepted_target=recorded.pending_assignment.as_ref()==Some(&expected_pending)
            ||(recorded.session_id==intent.session.id&&recorded.pending_assignment.is_none());
        if !matched||recorded.id!=task_id||!accepted_target{return Err(Fault::Store("handoff original admission differs".into()))}
        match (state.as_str(),to) {
            ("activated",Some(to))=>{let next=super::sessions::get(tx,&to)?;let links:(Option<String>,Option<String>)=tx.query_row("SELECT previous_session_id,operation_id FROM session_inputs WHERE session_id=?1",[&to],|r|Ok((r.get(0)?,r.get(1)?)))?;if next!=intent.session||to!=intent.session.id||super::sessions::input(tx,&next)?!=intent.input||links!=(Some(from),Some(id)){return Err(Fault::Store("activated handoff lineage differs".into()))}}
            ("pending",None)=>{let task=load(tx,&task_id)?;if task.task.session_id!=from||task.task.status!=TaskStatus::Blocked||task.task.pending_assignment.as_ref()!=Some(&expected_pending){return Err(Fault::Store("pending handoff current owner differs".into()))}}
            ("cancelled",None)=>{},
            _=>return Err(Fault::Store("handoff state differs".into())),
        }
    }
    // Each task's current projection is the unique latest activated session.
    let wrong_current:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM tasks t WHERE t.session!=(SELECT s.id FROM sessions s WHERE s.task_id=t.id ORDER BY s.seq DESC LIMIT 1))",[],|r|r.get(0))?;
    if wrong_current{return Err(Fault::Store("current session is not the lineage leaf".into()))}
    let mut task_query=tx.prepare("SELECT id FROM tasks ORDER BY seq")?;
    let mut task_rows=task_query.query([])?;
    while let Some(row)=task_rows.next()? {
        let task=load(tx,&row.get::<_,String>(0)?)?;
        match (&task.task.pending_assignment,pending(tx,&task.task.id)?) {
            (Some(_),true)|(None,false)=>{},
            _=>return Err(Fault::Store("pending assignment projection has no matching intent".into())),
        }
    }
    let mut original_inputs=std::collections::BTreeMap::new();
    let mut q=tx.prepare("SELECT s.id,s.task_id,i.previous_session_id,i.operation_id FROM sessions s LEFT JOIN session_inputs i ON i.session_id=s.id ORDER BY s.seq")?;
    let mut rows=q.query([])?;
    while let Some(row)=rows.next()? {
        let id:String=row.get(0)?;let task:String=row.get(1)?;let previous:Option<String>=row.get(2)?;let operation:Option<String>=row.get(3)?;
        let session=super::sessions::get(tx,&id)?;let input=super::sessions::input(tx,&session)?;
        if let (Some(previous),Some(operation))=(previous.as_ref(),operation.as_ref()) {
            let linked:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM task_assignments WHERE operation_id=?1 AND task_id=?2 AND from_session_id=?3 AND to_session_id=?4 AND state='activated')",params![operation,task,previous,id],|r|r.get(0))?;
            let ordered:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM sessions p,sessions n WHERE p.id=?1 AND n.id=?2 AND p.task_id=n.task_id AND p.seq<n.seq)",params![previous,id],|r|r.get(0))?;
            if !linked||!ordered{return Err(Fault::Store("session lineage has no original activation".into()))}
        } else if previous.is_some()||operation.is_some(){return Err(Fault::Store("partial session lineage".into()))}
        else {let initial:i64=tx.query_row("SELECT count(*) FROM session_inputs WHERE task_id=?1 AND previous_session_id IS NULL",[&task],|r|r.get(0))?;if initial!=1{return Err(Fault::Store("original session input is ambiguous".into()))} original_inputs.insert(task.clone(),(id.clone(),input.clone()));}
        super::operations::require_definition(&input.definition)?;
    }
    let mut admissions=std::collections::BTreeSet::new();
    let receipt_bound=cfg.max_receipts.checked_add(cfg.max_cleanup_receipts).ok_or_else(||Fault::Store("receipt bound overflow".into()))?;
    let mut q=tx.prepare("SELECT id FROM receipts ORDER BY id LIMIT ?1")?;
    let ids=q.query_map([sql_bound(receipt_bound.saturating_add(1))?],|r|r.get::<_,String>(0))?.collect::<std::result::Result<Vec<_>,_>>()?;
    if ids.len()>receipt_bound{return Err(Fault::Store("original admission inventory exceeds bound".into()))}
    for id in ids {
        let Some(receipt)=super::metadata::scan_inline(tx,&id)? else {continue};
        let initial=matches!(&receipt.request,Operation::SubmitTask{..}|Operation::SubmitTaskDraft{..}|Operation::DelegateTask{..})
            ||matches!(&receipt.request,Operation::CallCollaboration{call} if matches!(call.tool.as_str(),"collab_task_delegate"|"collab_task_create"));
        if !initial {continue}
        if let Outcome::Ok{data:Data::Task(task)}=&receipt.outcome {
            let (session,input)=original_inputs.get(&task.id).ok_or_else(||Fault::Store("original accepted input absent".into()))?;
            let expected=if matches!(&receipt.request,Operation::CallCollaboration{..}){crate::pull_request_view::input(input)}else{input.clone()};
            if &task.session_id!=session||task.input!=expected||!admissions.insert(task.id.clone()){return Err(Fault::Store("original admission/session input differs".into()))}
        }
    }
    if admissions.len()!=original_inputs.len(){return Err(Fault::Store("original accepted input lacks its receipt".into()))}
    Ok(())
}

// Historical typed assignment operands remain readable, but new retry metadata
// cannot be admitted under the preceding controller format.
pub(super) fn admit_prior_supplied_retry(tx:&Connection<'_>,bound:usize)->Result<()> {
    let mut query=tx.prepare("SELECT operation_id FROM task_assignments ORDER BY operation_id")?;
    let mut rows=query.query([])?;let mut count=0usize;
    while let Some(row)=rows.next()? {
        count+=1;if count>bound{return Err(Fault::Store("supplied assignment inventory exceeds bound".into()))}
        let id:String=row.get(0)?;let intent=read(tx,&id)?.ok_or_else(||Fault::Store("supplied assignment disappeared".into()))?;
        if crate::supplied_outer::input_uses(&intent.input)||crate::supplied_outer::definition_uses(&intent.session.definition) {return Err(Fault::Store("supplied retry assignment requires schema25".into()))}
    }
    Ok(())
}

pub(super) fn admit_prior_identity_limit_fields(tx: &Connection<'_>, bound: usize) -> Result<()> {
    let mut query = tx.prepare("SELECT operation_id FROM task_assignments ORDER BY operation_id")?;
    let mut rows = query.query([])?;
    let mut count = 0usize;
    while let Some(row) = rows.next()? {
        count += 1;
        if count > bound { return Err(Fault::Store("assignment inventory exceeds bound".into())); }
        let id: String = row.get(0)?;
        let intent = read(tx, &id)?.ok_or_else(|| Fault::Store("assignment disappeared".into()))?;
        if crate::protocol::identity_limit_fields::input_uses(&intent.input) {
            return Err(Fault::Store("identity limit field vocabulary requires schema26".into()));
        }
    }
    Ok(())
}
