//! The sole controller transaction releases terminal duplicate read bodies.
//! The original receipt row, intent, revision and integrity evidence survive.
use super::{database::*, metadata, ControllerConfig};
use crate::{protocol::*, read_retention::*};
use rusqlite::{params, OptionalExtension};

pub(super) fn apply(tx:&StoreContext<'_>, request:&TerminalReadRelease, cfg:&ControllerConfig, now:u64)->Result<(Data,bool)> {
    apply_checked(tx,request,cfg,now,false)
}
pub(super) fn observe_custody(tx:&StoreContext<'_>, report:&Report, proof:&TerminalReadRelease, cfg:&ControllerConfig, now:u64)->Result<(Data,bool)> {
    if proof.custody_operation_id!=report.report_id || proof.operation_id!=report.report_id || proof.first_safe_operation_id.is_some()
        || proof.caller.task_id!=report.task_id || proof.caller.attempt_id!=report.attempt_id
        || proof.caller.generation!=report.generation || proof.caller.worker_id!=report.worker_id || proof.caller.incarnation!=report.incarnation {
        return refuse("read_release_invalid","custody report differs from exact original worker");
    }
    apply_checked(tx,proof,cfg,now,true)?;
    Ok((Data::Task(Box::new(load(tx,&report.task_id)?.task)),false))
}
fn apply_checked(tx:&StoreContext<'_>, request:&TerminalReadRelease, _cfg:&ControllerConfig, now:u64, observing_custody:bool)->Result<(Data,bool)> {
    request.caller.validate(&tx.limits.transport).map_err(|e|Fault::Refused("invalid_input",e))?;
    let selected=tx.limits.retention.as_ref().ok_or_else(||Fault::Refused("read_release_held","worker retention is not configured".into()))?;
    if request.policy!=selected.worker || request.policy_revision!=tx.limits.revision
        || request.reads.is_empty() || request.reads.len()>request.policy.batch_entries
        || bounded_json_size(request,request.policy.record_bytes).is_err() {
        return refuse("read_release_bound","terminal release exceeds retention.worker policy");
    }
    let stored=load(tx,&request.caller.task_id)?;
    let task=&stored.task;
    let a=task.attempt.as_ref().ok_or_else(||Fault::Refused("read_release_held","original attempt missing".into()))?;
    let grace=match task.status {TaskStatus::Succeeded=>request.policy.success_grace_ms,TaskStatus::Failed=>request.policy.failed_grace_ms,TaskStatus::Cancelled=>request.policy.cancelled_grace_ms,_=>return refuse("read_release_held","task is active or resumable")};
    if a.id!=request.caller.attempt_id || a.task_id!=request.caller.task_id || a.session_id!=request.session_id
        || a.generation!=request.caller.generation || a.worker_id!=request.caller.worker_id || a.worker_incarnation!=request.caller.incarnation
        || task.session_id!=request.session_id || task.revision!=request.terminal_revision
        || !a.stopped || a.phase!=AttemptPhase::Stopped || a.uncertain || stored.unresolved_execution
        || stored.completion_hold.is_some() || task.pending_assignment.is_some() || task.waiting_child_id.is_some()
        || super::operations::pending_effects(tx,&task.id)?>0 {
        return refuse("read_release_held","terminal attempt closure is incomplete");
    }
    let command:Option<(String,i64)>=tx.query_row("SELECT body,acknowledged FROM commands WHERE id=?1",[&request.command_id],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let Some((body,1))=command else{return refuse("read_release_held","original run command is not acknowledged")};
    let command:WorkerCommand=decode(&body)?;
    if !matches!(command,WorkerCommand::RunAttempt {attempt:ref original,..} if original.id==a.id && original.session_id==a.session_id
        && original.task_id==a.task_id && original.worker_id==a.worker_id && original.worker_incarnation==a.worker_incarnation && original.generation==a.generation) {
        return refuse("read_release_held","original run command binding differs");
    }
    let pending:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM commands WHERE attempt_id=?1 AND acknowledged=0)",[&a.id],|r|r.get(0))?;
    if pending || !super::charters::handoff_closed(tx,&request.session_id)?
        || !super::collaboration_notifications::terminal_read_closed(tx,task)? {
        return refuse("read_release_held","delivery, command or charter obligations remain");
    }
    if !request.custody.valid() || !matches!((&task.input.definition.executor,&request.custody),
        (ExecutorKind::Coding,TranscriptCustody::CodingCopy {..}) | (ExecutorKind::Native,TranscriptCustody::NativeJournal {..})) {
        return refuse("read_release_held","required transcript custody proof is missing or mismatched");
    }
    if !observing_custody {
        let original=metadata::read(tx,&request.custody_operation_id)?.ok_or_else(||Fault::Refused("read_release_held","verified custody report missing".into()))?;
        let (Operation::ObserveWorker {report},Outcome::Ok {data:Data::Task(recorded)})=(original.request,original.outcome) else {
            return refuse("read_release_held","custody is not an original successful worker report");
        };
        let Observation::ReadTranscriptCustody {proof}=report.observation else {return refuse("read_release_held","custody report kind differs")};
        let mut expected=request.clone(); expected.operation_id=request.custody_operation_id.clone(); expected.first_safe_operation_id=None;
        if *proof!=expected || recorded.as_ref()!=task || report.report_id!=request.custody_operation_id
            || report.task_id!=task.id || report.attempt_id!=a.id || report.generation!=a.generation
            || report.worker_id!=a.worker_id || report.incarnation!=a.worker_incarnation {
            return refuse("read_release_held","original custody, worker, policy or closure differs");
        }
    }
    // Only this controller's earlier committed observation establishes grace.
    // The worker cannot backdate first-safe or substitute a newer policy/custody.
    let (first_safe_operation_id, first_safe_ms, eligible_at_ms) = match &request.first_safe_operation_id {
        None => (request.operation_id.clone(), now, now.checked_add(grace).ok_or_else(||Fault::Store("read grace overflow".into()))?),
        Some(id) => {
            let prior=metadata::read(tx,id)?.ok_or_else(||Fault::Refused("read_release_held","first-safe original receipt missing".into()))?;
            let (Operation::ReleaseTerminalReads(original), Outcome::Ok {data:Data::TerminalReadRelease(observed)})=(prior.request,prior.outcome) else {
                return refuse("read_release_held","first-safe receipt has another operation or outcome");
            };
            let mut expected=request.clone(); expected.operation_id=id.clone(); expected.first_safe_operation_id=None;
            if original!=expected || observed.first_safe_operation_id!=*id || !observed.released.is_empty()
                || observed.body_bytes_before!=0 || observed.body_bytes_after!=0
                || observed.attempt_id!=a.id || observed.first_safe_ms.checked_add(grace)!=Some(observed.eligible_at_ms)
                || observed.eligible_at_ms>now {
                return refuse("read_release_held","first-safe observation, original custody or due time differs");
            }
            (id.clone(),observed.first_safe_ms,observed.eligible_at_ms)
        }
    };
    let mut seen=std::collections::BTreeSet::new();
    let mut replacements=Vec::new();
    let mut before=0usize;
    let mut after=0usize;
    // Validate the entire batch before the first replacement. The enclosing
    // transaction/savepoint also rolls back replacements on capacity or COMMIT failure.
    for item in &request.reads {
        if !seen.insert(&item.operation_id) || item.operation_id==request.operation_id {return refuse("read_release_invalid","duplicate release selection")}
        let original=metadata::read_retained(tx,&item.operation_id)?.ok_or_else(||Fault::Refused("read_release_held","original read receipt missing".into()))?;
        let Operation::CallCollaboration {call}=&original.request else {return refuse("read_release_invalid","selected receipt is not a canonical read")};
        if call.caller!=request.caller || original.commit!=item.commit {return refuse("read_release_invalid","original read identity differs")}
        let metadata::RetainedOutcome::Inline(outcome)=original.outcome else {return refuse("read_release_held","read already released by a different original batch")};
        let result=crate::mcp::result(call,Reply {request_id:String::new(),commit:Some(item.commit.clone()),observed_controller_revision:Some(metadata::current(tx)?),outcome:outcome.clone()}).map_err(Fault::Store)?;
        if result.requires_quiescence {return refuse("read_release_invalid","quiescing result cannot be released")}
        item.evidence.verify(call,&outcome).map_err(|e|Fault::Refused("read_release_invalid",e))?;
        let compact=CompactOutcome {committed_read_reference:CompactRead {evidence:item.evidence.clone(),release_operation_id:request.operation_id.clone(),attempt_id:a.id.clone(),session_id:a.session_id.clone()}};
        let old=encode(&outcome)?;
        let new=encode(&compact)?;
        // Small bodies are cheaper to retain. This never affects refusals/writes.
        if new.len()>=old.len() {continue}
        before=before.checked_add(old.len()).ok_or_else(||Fault::Store("read bytes overflow".into()))?;
        after=after.checked_add(new.len()).ok_or_else(||Fault::Store("read bytes overflow".into()))?;
        replacements.push((item.operation_id.clone(),old,new));
    }
    if request.first_safe_operation_id.is_none() {
        return Ok((Data::TerminalReadRelease(ReleaseResult {attempt_id:a.id.clone(),released:vec![],body_bytes_before:0,body_bytes_after:0,
            first_safe_operation_id,first_safe_ms,eligible_at_ms}),false));
    }
    let result=ReleaseResult {attempt_id:a.id.clone(),released:replacements.iter().map(|v|v.0.clone()).collect(),body_bytes_before:before,body_bytes_after:after,
        first_safe_operation_id,first_safe_ms,eligible_at_ms};
    bounded_json_size(&result,request.policy.record_bytes).map_err(|_|Fault::Refused("read_release_bound","retention.worker.record_bytes".into()))?;
    for (id,old,new) in replacements {
        if tx.execute("UPDATE receipts SET outcome=?1 WHERE id=?2 AND outcome=?3",params![new,id,old])?!=1 {return Err(Fault::Store("original read changed during release".into()))}
    }
    Ok((Data::TerminalReadRelease(result),true))
}

pub(super) fn validate_origin(tx:&StoreContext<'_>,id:&str,operation:&Operation,reference:&CompactOutcome,revision:u64)->Result<()> {
    let v=&reference.committed_read_reference;
    let raw:Option<(String,String,i64)>=tx.query_row("SELECT request,outcome,controller_revision FROM receipts WHERE id=?1",[&v.release_operation_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    let Some((request,outcome,release_revision))=raw else{return Err(Fault::Store("compact read original release receipt missing".into()))};
    if request.len()>tx.limits.transport.body_bytes || outcome.len()>tx.limits.transport.body_bytes {return Err(Fault::Store("read release receipt exceeds bound".into()))}
    let Operation::ReleaseTerminalReads(release)=decode(&request)? else{return Err(Fault::Store("compact provenance is not a terminal release".into()))};
    let Outcome::Ok {data:Data::TerminalReadRelease(result)}=decode(&outcome)? else{return Err(Fault::Store("compact provenance lacks successful release".into()))};
    let Operation::CallCollaboration {call}=operation else {return Err(Fault::Store("compact operation differs".into()))};
    if release.operation_id!=v.release_operation_id || release.caller!=call.caller || release.session_id!=v.session_id
        || release_revision<=0 || release_revision as u64<=revision || release_revision as u64>metadata::current(tx)?
        || !release.custody.valid() || result.attempt_id!=v.attempt_id || !result.released.iter().any(|s|s==id)
        || !release.reads.iter().any(|item|item.operation_id==id && item.commit.operation_id==id && item.commit.controller_revision==revision && item.evidence==v.evidence) {
        return Err(Fault::Store("compact read release provenance differs".into()));
    }
    let first_id=release.first_safe_operation_id.as_ref().ok_or_else(||Fault::Store("compact receipt lacks first-safe observation".into()))?;
    let first=metadata::read(tx,first_id)?.ok_or_else(||Fault::Store("compact first-safe receipt missing".into()))?;
    let (Operation::ReleaseTerminalReads(first_request),Outcome::Ok {data:Data::TerminalReadRelease(first_result)})=(first.request,first.outcome) else {return Err(Fault::Store("compact first-safe provenance differs".into()))};
    let mut expected=release.clone();expected.operation_id=first_id.clone();expected.first_safe_operation_id=None;
    if first_request!=expected || first.commit.controller_revision>=release_revision as u64
        || first_result.first_safe_operation_id!=*first_id || first_result.first_safe_ms!=result.first_safe_ms
        || first_result.eligible_at_ms!=result.eligible_at_ms || !first_result.released.is_empty()
        || first_result.attempt_id!=result.attempt_id || first_result.body_bytes_before!=0 || first_result.body_bytes_after!=0
        || result.first_safe_operation_id!=*first_id {
        return Err(Fault::Store("compact first-safe/release order or binding differs".into()));
    }
    let custody=metadata::read(tx,&release.custody_operation_id)?.ok_or_else(||Fault::Store("compact custody receipt missing".into()))?;
    let (Operation::ObserveWorker{report},Outcome::Ok{data:Data::Task(task)})=(custody.request,custody.outcome) else {return Err(Fault::Store("compact custody outcome differs".into()))};
    let Observation::ReadTranscriptCustody{proof}=report.observation else {return Err(Fault::Store("compact custody kind differs".into()))};
    expected.operation_id=release.custody_operation_id.clone();
    if *proof!=expected || custody.commit.controller_revision>=first.commit.controller_revision
        || report.report_id!=release.custody_operation_id || report.task_id!=release.caller.task_id
        || report.attempt_id!=release.caller.attempt_id || report.generation!=release.caller.generation
        || report.worker_id!=release.caller.worker_id || report.incarnation!=release.caller.incarnation
        || task.id!=release.caller.task_id || task.session_id!=release.session_id || task.revision!=release.terminal_revision
        || task.pending_assignment.is_some() || task.waiting_child_id.is_some()
        || task.attempt.as_ref().is_none_or(|a|a.id!=release.caller.attempt_id || a.task_id!=task.id
            || a.session_id!=release.session_id || a.generation!=release.caller.generation || a.worker_id!=release.caller.worker_id
            || a.worker_incarnation!=release.caller.incarnation || a.phase!=AttemptPhase::Stopped || !a.stopped || a.uncertain) {
        return Err(Fault::Store("compact custody causal binding differs".into()));
    }
    let grace=match task.status {
        TaskStatus::Succeeded=>release.policy.success_grace_ms,
        TaskStatus::Failed=>release.policy.failed_grace_ms,
        TaskStatus::Cancelled=>release.policy.cancelled_grace_ms,
        _=>return Err(Fault::Store("compact custody was not terminal".into())),
    };
    if result.first_safe_ms.checked_add(grace)!=Some(result.eligible_at_ms)
        || !matches!((&task.input.definition.executor,&release.custody),
            (ExecutorKind::Coding,TranscriptCustody::CodingCopy{..}) | (ExecutorKind::Native,TranscriptCustody::NativeJournal{..})) {
        return Err(Fault::Store("compact custody executor or grace differs".into()));
    }
    Ok(())
}

pub(super) fn admit(tx:&StoreContext<'_>,version:i64)->Result<()> {
    if super::application_schema::admits_read_generation(version){return Ok(())}
    // All retained carriers are already bounded by their ordinary readers. A
    // new release request/refusal is vocabulary even before it removes bytes.
    let found:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM receipts WHERE (json_extract(request,'$.operation')='release_terminal_reads' OR json_extract(request,'$.input.report.observation.kind')='read_transcript_custody') OR json_type(outcome,'$.committed_read_reference') IS NOT NULL OR json_extract(outcome,'$.data.kind') IN ('original_read_body_unavailable','terminal_read_release'))",[],|r|r.get(0))?;
    if found {return Err(Fault::Store("read retention vocabulary requires schema26".into()))}
    Ok(())
}
