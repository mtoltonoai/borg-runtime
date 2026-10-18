//! Query-only integrity of a retained native hold, outside generic task loading.
//! No current-time retry decision or journal/state rewrite occurs at startup.
//! Deadline timing is enforced by the original expiry producer. Its retained
//! history has no clock field, so this reader proves occurrence, not elapsed time.
use super::{database::*, operations::native_retry as proof};
use crate::{mcp::Caller, native::{NativeRetryBlock, RetryBlockReason}, protocol::*};
use rusqlite::{params, OptionalExtension};

fn invalid() -> Fault { Fault::Store("retained native retry block provenance differs".into()) }

fn receipt(tx: &StoreContext<'_>, id: &str) -> Result<Option<super::metadata::Recorded>> {
    let sizes: Option<(i64,i64)> = tx.query_row(
        "SELECT length(CAST(request AS BLOB)),length(CAST(outcome AS BLOB)) FROM receipts WHERE id=?1",
        [id], |r| Ok((r.get(0)?,r.get(1)?))).optional()?;
    if let Some((request,outcome))=sizes {
        let cap=sql_bound(tx.limits.transport.body_bytes)?;
        if request<0 || outcome<0 || request>cap || outcome>cap { return Err(invalid()); }
    }
    super::metadata::read(tx,id)
}

fn same_attempt(task:&Task,caller:&Caller)->bool {
    task.id==caller.task_id && task.attempt.as_ref().is_some_and(|a|
        a.id==caller.attempt_id && a.generation==caller.generation
        && a.worker_id==caller.worker_id && a.worker_incarnation==caller.incarnation)
}

fn source(tx:&StoreContext<'_>, task:&Task, caller:&Caller, block:&NativeRetryBlock)->Result<u64> {
    let rejection=crate::native_loop_contract::rejection_id(&caller.attempt_id,block.request_index);
    let reservation=crate::native_loop_contract::reservation_id(&caller.attempt_id,block.request_index);
    let original=receipt(tx,&block.source_operation_id)?.ok_or_else(invalid)?;
    let reserved=receipt(tx,&reservation)?;
    let mut rejected=None;
    let mut coordination=false;
    if block.source_operation_id==rejection {
        if reserved.is_none() { return Err(invalid()); }
        rejected=Some(proof::confirmed_rejection(tx,caller,block.request_index)?.ok_or_else(invalid)?);
    } else if block.source_operation_id==crate::native_loop_contract::format_failure_id(&caller.attempt_id,block.request_index) {
        if reserved.is_none() || super::operations::native_format::confirmed(tx,caller,block.request_index)?.is_none()
            || !matches!(block.reason,RetryBlockReason::Deadline|RetryBlockReason::RequestAllowance) {return Err(invalid());}
    } else if block.source_operation_id==crate::native_loop_contract::output_cap_id(&caller.attempt_id,block.request_index) {
        if reserved.is_none() || super::operations::native_output_cap::confirmed(tx,caller,block.request_index)?.is_none()
            || !matches!(block.reason,RetryBlockReason::Deadline|RetryBlockReason::RequestAllowance) {return Err(invalid());}
    } else if block.source_operation_id==reservation {
        proof::reservation(tx,caller,block.request_index)?;
    } else {
        coordination=matches!((&original.request,&original.outcome),
            (Operation::AcquireNativeRequest {operation_id,caller:owner,request_index,..},
             Outcome::Ok {data:Data::NativeRequestAdmission(crate::native_retry::NativeRequestAdmission::Wait {..})})
             if operation_id==&block.source_operation_id && owner==caller && *request_index==block.request_index);
        if !coordination { return Err(invalid()); }
    }
    let retry=tx.limits.native_retry.as_ref();
    let valid=match block.reason {
        RetryBlockReason::RequestAllowance =>
            super::operations::used_model_requests(tx,&task.id)?.checked_add(u64::from(super::database::load(tx,&task.id)?.delegated_model_requests))
                .is_some_and(|used| used >= u64::from(task.input.limits.model_requests)),
        // Historical time/delay decisions are bound to their original successful
        // receipt or maintenance event below; today's wall clock is irrelevant.
        RetryBlockReason::Deadline => true,
        RetryBlockReason::RetryPolicyDisabled => rejected.is_some() && retry.is_none(),
        RetryBlockReason::InvalidRetryAfter => rejected.as_ref().is_some_and(|p|
            p.rejection.retry_after==crate::native::RetryAfter::Invalid),
        RetryBlockReason::RetryDelayUnavailable => rejected.is_some() && retry.is_some(),
        RetryBlockReason::CoordinationDelayUnavailable => coordination && retry.is_some(),
        RetryBlockReason::InvalidRequest => rejected.as_ref().is_some_and(|p|p.rejection.kind==crate::native::RejectionKind::InvalidRequest),
        RetryBlockReason::ContextLimit => rejected.as_ref().is_some_and(|p|p.rejection.kind==crate::native::RejectionKind::ContextLimit),
        RetryBlockReason::PolicyRefused => rejected.as_ref().is_some_and(|p|p.rejection.kind==crate::native::RejectionKind::PolicyRefused),
        RetryBlockReason::QuotaExhausted => rejected.as_ref().is_some_and(|p|p.rejection.kind==crate::native::RejectionKind::QuotaExhausted),
        RetryBlockReason::AuthenticationFailed => rejected.as_ref().is_some_and(|p|p.rejection.kind==crate::native::RejectionKind::AuthenticationFailed),
    };
    if !valid { return Err(invalid()); }
    Ok(original.commit.controller_revision)
}

fn history_matches(tx:&StoreContext<'_>, task:&Task, revision:u64, kind:&str, expected:&str)->Result<()> {
    let cap=tx.limits.transport.body_bytes;
    let mut q=tx.prepare("SELECT CAST(substr(CAST(body AS BLOB),1,?4) AS TEXT) FROM history WHERE task_id=?1 AND revision=?2 AND kind=?3 LIMIT 2")?;
    let values=q.query_map(params![task.id,integer(revision)?,kind,sql_bound(cap.saturating_add(1))?],|r|r.get::<_,String>(0))?
        .collect::<std::result::Result<Vec<_>,_>>()?;
    if values.len()!=1 || values[0].len()>cap || values[0]!=expected { return Err(invalid()); }
    Ok(())
}

pub(super) fn validate_task(root:&StoreContext<'_>, task:&Task)->Result<()> {
    let Some(block)=&task.native_retry_block else {
        // Pending handoffs are validated against their original intent and receipt by
        // task_handoffs::validate_existing; they are not native retry blocks.
        return if task.status==TaskStatus::Blocked && task.pending_assignment.is_none() {Err(invalid())} else {Ok(())};
    };
    if task.input.definition.executor!=ExecutorKind::Native || block.request_index==0
        || matches!(task.status,TaskStatus::Active|TaskStatus::Queued|TaskStatus::Succeeded) {
        return Err(invalid());
    }
    let snapshot=task.input.resolved_limits.as_ref().ok_or_else(invalid)?;
    snapshot.validate_for(&task.input).map_err(Fault::Store)?;
    let tx=root.with_limits(&snapshot.global_metadata);
    if !valid_id(tx.limits,&block.source_operation_id) { return Err(invalid()); }
    let a=task.attempt.as_ref().ok_or_else(invalid)?;
    let caller=Caller {task_id:task.id.clone(),attempt_id:a.id.clone(),generation:a.generation,
        worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone()};
    let source_revision=source(&tx,task,&caller,block)?;
    let cap=tx.limits.transport.body_bytes;
    let maximum=root.limits.controller.max_receipts.checked_add(root.limits.controller.max_cleanup_receipts)
        .ok_or_else(invalid)?;
    let retained_count:i64=tx.query_row(
        "SELECT count(*) FROM (SELECT 1 FROM receipts LIMIT ?1)",
        [sql_bound(maximum.saturating_add(1))?],|r|r.get(0))?;
    if unsigned(retained_count)? > maximum as u64 {return Err(invalid())}
    let scan_cap=cap.max(root.limits.transport.body_bytes);
    // Select only this attempt's actual block receipts. JSON sees a bounded byte
    // prefix; oversized/truncated invalid JSON fails closed rather than allocating
    // an unbounded retained body. The full receipt reader then proves its context.
    let mut q=tx.prepare("SELECT CAST(substr(CAST(id AS BLOB),1,?5) AS TEXT) FROM receipts WHERE json_extract(CAST(substr(CAST(request AS BLOB),1,?1) AS TEXT),'$.input.report.task_id')=?2 AND json_extract(CAST(substr(CAST(request AS BLOB),1,?1) AS TEXT),'$.input.report.attempt_id')=?3 AND json_extract(CAST(substr(CAST(request AS BLOB),1,?1) AS TEXT),'$.input.report.observation.kind')='native_retry_blocked' ORDER BY controller_revision LIMIT ?4")?;
    let ids=q.query_map(params![sql_bound(scan_cap.saturating_add(1))?,task.id,a.id,sql_bound(maximum.saturating_add(1))?,sql_bound(tx.limits.transport.identity_bytes.saturating_add(1))?],|r|r.get::<_,String>(0))?
        .collect::<std::result::Result<Vec<_>,_>>()?;
    if ids.len()>maximum { return Err(invalid()); }
    let mut found=false;
    for id in ids {
        if !valid_id(tx.limits,&id) { return Err(invalid()); }
        let original=receipt(&tx,&id)?.ok_or_else(invalid)?;
        let Operation::ObserveWorker {report}=&original.request else {return Err(invalid())};
        let observed=match &original.outcome {
            Outcome::Ok {data:Data::Task(observed)}=>observed,
            Outcome::Refused {..}=>continue,
            _=>return Err(invalid()),
        };
        if report.task_id!=task.id || report.attempt_id!=a.id || report.generation!=a.generation
            || report.worker_id!=a.worker_id || report.incarnation!=a.worker_incarnation
            || report.report_id!=id || report.observation!=(Observation::NativeRetryBlocked {block:block.clone()})
            || !same_attempt(observed,&caller) || observed.input!=task.input
            || observed.session_id!=task.session_id || observed.assigned_agent_id!=task.assigned_agent_id
            || observed.status!=TaskStatus::Blocked || observed.native_retry_block.as_ref()!=Some(block)
            || observed.revision>task.revision || original.commit.controller_revision<=source_revision {return Err(invalid())}
        history_matches(&tx,task,observed.revision,"worker_observation",&encode(&report.observation)?)?;
        found=true;
    }
    if found { return Ok(()); }
    // Maintenance has no worker report/receipt. The exact source occurrence binds
    // the attempt and its accepted policy; the single immutable history row
    // binds the original expiry transition without inventing a saved timestamp.
    if block.reason!=RetryBlockReason::Deadline || tx.limits.native_retry.is_none() {return Err(invalid())}
    let original_rejection = block.source_operation_id==crate::native_loop_contract::rejection_id(&a.id,block.request_index)
        && proof::confirmed_rejection(&tx,&caller,block.request_index)?.is_some();
    let original_format = block.source_operation_id==crate::native_loop_contract::format_failure_id(&a.id,block.request_index)
        && super::operations::native_format::confirmed(&tx,&caller,block.request_index)?.is_some();
    let original_cap = block.source_operation_id==crate::native_loop_contract::output_cap_id(&a.id,block.request_index)
        && super::operations::native_output_cap::confirmed(&tx,&caller,block.request_index)?.is_some();
    if !original_rejection && !original_format && !original_cap {return Err(invalid())}
    let proof=receipt(&tx,&block.source_operation_id)?.ok_or_else(invalid)?;
    let Outcome::Ok {data:Data::Task(rejected)}=proof.outcome else {return Err(invalid())};
    if !same_attempt(&rejected,&caller) || rejected.input!=task.input {return Err(invalid())}
    let charged:u32=tx.query_row("SELECT model_requests FROM attempts WHERE id=?1",[&a.id],|r|r.get(0))?;
    // Late Usage remains accounting, not a rewrite of the original expiry.
    if charged<block.request_index {return Err(invalid())}
    let mut q=tx.prepare("SELECT revision,CAST(substr(CAST(body AS BLOB),1,?4) AS TEXT) FROM history WHERE task_id=?1 AND kind='native_retry_deadline' AND revision>?2 AND revision<=?3 ORDER BY revision LIMIT 2")?;
    let rows=q.query_map(params![task.id,integer(rejected.revision)?,integer(task.revision)?,sql_bound(cap.saturating_add(1))?],
        |r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?)))?.collect::<std::result::Result<Vec<_>,_>>()?;
    if rows.len()!=1 || rows[0].1.len()>cap || decode::<NativeRetryBlock>(&rows[0].1)?!=*block {return Err(invalid())}
    Ok(())
}

