//! Canonical proof of a confirmed rejection, distinct from an unknown POST.
//! The trusted worker attests its actual normalized route; the accepted definition
//! supplies the model. No task field is invented as an endpoint authority.
use super::*;
use crate::{mcp::Caller, native::ProviderRejection, native_loop_contract as contract};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NativeRejectionProof {
    pub operation_id: String,
    pub request_index: u32,
    pub endpoint: String,
    pub model: String,
    pub rejection: ProviderRejection,
    pub request_binding: crate::native::ProviderRequestBinding,
    pub controller_revision: u64,
}

fn owns(report: &Report, caller: &Caller) -> bool {
    report.task_id == caller.task_id && report.attempt_id == caller.attempt_id
        && report.generation == caller.generation && report.worker_id == caller.worker_id
        && report.incarnation == caller.incarnation
}
fn accepted(task: &Task, caller: &Caller) -> bool {
    task.id == caller.task_id && task.input.definition.executor == ExecutorKind::Native
        && task.attempt.as_ref().is_some_and(|a| a.id == caller.attempt_id
            && a.generation == caller.generation && a.worker_id == caller.worker_id
            && a.worker_incarnation == caller.incarnation)
}
fn accepted_runtime(task:&Task)->Result<&crate::limits::RuntimeLimits> {
    let snapshot=task.input.resolved_limits.as_ref().ok_or_else(||Fault::Store("native accepted limits absent".into()))?;
    snapshot.validate_for(&task.input).map_err(Fault::Store)?;
    Ok(&snapshot.global_metadata)
}
fn route(task: &Task, endpoint: &str, model: &str, diagnostic: &str) -> Result<()> {
    let limits = task.input.execution_limits().map_err(|e| Fault::Refused("invalid_limits", e))?;
    if endpoint.len() > limits.native.endpoint_bytes
        || diagnostic.len() > accepted_runtime(task)?.controller.max_progress_bytes
        || diagnostic.chars().any(char::is_control)
        || task.input.definition.required_inference().map_err(|e| Fault::Refused("source_mismatch", e.into()))?.model != model {
        return refuse("source_mismatch", "native rejection route, model or diagnostic bound differs");
    }
    let url = reqwest::Url::parse(endpoint).map_err(|_| Fault::Refused("source_mismatch", "native rejection endpoint invalid".into()))?;
    let loopback = url.host_str().and_then(|h| h.trim_matches(['[', ']']).parse::<std::net::IpAddr>().ok()).is_some_and(|ip| ip.is_loopback());
    if url.as_str() != endpoint || url.host_str().is_none() || !url.username().is_empty()
        || url.password().is_some() || url.query().is_some() || url.fragment().is_some()
        || (url.scheme() != "https" && !(url.scheme() == "http" && loopback)) {
        return refuse("source_mismatch", "native rejection endpoint is not the normalized trusted route");
    }
    Ok(())
}

pub(crate) fn reservation(tx: &Connection<'_>, caller: &Caller, index: u32) -> Result<u64> {
    if index == 0 { return refuse("source_mismatch", "native request index is zero"); }
    let id = contract::reservation_id(&caller.attempt_id, index);
    let receipt = super::super::metadata::read(tx, &id)?.ok_or_else(|| Fault::Refused("source_mismatch", "native rejection reservation missing".into()))?;
    let Operation::ObserveWorker { report } = &receipt.request else {
        return refuse("source_mismatch", "native rejection reservation is not a worker report");
    };
    if report.report_id != id || !owns(report, caller)
        || !matches!(&report.observation, Observation::Usage { model_requests, .. } if *model_requests == index)
        || !matches!(&receipt.outcome, Outcome::Ok { data: Data::Task(task) }
            if accepted(task, caller) && task.status == TaskStatus::Active
                && task.attempt.as_ref().is_some_and(|a| !a.stopped && !a.uncertain && a.phase != AttemptPhase::StopRequested)) {
        return refuse("source_mismatch", "native rejection reservation body or original accepted caller differs");
    }
    Ok(receipt.commit.controller_revision)
}

/// Called in the existing ObserveWorker transaction, before its receipt commits.
/// A late known rejection may be retained during stop intent; it grants no retry.
pub(crate) fn validate_report(tx: &Connection<'_>, task: &Task, report: &Report) -> Result<()> {
    let Observation::NativeRequestRejected { request_index, endpoint, model, rejection: _, diagnostic, request_binding } = &report.observation else {
        return refuse("source_mismatch", "native rejection report kind differs");
    };
    let caller = Caller { task_id: report.task_id.clone(), attempt_id: report.attempt_id.clone(),
        generation: report.generation, worker_id: report.worker_id.clone(), incarnation: report.incarnation.clone() };
    if !accepted(task, &caller) || report.report_id != contract::rejection_id(&caller.attempt_id, *request_index) {
        return refuse("source_mismatch", "native rejection original caller or identity differs");
    }
    route(task, endpoint, model, diagnostic)?;
    request_binding.validate(task.input.execution_limits().map_err(|e| Fault::Refused("invalid_limits", e))?.native.request_bytes)
        .map_err(|e| Fault::Refused("source_mismatch", e))?;
    reservation(tx, &caller, *request_index)?;
    if super::super::metadata::read(tx,&contract::output_cap_id(&caller.attempt_id,*request_index))?.is_some()
        || super::native_format::confirmed(tx, &caller, *request_index)?.is_some() {
        return refuse("source_mismatch", "provider rejection conflicts with original completed format occurrence");
    }
    let charged: u32 = tx.query_row("SELECT model_requests FROM attempts WHERE id=?1", [&caller.attempt_id], |r| r.get(0))?;
    if charged != *request_index {
        return refuse("source_mismatch", "native rejection must settle the latest charged request");
    }
    super::super::native_request_diagnostics::validate_rejection(tx, report)?;
    Ok(())
}

/// None means the exact original receipt is absent. Malformed, refused, foreign,
/// or out-of-order records return Err; none of those is proof of no provider effect.
pub(crate) fn confirmed_rejection(tx: &Connection<'_>, caller: &Caller, index: u32) -> Result<Option<NativeRejectionProof>> {
    let id = contract::rejection_id(&caller.attempt_id, index);
    let Some(receipt) = super::super::metadata::read(tx, &id)? else { return Ok(None) };
    let Operation::ObserveWorker { report } = &receipt.request else {
        return refuse("source_mismatch", "native rejection receipt is not a worker report");
    };
    let Observation::NativeRequestRejected { request_index, endpoint, model, rejection, diagnostic, request_binding } = &report.observation else {
        return refuse("source_mismatch", "native rejection receipt kind differs");
    };
    let Outcome::Ok { data: Data::Task(task) } = &receipt.outcome else {
        return refuse("source_mismatch", "native rejection was not canonically accepted");
    };
    if report.report_id != id || *request_index != index || !owns(report, caller) || !accepted(task, caller) {
        return refuse("source_mismatch", "native rejection receipt original identity differs");
    }
    route(task, endpoint, model, diagnostic)?;
    request_binding.validate(task.input.execution_limits().map_err(|e| Fault::Refused("invalid_limits", e))?.native.request_bytes)
        .map_err(|e| Fault::Refused("source_mismatch", e))?;
    let retained = tx.with_limits(accepted_runtime(task)?);
    if reservation(&retained, caller, index)? >= receipt.commit.controller_revision {
        return refuse("source_mismatch", "native rejection does not follow its original charge");
    }
    super::super::native_request_diagnostics::validate_rejection(tx, report)?;
    Ok(Some(NativeRejectionProof { operation_id: id, request_index: index,
        endpoint: endpoint.clone(), model: model.clone(), rejection: *rejection, request_binding: request_binding.clone(),
        controller_revision: receipt.commit.controller_revision }))
}

/// Leaf validator for the shared bounded non-tool request evidence merge.
pub(crate) fn validate_native_request(tx: &Connection<'_>, task: &Task, index: u32, operation_id: &str) -> Result<()> {
    let a = task.attempt.as_ref().ok_or_else(|| Fault::Refused("source_mismatch", "native rejection attempt absent".into()))?;
    let caller = Caller { task_id: task.id.clone(), attempt_id: a.id.clone(), generation: a.generation,
        worker_id: a.worker_id.clone(), incarnation: a.worker_incarnation.clone() };
    let proof = confirmed_rejection(tx, &caller, index)?.ok_or_else(|| Fault::Refused("source_mismatch", "native rejection proof missing".into()))?;
    if !proof.rejection.kind.same_input_retryable() || proof.operation_id != operation_id || proof.model != task.input.definition.required_inference().map_err(|e| Fault::Refused("source_mismatch", e.into()))?.model {
        return refuse("source_mismatch", "native rejection evidence differs");
    }
    let next = index.checked_add(1).ok_or_else(|| Fault::Refused("source_mismatch", "native request index overflow".into()))?;
    if reservation(tx, &caller, next)? <= proof.controller_revision {
        return refuse("source_mismatch", "native retry charge does not follow original rejection proof");
    }
    Ok(())
}

/// Explicit blocked lifecycle; this is not a failed result or a retry grant.
// Traceability scope: The authentication block must join the original confirmed rejection and accepted policy before the retained reason-bearing task state is saved; shared cooldown is owned by native_request_coordination.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A provider authentication failure MUST be coordinated across workers for the same dependency and credential-configuration identity with metadata-controlled backoff and a retained alert.
pub(crate) fn block(tx:&Connection<'_>, stored:&mut StoredTask, report:&Report, now:u64)->Result<()> {
    use crate::native::RetryBlockReason;
    let Observation::NativeRetryBlocked { block }=&report.observation else {
        return refuse("source_mismatch", "native retry block kind differs");
    };
    let a=stored.task.attempt.as_ref().ok_or_else(||Fault::Refused("source_mismatch","native retry block attempt missing".into()))?;
    let caller=Caller {task_id:stored.task.id.clone(),attempt_id:a.id.clone(),generation:a.generation,
        worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone()};
    if !owns(report,&caller) || !matches!(stored.task.status,TaskStatus::Active|TaskStatus::Blocked)
        || a.uncertain || stored.unresolved_execution || pending_effects(tx,&stored.task.id)?>0 {
        return refuse("reconciliation_required","native retry block caller/effects not settled");
    }
    let policy=accepted_runtime(&stored.task)?;
    let accepted=tx.with_limits(policy);
    let rejection_id=contract::rejection_id(&caller.attempt_id,block.request_index);
    let reservation_id=contract::reservation_id(&caller.attempt_id,block.request_index);
    let mut coordination_due=None;
    let proof=if block.source_operation_id==rejection_id {
        Some(confirmed_rejection(&accepted,&caller,block.request_index)?.ok_or_else(||Fault::Refused("source_mismatch","native retry block rejection absent".into()))?)
    }else if block.source_operation_id==reservation_id {
        reservation(&accepted,&caller,block.request_index)?;None
    }else{
        let receipt=super::super::metadata::read(&accepted,&block.source_operation_id)?
            .ok_or_else(||Fault::Refused("source_mismatch","native block acquisition receipt absent".into()))?;
        let Operation::AcquireNativeRequest {operation_id,caller:original,request_index,..}=&receipt.request else {
            return refuse("source_mismatch","native retry block source differs");
        };
        let Outcome::Ok {data:Data::NativeRequestAdmission(crate::native_retry::NativeRequestAdmission::Wait {retry_at_ms,..})}=&receipt.outcome else {
            return refuse("source_mismatch","native block acquisition was not a committed Wait");
        };
        let charged:u32=tx.query_row("SELECT model_requests FROM attempts WHERE id=?1",[&caller.attempt_id],|r|r.get(0))?;
        if operation_id!=&block.source_operation_id||original!=&caller||*request_index!=block.request_index
            ||charged.checked_add(1)!=Some(block.request_index) {
            return refuse("source_mismatch","native block acquisition identity or next charge differs");
        }
        coordination_due=Some(*retry_at_ms);None
    };
    let exhausted=used_model_requests(tx,&stored.task.id)?+u64::from(stored.delegated_model_requests)
        >=u64::from(stored.task.input.limits.model_requests);
    match block.reason {
        RetryBlockReason::RequestAllowance if exhausted=>{},
        RetryBlockReason::Deadline if now>=stored.task.input.limits.deadline_unix_ms=>{},
        RetryBlockReason::RetryPolicyDisabled if proof.is_some() && policy.native_retry.is_none()=>{},
        RetryBlockReason::InvalidRetryAfter if proof.as_ref().is_some_and(|p|p.rejection.retry_after==crate::native::RetryAfter::Invalid)=>{},
        RetryBlockReason::RetryDelayUnavailable if proof.is_some() && policy.native_retry.is_some()=>{
            let retry=policy.native_retry.as_ref().unwrap();
            let floor=match proof.as_ref().unwrap().rejection.retry_after {crate::native::RetryAfter::DelayMs(ms)=>ms,_=>0};
            // Refusal is justified only if the selected maximum delay/floor can
            // cross the unchanged deadline, never an arbitrary retry-count cap.
            if now.checked_add(retry.max_backoff_ms.max(floor)).and_then(|n|n.checked_add(retry.jitter_ms))
                .is_some_and(|due|due<stored.task.input.limits.deadline_unix_ms) {
                return refuse("source_mismatch","native retry delay still fits original deadline");
            }
        },
        RetryBlockReason::CoordinationDelayUnavailable if coordination_due.is_some() && policy.native_retry.is_some()=>{
            let earliest=now.saturating_add(policy.native_retry.as_ref().unwrap().initial_backoff_ms);
            if coordination_due.flatten().unwrap_or(earliest).max(earliest)<stored.task.input.limits.deadline_unix_ms {
                return refuse("source_mismatch","native coordination wait still fits original deadline");
            }
        },
        RetryBlockReason::InvalidRequest if proof.as_ref().is_some_and(|p|p.rejection.kind==crate::native::RejectionKind::InvalidRequest)=>{},
        RetryBlockReason::ContextLimit if proof.as_ref().is_some_and(|p|p.rejection.kind==crate::native::RejectionKind::ContextLimit)=>{},
        RetryBlockReason::PolicyRefused if proof.as_ref().is_some_and(|p|p.rejection.kind==crate::native::RejectionKind::PolicyRefused)=>{},
        RetryBlockReason::QuotaExhausted if proof.as_ref().is_some_and(|p|p.rejection.kind==crate::native::RejectionKind::QuotaExhausted)=>{},
        RetryBlockReason::AuthenticationFailed if proof.as_ref().is_some_and(|p|p.rejection.kind==crate::native::RejectionKind::AuthenticationFailed)=>{},
        _=>return refuse("source_mismatch","native retry block reason is unsupported by original policy/evidence"),
    }
    if stored.task.native_retry_block.as_ref().is_some_and(|old|old!=block) {
        return refuse("operation_conflict","native retry block occurrence already differs");
    }
    stored.task.native_retry_block=Some(block.clone());
    stored.task.status=TaskStatus::Blocked;
    stored.task.blocked_reason=Some(match block.reason {
        RetryBlockReason::RequestAllowance=>"native_request_allowance_exhausted",
        RetryBlockReason::Deadline=>"native_retry_deadline",
        RetryBlockReason::RetryPolicyDisabled=>"native_retry_not_configured",
        RetryBlockReason::InvalidRetryAfter=>"native_retry_after_invalid",
        RetryBlockReason::RetryDelayUnavailable=>"native_retry_delay_exceeds_deadline",
        RetryBlockReason::CoordinationDelayUnavailable=>"native_coordination_delay_exceeds_deadline",
        RetryBlockReason::InvalidRequest=>"native_invalid_request",
        RetryBlockReason::ContextLimit=>"native_context_limit",
        RetryBlockReason::PolicyRefused=>"native_policy_refused",
        RetryBlockReason::QuotaExhausted=>"native_quota_exhausted",
        RetryBlockReason::AuthenticationFailed=>"native_authentication_failed",
    }.into());
    stop(tx,stored)
}

/// Controller expiry uses the already committed rejection as its source. This
/// does not manufacture a worker report, resolve an unknown POST, or refund it.
/// The caller saves the returned typed block as the original expiry event in
/// its existing transaction instead of overwriting this state with Failed.
pub(crate) fn deadline_block(tx:&Connection<'_>,stored:&mut StoredTask,now:u64)
    ->Result<Option<crate::native::NativeRetryBlock>> {
    if stored.task.status!=TaskStatus::Active||now<stored.task.input.limits.deadline_unix_ms
        ||stored.task.input.definition.executor!=ExecutorKind::Native
        ||stored.unresolved_execution||pending_effects(tx,&stored.task.id)?>0 {
        return Ok(None);
    }
    let Some(a)=stored.task.attempt.as_ref().filter(|a|!a.uncertain) else{return Ok(None)};
    let caller=Caller {task_id:stored.task.id.clone(),attempt_id:a.id.clone(),generation:a.generation,
        worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone()};
    let policy=accepted_runtime(&stored.task)?;
    if policy.native_retry.is_none() {return Ok(None)}
    let charged:u32=tx.query_row("SELECT model_requests FROM attempts WHERE id=?1",[&caller.attempt_id],|r|r.get(0))?;
    if charged==0 {return Ok(None)}
    let retained=tx.with_limits(policy);
    let source_operation_id = if let Some(proof)=confirmed_rejection(&retained,&caller,charged)? {
        proof.operation_id
    } else if let Some(proof)=super::native_format::confirmed(&retained,&caller,charged)? {
        proof.operation_id
    } else if let Some(proof)=super::native_output_cap::confirmed(&retained,&caller,charged)? {
        proof.operation_id
    } else {return Ok(None)};
    let block=crate::native::NativeRetryBlock {request_index:charged,source_operation_id,
        reason:crate::native::RetryBlockReason::Deadline};
    if stored.task.native_retry_block.as_ref().is_some_and(|prior|prior!=&block) {
        return Err(Fault::Store("native expiry conflicts with original retry block".into()));
    }
    stored.task.native_retry_block=Some(block.clone());
    stored.task.status=TaskStatus::Blocked;
    stored.task.blocked_reason=Some("native_retry_deadline".into());
    stop(tx,stored)?;
    Ok(Some(block))
}
