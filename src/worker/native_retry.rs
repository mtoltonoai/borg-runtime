//! Worker-owned, receipt-bound native retry timing and request coordination.
use crate::limits::NativeRetryLimits;
use crate::native::RetryAfter;

/// Plan the delay after the 1-based rejected POST within the current logical request.
/// A Retry-After value is a floor, including when it exceeds the configured backoff.
/// No delay is granted at or beyond the unchanged task deadline.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A retry of a throttled platform-owned model request MUST honor a valid provider retry-after value.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# Retry timing, jitter and circuit-breaker bounds MUST come from versioned metadata.
pub(super) fn plan_delay_ms(
    policy: &NativeRetryLimits,
    failed_post_index: u32,
    retry_after: RetryAfter,
    jitter_sample: u64,
    now_ms: u64,
    deadline_unix_ms: u64,
) -> Result<u64, String> {
    policy.validate()?;
    let exponent = failed_post_index
        .checked_sub(1)
        .ok_or("native retry failed_post_index must be positive")?;
    let provider_floor = match retry_after {
        RetryAfter::Absent => 0,
        RetryAfter::DelayMs(value) => value,
        RetryAfter::Invalid => return Err("native retry Retry-After is invalid".into()),
    };
    // Saturation happens before the policy cap, so even u32::MAX needs no linear loop.
    let backoff = policy
        .initial_backoff_ms
        .saturating_mul(u64::from(policy.backoff_multiplier).saturating_pow(exponent))
        .min(policy.max_backoff_ms);
    let jitter_width = policy
        .jitter_ms
        .checked_add(1)
        .ok_or("native retry jitter exceeds clock representation")?;
    let delay = backoff
        .max(provider_floor)
        .checked_add(jitter_sample % jitter_width)
        .ok_or("native retry delay exceeds clock representation")?;
    let retry_at = now_ms
        .checked_add(delay)
        .ok_or("native retry delay exceeds clock representation")?;
    if retry_at >= deadline_unix_ms {
        return Err("native retry delay reaches the task deadline".into());
    }
    Ok(delay)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> NativeRetryLimits {
        NativeRetryLimits {
            initial_backoff_ms: 10,
            backoff_multiplier: 2,
            max_backoff_ms: 80,
            jitter_ms: 7,
            circuit: crate::limits::NativeCircuitLimits { failure_threshold: 4, open_ms: 50, max_entries: 8 },
            auth_cooldown_ms: 80, auth_max_entries: 8, coordination_record_bytes: 65_536,
        }
    }

    #[test]
    //= spec/impl/v2/implementation.md#platform-owned-model-retries
    //= type=test
    //# A platform-owned model request rejected as transient MUST be retried with backoff and jitter within the task's remaining model-request allowance and execution deadline.
    fn exponential_backoff_caps_without_a_second_retry_budget() {
        let value = policy();
        for (post, expected) in [(1, 10), (2, 20), (3, 40), (4, 80), (5, 80), (u32::MAX, 80)] {
            assert_eq!(
                plan_delay_ms(&value, post, RetryAfter::Absent, 0, 100, 1000).unwrap(),
                expected,
            );
        }
        assert!(plan_delay_ms(&value, 0, RetryAfter::Absent, 0, 100, 1000).is_err());
    }

    #[test]
    //= spec/impl/v2/implementation.md#platform-owned-model-retries
    //= type=test
    //# A platform-owned model request rejected as transient MUST be retried with backoff and jitter within the task's remaining model-request allowance and execution deadline.
    //= spec/impl/v2/implementation.md#platform-owned-model-retries
    //= type=test
    //# Retry timing, jitter and circuit-breaker bounds MUST come from versioned metadata.
    fn deterministic_jitter_includes_both_endpoints_and_wraps_samples() {
        for (sample, expected) in [(0, 10), (7, 17), (8, 10), (u64::MAX, 17)] {
            assert_eq!(
                plan_delay_ms(&policy(), 1, RetryAfter::Absent, sample, 100, 1000).unwrap(),
                expected,
            );
        }
        let mut value = policy();
        value.backoff_multiplier = 1;
        assert_eq!(
            plan_delay_ms(&value, u32::MAX, RetryAfter::Absent, 7, 100, 1000).unwrap(),
            17,
        );
    }

    #[test]
    //= spec/impl/v2/implementation.md#platform-owned-model-retries
    //= type=test
    //# A retry of a throttled platform-owned model request MUST honor a valid provider retry-after value.
    fn provider_retry_after_is_never_shortened_by_the_backoff_cap() {
        for (floor, expected) in [(0, 17), (12, 19), (17, 24), (200, 207)] {
            assert_eq!(
                plan_delay_ms(&policy(), 1, RetryAfter::DelayMs(floor), 7, 100, 1000).unwrap(),
                expected,
            );
        }
        assert!(
            plan_delay_ms(&policy(), 1, RetryAfter::Invalid, 0, 100, 1000)
                .unwrap_err()
                .contains("Retry-After"),
        );
    }

    #[test]
    //= spec/impl/v2/implementation.md#platform-owned-model-retries
    //= type=test
    //# A platform-owned model request rejected as transient MUST be retried with backoff and jitter within the task's remaining model-request allowance and execution deadline.
    //= spec/impl/v2/implementation.md#platform-owned-model-retries
    //= type=test
    //# Exhaustion of the retry allowance or deadline MUST leave an explicit reason-bearing blocked state without another provider dispatch.
    fn unchanged_deadline_refuses_equal_expired_and_provider_floor_overrun() {
        assert_eq!(
            plan_delay_ms(&policy(), 1, RetryAfter::Absent, 0, 100, 111).unwrap(),
            10,
        );
        for deadline in [110, 100, 99] {
            assert!(
                plan_delay_ms(&policy(), 1, RetryAfter::Absent, 0, 100, deadline)
                    .unwrap_err()
                    .contains("deadline"),
            );
        }
        assert!(
            plan_delay_ms(&policy(), 1, RetryAfter::DelayMs(900), 0, 100, 1000)
                .unwrap_err()
                .contains("deadline"),
        );
        assert!(
            plan_delay_ms(&policy(), 1, RetryAfter::DelayMs(893), 7, 100, 1000)
                .unwrap_err()
                .contains("deadline"),
        );
    }

    #[test]
    fn caller_clock_and_retry_after_overflow_refuse() {
        for (now, floor) in [(u64::MAX - 5, 0), (1, u64::MAX)] {
            assert!(
                plan_delay_ms(&policy(), 1, RetryAfter::DelayMs(floor), 0, now, u64::MAX)
                    .unwrap_err()
                    .contains("clock representation"),
            );
        }
        assert!(
            plan_delay_ms(&policy(), 1, RetryAfter::DelayMs(u64::MAX), 7, 0, u64::MAX)
                .unwrap_err()
                .contains("clock representation"),
        );
        let mut invalid = policy();
        invalid.jitter_ms = 0;
        assert!(plan_delay_ms(&invalid, 1, RetryAfter::Absent, 0, 100, 1000).is_err());
    }
}

// Worker-owned retry orchestration. The HTTP client still performs exactly one POST.
use super::{Control, Reporter, TaskInput, Observation, Operation, Outcome, Data, TaskStatus, AttemptPhase};
use crate::native::{ProviderRejection, ProviderRequestBinding, RetryBlockReason, NativeRetryBlock};
use std::path::{Path, PathBuf};
use crate::native_retry::{CircuitKey, NativeRequestAdmission, NativeRequestOutcome};

pub(super) enum RetryAction { Retry, Blocked }

fn accepted_limits(input:&TaskInput)->Result<&crate::limits::RuntimeLimits,String> {
    let snapshot=input.resolved_limits.as_ref().ok_or("native retry accepted limits absent")?;
    snapshot.validate_for(input)?;
    Ok(&snapshot.global_metadata)
}
fn caller(reporter:&Reporter)->crate::mcp::Caller {
    crate::mcp::Caller {task_id:reporter.attempt.task_id.clone(),attempt_id:reporter.attempt.id.clone(),
        generation:reporter.attempt.generation,worker_id:reporter.attempt.worker_id.clone(),
        incarnation:reporter.attempt.worker_incarnation.clone()}
}
fn directory(state:&Path,reporter:&Reporter)->PathBuf {
    state.join("native-retry").join(super::component(&reporter.attempt.id))
}
fn journal_path(state:&Path,reporter:&Reporter,index:u32,kind:&str)->PathBuf {
    directory(state,reporter).join(format!("{index:010}-{kind}.json"))
}
fn sync_directory_chain(path:&Path,root:&Path)->Result<(),String> {
    let mut at=path;
    loop {
        std::fs::File::open(at).and_then(|f|f.sync_all()).map_err(|_|"native retry directory sync failed")?;
        if at==root {return Ok(())}
        at=at.parent().ok_or("native retry ancestor absent")?;
    }
}
fn same_request(left:&Operation,right:&Operation)->Result<bool,String> {
    Ok(serde_json::to_value(left).map_err(|e|e.to_string())?==serde_json::to_value(right).map_err(|e|e.to_string())?)
}
fn safe_request(request:&Operation,limits:&crate::limits::RuntimeLimits)->Result<(),String> {
    let value=serde_json::to_value(request).map_err(|e|e.to_string())?;
    if super::value_contains_credential(&value,&super::configured_credentials(limits)?,&limits.worker)? {
        return Err("native coordination request contains configured credential".into());
    }
    Ok(())
}
fn create(path:&Path,request:Operation,limits:&crate::limits::RuntimeLimits)->Result<super::mutations::Journal,String> {
    safe_request(&request,limits)?;
    super::mutations::create(path,request,0,limits)
}

/// Runs before the existing durable Usage charge and the single actual POST.
/// A committed Wait is retained before a distinct later acquire; a consumed
/// grant is never reused to dispatch the same request index a second time.
pub(super) async fn acquire(reporter:&Reporter,input:&TaskInput,control:&Control,state:&Path,
    config:&crate::native::NativeConfig,index:u32)->Result<RetryAction,String> {
    let limits=accepted_limits(input)?;
    let Some(policy)=limits.native_retry.as_ref() else {return Ok(RetryAction::Retry)};
    current(reporter,input,control).await?;
    let key=CircuitKey::new(&reqwest::Url::parse(&config.endpoint).map_err(|_|"native coordination endpoint invalid")?,&config.model)?;
    let root=directory(state,reporter);
    std::fs::create_dir_all(&root).map_err(|_|"native coordination journal directory unavailable")?;
    sync_directory_chain(&root,state)?;
    let path=journal_path(state,reporter,index,"acquire");
    let mut journal=match super::mutations::read(&path,limits)? {
        Some(journal)=>journal,
        None=>create(&path,Operation::AcquireNativeRequest {
            operation_id:super::mutations::fresh_id("native-acquire")?,caller:caller(reporter),
            request_index:index,key:key.clone(),credential_configuration:config.api_key_env.clone(),
        },limits)?,
    };
    loop {
        let Operation::AcquireNativeRequest {caller:original,request_index,key:original_key,credential_configuration,..}=&journal.request else {
            return Err("native acquisition journal kind differs".into());
        };
        if original!=&caller(reporter)||*request_index!=index||original_key!=&key||credential_configuration!=&config.api_key_env {
            return Err("native acquisition original request differs".into());
        }
        safe_request(&journal.request,limits)?;
        current(reporter,input,control).await?;
        let mut stopped=control.clone();
        let outcome=tokio::select! {biased;
            _=stopped.stopped()=>return Err("native acquisition cancelled or expired".into()),
            result=super::mutations::resolve(&reporter.client,&path,&mut journal)=>result?,
        }.ok_or("native acquisition original outcome unavailable; POST withheld")?;
        let Outcome::Ok {data:Data::NativeRequestAdmission(admission)}=outcome else {
            return Err("native acquisition refused; POST withheld".into());
        };
        let context=journal.outcome_context.as_ref().ok_or("native acquisition context absent")?;
        match admission {
            NativeRequestAdmission::Granted {permit}=>{
                if permit.circuit.caller!=caller(reporter)||permit.request_index!=index||permit.circuit.key!=key
                    ||permit.credential_configuration!=config.api_key_env
                    ||permit.circuit.operation_id!=context.commit.operation_id {
                    return Err("native acquisition permit differs".into());
                }
                if journal.consumed {return Err("native original request grant was already consumed; POST withheld".into())}
                super::mutations::consume(&path,&mut journal)?;
                current(reporter,input,control).await?;
                return Ok(RetryAction::Retry);
            },
            NativeRequestAdmission::Wait {retry_at_ms,..}=>{
                let now=super::process::now_ms();
                let due=retry_at_ms.unwrap_or_else(||now.saturating_add(policy.initial_backoff_ms));
                // A pending probe is polled only on the accepted timing policy;
                // an explicit provider/circuit time is never shortened.
                let mut due=due.max(now.saturating_add(policy.initial_backoff_ms));
                let revision=context.commit.controller_revision;
                let wait_id=context.commit.operation_id.clone();
                super::mutations::consume(&path,&mut journal)?;
                // Archive the complete old request/receipt before replacing the
                // active slot. A visible prior archive must be re-synced too.
                let archive=root.join(format!("{index:010}-wait-{revision:020}.json"));
                if let Some(old)=super::mutations::read(&archive,limits)? {
                    if serde_json::to_value(&old).map_err(|e|e.to_string())?!=serde_json::to_value(&journal).map_err(|e|e.to_string())? {
                        return Err("native coordination wait archive differs".into());
                    }
                }
                super::mutations::persist(&archive,&journal)?;
                sync_directory_chain(&root,state)?;
                loop {
                    if due>=input.limits.deadline_unix_ms {
                        if !super::native_registry_reconciled(state,&reporter.attempt,&control.runtime_limits).await? {
                            return Err("native coordination block cleanup unverified".into());
                        }
                        return blocked_from(reporter,index,RetryBlockReason::CoordinationDelayUnavailable,wait_id.clone()).await;
                    }
                    let mut stopped=control.clone();
                    tokio::select! {biased;
                        _=stopped.stopped()=>return Err("native coordination wait cancelled or expired".into()),
                        _=tokio::time::sleep(std::time::Duration::from_millis(due.saturating_sub(super::process::now_ms())))=>{},
                    }
                    current(reporter,input,control).await?;
                    let query=Operation::ObserveNativeRequest {caller:caller(reporter),acquire_operation_id:wait_id.clone()};
                    let mut stopped=control.clone();
                    let reply=tokio::select! {biased;
                        _=stopped.stopped()=>return Err("native readiness observation cancelled or expired".into()),
                        result=reporter.client.call(query.clone())=>result?,
                    };
                    reply.validate_for(&query)?;
                    let Outcome::Ok {data:Data::NativeRequestReadiness {acquire_operation_id,changed}}=reply.outcome else {
                        return Err("native readiness original Wait unavailable; POST withheld".into());
                    };
                    if acquire_operation_id!=wait_id {return Err("native readiness original Wait differs".into())}
                    if changed {break}
                    // This is a read only observation. The serving controller
                    // may wait on its existing revision watch; repeated unchanged
                    // observations create neither receipts nor Wait archives.
                    due=super::process::now_ms().saturating_add(policy.initial_backoff_ms);
                }
                current(reporter,input,control).await?;
                journal=create(&path,Operation::AcquireNativeRequest {
                    operation_id:super::mutations::fresh_id("native-acquire")?,caller:caller(reporter),
                    request_index:index,key:key.clone(),credential_configuration:config.api_key_env.clone(),
                },limits)?;
            },
        }
    }
}

/// Settle health for the exact original permit, never a new provider operation.
/// Missing acquisition proves no grant only when the callback was never entered;
/// a retained unresolved acquisition is an error, not inferred NotStarted.
pub(super) async fn settle(reporter:&Reporter,input:&TaskInput,state:&Path,index:u32,
    outcome:NativeRequestOutcome)->Result<(),String> {
    let limits=accepted_limits(input)?;
    if limits.native_retry.is_none() {return Ok(())}
    let acquisition=journal_path(state,reporter,index,"acquire");
    let Some(grant)=super::mutations::read(&acquisition,limits)? else {
        return if outcome==NativeRequestOutcome::NotStarted {Ok(())} else {Err("native outcome has no original acquisition".into())};
    };
    let permit=match &grant.outcome {
        Some(Outcome::Ok {data:Data::NativeRequestAdmission(NativeRequestAdmission::Granted {permit})})=>permit,
        Some(Outcome::Ok {data:Data::NativeRequestAdmission(NativeRequestAdmission::Wait {..})})
            if outcome==NativeRequestOutcome::NotStarted=>return Ok(()),
        _=>return Err("native acquisition remains unresolved; settlement withheld".into()),
    };
    let context=grant.outcome_context.as_ref().ok_or("native acquisition original context absent")?;
    if !grant.consumed||permit.request_index!=index||permit.circuit.caller!=caller(reporter)
        ||permit.circuit.operation_id!=context.commit.operation_id {
        return Err("native outcome original permit differs".into());
    }
    let path=journal_path(state,reporter,index,"settle");
    let operation=Operation::SettleNativeRequest {operation_id:format!("{}-native-health-{index}",reporter.attempt.id),
        caller:caller(reporter),acquire_operation_id:permit.circuit.operation_id.clone(),outcome:outcome.clone()};
    safe_request(&operation,limits)?;
    let mut journal=match super::mutations::read(&path,limits)? {
        Some(journal)=>{if !same_request(&journal.request,&operation)? {return Err("native health settlement original request differs".into())}journal},
        None=>create(&path,operation,limits)?,
    };
    let resolved=super::mutations::resolve(&reporter.client,&path,&mut journal).await?
        .ok_or("native health settlement unavailable; continuation withheld")?;
    let Outcome::Ok {data:Data::NativeRequestSettlement(settlement)}=resolved else {
        return Err("native health settlement refused; continuation withheld".into());
    };
    if settlement.permit!=*permit||settlement.outcome!=outcome
        ||settlement.source!=crate::native_retry::NativeSettlementSource::Reported
        ||journal.outcome_context.as_ref().is_none_or(|ctx|ctx.commit.operation_id!=settlement.operation_id) {
        return Err("native health settlement original receipt differs".into());
    }
    super::mutations::consume(&path,&mut journal)
}

//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A platform-owned retry MUST revalidate its current task, attempt, ownership and cancellation state before dispatch.
pub(super) async fn current(reporter: &Reporter, input: &TaskInput, control: &Control) -> Result<(), String> {
    control.check()?;
    let operation = Operation::GetTask { task_id: reporter.attempt.task_id.clone() };
    let mut stopped = control.clone();
    let reply = tokio::select! { biased;
        _=stopped.stopped()=>return Err("native retry cancelled or expired".into()),
        result=reporter.client.call(operation.clone())=>result?,
    };
    reply.validate_for(&operation)?;
    let Outcome::Ok { data: Data::Task(task) } = reply.outcome else { return Err("native retry current task unavailable".into()) };
    let mut expected=input.clone();
    expected.limits.model_requests=task.input.limits.model_requests;
    if task.status!=TaskStatus::Active || *task.input!=expected
        || task.session_id!=reporter.attempt.session_id || task.assigned_agent_id!=input.definition.id
        || task.waiting_child_id.is_some() || !task.attempt.as_ref().is_some_and(|a|
            a.id==reporter.attempt.id && a.generation==reporter.attempt.generation
            && a.session_id==reporter.attempt.session_id && a.worker_id==reporter.attempt.worker_id
            && a.worker_incarnation==reporter.attempt.worker_incarnation
            && a.phase==AttemptPhase::Running && !a.stopped && !a.uncertain) {
        return Err("native retry original owner/input no longer current".into());
    }
    control.check()
}

async fn blocked(reporter: &Reporter, index: u32, reason: RetryBlockReason) -> Result<RetryAction, String> {
    super::boxed_execution(|| blocked_from(reporter,index,reason,crate::native_loop_contract::rejection_id(&reporter.attempt.id,index))).await
}
async fn blocked_from(reporter:&Reporter,index:u32,reason:RetryBlockReason,source_operation_id:String)->Result<RetryAction,String> {
    let block=NativeRetryBlock {request_index:index,source_operation_id,reason};
    // Retain reporting order while constructing each substantial child future
    // outside this poll frame, including the post-block physical-stop report.
    if !super::boxed_execution(|| reporter.send_identified(format!("{}-native-retry-block-{index}",reporter.attempt.id),
        Observation::NativeRetryBlocked {block})).await? {
        return Err("native retry blocked-state receipt unacknowledged".into());
    }
    // This caller has already established its exact validator registry closure.
    // No fabricated failed ResultCandidate or successful provider outcome follows.
    if !super::boxed_execution(|| reporter.send_acknowledged(Observation::ProcessTreeStopped)).await? {
        return Err("native retry blocked stop acknowledgment unavailable".into());
    }
    Ok(RetryAction::Blocked)
}

/// Record the original rejection before considering retry. Every caller supplies
/// the actual adapter binding and preserves the same logical input across retries.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A platform-owned model request rejected as transient MUST be retried with backoff and jitter within the task's remaining model-request allowance and execution deadline.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A platform-owned model request rejected as invalid MUST NOT be retried.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A context-window rejection MUST NOT be retried with unchanged input.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A provider policy refusal MUST be reported as a policy block.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# Exhaustion of the retry allowance or deadline MUST leave an explicit reason-bearing blocked state without another provider dispatch.
pub(super) async fn rejected(
    reporter: &Reporter, input:&TaskInput, control:&Control, state:&Path,
    config:&crate::native::NativeConfig, index:u32, rejection:ProviderRejection,
    request_binding:&ProviderRequestBinding, diagnostic:&str, failed_post_index:u32,
) -> Result<RetryAction,String> {
    if failed_post_index==0||failed_post_index>index {return Err("native logical retry ordinal differs from charged index".into())}
    let execution=input.execution_limits()?;
    let limits=&input.resolved_limits.as_ref().ok_or("native retry accepted limits absent")?.global_metadata;
    request_binding.validate(execution.native.request_bytes)?;
    let endpoint=reqwest::Url::parse(&config.endpoint).map_err(|_|"native retry endpoint invalid")?.to_string();
    let diagnostic=super::scrub(diagnostic.replace(['\r','\n','\t']," "),
        limits.controller.max_progress_bytes,&control.runtime_limits);
    if !super::boxed_execution(|| reporter.send_identified(crate::native_loop_contract::rejection_id(&reporter.attempt.id,index),
        Observation::NativeRequestRejected {request_index:index,endpoint,model:config.model.clone(),
            rejection,request_binding:request_binding.clone(),diagnostic})).await? {
        return Err("native rejection receipt unacknowledged; next POST withheld".into());
    }
    super::boxed_execution(|| settle(reporter,input,state,index,NativeRequestOutcome::Rejected {rejection})).await?;
    // Known provider rejection and actual local process closure are independent.
    // Reconcile this attempt's exact registry, never a global count of other work.
    if !super::boxed_execution(|| super::native_registry_reconciled(state,&reporter.attempt,&control.runtime_limits)).await? {
        return Err("native retry local cleanup unverified".into());
    }
    if *control.cancel.borrow() { return Err("native retry cancelled".into()) }
    if !rejection.kind.same_input_retryable() {
        let reason=match rejection.kind {
            crate::native::RejectionKind::InvalidRequest=>RetryBlockReason::InvalidRequest,
            crate::native::RejectionKind::ContextLimit=>RetryBlockReason::ContextLimit,
            crate::native::RejectionKind::PolicyRefused=>RetryBlockReason::PolicyRefused,
            crate::native::RejectionKind::QuotaExhausted=>RetryBlockReason::QuotaExhausted,
            crate::native::RejectionKind::AuthenticationFailed=>RetryBlockReason::AuthenticationFailed,
            crate::native::RejectionKind::RateLimited=>unreachable!("retryable checked above"),
        };
        return super::boxed_execution(|| blocked(reporter,index,reason)).await;
    }
    let now=super::process::now_ms();
    if now>=input.limits.deadline_unix_ms { return super::boxed_execution(|| blocked(reporter,index,RetryBlockReason::Deadline)).await }
    if index>=input.limits.model_requests { return super::boxed_execution(|| blocked(reporter,index,RetryBlockReason::RequestAllowance)).await }
    let Some(policy)=limits.native_retry.as_ref() else { return super::boxed_execution(|| blocked(reporter,index,RetryBlockReason::RetryPolicyDisabled)).await };
    if rejection.retry_after==crate::native::RetryAfter::Invalid {
        return super::boxed_execution(|| blocked(reporter,index,RetryBlockReason::InvalidRetryAfter)).await;
    }
    let mut entropy=[0u8;8];
    aws_lc_rs::rand::fill(&mut entropy).map_err(|_|"native retry jitter unavailable")?;
    let delay=match plan_delay_ms(policy,failed_post_index,rejection.retry_after,u64::from_ne_bytes(entropy),now,input.limits.deadline_unix_ms) {
        Ok(delay)=>delay,
        Err(_)=>return super::boxed_execution(|| blocked(reporter,index,RetryBlockReason::RetryDelayUnavailable)).await,
    };
    super::boxed_execution(|| current(reporter,input,control)).await?;
    let mut stopped=control.clone();
    tokio::select! { biased;
        _=stopped.stopped()=>return Err("native retry wait cancelled or expired".into()),
        _=tokio::time::sleep(std::time::Duration::from_millis(delay))=>{},
    }
    super::boxed_execution(|| current(reporter,input,control)).await?;
    Ok(RetryAction::Retry)
}

/// A completed response requiring another step cannot authorize a tool effect
/// when its mandatory continuation has no remaining request allowance.
pub(super) async fn request_exhausted(reporter:&Reporter,control:&Control,state:&Path,index:u32)->Result<RetryAction,String> {
    if !super::native_registry_reconciled(state,&reporter.attempt,&control.runtime_limits).await? {
        return Err("native exhausted request cleanup unverified".into());
    }
    blocked_from(reporter,index,RetryBlockReason::RequestAllowance,
        crate::native_loop_contract::reservation_id(&reporter.attempt.id,index)).await
}
