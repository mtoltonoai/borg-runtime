//! Explicit collaboration profile orchestration over the existing worker
//! outbox and mutation journals. Provider requests are never retried or resumed
//! after worker loss; a known delegated child can still be quiesced safely.
use super::*;
use crate::governance_completion::{
    CompletionBoundary, CompletionTarget, NativeCompletionContinuationEvidence,
};
use crate::{mcp, native, native_loop_contract as contract};
#[path = "completion_compaction.rs"]
mod completion_compaction;

#[derive(Default)]
struct Usage {
    input: Option<u64>,
    output: Option<u64>,
    known: u32,
    unknown: u32,
}
impl Usage {
    fn add(&mut self, value: native::UsageObservation) -> Result<bool, String> {
        let native::UsageObservation::Reported(value) = value else {
            self.unknown = self.unknown.checked_add(1).ok_or("native usage overflow")?;
            return Ok(false);
        };
        self.input = Some(
            self.input
                .unwrap_or(0)
                .checked_add(value.input_tokens)
                .filter(|n| *n <= i64::MAX as u64)
                .ok_or("native input usage overflow")?,
        );
        self.output = Some(
            self.output
                .unwrap_or(0)
                .checked_add(value.output_tokens)
                .filter(|n| *n <= i64::MAX as u64)
                .ok_or("native output usage overflow")?,
        );
        self.known = self.known.checked_add(1).ok_or("native usage overflow")?;
        Ok(true)
    }
    fn diagnostic(&self) -> String {
        if self.unknown == 0 {
            format!(
                "native token usage: complete reported total for {} requests",
                self.known
            )
        } else {
            format!(
                "native token usage: reported subtotal for {} requests; {} requests unknown",
                self.known, self.unknown
            )
        }
    }
}
fn caller(attempt: &Attempt) -> mcp::Caller {
    mcp::Caller {
        task_id: attempt.task_id.clone(),
        attempt_id: attempt.id.clone(),
        generation: attempt.generation,
        worker_id: attempt.worker_id.clone(),
        incarnation: attempt.worker_incarnation.clone(),
    }
}
fn directory(state: &Path, attempt: &Attempt) -> PathBuf {
    state.join("collaboration").join(component(&attempt.id))
}
fn path(state: &Path, attempt: &Attempt, index: u32, call_index: u32) -> PathBuf {
    let name = if call_index == 1 {
        format!("{index:010}.json")
    } else {
        format!("{index:010}-{call_index:010}.json")
    };
    directory(state, attempt).join(name)
}
// Only the model continuation keeps reasoning items. Public progress retains
// the other validated observations, including exact search/citation data.
// The caller checks the complete original items for credentials and size first.
fn public_provider_observations(items: &serde_json::Value) -> Result<String, String> {
    let items = items
        .as_array()
        .ok_or("native provider observations invalid")?
        .iter()
        .filter(|item| item.get("type").and_then(serde_json::Value::as_str) != Some("reasoning"))
        .collect::<Vec<_>>();
    serde_json::to_string(&items).map_err(|e| e.to_string())
}

async fn current(
    reporter: &Reporter,
    input: &TaskInput,
    control: &Control,
) -> Result<Task, String> {
    control.check()?;
    let op = Operation::GetTask {
        task_id: reporter.attempt.task_id.clone(),
    };
    let mut stopped = control.clone();
    let reply = tokio::select! { biased;
        _ = stopped.stopped() => return Err("native collaboration cancelled or expired".into()),
        reply = reporter.client.call(op.clone()) => reply?,
    };
    control.check()?;
    validate_current(reporter, input, reply)
}
fn validate_current(reporter: &Reporter, input: &TaskInput, reply: Reply) -> Result<Task, String> {
    reply.validate_for(&Operation::GetTask {
        task_id: reporter.attempt.task_id.clone(),
    })?;
    let Outcome::Ok {
        data: Data::Task(task),
    } = reply.outcome
    else {
        return Err("native collaboration current owner unavailable".into());
    };
    let Some(a) = task.attempt.as_ref() else {
        return Err("native collaboration attempt absent".into());
    };
    let mut expected = input.clone();
    expected.limits.model_requests = task.input.limits.model_requests;
    if task.id != reporter.attempt.task_id
        || task.session_id != reporter.attempt.session_id
        || task.assigned_agent_id != input.definition.id
        || task.status != TaskStatus::Active
        || a.id != reporter.attempt.id
        || a.session_id != reporter.attempt.session_id
        || a.generation != reporter.attempt.generation
        || a.worker_id != reporter.attempt.worker_id
        || a.worker_incarnation != reporter.attempt.worker_incarnation
        || a.deadline_unix_ms != reporter.attempt.deadline_unix_ms
        || a.stopped
        || a.uncertain
        || a.phase != AttemptPhase::Running
        || task.waiting_child_id.is_some()
        || task.input != expected
        || !crate::execution_profile::collaboration(&task.input)
    {
        return Err("native collaboration owner/input changed".into());
    }
    Ok(*task)
}

// A failed current-owner read provides no authority, but is also not a known
// fence change. Required-report recovery waits without dispatch until it can
// check the same canonical predicates, or its outer Control bound expires.
async fn required_current(
    reporter: &Reporter,
    input: &TaskInput,
    control: &Control,
) -> Result<Task, String> {
    let backoff = input.execution_limits()?.collaboration.lookup_backoff_ms;
    loop {
        control.check()?;
        let reply = reporter
            .client
            .call_observed(Operation::GetTask {
                task_id: reporter.attempt.task_id.clone(),
            })
            .await;
        control.check()?;
        match reply {
            Ok(reply) => return validate_current(reporter, input, reply),
            Err(crate::client::CallFailure::Unavailable(_)) => {
                tokio::time::sleep(Duration::from_millis(backoff)).await
            }
            Err(crate::client::CallFailure::Contract(error)) => return Err(error),
        }
    }
}

/// Required final observations use their original durable outbox identity.
/// A missing ACK is not a refused report and cannot become a failed result.
pub(super) async fn required_report(
    reporter: &Reporter,
    input: &TaskInput,
    control: &Control,
    report_id: String,
    observation: Observation,
) -> Result<bool, String> {
    if !matches!(
        &observation,
        Observation::Progress { .. } | Observation::NativeLoopCompleted { .. }
    ) {
        return Err("unsupported required native report".into());
    }
    let mut stopped = control.clone();
    let reconcile = async {
        required_current(reporter, input, control).await?;
        let credentials = configured_credentials(&reporter.config.limits)?;
        let (acknowledged, original) = reporter
            .send_identified_retained(report_id.clone(), observation, &credentials)
            .await?;
        if acknowledged {
            required_current(reporter, input, control).await?;
            return Ok(true);
        }
        let path = reporter
            .outbox
            .join(format!("{}.json", component(&report_id)));
        // Progress may have been scrubbed by Reporter. It returns the exact
        // retained body even if another outbox flush already removed the file.
        if original.report_id != report_id
            || original.task_id != reporter.attempt.task_id
            || original.attempt_id != reporter.attempt.id
            || original.generation != reporter.attempt.generation
            || original.worker_id != reporter.config.id
            || original.incarnation != reporter.config.incarnation
        {
            return Err("required native report identity differs".into());
        }
        let intended = Operation::ObserveWorker {
            report: original.clone(),
        };
        let backoff = input.execution_limits()?.collaboration.lookup_backoff_ms;
        loop {
            required_current(reporter, input, control).await?;
            match acceptance::lookup_required(&reporter.client, &report_id, &intended).await? {
                acceptance::Lookup::Recorded(Outcome::Ok { .. }, _) => {
                    required_current(reporter, input, control).await?;
                    return Ok(true);
                }
                acceptance::Lookup::Recorded(Outcome::Refused { .. }, _) => return Ok(false),
                acceptance::Lookup::Recorded(_, _) => {
                    return Err("required native report outcome differs".into());
                }
                acceptance::Lookup::Absent => {
                    let _lock = reporter.outbox_lock.lock().await;
                    if path.exists() && read_report(&path, &reporter.config.limits)? != original {
                        return Err("required native report body changed".into());
                    }
                    // Only confirmed absence permits original outbox replay;
                    // this path cannot create a model request or tool effect.
                    flush_outbox(&reporter.client, &reporter.outbox).await?;
                }
                acceptance::Lookup::Unavailable => {}
            }
            tokio::time::sleep(Duration::from_millis(backoff)).await;
        }
    };
    tokio::select! { biased;
        _ = stopped.stopped() => Err("required native report cancelled or expired".into()),
        result = reconcile => result,
    }
}
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# Every platform-owned model request dispatched by a retry MUST have an acknowledged durable request reservation before dispatch.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# Each newly dispatched platform-owned model attempt MUST carry a distinct retained request-attempt identity.
async fn reserve(
    reporter: &Reporter,
    input: &TaskInput,
    control: &Control,
    usage: &Usage,
    index: u32,
    diagnostic: &mut NativeDiagnosticRequest,
) -> Result<(), String> {
    if index == 0 || index > input.limits.model_requests {
        return Err("native model request limit: task_model_requests".into());
    }
    current(reporter, input, control).await?;
    diagnostic
        .reserve(reporter, control, usage.input, usage.output)
        .await?;
    current(reporter, input, control).await?;
    Ok(())
}
async fn observed(
    reporter: &Reporter,
    usage: &mut Usage,
    index: u32,
    value: native::NativeLoopObservation,
) -> Result<native::NativeLoopStep, String> {
    if let Err(publication) =
        observed_usage(reporter, usage, index, value.usage, value.provider_outcome).await
    {
        return Err(match &value.result {
            Err(original) => format!("{original}; native usage recording failed: {publication}"),
            Ok(_) => publication,
        });
    }
    if reporter.provider_uncertain.load(Ordering::Relaxed) && value.result.is_ok() {
        return Err("native provider outcome remains uncertain".into());
    }
    value.result
}
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A local transport close or process cleanup MUST NOT establish that an ambiguous provider request had no effect.
async fn observed_usage(
    reporter: &Reporter,
    usage: &mut Usage,
    index: u32,
    tokens: native::UsageObservation,
    outcome: native::ProviderOutcome,
) -> Result<(), String> {
    // Exposure/outcome is retained before any fallible usage publication.
    if outcome == native::ProviderOutcome::Unknown {
        reporter.provider_uncertain.store(true, Ordering::Relaxed);
    }
    if usage.add(tokens)?
        && !reporter
            .send_identified(
                contract::usage_id(&reporter.attempt.id, index),
                Observation::Usage {
                    native_diagnostic_reserved: false,
                    model_requests: index,
                    input_tokens: usage.input,
                    output_tokens: usage.output,
                },
            )
            .await?
    {
        return Err("native usage unacknowledged; next request withheld".into());
    }
    Ok(())
}
// A low-priority compaction runs only at an already-needed continuation,
// after every prior tool result is committed, and only with room left for the
// required ordinary request. Exhaustion preserves due state; it never adds an
// idle wake or increases the accepted request allowance.
fn same_request(left: &Operation, right: &Operation) -> Result<bool, String> {
    Ok(serde_json::to_value(left).map_err(|e| e.to_string())?
        == serde_json::to_value(right).map_err(|e| e.to_string())?)
}
fn safe_value(
    value: &impl serde::Serialize,
    limits: &crate::limits::RuntimeLimits,
) -> Result<(), String> {
    let value = serde_json::to_value(value).map_err(|e| e.to_string())?;
    if value_contains_credential(&value, &configured_credentials(limits)?, &limits.worker)? {
        return Err("collaboration data contains configured credential".into());
    }
    Ok(())
}
fn retain_reply(
    journal: &mut mutations::Journal,
    path: &Path,
    value: mcp::CallResult,
) -> Result<(), String> {
    safe_value(&value.reply, &journal.limits)?;
    let context = CommitContext::from_reply(&value.reply, &journal.request)?
        .ok_or("collaboration result has no original committed receipt")?;
    journal.outcome = Some(value.reply.outcome);
    journal.outcome_context = Some(context);
    atomic_json(path, journal)
}
fn requires_quiescence(journal: &mutations::Journal) -> bool {
    matches!((&journal.request,&journal.outcome),
        (Operation::CallCollaboration {call}, Some(outcome))
        if call_requires_quiescence(call, outcome))
}
fn admit_call_boundary(name:&str,args:&serde_json::Value,following:bool,index:u32,input:&TaskInput,attempt:&Attempt)->Result<(),String> {
    let self_assignment=name=="collab_task_assign"
        &&args.get("task_id").and_then(serde_json::Value::as_str)==Some(attempt.task_id.as_str())
        &&args.get("agent_id").and_then(serde_json::Value::as_str).is_some_and(|id|id!=input.definition.id);
    if index==input.limits.model_requests && name!="collab_task_delegate" && !self_assignment {
        return Err("native continuation request allowance exhausted: task_model_requests; tool not dispatched".into());
    }
    if self_assignment&&following {
        return Err("self assignment must be the final call in its response; assignment not dispatched".into());
    }
    let self_wait=name=="collab_task_wait"
        &&args.get("task_id").and_then(serde_json::Value::as_str)==Some(attempt.task_id.as_str());
    if self_wait&&following {
        return Err("self wait must be the final call in its response; wait not dispatched".into());
    }
    Ok(())
}
fn call_requires_quiescence(call: &mcp::Invocation, outcome: &Outcome) -> bool {
    matches!(outcome, Outcome::Ok {data:Data::Task(child)}
        if call.tool == "collab_task_delegate" && child.parent_task_id.as_deref() == Some(call.caller.task_id.as_str()))
        || mcp::assignment_quiescence(call, outcome)
        || mcp::work::requires_quiescence(call, outcome)
}
// Traceability scope: The native occurrence stores the complete Invocation before lookup or send and refuses any changed retained request binding.
//= spec/impl/v2/implementation.md#native-tool-profiles-and-continuations
//# An execution attempt MUST retain a tool call's exact argument binding before sending a mutation.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A platform-owned idempotent tool call that is unreachable or timed out MUST be retried within its accepted budget and deadline.
async fn invoke(
    client: &mcp::Client,
    call: mcp::Invocation,
    state: &Path,
    reporter: &Reporter,
    input: &TaskInput,
    control: &Control,
    index: u32,
    call_index: u32,
) -> Result<mutations::Journal, String> {
    safe_value(&call, &control.runtime_limits)?;
    process::private_directory(&state.join("collaboration"))?;
    process::private_directory(&directory(state, &reporter.attempt))?;
    let path = path(state, &reporter.attempt, index, call_index);
    let request = Operation::CallCollaboration { call: call.clone() };
    let inherited = path.exists();
    let mut journal = if inherited {
        let old = mutations::read(&path, &control.runtime_limits)?
            .ok_or("collaboration journal disappeared")?;
        if !same_request(&old.request, &request)? {
            return Err("collaboration occurrence body changed".into());
        }
        old
    } else {
        mutations::create(&path, request, 0, &control.runtime_limits)?
    };
    if journal.outcome.is_some() {
        if let Err(error) = mutations::persist(&path, &journal) {
            reporter.provider_uncertain.store(true, Ordering::Relaxed);
            return Err(format!(
                "collaboration receipt retention uncertain: {error}"
            ));
        }
        return Ok(journal);
    }
    let read_retry = mcp::retryable_read_tool(&call.tool);
    let backoff = input.execution_limits()?.collaboration.lookup_backoff_ms;
    // A retained pending intent may already have crossed HTTP before a loss.
    let mut dispatched = inherited;
    let mut stopped_for_receipt = false;
    let recovered = async {
        let mut delayed = false;
        let mut failure = "collaboration occurrence has no committed receipt".to_owned();
        loop {
            if delayed {
                let mut stopped = control.clone();
                tokio::select! { biased;
                    _ = stopped.stopped() => {
                        stopped_for_receipt = true;
                        return Err("collaboration recovery cancelled or expired".to_owned());
                    },
                    _ = tokio::time::sleep(Duration::from_millis(backoff)) => {},
                }
            }
            // This lookup is repeated after backoff. An unavailable lookup is
            // never absence and cannot authorize another send, even for a read.
            let mut stopped = control.clone();
            let original = tokio::select! { biased;
                _ = stopped.stopped() => {
                    stopped_for_receipt = true;
                    return Err("collaboration lookup cancelled or expired".to_owned());
                },
                value = client.get_call_observed(&call) => value,
            };
            match original {
                Ok(Some(value)) => return Ok(value),
                Ok(None) => {}
                Err(mcp::CallFailure::Transport(error)) if read_retry => {
                    failure = error;
                    delayed = true;
                    continue;
                }
                Err(error) => return Err(error.into_message()),
            }
            if dispatched && !read_retry {
                return Err(failure);
            }
            if dispatched && !delayed {
                // Reconcile immediately after a lost reply, then wait before
                // another original-call lookup and eligible read dispatch.
                delayed = true;
                continue;
            }
            mutations::persist(&path, &journal)?;
            current(reporter, input, control).await?;
            resource::verify(&reporter.resource)?;
            let mut stopped = control.clone();
            // Set before polling HTTP so cancellation cannot clear uncertainty.
            dispatched = true;
            let observed = tokio::select! { biased;
                _ = stopped.stopped() => {
                    stopped_for_receipt = true;
                    return Err("collaboration call interrupted".to_owned());
                },
                value = client.call_observed(&call) => value,
            };
            match observed {
                Ok(value) => return Ok(value),
                Err(mcp::CallFailure::Transport(error)) => failure = error,
                Err(mcp::CallFailure::Contract(error)) => return Err(error),
            }
            delayed = false;
        }
    }
    .await;
    // Delegation/self-wait can commit the very StopAttempt which interrupts its
    // reply or original lookup. Reconcile that original receipt once, bounded by
    // the accepted MCP client timeout, even though dispatch authority has ended.
    // A stop is not resend authority; complete contract errors never enter here.
    let recovered = if stopped_for_receipt
        && dispatched
        && matches!(
            call.tool.as_str(),
            "collab_task_delegate" | "collab_task_wait" | "collab_task_assign"
        ) {
        match client.get_call_observed(&call).await {
            Ok(Some(value))
                if value.requires_quiescence
                    && call_requires_quiescence(&call, &value.reply.outcome) =>
            {
                Ok(value)
            }
            Ok(_) => Err("stopped collaboration call has no original quiescence receipt".into()),
            Err(error) => Err(error.into_message()),
        }
    } else {
        recovered
    };
    let observed = match recovered {
        Ok(value) => value,
        Err(error) => {
            if dispatched {
                reporter.provider_uncertain.store(true, Ordering::Relaxed);
                return Err(format!("collaboration call outcome uncertain: {error}"));
            }
            return Err(error);
        }
    };
    // Retention/check failures never re-enter the dispatch loop.
    if let Err(error) = retain_reply(&mut journal, &path, observed) {
        reporter.provider_uncertain.store(true, Ordering::Relaxed);
        return Err(format!(
            "collaboration receipt retention uncertain: {error}"
        ));
    }
    Ok(journal)
}
// Traceability scope: Continuation output is encoded from the exact retained outcome and validated original commit context, not a fresh current read.
//= spec/impl/v2/implementation.md#native-tool-profiles-and-continuations
//# A native model continuation MUST use the recorded outcome of its corresponding tool call.
fn output(journal: &mutations::Journal, input: &TaskInput) -> Result<String, String> {
    let Operation::CallCollaboration { call } = &journal.request else {
        return Err("collaboration journal type".into());
    };
    let outcome = journal
        .outcome
        .as_ref()
        .ok_or("collaboration outcome absent")?;
    let context = journal
        .outcome_context
        .as_ref()
        .ok_or("collaboration commit absent")?;
    context.validate(&journal.request, outcome)?;
    // Original outcome/commit is supplied unchanged. The current lookup
    // revision is not substituted for the historical result's source revision.
    let value = serde_json::json!({"operation_id":call.operation_id,"logical_call_id":call.logical_call_id,
        "outcome":outcome,"commit":context.commit});
    // The complete tool envelope is input to the next model request. The loop
    // separately bounds its JSON framing and the accumulated context; final
    // task output retains its own allowance.
    bounded_json_size(&value, input.execution_limits()?.native.context_bytes)
        .map_err(|_| "native collaboration result bound: native_context_bytes")?;
    let encoded = serde_json::to_string(&value).map_err(|e| e.to_string())?;
    if contains_credential(&encoded, &configured_credentials(&journal.limits)?) {
        return Err("collaboration result contains configured credential".into());
    }
    Ok(encoded)
}
async fn quiesce(
    reporter: &Reporter,
    state: &Path,
    journal: &mut mutations::Journal,
    path: &Path,
) -> Result<AttemptExit, String> {
    if !requires_quiescence(journal) {
        return Err("collaboration handoff has no committed caller quiescence".into());
    }
    // Recovery can reach this path without invoke. The exact delegation or
    // self-wait outcome must be durable before the stopped observation releases
    // the original execution slot. A wait on another owned task does not qualify.
    if let Err(error) = mutations::persist(path, journal) {
        reporter.provider_uncertain.store(true, Ordering::Relaxed);
        return Err(format!(
            "collaboration receipt retention uncertain: {error}"
        ));
    }
    if let (Operation::CallCollaboration { call }, Some(outcome)) =
        (&journal.request, &journal.outcome)
    {
        if mcp::assignment_quiescence(call, outcome)
            || mcp::work::requires_quiescence(call, outcome)
        {
            // The exact committed self-wait, like self-assignment, ends this
            // caller's turn. Close it before Stop releases a fresh attempt;
            // question/review resolution is not a delegated child result that
            // would authorize the successor to close the prior caller's turn.
            charter::close_assignment(reporter, state, &journal.limits, call).await?;
        }
    }
    if !native_registry_reconciled(state, &reporter.attempt, &journal.limits).await?
        || native::validation_cleanup_pending() != 0
    {
        return Err("native collaboration handoff cleanup unverified".into());
    }
    if !reporter
        .send_identified(
            match &journal.request {
                Operation::CallCollaboration { call } if call.tool == "collab_task_wait" => {
                    format!(
                        "{}-collaboration-workflow-wait-stopped",
                        reporter.attempt.id
                    )
                }
                Operation::CallCollaboration { call } if call.tool == "collab_task_assign" => {
                    format!("{}-collaboration-assignment-stopped", reporter.attempt.id)
                }
                _ => format!("{}-collaboration-delegation-stopped", reporter.attempt.id),
            },
            Observation::ProcessTreeStopped,
        )
        .await?
    {
        return Ok(AttemptExit::DelegationPending);
    }
    mutations::consume(path, journal)?;
    Ok(AttemptExit::Finished)
}

async fn completion_hold(
    reporter: &Reporter,
    state: &Path,
    control: &Control,
) -> Result<AttemptExit, String> {
    completion_hold_reason(reporter, state, control, None).await
}
async fn completion_hold_reason(
    reporter: &Reporter,
    state: &Path,
    control: &Control,
    error: Option<String>,
) -> Result<AttemptExit, String> {
    // Refusal remains a controller hold. Do not turn it into a failed result.
    let _ = reporter.send(Observation::Progress {
        text: "Voluntary completion is held".into(),
        output: match error {
            Some(error)=>scrub(error,reporter.output_limit,&control.runtime_limits),
            None=>"The recorded completion decision or input acknowledgment requires reconciliation; no continuation was replayed.".into(),
        },
        truncated: false,
    }).await;
    if reporter.provider_uncertain.load(Ordering::Relaxed) {
        let _ = reporter
            .send(Observation::ExecutionUncertain {
                message: "Completion continuation provider outcome requires reconciliation".into(),
            })
            .await;
    }
    if !native_registry_reconciled(state, &reporter.attempt, &control.runtime_limits).await?
        || native::validation_cleanup_pending() != 0
    {
        return Ok(AttemptExit::DelegationPending);
    }
    Ok(
        if reporter
            .send_identified(
                format!("{}-completion-held-stopped", reporter.attempt.id),
                Observation::ProcessTreeStopped,
            )
            .await?
        {
            AttemptExit::Finished
        } else {
            AttemptExit::DelegationPending
        },
    )
}

//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A complete model reply that violates its declared output format MUST be retried with the specific validation error within the remaining request allowance and deadline.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A complete validated provider terminal response identifying an output-cap truncation MUST be continued from retained output within the remaining request allowance and deadline.
pub(super) async fn execute(
    config: &WorkerConfig,
    input: &TaskInput,
    control: &Control,
    reporter: &Reporter,
    continuation: Option<&ChildOutcome>,
    completion_resume: Option<&crate::governance_completion::CompletionResume>,
    state: &Path,
    owner_lock: Arc<std::fs::File>,
) -> Result<AttemptExit, String> {
    let inference = input.definition.required_inference()?;
    input
        .definition
        .execution_profile
        .as_ref()
        .ok_or("template_profile_unbound")?;
    crate::execution_profile::validate(input)?;
    // Traceability scope: Native provider-tool capabilities are projected from the explicit accepted definition profile; absence refuses before provider work.
    //= spec/impl/v2/implementation.md#native-tool-profiles-and-continuations
    //# An agent definition MUST explicitly configure its native provider-tool capabilities.
    let Some(
        ExecutionProfile::CollaborationV1 { web_search }
        | ExecutionProfile::CollaborationV2 { web_search , .. },
    ) = &input.definition.execution_profile
    else {
        return Err("collaboration profile absent".into());
    };
    if input.definition.executor != ExecutorKind::Native {
        return Err("native profile executor mismatch".into());
    }
    boxed_execution(|| current(reporter, input, control)).await?;
    let client = mcp::Client::new_for_profile(
        &config.controller_url,
        caller(&reporter.attempt),
        (*control.runtime_limits).clone(),
        input
            .definition
            .execution_profile
            .clone()
            .ok_or("collaboration profile absent")?,
    )?;
    let mut stopped = control.clone();
    let tools = tokio::select! {biased;
        _=stopped.stopped()=>return Err("native collaboration discovery cancelled".into()),
        value=client.tools()=>value?,
    };
    boxed_execution(|| current(reporter, input, control)).await?;
    let profile = native::NativeLoopProfile {
        functions: tools
            .into_iter()
            .map(|t| native::NativeFunction {
                name: t.name,
                description: t.description,
                parameters: t.input_schema,
            })
            .collect(),
        web_search: web_search.as_ref().map(|s| native::NativeWebSearch {
            max_tool_calls: s.max_tool_calls,
        }),
    };
    let mut rendered = input.clone();
    if let Some(child) = continuation {
        rendered.instruction = crate::delegation::continuation_instruction(input, child)?;
    }
    track_native_launch(state, &reporter.attempt, &control.runtime_limits)?;
    let host = native::ValidationHost::with_owner_lock(
        config.validation_executable.clone(),
        native_registry(state, &reporter.attempt),
        owner_lock,
        Arc::new(input.execution_limits()?.native.clone()),
        control.validation_runtime.clone(),
    )
    .with_request_attempt(&reporter.attempt);
    let cfg = native::NativeConfig {
        endpoint: config.native_endpoint.clone(),
        api_key_env: "OPENAI_API_KEY".into(),
        model: inference.model.clone(),
    };
    let workflow_error = |error: String| {
        if !matches!(
            super::collaboration_notice::unresolved(state, &control.runtime_limits),
            Ok(false)
        ) {
            reporter.provider_uncertain.store(true, Ordering::Relaxed);
        }
        error
    };
    let mut workflow_notices = match control.runtime_limits.workflow.as_ref().filter(|_| {
        matches!(
            input.definition.execution_profile,
            Some(ExecutionProfile::CollaborationV2 { .. })
        )
    }) {
        Some(policy) => Some(super::collaboration_notice::Host::open(
            reporter.client.clone(),
            state,
            &caller(&reporter.attempt),
            &reporter.attempt.session_id,
            &input.definition.id,
            policy,
        )?),
        None => None,
    };
    // One context per actual native attempt. A retained ambiguous append is
    // never resubmitted by this loop; normal attempt recovery remains fenced.
    let workflow_context = reporter.attempt.id.clone();
    let completion_host = if control.runtime_limits.governance.is_some() {
        Some(completion::Host::open(
            reporter.client.clone(),
            caller(&reporter.attempt),
            &reporter.attempt.session_id,
            state,
        )?)
    } else {
        None
    };
    let mut pending_completion: Option<completion::Input> = None;
    let mut previous_completion: Option<native::NativeLoopFinal> = None;
    let mut paired_workflow_notice: Option<super::collaboration_notice::Notice> = None;
    let mut requests = Vec::<contract::NativeRequestEvidence>::new();
    let mut usage = Usage::default();
    let mut previous: Option<(native::NativeLoopCall, String)> = None;
    let mut previous_repair: Option<native::NativeLoopRepair> = None;
    let mut previous_cap: Option<native::NativeLoopCap> = None;
    let mut calls = Vec::new();
    let mut index = 1u32;
    if let Some(resume) = completion_resume {
        let Some(host) = completion_host.as_ref() else {
            return boxed_execution(|| completion_hold(reporter, state, control)).await;
        };
        let required = completion::required_events(&resume.evaluation, resume.reset_event.as_ref());
        let prepared = if let Some(workflow) = workflow_notices.as_mut() {
            match boxed_execution(|| workflow.prepare_completion(&required, control)).await {
                Ok(super::collaboration_notice::PrepareDisposition::Notice(notice)) => Some(notice),
                Ok(super::collaboration_notice::PrepareDisposition::NoChanges) => None,
                _ => return boxed_execution(|| completion_hold(reporter, state, control)).await,
            }
        } else {
            None
        };
        let feedback = boxed_execution(|| {
            host.begin_initial(
                resume,
                &workflow_context,
                CompletionTarget::InitialNativeRequest { request_index: 1 },
                prepared.as_ref().map(|notice| notice.batch_id.clone()),
                control,
            )
        })
        .await;
        let Ok(Some(feedback)) = feedback else {
            return boxed_execution(|| completion_hold(reporter, state, control)).await;
        };
        pending_completion = Some(feedback);
        paired_workflow_notice = prepared;
    }
    loop {
        if let Some((call, output)) = &previous {
            let carrier = completion_compaction::CompactionCarrier::Call {
                call: call.clone(),
                output: output.clone(),
            };
            // Request and compaction futures contain their own control/HTTP
            // state; pin them separately instead of nesting them in this frame.
            //= spec/impl/v2/implementation.md#native-charter-changes-preserve-prompt-continuity
            //# A native charter change MUST schedule a low-priority compaction that merges the change into the system prompt.
            match boxed_execution(|| {
                completion_compaction::compact_if_due(
                    reporter, input, state, control, &cfg, &host, &mut usage, &carrier, index,
                )
            })
            .await?
            {
                completion_compaction::CompactionProgress::NotDue => {}
                completion_compaction::CompactionProgress::Blocked => {
                    return Ok(AttemptExit::Blocked);
                }
                completion_compaction::CompactionProgress::Completed {
                    carrier,
                    requests: evidence,
                } => {
                    let completion_compaction::CompactionCarrier::Call { call, output } = carrier
                    else {
                        return Err("call compaction returned another carrier".into());
                    };
                    let last = evidence
                        .last()
                        .ok_or("completed compaction has no original request evidence")?
                        .request_index();
                    index = last.checked_add(1).ok_or("native request overflow")?;
                    previous = Some((call, output));
                    requests.extend(evidence);
                    requests.sort_by_key(contract::NativeRequestEvidence::request_index);
                    bounded_json_size(&requests, input.limits.result_bytes)
                        .map_err(|_| "native compaction evidence exceeds task_result_bytes")?;
                }
            }
        }
        boxed_execution(|| current(reporter, input, control)).await?;
        // Traceability scope: this point follows the preceding serial tool and precedes an already-required native request.
        //= spec/impl/v2/implementation.md#committed-resolutions-reach-their-consumers
        //# A resolution notice MUST be delivered to an active executor at a supported safe input boundary.
        //= spec/impl/v2/implementation.md#committed-resolutions-reach-their-consumers
        //# A resolution notice MUST NOT cancel a running side-effecting tool.
        let workflow_notice = if pending_completion.is_some() {
            paired_workflow_notice.take()
        } else {
            match workflow_notices.as_mut() {
                Some(host) => match boxed_execution(|| host.prepare(control))
                    .await
                    .map_err(&workflow_error)?
                {
                    super::collaboration_notice::PrepareDisposition::Notice(notice) => Some(notice),
                    // This acknowledges only an unrelated bounded scan prefix. It
                    // does not claim the later ordinary events reached this input.
                    super::collaboration_notice::PrepareDisposition::NoChanges
                    | super::collaboration_notice::PrepareDisposition::Deferred => None,
                },
                None => None,
            }
        };
        if pending_completion.as_ref().is_some_and(|input| {
            input.delivery_batch_id()
                != workflow_notice
                    .as_ref()
                    .map(|notice| notice.batch_id.as_str())
        }) {
            return boxed_execution(|| completion_hold(reporter, state, control)).await;
        }
        let workflow_delivery = if let (Some(host), Some(notice)) =
            (workflow_notices.as_mut(), workflow_notice.as_ref())
        {
            match boxed_execution(|| {
                host.begin_append(&notice.batch_id, &workflow_context, control)
            })
            .await
            .map_err(&workflow_error)?
            {
                super::collaboration_notice::AppendDisposition::Ready => true,
                super::collaboration_notice::AppendDisposition::Superseded => false,
                super::collaboration_notice::AppendDisposition::Deferred => {
                    return Err(workflow_error(
                        "workflow delivery original append remains unresolved".into(),
                    ));
                }
            }
        } else {
            false
        };
        if pending_completion.is_some() && workflow_notice.is_some() && !workflow_delivery {
            return boxed_execution(|| completion_hold(reporter, state, control)).await;
        }
        if let Some(feedback) = pending_completion.as_mut() {
            if feedback.mark_dispatch(&control.runtime_limits).is_err() {
                return boxed_execution(|| completion_hold(reporter, state, control)).await;
            }
        }
        // Every retry uses this same retained context and exact prepared
        // notices. Only the physical request index and ordinary reservation move.
        let mut retry_binding = None;
        let mut failed_post_index = 0u32;
        let mut original_charter_turn = None;
        let (step, delivered, delivered_binding, charter_turn) = loop {
            let mut request_diagnostic =
                NativeDiagnosticRequest::preflight(reporter, input, &cfg, index)?;
            match boxed_execution(|| {
                super::native_retry::acquire(reporter, input, control, state, &cfg, index)
            })
            .await?
            {
                super::native_retry::RetryAction::Retry => {}
                super::native_retry::RetryAction::Blocked => return Ok(AttemptExit::Blocked),
            }
            let prepared = async {
                boxed_execution(|| {
                    reserve(
                        reporter,
                        input,
                        control,
                        &usage,
                        index,
                        &mut request_diagnostic,
                    )
                })
                .await?;
                boxed_execution(|| {
                    charter::begin_same_input(
                        reporter,
                        input,
                        state,
                        control,
                        index,
                        original_charter_turn.as_ref(),
                    )
                })
                .await
            }
            .await;
            let turn = match prepared {
                Ok(turn) => turn,
                Err(error) => {
                    let retained = request_diagnostic.unstarted(reporter);
                    let primary = charter::with_secondary::<()>(
                        Err(error),
                        boxed_execution(|| {
                            super::native_retry::settle(
                                reporter,
                                input,
                                state,
                                index,
                                crate::native_retry::NativeRequestOutcome::NotStarted,
                            )
                        })
                        .await,
                        "native NotStarted settlement failed",
                    );
                    let error = charter::with_secondary(
                        primary,
                        boxed_execution(|| {
                            acknowledge_native_diagnostic(reporter, control, retained)
                        })
                        .await,
                        "native terminal diagnostic failed",
                    )
                    .unwrap_err();
                    if pending_completion.is_some() {
                        return boxed_execution(|| {
                            completion_hold_reason(reporter, state, control, Some(error))
                        })
                        .await;
                    }
                    return Err(error);
                }
            };
            let request_host = host.for_request(retry_binding.clone());
            let request_host = match &turn {
                Some(turn) => request_host.with_charter(turn.exchange.clone()),
                None => request_host,
            };
            let prior = previous.as_ref().map(|(call, output)| {
                (
                    call,
                    native::NativeLoopReply {
                        call_id: call.call_id(),
                        output,
                    },
                )
            });
            let mut stopped = control.clone();
            // The exact charter must exist before rendering; admission and
            // debit already committed. Recheck actual authority at final POST.
            let before_send = async {
                boxed_execution(|| current(reporter, input, control))
                    .await
                    .map(|_| ())
            };
            let response = if let Some(cap) = previous_cap.as_ref() {
                boxed_execution(|| {
                    native::execute_loop_step_after_output_cap_at(
                        &rendered,
                        &cfg,
                        &profile,
                        cap,
                        None,
                        workflow_notice
                            .as_ref()
                            .filter(|_| workflow_delivery)
                            .map(|n| n.text.as_str()),
                        Some(index),
                        &request_host,
                        before_send,
                        stopped.stopped(),
                    )
                })
                .await
            } else if let Some(repair) = previous_repair.as_ref() {
                boxed_execution(|| {
                    native::execute_loop_step_after_format_at(
                        &rendered,
                        &cfg,
                        &profile,
                        repair,
                        None,
                        workflow_notice
                            .as_ref()
                            .filter(|_| workflow_delivery)
                            .map(|n| n.text.as_str()),
                        Some(index),
                        &request_host,
                        before_send,
                        stopped.stopped(),
                    )
                })
                .await
            } else if let (Some(final_response), Some(feedback)) =
                (previous_completion.as_ref(), pending_completion.as_ref())
            {
                boxed_execution(|| {
                    native::execute_loop_step_after_completion_at(
                        &rendered,
                        &cfg,
                        &profile,
                        final_response,
                        feedback.text(),
                        None,
                        workflow_notice
                            .as_ref()
                            .filter(|_| workflow_delivery)
                            .map(|n| n.text.as_str()),
                        Some(index),
                        &request_host,
                        before_send,
                        stopped.stopped(),
                    )
                })
                .await
            } else if let Some(feedback) = pending_completion.as_ref() {
                boxed_execution(|| {
                    native::execute_loop_step_with_completion_input_at(
                        &rendered,
                        &cfg,
                        &profile,
                        feedback.text(),
                        None,
                        workflow_notice
                            .as_ref()
                            .filter(|_| workflow_delivery)
                            .map(|n| n.text.as_str()),
                        Some(index),
                        &request_host,
                        before_send,
                        stopped.stopped(),
                    )
                })
                .await
            } else {
                boxed_execution(|| {
                    native::execute_loop_step_with_host_inputs_at(
                        &rendered,
                        &cfg,
                        &profile,
                        prior,
                        None,
                        workflow_notice
                            .as_ref()
                            .filter(|_| workflow_delivery)
                            .map(|n| n.text.as_str()),
                        Some(index),
                        &request_host,
                        before_send,
                        stopped.stopped(),
                    )
                })
                .await
            };
            if response.provider_outcome == native::ProviderOutcome::Unknown {
                reporter.provider_uncertain.store(true, Ordering::Relaxed);
            }
            let retained = request_diagnostic.retain(
                reporter,
                &request_host,
                response.provider_outcome,
                response.diagnostic.as_ref(),
            );
            let recorded =
                boxed_execution(|| charter::record(reporter, input, state, control, turn.as_ref()))
                    .await;
            if let native::ProviderOutcome::Rejected(rejection) = response.provider_outcome {
                let diagnostic = response.result.as_ref().err().cloned().unwrap_or_else(|| {
                    "native confirmed rejection carried a completed response".into()
                });
                let usage_result = boxed_execution(|| {
                    observed_usage(
                        reporter,
                        &mut usage,
                        index,
                        response.usage,
                        response.provider_outcome,
                    )
                })
                .await;
                let usage_result = charter::with_secondary(
                    usage_result,
                    boxed_execution(|| acknowledge_native_diagnostic(reporter, control, retained))
                        .await,
                    "native terminal diagnostic failed",
                );
                let binding = match request_host.request_binding().and_then(|value| {
                    value
                        .ok_or_else(|| "native rejection has no actual request binding".to_string())
                }) {
                    Ok(binding) => binding,
                    Err(error) => {
                        let result = charter::with_secondary::<()>(
                            Err(format!("{diagnostic}; {error}")),
                            usage_result,
                            "native usage recording failed",
                        );
                        let result = charter::with_secondary(
                            result,
                            recorded,
                            "native charter recording failed",
                        );
                        let error = charter::with_secondary(
                            result,
                            boxed_execution(|| {
                                super::native_retry::settle(
                                    reporter,
                                    input,
                                    state,
                                    index,
                                    crate::native_retry::NativeRequestOutcome::Rejected {
                                        rejection,
                                    },
                                )
                            })
                            .await,
                            "native rejected health settlement failed",
                        )
                        .unwrap_err();
                        if pending_completion.is_some() {
                            return boxed_execution(|| {
                                completion_hold_reason(reporter, state, control, Some(error))
                            })
                            .await;
                        }
                        return Err(error);
                    }
                };
                if let Err(error) = usage_result {
                    let primary = charter::with_secondary::<()>(
                        Err(error),
                        recorded,
                        "native charter recording failed",
                    );
                    let error = charter::with_secondary(
                        primary,
                        boxed_execution(|| {
                            super::native_retry::settle(
                                reporter,
                                input,
                                state,
                                index,
                                crate::native_retry::NativeRequestOutcome::Rejected { rejection },
                            )
                        })
                        .await,
                        "native rejected health settlement failed",
                    )
                    .unwrap_err();
                    if pending_completion.is_some() {
                        return boxed_execution(|| {
                            completion_hold_reason(reporter, state, control, Some(error))
                        })
                        .await;
                    }
                    return Err(error);
                }
                failed_post_index = failed_post_index
                    .checked_add(1)
                    .ok_or("native rejected input count overflow")?;
                let action = boxed_execution(|| {
                    super::native_retry::rejected(
                        reporter,
                        input,
                        control,
                        state,
                        &cfg,
                        index,
                        rejection,
                        &binding,
                        &diagnostic,
                        failed_post_index,
                    )
                })
                .await
                .map_err(|error| format!("{diagnostic}; native retry failed: {error}"));
                let action =
                    charter::with_secondary(action, recorded, "native charter recording failed");
                let action = charter::with_secondary(
                    action,
                    if response.result.is_err() {
                        Ok(())
                    } else {
                        Err("rejection carried a completed response".into())
                    },
                    "native rejection shape",
                );
                match action {
                    Ok(super::native_retry::RetryAction::Blocked) => {
                        return Ok(AttemptExit::Blocked);
                    }
                    Ok(super::native_retry::RetryAction::Retry) => {}
                    Err(error) if pending_completion.is_some() => {
                        return boxed_execution(|| {
                            completion_hold_reason(reporter, state, control, Some(error))
                        })
                        .await;
                    }
                    Err(error) => return Err(error),
                }
                if !rejection.kind.same_input_retryable() {
                    return Err("fatal native rejection cannot authorize retry".into());
                }
                let operation_id = contract::rejection_id(&reporter.attempt.id, index);
                if let Some(feedback) = pending_completion.as_mut() {
                    if feedback
                        .record_native_rejection(
                            &binding,
                            crate::governance_completion::NativeCompletionRejection {
                                request_index: index,
                                operation_id: operation_id.clone(),
                            },
                            input.limits.model_requests,
                            input.execution_limits()?.native.request_bytes,
                            &control.runtime_limits,
                        )
                        .is_err()
                    {
                        return boxed_execution(|| completion_hold(reporter, state, control)).await;
                    }
                }
                requests.push(contract::NativeRequestEvidence::ConfirmedRejection {
                    request_index: index,
                    operation_id,
                });
                requests.sort_by_key(contract::NativeRequestEvidence::request_index);
                if bounded_json_size(&requests, input.limits.result_bytes).is_err() {
                    if pending_completion.is_some() {
                        return boxed_execution(|| completion_hold(reporter, state, control)).await;
                    }
                    return Err("native request evidence exceeds task_result_bytes".into());
                }
                if original_charter_turn.is_none() {
                    original_charter_turn = turn;
                }
                retry_binding = Some(binding);
                index = index
                    .checked_add(1)
                    .ok_or("native request index overflow")?;
                continue;
            }
            let outcome = match response.provider_outcome {
                native::ProviderOutcome::Completed | native::ProviderOutcome::OutputCapped => {
                    crate::native_retry::NativeRequestOutcome::Healthy
                }
                native::ProviderOutcome::Unknown => {
                    crate::native_retry::NativeRequestOutcome::Unknown
                }
                native::ProviderOutcome::NotStarted => {
                    crate::native_retry::NativeRequestOutcome::NotStarted
                }
                native::ProviderOutcome::Rejected(_) => unreachable!("handled above"),
            };
            let settlement = boxed_execution(|| {
                super::native_retry::settle(reporter, input, state, index, outcome)
            })
            .await;
            let delivered = response.result.is_ok()
                && matches!(
                    response.provider_outcome,
                    native::ProviderOutcome::Completed | native::ProviderOutcome::OutputCapped
                );
            let delivered_binding = if delivered {
                request_host.request_binding()
            } else {
                Ok(None)
            };
            let result = boxed_execution(|| observed(reporter, &mut usage, index, response)).await;
            let result = charter::with_secondary(
                result,
                boxed_execution(|| acknowledge_native_diagnostic(reporter, control, retained))
                    .await,
                "native terminal diagnostic failed",
            );
            let result =
                charter::with_secondary(result, settlement, "native health settlement failed");
            let result =
                charter::with_secondary(result, recorded, "native charter recording failed");
            let result = match (result, delivered_binding) {
                (Ok(step), Ok(binding)) => Ok((step, binding)),
                (Err(error), Ok(_)) => Err(error),
                (Ok(_), Err(error)) => Err(error),
                (Err(original), Err(error)) => Err(format!(
                    "{original}; native delivered input binding failed: {error}"
                )),
            };
            let (step, delivered_binding) = match result {
                Ok(value) => value,
                Err(error) if pending_completion.is_some() => {
                    return boxed_execution(|| {
                        completion_hold_reason(reporter, state, control, Some(error))
                    })
                    .await;
                }
                Err(error) => return Err(error),
            };
            break (step, delivered, delivered_binding, turn);
        };
        let mut delivery_ack_operation_id = None;
        if workflow_delivery && delivered {
            let host = workflow_notices.as_mut().unwrap();
            let batch = &workflow_notice.as_ref().unwrap().batch_id;
            if pending_completion.is_some() {
                match boxed_execution(|| {
                    host.appended_with_receipt(batch, &workflow_context, control)
                })
                .await
                {
                    Ok(id) => delivery_ack_operation_id = Some(id),
                    Err(_) => {
                        return boxed_execution(|| completion_hold(reporter, state, control)).await;
                    }
                }
            } else {
                boxed_execution(|| host.appended(batch, &workflow_context, control))
                    .await
                    .map_err(&workflow_error)?;
            }
        }
        if let Some(mut feedback) = pending_completion.take() {
            if !delivered {
                return boxed_execution(|| completion_hold(reporter, state, control)).await;
            }
            let Some(binding) = delivered_binding.as_ref() else {
                return boxed_execution(|| completion_hold(reporter, state, control)).await;
            };
            let proof = match feedback.native_consumption(
                binding,
                index,
                input.limits.model_requests,
                input.execution_limits()?.native.request_bytes,
                &control.runtime_limits,
            ) {
                Ok(proof) => proof,
                Err(_) => {
                    return boxed_execution(|| completion_hold(reporter, state, control)).await;
                }
            };
            let acknowledged = match completion_host
                .as_ref()
                .ok_or("completion host absent")?
                .acknowledged(
                    &mut feedback,
                    CompletionBoundary::NativeResponse {
                        request_index: index,
                    },
                    delivery_ack_operation_id,
                    Some(proof),
                    control,
                )
                .await
            {
                Ok(value) => value,
                Err(_) => {
                    return boxed_execution(|| completion_hold(reporter, state, control)).await;
                }
            };
            match &feedback.feedback().evaluation.boundary {
                CompletionBoundary::NativeResponse { request_index } => {
                    requests.push(contract::NativeRequestEvidence::CompletionContinuation {
                        evidence: NativeCompletionContinuationEvidence {
                            request_index: *request_index,
                            decision: acknowledged.decision,
                            begin_operation_id: acknowledged.begin_operation_id,
                            acknowledgement_operation_id: acknowledged
                                .acknowledged
                                .ok_or("completion input ACK missing")?
                                .operation_id,
                        },
                    })
                }
                CompletionBoundary::StoppedResult { .. } => {} // Initial input creates no missing response index.
                _ => return boxed_execution(|| completion_hold(reporter, state, control)).await,
            }
            requests.sort_by_key(contract::NativeRequestEvidence::request_index);
            bounded_json_size(&requests, input.limits.result_bytes)
                .map_err(|_| "native completion evidence bound: task_result_bytes")?;
            previous_completion = None;
        }
        previous_repair = None;
        previous_cap = None;
        match step {
            native::NativeLoopStep::OutputCap(cap) => {
                if cap.request_index() != index || !delivered {
                    return Err("native cap original response association mismatch".into());
                }
                let request_binding =
                    delivered_binding.ok_or("native cap request binding absent")?;
                safe_value(cap.output_items(), &control.runtime_limits)?;
                let bytes = serde_json::to_vec(cap.output_items()).map_err(|e| e.to_string())?;
                let output_binding = native::ProviderRequestBinding {
                    bytes: u64::try_from(bytes.len()).map_err(|_| "native cap size overflow")?,
                    sha256: aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &bytes)
                        .as_ref()
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect(),
                };
                let undispatched_calls = u32::try_from(
                    cap.output_items()
                        .as_array()
                        .ok_or("native cap items absent")?
                        .iter()
                        .filter(|v| v["type"] == "function_call")
                        .count(),
                )
                .map_err(|_| "native cap call count overflow")?;
                let operation_id = contract::output_cap_id(&reporter.attempt.id, index);
                if !boxed_execution(|| {
                    reporter.send_identified(
                        operation_id.clone(),
                        Observation::NativeOutputCapped {
                            request_index: index,
                            undispatched_calls,
                            request_binding,
                            output_binding,
                        },
                    )
                })
                .await?
                {
                    return Err("native cap original receipt unacknowledged".into());
                }
                // Close only after the original cap fact proves no call dispatch.
                boxed_execution(|| {
                    charter::close(reporter, state, control, charter_turn.as_ref(), Vec::new())
                })
                .await?;
                requests.push(contract::NativeRequestEvidence::OutputCap {
                    request_index: index,
                    operation_id,
                });
                requests.sort_by_key(contract::NativeRequestEvidence::request_index);
                bounded_json_size(&requests, input.limits.result_bytes)
                    .map_err(|_| "native cap evidence exceeds task_result_bytes")?;
                previous = None;
                previous_completion = None;
                if index >= input.limits.model_requests {
                    boxed_execution(|| {
                        super::native_retry::request_exhausted(reporter, control, state, index)
                    })
                    .await?;
                    return Ok(AttemptExit::Blocked);
                }
                boxed_execution(|| current(reporter, input, control)).await?;
                previous_cap = Some(cap);
                index = index.checked_add(1).ok_or("native cap request overflow")?;
            }
            native::NativeLoopStep::FormatRepair(repair) => {
                if repair.request_index() != index || !delivered {
                    return Err("native format original response association mismatch".into());
                }
                // Input ACK above records actual consumption. This final-text
                // response closes its original charter turn without tool calls.
                boxed_execution(|| {
                    charter::close(reporter, state, control, charter_turn.as_ref(), Vec::new())
                })
                .await?;
                let request_binding =
                    delivered_binding.ok_or("native format request binding absent")?;
                safe_value(repair.output_items(), &control.runtime_limits)?;
                let bytes = serde_json::to_vec(repair.output_items()).map_err(|e| e.to_string())?;
                let output_binding = native::ProviderRequestBinding {
                    bytes: u64::try_from(bytes.len())
                        .map_err(|_| "native format output size overflow")?,
                    sha256: aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &bytes)
                        .as_ref()
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect(),
                };
                let operation_id = contract::format_failure_id(&reporter.attempt.id, index);
                if !boxed_execution(|| {
                    reporter.send_identified(
                        operation_id.clone(),
                        Observation::NativeFormatFailure {
                            request_index: index,
                            issue: *repair.issue(),
                            request_binding,
                            output_binding,
                        },
                    )
                })
                .await?
                {
                    return Err("native format original receipt unacknowledged".into());
                }
                requests.push(contract::NativeRequestEvidence::FormatRepair {
                    request_index: index,
                    operation_id,
                });
                requests.sort_by_key(contract::NativeRequestEvidence::request_index);
                bounded_json_size(&requests, input.limits.result_bytes)
                    .map_err(|_| "native format evidence exceeds task_result_bytes")?;
                previous = None;
                previous_completion = None;
                if index >= input.limits.model_requests {
                    boxed_execution(|| {
                        super::native_retry::request_exhausted(reporter, control, state, index)
                    })
                    .await?;
                    return Ok(AttemptExit::Blocked);
                }
                boxed_execution(|| current(reporter, input, control)).await?;
                previous_repair = Some(repair);
                index = index.checked_add(1).ok_or("native request overflow")?;
            }
            native::NativeLoopStep::Call(mut call) => {
                let mut delivered = Vec::new();
                loop {
                    let call_index = call.call_index();
                    if call.request_index() != index {
                        return Err("native loop request association mismatch".into());
                    }
                    // Refuse a fresh local call before transport if the original
                    // response cannot finish without exceeding its authority.
                    admit_call_boundary(call.name(),call.arguments(),call.has_following_calls(),
                        index,input,&reporter.attempt)?;
                    let invocation = mcp::Invocation {
                        operation_id: contract::call_operation_id(
                            &reporter.attempt.id,
                            index,
                            call_index,
                        ),
                        logical_call_id: call.call_id().into(),
                        caller: caller(&reporter.attempt),
                        tool: call.name().into(),
                        arguments: call.arguments().clone(),
                    };
                    let mut journal = boxed_execution(|| {
                        invoke(
                            &client, invocation, state, reporter, input, control, index, call_index,
                        )
                    })
                    .await?;
                    if requires_quiescence(&journal) {
                        // The model step and every validation helper ended before
                        // this durable child. Do not run a final request here.
                        return match quiesce(
                            reporter,
                            state,
                            &mut journal,
                            &path(state, &reporter.attempt, index, call_index),
                        )
                        .await
                        {
                            Ok(exit) => Ok(exit),
                            Err(error) => {
                                let _ = reporter
                                    .send(Observation::Progress {
                                        text: "Collaboration delegation requires reconciliation"
                                            .into(),
                                        output: scrub(
                                            error,
                                            reporter.output_limit,
                                            &control.runtime_limits,
                                        ),
                                        truncated: false,
                                    })
                                    .await;
                                Ok(AttemptExit::DelegationPending)
                            }
                        };
                    }
                    let encoded = output(&journal, input)?;
                    boxed_execution(|| current(reporter, input, control)).await?;
                    calls.push(NativeCallEvidence {
                        request_index: index,
                        call_index,
                        operation_id: contract::call_operation_id(
                            &reporter.attempt.id,
                            index,
                            call_index,
                        ),
                        logical_call_id: call.call_id().into(),
                    });
                    bounded_json_size(&calls, input.limits.result_bytes)
                        .map_err(|_| "native collaboration evidence bound: task_result_bytes")?;
                    delivered.push(crate::charter::CharterResultReceipt::Collaboration {
                        call_id: call.call_id().into(),
                        operation_id: contract::call_operation_id(
                            &reporter.attempt.id,
                            index,
                            call_index,
                        ),
                    });
                    mutations::consume(
                        &path(state, &reporter.attempt, index, call_index),
                        &mut journal,
                    )?;
                    if let Some(next) = call.next_call(&encoded)? {
                        call = next;
                        continue;
                    }
                    boxed_execution(|| {
                        charter::close(reporter, state, control, charter_turn.as_ref(), delivered)
                    })
                    .await?;
                    previous = Some((call, encoded));
                    index = index.checked_add(1).ok_or("native request overflow")?;
                    break;
                }
            }
            native::NativeLoopStep::Complete {
                result,
                output_items,
                mut continuation,
            } => {
                boxed_execution(|| {
                    charter::close(reporter, state, control, charter_turn.as_ref(), Vec::new())
                })
                .await?;
                boxed_execution(|| current(reporter, input, control)).await?;
                // Check the original bounded observations before projecting
                // public progress; filtering must not bypass either guard.
                safe_value(&output_items, &control.runtime_limits)?;
                let encoded_items =
                    serde_json::to_string(&output_items).map_err(|e| e.to_string())?;
                if encoded_items.len() > input.limits.output_bytes {
                    return Err(
                        "native provider observations exceed output bound: task_output_bytes"
                            .into(),
                    );
                }
                if contains_credential(
                    &encoded_items,
                    &configured_credentials(&control.runtime_limits)?,
                ) {
                    return Err("native output contains configured credential".into());
                }
                let final_response_index = continuation.request_index();
                let mut intervening_requests = Vec::new();
                // Compaction precedes Evaluate: it cannot spend while a
                // completion hold exists, or change an already selected target.
                if completion_host.is_some() {
                    let carrier =
                        completion_compaction::CompactionCarrier::Final(continuation.clone());
                    let next_request_index = continuation.next_request_index()?;
                    match boxed_execution(|| {
                        completion_compaction::compact_if_due(
                            reporter,
                            input,
                            state,
                            control,
                            &cfg,
                            &host,
                            &mut usage,
                            &carrier,
                            next_request_index,
                        )
                    })
                    .await?
                    {
                        completion_compaction::CompactionProgress::NotDue => {}
                        completion_compaction::CompactionProgress::Blocked => {
                            return Ok(AttemptExit::Blocked);
                        }
                        completion_compaction::CompactionProgress::Completed {
                            carrier,
                            requests: evidence,
                        } => {
                            let completion_compaction::CompactionCarrier::Final(next) = carrier
                            else {
                                return Err("final compaction returned another carrier".into());
                            };
                            continuation = next;
                            index = continuation
                                .next_request_index()?
                                .checked_sub(1)
                                .ok_or("native request underflow")?;
                            intervening_requests = evidence;
                            requests.extend(intervening_requests.iter().cloned());
                            requests.sort_by_key(contract::NativeRequestEvidence::request_index);
                            bounded_json_size(&requests, input.limits.result_bytes)
                                .map_err(|_| "native request evidence bound: task_result_bytes")?;
                        }
                    }
                }
                let boundary = CompletionBoundary::NativeResponse {
                    request_index: final_response_index,
                };
                let gate = match completion_host.as_ref() {
                    Some(host) => {
                        boxed_execution(|| host.evaluate(&boundary, &intervening_requests, control))
                            .await
                    }
                    None => Ok(completion::Gate::Admit),
                };
                match gate {
                    Ok(completion::Gate::Admit) => {}
                    Ok(completion::Gate::Blocked(evaluation)) => {
                        let required = completion::required_events(&evaluation, None);
                        // Prepare the exact C batch before selecting it in Begin.
                        // Any canonical compaction has already fixed this index.
                        let next_index = continuation.next_request_index()?;
                        if next_index > input.limits.model_requests {
                            return boxed_execution(|| completion_hold(reporter, state, control))
                                .await;
                        }
                        let prepared = if let Some(host) = workflow_notices.as_mut() {
                            match boxed_execution(|| host.prepare_completion(&required, control))
                                .await
                            {
                                Ok(super::collaboration_notice::PrepareDisposition::Notice(
                                    notice,
                                )) => Some(notice),
                                Ok(super::collaboration_notice::PrepareDisposition::NoChanges) => {
                                    None
                                }
                                _ => {
                                    return boxed_execution(|| {
                                        completion_hold(reporter, state, control)
                                    })
                                    .await;
                                }
                            }
                        } else {
                            None
                        };
                        let feedback = completion_host
                            .as_ref()
                            .ok_or("completion host absent")?
                            .begin(
                                &boundary,
                                &evaluation.decision,
                                &workflow_context,
                                CompletionTarget::NativeRequest {
                                    request_index: next_index,
                                },
                                prepared.as_ref().map(|notice| notice.batch_id.clone()),
                                control,
                            )
                            .await;
                        let Ok(Some(feedback)) = feedback else {
                            return boxed_execution(|| completion_hold(reporter, state, control))
                                .await;
                        };
                        paired_workflow_notice = prepared;
                        pending_completion = Some(feedback);
                        previous_completion = Some(continuation);
                        previous = None;
                        index = next_index;
                        continue;
                    }
                    _ => {
                        return boxed_execution(|| completion_hold(reporter, state, control)).await;
                    }
                }
                let output_items = public_provider_observations(&output_items)?;
                if !boxed_execution(|| {
                    required_report(
                        reporter,
                        input,
                        control,
                        format!("{}-native-provider-output", reporter.attempt.id),
                        Observation::Progress {
                            text: "Native provider output observations".into(),
                            output: output_items,
                            truncated: false,
                        },
                    )
                })
                .await?
                {
                    return Err("native final observations unacknowledged".into());
                }
                if !boxed_execution(|| {
                    required_report(
                        reporter,
                        input,
                        control,
                        contract::completion_id(&reporter.attempt.id),
                        Observation::NativeLoopCompleted {
                            model_requests: index,
                            final_response_index: (final_response_index != index)
                                .then_some(final_response_index),
                            calls,
                            requests,
                        },
                    )
                })
                .await?
                {
                    return Err("native loop completion evidence unacknowledged".into());
                }
                reporter
                    .send(Observation::ExecutorOutcome {
                        success: true,
                        summary: scrub(
                            result.summary.clone(),
                            input.execution_limits()?.native.summary_bytes,
                            &control.runtime_limits,
                        ),
                    })
                    .await?;
                reporter.send(Observation::ProcessTreeStopped).await?;
                reporter
                    .send(Observation::ResultCandidate {
                        checks_passed: true,
                        summary: scrub(
                            result.summary,
                            input.execution_limits()?.native.summary_bytes,
                            &control.runtime_limits,
                        ),
                        checkpoint: None,
                        validation: vec![
                            "native structured output schema: pass".into(),
                            contract::evidence(&reporter.attempt.id),
                            usage.diagnostic(),
                        ],
                        output: Some(result.output),
                    })
                    .await?;
                return Ok(AttemptExit::Finished);
            }
        }
    }
}

/// Startup recovery does not resume a provider conversation or dispatch a new
/// tool. Only a proven original delegated-child receipt can close quiescence.
// Traceability scope: Native journal recovery validates and selects accepted task metadata for its MCP client, journal reads and call allowance.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//# Recovery MUST use the execution limits retained by the task being recovered.
pub(super) async fn recover_delegate(
    config: &WorkerConfig,
    input: &TaskInput,
    reporter: &Reporter,
    state: &Path,
) -> Result<Option<AttemptExit>, String> {
    if !crate::execution_profile::collaboration(input) {
        return Ok(None);
    }
    let dir = directory(state, &reporter.attempt);
    if !dir.exists() {
        return Ok(None);
    }
    let snapshot = input
        .resolved_limits
        .as_deref()
        .ok_or("execution limits unbound")?;
    snapshot.validate_for(input)?;
    // Recovery reads the same accepted policy that created the live journal.
    // Startup metadata may have changed while this attempt was stopped.
    let runtime_limits = snapshot.global_metadata.as_ref();
    let client = mcp::Client::new_for_profile(
        &config.controller_url,
        caller(&reporter.attempt),
        runtime_limits.clone(),
        input
            .definition
            .execution_profile
            .clone()
            .ok_or("collaboration profile absent")?,
    )?;
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(|e| e.to_string())? {
        let path = entry.map_err(|e| e.to_string())?.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            return Err("unexpected collaboration journal entry".into());
        }
        paths.push(path);
        if paths.len()
            > (input.limits.model_requests as usize)
                .checked_mul(input.execution_limits()?.native.function_calls_per_response)
                .ok_or("collaboration journal allowance overflow")?
        {
            return Err("collaboration journal count exceeds allowance: task_model_requests * native_function_calls_per_response".into());
        }
    }
    paths.sort();
    for path in paths {
        let mut journal =
            mutations::read(&path, runtime_limits)?.ok_or("collaboration journal disappeared")?;
        let Operation::CallCollaboration { call } = &journal.request else {
            return Err("collaboration journal operation invalid".into());
        };
        if call.caller != caller(&reporter.attempt) {
            return Err("collaboration journal caller changed".into());
        }
        if !matches!(
            call.tool.as_str(),
            "collab_task_delegate" | "collab_task_wait" | "collab_task_assign"
        ) {
            continue;
        }
        if journal.outcome.is_none() {
            if let Some(original) = client.get_call(call).await? {
                retain_reply(&mut journal, &path, original)?;
            } else {
                continue;
            }
        }
        if requires_quiescence(&journal) {
            return quiesce(reporter, state, &mut journal, &path)
                .await
                .map(Some);
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tool_result_input_tests {
    use super::*;

    // Synthetic component data, not a receipt from the failed live task.
    fn fixture(
        context_bytes: usize,
        global_context_bytes: usize,
    ) -> (TaskInput, mutations::Journal, String) {
        let mut limits = RuntimeLimits::seed().unwrap();
        limits.execution.native.context_bytes = global_context_bytes;
        let mut native = limits.execution.native.clone();
        native.context_bytes = context_bytes;
        let mut input: TaskInput = serde_json::from_value(json!({
            "instruction": "Read the synthetic retained tool result.",
            "definition": {
                "id": "tool-input-agent", "revision": "tool-input-r1", "executor": "native",
                "inference": InferenceSettings::selected(),
                "execution_profile": {"kind": "collaboration_v1"},
                "limit_overrides": {"native": native}
            },
            "repository": null, "acceptance_commands": [],
            "output_schema": {"type": "object"},
            "limits": limits.task_defaults.at(process::now_ms()).unwrap(),
            "dependencies": []
        }))
        .unwrap();
        input.limits.output_bytes = 65_536;
        input.resolved_limits = Some(limits.resolve_task(&input).unwrap());
        let call = mcp::Invocation {
            operation_id: "synthetic-tool-operation".into(),
            logical_call_id: "logical-first".into(),
            caller: mcp::Caller {
                task_id: "synthetic-task".into(),
                attempt_id: "synthetic-attempt".into(),
                generation: 1,
                worker_id: "synthetic-worker".into(),
                incarnation: "one".into(),
            },
            tool: "collab_memory_get".into(),
            arguments: json!({"key": "fixture"}),
        };
        let mut value = json!({
            "operation_id": "synthetic-tool-operation", "logical_call_id": "logical-first",
            "outcome": {"status": "ok", "data": {"kind": "memory", "value": {
                "agent_id": "tool-input-agent", "key": "fixture", "revision": 1,
                "value": "synthetic \"quoted\" \\ input\n\u{03bb}".repeat(256)
            }}},
            "commit": {"operation_id": "synthetic-tool-operation", "controller_revision": 7}
        });
        let padding = 95_028 - serde_json::to_vec(&value).unwrap().len();
        let payload = value["outcome"]["data"]["value"]["value"]
            .as_str()
            .unwrap()
            .to_owned();
        value["outcome"]["data"]["value"]["value"] = json!(payload + &"x".repeat(padding));
        let expected = serde_json::to_string(&value).unwrap();
        assert_eq!(expected.len(), 95_028);
        let mut journal = mutations::prepare(call.operation(), 0, &limits).unwrap();
        let reply = Reply {
            request_id: "synthetic-lookup".into(),
            commit: Some(serde_json::from_value(value["commit"].clone()).unwrap()),
            // A later lookup must not replace the original commit revision.
            observed_controller_revision: Some(13),
            outcome: serde_json::from_value(value["outcome"].clone()).unwrap(),
        };
        journal.outcome_context = CommitContext::from_reply(&reply, &journal.request).unwrap();
        journal.outcome = Some(reply.outcome);
        (input, journal, expected)
    }

    #[test]
    // Traceability scope: Synthetic retained component envelope preserves every byte and the original commit under an accepted definition override; no historical receipt is fabricated.
    //= spec/impl/v2/implementation.md#native-tool-profiles-and-continuations
    //= type=test
    //# A native model continuation MUST use the recorded outcome of its corresponding tool call.
    fn complete_tool_envelope_uses_accepted_context_override() {
        for context_bytes in [95_028, 131_072] {
            let (input, journal, expected) = fixture(context_bytes, 65_536);
            assert_eq!(input.limits.output_bytes, 65_536);
            assert_eq!(
                input.execution_limits().unwrap().native.context_bytes,
                context_bytes
            );
            assert_eq!(journal.limits.execution.native.context_bytes, 65_536);
            let retained = serde_json::to_vec(&journal).unwrap();
            assert_eq!(output(&journal, &input).unwrap(), expected);
            assert_eq!(serde_json::to_vec(&journal).unwrap(), retained);
            assert_eq!(input.limits.output_bytes, 65_536);
        }
    }

    #[test]
    fn tighter_accepted_context_rejects_despite_larger_global_context() {
        let (input, journal, expected) = fixture(95_027, 131_072);
        assert!(expected.len() < journal.limits.execution.native.context_bytes);
        assert_eq!(
            output(&journal, &input).unwrap_err(),
            "native collaboration result bound: native_context_bytes"
        );
    }

    #[test]
    fn tool_envelope_requires_valid_original_commit_context() {
        let (input, journal, _) = fixture(131_072, 65_536);
        let mut missing = journal.clone();
        missing.outcome_context = None;
        assert_eq!(
            output(&missing, &input).unwrap_err(),
            "collaboration commit absent"
        );
        let mut missing = journal.clone();
        missing.outcome = None;
        assert_eq!(
            output(&missing, &input).unwrap_err(),
            "collaboration outcome absent"
        );
        for invalid in 0..4 {
            let mut changed = journal.clone();
            let context = changed.outcome_context.as_mut().unwrap();
            match invalid {
                0 => context.commit.operation_id = "different-operation".into(),
                1 => context.commit.controller_revision = 0,
                2 => context.observed_controller_revision = 6,
                3 => context.commit.controller_revision = u64::MAX,
                _ => unreachable!(),
            }
            assert_eq!(
                output(&changed, &input).unwrap_err(),
                "invalid committed mutation context"
            );
        }
    }

    #[test]
    fn tool_envelope_refuses_unbound_or_inconsistent_execution_snapshot() {
        let (input, journal, _) = fixture(131_072, 65_536);
        let mut unbound = input.clone();
        unbound.resolved_limits = None;
        assert_eq!(
            output(&journal, &unbound).unwrap_err(),
            "execution limits are not bound at admission"
        );
        let mut inconsistent = input;
        inconsistent
            .resolved_limits
            .as_mut()
            .unwrap()
            .values
            .native
            .context_bytes += 1;
        assert_eq!(
            output(&journal, &inconsistent).unwrap_err(),
            "execution limits values differ from their recorded sources"
        );
    }
}

#[cfg(test)]
mod recovery_limits_tests {
    use super::*;

    #[tokio::test]
    // Traceability scope: Authored component regression: a manually resolved task and non-delegation journal distinguish accepted metadata from changed startup metadata. This does not exercise controller admission, worker restart or delegated-call settlement.
    //= spec/impl/v2/implementation.md#limits-are-versioned-metadata
    //= type=test
    //# Recovery MUST use the execution limits retained by the task being recovered.
    async fn recovery_uses_accepted_runtime_for_journal_and_mcp_with_agent_override() {
        let accepted = crate::limits::RuntimeLimits::seed().unwrap();
        let mut native = accepted.execution.native.clone();
        native.function_calls_per_response = 2;
        let mut input: TaskInput = serde_json::from_value(serde_json::json!({
            "instruction": "Recover the original accepted collaboration journal",
            "definition": {
                "id": "recovery-agent", "revision": "agent-r1", "executor": "native",
                "inference": {"model": "gpt-6-astra", "reasoning_effort": "max"},
                "execution_profile": {"kind": "collaboration_v1"},
                "limit_overrides": {"native": native}
            },
            "repository": null, "acceptance_commands": [],
            "output_schema": {"type": "integer"},
            "limits": accepted.task_defaults.at(process::now_ms()).unwrap(),
            "dependencies": []
        }))
        .unwrap();
        input.resolved_limits = Some(accepted.resolve_task(&input).unwrap());
        assert_eq!(
            input
                .execution_limits()
                .unwrap()
                .native
                .function_calls_per_response,
            2
        );
        assert_eq!(
            input
                .resolved_limits
                .as_ref()
                .unwrap()
                .global_metadata
                .as_ref(),
            &accepted
        );
        assert_ne!(
            input.execution_limits().unwrap().native,
            accepted.execution.native
        );

        let state = std::env::temp_dir()
            .join(mutations::fresh_id("native-recovery-accepted-limits").unwrap());
        let attempt = Attempt {
            id: "accepted-attempt".into(),
            session_id: "accepted-session".into(),
            task_id: "accepted-task".into(),
            generation: 1,
            worker_id: "accepted-worker".into(),
            worker_incarnation: "accepted-incarnation".into(),
            phase: AttemptPhase::Running,
            deadline_unix_ms: input.limits.deadline_unix_ms,
            stopped: false,
            uncertain: false,
        };
        std::fs::create_dir_all(directory(&state, &attempt)).unwrap();
        let journal_path = path(&state, &attempt, 1, 1);
        let invocation = mcp::Invocation {
            operation_id: contract::call_operation_id(&attempt.id, 1, 1),
            logical_call_id: "accepted-call".into(),
            caller: caller(&attempt),
            // A non-delegate journal is still read and validated by recovery;
            // it must not cause a lookup, dispatch, or provider continuation.
            tool: "collab_memory_get".into(),
            arguments: serde_json::json!({"key": "note"}),
        };
        mutations::create(
            &journal_path,
            Operation::CallCollaboration {
                call: invocation.clone(),
            },
            0,
            &accepted,
        )
        .unwrap();
        let original = std::fs::read(&journal_path).unwrap();
        assert!(mutations::read(&journal_path, &accepted).unwrap().is_some());

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        for caller_header_bytes in [accepted.interface.mcp_caller_header_bytes, 1] {
            let mut startup = accepted.clone();
            startup.revision = "different-startup-revision".into();
            startup.interface.mcp_caller_header_bytes = caller_header_bytes;
            startup.validate().unwrap();
            assert_eq!(
                mutations::read(&journal_path, &startup).err().as_deref(),
                Some("unsupported worker mutation journal")
            );
            if caller_header_bytes == 1 {
                assert_eq!(
                    mcp::Client::new(&url, caller(&attempt), startup.clone())
                        .err()
                        .as_deref(),
                    Some("caller exceeds configured MCP header bound")
                );
            }
            let config = WorkerConfig {
                limits: Arc::new(startup),
                resource_profile: None,
                controller_url: url.clone(),
                id: attempt.worker_id.clone(),
                incarnation: "replacement-incarnation".into(),
                workspace_root: state.clone(),
                codex_path: "/does-not-exist".into(),
                coding_capacity: 0,
                native_capacity: 1,
                native_endpoint: url.clone(),
                validation_executable: std::env::current_exe().unwrap(),
            };
            let reporter = Reporter {
                resource: None,
                client: ApiClient::new(&url, Arc::new(accepted.clone())).unwrap(),
                config: config.clone(),
                attempt: attempt.clone(),
                outbox: state.join("outbox"),
                sequence: Arc::new(AtomicU64::new(1)),
                executor_seen: Arc::new(AtomicBool::new(false)),
                provider_uncertain: Arc::new(AtomicBool::new(false)),
                uncertainty_retained: Arc::new(AtomicBool::new(false)),
                output_limit: input.limits.output_bytes,
                result_limit: input.limits.result_bytes,
                coding_limits: input.execution_limits().unwrap().coding.clone(),
                outbox_lock: Arc::new(tokio::sync::Mutex::new(())),
            };
            assert!(
                recover_delegate(&config, &input, &reporter, &state)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(std::fs::read(&journal_path).unwrap(), original);

            let mut invalid = input.clone();
            invalid.resolved_limits.as_mut().unwrap().agent_revision =
                "wrong-agent-revision".into();
            assert_eq!(
                recover_delegate(&config, &invalid, &reporter, &state)
                    .await
                    .err()
                    .as_deref(),
                Some("execution limits revision binding mismatch")
            );
        }
        assert!(
            matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
        );
        assert_eq!(std::fs::read(&journal_path).unwrap(), original);
        std::fs::remove_dir_all(state).unwrap();
    }
}

#[cfg(test)]
mod public_provider_observation_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reasoning_is_private_while_public_citations_remain_exact() {
        let reasoning = json!({"type":"reasoning","summary":[{"type":"summary_text","text":"PRIVATE_SUMMARY"}],"encrypted_content":"PRIVATE_ENCRYPTED","content":[{"type":"reasoning_text","text":"PRIVATE_CONTENT"}]});
        let search = json!({"type":"web_search_call","id":"search","status":"completed","action":{"type":"search","query":"synthetic","sources":[{"type":"url","url":"https://example.invalid/source"}]}});
        let message = json!({"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"7","annotations":[{"type":"url_citation","url":"https://example.invalid/source","title":"synthetic","start_index":0,"end_index":1}]}]});
        let original = json!([reasoning, search, message]);
        let original_bytes = original.to_string();
        let public = public_provider_observations(&original).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&public).unwrap(),
            json!([search, message])
        );
        assert!(!public.contains("PRIVATE_"));
        assert_eq!(original.to_string(), original_bytes);
        assert!(original_bytes.contains("PRIVATE_CONTENT"));
    }
}

#[cfg(test)]
mod storage_barrier_tests {
    use super::*;
    use process::storage_tests::{Point, inject};

    // Closed local records exercise the actual native authorization branches.
    // The listener never serves a request; no provider, executor or child runs.
    struct Fixture {
        root: PathBuf,
        input: TaskInput,
        reporter: Reporter,
        control: Control,
        _cancel: watch::Sender<bool>,
        listener: std::net::TcpListener,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
    impl Fixture {
        fn new() -> Self {
            let limits = crate::worker::test_limits();
            let mut input: TaskInput = serde_json::from_value(serde_json::json!({
                "instruction":"Controlled native journal barrier",
                "definition":{"id":"barrier-agent","revision":"one","executor":"native",
                    "inference":{"model":"gpt-6-astra","reasoning_effort":"max"},
                    "execution_profile":{"kind":"collaboration_v1"}},
                "repository":null,"acceptance_commands":[],"output_schema":{"type":"integer"},
                "limits":limits.task_defaults.at(process::now_ms()).unwrap(),"dependencies":[]
            }))
            .unwrap();
            input.resolved_limits = Some(limits.resolve_task(&input).unwrap());
            let root = std::env::temp_dir().join(mutations::fresh_id("native-barrier").unwrap());
            std::fs::create_dir(&root).unwrap();
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let attempt = Attempt {
                id: "barrier-attempt".into(),
                session_id: "barrier-session".into(),
                task_id: "barrier-task".into(),
                generation: 1,
                worker_id: "barrier-worker".into(),
                worker_incarnation: "one".into(),
                phase: AttemptPhase::Running,
                deadline_unix_ms: input.limits.deadline_unix_ms,
                stopped: false,
                uncertain: false,
            };
            let config = WorkerConfig {
                limits: limits.clone(),
                resource_profile: None,
                controller_url: url.clone(),
                id: attempt.worker_id.clone(),
                incarnation: attempt.worker_incarnation.clone(),
                workspace_root: root.clone(),
                codex_path: "/does-not-exist".into(),
                coding_capacity: 0,
                native_capacity: 1,
                native_endpoint: url.clone(),
                validation_executable: std::env::current_exe().unwrap(),
            };
            let reporter = Reporter {
                resource: None,
                client: ApiClient::new(&url, limits.clone()).unwrap(),
                config,
                attempt,
                outbox: root.join("outbox"),
                sequence: Arc::new(AtomicU64::new(1)),
                executor_seen: Arc::new(AtomicBool::new(false)),
                provider_uncertain: Arc::new(AtomicBool::new(false)),
                uncertainty_retained: Arc::new(AtomicBool::new(false)),
                output_limit: input.limits.output_bytes,
                result_limit: input.limits.result_bytes,
                coding_limits: input.execution_limits().unwrap().coding.clone(),
                outbox_lock: Arc::new(tokio::sync::Mutex::new(())),
            };
            let (cancel, rx) = watch::channel(false);
            let worker_limits = Arc::new(limits.worker.clone());
            let control = Control {
                deadline_ms: input.limits.deadline_unix_ms,
                cancel: rx,
                runtime_limits: limits,
                validation_runtime: Arc::new(
                    native::ValidationRuntime::new(worker_limits.clone()).unwrap(),
                ),
                worker_limits,
            };
            Self {
                root,
                input,
                reporter,
                control,
                _cancel: cancel,
                listener,
            }
        }
        fn retained(&self, delegate: bool) -> (PathBuf, mcp::Invocation, mutations::Journal) {
            let call = mcp::Invocation {
                operation_id: contract::call_operation_id(&self.reporter.attempt.id, 1, 1),
                logical_call_id: "barrier-call".into(),
                caller: caller(&self.reporter.attempt),
                tool: if delegate {
                    "collab_task_delegate"
                } else {
                    "collab_memory_get"
                }
                .into(),
                arguments: if delegate {
                    serde_json::json!({"instruction":"configured child"})
                } else {
                    serde_json::json!({"key":"note"})
                },
            };
            let path = path(&self.root, &self.reporter.attempt, 1, 1);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            let mut journal = mutations::create(
                &path,
                Operation::CallCollaboration { call: call.clone() },
                0,
                &self.control.runtime_limits,
            )
            .unwrap();
            let data = if delegate {
                Data::Task(Box::new(Task {
                    id: "barrier-child".into(),
                    session_id: "barrier-child-session".into(),
                    input: self.input.clone(),
                    assigned_agent_id: self.input.definition.id.clone(),
                    status: TaskStatus::Queued,
                    revision: 1,
                    attempt: None,
                    selected_checkpoint: None,
                    result: None,
                    blocked_reason: None,
                    pending_assignment: None,
                    native_retry_block: None,
                    progress: String::new(),
                    recent_output: String::new(),
                    output_truncated: false,
                    parent_task_id: Some(self.reporter.attempt.task_id.clone()),
                    waiting_child_id: None,
                    completed_child: None,
                }))
            } else {
                Data::Acknowledged
            };
            journal.outcome = Some(Outcome::Ok { data });
            journal.outcome_context = Some(CommitContext {
                commit: MutationCommit {
                    operation_id: call.operation_id.clone(),
                    controller_revision: 1,
                },
                observed_controller_revision: 1,
            });
            let fault = inject(&path, Point::DirectorySync, 5, 0);
            assert!(atomic_json(&path, &journal).is_err());
            assert_eq!(fault.hits(), 1);
            drop(fault);
            (path, call, journal)
        }
        fn no_connection(&self) {
            assert!(
                matches!(self.listener.accept(),Err(e) if e.kind()==std::io::ErrorKind::WouldBlock)
            );
        }
    }

    #[test]
    fn self_assignment_preflight_preserves_noop_and_other_task_continuations() {
        let f=Fixture::new();let limit=f.input.limits.model_requests;
        let self_args=serde_json::json!({"task_id":f.reporter.attempt.task_id,"agent_id":"different-agent","expected_revision":1});
        assert!(admit_call_boundary("collab_task_assign",&self_args,true,1,&f.input,&f.reporter.attempt).unwrap_err().contains("not dispatched"));
        assert!(admit_call_boundary("collab_task_assign",&self_args,false,limit,&f.input,&f.reporter.attempt).is_ok());
        let mut noop=self_args.clone();noop["agent_id"]=serde_json::json!(f.input.definition.id);
        assert!(limit>1);
        assert!(admit_call_boundary("collab_task_assign",&noop,true,1,&f.input,&f.reporter.attempt).is_ok());
        assert!(admit_call_boundary("collab_task_assign",&noop,false,limit,&f.input,&f.reporter.attempt).unwrap_err().contains("task_model_requests"));
        let mut other=self_args;other["task_id"]=serde_json::json!("another-task");
        assert!(admit_call_boundary("collab_task_assign",&other,true,1,&f.input,&f.reporter.attempt).is_ok());
        assert!(admit_call_boundary("collab_task_assign",&other,false,limit,&f.input,&f.reporter.attempt).is_err());
        f.no_connection();
    }

    #[test]
    fn self_wait_preflight_requires_final_call_and_preserves_other_task_and_budget() {
        let f=Fixture::new();let limit=f.input.limits.model_requests;
        let args=serde_json::json!({"task_id":f.reporter.attempt.task_id,"expected_revision":1,
            "resolver":{"kind":"question","question_id":"question-1"}});
        assert!(limit>1);
        assert!(admit_call_boundary("collab_task_wait",&args,true,1,&f.input,&f.reporter.attempt)
            .unwrap_err().contains("wait not dispatched"));
        assert!(admit_call_boundary("collab_task_wait",&args,false,1,&f.input,&f.reporter.attempt).is_ok());
        assert!(admit_call_boundary("collab_task_wait",&args,false,limit,&f.input,&f.reporter.attempt)
            .unwrap_err().contains("task_model_requests"));
        let mut other=args;other["task_id"]=serde_json::json!("another-task");
        assert!(admit_call_boundary("collab_task_wait",&other,true,1,&f.input,&f.reporter.attempt).is_ok());
        assert!(admit_call_boundary("collab_task_wait",&other,false,limit,&f.input,&f.reporter.attempt).is_err());
        assert!(admit_call_boundary("collab_task_coordination_get",&other,true,1,&f.input,&f.reporter.attempt).is_ok());
        f.no_connection();
    }

    #[test]
    fn only_exact_pending_self_assignment_quiesces_original_caller() {
        let f=Fixture::new();let (_,mut call,journal)=f.retained(true);
        let Some(Outcome::Ok{data:Data::Task(mut task)})=journal.outcome else{panic!("controlled task")};
        call.tool="collab_task_assign".into();call.arguments=serde_json::json!({"task_id":f.reporter.attempt.task_id,"agent_id":"different-agent","expected_revision":1});
        task.id=f.reporter.attempt.task_id.clone();task.session_id=f.reporter.attempt.session_id.clone();task.parent_task_id=None;
        task.attempt=Some(f.reporter.attempt.clone());task.status=TaskStatus::Blocked;
        let mut target=task.input.definition.clone();target.id="different-agent".into();
        task.pending_assignment=Some(PendingAssignment{operation_id:call.operation_id.clone(),from_session_id:task.session_id.clone(),session:Session{id:"reserved-successor".into(),task_id:task.id.clone(),agent_id:target.id.clone(),definition:target}});
        let outcome=Outcome::Ok{data:Data::Task(task.clone())};
        assert!(call_requires_quiescence(&call,&outcome));
        let mut wrong=call.clone();wrong.caller.attempt_id="another-attempt".into();assert!(!call_requires_quiescence(&wrong,&outcome));
        wrong=call.clone();wrong.operation_id="another-operation".into();assert!(!call_requires_quiescence(&wrong,&outcome));
        wrong=call.clone();wrong.arguments["task_id"]=serde_json::json!("another-task");assert!(!call_requires_quiescence(&wrong,&outcome));
        task.pending_assignment=None;assert!(!call_requires_quiescence(&call,&Outcome::Ok{data:Data::Task(task)}));
        f.no_connection();
    }

    #[tokio::test]
    async fn native_saved_outcome_barrier_precedes_function_output_without_repost() {
        let f = Fixture::new();
        let (path, call, original) = f.retained(false);
        let client = mcp::Client::new(
            &f.reporter.config.controller_url,
            caller(&f.reporter.attempt),
            (*f.control.runtime_limits).clone(),
        )
        .unwrap();
        let fault = inject(&path, Point::FileSync, 28, 0);
        assert!(
            invoke(
                &client,
                call.clone(),
                &f.root,
                &f.reporter,
                &f.input,
                &f.control,
                1,
                1
            )
            .await
            .is_err()
        );
        assert_eq!(fault.hits(), 1);
        assert!(f.reporter.provider_uncertain.load(Ordering::Relaxed));
        f.no_connection();
        drop(fault);
        let recovered = invoke(
            &client,
            call,
            &f.root,
            &f.reporter,
            &f.input,
            &f.control,
            1,
            1,
        )
        .await
        .unwrap();
        assert_eq!(
            serde_json::to_value(&recovered).unwrap(),
            serde_json::to_value(&original).unwrap()
        );
        let durable = mutations::read(&path, &f.control.runtime_limits)
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(durable).unwrap(),
            serde_json::to_value(original).unwrap()
        );
        f.no_connection();
    }

    #[tokio::test]
    async fn native_delegation_recovery_barrier_precedes_stopped_observation() {
        let f = Fixture::new();
        let (path, _, mut journal) = f.retained(true);
        let original = std::fs::read(&path).unwrap();
        let fault = inject(&path, Point::FileSync, 28, 0);
        assert!(
            quiesce(&f.reporter, &f.root, &mut journal, &path)
                .await
                .is_err()
        );
        assert_eq!(fault.hits(), 1);
        assert!(f.reporter.provider_uncertain.load(Ordering::Relaxed));
        assert_eq!(std::fs::read(&path).unwrap(), original);
        assert!(!f.reporter.outbox.exists());
        f.no_connection();
    }
}

#[cfg(test)]
#[path = "native_read_recovery_tests.rs"]
mod read_recovery_tests;
