//! Explicit collaboration profile orchestration over the existing worker
//! outbox and mutation journals. Provider requests are never retried or resumed
//! after worker loss; a known delegated child can still be quiesced safely.
use super::*;
use crate::{mcp, native, native_loop_contract as contract};

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
    reply.validate_for(&op)?;
    control.check()?;
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
async fn reserve(
    reporter: &Reporter,
    input: &TaskInput,
    control: &Control,
    usage: &Usage,
    index: u32,
) -> Result<(), String> {
    if index == 0 || index > input.limits.model_requests {
        return Err("native model request limit: task_model_requests".into());
    }
    current(reporter, input, control).await?;
    if !reporter
        .send_identified(
            contract::reservation_id(&reporter.attempt.id, index),
            Observation::Usage {
                model_requests: index,
                input_tokens: usage.input,
                output_tokens: usage.output,
            },
        )
        .await?
    {
        return Err("native request reservation unacknowledged; no provider request sent".into());
    }
    current(reporter, input, control).await?;
    Ok(())
}
async fn observed(
    reporter: &Reporter,
    usage: &mut Usage,
    index: u32,
    value: native::NativeLoopObservation,
) -> Result<native::NativeLoopStep, String> {
    // Exposure/outcome is retained before any fallible usage publication.
    if value.provider_outcome == native::ProviderOutcome::Unknown {
        reporter.provider_uncertain.store(true, Ordering::Relaxed);
    }
    if usage.add(value.usage)?
        && !reporter
            .send_identified(
                contract::usage_id(&reporter.attempt.id, index),
                Observation::Usage {
                    model_requests: index,
                    input_tokens: usage.input,
                    output_tokens: usage.output,
                },
            )
            .await?
    {
        return Err("native usage unacknowledged; next request withheld".into());
    }
    if reporter.provider_uncertain.load(Ordering::Relaxed) && value.result.is_ok() {
        return Err("native provider outcome remains uncertain".into());
    }
    value.result
}
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
fn delegated(journal: &mutations::Journal) -> bool {
    matches!((&journal.request,&journal.outcome),
        (Operation::CallCollaboration {call}, Some(Outcome::Ok {data:Data::Task(child)}))
        if call.tool == "collab_task_delegate" && child.parent_task_id.as_deref() == Some(call.caller.task_id.as_str()))
}
// Traceability scope: The native occurrence stores the complete Invocation before lookup or send and refuses any changed retained request binding.
//= spec/impl/v2/implementation.md#native-tool-profiles-and-continuations
//# An execution attempt MUST retain a tool call's exact argument binding before sending a mutation.
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
    let mut journal = if path.exists() {
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
    if let Some(original) = client.get_call(&call).await? {
        if let Err(error) = retain_reply(&mut journal, &path, original) {
            reporter.provider_uncertain.store(true, Ordering::Relaxed);
            return Err(format!(
                "collaboration receipt retention uncertain: {error}"
            ));
        }
        return Ok(journal);
    }
    mutations::persist(&path, &journal)?;
    current(reporter, input, control).await?;
    resource::verify(&reporter.resource)?;
    let mut stopped = control.clone();
    let result = tokio::select! {biased;
        _=stopped.stopped()=>Err("collaboration call interrupted".into()),
        result=client.call(&call)=>result,
    };
    match result {
        Ok(value) => {
            if let Err(e) = retain_reply(&mut journal, &path, value) {
                reporter.provider_uncertain.store(true, Ordering::Relaxed);
                return Err(format!("collaboration receipt retention uncertain: {e}"));
            }
        }
        Err(error) => {
            // A lost response is a lookup of this exact occurrence, never a new
            // logical call or an automatic repetition of the model POST.
            match client.get_call(&call).await {
                Ok(Some(original)) => {
                    if let Err(e) = retain_reply(&mut journal, &path, original) {
                        reporter.provider_uncertain.store(true, Ordering::Relaxed);
                        return Err(format!("collaboration receipt retention uncertain: {e}"));
                    }
                }
                _ => {
                    reporter.provider_uncertain.store(true, Ordering::Relaxed);
                    return Err(format!("collaboration call outcome uncertain: {error}"));
                }
            }
        }
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
    bounded_json_size(&value, input.limits.output_bytes)
        .map_err(|_| "native collaboration result bound: task_output_bytes")?;
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
    if !delegated(journal) {
        return Err("collaboration handoff is not committed delegation".into());
    }
    // Recovery can reach this path without invoke; its retained delegation
    // outcome must be durable before the stopped observation permits the child.
    if let Err(error) = mutations::persist(path, journal) {
        reporter.provider_uncertain.store(true, Ordering::Relaxed);
        return Err(format!(
            "collaboration receipt retention uncertain: {error}"
        ));
    }
    if !native_registry_reconciled(state, &reporter.attempt, &journal.limits).await?
        || native::validation_cleanup_pending() != 0
    {
        return Err("native collaboration handoff cleanup unverified".into());
    }
    if !reporter
        .send_identified(
            format!("{}-collaboration-delegation-stopped", reporter.attempt.id),
            Observation::ProcessTreeStopped,
        )
        .await?
    {
        return Ok(AttemptExit::DelegationPending);
    }
    mutations::consume(path, journal)?;
    Ok(AttemptExit::Finished)
}

pub(super) async fn execute(
    config: &WorkerConfig,
    input: &TaskInput,
    control: &Control,
    reporter: &Reporter,
    continuation: Option<&ChildOutcome>,
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
    let Some(ExecutionProfile::CollaborationV1 { web_search }) =
        &input.definition.execution_profile
    else {
        return Err("collaboration profile absent".into());
    };
    if input.definition.executor != ExecutorKind::Native {
        return Err("native profile executor mismatch".into());
    }
    current(reporter, input, control).await?;
    let client = mcp::Client::new(
        &config.controller_url,
        caller(&reporter.attempt),
        (*control.runtime_limits).clone(),
    )?;
    let mut stopped = control.clone();
    let tools = tokio::select! {biased;
        _=stopped.stopped()=>return Err("native collaboration discovery cancelled".into()),
        value=client.tools()=>value?,
    };
    current(reporter, input, control).await?;
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
    let mut notices = memory_notice::Host::open(
        reporter.client.clone(),
        state,
        &caller(&reporter.attempt),
        &reporter.attempt.session_id,
        &input.definition.id,
        &input.execution_limits()?.memory_notice,
    )?;
    // One context per actual native attempt. A retained ambiguous append is
    // never resubmitted by this loop; normal attempt recovery remains fenced.
    let notice_context = format!("native:{}", reporter.attempt.id);
    let mut usage = Usage::default();
    let mut previous: Option<(native::NativeLoopCall, String)> = None;
    let mut calls = Vec::new();
    let mut index = 1u32;
    loop {
        let mut stopped = control.clone();
        let prior = previous.as_ref().map(|(call, output)| {
            (
                call,
                native::NativeLoopReply {
                    call_id: call.call_id(),
                    output,
                },
            )
        });
        current(reporter, input, control).await?;
        // Traceability scope: The native loop prepares notices between required requests, after the preceding serial tool calls have returned; it does not interrupt a running tool.
        //= spec/impl/v2/implementation.md#shared-memory-changes-reach-sessions
        //# Memory-change notification delivery MUST NOT interrupt a tool that has begun execution.
        let notice = notices.prepare(control).await?;
        let delivery = if let Some(notice) = &notice {
            matches!(
                notices.begin_append(&notice.batch_id, &notice_context)?,
                memory_notice::AppendDisposition::Ready
            )
        } else {
            false
        };
        //= spec/impl/v2/implementation.md#shared-memory-changes-reach-sessions
        //# A memory-change notice MUST NOT initiate a model request by itself.
        // Native-only scope: the existing task loop already requires this
        // bounded request. Notices do not add a loop step or alter tool results.
        let response = native::execute_loop_step_with_notice(
            &rendered,
            &cfg,
            &profile,
            prior,
            notice
                .as_ref()
                .filter(|_| delivery)
                .map(|n| n.text.as_str()),
            &host,
            async { reserve(reporter, input, control, &usage, index).await },
            stopped.stopped(),
        )
        .await;
        // Only a complete validated response establishes that this exact input
        // reached the provider. Exposure/pending alone never advances a cursor.
        let delivered = response.result.is_ok()
            && response.provider_outcome == native::ProviderOutcome::Completed;
        let step = observed(reporter, &mut usage, index, response).await?;
        if delivery && delivered {
            notices
                .appended(&notice.as_ref().unwrap().batch_id, &notice_context, control)
                .await?;
        }
        match step {
            native::NativeLoopStep::Call(mut call) => {
                loop {
                    let call_index = call.call_index();
                    if call.request_index() != index {
                        return Err("native loop request association mismatch".into());
                    }
                    // No new side effect if there is no room for its mandatory
                    // continuation. Delegation is different: it quiesces this
                    // attempt and the controller checks the child's total budget.
                    if index == input.limits.model_requests && call.name() != "collab_task_delegate"
                    {
                        return Err(
                            "native continuation request allowance exhausted: task_model_requests; tool not dispatched"
                                .into(),
                        );
                    }
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
                    let mut journal = invoke(
                        &client, invocation, state, reporter, input, control, index, call_index,
                    )
                    .await?;
                    if delegated(&journal) {
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
                    current(reporter, input, control).await?;
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
                    mutations::consume(
                        &path(state, &reporter.attempt, index, call_index),
                        &mut journal,
                    )?;
                    if let Some(next) = call.next_call(&encoded)? {
                        call = next;
                        continue;
                    }
                    previous = Some((call, encoded));
                    index = index.checked_add(1).ok_or("native request overflow")?;
                    break;
                }
            }
            native::NativeLoopStep::Complete {
                result,
                output_items,
            } => {
                current(reporter, input, control).await?;
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
                let output_items = public_provider_observations(&output_items)?;
                if !reporter
                    .send_identified(
                        format!("{}-native-provider-output", reporter.attempt.id),
                        Observation::Progress {
                            text: "Native provider output observations".into(),
                            output: output_items,
                            truncated: false,
                        },
                    )
                    .await?
                {
                    return Err("native final observations unacknowledged".into());
                }
                if !reporter
                    .send_identified(
                        contract::completion_id(&reporter.attempt.id),
                        Observation::NativeLoopCompleted {
                            model_requests: index,
                            calls,
                        },
                    )
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
    let client = mcp::Client::new(
        &config.controller_url,
        caller(&reporter.attempt),
        runtime_limits.clone(),
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
        if call.tool != "collab_task_delegate" {
            continue;
        }
        if journal.outcome.is_none() {
            if let Some(original) = client.get_call(call).await? {
                retain_reply(&mut journal, &path, original)?;
            } else {
                continue;
            }
        }
        if delegated(&journal) {
            return quiesce(reporter, state, &mut journal, &path)
                .await
                .map(Some);
        }
    }
    Ok(None)
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
