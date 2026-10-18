//! A fixed two-request Responses function-call workflow. The only tool is
//! executed by the worker against its current controller-owned task.
use super::*;
use crate::limits::NativeLimits;
use crate::protocol::TaskStatus;
use crate::task_tool::{self as contract, ToolObservation};

pub struct CurrentTaskCall {
    call_id: String,
    context: Value,
}
impl CurrentTaskCall {
    pub fn call_id(&self) -> &str {
        &self.call_id
    }
}
pub struct ToolCallObservation {
    pub result: Result<CurrentTaskCall, String>,
    pub request_exposure: RequestExposure,
    pub usage: UsageObservation,
    pub provider_outcome: ProviderOutcome,
    pub diagnostic: Option<ProviderFailureDiagnostic>,
}

enum Phase<'a> {
    Request,
    Continue {
        call: &'a CurrentTaskCall,
        observation: &'a ToolObservation,
    },
}
enum Output {
    Call(CurrentTaskCall),
    Final(NativeResult),
}
struct Observed {
    result: Result<Output, String>,
    request_exposure: RequestExposure,
    usage: UsageObservation,
    provider_outcome: ProviderOutcome,
    diagnostic: Option<ProviderFailureDiagnostic>,
}

//= spec/impl/v2/implementation.md#the-native-executor-is-bounded
//# The native executor MUST support a bounded direct tool invocation followed by a model continuation.
pub async fn request_current_task<
    F: Future<Output = ()> + Send,
    G: Future<Output = Result<(), String>> + Send,
>(
    input: &TaskInput,
    config: &NativeConfig,
    host: &ValidationHost,
    before_send: G,
    cancelled: F,
) -> ToolCallObservation {
    let value = execute(input, config, host, Phase::Request, before_send, cancelled).await;
    ToolCallObservation {
        result: value.result.and_then(|result| match result {
            Output::Call(call) => Ok(call),
            Output::Final(_) => Err("native_tool_phase_invalid".into()),
        }),
        request_exposure: value.request_exposure,
        usage: value.usage,
        provider_outcome: value.provider_outcome,
        diagnostic: value.diagnostic,
    }
}

pub async fn continue_current_task<
    F: Future<Output = ()> + Send,
    G: Future<Output = Result<(), String>> + Send,
>(
    input: &TaskInput,
    config: &NativeConfig,
    call: &CurrentTaskCall,
    observation: &ToolObservation,
    host: &ValidationHost,
    before_send: G,
    cancelled: F,
) -> NativeObservation {
    let value = execute(
        input,
        config,
        host,
        Phase::Continue { call, observation },
        before_send,
        cancelled,
    )
    .await;
    NativeObservation {
        result: value.result.and_then(|result| match result {
            Output::Final(result) => Ok(result),
            Output::Call(_) => Err("native_tool_phase_invalid".into()),
        }),
        request_exposure: value.request_exposure,
        usage: value.usage,
        provider_outcome: value.provider_outcome,
        diagnostic: value.diagnostic,
    }
}

async fn execute<F: Future<Output = ()> + Send, G: Future<Output = Result<(), String>> + Send>(
    input: &TaskInput,
    config: &NativeConfig,
    host: &ValidationHost,
    phase: Phase<'_>,
    before_send: G,
    cancelled: F,
) -> Observed {
    let deadline = match super::deadline(input.limits.deadline_unix_ms) {
        Ok(value) => value,
        Err(error) => {
            return Observed {
                result: Err(error),
                request_exposure: RequestExposure::NotStarted,
                usage: UsageObservation::Unknown,
                provider_outcome: ProviderOutcome::NotStarted,
                diagnostic: Some(ProviderFailureDiagnostic::new(ProviderFailureClass::Deadline)),
            };
        }
    };
    let mut cpu = cpu::State::new(host);
    let mut exposure = RequestExposure::NotStarted;
    let mut interrupted = None;
    let mut result = {
        let work = before(
            input,
            config,
            host,
            phase,
            deadline,
            &mut cpu,
            &mut exposure,
            before_send,
        );
        tokio::pin!(work, cancelled);
        tokio::select! { biased;
            _=&mut cancelled=>{ interrupted=Some(ProviderFailureClass::Cancelled); Err("native_cancelled".into()) },
            _=tokio::time::sleep_until(deadline)=>{ interrupted=Some(ProviderFailureClass::Deadline); Err("native_deadline_exceeded".into()) },
            result=&mut work=>result,
        }
    };
    if let Some(class) = interrupted { cpu.record_failure(class); }
    if result.is_err() { cpu.record_result_failure(exposure, deadline); }
    if let Err(error) = cpu.finish(true).await {
        cpu.record_failure(ProviderFailureClass::ValidationCleanup);
        result = Err(error);
    }
    let diagnostic = cpu.finish_diagnostic(result.is_err(), exposure);
    Observed {
        result,
        diagnostic,
        request_exposure: exposure,
        usage: cpu.provider_usage(),
        provider_outcome: if exposure == RequestExposure::NotStarted {
            ProviderOutcome::NotStarted
        } else if let Some(rejection) = cpu.provider_rejection() {
            ProviderOutcome::Rejected(rejection)
        } else if cpu.provider_completed() {
            ProviderOutcome::Completed
        } else {
            ProviderOutcome::Unknown
        },
    }
}

async fn before<G: Future<Output = Result<(), String>> + Send>(
    input: &TaskInput,
    config: &NativeConfig,
    host: &ValidationHost,
    phase: Phase<'_>,
    deadline: Instant,
    cpu: &mut cpu::State,
    exposure: &mut RequestExposure,
    before_send: G,
) -> Result<Output, String> {
    let policy = &input.execution_limits()?.native;
    if input.definition.charter.is_some() && host.charter.is_none() {
        return Err("native_charter_consumer_required".into());
    }
    if *policy != *host.native_limits {
        return Err("native_validation_policy_mismatch".into());
    }
    if !contract::enabled(input) || input.definition.executor != ExecutorKind::Native {
        return Err("native_task_tool_not_configured".into());
    }
    if input.limits.model_requests < 2 {
        return Err("native_task_tool_request_budget_task_model_requests".into());
    }
    if input.repository.is_some() || !input.acceptance_commands.is_empty() {
        return Err("native_has_no_workspace_or_shell".into());
    }
    if input.instruction.is_empty() || input.instruction.len() > policy.context_bytes {
        return Err("native_instruction_bound_native_context_bytes".into());
    }
    if input.limits.output_bytes == 0 || input.limits.output_bytes > policy.output_bytes {
        return Err("native_output_limit_invalid_native_output_bytes".into());
    }
    if input.limits.result_bytes == 0 || input.limits.result_bytes > policy.result_bytes {
        return Err("native_result_limit_invalid_native_result_bytes".into());
    }
    crate::protocol::bounded_json_size(input, policy.input_bytes)
        .map_err(|_| "native_input_bound_native_input_bytes")?;
    let inference = input
        .definition
        .required_inference()
        .map_err(str::to_owned)?;
    input
        .definition
        .execution_profile
        .as_ref()
        .ok_or("template_profile_unbound")?;
    if config.model != inference.model {
        return Err("native_config_template_mismatch".into());
    }
    let url = endpoint(&config.endpoint, policy)?;
    let schema = input
        .output_schema
        .as_ref()
        .ok_or("native_output_schema_missing")?;
    cpu.run(
        host,
        schema,
        None,
        input.limits.output_bytes,
        cpu::ReplyBound::TaskOutput,
        input.limits.deadline_unix_ms,
        false,
    )
    .await?;
    let requesting = matches!(phase, Phase::Request);
    let bound = if requesting {
        policy.context_bytes
    } else {
        input.limits.output_bytes
    };
    let body = match phase {
        Phase::Request => json!({
            "model":inference.model,"reasoning":{"effort":inference.reasoning_effort},"input":input.instruction,"store":false,"stream":false,
            "include":["reasoning.encrypted_content"],"max_output_tokens":policy.max_output_tokens,
            "tools":[{"type":"function","name":contract::NAME,"description":"Read the current task assigned to this worker. No selectors or external access.","strict":true,
                "parameters":{"type":"object","properties":{},"required":[],"additionalProperties":false}}],
            "parallel_tool_calls":false,"tool_choice":{"type":"function","name":contract::NAME}
        }),
        Phase::Continue { call, observation } => {
            if observation.call_id != call.call_id
                || observation.task.definition != input.definition
                || observation.task.deadline_unix_ms != input.limits.deadline_unix_ms
                || observation.task.status != TaskStatus::Active
            {
                return Err("native_tool_observation_mismatch".into());
            }
            let output = contract::encode(observation, input.limits.output_bytes, policy)?;
            let context = call
                .context
                .as_array()
                .ok_or("native_tool_context_invalid")?;
            let mut items = Vec::with_capacity(context.len() + 2);
            items.push(json!({"role":"user","content":input.instruction}));
            items.extend(context.iter().cloned());
            items.push(
                json!({"type":"function_call_output","call_id":call.call_id,"output":output}),
            );
            json!({"model":inference.model,"reasoning":{"effort":inference.reasoning_effort},"input":items,"store":false,"stream":false,
                "max_output_tokens":policy.max_output_tokens,"tools":[],"tool_choice":"none","parallel_tool_calls":false,
                "text":{"format":{"type":"json_schema","name":"borg_native_result","strict":false,"schema":schema}}})
        }
    };
    live(deadline)?;
    // The actual owner read follows the awaited schema preflight. The outer
    // select also covers this read with the original deadline/cancellation.
    before_send.await?;
    live(deadline)?;
    let (wire, key) = send_request_observed(url, config, &body, bound, deadline, exposure, host, cpu).await?;
    if requesting {
        let encoded = cpu
            .run_tool(
                host,
                &wire,
                bound,
                cpu::ReplyBound::NativeContext,
                input.limits.deadline_unix_ms,
            )
            .await?
            .ok_or("native_validation_reply_invalid")?;
        let context = decode::parse(
            &encoded,
            bound,
            deadline,
            "native_tool_context_invalid",
            policy,
            "native_context_bytes",
        )?;
        if contains_credential(&context, &key) {
            return Err("native_output_contains_credential".into());
        }
        Ok(Output::Call(from_context(
            context, bound, deadline, policy,
        )?))
    } else {
        let encoded = cpu
            .run(
                host,
                schema,
                Some(&wire),
                bound,
                cpu::ReplyBound::TaskOutput,
                input.limits.deadline_unix_ms,
                false,
            )
            .await?
            .ok_or("native_validation_reply_invalid")?;
        let output = decode::parse(
            &encoded,
            bound,
            deadline,
            "native_output_not_json",
            policy,
            "task_output_bytes",
        )?;
        if contains_credential(&output, &key) {
            return Err("native_output_contains_credential".into());
        }
        live(deadline)?;
        Ok(Output::Final(NativeResult {
            summary: "Native tool continuation validated against the task schema".into(),
            output,
            requests: 1,
        }))
    }
}

pub(super) fn from_envelope(
    envelope: &Value,
    limit: usize,
    deadline: Instant,
    policy: &NativeLimits,
) -> Result<CurrentTaskCall, String> {
    if envelope.get("status").and_then(Value::as_str) != Some("completed")
        || envelope.get("error").is_some_and(|v| !v.is_null())
    {
        return Err("native_provider_response_not_completed".into());
    }
    let context = envelope.get("output").ok_or("native_response_shape")?;
    schema::measure(context, limit, policy, "native_context_bytes")?;
    from_context(context.clone(), limit, deadline, policy)
}
fn from_context(
    context: Value,
    limit: usize,
    deadline: Instant,
    policy: &NativeLimits,
) -> Result<CurrentTaskCall, String> {
    schema::measure(&context, limit, policy, "native_context_bytes")?;
    let items = context.as_array().ok_or("native_response_shape")?;
    let mut call_id = None;
    for item in items {
        live(deadline)?;
        match item.get("type").and_then(Value::as_str) {
            Some("reasoning") => {
                if item
                    .get("status")
                    .is_some_and(|value| !value.is_null() && value.as_str() != Some("completed"))
                {
                    return Err("native_provider_response_not_completed".into());
                }
            }
            Some("message") => {
                if item.get("role").and_then(Value::as_str) != Some("assistant")
                    || item.get("status").and_then(Value::as_str) != Some("completed")
                {
                    return Err("native_provider_response_not_completed".into());
                }
                let parts = item
                    .get("content")
                    .and_then(Value::as_array)
                    .ok_or("native_response_shape")?;
                if parts.is_empty()
                    || parts.iter().any(|part| {
                        part.get("type").and_then(Value::as_str) != Some("output_text")
                            || part.get("text").and_then(Value::as_str).is_none()
                    })
                {
                    return Err("native_unexpected_provider_action".into());
                }
                // Commentary is context only, never the final task result.
            }
            Some("function_call") => {
                if call_id.is_some()
                    || item.get("name").and_then(Value::as_str) != Some(contract::NAME)
                    || item.get("status").and_then(Value::as_str) != Some("completed")
                {
                    return Err("native_unexpected_tool_call".into());
                }
                let id = item
                    .get("call_id")
                    .and_then(Value::as_str)
                    .ok_or("native_tool_call_identity_invalid")?;
                if id.len() > policy.call_id_bytes {
                    return Err("native_tool_call_identity_invalid_native_call_id_bytes".into());
                }
                if id.is_empty()
                    || id.len() > policy.call_id_bytes
                    || id.chars().any(char::is_control)
                {
                    return Err("native_tool_call_identity_invalid".into());
                }
                let args = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .ok_or("native_tool_arguments_invalid")?;
                let args = decode::parse(
                    args.as_bytes(),
                    policy.task_tool_observation_bytes,
                    deadline,
                    "native_tool_arguments_invalid",
                    policy,
                    "native_task_tool_observation_bytes",
                )?;
                if !args.as_object().is_some_and(|v| v.is_empty()) {
                    return Err("native_tool_arguments_invalid".into());
                }
                call_id = Some(id.to_owned());
            }
            _ => return Err("native_unexpected_provider_action".into()),
        }
    }
    Ok(CurrentTaskCall {
        call_id: call_id.ok_or("native_response_has_no_tool_call")?,
        context,
    })
}
pub(super) fn context(call: CurrentTaskCall) -> Value {
    call.context
}
