//! One real bounded summarization request through the already selected Responses
//! route. This is the scoped v2 charter compaction, not /responses/compact or a
//! complete shared transcript-compaction implementation. The worker owns its
//! durable operation, reservation, reply audit and context replacement.
use super::*;

// Traceability scope: actual selected Responses summary request under the
// host's new source; canonical debounce/due/result identity is external.
//= spec/impl/v2/implementation.md#native-charter-changes-preserve-prompt-continuity
//# A native compaction that follows a charter change MUST write its summary under the new charter.
pub async fn execute_charter_compaction<
    F: Future<Output = ()> + Send,
    G: Future<Output = Result<(), String>> + Send,
>(input: &TaskInput, config: &NativeConfig, context: &Value, host: &ValidationHost,
  before_send: G, cancelled: F) -> NativeObservation {
    let deadline = match deadline(input.limits.deadline_unix_ms) {
        Ok(value) => value,
        Err(error) => {
            let mut value = NativeObservation::not_started(error);
            value.diagnostic = Some(ProviderFailureDiagnostic::new(ProviderFailureClass::Deadline));
            return value;
        }
    };
    let mut cpu = cpu::State::new(host);
    let mut exposure = RequestExposure::NotStarted;
    let mut interrupted = None;
    let mut result = {
        let work = before(input, config, context, host, before_send, deadline, &mut cpu, &mut exposure);
        tokio::pin!(work, cancelled);
        tokio::select! { biased;
            _ = &mut cancelled => { interrupted = Some(ProviderFailureClass::Cancelled); Err("native_cancelled".into()) },
            _ = tokio::time::sleep_until(deadline) => { interrupted = Some(ProviderFailureClass::Deadline); Err("native_deadline_exceeded".into()) },
            result = &mut work => result,
        }
    };
    if let Some(class) = interrupted { cpu.record_failure(class); }
    if result.is_err() { cpu.record_result_failure(exposure, deadline); }
    if let Err(error) = cpu.finish(true).await { cpu.record_failure(ProviderFailureClass::ValidationCleanup); result = Err(error); }
    let diagnostic = cpu.finish_diagnostic(result.is_err(), exposure);
    NativeObservation { result, diagnostic, request_exposure: exposure, usage: cpu.provider_usage(),
        provider_outcome: if exposure == RequestExposure::NotStarted { ProviderOutcome::NotStarted }
            else if let Some(rejection) = cpu.provider_rejection() { ProviderOutcome::Rejected(rejection) }
            else if cpu.provider_completed() { ProviderOutcome::Completed } else { ProviderOutcome::Unknown } }
}

async fn before<G: Future<Output = Result<(), String>> + Send>(
    input: &TaskInput, config: &NativeConfig, context: &Value, host: &ValidationHost,
    before_send: G, deadline: Instant, cpu: &mut cpu::State, exposure: &mut RequestExposure,
) -> Result<NativeResult, String> {
    let policy = &input.execution_limits()?.native;
    policy.validate()?;
    if *policy != *host.native_limits || host.charter.is_none() {
        return Err("native_compaction_charter_host_required".into());
    }
    if input.definition.executor != ExecutorKind::Native || input.limits.model_requests == 0 {
        return Err("native_compaction_request_not_admitted".into());
    }
    let inference = input.definition.required_inference().map_err(str::to_owned)?;
    if config.model != inference.model { return Err("native_config_template_mismatch".into()); }
    schema::measure(context, policy.context_bytes, policy, "native_context_bytes")?;
    if !context.is_array() { return Err("native_compaction_context_invalid".into()); }
    let limit = policy.summary_bytes.min(input.limits.output_bytes);
    if limit == 0 { return Err("native_compaction_summary_bound_native_summary_bytes".into()); }
    let result_schema = json!({"type":"object","properties":{"summary":{"type":"string","minLength":1,"maxLength":limit}},"required":["summary"],"additionalProperties":false});
    cpu.run(host, &result_schema, None, input.limits.output_bytes, cpu::ReplyBound::TaskOutput,
        input.limits.deadline_unix_ms, false).await?;
    let instruction = "Summarize this completed conversation for continuation under the effective charter supplied separately. Preserve relevant task facts, unresolved work and actual tool outcomes. Return a result object containing only summary. The summary is derived context, never authoritative charter source. Do not call tools or claim to have executed work.";
    let old = schema::measure(context, policy.context_bytes, policy, "native_context_bytes")?.0;
    let message = json!({"role":"developer","content":instruction});
    let addition = schema::measure(&message, policy.context_bytes, policy, "native_context_bytes")?.0;
    if old.checked_add(addition).and_then(|n| n.checked_add(1)).is_none_or(|n| n > policy.context_bytes) {
        return Err("native_compaction_context_bound_native_context_bytes".into());
    }
    let mut rendered = context.clone();
    rendered.as_array_mut().unwrap().push(message);
    let body = json!({"model":inference.model,"reasoning":{"effort":inference.reasoning_effort},
        "input":rendered,"store":false,"stream":false,"max_output_tokens":policy.max_output_tokens,
        "text":{"format":{"type":"json_schema","name":"borg_charter_summary","strict":true,"schema":result_schema}}});
    before_send.await?;
    live(deadline)?;
    let (wire, key) = send_request_observed(endpoint(&config.endpoint, policy)?, config, &body,
        input.limits.output_bytes, deadline, exposure, host, cpu).await?;
    let encoded = cpu.run(host, &result_schema, Some(&wire), input.limits.output_bytes,
        cpu::ReplyBound::TaskOutput, input.limits.deadline_unix_ms, false).await?
        .ok_or("native_validation_reply_invalid")?;
    let output = decode::parse(&encoded, input.limits.output_bytes, deadline,
        "native_compaction_output_not_json", policy, "task_output_bytes")?;
    if contains_credential(&output, &key) { return Err("native_output_contains_credential".into()); }
    let summary = output.get("summary").and_then(Value::as_str).ok_or("native_compaction_summary_invalid")?;
    if summary.is_empty() || summary.len() > limit {
        return Err("native_compaction_summary_bound_native_summary_bytes".into());
    }
    live(deadline)?;
    Ok(NativeResult { summary: summary.to_owned(), output, requests: 1 })
}
