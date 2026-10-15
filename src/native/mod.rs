//! Bounded native Responses executor. Dropping `execute` cancels its local HTTP
//! future. It never retries an uncertain POST or accepts a partial
//! response. Its configured task-tool workflow returns one typed call for the
//! worker to execute; it never runs arbitrary tools. Provider-side cancellation
//! or accounting is not claimed by local drop.
mod cpu;
mod decode;
mod error;
mod host;
mod loop_step;
mod request_observation;
mod schema;
mod step;
mod task_tool;
pub use loop_step::{
    NativeFunction, NativeLoopCall, NativeLoopObservation, NativeLoopProfile, NativeLoopReply,
    NativeLoopStep, NativeWebSearch, execute_loop_step, execute_loop_step_with_notice,
};
pub use task_tool::{
    CurrentTaskCall, ToolCallObservation, continue_current_task, request_current_task,
};
mod usage;
mod validation;
#[cfg(debug_assertions)]
pub use cpu::qualification_hold_direct_value_validation;
#[cfg(debug_assertions)]
pub use cpu::qualification_hold_next_validation;
#[cfg(debug_assertions)]
pub use cpu::qualification_hold_provider_after_usage;
pub use cpu::{validation_cleanup_pending, validation_worker_stdio};
pub use host::{ValidationHost, ValidationRuntime};
pub use step::{NativeStep, NativeStepObservation};
pub use usage::{
    NativeObservation, ProviderOutcome, ProviderTokenUsage, RequestExposure, UsageObservation,
};
pub use validation::{
    JsonValidationError, ValidatedJson, validate_json_with_host, validate_json_with_program,
};

use crate::protocol::{ChildOutcome, ExecutorKind, TaskInput};
use reqwest::{
    Url,
    header::{AUTHORIZATION, HeaderValue},
    redirect::Policy,
};
use serde_json::{Value, json};
use std::{
    future::Future,
    net::IpAddr,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::Instant;

/// Selected supported profile constants. Execution reads the accepted definition.
pub const MODEL: &str = crate::protocol::SELECTED_MODEL;
pub const REASONING_EFFORT: &str = crate::protocol::SELECTED_REASONING_EFFORT;

pub struct NativeConfig {
    /// Complete Responses endpoint, never a secret-bearing URL.
    pub endpoint: String,
    pub api_key_env: String,
    pub model: String,
}

pub struct NativeResult {
    pub summary: String,
    pub output: Value,
    pub requests: u32,
}

/// Executes at most one provider request. `model_requests` is a ceiling, not a
/// target. Worker cancellation should use execute_cancellable so child reaping
/// finishes before execution termination is reported. Direct drop is emergency
/// kill-on-drop only, not a verified termination acknowledgment.
pub async fn execute(input: &TaskInput, config: &NativeConfig) -> Result<NativeResult, String> {
    execute_cancellable(input, config, std::future::pending()).await
}

pub async fn execute_cancellable<F: Future<Output = ()> + Send>(
    input: &TaskInput,
    config: &NativeConfig,
    cancelled: F,
) -> Result<NativeResult, String> {
    execute_observed_cancellable(input, config, cancelled)
        .await
        .result
}

pub async fn execute_observed_cancellable<F: Future<Output = ()> + Send>(
    input: &TaskInput,
    config: &NativeConfig,
    cancelled: F,
) -> NativeObservation {
    if input.native_workflow.is_some() {
        return NativeObservation::not_started("native_step_consumer_required");
    }
    let Ok(program) = std::env::current_exe() else {
        return NativeObservation::not_started("native_validation_unavailable");
    };
    execute_observed_with_validation_program(input, config, &program, cancelled).await
}

/// Host integration/test injection only. The program must be the trusted Borg
/// application binary, never a task field, provider value or guest environment.
pub async fn execute_with_validation_program<F: Future<Output = ()> + Send>(
    input: &TaskInput,
    config: &NativeConfig,
    program: &Path,
    cancelled: F,
) -> Result<NativeResult, String> {
    execute_observed_with_validation_program(input, config, program, cancelled)
        .await
        .result
}

/// Host-only executable injection with observation-preserving cancellation.
pub async fn execute_observed_with_validation_program<F: Future<Output = ()> + Send>(
    input: &TaskInput,
    config: &NativeConfig,
    program: &Path,
    cancelled: F,
) -> NativeObservation {
    if input.native_workflow.is_some() {
        return NativeObservation::not_started("native_step_consumer_required");
    }
    let step =
        execute_step_observed_with_validation_program(input, config, None, program, cancelled)
            .await;
    NativeObservation {
        result: step.result.and_then(|result| match result {
            NativeStep::Complete(result) => Ok(result),
            NativeStep::DelegateCoding { .. } => Err("native_step_consumer_required".into()),
        }),
        request_exposure: step.request_exposure,
        usage: step.usage,
        provider_outcome: step.provider_outcome,
    }
}

pub async fn execute_step_observed_cancellable<F: Future<Output = ()> + Send>(
    input: &TaskInput,
    config: &NativeConfig,
    continuation: Option<&ChildOutcome>,
    cancelled: F,
) -> NativeStepObservation {
    let Ok(program) = std::env::current_exe() else {
        return NativeStepObservation::not_started("native_validation_unavailable");
    };
    execute_step_observed_with_validation_program(input, config, continuation, &program, cancelled)
        .await
}

/// Debug compatibility fixture only; not restart-qualified. Release builds
/// refuse this API because a trusted program path is not a durable registry.
pub async fn execute_step_observed_with_validation_program<F: Future<Output = ()> + Send>(
    input: &TaskInput,
    config: &NativeConfig,
    continuation: Option<&ChildOutcome>,
    program: &Path,
    cancelled: F,
) -> NativeStepObservation {
    if let Err(error) = input.definition.required_inference() {
        return NativeStepObservation::not_started(error);
    }
    if input.definition.execution_profile.is_none() {
        return NativeStepObservation::not_started("template_profile_unbound");
    }
    #[cfg(debug_assertions)]
    {
        let host = match host::FixtureHost::new(program) {
            Ok(host) => host,
            Err(error) => return NativeStepObservation::not_started(error),
        };
        let policy = match input.execution_limits() {
            Ok(limits) => std::sync::Arc::new(limits.native.clone()),
            Err(error) => return NativeStepObservation::not_started(error),
        };
        execute_step_observed_with_host(
            input,
            config,
            continuation,
            &host.0.with_limits(policy),
            cancelled,
        )
        .await
    }
    #[cfg(not(debug_assertions))]
    {
        let _ = (input, config, continuation, program, cancelled);
        NativeStepObservation::not_started("native_validation_host_required")
    }
}

/// Production host injection. The caller owns/reconciles the durable registry.
/// Continuation comes from the exact committed child outcome, not model data.
pub async fn execute_step_observed_with_host<F: Future<Output = ()> + Send>(
    input: &TaskInput,
    config: &NativeConfig,
    continuation: Option<&ChildOutcome>,
    host: &ValidationHost,
    cancelled: F,
) -> NativeStepObservation {
    let deadline = match deadline(input.limits.deadline_unix_ms) {
        Ok(deadline) => deadline,
        Err(error) => return NativeStepObservation::not_started(error),
    };
    let mut cpu = cpu::State::new(host);
    let mut exposure = RequestExposure::NotStarted;
    let mut result = {
        let work = execute_before(
            input,
            config,
            continuation,
            deadline,
            host,
            &mut cpu,
            &mut exposure,
        );
        tokio::pin!(work, cancelled);
        tokio::select! {
            biased;
            _ = &mut cancelled => Err("native_cancelled".into()),
            _ = tokio::time::sleep_until(deadline) => Err("native_deadline_exceeded".into()),
            result = &mut work => result,
        }
    };
    // This is deliberately outside the attempt's timed work: kill immediately,
    // then verify reaping before the caller may report local termination.
    if let Err(error) = cpu.finish(true).await {
        result = Err(error);
    }
    NativeStepObservation {
        result,
        request_exposure: exposure,
        usage: cpu.provider_usage(),
        provider_outcome: if exposure == RequestExposure::NotStarted {
            ProviderOutcome::NotStarted
        } else if cpu.provider_completed() {
            ProviderOutcome::Completed
        } else {
            ProviderOutcome::Unknown
        },
    }
}

fn deadline(deadline_unix_ms: u64) -> Result<Instant, String> {
    let started = Instant::now();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "native_clock_invalid")?;
    let duration = Duration::from_millis(deadline_unix_ms)
        .checked_sub(now)
        .filter(|v| !v.is_zero())
        .ok_or("native_deadline_exceeded")?;
    started
        .checked_add(duration)
        .ok_or_else(|| "native_deadline_invalid".into())
}

fn live(deadline: Instant) -> Result<(), String> {
    if Instant::now() >= deadline {
        Err("native_deadline_exceeded".into())
    } else {
        Ok(())
    }
}

async fn execute_before(
    input: &TaskInput,
    config: &NativeConfig,
    continuation: Option<&ChildOutcome>,
    deadline: Instant,
    host: &ValidationHost,
    cpu: &mut cpu::State,
    exposure: &mut RequestExposure,
) -> Result<NativeStep, String> {
    let policy = &input.execution_limits()?.native;
    policy.validate()?;
    if *policy != *host.native_limits {
        return Err("native_validation_policy_mismatch".into());
    }
    if input.definition.executor != ExecutorKind::Native {
        return Err("native_wrong_executor".into());
    }
    if input.repository.is_some() || !input.acceptance_commands.is_empty() {
        return Err("native_has_no_workspace_or_shell".into());
    }
    //= spec/impl/v2/implementation.md#the-native-executor-is-bounded
    //# A native task MUST declare a finite model-request budget.
    // Native component scope: requires a supplied positive finite u32 budget; one POST maximum, aggregate durable reservation belongs to the worker/controller.
    if input.limits.model_requests == 0 {
        return Err("native_request_budget_exhausted_task_model_requests".into());
    }
    // Native component scope: native output/resource admission only; not all workers or production capacity.
    if input.limits.output_bytes == 0 || input.limits.output_bytes > policy.output_bytes {
        return Err("native_output_limit_invalid_native_output_bytes".into());
    }
    if input.limits.result_bytes == 0 || input.limits.result_bytes > policy.result_bytes {
        return Err("native_result_limit_invalid_native_result_bytes".into());
    }
    if input.instruction.is_empty() || input.instruction.len() > policy.context_bytes {
        return Err("native_instruction_bound_native_context_bytes".into());
    }
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
    //= spec/impl/v2/implementation.md#the-native-executor-is-bounded
    //# A native task MUST declare its output schema.
    // Native component scope: missing schema refuses before any validation child or provider request.
    let output_schema = input
        .output_schema
        .as_ref()
        .ok_or("native_output_schema_missing")?;
    cpu.run(
        host,
        output_schema,
        None,
        input.limits.output_bytes,
        cpu::ReplyBound::TaskOutput,
        input.limits.deadline_unix_ms,
        false,
    )
    .await?;
    let plan = step::plan(input, output_schema, continuation)?;
    let reply_bound = if plan.decision {
        cpu::ReplyBound::TaskOutputWithDecisionEnvelope
    } else {
        cpu::ReplyBound::TaskOutput
    };
    if plan.decision {
        cpu.run(
            host,
            &plan.response_schema,
            None,
            plan.response_bound,
            reply_bound,
            input.limits.deadline_unix_ms,
            false,
        )
        .await?;
    }
    live(deadline)?;
    // The initial decision accepts arbitrary JSON as a complete result, then
    // validates it against the original task schema without relocating refs.
    // Keep that closed decision schema local: the provider rejected this
    // oneOf/boolean schema as invalid. JSON mode changes only
    // generation guidance; the same bounded helper and decoder still validate
    // every decision below. This is a fixed format choice, never a retry.
    let format = if plan.decision {
        json!({"type":"json_object"})
    } else {
        json!({"type":"json_schema", "name":"borg_native_result", "strict":false, "schema":plan.response_schema})
    };
    let body = json!({
        "model": inference.model,
        "reasoning": {"effort": inference.reasoning_effort},
        "input": plan.instruction,
        "store": false,
        "stream": false,
        "max_output_tokens": policy.max_output_tokens,
        "text": {"format": format}
    });
    let (wire, key) = send_request(
        url,
        config,
        &body,
        plan.response_bound,
        deadline,
        exposure,
        host,
    )
    .await?;
    let validated = cpu
        .run(
            host,
            &plan.response_schema,
            Some(&wire),
            plan.response_bound,
            reply_bound,
            input.limits.deadline_unix_ms,
            false,
        )
        .await?
        .ok_or("native_validation_reply_invalid")?;
    let text = std::str::from_utf8(&validated).map_err(|_| "native_validation_reply_invalid")?;
    // Refuse a provider echo of the actual configured credential without putting
    // either the credential or the echoed content in an error or summary.
    if text.contains(&key) {
        return Err("native_output_contains_credential".into());
    }
    let mut output = decode::parse(
        text.as_bytes(),
        plan.response_bound,
        deadline,
        "native_output_not_json",
        policy,
        reply_bound.field(),
    )?;
    if contains_credential(&output, &key) {
        return Err("native_output_contains_credential".into());
    }
    if plan.decision {
        match step::decode_decision(output, input.limits.output_bytes)? {
            step::Decision::Delegate(instruction) => {
                tokio::task::yield_now().await;
                live(deadline)?;
                return Ok(NativeStep::DelegateCoding { instruction });
            }
            step::Decision::Complete(value) => output = value,
        }
        schema::measure(
            &output,
            input.limits.output_bytes,
            policy,
            "task_output_bytes",
        )?;
        let encoded = serde_json::to_vec(&output).map_err(|_| "native_output_not_json")?;
        let actual = cpu
            .run(
                host,
                output_schema,
                Some(&encoded),
                input.limits.output_bytes,
                cpu::ReplyBound::TaskOutput,
                input.limits.deadline_unix_ms,
                true,
            )
            .await?;
        if actual.as_deref() != Some(encoded.as_slice()) {
            return Err("native_validation_reply_invalid".into());
        }
    }
    tokio::task::yield_now().await;
    live(deadline)?;
    Ok(NativeStep::Complete(NativeResult {
        summary: "Native output validated against the task schema".into(),
        output,
        requests: 1,
    }))
}

async fn send_request(
    url: Url,
    config: &NativeConfig,
    request_body: &Value,
    response_bound: usize,
    deadline: Instant,
    exposure: &mut RequestExposure,
    host: &ValidationHost,
) -> Result<(Vec<u8>, String), String> {
    let policy = &host.native_limits;
    policy.validate()?;
    schema::measure(
        request_body,
        policy.request_bytes,
        policy,
        "native_request_bytes",
    )?;
    let body = serde_json::to_vec(request_body).map_err(|_| "native_request_encoding")?;
    if config.api_key_env.len() > policy.api_key_env_bytes {
        return Err("native_key_configuration_invalid_native_api_key_env_bytes".into());
    }
    if config.api_key_env.is_empty()
        || !config
            .api_key_env
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_')
    {
        return Err("native_key_configuration_invalid".into());
    }
    let key = std::env::var(&config.api_key_env).map_err(|_| "native_api_key_unavailable")?;
    if key.len() > policy.api_key_bytes {
        return Err("native_api_key_invalid_native_api_key_bytes".into());
    }
    if key.is_empty() {
        return Err("native_api_key_invalid".into());
    }
    if contains_credential(request_body, &key) {
        return Err("native_input_contains_credential".into());
    }
    let mut authorization =
        HeaderValue::from_str(&format!("Bearer {key}")).map_err(|_| "native_api_key_invalid")?;
    authorization.set_sensitive(true);
    let client = client(url.scheme() == "http", policy)?;
    live(deadline)?;
    let request = client
        .post(url)
        .header(AUTHORIZATION, authorization)
        .header("content-type", "application/json")
        .body(body);
    *exposure = RequestExposure::MayHaveReachedProvider;
    let mut interval = request_observation::Interval::dispatch(host);
    use request_observation::End;
    let mut response = match request.send().await {
        Ok(response) => response,
        Err(error) => {
            interval.finish(End::TransportFailed);
            return Err(transport_failure(&error).into());
        }
    };
    *exposure = RequestExposure::HttpResponseReceived;
    interval.headers(response.status().as_u16());
    if !response.status().is_success() {
        let diagnostic = error::diagnostic(&mut response, &key, deadline, policy).await;
        interval.finish(End::HttpRefused);
        return Err(diagnostic);
    }
    let wire_limit = policy.response_wire_bytes;
    if response_bound > policy.helper_value_bytes {
        return Err("native_response_byte_bound_native_helper_value_bytes".into());
    }
    if response
        .content_length()
        .is_some_and(|n| n > wire_limit as u64)
    {
        interval.finish(End::ByteBound);
        return Err("native_response_byte_bound_native_response_wire_bytes".into());
    }
    let mut wire = Vec::new();
    loop {
        let chunk = match response.chunk().await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => {
                interval.body_eof();
                break;
            }
            Err(_) => {
                interval.finish(End::BodyFailed);
                return Err("native_response_incomplete".into());
            }
        };
        if let Err(error) = live(deadline) {
            interval.finish(End::Deadline);
            return Err(error);
        }
        if wire.len().saturating_add(chunk.len()) > wire_limit {
            interval.finish(End::ByteBound);
            return Err("native_response_byte_bound_native_response_wire_bytes".into());
        }
        wire.extend_from_slice(&chunk);
    }
    if let Err(error) = live(deadline) {
        interval.finish(End::Deadline);
        return Err(error);
    }
    interval.finish(End::BodyComplete);
    Ok((wire, key))
}

// This classification is bound to the pinned reqwest0.13.5 connector profile:
// no request/read timeout, custom connector layer, or streaming request body.
// is_timeout also includes OS timeouts, so the conjunction alone is insufficient.
// The configured hyper-util TCP timer wraps Tokio Elapsed in an I/O timeout;
// reqwest's outer configured connector timer uses its private TimedOut marker.
// Its public predicate checks that marker plus the public alternatives excluded
// below. No error formatting, URL, pattern, body or credential is returned.
fn transport_failure(error: &reqwest::Error) -> &'static str {
    use std::error::Error as _;
    const GENERAL: &str = "native_transport_failed_outcome_unknown";
    if !error.is_connect() || !error.is_timeout() {
        return GENERAL;
    }
    let mut cause = error.source();
    while let Some(error) = cause {
        if error
            .downcast_ref::<hyper::Error>()
            .is_some_and(hyper::Error::is_timeout)
        {
            return GENERAL;
        }
        if let Some(io) = error.downcast_ref::<std::io::Error>() {
            if io.kind() == std::io::ErrorKind::TimedOut
                && !io
                    .get_ref()
                    .is_some_and(|inner| inner.is::<tokio::time::error::Elapsed>())
            {
                return GENERAL;
            }
        }
        cause = error.source();
    }
    "native_transport_failed_outcome_unknown_native_http_connect_timeout_ms"
}

fn client(
    loopback_fixture: bool,
    policy: &crate::limits::NativeLimits,
) -> Result<reqwest::Client, String> {
    client_builder(loopback_fixture, policy)
        .build()
        .map_err(|_| "native_transport_configuration".to_string())
}

fn client_builder(
    loopback_fixture: bool,
    policy: &crate::limits::NativeLimits,
) -> reqwest::ClientBuilder {
    let mut builder = reqwest::Client::builder()
        .redirect(Policy::none())
        .retry(reqwest::retry::never())
        .connect_timeout(Duration::from_millis(policy.http_connect_timeout_ms));
    if loopback_fixture {
        builder = builder.no_proxy();
    }
    builder
}

fn contains_credential(output: &Value, key: &str) -> bool {
    let mut todo = vec![output];
    while let Some(item) = todo.pop() {
        match item {
            Value::String(value) if value.contains(key) => return true,
            Value::Object(items) => {
                if items.keys().any(|value| value.contains(key)) {
                    return true;
                }
                todo.extend(items.values());
            }
            Value::Array(items) => todo.extend(items),
            _ => {}
        }
    }
    false
}

fn endpoint(value: &str, policy: &crate::limits::NativeLimits) -> Result<Url, String> {
    if value.len() > policy.endpoint_bytes {
        return Err("native_endpoint_invalid_native_endpoint_bytes".into());
    }
    let url = Url::parse(value).map_err(|_| "native_endpoint_invalid")?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("native_endpoint_invalid".into());
    }
    let loopback = url
        .host_str()
        .and_then(|host| host.trim_matches(['[', ']']).parse::<IpAddr>().ok())
        .is_some_and(|ip| ip.is_loopback());
    if url.scheme() != "https" && !(url.scheme() == "http" && loopback) {
        return Err("native_endpoint_requires_https".into());
    }
    Ok(url)
}

fn completed_text(
    envelope: &Value,
    limit: usize,
    bound: cpu::ReplyBound,
) -> Result<String, String> {
    if envelope.get("status").and_then(Value::as_str) != Some("completed")
        || envelope.get("error").is_some_and(|x| !x.is_null())
    {
        return Err("native_provider_response_not_completed".into());
    }
    let mut text = String::new();
    let mut messages = 0usize;
    for item in envelope
        .get("output")
        .and_then(Value::as_array)
        .ok_or("native_response_shape")?
    {
        match item.get("type").and_then(Value::as_str) {
            Some("reasoning") => continue,
            Some("message") => {
                messages += 1;
                if messages != 1
                    || item.get("role").and_then(Value::as_str) != Some("assistant")
                    || item.get("status").and_then(Value::as_str) != Some("completed")
                {
                    return Err("native_response_shape".into());
                }
                for part in item
                    .get("content")
                    .and_then(Value::as_array)
                    .ok_or("native_response_shape")?
                {
                    match part.get("type").and_then(Value::as_str) {
                        Some("refusal") => return Err("native_provider_refusal".into()),
                        Some("output_text") => {
                            let fragment = part
                                .get("text")
                                .and_then(Value::as_str)
                                .ok_or("native_response_shape")?;
                            if text.len().saturating_add(fragment.len()) > limit {
                                return Err(format!("native_output_byte_bound_{}", bound.field()));
                            }
                            text.push_str(fragment);
                        }
                        _ => return Err("native_response_shape".into()),
                    }
                }
            }
            _ => return Err("native_unexpected_provider_action".into()),
        }
    }
    if messages != 1 || text.is_empty() {
        return Err("native_response_has_no_output".into());
    }
    Ok(text)
}

#[cfg(test)]
mod connect_limit_diagnostics {
    use super::*;
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

    #[derive(Clone, Copy)]
    enum Resolution {
        Pending,
        OsTimeout,
        Refused,
    }

    struct ControlledResolver {
        resolution: Resolution,
        calls: Arc<AtomicUsize>,
    }

    impl reqwest::dns::Resolve for ControlledResolver {
        fn resolve(&self, _: reqwest::dns::Name) -> reqwest::dns::Resolving {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let resolution = self.resolution;
            Box::pin(async move {
                let kind = match resolution {
                    Resolution::Pending => return std::future::pending().await,
                    Resolution::OsTimeout => std::io::ErrorKind::TimedOut,
                    Resolution::Refused => std::io::ErrorKind::ConnectionRefused,
                };
                Err(Box::new(std::io::Error::from(kind))
                    as Box<dyn std::error::Error + Send + Sync>)
            })
        }
    }

    // This resolver never returns an address, so no DNS or socket request occurs.
    // The reqwest send future and configured connector timeout are real.
    async fn failure(resolution: Resolution, request_timeout: bool) -> reqwest::Error {
        let mut policy: crate::limits::NativeLimits =
            serde_json::from_str(include_str!("../../limits-native-seed.json")).unwrap();
        policy.http_connect_timeout_ms = if request_timeout { 1000 } else { 50 };
        let calls = Arc::new(AtomicUsize::new(0));
        let mut builder = client_builder(true, &policy).dns_resolver(Arc::new(ControlledResolver {
            resolution,
            calls: calls.clone(),
        }));
        if request_timeout {
            // Test-only competing class, deliberately absent from production.
            builder = builder.timeout(Duration::from_millis(50));
        }
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            builder.build().unwrap()
                .post("http://diagnostic-only.invalid/private?synthetic=secret")
                .header(AUTHORIZATION, "Bearer synthetic-secret")
                .body("synthetic-private-body")
                .send(),
        ).await.expect("controlled resolver path must terminate");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        result.expect_err("the controlled resolver never supplies an address")
    }

    #[tokio::test]
    async fn configured_connect_timer_identifies_only_its_metadata_field() {
        let error = failure(Resolution::Pending, false).await;
        assert!(error.is_connect() && error.is_timeout());
        assert_eq!(transport_failure(&error),
            "native_transport_failed_outcome_unknown_native_http_connect_timeout_ms");
    }

    #[tokio::test]
    async fn operating_system_timeout_is_not_misattributed_to_configured_timer() {
        let error = failure(Resolution::OsTimeout, false).await;
        assert!(error.is_connect() && error.is_timeout());
        assert_eq!(transport_failure(&error), "native_transport_failed_outcome_unknown");
    }

    #[tokio::test]
    async fn non_timeout_connection_failure_keeps_generic_safe_diagnostic() {
        let error = failure(Resolution::Refused, false).await;
        assert!(error.is_connect());
        assert!(!error.is_timeout());
        assert_eq!(transport_failure(&error), "native_transport_failed_outcome_unknown");
    }

    #[tokio::test]
    async fn non_connect_request_timeout_is_not_misattributed_to_connect_timer() {
        let error = failure(Resolution::Pending, true).await;
        assert!(error.is_timeout());
        assert!(!error.is_connect());
        assert_eq!(transport_failure(&error), "native_transport_failed_outcome_unknown");
    }
}
