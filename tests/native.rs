use borg_runtime::{
    native::{
        NativeConfig, NativeObservation, NativeResult, NativeStep, NativeStepObservation,
        ProviderOutcome, ProviderTokenUsage, RequestExposure, UsageObservation,
        execute_observed_with_validation_program, execute_step_observed_with_validation_program,
        execute_with_validation_program,
    },
    protocol::{
        AcceptedResult, ChildOutcome, CodingTaskTemplate, Definition, ExecutorKind, Limits,
        NativeWorkflow, Repository, TaskInput, TaskStatus,
    },
};

async fn execute(input: &TaskInput, config: &NativeConfig) -> Result<NativeResult, String> {
    execute_with_validation_program(
        input,
        config,
        std::path::Path::new(env!("CARGO_BIN_EXE_borg-runtime")),
        std::future::pending(),
    )
    .await
}

async fn observed(input: &TaskInput, config: &NativeConfig) -> NativeObservation {
    execute_observed_with_validation_program(
        input,
        config,
        std::path::Path::new(env!("CARGO_BIN_EXE_borg-runtime")),
        std::future::pending(),
    )
    .await
}

fn reported() -> UsageObservation {
    UsageObservation::Reported(ProviderTokenUsage {
        input_tokens: 17,
        output_tokens: 9,
        total_tokens: 26,
    })
}

fn with_usage(mut envelope: Value) -> Value {
    envelope["usage"] = json!({"input_tokens":17,"output_tokens":9,"total_tokens":26,
        "input_tokens_details":{"cached_tokens":4},"output_tokens_details":{"reasoning_tokens":3}});
    envelope
}
use serde_json::{Value, json};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
    task::JoinHandle,
};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
fn require_send<F: std::future::Future + Send>(future: F) -> F {
    future
}
fn metadata() -> borg_runtime::limits::RuntimeLimits {
    borg_runtime::limits::RuntimeLimits::seed().unwrap()
}
// Direct executor fixtures emulate the controller acceptance boundary explicitly.
fn bind(mut input: TaskInput) -> TaskInput {
    input.resolved_limits = Some(metadata().resolve_task(&input).unwrap());
    input
}
fn validation_policy() -> std::sync::Arc<borg_runtime::limits::NativeLimits> {
    std::sync::Arc::new(metadata().execution.native)
}
fn validation_runtime() -> std::sync::Arc<borg_runtime::native::ValidationRuntime> {
    std::sync::Arc::new(
        borg_runtime::native::ValidationRuntime::new(std::sync::Arc::new(metadata().worker))
            .unwrap(),
    )
}
// This test writes the same-binary protocol directly to verify pre-allocation
// refusal. Field order is protocol representation; values come from metadata.
fn helper_policy_frame() -> Vec<u8> {
    let policy = metadata().execution.native;
    let mut frame = Vec::new();
    for value in [
        policy.context_bytes as u64,
        policy.input_bytes as u64,
        policy.output_bytes as u64,
        policy.result_bytes as u64,
        policy.schema_bytes as u64,
        policy.json_depth as u64,
        policy.json_nodes as u64,
        policy.schema_expansion as u64,
        policy.validation_work as u64,
        policy.regex_bytes as u64,
        policy.regex_dfa_bytes as u64,
        policy.request_bytes as u64,
        policy.response_wire_bytes as u64,
        policy.helper_value_bytes as u64,
        policy.max_output_tokens as u64,
        policy.function_count as u64,
        policy.function_name_bytes as u64,
        policy.function_description_bytes as u64,
        policy.call_id_bytes as u64,
        policy.function_calls_per_response as u64,
        policy.memory_notice_bytes as u64,
        policy.endpoint_bytes as u64,
        policy.api_key_env_bytes as u64,
        policy.api_key_bytes as u64,
        policy.http_connect_timeout_ms as u64,
        policy.error_body_bytes as u64,
        policy.error_read_timeout_ms as u64,
        policy.decode_check_bytes as u64,
        policy.helper_error_bytes as u64,
        policy.task_tool_observation_bytes as u64,
        policy.summary_bytes as u64,
        policy.decision_envelope_bytes as u64,
        policy.continuation_envelope_bytes as u64,
        policy.task_tool_identity_bytes as u64,
    ] {
        frame.extend_from_slice(&value.to_be_bytes());
    }
    frame
}

fn task() -> TaskInput {
    bind(TaskInput {
        resolved_limits: None,
        instruction: "Classify this synthetic input as good. Return JSON.".into(),
        definition: Definition {
            limit_overrides: None,
            inference: Some(borg_runtime::protocol::InferenceSettings::selected()),
            execution_profile: Some(borg_runtime::protocol::ExecutionProfile::CollaborationV1 {
                web_search: None,
            }),
            id: "native-fixture".into(),
            revision: "1".into(),
            executor: ExecutorKind::Native,
        },
        repository: None,
        repository_read: None,
        acceptance_commands: vec![],
        dependencies: vec![],
        native_workflow: None,
        output_schema: Some(
            json!({"type":"object", "properties":{"result":{"type":"string"}}, "required":["result"], "additionalProperties":false}),
        ),
        limits: Limits {
            deadline_unix_ms: now_ms() + 5000,
            model_requests: 1,
            output_bytes: 1024,
            result_bytes: metadata().execution.native.result_bytes,
            max_delegation_depth: 0,
        },
    })
}
fn config(endpoint: String) -> NativeConfig {
    // The public HOME value is deliberately the synthetic loopback credential.
    // No actual provider key is read and no process-global environment is mutated.
    NativeConfig {
        endpoint,
        api_key_env: "HOME".into(),
        model: "gpt-6-astra".into(),
    }
}
fn envelope(text: &str) -> Value {
    json!({"id":"fixture-response", "status":"completed", "error":null, "output":[
        {"type":"reasoning", "summary":[]},
        {"type":"message", "role":"assistant", "status":"completed", "content":[{"type":"output_text", "text":text}]}
    ]})
}

struct Fixture {
    endpoint: String,
    received: oneshot::Receiver<Value>,
    handle: JoinHandle<()>,
}
impl Fixture {
    async fn start(status: u16, body: Vec<u8>, delay: Duration, extra: &str) -> Self {
        Self::start_parts(status, body, delay, extra, 0).await
    }
    async fn start_parts(
        status: u16,
        body: Vec<u8>,
        delay: Duration,
        extra: &str,
        missing: usize,
    ) -> Self {
        Self::start_response(status, body, delay, extra, missing, Duration::ZERO, true).await
    }
    async fn start_response(
        status: u16,
        body: Vec<u8>,
        delay: Duration,
        extra: &str,
        missing: usize,
        body_delay: Duration,
        content_length: bool,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
        let (tx, received) = oneshot::channel();
        let extra = extra.to_owned();
        let handle = tokio::spawn(async move {
            let (mut stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut data = Vec::new();
            let mut buffer = [0u8; 4096];
            let end = loop {
                let n = stream.read(&mut buffer).await.unwrap();
                if n == 0 {
                    return;
                }
                data.extend_from_slice(&buffer[..n]);
                assert!(data.len() <= 2 * 1024 * 1024);
                if let Some(end) = data.windows(4).position(|s| s == b"\r\n\r\n") {
                    break end + 4;
                }
            };
            let headers = std::str::from_utf8(&data[..end]).unwrap();
            assert!(headers.starts_with("POST /v1/responses HTTP/1.1\r\n"));
            assert!(
                headers
                    .to_ascii_lowercase()
                    .contains("authorization: bearer ")
            );
            let length = headers
                .lines()
                .find_map(|line| {
                    line.split_once(':')
                        .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                        .map(|(_, n)| n.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            assert!(length <= 1024 * 1024);
            while data.len() < end + length {
                let n = stream.read(&mut buffer).await.unwrap();
                if n == 0 {
                    return;
                }
                data.extend_from_slice(&buffer[..n]);
            }
            let request = serde_json::from_slice(&data[end..end + length]).unwrap();
            let _ = tx.send(request);
            if !delay.is_zero() {
                let mut probe = [0u8; 1];
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {},
                    closed = stream.read(&mut probe) => {
                        match closed {
                            Ok(0) => {},
                            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {},
                            _ => panic!("expected cancelled connection, not a second request"),
                        }
                        return;
                    }
                }
            }
            let length = if content_length {
                format!("content-length: {}\r\n", body.len() + missing)
            } else {
                String::new()
            };
            let header = format!(
                "HTTP/1.1 {status} fixture\r\ncontent-type: application/json\r\n{length}connection: close\r\n{extra}\r\n"
            );
            if stream.write_all(header.as_bytes()).await.is_ok() {
                if !body_delay.is_zero() {
                    let mut probe = [0u8; 1];
                    tokio::select! {
                        _ = tokio::time::sleep(body_delay) => {},
                        closed = stream.read(&mut probe) => {
                            assert!(matches!(closed, Ok(0)) || closed.is_err());
                            return;
                        }
                    }
                }
                let _ = stream.write_all(&body).await;
            }
            let _ = stream.shutdown().await;
        });
        Self {
            endpoint,
            received,
            handle,
        }
    }
    async fn finish(self) {
        self.handle.abort();
        let _ = self.handle.await;
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#the-native-executor-is-bounded
//= type=test
//# The native executor MUST validate a result against that schema before accepting it.
// Native component scope: Actual completed structured output validated against the submitted schema; no controller terminal commit proof.
async fn native_success_binds_model_schema_and_validates_complete_response() {
    let f = Fixture::start(
        200,
        serde_json::to_vec(&envelope(r#"{"result":"good"}"#)).unwrap(),
        Duration::ZERO,
        "",
    )
    .await;
    let result = require_send(execute(&task(), &config(f.endpoint.clone()))).await;
    let request = f.received.await.unwrap();
    f.handle.await.unwrap();
    let result = result.ok().expect("successful bounded output");
    assert_eq!(result.output, json!({"result":"good"}));
    assert_eq!(result.requests, 1);
    assert_eq!(request["model"], "gpt-6-astra");
    assert_eq!(request["reasoning"], json!({"effort":"max"}));
    assert_eq!(request["store"], false);
    assert_eq!(request["stream"], false);
    assert_eq!(request["text"]["format"]["type"], "json_schema");
    assert_eq!(
        request["text"]["format"]["schema"],
        task().output_schema.unwrap()
    );
    assert!(request.get("tools").is_none());
}

#[tokio::test]
//= spec/impl/v2/implementation.md#the-native-executor-is-bounded
//= type=test
//# The native executor MUST validate a result against that schema before accepting it.
// Native component scope: Rejects schema mismatch, non-JSON, incomplete response, refusal and unexpected actions.
async fn native_refuses_invalid_incomplete_refused_or_tool_output() {
    let mut cases = vec![
        (envelope(r#"{"result":4}"#), "native_output_schema_mismatch"),
        (envelope("not-json"), "native_output_not_json"),
        (
            json!({"status":"incomplete","output":[]}),
            "native_provider_response_not_completed",
        ),
        (
            json!({"status":"completed","output":[{"type":"function_call","name":"shell"}]}),
            "native_unexpected_provider_action",
        ),
    ];
    let mut refusal = envelope("{}");
    refusal["output"][1]["content"] = json!([{"type":"refusal","refusal":"not copied to error"}]);
    cases.push((refusal, "native_provider_refusal"));
    for (body, expected) in cases {
        let f = Fixture::start(200, serde_json::to_vec(&body).unwrap(), Duration::ZERO, "").await;
        let result = execute(&task(), &config(f.endpoint.clone())).await;
        f.finish().await;
        assert_eq!(result.err().as_deref(), Some(expected));
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#the-native-executor-is-bounded
//= type=test
//# A native task MUST declare a finite model-request budget.
// Native component scope: Zero request budget, expired deadline and shell work refuse before network; no missing-schema case is present.
//= spec/impl/v2/implementation.md#the-native-executor-is-bounded
//= type=test
//# The native executor MUST terminate its attempt when the task's execution deadline is reached.
async fn native_budget_deadline_and_shell_admission_precede_network() {
    let mut input = task();
    let c = config("http://127.0.0.1:1/v1/responses".into());
    input.limits.model_requests = 0;
    assert_eq!(
        execute(&input, &c).await.err().as_deref(),
        Some("native_request_budget_exhausted_task_model_requests")
    );
    input.limits.model_requests = 1;
    input.limits.deadline_unix_ms = 0;
    assert_eq!(
        execute(&input, &c).await.err().as_deref(),
        Some("native_deadline_exceeded")
    );
    input.limits.deadline_unix_ms = now_ms() + 5000;
    input.acceptance_commands = vec![vec!["sh".into()]];
    assert_eq!(
        execute(&input, &c).await.err().as_deref(),
        Some("native_has_no_workspace_or_shell")
    );
}

#[tokio::test]
// Native component scope: Configured output/wire bounds and HTTP status labels; no provider body echo.
async fn native_response_limits_and_errors_never_echo_provider_body() {
    let mut input = task();
    input.limits.output_bytes = 16;
    let f = Fixture::start(
        200,
        serde_json::to_vec(&envelope(&"x".repeat(17))).unwrap(),
        Duration::ZERO,
        "",
    )
    .await;
    let r = execute(&input, &config(f.endpoint.clone())).await;
    f.finish().await;
    assert_eq!(
        r.err().as_deref(),
        Some("native_output_byte_bound_task_output_bytes")
    );
    let f = Fixture::start(
        200,
        vec![b'x'; input.execution_limits().unwrap().native.response_wire_bytes + 1],
        Duration::ZERO,
        "",
    )
    .await;
    let r = execute(&input, &config(f.endpoint.clone())).await;
    f.finish().await;
    assert_eq!(
        r.err().as_deref(),
        Some("native_response_byte_bound_native_response_wire_bytes")
    );
    for (status, error) in [
        (401, "native_provider_authentication_failed"),
        (429, "native_provider_rate_limited"),
        (503, "native_provider_unavailable"),
        (302, "native_provider_redirect_refused"),
    ] {
        let f = Fixture::start(
            status,
            b"DO_NOT_COPY_PROVIDER_SECRET".to_vec(),
            Duration::ZERO,
            "location: http://127.0.0.1:1/\r\n",
        )
        .await;
        let r = execute(&task(), &config(f.endpoint.clone())).await;
        f.finish().await;
        assert_eq!(r.err(), Some(format!("{error} status={status}")));
    }
}

#[tokio::test]
// Native component scope: Fixed diagnostic labels survive HTTP refusal without
// accepting output, completing provider accounting, or retaining response prose.
async fn native_http_diagnostics_keep_status_and_known_labels_only() {
    for (status, body, expected) in [
        (
            400,
            json!({"error":{"message":"DO_NOT_COPY_PROVIDER_SECRET", "type":"invalid_request_error", "code":"invalid_json_schema", "param":"text.format.schema"}}),
            "native_provider_request_refused status=400 type=invalid_request_error code=invalid_json_schema param=text.format.schema",
        ),
        (
            404,
            json!({"error":{"message":"DO_NOT_COPY_PROVIDER_SECRET", "type":"invalid_request_error", "code":"model_not_found", "param":"model"}}),
            "native_provider_request_refused status=404 type=invalid_request_error code=model_not_found param=model",
        ),
        (
            400,
            json!({"error":{"message":std::env::var("HOME").unwrap(), "type":"private_project_123", "code":"DO_NOT_COPY_PROVIDER_SECRET", "param":"text.format.schema.properties.DO_NOT_COPY_PROVIDER_SECRET"}}),
            "native_provider_request_refused status=400",
        ),
        (
            400,
            json!({"error":{"type":17,"code":["invalid_json_schema"],"param":false}}),
            "native_provider_request_refused status=400",
        ),
        (
            429,
            json!({"error":{"type":"insufficient_quota", "code":"project_spend_limit_exceeded", "param":null}}),
            "native_provider_rate_limited status=429 type=insufficient_quota code=project_spend_limit_exceeded",
        ),
    ] {
        let f = Fixture::start(
            status,
            serde_json::to_vec(&body).unwrap(),
            Duration::ZERO,
            "",
        )
        .await;
        let result = observed(&task(), &config(f.endpoint.clone())).await;
        f.finish().await;
        assert_eq!(result.result.err().as_deref(), Some(expected));
        assert_eq!(
            result.request_exposure,
            RequestExposure::HttpResponseReceived
        );
        assert_eq!(result.provider_outcome, ProviderOutcome::Unknown);
        assert_eq!(result.usage, UsageObservation::Unknown);
    }
}

#[tokio::test]
// Native component scope: Diagnostics discard malformed, duplicate, incomplete
// and over-bound bodies, including responses with no advertised length.
async fn native_http_diagnostics_bound_body_before_retaining_labels() {
    let known =
        br#"{"error":{"code":"invalid_json_schema","param":"text.format.schema"}}"#.to_vec();
    for (body, missing, declared) in [
        (b"DO_NOT_COPY_PROVIDER_SECRET".to_vec(), 0, true),
        (
            br#"{"error":{"code":"invalid_json_schema","code":"model_not_found"}}"#.to_vec(),
            0,
            true,
        ),
        (known.clone(), 1, true),
        ([known.clone(), vec![b' '; 8192]].concat(), 0, true),
        ([known, vec![b' '; 8192]].concat(), 0, false),
    ] {
        let f = Fixture::start_response(
            400,
            body,
            Duration::ZERO,
            "",
            missing,
            Duration::ZERO,
            declared,
        )
        .await;
        let result = observed(&task(), &config(f.endpoint.clone())).await;
        f.finish().await;
        assert_eq!(
            result.result.err().as_deref(),
            Some("native_provider_request_refused status=400")
        );
        assert_eq!(
            result.request_exposure,
            RequestExposure::HttpResponseReceived
        );
        assert_eq!(result.provider_outcome, ProviderOutcome::Unknown);
        assert_eq!(result.usage, UsageObservation::Unknown);
    }
}

#[tokio::test]
// Native component scope: A stalled error body cannot consume the remaining task
// deadline; client drop closes the fixture connection after the diagnostic cap.
async fn native_http_diagnostics_stop_waiting_for_stalled_error_body() {
    let f = Fixture::start_response(
        400,
        vec![b'x'; 64],
        Duration::ZERO,
        "",
        0,
        Duration::from_secs(30),
        true,
    )
    .await;
    let mut input = task();
    input.limits.deadline_unix_ms = now_ms() + 30_000;
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        observed(&input, &config(f.endpoint.clone())),
    )
    .await
    .expect("diagnostic read must have its own finite bound");
    tokio::time::timeout(Duration::from_secs(2), f.handle)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        result.result.err().as_deref(),
        Some("native_provider_request_refused status=400")
    );
    assert_eq!(
        result.request_exposure,
        RequestExposure::HttpResponseReceived
    );
    assert_eq!(result.provider_outcome, ProviderOutcome::Unknown);
    assert_eq!(result.usage, UsageObservation::Unknown);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#the-native-executor-is-bounded
//= type=test
//# The native executor MUST terminate its attempt when the task's execution deadline is reached.
// Native component scope: Local HTTP deadline and dropped-future connection cleanup; provider-side termination unproved.
async fn native_timeout_and_dropped_future_after_received_request_do_not_finish() {
    let f = Fixture::start(
        200,
        serde_json::to_vec(&envelope(r#"{"result":"late"}"#)).unwrap(),
        Duration::from_secs(10),
        "",
    )
    .await;
    let mut input = task();
    input.limits.deadline_unix_ms = now_ms() + 100;
    let r = execute(&input, &config(f.endpoint.clone())).await;
    f.finish().await;
    assert_eq!(r.err().as_deref(), Some("native_deadline_exceeded"));

    let mut f = Fixture::start(
        200,
        serde_json::to_vec(&envelope(r#"{"result":"late"}"#)).unwrap(),
        Duration::from_secs(10),
        "",
    )
    .await;
    let input = task();
    let c = config(f.endpoint.clone());
    let mut running = Box::pin(execute(&input, &c));
    tokio::select! { result=&mut running => panic!("completed before fixture release: {}",result.is_ok()), request=&mut f.received=>{request.unwrap();} }
    drop(running);
    tokio::time::timeout(Duration::from_secs(2), f.handle)
        .await
        .expect("dropped HTTP future must release its connection")
        .unwrap();
}

#[tokio::test]
// Native component scope: Local references, recursion/depth and supported regex/work admission.
async fn native_schema_references_and_pathological_work_refuse_before_send() {
    let c = config("http://127.0.0.1:1/v1/responses".into());
    for schema in [
        json!({"$ref":"https://example.invalid/schema"}),
        json!({"$ref":"file:///etc/passwd"}),
        json!({"$ref":"#"}),
        json!({"$dynamicRef":"#node"}),
        json!({"type":4}),
        json!({"pattern":"(?=a)a"}),
    ] {
        let mut input = task();
        input.output_schema = Some(schema);
        let e = execute(&input, &c).await.err().unwrap();
        assert!(e.starts_with("native_schema_"), "{e}");
    }
    let mut schema = json!({"type":"string"});
    for _ in 0..30 {
        schema = json!({"not":schema});
    }
    let mut input = task();
    input.output_schema = Some(schema);
    assert!(execute(&input, &c).await.err().unwrap().contains("bound"));
}

#[tokio::test]
//= spec/impl/v2/implementation.md#the-native-executor-is-bounded
//= type=test
//# The native executor MUST validate a result against that schema before accepting it.
// Native component scope: Validates local-ref output; additionally refuses known configured-key echo, not arbitrary secret discovery.
async fn native_local_ref_validates_and_credential_echo_is_refused() {
    let mut input = task();
    input.output_schema = Some(
        json!({"$defs":{"answer":{"type":"object","properties":{"result":{"type":"string"}},"required":["result"],"additionalProperties":false}},"$ref":"#/$defs/answer"}),
    );
    let f = Fixture::start(
        200,
        serde_json::to_vec(&envelope(r#"{"result":"good"}"#)).unwrap(),
        Duration::ZERO,
        "",
    )
    .await;
    let r = execute(&input, &config(f.endpoint.clone())).await;
    f.finish().await;
    assert!(r.is_ok());
    let key = std::env::var("HOME").unwrap();
    let escaped = key
        .chars()
        .map(|c| format!("\\u{:04x}", c as u32))
        .collect::<String>();
    let text = format!("{{\"result\":\"{escaped}\"}}");
    let f = Fixture::start(
        200,
        serde_json::to_vec(&envelope(&text)).unwrap(),
        Duration::ZERO,
        "",
    )
    .await;
    let r = execute(&input, &config(f.endpoint.clone())).await;
    f.finish().await;
    assert_eq!(
        r.err().as_deref(),
        Some("native_output_contains_credential")
    );
}

#[tokio::test]
async fn native_rejects_secret_urls_and_non_https_remote_targets() {
    for endpoint in [
        "http://example.invalid/v1/responses",
        "https://user:password@example.invalid/v1/responses",
        "https://example.invalid/v1/responses?key=secret",
    ] {
        let e = execute(&task(), &config(endpoint.into()))
            .await
            .err()
            .unwrap();
        assert!(e.starts_with("native_endpoint_"));
        assert!(!e.contains("password") && !e.contains("secret"));
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#the-native-executor-is-bounded
//= type=test
//# The native executor MUST validate a result against that schema before accepting it.
// Native component scope: Incomplete body cannot produce an accepted native result.
async fn native_truncated_transport_is_not_an_accepted_response() {
    let f = Fixture::start_parts(
        200,
        serde_json::to_vec(&envelope(r#"{"result":"good"}"#)).unwrap(),
        Duration::ZERO,
        "",
        20,
    )
    .await;
    let r = execute(&task(), &config(f.endpoint.clone())).await;
    f.finish().await;
    assert_eq!(r.err().as_deref(), Some("native_response_incomplete"));
}

#[tokio::test]
// Native component scope: Independent expanded-schema and actual result-work admission.
async fn native_ref_expansion_and_result_work_have_independent_bounds() {
    let mut defs = serde_json::Map::new();
    defs.insert("n0".into(), json!({"type":"integer"}));
    for i in 1..12 {
        let reference = format!("#/$defs/n{}", i - 1);
        defs.insert(
            format!("n{i}"),
            json!({"anyOf":[{"$ref":reference},{"$ref":reference}]}),
        );
    }
    let mut input = task();
    input.output_schema = Some(json!({"$defs":defs,"$ref":"#/$defs/n11"}));
    let r = execute(&input, &config("http://127.0.0.1:1/v1/responses".into())).await;
    assert_eq!(
        r.err().as_deref(),
        Some("native_schema_work_bound_native_schema_expansion")
    );

    input = task();
    input.output_schema = Some(json!({"type":"array","items":{"type":"integer"}}));
    input.limits.output_bytes = 16 * 1024;
    let text = serde_json::to_string(&vec![1; 1500]).unwrap();
    let f = Fixture::start(
        200,
        serde_json::to_vec(&envelope(&text)).unwrap(),
        Duration::ZERO,
        "",
    )
    .await;
    let r = execute(&input, &config(f.endpoint.clone())).await;
    f.finish().await;
    assert_eq!(
        r.err().as_deref(),
        Some("native_validation_work_bound_native_validation_work")
    );
}

#[tokio::test]
//= spec/impl/v2/implementation.md#the-native-executor-is-bounded
//= type=test
//# The native executor MUST validate a result against that schema before accepting it.
// Native component scope: Actual const semantics retained; input known-key refusal is an additional narrow check.
async fn native_reference_text_inside_const_is_data_and_input_key_is_refused() {
    let value = json!({"$ref":"https://example.invalid/not-a-schema"});
    let mut input = task();
    input.output_schema = Some(json!({"const":value}));
    let f = Fixture::start(
        200,
        serde_json::to_vec(&envelope(&value.to_string())).unwrap(),
        Duration::ZERO,
        "",
    )
    .await;
    let r = execute(&input, &config(f.endpoint.clone())).await;
    f.finish().await;
    assert!(r.is_ok());

    input = task();
    input.instruction = std::env::var("HOME").unwrap();
    let r = execute(&input, &config("http://127.0.0.1:1/v1/responses".into())).await;
    assert_eq!(r.err().as_deref(), Some("native_input_contains_credential"));
}

#[tokio::test]
// Native component scope: Malformed-tail discrimination proves early node refusal for envelope/output; duplicate keys refuse.
async fn native_json_tree_admission_precedes_unbounded_allocation() {
    // Deliberately malformed tail: admission must fail at node4097, before
    // parsing the remaining bytes. A post-parse bound would report malformed.
    let wire = format!("{{\"unused\":[{}!", "0,".repeat(5000)).into_bytes();
    let f = Fixture::start(200, wire, Duration::ZERO, "").await;
    let result = execute(&task(), &config(f.endpoint.clone())).await;
    f.finish().await;
    assert_eq!(
        result.err().as_deref(),
        Some("native_json_complexity_bound_native_json_nodes")
    );

    let mut input = task();
    input.output_schema = Some(json!(true));
    input.limits.output_bytes = 32 * 1024;
    let output = format!("[{}!", "0,".repeat(5000));
    let f = Fixture::start(
        200,
        serde_json::to_vec(&envelope(&output)).unwrap(),
        Duration::ZERO,
        "",
    )
    .await;
    let result = execute(&input, &config(f.endpoint.clone())).await;
    f.finish().await;
    assert_eq!(
        result.err().as_deref(),
        Some("native_json_complexity_bound_native_json_nodes")
    );

    let f = Fixture::start(
        200,
        serde_json::to_vec(&envelope(r#"{"result":"good","result":"bad"}"#)).unwrap(),
        Duration::ZERO,
        "",
    )
    .await;
    let result = execute(&task(), &config(f.endpoint.clone())).await;
    f.finish().await;
    assert_eq!(result.err().as_deref(), Some("native_json_duplicate_key"));
}

#[tokio::test]
//= spec/impl/v2/implementation.md#the-native-executor-is-bounded
//= type=test
//# A native task MUST declare a finite model-request budget.
// Native component scope: An available counted listener observes one POST under budget1; no transparent retry after unknown response.
async fn native_received_post_never_retries_status_or_lost_response() {
    for lost_response in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
        let (stop, mut stopped) = oneshot::channel::<()>();
        // The listener stays alive after every response/connection failure and
        // counts every actual fully received POST until explicit observation end.
        let server = tokio::spawn(async move {
            let mut requests = 0;
            loop {
                let mut stream = tokio::select! {
                    accepted=listener.accept()=>accepted.unwrap().0,
                    _=&mut stopped=>break,
                };
                let mut data = Vec::new();
                let mut chunk = [0; 4096];
                let (end, length) = loop {
                    let n = stream.read(&mut chunk).await.unwrap();
                    assert!(n > 0);
                    data.extend_from_slice(&chunk[..n]);
                    assert!(data.len() <= 2 * 1024 * 1024);
                    if let Some(end) = data.windows(4).position(|b| b == b"\r\n\r\n") {
                        let end = end + 4;
                        let headers = std::str::from_utf8(&data[..end]).unwrap();
                        assert!(headers.starts_with("POST /v1/responses HTTP/1.1\r\n"));
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                line.split_once(':')
                                    .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                                    .map(|(_, v)| v.trim().parse::<usize>().unwrap())
                            })
                            .unwrap();
                        assert!(length <= 1024 * 1024);
                        break (end, length);
                    }
                };
                while data.len() < end + length {
                    let n = stream.read(&mut chunk).await.unwrap();
                    assert!(n > 0);
                    data.extend_from_slice(&chunk[..n]);
                }
                requests += 1;
                if !lost_response {
                    stream.write_all(b"HTTP/1.1 503 fixture\r\ncontent-length: 0\r\nconnection: close\r\n\r\n").await.unwrap();
                }
                stream.shutdown().await.unwrap();
            }
            requests
        });
        let result = observed(&task(), &config(endpoint)).await;
        // Includes an observation window with a still accepting server, not a
        // refusal caused by destroying the listener after the first request.
        tokio::time::sleep(Duration::from_millis(200)).await;
        stop.send(()).unwrap();
        let count = tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(
            result.result.err().as_deref(),
            Some(if lost_response {
                "native_transport_failed_outcome_unknown"
            } else {
                "native_provider_unavailable status=503"
            })
        );
        assert_eq!(result.usage, UsageObservation::Unknown);
        assert_eq!(
            result.request_exposure,
            if lost_response {
                RequestExposure::MayHaveReachedProvider
            } else {
                RequestExposure::HttpResponseReceived
            }
        );
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#the-native-executor-is-bounded
//= type=test
//# The native executor MUST validate a result against that schema before accepting it.
// Native component scope: 12 finite/nonfinite/const/enum subcases on actual decode+validator; upstream f32 bug not repaired.
async fn native_numeric_schema_checks_avoid_the_serde_f32_infinity_path() {
    // serde_json1.0.151 has an upstream Value↔f32 comparison defect for finite
    // values beyond f32 range. Neither our decoder nor the validator's numeric
    // const/enum paths may use that narrowing conversion. These are actual
    // execute→decode→Draft202012 validation cases, not a replacement validator.
    let mut cases = vec![
        (json!({"const":1e300}), "1e300", None),
        (
            json!({"const":1e300}),
            "1e299",
            Some("native_output_schema_mismatch"),
        ),
        (json!({"enum":[-1e39,1e300,"other"]}), "-1e39", None),
        (
            json!({"enum":[-1e39,1e300,"other"]}),
            "-2e39",
            Some("native_output_schema_mismatch"),
        ),
        (
            json!({"const":{"n":[1e300]}}),
            r#"{"n":[1e299]}"#,
            Some("native_output_schema_mismatch"),
        ),
        (json!({"const":0}), "-0", None),
        (json!({"const":0}), "-0.0", None),
    ];
    for text in ["1e400", "-1e400", "Infinity", "-Infinity", "NaN"] {
        cases.push((
            json!({"type":"number"}),
            text,
            Some("native_output_not_json"),
        ));
    }
    for (schema, text, expected_error) in cases {
        let mut input = task();
        input.output_schema = Some(schema);
        let f = Fixture::start(
            200,
            serde_json::to_vec(&envelope(text)).unwrap(),
            Duration::ZERO,
            "",
        )
        .await;
        let result = execute(&input, &config(f.endpoint.clone())).await;
        f.finish().await;
        if let Some(expected_error) = expected_error {
            assert_eq!(result.err().as_deref(), Some(expected_error));
        } else {
            let result = result.ok().expect("valid finite numeric schema output");
            assert!(result.output.as_f64().is_some_and(f64::is_finite));
        }
    }
}

// A unique hard link identifies this test's actual application children even
// when libtest runs other executor cases concurrently. It is the same compiled
// inode, not an alternate helper implementation or guest-supplied executable.
struct LinkedProgram {
    directory: std::path::PathBuf,
    path: std::path::PathBuf,
}
impl LinkedProgram {
    fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let directory = std::path::Path::new(env!("CARGO_BIN_EXE_borg-runtime"))
            .parent()
            .unwrap()
            .join(format!(
                "borg-native-validation-{}-{}",
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("borg-runtime");
        std::fs::hard_link(env!("CARGO_BIN_EXE_borg-runtime"), &path).unwrap();
        Self { directory, path }
    }
    fn children(&self) -> Vec<u32> {
        let mut children = std::collections::BTreeSet::new();
        for entry in std::fs::read_dir("/proc/self/task").unwrap() {
            let entry = entry.unwrap();
            let Ok(text) = std::fs::read_to_string(entry.path().join("children")) else {
                continue;
            };
            for pid in text
                .split_whitespace()
                .filter_map(|s| s.parse::<u32>().ok())
            {
                if std::fs::read_link(format!("/proc/{pid}/exe")).ok().as_ref() == Some(&self.path)
                {
                    children.insert(pid);
                }
            }
        }
        children.into_iter().collect()
    }
}
impl Drop for LinkedProgram {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_dir(&self.directory);
    }
}

#[cfg(debug_assertions)]
#[tokio::test]
//= spec/impl/v2/implementation.md#the-native-executor-is-bounded
//= type=test
//# The native executor MUST terminate its attempt when the task's execution deadline is reached.
// Native component scope: READY/live helper is killed and reaped on cancel/deadline; dropped child retained in quarantine.
async fn native_validation_cancel_and_deadline_reap_the_actual_helper() {
    for mode in ["cancel", "deadline", "drop"] {
        let program = LinkedProgram::new();
        let mut input = task();
        let oracle_started = tokio::time::Instant::now();
        input.limits.deadline_unix_ms = now_ms() + 1200;
        let cfg = config("http://127.0.0.1:1/v1/responses".into());
        let (stop, stopped) = oneshot::channel::<()>();
        let mut ready = borg_runtime::native::qualification_hold_next_validation(&program.path);
        let mut running = Box::pin(execute_with_validation_program(
            &input,
            &cfg,
            &program.path,
            async {
                let _ = stopped.await;
            },
        ));
        let pid = tokio::select! {
            result = &mut running => panic!("execution ended before live helper READY: mode={mode}, elapsed_ms={}, error={:?}, ready_probe_after_return={:?}, deadline_unix_ms={}, observed_unix_ms={}",oracle_started.elapsed().as_millis(),result.as_ref().err(),ready.try_recv(),input.limits.deadline_unix_ms,now_ms()),
            ready = &mut ready => ready.unwrap_or_else(|error|panic!("actual child READY closed: mode={mode}, elapsed_ms={}, channel={error:?}",oracle_started.elapsed().as_millis())),
        };
        eprintln!(
            "native_helper_oracle: mode={mode}, ready_ms={}, remaining_ms={}, pid={pid}",
            oracle_started.elapsed().as_millis(),
            input.limits.deadline_unix_ms.saturating_sub(now_ms())
        );
        assert!(program.children().contains(&pid));
        assert!(
            std::fs::read(format!("/proc/{pid}/environ"))
                .unwrap()
                .is_empty()
        );
        let state = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
        assert!(
            !state.rsplit_once(") ").unwrap().1.starts_with('Z'),
            "READY helper must still be live"
        );
        // The exact same binary is now spinning in its debug-only qualification
        // mode. No guest TaskInput/config/environment can request that mode.
        if mode == "drop" {
            drop(running);
            tokio::time::timeout(Duration::from_secs(2), async {
                while std::path::Path::new(&format!("/proc/{pid}")).exists() {
                    assert!(borg_runtime::native::validation_cleanup_pending() <= 4);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("quarantined child must be reaped");
            borg_runtime::native::validation_cleanup_pending();
        } else {
            if mode == "cancel" {
                stop.send(()).unwrap();
            }
            let result = tokio::time::timeout(Duration::from_secs(3), running)
                .await
                .unwrap();
            assert_eq!(
                result.err().as_deref(),
                Some(if mode == "cancel" {
                    "native_cancelled"
                } else {
                    "native_deadline_exceeded"
                })
            );
            assert!(
                !std::path::Path::new(&format!("/proc/{pid}")).exists(),
                "child must be reaped before return"
            );
        }
        assert!(program.children().is_empty());
    }
}

#[tokio::test]
// Native component scope: No CPU child retained while the bounded network request waits; not a production concurrency benchmark.
async fn native_waiting_http_owns_no_validation_process() {
    let program = LinkedProgram::new();
    let mut f = Fixture::start(
        200,
        serde_json::to_vec(&envelope(r#"{"result":"late"}"#)).unwrap(),
        Duration::from_secs(10),
        "",
    )
    .await;
    let input = task();
    let cfg = config(f.endpoint.clone());
    let (stop, stopped) = oneshot::channel::<()>();
    let mut running = Box::pin(execute_with_validation_program(
        &input,
        &cfg,
        &program.path,
        async {
            let _ = stopped.await;
        },
    ));
    tokio::select! { result=&mut running=>panic!("completed before HTTP wait: {}",result.is_ok()), request=&mut f.received=>{request.unwrap();} }
    assert!(
        program.children().is_empty(),
        "schema child must exit before HTTP waiting begins"
    );
    stop.send(()).unwrap();
    assert_eq!(running.await.err().as_deref(), Some("native_cancelled"));
    tokio::time::timeout(Duration::from_secs(2), f.handle)
        .await
        .unwrap()
        .unwrap();
    assert!(program.children().is_empty());
}

#[tokio::test]
// Native component scope: Internal helper framing refuses size before reading body allocation.
async fn native_helper_refuses_oversized_frames_before_payload_read() {
    use std::process::Stdio;
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_borg-runtime"))
        .arg("__native_validate")
        .env_clear()
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let pid = child.id().unwrap();
    let mut header = Vec::from(&b"NV1\0"[..]);
    header.extend_from_slice(&[6, 0]);
    header.extend_from_slice(&1024u32.to_be_bytes());
    header.extend_from_slice(&(now_ms() + 5000).to_be_bytes());
    header.extend_from_slice(
        &(u32::try_from(metadata().execution.native.schema_bytes).unwrap() + 1).to_be_bytes(),
    );
    header.extend_from_slice(&0u32.to_be_bytes());
    header.push(0); // caller-selected task output bound
    header.extend_from_slice(&helper_policy_frame());
    let mut input = child.stdin.take().unwrap();
    input.write_all(&header).await.unwrap();
    // Retain stdin open with no body: refusal must follow the length header.
    let mut output = child.stdout.take().unwrap();
    let mut reply = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), output.read_to_end(&mut reply))
        .await
        .unwrap()
        .unwrap();
    drop(input);
    assert!(child.wait().await.unwrap().success());
    assert_eq!(&reply[..6], b"NVR\0\x06\x00");
    assert!(reply[6..31].iter().all(|b| *b == 0));
    assert_eq!(reply[31], 1);
    assert_eq!(
        u32::from_be_bytes(reply[32..36].try_into().unwrap()) as usize,
        reply.len() - 36
    );
    assert_eq!(
        &reply[36..],
        b"native_validation_frame_bound_native_schema_bytes"
    );
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
}

#[tokio::test]
//= spec/impl/v2/implementation.md#the-native-executor-is-bounded
//= type=test
//# The native executor MUST validate a result against that schema before accepting it.
// Native component scope: Opaque data success binds exact schema/optional Value/bound; no operation identity/currentness authority.
async fn native_shared_validation_binds_exact_schema_value_and_bound() {
    use borg_runtime::native::validate_json_with_program;
    let program = std::path::Path::new(env!("CARGO_BIN_EXE_borg-runtime"));
    let schema = json!({"type":"object","properties":{"answer":{"const":42}},"required":["answer"],"additionalProperties":false});
    let value = json!({"answer":42});
    let admitted = validate_json_with_program(
        &schema,
        None,
        1024,
        tokio::time::Instant::now() + Duration::from_secs(5),
        program,
    )
    .await
    .unwrap();
    assert!(admitted.matches(&schema, None, 1024));
    assert!(!admitted.matches(&schema, Some(&value), 1024));
    let checked = validate_json_with_program(
        &schema,
        Some(&value),
        1024,
        tokio::time::Instant::now() + Duration::from_secs(5),
        program,
    )
    .await
    .unwrap();
    assert!(checked.matches(&schema, Some(&value), 1024));
    assert!(!checked.matches(&schema, Some(&json!({"answer":43})), 1024));
    assert!(!checked.matches(&json!(true), Some(&value), 1024));
    assert!(!checked.matches(&schema, Some(&value), 1025));
    assert!(!checked.matches(&schema, None, 1024));
}

#[tokio::test]
//= spec/impl/v2/implementation.md#the-native-executor-is-bounded
//= type=test
//# The native executor MUST validate a result against that schema before accepting it.
// Native component scope: Invalid schema/output differ from unavailable helper; no failure can mint a success binding.
async fn native_shared_validation_distinguishes_refusal_from_helper_failure() {
    use borg_runtime::native::{JsonValidationError, validate_json_with_program};
    let program = std::path::Path::new(env!("CARGO_BIN_EXE_borg-runtime"));
    for (schema, output, bound, expected) in [
        (
            json!({"type":"number"}),
            Some(json!("wrong")),
            1024,
            "native_output_schema_mismatch",
        ),
        (
            json!({"type":7}),
            None,
            1024,
            "native_schema_invalid_or_unsupported",
        ),
        (
            json!({"$ref":"https://example.invalid/schema"}),
            None,
            1024,
            "native_schema_external_reference",
        ),
        (
            json!(true),
            Some(json!(1)),
            0,
            "native_output_limit_invalid_native_output_bytes",
        ),
    ] {
        let result = validate_json_with_program(
            &schema,
            output.as_ref(),
            bound,
            tokio::time::Instant::now() + Duration::from_secs(5),
            program,
        )
        .await;
        assert!(matches!(result,Err(JsonValidationError::Refused(code)) if code==expected));
    }
    let absent = LinkedProgram::new();
    let result = validate_json_with_program(
        &json!(true),
        None,
        1024,
        tokio::time::Instant::now() + Duration::from_secs(5),
        &absent.directory.join("not-installed"),
    )
    .await;
    assert!(
        matches!(result,Err(JsonValidationError::Helper(code)) if code=="native_validation_unavailable")
    );
}

#[cfg(debug_assertions)]
#[tokio::test]
//= spec/impl/v2/implementation.md#the-native-executor-is-bounded
//= type=test
//# The native executor MUST terminate its attempt when the task's execution deadline is reached.
// Native component scope: Controller-facing pure facade kills/reaps a confirmed live helper at its monotonic deadline.
async fn native_shared_validation_deadline_reaps_live_helper() {
    use borg_runtime::native::{JsonValidationError, validate_json_with_program};
    let program = LinkedProgram::new();
    let mut ready = borg_runtime::native::qualification_hold_next_validation(&program.path);
    let schema = json!(true);
    let mut running = Box::pin(validate_json_with_program(
        &schema,
        None,
        1024,
        tokio::time::Instant::now() + Duration::from_millis(1200),
        &program.path,
    ));
    let pid = tokio::select! {
        result=&mut running=>panic!("validation finished before READY: success={}, error={:?}",result.is_ok(),result.as_ref().err()),
        pid=&mut ready=>pid.unwrap(),
    };
    assert!(program.children().contains(&pid));
    let state = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    assert!(!state.rsplit_once(") ").unwrap().1.starts_with('Z'));
    let result = tokio::time::timeout(Duration::from_secs(3), running)
        .await
        .unwrap();
    assert!(matches!(result, Err(JsonValidationError::Deadline)));
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    assert!(program.children().is_empty());
}

#[tokio::test]
//= spec/impl/v2/implementation.md#the-native-executor-is-bounded
//= type=test
//# A native task MUST declare its output schema.
// Native component scope: missing schema refuses before helper launch and POST.
async fn native_missing_schema_is_not_started_and_never_contacts_provider() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cfg = config(format!(
        "http://{}/v1/responses",
        listener.local_addr().unwrap()
    ));
    let mut input = task();
    input.output_schema = None;
    let result = execute_observed_with_validation_program(
        &input,
        &cfg,
        std::path::Path::new("/nonexistent-borg-validation-helper"),
        std::future::pending(),
    )
    .await;
    assert_eq!(
        result.result.err().as_deref(),
        Some("native_output_schema_missing")
    );
    assert_eq!(result.request_exposure, RequestExposure::NotStarted);
    assert_eq!(result.usage, UsageObservation::Unknown);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn native_observed_usage_survives_success_refusal_incomplete_and_schema_failure() {
    let mut refused = with_usage(envelope("{}"));
    refused["output"][1]["content"] = json!([{"type":"refusal","refusal":"not logged"}]);
    for (body, error, provider_outcome) in [
        (
            with_usage(envelope(r#"{"result":"good"}"#)),
            None,
            ProviderOutcome::Completed,
        ),
        (
            refused,
            Some("native_provider_refusal"),
            ProviderOutcome::Completed,
        ),
        (
            with_usage(json!({"status":"incomplete","output":[]})),
            Some("native_provider_response_not_completed"),
            ProviderOutcome::Unknown,
        ),
        (
            with_usage(envelope(r#"{"result":4}"#)),
            Some("native_output_schema_mismatch"),
            ProviderOutcome::Completed,
        ),
        (
            with_usage(envelope("not JSON")),
            Some("native_output_not_json"),
            ProviderOutcome::Completed,
        ),
    ] {
        let f = Fixture::start(200, serde_json::to_vec(&body).unwrap(), Duration::ZERO, "").await;
        let result = require_send(observed(&task(), &config(f.endpoint.clone()))).await;
        f.finish().await;
        assert_eq!(
            result.request_exposure,
            RequestExposure::HttpResponseReceived
        );
        assert_eq!(result.usage, reported());
        assert_eq!(result.provider_outcome, provider_outcome);
        if let Some(error) = error {
            assert_eq!(result.result.err().as_deref(), Some(error));
        } else {
            assert_eq!(result.result.ok().expect("valid result").requests, 1);
        }
    }
}

#[tokio::test]
async fn native_observed_usage_unknown_invalid_and_zero_never_conflate() {
    let mut cases = vec![
        (None, UsageObservation::Unknown),
        (Some(Value::Null), UsageObservation::Unknown),
        (
            Some(json!({"input_tokens":0,"output_tokens":0,"total_tokens":0})),
            UsageObservation::Reported(ProviderTokenUsage {
                input_tokens: 0,
                output_tokens: 0,
                total_tokens: 0,
            }),
        ),
    ];
    for usage in [
        json!("bad"),
        json!({"input_tokens":17}),
        json!({"input_tokens":-1,"output_tokens":9,"total_tokens":8}),
        json!({"input_tokens":17.0,"output_tokens":9,"total_tokens":26}),
        json!({"input_tokens":17,"output_tokens":9,"total_tokens":27}),
        json!({"input_tokens":18446744073709551615u64,"output_tokens":1,"total_tokens":0}),
        json!({"input_tokens":9223372036854775808u64,"output_tokens":0,"total_tokens":9223372036854775808u64}),
    ] {
        cases.push((Some(usage), UsageObservation::Invalid));
    }
    for (usage, expected) in cases {
        let mut body = envelope(r#"{"result":"good"}"#);
        if let Some(usage) = usage {
            body["usage"] = usage;
        }
        let f = Fixture::start(200, serde_json::to_vec(&body).unwrap(), Duration::ZERO, "").await;
        let result = observed(&task(), &config(f.endpoint.clone())).await;
        f.finish().await;
        assert!(result.result.is_ok());
        assert_eq!(result.usage, expected);
        assert_eq!(
            result.usage.diagnostic(),
            if expected == UsageObservation::Invalid {
                Some("native_usage_invalid")
            } else {
                None
            }
        );
    }
}

#[tokio::test]
async fn native_provider_usage_is_not_taken_from_user_output_or_duplicate_fields() {
    let mut input = task();
    input.output_schema = Some(json!(true));
    let fake = json!({"usage":{"input_tokens":999,"output_tokens":999,"total_tokens":1998}});
    for body in [
        envelope(&fake.to_string()),
        with_usage(envelope(&fake.to_string())),
    ] {
        let expected = if body.get("usage").is_some() {
            reported()
        } else {
            UsageObservation::Unknown
        };
        let f = Fixture::start(200, serde_json::to_vec(&body).unwrap(), Duration::ZERO, "").await;
        let result = observed(&input, &config(f.endpoint.clone())).await;
        f.finish().await;
        assert_eq!(result.usage, expected);
        assert_eq!(result.result.ok().expect("valid user data").output, fake);
    }
    let mut duplicate =
        serde_json::to_string(&with_usage(envelope(r#"{"result":"good"}"#))).unwrap();
    duplicate.pop();
    duplicate.push_str(r#", "usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}"#);
    let f = Fixture::start(200, duplicate.into_bytes(), Duration::ZERO, "").await;
    let result = observed(&task(), &config(f.endpoint.clone())).await;
    f.finish().await;
    assert_eq!(
        result.result.err().as_deref(),
        Some("native_json_duplicate_key")
    );
    assert_eq!(result.usage, UsageObservation::Unknown);
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn native_usage_received_before_validation_cancellation_is_retained() {
    let program = LinkedProgram::new();
    let mut ready = borg_runtime::native::qualification_hold_provider_after_usage(&program.path);
    let f = Fixture::start(
        200,
        serde_json::to_vec(&with_usage(envelope(r#"{"result":"good"}"#))).unwrap(),
        Duration::ZERO,
        "",
    )
    .await;
    let input = task();
    let cfg = config(f.endpoint.clone());
    let (stop, stopped) = oneshot::channel::<()>();
    let mut running = Box::pin(execute_observed_with_validation_program(
        &input,
        &cfg,
        &program.path,
        async {
            let _ = stopped.await;
        },
    ));
    let pid = tokio::select! {result=&mut running=>panic!("finished before usage READY: {}",result.result.is_ok()),pid=&mut ready=>pid.unwrap()};
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    assert!(!stat.rsplit_once(") ").unwrap().1.starts_with('Z'));
    stop.send(()).unwrap();
    let result = running.await;
    f.finish().await;
    assert_eq!(result.result.err().as_deref(), Some("native_cancelled"));
    assert_eq!(
        result.request_exposure,
        RequestExposure::HttpResponseReceived
    );
    assert_eq!(result.usage, reported());
    assert_eq!(result.provider_outcome, ProviderOutcome::Completed);
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
}

#[tokio::test]
async fn native_cancel_after_received_post_preserves_unknown_exposure_and_usage() {
    let mut f = Fixture::start(
        200,
        serde_json::to_vec(&with_usage(envelope(r#"{"result":"good"}"#))).unwrap(),
        Duration::from_secs(10),
        "",
    )
    .await;
    let input = task();
    let cfg = config(f.endpoint.clone());
    let (stop, stopped) = oneshot::channel::<()>();
    let mut running = Box::pin(execute_observed_with_validation_program(
        &input,
        &cfg,
        std::path::Path::new(env!("CARGO_BIN_EXE_borg-runtime")),
        async {
            let _ = stopped.await;
        },
    ));
    tokio::select! {result=&mut running=>panic!("response completed before cancel: {}",result.result.is_ok()),request=&mut f.received=>{request.unwrap();}}
    stop.send(()).unwrap();
    let result = running.await;
    tokio::time::timeout(Duration::from_secs(2), f.handle)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.result.err().as_deref(), Some("native_cancelled"));
    assert_eq!(
        result.request_exposure,
        RequestExposure::MayHaveReachedProvider
    );
    assert_eq!(result.usage, UsageObservation::Unknown);
    assert_eq!(result.provider_outcome, ProviderOutcome::Unknown);
}

#[tokio::test]
async fn native_helper_revision_and_nonprovider_modes_preserve_usage_boundaries() {
    use std::process::Stdio;
    for (revision, mode, payload, error) in [
        (1, 0, Vec::new(), true),
        (2, 0, Vec::new(), true),
        (99, 0, Vec::new(), true),
        (3, 0, Vec::new(), true),
        (4, 0, Vec::new(), true),
        (5, 0, Vec::new(), true),
        (6, 0, Vec::new(), false),
        (
            6,
            3,
            br#"{"status":"completed","output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"true"}]}],"usage":{"input_tokens":17,"output_tokens":9,"total_tokens":26}}"#.to_vec(),
            false,
        ),
    ] {
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_borg-runtime"))
            .arg("__native_validate")
            .env_clear()
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let pid = child.id().unwrap();
        let mut pipe = child.stdin.take().unwrap();
        let mut frame = Vec::from(&b"NV1\0"[..]);
        frame.extend_from_slice(&[revision, mode]);
        frame.extend_from_slice(&1024u32.to_be_bytes());
        frame.extend_from_slice(&(now_ms() + 5000).to_be_bytes());
        frame.extend_from_slice(&4u32.to_be_bytes());
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.push(0); // caller-selected task output bound
        frame.extend_from_slice(&helper_policy_frame());
        frame.extend_from_slice(b"true");
        frame.extend_from_slice(&payload);
        pipe.write_all(&frame).await.unwrap();
        drop(pipe);
        let mut output = child.stdout.take().unwrap();
        let mut reply = Vec::new();
        tokio::time::timeout(Duration::from_secs(3), output.read_to_end(&mut reply))
            .await
            .unwrap()
            .unwrap();
        assert!(child.wait().await.unwrap().success());
        assert_eq!(&reply[..6], b"NVR\0\x06\x00");
        assert!(reply[6..31].iter().all(|b| *b == 0));
        assert_eq!(reply[31], u8::from(error));
        assert_eq!(
            u32::from_be_bytes(reply[32..36].try_into().unwrap()) as usize,
            reply.len() - 36
        );
        assert_eq!(
            &reply[36..],
            if error {
                b"native_validation_frame_invalid".as_slice()
            } else {
                payload.as_slice()
            }
        );
        assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    }
}

fn workflow_task() -> TaskInput {
    let mut input = task();
    input.limits.model_requests = 3;
    input.limits.max_delegation_depth = 1;
    input.native_workflow = Some(Box::new(NativeWorkflow::CodingThenContinue {
        child: CodingTaskTemplate {
            instruction_prefix: "Work only on the configured synthetic repository".into(),
            definition: Definition {
                limit_overrides: None,
                inference: Some(borg_runtime::protocol::InferenceSettings::selected()),
                execution_profile: Some(
                    borg_runtime::protocol::ExecutionProfile::CollaborationV1 { web_search: None },
                ),
                id: "fixed-coding".into(),
                revision: "7".into(),
                executor: ExecutorKind::Coding,
            },
            repository: Repository {
                registration: Some(borg_runtime::repository_catalogue::Revision {
                    repository_id: "fixture-repository".into(),
                    registration_revision: "1".into(),
                }),
                url: "file:///synthetic/repository".into(),
                start_commit: "a".repeat(40),
                checkpoint_remote: "fixture".into(),
            },
            acceptance_commands: vec![vec!["fixed-check".into()]],
            dependencies: vec!["fixed-prerequisite".into()],
            limits: Limits {
                deadline_unix_ms: input.limits.deadline_unix_ms,
                model_requests: 1,
                output_bytes: 1024,
                result_bytes: 4096,
                max_delegation_depth: 0,
            },
        },
    }));
    input
}

async fn step(
    input: &TaskInput,
    config: &NativeConfig,
    child: Option<&ChildOutcome>,
) -> NativeStepObservation {
    require_send(execute_step_observed_with_validation_program(
        input,
        config,
        child,
        std::path::Path::new(env!("CARGO_BIN_EXE_borg-runtime")),
        std::future::pending(),
    ))
    .await
}

async fn step_response(
    input: &TaskInput,
    text: &str,
    child: Option<&ChildOutcome>,
) -> (NativeStepObservation, Value) {
    let started_unix_ms = now_ms();
    let started = std::time::Instant::now();
    let f = Fixture::start(
        200,
        serde_json::to_vec(&with_usage(envelope(text))).unwrap(),
        Duration::ZERO,
        "",
    )
    .await;
    let result = step(input, &config(f.endpoint.clone()), child).await;
    eprintln!(
        "step_response: elapsed_ms={}, started_unix_ms={started_unix_ms}, finished_unix_ms={}, deadline_unix_ms={}, error={:?}, exposure={:?}, provider_outcome={:?}, usage={:?}",
        started.elapsed().as_millis(),
        now_ms(),
        input.limits.deadline_unix_ms,
        result.result.as_ref().err(),
        result.request_exposure,
        result.provider_outcome,
        result.usage,
    );
    let request = match tokio::time::timeout(Duration::from_secs(2), f.received).await {
        Ok(Ok(request)) => request,
        received => {
            f.handle.abort();
            let _ = f.handle.await;
            panic!(
                "step returned without a fixture request: received={received:?}, elapsed_ms={}, started_unix_ms={started_unix_ms}, observed_unix_ms={}, deadline_unix_ms={}, error={:?}, exposure={:?}, provider_outcome={:?}, usage={:?}",
                started.elapsed().as_millis(),
                now_ms(),
                input.limits.deadline_unix_ms,
                result.result.as_ref().err(),
                result.request_exposure,
                result.provider_outcome,
                result.usage,
            );
        }
    };
    f.handle.await.unwrap();
    (result, request)
}

#[tokio::test]
async fn native_workflow_requires_step_consumer_and_valid_first_budget() {
    let input = workflow_task();
    let cfg = config("http://127.0.0.1:1/v1/responses".into());
    let result = observed(&input, &cfg).await;
    assert_eq!(
        result.result.err().as_deref(),
        Some("native_step_consumer_required")
    );
    assert_eq!(result.request_exposure, RequestExposure::NotStarted);
    assert_eq!(result.usage, UsageObservation::Unknown);
    assert_eq!(result.provider_outcome, ProviderOutcome::NotStarted);
    let mut insufficient = input.clone();
    insufficient.limits.model_requests = 2;
    let refused = step(&insufficient, &cfg, None).await;
    assert_eq!(
        refused.result.err().as_deref(),
        Some("native_workflow_budget_invalid")
    );
    assert_eq!(refused.request_exposure, RequestExposure::NotStarted);
}

#[tokio::test]
async fn native_decision_uses_json_mode_and_delegates_only_validated_instruction() {
    let input = workflow_task();
    let (result, request) = step_response(
        &input,
        r#"{"kind":"delegate","instruction":"Fix the synthetic bug"}"#,
        None,
    )
    .await;
    assert_eq!(result.usage, reported());
    assert_eq!(
        result.request_exposure,
        RequestExposure::HttpResponseReceived
    );
    match result.result.unwrap_or_else(|e| panic!("{e}")) {
        NativeStep::DelegateCoding { instruction } => {
            assert_eq!(instruction, "Fix the synthetic bug")
        }
        NativeStep::Complete(_) => panic!("configured decision must delegate"),
    }
    assert_eq!(
        request["input"],
        borg_runtime::delegation::decision_instruction(&input).unwrap()
    );
    assert_eq!(request["text"]["format"], json!({"type":"json_object"}));
    assert!(request["input"].as_str().unwrap().contains("JSON"));
    let instruction = request["input"].as_str().unwrap();
    assert!(instruction.contains(r#"{"kind":"complete","result":...}"#));
    assert!(instruction.contains(r#"{"kind":"delegate","instruction":"..."}"#));
    assert!(instruction.contains("Return no other fields."));
    assert!(instruction.contains(
        "Do not provide repository, host, provider, credentials, checks, identity or limits. Those are fixed configuration."
    ));
    assert_eq!(request["model"], "gpt-6-astra");
    assert_eq!(request["reasoning"], json!({"effort":"max"}));
    assert_eq!(request["store"], false);
    assert_eq!(request["stream"], false);
    assert_eq!(
        request["max_output_tokens"],
        input.execution_limits().unwrap().native.max_output_tokens
    );
    assert!(request.get("tools").is_none());
    // Execution choices are absent from the model decision; the template stays
    // exactly in caller configuration and is not changed by this producer.
    let child = borg_runtime::delegation::coding_child(&input, "Fix the synthetic bug").unwrap();
    assert_eq!(child.definition.id, "fixed-coding");
    assert_eq!(child.definition.revision, "7");
    assert_eq!(child.dependencies, vec!["fixed-prerequisite"]);
    assert_eq!(child.acceptance_commands, vec![vec!["fixed-check"]]);
    assert!(child.native_workflow.is_none());
}

#[tokio::test]
async fn native_decision_refuses_malicious_or_ambiguous_branches() {
    let mut cases = vec![
        json!(null),
        json!([]),
        json!(true),
        json!({"kind":"delegate","instruction":""}),
        json!({"kind":"delegate","instruction":17}),
        json!({"kind":"delegate","instruction":"work","result":{"result":"good"}}),
        json!({"kind":"complete","result":{"result":"good"},"instruction":"work"}),
        json!({"kind":"unknown","instruction":"work"}),
        json!({"result":"good"}),
    ];
    for field in [
        "repository",
        "host",
        "credentials",
        "definition",
        "agent_id",
        "limits",
        "acceptance_commands",
    ] {
        let mut value = json!({"kind":"delegate","instruction":"work"});
        value[field] = json!("model-controlled");
        cases.push(value);
    }
    for value in cases {
        // Independent provider attempts each keep the same five-second bound;
        // earlier cases must not consume a later case's execution deadline.
        let input = workflow_task();
        let (result, request) = step_response(&input, &value.to_string(), None).await;
        assert_eq!(request["text"]["format"], json!({"type":"json_object"}));
        assert_eq!(
            result.result.err().as_deref(),
            Some("native_output_schema_mismatch")
        );
        assert_eq!(result.usage, reported());
        assert_eq!(result.provider_outcome, ProviderOutcome::Completed);
    }
    // JSON Schema maxLength counts characters; the actual instruction boundary
    // is bytes. This fits the envelope but exceeds the original byte budget.
    let mut small = workflow_task();
    small.limits.output_bytes = 16;
    let (result, _) = step_response(
        &small,
        &json!({"kind":"delegate","instruction":"é".repeat(9)}).to_string(),
        None,
    )
    .await;
    assert_eq!(
        result.result.err().as_deref(),
        Some("native_delegation_instruction_bound_task_output_bytes")
    );
    assert_eq!(result.usage, reported());
}

#[tokio::test]
async fn native_json_decision_preserves_arbitrary_complete_result_types() {
    for value in [
        json!(null),
        json!(true),
        json!(17),
        json!(1.25),
        json!("answer"),
        json!([1, {"nested":false}]),
    ] {
        let mut input = workflow_task();
        input.output_schema = Some(json!({"const":value}));
        let (result, request) = step_response(
            &input,
            &json!({"kind":"complete","result":value}).to_string(),
            None,
        )
        .await;
        assert_eq!(request["text"]["format"], json!({"type":"json_object"}));
        assert_eq!(result.usage, reported());
        assert_eq!(result.provider_outcome, ProviderOutcome::Completed);
        match result.result.unwrap_or_else(|e| panic!("{e}")) {
            NativeStep::Complete(result) => assert_eq!(result.output, value),
            NativeStep::DelegateCoding { .. } => panic!("complete data cannot delegate"),
        }
    }
}

#[tokio::test]
async fn native_json_decision_refuses_malformed_or_duplicate_json() {
    for text in [
        r#"{"kind":"delegate","instruction":"work""#,
        r#"{"kind":"complete","kind":"delegate","instruction":"work"}"#,
        r#"{"kind":"delegate","instruction":"work"} {}"#,
    ] {
        let input = workflow_task();
        let (result, request) = step_response(&input, text, None).await;
        assert_eq!(request["text"]["format"], json!({"type":"json_object"}));
        assert!(result.result.is_err());
        assert_eq!(result.usage, reported());
        assert_eq!(result.provider_outcome, ProviderOutcome::Completed);
    }
}

#[tokio::test]
async fn native_complete_revalidates_original_root_local_refs_and_retains_usage_on_refusal() {
    let mut input = workflow_task();
    input.output_schema = Some(
        json!({"$defs":{"answer":{"type":"object","properties":{"result":{"const":"good"}},"required":["result"],"additionalProperties":false}},"$ref":"#/$defs/answer"}),
    );
    for valid in [true, false] {
        let value = json!({"result":if valid {"good"} else {"bad"}});
        let (result, request) = step_response(
            &input,
            &json!({"kind":"complete","result":value}).to_string(),
            None,
        )
        .await;
        assert_eq!(result.usage, reported());
        assert_eq!(request["text"]["format"], json!({"type":"json_object"}));
        // Actual provider context includes the exact original schema as data,
        // with its root-local references unchanged, separately from the closed
        // decision schema and the subsequent local original-schema validation.
        let expected_schema = serde_json::to_string(input.output_schema.as_ref().unwrap()).unwrap();
        let expected_instruction = format!(
            "{}{}{}{}",
            borg_runtime::delegation::DECISION_INSTRUCTION,
            input.instruction,
            borg_runtime::delegation::DECISION_SCHEMA_SEPARATOR,
            expected_schema,
        );
        assert_eq!(request["input"], expected_instruction);
        let (_, supplied_schema) = request["input"]
            .as_str()
            .unwrap()
            .split_once(borg_runtime::delegation::DECISION_SCHEMA_SEPARATOR)
            .unwrap();
        assert_eq!(supplied_schema, expected_schema);
        assert_eq!(
            serde_json::from_str::<Value>(supplied_schema).unwrap(),
            *input.output_schema.as_ref().unwrap()
        );
        if valid {
            match result.result.unwrap_or_else(|e| panic!("{e}")) {
                NativeStep::Complete(complete) => {
                    assert_eq!(complete.output, value);
                    assert_eq!(complete.requests, 1);
                }
                NativeStep::DelegateCoding { .. } => panic!("not a delegate"),
            }
        } else {
            assert_eq!(
                result.result.err().as_deref(),
                Some("native_output_schema_mismatch")
            );
        }
    }
}

#[tokio::test]
async fn native_decision_framing_allowance_never_increases_final_output_budget() {
    let mut input = workflow_task();
    input.output_schema = Some(json!({"type":"string"}));
    input.limits.output_bytes = 64;
    for (characters, padding, accepted) in [(62, false, true), (63, false, false), (62, true, true)]
    {
        let value = json!("x".repeat(characters));
        let mut text = json!({"kind":"complete","result":value}).to_string();
        assert!(text.len() > input.limits.output_bytes);
        if padding {
            text.push_str(
                &" ".repeat(
                    input.limits.output_bytes
                        + input
                            .execution_limits()
                            .unwrap()
                            .native
                            .decision_envelope_bytes
                        - text.len(),
                ),
            );
        }
        let (result, _) = step_response(&input, &text, None).await;
        assert_eq!(result.usage, reported());
        if accepted {
            assert!(matches!(result.result, Ok(NativeStep::Complete(_))));
        } else {
            assert_eq!(
                result.result.err().as_deref(),
                Some("native_json_byte_bound_task_output_bytes")
            );
        }
    }
    let mut text = json!({"kind":"complete","result":"x"}).to_string();
    text.push_str(
        &" ".repeat(
            input.limits.output_bytes
                + input
                    .execution_limits()
                    .unwrap()
                    .native
                    .decision_envelope_bytes
                + 1
                - text.len(),
        ),
    );
    let (result, _) = step_response(&input, &text, None).await;
    assert!(result.result.is_err());
    assert_eq!(result.usage, reported());
}

#[tokio::test]
async fn native_plain_json_is_inert_and_continuation_is_result_only_with_remaining_one_request() {
    let data = json!({"kind":"delegate","instruction":"This is ordinary result data"});
    let mut plain = task();
    plain.output_schema = Some(json!(true));
    let (result, request) = step_response(&plain, &data.to_string(), None).await;
    assert_eq!(request["input"], plain.instruction);
    assert_eq!(
        request["text"]["format"],
        json!({
            "type":"json_schema", "name":"borg_native_result", "strict":false, "schema":true
        })
    );
    match result.result.unwrap_or_else(|e| panic!("{e}")) {
        NativeStep::Complete(value) => assert_eq!(value.output, data),
        NativeStep::DelegateCoding { .. } => panic!("ordinary data cannot delegate"),
    }
    for status in [
        TaskStatus::Failed,
        TaskStatus::Cancelled,
        TaskStatus::Succeeded,
    ] {
        let mut input = workflow_task();
        input.limits.model_requests = 1; // Actual remaining continuation accounting.
        input.output_schema = Some(json!(true));
        let child = ChildOutcome {
            task_id: "child-committed-17".into(),
            status,
            result: if status == TaskStatus::Succeeded {
                Some(AcceptedResult {
                    attempt_id: "child-attempt-1".into(),
                    summary: "actual child result".into(),
                    checkpoint: None,
                    validation: vec!["fixed-check passed".into()],
                    output: Some(json!({"original":true})),
                })
            } else {
                None
            },
            summary: format!("recorded {status:?} outcome; not recovered executor history"),
        };
        let (result, request) = step_response(&input, &data.to_string(), Some(&child)).await;
        assert_eq!(
            request["input"],
            borg_runtime::delegation::continuation_instruction(&input, &child).unwrap()
        );
        assert_eq!(
            request["text"]["format"],
            json!({
                "type":"json_schema", "name":"borg_native_result", "strict":false, "schema":true
            })
        );
        assert_eq!(result.usage, reported());
        match result.result.unwrap_or_else(|e| panic!("{e}")) {
            NativeStep::Complete(value) => assert_eq!(value.output, data),
            NativeStep::DelegateCoding { .. } => panic!("continuation cannot delegate"),
        }
    }
}

#[tokio::test]
async fn native_continuation_refuses_invalid_or_oversized_outcome_before_post() {
    let mut input = workflow_task();
    input.limits.model_requests = 1;
    let cfg = config("http://127.0.0.1:1/v1/responses".into());
    let cases = [
        (
            ChildOutcome {
                task_id: "child".into(),
                status: TaskStatus::Active,
                result: None,
                summary: "still running".into(),
            },
            "native_child_outcome_invalid",
        ),
        (
            ChildOutcome {
                task_id: "child".into(),
                status: TaskStatus::Succeeded,
                result: None,
                summary: "missing result".into(),
            },
            "native_child_outcome_invalid",
        ),
        (
            ChildOutcome {
                task_id: "child".into(),
                status: TaskStatus::Failed,
                result: None,
                summary: "x".repeat(64 * 1024),
            },
            "native_continuation_bound",
        ),
        (
            ChildOutcome {
                task_id: "child".into(),
                status: TaskStatus::Succeeded,
                result: Some(AcceptedResult {
                    attempt_id: "attempt".into(),
                    summary: "x".repeat(4096),
                    checkpoint: None,
                    validation: vec![],
                    output: None,
                }),
                summary: "result exceeds configured whole-result bound".into(),
            },
            "native_child_result_bound",
        ),
    ];
    for (child, error) in cases {
        let result = step(&input, &cfg, Some(&child)).await;
        assert_eq!(result.result.err().as_deref(), Some(error));
        assert_eq!(result.request_exposure, RequestExposure::NotStarted);
        assert_eq!(result.usage, UsageObservation::Unknown);
    }
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn native_decision_final_schema_cancellation_and_deadline_reap_and_preserve_usage() {
    for cancel in [true, false] {
        let program = LinkedProgram::new();
        let mut ready =
            borg_runtime::native::qualification_hold_direct_value_validation(&program.path);
        let f = Fixture::start(
            200,
            serde_json::to_vec(&with_usage(envelope(
                r#"{"kind":"complete","result":{"result":"good"}}"#,
            )))
            .unwrap(),
            Duration::ZERO,
            "",
        )
        .await;
        let mut input = workflow_task();
        if !cancel {
            input.limits.deadline_unix_ms = now_ms() + 1500;
            let NativeWorkflow::CodingThenContinue { child } =
                input.native_workflow.as_deref_mut().unwrap()
            else {
                panic!("expected coding workflow")
            };
            child.limits.deadline_unix_ms = input.limits.deadline_unix_ms;
        }
        let cfg = config(f.endpoint.clone());
        let (stop, stopped) = oneshot::channel::<()>();
        let mut running = Box::pin(execute_step_observed_with_validation_program(
            &input,
            &cfg,
            None,
            &program.path,
            async {
                let _ = stopped.await;
            },
        ));
        let pid = tokio::select! { result=&mut running => panic!("finished before final validation READY: {:?}",result.result.err()), pid=&mut ready => pid.unwrap() };
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
        assert!(!stat.rsplit_once(") ").unwrap().1.starts_with('Z'));
        if cancel {
            stop.send(()).unwrap();
        }
        let result = running.await;
        f.finish().await;
        assert_eq!(
            result.result.err().as_deref(),
            Some(if cancel {
                "native_cancelled"
            } else {
                "native_deadline_exceeded"
            })
        );
        assert_eq!(
            result.request_exposure,
            RequestExposure::HttpResponseReceived
        );
        assert_eq!(result.usage, reported());
        assert_eq!(result.provider_outcome, ProviderOutcome::Completed);
        assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    }
}

fn registry_records(path: &std::path::Path) -> Vec<std::path::PathBuf> {
    if !path.exists() {
        return Vec::new();
    }
    let mut paths = std::fs::read_dir(path)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("json"))
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn native_durable_host_records_final_validation_and_clears_verified_cleanup() {
    use borg_runtime::native::{ValidationHost, execute_step_observed_with_host};
    let program = LinkedProgram::new();
    let registry = program.directory.join("processes");
    let host = ValidationHost::new(
        program.path.clone(),
        registry.clone(),
        validation_policy(),
        validation_runtime(),
    );
    let mut ready = borg_runtime::native::qualification_hold_direct_value_validation(&program.path);
    let f = Fixture::start(
        200,
        serde_json::to_vec(&with_usage(envelope(
            r#"{"kind":"complete","result":{"result":"good"}}"#,
        )))
        .unwrap(),
        Duration::ZERO,
        "",
    )
    .await;
    let input = workflow_task();
    let cfg = config(f.endpoint.clone());
    let (stop, stopped) = oneshot::channel::<()>();
    let mut running = Box::pin(execute_step_observed_with_host(
        &input,
        &cfg,
        None,
        &host,
        async {
            let _ = stopped.await;
        },
    ));
    let pid = tokio::select! { result=&mut running => panic!("finished before durable final READY: {:?}",result.result.err()), pid=&mut ready => pid.unwrap() };
    let records = registry_records(&registry);
    assert_eq!(
        records.len(),
        1,
        "both preflights and provider validation were already cleaned"
    );
    let record: Value = serde_json::from_slice(&std::fs::read(&records[0]).unwrap()).unwrap();
    assert_eq!(record["pid"], pid);
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    let fields: Vec<_> = stat
        .rsplit_once(") ")
        .unwrap()
        .1
        .split_whitespace()
        .collect();
    assert_ne!(fields[0], "Z");
    assert_eq!(record["start_ticks"], fields[19].parse::<u64>().unwrap());
    stop.send(()).unwrap();
    let result = running.await;
    f.finish().await;
    assert_eq!(result.result.err().as_deref(), Some("native_cancelled"));
    assert_eq!(result.usage, reported());
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    assert!(registry_records(&registry).is_empty());
    assert!(program.children().is_empty());
    // The same durable host can perform a later schema/value validation after
    // verified cleanup; no process record from the cancelled call is reused.
    let schema = json!({"type":"integer"});
    let value = json!(7);
    let validated = borg_runtime::native::validate_json_with_host(
        &schema,
        Some(&value),
        64,
        tokio::time::Instant::now() + Duration::from_secs(5),
        &host,
    )
    .await
    .unwrap();
    assert!(validated.matches(&schema, Some(&value), 64));
    assert!(registry_records(&registry).is_empty());
    std::fs::remove_dir(&registry).unwrap();
}

#[tokio::test]
async fn native_durable_host_has_no_helper_during_provider_wait() {
    use borg_runtime::native::{ValidationHost, execute_step_observed_with_host};
    let program = LinkedProgram::new();
    let registry = program.directory.join("processes");
    let host = ValidationHost::new(
        program.path.clone(),
        registry.clone(),
        validation_policy(),
        validation_runtime(),
    );
    let mut f = Fixture::start(
        200,
        serde_json::to_vec(&with_usage(envelope(r#"{"result":"good"}"#))).unwrap(),
        Duration::from_secs(10),
        "",
    )
    .await;
    let input = task();
    let cfg = config(f.endpoint.clone());
    let (stop, stopped) = oneshot::channel::<()>();
    let mut running = Box::pin(execute_step_observed_with_host(
        &input,
        &cfg,
        None,
        &host,
        async {
            let _ = stopped.await;
        },
    ));
    tokio::select! { result=&mut running => panic!("finished before provider request: {:?}",result.result.err()), request=&mut f.received => {request.unwrap();} }
    assert!(registry_records(&registry).is_empty());
    assert!(program.children().is_empty());
    stop.send(()).unwrap();
    let result = running.await;
    tokio::time::timeout(Duration::from_secs(2), f.handle)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.result.err().as_deref(), Some("native_cancelled"));
    assert_eq!(
        result.request_exposure,
        RequestExposure::MayHaveReachedProvider
    );
    assert_eq!(result.usage, UsageObservation::Unknown);
    assert!(registry_records(&registry).is_empty());
    std::fs::remove_dir(&registry).unwrap();
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn native_dropped_validation_retains_owner_lock_until_quarantine_reaps() {
    use borg_runtime::native::{ValidationHost, validate_json_with_host};
    let program = LinkedProgram::new();
    let registry = program.directory.join("processes");
    let lock_path = program.directory.join("controller-owner.lock");
    let owner = std::sync::Arc::new(
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&lock_path)
            .unwrap(),
    );
    owner.try_lock().unwrap();
    let host = ValidationHost::with_owner_lock(
        program.path.clone(),
        registry.clone(),
        owner.clone(),
        validation_policy(),
        validation_runtime(),
    );
    let contender = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
        .unwrap();
    let mut ready = borg_runtime::native::qualification_hold_next_validation(&program.path);
    let schema = json!(true);
    let mut running = Box::pin(validate_json_with_host(
        &schema,
        None,
        64,
        tokio::time::Instant::now() + Duration::from_secs(5),
        &host,
    ));
    let pid = tokio::select! { result=&mut running => panic!("finished before owned READY: {}",result.is_ok()), pid=&mut ready => pid.unwrap() };
    assert_eq!(registry_records(&registry).len(), 1);
    assert!(contender.try_lock().is_err());
    // Drop all public ownership. Only State's moved quarantine owner can now
    // retain the actual file lock; synchronous kill request is not reaping.
    drop(running);
    drop(host);
    drop(owner);
    // Other parallel tests may observe the shared quarantine first. An already
    // released lock is valid only when actual helper/record cleanup is complete.
    if contender.try_lock().is_ok() {
        assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
        assert!(registry_records(&registry).is_empty());
        contender.unlock().unwrap();
    }
    let until = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        borg_runtime::native::validation_cleanup_pending();
        if contender.try_lock().is_ok() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < until,
            "native quarantine did not release verified owner"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    assert!(registry_records(&registry).is_empty());
    assert!(program.children().is_empty());
    drop(contender);
    std::fs::remove_file(&lock_path).unwrap();
    std::fs::remove_dir(&registry).unwrap();
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn native_post_spawn_registration_failure_retains_and_reaps_actual_owner() {
    use borg_runtime::native::{JsonValidationError, ValidationHost, validate_json_with_host};
    let program = LinkedProgram::new();
    let registry = program.directory.join("processes");
    let host = ValidationHost::new(
        program.path.clone(),
        registry.clone(),
        validation_policy(),
        validation_runtime(),
    );
    let pid = borg_runtime::worker::process::qualification_fail_next_registration(&registry);
    let result = validate_json_with_host(
        &json!(true),
        None,
        64,
        tokio::time::Instant::now() + Duration::from_secs(5),
        &host,
    )
    .await;
    let pid = pid
        .await
        .expect("failure hook observed a real spawned process");
    assert!(
        matches!(result, Err(JsonValidationError::Helper(ref code)) if code == "native_validation_registration_failed")
    );
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    assert!(registry_records(&registry).is_empty());
    assert!(program.children().is_empty());
    // The one-shot fault does not manufacture success or poison a later slot.
    let proof = validate_json_with_host(
        &json!(true),
        None,
        64,
        tokio::time::Instant::now() + Duration::from_secs(5),
        &host,
    )
    .await
    .unwrap();
    assert!(proof.matches(&json!(true), None, 64));
    assert!(registry_records(&registry).is_empty());
    std::fs::remove_dir(&registry).unwrap();
}

#[tokio::test]
async fn native_provider_completion_requires_the_entire_selected_envelope_shape() {
    let mut refusal = with_usage(envelope("{}"));
    refusal["output"][1]["content"] = json!([{"type":"refusal","refusal":"controlled refusal"}]);
    let mut later_unknown_part = refusal.clone();
    later_unknown_part["output"][1]["content"]
        .as_array_mut()
        .unwrap()
        .push(json!({"type":"unknown_future_part"}));
    let mut later_action = refusal.clone();
    later_action["output"]
        .as_array_mut()
        .unwrap()
        .push(json!({"type":"function_call","name":"never-executed"}));
    let mut invalid_refusal = refusal.clone();
    invalid_refusal["output"][1]["content"][0]["refusal"] = json!(17);
    let mut unfinished_message = with_usage(envelope(r#"{"result":"good"}"#));
    unfinished_message["output"][1]["status"] = json!("in_progress");
    for (body, expected) in [
        (refusal, ProviderOutcome::Completed),
        (later_unknown_part, ProviderOutcome::Unknown),
        (later_action, ProviderOutcome::Unknown),
        (invalid_refusal, ProviderOutcome::Unknown),
        (unfinished_message, ProviderOutcome::Unknown),
        (
            with_usage(json!({"status":"completed","output":[]})),
            ProviderOutcome::Unknown,
        ),
        (
            with_usage(json!({"status":"failed","error":{"code":"controlled"},"output":[]})),
            ProviderOutcome::Unknown,
        ),
    ] {
        let f = Fixture::start(200, serde_json::to_vec(&body).unwrap(), Duration::ZERO, "").await;
        let result = observed(&task(), &config(f.endpoint.clone())).await;
        f.finish().await;
        assert!(result.result.is_err());
        assert_eq!(result.provider_outcome, expected);
        assert_eq!(
            result.usage,
            reported(),
            "usage observation does not settle provider disposition"
        );
    }
    let mut duplicate =
        serde_json::to_string(&with_usage(envelope(r#"{"result":"good"}"#))).unwrap();
    duplicate.pop();
    duplicate.push_str(r#", "status":"completed"}"#);
    let f = Fixture::start(200, duplicate.into_bytes(), Duration::ZERO, "").await;
    let result = observed(&task(), &config(f.endpoint.clone())).await;
    f.finish().await;
    assert_eq!(
        result.result.err().as_deref(),
        Some("native_json_duplicate_key")
    );
    assert_eq!(result.provider_outcome, ProviderOutcome::Unknown);
}

#[tokio::test]
async fn native_http_errors_partial_body_and_lost_reply_remain_typed_unknown() {
    // Even a completed-looking body on a failed HTTP response cannot establish
    // a known provider outcome under this selected profile.
    for status in [401, 429, 503] {
        let f = Fixture::start(
            status,
            serde_json::to_vec(&with_usage(envelope(r#"{"result":"good"}"#))).unwrap(),
            Duration::ZERO,
            "",
        )
        .await;
        let result = observed(&task(), &config(f.endpoint.clone())).await;
        f.finish().await;
        assert!(result.result.is_err());
        assert_eq!(
            result.request_exposure,
            RequestExposure::HttpResponseReceived
        );
        assert_eq!(result.provider_outcome, ProviderOutcome::Unknown);
        assert_eq!(result.usage, UsageObservation::Unknown);
    }
    let f = Fixture::start_parts(
        200,
        serde_json::to_vec(&with_usage(envelope(r#"{"result":"good"}"#))).unwrap(),
        Duration::ZERO,
        "",
        1,
    )
    .await;
    let result = observed(&task(), &config(f.endpoint.clone())).await;
    f.finish().await;
    assert_eq!(
        result.result.err().as_deref(),
        Some("native_response_incomplete")
    );
    assert_eq!(result.provider_outcome, ProviderOutcome::Unknown);
    assert_eq!(result.usage, UsageObservation::Unknown);

    let listener = std::sync::Arc::new(TcpListener::bind("127.0.0.1:0").await.unwrap());
    let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
    let serving = listener.clone();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = serving.accept().await.unwrap();
        let mut wire = Vec::new();
        let mut chunk = [0; 1024];
        let header_end = loop {
            let n = stream.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            wire.extend_from_slice(&chunk[..n]);
            assert!(wire.len() <= 64 * 1024);
            if let Some(at) = wire.windows(4).position(|part| part == b"\r\n\r\n") {
                break at + 4;
            }
        };
        let headers = std::str::from_utf8(&wire[..header_end]).unwrap();
        assert!(headers.starts_with("POST /v1/responses HTTP/1.1\r\n"));
        let size: usize = headers
            .lines()
            .find_map(|line| {
                line.split_once(':')
                    .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .map(|(_, v)| v.trim().parse().unwrap())
            })
            .unwrap();
        assert!(size < 16 * 1024);
        while wire.len() < header_end + size {
            let n = stream.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            wire.extend_from_slice(&chunk[..n]);
        }
        let body: Value = serde_json::from_slice(&wire[header_end..header_end + size]).unwrap();
        assert_eq!(body["model"], "gpt-6-astra");
        assert_eq!(body["reasoning"], json!({"effort":"max"}));
        stream.shutdown().await.unwrap(); // Received full request; send no response.
    });
    let result = observed(&task(), &config(endpoint)).await;
    peer.await.unwrap();
    assert!(result.result.is_err());
    assert_eq!(
        result.request_exposure,
        RequestExposure::MayHaveReachedProvider
    );
    assert_eq!(result.provider_outcome, ProviderOutcome::Unknown);
    assert_eq!(result.usage, UsageObservation::Unknown);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err(),
        "uncertainty must not trigger a second POST"
    );
}

#[path = "native/task_tool.rs"]
mod task_tool;

#[path = "native/loop_step.rs"]
mod loop_step;

#[tokio::test]
async fn native_astra_selection_refuses_previous_model_before_network() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = config(format!(
        "http://{}/v1/responses",
        listener.local_addr().unwrap()
    ));
    cfg.model = "gpt-6.1-sol".into();
    let out = observed(&task(), &cfg).await;
    assert_eq!(
        out.result.err().as_deref(),
        Some("native_config_template_mismatch")
    );
    assert_eq!(out.request_exposure, RequestExposure::NotStarted);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn native_agent_token_override_above_seed_is_sent_without_hidden_clamp() {
    let f = Fixture::start(
        200,
        serde_json::to_vec(&envelope(r#"{"result":"good"}"#)).unwrap(),
        Duration::ZERO,
        "",
    )
    .await;
    let mut input = task();
    let mut native = input.execution_limits().unwrap().native.clone();
    native.max_output_tokens = native.max_output_tokens.checked_mul(2).unwrap();
    let expected = native.max_output_tokens;
    input.definition.limit_overrides =
        Some(Box::new(borg_runtime::limits::ExecutionLimitOverrides {
            native: Some(native),
            coding: None,
            collaboration: None,
            memory_notice: None,
        }));
    let input = bind(input);
    let result = require_send(execute(&input, &config(f.endpoint.clone()))).await;
    let request = f.received.await.unwrap();
    f.handle.await.unwrap();
    assert!(result.is_ok());
    assert_eq!(request["max_output_tokens"], expected);
    assert_eq!(
        input.execution_limits().unwrap().native.max_output_tokens,
        expected
    );
}

#[tokio::test]
async fn native_helper_error_buffer_bound_keeps_usage_and_provider_disposition() {
    let original = "native_output_not_json";
    for (error_bytes, expected) in [
        (original.len(), original),
        (
            original.len() - 1,
            "native_validation_reply_bound_native_helper_error_bytes",
        ),
        (1, "native_validation_reply_bound_native_helper_error_bytes"),
    ] {
        let mut input = task();
        let mut native = input.execution_limits().unwrap().native.clone();
        native.helper_error_bytes = error_bytes;
        input.definition.limit_overrides =
            Some(Box::new(borg_runtime::limits::ExecutionLimitOverrides {
                native: Some(native),
                coding: None,
                collaboration: None,
                memory_notice: None,
            }));
        let input = bind(input);
        let f = Fixture::start(
            200,
            serde_json::to_vec(&with_usage(envelope("not JSON"))).unwrap(),
            Duration::ZERO,
            "",
        )
        .await;
        let result = require_send(observed(&input, &config(f.endpoint.clone()))).await;
        f.finish().await;
        assert_eq!(result.result.err().as_deref(), Some(expected));
        assert_eq!(
            result.request_exposure,
            RequestExposure::HttpResponseReceived
        );
        assert_eq!(result.usage, reported());
        assert_eq!(result.provider_outcome, ProviderOutcome::Completed);
    }
}

#[tokio::test]
async fn native_decision_response_bound_identifies_both_selected_metadata_fields() {
    let input = workflow_task();
    let response_bytes = input.limits.output_bytes
        + input
            .execution_limits()
            .unwrap()
            .native
            .decision_envelope_bytes;
    let (result, _) = step_response(&input, &"x".repeat(response_bytes + 1), None).await;
    assert_eq!(
        result.result.err().as_deref(),
        Some("native_output_byte_bound_task_output_bytes_and_native_decision_envelope_bytes")
    );
    assert_eq!(
        result.request_exposure,
        RequestExposure::HttpResponseReceived
    );
    assert_eq!(result.usage, reported());
    assert_eq!(result.provider_outcome, ProviderOutcome::Completed);
}
