//! Actual executor/helper transport with controlled loopback Responses only.
//! These are carrier propagation checks, not durable controller receipt or
//! provider interoperability qualification. The parent owns post-header races.
use super::*;
use borg_runtime::native::{
    CategoryDiagnostic, CharterExchange, CharterInput, DiagnosticBodyState,
    HttpDiagnostic, HttpStatusDiagnostic, NativeLoopProfile, NumericDiagnostic,
    ProviderErrorCategory, ProviderFailureClass, ProviderFailureDiagnostic,
    ProviderRejection, RejectionKind, RetryAfter, RetryHeaderDiagnostic,
    SchedulerRetryDiagnostic, ValidationHost, continue_current_task,
    execute_charter_compaction, execute_loop_step, request_current_task,
};
use borg_runtime::task_tool::{CurrentTaskProjection, ToolObservation};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

const HEADERS: &str = concat!(
    "retry-after: 3\r\n",
    "retry-after-ms: 2500\r\n",
    "x-ratelimit-limit-requests: 600\r\n",
    "x-ratelimit-remaining-requests: 7\r\n",
    "x-ratelimit-limit-tokens: 30000\r\n",
    "x-ratelimit-remaining-tokens: 12\r\n",
    "x-ratelimit-reset-requests: 1.25s\r\n",
    "x-ratelimit-reset-tokens: 20ms\r\n",
);
const REFUSAL: &[u8] = br#"{"error":{"code":"rate_limit_exceeded"}}"#;

fn facts(
    diagnostic: &Option<ProviderFailureDiagnostic>,
    class: ProviderFailureClass,
    status: u16,
) -> &HttpDiagnostic {
    let diagnostic = diagnostic.as_ref().expect("typed diagnostic survives its display summary");
    assert_eq!(diagnostic.class, class);
    let http = diagnostic.http.as_ref().expect("actual response headers retained");
    assert_eq!(http.status, HttpStatusDiagnostic::Value(status));
    assert_eq!(http.retry_after, RetryHeaderDiagnostic::Seconds(3));
    assert_eq!(http.retry_after_ms, NumericDiagnostic::Value(2500));
    assert_eq!(http.requests_limit, NumericDiagnostic::Value(600));
    assert_eq!(http.requests_remaining, NumericDiagnostic::Value(7));
    assert_eq!(http.tokens_limit, NumericDiagnostic::Value(30000));
    assert_eq!(http.tokens_remaining, NumericDiagnostic::Value(12));
    assert_eq!(http.requests_reset_ms, NumericDiagnostic::Value(1250));
    assert_eq!(http.tokens_reset_ms, NumericDiagnostic::Value(20));
    http
}

async fn closed_peer(peer: Fixture) -> Value {
    let request = tokio::time::timeout(Duration::from_secs(2), peer.received)
        .await.unwrap().unwrap();
    // Await natural completion; Fixture::finish aborts and would not prove it.
    tokio::time::timeout(Duration::from_secs(2), peer.handle)
        .await.unwrap().unwrap();
    request
}

fn host() -> ValidationHost {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let registry = std::env::temp_dir().join(format!(
        "borg-diagnostic-carrier-{}-{}-{}",
        std::process::id(), now_ms(), NEXT.fetch_add(1, Ordering::Relaxed),
    ));
    std::fs::create_dir(&registry).unwrap();
    ValidationHost::new(
        env!("CARGO_BIN_EXE_borg-runtime").into(), registry,
        validation_policy(), validation_runtime(),
    )
}

fn closed_host(host: ValidationHost) {
    let registry = host.registry.clone();
    assert_eq!(std::fs::read_dir(&registry).unwrap().count(), 0,
        "actual helper records are removed only after verified cleanup");
    drop(host);
    std::fs::remove_dir(registry).unwrap();
}

fn known_refusal() -> ProviderOutcome {
    ProviderOutcome::Rejected(ProviderRejection {
        kind: RejectionKind::RateLimited,
        retry_after: RetryAfter::DelayMs(3000),
    })
}

#[tokio::test]
async fn tiny_summary_keeps_http_carrier_without_upgrading_unknown_or_retry_policy() {
    for known in [true, false] {
        let mut input = task();
        let mut selected = metadata();
        selected.execution.native.summary_bytes = 1;
        input.resolved_limits = Some(selected.resolve_task(&input).unwrap());
        let body = if known { REFUSAL.to_vec() } else { b"not-json".to_vec() };
        let peer = Fixture::start(429, body, Duration::ZERO, HEADERS).await;
        let value = observed(&input, &config(peer.endpoint.clone())).await;
        let _request = closed_peer(peer).await;
        assert_eq!(value.result.as_ref().err().map(String::as_str),
            Some("native_http_diagnostic_bound_native_summary_bytes"));
        assert_eq!(value.request_exposure, RequestExposure::HttpResponseReceived);
        assert_eq!(value.usage, UsageObservation::Unknown);
        assert_eq!(value.provider_outcome, if known {
            ProviderOutcome::Rejected(ProviderRejection {
                kind: RejectionKind::RateLimited,
                // Historical scheduling still uses its accepted summary bound.
                retry_after: RetryAfter::Invalid,
            })
        } else { ProviderOutcome::Unknown });
        let http = facts(&value.diagnostic, ProviderFailureClass::HttpRefused, 429);
        assert_eq!(http.scheduler_retry_after, SchedulerRetryDiagnostic::Value(RetryAfter::Invalid));
        assert_eq!(http.body, if known { DiagnosticBodyState::Complete }
            else { DiagnosticBodyState::InvalidJson });
        assert_eq!(http.category, if known {
            CategoryDiagnostic::Value(ProviderErrorCategory::RateLimited)
        } else { CategoryDiagnostic::Absent });
    }
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn completed_usage_then_cancel_preserves_http_carrier_and_reaps_helper() {
    let program = LinkedProgram::new();
    let mut ready = borg_runtime::native::qualification_hold_provider_after_usage(&program.path);
    let peer = Fixture::start(200,
        serde_json::to_vec(&with_usage(envelope(r#"{"result":"good"}"#))).unwrap(),
        Duration::ZERO, HEADERS).await;
    let input = task();
    let cfg = config(peer.endpoint.clone());
    let (stop, stopped) = oneshot::channel::<()>();
    let mut running = Box::pin(execute_observed_with_validation_program(
        &input, &cfg, &program.path, async { let _ = stopped.await; },
    ));
    let pid = tokio::select! {
        result = &mut running => panic!("finished before usage READY: {}", result.result.is_ok()),
        pid = &mut ready => pid.unwrap(),
    };
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    assert!(!stat.rsplit_once(") ").unwrap().1.starts_with('Z'));
    stop.send(()).unwrap();
    let value = running.await;
    let _request = closed_peer(peer).await;
    assert_eq!(value.result.as_ref().err().map(String::as_str), Some("native_cancelled"));
    assert_eq!(value.request_exposure, RequestExposure::HttpResponseReceived);
    assert_eq!(value.usage, reported());
    assert_eq!(value.provider_outcome, ProviderOutcome::Completed);
    facts(&value.diagnostic, ProviderFailureClass::Cancelled, 200);
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    assert!(program.children().is_empty());
}

#[tokio::test]
async fn malformed_success_body_keeps_actual_http_facts_and_unknown_outcome() {
    let peer = Fixture::start(200, b"{broken".to_vec(), Duration::ZERO, HEADERS).await;
    let value = observed(&task(), &config(peer.endpoint.clone())).await;
    let _request = closed_peer(peer).await;
    assert!(value.result.is_err());
    assert_eq!(value.request_exposure, RequestExposure::HttpResponseReceived);
    assert_eq!(value.usage, UsageObservation::Unknown);
    assert_eq!(value.provider_outcome, ProviderOutcome::Unknown);
    let http = facts(&value.diagnostic, ProviderFailureClass::ResponseValidation, 200);
    assert_eq!(http.category, CategoryDiagnostic::Absent);
}

#[tokio::test]
async fn loop_task_tool_and_compaction_keep_the_same_http_failure_carrier() {
    // Each iteration calls its real public producer and real preflight helper.
    // A complete refusal is distinct from successful model/tool execution.
    for producer in 0..3 {
        let mut input = task();
        if producer == 1 {
            input.native_workflow = Some(Box::new(NativeWorkflow::InspectCurrentTaskThenContinue {}));
            input.limits.model_requests = 2;
            input = bind(input);
        }
        let mut host = host();
        if producer == 2 {
            host = host.with_charter(Arc::new(CharterExchange::new(CharterInput {
                revision: json!({"definition_id":"native-fixture","definition_revision":"1"}),
                source: "# Purpose\n\nPreserve the exact task instruction.\n".into(),
                changed: true,
            }, &metadata().execution.native).unwrap()));
        }
        let peer = Fixture::start(429, REFUSAL.to_vec(), Duration::ZERO, HEADERS).await;
        let cfg = config(peer.endpoint.clone());
        let (failed, exposure, usage, outcome, diagnostic) = match producer {
            0 => {
                let value = require_send(execute_loop_step(&input, &cfg,
                    &NativeLoopProfile::default(), None, &host,
                    async { Ok(()) }, std::future::pending())).await;
                (value.result.is_err(), value.request_exposure, value.usage,
                    value.provider_outcome, value.diagnostic)
            }
            1 => {
                let value = require_send(request_current_task(&input, &cfg, &host,
                    async { Ok(()) }, std::future::pending())).await;
                (value.result.is_err(), value.request_exposure, value.usage,
                    value.provider_outcome, value.diagnostic)
            }
            _ => {
                let value = require_send(execute_charter_compaction(&input, &cfg, &json!([]),
                    &host, async { Ok(()) }, std::future::pending())).await;
                (value.result.is_err(), value.request_exposure, value.usage,
                    value.provider_outcome, value.diagnostic)
            }
        };
        let request = closed_peer(peer).await;
        assert!(failed);
        assert_eq!(exposure, RequestExposure::HttpResponseReceived);
        assert_eq!(usage, UsageObservation::Unknown);
        assert_eq!(outcome, known_refusal());
        let http = facts(&diagnostic, ProviderFailureClass::HttpRefused, 429);
        assert_eq!(http.body, DiagnosticBodyState::Complete);
        assert_eq!(http.category, CategoryDiagnostic::Value(ProviderErrorCategory::RateLimited));
        if producer == 2 { assert!(request.get("tools").is_none()); }
        closed_host(host);
    }
}

#[tokio::test]
async fn task_tool_continuation_preserves_failure_carrier_after_actual_call_response() {
    let mut input = task();
    input.native_workflow = Some(Box::new(NativeWorkflow::InspectCurrentTaskThenContinue {}));
    input.limits.model_requests = 2;
    input.limits.output_bytes = 4096;
    input = bind(input);
    let host = host();
    let call_response = with_usage(json!({"status":"completed","output":[{
        "type":"function_call","status":"completed","call_id":"diagnostic-call",
        "name":"get_current_task","arguments":"{}"
    }]}));
    let first = Fixture::start(200, serde_json::to_vec(&call_response).unwrap(),
        Duration::ZERO, HEADERS).await;
    let first_value = request_current_task(&input, &config(first.endpoint.clone()), &host,
        async { Ok(()) }, std::future::pending()).await;
    let _first_request = closed_peer(first).await;
    assert_eq!(first_value.provider_outcome, ProviderOutcome::Completed);
    assert_eq!(first_value.usage, reported());
    assert!(first_value.diagnostic.is_none(), "successful response is not a failure diagnostic");
    let call = first_value.result.unwrap();
    let observation = ToolObservation {
        call_id: call.call_id().into(),
        task: CurrentTaskProjection {
            task_id: "task".into(), agent_id: input.definition.id.clone(),
            definition: input.definition.clone(), status: TaskStatus::Active,
            observed_controller_revision: 1, deadline_unix_ms: input.limits.deadline_unix_ms,
            attempt_id: "attempt".into(), generation: 1,
        },
    };
    let second = Fixture::start(429, REFUSAL.to_vec(), Duration::ZERO, HEADERS).await;
    let value = continue_current_task(&input, &config(second.endpoint.clone()), &call,
        &observation, &host, async { Ok(()) }, std::future::pending()).await;
    let request = closed_peer(second).await;
    assert_eq!(request["tools"], json!([]));
    assert_eq!(request["tool_choice"], "none");
    assert!(value.result.is_err());
    assert_eq!(value.request_exposure, RequestExposure::HttpResponseReceived);
    assert_eq!(value.usage, UsageObservation::Unknown);
    assert_eq!(value.provider_outcome, known_refusal());
    facts(&value.diagnostic, ProviderFailureClass::HttpRefused, 429);
    closed_host(host);
}
