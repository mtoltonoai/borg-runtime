//! Actual central HTTP sender cancellation, before any body is available.
//! Canonical diagnostic receipt/outbox durability is a separate worker join.
use super::*;
use std::sync::Arc;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener, sync::oneshot};

fn host(sink: Arc<request_observation::Sink>) -> ValidationHost {
    let limits = crate::limits::RuntimeLimits::seed().unwrap();
    let runtime = ValidationRuntime::new(Arc::new(limits.worker)).unwrap();
    let mut host = ValidationHost::new("/unused-program".into(), "/unused-registry".into(),
        Arc::new(limits.execution.native), Arc::new(runtime));
    host.request_capture = Some(sink);
    host
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn headers_survive_actual_body_cancellation_and_deadline_without_retry() {
    for status in [200, 429] {
        for cancel in [true, false] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (mut stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept()).await.unwrap().unwrap();
                let mut bytes = Vec::new();
                let mut buffer = [0; 1024];
                let end = loop {
                    let n = stream.read(&mut buffer).await.unwrap();
                    assert!(n > 0); bytes.extend_from_slice(&buffer[..n]);
                    assert!(bytes.len() < 8192);
                    if let Some(p) = bytes.windows(4).position(|v| v == b"\r\n\r\n") { break p + 4; }
                };
                let length: usize = std::str::from_utf8(&bytes[..end]).unwrap().lines()
                    .find_map(|line| line.split_once(':').filter(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                        .map(|(_, value)| value.trim().parse().unwrap())).unwrap();
                while bytes.len() < end + length {
                    let n = stream.read(&mut buffer).await.unwrap(); assert!(n > 0);
                    bytes.extend_from_slice(&buffer[..n]); assert!(bytes.len() < 8192);
                }
                stream.write_all(format!("HTTP/1.1 {status} fixture\r\ncontent-length: 128\r\nretry-after: 3\r\nx-ratelimit-remaining-tokens: 17\r\nconnection: close\r\n\r\n").as_bytes()).await.unwrap();
                // The body remains unavailable. The local cancelled HTTP future
                // must close naturally; this is not a fixture abort as evidence.
                let closed = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buffer)).await.unwrap();
                assert!(matches!(closed, Ok(0)) || closed.is_err());
                assert!(tokio::time::timeout(Duration::from_millis(30), listener.accept()).await.is_err());
            });
            let (sink, events) = request_observation::Sink::capture(16);
            let host = host(sink);
            let (stop, stopped) = oneshot::channel::<()>();
            let work = tokio::spawn(async move {
                let config = NativeConfig { endpoint: endpoint.clone(), api_key_env: "HOME".into(), model: "fixture-model".into() };
                let mut cpu = cpu::State::new(&host);
                let mut exposure = RequestExposure::NotStarted;
                // Exercise the task deadline before the unchanged, independently
                // accepted error-body read timeout (250ms in this fixture).
                let task_ms = host.native_limits.error_read_timeout_ms / 2;
                assert!(task_ms > 0);
                let deadline = Instant::now() + Duration::from_millis(task_ms);
                let interrupted = {
                    let body = json!({"input":"fixture"});
                    let request = send_request_observed(Url::parse(&endpoint).unwrap(), &config, &body,
                        1024, deadline, &mut exposure, &host, &mut cpu);
                    tokio::pin!(request);
                    tokio::select! { biased;
                        _ = stopped => Some(ProviderFailureClass::Cancelled),
                        _ = tokio::time::sleep_until(deadline) => Some(ProviderFailureClass::Deadline),
                        result = &mut request => {
                            // The HTTP error reader uses this exact same deadline.
                            // Either timer may be polled first; neither permits
                            // an early body failure to satisfy this test.
                            assert!(!cancel);
                            assert!(result.is_err());
                            assert!(Instant::now() >= deadline);
                            None
                        },
                    }
                };
                if let Some(class) = interrupted { cpu.record_failure(class); }
                else { cpu.record_result_failure(exposure, deadline); }
                cpu.finish(true).await.unwrap();
                let facts = cpu.finish_diagnostic(true, exposure).unwrap();
                (facts, exposure, cpu.provider_usage(), cpu.provider_completed(), cpu.provider_rejection())
            });
            loop {
                let event = events.recv_timeout(Duration::from_secs(5)).unwrap();
                if matches!(event, request_observation::Observation::Http(v) if v.phase == request_observation::Phase::HeadersReceived) { break; }
            }
            if cancel { stop.send(()).unwrap(); }
            let (facts, exposure, usage, completed, rejection) = work.await.unwrap();
            assert_eq!(facts.class, if cancel { ProviderFailureClass::Cancelled } else { ProviderFailureClass::Deadline });
            let http = facts.http.unwrap();
            assert_eq!(http.status, HttpStatusDiagnostic::Value(status));
            assert_eq!(http.body, if cancel { DiagnosticBodyState::Cancelled } else { DiagnosticBodyState::TaskDeadline });
            assert_eq!(http.retry_after, RetryHeaderDiagnostic::Seconds(3));
            assert_eq!(http.tokens_remaining, NumericDiagnostic::Value(17));
            assert_eq!(exposure, RequestExposure::HttpResponseReceived);
            assert_eq!(usage, UsageObservation::Unknown);
            assert!(!completed); assert_eq!(rejection, None);
            server.await.unwrap();
        }
    }
}

#[test]
fn recording_cleanup_failure_preserves_original_cause_and_observed_headers() {
    let (sink, _) = request_observation::Sink::capture(16);
    let host = host(sink);
    let mut cpu = cpu::State::new(&host);
    let http = HttpDiagnostic {
        status: HttpStatusDiagnostic::Value(503), body: DiagnosticBodyState::Complete,
        category: CategoryDiagnostic::Absent,
        scheduler_retry_after: SchedulerRetryDiagnostic::Value(RetryAfter::Absent),
        retry_after: RetryHeaderDiagnostic::Seconds(3), retry_after_ms: NumericDiagnostic::Absent,
        requests_limit: NumericDiagnostic::Absent, requests_remaining: NumericDiagnostic::Absent,
        tokens_limit: NumericDiagnostic::Absent, tokens_remaining: NumericDiagnostic::Value(17),
        requests_reset_ms: NumericDiagnostic::Absent, tokens_reset_ms: NumericDiagnostic::Absent,
        quota: QuotaDiagnostic::Absent,
    };
    cpu.record_diagnostic(ProviderFailureDiagnostic::http(ProviderFailureClass::HttpRefused, http.clone()));
    cpu.record_failure(ProviderFailureClass::ValidationCleanup);
    let facts = cpu.finish_diagnostic(true, RequestExposure::HttpResponseReceived).unwrap();
    assert_eq!(facts.class, ProviderFailureClass::HttpRefused);
    assert_eq!(facts.http, Some(http));
    let mut cpu = cpu::State::new(&host);
    cpu.record_failure(ProviderFailureClass::ValidationCleanup);
    assert_eq!(cpu.finish_diagnostic(true, RequestExposure::NotStarted).unwrap().class,
        ProviderFailureClass::ValidationCleanup);
}
