use super::*;
use crate::native::{NativeConfig, RequestExposure, send_request};
use crate::protocol::AttemptPhase;
use serde_json::json;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
    task::JoinHandle,
};

fn attempt() -> Attempt {
    let task_id = "task-0123456789abcdef0123456789abcdef-00000000000000000042".to_owned();
    Attempt {
        session_id: "session-1".into(),
        id: format!("attempt-{task_id}-3"),
        task_id,
        generation: 7,
        worker_id: "operator-value-must-not-be-logged".into(),
        worker_incarnation: "operator-incarnation-must-not-be-logged".into(),
        phase: AttemptPhase::Running,
        deadline_unix_ms: 0,
        stopped: false,
        uncertain: false,
    }
}
fn host(sink: Arc<Sink>) -> ValidationHost {
    let limits = crate::limits::RuntimeLimits::seed().unwrap();
    let mut runtime = crate::native::ValidationRuntime::new(Arc::from(limits.worker)).unwrap();
    runtime.request_process.sink = (*sink).clone();
    let runtime = Arc::new(runtime);
    let mut host = ValidationHost::new(
        "/unused-program".into(),
        "/unused-registry".into(),
        Arc::new(limits.execution.native),
        runtime,
    )
    .with_request_attempt(&attempt());
    host.request_capture = Some(sink);
    host
}
fn observation(rx: &mpsc::Receiver<Observation>) -> Observation {
    rx.recv_timeout(Duration::from_secs(5))
        .expect("bounded diagnostic event")
}
fn receive(rx: &mpsc::Receiver<Observation>) -> Record {
    match observation(rx) {
        Observation::Http(record) => record,
        Observation::Clock(_) => panic!("unexpected clock record"),
    }
}
fn clock(rx: &mpsc::Receiver<Observation>) -> ClockRecord {
    match observation(rx) {
        Observation::Clock(record) => record,
        Observation::Http(_) => panic!("unexpected HTTP record"),
    }
}

struct Fixture {
    endpoint: String,
    received: oneshot::Receiver<()>,
    headers: oneshot::Sender<()>,
    body: oneshot::Sender<()>,
    handle: JoinHandle<()>,
}
impl Fixture {
    async fn start(status: u16, body: &'static [u8], length: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
        let (sent, received) = oneshot::channel();
        let (headers, wait_headers) = oneshot::channel();
        let (release_body, wait_body) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let (mut stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut bytes = Vec::new();
            let mut buffer = [0; 1024];
            let header_end = loop {
                let n = stream.read(&mut buffer).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buffer[..n]);
                assert!(bytes.len() < 16 * 1024);
                if let Some(end) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
                    break end + 4;
                }
            };
            let headers = std::str::from_utf8(&bytes[..header_end]).unwrap();
            assert!(headers.starts_with("POST /v1/responses HTTP/1.1\r\n"));
            let len: usize = headers
                .lines()
                .find_map(|line| {
                    line.split_once(':')
                        .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                        .map(|(_, n)| n.trim().parse().unwrap())
                })
                .unwrap();
            assert!(len < 8192);
            while bytes.len() < header_end + len {
                let n = stream.read(&mut buffer).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buffer[..n]);
            }
            let _ = sent.send(());
            if wait_headers.await.is_err() {
                return;
            }
            if status == 0 {
                return;
            } // Established connection, no HTTP headers.
            let headers = format!(
                "HTTP/1.1 {status} fixture\r\ncontent-length: {length}\r\nconnection: close\r\n\r\n"
            );
            if stream.write_all(headers.as_bytes()).await.is_err() {
                return;
            }
            if wait_body.await.is_err() {
                return;
            }
            let _ = stream.write_all(body).await;
            let _ = stream.shutdown().await;
        });
        Self {
            endpoint,
            received,
            headers,
            body: release_body,
            handle,
        }
    }
}
fn request(
    endpoint: String,
    host: Arc<ValidationHost>,
) -> JoinHandle<(Result<Vec<u8>, String>, RequestExposure)> {
    tokio::spawn(async move {
        // Public synthetic HOME credential, loopback only, no environment write.
        let config = NativeConfig {
            endpoint: endpoint.clone(),
            api_key_env: "HOME".into(),
            model: "fixture-model-not-logged".into(),
        };
        let mut exposure = RequestExposure::NotStarted;
        let result = send_request(
            reqwest::Url::parse(&endpoint).unwrap(),
            &config,
            &json!({"input":"prompt-value-must-not-be-logged"}),
            1024,
            tokio::time::Instant::now() + Duration::from_secs(5),
            &mut exposure,
            &host,
        )
        .await;
        (result.map(|(wire, _)| wire), exposure)
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_pending_requests_have_correlated_overlapping_intervals_and_distinct_eof() {
    let (sink, rx) = Sink::capture(16);
    let host = Arc::new(host(sink));
    let mut fixtures = Vec::new();
    let mut requests = Vec::new();
    for _ in 0..3 {
        let fixture = Fixture::start(200, b"fixture-body", 12).await;
        requests.push(request(fixture.endpoint.clone(), host.clone()));
        fixtures.push(fixture);
    }
    let mut release = Vec::new();
    for fixture in fixtures {
        fixture.received.await.unwrap();
        release.push((fixture.headers, fixture.body, fixture.handle));
    }
    let starts: Vec<_> = (0..3).map(|_| receive(&rx)).collect();
    assert!(
        starts
            .iter()
            .all(|r| r.phase == Phase::DispatchPending && r.headers_ns.is_none())
    );
    let mut bodies = Vec::new();
    for (headers, body, handle) in release {
        headers.send(()).unwrap();
        bodies.push((body, handle));
    }
    let headers: Vec<_> = (0..3).map(|_| receive(&rx)).collect();
    assert!(headers.iter().all(|r| r.phase == Phase::HeadersReceived
        && r.body_eof_ns.is_none()
        && r.end_ns.is_none()));
    assert!(
        starts.iter().map(|r| r.dispatch_ns).max().unwrap()
            <= headers.iter().map(|r| r.headers_ns.unwrap()).min().unwrap()
    );
    for (body, handle) in bodies {
        body.send(()).unwrap();
        handle.await.unwrap();
    }
    for request in requests {
        let (result, exposure) = request.await.unwrap();
        assert_eq!(result.unwrap(), b"fixture-body");
        assert_eq!(exposure, RequestExposure::HttpResponseReceived);
    }
    let ends: Vec<_> = (0..3).map(|_| receive(&rx)).collect();
    assert!(
        headers.iter().map(|r| r.headers_ns.unwrap()).max().unwrap()
            <= ends.iter().map(|r| r.end_ns.unwrap()).min().unwrap()
    );
    for end in ends {
        let start = starts.iter().find(|r| r.request == end.request).unwrap();
        assert_eq!(end.phase, Phase::Terminal);
        assert_eq!(end.end, Some(End::BodyComplete));
        assert!(end.dispatch_ns <= end.headers_ns.unwrap());
        assert!(end.headers_ns.unwrap() <= end.body_eof_ns.unwrap());
        assert!(end.body_eof_ns.unwrap() <= end.end_ns.unwrap());
        assert_eq!(end.dispatch_ns, start.dispatch_ns);
        assert_eq!(end.pid, std::process::id());
        assert!(end.process_start_ticks.is_some_and(|n| n > 0));
        assert!(end.anchor_unix_ns.is_some());
        assert!(end.anchor_after_ns <= end.dispatch_ns);
        let correlation = end.correlation.as_ref().unwrap();
        assert_eq!(correlation.task_id, attempt().task_id);
        assert_eq!(correlation.attempt_id, attempt().id);
        assert_eq!(correlation.generation, 7);
        let encoded = serde_json::to_string(&end).unwrap();
        for secret in [
            "prompt-value",
            "operator-value",
            "operator-incarnation",
            "fixture-model",
            "fixture-body",
            "Bearer",
            "authorization",
            "api_key",
        ] {
            assert!(!encoded.contains(secret));
        }
        assert!(!encoded.contains(&std::env::var("HOME").unwrap()));
        assert!(encoded.len() < 2048);
        assert_eq!(end.enqueue_dropped_total, 0);
        assert_eq!(end.write_failed_total, 0);
    }
    assert!(rx.try_recv().is_err()); // Exactly start, headers, terminal per request.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_refusal_partial_body_bound_and_transport_failure_remain_distinct() {
    for (status, body, length, terminal, error, count) in [
        (
            429,
            b"{}".as_slice(),
            2,
            End::HttpRefused,
            "native_provider_rate_limited",
            3,
        ),
        (
            200,
            b"short".as_slice(),
            50,
            End::BodyFailed,
            "native_response_incomplete",
            3,
        ),
        (
            200,
            b"".as_slice(),
            crate::limits::RuntimeLimits::seed()
                .unwrap()
                .execution
                .native
                .response_wire_bytes
                + 1,
            End::ByteBound,
            "native_response_byte_bound_native_response_wire_bytes",
            3,
        ),
        (
            0,
            b"".as_slice(),
            0,
            End::TransportFailed,
            "native_transport_failed_outcome_unknown",
            2,
        ),
    ] {
        let (sink, rx) = Sink::capture(4);
        let fixture = Fixture::start(status, body, length).await;
        let work = request(fixture.endpoint, Arc::new(host(sink)));
        fixture.received.await.unwrap();
        fixture.headers.send(()).unwrap();
        let _ = fixture.body.send(());
        fixture.handle.await.unwrap();
        let (result, _) = work.await.unwrap();
        assert!(result.unwrap_err().starts_with(error));
        let records: Vec<_> = (0..count).map(|_| receive(&rx)).collect();
        let end = records.last().unwrap();
        assert_eq!(end.end, Some(terminal));
        assert!(end.body_eof_ns.is_none());
        assert_eq!(
            end.http_status,
            if status == 0 { None } else { Some(status) }
        );
        assert!(rx.try_recv().is_err());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_pending_http_future_records_local_drop_without_inventing_headers_or_eof() {
    let (sink, rx) = Sink::capture(4);
    let fixture = Fixture::start(200, b"{}", 2).await;
    let work = request(fixture.endpoint, Arc::new(host(sink)));
    fixture.received.await.unwrap();
    assert_eq!(receive(&rx).phase, Phase::DispatchPending);
    work.abort();
    assert!(work.await.unwrap_err().is_cancelled());
    let end = receive(&rx);
    assert_eq!(end.end, Some(End::FutureDropped));
    assert!(end.end_ns.is_some());
    assert!(end.headers_ns.is_none() && end.http_status.is_none() && end.body_eof_ns.is_none());
    drop(fixture.headers);
    drop(fixture.body);
    fixture.handle.await.unwrap();
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn local_admission_failure_does_not_emit_dispatch() {
    let (sink, rx) = Sink::capture(4);
    let host = host(sink);
    let config = NativeConfig {
        endpoint: "http://127.0.0.1:1/v1/responses".into(),
        api_key_env: "invalid-key-name".into(),
        model: "fixture".into(),
    };
    let mut exposure = RequestExposure::NotStarted;
    let result = send_request(
        reqwest::Url::parse(&config.endpoint).unwrap(),
        &config,
        &json!({}),
        1024,
        tokio::time::Instant::now() + Duration::from_secs(5),
        &mut exposure,
        &host,
    )
    .await;
    assert_eq!(result.unwrap_err(), "native_key_configuration_invalid");
    assert_eq!(exposure, RequestExposure::NotStarted);
    assert!(rx.try_recv().is_err());
}

#[test]
fn bounded_queue_reports_loss_without_blocking_or_duplicating_terminal() {
    let (sink, rx) = Sink::capture(1);
    let host = host(sink);
    let mut first = Interval::dispatch(&host); // Fills queue.
    first.headers(200); // Drops rather than blocks.
    first.finish(End::BodyComplete); // Drops rather than blocks.
    drop(first); // Must not emit a second terminal.
    assert_eq!(receive(&rx).enqueue_dropped_total, 0);
    let second = Interval::dispatch(&host);
    assert_eq!(receive(&rx).enqueue_dropped_total, 2);
    drop(second);
    assert_eq!(receive(&rx).end, Some(End::FutureDropped));
    assert!(rx.try_recv().is_err());
}

#[test]
fn process_identity_read_uses_worker_metadata_bound() {
    let mut worker = crate::limits::RuntimeLimits::seed().unwrap().worker;
    worker.native_observation_proc_bytes = 1;
    assert!(Process::new(&worker).start_ticks.is_none());
    worker.native_observation_proc_bytes = std::fs::read("/proc/self/stat").unwrap().len() * 2;
    assert!(Process::new(&worker).start_ticks.is_some_and(|n| n > 0));
}

#[test]
fn correlation_rejects_non_generated_values_without_retaining_them() {
    let mut value = attempt();
    assert!(Correlation::from_attempt(&value).is_some());
    value.task_id = "secret-user-controlled-value".into();
    assert!(Correlation::from_attempt(&value).is_none());
    value = attempt();
    value.id.push_str("\nsecret");
    assert!(Correlation::from_attempt(&value).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_clock_probes_are_inside_actual_pending_http_without_changing_lifetimes() {
    let (sink, rx) = Sink::capture(16);
    let host = Arc::new(host(sink));
    let mut pending = Vec::new();
    let mut starts = Vec::new();
    for _ in 0..3 {
        let fixture = Fixture::start(200, b"original-body", 13).await;
        let work = request(fixture.endpoint.clone(), host.clone());
        fixture.received.await.unwrap();
        starts.push(receive(&rx));
        pending.push((work, fixture.headers, fixture.body, fixture.handle));
    }
    host.runtime.probe_request_clock();
    let first = clock(&rx);
    host.runtime.probe_request_clock();
    let second = clock(&rx);
    assert_eq!((first.probe_sequence, second.probe_sequence), (1, 2));
    assert!(starts.iter().all(|r| r.dispatch_ns < first.elapsed_ns));
    assert!(first.elapsed_ns < second.elapsed_ns);
    assert!(pending.iter().all(|(work, _, _, _)| !work.is_finished()));
    assert!(rx.try_recv().is_err());
    let mut events: Vec<_> = starts.iter().map(|r| r.event_sequence).collect();
    for (work, headers, body, handle) in pending {
        headers.send(()).unwrap();
        let received = receive(&rx);
        assert_eq!(received.phase, Phase::HeadersReceived);
        body.send(()).unwrap();
        handle.await.unwrap();
        let (result, exposure) = work.await.unwrap();
        assert_eq!(result.unwrap(), b"original-body");
        assert_eq!(exposure, RequestExposure::HttpResponseReceived);
        let ended = receive(&rx);
        assert_eq!(ended.end, Some(End::BodyComplete));
        assert!(second.elapsed_ns < ended.end_ns.unwrap());
        assert_eq!(ended.request, received.request);
        assert_eq!(
            ended.dispatch_ns,
            starts
                .iter()
                .find(|r| r.request == ended.request)
                .unwrap()
                .dispatch_ns
        );
        events.extend([received.event_sequence, ended.event_sequence]);
    }
    events.sort();
    assert_eq!(events, (1..=9).collect::<Vec<_>>());
    assert_eq!(
        host.runtime.request_process.request.load(Ordering::Relaxed),
        3
    );
    for probe in [first, second] {
        assert_eq!(probe.pid, std::process::id());
        assert!(probe.process_start_ticks.is_some_and(|n| n > 0));
        assert_eq!(
            (probe.enqueue_dropped_total, probe.write_failed_total),
            (0, 0)
        );
        let encoded = serde_json::to_value(probe).unwrap();
        let keys: std::collections::BTreeSet<_> = encoded
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            [
                "event",
                "pid",
                "process_start_ticks",
                "probe_sequence",
                "elapsed_ns",
                "enqueue_dropped_total",
                "write_failed_total"
            ]
            .into_iter()
            .collect()
        );
    }
    assert!(rx.try_recv().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_clock_probe_does_not_prevent_pending_http_cancellation() {
    let (sink, rx) = Sink::capture(4);
    let host = Arc::new(host(sink));
    let fixture = Fixture::start(200, b"{}", 2).await;
    let work = request(fixture.endpoint, host.clone());
    fixture.received.await.unwrap();
    let start = receive(&rx);
    host.runtime.probe_request_clock();
    let sample = clock(&rx);
    assert!(!work.is_finished());
    work.abort();
    assert!(work.await.unwrap_err().is_cancelled());
    let end = receive(&rx);
    assert_eq!((start.event_sequence, end.event_sequence), (1, 2));
    assert_eq!(end.end, Some(End::FutureDropped));
    assert!(start.dispatch_ns < sample.elapsed_ns && sample.elapsed_ns < end.end_ns.unwrap());
    assert!(end.headers_ns.is_none() && end.body_eof_ns.is_none());
    drop(fixture.headers);
    drop(fixture.body);
    fixture.handle.await.unwrap();
}

#[test]
fn request_clock_queue_loss_and_probe_overflow_cannot_erase_loss_or_change_http_sequence() {
    let (sink, rx) = Sink::capture(1);
    let host = host(sink.clone());
    host.runtime.probe_request_clock(); // Fills the existing shared queue.
    let interval = Interval::dispatch(&host); // Nonblocking drop, HTTP event1.
    host.runtime.probe_request_clock(); // Nonblocking drop, probe2.
    assert_eq!(clock(&rx).probe_sequence, 1);
    host.runtime.probe_request_clock();
    let third = clock(&rx);
    assert_eq!((third.probe_sequence, third.enqueue_dropped_total), (3, 2));
    host.runtime
        .request_process
        .probe
        .store(u64::MAX, Ordering::Relaxed);
    host.runtime.probe_request_clock(); // No wrapped/duplicate sample is emitted.
    assert!(rx.try_recv().is_err());
    assert_eq!(
        host.runtime.request_process.probe.load(Ordering::Relaxed),
        u64::MAX
    );
    drop(interval);
    let end = receive(&rx);
    assert_eq!((end.event_sequence, end.enqueue_dropped_total), (2, 3));
    sink.loss.enqueue.store(u64::MAX, Ordering::Relaxed);
    host.runtime.probe_request_clock();
    assert_eq!(sink.loss.enqueue.load(Ordering::Relaxed), u64::MAX);
    assert!(rx.try_recv().is_err());
}

#[test]
fn request_clock_writer_failure_is_reported_by_next_drained_record_and_saturates() {
    struct Broken;
    impl std::io::Write for Broken {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("synthetic writer failure"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let (sink, rx) = Sink::capture(2);
    let host = host(sink.clone());
    host.runtime.probe_request_clock();
    host.runtime.probe_request_clock();
    write_record(&mut Broken, observation(&rx), &sink.loss);
    let mut bytes = Vec::new();
    write_record(&mut bytes, observation(&rx), &sink.loss);
    let record: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(record["probe_sequence"], 2);
    assert_eq!(record["write_failed_total"], 1);
    assert_eq!(record["enqueue_dropped_total"], 0);
    sink.loss.write.store(u64::MAX, Ordering::Relaxed);
    host.runtime.probe_request_clock();
    write_record(&mut Broken, observation(&rx), &sink.loss);
    assert_eq!(sink.loss.write.load(Ordering::Relaxed), u64::MAX);
}

#[test]
fn request_clock_missing_process_identity_remains_explicitly_unqualified() {
    let (sink, rx) = Sink::capture(1);
    let mut process = Process::new(&crate::limits::RuntimeLimits::seed().unwrap().worker);
    process.sink = (*sink).clone();
    process.start_ticks = None;
    process.probe_clock();
    let value = serde_json::to_value(clock(&rx)).unwrap();
    assert!(value["process_start_ticks"].is_null());
    assert_eq!(value["probe_sequence"], 1);
    // Qualification requires a nonzero exact identity; no PID-only fallback.
}
