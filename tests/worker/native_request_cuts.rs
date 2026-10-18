//! Actual worker loss at original marked Usage and terminal diagnostic boundaries.
//! Nested under native_retry to reuse its process/provider fixtures unchanged.
//! A retained local reservation is not a canonical charge or permission to POST.
use super::*;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{
    Request as HttpRequest, Response as HttpResponse, body::Incoming, service::service_fn,
};
use hyper_util::rt::TokioIo;
use std::io;
use tokio::sync::{mpsc, watch};

#[derive(Clone, Copy, Debug)]
enum Cut {
    BeforeReservationForward,
    AfterReservationCommit,
    AfterDiagnosticRetention,
}

struct Boundary {
    request: Request,
    committed_reply: Option<Reply>,
}
struct ProxyState {
    original: Option<Report>,
    forwarded: usize,
}
struct ReservationProxy {
    url: String,
    state: Arc<Mutex<ProxyState>>,
    events: mpsc::Receiver<Boundary>,
    released: watch::Sender<bool>,
    join: Option<tokio::task::JoinHandle<()>>,
}
impl ReservationProxy {
    async fn start(lab: &Lab, cut: Cut) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(ProxyState {
            original: None,
            forwarded: 0,
        }));
        let (send, events) = mpsc::channel(1);
        let (released, _) = watch::channel(false);
        let (shared, gate, upstream, policy, root) = (
            state.clone(),
            released.clone(),
            lab.origin.clone(),
            lab.limits.clone(),
            lab.root.clone(),
        );
        let join = tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .retry(reqwest::retry::never())
                .timeout(WAIT)
                .build()
                .unwrap();
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        let (client, state, gate, send, upstream, policy, root) =
                            (client.clone(), shared.clone(), gate.clone(), send.clone(), upstream.clone(), policy.clone(), root.clone());
                        connections.spawn(async move {
                            let service = service_fn(move |request: HttpRequest<Incoming>| {
                                let (client, state, gate, send, upstream, policy, root) =
                                    (client.clone(), state.clone(), gate.clone(), send.clone(), upstream.clone(), policy.clone(), root.clone());
                                async move {
                                    let route = request.uri().path().to_owned();
                                    assert!(matches!(route.as_str(), "/api/operations" | "/mcp"));
                                    let path = request.uri().path_and_query().unwrap().as_str().to_owned();
                                    let method = request.method().clone();
                                    let mut headers = reqwest::header::HeaderMap::new();
                                    for name in ["content-type", "accept", "mcp-protocol-version", "x-borg-caller"] {
                                        if let Some(value) = request.headers().get(name) { headers.insert(name, value.clone()); }
                                    }
                                    let bytes = request.into_body().collect().await.map_err(io::Error::other)?.to_bytes();
                                    assert!(bytes.len() <= policy.transport.body_bytes);
                                    // The actual native worker initializes its MCP catalogue before
                                    // reserving a request. Pass that transport through unchanged;
                                    // only typed private API operations enter the cut logic below.
                                    if route == "/mcp" {
                                        let response = client.request(method, format!("{upstream}{path}"))
                                            .headers(headers).body(bytes.to_vec()).send().await.map_err(io::Error::other)?;
                                        let status = response.status();
                                        let content_type = response.headers().get("content-type").cloned();
                                        let body = response.bytes().await.map_err(io::Error::other)?;
                                        assert!(body.len() <= policy.transport.body_bytes);
                                        let mut reply = HttpResponse::builder().status(status);
                                        if let Some(value) = content_type { reply = reply.header("content-type", value); }
                                        return Ok::<_, io::Error>(reply.body(Full::new(body)).unwrap());
                                    }
                                    let original: Request = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                                    let (first, held, selected) = {
                                        let mut state = state.lock().unwrap();
                                        let marked = match &original.command {
                                            Operation::ObserveWorker { report } if match cut {
                                                Cut::BeforeReservationForward | Cut::AfterReservationCommit => matches!(report.observation,
                                                    Observation::Usage { native_diagnostic_reserved: true, model_requests: 1, .. }),
                                                Cut::AfterDiagnosticRetention => matches!(report.observation,
                                                    Observation::NativeRequestDiagnostic { request_index: 1, .. }),
                                            } => Some(report),
                                            _ => None,
                                        };
                                        let first = !*gate.borrow() && state.original.is_none() && marked.is_some();
                                        if first { state.original = marked.cloned(); }
                                        let selected = match (&state.original, &original.command) {
                                            (Some(expected), Operation::ObserveWorker { report }) if report.report_id == expected.report_id => {
                                                assert_eq!(report, expected, "original selected report body changed"); true
                                            },
                                            _ => false,
                                        };
                                        let lookup = matches!((&state.original, &original.command),
                                            (Some(expected), Operation::GetOperation { operation_id }) if operation_id == &expected.report_id);
                                        (first, !*gate.borrow() && (selected || lookup), selected)
                                    };
                                    if held && (!first || matches!(cut, Cut::BeforeReservationForward | Cut::AfterDiagnosticRetention)) {
                                        if first {
                                            process::atomic_json(&root.join("cut-original-request.json"), &original).unwrap();
                                            send.send(Boundary { request: original, committed_reply: None }).await.map_err(io::Error::other)?;
                                        }
                                        let mut wait = gate.subscribe();
                                        while !*wait.borrow_and_update() { wait.changed().await.map_err(io::Error::other)?; }
                                        // These connections belonged to the killed worker. Releasing
                                        // the barrier never forwards an old withheld request or ACK.
                                        return Err(io::Error::other("owned worker was killed at selected original-report boundary"));
                                    }
                                    let response = client.request(method, format!("{upstream}{path}"))
                                        .headers(headers).body(bytes.to_vec())
                                        .send().await.map_err(io::Error::other)?;
                                    assert!(response.status().is_success());
                                    let status = response.status();
                                    let content_type = response.headers().get("content-type").cloned();
                                    let response = response.bytes().await.map_err(io::Error::other)?;
                                    assert!(response.len() <= policy.transport.body_bytes);
                                    let reply: Reply = serde_json::from_slice(&response).map_err(io::Error::other)?;
                                    assert_eq!(reply.request_id, original.request_id);
                                    reply.validate_for(&original.command).unwrap();
                                    if selected {
                                        let number = { let mut state = state.lock().unwrap(); state.forwarded += 1; state.forwarded };
                                        process::atomic_json(&root.join(format!("original-report-forward-{number}.json")),
                                            &json!({"request": original, "reply": reply})).unwrap();
                                    }
                                    if first {
                                        assert!(matches!(&reply.outcome, Outcome::Ok { data: Data::Task(_) }));
                                        assert!(reply.commit.is_some());
                                        process::atomic_json(&root.join("cut-original-request.json"), &original).unwrap();
                                        process::atomic_json(&root.join("cut-committed-reply.json"), &reply).unwrap();
                                        send.send(Boundary { request: original, committed_reply: Some(reply) }).await.map_err(io::Error::other)?;
                                        let mut wait = gate.subscribe();
                                        while !*wait.borrow_and_update() { wait.changed().await.map_err(io::Error::other)?; }
                                        return Err(io::Error::other("original committed ACK withheld until worker was reaped"));
                                    }
                                    let mut result = HttpResponse::builder().status(status);
                                    if let Some(value) = content_type { result = result.header("content-type", value); }
                                    Ok::<_, io::Error>(result.body(Full::new(Bytes::from(response))).unwrap())
                                }
                            });
                            // Broken connections are the selected transport fault; any fixture
                            // panic is still propagated by the JoinSet branch below.
                            let _ = hyper::server::conn::http1::Builder::new()
                                .serve_connection(TokioIo::new(stream), service).await;
                        });
                    },
                    finished = connections.join_next(), if !connections.is_empty() => { finished.unwrap().unwrap(); },
                }
            }
        });
        Self {
            url,
            state,
            events,
            released,
            join: Some(join),
        }
    }
    async fn event(&mut self) -> Boundary {
        tokio::time::timeout(WAIT, self.events.recv())
            .await
            .unwrap()
            .unwrap()
    }
    fn release_after_reap(&self) {
        self.released.send_replace(true);
    }
    async fn close(&mut self) {
        let join = self.join.take().unwrap();
        join.abort();
        if let Err(error) = join.await {
            assert!(error.is_cancelled(), "controller proxy panicked: {error}");
        }
    }
}
impl Drop for ReservationProxy {
    fn drop(&mut self) {
        if let Some(join) = &self.join {
            join.abort();
        }
    }
}

fn worker_via(lab: &Lab, proxy: &ReservationProxy, endpoint: &str, incarnation: &str) -> Process {
    Process::start(
        &lab.image,
        &[
            "worker".into(),
            "--limits".into(),
            lab.root.join("limits.json").display().to_string(),
            "--controller".into(),
            proxy.url.clone(),
            "--id".into(),
            WORKER_ID.into(),
            "--incarnation".into(),
            incarnation.into(),
            "--workspace-root".into(),
            lab.root.join("worker").display().to_string(),
            "--codex".into(),
            "/usr/bin/false".into(),
            "--coding-capacity".into(),
            "0".into(),
            "--native-capacity".into(),
            "1".into(),
            "--native-endpoint".into(),
            endpoint.into(),
        ],
        &lab.root,
        incarnation,
    )
}

async fn reported_usage(lab: &Lab, attempt: &Attempt) -> (u32, Option<u64>, Option<u64>) {
    let reply = lab
        .api
        .call(Operation::QueryCollaboration {
            query: borg_runtime::collaboration::Query::TaskAttempts {
                task_id: attempt.task_id.clone(),
                after: None,
                limit: 1,
            },
        })
        .await
        .unwrap();
    let Outcome::Ok {
        data: Data::Collaboration(borg_runtime::collaboration::QueryResult::TaskAttempts(page)),
    } = reply.outcome
    else {
        panic!("canonical usage unavailable: {reply:?}")
    };
    assert!(page.next_cursor.is_none());
    assert_eq!(page.items.len(), 1);
    let value = &page.items[0];
    assert_eq!(value.attempt.id, attempt.id);
    assert_eq!(value.attempt.task_id, attempt.task_id);
    assert_eq!(value.attempt.session_id, attempt.session_id);
    assert_eq!(value.attempt.generation, attempt.generation);
    assert_eq!(value.attempt.worker_id, attempt.worker_id);
    assert_eq!(value.attempt.worker_incarnation, attempt.worker_incarnation);
    (
        value.reported_model_requests,
        value.input_tokens,
        value.output_tokens,
    )
}
async fn usage(lab: &Lab, attempt: &Attempt) -> u32 {
    let (requests, input, output) = reported_usage(lab, attempt).await;
    assert_eq!((input, output), (None, None));
    requests
}

/// The main worker may flush a retained diagnostic before the executor has
/// reached its independent token Usage write. Select that actual durable fact
/// separately before requiring token preservation across a process cut.
async fn retained_token_usage(lab: &Lab, attempt: &Attempt, worker: &mut Process) -> Report {
    let id = contract::usage_id(&attempt.id, 1);
    let path = lab
        .state()
        .join("outbox")
        .join(format!("{}.json", process::component(&id)));
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        worker.live();
        let local = match std::fs::read(&path) {
            Ok(bytes) => Some(serde_json::from_slice::<Report>(&bytes).unwrap()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => panic!("original token usage unavailable: {error}"),
        };
        let original = match local {
            Some(report) => Some(report),
            None => lookup(&lab.api, &id).await.map(|(_, saved)| report(&saved)),
        };
        if let Some(original) = original {
            assert_eq!(original.report_id, id);
            assert_eq!(
                (
                    original.task_id.as_str(),
                    original.attempt_id.as_str(),
                    original.generation,
                    original.worker_id.as_str(),
                    original.incarnation.as_str()
                ),
                (
                    attempt.task_id.as_str(),
                    attempt.id.as_str(),
                    attempt.generation,
                    attempt.worker_id.as_str(),
                    attempt.worker_incarnation.as_str()
                )
            );
            assert!(matches!(
                original.observation,
                Observation::Usage {
                    native_diagnostic_reserved: false,
                    model_requests: 1,
                    input_tokens: Some(5),
                    output_tokens: Some(1)
                }
            ));
            return original;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "original token Usage was not retained before selected cut"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn command(lab: &Lab, attempt: &Attempt) -> (PathBuf, Value) {
    let found: Vec<_> = std::fs::read_dir(lab.state().join("commands")).unwrap()
        .map(|entry| entry.unwrap().path()).filter(|path| path.extension().and_then(|s| s.to_str()) == Some("json"))
        .filter_map(|path| {
            let saved: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            let command: WorkerCommand = serde_json::from_value(saved["command"].clone()).unwrap();
            matches!(command, WorkerCommand::RunAttempt { attempt: ref selected, .. } if selected.id == attempt.id)
                .then_some((path, saved))
        }).collect();
    assert_eq!(found.len(), 1);
    found.into_iter().next().unwrap()
}

async fn reservation_cut(cut: Cut) {
    let lab = Lab::new(match cut {
        Cut::BeforeReservationForward => "native-cut-before-charge",
        Cut::AfterReservationCommit => "native-cut-after-charge",
        Cut::AfterDiagnosticRetention => unreachable!("separate terminal diagnostic fixture"),
    });
    let mut controller = lab.controller("controller-one").await;
    let submitted = lab.submit(4).await;
    let mut peer = Peer::start(
        &lab,
        &submitted.id,
        Replies::RejectThenSuccess {
            rejected: 0,
            delay_ms: 0,
        },
    )
    .await;
    let mut proxy = ReservationProxy::start(&lab, cut).await;
    let mut first = worker_via(&lab, &proxy, &peer.endpoint, "worker-one");
    let boundary = proxy.event().await;
    first.live();
    let Operation::ObserveWorker { report: original } = &boundary.request.command else {
        panic!("cut is not original report")
    };
    assert!(matches!(
        original.observation,
        Observation::Usage {
            native_diagnostic_reserved: true,
            model_requests: 1,
            input_tokens: None,
            output_tokens: None
        }
    ));
    let assigned = get(&lab.api, &submitted.id).await;
    let attempt = assigned.attempt.as_ref().unwrap().clone();
    assert_eq!(original.report_id, contract::reservation_id(&attempt.id, 1));
    assert_eq!(
        (
            original.task_id.as_str(),
            original.attempt_id.as_str(),
            original.generation,
            original.worker_id.as_str(),
            original.incarnation.as_str()
        ),
        (
            assigned.id.as_str(),
            attempt.id.as_str(),
            attempt.generation,
            WORKER_ID,
            "worker-one"
        )
    );
    assert_eq!(assigned.input, submitted.input);
    let outbox = lab
        .state()
        .join("outbox")
        .join(format!("{}.json", process::component(&original.report_id)));
    let local_bytes = std::fs::read(&outbox).unwrap();
    assert_eq!(
        serde_json::from_slice::<Report>(&local_bytes).unwrap(),
        *original
    );
    let (command_path, original_command) = command(&lab, &attempt);
    assert_eq!(original_command["effects_started"], true);
    assert_eq!(original_command["completed"], false);
    let prior = lookup(&lab.api, &original.report_id).await;
    match cut {
        Cut::BeforeReservationForward => {
            assert!(prior.is_none());
            assert!(boundary.committed_reply.is_none());
            assert_eq!(usage(&lab, &attempt).await, 0);
            assert_eq!(proxy.state.lock().unwrap().forwarded, 0);
        }
        Cut::AfterReservationCommit => {
            let prior = prior.as_ref().unwrap();
            assert_eq!(report(&prior.1), *original);
            assert_eq!(
                prior.0.commit,
                boundary.committed_reply.as_ref().unwrap().commit
            );
            assert_eq!(usage(&lab, &attempt).await, 1);
            assert_eq!(proxy.state.lock().unwrap().forwarded, 1);
        }
        Cut::AfterDiagnosticRetention => unreachable!("separate terminal diagnostic fixture"),
    }
    let diagnostic = contract::diagnostic_id(&attempt.id, 1);
    assert!(lookup(&lab.api, &diagnostic).await.is_none());
    assert!(
        !lab.state()
            .join("outbox")
            .join(format!("{}.json", process::component(&diagnostic)))
            .exists()
    );
    peer.quiet(0).await;
    process::atomic_json(&lab.root.join("cut-state.json"), &json!({
        "worker_pid": first.child.id(), "original_task": assigned, "local_reservation": original,
        "original_command": original_command, "canonical_reservation": prior.as_ref().map(|p| &p.1),
        "physical_provider_posts": peer.count(),
    })).unwrap();
    first.hard_loss();
    assert_eq!(
        std::fs::read(&outbox).unwrap(),
        local_bytes,
        "crash must preserve original pending reservation"
    );
    proxy.release_after_reap();
    controller.stop().await;
    let mut controller = lab.controller("controller-two").await;
    assert_eq!(
        lookup(&lab.api, &original.report_id).await.map(|p| p.1),
        prior.as_ref().map(|p| p.1.clone())
    );
    assert_eq!(get(&lab.api, &submitted.id).await.input, submitted.input);
    let mut replacement = worker_via(&lab, &proxy, &peer.endpoint, "worker-two");
    assert_ne!(first.child.id(), replacement.child.id());
    lab.registered("worker-two", &mut replacement).await;
    let recovered = until_task(&lab.api, &submitted.id, |task| {
        task.attempt
            .as_ref()
            .is_some_and(|a| a.stopped && a.uncertain)
    })
    .await;
    assert_eq!(recovered.input, submitted.input);
    assert_eq!(recovered.session_id, submitted.session_id);
    assert_eq!(recovered.attempt.as_ref().unwrap().id, attempt.id);
    assert_eq!(
        recovered.attempt.as_ref().unwrap().deadline_unix_ms,
        attempt.deadline_unix_ms
    );
    assert!(recovered.result.is_none());
    assert_eq!(usage(&lab, &attempt).await, 1);
    // Recovery may deliver the already durable original Usage. It cannot infer
    // a NotStarted/response diagnostic or use this old charge for a new POST.
    let charged = charged_once(&lab.api, &attempt, 1).await;
    assert_eq!(report(&charged.1), *original);
    assert_eq!(
        charged.1.intended,
        serde_json::to_value(&boundary.request.command).unwrap()
    );
    if let Some(prior) = prior {
        assert_eq!(charged.0.commit, prior.0.commit);
        assert_eq!(charged.1, prior.1);
    }
    assert!(lookup(&lab.api, &diagnostic).await.is_none());
    assert!(
        lookup(&lab.api, &contract::reservation_id(&attempt.id, 2))
            .await
            .is_none()
    );
    assert!(
        lookup(&lab.api, &contract::completion_id(&attempt.id))
            .await
            .is_none()
    );
    lab.completed_command(&attempt.id);
    let completed: Value = serde_json::from_slice(&std::fs::read(command_path).unwrap()).unwrap();
    assert_eq!(completed["command"], original_command["command"]);
    let refused = lab
        .api
        .call(Operation::RetryTask {
            operation_id: "retry-cut-original".into(),
            task_id: recovered.id.clone(),
            expected_revision: recovered.revision,
        })
        .await
        .unwrap();
    assert!(
        matches!(refused.outcome, Outcome::Refused { ref code, .. } if code == "reconciliation_required")
    );
    peer.quiet(0).await;
    replacement.stop().await;
    let stopped = get(&lab.api, &submitted.id).await;
    controller.stop().await;
    let mut controller = lab.controller("controller-three").await;
    assert_eq!(get(&lab.api, &submitted.id).await, stopped);
    let replay = lab
        .api
        .call(boundary.request.command.clone())
        .await
        .unwrap();
    assert_eq!(replay.commit, charged.0.commit);
    assert_eq!(
        replay.outcome,
        serde_json::from_value::<Outcome>(charged.1.outcome.clone().unwrap()).unwrap()
    );
    assert_eq!(receipt(&lab.api, &original.report_id).await.1, charged.1);
    assert_eq!(usage(&lab, &attempt).await, 1);
    peer.quiet(0).await;
    controller.stop().await;
    proxy.close().await;
    peer.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# Every platform-owned model request dispatched by a retry MUST have an acknowledged durable request reservation before dispatch.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A local transport close or process cleanup MUST NOT establish that an ambiguous provider request had no effect.
async fn worker_loss_before_reservation_forward_recovers_only_original_charge_without_post() {
    reservation_cut(Cut::BeforeReservationForward).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# Every platform-owned model request dispatched by a retry MUST have an acknowledged durable request reservation before dispatch.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A local transport close or process cleanup MUST NOT establish that an ambiguous provider request had no effect.
async fn worker_loss_after_committed_reservation_with_lost_ack_never_dispatches_old_request() {
    reservation_cut(Cut::AfterReservationCommit).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A failed platform-owned provider request MUST retain bounded diagnostics sufficient to identify the observed failure class and available provider retry or quota information.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A local transport close or process cleanup MUST NOT establish that an ambiguous provider request had no effect.
async fn worker_loss_after_retained_provider_diagnostic_reconciles_original_without_second_post() {
    for completed in [true, false] {
        let lab = Lab::new(if completed {
            "native-cut-known-response"
        } else {
            "native-cut-unknown-response"
        });
        let mut controller = lab.controller("controller-one").await;
        let submitted = lab.submit(4).await;
        let replies = if completed {
            Replies::RejectThenSuccess {
                rejected: 0,
                delay_ms: 0,
            }
        } else {
            Replies::LostResponse
        };
        let mut peer = Peer::start(&lab, &submitted.id, replies).await;
        let mut proxy = ReservationProxy::start(&lab, Cut::AfterDiagnosticRetention).await;
        let mut first = worker_via(&lab, &proxy, &peer.endpoint, "worker-one");
        let boundary = proxy.event().await;
        first.live();
        assert!(boundary.committed_reply.is_none());
        assert_eq!(proxy.state.lock().unwrap().forwarded, 0);
        let Operation::ObserveWorker { report: original } = &boundary.request.command else {
            panic!("terminal cut must be original report")
        };
        let assigned = get(&lab.api, &submitted.id).await;
        let attempt = assigned.attempt.as_ref().unwrap().clone();
        assert_eq!(assigned.input, submitted.input);
        assert!(assigned.result.is_none());
        assert_eq!(
            (
                original.task_id.as_str(),
                original.attempt_id.as_str(),
                original.generation,
                original.worker_id.as_str(),
                original.incarnation.as_str()
            ),
            (
                assigned.id.as_str(),
                attempt.id.as_str(),
                attempt.generation,
                WORKER_ID,
                "worker-one"
            )
        );
        assert_eq!(original.report_id, contract::diagnostic_id(&attempt.id, 1));
        let Observation::NativeRequestDiagnostic {
            request_index,
            endpoint,
            model,
            request_binding,
            observed_outcome,
            failure,
        } = &original.observation
        else {
            panic!("wrong terminal observation")
        };
        assert_eq!(*request_index, 1);
        assert_eq!(endpoint, &peer.endpoint);
        assert_eq!(model, "gpt-6-astra");
        assert_eq!(
            *observed_outcome,
            if completed {
                contract::NativeRequestObservedOutcome::Completed
            } else {
                contract::NativeRequestObservedOutcome::Unknown
            }
        );
        assert_eq!(failure.is_none(), completed);
        let actual_binding = {
            let seen = peer.seen.lock().unwrap();
            assert_eq!(seen.len(), 1);
            assert_eq!(seen[0].task_id, submitted.id);
            let body = &seen[0].wire_body;
            ProviderRequestBinding {
                bytes: u64::try_from(body.len()).unwrap(),
                sha256: aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, body)
                    .as_ref()
                    .iter()
                    .map(|v| format!("{v:02x}"))
                    .collect(),
            }
        };
        assert_eq!(request_binding.as_ref(), Some(&actual_binding));
        let charge = charged_once(&lab.api, &attempt, 1).await;
        assert_eq!(reported_usage(&lab, &attempt).await.0, 1);
        assert!(lookup(&lab.api, &original.report_id).await.is_none());
        let outbox = lab
            .state()
            .join("outbox")
            .join(format!("{}.json", process::component(&original.report_id)));
        let original_bytes = std::fs::read(&outbox).unwrap();
        assert_eq!(
            serde_json::from_slice::<Report>(&original_bytes).unwrap(),
            *original
        );
        let (command_path, original_command) = command(&lab, &attempt);
        assert_eq!(original_command["effects_started"], true);
        assert_eq!(original_command["completed"], false);
        if completed {
            let tokens = retained_token_usage(&lab, &attempt, &mut first).await;
            process::atomic_json(&lab.root.join("cut-original-token-usage.json"), &tokens).unwrap();
        }
        // This is a post-response AND post-local-retention cut. The report is
        // the worker's actual typed observation, not a status inferred from the
        // peer's write. Its canonical forwarding/ACK has not happened yet.
        process::atomic_json(&lab.root.join("cut-state.json"), &json!({
            "worker_pid":first.child.id(), "original_task":assigned, "original_command":original_command,
            "local_diagnostic":original, "canonical_reservation":charge.1,
            "physical_request_binding":actual_binding, "physical_provider_posts":peer.count(),
        })).unwrap();
        peer.quiet(1).await;
        first.hard_loss();
        assert_eq!(std::fs::read(&outbox).unwrap(), original_bytes);
        proxy.release_after_reap();
        controller.stop().await;
        let mut controller = lab.controller("controller-two").await;
        assert!(lookup(&lab.api, &original.report_id).await.is_none());
        assert_eq!(
            receipt(&lab.api, &contract::reservation_id(&attempt.id, 1))
                .await
                .1,
            charge.1
        );
        let mut replacement = worker_via(&lab, &proxy, &peer.endpoint, "worker-two");
        assert_ne!(first.child.id(), replacement.child.id());
        lab.registered("worker-two", &mut replacement).await;
        let recovered = until_task(&lab.api, &submitted.id, |task| {
            task.attempt
                .as_ref()
                .is_some_and(|a| a.stopped && a.uncertain)
        })
        .await;
        assert_eq!(recovered.input, submitted.input);
        assert_eq!(recovered.session_id, submitted.session_id);
        assert_eq!(recovered.attempt.as_ref().unwrap().id, attempt.id);
        assert_eq!(
            recovered.attempt.as_ref().unwrap().deadline_unix_ms,
            attempt.deadline_unix_ms
        );
        assert!(
            recovered.result.is_none(),
            "known provider completion does not fabricate accepted task output"
        );
        let diagnostic = receipt(&lab.api, &original.report_id).await;
        assert_eq!(
            report(&diagnostic.1),
            *original,
            "worker loss must not relabel Completed as Unknown or Unknown as Completed"
        );
        assert_eq!(
            diagnostic.1.intended,
            serde_json::to_value(&boundary.request.command).unwrap()
        );
        assert!(matches!(
            serde_json::from_value::<Outcome>(diagnostic.1.outcome.clone().unwrap()).unwrap(),
            Outcome::Ok {
                data: Data::Acknowledged
            }
        ));
        assert!(committed(&diagnostic.0) > committed(&charge.0));
        assert!(
            !outbox.exists(),
            "exact canonical ACK must reconcile the original outbox"
        );
        assert_eq!(
            reported_usage(&lab, &attempt).await,
            if completed {
                (1, Some(5), Some(1))
            } else {
                (1, None, None)
            }
        );
        assert_eq!(
            receipt(&lab.api, &contract::reservation_id(&attempt.id, 1))
                .await
                .1,
            charge.1
        );
        assert!(
            lookup(&lab.api, &contract::reservation_id(&attempt.id, 2))
                .await
                .is_none()
        );
        assert!(
            lookup(&lab.api, &contract::completion_id(&attempt.id))
                .await
                .is_none()
        );
        assert!(
            lookup(&lab.api, &rejection_id(&attempt.id, 1))
                .await
                .is_none()
        );
        lab.completed_command(&attempt.id);
        let recovered_command: Value =
            serde_json::from_slice(&std::fs::read(command_path).unwrap()).unwrap();
        assert_eq!(recovered_command["command"], original_command["command"]);
        let refused = lab
            .api
            .call(Operation::RetryTask {
                operation_id: "retry-retained-diagnostic-original".into(),
                task_id: recovered.id.clone(),
                expected_revision: recovered.revision,
            })
            .await
            .unwrap();
        assert!(
            matches!(refused.outcome, Outcome::Refused { ref code, .. } if code == "reconciliation_required")
        );
        peer.quiet(1).await;
        replacement.stop().await;
        let stopped = get(&lab.api, &submitted.id).await;
        controller.stop().await;
        let mut controller = lab.controller("controller-three").await;
        assert_eq!(get(&lab.api, &submitted.id).await, stopped);
        let replay = lab
            .api
            .call(boundary.request.command.clone())
            .await
            .unwrap();
        assert_eq!(replay.commit, diagnostic.0.commit);
        assert_eq!(
            replay.outcome,
            serde_json::from_value::<Outcome>(diagnostic.1.outcome.clone().unwrap()).unwrap()
        );
        assert_eq!(receipt(&lab.api, &original.report_id).await.1, diagnostic.1);
        assert_eq!(
            receipt(&lab.api, &contract::reservation_id(&attempt.id, 1))
                .await
                .1,
            charge.1
        );
        peer.quiet(1).await;
        controller.stop().await;
        proxy.close().await;
        peer.close().await;
    }
}
