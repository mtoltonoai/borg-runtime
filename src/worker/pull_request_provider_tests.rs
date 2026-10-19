//! Controlled HTTP transport fixtures, not GitHub or durable phase qualification.
//! The synthetic token is passed explicitly; the fixture does not read or mutate
//! credential environment variables.
use super::{pr, Provider};
use crate::{limits::RuntimeLimits, protocol::{Attempt, AttemptPhase, Checkpoint, Repository},
    repository_catalogue::Revision, worker::process::{self, Control}};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{body::Incoming, service::service_fn, Request, Response};
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use std::{collections::{BTreeMap, VecDeque}, io, sync::{Arc, Mutex}, time::Duration};
use tokio::{sync::{watch, Notify}, task::{JoinHandle, JoinSet}};

const TOKEN: &str = "synthetic-pr-fixture-token";
const REQUEST_LIMIT: usize = 16;
const BODY_LIMIT: usize = 16_384;

#[derive(Clone, Debug)]
struct Seen {
    method: String,
    uri: String,
    body: Vec<u8>,
    authorization: Option<String>,
    idempotency_key: Option<String>,
}
enum Reply {
    Json { status: u16, value: Value, headers: Vec<(String, String)> },
    // The peer consumes the complete POST before losing its response.
    LoseResponse,
    Hold,
}
impl Reply {
    fn json(status: u16, value: Value) -> Self { Self::Json { status, value, headers: vec![] } }
}
struct Peer {
    base: String,
    seen: Arc<Mutex<Vec<Seen>>>,
    replies: Arc<Mutex<VecDeque<Reply>>>,
    changed: Arc<Notify>,
    server: Option<JoinHandle<()>>,
}
impl Peer {
    async fn new() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let replies = Arc::new(Mutex::new(VecDeque::new()));
        let changed = Arc::new(Notify::new());
        let (captured, queued, notification) = (seen.clone(), replies.clone(), changed.clone());
        let server = tokio::spawn(async move {
            // Dropping this JoinSet aborts every accepted connection. The Peer
            // aborts this owner task on both normal close and unwinding Drop.
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        let (seen, replies, changed) = (captured.clone(), queued.clone(), notification.clone());
                        connections.spawn(async move {
                            let service = service_fn(move |request: Request<Incoming>| {
                                let (seen, replies, changed) = (seen.clone(), replies.clone(), changed.clone());
                                async move {
                                    let (parts, body) = request.into_parts();
                                    let body = Limited::new(body, BODY_LIMIT).collect().await
                                        .map_err(|_| io::Error::other("fixture request body limit"))?.to_bytes();
                                    {
                                        let mut seen = seen.lock().unwrap();
                                        if seen.len() >= REQUEST_LIMIT {
                                            return Err(io::Error::other("fixture request count limit"));
                                        }
                                        seen.push(Seen { method: parts.method.to_string(), uri: parts.uri.to_string(),
                                            body: body.to_vec(), authorization: parts.headers.get("authorization")
                                                .map(|v| v.to_str().unwrap().to_owned()),
                                            idempotency_key: parts.headers.get("idempotency-key")
                                                .map(|v| v.to_str().unwrap().to_owned()) });
                                    }
                                    changed.notify_one();
                                    let reply = replies.lock().unwrap().pop_front()
                                        .unwrap_or_else(|| Reply::json(500, json!({"unexpected_request":true})));
                                    match reply {
                                        Reply::LoseResponse => Err(io::Error::other("controlled response loss")),
                                        Reply::Hold => { std::future::pending::<()>().await; unreachable!() },
                                        Reply::Json { status, value, headers } => {
                                            let mut response = Response::builder().status(status)
                                                .header("content-type", "application/json").header("connection", "close");
                                            for (name, value) in headers { response = response.header(name, value); }
                                            Ok::<_, io::Error>(response.body(Full::new(Bytes::from(
                                                serde_json::to_vec(&value).unwrap()))).unwrap())
                                        }
                                    }
                                }
                            });
                            // Response-loss and cancelled exchanges intentionally close unsuccessfully.
                            let _ = hyper::server::conn::http1::Builder::new()
                                .serve_connection(TokioIo::new(stream), service).await;
                        });
                    }
                    done = connections.join_next(), if !connections.is_empty() => { done.unwrap().unwrap(); }
                }
            }
        });
        Self { base, seen, replies, changed, server: Some(server) }
    }
    fn push(&self, reply: Reply) {
        let mut replies = self.replies.lock().unwrap();
        assert!(replies.len() < REQUEST_LIMIT);
        replies.push_back(reply);
    }
    async fn wait_for_request(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let changed = self.changed.notified();
                if !self.seen.lock().unwrap().is_empty() { return; }
                changed.await;
            }
        }).await.expect("controlled request was not observed");
    }
    async fn close(mut self) -> Vec<Seen> {
        let server = self.server.take().unwrap();
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
        let seen = self.seen.lock().unwrap().clone();
        seen
    }
}
impl Drop for Peer {
    fn drop(&mut self) { if let Some(server) = self.server.take() { server.abort(); } }
}

fn policy() -> RuntimeLimits {
    let mut limits = RuntimeLimits::seed().unwrap();
    // Explicit synthetic fixture allowances; no production metadata selection.
    limits.controller.effect_value_bytes = 32_768;
    limits.controller.max_list_tasks = 4;
    limits.transport.body_bytes = 32_768;
    limits.transport.client_connect_timeout_ms = 2_000;
    limits.transport.client_timeout_ms = 5_000;
    limits.worker.credential_scan_units = 65_536;
    limits.worker.credential_scan_depth = 32;
    limits
}
fn control(limits: Arc<RuntimeLimits>) -> (watch::Sender<bool>, Control) {
    let (send, cancel) = watch::channel(false);
    let worker_limits: std::sync::Arc<crate::limits::WorkerLimits> = Arc::from(limits.worker.clone());
    let validation_runtime = Arc::new(crate::native::ValidationRuntime::new(worker_limits.clone()).unwrap());
    (send, Control { deadline_ms: process::now_ms() + 15_000, worker_limits,
        runtime_limits: limits, validation_runtime, cancel })
}
fn intent(base: &str, limits: &RuntimeLimits, body: &str) -> pr::Intent {
    let target = pr::Target { revision: "fixture-target-one".into(), api_base: base.into(),
        owner: "fixture-owner".into(), repository: "fixture-repository".into(), repository_id: 71,
        head_remote: "/controlled-fixture/head.git".into(), base_branch: "main".into(), credential_name: "GITHUB_TOKEN".into() };
    let registration = Revision { repository_id: "fixture-registration".into(), registration_revision: "one".into() };
    let attempt = Attempt { id: "fixture-attempt".into(), session_id: "fixture-session".into(),
        task_id: "fixture-task".into(), generation: 1, worker_id: "fixture-worker".into(),
        worker_incarnation: "one".into(), phase: AttemptPhase::Running, deadline_unix_ms: 10_000,
        stopped: false, uncertain: false };
    let checkpoint = Checkpoint { registration: registration.clone(), repository: "/controlled-fixture/checkpoints.git".into(),
        operation_id: "fixture-checkpoint".into(), commit: "a".repeat(40), remote_ref: format!(
            "refs/heads/borg/tasks/{}/{}/{}", process::component(&attempt.task_id),
            process::component(&attempt.id), process::component("fixture-checkpoint")) };
    let repository = Repository { registration: Some(registration), url: "/controlled-fixture/source.git".into(),
        start_commit: "b".repeat(40), checkpoint_remote: checkpoint.repository.clone(), pull_request_target: Some(target.clone()) };
    let bounds = pr::Bounds::from_limits(limits);
    let head = pr::prepare_head(&target, &repository, &attempt, &checkpoint, "fixture-original-pr", &bounds).unwrap();
    // A typed source fixture for transport validation, not a controller settlement claim.
    let mut settled = pr::head_record(&head, &bounds).unwrap();
    settled.state = "settled".into();
    settled.outcome = Some(json!(pr::HeadSettlement { acknowledged: true, proof: pr::HeadProof {
        operation_id: head.head_operation_id.clone(), target_revision: target.revision.clone(),
        remote: target.head_remote.clone(), remote_ref: head.head_ref.clone(), commit: checkpoint.commit } }));
    pr::prepare_pull_request(&head, &settled, &pr::Request { title: "Controlled fixture change".into(), body: body.into() }, &bounds).unwrap()
}
fn provider(intent: &pr::Intent, limits: Arc<RuntimeLimits>) -> Provider {
    Provider::with_credential(&intent.head.target, limits, TOKEN.into(), vec![]).unwrap()
}
fn repository_value(intent: &pr::Intent) -> Value {
    let t = &intent.head.target;
    json!({"id":t.repository_id,"name":t.repository,"owner":{"login":t.owner}})
}
fn branch_value(reference: &str, sha: &str) -> Value {
    json!({"ref":reference,"object":{"type":"commit","sha":sha}})
}
fn pull_value(intent: &pr::Intent, number: u64) -> Value {
    json!({"id":900+number,"number":number,
        "html_url":format!("{}/fixture-owner/fixture-repository/pull/{number}", intent.head.target.api_base),
        "title":intent.title,"body":intent.body,
        "head":{"ref":intent.head.head_ref.strip_prefix("refs/heads/").unwrap(),
            "sha":intent.head.binding.checkpoint.commit,"repo":repository_value(intent)},
        "base":{"ref":intent.head.target.base_branch,"sha":"c".repeat(40),"repo":repository_value(intent)},
        "future_provider_field":{"allowed":true}})
}
fn assert_request(request: &Seen, method: &str) {
    assert_eq!(request.method, method);
    assert_eq!(request.authorization.as_deref(), Some(format!("Bearer {TOKEN}").as_str()));
    assert!(request.idempotency_key.is_none());
}

#[tokio::test]
async fn lost_create_response_is_resolved_by_original_get_without_post_retry() {
    let peer = Peer::new().await;
    let limits = Arc::new(policy());
    let intent = intent(&peer.base, &limits, "Original accepted request");
    let provider = provider(&intent, limits.clone());
    let (_cancel, control) = control(limits);
    peer.push(Reply::LoseResponse);
    let error = provider.create(&intent, &control).await.unwrap_err();
    assert!(error.contains("unknown"));
    assert!(!error.contains(TOKEN));
    peer.push(Reply::json(200, json!([pull_value(&intent, 17)])));
    let pr::Reconciliation::Observed(proof) = provider.reconcile(&intent, &control).await.unwrap() else {
        panic!("complete original proof was not observed");
    };
    assert_eq!(proof.operation_id, intent.head.binding.operation_id);
    assert_eq!(proof.head.commit, intent.head.binding.checkpoint.commit);
    assert_eq!(proof.base.commit, "c".repeat(40));
    let seen = peer.close().await;
    assert_eq!(seen.len(), 2);
    assert_request(&seen[0], "POST");
    assert_eq!(seen[0].uri, "/repos/fixture-owner/fixture-repository/pulls");
    assert_eq!(serde_json::from_slice::<Value>(&seen[0].body).unwrap(),
        pr::create_body(&intent, &provider.bounds).unwrap());
    assert_request(&seen[1], "GET");
    assert!(seen[1].body.is_empty());
    let query: BTreeMap<String, String> = reqwest::Url::parse(&format!("http://127.0.0.1{}", seen[1].uri))
        .unwrap().query_pairs().map(|(k,v)| (k.into_owned(),v.into_owned())).collect();
    assert_eq!(query, BTreeMap::from([
        ("state".into(), "all".into()), ("base".into(), "main".into()),
        ("head".into(), format!("fixture-owner:{}", intent.head.head_ref.strip_prefix("refs/heads/").unwrap())),
        ("per_page".into(), "4".into()), ("page".into(), "1".into()),
    ]));
}

#[tokio::test]
async fn incomplete_empty_and_ambiguous_lookups_never_create_or_follow_links() {
    for case in 0..3 {
        let peer = Peer::new().await;
        let limits = Arc::new(policy());
        let intent = intent(&peer.base, &limits, "Lookup only");
        let provider = provider(&intent, limits.clone());
        let (_cancel, control) = control(limits);
        peer.push(match case {
            0 => Reply::json(200, json!([])),
            1 => Reply::json(200, json!([pull_value(&intent, 17), pull_value(&intent, 18)])),
            _ => Reply::Json { status: 200, value: json!([pull_value(&intent, 17)]),
                headers: vec![("link".into(), format!("<{}/must-not-follow>; rel=\"next\"", peer.base))] },
        });
        let outcome = provider.reconcile(&intent, &control).await;
        if case == 1 { assert!(outcome.unwrap_err().contains("Collision")); }
        else { assert_eq!(outcome.unwrap(), pr::Reconciliation::Unknown); }
        let seen = peer.close().await;
        assert_eq!(seen.len(), 1);
        assert_request(&seen[0], "GET");
    }
}

#[tokio::test]
async fn one_complete_result_at_the_selected_page_bound_remains_observable() {
    let peer = Peer::new().await;
    let mut limits = policy(); limits.controller.max_list_tasks = 1;
    let limits = Arc::new(limits);
    let intent = intent(&peer.base, &limits, "Single page");
    let provider = provider(&intent, limits.clone());
    let (_cancel, control) = control(limits);
    peer.push(Reply::json(200, json!([pull_value(&intent, 17)])));
    assert!(matches!(provider.reconcile(&intent, &control).await.unwrap(), pr::Reconciliation::Observed(_)));
    let seen = peer.close().await;
    assert_eq!(seen.len(), 1); assert_request(&seen[0], "GET");
    assert!(seen[0].uri.contains("per_page=1"));
}

#[tokio::test]
async fn preflight_checks_exact_repository_and_head_but_allows_base_advancement() {
    for wrong in 0..8 {
        let peer = Peer::new().await;
        let limits = Arc::new(policy());
        let intent = intent(&peer.base, &limits, "Preflight");
        let provider = provider(&intent, limits.clone());
        let (_cancel, control) = control(limits);
        let mut repository = repository_value(&intent);
        let mut head = branch_value(&intent.head.head_ref, &intent.head.binding.checkpoint.commit);
        let mut base = branch_value("refs/heads/main", &"d".repeat(40));
        match wrong {
            1 => repository["id"] = json!(72),
            2 => head["ref"] = json!("refs/heads/foreign"),
            3 => head["object"]["sha"] = json!("f".repeat(40)),
            4 => base["ref"] = json!("refs/heads/foreign"),
            5 => base["object"]["type"] = json!("tag"),
            6 => repository["owner"]["login"] = json!("foreign-owner"),
            7 => repository["name"] = json!("foreign-repository"),
            _ => {},
        }
        for value in [repository, head, base] { peer.push(Reply::json(200, value)); }
        let result = provider.preflight(&intent, &control).await;
        if wrong == 0 { assert!(result.is_ok(), "{result:?}"); } else { assert!(result.is_err()); }
        let seen = peer.close().await;
        assert_eq!(seen.len(), match wrong { 1 | 6 | 7 => 1, 2 | 3 => 2, _ => 3 });
        for request in &seen { assert_request(request, "GET"); assert!(request.body.is_empty()); }
        assert_eq!(seen[0].uri, "/repos/fixture-owner/fixture-repository");
        if seen.len() >= 2 { assert_eq!(seen[1].uri, format!("/repos/fixture-owner/fixture-repository/git/ref/{}",
            intent.head.head_ref.strip_prefix("refs/").unwrap())); }
        if seen.len() == 3 { assert_eq!(seen[2].uri, "/repos/fixture-owner/fixture-repository/git/ref/heads/main"); }
    }
}

#[tokio::test]
async fn create_rejects_wrong_provider_proof_after_one_original_post() {
    for wrong in 0..4 {
        let peer = Peer::new().await;
        let limits = Arc::new(policy());
        let intent = intent(&peer.base, &limits, "Proof binding");
        let provider = provider(&intent, limits.clone());
        let (_cancel, control) = control(limits);
        let mut proof = pull_value(&intent, 17);
        match wrong {
            0 => proof["head"]["repo"]["id"] = json!(72),
            1 => proof["head"]["sha"] = json!("f".repeat(40)),
            2 => proof["base"]["ref"] = json!("foreign"),
            _ => proof["body"] = json!("missing original operation marker"),
        }
        peer.push(Reply::json(201, proof));
        assert!(provider.create(&intent, &control).await.is_err());
        let seen = peer.close().await;
        assert_eq!(seen.len(), 1); assert_request(&seen[0], "POST");
    }
}

#[tokio::test]
async fn response_credentials_and_selected_bounds_fail_without_retaining_values() {
    for case in 0..5 {
        let peer = Peer::new().await;
        let mut limits = policy();
        match case {
            1 => limits.transport.body_bytes = 128,
            2 => limits.transport.http_header_count = 1,
            3 => limits.worker.credential_scan_depth = 1,
            4 => limits.worker.credential_scan_units = 1,
            _ => {},
        }
        let limits = Arc::new(limits);
        let intent = intent(&peer.base, &limits, "Bounded lookup");
        let provider = provider(&intent, limits.clone());
        let (_cancel, control) = control(limits);
        peer.push(Reply::json(200, match case {
            0 => json!({"provider_extra":TOKEN}),
            1 => json!({"padding":"x".repeat(256)}),
            3 => json!({"one":{"two":{"three":true}}}),
            4 => json!([true]),
            _ => json!([]),
        }));
        let error = provider.reconcile(&intent, &control).await.unwrap_err();
        assert!(!error.contains(TOKEN));
        assert!(error.contains(match case {
            0 => "configured credential", 1 => "transport.body_bytes", 2 => "headers exceed",
            _ => "inspection bounds",
        }), "unexpected closed diagnostic: {error}");
        let seen = peer.close().await;
        assert_eq!(seen.len(), 1); assert_request(&seen[0], "GET");
    }
}

#[tokio::test]
async fn outgoing_secret_and_pre_cancelled_attempt_do_not_send() {
    for secret in [true, false] {
        let peer = Peer::new().await;
        let limits = Arc::new(policy());
        let intent = intent(&peer.base, &limits, if secret { TOKEN } else { "Cancelled before send" });
        let provider = provider(&intent, limits.clone());
        let (cancel, control) = control(limits);
        if !secret { cancel.send(true).unwrap(); }
        let error = provider.create(&intent, &control).await.unwrap_err();
        assert!(!error.contains(TOKEN));
        assert!(error.contains(if secret { "configured credential" } else { "cancelled" }));
        assert!(peer.close().await.is_empty());
    }
}

#[tokio::test]
async fn cancellation_after_received_post_keeps_original_outcome_unresolved() {
    let peer = Peer::new().await;
    let limits = Arc::new(policy());
    let intent = intent(&peer.base, &limits, "Cancellation after request");
    let provider = provider(&intent, limits.clone());
    let (cancel, control) = control(limits);
    peer.push(Reply::Hold);
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(provider.create(&intent, &control), async {
            peer.wait_for_request().await;
            cancel.send(true).unwrap();
        })
    }).await.expect("controlled cancellation did not return");
    assert_eq!(result.unwrap_err(), "pull request transport interrupted; original effect requires reconciliation");
    let seen = peer.close().await;
    assert_eq!(seen.len(), 1); assert_request(&seen[0], "POST");
    assert_eq!(serde_json::from_slice::<Value>(&seen[0].body).unwrap(), pr::create_body(&intent, &provider.bounds).unwrap());
}

#[tokio::test]
async fn redirect_response_is_refused_without_forwarding_authorization() {
    let peer = Peer::new().await;
    let limits = Arc::new(policy());
    let intent = intent(&peer.base, &limits, "No redirect");
    let provider = provider(&intent, limits.clone());
    let (_cancel, control) = control(limits);
    peer.push(Reply::Json { status: 302, value: json!({"error":TOKEN}),
        headers: vec![("location".into(), format!("{}/must-not-follow", peer.base))] });
    let error = provider.create(&intent, &control).await.unwrap_err();
    assert!(error.contains("HTTP status 302")); assert!(!error.contains(TOKEN));
    let seen = peer.close().await;
    assert_eq!(seen.len(), 1); assert_request(&seen[0], "POST");
}

#[tokio::test]
async fn ipv6_loopback_literal_is_accepted_without_binding_or_proxy_environment() {
    let limits = Arc::new(policy());
    let intent = intent("http://[::1]:19418", &limits, "Literal endpoint only");
    let provider = provider(&intent, limits);
    assert_eq!(provider.endpoint(&["pulls"]).unwrap().host_str(), Some("[::1]"));
    // This exercises accepted endpoint/client construction only. It does not
    // claim an IPv6 listener or live ambient-proxy qualification.
}
