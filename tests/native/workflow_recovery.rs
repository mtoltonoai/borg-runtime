//! Actual private Controller/native worker with one committed MCP mutation.
//! Only its original lookup is withheld through the accepted task deadline.
//! Synthetic loopback provider; no provider, credential or host qualification.
use super::*;
use borg_runtime::mcp::{Caller, Invocation};

// A case-local transport barrier in front of the existing lost-reply Proxy.
// The original tool request still traverses the real MCP/Controller handler.
struct PendingLookup {
    url: String,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    holding: Arc<AtomicBool>,
    lookups: Arc<Mutex<Vec<Value>>>,
    sent: Arc<Mutex<Vec<Invocation>>>,
    reports: Arc<Mutex<Vec<(Report, Reply)>>>,
    _owner: Background,
}
impl PendingLookup {
    async fn start(proxy: &Proxy, state: PathBuf, limits: Arc<RuntimeLimits>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let holding = Arc::new(AtomicBool::new(true));
        let lookups = Arc::new(Mutex::new(Vec::new()));
        let sent = Arc::new(Mutex::new(Vec::new()));
        let reports = Arc::new(Mutex::new(Vec::new()));
        let (upstream, dropped) = (proxy.url.clone(), proxy.dropped.clone());
        let (seen, released, queries, calls, observed) = (
            entered.clone(),
            release.clone(),
            lookups.clone(),
            sent.clone(),
            reports.clone(),
        );
        let hold = holding.clone();
        let owner = tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .no_proxy()
                .retry(reqwest::retry::never())
                .build()
                .unwrap();
            let mut joins = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        let (upstream, dropped, seen, released, queries, calls, observed, client, state, limits, hold) =
                            (upstream.clone(), dropped.clone(), seen.clone(), released.clone(), queries.clone(), calls.clone(), observed.clone(), client.clone(), state.clone(), limits.clone(), hold.clone());
                        joins.spawn(async move {
                            let service = service_fn(move |request: HttpRequest<Incoming>| {
                                let (upstream, dropped, seen, released, queries, calls, observed, client, state, limits, hold) =
                                    (upstream.clone(), dropped.clone(), seen.clone(), released.clone(), queries.clone(), calls.clone(), observed.clone(), client.clone(), state.clone(), limits.clone(), hold.clone());
                                async move {
                                    let path = request.uri().path().to_owned();
                                    let caller = request.headers().get("x-borg-caller").cloned();
                                    let version = request.headers().get("mcp-protocol-version").cloned();
                                    let bytes = request.into_body().collect().await.unwrap().to_bytes();
                                    assert!(bytes.len() <= limits.transport.body_bytes);
                                    let value: Value = serde_json::from_slice(&bytes).unwrap();
                                    if path == "/mcp" && value["method"] == "tools/call" {
                                        let actor: Caller = serde_json::from_slice(caller.as_ref().unwrap().as_bytes()).unwrap();
                                        let call = Invocation {
                                            caller: actor,
                                            operation_id: value["params"]["_meta"]["borg"]["operation_id"].as_str().unwrap().into(),
                                            logical_call_id: value["params"]["_meta"]["borg"]["logical_call_id"].as_str().unwrap().into(),
                                            tool: value["params"]["name"].as_str().unwrap().into(),
                                            arguments: value["params"]["arguments"].clone(),
                                        };
                                        let journal = state.join("collaboration").join(process::component(&call.caller.attempt_id)).join("0000000001.json");
                                        let saved: Value = serde_json::from_slice(&std::fs::read(journal).unwrap()).unwrap();
                                        assert_eq!(saved["request"], serde_json::to_value(call.operation()).unwrap(), "exact intent must exist before real mutation dispatch");
                                        assert!(saved["outcome"].is_null());
                                        assert_eq!(saved["consumed"], false);
                                        calls.lock().unwrap().push(call);
                                    }
                                    if path == "/mcp" && value["method"] == "borg/calls/get" && dropped.load(Ordering::SeqCst) && hold.load(Ordering::SeqCst) {
                                        queries.lock().unwrap().push(value.clone());
                                        seen.notify_one();
                                        // This is longer than the original client timeout. The test
                                        // requires the earlier accepted task deadline to stop work.
                                        tokio::time::timeout(Duration::from_millis(limits.transport.client_timeout_ms + 10_000), released.notified()).await.expect("lookup barrier not released");
                                    }
                                    let mut request = client.post(format!("{upstream}{path}")).header("content-type", "application/json").body(bytes.to_vec());
                                    if let Some(caller) = caller { request = request.header("x-borg-caller", caller); }
                                    if let Some(version) = version { request = request.header("mcp-protocol-version", version); }
                                    let response = request.send().await.map_err(io::Error::other)?;
                                    let status = response.status();
                                    let bytes = response.bytes().await.map_err(io::Error::other)?;
                                    if path == "/api/operations" && value["operation"] == "observe_worker" {
                                        let request: Request = serde_json::from_value(value).unwrap();
                                        let reply: Reply = serde_json::from_slice(&bytes).unwrap();
                                        reply.validate_for(&request.command).unwrap();
                                        let Operation::ObserveWorker { report } = request.command else { unreachable!() };
                                        observed.lock().unwrap().push((report, reply));
                                    }
                                    Ok::<_, io::Error>(Response::builder().status(status.as_u16()).body(Full::new(bytes)).unwrap())
                                }
                            });
                            let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
                        });
                    },
                    _ = joins.join_next(), if !joins.is_empty() => {},
                }
            }
        });
        Self {
            url,
            entered,
            release,
            holding,
            lookups,
            sent,
            reports,
            _owner: Background(owner),
        }
    }
}

async fn exact_call_record(
    api: &ApiClient,
    call: &Invocation,
) -> (OperationRecord, Option<MutationCommit>) {
    let op = Operation::GetCollaborationCall {
        caller: call.caller.clone(),
        operation_id: call.operation_id.clone(),
    };
    let reply = api.call(op.clone()).await.unwrap();
    reply.validate_for(&op).unwrap();
    let Outcome::Ok {
        data: Data::Operation(record),
    } = reply.outcome
    else {
        panic!("original call lookup: {reply:?}")
    };
    assert_eq!(
        record.intended,
        serde_json::to_value(call.operation()).unwrap()
    );
    assert!(record.outcome.is_some());
    (record, reply.commit)
}

async fn refused(api: &ApiClient, op: Operation, code: &str) {
    let reply = api.call(op.clone()).await.unwrap();
    reply.validate_for(&op).unwrap();
    assert!(
        matches!(reply.outcome, Outcome::Refused { code: ref actual, .. } if actual == code),
        "{reply:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// Traceability scope: actual Controller/native pending mutation, original task
// deadline and changed-incarnation restart. No automatic uncertain-call resend.
//= spec/impl/v2/implementation.md#native-tool-profiles-and-continuations
//= type=test
//# An execution attempt MUST retain a tool call's exact argument binding before sending a mutation.
async fn pending_mutation_deadline_preserves_original_identity_across_incarnation_restart() {
    let lab = Lab::new("workflow-pending-deadline-restart");
    let mut controller = lab.controller("controller-one").await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let peer = Peer::start(|index, _| {
        assert_eq!(
            index, 0,
            "pending mutation cannot authorize another model POST"
        );
        Some(function(
            index,
            "collab_memory_set",
            json!({"key":"note","expected_revision":null,"value":7}),
        ))
    })
    .await;
    let proxy = Proxy::start(
        lab.url.clone(),
        Some("collab_memory_set"),
        false,
        lab.limits.clone(),
    )
    .await;
    let gate = PendingLookup::start(&proxy, lab.state(), lab.limits.clone()).await;
    let mut input = lab.input(3);
    assert!(15_000 < lab.limits.transport.client_timeout_ms);
    input.limits.deadline_unix_ms = process::now_ms() + 15_000;
    let task = submit(&api, input).await;
    let accepted = task.input.clone();
    let anchor = tokio::time::Instant::now();
    let mut worker = lab.worker(&gate.url, &peer.endpoint, "pending-first");
    tokio::time::timeout(Duration::from_secs(10), gate.entered.notified())
        .await
        .expect("original post-commit lookup not reached");
    assert!(proxy.dropped.load(Ordering::SeqCst));
    let call = {
        let sent = gate.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        sent[0].clone()
    };
    let active = get(&api, &task.id).await;
    assert_eq!(active.status, TaskStatus::Active);
    let attempt = active.attempt.as_ref().unwrap().clone();
    assert_eq!(call.caller.attempt_id, attempt.id);
    assert_eq!(attempt.worker_incarnation, "pending-first");
    assert!(process::now_ms() < accepted.limits.deadline_unix_ms);
    let journal_path = lab
        .state()
        .join("collaboration")
        .join(process::component(&attempt.id))
        .join("0000000001.json");
    let pending_bytes = std::fs::read(&journal_path).unwrap();
    let pending: Value = serde_json::from_slice(&pending_bytes).unwrap();
    assert!(pending["outcome"].is_null());
    assert!(pending["outcome_context"].is_null());
    assert_eq!(pending["consumed"], false);
    let (original, original_commit) = exact_call_record(&api, &call).await;
    let original_memory = memory(&api).await;
    assert!(
        matches!(&original_memory, Outcome::Ok {data: Data::Memory(Some(m))} if m.revision == 1 && m.value == json!(7))
    );
    assert_eq!(
        original.outcome.as_ref(),
        Some(&serde_json::to_value(&original_memory).unwrap())
    );

    // A different caller cannot borrow the retained outcome or start a new
    // occurrence, even while the original task is still legitimately active.
    let mut wrong = call.caller.clone();
    wrong.generation += 1;
    refused(
        &api,
        Operation::GetCollaborationCall {
            caller: wrong.clone(),
            operation_id: call.operation_id.clone(),
        },
        "caller_mismatch",
    )
    .await;
    let mut next = call.clone();
    next.operation_id = "wrong-generation-new-write".into();
    next.logical_call_id = "wrong-generation-new-write-logical".into();
    next.arguments = json!({"key":"note","expected_revision":1,"value":8});
    next.caller = wrong;
    refused(&api, next.operation(), "stale_generation").await;
    assert_eq!(memory(&api).await, original_memory);

    let terminal = until(&api, &task.id, |t| {
        t.status == TaskStatus::Failed
            && t.attempt.as_ref().is_some_and(|a| a.stopped && a.uncertain)
    })
    .await;
    assert!(process::now_ms() >= accepted.limits.deadline_unix_ms);
    assert!(
        anchor.elapsed() < Duration::from_millis(lab.limits.transport.client_timeout_ms),
        "task deadline must precede original HTTP timeout"
    );
    assert!(
        worker.child.try_wait().unwrap().is_none(),
        "deadline ends the task, not the worker daemon"
    );
    assert!(terminal.result.is_none());
    assert_eq!(terminal.input, accepted);
    assert_eq!(
        live_usage(&api, terminal.attempt.as_ref().unwrap()).await,
        (1, Some(5), Some(2))
    );
    worker.stop().await;
    let settled = get(&api, &task.id).await;
    assert_eq!(settled.input, accepted);
    assert!(settled.result.is_none());
    let reports = gate.reports.lock().unwrap().clone();
    let (uncertain, uncertainty_reply) = reports.iter().find(|(report, reply)|
        matches!(&report.observation, Observation::ExecutionUncertain { message }
            if message == "collaboration call outcome uncertain: collaboration lookup cancelled or expired")
        && matches!(reply.outcome, Outcome::Ok { .. })).expect("canonical deadline uncertainty report");
    assert_eq!(uncertain.attempt_id, attempt.id);
    assert_eq!(
        uncertainty_reply.commit.as_ref().unwrap().operation_id,
        uncertain.report_id
    );
    assert_eq!(std::fs::read(&journal_path).unwrap(), pending_bytes);
    assert_eq!(gate.sent.lock().unwrap().len(), 1);
    let lookups = gate.lookups.lock().unwrap().clone();
    assert_eq!(lookups.len(), 1);
    assert_eq!(lookups[0]["params"]["call_id"], call.operation_id);
    assert_eq!(peer.count(), 1);
    // The held lookup may now finish; it has no living original caller future.
    gate.holding.store(false, Ordering::SeqCst);
    gate.release.notify_one();
    controller.stop().await;

    let mut controller = lab.controller("controller-two").await;
    assert_eq!(
        get(&api, &task.id).await,
        settled,
        "complete quiescent task after Controller reopen"
    );
    assert_eq!(
        exact_call_record(&api, &call).await,
        (original.clone(), original_commit.clone())
    );
    let mut replacement = lab.worker(&gate.url, &peer.endpoint, "pending-second");
    let until_poll = tokio::time::Instant::now()
        + Duration::from_millis(
            lab.limits.controller.command_wait_ms + lab.limits.transport.client_timeout_ms,
        );
    while !proxy
        .await_replies
        .lock()
        .unwrap()
        .iter()
        .any(|(id, inc)| id == "loop-worker" && inc == "pending-second")
    {
        assert!(replacement.child.try_wait().unwrap().is_none());
        assert!(
            tokio::time::Instant::now() < until_poll,
            "replacement must actually complete its canonical command poll"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let current = get(&api, &task.id).await;
    assert_eq!(current, settled);
    assert_eq!(std::fs::read(&journal_path).unwrap(), pending_bytes);
    assert_eq!(
        exact_call_record(&api, &call).await,
        (original.clone(), original_commit.clone())
    );
    let mut changed = call.caller.clone();
    changed.incarnation = "pending-second".into();
    refused(
        &api,
        Operation::GetCollaborationCall {
            caller: changed,
            operation_id: call.operation_id.clone(),
        },
        "caller_mismatch",
    )
    .await;
    let mut expired = call.clone();
    expired.operation_id = "expired-new-write".into();
    expired.logical_call_id = "expired-new-write-logical".into();
    expired.arguments = json!({"key":"note","expected_revision":1,"value":8});
    refused(&api, expired.operation(), "invalid_transition").await;
    refused(
        &api,
        Operation::RetryTask {
            operation_id: "pending-retry".into(),
            task_id: task.id.clone(),
            expected_revision: current.revision,
        },
        "reconciliation_required",
    )
    .await;
    assert_eq!(get(&api, &task.id).await, settled);
    assert_eq!(memory(&api).await, original_memory);
    assert_eq!(
        live_usage(&api, current.attempt.as_ref().unwrap()).await,
        (1, Some(5), Some(2))
    );
    assert_eq!(peer.count(), 1);
    assert_eq!(
        gate.sent.lock().unwrap().len(),
        1,
        "replacement must not resend the pending mutation"
    );
    replacement.stop().await;
    controller.stop().await;
    assert_eq!(std::fs::read(&journal_path).unwrap(), pending_bytes);
    assert_eq!(lab.usage(&attempt.id), (1, Some(5), Some(2)));
}
