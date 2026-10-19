//! Real worker/controlled executor integration with the real Controller and HTTP
//! adapter. The existing injected clock proves ordinary governance admission is
//! unavailable; it does not simulate every possible rule-evaluator failure.
use super::*;
use borg_runtime::{
    collaboration::{AttemptView, Query, QueryResult},
    controller::{Controller, ControllerConfig},
    governance::*,
    governance_completion::{self, CompletionBoundary, EvaluateCompletion},
    limits::{DefinitionLimitField, GovernanceLimits, WorkflowLimits},
    mcp::Caller,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::AtomicBool;

// The public server spawns connection tasks. Give it an owned runtime so closing
// a listener cannot leave a connection's Controller clone owning the database.
struct Owner {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Owner {
    fn open(lab: &Lab, clock: Arc<AtomicU64>) -> Self {
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let (ready, started) = std::sync::mpsc::sync_channel(1);
        let path = lab.root.join("tasks.sqlite");
        let evidence = lab.root.clone();
        let owner_number = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let address = lab.address;
        let origin = lab.origin.clone();
        let config = ControllerConfig {
            limits: lab.limits.clone(),
            validation_executable: lab.image.clone(),
            clock: Arc::new(move || clock.load(Ordering::SeqCst)),
            ..ControllerConfig::default()
        };
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let controller = Controller::open_with_config(path, config).unwrap();
                let listener = borg_runtime::server::listen(address).await.unwrap();
                ready.send(()).unwrap();
                tokio::select! {
                    result = borg_runtime::server::serve(controller, listener, origin) => {
                        panic!("fixture server ended before owned shutdown: {result:?}")
                    }
                    _ = stopped => {}
                }
            });
            // Drop/abort all connection futures and their Controller clones.
            // The caller separately proves actual SQLite owner-lock release.
            runtime.shutdown_timeout(Duration::from_secs(3));
            save(
                evidence.join(format!("governance-http-thread-{owner_number}-closed.json")),
                &json!({
                    "server_future_closed": true, "owned_runtime_shutdown": true,
                    "scope": "test-owned real Controller HTTP runtime, not a controller OS process"
                }),
            );
        });
        started
            .recv_timeout(TIMEOUT)
            .expect("owned HTTP Controller ready");
        Self {
            stop: Some(stop),
            thread: Some(thread),
        }
    }
    fn close(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.close();
    }
}

async fn owner_released(lab: &Lab) {
    let path = lab.root.join("tasks.sqlite.controller-lock");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            if file.try_lock().is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("all old HTTP connections and Controller owner released");
}

async fn configured(label: &str) -> (Lab, Arc<AtomicU64>, Owner) {
    configured_with_reason_items(label, 16).await
}

async fn configured_with_reason_items(label: &str, reason_items: usize) -> (Lab, Arc<AtomicU64>, Owner) {
    let mut lab = Lab::new(label).await;
    // The existing Lab creates its actual repository through the canonical API.
    // It has accepted no tasks before this explicit fixture metadata selection.
    lab.controller.take().unwrap().kill();
    owner_released(&lab).await;
    let limits = Arc::make_mut(&mut lab.limits);
    limits.revision = format!("{}-s5-control", limits.revision);
    // Existing tests/controller.rs::config uses the same 20ms bounded wait.
    // The accepted task's absolute deadline and worker retry policy stay intact.
    limits.controller.command_wait_ms = 20;
    // Exact existing governance_config fixture values, not runtime defaults.
    limits.workflow = Some(WorkflowLimits {
        record_bytes: 32_768,
        text_bytes: 4_096,
        options: 8,
        records: 512,
        revisions: 4_096,
        page_items: 8,
        page_bytes: 32_768,
        waits_per_task: 8,
        graph_scan_nodes: 512,
        events_per_mutation: 64,
        event_bytes: 8_192,
        notice_bytes: 8_192,
        pending_delivery_bytes: 32_768,
        delivery_scan_rows: 64,
    });
    limits.governance = Some(GovernanceLimits {
        record_bytes: 65_536,
        reason_items,
        reason_bytes: 4_096,
        scan_rows: 512,
        rule_versions: 16,
        configuration_versions: 16,
        shadow_evidence_items: 16,
        completion_continuations: 2,
        override_max_ms: 10_000,
    });
    limits.validate().unwrap();
    save(lab.root.join("limits.json"), limits);
    lab.client = ApiClient::new(&lab.origin, lab.limits.clone()).unwrap();
    let clock = Arc::new(AtomicU64::new(now()));
    let owner = Owner::open(&lab, clock.clone());
    (lab, clock, owner)
}

#[derive(Default)]
struct Traffic {
    withheld_stop: Option<Report>,
    withheld_poll: Option<Request>,
    poll_ids: BTreeSet<String>,
    polls: Vec<(Request, Reply)>,
    pending_count: usize,
    last_pending_poll: Option<(Request, Reply)>,
    withheld_evaluation: Option<(Request, Reply)>,
}
struct ControlProxy {
    url: String,
    release_stop: Arc<AtomicBool>,
    hold_polls: Arc<AtomicBool>,
    hold_evaluation: Arc<AtomicBool>,
    traffic: Arc<Mutex<Traffic>>,
    join: tokio::task::JoinHandle<()>,
}
impl ControlProxy {
    async fn new(lab: &Lab, stop_task: Option<String>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let release_stop = Arc::new(AtomicBool::new(stop_task.is_none()));
        let traffic = Arc::new(Mutex::new(Traffic::default()));
        let hold_polls = Arc::new(AtomicBool::new(false));
        let polls_gate = hold_polls.clone();
        let hold_evaluation = Arc::new(AtomicBool::new(false));
        let evaluation_gate = hold_evaluation.clone();
        let (gate, saved, upstream, evidence, bound) = (
            release_stop.clone(),
            traffic.clone(),
            lab.origin.clone(),
            lab.root.clone(),
            lab.limits.transport.body_bytes,
        );
        let join = tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(28))
                .build()
                .unwrap();
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    incoming = listener.accept() => {
                        let (stream, _) = incoming.unwrap();
                        let (client, upstream, gate, saved, evidence, stop_task, polls_gate, evaluation_gate) =
                            (client.clone(), upstream.clone(), gate.clone(), saved.clone(), evidence.clone(), stop_task.clone(), polls_gate.clone(), evaluation_gate.clone());
                        connections.spawn(async move {
                            let service = service_fn(move |request: HttpRequest<Incoming>| {
                                let (client, upstream, gate, saved, evidence, stop_task, polls_gate, evaluation_gate) =
                                    (client.clone(), upstream.clone(), gate.clone(), saved.clone(), evidence.clone(), stop_task.clone(), polls_gate.clone(), evaluation_gate.clone());
                                async move {
                                    assert_eq!(request.uri().path(), "/api/operations");
                                    let bytes = request.into_body().collect().await.map_err(io::Error::other)?.to_bytes();
                                    assert!(bytes.len() <= bound);
                                    let original: Request = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                                    if let Operation::AwaitCommands { operation_id, .. } = &original.command {
                                        if polls_gate.load(Ordering::SeqCst) {
                                            let mut state = saved.lock().unwrap();
                                            if let Some(old) = &state.withheld_poll { assert_eq!(serde_json::to_value(&old.command).unwrap(), serde_json::to_value(&original.command).unwrap()); }
                                            else { state.withheld_poll = Some(original.clone()); }
                                            let reply = Reply { request_id: original.request_id.clone(),
                                                outcome: Outcome::Pending { operation_id: operation_id.clone() },
                                                commit: None, observed_controller_revision: None };
                                            return Ok::<_, io::Error>(HttpResponse::new(Full::new(Bytes::from(serde_json::to_vec(&reply).unwrap()))));
                                        }
                                    }
                                    if let Operation::ObserveWorker { report } = &original.command {
                                        if stop_task.as_ref() == Some(&report.task_id)
                                            && matches!(report.observation, Observation::ProcessTreeStopped)
                                            && !gate.load(Ordering::SeqCst)
                                        {
                                            let mut state = saved.lock().unwrap();
                                            if let Some(old) = &state.withheld_stop { assert_eq!(old, report); }
                                            else { state.withheld_stop = Some(report.clone()); }
                                            save(evidence.join("withheld-physical-stop.json"), &original);
                                            let reply = Reply { request_id: original.request_id,
                                                outcome: Outcome::Pending { operation_id: report.report_id.clone() },
                                                commit: None, observed_controller_revision: None };
                                            return Ok::<_, io::Error>(HttpResponse::new(Full::new(Bytes::from(serde_json::to_vec(&reply).unwrap()))));
                                        }
                                    }
                                    let response = client.post(format!("{upstream}/api/operations"))
                                        .header("content-type", "application/json").body(bytes.to_vec())
                                        .send().await.map_err(io::Error::other)?;
                                    let status = response.status();
                                    let body = response.bytes().await.map_err(io::Error::other)?;
                                    if matches!(&original.command, Operation::EvaluateCompletion(input)
                                        if stop_task.as_ref() == Some(&input.caller.task_id))
                                        && evaluation_gate.load(Ordering::SeqCst)
                                    {
                                        // Retain the actual original committed reply before
                                        // delaying its transport. Ordinary polls are untouched.
                                        let reply: Reply = serde_json::from_slice(&body).map_err(io::Error::other)?;
                                        reply.validate_for(&original.command).map_err(io::Error::other)?;
                                        assert!(reply.commit.is_some());
                                        assert!(matches!(&reply.outcome, Outcome::Refused { code, .. }
                                            if code == "governance_completion_blocked"));
                                        {
                                            let mut state = saved.lock().unwrap();
                                            if let Some((old, outcome)) = &state.withheld_evaluation {
                                                assert_eq!(serde_json::to_value(&old.command).unwrap(), serde_json::to_value(&original.command).unwrap());
                                                assert_eq!(outcome.outcome, reply.outcome);
                                                assert_eq!(outcome.commit, reply.commit);
                                            } else { state.withheld_evaluation = Some((original.clone(), reply)); }
                                            save(evidence.join("governance-withheld-evaluation.json"), &state.withheld_evaluation);
                                        }
                                        // Real transport withholding only; no Pending/receipt
                                        // is invented. The original worker control can cancel it.
                                        while evaluation_gate.load(Ordering::SeqCst) {
                                            tokio::time::sleep(Duration::from_millis(5)).await;
                                        }
                                    }
                                    if let Operation::AwaitCommands { operation_id, .. } = &original.command {
                                        let reply: Reply = serde_json::from_slice(&body).map_err(io::Error::other)?;
                                        reply.validate_for(&original.command).map_err(io::Error::other)?;
                                        let mut state = saved.lock().unwrap();
                                        if matches!(&reply.outcome, Outcome::Pending { .. }) {
                                            // An idle original Await can remain Pending. It grants
                                            // no command and is not a committed empty receipt.
                                            state.pending_count += 1;
                                            state.last_pending_poll = Some((original.clone(), reply));
                                            save(evidence.join("governance-worker-pending-poll.json"), &state.last_pending_poll);
                                        } else if state.poll_ids.insert(operation_id.clone()) {
                                            state.polls.push((original.clone(), reply));
                                            save(evidence.join("governance-worker-polls.json"), &state.polls);
                                        }
                                    }
                                    Ok(HttpResponse::builder().status(status.as_u16()).body(Full::new(body)).unwrap())
                                }
                            });
                            let _ = hyper::server::conn::http1::Builder::new()
                                .serve_connection(TokioIo::new(stream), service).await;
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Self {
            url,
            release_stop,
            hold_polls,
            hold_evaluation,
            traffic,
            join,
        }
    }
    fn count(&self) -> usize {
        self.traffic.lock().unwrap().polls.len()
    }
    async fn evaluation(&self) -> (Request, Reply) {
        tokio::time::timeout(TIMEOUT, async {
            loop {
                if let Some(value) = self.traffic.lock().unwrap().withheld_evaluation.clone() {
                    break value;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }).await.expect("actual worker completion evaluation committed before withheld reply")
    }
    async fn next_poll(&self, prior: usize) -> (Request, Reply) {
        tokio::time::timeout(TIMEOUT, async {
            loop {
                if let Some(value) = self.traffic.lock().unwrap().polls.get(prior).cloned() {
                    break value;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("real worker completed next distinct canonical AwaitCommands")
    }
    fn pending_count(&self) -> usize {
        self.traffic.lock().unwrap().pending_count
    }
    async fn next_pending_poll(&self, prior: usize, original: &Request) -> Reply {
        tokio::time::timeout(TIMEOUT, async {
            loop {
                {
                    let state = self.traffic.lock().unwrap();
                    if state.pending_count > prior {
                        let (observed, reply) = state.last_pending_poll.as_ref().unwrap();
                        assert_eq!(serde_json::to_value(&observed.command).unwrap(),
                            serde_json::to_value(&original.command).unwrap());
                        assert!(matches!(&reply.outcome, Outcome::Pending { operation_id }
                            if Some(operation_id.as_str()) == original.command.mutation_id()));
                        assert!(reply.commit.is_none());
                        return reply.clone();
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("same original Await observed Pending from the reopened Controller")
    }
    async fn physical_report(&self) -> Report {
        tokio::time::timeout(TIMEOUT, async {
            loop {
                if let Some(value) = self.traffic.lock().unwrap().withheld_stop.clone() {
                    break value;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("actual worker retained its physical-stop report")
    }
    async fn park_poll(&self) -> Request {
        self.traffic.lock().unwrap().withheld_poll = None;
        self.hold_polls.store(true, Ordering::SeqCst);
        tokio::time::timeout(TIMEOUT, async {
            loop {
                if let Some(value) = self.traffic.lock().unwrap().withheld_poll.clone() {
                    break value;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("real worker retained an unforwarded original AwaitCommands")
    }
}
impl Drop for ControlProxy {
    fn drop(&mut self) {
        self.join.abort();
    }
}

async fn request(lab: &Lab, operation: Operation) -> Reply {
    let reply = lab.client.call(operation.clone()).await.unwrap();
    reply.validate_for(&operation).unwrap();
    reply
}
fn value(reply: &Reply) -> GovernanceResult {
    match &reply.outcome {
        Outcome::Ok {
            data: Data::Governance(value),
        } => (**value).clone(),
        other => panic!("canonical governance success required: {other:?}"),
    }
}
fn change(id: &str, mutation: GovernanceMutation) -> Operation {
    Operation::MutateGovernance {
        operation_id: id.into(),
        mutation,
    }
}
async fn state(lab: &Lab) -> GovernanceState {
    let reply = request(
        lab,
        Operation::QueryGovernance {
            query: GovernanceQuery::State,
        },
    )
    .await;
    let GovernanceResult::State(state) = value(&reply) else {
        panic!("state required")
    };
    state
}
async fn replay(lab: &Lab, operation: &Operation, original: &Reply) {
    let again = request(lab, operation.clone()).await;
    assert_eq!(again.outcome, original.outcome);
    assert_eq!(again.commit, original.commit);
}
fn process_live(pid: u64) -> bool {
    fs::read_to_string(format!("/proc/{pid}/stat"))
        .ok()
        .and_then(|s| s.rsplit_once(") ").map(|(_, tail)| tail.as_bytes()[0]))
        .is_some_and(|state| state != b'Z' && state != b'X')
}
async fn no_physical_process(pid: u64) {
    tokio::time::timeout(TIMEOUT, async {
        while process_live(pid) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("actual executor process must exit, independently of canonical stop ACK");
}
fn same_identity(before: &Task, after: &Task) {
    assert_eq!(before.id, after.id);
    assert_eq!(before.session_id, after.session_id);
    assert_eq!(before.input, after.input);
    let (a, b) = (
        before.attempt.as_ref().unwrap(),
        after.attempt.as_ref().unwrap(),
    );
    assert_eq!(
        (
            &a.id,
            &a.task_id,
            &a.session_id,
            a.generation,
            &a.worker_id,
            &a.worker_incarnation
        ),
        (
            &b.id,
            &b.task_id,
            &b.session_id,
            b.generation,
            &b.worker_id,
            &b.worker_incarnation
        )
    );
    assert_eq!(a.deadline_unix_ms, b.deadline_unix_ms);
}

async fn attempt_view(lab: &Lab, task_id: &str) -> AttemptView {
    let reply = request(
        lab,
        Operation::QueryCollaboration {
            query: Query::TaskAttempts {
                task_id: task_id.into(),
                after: None,
                limit: 1,
            },
        },
    )
    .await;
    let Outcome::Ok {
        data: Data::Collaboration(QueryResult::TaskAttempts(page)),
    } = reply.outcome
    else {
        panic!("canonical attempt usage required")
    };
    assert!(page.next_cursor.is_none());
    assert_eq!(page.items.len(), 1);
    page.items.into_iter().next().unwrap()
}
fn same_usage(before: &AttemptView, after: &AttemptView) {
    assert_eq!(
        before.reported_model_requests,
        after.reported_model_requests
    );
    assert_eq!(before.input_tokens, after.input_tokens);
    assert_eq!(before.output_tokens, after.output_tokens);
}
fn original_command(proxy: &ControlProxy, task: &Task) -> WorkerCommand {
    let state = proxy.traffic.lock().unwrap();
    let mut found = None;
    for (_, reply) in &state.polls {
        if let Outcome::Ok {
            data: Data::Commands(commands),
        } = &reply.outcome
        {
            for command in commands {
                if let WorkerCommand::RunAttempt {
                    attempt,
                    task: input,
                    session,
                    ..
                } = command
                {
                    if attempt.task_id == task.id {
                        assert_eq!(input, &task.input);
                        session.validate_for(attempt, &task.input).unwrap();
                        if let Some(old) = &found {
                            assert_eq!(old, command);
                        }
                        found = Some(command.clone());
                    }
                }
            }
        }
    }
    found.expect("original real worker command captured from canonical Await receipt")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clock_unavailable_stop_replays_without_fabricating_missing_physical_evidence() {
    let (mut lab, clock, mut owner) = configured("governance-clock-stop").await;
    let accepted = lab.submit("clock-subject", "clock-subject-agent").await;
    let proxy = ControlProxy::new(&lab, Some(accepted.id.clone())).await;
    let worker = lab.start_worker("clock-worker", "inc-1", &proxy.url);
    let execution = lab.gate().await;
    let pid = execution.observed["pid"].as_u64().unwrap();
    assert!(process_live(pid));
    let active = lab
        .until(&accepted.id, "actual running attempt", |t| {
            t.attempt
                .as_ref()
                .is_some_and(|a| a.phase == AttemptPhase::Running)
        })
        .await;
    let original_input = active.input.clone();
    let original_usage = attempt_view(&lab, &active.id).await;
    // The supplied executor owns its inner request loop. Its initial turn
    // supplies no request count; only explicit outer continuation is reserved.
    assert_eq!(original_usage.reported_model_requests, 0);
    assert_eq!(original_usage.input_tokens, None);
    assert_eq!(original_usage.output_tokens, None);
    let command = original_command(&proxy, &active);
    save(lab.root.join("governance-original-command.json"), &command);
    let valid_time = clock.load(Ordering::SeqCst);
    clock.store(0, Ordering::SeqCst);
    let held_submission = Operation::SubmitTask {
        operation_id: "ordinary-clock-refusal".into(),
        input: (lab.input("clock-refused-agent")).into(),
    };
    let held_receipt = request(&lab, held_submission.clone()).await;
    assert!(held_receipt.commit.is_some());
    // Submission remains durable; unavailable ordinary admission queues it
    // without granting an attempt or executor. This is not a refused write.
    let held_task = task(held_receipt.clone());
    assert_eq!(held_task.status, TaskStatus::Queued);
    assert_eq!(held_task.blocked_reason.as_deref(), Some("governance_clock_unavailable"));
    assert!(held_task.attempt.is_none() && held_task.result.is_none());
    assert_eq!(lab.starts.len(), 1);
    replay(&lab, &held_submission, &held_receipt).await;
    let held_lookup = request(&lab, Operation::GetOperation {
        operation_id: held_submission.mutation_id().unwrap().into(),
    }).await;
    assert!(held_lookup.commit.is_some());
    assert!(matches!(&held_lookup.outcome, Outcome::Ok { data: Data::Operation(record) }
        if record.intended == serde_json::to_value(&held_submission).unwrap()
            && record.outcome == Some(serde_json::to_value(&held_receipt.outcome).unwrap())));
    save(lab.root.join("governance-clock-held-submission.json"),
        &json!({"request":held_submission,"reply":held_receipt,"lookup":held_lookup}));
    let stop = change(
        "clock-stop-original",
        GovernanceMutation::Stop {
            task_id: active.id.clone(),
            expected_revision: lab.get(&active.id).await.revision,
            reason: "stop while ordinary governance clock is unavailable".into(),
        },
    );
    let original = request(&lab, stop.clone()).await;
    assert_eq!(
        value(&original),
        GovernanceResult::StopRequested {
            task_id: active.id.clone()
        }
    );
    assert!(original.commit.is_some());
    replay(&lab, &stop, &original).await;
    let report = proxy.physical_report().await;
    assert_eq!(report.attempt_id, active.attempt.as_ref().unwrap().id);
    no_physical_process(pid).await;
    let missing = lab.get(&active.id).await;
    same_identity(&active, &missing);
    assert_eq!(missing.status, TaskStatus::Cancelled);
    assert!(
        !missing.attempt.as_ref().unwrap().stopped,
        "generation revocation, command ACK and locally observed exit do not create a missing canonical physical-stop report"
    );
    assert!(missing.result.is_none());
    let absent = request(
        &lab,
        Operation::GetOperation {
            operation_id: report.report_id.clone(),
        },
    )
    .await;
    assert_eq!(refused(absent), "not_found");
    // A second explicit Stop, as well as original replay, cannot fabricate proof.
    let repeated = change(
        "clock-stop-again",
        GovernanceMutation::Stop {
            task_id: missing.id.clone(),
            expected_revision: missing.revision,
            reason: "repeat independent stop while physical observation is withheld".into(),
        },
    );
    let repeated_reply = request(&lab, repeated.clone()).await;
    assert_eq!(
        value(&repeated_reply),
        GovernanceResult::StopRequested {
            task_id: missing.id.clone()
        }
    );
    assert!(!lab.get(&active.id).await.attempt.unwrap().stopped);
    replay(&lab, &stop, &original).await;
    proxy.release_stop.store(true, Ordering::SeqCst);
    let stopped = lab
        .until(
            &active.id,
            "original actual physical-stop observation",
            |t| t.attempt.as_ref().is_some_and(|a| a.stopped && a.uncertain),
        )
        .await;
    same_identity(&active, &stopped);
    assert_eq!(stopped.input, original_input);
    assert!(stopped.result.is_none());
    same_usage(&original_usage, &attempt_view(&lab, &active.id).await);
    let recorded = request(
        &lab,
        Operation::GetOperation {
            operation_id: report.report_id.clone(),
        },
    )
    .await;
    assert!(recorded.commit.is_some());
    assert!(
        matches!(&recorded.outcome, Outcome::Ok { data: Data::Operation(record) }
        if record.intended == serde_json::to_value(Operation::ObserveWorker { report: report.clone() }).unwrap())
    );
    assert_eq!(lab.starts.len(), 1);
    let still_held = lab.get(&held_task.id).await;
    assert_eq!(still_held.input, held_task.input);
    assert_eq!(still_held.status, TaskStatus::Queued);
    assert_eq!(still_held.blocked_reason.as_deref(), Some("governance_clock_unavailable"));
    assert!(still_held.attempt.is_none() && still_held.result.is_none());
    // Retire only this unstarted fixture probe before restoring the clock, so
    // subsequent ordinary admission cannot create an unrelated executor.
    let retired_probe = task(request(&lab, Operation::CancelTask {
        operation_id: "ordinary-clock-probe-retire".into(),
        task_id: still_held.id, expected_revision: still_held.revision,
    }).await);
    assert_eq!(retired_probe.status, TaskStatus::Cancelled);
    assert!(retired_probe.attempt.is_none() && retired_probe.result.is_none());
    replay(&lab, &held_submission, &held_receipt).await;
    clock.store(valid_time.max(now()), Ordering::SeqCst);
    lab.workers[worker].stop();
    drop(execution);
    drop(proxy);
    owner.close();
    owner_released(&lab).await;
    let mut reopened = Owner::open(&lab, clock);
    let after = lab.get(&active.id).await;
    same_identity(&stopped, &after);
    assert!(after.attempt.as_ref().unwrap().stopped && after.attempt.as_ref().unwrap().uncertain);
    assert!(after.result.is_none());
    same_usage(&original_usage, &attempt_view(&lab, &active.id).await);
    replay(&lab, &stop, &original).await;
    replay(&lab, &repeated, &repeated_reply).await;
    assert_eq!(
        refused(
            request(
                &lab,
                Operation::RetryTask {
                    operation_id: "clock-no-blind-retry".into(),
                    task_id: after.id.clone(),
                    expected_revision: after.revision,
                }
            )
            .await
        ),
        "reconciliation_required"
    );
    reopened.close();
    owner_released(&lab).await;
    save(
        lab.root.join("case-complete.json"),
        &json!({"status":"PASS", "real_workers":1,
        "controller":"actual library owner and HTTP adapter with fully joined runtime restart",
        "fault":"ordinary governance clock/admission unavailable; not arbitrary evaluator corruption",
        "executor":"controlled Codex peer", "provider_calls":0, "executor_starts":lab.starts}),
    );
}

// This supplements the clock case: its fatal ordinary-poll refusal can shut
// down the worker before Stop. Here a genuine bounded evaluator failure is
// recorded while ordinary command polling and the actual worker remain live.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_completion_evaluator_failure_still_delivers_stop_to_the_live_worker() {
    use borg_runtime::collaboration::{questions::*, workflow::*};

    let (mut lab, _clock, mut owner) = configured_with_reason_items("governance-evaluator-stop", 1).await;
    let policy = qualify(&lab, "bounded-evaluator-policy").await;
    assert_eq!(state(&lab).await.selected_revision, policy.revision);
    let mut input = lab.input("bounded-evaluator-agent");
    input.definition.execution_profile = Some(ExecutionProfile::CollaborationV2 { publication: None, web_search: None });
    let accepted = task(request(&lab, Operation::SubmitTask {
        operation_id: "bounded-evaluator-submit".into(), input: (input).into(),
    }).await);
    assert_eq!(accepted.input.resolved_limits.as_ref().unwrap().global_metadata.governance.as_ref().unwrap().reason_items, 1);
    let proxy = ControlProxy::new(&lab, Some(accepted.id.clone())).await;
    proxy.hold_evaluation.store(true, Ordering::SeqCst);
    let worker = lab.start_worker("evaluator-worker", "inc-1", &proxy.url);
    let execution = lab.gate().await;
    let pid = execution.observed["pid"].as_u64().unwrap();
    let thread = execution.observed["thread"].as_str().unwrap().to_owned();
    let turn = execution.observed["turn"].as_str().unwrap().to_owned();
    let active = lab.until(&accepted.id, "actual running evaluator subject", |t| {
        t.attempt.as_ref().is_some_and(|a| a.phase == AttemptPhase::Running)
    }).await;
    let original_usage = attempt_view(&lab, &active.id).await;
    let original = original_command(&proxy, &active);
    assert!(process_live(pid));
    assert!(lab.workers[worker].child.try_wait().unwrap().is_none());

    // Real operator question closures create owed terminal input through the
    // linked task's current owner. AddWait is deliberately not used: it would
    // itself request a stop before the emergency operation under test.
    let mut closed = Vec::new();
    for index in 0..2 {
        let posed = request(&lab, Operation::MutateQuestion {
            operation_id: format!("bounded-evaluator-question-{index}"),
            mutation: QuestionMutation::Pose { question: PoseQuestion {
                record: RecordRef::Task { task_id: active.id.clone() },
                recipient: PrincipalRef::TrustedHostOperator,
                prompt: "Choose the recorded option".into(),
                options: vec![
                    QuestionOption { id: "keep".into(), label: "Keep".into(), explanation: "Keep the current choice".into() },
                    QuestionOption { id: "change".into(), label: "Change".into(), explanation: "Use the other choice".into() },
                ], recommended_option_id: "keep".into(),
            } },
        }).await;
        let Outcome::Ok { data: Data::QuestionMutation(value) } = posed.outcome else { panic!("canonical question pose required") };
        assert_eq!(value.question.current_responsible_owner, PrincipalRef::Agent { agent_id: active.assigned_agent_id.clone() });
        let question = value.question.question;
        let operation = Operation::MutateQuestion {
            operation_id: format!("bounded-evaluator-close-{index}"),
            mutation: QuestionMutation::Cancel {
                question_id: question.id.clone(), expected_revision: question.revision,
                reason: "record a terminal input before actual voluntary completion".into(),
            },
        };
        let reply = request(&lab, operation.clone()).await;
        assert!(reply.commit.is_some());
        assert!(matches!(&reply.outcome, Outcome::Ok { data: Data::QuestionMutation(value) }
            if value.question.question.id == question.id && value.question.question.status == QuestionStatus::Cancelled));
        closed.push((operation, reply));
    }
    // The controlled peer emits the real terminal event, then remains an OS
    // process while the worker awaits its original completion-evaluation ACK.
    // This precedes the worker's evaluation call and its client timeout.
    let held_at = tokio::time::Instant::now();
    execution.release().await;
    let (evaluation_request, evaluation_reply) = proxy.evaluation().await;
    let Operation::EvaluateCompletion(evaluation) = &evaluation_request.command else { unreachable!() };
    assert_eq!(evaluation.caller.task_id, active.id);
    assert_eq!(evaluation.caller.attempt_id, active.attempt.as_ref().unwrap().id);
    assert_eq!(evaluation.boundary, CompletionBoundary::CodexTurn { thread_id: thread, turn_id: turn });
    assert_eq!(evaluation.operation_id, governance_completion::evaluation_id(&evaluation.caller, &evaluation.boundary).unwrap());
    let decision_id = DecisionRef { operation_id: evaluation.operation_id.clone(), point: DecisionPoint::Completion };
    let audit = request(&lab, Operation::QueryGovernance {
        query: GovernanceQuery::Decision { decision: decision_id.clone() },
    }).await;
    let GovernanceResult::Decision(decision) = value(&audit) else { panic!("original enforcing decision required") };
    assert_eq!(decision.id, decision_id);
    assert_eq!(decision.controller_revision, evaluation_reply.commit.as_ref().unwrap().controller_revision);
    assert_eq!(decision.mode, DecisionMode::Enforcing);
    assert_eq!(decision.actual_outcome, ActualDecisionOutcome::GovernanceBlocked);
    let DecisionDetail::Completion(completion) = &decision.detail else { panic!("typed ordinary completion decision required") };
    assert_eq!(completion.reasons, vec![CompletionReason::EvaluationUnavailable { code: CompletionFailure::BoundExceeded }]);
    assert_eq!(completion.disposition, CompletionDisposition::Blocked { continuation_eligible: false, exhausted: false });
    let blocked = lab.get(&active.id).await;
    same_identity(&active, &blocked);
    assert_eq!(blocked.status, TaskStatus::Active);
    assert_eq!(blocked.blocked_reason, Some(format!("governance_completion_blocked:{}", evaluation.operation_id)));
    assert!(!blocked.attempt.as_ref().unwrap().stopped && !blocked.attempt.as_ref().unwrap().uncertain);
    assert!(process_live(pid), "same executor must still be alive after genuine evaluator failure");
    assert!(lab.workers[worker].child.try_wait().unwrap().is_none(), "ordinary polling must not have stopped the worker");
    assert!(!proxy.hold_polls.load(Ordering::SeqCst), "ordinary command polls are never gated in this case");

    let stop = change("bounded-evaluator-stop", GovernanceMutation::Stop {
        task_id: blocked.id.clone(), expected_revision: blocked.revision,
        reason: "stop independently of the recorded bounded evaluator failure".into(),
    });
    let stop_reply = request(&lab, stop.clone()).await;
    assert_eq!(value(&stop_reply), GovernanceResult::StopRequested { task_id: blocked.id.clone() });
    assert!(stop_reply.commit.is_some());
    let (delivery, delivery_reply, stop_command, acknowledgement, acknowledgement_reply) = tokio::time::timeout(TIMEOUT, async {
        loop {
            let found = {
                let traffic = proxy.traffic.lock().unwrap();
                traffic.polls.iter().find_map(|(request, reply)| {
                    let Outcome::Ok { data: Data::Commands(commands) } = &reply.outcome else { return None };
                    let command = commands.iter().find(|command| matches!(command,
                        WorkerCommand::StopAttempt { task_id, attempt_id, incarnation, .. }
                        if task_id == &active.id && attempt_id == &evaluation.caller.attempt_id
                            && incarnation == &evaluation.caller.incarnation))?;
                    let WorkerCommand::StopAttempt { command_id, .. } = command else { unreachable!() };
                    let ack = traffic.polls.iter().find(|(request, reply)| {
                        matches!(&request.command, Operation::AwaitCommands { acknowledged, .. } if acknowledged.contains(command_id))
                            && matches!(&reply.outcome, Outcome::Ok { data: Data::Commands(_) }) && reply.commit.is_some()
                    })?;
                    Some((request.clone(), reply.clone(), command.clone(), ack.0.clone(), ack.1.clone()))
                })
            };
            if let Some(value) = found { break value; }
            assert!(lab.workers[worker].child.try_wait().unwrap().is_none(), "worker exited before original Stop command acknowledgement");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.expect("canonical StopAttempt delivery and original worker acknowledgement");
    assert!(delivery_reply.commit.is_some());
    assert!(delivery_reply.commit.as_ref().unwrap().controller_revision > stop_reply.commit.as_ref().unwrap().controller_revision);
    let physical = proxy.physical_report().await;
    assert_eq!(physical.attempt_id, evaluation.caller.attempt_id);
    assert_eq!(physical.incarnation, evaluation.caller.incarnation);
    no_physical_process(pid).await;
    let timeout = lab.limits.worker.mutation_call_timeout_ms.min(lab.limits.transport.client_timeout_ms);
    assert!(held_at.elapsed() < Duration::from_millis(timeout), "client timeout cannot explain this physical shutdown");
    assert!(lab.workers[worker].child.try_wait().unwrap().is_none(), "Stop cancels the attempt, not the worker command stream");
    assert!(proxy.traffic.lock().unwrap().polls.iter().all(|(_, reply)| !matches!(reply.outcome, Outcome::Refused { .. })));
    let missing = lab.get(&active.id).await;
    assert_eq!(missing.status, TaskStatus::Cancelled);
    assert!(!missing.attempt.as_ref().unwrap().stopped);
    assert_eq!(refused(request(&lab, Operation::GetOperation { operation_id: physical.report_id.clone() }).await), "not_found");
    proxy.hold_evaluation.store(false, Ordering::SeqCst);
    proxy.release_stop.store(true, Ordering::SeqCst);
    let stopped = lab.until(&active.id, "known completed executor physically stopped after original Stop", |task| {
        task.attempt.as_ref().is_some_and(|a| a.stopped)
    }).await;
    same_identity(&active, &stopped);
    assert_eq!(stopped.status, TaskStatus::Cancelled);
    assert!(!stopped.attempt.as_ref().unwrap().uncertain, "genuine terminal turn clears executor uncertainty");
    assert!(stopped.result.is_none());
    same_usage(&original_usage, &attempt_view(&lab, &active.id).await);
    assert_eq!(lab.starts.len(), 1);
    for (operation, reply) in [&closed[0], &closed[1]] { replay(&lab, operation, reply).await; }
    replay(&lab, &evaluation_request.command, &evaluation_reply).await;
    replay(&lab, &stop, &stop_reply).await;
    replay(&lab, &delivery.command, &delivery_reply).await;
    replay(&lab, &acknowledgement.command, &acknowledgement_reply).await;
    save(lab.root.join("bounded-evaluator-stop-proof.json"), &json!({
        "original_run":original,"question_closures":closed,"evaluation_request":evaluation_request,
        "evaluation_reply":evaluation_reply,"decision":decision,"stop":stop,"stop_reply":stop_reply,
        "delivery":delivery,"delivery_reply":delivery_reply,"stop_command":stop_command,
        "acknowledgement":acknowledgement,"acknowledgement_reply":acknowledgement_reply,
        "physical_report":physical,"same_executor_pid":pid,"task":stopped,
        "scope":"real worker and controlled executor; genuine bounded evaluator failure, not arbitrary evaluator corruption",
    }));
    lab.workers[worker].stop();
    drop(proxy);
    owner.close();
    owner_released(&lab).await;
}

// Policy setup mirrors the existing governance::promote fixture: real immutable
// registration, actual shadow receipts and promotion. Synthetic native reports
// qualify the policies only; all subject cancellation/recovery is a real worker.
async fn qualify(lab: &Lab, label: &str) -> ConfigurationVersion {
    let initial = request(
        lab,
        Operation::QueryGovernance {
            query: GovernanceQuery::Configuration {
                revision: lab.limits.revision.clone(),
            },
        },
    )
    .await;
    let GovernanceResult::Configuration(mut candidate) = value(&initial) else {
        panic!("bootstrap configuration")
    };
    candidate.revision = format!("{label}-configuration");
    let registered = request(
        lab,
        change(
            &format!("{label}-register"),
            GovernanceMutation::RegisterConfiguration {
                configuration: candidate.clone(),
            },
        ),
    )
    .await;
    assert_eq!(
        value(&registered),
        GovernanceResult::Configuration(candidate.clone())
    );
    let mut input = lab.input(&format!("{label}-policy-subject"));
    input.definition.executor = ExecutorKind::Native;
    input.definition.execution_profile =
        Some(ExecutionProfile::CollaborationV2 { publication: None, web_search: None });
    input.repository = None;
    input.acceptance_commands.clear();
    input.output_schema = Some(
        json!({"type":"object","required":["answer"],"properties":{"answer":{"type":"integer"}},"additionalProperties":false}),
    );
    let submitted = task(
        request(
            lab,
            Operation::SubmitTask {
                operation_id: format!("{label}-submit"),
                input: (input).into(),
            },
        )
        .await,
    );
    let worker = Worker {
        id: format!("{label}-policy-worker"),
        incarnation: "inc-1".into(),
        coding_capacity: 0,
        native_capacity: 1,
    };
    assert!(matches!(
        request(
            lab,
            Operation::RegisterWorker {
                operation_id: format!("{label}-worker"),
                worker: worker.clone()
            }
        )
        .await
        .outcome,
        Outcome::Ok { .. }
    ));
    let issued = request(
        lab,
        Operation::AwaitCommands {
            operation_id: format!("{label}-await"),
            worker_id: worker.id.clone(),
            incarnation: worker.incarnation.clone(),
            acknowledged: vec![],
        },
    )
    .await;
    let Outcome::Ok {
        data: Data::Commands(commands),
    } = issued.outcome
    else {
        panic!("policy subject command")
    };
    let (command_id, attempt) = commands
        .into_iter()
        .find_map(|command| match command {
            WorkerCommand::RunAttempt {
                command_id,
                attempt,
                ..
            } => Some((command_id, attempt)),
            _ => None,
        })
        .unwrap();
    assert_eq!(attempt.task_id, submitted.id);
    let observation = |id: String, observation| Operation::ObserveWorker {
        report: Report {
            report_id: id,
            worker_id: worker.id.clone(),
            incarnation: worker.incarnation.clone(),
            task_id: submitted.id.clone(),
            attempt_id: attempt.id.clone(),
            generation: attempt.generation,
            observation,
        },
    };
    task(
        request(
            lab,
            observation(
                format!("{label}-started"),
                Observation::Started {
                    thread_id: None,
                    turn_id: None,
                },
            ),
        )
        .await,
    );
    request(
        lab,
        Operation::AwaitCommands {
            operation_id: format!("{label}-ack"),
            worker_id: worker.id.clone(),
            incarnation: worker.incarnation.clone(),
            acknowledged: vec![command_id],
        },
    )
    .await;
    task(
        request(
            lab,
            observation(
                borg_runtime::native_loop_contract::reservation_id(&attempt.id, 1),
                Observation::Usage {
                    native_diagnostic_reserved: true,
                    model_requests: 1,
                    input_tokens: Some(4),
                    output_tokens: Some(2),
                },
            ),
        )
        .await,
    );
    let diagnostic = request(
        lab,
        observation(
            borg_runtime::native_loop_contract::diagnostic_id(&attempt.id, 1),
            Observation::NativeRequestDiagnostic {
                request_index: 1,
                endpoint: "https://api.openai.com/v1/responses".into(),
                model: submitted
                    .input
                    .definition
                    .required_inference()
                    .unwrap()
                    .model
                    .clone(),
                request_binding: Some(borg_runtime::native::ProviderRequestBinding {
                    bytes: 2,
                    sha256: "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
                        .into(),
                }),
                observed_outcome:
                    borg_runtime::native_loop_contract::NativeRequestObservedOutcome::Completed,
                failure: None,
            },
        ),
    )
    .await;
    assert!(matches!(
        diagnostic.outcome,
        Outcome::Ok {
            data: Data::Acknowledged
        }
    ));
    let caller = Caller {
        task_id: submitted.id.clone(),
        attempt_id: attempt.id.clone(),
        generation: attempt.generation,
        worker_id: worker.id.clone(),
        incarnation: worker.incarnation.clone(),
    };
    let boundary = CompletionBoundary::NativeResponse { request_index: 1 };
    let complete_id = governance_completion::evaluation_id(&caller, &boundary).unwrap();
    let completed = request(
        lab,
        Operation::EvaluateCompletion(EvaluateCompletion {
            operation_id: complete_id.clone(),
            caller,
            boundary,
            intervening_requests: vec![],
        }),
    )
    .await;
    assert!(matches!(
        completed.outcome,
        Outcome::Ok {
            data: Data::Completion(_)
        }
    ));
    let mut evidence = vec![];
    let completion_shadow = format!("{label}-completion-shadow");
    let shadow = request(
        lab,
        change(
            &completion_shadow,
            GovernanceMutation::EvaluateShadow {
                original: DecisionRef {
                    operation_id: complete_id,
                    point: DecisionPoint::Completion,
                },
                candidate: candidate.completion.clone(),
                configuration_revision: candidate.revision.clone(),
            },
        ),
    )
    .await;
    assert!(matches!(
        value(&shadow),
        GovernanceResult::Shadow(ShadowResult {
            evaluation: ShadowEvaluation::Evaluated { passing: true, .. },
            ..
        })
    ));
    evidence.push(ShadowEvidenceRef {
        operation_id: completion_shadow,
    });
    let definition_id = format!("{label}-definition-neutral");
    let update = request(
        lab,
        Operation::UpdateDefinition {
            operation_id: definition_id.clone(),
            update: DefinitionUpdate {
                agent_id: submitted.input.definition.id.clone(),
                expected_revision: submitted.input.definition.revision.clone(),
                expected_global_revision: lab.limits.revision.clone(),
                expected_policy_revision: lab
                    .limits
                    .definition_updates
                    .as_ref()
                    .unwrap()
                    .revision
                    .clone(),
                changes: BTreeMap::from([(
                    DefinitionLimitField::NativeContextBytes,
                    lab.limits
                        .resolve_definition(&submitted.input.definition)
                        .unwrap()
                        .native
                        .context_bytes,
                )]),
            },
        },
    )
    .await;
    assert!(matches!(
        update.outcome,
        Outcome::Ok {
            data: Data::DefinitionUpdate(_)
        }
    ));
    let definition_shadow = format!("{label}-definition-shadow");
    let shadow = request(
        lab,
        change(
            &definition_shadow,
            GovernanceMutation::EvaluateShadow {
                original: DecisionRef {
                    operation_id: definition_id,
                    point: DecisionPoint::DefinitionLimits,
                },
                candidate: candidate.definition_limits.clone(),
                configuration_revision: candidate.revision.clone(),
            },
        ),
    )
    .await;
    assert!(matches!(
        value(&shadow),
        GovernanceResult::Shadow(ShadowResult {
            evaluation: ShadowEvaluation::Evaluated { passing: true, .. },
            ..
        })
    ));
    evidence.push(ShadowEvidenceRef {
        operation_id: definition_shadow,
    });
    let promoted = request(
        lab,
        change(
            &format!("{label}-promote"),
            GovernanceMutation::Promote {
                expected_revision: state(lab).await.selected_revision,
                configuration_revision: candidate.revision.clone(),
                evidence,
                supersedes_override: None,
            },
        ),
    )
    .await;
    assert_eq!(
        value(&promoted),
        GovernanceResult::State(GovernanceState {
            selected_revision: candidate.revision.clone(),
            temporary_override: None
        })
    );
    let current = lab.get(&submitted.id).await;
    task(
        request(
            lab,
            Operation::CancelTask {
                operation_id: format!("{label}-retire"),
                task_id: current.id,
                expected_revision: current.revision,
            },
        )
        .await,
    );
    let stopped = task(
        request(
            lab,
            observation(format!("{label}-stopped"), Observation::ProcessTreeStopped),
        )
        .await,
    );
    assert!(stopped.attempt.unwrap().stopped);
    candidate
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn override_expiry_and_owner_restart_preserve_hold_until_exact_release_and_real_admission() {
    let (mut lab, clock, mut owner) = configured("governance-expiry-recovery").await;
    let first = qualify(&lab, "expiry-first").await;
    let second = qualify(&lab, "expiry-second").await;
    let held = lab.submit("held-coding-task", "held-coding-agent").await;
    let pause = change(
        "recovery-original-hold",
        GovernanceMutation::Pause {
            scope: ControlScope::AllTasks,
            reason: "retain hold across expiry and restart".into(),
        },
    );
    let original_hold = request(&lab, pause.clone()).await;
    let GovernanceResult::Hold(hold) = value(&original_hold) else {
        panic!("original hold")
    };
    let start = clock.load(Ordering::SeqCst);
    let override_op = change(
        "recovery-original-override",
        GovernanceMutation::Override {
            expected_revision: second.revision.clone(),
            configuration_revision: first.revision.clone(),
            expires_at_ms: start + 100,
            reason: "bounded temporary qualified policy".into(),
        },
    );
    let original_override = request(&lab, override_op.clone()).await;
    let selected = state(&lab).await;
    assert_eq!(selected.selected_revision, first.revision);
    assert_eq!(
        selected.temporary_override.as_ref().unwrap().prior_revision,
        second.revision
    );
    let early_op = change(
        "recovery-early-expiry",
        GovernanceMutation::ReconcileExpiry {
            expected_operation_id: "recovery-original-override".into(),
        },
    );
    let early = request(&lab, early_op.clone()).await;
    assert_eq!(refused(early.clone()), "override_not_expired");
    let proxy = ControlProxy::new(&lab, None).await;
    let worker = lab.start_worker("expiry-worker", "inc-1", &proxy.url);
    let (_, first_poll) = proxy.next_poll(0).await;
    assert!(
        matches!(first_poll.outcome, Outcome::Ok { data: Data::Commands(ref commands) } if commands.is_empty())
    );
    assert!(lab.get(&held.id).await.attempt.is_none());
    let parked = proxy.park_poll().await;
    assert!(
        matches!(&parked.command, Operation::AwaitCommands { worker_id, incarnation, .. }
        if worker_id == "expiry-worker" && incarnation == "inc-1")
    );
    clock.store(start + 100, Ordering::SeqCst);
    owner.close();
    owner_released(&lab).await;
    assert!(
        lab.workers[worker].child.try_wait().unwrap().is_none(),
        "same real worker remains live across Controller owner restart"
    );
    let mut reopened = Owner::open(&lab, clock.clone());
    let expiry_op = change(
        "recovery-exact-expiry",
        GovernanceMutation::ReconcileExpiry {
            expected_operation_id: "recovery-original-override".into(),
        },
    );
    let expired = request(&lab, expiry_op.clone()).await;
    assert_eq!(
        value(&expired),
        GovernanceResult::State(GovernanceState {
            selected_revision: second.revision.clone(),
            temporary_override: None
        })
    );
    replay(&lab, &early_op, &early).await;
    replay(&lab, &override_op, &original_override).await;
    replay(&lab, &pause, &original_hold).await;
    let observed = proxy.pending_count();
    proxy.hold_polls.store(false, Ordering::SeqCst);
    let after_expiry_poll = proxy.next_pending_poll(observed, &parked).await;
    save(lab.root.join("governance-after-expiry-pending.json"), &after_expiry_poll);
    assert_eq!(refused(request(&lab, Operation::GetOperation {
        operation_id: parked.command.mutation_id().unwrap().into(),
    }).await), "not_found", "idle Pending has no canonical operation receipt");
    let pending = lab.get(&held.id).await;
    assert_eq!(pending.input, held.input);
    assert_eq!(pending.session_id, held.session_id);
    assert_eq!(pending.status, TaskStatus::Queued);
    assert!(pending.attempt.is_none());
    assert!(lab.starts.is_empty());
    let stale = request(
        &lab,
        change(
            "recovery-wrong-release",
            GovernanceMutation::ReleaseHold {
                scope: ControlScope::AllTasks,
                expected_operation_id: "wrong-hold".into(),
                reason: "wrong original".into(),
            },
        ),
    )
    .await;
    assert_eq!(refused(stale), "configuration_conflict");
    let parked_again = proxy.park_poll().await;
    let exact = change(
        "recovery-explicit-release",
        GovernanceMutation::ReleaseHold {
            scope: ControlScope::AllTasks,
            expected_operation_id: hold.operation_id,
            reason: "explicit recovery after policy restoration".into(),
        },
    );
    let released = request(&lab, exact.clone()).await;
    assert_eq!(
        value(&released),
        GovernanceResult::Released(ControlScope::AllTasks)
    );
    assert!(
        lab.get(&held.id).await.attempt.is_none(),
        "ReleaseHold itself is not admission"
    );
    assert_eq!(
        refused(
            request(
                &lab,
                Operation::GetOperation {
                    operation_id: parked_again.command.mutation_id().unwrap().into(),
                }
            )
            .await
        ),
        "not_found",
        "the held original admission has not been forwarded"
    );
    proxy.hold_polls.store(false, Ordering::SeqCst);
    let execution = lab.gate().await;
    let running = lab.get(&held.id).await;
    assert_eq!(running.input, held.input);
    assert_eq!(running.session_id, held.session_id);
    assert_eq!(running.attempt.as_ref().unwrap().worker_id, "expiry-worker");
    assert_eq!(
        running.attempt.as_ref().unwrap().worker_incarnation,
        "inc-1"
    );
    assert_eq!(lab.starts.len(), 1);
    save(
        lab.root.join("governance-original-command.json"),
        &original_command(&proxy, &running),
    );
    execution.release().await;
    let succeeded = lab.successful(&held.id).await;
    assert_eq!(succeeded.input, held.input);
    let final_usage = attempt_view(&lab, &held.id).await;
    // No outer continuation or inner-request count was reported by this peer.
    assert_eq!(final_usage.reported_model_requests, 0);
    assert_eq!(final_usage.input_tokens, None);
    assert_eq!(final_usage.output_tokens, None);
    replay(&lab, &exact, &released).await;
    replay(&lab, &expiry_op, &expired).await;
    lab.workers[worker].stop();
    drop(proxy);
    reopened.close();
    owner_released(&lab).await;
    let mut final_owner = Owner::open(&lab, clock);
    assert_eq!(lab.get(&held.id).await, succeeded);
    assert_eq!(attempt_view(&lab, &held.id).await, final_usage);
    assert_eq!(state(&lab).await.selected_revision, second.revision);
    replay(&lab, &pause, &original_hold).await;
    replay(&lab, &exact, &released).await;
    final_owner.close();
    owner_released(&lab).await;
    save(
        lab.root.join("case-complete.json"),
        &json!({"status":"PASS", "real_workers":1,
        "controller":"actual library owner and HTTP adapter with fully joined runtime restart",
        "policy_setup":"canonical synthetic native evidence; no provider",
        "executor":"controlled Codex peer", "provider_calls":0, "executor_starts":lab.starts}),
    );
}

// S5 CANCEL: a normal configuration promotion is legitimate while work is held,
// but it is not an emergency-hold release or an attempt-admission operation.
// This is a controlled Controller/HTTP case; it starts no worker or executor.
//= spec/impl/v2/implementation.md#emergency-control-is-independent-of-ordinary-policy
//= type=test
//# An ordinary configuration change MUST NOT lift a hold imposed by an emergency control.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn normal_configuration_promotion_preserves_exact_emergency_hold_across_restart() {
    use rusqlite::OptionalExtension;

    async fn hold_bytes(lab: &Lab, clock: &Arc<AtomicU64>, owner: &mut Owner) -> Option<Vec<u8>> {
        // The controller deliberately holds SQLite EXCLUSIVE for its lifetime.
        // Join all owned HTTP connections before observing exact committed bytes;
        // the snapshot reader is closed before reopening the same fixed-clock owner.
        owner.close();
        owner_released(lab).await;
        let db = rusqlite::Connection::open_with_flags(
            lab.root.join("tasks.sqlite"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        let retained = db.query_row(
            "SELECT CAST(body AS BLOB) FROM governance_holds WHERE scope='all'",
            [],
            |row| row.get(0),
        )
        .optional()
        .unwrap();
        drop(db);
        *owner = Owner::open(lab, clock.clone());
        retained
    }

    let (lab, clock, mut owner) = configured("governance-held-promotion").await;
    // qualify() records the original passing completion/definition shadows and
    // retires each synthetic policy subject. Do this before the global Pause.
    let first = qualify(&lab, "held-first").await;
    let second = qualify(&lab, "held-second").await;
    assert_eq!(state(&lab).await.selected_revision, second.revision);
    let submitted = lab.submit("promotion-held-task", "promotion-held-agent").await;
    let pause = change(
        "promotion-original-hold",
        GovernanceMutation::Pause {
            scope: ControlScope::AllTasks,
            reason: "configuration changes cannot release this hold".into(),
        },
    );
    let original_hold = request(&lab, pause.clone()).await;
    let GovernanceResult::Hold(hold) = value(&original_hold) else {
        panic!("exact original emergency hold required")
    };
    let original_bytes = hold_bytes(&lab, &clock, &mut owner).await.expect("durable hold exists before promotion");
    let worker = Worker {
        id: "held-promotion-controlled-worker".into(),
        incarnation: "inc-1".into(),
        coding_capacity: 1,
        native_capacity: 0,
    };
    assert!(matches!(
        request(&lab, Operation::RegisterWorker {
            operation_id: "held-promotion-register".into(), worker: worker.clone(),
        }).await.outcome,
        Outcome::Ok { data: Data::Worker(ref actual) } if actual == &worker
    ));
    let before_poll = Operation::AwaitCommands {
        operation_id: "held-promotion-before-poll".into(),
        worker_id: worker.id.clone(), incarnation: worker.incarnation.clone(),
        acknowledged: vec![],
    };
    let before_reply = request(&lab, before_poll.clone()).await;
    assert!(matches!(before_reply.outcome,
        Outcome::Ok { data: Data::Commands(ref commands) } if commands.is_empty()),
        "the first held admission records its ordinary deferral: {:?}", before_reply);
    let held_task = lab.get(&submitted.id).await;
    assert_eq!(held_task.input, submitted.input);
    assert_eq!(held_task.session_id, submitted.session_id);
    assert_eq!(held_task.status, TaskStatus::Queued);
    assert!(held_task.attempt.is_none());
    assert_eq!(held_task.blocked_reason.as_deref(), Some("governance_emergency_pause_all_tasks"));

    let evidence = vec![
        ShadowEvidenceRef { operation_id: "held-first-completion-shadow".into() },
        ShadowEvidenceRef { operation_id: "held-first-definition-shadow".into() },
    ];
    let promote = change("held-promotion-original", GovernanceMutation::Promote {
        expected_revision: second.revision.clone(),
        configuration_revision: first.revision.clone(),
        evidence: evidence.clone(), supersedes_override: None,
    });
    let promoted = request(&lab, promote.clone()).await;
    let selected = GovernanceState {
        selected_revision: first.revision.clone(), temporary_override: None,
    };
    assert_eq!(value(&promoted), GovernanceResult::State(selected.clone()));
    assert_eq!(state(&lab).await, selected);
    // These live checks precede every post-promotion shutdown: a reopen cannot
    // repair a lost hold and hide an admission regression in the original owner.
    assert_eq!(lab.get(&submitted.id).await, held_task);
    let decision_id = DecisionRef {
        operation_id: "held-promotion-original".into(),
        point: DecisionPoint::RuleAdministration,
    };
    let audit_op = Operation::QueryGovernance {
        query: GovernanceQuery::Decision { decision: decision_id.clone() },
    };
    let audit = request(&lab, audit_op.clone()).await;
    let GovernanceResult::Decision(decision) = value(&audit) else {
        panic!("canonical promotion decision required")
    };
    assert_eq!(decision.id, decision_id);
    assert_eq!(decision.actual_outcome, ActualDecisionOutcome::Applied);
    assert_eq!(decision.controller_revision,
        promoted.commit.as_ref().unwrap().controller_revision);
    assert_eq!(decision.detail, DecisionDetail::RuleAdministration(
        RuleAdministrationDecision::Promoted {
            prior_revision: second.revision.clone(),
            selected_revision: first.revision.clone(), evidence,
        },
    ));
    replay(&lab, &promote, &promoted).await;
    replay(&lab, &pause, &original_hold).await;
    replay(&lab, &before_poll, &before_reply).await;

    // Stable held admission legitimately has Pending and no commit. It must not
    // turn a successful policy promotion into a Run or a fabricated receipt.
    let held_poll = Operation::AwaitCommands {
        operation_id: "held-promotion-after-poll".into(),
        worker_id: worker.id, incarnation: worker.incarnation,
        acknowledged: vec![],
    };
    let pending = request(&lab, held_poll.clone()).await;
    assert_eq!(pending.outcome, Outcome::Pending {
        operation_id: "held-promotion-after-poll".into(),
    });
    assert!(pending.commit.is_none());
    assert_eq!(refused(request(&lab, Operation::GetOperation {
        operation_id: "held-promotion-after-poll".into(),
    }).await), "not_found");
    assert_eq!(lab.get(&submitted.id).await, held_task);
    assert_eq!(hold_bytes(&lab, &clock, &mut owner).await, Some(original_bytes.clone()));
    assert!(lab.workers.is_empty() && lab.starts.is_empty());

    owner.close();
    owner_released(&lab).await;
    let mut reopened = Owner::open(&lab, clock.clone());
    assert_eq!(state(&lab).await, selected);
    assert_eq!(hold_bytes(&lab, &clock, &mut reopened).await, Some(original_bytes.clone()));
    assert_eq!(lab.get(&submitted.id).await, held_task);
    assert_eq!(request(&lab, audit_op.clone()).await.outcome, audit.outcome);
    replay(&lab, &promote, &promoted).await;
    replay(&lab, &pause, &original_hold).await;
    assert_eq!(request(&lab, held_poll).await.outcome, pending.outcome);
    let wrong_release = request(&lab, change("held-promotion-wrong-release",
        GovernanceMutation::ReleaseHold {
            scope: ControlScope::AllTasks, expected_operation_id: "wrong-hold".into(),
            reason: "does not identify the original hold".into(),
        })).await;
    assert_eq!(refused(wrong_release), "configuration_conflict");
    assert_eq!(lab.get(&submitted.id).await, held_task);
    assert_eq!(hold_bytes(&lab, &clock, &mut reopened).await, Some(original_bytes.clone()));
    let release = change("held-promotion-exact-release", GovernanceMutation::ReleaseHold {
        scope: ControlScope::AllTasks, expected_operation_id: hold.operation_id.clone(),
        reason: "explicit recovery independent of the policy update".into(),
    });
    let released = request(&lab, release.clone()).await;
    assert_eq!(value(&released), GovernanceResult::Released(ControlScope::AllTasks));
    let after_release = lab.get(&submitted.id).await;
    assert_eq!(after_release.input, held_task.input);
    assert_eq!(after_release.session_id, held_task.session_id);
    assert_eq!(after_release.status, TaskStatus::Queued);
    assert!(after_release.attempt.is_none(), "release itself is not admission");
    assert_eq!(state(&lab).await, selected);
    assert_eq!(hold_bytes(&lab, &clock, &mut reopened).await, None);
    replay(&lab, &release, &released).await;
    // Original successful Pause replay is historical and cannot recreate a hold.
    replay(&lab, &pause, &original_hold).await;
    assert_eq!(hold_bytes(&lab, &clock, &mut reopened).await, None);
    assert!(lab.workers.is_empty() && lab.starts.is_empty());
    reopened.close();
    owner_released(&lab).await;
    let mut final_owner = Owner::open(&lab, clock.clone());
    assert_eq!(state(&lab).await, selected);
    assert_eq!(hold_bytes(&lab, &clock, &mut final_owner).await, None);
    assert_eq!(lab.get(&submitted.id).await, after_release);
    replay(&lab, &promote, &promoted).await;
    replay(&lab, &release, &released).await;
    final_owner.close();
    owner_released(&lab).await;
    save(lab.root.join("held-promotion-proof.json"), &json!({
        "status": "PASS", "scope": "controlled Controller/HTTP; no worker or executor process",
        "held_task": held_task, "after_release": after_release,
        "hold": hold, "hold_bytes": original_bytes,
        "promotion_operation": promote, "promotion_reply": promoted,
        "promotion_audit": decision, "before_poll": before_reply,
        "after_poll": pending, "release_operation": release,
        "release_reply": released, "worker_processes": 0, "executor_starts": 0,
    }));
}
