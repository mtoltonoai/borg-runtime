//! Actual controller/worker processes with a controlled stdio executor, not a
//! provider or pinned Codex binary qualification. Each case preserves the
//! original operation/attempt identities across the boundary it observes.
use super::*;
use borg_runtime::{
    collaboration::{AttemptView, Query, QueryResult},
    worker::process::component,
};

const PEER: &str = r#"#!/usr/bin/python3
import json, os, socket, subprocess, sys
from pathlib import Path
config = json.loads(Path(__file__).with_suffix('.json').read_text())
root = Path(__file__).parent
thread = 'evidence-thread-' + str(os.getpid())
turn = 'evidence-turn-' + str(os.getpid())
def log(direction, message):
    with (root / 'executor-protocol.jsonl').open('a') as out:
        out.write(json.dumps({'pid':os.getpid(), 'direction':direction, 'message':message})+'\n')
def send(message):
    log('out', message)
    print(json.dumps(message), flush=True)
for line in sys.stdin:
    request = json.loads(line)
    log('in', request)
    method = request.get('method')
    if method == 'initialize':
        send({'id':request['id'],'result':{'userAgent':'controlled-supplied-evidence'}})
    elif method == 'command/exec':
        assert request['params']['command'] == ['/usr/bin/true']
        result = subprocess.run(['/usr/bin/true'], check=False)
        send({'id':request['id'],'result':{'exitCode':result.returncode,'stdout':'','stderr':''}})
    elif method == 'thread/start':
        assert request['params']['model'] == 'gpt-6-astra'
        assert request['params']['config']['model_reasoning_effort'] == 'max'
        history = root / 'executor-history'
        history.mkdir(exist_ok=True)
        (history / (thread+'.json')).write_text(json.dumps({'thread_id':thread,'available':True}))
        send({'id':request['id'],'result':{'thread':{'id':thread},'model':'gpt-6-astra','reasoningEffort':'max'}})
    elif method in ('thread/resume','thread/read'):
        send({'id':request['id'],'error':{'code':-32004,'message':'saved executor history unavailable'}})
    elif method == 'turn/start':
        assert request['params']['threadId'] == thread
        assert request['params']['model'] == 'gpt-6-astra' and request['params']['effort'] == 'max'
        observation = {'pid':os.getpid(),'cwd':os.getcwd(),'before':Path('answer.txt').read_text(),'thread':thread,'turn':turn,'mode':config['mode']}
        with socket.create_connection(('127.0.0.1', config['port']), timeout=20) as gate:
            gate.sendall((json.dumps(observation)+'\n').encode())
            assert gate.recv(1) == b'G'
        if config['mode'] == 'lost-start':
            # The original start reached the executor, but its response is lost.
            # No terminal event or invented turn ACK reaches the worker.
            sys.exit(4)
        Path('answer.txt').write_text('after\n')
        ack = {'id':request['id'],'result':{'turn':{'id':turn,'status':'inProgress'}}}
        completed = {'method':'turn/completed','params':{'threadId':thread,'turn':{'id':turn,'status':'completed'}}}
        message = {'method':'item/completed','params':{'item':{'type':'agentMessage','text':'controlled successful executor outcome'}}}
        if config['mode'] == 'completion-before-ack':
            send(message)
            send(completed)
            send(ack)
        else:
            send(ack)
            send(message)
            send(completed)
    elif method == 'turn/interrupt':
        send({'id':request['id'],'result':{}})
    elif 'id' in request:
        send({'id':request['id'],'error':{'code':-32601,'message':'unsupported controlled request'}})
"#;

fn peer(lab: &Lab, mode: &str) {
    fs::write(&lab.fixture, PEER).unwrap();
    save(
        lab.fixture.with_extension("json"),
        &json!({"port":lab.gate.local_addr().unwrap().port(),"mode":mode}),
    );
}
fn protocol(lab: &Lab) -> Vec<Value> {
    let bytes = fs::read(lab.root.join("executor-protocol.jsonl")).unwrap();
    assert!(bytes.len() <= lab.limits.transport.body_bytes);
    std::str::from_utf8(&bytes)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
fn incoming_count(log: &[Value], method: &str) -> usize {
    log.iter()
        .filter(|row| row["direction"] == "in" && row["message"]["method"] == method)
        .count()
}
fn state(lab: &Lab, worker: &str) -> PathBuf {
    lab.root
        .join(worker)
        .join(".borg-worker")
        .join(component(worker))
}
fn thread_record(lab: &Lab, worker: &str, attempt: &Attempt) -> PathBuf {
    state(lab, worker)
        .join("codex")
        .join(component(&attempt.id))
        .join("borg-thread.json")
}
async fn attempts(lab: &Lab, task_id: &str) -> Vec<AttemptView> {
    match lab
        .client
        .call(Operation::QueryCollaboration {
            query: Query::TaskAttempts {
                task_id: task_id.into(),
                after: None,
                limit: 10,
            },
        })
        .await
        .unwrap()
        .outcome
    {
        Outcome::Ok {
            data: Data::Collaboration(QueryResult::TaskAttempts(page)),
        } => {
            assert!(page.next_cursor.is_none());
            page.items
        }
        other => panic!("expected canonical attempts: {other:?}"),
    }
}
async fn original_report(
    lab: &Lab,
    attempt: &Attempt,
    suffix: &str,
) -> (Report, Outcome, MutationCommit) {
    let operation_id = format!("{}-{suffix}", attempt.id);
    let reply = lab
        .client
        .call(Operation::GetOperation {
            operation_id: operation_id.clone(),
        })
        .await
        .unwrap();
    let commit = reply.commit.clone().expect("original report commit");
    assert_eq!(commit.operation_id, operation_id);
    let Outcome::Ok {
        data: Data::Operation(record),
    } = reply.outcome
    else {
        panic!("original report absent: {reply:?}");
    };
    assert_eq!(record.kind, "application_mutation");
    assert_eq!(record.state, "settled");
    let Operation::ObserveWorker { report } = serde_json::from_value(record.intended).unwrap()
    else {
        panic!("original operation is not a worker report");
    };
    assert_eq!(report.report_id, operation_id);
    assert_eq!(report.task_id, attempt.task_id);
    assert_eq!(report.attempt_id, attempt.id);
    assert_eq!(report.generation, attempt.generation);
    assert_eq!(report.worker_id, attempt.worker_id);
    assert_eq!(report.incarnation, attempt.worker_incarnation);
    let outcome = serde_json::from_value(record.outcome.unwrap()).unwrap();
    (report, outcome, commit)
}
fn stopped_association(lab: &mut Lab, attempt: &Attempt, observed: &Value) {
    // This is the original canonical row, read only AFTER its sole owner stops.
    // It deliberately does not race a live EXCLUSIVE SQLite connection.
    for worker in &mut lab.workers {
        worker.stop();
    }
    lab.controller.as_mut().unwrap().stop();
    let db = rusqlite::Connection::open_with_flags(
        lab.root.join("tasks.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let (task_id, body, thread, turn): (String, String, String, String) = db
        .query_row(
            "SELECT task_id,body,thread_id,turn_id FROM attempts WHERE id=?1",
            [&attempt.id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();
    assert!(body.len() <= lab.limits.transport.body_bytes);
    let stored: Attempt = serde_json::from_str(&body).unwrap();
    assert_eq!(stored, *attempt);
    assert_eq!(task_id, attempt.task_id);
    assert_eq!(thread, observed["thread"].as_str().unwrap());
    assert_eq!(turn, observed["turn"].as_str().unwrap());
    save(
        lab.root
            .join(format!("canonical-association-{}.json", attempt.id)),
        &json!({"attempt":stored,"thread_id":thread,"turn_id":turn,"owner_stopped":true}),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
//= spec/impl/v2/implementation.md#the-supplied-executor-is-qualified-at-its-actual-boundary
//= type=test
//# A worker MUST record the association between its task attempt and the supplied executor's thread and turn identifiers.
async fn canonical_association_survives_completion_before_start_ack() {
    let mut lab = Lab::new("supplied-association-order").await;
    peer(&lab, "completion-before-ack");
    let submitted = lab
        .submit("association-order-submit", "association-order-agent")
        .await;
    let origin = lab.origin.clone();
    lab.start_worker("association-worker", "one", &origin);
    let gate = lab.gate().await;
    let observed = gate.observed.clone();
    gate.release().await;
    let accepted = lab.successful(&submitted.id).await;
    let attempt = accepted.attempt.clone().unwrap();
    let log = protocol(&lab);
    let request_id = log
        .iter()
        .find(|r| r["direction"] == "in" && r["message"]["method"] == "turn/start")
        .unwrap()["message"]["id"]
        .clone();
    let completion = log
        .iter()
        .position(|r| r["direction"] == "out" && r["message"]["method"] == "turn/completed")
        .unwrap();
    let ack = log
        .iter()
        .position(|r| r["direction"] == "out" && r["message"]["id"] == request_id)
        .unwrap();
    assert!(
        completion < ack,
        "the real peer must produce the ordering discriminator"
    );
    let (association, original, association_commit) =
        original_report(&lab, &attempt, "completed-association").await;
    assert_eq!(
        association.observation,
        Observation::Started {
            thread_id: Some(observed["thread"].as_str().unwrap().into()),
            turn_id: Some(observed["turn"].as_str().unwrap().into()),
        }
    );
    assert!(matches!(
        original,
        Outcome::Ok {
            data: Data::Task(_)
        }
    ));
    let (_, _, outcome_commit) = original_report(&lab, &attempt, "completed-executor").await;
    assert!(association_commit.controller_revision < outcome_commit.controller_revision);
    let before = attempts(&lab, &submitted.id).await;
    assert_eq!(before.len(), 1);
    assert!(before[0].executor_outcome.as_ref().unwrap().success);
    stopped_association(&mut lab, &attempt, &observed);
    lab.restart_controller().await;
    assert_eq!(attempts(&lab, &submitted.id).await, before);
    let (reopened, _, stamp) = original_report(&lab, &attempt, "completed-association").await;
    assert_eq!(reopened, association);
    assert_eq!(stamp, association_commit);
    assert_eq!(lab.get(&submitted.id).await.result, accepted.result);
    lab.close();
}

/// A single controlled transport fault, matching the existing duplicate-launch
/// fixture: forward real requests/receipts, then append the exact old command
/// to a genuine independent launch for a fresh process with the SAME worker
/// incarnation. Idle Await returns Pending, never a fabricated empty receipt.
/// The next distinct Await proves that process consumed the injected batch.
struct SameCommandReplay {
    url: String,
    state: Arc<Mutex<ReplayState>>,
    events: mpsc::Receiver<&'static str>,
    delivery_gate: Arc<tokio::sync::Notify>,
    join: tokio::task::JoinHandle<()>,
}
#[derive(Default)]
struct ReplayState {
    command: Option<WorkerCommand>,
    armed: bool,
    independent_task: Option<String>,
    fresh_poll_seen: bool,
    injected_operation: Option<String>,
    consumed: bool,
}
impl SameCommandReplay {
    async fn new(lab: &Lab) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(ReplayState::default()));
        let shared = state.clone();
        let delivery_gate = Arc::new(tokio::sync::Notify::new());
        let gate = delivery_gate.clone();
        let (send, events) = mpsc::channel(4);
        let upstream = lab.origin.clone();
        let evidence = lab.root.clone();
        let body_bound = lab.limits.transport.body_bytes;
        let join = tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(28))
                .build()
                .unwrap();
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        let (client, upstream, state, send, evidence, gate) = (client.clone(), upstream.clone(), shared.clone(), send.clone(), evidence.clone(), gate.clone());
                        connections.spawn(async move {
                            let service = service_fn(move |request: HttpRequest<Incoming>| {
                                let (client, upstream, state, send, evidence, gate) = (client.clone(), upstream.clone(), state.clone(), send.clone(), evidence.clone(), gate.clone());
                                async move {
                                    assert_eq!(request.uri().path(), "/api/operations");
                                    let bytes = request.into_body().collect().await.map_err(io::Error::other)?.to_bytes();
                                    assert!(bytes.len() <= body_bound);
                                    let original: Request = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                                    // Identify arrival after the old process stopped, not an
                                    // old long poll whose response happens to finish later.
                                    let (fresh_await, ready) = {
                                        let mut state = state.lock().unwrap();
                                        let fresh = state.armed && matches!(original.command, Operation::AwaitCommands { .. });
                                        let ready = fresh && !state.fresh_poll_seen;
                                        if ready { state.fresh_poll_seen = true; }
                                        (fresh, ready)
                                    };
                                    if ready {
                                        save(evidence.join("same-command-fresh-poll.json"), &original);
                                        send.send("ready").await.map_err(io::Error::other)?;
                                        gate.notified().await;
                                    }
                                    let consumed = {
                                        let mut state = state.lock().unwrap();
                                        let yes = matches!(original.command, Operation::AwaitCommands { .. })
                                            && state.injected_operation.as_deref().is_some_and(|old| Some(old) != original.command.mutation_id())
                                            && !state.consumed;
                                        if yes { state.consumed = true; }
                                        yes
                                    };
                                    if consumed {
                                        save(evidence.join("same-command-consumed.json"), &original);
                                        send.send("consumed").await.map_err(io::Error::other)?;
                                    }
                                    let response = client.post(format!("{upstream}/api/operations")).header("content-type", "application/json").body(bytes.to_vec()).send().await.map_err(io::Error::other)?;
                                    assert!(response.status().is_success());
                                    let bytes = response.bytes().await.map_err(io::Error::other)?;
                                    assert!(bytes.len() <= body_bound);
                                    let mut reply: Reply = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                                    let genuine = reply.clone();
                                    let injected = {
                                        let mut state = state.lock().unwrap();
                                        if let Outcome::Ok { data: Data::Commands(commands) } = &mut reply.outcome {
                                            if state.command.is_none() {
                                                state.command = commands.iter().find(|c| matches!(c, WorkerCommand::RunAttempt { .. })).cloned();
                                            }
                                            if fresh_await && state.armed && state.injected_operation.is_none() {
                                                assert_eq!(commands.len(), 1, "one genuine independent delivery triggers the duplicate");
                                                assert!(matches!(&commands[0], WorkerCommand::RunAttempt {attempt,..}
                                                    if Some(&attempt.task_id) == state.independent_task.as_ref()),
                                                    "the actual controller must supply the independent task's launch");
                                                assert!(genuine.commit.is_some(), "only an actual committed batch may carry a duplicate");
                                                commands.push(state.command.clone().expect("captured real original launch"));
                                                state.injected_operation = Some(original.command.mutation_id().unwrap().into());
                                                true
                                            } else { false }
                                        } else { false }
                                    };
                                    if injected {
                                        save(evidence.join("same-command-injected.json"), &json!({"request":original,"actual_reply":genuine,"duplicate_reply":reply}));
                                        send.send("injected").await.map_err(io::Error::other)?;
                                    }
                                    Ok::<_, io::Error>(HttpResponse::new(Full::new(Bytes::from(serde_json::to_vec(&reply).unwrap()))))
                                }
                            });
                            let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Self {
            url,
            state,
            events,
            delivery_gate,
            join,
        }
    }
    async fn event(&mut self, expected: &str) {
        assert_eq!(
            tokio::time::timeout(TIMEOUT, self.events.recv())
                .await
                .expect("fresh worker did not process exact duplicate")
                .unwrap(),
            expected
        );
    }
}
impl Drop for SameCommandReplay {
    fn drop(&mut self) {
        self.join.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
//= spec/impl/v2/implementation.md#the-supplied-executor-is-qualified-at-its-actual-boundary
//= type=test
//# A lost start response MUST be reconciled before another executor turn is started for that command.
// Traceability scope: A fresh worker receives the same completed command after the lost initial start, retains its original association and creates no second thread/turn for that command. The supplied peer is controlled.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# Platform recovery of a supplied executor MUST NOT create another session or repeat an uncertain start merely to implement a provider retry.
async fn lost_initial_start_fresh_process_reconciles_same_command_without_second_start() {
    let mut lab = Lab::new("supplied-lost-initial-start").await;
    peer(&lab, "lost-start");
    let mut transport = SameCommandReplay::new(&lab).await;
    let submitted = lab
        .submit("lost-initial-submit", "lost-initial-agent")
        .await;
    let worker = lab.start_worker("lost-start-worker", "same-incarnation", &transport.url);
    let first_pid = lab.workers[worker].child.id();
    let gate = lab.gate().await;
    let observed = gate.observed.clone();
    gate.release().await;
    let failed = lab
        .until(&submitted.id, "lost initial response retained", |t| {
            t.status == TaskStatus::Failed
                && t.attempt.as_ref().is_some_and(|a| a.stopped && a.uncertain)
        })
        .await;
    lab.workers[worker].stop();
    let settled = lab.get(&submitted.id).await;
    let attempt = settled.attempt.clone().unwrap();
    assert_eq!(attempt.id, failed.attempt.unwrap().id);
    assert!(settled.result.is_none());
    let path = thread_record(&lab, "lost-start-worker", &attempt);
    let original_bytes = fs::read(&path).unwrap();
    let original: Value = serde_json::from_slice(&original_bytes).unwrap();
    assert_eq!(original["thread_id"], observed["thread"]);
    assert_eq!(original["turn_start_sent"], true);
    assert!(
        original.get("turn_id").is_none(),
        "lost response must not synthesize an ACK"
    );
    let captured = transport.state.lock().unwrap().command.clone().unwrap();
    let WorkerCommand::RunAttempt {
        command_id,
        attempt: launched,
        ..
    } = &captured
    else {
        unreachable!()
    };
    assert_eq!(launched.id, attempt.id);
    assert_eq!(launched.worker_incarnation, "same-incarnation");
    let command_path = state(&lab, "lost-start-worker")
        .join("commands")
        .join(format!("{}.json", component(command_id)));
    let receipt_bytes = fs::read(&command_path).unwrap();
    let receipt: Value = serde_json::from_slice(&receipt_bytes).unwrap();
    assert_eq!(receipt["command"], serde_json::to_value(&captured).unwrap());
    assert_eq!(receipt["completed"], true);
    // A fresh worker's idle poll is correctly Pending. Give it one unrelated
    // real task so the controller commits a real delivery before the transport
    // appends the old duplicate. This is not a retry of the unknown task.
    transport.state.lock().unwrap().armed = true;
    // Keep the first process's logs and closure evidence, even though the
    // logical worker/incarnation intentionally stays identical.
    let workspace = lab.root.join("lost-start-worker");
    let fixture = lab.fixture.clone();
    let process = lab.spawn(
        "lost-start-worker-fresh-process".into(),
        &[
            "worker",
            "--controller",
            &transport.url,
            "--id",
            "lost-start-worker",
            "--incarnation",
            "same-incarnation",
            "--workspace-root",
            workspace.to_str().unwrap(),
            "--codex",
            fixture.to_str().unwrap(),
            "--coding-capacity",
            "1",
            "--native-capacity",
            "0",
        ],
    );
    lab.workers.push(process);
    let fresh = lab.workers.len() - 1;
    assert_ne!(lab.workers[fresh].child.id(), first_pid);
    // Fresh recovery/registration has finished before the independent task
    // exists. An older in-flight Await cannot consume the injected copy.
    transport.event("ready").await;
    let independent = lab
        .submit(
            "lost-start-independent-submit",
            "lost-start-independent-agent",
        )
        .await;
    peer(&lab, "normal");
    transport.state.lock().unwrap().independent_task = Some(independent.id.clone());
    transport.delivery_gate.notify_one();
    transport.event("injected").await;
    transport.event("consumed").await;
    let independent_gate = lab.gate().await;
    let independent_observed = independent_gate.observed.clone();
    assert_ne!(independent_observed["cwd"], observed["cwd"]);
    assert_ne!(independent_observed["thread"], observed["thread"]);
    let independent_running = lab.get(&independent.id).await;
    assert_ne!(independent_running.attempt.as_ref().unwrap().id, attempt.id);
    assert_eq!(
        independent_running
            .attempt
            .as_ref()
            .unwrap()
            .worker_incarnation,
        "same-incarnation"
    );
    independent_gate.release().await;
    let independent_result = lab.successful(&independent.id).await;
    assert_eq!(independent_result.input, independent.input);
    lab.workers[fresh].stop();
    assert_eq!(fs::read(&path).unwrap(), original_bytes);
    assert_eq!(fs::read(&command_path).unwrap(), receipt_bytes);
    let log = protocol(&lab);
    assert_eq!(incoming_count(&log, "thread/start"), 2);
    assert_eq!(incoming_count(&log, "turn/start"), 2);
    for cwd in [&observed["cwd"], &independent_observed["cwd"]] {
        assert_eq!(
            log.iter()
                .filter(|r| r["direction"] == "in"
                    && r["message"]["method"] == "turn/start"
                    && &r["message"]["params"]["cwd"] == cwd)
                .count(),
            1,
            "each exact attempt has one physical turn start"
        );
    }
    assert_eq!(incoming_count(&log, "thread/resume"), 0);
    assert!(
        log.iter()
            .all(|r| r["message"]["method"] != "turn/completed"
                || r["message"]["params"]["threadId"] != observed["thread"])
    );
    let observed_outcomes = attempts(&lab, &submitted.id).await;
    assert_eq!(observed_outcomes.len(), 1);
    // The existing failure reporter records its adapter failure separately;
    // it must never turn the absent terminal response into executor success.
    let failure = observed_outcomes[0].executor_outcome.as_ref().unwrap();
    assert!(!failure.success);
    assert!(failure.summary.contains("turn start outcome uncertain"));
    let before_retry = lab.get(&submitted.id).await;
    assert_eq!(before_retry.attempt, settled.attempt);
    let retry = Operation::RetryTask {
        operation_id: "lost-initial-retry-refused".into(),
        task_id: submitted.id.clone(),
        expected_revision: before_retry.revision,
    };
    let refusal = lab.client.call(retry.clone()).await.unwrap();
    assert_eq!(refused(refusal.clone()), "reconciliation_required");
    assert_eq!(
        lab.client.call(retry).await.unwrap().outcome,
        refusal.outcome
    );
    lab.restart_controller().await;
    assert_eq!(lab.get(&submitted.id).await, before_retry);
    assert_eq!(
        lab.starts.len(),
        2,
        "one original unknown start and one independent start"
    );
    lab.close();
}

async fn failed_acceptance(lab: &mut Lab, agent: &str, command: Vec<String>) -> (Task, Value) {
    peer(lab, "normal");
    let mut input = lab.input(agent);
    input.acceptance_commands.push(command);
    let submitted = task(
        lab.client
            .call(Operation::SubmitTask {
                operation_id: format!("{agent}-submit"),
                input,
            })
            .await
            .unwrap(),
    );
    let origin = lab.origin.clone();
    lab.start_worker("outcome-worker", "one", &origin);
    let gate = lab.gate().await;
    let observed = gate.observed.clone();
    gate.release().await;
    let failed = lab
        .until(
            &submitted.id,
            "platform acceptance rejects actual executor success",
            |t| {
                t.status == TaskStatus::Failed
                    && t.blocked_reason.as_deref() == Some("acceptance_failed")
                    && t.attempt
                        .as_ref()
                        .is_some_and(|a| a.stopped && !a.uncertain)
            },
        )
        .await;
    assert_eq!(failed.input, submitted.input);
    assert!(failed.result.is_none());
    assert!(failed.selected_checkpoint.is_some());
    (failed, observed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
//= spec/impl/v2/implementation.md#the-supplied-executor-owns-its-inner-loop
//= type=test
//# The adapter MUST record the supplied executor's reported outcome separately from the platform's task acceptance result.
async fn supplied_success_remains_canonical_when_platform_acceptance_fails() {
    let mut lab = Lab::new("supplied-success-acceptance-failure").await;
    let (failed, observed) = failed_acceptance(
        &mut lab,
        "separate-outcome-agent",
        vec!["/usr/bin/false".into()],
    )
    .await;
    let attempt = failed.attempt.clone().unwrap();
    let before = attempts(&lab, &failed.id).await;
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].attempt, attempt);
    assert_eq!(before[0].executor_outcome.as_ref().unwrap().success, true);
    assert_eq!(
        before[0].executor_outcome.as_ref().unwrap().summary,
        "controlled successful executor outcome"
    );
    let (report, original, stamp) = original_report(&lab, &attempt, "completed-executor").await;
    assert!(
        matches!(&report.observation, Observation::ExecutorOutcome {success:true,summary} if summary == "controlled successful executor outcome")
    );
    let Outcome::Ok {
        data: Data::Task(at_outcome),
    } = original
    else {
        panic!("executor outcome did not commit")
    };
    assert!(at_outcome.result.is_none());
    assert_ne!(at_outcome.status, TaskStatus::Succeeded);
    stopped_association(&mut lab, &attempt, &observed);
    lab.restart_controller().await;
    let reopened = lab.get(&failed.id).await;
    assert_eq!(reopened.status, TaskStatus::Failed);
    assert_eq!(
        reopened.blocked_reason.as_deref(),
        Some("acceptance_failed")
    );
    assert_eq!(reopened.result, None);
    assert_eq!(reopened.selected_checkpoint, failed.selected_checkpoint);
    assert_eq!(attempts(&lab, &failed.id).await, before);
    let (replayed, _, old_stamp) = original_report(&lab, &attempt, "completed-executor").await;
    assert_eq!(replayed, report);
    assert_eq!(old_stamp, stamp);
    save(
        lab.root.join("separate-outcomes.json"),
        &json!({"executor":before,"platform":reopened,"original_receipt":old_stamp}),
    );
    lab.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
//= spec/impl/v2/implementation.md#the-supplied-executor-owns-its-inner-loop
//= type=test
//# An unavailable executor history MUST NOT be reported as a recovered conversation.
async fn unavailable_saved_history_creates_new_attempt_and_fresh_conversation() {
    let mut lab = Lab::new("supplied-history-unavailable").await;
    let allow = lab.root.join("allow-second-acceptance");
    let check = format!(
        "from pathlib import Path; raise SystemExit(0 if Path({:?}).exists() else 1)",
        allow.to_str().unwrap()
    );
    let (failed, observed) = failed_acceptance(
        &mut lab,
        "history-unavailable-agent",
        vec!["/usr/bin/python3".into(), "-c".into(), check],
    )
    .await;
    let old = failed.attempt.clone().unwrap();
    for worker in &mut lab.workers {
        worker.stop();
    }
    let old_association_path = thread_record(&lab, "outcome-worker", &old);
    let association = fs::read(&old_association_path).unwrap();
    let history = lab
        .root
        .join("executor-history")
        .join(format!("{}.json", observed["thread"].as_str().unwrap()));
    let history_bytes = fs::read(&history).unwrap();
    // Remove only the controlled executor's saved conversation after its
    // process stopped. The platform's original association/receipts survive.
    save(
        lab.root.join("unavailable-history.json"),
        &json!({"path":history,"original":serde_json::from_slice::<Value>(&history_bytes).unwrap(),"attempt":old}),
    );
    fs::remove_file(&history).unwrap();
    assert!(!history.exists());
    lab.restart_controller().await;
    let ready = lab.get(&failed.id).await;
    assert_eq!(ready.input, failed.input);
    assert_eq!(ready.selected_checkpoint, failed.selected_checkpoint);
    assert_eq!(ready.attempt.as_ref().unwrap().id, old.id);
    fs::write(&allow, "allow\n").unwrap();
    let retried = task(
        lab.client
            .call(Operation::RetryTask {
                operation_id: "history-unavailable-retry".into(),
                task_id: ready.id.clone(),
                expected_revision: ready.revision,
            })
            .await
            .unwrap(),
    );
    assert_eq!(retried.id, ready.id);
    assert_eq!(retried.session_id, ready.session_id);
    assert_eq!(retried.input, ready.input);
    let origin = lab.origin.clone();
    lab.start_worker("outcome-worker", "two", &origin);
    let gate = lab.gate().await;
    let next = gate.observed.clone();
    assert_eq!(
        next["before"], "after\n",
        "reconstruction uses the accepted Git checkpoint"
    );
    assert_ne!(next["thread"], observed["thread"]);
    assert_ne!(next["turn"], observed["turn"]);
    let assigned = lab.get(&ready.id).await.attempt.unwrap();
    assert_ne!(assigned.id, old.id);
    assert!(assigned.generation > old.generation);
    assert_eq!(assigned.session_id, old.session_id);
    assert_eq!(fs::read(&old_association_path).unwrap(), association);
    assert!(
        !history.exists(),
        "fresh start must not fabricate the unavailable old history"
    );
    let log = protocol(&lab);
    assert_eq!(incoming_count(&log, "thread/start"), 2);
    assert_eq!(incoming_count(&log, "turn/start"), 2);
    assert_eq!(incoming_count(&log, "thread/resume"), 0);
    assert_eq!(incoming_count(&log, "thread/read"), 0);
    gate.release().await;
    let accepted = lab.successful(&ready.id).await;
    let views = attempts(&lab, &ready.id).await;
    assert_eq!(views.len(), 2);
    assert!(
        views
            .iter()
            .all(|v| v.executor_outcome.as_ref().unwrap().success)
    );
    assert_eq!(accepted.result.as_ref().unwrap().attempt_id, assigned.id);
    let (association_report, _, _) = original_report(
        &lab,
        accepted.attempt.as_ref().unwrap(),
        "completed-association",
    )
    .await;
    assert_eq!(
        association_report.observation,
        Observation::Started {
            thread_id: Some(next["thread"].as_str().unwrap().into()),
            turn_id: Some(next["turn"].as_str().unwrap().into()),
        }
    );
    stopped_association(&mut lab, accepted.attempt.as_ref().unwrap(), &next);
    lab.restart_controller().await;
    assert_eq!(attempts(&lab, &ready.id).await, views);
    assert_eq!(lab.get(&ready.id).await.result, accepted.result);
    assert_eq!(lab.starts.len(), 2);
    lab.close();
}
