//! Joined catalogue/recovery evidence using real controller, workers and Git.
//! The stdio executor and the local SSH-command transport are controlled peers;
//! no provider, SSH server, repository grant or production metadata is used.
//! Parent/child admission remains covered separately by repository_catalogue.
use super::*;
use borg_runtime::repository_catalogue::{Entry, Registration};
use tokio::sync::oneshot;

const LOCAL_GIT_TRANSPORT: &str = r#"#!/usr/bin/python3
import json, os, shlex, sys
from pathlib import Path
root = Path(__file__).parent
args = sys.argv[1:]
if '-G' in args:
    sys.exit(1)
assert len(args) >= 2 and args[-2] == 'catalogue-fixture'
command = shlex.split(args[-1])
assert len(command) == 2 and command[0] in ('git-upload-pack','git-receive-pack')
path = Path(command[1]).resolve(strict=True)
allowed = {str((root / name).resolve(strict=True)) for name in ('r1-source.git','r1-checkpoint.git','r2-source.git','r2-checkpoint.git')}
assert str(path) in allowed
record = json.dumps({'service':command[0],'repository':str(path),'pid':os.getpid()})+'\n'
fd = os.open(root / 'git-pack-invocations.jsonl', os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
try:
    assert os.write(fd, record.encode()) == len(record.encode())
    os.fsync(fd)
finally:
    os.close(fd)
os.execv('/usr/bin/git', ['git',command[0].removeprefix('git-'),str(path)])
"#;

struct LostAck {
    request: Request,
    reply: Reply,
    release: Option<oneshot::Sender<()>>,
}
struct CatalogueFaults {
    url: String,
    events: mpsc::Receiver<LostAck>,
    state: Arc<Mutex<Faults>>,
    join: tokio::task::JoinHandle<()>,
}
#[derive(Default)]
struct Faults {
    registration_lost: bool,
    settlement_id: Option<String>,
    settlement_lookups: usize,
}
impl CatalogueFaults {
    async fn new(lab: &Lab) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(Faults::default()));
        let shared = state.clone();
        let (send, events) = mpsc::channel(2);
        let (settlement_released, _) = tokio::sync::watch::channel(false);
        let origin = lab.origin.clone();
        let evidence = lab.root.clone();
        let bound = lab.limits.transport.body_bytes;
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
                        let (client, origin, state, send, evidence, released) = (client.clone(), origin.clone(), shared.clone(), send.clone(), evidence.clone(), settlement_released.clone());
                        connections.spawn(async move {
                            let service = service_fn(move |request: HttpRequest<Incoming>| {
                                let (client, origin, state, send, evidence, released) = (client.clone(), origin.clone(), state.clone(), send.clone(), evidence.clone(), released.clone());
                                async move {
                                    assert_eq!(request.uri().path(), "/api/operations");
                                    let bytes = request.into_body().collect().await.map_err(io::Error::other)?.to_bytes();
                                    assert!(bytes.len() <= bound);
                                    let request: Request = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                                    let mut resumed = released.subscribe();
                                    let original_settlement = {
                                        let state=state.lock().unwrap();
                                        match &request.command {
                                            Operation::GetOperation {operation_id} => state.settlement_id.as_ref()==Some(operation_id),
                                            Operation::SettleOperation {settlement_id,..} => state.settlement_id.as_ref()==Some(settlement_id),
                                            _ => false,
                                        }
                                    };
                                    // A timed-out first request cannot reconcile around the
                                    // selected restart cut. Hold its exact retries/readbacks,
                                    // never unrelated work, until the test releases that cut.
                                    if original_settlement {
                                        while !*resumed.borrow_and_update() {resumed.changed().await.map_err(io::Error::other)?;}
                                    }
                                    let response = client.post(format!("{origin}/api/operations")).header("content-type","application/json")
                                        .body(bytes.to_vec()).send().await.map_err(io::Error::other)?;
                                    assert!(response.status().is_success());
                                    let bytes = response.bytes().await.map_err(io::Error::other)?;
                                    assert!(bytes.len() <= bound);
                                    let reply: Reply = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                                    let fault = {
                                        let mut state = state.lock().unwrap();
                                        if let Operation::GetOperation {operation_id} = &request.command {
                                            if state.settlement_id.as_ref() == Some(operation_id)
                                                && matches!(&reply.outcome, Outcome::Ok {data:Data::Operation(record)} if record.state == "settled") {
                                                state.settlement_lookups += 1;
                                                save(evidence.join("worker-original-settlement-readback.json"), &json!({"request":request,"reply":reply}));
                                            }
                                        }
                                        if reply.commit.is_some() && matches!(reply.outcome, Outcome::Ok {..}) {
                                            match &request.command {
                                                Operation::RegisterRepository {..} if !state.registration_lost => {
                                                    state.registration_lost = true; Some("registration")
                                                }
                                                Operation::SettleOperation {settlement_id,..} if state.settlement_id.is_none() => {
                                                    state.settlement_id = Some(settlement_id.clone()); Some("settlement")
                                                }
                                                _ => None,
                                            }
                                        } else { None }
                                    };
                                    if let Some(kind) = fault {
                                        save(evidence.join(format!("lost-{kind}-ack.json")), &json!({"request":request,"committed_reply":reply}));
                                        if kind == "settlement" {
                                            let (release, held) = oneshot::channel();
                                            send.send(LostAck {request,reply,release:Some(release)}).await.map_err(io::Error::other)?;
                                            held.await.map_err(io::Error::other)?;
                                            released.send_replace(true);
                                        } else {
                                            send.send(LostAck {request,reply,release:None}).await.map_err(io::Error::other)?;
                                        }
                                        return Err(io::Error::other("controlled lost ACK after actual commit"));
                                    }
                                    Ok::<_,io::Error>(HttpResponse::new(Full::new(bytes)))
                                }
                            });
                            let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream),service).await;
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Self {
            url,
            events,
            state,
            join,
        }
    }
    async fn event(&mut self) -> LostAck {
        tokio::time::timeout(TIMEOUT, self.events.recv())
            .await
            .expect("selected actual ACK was not reached")
            .unwrap()
    }
}
impl Drop for CatalogueFaults {
    fn drop(&mut self) {
        self.join.abort();
    }
}

fn entry(reply: Reply) -> Entry {
    match reply.outcome {
        Outcome::Ok {
            data: Data::RepositoryRegistration(entry),
        } => entry,
        other => panic!("catalogue entry: {other:?}"),
    }
}
fn registration(lab: &Lab, revision: &str) -> Registration {
    Registration {
        pull_request_target: None,
        repository_id: "joined-catalogue".into(),
        registration_revision: revision.into(),
        display_name: format!("Joined recovery {revision}"),
        source_endpoint: format!(
            "catalogue-fixture:{}",
            lab.root.join(format!("{revision}-source.git")).display()
        ),
        checkpoint_endpoint: format!(
            "catalogue-fixture:{}",
            lab.root
                .join(format!("{revision}-checkpoint.git"))
                .display()
        ),
        source_store_key: format!("{revision}-source.git"),
        checkpoint_store_key: format!("{revision}-checkpoint.git"),
    }
}
fn start_worker(lab: &mut Lab, worker: &str, origin: &str, transport: &Path) {
    let workspace = lab.root.join(worker);
    let fixture = lab.fixture.clone();
    let process = lab.spawn_env(
        format!("{worker}-catalogue"),
        &[
            "worker",
            "--controller",
            origin,
            "--id",
            worker,
            "--incarnation",
            "one",
            "--workspace-root",
            workspace.to_str().unwrap(),
            "--codex",
            fixture.to_str().unwrap(),
            "--coding-capacity",
            "1",
            "--native-capacity",
            "0",
        ],
        &[("BORG_WORKER_GIT_SSH_COMMAND", transport.to_str().unwrap())],
        "limits.json",
    );
    lab.workers.push(process);
}
fn caches(lab: &Lab, worker: &str, r1: &Registration) -> Vec<(PathBuf, Vec<u8>)> {
    let root = lab.root.join(worker).join("repositories");
    let entries = fs::read_dir(&root)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        entries.len() <= 4,
        "one accepted repository cache and its bounded lock/use records"
    );
    let mut found = Vec::new();
    for directory in entries {
        if !directory.file_type().unwrap().is_dir() {
            continue;
        }
        let marker = directory.path().join("identity.json");
        let bytes = fs::read(&marker).unwrap();
        assert!(bytes.len() <= lab.limits.worker.git_cache_identity_bytes);
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            value,
            json!({"version":2,"registration":r1.binding(),"source_endpoint":r1.source_endpoint,"checkpoint_endpoint":r1.checkpoint_endpoint})
        );
        assert_eq!(
            git(
                &directory.path().join("repository.git"),
                &["config", "--get", "remote.origin.url"]
            ),
            r1.source_endpoint
        );
        found.push((marker, bytes));
    }
    assert_eq!(found.len(), 1);
    found
}
fn pack_invocations(lab: &Lab) -> Vec<Value> {
    let bytes = fs::read(lab.root.join("git-pack-invocations.jsonl")).unwrap();
    assert!(bytes.len() <= lab.limits.transport.body_bytes);
    std::str::from_utf8(&bytes)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
async fn operation(lab: &Lab, id: &str) -> Reply {
    lab.client
        .call(Operation::GetOperation {
            operation_id: id.into(),
        })
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// Traceability scope: Two real coding workers, actual Git objects and immutable R1 cache/checkpoint identities; controlled transport ACK loss, not provider or SSH access qualification. Parent/child binding remains separate Controller coverage.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//= type=test
//# An accepted task MUST retain its selected repository registration revision.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//= type=test
//# An accepted repository association MUST distinguish the source endpoint from the checkpoint destination.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//= type=test
//# A catalogue revision or enable-state change MUST NOT change an accepted task's repository association.
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# An exact retry of a committed mutating request MUST return its recorded outcome.
async fn accepted_r1_survives_catalogue_change_and_lost_registration_and_settlement_acks() {
    let mut lab = Lab::new("catalogue-r1-recovery").await;
    // Instrument only this materialized controlled peer. Count process entries
    // independently of gate accepts, including an unexpected second process
    // that never reaches (or whose gate is never accepted by) the test.
    let peer = fs::read_to_string(&lab.fixture).unwrap();
    let anchor = "config = json.loads(Path(__file__).with_suffix('.json').read_text())";
    assert_eq!(peer.matches(anchor).count(), 1);
    let logged = format!(
        "{anchor}\n{}",
        r#"entry = (json.dumps({'pid': os.getpid(), 'cwd': os.getcwd()})+'\n').encode()
entry_fd = os.open(Path(__file__).parent / 'catalogue-executor-entries.jsonl', os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
try:
    assert os.write(entry_fd, entry) == len(entry)
    os.fsync(entry_fd)
finally:
    os.close(entry_fd)"#
    );
    fs::write(&lab.fixture, peer.replacen(anchor, &logged, 1)).unwrap();
    for revision in ["r1", "r2"] {
        let source = lab.root.join(format!("{revision}-source.git"));
        let checkpoint = lab.root.join(format!("{revision}-checkpoint.git"));
        git(
            &lab.root,
            &[
                "clone",
                "--bare",
                "--quiet",
                lab.root.join("seed").to_str().unwrap(),
                source.to_str().unwrap(),
            ],
        );
        git(
            &lab.root,
            &["init", "--bare", "--quiet", checkpoint.to_str().unwrap()],
        );
    }
    let transport = lab.root.join("local-git-transport.py");
    fs::write(&transport, LOCAL_GIT_TRANSPORT).unwrap();
    fs::set_permissions(&transport, fs::Permissions::from_mode(0o700)).unwrap();
    let r1 = registration(&lab, "r1");
    let r2 = registration(&lab, "r2");
    let source_refs = git(
        &lab.root,
        &[
            "--git-dir",
            lab.root.join("r1-source.git").to_str().unwrap(),
            "for-each-ref",
            "--format=%(refname) %(objectname)",
        ],
    );
    lab.repository.registration = Some(r1.binding());
    lab.repository.url = r1.source_endpoint.clone();
    lab.repository.checkpoint_remote = r1.checkpoint_endpoint.clone();
    let mut faults = CatalogueFaults::new(&lab).await;
    let register = Operation::RegisterRepository {
        operation_id: "joined-register-r1".into(),
        registration: r1.clone(),
    };
    let fault_client = ApiClient::new(&faults.url, lab.limits.clone()).unwrap();
    assert!(fault_client.call(register.clone()).await.is_err());
    let lost_registration = faults.event().await;
    assert_eq!(
        serde_json::to_value(&lost_registration.request.command).unwrap(),
        serde_json::to_value(&register).unwrap()
    );
    assert!(lost_registration.release.is_none());
    let original_lookup = operation(&lab, "joined-register-r1").await;
    assert_eq!(original_lookup.commit, lost_registration.reply.commit);
    let Outcome::Ok {
        data: Data::Operation(original),
    } = &original_lookup.outcome
    else {
        panic!("registration receipt absent")
    };
    assert_eq!(original.intended, serde_json::to_value(&register).unwrap());
    assert_eq!(
        original.outcome.as_ref(),
        Some(&serde_json::to_value(&lost_registration.reply.outcome).unwrap())
    );
    let reconciled = lab.client.call(register.clone()).await.unwrap();
    assert_eq!(reconciled.outcome, lost_registration.reply.outcome);
    assert_eq!(reconciled.commit, lost_registration.reply.commit);
    assert_eq!(entry(reconciled).generation, 1);
    assert!(
        !lab.root.join("git-pack-invocations.jsonl").exists(),
        "registration must not acquire material"
    );
    let first = lab.submit("joined-first", "joined-agent-one").await;
    let second = lab.submit("joined-second", "joined-agent-two").await;
    for accepted in [&first, &second] {
        assert_eq!(accepted.input.repository.as_ref(), Some(&lab.repository));
    }
    let revised = entry(
        lab.client
            .call(Operation::ReviseRepository {
                operation_id: "joined-revise-r2".into(),
                expected_generation: 1,
                registration: r2.clone(),
            })
            .await
            .unwrap(),
    );
    assert_eq!(revised.current_revision, "r2");
    assert_eq!(revised.generation, 2);
    let disabled = entry(
        lab.client
            .call(Operation::SetRepositoryEnabled {
                operation_id: "joined-disable-r2".into(),
                repository_id: r1.repository_id.clone(),
                expected_generation: 2,
                enabled_for_new_tasks: false,
            })
            .await
            .unwrap(),
    );
    assert_eq!(disabled.generation, 3);
    assert!(!disabled.enabled_for_new_tasks);
    let mut current_input = lab.input("joined-refused-current");
    current_input.repository.as_mut().unwrap().registration = Some(r2.binding());
    assert_eq!(
        refused(
            lab.client
                .call(Operation::SubmitTask {
                    operation_id: "joined-disabled-admission".into(),
                    input: current_input
                })
                .await
                .unwrap()
        ),
        "repository_disabled"
    );
    assert!(
        !lab.root.join("git-pack-invocations.jsonl").exists(),
        "catalogue changes precede worker acquisition"
    );
    start_worker(&mut lab, "catalogue-worker-one", &faults.url, &transport);
    start_worker(&mut lab, "catalogue-worker-two", &faults.url, &transport);
    let one = lab.gate().await;
    let two = lab.gate().await;
    assert_eq!(one.observed["before"], "before\n");
    assert_eq!(two.observed["before"], "before\n");
    let assigned_one = lab.get(&first.id).await;
    let assigned_two = lab.get(&second.id).await;
    assert_ne!(
        assigned_one.attempt.as_ref().unwrap().worker_id,
        assigned_two.attempt.as_ref().unwrap().worker_id
    );
    let mut identities = caches(&lab, "catalogue-worker-one", &r1);
    identities.extend(caches(&lab, "catalogue-worker-two", &r1));
    one.release().await;
    two.release().await;
    let mut lost_settlement = faults.event().await;
    let Operation::SettleOperation {
        operation_id,
        settlement_id,
        task_id,
        checkpoint: Some(checkpoint),
        ..
    } = &lost_settlement.request.command
    else {
        panic!("expected actual checkpoint settlement")
    };
    let (operation_id, settlement_id, task_id, checkpoint) = (
        operation_id.clone(),
        settlement_id.clone(),
        task_id.clone(),
        checkpoint.clone(),
    );
    assert!(task_id == first.id || task_id == second.id);
    assert_eq!(checkpoint.registration, r1.binding());
    assert_eq!(checkpoint.repository, r1.checkpoint_endpoint);
    assert_eq!(
        git(
            &lab.root,
            &[
                "--git-dir",
                lab.root.join("r1-checkpoint.git").to_str().unwrap(),
                "rev-parse",
                &checkpoint.remote_ref
            ]
        ),
        checkpoint.commit
    );
    // The worker still awaits this exact response while the controller restarts.
    lab.restart_controller().await;
    let durable = operation(&lab, &operation_id).await;
    assert_eq!(durable.outcome, lost_settlement.reply.outcome);
    let receipt = operation(&lab, &settlement_id).await;
    assert_eq!(receipt.commit, lost_settlement.reply.commit);
    let Outcome::Ok {
        data: Data::Operation(receipt_record),
    } = &receipt.outcome
    else {
        panic!("original settlement receipt absent")
    };
    assert_eq!(
        receipt_record.intended,
        serde_json::to_value(&lost_settlement.request.command).unwrap()
    );
    assert_eq!(
        receipt_record.outcome.as_ref(),
        Some(&serde_json::to_value(&lost_settlement.reply.outcome).unwrap())
    );
    assert_eq!(
        faults.state.lock().unwrap().settlement_lookups,
        0,
        "original worker reconciliation stays behind the actual restart cut"
    );
    lost_settlement.release.take().unwrap().send(()).unwrap();
    // successful() assumes a local checkpoint URL; this case intentionally
    // retains separate SSH-shaped source/destination identities and checks the
    // allowlisted local destination explicitly instead.
    let mut accepted = Vec::new();
    for original in [&first, &second] {
        let final_task = lab
            .until(&original.id, "original R1 accepted result", |t| {
                t.status == TaskStatus::Succeeded && t.attempt.as_ref().is_some_and(|a| a.stopped)
            })
            .await;
        assert_eq!(final_task.input, original.input);
        let checkpoint = final_task.selected_checkpoint.as_ref().unwrap();
        assert_eq!(checkpoint.registration, r1.binding());
        assert_eq!(checkpoint.repository, r1.checkpoint_endpoint);
        assert_eq!(
            git(
                &lab.root,
                &[
                    "--git-dir",
                    lab.root.join("r1-checkpoint.git").to_str().unwrap(),
                    "rev-parse",
                    &checkpoint.remote_ref
                ]
            ),
            checkpoint.commit
        );
        assert_eq!(
            final_task.result.as_ref().unwrap().checkpoint.as_ref(),
            Some(checkpoint)
        );
        assert_eq!(
            git(
                &lab.root,
                &[
                    "--git-dir",
                    lab.root.join("r1-checkpoint.git").to_str().unwrap(),
                    "show",
                    &format!("{}:answer.txt", checkpoint.commit)
                ]
            ),
            "after"
        );
        accepted.push(final_task);
    }
    assert!(
        faults.state.lock().unwrap().settlement_lookups > 0,
        "actual worker must reconcile its original settlement receipt"
    );
    for worker in &mut lab.workers {
        worker.stop();
    }
    for (path, bytes) in &identities {
        assert_eq!(&fs::read(path).unwrap(), bytes);
    }
    let invocations = pack_invocations(&lab);
    let invocation_bytes = fs::read(lab.root.join("git-pack-invocations.jsonl")).unwrap();
    let pushes = invocations
        .iter()
        .filter(|v| v["service"] == "git-receive-pack")
        .collect::<Vec<_>>();
    assert_eq!(
        pushes.len(),
        2,
        "exactly one physical receive-pack per original attempt"
    );
    assert!(
        pushes
            .iter()
            .all(|v| v["repository"] == lab.root.join("r1-checkpoint.git").to_str().unwrap())
    );
    assert!(
        invocations
            .iter()
            .all(|v| !v["repository"].as_str().unwrap().contains("r2-")),
        "no current-head fallback or R2 material access"
    );
    assert_eq!(
        lab.starts.len(),
        2,
        "no executor restart from either lost ACK"
    );
    let executor_bytes = fs::read(lab.root.join("catalogue-executor-entries.jsonl")).unwrap();
    assert!(executor_bytes.len() <= lab.limits.transport.body_bytes);
    let executor_entries = std::str::from_utf8(&executor_bytes)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        executor_entries.len(),
        2,
        "exactly two physical executor processes"
    );
    for observed in &lab.starts {
        assert_eq!(
            executor_entries
                .iter()
                .filter(|entry| entry["pid"] == observed["pid"] && entry["cwd"] == observed["cwd"])
                .count(),
            1,
            "each physical process is the original observed R1 executor"
        );
    }
    let checkpoint_refs = git(
        &lab.root,
        &[
            "--git-dir",
            lab.root.join("r1-checkpoint.git").to_str().unwrap(),
            "for-each-ref",
            "--format=%(refname) %(objectname)",
        ],
    );
    let mut expected_refs = accepted
        .iter()
        .map(|t| {
            let c = t.selected_checkpoint.as_ref().unwrap();
            format!("{} {}", c.remote_ref, c.commit)
        })
        .collect::<Vec<_>>();
    expected_refs.sort();
    assert_eq!(
        checkpoint_refs.lines().collect::<Vec<_>>(),
        expected_refs.iter().map(String::as_str).collect::<Vec<_>>()
    );
    assert_eq!(
        git(
            &lab.root,
            &[
                "--git-dir",
                lab.root.join("r1-source.git").to_str().unwrap(),
                "for-each-ref",
                "--format=%(refname) %(objectname)"
            ]
        ),
        source_refs,
        "checkpoint publication must not rewrite the accepted source endpoint"
    );
    assert!(
        git(
            &lab.root,
            &[
                "--git-dir",
                lab.root.join("r2-checkpoint.git").to_str().unwrap(),
                "for-each-ref"
            ]
        )
        .is_empty()
    );
    lab.restart_controller().await;
    for old in &accepted {
        let current = lab.get(&old.id).await;
        assert_eq!(current.input, old.input);
        assert_eq!(current.attempt, old.attempt);
        assert_eq!(current.result, old.result);
        assert_eq!(current.selected_checkpoint, old.selected_checkpoint);
    }
    assert_eq!(
        operation(&lab, &operation_id).await.outcome,
        durable.outcome
    );
    let replay = lab
        .client
        .call(lost_settlement.request.command.clone())
        .await
        .unwrap();
    assert_eq!(replay.outcome, lost_settlement.reply.outcome);
    assert_eq!(replay.commit, lost_settlement.reply.commit);
    let replay = lab.client.call(register).await.unwrap();
    assert_eq!(replay.outcome, lost_registration.reply.outcome);
    assert_eq!(replay.commit, lost_registration.reply.commit);
    assert_eq!(
        fs::read(lab.root.join("git-pack-invocations.jsonl")).unwrap(),
        invocation_bytes
    );
    assert_eq!(
        git(
            &lab.root,
            &[
                "--git-dir",
                lab.root.join("r1-checkpoint.git").to_str().unwrap(),
                "for-each-ref",
                "--format=%(refname) %(objectname)"
            ]
        ),
        checkpoint_refs
    );
    assert_eq!(
        entry(
            lab.client
                .call(Operation::GetRepository {
                    repository_id: r1.repository_id.clone(),
                    registration_revision: None
                })
                .await
                .unwrap()
        ),
        disabled
    );
    save(
        lab.root.join("joined-catalogue-recovery.json"),
        &json!({"r1":r1,"disabled_current":disabled,"accepted":accepted,"original_registration":lost_registration.reply,"original_settlement":lost_settlement.reply,"physical_git_invocations":invocations,"scope":"two coding tasks; parent/child admission remains separately qualified"}),
    );
    lab.close();
}
