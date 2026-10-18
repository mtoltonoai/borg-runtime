//! Real controller and two worker processes, faulted loopback transport, local
//! Git, and a controlled Codex-protocol peer. This is not real Codex/provider
//! qualification. Every subprocess executes a retained copy of the same binary.
#[path = "support/retained_images.rs"]
mod retained_images;

use borg_runtime::{client::ApiClient, limits::RuntimeLimits, protocol::*};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{
    Request as HttpRequest, Response as HttpResponse, body::Incoming, service::service_fn,
};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io,
    net::SocketAddr,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};

static SEQUENCE: AtomicU64 = AtomicU64::new(1);
const TIMEOUT: Duration = Duration::from_secs(30);
fn selected_definition(id: &str, executor: ExecutorKind) -> Definition {
    Definition {
        charter: None,
        limit_overrides: None,
        id: id.into(),
        revision: "1".into(),
        executor,
        inference: Some(InferenceSettings::selected()),
        execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
    }
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
fn git(cwd: &Path, args: &[&str]) -> String {
    let o = Command::new("/usr/bin/git")
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "user.name=Recovery fixture",
            "-c",
            "user.email=recovery@example.invalid",
        ])
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap();
    assert!(
        o.status.success(),
        "Git {args:?}: {}",
        String::from_utf8_lossy(&o.stderr)
    );
    String::from_utf8(o.stdout).unwrap().trim().into()
}
fn task(reply: Reply) -> Task {
    match reply.outcome {
        Outcome::Ok {
            data: Data::Task(t),
        } => *t,
        x => panic!("expected task: {x:?}"),
    }
}
// Only controller-accepted inputs may supply recovery/execution metadata.
fn accepted_limits(task: &Task) -> Arc<RuntimeLimits> {
    task.input
        .execution_limits()
        .expect("controller-bound execution limits");
    Arc::new(
        *task
            .input
            .resolved_limits
            .as_ref()
            .unwrap()
            .global_metadata
            .clone(),
    )
}
fn refused(reply: Reply) -> String {
    match reply.outcome {
        Outcome::Refused { code, .. } => code,
        x => panic!("expected refusal: {x:?}"),
    }
}
fn save(path: impl AsRef<Path>, value: &impl serde::Serialize) {
    fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}

struct Process {
    child: Child,
    name: String,
    evidence: PathBuf,
}
impl Process {
    fn kill(&mut self) {
        if self.child.try_wait().unwrap().is_none() {
            self.child.kill().unwrap();
        }
        let status = self.child.wait().unwrap();
        save(
            self.evidence.join(format!("{}-closed.json", self.name)),
            &json!({"pid":self.child.id(),"status":status.to_string(),"forced":true}),
        );
    }
    fn stop(&mut self) {
        if self.child.try_wait().unwrap().is_some() {
            return;
        }
        let _ = Command::new("/bin/kill")
            .args(["-INT", "--", &self.child.id().to_string()])
            .status();
        let end = Instant::now() + Duration::from_secs(18);
        while Instant::now() < end {
            if let Some(status) = self.child.try_wait().unwrap() {
                save(
                    self.evidence.join(format!("{}-closed.json", self.name)),
                    &json!({"pid":self.child.id(),"status":status.to_string(),"forced":false}),
                );
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        self.kill();
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Clone, Copy)]
enum Fault {
    DropSubmission,
    DropSettlement,
    DropRegistration,
    DropAwait,
    PendingPrepare,
    PendingToolObservation,
    RefusePrepare,
    ObserveMissingTask,
    DuplicateLaunch,
    StaleIncarnationLaunch,
    WrongWorkerLaunch,
    DropDelegation,
}
struct FaultState {
    fault: Fault,
    used: bool,
    duplicates: usize,
    settlements: usize,
    missing_task_lookups: usize,
    launch_original: Option<(Request, Reply)>,
    launch_barrier_seen: bool,
    launch_current_released: bool,
    launch_gate: tokio::sync::watch::Sender<bool>,
    initial_start_reports: usize,
}
struct FaultEvent {
    original: Request,
    reply: Reply,
    release: Option<tokio::sync::oneshot::Sender<()>>,
}
struct Proxy {
    url: String,
    events: mpsc::Receiver<FaultEvent>,
    state: Arc<Mutex<FaultState>>,
    join: tokio::task::JoinHandle<()>,
}
impl Proxy {
    async fn new(upstream: String, fault: Fault, evidence: PathBuf, body_bytes: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(FaultState {
            fault,
            used: false,
            duplicates: 0,
            settlements: 0,
            missing_task_lookups: 0,
            launch_original: None,
            launch_barrier_seen: false,
            launch_current_released: false,
            launch_gate: tokio::sync::watch::channel(false).0,
            initial_start_reports: 0,
        }));
        let (send, events) = mpsc::channel(8);
        let shared = state.clone();
        let join = tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(28))
                .build()
                .unwrap();
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted=listener.accept()=>{
                        let (stream,_)=accepted.unwrap();
                        let (client,upstream,state,send,evidence)=(client.clone(),upstream.clone(),shared.clone(),send.clone(),evidence.clone());
                        connections.spawn(async move {
                            let service=service_fn(move |request:HttpRequest<Incoming>|{
                                let (client,upstream,state,send,evidence)=(client.clone(),upstream.clone(),state.clone(),send.clone(),evidence.clone());
                                async move {
                                    let path=request.uri().path().to_owned();
                                    let caller=request.headers().get("x-borg-caller").cloned();
                                    let version=request.headers().get("mcp-protocol-version").cloned();
                                    let bytes=request.into_body().collect().await.map_err(io::Error::other)?.to_bytes();
                                    if bytes.len()>body_bytes { return Err(io::Error::other("fixture input too large")); }
                                    // Forward the existing MCP transport unchanged. Faults target its
                                    // exact typed occurrence, not an invented private API substitute.
                                    if path=="/mcp" {
                                        let rpc:Value=serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                                        let invocation=if rpc["method"]=="tools/call" {
                                            Some(borg_runtime::mcp::Invocation{
                                                operation_id:rpc["params"]["_meta"]["borg"]["operation_id"].as_str().unwrap().into(),
                                                logical_call_id:rpc["params"]["_meta"]["borg"]["logical_call_id"].as_str().unwrap().into(),
                                                caller:serde_json::from_slice(caller.as_ref().unwrap().as_bytes()).map_err(io::Error::other)?,
                                                tool:rpc["params"]["name"].as_str().unwrap().into(),arguments:rpc["params"]["arguments"].clone(),
                                            })
                                        }else{None};
                                        let withheld={let mut state=state.lock().unwrap();
                                            if !state.used && matches!(state.fault,Fault::PendingToolObservation) && invocation.as_ref().is_some_and(|c|c.tool=="collab_task_get") {state.used=true;true}else{false}};
                                        if withheld {
                                            let call=invocation.clone().unwrap();
                                            let original=Request{protocol_revision:WIRE_REVISION,request_id:rpc["id"].as_str().unwrap().into(),command:Operation::CallCollaboration{call:call.clone()}};
                                            let reply=Reply{request_id:original.request_id.clone(),outcome:Outcome::Pending{operation_id:call.operation_id},commit:None,observed_controller_revision:None};
                                            save(evidence.join("withheld-tool-call.json"),&json!({"request":original,"reply":reply,"forwarded_to_controller":false}));
                                            let response=json!({"jsonrpc":"2.0","id":rpc["id"],"result":{"content":[{"type":"text","text":serde_json::to_string(&borg_runtime::mcp::CallResult{reply:reply.clone(),requires_quiescence:false}).unwrap()}],"isError":true}});
                                            let (release,held)=tokio::sync::oneshot::channel();
                                            send.send(FaultEvent{original,reply,release:Some(release)}).await.map_err(io::Error::other)?;
                                            held.await.map_err(io::Error::other)?;
                                            return Ok(HttpResponse::new(Full::new(Bytes::from(serde_json::to_vec(&response).unwrap()))));
                                        }
                                        let mut forwarded=client.post(format!("{upstream}{path}")).header("content-type","application/json").body(bytes.to_vec());
                                        if let Some(caller)=caller{forwarded=forwarded.header("x-borg-caller",caller);}
                                        if let Some(version)=version{forwarded=forwarded.header("mcp-protocol-version",version);}
                                        let response=forwarded.send().await.map_err(io::Error::other)?;
                                        let status=response.status();let response=response.bytes().await.map_err(io::Error::other)?;
                                        let candidate={let state=state.lock().unwrap();!state.used && matches!(state.fault,Fault::DropDelegation) && invocation.as_ref().is_some_and(|c|c.tool=="collab_task_delegate")};
                                        if candidate {
                                            let value:Value=serde_json::from_slice(&response).map_err(io::Error::other)?;
                                            let result:borg_runtime::mcp::CallResult=serde_json::from_str(value["result"]["content"][0]["text"].as_str().unwrap()).map_err(io::Error::other)?;
                                            if matches!(result.reply.outcome,Outcome::Ok{..}) && result.reply.commit.is_some() {
                                                let selected={let mut state=state.lock().unwrap();if state.used{false}else{state.used=true;true}};
                                                if selected {
                                                    let original=Request{protocol_revision:WIRE_REVISION,request_id:result.reply.request_id.clone(),command:Operation::CallCollaboration{call:invocation.unwrap()}};
                                                    save(evidence.join("dropped-response.json"),&json!({"request":original,"committed_reply":result.reply}));
                                                    send.send(FaultEvent{original,reply:result.reply,release:None}).await.map_err(io::Error::other)?;
                                                    return Err(io::Error::other("controlled lost MCP response AFTER actual controller commit"));
                                                }
                                            }
                                        }
                                        return Ok(HttpResponse::builder().status(status.as_u16()).body(Full::new(response)).unwrap());
                                    }
                                    assert_eq!(path,"/api/operations");
                                    let original:Request=serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                                    // The next distinct poll is reached only after processing the
                                    // injected batch. Hold it BEFORE upstream long polling. Every
                                    // retransmission shares this gate, so a lost transport cannot
                                    // accidentally release the genuine command during inspection.
                                    let launch_barrier = {
                                        let mut state=state.lock().unwrap();
                                        if matches!(state.fault,Fault::StaleIncarnationLaunch|Fault::WrongWorkerLaunch) {
                                            if matches!(&original.command,Operation::ObserveWorker{report}
                                                if report.report_id.ends_with("-initial-start")) {
                                                state.initial_start_reports+=1;
                                                save(evidence.join(format!("launch-initial-start-{}.json",state.initial_start_reports)),&original);
                                            }
                                            if matches!(&original.command,Operation::AwaitCommands{..})
                                                && !state.launch_current_released {
                                                state.launch_original.clone().map(|(prior,reply)| {
                                                    assert_ne!(original.command.mutation_id(),prior.command.mutation_id(),"barrier must be the next command occurrence");
                                                    let first=!state.launch_barrier_seen;
                                                    state.launch_barrier_seen=true;
                                                    (first,reply,state.launch_gate.subscribe())
                                                })
                                            } else {None}
                                        } else {None}
                                    };
                                    if let Some((first,prior_reply,mut gate))=launch_barrier {
                                        if first {
                                            save(evidence.join("launch-processing-barrier.json"),&json!({"next_request":original,"prior_reply":prior_reply,"forwarded":false}));
                                            send.send(FaultEvent{original:original.clone(),reply:prior_reply,release:None}).await.map_err(io::Error::other)?;
                                        }
                                        while !*gate.borrow_and_update() {gate.changed().await.map_err(io::Error::other)?;}
                                    }
                                    let withheld = {
                                        let mut state = state.lock().unwrap();
                                        if matches!(original.command, Operation::SettleOperation {..}) { state.settlements += 1; }
                                        if !state.used && ((matches!(original.command, Operation::PrepareOperation {..})
                                            && matches!(state.fault, Fault::PendingPrepare | Fault::RefusePrepare))
                                            || (matches!(state.fault, Fault::PendingToolObservation) && matches!(&original.command,Operation::CallCollaboration{call} if call.tool=="collab_task_get"))) {
                                            state.used = true; Some(state.fault)
                                        } else { None }
                                    };
                                    if let Some(mode) = withheld {
                                        let operation_id = original.command.mutation_id().unwrap().to_owned();
                                        let outcome = match mode {
                                            Fault::PendingPrepare | Fault::PendingToolObservation => Outcome::Pending { operation_id },
                                            Fault::RefusePrepare => Outcome::Refused { code: "capacity".into(), message: "controlled prepare refusal before controller commit".into() },
                                            _ => unreachable!(),
                                        };
                                        let reply = Reply { request_id: original.request_id.clone(), outcome,
                                            commit: None, observed_controller_revision: None };
                                        save(evidence.join("withheld-prepare.json"), &json!({"request":original,"reply":reply,"forwarded_to_controller":false}));
                                        let (release, held) = tokio::sync::oneshot::channel();
                                        send.send(FaultEvent { original, reply: reply.clone(), release: Some(release) }).await.map_err(io::Error::other)?;
                                        held.await.map_err(io::Error::other)?;
                                        return Ok(HttpResponse::new(Full::new(Bytes::from(serde_json::to_vec(&reply).unwrap()))));
                                    }
                                    let response=client.post(format!("{upstream}/api/operations")).header("content-type","application/json").body(bytes.to_vec()).send().await.map_err(io::Error::other)?;
                                    let bytes=response.bytes().await.map_err(io::Error::other)?;
                                    let mut reply:Reply=serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                                    {
                                        let mut state=state.lock().unwrap();
                                        if matches!(state.fault,Fault::StaleIncarnationLaunch|Fault::WrongWorkerLaunch) {
                                            let genuine=reply.clone();
                                            if let Outcome::Ok{data:Data::Commands(commands)}=&mut reply.outcome {
                                                if state.launch_original.is_none() && commands.iter().any(|c|matches!(c,WorkerCommand::RunAttempt{..})) {
                                                    assert_eq!(commands.len(),1,"one genuine launch isolates this guard");
                                                    let mut injected=commands[0].clone();
                                                    let WorkerCommand::RunAttempt{session,attempt,task,..}=&mut injected else {unreachable!()};
                                                    session.validate_for(attempt,task).unwrap();
                                                    if matches!(state.fault,Fault::StaleIncarnationLaunch) {
                                                        attempt.worker_incarnation="earlier-incarnation".into();
                                                    } else {attempt.worker_id="other-worker".into();}
                                                    session.validate_for(attempt,task).unwrap();
                                                    commands[0]=injected;
                                                    state.launch_original=Some((original.clone(),genuine.clone()));
                                                    save(evidence.join("launch-original-and-injected.json"),&json!({"request":original,"original_reply":genuine,"injected_reply":reply}));
                                                } else if state.launch_barrier_seen && !state.launch_current_released {
                                                    let (_,prior)=state.launch_original.as_ref().unwrap();
                                                    let Outcome::Ok{data:Data::Commands(first)}=&prior.outcome else {unreachable!()};
                                                    assert_eq!(commands.as_slice(),first.as_slice(),"controller must redeliver the unacknowledged original command");
                                                    commands.extend(first.clone());
                                                    state.duplicates+=1;
                                                    state.launch_current_released=true;
                                                    save(evidence.join("launch-current-and-duplicate.json"),&json!({"request":original,"reply":reply}));
                                                }
                                            }
                                        }
                                    }
                                    let missing_observed = {
                                        let mut state = state.lock().unwrap();
                                        if matches!(state.fault, Fault::ObserveMissingTask)
                                            && matches!(original.command, Operation::GetTask {..})
                                            && matches!(&reply.outcome, Outcome::Refused {code,..} if code=="not_found")
                                            && state.missing_task_lookups < 2 {
                                            state.missing_task_lookups += 1;
                                            Some(state.missing_task_lookups)
                                        } else { None }
                                    };
                                    if let Some(index) = missing_observed {
                                        save(evidence.join(format!("missing-task-lookup-{index}.json")), &json!({"request":original,"reply":reply}));
                                        send.send(FaultEvent { original:original.clone(), reply:reply.clone(), release:None }).await.map_err(io::Error::other)?;
                                    }
                                    let drop_response={
                                        let mut state=state.lock().unwrap();
                                        let selected=!state.used && (matches!((state.fault,&original.command),(Fault::DropSubmission,Operation::SubmitTask{..})|(Fault::DropSettlement,Operation::SettleOperation{..})|(Fault::DropRegistration,Operation::RegisterWorker{..})|(Fault::DropAwait,Operation::AwaitCommands{..})) || matches!((state.fault,&original.command),(Fault::DropDelegation,Operation::CallCollaboration{call}) if call.tool=="collab_task_delegate"));
                                        if selected && matches!(reply.outcome,Outcome::Ok{..}) { state.used=true; true }
                                        else {
                                            if matches!(state.fault,Fault::DuplicateLaunch) {
                                                if let Outcome::Ok{data:Data::Commands(commands)}=&mut reply.outcome {
                                                    let copies=commands.iter().filter(|c|matches!(c,WorkerCommand::RunAttempt{..})).cloned().collect::<Vec<_>>();
                                                    state.duplicates+=copies.len(); commands.extend(copies);
                                                }
                                            }
                                            false
                                        }
                                    };
                                    if drop_response {
                                        save(evidence.join("dropped-response.json"),&json!({"request":original,"committed_reply":reply}));
                                        send.send(FaultEvent{original,reply,release:None}).await.map_err(io::Error::other)?;
                                        return Err(io::Error::other("controlled lost response AFTER actual controller commit"));
                                    }
                                    Ok::<_,io::Error>(HttpResponse::new(Full::new(Bytes::from(serde_json::to_vec(&reply).unwrap()))))
                                }
                            });
                            let _=hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream),service).await;
                        });
                    },
                    _=connections.join_next(),if !connections.is_empty()=>{}
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
    async fn event(&mut self) -> FaultEvent {
        tokio::time::timeout(TIMEOUT, self.events.recv())
            .await
            .expect("fault not reached")
            .expect("fault owner closed")
    }
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.join.abort();
    }
}

struct Gate {
    stream: TcpStream,
    observed: Value,
}
impl Gate {
    async fn release(mut self) {
        self.stream.write_all(b"G").await.unwrap();
    }
}
struct Lab {
    root: PathBuf,
    image: PathBuf,
    address: SocketAddr,
    origin: String,
    client: ApiClient,
    limits: Arc<RuntimeLimits>,
    fixture: PathBuf,
    gate: TcpListener,
    repository: Repository,
    workers: Vec<Process>,
    controller: Option<Process>,
    starts: Vec<Value>,
    serial: usize,
}
impl Lab {
    async fn new(label: &str) -> Self {
        Self::new_with_private_image(label, false).await
    }
    async fn new_with_private_image(label: &str, private: bool) -> Self {
        let base = retained_images::artifact_root().join("recovery/runs");
        fs::create_dir_all(&base).unwrap();
        // PIDs are reused by sandbox namespaces. Allocate the case directory
        // exclusively BEFORE writing any subject/evidence, even on a clock tie.
        let root = loop {
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let candidate = base.join(format!(
                "{label}-{}-{stamp}-{}",
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
            match fs::create_dir(&candidate) {
                Ok(()) => break candidate,
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => panic!("fresh evidence directory: {e}"),
            }
        };
        let limits = Arc::new(RuntimeLimits::seed().unwrap());
        let limits_path = root.join("limits.json");
        save(&limits_path, limits.as_ref());
        // Controller/coding workers and native workers select distinct explicit
        // startup metadata. Accepted task snapshots still come from controller.
        let mut native_worker_limits = limits.as_ref().clone();
        native_worker_limits.revision = format!("{}-native-worker-fixture", limits.revision);
        native_worker_limits.worker.coding_capacity = 0;
        native_worker_limits.worker.native_capacity = 1;
        native_worker_limits.validate().unwrap();
        let native_limits_path = root.join("native-worker-limits.json");
        save(&native_limits_path, &native_worker_limits);
        let image = root.join("borg-runtime");
        let input = std::env::var_os("BORG_RECOVERY_BINARY")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_borg-runtime")));
        retained_images::materialize(&input, &image, private);
        let mut pins = vec![
            image.clone(),
            limits_path,
            native_limits_path,
            Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"),
            Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.lock"),
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/recovery.rs"),
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/retained_images.rs"),
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/recovery/fixtures/codex_protocol.py"),
        ];
        for folder in ["src", "tests/recovery"] {
            collect_rs(
                &Path::new(env!("CARGO_MANIFEST_DIR")).join(folder),
                &mut pins,
            );
        }
        let hashes = Command::new("/usr/bin/sha256sum")
            .args(pins)
            .output()
            .unwrap();
        assert!(hashes.status.success());
        fs::write(root.join("source-image.sha256"), hashes.stdout).unwrap();
        let seed = root.join("seed");
        fs::create_dir(&seed).unwrap();
        fs::write(seed.join("answer.txt"), "before\n").unwrap();
        git(&seed, &["init", "-q"]);
        git(&seed, &["add", "answer.txt"]);
        git(&seed, &["commit", "-qm", "controlled starting source"]);
        let start_commit = git(&seed, &["rev-parse", "HEAD"]);
        let remote = root.join("remote.git");
        git(
            &root,
            &[
                "clone",
                "--bare",
                "--quiet",
                seed.to_str().unwrap(),
                remote.to_str().unwrap(),
            ],
        );
        let repository = Repository {
            pull_request_target: None,
            registration: Some(borg_runtime::repository_catalogue::Revision {
                repository_id: "recovery-repository".into(), registration_revision: "initial".into(),
            }),
            url: remote.to_string_lossy().into(),
            start_commit,
            checkpoint_remote: remote.to_string_lossy().into(),
        };
        let gate = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fixture = root.join("codex_protocol.py");
        fs::write(
            &fixture,
            include_str!("recovery/fixtures/codex_protocol.py"),
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();
        save(
            fixture.with_extension("json"),
            &json!({"port":gate.local_addr().unwrap().port()}),
        );
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let origin = format!("http://{address}");
        let client = ApiClient::new(&origin, limits.clone()).unwrap();
        let mut lab = Self {
            root,
            image,
            address,
            origin,
            client,
            limits,
            fixture,
            gate,
            repository,
            workers: vec![],
            controller: None,
            starts: vec![],
            serial: 0,
        };
        lab.restart_controller().await;
        let binding = lab.repository.registration.as_ref().unwrap();
        let registered = lab.client.call(Operation::RegisterRepository {
            operation_id: "register-recovery-repository".into(),
            registration: borg_runtime::repository_catalogue::Registration {
                pull_request_target: None,
                repository_id: binding.repository_id.clone(),
                registration_revision: binding.registration_revision.clone(),
                display_name: "Recovery fixture repository".into(),
                source_endpoint: lab.repository.url.clone(),
                checkpoint_endpoint: lab.repository.checkpoint_remote.clone(),
                source_store_key: "remote.git".into(), checkpoint_store_key: "remote.git".into(),
            },
        }).await.unwrap();
        assert!(matches!(registered.outcome, Outcome::Ok { data: Data::RepositoryRegistration(_) }));
        lab
    }
    fn spawn(&mut self, name: String, args: &[&str]) -> Process {
        self.spawn_env(name, args, &[], "limits.json")
    }
    fn spawn_env(
        &mut self,
        name: String,
        args: &[&str],
        environment: &[(&str, &str)],
        limits_file: &str,
    ) -> Process {
        let log = File::create(self.root.join(format!("{name}.log"))).unwrap();
        let child = Command::new(&self.image)
            .args(args)
            .arg("--limits")
            .arg(self.root.join(limits_file))
            .env_remove("OPENAI_API_KEY")
            .env_remove("CODEX_API_KEY")
            .env_remove("GITHUB_TOKEN")
            .env_remove("GH_TOKEN")
            .envs(environment.iter().copied())
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        Process {
            child,
            name,
            evidence: self.root.clone(),
        }
    }
    async fn restart_controller(&mut self) {
        if let Some(mut old) = self.controller.take() {
            old.kill();
        }
        self.serial += 1;
        let (db, address, origin) = (
            self.root.join("tasks.sqlite"),
            self.address.to_string(),
            self.origin.clone(),
        );
        self.controller = Some(self.spawn(
            format!("controller-{}", self.serial),
            &[
                "controller",
                "--db",
                db.to_str().unwrap(),
                "--listen",
                &address,
                "--origin",
                &origin,
            ],
        ));
        let end = Instant::now() + TIMEOUT;
        loop {
            if let Ok(Reply {
                outcome: Outcome::Ok { .. },
                ..
            }) = self
                .client
                .call(Operation::ListTasks {
                    after: None,
                    limit: 1,
                })
                .await
            {
                break;
            }
            assert!(
                Instant::now() < end,
                "controller did not start: {}",
                self.root.display()
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
    fn start_worker(&mut self, id: &str, incarnation: &str, url: &str) -> usize {
        let workspace = self.root.join(id);
        let fixture = self.fixture.clone();
        let process = self.spawn(
            format!("{id}-{incarnation}"),
            &[
                "worker",
                "--controller",
                url,
                "--id",
                id,
                "--incarnation",
                incarnation,
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
        self.workers.push(process);
        self.workers.len() - 1
    }
    fn start_native_worker(&mut self, incarnation: &str, url: &str, endpoint: &str) -> usize {
        let workspace = self.root.join("native-worker");
        let fixture = self.fixture.clone();
        let process = self.spawn_env(
            format!("native-worker-{incarnation}"),
            &[
                "worker",
                "--controller",
                url,
                "--id",
                "native-worker",
                "--incarnation",
                incarnation,
                "--workspace-root",
                workspace.to_str().unwrap(),
                "--codex",
                fixture.to_str().unwrap(),
                "--coding-capacity",
                "0",
                "--native-capacity",
                "1",
                "--native-endpoint",
                endpoint,
            ],
            &[("OPENAI_API_KEY", "controlled-native-fixture-key")],
            "native-worker-limits.json",
        );
        self.workers.push(process);
        self.workers.len() - 1
    }
    // External submissions select metadata; only the accepted projection has endpoints.
    fn repository_selector(&self) -> Repository {
        let mut selected = self.repository.clone();
        selected.url.clear();
        selected.checkpoint_remote.clear();
        selected
    }
    fn input(&self, agent: &str) -> TaskInput {
        TaskInput { instruction_source: None,
            pull_request: None,
            resolved_limits: None,
            instruction: "Set answer.txt to after, preserving only the intended source.".into(),
            definition: Definition {
                charter: None,
                limit_overrides: None,
                inference: Some(InferenceSettings::selected()),
                execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
                id: agent.into(),
                revision: "v1".into(),
                executor: ExecutorKind::Coding,
            },
            repository: Some(self.repository_selector()),
            repository_read: None,
            acceptance_commands: vec![vec![
                "/bin/sh".into(),
                "-c".into(),
                "test \"$(cat answer.txt)\" = after".into(),
            ]],
            output_schema: None,
            limits: Limits {
                deadline_unix_ms: now() + 120_000,
                model_requests: 4,
                output_bytes: 8192,
                result_bytes: self.limits.task_defaults.result_bytes,
                max_delegation_depth: 4,
            },
            dependencies: vec![],
            native_workflow: None,
        }
    }
    async fn submit(&self, operation: &str, agent: &str) -> Task {
        task(
            self.client
                .call(Operation::SubmitTask {
                    operation_id: operation.into(),
                    input: self.input(agent),
                })
                .await
                .unwrap(),
        )
    }
    async fn gate(&mut self) -> Gate {
        let (stream, _) = tokio::time::timeout(TIMEOUT, self.gate.accept())
            .await
            .expect("executor did not reach controlled gate")
            .unwrap();
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        tokio::time::timeout(TIMEOUT, reader.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        assert!(line.len() < 4096);
        let observed: Value = serde_json::from_str(&line).unwrap();
        self.starts.push(observed.clone());
        save(self.root.join("executor-starts.json"), &self.starts);
        Gate {
            stream: reader.into_inner(),
            observed,
        }
    }
    async fn get(&self, id: &str) -> Task {
        task(
            self.client
                .call(Operation::GetTask { task_id: id.into() })
                .await
                .unwrap(),
        )
    }
    async fn until(&self, id: &str, why: &str, predicate: impl Fn(&Task) -> bool) -> Task {
        let end = Instant::now() + TIMEOUT;
        loop {
            let t = self.get(id).await;
            if predicate(&t) {
                return t;
            }
            assert!(
                Instant::now() < end,
                "{why}: {t:?}; evidence {}",
                self.root.display()
            );
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }
    async fn successful(&self, id: &str) -> Task {
        let task = self
            .until(id, "accepted result", |t| {
                t.status == TaskStatus::Succeeded && t.attempt.as_ref().is_some_and(|a| a.stopped)
            })
            .await;
        let cp = task.selected_checkpoint.as_ref().unwrap();
        assert_eq!(
            git(
                &self.root,
                &["--git-dir", &cp.repository, "rev-parse", &cp.remote_ref]
            ),
            cp.commit
        );
        assert_eq!(
            git(
                &self.root,
                &[
                    "--git-dir",
                    &cp.repository,
                    "show",
                    &format!("{}:answer.txt", cp.commit)
                ]
            ),
            "after"
        );
        save(self.root.join(format!("{}.json", id)), &task);
        task
    }
    fn close(&mut self) {
        for worker in &mut self.workers {
            worker.stop();
        }
        if let Some(mut controller) = self.controller.take() {
            controller.stop();
        }
        save(
            self.root.join("case-complete.json"),
            &json!({"status":"PASS","executor_starts":self.starts,"scope":"Actual controller/two worker processes; controlled stdio peer and local Git only"}),
        );
    }
}
impl Drop for Lab {
    fn drop(&mut self) {
        for worker in &mut self.workers {
            worker.stop();
        }
        if let Some(c) = &mut self.controller {
            c.stop();
        }
    }
}
fn collect_rs(dir: &Path, out: &mut Vec<PathBuf>) {
    let mut entries = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect::<Vec<_>>();
    entries.sort();
    for p in entries {
        if p.is_dir() {
            collect_rs(&p, out);
        } else if p.extension().is_some_and(|e| e == "rs") {
            out.push(p);
        }
    }
}
fn stale_report(a: &Attempt, id: &str) -> Operation {
    Operation::ObserveWorker {
        report: Report {
            report_id: id.into(),
            worker_id: a.worker_id.clone(),
            incarnation: a.worker_incarnation.clone(),
            task_id: a.task_id.clone(),
            attempt_id: a.id.clone(),
            generation: a.generation,
            observation: Observation::Progress {
                text: "stale".into(),
                output: String::new(),
                truncated: false,
            },
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acknowledged_coding_turn_eof_retains_uncertainty_and_refuses_retry() {
    let mut lab = Lab::new("coding-ack-eof").await;
    let original = lab.submit("coding-ack-eof-submit", "coding-ack-eof-agent").await;
    let origin = lab.origin.clone();
    lab.start_worker("coding-ack-eof-worker", "initial", &origin);
    let mut gate = lab.gate().await;
    // The existing peer sends the exact turn/start ACK before this barrier.
    // Anything except G makes it exit without any turn terminal notification.
    let observed = gate.observed.clone();
    let running = lab.until(&original.id, "initial turn association", |task| {
        task.attempt.as_ref().is_some_and(|attempt| attempt.phase == AttemptPhase::Running)
    }).await;
    let attempt = running.attempt.as_ref().unwrap().clone();
    let state = lab.root.join("coding-ack-eof-worker/.borg-worker")
        .join(borg_runtime::worker::process::component("coding-ack-eof-worker"));
    let thread = state.join("codex")
        .join(borg_runtime::worker::process::component(&attempt.id)).join("borg-thread.json");
    let until = Instant::now() + TIMEOUT;
    let (association_bytes, association) = loop {
        let bytes = fs::read(&thread).unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        if value["turn_id"] == observed["turn"] { break (bytes, value); }
        assert!(Instant::now() < until, "original turn ACK was not retained");
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(association["thread_id"], observed["thread"]);
    assert_eq!(association["turn_id"], observed["turn"]);
    gate.stream.write_all(b"E").await.unwrap();
    drop(gate);
    let stopped = lab.until(&original.id, "acknowledged turn EOF and actual stop", |task| {
        task.status == TaskStatus::Failed && task.attempt.as_ref().is_some_and(|attempt| attempt.stopped)
    }).await;
    assert_eq!(stopped.input, original.input);
    assert_eq!(stopped.attempt.as_ref().unwrap().id, attempt.id);
    assert!(stopped.result.is_none());
    assert_eq!(fs::read(&thread).unwrap(), association_bytes);
    let bridge: borg_runtime::worker::codex_mcp::Config = serde_json::from_slice(
        &fs::read(thread.parent().unwrap().join("borg-collaboration-config.json")).unwrap()).unwrap();
    assert_eq!(borg_runtime::worker::codex_mcp::outcome_disposition(&bridge).unwrap(),
        borg_runtime::worker::codex_mcp::OutcomeDisposition::NoCalls);
    let retried = lab.client.call(Operation::RetryTask {
        operation_id: "coding-ack-eof-retry".into(), task_id: original.id.clone(),
        expected_revision: stopped.revision,
    }).await.unwrap();
    save(lab.root.join("coding-ack-eof-observation.json"),
        &json!({"task":stopped,"retry":retried,"association":association,"executor_start":observed}));
    assert!(matches!(&retried.outcome, Outcome::Refused {code,..} if code == "reconciliation_required"),
        "an acknowledged turn without a terminal event is unresolved: {retried:?}");
    assert!(stopped.attempt.as_ref().unwrap().uncertain);
    assert!(tokio::time::timeout(Duration::from_millis(200), lab.gate.accept()).await.is_err(),
        "unknown original turn was dispatched again");
    lab.restart_controller().await;
    let reopened = lab.get(&original.id).await;
    assert!(reopened.attempt.as_ref().unwrap().uncertain);
    assert_eq!(reopened.attempt.as_ref().unwrap().id, attempt.id);
    assert_eq!(lab.starts.len(), 1);
    lab.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
//= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
//# An accepted task MUST have a durable input record.
//= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
//# Committed platform task state MUST survive an application-process restart while the durable store remains intact.
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//# An exact retry of a committed mutating request MUST return its recorded outcome.
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//# Task submission MUST commit its input and submission outcome atomically.
//= spec/impl/v2/implementation.md#worker-commands-do-not-create-duplicate-execution
//# A duplicate launch in the same worker incarnation MUST return the existing execution or outcome instead of starting another execution.
// Traceability scope: This is application-path behavior, not an OS prohibition on a trusted same-user process accessing files.
//= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
//= type=test
//# A worker MUST access task state through the controller's application interface.
// Traceability scope: Actual controller/workers and controlled peer; exact duplicate commands, one execution, accepted state retained across controller restart.
//= spec/impl/v2/implementation.md#the-supplied-executor-owns-its-inner-loop
//= type=test
//# The platform MUST keep accepted task results independently of the supplied executor's process.
// Traceability scope: Intact local SQLite persistence, not storage destruction or deployment authentication.
//= spec/impl/v2/implementation.md#observable-end-to-end-result
//= type=test
//# The initial system MUST durably record an accepted task before reporting acceptance.
async fn lost_submission_reply_restart_and_duplicate_launch_execute_once() {
    let mut lab = Lab::new("lost-submit").await;
    let mut loss = Proxy::new(
        lab.origin.clone(),
        Fault::DropSubmission,
        lab.root.clone(),
        lab.limits.transport.body_bytes,
    )
    .await;
    let request = Operation::SubmitTask {
        operation_id: "lost-submit".into(),
        input: lab.input("agent-a"),
    };
    assert!(
        ApiClient::new(&loss.url, lab.limits.clone())
            .unwrap()
            .call(request.clone())
            .await
            .is_err()
    );
    let committed = loss.event().await;
    let original = task(committed.reply);
    lab.restart_controller().await;
    assert_eq!(task(lab.client.call(request).await.unwrap()), original);
    let second = lab.submit("second", "agent-b").await;
    let duplicate = Proxy::new(
        lab.origin.clone(),
        Fault::DuplicateLaunch,
        lab.root.clone(),
        lab.limits.transport.body_bytes,
    )
    .await;
    lab.start_worker("worker-a", "inc-1", &duplicate.url);
    lab.start_worker("worker-b", "inc-1", &duplicate.url);
    let one = lab.gate().await;
    let two = lab.gate().await;
    one.release().await;
    two.release().await;
    let a = lab.successful(&original.id).await;
    let b = lab.successful(&second.id).await;
    assert_ne!(
        a.attempt.as_ref().unwrap().worker_id,
        b.attempt.as_ref().unwrap().worker_id
    );
    assert_eq!(lab.starts.len(), 2);
    assert_eq!(duplicate.state.lock().unwrap().duplicates, 2);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), lab.gate.accept())
            .await
            .is_err(),
        "duplicate execution unexpectedly started"
    );
    lab.restart_controller().await;
    assert_eq!(lab.get(&a.id).await.result, a.result);
    assert_eq!(lab.get(&b.id).await.result, b.result);
    lab.close();
}

fn assert_launch_path_absent(path: &Path) {
    assert!(matches!(fs::symlink_metadata(path),Err(error) if error.kind()==io::ErrorKind::NotFound),
        "unexpected local launch record or unreadable path: {}",path.display());
}

async fn rejected_launch_then_current_and_duplicate(fault: Fault, label: &str) {
    let mut lab=Lab::new(label).await;
    let accepted=lab.submit("launch-owner-submission","launch-owner-agent").await;
    let mut proxy=Proxy::new(lab.origin.clone(),fault,lab.root.clone(),lab.limits.transport.body_bytes).await;
    let worker=lab.start_worker("worker-a","current-incarnation",&proxy.url);
    let barrier=proxy.event().await;
    let (first_request,first_reply)=proxy.state.lock().unwrap().launch_original.clone().unwrap();
    assert_ne!(first_request.command.mutation_id(),barrier.original.command.mutation_id());
    assert!(matches!(&barrier.original.command,Operation::AwaitCommands{worker_id,incarnation,acknowledged,..}
        if worker_id=="worker-a"&&incarnation=="current-incarnation"&&acknowledged.is_empty()));
    let Outcome::Ok{data:Data::Commands(commands)}=&first_reply.outcome else {panic!("genuine controller launch missing")};
    assert_eq!(commands.len(),1);
    let genuine=commands[0].clone();
    let WorkerCommand::RunAttempt{command_id,session,attempt,task,..}=&genuine else {panic!("expected RunAttempt")};
    assert_eq!(attempt.task_id,accepted.id);assert_eq!(attempt.worker_id,"worker-a");
    assert_eq!(attempt.worker_incarnation,"current-incarnation");
    session.validate_for(attempt,task).unwrap();
    let injected:Value=serde_json::from_slice(&fs::read(lab.root.join("launch-original-and-injected.json")).unwrap()).unwrap();
    let injected_reply:Reply=serde_json::from_value(injected["injected_reply"].clone()).unwrap();
    let Outcome::Ok{data:Data::Commands(altered)}=injected_reply.outcome else {unreachable!()};
    let mut expected=genuine.clone();
    let WorkerCommand::RunAttempt{attempt:changed,..}=&mut expected else {unreachable!()};
    match fault {Fault::StaleIncarnationLaunch=>changed.worker_incarnation="earlier-incarnation".into(),
        Fault::WrongWorkerLaunch=>changed.worker_id="other-worker".into(),_=>unreachable!()}
    assert_eq!(altered,vec![expected]);
    let WorkerCommand::RunAttempt{session,attempt:altered_attempt,task,..}=&altered[0] else {unreachable!()};
    session.validate_for(altered_attempt,task).unwrap();

    // The receipt is written synchronously before the later controller admission
    // gate. Its absence discriminates the early guard even if a broken guard
    // would subsequently be refused before creating a workspace or executor.
    let component=borg_runtime::worker::process::component;
    let workspace=lab.root.join("worker-a");
    let state=workspace.join(".borg-worker").join(component("worker-a"));
    let receipt=state.join("commands").join(format!("{}.json",component(command_id)));
    let initial=format!("{}-initial-start",attempt.id);
    assert_launch_path_absent(&receipt);
    for extension in ["json","refused","outcome"] {
        assert_launch_path_absent(&state.join("outbox").join(format!("{}.{}",component(&initial),extension)));
    }
    assert_eq!(proxy.state.lock().unwrap().initial_start_reports,0);
    assert!(matches!(lab.client.call(Operation::GetOperation{operation_id:initial.clone()}).await.unwrap().outcome,
        Outcome::Refused{code,..} if code=="not_found"));
    assert_launch_path_absent(&workspace.join("workspaces").join(component(&attempt.id)));
    assert_launch_path_absent(&state.join("processes").join(component(&attempt.id)));
    assert!(lab.workers[worker].child.try_wait().unwrap().is_none(),"stale command must not terminate worker");
    assert!(lab.starts.is_empty());
    assert!(tokio::time::timeout(Duration::from_millis(100),lab.gate.accept()).await.is_err(),
        "rejected command unexpectedly launched the supplied executor");
    save(lab.root.join("launch-refusal-oracles.json"),&json!({"command_id":command_id,"attempt":attempt,
        "command_receipt_absent":true,"initial_start_absent":true,"workspace_absent":true,"process_registry_absent":true,
        "worker_alive":true,"next_await_operation":barrier.original.command.mutation_id()}));

    proxy.state.lock().unwrap().launch_gate.send(true).unwrap();
    let gate=lab.gate().await;
    assert_eq!(gate.observed["cwd"],workspace.join("workspaces").join(component(&attempt.id)).to_string_lossy().as_ref());
    let recorded:Value=serde_json::from_slice(&fs::read(&receipt).unwrap()).unwrap();
    assert_eq!(recorded["command"],serde_json::to_value(&genuine).unwrap());
    assert_eq!(recorded["effects_started"],true);
    gate.release().await;
    let completed=lab.successful(&accepted.id).await;
    let current=completed.attempt.as_ref().unwrap();
    assert_eq!(current.id,attempt.id);assert_eq!(current.worker_id,"worker-a");
    assert_eq!(current.worker_incarnation,"current-incarnation");
    assert_eq!(completed.input,accepted.input);
    assert_eq!(lab.starts.len(),1);
    assert_eq!(proxy.state.lock().unwrap().duplicates,1);
    assert_eq!(proxy.state.lock().unwrap().initial_start_reports,1);
    let recorded:Value=serde_json::from_slice(&fs::read(&receipt).unwrap()).unwrap();
    assert_eq!(recorded["command"],serde_json::to_value(&genuine).unwrap());
    let reply=lab.client.call(Operation::GetOperation{operation_id:initial}).await.unwrap();
    let Outcome::Ok{data:Data::Operation(record)}=reply.outcome else {panic!("original current start receipt missing")};
    let Operation::ObserveWorker{report}=serde_json::from_value(record.intended).unwrap() else {unreachable!()};
    assert_eq!(report.worker_id,attempt.worker_id);assert_eq!(report.incarnation,attempt.worker_incarnation);
    assert_eq!(report.attempt_id,attempt.id);assert_eq!(report.task_id,attempt.task_id);
    assert!(matches!(report.observation,Observation::Started{thread_id:None,turn_id:None}));
    assert!(matches!(serde_json::from_value::<Outcome>(record.outcome.unwrap()).unwrap(),Outcome::Ok{..}));
    assert!(tokio::time::timeout(Duration::from_millis(100),lab.gate.accept()).await.is_err(),"exact duplicate launched twice");
    lab.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
//= spec/impl/v2/implementation.md#worker-commands-do-not-create-duplicate-execution
//# A worker MUST reject a launch addressed to an earlier incarnation.
// Scope: genuine controller command with only incarnation altered; next-poll
// processing barrier proves no local receipt/start/workspace before exact retry.
async fn stale_incarnation_launch_has_no_receipt_and_current_duplicate_executes_once() {
    Box::pin(rejected_launch_then_current_and_duplicate(Fault::StaleIncarnationLaunch,"stale-launch")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// Scope: foreign worker dispatch is refused before a local command receipt or
// workspace; this is not a claim about every possible workspace mutation path.
async fn wrong_worker_launch_has_no_receipt_and_current_duplicate_executes_once() {
    Box::pin(rejected_launch_then_current_and_duplicate(Fault::WrongWorkerLaunch,"foreign-launch")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
//= spec/impl/v2/implementation.md#worker-commands-do-not-create-duplicate-execution
//# A restarted worker MUST account for surviving prior-incarnation processes before accepting new work.
// Traceability scope: Actual worker SIGKILL and controlled executor; prior process cleanup, unchanged task identity and unresolved-effect retry refusal.
//= spec/impl/v2/implementation.md#loss-returns-to-the-last-checkpoint
//= type=test
//# The task record MUST distinguish preserved work from work whose outcome is unknown after worker loss.
async fn killed_worker_reaps_prior_executor_and_keeps_effects_uncertain() {
    let mut lab = Lab::new("worker-loss").await;
    let first = lab.submit("first", "agent-a").await;
    let second = lab.submit("second", "agent-b").await;
    let origin = lab.origin.clone();
    let wa = lab.start_worker("worker-a", "inc-1", &origin);
    lab.start_worker("worker-b", "inc-1", &origin);
    let one = lab.gate().await;
    let two = lab.gate().await;
    let (lost, healthy) = if one.observed["cwd"].as_str().unwrap().contains("/worker-a/") {
        (one, two)
    } else {
        (two, one)
    };
    let a = lab.get(&first.id).await;
    let b = lab.get(&second.id).await;
    let (lost_task, healthy_task) = if a.attempt.as_ref().unwrap().worker_id == "worker-a" {
        (a, b)
    } else {
        (b, a)
    };
    let old = lost_task.attempt.clone().unwrap();
    let old_pid = lost.observed["pid"].as_u64().unwrap();
    lab.workers[wa].kill();
    lab.start_worker("worker-a", "inc-2", &origin);
    let reconciled = lab
        .until(&lost_task.id, "old owner recovery", |t| {
            t.status == TaskStatus::Failed
                && t.attempt.as_ref().is_some_and(|a| a.stopped && a.uncertain)
        })
        .await;
    assert!(reconciled.result.is_none());
    let third = lab.submit("third", "agent-c").await;
    let next = lab.gate().await;
    assert!(
        next.observed["cwd"]
            .as_str()
            .unwrap()
            .contains("/worker-a/")
    );
    let assigned = lab.get(&third.id).await;
    assert_eq!(
        assigned.attempt.as_ref().unwrap().worker_incarnation,
        "inc-2"
    );
    let proc_state = fs::read_to_string(format!("/proc/{old_pid}/stat"))
        .ok()
        .and_then(|s| {
            s.rsplit_once(") ")
                .map(|(_, tail)| tail.chars().next().unwrap())
        });
    assert!(
        proc_state.is_none() || proc_state == Some('Z'),
        "old executor still running: {proc_state:?}"
    );
    drop(lost);
    assert_eq!(
        refused(
            lab.client
                .call(Operation::RegisterWorker {
                    operation_id: "retired-registration-check".into(),
                    worker: Worker {
                        id: "worker-a".into(),
                        incarnation: "inc-1".into(),
                        coding_capacity: 1,
                        native_capacity: 0
                    }
                })
                .await
                .unwrap()
        ),
        "stale_incarnation"
    );
    assert_eq!(
        refused(
            lab.client
                .call(stale_report(&old, "stale-prior-worker"))
                .await
                .unwrap()
        ),
        "stale_incarnation"
    );
    assert_eq!(
        refused(
            lab.client
                .call(Operation::RetryTask {
                    operation_id: "unsafe-retry".into(),
                    task_id: reconciled.id.clone(),
                    expected_revision: reconciled.revision
                })
                .await
                .unwrap()
        ),
        "reconciliation_required"
    );
    next.release().await;
    healthy.release().await;
    lab.successful(&third.id).await;
    lab.successful(&healthy_task.id).await;
    lab.restart_controller().await;
    let after = lab.get(&lost_task.id).await;
    assert_eq!(after.status, TaskStatus::Failed);
    assert!(after.attempt.unwrap().uncertain);
    lab.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
//= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
//# A terminal task result MUST be committed before it is reported as final.
//= spec/impl/v2/implementation.md#task-state-separates-execution-from-acceptance
//# A retry MUST create a new attempt without changing the task identity.
//= spec/impl/v2/implementation.md#checkpoint-publication-is-reconciled
//# The publication record MUST identify the repository, exact commit and intended remote reference.
//= spec/impl/v2/implementation.md#work-starts-from-a-durable-source
//# A checkpoint reference MUST be committed to the task record before recovery selects it.
//= spec/impl/v2/implementation.md#interrupted-work-is-reconciled
//# A recovery attempt MUST preserve the original task identity.
// Traceability scope: Actual local bare-Git and controlled acknowledgment loss, not hosted provider availability or every transport-fault cut.
//= spec/impl/v2/implementation.md#checkpoint-publication-is-reconciled
//= type=test
//# A publication with an uncertain outcome MUST be reconciled against its original remote reference and commit.
// Traceability scope: Real local controller/workers/Git, controlled Codex peer; lost settlement reply finishes the original attempt; separate cancelled check plus fresh processes reconstruct the selected commit.
//= spec/impl/v2/implementation.md#interrupted-work-is-reconciled
//= type=test
//# A retry MUST preserve the original external-operation identity where the receiving interface supports deduplication.
// Traceability scope: Real local controller/workers/Git, controlled Codex peer; lost settlement reply finishes the original attempt; separate cancelled check plus fresh processes reconstruct the selected commit.
//= spec/impl/v2/implementation.md#interrupted-work-is-reconciled
//= type=test
//# An interrupted attempt MUST retain its last committed status and checkpoint references.
// Traceability scope: Real local controller/workers/Git, controlled Codex peer; lost settlement reply finishes the original attempt; separate cancelled check plus fresh processes reconstruct the selected commit.
//= spec/impl/v2/implementation.md#interrupted-work-is-reconciled
//= type=test
//# Recovery that cannot resume available saved executor history MUST create a new attempt.
async fn checkpoint_ack_loss_replays_exact_publication_and_fences_old_attempt() {
    let mut lab = Lab::new("checkpoint-loss").await;
    let mut proxy = Proxy::new(
        lab.origin.clone(),
        Fault::DropSettlement,
        lab.root.clone(),
        lab.limits.transport.body_bytes,
    )
    .await;
    let one = lab.submit("one", "agent-a").await;
    let two = lab.submit("two", "agent-b").await;
    lab.start_worker("worker-a", "inc-1", &proxy.url);
    lab.start_worker("worker-b", "inc-1", &proxy.url);
    lab.gate().await.release().await;
    lab.gate().await.release().await;
    let dropped = proxy.event().await;
    let (task_id, cp) = match &dropped.original.command {
        Operation::SettleOperation {
            task_id,
            checkpoint: Some(cp),
            ..
        } => (task_id.clone(), cp.clone()),
        x => panic!("wrong fault {x:?}"),
    };
    let accepted_original = lab.successful(&task_id).await;
    assert_eq!(accepted_original.selected_checkpoint, Some(cp.clone()));
    assert_eq!(
        lab.starts.len(),
        2,
        "lost settlement ACK must not repeat either executor turn"
    );
    lab.successful(if task_id == one.id { &two.id } else { &one.id })
        .await;
    assert_eq!(
        git(
            &lab.root,
            &["--git-dir", &cp.repository, "rev-parse", &cp.remote_ref]
        ),
        cp.commit
    );
    lab.restart_controller().await;
    let observed = lab
        .client
        .call(Operation::GetOperation {
            operation_id: cp.operation_id.clone(),
        })
        .await
        .unwrap();
    assert_eq!(observed.outcome, dropped.reply.outcome);
    assert_eq!(
        lab.client
            .call(dropped.original.command.clone())
            .await
            .unwrap()
            .outcome,
        dropped.reply.outcome
    );
    let mut conflict = dropped.original.command.clone();
    if let Operation::SettleOperation {
        checkpoint: Some(ref mut other),
        ..
    } = conflict
    {
        other.commit = lab.repository.start_commit.clone();
    }
    assert_eq!(
        refused(lab.client.call(conflict).await.unwrap()),
        "operation_conflict"
    );
    // Independent genuine interruption after a durable checkpoint preserves
    // the previous retry/source-reconstruction oracle. Transport loss alone
    // now reconciles successfully and must no longer manufacture failure.
    let check_gate = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = check_gate.local_addr().unwrap();
    let mut interrupted_input = lab.input("agent-interrupted");
    interrupted_input.acceptance_commands.push(vec![
        "/usr/bin/python3".into(), "-c".into(),
        format!("import socket; s=socket.create_connection(('127.0.0.1',{})); s.sendall(b'R'); assert s.recv(1)==b'G'", address.port()),
    ]);
    let interrupted = task(
        lab.client
            .call(Operation::SubmitTask {
                operation_id: "independent-check-interruption".into(),
                input: interrupted_input,
            })
            .await
            .unwrap(),
    );
    lab.gate().await.release().await;
    let (mut held_check, _) = tokio::time::timeout(TIMEOUT, check_gate.accept())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        tokio::time::timeout(TIMEOUT, held_check.read_u8())
            .await
            .unwrap()
            .unwrap(),
        b'R'
    );
    let before_cancel = lab.get(&interrupted.id).await;
    let recovery_checkpoint = before_cancel
        .selected_checkpoint
        .clone()
        .expect("check runs only after publication settlement");
    let old = before_cancel.attempt.clone().unwrap();
    task(
        lab.client
            .call(Operation::CancelTask {
                operation_id: "interrupt-actual-check".into(),
                task_id: interrupted.id.clone(),
                expected_revision: before_cancel.revision,
            })
            .await
            .unwrap(),
    );
    let current = lab
        .until(
            &interrupted.id,
            "actual acceptance process interruption",
            |t| {
                t.status == TaskStatus::Cancelled
                    && t.attempt
                        .as_ref()
                        .is_some_and(|a| a.stopped && !a.uncertain)
            },
        )
        .await;
    assert_eq!(
        current.selected_checkpoint,
        Some(recovery_checkpoint.clone())
    );
    assert_eq!(
        tokio::time::timeout(TIMEOUT, held_check.read(&mut [0u8; 1]))
            .await
            .unwrap()
            .unwrap(),
        0,
        "cancelled acceptance process must close its real socket"
    );
    for worker in &mut lab.workers {
        worker.stop();
    }
    lab.restart_controller().await;
    let ready = lab.get(&current.id).await;
    assert_eq!(ready.selected_checkpoint, Some(recovery_checkpoint.clone()));
    assert_eq!(ready.status, TaskStatus::Cancelled);
    assert!(
        ready
            .attempt
            .as_ref()
            .is_some_and(|a| a.stopped && !a.uncertain)
    );
    assert!(ready.revision > before_cancel.revision);
    // A stopped snapshot is not a settled revision: the worker can still
    // publish its acceptance-refusal notice during orderly shutdown. Keep a
    // deterministic stale-CAS witness, including exact replay, at this stable
    // gate; the original observed Stop/Progress race remains in its receipt.
    let stale_retry = Operation::RetryTask {
        operation_id: "checkpoint-retry-stale".into(),
        task_id: ready.id.clone(),
        expected_revision: before_cancel.revision,
    };
    let refused_retry = lab.client.call(stale_retry.clone()).await.unwrap();
    assert_eq!(refused(refused_retry.clone()), "stale_revision");
    assert_eq!(
        lab.client.call(stale_retry).await.unwrap().outcome,
        refused_retry.outcome
    );
    assert_eq!(
        lab.get(&ready.id).await,
        ready,
        "stale retry must not change the task"
    );
    task(
        lab.client
            .call(Operation::RetryTask {
                operation_id: "checkpoint-retry-after-restart".into(),
                task_id: ready.id.clone(),
                expected_revision: ready.revision,
            })
            .await
            .unwrap(),
    );
    let origin = lab.origin.clone();
    lab.start_worker("worker-a", "inc-2", &origin);
    let gate = lab.gate().await;
    assert_eq!(
        gate.observed["before"], "after\n",
        "retry must start at acknowledged remote checkpoint"
    );
    let running = lab.get(&current.id).await;
    assert!(running.attempt.as_ref().unwrap().generation > old.generation);
    assert_eq!(running.session_id, current.session_id);
    assert_eq!(running.session_id, old.session_id);
    assert_eq!(
        running.attempt.as_ref().unwrap().session_id,
        current.session_id
    );
    assert_eq!(
        refused(
            lab.client
                .call(stale_report(&old, "old-attempt-report"))
                .await
                .unwrap()
        ),
        "stale_generation"
    );
    assert_eq!(
        lab.client
            .call(dropped.original.command)
            .await
            .unwrap()
            .outcome,
        dropped.reply.outcome
    );
    assert_eq!(lab.get(&current.id).await.attempt, running.attempt);
    gate.release().await;
    let (mut resumed_check, _) = tokio::time::timeout(TIMEOUT, check_gate.accept())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        tokio::time::timeout(TIMEOUT, resumed_check.read_u8())
            .await
            .unwrap()
            .unwrap(),
        b'R'
    );
    resumed_check.write_all(b"G").await.unwrap();
    let accepted = lab.successful(&current.id).await;
    assert_eq!(
        accepted.selected_checkpoint.as_ref().unwrap().commit,
        recovery_checkpoint.commit
    );
    assert_eq!(
        git(
            &lab.root,
            &["--git-dir", &cp.repository, "rev-parse", &cp.remote_ref]
        ),
        cp.commit
    );
    lab.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registration_and_command_ack_loss_reconcile_before_fresh_incarnation() {
    let mut lab = Lab::new("lifecycle-mutation-loss").await;
    let registration_evidence = lab.root.join("registration-fault");
    let await_evidence = lab.root.join("await-fault");
    fs::create_dir(&registration_evidence).unwrap();
    fs::create_dir(&await_evidence).unwrap();
    let mut registration = Proxy::new(
        lab.origin.clone(),
        Fault::DropRegistration,
        registration_evidence,
        lab.limits.transport.body_bytes,
    )
    .await;
    let mut commands = Proxy::new(
        registration.url.clone(),
        Fault::DropAwait,
        await_evidence,
        lab.limits.transport.body_bytes,
    )
    .await;
    let first = lab.submit("lifecycle-one", "agent-one").await;
    let second = lab.submit("lifecycle-two", "agent-two").await;
    lab.start_worker("worker-a", "inc-1", &commands.url);
    let lost_registration = registration.event().await;
    let lost_commands = commands.event().await;
    let register_id = match &lost_registration.original.command {
        Operation::RegisterWorker { operation_id, .. } => operation_id.clone(),
        other => panic!("{other:?}"),
    };
    let await_id = match &lost_commands.original.command {
        Operation::AwaitCommands { operation_id, .. } => operation_id.clone(),
        other => panic!("{other:?}"),
    };
    assert!(
        matches!(&lost_commands.reply.outcome, Outcome::Ok { data: Data::Commands(batch) } if batch.iter().filter(|c|matches!(c,WorkerCommand::RunAttempt {..})).count()==1)
    );
    lab.gate().await.release().await;
    lab.gate().await.release().await;
    let accepted_first = lab.successful(&first.id).await;
    let accepted_second = lab.successful(&second.id).await;
    assert_eq!(
        lab.starts.len(),
        2,
        "neither lost mutation reply may repeat execution"
    );
    for (id, request, outcome) in [
        (
            &register_id,
            &lost_registration.original.command,
            &lost_registration.reply.outcome,
        ),
        (
            &await_id,
            &lost_commands.original.command,
            &lost_commands.reply.outcome,
        ),
    ] {
        let answer = lab
            .client
            .call(Operation::GetOperation {
                operation_id: id.clone(),
            })
            .await
            .unwrap();
        assert!(
            matches!(answer.outcome, Outcome::Ok { data: Data::Operation(ref record) }
            if record.intended == serde_json::to_value(request).unwrap() && record.outcome == Some(serde_json::to_value(outcome).unwrap()))
        );
    }
    let state = lab
        .root
        .join("worker-a/.borg-worker")
        .join(borg_runtime::worker::process::component("worker-a"));
    let wait_path = state.join("mutations/await.json");
    let until = Instant::now() + TIMEOUT;
    let pending = loop {
        if let Ok(bytes) = fs::read(&wait_path) {
            if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
                if value["outcome"].is_null() && value["consumed"] == false {
                    break value;
                }
            }
        }
        assert!(
            Instant::now() < until,
            "worker did not retain an idle pending wait"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    save(lab.root.join("idle-pending-before-restart.json"), &pending);
    let original_wait: Operation = serde_json::from_value(pending["request"].clone()).unwrap();
    let Operation::AwaitCommands {
        operation_id: pending_id,
        incarnation,
        ..
    } = &original_wait
    else {
        panic!()
    };
    assert_eq!(incarnation, "inc-1");
    for process in &mut lab.workers {
        process.stop();
    }
    lab.restart_controller().await;
    let origin = lab.origin.clone();
    lab.start_worker("worker-a", "inc-2", &origin);
    let until = Instant::now() + TIMEOUT;
    loop {
        let answer = lab
            .client
            .call(Operation::GetOperation {
                operation_id: pending_id.clone(),
            })
            .await
            .unwrap();
        if let Outcome::Ok {
            data: Data::Operation(record),
        } = answer.outcome
        {
            assert_eq!(
                record.intended,
                serde_json::to_value(&original_wait).unwrap()
            );
            let outcome: Outcome = serde_json::from_value(record.outcome.unwrap()).unwrap();
            assert!(matches!(outcome, Outcome::Refused { code, .. } if code=="stale_incarnation"));
            break;
        }
        assert!(
            Instant::now() < until,
            "retired pending wait was not reconciled under its original ID"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(lab.get(&first.id).await.result, accepted_first.result);
    assert_eq!(lab.get(&second.id).await.result, accepted_second.result);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), lab.gate.accept())
            .await
            .is_err(),
        "historical mutation receipts launched another executor"
    );
    lab.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// Traceability scope: Pending/refused original preparation leaves the exact Git ref absent; the accepted preparation publishes it. Controlled local Git and executor boundary.
//= spec/impl/v2/implementation.md#checkpoint-publication-is-reconciled
//= type=test
//# A checkpoint publication MUST have a durable operation record before it is sent.
async fn checkpoint_publication_waits_for_durable_prepare_before_git_push() {
    for (label, mode) in [
        ("prepare-pending", Fault::PendingPrepare),
        ("prepare-refused", Fault::RefusePrepare),
    ] {
        let mut lab = Lab::new(label).await;
        let mut proxy = Proxy::new(
            lab.origin.clone(),
            mode,
            lab.root.clone(),
            lab.limits.transport.body_bytes,
        )
        .await;
        let submitted = lab.submit("blocked-publication", "blocked-agent").await;
        lab.start_worker("worker-a", "inc-1", &proxy.url);
        lab.gate().await.release().await;
        let mut held = proxy.event().await;
        let (operation_id, attempt_id, commit, remote_ref) = match &held.original.command {
            Operation::PrepareOperation {
                operation_id,
                attempt_id,
                intended,
                kind,
                ..
            } => {
                assert_eq!(kind, "checkpoint");
                assert_eq!(intended["registration"], serde_json::to_value(lab.repository.registration.as_ref().unwrap()).unwrap());
                assert_eq!(intended["repository"], lab.repository.checkpoint_remote);
                (
                    operation_id.clone(),
                    attempt_id.clone(),
                    intended["commit"].as_str().unwrap().to_owned(),
                    intended["remote_ref"].as_str().unwrap().to_owned(),
                )
            }
            other => panic!("unexpected held request {other:?}"),
        };
        assert!(held.reply.commit.is_none());
        let workspace = lab
            .root
            .join("worker-a/workspaces")
            .join(borg_runtime::worker::process::component(&attempt_id));
        assert_eq!(
            git(&workspace, &["rev-parse", "HEAD"]),
            commit,
            "actual local commit must precede preparation"
        );
        assert_ne!(commit, lab.repository.start_commit);
        let remote_absent = || {
            let status = Command::new("/usr/bin/git")
                .args([
                    "--git-dir",
                    &lab.repository.checkpoint_remote,
                    "show-ref",
                    "--verify",
                    "--quiet",
                    &remote_ref,
                ])
                .status()
                .unwrap();
            assert_eq!(
                status.code(),
                Some(1),
                "blocked PrepareOperation must not publish its exact remote ref"
            );
        };
        remote_absent();
        assert!(
            matches!(lab.client.call(Operation::GetOperation { operation_id: operation_id.clone() }).await.unwrap().outcome, Outcome::Refused { code, .. } if code=="not_found")
        );
        held.release.take().unwrap().send(()).unwrap();
        let failed = lab
            .until(
                &submitted.id,
                "unaccepted preparation stops without publication",
                |task| {
                    task.status == TaskStatus::Failed
                        && task.attempt.as_ref().is_some_and(|a| a.stopped)
                },
            )
            .await;
        assert!(failed.selected_checkpoint.is_none());
        assert!(failed.result.is_none());
        remote_absent();
        assert_eq!(proxy.state.lock().unwrap().settlements, 0);
        assert!(
            !lab.root
                .join("worker-a/validation")
                .join(borg_runtime::worker::process::component(&attempt_id))
                .exists()
        );
        assert_eq!(lab.starts.len(), 1);
        // Same executable, worker and writable destination: an accepted real
        // preparation must reach Git and publish its distinct exact ref.
        let positive = lab.submit("accepted-publication", "positive-agent").await;
        lab.gate().await.release().await;
        let accepted = lab.successful(&positive.id).await;
        let checkpoint = accepted.selected_checkpoint.as_ref().unwrap();
        assert_eq!(
            git(
                &lab.root,
                &[
                    "--git-dir",
                    &checkpoint.repository,
                    "rev-parse",
                    &checkpoint.remote_ref
                ]
            ),
            checkpoint.commit
        );
        assert!(proxy.state.lock().unwrap().settlements > 0);
        assert_eq!(lab.starts.len(), 2);
        save(
            lab.root.join("publication-guard.json"),
            &json!({"blocked_prepare":held.original,"uncommitted_reply":held.reply,"blocked_task":failed,"accepted_control":accepted,"blocked_remote_ref":remote_ref,"observed_blocked_ref_absent":true}),
        );
        lab.close();
    }
}

/// Exactly two controlled HTTP responses; no provider or executor credentials.
struct NativePeer {
    endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
    join: tokio::task::JoinHandle<()>,
}
impl NativePeer {
    async fn start(evidence: PathBuf, request_bytes: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
        let observed = requests.clone();
        let join = tokio::spawn(async move {
            for step in 0..2 {
                let (socket, _) = tokio::time::timeout(TIMEOUT, listener.accept())
                    .await
                    .unwrap()
                    .unwrap();
                let mut stream = BufReader::new(socket);
                let mut head = String::new();
                loop {
                    let mut line = String::new();
                    assert!(
                        tokio::time::timeout(TIMEOUT, stream.read_line(&mut line))
                            .await
                            .unwrap()
                            .unwrap()
                            > 0
                    );
                    head.push_str(&line);
                    assert!(head.len() < 16 * 1024);
                    if line == "\r\n" {
                        break;
                    }
                }
                assert!(head.starts_with("POST /v1/responses HTTP/1.1\r\n"));
                assert!(
                    head.to_ascii_lowercase()
                        .contains("authorization: bearer controlled-native-fixture-key\r\n")
                );
                let len: usize = head
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                assert!(len <= request_bytes);
                let mut body = vec![0; len];
                tokio::time::timeout(TIMEOUT, stream.read_exact(&mut body))
                    .await
                    .unwrap()
                    .unwrap();
                let request: Value = serde_json::from_slice(&body).unwrap();
                assert_eq!(request["model"], "gpt-6-astra");
                assert_eq!(request["reasoning"], serde_json::json!({"effort":"max"}));
                assert_eq!(request["store"], false);
                let instruction = request["input"][0]["content"].as_str().unwrap();
                let result = if step == 0 {
                    assert!(instruction.starts_with("Use the configured coding task"));
                    json!({"instruction":"Set answer.txt to after and preserve the configured Rust tests."})
                } else {
                    assert!(
                        instruction.starts_with(borg_runtime::delegation::CONTINUATION_INSTRUCTION)
                    );
                    let encoded = instruction
                        .rsplit_once(borg_runtime::delegation::CHILD_OUTCOME_SEPARATOR)
                        .unwrap()
                        .1;
                    let child: ChildOutcome = serde_json::from_str(encoded).unwrap();
                    assert_eq!(child.status, TaskStatus::Succeeded);
                    let accepted = child.result.as_ref().unwrap();
                    assert!(accepted.checkpoint.is_some());
                    assert_eq!(accepted.validation.len(), 3);
                    save(evidence.join("native-observed-child.json"), &child);
                    json!({"answer":"child_succeeded"})
                };
                {
                    let mut list = observed.lock().unwrap();
                    list.push(request.clone());
                    save(evidence.join("native-requests.json"), &*list);
                }
                assert_eq!(request["tool_choice"], "auto");
                assert_eq!(request["parallel_tool_calls"], false);
                assert!(
                    request["tools"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|t| t["name"] == "collab_task_delegate")
                );
                let output = if step == 0 {
                    json!([{"type":"function_call","id":"fc_child","call_id":"call_child","name":"collab_task_delegate","arguments":result.to_string(),"status":"completed"}])
                } else {
                    json!([{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":result.to_string()}]}])
                };
                let envelope = json!({"status":"completed","usage":{"input_tokens":17,"output_tokens":9,"total_tokens":26},"output":output});
                let response = serde_json::to_vec(&envelope).unwrap();
                stream.get_mut().write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", response.len()).as_bytes()).await.unwrap();
                stream.get_mut().write_all(&response).await.unwrap();
                stream.get_mut().flush().await.unwrap();
            }
        });
        Self {
            endpoint,
            requests,
            join,
        }
    }
}
impl Drop for NativePeer {
    fn drop(&mut self) {
        self.join.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// Traceability scope: Actual controller/worker restart with controlled native/Codex peers retains the accepted deadline; this fixture does not qualify real provider or supplied-executor behavior.
//= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
//= type=test
//# Reconnecting or resuming an attempt MUST NOT reset its execution deadline.
async fn native_coding_continuation_survives_lost_delegation_reply_and_fresh_processes() {
    let mut lab = Lab::new("native-coding-continuation").await;
    let seed = lab.root.join("seed");
    fs::create_dir(seed.join("src")).unwrap();
    fs::write(seed.join("src/main.rs"), "fn main() { println!(\"{}\", include_str!(\"../answer.txt\").trim()); }\n#[test] fn corrected_source() { assert_eq!(include_str!(\"../answer.txt\").trim(), \"after\"); }\n").unwrap();
    git(&seed, &["add", "src/main.rs"]);
    git(
        &seed,
        &["commit", "-qm", "controlled Rust acceptance source"],
    );
    lab.repository.start_commit = git(&seed, &["rev-parse", "HEAD"]);
    git(
        &seed,
        &[
            "push",
            "--quiet",
            &lab.repository.url,
            "HEAD:refs/heads/fixture-source",
        ],
    );
    let rustc = std::env::var("RUSTC").expect("selected RUSTC from build environment");
    assert!(Path::new(&rustc).is_absolute());
    let mut peer =
        NativePeer::start(lab.root.clone(), lab.limits.execution.native.request_bytes).await;
    let mut proxy = Proxy::new(
        lab.origin.clone(),
        Fault::DropDelegation,
        lab.root.clone(),
        lab.limits.transport.body_bytes,
    )
    .await;
    let deadline = now() + 120_000;
    let input = TaskInput { instruction_source: None,
        pull_request: None,
        resolved_limits: None,
        instruction: "Use the configured coding task to correct the source, then report its accepted outcome.".into(),
        definition: Definition { charter: None, limit_overrides: None, inference:Some(InferenceSettings::selected()), execution_profile:Some(ExecutionProfile::CollaborationV1 { web_search:None }), id: "native-parent".into(), revision: "1".into(), executor: ExecutorKind::Native }, repository: None, repository_read: None, acceptance_commands: vec![],
        output_schema: Some(json!({"type":"object","required":["answer"],"properties":{"answer":{"type":"string"}},"additionalProperties":false})),
        limits: Limits { deadline_unix_ms: deadline, model_requests: 3, output_bytes: 4096, result_bytes: lab.limits.task_defaults.result_bytes, max_delegation_depth: 2 },
        dependencies: vec![],
        native_workflow: Some(Box::new(NativeWorkflow::CodingThenContinue {
            child: CodingTaskTemplate { instruction_source: None,
                pull_request: None,
                instruction_prefix: "Only the configured source and checks are in this task.".into(),
                definition: Definition { charter: None, limit_overrides: None, inference:Some(InferenceSettings::selected()), execution_profile:Some(ExecutionProfile::CollaborationV1 { web_search:None }), id: "coding-child".into(), revision: "1".into(), executor: ExecutorKind::Coding }, repository: lab.repository_selector(), dependencies: vec![],
                acceptance_commands: vec![vec![rustc.clone(), "--test".into(), "src/main.rs".into(), "-o".into(), "acceptance".into()], vec!["./acceptance".into()], vec![rustc, "src/main.rs".into(), "-o".into(), "demo".into()]],
                limits: Limits { deadline_unix_ms: deadline, model_requests: 1, output_bytes: 4096, result_bytes: 8192, max_delegation_depth: 0 },
            },
        })),
    };
    let parent = task(
        lab.client
            .call(Operation::SubmitTask {
                operation_id: "native-parent-submit".into(),
                input,
            })
            .await
            .unwrap(),
    );
    let direct = lab.origin.clone();
    lab.start_worker("coding-worker", "coding-inc-1", &direct);
    let native = lab.start_native_worker("native-inc-1", &proxy.url, &peer.endpoint);
    let dropped = proxy.event().await;
    assert!(matches!(
        dropped.original.command,
        Operation::CallCollaboration { ref call } if call.tool=="collab_task_delegate"
    ));
    let child = task(dropped.reply.clone());
    let gate = lab.gate().await;
    let waiting = lab
        .until(&parent.id, "parent quiescent with durable child", |t| {
            t.waiting_child_id.as_deref() == Some(&child.id)
                && t.attempt.as_ref().is_some_and(|a| a.stopped)
        })
        .await;
    assert_eq!(waiting.status, TaskStatus::Active);
    assert_eq!(waiting.session_id, parent.session_id);
    assert_eq!(
        waiting.attempt.as_ref().unwrap().session_id,
        parent.session_id
    );
    assert_ne!(child.session_id, parent.session_id);
    assert_eq!(peer.requests.lock().unwrap().len(), 1);
    assert_eq!(child.parent_task_id.as_deref(), Some(parent.id.as_str()));
    assert_eq!(
        task(
            lab.client
                .call(dropped.original.command.clone())
                .await
                .unwrap()
        )
        .id,
        child.id
    );
    // Stop the already-quiescent native worker while the child is held. Kill
    // the controller before releasing the child, so its old long-poll handler
    // cannot assign a continuation to the old incarnation after worker exit.
    lab.workers[native].stop();
    lab.restart_controller().await;
    lab.start_native_worker("native-inc-2", &proxy.url, &peer.endpoint);
    gate.release().await;
    let accepted = lab
        .until(&parent.id, "native continuation accepted", |t| {
            t.status == TaskStatus::Succeeded && t.attempt.as_ref().is_some_and(|a| a.stopped)
        })
        .await;
    assert_eq!(
        accepted.result.as_ref().unwrap().output,
        Some(json!({"answer":"child_succeeded"}))
    );
    assert_ne!(
        accepted.attempt.as_ref().unwrap().id,
        waiting.attempt.as_ref().unwrap().id
    );
    assert_eq!(accepted.input.limits.deadline_unix_ms, deadline);
    assert_eq!(accepted.session_id, parent.session_id);
    assert_eq!(
        accepted.attempt.as_ref().unwrap().session_id,
        parent.session_id
    );
    assert_eq!(
        accepted.attempt.as_ref().unwrap().worker_incarnation,
        "native-inc-2"
    );
    let child_accepted = lab.successful(&child.id).await;
    assert_eq!(child_accepted.session_id, child.session_id);
    assert_eq!(
        child_accepted.attempt.as_ref().unwrap().session_id,
        child.session_id
    );
    assert_eq!(
        accepted.completed_child.as_ref().unwrap().result,
        child_accepted.result
    );
    assert_eq!(lab.starts.len(), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(150), lab.gate.accept())
            .await
            .is_err()
    );
    tokio::time::timeout(TIMEOUT, &mut peer.join)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(peer.requests.lock().unwrap().len(), 2);
    let after_replay = task(lab.client.call(dropped.original.command).await.unwrap());
    assert_eq!(after_replay.id, child.id);
    assert_eq!(lab.get(&parent.id).await, accepted);
    let count = lab
        .client
        .call(Operation::ListTasks {
            after: None,
            limit: 20,
        })
        .await
        .unwrap();
    assert!(
        matches!(count.outcome, Outcome::Ok { data: Data::Tasks(ref tasks) } if tasks.len() == 2)
    );
    lab.restart_controller().await;
    assert_eq!(lab.get(&parent.id).await, accepted);
    assert_eq!(lab.get(&child.id).await, child_accepted);
    save(lab.root.join("native-parent-accepted.json"), &accepted);
    lab.close();
}

// This subprocess fixture runs the actual worker consumer. It supplies only a
// trusted retained validator image and a synthetic local credential. No task or
// environment field in the application can activate the debug hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "invoked only by native_helper_hard_loss_reconciles_before_new_work"]
async fn native_worker_held_validation_fixture() {
    use borg_runtime::{
        native,
        worker::{self, WorkerConfig},
    };
    let root = PathBuf::from(std::env::var_os("BORG_RECOVERY_FIXTURE_ROOT").unwrap());
    let image = root.join("borg-runtime");
    let ready = native::qualification_hold_next_validation(&image);
    let config = WorkerConfig {
        limits: Arc::new(RuntimeLimits::load(&root.join("native-worker-limits.json")).unwrap()),
        resource_profile: None,
        controller_url: std::env::var("BORG_RECOVERY_CONTROLLER").unwrap(),
        id: "native-worker".into(),
        incarnation: "held-inc-1".into(),
        workspace_root: root.join("native-worker"),
        codex_path: root.join("codex_protocol.py"),
        validation_executable: image,
        coding_capacity: 0,
        native_capacity: 1,
        native_endpoint: std::env::var("BORG_RECOVERY_ENDPOINT").unwrap(),
    };
    let worker = tokio::spawn(worker::run(config));
    let pid = tokio::time::timeout(TIMEOUT, ready).await.unwrap().unwrap();
    let mut stream = TcpStream::connect(std::env::var("BORG_RECOVERY_READY").unwrap())
        .await
        .unwrap();
    stream
        .write_all(format!("{}\n", json!({"helper_pid":pid})).as_bytes())
        .await
        .unwrap();
    // Keep the owning worker alive until the test injects abrupt process loss.
    worker.await.unwrap().unwrap();
}

struct SimpleNativePeer {
    endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
    join: tokio::task::JoinHandle<()>,
}
impl SimpleNativePeer {
    async fn start(evidence: PathBuf, request_bytes: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(vec![]));
        let observed = requests.clone();
        let join = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted=listener.accept()=>{
                        let (stream,_)=accepted.unwrap();
                        let (observed,evidence)=(observed.clone(),evidence.clone());
                        connections.spawn(async move {
                            let service=service_fn(move |request:HttpRequest<Incoming>| {
                                let (observed,evidence)=(observed.clone(),evidence.clone());
                                async move {
                                    assert_eq!(request.uri().path(),"/v1/responses");
                                    assert_eq!(request.headers()["authorization"],"Bearer controlled-native-fixture-key");
                                    let bytes=request.into_body().collect().await.unwrap().to_bytes();
                                    assert!(bytes.len()<=request_bytes);
                                    let request:Value=serde_json::from_slice(&bytes).unwrap();
                                    let mut requests=observed.lock().unwrap();
                                    requests.push(request);save(evidence.join("simple-native-requests.json"),&*requests);
                                    let envelope=json!({"status":"completed","usage":{"input_tokens":5,"output_tokens":1,"total_tokens":6},"output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"7"}]}]});
                                    Ok::<_,std::convert::Infallible>(HttpResponse::new(Full::new(Bytes::from(serde_json::to_vec(&envelope).unwrap()))))
                                }
                            });
                            let _=hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream),service).await;
                        });
                    }
                    _=connections.join_next(),if !connections.is_empty()=>{}
                }
            }
        });
        Self {
            endpoint,
            requests,
            join,
        }
    }
}
impl Drop for SimpleNativePeer {
    fn drop(&mut self) {
        self.join.abort();
    }
}

fn observed_process(pid: u32) -> Option<(u64, char)> {
    let text = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields = text
        .rsplit_once(") ")?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    Some((
        fields.get(19)?.parse().ok()?,
        fields.first()?.chars().next()?,
    ))
}
// Test-only emergency cleanup of the exact held helper, preserving the failed
// receipt if application recovery does not stop it. This cannot make a test pass.
struct HeldHelper {
    pid: u32,
    start: u64,
}
impl HeldHelper {
    fn live(&self) -> bool {
        observed_process(self.pid)
            .is_some_and(|(start, state)| start == self.start && state != 'Z' && state != 'X')
    }
}
impl Drop for HeldHelper {
    fn drop(&mut self) {
        if self.live() {
            let _ = Command::new("/bin/kill")
                .args(["-KILL", "--", &self.pid.to_string()])
                .status();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_helper_hard_loss_reconciles_before_new_work() {
    //= spec/impl/v2/implementation.md#worker-commands-do-not-create-duplicate-execution
    //# A restarted worker MUST account for surviving prior-incarnation processes before accepting new work.
    // Exact scope: owned native validation process after hard worker loss; this
    // does not settle a remote request, enforce all host resources or prove scale.
    // The ignored helper binds its validation hold to this private case path.
    let mut lab = Lab::new_with_private_image("native-helper-hard-loss", true).await;
    let peer =
        SimpleNativePeer::start(lab.root.clone(), lab.limits.execution.native.request_bytes).await;
    let input = |agent: &str| TaskInput { instruction_source: None,
        pull_request: None,
        resolved_limits: None,
        instruction: "Return the integer seven.".into(),
        definition: Definition {
            charter: None,
            limit_overrides: None,
            inference: Some(InferenceSettings::selected()),
            execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
            id: agent.into(),
            revision: "1".into(),
            executor: ExecutorKind::Native,
        },
        repository: None,
        repository_read: None,
        acceptance_commands: vec![],
        output_schema: Some(json!({"type":"integer","const":7})),
        limits: Limits {
            deadline_unix_ms: now() + 120_000,
            model_requests: 1,
            output_bytes: 1024,
            result_bytes: 8192,
            max_delegation_depth: 0,
        },
        dependencies: vec![],
        native_workflow: None,
    };
    let first = task(
        lab.client
            .call(Operation::SubmitTask {
                operation_id: "interrupted-native".into(),
                input: input("held-native"),
            })
            .await
            .unwrap(),
    );
    let log = File::create(lab.root.join("held-worker.log")).unwrap();
    let test_image = lab.root.join("recovery-test-image");
    fs::copy(std::env::current_exe().unwrap(), &test_image).unwrap();
    let hash = Command::new("/usr/bin/sha256sum")
        .arg(&test_image)
        .output()
        .unwrap();
    assert!(hash.status.success());
    fs::write(lab.root.join("recovery-test-image.sha256"), hash.stdout).unwrap();
    let child = Command::new(test_image)
        .args([
            "--ignored",
            "--exact",
            "native_worker_held_validation_fixture",
            "--nocapture",
        ])
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("LANG", "C")
        .env("OPENAI_API_KEY", "controlled-native-fixture-key")
        .env("BORG_RECOVERY_FIXTURE_ROOT", &lab.root)
        .env("BORG_RECOVERY_CONTROLLER", &lab.origin)
        .env("BORG_RECOVERY_ENDPOINT", &peer.endpoint)
        .env(
            "BORG_RECOVERY_READY",
            lab.gate.local_addr().unwrap().to_string(),
        )
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    lab.workers.push(Process {
        child,
        name: "held-native-worker".into(),
        evidence: lab.root.clone(),
    });
    let signal = lab.gate().await;
    let pid = u32::try_from(signal.observed["helper_pid"].as_u64().unwrap()).unwrap();
    let helper = HeldHelper {
        pid,
        start: observed_process(pid).expect("held helper is real").0,
    };
    assert!(helper.live());
    let active = lab.get(&first.id).await;
    let original = active
        .attempt
        .clone()
        .expect("actual worker admitted original task");
    assert!(!original.stopped);
    let state = lab
        .root
        .join("native-worker/.borg-worker")
        .join(borg_runtime::worker::process::component("native-worker"));
    let registry = state
        .join("processes")
        .join(borg_runtime::worker::process::component(&original.id));
    let ownership = registry.join(format!("{pid}.json"));
    let record: Value = serde_json::from_slice(&fs::read(&ownership).unwrap()).unwrap();
    assert_eq!(record["pid"], pid);
    assert_eq!(record["start_ticks"], helper.start);
    save(lab.root.join("held-helper-owned.json"), &record);
    assert!(
        peer.requests.lock().unwrap().is_empty(),
        "first schema helper precedes any HTTP request"
    );
    lab.workers[0].kill();
    assert!(
        helper.live(),
        "hard loss must leave the actual held helper for restart to account for"
    );
    let origin = lab.origin.clone();
    lab.start_native_worker("recovered-inc-2", &origin, &peer.endpoint);
    let recovered = lab
        .until(
            &first.id,
            "tracked prior execution stopped but outcome still uncertain",
            |t| t.attempt.as_ref().is_some_and(|a| a.stopped && a.uncertain),
        )
        .await;
    assert!(
        !helper.live(),
        "application restart, not test cleanup, must stop the helper"
    );
    assert!(
        !ownership.exists(),
        "only observed cleanup removes the owned record"
    );
    assert!(recovered.result.is_none());
    assert_eq!(recovered.attempt.as_ref().unwrap().id, original.id);
    let retry = lab
        .client
        .call(Operation::RetryTask {
            operation_id: "uncertain-no-retry".into(),
            task_id: first.id.clone(),
            expected_revision: recovered.revision,
        })
        .await
        .unwrap();
    assert!(
        matches!(retry.outcome, Outcome::Refused { .. }),
        "local cleanup must not authorize retry of unknown work"
    );
    assert!(peer.requests.lock().unwrap().is_empty());
    let never_reserved = lab
        .client
        .call(Operation::GetOperation {
            operation_id: borg_runtime::native_loop_contract::reservation_id(&original.id, 1),
        })
        .await
        .unwrap();
    assert!(matches!(never_reserved.outcome,Outcome::Refused{ref code,..} if code=="not_found"));
    let second = task(
        lab.client
            .call(Operation::SubmitTask {
                operation_id: "fresh-native".into(),
                input: input("fresh-native"),
            })
            .await
            .unwrap(),
    );
    let accepted = lab
        .until(
            &second.id,
            "new work after verified local capacity recovery",
            |t| t.status == TaskStatus::Succeeded && t.attempt.as_ref().is_some_and(|a| a.stopped),
        )
        .await;
    assert_eq!(accepted.result.as_ref().unwrap().output, Some(json!(7)));
    assert_eq!(
        accepted.attempt.as_ref().unwrap().worker_incarnation,
        "recovered-inc-2"
    );
    assert_eq!(
        peer.requests.lock().unwrap().len(),
        1,
        "only the fresh task makes a request; interrupted task is not resent"
    );
    save(
        lab.root.join("interrupted-native.json"),
        &lab.get(&first.id).await,
    );
    save(lab.root.join("fresh-native.json"), &accepted);
    lab.close();
    let database = rusqlite::Connection::open_with_flags(
        lab.root.join("tasks.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let original_usage: (i64, Option<i64>, Option<i64>) = database
        .query_row(
            "SELECT model_requests,input_tokens,output_tokens FROM attempts WHERE id=?1",
            [&original.id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        original_usage,
        (0, None, None),
        "loop schema preflight precedes reservation; cleanup must not fabricate usage"
    );
    save(lab.root.join("interrupted-usage.json"), &original_usage);
}

// Bounded private-fixture evidence, not a database backup/restore facility.
fn storage_loss_bytes(path: &Path, body_bytes: usize) -> Option<Vec<u8>> {
    use std::io::Read;
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return None,
        Err(error) => panic!("fixture preimage {path:?}: {error}"),
    };
    let mut bytes = Vec::new();
    file.take((16 * body_bytes + 1) as u64)
        .read_to_end(&mut bytes)
        .unwrap();
    assert!(bytes.len() <= 16 * body_bytes, "fixture preimage bound");
    Some(bytes)
}
fn storage_loss_sha(path: &Path) -> String {
    let output = Command::new("/usr/bin/sha256sum")
        .arg("--")
        .arg(path)
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    let digest = text.split_whitespace().next().unwrap();
    assert_eq!(digest.len(), 64);
    digest.into()
}
fn storage_loss_files(
    root: &Path,
    body_bytes: usize,
) -> std::collections::BTreeMap<String, Option<Vec<u8>>> {
    [
        "tasks.sqlite",
        "tasks.sqlite-wal",
        "tasks.sqlite-shm",
        "tasks.sqlite-journal",
        "tasks.sqlite.controller-lock",
        "tasks.sqlite.native-helpers/owner.binding",
    ]
    .into_iter()
    .map(|name| {
        (
            name.into(),
            storage_loss_bytes(&root.join(name), body_bytes),
        )
    })
    .collect()
}
fn retain_unavailable_fixture_database(
    lab: &mut Lab,
) -> (PathBuf, std::collections::BTreeMap<String, Option<Vec<u8>>>) {
    // A real committed WAL survives this fixture-only process loss. Only its
    // private main DB is moved; sidecars remain at the bound path so the oracle
    // detects SQLite touching them before refusing the missing main store.
    if let Some(mut controller) = lab.controller.take() {
        controller.kill();
    }
    let before = storage_loss_files(&lab.root, lab.limits.transport.body_bytes);
    assert!(before["tasks.sqlite"].is_some());
    assert!(
        before["tasks.sqlite-wal"].is_some(),
        "actual committed WAL must be present for this discriminator"
    );
    let retained = lab.root.join("unavailable-original-fixture");
    fs::create_dir(&retained).unwrap();
    for (name, bytes) in &before {
        if let Some(bytes) = bytes {
            let target = retained.join(name);
            fs::create_dir_all(target.parent().unwrap()).unwrap();
            if name == "tasks.sqlite" {
                fs::rename(lab.root.join(name), &target).unwrap();
            } else {
                fs::copy(lab.root.join(name), &target).unwrap();
            }
            assert_eq!(
                storage_loss_bytes(&target, lab.limits.transport.body_bytes).as_ref(),
                Some(bytes)
            );
        }
    }
    let manifest = before.iter().map(|(name,bytes)| (name.clone(), bytes.as_ref().map(|bytes| json!({"bytes":bytes.len(),"sha256":storage_loss_sha(&retained.join(name))})))).collect::<std::collections::BTreeMap<_,_>>();
    save(retained.join("preimage.json"), &manifest);
    assert!(!lab.root.join("tasks.sqlite").exists());
    (retained, before)
}
fn storage_loss_tree(
    root: &Path,
    body_bytes: usize,
) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = std::collections::BTreeMap::new();
    let mut entries = 0usize;
    let mut bytes = 0usize;
    while let Some(path) = pending.pop() {
        let metadata = fs::symlink_metadata(&path).unwrap();
        entries += 1;
        assert!(entries <= 4096, "fixture state inventory bound");
        assert!(
            !metadata.file_type().is_symlink(),
            "fixture snapshot does not follow symlinks"
        );
        if metadata.is_dir() {
            for child in fs::read_dir(path).unwrap() {
                pending.push(child.unwrap().path());
            }
        } else {
            assert!(
                metadata.is_file(),
                "fixture snapshot requires ordinary files"
            );
            let content = storage_loss_bytes(&path, body_bytes).unwrap();
            bytes += content.len();
            assert!(bytes <= 128 * body_bytes, "fixture state byte bound");
            files.insert(path.strip_prefix(root).unwrap().to_owned(), content);
        }
    }
    files
}
async fn wait_for_refused_fixture_controller(process: &mut Process) -> std::process::ExitStatus {
    let until = Instant::now() + TIMEOUT;
    loop {
        if let Some(status) = process.child.try_wait().unwrap() {
            save(
                process
                    .evidence
                    .join(format!("{}-closed.json", process.name)),
                &json!({"pid":process.child.id(),"status":status.to_string(),"forced":false}),
            );
            return status;
        }
        if Instant::now() >= until {
            process.kill();
            panic!("missing-store startup did not refuse");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// Scope: actual private fixture DB+WAL loss; original bytes are retained, not restored.
//= spec/impl/v2/implementation.md#backups-are-outside-the-initial-milestone
//= type=test
//# A recovery procedure MUST NOT fabricate previously committed tasks when their storage has been lost.
async fn missing_bound_database_refuses_before_recreating_or_rewriting_storage() {
    let mut lab = Lab::new("missing-bound-db").await;
    let original = lab
        .submit("original-committed-task", "original-agent")
        .await;
    let stored = lab.get(&original.id).await;
    assert_eq!(stored.id, original.id);
    assert_eq!(stored.input, original.input);
    assert_eq!(stored.revision, original.revision);
    assert_eq!(stored.status, TaskStatus::Queued);
    let (retained, before) = retain_unavailable_fixture_database(&mut lab);
    let db = lab.root.join("tasks.sqlite");
    let address = lab.address.to_string();
    let origin = lab.origin.clone();
    let mut refused_start = lab.spawn(
        "missing-db-refusal".into(),
        &[
            "controller",
            "--db",
            db.to_str().unwrap(),
            "--listen",
            &address,
            "--origin",
            &origin,
        ],
    );
    let status = wait_for_refused_fixture_controller(&mut refused_start).await;
    let after = storage_loss_files(&lab.root, lab.limits.transport.body_bytes);
    save(
        lab.root.join("missing-db-refusal.json"),
        &json!({"exit":status.to_string(),"replacement_db_created":after["tasks.sqlite"].is_some(),"original_task":original,"retained_preimage":retained}),
    );
    assert!(!status.success(), "bound missing store must be refused");
    for (name, bytes) in &before {
        assert_eq!(
            storage_loss_bytes(&retained.join(name), lab.limits.transport.body_bytes),
            *bytes,
            "retained original changed: {name}"
        );
        if name != "tasks.sqlite" {
            assert_eq!(
                after[name], *bytes,
                "startup rewrote/removed a bound sidecar: {name}"
            );
        }
    }
    assert!(
        after["tasks.sqlite"].is_none(),
        "startup fabricated a replacement database before refusal"
    );
    lab.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// Scope: fresh empty controller and an actual previous worker's copied journal,
// not recovery of the old database, history, conversation or source metadata.
//= spec/impl/v2/implementation.md#backups-are-outside-the-initial-milestone
//= type=test
//# A recovery procedure MUST NOT fabricate previously committed tasks when their storage has been lost.
async fn fresh_empty_controller_does_not_reconstruct_tasks_or_execute_old_worker_work() {
    let mut lab = Lab::new("fresh-store-old-worker").await;
    let completed = lab.submit("old-completed", "completed-agent").await;
    let origin = lab.origin.clone();
    let worker = lab.start_worker("worker-a", "inc-1", &origin);
    lab.gate().await.release().await;
    let accepted = lab.successful(&completed.id).await;
    let checkpoint = accepted.selected_checkpoint.clone().unwrap();
    let unfinished = lab.submit("old-unfinished", "unfinished-agent").await;
    let held = lab.gate().await;
    let old_attempt = lab.get(&unfinished.id).await.attempt.unwrap();
    lab.workers[worker].kill();
    // The controlled executor exits on EOF; no successful turn is manufactured.
    drop(held);
    let component = borg_runtime::worker::process::component("worker-a");
    let original_state = lab.root.join("worker-a/.borg-worker").join(&component);
    let command_path = original_state.join("commands").join(format!(
        "{}.json",
        borg_runtime::worker::process::component(&format!("run-{}", old_attempt.id))
    ));
    let command: Value = serde_json::from_slice(&fs::read(&command_path).unwrap()).unwrap();
    assert_eq!(command["completed"], false);
    assert_eq!(command["effects_started"], true);
    let state_before = storage_loss_tree(&original_state, lab.limits.transport.body_bytes);
    let (retained, database_before) = retain_unavailable_fixture_database(&mut lab);
    let refs_before = git(
        &lab.root,
        &[
            "--git-dir",
            &checkpoint.repository,
            "for-each-ref",
            "--format=%(refname) %(objectname)",
        ],
    );
    let fresh_root = lab.root.join("explicitly-fresh-controller");
    fs::create_dir(&fresh_root).unwrap();
    assert!(fs::read_dir(&fresh_root).unwrap().next().is_none());
    let fresh_db = fresh_root.join("tasks.sqlite");
    let address = lab.address.to_string();
    lab.controller = Some(lab.spawn(
        "explicitly-fresh-controller".into(),
        &[
            "controller",
            "--db",
            fresh_db.to_str().unwrap(),
            "--listen",
            &address,
            "--origin",
            &origin,
        ],
    ));
    let until = Instant::now() + TIMEOUT;
    loop {
        if let Ok(Reply {
            outcome: Outcome::Ok {
                data: Data::Tasks(tasks),
            },
            ..
        }) = lab
            .client
            .call(Operation::ListTasks {
                after: None,
                limit: 20,
            })
            .await
        {
            assert!(tasks.is_empty());
            break;
        }
        assert!(
            Instant::now() < until,
            "fresh controller did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for id in [&completed.id, &unfinished.id] {
        assert_eq!(
            refused(
                lab.client
                    .call(Operation::GetTask {
                        task_id: id.clone()
                    })
                    .await
                    .unwrap()
            ),
            "not_found"
        );
    }
    assert_eq!(
        refused(
            lab.client
                .call(Operation::GetOperation {
                    operation_id: checkpoint.operation_id.clone()
                })
                .await
                .unwrap()
        ),
        "not_found"
    );
    let working = lab.root.join("old-worker-working-copy");
    let copied_state = working.join(".borg-worker").join(&component);
    fs::create_dir_all(&copied_state).unwrap();
    for (relative, bytes) in &state_before {
        let target = copied_state.join(relative);
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(&target, bytes).unwrap();
        // Copy the original private file mode as well as its exact bytes. A
        // default-mode rewrite is not the retained worker state under test.
        let permissions = fs::metadata(original_state.join(relative))
            .unwrap()
            .permissions();
        fs::set_permissions(&target, permissions.clone()).unwrap();
        assert_eq!(
            fs::metadata(&target).unwrap().permissions().mode(),
            permissions.mode()
        );
    }
    assert_eq!(
        storage_loss_tree(&copied_state, lab.limits.transport.body_bytes),
        state_before
    );
    let probe_evidence = lab.root.join("old-worker-query-evidence");
    fs::create_dir(&probe_evidence).unwrap();
    let mut observer = Proxy::new(
        origin.clone(),
        Fault::ObserveMissingTask,
        probe_evidence,
        lab.limits.transport.body_bytes,
    )
    .await;
    let fixture = lab.fixture.clone();
    let recovery_worker = lab.spawn(
        "old-worker-on-empty-store".into(),
        &[
            "worker",
            "--controller",
            &observer.url,
            "--id",
            "worker-a",
            "--incarnation",
            "inc-2",
            "--workspace-root",
            working.to_str().unwrap(),
            "--codex",
            fixture.to_str().unwrap(),
            "--coding-capacity",
            "1",
            "--native-capacity",
            "0",
        ],
    );
    lab.workers.push(recovery_worker);
    let first_observation = observer.event().await;
    assert!(
        matches!(first_observation.original.command, Operation::GetTask { ref task_id } if task_id == &unfinished.id)
    );
    assert_eq!(refused(first_observation.reply), "not_found");
    // Observe the worker processing that refusal: it may retry reconciliation
    // or exit permanently. Neither behavior is required beyond the existing
    // no-fabrication/no-execution boundary, and no infinite retry is asserted.
    let mut observed_lookups = 1;
    let until = Instant::now() + TIMEOUT;
    loop {
        if let Some(status) = lab.workers.last_mut().unwrap().child.try_wait().unwrap() {
            let process = lab.workers.last().unwrap();
            save(
                process
                    .evidence
                    .join(format!("{}-closed.json", process.name)),
                &json!({"pid":process.child.id(),"status":status.to_string(),"forced":false,"after_missing_task_refusal":true}),
            );
            break;
        }
        let observed = tokio::select! {
            value = observer.events.recv() => Some(value.expect("recovery observer closed")),
            connection = lab.gate.accept() => {
                let _ = connection.unwrap();
                panic!("old journal started another executor after missing-task refusal");
            },
            _ = tokio::time::sleep(Duration::from_millis(20)) => None,
        };
        if let Some(observation) = observed {
            assert!(
                matches!(observation.original.command, Operation::GetTask { ref task_id } if task_id == &unfinished.id)
            );
            assert_eq!(refused(observation.reply), "not_found");
            observed_lookups += 1;
            break;
        }
        assert!(
            Instant::now() < until,
            "worker neither refused permanently nor retried reconciliation"
        );
    }
    lab.workers.last_mut().unwrap().stop();
    assert!(
        matches!(lab.client.call(Operation::ListTasks { after:None, limit:20 }).await.unwrap().outcome, Outcome::Ok { data:Data::Tasks(tasks) } if tasks.is_empty())
    );
    assert!(
        !working.join("workspaces").exists(),
        "old journal triggered a new checkout"
    );
    assert!(!working.join("validation").exists());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), lab.gate.accept())
            .await
            .is_err(),
        "old journal started another executor"
    );
    assert_eq!(
        lab.starts.len(),
        2,
        "only the two explicitly submitted original turns started"
    );
    assert_eq!(
        storage_loss_tree(&original_state, lab.limits.transport.body_bytes),
        state_before,
        "original worker evidence was rewritten"
    );
    for (name, bytes) in &database_before {
        assert_eq!(
            storage_loss_bytes(&retained.join(name), lab.limits.transport.body_bytes),
            *bytes,
            "retained original store changed: {name}"
        );
    }
    assert_eq!(
        git(
            &lab.root,
            &[
                "--git-dir",
                &checkpoint.repository,
                "for-each-ref",
                "--format=%(refname) %(objectname)"
            ]
        ),
        refs_before
    );
    assert_eq!(
        git(
            &lab.root,
            &[
                "--git-dir",
                &checkpoint.repository,
                "rev-parse",
                &checkpoint.remote_ref
            ]
        ),
        checkpoint.commit
    );
    save(
        lab.root.join("no-task-fabrication.json"),
        &json!({"lost_task_ids":[completed.id,unfinished.id],"available_git_checkpoint":checkpoint,"fresh_store_task_count":0,"old_worker_missing_task_observations":observed_lookups,"new_executor_starts":0,"retained_database":retained,"original_worker_state":original_state}),
    );
    lab.close();
}

#[path = "recovery/native_tool.rs"]
mod native_tool;

#[path = "recovery/git_publication.rs"]
mod git_publication;

#[path = "recovery/checkpoint_crash.rs"]
mod checkpoint_crash;

#[path = "recovery/supplied_executor_evidence.rs"]
mod supplied_executor_evidence;

#[path = "recovery/catalogue_recovery.rs"]
mod catalogue_recovery;

#[path = "recovery/governance_controls.rs"]
mod governance_controls;
#[path = "recovery/cleanup_recovery.rs"]
mod cleanup_recovery;

#[path = "recovery/controller_restart_live.rs"]
mod controller_restart_live;

#[path = "recovery/git_transport_ack.rs"]
mod git_transport_ack;
#[path = "recovery/pull_request_publication.rs"]
mod pull_request_publication;

#[path = "recovery/instruction_source.rs"]
mod instruction_source;

#[path = "recovery/publication_mcp.rs"]
mod publication_mcp;
