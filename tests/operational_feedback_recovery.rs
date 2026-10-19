//! Actual controller/two worker processes and the real MCP bridge.
//! Controlled stdio peer, post-commit reply loss, local Git; no live model.
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
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};

static SEQUENCE: AtomicU64 = AtomicU64::new(1);
const TIMEOUT: Duration = Duration::from_secs(30);
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
fn git(cwd: &Path, args: &[&str]) -> String {
    let o = Command::new("/usr/bin/git")
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
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
    closed: bool,
}
impl Process {
    fn kill(&mut self) {
        if self.closed {
            return;
        }
        if self.child.try_wait().unwrap().is_none() {
            self.child.kill().unwrap();
        }
        let status = self.child.wait().unwrap();
        self.closed = true;
        save(
            self.evidence.join(format!("{}-closed.json", self.name)),
            &json!({"pid":self.child.id(),"status":status.to_string(),"forced":true,"reaped":true}),
        );
    }
    fn stop(&mut self) -> bool {
        if self.closed {
            return true;
        }
        if let Some(status) = self.child.try_wait().unwrap() {
            self.closed = true;
            save(
                self.evidence.join(format!("{}-closed.json", self.name)),
                &json!({"pid":self.child.id(),"status":status.to_string(),"forced":false,"reaped":true}),
            );
            return status.success();
        }
        let _ = Command::new("/bin/kill")
            .args(["-INT", "--", &self.child.id().to_string()])
            .status();
        let end = Instant::now() + Duration::from_secs(18);
        while Instant::now() < end {
            if let Some(status) = self.child.try_wait().unwrap() {
                self.closed = true;
                save(
                    self.evidence.join(format!("{}-closed.json", self.name)),
                    &json!({"pid":self.child.id(),"status":status.to_string(),"forced":false,"reaped":true}),
                );
                return status.success();
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        self.kill();
        false
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        self.stop();
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
        let base =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("artifacts/operational-feedback-recovery");
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
        let mut selected = RuntimeLimits::seed().unwrap();
        selected.revision = "two-worker-recovery-fixture-v1".into();
        selected.worker.coding_capacity = 1;
        selected.worker.native_capacity = 0;
        selected.controller.command_wait_ms = 10;
        selected.validate().unwrap();
        let limits = Arc::new(selected);
        let limits_path = root.join("limits.json");
        save(&limits_path, limits.as_ref());
        let image = root.join("borg-runtime");
        let input = PathBuf::from(env!("CARGO_BIN_EXE_borg-runtime"));
        fs::copy(input, &image).unwrap();
        let mut pins = vec![
            image.clone(),
            limits_path,
            Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"),
            Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.lock"),
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/operational_feedback_recovery.rs"),
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/operational_feedback/recovery_peer.py"),
        ];
        for folder in ["src"] {
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
                repository_id: "recovery-repository".into(),
                registration_revision: "initial".into(),
            }),
            url: remote.to_string_lossy().into(),
            start_commit,
            checkpoint_remote: remote.to_string_lossy().into(),
        };
        let gate = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fixture = root.join("recovery_peer.py");
        fs::write(
            &fixture,
            include_str!("operational_feedback/recovery_peer.py"),
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();
        save(
            fixture.with_extension("json"),
            &json!({"port":gate.local_addr().unwrap().port(),"mcp_version":borg_runtime::mcp::VERSION}),
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
        let registered = lab
            .client
            .call(Operation::RegisterRepository {
                operation_id: "register-recovery-repository".into(),
                registration: borg_runtime::repository_catalogue::Registration {
                    pull_request_target: None,
                    repository_id: binding.repository_id.clone(),
                    registration_revision: binding.registration_revision.clone(),
                    display_name: "Recovery fixture repository".into(),
                    source_endpoint: lab.repository.url.clone(),
                    checkpoint_endpoint: lab.repository.checkpoint_remote.clone(),
                    source_store_key: "remote.git".into(),
                    checkpoint_store_key: "remote.git".into(),
                },
            })
            .await
            .unwrap();
        assert!(matches!(
            registered.outcome,
            Outcome::Ok {
                data: Data::RepositoryRegistration(_)
            }
        ));
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
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", &self.root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
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
            closed: false,
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
                "/usr/bin/python3".into(),
                "-I".into(),
                "-S".into(),
                "-B".into(),
                "-c".into(),
                "from pathlib import Path; assert Path('answer.txt').read_text()=='after\\n'"
                    .into(),
            ]],
            output_schema: None,
            limits: Limits {
                deadline_unix_ms: now() + 180_000,
                model_requests: 4,
                output_bytes: 8192,
                result_bytes: self.limits.task_defaults.result_bytes,
                max_delegation_depth: 4,
            },
            dependencies: vec![],
            native_workflow: None,
        }
    }
    async fn submit(&self, operation: &str, agent: &str, mode: &str) -> Task {
        let mut input = self.input(agent);
        input.instruction =
            format!("{mode}: Set answer.txt to after; controlled recovery fixture.");
        task(
            self.client
                .call(Operation::SubmitTask {
                    operation_id: operation.into(),
                    input: (input).into(),
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
            assert!(worker.stop(), "worker did not close gracefully");
        }
        if let Some(mut controller) = self.controller.take() {
            assert!(controller.stop(), "controller did not close gracefully");
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

// Hold exact replies only AFTER the real controller commits them. Each fault is
// one-shot and the original request/receipt is retained; this never fabricates a
// controller result or reissues an MCP mutation.
struct Held {
    call: Option<borg_runtime::mcp::Invocation>,
    reply: Reply,
    release: tokio::sync::oneshot::Sender<()>,
}
struct Proxy {
    url: String,
    events: mpsc::Receiver<Held>,
    writes: Arc<AtomicU64>,
    uncertainty_reports: Arc<Mutex<Vec<(Report, Reply)>>>,
    join: tokio::task::JoinHandle<()>,
}
impl Proxy {
    async fn new(upstream: String, body_bytes: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let writes = Arc::new(AtomicU64::new(0));
        let uncertainty_reports = Arc::new(Mutex::new(vec![]));
        let (send, events) = mpsc::channel(2);
        let (count, reports) = (writes.clone(), uncertainty_reports.clone());
        let join = tokio::spawn(async move {
            let client = reqwest::Client::builder().timeout(TIMEOUT).build().unwrap();
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted=listener.accept()=>{
                        let (stream,_)=accepted.unwrap();
                        let (client,upstream,count,reports,send)=(client.clone(),upstream.clone(),count.clone(),reports.clone(),send.clone());
                        connections.spawn(async move {
                            let service=service_fn(move |request:HttpRequest<Incoming>|{
                                let (client,upstream,count,reports,send)=(client.clone(),upstream.clone(),count.clone(),reports.clone(),send.clone());
                                async move {
                                    let path=request.uri().path().to_owned();
                                    let caller=request.headers().get("x-borg-caller").cloned();
                                    let version=request.headers().get("mcp-protocol-version").cloned();
                                    let bytes=request.into_body().collect().await.map_err(io::Error::other)?.to_bytes();
                                    if bytes.len()>body_bytes {return Err(io::Error::other("fixture input bound"));}
                                    let rpc:Value=serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                                    let call=if path=="/mcp" && rpc["method"]=="tools/call" && rpc["params"]["name"]=="collab_memory_set" {
                                        count.fetch_add(1,Ordering::SeqCst);
                                        Some(borg_runtime::mcp::Invocation{
                                            operation_id:rpc["params"]["_meta"]["borg"]["operation_id"].as_str().unwrap().into(),
                                            logical_call_id:rpc["params"]["_meta"]["borg"]["logical_call_id"].as_str().unwrap().into(),
                                            caller:serde_json::from_slice(caller.as_ref().unwrap().as_bytes()).map_err(io::Error::other)?,
                                            tool:"collab_memory_set".into(),arguments:rpc["params"]["arguments"].clone(),
                                        })
                                    }else{None};
                                    let report=if path=="/api/operations" {
                                        let request:Request=serde_json::from_value(rpc.clone()).map_err(io::Error::other)?;
                                        match request.command {Operation::ObserveWorker{report} if report.report_id.ends_with("-recovery-uncertain")=>Some(report),_=>None}
                                    }else{None};
                                    let mut forward=client.post(format!("{upstream}{path}")).header("content-type","application/json").body(bytes.to_vec());
                                    if let Some(caller)=caller {forward=forward.header("x-borg-caller",caller);}
                                    if let Some(version)=version {forward=forward.header("mcp-protocol-version",version);}
                                    let response=forward.send().await.map_err(io::Error::other)?;
                                    let status=response.status();
                                    let response=response.bytes().await.map_err(io::Error::other)?;
                                    if response.len()>body_bytes {return Err(io::Error::other("fixture response bound"));}
                                    let held=if call.is_some() {
                                        let value:Value=serde_json::from_slice(&response).map_err(io::Error::other)?;
                                        let result:borg_runtime::mcp::CallResult=serde_json::from_str(value["result"]["content"][0]["text"].as_str().unwrap()).map_err(io::Error::other)?;
                                        Some(result.reply)
                                    }else if let Some(report)=report {
                                        let reply:Reply=serde_json::from_slice(&response).map_err(io::Error::other)?;
                                        let first={let mut items=reports.lock().unwrap();let first=items.is_empty();items.push((report,reply.clone()));first};
                                        first.then_some(reply)
                                    }else{None};
                                    if let Some(reply)=held {
                                        assert!(matches!(reply.outcome,Outcome::Ok{..}) && reply.commit.is_some());
                                        let (release,wait)=tokio::sync::oneshot::channel();
                                        send.send(Held{call,reply,release}).await.map_err(io::Error::other)?;
                                        tokio::time::timeout(Duration::from_secs(90),wait).await.map_err(io::Error::other)?.map_err(io::Error::other)?;
                                    }
                                    Ok::<_,io::Error>(HttpResponse::builder().status(status.as_u16()).header("content-type","application/json").body(Full::new(response)).unwrap())
                                }
                            });
                            let _=hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream),service).await;
                        });
                    },
                    Some(_)=connections.join_next()=>{},
                }
            }
        });
        Self {
            url,
            events,
            writes,
            uncertainty_reports,
            join,
        }
    }
    async fn held(&mut self) -> Held {
        tokio::time::timeout(TIMEOUT, self.events.recv())
            .await
            .expect("actual committed response was not held")
            .unwrap()
    }
    async fn stop(&mut self) {
        self.join.abort();
        let _ = (&mut self.join).await;
        assert!(
            TcpStream::connect(self.url.trim_start_matches("http://"))
                .await
                .is_err()
        );
    }
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.join.abort();
    }
}

async fn registered(lab: &Lab, worker: &str, incarnation: &str) {
    let end = Instant::now() + TIMEOUT;
    loop {
        let reply = lab
            .client
            .call(Operation::QueryCollaboration {
                query: borg_runtime::collaboration::Query::Worker {
                    worker_id: worker.into(),
                },
            })
            .await
            .unwrap();
        if matches!(reply.outcome,Outcome::Ok{data:Data::Collaboration(borg_runtime::collaboration::QueryResult::Worker(ref value))} if value.worker.incarnation==incarnation)
        {
            return;
        }
        assert!(
            Instant::now() < end,
            "worker did not register after recovery: {}",
            lab.root.display()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
fn original_state(lab: &Lab, attempt: &Attempt) -> PathBuf {
    let worker = lab
        .root
        .join(&attempt.worker_id)
        .join(".borg-worker")
        .join(borg_runtime::worker::process::component(&attempt.worker_id));
    borg_runtime::worker::codex::state_path(&worker.join("codex"), &attempt.id)
}
fn not_running(pid: u64) -> bool {
    fs::read_to_string(format!("/proc/{pid}/stat"))
        .ok()
        .and_then(|s| s.rsplit_once(") ").map(|(_, tail)| tail.starts_with('Z')))
        .unwrap_or(true)
}
async fn original_receipt(
    lab: &Lab,
    call: &borg_runtime::mcp::Invocation,
) -> borg_runtime::mcp::CallResult {
    let reply = lab
        .client
        .call(Operation::GetCollaborationCall {
            operation_id: call.operation_id.clone(),
            caller: call.caller.clone(),
        })
        .await
        .unwrap();
    borg_runtime::mcp::recovered(call, reply).unwrap()
}

fn same_receipt(actual: &borg_runtime::mcp::CallResult, original: &borg_runtime::mcp::CallResult) {
    assert_eq!(actual.requires_quiescence, original.requires_quiescence);
    assert_eq!(actual.reply.commit, original.reply.commit);
    assert_eq!(actual.reply.outcome, original.reply.outcome);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_workers_recover_retained_call_through_interrupted_uncertainty_ack_without_reexecution()
{
    let mut lab = Lab::new("retained-call-interrupted-recovery").await;
    let mut proxy = Proxy::new(lab.origin.clone(), lab.limits.transport.body_bytes).await;
    let first = lab
        .submit("first-original", "shared-agent", "LOSS_ONCE")
        .await;
    let first_worker = lab.start_worker("worker-a", "inc-1", &proxy.url);
    let lost = lab.gate().await;
    assert_eq!(lost.observed["mode"], "LOSS_ONCE");
    let peer_pid = lost.observed["pid"].as_u64().unwrap();
    let bridge_pid = lost.observed["bridge_pid"].as_u64().unwrap();
    lost.release().await;
    let held_write = proxy.held().await;
    let call = held_write.call.clone().unwrap();
    let original = lab.get(&first.id).await;
    let prior = original.attempt.as_ref().unwrap().clone();
    assert_eq!(call.caller.task_id, first.id);
    assert_eq!(call.caller.attempt_id, prior.id);
    assert_eq!(prior.worker_id, "worker-a");
    assert_eq!(original.status, TaskStatus::Active);
    assert!(!prior.stopped && !prior.uncertain);
    let state = original_state(&lab, &prior);
    let journal = state.join("collaboration-codex/calls.json");
    let journal_before = fs::read(&journal).unwrap();
    let retained: Value = serde_json::from_slice(&journal_before).unwrap();
    assert_eq!(retained["entries"].as_array().unwrap().len(), 1);
    assert_eq!(
        retained["entries"][0]["invocation"],
        serde_json::to_value(&call).unwrap()
    );
    assert!(retained["entries"][0]["outcome"].is_null());
    assert_eq!(retained["entries"][0]["delivered"], false);
    let config = state.join("borg-collaboration-config.json");
    let config_before = fs::read(&config).unwrap();
    let retained_receipt = original_receipt(&lab, &call).await;
    assert_eq!(retained_receipt.reply.commit, held_write.reply.commit);
    assert_eq!(
        serde_json::to_value(&retained_receipt.reply.outcome).unwrap(),
        serde_json::to_value(&held_write.reply.outcome).unwrap()
    );

    // A different real worker remains occupied and healthy during both losses.
    let healthy_task = lab
        .submit("second-healthy", "healthy-agent", "HEALTHY")
        .await;
    lab.start_worker("worker-b", "inc-1", &proxy.url);
    let healthy = lab.gate().await;
    assert_eq!(healthy.observed["mode"], "HEALTHY");
    let healthy_before = lab.get(&healthy_task.id).await;
    assert_eq!(
        healthy_before.attempt.as_ref().unwrap().worker_id,
        "worker-b"
    );
    lab.workers[first_worker].kill();
    let replacement = lab.start_worker("worker-a", "inc-2", &proxy.url);
    let held_uncertainty = proxy.held().await;
    assert!(held_uncertainty.call.is_none());
    let intermediate = lab.get(&first.id).await;
    assert_eq!(intermediate.status, TaskStatus::Failed);
    assert!(intermediate.attempt.as_ref().unwrap().uncertain);
    assert!(!intermediate.attempt.as_ref().unwrap().stopped);
    assert_eq!(intermediate.input, original.input);
    assert!(not_running(peer_pid) && not_running(bridge_pid));
    assert_eq!(fs::read(&journal).unwrap(), journal_before);
    assert_eq!(lab.get(&healthy_task.id).await.input, healthy_before.input);
    assert_eq!(lab.get(&healthy_task.id).await.status, TaskStatus::Active);

    // The uncertainty report committed, but its ACK never reaches this worker.
    lab.workers[replacement].kill();
    let _ = held_uncertainty.release.send(());
    let _ = held_write.release.send(());
    lab.start_worker("worker-a", "inc-3", &proxy.url);
    registered(&lab, "worker-a", "inc-3").await;
    let failed = lab
        .until(&first.id, "exact stopped uncertain old attempt", |t| {
            t.status == TaskStatus::Failed
                && t.attempt.as_ref().is_some_and(|a| a.stopped && a.uncertain)
        })
        .await;
    assert_eq!(failed.input, original.input);
    assert_eq!(failed.session_id, original.session_id);
    assert_eq!(failed.attempt.as_ref().unwrap().id, prior.id);
    assert_eq!(failed.attempt.as_ref().unwrap().worker_incarnation, "inc-1");
    assert!(failed.result.is_none() && failed.selected_checkpoint.is_none());
    assert_eq!(fs::read(&journal).unwrap(), journal_before);
    assert_eq!(fs::read(&config).unwrap(), config_before);
    assert_eq!(proxy.writes.load(Ordering::SeqCst), 1);
    let reports = proxy.uncertainty_reports.lock().unwrap().clone();
    assert!(
        reports.len() >= 2,
        "lost recovery ACK must replay its original report"
    );
    for (report, reply) in &reports {
        assert_eq!(report, &reports[0].0);
        assert_eq!(reply.commit, reports[0].1.commit);
        assert_eq!(
            serde_json::to_value(&reply.outcome).unwrap(),
            serde_json::to_value(&reports[0].1.outcome).unwrap()
        );
    }
    let history = lab
        .client
        .call(Operation::QueryCollaboration {
            query: borg_runtime::collaboration::Query::TaskHistory {
                task_id: first.id.clone(),
                after: None,
                limit: 100,
            },
        })
        .await
        .unwrap();
    let Outcome::Ok {
        data: Data::Collaboration(borg_runtime::collaboration::QueryResult::TaskHistory(history)),
    } = history.outcome
    else {
        panic!("history")
    };
    assert!(history.next_cursor.is_none());
    assert_eq!(
        history
            .items
            .iter()
            .filter(|x| x.kind == "worker_observation" && x.body["kind"] == "execution_uncertain")
            .count(),
        1
    );
    assert_eq!(
        refused(
            lab.client
                .call(Operation::RetryTask {
                    operation_id: "never-retry-original".into(),
                    task_id: first.id.clone(),
                    expected_revision: failed.revision
                })
                .await
                .unwrap()
        ),
        "reconciliation_required"
    );
    assert_eq!(
        refused(
            lab.client
                .call(stale_report(&prior, "stale-original-report"))
                .await
                .unwrap()
        ),
        "stale_incarnation"
    );

    let later = lab
        .submit("third-independent", "shared-agent", "READ_ONCE")
        .await;
    let read = lab.gate().await;
    assert_eq!(read.observed["mode"], "READ_ONCE");
    let result: borg_runtime::mcp::CallResult =
        serde_json::from_value(read.observed["result"].clone()).unwrap();
    let Outcome::Ok {
        data: Data::Memory(Some(memory)),
    } = result.reply.outcome
    else {
        panic!("actual retained memory result")
    };
    assert_eq!(memory.revision, 1);
    assert_eq!(memory.value, json!({"write":"once"}));
    let later_running = lab.get(&later.id).await;
    assert_eq!(
        later_running.attempt.as_ref().unwrap().worker_id,
        "worker-a"
    );
    assert_eq!(
        later_running.attempt.as_ref().unwrap().worker_incarnation,
        "inc-3"
    );
    read.release().await;
    healthy.release().await;
    let accepted_later = lab.successful(&later.id).await;
    let accepted_healthy = lab.successful(&healthy_task.id).await;
    assert_eq!(proxy.writes.load(Ordering::SeqCst), 1);
    same_receipt(&original_receipt(&lab, &call).await, &retained_receipt);

    // An already-stopped uncertain original survives another fresh controller
    // and both worker processes; no task/conversation/tool call is recreated.
    for worker in &mut lab.workers {
        assert!(worker.stop());
    }
    let original_final = lab.get(&first.id).await;
    lab.restart_controller().await;
    lab.start_worker("worker-a", "inc-4", &proxy.url);
    lab.start_worker("worker-b", "inc-2", &proxy.url);
    registered(&lab, "worker-a", "inc-4").await;
    registered(&lab, "worker-b", "inc-2").await;
    assert_eq!(lab.get(&first.id).await, original_final);
    assert_eq!(lab.get(&later.id).await, accepted_later);
    assert_eq!(lab.get(&healthy_task.id).await, accepted_healthy);
    assert_eq!(fs::read(&journal).unwrap(), journal_before);
    assert_eq!(fs::read(&config).unwrap(), config_before);
    same_receipt(&original_receipt(&lab, &call).await, &retained_receipt);
    assert_eq!(proxy.writes.load(Ordering::SeqCst), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), lab.gate.accept())
            .await
            .is_err()
    );
    assert_eq!(lab.starts.len(), 3);
    save(
        lab.root.join("retained-call-proof.json"),
        &json!({"original_invocation":call,"retained_receipt":retained_receipt,"original_task":original_final,"healthy_task":accepted_healthy,"later_task":accepted_later,"uncertainty_reports":reports,"original_peer_pid":peer_pid,"original_bridge_pid":bridge_pid,"original_processes_not_running":not_running(peer_pid)&&not_running(bridge_pid),"original_journal_unchanged":true,"write_dispatches":1,"controlled_executor_starts":3}),
    );
    let evidence = lab.root.clone();
    let gate_address = lab.gate.local_addr().unwrap();
    let controller_address = lab.address;
    lab.close();
    proxy.stop().await;
    drop(lab);
    assert!(TcpStream::connect(gate_address).await.is_err());
    assert!(TcpStream::connect(controller_address).await.is_err());
    save(
        evidence.join("fixture-closed.json"),
        &json!({"controller_closed":true,"all_worker_children_reaped":true,"proxy_closed":true,"gate_closed":true,"original_peer_and_bridge_not_running":not_running(peer_pid)&&not_running(bridge_pid),"external_provider_calls":0,"controlled_executor_starts":3}),
    );
}
