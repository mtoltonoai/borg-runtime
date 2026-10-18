//! Explicitly ignored local component load qualification. Real controller/worker,
//! HTTP/SQLite/Git/rustc/checkpoints; controlled protocol peers, not live providers
//! or the supplied Codex executor, and not a host/cluster capacity guarantee.
use borg_runtime::{client::ApiClient, protocol::*};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{
    Request as HttpRequest, Response as HttpResponse, body::Incoming, service::service_fn,
};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io,
    net::SocketAddr,
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, watch},
};

const PHASE: Duration = Duration::from_secs(300);
const KEY: &str = "local-load-synthetic-key";
fn stamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .try_into()
        .unwrap()
}
fn save(path: impl AsRef<Path>, value: &impl serde::Serialize) {
    let path = path.as_ref();
    let temporary = path.with_extension("pending");
    fs::write(&temporary, serde_json::to_vec_pretty(value).unwrap()).unwrap();
    fs::rename(temporary, path).unwrap();
}
fn selected(name: &str, allowed: &[usize], default: usize) -> usize {
    let value = std::env::var(name)
        .map(|v| {
            v.parse::<usize>()
                .expect("closed load option must be an integer")
        })
        .unwrap_or(default);
    assert!(
        allowed.contains(&value),
        "unqualified load size {name}={value}"
    );
    value
}
fn git(cwd: &Path, args: &[&str]) -> String {
    let output = Command::new("/usr/bin/git")
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", cwd)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_TERMINAL_PROMPT", "0")
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "user.name=Local load fixture",
            "-c",
            "user.email=local-load@example.invalid",
        ])
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "local Git failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().into()
}
fn actual_task(reply: Reply) -> Task {
    match reply.outcome {
        Outcome::Ok {
            data: Data::Task(task),
        } => *task,
        other => panic!("task refused/unknown: {other:?}"),
    }
}

struct Process {
    child: Child,
    root: PathBuf,
    name: String,
    closed: bool,
}
impl Process {
    fn close(&mut self, hard: bool) {
        if self.closed {
            return;
        }
        let mut forced = hard;
        let mut status = self.child.try_wait().unwrap();
        if status.is_none() {
            if hard {
                self.child.kill().unwrap();
            } else {
                let _ = Command::new("/bin/kill")
                    .args(["-INT", "--", &self.child.id().to_string()])
                    .status();
            }
            let deadline = Instant::now() + Duration::from_secs(20);
            while Instant::now() < deadline {
                if let Some(value) = self.child.try_wait().unwrap() {
                    status = Some(value);
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            if status.is_none() {
                forced = true;
                let _ = self.child.kill();
                status = Some(self.child.wait().unwrap());
            }
        }
        save(
            self.root.join(format!("{}-closed.json", self.name)),
            &json!({"pid":self.child.id(),"status":status.unwrap().to_string(),"success":status.unwrap().success(),"forced":forced,"closed_unix_ns":stamp()}),
        );
        self.closed = true;
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        self.close(false);
    }
}

#[derive(Default)]
struct NativeState {
    received: BTreeMap<String, u64>,
    held: usize,
    peak: usize,
    responses: usize,
    errors: Vec<String>,
}
fn native_snapshot(state: &Arc<Mutex<NativeState>>) -> Value {
    let state = state.lock().unwrap();
    json!({"received":state.received,"held":state.held,"peak_held":state.peak,"responses":state.responses,"errors":state.errors})
}
struct Held {
    state: Arc<Mutex<NativeState>>,
    released: bool,
}
impl Drop for Held {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap();
        state.held -= 1;
        if !self.released {
            state
                .errors
                .push("parsed native request ended before response release".into());
        }
    }
}
struct NativePeer {
    endpoint: String,
    state: Arc<Mutex<NativeState>>,
    release: watch::Sender<bool>,
    events: Option<mpsc::Receiver<()>>,
    stop: Option<oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<()>>,
}
impl NativePeer {
    async fn start(expected: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(NativeState::default()));
        let (release, receiver) = watch::channel(false);
        let (events, receive) = mpsc::channel(1024);
        let (stop, mut stopped) = oneshot::channel();
        let shared = state.clone();
        let join = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _=&mut stopped=>break,
                    accepted=listener.accept()=>{
                        let (stream,_)=accepted.unwrap(); let state=shared.clone(); let receiver=receiver.clone(); let events=events.clone();
                        connections.spawn(async move {
                            let service=service_fn(move |request:HttpRequest<Incoming>| {
                                let state=state.clone(); let mut receiver=receiver.clone(); let events=events.clone();
                                async move {
                                    if request.method()!="POST" || request.uri().path()!="/v1/responses" || request.headers().get("authorization").and_then(|x|x.to_str().ok())!=Some(&format!("Bearer {KEY}")) { return Err(io::Error::other("unqualified provider request")); }
                                    let bytes=tokio::time::timeout(Duration::from_secs(10),Limited::new(request.into_body(),borg_runtime::limits::RuntimeLimits::seed().unwrap().transport.body_bytes).collect()).await.map_err(io::Error::other)?.map_err(io::Error::other)?.to_bytes();
                                    let value:Value=serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                                    let expected_tools=borg_runtime::mcp::catalogue(&borg_runtime::limits::RuntimeLimits::seed().unwrap()).into_iter().map(|tool|json!({"type":"function","name":tool.name,"description":tool.description,"parameters":tool.input_schema,"strict":false})).collect::<Vec<_>>();
                                    if value["model"]!=SELECTED_MODEL || value["reasoning"]["effort"]!=SELECTED_REASONING_EFFORT || value["store"]!=false || value["tools"]!=json!(expected_tools) || value["tool_choice"]!="auto" || value["parallel_tool_calls"]!=false || value.get("max_tool_calls").is_some() { return Err(io::Error::other("wrong native request profile")); }
                                    let messages=value["input"].as_array().filter(|items|items.len()==1).ok_or_else(||io::Error::other("missing single initial native message"))?;
                                    if messages[0]["role"]!="user" { return Err(io::Error::other("wrong initial native message role")); }
                                    let id=messages[0]["content"].as_str().ok_or_else(||io::Error::other("missing native identity"))?.to_owned();
                                    let number=id.strip_prefix("controlled-native-").and_then(|n|n.parse::<usize>().ok()).filter(|n|*n<expected).ok_or_else(||io::Error::other("unexpected native identity"))?;
                                    if id!=format!("controlled-native-{number}") { return Err(io::Error::other("noncanonical native identity")); }
                                    {
                                        let mut state=state.lock().unwrap();
                                        if state.received.contains_key(&id) { state.errors.push("duplicate native provider request".into()); return Err(io::Error::other("duplicate provider request")); }
                                        state.received.insert(id.clone(),stamp()); state.held+=1; state.peak=state.peak.max(state.held);
                                    }
                                    let mut held=Held{state:state.clone(),released:false}; let _=events.try_send(());
                                    while !*receiver.borrow() { receiver.changed().await.map_err(io::Error::other)?; }
                                    let output=json!({"case":id});
                                    let body=json!({"status":"completed","usage":{"input_tokens":17,"output_tokens":9,"total_tokens":26},"output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":output.to_string()}]}]});
                                    held.released=true; state.lock().unwrap().responses+=1;
                                    Ok::<_,io::Error>(HttpResponse::new(Full::new(Bytes::from(serde_json::to_vec(&body).unwrap()))))
                                }
                            });
                            hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream),service).await.map_err(|e|e.to_string())
                        });
                    },
                    completed=connections.join_next(),if !connections.is_empty()=>{
                        match completed {Some(Ok(Ok(())))=>{}, other=>{shared.lock().unwrap().errors.push(format!("native connection ended: {other:?}"));let _=events.try_send(());}}
                    }
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        Self {
            endpoint,
            state,
            release,
            events: Some(receive),
            stop: Some(stop),
            join: Some(join),
        }
    }
    async fn close(&mut self) -> bool {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(mut join) = self.join.take() {
            match tokio::time::timeout(Duration::from_secs(10), &mut join).await {
                Ok(result) => return result.is_ok(),
                Err(_) => {
                    join.abort();
                    let _ = join.await;
                    return false;
                }
            }
        }
        true
    }
}
impl Drop for NativePeer {
    fn drop(&mut self) {
        if let Some(join) = &self.join {
            join.abort();
        }
    }
}

struct Lab {
    root: PathBuf,
    image: PathBuf,
    address: SocketAddr,
    origin: String,
    client: ApiClient,
    repository: Repository,
    gate: TcpListener,
    peer_script: PathBuf,
    build_script: PathBuf,
    rustc: PathBuf,
    processes: Arc<Mutex<Vec<Process>>>,
    controller: usize,
    worker: Option<usize>,
    serial: usize,
    n: usize,
    c: usize,
    progress: Value,
}
impl Lab {
    async fn create(
        root: PathBuf,
        n: usize,
        c: usize,
        processes: Arc<Mutex<Vec<Process>>>,
    ) -> Self {
        let image = root.join("borg-runtime");
        fs::copy(env!("CARGO_BIN_EXE_borg-runtime"), &image).unwrap();
        fs::set_permissions(&image, fs::Permissions::from_mode(0o500)).unwrap();
        let rustc =
            PathBuf::from(std::env::var("RUSTC").expect("selected absolute RUSTC required"));
        assert!(rustc.is_absolute() && rustc.is_file());
        let seed = root.join("seed");
        fs::create_dir(&seed).unwrap();
        fs::create_dir(seed.join("src")).unwrap();
        fs::write(seed.join("answer.txt"), "before\n").unwrap();
        fs::write(seed.join("src/main.rs"),"fn main(){println!(\"{}\",include_str!(\"../answer.txt\").trim());}\n#[test] fn corrected_source(){assert_eq!(include_str!(\"../answer.txt\").trim(),\"after\");}\n").unwrap();
        git(&seed, &["init", "-q"]);
        git(&seed, &["add", "."]);
        git(&seed, &["commit", "-qm", "controlled Rust load source"]);
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
                repository_id: "load-repository".into(),
                registration_revision: "1".into(),
            }),
            url: remote.to_string_lossy().into(),
            start_commit,
            checkpoint_remote: remote.to_string_lossy().into(),
        };
        let gate = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_script = root.join("codex_peer.py");
        fs::write(&peer_script, include_str!("load/fixtures/codex_peer.py")).unwrap();
        fs::set_permissions(&peer_script, fs::Permissions::from_mode(0o700)).unwrap();
        let build_script = root.join("build_observer.py");
        fs::write(
            &build_script,
            include_str!("load/fixtures/build_observer.py"),
        )
        .unwrap();
        save(
            peer_script.with_extension("json"),
            &json!({"port":gate.local_addr().unwrap().port(),"evidence":root,"release":root.join("coding-release")}),
        );
        save(
            build_script.with_extension("json"),
            &json!({"rustc":rustc,"evidence":root}),
        );
        fs::create_dir(root.join("home")).unwrap();
        fs::create_dir(root.join("home/code")).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let origin = format!("http://{address}");
        let mut limits = borg_runtime::limits::RuntimeLimits::seed().unwrap();
        limits.worker.coding_capacity = c;
        limits.worker.native_capacity = n;
        limits.validate().unwrap();
        save(root.join("limits.json"), &limits);
        let client = ApiClient::new(&origin, Arc::new(limits)).unwrap();
        let mut lab = Self {
            root,
            image,
            address,
            origin,
            client,
            repository,
            gate,
            peer_script,
            build_script,
            rustc,
            processes,
            controller: 0,
            worker: None,
            serial: 0,
            n,
            c,
            progress: json!({"native_target":n,"coding_capacity":c,"coding_tasks":c+1,"status":"running"}),
        };
        lab.restart_controller(false).await;
        lab
    }
    fn progress(&mut self, phase: &str, data: Value) {
        self.progress["phase"] = json!(phase);
        self.progress["observed_unix_ns"] = json!(stamp());
        self.progress[phase] = data;
        save(self.root.join("progress.json"), &self.progress);
    }
    fn spawn(&mut self, name: String, args: &[String], native: bool) -> usize {
        let log = File::create(self.root.join(format!("{name}.log"))).unwrap();
        let mut command = Command::new(&self.image);
        command
            .args(args)
            .env_clear()
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", self.rustc.parent().unwrap().display()),
            )
            .env("HOME", self.root.join("home"))
            .env("BORG_WORKER_HOME", self.root.join("home"))
            .env("LANG", "C")
            .env("LC_ALL", "C")
            .env("TZ", "UTC")
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .process_group(0);
        if native {
            command.env("OPENAI_API_KEY", KEY);
        }
        let child = command.spawn().unwrap();
        save(
            self.root.join(format!("{name}-started.json")),
            &json!({"pid":child.id(),"argv":args,"image":self.image,"started_unix_ns":stamp(),"credential":"synthetic only; clean environment"}),
        );
        let mut processes = self.processes.lock().unwrap();
        let index = processes.len();
        processes.push(Process {
            child,
            root: self.root.clone(),
            name,
            closed: false,
        });
        index
    }
    async fn restart_controller(&mut self, restart: bool) {
        if restart {
            self.processes.lock().unwrap()[self.controller].close(true);
        }
        self.serial += 1;
        self.controller = self.spawn(
            format!("controller-{}", self.serial),
            &[
                "controller".into(),
                "--limits".into(),
                self.root.join("limits.json").to_string_lossy().into(),
                "--db".into(),
                self.root.join("tasks.sqlite").to_string_lossy().into(),
                "--listen".into(),
                self.address.to_string(),
                "--origin".into(),
                self.origin.clone(),
            ],
            false,
        );
        let end = Instant::now() + Duration::from_secs(30);
        loop {
            if matches!(
                self.client
                    .call(Operation::ListTasks {
                        after: None,
                        limit: 1
                    })
                    .await,
                Ok(Reply {
                    outcome: Outcome::Ok { .. },
                    ..
                })
            ) {
                break;
            }
            assert!(Instant::now() < end, "controller startup failed");
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }
    fn start_worker(&mut self, endpoint: &str) {
        self.serial += 1;
        self.worker = Some(self.spawn(
            format!("mixed-worker-{}", self.serial),
            &[
                "worker".into(),
                "--limits".into(),
                self.root.join("limits.json").to_string_lossy().into(),
                "--controller".into(),
                self.origin.clone(),
                "--id".into(),
                "mixed-worker".into(),
                "--incarnation".into(),
                format!("load-inc-{}", self.serial),
                "--workspace-root".into(),
                self.root.join("workspaces").to_string_lossy().into(),
                "--codex".into(),
                self.peer_script.to_string_lossy().into(),
                "--coding-capacity".into(),
                self.c.to_string(),
                "--native-capacity".into(),
                self.n.to_string(),
                "--native-endpoint".into(),
                endpoint.into(),
            ],
            true,
        ));
    }
    async fn tasks(&self) -> Vec<Task> {
        let mut after = None;
        let mut all = Vec::new();
        loop {
            let reply = self
                .client
                .call(Operation::ListTasks {
                    after: after.clone(),
                    limit: 100,
                })
                .await
                .unwrap();
            let Outcome::Ok {
                data: Data::Tasks(tasks),
            } = reply.outcome
            else {
                panic!("task listing failed")
            };
            let count = tasks.len();
            after = tasks.last().map(|t| t.id.clone());
            all.extend(tasks);
            if count < 100 {
                break;
            }
            assert!(all.len() <= 300);
        }
        save(self.root.join("last-tasks.json"), &all);
        all
    }
    async fn get(&self, id: &str) -> Task {
        actual_task(
            self.client
                .call(Operation::GetTask { task_id: id.into() })
                .await
                .unwrap(),
        )
    }
    fn input(&self, index: usize, coding: bool) -> TaskInput {
        let instruction = if coding {
            "Set answer.txt to after and preserve the configured Rust source".into()
        } else {
            format!("controlled-native-{index}")
        };
        TaskInput { instruction_source: None,
            pull_request: None,
            resolved_limits: None,
            repository_read: None,
            instruction: instruction.clone(),
            definition: Definition {
                charter: None,
                limit_overrides: None,
                inference: Some(InferenceSettings::selected()),
                execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
                id: if coding { "load-coding" } else { "load-native" }.into(),
                revision: "1".into(),
                executor: if coding {
                    ExecutorKind::Coding
                } else {
                    ExecutorKind::Native
                },
            },
            repository: if coding {
                let mut submitted = self.repository.clone();
                submitted.url.clear();
                submitted.checkpoint_remote.clear();
                Some(submitted)
            } else {
                None
            },
            acceptance_commands: if coding {
                vec![
                    vec![
                        "/usr/bin/python3".into(),
                        "-I".into(),
                        "-S".into(),
                        "-B".into(),
                        self.build_script.to_string_lossy().into(),
                        "test".into(),
                    ],
                    vec!["./acceptance".into()],
                    vec![
                        "/usr/bin/python3".into(),
                        "-I".into(),
                        "-S".into(),
                        "-B".into(),
                        self.build_script.to_string_lossy().into(),
                        "binary".into(),
                    ],
                ]
            } else {
                vec![]
            },
            output_schema: if coding {
                None
            } else {
                Some(
                    json!({"type":"object","properties":{"case":{"const":instruction}},"required":["case"],"additionalProperties":false}),
                )
            },
            limits: Limits {
                deadline_unix_ms: stamp() / 1_000_000 + 1_200_000,
                model_requests: 1,
                output_bytes: 4096,
                result_bytes: self.client.limits().task_defaults.result_bytes,
                max_delegation_depth: 0,
            },
            dependencies: vec![],
            native_workflow: None,
        }
    }
    async fn gate(&self) -> (TcpStream, Value) {
        let (stream, _) = tokio::time::timeout(PHASE, self.gate.accept())
            .await
            .expect("coding turn gate not reached")
            .unwrap();
        let mut reader = BufReader::new(stream);
        let mut line = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let byte = reader.read_u8().await?;
                line.push(byte);
                if byte == b'\n' {
                    break;
                }
                if line.len() > 8192 {
                    return Err(io::Error::other("coding gate bound"));
                }
            }
            Ok::<_, io::Error>(())
        })
        .await
        .unwrap()
        .unwrap();
        assert!(line.len() <= 8192 && line.ends_with(b"\n"));
        let value = serde_json::from_slice(&line).unwrap();
        (reader.into_inner(), value)
    }
}

async fn run_case(
    lab: &mut Lab,
    endpoint: &str,
    state: Arc<Mutex<NativeState>>,
    release: watch::Sender<bool>,
    mut events: mpsc::Receiver<()>,
) {
    let registration = borg_runtime::repository_catalogue::Registration {
        pull_request_target: None,
        repository_id: "load-repository".into(),
        registration_revision: "1".into(),
        display_name: "Local load repository".into(),
        source_endpoint: lab.repository.url.clone(),
        checkpoint_endpoint: lab.repository.checkpoint_remote.clone(),
        source_store_key: "remote.git".into(),
        checkpoint_store_key: "remote.git".into(),
    };
    assert!(matches!(
        lab.client
            .call(Operation::RegisterRepository {
                operation_id: "load-register-repository".into(),
                registration
            })
            .await
            .unwrap()
            .outcome,
        Outcome::Ok {
            data: Data::RepositoryRegistration(_)
        }
    ));
    let mut submissions = Vec::new();
    let mut native_ids = BTreeSet::new();
    let mut coding_ids = Vec::new();
    let mut started = BTreeMap::new();
    for i in 0..lab.n {
        let op = Operation::SubmitTask {
            operation_id: format!("load-native-submit-{i}"),
            input: lab.input(i, false),
        };
        let reply = lab.client.call(op.clone()).await.unwrap();
        let task = actual_task(reply.clone());
        started.insert(task.id.clone(), Instant::now());
        native_ids.insert(task.id.clone());
        submissions.push((op, reply.outcome));
    }
    lab.progress("submitted_native", json!({"count":native_ids.len()}));
    lab.start_worker(endpoint);
    let end = Instant::now() + PHASE;
    loop {
        let snapshot = native_snapshot(&state);
        lab.progress("received_held", snapshot.clone());
        assert!(
            snapshot["errors"].as_array().unwrap().is_empty(),
            "native peer error"
        );
        if snapshot["held"].as_u64() == Some(lab.n as u64) {
            break;
        }
        assert!(
            Instant::now() < end,
            "native target never became actually in-flight"
        );
        match tokio::time::timeout(Duration::from_secs(1), events.recv()).await {
            Ok(Some(())) => {}
            Ok(None) => panic!("native peer event owner closed"),
            Err(_) => {
                let tasks = lab.tasks().await;
                assert!(
                    tasks
                        .iter()
                        .all(|t| !matches!(t.status, TaskStatus::Failed | TaskStatus::Cancelled)),
                    "native failed before held target; original task/outbox evidence retained"
                );
            }
        }
    }
    for i in 0..=lab.c {
        let op = Operation::SubmitTask {
            operation_id: format!("load-coding-submit-{i}"),
            input: lab.input(i, true),
        };
        let reply = lab.client.call(op.clone()).await.unwrap();
        let task = actual_task(reply.clone());
        started.insert(task.id.clone(), Instant::now());
        coding_ids.push(task.id.clone());
        submissions.push((op, reply.outcome));
    }
    let mut gates = Vec::new();
    let mut turns = Vec::new();
    let mut cwd = BTreeSet::new();
    for _ in 0..lab.c {
        let (gate, observed) = lab.gate().await;
        assert_eq!(observed["before"], "before\n");
        assert_eq!(observed["auto_released"], false);
        assert!(cwd.insert(observed["cwd"].as_str().unwrap().to_owned()));
        turns.push(observed);
        gates.push(gate);
    }
    for turn in &turns {
        let pid = turn["pid"].as_u64().unwrap();
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
        assert!(!stat.rsplit_once(") ").unwrap().1.starts_with('Z'));
    }
    let extra = lab.get(coding_ids.last().unwrap()).await;
    assert_eq!(extra.status, TaskStatus::Queued);
    assert!(
        extra.attempt.is_none(),
        "capacity overflow task already has attempt"
    );
    assert_eq!(state.lock().unwrap().held, lab.n);
    lab.progress("coding_turns",json!({"simultaneous_controlled_turns":turns,"extra_queued":extra.id,"native":native_snapshot(&state)}));
    fs::write(
        lab.root.join("coding-release"),
        b"all initial coding turns observed\n",
    )
    .unwrap();
    for mut gate in gates {
        gate.write_all(b"G").await.unwrap();
    }
    let (_, extra_turn) = lab.gate().await;
    assert_eq!(extra_turn["auto_released"], true);
    assert!(cwd.insert(extra_turn["cwd"].as_str().unwrap().to_owned()));
    assert!(
        lab.tasks()
            .await
            .iter()
            .any(|t| coding_ids[..lab.c].contains(&t.id)
                && t.status == TaskStatus::Succeeded
                && t.attempt.as_ref().is_some_and(|a| a.stopped)),
        "extra coding turn started before a verified prior completion"
    );
    turns.push(extra_turn);
    save(lab.root.join("coding-turns.json"), &turns);
    let end = Instant::now() + PHASE;
    let coding;
    loop {
        let tasks = lab.tasks().await;
        assert!(
            tasks
                .iter()
                .all(|t| !matches!(t.status, TaskStatus::Failed | TaskStatus::Cancelled)),
            "task failed during coding overlap"
        );
        let done = tasks
            .iter()
            .filter(|t| coding_ids.contains(&t.id) && t.status == TaskStatus::Succeeded)
            .cloned()
            .collect::<Vec<_>>();
        lab.progress(
            "coding_completed",
            json!({"accepted":done.len(),"target":lab.c+1,"native":native_snapshot(&state)}),
        );
        if done.len() == lab.c + 1 {
            coding = done;
            break;
        }
        assert!(
            Instant::now() < end,
            "coding completions exceeded phase bound"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for task in &coding {
        let result = task.result.as_ref().unwrap();
        assert_eq!(result.validation.len(), 3);
        assert!(task.attempt.as_ref().unwrap().stopped);
        let cp = task.selected_checkpoint.as_ref().unwrap();
        assert_eq!(result.checkpoint.as_ref(), Some(cp));
        assert_eq!(
            git(
                &lab.root,
                &["--git-dir", &cp.repository, "rev-parse", &cp.remote_ref]
            ),
            cp.commit
        );
        assert_eq!(
            git(
                &lab.root,
                &[
                    "--git-dir",
                    &cp.repository,
                    "show",
                    &format!("{}:answer.txt", cp.commit)
                ]
            ),
            "after"
        );
    }
    let builds = fs::read_dir(&lab.root)
        .unwrap()
        .filter_map(|entry| {
            let p = entry.unwrap().path();
            p.file_name()
                .unwrap()
                .to_str()
                .filter(|n| n.starts_with("build-") && n.ends_with(".json"))
                .map(|_| serde_json::from_slice::<Value>(&fs::read(&p).unwrap()).unwrap())
        })
        .collect::<Vec<_>>();
    assert_eq!(builds.len(), 2 * (lab.c + 1));
    let mut build_events = Vec::new();
    let mut built_cwd = BTreeMap::<String, BTreeSet<String>>::new();
    for build in &builds {
        assert_eq!(build["exit"], 0);
        built_cwd
            .entry(build["cwd"].as_str().unwrap().into())
            .or_default()
            .insert(build["kind"].as_str().unwrap().into());
        if let (Some(first), Some(last)) = (
            build["first_live_monotonic_ns"].as_u64(),
            build["last_live_monotonic_ns"].as_u64(),
        ) {
            assert!(last >= first);
            build_events.push((first, 1i32));
            build_events.push((last, -1i32));
        }
    }
    assert_eq!(built_cwd.len(), lab.c + 1);
    assert!(built_cwd.values().all(|v| v.len() == 2));
    build_events.sort_by_key(|(time, delta)| (*time, -*delta));
    let (mut active, mut peak) = (0i32, 0i32);
    for (_, delta) in build_events {
        active += delta;
        peak = peak.max(active);
    }
    let snapshot = native_snapshot(&state);
    assert_eq!(snapshot["held"], lab.n);
    assert_eq!(snapshot["responses"], 0);
    assert!(snapshot["errors"].as_array().unwrap().is_empty());
    lab.progress("builds_completed",json!({"real_rustc_children":builds,"observed_compiler_peak":peak,"coding_capacity":lab.c,"coding_tasks_completed":lab.c+1,"native_held":snapshot}));
    release.send(true).unwrap();
    lab.progress("native_released", json!({"count":lab.n}));
    let end = Instant::now() + PHASE;
    let accepted;
    loop {
        let tasks = lab.tasks().await;
        assert!(
            tasks
                .iter()
                .all(|t| !matches!(t.status, TaskStatus::Failed | TaskStatus::Cancelled)),
            "native failed after response release"
        );
        let count = tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Succeeded)
            .count();
        lab.progress(
            "accepted",
            json!({"count":count,"target":lab.n+lab.c+1,"native":native_snapshot(&state)}),
        );
        if count == lab.n + lab.c + 1 {
            accepted = tasks;
            break;
        }
        assert!(
            Instant::now() < end,
            "native acceptance exceeded phase bound"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let latencies = accepted
        .iter()
        .map(|t| (t.id.clone(), started[&t.id].elapsed().as_millis()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        accepted
            .iter()
            .map(|t| &t.session_id)
            .collect::<BTreeSet<_>>()
            .len(),
        accepted.len()
    );
    for task in &accepted {
        assert_eq!(task.assigned_agent_id, task.input.definition.id);
        assert_eq!(task.attempt.as_ref().unwrap().session_id, task.session_id);
        assert!(
            task.attempt
                .as_ref()
                .is_some_and(|a| a.stopped && !a.uncertain)
        );
        if native_ids.contains(&task.id) {
            assert_eq!(
                task.result.as_ref().unwrap().output,
                Some(json!({"case":task.input.instruction}))
            );
        }
    }
    for (op, outcome) in &submissions {
        assert_eq!(
            &lab.client.call(op.clone()).await.unwrap().outcome,
            outcome,
            "duplicate original operation changed result"
        );
    }
    lab.processes.lock().unwrap()[lab.worker.unwrap()].close(false);
    lab.restart_controller(true).await;
    lab.start_worker(endpoint);
    for task in &accepted {
        assert_eq!(lab.get(&task.id).await, *task);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(lab.tasks().await.len(), lab.n + lab.c + 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), lab.gate.accept())
            .await
            .is_err(),
        "restarted worker repeated a coding turn"
    );
    let snapshot = native_snapshot(&state);
    assert_eq!(snapshot["received"].as_object().unwrap().len(), lab.n);
    assert_eq!(snapshot["responses"], lab.n);
    assert_eq!(snapshot["held"], 0);
    assert!(snapshot["errors"].as_array().unwrap().is_empty());
    lab.progress("restart",json!({"exact_terminal_tasks":accepted.len(),"duplicate_original_operations":submissions.len(),"restarted_controller":true,"restarted_worker":true,"native":snapshot}));
    save(lab.root.join("accepted.json"), &accepted);
    save(
        lab.root.join("summary.json"),
        &json!({"status":"scenario_complete_pending_source_guard_and_cleanup","native_target":lab.n,"coding_capacity":lab.c,"coding_completed":lab.c+1,"total_completed":accepted.len(),"refused":0,"unknown":0,"native_peak":lab.n,"compiler_peak_sampled_live":peak,"latency_to_final_observation_ms":latencies,"scope":"one local actual worker/controller; controlled HTTP/Codex peers; no production/real-provider/supplied-Codex/cluster-capacity proof"}),
    );
}

fn source_files(path: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(path).unwrap() {
        let p = entry.unwrap().path();
        if p.is_dir() {
            source_files(&p, out)
        } else if p.extension().and_then(|s| s.to_str()) == Some("rs") {
            out.push(p)
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit local load ramp; root runs closed BORG_LOAD_NATIVE/BORG_LOAD_CODING values"]
// Traceability scope: Supports per-workspace owned build behavior, not four-host representative builds, real Codex concurrency or resource enforcement.
//= spec/impl/v2/implementation.md#initial-four-host-qualification
//= type=test
//# Every coding workspace MUST be able to run repository builds and tests.
// Traceability scope: Positive browser root-result navigation is a separate test gap; validation strings are not protected-check proof. The load oracle asserts exact selected checkpoint and three returned check outcomes; it does not provide protected checker provenance.
//= spec/impl/v2/implementation.md#observable-end-to-end-result
//= type=test
//# The initial system MUST return references to the source changes and validation results produced by a successful task.
async fn local_mixed_native_and_coding_load() {
    let n = selected("BORG_LOAD_NATIVE", &[32, 64, 128, 256], 32);
    let c = selected("BORG_LOAD_CODING", &[2, 4, 8], 2);
    let base = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("artifacts/v2/build-60156282/local-load-4bjl85jp/runs");
    fs::create_dir_all(&base).unwrap();
    let root = if let Some(path) = std::env::var_os("BORG_LOAD_DIR") {
        let path = PathBuf::from(path);
        assert!(path.is_absolute() && path.ancestors().all(|p| !p.is_symlink()));
        fs::create_dir(&path).expect("BORG_LOAD_DIR must not exist");
        path
    } else {
        (0..8)
            .find_map(|i| {
                let p = base.join(format!("n{n}-c{c}-{}-{}-{i}", std::process::id(), stamp()));
                match fs::create_dir(&p) {
                    Ok(()) => Some(p),
                    Err(e) if e.kind() == io::ErrorKind::AlreadyExists => None,
                    Err(e) => panic!("fresh load directory: {e}"),
                }
            })
            .expect("fresh load directory collision bound")
    };
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    println!("LOCAL_LOAD_EVIDENCE={}", root.display());
    save(
        root.join("progress.json"),
        &json!({"status":"starting","native":n,"coding_capacity":c}),
    );
    let processes = Arc::new(Mutex::new(Vec::new()));
    let mut peer = NativePeer::start(n).await;
    let endpoint = peer.endpoint.clone();
    let state = peer.state.clone();
    let release = peer.release.clone();
    let events = peer.events.take().unwrap();
    let scenario_root = root.clone();
    let owned = processes.clone();
    let mut scenario = tokio::spawn(async move {
        let mut lab = Lab::create(scenario_root, n, c, owned).await;
        let mut pins = vec![
            std::env::current_exe().expect("running load-test executable identity"),
            lab.image.clone(),
            lab.rustc.clone(),
            lab.peer_script.clone(),
            lab.build_script.clone(),
            Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"),
            Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.lock"),
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/load.rs"),
        ];
        source_files(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
            &mut pins,
        );
        pins.sort();
        let hashes = Command::new("/usr/bin/sha256sum")
            .args(&pins)
            .output()
            .unwrap();
        assert!(hashes.status.success());
        fs::write(lab.root.join("inputs.sha256"), &hashes.stdout).unwrap();
        run_case(&mut lab, &endpoint, state, release, events).await;
        let after = Command::new("/usr/bin/sha256sum")
            .args(&pins)
            .output()
            .unwrap();
        assert!(after.status.success());
        fs::write(lab.root.join("inputs-after.sha256"), &after.stdout).unwrap();
        assert_eq!(
            hashes.stdout, after.stdout,
            "source/image changed during load"
        );
    });
    let result = match tokio::time::timeout(Duration::from_secs(1200), &mut scenario).await {
        Ok(result) => result,
        Err(_) => {
            scenario.abort();
            scenario.await
        }
    };
    if let Err(error) = &result {
        save(
            root.join("failure.json"),
            &json!({"status":"FAIL","error":error.to_string(),"evidence":root,"last_progress":"progress.json","task_snapshot":"last-tasks.json","outboxes_preserved":true}),
        );
    }
    for process in processes.lock().unwrap().iter_mut().rev() {
        process.close(false);
    }
    let peer_closed = peer.close().await;
    let registry = root
        .join("workspaces/.borg-worker")
        .join(borg_runtime::worker::process::component("mixed-worker"))
        .join("processes");
    let mut cleanup_errors = Vec::new();
    let cleanup_deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    if registry.is_dir() {
        for entry in fs::read_dir(&registry).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                match tokio::time::timeout_at(
                    cleanup_deadline,
                    borg_runtime::worker::process::reconcile_processes(
                        &path,
                        &borg_runtime::limits::RuntimeLimits::seed().unwrap().worker,
                    ),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => cleanup_errors.push(error),
                    Err(_) => {
                        cleanup_errors.push("registry cleanup deadline; evidence retained".into());
                        break;
                    }
                }
            }
        }
    }
    if !peer_closed {
        cleanup_errors.push("native peer join unverified".into());
    }
    save(
        root.join("cleanup.json"),
        &json!({"owned_application_processes_closed":processes.lock().unwrap().iter().all(|p|p.closed),"native_peer_joined":peer_closed,"registry_errors":cleanup_errors,"native":native_snapshot(&peer.state)}),
    );
    let passed = result.is_ok() && cleanup_errors.is_empty();
    let mut progress: Value =
        serde_json::from_slice(&fs::read(root.join("progress.json")).unwrap()).unwrap();
    progress["status"] = json!(if passed { "PASS" } else { "FAIL" });
    progress["cleanup_complete"] = json!(cleanup_errors.is_empty());
    save(root.join("progress.json"), &progress);
    save(
        root.join("result.json"),
        &json!({"status":if passed{"PASS"}else{"FAIL"},"native_target":n,"coding_capacity":c,"coding_tasks":c+1,"source_guard_passed":result.is_ok(),"cleanup_errors":cleanup_errors,"finished_unix_ns":stamp()}),
    );
    assert!(
        cleanup_errors.is_empty(),
        "owned registry cleanup incomplete; evidence {}",
        root.display()
    );
    assert!(
        result.is_ok(),
        "local load failed; evidence {}",
        root.display()
    );
}
