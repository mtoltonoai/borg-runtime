//! Actual Borg stdio MCP bridge and HTTP/controller transactions. The test is
//! the controlled executor-side peer; no Codex binary, model or Git is launched.
use borg_runtime::{
    client::ApiClient,
    controller::{Controller, ControllerConfig},
    limits::{DefinitionLimitField, ExecutionLimitOverrides, RuntimeLimits},
    mcp::{self, CallResult, Caller, Invocation},
    protocol::*,
    repository_catalogue::Registration,
    worker::{codex_mcp, process},
};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request as HttpRequest, Response, body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use std::{
    io,
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    process::{Child, ChildStdin, ChildStdout},
    sync::Notify,
};

static NEXT: AtomicU64 = AtomicU64::new(1);
const WAIT: Duration = Duration::from_secs(15);

struct Background(Option<tokio::task::JoinHandle<()>>);
impl Background {
    async fn stop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
            let _ = task.await;
        }
    }
}
impl Drop for Background {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}
struct Lab {
    root: PathBuf,
    db: PathBuf,
    _controller: Controller,
    api: ApiClient,
    url: String,
    server: Background,
    limits: Arc<RuntimeLimits>,
    registration: Registration,
    worker: Worker,
    acknowledged: Vec<String>,
}
impl Lab {
    async fn new(label: &str, policy_enabled: bool) -> Self {
        let mut limits = RuntimeLimits::seed().unwrap();
        // Leave room for the existing context-widening discriminator.
        limits.execution.native.context_bytes = 65_536;
        if !policy_enabled {
            limits.definition_updates = None;
        }
        Self::with_limits(label, limits).await
    }
    async fn with_limits(label: &str, mut limits: RuntimeLimits) -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("artifacts/operational-feedback-codex")
            .join(format!(
                "{label}-{}-{}-{}",
                process::now_ms(),
                std::process::id(),
                NEXT.fetch_add(1, Ordering::SeqCst)
            ));
        std::fs::create_dir_all(&root).unwrap();
        let db = root.join("tasks.sqlite");
        limits.controller.command_wait_ms = 10;
        limits.validate().unwrap();
        let limits = Arc::new(limits);
        let controller = Controller::open_with_config(
            &db,
            ControllerConfig {
                limits: limits.clone(),
                validation_executable: env!("CARGO_BIN_EXE_borg-runtime").into(),
                ..Default::default()
            },
        )
        .unwrap();
        let listener = borg_runtime::server::listen("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (c, origin) = (controller.clone(), url.clone());
        let server = Background(Some(tokio::spawn(async move {
            borg_runtime::server::serve(c, listener, origin)
                .await
                .unwrap();
        })));
        let api = ApiClient::new(&url, limits.clone()).unwrap();
        let registration = Registration {
            pull_request_target: None,
            repository_id: "coding-fixture".into(),
            registration_revision: "v1".into(),
            display_name: "Metadata only; never fetched".into(),
            source_endpoint: "https://example.invalid/borg-fixture.git".into(),
            checkpoint_endpoint: "origin".into(),
            source_store_key: "source".into(),
            checkpoint_store_key: "checkpoints".into(),
        };
        assert!(matches!(
            api.call(Operation::RegisterRepository {
                operation_id: "register-repository".into(),
                registration: registration.clone()
            })
            .await
            .unwrap()
            .outcome,
            Outcome::Ok { .. }
        ));
        let worker = Worker {
            id: "controlled-coding-worker".into(),
            incarnation: "incarnation-1".into(),
            coding_capacity: 2,
            native_capacity: 0,
        };
        assert!(matches!(
            api.call(Operation::RegisterWorker {
                operation_id: "register-worker".into(),
                worker: worker.clone()
            })
            .await
            .unwrap()
            .outcome,
            Outcome::Ok { .. }
        ));
        Self {
            root,
            db,
            _controller: controller,
            api,
            url,
            server,
            limits,
            registration,
            worker,
            acknowledged: vec![],
        }
    }
    fn input(&self, result_bound: usize) -> TaskInput {
        let mut collaboration = self.limits.execution.collaboration.clone();
        collaboration.complete_result_bytes = result_bound;
        TaskInput { instruction_source: None,
            pull_request: None,
            resolved_limits: None,
            repository_read: None,
            instruction:
                "Controlled bridge settings qualification; no executor or build is launched.".into(),
            definition: Definition {
                charter: None,
                id: "coding-agent".into(),
                revision: "original-v1".into(),
                executor: ExecutorKind::Coding,
                inference: Some(InferenceSettings::selected()),
                execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
                limit_overrides: Some(Box::new(ExecutionLimitOverrides {
                    collaboration: Some(collaboration),
                    ..Default::default()
                })),
            },
            repository: Some(Repository {
                pull_request_target: None,
                registration: Some(self.registration.binding()),
                url: String::new(),
                start_commit: "a".repeat(40),
                checkpoint_remote: String::new(),
            }),
            acceptance_commands: vec![vec!["/usr/bin/true".into()]],
            output_schema: None,
            limits: Limits {
                deadline_unix_ms: process::now_ms() + 120_000,
                model_requests: 2,
                output_bytes: self.limits.task_defaults.output_bytes,
                result_bytes: self.limits.task_defaults.result_bytes,
                max_delegation_depth: 1,
            },
            dependencies: vec![],
            native_workflow: None,
        }
    }
    async fn start(&mut self, input: TaskInput, id: &str) -> Task {
        let task = task(
            self.api
                .call(Operation::SubmitTask {
                    operation_id: format!("submit-{id}"),
                    input,
                })
                .await
                .unwrap(),
        );
        let end = tokio::time::Instant::now() + WAIT;
        let attempt = loop {
            let reply = self
                .api
                .call(Operation::AwaitCommands {
                    operation_id: format!("await-{}", NEXT.fetch_add(1, Ordering::SeqCst)),
                    worker_id: self.worker.id.clone(),
                    incarnation: self.worker.incarnation.clone(),
                    acknowledged: self.acknowledged.clone(),
                })
                .await
                .unwrap();
            if let Outcome::Ok {
                data: Data::Commands(commands),
            } = reply.outcome
            {
                let mut found = None;
                for command in commands {
                    if let WorkerCommand::RunAttempt {
                        command_id,
                        attempt,
                        ..
                    } = command
                    {
                        if !self.acknowledged.contains(&command_id) {
                            self.acknowledged.push(command_id);
                        }
                        if attempt.task_id == task.id {
                            found = Some(attempt);
                        }
                    }
                }
                if let Some(attempt) = found {
                    break attempt;
                }
            }
            assert!(
                tokio::time::Instant::now() < end,
                "controlled coding launch not assigned"
            );
        };
        self.report(
            &attempt,
            Observation::Started {
                thread_id: Some(thread(&attempt.id)),
                turn_id: Some(format!("turn-{}", attempt.id)),
            },
        )
        .await;
        self.get(&task.id).await
    }
    async fn report(&self, attempt: &Attempt, observation: Observation) {
        let reply = self
            .api
            .call(Operation::ObserveWorker {
                report: Report {
                    report_id: format!("report-{}", NEXT.fetch_add(1, Ordering::SeqCst)),
                    worker_id: attempt.worker_id.clone(),
                    incarnation: attempt.worker_incarnation.clone(),
                    task_id: attempt.task_id.clone(),
                    attempt_id: attempt.id.clone(),
                    generation: attempt.generation,
                    observation,
                },
            })
            .await
            .unwrap();
        assert!(matches!(reply.outcome, Outcome::Ok { .. }), "{reply:?}");
    }
    async fn get(&self, id: &str) -> Task {
        task(
            self.api
                .call(Operation::GetTask { task_id: id.into() })
                .await
                .unwrap(),
        )
    }
    async fn selected(&self) -> Definition {
        match self
            .api
            .call(Operation::GetAgent {
                agent_id: "coding-agent".into(),
            })
            .await
            .unwrap()
            .outcome
        {
            Outcome::Ok {
                data: Data::Agent(agent),
            } => agent.definition,
            other => panic!("{other:?}"),
        }
    }
    fn config(&self, task: &Task, url: &str, label: &str) -> codex_mcp::Config {
        let attempt = task.attempt.as_ref().unwrap();
        codex_mcp::Config { instruction_source: None,
            execution_profile: None,
            runtime_limits: (*task.input.resolved_limits.as_ref().unwrap().global_metadata).clone(),
            limits: task.input.execution_limits().unwrap().collaboration.clone(),
            worker_limits: self.limits.worker.clone(),
            controller_url: url.into(),
            caller: Caller {
                task_id: task.id.clone(),
                attempt_id: attempt.id.clone(),
                generation: attempt.generation,
                worker_id: attempt.worker_id.clone(),
                incarnation: attempt.worker_incarnation.clone(),
            },
            deadline_unix_ms: attempt
                .deadline_unix_ms
                .min(task.input.limits.deadline_unix_ms),
            state: self.root.join(label),
        }
    }
    fn changes(&self, task: &Task, changes: Value) -> Value {
        json!({"agent_id":task.input.definition.id,"expected_revision":task.input.definition.revision,
            "expected_global_revision":self.limits.revision,
            "expected_policy_revision":self.limits.definition_updates.as_ref().map(|p|p.revision.as_str()).unwrap_or("disabled-policy"),"changes":changes})
    }
    async fn cancel(&self, task: &Task) {
        let current = self.get(&task.id).await;
        if !matches!(
            current.status,
            TaskStatus::Cancelled | TaskStatus::Failed | TaskStatus::Succeeded
        ) {
            assert!(matches!(
                self.api
                    .call(Operation::CancelTask {
                        operation_id: format!("cancel-{}", task.id),
                        task_id: task.id.clone(),
                        expected_revision: current.revision
                    })
                    .await
                    .unwrap()
                    .outcome,
                Outcome::Ok { .. }
            ));
        }
    }
    async fn finish(mut self, tasks: &[Task], expected_counts: (i64, i64)) {
        let mut final_tasks = Vec::new();
        for task in tasks {
            self.cancel(task).await;
            self.report(
                task.attempt.as_ref().unwrap(),
                Observation::ProcessTreeStopped,
            )
            .await;
            let final_task = self.get(&task.id).await;
            assert!(final_task.attempt.as_ref().unwrap().stopped);
            final_tasks.push(final_task);
        }
        self.server.stop().await;
        let address = self.url.strip_prefix("http://").unwrap().to_owned();
        let connect = tokio::time::timeout(WAIT, tokio::net::TcpStream::connect(&address))
            .await
            .unwrap();
        assert_eq!(
            connect.unwrap_err().kind(),
            io::ErrorKind::ConnectionRefused,
            "owned controller listener did not close cleanly"
        );
        // The canonical owner intentionally holds an exclusive SQLite lock.
        // Drop every fixture client/owner before the final independent audit;
        // busy_timeout only waits for its asynchronous close, never changes it.
        drop(self.api);
        drop(self._controller);
        let db = rusqlite::Connection::open_with_flags(
            &self.db,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        db.busy_timeout(WAIT).unwrap();
        let counts: (i64, i64) = (
            db.query_row(
                "SELECT count(*) FROM definitions WHERE id='coding-agent'",
                [],
                |row| row.get(0),
            )
            .unwrap(),
            db.query_row(
                "SELECT coalesce((SELECT value FROM counters WHERE kind='definition_update'),0)",
                [],
                |row| row.get(0),
            )
            .unwrap(),
        );
        assert_eq!(
            counts, expected_counts,
            "closed-owner durable row and counter audit"
        );
        drop(db);
        process::atomic_json(&self.root.join("fixture-closure.json"),&json!({"tasks":final_tasks,"listener":address,"listener_closed":true,"controller_owners_dropped_before_audit":true,"definition_rows":counts.0,"definition_update_counter":counts.1,"scope":"controlled stopped observations after actual bridge reap; no installed worker/process-registry proof"})).unwrap();
    }
}
fn thread(attempt: &str) -> String {
    format!("thread-{attempt}")
}
fn task(reply: Reply) -> Task {
    match reply.outcome {
        Outcome::Ok {
            data: Data::Task(task),
        } => *task,
        other => panic!("{other:?}"),
    }
}

#[derive(Default)]
struct HeldReply {
    committed: Notify,
    release: Notify,
}

struct Proxy {
    url: String,
    calls: Arc<Mutex<Vec<Invocation>>>,
    lookups: Arc<AtomicU64>,
    task_reads: Arc<Mutex<Vec<String>>>,
    dropped: Arc<AtomicBool>,
    owner: Background,
}
impl Proxy {
    async fn start(upstream: String, drop_update: bool) -> Self {
        Self::with_hold(upstream, drop_update, None).await
    }
    async fn with_hold(upstream: String, drop_update: bool, hold: Option<Arc<HeldReply>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let lookups = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let task_reads = Arc::new(Mutex::new(Vec::new()));
        let (saved, reads, lost, tasks) = (calls.clone(), lookups.clone(), dropped.clone(), task_reads.clone());
        let owner = Background(Some(tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(WAIT)
                .build()
                .unwrap();
            let mut joins = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted=listener.accept()=>{
                        let (stream,_)=accepted.unwrap();let (client,upstream,saved,reads,lost,hold,tasks)=(client.clone(),upstream.clone(),saved.clone(),reads.clone(),lost.clone(),hold.clone(),tasks.clone());
                        joins.spawn(async move {
                            let service=service_fn(move|request:HttpRequest<Incoming>| {
                                let (client,upstream,saved,reads,lost,hold,tasks)=(client.clone(),upstream.clone(),saved.clone(),reads.clone(),lost.clone(),hold.clone(),tasks.clone());
                                async move {
                                    assert_eq!(request.method(),hyper::Method::POST);
                                    assert!(request.uri().query().is_none());
                                    let path=request.uri().path().to_owned();
                                    assert!(matches!(path.as_str(),"/mcp"|"/api/operations"));
                                    let caller=request.headers().get("x-borg-caller").cloned();let version=request.headers().get("mcp-protocol-version").cloned();
                                    let bytes=request.into_body().collect().await.unwrap().to_bytes();
                                    let rpc:Value=serde_json::from_slice(&bytes).unwrap();
                                    let update=path=="/mcp"&&rpc["method"]=="tools/call"&&rpc["params"]["name"]=="collab_definition_update";
                                    if path=="/api/operations" {
                                        // Current opted-in catalogues verify the accepted task through
                                        // the ordinary API. This fixture forwards only that typed read.
                                        assert!(caller.is_none()&&version.is_none());
                                        let api:Request=serde_json::from_slice(&bytes).unwrap();
                                        assert_eq!(api.protocol_revision,WIRE_REVISION);
                                        let Operation::GetTask{task_id}=api.command else {panic!("unexpected proxy API operation")};
                                        tasks.lock().unwrap().push(task_id);
                                    } else if rpc["method"]=="tools/call" {
                                        let actor:Caller=serde_json::from_slice(caller.as_ref().unwrap().as_bytes()).unwrap();
                                        let meta=&rpc["params"]["_meta"]["borg"];
                                        saved.lock().unwrap().push(Invocation{caller:actor,operation_id:meta["operation_id"].as_str().unwrap().into(),logical_call_id:meta["logical_call_id"].as_str().unwrap().into(),tool:rpc["params"]["name"].as_str().unwrap().into(),arguments:rpc["params"]["arguments"].clone()});
                                    } else if rpc["method"]=="borg/calls/get" {reads.fetch_add(1,Ordering::SeqCst);}
                                    let mut post=client.post(format!("{upstream}{path}")).header("content-type","application/json").body(bytes.to_vec());
                                    if let Some(caller)=caller {post=post.header("x-borg-caller",caller);}if let Some(version)=version {post=post.header("mcp-protocol-version",version);}
                                    let response=post.send().await.map_err(io::Error::other)?;let status=response.status();let bytes=response.bytes().await.map_err(io::Error::other)?;
                                    if update {if let Some(hold)=hold {
                                        hold.committed.notify_one();
                                        tokio::time::timeout(WAIT,hold.release.notified()).await.map_err(io::Error::other)?;
                                    }}
                                    // The upstream reply is complete: lose the transport after commit,
                                    // without manufacturing an HTTP status response for the bridge.
                                    if update&&drop_update&&!lost.swap(true,Ordering::SeqCst) {return Err(io::Error::new(io::ErrorKind::ConnectionReset,"controlled lost committed reply"));}
                                    Ok(Response::builder().status(status.as_u16()).body(Full::new(bytes)).unwrap())
                                }
                            });
                            let _=hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream),service).await;
                        });
                    },_=joins.join_next(),if !joins.is_empty()=>{}
                }
            }
        })));
        Self {
            url,
            calls,
            lookups,
            task_reads,
            dropped,
            owner,
        }
    }
}

struct Peer {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    config: codex_mcp::Config,
    sequence: u64,
    closed: bool,
}
impl Peer {
    async fn start(config: codex_mcp::Config) -> Self {
        process::private_directory(&config.state).unwrap();
        process::atomic_json(
            &config.state.join("borg-thread.json"),
            &json!({"thread_id":thread(&config.caller.attempt_id),"turn_start_sent":true}),
        )
        .unwrap();
        let path = config.state.join("bridge-config.json");
        process::atomic_json(&path, &config).unwrap();
        let log = std::fs::File::create(config.state.join(format!(
            "bridge-{}.log",
            NEXT.fetch_add(1, Ordering::SeqCst)
        )))
        .unwrap();
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_borg-runtime"))
            .args(["__collaboration_mcp", "--config"])
            .arg(path)
            .arg("--config-bytes")
            .arg(config.limits.config_bytes.to_string())
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", &config.state)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(log)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let stdin = child.stdin.take();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let mut peer = Self {
            child,
            stdin,
            stdout,
            config,
            sequence: 0,
            closed: false,
        };
        let init=peer.rpc("initialize",json!({"protocolVersion":mcp::VERSION,"capabilities":{},"clientInfo":{"name":"controlled-peer","version":"1"}})).await;
        assert_eq!(init["protocolVersion"], mcp::VERSION);
        peer
    }
    async fn rpc(&mut self, method: &str, params: Value) -> Value {
        let id = self.send_rpc(method, params).await;
        self.receive_rpc(id).await
    }
    async fn send_rpc(&mut self, method: &str, params: Value) -> u64 {
        self.sequence += 1;
        let id = self.sequence;
        let mut bytes =
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
                .unwrap();
        bytes.push(b'\n');
        assert!(bytes.len() <= self.config.limits.frame_bytes);
        tokio::time::timeout(WAIT, async {
            let stdin = self.stdin.as_mut().unwrap();
            stdin.write_all(&bytes).await.unwrap();
            stdin.flush().await.unwrap();
        })
        .await
        .unwrap();
        id
    }
    async fn receive_rpc(&mut self, id: u64) -> Value {
        let value = self.receive_frame(id).await;
        assert!(value.get("error").is_none(), "{value:?}");
        value["result"].clone()
    }
    async fn receive_frame(&mut self, id: u64) -> Value {
        let mut line = Vec::new();
        let n = tokio::time::timeout(WAIT, self.stdout.read_until(b'\n', &mut line))
            .await
            .unwrap()
            .unwrap();
        assert!(
            n > 0 && n <= self.config.limits.frame_bytes,
            "bridge did not return a bounded complete frame"
        );
        let value: Value = serde_json::from_slice(&line).unwrap();
        assert_eq!(value["jsonrpc"], "2.0");
        assert_eq!(value["id"], id);
        value
    }
    async fn call(&mut self, logical: &str, tool: &str, args: Value) -> CallResult {
        let body=self.rpc("tools/call",json!({"name":tool,"arguments":args,"_meta":{"callId":logical,"threadId":thread(&self.config.caller.attempt_id),"sessionId":"controlled-session"}})).await;
        let text = body["content"][0]["text"].as_str().unwrap();
        let result: CallResult = serde_json::from_str(text).unwrap();
        assert_eq!(
            body["isError"],
            !matches!(result.reply.outcome, Outcome::Ok { .. })
        );
        // Ping runs after the bridge's stdout acknowledgement and durable
        // delivered marker, avoiding a timing assumption at the read boundary.
        self.rpc("ping", json!({})).await;
        let journal = self.journal();
        let entries = journal["entries"].as_array().unwrap();
        let entry = entries
            .iter()
            .find(|entry| entry["invocation"]["logical_call_id"] == logical)
            .unwrap();
        assert_eq!(entry["delivered"], true);
        assert_eq!(entry["outcome"], serde_json::to_value(&result).unwrap());
        assert_eq!(text, serde_json::to_string(&result).unwrap());
        assert!(result.reply.commit.is_some());
        result
    }
    fn journal(&self) -> Value {
        serde_json::from_slice(&std::fs::read(self.config.directory().join("calls.json")).unwrap())
            .unwrap()
    }
    fn invocation(&self, logical: &str) -> Invocation {
        let journal = self.journal();
        serde_json::from_value(
            journal["entries"]
                .as_array()
                .unwrap()
                .iter()
                .find(|entry| entry["invocation"]["logical_call_id"] == logical)
                .unwrap()["invocation"]
                .clone(),
        )
        .unwrap()
    }
    async fn close(&mut self, crash: bool) {
        let pid = self.child.id().unwrap();
        let journal_path = self.config.directory().join("calls.json");
        let before = std::fs::read(&journal_path).unwrap();
        let journal: Value = serde_json::from_slice(&before).unwrap();
        if crash {
            self.child.start_kill().unwrap();
        } else {
            drop(self.stdin.take());
        }
        let status = tokio::time::timeout(WAIT, self.child.wait())
            .await
            .unwrap()
            .unwrap();
        if crash {
            assert_eq!(status.signal(), Some(9));
        } else {
            assert_eq!(status.code(), Some(1));
            let stop: Value =
                serde_json::from_slice(&std::fs::read(self.config.stop_path()).unwrap()).unwrap();
            assert_eq!(
                stop["caller"],
                serde_json::to_value(&self.config.caller).unwrap()
            );
        }
        assert_eq!(
            std::fs::read(&journal_path).unwrap(),
            before,
            "process stop cannot settle or delete an occurrence"
        );
        let unresolved: Vec<_> = journal["entries"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|entry| entry["outcome"].is_null())
            .map(|entry| entry["invocation"]["operation_id"].clone())
            .collect();
        self.closed = true;
        process::atomic_json(
            &self.config.state.join(format!(
                "closed-{}.json",
                NEXT.fetch_add(1, Ordering::SeqCst)
            )),
            &json!({"pid":pid,"caller":self.config.caller,"reaped":true,"crash":crash,"code":status.code(),"signal":status.signal(),"journal_unchanged":true,"unresolved_operations":unresolved,"stop_marker":self.config.stop_path().exists()}),
        )
        .unwrap();
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        if !self.closed {
            let _ = self.child.start_kill();
        }
    }
}
fn refused(result: &CallResult, expected: &str) {
    assert!(
        matches!(&result.reply.outcome,Outcome::Refused{code,..}if code==expected),
        "{result:?}"
    );
}
async fn exact_lookup(lab: &Lab, call: &Invocation, result: &CallResult) {
    let original = mcp::recovered(
        call,
        lab.api
            .call(Operation::GetCollaborationCall {
                caller: call.caller.clone(),
                operation_id: call.operation_id.clone(),
            })
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(original.reply.outcome, result.reply.outcome);
    assert_eq!(original.reply.commit, result.reply.commit);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn codex_definition_update_lost_reply_and_fresh_bridge_keep_old_pin_and_apply_later_bound() {
    // This control exercises an authorized identity manager. The canonical
    // actor still comes from the accepted session; tool arguments grant none.
    let mut limits = RuntimeLimits::seed().unwrap();
    limits.execution.native.context_bytes = 65_536;
    limits.identity_administration = Some(borg_runtime::limits::IdentityAdministrationPolicy {
        version: 1,
        revision: "coding-manager-fixture-1".into(),
        enabled: true,
        manager_agent_id: "coding-agent".into(),
        complete_definitions: false,
    });
    let mut lab = Lab::with_limits("apply", limits).await;
    let old_bound = 32_768;
    let new_bound = 65_536;
    let mut input = lab.input(old_bound);
    input.definition.execution_profile = Some(ExecutionProfile::CollaborationV2 {
        publication: None, web_search: None,
    });
    input.definition.limit_overrides.as_mut().unwrap().collaboration.as_mut().unwrap()
        .capabilities = Some(borg_runtime::limits::CollaborationCapabilities {
            version: 1, task_creation: false, identity_administration: true,
        });
    let first = lab.start(input, "first").await;
    let pinned = serde_json::to_vec(&first.input).unwrap();
    let mut proxy = Proxy::start(lab.url.clone(), true).await;
    let mut config = lab.config(&first, &proxy.url, "first-bridge");
    config.execution_profile = first.input.definition.execution_profile.clone();
    let config_bytes = serde_json::to_vec(&config).unwrap();
    let mut peer = Peer::start(config.clone()).await;
    let tools = peer.rpc("tools/list", json!({})).await;
    assert_eq!(*proxy.task_reads.lock().unwrap(), vec![first.id.clone()]);
    assert!(proxy.calls.lock().unwrap().is_empty());
    assert_eq!(proxy.lookups.load(Ordering::SeqCst), 0);
    assert!(
        tools["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "collab_definition_update")
    );
    assert!(
        !tools["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "collab_task_delegate")
    );
    let args = lab.changes(
        &first,
        json!({"collaboration.complete_result_bytes":new_bound}),
    );
    let changed = peer
        .call("change", "collab_definition_update", args.clone())
        .await;
    let Outcome::Ok {
        data: Data::DefinitionUpdate(update),
    } = &changed.reply.outcome
    else {
        panic!("{changed:?}")
    };
    assert_eq!(update.effective_for, ConfigurationEffect::FutureSubmissions);
    assert_eq!(update.classification, ConfigurationChangeClass::Widening);
    let mut expected = first.input.execution_limits().unwrap().clone();
    expected.collaboration.complete_result_bytes = new_bound;
    assert_eq!(
        lab.limits
            .resolve_definition(&update.selected_definition)
            .unwrap(),
        expected
    );
    assert_eq!(update.selected_definition.executor, ExecutorKind::Coding);
    assert_eq!(
        update.selected_definition.inference,
        first.input.definition.inference
    );
    assert_eq!(
        update.selected_definition.execution_profile,
        first.input.definition.execution_profile
    );
    assert_eq!(lab.selected().await, update.selected_definition);
    assert_eq!(lab.selected().await.revision, "definition-1");
    assert!(proxy.dropped.load(Ordering::SeqCst));
    assert!(proxy.lookups.load(Ordering::SeqCst) > 0);
    assert_eq!(proxy.calls.lock().unwrap().len(), 1);
    let call = peer.invocation("change");
    assert_eq!(proxy.calls.lock().unwrap()[0], call);
    exact_lookup(&lab, &call, &changed).await;
    let journal_bytes = std::fs::read(config.directory().join("calls.json")).unwrap();
    let original_pid = peer.child.id().unwrap();
    peer.close(true).await;
    assert!(!config.stop_path().exists());
    let mut peer = Peer::start(config.clone()).await;
    assert_ne!(peer.child.id().unwrap(), original_pid);
    let replay = peer.call("change", "collab_definition_update", args).await;
    assert_eq!(replay, changed);
    assert_eq!(proxy.calls.lock().unwrap().len(), 1);
    assert_eq!(
        std::fs::read(config.directory().join("calls.json")).unwrap(),
        journal_bytes
    );
    assert_eq!(
        std::fs::read(config.state.join("bridge-config.json")).unwrap(),
        config_bytes
    );
    assert_eq!(lab.selected().await.revision, "definition-1");
    assert_eq!(
        serde_json::to_vec(&lab.get(&first.id).await.input).unwrap(),
        pinned
    );

    // The same canonical value needs a partial page under the old caller's
    // pin and fits whole under the later bridge's wider accepted bound.
    let memory = json!("x".repeat(old_bound));
    let attempt = first.attempt.as_ref().unwrap();
    assert!(matches!(
        lab.api
            .call(Operation::SetMemory {
                operation_id: "seed-large-value".into(),
                task_id: first.id.clone(),
                attempt_id: attempt.id.clone(),
                generation: attempt.generation,
                key: "large".into(),
                expected_revision: None,
                value: memory.clone()
            })
            .await
            .unwrap()
            .outcome,
        Outcome::Ok { .. }
    ));
    let old_read = peer
        .call("old-read", "collab_memory_get", json!({"key":"large"}))
        .await;
    let Outcome::Ok { data: Data::ReadPage(page) } = &old_read.reply.outcome else {
        panic!("old caller must receive a bounded partial page: {old_read:?}")
    };
    use borg_runtime::read_page::{Cursor, Encoding, ReadTool, Selection, Source, Summary};
    assert_eq!(page.subject, Selection::Public {
        tool: ReadTool::Memory, arguments: json!({"key":"large"}),
        item: false, caller_task_id: Some(first.id.clone()),
    });
    assert_eq!(page.identity.source, Source::Public {
        tool: ReadTool::Memory, item_id: None, revision: None,
    });
    assert_eq!(page.summary, Summary::Public { tool: ReadTool::Memory });
    assert_eq!(page.encoding, Encoding::CanonicalJsonUtf8);
    assert_eq!(page.offset, 0);
    assert_eq!(page.returned_bytes, page.content.len());
    assert!(page.returned_bytes > 0 && page.returned_bytes < page.total_bytes);
    assert!(!page.complete);
    assert_eq!(page.retained_truncation, None);
    let cursor = Cursor::decode(page.next_cursor.as_deref().unwrap(),
        lab.limits.controller.collaboration_cursor_bytes).unwrap();
    assert_eq!(cursor.selection, page.subject);
    assert_eq!(cursor.identity, page.identity);
    assert_eq!(cursor.encoding, page.encoding);
    assert_eq!(cursor.offset, page.returned_bytes);
    assert_eq!(cursor.max_bytes, None);
    assert!(serde_json::to_vec(&old_read).unwrap().len() <= old_bound);
    exact_lookup(&lab, &peer.invocation("old-read"), &old_read).await;
    assert_eq!(peer.config.limits.complete_result_bytes, old_bound);
    let mut next = lab.input(old_bound);
    next.definition = update.selected_definition.clone();
    let later = lab.start(next, "later").await;
    assert_eq!(later.input.execution_limits().unwrap(), &expected);
    let mut later_config = lab.config(&later, &proxy.url, "later-bridge");
    later_config.execution_profile = later.input.definition.execution_profile.clone();
    let mut next_peer = Peer::start(later_config).await;
    assert_eq!(next_peer.rpc("tools/list", json!({})).await, tools);
    assert_eq!(*proxy.task_reads.lock().unwrap(), vec![first.id.clone(), later.id.clone()]);
    assert_eq!(next_peer.config.limits.complete_result_bytes, new_bound);
    // A second accepted caller cannot read the first caller's occurrence.
    let mismatch = lab
        .api
        .call(Operation::GetCollaborationCall {
            caller: next_peer.config.caller.clone(),
            operation_id: call.operation_id.clone(),
        })
        .await
        .unwrap();
    assert!(matches!(mismatch.outcome,Outcome::Refused{ref code,..}if code=="caller_mismatch"));
    exact_lookup(&lab, &call, &changed).await;
    let read = next_peer
        .call("later-read", "collab_memory_get", json!({"key":"large"}))
        .await;
    assert!(
        matches!(&read.reply.outcome,Outcome::Ok{data:Data::Memory(Some(value))}if value.value==memory)
    );
    assert!(serde_json::to_vec(&read).unwrap().len() > old_bound);
    assert!(serde_json::to_vec(&read).unwrap().len() <= new_bound);
    let Outcome::Ok { data } = &read.reply.outcome else { unreachable!() };
    let full = borg_runtime::read_page::canonical(data).unwrap();
    assert_eq!(page.total_bytes, full.len());
    assert_eq!(page.identity.digest, borg_runtime::read_page::digest(full.as_bytes()));
    assert_eq!(page.content, full[..page.returned_bytes]);
    exact_lookup(&lab, &next_peer.invocation("later-read"), &read).await;
    assert_eq!(
        serde_json::to_vec(&lab.get(&first.id).await.input).unwrap(),
        pinned
    );
    assert_eq!(lab.selected().await.revision, "definition-1");
    next_peer.close(false).await;
    peer.close(false).await;
    proxy.owner.stop().await;
    lab.finish(&[first, later], (2, 1)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn codex_definition_update_refusals_keep_head_counter_and_old_caller() {
    for case in [
        "stale-caller",
        "stale-global",
        "stale-policy",
        "stale-definition",
        "cancelled",
        "no-policy",
        "denied-field",
        "unknown-field",
        "invalid-value",
        "response",
    ] {
        let mut limits = RuntimeLimits::seed().unwrap();
        // Leave room for the existing context-widening discriminator.
        limits.execution.native.context_bytes = 65_536;
        if case == "no-policy" {
            limits.definition_updates = None;
        }
        if case == "denied-field" {
            limits
                .definition_updates
                .as_mut()
                .unwrap()
                .allowed_fields
                .retain(|field| *field != DefinitionLimitField::NativeContextBytes);
        }
        let mut lab = Lab::with_limits(case, limits).await;
        let bound = if case == "response" { 2048 } else { 32_768 };
        let input = lab.input(bound);
        let first = lab.start(input, "first").await;
        let pinned = serde_json::to_vec(&first.input).unwrap();
        let selected = lab.selected().await;

        let mut proxy = Proxy::start(lab.url.clone(), false).await;
        let mut config = lab.config(&first, &proxy.url, "refusal-bridge");
        let mut args = lab.changes(
            &first,
            json!({"native.context_bytes":lab.limits.execution.native.context_bytes+1}),
        );
        let expected = match case {
            "stale-caller" => {
                config.caller.generation += 1;
                "stale_generation"
            }
            "stale-global" => {
                args["expected_global_revision"] = json!("old-global");
                "configuration_conflict"
            }
            "stale-policy" => {
                args["expected_policy_revision"] = json!("old-policy");
                "configuration_conflict"
            }
            "stale-definition" => {
                args["expected_revision"] = json!("old-revision");
                "configuration_conflict"
            }
            "cancelled" => {
                lab.cancel(&first).await;
                "invalid_transition"
            }
            "no-policy" => "configuration_disabled",
            "denied-field" => "configuration_policy",
            "unknown-field" => {
                args["changes"] = json!({"host_admin":1});
                "invalid_input"
            }
            "invalid-value" => {
                args["changes"] = json!({"native.context_bytes":1});
                "invalid_limits"
            }
            "response" => {
                // Touch all existing groups so the successful receipt exceeds
                // the small accepted bound, without changing that caller's cap.
                // An unspecified optional setting has no neutral numeric operand.
                let values = first.input.execution_limits().unwrap();
                let changes: std::collections::BTreeMap<_, _> = lab
                    .limits
                    .definition_updates
                    .as_ref()
                    .unwrap()
                    .allowed_fields
                    .iter()
                    .filter_map(|field| field.value(values).map(|value| (*field, value)))
                    .collect();
                args["changes"] = serde_json::to_value(changes).unwrap();
                "response_bound"
            }
            _ => unreachable!(),
        };
        let mut peer = Peer::start(config).await;
        let result = peer
            .call("refused", "collab_definition_update", args.clone())
            .await;
        refused(&result, expected);
        let original = peer.invocation("refused");
        exact_lookup(&lab, &original, &result).await;
        assert_eq!(
            peer.call("refused", "collab_definition_update", args.clone())
                .await,
            result
        );
        assert_eq!(proxy.calls.lock().unwrap().len(), 1);
        assert_eq!(lab.selected().await, selected);

        assert_eq!(
            serde_json::to_vec(&lab.get(&first.id).await.input).unwrap(),
            pinned
        );
        let mut tasks = vec![first.clone()];
        if case == "response" {
            // Positive control: the exact same settings proposal commits for a
            // different accepted caller with sufficient complete-result space.
            let mut input = lab.input(65_536);
            input.definition.id = "control-agent".into();
            let control = lab.start(input, "control").await;
            let mut control_peer =
                Peer::start(lab.config(&control, &proxy.url, "control-bridge")).await;
            let success = control_peer
                .call("same-valid-proposal", "collab_definition_update", args)
                .await;
            let Outcome::Ok {
                data: Data::DefinitionUpdate(update),
            } = &success.reply.outcome
            else {
                panic!("{success:?}")
            };
            assert_eq!(update.classification, ConfigurationChangeClass::Neutral);
            assert!(serde_json::to_vec(&success).unwrap().len() > bound);
            assert_eq!(
                lab.limits
                    .resolve_definition(&update.selected_definition)
                    .unwrap(),
                *first.input.execution_limits().unwrap()
            );

            // Rollback did not consume a generated definition revision.
            assert_eq!(update.selected_definition.revision, "definition-1");
            exact_lookup(&lab, &original, &result).await;
            assert_eq!(
                serde_json::to_vec(&lab.get(&first.id).await.input).unwrap(),
                pinned
            );
            control_peer.close(false).await;
            tasks.push(control);
        }
        peer.close(false).await;
        proxy.owner.stop().await;
        lab.finish(&tasks, if case == "response" { (2, 1) } else { (1, 0) })
            .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn codex_bridge_carries_each_allowed_field_without_changing_accepted_execution() {
    let mut limits = RuntimeLimits::seed().unwrap();
    limits.execution.native.context_bytes = 65_536;
    // This existing independent-field discriminator uses an explicit subset;
    // coupled native carriage fields are exercised by their own fixtures.
    let policy = limits.definition_updates.as_mut().unwrap();
    policy.revision = "codex-eight-field-fixture-policy-1".into();
    policy.allowed_fields = vec![
        DefinitionLimitField::NativeContextBytes,
        DefinitionLimitField::NativeOutputBytes,
        DefinitionLimitField::NativeResultBytes,
        DefinitionLimitField::NativeMaxOutputTokens,
        DefinitionLimitField::CodingCommandOutputBytes,
        DefinitionLimitField::CodingSummaryChars,
        DefinitionLimitField::CodingSummaryBytes,
        DefinitionLimitField::CollaborationCompleteResultBytes,
    ];
    let mut lab = Lab::with_limits("eight-fields", limits).await;
    let input = lab.input(lab.limits.execution.collaboration.complete_result_bytes);
    let first = lab.start(input, "first").await;
    let pinned = serde_json::to_vec(&first.input).unwrap();
    let mut selected = first.input.definition.clone();
    let mut effective = first.input.execution_limits().unwrap().clone();
    let mut proxy = Proxy::start(lab.url.clone(), false).await;
    let mut peer = Peer::start(lab.config(&first, &proxy.url, "fields-bridge")).await;
    let fields = lab
        .limits
        .definition_updates
        .as_ref()
        .unwrap()
        .allowed_fields
        .clone();
    assert_eq!(
        fields,
        vec![
            DefinitionLimitField::NativeContextBytes,
            DefinitionLimitField::NativeOutputBytes,
            DefinitionLimitField::NativeResultBytes,
            DefinitionLimitField::NativeMaxOutputTokens,
            DefinitionLimitField::CodingCommandOutputBytes,
            DefinitionLimitField::CodingSummaryChars,
            DefinitionLimitField::CodingSummaryBytes,
            DefinitionLimitField::CollaborationCompleteResultBytes
        ]
    );
    let mut operations = std::collections::BTreeSet::new();
    for (index, field) in fields.into_iter().enumerate() {
        let value = field.value(&effective).expect("explicit numeric bridge fixture field") - 1;
        let key = serde_json::to_value(field)
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned();
        let mut args = lab.changes(&first, json!({(key):value}));
        args["expected_revision"] = json!(selected.revision);
        let logical = format!("field-{index}");
        let result = peer.call(&logical, "collab_definition_update", args).await;
        let Outcome::Ok {
            data: Data::DefinitionUpdate(update),
        } = &result.reply.outcome
        else {
            panic!("{result:?}")
        };
        field.set(&mut effective, value);
        assert_eq!(update.changed_fields, vec![field]);
        assert_eq!(update.previous_revision, selected.revision);
        assert_eq!(update.classification, ConfigurationChangeClass::Narrowing);
        assert_eq!(update.effective_for, ConfigurationEffect::FutureSubmissions);
        assert_eq!(
            update.selected_definition.executor,
            first.input.definition.executor
        );
        assert_eq!(
            update.selected_definition.inference,
            first.input.definition.inference
        );
        assert_eq!(
            update.selected_definition.execution_profile,
            first.input.definition.execution_profile
        );
        assert_eq!(
            lab.limits
                .resolve_definition(&update.selected_definition)
                .unwrap(),
            effective
        );
        selected = update.selected_definition.clone();
        assert_eq!(lab.selected().await, selected);
        let invocation = peer.invocation(&logical);
        assert!(operations.insert(invocation.operation_id.clone()));
        exact_lookup(&lab, &invocation, &result).await;
        assert_eq!(selected.revision, format!("definition-{}", index + 1));
        assert_eq!(
            serde_json::to_vec(&lab.get(&first.id).await.input).unwrap(),
            pinned
        );
    }
    assert_eq!(proxy.calls.lock().unwrap().len(), 8);
    assert_eq!(peer.journal()["entries"].as_array().unwrap().len(), 8);
    assert_eq!(
        peer.config.limits,
        first.input.execution_limits().unwrap().collaboration
    );
    peer.close(false).await;
    proxy.owner.stop().await;
    lab.finish(&[first], (9, 8)).await;
}

// This is controlled worker observation and actual bridge/controller recovery.
// It does not launch worker::run or qualify an installed historical root decoder.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn codex_pending_original_receipt_survives_stop_and_second_worker_selection() {
    let mut lab = Lab::new("two-workers-retained-call", true).await;
    let historical = include_bytes!("fixtures/definition-update-old-worker-limits.json");
    let startup = RuntimeLimits::parse(historical).unwrap();
    assert!(startup.definition_updates.is_none());
    let input = lab.input(32_768);
    let first = lab.start(input, "first").await;
    let pinned = serde_json::to_vec(&first.input).unwrap();
    assert!(
        first
            .input
            .resolved_limits
            .as_ref()
            .unwrap()
            .global_metadata
            .definition_updates
            .is_some()
    );
    let first_root = lab.root.join("worker-one");
    let second_root = lab.root.join("worker-two");
    for root in [&first_root, &second_root] {
        process::private_directory(root).unwrap();
        std::fs::write(root.join("startup-limits.json"), historical).unwrap();
    }
    let held = Arc::new(HeldReply::default());
    let mut proxy = Proxy::with_hold(lab.url.clone(), false, Some(held.clone())).await;
    let mut config = lab.config(&first, &proxy.url, "worker-one/attempt");
    config.worker_limits = startup.worker.clone();
    let config_bytes = serde_json::to_vec(&config).unwrap();
    let mut peer = Peer::start(config.clone()).await;
    let args = lab.changes(
        &first,
        json!({"collaboration.complete_result_bytes":65_536}),
    );
    peer.send_rpc("tools/call",json!({"name":"collab_definition_update","arguments":args,"_meta":{"callId":"retained-update","threadId":thread(&config.caller.attempt_id),"sessionId":"controlled-session"}})).await;
    tokio::time::timeout(WAIT, held.committed.notified())
        .await
        .unwrap();
    let pending = peer.journal();
    assert_eq!(pending["entries"].as_array().unwrap().len(), 1);
    assert!(pending["entries"][0]["outcome"].is_null());
    assert_eq!(pending["entries"][0]["delivered"], false);
    let invocation = peer.invocation("retained-update");
    assert_eq!(
        proxy.calls.lock().unwrap().as_slice(),
        &[invocation.clone()]
    );
    let canonical = mcp::recovered(
        &invocation,
        lab.api
            .call(Operation::GetCollaborationCall {
                caller: invocation.caller.clone(),
                operation_id: invocation.operation_id.clone(),
            })
            .await
            .unwrap(),
    )
    .unwrap();
    let Outcome::Ok {
        data: Data::DefinitionUpdate(update),
    } = &canonical.reply.outcome
    else {
        panic!("{canonical:?}")
    };
    let selected = update.selected_definition.clone();
    assert_eq!(lab.selected().await.revision, "definition-1");
    let old_pid = peer.child.id().unwrap();
    peer.close(true).await;
    held.release.notify_one();
    assert_eq!(peer.journal(), pending);
    assert!(!config.stop_path().exists());
    let attempt = first.attempt.as_ref().unwrap();
    for observation in [
        Observation::ExecutionUncertain {
            message: "controlled bridge died while original receipt remained undelivered".into(),
        },
        Observation::ExecutorOutcome {
            success: false,
            summary: "controlled retained-call fixture".into(),
        },
        Observation::ProcessTreeStopped,
    ] {
        lab.report(attempt, observation).await;
    }
    let stopped = lab.get(&first.id).await;
    assert_eq!(stopped.status, TaskStatus::Failed);
    assert!(
        stopped.attempt.as_ref().unwrap().stopped && stopped.attempt.as_ref().unwrap().uncertain
    );
    assert_eq!(serde_json::to_vec(&stopped.input).unwrap(), pinned);
    let retired = Worker {
        id: lab.worker.id.clone(),
        incarnation: "incarnation-2".into(),
        coding_capacity: 0,
        native_capacity: 0,
    };
    assert!(matches!(
        lab.api
            .call(Operation::RegisterWorker {
                operation_id: "fresh-old-worker".into(),
                worker: retired
            })
            .await
            .unwrap()
            .outcome,
        Outcome::Ok { .. }
    ));
    lab.worker = Worker {
        id: "controlled-coding-worker-two".into(),
        incarnation: "worker-two-incarnation-1".into(),
        coding_capacity: 1,
        native_capacity: 0,
    };
    lab.acknowledged.clear();
    assert!(matches!(
        lab.api
            .call(Operation::RegisterWorker {
                operation_id: "register-second-worker".into(),
                worker: lab.worker.clone()
            })
            .await
            .unwrap()
            .outcome,
        Outcome::Ok { .. }
    ));
    let mut input = lab.input(32_768);
    input.definition = selected.clone();
    let later = lab.start(input, "later").await;
    assert_ne!(later.attempt.as_ref().unwrap().worker_id, attempt.worker_id);
    assert_ne!(
        later.attempt.as_ref().unwrap().worker_incarnation,
        attempt.worker_incarnation
    );
    assert_ne!(later.session_id, first.session_id);
    assert_eq!(later.input.definition, selected);
    assert_eq!(
        later
            .input
            .execution_limits()
            .unwrap()
            .collaboration
            .complete_result_bytes,
        65_536
    );
    let mut second_config = lab.config(&later, &proxy.url, "worker-two/attempt");
    second_config.worker_limits = startup.worker.clone();
    assert_ne!(second_config.state, config.state);
    assert!(second_config.runtime_limits.definition_updates.is_some());
    let mut second = Peer::start(second_config).await;
    let read = second
        .call(
            "selected-definition",
            "collab_agent_get",
            json!({"agent_id":"coding-agent"}),
        )
        .await;
    assert!(
        matches!(&read.reply.outcome,Outcome::Ok{data:Data::Agent(agent)}if agent.definition==selected)
    );
    let sends_before = proxy.calls.lock().unwrap().len();
    let mut reopened = Peer::start(config.clone()).await;
    assert_ne!(reopened.child.id().unwrap(), old_pid);
    let resolved = reopened
        .call(
            "retained-update",
            "collab_definition_update",
            invocation.arguments.clone(),
        )
        .await;
    assert_eq!(resolved.reply.outcome, canonical.reply.outcome);
    assert_eq!(resolved.reply.commit, canonical.reply.commit);
    assert_eq!(proxy.calls.lock().unwrap().len(), sends_before);
    assert!(proxy.lookups.load(Ordering::SeqCst) > 0);
    let mut expected_journal = pending.clone();
    expected_journal["entries"][0]["outcome"] = serde_json::to_value(&resolved).unwrap();
    expected_journal["entries"][0]["delivered"] = json!(true);
    assert_eq!(reopened.journal(), expected_journal);
    assert_eq!(
        std::fs::read(config.state.join("bridge-config.json")).unwrap(),
        config_bytes
    );
    assert_eq!(reopened.config.caller, invocation.caller);
    assert_ne!(reopened.config.caller.incarnation, "incarnation-2");
    assert_eq!(
        lab.get(&first.id).await,
        stopped,
        "receipt recovery cannot clear original uncertainty or reopen the task"
    );
    let retry = lab
        .api
        .call(Operation::RetryTask {
            operation_id: "refuse-original-retry".into(),
            task_id: first.id.clone(),
            expected_revision: stopped.revision,
        })
        .await
        .unwrap();
    assert!(
        matches!(retry.outcome,Outcome::Refused{ref code,..}if code=="reconciliation_required")
    );
    assert_eq!(lab.get(&first.id).await, stopped);
    assert_eq!(lab.selected().await.revision, "definition-1");
    for root in [&first_root, &second_root] {
        assert_eq!(
            std::fs::read(root.join("startup-limits.json")).unwrap(),
            historical
        );
    }
    process::atomic_json(&lab.root.join("retained-call-proof.json"),&json!({"original_task":stopped,"later_task":later,"original_invocation":invocation,"original_commit":canonical.reply.commit,"old_journal_before":pending,"old_journal_after":reopened.journal(),"old_config_unchanged":true,"startup_bytes_unchanged":true,"original_retry_refused":true,"controller_write_count":1,"scope":"controlled worker observations; actual private bridge processes and HTTP/controller; no installed decoder or worker-run claim"})).unwrap();
    second.close(false).await;
    reopened.close(false).await;
    proxy.owner.stop().await;
    lab.finish(&[later], (2, 1)).await;
}

// Actual stdio request/response and controller commits; no supplied Codex or
// provider is launched. Resource discovery must not terminate a tools-only peer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn codex_optional_discovery_errors_preserve_bridge_and_original_tool_receipt() {
    let mut lab = Lab::new("optional-discovery", true).await;
    let input = lab.input(65_536);
    let task = lab.start(input, "discovery").await;
    let pinned = serde_json::to_vec(&task.input).unwrap();
    let mut proxy = Proxy::start(lab.url.clone(), false).await;
    let config = lab.config(&task, &proxy.url, "bridge");
    let mut peer = Peer::start(config.clone()).await;
    let journal = config.directory().join("calls.json");
    assert!(!journal.exists(), "initialization must not create an occurrence journal");
    let first = peer.send_rpc("resources/list", json!({})).await;
    let second = peer.send_rpc("resources/templates/list", json!({})).await;
    for id in [first, second] {
        assert_eq!(peer.receive_frame(id).await,
            json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"Unknown MCP method"}}));
    }
    assert!(peer.child.try_wait().unwrap().is_none());
    assert!(!config.stop_path().exists());
    assert!(!journal.exists(), "unsupported discovery must not create an occurrence journal");
    assert!(proxy.calls.lock().unwrap().is_empty());
    assert_eq!(proxy.lookups.load(Ordering::SeqCst), 0);
    let args = json!({"key":"optional-discovery","expected_revision":null,"value":"original"});
    let result = peer.call("original", "collab_memory_set", args.clone()).await;
    assert!(matches!(result.reply.outcome, Outcome::Ok {..}), "{result:?}");
    let original = peer.invocation("original");
    exact_lookup(&lab, &original, &result).await;
    let committed = std::fs::read(&journal).unwrap();
    for method in ["resources/list", "resources/templates/list", "unadvertised/fixture"] {
        let id = peer.send_rpc(method, json!({})).await;
        assert_eq!(peer.receive_frame(id).await,
            json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"Unknown MCP method"}}));
        assert_eq!(std::fs::read(&journal).unwrap(), committed);
        assert!(!config.stop_path().exists());
    }
    assert_eq!(peer.call("original", "collab_memory_set", args).await, result);
    assert_eq!(proxy.calls.lock().unwrap().as_slice(), &[original]);
    assert_eq!(proxy.lookups.load(Ordering::SeqCst), 0);
    assert_eq!(serde_json::to_vec(&lab.get(&task.id).await.input).unwrap(), pinned);
    peer.close(false).await;
    proxy.owner.stop().await;
    lab.finish(&[task], (1, 0)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn codex_discovery_response_does_not_mask_invalid_frames_or_cancellation() {
    let mut lab = Lab::new("discovery-fail-closed", true).await;
    let input = lab.input(65_536);
    let task = lab.start(input, "fail-closed").await;
    let mut proxy = Proxy::start(lab.url.clone(), false).await;
    for case in ["malformed", "cancelled", "changed-occurrence"] {
        let config = lab.config(&task, &proxy.url, case);
        let mut peer = Peer::start(config.clone()).await;
        let logical = format!("original-{case}");
        let args = json!({"key":case,"expected_revision":null,"value":"original"});
        let result = peer.call(&logical, "collab_memory_set", args).await;
        assert!(matches!(result.reply.outcome, Outcome::Ok {..}), "{result:?}");
        let id = peer.send_rpc("resources/list", json!({})).await;
        assert_eq!(peer.receive_frame(id).await["error"]["code"], -32601);
        let journal = config.directory().join("calls.json");
        let committed = std::fs::read(&journal).unwrap();
        let sends = proxy.calls.lock().unwrap().len();
        let frame = match case {
            "malformed" => json!({"jsonrpc":"2.0","id":99,"method":"ping","unexpected":true}),
            "cancelled" => json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{}}),
            "changed-occurrence" => json!({"jsonrpc":"2.0","id":99,"method":"tools/call","params":{
                "name":"collab_memory_set","arguments":{"key":"changed","expected_revision":null,"value":"different"},
                "_meta":{"callId":logical,"threadId":thread(&config.caller.attempt_id),"sessionId":"controlled-session"}}}),
            _ => unreachable!(),
        };
        let mut bytes = serde_json::to_vec(&frame).unwrap(); bytes.push(b'\n');
        peer.stdin.as_mut().unwrap().write_all(&bytes).await.unwrap();
        peer.stdin.as_mut().unwrap().flush().await.unwrap();
        let pid = peer.child.id().unwrap();
        // Keep stdin open: termination must follow the rejected frame, not EOF.
        let status = tokio::time::timeout(WAIT, peer.child.wait()).await.unwrap().unwrap();
        assert_eq!(status.code(), Some(1));
        assert_eq!(std::fs::read(&journal).unwrap(), committed);
        assert_eq!(proxy.calls.lock().unwrap().len(), sends);
        let stop: Value = serde_json::from_slice(&std::fs::read(config.stop_path()).unwrap()).unwrap();
        assert_eq!(stop["caller"], serde_json::to_value(&config.caller).unwrap());
        peer.closed = true;
        process::atomic_json(&config.state.join("closed-invalid-request.json"),
            &json!({"pid":pid,"reaped":true,"code":status.code(),"stdin_held_open":true,
                "journal_unchanged":true,"no_additional_tool_dispatch":true,"case":case})).unwrap();
    }
    assert_eq!(proxy.calls.lock().unwrap().len(), 3);
    proxy.owner.stop().await;
    lab.finish(&[task], (1, 0)).await;
}
