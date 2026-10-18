//! Controlled local provider + actual worker/controller/MCP integration. No real
//! provider, remote, configured Codex, protected account or deployment claim.
use borg_runtime::{client::ApiClient, limits::RuntimeLimits, protocol::*, worker::process};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request as HttpRequest, Response, body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use std::{
    io,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

static NEXT: AtomicU64 = AtomicU64::new(1);
fn area(label: &str) -> PathBuf {
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("artifacts/native-collaboration-tests");
    std::fs::create_dir_all(&base).unwrap();
    let p = base.join(format!(
        "{label}-{}-{}-{}",
        process::now_ms(),
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir(&p).unwrap();
    p
}
struct Process {
    child: Child,
    closed: PathBuf,
}
impl Process {
    fn start(args: &[String], root: &Path, label: &str) -> Self {
        Self::start_with_limits(args, root, label, &root.join("limits.json"))
    }
    fn start_with_limits(args: &[String], root: &Path, label: &str, limits: &Path) -> Self {
        let log = std::fs::File::create(root.join(format!("{label}.log"))).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_borg-runtime"))
            .args(args)
            .arg("--limits")
            .arg(limits)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", root)
            .env(
                "OPENAI_API_KEY",
                if label == "quoted-credential-worker" {
                    QUOTED_SECRET
                } else {
                    "synthetic-native-loop-key"
                },
            )
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        Self {
            child,
            closed: root.join(format!("{label}-closed.json")),
        }
    }
    async fn stop(&mut self) {
        if self.child.try_wait().unwrap().is_none() {
            assert!(
                Command::new("/usr/bin/kill")
                    .args(["-INT", &self.child.id().to_string()])
                    .status()
                    .unwrap()
                    .success()
            );
        }
        let until = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(s) = self.child.try_wait().unwrap() {
                assert!(s.success(), "exit {s}");
                break;
            }
            assert!(tokio::time::Instant::now() < until, "stop timeout");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let exit = self.child.wait();
        let _=std::fs::write(&self.closed,serde_json::to_vec(&json!({"pid":self.child.id(),"reaped":exit.is_ok(),"code":exit.ok().and_then(|s|s.code())})).unwrap());
    }
}
struct Background(tokio::task::JoinHandle<()>);
impl Drop for Background {
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct Peer {
    endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
    _owner: Background,
}
impl Peer {
    async fn start(reply: impl Fn(usize, &Value) -> Option<Value> + Send + Sync + 'static) -> Self {
        Self::start_async(move |index, request| std::future::ready(reply(index, &request))).await
    }
    async fn start_async<F, Fut>(reply: F) -> Self
    where
        F: Fn(usize, Value) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Option<Value>> + Send + 'static,
    {
        let body_bytes = RuntimeLimits::seed().unwrap().transport.body_bytes;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let reply = Arc::new(reply);
        let owner = tokio::spawn(async move {
            let mut joins = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted=listener.accept()=>{let (mut stream,_)=accepted.unwrap();let recorded=recorded.clone();let reply=reply.clone();joins.spawn(async move{
                        let mut head=Vec::new();while !head.ends_with(b"\r\n\r\n"){head.push(stream.read_u8().await.unwrap());assert!(head.len()<16384);}
                        let headers=String::from_utf8(head).unwrap();assert!(headers.starts_with("POST /v1/responses "));
                        let length=headers.lines().find_map(|l|{let(k,v)=l.split_once(':')?;k.eq_ignore_ascii_case("content-length").then(||v.trim().parse::<usize>().unwrap())}).unwrap();assert!(length<=body_bytes);
                        let mut bytes=vec![0;length];stream.read_exact(&mut bytes).await.unwrap();let value:Value=serde_json::from_slice(&bytes).unwrap();
                        assert_eq!(value["model"],"gpt-6-astra");assert_eq!(value["reasoning"],json!({"effort":"max"}));
                        let index={let mut all=recorded.lock().unwrap();let n=all.len();all.push(value.clone());n};
                        if let Some(body)=reply(index,value).await{let body=serde_json::to_vec(&body).unwrap();stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();stream.write_all(&body).await.unwrap();}
                        let _=stream.shutdown().await;
                    });},_ = joins.join_next(),if !joins.is_empty()=>{}
                }
            }
        });
        Self {
            endpoint,
            requests,
            _owner: Background(owner),
        }
    }
    fn count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}
fn envelope(output: Value) -> Value {
    json!({"id":"synthetic-loop-response","status":"completed","error":null,"usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7},"output":output})
}
fn function(index: usize, name: &str, args: Value) -> Value {
    envelope(
        json!([{"type":"function_call","status":"completed","call_id":format!("call-{index}"),"name":name,"arguments":args.to_string()}]),
    )
}
fn final_value() -> Value {
    envelope(
        json!([{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"7"}]}]),
    )
}
fn previous(request: &Value) -> Value {
    let item = request["input"].as_array().unwrap().last().unwrap();
    assert_eq!(item["type"], "function_call_output");
    serde_json::from_str(item["output"].as_str().unwrap()).unwrap()
}
// Controlled transport seam: the canonical operation has completed, but its
// original MCP response is still in flight to the actual worker. No production
// callback, executor behavior or configured limit is changed by this fixture.
struct HeldToolReply {
    tool: &'static str,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    original: Mutex<Option<Value>>,
    calls: AtomicU64,
    released: AtomicBool,
}
impl HeldToolReply {
    fn new(tool: &'static str) -> Self {
        Self {
            tool,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            original: Mutex::new(None),
            calls: AtomicU64::new(0),
            released: AtomicBool::new(false),
        }
    }
}
struct Proxy {
    url: String,
    dropped: Arc<AtomicBool>,
    hide_lookup: Arc<AtomicBool>,
    registrations: Arc<AtomicU64>,
    await_replies: Arc<Mutex<Vec<(String, String)>>>,
    cancel_before_tool: Arc<Mutex<Option<String>>>,
    seed_memory_before_tool: Arc<Mutex<Option<Value>>>,
    held_reply: Arc<Mutex<Option<Arc<HeldToolReply>>>>,
    _owner: Background,
}
impl Proxy {
    async fn start(
        upstream: String,
        drop_tool: Option<&str>,
        hide: bool,
        limits: Arc<RuntimeLimits>,
    ) -> Self {
        let api = ApiClient::new(upstream.clone(), limits).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let dropped = Arc::new(AtomicBool::new(false));
        let hide_lookup = Arc::new(AtomicBool::new(hide));
        let registrations = Arc::new(AtomicU64::new(0));
        let await_replies = Arc::new(Mutex::new(Vec::new()));
        let awaits = await_replies.clone();
        let cancel_before_tool = Arc::new(Mutex::new(None::<String>));
        let cancel = cancel_before_tool.clone();
        let seed_memory_before_tool = Arc::new(Mutex::new(None::<Value>));
        let seed = seed_memory_before_tool.clone();
        let held_reply = Arc::new(Mutex::new(None::<Arc<HeldToolReply>>));
        let held = held_reply.clone();
        let (d, h, regs) = (dropped.clone(), hide_lookup.clone(), registrations.clone());
        let drop_tool = drop_tool.map(str::to_owned);
        let owner = tokio::spawn(async move {
            let client = reqwest::Client::builder().no_proxy().build().unwrap();
            let mut joins = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted=listener.accept()=>{let(stream,_)=accepted.unwrap();let(upstream,client,d,h,regs,tool,cancel,seed,api,held,awaits)=(upstream.clone(),client.clone(),d.clone(),h.clone(),regs.clone(),drop_tool.clone(),cancel.clone(),seed.clone(),api.clone(),held.clone(),awaits.clone());joins.spawn(async move{
                        let service=service_fn(move|request:HttpRequest<Incoming>|{let(upstream,client,d,h,regs,tool,cancel,seed,api,held,awaits)=(upstream.clone(),client.clone(),d.clone(),h.clone(),regs.clone(),tool.clone(),cancel.clone(),seed.clone(),api.clone(),held.clone(),awaits.clone());async move{
                            let path=request.uri().path().to_owned();let caller=request.headers().get("x-borg-caller").cloned();let version=request.headers().get("mcp-protocol-version").cloned();
                            let bytes=request.into_body().collect().await.unwrap().to_bytes();let value:Value=serde_json::from_slice(&bytes).unwrap();
                            if path=="/mcp" && value["method"]=="tools/call" {
                                let seeded={seed.lock().unwrap().take()};
                                if let Some(value)=seeded {let actor:Value=serde_json::from_slice(caller.as_ref().unwrap().as_bytes()).unwrap();let current=get(&api,actor["task_id"].as_str().unwrap()).await;let a=current.attempt.as_ref().unwrap();let out=api.call(Operation::SetMemory{operation_id:"external-seed-credential-memory".into(),task_id:current.id.clone(),attempt_id:a.id.clone(),generation:a.generation,key:"note".into(),expected_revision:None,value}).await.unwrap();assert!(matches!(out.outcome,Outcome::Ok{..}));}
                                let cancelled={cancel.lock().unwrap().take()};
                                if let Some(id)=cancelled {let current=get(&api,&id).await;let out=api.call(Operation::CancelTask{operation_id:"external-cancel-before-tool".into(),task_id:id,expected_revision:current.revision}).await.unwrap();assert!(matches!(out.outcome,Outcome::Ok{..}));}
                            }
                            if path=="/mcp" && value["method"]=="borg/calls/get" && h.load(Ordering::SeqCst) && d.load(Ordering::SeqCst){return Ok::<_,io::Error>(Response::builder().status(503).body(Full::new(Bytes::new())).unwrap());}
                            let mut req=client.post(format!("{upstream}{path}")).header("content-type","application/json").body(bytes.to_vec());if let Some(c)=caller{req=req.header("x-borg-caller",c);}if let Some(v)=version{req=req.header("mcp-protocol-version",v);}
                            let response=req.send().await.map_err(io::Error::other)?;let status=response.status();let bytes=response.bytes().await.map_err(io::Error::other)?;
                            if path=="/api/operations" && value["operation"]=="register_worker" && serde_json::from_slice::<Value>(&bytes).unwrap()["status"]=="ok" {regs.fetch_add(1,Ordering::SeqCst);}
                            if path=="/api/operations" && value["operation"]=="await_commands" && status.is_success() {
                                let original: Request = serde_json::from_value(value.clone()).unwrap();
                                let reply: Reply = serde_json::from_slice(&bytes).unwrap();
                                assert_eq!(reply.request_id, original.request_id);
                                reply.validate_for(&original.command).unwrap();
                                if matches!(reply.outcome, Outcome::Ok { data: Data::Commands(_) } | Outcome::Pending { .. }) {
                                    let Operation::AwaitCommands {worker_id, incarnation, ..} = original.command else {unreachable!()};
                                    awaits.lock().unwrap().push((worker_id, incarnation));
                                }
                            }
                            if path=="/mcp" && value["method"]=="tools/call" && tool.as_ref().is_some_and(|s|value["params"]["name"]==*s) && !d.swap(true,Ordering::SeqCst){
                                assert_eq!(status, reqwest::StatusCode::OK);
                                let original: Value = serde_json::from_slice(&bytes).unwrap();
                                assert_eq!(original["id"], value["id"]);
                                assert_eq!(original["result"]["isError"], false);
                                let committed: borg_runtime::mcp::CallResult = serde_json::from_str(
                                    original["result"]["content"][0]["text"].as_str().unwrap()
                                ).unwrap();
                                assert!(matches!(committed.reply.outcome, Outcome::Ok { .. }));
                                assert_eq!(committed.reply.commit.as_ref().unwrap().operation_id,
                                    value["params"]["_meta"]["borg"]["operation_id"].as_str().unwrap());
                                // The canonical mutation completed. Lose its transport reply
                                // before response headers, rather than returning an HTTP refusal.
                                return Err(io::Error::new(io::ErrorKind::ConnectionAborted,
                                    "controlled loss of committed MCP reply"));
                            }
                            let held = { held.lock().unwrap().clone() };
                            if let Some(gate) = held.filter(|gate| path == "/mcp"
                                && value["method"] == "tools/call"
                                && value["params"]["name"] == gate.tool) {
                                assert_eq!(gate.calls.fetch_add(1, Ordering::SeqCst), 0,
                                    "held tool occurrence must not be dispatched twice");
                                *gate.original.lock().unwrap() = Some(serde_json::from_slice(&bytes).unwrap());
                                gate.entered.notify_one();
                                tokio::time::timeout(Duration::from_secs(10), gate.release.notified())
                                    .await.expect("controlled MCP reply was not released");
                                gate.released.store(true, Ordering::SeqCst);
                            }
                            Ok(Response::builder().status(status.as_u16()).body(Full::new(bytes)).unwrap())
                        }});
                        let _=hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream),service).await;
                    });},_=joins.join_next(),if !joins.is_empty()=>{}
                }
            }
        });
        Self {
            url,
            dropped,
            hide_lookup,
            registrations,
            await_replies,
            cancel_before_tool,
            seed_memory_before_tool,
            held_reply,
            _owner: Background(owner),
        }
    }
}
struct Lab {
    root: PathBuf,
    address: String,
    url: String,
    limits: Arc<RuntimeLimits>,
    worker_limits_file: PathBuf,
}
impl Lab {
    fn new(label: &str) -> Self {
        let root = area(label);
        let mut limits = RuntimeLimits::seed().unwrap();
        limits.worker.coding_capacity = 0;
        limits.worker.native_capacity = 1;
        let limits = Arc::new(limits);
        std::fs::write(
            root.join("limits.json"),
            serde_json::to_vec_pretty(&*limits).unwrap(),
        )
        .unwrap();
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = l.local_addr().unwrap().to_string();
        Self {
            worker_limits_file: root.join("limits.json"),
            root,
            url: format!("http://{address}"),
            address,
            limits,
        }
    }
    async fn controller(&self, label: &str) -> Process {
        let p = Process::start(
            &[
                "controller".into(),
                "--db".into(),
                self.root.join("tasks.sqlite").display().to_string(),
                "--listen".into(),
                self.address.clone(),
                "--origin".into(),
                self.url.clone(),
            ],
            &self.root,
            label,
        );
        let api = ApiClient::new(&self.url, self.limits.clone()).unwrap();
        let until = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if api
                .call(Operation::ListTasks {
                    after: None,
                    limit: 1,
                })
                .await
                .is_ok()
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "controller start timeout"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        p
    }
    fn worker(&self, url: &str, endpoint: &str, inc: &str) -> Process {
        self.worker_with_codex(url, endpoint, inc, Path::new("/usr/bin/false"))
    }
    fn worker_with_codex(&self, url: &str, endpoint: &str, inc: &str, codex: &Path) -> Process {
        Process::start_with_limits(
            &[
                "worker".into(),
                "--controller".into(),
                url.into(),
                "--id".into(),
                "loop-worker".into(),
                "--incarnation".into(),
                inc.into(),
                "--workspace-root".into(),
                self.root.join("worker").display().to_string(),
                "--codex".into(),
                codex.display().to_string(),
                "--native-endpoint".into(),
                endpoint.into(),
            ],
            &self.root,
            inc,
            &self.worker_limits_file,
        )
    }
    fn input(&self, requests: u32) -> TaskInput {
        TaskInput { instruction_source: None,
            pull_request: None,
            resolved_limits: None,
            repository_read: None,
            instruction: "Use the configured collaboration interface; return integer seven.".into(),
            definition: Definition {
                charter: None,
                limit_overrides: None,
                inference: Some(InferenceSettings::selected()),
                execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
                id: "loop-definition".into(),
                revision: "collaboration-v1".into(),
                executor: ExecutorKind::Native,
            },
            repository: None,
            acceptance_commands: vec![],
            output_schema: Some(json!({"type":"integer"})),
            limits: Limits {
                deadline_unix_ms: process::now_ms() + 120_000,
                model_requests: requests,
                output_bytes: 32768,
                result_bytes: 65536,
                max_delegation_depth: 1,
            },
            dependencies: vec![],
            native_workflow: None,
        }
    }
    fn state(&self) -> PathBuf {
        self.root
            .join("worker/.borg-worker")
            .join(process::component("loop-worker"))
    }
    fn usage(&self, attempt: &str) -> (u32, Option<i64>, Option<i64>) {
        let c = rusqlite::Connection::open_with_flags(
            self.root.join("tasks.sqlite"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        c.query_row(
            "SELECT model_requests,input_tokens,output_tokens FROM attempts WHERE id=?1",
            [attempt],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap()
    }
}
/// Observe the exact persisted usage through the controller while it owns the
/// exclusive SQLite connection. These charter fixtures each have one attempt;
/// reject a truncated or different attempt instead of polling an external DB.
async fn live_usage(api: &ApiClient, attempt: &Attempt) -> (u32, Option<i64>, Option<i64>) {
    let reply = api
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
    } = reply.outcome else {
        panic!("canonical attempt usage required: {reply:?}")
    };
    assert!(page.next_cursor.is_none(), "usage query must contain the complete one-attempt fixture");
    assert_eq!(page.items.len(), 1);
    let saved = &page.items[0];
    assert_eq!(saved.attempt.id, attempt.id);
    assert_eq!(saved.attempt.task_id, attempt.task_id);
    assert_eq!(saved.attempt.session_id, attempt.session_id);
    assert_eq!(saved.attempt.generation, attempt.generation);
    assert_eq!(saved.attempt.worker_id, attempt.worker_id);
    assert_eq!(saved.attempt.worker_incarnation, attempt.worker_incarnation);
    (
        saved.reported_model_requests,
        saved.input_tokens.map(|value| i64::try_from(value).unwrap()),
        saved.output_tokens.map(|value| i64::try_from(value).unwrap()),
    )
}
async fn submit(api: &ApiClient, input: TaskInput) -> Task {
    submit_identified(api, input, "submit-loop").await
}
async fn submit_identified(api: &ApiClient, input: TaskInput, operation_id: &str) -> Task {
    match api
        .call(Operation::SubmitTask {
            operation_id: operation_id.into(),
            input,
        })
        .await
        .unwrap()
        .outcome
    {
        Outcome::Ok {
            data: Data::Task(t),
        } => *t,
        other => panic!("submit: {other:?}"),
    }
}
async fn get(api: &ApiClient, id: &str) -> Task {
    match api
        .call(Operation::GetTask { task_id: id.into() })
        .await
        .unwrap()
        .outcome
    {
        Outcome::Ok {
            data: Data::Task(t),
        } => *t,
        other => panic!("get: {other:?}"),
    }
}
async fn until(api: &ApiClient, id: &str, ready: impl Fn(&Task) -> bool) -> Task {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let t = get(api, id).await;
        if ready(&t) {
            return t;
        }
        assert!(
            !matches!(
                t.status,
                TaskStatus::Failed | TaskStatus::Succeeded | TaskStatus::Cancelled
            ) || !t.attempt.as_ref().is_some_and(|a| a.stopped),
            "unexpected terminal task: {t:?}"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "task stalled: {t:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
async fn memory(api: &ApiClient) -> Outcome {
    api.call(Operation::GetMemory {
        agent_id: "loop-definition".into(),
        key: "note".into(),
    })
    .await
    .unwrap()
    .outcome
}

fn no_memory_notices(request: &Value) {
    for item in request["input"].as_array().unwrap() {
        if item["role"] == "developer" {
            assert!(!item.to_string().contains("agent_memory_hints"),
                "retired automatic memory notice reached the provider");
        }
    }
}

// Controlled actual-controller/native case: explicit agent-scoped writes remain
// visible through requested MCP reads, without an automatic context append,
// model request, session, or delivery cursor advancement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_explicit_memory_cross_sessions_preserves_tool_results_without_notices() {
    fn tool_item(request: &Value, call_id: &str) -> Value {
        let items: Vec<_> = request["input"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| v["type"] == "function_call_output" && v["call_id"] == call_id)
            .cloned()
            .collect();
        assert_eq!(items.len(), 1, "one original output per actual tool call");
        items[0].clone()
    }
    async fn report(api: &ApiClient, attempt: &Attempt, id: &str, observation: Observation) {
        let reply = api
            .call(Operation::ObserveWorker {
                report: Report {
                    report_id: id.into(),
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
    let lab = Lab::new("cross-session-explicit-memory");
    let mut controller = lab.controller("controller").await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    // Only the writer exists while this controlled API actor claims its slot.
    let writer = submit_identified(&api, lab.input(1), "submit-notice-writer").await;
    let worker = Worker {
        id: "notice-writer".into(),
        incarnation: "one".into(),
        coding_capacity: 0,
        native_capacity: 1,
    };
    assert!(matches!(
        api.call(Operation::RegisterWorker {
            operation_id: "register-notice-writer".into(),
            worker: worker.clone(),
        })
        .await
        .unwrap()
        .outcome,
        Outcome::Ok {
            data: Data::Worker(_)
        }
    ));
    let commands = match api
        .call(Operation::AwaitCommands {
            operation_id: "claim-notice-writer".into(),
            worker_id: worker.id.clone(),
            incarnation: worker.incarnation.clone(),
            acknowledged: vec![],
        })
        .await
        .unwrap()
        .outcome
    {
        Outcome::Ok {
            data: Data::Commands(commands),
        } => commands,
        other => panic!("writer claim: {other:?}"),
    };
    assert_eq!(commands.len(), 1);
    let WorkerCommand::RunAttempt {
        attempt: writer_attempt,
        session,
        ..
    } = &commands[0]
    else {
        panic!("writer run command absent")
    };
    let writer_attempt = writer_attempt.clone();
    assert_eq!(writer_attempt.task_id, writer.id);
    assert_eq!(session.id, writer.session_id);
    report(
        &api,
        &writer_attempt,
        "notice-writer-started",
        Observation::Started {
            thread_id: None,
            turn_id: None,
        },
    )
    .await;

    let reader = submit_identified(&api, lab.input(3), "submit-notice-reader").await;
    assert_eq!(reader.assigned_agent_id, writer.assigned_agent_id);
    assert_ne!(reader.session_id, writer.session_id);
    // Both revisions are committed after the reader session's creation watermark.
    let first_write = api
        .call(Operation::SetMemory {
            operation_id: "notice-writer-revision-one".into(),
            task_id: writer.id.clone(),
            attempt_id: writer_attempt.id.clone(),
            generation: writer_attempt.generation,
            key: "note".into(),
            expected_revision: None,
            value: json!({"answer":3,"source":"writer-revision-one"}),
        })
        .await
        .unwrap();
    assert!(
        matches!(first_write.outcome, Outcome::Ok { data: Data::Memory(Some(ref m)) }
        if m.revision == 1)
    );
    let first_output = Arc::new(Mutex::new(None::<Value>));
    let saved_output = first_output.clone();
    let writer_api = api.clone();
    let writer_actor = writer_attempt.clone();
    let peer = Peer::start_async(move |index, request| {
        let api = writer_api.clone();
        let actor = writer_actor.clone();
        let saved_output = saved_output.clone();
        async move {
            no_memory_notices(&request);
            match index {
                0 => {
                    assert!(!request["input"].to_string().contains("writer-revision-one"));
                    // Await this actual controller write before returning the first
                    // already-required provider response: no timing sleeps/proxy.
                    let reply = api
                        .call(Operation::SetMemory {
                            operation_id: "notice-writer-revision-two".into(),
                            task_id: actor.task_id,
                            attempt_id: actor.id,
                            generation: actor.generation,
                            key: "note".into(),
                            expected_revision: Some(1),
                            value: json!({"answer":7,"source":"writer-revision-two"}),
                        })
                        .await
                        .unwrap();
                    assert!(
                        matches!(reply.outcome, Outcome::Ok { data: Data::Memory(Some(ref m)) }
                        if m.revision == 2)
                    );
                    Some(function(index, "collab_memory_get", json!({"key":"note"})))
                }
                1 => {
                    let original = tool_item(&request, "call-0");
                    let result: Value =
                        serde_json::from_str(original["output"].as_str().unwrap()).unwrap();
                    assert_eq!(result["outcome"]["status"], "ok");
                    assert_eq!(result["outcome"]["data"]["value"]["revision"], 2);
                    assert_eq!(
                        result["outcome"]["data"]["value"]["value"],
                        json!({"answer":7,"source":"writer-revision-two"})
                    );
                    *saved_output.lock().unwrap() = Some(original);
                    // Only an explicit tool call reads the canonical value.
                    Some(function(index, "collab_memory_get", json!({"key":"note"})))
                }
                2 => {
                    assert_eq!(
                        tool_item(&request, "call-0"),
                        saved_output.lock().unwrap().as_ref().unwrap().clone()
                    );
                    let reread = tool_item(&request, "call-1");
                    let result: Value =
                        serde_json::from_str(reread["output"].as_str().unwrap()).unwrap();
                    assert_eq!(result["outcome"]["status"], "ok");
                    assert_eq!(result["outcome"]["data"]["value"]["revision"], 2);
                    let answer = result["outcome"]["data"]["value"]["value"]["answer"]
                        .as_i64()
                        .unwrap();
                    Some(envelope(
                        json!([{"type":"message","role":"assistant","status":"completed",
                        "content":[{"type":"output_text","text":answer.to_string()}]}]),
                    ))
                }
                _ => panic!("notice must not create an additional model request"),
            }
        }
    })
    .await;
    let mut reader_worker = lab.worker(&lab.url, &peer.endpoint, "notice-reader");
    let accepted = until(&api, &reader.id, |t| t.status == TaskStatus::Succeeded).await;
    assert_eq!(accepted.result.as_ref().unwrap().output, Some(json!(7)));
    assert_eq!(peer.count(), 3);
    assert!(!lab.state().join("memory-notices").exists());
    let attempt = accepted.attempt.as_ref().unwrap();
    assert_eq!(attempt.session_id, reader.session_id);
    let journal_dir = lab
        .state()
        .join("collaboration")
        .join(process::component(&attempt.id));
    assert_eq!(std::fs::read_dir(&journal_dir).unwrap().count(), 2);
    let journal: Value =
        serde_json::from_slice(&std::fs::read(journal_dir.join("0000000001.json")).unwrap())
            .unwrap();
    assert_eq!(journal["consumed"], true);
    let call = &journal["request"]["input"]["call"];
    let exact =
        json!({"operation_id":call["operation_id"],"logical_call_id":call["logical_call_id"],
        "outcome":journal["outcome"],"commit":journal["outcome_context"]["commit"]})
        .to_string();
    assert_eq!(
        first_output.lock().unwrap().as_ref().unwrap()["output"],
        exact
    );

    reader_worker.stop().await;
    let writer = get(&api, &writer.id).await;
    assert!(matches!(
        api.call(Operation::CancelTask {
            operation_id: "close-notice-writer".into(),
            task_id: writer.id.clone(),
            expected_revision: writer.revision
        })
        .await
        .unwrap()
        .outcome,
        Outcome::Ok { .. }
    ));
    report(
        &api,
        &writer_attempt,
        "notice-writer-stopped",
        Observation::ProcessTreeStopped,
    )
    .await;
    assert!(get(&api, &writer.id).await.attempt.unwrap().stopped);
    controller.stop().await;

    // Inspect retained inert delivery fields only after the controller has
    // closed its exclusive SQLite connection; explicit memory still persists.
    let db = rusqlite::Connection::open_with_flags(
        lab.root.join("tasks.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    // Only the two explicit writer/reader submissions create sessions.
    let session_count: i64 = db
        .query_row("SELECT count(*) FROM sessions", [], |row| row.get(0))
        .unwrap();
    assert_eq!(session_count, 2, "memory writes must not create a session");
    let (start, cursor, pending): (i64, i64, Option<String>) = db
        .query_row(
            "SELECT memory_start_seq,memory_cursor_seq,memory_pending FROM sessions WHERE id=?1",
            [&reader.session_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    let writes: Vec<i64> = db
        .prepare("SELECT seq FROM history WHERE task_id=?1 AND kind='memory_recorded' ORDER BY seq")
        .unwrap()
        .query_map([&writer.id], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(writes.len(), 2);
    assert!(start < writes[0] && writes[0] < writes[1]);
    assert_eq!(cursor, start, "retired automatic delivery advanced its cursor");
    assert!(pending.is_none(), "retired automatic delivery prepared a batch");
    drop(db);
    assert_eq!(lab.usage(&attempt.id), (3, Some(15), Some(6)));
}

// Measure the actual selected catalogue with the production serializer. The
// validation envelope below is the non-charter/no-prefix ValidationInput shape
// in native/loop_step.rs; the actual worker still performs its full admission.
fn assert_fixture_native_profile_fits(
    limits: &RuntimeLimits,
    input: &TaskInput,
    expected_functions: usize,
) -> usize {
    assert!(input.definition.charter.is_none());
    assert!(matches!(&input.definition.execution_profile,
        Some(ExecutionProfile::CollaborationV1 { web_search: None }
            | ExecutionProfile::CollaborationV2 { web_search: None, publication: None })));
    let profile = borg_runtime::native::NativeLoopProfile {
        functions: borg_runtime::mcp::catalogue_for_profile(
            limits, input.definition.execution_profile.as_ref(),
        ).into_iter().map(|tool| borg_runtime::native::NativeFunction {
            name: tool.name, description: tool.description, parameters: tool.input_schema,
        }).collect(),
        web_search: None,
    };
    assert_eq!(profile.functions.len(), expected_functions);
    assert!(profile.functions.len() <= limits.execution.native.function_count);
    let profile_bytes = bounded_json_size(&profile, limits.execution.native.schema_bytes)
        .expect("selected native catalogue exceeds explicit fixture schema bound");
    let mut validation = json!({"schema":input.output_schema.as_ref().unwrap(),
        "profile":profile,"output_bytes":input.limits.output_bytes});
    if limits.native_retry.is_some() {
        validation["repair_enabled"] = json!(true);
    }
    let validation_bytes = bounded_json_size(&validation, limits.execution.native.schema_bytes)
        .expect("complete native validation envelope exceeds explicit fixture schema bound");
    assert!(validation_bytes > profile_bytes);
    profile_bytes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_collaboration_actual_read_write_read_final_and_lost_ack_restart() {
    let mut lab = Lab::new("read-write");
    let mut limits = (*lab.limits).clone();
    let seed_schema_bytes = limits.execution.native.schema_bytes;
    // This selected V1 catalogue outgrew the seed's schema allowance. Keep the
    // finite override local to this positive fixture, not Lab or production.
    limits.execution.native.schema_bytes = 32_768;
    assert_eq!(limits.execution.native.function_count, 64);
    assert_eq!(limits.execution.native.json_nodes, 4_096);
    limits.validate().unwrap();
    lab.limits = Arc::new(limits);
    assert!(assert_fixture_native_profile_fits(&lab.limits, &lab.input(4), 36) > seed_schema_bytes);
    std::fs::write(lab.root.join("limits.json"),
        serde_json::to_vec_pretty(&*lab.limits).unwrap()).unwrap();
    let mut c = lab.controller("controller-one").await;
    let peer = Peer::start(|index, request| {
        no_memory_notices(request);
        assert_eq!(request["store"], false);
        assert_eq!(request["parallel_tool_calls"], false);
        let tools = request["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 36);
        let names: std::collections::BTreeSet<_> = tools.iter()
            .map(|tool| tool["name"].as_str().unwrap()).collect();
        assert_eq!(names, std::collections::BTreeSet::from([
            "collab_tasks_list", "collab_task_get", "collab_task_history_list",
            "collab_agents_list", "collab_agent_get", "collab_definition_get",
            "collab_memory_list", "collab_memory_get", "collab_memory_set",
            "collab_task_progress", "collab_task_delegate", "collab_task_assign",
            "collab_task_cancel", "collab_task_retry", "collab_workers_list",
            "collab_worker_get", "collab_task_attempts_list", "collab_task_checkpoints_list",
            "collab_task_releases_list", "collab_release_get", "collab_release_checks_list",
            "collab_call_get", "collab_repository_tree", "collab_repository_read",
            "collab_repository_search", "collab_repository_diff", "collab_repository_history",
            "collab_session_get", "collab_sessions_list", "collab_limits_get",
            "collab_repositories_list", "collab_repository_get", "collab_definition_update",
            "collab_machines_list", "collab_machine_get", "collab_read_continue",
        ]));
        assert!(request["tools"].as_array().unwrap().iter().any(|tool| tool["name"] == "collab_read_continue"));
        assert!(
            request["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|tool| tool["name"] == "collab_limits_get")
        );
        assert!(
            request["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|tool| tool["name"] == "collab_definition_update")
        );
        match index {
            0 => Some(function(index, "collab_memory_get", json!({"key":"note"}))),
            1 => {
                let p = previous(request);
                assert_eq!(p["outcome"]["status"], "ok");
                assert!(p["outcome"]["data"]["value"].is_null());
                Some(function(
                    index,
                    "collab_memory_set",
                    json!({"key":"note","expected_revision":null,"value":{"answer":7}}),
                ))
            }
            2 => {
                let p = previous(request);
                assert_eq!(p["outcome"]["status"], "ok");
                assert_eq!(p["outcome"]["data"]["value"]["revision"], 1);
                Some(function(index, "collab_memory_get", json!({"key":"note"})))
            }
            3 => {
                let p = previous(request);
                assert_eq!(p["outcome"]["data"]["value"]["value"], json!({"answer":7}));
                Some(final_value())
            }
            _ => panic!("unexpected additional model request"),
        }
    })
    .await;
    let proxy = Proxy::start(
        lab.url.clone(),
        Some("collab_memory_set"),
        false,
        lab.limits.clone(),
    )
    .await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let task = submit(&api, lab.input(4)).await;
    let mut w = lab.worker(&proxy.url, &peer.endpoint, "first");
    let accepted = until(&api, &task.id, |t| t.status == TaskStatus::Succeeded).await;
    assert_eq!(accepted.result.as_ref().unwrap().output, Some(json!(7)));
    assert!(proxy.dropped.load(Ordering::SeqCst));
    assert_eq!(peer.count(), 4);

    assert!(
        matches!(memory(&api).await,Outcome::Ok{data:Data::Memory(Some(ref m))} if m.revision==1&&m.value==json!({"answer":7}))
    );
    let dir = lab
        .state()
        .join("collaboration")
        .join(process::component(&accepted.attempt.as_ref().unwrap().id));
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 3);
    for e in std::fs::read_dir(&dir).unwrap() {
        let j: Value = serde_json::from_slice(&std::fs::read(e.unwrap().path()).unwrap()).unwrap();
        assert_eq!(j["consumed"], true);
        assert!(!j["outcome_context"].is_null());
    }
    w.stop().await;
    c.stop().await;
    assert_eq!(
        lab.usage(&accepted.attempt.as_ref().unwrap().id),
        (4, Some(20), Some(8))
    );
    let mut c = lab.controller("controller-two").await;
    let registered = proxy.registrations.load(Ordering::SeqCst);
    let mut w = lab.worker(&proxy.url, &peer.endpoint, "second");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while proxy.registrations.load(Ordering::SeqCst) <= registered {
        assert!(
            tokio::time::Instant::now() < deadline,
            "replacement registration absent"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(get(&api, &task.id).await, accepted);
    assert!(matches!(memory(&api).await, Outcome::Ok { data: Data::Memory(Some(ref value)) }
        if value.revision == 1 && value.value == json!({"answer":7})));
    assert!(!lab.state().join("memory-notices").exists());
    assert_eq!(peer.count(), 4);
    w.stop().await;
    c.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_collaboration_exhausted_allowance_does_not_dispatch_write() {
    let lab = Lab::new("allowance");
    let mut c = lab.controller("controller").await;
    let peer = Peer::start(|i, _| {
        Some(function(
            i,
            "collab_memory_set",
            json!({"key":"note","expected_revision":null,"value":7}),
        ))
    })
    .await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let task = submit(&api, lab.input(1)).await;
    let mut w = lab.worker(&lab.url, &peer.endpoint, "worker");
    let failed = until(&api, &task.id, |t| {
        t.status == TaskStatus::Failed && t.attempt.as_ref().is_some_and(|a| a.stopped)
    })
    .await;
    assert!(!failed.attempt.as_ref().unwrap().uncertain);
    assert_eq!(peer.count(), 1);
    assert!(matches!(
        memory(&api).await,
        Outcome::Ok {
            data: Data::Memory(None)
        }
    ));
    w.stop().await;
    c.stop().await;
    assert_eq!(lab.usage(&failed.attempt.as_ref().unwrap().id).0, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_collaboration_unknown_tool_outcome_blocks_next_post_and_retry_after_restart() {
    let lab = Lab::new("unknown-tool");
    let mut c = lab.controller("controller").await;
    let peer = Peer::start(|i, _| {
        Some(function(
            i,
            "collab_memory_set",
            json!({"key":"note","expected_revision":null,"value":7}),
        ))
    })
    .await;
    let proxy = Proxy::start(
        lab.url.clone(),
        Some("collab_memory_set"),
        true,
        lab.limits.clone(),
    )
    .await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let task = submit(&api, lab.input(3)).await;
    let mut w = lab.worker(&proxy.url, &peer.endpoint, "first");
    let failed = until(&api, &task.id, |t| {
        t.status == TaskStatus::Failed
            && t.attempt.as_ref().is_some_and(|a| a.stopped && a.uncertain)
    })
    .await;
    assert_eq!(peer.count(), 1);
    assert!(
        matches!(memory(&api).await,Outcome::Ok{data:Data::Memory(Some(ref m))}if m.revision==1)
    );
    w.stop().await;
    c.stop().await;
    let mut c = lab.controller("controller-two").await;
    proxy.hide_lookup.store(false, Ordering::SeqCst);
    let registered = proxy.registrations.load(Ordering::SeqCst);
    let mut w = lab.worker(&proxy.url, &peer.endpoint, "second");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while proxy.registrations.load(Ordering::SeqCst) <= registered {
        assert!(
            tokio::time::Instant::now() < deadline,
            "replacement registration absent"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let current = get(&api, &task.id).await;
    assert_eq!(
        current.attempt.as_ref().unwrap().id,
        failed.attempt.as_ref().unwrap().id
    );
    assert!(current.attempt.as_ref().unwrap().uncertain);
    assert!(
        matches!(api.call(Operation::RetryTask{operation_id:"retry".into(),task_id:task.id.clone(),expected_revision:current.revision}).await.unwrap().outcome,Outcome::Refused{code,..}if code=="reconciliation_required")
    );
    assert_eq!(peer.count(), 1);
    w.stop().await;
    c.stop().await;
}

// Traceability scope: Controlled actual-worker requests include search only for the accepted opt-in profile; unconfigured search output is refused. This does not independently prove no attempted executor fallback.
//= spec/impl/v2/implementation.md#native-tool-profiles-and-continuations
//= type=test
//# An agent definition MUST explicitly configure its native provider-tool capabilities.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_collaboration_search_is_explicit_and_unknown_tools_never_fall_back() {
    for enabled in [false, true] {
        let lab = Lab::new(if enabled { "search-on" } else { "search-off" });
        let mut c = lab.controller("controller").await;
        let peer=Peer::start(move|_,request|{assert_eq!(request["tools"].as_array().unwrap().iter().any(|t|t["type"]=="web_search"),enabled);if enabled{assert_eq!(request["max_tool_calls"],2);}else{assert!(request.get("max_tool_calls").is_none());}
            Some(envelope(json!([{"type":"web_search_call","id":"search-one","status":"completed","action":{"type":"search","query":"fixture","sources":[]}},
                {"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"7","annotations":[]}]}])))
        }).await;
        let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
        let mut input = lab.input(2);
        if enabled {
            input.definition.execution_profile = Some(ExecutionProfile::CollaborationV1 {
                web_search: Some(NativeSearchProfile { max_tool_calls: 2 }),
            });
        }
        let task = submit(&api, input).await;
        let mut w = lab.worker(&lab.url, &peer.endpoint, "worker");
        let terminal = until(&api, &task.id, |t| {
            matches!(t.status, TaskStatus::Failed | TaskStatus::Succeeded)
                && t.attempt.as_ref().is_some_and(|a| a.stopped)
        })
        .await;
        assert_eq!(
            terminal.status,
            if enabled {
                TaskStatus::Succeeded
            } else {
                TaskStatus::Failed
            }
        );
        assert_eq!(peer.count(), 1);
        assert_eq!(terminal.attempt.unwrap().uncertain, !enabled);
        w.stop().await;
        c.stop().await;
    }
}

#[test]
fn native_collaboration_profile_absence_roundtrips_and_ambiguities_refuse() {
    let lab = Lab::new("profile");
    let mut legacy = lab.input(5);
    legacy.definition.execution_profile = None;
    let bytes = serde_json::to_vec(&legacy).unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("execution_profile"));
    let parsed: TaskInput = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(serde_json::to_vec(&parsed).unwrap(), bytes);
    assert!(!borg_runtime::execution_profile::collaboration(&parsed));
    let mut bad = lab.input(5);
    bad.native_workflow = Some(Box::new(NativeWorkflow::InspectCurrentTaskThenContinue {}));
    assert!(borg_runtime::execution_profile::validate(&bad).is_err());
    bad.native_workflow = None;
    bad.definition.executor = ExecutorKind::Coding;
    bad.definition.execution_profile = Some(ExecutionProfile::CollaborationV1 {
        web_search: Some(NativeSearchProfile { max_tool_calls: 1 }),
    });
    assert!(borg_runtime::execution_profile::validate(&bad).is_err());
    // The original unknown-kind specimen is now an explicitly supported profile.
    let current_value = json!({"kind":"collaboration_v2"});
    let current: ExecutionProfile = serde_json::from_value(current_value.clone()).unwrap();
    assert_eq!(current, ExecutionProfile::CollaborationV2 { publication: None, web_search: None });
    assert_eq!(serde_json::to_value(current).unwrap(), current_value);
    for value in [
        json!({"kind":"unsupported_profile_fixture"}),
        json!({"kind":"collaboration_v1","unknown":true}),
        json!({"kind":"collaboration_v1","web_search":{"max_tool_calls":1,"extra":1}}),
    ] {
        let decoded = serde_json::from_value::<ExecutionProfile>(value.clone());
        assert!(decoded.is_err(), "unsupported or ambiguous profile accepted: {value}; decoded={decoded:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_collaboration_cancel_before_dispatch_refuses_write_and_continuation() {
    let lab = Lab::new("cancel");
    let mut c = lab.controller("controller").await;
    let peer = Peer::start(|i, _| {
        Some(function(
            i,
            "collab_memory_set",
            json!({"key":"note","expected_revision":null,"value":7}),
        ))
    })
    .await;
    let proxy = Proxy::start(lab.url.clone(), None, false, lab.limits.clone()).await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let task = submit(&api, lab.input(3)).await;
    *proxy.cancel_before_tool.lock().unwrap() = Some(task.id.clone());
    let mut w = lab.worker(&proxy.url, &peer.endpoint, "worker");
    let cancelled = until(&api, &task.id, |t| {
        t.status == TaskStatus::Cancelled && t.attempt.as_ref().is_some_and(|a| a.stopped)
    })
    .await;
    assert_eq!(peer.count(), 1);
    assert!(!cancelled.attempt.as_ref().unwrap().uncertain);
    assert!(matches!(
        memory(&api).await,
        Outcome::Ok {
            data: Data::Memory(None)
        }
    ));
    w.stop().await;
    c.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_collaboration_configured_child_quiesces_and_restarts_without_another_model_request()
{
    let mut lab = Lab::new("delegate");
    let mut metadata = (*lab.limits).clone();
    charter_worker::configure_native_schema(&mut metadata);
    metadata.validate().unwrap();
    lab.limits = Arc::new(metadata);
    std::fs::write(lab.root.join("limits.json"), serde_json::to_vec_pretty(&*lab.limits).unwrap()).unwrap();
    let mut c = lab.controller("controller").await;
    let peer = Peer::start(|i, _| {
        Some(function(
            i,
            "collab_task_delegate",
            json!({"instruction":"Implement the selected change"}),
        ))
    })
    .await;
    let proxy = Proxy::start(
        lab.url.clone(),
        Some("collab_task_delegate"),
        false,
        lab.limits.clone(),
    )
    .await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let mut input = lab.input(4);
    let mut limits = input.limits.clone();
    limits.model_requests = 1;
    limits.result_bytes = 8192;
    let repository_registration = borg_runtime::repository_catalogue::Registration {
        pull_request_target: None,
        repository_id: "configured-child-repository".into(),
        registration_revision: "1".into(),
        display_name: "Configured child fixture".into(),
        source_endpoint: lab
            .root
            .join("unopened-repository.git")
            .display()
            .to_string(),
        checkpoint_endpoint: lab
            .root
            .join("unopened-repository.git")
            .display()
            .to_string(),
        source_store_key: "unopened-repository.git".into(),
        checkpoint_store_key: "unopened-repository.git".into(),
    };
    assert!(matches!(
        api.call(Operation::RegisterRepository {
            operation_id: "register-configured-child-repository".into(),
            registration: repository_registration.clone()
        })
        .await
        .unwrap()
        .outcome,
        Outcome::Ok {
            data: Data::RepositoryRegistration(_)
        }
    ));
    input.native_workflow = Some(Box::new(NativeWorkflow::CodingThenContinue {
        child: CodingTaskTemplate { instruction_source: None,
            pull_request: None,
            instruction_prefix: "Bounded configured child".into(),
            definition: Definition {
                charter: None,
                limit_overrides: None,
                inference: Some(InferenceSettings::selected()),
                execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
                id: "coding-definition".into(),
                revision: "collaboration-v1".into(),
                executor: ExecutorKind::Coding,
            },
            repository: Repository {
                pull_request_target: None,
                registration: Some(repository_registration.binding()),
                url: String::new(),
                start_commit: "a".repeat(40),
                checkpoint_remote: String::new(),
            },
            acceptance_commands: vec![],
            dependencies: vec![],
            limits,
        },
    }));
    let task = submit(&api, input).await;
    let mut w = lab.worker(&proxy.url, &peer.endpoint, "first");
    let waiting = until(&api, &task.id, |t| {
        t.waiting_child_id.is_some() && t.attempt.as_ref().is_some_and(|a| a.stopped)
    })
    .await;
    assert_eq!(peer.count(), 1);
    assert!(!waiting.attempt.as_ref().unwrap().uncertain);
    assert!(waiting.result.is_none());
    assert!(proxy.dropped.load(Ordering::SeqCst));
    let child = get(&api, waiting.waiting_child_id.as_ref().unwrap()).await;
    assert!(borg_runtime::execution_profile::collaboration(&child.input));
    assert_eq!(child.parent_task_id, Some(task.id.clone()));
    assert_eq!(child.status, TaskStatus::Queued);
    w.stop().await;
    c.stop().await;
    let mut c = lab.controller("controller-two").await;
    let registered = proxy.registrations.load(Ordering::SeqCst);
    let mut w = lab.worker(&proxy.url, &peer.endpoint, "second");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while proxy.registrations.load(Ordering::SeqCst) <= registered {
        assert!(
            tokio::time::Instant::now() < deadline,
            "replacement registration absent"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(get(&api, &task.id).await, waiting);
    assert_eq!(peer.count(), 1);
    w.stop().await;
    c.stop().await;
}

fn no_worker_credential(path: &Path) {
    for entry in std::fs::read_dir(path).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            no_worker_credential(&path);
        } else if path.is_file() {
            let bytes = std::fs::read(&path).unwrap();
            assert!(
                !bytes
                    .windows(b"synthetic-native-loop-key".len())
                    .any(|w| w == b"synthetic-native-loop-key"),
                "credential persisted at {}",
                path.display()
            );
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_collaboration_credential_intent_never_journals_or_dispatches() {
    let lab = Lab::new("credential-intent");
    let mut c = lab.controller("controller").await;
    let peer = Peer::start(|i, _| {
        Some(function(
            i,
            "collab_memory_set",
            json!({"key":"note","expected_revision":null,"value":"synthetic-native-loop-key"}),
        ))
    })
    .await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let task = submit(&api, lab.input(2)).await;
    let mut w = lab.worker(&lab.url, &peer.endpoint, "worker");
    let failed = until(&api, &task.id, |t| {
        t.status == TaskStatus::Failed && t.attempt.as_ref().is_some_and(|a| a.stopped)
    })
    .await;
    assert!(!failed.attempt.as_ref().unwrap().uncertain);
    assert_eq!(peer.count(), 1);
    assert!(matches!(
        memory(&api).await,
        Outcome::Ok {
            data: Data::Memory(None)
        }
    ));
    w.stop().await;
    c.stop().await;
    no_worker_credential(&lab.state());
    assert!(!lab.state().join("collaboration").exists());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_collaboration_credential_original_outcome_refuses_retention_and_stays_uncertain() {
    let lab = Lab::new("credential-result");
    let mut c = lab.controller("controller").await;
    let peer =
        Peer::start(|i, _| Some(function(i, "collab_memory_get", json!({"key":"note"})))).await;
    let proxy = Proxy::start(lab.url.clone(), None, false, lab.limits.clone()).await;
    *proxy.seed_memory_before_tool.lock().unwrap() = Some(json!("synthetic-native-loop-key"));
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let task = submit(&api, lab.input(2)).await;
    let mut w = lab.worker(&proxy.url, &peer.endpoint, "worker");
    let failed = until(&api, &task.id, |t| {
        t.status == TaskStatus::Failed
            && t.attempt.as_ref().is_some_and(|a| a.stopped && a.uncertain)
    })
    .await;
    assert_eq!(peer.count(), 1);
    assert!(
        matches!(memory(&api).await,Outcome::Ok{data:Data::Memory(Some(ref m))}if m.value==json!("synthetic-native-loop-key"))
    );
    w.stop().await;
    c.stop().await;
    no_worker_credential(&lab.state());
    let dir = lab
        .state()
        .join("collaboration")
        .join(process::component(&failed.attempt.as_ref().unwrap().id));
    for e in std::fs::read_dir(&dir).unwrap() {
        let j: Value = serde_json::from_slice(&std::fs::read(e.unwrap().path()).unwrap()).unwrap();
        assert!(j["outcome"].is_null());
        assert_eq!(j["consumed"], false);
    }
}

const QUOTED_SECRET: &str = r#"synthetic-native-"loop-key\token"#;
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_collaboration_quoted_provider_observation_refuses_before_progress_persistence() {
    let lab = Lab::new("quoted-credential-observation");
    let mut c = lab.controller("controller").await;
    let peer = Peer::start(|_, _| {
        let mut response = final_value();
        response["output"].as_array_mut().unwrap().insert(
            0,
            json!({"type":"reasoning","summary":[],"encrypted_content":QUOTED_SECRET}),
        );
        Some(response)
    })
    .await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let task = submit(&api, lab.input(1)).await;
    let mut w = lab.worker(&lab.url, &peer.endpoint, "quoted-credential-worker");
    let failed = until(&api, &task.id, |t| {
        t.status == TaskStatus::Failed && t.attempt.as_ref().is_some_and(|a| a.stopped)
    })
    .await;
    assert!(!failed.attempt.as_ref().unwrap().uncertain);
    assert_eq!(peer.count(), 1);
    w.stop().await;
    c.stop().await;
    let raw = QUOTED_SECRET.to_string();
    let encoded = serde_json::to_string(QUOTED_SECRET).unwrap();
    let twice = serde_json::to_string(&encoded).unwrap();
    let needles = [
        raw,
        encoded.trim_matches('"').to_string(),
        twice.trim_matches('"').to_string(),
    ];
    let mut dirs = vec![lab.state()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let p = entry.unwrap().path();
            if p.is_dir() {
                dirs.push(p);
            } else if p.is_file() {
                let bytes = std::fs::read(&p).unwrap();
                for n in &needles {
                    assert!(
                        !bytes.windows(n.len()).any(|w| w == n.as_bytes()),
                        "quoted credential persisted at {}",
                        p.display()
                    );
                }
            }
        }
    }
}

// Traceability scope: Before the final controlled provider reply, both actual worker journals are durable and consumed and their exact original receipt strings equal the corresponding continuation outputs.
//= spec/impl/v2/implementation.md#native-tool-profiles-and-continuations
//= type=test
//# A native model continuation MUST use the recorded outcome of its corresponding tool call.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_collaboration_metadata_two_call_batch_retains_both_receipts_before_final() {
    let lab = Lab::new("metadata-two-call-batch");
    let mut controller = lab.controller("controller").await;
    let observed_outputs = Arc::new(Mutex::new(Vec::<Value>::new()));
    let saved_outputs = observed_outputs.clone();
    let journal_root = lab.state().join("collaboration");
    let peer = Peer::start(move |index, request| {
        assert_eq!(request["parallel_tool_calls"], true);
        match index {
            0 => Some(envelope(json!([
                function(
                    0,
                    "collab_memory_set",
                    json!({"key":"note","expected_revision":null,"value":{"answer":7}}),
                )["output"][0]
                    .clone(),
                function(1, "collab_memory_get", json!({"key":"note"}))["output"][0].clone()
            ]))),
            1 => {
                let outputs: Vec<_> = request["input"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|item| item["type"] == "function_call_output")
                    .cloned()
                    .collect();
                assert_eq!(outputs.len(), 2);
                assert_eq!(outputs[0]["call_id"], "call-0");
                assert_eq!(outputs[1]["call_id"], "call-1");
                for output in &outputs {
                    let receipt: Value =
                        serde_json::from_str(output["output"].as_str().unwrap()).unwrap();
                    assert_eq!(receipt["outcome"]["status"], "ok");
                    assert_eq!(receipt["outcome"]["data"]["value"]["revision"], 1);
                    assert_eq!(
                        receipt["outcome"]["data"]["value"]["value"],
                        json!({"answer":7})
                    );
                }
                // Both receipts must already be durable and consumed when the
                // final provider request arrives, before its response exists.
                let mut attempts = std::fs::read_dir(&journal_root).unwrap();
                let journal_dir = attempts.next().unwrap().unwrap().path();
                assert!(attempts.next().is_none());
                for (ordinal, filename) in ["0000000001.json", "0000000001-0000000002.json"]
                    .iter()
                    .enumerate()
                {
                    let journal: Value =
                        serde_json::from_slice(&std::fs::read(journal_dir.join(filename)).unwrap())
                            .unwrap();
                    assert_eq!(journal["consumed"], true);
                    let call = &journal["request"]["input"]["call"];
                    let retained = json!({
                        "operation_id": call["operation_id"],
                        "logical_call_id": call["logical_call_id"],
                        "outcome": journal["outcome"],
                        "commit": journal["outcome_context"]["commit"]
                    })
                    .to_string();
                    assert_eq!(outputs[ordinal]["output"], retained);
                }
                *saved_outputs.lock().unwrap() = outputs;
                Some(final_value())
            }
            _ => panic!("a two-call batch requires only one final continuation"),
        }
    })
    .await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let mut input = lab.input(2);
    let mut native = lab.limits.execution.native.clone();
    assert_eq!(native.function_calls_per_response, 1);
    native.function_calls_per_response = 2;
    input.definition.limit_overrides =
        Some(Box::new(borg_runtime::limits::ExecutionLimitOverrides {
            native: Some(native),
            ..Default::default()
        }));
    assert!(input.resolved_limits.is_none());
    let task = submit(&api, input).await;
    assert_eq!(
        task.input
            .execution_limits()
            .unwrap()
            .native
            .function_calls_per_response,
        2
    );
    let mut worker = lab.worker(&lab.url, &peer.endpoint, "batch-worker");
    let accepted = until(&api, &task.id, |t| {
        t.status == TaskStatus::Succeeded && t.attempt.as_ref().is_some_and(|a| a.stopped)
    })
    .await;
    assert_eq!(accepted.result.as_ref().unwrap().output, Some(json!(7)));
    assert_eq!(peer.count(), 2);
    assert!(matches!(
        memory(&api).await,
        Outcome::Ok { data: Data::Memory(Some(ref m)) }
            if m.revision == 1 && m.value == json!({"answer":7})
    ));
    let attempt = accepted.attempt.as_ref().unwrap();
    assert!(!attempt.uncertain);
    let dir = lab
        .state()
        .join("collaboration")
        .join(process::component(&attempt.id));
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 2);
    let outputs = observed_outputs.lock().unwrap().clone();
    for (index, filename) in ["0000000001.json", "0000000001-0000000002.json"]
        .iter()
        .enumerate()
    {
        let journal: Value =
            serde_json::from_slice(&std::fs::read(dir.join(filename)).unwrap()).unwrap();
        assert_eq!(journal["consumed"], true);
        let call = &journal["request"]["input"]["call"];
        let expected_id = if index == 0 {
            format!("native-collab-{}-1", attempt.id)
        } else {
            format!("native-collab-{}-1-call-2", attempt.id)
        };
        assert_eq!(call["operation_id"], expected_id);
        assert_eq!(call["logical_call_id"], format!("call-{index}"));
        let retained = json!({
            "operation_id": call["operation_id"],
            "logical_call_id": call["logical_call_id"],
            "outcome": journal["outcome"],
            "commit": journal["outcome_context"]["commit"]
        })
        .to_string();
        assert_eq!(outputs[index]["output"], retained);
    }
    worker.stop().await;
    controller.stop().await;
    assert_eq!(lab.usage(&attempt.id), (2, Some(10), Some(4)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_reasoning_content_continues_exactly_without_public_disclosure() {
    const PRIVATE: &str = "SYNTHETIC_PRIVATE_REASONING_SENTINEL";
    let lab = Lab::new("reasoning-private");
    let mut controller = lab.controller("controller").await;
    let first_reasoning = json!({
        "type":"reasoning","id":"rs-first","status":"completed",
        "summary":[{"type":"summary_text","text":format!("{PRIVATE} summary")}],
        "encrypted_content":format!("{PRIVATE} encrypted"),
        "content":[
            {"type":"reasoning_text","text":format!("{PRIVATE} first part")},
            {"type":"reasoning_text","text":format!("{PRIVATE} second part")}
        ]
    });
    let expected_reasoning = first_reasoning.clone();
    let peer = Peer::start(move |index, request| match index {
        0 => {
            let mut response = function(index, "collab_memory_get", json!({"key":"note"}));
            response["output"]
                .as_array_mut()
                .unwrap()
                .insert(0, expected_reasoning.clone());
            Some(response)
        }
        1 => {
            assert_eq!(request["input"][1], expected_reasoning);
            let original = previous(request);
            assert_eq!(original["outcome"]["status"], "ok");
            let mut response = final_value();
            let mut final_reasoning = expected_reasoning.clone();
            final_reasoning["id"] = json!("rs-final");
            final_reasoning["content"][0]["text"] = json!(format!("{PRIVATE} final part"));
            response["output"]
                .as_array_mut()
                .unwrap()
                .insert(0, final_reasoning);
            Some(response)
        }
        _ => panic!("reasoning content cannot create an extra model request"),
    })
    .await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let task = submit(&api, lab.input(2)).await;
    let mut worker = lab.worker(&lab.url, &peer.endpoint, "worker");
    let accepted = until(&api, &task.id, |t| t.status == TaskStatus::Succeeded).await;
    assert_eq!(accepted.result.as_ref().unwrap().output, Some(json!(7)));
    assert_eq!(peer.count(), 2);
    assert_eq!(
        peer.requests.lock().unwrap()[1]["input"][1],
        first_reasoning
    );
    assert!(!serde_json::to_string(&accepted).unwrap().contains(PRIVATE));
    assert_eq!(
        serde_json::from_str::<Value>(&accepted.recent_output).unwrap(),
        final_value()["output"]
    );

    let browser = reqwest::Client::builder().no_proxy().build().unwrap();
    for path in [
        format!("/tasks/{}", task.id),
        format!("/tasks/{}/history", task.id),
        format!("/tasks/{}/attempts", task.id),
        "/activity".into(),
    ] {
        let response = browser
            .get(format!("{}{path}", lab.url))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert!(
            !response.text().await.unwrap().contains(PRIVATE),
            "private reasoning in UI {path}"
        );
    }
    worker.stop().await;
    controller.stop().await;
    assert_eq!(
        lab.usage(&accepted.attempt.as_ref().unwrap().id),
        (2, Some(10), Some(4))
    );
    let db = rusqlite::Connection::open_with_flags(
        lab.root.join("tasks.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let history: Vec<String> = db
        .prepare("SELECT body FROM history WHERE task_id=?1 ORDER BY seq")
        .unwrap()
        .query_map([&task.id], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(
        history
            .iter()
            .any(|body| body.contains("Native provider output observations"))
    );
    assert!(history.iter().all(|body| !body.contains(PRIVATE)));
    drop(db);

    // This scans actual logs, the closed controller database and worker outbox
    // and journals. The controlled provider retains requests in memory only.
    let mut directories = vec![lab.root.clone()];
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                directories.push(path);
            } else if path.is_file() {
                let bytes = std::fs::read(&path).unwrap();
                assert!(
                    !bytes
                        .windows(PRIVATE.len())
                        .any(|part| part == PRIVATE.as_bytes()),
                    "private reasoning persisted at {}",
                    path.display()
                );
            }
        }
    }
}

// Traceability scope: Real worker/controller paths with controlled model and MCP
// failures retain the native executor even with coding capacity and a positively
// checked executable launch tripwire. The fixture is not an actual Codex launch.
//= spec/impl/v2/implementation.md#native-tool-profiles-and-continuations
//= type=test
//# The platform MUST NOT switch a task's executor automatically after a model or tool failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_failures_preserve_executor_and_never_launch_configured_coding_tripwire() {
    use std::os::unix::fs::PermissionsExt;

    for tool_failure in [false, true] {
        let mut lab = Lab::new(if tool_failure {
            "no-fallback-tool"
        } else {
            "no-fallback-model"
        });
        let mut policy = (*lab.limits).clone();
        policy.revision = format!(
            "fixture-no-fallback-{}",
            if tool_failure { "tool" } else { "model" }
        );
        policy.worker.coding_capacity = 1;
        lab.limits = Arc::new(policy);
        std::fs::write(
            lab.root.join("limits.json"),
            serde_json::to_vec_pretty(&*lab.limits).unwrap(),
        )
        .unwrap();
        let marker = lab.root.join("coding-executor-launched");
        let tripwire = lab.root.join("coding-launch-tripwire.sh");
        assert!(
            marker
                .to_str()
                .unwrap()
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || "/-_.".contains(ch))
        );
        std::fs::write(
            &tripwire,
            format!(
                "#!/bin/sh\nprintf '%s\\n' launched > '{}'\nexit 86\n",
                marker.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&tripwire, std::fs::Permissions::from_mode(0o700)).unwrap();
        // Positive control: this exact configured executable produces the marker.
        assert_eq!(
            Command::new(&tripwire).env_clear().status().unwrap().code(),
            Some(86)
        );
        assert_eq!(std::fs::read_to_string(&marker).unwrap(), "launched\n");
        std::fs::remove_file(&marker).unwrap();

        let mut controller = lab.controller("controller").await;
        let peer = Peer::start(move |index, _| {
            assert_eq!(
                index, 0,
                "native failure must not start another model request"
            );
            if tool_failure {
                Some(function(
                    index,
                    "collab_memory_set",
                    json!({"key":"note","expected_revision":null,"value":7}),
                ))
            } else {
                let mut response = final_value();
                response["status"] = json!("incomplete");
                Some(response)
            }
        })
        .await;
        let proxy = Proxy::start(
            lab.url.clone(),
            tool_failure.then_some("collab_memory_set"),
            tool_failure,
            lab.limits.clone(),
        )
        .await;
        let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
        let accepted = submit(&api, lab.input(3)).await;
        let mut worker =
            lab.worker_with_codex(&proxy.url, &peer.endpoint, "tripwire-worker", &tripwire);
        let failed = until(&api, &accepted.id, |task| {
            task.status == TaskStatus::Failed
                && task.attempt.as_ref().is_some_and(|attempt| attempt.stopped)
        })
        .await;
        assert_eq!(failed.input, accepted.input);
        assert_eq!(failed.input.definition.executor, ExecutorKind::Native);
        assert_eq!(failed.session_id, accepted.session_id);
        assert_eq!(peer.count(), 1);
        assert!(
            !marker.exists(),
            "native failure launched the coding executor"
        );
        assert!(
            !lab.root.join("worker/workspaces").exists(),
            "native failure created a coding workspace"
        );
        assert_eq!(proxy.dropped.load(Ordering::SeqCst), tool_failure);
        if tool_failure {
            assert!(
                matches!(memory(&api).await, Outcome::Ok { data: Data::Memory(Some(ref value)) } if value.revision == 1 && value.value == json!(7))
            );
        }
        worker.stop().await;
        controller.stop().await;
        assert!(!marker.exists(), "cleanup launched the coding executor");
        let db = rusqlite::Connection::open_with_flags(
            lab.root.join("tasks.sqlite"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        let mut query = db.prepare("SELECT body FROM history WHERE task_id=?1 AND kind='worker_observation' ORDER BY seq").unwrap();
        let observations: Vec<Value> = query
            .query_map([&accepted.id], |row| row.get::<_, String>(0))
            .unwrap()
            .map(|row| serde_json::from_str(&row.unwrap()).unwrap())
            .collect();
        let summaries: Vec<_> = observations
            .iter()
            .filter(|value| value["kind"] == "executor_outcome")
            .collect();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0]["success"], false);
        let summary = summaries[0]["summary"].as_str().unwrap();
        assert!(
            summary.contains(if tool_failure {
                "collaboration call outcome uncertain"
            } else {
                "native_provider_response_not_completed"
            }),
            "original native failure was replaced: {summary}"
        );
        let count: i64 = db
            .query_row(
                "SELECT count(*) FROM attempts WHERE task_id=?1",
                [&accepted.id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "failure created an automatic successor attempt");
    }
}

// A canonical MCP read has committed while its exact reply remains in flight
// to the real native worker. A later same-agent write neither changes that
// original result nor injects a notice into the next required request. This
// does not hold computation inside the controller or qualify Codex tools.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_explicit_memory_write_preserves_in_flight_mcp_reply_without_notice() {
    async fn report(api: &ApiClient, attempt: &Attempt, id: &str, observation: Observation) {
        let reply = api
            .call(Operation::ObserveWorker {
                report: Report {
                    report_id: id.into(),
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
    let lab = Lab::new("explicit-memory-held-tool-reply");
    let mut controller = lab.controller("controller").await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let writer = submit_identified(&api, lab.input(1), "held-notice-writer").await;
    let actor_worker = Worker {
        id: "held-notice-writer".into(),
        incarnation: "one".into(),
        coding_capacity: 0,
        native_capacity: 1,
    };
    assert!(matches!(
        api.call(Operation::RegisterWorker {
            operation_id: "held-register-writer".into(),
            worker: actor_worker.clone()
        })
        .await
        .unwrap()
        .outcome,
        Outcome::Ok { .. }
    ));
    let commands = match api
        .call(Operation::AwaitCommands {
            operation_id: "held-claim-writer".into(),
            worker_id: actor_worker.id.clone(),
            incarnation: actor_worker.incarnation.clone(),
            acknowledged: vec![],
        })
        .await
        .unwrap()
        .outcome
    {
        Outcome::Ok {
            data: Data::Commands(commands),
        } => commands,
        other => panic!("writer claim: {other:?}"),
    };
    assert_eq!(commands.len(), 1);
    let WorkerCommand::RunAttempt {
        attempt: writer_attempt,
        ..
    } = &commands[0]
    else {
        panic!("writer command missing")
    };
    let writer_attempt = writer_attempt.clone();
    assert_eq!(writer_attempt.task_id, writer.id);
    report(
        &api,
        &writer_attempt,
        "held-writer-start",
        Observation::Started {
            thread_id: None,
            turn_id: None,
        },
    )
    .await;
    let first = api
        .call(Operation::SetMemory {
            operation_id: "held-memory-before-reader".into(),
            task_id: writer.id.clone(),
            attempt_id: writer_attempt.id.clone(),
            generation: writer_attempt.generation,
            key: "note".into(),
            expected_revision: None,
            value: json!(1),
        })
        .await
        .unwrap();
    assert!(
        matches!(first.outcome, Outcome::Ok { data: Data::Memory(Some(ref value)) } if value.revision == 1)
    );

    let held = Arc::new(HeldToolReply::new("collab_memory_get"));
    let proxy = Proxy::start(lab.url.clone(), None, false, lab.limits.clone()).await;
    *proxy.held_reply.lock().unwrap() = Some(held.clone());
    let observed = Arc::new(Mutex::new(None::<String>));
    let observed_by_peer = observed.clone();
    let held_by_peer = held.clone();
    let peer = Peer::start(move |index, request| {
        no_memory_notices(request);
        match index {
            0 => Some(function(index, "collab_memory_get", json!({"key":"note"}))),
            1 => {
                assert!(held_by_peer.released.load(Ordering::SeqCst));
                let raw = held_by_peer.original.lock().unwrap().clone().unwrap();
                let original: borg_runtime::mcp::CallResult = serde_json::from_str(raw["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
                let commit = original.reply.commit.as_ref().unwrap();
                let expected = json!({"operation_id":commit.operation_id,"logical_call_id":"call-0","outcome":original.reply.outcome,"commit":commit}).to_string();
                let items = request["input"].as_array().unwrap();
                let outputs: Vec<_> = items.iter().enumerate().filter(|(_, item)| item["type"] == "function_call_output").collect();
                assert_eq!(outputs.len(), 1);
                assert_eq!(outputs[0].1["call_id"], "call-0");
                assert_eq!(outputs[0].1["output"], expected);
                let result: Value = serde_json::from_str(outputs[0].1["output"].as_str().unwrap()).unwrap();
                assert_eq!(result["outcome"]["data"]["value"]["revision"], 1);
                assert_eq!(result["outcome"]["data"]["value"]["value"], 1);
                *observed_by_peer.lock().unwrap() = Some(expected);
                Some(final_value())
            }
            _ => panic!("notice created an extra model request"),
        }
    }).await;
    let reader = submit_identified(&api, lab.input(2), "held-notice-reader").await;
    assert_eq!(reader.assigned_agent_id, writer.assigned_agent_id);
    assert_ne!(reader.session_id, writer.session_id);
    let mut worker = lab.worker(&proxy.url, &peer.endpoint, "held-reader-worker");
    tokio::time::timeout(Duration::from_secs(10), held.entered.notified())
        .await
        .expect("MCP result never reached the hold");
    assert_eq!(held.calls.load(Ordering::SeqCst), 1);
    let active = get(&api, &reader.id).await;
    let reader_attempt = active.attempt.as_ref().unwrap().clone();
    assert_eq!(active.status, TaskStatus::Active);
    assert_eq!(reader_attempt.phase, AttemptPhase::Running);
    let journal_path = lab
        .state()
        .join("collaboration")
        .join(process::component(&reader_attempt.id))
        .join("0000000001.json");
    let before = std::fs::read(&journal_path).unwrap();
    let pending: Value = serde_json::from_slice(&before).unwrap();
    assert!(pending["outcome"].is_null());
    assert_eq!(pending["consumed"], false);
    assert_eq!(
        pending["request"]["input"]["call"]["arguments"],
        json!({"key":"note"})
    );
    let changed = api
        .call(Operation::SetMemory {
            operation_id: "held-memory-during-invocation".into(),
            task_id: writer.id.clone(),
            attempt_id: writer_attempt.id.clone(),
            generation: writer_attempt.generation,
            key: "note".into(),
            expected_revision: Some(1),
            value: json!(2),
        })
        .await
        .unwrap();
    assert!(
        matches!(changed.outcome, Outcome::Ok { data: Data::Memory(Some(ref value)) } if value.revision == 2)
    );
    let observation = api
        .call(Operation::ObserveAgentMemory {
            caller: borg_runtime::mcp::Caller {
                task_id: reader.id.clone(),
                attempt_id: reader_attempt.id.clone(),
                generation: reader_attempt.generation,
                worker_id: reader_attempt.worker_id.clone(),
                incarnation: reader_attempt.worker_incarnation.clone(),
            },
            limit: 1,
        })
        .await
        .unwrap();
    assert!(matches!(observation.outcome, Outcome::Refused { code, message }
        if code == "disabled" && message == "automatic shared-memory change notifications are retired"));
    assert!(observation.commit.is_none(), "retired Observe must remain read only");
    // The operation is committed, but the worker's tool future is still held.
    // Give the concurrent controller/worker wake path a finite opportunity to act.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let still = get(&api, &reader.id).await;
    assert_eq!(still.status, TaskStatus::Active);
    assert_eq!(still.attempt.as_ref().unwrap(), &reader_attempt);
    assert!(!still.attempt.as_ref().unwrap().stopped);
    assert_eq!(
        peer.count(),
        1,
        "memory write released another model request while the tool was in flight"
    );
    assert!(!held.released.load(Ordering::SeqCst));
    assert_eq!(
        std::fs::read(&journal_path).unwrap(),
        before,
        "memory write changed the unresolved original tool journal"
    );
    held.release.notify_one();

    let accepted = until(&api, &reader.id, |task| {
        task.status == TaskStatus::Succeeded
            && task.attempt.as_ref().is_some_and(|attempt| attempt.stopped)
    })
    .await;
    assert_eq!(peer.count(), 2);
    assert_eq!(held.calls.load(Ordering::SeqCst), 1);
    assert!(!lab.state().join("memory-notices").exists());
    assert_eq!(accepted.result.as_ref().unwrap().output, Some(json!(7)));
    let journal: Value = serde_json::from_slice(&std::fs::read(&journal_path).unwrap()).unwrap();
    assert_eq!(journal["consumed"], true);
    let invocation = &journal["request"]["input"]["call"];
    let original_output = json!({"operation_id":invocation["operation_id"],"logical_call_id":invocation["logical_call_id"],"outcome":journal["outcome"],"commit":journal["outcome_context"]["commit"]}).to_string();
    assert_eq!(
        observed.lock().unwrap().as_deref(),
        Some(original_output.as_str())
    );
    assert!(
        matches!(memory(&api).await, Outcome::Ok { data: Data::Memory(Some(ref value)) } if value.revision == 2 && value.value == json!(2))
    );
    worker.stop().await;
    let current_writer = get(&api, &writer.id).await;
    assert!(matches!(
        api.call(Operation::CancelTask {
            operation_id: "held-writer-cleanup".into(),
            task_id: writer.id,
            expected_revision: current_writer.revision
        })
        .await
        .unwrap()
        .outcome,
        Outcome::Ok { .. }
    ));
    report(
        &api,
        &writer_attempt,
        "held-writer-stopped",
        Observation::ProcessTreeStopped,
    )
    .await;
    controller.stop().await;
    assert_eq!(lab.usage(&reader_attempt.id), (2, Some(10), Some(4)));
}

// Controlled Responses peer, actual native worker and MCP/controller: the
// permitted update affects a later task while the calling attempt stays pinned.
// No real provider, automatic diagnostic scheduling or deployment is exercised.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_definition_update_consumes_original_receipt_and_changes_only_later_work() {
    definition_update_native_consumer(None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_definition_update_retains_old_worker_policy_with_new_task_snapshots() {
    definition_update_native_consumer(Some(include_bytes!(
        "fixtures/definition-update-old-worker-limits.json"
    )))
    .await;
}

async fn definition_update_native_consumer(old_worker_bytes: Option<&[u8]>) {
    let mut lab = Lab::new(if old_worker_bytes.is_some() {
        "definition-update-mixed-metadata"
    } else {
        "definition-update-consumer"
    });
    let startup_limits = if let Some(bytes) = old_worker_bytes {
        let old = RuntimeLimits::parse(bytes).unwrap();
        assert!(old.definition_updates.is_none());
        assert!(!serde_json::from_slice::<Value>(bytes)
            .unwrap()
            .as_object()
            .unwrap()
            .contains_key("definition_updates"));
        assert_ne!(old.revision, lab.limits.revision);
        lab.worker_limits_file = lab.root.join("old-worker-limits.json");
        std::fs::write(&lab.worker_limits_file, bytes).unwrap();
        old
    } else {
        (*lab.limits).clone()
    };
    let selected_worker_bytes = std::fs::read(&lab.worker_limits_file).unwrap();
    let expected_retained_bytes = serde_json::to_vec(&startup_limits).unwrap();
    let retained_limits_path = lab.state().join("limits.json");
    let old_max = lab.limits.execution.native.max_output_tokens;
    let new_max = old_max.checked_add(1).unwrap();
    let policy = lab.limits.definition_updates.as_ref().unwrap().clone();
    assert!(policy
        .allowed_fields
        .contains(&borg_runtime::limits::DefinitionLimitField::NativeMaxOutputTokens));
    let mut controller = lab.controller("definition-controller-one").await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let original = submit_identified(&api, lab.input(2), "definition-caller").await;
    let original_input = serde_json::to_vec(&original.input).unwrap();
    assert_eq!(
        *original
            .input
            .resolved_limits
            .as_ref()
            .unwrap()
            .global_metadata,
        *lab.limits
    );
    assert!(original
        .input
        .resolved_limits
        .as_ref()
        .unwrap()
        .global_metadata
        .definition_updates
        .is_some());
    let expected_journal_limits = serde_json::to_value(&*lab.limits).unwrap();
    let args = json!({
        "agent_id": original.input.definition.id,
        "expected_revision": original.input.definition.revision,
        "expected_global_revision": lab.limits.revision,
        "expected_policy_revision": policy.revision,
        "changes": {"native.max_output_tokens": new_max}
    });
    let journal_root = lab.state().join("collaboration");
    let caller_task_id = original.id.clone();
    let retained = Arc::new(Mutex::new(None));
    let observed = retained.clone();
    let peer = Peer::start(move |index, request| {
        assert_eq!(
            request["max_output_tokens"],
            if index < 2 { old_max } else { new_max }
        );
        assert!(request["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "collab_definition_update"));
        match index {
            0 => Some(function(index, "collab_definition_update", args.clone())),
            1 => {
                let outputs: Vec<_> = request["input"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|item| item["type"] == "function_call_output")
                    .collect();
                assert_eq!(outputs.len(), 1);
                assert_eq!(outputs[0]["call_id"], "call-0");
                let mut attempts = std::fs::read_dir(&journal_root).unwrap();
                let path = attempts
                    .next()
                    .unwrap()
                    .unwrap()
                    .path()
                    .join("0000000001.json");
                assert!(attempts.next().is_none());
                let bytes = std::fs::read(&path).unwrap();
                let journal: Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(journal["limits"], expected_journal_limits);
                assert_eq!(journal["consumed"], true);
                let call: borg_runtime::mcp::Invocation =
                    serde_json::from_value(journal["request"]["input"]["call"].clone()).unwrap();
                assert_eq!(call.caller.task_id, caller_task_id);
                assert_eq!(
                    call.operation_id,
                    format!("native-collab-{}-1", call.caller.attempt_id)
                );
                assert_eq!(call.logical_call_id, "call-0");
                assert_eq!(call.tool, "collab_definition_update");
                assert_eq!(call.arguments, args);
                let exact = json!({
                    "operation_id": call.operation_id,
                    "logical_call_id": call.logical_call_id,
                    "outcome": journal["outcome"],
                    "commit": journal["outcome_context"]["commit"]
                });
                assert_eq!(outputs[0]["output"], exact.to_string());
                assert_eq!(exact["commit"]["operation_id"], call.operation_id);
                let outcome: Outcome = serde_json::from_value(exact["outcome"].clone()).unwrap();
                let Outcome::Ok {
                    data: Data::DefinitionUpdate(update),
                } = outcome
                else {
                    panic!("definition update did not commit an application outcome")
                };
                assert_eq!(
                    update.previous_revision,
                    args["expected_revision"].as_str().unwrap()
                );
                assert_eq!(update.effective_for, ConfigurationEffect::FutureSubmissions);
                assert_eq!(update.classification, ConfigurationChangeClass::Widening);
                assert_eq!(
                    update.changed_fields,
                    vec![borg_runtime::limits::DefinitionLimitField::NativeMaxOutputTokens]
                );
                assert_ne!(
                    update.selected_definition.revision,
                    update.previous_revision
                );
                *observed.lock().unwrap() = Some((call, exact, update, path, bytes));
                Some(final_value())
            }
            2 => {
                assert!(!request["input"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|item| item["type"] == "function_call_output"));
                Some(final_value())
            }
            _ => panic!("definition change added an unplanned model request"),
        }
    })
    .await;
    let proxy = Proxy::start(
        lab.url.clone(),
        Some("collab_definition_update"),
        false,
        lab.limits.clone(),
    )
    .await;
    let mut worker = lab.worker(&proxy.url, &peer.endpoint, "definition-worker-one");
    let first = until(&api, &original.id, |task| {
        task.status == TaskStatus::Succeeded
            && task.attempt.as_ref().is_some_and(|attempt| attempt.stopped)
    })
    .await;
    assert_eq!(first.result.as_ref().unwrap().output, Some(json!(7)));
    assert!(!first.attempt.as_ref().unwrap().uncertain);
    assert_eq!(serde_json::to_vec(&first.input).unwrap(), original_input);
    assert!(proxy.dropped.load(Ordering::SeqCst));
    assert_eq!(peer.count(), 2);
    let (call, exact, update, journal_path, journal_bytes) =
        retained.lock().unwrap().clone().unwrap();
    assert_eq!(update.global_revision, lab.limits.revision);
    assert_eq!(update.policy, policy);
    let mut expected_definition = original.input.definition.clone();
    expected_definition.revision = update.selected_definition.revision.clone();
    let mut native = original.input.execution_limits().unwrap().native.clone();
    native.max_output_tokens = new_max;
    expected_definition.limit_overrides =
        Some(Box::new(borg_runtime::limits::ExecutionLimitOverrides {
            native: Some(native),
            ..Default::default()
        }));
    assert_eq!(update.selected_definition, expected_definition);
    let retained_worker_bytes = std::fs::read(&retained_limits_path).unwrap();
    assert_eq!(retained_worker_bytes, expected_retained_bytes);
    assert_eq!(
        RuntimeLimits::parse(&retained_worker_bytes).unwrap(),
        startup_limits
    );
    assert_eq!(
        std::fs::read(&lab.worker_limits_file).unwrap(),
        selected_worker_bytes
    );
    worker.stop().await;
    controller.stop().await;

    let mut controller = lab.controller("definition-controller-two").await;
    assert_eq!(get(&api, &original.id).await, first);
    let selected = match api
        .call(Operation::GetAgent {
            agent_id: original.assigned_agent_id.clone(),
        })
        .await
        .unwrap()
        .outcome
    {
        Outcome::Ok {
            data: Data::Agent(agent),
        } => agent.definition,
        other => panic!("selected definition unavailable: {other:?}"),
    };
    assert_eq!(selected, update.selected_definition);
    let lookup = api
        .call(Operation::GetCollaborationCall {
            caller: call.caller.clone(),
            operation_id: call.operation_id.clone(),
        })
        .await
        .unwrap();
    let recovered = borg_runtime::mcp::recovered(&call, lookup).unwrap();
    assert_eq!(
        serde_json::to_value(&recovered.reply.outcome).unwrap(),
        exact["outcome"]
    );
    assert_eq!(
        serde_json::to_value(&recovered.reply.commit).unwrap(),
        exact["commit"]
    );
    let mut next_input = lab.input(1);
    next_input.definition = selected;
    let later = submit_identified(&api, next_input, "definition-later").await;
    let mut expected_limits = original.input.execution_limits().unwrap().clone();
    expected_limits.native.max_output_tokens = new_max;
    assert_eq!(later.input.execution_limits().unwrap(), &expected_limits);
    assert_eq!(
        later
            .input
            .resolved_limits
            .as_ref()
            .unwrap()
            .global_metadata,
        original
            .input
            .resolved_limits
            .as_ref()
            .unwrap()
            .global_metadata
    );
    let assigned = api
        .call(Operation::AssignTask {
            operation_id: "assign-definition-later".into(),
            task_id: later.id.clone(),
            expected_revision: later.revision,
            agent_id: later.assigned_agent_id.clone(),
        })
        .await
        .unwrap();
    assert!(
        matches!(assigned.outcome, Outcome::Ok { data: Data::Task(ref task) } if task.input == later.input)
    );
    assert_eq!(
        std::fs::read(&retained_limits_path).unwrap(),
        retained_worker_bytes
    );
    let mut worker = lab.worker(&proxy.url, &peer.endpoint, "definition-worker-two");
    let completed = until(&api, &later.id, |task| {
        task.status == TaskStatus::Succeeded
            && task.attempt.as_ref().is_some_and(|attempt| attempt.stopped)
    })
    .await;
    assert_eq!(completed.input, later.input);
    assert_eq!(completed.result.as_ref().unwrap().output, Some(json!(7)));
    assert!(!completed.attempt.as_ref().unwrap().uncertain);
    assert_eq!(get(&api, &original.id).await, first);
    assert_eq!(std::fs::read(&journal_path).unwrap(), journal_bytes);
    assert_eq!(
        std::fs::read(&retained_limits_path).unwrap(),
        retained_worker_bytes
    );
    assert_eq!(
        std::fs::read(&lab.worker_limits_file).unwrap(),
        selected_worker_bytes
    );
    assert_eq!(peer.count(), 3);
    worker.stop().await;
    controller.stop().await;
    assert_eq!(
        lab.usage(&first.attempt.as_ref().unwrap().id),
        (2, Some(10), Some(4))
    );
    assert_eq!(
        lab.usage(&completed.attempt.as_ref().unwrap().id),
        (1, Some(5), Some(2))
    );
    let db = rusqlite::Connection::open_with_flags(
        lab.root.join("tasks.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM definitions WHERE id=?1",
            [&original.assigned_agent_id],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        2
    );
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM receipts WHERE id=?1",
            [&call.operation_id],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
}

#[path = "native/workflow_join.rs"]
mod workflow_join;

#[path = "native/charter_worker.rs"]
mod charter_worker;

#[path = "native/http_route_stack.rs"]
mod http_route_stack;

#[path = "native/task_reassignment.rs"]
mod task_reassignment;

#[path = "native/read_pagination.rs"]
mod read_pagination;
