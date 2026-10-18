//! Actual native invoke, ApiClient and MCP Client against a bounded local peer.
//! The peer supplies typed fixture receipts; it is not a durability/controller test.
use super::*;
use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::Mutex,
    thread::{self, JoinHandle},
    time::Instant,
};

#[derive(Clone, Copy, Default)]
enum LookupMode { #[default] Normal, Unavailable, UnavailableAfterSend, Hold, HoldAfterSend, Invalid, InvalidAfterSend, ForeignInvocation, Quiescence }
#[derive(Clone, Copy, Default)]
enum ReplyMode { #[default] Valid, ForeignCommit, Quiescence, Malformed }
#[derive(Clone, Copy, Default)]
enum CallHold { #[default] None, BeforeCommit, AfterCommit }
#[derive(Default)]
struct Script {
    failures: usize,
    lost_reply: bool,
    refused: bool,
    uncommitted: bool,
    lookup: LookupMode,
    reply: ReplyMode,
    stop_after_commit: bool,
    hold_first_call: CallHold,
    client_timeout_ms: Option<u64>,
}
#[derive(Clone, Default)]
struct Seen {
    calls: Vec<mcp::Invocation>,
    events: Vec<&'static str>,
    lookups: usize,
    owner_reads: usize,
    receipt: Option<mcp::CallResult>,
    call_params: Vec<serde_json::Value>,
    lookup_receipts: Vec<bool>,
    held_calls: usize,
    commits: usize,
}
struct Peer {
    url: String,
    seen: Arc<Mutex<Seen>>,
    stopped: Arc<AtomicBool>,
    sockets: Arc<Mutex<Vec<TcpStream>>>,
    thread: Option<JoinHandle<()>>,
}
fn read_wire(socket: &mut TcpStream, bound: usize) -> Option<(String, serde_json::Value)> {
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        if socket.read(&mut byte).ok()? == 0 { return None; }
        header.push(byte[0]);
        assert!(header.len() <= 32_768, "fixture header bound");
    }
    let header = String::from_utf8(header).unwrap();
    let route = header.lines().next().unwrap().split_whitespace().nth(1).unwrap().to_owned();
    let length = header.lines().find_map(|line| line.split_once(':')
        .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .map(|(_, value)| value.trim().parse::<usize>().unwrap())).unwrap();
    assert!(length <= bound);
    let mut body = vec![0; length];
    socket.read_exact(&mut body).ok()?;
    Some((route, serde_json::from_slice(&body).unwrap()))
}
fn rpc_reply(id: serde_json::Value, result: mcp::CallResult) -> serde_json::Value {
    json!({"jsonrpc":"2.0","id":id,"result":{
        "content":[{"type":"text","text":serde_json::to_string(&result).unwrap()}],
        "isError":!matches!(result.reply.outcome,Outcome::Ok{..})}})
}
fn send_wire(socket: &mut TcpStream, value: serde_json::Value) {
    let bytes = serde_json::to_vec(&value).unwrap();
    let header = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n", bytes.len());
    let _ = socket.write_all(header.as_bytes()).and_then(|_| socket.write_all(&bytes));
}
fn fixture_outcome(task: &Task, call: &mcp::Invocation) -> Outcome {
    let mut task = task.clone();
    if call.tool == "collab_task_delegate" {
        task.id = "fixture-child".into();
        task.parent_task_id = Some(call.caller.task_id.clone());
    } else if call.tool == "collab_task_assign" {
        let mut target=task.input.definition.clone();target.id="assigned-agent".into();
        task.status=TaskStatus::Blocked;
        task.pending_assignment=Some(PendingAssignment{operation_id:call.operation_id.clone(),from_session_id:task.session_id.clone(),session:Session{id:"reserved-assignment".into(),task_id:task.id.clone(),agent_id:target.id.clone(),definition:target}});
    } else if call.tool == "collab_task_wait" {
        return Outcome::Ok {data:Data::WorkMutation(serde_json::from_value(json!({
            "view":{"task":task,"coordination_revision":1,
                "responsible_owner":{"kind":"agent","agent_id":"read-agent"},
                "state":{"kind":"waiting","prior":{"status":"active","assigned_agent_id":"read-agent"}},
                "open_waits":1,"coordination_blocked":true,"waits":{"items":[],"next_cursor":null}},
            "changed_wait":{"task_id":call.caller.task_id,"wait_id":"fixture-wait"},
            "product_revision":7
        })).unwrap())};
    }
    Outcome::Ok {data:Data::Task(Box::new(task))}
}
impl Peer {
    fn new(task: Task, call: mcp::Invocation, journal_path: PathBuf,
        limits: Arc<RuntimeLimits>, script: Script, cancel: watch::Sender<bool>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Seen::default()));
        let stopped = Arc::new(AtomicBool::new(false));
        let sockets = Arc::new(Mutex::new(Vec::new()));
        let (observed, stop, saved) = (seen.clone(), stopped.clone(), sockets.clone());
        let thread = thread::spawn(move || {
            let end = Instant::now() + Duration::from_secs(30);
            while !stop.load(Ordering::SeqCst) && Instant::now() < end {
                let (mut socket, _) = match listener.accept() {
                    Ok(value) => value,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1)); continue;
                    },
                    Err(error) => panic!("fixture listener: {error}"),
                };
                socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
                socket.set_write_timeout(Some(Duration::from_secs(1))).unwrap();
                saved.lock().unwrap().push(socket.try_clone().unwrap());
                let Some((route, body)) = read_wire(&mut socket, limits.transport.body_bytes) else { continue };
                if route == "/api/operations" {
                    let request: Request = serde_json::from_value(body).unwrap();
                    assert!(matches!(&request.command, Operation::GetTask {task_id} if task_id == &task.id),
                        "tool recovery must not issue Usage, a model request, or a new operation");
                    let mut seen = observed.lock().unwrap();
                    seen.owner_reads += 1; seen.events.push("current"); drop(seen);
                    send_wire(&mut socket, serde_json::to_value(Reply { request_id:request.request_id,
                        commit:None, observed_controller_revision:Some(8),
                        outcome:Outcome::Ok {data:Data::Task(Box::new(task.clone()))} }).unwrap());
                    continue;
                }
                assert_eq!(route, "/mcp", "no provider route is used");
                let id = body["id"].clone();
                match body["method"].as_str().unwrap() {
                    "borg/calls/get" => {
                        assert_eq!(body["params"]["call_id"], call.operation_id);
                        let (calls, lookups, receipt) = {
                            let mut seen = observed.lock().unwrap(); seen.lookups += 1;
                            let found = seen.receipt.is_some();
                            seen.lookup_receipts.push(found);
                            seen.events.push("lookup"); (seen.calls.len(), seen.lookups, seen.receipt.clone())
                        };
                        if matches!(script.lookup, LookupMode::Hold)
                            || calls > 0 && matches!(script.lookup, LookupMode::HoldAfterSend) {
                            while !stop.load(Ordering::SeqCst) && Instant::now() < end {
                                thread::sleep(Duration::from_millis(1));
                            }
                            continue;
                        }
                        if matches!(script.lookup, LookupMode::Unavailable)
                            || calls > 0 && matches!(script.lookup, LookupMode::UnavailableAfterSend) {
                            let _ = socket.shutdown(Shutdown::Both); continue;
                        }
                        if matches!(script.lookup, LookupMode::Invalid) && lookups == 1
                            || matches!(script.lookup, LookupMode::InvalidAfterSend) && calls > 0 && lookups == 2 {
                            send_wire(&mut socket, json!({"jsonrpc":"2.0","id":id,"result":{
                                "content":[{"type":"text","text":"{}"}],"isError":false}}));
                            continue;
                        }
                        let mut reply = match receipt {
                            Some(original) => Reply { request_id:"lookup".into(),
                                commit:original.reply.commit, observed_controller_revision:Some(8),
                                outcome:Outcome::Ok {data:Data::Operation(OperationRecord {
                                    id:call.operation_id.clone(), task_id:call.caller.task_id.clone(),
                                    attempt_id:call.caller.attempt_id.clone(), generation:call.caller.generation,
                                    kind:"application_mutation".into(), intended:serde_json::to_value(call.operation()).unwrap(),
                                    state:"settled".into(), outcome:Some(serde_json::to_value(original.reply.outcome).unwrap()),
                                })} },
                            None => Reply::refused("lookup", "not_found", "fixture has no committed outcome"),
                        };
                        reply.observed_controller_revision = Some(8);
                        if matches!(script.lookup, LookupMode::ForeignInvocation) {
                            if let Outcome::Ok {data:Data::Operation(record)} = &mut reply.outcome {
                                let mut foreign = call.clone();
                                foreign.logical_call_id = "foreign-logical-call".into();
                                record.intended = serde_json::to_value(foreign.operation()).unwrap();
                            }
                        }
                        send_wire(&mut socket, rpc_reply(id, mcp::CallResult {reply,
                            requires_quiescence:matches!(script.lookup, LookupMode::Quiescence)}));
                    },
                    "tools/call" => {
                        assert_eq!(body["params"]["name"], call.tool);
                        assert_eq!(body["params"]["arguments"], call.arguments);
                        assert_eq!(body["params"]["_meta"]["borg"]["operation_id"], call.operation_id);
                        assert_eq!(body["params"]["_meta"]["borg"]["logical_call_id"], call.logical_call_id);
                        let journal = mutations::read(&journal_path, &limits).unwrap().unwrap();
                        assert_eq!(serde_json::to_value(&journal.request).unwrap(), serde_json::to_value(call.operation()).unwrap(), "exact durable intent before every send");
                        assert!(journal.outcome.is_none());
                        let count = {
                            let mut seen = observed.lock().unwrap(); seen.calls.push(call.clone());
                            seen.call_params.push(body["params"].clone());
                            seen.events.push("call"); seen.calls.len()
                        };
                        if count <= script.failures { let _ = socket.shutdown(Shutdown::Both); continue; }
                        if count == 1 && matches!(script.hold_first_call, CallHold::BeforeCommit) {
                            // The saved clone keeps this socket open without any
                            // response or shutdown until Peer::stop. Continue
                            // accepting so the actual client timeout, not a
                            // blocked fixture lookup, drives recovery.
                            observed.lock().unwrap().held_calls += 1;
                            continue;
                        }
                        let mut reply = if script.refused || script.uncommitted {
                            Reply::refused("original", "capacity", "fixture original refusal")
                        } else { Reply {request_id:"original".into(), commit:None,
                            observed_controller_revision:None,
                            outcome:fixture_outcome(&task, &call)} };
                        if !script.uncommitted {
                            reply.commit = Some(MutationCommit {operation_id:call.operation_id.clone(),controller_revision:7});
                            reply.observed_controller_revision = Some(7);
                        }
                        let mut result = mcp::result(&call, reply).unwrap();
                        // The first complete reply is invalid; later sends would
                        // succeed. No original receipt exists for a lookup to find.
                        if count == 1 && !matches!(script.reply, ReplyMode::Valid) {
                            match script.reply {
                                ReplyMode::ForeignCommit => result.reply.commit.as_mut().unwrap().operation_id = "foreign-operation".into(),
                                ReplyMode::Quiescence => result.requires_quiescence = true,
                                ReplyMode::Malformed => {
                                    send_wire(&mut socket, json!({"jsonrpc":"2.0","id":id,"result":{
                                        "content":[{"type":"text","text":"{}"}],"isError":false}}));
                                    continue;
                                },
                                ReplyMode::Valid => unreachable!(),
                            }
                            send_wire(&mut socket, rpc_reply(id, result));
                            continue;
                        }
                        if !script.uncommitted {
                            let mut seen = observed.lock().unwrap();
                            seen.receipt = Some(result.clone()); seen.commits += 1;
                        }
                        if script.stop_after_commit { cancel.send(true).unwrap(); }
                        if script.lost_reply { let _ = socket.shutdown(Shutdown::Both); continue; }
                        if count == 1 && matches!(script.hold_first_call, CallHold::AfterCommit) {
                            // Original result exists, but the open socket never
                            // receives headers or a body. Only lookup can settle it.
                            observed.lock().unwrap().held_calls += 1;
                            continue;
                        }
                        send_wire(&mut socket, rpc_reply(id, result));
                    },
                    other => panic!("unexpected fixture MCP method: {other}"),
                }
            }
        });
        Self {url, seen, stopped, sockets, thread:Some(thread)}
    }
    fn stop(&mut self) {
        self.stopped.store(true, Ordering::SeqCst);
        for socket in self.sockets.lock().unwrap().iter() { let _ = socket.shutdown(Shutdown::Both); }
        if let Some(thread) = self.thread.take() { thread.join().unwrap(); }
    }
}
impl Drop for Peer { fn drop(&mut self) { self.stop(); } }
struct Fixture {
    peer: Peer,
    root: PathBuf,
    input: TaskInput,
    reporter: Reporter,
    control: Control,
    cancel: watch::Sender<bool>,
    call: mcp::Invocation,
    task: Task,
}
impl Fixture {
    fn new(mutation: bool, script: Script, lifetime_ms: u64, backoff_ms: u64) -> Self {
        Self::for_tool(if mutation {"collab_memory_set"} else {"collab_task_get"}, script, lifetime_ms, backoff_ms)
    }
    fn for_tool(tool: &str, script: Script, lifetime_ms: u64, backoff_ms: u64) -> Self {
        let mut metadata = (*crate::worker::test_limits()).clone();
        metadata.execution.collaboration.lookup_backoff_ms = backoff_ms;
        if let Some(timeout_ms) = script.client_timeout_ms {
            // Explicit fixture policy, captured before task acceptance. Existing
            // tests retain their original policy and no production default changes.
            metadata.transport.client_timeout_ms = timeout_ms;
            metadata.transport.client_connect_timeout_ms = timeout_ms;
        }
        let limits = Arc::new(metadata);
        let mut input: TaskInput = serde_json::from_value(json!({
            "instruction":"Controlled idempotent read recovery fixture",
            "definition":{"id":"read-agent","revision":"one","executor":"native",
                "inference":{"model":"gpt-6-astra","reasoning_effort":"max"},
                "execution_profile":{"kind":"collaboration_v1"}},
            "repository":null,"acceptance_commands":[],"output_schema":{"type":"integer"},
            "limits":limits.task_defaults.at(process::now_ms()).unwrap(),"dependencies":[]
        })).unwrap();
        input.limits.deadline_unix_ms = process::now_ms() + lifetime_ms;
        if tool == "collab_task_wait" {
            input.definition.execution_profile = Some(serde_json::from_value(json!({"kind":"collaboration_v2"})).unwrap());
        }
        input.resolved_limits = Some(limits.resolve_task(&input).unwrap());
        let root = std::env::temp_dir().join(mutations::fresh_id("native-read-recovery").unwrap());
        std::fs::create_dir(&root).unwrap();
        let attempt = Attempt {id:"read-attempt".into(),session_id:"read-session".into(),
            task_id:"read-task".into(),generation:1,worker_id:"read-worker".into(),
            worker_incarnation:"one".into(),phase:AttemptPhase::Running,
            deadline_unix_ms:input.limits.deadline_unix_ms,stopped:false,uncertain:false};
        let call = mcp::Invocation {operation_id:contract::call_operation_id(&attempt.id,1,1),
            logical_call_id:"read-logical".into(),caller:caller(&attempt),
            tool:tool.into(),
            arguments:match tool {
                "collab_memory_set" => json!({"key":"fixture","expected_revision":null,"value":"same"}),
                "collab_task_delegate" => json!({"instruction":"controlled child"}),
                "collab_task_assign" => json!({"task_id":attempt.task_id,"expected_revision":1,"agent_id":"assigned-agent"}),
                "collab_task_wait" => json!({"task_id":attempt.task_id,"expected_revision":1,
                    "resolver":{"kind":"question","question_id":"fixture-question"}}),
                _ => json!({"task_id":attempt.task_id}),
            }};
        let task: Task = serde_json::from_value(json!({"id":attempt.task_id,"session_id":attempt.session_id,
            "input":input,"assigned_agent_id":"read-agent","status":"active","revision":1,
            "attempt":attempt,"selected_checkpoint":null,"result":null,"blocked_reason":null,
            "progress":"","recent_output":"","output_truncated":false,"parent_task_id":null,
            "waiting_child_id":null,"completed_child":null})).unwrap();
        let (cancel, rx) = watch::channel(false);
        let peer = Peer::new(task.clone(), call.clone(), path(&root,&attempt,1,1), limits.clone(), script, cancel.clone());
        let config = WorkerConfig {limits:limits.clone(),resource_profile:None,controller_url:peer.url.clone(),
            id:attempt.worker_id.clone(),incarnation:attempt.worker_incarnation.clone(),workspace_root:root.clone(),
            codex_path:"/does-not-exist".into(),coding_capacity:0,native_capacity:1,
            native_endpoint:peer.url.clone(),validation_executable:std::env::current_exe().unwrap()};
        let reporter = Reporter {resource:None,client:ApiClient::new(&peer.url,limits.clone()).unwrap(),config,
            attempt,outbox:root.join("outbox"),sequence:Arc::new(AtomicU64::new(1)),
            executor_seen:Arc::new(AtomicBool::new(false)),provider_uncertain:Arc::new(AtomicBool::new(false)),
            uncertainty_retained:Arc::new(AtomicBool::new(false)),output_limit:input.limits.output_bytes,
            result_limit:input.limits.result_bytes,coding_limits:input.execution_limits().unwrap().coding.clone(),
            outbox_lock:Arc::new(tokio::sync::Mutex::new(()))};
        let worker_limits = Arc::new(limits.worker.clone());
        let control = Control {deadline_ms:input.limits.deadline_unix_ms,cancel:rx,runtime_limits:limits,
            validation_runtime:Arc::new(native::ValidationRuntime::new(worker_limits.clone()).unwrap()),worker_limits};
        Self {peer,root,input,reporter,control,cancel,call,task}
    }
    async fn invoke(&self) -> Result<mutations::Journal,String> {
        let client = mcp::Client::new_for_profile(&self.peer.url,self.call.caller.clone(),
            (*self.control.runtime_limits).clone(),self.input.definition.execution_profile.clone().unwrap())?;
        invoke(&client,self.call.clone(),&self.root,&self.reporter,&self.input,&self.control,1,1).await
    }
    fn retained(&self) -> mutations::Journal {
        mutations::read(&path(&self.root,&self.reporter.attempt,1,1),&self.control.runtime_limits).unwrap().unwrap()
    }
    fn inherit(&self) {
        let p = path(&self.root,&self.reporter.attempt,1,1);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        mutations::create(&p,self.call.operation(),0,&self.control.runtime_limits).unwrap();
    }
    fn original(&self) -> mcp::CallResult {
        mcp::result(&self.call, Reply {request_id:"original".into(),
            commit:Some(MutationCommit {operation_id:self.call.operation_id.clone(),controller_revision:7}),
            observed_controller_revision:Some(7),outcome:fixture_outcome(&self.task,&self.call)}).unwrap()
    }
}
impl Drop for Fixture { fn drop(&mut self) { self.peer.stop(); let _=std::fs::remove_dir_all(&self.root); } }

#[tokio::test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A platform-owned idempotent tool call that is unreachable or timed out MUST be retried within its accepted budget and deadline.
async fn read_transport_loss_succeeds_on_fourth_exact_dispatch_without_model_or_usage() {
    let fixture = Fixture::new(false,Script {failures:3,..Script::default()},10_000,2);
    let journal = fixture.invoke().await.unwrap();
    let seen = fixture.peer.seen.lock().unwrap().clone();
    assert_eq!(seen.calls.len(),4);
    assert!(seen.calls.iter().all(|call| call == &fixture.call));
    assert_eq!(seen.owner_reads,4);
    for position in seen.events.iter().enumerate().filter_map(|(i,e)| (*e=="call").then_some(i)).skip(1) {
        assert_eq!(&seen.events[position-3..position],&["lookup","lookup","current"]);
    }
    assert_eq!(serde_json::to_value(&journal.request).unwrap(),serde_json::to_value(fixture.call.operation()).unwrap());
    assert_eq!(journal.outcome,seen.receipt.map(|value|value.reply.outcome));
    assert_eq!(fixture.reporter.sequence.load(Ordering::SeqCst),1);
    assert!(!fixture.reporter.provider_uncertain.load(Ordering::SeqCst));
}

#[tokio::test]
async fn original_read_receipt_wins_after_reply_loss_and_known_refusal_never_retries() {
    for script in [Script {lost_reply:true,..Script::default()},Script {refused:true,..Script::default()}] {
        let fixture = Fixture::new(false,script,10_000,2);
        let journal = fixture.invoke().await.unwrap();
        let seen = fixture.peer.seen.lock().unwrap().clone();
        assert_eq!(seen.calls.len(),1);
        let receipt = seen.receipt.unwrap();
        assert_eq!(journal.outcome,Some(receipt.reply.outcome));
        assert_eq!(journal.outcome_context.unwrap().commit,receipt.reply.commit.unwrap());
    }
}

#[tokio::test]
async fn unresolved_mutation_and_inherited_mutation_never_redispatch_after_absence() {
    for inherited in [false,true] {
        let fixture = Fixture::new(true,Script {failures:1,..Script::default()},10_000,2);
        if inherited { fixture.inherit(); }
        assert!(fixture.invoke().await.is_err());
        assert_eq!(fixture.peer.seen.lock().unwrap().calls.len(),usize::from(!inherited));
        assert!(fixture.retained().outcome.is_none());
        assert!(fixture.reporter.provider_uncertain.load(Ordering::SeqCst));
    }
}

#[tokio::test]
async fn uncommitted_read_result_is_not_redispatch_authority() {
    let fixture = Fixture::new(false,Script {uncommitted:true,..Script::default()},10_000,2);
    assert!(fixture.invoke().await.is_err());
    assert_eq!(fixture.peer.seen.lock().unwrap().calls.len(),1);
    assert!(fixture.retained().outcome.is_none());
    assert!(fixture.reporter.provider_uncertain.load(Ordering::SeqCst));
}

#[tokio::test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A platform-owned idempotent tool call that is unreachable or timed out MUST be retried within its accepted budget and deadline.
async fn unavailable_lookup_cannot_authorize_send_and_deadline_preserves_prior_uncertainty() {
    for after_send in [false,true] {
        let script = Script {failures:1,lookup:if after_send {LookupMode::UnavailableAfterSend}
            else {LookupMode::Unavailable},..Script::default()};
        let fixture = Fixture::new(false,script,500,2);
        assert!(fixture.invoke().await.is_err());
        assert_eq!(fixture.peer.seen.lock().unwrap().calls.len(),usize::from(after_send));
        assert!(fixture.retained().outcome.is_none());
        assert_eq!(fixture.reporter.provider_uncertain.load(Ordering::SeqCst),after_send);
    }
}

#[tokio::test]
async fn cancellation_interrupts_initial_and_post_error_lookup_without_clearing_pending_intent() {
    for after_send in [false,true] {
        let script = Script {failures:1,lookup:if after_send {LookupMode::HoldAfterSend}
            else {LookupMode::Hold},..Script::default()};
        let fixture = Fixture::new(false,script,10_000,2);
        let observed = fixture.peer.seen.clone();
        let cancel = fixture.cancel.clone();
        let trigger = async move {
            tokio::time::timeout(Duration::from_secs(2),async {
                loop {
                    let ready = {let seen=observed.lock().unwrap();seen.lookups>=if after_send {2}else{1}};
                    if ready {break;}
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }).await.expect("actual lookup reached");
            cancel.send(true).unwrap();
        };
        let (result,()) = tokio::join!(fixture.invoke(),trigger);
        assert!(result.is_err());
        assert_eq!(fixture.peer.seen.lock().unwrap().calls.len(),usize::from(after_send));
        assert!(fixture.retained().outcome.is_none());
        assert_eq!(fixture.reporter.provider_uncertain.load(Ordering::SeqCst),after_send);
    }
}

#[tokio::test]
async fn cancellation_during_read_backoff_preserves_original_pending_journal() {
    let fixture = Fixture::new(false,Script {failures:1,..Script::default()},10_000,1_000);
    let observed = fixture.peer.seen.clone();
    let cancel = fixture.cancel.clone();
    let trigger = async move {
        tokio::time::timeout(Duration::from_secs(2),async {
            loop {
                if observed.lock().unwrap().lookups >= 2 { break; }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }).await.expect("post-error lookup reached");
        cancel.send(true).unwrap();
    };
    let (result,()) = tokio::join!(fixture.invoke(),trigger);
    assert!(result.is_err());
    assert_eq!(fixture.peer.seen.lock().unwrap().calls.len(),1);
    assert!(fixture.retained().outcome.is_none());
    assert_eq!(serde_json::to_value(fixture.retained().request).unwrap(),serde_json::to_value(fixture.call.operation()).unwrap());
    assert!(fixture.reporter.provider_uncertain.load(Ordering::SeqCst));
}

// Complete invalid evidence is a terminal contract error, even for a read whose
// later original lookup would be absent and whose second send would succeed.
#[tokio::test]
async fn complete_foreign_quiescence_or_malformed_read_reply_never_reenters_recovery() {
    for reply in [ReplyMode::ForeignCommit, ReplyMode::Quiescence, ReplyMode::Malformed] {
        let fixture = Fixture::new(false, Script {reply,..Script::default()}, 10_000, 2);
        let error = fixture.invoke().await.err().unwrap();
        assert!(error.starts_with("collaboration call outcome uncertain:"));
        let seen = fixture.peer.seen.lock().unwrap().clone();
        assert_eq!(seen.calls.len(), 1);
        assert_eq!(seen.lookups, 1, "only the initial original lookup; contract error exits immediately");
        assert_eq!(seen.owner_reads, 1);
        assert!(seen.receipt.is_none());
        assert!(fixture.retained().outcome.is_none());
        assert!(fixture.reporter.provider_uncertain.load(Ordering::SeqCst));
        assert_eq!(fixture.reporter.sequence.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn complete_malformed_original_lookup_is_not_absence_or_transport_retry() {
    for after_send in [false,true] {
        let script = Script {failures:usize::from(after_send),
            lookup:if after_send {LookupMode::InvalidAfterSend} else {LookupMode::Invalid},
            ..Script::default()};
        let fixture = Fixture::new(false, script, 10_000, 2);
        assert!(fixture.invoke().await.is_err());
        let seen = fixture.peer.seen.lock().unwrap().clone();
        assert_eq!(seen.calls.len(), usize::from(after_send));
        assert_eq!(seen.lookups, 1 + usize::from(after_send));
        assert!(seen.receipt.is_none());
        assert!(fixture.retained().outcome.is_none());
        assert_eq!(fixture.reporter.provider_uncertain.load(Ordering::SeqCst), after_send);
    }
}

// This controlled peer establishes a receipt-before-stop ordering at the real
// invoke/MCP boundary. Controller durability and the physical handoff remain the
// separate actual recovery integration test's responsibility.
#[tokio::test]
async fn stopped_delegation_and_self_wait_reconcile_only_the_original_committed_receipt() {
    for tool in ["collab_task_delegate", "collab_task_wait", "collab_task_assign"] {
        for inherited in [false, true] {
            let fixture = Fixture::for_tool(tool, Script {
                stop_after_commit:true, lost_reply:true, ..Script::default()
            }, 10_000, 2);
            let original = fixture.original();
            if inherited {
                fixture.inherit();
                fixture.peer.seen.lock().unwrap().receipt = Some(original.clone());
                fixture.cancel.send(true).unwrap();
            }
            let journal = fixture.invoke().await.unwrap();
            assert!(fixture.control.check().is_err(), "dispatch authority has ended");
            assert!(requires_quiescence(&journal));
            assert_eq!(journal.outcome, Some(original.reply.outcome));
            assert_eq!(journal.outcome_context.as_ref().unwrap().commit, original.reply.commit.unwrap());
            assert_eq!(serde_json::to_value(&journal).unwrap(), serde_json::to_value(fixture.retained()).unwrap());
            assert!(!journal.consumed, "physical handoff has not been reported by invoke");
            let seen = fixture.peer.seen.lock().unwrap().clone();
            assert_eq!(seen.calls.len(), usize::from(!inherited));
            assert_eq!(seen.owner_reads, usize::from(!inherited));
            assert_eq!(seen.lookups, if inherited {1} else {2});
            assert_eq!(seen.events.last(), Some(&"lookup"));
            assert!(!fixture.reporter.provider_uncertain.load(Ordering::SeqCst));
            assert_eq!(fixture.reporter.sequence.load(Ordering::SeqCst), 1);
            assert!(!fixture.reporter.outbox.exists(), "no model/usage/report from receipt reconciliation");
        }
    }
}

#[tokio::test]
async fn stopped_original_lookup_absence_unavailable_or_invalid_evidence_remains_uncertain() {
    for tool in ["collab_task_delegate","collab_task_assign"] {
    for mode in ["absent", "unavailable", "foreign_commit", "foreign_invocation", "lookup_quiescence", "wrong_child", "refused"] {
        let lookup = match mode {
            "unavailable" => LookupMode::Unavailable,
            "foreign_invocation" => LookupMode::ForeignInvocation,
            "lookup_quiescence" => LookupMode::Quiescence,
            _ => LookupMode::Normal,
        };
        let fixture = Fixture::for_tool(tool, Script {lookup,..Script::default()}, 10_000, 2);
        fixture.inherit();
        let mut original = fixture.original();
        match mode {
            "foreign_commit" => original.reply.commit.as_mut().unwrap().operation_id = "foreign-original".into(),
            "wrong_child" => if let Outcome::Ok {data:Data::Task(child)} = &mut original.reply.outcome {
                if tool=="collab_task_assign" {child.pending_assignment.as_mut().unwrap().from_session_id="foreign-session".into();}
                else {child.parent_task_id = Some("different-parent".into());}
            },
            "refused" => original.reply.outcome = Outcome::Refused {code:"capacity".into(),message:"fixture refusal".into()},
            _ => {},
        }
        if mode != "absent" { fixture.peer.seen.lock().unwrap().receipt = Some(original); }
        fixture.cancel.send(true).unwrap();
        let error = fixture.invoke().await.err().unwrap();
        assert!(error.starts_with("collaboration call outcome uncertain:"), "{mode}: {error}");
        let seen = fixture.peer.seen.lock().unwrap().clone();
        assert_eq!(seen.lookups, 1, "one bounded original lookup only: {mode}");
        assert!(seen.calls.is_empty());
        assert_eq!(seen.owner_reads, 0);
        let journal = fixture.retained();
        assert!(journal.outcome.is_none() && journal.outcome_context.is_none() && !journal.consumed);
        assert!(fixture.reporter.provider_uncertain.load(Ordering::SeqCst));
        assert_eq!(fixture.reporter.sequence.load(Ordering::SeqCst), 1);
    }
}
}

#[tokio::test]
async fn stop_without_dispatched_quiescent_call_never_grants_lookup_or_send() {
    for (tool, inherited) in [("collab_task_delegate", false), ("collab_task_get", true), ("collab_memory_set", true)] {
        let fixture = Fixture::for_tool(tool, Script::default(), 10_000, 2);
        if inherited { fixture.inherit(); }
        fixture.cancel.send(true).unwrap();
        assert!(fixture.invoke().await.is_err());
        let seen = fixture.peer.seen.lock().unwrap().clone();
        assert!(seen.calls.is_empty());
        assert_eq!(seen.lookups, 0);
        assert_eq!(seen.owner_reads, 0);
        assert!(fixture.retained().outcome.is_none());
        assert_eq!(fixture.reporter.provider_uncertain.load(Ordering::SeqCst), inherited);
    }
}

#[tokio::test]
async fn stopped_original_receipt_must_be_durable_before_quiescence_can_return() {
    use process::storage_tests::{inject, Point};
    let fixture = Fixture::for_tool("collab_task_delegate", Script::default(), 10_000, 2);
    fixture.inherit();
    fixture.peer.seen.lock().unwrap().receipt = Some(fixture.original());
    fixture.cancel.send(true).unwrap();
    let journal_path = path(&fixture.root, &fixture.reporter.attempt, 1, 1);
    let before = std::fs::read(&journal_path).unwrap();
    let fault = inject(&journal_path, Point::FileSync, 28, 0);
    let error = fixture.invoke().await.err().unwrap();
    assert!(error.starts_with("collaboration receipt retention uncertain:"));
    assert_eq!(fault.hits(), 1);
    assert_eq!(std::fs::read(&journal_path).unwrap(), before);
    assert!(fixture.reporter.provider_uncertain.load(Ordering::SeqCst));
    assert_eq!(fixture.peer.seen.lock().unwrap().lookups, 1);
    assert!(fixture.peer.seen.lock().unwrap().calls.is_empty());
    assert!(!fixture.reporter.outbox.exists());
}

#[tokio::test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A platform-owned idempotent tool call that is unreachable or timed out MUST be retried within its accepted budget and deadline.
async fn read_response_timeout_retries_same_invocation_after_original_absence_without_model_or_usage() {
    let timeout_ms = 250;
    let fixture = Fixture::new(false, Script {
        hold_first_call: CallHold::BeforeCommit,
        client_timeout_ms: Some(timeout_ms),
        ..Script::default()
    }, 10_000, 2);
    let original_input = serde_json::to_value(&fixture.input).unwrap();
    let deadline = fixture.control.deadline_ms;
    let started = Instant::now();
    let journal = fixture.invoke().await.unwrap();
    assert!(started.elapsed() >= Duration::from_millis(timeout_ms));
    assert!(process::now_ms() < deadline, "recovery finishes within the original deadline");
    assert!(!*fixture.cancel.borrow());
    let seen = fixture.peer.seen.lock().unwrap().clone();
    assert_eq!(seen.held_calls, 1, "first response remains open without headers or shutdown");
    assert!(!fixture.peer.stopped.load(Ordering::SeqCst));
    assert_eq!(seen.calls, vec![fixture.call.clone(), fixture.call.clone()]);
    assert_eq!(seen.call_params[0], seen.call_params[1], "exact original wire arguments and logical/operation IDs");
    assert_eq!(seen.lookups, 3);
    assert_eq!(seen.lookup_receipts, vec![false, false, false]);
    assert_eq!(seen.events, vec!["lookup", "current", "call", "lookup", "lookup", "current", "call"]);
    assert_eq!(seen.owner_reads, 2);
    assert_eq!(seen.commits, 1, "the timed-out first dispatch has no recorded effect");
    let receipt = seen.receipt.unwrap();
    assert_eq!(journal.outcome, Some(receipt.reply.outcome));
    assert_eq!(journal.outcome_context.as_ref().unwrap().commit, receipt.reply.commit.unwrap());
    assert_eq!(serde_json::to_value(&journal.request).unwrap(), serde_json::to_value(fixture.call.operation()).unwrap());
    assert_eq!(serde_json::to_value(&fixture.retained()).unwrap(), serde_json::to_value(&journal).unwrap());
    assert_eq!(serde_json::to_value(&fixture.input).unwrap(), original_input);
    assert_eq!(fixture.control.deadline_ms, deadline);
    assert_eq!(fixture.control.runtime_limits.transport.client_timeout_ms, timeout_ms);
    assert_eq!(fixture.input.execution_limits().unwrap().collaboration.lookup_backoff_ms, 2);
    assert_eq!(fixture.reporter.sequence.load(Ordering::SeqCst), 1);
    assert!(!fixture.reporter.outbox.exists(), "no Usage or model-request report is created");
    assert!(!fixture.reporter.provider_uncertain.load(Ordering::SeqCst));
}

#[tokio::test]
async fn tool_response_timeout_uses_original_receipt_and_never_blindly_repeats_mutation() {
    for (mutation, committed) in [(false, true), (true, true), (true, false)] {
        let timeout_ms = 250;
        let fixture = Fixture::new(mutation, Script {
            hold_first_call: if committed { CallHold::AfterCommit } else { CallHold::BeforeCommit },
            client_timeout_ms: Some(timeout_ms),
            ..Script::default()
        }, 10_000, 2);
        let original_input = serde_json::to_value(&fixture.input).unwrap();
        let deadline = fixture.control.deadline_ms;
        let started = Instant::now();
        let result = fixture.invoke().await;
        assert!(started.elapsed() >= Duration::from_millis(timeout_ms));
        assert!(process::now_ms() < deadline);
        assert!(!*fixture.cancel.borrow());
        let seen = fixture.peer.seen.lock().unwrap().clone();
        assert_eq!(seen.held_calls, 1);
        assert!(!fixture.peer.stopped.load(Ordering::SeqCst));
        assert_eq!(seen.calls, vec![fixture.call.clone()], "timeout never duplicates a recorded effect or unresolved mutation");
        assert_eq!(seen.events, vec!["lookup", "current", "call", "lookup"]);
        assert_eq!(seen.lookups, 2);
        assert_eq!(seen.lookup_receipts, vec![false, committed]);
        assert_eq!(seen.owner_reads, 1);
        assert_eq!(seen.commits, usize::from(committed));
        let retained = fixture.retained();
        assert_eq!(serde_json::to_value(&retained.request).unwrap(), serde_json::to_value(fixture.call.operation()).unwrap());
        if committed {
            let journal = result.unwrap();
            let receipt = seen.receipt.unwrap();
            assert_eq!(journal.outcome, Some(receipt.reply.outcome));
            assert_eq!(journal.outcome_context.unwrap().commit, receipt.reply.commit.unwrap());
            assert_eq!(retained.outcome, journal.outcome);
        } else {
            assert!(result.err().unwrap().starts_with("collaboration call outcome uncertain:"));
            assert!(seen.receipt.is_none());
            assert!(retained.outcome.is_none());
            assert!(retained.outcome_context.is_none());
        }
        assert_eq!(fixture.reporter.provider_uncertain.load(Ordering::SeqCst), !committed);
        assert_eq!(serde_json::to_value(&fixture.input).unwrap(), original_input);
        assert_eq!(fixture.control.deadline_ms, deadline);
        assert_eq!(fixture.control.runtime_limits.transport.client_timeout_ms, timeout_ms);
        assert_eq!(fixture.reporter.sequence.load(Ordering::SeqCst), 1);
        assert!(!fixture.reporter.outbox.exists());
    }
}
