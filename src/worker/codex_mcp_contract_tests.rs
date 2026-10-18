//! Actual Client-backed Bridge with complete invalid HTTP replies and controlled
//! transport loss. Typed fixture receipts do not claim controller durability.
use super::*;
use std::{
    net::{Shutdown, TcpListener, TcpStream},
    thread::{self, JoinHandle},
    time::Instant,
};

#[derive(Clone, Copy)]
enum ResponseMode { ForeignCommit, Quiescence, MalformedCall, MalformedLookup, LossBefore, LossAfter }
#[derive(Default, Clone)]
struct Observed {
    calls: Vec<Invocation>,
    lookups: usize,
    receipt: Option<CallResult>,
}
struct HttpPeer {
    url: String,
    seen: Arc<Mutex<Observed>>,
    stop: Arc<AtomicBool>,
    sockets: Arc<Mutex<Vec<TcpStream>>>,
    thread: Option<JoinHandle<()>>,
}
fn read_request(socket: &mut TcpStream, bound: usize) -> Option<Value> {
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        if socket.read(&mut byte).ok()? == 0 { return None; }
        header.push(byte[0]);
        assert!(header.len() <= 32_768, "fixture header bound");
    }
    let header = String::from_utf8(header).unwrap();
    assert!(header.starts_with("POST /mcp HTTP/1.1\r\n"), "no provider or executor route");
    let length = header.lines().find_map(|line| line.split_once(':')
        .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .map(|(_, value)| value.trim().parse::<usize>().unwrap())).unwrap();
    assert!(length <= bound);
    let mut body = vec![0; length];
    socket.read_exact(&mut body).ok()?;
    Some(serde_json::from_slice(&body).unwrap())
}
fn wire(id: Value, result: &CallResult) -> Value {
    json!({"jsonrpc":"2.0","id":id,"result":{
        "content":[{"type":"text","text":serde_json::to_string(result).unwrap()}],
        "isError":!matches!(result.reply.outcome,Outcome::Ok{..})}})
}
fn malformed(id: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"result":{
        "content":[{"type":"text","text":"{}"}],"isError":false}})
}
fn send(socket: &mut TcpStream, value: Value) {
    let body = serde_json::to_vec(&value).unwrap();
    let header = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n", body.len());
    let _ = socket.write_all(header.as_bytes()).and_then(|_| socket.write_all(&body));
}
impl HttpPeer {
    fn new(config: &Config, mode: ResponseMode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Observed::default()));
        let stop = Arc::new(AtomicBool::new(false));
        let sockets = Arc::new(Mutex::new(Vec::new()));
        let (observed, stopped, saved) = (seen.clone(), stop.clone(), sockets.clone());
        let config = config.clone();
        let thread = thread::spawn(move || {
            let end = Instant::now() + Duration::from_secs(30);
            while !stopped.load(Ordering::SeqCst) && Instant::now() < end {
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
                let Some(body) = read_request(&mut socket, config.runtime_limits.transport.body_bytes) else { continue };
                let id = body["id"].clone();
                // The real bridge must durably record exact intent before Client
                // sends either the call or an original-occurrence lookup.
                let journal: Journal = decode(&std::fs::read(config.directory().join("calls.json")).unwrap(),
                    config.limits.journal_bytes).unwrap();
                assert_eq!(journal.entries.len(), 1);
                let call = journal.entries[0].invocation.clone();
                assert_eq!(call.caller, config.caller);
                assert!(!journal.entries[0].delivered);
                assert!(journal.entries[0].outcome.is_none());
                match body["method"].as_str().unwrap() {
                    "tools/call" => {
                        assert_eq!(body["params"]["name"], call.tool);
                        assert_eq!(body["params"]["arguments"], call.arguments);
                        assert_eq!(body["params"]["_meta"]["borg"]["operation_id"], call.operation_id);
                        assert_eq!(body["params"]["_meta"]["borg"]["logical_call_id"], call.logical_call_id);
                        let count = {
                            let mut seen = observed.lock().unwrap();
                            seen.calls.push(call.clone()); seen.calls.len()
                        };
                        let mut result = super::result(&call, Mode::Refused);
                        if count == 1 {
                            match mode {
                                ResponseMode::ForeignCommit => {
                                    result.reply.commit.as_mut().unwrap().operation_id = "foreign-operation".into();
                                    send(&mut socket, wire(id, &result)); continue;
                                },
                                ResponseMode::Quiescence => {
                                    result.requires_quiescence = true;
                                    send(&mut socket, wire(id, &result)); continue;
                                },
                                ResponseMode::MalformedCall => {send(&mut socket, malformed(id)); continue;},
                                ResponseMode::LossBefore | ResponseMode::MalformedLookup => {
                                    let _ = socket.shutdown(Shutdown::Both); continue;
                                },
                                ResponseMode::LossAfter => {
                                    observed.lock().unwrap().receipt = Some(result);
                                    let _ = socket.shutdown(Shutdown::Both); continue;
                                },
                            }
                        }
                        observed.lock().unwrap().receipt = Some(result.clone());
                        send(&mut socket, wire(id, &result));
                    },
                    "borg/calls/get" => {
                        assert_eq!(body["params"]["call_id"], call.operation_id);
                        let (count, receipt) = {
                            let mut seen = observed.lock().unwrap(); seen.lookups += 1;
                            (seen.lookups, seen.receipt.clone())
                        };
                        if matches!(mode, ResponseMode::MalformedLookup) && count == 1 {
                            send(&mut socket, malformed(id)); continue;
                        }
                        let mut reply = match receipt {
                            Some(original) => Reply {request_id:"lookup".into(),commit:original.reply.commit,
                                observed_controller_revision:Some(8), outcome:Outcome::Ok {data:Data::Operation(OperationRecord {
                                    id:call.operation_id.clone(),task_id:call.caller.task_id.clone(),
                                    attempt_id:call.caller.attempt_id.clone(),generation:call.caller.generation,
                                    kind:"application_mutation".into(),intended:serde_json::to_value(call.operation()).unwrap(),
                                    state:"settled".into(),outcome:Some(serde_json::to_value(original.reply.outcome).unwrap()),
                                })}},
                            None => Reply::refused("lookup", "not_found", "fixture has no original receipt"),
                        };
                        reply.observed_controller_revision = Some(8);
                        send(&mut socket, wire(id, &CallResult {reply,requires_quiescence:false}));
                    },
                    other => panic!("unexpected fixture MCP method: {other}"),
                }
            }
        });
        Self {url, seen, stop, sockets, thread:Some(thread)}
    }
}
impl Drop for HttpPeer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        for socket in self.sockets.lock().unwrap().iter() { let _ = socket.shutdown(Shutdown::Both); }
        if let Some(thread) = self.thread.take() { thread.join().unwrap(); }
    }
}
fn configure(mode: ResponseMode) -> (Area, Config, HttpPeer, Arc<AtomicBool>) {
    let (area, mut config, _, cancel) = setup(Mode::Good);
    config.limits.lookup_backoff_ms = 2;
    config.limits.liveness_interval_ms = 1;
    config.runtime_limits.execution.collaboration = config.limits.clone();
    let peer = HttpPeer::new(&config, mode);
    config.controller_url = peer.url.clone();
    (area, config, peer, cancel)
}
fn read_arguments() -> Value { json!({"task_id":"fixture-target"}) }

#[tokio::test]
async fn actual_client_complete_invalid_read_reply_is_terminal_not_retry_authority() {
    for mode in [ResponseMode::ForeignCommit, ResponseMode::Quiescence, ResponseMode::MalformedCall] {
        let (_area, config, peer, cancel) = configure(mode);
        let client = config.client().unwrap();
        let mut bridge = Bridge::open(config, client, vec![], cancel).unwrap();
        assert!(bridge.invoke("collab_task_get", read_arguments(), &meta("invalid-receipt")).await.is_err());
        let seen = peer.seen.lock().unwrap().clone();
        assert_eq!(seen.calls.len(), 1, "a second send would succeed");
        assert_eq!(seen.lookups, 0, "complete invalid receipt exits before reconciliation");
        assert!(seen.receipt.is_none());
        let retained: Journal = decode(&std::fs::read(&bridge.path).unwrap(), bridge.config.limits.journal_bytes).unwrap();
        assert_eq!(retained.entries[0].invocation, seen.calls[0]);
        assert!(retained.entries[0].outcome.is_none());
        assert!(!retained.entries[0].delivered);
        assert_eq!(outcome_disposition(&bridge.config).unwrap(), OutcomeDisposition::Unresolved);
    }
}

#[tokio::test]
async fn actual_client_complete_invalid_lookup_is_not_absence_or_retry_authority() {
    let (_area, config, peer, cancel) = configure(ResponseMode::MalformedLookup);
    let client = config.client().unwrap();
    let mut bridge = Bridge::open(config, client, vec![], cancel).unwrap();
    assert!(bridge.invoke("collab_task_get", read_arguments(), &meta("invalid-lookup")).await.is_err());
    let seen = peer.seen.lock().unwrap().clone();
    assert_eq!(seen.calls.len(), 1);
    assert_eq!(seen.lookups, 1, "later lookup would return absence; it must not be attempted");
    assert!(bridge.journal.entries[0].outcome.is_none());
    assert!(!bridge.journal.entries[0].delivered);
    assert_eq!(outcome_disposition(&bridge.config).unwrap(), OutcomeDisposition::Unresolved);
}

#[tokio::test]
async fn actual_client_transport_loss_recovers_original_mutation_receipt_without_repost() {
    let (_area, config, peer, cancel) = configure(ResponseMode::LossAfter);
    let client = config.client().unwrap();
    let mut bridge = Bridge::open(config, client, vec![], cancel).unwrap();
    let (wire, index) = bridge.invoke("collab_memory_set", args(), &meta("lost-mutation")).await.unwrap();
    assert_eq!(wire["isError"], true, "the original committed refusal is final");
    let seen = peer.seen.lock().unwrap().clone();
    assert_eq!(seen.calls.len(), 1);
    assert_eq!(seen.lookups, 1);
    let recovered = bridge.journal.entries[index].outcome.as_ref().unwrap().inline().unwrap();
    let original = seen.receipt.unwrap();
    assert_eq!(recovered.reply.outcome, original.reply.outcome);
    assert_eq!(recovered.reply.commit, original.reply.commit);
    bridge.delivered(index).unwrap();
    let replay = bridge.invoke("collab_memory_set", args(), &meta("lost-mutation")).await.unwrap().0;
    assert_eq!(wire, replay);
    assert_eq!(peer.seen.lock().unwrap().calls.len(), 1);
}

#[tokio::test]
async fn actual_client_read_transport_loss_retries_only_original_after_fresh_absence() {
    let (_area, config, peer, cancel) = configure(ResponseMode::LossBefore);
    let client = config.client().unwrap();
    let mut bridge = Bridge::open(config, client, vec![], cancel).unwrap();
    bridge.invoke("collab_task_get", read_arguments(), &meta("lost-read")).await.unwrap();
    let seen = peer.seen.lock().unwrap().clone();
    assert_eq!(seen.calls.len(), 2);
    assert_eq!(seen.calls[0], seen.calls[1]);
    assert_eq!(seen.lookups, 2, "immediate original lookup plus fresh absence after backoff");
    assert!(bridge.journal.entries[0].outcome.is_some());
}

// A legacy/third-party Transport implementation cannot grant transport retry
// authority merely by returning a String with a familiar diagnostic prefix.
struct StringOnly(Fake);
impl Transport for StringOnly {
    async fn call(&self, call: &Invocation) -> Result<CallResult, String> { self.0.call(call).await }
    async fn lookup(&self, call: &Invocation) -> Result<Option<CallResult>, String> { self.0.lookup(call).await }
}
#[tokio::test]
async fn string_only_transport_error_defaults_to_contract_even_with_transport_message() {
    let (_area, config, fake, cancel) = setup(Mode::LossBefore);
    let mut bridge = Bridge::open(config, StringOnly(fake.clone()), vec![], cancel).unwrap();
    assert!(bridge.invoke("collab_task_get", read_arguments(), &meta("untyped-failure")).await.is_err());
    let seen = fake.state.lock().unwrap();
    assert_eq!(seen.calls, 1);
    assert_eq!(seen.lookups, 0);
    assert!(bridge.journal.entries[0].outcome.is_none());
    assert!(!bridge.journal.entries[0].delivered);
}
