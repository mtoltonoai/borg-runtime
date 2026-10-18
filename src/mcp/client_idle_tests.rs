//! Controlled HTTP peer around the actual Client; no provider, Codex, or host.
//! A typed fixture receipt is not a real controller durability qualification.
use super::*;
use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{Mutex, atomic::AtomicBool, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

fn selected_limits() -> RuntimeLimits {
    let limits: RuntimeLimits = serde_json::from_slice(include_bytes!("../../metadata/limits.json")).unwrap();
    limits.validate().unwrap();
    assert_eq!(limits.transport.http_header_timeout_ms, 10_000);
    limits
}
fn invocation() -> Invocation {
    Invocation {
        operation_id: "idle-original-call".into(), logical_call_id: "idle-original-logical".into(),
        caller: Caller { task_id: "idle-task".into(), attempt_id: "idle-attempt".into(),
            generation: 1, worker_id: "idle-worker".into(), incarnation: "idle-incarnation".into() },
        tool: "collab_task_get".into(), arguments: json!({"task_id":"absent-fixture-task"}),
    }
}
fn original_result(call: &Invocation) -> CallResult {
    let mut reply=Reply::refused("fixture-controller-read", "not_found", "fixture target absent");
    reply.commit=Some(MutationCommit { operation_id:call.operation_id.clone(), controller_revision:7 });
    reply.observed_controller_revision=Some(7);
    result(call,reply).unwrap()
}
#[derive(Default, Clone, Debug)]
struct Observed {
    sockets: usize,
    requests: Vec<(String, Option<String>, usize)>,
    committed: Option<CallResult>,
    idle_socket: Option<usize>,
}
struct Peer {
    url: String,
    stop: Arc<AtomicBool>,
    sockets: Arc<Mutex<Vec<TcpStream>>>,
    seen: Arc<Mutex<Observed>>,
    closed: mpsc::Receiver<usize>,
    thread: Option<JoinHandle<()>>,
}
fn read_request(stream: &mut TcpStream, bound: usize) -> Option<Value> {
    let mut header=Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        let mut byte=[0];
        match stream.read(&mut byte) { Ok(0)=>return None, Ok(_)=>header.push(byte[0]),
            Err(e) if matches!(e.kind(),std::io::ErrorKind::WouldBlock|std::io::ErrorKind::TimedOut)=>return None,
            Err(e) if matches!(e.kind(),std::io::ErrorKind::ConnectionReset|std::io::ErrorKind::NotConnected)=>return None,
            Err(e)=>panic!("fixture read: {e:?}"), }
        assert!(header.len()<=32768);
    }
    let header=String::from_utf8(header).unwrap();
    assert!(header.starts_with("POST /mcp HTTP/1.1\r\n"));
    let length=header.lines().find_map(|line|line.split_once(':').filter(|(name,_)|name.eq_ignore_ascii_case("content-length")).map(|(_,value)|value.trim().parse::<usize>().unwrap())).unwrap();
    assert!(length<=bound);
    let mut body=vec![0;length];stream.read_exact(&mut body).unwrap();
    serde_json::from_slice(&body).ok()
}
fn send_response(stream: &mut TcpStream, value: &Value) {
    let body=serde_json::to_vec(value).unwrap();
    write!(stream,"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",body.len()).unwrap();
    stream.write_all(&body).unwrap();stream.flush().unwrap();
}
impl Peer {
    fn new(limits: RuntimeLimits, close_idle: bool, drop_committed_reply: bool) -> Self {
        let listener=TcpListener::bind("127.0.0.1:0").unwrap();listener.set_nonblocking(true).unwrap();
        let url=format!("http://{}",listener.local_addr().unwrap());
        let stop=Arc::new(AtomicBool::new(false));let sockets=Arc::new(Mutex::new(Vec::new()));
        let seen=Arc::new(Mutex::new(Observed::default()));let (closed_tx,closed)=mpsc::channel();
        let (st,ss,observed)=(stop.clone(),sockets.clone(),seen.clone());
        let thread=thread::spawn(move|| {
            let deadline=Instant::now()+Duration::from_secs(60);
            while !st.load(Ordering::SeqCst) && Instant::now()<deadline {
                let (mut socket,_)=match listener.accept() { Ok(pair)=>pair,
                    Err(e) if e.kind()==std::io::ErrorKind::WouldBlock=>{thread::sleep(Duration::from_millis(2));continue},
                    Err(e)=>panic!("fixture accept: {e:?}"), };
                socket.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
                socket.set_write_timeout(Some(Duration::from_secs(2))).unwrap();
                ss.lock().unwrap().push(socket.try_clone().unwrap());
                let index={let mut s=observed.lock().unwrap();s.sockets+=1;s.sockets};
                while !st.load(Ordering::SeqCst) {
                    let Some(rpc)=read_request(&mut socket,limits.transport.body_bytes) else {break};
                    let method=rpc["method"].as_str().unwrap().to_owned();
                    let op=if method=="tools/call" {rpc["params"]["_meta"]["borg"]["operation_id"].as_str().map(str::to_owned)}
                        else if method=="borg/calls/get" {rpc["params"]["call_id"].as_str().map(str::to_owned)} else {None};
                    observed.lock().unwrap().requests.push((method.clone(),op.clone(),index));
                    let response=match method.as_str() {
                        "initialize"=>success(rpc["id"].clone(),json!({"protocolVersion":VERSION})),
                        "tools/list"=>success(rpc["id"].clone(),json!({"tools":catalogue(&limits)})),
                        "tools/call"=>{
                            let call=invocation();assert_eq!(op.as_deref(),Some(call.operation_id.as_str()));
                            assert_eq!(rpc["params"]["name"],call.tool);assert_eq!(rpc["params"]["arguments"],call.arguments);
                            let value=original_result(&call);
                            let mut state=observed.lock().unwrap();assert!(state.committed.is_none(),"tools/call must never be resent");state.committed=Some(value.clone());drop(state);
                            if drop_committed_reply {let _=socket.shutdown(Shutdown::Both);break;}
                            tool_result(rpc["id"].clone(),&value)
                        },
                        "borg/calls/get"=>{
                            let call=invocation();assert_eq!(op.as_deref(),Some(call.operation_id.as_str()));
                            let old=observed.lock().unwrap().committed.clone();
                            let mut reply=match old {
                                Some(value)=>Reply { request_id:"fixture-original-lookup".into(),commit:value.reply.commit.clone(),observed_controller_revision:Some(8),
                                    outcome:Outcome::Ok {data:Data::Operation(OperationRecord {id:call.operation_id.clone(),task_id:call.caller.task_id.clone(),attempt_id:call.caller.attempt_id.clone(),generation:call.caller.generation,kind:"application_mutation".into(),intended:serde_json::to_value(call.operation()).unwrap(),state:"settled".into(),outcome:Some(serde_json::to_value(value.reply.outcome).unwrap())})}},
                                None=>Reply::refused("fixture-no-record","not_found","fixture has no committed call"),
                            };reply.observed_controller_revision=Some(8);
                            tool_result(rpc["id"].clone(),&CallResult{reply,requires_quiescence:false})
                        },
                        _=>panic!("unexpected fixed fixture method"),
                    };
                    send_response(&mut socket,&response);
                    if method=="tools/list" && close_idle {
                        // Real selected header timeout, not client eviction (90s).
                        // The peer OS thread runs while the client runtime is not polled.
                        thread::sleep(Duration::from_millis(limits.transport.http_header_timeout_ms+50));
                        let _=socket.shutdown(Shutdown::Both);
                        observed.lock().unwrap().idle_socket=Some(index);
                        let _=closed_tx.send(index);break;
                    }
                }
            }
        });
        Self {url,stop,sockets,seen,closed,thread:Some(thread)}
    }
    fn finish(&mut self) -> Observed {
        self.stop.store(true,Ordering::SeqCst);
        for socket in self.sockets.lock().unwrap().iter() {let _=socket.shutdown(Shutdown::Both);}
        if let Some(worker)=self.thread.take(){worker.join().unwrap();}
        self.seen.lock().unwrap().clone()
    }
}
impl Drop for Peer {fn drop(&mut self){let _=self.finish();}}
#[test]
fn unpolled_current_thread_after_catalogue_uses_fresh_connection_once() {
    let limits=selected_limits();let mut peer=Peer::new(limits.clone(),true,false);
    let runtime=tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let client=Client::new(&peer.url,invocation().caller,limits.clone()).unwrap();
    assert_eq!(runtime.block_on(client.tools()).unwrap(),catalogue(&limits));
    let idle=peer.closed.recv_timeout(Duration::from_secs(15)).expect("peer must close while client reactor is unpolled");
    // The regression invokes only the production constructor. Unlike the
    // preserved diagnostic, transport-unknown is no longer an accepted result.
    let received=runtime.block_on(client.call(&invocation())).expect("fresh MCP connection must dispatch the original call");
    let original=original_result(&invocation());assert_eq!(received,original);
    drop(client);drop(runtime);let observed=peer.finish();
    assert_eq!(idle,2);assert_eq!(observed.sockets,3);
    assert_eq!(observed.requests,vec![("initialize".into(),None,1),("tools/list".into(),None,2),
        ("tools/call".into(),Some(invocation().operation_id),3)]);
    assert_eq!(observed.committed,Some(original));
    assert!(peer.thread.is_none());
    println!("BORG_MCP_IDLE_REGRESSION={}",json!({"production_constructor":true,"accepted_sockets":3,"original_tools_calls":1,"lookups":0,"peer_thread_joined":true,"historical_cause_proven":false}));
}
#[test]
fn dropped_committed_reply_remains_unknown_then_original_lookup_without_resend() {
    let limits=selected_limits();let mut peer=Peer::new(limits.clone(),false,true);
    let runtime=tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let client=Client::new(&peer.url,invocation().caller,limits.clone()).unwrap();
    assert_eq!(runtime.block_on(client.tools()).unwrap(),catalogue(&limits));
    let error=runtime.block_on(client.call(&invocation())).unwrap_err();
    assert_eq!(error,"MCP transport outcome unknown; reconcile original occurrence");
    let recovered=runtime.block_on(client.get_call(&invocation())).unwrap().expect("original committed fixture receipt");
    let original=original_result(&invocation());
    assert_eq!(recovered.reply.outcome,original.reply.outcome);assert_eq!(recovered.reply.commit,original.reply.commit);
    assert_eq!(recovered.requires_quiescence,original.requires_quiescence);
    drop(client);drop(runtime);let observed=peer.finish();
    assert_eq!(observed.sockets,4);
    assert_eq!(observed.requests,vec![("initialize".into(),None,1),("tools/list".into(),None,2),
        ("tools/call".into(),Some(invocation().operation_id.clone()),3),
        ("borg/calls/get".into(),Some(invocation().operation_id),4)]);
    assert_eq!(observed.committed,Some(original));assert!(peer.thread.is_none());
    println!("BORG_MCP_DROPPED_REPLY_REGRESSION={}",json!({"production_constructor":true,"original_tools_calls":1,"original_lookups":1,"peer_thread_joined":true,"controller_durability_claim":false}));
}
