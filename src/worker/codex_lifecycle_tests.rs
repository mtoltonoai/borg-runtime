//! Controlled stdio lifecycle evidence; no provider, key, or fabricated controller receipt.
//! Included as a child of codex.rs to exercise the actual private typed adapter.
use super::*;
use std::{
    os::unix::fs::PermissionsExt,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn area() -> PathBuf {
    let base = std::env::var_os("BORG_TEST_ARTIFACT").map(PathBuf::from)
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("artifacts/v2/build-60156282"))
        .join("worker/tests");
    std::fs::create_dir_all(&base).unwrap();
    let root = base.join(format!("codex-lifecycle-{}-{}-{}",
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),
        std::process::id(), SEQUENCE.fetch_add(1, Ordering::Relaxed)));
    std::fs::create_dir(&root).unwrap();
    root
}

struct Observed {
    root: PathBuf,
    result: std::result::Result<Result, ExecutionFailure>,
    calls: Vec<Value>,
    started: usize,
}

async fn run(mode: &str) -> Observed {
    let root = area();
    let binary = root.join("lifecycle-peer.py");
    std::fs::write(root.join("mode"), mode).unwrap();
    std::fs::write(&binary, r#"#!/usr/bin/python3
import json,pathlib,sys
mode=pathlib.Path('mode').read_text()
def emit(value): print(json.dumps(value),flush=True)
for line in sys.stdin:
    request=json.loads(line)
    if 'id' not in request: continue
    method=request['method']
    with open('requests.jsonl','a') as out: out.write(json.dumps(request)+'\n')
    if method=='initialize': result={}
    elif method=='command/exec':
        if mode=='preflight-refused':
            emit({'id':request['id'],'error':{'code':-32000,'message':'controlled command refusal'}})
            continue
        result={'exitCode':1 if mode=='preflight-failed' else 0,'stdout':'','stderr':'controlled preflight outcome'}
    elif method=='thread/start':
        assert request['params']['model']=='gpt-6-astra'
        assert request['params']['config']['model_reasoning_effort']=='max'
        result={'thread':{'id':'lifecycle-thread'},'model':'gpt-6-astra','reasoningEffort':'max'}
    elif method=='turn/start':
        assert request['params']['threadId']=='lifecycle-thread'
        assert request['params']['model']=='gpt-6-astra' and request['params']['effort']=='max'
        assert request['params']['input']==[{'type':'text','text':'controlled lifecycle instruction'}]
        if mode=='lost-start': sys.exit(0)
        emit({'id':request['id'],'result':{'turn':{'id':'lifecycle-turn'}}})
        if mode=='ack-then-eof': sys.exit(0)
        if mode=='foreign-terminal':
            emit({'method':'turn/completed','params':{'threadId':'lifecycle-thread','turn':{'id':'another-turn','status':'completed'}}})
            sys.exit(0)
        assert mode in ('completed','failed','interrupted')
        emit({'method':'turn/completed','params':{'threadId':'lifecycle-thread','turn':{'id':'lifecycle-turn','status':mode}}})
        continue
    else: result={}
    emit({'id':request['id'],'result':result})
"#).unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
    let (_cancel, rx) = tokio::sync::watch::channel(false);
    let control = crate::worker::test_control(process::now_ms() + 10_000, rx);
    let (events, mut received) = mpsc::channel(16);
    let registry = root.join("processes");
    let result = Box::pin(execute_with_credentials_and_collaboration(
        &binary, &root, &root.join("state"), &registry, &InferenceSettings::selected(),
        &control.runtime_limits.execution, "controlled lifecycle instruction", &control,
        events, &[], None, None, None,
    )).await;
    // The production adapter has awaited its supervisor before returning. Local
    // reap evidence is checked independently of provider/turn outcome certainty.
    assert!(std::fs::read_dir(&registry).unwrap().all(|entry|
        entry.unwrap().path().extension().is_none_or(|extension| extension != "json")),
        "adapter returned without clearing its owned process registry");
    let calls = std::fs::read_to_string(root.join("requests.jsonl")).unwrap().lines()
        .map(|line| serde_json::from_str(line).unwrap()).collect();
    let mut started = 0;
    while let Ok(event) = received.try_recv() {
        match event {
            Event::Started { thread_id, turn_id } => {
                assert_eq!(thread_id, "lifecycle-thread");
                assert_eq!(turn_id, "lifecycle-turn");
                started += 1;
            },
            Event::Progress(_) => {},
            other => panic!("unexpected ordinary execution event: {other:?}"),
        }
    }
    Observed { root, result, calls, started }
}

fn count(calls: &[Value], method: &str) -> usize {
    calls.iter().filter(|call| call["method"] == method).count()
}

#[tokio::test]
async fn command_preflight_failure_is_certain_and_never_starts_a_model_turn() {
    for mode in ["preflight-refused", "preflight-failed"] {
        let observed = run(mode).await;
        assert_eq!(count(&observed.calls, "initialize"), 1);
        assert_eq!(count(&observed.calls, "command/exec"), 1);
        assert_eq!(count(&observed.calls, "thread/start"), 0);
        assert_eq!(count(&observed.calls, "turn/start"), 0);
        assert_eq!(observed.started, 0);
        assert!(!observed.root.join("state/borg-thread.json").exists());
        let error = observed.result.unwrap_err();
        assert!(!error.execution_uncertain, "pre-model refusal became an unknown turn");
        assert!(!error.collaboration_uncertain);
        assert!(!error.completion_held && !error.completion_uncertain);
        assert!(error.message.contains(if mode == "preflight-refused" {
            "executor command/exec refused"
        } else { "preflight failed before model start" }));
    }
}

#[tokio::test]
async fn exact_terminal_status_closes_the_original_turn_without_an_extra_start() {
    for status in ["completed", "failed", "interrupted"] {
        let observed = run(status).await;
        assert_eq!(count(&observed.calls, "thread/start"), 1);
        assert_eq!(count(&observed.calls, "turn/start"), 1);
        assert_eq!(observed.started, 1);
        let result = observed.result.unwrap();
        assert_eq!(result.success, status == "completed");
        assert_eq!(result.thread_id, "lifecycle-thread");
        assert_eq!(result.turn_id, "lifecycle-turn");
        assert_eq!(result.initial_turn_id, "lifecycle-turn");
        assert_eq!(result.summary, format!("Executor turn {status}"));
        assert!(!result.completion_blocked && !result.completion_uncertain);
        let association: Value = serde_json::from_slice(&std::fs::read(
            observed.root.join("state/borg-thread.json")).unwrap()).unwrap();
        assert_eq!(association["thread_id"], result.thread_id);
        assert_eq!(association["turn_id"], result.turn_id);
    }
}

#[tokio::test]
async fn lost_initial_turn_ack_or_terminal_keeps_execution_uncertain_after_reap() {
    for mode in ["lost-start", "ack-then-eof", "foreign-terminal"] {
        let observed = run(mode).await;
        assert_eq!(count(&observed.calls, "thread/start"), 1);
        assert_eq!(count(&observed.calls, "turn/start"), 1,
            "an uncertain original turn must not be started again");
        assert_eq!(observed.started, usize::from(mode != "lost-start"));
        let error = observed.result.unwrap_err();
        assert!(error.execution_uncertain, "{mode} lost the original turn uncertainty");
        assert!(!error.collaboration_uncertain);
        assert!(!error.completion_held && !error.completion_uncertain);
        let association: Value = serde_json::from_slice(&std::fs::read(
            observed.root.join("state/borg-thread.json")).unwrap()).unwrap();
        assert_eq!(association["thread_id"], "lifecycle-thread");
        if mode == "lost-start" {
            assert_eq!(association["turn_start_sent"], true);
            assert!(association.get("turn_id").is_none());
        } else {
            assert_eq!(association["turn_id"], "lifecycle-turn");
        }
    }
}

// The notice shape is the retained pinned ErrorNotification schema: method
// "error", exact thread/turn, TurnError.message and willRetry. The peer controls
// only stdio; the production adapter still owns and reaps the actual process.
async fn notice_peer(mode: &str) -> (PathBuf, PathBuf, tokio::net::TcpListener) {
    let root = area();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let binary = root.join("notice-peer.py");
    std::fs::write(root.join("notice-config.json"), serde_json::to_vec(&json!({
        "mode":mode, "port":listener.local_addr().unwrap().port()
    })).unwrap()).unwrap();
    std::fs::write(&binary, r#"#!/usr/bin/python3
import json,os,pathlib,socket,sys
root=pathlib.Path.cwd()
config=json.loads((root/'notice-config.json').read_text())
def log(direction,value):
    with (root/'notice-protocol.jsonl').open('a') as out:
        out.write(json.dumps({'pid':os.getpid(),'direction':direction,'message':value})+'\n')
def emit(value):
    log('out',value)
    print(json.dumps(value),flush=True)
for line in sys.stdin:
    request=json.loads(line)
    log('in',request)
    if 'id' not in request: continue
    method=request['method']
    if method=='initialize': result={}
    elif method=='command/exec': result={'exitCode':0,'stdout':'','stderr':''}
    elif method=='thread/start':
        assert request['params']['model']=='gpt-6-astra'
        assert request['params']['config']['model_reasoning_effort']=='max'
        result={'thread':{'id':'notice-thread'},'model':'gpt-6-astra','reasoningEffort':'max'}
    elif method=='turn/start':
        assert request['params']['threadId']=='notice-thread'
        assert request['params']['model']=='gpt-6-astra' and request['params']['effort']=='max'
        emit({'id':request['id'],'result':{'turn':{'id':'notice-turn','status':'inProgress'}}})
        if config['mode']=='retry':
            emit({'method':'error','params':{'threadId':'notice-thread','turnId':'notice-turn',
                'error':{'message':'controlled executor-owned retry'},'willRetry':True}})
            # Receipt of this later progress event proves the adapter read past
            # the retry notice. The test holds the terminal event behind G.
            emit({'method':'item/agentMessage/delta','params':{'threadId':'notice-thread',
                'turnId':'notice-turn','itemId':'retry-barrier','delta':'retry-observed-barrier'}})
            with socket.create_connection(('127.0.0.1',config['port']),timeout=10) as gate:
                assert gate.recv(1)==b'G'
            emit({'method':'turn/completed','params':{'threadId':'notice-thread',
                'turn':{'id':'notice-turn','status':'completed'}}})
        continue
    elif method=='turn/interrupt':
        assert config['mode']=='interrupt-ack'
        assert request['params']=={'threadId':'notice-thread','turnId':'notice-turn'}
        emit({'id':request['id'],'result':{}})
        # ACK and EOF are real, but no terminal event is emitted.
        sys.exit(0)
    else: raise AssertionError(method)
    emit({'id':request['id'],'result':result})
"#).unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
    (root, binary, listener)
}

fn notice_protocol(root: &Path, bound: usize) -> Vec<Value> {
    let bytes = std::fs::read(root.join("notice-protocol.jsonl")).unwrap();
    assert!(bytes.len() <= bound);
    std::str::from_utf8(&bytes).unwrap().lines()
        .map(|line| serde_json::from_str(line).unwrap()).collect()
}

fn assert_single_notice_turn(rows: &[Value]) {
    let incoming: Vec<Value> = rows.iter().filter(|row| row["direction"] == "in")
        .map(|row| row["message"].clone()).collect();
    assert_eq!(count(&incoming, "initialize"), 1);
    assert_eq!(count(&incoming, "thread/start"), 1);
    assert_eq!(count(&incoming, "turn/start"), 1);
    assert_eq!(count(&incoming, "thread/resume"), 0);
    assert_eq!(count(&incoming, "turn/steer"), 0);
    let pid = rows.first().unwrap()["pid"].as_u64().unwrap();
    assert!(rows.iter().all(|row| row["pid"] == pid));
}

#[tokio::test]
async fn retry_notice_preserves_original_session_and_turn_until_exact_terminal() {
    let (root, binary, listener) = notice_peer("retry").await;
    let (_cancel, rx) = tokio::sync::watch::channel(false);
    let control = crate::worker::test_control(process::now_ms() + 10_000, rx);
    let (events, mut received) = mpsc::channel(16);
    let registry = root.join("processes");
    let state = root.join("state");
    let inference = InferenceSettings::selected();
    let mut execution = Box::pin(execute_with_credentials_and_collaboration(
        &binary, &root, &state, &registry, &inference,
        &control.runtime_limits.execution, "controlled retry notice", &control,
        events, &[], None, None, None,
    ));
    let mut started = 0;
    let barrier = async {
        let (gate, _) = listener.accept().await.unwrap();
        loop {
            match received.recv().await.expect("adapter event before terminal") {
                Event::Started { thread_id, turn_id } => {
                    assert_eq!(thread_id, "notice-thread");
                    assert_eq!(turn_id, "notice-turn");
                    started += 1;
                },
                Event::Progress(text) if text == "retry-observed-barrier" => break,
                _ => panic!("unexpected retry barrier event"),
            }
        }
        gate
    };
    let mut gate = tokio::select! {
        _ = &mut execution => panic!("retry notice completed execution before its exact terminal"),
        gate = barrier => gate,
    };
    assert_eq!(started, 1);
    let before = notice_protocol(&root, control.runtime_limits.transport.body_bytes);
    assert_single_notice_turn(&before);
    assert!(before.iter().any(|row| row["direction"] == "out"
        && row["message"]["method"] == "error" && row["message"]["params"]["willRetry"] == true));
    assert!(!before.iter().any(|row| row["message"]["method"] == "turn/completed"));
    let association = std::fs::read(state.join("borg-thread.json")).unwrap();
    let original: Value = serde_json::from_slice(&association).unwrap();
    assert_eq!(original["thread_id"], "notice-thread");
    assert_eq!(original["turn_id"], "notice-turn");
    tokio::io::AsyncWriteExt::write_all(&mut gate, b"G").await.unwrap();
    let result = execution.await.expect("exact original terminal succeeds");
    assert!(result.success);
    assert_eq!(result.thread_id, "notice-thread");
    assert_eq!(result.turn_id, "notice-turn");
    assert_eq!(result.initial_turn_id, "notice-turn");
    assert_eq!(std::fs::read(state.join("borg-thread.json")).unwrap(), association);
    assert_single_notice_turn(&notice_protocol(&root, control.runtime_limits.transport.body_bytes));
    assert!(received.try_recv().is_err(), "retry notice created another start event");
    assert!(std::fs::read_dir(&registry).unwrap().all(|entry|
        entry.unwrap().path().extension().is_none_or(|extension| extension != "json")));
}

#[tokio::test]
async fn interrupt_ack_without_terminal_keeps_original_lifecycle_uncertain_after_reap() {
    let (root, binary, _listener) = notice_peer("interrupt-ack").await;
    let (_cancel, rx) = tokio::sync::watch::channel(false);
    let control = crate::worker::test_control(process::now_ms() + 10_000, rx);
    let registry = root.join("processes");
    let mut session = Session::start(&binary, &root, &root.join("state"), &registry,
        &InferenceSettings::selected(), &control.runtime_limits.execution,
        control.worker_limits.clone(), &control.runtime_limits).await.unwrap();
    let lifecycle = session.lifecycle.clone();
    session.initialize(&control).await.unwrap();
    session.preflight_command(&root, &control).await.unwrap();
    let thread = session.request("thread/start", json!({
        "model":"gpt-6-astra", "config":{"model_reasoning_effort":"max"}
    }), &control).await.unwrap();
    assert_eq!(thread["thread"]["id"], "notice-thread");
    let turn = session.request("turn/start", json!({"threadId":"notice-thread",
        "model":"gpt-6-astra", "effort":"max", "input":[{"type":"text","text":"interrupt boundary"}]
    }), &control).await.unwrap();
    assert_eq!(turn["turn"]["id"], "notice-turn");
    assert!(lifecycle.uncertain());
    // This is the actual Session request/reply boundary. The automatic error
    // path immediately kills its peer and is not delayed merely to receive ACK.
    let ack = session.request("turn/interrupt", json!({
        "threadId":"notice-thread", "turnId":"notice-turn"
    }), &control).await.unwrap();
    assert_eq!(ack, json!({}));
    assert!(lifecycle.uncertain(), "interrupt ACK was mistaken for exact terminal");
    assert!(session.next(&control, None).await.is_err(), "peer emitted no terminal");
    assert!(lifecycle.uncertain(), "EOF was mistaken for exact terminal");
    session.shutdown().await.unwrap();
    assert!(lifecycle.uncertain(), "physical reap was mistaken for exact terminal");
    let rows = notice_protocol(&root, control.runtime_limits.transport.body_bytes);
    assert_single_notice_turn(&rows);
    let interrupts: Vec<_> = rows.iter().filter(|row| row["direction"] == "in"
        && row["message"]["method"] == "turn/interrupt").collect();
    assert_eq!(interrupts.len(), 1);
    let id = &interrupts[0]["message"]["id"];
    assert!(rows.iter().any(|row| row["direction"] == "out"
        && &row["message"]["id"] == id && row["message"]["result"] == json!({})));
    assert!(!rows.iter().any(|row| row["message"]["method"] == "turn/completed"));
    assert!(std::fs::read_dir(&registry).unwrap().all(|entry|
        entry.unwrap().path().extension().is_none_or(|extension| extension != "json")));
}
