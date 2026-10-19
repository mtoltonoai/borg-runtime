//! Controlled stdio only. Exercises the real framed reader, authority refusal,
//! supervisor and accepted queue bound without Codex, credentials or a provider.
use super::*;
use std::{os::unix::fs::PermissionsExt, sync::atomic::AtomicU64};
static SEQUENCE: AtomicU64 = AtomicU64::new(1);

const PEER: &str = r#"#!/usr/bin/python3
import json,pathlib,sys
root=pathlib.Path.cwd(); mode=(root/'mode').read_text()
def emit(v): print(json.dumps(v),flush=True)
def delta(text): return {'method':'item/agentMessage/delta','params':{'threadId':'thread','turnId':'prior','itemId':'progress','delta':text}}
def required():
 return [
 {'method':'item/completed','params':{'threadId':'thread','turnId':'prior','item':{'id':'final','type':'agentMessage','phase':'final_answer','text':'authoritative final'}}},
 {'method':'thread/tokenUsage/updated','params':{'threadId':'thread','turnId':'prior','tokenUsage':{'total':{'inputTokens':11,'outputTokens':3}}}},
 {'method':'error','params':{'threadId':'thread','turnId':'prior','willRetry':True,'error':{'codexErrorInfo':'rateLimitExceeded'}}},
 {'method':'turn/completed','params':{'threadId':'thread','turn':{'id':'prior','status':'completed'}}},
 {'method':'future/unknown','params':{'private':'must stay queued'}}]
for line in sys.stdin:
 request=json.loads(line)
 with (root/'requests.jsonl').open('a') as f: f.write(json.dumps(request)+'\n')
 if 'method' not in request:
  assert request=={'id':900,'error':{'code':-32601,'message':'This worker does not grant interactive requests'}}
  (root/'authority-refused').write_text('yes')
  for item in required(): emit(item)
  emit(delta('drained'))
  continue
 if mode=='partial' and request['method']=='finish':
  sys.stdout.write('"complete"}}\n');sys.stdout.flush()
  emit({'id':request['id'],'result':{}});continue
 if mode=='partial':
  emit({'id':request['id'],'result':{}})
  sys.stdout.write('{"method":"future/partial","params":{"value":');sys.stdout.flush();continue
 if mode=='wait' and request['method']=='begin':
  emit({'id':request['id'],'result':{}})
  for n in range(256): emit(delta('x'*32768))
  emit({'id':900,'method':'item/commandExecution/requestApproval','params':{'command':'NEVER EXECUTE'}})
  continue
 if mode in ('required','unknown','malformed'):
  for n in range(65):
   item=required()[0] if mode=='required' else {'method':'secret-unrecognized-method','params':{'value':'secret-payload'}}
   if mode=='malformed': item={'method':'item/agentMessage/delta','params':{'delta':{'not':'text'}}}
   emit(item)
  continue
 if mode=='eof':
  emit({'id':request['id'],'result':{}});sys.exit(0)
 if mode in ('preack','lost'):
  for n in range(128): emit(delta('progress-'+str(n)))
  if mode=='lost': sys.exit(0)
  for item in required():emit(item)
 emit({'id':request['id'],'result':{'turn':{'id':'prior'}}})
"#;

async fn fixture(mode: &str) -> (PathBuf, Session, Control, tokio::sync::watch::Sender<bool>, mpsc::Receiver<Event>) {
    let base = std::env::var_os("BORG_TEST_ARTIFACT").map(PathBuf::from)
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("artifacts/v2/build-60156282"))
        .join("worker/tests");
    std::fs::create_dir_all(&base).unwrap();
    let root = base.join(format!("codex-notification-{}-{}-{}", process::now_ms(), std::process::id(), SEQUENCE.fetch_add(1, Ordering::Relaxed)));
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("mode"), mode).unwrap();
    let binary = root.join("peer.py");
    std::fs::write(&binary, PEER).unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut limits = (*crate::worker::test_limits()).clone();
    limits.execution.coding.pending_notifications = 64;
    limits.execution.coding.progress_chars = 32;
    let (cancel, rx) = tokio::sync::watch::channel(false);
    let control = crate::worker::test_control(process::now_ms() + 10_000, rx);
    let mut session = Session::start_with_key(&binary, &root, &root.join("state"), &root.join("processes"),
        &InferenceSettings::selected(), &limits.execution, Arc::from(limits.worker.clone()), None).await.unwrap();
    let (events, received) = mpsc::channel(512);
    session.progress = Some(events);
    (root, session, control, cancel, received)
}
fn assert_required(pending: &VecDeque<Value>) {
    assert_eq!(pending.iter().map(|v|v["method"].as_str().unwrap()).collect::<Vec<_>>(),
        vec!["item/completed", "thread/tokenUsage/updated", "error", "turn/completed", "future/unknown"]);
    assert_eq!(pending[0]["params"]["item"]["text"], "authoritative final");
    assert_eq!(pending[3]["params"]["turn"]["status"], "completed");
    assert_eq!(pending[4]["params"]["private"], "must stay queued");
    let mut usage = ObservedUsage::default();
    assert!(usage.observe(&pending[1], "thread", "prior"));
    assert!(!usage.observe(&pending[1], "thread", "prior"));
    assert!(!usage.observe(&pending[1], "thread", "successor"));
    assert_eq!(usage.input_tokens, Some(11));
    assert_eq!(usage.output_tokens, Some(3));
}

#[tokio::test]
async fn pre_ack_progress_burst_preserves_terminal_item_usage_and_unknown_fifo() {
    let (_, mut session, control, _cancel, mut received) = fixture("preack").await;
    let result = session.request("turn/start", json!({}), &control).await;
    let pending = session.pending.clone();
    let uncertain = session.lifecycle.uncertain();
    session.shutdown().await.unwrap();
    assert_eq!(result.unwrap()["turn"]["id"], "prior");
    assert!(uncertain, "buffering a terminal before ACK must not settle execution");
    assert_required(&pending);
    let mut progress = Vec::new();
    while let Some(event) = received.recv().await {
        if let Event::Progress(text) = event { progress.push(text); } else { panic!("unexpected observer effect"); }
    }
    assert_eq!(progress.len(), 128);
    assert_eq!(progress[0], "progress-0");
    assert_eq!(progress[127], "progress-127");
}

#[tokio::test]
async fn required_unknown_and_malformed_notifications_keep_bound_and_redacted_diagnostics() {
    for mode in ["required", "unknown", "malformed"] {
        let (root, mut session, control, _cancel, _) = fixture(mode).await;
        let result = session.request("turn/start", json!({}), &control).await;
        assert_eq!(session.pending.len(), 64);
        assert!(session.lifecycle.uncertain());
        session.shutdown().await.unwrap();
        let error = result.unwrap_err();
        assert!(error.starts_with("executor notification backlog exceeds bound (execution.coding.pending_notifications)"));
        assert!(error.contains("waiting=turn_start; queued=64"));
        assert!(error.contains(if mode == "required" { "incoming=item; terminal=0; item=64" } else { "other=64" }));
        assert!(!error.contains("secret") && !error.contains("not text") && error.len() < 300);
        assert_eq!(std::fs::read_to_string(root.join("requests.jsonl")).unwrap().lines().count(), 1);
    }
}

#[tokio::test]
async fn canonical_wait_drains_pipe_progress_and_refuses_authority_without_reordering() {
    let (root, mut session, control, _cancel, mut received) = fixture("wait").await;
    session.request("begin", json!({}), &control).await.unwrap();
    let result = session.wait_with_notifications(|| async {
        let mut count = 0;
        while let Some(event) = received.recv().await {
            if let Event::Progress(text) = event {
                if text == "drained" { return Ok(count); }
                assert_eq!(text, "x".repeat(32));
                count += 1;
            } else { panic!("unexpected observer effect"); }
        }
        Err("progress observer closed".into())
    }, &control).await;
    let pending = session.pending.clone();
    session.shutdown().await.unwrap();
    assert_eq!(result.unwrap(), 256, "peer writes eight MiB before its wait barrier");
    assert_required(&pending);
    assert_eq!(std::fs::read_to_string(root.join("authority-refused")).unwrap(), "yes");
    let rows = std::fs::read_to_string(root.join("requests.jsonl")).unwrap();
    assert_eq!(rows.lines().count(), 2, "one RPC and one refusal, no new start");
    assert!(!rows.contains("turn/start"));
}

#[tokio::test]
async fn canonical_wait_cancellation_after_eof_does_not_grant_dispatch() {
    let (root, mut session, control, cancel, _) = fixture("eof").await;
    session.request("begin", json!({}), &control).await.unwrap();
    let stop = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(25)).await;
        cancel.send(true).unwrap();
    });
    let result = session.wait_with_notifications(|| std::future::pending::<std::result::Result<(), String>>(), &control).await;
    session.shutdown().await.unwrap();
    stop.await.unwrap();
    assert_eq!(result.unwrap_err(), "supplied retry cancelled or deadline reached");
    assert_eq!(std::fs::read_to_string(root.join("requests.jsonl")).unwrap().lines().count(), 1);
}

#[tokio::test]
async fn live_wait_cancellation_stops_before_dispatch() {
    let (root, mut session, control, cancel, mut received) = fixture("wait").await;
    session.request("begin", json!({}), &control).await.unwrap();
    let result = session.wait_with_notifications(|| async {
        assert!(matches!(received.recv().await, Some(Event::Progress(_))));
        cancel.send(true).unwrap();
        // Even if a canonical reply becomes ready in the same poll, this
        // helper's caller still owns before_send; this fake grant sends none.
        std::future::pending::<std::result::Result<(), String>>().await
    }, &control).await;
    session.shutdown().await.unwrap();
    assert!(result.unwrap_err().contains("cancelled or deadline reached"));
    assert_eq!(std::fs::read_to_string(root.join("requests.jsonl")).unwrap().lines().count(), 1);
}

#[tokio::test]
async fn cancelled_framed_read_preserves_partial_notification_for_next_rpc() {
    let (_, mut session, control, _cancel, _) = fixture("partial").await;
    session.request("begin", json!({}), &control).await.unwrap();
    let cut = tokio::time::timeout(Duration::from_millis(100), session.next(&control, None)).await;
    assert!(cut.is_err());
    assert!(!session.frame.is_empty(), "the cut must follow a real partial framed read");
    let result = session.request("finish", json!({}), &control).await;
    let pending = session.pending.clone();
    session.shutdown().await.unwrap();
    assert_eq!(result.unwrap(), json!({}));
    assert_eq!(pending, VecDeque::from([json!({"method":"future/partial","params":{"value":"complete"}})]));
}

#[tokio::test]
async fn progress_before_lost_start_ack_remains_uncertain_and_is_not_resent() {
    let (root, mut session, control, _cancel, _) = fixture("lost").await;
    let result = session.request("turn/start", json!({}), &control).await;
    assert!(session.lifecycle.uncertain());
    assert!(session.pending.is_empty());
    session.shutdown().await.unwrap();
    assert!(result.unwrap_err().contains("executor exited before its result"));
    assert_eq!(std::fs::read_to_string(root.join("requests.jsonl")).unwrap().lines().count(), 1);
}

#[test]
fn progress_projection_whitelist_preserves_unknown_shapes() {
    let original = json!({"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"text"}});
    assert!(progress_notification(&original));
    for value in [
        {let mut v=original.clone();v["id"]=json!(1);v},
        {let mut v=original.clone();v["params"]["futureAuthority"]=json!(true);v},
        {let mut v=original.clone();v["params"]["delta"]=json!({});v},
        {let mut v=original.clone();v["params"].as_object_mut().unwrap().remove("turnId");v},
        {let mut v=original.clone();v["method"]=json!("item/completed");v},
    ] { assert!(!progress_notification(&value)); }
}
