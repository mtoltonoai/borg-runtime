//! Real SQLite writer contention at a completed mutating-tool receipt boundary.
//! No helper/model/tool is replayed to recover a lock acquisition failure.
use native_runtime::*;
use rusqlite::{Connection, TransactionBehavior};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Notify;

struct Model {
    requests: Mutex<Vec<ProviderRequest>>,
}
impl Provider for Model {
    fn respond(
        &self,
        request: ProviderRequest,
    ) -> BoxFuture<'_, Result<ProviderResponse, ProviderError>> {
        Box::pin(async move {
            let count = {
                let mut all = self.requests.lock().unwrap();
                all.push(request);
                all.len()
            };
            Ok(ProviderResponse {
                id: None,
                status: "completed".into(),
                usage: None,
                output: if count == 1 {
                    vec![
                        json!({"type":"function_call","call_id":"once","name":"mutate","arguments":"{}"}),
                    ]
                } else {
                    vec![
                        json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"receipt retained"}]}),
                    ]
                },
            })
        })
    }
}
struct ContendedTool {
    database: PathBuf,
    effects: AtomicUsize,
    entered: Notify,
    blocker: Mutex<Option<std::thread::JoinHandle<()>>>,
}
impl ToolExecutor for ContendedTool {
    fn list_tools<'a>(
        &'a self,
        _: &'a str,
        _: &'a [McpServerConfig],
    ) -> BoxFuture<'a, Result<Vec<Value>, String>> {
        Box::pin(async { Ok(vec![]) })
    }
    fn call<'a>(
        &'a self,
        _: &'a str,
        _: &'a [McpServerConfig],
        _: &'a str,
        _: Value,
        _: &'a str,
    ) -> BoxFuture<'a, Result<Value, String>> {
        Box::pin(async move {
            self.effects.fetch_add(1, Ordering::SeqCst);
            let database = self.database.clone();
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            let thread = std::thread::spawn(move || {
                let mut connection = Connection::open(database).unwrap();
                let tx = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .unwrap();
                tx.execute("UPDATE sessions SET stop_requested=stop_requested", [])
                    .unwrap();
                ready_tx.send(()).unwrap();
                // Longer than the former 5s busy_timeout: the original result
                // must remain in the same persistence operation until commit.
                std::thread::sleep(Duration::from_millis(5300));
                tx.commit().unwrap();
            });
            *self.blocker.lock().unwrap() = Some(thread);
            ready_rx.recv().unwrap();
            self.entered.notify_one();
            Ok(json!({"original_receipt":"effect committed exactly once"}))
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_writer_cannot_discard_a_completed_tool_receipt_or_hide_read_status() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("runtime.sqlite");
    let config = dir.path().join("runtime.json");
    std::fs::write(&config,serde_json::to_vec(&json!({"database_path":path,"agents":[{"id":"engineer","instructions":"work","max_turns":null}]})).unwrap()).unwrap();
    let model = Arc::new(Model {
        requests: Mutex::new(vec![]),
    });
    let tool = Arc::new(ContendedTool {
        database: path,
        effects: AtomicUsize::new(0),
        entered: Notify::new(),
        blocker: Mutex::new(None),
    });
    let runtime = Runtime::with_backends(&config, model.clone(), tool.clone()).unwrap();
    let session = runtime
        .create_session("engineer", None, "perform once")
        .unwrap();
    let running = runtime.clone();
    let id = session.id.clone();
    let driver = tokio::spawn(async move { running.run(&id).await });
    tokio::time::timeout(Duration::from_secs(5), tool.entered.notified())
        .await
        .unwrap();
    let observer = runtime.clone();
    let id = session.id.clone();
    let before = Instant::now();
    let observed = tokio::time::timeout(
        Duration::from_secs(2),
        tokio::task::spawn_blocking(move || observer.get_session(&id)),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert!(before.elapsed() < Duration::from_secs(2));
    assert_eq!(observed.status, SessionStatus::Running);
    assert!(observed.driver_running);
    let done = tokio::time::timeout(Duration::from_secs(15), driver)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(done.status, SessionStatus::Completed);
    assert_eq!(tool.effects.load(Ordering::SeqCst), 1);
    let calls = runtime.tool_calls(&session.id).unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].status, "succeeded");
    assert_eq!(
        calls[0].result,
        Some(json!({"original_receipt":"effect committed exactly once"}))
    );
    assert_eq!(
        runtime
            .transcript(&session.id)
            .unwrap()
            .iter()
            .filter(|e| e.kind == "tool_result")
            .count(),
        1
    );
    assert_eq!(model.requests.lock().unwrap().len(), 2);
    tool.blocker.lock().unwrap().take().unwrap().join().unwrap();
    drop(runtime);
    let reopened = Runtime::with_backends(&config, model.clone(), tool.clone()).unwrap();
    assert_eq!(
        reopened.run(&session.id).await.unwrap().status,
        SessionStatus::Completed
    );
    assert_eq!(model.requests.lock().unwrap().len(), 2);
    assert_eq!(tool.effects.load(Ordering::SeqCst), 1);
}

#[test]
fn deferred_read_to_write_upgrade_reproduces_busy_snapshot_ignored_by_busy_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.sqlite");
    let mut first = Connection::open(&path).unwrap();
    first.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(0)").unwrap();
    first.busy_timeout(Duration::from_secs(5)).unwrap();
    let second = Connection::open(path).unwrap();
    let transaction = first
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .unwrap();
    let _: i64 = transaction
        .query_row("SELECT value FROM counter", [], |r| r.get(0))
        .unwrap();
    second.execute("UPDATE counter SET value=1", []).unwrap();
    let before = Instant::now();
    let failure = transaction
        .execute("UPDATE counter SET value=2", [])
        .unwrap_err();
    assert_eq!(
        failure.sqlite_error().unwrap().extended_code,
        rusqlite::ffi::SQLITE_BUSY_SNAPSHOT
    );
    assert!(
        before.elapsed() < Duration::from_secs(1),
        "the ordinary busy timeout cannot repair an old read snapshot"
    );
    transaction.rollback().unwrap();
}
