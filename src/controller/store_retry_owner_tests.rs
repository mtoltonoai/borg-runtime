//! Actual owner loop and Controller admission under linked SQLite contention.
//! Timing values below are explicit fixture policy, not installed defaults.
//! No provider, repository effect, or synthetic validation proof.
#[path = "store_retry_maintenance_tests.rs"]
mod maintenance;
use super::run;
use crate::{
    collaboration::workflow::TrustedHostPath,
    controller::{Controller, ControllerConfig, Envelope, Inner, Message,
        OrdinaryMessage, OriginalRequest, database::{Database, Response}},
    limits::{RuntimeLimits, StoreRetryLimits},
    protocol::*,
    repository_catalogue::Registration,
};
use rusqlite::{Connection, hooks::Action};
use std::{
    path::PathBuf,
    sync::{Arc, mpsc, atomic::{AtomicU64, AtomicUsize, Ordering}},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Semaphore, oneshot, watch};

static NEXT: AtomicU64 = AtomicU64::new(1);
#[derive(Default)]
struct Observed { receipt_attempts: AtomicUsize, task_attempts: AtomicUsize, rollbacks: AtomicUsize }
struct Lab {
    controller: Option<Controller>,
    owner: Option<thread::JoinHandle<()>>,
    finished: mpsc::Receiver<()>,
    path: PathBuf,
    seen: Arc<Observed>,
    clock: Arc<AtomicU64>,
}
impl Lab {
    fn open(capacity: usize, backoff_ms: u64, operation_timeout_ms: u64) -> Self {
        let base = PathBuf::from(std::env::var_os("BORG_TEST_ARTIFACT").expect("owned fixture directory"));
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let directory = base.join(format!("store-retry-owner-{}-{stamp}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("tasks.db");
        let policy = StoreRetryLimits { backoff_ms, operation_timeout_ms };
        let mut limits = RuntimeLimits::seed().unwrap();
        limits.controller.queue_capacity = capacity;
        limits.controller.store_retry = Some(policy.clone());
        limits.validate().unwrap();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis().try_into().unwrap();
        let clock = Arc::new(AtomicU64::new(now));
        let held_clock = clock.clone();
        let config = ControllerConfig {
            limits: Arc::new(limits),
            validation_executable: PathBuf::from(std::env::var_os("BORG_TEST_RUNTIME").expect("qualified genuine helper")),
            clock: Arc::new(move || held_clock.load(Ordering::SeqCst)),
            ..ControllerConfig::default()
        };
        let (sender, receiver) = mpsc::channel::<Envelope>();
        let (changed, changes) = watch::channel(0);
        let (ready, startup) = mpsc::sync_channel(1);
        let (done, finished) = mpsc::sync_channel(1);
        let seen = Arc::new(Observed::default());
        let observed = seen.clone();
        let settings = config.clone();
        let database_path = path.clone();
        let owner = thread::spawn(move || {
            // Real Database startup owns a blocking runtime; keep it on its own
            // plain thread exactly as the normal Controller constructor does.
            let db = Database::open(&database_path, settings).unwrap();
            let mode: String = db.conn.query_row("PRAGMA journal_mode=DELETE", [], |r| r.get(0)).unwrap();
            assert_eq!(mode, "delete");
            let mode: String = db.conn.query_row("PRAGMA locking_mode=NORMAL", [], |r| r.get(0)).unwrap();
            assert_eq!(mode, "normal");
            db.conn.execute_batch("BEGIN EXCLUSIVE; COMMIT").unwrap();
            db.conn.busy_timeout(Duration::ZERO).unwrap();
            let writes = observed.clone();
            db.conn.update_hook(Some(move |action, database: &str, table: &str, _| {
                if action == Action::SQLITE_INSERT && database == "main" {
                    if table == "receipts" { writes.receipt_attempts.fetch_add(1, Ordering::SeqCst); }
                    if table == "tasks" { writes.task_attempts.fetch_add(1, Ordering::SeqCst); }
                }
            })).unwrap();
            db.conn.rollback_hook(Some(move || { observed.rollbacks.fetch_add(1, Ordering::SeqCst); })).unwrap();
            ready.send(db.validation_host.clone()).unwrap();
            run(db, receiver, changed, policy);
            let _ = done.send(());
        });
        let validation_host = startup.recv_timeout(Duration::from_secs(10)).expect("actual owner startup");
        let controller = Controller { inner: Arc::new(Inner {
            sender,
            pending: Some(Arc::new(Semaphore::new(capacity))),
            episodes: Default::default(),
            changes,
            waiters: Semaphore::new(config.worker_waiters),
            ui_waiters: Semaphore::new(config.ui_waiters),
            validations: Semaphore::new(config.validation_parallelism),
            #[cfg(debug_assertions)]
            finalization_probe: std::sync::Mutex::new(None),
            config,
            validation_host,
        }) };
        Self { controller: Some(controller), owner: Some(owner), finished, path, seen, clock }
    }

    fn controller(&self) -> &Controller { self.controller.as_ref().unwrap() }
    fn reader(&self) -> Connection {
        let connection = Connection::open(&self.path).unwrap();
        connection.busy_timeout(Duration::ZERO).unwrap();
        connection.execute_batch("BEGIN").unwrap();
        assert_eq!(sql_count(&connection, "receipts"), 0);
        connection
    }
    fn enqueue(&self, request: Request, invocation_path: TrustedHostPath) -> oneshot::Receiver<Response> {
        let (reply, receive) = oneshot::channel();
        let message = Message::Ordinary(OrdinaryMessage {
            original: OriginalRequest { request, invocation_path, validation: None }, reply,
        });
        if self.controller().inner.send(message).is_err() { panic!("fixture admission unexpectedly full or closed"); }
        receive
    }
    fn available(&self) -> usize {
        self.controller().inner.pending.as_ref().unwrap().available_permits()
    }
    fn close(&mut self) {
        self.controller.take();
        self.finished.recv_timeout(Duration::from_secs(3)).expect("owner must release parked work on channel closure");
        self.owner.take().unwrap().join().unwrap();
    }
}
impl Drop for Lab {
    fn drop(&mut self) {
        self.controller.take();
        if self.owner.is_some() && self.finished.recv_timeout(Duration::from_secs(3)).is_ok() {
            let _ = self.owner.take().unwrap().join();
        }
    }
}

fn request(id: &str, command: Operation) -> Request {
    Request { protocol_revision: WIRE_REVISION, request_id: id.into(), command }
}
fn register(id: &str) -> Operation {
    Operation::RegisterRepository { operation_id: id.into(), registration: Registration {
        pull_request_target: None,
        repository_id: "owner-retry".into(), registration_revision: "1".into(),
        display_name: "Original repository".into(), source_endpoint: "https://example.invalid/owner.git".into(),
        checkpoint_endpoint: "origin".into(), source_store_key: "source.git".into(), checkpoint_store_key: "checkpoint.git".into(),
    } }
}
fn sql_count(connection: &Connection, table: &str) -> i64 {
    connection.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0)).unwrap()
}
fn revision(connection: &Connection) -> i64 {
    connection.query_row("SELECT value FROM counters WHERE kind='controller_revision'", [], |r| r.get(0)).unwrap()
}
async fn until(reason: &str, condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !condition() { tokio::time::sleep(Duration::from_millis(2)).await; }
    }).await.expect(reason);
}
async fn parked(lab: &Lab) {
    until("actual receipt write and checked rollback must precede parking", ||
        lab.seen.receipt_attempts.load(Ordering::SeqCst) > 0 && lab.seen.rollbacks.load(Ordering::SeqCst) > 0).await;
}
async fn receive(receiver: oneshot::Receiver<Response>) -> Reply {
    match tokio::time::timeout(Duration::from_secs(3), receiver).await.expect("bounded owner response").expect("owner retains original reply") {
        Response::Reply(reply) => reply,
        Response::StoreFailure(failure) => panic!("scheduler exposed unhandled store failure: {:?}", failure.disposition()),
        _ => panic!("unexpected external ticket"),
    }
}
fn refused(reply: &Reply, expected: &str) {
    assert!(matches!(&reply.outcome, Outcome::Refused { code, .. } if code == expected), "{reply:?}");
    assert!(reply.commit.is_none());
}

#[tokio::test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A transient failure of a platform-owned durable-store transaction MUST be retried with metadata-controlled backoff while retaining the original operation identity.
async fn commit_contention_yields_to_reads_and_exact_duplicate_keeps_its_transport_identity() {
    let mut lab = Lab::open(8, 20, 2_000);
    let reader = lab.reader();
    let command = register("same-original");
    let first = lab.enqueue(request("first-transport", command.clone()), TrustedHostPath::OperatorApi);
    parked(&lab).await;
    let second = lab.enqueue(request("second-transport", command), TrustedHostPath::OperatorApi);
    // Ordinary ListRepositories currently finishes BEGIN IMMEDIATE with COMMIT,
    // which itself requires an exclusive lock in SQLite's DELETE mode. Use the
    // existing UI revision read to discriminate owner fairness independently of
    // that transaction's own lock dependency. after=1 returns observed reset=0.
    let observed = tokio::time::timeout(Duration::from_secs(1), lab.controller().wait_for_change(1))
        .await.expect("read must finish while original write remains blocked").unwrap();
    assert_eq!(observed.revision, 0);
    assert!(observed.changed && observed.reset);
    assert_eq!(sql_count(&reader, "receipts"), 0);
    reader.execute_batch("ROLLBACK").unwrap();
    let first = receive(first).await;
    let second = receive(second).await;
    assert_eq!(first.request_id, "first-transport");
    assert_eq!(second.request_id, "second-transport");
    assert!(matches!(first.outcome, Outcome::Ok { .. }));
    assert_eq!(first.commit, second.commit);
    assert_eq!(first.outcome, second.outcome);
    assert_eq!(first.commit.as_ref().unwrap().operation_id, "same-original");
    assert_eq!(sql_count(&reader, "receipts"), 1);
    assert_eq!(sql_count(&reader, "repository_revisions"), 1);
    assert_eq!(revision(&reader), 1);
    lab.close();
}

#[tokio::test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A transient failure of a platform-owned durable-store transaction MUST be retried with metadata-controlled backoff while retaining the original operation identity.
async fn parked_original_rejects_changed_body_and_changed_invocation_path() {
    let mut lab = Lab::open(8, 40, 2_000);
    let reader = lab.reader();
    let command = register("conflict-original");
    let first = lab.enqueue(request("kept-first", command.clone()), TrustedHostPath::OperatorApi);
    parked(&lab).await;
    let mut changed = command.clone();
    if let Operation::RegisterRepository { registration, .. } = &mut changed { registration.display_name = "Changed repository".into(); }
    let body = receive(lab.enqueue(request("changed-body", changed), TrustedHostPath::OperatorApi)).await;
    let provenance = receive(lab.enqueue(request("changed-path", command), TrustedHostPath::Browser)).await;
    refused(&body, "operation_conflict");
    refused(&provenance, "operation_conflict");
    assert_eq!(body.request_id, "changed-body");
    assert_eq!(provenance.request_id, "changed-path");
    reader.execute_batch("ROLLBACK").unwrap();
    assert!(matches!(receive(first).await.outcome, Outcome::Ok { .. }));
    let raw: String = reader.query_row("SELECT body FROM repository_revisions", [], |r| r.get(0)).unwrap();
    assert_eq!(serde_json::from_str::<Registration>(&raw).unwrap().display_name, "Original repository");
    assert_eq!(sql_count(&reader, "receipts"), 1);
    lab.close();
}

#[tokio::test]
// Traceability scope: The actual SQLite owner holds both configured queue permits across a dropped receiver and refuses the next request until settlement.
//= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
//= type=test
//# Admission MUST enforce configured queue and resource bounds.
async fn parked_and_duplicate_waiters_keep_total_capacity_after_http_receiver_drop() {
    let mut lab = Lab::open(2, 40, 2_000);
    let reader = lab.reader();
    let command = register("capacity-original");
    let abandoned = lab.enqueue(request("abandoned", command.clone()), TrustedHostPath::OperatorApi);
    parked(&lab).await;
    let duplicate = lab.enqueue(request("retained-duplicate", command), TrustedHostPath::OperatorApi);
    assert_eq!(lab.available(), 0);
    let full = lab.controller().handle(request("over-capacity", Operation::ListRepositories { after: None, limit: 1 })).await;
    refused(&full, "capacity");
    drop(abandoned);
    assert_eq!(lab.available(), 0, "transport cancellation must not release owned original or duplicate slots");
    reader.execute_batch("ROLLBACK").unwrap();
    let retained = receive(duplicate).await;
    assert_eq!(retained.request_id, "retained-duplicate");
    assert!(matches!(retained.outcome, Outcome::Ok { .. }));
    assert_eq!(sql_count(&reader, "receipts"), 1);
    until("settled waiters release their admission permits", || lab.available() == 2).await;
    lab.close();
}

#[tokio::test]
async fn actual_parked_timeout_does_not_reset_the_original_phase_lifetime() {
    let mut lab = Lab::open(4, 20, 500);
    let reader = lab.reader();
    let original = lab.enqueue(request("real-timeout", register("timeout-original")), TrustedHostPath::OperatorApi);
    parked(&lab).await;
    let result = receive(original).await;
    refused(&result, "store_unavailable");
    assert_eq!(result.request_id, "real-timeout");
    assert!(lab.seen.receipt_attempts.load(Ordering::SeqCst) >= 2, "more than one actual SQL attempt must share the same finite lifetime");
    assert_eq!(sql_count(&reader, "receipts"), 0);
    reader.execute_batch("ROLLBACK").unwrap();
    let observed = lab.controller().handle(request("after-expiry", Operation::ListRepositories { after: None, limit: 1 })).await;
    assert!(matches!(observed.outcome, Outcome::Ok { .. }));
    assert_eq!(sql_count(&reader, "repositories"), 0);
    assert_eq!(revision(&reader), 0);
    lab.close();
}

#[tokio::test]
async fn preexpired_admission_cannot_start_a_fresh_phase_budget() {
    let mut lab = Lab::open(4, 20, 1_000);
    let (reply, response) = oneshot::channel();
    let permit = lab.controller().inner.pending.as_ref().unwrap().clone().try_acquire_owned().unwrap();
    let envelope = Envelope {
        message: Message::Ordinary(OrdinaryMessage {
            original: OriginalRequest { request: request("already-expired", register("old-admission")),
                validation: None, invocation_path: TrustedHostPath::OperatorApi }, reply,
        }),
        // Fixture-controlled original admission age, not a refreshed timestamp
        // assigned by the consumer after waiting in its queue.
        admitted: Instant::now().checked_sub(Duration::from_millis(1_001)).unwrap(),
        permit: Some(permit),
        episode: None,
    };
    if lab.controller().inner.sender.send(envelope).is_err() { panic!("fixture queue unexpectedly unavailable"); }
    let result = receive(response).await;
    refused(&result, "store_unavailable");
    assert_eq!(result.request_id, "already-expired");
    assert_eq!(lab.seen.receipt_attempts.load(Ordering::SeqCst), 0);
    lab.close();
    let reader = Connection::open(&lab.path).unwrap();
    assert_eq!(sql_count(&reader, "receipts"), 0);
    assert_eq!(sql_count(&reader, "repositories"), 0);
}

#[tokio::test]
async fn queued_duplicate_retains_original_expiry_after_first_waiter_has_finished() {
    let mut lab = Lab::open(4, 20, 1_500);
    let reader = lab.reader();
    let command = register("shared-episode-original");
    let first = lab.enqueue(request("original-waiter", command.clone()), TrustedHostPath::OperatorApi);
    parked(&lab).await;

    // Keep this admitted duplicate outside the owner channel. This exact seam
    // fixes its queue position without depending on a racing dequeue, while its
    // real permit and episode remain live throughout the original's timeout.
    tokio::time::sleep(Duration::from_millis(750)).await;
    let admitted = Instant::now();
    let (reply, response) = oneshot::channel();
    let message = Message::Ordinary(OrdinaryMessage {
        original: OriginalRequest { request: request("late-dequeued-duplicate", command),
            validation: None, invocation_path: TrustedHostPath::OperatorApi }, reply,
    });
    let inner = &lab.controller().inner;
    let permit = inner.pending.as_ref().unwrap().clone().try_acquire_owned().unwrap();
    let policy = inner.config.store_retry.as_ref().unwrap();
    let episode = inner.episodes.admit(&message, admitted, policy).expect("mutation must retain its admitted episode");
    let own_expiry = admitted.checked_add(Duration::from_millis(policy.operation_timeout_ms)).unwrap();
    let held = Envelope { message, admitted, permit: Some(permit), episode: Some(episode) };

    let expired = receive(first).await;
    refused(&expired, "store_unavailable");
    assert_eq!(expired.request_id, "original-waiter");
    assert!(Instant::now() < own_expiry, "the duplicate's separate timestamp must still allow a fresh attempt if incorrectly used");
    let attempts = lab.seen.receipt_attempts.load(Ordering::SeqCst);
    let rollbacks = lab.seen.rollbacks.load(Ordering::SeqCst);
    assert!(attempts > 0 && rollbacks > 0);
    reader.execute_batch("ROLLBACK").unwrap();
    if inner.sender.send(held).is_err() { panic!("held duplicate must fit its reserved admission slot"); }
    let duplicate = receive(response).await;
    refused(&duplicate, "store_unavailable");
    assert_eq!(duplicate.request_id, "late-dequeued-duplicate");
    assert_eq!(lab.seen.receipt_attempts.load(Ordering::SeqCst), attempts,
        "dequeue after original expiry must not enter a second SQL phase");
    assert_eq!(lab.seen.rollbacks.load(Ordering::SeqCst), rollbacks);
    assert_eq!(sql_count(&reader, "receipts"), 0);
    assert_eq!(sql_count(&reader, "repositories"), 0);
    assert_eq!(revision(&reader), 0);
    until("expired original and queued duplicate release both permits", || lab.available() == 4).await;
    lab.close();
}

#[tokio::test]
async fn owner_shutdown_releases_parked_original_without_waiting_or_retrying() {
    let mut lab = Lab::open(4, 60_000, 120_000);
    let reader = lab.reader();
    let original = lab.enqueue(request("shutdown-original", register("shutdown-operation")), TrustedHostPath::OperatorApi);
    parked(&lab).await;
    assert_eq!(lab.seen.receipt_attempts.load(Ordering::SeqCst), 1);
    lab.close();
    let result = receive(original).await;
    refused(&result, "store_unavailable");
    assert_eq!(result.request_id, "shutdown-original");
    assert_eq!(lab.seen.receipt_attempts.load(Ordering::SeqCst), 1);
    reader.execute_batch("ROLLBACK").unwrap();
    assert_eq!(sql_count(&reader, "receipts"), 0);
    assert_eq!(revision(&reader), 0);
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn native_submission_finalization_rechecks_original_deadline_after_real_helper_and_sql_rollback() {
    let mut lab = Lab::open(8, 40, 3_000);
    let mut input = TaskInput { instruction_source: None,
        pull_request: None,
        resolved_limits: None, repository_read: None, instruction: "produce one structured answer".into(),
        definition: Definition { charter: None, limit_overrides: None,
            execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
            inference: Some(InferenceSettings::selected()), id: "owner-proof-agent".into(), revision: "1".into(), executor: ExecutorKind::Native },
        repository: None, acceptance_commands: vec![],
        output_schema: Some(serde_json::json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false})),
        limits: lab.controller().limits().task_defaults.at(lab.clock.load(Ordering::SeqCst)).unwrap(),
        dependencies: vec![], native_workflow: None,
    };
    input.limits.deadline_unix_ms = lab.clock.load(Ordering::SeqCst) + 30_000;
    let deadline = input.limits.deadline_unix_ms;
    let (ready, release) = lab.controller().qualification_pause_next_finalization();
    let controller = lab.controller().clone();
    let result = tokio::spawn(async move { Box::pin(controller.handle(request("native-phase", Operation::SubmitTask {
        operation_id: "native-phase-original".into(), input,
    }))).await });
    tokio::time::timeout(Duration::from_secs(10), ready).await.expect("genuine local validation helper must complete").unwrap();
    let reader = lab.reader();
    // Startup finished before these hooks, and ready follows the helper's
    // preflight rollback. With no committed task or other queued operation,
    // maintenance only SELECTs the absent deadline. The next owner rollback
    // therefore belongs to this finalization, after its receipt INSERT.
    let preflight_rollbacks = lab.seen.rollbacks.load(Ordering::SeqCst);
    release.send(()).unwrap();
    until("this finalization COMMIT must fail and explicitly roll back", ||
        lab.seen.receipt_attempts.load(Ordering::SeqCst) > 0
            && lab.seen.rollbacks.load(Ordering::SeqCst) > preflight_rollbacks).await;
    assert!(lab.seen.task_attempts.load(Ordering::SeqCst) > 0,
        "genuine helper proof must permit task insertion before the failed commit is rolled back");
    lab.clock.store(deadline + 1, Ordering::SeqCst);
    reader.execute_batch("ROLLBACK").unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(3), result).await.expect("finalization must settle under its original deadline").unwrap();
    assert_eq!(reply.request_id, "native-phase");
    assert!(matches!(&reply.outcome, Outcome::Refused { code, message }
        if code == "invalid_input" && message == "task deadline is expired or unsupported"), "{reply:?}");
    assert_eq!(reply.commit.as_ref().unwrap().operation_id, "native-phase-original");
    assert_eq!(sql_count(&reader, "tasks"), 0);
    assert_eq!(sql_count(&reader, "receipts"), 1);
    assert_eq!(revision(&reader), 1);
    lab.close();
}
