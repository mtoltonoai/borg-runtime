// Protected SQL disposition only: real Controller-produced release/check/action
// records, linked SQLite failures, explicit later SQL calls and one actual owner
// retry/duplicate case. No repository
// adapter, runtime exchange, merge, rollout, or positive external effect is run.
use super::Fixture;
use super::super::{ProtectedFailure, ProtectedResponse, Recorded};
use crate::{
    controller::{
        checked_transaction::{Phase, RetryDisposition, Rollback, SqliteCode},
        database::{Database, Fault},
    },
    protected_release::{Action, Command, Reply, Request, State, REVISION},
};
use rusqlite::{
    Connection as SqlConnection, ErrorCode, ffi,
    hooks::{AuthAction, AuthContext, Authorization, TransactionOperation},
};
use std::{path::PathBuf, sync::{Arc, atomic::{AtomicUsize, Ordering}}, time::Duration};

const REPOSITORY_UID: u32 = 9001;
const ACTION: &str = "retry-action";

struct Lab { db: Database, path: PathBuf }
impl Lab {
    fn open(prepared: bool) -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let (path, config) = runtime.block_on(async {
            let fixture = Fixture::new();
            if prepared {
                let subject = Box::pin(fixture.seed()).await;
                fixture.checks(&subject).await;
                assert_eq!(fixture.merge(&subject, ACTION).await.status, "ok");
            }
            let Fixture { controller, path, config, clock: _ } = fixture;
            drop(controller);
            let mut lock_path = path.as_os_str().to_os_string();
            lock_path.push(".controller-lock");
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let lock = std::fs::OpenOptions::new().read(true).write(true).open(&lock_path).unwrap();
                    if lock.try_lock().is_ok() { break; }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }).await.expect("the original Controller must drain and release its real owner lock");
            (path, config)
        });
        drop(runtime);
        // Database::open owns bounded blocking startup work; never enter it from
        // the async fixture runtime. Existing immutable provenance is validated.
        let db = Database::open(&path, config).unwrap();
        Self { db, path }
    }

    fn allow_contention(&self) {
        // Test connection only. A retained reader in DELETE mode blocks COMMIT
        // after the body, while NORMAL releases the exclusive startup lock.
        let mode: String = self.db.conn.query_row("PRAGMA journal_mode=DELETE", [], |r| r.get(0)).unwrap();
        assert_eq!(mode, "delete");
        let mode: String = self.db.conn.query_row("PRAGMA locking_mode=NORMAL", [], |r| r.get(0)).unwrap();
        assert_eq!(mode, "normal");
        self.db.conn.execute_batch("BEGIN EXCLUSIVE; COMMIT").unwrap();
        self.db.conn.busy_timeout(Duration::ZERO).unwrap();
    }

    fn contender(&self) -> SqlConnection {
        let connection = SqlConnection::open(&self.path).unwrap();
        connection.busy_timeout(Duration::ZERO).unwrap();
        connection
    }

    fn position(&self) -> (i64, i64) {
        (position(&self.db.conn), self.db.conn.query_row("SELECT count(*) FROM protected_receipts", [], |r| r.get(0)).unwrap())
    }

    fn action(&self) -> Action {
        let body: String = self.db.conn.query_row("SELECT body FROM protected_actions WHERE id=?1", [ACTION], |r| r.get(0)).unwrap();
        serde_json::from_str(&body).unwrap()
    }
}

fn position(connection: &SqlConnection) -> i64 {
    connection.query_row("SELECT value FROM counters WHERE kind='controller_revision'", [], |r| r.get(0)).unwrap()
}
fn hold_read(connection: &SqlConnection) {
    connection.execute_batch("BEGIN").unwrap();
    let _ = position(connection);
}
fn request(id: &str, command: Command) -> Request {
    Request { revision: REVISION, operation_id: id.into(), command }
}
fn execute(id: &str) -> Request {
    request(id, Command::ExecuteMerge { action_id: ACTION.into() })
}
fn failure(result: (ProtectedResponse, bool)) -> Box<ProtectedFailure> {
    assert!(!result.1, "failed protected SQL cannot publish a change");
    match result.0 {
        ProtectedResponse::StoreFailure(failure) => failure,
        ProtectedResponse::Reply(reply) => panic!("typed protected failure was lost: {reply:?}"),
    }
}
fn reply(result: (ProtectedResponse, bool)) -> (Reply, bool) {
    match result.0 {
        ProtectedResponse::Reply(reply) => (reply, result.1),
        ProtectedResponse::StoreFailure(failure) => panic!("unexpected failure: {:?}", failure.disposition()),
    }
}
fn acknowledge_transient(failure: &ProtectedFailure) {
    assert_eq!(failure.disposition(), RetryDisposition::RolledBackTransient {
        phase: Phase::Commit,
        code: SqliteCode { primary: ErrorCode::DatabaseBusy, extended: ffi::SQLITE_BUSY },
    });
    assert!(matches!(&failure.cause, Fault::Transaction(cause) if matches!(cause.rollback, Rollback::Acknowledged)));
}
fn clear_authorizer(connection: &SqlConnection) {
    connection.authorizer(None::<fn(AuthContext<'_>) -> Authorization>).unwrap();
}

#[test]
fn begin_failure_retains_peer_and_full_internal_observation_without_normalization() {
    let mut lab = Lab::open(false);
    lab.allow_contention();
    let blocker = lab.contender();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let original = request("readback-original", Command::ReconcileObservation {
        action_id: ACTION.into(), state: State::Unknown,
        observation: "controlled unavailable readback; no external effect asserted".into(),
    });
    let failed = failure(lab.db.protected_observed(REPOSITORY_UID, original.clone()));
    assert_eq!(failed.uid, REPOSITORY_UID);
    assert_eq!(failed.request, original);
    assert_eq!(failed.disposition(), RetryDisposition::BeginNotStartedTransient {
        code: SqliteCode { primary: ErrorCode::DatabaseBusy, extended: ffi::SQLITE_BUSY },
    });
    assert!(matches!(&failed.cause, Fault::Transaction(cause) if matches!(cause.rollback, Rollback::NotStarted)));
    let fallback = (*failed).into_reply();
    assert_eq!(fallback, Reply::refused("store_unavailable_reconcile_original"));
    let (legacy, changed) = lab.db.protected(REPOSITORY_UID, original);
    assert_eq!(legacy, fallback);
    assert!(!changed);
    assert!(lab.db.conn.is_autocommit());
    assert!(!blocker.is_autocommit());
    blocker.execute_batch("ROLLBACK").unwrap();
    assert_eq!(lab.position(), (0, 0));
}

#[test]
fn commit_rollback_preserves_action_then_exact_execute_replay_is_not_a_second_grant() {
    let mut lab = Lab::open(true);
    let baseline = lab.position();
    let prepared = lab.action();
    assert_eq!(prepared.state, State::Prepared);
    lab.allow_contention();
    let reader = lab.contender();
    hold_read(&reader);
    let original = execute("execute-original");
    let failed = failure(lab.db.protected_observed(REPOSITORY_UID, original.clone()));
    acknowledge_transient(&failed);
    assert_eq!(failed.uid, REPOSITORY_UID);
    assert_eq!(failed.request, original);
    assert!(lab.db.conn.is_autocommit());
    assert_eq!(lab.action(), prepared);
    assert_eq!(lab.position(), baseline);
    reader.execute_batch("ROLLBACK").unwrap();
    assert_eq!(lab.action(), prepared, "unlocking does not dispatch or retry");
    let ProtectedFailure { uid, request: retained, .. } = *failed;
    let (first, changed) = reply(lab.db.protected_observed(uid, retained));
    assert_eq!(first.status, "ok");
    assert!(!first.replayed);
    assert!(changed);
    assert_eq!(lab.action().state, State::Started);
    assert_eq!(lab.position(), (baseline.0 + 1, baseline.1 + 1));
    let (replayed, changed) = reply(lab.db.protected_observed(REPOSITORY_UID, original));
    assert!(!changed);
    assert_eq!(replayed.status, "ok");
    assert!(replayed.replayed);
    assert_eq!(replayed.data, first.data);
    assert!(!(replayed.status == "ok" && !replayed.replayed),
        "the existing ExecuteMerge adapter gate must not authorize another execution");
    assert_eq!(lab.position(), (baseline.0 + 1, baseline.1 + 1));
    assert_eq!(lab.action().state, State::Started,
        "this boundary test grants dispatch but performs no external merge");
}

#[test]
fn observation_retry_retains_original_state_but_receipt_replay_keeps_normalized_identity() {
    let mut lab = Lab::open(true);
    let (started, _) = reply(lab.db.protected_observed(REPOSITORY_UID, execute("start-for-observation")));
    assert_eq!(started.status, "ok");
    let baseline = lab.position();
    lab.allow_contention();
    let reader = lab.contender();
    hold_read(&reader);
    let original = request("observation-original", Command::ReconcileObservation {
        action_id: ACTION.into(), state: State::Unknown,
        observation: "controlled unavailable readback; no external effect asserted".into(),
    });
    let failed = failure(lab.db.protected_observed(REPOSITORY_UID, original.clone()));
    acknowledge_transient(&failed);
    assert_eq!(failed.uid, REPOSITORY_UID);
    assert_eq!(failed.request, original);
    assert_eq!(lab.action().state, State::Started);
    assert_eq!(lab.position(), baseline);
    reader.execute_batch("ROLLBACK").unwrap();
    let ProtectedFailure { uid, request: retained, .. } = *failed;
    let (first, changed) = reply(lab.db.protected_observed(uid, retained));
    assert_eq!(first.status, "ok");
    assert!(changed);
    assert!(!first.replayed);
    assert_eq!(lab.action().state, State::Unknown);
    let (producer, stored, outcome): (String, String, String) = lab.db.conn.query_row(
        "SELECT producer,request,reply FROM protected_receipts WHERE id='observation-original'", [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
    let record = Recorded { producer: serde_json::from_str(&producer).unwrap(),
        request: serde_json::from_str(&stored).unwrap(), reply: serde_json::from_str(&outcome).unwrap() };
    let normalized = request("observation-original", Command::ReconcileMerge { action_id: ACTION.into() });
    assert_eq!(record.request, normalized);
    assert_eq!(record.producer.uid, REPOSITORY_UID);
    assert_eq!(record.reply, first);
    let (replayed, changed) = reply(lab.db.protected_observed(REPOSITORY_UID, normalized));
    assert!(!changed);
    assert!(replayed.replayed);
    assert_eq!(replayed.data, first.data);
    let (foreign, changed) = reply(lab.db.protected_observed(9000, original));
    assert!(!changed);
    assert_eq!(foreign.status, "operation_conflict");
    assert_eq!(lab.position(), (baseline.0 + 1, baseline.1 + 1));
    assert_eq!(lab.action().state, State::Unknown);
}

#[test]
fn reads_and_exact_replays_require_actual_rollback_acknowledgement() {
    let mut lab = Lab::open(true);
    let original = execute("execute-before-read");
    let (first, _) = reply(lab.db.protected_observed(REPOSITORY_UID, original.clone()));
    assert_eq!(first.status, "ok");
    let baseline = lab.position();
    for req in [request("action-read", Command::GetAction { action_id: ACTION.into() }), original] {
        let rollbacks = Arc::new(AtomicUsize::new(0));
        let seen = rollbacks.clone();
        lab.db.conn.authorizer(Some(move |context: AuthContext<'_>| {
            if matches!(context.action, AuthAction::Transaction { operation: TransactionOperation::Rollback }) {
                seen.fetch_add(1, Ordering::SeqCst);
                Authorization::Deny
            } else { Authorization::Allow }
        })).unwrap();
        let failed = failure(lab.db.protected_observed(REPOSITORY_UID, req.clone()));
        assert_eq!(failed.request, req);
        assert_eq!(failed.disposition(), RetryDisposition::Ineligible);
        assert!(matches!(&failed.cause, Fault::Transaction(cause)
            if cause.phase == Phase::Rollback && matches!(cause.rollback, Rollback::Unproven { autocommit: false, .. })));
        assert_eq!(rollbacks.load(Ordering::SeqCst), 1, "no unchecked Drop retry");
        assert!(!lab.db.conn.is_autocommit());
        assert_eq!((*failed).into_reply().status, "store_unavailable_reconcile_original");
        clear_authorizer(&lab.db.conn);
        lab.db.conn.execute_batch("ROLLBACK").unwrap();
        let (recovered, changed) = reply(lab.db.protected_observed(REPOSITORY_UID, req.clone()));
        assert_eq!(recovered.status, "ok");
        assert!(!changed);
        assert_eq!(recovered.replayed, matches!(req.command, Command::ExecuteMerge { .. }));
        assert_eq!(lab.position(), baseline);
    }
}

#[test]
fn permanent_and_unproven_failures_cannot_grant_protected_execution() {
    let mut lab = Lab::open(true);
    let baseline = lab.position();
    let prepared = lab.action();
    lab.db.conn.execute_batch("CREATE TEMP TRIGGER refuse_protected_receipt BEFORE INSERT ON protected_receipts BEGIN SELECT RAISE(ABORT,'controlled permanent protected receipt refusal'); END;").unwrap();
    let failed = failure(lab.db.protected_observed(REPOSITORY_UID, execute("permanent-execute")));
    assert_eq!(failed.disposition(), RetryDisposition::Ineligible);
    assert!(matches!(&failed.cause, Fault::Transaction(cause)
        if cause.phase == Phase::Body && matches!(cause.rollback, Rollback::Acknowledged)
            && cause.original_sqlite_code().is_some_and(|code| code.primary == ErrorCode::ConstraintViolation)));
    assert_eq!((*failed).into_reply().status, "store_unavailable_reconcile_original");
    assert!(lab.db.conn.is_autocommit());
    assert_eq!(lab.action(), prepared);
    assert_eq!(lab.position(), baseline);
    lab.db.conn.execute_batch("DROP TRIGGER refuse_protected_receipt").unwrap();
    let commits = Arc::new(AtomicUsize::new(0));
    let seen = commits.clone();
    lab.db.conn.commit_hook(Some(move || { seen.fetch_add(1, Ordering::SeqCst); true })).unwrap();
    let failed = failure(lab.db.protected_observed(REPOSITORY_UID, execute("unproven-execute")));
    assert_eq!(commits.load(Ordering::SeqCst), 1);
    assert_eq!(failed.disposition(), RetryDisposition::Ineligible);
    assert!(matches!(&failed.cause, Fault::Transaction(cause)
        if cause.phase == Phase::Commit && matches!(cause.rollback, Rollback::Unproven { autocommit: true, .. })));
    assert_eq!((*failed).into_reply().status, "store_unavailable_reconcile_original");
    lab.db.conn.commit_hook(None::<fn() -> bool>).unwrap();
    assert!(lab.db.conn.is_autocommit());
    assert_eq!(lab.action(), prepared);
    assert_eq!(lab.position(), baseline);
}

#[test]
fn replay_only_requires_the_original_receipt_and_cannot_prepare_or_dispatch() {
    let mut lab = Lab::open(true);
    let baseline = lab.position();
    let prepared = lab.action();
    let original = execute("parked-execute");
    let prepare = request("parked-prepare", Command::Prepare {
        kind: prepared.kind.clone(), subject: prepared.subject.clone(),
        check_id: prepared.check_id.clone(), grant_id: prepared.grant_id.clone(),
    });
    let read = request("parked-read", Command::GetAction { action_id: ACTION.into() });
    for req in [original.clone(), prepare, read] {
        let (refused, changed) = reply(lab.db.protected_replay_observed(REPOSITORY_UID, req));
        assert_eq!(refused, Reply::refused("store_unavailable_reconcile_original"));
        assert!(!changed);
        assert!(lab.db.conn.is_autocommit());
        assert_eq!(lab.position(), baseline);
        assert_eq!(lab.action(), prepared);
        assert_eq!(lab.db.conn.query_row("SELECT count(*) FROM protected_actions", [], |r| r.get::<_, i64>(0)).unwrap(), 1);
    }

    let (first, changed) = reply(lab.db.protected_observed(REPOSITORY_UID, original.clone()));
    assert_eq!(first.status, "ok");
    assert!(!first.replayed);
    assert!(changed);
    let (replayed, changed) = reply(lab.db.protected_replay_observed(REPOSITORY_UID, original.clone()));
    assert_eq!(replayed.status, "ok");
    assert!(replayed.replayed);
    assert_eq!(replayed.data, first.data);
    assert!(!changed);
    let (foreign, changed) = reply(lab.db.protected_replay_observed(9000, original));
    assert_eq!(foreign.status, "operation_conflict");
    assert!(!changed);
    assert_eq!(lab.action().state, State::Started);
    assert_eq!(lab.position(), (baseline.0 + 1, baseline.1 + 1));
}

#[test]
fn owner_loop_duplicate_execute_merge_gets_canonical_replay_after_commit_contention() {
    use crate::controller::{Envelope, Message, store_retry::{self, Episodes}};
    use crate::limits::StoreRetryLimits;
    use rusqlite::hooks::Action as SqlAction;
    use std::{sync::mpsc, thread, time::Instant};
    use tokio::sync::{Semaphore, oneshot, watch};

    fn receive<T>(mut receiver: oneshot::Receiver<T>) -> T {
        let end = Instant::now() + Duration::from_secs(3);
        loop {
            match receiver.try_recv() {
                Ok(value) => return value,
                Err(oneshot::error::TryRecvError::Closed) => panic!("owner dropped protected response"),
                Err(oneshot::error::TryRecvError::Empty) => {
                    assert!(Instant::now() < end, "bounded protected owner response");
                    thread::sleep(Duration::from_millis(2));
                }
            }
        }
    }

    let mut lab = Lab::open(true);
    let baseline = lab.position();
    assert_eq!(lab.action().state, State::Prepared);
    lab.allow_contention();
    let reader = lab.contender();
    hold_read(&reader);
    // Explicit fixture metadata; no installed retry values or external merge.
    let policy = StoreRetryLimits { backoff_ms: 20, operation_timeout_ms: 3_000 };
    let mut limits = (*lab.db.config.limits).clone();
    limits.controller.queue_capacity = 4;
    limits.controller.store_retry = Some(policy.clone());
    limits.validate().unwrap();
    lab.db.config.limits = Arc::new(limits);
    let writes = Arc::new(AtomicUsize::new(0));
    let seen = writes.clone();
    lab.db.conn.update_hook(Some(move |action, database: &str, table: &str, _| {
        if action == SqlAction::SQLITE_INSERT && database == "main" && table == "protected_receipts" {
            seen.fetch_add(1, Ordering::SeqCst);
        }
    })).unwrap();
    let (rolled_back, rollback) = mpsc::channel();
    lab.db.conn.rollback_hook(Some(move || { let _ = rolled_back.send(()); })).unwrap();
    let (sender, receiver) = mpsc::sync_channel(4);
    let (changed, _changes) = watch::channel(0);
    let (done, finished) = mpsc::sync_channel(1);
    let Lab { db, path: _ } = lab;
    let owner_policy = policy.clone();
    let owner = thread::spawn(move || {
        store_retry::run(db, receiver, changed, owner_policy);
        let _ = done.send(());
    });
    let permits = Arc::new(Semaphore::new(4));
    let episodes = Episodes::default();
    let enqueue = |message: Message| {
        let admitted = Instant::now();
        let episode = episodes.admit(&message, admitted, &policy);
        let permit = permits.clone().try_acquire_owned().unwrap();
        let envelope = Envelope { message, admitted, episode, permit: Some(permit) };
        if sender.try_send(envelope).is_err() { panic!("protected fixture admission unexpectedly unavailable"); }
    };
    let original = execute("owner-execute-original");
    let (first_reply, first_receive) = oneshot::channel();
    enqueue(Message::Protected { uid: REPOSITORY_UID, request: original.clone(), reply: first_reply });
    rollback.recv_timeout(Duration::from_secs(3)).expect("real linked SQLite COMMIT must fail and roll back before retry");
    assert!(writes.load(Ordering::SeqCst) > 0, "protected receipt insertion must precede rollback");
    let (second_reply, second_receive) = oneshot::channel();
    enqueue(Message::Protected { uid: REPOSITORY_UID, request: original, reply: second_reply });
    // FIFO owner handling of this read proves the duplicate was admitted and
    // processed while the original remained parked behind the reader's lock.
    let (barrier_reply, barrier_receive) = oneshot::channel();
    enqueue(Message::Revision { reply: barrier_reply });
    assert_eq!(receive(barrier_receive).unwrap(), u64::try_from(baseline.0).unwrap());
    reader.execute_batch("ROLLBACK").unwrap();
    let first = receive(first_receive);
    let second = receive(second_receive);
    assert_eq!(first.status, "ok");
    assert!(!first.replayed, "only the original waiter receives a fresh ExecuteMerge grant");
    assert_eq!(second.status, "ok");
    assert!(second.replayed, "duplicate must read its canonical receipt rather than copy the fresh grant");
    assert_eq!(second.data, first.data);
    let stored: String = reader.query_row("SELECT reply FROM protected_receipts WHERE id='owner-execute-original'", [], |r| r.get(0)).unwrap();
    assert_eq!(serde_json::from_str::<Reply>(&stored).unwrap(), first);
    assert_eq!(position(&reader), baseline.0 + 1);
    assert_eq!(reader.query_row("SELECT count(*) FROM protected_receipts", [], |r| r.get::<_, i64>(0)).unwrap(), baseline.1 + 1);
    assert_eq!(reader.query_row("SELECT count(*) FROM protected_actions", [], |r| r.get::<_, i64>(0)).unwrap(), 1);
    let body: String = reader.query_row("SELECT body FROM protected_actions WHERE id=?1", [ACTION], |r| r.get(0)).unwrap();
    assert_eq!(serde_json::from_str::<Action>(&body).unwrap().state, State::Started);
    drop(sender);
    finished.recv_timeout(Duration::from_secs(3)).expect("protected owner must close after bounded work");
    owner.join().unwrap();
    assert_eq!(permits.available_permits(), 4);
}
