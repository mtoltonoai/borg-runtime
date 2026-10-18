//! Actual Database boundary tests for the typed disposition primitive only.
//! Explicit test re-entry is not an automatic retry policy. All contention is
//! produced by linked SQLite; no Failure, receipt, or validation proof is forged.
use super::{
    ControllerConfig, ExternalProof, OriginalRequest,
    checked_transaction::{Phase, RetryDisposition, SqliteCode},
    database::{Database, Response, StoreFailure},
};
use crate::{
    collaboration::workflow::TrustedHostPath,
    limits::RuntimeLimits,
    protocol::*,
    repository_catalogue::Registration,
};
use rusqlite::{Connection, ErrorCode, ffi};
use serde_json::json;
use std::{
    path::PathBuf,
    sync::{Arc, atomic::{AtomicU64, AtomicUsize, Ordering}},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

static NEXT: AtomicU64 = AtomicU64::new(1);
const NOW: u64 = 1_800_000_000_000;

struct Area(PathBuf);
impl Area {
    fn new() -> Self {
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let base = std::env::var_os("BORG_TEST_ARTIFACT").map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let path = base.join(format!("borg-store-disposition-{}-{stamp}-{}",
            std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn open(&self) -> Database {
        let config = ControllerConfig {
            limits: Arc::new(RuntimeLimits::seed().unwrap()),
            validation_executable: PathBuf::from(std::env::var_os("BORG_TEST_RUNTIME")
                .expect("qualified local helper must be supplied explicitly")),
            clock: Arc::new(|| NOW),
            ..ControllerConfig::default()
        };
        Database::open(&self.0.join("tasks.db"), config).unwrap()
    }

    fn contender(&self) -> Connection {
        let connection = Connection::open(self.0.join("tasks.db")).unwrap();
        connection.busy_timeout(Duration::ZERO).unwrap();
        connection
    }
}
impl Drop for Area {
    fn drop(&mut self) {
        if std::env::var_os("BORG_TEST_ARTIFACT").is_none() {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

fn contendible(db: &Database) {
    // Only this isolated fixture changes journal/lock mode after actual startup.
    // DELETE mode makes a retained reader block COMMIT, not BEGIN IMMEDIATE.
    let journal: String = db.conn.query_row("PRAGMA journal_mode=DELETE", [], |r| r.get(0)).unwrap();
    assert_eq!(journal, "delete");
    let locking: String = db.conn.query_row("PRAGMA locking_mode=NORMAL", [], |r| r.get(0)).unwrap();
    assert_eq!(locking, "normal");
    db.conn.execute_batch("BEGIN EXCLUSIVE; COMMIT").unwrap();
    db.conn.busy_timeout(Duration::ZERO).unwrap();
}

fn original(id: &str, command: Operation, invocation_path: TrustedHostPath) -> OriginalRequest {
    OriginalRequest {
        request: Request { protocol_revision: WIRE_REVISION, request_id: id.into(), command },
        validation: None,
        invocation_path,
    }
}

fn registration(operation_id: &str) -> Operation {
    Operation::RegisterRepository {
        operation_id: operation_id.into(),
        registration: Registration {
            pull_request_target: None,
            repository_id: "retry-fixture".into(),
            registration_revision: "1".into(),
            display_name: "Retry fixture".into(),
            source_endpoint: "https://example.invalid/retry.git".into(),
            checkpoint_endpoint: "origin".into(),
            source_store_key: "source.git".into(),
            checkpoint_store_key: "checkpoint.git".into(),
        },
    }
}

fn failure(result: (Response, bool)) -> Box<StoreFailure> {
    assert!(!result.1, "a failed transaction must not publish a change");
    match result.0 {
        Response::StoreFailure(failure) => failure,
        Response::Reply(reply) => panic!("typed store failure was flattened: {reply:?}"),
        _ => panic!("unexpected external ticket"),
    }
}

fn reply(result: (Response, bool)) -> (Reply, bool) {
    match result.0 {
        Response::Reply(reply) => (reply, result.1),
        Response::StoreFailure(failure) => panic!("unexpected store failure: {:?}", failure.disposition()),
        _ => panic!("explicit re-entry requested a second external proof"),
    }
}

fn candidate(failure: Box<StoreFailure>) -> OriginalRequest {
    assert_eq!(failure.disposition(), RetryDisposition::RolledBackTransient {
        phase: Phase::Commit,
        code: SqliteCode { primary: ErrorCode::DatabaseBusy, extended: ffi::SQLITE_BUSY },
    });
    match (*failure).into_retry_candidate() {
        Ok(original) => original,
        Err(failure) => panic!("acknowledged transient was not retained: {:?}", failure.disposition()),
    }
}

fn count(connection: &Connection, table: &str) -> i64 {
    connection.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0)).unwrap()
}

fn revision(connection: &Connection) -> i64 {
    connection.query_row("SELECT value FROM counters WHERE kind='controller_revision'", [], |r| r.get(0)).unwrap()
}

fn hold_read(connection: &Connection) {
    connection.execute_batch("BEGIN").unwrap();
    let _ = revision(connection); // Materialize and retain the real shared lock.
}

fn refused(reply: &Reply, expected: &str) {
    assert!(matches!(&reply.outcome, Outcome::Refused { code, .. } if code == expected), "{reply:?}");
}

#[test]
fn busy_begin_is_not_a_candidate_and_preserves_both_legacy_fallbacks() {
    let area = Area::new();
    let mut db = area.open();
    contendible(&db);
    let blocker = area.contender();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let commands = [registration("begin-registration"), Operation::ReconcileResult(ReconcileResultInput {
        operation_id: "begin-reconcile".into(), original_report_id: "original-report".into(),
    })];
    for (index, command) in commands.into_iter().enumerate() {
        let correlation = format!("begin-{index}");
        let failed = failure(db.handle(original(&correlation, command, TrustedHostPath::OperatorApi)));
        assert_eq!(failed.disposition(), RetryDisposition::BeginNotStartedTransient {
            code: SqliteCode { primary: ErrorCode::DatabaseBusy, extended: ffi::SQLITE_BUSY },
        });
        let retained = match (*failed).into_retry_candidate() {
            Err(retained) => retained,
            Ok(_) => panic!("BEGIN without an owned transaction must not grant re-entry"),
        };
        let fallback = retained.into_reply();
        assert_eq!(fallback.request_id, correlation);
        assert!(fallback.commit.is_none());
        assert!(fallback.observed_controller_revision.is_none());
        if index == 0 { refused(&fallback, "store_unavailable"); }
        else { assert!(matches!(fallback.outcome, Outcome::Pending { operation_id } if operation_id == "begin-reconcile")); }
        assert!(db.conn.is_autocommit());
        assert!(!blocker.is_autocommit(), "the other owner's transaction is untouched");
    }
    blocker.execute_batch("ROLLBACK").unwrap();
    assert_eq!(revision(&db.conn), 0);
    assert_eq!(count(&db.conn, "receipts"), 0);
    assert_eq!(count(&db.conn, "repositories"), 0);
}

#[test]
fn explicit_candidate_reentry_replays_one_receipt_and_rechecks_changed_state() {
    let area = Area::new();
    let mut db = area.open();
    contendible(&db);
    let reader = area.contender();
    hold_read(&reader);
    let request = original("register-exact", registration("register-original"), TrustedHostPath::ControllerMaintenance);
    let exact = serde_json::to_value(&request.request).unwrap();
    let retained = candidate(failure(db.handle(request)));
    assert_eq!(serde_json::to_value(&retained.request).unwrap(), exact);
    assert_eq!(retained.invocation_path, TrustedHostPath::ControllerMaintenance);
    assert!(retained.validation.is_none());
    assert!(db.conn.is_autocommit());
    assert_eq!(revision(&db.conn), 0);
    assert_eq!(count(&db.conn, "repositories"), 0);
    assert_eq!(count(&db.conn, "repository_revisions"), 0);
    assert_eq!(count(&db.conn, "receipts"), 0);
    reader.execute_batch("ROLLBACK").unwrap();
    assert_eq!(revision(&db.conn), 0, "unlocking alone must not retry");
    let (committed, changed) = reply(db.handle(retained));
    assert!(changed);
    assert!(matches!(committed.outcome, Outcome::Ok { .. }));
    assert_eq!(committed.commit.as_ref().unwrap().operation_id, "register-original");
    assert_eq!(committed.commit.as_ref().unwrap().controller_revision, 1);
    let (replayed, changed) = reply(db.handle(original("register-replay", registration("register-original"), TrustedHostPath::ControllerMaintenance)));
    assert!(!changed);
    assert_eq!(replayed.commit, committed.commit);
    assert_eq!(replayed.outcome, committed.outcome);
    assert_eq!(revision(&db.conn), 1);
    assert_eq!(count(&db.conn, "receipts"), 1);
    assert_eq!(count(&db.conn, "repository_revisions"), 1);

    let toggle = |id: &str| Operation::SetRepositoryEnabled {
        operation_id: id.into(), repository_id: "retry-fixture".into(),
        expected_generation: 1, enabled_for_new_tasks: false,
    };
    hold_read(&reader);
    let pending = candidate(failure(db.handle(original("old-toggle", toggle("toggle-original"), TrustedHostPath::OperatorApi))));
    reader.execute_batch("ROLLBACK").unwrap();
    let (newer, _) = reply(db.handle(original("new-toggle", toggle("toggle-newer"), TrustedHostPath::OperatorApi)));
    assert!(matches!(newer.outcome, Outcome::Ok { .. }));
    let (stale, changed) = reply(db.handle(pending));
    refused(&stale, "revision_conflict");
    assert!(!changed, "the original stale operation cannot change the repository head");
    assert_eq!(stale.commit.as_ref().unwrap().operation_id, "toggle-original");
    let head: (i64, i64) = db.conn.query_row("SELECT generation,enabled FROM repositories WHERE id='retry-fixture'", [], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
    assert_eq!(head, (2, 0));
    assert_eq!(revision(&db.conn), 3);
    assert_eq!(count(&db.conn, "receipts"), 3);
}

#[test]
fn acknowledged_transient_retains_the_genuine_native_proof_for_manual_reentry() {
    let area = Area::new();
    let mut db = area.open();
    let input = TaskInput { instruction_source: None,
        pull_request: None,
        resolved_limits: None, repository_read: None,
        instruction: "produce one structured answer".into(),
        definition: Definition {
            charter: None, limit_overrides: None,
            execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
            inference: Some(InferenceSettings::selected()),
            id: "proof-agent".into(), revision: "1".into(), executor: ExecutorKind::Native,
        },
        repository: None, acceptance_commands: vec![],
        output_schema: Some(json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false})),
        limits: db.config.limits.task_defaults.at(NOW).unwrap(),
        dependencies: vec![], native_workflow: None,
    };
    let command = Operation::SubmitTask { operation_id: "native-original".into(), input };
    let request = original("native-exact", command.clone(), TrustedHostPath::Browser);
    let exact = serde_json::to_value(&request.request).unwrap();
    let ticket = match db.handle(request) {
        (Response::Validation(ticket), false) => ticket,
        (Response::Reply(reply), _) => panic!("native preflight did not issue its genuine ticket: {reply:?}"),
        _ => panic!("unexpected preflight result"),
    };
    let proof = {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(Box::pin((*ticket).execute(&db.validation_host)))
    };
    contendible(&db);
    let reader = area.contender();
    hold_read(&reader);
    let mut finalization = original("native-exact", command.clone(), TrustedHostPath::Browser);
    finalization.validation = Some(ExternalProof::Native(proof));
    let retained = candidate(failure(db.handle(finalization)));
    assert_eq!(serde_json::to_value(&retained.request).unwrap(), exact);
    assert_eq!(retained.invocation_path, TrustedHostPath::Browser);
    assert!(matches!(&retained.validation, Some(ExternalProof::Native(_))));
    assert_eq!(count(&db.conn, "tasks"), 0);
    assert_eq!(count(&db.conn, "receipts"), 0);
    reader.execute_batch("ROLLBACK").unwrap();
    let (committed, changed) = reply(db.handle(retained));
    assert!(changed);
    assert!(matches!(committed.outcome, Outcome::Ok { data: Data::Task(_) }));
    assert_eq!(committed.commit.as_ref().unwrap().operation_id, "native-original");
    let (replayed, changed) = reply(db.handle(original("native-replay", command, TrustedHostPath::Browser)));
    assert!(!changed);
    assert_eq!(replayed.outcome, committed.outcome);
    assert_eq!(replayed.commit, committed.commit);
    assert_eq!(count(&db.conn, "tasks"), 1);
    assert_eq!(count(&db.conn, "receipts"), 1);
    assert_eq!(revision(&db.conn), 1);
}

#[test]
fn permanent_and_unproven_failures_never_release_a_candidate() {
    let area = Area::new();
    let mut db = area.open();
    db.conn.execute_batch("CREATE TEMP TRIGGER refuse_receipt BEFORE INSERT ON receipts BEGIN SELECT RAISE(ABORT,'fixture permanent receipt refusal'); END;").unwrap();
    let failed = failure(db.handle(original("permanent", registration("permanent-original"), TrustedHostPath::OperatorApi)));
    assert_eq!(failed.disposition(), RetryDisposition::Ineligible);
    let retained = match (*failed).into_retry_candidate() {
        Err(retained) => retained,
        Ok(_) => panic!("a permanent SQLite constraint must not grant re-entry"),
    };
    let fallback = retained.into_reply();
    refused(&fallback, "store_unavailable");
    assert_eq!(fallback.request_id, "permanent");
    assert!(fallback.commit.is_none());
    assert!(fallback.observed_controller_revision.is_none());
    assert!(db.conn.is_autocommit());
    assert_eq!(count(&db.conn, "repositories"), 0);
    assert_eq!(count(&db.conn, "receipts"), 0);
    assert_eq!(revision(&db.conn), 0);
    db.conn.execute_batch("DROP TRIGGER refuse_receipt").unwrap();

    let commits = Arc::new(AtomicUsize::new(0));
    let observed = commits.clone();
    db.conn.commit_hook(Some(move || { observed.fetch_add(1, Ordering::SeqCst); true })).unwrap();
    let failed = failure(db.handle(original("unproven", registration("unproven-original"), TrustedHostPath::OperatorApi)));
    assert_eq!(commits.load(Ordering::SeqCst), 1);
    assert!(db.conn.is_autocommit(), "SQLite's automatic rollback alone is not an acknowledged rollback");
    assert_eq!(failed.disposition(), RetryDisposition::Ineligible);
    let retained = match (*failed).into_retry_candidate() {
        Err(retained) => retained,
        Ok(_) => panic!("unproven transaction release must not grant re-entry"),
    };
    let fallback = retained.into_reply();
    refused(&fallback, "store_unavailable");
    assert_eq!(fallback.request_id, "unproven");
    assert!(fallback.commit.is_none());
    db.conn.commit_hook(None::<fn() -> bool>).unwrap();
    assert_eq!(count(&db.conn, "repositories"), 0);
    assert_eq!(count(&db.conn, "receipts"), 0);
    assert_eq!(revision(&db.conn), 0);
}
