//! Actual linked SQLite, existing StoreContext/receipt revision helpers, and
//! owned commit/authorizer hooks. The closed classifier matrix constructs typed
//! error codes separately; no test enables automatic transaction retry.
use super::*;
use crate::controller::{database::{StoreContext, encode}, metadata};
use crate::protocol::{Operation, Outcome, Data};
use rusqlite::{ffi, hooks::{AuthAction, AuthContext, Authorization, TransactionOperation}};
use std::{cell::Cell, path::PathBuf, sync::{Arc, atomic::{AtomicU64, AtomicUsize, Ordering}}, time::Duration};

fn database() -> Connection {
    let connection = Connection::open_in_memory().unwrap();
    connection.execute_batch("CREATE TABLE entries(id INTEGER PRIMARY KEY); CREATE TABLE counters(kind TEXT PRIMARY KEY,value INTEGER); INSERT INTO counters VALUES('controller_revision',0);").unwrap();
    connection
}
fn count(connection: &Connection) -> i64 {
    connection.query_row("SELECT count(*) FROM entries", [], |row| row.get(0)).unwrap()
}
fn failed(error: Fault) -> Failure {
    match error {Fault::Transaction(failure) => *failure, other => panic!("missing transaction phase: {other:?}")}
}
fn deny_rollback(context: AuthContext<'_>) -> Authorization {
    match context.action {
        AuthAction::Transaction {operation:TransactionOperation::Rollback} => Authorization::Deny,
        _ => Authorization::Allow,
    }
}
fn clear_authorizer(connection: &Connection) {
    connection.authorizer(None::<fn(AuthContext<'_>) -> Authorization>).unwrap();
}

#[test]
fn body_sqlite_code_and_revision_are_retained_but_partial_writes_are_rolled_back() {
    let mut connection = database();
    let limits = crate::limits::RuntimeLimits::seed().unwrap();
    let calls = Cell::new(0);
    let error = immediate::<()>(&mut connection, |sql| {
        calls.set(calls.get()+1);
        assert_eq!(metadata::advance(&StoreContext::new(sql, &limits))?, 1);
        sql.execute("INSERT INTO entries VALUES(1)", [])?;
        sql.execute("INSERT INTO entries VALUES(1)", [])?;
        Ok(Completion::Commit(()))
    }).unwrap_err();
    let failure = failed(error);
    assert_eq!(failure.phase, Phase::Body);
    assert_eq!(failure.original_sqlite_code(), Some(SqliteCode {
        primary:ErrorCode::ConstraintViolation, extended:ffi::SQLITE_CONSTRAINT_PRIMARYKEY}));
    assert!(matches!(failure.original, Fault::Sqlite(_)), "do not erase the original error into text");
    assert!(failure.rollback.acknowledged());
    assert_eq!(failure.retry_disposition(), RetryDisposition::Ineligible);
    assert!(connection.is_autocommit());
    assert_eq!(count(&connection), 0);
    assert_eq!(metadata::current(&StoreContext::new(&connection, &limits)).unwrap(), 0);
    assert_eq!(calls.get(), 1, "this primitive never retries a body");
}

#[test]
fn failed_commit_requires_an_actual_acknowledged_rollback() {
    let mut connection = database();
    connection.execute_batch("PRAGMA foreign_keys=ON; CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE child(parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);").unwrap();
    let error = immediate(&mut connection, |sql| {
        sql.execute("INSERT INTO entries VALUES(1)", [])?;
        sql.execute("INSERT INTO child VALUES(99)", [])?;
        Ok(Completion::Commit(()))
    }).unwrap_err();
    let failure = failed(error);
    assert_eq!(failure.phase, Phase::Commit);
    assert_eq!(failure.original_sqlite_code(), Some(SqliteCode {
        primary:ErrorCode::ConstraintViolation, extended:ffi::SQLITE_CONSTRAINT_FOREIGNKEY}));
    assert!(failure.rollback.acknowledged());
    assert_eq!(failure.retry_disposition(), RetryDisposition::Ineligible);
    assert!(connection.is_autocommit());
    assert_eq!(count(&connection), 0);
    assert_eq!(connection.query_row("SELECT count(*) FROM child", [], |row| row.get::<_,i64>(0)).unwrap(), 0);
}

#[test]
fn commit_hook_auto_rollback_is_not_promoted_from_autocommit_to_checked_proof() {
    let mut connection = database();
    let commits = Arc::new(AtomicUsize::new(0));
    let seen = commits.clone();
    connection.commit_hook(Some(move || {seen.fetch_add(1,Ordering::SeqCst); true})).unwrap();
    let error = immediate(&mut connection, |sql| {
        sql.execute("INSERT INTO entries VALUES(1)", [])?;
        Ok(Completion::Commit(()))
    }).unwrap_err();
    let failure = failed(error);
    assert_eq!(failure.phase, Phase::Commit);
    assert_eq!(failure.original_sqlite_code(), Some(SqliteCode {
        primary:ErrorCode::ConstraintViolation, extended:ffi::SQLITE_CONSTRAINT_COMMITHOOK}));
    assert!(matches!(failure.rollback, Rollback::Unproven {error:Some(_), autocommit:true}),
        "ROLLBACK fails because SQLite already ended the transaction; that is not an acknowledged ROLLBACK");
    assert_eq!(failure.retry_disposition(), RetryDisposition::Ineligible);
    assert!(connection.is_autocommit());
    assert_eq!(count(&connection), 0);
    assert_eq!(commits.load(Ordering::SeqCst), 1);
}

#[test]
fn failed_rollback_preserves_original_code_and_cleanup_code_without_drop_retry() {
    let mut connection = database();
    let rollbacks = Arc::new(AtomicUsize::new(0));
    let observed = rollbacks.clone();
    connection.authorizer(Some(move |context: AuthContext<'_>| {
        if matches!(context.action, AuthAction::Transaction {operation:TransactionOperation::Rollback}) {
            observed.fetch_add(1, Ordering::SeqCst);
            Authorization::Deny
        } else {Authorization::Allow}
    })).unwrap();
    let error = immediate::<()>(&mut connection, |sql| {
        sql.execute("INSERT INTO entries VALUES(1)", [])?;
        sql.execute("INSERT INTO entries VALUES(1)", [])?;
        Ok(Completion::Commit(()))
    }).unwrap_err();
    let failure = failed(error);
    assert_eq!(failure.phase, Phase::Body);
    assert_eq!(failure.original_sqlite_code().unwrap().extended, ffi::SQLITE_CONSTRAINT_PRIMARYKEY);
    match &failure.rollback {
        Rollback::Unproven {error:Some(error),autocommit:false} =>
            assert_eq!(sqlite_code(error).unwrap().extended, ffi::SQLITE_AUTH),
        other => panic!("rollback refusal was hidden: {other:?}"),
    }
    assert_eq!(rollbacks.load(Ordering::SeqCst), 1, "no unchecked second rollback on Drop");
    assert_eq!(failure.retry_disposition(), RetryDisposition::Ineligible);
    assert!(!connection.is_autocommit());
    assert_eq!(count(&connection), 1, "uncommitted state remains owned and cannot be reported absent");
    let entered = Cell::new(false);
    let blocked = failed(immediate(&mut connection, |_| {entered.set(true); Ok(Completion::Commit(()))}).unwrap_err());
    assert_eq!(blocked.phase, Phase::Begin);
    assert!(matches!(blocked.rollback, Rollback::NotOwned));
    assert!(!entered.get());
    assert_eq!(rollbacks.load(Ordering::SeqCst), 1);
    clear_authorizer(&connection);
    connection.execute_batch("ROLLBACK").unwrap();
    assert_eq!(count(&connection), 0);
}

#[test]
fn refuses_foreign_active_transaction_without_touching_it_or_running_body() {
    let mut connection = database();
    connection.execute_batch("BEGIN IMMEDIATE; INSERT INTO entries VALUES(1)").unwrap();
    let calls = Cell::new(0);
    let failure = failed(immediate(&mut connection, |_| {
        calls.set(calls.get()+1); Ok(Completion::Commit(()))
    }).unwrap_err());
    assert_eq!(failure.phase, Phase::Begin);
    assert!(matches!(failure.rollback, Rollback::NotOwned));
    assert_eq!(calls.get(), 0);
    assert!(!connection.is_autocommit());
    assert_eq!(count(&connection), 1);
    connection.execute_batch("ROLLBACK").unwrap();
}

static NEXT: AtomicU64 = AtomicU64::new(1);
struct Area(PathBuf);
impl Area {
    fn new() -> Self {
        let base = std::env::var_os("BORG_TEST_ARTIFACT").map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let stamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let path = base.join(format!("borg-checked-sqlite-{}-{stamp}-{}",
            std::process::id(), NEXT.fetch_add(1,Ordering::Relaxed)));
        std::fs::create_dir(&path).unwrap(); Self(path)
    }
}
impl Drop for Area {fn drop(&mut self) {
    if std::env::var_os("BORG_TEST_ARTIFACT").is_none() { let _ = std::fs::remove_dir_all(&self.0); }
}}
#[test]
fn busy_begin_preserves_code_without_body_execution_or_implicit_retry() {
    let area = Area::new();
    let first = Connection::open(area.0.join("fixture.db")).unwrap();
    let mut second = Connection::open(area.0.join("fixture.db")).unwrap();
    // Test-only choice for deterministic contention. Production retains its
    // explicitly selected controller.sqlite_busy_timeout_ms unchanged.
    first.busy_timeout(Duration::ZERO).unwrap();
    second.busy_timeout(Duration::ZERO).unwrap();
    first.execute_batch("CREATE TABLE entries(id INTEGER PRIMARY KEY); BEGIN IMMEDIATE; INSERT INTO entries VALUES(1)").unwrap();
    let calls = Cell::new(0);
    let failure = failed(immediate(&mut second, |_| {
        calls.set(calls.get()+1); Ok(Completion::Commit(()))
    }).unwrap_err());
    assert_eq!(failure.phase, Phase::Begin);
    assert_eq!(failure.original_sqlite_code(), Some(SqliteCode {primary:ErrorCode::DatabaseBusy,extended:ffi::SQLITE_BUSY}));
    assert!(matches!(failure.rollback, Rollback::NotStarted));
    assert_eq!(failure.retry_disposition(), RetryDisposition::BeginNotStartedTransient {
        code: SqliteCode {primary:ErrorCode::DatabaseBusy,extended:ffi::SQLITE_BUSY},
    });
    assert_eq!(calls.get(), 0);
    assert!(!first.is_autocommit());
    assert_eq!(count(&first), 1, "another owner's transaction was not rolled back");
    assert!(second.is_autocommit());
    first.execute_batch("ROLLBACK").unwrap();
    immediate(&mut second, |sql| {
        calls.set(calls.get()+1); sql.execute("INSERT INTO entries VALUES(2)", [])?;
        Ok(Completion::Commit(()))
    }).unwrap();
    assert_eq!(calls.get(), 1, "only the explicit later invocation enters the body");
    assert_eq!(count(&second), 1);
}

#[test]
fn early_original_receipt_is_returned_only_after_acknowledged_rollback() {
    let mut connection = database();
    let limits = crate::limits::RuntimeLimits::seed().unwrap();
    connection.execute_batch("CREATE TABLE receipts(id TEXT PRIMARY KEY,request TEXT,outcome TEXT,cleanup INTEGER,controller_revision INTEGER)").unwrap();
    let operation = Operation::CancelTask {operation_id:"original".into(),task_id:"task".into(),expected_revision:1};
    let outcome = Outcome::Ok {data:Data::Acknowledged};
    immediate(&mut connection, |sql| {
        let tx = StoreContext::new(sql, &limits);
        let revision = metadata::advance(&tx)?;
        sql.execute("INSERT INTO receipts VALUES(?1,?2,?3,0,?4)",
            rusqlite::params!["original",encode(&operation)?,encode(&outcome)?,crate::controller::database::integer(revision)?])?;
        Ok(Completion::Commit(()))
    }).unwrap();
    let rollbacks = Arc::new(AtomicUsize::new(0));
    let observed = rollbacks.clone();
    connection.authorizer(Some(move |context: AuthContext<'_>| {
        if matches!(context.action, AuthAction::Transaction {operation:TransactionOperation::Rollback}) {
            observed.fetch_add(1,Ordering::SeqCst);
        }
        Authorization::Allow
    })).unwrap();
    let original = immediate(&mut connection, |sql| {
        let receipt = metadata::read(&StoreContext::new(sql, &limits), "original")?.unwrap();
        Ok(Completion::Rollback(receipt))
    }).unwrap();
    assert_eq!(serde_json::to_value(&original.request).unwrap(),serde_json::to_value(&operation).unwrap());
    assert_eq!(original.outcome, outcome);
    assert_eq!(original.commit.operation_id, "original");
    assert_eq!(original.commit.controller_revision, 1);
    assert!(connection.is_autocommit());
    assert_eq!(rollbacks.load(Ordering::SeqCst), 1,
        "the early receipt path executes a real ROLLBACK before returning");
    connection.authorizer(Some(deny_rollback)).unwrap();
    let failure = failed(immediate(&mut connection, |sql| {
        let receipt = metadata::read(&StoreContext::new(sql, &limits), "original")?.unwrap();
        Ok(Completion::Rollback(receipt))
    }).err().unwrap());
    assert_eq!(failure.phase, Phase::Rollback);
    assert!(matches!(failure.rollback, Rollback::Unproven {error:Some(_),autocommit:false}));
    clear_authorizer(&connection);
    connection.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn non_store_control_transfer_cannot_escape_when_transaction_release_fails() {
    for deny in [false,true] {
        let mut connection = database();
        if deny {connection.authorizer(Some(deny_rollback)).unwrap();}
        let error = immediate::<()>(&mut connection, |sql| {
            sql.execute("INSERT INTO entries VALUES(1)", [])?;
            Err(Fault::TemporaryValidation("validation_capacity", "fixture external work handoff"))
        }).unwrap_err();
        if deny {
            let failure = failed(error);
            assert_eq!(failure.phase, Phase::Body);
            assert!(matches!(failure.original, Fault::TemporaryValidation("validation_capacity", _)));
            assert!(matches!(failure.rollback, Rollback::Unproven {error:Some(_),autocommit:false}));
            clear_authorizer(&connection);
            connection.execute_batch("ROLLBACK").unwrap();
        } else {
            assert!(matches!(error, Fault::TemporaryValidation("validation_capacity", _)));
        }
        assert!(connection.is_autocommit());
        assert_eq!(count(&connection), 0);
    }
}

#[test]
fn locked_body_requires_acknowledged_rollback_before_becoming_a_candidate() {
    let mut connection = database();
    connection.execute("INSERT INTO entries VALUES(0)", []).unwrap();
    let error = immediate::<()>(&mut connection, |sql| {
        sql.execute("INSERT INTO entries VALUES(1)", [])?;
        let mut statement = sql.prepare("SELECT id FROM entries ORDER BY id")?;
        let mut rows = statement.query([])?;
        assert!(rows.next()?.is_some());
        // An actual open statement prevents DROP. It is released by the error
        // return before the owned boundary asks SQLite to acknowledge rollback.
        sql.execute_batch("DROP TABLE entries")?;
        Ok(Completion::Commit(()))
    }).unwrap_err();
    let failure = failed(error);
    assert_eq!(failure.retry_disposition(), RetryDisposition::RolledBackTransient {
        phase:Phase::Body,
        code:SqliteCode {primary:ErrorCode::DatabaseLocked,extended:ffi::SQLITE_LOCKED},
    });
    assert!(connection.is_autocommit());
    assert_eq!(count(&connection), 1, "the partial body insert was rolled back");
}

#[test]
fn busy_commit_is_classified_only_after_actual_rollback_removes_its_writes() {
    let area = Area::new();
    let mut writer = Connection::open(area.0.join("commit.db")).unwrap();
    let reader = Connection::open(area.0.join("commit.db")).unwrap();
    writer.execute_batch("PRAGMA journal_mode=DELETE; CREATE TABLE entries(id INTEGER PRIMARY KEY); INSERT INTO entries VALUES(0)").unwrap();
    writer.busy_timeout(Duration::ZERO).unwrap();
    reader.execute_batch("BEGIN").unwrap();
    assert_eq!(count(&reader), 1); // Retain a real shared lock through COMMIT.
    let error = immediate(&mut writer, |sql| {
        sql.execute("INSERT INTO entries VALUES(1)", [])?;
        Ok(Completion::Commit(()))
    }).unwrap_err();
    let failure = failed(error);
    assert_eq!(failure.retry_disposition(), RetryDisposition::RolledBackTransient {
        phase:Phase::Commit,
        code:SqliteCode {primary:ErrorCode::DatabaseBusy,extended:ffi::SQLITE_BUSY},
    });
    assert!(writer.is_autocommit());
    assert_eq!(count(&writer), 1);
    assert_eq!(count(&reader), 1);
    reader.execute_batch("ROLLBACK").unwrap();
    assert_eq!(count(&writer), 1, "no automatic transaction reentry");
}

#[test]
fn retry_classification_is_closed_over_original_code_phase_and_release_proof() {
    let sqlite = |code| Fault::Sqlite(rusqlite::Error::SqliteFailure(ffi::Error::new(code), None));
    for code in [ffi::SQLITE_BUSY,ffi::SQLITE_BUSY_RECOVERY,ffi::SQLITE_BUSY_SNAPSHOT,
        ffi::SQLITE_BUSY_TIMEOUT,ffi::SQLITE_LOCKED,ffi::SQLITE_LOCKED_SHAREDCACHE] {
        for phase in [Phase::Begin,Phase::Body,Phase::Commit] {
            let failure = Failure {phase,original:sqlite(code),rollback:Rollback::Acknowledged};
            assert!(matches!(failure.retry_disposition(), RetryDisposition::RolledBackTransient { .. }));
        }
        for rollback in [Rollback::NotOwned,Rollback::Unproven {error:None,autocommit:true},
            Rollback::Unproven {error:None,autocommit:false},Rollback::NotStarted] {
            let failure = Failure {phase:Phase::Body,original:sqlite(code),rollback};
            assert_eq!(failure.retry_disposition(),RetryDisposition::Ineligible);
        }
        let failure = Failure {phase:Phase::Rollback,original:sqlite(code),rollback:Rollback::Acknowledged};
        assert_eq!(failure.retry_disposition(),RetryDisposition::Ineligible);
    }
    for code in [ffi::SQLITE_IOERR,ffi::SQLITE_FULL,ffi::SQLITE_CORRUPT,ffi::SQLITE_AUTH,
        ffi::SQLITE_CONSTRAINT,ffi::SQLITE_LOCKED_VTAB,ffi::SQLITE_BUSY | (99 << 8)] {
        let failure = Failure {phase:Phase::Body,original:sqlite(code),rollback:Rollback::Acknowledged};
        assert_eq!(failure.retry_disposition(),RetryDisposition::Ineligible);
    }
    let nested = Failure {phase:Phase::Body,original:sqlite(ffi::SQLITE_BUSY),rollback:Rollback::Unproven {error:None,autocommit:true}};
    let outer = Failure {phase:Phase::Body,original:Fault::Transaction(Box::new(nested)),rollback:Rollback::Acknowledged};
    assert_eq!(outer.retry_disposition(),RetryDisposition::Ineligible,
        "an outer release must not upgrade the original unproven transaction");
}
