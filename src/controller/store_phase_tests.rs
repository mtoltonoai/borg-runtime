//! Real linked-SQLite contention and the production startup phase bodies.
//! Fixture timing is explicit existing policy, not deployment activation.
use super::*;
use crate::controller::{application_schema, tracking::Tracking};
use rusqlite::{Connection, hooks::{AuthAction, AuthContext, Authorization, TransactionOperation}};
use std::{fs, os::unix::fs::MetadataExt, path::PathBuf, sync::{mpsc, atomic::{AtomicUsize, Ordering}}, thread,
    time::{SystemTime, UNIX_EPOCH}};

static NEXT: AtomicUsize = AtomicUsize::new(0);
fn area(name: &str) -> PathBuf {
    let root = PathBuf::from(std::env::var_os("BORG_TEST_ARTIFACT").expect("owned test artifact directory"));
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let path = root.join(format!("store-phase-{name}-{}-{stamp}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::SeqCst)));
    fs::create_dir(&path).unwrap(); path
}
fn policy() -> StoreRetryLimits { StoreRetryLimits { backoff_ms: 20, operation_timeout_ms: 2_000 } }
fn open(path: &std::path::Path) -> Connection {
    let sql = Connection::open(path).unwrap(); sql.busy_timeout(Duration::ZERO).unwrap(); sql
}
fn identity(path: &std::path::Path) -> (u64, u64) { let m=fs::metadata(path).unwrap(); (m.dev(), m.ino()) }
fn shape(sql: &Connection) -> Vec<(String,String,Option<String>)> {
    let mut q=sql.prepare("SELECT type,name,sql FROM sqlite_master ORDER BY type,name").unwrap();
    q.query_map([], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap().collect::<std::result::Result<Vec<_>,_>>().unwrap()
}

#[test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A transient failure of a platform-owned durable-store transaction MUST be retried with metadata-controlled backoff while retaining the original operation identity.
fn fresh_original_identity_and_completed_binding_survive_real_body_rollback() {
    let directory=area("fresh"); let locked=directory.join("locked.db"); let blocker=open(&locked);
    blocker.execute_batch("CREATE TABLE held(value INTEGER); BEGIN EXCLUSIVE").unwrap();
    let (rolled, wait)=mpsc::sync_channel(1); let path=directory.join("tasks.db");
    let worker=thread::spawn(move || {
        let limits=crate::limits::RuntimeLimits::seed().unwrap();
        let mut tracking=Tracking::acquire(&path,&limits.controller).unwrap(); tracking.before_sqlite().unwrap();
        let mut sql=open(&path); let mut original=FreshSchema::new(&sql).unwrap(); let id=original.id.clone();
        let registry=tracking.registry.clone(); let original_inode=identity(&path);
        let attempts=Arc::new(AtomicUsize::new(0)); let rollback_count=Arc::new(AtomicUsize::new(0));
        let counted=rollback_count.clone(); let captured=registry.clone();
        sql.rollback_hook(Some(move || {
            if counted.fetch_add(1,Ordering::SeqCst)==0 {
                let marker=captured.join("owner.binding");
                rolled.send((fs::read(&marker).unwrap(),identity(&marker),identity(&captured))).unwrap();
            }
        })).unwrap();
        transaction(&mut sql,Some(&policy()),&StartupCancellation::default(),true,|tx| {
            attempts.fetch_add(1,Ordering::SeqCst); original.write(tx,&tracking)?;
            let attached: bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM pragma_database_list WHERE name='held_store')",[],|r|r.get(0))?;
            if !attached { tx.execute("ATTACH DATABASE ?1 AS held_store",[locked.to_str().unwrap()])?; }
            tx.execute("INSERT INTO held_store.held VALUES(1)",[])?;
            Ok(Completion::Commit(()))
        }).unwrap();
        assert!(attempts.load(Ordering::SeqCst)>=2); assert!(rollback_count.load(Ordering::SeqCst)>=1);
        assert_eq!(sql.query_row("SELECT value FROM settings WHERE name='database_id'",[],|r|r.get::<_,String>(0)).unwrap(),id);
        assert_eq!(identity(&path),original_inode);
        tracking.verify(&id,original.binding.as_ref().unwrap()).unwrap();
        (fs::read(registry.join("owner.binding")).unwrap(),identity(&registry.join("owner.binding")),identity(&registry))
    });
    let before=wait.recv_timeout(Duration::from_secs(3)).expect("actual SQLite body failure and checked rollback");
    blocker.execute_batch("ROLLBACK").unwrap();
    assert_eq!(worker.join().unwrap(),before,"original durable binding/inodes cannot be recreated after rollback");
    assert_eq!(blocker.query_row("SELECT count(*) FROM held",[],|r|r.get::<_,i64>(0)).unwrap(),1);
}

#[test]
fn fresh_replaced_binding_is_terminal_without_recreating_it() {
    let directory=area("binding");let path=directory.join("tasks.db");let limits=crate::limits::RuntimeLimits::seed().unwrap();
    let mut tracking=Tracking::acquire(&path,&limits.controller).unwrap();tracking.before_sqlite().unwrap();
    let mut sql=open(&path);let mut original=FreshSchema::new(&sql).unwrap();let id=original.id.clone();
    checked_transaction::exclusive(&mut sql,|tx| { original.write(tx,&tracking)?;Ok(Completion::Rollback(())) }).unwrap();
    let marker=tracking.registry.join("owner.binding");let marker_identity=identity(&marker);
    fs::write(&marker,b"retained corruption").unwrap();let mut attempts=0;
    assert!(transaction(&mut sql,Some(&policy()),&StartupCancellation::default(),true,|tx| {
        attempts+=1; original.write(tx,&tracking)?;Ok(Completion::Commit(()))
    }).is_err());
    assert_eq!(attempts,1);assert_eq!(original.id,id);assert_eq!(identity(&marker),marker_identity);
    assert_eq!(fs::read(marker).unwrap(),b"retained corruption");assert!(shape(&sql).is_empty());
}

#[test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A transient failure of a platform-owned durable-store transaction MUST be retried with metadata-controlled backoff while retaining the original operation identity.
fn original_validated_migration_retries_busy_commit_without_changing_old_rows() {
    let directory=area("upgrade");let path=directory.join("tasks.db");let mut sql=open(&path);
    application_schema::expected_schema(&sql,17).unwrap();
    sql.execute("INSERT INTO receipts(id,request,outcome,cleanup,controller_revision) VALUES('original','request','outcome',0,1)",[]).unwrap();
    let old=shape(&sql);let reader=open(&path);reader.execute_batch("BEGIN").unwrap();
    let id:String=reader.query_row("SELECT value FROM settings WHERE name='database_id'",[],|r|r.get(0)).unwrap();
    let (rolled,wait)=mpsc::sync_channel(1);let seen=Arc::new(AtomicUsize::new(0));let count=seen.clone();
    sql.rollback_hook(Some(move || { if count.fetch_add(1,Ordering::SeqCst)==0 { rolled.send(()).unwrap(); } })).unwrap();
    let worker=thread::spawn(move || {
        transaction(&mut sql,Some(&policy()),&StartupCancellation::default(),false,|tx| {
            application_schema::upgrade_checked(tx,17)?;Ok(Completion::Commit(()))
        }).unwrap();sql
    });
    wait.recv_timeout(Duration::from_secs(3)).expect("real failed migration COMMIT");
    assert_eq!(shape(&reader),old);assert_eq!(reader.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),17);
    reader.execute_batch("ROLLBACK").unwrap();let sql=worker.join().unwrap();
    assert!(seen.load(Ordering::SeqCst)>0);assert_eq!(sql.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),application_schema::VERSION);
    assert_eq!(sql.query_row("SELECT value FROM settings WHERE name='database_id'",[],|r|r.get::<_,String>(0)).unwrap(),id);
    assert_eq!(sql.query_row("SELECT request||':'||outcome FROM receipts WHERE id='original'",[],|r|r.get::<_,String>(0)).unwrap(),"request:outcome");
}

#[test]
fn startup_busy_begin_wait_is_cancellable_and_body_never_runs() {
    let directory=area("cancel");let path=directory.join("tasks.db");let blocker=open(&path);
    blocker.execute_batch("CREATE TABLE entries(value INTEGER); BEGIN EXCLUSIVE").unwrap();
    let sql=open(&path);let cancel=StartupCancellation::default();let worker_cancel=cancel.clone();
    let (began,wait)=mpsc::sync_channel(1);let attempts=Arc::new(AtomicUsize::new(0));let observed=attempts.clone();
    sql.authorizer(Some(move |context:AuthContext<'_>| {
        if matches!(context.action,AuthAction::Transaction { operation:TransactionOperation::Begin }) {
            if observed.fetch_add(1,Ordering::SeqCst)==0 { began.send(()).unwrap(); }
        } Authorization::Allow
    })).unwrap();
    let worker=thread::spawn(move || { let mut sql=sql;
        transaction(&mut sql,Some(&StoreRetryLimits{backoff_ms:1_000,operation_timeout_ms:2_000}),&worker_cancel,true,|_| {
            panic!("a failed BEGIN cannot execute the body");
            #[allow(unreachable_code)] Ok(Completion::Commit(()))
        })
    });
    wait.recv_timeout(Duration::from_secs(3)).unwrap();cancel.cancel();let start=Instant::now();
    assert!(worker.join().unwrap().unwrap_err().contains("cancelled"));assert!(start.elapsed()<Duration::from_millis(500));
    assert_eq!(attempts.load(Ordering::SeqCst),1);blocker.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn startup_unknown_rollback_and_nontransient_errors_never_repeat() {
    for unknown in [false,true] {
        let mut sql=Connection::open_in_memory().unwrap();sql.execute_batch("CREATE TABLE entries(value INTEGER PRIMARY KEY)").unwrap();
        if unknown { sql.authorizer(Some(|context:AuthContext<'_>| match context.action {
            AuthAction::Transaction { operation:TransactionOperation::Rollback }=>Authorization::Deny,_=>Authorization::Allow
        })).unwrap(); }
        let mut calls=0;
        let error=transaction(&mut sql,Some(&policy()),&StartupCancellation::default(),false,|tx| {
            calls+=1;tx.execute("INSERT INTO entries VALUES(1)",[])?;tx.execute("INSERT INTO entries VALUES(1)",[])?;
            Ok(Completion::Commit(()))
        }).unwrap_err();
        assert_eq!(calls,1);assert!(error.contains("ConstraintViolation"));
        assert_eq!(sql.is_autocommit(),!unknown);
        if unknown {sql.authorizer(None::<fn(AuthContext<'_>)->Authorization>).unwrap();sql.execute_batch("ROLLBACK").unwrap();}
        assert_eq!(sql.query_row("SELECT count(*) FROM entries",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    }
}

#[test]
fn absent_policy_never_schedules_a_transient_begin_retry() {
    let directory=area("absent");let path=directory.join("tasks.db");let blocker=open(&path);
    blocker.execute_batch("CREATE TABLE entries(value INTEGER); BEGIN EXCLUSIVE").unwrap();let mut sql=open(&path);
    let started=Instant::now();assert!(transaction(&mut sql,None,&StartupCancellation::default(),true,|_| {
        panic!("body cannot run");#[allow(unreachable_code)] Ok(Completion::Commit(()))
    }).is_err());assert!(started.elapsed()<Duration::from_millis(500));blocker.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn startup_transient_phase_exhausts_one_lifetime_without_body_or_reopen() {
    let directory=area("exhaust");let path=directory.join("tasks.db");let blocker=open(&path);
    blocker.execute_batch("CREATE TABLE entries(value INTEGER); BEGIN EXCLUSIVE").unwrap();let mut sql=open(&path);
    let attempts=Arc::new(AtomicUsize::new(0));let counted=attempts.clone();
    sql.authorizer(Some(move |context:AuthContext<'_>| {
        if matches!(context.action,AuthAction::Transaction{operation:TransactionOperation::Begin}) {
            counted.fetch_add(1,Ordering::SeqCst);
        } Authorization::Allow
    })).unwrap();
    let before=identity(&path);let started=Instant::now();
    let error=transaction(&mut sql,Some(&StoreRetryLimits{backoff_ms:20,operation_timeout_ms:100}),
        &StartupCancellation::default(),true,|_| {panic!("BEGIN is still held");#[allow(unreachable_code)] Ok(Completion::Commit(()))}).unwrap_err();
    assert!(error.contains("exhausted"));assert!(started.elapsed()>=Duration::from_millis(100));
    assert!(started.elapsed()<Duration::from_secs(1));assert!((2..=8).contains(&attempts.load(Ordering::SeqCst)));
    assert_eq!(identity(&path),before);assert!(sql.is_autocommit());blocker.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn partial_filesystem_binding_is_not_retried_or_repaired_by_sql_rollback() {
    let directory=area("partial");let path=directory.join("tasks.db");let limits=crate::limits::RuntimeLimits::seed().unwrap();
    let mut tracking=Tracking::acquire(&path,&limits.controller).unwrap();tracking.before_sqlite().unwrap();
    fs::create_dir(&tracking.registry).unwrap();fs::write(tracking.registry.join("partial"),b"original").unwrap();
    let before=identity(&tracking.registry);let mut sql=open(&path);let mut original=FreshSchema::new(&sql).unwrap();let mut calls=0;
    assert!(transaction(&mut sql,Some(&policy()),&StartupCancellation::default(),true,|tx| {
        calls+=1;original.write(tx,&tracking)?;Ok(Completion::Commit(()))
    }).is_err());
    assert_eq!(calls,1);assert!(original.binding.is_none());assert!(shape(&sql).is_empty());
    assert_eq!(identity(&tracking.registry),before);assert_eq!(fs::read(tracking.registry.join("partial")).unwrap(),b"original");
    assert!(!tracking.registry.join("owner.binding").exists());
}

#[test]
fn actual_startup_owner_lock_wait_is_cancelled_and_joined_without_replacing_root() {
    use crate::controller::{Controller,ControllerConfig};
    let directory=area("owner-cancel");let path=directory.join("tasks.db");
    let config=ControllerConfig { validation_executable:PathBuf::from(std::env::var_os("BORG_TEST_RUNTIME").expect("qualified helper")),
        ..ControllerConfig::default() };
    let controller=Controller::open_with_config(&path,config.clone()).unwrap();
    let before=identity(&path);let binding_path=directory.join("tasks.db.native-helpers/owner.binding");
    let binding=fs::read(&binding_path).unwrap();let binding_identity=identity(&binding_path);
    let cancel=StartupCancellation::default();let signalled=cancel.clone();let candidate_path=path.clone();
    let (begin,ready)=mpsc::sync_channel(1);
    let opening=thread::spawn(move || {begin.send(()).unwrap();Controller::open_with_config_cancellable(candidate_path,config,signalled)});
    ready.recv_timeout(Duration::from_secs(1)).unwrap();
    // Observe the actual owner entering its cancellable lock wait. Merely
    // signalling before open would also pass through the initial preflight.
    let (state,changed)=&*cancel.0;
    let (observed,timeout)=changed.wait_timeout_while(state.lock().unwrap(),Duration::from_secs(2),|s|s.waits==0).unwrap();
    assert!(!timeout.timed_out() && observed.waits>0 && observed.owners==1);
    drop(observed);cancel.cancel();
    assert!(opening.join().unwrap().err().expect("second owner is cancelled").contains("cancelled"));
    cancel.wait_for_owners();
    assert_eq!(identity(&path),before);assert_eq!(identity(&binding_path),binding_identity);assert_eq!(fs::read(binding_path).unwrap(),binding);
    drop(controller);
}

#[test]
fn retryable_busy_body_with_unproven_rollback_does_not_reenter() {
    let directory=area("busy-unknown");let locked=directory.join("locked.db");let blocker=open(&locked);
    blocker.execute_batch("CREATE TABLE held(value INTEGER); BEGIN EXCLUSIVE").unwrap();
    let mut sql=Connection::open_in_memory().unwrap();sql.busy_timeout(Duration::ZERO).unwrap();
    sql.execute_batch("CREATE TABLE entries(value INTEGER)").unwrap();
    sql.authorizer(Some(|context:AuthContext<'_>| match context.action {
        AuthAction::Transaction { operation:TransactionOperation::Rollback }=>Authorization::Deny,_=>Authorization::Allow
    })).unwrap();let mut calls=0;
    let error=transaction(&mut sql,Some(&policy()),&StartupCancellation::default(),false,|tx| {
        calls+=1;tx.execute("INSERT INTO entries VALUES(1)",[])?;
        tx.execute("ATTACH DATABASE ?1 AS held_store",[locked.to_str().unwrap()])?;
        tx.execute("INSERT INTO held_store.held VALUES(1)",[])?;Ok(Completion::Commit(()))
    }).unwrap_err();
    assert!(error.contains("DatabaseBusy"),"{error}");assert!(error.contains("Unproven"),"{error}");
    assert_eq!(calls,1);assert!(!sql.is_autocommit());
    sql.authorizer(None::<fn(AuthContext<'_>)->Authorization>).unwrap();sql.execute_batch("ROLLBACK").unwrap();
    blocker.execute_batch("ROLLBACK").unwrap();assert_eq!(sql.query_row("SELECT count(*) FROM entries",[],|r|r.get::<_,i64>(0)).unwrap(),0);
}

#[test]
fn full_fresh_initialization_retries_original_sql_exclusion_without_reopening_root() {
    use crate::controller::{ControllerConfig,database::Database};
    let directory=area("full-startup");let path=directory.join("tasks.db");
    let mut limits=crate::limits::RuntimeLimits::seed().unwrap();limits.controller.store_retry=Some(policy());
    limits.controller.sqlite_busy_timeout_ms=1;
    let config=ControllerConfig { limits:Arc::new(limits),
        validation_executable:PathBuf::from(std::env::var_os("BORG_TEST_RUNTIME").expect("qualified helper")),
        ..ControllerConfig::default() };
    let mut tracking=Tracking::acquire(&path,&config.limits.controller).unwrap();tracking.before_sqlite().unwrap();
    let before=identity(&path);let mut sql=open(&path);
    let held=Arc::new(Mutex::new(None::<Connection>));let held_for_hook=held.clone();let locked_path=path.clone();
    let began=Arc::new(AtomicUsize::new(0));let observed=began.clone();
    let (second,wait)=mpsc::sync_channel(1);let (released,release)=mpsc::sync_channel(1);
    sql.authorizer(Some(move |context:AuthContext<'_>| {
        if matches!(context.action,AuthAction::Transaction{operation:TransactionOperation::Begin}) {
            match observed.fetch_add(1,Ordering::SeqCst) {
                0=>{let reader=open(&locked_path);reader.execute_batch("BEGIN").unwrap();
                    assert_eq!(reader.query_row("SELECT count(*) FROM sqlite_master",[],|r|r.get::<_,i64>(0)).unwrap(),0);
                    *held_for_hook.lock().unwrap()=Some(reader);},
                1=>{second.send(()).unwrap();release.recv_timeout(Duration::from_secs(3)).unwrap();},
                _=>{},
            }
        } Authorization::Allow
    })).unwrap();
    let worker=thread::spawn(move || {
        let host=Database::initialize_for_qualification(&mut sql,&tracking,&config,&StartupCancellation::default()).unwrap();
        let id:String=sql.query_row("SELECT value FROM settings WHERE name='database_id'",[],|r|r.get(0)).unwrap();
        let binding:String=sql.query_row("SELECT value FROM settings WHERE name='native_helper_binding'",[],|r|r.get(0)).unwrap();
        tracking.verify(&id,&binding).unwrap();
        assert_eq!(sql.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),application_schema::VERSION);
        assert_eq!(sql.query_row("SELECT count(*) FROM receipts",[],|r|r.get::<_,i64>(0)).unwrap(),0);
        assert!(host.registry.join("owner.binding").is_file());
        assert_eq!(fs::read_dir(&host.registry).unwrap().count(),1,"no fabricated helper process or second binding");
        identity(&tracking.path)
    });
    wait.recv_timeout(Duration::from_secs(3)).expect("second actual SQL BEGIN proves the first refused and retry backoff elapsed");
    held.lock().unwrap().take().unwrap().execute_batch("ROLLBACK").unwrap();released.send(()).unwrap();
    assert_eq!(worker.join().unwrap(),before);assert!(began.load(Ordering::SeqCst)>=3,"failed fresh BEGIN, successful fresh BEGIN and later exclusion phase");
}
