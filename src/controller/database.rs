use super::{ControllerConfig, bounded_json_size, tracking, validation};
use crate::protocol::*;
use rusqlite::{Connection as SqlConnection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::{path::Path, time::Duration};

/// A borrowed SQL view carrying the exact selected metadata. No second store,
/// global defaults, reload, or mutable policy copy is introduced.
pub(crate) struct StoreContext<'a> {
    sql: &'a SqlConnection,
    pub limits: &'a crate::limits::RuntimeLimits,
}
impl<'a> StoreContext<'a> {
    pub fn new(sql: &'a SqlConnection, limits: &'a crate::limits::RuntimeLimits) -> Self {
        Self { sql, limits }
    }
}
impl std::ops::Deref for StoreContext<'_> {
    type Target = SqlConnection;
    fn deref(&self) -> &Self::Target {
        self.sql
    }
}
type Connection<'a> = StoreContext<'a>;

//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//# The application MUST identify the schema version of its stored task state.
const SCHEMA: i64 = 12;
const APPLICATION: i64 = 0x424f5232;

#[derive(Debug)]
pub(crate) enum Fault {
    Refused(&'static str, String),
    Store(String),
    Validation(Box<validation::Ticket>),
    Repository(Box<super::repository_queries::Ticket>),
    TemporaryValidation(&'static str, &'static str),
    Pending(String),
}
pub(crate) type Result<T> = std::result::Result<T, Fault>;
impl From<rusqlite::Error> for Fault {
    fn from(e: rusqlite::Error) -> Self {
        Self::Store(e.to_string())
    }
}
impl From<serde_json::Error> for Fault {
    fn from(e: serde_json::Error) -> Self {
        Self::Store(e.to_string())
    }
}
pub(crate) fn refuse<T>(code: &'static str, message: impl Into<String>) -> Result<T> {
    Err(Fault::Refused(code, message.into()))
}
pub(crate) fn valid_id(limits: &crate::limits::RuntimeLimits, s: &str) -> bool {
    !s.is_empty() && s.len() <= limits.transport.identity_bytes && !s.chars().any(char::is_control)
}
pub(crate) fn integer(n: u64) -> Result<i64> {
    i64::try_from(n)
        .map_err(|_| Fault::Refused("invalid_input", "integer exceeds durable range".into()))
}
pub(crate) fn unsigned(n: i64) -> Result<u64> {
    u64::try_from(n).map_err(|_| Fault::Store("negative stored unsigned integer".into()))
}
pub(crate) fn encode<T: Serialize>(v: &T) -> Result<String> {
    Ok(serde_json::to_string(v)?)
}
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//# Corrupt stored task state MUST be refused without rewriting its data.
pub(crate) fn decode<T: serde::de::DeserializeOwned + Serialize>(s: &str) -> Result<T> {
    let value: T = serde_json::from_str(s)?;
    // These are our canonical persisted records, not loose external DTOs.
    // Refuse unknown/ignored fields instead of dropping them on a later write.
    if encode(&value)? != s {
        return Err(Fault::Store(
            "stored record is not the supported canonical representation".into(),
        ));
    }
    Ok(value)
}
pub(crate) fn terminal(s: TaskStatus) -> bool {
    matches!(
        s,
        TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::Cancelled
    )
}
pub(crate) fn active(t: &Task) -> bool {
    t.attempt.as_ref().is_some_and(|a| !a.stopped)
}
pub(crate) fn executor(e: ExecutorKind) -> &'static str {
    match e {
        ExecutorKind::Coding => "coding",
        ExecutorKind::Native => "native",
    }
}
fn status(s: TaskStatus) -> &'static str {
    match s {
        TaskStatus::Queued => "queued",
        TaskStatus::Active => "active",
        TaskStatus::Succeeded => "succeeded",
        TaskStatus::Failed => "failed",
        TaskStatus::Cancelled => "cancelled",
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredTask {
    pub task: Task,
    pub generation: u64,
    pub attempt_counter: u64,
    pub depth: u32,
    /// Non-refundable delegated budget reservation. The same root budget cannot
    /// be allocated again by retrying a delegation command.
    pub delegated_model_requests: u32,
    pub executor_success: Option<bool>,
    pub unresolved_execution: bool,
    /// Already reserved terminal receipt slots for this queued task/attempt.
    pub cleanup_remaining: u32,
}

pub(super) enum Response {
    Reply(Reply),
    Validation(Box<validation::Ticket>),
    Repository(Box<super::repository_queries::Ticket>),
}

pub(crate) struct Database {
    validation_issuer: std::sync::Arc<()>,
    pub(super) conn: SqlConnection,
    pub config: ControllerConfig,
    // Declared after conn: close SQLite before releasing this lifetime owner.
    pub validation_host: std::sync::Arc<crate::native::ValidationHost>,
}
impl Database {
    //= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
    //# Committed platform task state MUST survive an application-process restart while the durable store remains intact.
    pub fn open(path: &Path, config: ControllerConfig) -> std::result::Result<Self, String> {
        let mut tracking = tracking::Tracking::acquire(path, &config.limits.controller)?;
        tracking.before_sqlite()?;
        let path = &tracking.path;
        let startup_wal = tracking::StartupWal::before(path)?;
        let mut conn = SqlConnection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|e| e.to_string())?;
        let startup = Self::initialize(&mut conn, &tracking, &config);
        #[cfg(debug_assertions)]
        let startup = startup.and_then(|host| {
            if let Some(probe) = &config.qualification_fail_next_commit {
                use std::sync::{
                    Arc,
                    atomic::{AtomicBool, Ordering},
                };
                // Empty BEGIN IMMEDIATE maintenance/read transactions also
                // commit. Only a real receipt insertion arms the owned callback.
                let receipt_written = Arc::new(AtomicBool::new(false));
                let inserted = receipt_written.clone();
                conn.update_hook(Some(move |action, database: &str, table: &str, _| {
                    if action == rusqlite::hooks::Action::SQLITE_INSERT
                        && database == "main"
                        && table == "receipts"
                    {
                        inserted.store(true, Ordering::SeqCst);
                    }
                }))
                .map_err(|e| e.to_string())?;
                let rolled_back = receipt_written.clone();
                conn.rollback_hook(Some(move || {
                    rolled_back.store(false, Ordering::SeqCst);
                }))
                .map_err(|e| e.to_string())?;
                let probe = probe.clone();
                conn.commit_hook(Some(move || {
                    receipt_written.swap(false, Ordering::SeqCst)
                        && probe.swap(false, Ordering::SeqCst)
                }))
                .map_err(|e| e.to_string())?;
            }
            Ok(host)
        });
        match startup {
            Ok(validation_host) => Ok(Self {
                conn,
                validation_host,
                config,
                validation_issuer: std::sync::Arc::new(()),
            }),
            Err(error) => {
                // Capture only our post-query zero-byte inode, then actually
                // close SQLite before considering its removal. Tracking keeps
                // the real lifetime lock held across both steps.
                let created = startup_wal.observe_created();
                if let Err((connection, close_error)) = conn.close() {
                    drop(connection);
                    return Err(format!(
                        "{error}; startup connection close failed: {close_error}"
                    ));
                }
                let cleanup = created.and_then(|identity| startup_wal.remove_after_close(identity));
                if let Err(cleanup_error) = cleanup {
                    return Err(format!("{error}; {cleanup_error}"));
                }
                Err(error)
            }
        }
    }

    fn initialize(
        conn: &mut SqlConnection,
        tracking: &tracking::Tracking,
        config: &ControllerConfig,
    ) -> std::result::Result<std::sync::Arc<crate::native::ValidationHost>, String> {
        tracking.check_database_identity()?;
        // Pinned SQLite 3.54: NO_CKPT_ON_CLOSE suppresses destructor WAL
        // checkpoint/deletion. EXCLUSIVE before any page read uses a heap WAL
        // index, so retained SHM bytes are not rebuilt. Query-only additionally
        // prohibits application SQL writes during existing-store validation.
        conn.set_db_config(
            rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
            true,
        )
        .map_err(|e| e.to_string())?;
        conn.pragma_update(None, "query_only", !tracking.fresh())
            .map_err(|e| e.to_string())?;
        conn.pragma_update(None, "locking_mode", "EXCLUSIVE")
            .map_err(|e| e.to_string())?;
        conn.busy_timeout(Duration::from_millis(config.sqlite_busy_timeout_ms))
            .map_err(|e| e.to_string())?;
        // Read validation precedes any journal/schema setting. An unknown schema
        // or corrupt store is never "repaired" by writing a new empty database.
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .map_err(|e| e.to_string())?;
        let tables: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'",
                [],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())?;
        let app: i64 = conn
            .query_row("PRAGMA application_id", [], |r| r.get(0))
            .map_err(|e| e.to_string())?;
        let fresh = version == 0 && tables == 0 && app == 0;
        if fresh && !tracking.fresh() {
            return Err(
                "existing controller database is empty or uninitialized; original data retained"
                    .into(),
            );
        }
        //= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
        //# An unsupported stored schema version MUST be refused without rewriting its data.
        if !fresh && (app != APPLICATION || version != SCHEMA) {
            return Err("unsupported controller database schema; clean development reset required; original data retained".into());
        }
        // The OS owner lock also excludes validators whose original DB thread
        // has exited. SQLite continues to own every database transaction.
        if fresh {
            conn.execute_batch("BEGIN EXCLUSIVE; COMMIT;")
                .map_err(|e| e.to_string())?;
            let tx = conn.transaction().map_err(|e| e.to_string())?;
            tx.execute_batch(include_str!("schema12.sql"))
                .map_err(|e| e.to_string())?;
            let id: String = tx
                .query_row(
                    "SELECT value FROM settings WHERE name='database_id'",
                    [],
                    |r| r.get(0),
                )
                .map_err(|e| e.to_string())?;
            let binding = tracking.create(&id)?;
            tx.execute(
                "INSERT INTO settings(name,value) VALUES('native_helper_binding',?1)",
                [&binding],
            )
            .map_err(|e| e.to_string())?;
            tx.commit().map_err(|e| e.to_string())?;
        }
        let context = StoreContext::new(conn, &config.limits);
        let conn = &context;
        //= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
        //# Corrupt stored task state MUST be refused without rewriting its data.
        let check: String = conn
            .query_row("PRAGMA quick_check", [], |r| r.get(0))
            .map_err(|e| e.to_string())?;
        if check != "ok" {
            return Err("corrupt controller database; original data retained".into());
        }
        let database_id: String = conn
            .query_row(
                "SELECT value FROM settings WHERE name='database_id'",
                [],
                |r| r.get(0),
            )
            .map_err(|_| "missing durable database identity".to_string())?;
        if database_id.len() != 32
            || !database_id
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err("corrupt durable database identity".into());
        }
        let binding: String = conn
            .query_row(
                "SELECT value FROM settings WHERE name='native_helper_binding'",
                [],
                |r| r.get(0),
            )
            .map_err(|_| "missing durable helper registry binding".to_string())?;
        tracking.verify(&database_id, &binding)?;
        {
            let mut q = conn
                .prepare("SELECT id FROM tasks ORDER BY seq")
                .map_err(|e| e.to_string())?;
            let mut rows = q.query([]).map_err(|e| e.to_string())?;
            let mut count = 0;
            while let Some(row) = rows.next().map_err(|e| e.to_string())? {
                count += 1;
                if count > config.max_tasks {
                    return Err("existing task count exceeds configured capacity".into());
                }
                let id: String = row.get(0).map_err(|e| e.to_string())?;
                load(conn, &id).map_err(|_| {
                    "corrupt stored task/index association; original bytes retained".to_string()
                })?;
            }
        }
        super::metadata::validate_existing(conn, config).map_err(|_| {
            "invalid controller commit revision or receipt; original bytes retained".to_string()
        })?;
        super::credits::validate_existing(conn, config).map_err(|_| {
            "invalid control/effect credit association; original bytes retained".to_string()
        })?;
        super::operations::releases::validate_existing(conn, config).map_err(|_| {
            "corrupt or unsupported release candidate state; original bytes retained".to_string()
        })?;
        super::protected::validate_existing(conn, config).map_err(|_| {
            "corrupt protected release records; original bytes retained".to_string()
        })?;
        super::compatibility::validate_shape(conn)?;
        super::compatibility::validate_records(conn)?;
        super::repositories::validate_existing(conn)
            .map_err(|_| "invalid repository catalogue; original bytes retained".to_string())?;
        super::sessions::validate_existing(conn, config)?;
        if !cleanup_available(conn, config, 0)
            .map_err(|_| "invalid cleanup reservation ledger".to_string())?
        {
            return Err("existing cleanup reservations exceed configured capacity".into());
        }
        // There is no stale-owner deletion or controller election. The persistent
        // lock inode and SQLite ownership must both be held before recovery.
        let validation_host = std::sync::Arc::new(crate::native::ValidationHost::with_owner_lock(
            config.validation_executable.clone(),
            tracking.registry.clone(),
            tracking.lock.clone(),
            std::sync::Arc::new(config.limits.execution.native.clone()),
            std::sync::Arc::new(crate::native::ValidationRuntime::new(std::sync::Arc::new(
                config.limits.worker.clone(),
            ))?),
        ));
        // Run on this existing database thread, outside any SQL transaction.
        // Readiness is withheld until prior recorded process trees are stopped.
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?
            .block_on(crate::worker::process::reconcile_processes(
                &validation_host.registry,
                &config.limits.worker,
            ))?;
        // Only a validated store and reconciled helper owner may enter normal
        // write mode and restore SQLite's normal checkpoint-on-close behavior.
        conn.pragma_update(None, "query_only", false)
            .map_err(|e| e.to_string())?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE; COMMIT;").map_err(|e|e.to_string())?;
        conn.set_db_config(
            rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
            false,
        )
        .map_err(|e| e.to_string())?;
        Ok(validation_host)
    }

    pub fn next_deadline_delay(&self) -> Result<Option<Duration>> {
        let next: Option<i64> = self.conn.query_row(
            "SELECT min(deadline) FROM tasks WHERE status IN ('queued','active')",
            [],
            |r| r.get(0),
        )?;
        Ok(next.map(|n| Duration::from_millis((n as u64).saturating_sub((self.config.clock)()))))
    }

    pub fn expire(&mut self) -> Result<bool> {
        let now = (self.config.clock)();
        let transaction = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let tx = StoreContext::new(&transaction, &self.config.limits);
        let ids = {
            let mut q=tx.prepare("SELECT id FROM tasks WHERE status IN ('queued','active') AND deadline<=?1 ORDER BY seq")?;
            q.query_map([integer(now)?], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        for id in &ids {
            let mut t = load(&tx, id)?;
            t.task.status = TaskStatus::Failed;
            t.task.blocked_reason = Some("execution_deadline".into());
            stop(&tx, &mut t)?;
            save(&tx, &mut t, "deadline", &serde_json::Value::Null)?;
            settle_parent(&tx, &t)?;
        }
        if !ids.is_empty() {
            super::metadata::advance(&tx)?;
        }
        transaction.commit()?;
        Ok(!ids.is_empty())
    }

    pub fn handle(
        &mut self,
        request: Request,
        proof: Option<super::ExternalProof>,
    ) -> (Response, bool) {
        let retry_id = match &request.command {
            Operation::ReconcileResult(input) => Some(input.operation_id.clone()),
            _ => None,
        };
        let id = request.request_id;
        let result = self.process(&id, &request.command, proof.as_ref());
        match result {
            Ok((reply, changed)) => (Response::Reply(reply), changed),
            Err(Fault::Validation(ticket)) => (Response::Validation(ticket), false),
            Err(Fault::Repository(ticket)) => (Response::Repository(ticket), false),
            Err(Fault::Pending(operation_id)) => (
                Response::Reply(Reply {
                    request_id: id,
                    commit: None,
                    observed_controller_revision: None,
                    outcome: Outcome::Pending { operation_id },
                }),
                false,
            ),
            Err(Fault::TemporaryValidation(code, message)) => {
                (Response::Reply(Reply::refused(id, code, message)), false)
            }
            Err(Fault::Refused("capacity", _)) if retry_id.is_some() => (
                Response::Reply(Reply {
                    request_id: id,
                    commit: None,
                    observed_controller_revision: None,
                    outcome: Outcome::Pending {
                        operation_id: retry_id.unwrap(),
                    },
                }),
                false,
            ),
            Err(Fault::Refused(code, message)) => {
                (Response::Reply(Reply::refused(id, code, message)), false)
            }
            Err(Fault::Store(_)) if retry_id.is_some() => (
                Response::Reply(Reply {
                    request_id: id,
                    commit: None,
                    observed_controller_revision: None,
                    outcome: Outcome::Pending {
                        operation_id: retry_id.unwrap(),
                    },
                }),
                false,
            ),
            Err(Fault::Store(_details)) => (
                Response::Reply(Reply::refused(
                    id,
                    "store_unavailable",
                    "store operation failed; reconcile original operation before retry",
                )),
                false,
            ),
        }
    }

    // Traceability scope: Intact local SQLite persistence, not storage destruction or deployment authentication.
    //= spec/impl/v2/implementation.md#observable-end-to-end-result
    //# The initial system MUST durably record an accepted task before reporting acceptance.
    // Traceability scope: An exact retained receipt is returned before new-operation limit validation, preserving its original outcome and commit.
    //= spec/impl/v2/implementation.md#limits-are-versioned-metadata
    //# A retained operation MUST NOT be discarded solely because a later limit configuration is lower.
    fn process(
        &mut self,
        request_id: &str,
        operation: &Operation,
        proof: Option<&super::ExternalProof>,
    ) -> Result<(Reply, bool)> {
        let limits = &self.config.limits;
        let now = (self.config.clock)();
        let transaction = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let tx = StoreContext::new(&transaction, &self.config.limits);
        let observed = super::metadata::current(&tx)?;
        let request = encode(operation)?;
        let identity = operation_identity(operation);
        let mut cleanup_task = None;
        let mut regular_room = true;
        // Traceability scope: Typed task mutation IDs are validated independently of transport request_id.
        //= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
        //# A mutating request MUST carry an operation identity.
        if let Some(op) = identity {
            if !valid_id(limits, op) {
                return refuse("invalid_input", "invalid operation identity");
            }
            if let Some(receipt) = super::metadata::read(&tx, op)? {
                //= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
                //# Reusing a mutating request's identity with different input MUST fail without changing its recorded outcome.
                if encode(&receipt.request)? != request {
                    return refuse(
                        "operation_conflict",
                        "operation identity already has different input",
                    );
                }
                //= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
                //# An exact retry of a committed mutating request MUST return its recorded outcome.
                let reply = super::metadata::reply(
                    &self.config.limits,
                    request_id,
                    operation,
                    receipt.outcome,
                    Some(receipt.commit),
                    observed,
                )?;
                return Ok((reply, false));
            }
            regular_room = regular_receipt_room(&tx, &self.config)?;
            cleanup_task = cleanup_candidate(&tx, operation)?;
            if !regular_room
                && cleanup_task.is_none()
                && !matches!(operation, Operation::AwaitCommands { .. })
                && super::credits::final_candidate(&tx, operation)?.is_none()
            {
                return refuse(
                    "capacity",
                    "durable receipt capacity exhausted: controller.max_receipts and controller.max_cleanup_receipts",
                );
            }
        }
        // The accepted task owns its full adapter-result reservation. Current
        // HTTP/storage limits remain separate service bounds. Unknown callers
        // have no accepted authority and use the explicit service default.
        let call_task = if let Operation::CallCollaboration { call } = operation {
            match load(&tx, &call.caller.task_id) {
                Ok(task) => Some(task),
                Err(Fault::Refused("not_found", _)) => None,
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        tx.execute_batch("SAVEPOINT request_action")?;
        let context = validation::Context {
            issuer: &self.validation_issuer,
            limits: &self.config.limits,
            request: &request,
            proof: match proof {
                Some(super::ExternalProof::Native(p)) => Some(p),
                _ => None,
            },
            repository: match proof {
                Some(super::ExternalProof::Repository(p)) => Some(p),
                _ => None,
            },
        };
        let mut control_credit = None;
        let final_credit = super::credits::final_candidate(&tx, operation)?;
        let action = if matches!(operation, Operation::AwaitCommands { .. }) {
            super::operations::await_request(&tx, operation, &self.config, now).map(
                |(data, changed, credit)| {
                    control_credit = credit;
                    (data, changed)
                },
            )
        } else {
            super::operations::apply(&tx, operation, &self.config, now, &context)
        };
        let (mut outcome, mut changed) = match action {
            Ok((data, changed)) => {
                // Mutation receipts keep the snapshot observed at their own
                // commit; future Get/List recompute diagnostics, not receipts.
                let data = match data {
                    Data::Task(task) if identity.is_some() && task.status == TaskStatus::Queued => {
                        Data::Task(Box::new(super::operations::view(
                            &tx,
                            load(&tx, &task.id)?,
                            &self.config,
                            now,
                        )?))
                    }
                    other => other,
                };
                (Outcome::Ok { data }, changed)
            }
            Err(Fault::TemporaryValidation(code, message)) => {
                if let Operation::ReconcileResult(input) = operation {
                    return Err(Fault::Pending(input.operation_id.clone()));
                }
                tx.execute_batch("ROLLBACK TO request_action")?;
                (
                    Outcome::Refused {
                        code: code.into(),
                        message: message.into(),
                    },
                    false,
                )
            }
            Err(Fault::Refused(code, message)) => {
                tx.execute_batch("ROLLBACK TO request_action")?;
                (
                    Outcome::Refused {
                        code: code.into(),
                        message,
                    },
                    false,
                )
            }
            Err(e) => return Err(e),
        };
        if let Operation::CallCollaboration { call } = operation {
            let (transport, cap) = match &call_task {
                Some(task) => {
                    let snapshot = task
                        .task
                        .input
                        .resolved_limits
                        .as_ref()
                        .ok_or_else(|| Fault::Store("caller limits absent".into()))?;
                    let execution = task.task.input.execution_limits().map_err(Fault::Store)?;
                    (
                        &snapshot.global_metadata.transport,
                        execution.collaboration.complete_result_bytes,
                    )
                }
                None => (
                    &self.config.limits.transport,
                    self.config
                        .limits
                        .execution
                        .collaboration
                        .complete_result_bytes,
                ),
            };
            if super::metadata::complete_call_bound(transport, call, &outcome, cap).is_err() {
                tx.execute_batch("ROLLBACK TO request_action")?;
                outcome = super::metadata::complete_call_refusal();
                changed = false;
                // Metadata admission guarantees the bounded refusal fits. A
                // corrupt or mismatched policy fails without any commit.
                super::metadata::complete_call_bound(transport, call, &outcome, cap)?;
            }
        }
        tx.execute_batch("RELEASE request_action")?;
        if bounded_json_size(&outcome, self.config.outcome_bytes).is_err() {
            return refuse(
                "capacity",
                "operation response exceeds response bound: controller.outcome_bytes",
            );
        }
        if identity.is_some() {
            super::metadata::receipt_bound(&self.config.limits, operation, &outcome)?;
        }
        let mut commit = None;
        if let Some(op) = identity {
            let successful_change = changed && matches!(&outcome, Outcome::Ok { .. });
            let task_cleanup = cleanup_task.is_some() && successful_change;
            let control_cleanup = control_credit.is_some() && successful_change && !regular_room;
            let effect_cleanup = final_credit.is_some() && successful_change;
            let cleanup = task_cleanup || control_cleanup || effect_cleanup;
            if !cleanup && !regular_room {
                // A malformed/repeated/nonterminal request cannot spend the
                // reserved cleanup slots. Its action has not been committed.
                return refuse(
                    "capacity",
                    "normal receipt capacity is full; cleanup reserve was not consumed: controller.max_receipts",
                );
            }
            if task_cleanup {
                let mut t = load(&tx, cleanup_task.as_deref().unwrap())?;
                t.cleanup_remaining = t
                    .cleanup_remaining
                    .checked_sub(1)
                    .ok_or_else(|| Fault::Store("cleanup reservation missing".into()))?;
                tx.execute(
                    "UPDATE tasks SET body=?2,cleanup=?3 WHERE id=?1",
                    params![t.task.id, encode(&t)?, t.cleanup_remaining],
                )?;
            }
            if control_cleanup {
                super::credits::debit_control(&tx, control_credit.as_deref().unwrap())?;
            }
            if effect_cleanup {
                super::credits::debit_effect(&tx, final_credit.as_deref().unwrap())?;
            }
            let revision = super::metadata::advance(&tx)?;
            tx.execute(
                //= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
                //# Task submission MUST commit its input and submission outcome atomically.
                "INSERT INTO receipts(id,request,outcome,cleanup,controller_revision) VALUES(?1,?2,?3,?4,?5)",
                params![op, request, encode(&outcome)?, cleanup, integer(revision)?],
            )?;
            commit = Some(MutationCommit {
                operation_id: op.into(),
                controller_revision: revision,
            });
        } else if let (
            Operation::GetOperation { operation_id }
            | Operation::GetCollaborationCall { operation_id, .. },
            Outcome::Ok {
                data: Data::Operation(record),
            },
        ) = (operation, &outcome)
        {
            // This stamp binds the original nested receipt, never mutable effect state.
            if record.kind == "application_mutation" {
                let receipt = super::metadata::read(&tx, operation_id)?
                    .ok_or_else(|| Fault::Store("mutation lookup lost original receipt".into()))?;
                if record.intended != serde_json::to_value(&receipt.request)?
                    || record.outcome.as_ref() != Some(&serde_json::to_value(&receipt.outcome)?)
                {
                    return Err(Fault::Store(
                        "mutation lookup changed original outcome".into(),
                    ));
                }
                commit = Some(receipt.commit);
            }
        }
        let observed = super::metadata::current(&tx)?;
        let reply = super::metadata::reply(
            &self.config.limits,
            request_id,
            operation,
            outcome,
            commit,
            observed,
        )?;
        transaction.commit()?;
        //= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
        //# A terminal task result MUST be committed before it is reported as final.
        Ok((reply, changed))
    }
}

pub(crate) fn operation_identity(op: &Operation) -> Option<&str> {
    op.mutation_id()
}

pub(crate) fn regular_receipt_room(tx: &Connection, cfg: &ControllerConfig) -> Result<bool> {
    let n: i64 = tx.query_row("SELECT count(*) FROM receipts WHERE cleanup=0", [], |r| {
        r.get(0)
    })?;
    Ok(unsigned(n)? < cfg.max_receipts as u64)
}

pub(crate) fn cleanup_available(
    tx: &Connection,
    cfg: &ControllerConfig,
    extra: u32,
) -> Result<bool> {
    let used: i64 = tx.query_row("SELECT count(*) FROM receipts WHERE cleanup=1", [], |r| {
        r.get(0)
    })?;
    let reserved: i64 = tx.query_row("SELECT coalesce(sum(cleanup),0) FROM tasks", [], |r| {
        r.get(0)
    })?;
    let control: i64 = tx.query_row(
        "SELECT coalesce(sum(control_remaining),0) FROM attempts",
        [],
        |r| r.get(0),
    )?;
    let effects: i64 = tx.query_row(
        "SELECT coalesce(sum(finalization_reserved),0) FROM external_operations",
        [],
        |r| r.get(0),
    )?;
    let total = unsigned(used)?
        .checked_add(unsigned(reserved)?)
        .and_then(|n| n.checked_add(u64::try_from(control).ok()?))
        .and_then(|n| n.checked_add(u64::try_from(effects).ok()?))
        .and_then(|n| n.checked_add(u64::from(extra)));
    Ok(total.is_some_and(|n| n <= cfg.max_cleanup_receipts as u64))
}

fn cleanup_candidate(tx: &Connection, op: &Operation) -> Result<Option<String>> {
    if let Operation::CallCollaboration { call } = op {
        return match super::collaboration_calls::cancel_operation(tx.limits, call) {
            Some(inner) => cleanup_candidate(tx, &inner),
            None => Ok(None),
        };
    }
    if let Operation::ReconcileResult(input) = op {
        return match super::operations::reconciliation::original_report(
            tx,
            &input.original_report_id,
        ) {
            Ok(report) => cleanup_candidate(tx, &Operation::ObserveWorker { report }),
            Err(Fault::Refused(..)) => Ok(None),
            Err(error) => Err(error),
        };
    }
    let id = match op {
        Operation::CancelTask { task_id, .. } => task_id,
        Operation::ObserveWorker { report } => &report.task_id,
        _ => return Ok(None),
    };
    let t = match load(tx, id) {
        Ok(t) => t,
        Err(Fault::Refused(..)) => return Ok(None),
        Err(e) => return Err(e),
    };
    if t.cleanup_remaining == 0 {
        return Ok(None);
    }
    let eligible = match op {
        Operation::CancelTask {
            expected_revision, ..
        } => !terminal(t.task.status) && t.task.revision == *expected_revision,
        Operation::ObserveWorker { report } => {
            let Some(a) = &t.task.attempt else {
                return Ok(None);
            };
            if a.id != report.attempt_id
                || a.generation != report.generation
                || a.worker_id != report.worker_id
                || a.worker_incarnation != report.incarnation
            {
                return Ok(None);
            }
            match &report.observation {
                Observation::ProcessTreeStopped => !a.stopped,
                Observation::ExecutorOutcome { .. } => t.executor_success.is_none(),
                Observation::ExecutionUncertain { .. } => !t.unresolved_execution,
                Observation::ResultCandidate { .. } => {
                    t.task.status == TaskStatus::Active
                        && a.phase != AttemptPhase::StopRequested
                        && t.task.waiting_child_id.is_none()
                }
                _ => false,
            }
        }
        _ => false,
    };
    Ok(eligible.then(|| id.clone()))
}

pub(crate) fn next(tx: &Connection, kind: &str) -> Result<u64> {
    tx.execute(
        "INSERT OR IGNORE INTO counters(kind,value) VALUES(?1,0)",
        [kind],
    )?;
    let n:i64=tx.query_row("UPDATE counters SET value=value+1 WHERE kind=?1 AND value<9223372036854775807 RETURNING value",[kind],|r|r.get(0))?;
    Ok(n as u64)
}
pub(crate) fn load(tx: &Connection, id: &str) -> Result<StoredTask> {
    type Row = (
        String,
        String,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        bool,
        i64,
        u32,
    );
    let row: Option<Row> = tx
        .query_row("SELECT body,session,agent,executor,status,worker,incarnation,active,deadline,cleanup FROM tasks WHERE id=?1", [id],
            |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?,r.get(9)?)))
        .optional()?;
    match row {
        Some((
            body,
            session,
            agent,
            kind,
            state,
            worker,
            incarnation,
            is_active,
            deadline,
            cleanup,
        )) => {
            let t: StoredTask = decode(&body)?;
            let a = t.task.attempt.as_ref();
            if t.task.id != id
                || t.task.revision == 0
                || session != t.task.session_id
                || agent != t.task.assigned_agent_id
                || agent != t.task.input.definition.id
                || kind != executor(t.task.input.definition.executor)
                || state != status(t.task.status)
                || worker.as_deref() != a.map(|a| a.worker_id.as_str())
                || incarnation.as_deref() != a.map(|a| a.worker_incarnation.as_str())
                || is_active != active(&t.task)
                || deadline != integer(t.task.input.limits.deadline_unix_ms)?
                || cleanup != t.cleanup_remaining
                || (a.is_none() && (t.generation != 0 || t.attempt_counter != 0))
                || a.is_some_and(|a| {
                    a.task_id != t.task.id
                        || a.session_id != t.task.session_id
                        || a.generation != t.generation
                        || a.deadline_unix_ms != t.task.input.limits.deadline_unix_ms
                })
            {
                return Err(Fault::Store("task/index identity mismatch".into()));
            }
            t.task.input.execution_limits().map_err(|_| {
                Fault::Store("stored task lacks exact accepted limits metadata".into())
            })?;
            let mut checked_input = t.task.input.clone();
            let accepted_policy = t
                .task
                .input
                .resolved_limits
                .as_ref()
                .ok_or_else(|| Fault::Store("stored task policy absent".into()))?
                .global_metadata
                .as_ref();
            let accepted_context = StoreContext::new(tx, accepted_policy);
            super::repositories::resolve_input(&accepted_context, &mut checked_input, true)
                .map_err(|_| {
                    Fault::Store(
                        "stored task repository binding differs from immutable registration".into(),
                    )
                })?;
            if checked_input != t.task.input {
                return Err(Fault::Store("stored repository projection changed".into()));
            }
            if let Some(cp) = &t.task.selected_checkpoint {
                let repo =
                    t.task.input.repository.as_ref().ok_or_else(|| {
                        Fault::Store("selected checkpoint has no repository".into())
                    })?;
                if Some(&cp.registration) != repo.registration.as_ref()
                    || cp.repository != repo.checkpoint_remote
                {
                    return Err(Fault::Store(
                        "selected checkpoint registration differs from task".into(),
                    ));
                }
            }
            super::sessions::validate_task(tx, &t.task)?;
            Ok(t)
        }
        None => refuse("not_found", "task not found"),
    }
}
pub(crate) fn insert(tx: &Connection, t: &StoredTask, seq: u64) -> Result<()> {
    tx.execute("INSERT INTO tasks(id,seq,session,agent,executor,status,active,deadline,body,cleanup) VALUES(?1,?2,?3,?4,?5,?6,0,?7,?8,?9)",params![t.task.id,integer(seq)?,t.task.session_id,t.task.assigned_agent_id,executor(t.task.input.definition.executor),status(t.task.status),integer(t.task.input.limits.deadline_unix_ms)?,encode(t)?,t.cleanup_remaining])?;
    history(tx, &t.task, "submitted", &serde_json::Value::Null)
}
//= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
//# The platform MUST persist task history in its database.
pub(crate) fn history<T: Serialize>(tx: &Connection, t: &Task, kind: &str, body: &T) -> Result<()> {
    tx.execute(
        "INSERT INTO history(task_id,revision,kind,body) VALUES(?1,?2,?3,?4)",
        params![t.id, integer(t.revision)?, kind, encode(body)?],
    )?;
    Ok(())
}
//= spec/impl/v2/implementation.md#task-state-separates-execution-from-acceptance
//# An accepted terminal result MUST commit the task state and corresponding history atomically.
// Traceability scope: Stores task status, checkpoint and history in the controller database.
//= spec/impl/v2/implementation.md#interrupted-work-is-reconciled
//# An interrupted attempt MUST retain its last committed status and checkpoint references.
pub(crate) fn save<T: Serialize>(
    tx: &Connection,
    t: &mut StoredTask,
    kind: &str,
    event: &T,
) -> Result<()> {
    super::sessions::validate_task(tx, &t.task)?;
    t.task.revision = t
        .task
        .revision
        .checked_add(1)
        .ok_or_else(|| Fault::Store("revision exhausted".into()))?;
    let a = t.task.attempt.as_ref();
    tx.execute("UPDATE tasks SET agent=?2,executor=?3,status=?4,worker=?5,incarnation=?6,active=?7,deadline=?8,body=?9,cleanup=?10 WHERE id=?1",params![t.task.id,t.task.assigned_agent_id,executor(t.task.input.definition.executor),status(t.task.status),a.map(|x|x.worker_id.as_str()),a.map(|x|x.worker_incarnation.as_str()),active(&t.task),integer(t.task.input.limits.deadline_unix_ms)?,encode(t)?,t.cleanup_remaining])?;
    if let Some(a) = a {
        tx.execute("INSERT INTO attempts(id,task_id,body) VALUES(?1,?2,?3) ON CONFLICT(id) DO UPDATE SET body=excluded.body",params![a.id,t.task.id,encode(a)?])?;
    }
    history(tx, &t.task, kind, event)
}
pub(crate) fn command(
    tx: &Connection,
    w: &str,
    inc: &str,
    task: &str,
    attempt: &str,
    generation: u64,
    body: &WorkerCommand,
    id: &str,
) -> Result<()> {
    tx.execute("INSERT OR IGNORE INTO commands(id,worker,incarnation,task_id,attempt_id,generation,body) VALUES(?1,?2,?3,?4,?5,?6,?7)",params![id,w,inc,task,attempt,integer(generation)?,encode(body)?])?;
    Ok(())
}
//= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
//# A cancellation request MUST commit cancellation intent and its stop command atomically.
pub(crate) fn stop(tx: &Connection, t: &mut StoredTask) -> Result<()> {
    if let Some(a) = t.task.attempt.as_mut().filter(|a| !a.stopped) {
        a.phase = AttemptPhase::StopRequested;
        // A run superseded by its durable stop is not redelivered. If it was
        // already received, the separate stop still requires real cleanup.
        tx.execute(
            "UPDATE commands SET acknowledged=1 WHERE id=?1",
            [format!("run-{}", a.id)],
        )?;
        let id = format!("stop-{}", a.id);
        let c = WorkerCommand::StopAttempt {
            command_id: id.clone(),
            task_id: t.task.id.clone(),
            attempt_id: a.id.clone(),
            generation: a.generation,
            incarnation: a.worker_incarnation.clone(),
        };
        command(
            tx,
            &a.worker_id,
            &a.worker_incarnation,
            &t.task.id,
            &a.id,
            a.generation,
            &c,
            &id,
        )?;
    }
    Ok(())
}
//= spec/impl/v2/implementation.md#delegation-is-a-task-handoff
//# A child's terminal outcome MUST settle its parent-wait association before the parent resumes from that outcome.
pub(crate) fn settle_parent(tx: &Connection, t: &StoredTask) -> Result<()> {
    if !terminal(t.task.status) {
        return Ok(());
    }
    let parent: Option<String> = tx
        .query_row(
            "SELECT parent FROM waits WHERE child=?1 AND state='pending'",
            [&t.task.id],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(parent) = parent {
        tx.execute(
            "UPDATE waits SET state='settled',outcome=?2 WHERE child=?1",
            params![t.task.id, encode(&t.task)?],
        )?;
        let mut p = load(tx, &parent)?;
        if p.task.waiting_child_id.as_deref() == Some(&t.task.id) {
            p.task.waiting_child_id = None;
            p.task.completed_child = Some(ChildOutcome {
                task_id: t.task.id.clone(),
                status: t.task.status,
                result: t.task.result.clone(),
                summary: t
                    .task
                    .result
                    .as_ref()
                    .map(|r| r.summary.clone())
                    .or_else(|| t.task.blocked_reason.clone())
                    .unwrap_or_default(),
            });
            if bounded_json_size(&p.task, tx.limits.controller.outcome_bytes).is_err() {
                return refuse(
                    "capacity",
                    "inline child outcome exceeds parent response bound; child acceptance was not committed",
                );
            }
            if !terminal(p.task.status) && !active(&p.task) {
                p.task.status = TaskStatus::Queued;
                p.task.blocked_reason = Some("child_outcome_ready".into());
            }
            save(tx, &mut p, "child_settled", &t.task.id)?;
        }
    }
    Ok(())
}
