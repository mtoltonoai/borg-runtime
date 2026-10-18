use super::{ControllerConfig, bounded_json_size, tracking, validation};
use crate::protocol::*;
use rusqlite::{Connection as SqlConnection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{path::Path, time::Duration};

/// A borrowed SQL view carrying the exact selected metadata. No second store,
/// global defaults, reload, or mutable policy copy is introduced.
pub(crate) struct StoreContext<'a> {
    /// Only the closed public read dispatcher sets this while it fits a page.
    /// Source admission and mutation limits are never bypassed by this flag.
    pub(super) fitting_read: bool,
    pub(super) machine_resources: Option<&'a super::machine_resources::State>,
    sql: &'a SqlConnection,
    pub(super) bound_actor: Option<crate::collaboration::workflow::RecordedActor>,
    pub limits: &'a crate::limits::RuntimeLimits,
    /// Established by the in-process invocation path, never by request fields.
    pub invocation_path: crate::collaboration::workflow::TrustedHostPath,
    /// Root transaction policy and buffer survive nested accepted-policy views.
    pub(super) workflow_policy: Option<&'a crate::limits::WorkflowLimits>,
    pub(super) workflow_terminals: super::collaboration_finalizer::TerminalBuffer,
    /// Current root change policy is never replaced by accepted execution limits.
    pub(super) governance_limits: &'a crate::limits::RuntimeLimits,
    /// Shared across accepted-policy views and action savepoint rollback only.
    pub(super) governance_decisions: super::governance::DecisionBuffer,
}
impl<'a> StoreContext<'a> {
    pub fn new(sql: &'a SqlConnection, limits: &'a crate::limits::RuntimeLimits) -> Self {
        Self { sql, limits, fitting_read: false, bound_actor: None, machine_resources: None, invocation_path: crate::collaboration::workflow::TrustedHostPath::OperatorApi,
            workflow_policy: limits.workflow.as_ref(),
            workflow_terminals: super::collaboration_finalizer::TerminalBuffer::new(),
            governance_limits: limits, governance_decisions: super::governance::DecisionBuffer::new() }
    }
    /// Borrow another accepted policy without losing request provenance or the
    /// root transaction's terminal buffer. This never creates a SQL transaction.
    pub fn with_limits<'b>(&'b self, limits: &'b crate::limits::RuntimeLimits) -> StoreContext<'b> {
        StoreContext { sql: self.sql, limits, fitting_read: self.fitting_read, bound_actor: self.bound_actor.clone(), machine_resources: self.machine_resources, invocation_path: self.invocation_path,
            workflow_policy: self.workflow_policy, workflow_terminals: self.workflow_terminals.clone(),
            governance_limits: self.governance_limits, governance_decisions: self.governance_decisions.clone() }
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
const SCHEMA: i64 = super::application_schema::VERSION;
const APPLICATION: i64 = 0x424f5232;

#[derive(Debug)]
pub(crate) enum Fault {
    Refused(&'static str, String),
    Store(String),
    Sqlite(rusqlite::Error),
    Transaction(Box<super::checked_transaction::Failure>),
    Validation(Box<validation::Ticket>),
    Repository(Box<super::repository_queries::Ticket>),
    TemporaryValidation(&'static str, &'static str),
    Pending(String),
}
pub(crate) type Result<T> = std::result::Result<T, Fault>;
impl From<rusqlite::Error> for Fault {
    fn from(e: rusqlite::Error) -> Self {
        Self::Sqlite(e)
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
/// A host-sized configured SQL bound must fit SQLite's signed integer domain.
/// Refuse an unrepresentable value instead of wrapping it into a negative LIMIT.
pub(crate) fn sql_bound(n: usize) -> Result<i64> {
    i64::try_from(n)
        .map_err(|_| Fault::Refused("input_bound", "configured SQL bound exceeds durable range".into()))
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

// Traceability scope: Physical slot accounting retains an attempt until its stopped observation is committed.
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//# A task suspended on a collaboration wait MUST release its execution slot after its current execution has stopped.

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
        TaskStatus::Blocked => "blocked",
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_hold: Option<crate::governance::CompletionHold>,
}

pub(super) enum Response {
    Reply(Reply),
    Validation(Box<validation::Ticket>),
    Repository(Box<super::repository_queries::Ticket>),
    StoreFailure(Box<StoreFailure>),
}

/// No wire or durable representation: the original request/proof and typed cause
/// remain together until the application boundary selects its existing outcome.
pub(super) struct StoreFailure {
    pub(super) original: super::OriginalRequest,
    cause: Fault,
}
impl StoreFailure {
    pub(super) fn disposition(&self) -> super::checked_transaction::RetryDisposition {
        match &self.cause {
            Fault::Transaction(failure) => failure.retry_disposition(),
            _ => super::checked_transaction::RetryDisposition::Ineligible,
        }
    }

    /// Does not dispatch, sleep, or grant a retry lifetime. The chosen consumer
    /// must supply an applicable accepted policy and rerun normal admission.
    /// Keeping the proof in this owned bundle prevents an accidental second
    /// validator/repository operation while reconciling the same transaction.
    pub(super) fn into_retry_candidate(self) -> std::result::Result<super::OriginalRequest, Self> {
        if matches!(self.disposition(),
            super::checked_transaction::RetryDisposition::RolledBackTransient { .. }) {
            Ok(self.original)
        } else { Err(self) }
    }

    pub(super) fn into_reply(self) -> Reply {
        let request = self.original.request;
        match request.command {
            Operation::ReconcileResult(input) => Reply {
                request_id: request.request_id,
                commit: None,
                observed_controller_revision: None,
                outcome: Outcome::Pending { operation_id: input.operation_id },
            },
            _ => Reply::refused(request.request_id, "store_unavailable",
                "store operation failed; reconcile original operation before retry"),
        }
    }
}

pub(crate) struct Database {
    machine_resources: super::machine_resources::State,
    validation_issuer: std::sync::Arc<()>,
    pub(super) conn: SqlConnection,
    pub config: ControllerConfig,
    pub(super) maintenance_pending: bool,
    // Declared after conn: close SQLite before releasing this lifetime owner.
    pub validation_host: std::sync::Arc<crate::native::ValidationHost>,
}
impl Database {
    //= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
    //# Committed platform task state MUST survive an application-process restart while the durable store remains intact.
    pub fn open(path: &Path, config: ControllerConfig) -> std::result::Result<Self, String> {
        Self::open_cancellable(path, config, &super::StartupCancellation::default())
    }
    pub(super) fn open_cancellable(path: &Path, config: ControllerConfig,
        cancellation: &super::StartupCancellation) -> std::result::Result<Self, String> {
        cancellation.check()?;
        let mut tracking = tracking::Tracking::acquire_cancellable(path, &config.limits.controller, cancellation)?;
        cancellation.check()?;
        tracking.before_sqlite()?;
        let path = &tracking.path;
        let startup_wal = tracking::StartupWal::before(path)?;
        let mut conn = SqlConnection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|e| e.to_string())?;
        let startup = Self::initialize(&mut conn, &tracking, &config, cancellation);
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
                machine_resources: super::machine_resources::State::default(),
                conn,
                validation_host,
                config,
                maintenance_pending: false,
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
        sql: &mut SqlConnection,
        tracking: &tracking::Tracking,
        config: &ControllerConfig,
        cancellation: &super::StartupCancellation,
    ) -> std::result::Result<std::sync::Arc<crate::native::ValidationHost>, String> {
        let conn = &mut *sql;
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
        if !fresh && (app != APPLICATION || ![12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, SCHEMA].contains(&version)) {
            return Err("unsupported controller database schema; clean development reset required; original data retained".into());
        }
        // The OS owner lock also excludes validators whose original DB thread
        // has exited. SQLite continues to own every database transaction.
        if fresh {
            // Capture identity once before any retryable body. The schema's
            // ordinary generator is replaced by this parameterized insertion,
            // so rollback never draws or substitutes a second identity.
            let mut original = super::store_phase::FreshSchema::new(conn).map_err(|e| format!("{e:?}"))?;
            super::store_phase::transaction(conn, config.store_retry.as_ref(), cancellation, true, |tx| {
                original.write(tx, tracking)?;
                Ok(super::checked_transaction::Completion::Commit(()))
            })?;
        }
        let stored_version = if fresh { SCHEMA } else { version };
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
                let stored = load(conn, &id).map_err(|_| {
                    "corrupt stored task/index association; original bytes retained".to_string()
                })?;
                super::native_retry_validation::validate_task(conn, &stored.task)
                    .map_err(|_| "invalid retained native retry block; original bytes retained".to_string())?;
                // Outside load: the original receipt validator itself loads its task.
                // Historical absence is inert; present state must have its exact audit.
                if let Some(hold) = &stored.completion_hold {
                    super::governance_completion::validate_hold(conn, &stored.task, hold)
                        .map_err(|_| "invalid retained completion hold; original bytes retained".to_string())?;
                }
            }
        }
        super::read_retention::admit(conn,stored_version).map_err(|e|format!("read retention admission: {e:?}"))?;
        super::metadata::validate_existing(conn, config).map_err(|_| {
            "invalid controller commit revision or receipt; original bytes retained".to_string()
        })?;
        super::read_pagination::validate_existing(conn, config, stored_version)
            .map_err(|_| "read pagination vocabulary requires schema26; original bytes retained".to_string())?;
        super::machine_resources::validate_existing(conn, config, stored_version)
            .map_err(|_| "machine resource vocabulary requires schema25; original bytes retained".to_string())?;
        super::credits::validate_existing(conn, config).map_err(|_| {
            "invalid control/effect credit association; original bytes retained".to_string()
        })?;
        super::native_request_diagnostics::validate_existing(conn, stored_version >= 18)
            .map_err(|_| "invalid native diagnostic credit association; original bytes retained".to_string())?;
        super::operations::releases::validate_existing(conn, config).map_err(|_| {
            "corrupt or unsupported release candidate state; original bytes retained".to_string()
        })?;
        super::protected::validate_existing(conn, config).map_err(|_| {
            "corrupt protected release records; original bytes retained".to_string()
        })?;
        super::compatibility::validate_shape(conn, stored_version)?;
        super::compatibility::validate_records(conn)?;
        super::identity_limit_fields::validate_existing(conn, config, stored_version)
            .map_err(|e| format!("identity limit field admission: {e:?}"))?;
        super::identity_administration::validate_existing(conn, config, stored_version)
            .map_err(|_| "invalid identity/task creation records; original bytes retained".to_string())?;
        super::operations::supplied_outer::validate_existing(conn,config,stored_version).map_err(|e|format!("supplied retry admission: {e:?}"))?;
        super::task_submission::validate_existing(conn, config, stored_version)
            .map_err(|_| "invalid retained default assignment; original bytes retained".to_string())?;
        super::agents::validate_existing(conn, config, stored_version)
            .map_err(|_| "invalid retained agent lifecycle; original bytes retained".to_string())?;
        super::repositories::validate_existing(conn)
            .map_err(|_| "invalid repository catalogue; original bytes retained".to_string())?;
        super::repositories::validate_instruction_sources(conn, config, stored_version)
            .map_err(|_| "invalid retained instruction source; original bytes retained".to_string())?;
        if stored_version >= super::workflow_schema::VERSION {
            super::collaboration_workflow::validate_existing(conn, (config.clock)())
                .map_err(|_| "invalid stored collaboration state; original bytes retained".to_string())?;
        }
        super::sessions::validate_existing(conn, config, stored_version >= super::workflow_schema::VERSION)?;
        super::task_handoffs::validate_existing(conn, config, stored_version).map_err(|_| "invalid task handoff lineage; original bytes retained".to_string())?;
        super::publication_requests::validate_existing(conn, config, stored_version)
            .map_err(|e| format!("invalid retained shared publication: {e:?}; original bytes retained"))?;
        super::pull_request::validate_existing(conn, config, stored_version)
            .map_err(|_| "invalid retained PR publication; original bytes retained".to_string())?;
        // Root's composed schema14 has all additive governance structures.
        // Historical schemas use their exact query-only readers before migration.
        if stored_version >= 14 {
            super::charters::validate_existing(conn)
                .map_err(|_| "invalid stored charter state; original bytes retained".to_string())?;
            super::native_request_coordination::validate_stored(conn, config)
                .map_err(|_| "invalid stored native coordination; original bytes retained".to_string())?;
            super::governance_validation::validate_stored(conn, config)
                .map_err(|_| "invalid retained governance; original bytes retained".to_string())?;
        }
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
        cancellation.check()?;
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?
            .block_on(crate::worker::process::reconcile_processes(
                &validation_host.registry,
                &config.limits.worker,
            ))?;
        cancellation.check()?;
        // Only a validated store and reconciled helper owner may enter normal
        // write mode and restore SQLite's normal checkpoint-on-close behavior.
        conn.pragma_update(None, "query_only", false)
            .map_err(|e| e.to_string())?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA locking_mode=EXCLUSIVE;").map_err(|e|e.to_string())?;
        // Release the metadata view before borrowing the same owned connection.
        let conn = &mut *sql;
        super::store_phase::transaction(conn, config.store_retry.as_ref(), cancellation, true,
            |_| Ok(super::checked_transaction::Completion::Commit(())))?;
        if stored_version < SCHEMA {
            // Exact historical shape and records passed read-only validation.
            // Additive steps and the vocabulary barrier share one owner transaction: failed COMMIT
            // preserves the original version and every retained body.
            super::store_phase::transaction(conn, config.store_retry.as_ref(), cancellation, false, |tx| {
                tracking.verify(&database_id, &binding).map_err(Fault::Store)?;
                super::application_schema::upgrade_checked(tx, stored_version)?;
                Ok(super::checked_transaction::Completion::Commit(()))
            })?;
        }
        conn.set_db_config(
            rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
            false,
        )
        .map_err(|e| e.to_string())?;
        Ok(validation_host)
    }

    #[cfg(test)]
    pub(super) fn initialize_for_qualification(sql: &mut SqlConnection, tracking: &tracking::Tracking,
        config: &ControllerConfig, cancellation: &super::StartupCancellation)
        -> std::result::Result<std::sync::Arc<crate::native::ValidationHost>, String> {
        Self::initialize(sql, tracking, config, cancellation)
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
        self.expire_at(now)
    }

    pub(super) fn expire_at(&mut self, now: u64) -> Result<bool> {
        super::checked_transaction::immediate(&mut self.conn, |sql| {
        let tx = StoreContext::new(sql, &self.config.limits);
        let ids = {
            let mut q=tx.prepare("SELECT id FROM tasks WHERE status IN ('queued','active') AND deadline<=?1 ORDER BY seq")?;
            q.query_map([integer(now)?], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        for id in &ids {
            let mut t = load(&tx, id)?;
            // This controller-owned expiry consumes only an already committed
            // rejection. It is not a synthetic worker report or an unknown-POST
            // reconciliation, and the typed nonterminal hold does not settle a
            // waiting parent as though this task had failed.
            if let Some(block) = super::operations::native_retry::deadline_block(&tx, &mut t, now)? {
                save(&tx, &mut t, "native_retry_deadline", &block)?;
                continue;
            }
            t.task.status = TaskStatus::Failed;
            t.task.blocked_reason = Some("execution_deadline".into());
            stop(&tx, &mut t)?;
            save(&tx, &mut t, "deadline", &serde_json::Value::Null)?;
            settle_parent(&tx, &t)?;
        }
        if !ids.is_empty() {
            super::collaboration_finalizer::finalize_expiry(&tx, now)?;
            super::metadata::advance(&tx)?;
        }
        Ok(super::checked_transaction::Completion::Commit(!ids.is_empty()))
        })
    }

    pub fn handle(
        &mut self,
        original: super::OriginalRequest,
    ) -> (Response, bool) {
        self.handle_mode(original, false)
    }

    pub(super) fn handle_replay(&mut self, original: super::OriginalRequest) -> (Response, bool) {
        self.handle_mode(original, true)
    }

    fn handle_mode(&mut self, original: super::OriginalRequest, receipt_only: bool) -> (Response, bool) {
        let request = &original.request;
        let retry_id = match &request.command {
            Operation::ReconcileResult(input) => Some(input.operation_id.clone()),
            _ => None,
        };
        let id = request.request_id.clone();
        let result = self.process(&id, &request.command, original.validation.as_ref(), original.invocation_path, receipt_only);
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
            Err(cause @ (Fault::Store(_) | Fault::Sqlite(_) | Fault::Transaction(_))) => (
                Response::StoreFailure(Box::new(StoreFailure { original, cause })),
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
        invocation_path: crate::collaboration::workflow::TrustedHostPath,
        receipt_only: bool,
    ) -> Result<(Reply, bool)> {
        let limits = &self.config.limits;
        let now = (self.config.clock)();
        // This observational boundary precedes durable receipt processing. A
        // report only reads current registration and replaces one memory slot.
        // No accepted task policy, task state or mutation revision is changed.
        if let Operation::ReportMachineResources { sample } = operation {
            let tx = StoreContext::new(&self.conn, limits);
            let observed = super::metadata::current(&tx)?;
            let data = self.machine_resources.report(&tx, &self.config, sample)?;
            let mut reply = Reply::ok(request_id, data);
            reply.observed_controller_revision = Some(observed);
            return Ok((reply, false));
        }
        super::checked_transaction::immediate(&mut self.conn, |sql| {
        let mut tx = StoreContext::new(sql, &self.config.limits);
        tx.machine_resources = Some(&self.machine_resources);
        tx.invocation_path = invocation_path;
        let observed = super::metadata::current(&tx)?;
        let request = encode(operation)?;
        let identity = operation_identity(operation);
        if receipt_only && identity.is_none() {
            return Err(Fault::TemporaryValidation("store_unavailable",
                "receipt-only reentry requires an original mutation identity"));
        }
        let mut cleanup_task = None;
        let mut regular_room = true;
        // Traceability scope: Typed task mutation IDs are validated independently of transport request_id.
        //= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
        //# A mutating request MUST carry an operation identity.
        if let Some(op) = identity {
            if !valid_id(limits, op) {
                return refuse("invalid_input", "invalid operation identity");
            }
            if let Some(receipt) = super::metadata::read_retained(&tx, op)? {
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
                let commit=receipt.commit.clone();
                let replay=receipt.replay()?;
                let reply = super::metadata::reply(
                    &self.config.limits,
                    request_id,
                    operation,
                    replay,
                    Some(commit),
                    observed,
                )?;
                return Ok(super::checked_transaction::Completion::Rollback((reply, false)));
            }
            if receipt_only {
                return Err(Fault::TemporaryValidation("store_unavailable",
                    "original receipt unavailable; no replacement operation was executed"));
            }
            if self.maintenance_pending && !super::store_retry::maintenance_safe(operation) {
                return Err(Fault::TemporaryValidation("store_unavailable",
                    "maintenance is unresolved; original receipt replay and protective controls remain available"));
            }
            regular_room = regular_receipt_room(&tx, &self.config)?;
            cleanup_task = cleanup_candidate(&tx, operation)?;
            if !regular_room
                && cleanup_task.is_none()
                && !matches!(operation, Operation::AwaitCommands { .. })
                && super::credits::final_candidate(&tx, operation)?.is_none()
                && super::native_request_diagnostics::candidate(&tx, operation)?.is_none()
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
            creation: None, issuer: &self.validation_issuer,
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
        let diagnostic_credit = super::native_request_diagnostics::candidate(&tx, operation)?;
        let action = if matches!(operation, Operation::AwaitCommands { .. }) {
            super::operations::await_request(&tx, operation, &self.config, now, self.maintenance_pending).map(
                |(data, changed, credit)| {
                    control_credit = credit;
                    (data, changed)
                },
            )
        } else {
            super::operations::apply(&tx, operation, &self.config, now, &context)
        };
        // Finish domain activity and actual first Task-terminal transitions
        // before the action savepoint is released or a receipt is persisted.
        let action = action.and_then(|(data, changed)| {
            if matches!(operation, Operation::ObserveWorker { report: Report {
                observation: Observation::NativeRequestDiagnostic { .. }, ..
            } }) {
                // This append has no task/domain transition. In particular it
                // must not resolve its old actor through today's task owner.
                return Ok((data, changed));
            }
            let workflow_changed = super::collaboration_finalizer::finalize_request(&tx, operation, now)?;
            Ok((data, changed || workflow_changed))
        });
        let (mut outcome, mut changed) = match action {
            Ok((data, changed)) => {
                // Mutation receipts keep the snapshot observed at their own
                // commit; future Get/List recompute diagnostics, not receipts.
                let data = match data {
                    Data::Task(task) if identity.is_some() && task.status == TaskStatus::Queued => {
                        let task = super::operations::view(
                            &tx,
                            load(&tx, &task.id)?,
                            &self.config,
                            now,
                        )?;
                        // Refreshing commit-time diagnostics reloads canonical
                        // input after the collaboration adapter projected it.
                        // Preserve the public boundary in the original receipt;
                        // canonical GetTask/RunAttempt authority stays unchanged.
                        let task = if matches!(operation, Operation::CallCollaboration { .. }) {
                            crate::pull_request_view::task(&task)
                        } else {
                            task
                        };
                        Data::Task(Box::new(task))
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
                tx.workflow_terminals.clear();
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
                tx.workflow_terminals.clear();
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
                tx.workflow_terminals.clear();
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
            // A terminal diagnostic records an immutable fact without changing
            // today's task. Its original reserved credit is consumed exactly
            // once even when ordinary receipt capacity is still available.
            let diagnostic_cleanup = diagnostic_credit.is_some()
                && matches!(&outcome, Outcome::Ok { data: Data::Acknowledged });
            let cleanup = task_cleanup || control_cleanup || effect_cleanup || diagnostic_cleanup;
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
                if let Some(super::publication_requests::CommandCredit::Attempt(attempt)) = &control_credit {
                    super::credits::debit_control(&tx, attempt)?;
                }
                // Publication delivery/ACK credits are consumed by their exact
                // existing command flags in this same receipt transaction.
            }
            if effect_cleanup {
                super::credits::debit_effect(&tx, final_credit.as_deref().unwrap())?;
            }
            if diagnostic_cleanup {
                super::native_request_diagnostics::debit(&tx, diagnostic_credit.unwrap(), &outcome)?;
            }
            let revision = super::metadata::advance(&tx)?;
            // Drafts survive only the action savepoint. Final replacement
            // refusals are audited here, after all response and credit guards.
            // Audit, any applicable typed hold and the original receipt share
            // this transaction; a failed insert/commit rolls back all three.
            let governance = super::governance::commit_decisions(&tx, operation, &outcome, revision, now)?;
            changed |= governance.is_some();
            tx.execute(
                //= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
                //# Task submission MUST commit its input and submission outcome atomically.
                "INSERT INTO receipts(id,request,outcome,cleanup,controller_revision,governance) VALUES(?1,?2,?3,?4,?5,?6)",
                params![op, request, encode(&outcome)?, cleanup, integer(revision)?, governance],
            )?;
            // Closure readers validate the exact original report/ACK receipt.
            // Advance pending assignment only after that receipt exists in this
            // same transaction; returned observation remains its original snapshot.
            if successful_change {
                changed |= super::task_handoffs::progress(&tx, operation, &self.config, now)?;
            }
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
            if matches!(record.kind.as_str(), "application_mutation" | "committed_read_reference") {
                let receipt = super::metadata::read_retained(&tx, operation_id)?
                    .ok_or_else(|| Fault::Store("mutation lookup lost original receipt".into()))?;
                if *record != receipt.lookup()? {
                    // Only the caller-scoped immutable read reader may restore
                    // a removed body. Ordinary GetOperation equality is unchanged.
                    super::collaboration_calls::validate_recovered_lookup(operation, &receipt, record)?;
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
        //= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
        //# A terminal task result MUST be committed before it is reported as final.
        Ok(super::checked_transaction::Completion::Commit((reply, changed)))
        })
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
    let diagnostics = super::native_request_diagnostics::outstanding_credits(tx)?;
    let publication_commands = super::publication_requests::command_credits(tx)?;
    let total = unsigned(used)?
        .checked_add(unsigned(reserved)?)
        .and_then(|n| n.checked_add(u64::try_from(control).ok()?))
        .and_then(|n| n.checked_add(u64::try_from(effects).ok()?))
        .and_then(|n| n.checked_add(diagnostics))
        .and_then(|n| n.checked_add(publication_commands))
        .and_then(|n| n.checked_add(u64::from(extra)));
    Ok(total.is_some_and(|n| n <= cfg.max_cleanup_receipts as u64))
}

fn cleanup_candidate(tx: &Connection, op: &Operation) -> Result<Option<String>> {
    if matches!(op, Operation::ObserveWorker { report: Report {
        observation: Observation::NativeRequestDiagnostic { .. }, ..
    } }) {
        // Late original facts do not borrow a successor task's cleanup grant.
        return Ok(None);
    }
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
        Operation::CancelTask { task_id, .. }
        | Operation::MutateGovernance { mutation: crate::governance::GovernanceMutation::Stop { task_id, .. }, .. } => task_id,
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
        Operation::CancelTask { expected_revision, .. }
        | Operation::MutateGovernance { mutation: crate::governance::GovernanceMutation::Stop { expected_revision, .. }, .. }
            => !terminal(t.task.status) && t.task.revision == *expected_revision,
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
                || (a.is_none() && (t.generation != 0 || t.attempt_counter != 0)
                    && !super::sessions::is_successor(tx, &t.task.session_id)?)
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
            let accepted_context = tx.with_limits(accepted_policy);
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
    let first_terminal = if tx.workflow_policy.is_some() && terminal(t.task.status) {
        let previous_status: String = tx.query_row("SELECT status FROM tasks WHERE id=?1", [&t.task.id], |r| r.get(0))?;
        match previous_status.as_str() {
            "queued" | "active" | "blocked" => true,
            "succeeded" | "failed" | "cancelled" => false,
            _ => return Err(Fault::Store("unknown pre-transition task status".into())),
        }
    } else { false };
    t.task.revision = t
        .task
        .revision
        .checked_add(1)
        .ok_or_else(|| Fault::Store("revision exhausted".into()))?;
    let a = t.task.attempt.as_ref();
    tx.execute("UPDATE tasks SET agent=?2,executor=?3,status=?4,worker=?5,incarnation=?6,active=?7,deadline=?8,body=?9,cleanup=?10,session=?11 WHERE id=?1",params![t.task.id,t.task.assigned_agent_id,executor(t.task.input.definition.executor),status(t.task.status),a.map(|x|x.worker_id.as_str()),a.map(|x|x.worker_incarnation.as_str()),active(&t.task),integer(t.task.input.limits.deadline_unix_ms)?,encode(t)?,t.cleanup_remaining,t.task.session_id])?;
    if let Some(a) = a {
        tx.execute("INSERT INTO attempts(id,task_id,body) VALUES(?1,?2,?3) ON CONFLICT(id) DO UPDATE SET body=excluded.body",params![a.id,t.task.id,encode(a)?])?;
    }
    history(tx, &t.task, kind, event)?;
    if first_terminal { tx.workflow_terminals.record(tx, &t.task)?; }
    Ok(())
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
            if !terminal(p.task.status) && !active(&p.task)
                && p.task.status != TaskStatus::Blocked && p.task.native_retry_block.is_none() {
                p.task.status = TaskStatus::Queued;
                p.task.blocked_reason = Some("child_outcome_ready".into());
            }
            save(tx, &mut p, "child_settled", &t.task.id)?;
        }
    }
    Ok(())
}
