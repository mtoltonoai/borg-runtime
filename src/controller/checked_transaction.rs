//! One checked SQLite transaction, with no resend or automatic retry policy.
//! A rollback acknowledgment is a database fact, not proof that external effects
//! are absent. Callers retain original operation identity and own retry policy.
use super::database::{Fault, Result};
use rusqlite::{Connection, ErrorCode};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase { Begin, Body, Commit, Rollback }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SqliteCode {
    pub primary: ErrorCode,
    pub extended: i32,
}
pub(crate) fn sqlite_code(error: &rusqlite::Error) -> Option<SqliteCode> {
    match error {
        rusqlite::Error::SqliteFailure(code, _) => Some(SqliteCode {
            primary: code.code, extended: code.extended_code,
        }),
        _ => None,
    }
}

#[derive(Debug)]
pub(crate) enum Rollback {
    /// BEGIN was refused before this boundary owned a transaction. This is not
    /// evidence about an earlier operation or external effect.
    NotStarted,
    /// Refuse an already-active transaction without attempting to roll it back.
    NotOwned,
    /// ROLLBACK returned success AND SQLite reports no active transaction.
    Acknowledged,
    /// Preserve the rollback error independently of the original failure.
    /// Autocommit is diagnostic state only; it cannot turn this into proof.
    Unproven { error: Option<rusqlite::Error>, autocommit: bool },
}
impl Rollback {
    pub(crate) fn acknowledged(&self) -> bool { matches!(self, Self::Acknowledged) }
}

#[derive(Debug)]
pub(crate) struct Failure {
    pub phase: Phase,
    pub original: Fault,
    pub rollback: Rollback,
}
impl Failure {
    pub(crate) fn original_sqlite_code(&self) -> Option<SqliteCode> {
        match &self.original {
            Fault::Sqlite(error) => sqlite_code(error),
            Fault::Transaction(failure) => failure.original_sqlite_code(),
            _ => None,
        }
    }

    /// This is a database fact, not a scheduling permit. In particular, neither
    /// this classification nor an autocommit bit establishes external absence.
    pub(crate) fn retry_disposition(&self) -> RetryDisposition {
        // Only this boundary's original SQLite failure is classifiable. A nested
        // transaction/control transfer cannot borrow our outer rollback proof.
        let Fault::Sqlite(error) = &self.original else {
            return RetryDisposition::Ineligible;
        };
        let Some(code) = sqlite_code(error).filter(known_transient) else {
            return RetryDisposition::Ineligible;
        };
        match (&self.rollback, self.phase) {
            (Rollback::Acknowledged, Phase::Begin | Phase::Body | Phase::Commit) =>
                RetryDisposition::RolledBackTransient { phase: self.phase, code },
            (Rollback::NotStarted, Phase::Begin) =>
                RetryDisposition::BeginNotStartedTransient { code },
            _ => RetryDisposition::Ineligible,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetryDisposition {
    RolledBackTransient { phase: Phase, code: SqliteCode },
    /// A failed BEGIN did not own a transaction. Kept distinct from an actual
    /// acknowledged ROLLBACK; this increment grants no reentry for that case.
    BeginNotStartedTransient { code: SqliteCode },
    Ineligible,
}

pub(super) fn known_transient(code: &SqliteCode) -> bool {
    use rusqlite::ffi;
    // A closed list preserves extended causes. Unknown extensions, I/O errors,
    // disk full, constraints, corruption and authorization failures fail closed.
    match code.primary {
        ErrorCode::DatabaseBusy => matches!(code.extended,
            ffi::SQLITE_BUSY | ffi::SQLITE_BUSY_RECOVERY | ffi::SQLITE_BUSY_SNAPSHOT
            | ffi::SQLITE_BUSY_TIMEOUT),
        ErrorCode::DatabaseLocked => matches!(code.extended,
            ffi::SQLITE_LOCKED | ffi::SQLITE_LOCKED_SHAREDCACHE),
        _ => false,
    }
}
fn failure(phase: Phase, original: Fault, rollback: Rollback) -> Fault {
    Fault::Transaction(Box::new(Failure {phase, original, rollback}))
}

/// The body must explicitly choose whether its value requires COMMIT or an
/// acknowledged read-only/early-return rollback. Neither case retries the body.
pub(crate) enum Completion<T> { Commit(T), Rollback(T) }

struct Owned<'a> {
    connection: &'a mut Connection,
    finished: bool,
}
impl<'a> Owned<'a> {
    fn begin(connection: &'a mut Connection, exclusive: bool) -> Result<Self> {
        if !connection.is_autocommit() {
            return Err(failure(Phase::Begin,
                Fault::Store("checked transaction cannot adopt an active transaction".into()),
                Rollback::NotOwned));
        }
        if let Err(error) = connection.execute_batch(if exclusive { "BEGIN EXCLUSIVE" } else { "BEGIN IMMEDIATE" }) {
            let rollback = if connection.is_autocommit() { Rollback::NotStarted }
                else { checked_rollback(connection) };
            return Err(failure(Phase::Begin, Fault::Sqlite(error), rollback));
        }
        if connection.is_autocommit() {
            return Err(failure(Phase::Begin,
                Fault::Store("successful BEGIN did not retain an active transaction".into()),
                Rollback::Unproven {error:None, autocommit:true}));
        }
        Ok(Self {connection, finished:false})
    }
    fn finish<T>(mut self, body: Result<Completion<T>>) -> Result<T> {
        let result = match body {
            Ok(Completion::Commit(value)) => match self.connection.execute_batch("COMMIT") {
                Ok(()) if self.connection.is_autocommit() => Ok(value),
                Ok(()) => Err(failure(Phase::Commit,
                    Fault::Store("successful COMMIT left an active transaction".into()),
                    checked_rollback(self.connection))),
                Err(error) => Err(failure(Phase::Commit, Fault::Sqlite(error),
                    checked_rollback(self.connection))),
            },
            Ok(Completion::Rollback(value)) => {
                let rollback = checked_rollback(self.connection);
                if rollback.acknowledged() { Ok(value) }
                else { Err(failure(Phase::Rollback,
                    Fault::Store("early transaction result requires acknowledged rollback".into()), rollback)) }
            },
            Err(original) => {
                let rollback = checked_rollback(self.connection);
                // These are existing non-storage control transfers. In
                // particular, a helper/repository ticket cannot escape while
                // transaction release is unknown. Typed store failures keep the
                // original SQLite code together with this boundary's phase.
                if rollback.acknowledged() && matches!(&original,
                    Fault::Refused(..) | Fault::Validation(_) | Fault::Repository(_)
                    | Fault::TemporaryValidation(..) | Fault::Pending(_)) {
                    Err(original)
                } else {
                    Err(failure(Phase::Body, original, rollback))
                }
            },
        };
        // Even a failed rollback is not silently replaced by a Drop retry.
        // An active unresolved transaction makes the next boundary refuse entry.
        self.finished = true;
        result
    }
}
impl Drop for Owned<'_> {
    fn drop(&mut self) {
        if !self.finished {
            // Panic cleanup only. Its unchecked result is NEVER returned as
            // rollback proof and cannot authorize a retry or external ticket.
            let _ = self.connection.execute_batch("ROLLBACK");
        }
    }
}
fn checked_rollback(connection: &Connection) -> Rollback {
    let result = connection.execute_batch("ROLLBACK");
    let autocommit = connection.is_autocommit();
    match result {
        Ok(()) if autocommit => Rollback::Acknowledged,
        Ok(()) => Rollback::Unproven {error:None, autocommit},
        Err(error) => Rollback::Unproven {error:Some(error), autocommit},
    }
}

pub(crate) fn immediate<T>(connection: &mut Connection,
    body: impl FnOnce(&Connection) -> Result<Completion<T>>) -> Result<T> {
    let owner = Owned::begin(connection, false)?;
    let outcome = body(&*owner.connection);
    owner.finish(outcome)
}

pub(super) fn exclusive<T>(connection: &mut Connection,
    body: impl FnOnce(&Connection) -> Result<Completion<T>>) -> Result<T> {
    let owner = Owned::begin(connection, true)?;
    let outcome = body(&*owner.connection);
    owner.finish(outcome)
}

#[cfg(test)]
#[path = "checked_transaction_tests.rs"]
mod tests;
