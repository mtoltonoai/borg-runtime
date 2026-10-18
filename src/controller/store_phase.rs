//! Startup SQL phases retain one lifetime and an owned cancellation wakeup.
//! Neither filesystem setup nor process reconciliation belongs in a retry body.
use super::{checked_transaction::{self, Completion, RetryDisposition}, database::{Fault, Result}};
use crate::limits::StoreRetryLimits;
use std::{sync::{Arc, Condvar, Mutex}, time::{Duration, Instant}};

#[derive(Clone, Default)]
pub struct StartupCancellation(Arc<(Mutex<StartupState>, Condvar)>);
#[derive(Default)]
struct StartupState { cancelled: bool, owners: usize, #[cfg(test)] waits: usize }
pub(super) struct StartupOwner(StartupCancellation);
impl Drop for StartupOwner {
    fn drop(&mut self) {
        let (state, changed) = &*self.0.0;
        state.lock().expect("startup cancellation poisoned").owners -= 1;
        changed.notify_all();
    }
}
impl StartupCancellation {
    pub fn cancel(&self) {
        let (state, changed) = &*self.0;
        state.lock().expect("startup cancellation poisoned").cancelled = true;
        changed.notify_all();
    }
    pub(super) fn check(&self) -> std::result::Result<(), String> {
        if self.0.0.lock().expect("startup cancellation poisoned").cancelled {
            Err("controller startup cancelled; original state retained".into())
        } else { Ok(()) }
    }
    pub(super) fn wait(&self, duration: Duration) -> std::result::Result<(), String> {
        let (state, changed) = &*self.0;
        let cancelled = state.lock().expect("startup cancellation poisoned");
        #[cfg(test)]
        let cancelled = {
            let mut observed = cancelled;
            observed.waits += 1;
            changed.notify_all();
            observed
        };
        let (cancelled, _) = changed.wait_timeout_while(cancelled, duration, |value| !value.cancelled)
            .expect("startup cancellation poisoned");
        if cancelled.cancelled { Err("controller startup cancelled; original state retained".into()) }
        else { Ok(()) }
    }
    pub(super) fn owner(&self) -> StartupOwner {
        self.0.0.lock().expect("startup cancellation poisoned").owners += 1;
        StartupOwner(self.clone())
    }
    /// Call after the startup result and after dropping any concurrently opened
    /// Controller. This observes actual owner exit; it does not abort a thread.
    pub fn wait_for_owners(&self) {
        let (state, changed) = &*self.0;
        drop(changed.wait_while(state.lock().expect("startup cancellation poisoned"),
            |state| state.owners != 0).expect("startup cancellation poisoned"));
    }
}

pub(super) fn retryable(cause: &Fault) -> bool {
    matches!(cause, Fault::Transaction(failure) if matches!(failure.retry_disposition(),
        RetryDisposition::RolledBackTransient { .. } | RetryDisposition::BeginNotStartedTransient { .. }))
}

/// One fresh-store identity and its first completed filesystem binding. A
/// failed/partial create is terminal; only SQL work can be retried around it.
pub(super) struct FreshSchema { id: String, binding: Option<String> }
impl FreshSchema {
    pub(super) fn new(connection: &rusqlite::Connection) -> Result<Self> {
        Ok(Self { id: connection.query_row("SELECT lower(hex(randomblob(16)))", [], |r| r.get(0))?, binding: None })
    }
    pub(super) fn write(&mut self, tx: &rusqlite::Connection, tracking: &super::tracking::Tracking) -> Result<()> {
        tracking.check_database_identity().map_err(Fault::Store)?;
        const ID: &str = "INSERT INTO settings(name,value) VALUES('database_id',lower(hex(randomblob(16))));";
        let (before, after) = include_str!("schema12.sql").split_once(ID)
            .ok_or_else(|| Fault::Store("fresh schema identity statement changed".into()))?;
        tx.execute_batch(before)?;
        tx.execute("INSERT INTO settings(name,value) VALUES('database_id',?1)", [&self.id])?;
        tx.execute_batch(after)?;
        super::workflow_schema::install_checked(tx)?;
        super::application_schema::install_checked(tx)?;
        if let Some(original) = &self.binding {
            tracking.verify(&self.id, original).map_err(Fault::Store)?;
        } else {
            self.binding = Some(tracking.create(&self.id).map_err(Fault::Store)?);
        }
        tx.execute("INSERT INTO settings(name,value) VALUES('native_helper_binding',?1)",
            [self.binding.as_ref().expect("original completed binding")])?;
        Ok(())
    }
}

/// One already-identified SQL phase. The caller captures all external inputs
/// before entering and keeps the same connection and owner lock throughout.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A transient failure of a platform-owned durable-store transaction MUST be retried with metadata-controlled backoff while retaining the original operation identity.
pub(super) fn transaction<T>(connection: &mut rusqlite::Connection,
    policy: Option<&StoreRetryLimits>, cancel: &StartupCancellation,
    exclusive: bool, mut body: impl FnMut(&rusqlite::Connection) -> Result<Completion<T>>,
) -> std::result::Result<T, String> {
    let expires = policy.and_then(|p| Instant::now().checked_add(Duration::from_millis(p.operation_timeout_ms)));
    let mut retried = false;
    loop {
        cancel.check()?;
        if retried && expires.is_none_or(|end| Instant::now() >= end) {
            return Err("controller startup SQL phase exhausted its original store_retry lifetime".into());
        }
        let result = if exclusive { checked_transaction::exclusive(connection, &mut body) }
            else { checked_transaction::immediate(connection, &mut body) };
        match result {
            Ok(value) => return Ok(value),
            Err(cause) => {
                let Some(policy) = policy.filter(|_| retryable(&cause)) else {
                    return Err(format!("controller startup SQL phase failed: {cause:?}"));
                };
                let remaining = expires.map(|end| end.saturating_duration_since(Instant::now())).unwrap_or_default();
                if remaining.is_zero() { return Err(format!("controller startup SQL phase exhausted: {cause:?}")); }
                cancel.wait(Duration::from_millis(policy.backoff_ms).min(remaining))?;
                retried = true;
            }
        }
    }
}

#[cfg(test)]
#[path = "store_phase_tests.rs"]
mod tests;
