//! Read-only admission of the two new field tags before any schema upgrade.
use super::{ControllerConfig, database::*};
use crate::{governance::{ConfigurationVersion, DecisionRecord}, protocol::{*, identity_limit_fields as vocabulary}};
use rusqlite::params;

fn invalid() -> Fault {
    Fault::Store("identity limit field vocabulary requires schema26; original bytes retained".into())
}

/// Reuse the enclosing inventory/body bounds, reading only a bounded prefix of
/// each retained record. This does not reinterpret user text as a format tag.
fn scan<T: serde::de::DeserializeOwned + serde::Serialize>(
    tx: &StoreContext<'_>, table: &str, column: &str, count_bound: usize,
    byte_bound: usize, uses: impl Fn(&T) -> bool,
) -> Result<()> {
    let mut q = tx.prepare(&format!(
        "SELECT CAST(substr(CAST({column} AS BLOB),1,?1) AS TEXT) FROM {table} WHERE {column} IS NOT NULL LIMIT ?2"
    ))?;
    let mut rows = q.query(params![sql_bound(byte_bound.checked_add(1).ok_or_else(invalid)?)?, sql_bound(count_bound.checked_add(1).ok_or_else(invalid)?)?])?;
    let mut count = 0;
    while let Some(row) = rows.next()? {
        count += 1;
        let raw: String = row.get(0)?;
        if count > count_bound || raw.len() > byte_bound { return Err(invalid()); }
        if uses(&decode::<T>(&raw)?) { return Err(invalid()); }
    }
    Ok(())
}

pub(super) fn validate_existing(tx: &StoreContext<'_>, cfg: &ControllerConfig, version: i64) -> Result<()> {
    if super::application_schema::admits_read_generation(version) { return Ok(()); }
    let receipts = cfg.max_receipts.checked_add(cfg.max_cleanup_receipts).ok_or_else(invalid)?;
    let commands = receipts.checked_add(cfg.max_tasks).and_then(|n| n.checked_mul(2)).ok_or_else(invalid)?;
    let bytes = tx.limits.transport.body_bytes;
    scan::<Operation>(tx, "receipts", "request", receipts, bytes, vocabulary::operation_uses)?;
    scan::<Outcome>(tx, "receipts", "outcome", receipts, bytes, vocabulary::outcome_uses)?;
    scan::<StoredTask>(tx, "tasks", "body", cfg.max_tasks, bytes, |s| vocabulary::input_uses(&s.task.input))?;
    scan::<WorkerCommand>(tx, "commands", "body", commands, bytes, vocabulary::command_uses)?;
    if version >= super::application_schema::DEFAULT_ASSIGNMENT_VERSION {
        scan::<TaskInput>(tx, "session_inputs", "body", super::sessions::bound(cfg)?, bytes, vocabulary::input_uses)?;
        super::task_handoffs::admit_prior_identity_limit_fields(tx, receipts)?;
    }
    if version >= 14 {
        let bound = cfg.limits.governance.as_ref();
        scan::<Vec<DecisionRecord>>(tx, "receipts", "governance", receipts,
            bound.map_or(bytes, |b| b.record_bytes), |records| records.iter().any(vocabulary::decision_uses))?;
        scan::<ConfigurationVersion>(tx, "governance_configurations", "body",
            bound.map_or(receipts, |b| b.configuration_versions), bound.map_or(bytes, |b| b.record_bytes), vocabulary::configuration_uses)?;
    }
    Ok(())
}
