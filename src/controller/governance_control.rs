//! Independent trusted-operator controls in the original controller transaction.
//! A stop records intent; a hold release or policy expiry never dispatches work.
use super::{database::{self, decode, refuse, valid_id, Fault, Result, StoreContext}, governance, ControllerConfig};
use crate::collaboration::workflow::{ActorProvenance, PrincipalRef, RecordedActor, TrustedHostPath};
use crate::governance::{ControlDecision, ControlScope, EmergencyHold, GovernanceMutation, GovernanceResult, GovernanceState, TemporaryOverride};
use rusqlite::{params, OptionalExtension};

fn identity(tx: &StoreContext<'_>, value: &str) -> Result<()> {
    if !valid_id(tx.governance_limits, value) {
        return refuse("invalid_input", "invalid control identity: transport.identity_bytes");
    }
    Ok(())
}
fn reason(tx: &StoreContext<'_>, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return refuse("invalid_input", "control reason must be nonempty");
    }
    if value.len() > governance::limits(tx)?.reason_bytes {
        return refuse("capacity", "Configured limit exceeded: governance.reason_bytes");
    }
    Ok(())
}
fn transaction(tx: &StoreContext<'_>) -> Result<()> {
    if tx.is_autocommit() {
        return Err(Fault::Store("governance control requires the original owner transaction".into()));
    }
    Ok(())
}
fn operator_actor() -> RecordedActor {
    RecordedActor { principal: PrincipalRef::TrustedHostOperator,
        provenance: ActorProvenance::TrustedHost { path: TrustedHostPath::OperatorApi } }
}
fn scope_key(tx: &StoreContext<'_>, scope: &ControlScope) -> Result<String> {
    match scope {
        ControlScope::AllTasks => Ok("all".into()),
        ControlScope::Task { task_id } => {
            identity(tx, task_id)?;
            Ok(format!("task:{task_id}"))
        }
    }
}
fn read_hold(tx: &StoreContext<'_>, scope: &ControlScope) -> Result<Option<EmergencyHold>> {
    let key = scope_key(tx, scope)?;
    let maximum = governance::limits(tx)?.record_bytes;
    let raw: Option<String> = tx.query_row(
        "SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM governance_holds WHERE scope=?1",
        params![key, database::sql_bound(maximum.saturating_add(1))?], |row| row.get(0),
    ).optional()?;
    let Some(raw) = raw else { return Ok(None) };
    if raw.len() > maximum {
        return Err(Fault::Store("retained emergency hold exceeds governance.record_bytes".into()));
    }
    let hold: EmergencyHold = decode(&raw)?;
    if hold.scope != *scope || hold.actor != operator_actor()
        || !valid_id(tx.governance_limits, &hold.operation_id)
        || hold.reason.trim().is_empty() || hold.reason.len() > governance::limits(tx)?.reason_bytes {
        return Err(Fault::Store("retained emergency hold has an invalid scope, actor, identity or reason".into()));
    }
    Ok(Some(hold))
}
fn checked_state(tx: &StoreContext<'_>) -> Result<GovernanceState> {
    let active = governance::state(tx)?;
    if !valid_id(tx.governance_limits, &active.selected_revision) {
        return Err(Fault::Store("invalid retained governance selection identity".into()));
    }
    if let Some(value) = &active.temporary_override {
        if !valid_id(tx.governance_limits, &value.operation_id)
            || !valid_id(tx.governance_limits, &value.prior_revision)
            || !valid_id(tx.governance_limits, &value.replacement_revision)
            || value.actor != operator_actor() || value.reason.trim().is_empty()
            || value.reason.len() > governance::limits(tx)?.reason_bytes
            || value.expires_at_ms == 0 || value.expires_at_ms > i64::MAX as u64
            || value.prior_revision == value.replacement_revision
            || active.selected_revision != value.replacement_revision {
            return Err(Fault::Store("retained governance override does not match its exact selection".into()));
        }
    }
    Ok(active)
}
fn clock(now: u64) -> Result<()> {
    if now == 0 || now > i64::MAX as u64 {
        return refuse("governance_clock_unavailable", "current override expiry cannot be established");
    }
    Ok(())
}
//= spec/impl/v2/implementation.md#emergency-control-is-independent-of-ordinary-policy
//# An expired temporary governance override MUST restore its prior policy unless a normal governed change has explicitly superseded that override.
fn restore(
    tx: &StoreContext<'_>, operation_id: &str, actor: RecordedActor, active: GovernanceState, now: u64,
) -> Result<bool> {
    clock(now)?;
    let Some(value) = active.temporary_override else { return Ok(false) };
    if now < value.expires_at_ms { return Ok(false) }
    // Read the exact old configuration, without evaluating its ordinary rules.
    // A broken old rule may hold subsequent admission; expiry cannot silently
    // substitute bootstrap policy or keep extending the temporary replacement.
    governance::read_configuration(tx, &value.prior_revision)?;
    transaction(tx)?;
    governance::save_state(tx, &GovernanceState {
        selected_revision: value.prior_revision.clone(), temporary_override: None,
    })?;
    governance::stage_control(tx, operation_id, actor, ControlDecision::OverrideExpired {
        original_operation_id: value.operation_id, restored_revision: value.prior_revision,
    })?;
    Ok(true)
}

/// Invoked only by the existing operator wrapper for these closed actions.
/// Original operation replay and rollback are owned by the surrounding receipt
/// transaction. This rechecks the actual transport path and supplied actor.
//= spec/impl/v2/implementation.md#emergency-control-is-independent-of-ordinary-policy
//# An authorized operator stop or pause MUST remain available when ordinary governance is unavailable or refuses progress.
//= spec/impl/v2/implementation.md#emergency-control-is-independent-of-ordinary-policy
//# An ordinary configuration change MUST NOT lift a hold imposed by an emergency control.
//= spec/impl/v2/implementation.md#emergency-control-is-independent-of-ordinary-policy
//# A temporary governance override MUST require the configured trusted operator invocation path.
//= spec/impl/v2/implementation.md#emergency-control-is-independent-of-ordinary-policy
//# A temporary governance override MUST record its exact scope, prior policy, replacement policy, reason and expiry in a durable audit event.
//= spec/impl/v2/implementation.md#emergency-control-is-independent-of-ordinary-policy
//# A temporary governance override MUST NOT bypass protected-release authorization, mandatory validation, ownership fencing or uncertain-effect reconciliation.
pub(super) fn mutate(
    tx: &StoreContext<'_>, operation_id: &str, mutation: &GovernanceMutation,
    now: u64, cfg: &ControllerConfig, actor: RecordedActor,
) -> Result<GovernanceResult> {
    let expected_actor = governance::operator(tx)?;
    if actor != expected_actor || actor != operator_actor() {
        return refuse("governance_operator_required", "control actor differs from the actual operator API invocation");
    }
    identity(tx, operation_id)?;
    governance::limits(tx)?;
    transaction(tx)?;
    match mutation {
        GovernanceMutation::Pause { scope, reason: why } => {
            reason(tx, why)?;
            let key = scope_key(tx, scope)?;
            if let ControlScope::Task { task_id } = scope { database::load(tx, task_id)?; }
            if read_hold(tx, scope)?.is_some() {
                return refuse("governance_hold_exists", "this scope retains its original emergency hold");
            }
            let maximum = governance::limits(tx)?.scan_rows;
            let count: i64 = tx.query_row(
                "SELECT count(*) FROM (SELECT 1 FROM governance_holds LIMIT ?1)",
                [database::sql_bound(maximum.saturating_add(1))?], |row| row.get(0),
            )?;
            if count < 0 { return Err(Fault::Store("invalid emergency hold inventory".into())) }
            if usize::try_from(count).ok().is_none_or(|n| n >= maximum) {
                return refuse("capacity", "Configured limit exceeded: governance.scan_rows");
            }
            let hold = EmergencyHold { scope: scope.clone(), operation_id: operation_id.into(),
                actor: actor.clone(), reason: why.clone() };
            tx.execute("INSERT INTO governance_holds(scope,body) VALUES(?1,?2)",
                params![key, governance::bounded(tx, &hold)?])?;
            governance::stage_control(tx, operation_id, actor, ControlDecision::Paused { scope: scope.clone() })?;
            Ok(GovernanceResult::Hold(hold))
        }
        GovernanceMutation::ReleaseHold { scope, expected_operation_id, reason: why } => {
            identity(tx, expected_operation_id)?;
            reason(tx, why)?;
            let key = scope_key(tx, scope)?;
            let hold = read_hold(tx, scope)?.ok_or_else(|| Fault::Refused("not_found", "emergency hold absent".into()))?;
            if hold.operation_id != *expected_operation_id {
                return refuse("configuration_conflict", "emergency hold operation changed");
            }
            if tx.execute("DELETE FROM governance_holds WHERE scope=?1 AND body=?2",
                params![key, governance::bounded(tx, &hold)?])? != 1 {
                return Err(Fault::Store("emergency hold changed in the owner transaction".into()));
            }
            governance::stage_control(tx, operation_id, actor, ControlDecision::HoldReleased { scope: scope.clone() })?;
            Ok(GovernanceResult::Released(scope.clone()))
        }

        // Traceability scope: Authorized emergency Stop uses its direct control path independently of voluntary completion admission.
        //= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
        //# An authorized cancellation or emergency stop MUST NOT wait for voluntary completion admission.

        GovernanceMutation::Stop { task_id, expected_revision, reason: why } => {
            identity(tx, task_id)?;
            reason(tx, why)?;
            super::operations::governance_stop(tx, task_id, *expected_revision, cfg)?;
            governance::stage_control(tx, operation_id, actor, ControlDecision::StopRequested { task_id: task_id.clone() })?;
            Ok(GovernanceResult::StopRequested { task_id: task_id.clone() })
        }
        GovernanceMutation::Override { expected_revision, configuration_revision, expires_at_ms, reason: why } => {
            clock(now)?;
            identity(tx, expected_revision)?;
            identity(tx, configuration_revision)?;
            reason(tx, why)?;
            let active = checked_state(tx)?;
            if active.selected_revision != *expected_revision {
                return refuse("configuration_conflict", "active governance revision changed");
            }
            if active.temporary_override.is_some() {
                return refuse("override_active", "an existing temporary override must first be reconciled or explicitly superseded");
            }
            if active.selected_revision == *configuration_revision {
                return refuse("invalid_input", "temporary override must select a distinct immutable configuration");
            }
            let maximum = governance::limits(tx)?.override_max_ms;
            if *expires_at_ms <= now || *expires_at_ms > i64::MAX as u64
                || expires_at_ms.checked_sub(now).is_none_or(|duration| duration > maximum) {
                return refuse("invalid_input", "temporary expiry exceeds the positive governance.override_max_ms window");
            }
            // Only require the prior version to exist. Repair must remain
            // possible when its ordinary rule evaluator is broken.
            governance::read_configuration(tx, &active.selected_revision)?;
            let replacement = governance::read_configuration(tx, configuration_revision)?;
            governance::validate_configuration(tx, &replacement)?;
            governance::require_qualified(tx, &replacement.revision)?;
            let next = GovernanceState { selected_revision: replacement.revision.clone(),
                temporary_override: Some(TemporaryOverride {
                    operation_id: operation_id.into(), prior_revision: active.selected_revision.clone(),
                    replacement_revision: replacement.revision.clone(), expires_at_ms: *expires_at_ms,
                    actor: actor.clone(), reason: why.clone(),
                }) };
            governance::save_state(tx, &next)?;
            governance::stage_control(tx, operation_id, actor, ControlDecision::OverrideSelected {
                scope: ControlScope::AllTasks, prior_revision: active.selected_revision,
                replacement_revision: replacement.revision, expires_at_ms: *expires_at_ms, reason: why.clone(),
            })?;
            Ok(GovernanceResult::State(next))
        }
        GovernanceMutation::ReconcileExpiry { expected_operation_id } => {
            identity(tx, expected_operation_id)?;
            clock(now)?;
            let active = checked_state(tx)?;
            let value = active.temporary_override.as_ref()
                .ok_or_else(|| Fault::Refused("not_found", "temporary override absent".into()))?;
            if value.operation_id != *expected_operation_id {
                return refuse("configuration_conflict", "temporary override operation changed");
            }
            if now < value.expires_at_ms {
                return refuse("override_not_expired", "temporary override has not expired");
            }
            restore(tx, operation_id, actor, active, now)?;
            Ok(GovernanceResult::State(governance::state(tx)?))
        }
        _ => refuse("invalid_input", "not an independent governance control action"),
    }
}

/// Read-only admission check. Expired policy is held until its restoration is
/// committed by an original mutation; reads never create an audit or resume work.
//= spec/impl/v2/implementation.md#emergency-control-is-independent-of-ordinary-policy
//# Expiry or removal of a temporary governance override MUST NOT resume held work without the normal recovery checks.
pub(super) fn admission_reason(tx: &StoreContext<'_>, task_id: &str, now: u64) -> Result<Option<String>> {
    identity(tx, task_id)?;
    if tx.governance_limits.governance.is_none() {
        let retained: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM governance_holds) OR EXISTS(SELECT 1 FROM governance_state)",
            [], |row| row.get(0),
        )?;
        return if retained { Ok(Some("governance_metadata_unavailable".into())) } else { Ok(None) };
    }
    if now == 0 || now > i64::MAX as u64 { return Ok(Some("governance_clock_unavailable".into())) }
    if read_hold(tx, &ControlScope::AllTasks)?.is_some() {
        return Ok(Some("governance_emergency_pause_all_tasks".into()));
    }
    if read_hold(tx, &ControlScope::Task { task_id: task_id.into() })?.is_some() {
        return Ok(Some("governance_emergency_pause_task".into()));
    }
    let active = checked_state(tx)?;
    if active.temporary_override.as_ref().is_some_and(|value| now >= value.expires_at_ms) {
        return Ok(Some("governance_override_expiry_pending".into()));
    }
    let selected = governance::selection(tx)?;
    if selected.revision != governance::bootstrap(tx).revision {
        governance::require_qualified(tx, &selected.revision)?;
    }
    Ok(None)
}

/// Root calls this only for its closed admission mutations, after original
/// receipt lookup and before dispatch. Do not call it on explicit control routes:
/// each original operation has one EmergencyControl decision slot.
/// The receipt finalizer separately validates this exact maintenance exception.
//= spec/impl/v2/implementation.md#emergency-control-is-independent-of-ordinary-policy
//# Recovery MUST reconcile temporary-override expiry before admitting work under the override.
pub(super) fn reconcile_expiry(tx: &StoreContext<'_>, operation_id: &str, now: u64) -> Result<bool> {
    identity(tx, operation_id)?;
    if tx.governance_limits.governance.is_none() {
        let retained: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM governance_holds) OR EXISTS(SELECT 1 FROM governance_state)",
            [], |row| row.get(0),
        )?;
        if retained { return refuse("governance_unavailable", "retained emergency state requires governance metadata") }
        return Ok(false);
    }
    clock(now)?;
    let active = checked_state(tx)?;
    restore(tx, operation_id, RecordedActor {
        principal: PrincipalRef::TrustedHostOperator,
        provenance: ActorProvenance::TrustedHost { path: TrustedHostPath::ControllerMaintenance },
    }, active, now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governance::{ActualDecisionOutcome, ConfigurationBinding, DecisionDetail, DecisionMode, DecisionPoint,
        DecisionRecord, DecisionRef, RuleAdministrationDecision, RuleRef, ShadowEvidenceRef};
    use crate::limits::{GovernanceLimits, RuntimeLimits};
    use crate::protocol::{Data, Operation, Outcome};
    use rusqlite::Connection;
    use std::sync::Arc;

    fn policy() -> RuntimeLimits {
        let mut value = RuntimeLimits::seed().unwrap();
        value.governance = Some(GovernanceLimits {
            record_bytes: 16_384, reason_items: 16, reason_bytes: 1_024,
            scan_rows: 32, rule_versions: 16, configuration_versions: 16,
            shadow_evidence_items: 16, completion_continuations: 1, override_max_ms: 100,
        });
        value
    }
    fn sql() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        // Reduced real SQLite fixture for control reads/writes, not a migration
        // or Controller qualification claim. No task/command tables or stop
        // mock: these global hold/expiry cases do not dispatch ordinary work.
        conn.execute_batch("CREATE TABLE governance_holds(scope TEXT PRIMARY KEY,body TEXT NOT NULL);
            CREATE TABLE governance_state(singleton INTEGER PRIMARY KEY CHECK(singleton=1),body TEXT NOT NULL);
            CREATE TABLE governance_configurations(revision TEXT PRIMARY KEY,body TEXT NOT NULL,operation_id TEXT NOT NULL,qualification TEXT);
            CREATE TABLE governance_rules(id TEXT,revision TEXT,body TEXT,operation_id TEXT NOT NULL,PRIMARY KEY(id,revision));
            CREATE TABLE receipts(id TEXT PRIMARY KEY,request TEXT NOT NULL,outcome TEXT NOT NULL,cleanup INTEGER NOT NULL,controller_revision INTEGER NOT NULL,governance TEXT);
            CREATE TABLE counters(kind TEXT PRIMARY KEY,value INTEGER NOT NULL);
            INSERT INTO counters(kind,value) VALUES('controller_revision',2);").unwrap();
        conn
    }
    fn rows(conn: &Connection) -> Vec<(String, String)> {
        let mut q = conn.prepare("SELECT scope,body FROM governance_holds ORDER BY scope").unwrap();
        q.query_map([], |row| Ok((row.get(0)?, row.get(1)?))).unwrap()
            .collect::<std::result::Result<Vec<_>, _>>().unwrap()
    }
    fn action(conn: &Connection, p: &RuntimeLimits, id: &str, action: GovernanceMutation, now: u64) -> Result<GovernanceResult> {
        let sql = conn.unchecked_transaction().unwrap();
        let tx = StoreContext::new(&sql, p);
        let cfg = ControllerConfig { limits: Arc::new(p.clone()), ..ControllerConfig::default() };
        let result = mutate(&tx, id, &action, now, &cfg, operator_actor());
        drop(tx);
        match result {
            Ok(value) => { sql.commit().unwrap(); Ok(value) }
            Err(error) => { sql.rollback().unwrap(); Err(error) }
        }
    }
    fn replacement(conn: &Connection, p: &RuntimeLimits, name: &str) {
        let tx = StoreContext::new(conn, p);
        let mut candidate = governance::bootstrap(&tx);
        candidate.revision = name.into();
        // Exact typed immutable registration for the production reader. This is
        // fixture construction, not a controller registration execution.
        let id = format!("registration-{name}");
        let request = Operation::MutateGovernance { operation_id: id.clone(),
            mutation: GovernanceMutation::RegisterConfiguration { configuration: candidate.clone() } };
        let outcome = Outcome::Ok { data: Data::Governance(Box::new(GovernanceResult::Configuration(candidate.clone()))) };
        conn.execute("INSERT INTO receipts(id,request,outcome,cleanup,controller_revision) VALUES(?1,?2,?3,0,1)",
            params![id, database::encode(&request).unwrap(), database::encode(&outcome).unwrap()]).unwrap();
        conn.execute("INSERT INTO governance_configurations(revision,body,operation_id) VALUES(?1,?2,?3)",
            params![candidate.revision, governance::bounded(&tx, &candidate).unwrap(), id]).unwrap();
    }
    fn promotion_fixture(conn: &Connection, p: &RuntimeLimits, revision: &str) -> DecisionRecord {
        // Typed original receipt fixture for the actual reader join, not proof
        // that a controller ran shadow evaluation or promoted these policies.
        let id = format!("promotion-{revision}");
        let evidence = vec![
            ShadowEvidenceRef { operation_id: "fixture-shadow-completion".into() },
            ShadowEvidenceRef { operation_id: "fixture-shadow-definition".into() },
        ];
        let record = DecisionRecord {
            configuration_metadata: Some(Box::new(p.clone())),
            mode: DecisionMode::Enforcing,
            id: DecisionRef { operation_id: id.clone(), point: DecisionPoint::RuleAdministration },
            rule: RuleRef { id: "governance-administration".into(), revision: "1".into() },
            configuration: ConfigurationBinding {
                global_revision: p.revision.clone(), governance_revision: p.revision.clone(),
                accepted_global_revision: None,
                definition_update_policy_revision: p.definition_updates.as_ref().map(|policy| policy.revision.clone()),
            },
            actor: operator_actor(),
            detail: DecisionDetail::RuleAdministration(RuleAdministrationDecision::Promoted {
                prior_revision: p.revision.clone(), selected_revision: revision.into(), evidence: evidence.clone(),
            }),
            actual_outcome: ActualDecisionOutcome::Applied, controller_revision: 2,
            observed_product_revision: None,
        };
        let tx = StoreContext::new(conn, p);
        let request = Operation::MutateGovernance { operation_id: id.clone(), mutation: GovernanceMutation::Promote {
            expected_revision: p.revision.clone(), configuration_revision: revision.into(), evidence,
            supersedes_override: None,
        } };
        let outcome = Outcome::Ok { data: Data::Governance(Box::new(GovernanceResult::State(GovernanceState {
            selected_revision: revision.into(), temporary_override: None,
        }))) };
        conn.execute("INSERT INTO receipts(id,request,outcome,cleanup,controller_revision,governance) VALUES(?1,?2,?3,0,2,?4)",
            params![id, database::encode(&request).unwrap(), database::encode(&outcome).unwrap(),
                governance::bounded(&tx, &vec![record.clone()]).unwrap()]).unwrap();
        assert_eq!(conn.execute("UPDATE governance_configurations SET qualification=?2 WHERE revision=?1",
            params![revision, record.id.operation_id]).unwrap(), 1);
        record
    }
    fn select_override(conn: &Connection, p: &RuntimeLimits) {
        replacement(conn, p, "replacement");
        promotion_fixture(conn, p, "replacement");
        action(conn, p, "override-original", GovernanceMutation::Override {
            expected_revision: p.revision.clone(), configuration_revision: "replacement".into(),
            expires_at_ms: 150, reason: "bounded operator recovery".into(),
        }, 100).unwrap();
    }
    fn assert_refused<T: std::fmt::Debug>(result: Result<T>, code: &str) {
        assert!(matches!(&result, Err(Fault::Refused(actual, _)) if *actual == code), "{result:?}");
    }

    #[test]
    fn pause_keeps_original_identity_and_release_requires_exact_hold() {
        let p = policy(); let conn = sql();
        let pause = GovernanceMutation::Pause { scope: ControlScope::AllTasks, reason: "retain original reason".into() };
        let result = action(&conn, &p, "pause-original", pause.clone(), 0).unwrap();
        let GovernanceResult::Hold(hold) = result else { panic!("hold expected") };
        assert_eq!(hold.operation_id, "pause-original"); assert_eq!(hold.reason, "retain original reason");
        let retained = rows(&conn);
        assert_refused(action(&conn, &p, "pause-again", pause, 100), "governance_hold_exists");
        assert_eq!(rows(&conn), retained);
        let tx = StoreContext::new(&conn, &p);
        assert_eq!(admission_reason(&tx, "task", 100).unwrap().as_deref(), Some("governance_emergency_pause_all_tasks"));
        assert_refused(action(&conn, &p, "stale-release", GovernanceMutation::ReleaseHold {
            scope: ControlScope::AllTasks, expected_operation_id: "another-pause".into(), reason: "stale".into(),
        }, 100), "configuration_conflict");
        assert_eq!(rows(&conn), retained);
        action(&conn, &p, "release", GovernanceMutation::ReleaseHold {
            scope: ControlScope::AllTasks, expected_operation_id: "pause-original".into(), reason: "release intent only".into(),
        }, 0).unwrap();
        assert!(rows(&conn).is_empty());
    }

    #[test]
    //= spec/impl/v2/implementation.md#emergency-control-is-independent-of-ordinary-policy
    //= type=test
    //# An authorized operator stop or pause MUST remain available when ordinary governance is unavailable or refuses progress.
    fn ordinary_rule_corruption_does_not_disable_pause_or_release() {
        let p = policy(); let conn = sql();
        let tx = StoreContext::new(&conn, &p);
        governance::save_state(&tx, &GovernanceState { selected_revision: "missing-ordinary-configuration".into(), temporary_override: None }).unwrap();
        assert!(governance::selection(&tx).is_err());
        action(&conn, &p, "pause", GovernanceMutation::Pause { scope: ControlScope::AllTasks, reason: "independent control".into() }, 0).unwrap();
        action(&conn, &p, "release", GovernanceMutation::ReleaseHold { scope: ControlScope::AllTasks,
            expected_operation_id: "pause".into(), reason: "ordinary admission still validates policy".into() }, 0).unwrap();
        assert!(governance::selection(&tx).is_err());
        assert!(admission_reason(&tx, "task", 100).is_err());
    }

    #[test]
    //= spec/impl/v2/implementation.md#configuration-changes-preserve-governance
    //= type=test
    //# A governance-rule or change-policy revision MUST be applied through an operator-controlled path outside agent-writable work.
    //= spec/impl/v2/implementation.md#emergency-control-is-independent-of-ordinary-policy
    //= type=test
    //# An emergency-control record MUST identify the actual authenticated invocation boundary rather than a caller-supplied identity.
    //= spec/impl/v2/implementation.md#emergency-control-is-independent-of-ordinary-policy
    //= type=test
    //# A temporary governance override MUST require the configured trusted operator invocation path.
    fn transport_and_recorded_actor_must_both_be_exact_operator_api() {
        let p = policy(); let conn = sql(); let cfg = ControllerConfig::default();
        let sql = conn.unchecked_transaction().unwrap();
        let mut tx = StoreContext::new(&sql, &p);
        let pause = GovernanceMutation::Pause { scope: ControlScope::AllTasks, reason: "test".into() };
        tx.invocation_path = TrustedHostPath::ControllerMaintenance;
        assert_refused(mutate(&tx, "wrong-path", &pause, 100, &cfg, operator_actor()), "governance_operator_required");
        tx.invocation_path = TrustedHostPath::OperatorApi;
        let mut forged = operator_actor();
        forged.provenance = ActorProvenance::TrustedHost { path: TrustedHostPath::ControllerMaintenance };
        assert_refused(mutate(&tx, "wrong-actor", &pause, 100, &cfg, forged), "governance_operator_required");
        assert!(rows(&sql).is_empty());
    }

    #[test]
    //= spec/impl/v2/implementation.md#emergency-control-is-independent-of-ordinary-policy
    //= type=test
    //# An ordinary configuration change MUST NOT lift a hold imposed by an emergency control.
    //= spec/impl/v2/implementation.md#emergency-control-is-independent-of-ordinary-policy
    //= type=test
    //# An expired temporary governance override MUST restore its prior policy unless a normal governed change has explicitly superseded that override.
    fn expiry_restores_exact_prior_and_leaves_hold_bytes_untouched() {
        let p = policy(); let conn = sql();
        action(&conn, &p, "hold", GovernanceMutation::Pause { scope: ControlScope::AllTasks, reason: "stay held".into() }, 100).unwrap();
        let held = rows(&conn); select_override(&conn, &p);
        let tx = StoreContext::new(&conn, &p);
        let active = checked_state(&tx).unwrap();
        assert_eq!(active.selected_revision, "replacement");
        assert_eq!(active.temporary_override.as_ref().unwrap().operation_id, "override-original");
        assert_refused(action(&conn, &p, "early", GovernanceMutation::ReconcileExpiry { expected_operation_id: "override-original".into() }, 149), "override_not_expired");
        assert_refused(action(&conn, &p, "wrong-original", GovernanceMutation::ReconcileExpiry { expected_operation_id: "different".into() }, 150), "configuration_conflict");
        let next = action(&conn, &p, "expiry", GovernanceMutation::ReconcileExpiry { expected_operation_id: "override-original".into() }, 150).unwrap();
        assert_eq!(next, GovernanceResult::State(GovernanceState { selected_revision: p.revision.clone(), temporary_override: None }));
        assert_eq!(rows(&conn), held);
        assert_eq!(admission_reason(&tx, "task", 151).unwrap().as_deref(), Some("governance_emergency_pause_all_tasks"));
    }

    #[test]
    //= spec/impl/v2/implementation.md#emergency-control-is-independent-of-ordinary-policy
    //= type=test
    //# Recovery MUST reconcile temporary-override expiry before admitting work under the override.
    fn readonly_admission_holds_unknown_time_and_expired_policy_until_original_mutation() {
        let p = policy(); let conn = sql(); select_override(&conn, &p);
        let tx = StoreContext::new(&conn, &p); let before = governance::state(&tx).unwrap();
        assert_eq!(admission_reason(&tx, "task", 0).unwrap().as_deref(), Some("governance_clock_unavailable"));
        assert_eq!(admission_reason(&tx, "task", 149).unwrap(), None);
        assert_eq!(admission_reason(&tx, "task", 150).unwrap().as_deref(), Some("governance_override_expiry_pending"));
        assert_eq!(governance::state(&tx).unwrap(), before);
        let sql = conn.unchecked_transaction().unwrap();
        let tx = StoreContext::new(&sql, &p);
        assert!(reconcile_expiry(&tx, "admission-original", 150).unwrap());
        let restored = governance::state(&tx).unwrap();
        assert_eq!(restored.selected_revision, p.revision); assert!(restored.temporary_override.is_none());
        drop(tx); sql.commit().unwrap();
        let tx = StoreContext::new(&conn, &p);
        assert!(!reconcile_expiry(&tx, "next-admission", 151).unwrap());
        assert_eq!(admission_reason(&tx, "task", 151).unwrap(), None);
    }

    #[test]
    fn invalid_nested_or_stale_overrides_preserve_current_selection() {
        let p = policy(); let conn = sql(); replacement(&conn, &p, "replacement");
        promotion_fixture(&conn, &p, "replacement");
        for (expected, now, expiry, code) in [
            (p.revision.as_str(), 0, 150, "governance_clock_unavailable"),
            ("wrong", 100, 150, "configuration_conflict"),
            (p.revision.as_str(), 100, 100, "invalid_input"),
            (p.revision.as_str(), 100, 201, "invalid_input"),
            (p.revision.as_str(), 100, u64::MAX, "invalid_input"),
        ] {
            assert_refused(action(&conn, &p, "rejected", GovernanceMutation::Override {
                expected_revision: expected.into(), configuration_revision: "replacement".into(),
                expires_at_ms: expiry, reason: "bounded".into(),
            }, now), code);
            assert!(governance::state(&StoreContext::new(&conn, &p)).unwrap().temporary_override.is_none());
        }
        action(&conn, &p, "override-original", GovernanceMutation::Override {
            expected_revision: p.revision.clone(), configuration_revision: "replacement".into(), expires_at_ms: 150, reason: "bounded".into(),
        }, 100).unwrap();
        let before = governance::state(&StoreContext::new(&conn, &p)).unwrap();
        assert_refused(action(&conn, &p, "nested", GovernanceMutation::Override {
            expected_revision: "replacement".into(), configuration_revision: p.revision.clone(), expires_at_ms: 200, reason: "nested".into(),
        }, 150), "override_active");
        assert_eq!(governance::state(&StoreContext::new(&conn, &p)).unwrap(), before);
    }

    #[test]
    fn missing_policy_retained_state_and_control_bounds_fail_closed() {
        let mut p = policy(); let conn = sql();
        let mut historical=p.clone();historical.governance=None;
        let historical_tx=StoreContext::new(&conn,&historical);
        assert_eq!(admission_reason(&historical_tx,"task",0).unwrap(),None);
        assert!(!reconcile_expiry(&historical_tx,"historical-admission",0).unwrap());
        p.governance.as_mut().unwrap().reason_bytes = 4;
        assert_refused(action(&conn, &p, "reason-too-large", GovernanceMutation::Pause {
            scope: ControlScope::AllTasks, reason: "longer".into(),
        }, 100), "capacity");
        assert!(rows(&conn).is_empty());
        action(&conn, &p, "hold", GovernanceMutation::Pause { scope: ControlScope::AllTasks, reason: "hold".into() }, 100).unwrap();
        let before = rows(&conn); p.governance = None;
        let tx = StoreContext::new(&conn, &p);
        assert_eq!(admission_reason(&tx, "task", 100).unwrap().as_deref(), Some("governance_metadata_unavailable"));
        assert_refused(reconcile_expiry(&tx, "admission", 100), "governance_unavailable");
        assert_eq!(rows(&conn), before);
    }

    #[test]
    fn task_scope_key_cannot_alias_global_hold_and_corrupt_hold_is_not_removed() {
        let p = policy(); let conn = sql();
        let tx = StoreContext::new(&conn, &p);
        // A deliberately inserted typed retained row tests only the reader's
        // scope binding. Task-scoped Pause ingress requires a real load above.
        let hold = EmergencyHold { scope: ControlScope::Task { task_id: "all".into() },
            operation_id: "task-hold".into(), actor: operator_actor(), reason: "bounded".into() };
        conn.execute("INSERT INTO governance_holds(scope,body) VALUES(?1,?2)",
            params![scope_key(&tx, &hold.scope).unwrap(), governance::bounded(&tx, &hold).unwrap()]).unwrap();
        assert_eq!(admission_reason(&tx, "all", 100).unwrap().as_deref(), Some("governance_emergency_pause_task"));
        assert_eq!(admission_reason(&tx, "other-task", 100).unwrap(), None);
        conn.execute("UPDATE governance_holds SET scope='all'", []).unwrap();
        let corrupted = rows(&conn);
        assert!(admission_reason(&tx, "other-task", 100).is_err());
        assert!(action(&conn, &p, "bad-release", GovernanceMutation::ReleaseHold {
            scope: ControlScope::AllTasks, expected_operation_id: "task-hold".into(), reason: "do not repair".into(),
        }, 100).is_err());
        assert_eq!(rows(&conn), corrupted);
    }

    #[test]
    fn audit_staging_failure_rolls_back_the_hold_in_the_owner_transaction() {
        let mut p = policy(); let conn = sql();
        let hold = EmergencyHold { scope: ControlScope::AllTasks, operation_id: "bounded-pause".into(),
            actor: operator_actor(), reason: "bounded".into() };
        // The hold itself fits exactly; its enclosing decision audit does not.
        // Failure must roll back the earlier INSERT through the owner wrapper.
        p.governance.as_mut().unwrap().record_bytes = database::encode(&hold).unwrap().len();
        assert_refused(action(&conn, &p, "bounded-pause", GovernanceMutation::Pause {
            scope: ControlScope::AllTasks, reason: "bounded".into(),
        }, 100), "capacity");
        assert!(rows(&conn).is_empty());
    }

    #[test]
    fn temporary_replacement_requires_its_exact_applied_promotion_receipt() {
        let p = policy(); let conn = sql(); replacement(&conn, &p, "replacement");
        let request = GovernanceMutation::Override { expected_revision: p.revision.clone(),
            configuration_revision: "replacement".into(), expires_at_ms: 150, reason: "bounded".into() };
        assert_refused(action(&conn, &p, "unqualified", request.clone(), 100), "governance_evidence");
        let original = promotion_fixture(&conn, &p, "replacement");
        for outcome in [ActualDecisionOutcome::GovernanceBlocked,
            ActualDecisionOutcome::MandatoryRefusal { code: "fixture-refusal".into() }] {
            let mut wrong = original.clone();
            wrong.actual_outcome = outcome;
            conn.execute("UPDATE receipts SET governance=?2 WHERE id=?1",
                params![original.id.operation_id, database::encode(&vec![wrong]).unwrap()]).unwrap();
            // The original final outcome remains Ok: an altered audit must be
            // rejected as storage corruption before it can qualify anything.
            assert!(matches!(action(&conn, &p, "refused-proof", request.clone(), 100), Err(Fault::Store(_))));
        }
        let mut wrong = original.clone();
        let DecisionDetail::RuleAdministration(RuleAdministrationDecision::Promoted { selected_revision, .. }) = &mut wrong.detail else { unreachable!() };
        *selected_revision = "other-configuration".into();
        conn.execute("UPDATE receipts SET governance=?2 WHERE id=?1",
            params![original.id.operation_id, database::encode(&vec![wrong]).unwrap()]).unwrap();
        assert!(matches!(action(&conn, &p, "wrong-subject", request.clone(), 100), Err(Fault::Store(_))));
        assert!(governance::state(&StoreContext::new(&conn, &p)).unwrap().temporary_override.is_none());
        conn.execute("UPDATE receipts SET governance=?2 WHERE id=?1",
            params![original.id.operation_id, database::encode(&vec![original.clone()]).unwrap()]).unwrap();
        action(&conn, &p, "exact-qualified", request, 100).unwrap();
    }

    #[test]
    fn bound_agent_cannot_pause_even_with_operator_label_and_operator_control_still_commits() {
        use crate::controller::{database::{Database, Response}, ExternalProof, OriginalRequest};
        use crate::mcp::Caller;
        use crate::protocol::*;
        use std::{path::PathBuf, time::{SystemTime, UNIX_EPOCH}};

        const NOW: u64 = 1_800_000_000_000;
        // This is a caller-chosen agent label, never an authenticated principal.
        const OPERATOR_LABEL: &str = "trusted_host_operator";
        struct Area(PathBuf);
        impl Drop for Area {
            fn drop(&mut self) {
                if std::env::var_os("BORG_TEST_ARTIFACT").is_none() {
                    let _ = std::fs::remove_dir_all(&self.0);
                }
            }
        }
        fn call(db: &mut Database, id: &str, command: Operation) -> Reply {
            let request = Request { protocol_revision: WIRE_REVISION, request_id: id.into(), command: command.clone() };
            let (response, _) = db.handle(OriginalRequest {
                request: request.clone(), invocation_path: TrustedHostPath::OperatorApi, validation: None,
            });
            let response = match response {
                Response::Validation(ticket) => {
                    // Genuine bounded schema validation, followed by exact
                    // original-request finalization. No synthetic proof/token.
                    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
                    let proof = runtime.block_on(Box::pin((*ticket).execute(&db.validation_host, &db.config.repository_read)));
                    db.handle(OriginalRequest { request, invocation_path: TrustedHostPath::OperatorApi,
                        validation: Some(ExternalProof::Native(proof)) }).0
                }
                other => other,
            };
            let Response::Reply(reply) = response else { panic!("unexpected repeated validation or store failure") };
            reply.validate_for(&command).unwrap();
            reply
        }
        fn task(reply: Reply) -> Task {
            match reply.outcome {
                Outcome::Ok { data: Data::Task(task) } => *task,
                other => panic!("real accepted task expected: {other:?}"),
            }
        }
        fn snapshot(conn: &Connection) -> (Vec<(String, String)>, Option<String>, Vec<(String, Option<String>)>) {
            let state = conn.query_row("SELECT body FROM governance_state WHERE singleton=1", [], |row| row.get(0))
                .optional().unwrap();
            let mut query = conn.prepare("SELECT id,governance FROM receipts ORDER BY id").unwrap();
            let audits = query.query_map([], |row| Ok((row.get(0)?, row.get(1)?))).unwrap()
                .collect::<std::result::Result<Vec<_>, _>>().unwrap();
            (rows(conn), state, audits)
        }

        let stamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let base = std::env::var_os("BORG_TEST_ARTIFACT").map(PathBuf::from).unwrap_or_else(std::env::temp_dir);
        let area = Area(base.join(format!("borg-emergency-agent-origin-{}-{stamp}", std::process::id())));
        std::fs::create_dir(&area.0).unwrap();
        let mut p = policy();
        // Existing real-task governance fixture envelope, not a runtime default.
        p.governance.as_mut().unwrap().record_bytes = 65_536;
        p.execution.native.context_bytes = 65_536;
        p.validate().unwrap();
        let cfg = ControllerConfig {
            limits: Arc::new(p.clone()), clock: Arc::new(|| NOW),
            validation_executable: PathBuf::from(std::env::var_os("BORG_TEST_RUNTIME")
                .expect("explicit qualified library-test runtime binding")),
            ..ControllerConfig::default()
        };
        let mut db = Database::open(&area.0.join("tasks.sqlite"), cfg.clone()).unwrap();
        let input = TaskInput {
            instruction_source: None, pull_request: None, resolved_limits: None,
            instruction: "Return a bounded integer".into(),
            definition: Definition { id: OPERATOR_LABEL.into(), revision: "1".into(), executor: ExecutorKind::Native,
                inference: Some(InferenceSettings::selected()), execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
                charter: None, limit_overrides: None },
            repository: None, repository_read: None, acceptance_commands: vec![],
            output_schema: Some(serde_json::json!({"type":"integer"})), limits: p.task_defaults.at(NOW).unwrap(),
            dependencies: vec![], native_workflow: None,
        };
        let queued = task(call(&mut db, "agent-submit", Operation::SubmitTask { operation_id: "agent-submit".into(), input }));
        assert_eq!(queued.assigned_agent_id, OPERATOR_LABEL);
        let worker = Worker { id: "agent-worker".into(), incarnation: "agent-incarnation".into(), native_capacity: 1, coding_capacity: 0 };
        let registered = call(&mut db, "agent-register", Operation::RegisterWorker { operation_id: "agent-register".into(), worker: worker.clone() });
        assert!(matches!(registered.outcome, Outcome::Ok { .. }), "{registered:?}");
        let commands = call(&mut db, "agent-await", Operation::AwaitCommands { operation_id: "agent-await".into(),
            worker_id: worker.id.clone(), incarnation: worker.incarnation.clone(), acknowledged: vec![] });
        let Outcome::Ok { data: Data::Commands(commands) } = commands.outcome else { panic!("actual admission expected: {commands:?}") };
        let attempt = commands.into_iter().find_map(|command| match command {
            WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == queued.id => Some(attempt), _ => None,
        }).expect("actual attempt for this agent's accepted task");
        let caller = Caller { task_id: attempt.task_id.clone(), attempt_id: attempt.id.clone(), generation: attempt.generation,
            worker_id: attempt.worker_id.clone(), incarnation: attempt.worker_incarnation.clone() };
        let started = task(call(&mut db, "agent-started", Operation::ObserveWorker { report: Report {
            report_id: "agent-started".into(), task_id: caller.task_id.clone(), attempt_id: caller.attempt_id.clone(),
            generation: caller.generation, worker_id: caller.worker_id.clone(), incarnation: caller.incarnation.clone(),
            observation: Observation::Started { thread_id: None, turn_id: None },
        } }));
        assert_eq!(started.attempt.as_ref().unwrap().phase, AttemptPhase::Running);
        let pause = GovernanceMutation::Pause { scope: ControlScope::AllTasks, reason: "operator requested pause".into() };
        let before = snapshot(&db.conn);
        assert!(before.0.is_empty());
        {
            let sql = db.conn.unchecked_transaction().unwrap();
            let tx = StoreContext::new(&sql, &p);
            let current = crate::controller::collaboration_calls::caller(&tx, &caller, NOW).unwrap().task;
            assert_eq!(current.session_id, started.session_id);
            // Match the existing agent producer after its current-owner check;
            // validate all retained agent/session/task/attempt bindings as well.
            let agent = RecordedActor {
                principal: PrincipalRef::Agent { agent_id: current.assigned_agent_id.clone() },
                provenance: ActorProvenance::BoundAgentSession {
                    agent_id: current.assigned_agent_id.clone(), session_id: current.session_id.clone(),
                    task_id: caller.task_id.clone(), attempt_id: caller.attempt_id.clone(), generation: caller.generation,
                    worker_id: caller.worker_id.clone(), incarnation: caller.incarnation.clone(),
                },
            };
            crate::controller::collaboration_workflow::validate_recorded_actor(&tx, &agent).unwrap();
            assert_eq!(agent.principal, PrincipalRef::Agent { agent_id: OPERATOR_LABEL.into() });
            // The strongest ordinary invocation path still cannot turn the
            // actual bound agent into the trusted operator. Then attempt the
            // claimed operator principal without replacing the agent provenance.
            let mut claimed_operator = agent.clone();
            claimed_operator.principal = PrincipalRef::TrustedHostOperator;
            for (id, actor) in [("agent-pause", agent), ("claimed-operator-pause", claimed_operator)] {
                let refusal = match mutate(&tx, id, &pause, NOW, &cfg, actor) {
                    Err(Fault::Refused(code, message)) => {
                        assert_eq!(code, "governance_operator_required");
                        assert_eq!(message, "control actor differs from the actual operator API invocation");
                        Outcome::Refused { code: code.into(), message }
                    }
                    other => panic!("agent must not acquire emergency authority: {other:?}"),
                };
                let op = Operation::MutateGovernance { operation_id: id.into(), mutation: pause.clone() };
                assert_eq!(governance::commit_decisions(&tx, &op, &refusal, 1, NOW).unwrap(), None,
                    "denial must not stage a successful emergency audit");
                assert_eq!(snapshot(&sql), before, "denial must not change hold, selection or original audit rows");
            }
            drop(tx);
            sql.rollback().unwrap();
        }
        assert_eq!(snapshot(&db.conn), before);
        // Real operator positive control crosses the normal Database dispatcher
        // and receipt/audit commit path with the identical emergency mutation.
        let allowed = call(&mut db, "actual-operator-pause", Operation::MutateGovernance {
            operation_id: "actual-operator-pause".into(), mutation: pause,
        });
        assert_eq!(allowed.commit.as_ref().unwrap().operation_id, "actual-operator-pause");
        let Outcome::Ok { data: Data::Governance(value) } = allowed.outcome else { panic!("operator pause expected: {allowed:?}") };
        let GovernanceResult::Hold(hold) = *value else { panic!("operator hold expected") };
        assert_eq!(hold.actor, operator_actor());
        assert_eq!(hold.operation_id, "actual-operator-pause");
        assert_eq!(snapshot(&db.conn).1, before.1, "emergency pause does not select an ordinary policy");
        let audit = call(&mut db, "actual-operator-audit", Operation::QueryGovernance {
            query: crate::governance::GovernanceQuery::Decision { decision: DecisionRef {
                operation_id: "actual-operator-pause".into(), point: DecisionPoint::EmergencyControl,
            } },
        });
        let Outcome::Ok { data: Data::Governance(value) } = audit.outcome else { panic!("canonical operator audit expected: {audit:?}") };
        let GovernanceResult::Decision(record) = *value else { panic!("operator decision expected") };
        assert_eq!(record.actor, operator_actor());
        assert_eq!(record.actual_outcome, ActualDecisionOutcome::Applied);
        assert_eq!(record.detail, DecisionDetail::EmergencyControl(ControlDecision::Paused { scope: ControlScope::AllTasks }));
    }
}
