//! One terminal observation credit for each newly charged native request.
//! The original Usage receipt supplies caller and accepted-policy authority.
//! Appending a diagnostic never changes today's task, usage, circuit or retry
//! decision, and never grants another provider request.
use super::{ControllerConfig, database::*, metadata};
use super::database::StoreContext as Connection;
use crate::{
    limits::RuntimeLimits,
    mcp::Caller,
    native::{CategoryDiagnostic, DiagnosticBodyState, HttpStatusDiagnostic,
        ProviderErrorCategory, ProviderFailureDiagnostic, ProviderRequestBinding,
        RejectionKind, SchedulerRetryDiagnostic},
    native_loop_contract::{self as contract, NativeRequestObservedOutcome as Observed},
    protocol::*,
};
use rusqlite::{OptionalExtension, params};

/// An opaque candidate cannot be manufactured by the receipt-accounting caller.
pub(super) struct DiagnosticCreditId { credit: Credit, report: Report }

#[derive(Debug, Clone, PartialEq, Eq)]
struct Credit {
    attempt_id: String,
    request_index: u32,
    reservation_id: String,
    diagnostic_id: String,
    consumed: bool,
}
struct Original { task: Task, revision: u64 }

fn mismatch<T>(message: &str) -> Result<T> { refuse("source_mismatch", message) }
fn caller(r: &Report) -> Caller {
    Caller { task_id: r.task_id.clone(), attempt_id: r.attempt_id.clone(), generation: r.generation,
        worker_id: r.worker_id.clone(), incarnation: r.incarnation.clone() }
}
fn same_owner(task: &Task, c: &Caller) -> bool {
    task.id == c.task_id && task.input.definition.executor == ExecutorKind::Native
        && task.attempt.as_ref().is_some_and(|a| a.id == c.attempt_id && a.task_id == c.task_id
            && a.generation == c.generation && a.worker_id == c.worker_id && a.worker_incarnation == c.incarnation)
}
fn accepted(task: &Task) -> Result<&RuntimeLimits> {
    let snapshot = task.input.resolved_limits.as_ref()
        .ok_or_else(|| Fault::Store("native diagnostic accepted policy absent".into()))?;
    snapshot.validate_for(&task.input).map_err(Fault::Store)?;
    Ok(&snapshot.global_metadata)
}
fn inventory_bound(tx: &Connection<'_>) -> Result<usize> {
    tx.limits.controller.max_receipts.checked_add(tx.limits.controller.max_cleanup_receipts)
        .ok_or_else(|| Fault::Store("native diagnostic credit inventory bound overflow".into()))
}
fn table_available(tx: &Connection<'_>) -> Result<bool> {
    let version: i64 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    let present: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='native_request_diagnostic_credits')", [], |r| r.get(0))?;
    // This vocabulary starts at the explicitly coordinated schema18 barrier.
    // The database owner separately validates each complete schema's shape.
    match (version, present) {
        (12..=17, false) => Ok(false),
        (18 | 19 | 20 | 21 | 22 | 23 | 24, true) => Ok(true),
        _ => Err(Fault::Store("native diagnostic credit table differs from original schema".into())),
    }
}
fn legacy_vocabulary(tx: &Connection<'_>) -> Result<()> {
    let bound = inventory_bound(tx)?;
    let limit = bound.checked_add(1).ok_or_else(|| Fault::Store("native diagnostic inventory overflow".into()))?;
    let bytes = tx.limits.transport.identity_bytes.checked_add(1).ok_or_else(|| Fault::Store("native diagnostic identity bound overflow".into()))?;
    let mut query = tx.prepare("SELECT CAST(substr(CAST(id AS BLOB),1,?1) AS TEXT) FROM receipts ORDER BY id LIMIT ?2")?;
    let mut rows = query.query(params![sql_bound(bytes)?, sql_bound(limit)?])?;
    let mut count = 0usize;
    while let Some(row) = rows.next()? {
        count += 1;
        if count > bound { return Err(Fault::Store("native diagnostic legacy inventory exceeds selected receipt bound".into())); }
        let id: String = row.get(0)?;
        let record = metadata::read(tx, &id)?.ok_or_else(|| Fault::Store("native diagnostic legacy receipt disappeared".into()))?;
        if matches!(record.request, Operation::ObserveWorker { report: Report { observation:
            Observation::NativeRequestDiagnostic { .. } | Observation::Usage { native_diagnostic_reserved: true, .. }, .. } }) {
            return Err(Fault::Store("historical schema contains native diagnostic vocabulary".into()));
        }
    }
    Ok(())
}
fn credit(attempt: &str, index: u32) -> Credit {
    Credit { attempt_id: attempt.into(), request_index: index,
        reservation_id: contract::reservation_id(attempt, index),
        diagnostic_id: contract::diagnostic_id(attempt, index), consumed: false }
}
fn read_credit(tx: &Connection<'_>, attempt: &str, index: u32) -> Result<Option<Credit>> {
    if !table_available(tx)? { return Ok(None); }
    let cap = tx.limits.transport.identity_bytes.checked_add(1)
        .ok_or_else(|| Fault::Store("native diagnostic identity bound overflow".into()))?;
    let row: Option<(String, String, i64)> = tx.query_row(
        "SELECT CAST(substr(CAST(reservation_id AS BLOB),1,?3) AS TEXT),CAST(substr(CAST(diagnostic_id AS BLOB),1,?3) AS TEXT),consumed FROM native_request_diagnostic_credits WHERE attempt_id=?1 AND request_index=?2",
        params![attempt, i64::from(index), sql_bound(cap)?],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).optional()?;
    row.map(|(reservation_id, diagnostic_id, consumed)| {
        if index == 0 || !valid_id(tx.limits, attempt) || !valid_id(tx.limits, &reservation_id)
            || !valid_id(tx.limits, &diagnostic_id) || !matches!(consumed, 0 | 1)
            || reservation_id != contract::reservation_id(attempt, index)
            || diagnostic_id != contract::diagnostic_id(attempt, index) {
            return Err(Fault::Store("native diagnostic credit identity or state differs".into()));
        }
        Ok(Credit { attempt_id: attempt.into(), request_index: index,
            reservation_id, diagnostic_id, consumed: consumed == 1 })
    }).transpose()
}

/// Uses the original acknowledged Usage snapshot, never the current task owner.
fn original(tx: &Connection<'_>, c: &Caller, index: u32) -> Result<Original> {
    let id = contract::reservation_id(&c.attempt_id, index);
    let record = metadata::read(tx, &id)?.ok_or_else(||
        Fault::Refused("source_mismatch", "native diagnostic original reservation absent".into()))?;
    let (Operation::ObserveWorker { report }, Outcome::Ok { data: Data::Task(task) }) =
        (&record.request, &record.outcome) else {
        return mismatch("native diagnostic reservation is not an acknowledged original Usage");
    };
    if index == 0 || report.report_id != id || caller(report) != *c || !same_owner(task, c)
        || !matches!(&report.observation, Observation::Usage { model_requests, native_diagnostic_reserved: true, .. } if *model_requests == index)
        || task.status != TaskStatus::Active
        || !task.attempt.as_ref().is_some_and(|a| !a.stopped && !a.uncertain && a.phase != AttemptPhase::StopRequested) {
        return mismatch("native diagnostic original reservation caller, marker or charge differs");
    }
    let runtime = accepted(task)?;
    c.validate(&runtime.transport).map_err(Fault::Store)?;
    let bound = runtime.transport.body_bytes.checked_add(1)
        .ok_or_else(|| Fault::Store("native diagnostic attempt bound overflow".into()))?;
    let raw: Option<String> = tx.query_row(
        "SELECT CAST(substr(CAST(body AS BLOB),1,?3) AS TEXT) FROM attempts WHERE id=?1 AND task_id=?2",
        params![c.attempt_id, c.task_id, sql_bound(bound)?], |r| r.get(0)).optional()?;
    let raw = raw.ok_or_else(|| Fault::Store("native diagnostic retained attempt absent".into()))?;
    if raw.len() > runtime.transport.body_bytes { return Err(Fault::Store("native diagnostic retained attempt exceeds accepted bound".into())); }
    let retained: Attempt = decode(&raw)?;
    let first = task.attempt.as_ref().unwrap();
    if retained.id != first.id || retained.task_id != first.task_id || retained.session_id != first.session_id
        || retained.generation != first.generation || retained.worker_id != first.worker_id
        || retained.worker_incarnation != first.worker_incarnation || retained.deadline_unix_ms != first.deadline_unix_ms {
        return mismatch("native diagnostic retained original attempt differs");
    }
    // Stop/uncertainty/current-owner changes are intentionally not an append gate.
    Ok(Original { task: (**task).clone(), revision: record.commit.controller_revision })
}

fn route(task: &Task, endpoint: &str, model: &str) -> Result<()> {
    let p = &task.input.execution_limits().map_err(Fault::Store)?.native;
    if endpoint.len() > p.endpoint_bytes || task.input.definition.required_inference()
        .map_err(|e| Fault::Refused("source_mismatch", e.into()))?.model != model {
        return mismatch("native diagnostic route or accepted model differs");
    }
    let url = reqwest::Url::parse(endpoint).map_err(|_| Fault::Refused("source_mismatch", "native diagnostic endpoint invalid".into()))?;
    let loopback = url.host_str().and_then(|h| h.trim_matches(['[', ']']).parse::<std::net::IpAddr>().ok()).is_some_and(|ip| ip.is_loopback());
    if url.as_str() != endpoint || url.host_str().is_none() || !url.username().is_empty()
        || url.password().is_some() || url.query().is_some() || url.fragment().is_some()
        || (url.scheme() != "https" && !(url.scheme() == "http" && loopback)) {
        return mismatch("native diagnostic endpoint is not the normalized trusted route");
    }
    Ok(())
}

fn known_facts(kind: RejectionKind, failure: &ProviderFailureDiagnostic) -> Result<()> {
    let Some(http) = &failure.http else { return mismatch("known native rejection lacks HTTP diagnostic facts") };
    if !matches!(http.body, DiagnosticBodyState::Complete | DiagnosticBodyState::Redacted) {
        return mismatch("known native rejection requires a complete original body");
    }
    let category = match kind {
        RejectionKind::RateLimited => ProviderErrorCategory::RateLimited,
        RejectionKind::InvalidRequest => ProviderErrorCategory::InvalidRequest,
        RejectionKind::ContextLimit => ProviderErrorCategory::ContextLimit,
        RejectionKind::PolicyRefused => ProviderErrorCategory::PolicyRefused,
        RejectionKind::QuotaExhausted => ProviderErrorCategory::QuotaExhausted,
        RejectionKind::AuthenticationFailed => ProviderErrorCategory::AuthenticationFailed,
    };
    if !matches!(http.category, CategoryDiagnostic::Redacted)
        && http.category != CategoryDiagnostic::Value(category) {
        return mismatch("native rejection diagnostic category differs");
    }
    if let HttpStatusDiagnostic::Value(status) = http.status {
        let compatible = match kind {
            RejectionKind::RateLimited => status == 429,
            RejectionKind::InvalidRequest => matches!(status, 400 | 404 | 422),
            RejectionKind::ContextLimit => matches!(status, 400 | 413 | 422),
            RejectionKind::PolicyRefused => matches!(status, 400 | 403 | 422),
            RejectionKind::QuotaExhausted => matches!(status, 402 | 403 | 429),
            RejectionKind::AuthenticationFailed => matches!(status, 401 | 403),
        };
        if !compatible { return mismatch("native rejection diagnostic HTTP status differs"); }
    }
    Ok(())
}

fn not_started_facts(failure: Option<&ProviderFailureDiagnostic>) -> Result<()> {
    let Some(failure) = failure else { return mismatch("unstarted native request lacks failure diagnostics") };
    if failure.http.is_some() { return mismatch("unstarted native request cannot have observed HTTP facts"); }
    Ok(())
}

fn shape(task: &Task, report: &Report) -> Result<u32> {
    let Observation::NativeRequestDiagnostic { request_index, endpoint, model, request_binding, observed_outcome, failure } = &report.observation else {
        return mismatch("native diagnostic observation kind differs");
    };
    if *request_index == 0 || report.report_id != contract::diagnostic_id(&report.attempt_id, *request_index) {
        return mismatch("native diagnostic original operation identity differs");
    }
    route(task, endpoint, model)?;
    let p = &task.input.execution_limits().map_err(Fault::Store)?.native;
    if let Some(binding) = request_binding { binding.validate(p.request_bytes).map_err(|e| Fault::Refused("source_mismatch", e))?; }
    else if !matches!(observed_outcome, Observed::NotStarted) {
        return mismatch("exposed native diagnostic lacks original request binding");
    }
    if let Some(facts) = failure { facts.validate(p).map_err(|e| Fault::Refused("source_mismatch", e))?; }
    match observed_outcome {
        Observed::Unknown if failure.is_none() => return mismatch("unknown native request lacks failure diagnostics"),
        Observed::KnownRejected { rejection_kind } => known_facts(*rejection_kind, failure.as_ref()
            .ok_or_else(|| Fault::Refused("source_mismatch", "known native rejection lacks failure diagnostics".into()))?)?,
        Observed::NotStarted => not_started_facts(failure.as_ref())?,
        Observed::Completed | Observed::OutputCapped => {
            if failure.as_ref().and_then(|f| f.http.as_ref()).is_some_and(|h|
                matches!(h.status, HttpStatusDiagnostic::Value(status) if !(200..=299).contains(&status))) {
                return mismatch("completed native diagnostic has a non-success HTTP status");
            }
        }
        _ => {},
    }
    Ok(*request_index)
}

fn complete_bound(limits: &RuntimeLimits, report: &Report) -> Result<()> {
    let op = Operation::ObserveWorker { report: report.clone() };
    let frame = Request { protocol_revision: WIRE_REVISION, request_id: String::new(), command: op.clone() };
    let size = bounded_json_size(&frame, limits.transport.body_bytes)
        .map_err(|_| Fault::Refused("capacity", "native diagnostic request exceeds transport.body_bytes".into()))?;
    if limits.transport.identity_bytes.checked_mul(2).and_then(|n| size.checked_add(n))
        .is_none_or(|n| n > limits.transport.body_bytes) {
        return refuse("capacity", "native diagnostic request exceeds transport.body_bytes");
    }
    metadata::receipt_bound(limits, &op, &Outcome::Ok { data: Data::Acknowledged })
}

/// Conservatively bound the concrete serializer before charging. Variable
/// source bytes are counted, never allocated or taken from display summaries.
fn admission_bound(tx: &Connection<'_>, task: &Task, reservation: &Report, index: u32) -> Result<()> {
    let native = &task.input.execution_limits().map_err(Fault::Store)?.native;
    let facts = ProviderFailureDiagnostic::maximum_encoded_bytes(native).map_err(Fault::Store)?;
    let extra = native.endpoint_bytes.checked_mul(6)
        .and_then(|n| n.checked_add(facts.checked_sub(4)?))
        .ok_or_else(|| Fault::Refused("capacity", "native diagnostic envelope bound overflow".into()))?;
    let mut report = reservation.clone();
    report.report_id = contract::diagnostic_id(&report.attempt_id, index);
    report.observation = Observation::NativeRequestDiagnostic {
        request_index: index, endpoint: String::new(),
        model: task.input.definition.required_inference().map_err(|e| Fault::Refused("source_mismatch", e.into()))?.model.clone(),
        request_binding: Some(ProviderRequestBinding { bytes: u64::MAX, sha256: "0".repeat(64) }),
        observed_outcome: Observed::KnownRejected { rejection_kind: RejectionKind::AuthenticationFailed }, failure: None,
    };
    for policy in [accepted(task)?, tx.limits] {
        if !valid_id(policy, &report.report_id) { return refuse("capacity", "native diagnostic identity exceeds transport.identity_bytes"); }
        let mut reduced = policy.clone();
        reduced.transport.body_bytes = reduced.transport.body_bytes.checked_sub(extra)
            .ok_or_else(|| Fault::Refused("capacity", "native diagnostic envelope exceeds transport.body_bytes".into()))?;
        reduced.controller.outcome_bytes = reduced.controller.outcome_bytes.checked_sub(extra)
            .ok_or_else(|| Fault::Refused("capacity", "native diagnostic lookup exceeds controller.outcome_bytes".into()))?;
        complete_bound(&reduced, &report).map_err(|_| Fault::Refused("capacity", "native diagnostic complete envelope cannot fit selected transport/receipt bounds".into()))?;
    }
    Ok(())
}

fn core_report(tx: &Connection<'_>, report: &Report) -> Result<Original> {
    let Observation::NativeRequestDiagnostic { request_index, .. } = &report.observation else {
        return mismatch("native diagnostic observation kind differs");
    };
    let source = original(tx, &caller(report), *request_index)?;
    shape(&source.task, report)?;
    complete_bound(accepted(&source.task)?, report)?;
    complete_bound(tx.limits, report)?;
    Ok(source)
}
fn same_rejection(diagnostic: &Report, rejection: &Report) -> Result<()> {
    let (Observation::NativeRequestDiagnostic { request_index: di, endpoint: de, model: dm,
        request_binding: db, observed_outcome, failure },
        Observation::NativeRequestRejected { request_index: ri, endpoint: re, model: rm,
            request_binding: rb, rejection: proof, .. }) = (&diagnostic.observation, &rejection.observation) else {
        return mismatch("native diagnostic/rejection observation kind differs");
    };
    if caller(diagnostic) != caller(rejection) || di != ri || de != re || dm != rm || db.as_ref() != Some(rb)
        || !matches!(observed_outcome, Observed::KnownRejected { rejection_kind } if *rejection_kind == proof.kind) {
        return mismatch("native diagnostic contradicts original canonical rejection");
    }
    let facts = failure.as_ref().ok_or_else(|| Fault::Refused("source_mismatch", "native rejection diagnostics absent".into()))?;
    known_facts(proof.kind, facts)?;
    if let SchedulerRetryDiagnostic::Value(retry) = facts.http.as_ref().unwrap().scheduler_retry_after {
        if retry != proof.retry_after { return mismatch("native diagnostic scheduler retry fact differs from original rejection"); }
    }
    Ok(())
}

fn read_terminal(tx: &Connection<'_>, c: &Credit) -> Result<Option<(Report, u64)>> {
    let Some(record) = metadata::read(tx, &c.diagnostic_id)? else { return Ok(None) };
    // A refused malformed/foreign append did not consume this obligation. Its
    // immutable refusal still replays normally; it is never a successful ACK.
    if matches!(record.outcome, Outcome::Refused { .. }) { return Ok(None); }
    let (Operation::ObserveWorker { report }, Outcome::Ok { data: Data::Acknowledged }) = (record.request, record.outcome) else {
        return mismatch("native diagnostic terminal receipt is not the exact acknowledged observation");
    };
    let source = core_report(tx, &report)?;
    if report.attempt_id != c.attempt_id || report.report_id != c.diagnostic_id
        || !matches!(&report.observation, Observation::NativeRequestDiagnostic { request_index, .. } if *request_index == c.request_index)
        || record.commit.controller_revision <= source.revision {
        return mismatch("native diagnostic terminal precedes or differs from original charge");
    }
    let cleanup: i64 = tx.query_row("SELECT cleanup FROM receipts WHERE id=?1", [&c.diagnostic_id], |r| r.get(0))?;
    if cleanup != 1 { return Err(Fault::Store("native diagnostic terminal did not consume its reserved receipt credit".into())); }
    Ok(Some((report, record.commit.controller_revision)))
}

/// Call both before accepting a rejection and from its retained proof reader.
/// It does not call that reader, avoiding a circular historical validator.
pub(crate) fn validate_rejection(tx: &Connection<'_>, rejection: &Report) -> Result<()> {
    let Observation::NativeRequestRejected { request_index, .. } = &rejection.observation else {
        return mismatch("native rejection join kind differs");
    };
    let Some(c) = read_credit(tx, &rejection.attempt_id, *request_index)? else {
        // Historical unmarked rejection receipts do not acquire a credit.
        let id = contract::reservation_id(&rejection.attempt_id, *request_index);
        if metadata::read(tx, &id)?.is_some_and(|r| matches!(r.request,
            Operation::ObserveWorker { report: Report { observation: Observation::Usage { native_diagnostic_reserved: true, .. }, .. } })) {
            return Err(Fault::Store("marked native charge lost its diagnostic credit".into()));
        }
        return Ok(());
    };
    match (c.consumed, read_terminal(tx, &c)?) {
        (false, None) => Ok(()),
        (true, Some((diagnostic, _))) => same_rejection(&diagnostic, rejection),
        _ => Err(Fault::Store("native diagnostic credit and terminal receipt disagree".into())),
    }
}
fn check_rejection(tx: &Connection<'_>, diagnostic: &Report) -> Result<()> {
    let Observation::NativeRequestDiagnostic { request_index, .. } = &diagnostic.observation else { unreachable!() };
    let id = contract::rejection_id(&diagnostic.attempt_id, *request_index);
    if let Some(record) = metadata::read(tx, &id)? {
        // A canonical refusal records no accepted rejection to cross-check.
        // It must not invalidate an earlier successful terminal diagnostic.
        // Successful receipts still require the complete original proof below.
        if matches!(record.outcome, Outcome::Refused { .. }) { return Ok(()); }
        let Operation::ObserveWorker { report } = record.request else { return mismatch("native rejection receipt kind differs") };
        // The existing canonical reader verifies original charge, accepted
        // caller/route, committed success and ordering. Its reverse join above
        // only uses core_report, so this does not recurse.
        if super::operations::native_retry::confirmed_rejection(tx, &caller(diagnostic), *request_index)?.is_none() {
            return mismatch("native diagnostic canonical rejection absent");
        }
        same_rejection(diagnostic, &report)?;
    }
    Ok(())
}

//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A failed platform-owned provider request MUST retain bounded diagnostics sufficient to identify the observed failure class and available provider retry or quota information.
pub(super) fn observe(tx: &Connection<'_>, report: &Report) -> Result<(Data, bool)> {
    let op = Operation::ObserveWorker { report: report.clone() };
    candidate(tx, &op)?.ok_or_else(|| Fault::Refused("source_mismatch", "native diagnostic candidate absent".into()))?;
    Ok((Data::Acknowledged, false))
}

/// Leaf for the caller's already-bounded final/evidence indices. Historical
/// unmarked charges keep their prior semantics; new marked charges require the
/// exact consumed terminal receipt. This does not scan or grant current scope.
pub(super) fn require_terminal(tx: &Connection<'_>, owner: &Caller, index: u32) -> Result<Option<(Observed, u64)>> {
    let reservation = metadata::read(tx, &contract::reservation_id(&owner.attempt_id, index))?
        .ok_or_else(|| Fault::Refused("source_mismatch", "native completion original reservation absent".into()))?;
    let Operation::ObserveWorker { report } = &reservation.request else {
        return mismatch("native completion reservation kind differs");
    };
    let Observation::Usage { native_diagnostic_reserved, .. } = &report.observation else {
        return mismatch("native completion reservation is not original Usage");
    };
    if !*native_diagnostic_reserved {
        super::operations::native_retry::reservation(tx, owner, index)?;
        return Ok(None);
    }
    original(tx, owner, index)?;
    let c = read_credit(tx, &owner.attempt_id, index)?
        .ok_or_else(|| Fault::Refused("source_mismatch", "native completion marked charge lacks original credit".into()))?;
    if !c.consumed { return mismatch("native completion diagnostic obligation remains outstanding"); }
    let (report, revision) = read_terminal(tx, &c)?
        .ok_or_else(|| Fault::Refused("source_mismatch", "native completion exact terminal diagnostic absent".into()))?;
    if caller(&report) != *owner { return mismatch("native completion terminal original caller differs"); }
    check_rejection(tx, &report)?;
    let Observation::NativeRequestDiagnostic { observed_outcome, .. } = report.observation else { unreachable!() };
    Ok(Some((observed_outcome, revision)))
}

/// Successful native result admission calls this after its existing currentness
/// proof. Walk retained records, not the configured model-request allowance.
/// Historical unmarked charges have no new diagnostic obligation.
pub(super) fn require_attempt_terminals(tx: &Connection<'_>, task: &Task) -> Result<()> {
    let attempt = task.attempt.as_ref().ok_or_else(||
        Fault::Refused("source_mismatch", "native result diagnostic attempt absent".into()))?;
    let owner = Caller { task_id: task.id.clone(), attempt_id: attempt.id.clone(),
        generation: attempt.generation, worker_id: attempt.worker_id.clone(),
        incarnation: attempt.worker_incarnation.clone() };
    if !same_owner(task, &owner) { return mismatch("native result diagnostic attempt differs"); }
    if !table_available(tx)? { legacy_vocabulary(tx)?; return Ok(()); }
    let bound = inventory_bound(tx)?;
    let limit = bound.checked_add(1).ok_or_else(|| Fault::Store("native result diagnostic inventory overflow".into()))?;
    let bytes = tx.limits.transport.identity_bytes.checked_add(1)
        .ok_or_else(|| Fault::Store("native result diagnostic identity bound overflow".into()))?;

    // Every marked charge was admitted at reservation_id. Select its byte-exact
    // prefix before decoding, so unrelated output receipts cost no body reads.
    // Deleting a credit cannot hide the original marked reservation here.
    let zero = contract::reservation_id(&owner.attempt_id, 0);
    let stem = zero.strip_suffix("-0")
        .ok_or_else(|| Fault::Store("native reservation identity prefix differs".into()))?;
    let prefix = format!("{stem}-");
    let upper = format!("{stem}.");
    let mut query = tx.prepare("SELECT CAST(substr(CAST(id AS BLOB),1,?1) AS TEXT) FROM receipts WHERE id COLLATE BINARY>=?3 AND id COLLATE BINARY<?4 ORDER BY id COLLATE BINARY LIMIT ?2")?;
    let mut rows = query.query(params![sql_bound(bytes)?, sql_bound(limit)?, prefix, upper])?;
    let mut count = 0usize;
    while let Some(row) = rows.next()? {
        count += 1;
        if count > bound { return Err(Fault::Store("native result diagnostic receipt inventory exceeds selected bound".into())); }
        let id: String = row.get(0)?;
        let record = metadata::read(tx, &id)?
            .ok_or_else(|| Fault::Store("native result diagnostic receipt disappeared".into()))?;
        let Operation::ObserveWorker { report } = &record.request else { continue };
        if !matches!(record.outcome, Outcome::Ok { .. }) { continue; }
        let Observation::Usage { model_requests, native_diagnostic_reserved: true, .. } = &report.observation else { continue };
        if caller(report) != owner || id != contract::reservation_id(&owner.attempt_id, *model_requests) {
            return mismatch("native result marked reservation caller or identity differs");
        }
        if require_terminal(tx, &owner, *model_requests)?.is_none() {
            return mismatch("native result marked reservation lacks its terminal diagnostic");
        }
    }

    // Check the reverse direction as well: a surviving credit must still name
    // a marked original charge and its consumed, acknowledged terminal receipt.
    let mut query = tx.prepare("SELECT request_index FROM native_request_diagnostic_credits WHERE attempt_id=?1 ORDER BY request_index LIMIT ?2")?;
    let mut rows = query.query(params![owner.attempt_id, sql_bound(limit)?])?;
    count = 0;
    while let Some(row) = rows.next()? {
        count += 1;
        if count > bound { return Err(Fault::Store("native result diagnostic credit inventory exceeds selected bound".into())); }
        let index = u32::try_from(row.get::<_, i64>(0)?)
            .map_err(|_| Fault::Store("native result diagnostic request index invalid".into()))?;
        read_credit(tx, &owner.attempt_id, index)?
            .ok_or_else(|| Fault::Store("native result diagnostic credit disappeared".into()))?;
        if require_terminal(tx, &owner, index)?.is_none() {
            return mismatch("native result diagnostic credit lacks a marked original charge");
        }
    }
    Ok(())
}

pub(super) fn candidate(tx: &Connection<'_>, op: &Operation) -> Result<Option<DiagnosticCreditId>> {
    let Operation::ObserveWorker { report } = op else { return Ok(None) };
    let Observation::NativeRequestDiagnostic { request_index, .. } = &report.observation else { return Ok(None) };
    core_report(tx, report)?;
    let c = read_credit(tx, &report.attempt_id, *request_index)?.ok_or_else(||
        Fault::Refused("source_mismatch", "native diagnostic original reserved credit absent".into()))?;
    if c.consumed || read_terminal(tx, &c)?.is_some() {
        return mismatch("native diagnostic credit already consumed; reconcile original receipt");
    }
    check_rejection(tx, report)?;
    Ok(Some(DiagnosticCreditId { credit: c, report: report.clone() }))
}
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A failed platform-owned provider request MUST retain bounded diagnostics sufficient to identify the observed failure class and available provider retry or quota information.
pub(super) fn debit(tx: &Connection<'_>, id: DiagnosticCreditId, outcome: &Outcome) -> Result<()> {
    if !matches!(outcome, Outcome::Ok { data: Data::Acknowledged }) {
        return mismatch("native diagnostic receipt credit requires its exact acknowledged observation");
    }
    core_report(tx, &id.report)?;
    check_rejection(tx, &id.report)?;
    let c = id.credit;
    if tx.execute("UPDATE native_request_diagnostic_credits SET consumed=1 WHERE attempt_id=?1 AND request_index=?2 AND reservation_id=?3 AND diagnostic_id=?4 AND consumed=0",
        params![c.attempt_id, i64::from(c.request_index), c.reservation_id, c.diagnostic_id])? != 1 {
        return Err(Fault::Store("native diagnostic original credit missing at receipt debit".into()));
    }
    Ok(())
}

/// Called before the Usage counter update, inside that same action transaction.
/// Replayed Usage never reaches here. A token-only Usage earns no extra credit.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A failed platform-owned provider request MUST retain bounded diagnostics sufficient to identify the observed failure class and available provider retry or quota information.
pub(super) fn reserve(tx: &Connection<'_>, task: &Task, report: &Report, previous_requests: u32, cfg: &ControllerConfig) -> Result<()> {
    let Observation::Usage { model_requests, native_diagnostic_reserved, .. } = &report.observation else {
        return mismatch("native diagnostic reservation is not Usage");
    };
    if task.input.definition.executor != ExecutorKind::Native {
        return if *native_diagnostic_reserved { mismatch("non-native Usage cannot reserve a native diagnostic") } else { Ok(()) };
    }
    if *model_requests == previous_requests {
        return if *native_diagnostic_reserved { mismatch("unchanged Usage cannot reserve another native diagnostic") } else { Ok(()) };
    }
    let index = previous_requests.checked_add(1).ok_or_else(|| Fault::Refused("capacity", "native request charge overflow".into()))?;
    if *model_requests != index || !*native_diagnostic_reserved || !same_owner(task, &caller(report))
        || report.report_id != contract::reservation_id(&report.attempt_id, index) || task.status != TaskStatus::Active
        || !task.attempt.as_ref().is_some_and(|a| !a.stopped && !a.uncertain && a.phase != AttemptPhase::StopRequested) {
        return mismatch("new native charge requires exactly one marked original Usage reservation");
    }
    let charged: u32 = tx.query_row("SELECT model_requests FROM attempts WHERE id=?1 AND task_id=?2",
        params![report.attempt_id, report.task_id], |r| r.get(0))?;
    if charged != previous_requests { return mismatch("native diagnostic reservation does not follow its original charged counter"); }
    let stored = load(tx, &task.id)?;
    if stored.task != *task || super::operations::used_model_requests(tx, &task.id)?
        .checked_add(u64::from(stored.delegated_model_requests)).and_then(|n| n.checked_add(1))
        .is_none_or(|n| n > u64::from(task.input.limits.model_requests)) {
        return refuse("capacity", "native diagnostic reservation exceeds original model-request allowance");
    }
    admission_bound(tx, task, report, index)?;
    if !table_available(tx)? { return mismatch("new native diagnostic reservation requires schema18"); }
    if !cleanup_available(tx, cfg, 1)? {
        return refuse("capacity", "native diagnostic reservation exceeds controller.max_cleanup_receipts");
    }
    if read_credit(tx, &report.attempt_id, index)?.is_some() {
        return mismatch("native diagnostic reservation already exists without original receipt replay");
    }
    let c = credit(&report.attempt_id, index);
    if metadata::read(tx, &c.diagnostic_id)?.is_some() {
        return mismatch("native diagnostic identity was already occupied before its charge");
    }
    tx.execute("INSERT INTO native_request_diagnostic_credits(attempt_id,request_index,reservation_id,diagnostic_id,consumed) VALUES(?1,?2,?3,?4,0)",
        params![c.attempt_id, i64::from(index), c.reservation_id, c.diagnostic_id])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::{HttpDiagnostic, NumericDiagnostic, ProviderFailureClass,
        ProviderRejection, QuotaDiagnostic, RetryAfter, RetryHeaderDiagnostic};

    // Pure consistency specimens, never positive original-transaction proof.
    fn facts() -> ProviderFailureDiagnostic {
        ProviderFailureDiagnostic::http(ProviderFailureClass::HttpRefused, HttpDiagnostic {
            status: HttpStatusDiagnostic::Value(429), body: DiagnosticBodyState::Complete,
            category: CategoryDiagnostic::Value(ProviderErrorCategory::RateLimited),
            scheduler_retry_after: SchedulerRetryDiagnostic::Value(RetryAfter::DelayMs(40)),
            retry_after: RetryHeaderDiagnostic::Absent,
            retry_after_ms: NumericDiagnostic::Value(40),
            requests_limit: NumericDiagnostic::Absent, requests_remaining: NumericDiagnostic::Absent,
            tokens_limit: NumericDiagnostic::Absent, tokens_remaining: NumericDiagnostic::Absent,
            requests_reset_ms: NumericDiagnostic::Absent, tokens_reset_ms: NumericDiagnostic::Absent,
            quota: QuotaDiagnostic::Absent,
        })
    }
    fn reports() -> (Report, Report) {
        let binding = ProviderRequestBinding { bytes: 42, sha256: "a".repeat(64) };
        let diagnostic = Report { report_id: contract::diagnostic_id("attempt", 1),
            task_id: "task".into(), attempt_id: "attempt".into(), generation: 1,
            worker_id: "worker".into(), incarnation: "incarnation".into(),
            observation: Observation::NativeRequestDiagnostic { request_index: 1,
                endpoint: "https://example.invalid/v1/responses".into(), model: "selected-model".into(),
                request_binding: Some(binding.clone()),
                observed_outcome: Observed::KnownRejected { rejection_kind: RejectionKind::RateLimited },
                failure: Some(facts()) } };
        let mut rejection = diagnostic.clone();
        rejection.report_id = contract::rejection_id("attempt", 1);
        rejection.observation = Observation::NativeRequestRejected { request_index: 1,
            endpoint: "https://example.invalid/v1/responses".into(), model: "selected-model".into(),
            request_binding: binding,
            rejection: ProviderRejection { kind: RejectionKind::RateLimited, retry_after: RetryAfter::DelayMs(40) },
            diagnostic: "x".into() };
        (diagnostic, rejection)
    }

    #[test]
    fn not_started_requires_typed_failure_without_http_facts() {
        assert!(not_started_facts(None).is_err());
        let local = ProviderFailureDiagnostic::new(ProviderFailureClass::LocalAdmission);
        assert!(not_started_facts(Some(&local)).is_ok());
        assert!(not_started_facts(Some(&facts())).is_err());
    }

    #[test]
    fn partial_or_overloaded_http_facts_cannot_be_promoted_to_known_rejection() {
        assert!(known_facts(RejectionKind::RateLimited, &facts()).is_ok());
        for body in [DiagnosticBodyState::Pending, DiagnosticBodyState::ReadFailed,
            DiagnosticBodyState::TaskDeadline, DiagnosticBodyState::InvalidJson] {
            let mut partial = facts(); partial.http.as_mut().unwrap().body = body;
            assert!(known_facts(RejectionKind::RateLimited, &partial).is_err());
        }
        let mut overloaded = facts();
        overloaded.http.as_mut().unwrap().status = HttpStatusDiagnostic::Value(503);
        assert!(known_facts(RejectionKind::RateLimited, &overloaded).is_err());
        overloaded.http.as_mut().unwrap().status = HttpStatusDiagnostic::Value(429);
        overloaded.http.as_mut().unwrap().category = CategoryDiagnostic::Value(ProviderErrorCategory::Overloaded);
        assert!(known_facts(RejectionKind::RateLimited, &overloaded).is_err());
    }

    #[test]
    fn original_rejection_requires_matching_observed_outcome_route_owner_and_input() {
        let (diagnostic, rejection) = reports();
        assert!(same_rejection(&diagnostic, &rejection).is_ok());
        let mut unknown = diagnostic.clone();
        if let Observation::NativeRequestDiagnostic { observed_outcome, .. } = &mut unknown.observation { *observed_outcome = Observed::Unknown; }
        assert!(same_rejection(&unknown, &rejection).is_err(), "identical headers cannot turn Unknown into the canonical rejection");
        let mut changed = diagnostic.clone(); changed.incarnation = "another-incarnation".into();
        assert!(same_rejection(&changed, &rejection).is_err());
        let mut changed = diagnostic.clone();
        if let Observation::NativeRequestDiagnostic { endpoint, .. } = &mut changed.observation { *endpoint = "https://other.invalid/v1/responses".into(); }
        assert!(same_rejection(&changed, &rejection).is_err());
        let mut changed = diagnostic;
        if let Observation::NativeRequestDiagnostic { request_binding, .. } = &mut changed.observation { request_binding.as_mut().unwrap().sha256 = "b".repeat(64); }
        assert!(same_rejection(&changed, &rejection).is_err());
    }

    #[test]
    fn redacted_retry_fact_is_unavailable_and_never_reinterpreted_as_absent_or_zero() {
        let (mut diagnostic, rejection) = reports();
        if let Observation::NativeRequestDiagnostic { failure, .. } = &mut diagnostic.observation {
            failure.as_mut().unwrap().http.as_mut().unwrap().scheduler_retry_after = SchedulerRetryDiagnostic::Value(RetryAfter::Absent);
        }
        assert!(same_rejection(&diagnostic, &rejection).is_err());
        if let Observation::NativeRequestDiagnostic { failure, .. } = &mut diagnostic.observation {
            let http = failure.as_mut().unwrap().http.as_mut().unwrap();
            http.scheduler_retry_after = SchedulerRetryDiagnostic::Redacted;
            http.status = HttpStatusDiagnostic::Redacted;
            http.category = CategoryDiagnostic::Redacted;
        }
        assert!(same_rejection(&diagnostic, &rejection).is_ok());
        if let Observation::NativeRequestDiagnostic { failure, .. } = &mut diagnostic.observation {
            failure.as_mut().unwrap().http.as_mut().unwrap().scheduler_retry_after = SchedulerRetryDiagnostic::Value(RetryAfter::DelayMs(0));
        }
        assert!(same_rejection(&diagnostic, &rejection).is_err());
    }

    // Deliberately minimal corruption-reader fixture. It contains only refused
    // receipts and cannot manufacture a successful task, charge or diagnostic.
    fn legacy() -> rusqlite::Connection {
        let sql = rusqlite::Connection::open_in_memory().unwrap();
        sql.execute_batch("CREATE TABLE counters(kind TEXT PRIMARY KEY,value INTEGER NOT NULL); INSERT INTO counters VALUES('controller_revision',1); CREATE TABLE receipts(id TEXT PRIMARY KEY,request TEXT NOT NULL,outcome TEXT NOT NULL,controller_revision INTEGER NOT NULL); PRAGMA user_version=17;").unwrap();
        sql
    }
    fn refused_usage(sql: &rusqlite::Connection, marked: bool) -> String {
        let report = Report { report_id: contract::reservation_id("attempt", 1),
            task_id: "task".into(), attempt_id: "attempt".into(), generation: 1,
            worker_id: "worker".into(), incarnation: "incarnation".into(),
            observation: Observation::Usage { model_requests: 1, input_tokens: None, output_tokens: None,
                native_diagnostic_reserved: marked } };
        let body = encode(&Operation::ObserveWorker { report: report.clone() }).unwrap();
        sql.execute("INSERT INTO receipts VALUES(?1,?2,?3,1)", params![report.report_id, body,
            encode(&Outcome::Refused { code: "controlled_refusal".into(), message: "no accepted occurrence".into() }).unwrap()]).unwrap();
        body
    }

    #[test]
    fn legacy_absence_is_only_zero_after_bounded_vocabulary_validation() {
        let limits = RuntimeLimits::seed().unwrap();
        let sql = legacy();
        let before = refused_usage(&sql, false);
        assert!(!before.contains("native_diagnostic_reserved"), "historical absent bytes stay absent");
        let tx = Connection::new(&sql, &limits);
        assert_eq!(outstanding_credits(&tx).unwrap(), 0);
        assert!(validate_existing(&tx, false).is_ok());
        let after: String = sql.query_row("SELECT request FROM receipts", [], |r| r.get(0)).unwrap();
        assert_eq!(after, before);
        let marked = legacy(); refused_usage(&marked, true);
        let tx = Connection::new(&marked, &limits);
        assert!(outstanding_credits(&tx).is_err());
        assert!(validate_existing(&tx, false).is_err());
        assert_eq!(marked.query_row("SELECT count(*) FROM sqlite_master WHERE name='native_request_diagnostic_credits'", [], |r| r.get::<_, i64>(0)).unwrap(), 0);
    }

    #[test]
    fn only_explicit_schema18_through24_accept_the_retained_credit_table() {
        let limits=RuntimeLimits::seed().unwrap();
        for version in [17,18,19,20,21,22,23,24,25] {
            let sql=legacy();sql.execute_batch(include_str!("schema18-from17.sql")).unwrap();
            sql.pragma_update(None,"user_version",version).unwrap();
            let tx=Connection::new(&sql,&limits);
            let supported=matches!(version,18|19|20|21|22|23|24);
            assert_eq!(table_available(&tx).is_ok(),supported);
            assert_eq!(validate_existing(&tx,true).is_ok(),supported);
            if supported {
                // The new explicit reader version does not weaken the original
                // reservation/credit join, even for a structurally valid row.
                let c=credit("attempt",1);
                sql.execute("INSERT INTO native_request_diagnostic_credits VALUES(?1,1,?2,?3,0)",params![c.attempt_id,c.reservation_id,c.diagnostic_id]).unwrap();
                assert!(validate_existing(&tx,true).is_err());
            }
            assert_eq!(sql.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),version);
        }
        for version in [18,19,20,21,22,23,24,25] {
            let sql=legacy();sql.pragma_update(None,"user_version",version).unwrap();
            assert!(table_available(&Connection::new(&sql,&limits)).is_err());
        }
    }

    #[test]
    fn schema18_missing_credit_table_is_never_historical_zero() {
        let limits = RuntimeLimits::seed().unwrap();
        let sql = legacy(); sql.pragma_update(None, "user_version", 18).unwrap();
        let tx = Connection::new(&sql, &limits);
        assert!(outstanding_credits(&tx).is_err());
        assert!(validate_existing(&tx, false).is_err());
        assert!(validate_existing(&tx, true).is_err());
    }
}

pub(super) fn outstanding_credits(tx: &Connection<'_>) -> Result<u64> {
    if !table_available(tx)? { legacy_vocabulary(tx)?; return Ok(0); }
    let bound = inventory_bound(tx)?;
    let limit = bound.checked_add(1).ok_or_else(|| Fault::Store("native diagnostic inventory overflow".into()))?;
    let (count, outstanding, malformed): (i64, i64, i64) = tx.query_row(
        "SELECT count(*),coalesce(sum(consumed=0),0),coalesce(sum(consumed NOT IN(0,1)),0) FROM (SELECT consumed FROM native_request_diagnostic_credits LIMIT ?1)",
        [sql_bound(limit)?], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
    if unsigned(count)? > bound as u64 || malformed != 0 { return Err(Fault::Store("native diagnostic credit inventory exceeds bound or has invalid state".into())); }
    unsigned(outstanding)
}

/// Validated schema18/19/20/21/22/23/24 caller only, before startup writes. No credit is reconstructed.
pub(super) fn validate_existing(tx: &Connection<'_>, has_credit_table: bool) -> Result<()> {
    if table_available(tx)? != has_credit_table { return Err(Fault::Store("native diagnostic startup schema selection differs".into())); }
    if !has_credit_table { return legacy_vocabulary(tx); }
    outstanding_credits(tx)?;
    let bound = inventory_bound(tx)?;
    let limit = bound.checked_add(1).ok_or_else(|| Fault::Store("native diagnostic inventory overflow".into()))?;
    let bytes = tx.limits.transport.identity_bytes.checked_add(1).ok_or_else(|| Fault::Store("native diagnostic identity bound overflow".into()))?;
    let mut rows_query = tx.prepare("SELECT CAST(substr(CAST(attempt_id AS BLOB),1,?1) AS TEXT),request_index FROM native_request_diagnostic_credits ORDER BY attempt_id,request_index LIMIT ?2")?;
    let mut rows = rows_query.query(params![sql_bound(bytes)?, sql_bound(limit)?])?;
    let mut count = 0usize;
    while let Some(row) = rows.next()? {
        count += 1;
        if count > bound { return Err(Fault::Store("native diagnostic inventory exceeds selected receipt bound".into())); }
        let attempt: String = row.get(0)?;
        let index = u32::try_from(row.get::<_, i64>(1)?).map_err(|_| Fault::Store("native diagnostic request index invalid".into()))?;
        let c = read_credit(tx, &attempt, index)?.ok_or_else(|| Fault::Store("native diagnostic inventory key differs".into()))?;
        let reservation = metadata::read(tx, &c.reservation_id)?.ok_or_else(|| Fault::Store("native diagnostic credit lacks original reservation".into()))?;
        let Operation::ObserveWorker { report } = reservation.request else { return Err(Fault::Store("native diagnostic credit source kind differs".into())) };
        original(tx, &caller(&report), index)?;
        match (c.consumed, read_terminal(tx, &c)?) {
            (false, None) => {},
            (true, Some((diagnostic, _))) => check_rejection(tx, &diagnostic)?,
            _ => return Err(Fault::Store("native diagnostic reserved credit/terminal receipt join differs".into())),
        }
    }
    // Scan the complete bounded receipt inventory, not just surviving rows or
    // json_extract matches: deleted credits and malformed bodies must not hide.
    let mut receipts_query = tx.prepare("SELECT CAST(substr(CAST(id AS BLOB),1,?1) AS TEXT) FROM receipts ORDER BY id LIMIT ?2")?;
    let mut receipts = receipts_query.query(params![sql_bound(bytes)?, sql_bound(limit)?])?;
    count = 0;
    while let Some(row) = receipts.next()? {
        count += 1;
        if count > bound { return Err(Fault::Store("native diagnostic receipt inventory exceeds selected bound".into())); }
        let id: String = row.get(0)?;
        let record = metadata::read(tx, &id)?.ok_or_else(|| Fault::Store("native diagnostic receipt inventory changed".into()))?;
        let Operation::ObserveWorker { report } = &record.request else { continue };
        match &report.observation {
            Observation::Usage { native_diagnostic_reserved: true, model_requests, .. } => {
                // Rejected attempts to reserve never earned a slot.
                if !matches!(record.outcome, Outcome::Ok { .. }) { continue; }
                original(tx, &caller(report), *model_requests)?;
                let c = read_credit(tx, &report.attempt_id, *model_requests)?.ok_or_else(|| Fault::Store("marked native charge lost its diagnostic credit".into()))?;
                if c.reservation_id != id { return Err(Fault::Store("marked native charge has another original reservation".into())); }
            }
            Observation::NativeRequestDiagnostic { request_index, .. } => {
                if !matches!(record.outcome, Outcome::Ok { .. }) { continue; }
                let c = read_credit(tx, &report.attempt_id, *request_index)?.ok_or_else(|| Fault::Store("native diagnostic terminal lost its original credit".into()))?;
                if !c.consumed || c.diagnostic_id != id || read_terminal(tx, &c)?.is_none() {
                    return Err(Fault::Store("native diagnostic terminal has no consumed original credit".into()));
                }
                check_rejection(tx, report)?;
            }
            Observation::NativeRequestRejected { .. } if matches!(record.outcome, Outcome::Ok { .. }) => validate_rejection(tx, report)?,
            _ => {},
        }
    }
    Ok(())
}
