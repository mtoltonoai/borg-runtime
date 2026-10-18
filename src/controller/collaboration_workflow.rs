//! Shared collaboration helpers, called inside the controller's existing transaction.
//! This artifact excludes routing, finalization and migrations.
//! Retained actor values describe history; they never grant current caller authority.
use super::database::{self, decode, encode, integer, refuse, unsigned, Fault, Result, StoreContext};
use crate::collaboration::{documents::*, questions::DecisionQueueItem, workflow::*, Page};
use crate::limits::WorkflowLimits;
use crate::protocol::{AcceptedResult, Attempt, Definition, Observation, Task, TaskStatus};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub(crate) struct Context<'tx, 'db> {
    pub tx: &'tx StoreContext<'db>,
    pub actor: &'tx RecordedActor,
    pub operation_id: &'tx str,
    pub product_revision: u64,
    pub now_ms: u64,
    pub limits: &'tx WorkflowLimits,
}
pub(crate) struct QueryContext<'tx, 'db> {
    pub tx: &'tx StoreContext<'db>,
    pub actor: &'tx RecordedActor,
    pub now_ms: u64,
    pub limits: &'tx WorkflowLimits,
}
impl<'tx, 'db> Context<'tx, 'db> {
    pub fn query(&self) -> QueryContext<'tx, 'db> {
        QueryContext { tx: self.tx, actor: self.actor, now_ms: self.now_ms, limits: self.limits }
    }
}
pub(crate) struct DomainChange<T> {
    pub outcome: T,
    pub effects: Vec<WorkflowEffect>,
}

fn identity(tx: &StoreContext<'_>, value: &str) -> Result<()> {
    if value.len() > tx.limits.transport.identity_bytes {
        return refuse("input_bound", "Configured limit exceeded: transport.identity_bytes");
    }
    if value.is_empty() || value.chars().any(char::is_control) {
        return refuse("invalid_input", "malformed collaboration identity");
    }
    Ok(())
}
fn record_key(record: &RecordRef) -> (&'static str, &str) {
    match record {
        RecordRef::Task { task_id } => ("task", task_id),
        RecordRef::Question { question_id } => ("question", question_id),
        RecordRef::Document { document_id } => ("document", document_id),
        RecordRef::Review { review_id } => ("review", review_id),
    }
}
fn selected_limits<'a>(tx: &'a StoreContext<'_>) -> Result<&'a WorkflowLimits> {
    tx.limits.workflow.as_ref().ok_or_else(|| Fault::Refused(
        "workflow_disabled", "workflow policy is not selected".into()))
}
fn read<T: Serialize + serde::de::DeserializeOwned>(raw: &str, maximum: usize) -> Result<T> {
    if raw.len() > maximum {
        return Err(Fault::Store("stored collaboration record exceeds configured bound".into()));
    }
    decode(raw)
}
fn task(tx: &StoreContext<'_>, id: &str) -> Result<Task> {
    identity(tx, id)?;
    // Keep the existing exact Task/session/repository validator, but bound its
    // body materialization first in this same transaction.
    let length: Option<i64> = tx.query_row(
        "SELECT length(CAST(body AS BLOB)) FROM tasks WHERE id=?1", [id], |r| r.get(0)).optional()?;
    let length = length.ok_or_else(|| Fault::Refused("not_found", "task not found".into()))?;
    if unsigned(length)? > tx.limits.transport.body_bytes as u64 {
        return Err(Fault::Store("stored task exceeds transport.body_bytes".into()));
    }
    Ok(database::load(tx, id)?.task)
}

/// Existing principal, not an authorization decision or a named-human claim.
pub(crate) fn validate_direct_principal(tx: &StoreContext<'_>, principal: &PrincipalRef) -> Result<()> {
    match principal {
        PrincipalRef::TrustedHostOperator => Ok(()),
        PrincipalRef::Agent { agent_id } => {
            identity(tx, agent_id)?;
            let raw: Option<String> = tx.query_row(
                "SELECT CAST(substr(CAST(definition AS BLOB),1,?2) AS TEXT) FROM agents WHERE id=?1",
                params![agent_id, super::database::sql_bound(tx.limits.transport.body_bytes.saturating_add(1))?], |r| r.get(0)).optional()?;
            let raw = raw.ok_or_else(|| Fault::Refused("not_found", "agent not found".into()))?;
            let definition: Definition = read(&raw, tx.limits.transport.body_bytes)?;
            if definition.id != *agent_id {
                return Err(Fault::Store("agent definition identity mismatch".into()));
            }
            Ok(())
        }
    }
}

/// New recipient/owner selection only. Historical actors retain existence-based validation.
pub(crate) fn validate_new_principal(tx: &StoreContext<'_>, principal: &PrincipalRef) -> Result<()> {
    validate_direct_principal(tx, principal)?;
    if let PrincipalRef::Agent { agent_id } = principal { super::agents::require_active(tx, agent_id)?; }
    Ok(())
}

/// Historical binding only: stopped/replaced attempts and workers are allowed.
/// Root must separately establish the current actor before constructing Context.
pub(crate) fn validate_recorded_actor(tx: &StoreContext<'_>, actor: &RecordedActor) -> Result<()> {
    validate_direct_principal(tx, &actor.principal)?;
    match (&actor.principal, &actor.provenance) {
        (PrincipalRef::TrustedHostOperator, ActorProvenance::TrustedHost { .. }) => Ok(()),
        (PrincipalRef::Agent { agent_id: principal }, ActorProvenance::BoundAgentSession {
            agent_id, session_id, task_id, attempt_id, generation, worker_id, incarnation,
        }) => {
            for id in [agent_id, session_id, task_id, attempt_id, worker_id, incarnation] { identity(tx, id)?; }
            if principal != agent_id || *generation == 0 || *generation > i64::MAX as u64 {
                return Err(Fault::Store("recorded actor principal/generation mismatch".into()));
            }
            let saved = super::sessions::historical_task(tx, task_id, session_id)?;
            let raw: Option<(String, String)> = tx.query_row(
                "SELECT task_id,CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM attempts WHERE id=?1",
                params![attempt_id, super::database::sql_bound(tx.limits.transport.body_bytes.saturating_add(1))?],
                |r| Ok((r.get(0)?, r.get(1)?))).optional()?;
            let (owner, raw) = raw.ok_or_else(|| Fault::Store("recorded actor attempt is absent".into()))?;
            let attempt: Attempt = read(&raw, tx.limits.transport.body_bytes)?;
            if owner != *task_id || attempt.id != *attempt_id || attempt.task_id != *task_id
                || attempt.session_id != *session_id || attempt.generation != *generation
                || attempt.worker_id != *worker_id || attempt.worker_incarnation != *incarnation
                || saved.session_id != *session_id || saved.assigned_agent_id != *agent_id
                || saved.input.definition.id != *agent_id {
                return Err(Fault::Store("recorded actor retained binding mismatch".into()));
            }
            let session = super::sessions::get(tx, session_id)?;
            session.validate_for(&attempt, &saved.input)
                .map_err(|_| Fault::Store("recorded actor session binding mismatch".into()))?;
            // Deliberately no active(), owned(), current worker incarnation,
            // latest attempt, deadline or wall-clock test on retained history.
            Ok(())
        }
        _ => Err(Fault::Store("recorded actor principal/provenance mismatch".into())),
    }
}

pub(crate) fn record_owner(tx: &StoreContext<'_>, record: &RecordRef) -> Result<PrincipalRef> {
    Ok(record_owner_revision(tx, record)?.owner)
}

/// Follow immutable linked-record ownership without constructing a fake caller.
/// The returned revision belongs to the requested aggregate, not its owner target.
pub(crate) fn record_owner_revision(tx: &StoreContext<'_>, record: &RecordRef) -> Result<RecordOwnerRevision> {
    Ok(record_owner_and_change(tx, record)?.0)
}

/// The requested aggregate's canonical change marker, not its linked owner's.
/// This grants no authority and does not assume committed-only startup context.
pub(crate) fn record_changed_product_revision(tx: &StoreContext<'_>, record: &RecordRef) -> Result<u64> {
    let changed = record_owner_and_change(tx, record)?.1;
    let _ = integer(changed)?;
    Ok(changed)
}

fn record_owner_and_change(tx: &StoreContext<'_>, record: &RecordRef) -> Result<(RecordOwnerRevision, u64)> {
    let limits = selected_limits(tx)?;
    let mut current = record.clone();
    let mut first_revision = None;
    let mut first_changed_revision = None;
    let mut visited = BTreeSet::new();
    for _ in 0..limits.graph_scan_nodes {
        let (kind, id) = record_key(&current);
        identity(tx, id)?;
        if !visited.insert((kind, id.to_owned())) {
            return Err(Fault::Store("collaboration owner linkage cycle".into()));
        }
        match &current {
            RecordRef::Task { task_id } => {
                let _ = task(tx, task_id)?;
                let owner = super::collaboration_work::owner_revision(tx, task_id, limits)?;
                validate_direct_principal(tx, &owner.owner)?;
                let changed = match first_changed_revision {
                    Some(changed) => changed,
                    None => super::collaboration_work::changed_product_revision(tx, task_id, limits)?,
                };
                return Ok((RecordOwnerRevision { owner: owner.owner, revision: first_revision.unwrap_or(owner.revision) }, changed));
            }
            RecordRef::Question { question_id } => {
                let q = super::collaboration_questions::get(tx, question_id, limits.record_bytes)?;
                first_revision.get_or_insert(q.revision);
                first_changed_revision.get_or_insert(q.changed_product_revision);
                current = q.posed.record;
            }
            RecordRef::Document { document_id } => {
                let row: Option<(i64, i64, String)> = tx.query_row(
                    "SELECT revision,created_product_revision,CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM workflow_documents WHERE id=?1",
                    params![document_id, super::database::sql_bound(limits.record_bytes.saturating_add(1))?],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).optional()?;
                let (revision, created, raw) = row.ok_or_else(|| Fault::Refused("not_found", "document not found".into()))?;
                let d: Document = read(&raw, limits.record_bytes)?;
                if d.id != *document_id || revision != integer(d.revision)? || revision <= 0
                    || created != integer(d.created_product_revision)? || created <= 0
                    || d.changed_product_revision < d.created_product_revision
                    || d.current.document_id != d.id || d.current.version == 0 || d.current.version > d.revision {
                    return Err(Fault::Store("document ownership head mismatch".into()));
                }
                identity(tx, &d.current.content_id)?;
                validate_direct_principal(tx, &d.responsible_owner)?;
                return Ok((RecordOwnerRevision { owner: d.responsible_owner, revision: first_revision.unwrap_or(d.revision) },
                    first_changed_revision.unwrap_or(d.changed_product_revision)));
            }
            RecordRef::Review { review_id } => {
                let row: Option<(i64, i64, String, String, String)> = tx.query_row(
                    "SELECT revision,created_product_revision,CAST(substr(CAST(reviewer AS BLOB),1,?2) AS TEXT),CAST(substr(CAST(status AS BLOB),1,?2) AS TEXT),CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM workflow_reviews WHERE id=?1",
                    params![review_id, super::database::sql_bound(limits.record_bytes.saturating_add(1))?],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))).optional()?;
                let (revision, created, reviewer, state, raw) = row.ok_or_else(|| Fault::Refused("not_found", "review not found".into()))?;
                // This owner lookup also runs before domain startup validation.
                // Bound index materialization independently, then compare exact
                // index bytes with the canonical body below; no prefix is proof.
                if reviewer.len() > limits.record_bytes || state.len() > limits.record_bytes {
                    return Err(Fault::Store("stored review index exceeds configured workflow.record_bytes".into()));
                }
                let r: Review = read(&raw, limits.record_bytes)?;
                let expected_state = match &r.status { ReviewStatus::Open => "open", ReviewStatus::Decided => "decided", ReviewStatus::Withdrawn => "withdrawn" };
                if r.id != *review_id || revision != integer(r.revision)? || revision <= 0
                    || created != integer(r.created_product_revision)? || created <= 0
                    || r.changed_product_revision < r.created_product_revision || reviewer != encode(&r.reviewer)?
                    || state != expected_state || (r.status == ReviewStatus::Decided) != r.decision_id.is_some() {
                    return Err(Fault::Store("review ownership head mismatch".into()));
                }
                validate_direct_principal(tx, &r.reviewer)?;
                validate_recorded_actor(tx, &r.requested_by)?;
                validate_target_shape(tx, &r.target)?;
                first_revision.get_or_insert(r.revision);
                first_changed_revision.get_or_insert(r.changed_product_revision);
                current = super::collaboration_reviews::target_record(&r.target);
            }
        }
    }
    refuse("capacity", "Configured limit exceeded: workflow.graph_scan_nodes")
}

fn validate_target_shape(tx: &StoreContext<'_>, target: &ReviewTarget) -> Result<()> {
    match target {
        ReviewTarget::DocumentVersion { version } => {
            identity(tx, &version.document_id)?;
            identity(tx, &version.content_id)?;
            if version.version == 0 || version.version > i64::MAX as u64 {
                return refuse("invalid_input", "invalid document version");
            }
        }
        ReviewTarget::AcceptedTaskResult { result } | ReviewTarget::AcceptedTaskCheckpoint { result } => {
            identity(tx, &result.task_id)?;
            identity(tx, &result.attempt_id)?;
            if result.terminal_task_revision == 0 || result.terminal_task_revision > i64::MAX as u64
                || result.acceptance_history_sequence == 0 || result.acceptance_history_sequence > i64::MAX as u64 {
                return refuse("invalid_input", "invalid accepted history identity");
            }
            if matches!(target, ReviewTarget::AcceptedTaskCheckpoint { .. }) && result.checkpoint.is_none() {
                return refuse("invalid_input", "accepted checkpoint target has no checkpoint");
            }
            if let Some(checkpoint) = &result.checkpoint {
                identity(tx, &checkpoint.registration.repository_id)?;
                identity(tx, &checkpoint.registration.registration_revision)?;
                identity(tx, &checkpoint.operation_id)?;
                if checkpoint.repository.len() > tx.limits.controller.max_repository_identity_bytes {
                    return refuse("input_bound", "Configured limit exceeded: controller.max_repository_identity_bytes");
                }
                if checkpoint.remote_ref.len() > tx.limits.controller.max_checkpoint_ref_bytes {
                    return refuse("input_bound", "Configured limit exceeded: controller.max_checkpoint_ref_bytes");
                }
            }
            // Nested checkpoint fields are compared to the exact retained result,
            // with its original accepted repository/representation policy. They
            // are not reinterpreted as a newly selected repository or result.
        }
    }
    Ok(())
}

/// Find the retained acceptance, not a present mutable task revision. The exact
/// observation bytes are bounded, canonical, and confined to the accepted
/// attempt's immutable assignment interval. At most two candidate rows are read.
fn accepted_occurrence(tx: &StoreContext<'_>, saved: &Task, result: &AcceptedResult) -> Result<(u64, u64)> {
    let mut stmt = tx.prepare(
        "SELECT seq,revision,CAST(substr(CAST(body AS BLOB),1,?3) AS TEXT) FROM history WHERE task_id=?1 AND kind='attempt_assigned' AND json_extract(CAST(substr(CAST(body AS BLOB),1,?3) AS TEXT),'$.id')=?2 ORDER BY seq LIMIT 2")?;
    let rows = stmt.query_map(params![saved.id, result.attempt_id, super::database::sql_bound(tx.limits.transport.body_bytes.saturating_add(1))?],
        |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, String>(2)?)))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if rows.len() != 1 { return Err(Fault::Store("accepted attempt assignment is absent or ambiguous".into())); }
    let (assignment_seq, assignment_revision, raw) = &rows[0];
    let original: Attempt = read(raw, tx.limits.transport.body_bytes)?;
    let retained: Option<(String, String)> = tx.query_row(
        "SELECT task_id,CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM attempts WHERE id=?1",
        params![result.attempt_id, super::database::sql_bound(tx.limits.transport.body_bytes.saturating_add(1))?],
        |r| Ok((r.get(0)?, r.get(1)?))).optional()?;
    let (owner, raw) = retained.ok_or_else(|| Fault::Store("accepted attempt is absent".into()))?;
    let retained: Attempt = read(&raw, tx.limits.transport.body_bytes)?;
    if *assignment_seq <= 0 || *assignment_revision <= 0 || owner != saved.id
        || original.id != result.attempt_id || original.task_id != saved.id || original.session_id != saved.session_id
        || original.generation == 0 || original.generation > i64::MAX as u64
        || original.phase != crate::protocol::AttemptPhase::Assigned || original.stopped || original.uncertain
        || original.id != retained.id || original.task_id != retained.task_id
        || original.session_id != retained.session_id || original.generation != retained.generation
        || original.worker_id != retained.worker_id || original.worker_incarnation != retained.worker_incarnation
        || original.deadline_unix_ms != retained.deadline_unix_ms {
        return Err(Fault::Store("accepted attempt/history binding mismatch".into()));
    }
    let session = super::sessions::get(tx, &saved.session_id)?;
    session.validate_for(&original, &saved.input)
        .map_err(|_| Fault::Store("accepted original attempt/session mismatch".into()))?;
    let next_assignment: Option<i64> = tx.query_row(
        "SELECT MIN(seq) FROM history WHERE task_id=?1 AND kind='attempt_assigned' AND seq>?2",
        params![saved.id, assignment_seq], |r| r.get(0))?;
    let expected = encode(&Observation::ResultCandidate {
        checks_passed: true, summary: result.summary.clone(), checkpoint: result.checkpoint.clone(),
        validation: result.validation.clone(), output: result.output.clone(),
    })?;
    if expected.len() > tx.limits.transport.body_bytes {
        return Err(Fault::Store("accepted history exceeds transport.body_bytes".into()));
    }
    let mut stmt = tx.prepare(
        "SELECT seq,revision,CAST(substr(CAST(body AS BLOB),1,?4) AS TEXT) FROM history WHERE task_id=?1 AND kind='worker_observation' AND seq>?2 AND (?3 IS NULL OR seq<?3) AND json_extract(CAST(substr(CAST(body AS BLOB),1,?4) AS TEXT),'$.kind')='result_candidate' ORDER BY seq LIMIT 2")?;
    let rows = stmt.query_map(params![saved.id, assignment_seq, next_assignment, super::database::sql_bound(tx.limits.transport.body_bytes.saturating_add(1))?],
        |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, String>(2)?)))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if rows.len() != 1 { return Err(Fault::Store("accepted result history is absent or ambiguous".into())); }
    let (sequence, revision, raw) = &rows[0];
    let _: Observation = read(raw, tx.limits.transport.body_bytes)?;
    if *sequence <= *assignment_seq || *revision <= *assignment_revision
        || unsigned(*revision)? > saved.revision || *raw != expected {
        return Err(Fault::Store("accepted result canonical occurrence mismatch".into()));
    }
    Ok((unsigned(*sequence)?, unsigned(*revision)?))
}

pub(crate) fn accepted_target(q: &QueryContext<'_, '_>, task_id: &str, scope: AcceptedReviewScope) -> Result<ReviewTarget> {
    let saved = task(q.tx, task_id)?;
    if saved.status != TaskStatus::Succeeded {
        return refuse("not_accepted", "task has no successful accepted result");
    }
    let result = saved.result.as_ref().ok_or_else(|| Fault::Store("successful task lacks accepted result".into()))?;
    identity(q.tx, &result.attempt_id)?;
    if !crate::delegation::accepted_result_fits(&saved.input.limits, result) {
        return Err(Fault::Store("accepted result exceeds its original accepted bounds".into()));
    }
    let (acceptance_history_sequence, terminal_task_revision) = accepted_occurrence(q.tx, &saved, result)?;
    let reference = AcceptedResultRef { task_id: saved.id, attempt_id: result.attempt_id.clone(),
        terminal_task_revision, acceptance_history_sequence, checkpoint: result.checkpoint.clone() };
    match scope {
        AcceptedReviewScope::Result => Ok(ReviewTarget::AcceptedTaskResult { result: reference }),
        AcceptedReviewScope::Checkpoint if reference.checkpoint.is_some() => Ok(ReviewTarget::AcceptedTaskCheckpoint { result: reference }),
        AcceptedReviewScope::Checkpoint => refuse("not_found", "accepted result has no checkpoint"),
    }
}

pub(crate) fn validate_review_target(ctx: &Context<'_, '_>, target: &ReviewTarget) -> Result<()> {
    validate_target_shape(ctx.tx, target)?;
    let q = ctx.query();
    let exact = match target {
        ReviewTarget::DocumentVersion { version } => {
            super::collaboration_documents::get_version(&q, &version.document_id, version.version)?.reference == *version
        }
        ReviewTarget::AcceptedTaskResult { result } => accepted_target(&q, &result.task_id, AcceptedReviewScope::Result)? == *target,
        ReviewTarget::AcceptedTaskCheckpoint { result } => accepted_target(&q, &result.task_id, AcceptedReviewScope::Checkpoint)? == *target,
    };
    if !exact { return refuse("target_mismatch", "review target differs from exact immutable subject"); }
    Ok(())
}

pub(crate) fn stage_record_change(ctx: &Context<'_, '_>, record: &RecordRef, expected_revision: u64) -> Result<u64> {
    identity(ctx.tx, record_key(record).1)?;
    let actual = record_owner_revision(ctx.tx, record)?;
    if actual.revision != expected_revision { return refuse("revision_conflict", "collaboration revision changed"); }
    if product_revision(ctx.tx)?.checked_add(1) != Some(ctx.product_revision) {
        return Err(Fault::Store("context does not name the pending product revision".into()));
    }
    let sql = match record {
        RecordRef::Task { .. } => "SELECT json_extract(body,'$.changed_product_revision') FROM workflow_task_state WHERE task_id=?1",
        RecordRef::Question { .. } => "SELECT json_extract(body,'$.changed_product_revision') FROM workflow_questions WHERE id=?1",
        RecordRef::Document { .. } => "SELECT json_extract(body,'$.changed_product_revision') FROM workflow_documents WHERE id=?1",
        RecordRef::Review { .. } => "SELECT json_extract(body,'$.changed_product_revision') FROM workflow_reviews WHERE id=?1",
    };
    let changed: Option<i64> = ctx.tx.query_row(sql, [record_key(record).1], |r| r.get(0)).optional()?;
    if changed == Some(integer(ctx.product_revision)?) {
        return refuse("duplicate_record_change", "aggregate already changed in this product transaction");
    }
    match record {
        RecordRef::Task { task_id } => super::collaboration_work::touch(ctx, task_id, expected_revision),
        RecordRef::Question { question_id } => super::collaboration_questions::touch(ctx, question_id, expected_revision),
        RecordRef::Document { document_id } => super::collaboration_documents::touch(ctx, document_id, expected_revision),
        RecordRef::Review { review_id } => super::collaboration_reviews::touch(ctx, review_id, expected_revision),
    }
}

// Closed table set shared with root migration and domain ownership. A missing
// table is a store error, never silently counted as zero. Existing tasks/history
// are not charged again as new product heads/revisions.
const HEAD_TABLES: &[&str] = &["workflow_task_state", "workflow_waits", "workflow_questions", "workflow_documents", "workflow_reviews"];
const IMMUTABLE_TABLES: &[&str] = &["workflow_answers", "workflow_question_actions", "workflow_document_versions", "workflow_comments", "workflow_review_decisions", "workflow_review_withdrawals", "workflow_wait_closures", "workflow_wait_consumptions", "workflow_events"];
fn count_rows(tx: &StoreContext<'_>, tables: &[&str]) -> Result<usize> {
    let mut total = 0usize;
    for table in tables {
        // table comes only from closed source constants, never caller data.
        let n: i64 = tx.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))?;
        let n = usize::try_from(n).map_err(|_| Fault::Store("invalid workflow row count".into()))?;
        total = total.checked_add(n).ok_or_else(|| Fault::Store("workflow row count overflow".into()))?;
    }
    Ok(total)
}
pub(crate) fn ensure_capacity(ctx: &Context<'_, '_>, additional_heads: usize, additional_immutable: usize) -> Result<()> {
    let heads = count_rows(ctx.tx, HEAD_TABLES)?;
    let revisions = count_rows(ctx.tx, IMMUTABLE_TABLES)?;
    if heads.checked_add(additional_heads).is_none_or(|n| n > ctx.limits.records) {
        return refuse("capacity", "Configured limit exceeded: workflow.records");
    }
    if revisions.checked_add(additional_immutable).is_none_or(|n| n > ctx.limits.revisions) {
        return refuse("capacity", "Configured limit exceeded: workflow.revisions");
    }
    Ok(())
}

/// Validate all retained workflow rows before the database owner enables
/// writes. An absent policy permits an empty additive schema, not unreadable
/// retained data. No actor here is a grant to invoke a mutation.
pub(super) fn validate_existing(tx: &StoreContext<'_>, now_ms: u64) -> Result<()> {
    let revision = product_revision(tx)?;
    let Some(limits) = tx.limits.workflow.as_ref() else {
        let deliveries: i64 = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM sessions WHERE workflow_delivery IS NOT NULL)",
            [], |row| row.get(0))?;
        if revision != 0 || deliveries != 0 || count_rows(tx, HEAD_TABLES)? != 0
            || count_rows(tx, IMMUTABLE_TABLES)? != 0 {
            return Err(Fault::Store("stored workflow data requires its configured policy".into()));
        }
        return Ok(());
    };
    let actor = RecordedActor {
        principal: PrincipalRef::TrustedHostOperator,
        provenance: ActorProvenance::TrustedHost { path: TrustedHostPath::ControllerMaintenance },
    };
    let context = Context { tx, actor: &actor, operation_id: "startup-validation",
        product_revision: revision, now_ms, limits };
    ensure_capacity(&context, 0, 0)?;
    super::collaboration_questions::validate_stored(tx, limits)?;
    super::collaboration_work::validate_stored(tx, limits)?;
    super::collaboration_documents::validate_stored(&context.query())?;
    super::collaboration_reviews::validate_stored(&context.query())?;
    super::collaboration_notifications::validate_stored(tx)?;
    Ok(())
}

pub(crate) fn product_revision(tx: &StoreContext<'_>) -> Result<u64> {
    // TEXT substr stops at embedded NUL in SQLite. Read a bounded BLOB and
    // check the complete stored byte length/type before parsing any prefix.
    let row: Option<(String, i64, Vec<u8>)> = tx.query_row(
        "SELECT typeof(value),length(CAST(value AS BLOB)),substr(CAST(value AS BLOB),1,20) FROM settings WHERE name='workflow_revision'",
        [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).optional()?;
    let (kind, length, bytes) = row.ok_or_else(|| Fault::Store("workflow product revision is absent".into()))?;
    if kind != "text" || !(1..=19).contains(&length) || bytes.len() != length as usize {
        return Err(Fault::Store("invalid workflow product revision representation".into()));
    }
    let raw = std::str::from_utf8(&bytes).map_err(|_| Fault::Store("invalid workflow product revision bytes".into()))?;
    let n: u64 = raw.parse().map_err(|_| Fault::Store("invalid workflow product revision".into()))?;
    if n > i64::MAX as u64 || n.to_string() != raw {
        return Err(Fault::Store("noncanonical workflow product revision".into()));
    }
    Ok(n)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewCursor { kind: String, viewer: PrincipalRef, through: u64, revision: u64, id: String }
fn review_cursor(q: &QueryContext<'_, '_>, value: &ReviewCursor) -> Result<String> {
    let raw = encode(value)?;
    if raw.len() > q.tx.limits.controller.collaboration_cursor_bytes {
        return refuse("response_bound", "Configured limit exceeded: controller.collaboration_cursor_bytes");
    }
    Ok(raw)
}
pub(crate) fn pending_reviews(q: &QueryContext<'_, '_>, viewer: &PrincipalRef, after: Option<&str>, limit: usize) -> Result<Page<ReviewView>> {
    validate_direct_principal(q.tx, viewer)?;
    if q.actor.principal != PrincipalRef::TrustedHostOperator && q.actor.principal != *viewer {
        return refuse("recipient_mismatch", "agent review queue is bound to the current caller");
    }
    if limit == 0 { return refuse("invalid_input", "review page limit must be positive"); }
    if limit > q.limits.page_items { return refuse("input_bound", "Configured limit exceeded: workflow.page_items"); }
    let head = product_revision(q.tx)?;
    let mut c = match after {
        None => ReviewCursor { kind: "pending_reviews".into(), viewer: viewer.clone(), through: head, revision: 0, id: String::new() },
        Some(raw) => {
            if raw.len() > q.tx.limits.controller.collaboration_cursor_bytes {
                return refuse("input_bound", "Configured limit exceeded: controller.collaboration_cursor_bytes");
            }
            let c: ReviewCursor = decode(raw).map_err(|_| Fault::Refused("invalid_input", "invalid review cursor".into()))?;
            identity(q.tx, &c.id)?;
            if c.kind != "pending_reviews" || c.viewer != *viewer || c.revision == 0 || c.revision > c.through || c.through > head {
                return refuse("invalid_input", "review cursor differs from query scope");
            }
            c
        }
    };
    let bound = limit.checked_add(1).ok_or_else(|| Fault::Refused("input_bound", "workflow.page_items overflow".into()))?;
    let rows = super::collaboration_reviews::pending_review_rows(q, viewer,
        after.map(|_| (c.revision, c.id.clone())), c.through, bound)?;
    let more = rows.len() > limit;
    let mut page = Page { items: Vec::new(), next_cursor: None };
    let mut previous = None;
    for item in rows.into_iter().take(limit) {
        let DecisionQueueItem::Review { review_id, created_product_revision, .. } = item else {
            return Err(Fault::Store("review-only query returned another domain".into()));
        };
        let view = super::collaboration_reviews::review_view(q, super::collaboration_reviews::get_review(q, &review_id)?)?;
        c.revision = created_product_revision;
        c.id = review_id;
        page.items.push(view);
        page.next_cursor = Some(review_cursor(q, &c)?);
        if super::bounded_json_size(&page, q.limits.page_bytes).is_err() {
            page.items.pop();
            if page.items.is_empty() { return refuse("response_bound", "Configured limit exceeded: workflow.page_bytes"); }
            page.next_cursor = previous;
            return Ok(page);
        }
        previous = page.next_cursor.clone();
    }
    page.next_cursor = if more { previous } else { None };
    super::bounded_json_size(&page, q.limits.page_bytes)
        .map_err(|_| Fault::Refused("response_bound", "Configured limit exceeded: workflow.page_bytes".into()))?;
    Ok(page)
}

mod content_identity;
pub(crate) use content_identity::content_identity;

#[cfg(test)]
#[path = "collaboration_workflow_helpers_tests.rs"]
mod helpers_tests;
