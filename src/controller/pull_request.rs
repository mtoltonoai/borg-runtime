//! Controller joins for the two requested publication effects. The provider
//! values are evidence, never authority to choose a repository or a new attempt.
use super::{bounded_json_size, database::*};
use crate::{protocol::*, pull_request as pr};
use rusqlite::{OptionalExtension, params};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

fn checked<T>(result: pr::Result<T>) -> Result<T> {
    result.map_err(|e| Fault::Refused("pull_request_invalid", e.to_string()))
}
fn shape<T: DeserializeOwned>(value: &Value, bounds: &pr::Bounds) -> Result<T> {
    if bounded_json_size(value, bounds.record_bytes).is_err() {
        return refuse("capacity", "pull request exceeds controller.effect_value_bytes");
    }
    serde_json::from_value(value.clone()).map_err(|_| {
        Fault::Refused("pull_request_invalid", "invalid closed publication value".into())
    })
}
fn bounds(task: &Task) -> Result<pr::Bounds> {
    let snapshot = task.input.resolved_limits.as_ref().ok_or_else(|| {
        Fault::Refused("invalid_limits", "accepted publication limits missing".into())
    })?;
    Ok(pr::Bounds::from_limits(&snapshot.global_metadata))
}

pub(super) fn validate_input(input: &TaskInput, limits: &crate::limits::RuntimeLimits) -> Result<()> {
    let b = pr::Bounds::from_limits(limits);
    if input.pull_request.is_some() && input.definition.executor != ExecutorKind::Coding {
        return refuse("invalid_input", "only coding tasks may request a pull request");
    }
    checked(pr::validate_selection(input.pull_request.as_ref(),
        input.repository.as_ref().and_then(|r| r.pull_request_target.as_ref()), &b))?;
    if let Some(NativeWorkflow::CodingThenContinue { child } | NativeWorkflow::Reassign { target: child }) = input.native_workflow.as_deref() {
        if child.pull_request.is_some() && child.definition.executor != ExecutorKind::Coding {
            return refuse("invalid_input", "only a coding child may request a pull request");
        }
        checked(pr::validate_selection(child.pull_request.as_ref(),
            child.repository.pull_request_target.as_ref(), &b))?;
    }
    Ok(())
}

/// Retrieve the original committed intent and settlement, not a supplied record.
/// Existing checkpoint receipts remain their original representation.
fn original(tx: &StoreContext<'_>, id: &str) -> Result<(OperationRecord, Option<Operation>)> {
    let row: Option<(String, String, Option<String>)> = tx.query_row(
        "SELECT body,prepare,settlement FROM external_operations WHERE id=?1", [id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).optional()?;
    let Some((body, prepare, settlement)) = row else {
        return refuse("pull_request_unsettled", "original publication operation is absent");
    };
    let record: OperationRecord = decode(&body)?;
    let prepare: Operation = decode(&prepare)?;
    if !matches!(&prepare, Operation::PrepareOperation {
        operation_id, task_id, attempt_id, generation, kind, intended
    } if operation_id == id && operation_id == &record.id
        && task_id == &record.task_id && attempt_id == &record.attempt_id
        && generation == &record.generation && kind == &record.kind && intended == &record.intended) {
        return refuse("pull_request_invalid", "original publication prepare differs from record");
    }
    let settlement: Option<Operation> = settlement.as_deref().map(decode).transpose()?;
    if let Some(settlement) = &settlement {
        if record.state != "settled" || !matches!(settlement, Operation::SettleOperation {
            operation_id, task_id, attempt_id, generation, outcome, ..
        } if operation_id == id && task_id == &record.task_id && attempt_id == &record.attempt_id
            && generation == &record.generation && Some(outcome) == record.outcome.as_ref()) {
            return refuse("pull_request_invalid", "original publication settlement differs from record");
        }
    } else if record.state == "settled" {
        return refuse("pull_request_unsettled", "original publication settlement is absent");
    }
    Ok((record, settlement))
}

fn checkpoint(tx: &StoreContext<'_>, head: &pr::HeadIntent) -> Result<()> {
    let cp = &head.binding.checkpoint;
    let (record, settlement) = original(tx, &cp.operation_id)?;
    if record.kind != "checkpoint" || record.state != "settled"
        || record.task_id != head.binding.task_id || record.attempt_id != head.binding.attempt_id
        || record.generation != head.binding.generation
        || record.intended.get("registration") != Some(&json!(cp.registration))
        || record.intended.get("repository").and_then(Value::as_str) != Some(cp.repository.as_str())
        || record.intended.get("remote_ref").and_then(Value::as_str) != Some(cp.remote_ref.as_str())
        || record.intended.get("commit").and_then(Value::as_str) != Some(cp.commit.as_str())
        || !matches!(settlement, Some(Operation::SettleOperation {
            outcome, checkpoint: Some(ref selected), ..
        }) if selected == cp && outcome.get("acknowledged").and_then(Value::as_bool) == Some(true)
            && outcome.get("not_applied").and_then(Value::as_bool) != Some(true)) {
        return refuse("pull_request_unsettled", "exact original checkpoint is not acknowledged");
    }
    Ok(())
}

fn head_context(tx: &StoreContext<'_>, task: &Task, head: &pr::HeadIntent,
    require_selected: bool, b: &pr::Bounds) -> Result<()> {
    if task.input.definition.executor != ExecutorKind::Coding || task.input.pull_request.is_none()
        || task.input.repository.as_ref().and_then(|r| r.pull_request_target.as_ref()) != Some(&head.target) {
        return refuse("pull_request_invalid", "publication differs from accepted request/target");
    }
    // Late settlement may record the original fact after another checkpoint was
    // selected in this same attempt. It never selects that old checkpoint again.
    let mut subject = task.clone();
    if !require_selected { subject.selected_checkpoint = Some(head.binding.checkpoint.clone()); }
    checked(pr::validate_head_for_task(head, &subject, b))?;
    checkpoint(tx, head)
}

fn same_identity(record: &OperationRecord, expected: OperationRecord) -> Result<()> {
    if record.id != expected.id || record.task_id != expected.task_id
        || record.attempt_id != expected.attempt_id || record.generation != expected.generation
        || record.kind != expected.kind || record.intended != expected.intended {
        return refuse("pull_request_invalid", "publication identity differs from original typed intent");
    }
    Ok(())
}

fn create_context(tx: &StoreContext<'_>, task: &Task, record: &OperationRecord,
    require_selected: bool, b: &pr::Bounds) -> Result<pr::Intent> {
    let intent: pr::Intent = shape(&record.intended, b)?;
    same_identity(record, checked(pr::intent_record(&intent, b))?)?;
    head_context(tx, task, &intent.head, require_selected, b)?;
    let (head, _) = original(tx, &intent.head.head_operation_id)?;
    let request = task.input.pull_request.as_ref().ok_or_else(|| {
        Fault::Refused("pull_request_invalid", "accepted publication request missing".into())
    })?;
    let expected = checked(pr::prepare_pull_request(&intent.head, &head, request, b))?;
    if expected != intent {
        return refuse("pull_request_invalid", "publication differs from canonical head/accepted request");
    }
    Ok(intent)
}

pub(super) fn prepare(tx: &StoreContext<'_>, task: &Task, record: &OperationRecord) -> Result<()> {
    if !matches!(record.kind.as_str(), pr::HEAD_KIND | pr::CREATE_KIND) { return Ok(()); }
    let b = bounds(task)?;
    if record.kind == pr::HEAD_KIND {
        let head: pr::HeadIntent = shape(&record.intended, &b)?;
        same_identity(record, checked(pr::head_record(&head, &b))?)?;
        head_context(tx, task, &head, true, &b)?;
        // A different operation ID cannot bypass the original effect's unknown
        // outcome. Exact original Prepare replay is handled by the receipt layer.
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM external_operations WHERE json_extract(body,'$.task_id')=?1 AND json_extract(body,'$.attempt_id')=?2 AND json_extract(body,'$.kind')='pull_request_head')",
            params![task.id, record.attempt_id], |r| r.get(0))?;
        let used: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM external_operations WHERE id=?1)",
            [&head.binding.operation_id], |r| r.get(0))?;
        if exists || used {
            return refuse("operation_conflict", "attempt already has an original publication identity");
        }
    } else {
        create_context(tx, task, record, true, &b)?;
    }
    Ok(())
}

/// Called only for an acknowledged terminal fact. ACK-false observations retain
/// the existing uncertain state; a generic ACK-true is never sufficient here.
pub(super) fn settle(tx: &StoreContext<'_>, task: &Task, record: &OperationRecord,
    outcome: &Value) -> Result<()> {
    if !matches!(record.kind.as_str(), pr::HEAD_KIND | pr::CREATE_KIND) { return Ok(()); }
    // Before typed publication existed, this kind accepted arbitrary intents.
    // Preserve only an already committed original Prepare for an unrequested
    // task. New Prepare always passes the typed guard above; malformed new
    // requested intents cannot acquire this historical settlement path.
    if legacy(tx, task, record)? { return Ok(()); }
    let b = bounds(task)?;
    if record.kind == pr::HEAD_KIND {
        let head: pr::HeadIntent = shape(&record.intended, &b)?;
        same_identity(record, checked(pr::head_record(&head, &b))?)?;
        head_context(tx, task, &head, false, &b)?;
        checked(pr::validate_head_outcome(&head, &shape(outcome, &b)?, &b))?;
    } else {
        let intent = create_context(tx, task, record, false, &b)?;
        checked(pr::validate_outcome(&intent, &shape(outcome, &b)?, &b))?;
    }
    Ok(())
}

/// A requested result needs a concrete original settled PR, even when there are
/// zero pending effects. Failed task results do not call this success gate.
pub(super) fn accepted_reference(tx: &StoreContext<'_>, task: &Task) -> Result<Option<pr::ResultReference>> {
    if task.input.pull_request.is_none() { return Ok(None); }
    let a = task.attempt.as_ref().ok_or_else(|| Fault::Refused("stale_generation", "attempt absent".into()))?;
    let mut q = tx.prepare("SELECT id FROM external_operations WHERE json_extract(body,'$.task_id')=?1 AND json_extract(body,'$.attempt_id')=?2 AND json_extract(body,'$.kind')='pull_request' ORDER BY id LIMIT 2")?;
    let ids = q.query_map(params![task.id, a.id], |r| r.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if ids.len() != 1 {
        return refuse("pull_request_unsettled", "successful requested task needs one original settled pull request");
    }
    let (record, _) = original(tx, &ids[0])?;
    let b = bounds(task)?;
    create_context(tx, task, &record, true, &b)?;
    checked(pr::reference_from_record(&record, &b)).map(Some)
}

fn invalid(message: &str) -> Fault { Fault::Store(format!("retained publication: {message}")) }

/// Both directions matter: a surviving row must have its original receipt, and
/// a successful receipt must still have the row. A later state never replaces
/// the original pending Prepare result.
fn original_receipts(tx: &StoreContext<'_>, record: &OperationRecord) -> Result<()> {
    let (stored, settlement) = original(tx, &record.id)?;
    if stored != *record { return Err(invalid("record changed during validation")); }
    let prepared = super::metadata::read(tx, &record.id)?
        .ok_or_else(|| invalid("original Prepare receipt missing"))?;
    let expected = Operation::PrepareOperation { operation_id: record.id.clone(),
        task_id: record.task_id.clone(), attempt_id: record.attempt_id.clone(),
        generation: record.generation, kind: record.kind.clone(), intended: record.intended.clone() };
    let mut pending = record.clone(); pending.state = "pending".into(); pending.outcome = None;
    if encode(&prepared.request)? != encode(&expected)?
        || prepared.outcome != (Outcome::Ok { data: Data::Operation(pending) }) {
        return Err(invalid("original Prepare receipt differs"));
    }
    if let Some(settlement) = settlement {
        let id = settlement.mutation_id().ok_or_else(|| invalid("settlement identity missing"))?;
        let receipt = super::metadata::read(tx, id)?.ok_or_else(|| invalid("settlement receipt missing"))?;
        if encode(&receipt.request)? != encode(&settlement)?
            || receipt.outcome != (Outcome::Ok { data: Data::Operation(record.clone()) })
            || receipt.commit.controller_revision <= prepared.commit.controller_revision {
            return Err(invalid("original settlement receipt differs"));
        }
    }
    Ok(())
}

fn legacy(tx: &StoreContext<'_>, task: &Task, record: &OperationRecord) -> Result<bool> {
    if record.kind != pr::CREATE_KIND || task.input.pull_request.is_some() { return Ok(false); }
    original_receipts(tx, record)?;
    Ok(true)
}

fn selection(input: &TaskInput) -> Value {
    let child = match input.native_workflow.as_deref() {
        Some(NativeWorkflow::CodingThenContinue { child } | NativeWorkflow::Reassign { target: child }) =>
            json!([child.pull_request, child.repository.pull_request_target]),
        _ => Value::Null,
    };
    json!([input.pull_request, input.repository.as_ref().and_then(|r| r.pull_request_target.as_ref()), child])
}

fn has_selection(input: &TaskInput) -> bool {
    input.pull_request.is_some()
        || input.repository.as_ref().is_some_and(|r| r.pull_request_target.is_some())
        || matches!(input.native_workflow.as_deref(), Some(NativeWorkflow::CodingThenContinue { child } | NativeWorkflow::Reassign { target: child })
            if child.pull_request.is_some() || child.repository.pull_request_target.is_some())
}

fn retained_input(tx: &StoreContext<'_>, input: &TaskInput, version: i64) -> Result<()> {
    if !has_selection(input) { return Ok(()); }
    if version < 20 { return Err(invalid("typed selection precedes schema 20")); }
    let limits = &input.resolved_limits.as_ref().ok_or_else(|| invalid("accepted policy missing"))?.global_metadata;
    validate_input(input, limits)?;
    let accepted = tx.with_limits(limits);
    let check = |repo: &Repository| -> Result<()> {
        let binding = repo.registration.as_ref().ok_or_else(|| invalid("accepted registration missing"))?;
        let original = super::repositories::revision(&accepted, binding)?;
        if repo.url != original.source_endpoint || repo.checkpoint_remote != original.checkpoint_endpoint
            || repo.pull_request_target != original.pull_request_target {
            return Err(invalid("accepted immutable repository revision differs"));
        }
        Ok(())
    };
    if let Some(repo) = &input.repository { check(repo)?; }
    if let Some(NativeWorkflow::CodingThenContinue { child } | NativeWorkflow::Reassign { target: child }) = input.native_workflow.as_deref() {
        check(&child.repository)?;
    }
    Ok(())
}

/// Use the recorded original attempt, never the task's newer retry attempt.
fn historical_subject(tx: &StoreContext<'_>, record: &OperationRecord) -> Result<Task> {
    let mut task = load(tx, &record.task_id)?.task;
    let raw: String = tx.query_row("SELECT body FROM attempts WHERE id=?1 AND task_id=?2",
        params![record.attempt_id, record.task_id], |r| r.get(0))?;
    let attempt: Attempt = decode(&raw)?;
    task=super::sessions::historical_task(tx,&record.task_id,&attempt.session_id)?;
    if attempt.id != record.attempt_id || attempt.task_id != record.task_id
        || attempt.generation != record.generation || attempt.session_id != task.session_id {
        return Err(invalid("original attempt differs"));
    }
    task.attempt = Some(attempt);
    Ok(task)
}

fn retained_record(tx: &StoreContext<'_>, record: &OperationRecord, version: i64) -> Result<()> {
    if !matches!(record.kind.as_str(), pr::HEAD_KIND | pr::CREATE_KIND) { return Ok(()); }
    let task = historical_subject(tx, record)?;
    original_receipts(tx, record)?;
    if legacy(tx, &task, record)? { return Ok(()); }
    if version < 20 { return Err(invalid("typed effect precedes schema 20")); }
    retained_input(tx, &task.input, version)?;
    let b = bounds(&task)?;
    let head = if record.kind == pr::HEAD_KIND {
        let head: pr::HeadIntent = shape(&record.intended, &b)?;
        same_identity(record, checked(pr::head_record(&head, &b))?)?;
        head_context(tx, &task, &head, false, &b)?;
        head
    } else { create_context(tx, &task, record, false, &b)?.head };
    let (cp, _) = original(tx, &head.binding.checkpoint.operation_id)?;
    original_receipts(tx, &cp)?;
    match (record.state.as_str(), &record.outcome) {
        ("pending", None) => {},
        ("uncertain", Some(outcome)) if outcome.get("acknowledged").and_then(Value::as_bool) != Some(true) => {
            shape::<Value>(outcome, &b)?;
        },
        ("settled", Some(outcome)) => settle(tx, &task, record, outcome)?,
        _ => return Err(invalid("typed effect state/outcome differs")),
    }
    Ok(())
}

fn retained_task(tx: &StoreContext<'_>, task: &Task, version: i64) -> Result<()> {
    let current = super::sessions::historical_task(tx,&task.id,&task.session_id)?;
    if selection(&task.input) != selection(&current.input)
        || has_selection(&task.input) && task.input.resolved_limits != current.input.resolved_limits {
        return Err(invalid("original accepted selection differs"));
    }
    retained_input(tx, &task.input, version)?;
    if let Some(result) = &task.result {
        if result.pull_request.is_some() && (version < 20 || task.input.pull_request.is_none()) {
            return Err(invalid("unexpected typed result reference"));
        }
        if task.input.pull_request.is_some() {
            let expected = accepted_reference(tx, task)?;
            if result.pull_request != expected || task.attempt.as_ref().map(|a| a.id.as_str()) != Some(result.attempt_id.as_str())
                || result.checkpoint != task.selected_checkpoint || current.result.as_ref() != Some(result) {
                return Err(invalid("original requested result reference differs or is absent"));
            }
        }
    } else if task.status == TaskStatus::Succeeded && task.input.pull_request.is_some() {
        return Err(invalid("requested success has no retained result"));
    }
    if let Some(child) = &task.completed_child {
        if let Some(result) = &child.result {
            if result.pull_request.is_some() {
                let original = load(tx, &child.task_id)?.task;
                if version < 20 || original.result.as_ref() != Some(result) {
                    return Err(invalid("completed child publication result differs"));
                }
            }
        }
    }
    Ok(())
}

fn receipt_task(tx: &StoreContext<'_>, task: &Task, request: &Operation) -> Result<Task> {
    let mut subject = task.clone();
    if matches!(request, Operation::CallCollaboration { .. }) {
        let canonical = super::sessions::historical_task(tx,&task.id,&task.session_id)?;
        // Public absence is intentional redaction, not an absent accepted
        // authority. Compare the exact projection before borrowing authority
        // from the independently validated canonical admission record.
        if task.input != crate::pull_request_view::input(&canonical.input) {
            return Err(invalid("original public task projection differs"));
        }
        subject.input = canonical.input;
    }
    Ok(subject)
}

fn configured_admission(tx: &StoreContext<'_>, task: &Task, request: &Operation) -> Result<bool> {
    let Operation::CallCollaboration { call } = request else { return Ok(false); };
    if call.tool != "collab_task_delegate" { return Ok(false); }
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Args { instruction: String }
    let args: Args = serde_json::from_value(call.arguments.clone())?;
    let raw: String = tx.query_row("SELECT body FROM attempts WHERE id=?1 AND task_id=?2",
        params![call.caller.attempt_id, call.caller.task_id], |r| r.get(0))?;
    let attempt: Attempt = decode(&raw)?;
    let parent=super::sessions::historical_task(tx,&call.caller.task_id,&attempt.session_id)?;
    if attempt.id != call.caller.attempt_id || attempt.task_id != parent.id
        || attempt.session_id != parent.session_id || attempt.generation != call.caller.generation
        || attempt.worker_id != call.caller.worker_id || attempt.worker_incarnation != call.caller.incarnation
        || task.parent_task_id.as_ref() != Some(&parent.id) {
        return Err(invalid("original configured delegation caller differs"));
    }
    let mut expected = crate::delegation::coding_child(&parent.input, &args.instruction)
        .map_err(|_| invalid("original configured child differs"))?;
    let limits = &parent.input.resolved_limits.as_ref().ok_or_else(|| invalid("parent policy missing"))?.global_metadata;
    super::repositories::resolve_input(&tx.with_limits(limits), &mut expected, true)?;
    let wait: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM waits WHERE child=?1 AND parent=?2 AND parent_attempt=?3 AND parent_generation=?4 AND operation_id=?5)",
        params![task.id, parent.id, attempt.id, integer(attempt.generation)?, call.operation_id], |r| r.get(0))?;
    if task.input != expected || !wait { return Err(invalid("original configured child/wait differs")); }
    Ok(true)
}

/// Query-only startup qualification, after generic receipt/credit/repository
/// validation and before migrations or process recovery. No repair is attempted.
/// `decode` compares canonical bytes, including nested AcceptedResult, so unknown
/// fields cannot disappear through the historically permissive outer DTO.
pub(super) fn validate_existing(tx: &StoreContext<'_>, cfg: &super::ControllerConfig, version: i64) -> Result<()> {
    let bound = cfg.max_receipts.checked_add(cfg.max_cleanup_receipts)
        .and_then(|n| n.checked_add(cfg.max_tasks)).ok_or_else(|| invalid("inventory overflow"))?;
    let mut q = tx.prepare("SELECT id FROM tasks ORDER BY id")?;
    let mut rows = q.query([])?;
    let mut count = 0usize;
    let mut selected = BTreeSet::new();
    let mut accepted = BTreeSet::new();
    let mut admitted = BTreeSet::new();
    let mut completed = BTreeSet::new();
    let mut registered = BTreeSet::new();
    let mut effects = BTreeMap::new();
    let mut latest = BTreeMap::new();
    while let Some(row) = rows.next()? {
        count += 1; if count > cfg.max_tasks { return Err(invalid("task inventory exceeds bound")); }
        let task = load(tx, &row.get::<_, String>(0)?)?.task;
        retained_task(tx, &task, version)?;
        if has_selection(&super::sessions::initial_input(tx,&task.id)?) { selected.insert(task.id.clone()); }
        if task.result.as_ref().is_some_and(|r| r.pull_request.is_some()) { accepted.insert(task.id); }
    }
    let mut q = tx.prepare("SELECT body FROM external_operations ORDER BY id")?;
    let mut rows = q.query([])?; count = 0;
    while let Some(row) = rows.next()? {
        count += 1; if count > bound { return Err(invalid("effect inventory exceeds bound")); }
        let record: OperationRecord = decode(&row.get::<_, String>(0)?)?;
        retained_record(tx, &record, version)?;
        if matches!(record.kind.as_str(), pr::HEAD_KIND | pr::CREATE_KIND) { effects.insert(record.id.clone(), record); }
    }
    let mut q = tx.prepare("SELECT body FROM commands ORDER BY seq")?;
    let mut rows = q.query([])?; count = 0;
    while let Some(row) = rows.next()? {
        count += 1; if count > bound.saturating_mul(2) { return Err(invalid("command inventory exceeds bound")); }
        if let WorkerCommand::RunAttempt { attempt, task, .. } = decode::<WorkerCommand>(&row.get::<_, String>(0)?)? {
            retained_input(tx, &task, version)?;
            let current = super::sessions::historical_task(tx,&attempt.task_id,&attempt.session_id)?;
            if selection(&task) != selection(&current.input)
                || has_selection(&task) && task.resolved_limits != current.input.resolved_limits {
                return Err(invalid("retained command selection/policy differs"));
            }
        }
    }
    let mut q = tx.prepare("SELECT id FROM receipts ORDER BY controller_revision")?;
    let mut rows = q.query([])?; count = 0;
    while let Some(row) = rows.next()? {
        count += 1; if count > bound { return Err(invalid("receipt inventory exceeds bound")); }
        let receipt = super::metadata::read(tx, &row.get::<_, String>(0)?)?
            .ok_or_else(|| invalid("receipt disappeared"))?;
        if version < 20 {
            let typed = match &receipt.request {
                Operation::SubmitTask { input, .. } => has_selection(input),
                Operation::DelegateTask { child, .. } => has_selection(child),
                Operation::RegisterRepository { registration, .. } | Operation::ReviseRepository { registration, .. } => registration.pull_request_target.is_some(),
                Operation::PrepareOperation { kind, .. } => kind == pr::HEAD_KIND,
                _ => false,
            };
            if typed { return Err(invalid("typed request precedes schema 20")); }
        }
        let Outcome::Ok { data } = &receipt.outcome else { continue };
        match data {
            Data::Task(task) => {
                let task = receipt_task(tx, task, &receipt.request)?;
                retained_task(tx, &task, version)?;
                if has_selection(&task.input) && matches!(receipt.request,
                    Operation::SubmitTask { .. } | Operation::SubmitTaskDraft { .. } | Operation::DelegateTask { .. }) {
                    admitted.insert(task.id.clone());
                }
                if has_selection(&task.input) && configured_admission(tx, &task, &receipt.request)? {
                    admitted.insert(task.id.clone());
                }
                // ReconcileResult preserves its original eligible refusal; it
                // must not invent a second direct ObserveWorker receipt.
                let original_report = match &receipt.request {
                    Operation::ObserveWorker { report } => Some(report.clone()),
                    Operation::ReconcileResult(input) => Some(super::operations::reconciliation::original_report(tx, &input.original_report_id)?),
                    _ => None,
                };
                if let Some(report) = original_report {
                    if let Observation::ResultCandidate { checks_passed: true, summary, checkpoint, validation, output } = &report.observation {
                        if let Some(result) = &task.result {
                            if result.pull_request.is_some() {
                                let a = task.attempt.as_ref().ok_or_else(|| invalid("result original attempt missing"))?;
                                if report.task_id != task.id || report.attempt_id != result.attempt_id
                                    || report.attempt_id != a.id || report.generation != a.generation
                                    || report.worker_id != a.worker_id || report.incarnation != a.worker_incarnation
                                    || summary != &result.summary || checkpoint != &result.checkpoint
                                    || validation != &result.validation || output != &result.output {
                                    return Err(invalid("original result receipt differs"));
                                }
                                completed.insert(task.id.clone());
                            }
                        }
                    }
                }
            },
            Data::Operation(record) if matches!(record.kind.as_str(), pr::HEAD_KIND | pr::CREATE_KIND) => {
                let (current, _) = original(tx, &record.id)?;
                same_identity(&current, record.clone())?;
                retained_record(tx, &current, version)?;
                match &receipt.request {
                    Operation::PrepareOperation { operation_id, task_id, attempt_id, generation, kind, intended }
                        if operation_id == &record.id && task_id == &record.task_id && attempt_id == &record.attempt_id
                            && generation == &record.generation && kind == &record.kind && intended == &record.intended
                            && record.state == "pending" && record.outcome.is_none() => {},
                    Operation::SettleOperation { operation_id, task_id, attempt_id, generation, outcome, checkpoint, .. }
                        if operation_id == &record.id && task_id == &record.task_id && attempt_id == &record.attempt_id
                            && generation == &record.generation && checkpoint.is_none() && record.outcome.as_ref() == Some(outcome)
                        && (record.state == "settled" && current == *record
                            || record.state == "uncertain" && outcome.get("acknowledged").and_then(Value::as_bool) != Some(true)) => {},
                    _ => return Err(invalid("successful publication receipt differs")),
                }
                // Receipts are visited in their immutable commit order. Earlier
                // uncertain observations remain valid history, but only the
                // latest successful observation may be the current row.
                latest.insert(record.id.clone(), record.clone());
            },
            Data::Commands(commands) => for command in commands {
                if let WorkerCommand::RunAttempt { attempt, task, continuation, .. } = command {
                    retained_input(tx, task, version)?;
                    let current = super::sessions::historical_task(tx,&attempt.task_id,&attempt.session_id)?;
                    if selection(task) != selection(&current.input)
                        || has_selection(task) && task.resolved_limits != current.input.resolved_limits {
                        return Err(invalid("original command selection differs"));
                    }
                    if let Some(child) = continuation {
                        if child.result.as_ref().is_some_and(|r| r.pull_request.is_some())
                            && (version < 20 || child.result != load(tx, &child.task_id)?.task.result) {
                            return Err(invalid("original continuation publication differs"));
                        }
                    }
                }
            },
            _ => {},
        }
        if let Operation::RegisterRepository { registration, .. } | Operation::ReviseRepository { registration, .. } = &receipt.request {
            if registration.pull_request_target.is_some() && version < 20 {
                return Err(invalid("typed registration precedes schema 20"));
            }
            let original = super::repositories::revision(tx, &registration.binding())?;
            if original != *registration { return Err(invalid("original registration differs")); }
            if registration.pull_request_target.is_some() {
                registered.insert((registration.repository_id.clone(), registration.registration_revision.clone()));
            }
        }
    }
    if selected != admitted || accepted != completed {
        return Err(invalid("original admission/result receipt membership differs"));
    }
    if effects != latest { return Err(invalid("latest original effect observation differs or is absent")); }
    let mut q = tx.prepare("SELECT repository_id,revision FROM repository_revisions WHERE json_type(body,'$.pull_request_target') IS NOT NULL ORDER BY repository_id,revision")?;
    let mut rows = q.query([])?;
    let mut catalogue = BTreeSet::new();
    while let Some(row) = rows.next()? {
        if catalogue.len() >= tx.limits.repository.catalogue_revisions { return Err(invalid("catalogue inventory exceeds bound")); }
        catalogue.insert((row.get::<_, String>(0)?, row.get::<_, String>(1)?));
    }
    if catalogue != registered { return Err(invalid("original typed registration receipt membership differs")); }
    // A catalogue can have no accepted tasks yet; still reject typed fields on
    // an old marker rather than silently manufacturing their migration history.
    if version < 20 {
        let present: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM repository_revisions WHERE json_type(body,'$.pull_request_target') IS NOT NULL)", [], |r| r.get(0))?;
        if present { return Err(invalid("typed catalogue precedes schema 20")); }
    }
    Ok(())
}
