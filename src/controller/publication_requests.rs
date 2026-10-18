//! Publication admission uses original receipts, commands and effect credits.
//! No provider I/O, source-attempt resumption or accepted-input rewrite occurs
//! in this controller transaction.
use super::{database::*, ControllerConfig};
use crate::{protocol::*, mcp::{Invocation, publication as p}, pull_request as pr};
use crate::collaboration::{documents::AcceptedReviewScope, workflow::ReviewTarget};
use rusqlite::{params, OptionalExtension};
use serde_json::json;

/// Existing command flags durably reserve one delivery and one acknowledgment
/// receipt. They are separate from the requester's original Run/Stop credits.
pub(super) enum CommandCredit { Attempt(String), Publication }
pub(super) fn command_credits(tx: &StoreContext<'_>) -> Result<u64> {
    let n: i64 = tx.query_row("SELECT coalesce(sum((delivery_operation_id IS NULL)+(acknowledged=0)),0) FROM commands WHERE json_extract(body,'$.kind')='publish_repository'", [], |r| r.get(0))?;
    unsigned(n)
}
pub(super) fn is_command(tx: &StoreContext<'_>, id: &str) -> Result<bool> {
    tx.query_row("SELECT EXISTS(SELECT 1 FROM commands WHERE id=?1 AND json_extract(body,'$.kind')='publish_repository')", [id], |r| r.get(0)).map_err(Into::into)
}
pub(super) fn pending_delivery(tx: &StoreContext<'_>, worker: &str, incarnation: &str, cfg: &ControllerConfig) -> Result<Vec<WorkerCommand>> {
    let mut q = tx.prepare("SELECT body FROM commands WHERE worker=?1 AND incarnation=?2 AND acknowledged=0 AND json_extract(body,'$.kind')='publish_repository' ORDER BY seq LIMIT ?3")?;
    let raw = q.query_map(params![worker,incarnation,cfg.max_commands_per_reply as i64], |r|r.get::<_,String>(0))?.collect::<std::result::Result<Vec<_>,_>>()?;
    raw.iter().map(|s|decode(s)).collect()
}
pub(super) fn unacknowledged(tx: &StoreContext<'_>, report: &Report) -> Result<Vec<String>> {
    if !matches!(report.observation, Observation::ProcessTreeStopped) { return Ok(vec![]); }
    let mut q = tx.prepare("SELECT id FROM commands WHERE attempt_id=?1 AND acknowledged=0 AND json_extract(body,'$.kind')='publish_repository'")?;
    Ok(q.query_map([&report.attempt_id], |r| r.get(0))?.collect::<std::result::Result<Vec<_>,_>>()?)
}

pub(super) fn validate_definition(tx: &StoreContext<'_>, definition: &Definition) -> Result<()> {
    let Some(selection) = p::selection(definition.execution_profile.as_ref()) else { return Ok(()); };
    selection.validate(tx.limits).map_err(|e| Fault::Refused("publication_policy", e))?;
    let registration = super::repositories::revision(tx, &selection.registration())?;
    let target = registration.pull_request_target.as_ref().ok_or_else(|| Fault::Refused("publication_configuration", "registered publication target is absent".into()))?;
    if target.revision != selection.target_revision() { return refuse("publication_configuration", "target revision differs from accepted publication selection"); }
    pr::validate_target(target, &pr::Bounds::from_limits(tx.limits)).map_err(|e| Fault::Refused("publication_configuration", e.to_string()))
}

fn command(tx: &StoreContext<'_>, id: &str) -> Result<p::Command> {
    let raw: Option<String> = tx.query_row("SELECT body FROM commands WHERE id=?1", [id], |r| r.get(0)).optional()?;
    let Some(raw) = raw else { return refuse("not_found", "original publication command is absent"); };
    let WorkerCommand::PublishRepository { command_id, request } = decode(&raw)? else {
        return refuse("not_found", "identity is not a publication command");
    };
    if command_id != id { return Err(Fault::Store("publication command/index differs".into())); }
    request.validate(id).map_err(Fault::Store)?;
    Ok(*request)
}

fn accepted_source(tx: &StoreContext<'_>, source: &Task, actor: &crate::collaboration::workflow::RecordedActor, now: u64) -> Result<p::Source> {
    let limits = tx.limits.workflow.as_ref().ok_or_else(|| Fault::Refused("publication_policy", "accepted source proof policy is unavailable".into()))?;
    let q = super::collaboration_workflow::QueryContext { tx, actor, now_ms: now, limits };
    let ReviewTarget::AcceptedTaskCheckpoint { result: accepted_result } = super::collaboration_workflow::accepted_target(&q, &source.id, AcceptedReviewScope::Checkpoint)? else {
        return refuse("publication_source", "source has no accepted checkpoint proof");
    };
    if source.input.definition.executor != ExecutorKind::Coding || source.attempt.as_ref().is_none_or(|a| !a.stopped || a.uncertain)
        || source.result.as_ref().and_then(|r| r.checkpoint.as_ref()) != source.selected_checkpoint.as_ref() {
        return refuse("publication_source", "source execution or checkpoint is unresolved");
    }
    let checkpoint = accepted_result.checkpoint.as_ref().ok_or_else(|| Fault::Refused("publication_source", "source checkpoint missing".into()))?;
    let (original, _) = super::pull_request::original(tx, &checkpoint.operation_id)?;
    let repository = source.input.repository.clone().ok_or_else(|| Fault::Refused("publication_source", "source repository missing".into()))?;
    Ok(p::Source { task_id: source.id.clone(), session_id: source.session_id.clone(), task_revision: source.revision,
        accepted_result, result_sha256: p::digest(source.result.as_ref().unwrap()).map_err(Fault::Store)?,
        checkpoint_attempt_id: original.attempt_id, checkpoint_generation: original.generation, repository })
}

/// Historical source and requester validation; no current lease is borrowed
/// from the successful source, and no capability comes from a read binding.
pub(super) fn validate_head(tx: &StoreContext<'_>, head: &pr::HeadIntent) -> Result<()> {
    let binding = head.publication.as_deref().ok_or_else(|| Fault::Store("publication binding absent".into()))?;
    let original = command(tx, &binding.publication_id)?;
    if original.head != *head || original.create.is_some() { return refuse("publication_binding", "original publication command differs"); }
    let requester = super::sessions::historical_task(tx, &binding.requester.caller.task_id, &binding.requester.session_id)?;
    if requester.input != original.input || requester.session_id != original.session.id
        || p::selection(requester.input.definition.execution_profile.as_ref()) != Some(&binding.selection) {
        return refuse("publication_binding", "accepted requesting session differs");
    }
    validate_definition(tx, &requester.input.definition)?;
    let source = load(tx, &binding.source.task_id)?.task;
    let actor = super::identity_administration::bound_actor(&requester, &binding.requester.caller);
    let mut expected = accepted_source(tx, &source, &actor, 0)?;
    if expected.task_revision < binding.source.task_revision { return refuse("publication_source", "source revision regressed"); }
    expected.task_revision = binding.source.task_revision;
    if expected != binding.source { return refuse("publication_source", "original accepted source proof differs"); }
    let permitted = requester.input.dependencies.contains(&source.id)
        || source.parent_task_id.as_ref() == Some(&requester.id) && source.status == TaskStatus::Succeeded;
    if !permitted { return refuse("publication_source", "source is not an accepted dependency or completed owned child"); }
    super::pull_request::checkpoint(tx, head)
}

pub(super) fn create_request(tx: &StoreContext<'_>, head: &pr::HeadIntent) -> Result<pr::Request> {
    let binding = head.publication.as_deref().ok_or_else(|| Fault::Store("publication binding absent".into()))?;
    let original = command(tx, &p::create_command_id(&binding.publication_id).map_err(Fault::Store)?)?;
    if original.head != *head || original.create.is_none() { return refuse("publication_binding", "original create command differs"); }
    match p::parse(&original.invocation.tool, &original.invocation.arguments).map_err(Fault::Store)? {
        p::Arguments::Open { title, body, .. } => Ok(pr::Request { title, body }),
        _ => refuse("publication_binding", "original create invocation differs"),
    }
}

fn effect(tx: &StoreContext<'_>, id: &str) -> Result<OperationRecord> { Ok(super::pull_request::original(tx, id)?.0) }
fn action_view(record: &OperationRecord, head: &pr::HeadIntent, limits: &crate::limits::RuntimeLimits) -> Result<p::ActionView> {
    let mut view = p::ActionView { state: p::State::Pending, refusal: None, head: None, pull_request: None };
    if record.state == "uncertain" { view.state = p::State::Unknown; return Ok(view); }
    if record.state == "pending" { return Ok(view); }
    if record.state != "settled" { return Err(Fault::Store("unknown publication effect state".into())); }
    let outcome = record.outcome.clone().ok_or_else(|| Fault::Store("publication settlement missing".into()))?;
    let b = pr::Bounds::from_limits(limits);
    if record.kind == pr::HEAD_KIND {
        let outcome: pr::HeadOutcome = serde_json::from_value(outcome)?;
        pr::validate_head_outcome(head, &outcome, &b).map_err(|e| Fault::Store(e.to_string()))?;
        match outcome {
            pr::HeadOutcome::Applied(s) => { view.state = p::State::Completed; view.head = Some(s.proof); },
            pr::HeadOutcome::NotApplied(s) => { view.state = p::State::Refused; view.refusal = s.refusal.or(Some(p::Refusal::NotDispatched)); },
        }
    } else {
        let intent: pr::Intent = serde_json::from_value(record.intended.clone())?;
        let outcome: pr::CreateOutcome = serde_json::from_value(outcome)?;
        match pr::validate_outcome(&intent, &outcome, &b).map_err(|e| Fault::Store(e.to_string()))? {
            Some(reference) => { view.state = p::State::Completed; view.pull_request = Some(reference); },
            None => { view.state = p::State::Refused; if let pr::CreateOutcome::NotApplied(s) = outcome { view.refusal = s.refusal.or(Some(p::Refusal::NotDispatched)); } },
        }
    }
    Ok(view)
}
fn view(tx: &StoreContext<'_>, request: &p::Command) -> Result<p::View> {
    let head = &request.head;
    let binding = head.publication.as_ref().ok_or_else(|| Fault::Store("publication binding absent".into()))?;
    let record = effect(tx, &head.head_operation_id)?;
    let create: Option<String> = tx.query_row("SELECT body FROM external_operations WHERE id=?1", [&head.binding.operation_id], |r| r.get(0)).optional()?;
    Ok(p::View { publication_id: binding.publication_id.clone(), source_task_id: binding.source.task_id.clone(),
        source_revision: binding.source.task_revision, source_attempt_id: binding.source.checkpoint_attempt_id.clone(),
        checkpoint_operation_id: head.binding.checkpoint.operation_id.clone(), registration: head.binding.registration.clone(),
        target_revision: head.target.revision.clone(), commit: head.binding.checkpoint.commit.clone(),
        branch: action_view(&record, head, tx.limits)?,
        pull_request: create.as_deref().map(decode::<OperationRecord>).transpose()?.as_ref().map(|r| action_view(r, head, tx.limits)).transpose()? })
}

pub(super) fn get(tx: &StoreContext<'_>, call: &Invocation) -> Result<(Data, bool)> {
    // Status is a retained original-fact read, including after cancellation or
    // uncertainty. It cannot prepare, dispatch, or reconcile an external effect.
    call.validate(tx.limits).map_err(|e| Fault::Refused("invalid_input", e))?;
    let p::Arguments::Get { publication_id } = p::parse(&call.tool, &call.arguments).map_err(|e| Fault::Refused("invalid_input", e))? else { return refuse("invalid_input", "publication read expected"); };
    let request = command(tx, &publication_id)?;
    if request.caller() != &call.caller { return refuse("not_found", "publication is not owned by caller"); }
    let limits = &request.input.resolved_limits.as_ref().ok_or_else(|| Fault::Store("publication policy absent".into()))?.global_metadata;
    p::validate(&call.tool, &call.arguments, limits, p::selection(request.input.definition.execution_profile.as_ref()))
        .map_err(|e| Fault::Refused("publication_policy", e))?;
    Ok((Data::Publication(Box::new(view(&tx.with_limits(limits), &request)?)), false))
}

pub(super) fn apply(tx: &StoreContext<'_>, call: &Invocation, cfg: &ControllerConfig, now: u64) -> Result<(Data, bool)> {
    let requester = super::collaboration_calls::caller(tx, &call.caller, now)?.task;
    let selected = p::selection(requester.input.definition.execution_profile.as_ref());
    p::validate(&call.tool, &call.arguments, tx.limits, selected).map_err(|e| Fault::Refused("publication_policy", e))?;
    let selected = selected.ok_or_else(|| Fault::Refused("publication_policy", "publication capability absent".into()))?;
    validate_definition(tx, &requester.input.definition)?;
    let args = p::parse(&call.tool, &call.arguments).map_err(|e| Fault::Refused("invalid_input", e))?;
    let (id, head, create) = match args {
        p::Arguments::Publish { source_task_id, expected_source_revision, checkpoint_operation_id, repository_id, registration_revision, target_revision } => {
            if selected.registration() != (crate::repository_catalogue::Revision { repository_id, registration_revision }) || selected.target_revision() != target_revision {
                return refuse("publication_selection", "requested repository or target differs from accepted capability");
            }
            let source = load(tx, &source_task_id)?.task;
            if source.revision != expected_source_revision { return refuse("stale_revision", "source task revision changed"); }
            if !requester.input.dependencies.contains(&source.id) && source.parent_task_id.as_ref() != Some(&requester.id) {
                return refuse("publication_source", "source is not an accepted dependency or completed owned child");
            }
            let actor = super::identity_administration::bound_actor(&requester, &call.caller);
            let source = accepted_source(tx, &source, &actor, now)?;
            let cp = source.accepted_result.checkpoint.as_ref().unwrap();
            if cp.operation_id != checkpoint_operation_id || cp.registration != selected.registration() { return refuse("publication_source", "selected source checkpoint differs"); }
            // A new logical call cannot evade an earlier unknown effect by
            // assigning a fresh publication identity to the same source.
            let duplicate: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM commands WHERE json_extract(body,'$.kind')='publish_repository' AND json_extract(body,'$.request.head.publication.requester.session_id')=?1 AND json_extract(body,'$.request.head.binding.checkpoint.operation_id')=?2)", params![requester.session_id, checkpoint_operation_id], |r| r.get(0))?;
            if duplicate { return refuse("operation_conflict", "source already has an original publication; read its outcome"); }
            let target = super::repositories::revision(tx, &selected.registration())?.pull_request_target.ok_or_else(|| Fault::Refused("publication_configuration", "publication target absent".into()))?;
            let id = p::publication_id(&call.operation_id).map_err(Fault::Store)?;
            let binding = p::Binding { publication_id: id.clone(), admission_operation_id: call.operation_id.clone(),
                requester: p::Requester { caller: call.caller.clone(), session_id: requester.session_id.clone() }, source, selection: selected.clone() };
            let head = pr::prepare_publication_head(binding, target, &pr::Bounds::from_limits(tx.limits)).map_err(|e| Fault::Refused("publication_source", e.to_string()))?;
            (id, head, None)
        },
        p::Arguments::Open { publication_id, title, body } => {
            let original = command(tx, &publication_id)?;
            if original.caller() != &call.caller || p::selection(original.input.definition.execution_profile.as_ref()) != Some(selected) {
                return refuse("publication_selection", "publication belongs to another accepted requester");
            }
            let head = original.head;
            validate_head(tx, &head)?;
            let settled = effect(tx, &head.head_operation_id)?;
            let intent = pr::prepare_pull_request(&head, &settled, &pr::Request { title, body }, &pr::Bounds::from_limits(tx.limits))
                .map_err(|e| Fault::Refused("publication_pending", e.to_string()))?;
            (p::create_command_id(&publication_id).map_err(Fault::Store)?, head, Some(intent))
        },
        p::Arguments::Get { .. } => return get(tx, call),
    };
    if tx.query_row("SELECT EXISTS(SELECT 1 FROM commands WHERE id=?1)", [&id], |r| r.get::<_, bool>(0))? {
        return refuse("operation_conflict", "publication already has its original command");
    }
    let attempt = requester.attempt.clone().ok_or_else(|| Fault::Store("publication caller attempt missing".into()))?;
    let request = p::Command { invocation: call.clone(), session: super::sessions::get(tx, &requester.session_id)?,
        attempt: attempt.clone(), input: requester.input.clone(), head, create };
    request.validate(&id).map_err(|e| Fault::Refused("publication_binding", e))?;
    let command = WorkerCommand::PublishRepository { command_id: id.clone(), request: Box::new(request.clone()) };
    preflight(tx, cfg, &command)?;
    super::database::command(tx, &attempt.worker_id, &attempt.worker_incarnation, &attempt.task_id, &attempt.id, attempt.generation, &command, &id)?;
    let intended = request.effect(tx.limits).map_err(Fault::Store)?;
    prepare_original(tx, cfg, now, intended)?;
    history(tx, &requester, "publication_admitted", &json!({"command_id":id,"publication_id":request.head.publication.as_ref().unwrap().publication_id,"operation_id":call.operation_id}))?;
    Ok((Data::Publication(Box::new(view(tx, &request)?)), true))
}

fn preflight(tx: &StoreContext<'_>, cfg: &ControllerConfig, command: &WorkerCommand) -> Result<()> {
    let WorkerCommand::PublishRepository { request, .. } = command else { unreachable!() };
    let count: i64 = tx.query_row("SELECT count(*) FROM receipts WHERE cleanup=0", [], |r| r.get(0))?;
    if unsigned(count)?.checked_add(2).is_none_or(|n| n > cfg.max_receipts as u64) || !cleanup_available(tx, cfg, 3)? {
        return refuse("capacity", "publication needs original admission, prepare and settlement capacity");
    }
    let count: i64 = tx.query_row("SELECT count(*) FROM commands WHERE worker=?1 AND incarnation=?2", params![request.caller().worker_id, request.caller().incarnation], |r| r.get(0))?;
    if unsigned(count)? >= tx.limits.worker.command_records as u64 { return refuse("capacity", "publication exceeds worker.command_records"); }
    let await_request = Operation::AwaitCommands { operation_id: "x".repeat(tx.limits.transport.identity_bytes),
        worker_id: request.caller().worker_id.clone(), incarnation: request.caller().incarnation.clone(), acknowledged: vec![] };
    if !super::operations::delivery_fits(&[], command, cfg, &await_request) { return refuse("capacity", "publication command cannot fit original command reply"); }
    crate::worker::publication_requests::preflight(command).map_err(|e| Fault::Refused("capacity", e))?;
    Ok(())
}

fn prepare_original(tx: &StoreContext<'_>, cfg: &ControllerConfig, now: u64, record: OperationRecord) -> Result<()> {
    let op = Operation::PrepareOperation { operation_id: record.id.clone(), task_id: record.task_id.clone(),
        attempt_id: record.attempt_id.clone(), generation: record.generation, kind: record.kind.clone(), intended: record.intended.clone() };
    let (data, _) = super::operations::publication_effect(tx, &op, cfg, now)?;
    let outcome = Outcome::Ok { data };
    super::metadata::receipt_bound(&cfg.limits, &op, &outcome)?;
    let revision = super::metadata::advance(tx)?;
    tx.execute("INSERT INTO receipts(id,request,outcome,cleanup,controller_revision) VALUES(?1,?2,?3,0,?4)",
        params![record.id, encode(&op)?, encode(&outcome)?, integer(revision)?])?;
    Ok(())
}

pub(super) fn typed(record: &OperationRecord) -> bool {
    match record.kind.as_str() { pr::HEAD_KIND => record.intended.get("publication").is_some(),
        pr::CREATE_KIND => record.intended.get("head").is_some_and(|h| h.get("publication").is_some()), _ => false }
}

pub(super) fn validate_existing(tx: &StoreContext<'_>, cfg: &ControllerConfig, version: i64) -> Result<()> {
    // Strict typed inventory, including inactive definitions and original refused
    // calls. JSON strings in instructions never count as vocabulary markers.
    for sql in ["SELECT body FROM definitions", "SELECT definition FROM agents"] {
        let mut q = tx.prepare(sql)?;
        let mut rows = q.query([])?;
        while let Some(row) = rows.next()? {
            let d: Definition = decode(&row.get::<_, String>(0)?)?;
            if p::selection(d.execution_profile.as_ref()).is_some() {
                if version < 27 { return Err(Fault::Store("publication selection requires schema27".into())); }
                validate_definition(tx, &d)?;
            }
        }
    }
    let mut q = tx.prepare("SELECT id,body FROM commands ORDER BY seq")?;
    let mut rows = q.query([])?;
    let mut count = 0usize;
    while let Some(row) = rows.next()? {
        let id: String = row.get(0)?;
        let c: WorkerCommand = decode(&row.get::<_, String>(1)?)?;
        if let WorkerCommand::PublishRepository { command_id, request } = c {
            count += 1;
            if version < 27 || count > cfg.max_receipts || command_id != id { return Err(Fault::Store("publication command requires bounded schema27".into())); }
            request.validate(&id).map_err(Fault::Store)?;
            validate_head(tx, &request.head)?;
            let receipt = super::metadata::read(tx, &request.invocation.operation_id)?.ok_or_else(|| Fault::Store("publication admission receipt absent".into()))?;
            if !matches!(&receipt.request, Operation::CallCollaboration { call } if call == &request.invocation)
                || !matches!(&receipt.outcome, Outcome::Ok { data: Data::Publication(v) }
                    if v.publication_id == request.head.publication.as_ref().unwrap().publication_id
                    && if request.create.is_some() { v.pull_request.as_ref().is_some_and(|p|p.state==p::State::Pending) } else { v.branch.state==p::State::Pending }) {
                return Err(Fault::Store("publication admission receipt differs".into()));
            }
            let expected = request.effect(tx.limits).map_err(Fault::Store)?;
            let original = effect(tx, &expected.id)?;
            if original.intended != expected.intended || original.task_id != expected.task_id || original.attempt_id != expected.attempt_id || original.generation != expected.generation {
                return Err(Fault::Store("publication command effect differs".into()));
            }
        }
    }
    let mut q = tx.prepare("SELECT id FROM receipts ORDER BY controller_revision")?;
    let mut rows = q.query([])?;
    while let Some(row) = rows.next()? {
        // The ordinary reader validates compact read provenance before excluding
        // that non-publication outcome from this typed vocabulary inventory.
        let Some(record) = super::metadata::scan_inline(tx, &row.get::<_, String>(0)?)? else { continue; };
        if version < 27 && (requires_publication(&record.request) || outcome_requires_publication(&record.outcome)) {
            return Err(Fault::Store("publication vocabulary requires schema27".into()));
        }
    }
    Ok(())
}

pub(crate) fn requires_publication(op: &Operation) -> bool { crate::protocol::requires_publication(op) }
pub(crate) fn outcome_requires_publication(out: &Outcome) -> bool { crate::protocol::outcome_requires_publication(out) }
