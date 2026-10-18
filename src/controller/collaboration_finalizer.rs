//! Canonical workflow occurrences inside the existing controller transaction.
//! No transaction, receipt, revision counter, scheduler or delivery store owner.
use super::database::{self, integer, load, refuse, unsigned, Fault, Result, StoreContext};
use super::collaboration_workflow::{self as workflow, Context, QueryContext};
use crate::collaboration::workflow::*;
use crate::limits::WorkflowLimits;
use crate::protocol::{AttemptPhase, ExecutionProfile, Operation, Task, TaskStatus};
use rusqlite::params;
use std::{cell::RefCell, collections::{BTreeSet, VecDeque}, rc::Rc};

/// One bounded, transient buffer per SQL request/maintenance transaction. All
/// accepted-policy views share it; a new SQL transaction always creates a new
/// buffer. This is not durable pending delivery and never survives rollback.
#[derive(Clone)]
pub(super) struct TerminalBuffer(Rc<RefCell<Vec<(String, u64, TaskStatus)>>>);
impl TerminalBuffer {
    pub(super) fn new() -> Self { Self(Rc::new(RefCell::new(Vec::new()))) }
    pub(super) fn clear(&self) { self.0.borrow_mut().clear(); }
    pub(super) fn record(&self, tx: &StoreContext<'_>, task: &Task) -> Result<()> {
        let Some(limits) = tx.workflow_policy else { return Ok(()) };
        let mut pending = self.0.borrow_mut();
        if pending.iter().any(|(id, _, _)| id == &task.id) {
            return Err(Fault::Store("duplicate first terminal transition in one transaction".into()));
        }
        if pending.len() >= limits.events_per_mutation {
            return refuse("capacity", "terminal changes exceed workflow.events_per_mutation");
        }
        pending.push((task.id.clone(), task.revision, task.status));
        Ok(())
    }
    fn take(&self) -> Vec<(String, u64, TaskStatus)> { std::mem::take(&mut *self.0.borrow_mut()) }
}

fn occurrence(detail: &ActivityDetail) -> Option<ResolverOutcome> {
    match detail {
        ActivityDetail::Task(TaskChange::Closed { resolution }) => Some(ResolverOutcome::Task(resolution.clone())),
        ActivityDetail::Question(QuestionChange::Closed { resolution }) => Some(ResolverOutcome::Question(resolution.clone())),
        ActivityDetail::Review(ReviewChange::Closed { resolution }) => Some(ResolverOutcome::Review(resolution.clone())),
        _ => None,
    }
}
fn resolver_subject(outcome: &ResolverOutcome) -> RecordRef {
    match outcome {
        ResolverOutcome::Task(t) => RecordRef::Task { task_id: t.task_id.clone() },
        ResolverOutcome::Question(q) => RecordRef::Question { question_id: match q {
            QuestionResolution::Answered { question_id, .. } |
            QuestionResolution::Cancelled { question_id, .. } |
            QuestionResolution::Superseded { question_id, .. } => question_id.clone(),
        } },
        ResolverOutcome::Review(r) => RecordRef::Review { review_id: match r {
            ReviewResolution::Decided { review_id, .. } |
            ReviewResolution::Withdrawn { review_id, .. } => review_id.clone(),
        } },
    }
}
fn truncated(detail: &ActivityDetail) -> bool {
    match detail {
        ActivityDetail::Question(QuestionChange::Posed { prompt, .. }) => prompt.truncated,
        ActivityDetail::Question(QuestionChange::AnswerReplaced { reason, .. } |
            QuestionChange::AnswerWithdrawn { reason, .. } | QuestionChange::MarkedMoot { reason, .. }) => reason.truncated,
        ActivityDetail::Document(DocumentChange::Created { title, body, .. } |
            DocumentChange::VersionAdded { title, body, .. }) => title.truncated || body.truncated,
        ActivityDetail::Document(DocumentChange::CommentAdded { body, .. }) => body.truncated,
        ActivityDetail::Review(ReviewChange::Requested { summary, .. }) => summary.truncated,
        ActivityDetail::Wait(WaitChange::Removed { reason, .. }) => reason.truncated,
        _ => false,
    }
}
fn scope(task: &Task) -> DeliveryScope {
    DeliveryScope { task_id: task.id.clone(), session_id: task.session_id.clone(), agent_id: task.assigned_agent_id.clone() }
}
fn add_scope(targets: &mut Vec<DeliveryScope>, value: DeliveryScope, limits: &WorkflowLimits) -> Result<()> {
    if !targets.contains(&value) { targets.push(value); }
    if targets.len() > limits.page_items { return refuse("capacity", "workflow destinations exceed workflow.page_items"); }
    Ok(())
}
fn eligible(q: &QueryContext<'_, '_>, task: &Task) -> Result<bool> {
    if task.status != TaskStatus::Active
        || !matches!(task.input.definition.execution_profile, Some(ExecutionProfile::CollaborationV2 { .. }))
        || task.input.resolved_limits.as_ref().is_none_or(|s| s.global_metadata.workflow.is_none()) {
        return Ok(false);
    }
    let Some(a) = &task.attempt else { return Ok(false) };
    if a.phase != AttemptPhase::Running || a.stopped || a.uncertain
        || q.now_ms >= a.deadline_unix_ms || q.now_ms >= task.input.limits.deadline_unix_ms {
        return Ok(false);
    }
    let caller = crate::mcp::Caller { task_id: task.id.clone(), attempt_id: a.id.clone(),
        generation: a.generation, worker_id: a.worker_id.clone(), incarnation: a.worker_incarnation.clone() };
    match super::collaboration_calls::caller(q.tx, &caller, q.now_ms) {
        Ok(_) => Ok(true),
        Err(Fault::Refused(_, _)) => Ok(false),
        Err(e) => Err(e),
    }
}
fn principal_targets(q: &QueryContext<'_, '_>, principal: &PrincipalRef) -> Result<Vec<DeliveryScope>> {
    workflow::validate_direct_principal(q.tx, principal)?;
    let PrincipalRef::Agent { agent_id } = principal else { return Ok(vec![]) };
    let mut statement = q.tx.prepare("SELECT id FROM tasks WHERE agent=?1 AND status='active' ORDER BY id LIMIT ?2")?;
    let ids = statement.query_map(params![agent_id, super::database::sql_bound(q.limits.graph_scan_nodes.saturating_add(1))?], |r| r.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if ids.len() > q.limits.graph_scan_nodes { return refuse("capacity", "recipient scan exceeds workflow.graph_scan_nodes"); }
    let mut targets = vec![];
    for id in ids {
        let task = load(q.tx, &id)?.task;
        if eligible(q, &task)? { add_scope(&mut targets, scope(&task), q.limits)?; }
    }
    Ok(targets)
}

/// Alternatives for ONE retained routing obligation, not one copy per owner
/// session. Delivery selects/fences an exact Begin and discharges only its ACK.
/// Empty candidates leave the immutable intent owed; they do not mean delivered.

// Traceability scope: Prefer the exact eligible active asker; otherwise resolve the linked record current responsible owner.
//= spec/impl/v2/implementation.md#committed-resolutions-reach-their-consumers
//# An answer delivery MUST target its active asker or otherwise the record's current responsible owner.

pub(super) fn resolution_targets(q: &QueryContext<'_, '_>, routing: &ResolutionRouting) -> Result<Vec<DeliveryScope>> {
    workflow::validate_recorded_actor(q.tx, &routing.requested_by)?;
    if let ActorProvenance::BoundAgentSession { task_id, session_id, attempt_id, generation, worker_id, incarnation, .. } = &routing.requested_by.provenance {
        let task = load(q.tx, task_id)?.task;
        if task.session_id == *session_id && task.attempt.as_ref().is_some_and(|a|
            a.id == *attempt_id && a.generation == *generation && a.worker_id == *worker_id && a.worker_incarnation == *incarnation)
            && eligible(q, &task)? {
            return Ok(vec![scope(&task)]);
        }
    }
    let owner = workflow::record_owner(q.tx, &routing.linked_record)?;
    principal_targets(q, &owner)
}

fn ordinary_targets(ctx: &Context<'_, '_>, subject: &RecordRef, detail: &ActivityDetail) -> Result<Vec<DeliveryScope>> {
    // Resolution audits remain queryable, but never repeat the same resolved
    // outcome as another executor input beside the original terminal event.
    if matches!(detail, ActivityDetail::Wait(WaitChange::Resolved { .. })) { return Ok(vec![]); }
    let recipient = match detail {
        ActivityDetail::Question(QuestionChange::Posed { recipient, .. }) => recipient.clone(),
        ActivityDetail::Review(ReviewChange::Requested { reviewer, .. }) => reviewer.clone(),
        _ => workflow::record_owner(ctx.tx, subject)?,
    };
    principal_targets(&ctx.query(), &recipient)
}

/// Finish one domain change in the original receipt transaction. The root calls
/// this before advancing workflow_revision. A later Task-terminal flush can use
/// the same pending revision; SQL ordinals and all limits are aggregate.
// Traceability scope: Question/review effects resolve recorded waits and stage activity; ordinary continuation admission retains accepted inputs and does not grant access.
//= spec/impl/v2/implementation.md#collaboration-surfaces-preserve-existing-authority
//# A product question or review decision MUST NOT grant host, repository, provider or credential access.

pub(crate) fn finish(ctx: &Context<'_, '_>, effects: &[WorkflowEffect]) -> Result<()> {
    if effects.is_empty() { return Ok(()) }
    if workflow::product_revision(ctx.tx)?.checked_add(1) != Some(ctx.product_revision)
        || ctx.product_revision > i64::MAX as u64 {
        return Err(Fault::Store("workflow finalizer revision is not the pending transaction revision".into()));
    }
    workflow::validate_recorded_actor(ctx.tx, ctx.actor)?;
    if effects.len() > ctx.limits.events_per_mutation.saturating_mul(2) {
        return refuse("capacity", "workflow effects exceed workflow.events_per_mutation");
    }
    // One exact typed Closed occurrence and one Resolve; malformed domain
    // effects cannot synthesize a second occurrence or silently drop a closure.
    for effect in effects {
        if let WorkflowEffect::Resolve { resolution, .. } = effect {
            let count = effects.iter().filter(|e| matches!(e, WorkflowEffect::Activity { subject, detail, .. }
                if occurrence(detail).as_ref() == Some(resolution) && *subject == resolver_subject(resolution))).count();
            if count != 1 { return Err(Fault::Store("resolver has no unique exact terminal activity".into())); }
        }
        if let WorkflowEffect::Activity { subject, detail, .. } = effect {
            if let Some(outcome) = occurrence(detail) {
                let count = effects.iter().filter(|e| matches!(e, WorkflowEffect::Resolve { resolution, .. } if *resolution == outcome)).count();
                if count != 1 || *subject != resolver_subject(&outcome) {
                    return Err(Fault::Store("terminal activity has no unique exact resolver effect".into()));
                }
            }
        }
    }
    let database_id: String = ctx.tx.query_row("SELECT value FROM settings WHERE name='database_id'", [], |r| r.get(0))?;
    let (count, maximum): (i64, Option<i64>) = ctx.tx.query_row(
        "SELECT count(*),max(ordinal) FROM workflow_events WHERE product_revision=?1", [integer(ctx.product_revision)?], |r| Ok((r.get(0)?, r.get(1)?)))?;
    let mut ordinal = usize::try_from(unsigned(count)?).map_err(|_| Fault::Store("event count exceeds address space".into()))?;
    if maximum != if ordinal == 0 { None } else { Some(i64::try_from(ordinal - 1).map_err(|_| Fault::Store("ordinal exceeds durable range".into()))?) } {
        return Err(Fault::Store("workflow transaction event ordinals are not contiguous".into()));
    }
    let mut queue: VecDeque<WorkflowEffect> = effects.iter().filter(|e| matches!(e, WorkflowEffect::Activity { .. })).cloned().collect();
    while let Some(effect) = queue.pop_front() {
        if ordinal >= ctx.limits.events_per_mutation { return refuse("capacity", "workflow events exceed workflow.events_per_mutation"); }
        let WorkflowEffect::Activity { subject, subject_revision, detail } = effect else {
            return Err(Fault::Store("wait resolution emitted unsupported nested effect".into()));
        };
        let id = EventId { database_id: database_id.clone(), product_revision: ctx.product_revision,
            ordinal: u32::try_from(ordinal).map_err(|_| Fault::Refused("capacity", "workflow event ordinal exhausted".into()))? };
        let mut targets = vec![];
        let mut routing = None;
        if let Some(outcome) = occurrence(&detail) {
            let (requested_by, linked_record) = effects.iter().find_map(|e| match e {
                WorkflowEffect::Resolve { resolution, requested_by, linked_record } if *resolution == outcome => Some((requested_by, linked_record)), _ => None,
            }).ok_or_else(|| Fault::Store("terminal resolver pairing was lost".into()))?;
            let intent = ResolutionRouting { requested_by: requested_by.clone(), linked_record: linked_record.clone() };
            // Validate current routing now, but do not turn candidates into
            // permanent extra destinations. C selects one at actual Begin.
            let _ = resolution_targets(&ctx.query(), &intent)?;
            routing = Some(intent);

            // Traceability scope: One canonical terminal occurrence closes matching waits and stages delivery in the owner transaction.
            //= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
            //# A resolver's terminal transition MUST commit its typed outcome, affected wait resolutions and delivery associations atomically.

            let resolved = super::collaboration_work::resolve(ctx, &outcome, &id)?;
            if ordinal.checked_add(queue.len()).and_then(|n| n.checked_add(resolved.effects.len())).and_then(|n| n.checked_add(1))
                .is_none_or(|n| n > ctx.limits.events_per_mutation) {
                return refuse("capacity", "workflow cascade exceeds workflow.events_per_mutation");
            }
            for wait in resolved.outcome {
                let task = load(ctx.tx, &wait.wait.task_id)?.task;
                if !database::terminal(task.status) { add_scope(&mut targets, scope(&task), ctx.limits)?; }
            }
            queue.extend(resolved.effects);
        } else {
            targets = ordinary_targets(ctx, &subject, &detail)?;
        }
        targets.sort_by(|a, b| (&a.task_id, &a.session_id, &a.agent_id).cmp(&(&b.task_id, &b.session_id, &b.agent_id)));
        workflow::ensure_capacity(ctx, 0, 1)?;
        let event = WorkflowActivity { id, subject, subject_revision, actor: ctx.actor.clone(), truncated: truncated(&detail), detail, routing };
        super::collaboration_notifications::stage_event(ctx.tx, &event, &targets)?;
        ordinal += 1;
    }
    workflow::ensure_capacity(ctx, 0, 0)
}

fn bound_actor(tx: &StoreContext<'_>, caller: &crate::mcp::Caller) -> Result<RecordedActor> {
    let raw:String=tx.query_row("SELECT body FROM attempts WHERE id=?1 AND task_id=?2",params![caller.attempt_id,caller.task_id],|r|r.get(0))?;
    let attempt:crate::protocol::Attempt=database::decode(&raw)?;
    let task = super::sessions::historical_task(tx,&caller.task_id,&attempt.session_id)?;
    let actor = RecordedActor { principal: PrincipalRef::Agent { agent_id: task.assigned_agent_id.clone() },
        provenance: ActorProvenance::BoundAgentSession { agent_id: task.assigned_agent_id, session_id: task.session_id,
            task_id: caller.task_id.clone(), attempt_id: caller.attempt_id.clone(), generation: caller.generation,
            worker_id: caller.worker_id.clone(), incarnation: caller.incarnation.clone() } };
    workflow::validate_recorded_actor(tx, &actor)?;
    Ok(actor)
}
fn maintenance_actor() -> RecordedActor {
    RecordedActor { principal: PrincipalRef::TrustedHostOperator,
        provenance: ActorProvenance::TrustedHost { path: TrustedHostPath::ControllerMaintenance } }
}
/// Called only after the existing operation has succeeded and validated its
/// current worker/report or MCP actor. Historical validation below preserves
/// that attribution after the action itself stopped the original attempt.
fn successful_actor(tx: &StoreContext<'_>, operation: &Operation) -> Result<RecordedActor> {
    match operation {
        Operation::CallCollaboration { call } => bound_actor(tx, &call.caller),
        Operation::ObserveWorker { report } => bound_actor(tx, &crate::mcp::Caller {
            task_id: report.task_id.clone(), attempt_id: report.attempt_id.clone(), generation: report.generation,
            worker_id: report.worker_id.clone(), incarnation: report.incarnation.clone() }),
        Operation::RegisterWorker { .. } | Operation::AwaitCommands { .. } => Ok(maintenance_actor()),
        _ => Ok(RecordedActor { principal: PrincipalRef::TrustedHostOperator,
            provenance: ActorProvenance::TrustedHost { path: tx.invocation_path } }),
    }
}
/// One immutable canonical outcome for the exact first terminal transition.
/// Existing history and the caller's transaction own durability; there is no
/// separate store or counter. A failed cascade/COMMIT rolls this row back too.
fn record_task_resolution(ctx: &Context<'_, '_>, resolution: &TaskResolution) -> Result<()> {
    super::bounded_json_size(resolution, ctx.limits.event_bytes)
        .map_err(|_| Fault::Refused("capacity", "task terminal occurrence exceeds workflow.event_bytes".into()))?;
    let exists: bool = ctx.tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM history WHERE task_id=?1 AND revision=?2 AND kind='workflow_task_closed')",
        params![resolution.task_id, integer(resolution.terminal_task_revision)?], |row| row.get(0))?;
    if exists { return Err(Fault::Store("duplicate canonical task terminal occurrence".into())); }
    ctx.tx.execute(
        "INSERT INTO history(task_id,revision,kind,body) VALUES(?1,?2,'workflow_task_closed',?3)",
        params![resolution.task_id, integer(resolution.terminal_task_revision)?, database::encode(resolution)?])?;
    Ok(())
}

/// Startup consumers compare to the original outcome, never a mutable retried
/// task head. All fields (status, exact accepted-result occurrence and summary)
/// belong to the immutable original transaction. No writes or repair occur here.
pub(super) fn validate_task_resolution(tx: &StoreContext<'_>, resolution: &TaskResolution) -> Result<()> {
    let limits = tx.workflow_policy.ok_or_else(|| Fault::Store("task terminal occurrence lacks workflow policy".into()))?;
    if !database::valid_id(tx.limits, &resolution.task_id)
        || resolution.terminal_task_revision == 0 || resolution.terminal_task_revision > i64::MAX as u64 {
        return Err(Fault::Store("invalid task terminal occurrence identity".into()));
    }
    let mut statement = tx.prepare(
        "SELECT CAST(substr(CAST(body AS BLOB),1,?3) AS TEXT) FROM history WHERE task_id=?1 AND revision=?2 AND kind='workflow_task_closed' ORDER BY seq LIMIT 2")?;
    let rows = statement.query_map(
        params![resolution.task_id, integer(resolution.terminal_task_revision)?, super::database::sql_bound(limits.event_bytes.saturating_add(1))?],
        |row| row.get::<_, String>(0))?.collect::<std::result::Result<Vec<_>, _>>()?;
    if rows.len() != 1 || rows[0].len() > limits.event_bytes {
        return Err(Fault::Store("task terminal occurrence missing, duplicated or over bound".into()));
    }
    let original: TaskResolution = database::decode(&rows[0])?;
    if original != *resolution {
        return Err(Fault::Store("task resolution differs from original terminal occurrence".into()));
    }
    Ok(())
}

fn terminal_effects(ctx: &Context<'_, '_>) -> Result<Vec<WorkflowEffect>> {
    let pending = ctx.tx.workflow_terminals.take();
    let mut effects = Vec::new();
    let mut seen = BTreeSet::new();
    for (id, revision, first_status) in pending {
        if !seen.insert(id.clone()) { return Err(Fault::Store("duplicate buffered terminal task".into())); }
        let task = load(ctx.tx, &id)?.task;
        if !database::terminal(task.status) || task.status != first_status || task.revision < revision {
            return Err(Fault::Store("buffered terminal task no longer matches committed action".into()));
        }
        let status = match task.status {
            TaskStatus::Succeeded => TerminalTaskStatus::Succeeded,
            TaskStatus::Failed => TerminalTaskStatus::Failed,
            TaskStatus::Cancelled => TerminalTaskStatus::Cancelled,
            _ => unreachable!(),
        };
        let accepted_result = if task.status == TaskStatus::Succeeded {
            let target = workflow::accepted_target(&ctx.query(), &id, crate::collaboration::documents::AcceptedReviewScope::Result)?;
            let ReviewTarget::AcceptedTaskResult { result } = target else { return Err(Fault::Store("accepted result target differs".into())) };
            if result.terminal_task_revision != revision { return Err(Fault::Store("terminal event differs from accepted history occurrence".into())); }
            Some(result)
        } else { None };
        let summary = task.result.as_ref().map(|r| r.summary.clone()).or(task.blocked_reason.clone()).unwrap_or_default();
        let resolution = TaskResolution { task_id: id.clone(), terminal_task_revision: revision, status, accepted_result, summary };
        record_task_resolution(ctx, &resolution)?;
        let subject = RecordRef::Task { task_id: id };
        effects.push(WorkflowEffect::Activity { subject: subject.clone(), subject_revision: revision,
            detail: ActivityDetail::Task(TaskChange::Closed { resolution: resolution.clone() }) });
        effects.push(WorkflowEffect::Resolve { resolution: ResolverOutcome::Task(resolution),
            requested_by: ctx.actor.clone(), linked_record: subject });
    }
    Ok(effects)
}

// Traceability scope: Only committed collaboration effects consume the next workflow revision; receipt-only changes do not.
//= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
//# A committed collaboration change MUST receive the next collaboration revision in commit order.

fn flush(tx: &StoreContext<'_>, actor: &RecordedActor, operation_id: &str, now_ms: u64) -> Result<bool> {
    let Some(limits) = tx.workflow_policy else { return Ok(false) };
    if !database::valid_id(tx.limits, operation_id) {
        return refuse("capacity", "workflow action attribution exceeds transport.identity_bytes");
    }
    let before = workflow::product_revision(tx)?;
    // Exhausted workflow identity must not refuse an unrelated old operation
    // that produced no domain effect or terminal transition.
    if before == i64::MAX as u64 && tx.workflow_terminals.0.borrow().is_empty() { return Ok(false) }
    let revision = before.checked_add(1).filter(|r| *r <= i64::MAX as u64)
        .ok_or_else(|| Fault::Refused("capacity", "workflow revision exhausted".into()))?;
    let ctx = Context { tx, actor, operation_id, product_revision: revision, now_ms, limits };
    let effects = terminal_effects(&ctx)?;
    finish(&ctx, &effects)?;
    let changed: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM workflow_events WHERE product_revision=?1)", [integer(revision)?], |r| r.get(0))?;
    if changed {
        workflow::ensure_capacity(&ctx, 0, 0)?;
        if tx.execute("UPDATE settings SET value=?2 WHERE name='workflow_revision' AND value=?1", params![before.to_string(), revision.to_string()])? != 1 {
            return Err(Fault::Store("workflow revision changed outside finalizer".into()));
        }
    }
    Ok(changed)
}
pub(super) fn finalize_request(tx: &StoreContext<'_>, operation: &Operation, now_ms: u64) -> Result<bool> {
    if tx.workflow_policy.is_none() { return Ok(false) }
    let Some(operation_id) = database::operation_identity(operation) else { return Ok(false) };
    flush(tx, &successful_actor(tx, operation)?, operation_id, now_ms)
}
pub(super) fn finalize_expiry(tx: &StoreContext<'_>, now_ms: u64) -> Result<bool> {
    if tx.workflow_policy.is_none() { return Ok(false) }
    // Attribution within the existing maintenance transaction, not a new
    // request/receipt or caller-supplied operation. The revision cannot repeat.
    let revision = workflow::product_revision(tx)?.checked_add(1)
        .ok_or_else(|| Fault::Refused("capacity", "workflow revision exhausted".into()))?;
    flush(tx, &maintenance_actor(), &format!("workflow-expiry-{revision}"), now_ms)
}
