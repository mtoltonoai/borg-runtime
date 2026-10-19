//! Task coordination under the existing controller transaction and receipt owner.
//! No executor is started here; stop() records ordinary durable quiescence intent.
use super::database::{self, decode, encode, integer, load, next, refuse, valid_id, Fault, Result, StoreContext};
use super::collaboration_workflow::{self as workflow, Context, DomainChange, QueryContext};
use crate::collaboration::{Page, work::*, workflow::*};
use crate::limits::WorkflowLimits;
use crate::protocol::{Attempt, Task, TaskStatus};
use crate::mcp::Caller;
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    task_id: String,
    revision: u64,
    owner: PrincipalRef,
    /// Retained while waiting, and until the next genuinely admitted attempt.
    resume: Option<ResumeState>,
    changed_product_revision: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct InputAck { caller: Caller, context_binding: String }

fn record<T: Serialize>(limits: &WorkflowLimits, value: &T) -> Result<String> {
    let raw = encode(value)?;
    if raw.len() > limits.record_bytes {
        return refuse("capacity", "task coordination record exceeds workflow.record_bytes");
    }
    Ok(raw)
}
fn read<T: Serialize + serde::de::DeserializeOwned>(limits: &WorkflowLimits, raw: &str) -> Result<T> {
    if raw.len() > limits.record_bytes { return Err(Fault::Store("oversized task coordination record".into())); }
    decode(raw)
}
fn bump(revision: u64) -> Result<u64> {
    revision.checked_add(1).filter(|r| *r <= i64::MAX as u64)
        .ok_or_else(|| Fault::Refused("capacity", "task coordination revision exhausted".into()))
}
fn resolver_key(resolver: &ResolverRef) -> (&'static str, &str) {
    match resolver {
        ResolverRef::Task { task_id } => ("task", task_id),
        ResolverRef::Question { question_id } => ("question", question_id),
        ResolverRef::Review { review_id } => ("review", review_id),
    }
}
fn outcome_resolver(outcome: &ResolverOutcome) -> ResolverRef {
    match outcome {
        ResolverOutcome::Task(t) => ResolverRef::Task { task_id: t.task_id.clone() },
        ResolverOutcome::Question(q) => ResolverRef::Question { question_id: match q {
            QuestionResolution::Answered { question_id, .. } |
            QuestionResolution::Cancelled { question_id, .. } |
            QuestionResolution::Superseded { question_id, .. } => question_id.clone(),
        } },
        ResolverOutcome::Review(r) => ResolverRef::Review { review_id: match r {
            ReviewResolution::Decided { review_id, .. } |
            ReviewResolution::Withdrawn { review_id, .. } => review_id.clone(),
        } },
    }
}
fn header(tx: &StoreContext<'_>, task: &Task, limits: &WorkflowLimits) -> Result<Header> {
    let row: Option<(i64, String)> = tx.query_row(
        "SELECT revision,CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM workflow_task_state WHERE task_id=?1",
        params![task.id, super::database::sql_bound(limits.record_bytes.saturating_add(1))?], |r| Ok((r.get(0)?,r.get(1)?))).optional()?;
    let Some((revision, raw)) = row else {
        return Ok(Header { task_id: task.id.clone(), revision: 0,
            owner: PrincipalRef::Agent { agent_id: task.assigned_agent_id.clone() },
            resume: None, changed_product_revision: 0 });
    };
    let h: Header = read(limits, &raw)?;
    if h.task_id != task.id || h.revision == 0 || integer(h.revision)? != revision
        || h.changed_product_revision == 0 || h.resume.as_ref().is_some_and(|p|
            database::terminal(p.status) || p.assigned_agent_id != task.assigned_agent_id) {
        return Err(Fault::Store("task coordination header identity mismatch".into()));
    }
    workflow::validate_direct_principal(tx, &h.owner)?;
    Ok(h)
}
fn save_header(ctx: &Context<'_, '_>, previous: u64, h: &Header) -> Result<()> {
    let body = record(ctx.limits, h)?;
    if previous == 0 {
        workflow::ensure_capacity(ctx, 1, 0)?;
        ctx.tx.execute("INSERT INTO workflow_task_state(task_id,revision,body) VALUES(?1,?2,?3)",
            params![h.task_id, integer(h.revision)?, body])?;
    } else if ctx.tx.execute("UPDATE workflow_task_state SET revision=?3,body=?4 WHERE task_id=?1 AND revision=?2",
        params![h.task_id, integer(previous)?, integer(h.revision)?, body])? != 1 {
        return refuse("revision_conflict", "task coordination revision changed");
    }
    Ok(())
}
fn authorized(ctx: &Context<'_, '_>, h: &Header) -> Result<()> {
    if ctx.actor.principal != PrincipalRef::TrustedHostOperator && ctx.actor.principal != h.owner {
        return refuse("not_owner", "task coordination requires its current responsible owner");
    }
    Ok(())
}
fn expected(h: &Header, revision: u64) -> Result<()> {
    if h.revision != revision { return refuse("revision_conflict", "task coordination revision changed"); }
    Ok(())
}
fn open_count(tx: &StoreContext<'_>, task_id: &str) -> Result<usize> {
    let n: i64 = tx.query_row("SELECT count(*) FROM workflow_waits WHERE task_id=?1 AND closed=0", [task_id], |r| r.get(0))?;
    usize::try_from(n).map_err(|_| Fault::Store("invalid wait count".into()))
}

/// Called before every ordinary admission, even when operational workflow policy
/// is disabled. Existing durable waits must never become an implicit launch grant.
/// A retained wait-resume snapshot belongs to its original execution assignee.
/// Reassignment never rewrites that obligation into another agent's grant.
pub(super) fn handoff_blocked(tx:&StoreContext<'_>,task:&Task)->Result<bool> {
    if open_count(tx,&task.id)? != 0 {return Ok(true)}
    let Some(limits)=tx.limits.workflow.as_ref() else{return Ok(false)};
    Ok(header(tx,task,limits)?.resume.is_some())
}
pub(super) fn admission_blocked(tx: &StoreContext<'_>, task_id: &str) -> Result<bool> {
    Ok(open_count(tx, task_id)? != 0)
}

fn wait(tx: &StoreContext<'_>, id: &str, limits: &WorkflowLimits) -> Result<WorkflowWait> {
    let row: Option<(String,String,String,bool,String)> = tx.query_row(
        "SELECT task_id,resolver_kind,resolver_id,closed,CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM workflow_waits WHERE id=?1",
        params![id,super::database::sql_bound(limits.record_bytes.saturating_add(1))?], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
    let Some((task_id,kind,resolver_id,closed,raw)) = row else { return refuse("not_found", "workflow wait not found"); };
    let w: WorkflowWait = read(limits, &raw)?;
    let (expected_kind,expected_id) = resolver_key(&w.resolver);
    if w.reference.wait_id != id || w.reference.task_id != task_id || kind != expected_kind
        || resolver_id != expected_id || closed == matches!(w.status, WaitStatus::Open)
        || w.product_revision == 0 || !valid_id(tx.limits, id) || !valid_id(tx.limits, &w.operation_id)
        || !valid_id(tx.limits, &task_id) || !valid_id(tx.limits, &resolver_id) {
        return Err(Fault::Store("workflow wait index and body disagree".into()));
    }
    workflow::validate_recorded_actor(tx, &w.created_by)?;
    let closure: Option<String> = tx.query_row(
        "SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM workflow_wait_closures WHERE wait_id=?1",
        params![id,super::database::sql_bound(limits.record_bytes.saturating_add(1))?], |r| r.get(0)).optional()?;
    match &w.status {
        WaitStatus::Open if closure.is_none() => {},
        WaitStatus::Resolved { resolution, actor, operation_id } => {
            if resolution.wait != w.reference || resolution.resolver != w.resolver
                || outcome_resolver(&resolution.outcome) != w.resolver
                || resolution.event_id.product_revision < w.product_revision
                || !valid_id(tx.limits,&resolution.event_id.database_id)
                || !valid_id(tx.limits,operation_id) {
                return Err(Fault::Store("workflow wait resolution identity mismatch".into()));
            }
            workflow::validate_recorded_actor(tx,actor)?;
            if closure.as_deref() != Some(record(limits,&w.status)?.as_str()) {
                return Err(Fault::Store("workflow wait closure mismatch".into()));
            }
        },
        WaitStatus::Removed { reason, actor, operation_id, product_revision } => {
            if reason.trim().is_empty() || reason.len() > limits.text_bytes || reason.contains('\0')
                || !valid_id(tx.limits,operation_id) || *product_revision < w.product_revision {
                return Err(Fault::Store("invalid stored wait removal".into()));
            }
            workflow::validate_recorded_actor(tx,actor)?;
            if closure.as_deref() != Some(record(limits,&w.status)?.as_str()) {
                return Err(Fault::Store("workflow wait removal record mismatch".into()));
            }
        },
        _ => return Err(Fault::Store("workflow wait open/closed mismatch".into())),
    }
    Ok(w)
}
fn page(q: &QueryContext<'_, '_>, task_id: &str, after: Option<&str>, limit: usize) -> Result<Page<WorkflowWait>> {
    if limit == 0 || limit > q.limits.page_items || after.is_some_and(|v| !valid_id(q.tx.limits,v)) {
        return refuse("invalid_input", "invalid workflow wait page");
    }
    if let Some(cursor) = after {
        if wait(q.tx,cursor,q.limits)?.reference.task_id != task_id { return refuse("invalid_input","wait cursor belongs to another task"); }
    }
    let mut statement = q.tx.prepare("SELECT id FROM workflow_waits WHERE task_id=?1 AND (?2 IS NULL OR id>?2) ORDER BY id LIMIT ?3")?;
    let ids = statement.query_map(params![task_id,after,super::database::sql_bound(limit.saturating_add(1))?], |r| r.get::<_,String>(0))?
        .collect::<std::result::Result<Vec<_>,_>>()?;
    let mut out = Page { items: Vec::new(), next_cursor: None };
    for (index,id) in ids.iter().take(limit).enumerate() {
        out.items.push(wait(q.tx,id,q.limits)?);
        out.next_cursor=(index+1<ids.len()).then(||id.clone());
        if encode(&out)?.len()>q.limits.page_bytes {
            out.items.pop();
            let Some(last)=out.items.last() else { return refuse("capacity","one workflow wait exceeds workflow.page_bytes"); };
            out.next_cursor=Some(last.reference.wait_id.clone());
            break;
        }
    }
    Ok(out)
}
fn view(q: &QueryContext<'_, '_>, task_id: &str) -> Result<WorkView> {
    if !valid_id(q.tx.limits,task_id) { return refuse("invalid_input","invalid task identity"); }
    let task = load(q.tx,task_id)?.task;
    let h = header(q.tx,&task,q.limits)?;
    let open_waits = open_count(q.tx,task_id)?;
    if open_waits > q.limits.waits_per_task || (open_waits != 0 && h.resume.is_none()) {
        return Err(Fault::Store("workflow wait state exceeds or lacks retained state".into()));
    }

    // Traceability scope: The view preserves terminal status and otherwise restores the retained workflow state and assignee after the last wait.
    //= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
    //# Ending the last collaboration wait on a nonterminal task MUST restore its recorded workflow state and assignee.

    let state = if database::terminal(task.status) { WorkState::Terminal { status: task.status } }
        else if open_waits != 0 { WorkState::Waiting { prior: h.resume.clone().unwrap() } }
        else { WorkState::Runnable { state: h.resume.clone().unwrap_or_else(|| ResumeState {
            status: task.status, assigned_agent_id: task.assigned_agent_id.clone() }) } };
    Ok(WorkView { task: Box::new(task), coordination_revision:h.revision, responsible_owner:h.owner,
        state, open_waits, coordination_blocked:open_waits != 0,
        waits:page(q,task_id,None,q.limits.page_items)? })
}
pub(super) fn owner_revision(tx: &StoreContext<'_>, task_id: &str, limits: &WorkflowLimits) -> Result<RecordOwnerRevision> {
    let h = header(tx,&load(tx,task_id)?.task,limits)?;
    Ok(RecordOwnerRevision { owner:h.owner, revision:h.revision })
}
// Expose the existing typed, bounded Header marker; no duplicate header schema.
pub(super) fn changed_product_revision(tx: &StoreContext<'_>, task_id: &str, limits: &WorkflowLimits) -> Result<u64> {
    Ok(header(tx, &load(tx, task_id)?.task, limits)?.changed_product_revision)
}
pub(super) fn touch(ctx: &Context<'_, '_>, task_id: &str, revision: u64) -> Result<u64> {
    let mut h = header(ctx.tx,&load(ctx.tx,task_id)?.task,ctx.limits)?;
    expected(&h,revision)?;
    h.revision = bump(h.revision)?; h.changed_product_revision = ctx.product_revision;
    save_header(ctx,revision,&h)?;
    Ok(h.revision)
}


// Traceability scope: A supported resolver must name an existing open task, question or review.
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//# A collaboration wait MUST identify a concrete task, question or review resolver.
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//# A wait on a resolver that is already closed MUST be refused.

fn open_resolver(q: &QueryContext<'_, '_>, resolver: &ResolverRef) -> Result<()> {
    if !valid_id(q.tx.limits,resolver_key(resolver).1) { return refuse("invalid_input","invalid resolver identity"); }
    let open = match resolver {
        ResolverRef::Task { task_id } => !database::terminal(load(q.tx,task_id)?.task.status),
        ResolverRef::Question { question_id } => super::collaboration_questions::get(q.tx,question_id,q.limits.record_bytes)?.status == crate::collaboration::questions::QuestionStatus::Open,
        ResolverRef::Review { review_id } => super::collaboration_reviews::get_review(q,review_id)?.status == crate::collaboration::documents::ReviewStatus::Open,
    };
    if !open { return refuse("resolver_closed","a closed resolver cannot acquire a new wait"); }
    Ok(())
}
/// Direction is the dependency direction: parent -> child, waiter -> resolver.
/// Questions and reviews are leaves; their linked record is not a dependency.

// Traceability scope: The bounded dependency traversal includes existing waits and delegation parent links.
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//# A wait that would close a cycle through waits and parent links MUST be refused.

fn cycle(q: &QueryContext<'_, '_>, waiting: &str, resolver: &ResolverRef) -> Result<bool> {
    let ResolverRef::Task { task_id } = resolver else { return Ok(false); };
    let mut pending = BTreeSet::from([task_id.clone()]); let mut seen = BTreeSet::new();
    while let Some(id) = pending.pop_first() {
        if id == waiting { return Ok(true); }
        if !seen.insert(id.clone()) { continue; }
        if seen.len() > q.limits.graph_scan_nodes { return refuse("capacity","workflow graph exceeds workflow.graph_scan_nodes"); }
        let task = load(q.tx,&id)?.task;
        if database::terminal(task.status) { continue; }
        pending.extend(task.input.dependencies);
        if let Some(child) = task.waiting_child_id { pending.insert(child); }
        for sql in [
            "SELECT resolver_id FROM workflow_waits WHERE task_id=?1 AND closed=0 AND resolver_kind='task' ORDER BY resolver_id LIMIT ?2",
            "SELECT id FROM tasks WHERE json_extract(body,'$.task.parent_task_id')=?1 AND status IN ('queued','active') ORDER BY id LIMIT ?2",
        ] {
            let mut statement=q.tx.prepare(sql)?;
            let values=statement.query_map(params![id,super::database::sql_bound(q.limits.graph_scan_nodes.saturating_add(1))?],|r|r.get::<_,String>(0))?
                .collect::<std::result::Result<Vec<_>,_>>()?;
            if values.len()>q.limits.graph_scan_nodes { return refuse("capacity","workflow graph fanout exceeds configured bound"); }
            pending.extend(values);
        }
        if pending.len() > q.limits.graph_scan_nodes {
            return refuse("capacity","workflow graph pending traversal exceeds configured bound");
        }
    }
    Ok(false)
}
fn activity(task_id: &str, revision: u64, change: WaitChange) -> WorkflowEffect {
    WorkflowEffect::Activity { subject:RecordRef::Task { task_id:task_id.into() },
        subject_revision:revision, detail:ActivityDetail::Wait(change) }
}
fn close(ctx: &Context<'_, '_>, w: &WorkflowWait) -> Result<()> {
    workflow::ensure_capacity(ctx,0,1)?;
    ctx.tx.execute("INSERT INTO workflow_wait_closures(wait_id,body) VALUES(?1,?2)",
        params![w.reference.wait_id,record(ctx.limits,&w.status)?])?;
    if ctx.tx.execute("UPDATE workflow_waits SET closed=1,body=?2 WHERE id=?1 AND closed=0",
        params![w.reference.wait_id,record(ctx.limits,w)?])? != 1 {
        return refuse("revision_conflict","workflow wait is no longer open");
    }
    Ok(())
}

// Traceability scope: Last-wait release queues only an eligible nonterminal task; it does not revive a stopped attempt.
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//# Ending the last collaboration wait on a nonterminal task MUST restore its recorded workflow state and assignee.

fn released_task(ctx: &Context<'_, '_>, task_id: &str) -> Result<()> {
    if open_count(ctx.tx,task_id)? != 0 { return Ok(()); }
    let mut t=load(ctx.tx,task_id)?;
    if !database::terminal(t.task.status) && t.task.status != TaskStatus::Blocked
        && t.task.native_retry_block.is_none() {
        // A stopped attempt is never changed back to Running. Physical active
        // accounting remains true until ProcessTreeStopped acknowledges cleanup.
        t.task.status=TaskStatus::Queued;
        t.task.blocked_reason=Some("workflow_waits_ended".into());
        database::save(ctx.tx,&mut t,"workflow_waits_ended",&ctx.operation_id)?;
    }
    Ok(())
}
pub(super) fn apply(ctx: &Context<'_, '_>, mutation: &WorkMutation) -> Result<DomainChange<WorkMutationOutcome>> {
    let (task_id,revision)=match mutation {
        WorkMutation::AddWait { task_id,expected_revision,.. } |
        WorkMutation::RemoveWait { task_id,expected_revision,.. } |
        WorkMutation::ChangeOwner { task_id,expected_revision,.. } => (task_id,*expected_revision),
    };
    if !valid_id(ctx.tx.limits,task_id) { return refuse("invalid_input","invalid task identity"); }
    let mut t=load(ctx.tx,task_id)?;
    let mut h=header(ctx.tx,&t.task,ctx.limits)?;
    authorized(ctx,&h)?; expected(&h,revision)?;
    let mut effects=Vec::new(); let mut changed_wait=None;
    h.revision=bump(h.revision)?; h.changed_product_revision=ctx.product_revision;
    match mutation {
        WorkMutation::AddWait { resolver,.. } => {
            if database::terminal(t.task.status) { return refuse("invalid_transition","terminal task cannot begin a wait"); }
            if t.task.status == TaskStatus::Blocked || t.task.native_retry_block.is_some() {
                return refuse("invalid_transition","workflow wait cannot bypass a native retry block");
            }
            open_resolver(&ctx.query(),resolver)?;
            if cycle(&ctx.query(),task_id,resolver)? { return refuse("cycle","workflow wait would close a dependency cycle"); }
            let count=open_count(ctx.tx,task_id)?;
            if count>=ctx.limits.waits_per_task { return refuse("capacity","task exceeds workflow.waits_per_task"); }
            let (kind,id)=resolver_key(resolver);
            let duplicate:bool=ctx.tx.query_row("SELECT EXISTS(SELECT 1 FROM workflow_waits WHERE task_id=?1 AND resolver_kind=?2 AND resolver_id=?3 AND closed=0)",params![task_id,kind,id],|r|r.get(0))?;
            if duplicate { return refuse("already_waiting","task already waits for this resolver"); }

            // Traceability scope: Only the first open wait snapshots the original workflow state and assignee.
            //= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
            //# The first open collaboration wait MUST retain the task's prior workflow state and assignee.

            if count==0 && h.resume.is_none() { h.resume=Some(ResumeState { status:t.task.status,assigned_agent_id:t.task.assigned_agent_id.clone() }); }
            workflow::ensure_capacity(ctx,1,0)?;

            // Traceability scope: The task and generated wait identity are retained in the same canonical mutation.
            //= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
            //# Every collaboration record MUST have a durable identity.

            let wait_id=format!("workflow-wait-{}",next(ctx.tx,"workflow_wait")?);
            if !valid_id(ctx.tx.limits,&wait_id) { return refuse("capacity","generated wait identity exceeds identity bound"); }
            let w=WorkflowWait { reference:WaitRef {task_id:task_id.clone(),wait_id}, resolver:resolver.clone(),
                created_by:ctx.actor.clone(), operation_id:ctx.operation_id.into(), product_revision:ctx.product_revision,status:WaitStatus::Open };
            ctx.tx.execute("INSERT INTO workflow_waits(id,task_id,resolver_kind,resolver_id,closed,body) VALUES(?1,?2,?3,?4,0,?5)",
                params![w.reference.wait_id,task_id,kind,id,record(ctx.limits,&w)?])?;

            // Traceability scope: The wait and stop intent share the owner transaction; consumers act on its committed result.
            //= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
            //# A requested collaboration wait MUST be committed before initiating the current attempt's quiescence.

            // This durable wait and stop intent share the outer receipt commit.
            database::stop(ctx.tx,&mut t)?;
            t.task.status=TaskStatus::Queued;
            t.task.blocked_reason=Some("workflow_wait".into());
            database::save(ctx.tx,&mut t,"workflow_wait_added",&w.reference)?;
            effects.push(activity(task_id,h.revision,WaitChange::Added { wait:w.reference.clone(),resolver:resolver.clone() }));
            changed_wait=Some(w.reference);
        },
        WorkMutation::RemoveWait { wait_id,reason,.. } => {
            if reason.trim().is_empty() || reason.len()>ctx.limits.text_bytes || reason.contains('\0') { return refuse("invalid_input","invalid wait removal reason"); }
            let mut w=wait(ctx.tx,wait_id,ctx.limits)?;
            if w.reference.task_id != *task_id || !matches!(w.status,WaitStatus::Open) { return refuse("invalid_transition","wait is not open on this task"); }
            w.status=WaitStatus::Removed { reason:reason.clone(),actor:ctx.actor.clone(),operation_id:ctx.operation_id.into(),product_revision:ctx.product_revision };
            close(ctx,&w)?;
            released_task(ctx,task_id)?;
            effects.push(activity(task_id,h.revision,WaitChange::Removed {wait:w.reference.clone(),resolver:w.resolver.clone(),reason:BoundedText {text:reason.clone(),truncated:false}}));
            changed_wait=Some(w.reference);
        },

        // Traceability scope: Workflow responsible-owner changes do not assign the execution session or partition.
        //= spec/impl/v2/implementation.md#shared-product-state
        //# Task ownership MUST remain distinct from session and partition ownership.

        WorkMutation::ChangeOwner { owner,.. } => {
            workflow::validate_new_principal(ctx.tx,owner)?;
            if h.owner==*owner { return refuse("invalid_transition","responsible owner is unchanged"); }
            let previous=std::mem::replace(&mut h.owner,owner.clone());
            effects.push(WorkflowEffect::Activity { subject:RecordRef::Task {task_id:task_id.clone()},subject_revision:h.revision,
                detail:ActivityDetail::Task(TaskChange::OwnerChanged {previous,current:owner.clone()}) });
        },
    }
    save_header(ctx,revision,&h)?;
    Ok(DomainChange { outcome:WorkMutationOutcome {view:view(&ctx.query(),task_id)?,changed_wait,product_revision:ctx.product_revision},effects })
}
pub(super) fn query(q: &QueryContext<'_, '_>, query: &WorkQuery) -> Result<WorkQueryResult> {
    match query {
        WorkQuery::Get { task_id } => Ok(WorkQueryResult::Task(view(q,task_id)?)),
        WorkQuery::Waits { task_id,after,limit } => {
            load(q.tx,task_id)?;
            Ok(WorkQueryResult::Waits(page(q,task_id,after.as_deref(),*limit)?))
        },
    }
}

/// The common finalizer calls this with the exact canonical terminal occurrence
/// EventId it just allocated. Caller-supplied outcomes never reach this function.
/// All wait closures, logical restoration and returned delivery associations are
/// written under that original terminal operation's single transaction.
pub(super) fn resolve(ctx: &Context<'_, '_>, outcome: &ResolverOutcome, event_id: &EventId)
    -> Result<DomainChange<Vec<ResolvedWait>>> {
    if event_id.product_revision != ctx.product_revision || !valid_id(ctx.tx.limits,&event_id.database_id) {
        return Err(Fault::Store("resolution occurrence is not this product transaction".into()));
    }
    let resolver=outcome_resolver(outcome); let (kind,id)=resolver_key(&resolver);
    let mut statement=ctx.tx.prepare("SELECT id FROM workflow_waits WHERE resolver_kind=?1 AND resolver_id=?2 AND closed=0 ORDER BY task_id,id LIMIT ?3")?;
    let ids=statement.query_map(params![kind,id,super::database::sql_bound(ctx.limits.events_per_mutation.saturating_add(1))?],|r|r.get::<_,String>(0))?
        .collect::<std::result::Result<Vec<_>,_>>()?;
    if ids.len()>ctx.limits.events_per_mutation { return refuse("capacity","wait resolution exceeds workflow.events_per_mutation"); }
    let mut headers=BTreeMap::<String,(u64,Header)>::new();
    let mut values=Vec::new(); let mut effects=Vec::new();
    for id in ids {
        let mut w=wait(ctx.tx,&id,ctx.limits)?;
        if w.resolver != resolver { return Err(Fault::Store("indexed resolver differs".into())); }
        let task_id=w.reference.task_id.clone();
        if !headers.contains_key(&task_id) {
            let h=header(ctx.tx,&load(ctx.tx,&task_id)?.task,ctx.limits)?;
            headers.insert(task_id.clone(),(h.revision,h));
        }
        let (_,h)=headers.get_mut(&task_id).unwrap();
        h.revision=bump(h.revision)?; h.changed_product_revision=ctx.product_revision;

        // Traceability scope: The immutable closure keeps the terminal EventId and typed outcome; each unblock activity keeps that outcome.
        //= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
        //# A resolved wait MUST retain the exact terminal outcome occurrence that resolved it.
        //= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
        //# An unblock activity MUST carry the typed outcome of each wait it ends.

        let resolution=ResolvedWait {wait:w.reference.clone(),resolver:resolver.clone(),event_id:event_id.clone(),outcome:outcome.clone()};
        w.status=WaitStatus::Resolved {resolution:resolution.clone(),actor:ctx.actor.clone(),operation_id:ctx.operation_id.into()};
        close(ctx,&w)?;
        effects.push(activity(&task_id,h.revision,WaitChange::Resolved {wait:w.reference,resolver:resolver.clone(),outcome:outcome.clone()}));
        values.push(resolution);
    }
    for (task_id,(before,h)) in headers { save_header(ctx,before,&h)?; released_task(ctx,&task_id)?; }
    Ok(DomainChange {outcome:values,effects})
}

/// Exact durable associations for C's ordinary RunAttempt continuation. The
/// original event identity is reused; no second notice or implicit task exists.
pub(super) fn pending_resolution_refs(tx: &StoreContext<'_>, task_id: &str, limits: &WorkflowLimits) -> Result<Vec<ResolvedWait>> {
    // Non-null malformed ACK bytes must not silently suppress a resolution.
    consumption_rows(tx,task_id,None,limits)?;
    let mut statement=tx.prepare("SELECT w.id FROM workflow_waits w WHERE w.task_id=?1 AND w.closed=1 AND NOT EXISTS(SELECT 1 FROM workflow_wait_consumptions c WHERE c.wait_id=w.id AND c.input_ack IS NOT NULL) ORDER BY w.id LIMIT ?2")?;
    let ids=statement.query_map(params![task_id,super::database::sql_bound(limits.revisions.saturating_add(1))?],|r|r.get::<_,String>(0))?
        .collect::<std::result::Result<Vec<_>,_>>()?;
    if ids.len()>limits.revisions { return refuse("capacity","pending wait references exceed workflow.revisions"); }
    let mut out=Vec::new();
    for id in ids {
        if let WaitStatus::Resolved {resolution,..}=wait(tx,&id,limits)?.status { out.push(resolution); }
    }
    out.sort_by(|a,b|a.event_id.cmp(&b.event_id).then(a.wait.wait_id.cmp(&b.wait.wait_id)));
    if encode(&out)?.len()>limits.pending_delivery_bytes { return refuse("capacity","continuation exceeds workflow.pending_delivery_bytes"); }
    Ok(out)
}

/// Called only after the existing admission predicate has selected this exact
/// fresh attempt and command, in the same transaction that persists RunAttempt.
/// The worker's durable command retention remains C's acknowledgment boundary.
pub(super) fn admitted(tx: &StoreContext<'_>, task: &Task, attempt: &Attempt, continuation: Option<&WorkflowContinuation>, limits: &WorkflowLimits) -> Result<()> {
    if admission_blocked(tx,&task.id)? || database::terminal(task.status)
        || attempt.task_id!=task.id || attempt.session_id!=task.session_id || attempt.stopped || attempt.uncertain
        || task.attempt.as_ref().is_some_and(|old| !old.stopped || old.uncertain || old.id==attempt.id)
        || attempt.deadline_unix_ms!=task.input.limits.deadline_unix_ms {
        return Err(Fault::Store("workflow continuation bypassed ordinary fresh admission".into()));
    }
    let current=load(tx,&task.id)?.task;
    if current.status!=TaskStatus::Active || current.attempt.as_ref()!=Some(attempt)
        || current.id!=task.id || current.session_id!=task.session_id
        || current.assigned_agent_id!=task.assigned_agent_id || current.input!=task.input
        || task.revision.checked_add(1)!=Some(current.revision) {
        return Err(Fault::Store("workflow admission must follow its exact selected attempt in the same transaction".into()));
    }
    let expected=pending_resolution_refs(tx,&task.id,limits)?;
    match continuation {
        None if expected.is_empty()=>{},
        Some(c) if c.task_id==task.id && c.session_id==task.session_id && c.resolved_waits==expected=>{},
        _=>return Err(Fault::Store("admitted workflow continuation differs from durable resolutions".into())),
    }
    for r in expected {
        tx.execute("INSERT INTO workflow_wait_consumptions(wait_id,task_id,attempt_id,body,input_ack) VALUES(?1,?2,?3,?4,NULL)",
            params![r.wait.wait_id,task.id,attempt.id,record(limits,&r)?])?;
    }
    let mut h=header(tx,task,limits)?;
    if h.revision != 0 && h.resume.take().is_some() {
        // Physical admission is not a new logical owner/wait mutation or CAS.
        tx.execute("UPDATE workflow_task_state SET body=?2 WHERE task_id=?1 AND revision=?3",
            params![task.id,record(limits,&h)?,integer(h.revision)?])?;
    }
    Ok(())
}

fn bound_caller(tx: &StoreContext<'_>, caller: &Caller) -> Result<Attempt> {
    let body:String=tx.query_row("SELECT body FROM attempts WHERE id=?1 AND task_id=?2",params![caller.attempt_id,caller.task_id],|r|r.get(0))?;
    let attempt:Attempt=decode(&body)?;
    let task=super::sessions::historical_task(tx,&caller.task_id,&attempt.session_id)?;
    if attempt.id!=caller.attempt_id || attempt.task_id!=caller.task_id
        || attempt.generation!=caller.generation || attempt.worker_id!=caller.worker_id
        || attempt.worker_incarnation!=caller.incarnation || attempt.session_id!=task.session_id {
        return Err(Fault::Store("workflow input acknowledgment caller binding differs".into()));
    }
    Ok(attempt)
}
fn consumption_rows(tx: &StoreContext<'_>, task_id: &str, attempt_id: Option<&str>, limits: &WorkflowLimits)
    -> Result<Vec<(ResolvedWait,String,Option<InputAck>)>> {
    let mut statement=tx.prepare("SELECT wait_id,attempt_id,CAST(substr(CAST(body AS BLOB),1,?3) AS TEXT),CAST(substr(CAST(input_ack AS BLOB),1,?3) AS TEXT) FROM workflow_wait_consumptions WHERE task_id=?1 AND (?2 IS NULL OR attempt_id=?2) ORDER BY wait_id,attempt_id LIMIT ?4")?;
    let rows=statement.query_map(params![task_id,attempt_id,super::database::sql_bound(limits.record_bytes.saturating_add(1))?,super::database::sql_bound(limits.revisions.saturating_add(1))?],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,Option<String>>(3)?)))?
        .collect::<std::result::Result<Vec<_>,_>>()?;
    if rows.len()>limits.revisions { return Err(Fault::Store("workflow admission associations exceed bound".into())); }
    let mut out=Vec::new();
    for (id,attempt_id,body,ack) in rows {
        let resolution:ResolvedWait=read(limits,&body)?;
        if resolution.wait.task_id!=task_id || resolution.wait.wait_id!=id
            || !matches!(wait(tx,&id,limits)?.status,WaitStatus::Resolved {resolution:original,..} if original==resolution) {
            return Err(Fault::Store("workflow admitted resolution differs from original wait".into()));
        }
        let raw:String=tx.query_row("SELECT body FROM attempts WHERE id=?1 AND task_id=?2",params![attempt_id,task_id],|r|r.get(0))?;
        let attempt:Attempt=decode(&raw)?;
        if attempt.id!=attempt_id || attempt.task_id!=task_id { return Err(Fault::Store("workflow admitted attempt identity differs".into())); }
        let ack:Option<InputAck>=ack.as_deref().map(|raw|read(limits,raw)).transpose()?;
        if let Some(ack)=&ack {
            if ack.caller.task_id!=task_id || ack.caller.attempt_id!=attempt_id || !valid_id(tx.limits,&ack.context_binding) {
                return Err(Fault::Store("workflow input ACK identity differs".into()));
            }
            bound_caller(tx,&ack.caller)?;
        }
        out.push((resolution,attempt_id,ack));
    }
    Ok(out)
}
/// An admission association is deliberately distinct from an input ACK. Exact
/// unacknowledged refs can be associated with a later admitted attempt only after
/// root/C's existing no-unknown-Begin gate has admitted that attempt.
pub(super) fn admitted_continuation(tx: &StoreContext<'_>, task_id: &str, attempt_id: &str, limits: &WorkflowLimits) -> Result<Option<WorkflowContinuation>> {
    let task=load(tx,task_id)?.task;
    let mut refs=consumption_rows(tx,task_id,Some(attempt_id),limits)?.into_iter().filter_map(|(r,_,ack)|ack.is_none().then_some(r)).collect::<Vec<_>>();
    refs.sort_by(|a,b|a.event_id.cmp(&b.event_id).then(a.wait.wait_id.cmp(&b.wait.wait_id)));
    if refs.is_empty() { return Ok(None); }
    Ok(Some(WorkflowContinuation {task_id:task_id.into(),session_id:task.session_id,resolved_waits:refs}))
}
pub(super) fn acknowledged_event(tx: &StoreContext<'_>, task_id: &str, session_id: &str, event_id: &EventId, limits: &WorkflowLimits) -> Result<bool> {
    if super::sessions::get(tx,session_id)?.task_id!=task_id { return Err(Fault::Store("workflow ACK session differs".into())); }
    Ok(consumption_rows(tx,task_id,None,limits)?.iter().any(|(r,_,ack)|ack.is_some() && &r.event_id==event_id))
}
/// Called only after C has verified the exact canonical pending Begin caller and
/// context. The pending-batch ACK and these annotations commit atomically.
pub(super) fn acknowledge_continuation(tx: &StoreContext<'_>, caller: &Caller, context: &str, continuation: &WorkflowContinuation, limits: &WorkflowLimits) -> Result<()> {
    bound_caller(tx,caller)?;
    let mut refs=consumption_rows(tx,&caller.task_id,Some(&caller.attempt_id),limits)?.into_iter().map(|(r,_,_)|r).collect::<Vec<_>>();
    refs.sort_by(|a,b|a.event_id.cmp(&b.event_id).then(a.wait.wait_id.cmp(&b.wait.wait_id)));
    if !valid_id(tx.limits,context) || continuation.task_id!=caller.task_id
        || continuation.session_id!=load(tx,&caller.task_id)?.task.session_id || continuation.resolved_waits!=refs || refs.is_empty() {
        return refuse("delivery_pending","workflow continuation ACK is not its admitted exact input");
    }
    let expected=InputAck {caller:caller.clone(),context_binding:context.into()};
    let raw=record(limits,&expected)?;
    for (r,attempt_id,old) in consumption_rows(tx,&caller.task_id,Some(&caller.attempt_id),limits)? {
        if let Some(old)=old {
            if old!=expected { return refuse("delivery_pending","workflow continuation already has another input context"); }
        } else if tx.execute("UPDATE workflow_wait_consumptions SET input_ack=?3 WHERE wait_id=?1 AND attempt_id=?2 AND input_ack IS NULL",
            params![r.wait.wait_id,attempt_id,raw])?!=1 {
            return refuse("revision_conflict","workflow continuation ACK changed");
        }
    }
    Ok(())
}

fn validate_resolution_event(tx: &StoreContext<'_>, resolution: &ResolvedWait, limits: &WorkflowLimits) -> Result<()> {
    let database_id:String=tx.query_row("SELECT value FROM settings WHERE name='database_id'",[],|r|r.get(0))?;
    if resolution.event_id.database_id!=database_id { return Err(Fault::Store("wait resolution belongs to another database".into())); }
    let raw:String=tx.query_row("SELECT CAST(substr(CAST(body AS BLOB),1,?3) AS TEXT) FROM workflow_events WHERE product_revision=?1 AND ordinal=?2",
        params![integer(resolution.event_id.product_revision)?,i64::from(resolution.event_id.ordinal),super::database::sql_bound(limits.event_bytes.saturating_add(1))?],|r|r.get(0))?;
    if raw.len()>limits.event_bytes { return Err(Fault::Store("wait terminal event exceeds bound".into())); }
    let event:WorkflowActivity=decode(&raw)?;
    let matches=match (&resolution.outcome,&event.detail,&event.subject) {
        (ResolverOutcome::Task(value),ActivityDetail::Task(TaskChange::Closed {resolution:actual}),RecordRef::Task {task_id})=>value==actual && task_id==&value.task_id,
        (ResolverOutcome::Question(value),ActivityDetail::Question(QuestionChange::Closed {resolution:actual}),RecordRef::Question {question_id})=>value==actual && resolver_key(&resolution.resolver).1==question_id,
        (ResolverOutcome::Review(value),ActivityDetail::Review(ReviewChange::Closed {resolution:actual}),RecordRef::Review {review_id})=>value==actual && resolver_key(&resolution.resolver).1==review_id,
        _=>false,
    };
    if event.id!=resolution.event_id || !matches { return Err(Fault::Store("wait resolution differs from its exact terminal event".into())); }
    workflow::validate_recorded_actor(tx,&event.actor)?;
    Ok(())
}

pub(super) fn validate_stored(tx: &StoreContext<'_>, limits: &WorkflowLimits) -> Result<()> {
    for (table,key) in [("workflow_task_state","task_id"),("workflow_waits","id")] {
        let mut statement=tx.prepare(&format!("SELECT {key} FROM {table} ORDER BY {key} LIMIT ?1"))?;
        let ids=statement.query_map([super::database::sql_bound(limits.records.saturating_add(1))?],|r|r.get::<_,String>(0))?
            .collect::<std::result::Result<Vec<_>,_>>()?;
        if ids.len()>limits.records { return Err(Fault::Store("workflow record membership exceeds configured bound".into())); }
        for id in ids {
            if table=="workflow_task_state" { header(tx,&load(tx,&id)?.task,limits)?; }
            else {
                let w=wait(tx,&id,limits)?;
                let task=load(tx,&w.reference.task_id)?.task;
                let h=header(tx,&task,limits)?;
                let count=open_count(tx,&w.reference.task_id)?;
                if h.revision==0 || count>limits.waits_per_task || (count!=0 && h.resume.is_none()) { return Err(Fault::Store("stored task exceeds or lacks wait state".into())); }
                match &w.status {
                    WaitStatus::Resolved {resolution,..}=>validate_resolution_event(tx,resolution,limits)?,
                    WaitStatus::Open=>{
                        let q=QueryContext {tx,actor:&w.created_by,now_ms:0,limits};
                        open_resolver(&q,&w.resolver).map_err(|_|Fault::Store("open wait has a missing or closed resolver".into()))?;
                        if cycle(&q,&w.reference.task_id,&w.resolver)? { return Err(Fault::Store("stored workflow wait cycle".into())); }
                    },
                    WaitStatus::Removed {..}=>{},
                }
            }
        }
    }
    let orphan:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM workflow_wait_closures c LEFT JOIN workflow_waits w ON w.id=c.wait_id WHERE w.id IS NULL OR w.closed=0)",[],|r|r.get(0))?;
    if orphan { return Err(Fault::Store("orphan workflow wait closure".into())); }
    let mut statement=tx.prepare("SELECT wait_id,task_id,attempt_id,CAST(substr(CAST(body AS BLOB),1,?1) AS TEXT) FROM workflow_wait_consumptions ORDER BY wait_id LIMIT ?2")?;
    let rows=statement.query_map(params![super::database::sql_bound(limits.record_bytes.saturating_add(1))?,super::database::sql_bound(limits.revisions.saturating_add(1))?],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?)))?
        .collect::<std::result::Result<Vec<_>,_>>()?;
    if rows.len()>limits.revisions { return Err(Fault::Store("workflow consumption bound exceeded".into())); }
    for (id,task_id,attempt_id,raw) in rows {
        let value:ResolvedWait=read(limits,&raw)?;
        if value.wait.wait_id!=id || value.wait.task_id!=task_id || !valid_id(tx.limits,&attempt_id)
            || !matches!(wait(tx,&id,limits)?.status,WaitStatus::Resolved {resolution,..} if resolution==value) {
            return Err(Fault::Store("workflow continuation association mismatch".into()));
        }
        let attempt_body:String=tx.query_row("SELECT body FROM attempts WHERE id=?1 AND task_id=?2",params![attempt_id,task_id],|r|r.get(0))?;
        let attempt:Attempt=decode(&attempt_body)?;
        if attempt.id!=attempt_id || attempt.task_id!=task_id { return Err(Fault::Store("workflow continuation attempt mismatch".into())); }
        consumption_rows(tx,&task_id,Some(&attempt_id),limits)?;
    }
    Ok(())
}

/// Exact already-retained input acknowledgement for one original event. This
/// supplies attribution to the same-event routing discharge; it grants no new
/// caller authority and writes no row. Admission alone never appears here.
pub(super) fn acknowledged_event_input(
    tx: &StoreContext<'_>, task_id: &str, session_id: &str,
    event_id: &EventId, limits: &WorkflowLimits,
) -> Result<Option<(Caller, String)>> {
    if super::sessions::get(tx,session_id)?.task_id != task_id {
        return Err(Fault::Store("workflow ACK session differs".into()));
    }
    let mut exact: Option<(Caller, String)> = None;
    for (resolution, _, ack) in consumption_rows(tx, task_id, None, limits)? {
        if &resolution.event_id != event_id { continue; }
        if let Some(ack) = ack {
            let raw:String=tx.query_row("SELECT body FROM attempts WHERE id=?1 AND task_id=?2",params![ack.caller.attempt_id,task_id],|r|r.get(0))?;
            let attempt:Attempt=decode(&raw)?;
            if attempt.session_id!=session_id {continue}
            let retained = (ack.caller, ack.context_binding);
            if exact.as_ref().is_some_and(|previous| previous != &retained) {
                return Err(Fault::Store("workflow event has conflicting retained input ACKs".into()));
            }
            exact = Some(retained);
        }
    }
    Ok(exact)
}


/// Whole bounded canonical wait projection for completion admission. A page is
/// deliberately insufficient: an omitted open wait must never look complete.
/// Uses the same closed record/closure/event decoders as ordinary WORK startup.
pub(super) fn completion_waits(
    tx: &StoreContext<'_>, task_id: &str, limits: &WorkflowLimits, scan_rows: usize,
) -> Result<Vec<WorkflowWait>> {
    if scan_rows == 0 { return refuse("capacity", "completion wait scan has no allowance"); }
    let task = load(tx, task_id)?.task;
    header(tx, &task, limits)?;
    let mut statement = tx.prepare(
        "SELECT id FROM workflow_waits WHERE task_id=?1 ORDER BY id LIMIT ?2")?;
    let ids = statement.query_map(params![task_id, super::database::sql_bound(scan_rows.saturating_add(1))?],
        |row| row.get::<_, String>(0))?.collect::<std::result::Result<Vec<_>, _>>()?;
    if ids.len() > scan_rows { return refuse("capacity", "completion wait scan exceeds governance.scan_rows"); }
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        let value = wait(tx, &id, limits)?;
        if value.reference.task_id != task_id {
            return Err(Fault::Store("completion wait index differs from its original task".into()));
        }
        if let WaitStatus::Resolved { resolution, .. } = &value.status {
            validate_resolution_event(tx, resolution, limits)?;
        }
        out.push(value);
    }
    if out.iter().filter(|w| matches!(w.status, WaitStatus::Open)).count() != open_count(tx, task_id)? {
        return Err(Fault::Store("completion wait membership changed".into()));
    }
    Ok(out)
}
