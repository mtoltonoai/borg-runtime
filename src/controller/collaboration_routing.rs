//! Closed collaboration operations share the original controller request action.
//! Caller provenance is established here, never taken from a model argument.
use super::database::{refuse, Fault, Result, StoreContext};
use super::collaboration_workflow::{self as workflow, Context, DomainChange, QueryContext};
use crate::collaboration::workflow::*;
use crate::protocol::{Data, Operation};

//= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
//# An application-supplied actor label MUST NOT be recorded as authenticated-human identity.
//= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
//# The controller MUST derive a collaboration mutation's actor provenance from its invocation context.
pub(super) fn direct(tx: &StoreContext<'_>, op: &Operation, now: u64) -> Result<(Data, bool)> {
    let actor = RecordedActor {
        principal: PrincipalRef::TrustedHostOperator,
        provenance: ActorProvenance::TrustedHost { path: tx.invocation_path },
    };
    dispatch(tx, op, &actor, now)
}

//= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
//# A collaboration mutation MUST commit its record change and corresponding activity atomically.
fn complete<T>(ctx: &Context<'_, '_>, change: DomainChange<T>, wrap: fn(T) -> Data) -> Result<(Data, bool)> {
    super::collaboration_finalizer::finish(ctx, &change.effects)?;
    workflow::ensure_capacity(ctx, 0, 0)?;
    Ok((wrap(change.outcome), true))
}

fn dispatch(tx: &StoreContext<'_>, op: &Operation, actor: &RecordedActor, now: u64) -> Result<(Data, bool)> {
    let limits = tx.limits.workflow.as_ref().ok_or_else(|| Fault::Refused(
        "workflow_disabled", "workflow policy is not selected".into()))?;
    workflow::validate_recorded_actor(tx, actor)?;
    let query = QueryContext { tx, actor, now_ms: now, limits };
    match op {
        Operation::QueryQuestion { query: value } => return Ok((Data::QuestionQuery(super::collaboration_questions::query(&query, value)?), false)),
        Operation::QueryDocument { query: value } => return Ok((Data::DocumentQuery(super::collaboration_documents::query_document(&query, value)?), false)),
        Operation::QueryReview { query: value } => return Ok((Data::ReviewQuery(super::collaboration_reviews::query_review(&query, value)?), false)),
        Operation::QueryWork { query: value } => return Ok((Data::WorkQuery(super::collaboration_work::query(&query, value)?), false)),
        _ => (),
    }
    let operation_id = op.mutation_id().ok_or_else(|| Fault::Store("workflow mutation lacks its original operation identity".into()))?;
    let product_revision = workflow::product_revision(tx)?.checked_add(1)
        .filter(|revision| *revision <= i64::MAX as u64)
        .ok_or_else(|| Fault::Refused("capacity", "workflow revision exhausted".into()))?;
    let ctx = Context { tx, actor, operation_id, product_revision, now_ms: now, limits };
    match op {
        Operation::MutateQuestion { mutation, .. } => complete(&ctx, super::collaboration_questions::apply(&ctx, mutation)?, Data::QuestionMutation),
        Operation::MutateDocument { mutation, .. } => complete(&ctx, super::collaboration_documents::apply_document(&ctx, mutation)?, Data::DocumentMutation),
        Operation::MutateReview { mutation, .. } => complete(&ctx, super::collaboration_reviews::apply_review(&ctx, mutation)?, Data::ReviewMutation),
        Operation::MutateWork { mutation, .. } => complete(&ctx, super::collaboration_work::apply(&ctx, mutation)?, Data::WorkMutation),
        _ => refuse("invalid_input", "operation is outside the closed workflow interface"),
    }
}

/// The enclosing collaboration handler has bound accepted limits and performs
/// the last-use deadline check. This path preserves its original receipt ID.
pub(super) fn agent(tx: &StoreContext<'_>, call: &crate::mcp::Invocation, now: u64) -> Result<(Data, bool)> {
    //= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
    //# The controller MUST derive a collaboration mutation's actor provenance from its invocation context.
    let task = super::collaboration_calls::caller(tx, &call.caller, now)?.task;
    call.validate(tx.limits).map_err(|_| Fault::Refused("invalid_input", "invalid collaboration invocation".into()))?;
    //= spec/impl/v2/implementation.md#collaboration-surfaces-preserve-existing-authority
    //# Adding collaboration operations MUST NOT expand an already accepted task's configured tool profile.
    crate::mcp::validate_arguments_for_profile(&call.tool, &call.arguments, tx.limits,
        task.input.definition.execution_profile.as_ref())
        .map_err(|reason| Fault::Refused("invalid_input", reason))?;
    let actor = RecordedActor {
        principal: PrincipalRef::Agent { agent_id: task.assigned_agent_id.clone() },
        provenance: ActorProvenance::BoundAgentSession {
            agent_id: task.assigned_agent_id, session_id: task.session_id,
            task_id: call.caller.task_id.clone(), attempt_id: call.caller.attempt_id.clone(),
            generation: call.caller.generation, worker_id: call.caller.worker_id.clone(),
            incarnation: call.caller.incarnation.clone(),
        },
    };
    let operation = if crate::mcp::questions::handles(&call.tool) {
        match crate::mcp::questions::parse(&call.tool, &call.arguments, &actor)
            .map_err(|reason| Fault::Refused("invalid_input", reason))? {
            crate::mcp::questions::Command::Mutation(mutation) => Operation::MutateQuestion { operation_id: call.operation_id.clone(), mutation },
            crate::mcp::questions::Command::Query(query) => Operation::QueryQuestion { query },
        }
    } else if crate::mcp::work::handles(&call.tool) {
        match crate::mcp::work::parse(&call.tool, &call.arguments)
            .map_err(|reason| Fault::Refused("invalid_input", reason))? {
            crate::mcp::work::Command::Mutation(mutation) => Operation::MutateWork { operation_id: call.operation_id.clone(), mutation },
            crate::mcp::work::Command::Query(query) => Operation::QueryWork { query },
        }
    } else {
        super::collaboration_document_tools::operation(call)?
            .ok_or_else(|| Fault::Refused("invalid_input", "unknown collaboration workflow tool".into()))?
    };
    dispatch(tx, &operation, &actor, now)
}
