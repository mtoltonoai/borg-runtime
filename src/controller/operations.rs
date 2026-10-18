use super::database::StoreContext as Connection;
use super::{ControllerConfig, bounded_json_size, database::*, validation::Context};
use crate::protocol::*;
use rusqlite::{OptionalExtension, Transaction, params};
use std::collections::HashSet;
mod admission;
mod effects;
mod native_loop;
pub(crate) mod native_format;
pub(crate) mod native_output_cap;
pub(crate) mod native_retry;
pub(super) use native_loop::{validate_completion_gap, validate_rejection_chain};
pub(super) mod reconciliation;
pub(super) mod releases;
mod reports;
mod task_tool;

pub(crate) fn apply(
    tx: &Connection,
    op: &Operation,
    cfg: &ControllerConfig,
    now: u64,
    validation: &Context<'_>,
) -> Result<(Data, bool)> {
    let limits = tx.limits;
    match op {
        Operation::AdministerIdentity { operation_id, input } => super::identity_administration::apply(tx, operation_id, input, cfg, now),
        Operation::AcquireNativeRequest { .. } | Operation::SettleNativeRequest { .. }
        | Operation::ObserveNativeRequest { .. } => super::native_request_coordination::apply(tx, op, cfg, now),
        Operation::MutateGovernance { operation_id, mutation } => {
            super::governance::operator(tx)?;
            super::governance::bounded(tx, mutation)?;
            let result = super::governance::mutate(tx, operation_id, mutation, now, cfg)?;
            super::governance::bounded(tx, &result)?;
            Ok((Data::Governance(Box::new(result)), true))
        }
        Operation::QueryGovernance { query } => {
            super::governance::operator(tx)?;
            super::governance::bounded(tx, query)?;
            let result = super::governance::query(tx, query)?;
            super::governance::bounded(tx, &result)?;
            Ok((Data::Governance(Box::new(result)), false))
        }
        Operation::MutateCharter { operation_id, mutation } => {
            let charter = limits.charter.as_ref().ok_or_else(|| Fault::Refused("not_configured", "charter policy is absent".into()))?;
            let context = super::charters::Context { tx, operation_id, now_ms: now, limits: charter };
            Ok((Data::Charter(super::charters::mutate(&context, mutation)?), true))
        }
        Operation::ObserveCharter { operation_id, caller, observation } => {
            let charter = limits.charter.as_ref().ok_or_else(|| Fault::Refused("not_configured", "charter policy is absent".into()))?;
            let context = super::charters::Context { tx, operation_id, now_ms: now, limits: charter };
            // The producer distinguishes current new execution from retained
            // late audit. A blanket current-caller check would lose real replies.
            Ok((Data::Charter(super::charters::observe(&context, caller, observation)?), true))
        }
        Operation::QueryCharter { query } => Ok((Data::CharterQuery(super::charters::query(tx, query)?), false)),
        Operation::EvaluateCompletion(_)
        | Operation::BeginCompletionContinuation(_)
        | Operation::AcknowledgeCompletionInput(_) => {
            super::governance_completion::apply(tx, op, cfg, now)
        }
        Operation::ObserveCollaborationDelivery { .. }
        | Operation::PrepareCollaborationDelivery { .. }
        | Operation::BeginCollaborationDelivery { .. }
        | Operation::AcknowledgeCollaborationDelivery { .. } => {
            super::governance_completion::before_delivery(tx, op)?;
            super::collaboration_notifications::apply(tx, op, cfg, now)
        }
        Operation::QueryWorkflowActivity { cursor, limit } => Ok((
            Data::WorkflowActivity(super::collaboration_notifications::activity(tx, cursor, *limit)?),
            false,
        )),
        Operation::MutateQuestion { .. } | Operation::QueryQuestion { .. }
        | Operation::MutateDocument { .. } | Operation::QueryDocument { .. }
        | Operation::MutateReview { .. } | Operation::QueryReview { .. }
        | Operation::MutateWork { .. } | Operation::QueryWork { .. } => {
            super::collaboration_routing::direct(tx, op, now)
        }
        Operation::UpdateDefinition { operation_id, update } => {
            let actor = crate::collaboration::workflow::RecordedActor {
                principal: crate::collaboration::workflow::PrincipalRef::TrustedHostOperator,
                provenance: crate::collaboration::workflow::ActorProvenance::TrustedHost { path: tx.invocation_path },
            };
            super::definition_updates::apply(tx, update, cfg, operation_id, &actor)
        }
        Operation::RegisterRepository { .. }
        | Operation::ReviseRepository { .. }
        | Operation::SetRepositoryEnabled { .. }
        | Operation::ListRepositories { .. }
        | Operation::GetRepository { .. } => super::repositories::apply(tx, op),
        Operation::ObserveAgentMemory { .. }
        | Operation::PrepareAgentMemory { .. }
        | Operation::AcknowledgeAgentMemory { .. } => {
            super::memory_notifications::apply(tx, op, cfg, now)
        }
        Operation::CallCollaboration { call } => {
            super::collaboration_calls::apply(tx, call, cfg, now, validation)
        }
        Operation::GetCollaborationCall {
            caller,
            operation_id,
        } => super::collaboration_calls::lookup(tx, caller, operation_id),
        Operation::QueryCollaboration {
            query: crate::collaboration::Query::Repository { task_id, request },
        } => {
            if !valid_id(limits, task_id) {
                return refuse("invalid_input", "invalid task identity");
            }
            let original = load(tx, task_id)?;
            let output =
                super::repository_queries::observe(tx, validation, &original, request, now)?;
            Ok((
                Data::Collaboration(crate::collaboration::QueryResult::Repository(Box::new(
                    output,
                ))),
                false,
            ))
        }
        Operation::QueryCollaboration { query } => Ok((
            Data::Collaboration(super::collaboration_queries::apply(tx, query, cfg)?),
            false,
        )),
        Operation::RegisterAgent { .. }
        | Operation::RetireAgent { .. }
        | Operation::ListAgents { .. }
        | Operation::GetAgent { .. }
        | Operation::ListAgentTasks { .. } => super::agents::apply(tx, op, cfg, now),
        Operation::GetProtectedRelease { release_id } => Ok((
            Data::ProtectedRelease(Box::new(super::protected::view(tx, cfg, release_id)?)),
            false,
        )),
        Operation::ReconcileResult(input) => {
            let report = reconciliation::original_report(tx, &input.original_report_id)?;
            reports::observe(tx, &report, cfg, now, validation)
        }
        Operation::PrepareRelease(_)
        | Operation::GetRelease { .. }
        | Operation::ListReleases { .. } => releases::apply(tx, op, cfg),
        Operation::SubmitTaskDraft { input, .. } => {
            let selected = super::task_submission::resolve(tx, input)?;
            let t = submit(tx, &selected, cfg, now, None, 0, validation)?;
            Ok((Data::Task(Box::new(t.task)), true))
        }
        Operation::SubmitTask { input, .. } => {
            if input.resolved_limits.is_some() {
                return refuse(
                    "invalid_input",
                    "external submission cannot supply accepted limits metadata",
                );
            }
            let t = submit(tx, input, cfg, now, None, 0, validation)?;
            Ok((Data::Task(Box::new(t.task)), true))
        }
        Operation::GetTask { task_id } => Ok((
            Data::Task(Box::new(view(tx, load(tx, task_id)?, cfg, now)?)),
            false,
        )),
        Operation::ListTasks { after, limit } => {
            if *limit == 0 {
                return refuse("invalid_input", "invalid list page bound");
            }
            if *limit > cfg.max_list_tasks {
                return refuse(
                    "invalid_input",
                    "invalid list page bound: controller.max_list_tasks",
                );
            }
            let seq = match after {
                Some(id) => tx
                    .query_row("SELECT seq FROM tasks WHERE id=?1", [id], |r| {
                        r.get::<_, i64>(0)
                    })
                    .optional()?
                    .ok_or_else(|| Fault::Refused("invalid_input", "unknown page cursor".into()))?,
                None => 0,
            };
            let mut q = tx.prepare("SELECT body FROM tasks WHERE seq>?1 ORDER BY seq LIMIT ?2")?;
            let bodies = q
                .query_map(params![seq, *limit as i64], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let mut tasks = Vec::new();
            for body in bodies {
                let t: StoredTask = decode(&body)?;
                let t = view(tx, t, cfg, now)?;
                tasks.push(t);
                if bounded_json_size(&tasks, cfg.list_page_bytes).is_err() {
                    tasks.pop();
                    break;
                }
            }
            Ok((Data::Tasks(tasks), false))
        }
        Operation::AssignTask { operation_id, task_id, expected_revision, agent_id } => {
            super::task_handoffs::request(tx, operation_id, task_id, *expected_revision, agent_id, cfg, now, validation)
        }
        //= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
        //# Successful completion and cancellation MUST be ordered by the controller's committed task revision.
        Operation::CancelTask {
            task_id,
            expected_revision,
            ..
        } => {
            let mut t = load(tx, task_id)?;
            if terminal(t.task.status) {
                return Ok((Data::Task(Box::new(t.task)), false));
            }
            revision(&t, *expected_revision)?;
            super::task_handoffs::cancel_pending(tx, task_id)?;
            t.task.pending_assignment = None;
            cancel(tx, &mut t, cfg)?;
            Ok((Data::Task(Box::new(t.task)), true))
        }
        //= spec/impl/v2/implementation.md#task-state-separates-execution-from-acceptance
        //# A retry MUST create a new attempt without changing the task identity.
        //= spec/impl/v2/implementation.md#interrupted-work-is-reconciled
        //# A recovery attempt MUST preserve the original task identity.
        Operation::RetryTask {
            task_id,
            expected_revision,
            ..
        } => {
            let mut t = load(tx, task_id)?;
            revision(&t, *expected_revision)?;
            if super::task_handoffs::pending(tx, task_id)? { return refuse("handoff_pending", "assignment awaits original execution closure"); }
            //= spec/impl/v2/implementation.md#task-state-separates-execution-from-acceptance
            //# A succeeded task MUST NOT be reopened by a retry request.
            if !matches!(t.task.status, TaskStatus::Failed | TaskStatus::Cancelled | TaskStatus::Blocked) {
                return refuse(
                    "invalid_transition",
                    "only failed, cancelled or blocked tasks can retry",
                );
            }
            //= spec/impl/v2/implementation.md#interrupted-work-is-reconciled
            //# An external effect with an uncertain outcome MUST be reconciled before the platform retries that effect.
            if active(&t.task)
                || t.unresolved_execution
                || t.task.attempt.as_ref().is_some_and(|a| a.uncertain)
                || pending_effects(tx, &t.task.id)? > 0
                || !descendants_settled(tx, &t.task.id, cfg.max_tasks)?
            {
                return refuse(
                    "reconciliation_required",
                    "prior execution/effects are not settled",
                );
            }
            if t.task.input.limits.deadline_unix_ms <= now {
                return refuse("deadline", "retry cannot extend the original task deadline");
            }
            if used_model_requests(tx, &t.task.id)? + u64::from(t.delegated_model_requests)
                >= u64::from(t.task.input.limits.model_requests)
                && !direct_child_continuation(&t)
            {
                return refuse("capacity", "retry has no remaining model-request budget");
            }
            // A new attempt cannot disguise an unchanged fatal request as a
            // correction. No accepted-input/config remediation proof exists here.
            if t.task.native_retry_block.as_ref().is_some_and(|block| matches!(block.reason,
                crate::native::RetryBlockReason::InvalidRequest
                | crate::native::RetryBlockReason::ContextLimit
                | crate::native::RetryBlockReason::PolicyRefused
                | crate::native::RetryBlockReason::QuotaExhausted
                | crate::native::RetryBlockReason::AuthenticationFailed)) {
                return refuse("retry_correction_required", "native rejection requires a separately corrected request or configuration");
            }
            // A fresh attempt cannot reinterpret an older unbound definition.
            require_definition(&t.task.input.definition)?;
            if t.cleanup_remaining == 0 {
                if !cleanup_available(tx, cfg, 1)? {
                    return refuse("capacity", "no cancellation reserve available for retry");
                }
                t.cleanup_remaining = 1;
            }
            // Only explicit retry clears this hold after the existing guards.
            // Independent completion/uncertainty state is never reset here.
            t.task.native_retry_block = None;
            t.task.status = TaskStatus::Queued;
            t.task.result = None;
            t.task.blocked_reason = None;
            t.executor_success = None;
            save(tx, &mut t, "retry_queued", &serde_json::Value::Null)?;
            Ok((Data::Task(Box::new(t.task)), true))
        }
        //= spec/impl/v2/implementation.md#delegation-is-a-task-handoff
        //# A delegation MUST create a separately identified task.
        Operation::DelegateTask {
            operation_id,
            parent_task_id,
            parent_attempt_id,
            generation,
            child,
        } => {
            let mut parent = owned(tx, parent_task_id, parent_attempt_id, *generation, true)?;
            if parent.task.waiting_child_id.is_some() {
                return refuse("invalid_transition", "parent already waits on a child");
            }
            let continuation_reserve = configured_child(&parent, child)?;
            let depth = parent
                .depth
                .checked_add(1)
                .ok_or_else(|| Fault::Refused("capacity", "delegation depth overflow".into()))?;
            if depth > parent.task.input.limits.max_delegation_depth
                || child.limits.max_delegation_depth > parent.task.input.limits.max_delegation_depth
            {
                return refuse("capacity", "delegation exceeds the original depth bound");
            }
            if child.limits.deadline_unix_ms > parent.task.input.limits.deadline_unix_ms {
                return refuse("invalid_input", "child deadline exceeds parent deadline");
            }
            // Traceability scope: Durable parent usage plus configured child reservation and required continuation must fit; child accounting is reserved allowance, not hidden supplied-executor request measurement.
            //= spec/impl/v2/implementation.md#delegation-is-a-task-handoff
            //# The task's model-request accounting MUST include platform-mediated requests and configured child-task reservations.
            let spent = used_model_requests(tx, &parent.task.id)?;
            let reserved = parent
                .delegated_model_requests
                .checked_add(child.limits.model_requests)
                .ok_or_else(|| Fault::Refused("capacity", "delegated budget overflow".into()))?;
            if u64::from(reserved) + spent + continuation_reserve
                > u64::from(parent.task.input.limits.model_requests)
            {
                return refuse(
                    "capacity",
                    "delegated budget exceeds remaining parent budget",
                );
            }
            for dependency in &child.dependencies {
                if reaches(tx, dependency, parent_task_id, cfg.max_tasks)? {
                    return refuse(
                        "dependency_cycle",
                        "child dependency reaches waiting parent",
                    );
                }
            }
            let mut agent_tx=tx.with_limits(tx.limits);
            let a=parent.task.attempt.as_ref().ok_or_else(||Fault::Store("delegation caller attempt absent".into()))?;
            let caller=crate::mcp::Caller{task_id:parent.task.id.clone(),attempt_id:a.id.clone(),generation:a.generation,worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone()};
            agent_tx.bound_actor=Some(super::identity_administration::bound_actor(&parent.task,&caller));
            let child = submit(&agent_tx, child, cfg, now, Some(&parent), depth, validation)?;
            let final_now = (cfg.clock)();
            if final_now < now {
                return refuse(
                    "clock_unavailable",
                    "clock moved backwards during delegation",
                );
            }
            if final_now >= parent.task.input.limits.deadline_unix_ms
                || final_now >= child.task.input.limits.deadline_unix_ms
            {
                return refuse("deadline", "delegation crossed an original deadline");
            }
            //= spec/impl/v2/implementation.md#delegation-is-a-task-handoff
            //# A delegation MUST commit its child task and parent-wait association atomically.
            tx.execute("INSERT INTO waits(child,parent,parent_attempt,parent_generation,operation_id,state) VALUES(?1,?2,?3,?4,?5,'pending')",params![child.task.id,parent_task_id,parent_attempt_id,integer(*generation)?,operation_id])?;
            parent.delegated_model_requests = reserved;
            parent.task.waiting_child_id = Some(child.task.id.clone());
            parent.task.blocked_reason = Some("waiting_child".into());
            //= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
            //# A task waiting for delegated work MUST release its execution slot.
            stop(tx, &mut parent)?;
            save(tx, &mut parent, "child_delegated", &child.task.id)?;
            Ok((Data::Task(Box::new(child.task)), true))
        }
        Operation::RegisterWorker { worker, .. } => register(tx, worker, cfg),
        Operation::AwaitCommands { .. } => Err(Fault::Store(
            "command request missed receipt boundary".into(),
        )),
        Operation::ObserveWorker { report } => reports::observe(tx, report, cfg, now, validation),
        Operation::PrepareOperation { .. }
        | Operation::SettleOperation { .. }
        | Operation::GetOperation { .. }
        | Operation::GetMemory { .. }
        | Operation::SetMemory { .. } => effects::apply(tx, op, cfg, now),
    }
}

pub(super) fn require_definition(definition: &Definition) -> Result<()> {
    definition
        .required_inference()
        .map(|_| ())
        .map_err(|code| {
            Fault::Refused(
                code,
                "definition must explicitly bind the selected model and reasoning effort".into(),
            )
        })?;
    if definition.execution_profile.is_none() {
        return refuse(
            "template_profile_unbound",
            "new execution requires an explicit collaboration profile",
        );
    }
    Ok(())
}

pub(super) fn validate(input: &TaskInput, cfg: &ControllerConfig, now: u64) -> Result<()> {
    input
        .execution_limits()
        .map_err(|reason| Fault::Refused("invalid_limits", reason))?;
    let limits = input
        .resolved_limits
        .as_ref()
        .ok_or_else(|| Fault::Refused("invalid_limits", "accepted limit snapshot missing".into()))?
        .global_metadata
        .as_ref();
    let admission = &limits.controller;
    require_definition(&input.definition)?;
    if input.definition.id.len() > limits.transport.identity_bytes
        || input.definition.revision.len() > limits.transport.identity_bytes
    {
        return refuse(
            "invalid_input",
            "task identity/instruction is empty or invalid: transport.identity_bytes",
        );
    }
    if !valid_id(limits, &input.definition.id)
        || !valid_id(limits, &input.definition.revision)
        || input.instruction.is_empty()
    {
        return refuse(
            "invalid_input",
            "task identity/instruction is empty or invalid",
        );
    }
    if bounded_json_size(input, admission.max_task_input_bytes).is_err() {
        return refuse(
            "capacity",
            "task input exceeds bound: controller.max_task_input_bytes",
        );
    }
    if input.limits.deadline_unix_ms <= now || integer(input.limits.deadline_unix_ms).is_err() {
        return refuse("invalid_input", "task deadline is expired or unsupported");
    }
    if input.limits.output_bytes == 0 {
        return refuse(
            "invalid_input",
            "invalid finite task limits: task.input.limits.output_bytes must be positive",
        );
    }
    if input.limits.output_bytes > admission.max_task_output_bytes {
        return refuse(
            "invalid_input",
            "invalid finite task limits: controller.max_task_output_bytes",
        );
    }
    if input.limits.model_requests == 0 {
        return refuse(
            "invalid_input",
            "invalid finite task limits: task.input.limits.model_requests must be positive",
        );
    }
    if input.limits.result_bytes == 0 {
        return refuse(
            "invalid_input",
            "invalid finite task limits: task.input.limits.result_bytes must be positive",
        );
    }
    if input.limits.result_bytes > admission.max_task_result_bytes {
        return refuse(
            "invalid_input",
            "invalid finite task limits: controller.max_task_result_bytes",
        );
    }
    if input.limits.max_delegation_depth as usize > admission.max_delegation_depth {
        return refuse(
            "invalid_input",
            "invalid finite task limits: controller.max_delegation_depth",
        );
    }
    if input.dependencies.len() > admission.max_dependencies {
        return refuse(
            "invalid_input",
            "duplicate or excessive prerequisites: controller.max_dependencies",
        );
    }
    if input.dependencies.iter().collect::<HashSet<_>>().len() != input.dependencies.len() {
        return refuse("invalid_input", "duplicate or excessive prerequisites");
    }
    if input.acceptance_commands.len() > admission.max_acceptance_commands {
        return refuse(
            "invalid_input",
            "invalid acceptance command: controller.max_acceptance_commands",
        );
    }
    for command in &input.acceptance_commands {
        if command.is_empty() {
            return refuse("invalid_input", "invalid acceptance command");
        }
        if command.len() > admission.max_acceptance_arguments {
            return refuse(
                "invalid_input",
                "invalid acceptance command: controller.max_acceptance_arguments",
            );
        }
        for argument in command {
            if argument.len() > admission.max_acceptance_argument_bytes {
                return refuse(
                    "invalid_input",
                    "invalid acceptance command: controller.max_acceptance_argument_bytes",
                );
            }
            if argument.contains('\0') {
                return refuse("invalid_input", "invalid acceptance command");
            }
        }
    }
    if let Some(child) = crate::delegation::configured_template(input) {
        require_definition(&child.definition)?;
    }
    crate::execution_profile::validate(input)
        .map_err(|reason| Fault::Refused("invalid_input", reason))?;
    if let Some(target)=crate::delegation::handoff_template(input) {
        if target.limits!=input.limits{return refuse("invalid_input","same-task handoff limits must equal the accepted task limits")}
    }
    crate::delegation::validate_workflow(input)
        .map_err(|reason| Fault::Refused("invalid_input", reason))?;
    super::pull_request::validate_input(input, limits)?;
    if let Some(source) = &input.instruction_source {
        source.validate_instruction_binding(limits)
            .map_err(|reason| Fault::Refused("invalid_input", reason))?;
    }
    //= spec/impl/v2/implementation.md#definitions-select-execution
    //# An agent definition MUST identify the executor used for its tasks.
    if let Some(source) = &input.repository_read {
        if source.repository.len() > admission.max_repository_identity_bytes {
            return refuse(
                "invalid_input",
                "invalid explicit read-only repository source: controller.max_repository_identity_bytes",
            );
        }
        if input.definition.executor != ExecutorKind::Native
            || source.repository.is_empty()
            || source.repository.chars().any(char::is_control)
            || !commit_id(&source.start_commit)
        {
            return refuse(
                "invalid_input",
                "invalid explicit read-only repository source",
            );
        }
    }
    match input.definition.executor {
        ExecutorKind::Coding => {
            let Some(repo) = &input.repository else {
                return refuse("invalid_input", "coding task needs a repository");
            };
            if repo.url.len() > admission.max_repository_identity_bytes
                || repo.checkpoint_remote.len() > admission.max_repository_identity_bytes
            {
                return refuse(
                    "invalid_input",
                    "invalid immutable repository source: controller.max_repository_identity_bytes",
                );
            }
            if repo.url.is_empty()
                || repo.checkpoint_remote.is_empty()
                || !commit_id(&repo.start_commit)
            {
                return refuse("invalid_input", "invalid immutable repository source");
            }
        }
        ExecutorKind::Native => {
            if input.output_schema.is_none() {
                return refuse("invalid_input", "native task needs an output schema");
            }
            if input.repository.is_some() || !input.acceptance_commands.is_empty() {
                return refuse(
                    "invalid_input",
                    "native executor cannot run repository commands",
                );
            }
        }
    }
    Ok(())
}

/// The existing trusted generic handoff may finish directly from child data.
/// A configured native workflow always makes its separately reserved final call.
pub(super) fn direct_child_continuation(task: &StoredTask) -> bool {
    task.task.completed_child.is_some() && task.task.input.native_workflow.is_none()
}

fn configured_child(parent: &StoredTask, child: &TaskInput) -> Result<u64> {
    if crate::task_tool::enabled(&parent.task.input) {
        return refuse(
            "invalid_transition",
            "configured current-task inspection cannot delegate",
        );
    }
    let Some(template) = crate::delegation::template(&parent.task.input) else {
        return Ok(0);
    };
    if parent.task.completed_child.is_some() {
        return refuse(
            "invalid_transition",
            "configured continuation cannot delegate again",
        );
    }
    let instruction = if template.instruction_prefix.is_empty() {
        child.instruction.as_str()
    } else {
        let prefix = format!("{}\n\n", template.instruction_prefix);
        child
            .instruction
            .strip_prefix(prefix.as_str())
            .ok_or_else(|| {
                Fault::Refused(
                    "workflow_mismatch",
                    "child instruction omits configured prefix".into(),
                )
            })?
    };
    let expected = crate::delegation::coding_child(&parent.task.input, instruction)
        .map_err(|reason| Fault::Refused("workflow_mismatch", reason))?;
    if child != &expected {
        return refuse(
            "workflow_mismatch",
            "child differs from immutable workflow template",
        );
    }
    // Reserve the child non-refundably, while LEAVING one native request for
    // continuation. Adding this one to delegated_model_requests would consume it.
    Ok(1)
}

pub(super) fn configured_sources(
    tx: &Connection,
    input: &TaskInput,
    cfg: &ControllerConfig,
    now: u64,
    cycle_target: Option<&str>,
    depth: u32,
) -> Result<()> {
    if let Some(target) = crate::delegation::handoff_template(input) {
        // Validate accepted writable scope now without creating or delegating a task.
        super::task_handoffs::successor(tx, input, &target.definition, cfg, now)?;
        return Ok(());
    }
    if crate::delegation::template(input).is_none() {
        return Ok(());
    }
    if depth >= input.limits.max_delegation_depth {
        return refuse(
            "capacity",
            "configured coding child has no remaining delegation depth",
        );
    }
    // A minimal permitted instruction checks immutable fields without inventing
    // a future model decision. Actual instruction/body bounds are checked again
    // by configured_child and submit at the real delegation.
    let mut child = crate::delegation::coding_child(input, "x")
        .map_err(|reason| Fault::Refused("invalid_input", reason))?;
    let selected = input
        .resolved_limits
        .as_ref()
        .ok_or_else(|| Fault::Refused("invalid_limits", "parent limits absent".into()))?
        .global_metadata
        .as_ref();
    child.resolved_limits = Some(
        selected
            .resolve_task(&child)
            .map_err(|reason| Fault::Refused("invalid_limits", reason))?,
    );
    validate(&child, cfg, now)?;
    super::identity_administration::admit_definition(tx, &child.definition, None)?;
    super::charters::validate_definition(&tx.with_limits(selected), &child.definition)?;
    for dependency in &child.dependencies {
        load(tx, dependency)?;
        // For a new delegated native task, check its prospective waiting
        // parent. For an existing queued native task, check that task itself;
        // later wait edges may have changed since original submission.
        if let Some(target) = cycle_target {
            if reaches(tx, dependency, target, cfg.max_tasks)? {
                return refuse(
                    "dependency_cycle",
                    "configured child prerequisite reaches waiting parent",
                );
            }
        }
    }
    if child.definition.id == input.definition.id
        && child.definition.revision == input.definition.revision
        && child.definition != input.definition
    {
        return refuse(
            "definition_conflict",
            "workflow template conflicts with its own definition revision",
        );
    }
    let defined: Option<String> = tx
        .query_row(
            "SELECT body FROM definitions WHERE id=?1 AND revision=?2",
            params![child.definition.id, child.definition.revision],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(defined) = defined {
        if decode::<Definition>(&defined)? != child.definition {
            return refuse(
                "definition_conflict",
                "configured child definition revision changed its meaning",
            );
        }
    }
    let agent: Option<String> = tx
        .query_row(
            "SELECT definition FROM agents WHERE id=?1",
            [&child.definition.id],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(agent) = agent {
        if decode::<Definition>(&agent)?.id != child.definition.id {
            return refuse(
                "definition_conflict",
                "configured child agent has another definition",
            );
        }
    }
    Ok(())
}

pub(super) fn commit_id(s: &str) -> bool {
    matches!(s.len(), 40 | 64)
        && s.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

//= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
//# An accepted task MUST have a durable input record.
// Traceability scope: Resolution is attached to accepted input and persisted in the same transaction as acceptance.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//# An accepted task MUST retain its resolved execution-limit values.
pub(super) fn submit(
    tx: &Connection,
    input: &TaskInput,
    cfg: &ControllerConfig,
    now: u64,
    parent: Option<&StoredTask>,
    depth: u32,
    validation: &Context<'_>,
) -> Result<StoredTask> {
    super::identity_administration::admit_definition(tx, &input.definition, parent.filter(|p| crate::delegation::template(&p.task.input).is_some()).map(|p| &p.task.input))?;
    super::agents::require_active(tx, &input.definition.id)?;
    // This runs only after the original durable operation receipt lookup.
    // The receipt retains caller bytes; only the accepted task receives this
    // controller-computed snapshot in the same committing transaction.
    let selected = parent
        .filter(|p| crate::delegation::template(&p.task.input).is_some())
        .and_then(|p| p.task.input.resolved_limits.as_ref())
        .map(|s| s.global_metadata.as_ref())
        .unwrap_or(cfg.limits.as_ref());
    let snapshot = selected
        .resolve_task(input)
        .map_err(|reason| Fault::Refused("invalid_limits", reason))?;
    if input
        .resolved_limits
        .as_ref()
        .is_some_and(|claimed| claimed != &snapshot)
    {
        return refuse(
            "invalid_limits",
            "supplied child limits differ from accepted metadata",
        );
    }
    let mut accepted = input.clone();
    accepted.resolved_limits = Some(snapshot);
    let accepted_child =
        parent.is_some_and(|p| crate::delegation::template(&p.task.input).is_some());
    let repository_context = tx.with_limits(selected);
    super::repositories::resolve_input(&repository_context, &mut accepted, accepted_child)?;
    let input = &accepted;
    validate(input, cfg, now)?;
    super::charters::validate_definition(&repository_context, &input.definition)?;
    configured_sources(
        tx,
        input,
        cfg,
        now,
        parent.map(|p| p.task.id.as_str()),
        depth,
    )?;
    if input.definition.executor == ExecutorKind::Native || validation.creation.is_some() {
        validation.require(input, None, parent, now)?;
    }
    // Traceability scope: Durable task admission checks the configured task count and cancellation reserve; worker and owner-queue bounds are checked at their own admission boundaries.
    //= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
    //# Admission MUST enforce configured queue and resource bounds.
    let count: i64 = tx.query_row("SELECT count(*) FROM tasks", [], |r| r.get(0))?;
    if count >= cfg.max_tasks as i64 {
        return refuse(
            "capacity",
            "durable task capacity exhausted: controller.max_tasks",
        );
    }
    if !cleanup_available(tx, cfg, 1)? {
        return refuse(
            "capacity",
            "no cancellation reserve available for a new task: controller.max_cleanup_receipts",
        );
    }
    //= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
    //# Task dependencies MUST refer to existing tasks.
    for dep in &input.dependencies {
        load(tx, dep)?;
    }
    let defined: Option<String> = tx
        .query_row(
            "SELECT body FROM definitions WHERE id=?1 AND revision=?2",
            params![input.definition.id, input.definition.revision],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(defined) = defined {
        if decode::<Definition>(&defined)? != input.definition {
            return refuse(
                "definition_conflict",
                "definition revision changed its meaning",
            );
        }
    } else {
        tx.execute(
            "INSERT INTO definitions(id,revision,body) VALUES(?1,?2,?3)",
            params![
                input.definition.id,
                input.definition.revision,
                encode(&input.definition)?
            ],
        )?;
    }
    // Traceability scope: The stable agent key is the definition identity; selecting a newer revision does not retarget accepted sessions.
    //= spec/impl/v2/implementation.md#definitions-select-execution
    //# An agent identity MUST be the stable identity of its definition.
    let prior: Option<String> = tx
        .query_row(
            "SELECT definition FROM agents WHERE id=?1",
            [&input.definition.id],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(prior) = prior {
        if decode::<Definition>(&prior)?.id != input.definition.id {
            return refuse(
                "definition_conflict",
                "agent is bound to another definition identity",
            );
        }
        // Explicit task inputs never advance or revert an existing selected
        // head. Definition updates use the canonical revision-CAS operation.
        //= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
        //# Submitting an explicitly pinned definition MUST NOT change an existing agent's selected definition revision.
    } else {
        super::agents::require_directory_capacity(tx, cfg)?;
        tx.execute(
            "INSERT INTO agents(id,definition) VALUES(?1,?2)",
            params![input.definition.id, encode(&input.definition)?],
        )?;
    }
    let seq = next(tx, "task")?;
    let database_id: String = tx.query_row(
        "SELECT value FROM settings WHERE name='database_id'",
        [],
        |r| r.get(0),
    )?;
    if database_id.len() != 32
        || !database_id
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(Fault::Store("invalid database identity".into()));
    }
    let task_id = format!("task-{database_id}-{seq:020}");
    // Traceability scope: Each committed instantiation allocates a distinct durable session sequence; replay returns the prior operation receipt.
    //= spec/impl/v2/implementation.md#definitions-select-execution
    //# Every instantiation of an agent MUST have a distinct session identity.
    let mut session_sequence = next(tx, "session")?;
    let mut session_id = format!("session-{database_id}-{session_sequence:020}");
    if session_id == input.definition.id {
        session_sequence = next(tx, "session")?;
        session_id = format!("session-{database_id}-{session_sequence:020}");
    }
    // Traceability scope: The persisted session carries the accepted definition identity as its agent identity.
    //= spec/impl/v2/implementation.md#definitions-select-execution
    //# Every session MUST carry the agent identity it runs as.
    let session = Session {
        id: session_id,
        agent_id: input.definition.id.clone(),
        definition: input.definition.clone(),
        task_id: task_id.clone(),
    };
    super::sessions::insert(tx, &session, session_sequence)?;
    let t = StoredTask {
        task: Task {
            id: task_id,
            session_id: session.id.clone(),
            //= spec/impl/v2/implementation.md#definitions-select-execution
            //# A task MUST retain the definition revision accepted at submission.
            //= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
            //# A task's dependency set MUST remain immutable after submission.
            input: input.clone(),
            //= spec/impl/v2/implementation.md#definitions-select-execution
            //# An agent identity MUST persist independently of any executor process.
            assigned_agent_id: input.definition.id.clone(),
            status: TaskStatus::Queued,
            revision: 1,
            attempt: None,
            selected_checkpoint: None,
            result: None,
            blocked_reason: None, native_retry_block: None, pending_assignment: None,
            progress: String::new(),
            recent_output: String::new(),
            output_truncated: false,
            //= spec/impl/v2/implementation.md#delegation-is-a-task-handoff
            //# A delegated task MUST retain its parent task association.
            parent_task_id: parent.map(|p| p.task.id.clone()),
            waiting_child_id: None,
            completed_child: None,
        },
        generation: 0,
        attempt_counter: 0,
        depth,
        delegated_model_requests: 0,
        executor_success: None,
        unresolved_execution: false,
        cleanup_remaining: 1,
        completion_hold: None,
    };
    // IDs and accepted metadata now exist; reserve the minimal typed failure
    // envelope before this accepted task can reach a completion boundary.
    super::governance::validate_failure_envelope(tx, &t)?;
    insert(tx, &t, seq)?;
    super::sessions::bind_input(tx, &session, input, None, None)?;
    // Same transaction as accepted immutable definition, session and task.
    super::charters::initialize(&repository_context, &t.task.session_id)?;
    Ok(t)
}

// Traceability scope: Operator CAS covers ownership because each admission commits its generation through save(), which advances revision; terminal cancellation and exact receipt replay do not mutate a successor.
//= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
//# A task mutation MUST verify its current ownership generation.
pub(super) fn revision(t: &StoredTask, n: u64) -> Result<()> {
    if t.task.revision != n {
        return refuse("stale_revision", "task revision changed");
    }
    Ok(())
}
//= spec/impl/v2/implementation.md#worker-commands-do-not-create-duplicate-execution
//# A stale-generation report MUST NOT change the current task result, checkpoint or agent memory.
// Traceability scope: Attempt-origin mutations compare both stored current-generation values and the exact attempt identity in the controller transaction.
//= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
//# A task mutation MUST verify its current ownership generation.
pub(super) fn owned(
    tx: &Connection,
    task: &str,
    attempt: &str,
    generation: u64,
    require_running: bool,
) -> Result<StoredTask> {
    let t = load(tx, task)?;
    let Some(a) = &t.task.attempt else {
        return refuse("stale_generation", "task has no current attempt");
    };
    if a.id != attempt || a.generation != generation || t.generation != generation {
        return refuse("stale_generation", "attempt no longer owns task");
    }
    if require_running
        && (terminal(t.task.status) || a.stopped || a.phase == AttemptPhase::StopRequested)
    {
        return refuse("invalid_transition", "attempt cannot start new work");
    }
    Ok(t)
}
pub(super) fn pending_effects(tx: &Connection, task: &str) -> Result<i64> {
    // Identity columns are carried in the exact typed JSON, not inferred from a
    // caller key. JSON functions only index already validated local records.
    Ok(tx.query_row("SELECT count(*) FROM external_operations WHERE json_extract(body,'$.task_id')=?1 AND json_extract(body,'$.state')!='settled'",[task],|r|r.get(0))?)
}
pub(super) fn used_model_requests(tx: &Connection, task: &str) -> Result<u64> {
    let value: i64 = tx.query_row(
        "SELECT coalesce(sum(model_requests),0) FROM attempts WHERE task_id=?1",
        [task],
        |r| r.get(0),
    )?;
    Ok(value as u64)
}
fn reaches(tx: &Connection, start: &str, target: &str, bound: usize) -> Result<bool> {
    let mut pending = vec![start.to_string()];
    let mut visited = HashSet::new();
    while let Some(id) = pending.pop() {
        if id == target {
            return Ok(true);
        }
        if !visited.insert(id.clone()) {
            continue;
        }
        if visited.len() > bound {
            return refuse(
                "capacity",
                "dependency walk exceeds task bound: controller.max_tasks",
            );
        }
        let t = load(tx, &id)?;
        pending.extend(t.task.input.dependencies);
        if let Some(child) = t.task.waiting_child_id {
            pending.push(child);
        }
    }
    Ok(false)
}

fn delegated_children(tx: &Connection, parent: &str, bound: usize) -> Result<Vec<String>> {
    let limit = bound
        .checked_add(1)
        .and_then(|n| i64::try_from(n).ok())
        .ok_or_else(|| {
            Fault::Refused(
                "capacity",
                "delegation walk bound exceeds durable range".into(),
            )
        })?;
    let mut q = tx.prepare("SELECT child FROM waits WHERE parent=?1 ORDER BY child LIMIT ?2")?;
    let children = q
        .query_map(params![parent, limit], |r| r.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if children.len() > bound {
        return refuse(
            "capacity",
            "delegation walk exceeds task bound: controller.max_tasks",
        );
    }
    Ok(children)
}

pub(super) fn descendants_settled(tx: &Connection, root: &str, bound: usize) -> Result<bool> {
    let mut pending = vec![root.to_string()];
    let mut visited = HashSet::new();
    while let Some(parent) = pending.pop() {
        if !visited.insert(parent.clone()) {
            continue;
        }
        if visited.len() > bound {
            return refuse(
                "capacity",
                "descendant cleanup exceeds task bound: controller.max_tasks",
            );
        }
        let children = delegated_children(tx, &parent, bound)?;
        for child in children {
            let t = load(tx, &child)?;
            if !terminal(t.task.status)
                || active(&t.task)
                || t.unresolved_execution
                || t.task.attempt.as_ref().is_some_and(|a| a.uncertain)
                || pending_effects(tx, &child)? > 0
            {
                return Ok(false);
            }
            pending.push(child);
        }
    }
    Ok(true)
}
pub(crate) fn view(
    tx: &Connection,
    mut stored: StoredTask,
    cfg: &ControllerConfig,
    now: u64,
) -> Result<Task> {
    if stored.task.status == TaskStatus::Queued {
        // Current derived diagnostics, never a reservation or a liveness claim.
        // Do not carry a previous poller's capacity or child-ready reason.
        stored.task.blocked_reason = admission::current_reason(tx, &stored, cfg, now)?;
    }
    Ok(stored.task)
}

//= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
//# A cancellation request MUST commit cancellation intent and its stop command atomically.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# Every retry-related blocked state MUST have an explicit canonical exit transition available through the supported adapter or control interface.

// Traceability scope: Authorized cancellation commits its own stop intent without requesting voluntary completion admission.
//= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
//# An authorized cancellation or emergency stop MUST NOT wait for voluntary completion admission.

fn cancel(tx: &Connection, t: &mut StoredTask, cfg: &ControllerConfig) -> Result<()> {
    let mut pending = vec![t.task.id.clone()];
    let mut visited = HashSet::new();
    while let Some(id) = pending.pop() {
        if !visited.insert(id.clone()) {
            continue;
        }
        if visited.len() > cfg.max_tasks {
            return refuse(
                "capacity",
                "child cancellation exceeds task bound: controller.max_tasks",
            );
        }
        let mut current = load(tx, &id)?;
        // Settled waits retain lineage after waiting_child_id is cleared. A
        // terminal intermediate can still have outstanding descendants.
        for child in delegated_children(tx, &id, cfg.max_tasks)? {
            if load(tx, &child)?.task.parent_task_id.as_deref() != Some(&id) {
                return Err(Fault::Store(
                    "delegation lineage differs from original child".into(),
                ));
            }
            pending.push(child);
        }
        if !terminal(current.task.status) {
            current.task.status = TaskStatus::Cancelled;
            //= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
            //# A cancellation acknowledgment MUST distinguish requested cancellation from verified execution termination.
            current.task.blocked_reason = Some("cancellation_requested".into());
            stop(tx, &mut current)?;
            save(
                tx,
                &mut current,
                "cancel_requested",
                &serde_json::Value::Null,
            )?;
            settle_parent(tx, &current)?;
        } else if active(&current.task)
            && current
                .task
                .attempt
                .as_ref()
                .is_some_and(|a| a.phase != AttemptPhase::StopRequested)
        {
            // A terminal result is not process-termination evidence. Retain it
            // while stopping still-active descendants of this live root.
            stop(tx, &mut current)?;
            save(
                tx,
                &mut current,
                "descendant_stop_requested",
                &serde_json::Value::Null,
            )?;
        }
    }
    *t = load(tx, &t.task.id)?;
    Ok(())
}

fn register(tx: &Connection, worker: &Worker, cfg: &ControllerConfig) -> Result<(Data, bool)> {
    let limits = tx.limits;
    if worker.id.len() > limits.transport.identity_bytes
        || worker.incarnation.len() > limits.transport.identity_bytes
    {
        return refuse(
            "invalid_input",
            "invalid worker identity/capacity: transport.identity_bytes",
        );
    }
    if !valid_id(limits, &worker.id) || !valid_id(limits, &worker.incarnation) {
        return refuse("invalid_input", "invalid worker identity/capacity");
    }
    if worker.coding_capacity > cfg.max_worker_capacity
        || worker.native_capacity > cfg.max_worker_capacity
    {
        return refuse(
            "invalid_input",
            "invalid worker identity/capacity: controller.max_worker_capacity",
        );
    }
    let retired: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM retired_incarnations WHERE worker=?1 AND incarnation=?2)",
        params![worker.id, worker.incarnation],
        |r| r.get(0),
    )?;
    if retired {
        return refuse(
            "stale_incarnation",
            "worker incarnation was already retired",
        );
    }
    let old: Option<String> = tx
        .query_row("SELECT body FROM workers WHERE id=?1", [&worker.id], |r| {
            r.get(0)
        })
        .optional()?;
    if let Some(old) = old {
        let old: Worker = decode(&old)?;
        if old == *worker {
            return Ok((Data::Worker(old), false));
        }
        if old.incarnation == worker.incarnation {
            return refuse(
                "operation_conflict",
                "same incarnation changed registration",
            );
        }
        let live: i64 = tx.query_row(
            "SELECT count(*) FROM tasks WHERE worker=?1 AND active=1",
            [&worker.id],
            |r| r.get(0),
        )?;
        if live != 0 {
            return refuse(
                "reconciliation_required",
                "prior-incarnation processes are not confirmed stopped",
            );
        }
        let retired_count: i64 =
            tx.query_row("SELECT count(*) FROM retired_incarnations", [], |r| {
                r.get(0)
            })?;
        if unsigned(retired_count)? >= cfg.max_receipts as u64 {
            return refuse(
                "capacity",
                "retired worker incarnation history is full: controller.max_receipts",
            );
        }
        tx.execute(
            "INSERT INTO retired_incarnations(worker,incarnation) VALUES(?1,?2)",
            params![worker.id, old.incarnation],
        )?;
        tx.execute(
            "UPDATE workers SET body=?2 WHERE id=?1",
            params![worker.id, encode(worker)?],
        )?;
    } else {
        let count: i64 = tx.query_row("SELECT count(*) FROM workers", [], |r| r.get(0))?;
        if count >= cfg.max_workers as i64 {
            return refuse("capacity", "worker registry full: controller.max_workers");
        }
        tx.execute(
            "INSERT INTO workers(id,body) VALUES(?1,?2)",
            params![worker.id, encode(worker)?],
        )?;
    }
    Ok((Data::Worker(worker.clone()), true))
}

pub(crate) fn await_request(
    tx: &Connection,
    op: &Operation,
    cfg: &ControllerConfig,
    now: u64,
    maintenance_pending: bool,
) -> Result<(Data, bool, Option<String>)> {
    let Operation::AwaitCommands {
        operation_id,
        worker_id: id,
        incarnation: inc,
        acknowledged: ack,
    } = op
    else {
        return Err(Fault::Store("invalid command request route".into()));
    };
    let ordinary = regular_receipt_room(tx, cfg)?;
    let mut credit = None;
    let body: Option<String> = tx
        .query_row("SELECT body FROM workers WHERE id=?1", [id], |r| r.get(0))
        .optional()?;
    let Some(body) = body else {
        return refuse("unknown_worker", "worker is not registered");
    };
    let worker: Worker = decode(&body)?;
    if worker.incarnation.as_str() != inc.as_str() {
        return refuse("stale_incarnation", "worker registration changed");
    }
    if ack.len() > cfg.max_commands_per_reply {
        return refuse(
            "capacity",
            "too many command acknowledgments: controller.max_commands_per_reply",
        );
    }
    let mut changed = false;
    let mut stop_acknowledged = false;
    for command in ack {
        let belongs: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM commands WHERE id=?1 AND worker=?2 AND incarnation=?3)",
            params![command, id, inc],
            |r| r.get(0),
        )?;
        if !belongs {
            return refuse(
                "invalid_input",
                "acknowledgment does not identify this worker's command",
            );
        }
        let owner: (String, bool) = tx.query_row(
            "SELECT attempt_id,acknowledged FROM commands WHERE id=?1",
            [command],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if !owner.1 && tx.governance_limits.governance.is_some() {
            let maximum = cfg.limits.transport.body_bytes;
            let body: String = tx.query_row(
                "SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM commands WHERE id=?1",
                params![command, sql_bound(maximum.saturating_add(1))?], |r| r.get(0))?;
            if body.len() > maximum {
                return Err(Fault::Store("acknowledged command exceeds transport.body_bytes".into()));
            }
            let acknowledged_command: WorkerCommand = decode(&body)?;
            stop_acknowledged |= matches!(&acknowledged_command,
                WorkerCommand::StopAttempt { command_id, .. } if command_id == command);
        }
        if !owner.1 && !ordinary && credit.is_none() {
            if !super::credits::control_available(tx, &owner.0)? {
                return Err(Fault::Store(
                    "unacknowledged command lacks reserved credit".into(),
                ));
            }
            credit = Some(owner.0);
        }
        changed |= tx.execute(
            "UPDATE commands SET acknowledged=1 WHERE id=?1 AND acknowledged=0",
            [command],
        )? > 0;
    }
    // A pending Stop or its new ACK must not depend on unrelated ordinary
    // policy. Use the existing bounded Stop-only projection and original
    // receipt/credit path; this exchange admits no new work. Replay/identity,
    // storage, body bounds and delivery accounting below remain mandatory.
    let stop_commands = if maintenance_pending || tx.governance_limits.governance.is_some() {
        delivery_snapshot(tx, id, inc, cfg, false)?
    } else { Vec::new() };
    let control_exchange = maintenance_pending || stop_acknowledged || !stop_commands.is_empty();
    // Expiry remains in the original admission transaction. A control-only
    // exchange defers it along with all fresh admission, without masking errors.
    if ordinary && !control_exchange {
        changed |= super::governance_control::reconcile_expiry(tx, operation_id, now)?;
    }
    //= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
    //# Task admission MUST select the oldest eligible task within the requested executor's available capacity.
    let ids = if ordinary && !control_exchange {
        let mut q = tx.prepare("SELECT id FROM tasks WHERE status='queued' ORDER BY seq")?;
        q.query_map([], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?
    } else {
        Vec::new()
    };
    let mut commands = if control_exchange { stop_commands }
        else { delivery_snapshot(tx, id, inc, cfg, ordinary)? };
    // Existing commands remain backed by their original delivery receipts.
    // This exchange must also fit its full ACK/input lookup representation.
    while !super::metadata::commands_fit(&cfg.limits, op, &commands.iter().collect::<Vec<_>>()) {
        if commands.pop().is_none() {
            return refuse(
                "capacity",
                "command exchange input leaves no bounded receipt response",
            );
        }
    }
    let admission_open = regular_receipt_room(tx, cfg)?;
    for task_id in ids {
        if !admission_open {
            break;
        }
        if commands.len() >= cfg.max_commands_per_reply {
            break;
        }
        let mut t = load(tx, &task_id)?;
        let fresh_now = (cfg.clock)();
        if fresh_now < now {
            return refuse(
                "clock_unavailable",
                "clock moved backwards during admission",
            );
        }
        let (a, body, next_counter) = match admission::check(tx, &t, &worker, cfg, fresh_now)? {
            admission::Decision::Blocked(reason) => {
                // Preserve the typed completion marker across capacity/wait
                // deferrals. Queued views still compute the live admission reason;
                // a diagnostic deferral cannot erase original resume provenance.
                let completion_held = t.completion_hold.as_ref().is_some_and(|hold|
                    matches!(hold.state.disposition, crate::governance::CompletionDisposition::Blocked { .. }));
                if !completion_held && t.task.blocked_reason.as_ref() != Some(&reason) {
                    t.task.blocked_reason = Some(reason.clone());
                    save(tx, &mut t, "admission_deferred", &reason)?;
                    changed = true;
                }
                continue;
            }
            admission::Decision::BudgetExhausted => {
                t.task.status = TaskStatus::Failed;
                t.task.blocked_reason = Some("model_request_budget_exhausted".into());
                save(tx, &mut t, "budget_exhausted", &serde_json::Value::Null)?;
                settle_parent(tx, &t)?;
                changed = true;
                continue;
            }
            admission::Decision::Ready {
                attempt,
                command,
                next_counter,
            } => (attempt, command, next_counter),
        };
        // An attempt may commit only when its Run is in this exact response.
        // Existing unacknowledged commands consume both page and byte space.
        if !delivery_fits(&commands, &body, cfg, op) {
            let reason = "command_delivery_capacity".to_string();
            if t.task.blocked_reason.as_ref() != Some(&reason) {
                t.task.blocked_reason = Some(reason.clone());
                save(tx, &mut t, "admission_deferred", &reason)?;
                changed = true;
            }
            continue;
        }
        // Still sample the original deadline after all bounded eligibility and
        // command-copy work. No await, network work or permission is introduced.
        let final_now = (cfg.clock)();
        if final_now < fresh_now {
            return refuse(
                "clock_unavailable",
                "clock moved backwards during admission",
            );
        }
        if final_now >= t.task.input.limits.deadline_unix_ms {
            continue;
        }
        if t.task.input.native_workflow.is_some() && t.task.completed_child.is_none() {
            match configured_sources(tx, &t.task.input, cfg, final_now, Some(&t.task.id), t.depth) {
                Ok(()) => {}
                Err(Fault::Refused(code, _)) => {
                    let reason = format!("configured_child_unavailable:{code}");
                    if t.task.blocked_reason.as_ref() != Some(&reason) {
                        t.task.blocked_reason = Some(reason.clone());
                        save(tx, &mut t, "admission_deferred", &reason)?;
                        changed = true;
                    }
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        // Keep the exact pre-admission task for the wait/unknown-owner guard.
        // The selected attempt must exist before its FK-backed consumption rows.
        let prior_task = t.task.clone();
        t.generation = a.generation;
        t.attempt_counter = next_counter;
        t.task.attempt = Some(a.clone());
        t.task.status = TaskStatus::Active;
        t.task.blocked_reason = None;
        t.executor_success = None;
        t.cleanup_remaining = 5;
        let command_id = format!("run-{}", a.id);
        //= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
        //# An execution attempt MUST have a durable identity before it begins.
        save(tx, &mut t, "attempt_assigned", &a)?;
        if let Some(limits) = tx.limits.workflow.as_ref() {
            let WorkerCommand::RunAttempt { workflow_continuation, .. } = &body else {
                return Err(Fault::Store("admission command is not an execution attempt".into()));
            };
            // Same original request transaction: an association failure rolls
            // back the attempt, task, history and later RunAttempt together.
            super::collaboration_work::admitted(tx, &prior_task, &a, workflow_continuation.as_ref(), limits)?;
        }
        tx.execute(
            "UPDATE attempts SET control_remaining=3 WHERE id=?1",
            [&a.id],
        )?;
        //= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
        //# An attempt assignment MUST commit its worker command in the same transaction.
        command(
            tx,
            id,
            inc,
            &t.task.id,
            &a.id,
            a.generation,
            &body,
            &command_id,
        )?;
        commands.push(body);
        changed = true;
    }
    // Every newly committed Run is in this exact snapshot. Old commands are
    // current filtered data; replayed receipts remain historical data.
    for command in &commands {
        let (command_id, attempt, is_stop) = match command {
            WorkerCommand::RunAttempt {
                command_id,
                attempt,
                ..
            } => (command_id, &attempt.id, false),
            WorkerCommand::StopAttempt {
                command_id,
                attempt_id,
                ..
            } => (command_id, attempt_id, true),
            WorkerCommand::CollaborationReady { command_id, caller, .. } =>
                (command_id, &caller.attempt_id, false),
        };
        let delivered: Option<String> = tx.query_row(
            "SELECT delivery_operation_id FROM commands WHERE id=?1",
            [command_id],
            |r| r.get(0),
        )?;
        if delivered.is_none() {
            if !ordinary && is_stop && credit.is_none() {
                if !super::credits::control_available(tx, attempt)? {
                    return Err(Fault::Store("Stop delivery lacks reserved credit".into()));
                }
                credit = Some(attempt.clone());
            }
            tx.execute("UPDATE commands SET delivery_operation_id=?2 WHERE id=?1 AND delivery_operation_id IS NULL", params![command_id, operation_id])?;
            changed = true;
        }
    }
    if (!changed && commands.is_empty()) || (!ordinary && credit.is_none()) {
        return Err(Fault::Pending(operation_id.clone()));
    }
    Ok((Data::Commands(commands), changed, credit))
}

pub(super) fn delivery_snapshot(
    tx: &Connection,
    id: &str,
    inc: &str,
    cfg: &ControllerConfig,
    ordinary: bool,
) -> Result<Vec<WorkerCommand>> {
    let bodies = {
        let mut q = tx.prepare(
            "SELECT c.body FROM commands c JOIN tasks t ON t.id=c.task_id
            WHERE c.worker=?1 AND c.incarnation=?2 AND c.acknowledged=0
            AND (?4 OR json_extract(c.body,'$.kind')='stop_attempt')
            AND (json_extract(c.body,'$.kind')!='run_attempt' OR (t.status='active' AND t.active=1
              AND c.attempt_id=json_extract(t.body,'$.task.attempt.id')
              AND json_extract(t.body,'$.task.attempt.phase')!='stop_requested'))
            ORDER BY c.seq LIMIT ?3",
        )?;
        q.query_map(
            params![id, inc, cfg.max_commands_per_reply as i64, ordinary],
            |r| r.get::<_, String>(0),
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let mut commands = Vec::new();
    for body in bodies {
        let c: WorkerCommand = decode(&body)?;
        if let WorkerCommand::RunAttempt { attempt, .. } = &c {
            let t = load(tx, &attempt.task_id)?;
            if t.task.status != TaskStatus::Active
                || t.task.attempt.as_ref().is_none_or(|a| {
                    a.id != attempt.id || a.phase == AttemptPhase::StopRequested || a.stopped
                })
            {
                continue;
            }
        }
        commands.push(c);
        let count_bound = commands.len() > cfg.max_commands_per_reply;
        if count_bound || bounded_json_size(&commands, cfg.command_page_bytes).is_err() {
            commands.pop();
            if commands.is_empty() {
                return refuse(
                    "capacity",
                    if count_bound {
                        "persisted command exceeds reply bound; no command was silently omitted: controller.max_commands_per_reply"
                    } else {
                        "persisted command exceeds reply bound; no command was silently omitted: controller.command_page_bytes"
                    },
                );
            }
            break;
        }
    }
    Ok(commands)
}

pub(super) fn delivery_fits(
    commands: &[WorkerCommand],
    candidate: &WorkerCommand,
    cfg: &ControllerConfig,
    operation: &Operation,
) -> bool {
    if commands.len() >= cfg.max_commands_per_reply {
        return false;
    }
    // References serialize to the same exact array without copying task bodies.
    let proposed: Vec<_> = commands.iter().chain(std::iter::once(candidate)).collect();
    bounded_json_size(&proposed, cfg.command_page_bytes).is_ok()
        && super::metadata::commands_fit(&cfg.limits, operation, &proposed)
}

// Append inside controller::operations; uses the original cancellation body.
// No physical termination claim and no ordinary completion policy evaluation.
pub(super) fn governance_stop(
    tx: &Connection, task_id: &str, expected_revision: u64, cfg: &ControllerConfig,
) -> Result<()> {
    let mut stored=load(tx,task_id)?;
    if terminal(stored.task.status) {return Ok(())}
    revision(&stored,expected_revision)?;
    cancel(tx,&mut stored,cfg)
}
