//! Data-only joins for the explicitly configured native/coding workflow.
//! No provider, database, process or access-control side effects occur here.
use crate::protocol::*;

pub const DECISION_INSTRUCTION: &str = "Choose exactly one JSON object with two fields: {\"kind\":\"complete\",\"result\":...} or {\"kind\":\"delegate\",\"instruction\":\"...\"}. For complete, replace ... with a JSON value matching the required result schema. For delegate, replace ... with a nonempty instruction for the configured coding task. Return no other fields. Do not provide repository, host, provider, credentials, checks, identity or limits. Those are fixed configuration.\n\n";
pub const DECISION_SCHEMA_SEPARATOR: &str =
    "\n\nRequired schema of a complete result (references remain rooted in this schema):\n";
pub const CONTINUATION_INSTRUCTION: &str = "Return the final result using the recorded child outcome below as data. This continuation cannot create another child. A failed or cancelled child is not a successful child.\n\n";
pub const CHILD_OUTCOME_SEPARATOR: &str =
    "\n\nRecorded child task outcome for this new continuation (not recovered executor history):\n";

pub fn template(input: &TaskInput) -> Option<&CodingTaskTemplate> {
    input
        .native_workflow
        .as_deref()
        .and_then(|workflow| match workflow {
            NativeWorkflow::CodingThenContinue { child } => Some(child),
            NativeWorkflow::InspectCurrentTaskThenContinue {} | NativeWorkflow::Reassign { .. } => None,
        })
}

/// Same-task work scope, never a child-delegation decision or continuation.
pub fn handoff_template(input: &TaskInput) -> Option<&CodingTaskTemplate> {
    match input.native_workflow.as_deref() {
        Some(NativeWorkflow::Reassign { target }) => Some(target),
        _ => None,
    }
}

/// Shared validation only: callers must not infer child creation from this helper.
pub fn configured_template(input: &TaskInput) -> Option<&CodingTaskTemplate> {
    template(input).or_else(|| handoff_template(input))
}

/// Checks immutable configuration. The controller separately checks existing
/// prerequisites, current authority, consumed budget and current deadlines.
// Traceability scope: The accepted handoff target keeps the exact root task limits and dependencies; controller ownership and session history are validated separately.
//= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
//# A task reassignment MUST preserve the task's accepted deadline.
pub fn validate_workflow(input: &TaskInput) -> Result<(), String> {
    if crate::task_tool::enabled(input) {
        if input.definition.executor != ExecutorKind::Native || input.limits.model_requests < 2 {
            return Err("native_task_tool_budget_or_executor_invalid".into());
        }
        return Ok(());
    }
    let Some(child) = configured_template(input) else {
        return Ok(());
    };
    if input.definition.executor != ExecutorKind::Native
        || child.definition.executor != ExecutorKind::Coding
    {
        return Err("native_workflow_executor_invalid".into());
    }
    if crate::credential::repository_contains_credentials(&child.repository.url)
        || crate::credential::repository_contains_credentials(&child.repository.checkpoint_remote)
    {
        return Err("native_workflow_repository_contains_credentials".into());
    }
    let snapshot = input
        .resolved_limits
        .as_ref()
        .ok_or("execution limits are not bound at admission")?;
    input.execution_limits()?;
    if handoff_template(input).is_some() {
        // RunAttempt carries only the unspent request allowance. Original
        // admission separately requires exact target/root limits equality.
        let mut remaining = child.limits.clone();
        remaining.model_requests = input.limits.model_requests;
        if !child.instruction_prefix.is_empty()
            || child.dependencies != input.dependencies
            || remaining != input.limits
            || child.limits.model_requests < input.limits.model_requests
        {
            return Err("native_reassignment_obligations_differ".into());
        }
        // No decision/continuation or separate child budget is reserved. The
        // controller retains this task's instruction, result schema and usage.
        return Ok(());
    }
    let native = &input.execution_limits()?.native;
    if child.limits.model_requests == 0
        || child.limits.result_bytes == 0
        || child.limits.result_bytes > snapshot.global_metadata.controller.max_task_result_bytes
        || child.limits.deadline_unix_ms > input.limits.deadline_unix_ms
        || child.limits.max_delegation_depth > input.limits.max_delegation_depth
        || input.limits.max_delegation_depth == 0
        || child
            .limits
            .model_requests
            .checked_add(2)
            .is_none_or(|minimum| minimum > input.limits.model_requests)
    {
        return Err("native_workflow_budget_invalid".into());
    }
    decision_instruction(input)?;
    // ChildOutcome carries AcceptedResult plus a repeated result summary.
    // The result bound already includes that summary's JSON encoding. Twice
    // that bound, plus the configured envelope for identities/status/framing,
    // therefore bounds the full inline outcome without discarding any field.
    let continuation = child
        .limits
        .result_bytes
        .checked_mul(2)
        .and_then(|n| n.checked_add(native.continuation_envelope_bytes))
        .and_then(|n| n.checked_add(input.instruction.len()))
        .and_then(|n| n.checked_add(CHILD_OUTCOME_SEPARATOR.len()))
        .and_then(|n| n.checked_add(CONTINUATION_INSTRUCTION.len()))
        .ok_or("native_continuation_bound")?;
    if continuation > native.context_bytes {
        return Err("native_continuation_bound".into());
    }
    Ok(())
}

/// A model supplies only bounded instruction data. Every execution choice is
/// copied from immutable configured input, never read from model JSON fields.

// Traceability scope: Delegation supplies instruction text to the accepted template; execution settings and repository remain configured.
//= spec/impl/v2/implementation.md#delegation-is-a-task-handoff
//# A delegation request MUST NOT change configured host, repository or provider access.

pub fn coding_child(input: &TaskInput, instruction: &str) -> Result<TaskInput, String> {
    let child = template(input).ok_or("native_workflow_missing")?;
    if crate::credential::repository_contains_credentials(&child.repository.url)
        || crate::credential::repository_contains_credentials(&child.repository.checkpoint_remote)
    {
        return Err("native_workflow_repository_contains_credentials".into());
    }
    if instruction.is_empty() || instruction.len() > input.limits.output_bytes {
        return Err("native_delegation_instruction_bound".into());
    }
    let instruction = if child.instruction_prefix.is_empty() {
        instruction.to_owned()
    } else {
        format!("{}\n\n{instruction}", child.instruction_prefix)
    };
    let mut result = TaskInput {
        instruction_source: child.instruction_source.clone(),
        pull_request: child.pull_request.clone(),
        resolved_limits: None,
        repository_read: None,
        instruction,
        definition: child.definition.clone(),
        repository: Some(child.repository.clone()),
        acceptance_commands: child.acceptance_commands.clone(),
        output_schema: None,
        limits: child.limits.clone(),
        dependencies: child.dependencies.clone(),
        native_workflow: None,
    };
    let snapshot = input
        .resolved_limits
        .as_ref()
        .ok_or("execution limits are not bound at admission")?;
    input.execution_limits()?;
    result.resolved_limits = Some(snapshot.global_metadata.resolve_task(&result)?);
    bounded_json_size(
        &result,
        snapshot.global_metadata.controller.max_task_input_bytes,
    )?;
    Ok(result)
}

pub fn decision_instruction(input: &TaskInput) -> Result<String, String> {
    let schema = input
        .output_schema
        .as_ref()
        .ok_or("native_output_schema_missing")?;
    let remaining = input
        .execution_limits()?
        .native
        .context_bytes
        .checked_sub(DECISION_INSTRUCTION.len())
        .and_then(|n| n.checked_sub(input.instruction.len()))
        .and_then(|n| n.checked_sub(DECISION_SCHEMA_SEPARATOR.len()))
        .ok_or("native_instruction_bound")?;
    bounded_json_size(schema, remaining).map_err(|_| "native_instruction_bound")?;
    let schema = serde_json::to_string(schema).map_err(|_| "native_schema_invalid")?;
    Ok(format!(
        "{DECISION_INSTRUCTION}{}{DECISION_SCHEMA_SEPARATOR}{schema}",
        input.instruction
    ))
}

pub fn continuation_instruction(input: &TaskInput, child: &ChildOutcome) -> Result<String, String> {
    if handoff_template(input).is_some() {
        return Err("native_reassignment_has_no_child_continuation".into());
    }
    if !matches!(
        child.status,
        TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::Cancelled
    ) || (child.status == TaskStatus::Succeeded && child.result.is_none())
    {
        return Err("native_child_outcome_invalid".into());
    }
    if let Some(template) = template(input) {
        if let Some(result) = &child.result {
            bounded_json_size(result, template.limits.result_bytes)
                .map_err(|_| "native_child_result_bound")?;
        }
    }
    let prefix = if input.native_workflow.is_some() {
        CONTINUATION_INSTRUCTION
    } else {
        ""
    };
    let remaining = input
        .execution_limits()?
        .native
        .context_bytes
        .checked_sub(prefix.len())
        .and_then(|n| n.checked_sub(input.instruction.len()))
        .and_then(|n| n.checked_sub(CHILD_OUTCOME_SEPARATOR.len()))
        .ok_or("native_continuation_bound")?;
    bounded_json_size(child, remaining).map_err(|_| "native_continuation_bound")?;
    let child = serde_json::to_string(child).map_err(|_| "native_child_outcome_invalid")?;
    Ok(format!(
        "{prefix}{}{CHILD_OUTCOME_SEPARATOR}{child}",
        input.instruction
    ))
}

pub fn accepted_result_fits(limits: &Limits, result: &AcceptedResult) -> bool {
    limits.result_bytes > 0 && bounded_json_size(result, limits.result_bytes).is_ok()
}

#[cfg(test)]
mod metadata_tests {
    use super::*;

    #[test]
    fn decision_context_uses_bound_agent_metadata_and_refuses_missing_snapshot() {
        let mut global = crate::limits::RuntimeLimits::seed().unwrap();
        global.execution.native.context_bytes=65_536;
        let mut input = TaskInput {
            instruction_source: None,
            pull_request: None,
            resolved_limits: None,
            repository_read: None,
            instruction: "x".repeat(global.execution.native.context_bytes),
            definition: Definition {
                charter: None,
                id: "definition".into(),
                revision: "r1".into(),
                executor: ExecutorKind::Native,
                inference: None,
                execution_profile: None,
                limit_overrides: None,
            },
            repository: None,
            acceptance_commands: vec![],
            output_schema: Some(serde_json::json!({"type":"string"})),
            limits: global.task_defaults.at(0).unwrap(),
            dependencies: vec![],
            native_workflow: None,
        };
        assert!(decision_instruction(&input).is_err());
        input.resolved_limits = Some(global.resolve_task(&input).unwrap());
        assert!(decision_instruction(&input).is_err());
        let mut native = global.execution.native.clone();
        native.context_bytes = input.instruction.len()
            + DECISION_INSTRUCTION.len()
            + DECISION_SCHEMA_SEPARATOR.len()
            + serde_json::to_vec(input.output_schema.as_ref().unwrap())
                .unwrap()
                .len();
        input.definition.limit_overrides = Some(Box::new(crate::limits::ExecutionLimitOverrides {
            native: Some(native),
            coding: None,
            collaboration: None,
            memory_notice: None,
        }));
        input.resolved_limits = Some(global.resolve_task(&input).unwrap());
        assert!(decision_instruction(&input).is_ok());
    }
    #[test]
    // Traceability scope: Pure accepted-scope validation: equal task limits, explicit target and no child conversion; no execution or session transaction is claimed.
    //= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
    //= type=test
    //# A task reassignment MUST preserve the task's accepted deadline.
    fn reassignment_scope_preserves_task_obligations_without_a_child_budget() {
        let metadata = crate::limits::RuntimeLimits::seed().unwrap();
        let mut input = TaskInput {
            instruction_source: None, pull_request: None, resolved_limits: None, repository_read: None,
            instruction: "Keep this exact work".into(),
            definition: Definition { charter: None, id: "triage".into(), revision: "r1".into(),
                executor: ExecutorKind::Native, inference: Some(InferenceSettings::selected()),
                execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }), limit_overrides: None },
            repository: None, acceptance_commands: vec![], output_schema: Some(serde_json::json!({"type":"object"})),
            limits: metadata.task_defaults.at(0).unwrap(), dependencies: vec!["prerequisite".into()], native_workflow: None,
        };
        // These values cannot reserve one child plus two native requests.
        input.limits.model_requests = 1; input.limits.max_delegation_depth = 0;
        let mut coding = input.definition.clone(); coding.id = "coding".into(); coding.executor = ExecutorKind::Coding;
        let target = CodingTaskTemplate {
            instruction_source: None, pull_request: None, instruction_prefix: String::new(), definition: coding,
            repository: Repository { registration: None, url: "file:///repo".into(), start_commit: "a".repeat(40),
                checkpoint_remote: "origin".into(), pull_request_target: None },
            acceptance_commands: vec![vec!["check".into()]], dependencies: input.dependencies.clone(), limits: input.limits.clone(),
        };
        input.native_workflow = Some(Box::new(NativeWorkflow::Reassign { target: target.clone() }));
        input.resolved_limits = Some(metadata.resolve_task(&input).unwrap());
        let before = input.clone();
        validate_workflow(&input).unwrap(); crate::execution_profile::validate(&input).unwrap();
        assert!(template(&input).is_none()); assert_eq!(handoff_template(&input), Some(&target));
        assert_eq!(configured_template(&input), Some(&target));
        assert_eq!(coding_child(&input, "new instruction").unwrap_err(), "native_workflow_missing");
        assert_eq!(input, before);
        let mut remaining = input.clone();
        remaining.limits.model_requests = 0;
        validate_workflow(&remaining).unwrap();
        assert_eq!(handoff_template(&remaining).unwrap().limits, input.limits);
        assert_eq!(remaining.limits.model_requests, 0, "validation never refunds consumed requests");
        for field in ["prefix", "dependencies", "limits", "request_allowance", "executor", "credentials"] {
            let mut changed = input.clone();
            let Some(NativeWorkflow::Reassign { target }) = changed.native_workflow.as_deref_mut() else { unreachable!() };
            match field {
                "prefix" => target.instruction_prefix = "Replace the instruction".into(),
                "dependencies" => target.dependencies.clear(),
                "limits" => target.limits.deadline_unix_ms += 1,
                "request_allowance" => target.limits.model_requests = 0,
                "executor" => target.definition.executor = ExecutorKind::Native,
                "credentials" => target.repository.url = "https://user:secret@example.com/repo".into(),
                _ => unreachable!(),
            }
            assert!(validate_workflow(&changed).is_err(), "{field}");
        }
    }

}
