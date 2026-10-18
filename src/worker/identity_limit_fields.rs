//! Pre-replay and pre-marker admission using the ordinary bounded typed readers.
use super::*;
use crate::protocol::identity_limit_fields as vocabulary;

pub(super) fn admit(state: &Path, limits: &RuntimeLimits) -> Result<(), String> {
    if handoff::admits_identity_limit_fields(state, &limits.worker)? { return Ok(()); }
    if vocabulary::metadata_uses(limits) {
        return Err("identity limit field policy requires worker20".into());
    }
    let receipts = command_receipts(state, limits)?;
    if receipts.iter().any(|(_, r)| vocabulary::command_uses(&r.command)) {
        return Err("accepted identity limit field policy requires worker20".into());
    }
    handoff::admit_retained_identity_limit_fields(state, limits)?;
    admit_native_journals(state, limits, |journal|
        vocabulary::metadata_uses(&journal.limits)
            || vocabulary::operation_uses(&journal.request)
            || journal.outcome.as_ref().is_some_and(vocabulary::outcome_uses))?;
    if mutations::read(&state.join("mutations/await.json"), limits)?.is_some_and(|journal|
        vocabulary::operation_uses(&journal.request)
            || journal.outcome.as_ref().is_some_and(vocabulary::outcome_uses)) {
        return Err("retained await identity limit fields require worker20".into());
    }
    for (_, receipt) in receipts {
        let WorkerCommand::RunAttempt { attempt, task, .. } = receipt.command else { continue; };
        if task.definition.executor != ExecutorKind::Coding { continue; }
        let metadata = &task.resolved_limits.as_ref().ok_or("accepted metadata absent")?.global_metadata;
        let config = codex_mcp::Config {
            instruction_source: task.instruction_source.clone(),
            execution_profile: task.definition.execution_profile.as_ref().filter(|p| matches!(p, ExecutionProfile::CollaborationV2 { .. })).cloned(),
            controller_url: String::new(), caller: collaboration_caller(&attempt),
            deadline_unix_ms: attempt.deadline_unix_ms.min(task.limits.deadline_unix_ms),
            state: codex::state_path(&state.join("codex"), &attempt.id),
            runtime_limits: (**metadata).clone(),
            limits: task.execution_limits()?.collaboration.clone(), worker_limits: limits.worker.clone(),
        };
        codex_mcp::admit_identity_limit_fields_from_retained_config(config)?;
    }
    Ok(())
}
