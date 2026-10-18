//! Root submission drafts resolve routing only after canonical receipt replay.
//! Accepted task/session records continue to carry an explicit immutable definition.
use super::{database::StoreContext as Connection, database::*};
use crate::protocol::*;

//= spec/impl/v2/implementation.md#default-task-assignment
//# A new root task submitted without an explicit agent MUST resolve the configured default agent identity.
//= spec/impl/v2/implementation.md#default-task-assignment
//# An explicitly selected task agent MUST take precedence over the default agent identity.
pub(super) fn resolve(tx: &Connection<'_>, draft: &TaskDraft) -> Result<TaskInput> {
    let definition = match &draft.definition {
        Some(explicit) => explicit.clone(),
        None => {
            //= spec/impl/v2/implementation.md#default-task-assignment
            //# The default task agent MUST be selected through metadata.
            let id = tx.limits.task_defaults.agent_id.as_deref().ok_or_else(|| {
                Fault::Refused(
                    "default_agent_not_configured",
                    "task_defaults.agent_id is not configured".into(),
                )
            })?;
            //= spec/impl/v2/implementation.md#default-task-assignment
            //# Default task assignment MUST refuse when no configured active registered identity can be resolved.
            super::agents::selected_active_definition(tx, id).map_err(|fault| match fault {
                Fault::Refused("not_found", _) => Fault::Refused(
                    "default_agent_not_found",
                    "task_defaults.agent_id does not identify a registered agent".into(),
                ),
                Fault::Refused("agent_retired", _) => Fault::Refused(
                    "agent_retired",
                    "task_defaults.agent_id identifies a retired agent".into(),
                ),
                other => other,
            })?
        }
    };
    Ok(draft.resolve(definition))
}

fn selected_input(input: &TaskInput) -> bool {
    matches!(input.native_workflow.as_deref(), Some(NativeWorkflow::Reassign { .. })) || input
        .resolved_limits
        .as_ref()
        .is_some_and(|snapshot| snapshot.global_metadata.task_defaults.agent_id.is_some())
}
fn selected_command(command: &WorkerCommand) -> bool {
    matches!(command, WorkerCommand::RunAttempt { task, .. } if selected_input(task))
}
fn selected_task(task: &Task) -> bool { task.pending_assignment.is_some() || selected_input(&task.input) }
fn selected_data(data: &Data) -> bool {
    match data {
        Data::Task(task) => selected_task(task),
        Data::Tasks(tasks) => tasks.iter().any(|task| selected_task(task)),
        Data::Commands(commands) => commands.iter().any(selected_command),
        Data::WorkMutation(result) => selected_task(&result.view.task),
        Data::WorkQuery(crate::collaboration::work::WorkQueryResult::Task(view)) => {
            selected_task(&view.task)
        }
        Data::Collaboration(crate::collaboration::QueryResult::SessionInput { .. }) => true,
        Data::Collaboration(crate::collaboration::QueryResult::Limits { global, execution }) => {
            global.task_defaults.agent_id.is_some()
                || execution.as_ref().is_some_and(|snapshot| {
                    snapshot.global_metadata.task_defaults.agent_id.is_some()
                })
        }
        _ => false,
    }
}

/// Schema24 fences new routing vocabulary before its owned lineage migration. Never rewrite an older receipt to make
/// an omitted request appear explicit, or resolve historical default routing.
pub(super) fn validate_existing(
    tx: &Connection<'_>,
    cfg: &super::ControllerConfig,
    version: i64,
) -> Result<()> {
    if version >= super::application_schema::DEFAULT_ASSIGNMENT_VERSION {
        return Ok(());
    }
    let bound = cfg
        .max_receipts
        .checked_add(cfg.max_cleanup_receipts)
        .ok_or_else(|| Fault::Store("default-assignment inventory overflow".into()))?;
    let mut count = 0usize;
    let mut query = tx.prepare("SELECT id FROM receipts ORDER BY id")?;
    let mut rows = query.query([])?;
    while let Some(row) = rows.next()? {
        count += 1;
        if count > bound {
            return Err(Fault::Store(
                "default-assignment receipts exceed configured bound".into(),
            ));
        }
        let id: String = row.get(0)?;
        let record = super::metadata::read(tx, &id)?
            .ok_or_else(|| Fault::Store("default-assignment receipt disappeared".into()))?;
        let selected_request = match &record.request {
            Operation::SubmitTaskDraft { .. } => true,
            Operation::SubmitTask { input, .. } | Operation::DelegateTask { child: input, .. } => {
                selected_input(input)
            }
            _ => false,
        };
        if selected_request
            || matches!(&record.outcome, Outcome::Ok { data } if selected_data(data))
        {
            return Err(Fault::Store(
                "historical schema contains default-assignment request vocabulary".into(),
            ));
        }
    }
    let mut query=tx.prepare("SELECT length(CAST(body AS BLOB)), CASE WHEN length(CAST(body AS BLOB))<=?1 THEN body ELSE NULL END FROM commands ORDER BY seq")?;
    let mut rows = query.query([sql_bound(tx.limits.transport.body_bytes)?])?;
    let command_bound = bound
        .checked_add(cfg.max_tasks)
        .and_then(|n| n.checked_mul(2))
        .ok_or_else(|| Fault::Store("default-assignment command bound overflow".into()))?;
    count = 0;
    while let Some(row) = rows.next()? {
        count += 1;
        if count > command_bound || unsigned(row.get(0)?)? > u64::try_from(tx.limits.transport.body_bytes).map_err(|_|Fault::Store("transport body bound exceeds u64".into()))? {
            return Err(Fault::Store(
                "default-assignment command inventory exceeds bound".into(),
            ));
        }
        let command: WorkerCommand = decode(&row.get::<_, String>(1)?)?;
        if selected_command(&command) {
            return Err(Fault::Store(
                "historical command contains default-assignment metadata".into(),
            ));
        }
    }
    count = 0;
    let mut query = tx.prepare("SELECT id FROM tasks ORDER BY seq")?;
    let mut rows = query.query([])?;
    while let Some(row) = rows.next()? {
        let id: String = row.get(0)?;
        count += 1;
        if count > cfg.max_tasks {
            return Err(Fault::Store(
                "default-assignment task inventory exceeds bound".into(),
            ));
        }
        let task = load(tx, &id)?;
        if selected_task(&task.task) {
            return Err(Fault::Store(
                "historical schema contains default-assignment metadata".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "task_submission_tests.rs"]
mod tests;
