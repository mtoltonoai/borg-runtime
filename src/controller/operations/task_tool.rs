//! Current-attempt binding for the one native tool observation. The existing
//! immutable report receipt is the evidence; generic progress is not authority.
use super::*;
use crate::task_tool as contract;

fn observation(task: &Task, report: &Report) -> Result<contract::ToolObservation> {
    let Some(a) = task.attempt.as_ref() else {
        return refuse("stale_generation", "native tool lacks current attempt");
    };
    if report.report_id != contract::report_id(&a.id)
        || report.task_id != task.id
        || report.attempt_id != a.id
        || report.generation != a.generation
        || report.worker_id != a.worker_id
        || report.incarnation != a.worker_incarnation
    {
        return refuse(
            "source_mismatch",
            "native tool report belongs to another owner",
        );
    }
    let Observation::Progress {
        text,
        output,
        truncated,
    } = &report.observation
    else {
        return refuse(
            "source_mismatch",
            "native tool evidence is not its progress observation",
        );
    };
    if text != contract::PROGRESS_TEXT || *truncated {
        return refuse(
            "source_mismatch",
            "native tool observation is altered or truncated",
        );
    }
    let value = contract::decode(output, task.input.limits.output_bytes, &task.input.execution_limits().map_err(|reason| Fault::Refused("invalid_limits", reason))?.native).map_err(|_| {
        Fault::Refused(
            "source_mismatch",
            "native tool observation is malformed".into(),
        )
    })?;
    if !contract::matches_task(&value, task) {
        return refuse(
            "source_mismatch",
            "native tool observation differs from current task",
        );
    }
    Ok(value)
}

//= spec/impl/v2/implementation.md#the-native-executor-is-bounded
//# A native tool invocation MUST be restricted to the task's configured tool interface.
pub(super) fn progress(tx: &Connection, task: &Task, report: &Report, now: u64) -> Result<()> {
    let Some(a) = task.attempt.as_ref() else {
        return Ok(());
    };
    if report.report_id != contract::report_id(&a.id) {
        return Ok(());
    }
    if !contract::enabled(&task.input) {
        return refuse("invalid_transition", "native task tool is not configured");
    }
    if task.status != TaskStatus::Active
        || a.stopped
        || a.uncertain
        || a.phase == AttemptPhase::StopRequested
        || now >= task.input.limits.deadline_unix_ms
    {
        return refuse("invalid_transition", "native tool owner is not runnable");
    }
    let value = observation(task, report)?;
    let requests: u32 = tx.query_row(
        "SELECT model_requests FROM attempts WHERE id=?1",
        [&a.id],
        |r| r.get(0),
    )?;
    if requests != 1
        || value.task.observed_controller_revision > super::super::metadata::current(tx)?
    {
        return refuse(
            "source_mismatch",
            "native tool read lacks its original request/reservation order",
        );
    }
    Ok(())
}

pub(super) fn acceptance(tx: &Connection, task: &Task, validation: &[String]) -> Result<()> {
    if !contract::enabled(&task.input) {
        return Ok(());
    }
    let a = task
        .attempt
        .as_ref()
        .ok_or_else(|| Fault::Store("native tool attempt missing".into()))?;
    if !a.stopped || a.uncertain {
        return refuse(
            "invalid_transition",
            "native tool completion needs verified local stop",
        );
    }
    let requests: u32 = tx.query_row(
        "SELECT model_requests FROM attempts WHERE id=?1",
        [&a.id],
        |r| r.get(0),
    )?;
    if requests != 2 {
        return refuse(
            "usage_unavailable",
            "native tool workflow requires exactly two reserved requests",
        );
    }
    let receipt =
        super::super::metadata::read(tx, &contract::report_id(&a.id))?.ok_or_else(|| {
            Fault::Refused(
                "source_mismatch",
                "native tool observation was not committed".into(),
            )
        })?;
    let Operation::ObserveWorker { report } = receipt.request else {
        return refuse(
            "source_mismatch",
            "native tool receipt has another operation",
        );
    };
    let value = observation(task, &report)?;
    let Outcome::Ok {
        data: Data::Task(observed),
    } = receipt.outcome
    else {
        return refuse("source_mismatch", "native tool report was not accepted");
    };
    if observed.status != TaskStatus::Active
        || !contract::matches_task(&value, &observed)
        || observed
            .attempt
            .as_ref()
            .is_none_or(|a| a.stopped || a.uncertain)
        || value.task.observed_controller_revision >= receipt.commit.controller_revision
        || !validation
            .iter()
            .any(|entry| entry == &contract::evidence(&a.id))
    {
        return refuse(
            "source_mismatch",
            "native tool acceptance lacks its exact retained observation",
        );
    }
    Ok(())
}
