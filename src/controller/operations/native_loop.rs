//! Typed completion binds real canonical receipts and cumulative reservations.
//! An executor result or free-form progress cannot stand in for these records.
use super::*;
use crate::{mcp::Caller, native_loop_contract as contract};
fn enabled(task: &Task) -> bool {
    task.input.definition.executor == ExecutorKind::Native
        && task.input.definition.execution_profile.is_some()
}
fn owner(task: &Task) -> Result<Caller> {
    let a = task
        .attempt
        .as_ref()
        .ok_or_else(|| Fault::Store("native loop attempt missing".into()))?;
    Ok(Caller {
        task_id: task.id.clone(),
        attempt_id: a.id.clone(),
        generation: a.generation,
        worker_id: a.worker_id.clone(),
        incarnation: a.worker_incarnation.clone(),
    })
}
fn report_owner(r: &Report, c: &Caller) -> bool {
    r.task_id == c.task_id
        && r.attempt_id == c.attempt_id
        && r.generation == c.generation
        && r.worker_id == c.worker_id
        && r.incarnation == c.incarnation
}
fn reservation(tx: &Connection, c: &Caller, index: u32) -> Result<u64> {
    let id = contract::reservation_id(&c.attempt_id, index);
    let receipt = super::super::metadata::read(tx, &id)?.ok_or_else(|| {
        Fault::Refused("source_mismatch", "native loop reservation missing".into())
    })?;
    let Operation::ObserveWorker { report } = &receipt.request else {
        return refuse("source_mismatch", "native loop reservation is not a report");
    };
    if report.report_id != id
        || !report_owner(report, c)
        || !matches!(&report.observation,Observation::Usage{model_requests,..} if *model_requests==index)
        || !matches!(receipt.outcome,Outcome::Ok{data:Data::Task(ref t)} if t.id==c.task_id&&t.attempt.as_ref().is_some_and(|a|a.id==c.attempt_id&&!a.stopped&&!a.uncertain&&a.phase!=AttemptPhase::StopRequested))
    {
        return refuse(
            "source_mismatch",
            "native loop reservation body or accepted owner differs",
        );
    }
    Ok(receipt.commit.controller_revision)
}
fn evidence(tx: &Connection, task: &Task, total: u32, calls: &[NativeCallEvidence]) -> Result<()> {
    let limits = tx.limits;
    let c = owner(task)?;
    let policy = &task.input.execution_limits().map_err(|reason| Fault::Refused("invalid_limits", reason))?.native;
    if total == 0 { return refuse("source_mismatch", "native loop reservation or evidence bound differs"); }
    if bounded_json_size(&calls, task.input.limits.result_bytes).is_err() {
        return refuse("source_mismatch", "native loop reservation or evidence bound differs: task.input.limits.result_bytes");
    }
    let used: u32 = tx.query_row("SELECT model_requests FROM attempts WHERE id=?1", [&c.attempt_id], |r| r.get(0))?;
    if used != total { return refuse("usage_unavailable", "native loop cumulative reservation differs"); }
    let mut group = 0u32;
    let mut ordinal = 0u32;
    let mut previous = reservation(tx, &c, 1)?;
    let mut next = previous;
    let mut logical = std::collections::HashSet::new();
    for entry in calls {
        if entry.request_index != group {
            if entry.request_index != group.checked_add(1).ok_or_else(|| Fault::Store("native request index overflow".into()))?
                || entry.request_index >= total {
                return refuse("source_mismatch", "native request groups are not contiguous");
            }
            group = entry.request_index;
            ordinal = 0;
            previous = reservation(tx, &c, group)?;
            next = reservation(tx, &c, group + 1)?;
        }
        ordinal = ordinal.checked_add(1).ok_or_else(|| Fault::Store("native call index overflow".into()))?;
        if ordinal as usize > policy.function_calls_per_response {
            return refuse("source_mismatch", "native call identity, ordinal or accepted bound differs: execution.native.function_calls_per_response");
        }
        if entry.call_index != ordinal
            || entry.operation_id != contract::call_operation_id(&c.attempt_id, group, ordinal)
            || !valid_id(limits, &entry.logical_call_id) || !logical.insert(&entry.logical_call_id) {
            return refuse("source_mismatch", "native call identity, ordinal or accepted bound differs");
        }
        let receipt = super::super::metadata::read(tx, &entry.operation_id)?.ok_or_else(|| Fault::Refused("source_mismatch", "native call receipt missing".into()))?;
        let Operation::CallCollaboration { call } = &receipt.request else {
            return refuse("source_mismatch", "native evidence is not a collaboration occurrence");
        };
        if call.caller != c || call.logical_call_id != entry.logical_call_id
            || call.operation_id != entry.operation_id || receipt.commit.controller_revision <= previous
            || receipt.commit.controller_revision >= next
            || !matches!(receipt.outcome, Outcome::Ok { .. } | Outcome::Refused { .. })
            || (call.tool == "collab_task_delegate" && matches!(receipt.outcome, Outcome::Ok { .. })) {
            return refuse("source_mismatch", "native call owner/outcome/reservation order differs");
        }
        previous = receipt.commit.controller_revision;
    }
    // Every non-final request contributes a nonempty serial group. The final
    // request contributes no owned call. Validation work remains O(receipts),
    // never an unbounded walk over the configured u32 request allowance.
    if group != total - 1 { return refuse("source_mismatch", "native request group missing"); }
    Ok(())
}

pub(super) fn completed(
    tx: &Connection,
    task: &Task,
    report: &Report,
    total: u32,
    calls: &[NativeCallEvidence],
    now: u64,
) -> Result<()> {
    let c = owner(task)?;
    let a = task.attempt.as_ref().unwrap();
    if !enabled(task)
        || report.report_id != contract::completion_id(&c.attempt_id)
        || !report_owner(report, &c)
        || task.status != TaskStatus::Active
        || a.stopped
        || a.uncertain
        || a.phase == AttemptPhase::StopRequested
        || task.waiting_child_id.is_some()
        || now >= task.input.limits.deadline_unix_ms
    {
        return refuse(
            "invalid_transition",
            "native loop completion owner is not current",
        );
    }
    evidence(tx, task, total, calls)
}
pub(super) fn acceptance(tx: &Connection, task: &Task, validation: &[String]) -> Result<()> {
    if !enabled(task) {
        return Ok(());
    }
    let c = owner(task)?;
    let a = task.attempt.as_ref().unwrap();
    if !a.stopped || a.uncertain {
        return refuse(
            "invalid_transition",
            "native loop acceptance requires verified stop",
        );
    }
    let receipt =
        super::super::metadata::read(tx, &contract::completion_id(&a.id))?.ok_or_else(|| {
            Fault::Refused(
                "source_mismatch",
                "native loop completion receipt missing".into(),
            )
        })?;
    let Operation::ObserveWorker { report } = &receipt.request else {
        return refuse("source_mismatch", "native loop completion kind differs");
    };
    let Observation::NativeLoopCompleted {
        model_requests,
        calls,
    } = &report.observation
    else {
        return refuse("source_mismatch", "native loop completion evidence differs");
    };
    if report.report_id != contract::completion_id(&a.id)
        || !report_owner(report, &c)
        || !validation.iter().any(|v| v == &contract::evidence(&a.id))
        || !matches!(&receipt.outcome,Outcome::Ok{data:Data::Task(t)} if t.id==task.id&&t.attempt.as_ref().is_some_and(|a|a.id==c.attempt_id&&!a.stopped&&!a.uncertain&&a.phase!=AttemptPhase::StopRequested))
    {
        return refuse(
            "source_mismatch",
            "native loop acceptance lacks its exact accepted completion",
        );
    }
    if reservation(tx, &c, *model_requests)? >= receipt.commit.controller_revision {
        return refuse(
            "source_mismatch",
            "native loop completion precedes final reservation",
        );
    }
    evidence(tx, task, *model_requests, calls)
}
