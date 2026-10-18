//! Original typed completion/format fact. No provider content selects this
//! classification, supplies a rule, or grants a retry. The trusted worker only
//! emits it after complete same-binary validation and original usage recording.
use super::*;
use crate::{mcp::Caller, native_loop_contract as contract};

pub(crate) struct Proof { pub operation_id: String, pub controller_revision: u64 }
fn caller(report: &Report) -> Caller {
    Caller { task_id: report.task_id.clone(), attempt_id: report.attempt_id.clone(),
        generation: report.generation, worker_id: report.worker_id.clone(), incarnation: report.incarnation.clone() }
}
fn subject(task: &Task, c: &Caller) -> Result<()> {
    let snapshot = task.input.resolved_limits.as_ref().ok_or_else(|| Fault::Refused("source_mismatch", "format accepted policy absent".into()))?;
    snapshot.validate_for(&task.input).map_err(Fault::Store)?;
    if task.id != c.task_id || task.input.definition.executor != ExecutorKind::Native
        || task.input.definition.execution_profile.is_none() || snapshot.global_metadata.native_retry.is_none()
        || !task.attempt.as_ref().is_some_and(|a| a.id == c.attempt_id && a.generation == c.generation
            && a.worker_id == c.worker_id && a.worker_incarnation == c.incarnation) {
        return refuse("source_mismatch", "format original owner or selected policy differs");
    }
    Ok(())
}
fn body(task: &Task, report: &Report) -> Result<u32> {
    let Observation::NativeFormatFailure { request_index, issue, request_binding, output_binding } = &report.observation else {
        return refuse("source_mismatch", "format occurrence kind differs");
    };
    subject(task, &caller(report))?;
    let policy = &task.input.execution_limits().map_err(Fault::Store)?.native;
    request_binding.validate(policy.request_bytes).map_err(Fault::Store)?;
    output_binding.validate(task.input.limits.output_bytes.max(policy.context_bytes)).map_err(Fault::Store)?;
    if *request_index == 0 || report.report_id != contract::format_failure_id(&report.attempt_id, *request_index)
        || matches!(issue, crate::native::FormatIssue::InvalidJson {line, column} if *line > task.input.limits.output_bytes as u64 + 1 || *column > task.input.limits.output_bytes as u64 + 1) {
        return refuse("source_mismatch", "format occurrence identity or position differs");
    }
    Ok(*request_index)
}
fn original_response(tx: &Connection<'_>, task: &Task, c: &Caller, index: u32) -> Result<u64> {
    let charge = super::native_retry::reservation(tx, c, index)?;
    if super::super::metadata::read(tx,&contract::output_cap_id(&c.attempt_id,index))?.is_some() {return refuse("source_mismatch","format conflicts with output cap");}
    if super::native_retry::confirmed_rejection(tx, c, index)?.is_some() {
        return refuse("source_mismatch", "format occurrence conflicts with original provider rejection");
    }
    if task.input.definition.charter.is_some() {
        let turn = super::super::charters::completed_native_turn(tx, c, index)?;
        if !turn.reply.as_ref().ok_or_else(|| Fault::Refused("source_mismatch", "format charter reply absent".into()))?.observation.tool_call_ids.is_empty() {
            return refuse("source_mismatch", "format occurrence substitutes for a tool response");
        }
    }
    Ok(charge)
}
pub(crate) fn validate_report(tx: &Connection<'_>, task: &Task, report: &Report) -> Result<()> {
    let index = body(task, report)?;
    original_response(tx, task, &caller(report), index)?;
    let charged: u32 = tx.query_row("SELECT model_requests FROM attempts WHERE id=?1", [&report.attempt_id], |r| r.get(0))?;
    if charged != index { return refuse("source_mismatch", "format report must settle latest original charge"); }
    Ok(())
}
pub(crate) fn confirmed(tx: &Connection<'_>, c: &Caller, index: u32) -> Result<Option<Proof>> {
    let id = contract::format_failure_id(&c.attempt_id, index);
    let Some(record) = super::super::metadata::read(tx, &id)? else { return Ok(None) };
    let (Operation::ObserveWorker {report}, Outcome::Ok {data: Data::Task(task)}) = (&record.request, &record.outcome) else {
        return refuse("source_mismatch", "format report is not an original successful receipt");
    };
    if caller(report) != *c || body(task, report)? != index {
        return refuse("source_mismatch", "format receipt original subject differs");
    }
    let accepted = tx.with_limits(&task.input.resolved_limits.as_ref().unwrap().global_metadata);
    if original_response(&accepted, task, c, index)? >= record.commit.controller_revision {
        return refuse("source_mismatch", "format occurrence must follow original charge");
    }
    Ok(Some(Proof {operation_id: id, controller_revision: record.commit.controller_revision}))
}
