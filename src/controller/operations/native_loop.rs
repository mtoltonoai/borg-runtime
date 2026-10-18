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
/// Bound every supplied evidence list before building its index. Work follows
/// actual entries, never the configured u32 request allowance.
fn bound_evidence(tx: &Connection<'_>, value: &impl serde::Serialize) -> Result<()> {
    bounded_json_size(value, tx.limits.transport.body_bytes)
        .map(|_| ()).map_err(|_| Fault::Refused("source_mismatch", "native evidence exceeds transport.body_bytes".into()))
}
fn before_completion(revision: u64, completion: Option<u64>) -> Result<()> {
    if completion.is_some_and(|end| revision >= end) {
        return refuse("source_mismatch", "native proof does not precede original completion");
    }
    Ok(())
}
fn terminal_diagnostic(tx: &Connection<'_>, caller: &Caller, index: u32,
    expected: Option<&contract::NativeRequestEvidence>, completion: Option<u64>) -> Result<()> {
    use contract::{NativeRequestEvidence as Kind, NativeRequestObservedOutcome as Observed};
    let Some((observed, revision)) =
        super::super::native_request_diagnostics::require_terminal(tx, caller, index)? else {
            // Historical unmarked charges retain their original proof rules.
            return Ok(());
        };
    let fits = match expected {
        Some(Kind::ConfirmedRejection { .. }) => matches!(observed, Observed::KnownRejected { .. }),
        Some(Kind::OutputCap { .. }) => matches!(observed, Observed::OutputCapped),
        _ => matches!(observed, Observed::Completed),
    };
    if !fits { return refuse("source_mismatch", "native diagnostic terminal kind differs from completion evidence"); }
    before_completion(revision, completion)
}
fn no_rejection(tx: &Connection<'_>, caller: &Caller, index: u32) -> Result<()> {
    if super::native_retry::confirmed_rejection(tx, caller, index)?.is_some()
        || super::native_format::confirmed(tx, caller, index)?.is_some()
        || super::native_output_cap::confirmed(tx, caller, index)?.is_some() {
        return refuse("source_mismatch", "confirmed rejected request is not an ordinary response");
    }
    Ok(())
}
fn compaction_proof(tx: &Connection<'_>, caller: &Caller, index: u32,
    operation_id: &str, next: Option<u64>, completion: Option<u64>) -> Result<u64> {
    no_rejection(tx, caller, index)?;
    let run = super::super::charters::completed_compaction(tx, caller, operation_id, index)?;
    let begin = super::super::metadata::read(tx, operation_id)?.ok_or_else(|| Fault::Refused("source_mismatch", "compaction begin receipt missing".into()))?;
    let reply_id = &run.reply.as_ref().ok_or_else(|| Fault::Refused("source_mismatch", "compaction reply missing".into()))?.reply_id;
    let reply = super::super::metadata::read(tx, reply_id)?.ok_or_else(|| Fault::Refused("source_mismatch", "compaction reply receipt missing".into()))?;
    if begin.commit.controller_revision <= reservation(tx, caller, index)?
        || reply.commit.controller_revision <= begin.commit.controller_revision
        || next.is_some_and(|end| reply.commit.controller_revision >= end) {
        return refuse("source_mismatch", "compaction original charge/begin/reply order differs");
    }
    before_completion(reply.commit.controller_revision, completion)?;
    Ok(reply.commit.controller_revision)
}

/// A finite explicit rejection chain. It proves original rejected occurrences,
/// not successful input consumption, a model reply, or a grant to retry.
pub(crate) fn validate_rejection_chain(tx: &Connection<'_>, caller: &Caller,
    first_index: u32, entries: &[contract::NativeRequestEvidence]) -> Result<(u32, u64)> {
    bound_evidence(tx, &entries)?;
    if entries.is_empty() || first_index == 0 { return refuse("source_mismatch", "native rejection chain is empty or starts at zero"); }
    let mut expected = first_index;
    let mut last_revision = 0;
    for (position, entry) in entries.iter().enumerate() {
        let contract::NativeRequestEvidence::ConfirmedRejection { request_index, operation_id } = entry else {
            return refuse("source_mismatch", "native rejection chain contains another request kind");
        };
        if *request_index != expected { return refuse("source_mismatch", "native rejection chain has a gap or duplicate"); }
        let charge = reservation(tx, caller, expected)?;
        let proof = super::native_retry::confirmed_rejection(tx, caller, expected)?.ok_or_else(|| Fault::Refused("source_mismatch", "native rejection proof absent".into()))?;
        if !proof.rejection.kind.same_input_retryable() || proof.operation_id != *operation_id || proof.request_index != expected
            || proof.controller_revision <= charge || (position > 0 && charge <= last_revision) {
            return refuse("source_mismatch", "native rejection chain original identity or order differs");
        }
        last_revision = proof.controller_revision;
        if position + 1 < entries.len() { expected = expected.checked_add(1).ok_or_else(|| Fault::Refused("source_mismatch", "native rejection index overflow".into()))?; }
    }
    Ok((expected, last_revision))
}

/// Before governance evaluates an already observed final response, an explicit
/// bounded chain may contain rejected summary attempts followed by one completed
/// summary. This cannot rebase a later Begin target or fabricate a tool call.
/// Root re-exports this through controller::operations for M's original join.
pub(crate) fn validate_completion_gap(tx: &Connection<'_>, caller: &Caller,
    final_index: u32, entries: &[contract::NativeRequestEvidence]) -> Result<(u32, u64)> {
    bound_evidence(tx, &entries)?;
    let original_charge = reservation(tx, caller, final_index)?;
    no_rejection(tx, caller, final_index)?;
    if entries.is_empty() { return Ok((final_index, original_charge)); }
    // Compaction is charter-scoped, so a genuine completed ordinary no-tool
    // occurrence exists independently of the governance evidence.
    let original = super::super::charters::completed_native_turn(tx, caller, final_index)?;
    if !original.reply.as_ref().ok_or_else(|| Fault::Refused("source_mismatch", "original final reply absent".into()))?.observation.tool_call_ids.is_empty() {
        return refuse("source_mismatch", "completion gap follows a tool response");
    }
    let mut expected = final_index;
    let mut previous = original_charge;
    for (position, entry) in entries.iter().enumerate() {
        expected = expected.checked_add(1).ok_or_else(|| Fault::Refused("source_mismatch", "native gap index overflow".into()))?;
        if entry.request_index() != expected || reservation(tx, caller, expected)? <= previous {
            return refuse("source_mismatch", "native completion gap is not the original consecutive charge sequence");
        }
        match entry {
            contract::NativeRequestEvidence::ConfirmedRejection { operation_id, .. } if position + 1 < entries.len() => {
                let proof = super::native_retry::confirmed_rejection(tx, caller, expected)?.ok_or_else(|| Fault::Refused("source_mismatch", "rejected compaction proof absent".into()))?;
                if !proof.rejection.kind.same_input_retryable() || proof.operation_id != *operation_id { return refuse("source_mismatch", "rejected compaction report differs"); }
                super::super::charters::rejected_compaction(tx, caller, expected, operation_id)?;
                previous = proof.controller_revision;
            }
            contract::NativeRequestEvidence::Compaction { operation_id, .. } if position + 1 == entries.len() => {
                previous = compaction_proof(tx, caller, expected, operation_id, None, None)?;
            }
            _ => return refuse("source_mismatch", "completion gap must end with one actual compaction after only its confirmed rejected attempts"),
        }
    }
    Ok((expected, previous))
}

// Exactly one original kind for every charge other than the explicitly retained
// final response. The typed non-tool vector and call groups are individually
// ordered; overlapping or missing indices fail before any proof is accepted.
fn evidence(tx: &Connection<'_>, task: &Task, total: u32, final_response: Option<u32>,
    calls: &[NativeCallEvidence], requests: &[contract::NativeRequestEvidence], completion: Option<u64>) -> Result<()> {
    use contract::NativeRequestEvidence as Kind;
    let c = owner(task)?;
    let final_index = final_response.unwrap_or(total);
    // Preserve the exact historical calls-only bound when no new field is used.
    let bounded = if requests.is_empty() && final_response.is_none() {
        bounded_json_size(&calls, task.input.limits.result_bytes)
    } else {
        bounded_json_size(&(calls, requests, final_response), task.input.limits.result_bytes)
    };
    if total == 0 || final_index == 0 || final_index > total || bounded.is_err() {
        return refuse("source_mismatch", "native request indices or evidence exceed accepted result bounds");
    }
    let policy = &task.input.execution_limits().map_err(|e| Fault::Refused("invalid_limits", e))?.native;
    let used: u32 = tx.query_row("SELECT model_requests FROM attempts WHERE id=?1", [&c.attempt_id], |r| r.get(0))?;
    if used != total { return refuse("usage_unavailable", "native cumulative reservation differs"); }
    before_completion(reservation(tx, &c, total)?, completion)?;
    terminal_diagnostic(tx, &c, final_index, None, completion)?;
    no_rejection(tx, &c, final_index)?;
    if task.input.definition.charter.is_some() {
        let final_turn = super::super::charters::completed_native_turn(tx, &c, final_index)?;
        if !final_turn.reply.as_ref().ok_or_else(|| Fault::Refused("source_mismatch", "final charter reply missing".into()))?.observation.tool_call_ids.is_empty() {
            return refuse("source_mismatch", "final native response contains owned calls");
        }
        let closure = super::super::metadata::read(tx, final_turn.completed_by.as_ref().unwrap())?.ok_or_else(|| Fault::Refused("source_mismatch", "final ordinary closure missing".into()))?;
        before_completion(closure.commit.controller_revision, completion)?;
    }
    // Non-charter legacy final replies retain the existing trusted completion
    // producer. Absence of a rejection is not claimed as successful-reply proof.
    if final_index != total {
        super::super::governance_completion::validate_native_final(tx, task, final_index, total)?;
    }
    let mut groups = std::collections::BTreeMap::<u32, Vec<&NativeCallEvidence>>::new();
    let mut previous_call = (0, 0);
    for call in calls {
        if call.request_index >= final_index || (call.request_index, call.call_index) <= previous_call {
            return refuse("source_mismatch", "native call group order or final response association differs");
        }
        previous_call = (call.request_index, call.call_index);
        groups.entry(call.request_index).or_default().push(call);
    }
    let mut typed = std::collections::BTreeMap::new();
    let mut previous_request = 0;
    for entry in requests {
        let index = entry.request_index();
        if index <= previous_request || index == final_index || index > total || groups.contains_key(&index)
            || typed.insert(index, entry).is_some() {
            return refuse("source_mismatch", "native request kind duplicated, unordered or assigned to final response");
        }
        previous_request = index;
        groups.insert(index, Vec::new());
    }
    if groups.len().checked_add(1) != Some(total as usize) { return refuse("source_mismatch", "native charged request kind missing"); }
    let mut logical = std::collections::HashSet::new();
    let mut expected = 0u32;
    for (index, group) in &groups {
        expected = expected.checked_add(1).ok_or_else(|| Fault::Refused("source_mismatch", "native evidence index overflow".into()))?;
        if expected == final_index { expected = expected.checked_add(1).ok_or_else(|| Fault::Refused("source_mismatch", "native evidence index overflow".into()))?; }
        if *index != expected || *index == 0 { return refuse("source_mismatch", "native charged request kinds are not contiguous"); }
        let charge = reservation(tx, &c, *index)?;
        terminal_diagnostic(tx, &c, *index, typed.get(index).copied(), completion)?;
        let next = if *index < total { Some(reservation(tx, &c, index.checked_add(1).unwrap())?) } else { None };
        if let Some(entry) = typed.get(index) {
            match entry {
                Kind::FormatRepair { operation_id, .. } => {
                    let proof = super::native_format::confirmed(tx, &c, *index)?.ok_or_else(|| Fault::Refused("source_mismatch", "native format proof absent".into()))?;
                    if proof.operation_id != *operation_id || proof.controller_revision <= charge
                        || next.is_none_or(|end| proof.controller_revision >= end) {
                        return refuse("source_mismatch", "native format repair original identity/order differs");
                    }
                    before_completion(proof.controller_revision, completion)?;
                }
                Kind::OutputCap { operation_id, .. } => {
                    let proof=super::native_output_cap::confirmed(tx,&c,*index)?.ok_or_else(||Fault::Refused("source_mismatch","output cap proof absent".into()))?;
                    if proof.operation_id!=*operation_id || proof.controller_revision<=charge || next.is_none_or(|end|proof.controller_revision>=end) {return refuse("source_mismatch","output cap original identity/order differs");}
                    if task.input.definition.charter.is_some() {super::super::charters::completed_native_turn(tx,&c,*index)?;}
                    before_completion(proof.controller_revision,completion)?;
                }
                Kind::Compaction { operation_id, .. } => { compaction_proof(tx, &c, *index, operation_id, next, completion)?; }
                Kind::ConfirmedRejection { operation_id, .. } => {
                    let proof = super::native_retry::confirmed_rejection(tx, &c, *index)?.ok_or_else(|| Fault::Refused("source_mismatch", "native rejection proof absent".into()))?;
                    if !proof.rejection.kind.same_input_retryable() || proof.operation_id != *operation_id || proof.controller_revision <= charge
                        || next.is_some_and(|end| proof.controller_revision >= end) {
                        return refuse("source_mismatch", "native rejection original identity/order differs");
                    }
                    if task.input.definition.charter.is_some() {
                        super::super::charters::rejected_request_kind(tx, &c, *index, operation_id)?;
                    }
                    before_completion(proof.controller_revision, completion)?;
                }
                Kind::CompletionContinuation { evidence } => {
                    no_rejection(tx, &c, *index)?;
                    if task.input.definition.charter.is_some() {
                        let turn = super::super::charters::completed_native_turn(tx, &c, *index)?;
                        if !turn.reply.as_ref().unwrap().observation.tool_call_ids.is_empty() { return refuse("source_mismatch", "governed continuation substitutes for a tool response"); }
                    }
                    // M checks the rejected ordinary response, actual decision,
                    // fixed Begin target and ACK after real input consumption.
                    // Its ACK necessarily follows the next reservation; it is
                    // not subject to the tool-result-before-next-charge rule.
                    super::super::governance_completion::validate_native_request(tx, task, evidence)?;
                    let ack = super::super::metadata::read(tx, &evidence.acknowledgement_operation_id)?.ok_or_else(|| Fault::Refused("source_mismatch", "native continuation ACK absent".into()))?;
                    before_completion(ack.commit.controller_revision, completion)?;
                }
            }
            continue;
        }
        no_rejection(tx, &c, *index)?;
        if group.is_empty() || group.len() > policy.function_calls_per_response { return refuse("source_mismatch", "native call group exceeds accepted bound"); }
        if task.input.definition.charter.is_some() {
            let turn = super::super::charters::completed_native_turn(tx, &c, *index)?;
            let ids = &turn.reply.as_ref().unwrap().observation.tool_call_ids;
            if ids.len() != group.len() || ids.iter().zip(group).any(|(id, call)| id != &call.logical_call_id) {
                return refuse("source_mismatch", "native group differs from its original charter reply");
            }
        }
        let mut previous = charge;
        let next = next.ok_or_else(|| Fault::Refused("source_mismatch", "native tool group follows final request".into()))?;
        for (ordinal, call) in group.iter().enumerate() {
            if call.call_index as usize != ordinal + 1 || call.operation_id != contract::call_operation_id(&c.attempt_id, *index, call.call_index)
                || !valid_id(tx.limits, &call.logical_call_id) || !logical.insert(&call.logical_call_id) {
                return refuse("source_mismatch", "native call identity or ordinal differs");
            }
            let receipt = super::super::metadata::read_retained(tx, &call.operation_id)?.ok_or_else(|| Fault::Refused("source_mismatch", "native call receipt missing".into()))?;
            let Operation::CallCollaboration { call: original } = &receipt.request else { return refuse("source_mismatch", "native evidence is not an actual collaboration occurrence"); };
            if original.caller != c || original.logical_call_id != call.logical_call_id || original.operation_id != call.operation_id
                || receipt.commit.controller_revision <= previous || receipt.commit.controller_revision >= next
                || !matches!(receipt.outcome, super::super::metadata::RetainedOutcome::Inline(Outcome::Ok { .. } | Outcome::Refused { .. }) | super::super::metadata::RetainedOutcome::ReadReference(_))
                || (original.tool == "collab_task_delegate" && matches!(receipt.outcome, super::super::metadata::RetainedOutcome::Inline(Outcome::Ok { .. }))) {
                return refuse("source_mismatch", "native call owner/outcome/reservation order differs");
            }
            previous = receipt.commit.controller_revision;
        }
    }
    // Only an actual canonical completion gap may follow the final answer.
    let suffix = &requests[requests.partition_point(|v| v.request_index() < final_index)..];
    let (through, _) = validate_completion_gap(tx, &c, final_index, suffix)?;
    if through != total { return refuse("source_mismatch", "native final response does not account for trailing charged work"); }
    Ok(())
}

pub(super) fn completed(
    tx: &Connection,
    task: &Task,
    report: &Report,
    total: u32,
    calls: &[NativeCallEvidence],
    requests: &[contract::NativeRequestEvidence],
    final_response_index: Option<u32>,
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
    evidence(tx, task, total, final_response_index, calls, requests, None)
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
        requests,
        final_response_index,
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
    evidence(tx, task, *model_requests, *final_response_index, calls, requests, Some(receipt.commit.controller_revision))
}
