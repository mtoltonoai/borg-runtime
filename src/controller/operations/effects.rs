use super::*;
use serde_json::{Value, json};

pub(super) fn apply(
    tx: &Connection,
    op: &Operation,
    cfg: &ControllerConfig,
    now: u64,
) -> Result<(Data, bool)> {
    let limits = tx.limits;
    match op {
        Operation::PrepareOperation {
            operation_id,
            task_id,
            attempt_id,
            generation,
            kind,
            intended,
        } => {
            let t = owned(tx, task_id, attempt_id, *generation, true)?;
            if now >= t.task.input.limits.deadline_unix_ms {
                return refuse("deadline", "attempt deadline expired");
            }
            if !matches!(kind.as_str(), "checkpoint" | "pull_request" | "merge") {
                return refuse("invalid_input", "unsupported external operation kind");
            }
            if !intended.is_object() {
                return refuse("invalid_input", "invalid or oversized external intent");
            }
            if bounded_json_size(intended, limits.controller.effect_value_bytes).is_err() {
                return refuse(
                    "invalid_input",
                    "invalid or oversized external intent: controller.effect_value_bytes",
                );
            }
            if kind == "checkpoint" {
                let cp = intended_checkpoint(operation_id, intended)?;
                check_checkpoint(limits, &t.task, &cp)?;
                let reused:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM external_operations WHERE json_extract(body,'$.kind')='checkpoint' AND json_extract(body,'$.intended.repository')=?1 AND json_extract(body,'$.intended.remote_ref')=?2)",params![cp.repository,cp.remote_ref],|r|r.get(0))?;
                if reused {
                    return refuse(
                        "operation_conflict",
                        "checkpoint remote reference already belongs to another publication",
                    );
                }
            }
            //= spec/impl/v2/implementation.md#checkpoint-publication-is-reconciled
            //# The publication record MUST identify the repository, exact commit and intended remote reference.
            let record = OperationRecord {
                id: operation_id.clone(),
                task_id: task_id.clone(),
                attempt_id: attempt_id.clone(),
                generation: *generation,
                kind: kind.clone(),
                intended: intended.clone(),
                state: "pending".into(),
                outcome: None,
            };
            final_deadline(&t.task, cfg, now)?;
            if !cleanup_available(tx, cfg, 1)? {
                return refuse("capacity", "no final settlement reservation available");
            }
            tx.execute(
                "INSERT INTO external_operations(id,body,prepare,finalization_reserved) VALUES(?1,?2,?3,1)",
                params![operation_id, encode(&record)?, encode(op)?],
            )?;
            history(tx, &t.task, "external_intent", &record)?;
            Ok((Data::Operation(record), true))
        }
        Operation::SettleOperation {
            settlement_id,
            operation_id,
            task_id,
            attempt_id,
            generation,
            outcome,
            checkpoint,
        } => {
            if !valid_id(limits, operation_id) || settlement_id == operation_id {
                return refuse("invalid_input", "invalid settlement");
            }
            if bounded_json_size(outcome, limits.controller.effect_value_bytes).is_err() {
                return refuse(
                    "invalid_input",
                    "invalid settlement: controller.effect_value_bytes",
                );
            }
            let original: Option<(String, Option<String>)> = tx
                .query_row(
                    "SELECT body,settlement FROM external_operations WHERE id=?1",
                    [operation_id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            let Some((body, settlement)) = original else {
                return refuse("not_found", "no original external operation");
            };
            let mut record: OperationRecord = decode(&body)?;
            if record.task_id != *task_id
                || record.attempt_id != *attempt_id
                || record.generation != *generation
            {
                return refuse(
                    "operation_conflict",
                    "settlement identity does not match original intent",
                );
            }
            if let Some(original) = settlement {
                if !same_settlement(&decode::<Operation>(&original)?, op) {
                    return refuse(
                        "operation_conflict",
                        "external operation already has a different settlement",
                    );
                }
                return Ok((Data::Operation(record), false));
            }
            //= spec/impl/v2/implementation.md#checkpoint-publication-is-reconciled
            //# A superseded attempt's publication MUST NOT advance the task's selected checkpoint.
            let mut t = owned(tx, task_id, attempt_id, *generation, false)?;
            let acknowledged = outcome.get("acknowledged").and_then(Value::as_bool) == Some(true);
            //= spec/impl/v2/implementation.md#work-starts-from-a-durable-source
            //# A checkpoint MUST NOT be reported durable before its external storage acknowledges it.
            if !acknowledged {
                if checkpoint.is_some() {
                    return refuse(
                        "checkpoint_unsettled",
                        "unacknowledged publication cannot select a checkpoint",
                    );
                }
                if record.state == "uncertain" && record.outcome.as_ref() == Some(outcome) {
                    return Ok((Data::Operation(record), false));
                }
                record.state = "uncertain".into();
                record.outcome = Some(outcome.clone());
                tx.execute(
                    "UPDATE external_operations SET body=?2 WHERE id=?1",
                    params![operation_id, encode(&record)?],
                )?;
                t.task.attempt.as_mut().unwrap().uncertain = true;
                save(tx, &mut t, "external_uncertain", operation_id)?;
                return Ok((Data::Operation(record), true));
            }
            if record.kind == "checkpoint" {
                if let Some(cp) = checkpoint {
                    if cp != &intended_checkpoint(operation_id, &record.intended)? {
                        return refuse(
                            "operation_conflict",
                            "checkpoint does not match original publication intent",
                        );
                    }
                    check_checkpoint(limits, &t.task, cp)?;
                    //= spec/impl/v2/implementation.md#work-starts-from-a-durable-source
                    //# A checkpoint reference MUST be committed to the task record before recovery selects it.
                    t.task.selected_checkpoint = Some(cp.clone());
                } else if outcome.get("not_applied").and_then(Value::as_bool) != Some(true) {
                    return refuse(
                        "invalid_input",
                        "checkpoint settlement needs exact acknowledged checkpoint or observed not-applied outcome",
                    );
                }
            } else if checkpoint.is_some() {
                return refuse(
                    "invalid_input",
                    "only checkpoint publication may select a checkpoint",
                );
            }
            record.state = "settled".into();
            record.outcome = Some(outcome.clone());
            tx.execute(
                "UPDATE external_operations SET body=?2,settlement=?3 WHERE id=?1",
                params![operation_id, encode(&record)?, encode(op)?],
            )?;
            t.task.attempt.as_mut().unwrap().uncertain =
                t.unresolved_execution || pending_effects(tx, task_id)? > 0;
            save(tx, &mut t, "external_settled", operation_id)?;
            Ok((Data::Operation(record), true))
        }
        Operation::GetOperation { operation_id } => {
            let body: Option<String> = tx
                .query_row(
                    "SELECT body FROM external_operations WHERE id=?1",
                    [operation_id],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(body) = body {
                return Ok((Data::Operation(decode(&body)?), false));
            }
            let Some(receipt) = super::super::metadata::read(tx, operation_id)? else {
                return refuse("not_found", "operation has no committed outcome");
            };
            Ok((
                Data::Operation(super::super::metadata::lookup_record(
                    operation_id,
                    &receipt.request,
                    &receipt.outcome,
                )?),
                false,
            ))
        }
        Operation::GetMemory { agent_id, key } => {
            if !valid_id(limits, agent_id) || !valid_id(limits, key) {
                return refuse("invalid_input", "invalid memory identity");
            }
            let row: Option<(i64, String)> = tx
                .query_row(
                    "SELECT revision,body FROM memories WHERE agent=?1 AND key=?2",
                    params![agent_id, key],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            let value = row
                .map(|(revision, body)| -> Result<MemoryValue> {
                    Ok(MemoryValue {
                        agent_id: agent_id.clone(),
                        key: key.clone(),
                        revision: unsigned(revision)?,
                        value: decode(&body)?,
                    })
                })
                .transpose()?;
            Ok((Data::Memory(value), false))
        }
        //= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
        //# The platform MUST persist explicitly recorded agent memory in its database.
        // Traceability scope: Memory is stored under the current caller task agent and key, and the same transaction records the committed key/revision change.
        //= spec/impl/v2/implementation.md#definitions-select-execution
        //# A memory MUST be keyed by agent identity rather than by session.
        //= spec/impl/v2/implementation.md#definitions-select-execution
        //# An agent's memories MUST persist across every session of that agent identity.
        //= spec/impl/v2/implementation.md#shared-memory-changes-reach-sessions
        //# A committed memory update MUST make its changed key and committed revision available to other active sessions of the same agent.
        Operation::SetMemory {
            operation_id,
            task_id,
            attempt_id,
            generation,
            key,
            expected_revision,
            value,
        } => {
            let mut t = owned(tx, task_id, attempt_id, *generation, true)?;
            if now >= t.task.input.limits.deadline_unix_ms {
                return refuse("deadline", "memory writer deadline expired");
            }
            if !valid_id(limits, key) {
                return refuse("invalid_input", "invalid memory key/value");
            }
            if bounded_json_size(value, limits.controller.memory_value_bytes).is_err() {
                return refuse(
                    "invalid_input",
                    "invalid memory key/value: controller.memory_value_bytes",
                );
            }
            let current: Option<i64> = tx
                .query_row(
                    "SELECT revision FROM memories WHERE agent=?1 AND key=?2",
                    params![t.task.assigned_agent_id, key],
                    |r| r.get(0),
                )
                .optional()?;
            let current = current.map(unsigned).transpose()?;
            if current != *expected_revision {
                return refuse("stale_revision", "memory revision changed");
            }
            let revision = current
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| Fault::Store("memory revision exhausted".into()))?;
            final_deadline(&t.task, cfg, now)?;
            tx.execute("INSERT INTO memories(agent,key,revision,body,operation_id) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(agent,key) DO UPDATE SET revision=excluded.revision,body=excluded.body,operation_id=excluded.operation_id",params![t.task.assigned_agent_id,key,integer(revision)?,encode(value)?,operation_id])?;
            save(
                tx,
                &mut t,
                "memory_recorded",
                &json!({"key":key,"revision":revision,"operation_id":operation_id}),
            )?;
            Ok((
                Data::Memory(Some(MemoryValue {
                    agent_id: t.task.assigned_agent_id,
                    key: key.clone(),
                    revision,
                    value: value.clone(),
                })),
                true,
            ))
        }
        _ => refuse(
            "unsupported_operation",
            "unsupported controller effect operation",
        ),
    }
}

fn final_deadline(task: &Task, cfg: &ControllerConfig, earlier: u64) -> Result<()> {
    let now = (cfg.clock)();
    if now < earlier {
        return refuse(
            "clock_unavailable",
            "clock moved backwards during task mutation",
        );
    }
    if now >= task.input.limits.deadline_unix_ms {
        return refuse("deadline", "task mutation crossed the original deadline");
    }
    Ok(())
}

pub(super) fn intended_checkpoint(id: &str, intended: &Value) -> Result<Checkpoint> {
    let field = |name: &str| {
        intended
            .get(name)
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                Fault::Refused("invalid_input", format!("checkpoint intent missing {name}"))
            })
    };
    Ok(Checkpoint {
        registration: serde_json::from_value(intended.get("registration").cloned().ok_or_else(
            || {
                Fault::Refused(
                    "invalid_input",
                    "checkpoint intent missing registration".into(),
                )
            },
        )?)
        .map_err(|_| Fault::Refused("invalid_input", "invalid checkpoint registration".into()))?,
        repository: field("repository")?,
        commit: field("commit")?,
        remote_ref: field("remote_ref")?,
        operation_id: id.into(),
    })
}
//= spec/impl/v2/implementation.md#checkpoint-publication-is-reconciled
//# The publication record MUST identify the repository, exact commit and intended remote reference.
pub(super) fn check_checkpoint(
    limits: &crate::limits::RuntimeLimits,
    task: &Task,
    cp: &Checkpoint,
) -> Result<()> {
    let a = task
        .attempt
        .as_ref()
        .ok_or_else(|| Fault::Refused("invalid_transition", "checkpoint has no attempt".into()))?;
    check_checkpoint_for_attempt(limits, task, cp, &a.id)
}

/// Read-only original publication validation may use a previous durable attempt
/// of this task. New publication still uses check_checkpoint's current owner.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//# A checkpoint publication MUST identify its accepted repository registration revision.
pub(super) fn check_checkpoint_for_attempt(
    limits: &crate::limits::RuntimeLimits,
    task: &Task,
    cp: &Checkpoint,
    publication_attempt: &str,
) -> Result<()> {
    let repo = task
        .input
        .repository
        .as_ref()
        .ok_or_else(|| Fault::Refused("invalid_input", "task has no repository".into()))?;
    if Some(&cp.registration) != repo.registration.as_ref()
        || cp.repository != repo.checkpoint_remote
        || !commit_id(&cp.commit)
        || !valid_id(limits, &cp.operation_id)
    {
        return refuse(
            "invalid_input",
            "checkpoint differs from task repository or has invalid identity",
        );
    }
    // Match the existing Git producer's exact, injective byte-hex namespace.
    // Raw task identifiers are not Git path components; all three independent
    // identities bind this publication, with no caller-supplied suffix.
    let component =
        |s: &str| -> String { s.as_bytes().iter().map(|b| format!("{b:02x}")).collect() };
    let expected = format!(
        "refs/heads/borg/tasks/{}/{}/{}",
        component(&task.id),
        component(publication_attempt),
        component(&cp.operation_id)
    );
    if cp.remote_ref != expected || cp.remote_ref.len() > limits.controller.max_checkpoint_ref_bytes
    {
        return refuse(
            "invalid_input",
            "checkpoint remote reference is not attempt-specific",
        );
    }
    Ok(())
}

// The observation receipt has its own identity; the already settled effect is
// immutable even when an equivalent observation arrives under another ID.
fn same_settlement(a: &Operation, b: &Operation) -> bool {
    match (a, b) {
        (
            Operation::SettleOperation {
                operation_id: a,
                task_id: at,
                attempt_id: aa,
                generation: ag,
                outcome: ao,
                checkpoint: ac,
                ..
            },
            Operation::SettleOperation {
                operation_id: b,
                task_id: bt,
                attempt_id: ba,
                generation: bg,
                outcome: bo,
                checkpoint: bc,
                ..
            },
        ) => a == b && at == bt && aa == ba && ag == bg && ao == bo && ac == bc,
        _ => false,
    }
}
