//! Bounded read projections; no model-selected SQL or external I/O.
//! Task caller/currentness enforcement belongs to the shared MCP call wrapper.
use super::database::StoreContext as Connection;
use super::{ControllerConfig, bounded_json_size, database::*};
use crate::{collaboration::*, protected_release as protected, protocol::*};
use rusqlite::{OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};

fn read_record<T: serde::de::DeserializeOwned + Serialize>(
    limits: &crate::limits::RuntimeLimits,
    raw: &str,
) -> Result<T> {
    if raw.len() > limits.transport.body_bytes {
        return Err(Fault::Store(
            "collaboration stored record exceeds bound".into(),
        ));
    }
    decode(raw)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    kind: String,
    scope: String,
    position: String,
}
fn cursor(kind: &str, scope: &str, position: &str) -> Result<String> {
    encode(&Cursor {
        kind: kind.into(),
        scope: scope.into(),
        position: position.into(),
    })
}
fn start(
    limits: &crate::limits::RuntimeLimits,
    after: &Option<String>,
    kind: &str,
    scope: &str,
) -> Result<String> {
    let Some(raw) = after else {
        return Ok(String::new());
    };
    if raw.len() > limits.controller.collaboration_cursor_bytes {
        return refuse(
            "invalid_input",
            "collaboration cursor exceeds bound: controller.collaboration_cursor_bytes",
        );
    }
    let c: Cursor = serde_json::from_str(raw)
        .map_err(|_| Fault::Refused("invalid_input", "invalid collaboration cursor".into()))?;
    if c.kind != kind || c.scope != scope || !valid_id(limits, &c.position) || encode(&c)? != *raw {
        return refuse(
            "invalid_input",
            "collaboration cursor differs from query scope",
        );
    }
    Ok(c.position)
}
fn sequence(value: &str) -> Result<i64> {
    if value.is_empty() {
        return Ok(0);
    }
    let n: i64 = value
        .parse()
        .map_err(|_| Fault::Refused("invalid_input", "invalid sequence cursor".into()))?;
    if n <= 0 || n.to_string() != value {
        return refuse("invalid_input", "invalid sequence cursor");
    }
    Ok(n)
}
fn bound(limit: usize, limits: &crate::limits::RuntimeLimits) -> Result<i64> {
    if limit == 0 {
        return refuse("invalid_input", "invalid collaboration page limit");
    }
    if limit > limits.controller.max_list_tasks {
        return refuse(
            "invalid_input",
            "invalid collaboration page limit: controller.max_list_tasks",
        );
    }
    if limit > limits.controller.collaboration_page_items {
        return refuse(
            "invalid_input",
            "invalid collaboration page limit: controller.collaboration_page_items",
        );
    }
    Ok((limit + 1) as i64)
}
fn identity(limits: &crate::limits::RuntimeLimits, value: &str) -> Result<()> {
    if !valid_id(limits, value) {
        return refuse("invalid_input", "invalid collaboration identity");
    }
    Ok(())
}
/// At most limit+1 rows are materialized. Filtered pages still advance by the
/// scanned row, so an empty partial page cannot masquerade as complete.
fn page<T: Serialize>(
    limits: &crate::limits::RuntimeLimits,
    rows: Vec<(String, Option<T>)>,
    limit: usize,
    kind: &str,
    scope: &str,
) -> Result<Page<T>> {
    let more = rows.len() > limit;
    let mut result = Page {
        items: vec![],
        next_cursor: None,
    };
    let mut last = None;
    for (position, item) in rows.into_iter().take(limit) {
        if let Some(item) = item {
            result.items.push(item);
            result.next_cursor = Some(cursor(kind, scope, &position)?);
            if bounded_json_size(&result, limits.controller.collaboration_page_bytes).is_err() {
                result.items.pop();
                if result.items.is_empty() {
                    return refuse(
                        "response_bound",
                        "collaboration item exceeds page bound: controller.collaboration_page_bytes",
                    );
                }
                result.next_cursor = last;
                return Ok(result);
            }
        }
        last = Some(cursor(kind, scope, &position)?);
    }
    result.next_cursor = if more { last } else { None };
    if bounded_json_size(&result, limits.controller.collaboration_page_bytes).is_err() {
        return refuse(
            "response_bound",
            "collaboration page exceeds bound: controller.collaboration_page_bytes",
        );
    }
    Ok(result)
}
fn worker(tx: &Connection, id: &str) -> Result<WorkerCapacity> {
    let limits = tx.limits;
    identity(limits, id)?;
    let raw: Option<String> = tx
        .query_row(
            &format!("SELECT CAST(substr(CAST(body AS BLOB),1,{body_probe}) AS TEXT) FROM workers WHERE id=?1", body_probe=limits.transport.body_bytes.saturating_add(1)),
            [id],
            |r| r.get(0),
        )
        .optional()?;
    let w: Worker = read_record(
        limits,
        &raw.ok_or_else(|| Fault::Refused("not_found", "worker not found".into()))?,
    )?;
    if w.id != id {
        return Err(Fault::Store("worker identity mismatch".into()));
    }
    let occupied = |kind: &str| -> Result<u64> {
        let count: i64 = tx.query_row("SELECT count(*) FROM tasks WHERE worker=?1 AND incarnation=?2 AND executor=?3 AND active=1", params![w.id,w.incarnation,kind], |r| r.get(0))?;
        unsigned(count)
    };
    Ok(WorkerCapacity {
        coding_occupied: occupied("coding")?,
        native_occupied: occupied("native")?,
        worker: w,
    })
}
fn safe_repository(limits: &crate::limits::RuntimeLimits, value: &str) -> Result<()> {
    if crate::credential::repository_contains_credentials(value) {
        return refuse(
            "credential_metadata",
            "credential-bearing repository metadata is not exposed",
        );
    }
    Ok(())
}
fn safe_checkpoint(limits: &crate::limits::RuntimeLimits, value: &Checkpoint) -> Result<()> {
    safe_repository(limits, &value.repository)?;
    safe_repository(limits, &value.remote_ref)
}
fn checkpoint_outcome(value: Option<&serde_json::Value>) -> Option<CheckpointOutcome> {
    value.map(|v| CheckpointOutcome {
        acknowledged: v.get("acknowledged").and_then(serde_json::Value::as_bool),
        not_applied: v.get("not_applied").and_then(serde_json::Value::as_bool),
    })
}
fn history_body(
    limits: &crate::limits::RuntimeLimits,
    kind: &str,
    raw: &str,
) -> Result<serde_json::Value> {
    use serde_json::json;
    if kind == "created_by" {
        let origin: TaskCreationProvenance = read_record(limits, raw)?;
        return Ok(serde_json::to_value(origin)?);
    }
    // A historical provider association is not a tool-accessible conversation.
    if kind == "worker_observation" {
        let observation: Observation = read_record(limits, raw)?;
        return match observation {
            Observation::Started { .. } => {
                Ok(json!({"kind":"started","executor_association_omitted":true}))
            }
            Observation::ResultCandidate { ref checkpoint, .. } => {
                if let Some(cp) = checkpoint {
                    safe_checkpoint(limits, cp)?;
                }
                Ok(serde_json::to_value(observation)?)
            }
            other => Ok(serde_json::to_value(other)?),
        };
    }
    if kind == "external_intent" {
        let r: OperationRecord = read_record(limits, raw)?;
        if matches!(r.kind.as_str(), crate::pull_request::HEAD_KIND | crate::pull_request::CREATE_KIND) {
            // Older generic effects can share this kind without the new closed
            // PR intent. Keep their existing opaque view; never expose raw data.
            if let Ok(public) = crate::pull_request_view::record(&r) {
                return Ok(serde_json::to_value(public)?);
            }
        }
        // Original effect payload may contain arbitrary adapter data. The
        // checkpoint-specific query is the typed publication reader.
        return Ok(
            json!({"operation_id":r.id,"attempt_id":r.attempt_id,"generation":r.generation,"kind":r.kind,"state":r.state,"opaque_intent_omitted":true}),
        );
    }
    if kind == "attempt_assigned" {
        let attempt: Attempt = read_record(limits, raw)?;
        return Ok(serde_json::to_value(attempt)?);
    }
    if kind == "release_prepared" {
        let r: ReleaseCandidate = read_record(limits, raw)?;
        safe_checkpoint(limits, &r.checkpoint)?;
        return Ok(serde_json::to_value(r)?);
    }
    match kind {
        "submitted"
        | "assigned"
        | "retry_queued"
        | "child_delegated"
        | "child_settled"
        | "deadline"
        | "cancel_requested"
        | "descendant_stop_requested"
        | "admission_deferred"
        | "budget_exhausted"
        | "attempt_assigned"
        | "external_uncertain"
        | "external_settled"
        | "memory_recorded" => read_record(limits, raw),
        _ => Ok(json!({"details_omitted":true})),
    }
}
fn check_record(
    limits: &crate::limits::RuntimeLimits,
    release_id: &str,
    id: String,
    p: String,
    q: String,
    r: String,
    revision: i64,
) -> Result<(String, Option<ReleaseCheck>)> {
    let producer: protected::Producer = read_record(limits, &p)?;
    let request: protected::Request = read_record(limits, &q)?;
    let reply: protected::Reply = read_record(limits, &r)?;
    if request.operation_id != id || revision <= 0 {
        return Err(Fault::Store("check receipt identity mismatch".into()));
    }
    let key = revision.to_string();
    match request.command {
        protected::Command::Check(check)
            if check.subject.release_id == release_id && reply.status == "ok" =>
        {
            if producer.role != protected::Role::Checker {
                return Err(Fault::Store("check producer role mismatch".into()));
            }
            safe_repository(limits, &check.subject.repository)?;
            safe_repository(limits, &check.subject.target_ref)?;
            Ok((
                key,
                Some(ReleaseCheck {
                    operation_id: id,
                    producer_id: producer.id,
                    controller_revision: unsigned(revision)?,
                    check,
                }),
            ))
        }
        _ => Ok((key, None)),
    }
}
// Traceability scope: Limits, bounded History/Attempts and typed worker/checkpoint/release-check projections provide operational evidence through the existing MCP wrapper. These read-only queries neither issue worker commands nor protected decisions; they do not implement an automatic diagnostic scheduler or a general evaluator.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//# The shared agent interface MUST support diagnosis of configuration refusals using existing bounded operational records.
//= spec/impl/v2/implementation.md#shared-collaboration-tool-interface
//# The collaboration tool interface MUST provide read-only views of worker capacity, task attempts, checkpoints and release-check records.
//= spec/impl/v2/implementation.md#shared-collaboration-tool-interface
//# Operational queries MUST NOT grant authority to mutate worker controls or protected release state.
pub(super) fn apply(tx: &Connection, query: &Query, cfg: &ControllerConfig) -> Result<QueryResult> {
    let limits = tx.limits;
    match query {
        Query::Repositories { after, limit } => Ok(QueryResult::Repositories(
            super::repositories::list(tx, after, *limit)?,
        )),
        Query::RepositoryRegistration {
            repository_id,
            registration_revision,
        } => Ok(QueryResult::RepositoryRegistration(
            super::repositories::get(tx, repository_id, registration_revision.as_deref())?,
        )),
        Query::Limits { task_id } => {
            let execution = task_id
                .as_ref()
                .map(|id| {
                    load(tx, id).and_then(|t| {
                        t.task
                            .input
                            .execution_limits()
                            .map_err(|reason| Fault::Store(reason))?;
                        Ok(t.task.input.resolved_limits)
                    })
                })
                .transpose()?
                .flatten();
            Ok(QueryResult::Limits {
                global: Box::new(cfg.limits.as_ref().clone()),
                execution,
            })
        }
        Query::Session { session_id } => {
            super::sessions::get(tx, session_id).map(QueryResult::Session)
        }
        Query::SessionInput { session_id } => {
            let session=super::sessions::get(tx,session_id)?;
            let input=super::sessions::input(tx,&session)?;
            let current=load(tx,&session.task_id)?.task.session_id==session.id;
            Ok(QueryResult::SessionInput { session, input:Box::new(input), current })
        }
        Query::Sessions {
            agent_id,
            after,
            limit,
        } => {
            if let Some(id) = agent_id {
                identity(limits, id)?;
            }
            let scope = serde_json::to_string(agent_id)?;
            let position = start(limits, after, "sessions", &scope)?;
            let n = bound(*limit, limits)?;
            let mut q = tx.prepare("SELECT id FROM sessions WHERE id>?1 AND (?2 IS NULL OR agent=?2) ORDER BY id LIMIT ?3")?;
            let ids = q
                .query_map(params![position, agent_id, n], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let rows = ids
                .into_iter()
                .map(|id| super::sessions::get(tx, &id).map(|v| (id, Some(v))))
                .collect::<Result<Vec<_>>>()?;
            page(limits, rows, *limit, "sessions", &scope).map(QueryResult::Sessions)
        }
        Query::Repository { .. } => Err(Fault::Store(
            "repository query requires private read ticket".into(),
        )),
        Query::Activity { after, limit } => {
            activity(tx, *after, *limit, cfg).map(QueryResult::Activity)
        }
        Query::Tasks {
            agent_id,
            parent_task_id,
            after,
            limit,
        } => {
            super::collaboration_calls::task_page(tx, cfg, agent_id, parent_task_id, after, *limit)
                .map(QueryResult::Tasks)
        }
        Query::Call {
            task_id,
            operation_id,
        } => {
            identity(limits, task_id)?;
            identity(limits, operation_id)?;
            load(tx, task_id)?;
            let record = super::metadata::read(tx, operation_id)?.ok_or_else(|| {
                Fault::Refused("not_found", "collaboration call not found".into())
            })?;
            let Operation::CallCollaboration { call } = record.request else {
                return refuse("not_found", "receipt is not a collaboration call");
            };
            if call.caller.task_id != *task_id {
                return refuse("not_found", "collaboration call does not belong to task");
            }
            call.validate(limits)
                .map_err(|_| Fault::Store("invalid stored collaboration call".into()))?;
            if !crate::mcp::catalogue(limits)
                .iter()
                .any(|t| t.name == call.tool)
            {
                return refuse("not_found", "call is outside the closed catalogue");
            }
            let reply = super::metadata::reply(
                limits,
                "ui-call-observation",
                &call.operation(),
                record.outcome,
                Some(record.commit),
                super::metadata::current(tx)?,
            )?;
            let result = crate::mcp::result(&call, reply)
                .map_err(|_| Fault::Store("invalid call receipt context".into()))?;
            let view = QueryResult::Call(Box::new(CallRecord {
                invocation: call,
                result,
            }));
            if bounded_json_size(&view, limits.controller.collaboration_page_bytes).is_err() {
                return refuse("response_bound", "call view exceeds response bound");
            }
            Ok(view)
        }
        Query::Definition {
            definition_id,
            revision,
        } => {
            identity(limits, definition_id)?;
            identity(limits, revision)?;
            let raw: Option<String> = tx.query_row(&format!("SELECT CAST(substr(CAST(body AS BLOB),1,{body_probe}) AS TEXT) FROM definitions WHERE id=?1 AND revision=?2", body_probe=limits.transport.body_bytes.saturating_add(1)), params![definition_id,revision], |r| r.get(0)).optional()?;
            let value: Definition = read_record(
                limits,
                &raw.ok_or_else(|| {
                    Fault::Refused("not_found", "definition revision not found".into())
                })?,
            )?;
            if value.id != *definition_id || value.revision != *revision {
                return Err(Fault::Store("definition identity mismatch".into()));
            }
            Ok(QueryResult::Definition(value))
        }
        Query::Worker { worker_id } => Ok(QueryResult::Worker(worker(tx, worker_id)?)),
        Query::Workers { after, limit } => {
            let n = bound(*limit, limits)?;
            let at = start(limits, after, "workers", "")?;
            let mut q = tx.prepare(
                "SELECT id FROM workers WHERE id>?1 ORDER BY id COLLATE BINARY LIMIT ?2",
            )?;
            let ids = q
                .query_map(params![at, n], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let rows = ids
                .into_iter()
                .map(|id| worker(tx, &id).map(|w| (id, Some(w))))
                .collect::<Result<Vec<_>>>()?;
            Ok(QueryResult::Workers(page(
                limits, rows, *limit, "workers", "",
            )?))
        }
        Query::MemoryKeys {
            agent_id,
            after,
            limit,
        } => {
            identity(limits, agent_id)?;
            let n = bound(*limit, limits)?;
            let exists: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM agents WHERE id=?1)",
                [agent_id],
                |r| r.get(0),
            )?;
            if !exists {
                return refuse("not_found", "agent not found");
            }
            let at = start(limits, after, "memory_keys", agent_id)?;
            let mut q=tx.prepare("SELECT key,revision FROM memories WHERE agent=?1 AND key>?2 ORDER BY key COLLATE BINARY LIMIT ?3")?;
            let raw = q
                .query_map(params![agent_id, at, n], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let rows = raw
                .into_iter()
                .map(|(key, revision)| {
                    identity(limits, &key)?;
                    let revision = unsigned(revision)?;
                    if revision == 0 {
                        return Err(Fault::Store("memory revision invalid".into()));
                    }
                    Ok((key.clone(), Some(MemoryKey { key, revision })))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(QueryResult::MemoryKeys(page(
                limits,
                rows,
                *limit,
                "memory_keys",
                agent_id,
            )?))
        }
        Query::TaskHistory {
            task_id,
            after,
            limit,
        } => {
            identity(limits, task_id)?;
            let task = load(tx, task_id)?;
            let n = bound(*limit, limits)?;
            let at = sequence(&start(limits, after, "task_history", task_id)?)?;
            let mut q=tx.prepare(&format!("SELECT seq,revision,kind,CAST(substr(CAST(body AS BLOB),1,{body_probe}) AS TEXT) FROM history WHERE task_id=?1 AND seq>?2 ORDER BY seq LIMIT ?3", body_probe=limits.transport.body_bytes.saturating_add(1)))?;
            let raw = q
                .query_map(params![task_id, at, n], |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                    ))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let rows = raw
                .into_iter()
                .map(|(seq, revision, kind, body)| {
                    let revision = unsigned(revision)?;
                    if revision == 0
                        || revision > task.task.revision
                        || !valid_id(limits, &kind)
                        || body.len() > limits.transport.body_bytes
                    {
                        return Err(Fault::Store("history projection invalid".into()));
                    }
                    Ok((
                        seq.to_string(),
                        Some(HistoryEntry {
                            sequence: unsigned(seq)?,
                            task_revision: revision,
                            body: history_body(limits, &kind, &body)?,
                            kind,
                        }),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(QueryResult::TaskHistory(page(
                limits,
                rows,
                *limit,
                "task_history",
                task_id,
            )?))
        }
        Query::TaskAttempts {
            task_id,
            after,
            limit,
        } => {
            identity(limits, task_id)?;
            load(tx, task_id)?;
            let n = bound(*limit, limits)?;
            let at = start(limits, after, "task_attempts", task_id)?;
            let mut q=tx.prepare(&format!("SELECT id,CAST(substr(CAST(body AS BLOB),1,{body_probe}) AS TEXT),CAST(substr(CAST(executor_outcome AS BLOB),1,{body_probe}) AS TEXT),model_requests,input_tokens,output_tokens FROM attempts WHERE task_id=?1 AND id>?2 ORDER BY id COLLATE BINARY LIMIT ?3", body_probe=limits.transport.body_bytes.saturating_add(1)))?;
            let raw = q
                .query_map(params![task_id, at, n], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, Option<String>>(2)?,
                        r.get::<_, i64>(3)?,
                        r.get::<_, Option<i64>>(4)?,
                        r.get::<_, Option<i64>>(5)?,
                    ))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let rows = raw
                .into_iter()
                .map(|(id, body, outcome, requests, input, output)| {
                    let attempt: Attempt = read_record(limits, &body)?;
                    if attempt.id != id || attempt.task_id != *task_id {
                        return Err(Fault::Store("attempt identity mismatch".into()));
                    }
                    let executor_outcome = match outcome {
                        None => None,
                        Some(raw) => match read_record::<Observation>(limits, &raw)? {
                            Observation::ExecutorOutcome { success, summary } => {
                                Some(ExecutorOutcomeView { success, summary })
                            }
                            _ => return Err(Fault::Store("attempt outcome kind mismatch".into())),
                        },
                    };
                    let reported_model_requests = u32::try_from(requests)
                        .map_err(|_| Fault::Store("attempt request counter invalid".into()))?;
                    Ok((
                        id,
                        Some(AttemptView {
                            attempt,
                            executor_outcome,
                            reported_model_requests,
                            input_tokens: input.map(unsigned).transpose()?,
                            output_tokens: output.map(unsigned).transpose()?,
                        }),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(QueryResult::TaskAttempts(page(
                limits,
                rows,
                *limit,
                "task_attempts",
                task_id,
            )?))
        }
        Query::TaskCheckpoints {
            task_id,
            after,
            limit,
        } => {
            identity(limits, task_id)?;
            let task = load(tx, task_id)?;
            let n = bound(*limit, limits)?;
            let at = start(limits, after, "task_checkpoints", task_id)?;
            // Limit scanned original records even when none match this task.
            let mut q=tx.prepare(&format!("SELECT id,CAST(substr(CAST(body AS BLOB),1,{body_probe}) AS TEXT) FROM external_operations WHERE id>?1 ORDER BY id COLLATE BINARY LIMIT ?2", body_probe=limits.transport.body_bytes.saturating_add(1)))?;
            let raw = q
                .query_map(params![at, n], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let rows = raw
                .into_iter()
                .map(|(id, body)| {
                    let record: OperationRecord = read_record(limits, &body)?;
                    if record.id != id {
                        return Err(Fault::Store(
                            "checkpoint operation identity mismatch".into(),
                        ));
                    }
                    if record.task_id != *task_id || record.kind != "checkpoint" {
                        return Ok((id, None));
                    }
                    let string = |name: &str| -> Result<String> {
                        record
                            .intended
                            .get(name)
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned)
                            .ok_or_else(|| Fault::Store("checkpoint intent field missing".into()))
                    };
                    let checkpoint = Checkpoint {
                        registration: serde_json::from_value(
                            record
                                .intended
                                .get("registration")
                                .cloned()
                                .ok_or_else(|| {
                                    Fault::Store("checkpoint registration missing".into())
                                })?,
                        )
                        .map_err(|_| Fault::Store("checkpoint registration invalid".into()))?,
                        repository: string("repository")?,
                        commit: string("commit")?,
                        remote_ref: string("remote_ref")?,
                        operation_id: id.clone(),
                    };
                    if !matches!(record.state.as_str(), "pending" | "uncertain" | "settled") {
                        return Err(Fault::Store("checkpoint state invalid".into()));
                    }
                    safe_checkpoint(limits, &checkpoint)?;
                    let selected = task.task.selected_checkpoint.as_ref() == Some(&checkpoint);
                    Ok((
                        id.clone(),
                        Some(CheckpointPublication {
                            operation_id: id,
                            attempt_id: record.attempt_id,
                            generation: record.generation,
                            checkpoint,
                            state: record.state,
                            outcome: checkpoint_outcome(record.outcome.as_ref()),
                            selected,
                        }),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(QueryResult::TaskCheckpoints(page(
                limits,
                rows,
                *limit,
                "task_checkpoints",
                task_id,
            )?))
        }
        Query::ReleaseChecks {
            release_id,
            after,
            limit,
        } => {
            identity(limits, release_id)?;
            super::operations::releases::read(tx, release_id)?;
            let n = bound(*limit, limits)?;
            let at = sequence(&start(limits, after, "release_checks", release_id)?)?;
            let mut q=tx.prepare(&format!("SELECT id,CAST(substr(CAST(producer AS BLOB),1,{body_probe}) AS TEXT),CAST(substr(CAST(request AS BLOB),1,{body_probe}) AS TEXT),CAST(substr(CAST(reply AS BLOB),1,{body_probe}) AS TEXT),revision FROM protected_receipts WHERE revision>?1 ORDER BY revision LIMIT ?2", body_probe=limits.transport.body_bytes.saturating_add(1)))?;
            let raw = q
                .query_map(params![at, n], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, i64>(4)?,
                    ))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let rows = raw
                .into_iter()
                .map(|(id, p, q, r, revision)| {
                    check_record(limits, release_id, id, p, q, r, revision)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(QueryResult::ReleaseChecks(page(
                limits,
                rows,
                *limit,
                "release_checks",
                release_id,
            )?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cursor_is_scope_bound_and_empty_filtered_pages_advance() {
        let limits = &crate::limits::RuntimeLimits::seed().expect("test metadata");
        let p = page::<String>(
            limits,
            vec![("1".into(), None), ("2".into(), Some("visible".into()))],
            1,
            "checks",
            "release-a",
        )
        .unwrap();
        assert!(p.items.is_empty());
        assert!(p.next_cursor.is_some());
        assert_eq!(
            start(limits, &p.next_cursor, "checks", "release-a").unwrap(),
            "1"
        );
        assert!(start(limits, &p.next_cursor, "checks", "release-b").is_err());
        assert!(start(limits, &p.next_cursor, "tasks", "release-a").is_err());
        let next = page(
            limits,
            vec![("2".into(), Some("visible".to_string()))],
            1,
            "checks",
            "release-a",
        )
        .unwrap();
        assert_eq!(next.items, vec!["visible"]);
        assert!(next.next_cursor.is_none());
    }
    #[test]
    fn oversized_first_record_refuses_and_later_record_is_not_skipped() {
        let limits = &crate::limits::RuntimeLimits::seed().expect("test metadata");
        let huge = "x".repeat(limits.controller.collaboration_page_bytes);
        assert!(page(limits, vec![("a".into(), Some(huge.clone()))], 1, "q", "s").is_err());
        let p = page(
            limits,
            vec![
                ("a".into(), Some("small".to_string())),
                ("b".into(), Some(huge)),
            ],
            2,
            "q",
            "s",
        )
        .unwrap();
        assert_eq!(p.items, vec!["small"]);
        assert_eq!(start(limits, &p.next_cursor, "q", "s").unwrap(), "a");
    }
    #[test]
    fn closed_query_rejects_unknown_fields_and_invalid_cursor() {
        let limits = &crate::limits::RuntimeLimits::seed().expect("test metadata");
        assert!(
            serde_json::from_str::<Query>(
                r#"{"query":"worker","input":{"worker_id":"w","commands":true}}"#
            )
            .is_err()
        );
        assert!(start(limits, &Some("null".into()), "q", "").is_err());
        assert!(sequence("-1").is_err());
        assert!(sequence("01").is_err());
        assert!(bound(0, &crate::limits::RuntimeLimits::seed().unwrap()).is_err());
        assert!(bound(101, &crate::limits::RuntimeLimits::seed().unwrap()).is_err());
    }
    fn sample_check() -> protected::Check {
        serde_json::from_value(serde_json::json!({
          "subject":{"release_id":"release-a","artifact":{"format":"borg-v2-release/1","package_sha256":"1".repeat(64),"package_bytes":20480,"manifest_sha256":"2".repeat(64),"source_commit":"a".repeat(40),"source_tree":"b".repeat(40),"object_format":"sha1","binary_sha256":"3".repeat(64),"binary_bytes":1024,"target":"x86_64-unknown-linux-gnu"},"repository":"https://example.invalid/repo","target_ref":"refs/heads/main","expected_base":"c".repeat(40),"policy":"policy-1","deployment_target":"target-1","target_revision":"r1","installed_package_sha256":"4".repeat(64)},
          "recipe_sha256":"5".repeat(64),"checked_at_ms":1,"valid_until_ms":2,"results":{"test":false},"cleanup_verified":false
        })).unwrap()
    }
    #[test]
    fn check_projection_excludes_grants_uids_and_refused_claims() {
        let limits = &crate::limits::RuntimeLimits::seed().expect("test metadata");
        let producer = protected::Producer {
            id: "checker".into(),
            uid: 12345,
            role: protected::Role::Checker,
        };
        let check = sample_check();
        let request = protected::Request {
            revision: 1,
            operation_id: "check-a".into(),
            command: protected::Command::Check(check.clone()),
        };
        let reply = protected::Reply {
            status: "ok".into(),
            data: serde_json::json!({"private_response":"not projected"}),
            replayed: false,
        };
        let row = || {
            (
                "check-a".into(),
                encode(&producer).unwrap(),
                encode(&request).unwrap(),
                encode(&reply).unwrap(),
                1,
            )
        };
        let (id, p, q, r, n) = row();
        let (_, projected) = check_record(limits, "release-a", id, p, q, r, n).unwrap();
        let projected = projected.unwrap();
        assert_eq!(projected.check.results["test"], false);
        assert!(!projected.check.cleanup_verified);
        let raw = encode(&projected).unwrap();
        assert!(!raw.contains("12345"));
        assert!(!raw.contains("private_response"));
        let (id, p, q, r, n) = row();
        assert!(
            check_record(limits, "other-release", id, p, q, r, n)
                .unwrap()
                .1
                .is_none()
        );
        let grant = protected::Request {
            revision: 1,
            operation_id: "check-a".into(),
            command: protected::Command::Grant {
                subject: check.subject,
                valid_until_ms: 99,
            },
        };
        let (id, p, _, r, n) = row();
        assert!(
            check_record(limits, "release-a", id, p, encode(&grant).unwrap(), r, n)
                .unwrap()
                .1
                .is_none()
        );
        let (id, p, q, _, n) = row();
        let refused = protected::Reply {
            status: "wrong_producer_role".into(),
            data: serde_json::Value::Null,
            replayed: false,
        };
        assert!(
            check_record(limits, "release-a", id, p, q, encode(&refused).unwrap(), n)
                .unwrap()
                .1
                .is_none()
        );
    }
    #[test]
    fn credential_url_and_opaque_payloads_are_not_projected() {
        let limits = &crate::limits::RuntimeLimits::seed().expect("test metadata");
        let mut check = sample_check();
        check.subject.repository = "https://user:synthetic-password@example.invalid/repo".into();
        let p = protected::Producer {
            id: "checker".into(),
            uid: 12345,
            role: protected::Role::Checker,
        };
        let q = protected::Request {
            revision: 1,
            operation_id: "check-a".into(),
            command: protected::Command::Check(check),
        };
        let r = protected::Reply {
            status: "ok".into(),
            data: serde_json::Value::Null,
            replayed: false,
        };
        assert!(
            check_record(
                limits,
                "release-a",
                "check-a".into(),
                encode(&p).unwrap(),
                encode(&q).unwrap(),
                encode(&r).unwrap(),
                1
            )
            .is_err()
        );
        let observation = Observation::Started {
            thread_id: Some("opaque-thread".into()),
            turn_id: Some("opaque-turn".into()),
        };
        assert!(
            !encode(
                &history_body(limits, "worker_observation", &encode(&observation).unwrap())
                    .unwrap()
            )
            .unwrap()
            .contains("opaque-thread")
        );
        let record = OperationRecord {
            id: "op".into(),
            task_id: "t".into(),
            attempt_id: "a".into(),
            generation: 1,
            kind: "pull_request".into(),
            intended: serde_json::json!({"credential":"synthetic-value"}),
            state: "pending".into(),
            outcome: None,
        };
        assert!(
            !encode(&history_body(limits, "external_intent", &encode(&record).unwrap()).unwrap())
                .unwrap()
                .contains("synthetic-value")
        );
        let projected = checkpoint_outcome(Some(
            &serde_json::json!({"acknowledged":false,"credential":"synthetic-value"}),
        ))
        .unwrap();
        assert_eq!(projected.acknowledged, Some(false));
        assert_eq!(projected.not_applied, None);
        assert!(!encode(&projected).unwrap().contains("synthetic-value"));
        assert!(
            read_record::<serde_json::Value>(limits, &"x".repeat(limits.transport.body_bytes + 1))
                .is_err()
        );
    }
    #[test]
    fn typed_attempt_history_preserves_exact_stored_shape() {
        let limits = &crate::limits::RuntimeLimits::seed().expect("test metadata");
        let a = Attempt {
            id: "attempt-t-10".into(),
            session_id: "session-t".into(),
            task_id: "t".into(),
            generation: 10,
            worker_id: "w".into(),
            worker_incarnation: "i".into(),
            phase: AttemptPhase::Assigned,
            deadline_unix_ms: 99,
            stopped: false,
            uncertain: false,
        };
        let raw = encode(&a).unwrap();
        assert!(decode::<serde_json::Value>(&raw).is_err());
        assert_eq!(
            history_body(limits, "attempt_assigned", &raw).unwrap(),
            serde_json::to_value(&a).unwrap()
        );
        assert!(history_body(limits, "attempt_assigned", r#"{"unexpected":true}"#).is_err());
    }
}

fn activity(
    tx: &Connection,
    after: Option<u64>,
    limit: usize,
    cfg: &ControllerConfig,
) -> Result<ActivityPage> {
    let limits = tx.limits;
    let n = bound(limit, limits)?;
    let start = after.map(integer).transpose()?.unwrap_or(0);
    let (sql, parameter) = if after.is_some() {
        (
            &format!(
                "SELECT seq,task_id,revision,kind,CAST(substr(CAST(body AS BLOB),1,{body_probe}) AS TEXT) FROM history WHERE seq>?1 ORDER BY seq LIMIT ?2",
                body_probe = limits.transport.body_bytes.saturating_add(1)
            ),
            start,
        )
    } else {
        (
            &format!(
                "SELECT seq,task_id,revision,kind,CAST(substr(CAST(body AS BLOB),1,{body_probe}) AS TEXT) FROM history WHERE seq>?1 ORDER BY seq DESC LIMIT ?2",
                body_probe = limits.transport.body_bytes.saturating_add(1)
            ),
            0,
        )
    };
    let mut q = tx.prepare(sql)?;
    let mut rows = q
        .query_map(params![parameter, n], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut more = rows.len() > limit;
    if after.is_none() {
        rows.truncate(limit);
        rows.reverse();
        more = false;
    }
    let mut page = ActivityPage {
        items: vec![],
        cursor: after.unwrap_or(0),
        has_more: more,
    };
    for (seq, task_id, revision, kind, body) in rows.into_iter().take(limit) {
        identity(limits, &task_id)?;
        identity(limits, &kind)?;
        let task = load(tx, &task_id)?;
        let revision = unsigned(revision)?;
        let seq = unsigned(seq)?;
        if revision == 0
            || revision > task.task.revision
            || seq == 0
            || body.len() > limits.transport.body_bytes
        {
            return Err(Fault::Store("invalid activity record".into()));
        }
        let prior_cursor = page.cursor;
        page.cursor = seq;
        page.items.push(ActivityEntry {
            sequence: seq,
            task_id,
            task_revision: revision,
            body: history_body(limits, &kind, &body)?,
            kind,
        });
        if bounded_json_size(&page, limits.controller.collaboration_page_bytes).is_err() {
            page.items.pop();
            page.cursor = prior_cursor;
            if page.items.is_empty() {
                return refuse(
                    "response_bound",
                    "activity item exceeds page bound: controller.collaboration_page_bytes",
                );
            }
            page.has_more = true;
            return Ok(page);
        }
        page.cursor = seq;
    }
    Ok(page)
}
