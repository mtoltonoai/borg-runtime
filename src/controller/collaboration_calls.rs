//! Closed collaboration dispatch inside the existing receipt transaction.
//! Exact receipt replay happens before currentness in Database::process. A new
//! occurrence always rechecks the actor and reuses canonical task operations.
use super::{ControllerConfig, database::*, operations, validation::Context};
use crate::{
    collaboration::Query,
    mcp::{Caller, Invocation, ListPage},
    protocol::*,
};
use super::database::StoreContext as Connection;
use rusqlite::{ OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
#[derive(Deserialize)]
#[serde(tag = "tool", content = "arguments", deny_unknown_fields)]
enum ToolArgs {
    #[serde(rename = "collab_repositories_list")]
    Repositories { after: Option<String>, limit: usize },
    #[serde(rename = "collab_repository_get")]
    RepositoryRegistration { repository_id: String, registration_revision: Option<String> },
    #[serde(rename = "collab_limits_get")]
    Limits {},
    #[serde(rename = "collab_session_get")]
    Session { session_id: String },
    #[serde(rename = "collab_sessions_list")]
    Sessions {
        agent_id: Option<String>,
        cursor: Option<String>,
        limit: usize,
    },
    #[serde(rename = "collab_repository_tree")]
    RepositoryTree {
        source: super::repository_queries::Selector,
        path: String,
        after: Option<String>,
        limit: usize,
    },
    #[serde(rename = "collab_repository_read")]
    RepositoryRead {
        source: super::repository_queries::Selector,
        path: String,
        start_line: usize,
        max_lines: usize,
    },
    #[serde(rename = "collab_repository_search")]
    RepositorySearch {
        source: super::repository_queries::Selector,
        path: String,
        needle: String,
    },
    #[serde(rename = "collab_repository_diff")]
    RepositoryDiff {
        source: super::repository_queries::Selector,
        base_source: super::repository_queries::Selector,
        path: String,
    },
    #[serde(rename = "collab_repository_history")]
    RepositoryHistory {
        source: super::repository_queries::Selector,
        limit: usize,
    },
    #[serde(rename = "collab_call_get")]
    CallGet { call_id: String },
    #[serde(rename = "collab_tasks_list")]
    Tasks {
        cursor: Option<String>,
        limit: usize,
        agent_id: Option<String>,
        parent_task_id: Option<String>,
    },
    #[serde(rename = "collab_task_get")]
    Task { task_id: String },
    #[serde(rename = "collab_task_history_list")]
    History {
        task_id: String,
        cursor: Option<String>,
        limit: usize,
    },
    #[serde(rename = "collab_agents_list")]
    Agents {
        cursor: Option<String>,
        limit: usize,
    },
    #[serde(rename = "collab_agent_get")]
    Agent { agent_id: String },
    #[serde(rename = "collab_definition_get")]
    Definition {
        definition_id: String,
        revision: String,
    },
    #[serde(rename = "collab_memory_list")]
    MemoryKeys {
        cursor: Option<String>,
        limit: usize,
    },
    #[serde(rename = "collab_memory_get")]
    Memory { key: String },
    #[serde(rename = "collab_memory_set")]
    SetMemory {
        key: String,
        expected_revision: Option<u64>,
        value: Value,
    },
    #[serde(rename = "collab_task_progress")]
    Progress { text: String },
    #[serde(rename = "collab_task_delegate")]
    Delegate { instruction: String },
    #[serde(rename = "collab_task_assign")]
    Assign {
        task_id: String,
        expected_revision: u64,
        agent_id: String,
    },
    #[serde(rename = "collab_task_cancel")]
    Cancel {
        task_id: String,
        expected_revision: u64,
    },
    #[serde(rename = "collab_task_retry")]
    Retry {
        task_id: String,
        expected_revision: u64,
    },
    #[serde(rename = "collab_workers_list")]
    Workers {
        cursor: Option<String>,
        limit: usize,
    },
    #[serde(rename = "collab_worker_get")]
    Worker { worker_id: String },
    #[serde(rename = "collab_task_attempts_list")]
    Attempts {
        task_id: String,
        cursor: Option<String>,
        limit: usize,
    },
    #[serde(rename = "collab_task_checkpoints_list")]
    Checkpoints {
        task_id: String,
        cursor: Option<String>,
        limit: usize,
    },
    #[serde(rename = "collab_task_releases_list")]
    Releases {
        task_id: String,
        cursor: Option<String>,
        limit: usize,
    },
    #[serde(rename = "collab_release_get")]
    Release { release_id: String },
    #[serde(rename = "collab_release_checks_list")]
    Checks {
        release_id: String,
        cursor: Option<String>,
        limit: usize,
    },
}
fn parse(limits: &crate::limits::RuntimeLimits, call: &Invocation) -> Result<ToolArgs> {
    call.validate(limits)
        .map_err(|_| Fault::Refused("invalid_input", "invalid collaboration invocation".into()))?;
    crate::mcp::validate_arguments(&call.tool, &call.arguments, limits)
        .map_err(|reason| Fault::Refused("invalid_input", reason))?;
    serde_json::from_value(json!({"tool":call.tool,"arguments":call.arguments})).map_err(|_| {
        Fault::Refused(
            "invalid_input",
            "tool arguments differ from closed schema".into(),
        )
    })
}
pub(super) fn cancel_operation(limits: &crate::limits::RuntimeLimits, call: &Invocation) -> Option<Operation> {
    match parse(limits, call).ok()? {
        ToolArgs::Cancel {
            task_id,
            expected_revision,
        } => Some(Operation::CancelTask {
            operation_id: call.operation_id.clone(),
            task_id,
            expected_revision,
        }),
        _ => None,
    }
}
pub(super) fn caller(tx: &Connection, actor: &Caller, now: u64) -> Result<StoredTask> {
    actor
        .validate(&tx.limits.transport)
        .map_err(|_| Fault::Refused("invalid_input", "invalid collaboration caller".into()))?;
    let task = operations::owned(
        tx,
        &actor.task_id,
        &actor.attempt_id,
        actor.generation,
        true,
    )?;
    let a = task
        .task
        .attempt
        .as_ref()
        .ok_or_else(|| Fault::Store("current caller lacks attempt".into()))?;
    if a.worker_id != actor.worker_id || a.worker_incarnation != actor.incarnation {
        return refuse("stale_owner", "collaboration worker binding differs");
    }
    let incarnation: Option<String> = tx
        .query_row(
            "SELECT body FROM workers WHERE id=?1",
            [&actor.worker_id],
            |r| r.get(0),
        )
        .optional()?;
    if incarnation
        .as_deref()
        .map(decode::<Worker>)
        .transpose()?
        .as_ref()
        .is_none_or(|w| w.incarnation != actor.incarnation)
    {
        return refuse(
            "stale_owner",
            "collaboration incarnation is no longer current",
        );
    }
    if a.uncertain || task.unresolved_execution || task.task.waiting_child_id.is_some() {
        return refuse(
            "reconciliation_required",
            "caller cannot start another collaboration effect",
        );
    }
    if now >= a.deadline_unix_ms || now >= task.task.input.limits.deadline_unix_ms {
        return refuse("deadline", "collaboration caller deadline expired");
    }
    if task.task.input.definition.execution_profile.is_none() {
        return refuse("not_configured", "collaboration profile is not configured");
    }
    Ok(task)
}
pub(super) fn lookup(tx: &Connection, actor: &Caller, id: &str) -> Result<(Data, bool)> {
    let limits = tx.limits;
    actor
        .validate(&limits.transport)
        .map_err(|_| Fault::Refused("invalid_input", "invalid lookup caller".into()))?;
    if !valid_id(limits, id) {
        return refuse("invalid_input", "invalid call identity");
    }
    let Some(receipt) = super::metadata::read(tx, id)? else {
        return refuse("not_found", "collaboration call has no committed outcome");
    };
    let Operation::CallCollaboration { call } = &receipt.request else {
        return refuse("not_found", "receipt is not a collaboration call");
    };
    if &call.caller != actor {
        return refuse("caller_mismatch", "receipt belongs to another caller");
    }
    // Unknown tool refusals are retained but never provide recursive GetOperation.
    call.validate(limits)
        .map_err(|_| Fault::Store("stored collaboration invocation is invalid".into()))?;
    Ok((
        Data::Operation(super::metadata::lookup_record(
            id,
            &receipt.request,
            &receipt.outcome,
        )?),
        false,
    ))
}
// Traceability scope: New accepted-caller operations bind retained metadata. The interface exposes its retained snapshot through collab_limits_get and explicit TaskInput.limits through collab_task_get.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//# Changing default metadata MUST NOT silently change the limits of an already accepted task.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//# The shared agent interface MUST expose the effective limits applicable to its caller.
pub(super) fn apply(
    tx: &Connection,
    call: &Invocation,
    cfg: &ControllerConfig,
    now: u64,
    validation: &Context<'_>,
) -> Result<(Data, bool)> {
    let original = caller(tx, &call.caller, now)?;
    original.task.input.execution_limits().map_err(|reason| Fault::Store(reason))?;
    let selected = original.task.input.resolved_limits.as_ref().ok_or_else(|| Fault::Store("accepted caller limits absent".into()))?;
    let limits = selected.global_metadata.as_ref();
    let bound_tx = Connection::new(tx, limits);
    let bound_validation = Context { issuer:validation.issuer, request:validation.request, proof:validation.proof, repository:validation.repository, limits };
    let result = apply_inner(&bound_tx, call, cfg, now, &bound_validation)?;
    let observed = (cfg.clock)();
    if observed < now {
        return refuse(
            "clock_unavailable",
            "clock moved backwards during collaboration call",
        );
    }
    if observed >= original.task.input.limits.deadline_unix_ms {
        return refuse("deadline", "collaboration caller expired before commit");
    }
    bounded(limits, result)
}
fn apply_inner(
    tx: &Connection,
    call: &Invocation,
    cfg: &ControllerConfig,
    now: u64,
    validation: &Context<'_>,
) -> Result<(Data, bool)> {
    let limits = tx.limits;
    let original = caller(tx, &call.caller, now)?;
    let actor = &call.caller;
    let id = call.operation_id.clone();
    let args = parse(limits, call)?;
    let query = |q| -> Result<(Data, bool)> {
        Ok((
            Data::Collaboration(super::collaboration_queries::apply(tx, &q, cfg)?),
            false,
        ))
    };
    let repository = |source, base, query| -> Result<(Data, bool)> {
        bounded(limits, (
            Data::Repository(Box::new(super::repository_queries::require(
                tx, validation, &original, source, base, query, now,
            )?)),
            false,
        ))
    };
    let inner = match args {
        ToolArgs::Repositories { after, limit } => return bounded(limits, query(Query::Repositories { after, limit })?),
        ToolArgs::RepositoryRegistration { repository_id, registration_revision } => return bounded(limits, query(Query::RepositoryRegistration { repository_id, registration_revision })?),
        ToolArgs::Limits {} => return query(Query::Limits { task_id: Some(call.caller.task_id.clone()) }),
        ToolArgs::Session { session_id } => return bounded(limits, query(Query::Session { session_id })?),
        ToolArgs::Sessions {
            agent_id,
            cursor,
            limit,
        } => {
            return bounded(limits, query(Query::Sessions {
                agent_id,
                after: cursor,
                limit,
            })?);
        }
        ToolArgs::RepositoryTree {
            source,
            path,
            after,
            limit,
        } => {
            return repository(
                source,
                None,
                crate::repository_read::Query::Tree { path, after, limit },
            );
        }
        ToolArgs::RepositoryRead {
            source,
            path,
            start_line,
            max_lines,
        } => {
            return repository(
                source,
                None,
                crate::repository_read::Query::ReadFile {
                    path,
                    start_line,
                    max_lines,
                },
            );
        }
        ToolArgs::RepositorySearch {
            source,
            path,
            needle,
        } => {
            return repository(
                source,
                None,
                crate::repository_read::Query::Search { path, needle },
            );
        }
        ToolArgs::RepositoryDiff {
            source,
            base_source,
            path,
        } => {
            return repository(
                source,
                Some(base_source),
                crate::repository_read::Query::Diff { path },
            );
        }
        ToolArgs::RepositoryHistory { source, limit } => {
            return repository(
                source,
                None,
                crate::repository_read::Query::History { limit },
            );
        }
        ToolArgs::CallGet { call_id } => return bounded(limits, lookup(tx, actor, &call_id)?),
        ToolArgs::Tasks {
            cursor,
            limit,
            agent_id,
            parent_task_id,
        } => {
            return list(
                tx,
                "tasks",
                &json!([agent_id, parent_task_id]),
                cursor,
                limit,
                cfg,
                now,
            );
        }
        ToolArgs::Agents { cursor, limit } => {
            return list(tx, "agents", &Value::Null, cursor, limit, cfg, now);
        }
        ToolArgs::Releases {
            task_id,
            cursor,
            limit,
        } => return list(tx, "releases", &json!(task_id), cursor, limit, cfg, now),
        ToolArgs::Task { task_id } => Operation::GetTask { task_id },
        ToolArgs::Agent { agent_id } => Operation::GetAgent { agent_id },
        ToolArgs::Definition {
            definition_id,
            revision,
        } => {
            return bounded(limits, query(Query::Definition {
                definition_id,
                revision,
            })?);
        }
        ToolArgs::MemoryKeys { cursor, limit } => {
            return bounded(limits, query(Query::MemoryKeys {
                agent_id: original.task.assigned_agent_id,
                after: cursor,
                limit,
            })?);
        }
        ToolArgs::Memory { key } => Operation::GetMemory {
            agent_id: original.task.assigned_agent_id.clone(),
            key,
        },
        ToolArgs::SetMemory {
            key,
            expected_revision,
            value,
        } => Operation::SetMemory {
            operation_id: id,
            task_id: actor.task_id.clone(),
            attempt_id: actor.attempt_id.clone(),
            generation: actor.generation,
            key,
            expected_revision,
            value,
        },
        ToolArgs::Progress { text } => Operation::ObserveWorker {
            report: Report {
                report_id: id,
                worker_id: actor.worker_id.clone(),
                incarnation: actor.incarnation.clone(),
                task_id: actor.task_id.clone(),
                attempt_id: actor.attempt_id.clone(),
                generation: actor.generation,
                observation: Observation::Progress {
                    text,
                    output: original.task.recent_output.clone(),
                    truncated: original.task.output_truncated,
                },
            },
        },
        ToolArgs::Delegate { instruction } => {
            if crate::delegation::template(&original.task.input).is_none() {
                return refuse("not_configured", "no configured child template");
            }
            let child = crate::delegation::coding_child(&original.task.input, &instruction)
                .map_err(|_| {
                    Fault::Refused(
                        "invalid_input",
                        "configured child instruction refused".into(),
                    )
                })?;
            Operation::DelegateTask {
                operation_id: id,
                parent_task_id: actor.task_id.clone(),
                parent_attempt_id: actor.attempt_id.clone(),
                generation: actor.generation,
                child,
            }
        }
        ToolArgs::Assign {
            task_id,
            expected_revision,
            agent_id,
        } => Operation::AssignTask {
            operation_id: id,
            task_id,
            expected_revision,
            agent_id,
        },
        ToolArgs::Cancel {
            task_id,
            expected_revision,
        } => Operation::CancelTask {
            operation_id: id,
            task_id,
            expected_revision,
        },
        ToolArgs::Retry {
            task_id,
            expected_revision,
        } => Operation::RetryTask {
            operation_id: id,
            task_id,
            expected_revision,
        },
        ToolArgs::Workers { cursor, limit } => {
            return bounded(limits, query(Query::Workers {
                after: cursor,
                limit,
            })?);
        }
        ToolArgs::Worker { worker_id } => return bounded(limits, query(Query::Worker { worker_id })?),
        ToolArgs::History {
            task_id,
            cursor,
            limit,
        } => {
            return bounded(limits, query(Query::TaskHistory {
                task_id,
                after: cursor,
                limit,
            })?);
        }
        ToolArgs::Attempts {
            task_id,
            cursor,
            limit,
        } => {
            return bounded(limits, query(Query::TaskAttempts {
                task_id,
                after: cursor,
                limit,
            })?);
        }
        ToolArgs::Checkpoints {
            task_id,
            cursor,
            limit,
        } => {
            return bounded(limits, query(Query::TaskCheckpoints {
                task_id,
                after: cursor,
                limit,
            })?);
        }
        ToolArgs::Release { release_id } => Operation::GetRelease { release_id },
        ToolArgs::Checks {
            release_id,
            cursor,
            limit,
        } => {
            return bounded(limits, query(Query::ReleaseChecks {
                release_id,
                after: cursor,
                limit,
            })?);
        }
    };
    let result = operations::apply(tx, &inner, cfg, now, validation)?;
    // Recheck the original immutable deadline at finalization. No database
    // transaction survives helper waits; validation dispatch re-enters here.
    let final_now = (cfg.clock)();
    if final_now < now {
        return refuse(
            "clock_unavailable",
            "clock moved backwards during collaboration call",
        );
    }
    if final_now >= original.task.input.limits.deadline_unix_ms {
        return refuse("deadline", "collaboration caller expired before commit");
    }
    bounded(limits, result)
}
fn bounded(limits: &crate::limits::RuntimeLimits, value: (Data, bool)) -> Result<(Data, bool)> {
    crate::protocol::bounded_json_size(&value.0, limits.interface.mcp_data_bytes).map_err(|_| {
        Fault::Refused(
            "response_bound",
            "collaboration result exceeds tool bound; use smaller pages: interface.mcp_data_bytes".into(),
        )
    })?;
    // Known repository operands cannot expose credential-bearing identities.
    let safe = match &value.0 {
        Data::Task(t) => !super::credential_admission::rejected(&Operation::SubmitTask {
            operation_id: "read-projection".into(),
            input: t.input.clone(),
        }),
        Data::Release(r) => {
            !crate::credential::repository_contains_credentials(&r.checkpoint.repository)
                && !crate::credential::repository_contains_credentials(&r.checkpoint.remote_ref)
        }
        _ => true,
    };
    if !safe {
        return refuse(
            "credential_metadata",
            "credential-bearing metadata is not exposed",
        );
    }
    Ok(value)
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    kind: String,
    scope: Value,
    position: String,
}
pub(super) fn task_page(
    tx: &Connection,
    cfg: &ControllerConfig,
    agent_id: &Option<String>,
    parent_task_id: &Option<String>,
    after: &Option<String>,
    limit: usize,
) -> Result<ListPage> {
    let limits = tx.limits;
    if agent_id.as_deref().is_some_and(|s| !valid_id(limits, s))
        || parent_task_id.as_deref().is_some_and(|s| !valid_id(limits, s))
    {
        return refuse("invalid_input", "invalid task filter");
    }
    let (data, _) = list(
        tx,
        "tasks",
        &json!([agent_id, parent_task_id]),
        after.clone(),
        limit,
        cfg,
        (cfg.clock)(),
    )?;
    match data {
        Data::CollaborationPage(page) => Ok(page),
        _ => Err(Fault::Store("task page shape".into())),
    }
}
fn list(
    tx: &Connection,
    kind: &str,
    scope: &Value,
    after: Option<String>,
    limit: usize,
    cfg: &ControllerConfig,
    now: u64,
) -> Result<(Data, bool)> {
    let limits = tx.limits;
    if limit == 0 { return refuse("invalid_input", "invalid collaboration page limit"); }
    if limit > limits.controller.collaboration_page_items {
        return refuse("invalid_input", "invalid collaboration page limit: controller.collaboration_page_items");
    }
    if limit > limits.controller.max_list_tasks {
        return refuse("invalid_input", "invalid collaboration page limit: controller.max_list_tasks");
    }
    let start = match after {
        None => String::new(),
        Some(raw) => {
            if raw.len() > limits.controller.collaboration_cursor_bytes {
                return refuse("invalid_input", "cursor bound: controller.collaboration_cursor_bytes");
            }
            let c: Cursor = serde_json::from_str(&raw).map_err(|_| {
                Fault::Refused("invalid_input", "invalid collaboration cursor".into())
            })?;
            if c.kind != kind || c.scope != *scope || !valid_id(limits, &c.position) || encode(&c)? != raw {
                return refuse("invalid_input", "cursor differs from query scope");
            };
            c.position
        }
    };
    let cursor = |position: String| {
        encode(&Cursor {
            kind: kind.into(),
            scope: scope.clone(),
            position,
        })
    };
    let mut values = Vec::new();
    if kind == "tasks" {
        let seq = if start.is_empty() {
            0
        } else {
            start
                .parse::<i64>()
                .ok()
                .filter(|v| *v > 0 && v.to_string() == start)
                .ok_or_else(|| Fault::Refused("invalid_input", "invalid task cursor".into()))?
        };
        let agent = scope[0].as_str();
        let parent = scope[1].as_str();
        // Parent filtering is bounded by scanned rows, not an unbounded walk.
        let mut q=tx.prepare("SELECT id,seq FROM tasks WHERE seq>?1 AND (?2 IS NULL OR agent=?2) ORDER BY seq LIMIT ?3")?;
        let rows = q
            .query_map(params![seq, agent, (limit + 1) as i64], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for (id, seq) in rows {
            let t = operations::view(tx, load(tx, &id)?, cfg, now)?;
            let keep = parent.is_none_or(|p| t.parent_task_id.as_deref() == Some(p));
            let value=keep.then(||json!({"id":t.id,"session_id":t.session_id,"revision":t.revision,"status":t.status,"assigned_agent_id":t.assigned_agent_id,"definition":t.input.definition,"dependencies":t.input.dependencies,"parent_task_id":t.parent_task_id,"attempt":t.attempt,"has_result":t.result.is_some(),"has_checkpoint":t.selected_checkpoint.is_some()}));
            values.push((seq.to_string(), value));
        }
    } else if kind == "agents" {
        let mut q =
            tx.prepare("SELECT id FROM agents WHERE id>?1 ORDER BY id COLLATE BINARY LIMIT ?2")?;
        let rows = q
            .query_map(params![start, (limit + 1) as i64], |r| {
                r.get::<_, String>(0)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for id in rows {
            let (data, _) = super::agents::apply(
                tx,
                &Operation::GetAgent {
                    agent_id: id.clone(),
                },
                cfg,
                now,
            )?;
            let Data::Agent(agent) = data else {
                unreachable!()
            };
            values.push((id, Some(serde_json::to_value(agent)?)));
        }
    } else {
        let task_id = scope
            .as_str()
            .ok_or_else(|| Fault::Store("release scope".into()))?;
        load(tx, task_id)?;
        let mut q=tx.prepare("SELECT id FROM releases WHERE task_id=?1 AND id>?2 ORDER BY id COLLATE BINARY LIMIT ?3")?;
        let rows = q
            .query_map(params![task_id, start, (limit + 1) as i64], |r| {
                r.get::<_, String>(0)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for id in rows {
            let data = operations::apply(
                tx,
                &Operation::GetRelease {
                    release_id: id.clone(),
                },
                cfg,
                now,
                &Context {
                    limits,
                    issuer: &std::sync::Arc::new(()),
                    request: "read",
                    proof: None,
                    repository: None,
                },
            )?;
            let (data, _) = bounded(limits, data)?;
            let Data::Release(release) = data else {
                unreachable!()
            };
            values.push((id, Some(serde_json::to_value(release)?)));
        }
    }
    let more = values.len() > limit;
    let mut page = ListPage {
        items: vec![],
        next_cursor: None,
    };
    let mut last = None;
    for (position, item) in values.into_iter().take(limit) {
        if let Some(item) = item {
            page.items.push(item);
            page.next_cursor = Some(cursor(position.clone())?);
            if crate::protocol::bounded_json_size(&page, limits.controller.collaboration_item_bytes).is_err() {
                page.items.pop();
                if page.items.is_empty() {
                    return refuse(
                        "response_bound",
                        "first collaboration page item exceeds bound",
                    );
                };
                page.next_cursor = last;
                return bounded(limits, (Data::CollaborationPage(page), false));
            }
        }
        last = Some(cursor(position)?);
    }
    page.next_cursor = if more { last } else { None };
    bounded(limits, (Data::CollaborationPage(page), false))
}
