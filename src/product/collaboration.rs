//! Operator read views over the same bounded records exposed through MCP.
//! No browser request manufactures an active agent/worker caller.
use super::*;
use crate::collaboration::{Query, QueryResult};

fn fields(query: &str, allowed: &[&str], metadata: &RuntimeLimits) -> Result<Form, FormError> {
    let f = parse_form(query.as_bytes(), metadata)?;
    fields_only(&f, allowed)?;
    Ok(f)
}
fn after(f: &Form) -> Option<String> {
    f.get("after").filter(|v| !v.is_empty()).cloned()
}
fn id(raw: &str, metadata: &RuntimeLimits) -> Result<String, String> {
    let v = decode(raw, false)?;
    if !crate::mcp::valid_identity(&v, metadata.transport.identity_bytes) {
        return Err("Invalid record identity".into());
    }
    Ok(v)
}
fn json(value: &impl serde::Serialize) -> String {
    format!(
        "<pre>{}</pre>",
        escape(&serde_json::to_string_pretty(value).unwrap_or_default())
    )
}
fn next(body: &mut String, route: &str, cursor: Option<&str>) {
    if let Some(c) = cursor {
        let _ = write!(
            body,
            "<p><a href=\"{}?after={}\">Next page</a></p>",
            route,
            encode(c)
        );
    }
}
async fn query(controller: &Controller, query: Query) -> Result<QueryResult, UiResponse> {
    let reply = call(controller, Operation::QueryCollaboration { query }).await;
    match reply.outcome {
        Outcome::Ok {
            data: Data::Collaboration(value),
        } => Ok(value),
        _ => Err(not_task(reply)),
    }
}
pub(super) fn index(workflow_enabled: bool) -> UiResponse {
    let mut body: String = "<p>Browse the same committed collaboration information available to agents.</p><ul><li><a href=\"/tasks\">Tasks, progress and results</a> — each task links to its history, attempts, checkpoints, repository, child tasks and release candidates.</li><li><a href=\"/agents\">Agents, sessions and shared memory</a> — recorded agent identities, memory and accepted agent revisions.</li><li><a href=\"/workers\">Workers and capacity</a> — current registered incarnation and controller-known occupancy.</li></ul><p>Cancellation and retry use the existing task controls. Agent progress reports, configured child delegation and memory writes remain tied to the actual executing agent; this interface displays their committed records and does not act as that agent. Release checks are observations; this interface cannot grant protected release or rollout authority.</p>".into();
    if workflow_enabled {
        //= spec/impl/v2/implementation.md#collaboration-surfaces-preserve-existing-authority
        //# The browser interface MUST expose every supported collaboration operation permitted to its caller.
        body.push_str("<h2>Collaboration workflows</h2><ul><li><a href=\"/documents\">Documents and immutable versions</a> · <a href=\"/documents/new\">Create a document</a></li><li><a href=\"/questions\">Questions and answer history</a> · <a href=\"/questions/new\">Pose a question</a></li><li><a href=\"/reviews\">Exact-subject reviews</a></li><li><a href=\"/decisions\">Your decision queue</a></li></ul><p>Each task links to its collaboration owner, resolver waits, comments and available accepted-result review targets. Product decisions do not authorize protected actions.</p>");
    }
    page(200, "Collaboration", body)
}

pub(super) async fn get(
    controller: &Controller,
    path: &str,
    raw_query: &str,
) -> Option<UiResponse> {
    match route(controller, path, raw_query).await {
        Ok(result) => result,
        Err(response) => Some(response),
    }
}
async fn route(
    controller: &Controller,
    path: &str,
    raw_query: &str,
) -> Result<Option<UiResponse>, UiResponse> {
    if path == "/limits" {
        let f = fields(raw_query, &["task_id"], controller.limits()).map_err(|e| error(400, &e))?;
        let task_id = f.get("task_id").filter(|v| !v.is_empty()).cloned();
        let QueryResult::Limits { global, execution } = query(
            controller,
            Query::Limits {
                task_id: task_id.clone(),
            },
        )
        .await?
        else {
            return Err(error(502, "Unexpected limits metadata projection"));
        };
        let mut body = String::from(
            "<p>Current global metadata and, when a task is selected, the exact execution metadata retained at its admission. Reading metadata does not change it or grant execution authority.</p><form method=\"get\" action=\"/limits\">",
        );
        body.push_str(&field(
            "task_id",
            "Task ID (optional)",
            task_id.as_deref().unwrap_or(""),
            "text",
            false,
        ));
        body.push_str("<button>Read metadata</button></form><h2>Current global metadata</h2>");
        body.push_str(&json(&global));
        if let Some(execution) = execution {
            body.push_str("<h2>Recorded task execution metadata</h2>");
            body.push_str(&json(&execution));
        }
        if let Some(task_id) = task_id {
            // The accepted input is immutable. This ordinary task read adds its
            // explicit bounds without treating current defaults as task values.
            let reply = call(controller, Operation::GetTask { task_id }).await;
            match reply.outcome {
                Outcome::Ok { data: Data::Task(task) } => {
                    body.push_str(&task_limits_view(&task));
                    let _ = write!(body, "<p><a href=\"{}/settings?task_id={}\">Compare selected and accepted agent settings</a></p>", agents::path(&task.input.definition.id), encode(&task.id));
                },
                _ => return Err(not_task(reply)),
            }
        }
        return Ok(Some(page(200, "Limits metadata", body)));
    }

    if path == "/collaboration" {
        fields(raw_query, &[], controller.limits()).map_err(|e| error(400, &e))?;
        return Ok(Some(index(controller.limits().workflow.is_some())));
    }
    if path == "/tasks"
        && (raw_query.contains("agent_id=") || raw_query.contains("parent_task_id="))
    {
        let f = fields(
            raw_query,
            &["agent_id", "parent_task_id", "after"],
            controller.limits(),
        )
        .map_err(|e| error(400, &e))?;
        let agent_id = f.get("agent_id").filter(|x| !x.is_empty()).cloned();
        let parent_task_id = f.get("parent_task_id").filter(|x| !x.is_empty()).cloned();
        let QueryResult::Tasks(tasks) = query(
            controller,
            Query::Tasks {
                agent_id: agent_id.clone(),
                parent_task_id: parent_task_id.clone(),
                after: after(&f),
                limit: controller.limits().product.page_items,
            },
        )
        .await?
        else {
            return Err(error(502, "Unexpected task summary projection"));
        };
        let mut body = String::from(
            "<p><a href=\"/tasks\">All tasks</a></p><p>Filtered committed tasks. A page can contain no matches while later pages remain.</p><table><thead><tr><th>Task</th><th>Agent</th><th>Status</th></tr></thead><tbody>",
        );
        for item in &tasks.items {
            let id = item
                .get("id")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| error(502, "Task summary has no identity"))?;
            let agent = item
                .get("assigned_agent_id")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| error(502, "Task summary has no agent"))?;
            let status = item
                .get("status")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| error(502, "Task summary has no status"))?;
            let _ = write!(
                body,
                "<tr><td><a href=\"{}\">{}</a></td><td><a href=\"{}\">{}</a></td><td>{}</td></tr>",
                task_path(id),
                escape(id),
                agents::path(agent),
                escape(agent),
                escape(status)
            );
        }
        body.push_str("</tbody></table>");
        if tasks.items.is_empty() {
            body.push_str("<p>No matching tasks on this page.</p>");
        }
        if let Some(cursor) = tasks.next_cursor {
            let _ = write!(
                body,
                "<p><a href=\"/tasks?agent_id={}&amp;parent_task_id={}&amp;after={}\">Next filtered page</a></p>",
                encode(agent_id.as_deref().unwrap_or("")),
                encode(parent_task_id.as_deref().unwrap_or("")),
                encode(&cursor)
            );
        }
        return Ok(Some(page(200, "Filtered tasks", body)));
    }
    if path == "/workers" {
        let f = fields(raw_query, &["after"], controller.limits()).map_err(|e| error(400, &e))?;
        let QueryResult::Workers(workers) = query(
            controller,
            Query::Workers {
                after: after(&f),
                limit: controller.limits().product.page_items,
            },
        )
        .await?
        else {
            return Err(error(502, "Unexpected worker projection"));
        };
        let mut body = String::from(
            "<p>Capacity is the controller's current registered view. Free capacity is not a reservation or proof of host availability.</p><table><thead><tr><th>Worker</th><th>Incarnation</th><th>Coding occupied / capacity</th><th>Native occupied / capacity</th></tr></thead><tbody>",
        );
        for w in &workers.items {
            let _ = write!(
                body,
                "<tr><td><a href=\"/workers/{}\">{}</a></td><td>{}</td><td>{} / {}</td><td>{} / {}</td></tr>",
                encode(&w.worker.id),
                escape(&w.worker.id),
                escape(&w.worker.incarnation),
                w.coding_occupied,
                w.worker.coding_capacity,
                w.native_occupied,
                w.worker.native_capacity
            );
        }
        body.push_str("</tbody></table>");
        if workers.items.is_empty() {
            body.push_str("<p>No workers on this page.</p>");
        }
        next(&mut body, path, workers.next_cursor.as_deref());
        return Ok(Some(page(200, "Workers", body)));
    }
    if let Some(raw) = path.strip_prefix("/workers/").filter(|x| !x.contains('/')) {
        fields(raw_query, &[], controller.limits()).map_err(|e| error(400, &e))?;
        let worker_id = id(raw, controller.limits()).map_err(|e| error(400, &e))?;
        let QueryResult::Worker(w) = query(controller, Query::Worker { worker_id }).await? else {
            return Err(error(502, "Unexpected worker projection"));
        };
        return Ok(Some(page(
            200,
            &format!("Worker {}", w.worker.id),
            format!(
                "<p><a href=\"/workers\">All workers</a></p><p>Observed registration and occupancy; no liveness or deployment authority is inferred.</p>{}",
                json(&w)
            ),
        )));
    }
    if let Some(raw) = path
        .strip_prefix("/definitions/")
        .filter(|x| !x.contains('/'))
    {
        let f = fields(raw_query, &["revision"], controller.limits()).map_err(|e| error(400, &e))?;
        let definition_id = id(raw, controller.limits()).map_err(|e| error(400, &e))?;
        let revision = required(&f, "revision").map_err(|e| error(400, &e))?.to_string();
        let QueryResult::Definition(d) = query(
            controller,
            Query::Definition {
                definition_id,
                revision,
            },
        )
        .await?
        else {
            return Err(error(502, "Unexpected agent definition projection"));
        };
        return Ok(Some(page(
            200,
            "Immutable agent definition",
            format!(
                "<p>This is the exact retained agent revision; its settings are not supplied by a worker.</p>{}",
                json(&d)
            ),
        )));
    }
    if let Some((raw, sub)) = path
        .strip_prefix("/agents/")
        .and_then(|r| r.split_once('/'))
    {
        if sub == "memory" || sub.starts_with("memory/") {
            let agent_id = id(raw, controller.limits()).map_err(|e| error(400, &e))?;
            let agent = call(
                controller,
                Operation::GetAgent {
                    agent_id: agent_id.clone(),
                },
            )
            .await;
            if !matches!(
                agent.outcome,
                Outcome::Ok {
                    data: Data::Agent(_)
                }
            ) {
                return Err(not_task(agent));
            }
            let route = format!("/agents/{}/memory", encode(&agent_id));
            let mut body = format!(
                "<p><a href=\"{}\">Agent</a></p><p>Memory is shown for this exact recorded agent ID. A displayed memory revision does not establish that any particular session consumed it. This is a read-only operator view; writes require the actual current agent caller.</p>",
                agents::path(&agent_id)
            );
            if sub == "memory" {
                let f = fields(raw_query, &["after"], controller.limits()).map_err(|e| error(400, &e))?;
                let QueryResult::MemoryKeys(keys) = query(
                    controller,
                    Query::MemoryKeys {
                        agent_id,
                        after: after(&f),
                        limit: controller.limits().product.page_items,
                    },
                )
                .await?
                else {
                    return Err(error(502, "Unexpected memory projection"));
                };
                body.push_str(
                    "<table><thead><tr><th>Key</th><th>Revision</th></tr></thead><tbody>",
                );
                for key in &keys.items {
                    let _ = write!(
                        body,
                        "<tr><td><a href=\"{}/{}\">{}</a></td><td>{}</td></tr>",
                        route,
                        encode(&key.key),
                        escape(&key.key),
                        key.revision
                    );
                }
                body.push_str("</tbody></table>");
                if keys.items.is_empty() {
                    body.push_str("<p>No memory keys on this page.</p>");
                }
                next(&mut body, &route, keys.next_cursor.as_deref());
            } else {
                fields(raw_query, &[], controller.limits()).map_err(|e| error(400, &e))?;
                let key = id(sub.strip_prefix("memory/").unwrap(), controller.limits())
                    .map_err(|e| error(400, &e))?;
                let reply = call(controller, Operation::GetMemory { agent_id, key }).await;
                match reply.outcome {
                    Outcome::Ok {
                        data: Data::Memory(value),
                    } => {
                        body.push_str(&format!("<p><a href=\"{route}\">All memory keys</a></p>"));
                        if let Some(value) = value {
                            body.push_str(&json(&value));
                        } else {
                            body.push_str("<p>This key has no recorded value.</p>");
                        }
                    }
                    _ => return Err(not_task(reply)),
                }
            }
            return Ok(Some(page(200, "Agent memory", body)));
        }
    }
    if let Some((raw, sub)) = path.strip_prefix("/tasks/").and_then(|r| r.split_once('/')) {
        if ["history", "attempts", "checkpoints"].contains(&sub) {
            let task_id = id(raw, controller.limits()).map_err(|e| error(400, &e))?;
            let f = fields(raw_query, &["after"], controller.limits()).map_err(|e| error(400, &e))?;
            let q = match sub {
                "history" => Query::TaskHistory {
                    task_id: task_id.clone(),
                    after: after(&f),
                    limit: controller.limits().product.page_items,
                },
                "attempts" => Query::TaskAttempts {
                    task_id: task_id.clone(),
                    after: after(&f),
                    limit: controller.limits().product.page_items,
                },
                _ => Query::TaskCheckpoints {
                    task_id: task_id.clone(),
                    after: after(&f),
                    limit: controller.limits().product.page_items,
                },
            };
            let value = query(controller, q).await?;
            let mut body = format!("<p><a href=\"{}\">Task</a></p>", task_path(&task_id));
            let (title, cursor) = match value {
                QueryResult::TaskHistory(p) => {
                    body.push_str("<p>Committed task events; this is not a complete provider conversation.</p>");
                    for item in &p.items {
                        let _ = write!(
                            body,
                            "<article><h2>{} · event {}</h2><p>Task revision {}</p>{}</article>",
                            escape(&item.kind),
                            item.sequence,
                            item.task_revision,
                            json(&item.body)
                        );
                    }
                    ("Task history", p.next_cursor)
                }
                QueryResult::TaskAttempts(p) => {
                    body.push_str("<p>Attempts, observed usage and execution outcomes. A completed executor is not necessarily an accepted task result.</p>");
                    for item in &p.items {
                        body.push_str(&format!(
                            "<article><h2>{}</h2>{}</article>",
                            escape(&item.attempt.id),
                            json(item)
                        ));
                    }
                    ("Task attempts", p.next_cursor)
                }
                QueryResult::TaskCheckpoints(p) => {
                    body.push_str("<p>Exact publication records and selected checkpoint. A published checkpoint is not proof of acceptance or rollout.</p>");
                    for item in &p.items {
                        body.push_str(&format!(
                            "<article><h2>{}</h2>{}</article>",
                            escape(&item.operation_id),
                            json(item)
                        ));
                    }
                    ("Task checkpoints", p.next_cursor)
                }
                _ => return Err(error(502, "Unexpected task projection")),
            };
            next(&mut body, path, cursor.as_deref());
            return Ok(Some(page(200, title, body)));
        }
    }
    if let Some((raw, "checks")) = path
        .strip_prefix("/releases/")
        .and_then(|r| r.split_once('/'))
    {
        let release_id = id(raw, controller.limits()).map_err(|e| error(400, &e))?;
        let f = fields(raw_query, &["after"], controller.limits()).map_err(|e| error(400, &e))?;
        let QueryResult::ReleaseChecks(checks) = query(
            controller,
            Query::ReleaseChecks {
                release_id: release_id.clone(),
                after: after(&f),
                limit: controller.limits().product.page_items,
            },
        )
        .await?
        else {
            return Err(error(502, "Unexpected release check projection"));
        };
        let mut body = format!(
            "<p><a href=\"/releases/{}\">Release candidate and protected stages</a></p><p>Recorded checks retain their exact subject, producer and expiry. Presence is not current eligibility or human rollout approval.</p>",
            encode(&release_id)
        );
        for item in &checks.items {
            body.push_str(&format!(
                "<article><h2>{}</h2>{}</article>",
                escape(&item.operation_id),
                json(item)
            ));
        }
        if checks.items.is_empty() {
            body.push_str("<p>No checks on this page.</p>");
        }
        next(&mut body, path, checks.next_cursor.as_deref());
        return Ok(Some(page(200, "Release checks", body)));
    }
    Ok(None)
}
