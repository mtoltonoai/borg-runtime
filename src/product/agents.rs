//! Identity directory and bounded current-assignment views from controller state.
use super::*;

pub(super) fn path(id: &str) -> String {
    format!("/agents/{}", encode(id))
}

fn definition(value: &Definition) -> String {
    format!(
        "{} @ {} ({})",
        value.id,
        value.revision,
        if value.executor == ExecutorKind::Coding {
            "coding"
        } else {
            "native"
        }
    )
}

//= spec/impl/v2/implementation.md#shared-product-state
//# The browser interface MUST provide an agent list.
pub(super) async fn get(controller: &Controller, route: &str, query: &str) -> UiResponse {
    let fields = match parse_form(query.as_bytes(), controller.limits()) {
        Ok(fields) => fields,
        Err(message) => return error(400, &message),
    };
    if let Err(message) = fields_only(&fields, &["after"]) {
        return error(400, &message);
    }
    let after = fields.get("after").filter(|s| !s.is_empty()).cloned();
    if route == "/agents" {
        let reply = call(
            controller,
            Operation::ListAgents {
                after,
                limit: controller.limits().product.page_items,
            },
        )
        .await;
        return match reply.outcome {
            Outcome::Ok {
                data: Data::Agents(agents),
            } => {
                let mut body = String::from("<p><a href=\"/agents\">Refresh agents</a></p>");
                if agents.is_empty() {
                    body.push_str("<p>No agents found.</p>");
                } else {
                    body.push_str("<table><caption>Agents</caption><thead><tr><th scope=\"col\">Agent</th><th scope=\"col\">Latest recorded definition</th><th scope=\"col\">Assigned tasks</th></tr></thead><tbody>");
                    for agent in &agents {
                        let _ = write!(
                            body,
                            "<tr><th scope=\"row\"><a href=\"{}\">{}</a></th><td>{}</td><td><a href=\"{}\">View assigned tasks</a></td></tr>",
                            path(&agent.id),
                            escape(&agent.id),
                            escape(&definition(&agent.definition)),
                            path(&agent.id)
                        );
                    }
                    body.push_str("</tbody></table>");
                    if let Some(last) = agents.last() {
                        let _ = write!(
                            body,
                            "<p><a href=\"/agents?after={}\">Next agent page</a></p>",
                            encode(&last.id)
                        );
                    }
                }
                page(200, "Agents", body)
            }
            _ => not_task(reply),
        };
    }
    let id = match route
        .strip_prefix("/agents/")
        .filter(|id| !id.contains('/'))
        .and_then(|id| decode(id, false).ok())
        .filter(|id| !id.is_empty())
    {
        Some(id) => id,
        None => return error(400, "Invalid agent identity route"),
    };
    let reply = call(
        controller,
        Operation::GetAgent {
            agent_id: id.clone(),
        },
    )
    .await;
    let agent = match reply.outcome {
        Outcome::Ok {
            data: Data::Agent(agent),
        } => agent,
        _ => return not_task(reply),
    };
    let reply = call(
        controller,
        Operation::ListAgentTasks {
            agent_id: id.clone(),
            after,
            limit: controller.limits().product.page_items,
        },
    )
    .await;
    let tasks = match reply.outcome {
        Outcome::Ok {
            data: Data::Tasks(tasks),
        } => tasks,
        _ => return not_task(reply),
    };
    if tasks.iter().any(|task| task.assigned_agent_id != id) {
        return error(502, "Controller returned a task assigned to another agent");
    }
    let mut body = format!(
        "<p><a href=\"/agents\">All agents</a> | <a href=\"{}\">Refresh assigned tasks</a></p><dl>",
        path(&id)
    );
    term(&mut body, "Agent", &agent.id);
    term(
        &mut body,
        "Latest recorded definition",
        &definition(&agent.definition),
    );
    body.push_str("</dl>");
    let _ = write!(
        body,
        "<p>Sessions of this agent share its durable memory; each session keeps its own accepted revision and task context.</p><p><a href=\"/tasks/new?agent_id={}\">Start a new session</a> · <a href=\"/sessions?agent_id={}\">All sessions</a></p>",
        encode(&id),
        encode(&id)
    );
    let _ = write!(
        body,
        "<p><a href=\"/agents/{}/memory\">Shared agent memory</a> · <a href=\"/definitions/{}?revision={}\">Exact agent revision</a></p>",
        encode(&id),
        encode(&agent.definition.id),
        encode(&agent.definition.revision)
    );
    if tasks.is_empty() {
        body.push_str("<p>No tasks on this page.</p>");
    } else {
        body.push_str("<table><caption>Assigned tasks</caption><thead><tr><th scope=\"col\">Task</th><th scope=\"col\">Task definition</th><th scope=\"col\">Status</th><th scope=\"col\">Execution and waits</th></tr></thead><tbody>");
        for task in &tasks {
            let _ = write!(
                body,
                "<tr><th scope=\"row\"><a href=\"{}\">{}</a></th><td>{}</td><td>{}</td><td>",
                task_path(&task.id),
                escape(&task.id),
                escape(&definition(&task.input.definition)),
                status(task.status)
            );
            if let Some(reason) = &task.blocked_reason {
                let _ = write!(body, "<p>Blocker: {}</p>", escape(reason));
            }
            if let Some(child) = &task.waiting_child_id {
                let _ = write!(
                    body,
                    "<p>Waiting for child: <a href=\"{}\">{}</a></p>",
                    task_path(child),
                    escape(child)
                );
            }
            if let Some(parent) = &task.parent_task_id {
                let _ = write!(
                    body,
                    "<p>Parent task: <a href=\"{}\">{}</a></p>",
                    task_path(parent),
                    escape(parent)
                );
            }
            if let Some(attempt) = &task.attempt {
                let _ = write!(
                    body,
                    "<p>Attempt phase: {}; termination confirmed: {}</p>",
                    phase(attempt.phase),
                    if attempt.stopped { "yes" } else { "no" }
                );
                if attempt.uncertain {
                    body.push_str(
                        "<p>Unresolved outcome: execution or effects remain uncertain.</p>",
                    );
                }
            } else {
                body.push_str("<p>No current attempt.</p>");
            }
            body.push_str("</td></tr>");
        }
        body.push_str("</tbody></table>");
        if let Some(last) = tasks.last() {
            let _ = write!(
                body,
                "<p><a href=\"{}?after={}\">Next assigned-task page</a></p>",
                path(&id),
                encode(&last.id)
            );
        }
    }
    page(200, &format!("Agent {}", agent.id), body)
}
