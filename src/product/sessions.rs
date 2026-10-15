//! Bounded durable sessions, distinct from agent identities and execution attempts.
use super::*;
use crate::collaboration::{Query, QueryResult};
pub(super) fn path(id: &str) -> String {
    format!("/sessions/{}", encode(id))
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
pub(super) async fn get(controller: &Controller, route: &str, raw_query: &str) -> UiResponse {
    match view(controller, route, raw_query).await {
        Ok(page) | Err(page) => page,
    }
}
async fn view(
    controller: &Controller,
    route: &str,
    raw_query: &str,
) -> Result<UiResponse, UiResponse> {
    let invalid = |e: String| error(400, &e);
    let fields = parse_form(raw_query.as_bytes(), controller.limits()).map_err(invalid)?;
    if route == "/sessions" {
        fields_only(&fields, &["agent_id", "after"]).map_err(invalid)?;
        let agent_id = fields.get("agent_id").filter(|v| !v.is_empty()).cloned();
        let after = fields.get("after").filter(|v| !v.is_empty()).cloned();
        let QueryResult::Sessions(sessions) = query(
            controller,
            Query::Sessions {
                agent_id: agent_id.clone(),
                after,
                limit: controller.limits().product.page_items,
            },
        )
        .await?
        else {
            return Err(error(502, "Unexpected session page"));
        };
        let mut body = String::from(
            "<p>Each committed task has a durable session pinned to its accepted agent revision. Attempts are executions within that session. Sessions of one agent share agent memory and can run concurrently.</p><form method=\"get\" action=\"/sessions\">",
        );
        body.push_str(&field(
            "agent_id",
            "Agent ID",
            agent_id.as_deref().unwrap_or(""),
            "text",
            false,
        ));
        body.push_str("<button>Filter sessions</button></form><table><thead><tr><th>Session</th><th>Agent</th><th>Accepted revision</th><th>Task</th></tr></thead><tbody>");
        for session in &sessions.items {
            if agent_id.as_ref().is_some_and(|id| id != &session.agent_id) {
                return Err(error(502, "Session belongs to another agent"));
            }
            let _ = write!(
                body,
                "<tr><td><a href=\"{}\">{}</a></td><td><a href=\"{}\">{}</a></td><td>{}</td><td><a href=\"{}\">{}</a></td></tr>",
                path(&session.id),
                escape(&session.id),
                agents::path(&session.agent_id),
                escape(&session.agent_id),
                escape(&session.definition.revision),
                task_path(&session.task_id),
                escape(&session.task_id)
            );
        }
        body.push_str("</tbody></table>");
        if sessions.items.is_empty() {
            body.push_str("<p>No sessions on this page.</p>");
        }
        if let Some(after) = sessions.next_cursor {
            let _ = write!(
                body,
                "<p><a href=\"/sessions?agent_id={}&amp;after={}\">Next session page</a></p>",
                encode(agent_id.as_deref().unwrap_or("")),
                encode(&after)
            );
        }
        return Ok(page(200, "Sessions", body));
    }
    fields_only(&fields, &[]).map_err(invalid)?;
    let id = route
        .strip_prefix("/sessions/")
        .filter(|s| !s.contains('/'))
        .and_then(|s| decode(s, false).ok())
        .filter(|s| crate::mcp::valid_identity(s, controller.limits().transport.identity_bytes))
        .ok_or_else(|| error(400, "Invalid session identity"))?;
    let QueryResult::Session(session) = query(
        controller,
        Query::Session {
            session_id: id.clone(),
        },
    )
    .await?
    else {
        return Err(error(502, "Unexpected session record"));
    };
    let reply = call(
        controller,
        Operation::GetTask {
            task_id: session.task_id.clone(),
        },
    )
    .await;
    let Outcome::Ok {
        data: Data::Task(task),
    } = reply.outcome
    else {
        return Err(not_task(reply));
    };
    if session.id != id
        || task.session_id != session.id
        || task.assigned_agent_id != session.agent_id
        || task.input.definition != session.definition
    {
        return Err(error(502, "Session and task association disagree"));
    }
    let mut body = format!(
        "<p><a href=\"/sessions\">All sessions</a> · <a href=\"{}\">Agent</a> · <a href=\"/agents/{}/memory\">Shared agent memory</a> · <a href=\"{}\">Task and actions</a></p><dl>",
        agents::path(&session.agent_id),
        encode(&session.agent_id),
        task_path(&task.id)
    );
    term(&mut body, "Session", &session.id);
    term(&mut body, "Agent", &session.agent_id);
    term(
        &mut body,
        "Accepted agent revision",
        &session.definition.revision,
    );
    term(&mut body, "Task status", status(task.status));
    if let Some(attempt) = &task.attempt {
        term(&mut body, "Current attempt", &attempt.id);
        term(&mut body, "Attempt phase", phase(attempt.phase));
        term(
            &mut body,
            "Execution uncertain",
            if attempt.uncertain { "yes" } else { "no" },
        );
        term(
            &mut body,
            "Stopped",
            if attempt.stopped { "yes" } else { "no" },
        );
    }
    body.push_str("</dl><h2>Pinned agent definition</h2><pre>");
    body.push_str(&escape(
        &serde_json::to_string_pretty(&session.definition).unwrap_or_default(),
    ));
    body.push_str("</pre>");
    Ok(page(200, &format!("Session {}", session.id), body))
}
