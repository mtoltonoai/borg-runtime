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
    let fields = parse_form(raw_query.as_bytes(), controller.limits()).map_err(|e| error(400, &e))?;
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
            "<p>Each session is pinned to its accepted agent revision. Reassigning a task creates a new session while preserving earlier sessions and their history. Attempts are executions within a session. Sessions of one agent share agent memory and can run concurrently.</p><form method=\"get\" action=\"/sessions\">",
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
    let QueryResult::SessionInput { session, input, current } = query(
        controller,
        Query::SessionInput {
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
    if session.id != id || task.id != session.task_id || input.definition != session.definition {
        return Err(error(502, "Session and accepted input association disagree"));
    }
    // These reads may straddle a committed handoff. A formerly current session
    // becomes historical; it never borrows a successor's attempt or settings.
    let current = current && task.session_id == session.id;
    render(&session, &input, current, &task)
}
// Traceability scope: Historical detail renders its retained accepted input and never borrows a successor attempt or current-settings association.
//= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
//# A task reassignment MUST retain the task's prior session and execution-input history.
fn render(session: &Session, input: &TaskInput, current: bool, task: &Task) -> Result<UiResponse, UiResponse> {
    if input.definition != session.definition || task.id != session.task_id
        || (current && (task.session_id != session.id || task.assigned_agent_id != session.agent_id
            || task.input.definition != session.definition))
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
    term(&mut body, "Session authority", if current { "Current" } else { "Historical — authority closed" });
    term(&mut body, "Current task status", status(task.status));
    if let Some(attempt) = task.attempt.as_ref().filter(|attempt| current && attempt.session_id == session.id) {
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
    body.push_str("<h2>Accepted work</h2><pre>");
    body.push_str(&escape(&input.instruction));
    body.push_str("</pre><h3>Accepted task limits</h3><pre>");
    body.push_str(&escape(&serde_json::to_string_pretty(&input.limits).unwrap_or_default()));
    body.push_str("</pre>");
    if let Some(snapshot) = &input.resolved_limits {
        body.push_str("<dl>");
        term(&mut body, "Accepted metadata revision", &snapshot.global_revision);
        term(&mut body, "Accepted agent limit revision", &snapshot.agent_revision);
        body.push_str("</dl>");
    }
    if let Some(schema) = &input.output_schema {
        body.push_str("<h3>Required result schema</h3><pre>");
        body.push_str(&escape(&serde_json::to_string_pretty(schema).unwrap_or_default()));
        body.push_str("</pre>");
    }
    if current {
    let _ = write!(body, "<p><a href=\"{}/settings?task_id={}\">Compare selected settings with this session's accepted settings</a></p>", agents::path(&session.agent_id), encode(&task.id));
    }
    Ok(page(200, &format!("Session {}", session.id), body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // Traceability scope: Renderer discriminator with distinct old session and current task; persistence and current authority are controller-owned.
    //= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
    //= type=test
    //# A task reassignment MUST retain the task's prior session and execution-input history.
    fn historical_session_keeps_accepted_input_without_successor_attempt_or_settings_link() {
        let metadata = RuntimeLimits::seed().unwrap();
        let mut input = TaskInput {
            instruction_source: None, pull_request: None, resolved_limits: None, repository_read: None,
            instruction: "Original <triage> work".into(),
            definition: Definition { charter: None, id: "triage".into(), revision: "r1".into(),
                executor: ExecutorKind::Native, inference: Some(InferenceSettings::selected()),
                execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }), limit_overrides: None },
            repository: None, acceptance_commands: vec![], output_schema: Some(serde_json::json!({"type":"object"})),
            limits: metadata.task_defaults.at(0).unwrap(), dependencies: vec![], native_workflow: None,
        };
        input.resolved_limits = Some(metadata.resolve_task(&input).unwrap());
        let session = Session { id: "session-old".into(), agent_id: input.definition.id.clone(),
            definition: input.definition.clone(), task_id: "same-task".into() };
        let mut current_input = input.clone(); current_input.definition.id = "coding".into();
        current_input.definition.executor = ExecutorKind::Coding;
        let task: Task = serde_json::from_value(serde_json::json!({
            "id":"same-task", "session_id":"session-new", "input":current_input, "assigned_agent_id":"coding",
            "status":"active", "revision":3, "attempt":{
                "id":"successor-attempt", "session_id":"session-new", "task_id":"same-task", "generation":2,
                "worker_id":"worker", "worker_incarnation":"incarnation", "phase":"running",
                "deadline_unix_ms":input.limits.deadline_unix_ms, "stopped":false, "uncertain":false
            }, "selected_checkpoint":null, "result":null, "blocked_reason":null, "native_retry_block":null,
            "progress":"", "recent_output":"", "output_truncated":false,
            "parent_task_id":null, "waiting_child_id":null, "completed_child":null
        })).unwrap();
        let page = render(&session, &input, false, &task).unwrap();
        assert_eq!(page.status, 200);
        assert!(page.body.contains("Historical") && page.body.contains("Original &lt;triage&gt; work"));
        assert!(page.body.contains("Accepted metadata revision"));
        assert!(!page.body.contains("successor-attempt"));
        assert!(!page.body.contains("settings?task_id="));
        assert!(render(&session, &input, true, &task).is_err(), "a historical session cannot claim current authority");
    }
}
