//! Task coordination is a browser adapter over canonical work operations.
//! The parent dispatcher owns same-origin checks, form bounds and receipts.
use super::*;
use crate::collaboration::{work::*, workflow::*};

fn parts(path: &str) -> Option<(&str, &str)> {
    let (id, tail) = path.strip_prefix("/tasks/")?.split_once('/')?;
    let action = tail.strip_prefix("coordination")?;
    (action.is_empty() || action.starts_with('/')).then_some((id, action))
}

pub(super) fn route(path: &str) -> bool { parts(path).is_some() }

fn record_id(raw: &str, limits: &RuntimeLimits) -> Result<String, String> {
    let id = decode(raw, false)?;
    checked_id(&id, limits)?;
    Ok(id)
}

fn checked_id(id: &str, limits: &RuntimeLimits) -> Result<(), String> {
    if !crate::mcp::valid_identity(id, limits.transport.identity_bytes) {
        return Err("Invalid record identity".into());
    }
    Ok(())
}

fn work_path(id: &str) -> String { format!("{}/coordination", task_path(id)) }

fn principal(owner: &PrincipalRef) -> String {
    match owner {
        PrincipalRef::Agent { agent_id } => format!("Agent {agent_id}"),
        PrincipalRef::TrustedHostOperator => "Trusted host operator".into(),
    }
}

fn resolver_parts(resolver: &ResolverRef) -> (&'static str, &str, String) {
    match resolver {
        ResolverRef::Task { task_id } => ("Task", task_id, task_path(task_id)),
        ResolverRef::Question { question_id } => ("Question", question_id, format!("/questions/{}", encode(question_id))),
        ResolverRef::Review { review_id } => ("Review", review_id, format!("/reviews/{}", encode(review_id))),
    }
}

fn raw_json(value: &impl serde::Serialize) -> String {
    escape(&serde_json::to_string_pretty(value).expect("work records serialize"))
}

async fn query(controller: &Controller, query: WorkQuery) -> Result<WorkQueryResult, UiResponse> {
    let reply = call(controller, Operation::QueryWork { query }).await;
    match reply.outcome {
        Outcome::Ok { data: Data::WorkQuery(value) } => Ok(value),
        _ => Err(not_task(reply)),
    }
}

fn mutation_fields(body: &mut String, revision: u64) {
    body.push_str(&hidden("operation_id", &identity()));
    body.push_str(&hidden("expected_revision", &revision.to_string()));
}

fn render_waits(body: &mut String, task_id: &str, waits: &crate::collaboration::Page<WorkflowWait>, revision: Option<u64>) {
    if waits.items.is_empty() { body.push_str("<p>No waits on this page.</p>"); }
    for wait in &waits.items {
        let (kind, id, target) = resolver_parts(&wait.resolver);
        let _ = write!(body, "<section aria-label=\"Resolver wait\"><h3>Wait {}</h3><p>{kind}: <a href=\"{}\">{}</a></p><dl>", escape(&wait.reference.wait_id), escape(&target), escape(id));
        term(body, "Recorded by", &principal(&wait.created_by.principal));
        term(body, "Created at product revision", &wait.product_revision.to_string());
        match &wait.status {
            WaitStatus::Open => term(body, "Wait status", "Open"),
            WaitStatus::Resolved { resolution, .. } => {
                term(body, "Wait status", "Resolved");
                body.push_str("</dl><p>The recorded resolver outcome ended this wait.</p><pre>");
                body.push_str(&raw_json(&resolution.outcome));
                body.push_str("</pre><dl>");
            },
            WaitStatus::Removed { reason, .. } => {
                term(body, "Wait status", "Removed");
                term(body, "Removal reason", reason);
            },
        }
        body.push_str("</dl><details><summary>Exact wait record and provenance</summary><pre>");
        body.push_str(&raw_json(wait));
        body.push_str("</pre></details>");
        if let (Some(revision), WaitStatus::Open) = (revision, &wait.status) {
            let _ = write!(body, "<form method=\"post\" action=\"{}/waits/{}/remove\"><fieldset><legend>Remove this wait</legend>", work_path(task_id), encode(&wait.reference.wait_id));
            mutation_fields(body, revision);
            body.push_str(&field("reason", "Reason", "", "text", true));
            body.push_str("<button type=\"submit\">Remove wait</button></fieldset></form>");
        }
        body.push_str("</section>");
    }
    if let Some(after) = &waits.next_cursor {
        let _ = write!(body, "<p><a href=\"{}/waits?after={}\">Next waits page</a></p>", work_path(task_id), encode(after));
    }
}

fn render_view(view: &WorkView, actions: bool) -> String {
    let task = &view.task;
    let path = work_path(&task.id);
    let mut body = format!("<p><a href=\"{}\">Task execution and accepted input</a> · <a href=\"{path}/waits\">All resolver waits</a></p><dl>", task_path(&task.id));
    term(&mut body, "Task", &task.id);
    term(&mut body, "Coordination revision", &view.coordination_revision.to_string());
    term(&mut body, if actions { "Current responsible owner" } else { "Responsible owner at commit" }, &principal(&view.responsible_owner));
    term(&mut body, "Accepted execution agent", &task.assigned_agent_id);
    term(&mut body, "Open resolver waits", &view.open_waits.to_string());
    let prior = match &view.state {
        WorkState::Runnable { state } => { term(&mut body, "Workflow state", "Open"); Some(state) },
        WorkState::Waiting { prior } => { term(&mut body, "Workflow state", "Blocked by resolver waits"); Some(prior) },
        WorkState::Terminal { status: terminal } => { term(&mut body, "Workflow state", &format!("Terminal: {}", status(*terminal))); None },
    };
    if let Some(prior) = prior {
        term(&mut body, "Logical execution status retained by coordination", status(prior.status));
        term(&mut body, "Retained execution assignee", &prior.assigned_agent_id);
    }
    term(&mut body, "Execution status", status(task.status));
    term(&mut body, "Task revision", &task.revision.to_string());
    if let Some(reason) = &task.blocked_reason { term(&mut body, "Execution blocker", reason); }
    if let Some(attempt) = &task.attempt {
        term(&mut body, "Attempt", &attempt.id);
        term(&mut body, "Attempt phase", phase(attempt.phase));
        term(&mut body, "Execution stopped", if attempt.stopped { "yes" } else { "not confirmed" });
        if attempt.uncertain { term(&mut body, "Reconciliation", "Required: execution effects remain uncertain"); }
    }
    body.push_str("</dl>");
    if view.coordination_blocked {
        body.push_str("<p class=\"notice\">Open resolver waits block this task's workflow. Each wait ends with its resolver's recorded outcome, including a negative outcome.</p>");
    } else if matches!(task.status, TaskStatus::Queued | TaskStatus::Active) {
        body.push_str("<p>No coordination waits remain. Execution still depends on ordinary admission, available capacity, accepted prerequisites, deadlines, budgets and any required stop reconciliation.</p>");
    } else {
        body.push_str("<p>The task is terminal. Resolving or removing a wait does not restart it.</p>");
    }
    if !task.input.dependencies.is_empty() {
        body.push_str("<h2>Accepted execution prerequisites</h2><ul>");
        for dependency in &task.input.dependencies {
            let _ = write!(body, "<li><a href=\"{}\">{}</a></li>", task_path(dependency), escape(dependency));
        }
        body.push_str("</ul><p>These prerequisites belong to the accepted task input.</p>");
    }
    body.push_str("<h2>Resolver waits</h2>");
    render_waits(&mut body, &task.id, &view.waits, actions.then_some(view.coordination_revision));
    if actions {
        let _ = write!(body, "<h2>Change responsibility</h2><p>Responsibility changes leave the accepted execution agent, session and input intact.</p><form method=\"post\" action=\"{path}/owner\"><fieldset><legend>Agent responsibility</legend>");
        mutation_fields(&mut body, view.coordination_revision);
        let agent = match &view.responsible_owner { PrincipalRef::Agent { agent_id } => Some(agent_id.as_str()), _ => None };
        body.push_str(&hidden("owner_kind", "agent"));
        body.push_str(&field("owner_agent_id", "Owner agent ID", agent.unwrap_or(""), "text", true));
        body.push_str("<button type=\"submit\">Assign agent responsibility</button></fieldset></form>");
        let _ = write!(body, "<form method=\"post\" action=\"{path}/owner\"><fieldset><legend>Operator responsibility</legend>");
        mutation_fields(&mut body, view.coordination_revision);
        body.push_str(&hidden("owner_kind", "operator"));
        body.push_str("<button type=\"submit\">Assign trusted host operator</button></fieldset></form>");
        if !matches!(view.state, WorkState::Terminal { .. }) {
            let _ = write!(body, "<h2>Add a resolver wait</h2><form method=\"post\" action=\"{path}/waits\"><fieldset><legend>Concrete resolver</legend>");
            mutation_fields(&mut body, view.coordination_revision);
            body.push_str("<label>Resolver kind<select name=\"resolver_kind\"><option value=\"task\">Task</option><option value=\"question\">Question</option><option value=\"review\">Review (including document review)</option></select></label>");
            body.push_str(&field("resolver_id", "Resolver ID", "", "text", true));
            body.push_str("<button type=\"submit\">Add wait</button></fieldset></form>");
        }
    }
    body
}

pub(super) async fn get(controller: &Controller, path: &str, raw_query: &str) -> UiResponse {
    match get_inner(controller, path, raw_query).await { Ok(response) | Err(response) => response }
}

async fn get_inner(controller: &Controller, path: &str, raw_query: &str) -> Result<UiResponse, UiResponse> {
    let (raw_id, action) = parts(path).ok_or_else(|| error(404, "Unknown task coordination route"))?;
    let task_id = record_id(raw_id, controller.limits()).map_err(|e| error(400, &e))?;
    let fields = parse_form(raw_query.as_bytes(), controller.limits()).map_err(|e| error(400, &e))?;
    match action {
        "" => {
            fields_only(&fields, &[]).map_err(|e| error(400, &e))?;
            let WorkQueryResult::Task(view) = query(controller, WorkQuery::Get { task_id }).await? else {
                return Err(error(502, "Unexpected task coordination projection"));
            };
            Ok(page(200, "Task coordination", render_view(&view, true)))
        },
        "/waits" => {
            fields_only(&fields, &["after"]).map_err(|e| error(400, &e))?;
            let policy = controller.limits().workflow.as_ref().ok_or_else(|| error(409, "Collaboration workflows are disabled"))?;
            // The current CAS snapshot is read before the separately paged history.
            // A concurrent mutation makes a removal stale; it never rebases it.
            let WorkQueryResult::Task(view) = query(controller, WorkQuery::Get { task_id: task_id.clone() }).await? else {
                return Err(error(502, "Unexpected task coordination projection"));
            };
            let WorkQueryResult::Waits(waits) = query(controller, WorkQuery::Waits {
                task_id: task_id.clone(), after: fields.get("after").filter(|v| !v.is_empty()).cloned(),
                limit: policy.page_items.min(controller.limits().product.page_items),
            }).await? else { return Err(error(502, "Unexpected wait history projection")); };
            let mut body = format!("<p><a href=\"{}\">Task coordination</a></p><p>Wait history. Removal forms retain coordination revision {}; a later change requires refreshing the form.</p>", work_path(&task_id), view.coordination_revision);
            render_waits(&mut body, &task_id, &waits, Some(view.coordination_revision));
            Ok(page(200, "Task resolver waits", body))
        },
        _ => Err(error(405, "Coordination mutations require a same-origin POST form")),
    }
}

fn parse_mutation(path: &str, fields: &Form, limits: &RuntimeLimits) -> Result<WorkMutation, String> {
    let (raw_id, action) = parts(path).ok_or("Unknown task coordination action")?;
    let task_id = record_id(raw_id, limits)?;
    let revision = required(fields, "expected_revision")?;
    if !revision.bytes().all(|b| b.is_ascii_digit()) { return Err("Invalid expected_revision".into()); }
    let expected_revision = number(fields, "expected_revision")?;
    match action {
        "/owner" => {
            fields_only(fields, &["operation_id", "expected_revision", "owner_kind", "owner_agent_id"])?;
            let owner = match required(fields, "owner_kind")? {
                "operator" => {
                    if fields.get("owner_agent_id").is_some_and(|id| !id.is_empty()) { return Err("Trusted host operator has no agent ID".into()); }
                    PrincipalRef::TrustedHostOperator
                },
                "agent" => {
                    let agent_id = required(fields, "owner_agent_id")?.to_owned();
                    checked_id(&agent_id, limits)?;
                    PrincipalRef::Agent { agent_id }
                },
                _ => return Err("Unknown responsible owner kind".into()),
            };
            Ok(WorkMutation::ChangeOwner { task_id, expected_revision, owner })
        },
        "/waits" => {
            fields_only(fields, &["operation_id", "expected_revision", "resolver_kind", "resolver_id"])?;
            let id = required(fields, "resolver_id")?.to_owned();
            checked_id(&id, limits)?;
            let resolver = match required(fields, "resolver_kind")? {
                "task" => ResolverRef::Task { task_id: id },
                "question" => ResolverRef::Question { question_id: id },
                "review" => ResolverRef::Review { review_id: id },
                _ => return Err("A wait requires a task, question or review resolver".into()),
            };
            Ok(WorkMutation::AddWait { task_id, expected_revision, resolver })
        },
        action => {
            let raw_wait = action.strip_prefix("/waits/").and_then(|v| v.strip_suffix("/remove"))
                .filter(|v| !v.is_empty() && !v.contains('/')).ok_or("Unknown task coordination action")?;
            fields_only(fields, &["operation_id", "expected_revision", "reason"])?;
            Ok(WorkMutation::RemoveWait { task_id, expected_revision, wait_id: record_id(raw_wait, limits)?, reason: required(fields, "reason")?.into() })
        },
    }
}

pub(super) async fn post(controller: &Controller, path: &str, fields: Form, operation_id: &str) -> UiResponse {
    let mutation = match parse_mutation(path, &fields, controller.limits()) {
        Ok(mutation) => mutation,
        Err(e) => return error(400, &e),
    };
    let fallback = work_path(mutation_task(&mutation));
    let reply = call(controller, Operation::MutateWork { operation_id: operation_id.into(), mutation }).await;
    mutation_reply_for(reply, operation_id, None, &fallback)
}

fn mutation_task(mutation: &WorkMutation) -> &str {
    match mutation {
        WorkMutation::AddWait { task_id, .. } | WorkMutation::RemoveWait { task_id, .. } | WorkMutation::ChangeOwner { task_id, .. } => task_id,
    }
}

pub(super) fn committed(operation_id: &str, outcome: &WorkMutationOutcome) -> UiResponse {
    let public_outcome = crate::pull_request_view::work(outcome);
    let outcome = &public_outcome;
    let mut body = format!("<p>Operation <code>{}</code> committed coordination revision {} at product revision {}. This receipt retains the original outcome.</p><p><a href=\"{}\">Read current coordination</a> · <a href=\"/operations/{}\">Look up the original operation</a></p>", escape(operation_id), outcome.view.coordination_revision, outcome.product_revision, work_path(&outcome.view.task.id), encode(operation_id));
    body.push_str(&render_view(&outcome.view, false));
    let _ = write!(body, "<details><summary>Exact committed coordination outcome</summary><pre id=\"coordination-receipt\">{}</pre></details>", raw_json(outcome));
    let mut response = page(200, "Committed task coordination", body);
    // A historical receipt never becomes the current view through live refresh.
    response.body = response.body.replacen("<main data-live-refresh=\"true\">", "<main data-live-refresh=\"false\">", 1);
    response
}

pub(super) fn receipt(reply: &Reply, operation_id: &str) -> Option<UiResponse> {
    let Outcome::Ok { data: Data::Operation(record) } = &reply.outcome else { return None; };
    if record.kind != "application_mutation" { return None; }
    let Operation::MutateWork { operation_id: original_id, mutation } = serde_json::from_value(record.intended.clone()).ok()? else { return None; };
    if reply.validate_for(&Operation::GetOperation { operation_id: operation_id.into() }).is_err()
        || record.id != operation_id || original_id != operation_id
    {
        return Some(uncertain(503, operation_id, "The stored coordination operation identity differs; preserve the original request for reconciliation.", None));
    }
    let outcome = record.outcome.as_ref().and_then(|value| serde_json::from_value::<Outcome>(value.clone()).ok());
    Some(match outcome {
        Some(outcome) => mutation_reply_for(Reply { request_id: reply.request_id.clone(), commit: reply.commit.clone(), observed_controller_revision: reply.observed_controller_revision, outcome }, operation_id, None, &work_path(mutation_task(&mutation))),
        None => uncertain(503, operation_id, "The stored coordination outcome is unavailable; preserve the original request for reconciliation.", None),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observed() -> WorkView {
        let task = Task {
            id: "task-observed".into(), session_id: "session-original".into(),
            input: TaskInput { instruction_source: None,
                pull_request: None,
                resolved_limits: None, repository_read: None, instruction: "Original instruction".into(),
                definition: Definition { charter: None, id: "agent-original".into(), revision: "r1".into(), executor: ExecutorKind::Native, inference: None, execution_profile: None, limit_overrides: None },
                repository: None, acceptance_commands: vec![], output_schema: None,
                limits: Limits { deadline_unix_ms: 9999, model_requests: 1, output_bytes: 100, result_bytes: 100, max_delegation_depth: 0 },
                dependencies: vec!["accepted-prerequisite".into()], native_workflow: None,
            },
            assigned_agent_id: "agent-original".into(), status: TaskStatus::Cancelled, revision: 19,
            attempt: Some(Attempt { id: "attempt-original".into(), task_id: "task-observed".into(), session_id: "session-original".into(), generation: 1, worker_id: "worker".into(), worker_incarnation: "incarnation".into(), phase: AttemptPhase::StopRequested, deadline_unix_ms: 9999, stopped: false, uncertain: true }),
            selected_checkpoint: None, result: None, blocked_reason: Some("Uncertain <effect>".into()), pending_assignment: None, native_retry_block: None, progress: String::new(), recent_output: String::new(), output_truncated: false,
            parent_task_id: None, waiting_child_id: None, completed_child: None,
        };
        WorkView { task, coordination_revision: 4, responsible_owner: PrincipalRef::TrustedHostOperator, state: WorkState::Terminal { status: TaskStatus::Cancelled }, open_waits: 0, coordination_blocked: false, waits: crate::collaboration::Page { items: vec![], next_cursor: None } }
    }

    #[test]
    fn coordination_render_keeps_cancelled_uncertain_execution_and_original_agent_visible() {
        let view = observed();
        let html = render_view(&view, true);
        for value in ["Terminal: cancelled", "stop requested", "not confirmed", "effects remain uncertain", "Uncertain &lt;effect&gt;", "agent-original", "accepted-prerequisite", "does not restart it"] {
            assert!(html.contains(value), "missing {value}: {html}");
        }
        assert!(!html.contains("Add a resolver wait"));
        assert!(!html.contains("/retry"));
        assert!(!html.contains("ready to run"));
        let outcome = WorkMutationOutcome { view, changed_wait: None, product_revision: 7 };
        let receipt = committed("original-operation", &outcome);
        assert!(receipt.body.contains("Responsible owner at commit"));
        assert!(!receipt.body.contains("Current responsible owner"));
        assert!(!receipt.body.contains("method=\"post\""));
        assert!(receipt.body.contains("data-live-refresh=\"false\""));
        assert!(receipt.location.is_none());
    }

    #[test]
    fn coordination_forms_preserve_cas_and_refuse_principals_as_resolvers_or_actor_inputs() {
        let limits = RuntimeLimits::seed().unwrap();
        let mut fields = Form::from([("operation_id".into(), "original".into()), ("expected_revision".into(), "0".into()), ("resolver_kind".into(), "task".into()), ("resolver_id".into(), "resolver/id".into())]);
        for (kind, expected) in [
            ("task", ResolverRef::Task { task_id: "resolver/id".into() }),
            ("question", ResolverRef::Question { question_id: "resolver/id".into() }),
            ("review", ResolverRef::Review { review_id: "resolver/id".into() }),
        ] {
            fields.insert("resolver_kind".into(), kind.into());
            assert_eq!(parse_mutation("/tasks/task%2F1/coordination/waits", &fields, &limits).unwrap(), WorkMutation::AddWait { task_id: "task/1".into(), expected_revision: 0, resolver: expected });
        }
        for invalid in ["agent", "team", "external", "document", ""] {
            fields.insert("resolver_kind".into(), invalid.into());
            assert!(parse_mutation("/tasks/task/coordination/waits", &fields, &limits).is_err());
        }
        fields.insert("resolver_kind".into(), "task".into());
        fields.insert("actor".into(), "operator".into());
        assert!(parse_mutation("/tasks/task/coordination/waits", &fields, &limits).is_err());
        fields.remove("actor");
        for invalid in ["-1", "+1", "1.0", "18446744073709551616"] {
            fields.insert("expected_revision".into(), invalid.into());
            assert!(parse_mutation("/tasks/task/coordination/waits", &fields, &limits).is_err());
        }
    }

    #[test]
    fn coordination_wait_history_escapes_outcomes_and_preserves_exact_paging_cursor() {
        let actor = RecordedActor { principal: PrincipalRef::TrustedHostOperator, provenance: ActorProvenance::TrustedHost { path: TrustedHostPath::Browser } };
        let reference = WaitRef { task_id: "task".into(), wait_id: "wait/<one>".into() };
        let resolver = ResolverRef::Review { review_id: "review/<one>".into() };
        let waits = crate::collaboration::Page { items: vec![WorkflowWait {
            reference, resolver, created_by: actor.clone(), operation_id: "add-original".into(), product_revision: 1,
            status: WaitStatus::Removed { reason: "<script>removed</script>".into(), actor, operation_id: "remove-original".into(), product_revision: 2 },
        }], next_cursor: Some("cursor&+/<next>".into()) };
        let mut html = String::new();
        render_waits(&mut html, "task", &waits, Some(9));
        assert!(html.contains("/reviews/review%2F%3Cone%3E"));
        assert!(html.contains("&lt;script&gt;removed&lt;/script&gt;"));
        assert!(!html.contains("<script>removed"));
        assert!(html.contains("after=cursor%26%2B%2F%3Cnext%3E"));
        assert!(!html.contains("method=\"post\""));
    }
}
