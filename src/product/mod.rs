//! Stateless browser views over the controller's committed task operations.
//!
//! Authentication belongs to the private HTTP/host boundary. This module adds
//! same-origin mutation checks; it never treats a supplied agent ID as identity.
use crate::controller::Controller;
use crate::limits::RuntimeLimits;
use crate::protocol::*;
use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

mod governance;
#[cfg(test)]
mod governance_tests;
mod agents;
mod collaboration;
mod collaboration_questions;
mod collaboration_work;
mod documents_reviews;
mod definition_settings;
pub(crate) mod failure;
mod live;
mod machine_resources;
mod protected_view;
mod repository;
pub(crate) mod read_pages;
mod repository_catalogue;
mod sessions;
mod task_defaults;
mod default_task_submission;
#[cfg(test)]
mod instruction_source_tests;
#[cfg(test)]
mod pull_request_tests;

pub struct UiRequest {
    pub method: String,
    pub path: String,
    pub body: Vec<u8>,
    pub origin: Option<String>,
    pub expected_origin: String,
}

#[derive(Debug)]
pub struct UiResponse {
    pub status: u16,
    pub content_type: String,
    pub body: String,
    pub location: Option<String>,
    limiting_field: Option<failure::LimitingField>,
}

static SEQUENCE: AtomicU64 = AtomicU64::new(1);
type Form = BTreeMap<String, String>;

fn identity() -> String {
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!(
        "browser-{}-{time}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
}

// Content and attribute values always pass through this same escaping path.
fn escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

fn encode(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

fn decode(value: &str, plus_is_space: bool) -> Result<String, String> {
    let mut out = Vec::with_capacity(value.len());
    let mut input = value.as_bytes().iter().copied();
    while let Some(byte) = input.next() {
        match byte {
            b'%' => {
                let a = input.next().and_then(|b| char::from(b).to_digit(16));
                let b = input.next().and_then(|b| char::from(b).to_digit(16));
                match (a, b) {
                    (Some(a), Some(b)) => out.push((a * 16 + b) as u8),
                    _ => return Err("Malformed percent encoding".into()),
                }
            }
            b'+' if plus_is_space => out.push(b' '),
            _ => out.push(byte),
        }
    }
    let out = String::from_utf8(out).map_err(|_| "Form text is not UTF-8".to_string())?;
    if out
        .chars()
        .any(|c| c == '\0' || (c.is_control() && c != '\n' && c != '\r' && c != '\t'))
    {
        return Err("Control characters are not accepted".into());
    }
    Ok(out)
}

trait Diagnostic {
    fn message(&self) -> &str;
    fn limiting_field(&self) -> Option<failure::LimitingField> { None }
}
impl Diagnostic for str { fn message(&self) -> &str { self } }
impl Diagnostic for String { fn message(&self) -> &str { self } }
impl<T: Diagnostic + ?Sized> Diagnostic for &T {
    fn message(&self) -> &str { T::message(*self) }
    fn limiting_field(&self) -> Option<failure::LimitingField> { T::limiting_field(*self) }
}
#[derive(Debug)]
struct FormError {
    message: String,
    limiting_field: Option<failure::LimitingField>,
}
impl FormError {
    fn limit(field: failure::LimitingField) -> Self {
        Self { message: format!("Configured limit exceeded: {}", field.name()), limiting_field: Some(field) }
    }
}
impl From<String> for FormError {
    fn from(message: String) -> Self { Self { message, limiting_field: None } }
}
impl From<&str> for FormError {
    fn from(message: &str) -> Self { message.to_owned().into() }
}
impl std::ops::Deref for FormError {
    type Target = str;
    fn deref(&self) -> &str { &self.message }
}
impl PartialEq<&str> for FormError {
    fn eq(&self, other: &&str) -> bool { self.message == *other }
}
impl std::fmt::Display for FormError {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { out.write_str(&self.message) }
}
impl Diagnostic for FormError {
    fn message(&self) -> &str { &self.message }
    fn limiting_field(&self) -> Option<failure::LimitingField> { self.limiting_field }
}

// Traceability scope: Form body, field-count and field-name byte refusals identify
// their configured fields; malformed forms retain separate errors.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//# An operation refused by a configured limit MUST identify the limiting metadata field.
fn parse_form(body: &[u8], metadata: &RuntimeLimits) -> Result<Form, FormError> {
    if body.len() > metadata.transport.body_bytes {
        return Err(FormError::limit(failure::LimitingField::Body));
    }
    let body = std::str::from_utf8(body).map_err(|_| "Form body is not UTF-8".to_string())?;
    let mut fields = Form::new();
    if body.is_empty() {
        return Ok(fields);
    }
    for pair in body.split('&') {
        if fields.len() == metadata.product.form_fields {
            return Err(FormError::limit(failure::LimitingField::FormFields));
        }
        let (key, value) = pair.split_once('=').ok_or("Missing form value")?;
        let key = decode(key, true)?;
        if key.is_empty() {
            return Err("Invalid form field name".into());
        }
        if key.len() > metadata.product.form_key_bytes {
            return Err(FormError::limit(failure::LimitingField::FormKey));
        }
        if fields.insert(key, decode(value, true)?).is_some() {
            return Err("Duplicate form field".into());
        }
    }
    Ok(fields)
}

fn required<'a>(fields: &'a Form, name: &str) -> Result<&'a str, String> {
    fields
        .get(name)
        .map(String::as_str)
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| format!("Missing {name}"))
}

fn number<T: std::str::FromStr>(fields: &Form, name: &str) -> Result<T, String> {
    required(fields, name)?
        .parse()
        .map_err(|_| format!("Invalid {name}"))
}

fn fields_only(fields: &Form, allowed: &[&str]) -> Result<(), String> {
    if fields.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err("Unknown form field".into());
    }
    Ok(())
}

// Traceability scope: The shared page shell places the activity aside to the right on wide layouts, with a responsive narrow-screen fallback. live.js fills it from bounded durable activity; this does not assert full browser/API parity.
//= spec/impl/v2/implementation.md#shared-product-state
//# The browser interface MUST display a live activity feed on the right side of the page.
fn page(status: u16, title: &str, content: String) -> UiResponse {
    UiResponse {
        status,
        content_type: "text/html; charset=utf-8".into(),
        location: None,
        limiting_field: None,
        body: format!(
            "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>{} | Borg</title><style>body{{font-family:system-ui,sans-serif;max-width:1500px;margin:2rem auto;padding:0 1rem;color:#17202a;background:#fafafa}}nav a{{margin-right:1rem}}.workspace{{display:grid;grid-template-columns:minmax(0,1fr) 20rem;gap:2rem}}.activity{{border-left:1px solid #ccd1d1;padding-left:1rem;overflow-wrap:anywhere}}.activity ol{{padding-left:1.2rem}}.activity li{{margin-bottom:1rem}}.activity small{{display:block}}@media(max-width:850px){{.workspace{{display:block}}.activity{{border-left:0;border-top:1px solid #ccd1d1;padding:1rem 0}}}}table{{width:100%;border-collapse:collapse}}th,td{{text-align:left;padding:.6rem;border-bottom:1px solid #ccd1d1}}pre{{white-space:pre-wrap;overflow-wrap:anywhere;background:#eef0f2;padding:1rem}}label{{display:block;margin:1rem 0}}input,textarea,select{{display:block;box-sizing:border-box;max-width:100%;width:100%;padding:.5rem}}input[type=hidden]{{display:none}}button{{padding:.6rem 1rem}}fieldset{{margin:1rem 0;border:1px solid #bbc}}.notice{{padding:1rem;border-left:4px solid #b56b00;background:#fff3cd}}dt{{font-weight:bold;margin-top:.6rem}}dd{{margin-left:0;overflow-wrap:anywhere}}.actions form{{display:inline-block;margin-right:1rem}}.actions input{{width:auto}}a{{color:#075c93}}small{{color:#455}}</style></head><body><nav aria-label=\"Primary navigation\"><a href=\"/tasks\">Tasks</a><a href=\"/agents\">Agents</a><a href=\"/sessions\">Sessions</a><a href=\"/workers\">Workers</a><a href=\"/machines\">Machines</a><a href=\"/collaboration\">Collaboration</a><a href=\"/repositories\">Repositories</a><a href=\"/limits\">Limits</a><a href=\"/governance\">Governance</a><a href=\"/tasks/new\">Create task</a></nav><div class=\"workspace\"><main data-live-refresh=\"{}\"><h1>{}</h1>{content}</main><aside class=\"activity\" aria-label=\"Borg activity\"><h2>Borg activity</h2><p id=\"live-status\" role=\"status\">Connecting</p><button id=\"live-refresh\" type=\"button\" hidden>Update this view</button><p>Latest recorded task events</p><p id=\"activity-empty\">No activity received.</p><ol id=\"activity-items\"></ol><p><a href=\"/activity\">Browse activity history</a></p><section id=\"workflow-activity\" hidden><h3>Workflow activity</h3><p>Latest recorded questions, documents, reviews and waits</p><p id=\"workflow-empty\">No workflow activity received.</p><ol id=\"workflow-items\"></ol></section></aside></div><script src=\"/ui/live.js\" defer></script></body></html>",
            escape(title),
            status < 400
                && !matches!(
                    title,
                    "Create task"
                        | "Register repository"
                        | "Revise repository"
                        | "Repository registration"
                        | "Committed settings change"
                        | "Operation outcome unresolved"
                ),
            escape(title)
        ),
    }
}

fn error<M: Diagnostic + ?Sized>(status: u16, message: &M) -> UiResponse {
    let mut response = page(
        status,
        "Request not completed",
        format!(
            "<p class=\"notice\" role=\"alert\" tabindex=\"-1\" autofocus>{}</p><p><a href=\"/tasks\">Return to committed task state</a></p>",
            escape(message.message())
        ),
    );
    response.limiting_field = message.limiting_field();
    response
}

fn limit_error(status: u16, field: failure::LimitingField) -> UiResponse {
    error(status, &FormError::limit(field))
}

fn field(name: &str, label: &str, value: &str, kind: &str, required: bool) -> String {
    format!(
        "<label>{}<input name=\"{}\" type=\"{}\" value=\"{}\" {}></label>",
        escape(label),
        escape(name),
        kind,
        escape(value),
        if required { "required" } else { "" }
    )
}

fn hidden(name: &str, value: &str) -> String {
    format!(
        "<input type=\"hidden\" name=\"{}\" value=\"{}\">",
        escape(name),
        escape(value)
    )
}

fn task_path(id: &str) -> String {
    format!("/tasks/{}", encode(id))
}

fn status(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Queued => "queued",
        TaskStatus::Active => "active",
        TaskStatus::Blocked => "blocked",
        TaskStatus::Succeeded => "succeeded",
        TaskStatus::Failed => "failed",
        TaskStatus::Cancelled => "cancelled",
    }
}

fn phase(phase: AttemptPhase) -> &'static str {
    match phase {
        AttemptPhase::Assigned => "assigned",
        AttemptPhase::Running => "running",
        AttemptPhase::StopRequested => "stop requested",
        AttemptPhase::Stopped => "stopped",
    }
}

//= spec/impl/v2/implementation.md#shared-product-state
//# The collaboration product MUST use the platform's task state and event interfaces.
//= spec/impl/v2/implementation.md#shared-product-state
//# The collaboration product MUST NOT create an independent agent inbox.
async fn call(controller: &Controller, command: Operation) -> Reply {
    controller
        .handle_browser(Request {
            protocol_revision: WIRE_REVISION,
            request_id: identity(),
            command,
        })
        .await
}

fn not_task(reply: Reply) -> UiResponse {
    match reply.outcome {
        Outcome::Refused { code, message } => error(
            if code == "not_found" { 404 } else { 409 },
            &format!("{code}: {message}"),
        ),
        Outcome::Pending { operation_id } => uncertain(
            202,
            &operation_id,
            "The controller reports this operation as pending.",
            None,
        ),
        Outcome::Uncertain {
            operation_id,
            message,
        } => uncertain(202, &operation_id, &message, None),
        Outcome::Ok { .. } => error(
            502,
            "Controller returned an unexpected response; no successful mutation is inferred.",
        ),
    }
}

fn uncertain(code: u16, operation_id: &str, message: &str, task_id: Option<&str>) -> UiResponse {
    let link = task_id
        .map(|id| {
            format!(
                "<p><a href=\"{}\">Refresh committed task state</a></p>",
                task_path(id)
            )
        })
        .unwrap_or_default();
    page(
        code,
        "Operation outcome unresolved",
        format!(
            "<p class=\"notice\" role=\"alert\" tabindex=\"-1\" autofocus>{}</p><p>Operation identity: <code>{}</code>. Keep this identity for lookup or retry; do not create a replacement operation to guess the outcome.</p><a href=\"/operations/{}\">Look up this operation</a>{link}",
            escape(message),
            escape(operation_id),
            encode(operation_id)
        ),
    )
}

fn mutation_reply(reply: Reply, operation_id: &str, task_id: Option<&str>) -> UiResponse {
    mutation_reply_for(reply, operation_id, task_id, "/tasks")
}

fn mutation_reply_for(
    reply: Reply,
    operation_id: &str,
    task_id: Option<&str>,
    fallback: &str,
) -> UiResponse {
    if matches!(&reply.outcome, Outcome::Pending { operation_id: returned } | Outcome::Uncertain { operation_id: returned, .. } if returned != operation_id)
    {
        return uncertain(
            503,
            operation_id,
            "Controller returned a different operation identity. Preserve the original request for reconciliation.",
            task_id,
        );
    }
    let committed = reply.commit.as_ref().is_some_and(|commit| {
        commit.operation_id == operation_id
            && commit.controller_revision > 0
            && reply
                .observed_controller_revision
                .is_some_and(|revision| revision >= commit.controller_revision)
    });
    if !committed
        && matches!(&reply.outcome, Outcome::Ok { .. } | Outcome::Refused { .. })
        && !matches!(&reply.outcome, Outcome::Refused { code, .. } if code == "operation_conflict")
    {
        return uncertain(
            503,
            operation_id,
            "No committed outcome was returned. Reconcile this operation before deciding whether to retry.",
            task_id,
        );
    }
    match reply.outcome {
        Outcome::Ok { data: Data::DefinitionUpdate(outcome) } => {
            definition_settings::committed(operation_id, &outcome)
        }
        Outcome::Ok {
            data: Data::RepositoryRegistration(entry),
        } => {
            let path = repository_catalogue::entry_path(
                &entry.repository_id,
                Some(&entry.registration_revision),
            );
            let mut response = page(
                303,
                "Committed repository state",
                format!(
                    "<p>Operation <code>{}</code> returned recorded repository metadata.</p><p><a href=\"{}\">View registration</a></p>",
                    escape(operation_id),
                    escape(&path)
                ),
            );
            response.location = Some(path);
            response
        }
        Outcome::Ok { data: Data::QuestionMutation(value) } => collaboration_questions::committed(operation_id, &value),
        Outcome::Ok { data: Data::DocumentMutation(value) } => documents_reviews::document_committed(operation_id, &value),
        Outcome::Ok { data: Data::ReviewMutation(value) } => documents_reviews::review_committed(operation_id, &value),
        Outcome::Ok { data: Data::WorkMutation(value) } => collaboration_work::committed(operation_id, &value),
        Outcome::Ok {
            data: Data::Task(task),
        } => {
            let mut response = page(
                303,
                "Committed task state",
                format!(
                    "<p>Operation <code>{}</code> returned the controller's recorded task state.</p><p><a href=\"{}\">View task</a></p>",
                    escape(operation_id),
                    task_path(&task.id)
                ),
            );
            response.location = Some(task_path(&task.id));
            response
        }
        Outcome::Refused { code, message } if code == "operation_conflict" => page(
            409,
            "Action refused: operation identity conflict",
            format!(
                "<p class=\"notice\" role=\"alert\" tabindex=\"-1\" autofocus>{}: {}</p><p>This request differs from the original request recorded under operation <code>{}</code>. The original operation may already have committed. Inspect its original outcome before creating another action.</p><a href=\"/operations/{}\">Look up the original operation</a>",
                escape(&code), escape(&message), escape(operation_id), encode(operation_id)
            ),
        ),
        Outcome::Refused { code, message } => page(
            409,
            "Action refused",
            format!(
                "<p class=\"notice\" role=\"alert\" tabindex=\"-1\" autofocus>{}: {}</p><p>Operation identity: <code>{}</code>. The action was not accepted. Refresh before creating a new action; do not automatically retry a stale form with a new revision.</p><a href=\"{}\">Refresh committed state</a>",
                escape(&code),
                escape(&message),
                escape(operation_id),
                task_id.map(task_path).unwrap_or_else(|| fallback.into())
            ),
        ),
        Outcome::Pending { operation_id } => uncertain(
            202,
            &operation_id,
            "The action is pending; acknowledgment is not completion.",
            task_id,
        ),
        Outcome::Uncertain {
            operation_id,
            message,
        } => uncertain(202, &operation_id, &message, task_id),
        _ => error(
            502,
            "Controller returned an unexpected mutation response. Preserve the original operation identity and inspect committed state.",
        ),
    }
}

//= spec/impl/v2/implementation.md#shared-product-state
//# The browser interface MUST support task submission.
fn create_defaults(executor: ExecutorKind, metadata: &RuntimeLimits) -> Result<Form, String> {
    let now = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "Clock precedes epoch")?
            .as_millis(),
    )
    .map_err(|_| "Clock exceeds task representation")?;
    let selected = metadata.task_defaults.at(now)?;
    let mut defaults = Form::new();
    // The submission identity is retained across redisplay/replay; the controller
    // creates exactly one session for a newly committed task.
    defaults.insert("operation_id".into(), identity());
    defaults.insert("model".into(), SELECTED_MODEL.into());
    defaults.insert("reasoning_effort".into(), SELECTED_REASONING_EFFORT.into());
    defaults.insert("execution_profile".into(), "collaboration_v1".into());
    defaults.insert(
        "executor".into(),
        if executor == ExecutorKind::Native {
            "native"
        } else {
            "coding"
        }
        .into(),
    );
    defaults.insert(
        "deadline_unix_ms".into(),
        selected.deadline_unix_ms.to_string(),
    );
    defaults.insert("model_requests".into(), selected.model_requests.to_string());
    defaults.insert("output_bytes".into(), selected.output_bytes.to_string());
    defaults.insert("result_bytes".into(), selected.result_bytes.to_string());
    defaults.insert(
        "max_delegation_depth".into(),
        selected.max_delegation_depth.to_string(),
    );
    defaults.insert(
        "acceptance_commands".into(),
        if executor == ExecutorKind::Native {
            "[]"
        } else {
            "[[\"cargo\",\"test\",\"--locked\"]]"
        }
        .into(),
    );
    Ok(defaults)
}
fn create_form(
    fields: Option<&Form>,
    problem: Option<&str>,
    executor: ExecutorKind,
    metadata: &RuntimeLimits,
) -> UiResponse {
    let defaults;
    let fields = match fields {
        Some(fields) => fields,
        None => {
            defaults = match create_defaults(executor, metadata) {
                Ok(f) => f,
                Err(e) => return error(400, &e),
            };
            &defaults
        }
    };
    let value = |key: &str| fields.get(key).map(String::as_str).unwrap_or("");
    let mut body = String::new();
    if let Some(problem) = problem {
        let _ = write!(body, "<p class=\"notice\">{}</p>", escape(problem));
    }
    body.push_str("<p><a href=\"/tasks/new?executor=coding\">New coding task</a> | <a href=\"/tasks/new?executor=native\">New native task</a></p><p>Submission records work; it does not mean execution or acceptance succeeded. All workspaces can build and test. Existing worker access applies.</p><form method=\"post\" action=\"/tasks\">");
    body.push_str(&hidden("operation_id", value("operation_id")));
    if let Some(limit) = fields.get("web_search_max_tool_calls") {
        body.push_str(&hidden("web_search_max_tool_calls", limit));
        if !limit.is_empty() {
            let _ = write!(
                body,
                "<p>Selected agent revision enables native web search with at most {} built-in tool calls per response.</p>",
                escape(limit)
            );
        }
    }
    if let Some(overrides) = fields.get("limit_overrides") {
        body.push_str(&hidden("limit_overrides", overrides));
    }
    if let Some(charter) = fields.get("charter").filter(|value| !value.is_empty()) {
        body.push_str(&hidden("charter",charter));
        body.push_str("<p>The selected revision retains its exact reusable charter.</p>");
    }
    if fields.contains_key("charter_definition_id") || fields.contains_key("charter_definition_revision")
        || (metadata.charter.is_some() && fields.get("charter").is_none_or(|value|value.is_empty())) {
        body.push_str("<fieldset><legend>Reusable charter</legend><p>Use an already published immutable source. Task instructions describe this task's work.</p>");
        body.push_str(&field("charter_definition_id","Charter agent ID",value("charter_definition_id"),"text",true));
        body.push_str(&field("charter_definition_revision","Charter source revision",value("charter_definition_revision"),"text",true));
        body.push_str("</fieldset>");
    }
    // Invalid input is redisplayed exactly; never fill in missing settings.
    if fields.contains_key("execution_profile") {
        body.push_str(&hidden("execution_profile", value("execution_profile")));
    }
    // Preserve any explicitly submitted allowance on validation redisplay.
    if fields.contains_key("model_requests") {
        body.push_str(&hidden("model_requests", value("model_requests")));
    }
    body.push_str("<label>Instruction<textarea name=\"instruction\" rows=\"7\" required>");
    body.push_str(&escape(value("instruction")));
    body.push_str("</textarea></label>");
    body.push_str("<p>The agent ID identifies the durable agent. Each new task creates a fresh session pinned to the chosen agent-definition revision. Sessions of the same agent can run concurrently and share that agent's durable memory. Their task results, attempts and working context remain separate.</p>");
    for (name, label) in [
        ("definition_id", "Agent ID"),
        ("definition_revision", "Immutable agent revision"),
    ] {
        body.push_str(&field(name, label, value(name), "text", true));
    }
    body.push_str("<fieldset><legend>Agent inference</legend><p>The model and reasoning effort belong to this immutable agent revision. New definitions use collaboration tools; native web search is disabled unless separately configured.</p>");
    for (name, label, selected) in [
        ("model", "Model", SELECTED_MODEL),
        (
            "reasoning_effort",
            "Reasoning effort",
            SELECTED_REASONING_EFFORT,
        ),
    ] {
        let actual = value(name);
        let _ = write!(body, "<label>{}<select name=\"{}\" required>", label, name);
        if actual != selected {
            let _ = write!(
                body,
                "<option value=\"{}\" selected>{}</option>",
                escape(actual),
                if actual.is_empty() {
                    "Select an agent setting".to_string()
                } else {
                    escape(actual)
                }
            );
        }
        let _ = write!(
            body,
            "<option value=\"{}\" {}>{}</option></select></label>",
            selected,
            if actual == selected { "selected" } else { "" },
            selected
        );
    }
    body.push_str("</fieldset>");
    let _ = write!(
        body,
        "<label>Executor<select name=\"executor\"><option value=\"coding\" {}>Coding workspace</option><option value=\"native\" {}>Native model and collaboration tools</option></select></label>",
        if value("executor") == "coding" {
            "selected"
        } else {
            ""
        },
        if value("executor") == "native" {
            "selected"
        } else {
            ""
        }
    );
    body.push_str("<fieldset><legend>Coding task repository</legend><p>Required for coding tasks; leave these fields empty for native tasks. Choose an explicit ID and immutable revision from the <a href=\"/repositories\">repository catalogue</a>. No repository is selected automatically.</p>");
    for (name, label) in [
        ("repository_id", "Registered repository ID"),
        ("registration_revision", "Immutable registration revision"),
        ("start_commit", "Exact starting commit"),
    ] {
        body.push_str(&field(name, label, value(name), "text", false));
    }
    body.push_str("</fieldset><fieldset><legend>Optional instruction source</legend><p>For a coding task, select an already available read-only repository by its registered ID, immutable revision and exact commit. Leave all three fields empty to keep the task's existing sources. This does not create a checkout or grant repository access.</p>");
    for (name, label) in [
        ("instruction_source_id", "Registered instruction repository ID"),
        ("instruction_source_revision", "Immutable instruction registration revision"),
        ("instruction_source_commit", "Exact instruction source commit"),
    ] {
        body.push_str(&field(name, label, value(name), "text", false));
    }
    body.push_str("</fieldset><fieldset><legend>Optional pull request</legend><p>For a coding task, supply a title to request a pull request after its acceptance checks pass. The selected repository revision must have pull request publication configured. Leave both fields empty for a checkpoint result.</p>");
    body.push_str(&field("pull_request_title", "Pull request title", value("pull_request_title"), "text", false));
    body.push_str("<label>Pull request body<textarea name=\"pull_request_body\" rows=\"4\">");
    body.push_str(&escape(value("pull_request_body")));
    body.push_str("</textarea></label></fieldset><fieldset><legend>Optional native repository read scope</legend><p>For native analysis, provide the registered repository ID, immutable registration revision and exact starting commit together. This grants read access at that revision, not editing or a checkout from another task. Coding tasks use their own repository above; leave this separate read scope empty.</p>");
    for (name, label) in [
        (
            "repository_read_id",
            "Registered repository ID for native reads",
        ),
        (
            "repository_read_revision",
            "Immutable registration revision for native reads",
        ),
        ("repository_read_commit", "Exact native read commit"),
    ] {
        body.push_str(&field(name, label, value(name), "text", false));
    }
    body.push_str("</fieldset><p>Native tasks leave acceptance commands empty; coding-task checks for an optional native workflow belong inside that workflow's configuration.</p><label>Acceptance commands (JSON array of argument arrays; no implicit shell)<textarea name=\"acceptance_commands\" rows=\"3\">");
    body.push_str(&escape(value("acceptance_commands")));
    body.push_str("</textarea></label>");
    body.push_str("<label>Native output JSON schema (required only for native tasks)<textarea name=\"output_schema\" rows=\"3\">");
    body.push_str(&escape(value("output_schema")));
    body.push_str("</textarea></label>");
    body.push_str("<fieldset><legend>Optional native workflow</legend><p>Leave empty to use the normal collaboration tools and return a native result. An explicit reassign target supplies a coding agent, repository and checks for this same task. Its complete limits and dependencies must match this task, and instruction_prefix must be empty. The original instruction and result schema remain; no child or return continuation is created. Optional coding_then_continue supplies a fixed coding child with its own agent model, effort and collaboration profile. The child repository JSON must explicitly supply registration.repository_id, registration.registration_revision and start_commit; omit server-owned url, checkpoint_remote and pull_request_target. Optional child.pull_request supplies an explicit title and body. The model can provide only the instruction, not the configured repository, publication request, agent, checks or limits. Omit the child model_requests field to use its largest applicable allowance.</p><label>Native workflow JSON (native tasks only)<textarea name=\"native_workflow\" rows=\"8\">");
    body.push_str(&escape(value("native_workflow")));
    body.push_str("</textarea></label></fieldset>");
    body.push_str(&task_defaults::reassignment_help(fields));
    body.push_str("<p>A configured coding child may also supply instruction_source with registration.repository_id, registration.registration_revision and start_commit. Omit its server-owned repository endpoint. This fixes the child's separate read source without changing the parent's read scope.</p>");
    body.push_str(&field(
        "dependencies",
        "Existing prerequisite task IDs (one per line or comma separated)",
        value("dependencies"),
        "text",
        false,
    ));
    body.push_str("<fieldset><legend>Finite execution bounds</legend><p>New tasks use the selected metadata model-request allowance. Deadlines, output limits and provider limits still apply.</p>");
    for (name, label) in [
        ("deadline_unix_ms", "Absolute deadline (Unix milliseconds)"),
        ("output_bytes", "Maximum output bytes"),
        ("result_bytes", "Maximum complete accepted result bytes"),
        ("max_delegation_depth", "Maximum delegation depth"),
    ] {
        body.push_str(&field(name, label, value(name), "number", true));
    }
    body.push_str("</fieldset><button type=\"submit\">Submit task</button></form>");
    page(
        if problem.is_some() { 400 } else { 200 },
        "Create task",
        body,
    )
}

// Defaults only a new operator draft. Legacy explicit values still construct
// their original request, so old receipts and immutable tasks replay exactly.
//= spec/impl/v2/implementation.md#initial-model-budget-policy
//# The default task model-request allowance MUST be the maximum finite value supported by the task representation.
fn model_request_allowance(fields: &Form, metadata: &RuntimeLimits) -> Result<u32, String> {
    if fields.contains_key("model_requests") {
        number(fields, "model_requests")
    } else {
        Ok(metadata.task_defaults.model_requests)
    }
}

fn task_input(fields: &Form, metadata: &RuntimeLimits) -> Result<TaskInput, String> {
    fields_only(
        fields,
        &[
            "operation_id",
            "instruction",
            "definition_id",
            "definition_revision",
            "web_search_max_tool_calls",
            "model",
            "reasoning_effort",
            "execution_profile",
            "limit_overrides",
            "charter",
            "charter_definition_id",
            "charter_definition_revision",
            "repository_read_id",
            "repository_read_revision",
            "repository_read_commit",
            "instruction_source_id",
            "instruction_source_revision",
            "instruction_source_commit",
            "executor",
            "repository_id",
            "registration_revision",
            "start_commit",
            "acceptance_commands",
            "output_schema",
            "native_workflow",
            "pull_request_title",
            "pull_request_body",
            "dependencies",
            "deadline_unix_ms",
            "model_requests",
            "output_bytes",
            "result_bytes",
            "max_delegation_depth",
        ],
    )?;
    required(fields, "operation_id")?;
    let executor = match required(fields, "executor")? {
        "coding" => ExecutorKind::Coding,
        "native" => ExecutorKind::Native,
        _ => return Err("Unknown executor".into()),
    };
    if required(fields, "model")? != SELECTED_MODEL
        || required(fields, "reasoning_effort")? != SELECTED_REASONING_EFFORT
        || !matches!(required(fields, "execution_profile")?, "collaboration_v1" | "collaboration_v2")
    {
        return Err("Unsupported agent model, reasoning effort or collaboration profile".into());
    }
    let web_search = match fields
        .get("web_search_max_tool_calls")
        .filter(|v| !v.is_empty())
    {
        Some(value) => {
            let max_tool_calls = value
                .parse::<u32>()
                .map_err(|_| "Invalid native web-search bound")?;
            if executor != ExecutorKind::Native || max_tool_calls == 0 {
                return Err("Native web search requires a positive bound on a native agent".into());
            }
            Some(NativeSearchProfile { max_tool_calls })
        }
        None => None,
    };
    let inference = Some(InferenceSettings::selected());
    let execution_profile = Some(match required(fields, "execution_profile")? {
        "collaboration_v1" => ExecutionProfile::CollaborationV1 { web_search },
        "collaboration_v2" => ExecutionProfile::CollaborationV2 { publication: None, web_search },
        _ => unreachable!("validated closed profile"),
    });
    // Preserve the original submitted body on replay. Current-policy admission
    // remains after canonical receipt lookup in the controller.
    let charter = match fields.get("charter").filter(|value| !value.is_empty()) {
        Some(value) => {
            if ["charter_definition_id","charter_definition_revision"].iter().any(|key| fields.get(*key).is_some_and(|v| !v.is_empty())) {
                return Err("Choose one exact charter source".into());
            }
            Some(serde_json::from_str::<crate::charter::DefinitionCharter>(value).map_err(|_| "Invalid charter source".to_string())?)
        }
        None if ["charter_definition_id","charter_definition_revision"].iter().any(|key| fields.get(*key).is_some_and(|v| !v.is_empty())) =>
            Some(crate::charter::DefinitionCharter::Reference {source:crate::charter::CharterSourceRef {
                definition_id:required(fields,"charter_definition_id")?.into(),
                definition_revision:required(fields,"charter_definition_revision")?.into(),
            }}),
        None => None,
    };
    let read_fields = [
        "repository_read_id",
        "repository_read_revision",
        "repository_read_commit",
    ];
    let repository_read = if read_fields
        .iter()
        .all(|key| fields.get(*key).is_none_or(|v| v.is_empty()))
    {
        None
    } else {
        if executor != ExecutorKind::Native {
            return Err(
                "Coding tasks use their own repository; separate native read scope is not accepted"
                    .into(),
            );
        }
        let registration = repository_catalogue::binding(
            fields,
            "repository_read_id",
            "repository_read_revision",
            metadata,
        )?;
        let start_commit = required(fields, "repository_read_commit")?;
        if !matches!(start_commit.len(), 40 | 64)
            || !start_commit
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("Native repository reads require an exact lowercase commit".into());
        }
        Some(RepositoryReadSource {
            registration: Some(registration),
            repository: String::new(),
            start_commit: start_commit.into(),
        })
    };
    let instruction_source = if ["instruction_source_id", "instruction_source_revision", "instruction_source_commit"]
        .iter().all(|key| fields.get(*key).is_none_or(|value| value.is_empty()))
    {
        None
    } else {
        Some(RepositoryReadSource {
            registration: Some(repository_catalogue::binding(fields, "instruction_source_id", "instruction_source_revision", metadata)?),
            repository: String::new(),
            start_commit: required(fields, "instruction_source_commit")?.into(),
        })
    };
    let repository = if executor == ExecutorKind::Coding {
        Some(Repository {
            pull_request_target: None,
            registration: Some(repository_catalogue::binding(
                fields,
                "repository_id",
                "registration_revision",
                metadata,
            )?),
            url: String::new(),
            start_commit: required(fields, "start_commit")?.into(),
            checkpoint_remote: String::new(),
        })
    } else {
        if ["repository_id", "registration_revision", "start_commit"]
            .iter()
            .any(|k| fields.get(*k).is_some_and(|v| !v.is_empty()))
        {
            return Err(
                "Native tasks do not use coding repository fields; use the native read scope"
                    .into(),
            );
        }
        None
    };
    let acceptance_commands = match fields
        .get("acceptance_commands")
        .filter(|v| !v.trim().is_empty())
    {
        Some(value) => serde_json::from_str::<Vec<Vec<String>>>(value).map_err(|_| {
            "Acceptance commands must be a JSON array of argument arrays".to_string()
        })?,
        None => vec![],
    };
    let output_schema = fields
        .get("output_schema")
        .filter(|v| !v.trim().is_empty())
        .map(|v| serde_json::from_str(v).map_err(|_| "Invalid output schema JSON".to_string()))
        .transpose()?;
    if executor == ExecutorKind::Native && output_schema.is_none() {
        return Err("Native tasks require an output schema".into());
    }
    let model_requests = model_request_allowance(fields, metadata)?;
    let native_workflow = fields
        .get("native_workflow")
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            task_defaults::native_workflow(
                value,
                model_requests,
                execution_profile.is_none(),
                metadata,
            )
        })
        .transpose()?
        .flatten();
    if executor != ExecutorKind::Native && native_workflow.is_some() {
        return Err("A native workflow can only configure a native task".into());
    }
    let dependencies = fields
        .get("dependencies")
        .map(|v| {
            v.split([',', '\n', '\r'])
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let limits = Limits {
        deadline_unix_ms: number(fields, "deadline_unix_ms")?,
        model_requests,
        output_bytes: number(fields, "output_bytes")?,
        result_bytes: if fields
            .get("result_bytes")
            .is_none_or(|value| value.trim().is_empty())
        {
            metadata.task_defaults.result_bytes
        } else {
            number(fields, "result_bytes")?
        },
        max_delegation_depth: number(fields, "max_delegation_depth")?,
    };
    // Policy admission follows exact receipt lookup in the controller. The UI
    // reconstructs explicitly submitted values without revalidating them against
    // a newer policy, so retries can recover their original recorded outcome.
    let limit_overrides = fields
        .get("limit_overrides")
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            serde_json::from_str(value).map_err(|_| "Invalid agent limits metadata".to_string())
        })
        .transpose()?
        .flatten();
    let title = fields.get("pull_request_title").map(String::as_str).unwrap_or("");
    let body = fields.get("pull_request_body").map(String::as_str).unwrap_or("");
    let pull_request = if title.is_empty() && body.is_empty() {
        None
    } else {
        if executor != ExecutorKind::Coding { return Err("Request the coding child's pull request in its configured workflow; native tasks do not publish a pull request directly".into()); }
        if title.trim().is_empty() { return Err("A pull request title is required when a body is supplied".into()); }
        Some(crate::pull_request::Request { title: title.into(), body: body.into() })
    };
    let input = TaskInput {
        pull_request,
        resolved_limits: None,
        instruction: required(fields, "instruction")?.into(),
        definition: Definition {
            charter,
            id: required(fields, "definition_id")?.into(),
            revision: required(fields, "definition_revision")?.into(),
            executor,
            inference,
            execution_profile,
            limit_overrides,
        },
        repository_read,
        instruction_source,
        repository,
        acceptance_commands,
        output_schema,
        limits,
        dependencies,
        native_workflow,
    };
    if let Some(source) = &input.instruction_source {
        task_defaults::validate_instruction_source(source, &input.definition, metadata)?;
    }
    Ok(input)
}

fn term(body: &mut String, label: &str, value: &str) {
    let _ = write!(body, "<dt>{}</dt><dd>{}</dd>", escape(label), escape(value));
}

//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//# The browser interface MUST expose the effective limits applicable to displayed work.
fn task_limits_view(task: &Task) -> String {
    let mut body = String::from("<h2>Accepted task limits</h2><dl>");
    for (label, value) in [
        (
            "Deadline (Unix milliseconds)",
            task.input.limits.deadline_unix_ms.to_string(),
        ),
        (
            "Model requests",
            task.input.limits.model_requests.to_string(),
        ),
        ("Output bytes", task.input.limits.output_bytes.to_string()),
        (
            "Complete result bytes",
            task.input.limits.result_bytes.to_string(),
        ),
        (
            "Maximum delegation depth",
            task.input.limits.max_delegation_depth.to_string(),
        ),
    ] {
        term(&mut body, label, &value);
    }
    if let Some(snapshot) = &task.input.resolved_limits {
        term(
            &mut body,
            "Accepted global metadata revision",
            &snapshot.global_revision,
        );
        term(
            &mut body,
            "Accepted agent revision",
            &snapshot.agent_revision,
        );
    }
    body.push_str("</dl>");
    body
}

fn checkpoint_view(checkpoint: &Checkpoint) -> String {
    let mut body = "<dl>".to_string();
    term(
        &mut body,
        "Repository ID",
        &checkpoint.registration.repository_id,
    );
    term(
        &mut body,
        "Registration revision",
        &checkpoint.registration.registration_revision,
    );
    term(&mut body, "Repository", &checkpoint.repository);
    term(&mut body, "Exact commit", &checkpoint.commit);
    term(&mut body, "Published reference", &checkpoint.remote_ref);
    term(&mut body, "Publication operation", &checkpoint.operation_id);
    body.push_str(
        "</dl><p>This is an acknowledged source checkpoint, not proof that the task passed.</p>",
    );
    if let Some(url) = safe_external_link(&checkpoint.repository) {
        let _ = write!(
            body,
            "<p><a href=\"{}\" rel=\"noreferrer noopener\">Open repository</a></p>",
            escape(&url)
        );
    } else {
        body.push_str("<p>The repository reference is displayed as text because it is not a safe browser HTTP(S) link. Use the configured Git client to fetch the exact commit.</p>");
    }
    body
}

fn safe_external_link(value: &str) -> Option<String> {
    let url = reqwest::Url::parse(value).ok()?;
    if !["http", "https"].contains(&url.scheme())
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return None;
    }
    Some(url.to_string())
}

fn pull_request_result_view(value: &crate::pull_request::ResultReference) -> String {
    let mut body = String::from("<section><h2>Published pull request</h2><dl>");
    term(&mut body, "Pull request number", &value.number.to_string());
    term(&mut body, "Provider identity", &value.provider_id.to_string());
    term(&mut body, "Target revision", &value.target_revision);
    term(&mut body, "Original publication operation", &value.operation_id);
    term(&mut body, "Original branch operation", &value.head_operation_id);
    term(&mut body, "Original checkpoint operation", &value.checkpoint_operation_id);
    term(&mut body, "Head branch", &format!("{}/{}:{}", value.head.owner, value.head.repository, value.head.branch));
    term(&mut body, "Head commit", &value.head.commit);
    term(&mut body, "Base branch", &format!("{}/{}:{}", value.base.owner, value.base.repository, value.base.branch));
    term(&mut body, "Observed base commit", &value.base.commit);
    body.push_str("</dl>");
    if let Some(url) = safe_external_link(&value.url) {
        let _ = write!(body, "<p><a href=\"{}\" rel=\"noreferrer noopener\">Open pull request</a></p>", escape(&url));
    } else {
        body.push_str("<dl>");
        term(&mut body, "Pull request reference", &value.url);
        body.push_str("</dl>");
    }
    body.push_str("</section>");
    body
}

fn child_outcome_view(child: &ChildOutcome) -> String {
    let mut body = format!(
        "<section><h2>Recorded child outcome</h2><p><a href=\"{}\">{}</a></p><dl>",
        task_path(&child.task_id),
        escape(&child.task_id)
    );
    term(&mut body, "Child status", status(child.status));
    body.push_str("</dl><pre>");
    body.push_str(&escape(&child.summary));
    body.push_str("</pre>");
    if child.status == TaskStatus::Succeeded && child.result.is_some() {
        let _ = write!(
            body,
            "<p><a href=\"{}/result\">Accepted child result</a></p>",
            task_path(&child.task_id)
        );
    } else {
        body.push_str("<p>No accepted successful child result is reported.</p>");
    }
    body.push_str("<p>A child outcome does not by itself complete this task.</p></section>");
    body
}

fn release_path(id: &str) -> String {
    format!("/releases/{}", encode(id))
}

fn protected_status(value: &ProtectedStatus) -> &'static str {
    match value {
        ProtectedStatus::Unconfigured => "Not configured",
    }
}

fn artifact_qualification(value: &ArtifactQualification) -> &'static str {
    match value {
        ArtifactQualification::Claimed => {
            "Identity claimed; independent verification is not established"
        }
    }
}

#[cfg(test)]
fn release_detail(candidate: &ReleaseCandidate) -> UiResponse {
    render_release(candidate, None)
}

fn render_release(
    candidate: &ReleaseCandidate,
    protected: Option<&crate::protected_release::StageView>,
) -> UiResponse {
    let mut body = format!(
        "<p><a href=\"{}\">Task</a> | <a href=\"{}/releases\">All candidates for this task</a></p><nav><a href=\"#source\">Source</a><a href=\"#artifact\">Artifact identity</a><a href=\"#checks\">Checks and rollout status</a></nav><p class=\"notice\">A recorded candidate is not protected release validation, human rollout authorization or a deployment result.</p><dl>",
        task_path(&candidate.task_id),
        task_path(&candidate.task_id)
    );
    term(&mut body, "Preparation", "Source-linked candidate recorded");
    term(
        &mut body,
        "Candidate revision",
        &candidate.revision.to_string(),
    );
    term(
        &mut body,
        "Task revision at preparation",
        &candidate.task_revision.to_string(),
    );
    term(
        &mut body,
        "Accepted task attempt",
        &candidate.accepted_attempt_id,
    );
    body.push_str("</dl><section id=\"source\"><h2>Source checkpoint</h2>");
    body.push_str(&checkpoint_view(&candidate.checkpoint));
    body.push_str("<dl>");
    term(
        &mut body,
        "Artifact source commit",
        &candidate.artifact.source_commit,
    );
    term(&mut body, "Source tree", &candidate.artifact.source_tree);
    term(
        &mut body,
        "Git object format",
        &candidate.artifact.object_format,
    );
    body.push_str("</dl></section><section id=\"artifact\"><h2>Artifact identity</h2><dl>");
    term(
        &mut body,
        "Artifact qualification",
        artifact_qualification(&candidate.qualification.artifact),
    );
    term(&mut body, "Package format", &candidate.artifact.format);
    term(
        &mut body,
        "Package SHA-256",
        &candidate.artifact.package_sha256,
    );
    term(
        &mut body,
        "Package bytes",
        &candidate.artifact.package_bytes.to_string(),
    );
    term(
        &mut body,
        "Manifest SHA-256",
        &candidate.artifact.manifest_sha256,
    );
    term(
        &mut body,
        "Executable SHA-256",
        &candidate.artifact.binary_sha256,
    );
    term(
        &mut body,
        "Executable bytes",
        &candidate.artifact.binary_bytes.to_string(),
    );
    term(&mut body, "Build target", &candidate.artifact.target);
    body.push_str("</dl><p>These are recorded artifact identities. No artifact download location is recorded here. The build target identifies a platform, not a deployment destination.</p></section><section id=\"checks\"><h2>Checks and rollout status</h2><dl>");
    term(
        &mut body,
        "Repository submission",
        protected_status(&candidate.qualification.repository_submission),
    );
    term(
        &mut body,
        "Protected checks",
        protected_status(&candidate.qualification.checks),
    );
    term(
        &mut body,
        "Human rollout authorization",
        protected_status(&candidate.qualification.human_rollout),
    );
    term(&mut body, "Deployment target", "Not recorded");
    term(&mut body, "Deployment outcome", "Not recorded");
    body.push_str("</dl><p>These preparation-time fields remain the candidate claim. Worker check results and package identities do not supply protected checker evidence. Current protected-stage records are displayed separately below when available.</p></section>");
    let _ = write!(
        body,
        "<p><a href=\"/releases/{}/checks\">Browse recorded release checks</a></p>",
        encode(&candidate.id)
    );
    if let Some(view) = protected {
        body.push_str(&protected_view::render(view));
    }
    page(200, &format!("Release candidate {}", candidate.id), body)
}

fn release_list(task: &Task, candidates: &[ReleaseCandidate]) -> UiResponse {
    if candidates
        .iter()
        .any(|candidate| candidate.task_id != task.id)
    {
        return error(
            502,
            "Controller returned a release candidate for another task",
        );
    }
    let mut body = format!(
        "<p><a href=\"{}\">Task</a> | <a href=\"{}/releases\">Refresh candidates</a></p><p>Task acceptance and candidate preparation do not establish protected checks, rollout authorization or deployment.</p>",
        task_path(&task.id),
        task_path(&task.id)
    );
    if candidates.is_empty() {
        body.push_str("<p>No release candidates on this page.</p>");
    } else {
        body.push_str("<table><thead><tr><th>Candidate</th><th>Source commit</th><th>Package SHA-256</th><th>Protected checks</th><th>Human rollout</th></tr></thead><tbody>");
        for candidate in candidates {
            let _ = write!(
                body,
                "<tr><td><a href=\"{}\">{}</a></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                release_path(&candidate.id),
                escape(&candidate.id),
                escape(&candidate.artifact.source_commit),
                escape(&candidate.artifact.package_sha256),
                protected_status(&candidate.qualification.checks),
                protected_status(&candidate.qualification.human_rollout)
            );
        }
        body.push_str("</tbody></table>");
        if let Some(last) = candidates.last() {
            let _ = write!(
                body,
                "<p><a href=\"{}/releases?after={}\">Next candidate page</a></p>",
                task_path(&task.id),
                encode(&last.id)
            );
        }
    }
    page(200, &format!("Release candidates for {}", task.id), body)
}

//= spec/impl/v2/implementation.md#shared-product-state
//# The browser interface MUST support task assignment.
//= spec/impl/v2/implementation.md#shared-product-state
//# The browser interface MUST show task dependencies.
//= spec/impl/v2/implementation.md#shared-product-state
//# The browser interface MUST show actual task status.
//= spec/impl/v2/implementation.md#shared-product-state
//# The browser interface MUST show reported task progress.
//= spec/impl/v2/implementation.md#shared-product-state
//# The browser interface MUST show available recent task output.
//= spec/impl/v2/implementation.md#shared-product-state
//# The browser interface MUST expose task cancellation.
//= spec/impl/v2/implementation.md#shared-product-state
//# The browser interface MUST support retrying a failed task.
//= spec/impl/v2/implementation.md#shared-product-state
//# The browser interface MUST present accepted task results.
//= spec/impl/v2/implementation.md#shared-product-state
//# The browser interface MUST link the task's published source checkpoints.
// Traceability scope: Controlled event producers with real controller; no full live browser/model journey.
//= spec/impl/v2/implementation.md#observable-end-to-end-result
//# The initial system MUST expose the task's actual execution status through its product interface.
fn render_task(task: &Task, workflow_enabled: bool) -> UiResponse {
    let public_task = crate::pull_request_view::task(task);
    let task = &public_task;
    let path = task_path(&task.id);
    let mut body = format!(
        "<p><a href=\"{path}\">Refresh committed state</a> | <a href=\"/tasks\">All tasks</a></p>"
    );
    let _ = write!(body, "<p><a href=\"/governance?task_id={}\">Governance decisions and operator controls</a></p>", encode(&task.id));
    if workflow_enabled {
        //= spec/impl/v2/implementation.md#collaboration-surfaces-preserve-existing-authority
        //# The browser interface MUST expose every supported collaboration operation permitted to its caller.
        let _ = write!(body, "<nav aria-label=\"Task collaboration\"><a href=\"{path}/coordination\">Responsibility and resolver waits</a><a href=\"{path}/comments\">Comments</a>");
        if task.status == TaskStatus::Succeeded && task.result.is_some() {
            let _ = write!(body, "<a href=\"{path}/review-target?scope=result\">Request accepted-result review</a>");
            if task.result.as_ref().is_some_and(|result| result.checkpoint.is_some()) {
                let _ = write!(body, "<a href=\"{path}/review-target?scope=checkpoint\">Request accepted-checkpoint review</a>");
            }
        }
        body.push_str("</nav>");
    }
    body.push_str("<dl>");
    let _ = write!(
        body,
        "<dt>Session</dt><dd><a href=\"/sessions/{}\">{}</a></dd>",
        encode(&task.session_id),
        escape(&task.session_id)
    );
    term(&mut body, "Status", status(task.status));
    term(&mut body, "Committed revision", &task.revision.to_string());
    let _ = write!(
        body,
        "<dt>Assigned agent</dt><dd><a href=\"{}\">{}</a></dd>",
        agents::path(&task.assigned_agent_id),
        escape(&task.assigned_agent_id)
    );
    term(
        &mut body,
        "Definition",
        &format!(
            "{} @ {}",
            task.input.definition.id, task.input.definition.revision
        ),
    );
    term(
        &mut body,
        "Executor",
        if task.input.definition.executor == ExecutorKind::Coding {
            "coding"
        } else {
            "native"
        },
    );
    if repository::instruction_source_enabled(&task.input) {
        if let Some(source) = &task.input.instruction_source {
            if let Some(registration) = &source.registration {
                term(&mut body, "Instruction repository ID", &registration.repository_id);
                term(&mut body, "Instruction registration revision", &registration.registration_revision);
            }
            term(&mut body, "Instruction source commit", &source.start_commit);
            let _ = write!(body, "<dt>Instruction source</dt><dd><a href=\"{path}/repository?source=instructions\">Read accepted instructions</a></dd>");
        }
    }
    if let Some(reason) = &task.blocked_reason {
        term(&mut body, "Blocked reason", reason);
    }
    if let Some(attempt) = &task.attempt {
        term(&mut body, "Attempt", &attempt.id);
        term(&mut body, "Execution phase", phase(attempt.phase));
        term(
            &mut body,
            "Worker / incarnation",
            &format!("{} / {}", attempt.worker_id, attempt.worker_incarnation),
        );
        term(&mut body, "Generation", &attempt.generation.to_string());
        term(
            &mut body,
            "Termination confirmed",
            if attempt.stopped { "yes" } else { "no" },
        );
        if attempt.uncertain {
            term(
                &mut body,
                "Unresolved outcome",
                "Execution or external effects remain uncertain; retry is not safe until reconciled.",
            );
        }
    }
    body.push_str("</dl>");
    body.push_str(&task_limits_view(task));
    let _ = write!(
        body,
        "<p><a href=\"/limits?task_id={}\">Task execution metadata and current global defaults</a></p>",
        encode(&task.id)
    );
    body.push_str("<h2>Instruction</h2><pre>");
    body.push_str(&escape(&task.input.instruction));
    body.push_str("</pre>");
    if let Some(request) = &task.input.pull_request {
        body.push_str("<section><h2>Requested pull request</h2><p>");
        body.push_str(&escape(&request.title));
        body.push_str("</p><pre>");
        body.push_str(&escape(&request.body));
        body.push_str("</pre></section>");
    }
    body.push_str("<h2>Dependencies</h2>");
    if task.input.dependencies.is_empty() {
        body.push_str("<p>No prerequisite tasks.</p>");
    } else {
        body.push_str("<ul>");
        for id in &task.input.dependencies {
            let _ = write!(
                body,
                "<li><a href=\"{}\">{}</a></li>",
                task_path(id),
                escape(id)
            );
        }
        body.push_str("</ul>");
    }
    for (label, id) in [
        ("Parent task", &task.parent_task_id),
        ("Waiting for child", &task.waiting_child_id),
    ] {
        if let Some(id) = id {
            let _ = write!(
                body,
                "<p>{label}: <a href=\"{}\">{}</a></p>",
                task_path(id),
                escape(id)
            );
        }
    }
    if let Some(child) = &task.completed_child {
        body.push_str(&child_outcome_view(child));
    }
    if let Some(workflow) = &task.input.native_workflow {
        body.push_str("<details><summary>Configured native workflow</summary><pre>");
        body.push_str(&escape(
            &serde_json::to_string_pretty(workflow).unwrap_or_default(),
        ));
        body.push_str("</pre></details>");
    } else if task.input.definition.executor == ExecutorKind::Native {
        body.push_str("<p>Native workflow: result only; no coding handoff configured.</p>");
    }
    body.push_str("<h2>Reported progress</h2><pre>");
    body.push_str(&escape(if task.progress.is_empty() {
        "No progress reported."
    } else {
        &task.progress
    }));
    body.push_str("</pre><h2>Available recent output</h2>");
    if task.output_truncated {
        body.push_str(
            "<p class=\"notice\">Output is truncated. This is not a complete transcript.</p>",
        );
    }
    body.push_str("<pre>");
    body.push_str(&escape(if task.recent_output.is_empty() {
        "No output available."
    } else {
        &task.recent_output
    }));
    body.push_str("</pre>");
    if task.selected_checkpoint.is_some() {
        let _ = write!(
            body,
            "<p><a href=\"{path}/checkpoint\">Published source checkpoint</a></p>"
        );
    }
    if let Some(result) = &task.result {
        let _ = write!(
            body,
            "<h2>Accepted result</h2><pre>{}</pre><a href=\"{path}/result\">Result, validation and source references</a>",
            escape(&result.summary)
        );
        if let Some(reference) = &result.pull_request { body.push_str(&pull_request_result_view(reference)); }
    } else {
        body.push_str("<p>No accepted task result. Executor progress or exit status alone is not task acceptance.</p>");
    }
    let _ = write!(
        body,
        "<h2>Release preparation</h2><p><a href=\"{path}/releases\">View source-linked release candidates</a></p><p>Task acceptance is not protected release validation or human rollout authorization.</p>"
    );
    let _ = write!(
        body,
        "<h2>Collaboration records</h2><p><a href=\"{path}/history\">History</a> · <a href=\"{path}/attempts\">Attempts and usage</a> · <a href=\"{path}/checkpoints\">All checkpoint publications</a> · <a href=\"{path}/repository\">Repository</a> · <a href=\"{path}/calls\">Tool call outcomes</a> · <a href=\"/tasks?parent_task_id={}\">Child tasks</a> · <a href=\"/definitions/{}?revision={}\">Exact agent revision</a></p>",
        encode(&task.id),
        encode(&task.input.definition.id),
        encode(&task.input.definition.revision)
    );
    body.push_str("<details><summary>Accepted task configuration</summary><p>This is the configuration retained when this task was accepted, including its output schema. Later selected definitions and current defaults do not replace it.</p>");
    body.push_str("<pre>");
    body.push_str(&escape(&serde_json::to_string_pretty(&task.input).unwrap_or_default()));
    body.push_str("</pre></details>");
    body.push_str("<h2>Task actions</h2>");
    body.push_str("<p>Each session retains its accepted agent definition. Assigning a different registered agent creates a new session for this same task, preserving its work, deadline and prior sessions.</p>");
    body.push_str("<div class=\"actions\">");
    for (action, label, show) in [
        (
            "cancel",
            "Request cancellation",
            matches!(task.status, TaskStatus::Queued | TaskStatus::Active | TaskStatus::Blocked),
        ),
        (
            "retry",
            "Request retry",
            matches!(task.status, TaskStatus::Failed | TaskStatus::Cancelled | TaskStatus::Blocked),
        ),
    ] {
        if show {
            let _ = write!(
                body,
                "<form method=\"post\" action=\"{path}/{action}\">{}{}<button type=\"submit\">{label}</button></form>",
                hidden("operation_id", &identity()),
                hidden("expected_revision", &task.revision.to_string())
            );
        }
    }
    body.push_str("</div><p><small>Cancellation acknowledgment is not proof of stopped execution. The controller checks the displayed revision, current state and safe retry conditions.</small></p>");
    if let Some(pending) = &task.pending_assignment {
        body.push_str("<h3>Assignment pending</h3><p>The accepted assignment is waiting for prior execution and unresolved obligations to settle. The task retains its current session until the controller activates its successor.</p><dl>");
        term(&mut body, "Target agent", &pending.session.agent_id);
        term(&mut body, "Target revision", &pending.session.definition.revision);
        let _ = write!(body, "<dt>Original assignment</dt><dd><a href=\"/operations/{}\">{}</a></dd></dl>",
            encode(&pending.operation_id), escape(&pending.operation_id));
    } else if matches!(task.status, TaskStatus::Queued | TaskStatus::Active | TaskStatus::Blocked) {
        // This is the existing operator form route, not a local eligibility or
        // authority decision. Target compatibility, CAS and stop/effect closure
        // remain in the canonical AssignTask transaction and original receipt.
        body.push_str("<h3>Assign task</h3><p>Choose a registered agent from the <a href=\"/agents\">agent directory</a>. The controller checks the target and displayed task revision. An accepted assignment may wait for current execution to stop and unresolved effects to be reconciled.</p>");
        let _ = write!(body, "<form method=\"post\" action=\"{path}/assign\">{}{}{}<button type=\"submit\">Assign task</button></form>",
            hidden("operation_id", &identity()),
            hidden("expected_revision", &task.revision.to_string()),
            field("agent_id", "Target agent ID", "", "text", true));
    }
    page(200, &format!("Task {}", task.id), body)
}

// Keep construction of a route future out of its caller's polling frame.
// In debug builds, Box::pin(future()) at the call site still constructs the
// unboxed future there. Match the worker's concrete noninline constructor.
#[inline(never)]
fn boxed_route<F, Fut>(make: F) -> std::pin::Pin<Box<Fut>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future,
{
    Box::pin(make())
}

async fn get(controller: &Controller, path: &str, query: &str) -> UiResponse {
    if governance::route(path) { return governance::get(controller, path, query); }
    if let Some(response) = boxed_route(|| documents_reviews::get(controller, path, query)).await { return response; }
    if collaboration_questions::route(path) { return boxed_route(|| collaboration_questions::get(controller, path, query)).await; }
    boxed_route(|| get_other(controller, path, query)).await
}

// These routes retain their original order and body. Their larger state machine
// is constructed and polled only after the document/question probes decline.
async fn get_other(controller: &Controller, path: &str, query: &str) -> UiResponse {
    if path == "/ui/definition-settings.js" {
        return UiResponse { status: 200, content_type: "text/javascript; charset=utf-8".into(),
            body: include_str!("definition_settings.js").into(), location: None, limiting_field: None };
    }
    if let Some(response) = machine_resources::get(controller, path, query).await {
        return response;
    }
    if collaboration_work::route(path) { return boxed_route(|| collaboration_work::get(controller, path, query)).await; }
    if let Some(raw_id) = definition_settings::route(path) {
        return boxed_route(|| definition_settings::get(controller, raw_id, query)).await;
    }
    if path == "/repositories" || path.starts_with("/repositories/") {
        return boxed_route(|| repository_catalogue::get(controller, path, query)).await;
    }
// Traceability scope: Serve the embedded live.js consumer of /ui/changes and /ui/activity: it refreshes clean views, preserves unsent edits and catches up the activity cursor. Rust endpoint and separate Node script tests cover different parts; this is not a full-browser execution claim.
//= spec/impl/v2/implementation.md#shared-product-state
//# The browser interface MUST update displayed state without requiring a manual page reload.
    if path == "/ui/live.js" {
        return UiResponse {
            status: 200,
            content_type: "text/javascript; charset=utf-8".into(),
            body: include_str!("live.js").into(),
            location: None,
            limiting_field: None,
        };
    }
    if let Some(response) = boxed_route(|| repository::get(controller, path, query)).await {
        return response;
    }
    if let Some(response) = boxed_route(|| collaboration::get(controller, path, query)).await {
        return response;
    }
    if path == "/sessions" || path.starts_with("/sessions/") {
        return boxed_route(|| sessions::get(controller, path, query)).await;
    }
    if path == "/agents" || path.starts_with("/agents/") {
        return boxed_route(|| agents::get(controller, path, query)).await;
    }
    if path == "/tasks/new" {
        let fields = match parse_form(query.as_bytes(), controller.limits()) {
            Ok(fields) => fields,
            Err(e) => return error(400, &e),
        };
        if let Err(e) = fields_only(&fields, &["executor", "agent_id"]) {
            return error(400, &e);
        }
        if let Some(id) = fields.get("agent_id") {
            if fields.contains_key("executor") {
                return error(400, "An existing agent determines its executor");
            }
            let reply = boxed_route(|| call(
                controller,
                Operation::GetAgent {
                    agent_id: id.clone(),
                },
            ))
            .await;
            let Outcome::Ok {
                data: Data::Agent(agent),
            } = reply.outcome
            else {
                return not_task(reply);
            };
            if agent.retired { return error(409, "This agent is retired; historical sessions remain available"); }
            let definition = agent.definition;
            let Some(inference) = &definition.inference else {
                return error(409, "Agent definition has no selected inference settings");
            };
            let (profile_name, web_search) = match &definition.execution_profile {
                Some(ExecutionProfile::CollaborationV1 { web_search }) => ("collaboration_v1",web_search),
                Some(ExecutionProfile::CollaborationV2 { web_search , .. }) => ("collaboration_v2",web_search),
                None => return error(409,"Agent definition has no collaboration profile"),
            };
            let mut draft = match create_defaults(definition.executor, controller.limits()) {
                Ok(f) => f,
                Err(e) => return error(400, &e),
            };
            if let Some(overrides) = &definition.limit_overrides {
                draft.insert(
                    "limit_overrides".into(),
                    serde_json::to_string(overrides).expect("typed limits serialize"),
                );
            }
            draft.insert("execution_profile".into(), profile_name.into());
            if let Some(charter) = &definition.charter {
                draft.insert("charter".into(), serde_json::to_string(charter).expect("typed charter serializes"));
            }
            draft.insert("definition_id".into(), definition.id);
            draft.insert("definition_revision".into(), definition.revision);
            draft.insert("model".into(), inference.model.clone());
            draft.insert(
                "reasoning_effort".into(),
                inference.reasoning_effort.clone(),
            );
            if let Some(search) = web_search {
                draft.insert(
                    "web_search_max_tool_calls".into(),
                    search.max_tool_calls.to_string(),
                );
            }
            return create_form(Some(&draft), None, definition.executor, controller.limits());
        }
        if !fields.contains_key("executor") {
            return default_task_submission::form(None, None, controller.limits());
        }
        let executor = match fields.get("executor").map(String::as_str) {
            Some("native") => ExecutorKind::Native,
            None | Some("coding") => ExecutorKind::Coding,
            _ => return error(400, "Unknown executor"),
        };
        return create_form(None, None, executor, controller.limits());
    }
    if path == "/" || path == "/tasks" {
        let fields = match parse_form(query.as_bytes(), controller.limits()) {
            Ok(f) => f,
            Err(e) => return error(400, &e),
        };
        if let Err(e) = fields_only(&fields, &["after","page_cursor","cursor","max_bytes"]) {
            return error(400, &e);
        }
        let scoped = match read_pages::list_cursor(&fields) {Ok(cursor)=>cursor,Err(response)=>return response};
        if fields.contains_key("page_cursor") || fields.contains_key("cursor") || fields.contains_key("max_bytes") {
            return match boxed_route(||read_pages::public_list(controller,crate::read_page::ReadTool::Tasks,
                serde_json::json!({"cursor":scoped,"limit":controller.limits().product.page_items,"agent_id":null,"parent_task_id":null}),&fields)).await {Ok(response)|Err(response)=>response};
        }
        let reply = boxed_route(|| call(
            controller,
            Operation::ListTasks {
                after: fields.get("after").filter(|s| !s.is_empty()).cloned(),
                limit: controller.limits().product.page_items,
            },
        ))
        .await;
        return match reply.outcome {
            Outcome::Ok {
                data: Data::Tasks(tasks),
            } => {
                // Preserve the normal full-task/progress view. An empty legacy
                // result may instead mean its first record exceeded the old
                // whole-record cap; the shared projection distinguishes that
                // from exhaustion without skipping the original keyset item.
                if tasks.is_empty() {
                    return match boxed_route(||read_pages::public_list(controller,crate::read_page::ReadTool::Tasks,
                        serde_json::json!({"cursor":scoped,"limit":controller.limits().product.page_items,"agent_id":null,"parent_task_id":null}),&fields)).await {Ok(response)|Err(response)=>response};
                }
                let mut body = "<p><a href=\"/tasks/new\">Create a task</a> | <a href=\"/tasks\">Refresh</a></p><p>Recorded task status and progress update automatically while this page is visible.</p><form method=\"get\" action=\"/tasks\"><label>Agent<input name=\"agent_id\"></label><label>Parent task<input name=\"parent_task_id\"></label><button>Filter tasks</button></form><table><thead><tr><th>Task</th><th>Agent</th><th>Status</th><th>Progress or blocker</th></tr></thead><tbody>".to_string();
                for task in &tasks {
                    let _ = write!(
                        body,
                        "<tr><td><a href=\"{}\">{}</a></td><td><a href=\"{}\">{}</a></td><td>{}</td><td>{}</td></tr>",
                        task_path(&task.id),
                        escape(&task.id),
                        agents::path(&task.assigned_agent_id),
                        escape(&task.assigned_agent_id),
                        status(task.status),
                        escape(task.blocked_reason.as_deref().unwrap_or(&task.progress))
                    );
                }
                body.push_str("</tbody></table>");
                if tasks.is_empty() {
                    body.push_str("<p>No tasks in this page.</p>");
                }
                // The controller can return fewer than controller.limits().product.page_items to respect
                // its serialized response bound. A short page is not EOF.
                if let Some(last) = tasks.last() {
                    let _ = write!(
                        body,
                        "<p><a href=\"/tasks?after={}\">Next page</a></p>",
                        encode(&last.id)
                    );
                }
                page(200, "Tasks", body)
            }
            _ => not_task(reply),
        };
    }
    if let Some(id) = path.strip_prefix("/operations/") {
        let id = match decode(id, false) {
            Ok(id) if !id.is_empty() => id,
            _ => return error(400, "Invalid operation ID"),
        };
        let reply = boxed_route(|| call(
            controller,
            Operation::GetOperation {
                operation_id: id.clone(),
            },
        ))
        .await;
        if let Some(response) = definition_settings::receipt(&reply, &id) {
            return response;
        }
        if let Some(response) = collaboration_work::receipt(&reply, &id) {
            return response;
        }
        if matches!(&reply.outcome, Outcome::Refused { code, .. } if code == "not_found") {
            return uncertain(404, &id, "No committed outcome is currently recorded. This does not establish whether an earlier request applied; preserve its original identity and body for reconciliation.", None);
        }
        return match reply.outcome {
            Outcome::Ok {
                data: Data::Operation(record),
            } => {
                let public_record = match crate::pull_request_view::record(&record) {
                    Ok(value) => value,
                    Err(_) => return error(503, "The public operation view is unavailable; preserve the original request for reconciliation"),
                };
                let mut body = String::from("<p>This lookup reads the original recorded operation; it does not retry it. Private publication configuration is omitted from this view.</p>");
                if let Some(commit) = &reply.commit {
                    body.push_str("<dl>");
                    term(&mut body, "Original committed controller revision", &commit.controller_revision.to_string());
                    body.push_str("</dl>");
                }
                let _ = write!(body, "<pre>{}</pre>", escape(&serde_json::to_string_pretty(&public_record).unwrap_or_default()));
                if !record.task_id.is_empty() {
                    let _ = write!(body, "<a href=\"{}\">Task</a>", task_path(&record.task_id));
                }
                page(200, "Recorded operation", body)
            },
            _ => not_task(reply),
        };
    }
    if let Some(id) = path.strip_prefix("/releases/") {
        if !query.is_empty() || id.contains('/') {
            return error(400, "Invalid release candidate route");
        }
        let id = match decode(id, false) {
            Ok(id) if !id.is_empty() => id,
            _ => return error(400, "Invalid release candidate ID"),
        };
        let reply = boxed_route(|| call(
            controller,
            Operation::GetProtectedRelease { release_id: id },
        ))
        .await;
        return match reply.outcome {
            Outcome::Ok {
                data: Data::ProtectedRelease(view),
            } => render_release(&view.candidate, Some(&view)),
            _ => not_task(reply),
        };
    }
    if let Some(response)=boxed_route(|| read_pages::task(controller,path,query)).await { return response; }
    let Some(rest) = path.strip_prefix("/tasks/") else {
        return error(404, "Unknown browser route");
    };
    let (id, subpath) = rest.split_once('/').unwrap_or((rest, ""));
    let id = match decode(id, false) {
        Ok(id) if !id.is_empty() => id,
        _ => return error(400, "Invalid task ID"),
    };
    if !["", "checkpoint", "result", "releases"].contains(&subpath) {
        return error(405, "Task mutations require a same-origin POST form");
    }
    let reply = boxed_route(|| call(controller, Operation::GetTask { task_id: id })).await;
    match reply.outcome {
        Outcome::Ok {
            data: Data::Task(task),
        } => match subpath {
            "releases" => {
                let fields = match parse_form(query.as_bytes(), controller.limits()) {
                    Ok(fields) => fields,
                    Err(e) => return error(400, &e),
                };
                if let Err(e) = fields_only(&fields, &["after"]) {
                    return error(400, &e);
                }
                let reply = boxed_route(|| call(
                    controller,
                    Operation::ListReleases {
                        task_id: task.id.clone(),
                        after: fields.get("after").filter(|id| !id.is_empty()).cloned(),
                        limit: controller.limits().product.page_items,
                    },
                ))
                .await;
                match reply.outcome {
                    Outcome::Ok {
                        data: Data::Releases(candidates),
                    } => release_list(&task, &candidates),
                    _ => not_task(reply),
                }
            }
            "checkpoint" => task
                .selected_checkpoint
                .as_ref()
                .map(|c| page(200, "Published source checkpoint", checkpoint_view(c)))
                .unwrap_or_else(|| error(404, "No acknowledged source checkpoint is selected")),
            "result" => task
                .result
                .as_ref()
                .map(|r| {
                    let mut body = format!(
                        "<p><a href=\"{}\">Task</a></p><pre>{}</pre>",
                        task_path(&task.id),
                        escape(&serde_json::to_string_pretty(r).unwrap_or_default())
                    );
                    if let Some(c) = &r.checkpoint {
                        body.push_str(&checkpoint_view(c));
                    }
                    if let Some(reference) = &r.pull_request { body.push_str(&pull_request_result_view(reference)); }
                    page(200, "Accepted task result", body)
                })
                .unwrap_or_else(|| error(404, "No accepted task result")),
            _ => render_task(&task, controller.limits().workflow.is_some()),
        },
        _ => not_task(reply),
    }
}

async fn post(controller: &Controller, path: &str, fields: Form) -> UiResponse {
    let operation_id = match required(&fields, "operation_id") {
        Ok(v) => v.to_string(),
        Err(e) => return error(400, &e),
    };
    if operation_id.len() > controller.limits().transport.identity_bytes {
        return limit_error(400, failure::LimitingField::Identity);
    }
    if let Some(response) = boxed_route(|| documents_reviews::post(controller, path, &fields)).await { return response; }
    if collaboration_questions::route(path) { return boxed_route(|| collaboration_questions::post(controller, path, fields, &operation_id)).await; }
    boxed_route(|| post_other(controller, path, fields, operation_id)).await
}

// Identity and document/question checks still precede this unchanged route tail.
async fn post_other(controller: &Controller, path: &str, fields: Form, operation_id: String) -> UiResponse {
    if collaboration_work::route(path) { return boxed_route(|| collaboration_work::post(controller, path, fields, &operation_id)).await; }
    if path == "/repositories" || path.starts_with("/repositories/") {
        return boxed_route(|| repository_catalogue::post(controller, path, fields, &operation_id)).await;
    }
    if let Some(raw_id) = definition_settings::route(path) {
        return boxed_route(|| definition_settings::post(controller, raw_id, fields, &operation_id)).await;
    }
    if path == "/tasks" {
        let has_id = fields.get("definition_id").is_some_and(|value| !value.trim().is_empty());
        let has_revision = fields.get("definition_revision").is_some_and(|value| !value.trim().is_empty());
        if has_id != has_revision {
            return error(400, "Specify both agent ID and immutable revision, or omit both to use the default agent");
        }
        if !has_id {
            return boxed_route(|| default_task_submission::post(controller, fields, operation_id)).await;
        }
        let input = match task_input(&fields, controller.limits()) {
            Ok(input) => input,
            Err(e) => {
                // A rejected workflow may contain a credential-bearing endpoint
                // in a server-owned field. Do not echo that draft into the HTML.
                // Preserve the other safe form fields and the original refusal.
                let mut redisplay = fields.clone();
                if redisplay.get("native_workflow").is_some_and(|value|
                    !task_defaults::may_redisplay_native_workflow(value)) {
                    redisplay.remove("native_workflow");
                }
                return create_form(
                    Some(&redisplay),
                    Some(&e),
                    ExecutorKind::Coding,
                    controller.limits(),
                );
            }
        };
        let reply = boxed_route(|| call(
            controller,
            Operation::SubmitTask {
                operation_id: operation_id.clone(),
                input,
            },
        ))
        .await;
        return mutation_reply(reply, &operation_id, None);
    }
    let Some((id, action)) = path.strip_prefix("/tasks/").and_then(|p| p.split_once('/')) else {
        return error(404, "Unknown browser action");
    };
    let id = match decode(id, false) {
        Ok(id) if !id.is_empty() => id,
        _ => return error(400, "Invalid task ID"),
    };
    let allowed: &[&str] = if action == "assign" {
        &["operation_id", "expected_revision", "agent_id"]
    } else {
        &["operation_id", "expected_revision"]
    };
    if let Err(e) = fields_only(&fields, allowed) {
        return error(400, &e);
    }
    let expected_revision = match number(&fields, "expected_revision") {
        Ok(n) => n,
        Err(e) => return error(400, &e),
    };
    let command = match action {
        "assign" => {
            let agent_id = match required(&fields, "agent_id") {
                Ok(id) => id.into(),
                Err(e) => return error(400, &e),
            };
            Operation::AssignTask {
                operation_id: operation_id.clone(),
                task_id: id.clone(),
                expected_revision,
                agent_id,
            }
        }
        "cancel" => Operation::CancelTask {
            operation_id: operation_id.clone(),
            task_id: id.clone(),
            expected_revision,
        },
        "retry" => Operation::RetryTask {
            operation_id: operation_id.clone(),
            task_id: id.clone(),
            expected_revision,
        },
        _ => return error(404, "Unknown browser action"),
    };
    let reply = boxed_route(|| call(controller, command)).await;
    mutation_reply(reply, &operation_id, Some(&id))
}

/// All mutations share the controller's idempotency, revision and state checks.
//= spec/impl/v2/implementation.md#shared-product-state
//# The collaboration product MUST provide a browser user interface.
// Traceability scope: Browser request-path, body and rendered-response policy refusals.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//# An operation refused by a configured limit MUST identify the limiting metadata field.
async fn dispatch(controller: &Controller, request: UiRequest, response_operation_id: &mut Option<String>) -> UiResponse {
    if request.path.len() > controller.limits().product.request_path_bytes {
        return limit_error(413, failure::LimitingField::RequestPath);
    }
    if request.body.len() > controller.limits().transport.body_bytes {
        return limit_error(413, failure::LimitingField::Body);
    }
    let (path, query) = request.path.split_once('?').unwrap_or((&request.path, ""));
    *response_operation_id = if request.method == "GET" {
        path.strip_prefix(failure::OPERATION_PREFIX)
            .and_then(|raw| decode(raw, false).ok())
            .filter(|id| !id.is_empty())
    } else { None };
    match request.method.as_str() {
        "GET" => {
            // Live endpoints retain their exact existing route/response rules.
            // Dispatch them before polling the much larger ordinary-page future
            // so their controller call does not nest on that page's stack frame.
            if let Some(response) = live::get(controller, path, query).await {
                response
            } else {
                boxed_route(|| get(controller, path, query)).await
            }
        },
        "POST" => {
            if request.expected_origin.is_empty()
                || request.origin.as_deref() != Some(request.expected_origin.as_str())
            {
                return error(
                    403,
                    "State-changing browser requests require the configured same origin",
                );
            }
            if !query.is_empty() {
                return error(400, "Mutation values belong in the form body");
            }
            if path == "/sessions" || path.starts_with("/sessions/") {
                return error(405, "Sessions have read-only browser views");
            }
            if (path == "/agents" || path.starts_with("/agents/"))
                && definition_settings::route(path).is_none()
            {
                return error(405, "Agent identities have read-only browser views");
            }
            if path.starts_with("/releases/") || path.ends_with("/releases") {
                return error(405, "Release candidates have read-only browser views");
            }
            match parse_form(&request.body, controller.limits()) {
                Ok(fields) => {
                    *response_operation_id = fields.get("operation_id")
                        .filter(|id| crate::mcp::valid_identity(id, controller.limits().transport.identity_bytes))
                        .cloned();
                    boxed_route(|| post(controller, path, fields)).await
                },
                Err(e) => error(400, &e),
            }
        }
        _ => error(
            405,
            "Browser routes support GET views and same-origin POST forms",
        ),
    }
}

pub async fn handle(controller: &Controller, request: UiRequest) -> UiResponse {
    let mut operation_id = None;
    let response = boxed_route(|| dispatch(controller, request, &mut operation_id)).await;
    finalize(controller.limits(), response, operation_id.as_deref())
}

fn finalize(limits: &RuntimeLimits, mut response: UiResponse, operation_id: Option<&str>) -> UiResponse {
    // Every product return, including early refusal, passes the same bound after
    // optional metadata bootstrap. Compact failures never add another full page.
    if response.content_type.starts_with("text/html") {
        let bootstrap = serde_json::json!({"revision": limits.revision, "product": limits.product, "cursor_max": i64::MAX.to_string(), "workflow": limits.workflow, "workflow_cursor_bytes": limits.controller.collaboration_cursor_bytes, "workflow_identity_bytes": limits.transport.identity_bytes});
        let attribute = escape(&bootstrap.to_string());
        response.body = response.body.replacen(
            "<body>", &format!("<body data-runtime-limits=\"{attribute}\">"), 1,
        );
    }
    if response.body.len() > limits.product.response_bytes {
        response = failure::response(response.status, response.limiting_field, operation_id);
    }
    // Controller construction validates the actual encoder against every admitted
    // correlation shape. Do not silently truncate an identity or exempt errors.
    assert!(response.body.len() <= limits.product.response_bytes, "validated product response envelope");
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    fn metadata() -> RuntimeLimits {
        RuntimeLimits::seed().unwrap()
    }
    #[test]
    fn compact_finalization_keeps_pending_identity_and_removes_redirects_and_details() {
        let mut limits = metadata();
        limits.product.request_path_bytes = 128;
        limits.product.response_bytes = limits.product.minimum_response_bytes(&limits.transport).unwrap();
        limits.validate().unwrap();
        let original = "original-\"&é";
        let pending = finalize(&limits, uncertain(202, original, "private error detail", None), Some(original));
        assert_eq!(pending.status, 202);
        assert!(pending.body.len() <= limits.product.response_bytes);
        assert_eq!(pending.content_type, "text/plain; charset=utf-8");
        assert!(!pending.body.contains("private error detail"));
        assert!(!pending.body.contains("<script"));
        let encoded = pending.body.lines().find_map(|line| line.strip_prefix("Original operation: ")).unwrap();
        assert_eq!(serde_json::from_str::<String>(encoded).unwrap(), original);
        let mut committed = page(303, "Committed task state", "private record detail".into());
        committed.location = Some("/tasks/committed".into());
        let compact = finalize(&limits, committed, Some(original));
        assert_eq!(compact.status, 502);
        assert!(compact.location.is_none());
        assert!(!compact.body.contains("private record detail"));
        assert!(compact.body.contains("does not establish its outcome"));
    }

    #[test]
    //= spec/impl/v2/implementation.md#limits-are-versioned-metadata
    //= type=test
    //# An operation refused by a configured limit MUST identify the limiting metadata field.
    fn form_limit_refusals_identify_fields_and_accept_exact_bounds() {
        let mut limits = metadata();
        limits.transport.body_bytes = 3;
        assert!(parse_form(b"a=x", &limits).is_ok());
        assert_eq!(
            parse_form(b"a=xx", &limits).unwrap_err(),
            "Configured limit exceeded: transport.body_bytes"
        );
        limits.transport.body_bytes = 64;
        limits.product.form_fields = 1;
        assert!(parse_form(b"a=x", &limits).is_ok());
        assert_eq!(
            parse_form(b"a=x&b=y", &limits).unwrap_err(),
            "Configured limit exceeded: product.form_fields"
        );
        limits.product.form_key_bytes = 2;
        assert!(parse_form("%C3%A9=x".as_bytes(), &limits).is_ok());
        assert_eq!(
            parse_form("%C3%A9a=x".as_bytes(), &limits).unwrap_err(),
            "Configured limit exceeded: product.form_key_bytes"
        );
        for malformed in [b"=x".as_slice(), b"a=%GG", b"a"] {
            let message = parse_form(malformed, &limits).unwrap_err();
            assert!(!message.contains("Configured limit"), "{message}");
        }
    }

    #[test]
    fn form_decoder_rejects_ambiguous_or_malformed_input() {
        assert_eq!(parse_form(b"a=a%2Bb+c", &metadata()).unwrap()["a"], "a+b c");
        for invalid in ["a=x&a=y", "a=%", "a=%GG", "a=%FF", "a=%00", "a", "=x"] {
            assert!(
                parse_form(invalid.as_bytes(), &metadata()).is_err(),
                "{invalid}"
            );
        }
        assert!(
            parse_form(
                &vec![b'x'; metadata().transport.body_bytes + 1],
                &metadata()
            )
            .is_err()
        );
    }
    #[test]
    fn markup_and_links_do_not_execute_task_text() {
        assert_eq!(
            escape("<script a='\"'>&"),
            "&lt;script a=&#39;&quot;&#39;&gt;&amp;"
        );
        for url in [
            "javascript:alert(1)",
            "data:text/html,hello",
            "file:///etc/passwd",
            "https://user:secret@example.com/",
            "//example.com/",
        ] {
            assert!(safe_external_link(url).is_none());
        }
        assert_eq!(
            safe_external_link("https://example.com/repo").unwrap(),
            "https://example.com/repo"
        );
        assert_eq!(decode(&encode("a/b <&"), false).unwrap(), "a/b <&");
    }
    #[test]
    fn unresolved_outcome_is_not_rendered_as_success() {
        for outcome in [
            Outcome::Pending {
                operation_id: "same-id".into(),
            },
            Outcome::Uncertain {
                operation_id: "same-id".into(),
                message: "lost acknowledgment".into(),
            },
        ] {
            let reply = mutation_reply(
                Reply {
                    request_id: "r".into(),
                    commit: None,
                    observed_controller_revision: None,
                    outcome,
                },
                "same-id",
                Some("task"),
            );
            assert_eq!(reply.status, 202);
            assert!(reply.location.is_none());
            assert!(reply.body.contains("Operation outcome unresolved"));
            assert!(reply.body.contains("same-id"));
        }
    }

    #[test]
    fn store_response_loss_preserves_original_operation() {
        let unavailable = mutation_reply(
            Reply::refused(
                "r",
                "store_unavailable",
                "database owner unavailable; reconcile original operation",
            ),
            "original-submit",
            None,
        );
        assert_eq!(unavailable.status, 503);
        assert!(unavailable.body.contains("original-submit"));
        assert!(unavailable.body.contains("Operation outcome unresolved"));
        assert!(!unavailable.body.contains("The action was not accepted"));
    }

    #[test]
    // Traceability scope: Task data fixture verifies UI behavior; actual publication correctness is separately tested.
    //= spec/impl/v2/implementation.md#shared-product-state
    //= type=test
    //# The browser interface MUST link the task's published source checkpoints.
    fn task_view_retains_uncertainty_truncation_and_only_safe_checkpoint_navigation() {
        let checkpoint = Checkpoint {
            registration: crate::repository_catalogue::Revision {
                repository_id: "display-fixture".into(),
                registration_revision: "r1".into(),
            },
            repository: "javascript:alert('repository')".into(),
            commit: "<commit>".into(),
            remote_ref: "refs/heads/task".into(),
            operation_id: "checkpoint-original".into(),
        };
        let task = Task {
            pending_assignment: None,
            id: "task-view".into(),
            input: TaskInput { instruction_source: None,
                pull_request: None,
                resolved_limits: None,
                instruction: "<img src=x onerror=alert(1)>".into(),
                definition: Definition {
                    charter: None,
                    id: "template".into(),
                    revision: "r1".into(),
                    executor: ExecutorKind::Coding,
                    inference: None,
                    execution_profile: None,
                    limit_overrides: None,
                },
                repository_read: None,
                repository: None,
                acceptance_commands: vec![],
                output_schema: None,
                limits: Limits {
                    deadline_unix_ms: 9999,
                    model_requests: 1,
                    output_bytes: 100,
                    result_bytes: metadata().task_defaults.result_bytes,
                    max_delegation_depth: 0,
                },
                dependencies: vec!["prerequisite".into()],
                native_workflow: None,
            },
            session_id: "session".into(),
            assigned_agent_id: "template".into(),
            status: TaskStatus::Active,
            revision: 7,
            attempt: Some(Attempt {
                id: "attempt".into(),
                task_id: "task-view".into(),
                session_id: "session".into(),
                generation: 2,
                worker_id: "worker".into(),
                worker_incarnation: "incarnation".into(),
                phase: AttemptPhase::StopRequested,
                deadline_unix_ms: 9999,
                stopped: false,
                uncertain: true,
            }),
            selected_checkpoint: Some(checkpoint.clone()),
            result: None,
            blocked_reason: None, native_retry_block: None,
            progress: "<b>stopping</b>".into(),
            recent_output: "<script>output</script>".into(),
            output_truncated: true,
            parent_task_id: Some("parent<&".into()),
            waiting_child_id: Some("child/wait".into()),
            completed_child: None,
        };
        let view = render_task(&task, false);
        assert!(view.body.contains("stop requested"));
        assert!(view.body.contains("Unresolved outcome"));
        assert!(view.body.contains("Output is truncated"));
        assert!(view.body.contains("&lt;script&gt;output&lt;/script&gt;"));
        assert!(view.body.contains("/tasks/task-view/checkpoint"));
        assert!(view.body.contains("href=\"/tasks/parent%3C%26\""));
        assert!(view.body.contains("parent&lt;&amp;"));
        assert!(view.body.contains("href=\"/tasks/child%2Fwait\""));
        assert!(!view.body.contains("<img"));
        assert!(!view.body.contains("Request retry"));
        let checkpoint_html = checkpoint_view(&checkpoint);
        assert!(checkpoint_html.contains("&lt;commit&gt;"));
        assert!(!checkpoint_html.contains("href=\"javascript:"));
        assert!(checkpoint_html.contains("not a safe browser"));
    }

    #[test]
    fn child_outcome_keeps_failure_cancellation_and_success_distinct() {
        for child_status in [
            TaskStatus::Failed,
            TaskStatus::Cancelled,
            TaskStatus::Succeeded,
        ] {
            let result = AcceptedResult {
                pull_request: None,
                attempt_id: "attempt-child".into(),
                summary: "checked child result".into(),
                checkpoint: None,
                validation: vec!["passed".into()],
                output: None,
            };
            let child = ChildOutcome {
                task_id: "child/<script>".into(),
                status: child_status,
                result: Some(result),
                summary: "<script>child summary</script>".into(),
            };
            let html = child_outcome_view(&child);
            assert!(html.contains(&format!(
                "<dt>Child status</dt><dd>{}</dd>",
                status(child_status)
            )));
            assert!(html.contains("href=\"/tasks/child%2F%3Cscript%3E\""));
            assert!(html.contains("&lt;script&gt;child summary&lt;/script&gt;"));
            assert!(!html.contains("<script>"));
            assert!(html.contains("does not by itself complete this task"));
            assert_eq!(
                html.contains("Accepted child result</a>"),
                child_status == TaskStatus::Succeeded
            );
            assert_eq!(
                html.contains("No accepted successful child result"),
                child_status != TaskStatus::Succeeded
            );
        }
    }

    #[test]
    fn release_identity_and_links_are_escaped_without_inventing_artifact_access() {
        let candidate = ReleaseCandidate {
            id: "candidate/<script>".into(),
            task_id: "task/<script>".into(),
            task_revision: 9,
            accepted_attempt_id: "attempt<&".into(),
            revision: 1,
            checkpoint: Checkpoint {
                registration: crate::repository_catalogue::Revision {
                    repository_id: "display-fixture".into(),
                    registration_revision: "r1".into(),
                },
                repository: "javascript:alert(1)".into(),
                commit: "<commit>".into(),
                remote_ref: "refs/heads/quoted\"".into(),
                operation_id: "publication".into(),
            },
            artifact: ReleaseArtifact {
                format: "borg-v2-release/1".into(),
                package_sha256: "c".repeat(64),
                package_bytes: 20_480,
                manifest_sha256: "d".repeat(64),
                source_commit: "<commit>".into(),
                source_tree: "<tree>".into(),
                object_format: "sha1".into(),
                binary_sha256: "e".repeat(64),
                binary_bytes: 1024,
                target: "<img src=x>".into(),
            },
            qualification: ReleaseQualification {
                artifact: ArtifactQualification::Claimed,
                repository_submission: ProtectedStatus::Unconfigured,
                checks: ProtectedStatus::Unconfigured,
                human_rollout: ProtectedStatus::Unconfigured,
            },
        };
        let response = release_detail(&candidate);
        assert_eq!(response.status, 200);
        assert!(
            response
                .body
                .contains("href=\"/tasks/task%2F%3Cscript%3E\"")
        );
        assert!(response.body.contains("candidate/&lt;script&gt;"));
        assert!(response.body.contains("&lt;commit&gt;"));
        assert!(response.body.contains("&lt;img src=x&gt;"));
        assert!(!response.body.contains("<script>"));
        assert!(!response.body.contains("<img"));
        assert!(!response.body.contains("href=\"javascript:"));
        assert!(!response.body.contains("href=\"file:"));
        assert!(
            response
                .body
                .contains("No artifact download location is recorded here")
        );
        assert!(
            response
                .body
                .contains("Deployment outcome</dt><dd>Not recorded")
        );
        assert!(!response.body.contains("<form"));
    }

    #[test]
    fn uncommitted_capacity_is_unresolved_but_recorded_refusal_is_final() {
        for code in ["capacity", "store_unavailable"] {
            let mut reply = Reply::refused("r", code, "finite bound");
            let unresolved = mutation_reply(reply.clone(), "same-id", Some("task"));
            assert_eq!(unresolved.status, 503);
            assert!(unresolved.body.contains("Operation outcome unresolved"));
            assert!(unresolved.location.is_none());
            reply.commit = Some(MutationCommit {
                operation_id: "same-id".into(),
                controller_revision: 3,
            });
            reply.observed_controller_revision = Some(9);
            let recorded = mutation_reply(reply.clone(), "same-id", Some("task"));
            assert_eq!(recorded.status, 409);
            assert!(recorded.body.contains("Action refused"));
            assert_eq!(mutation_reply(reply, "other-id", Some("task")).status, 503);
        }
        for outcome in [
            Outcome::Pending {
                operation_id: "foreign".into(),
            },
            Outcome::Uncertain {
                operation_id: "foreign".into(),
                message: "unknown".into(),
            },
        ] {
            let response = mutation_reply(
                Reply {
                    request_id: "r".into(),
                    commit: None,
                    observed_controller_revision: None,
                    outcome,
                },
                "original",
                Some("task"),
            );
            assert_eq!(response.status, 503);
            assert!(response.body.contains("/operations/original"));
            assert!(!response.body.contains("/operations/foreign"));
        }
    }
}

#[cfg(test)]
mod agent_form_requirements {
    use super::*;
    fn metadata() -> RuntimeLimits {
        RuntimeLimits::seed().unwrap()
    }
    #[test]
    fn missing_settings_are_not_injected_into_invalid_input() {
        let fields = Form::from([
            ("operation_id".into(), "operation".into()),
            ("executor".into(), "native".into()),
        ]);
        assert!(task_input(&fields, &metadata()).is_err());
        let page = create_form(
            Some(&fields),
            Some("missing settings"),
            ExecutorKind::Native,
            &metadata(),
        );
        assert!(!page.body.contains("name=\"execution_profile\""));
        assert!(!page.body.contains("value=\"gpt-6-astra\" selected"));
        assert!(!page.body.contains("name=\"agent_id\""));
    }
}

#[cfg(test)]
mod metadata_tests {
    use super::*;
    #[test]
    fn changed_metadata_reaches_forms_and_explicit_replay_values_are_preserved() {
        let mut metadata = RuntimeLimits::seed().unwrap();
        metadata.task_defaults.model_requests = 37;
        metadata.task_defaults.output_bytes = 1234;
        metadata.task_defaults.result_bytes = 4567;
        metadata.task_defaults.max_delegation_depth = 3;
        let mut fields = create_defaults(ExecutorKind::Native, &metadata).unwrap();
        assert_eq!(fields["model_requests"], "37");
        assert_eq!(fields["output_bytes"], "1234");
        assert_eq!(fields["result_bytes"], "4567");
        assert_eq!(fields["max_delegation_depth"], "3");
        metadata.task_defaults.model_requests = 11;
        assert_eq!(model_request_allowance(&fields, &metadata).unwrap(), 37);
        fields.remove("model_requests");
        assert_eq!(model_request_allowance(&fields, &metadata).unwrap(), 11);
        metadata.product.form_fields = 1;
        assert!(parse_form(b"a=x", &metadata).is_ok());
        assert!(parse_form(b"a=x&b=y", &metadata).is_err());
        metadata.product.form_key_bytes = 1;
        assert!(parse_form(b"ab=x", &metadata).is_err());
    }
}
