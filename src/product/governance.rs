//! Browser presentation of existing operator API records. This adapter never
//! calls the operator controller entrypoint on behalf of a Browser invocation.
use super::*;

pub(super) fn route(path: &str) -> bool {
    matches!(path, "/governance" | "/ui/governance.js")
}

pub(super) fn get(controller: &Controller, path: &str, query: &str) -> UiResponse {
    if path == "/ui/governance.js" {
        return UiResponse { status: 200, content_type: "text/javascript; charset=utf-8".into(),
            location: None, limiting_field: None, body: include_str!("governance.js").into() };
    }
    let fields = match parse_form(query.as_bytes(), controller.limits()) {
        Ok(value) => value,
        Err(message) => return error(400, &message),
    };
    if let Err(message) = fields_only(&fields, &["task_id", "decision_id", "point", "original_control"]) {
        return error(400, &message);
    }
    for key in ["task_id", "decision_id", "original_control"] {
        if fields.get(key).is_some_and(|value| !value.is_empty()
            && !crate::mcp::valid_identity(value, controller.limits().transport.identity_bytes)) {
            return error(400, "Invalid governance record identity");
        }
    }
    let point = fields.get("point").map(String::as_str).unwrap_or("completion");
    if !["completion", "definition_limits", "charter_configuration", "emergency_control", "rule_administration"].contains(&point) {
        return error(400, "Unknown governance decision point");
    }
    let limits = controller.limits();
    let bootstrap = serde_json::json!({
        "wire": WIRE_REVISION,
        "body_bytes": limits.transport.body_bytes,
        "identity_bytes": limits.transport.identity_bytes,
        "request_timeout_ms": limits.product.live.request_timeout_ms,
        "task_id": fields.get("task_id"),
        "decision_id": fields.get("decision_id"), "point": point,
        "original_control": fields.get("original_control"),
    });
    let mut body = format!("<section id=\"governance-root\" data-governance=\"{}\"><p>This view reads the controller's existing operator API. It does not identify a person, confer authority or prove a process stopped.</p><p id=\"governance-status\" role=\"status\">Loading recorded governance</p><button id=\"governance-refresh\" type=\"button\">Read current governance</button><section id=\"governance-selection\"><h2>Selected configuration</h2></section><section id=\"governance-task\"></section><section id=\"governance-decision\"><h2>Recorded decision</h2><p>No decision selected.</p></section>", escape(&bootstrap.to_string()));
    body.push_str("<form method=\"get\" action=\"/governance\"><fieldset><legend>Read an original decision</legend>");
    body.push_str(&field("decision_id", "Original operation ID", fields.get("decision_id").map(String::as_str).unwrap_or(""), "text", true));
    body.push_str("<label>Decision point<select name=\"point\">");
    for value in ["completion", "definition_limits", "charter_configuration", "emergency_control", "rule_administration"] {
        let _ = write!(body, "<option value=\"{value}\"{}>{}</option>", if point == value { " selected" } else { "" }, value.replace('_', " "));
    }
    body.push_str("</select></label><button>Read recorded decision</button></fieldset></form><h2>Operator controls</h2><p>These actions use the existing same-origin operator API. A pause blocks fresh admission; releasing it only removes that hold. Stop records an intent: read the task's termination confirmation separately. Expiry reconciliation does not release a hold or resume work.</p><noscript>JavaScript is required for the existing operator API controls. No action has been sent.</noscript>");
    for (action, title) in [("pause", "Pause admission"), ("release_hold", "Release exact hold"), ("stop", "Request task stop"), ("reconcile_expiry", "Reconcile recorded override expiry")] {
        let _ = write!(body, "<form method=\"post\" action=\"/governance\" class=\"governance-control\" data-control=\"{action}\"><fieldset><legend>{title}</legend>");
        body.push_str(&hidden("operation_id", &identity()));
        if matches!(action, "pause" | "release_hold") {
            body.push_str("<label>Scope<select name=\"scope\"><option value=\"task\">One task</option><option value=\"all_tasks\">All tasks</option></select></label>");
        }
        if action != "reconcile_expiry" {
            body.push_str(&field("task_id", "Task ID (required for one task)", fields.get("task_id").map(String::as_str).unwrap_or(""), "text", false));
            body.push_str(&field("reason", "Reason", "", "text", true));
        }
        if action == "stop" {
            body.push_str(&field("expected_revision", "Exact current task revision", "", "text", true));
        }
        if matches!(action, "release_hold" | "reconcile_expiry") {
            body.push_str(&field("expected_operation_id", if action == "release_hold" { "Original pause operation ID" } else { "Original override operation ID" }, "", "text", true));
        }
        let _ = write!(body, "<button type=\"submit\" disabled>{title}</button></fieldset><p class=\"control-status\" role=\"status\">Not submitted</p><a class=\"control-lookup\" hidden>Read original operation</a></form>");
    }
    body.push_str("</section><script src=\"/ui/governance.js\" defer></script>");
    let mut response = page(200, "Governance", body);
    // This page retains original control intents until explicit navigation. Its
    // dedicated read button refreshes only projections, never forms or writes.
    response.body = response.body.replacen("data-live-refresh=\"true\"", "data-live-refresh=\"false\"", 1);
    response
}
