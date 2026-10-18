//! Browser settings are an operator adapter over canonical immutable revisions.
//! No worker caller is manufactured and no policy or task state is kept here.
use super::*;
use crate::collaboration::{Query, QueryResult};
use crate::limits::DefinitionLimitField;

pub(super) fn route(path: &str) -> Option<&str> {
    path.strip_prefix("/agents/")?
        .strip_suffix("/settings")
        .filter(|id| !id.is_empty() && !id.contains('/'))
}

fn agent_id(raw: &str, limits: &RuntimeLimits) -> Result<String, String> {
    let id = decode(raw, false)?;
    if !crate::mcp::valid_identity(&id, limits.transport.identity_bytes) {
        return Err("Invalid agent identity".into());
    }
    Ok(id)
}

fn settings_path(id: &str) -> String {
    format!("{}/settings", agents::path(id))
}

fn field_name(field: DefinitionLimitField) -> String {
    serde_json::to_value(field)
        .expect("closed limit field serializes")
        .as_str()
        .expect("closed limit field is a string")
        .to_string()
}

fn field_unit(field: DefinitionLimitField) -> &'static str {
    match field {
        DefinitionLimitField::CodingModelContextWindow | DefinitionLimitField::NativeMaxOutputTokens => "tokens",
        DefinitionLimitField::CodingSummaryChars => "characters",
        DefinitionLimitField::NativeContextBytes | DefinitionLimitField::NativeRequestBytes
        | DefinitionLimitField::NativeHelperValueBytes | DefinitionLimitField::NativeResponseWireBytes
        | DefinitionLimitField::NativeOutputBytes | DefinitionLimitField::NativeResultBytes
        | DefinitionLimitField::CodingCommandOutputBytes | DefinitionLimitField::CodingSummaryBytes
        | DefinitionLimitField::CollaborationCompleteResultBytes => "bytes",
    }
}

fn json(value: &impl serde::Serialize) -> String {
    format!("<pre>{}</pre>", escape(&serde_json::to_string_pretty(value).unwrap_or_default()))
}

pub(super) async fn get(controller: &Controller, raw_id: &str, query: &str) -> UiResponse {
    match view(controller, raw_id, query).await {
        Ok(response) | Err(response) => response,
    }
}

async fn view(controller: &Controller, raw_id: &str, query: &str) -> Result<UiResponse, UiResponse> {
    let invalid = |e: String| error(400, &e);
    let id = agent_id(raw_id, controller.limits()).map_err(invalid)?;
    let fields = parse_form(query.as_bytes(), controller.limits()).map_err(|e| error(400, &e))?;
    fields_only(&fields, &["task_id"]).map_err(invalid)?;
    let reply = call(controller, Operation::GetAgent { agent_id: id.clone() }).await;
    let Outcome::Ok { data: Data::Agent(agent) } = reply.outcome else {
        return Err(not_task(reply));
    };
    if agent.retired { return Err(error(409, "This agent is retired; its accepted settings remain in historical sessions")); }
    let reply = call(controller, Operation::QueryCollaboration { query: Query::Limits { task_id: None } }).await;
    let Outcome::Ok { data: Data::Collaboration(QueryResult::Limits { global, .. }) } = reply.outcome else {
        return Err(not_task(reply));
    };
    // Reuse the shared resolution function. Only the canonical write validates
    // and commits a resulting configuration against current policy.
    let effective = global.resolve_definition(&agent.definition)
        .map_err(|e| error(409, &format!("Selected settings cannot be resolved: {e}")))?;
    let accepted = if let Some(task_id) = fields.get("task_id").filter(|id| !id.is_empty()) {
        let reply = call(controller, Operation::GetTask { task_id: task_id.clone() }).await;
        let Outcome::Ok { data: Data::Task(task) } = reply.outcome else {
            return Err(not_task(reply));
        };
        if task.input.definition.id != id {
            return Err(error(400, "The selected task belongs to another agent definition"));
        }
        Some(task)
    } else { None };
    let route = settings_path(&id);
    let mut body = format!("<p><a href=\"{}\">Agent and sessions</a> · <a href=\"{}\">Refresh selected settings</a></p><p>Changes select a new immutable definition for future submissions. Already accepted tasks and configured children retain their original settings. Global metadata and worker startup settings are unchanged.</p><dl>", agents::path(&id), route);
    term(&mut body, "Agent", &id);
    term(&mut body, "Selected definition revision", &agent.definition.revision);
    term(&mut body, "Current global metadata revision", &global.revision);
    term(&mut body, "Executor", if agent.definition.executor == ExecutorKind::Native { "native" } else { "coding" });
    if let Some(inference) = &agent.definition.inference {
        term(&mut body, "Model", &inference.model);
        term(&mut body, "Reasoning effort", &inference.reasoning_effort);
    }
    body.push_str("</dl><details><summary>Selected definition and current effective execution settings</summary>");
    body.push_str(&json(&agent.definition));
    body.push_str(&json(&effective));
    body.push_str("</details>");
    if let Some(policy) = &global.definition_updates {
        body.push_str("<dl>");
        term(&mut body, "Current change-policy revision", &policy.revision);
        body.push_str("</dl>");
        if policy.allowed_fields.is_empty() {
            body.push_str("<p class=\"notice\">The current policy allows no settings changes.</p>");
        } else {
            let _ = write!(body, "<h2>Change future-task limits</h2><p id=\"settings-change-help\">Only the fields allowed by the current policy are editable. Leave a field blank to retain its effective value. The controller checks the complete resulting configuration and all three displayed revisions before committing.</p><form method=\"post\" action=\"{route}\" aria-describedby=\"settings-change-help\"><fieldset><legend>Allowed execution limits</legend>");
            body.push_str(&hidden("operation_id", &identity()));
            body.push_str(&hidden("expected_revision", &agent.definition.revision));
            body.push_str(&hidden("expected_global_revision", &global.revision));
            body.push_str(&hidden("expected_policy_revision", &policy.revision));
            for limit in &policy.allowed_fields {
                let name = field_name(*limit);
                let value = limit.value(&effective).map(|n| n.to_string()).unwrap_or_else(|| "executor default (unspecified)".into());
                let _ = write!(body, "<label>{} <small>Current effective value: {} · {}</small><input name=\"{}\" type=\"text\" inputmode=\"numeric\" pattern=\"[0-9]+\" placeholder=\"Keep {}\"></label>", escape(&name), escape(&value), field_unit(*limit), escape(&name), escape(&value));
            }
            body.push_str("</fieldset><button type=\"submit\">Commit settings for future submissions</button></form>");
        }
    } else {
        body.push_str("<p class=\"notice\">Settings changes are disabled: no definition update policy is configured.</p>");
    }
    let _ = write!(body, "<h2>Compare an accepted task</h2><form method=\"get\" action=\"{route}\">");
    body.push_str(&field("task_id", "Task ID", fields.get("task_id").map(String::as_str).unwrap_or(""), "text", true));
    body.push_str("<button>Read accepted settings</button></form>");
    if let Some(task) = accepted {
        let snapshot = task.input.resolved_limits.as_ref().ok_or_else(|| error(502, "Accepted task has no retained execution settings"))?;
        body.push_str("<h3>Accepted settings remain pinned</h3><dl>");
        term(&mut body, "Task", &task.id);
        term(&mut body, "Accepted definition revision", &task.input.definition.revision);
        term(&mut body, "Accepted global metadata revision", &snapshot.global_metadata.revision);
        body.push_str("</dl>");
        body.push_str(&json(snapshot));
        let _ = write!(body, "<p><a href=\"/limits?task_id={}\">All accepted task limits</a> · <a href=\"{}\">Task evidence</a></p>", encode(&task.id), task_path(&task.id));
    }
    Ok(page(200, "Agent settings", body))
}

pub(super) async fn post(controller: &Controller, raw_id: &str, fields: Form, operation_id: &str) -> UiResponse {
    let update = match parse_update(raw_id, &fields, controller.limits()) {
        Ok(update) => update,
        Err(message) => return error(400, &format!("Settings were not submitted: {message}")),
    };
    let fallback = settings_path(&update.agent_id);
    let reply = call(controller, Operation::UpdateDefinition { operation_id: operation_id.into(), update }).await;
    mutation_reply_for(reply, operation_id, None, &fallback)
}

fn parse_update(raw_id: &str, fields: &Form, limits: &RuntimeLimits) -> Result<DefinitionUpdate, String> {
    let mut changes = BTreeMap::new();
    for (name, value) in fields {
        if ["operation_id", "expected_revision", "expected_global_revision", "expected_policy_revision"].contains(&name.as_str()) { continue; }
        let field: DefinitionLimitField = serde_json::from_value(serde_json::Value::String(name.clone()))
            .map_err(|_| "Unknown definition-limit field".to_string())?;
        if value.is_empty() { continue; }
        if !value.bytes().all(|b| b.is_ascii_digit()) {
            return Err(format!("{name} must be an integer"));
        }
        let value = value.parse::<usize>().map_err(|_| format!("{name} exceeds the supported integer representation"))?;
        changes.insert(field, value);
    }
    Ok(DefinitionUpdate {
        agent_id: agent_id(raw_id, limits)?,
        expected_revision: required(fields, "expected_revision")?.into(),
        expected_global_revision: required(fields, "expected_global_revision")?.into(),
        expected_policy_revision: required(fields, "expected_policy_revision")?.into(),
        changes,
    })
}

pub(super) fn committed(operation_id: &str, outcome: &DefinitionUpdateOutcome) -> UiResponse {
    let definition = &outcome.selected_definition;
    let class = match outcome.classification {
        ConfigurationChangeClass::Neutral => "neutral",
        ConfigurationChangeClass::Narrowing => "narrowing",
        ConfigurationChangeClass::Widening => "widening",
        ConfigurationChangeClass::PreviousUnspecified => "previous executor window unspecified; other changed fields may widen",
    };
    let mut body = String::from("<p>The controller committed a new immutable definition and selected it for future submissions. Already accepted tasks and configured children retain their original settings. This receipt does not report a retry, completed task, or measured improvement.</p><dl>");
    term(&mut body, "Operation identity", operation_id);
    term(&mut body, "Previous definition revision", &outcome.previous_revision);
    term(&mut body, "Committed selected definition revision", &definition.revision);
    term(&mut body, "Global metadata revision at commit", &outcome.global_revision);
    term(&mut body, "Change-policy revision at commit", &outcome.policy.revision);
    term(&mut body, "Classification", class);
    term(&mut body, "Effective for", "future_submissions");
    body.push_str("</dl>");
    if outcome.classification == ConfigurationChangeClass::Neutral {
        body.push_str("<p>Effective resource allowances were unchanged. A new immutable revision was still committed.</p>");
    }
    let _ = write!(body, "<p><a href=\"/operations/{}\">Look up this original operation</a> · <a href=\"{}\">Read current selected settings</a> · <a href=\"/tasks/new?agent_id={}\">Start a new session from the current selected head</a></p><p>A later change may have selected another revision; this receipt retains the original outcome.</p><details><summary>Committed change outcome</summary>{}</details>", encode(operation_id), settings_path(&definition.id), encode(&definition.id), json(outcome));
    page(200, "Committed settings change", body)
}

/// Reconcile from the original durable operation without reconstructing a write
/// or substituting the current selected head for the original outcome.
pub(super) fn receipt(reply: &Reply, operation_id: &str) -> Option<UiResponse> {
    let Outcome::Ok { data: Data::Operation(record) } = &reply.outcome else { return None; };
    if record.kind != "application_mutation" { return None; }
    let operation: Operation = serde_json::from_value(record.intended.clone()).ok()?;
    let Operation::UpdateDefinition { operation_id: original_id, update } = operation else { return None; };
    if reply.validate_for(&Operation::GetOperation { operation_id: operation_id.into() }).is_err()
        || record.id != operation_id || original_id != operation_id
    {
        return Some(uncertain(503, operation_id, "The stored settings operation identity differs; preserve the original request for reconciliation.", None));
    }
    let outcome = record.outcome.as_ref().and_then(|value| serde_json::from_value::<Outcome>(value.clone()).ok());
    Some(match outcome {
        Some(outcome) => mutation_reply_for(Reply { request_id: reply.request_id.clone(), commit: reply.commit.clone(), observed_controller_revision: reply.observed_controller_revision, outcome }, operation_id, None, &settings_path(&update.agent_id)),
        None => uncertain(503, operation_id, "The stored settings outcome is unavailable; preserve the original request for reconciliation.", None),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parsing_retains_cas_values_and_leaves_policy_to_the_canonical_operation() {
        let limits = RuntimeLimits::seed().unwrap();
        let mut form = Form::from([
            ("operation_id".into(), "same-operation".into()),
            ("expected_revision".into(), "shown-revision".into()),
            ("expected_global_revision".into(), "shown-global".into()),
            ("expected_policy_revision".into(), "shown-policy".into()),
            ("native.output_bytes".into(), "0".into()),
            ("native.context_bytes".into(), "".into()),
        ]);
        let update = parse_update("agent", &form, &limits).unwrap();
        assert_eq!(update.expected_revision, "shown-revision");
        assert_eq!(update.expected_global_revision, "shown-global");
        assert_eq!(update.expected_policy_revision, "shown-policy");
        assert_eq!(update.changes.len(), 1);
        assert_eq!(update.changes[&DefinitionLimitField::NativeOutputBytes], 0);
        form.insert("executor".into(), "coding".into());
        assert!(parse_update("agent", &form, &limits).is_err());
    }
}
