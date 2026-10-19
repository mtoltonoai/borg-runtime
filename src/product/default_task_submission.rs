//! Browser drafts preserve an omitted assignee until canonical admission.
//! This module never reads an agent head or constructs a substitute definition.
use super::*;

// Traceability scope: The ordinary form offers explicit target configuration and retains draft limits; only the canonical operation admits assignment.
//= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
//# A reassigned session MUST bind the selected target agent's accepted definition revision.
//= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
//# A task reassignment MUST preserve the task's accepted deadline.
//= spec/impl/v2/implementation.md#default-task-assignment
//# The browser interface MUST support task submission without an explicit agent selection.
pub(super) fn form(fields: Option<&Form>, problem: Option<&str>, metadata: &RuntimeLimits) -> UiResponse {
    let defaults;
    let fields = match fields {
        Some(fields) => fields,
        None => {
            let now = match SystemTime::now().duration_since(UNIX_EPOCH)
                .ok().and_then(|value| u64::try_from(value.as_millis()).ok()) {
                Some(now) => now,
                None => return error(400, "Clock is outside task representation"),
            };
            let limits = match metadata.task_defaults.at(now) {
                Ok(limits) => limits,
                Err(message) => return error(400, &message),
            };
            defaults = Form::from([
                ("operation_id".into(), identity()),
                ("deadline_unix_ms".into(), limits.deadline_unix_ms.to_string()),
                ("model_requests".into(), limits.model_requests.to_string()),
                ("output_bytes".into(), limits.output_bytes.to_string()),
                ("result_bytes".into(), limits.result_bytes.to_string()),
                ("max_delegation_depth".into(), limits.max_delegation_depth.to_string()),
            ]);
            &defaults
        }
    };
    let value = |key: &str| fields.get(key).map(String::as_str).unwrap_or("");
    let mut body = String::new();
    if let Some(problem) = problem {
        let _ = write!(body, "<p class=\"notice\" role=\"alert\">{}</p>", escape(problem));
    }
    match &metadata.task_defaults.agent_id {
        Some(id) => { let _ = write!(body,
            "<p>Default agent: <strong>{}</strong>. The controller selects its active definition when the task is accepted.</p>", escape(id)); }
        None => body.push_str("<p class=\"notice\">No default agent is configured (task_defaults.agent_id). Choose an explicit agent or configure a default before submitting.</p>"),
    }
    body.push_str("<p><a href=\"/agents\">Choose an agent</a> · <a href=\"/tasks/new?executor=coding\">Specify a coding definition</a> · <a href=\"/tasks/new?executor=native\">Specify a native definition</a></p><form method=\"post\" action=\"/tasks\">");
    body.push_str(&hidden("operation_id", value("operation_id")));
    body.push_str(&hidden("model_requests", value("model_requests")));
    body.push_str("<label>Instruction<textarea name=\"instruction\" rows=\"7\" required>");
    body.push_str(&escape(value("instruction")));
    body.push_str("</textarea></label><label>Native output JSON schema (required for native agents)<textarea name=\"output_schema\" rows=\"3\">");
    body.push_str(&escape(value("output_schema")));
    body.push_str("</textarea></label><details><summary>Repository and workflow options</summary><fieldset><legend>Coding repository</legend><p>For a coding agent, select a registered repository, revision and exact starting commit. Leave empty for a native agent.</p>");
    for (name, label) in [("repository_id", "Registered repository ID"), ("registration_revision", "Immutable registration revision"), ("start_commit", "Exact starting commit")] {
        body.push_str(&field(name, label, value(name), "text", false));
    }
    body.push_str("</fieldset><label>Coding acceptance commands (JSON argument arrays)<textarea name=\"acceptance_commands\" rows=\"3\">");
    body.push_str(&escape(value("acceptance_commands")));
    body.push_str("</textarea></label>");
    for (legend, entries) in [
        ("Optional coding instruction source", [("instruction_source_id", "Repository ID"), ("instruction_source_revision", "Registration revision"), ("instruction_source_commit", "Exact commit")]),
        ("Optional native repository read scope", [("repository_read_id", "Repository ID"), ("repository_read_revision", "Registration revision"), ("repository_read_commit", "Exact commit")]),
    ] {
        let _ = write!(body, "<fieldset><legend>{legend}</legend><p>Supply all three fields or leave all empty.</p>");
        for (name, label) in entries { body.push_str(&field(name, label, value(name), "text", false)); }
        body.push_str("</fieldset>");
    }
    body.push_str(&field("pull_request_title", "Optional coding pull request title", value("pull_request_title"), "text", false));
    body.push_str("<label>Pull request body<textarea name=\"pull_request_body\" rows=\"3\">");
    body.push_str(&escape(value("pull_request_body")));
    body.push_str("</textarea></label><p>Use an explicit reassign target to move this same task to a coding agent. Supply its repository, checks and complete limits matching this task, identical dependencies and an empty instruction_prefix. The task keeps its instruction and result schema; no child or return continuation is created.</p><label>Optional native workflow JSON<textarea name=\"native_workflow\" rows=\"5\">");
    body.push_str(&escape(value("native_workflow")));
    body.push_str("</textarea></label><p>A coding child requires its explicit definition, repository registration, exact commit and all task limits. Repository endpoints are controller-owned.</p></details>");
    body.push_str(&task_defaults::reassignment_help(fields));
    body.push_str(&field("dependencies", "Existing prerequisite task IDs (comma separated)", value("dependencies"), "text", false));
    body.push_str("<details><summary>Execution bounds</summary><p>The selected metadata model-request allowance is retained in this draft.</p>");
    for (name, label) in [("deadline_unix_ms", "Absolute deadline (Unix milliseconds)"), ("output_bytes", "Maximum output bytes"), ("result_bytes", "Maximum accepted result bytes"), ("max_delegation_depth", "Maximum delegation depth")] {
        body.push_str(&field(name, label, value(name), "number", true));
    }
    body.push_str("</details><button type=\"submit\">Submit task</button></form>");
    page(if problem.is_some() { 400 } else { 200 }, "Create task", body)
}

fn present<'a>(fields: &'a Form, key: &str) -> Option<&'a str> {
    fields.get(key).map(String::as_str).filter(|value| !value.trim().is_empty())
}
fn read_source(fields: &Form, keys: [&str; 3], metadata: &RuntimeLimits) -> Result<Option<RepositoryReadSource>, String> {
    if keys.iter().all(|key| present(fields, key).is_none()) { return Ok(None); }
    let registration = repository_catalogue::binding(fields, keys[0], keys[1], metadata)?;
    let commit = required(fields, keys[2])?;
    if !matches!(commit.len(), 40 | 64) || !commit.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) {
        return Err("Repository reads require an exact lowercase commit".into());
    }
    Ok(Some(RepositoryReadSource { registration: Some(registration), repository: String::new(), start_commit: commit.into() }))
}

fn draft(fields: &Form, metadata: &RuntimeLimits) -> Result<TaskDraft, String> {
    fields_only(fields, &[
        "operation_id", "instruction", "definition_id", "definition_revision",
        "repository_id", "registration_revision", "start_commit", "acceptance_commands", "output_schema",
        "instruction_source_id", "instruction_source_revision", "instruction_source_commit",
        "repository_read_id", "repository_read_revision", "repository_read_commit",
        "pull_request_title", "pull_request_body", "native_workflow", "dependencies",
        "deadline_unix_ms", "model_requests", "output_bytes", "result_bytes", "max_delegation_depth",
    ])?;
    if present(fields, "definition_id").is_some() || present(fields, "definition_revision").is_some() {
        return Err("Specify both agent ID and immutable revision, or omit both to use the default agent".into());
    }
    let repository = if ["repository_id", "registration_revision", "start_commit"].iter().all(|key| present(fields, key).is_none()) {
        None
    } else {
        Some(Repository {
            registration: Some(repository_catalogue::binding(fields, "repository_id", "registration_revision", metadata)?),
            start_commit: required(fields, "start_commit")?.into(),
            url: String::new(), checkpoint_remote: String::new(), pull_request_target: None,
        })
    };
    let title = fields.get("pull_request_title").map(String::as_str).unwrap_or("");
    let body = fields.get("pull_request_body").map(String::as_str).unwrap_or("");
    let pull_request = if title.is_empty() && body.is_empty() { None } else {
        if title.trim().is_empty() { return Err("A pull request title is required when a body is supplied".into()); }
        Some(crate::pull_request::Request { title: title.into(), body: body.into() })
    };
    // Decode exact child limits rather than re-defaulting a retained request
    // from current metadata. The controller validates the selected executor.
    let native_workflow = present(fields, "native_workflow")
        .map(|value| serde_json::from_str::<Option<Box<NativeWorkflow>>>(value)
            .map_err(|_| "Invalid native workflow JSON; supply all configured limits".to_string()))
        .transpose()?.flatten();
    if matches!(native_workflow.as_deref(), Some(NativeWorkflow::InspectCurrentTaskThenContinue {})) {
        return Err("Use the normal collaboration tools to inspect the current task; inspect_current_task_then_continue is no longer accepted by this form".into());
    }
    match native_workflow.as_deref() {
        Some(NativeWorkflow::CodingThenContinue { child }) => task_defaults::validate_configured_target(child, metadata)?,
        Some(NativeWorkflow::Reassign { target }) => {
            task_defaults::validate_configured_target(target, metadata)?;
            if target.definition.executor != ExecutorKind::Coding || !target.instruction_prefix.is_empty() {
                return Err("Reassignment requires a coding target and an empty instruction_prefix".into());
            }
        }
        _ => {}
    }
    Ok(TaskDraft {
        definition: None,
        instruction: required(fields, "instruction")?.into(),
        instruction_source: read_source(fields, ["instruction_source_id", "instruction_source_revision", "instruction_source_commit"], metadata)?,
        repository_read: read_source(fields, ["repository_read_id", "repository_read_revision", "repository_read_commit"], metadata)?,
        repository,
        pull_request,
        acceptance_commands: present(fields, "acceptance_commands")
            .map(|value| serde_json::from_str(value).map_err(|_| "Acceptance commands must be JSON argument arrays".to_string()))
            .transpose()?.unwrap_or_default(),
        output_schema: present(fields, "output_schema")
            .map(|value| serde_json::from_str(value).map_err(|_| "Invalid output schema JSON".to_string())).transpose()?,
        dependencies: fields.get("dependencies").map(|value| value.split([',', '\n', '\r'])
            .map(str::trim).filter(|value| !value.is_empty()).map(String::from).collect()).unwrap_or_default(),
        limits: Limits {
            deadline_unix_ms: number(fields, "deadline_unix_ms")?,
            model_requests: number(fields, "model_requests")?,
            output_bytes: number(fields, "output_bytes")?,
            result_bytes: number(fields, "result_bytes")?,
            max_delegation_depth: number(fields, "max_delegation_depth")?,
        },
        native_workflow,
    })
}

pub(super) async fn post(controller: &Controller, fields: Form, operation_id: String) -> UiResponse {
    let input = match draft(&fields, controller.limits()) {
        Ok(input) => input,
        Err(message) => {
            let mut redisplay = fields;
            if redisplay.get("native_workflow").is_some_and(|value| !task_defaults::may_redisplay_native_workflow(value)) {
                redisplay.remove("native_workflow");
            }
            return form(Some(&redisplay), Some(&message), controller.limits());
        }
    };
    let reply = boxed_route(|| call(controller, Operation::SubmitTaskDraft {
        operation_id: operation_id.clone(), input: (input).into(),
    })).await;
    mutation_reply(reply, &operation_id, None)
}
