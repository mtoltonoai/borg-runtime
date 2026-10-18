//! Shared private-host MCP contract. Caller headers are host correlation, not
//! authentication or project isolation. Occurrences are issued durably by the
//! host adapter; JSON-RPC IDs and model arguments never supply authority.
use crate::{
    controller::Controller,
    limits::{RuntimeLimits, TransportLimits},
    protocol::*,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
mod client;
pub mod identity_administration;
pub(crate) mod documents_reviews;
#[cfg(test)]
mod instruction_source_tests;
#[cfg(test)]
mod profile_tests;
pub(crate) mod questions;
pub(crate) mod strict;
pub(crate) mod work;
pub(crate) use client::CallFailure;
pub use client::Client;
mod read_recovery;
pub(crate) use read_recovery::retryable_read_tool;
pub const VERSION: &str = "2025-06-18";
pub const CALLER_HEADER: &str = "x-borg-caller";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Caller {
    pub task_id: String,
    pub attempt_id: String,
    pub generation: u64,
    pub worker_id: String,
    pub incarnation: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Invocation {
    pub operation_id: String,
    pub logical_call_id: String,
    pub caller: Caller,
    pub tool: String,
    pub arguments: Value,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CallResult {
    pub reply: Reply,
    /// Historical disposition of this exact call, not fresh launch authority.
    pub requires_quiescence: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ListPage {
    pub items: Vec<Value>,
    pub next_cursor: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Tool {
    pub name: String,
    pub description: String,
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
    pub annotations: Value,
}
/// Render the closed tool mechanisms against the selected metadata. UTF-8
/// byte bounds are explicit extensions because JSON Schema maxLength counts
/// Unicode scalar values, while controller records consume encoded bytes.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//# The shared agent interface MUST provide bounded repository-catalogue list and get operations.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//# Agent tools MUST NOT mutate repository registrations.
pub fn catalogue(limits: &RuntimeLimits) -> Vec<Tool> {
    let mut tools: Vec<Tool> = serde_json::from_str(include_str!("catalogue.json"))
        .expect("fixed MCP catalogue mechanisms");
    for tool in &mut tools {
        let repository = repository_query(&tool.name);
        let properties = tool.input_schema["properties"]
            .as_object_mut()
            .expect("fixed MCP property shape");
        for (name, schema) in properties {
            let types = &schema["type"];
            let string = types == "string"
                || types
                    .as_array()
                    .is_some_and(|types| types.iter().any(|t| t == "string"));
            if string && schema.get("enum").is_none() {
                let (_, bytes) = string_limit(name, repository, limits);
                schema["maxLength"] = json!(bytes);
                schema["x-borg-maxUtf8Bytes"] = json!(bytes);
            }
            if let Some((_, maximum)) = integer_limit(name, repository, limits) {
                schema["maximum"] = json!(maximum);
            } else if name == "expected_revision" && !string {
                // Revisions are represented by SQLite signed integers. This
                // numeric width is mechanism, not adjustable operator policy.
                schema["maximum"] = json!(i64::MAX);
            }
        }
    }
    tools
}

/// Exact accepted profile projection. The original catalogue remains V1; only
/// an explicitly selected V2 plus configured workflow exposes added tools.
/// Host delivery observation, preparation, begin and acknowledgment are absent.
pub fn catalogue_for_profile(
    limits: &RuntimeLimits,
    profile: Option<&ExecutionProfile>,
) -> Vec<Tool> {
    let Some(profile) = profile else {
        return Vec::new();
    };
    let mut tools = catalogue(limits);
    if matches!(profile, ExecutionProfile::CollaborationV2 { .. }) && limits.workflow.is_some() {
        // Append after the original flat-field metadata renderer. Each nested
        // domain schema retains its own metadata field and structural validator.
        tools.extend(questions::catalogue(limits));
        tools.extend(documents_reviews::catalogue(limits));
        tools.extend(work::catalogue(limits));
    }
    if matches!(profile, ExecutionProfile::CollaborationV2 { .. }) {
        tools.extend(identity_administration::catalogue(limits, limits.execution.collaboration.capabilities.as_ref()));
    }
    tools
}

/// An optional accepted Coding source changes only that task's repository
/// selectors. The original profile-only projection remains byte-for-byte V1/V2.
pub fn catalogue_for_task(
    limits: &RuntimeLimits,
    profile: Option<&ExecutionProfile>,
    instruction_source: Option<&RepositoryReadSource>,
) -> Vec<Tool> {
    let mut tools = catalogue_for_profile(limits, profile);
    if instruction_source.is_some() {
        for tool in &mut tools {
            if repository_query(&tool.name) {
                for name in ["source", "base_source"] {
                    if let Some(values) = tool
                        .input_schema
                        .get_mut("properties")
                        .and_then(|properties| properties.get_mut(name))
                        .and_then(|property| property.get_mut("enum"))
                        .and_then(Value::as_array_mut)
                    {
                        values.push(json!("instructions"));
                    }
                }
            }
        }
    }
    tools
}

pub fn validate_arguments_for_task(
    tool_name: &str,
    arguments: &Value,
    limits: &RuntimeLimits,
    profile: Option<&ExecutionProfile>,
    instruction_source: Option<&RepositoryReadSource>,
) -> Result<(), String> {
    if workflow_tool(tool_name) || instruction_source.is_none() {
        return validate_arguments_for_profile(tool_name, arguments, limits, profile);
    }
    let tool = catalogue_for_task(limits, profile, instruction_source)
        .into_iter()
        .find(|tool| tool.name == tool_name)
        .ok_or("tool is outside the collaboration catalogue")?;
    validate_tool_arguments(tool_name, arguments, limits, &tool)
}

pub(crate) fn workflow_tool(name: &str) -> bool {
    questions::handles(name) || documents_reviews::owns(name) || work::handles(name)
}

/// Structural admission is profile-bound; current actor and configured domain
/// limits are still checked by the canonical controller receipt transaction.
pub fn validate_arguments_for_profile(
    tool_name: &str,
    arguments: &Value,
    limits: &RuntimeLimits,
    profile: Option<&ExecutionProfile>,
) -> Result<(), String> {
    let profile = profile.ok_or("collaboration profile is not configured")?;
    if identity_administration::handles(tool_name) {
        if !matches!(profile, ExecutionProfile::CollaborationV2 { .. }) { return Err("identity/task tools require V2".into()); }
        return identity_administration::validate(tool_name, arguments, limits, limits.execution.collaboration.capabilities.as_ref());
    }
    if workflow_tool(tool_name) {
        if !matches!(profile, ExecutionProfile::CollaborationV2 { .. }) || limits.workflow.is_none()
        {
            return Err("collaboration workflow tools are not configured".into());
        }
        if questions::handles(tool_name) {
            return questions::validate_arguments(tool_name, arguments, limits);
        }
        if documents_reviews::owns(tool_name) {
            return documents_reviews::validate_arguments(tool_name, arguments, limits);
        }
        return work::validate_arguments(tool_name, arguments, limits);
    }
    validate_arguments(tool_name, arguments, limits)
}

fn repository_query(tool: &str) -> bool {
    matches!(
        tool,
        "collab_repository_tree"
            | "collab_repository_read"
            | "collab_repository_search"
            | "collab_repository_diff"
            | "collab_repository_history"
    )
}

// The catalogue and its refusal messages use the same field-to-bound mapping.
fn string_limit(name: &str, repository: bool, limits: &RuntimeLimits) -> (&'static str, usize) {
    match name {
        "path" | "after" if repository => ("repository.path_bytes", limits.repository.path_bytes),
        "needle" if repository => ("repository.needle_bytes", limits.repository.needle_bytes),
        "cursor" => (
            "controller.collaboration_cursor_bytes",
            limits.controller.collaboration_cursor_bytes,
        ),
        "text" => (
            "controller.collaboration_progress_bytes",
            limits.controller.collaboration_progress_bytes,
        ),
        "instruction" => (
            "controller.collaboration_child_instruction_bytes",
            limits.controller.collaboration_child_instruction_bytes,
        ),
        _ => ("transport.identity_bytes", limits.transport.identity_bytes),
    }
}

fn integer_limit(
    name: &str,
    repository: bool,
    limits: &RuntimeLimits,
) -> Option<(&'static str, usize)> {
    match name {
        "limit" if repository => Some((
            "repository.entries_per_page",
            limits.repository.entries_per_page,
        )),
        "limit" => Some((
            "controller.collaboration_page_items",
            limits.controller.collaboration_page_items,
        )),
        "start_line" => Some((
            "repository.start_line_max",
            limits.repository.start_line_max,
        )),
        "max_lines" => Some((
            "repository.lines_per_page",
            limits.repository.lines_per_page,
        )),
        _ => None,
    }
}

pub fn valid_identity(value: &str, identity_bytes: usize) -> bool {
    !value.is_empty() && value.len() <= identity_bytes && !value.chars().any(char::is_control)
}

/// The HTTP adapter and durable controller call this same validation path.
/// The model-visible schema and actual acceptance consume identical metadata.
// Traceability scope: Policy maxima and UTF-8 byte limits in the closed MCP catalogue.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//# An operation refused by a configured limit MUST identify the limiting metadata field.
pub fn validate_arguments(
    tool_name: &str,
    arguments: &Value,
    limits: &RuntimeLimits,
) -> Result<(), String> {
    let tool = catalogue(limits)
        .into_iter()
        .find(|t| t.name == tool_name)
        .ok_or("tool is outside the collaboration catalogue")?;
    validate_tool_arguments(tool_name, arguments, limits, &tool)
}
fn validate_tool_arguments(
    tool_name: &str,
    arguments: &Value,
    limits: &RuntimeLimits,
    tool: &Tool,
) -> Result<(), String> {
    let validator = jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .build(&tool.input_schema)
        .map_err(|_| "invalid metadata-bound tool schema")?;
    let repository = repository_query(tool_name);
    let mut limiting_field: Option<&str> = None;
    for violation in validator.iter_errors(arguments) {
        // Malformed arguments are not falsely labelled as configurable policy
        // refusals. Representation bounds, such as revision width, stay distinct.
        let field = violation
            .instance_path()
            .as_str()
            .strip_prefix('/')
            .filter(|name| !name.contains('/'))
            .and_then(|name| match violation.kind() {
                jsonschema::error::ValidationErrorKind::MaxLength { .. }
                    if tool.input_schema["properties"][name]["x-borg-maxUtf8Bytes"].is_u64() =>
                {
                    Some(string_limit(name, repository, limits).0)
                }
                jsonschema::error::ValidationErrorKind::Maximum { .. } => {
                    integer_limit(name, repository, limits).map(|(field, _)| field)
                }
                _ => None,
            });
        let Some(field) = field else {
            return Err("tool arguments differ from configured schema".into());
        };
        limiting_field = Some(limiting_field.map_or(field, |prior| prior.min(field)));
    }
    if let Some(field) = limiting_field {
        return Err(format!("Configured limit exceeded: {field}"));
    }
    let args = arguments
        .as_object()
        .ok_or("tool arguments must be an object")?;
    for (name, value) in args {
        if let Some(value) = value.as_str() {
            if tool.input_schema["properties"][name]["x-borg-maxUtf8Bytes"]
                .as_u64()
                .is_some_and(|bound| value.len() as u64 > bound)
            {
                let (field, _) = string_limit(name, repository, limits);
                return Err(format!("Configured limit exceeded: {field}"));
            }
            // Identify only fields whose closed tool contract names an
            // identity. In particular, memory `value` is arbitrary JSON;
            // its string contents are data, including empty/control text.
            let identity_field = matches!(
                name.as_str(),
                "task_id"
                    | "agent_id"
                    | "parent_task_id"
                    | "definition_id"
                    | "revision"
                    | "expected_revision"
                    | "expected_global_revision"
                    | "expected_policy_revision"
                    | "key"
                    | "worker_id"
                    | "release_id"
                    | "call_id"
                    | "session_id"
                    | "repository_id"
                    | "registration_revision"
            ) || (tool_name == "collab_repositories_list" && name == "after");
            if identity_field && !valid_identity(value, limits.transport.identity_bytes) {
                return Err("invalid tool argument identity".into());
            }
        }
    }
    Ok(())
}
impl Caller {
    pub fn validate(&self, limits: &TransportLimits) -> Result<(), String> {
        if [
            &self.task_id,
            &self.attempt_id,
            &self.worker_id,
            &self.incarnation,
        ]
        .into_iter()
        .all(|s| valid_identity(s, limits.identity_bytes))
            && self.generation > 0
            && self.generation <= i64::MAX as u64
        {
            Ok(())
        } else {
            Err("invalid collaboration caller".into())
        }
    }
}
impl Invocation {
    pub fn validate(&self, limits: &RuntimeLimits) -> Result<(), String> {
        self.caller.validate(&limits.transport)?;
        if !valid_identity(&self.operation_id, limits.transport.identity_bytes)
            || !valid_identity(&self.logical_call_id, limits.transport.identity_bytes)
            || !valid_identity(&self.tool, limits.transport.identity_bytes)
            || !self.arguments.is_object()
            || crate::protocol::bounded_json_size(self, limits.interface.mcp_call_bytes).is_err()
        {
            return Err("invalid bounded collaboration invocation".into());
        }
        Ok(())
    }
    pub fn operation(&self) -> Operation {
        Operation::CallCollaboration { call: self.clone() }
    }
}
/// Exact original self-assignment only; coordinating another task does not stop
/// this caller. A same-agent no-op has no pending assignment and is not a handoff.
pub(crate) fn assignment_quiescence(call: &Invocation, outcome: &Outcome) -> bool {
    matches!(outcome,Outcome::Ok{data:Data::Task(task)} if call.tool=="collab_task_assign"
        &&task.id==call.caller.task_id
        &&call.arguments.get("task_id").and_then(Value::as_str)==Some(task.id.as_str())
        &&task.attempt.as_ref().is_some_and(|a|a.id==call.caller.attempt_id&&a.generation==call.caller.generation
            &&a.worker_id==call.caller.worker_id&&a.worker_incarnation==call.caller.incarnation&&a.session_id==task.session_id)
        &&task.pending_assignment.as_ref().is_some_and(|pending|pending.operation_id==call.operation_id
            &&pending.from_session_id==task.session_id&&pending.session.task_id==task.id
            &&pending.session.id!=task.session_id&&pending.session.agent_id==pending.session.definition.id
            &&call.arguments.get("agent_id").and_then(Value::as_str)==Some(pending.session.agent_id.as_str())))
}
fn requires_quiescence(call: &Invocation, outcome: &Outcome) -> bool {
    (call.tool == "collab_task_delegate" && matches!(outcome, Outcome::Ok { .. }))
        || assignment_quiescence(call, outcome)
        || work::requires_quiescence(call, outcome)
}
pub fn result(call: &Invocation, reply: Reply) -> Result<CallResult, String> {
    reply.validate_for(&call.operation())?;
    Ok(CallResult {
        requires_quiescence: requires_quiescence(call, &reply.outcome),
        reply,
    })
}
/// Recover the original outcome only after checking its exact host occurrence.
pub fn recovered(call: &Invocation, lookup: Reply) -> Result<CallResult, String> {
    let op = Operation::GetCollaborationCall {
        caller: call.caller.clone(),
        operation_id: call.operation_id.clone(),
    };
    lookup.validate_for(&op)?;
    let Outcome::Ok {
        data: Data::Operation(record),
    } = &lookup.outcome
    else {
        return Err("collaboration occurrence has no committed receipt".into());
    };
    if record.intended
        != serde_json::to_value(call.operation()).map_err(|_| "invalid invocation")?
    {
        return Err("collaboration occurrence differs from original input".into());
    }
    let outcome: Outcome =
        serde_json::from_value(record.outcome.clone().ok_or("missing original outcome")?)
            .map_err(|_| "invalid original outcome")?;
    result(
        call,
        Reply {
            request_id: lookup.request_id,
            commit: lookup.commit,
            observed_controller_revision: lookup.observed_controller_revision,
            outcome,
        },
    )
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Rpc {
    jsonrpc: String,
    #[serde(default)]
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CallParams {
    name: String,
    arguments: Value,
    #[serde(rename = "_meta")]
    meta: Value,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Occurrence {
    pub operation_id: String,
    pub logical_call_id: String,
}
fn error(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
}
fn success(id: Value, value: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"result":value})
}
fn tool_result(id: Value, value: &CallResult) -> Value {
    success(
        id,
        json!({"content":[{"type":"text","text":serde_json::to_string(value).expect("typed tool reply")}],"isError":!matches!(value.reply.outcome,Outcome::Ok{..})}),
    )
}
/// Read the same canonical projection used by operators. Selecting the task's
/// stored policy is observation only; the call transaction still checks the
/// complete current caller and performs the normal durable receipt handling.
async fn caller_limits(
    controller: &Controller,
    caller: &Caller,
) -> Result<Box<RuntimeLimits>, String> {
    let command = Operation::QueryCollaboration {
        query: crate::collaboration::Query::Limits {
            task_id: Some(caller.task_id.clone()),
        },
    };
    let reply = controller
        .handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: "mcp-limits".into(),
            command: command.clone(),
        })
        .await;
    reply.validate_for(&command)?;
    match reply.outcome {
        Outcome::Ok {
            data:
                Data::Collaboration(crate::collaboration::QueryResult::Limits {
                    execution: Some(snapshot),
                    ..
                }),
        } => {
            snapshot.global_metadata.validate()?;
            let mut limits = snapshot.global_metadata;
            limits.execution.collaboration.capabilities = snapshot.values.collaboration.capabilities;
            Ok(limits)
        }
        _ => Err("accepted caller limits are unavailable".into()),
    }
}
/// Obtain the immutable accepted task profile, never a caller-supplied kind.
/// This observation does not replace the call transaction's currentness checks.
async fn caller_profile(
    controller: &Controller,
    caller: &Caller,
) -> Result<(ExecutionProfile, Option<RepositoryReadSource>), String> {
    let command = Operation::GetTask {
        task_id: caller.task_id.clone(),
    };
    let reply = controller
        .handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: "mcp-profile".into(),
            command: command.clone(),
        })
        .await;
    reply.validate_for(&command)?;
    let Outcome::Ok {
        data: Data::Task(task),
    } = reply.outcome
    else {
        return Err("accepted caller profile is unavailable".into());
    };
    let attempt = task
        .attempt
        .as_ref()
        .ok_or("accepted caller attempt is unavailable")?;
    if task.id != caller.task_id
        || attempt.id != caller.attempt_id
        || attempt.generation != caller.generation
        || attempt.worker_id != caller.worker_id
        || attempt.worker_incarnation != caller.incarnation
    {
        return Err("accepted caller attempt differs".into());
    }
    let profile = task
        .input
        .definition
        .execution_profile
        .ok_or("collaboration profile is not configured")?;
    let source = (task.input.definition.executor == ExecutorKind::Coding)
        .then_some(task.input.instruction_source)
        .flatten();
    Ok((profile, source))
}

/// Existing HTTP adapter owns body/connection deadlines and host/origin checks.
/// This function never retries a controller operation or starts provider work.
pub async fn handle(
    controller: &Controller,
    caller_header: Option<&str>,
    version: Option<&str>,
    body: &[u8],
) -> (u16, Vec<u8>) {
    let response = exchange(controller, caller_header, version, body).await;
    match response {
        None => (202, vec![]),
        Some(value) => match serde_json::to_vec(&value) {
            Ok(bytes) if bytes.len() <= controller.limits().transport.body_bytes => (200, bytes),
            _ => (
                500,
                b"MCP response bound; reconcile the original occurrence".to_vec(),
            ),
        },
    }
}
async fn exchange(
    controller: &Controller,
    caller_header: Option<&str>,
    version: Option<&str>,
    body: &[u8],
) -> Option<Value> {
    let limits = controller.limits();
    let fail = |id, code, msg| Some(error(id, code, msg));
    let value = match strict::parse(body, limits.transport.body_bytes) {
        Ok(v) => v,
        Err(_) => return fail(Value::Null, -32700, "Invalid or duplicate JSON"),
    };
    let rpc: Rpc = match serde_json::from_value(value) {
        Ok(v) => v,
        Err(_) => return fail(Value::Null, -32600, "Invalid MCP request"),
    };
    let id = rpc.id.clone().unwrap_or(Value::Null);
    if rpc.jsonrpc != "2.0"
        || rpc
            .id
            .as_ref()
            .is_some_and(|v| !v.is_string() && !v.is_i64())
        || id
            .as_str()
            .is_some_and(|s| !valid_identity(s, limits.transport.identity_bytes))
    {
        return fail(Value::Null, -32600, "Invalid MCP correlation");
    }
    let caller: Caller = match caller_header
        .filter(|s| s.len() <= limits.interface.mcp_caller_header_bytes)
        .and_then(|s| strict::parse(s.as_bytes(), limits.interface.mcp_caller_header_bytes).ok())
        .and_then(|v| serde_json::from_value::<Caller>(v).ok())
    {
        Some(c) if c.validate(&limits.transport).is_ok() => c,
        _ => return fail(id, -32602, "Missing or invalid host caller"),
    };
    if rpc.method != "initialize" && version != Some(VERSION) {
        return fail(id, -32600, "Unsupported MCP protocol version");
    }
    if rpc.method == "notifications/initialized" {
        return if rpc.id.is_none() && (rpc.params.is_null() || rpc.params == json!({})) {
            None
        } else {
            fail(id, -32600, "Invalid initialized notification")
        };
    }
    if rpc.id.is_none() {
        return fail(id, -32600, "Request identity required");
    }
    match rpc.method.as_str() {
        "initialize" => {
            let Some(params) = rpc.params.as_object() else {
                return fail(id, -32602, "Invalid initialize parameters");
            };
            if params.get("protocolVersion").and_then(Value::as_str) != Some(VERSION)
                || !params.get("capabilities").is_some_and(Value::is_object)
                || !params.get("clientInfo").is_some_and(Value::is_object)
                || params.keys().any(|k| {
                    !["protocolVersion", "capabilities", "clientInfo"].contains(&k.as_str())
                })
            {
                return fail(id, -32602, "Unsupported initialize parameters");
            }
            Some(success(
                id,
                json!({"protocolVersion":VERSION,"capabilities":{"tools":{"listChanged":false}},"serverInfo":{"name":"borg-collaboration","version":"1"}}),
            ))
        }
        "tools/list" => {
            if !(rpc.params.is_null() || rpc.params == json!({})) {
                return fail(id, -32602, "Tool catalogue has no continuation");
            }
            let accepted = match caller_limits(controller, &caller).await {
                Ok(limits) => limits,
                Err(_) => return fail(id, -32602, "Accepted caller limits are unavailable"),
            };
            let (profile, instruction_source) = match caller_profile(controller, &caller).await {
                Ok(profile) => profile,
                Err(_) => return fail(id, -32602, "Accepted caller profile is unavailable"),
            };
            Some(success(
                id,
                json!({"tools":catalogue_for_task(&accepted, Some(&profile), instruction_source.as_ref())}),
            ))
        }
        "borg/calls/get" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Get {
                call_id: String,
            }
            let arg: Get = match serde_json::from_value(rpc.params) {
                Ok(v) => v,
                Err(_) => return fail(id, -32602, "Invalid call lookup"),
            };
            let command = Operation::GetCollaborationCall {
                caller,
                operation_id: arg.call_id,
            };
            let reply = controller
                .handle(Request {
                    protocol_revision: WIRE_REVISION,
                    request_id: "mcp-lookup".into(),
                    command,
                })
                .await;
            Some(tool_result(
                id,
                &CallResult {
                    reply,
                    requires_quiescence: false,
                },
            ))
        }
        "tools/call" => {
            let params: CallParams = match serde_json::from_value(rpc.params) {
                Ok(v) => v,
                Err(_) => return fail(id, -32602, "Invalid tool parameters"),
            };
            let occurrence: Occurrence = match params
                .meta
                .get("borg")
                .cloned()
                .and_then(|v| serde_json::from_value(v).ok())
            {
                Some(v) => v,
                None => return fail(id, -32602, "Host occurrence is required"),
            };
            let accepted = match caller_limits(controller, &caller).await {
                Ok(limits) => limits,
                Err(_) => return fail(id, -32602, "Accepted caller limits are unavailable"),
            };
            let call = Invocation {
                operation_id: occurrence.operation_id,
                logical_call_id: occurrence.logical_call_id,
                caller,
                tool: params.name,
                arguments: params.arguments,
            };
            if call.validate(&accepted).is_err() {
                return fail(id, -32602, "Invalid bounded host invocation");
            }
            let reply = controller
                .handle(Request {
                    protocol_revision: WIRE_REVISION,
                    request_id: "mcp-call".into(),
                    command: call.operation(),
                })
                .await;
            Some(tool_result(
                id,
                &CallResult {
                    requires_quiescence: requires_quiescence(&call, &reply.outcome),
                    reply,
                },
            ))
        }
        _ => fail(id, -32601, "Unknown MCP method"),
    }
}

#[cfg(test)]
mod metadata_tests {
    use super::*;

    fn schema(limits: &RuntimeLimits, tool: &str) -> Value {
        catalogue(limits)
            .into_iter()
            .find(|t| t.name == tool)
            .unwrap()
            .input_schema
    }

    #[test]
    //= spec/impl/v2/implementation.md#limits-are-versioned-metadata
    //= type=test
    //# An operation refused by a configured limit MUST identify the limiting metadata field.
    fn numeric_policy_refusals_identify_exact_metadata_fields() {
        let mut limits = RuntimeLimits::seed().unwrap();
        limits.controller.collaboration_page_items = 2;
        limits.repository.entries_per_page = 2;
        limits.repository.start_line_max = 2;
        limits.repository.lines_per_page = 2;
        for (tool, mut args, argument, field) in [
            (
                "collab_tasks_list",
                json!({"cursor":null,"limit":2,"agent_id":null,"parent_task_id":null}),
                "limit",
                "controller.collaboration_page_items",
            ),
            (
                "collab_repository_history",
                json!({"source":"start","limit":2}),
                "limit",
                "repository.entries_per_page",
            ),
            (
                "collab_repository_read",
                json!({"source":"start","path":"a","start_line":2,"max_lines":1}),
                "start_line",
                "repository.start_line_max",
            ),
            (
                "collab_repository_read",
                json!({"source":"start","path":"a","start_line":1,"max_lines":2}),
                "max_lines",
                "repository.lines_per_page",
            ),
        ] {
            assert!(validate_arguments(tool, &args, &limits).is_ok(), "{tool}");
            args[argument] = json!(3);
            assert_eq!(
                validate_arguments(tool, &args, &limits).unwrap_err(),
                format!("Configured limit exceeded: {field}"),
                "{tool}: {argument}"
            );
        }
    }

    #[test]
    //= spec/impl/v2/implementation.md#limits-are-versioned-metadata
    //= type=test
    //# An operation refused by a configured limit MUST identify the limiting metadata field.
    fn string_policy_refusals_cover_character_and_utf8_byte_bounds() {
        let mut limits = RuntimeLimits::seed().unwrap();
        limits.transport.identity_bytes = 2;
        limits.repository.path_bytes = 2;
        limits.repository.needle_bytes = 2;
        limits.controller.collaboration_cursor_bytes = 2;
        limits.controller.collaboration_progress_bytes = 2;
        limits.controller.collaboration_child_instruction_bytes = 2;
        for (tool, mut args, argument, field) in [
            (
                "collab_task_get",
                json!({"task_id":"aa"}),
                "task_id",
                "transport.identity_bytes",
            ),
            (
                "collab_repository_read",
                json!({"source":"start","path":"aa","start_line":1,"max_lines":1}),
                "path",
                "repository.path_bytes",
            ),
            (
                "collab_repository_tree",
                json!({"source":"start","path":"","after":"aa","limit":1}),
                "after",
                "repository.path_bytes",
            ),
            (
                "collab_repository_search",
                json!({"source":"start","path":"","needle":"aa"}),
                "needle",
                "repository.needle_bytes",
            ),
            (
                "collab_tasks_list",
                json!({"cursor":"aa","limit":1,"agent_id":null,"parent_task_id":null}),
                "cursor",
                "controller.collaboration_cursor_bytes",
            ),
            (
                "collab_task_progress",
                json!({"text":"aa"}),
                "text",
                "controller.collaboration_progress_bytes",
            ),
            (
                "collab_task_delegate",
                json!({"instruction":"aa"}),
                "instruction",
                "controller.collaboration_child_instruction_bytes",
            ),
        ] {
            for accepted in ["aa", "é"] {
                args[argument] = json!(accepted);
                assert!(
                    validate_arguments(tool, &args, &limits).is_ok(),
                    "{tool}: {accepted}"
                );
            }
            // ASCII exceeds schema maxLength; multibyte text passes maxLength
            // but exceeds the encoded-byte limit used by durable records.
            for refused in ["aaa", "éa"] {
                args[argument] = json!(refused);
                assert_eq!(
                    validate_arguments(tool, &args, &limits).unwrap_err(),
                    format!("Configured limit exceeded: {field}"),
                    "{tool}: {refused}"
                );
            }
        }
    }

    #[test]
    fn malformed_arguments_and_fixed_widths_are_not_reported_as_policy_limits() {
        let mut limits = RuntimeLimits::seed().unwrap();
        limits.transport.identity_bytes = 2;
        for args in [
            json!({"task_id":7}),
            json!({"task_id":""}),
            json!({"task_id":"aaa","unknown":1}),
            json!({}),
        ] {
            assert_eq!(
                validate_arguments("collab_task_get", &args, &limits).unwrap_err(),
                "tool arguments differ from configured schema"
            );
        }
        assert_eq!(
            validate_arguments("collab_task_get", &json!({"task_id":"a\n"}), &limits).unwrap_err(),
            "invalid tool argument identity"
        );
        assert_eq!(
            validate_arguments(
                "collab_memory_set",
                &json!({"key":"a","expected_revision":u64::MAX,"value":0}),
                &limits
            )
            .unwrap_err(),
            "tool arguments differ from configured schema"
        );
    }

    #[test]
    fn multiple_policy_refusals_choose_a_stable_limiting_field() {
        let mut limits = RuntimeLimits::seed().unwrap();
        limits.controller.collaboration_page_items = 1;
        limits.controller.collaboration_cursor_bytes = 1;
        let args = json!({"cursor":"aa","limit":2,"agent_id":null,"parent_task_id":null});
        assert_eq!(
            validate_arguments("collab_tasks_list", &args, &limits).unwrap_err(),
            "Configured limit exceeded: controller.collaboration_cursor_bytes"
        );
    }

    #[test]
    fn catalogue_and_validation_follow_changed_page_metadata_in_both_directions() {
        let mut limits = RuntimeLimits::seed().unwrap();
        let args = json!({"cursor":null,"limit":2,"agent_id":null,"parent_task_id":null});
        limits.controller.collaboration_page_items = 1;
        assert_eq!(
            schema(&limits, "collab_tasks_list")["properties"]["limit"]["maximum"],
            1
        );
        assert!(validate_arguments("collab_tasks_list", &args, &limits).is_err());
        limits.controller.collaboration_page_items = 137;
        let args = json!({"cursor":null,"limit":137,"agent_id":null,"parent_task_id":null});
        assert_eq!(
            schema(&limits, "collab_tasks_list")["properties"]["limit"]["maximum"],
            137
        );
        assert!(validate_arguments("collab_tasks_list", &args, &limits).is_ok());
    }

    #[test]
    fn repository_schema_and_byte_validation_share_selected_metadata() {
        let mut limits = RuntimeLimits::seed().unwrap();
        limits.repository.lines_per_page = 3;
        limits.repository.start_line_max = 11;
        limits.repository.path_bytes = 5;
        let read = schema(&limits, "collab_repository_read");
        assert_eq!(read["properties"]["max_lines"]["maximum"], 3);
        assert_eq!(read["properties"]["start_line"]["maximum"], 11);
        assert_eq!(read["properties"]["path"]["x-borg-maxUtf8Bytes"], 5);
        let args = json!({"source":"start","path":"ééé","start_line":1,"max_lines":1});
        assert!(validate_arguments("collab_repository_read", &args, &limits).is_err());
        limits.repository.path_bytes = 6;
        assert!(validate_arguments("collab_repository_read", &args, &limits).is_ok());
        limits.transport.identity_bytes = 300;
        let args = json!({"task_id":"x".repeat(300)});
        assert!(validate_arguments("collab_task_get", &args, &limits).is_ok());
        limits.transport.identity_bytes = 299;
        assert!(validate_arguments("collab_task_get", &args, &limits).is_err());
    }

    #[test]
    // Traceability scope: Selected metadata drives catalogue schema/argument checks; registration mutation tool is rejected; no actual model calls.
    //= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
    //= type=test
    //# The shared agent interface MUST provide bounded repository-catalogue list and get operations.
    //= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
    //= type=test
    //# Agent tools MUST NOT mutate repository registrations.
    fn registration_discovery_is_read_only_and_uses_catalogue_identity_and_page_bounds() {
        let mut limits = RuntimeLimits::seed().unwrap();
        limits.controller.collaboration_page_items = 2;
        limits.repository.entries_per_page = 1;
        limits.repository.path_bytes = 1;
        limits.transport.identity_bytes = 6;
        let list = json!({"after":"répo", "limit":2});
        assert!(validate_arguments("collab_repositories_list", &list, &limits).is_ok());
        assert_eq!(
            schema(&limits, "collab_repositories_list")["properties"]["limit"]["maximum"],
            2
        );
        assert!(
            validate_arguments(
                "collab_repository_history",
                &json!({"source":"start","limit":2}),
                &limits
            )
            .is_err()
        );
        let get = json!({"repository_id":"répo", "registration_revision":"ééé"});
        assert!(validate_arguments("collab_repository_get", &get, &limits).is_ok());
        limits.transport.identity_bytes = 5;
        assert!(validate_arguments("collab_repository_get", &get, &limits).is_err());
        for after in ["", "bad\n"] {
            assert!(
                validate_arguments(
                    "collab_repositories_list",
                    &json!({"after":after,"limit":1}),
                    &limits
                )
                .is_err()
            );
        }
        assert!(validate_arguments("collab_repository_get", &json!({"repository_id":"repo","registration_revision":null,"source_store_key":"private"}), &limits).is_err());
        assert!(validate_arguments("collab_repository_register", &json!({}), &limits).is_err());
        for name in ["collab_repositories_list", "collab_repository_get"] {
            let tool = catalogue(&limits)
                .into_iter()
                .find(|tool| tool.name == name)
                .unwrap();
            assert_eq!(tool.annotations["readOnlyHint"], true);
        }
    }

    #[test]
    fn effective_limits_tool_cannot_select_another_task_or_change_policy() {
        let limits = RuntimeLimits::seed().unwrap();
        assert!(validate_arguments("collab_limits_get", &json!({}), &limits).is_ok());
        assert!(
            validate_arguments("collab_limits_get", &json!({"task_id":"other"}), &limits).is_err()
        );
        assert!(validate_arguments("collab_limits_get", &json!({"limits":{}}), &limits).is_err());
    }

    #[test]
    fn memory_json_strings_are_data_while_memory_keys_remain_bounded_identities() {
        let limits = RuntimeLimits::seed().unwrap();
        let identity_bytes = limits.transport.identity_bytes;
        for value in [
            json!("x".repeat(8192)),
            json!(""),
            json!("line\n\u{0000}\tend"),
        ] {
            let arguments = json!({"key":"memory","expected_revision":null,"value":value});
            assert!(validate_arguments("collab_memory_set", &arguments, &limits).is_ok());
            assert!(bounded_json_size(&value, limits.controller.memory_value_bytes).is_ok());
        }
        for key in ["".into(), "k".repeat(identity_bytes + 1), "bad\nkey".into()] {
            let arguments = json!({"key":key,"expected_revision":null,"value":"valid data"});
            assert!(validate_arguments("collab_memory_set", &arguments, &limits).is_err());
        }
        assert!(
            validate_arguments(
                "collab_task_get",
                &json!({"task_id":"t".repeat(identity_bytes + 1)}),
                &limits
            )
            .is_err()
        );
    }

    #[test]
    fn memory_data_still_consumes_the_complete_invocation_byte_budget() {
        let mut limits = RuntimeLimits::seed().unwrap();
        let call = Invocation {
            operation_id: "memory-operation".into(),
            logical_call_id: "memory-logical-call".into(),
            caller: Caller {
                task_id: "task".into(),
                attempt_id: "attempt".into(),
                generation: 1,
                worker_id: "worker".into(),
                incarnation: "incarnation".into(),
            },
            tool: "collab_memory_set".into(),
            arguments: json!({"key":"memory","expected_revision":null,"value":"x".repeat(8192)}),
        };
        assert!(validate_arguments(&call.tool, &call.arguments, &limits).is_ok());
        let bytes = serde_json::to_vec(&call).unwrap().len();
        limits.interface.mcp_call_bytes = bytes;
        assert!(call.validate(&limits).is_ok());
        limits.interface.mcp_call_bytes = bytes - 1;
        assert!(call.validate(&limits).is_err());
    }
}

#[cfg(test)]
mod definition_update_schema_tests {
    use super::*;
    #[test]
    fn definition_updates_schema_has_closed_positive_fields_and_string_revision() {
        let limits = RuntimeLimits::seed().unwrap();
        let args = json!({"agent_id":"a","expected_revision":"definition-1","expected_global_revision":limits.revision,"expected_policy_revision":"policy-1","changes":{"native.context_bytes":16777217}});
        assert!(validate_arguments("collab_definition_update", &args, &limits).is_ok());
        let tool = catalogue(&limits)
            .into_iter()
            .find(|x| x.name == "collab_definition_update")
            .unwrap();
        assert!(
            tool.input_schema["properties"]["expected_revision"]
                .get("maximum")
                .is_none()
        );
        assert_eq!(tool.annotations["readOnlyHint"], false);
        for changes in [
            json!({}),
            json!({"native.context_bytes":0}),
            json!({"native.context_bytes":-1}),
            json!({"native.context_bytes":1.5}),
            json!({"worker.max_total_capacity":100}),
            json!({"native.context_bytes":1,"model":"other"}),
        ] {
            let mut bad = args.clone();
            bad["changes"] = changes;
            assert!(validate_arguments("collab_definition_update", &bad, &limits).is_err());
        }
        for field in [
            "expected_revision",
            "expected_global_revision",
            "expected_policy_revision",
        ] {
            let mut bad = args.clone();
            bad[field] = json!("bad\nidentity");
            assert!(validate_arguments("collab_definition_update", &bad, &limits).is_err());
        }
    }
}
