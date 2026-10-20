//! Model-callable native lifecycle tools. The URL binds the caller session;
//! caller identity and tool grants are reloaded from metadata. Optional target
//! identities require an explicit metadata permission; they never impersonate
//! the bound caller.
//! In Board mode creates go through Fleet's atomic placement broker. Mutating
//! requests are transmitted once: a lost reply is uncertainty, not permission
//! to retry a side effect. `session_get` supplies reconciliation evidence.
use super::{ApiError, App, MAX_BODY_BYTES, drive, nonempty, runtime_error};
use axum::{
    Json,
    extract::{Path, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use native_runtime::{McpServerConfig, Session, SessionOptions};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};

const PROTOCOLS: &[&str] = &["2025-03-26", "2025-06-18", "2025-11-25"];
const MAX_REMOTE_BYTES: usize = 16 * 1024 * 1024;

fn schema(properties: Value, required: &[&str]) -> Value {
    json!({"type":"object", "properties":properties, "required":required, "additionalProperties":false})
}

pub(super) fn catalog() -> Vec<Value> {
    let id = json!({"type":"string", "minLength":1, "maxLength":256,
        "description":"The independent native session ID, not an agent ID or Fleet dispatch ID."});
    vec![
        json!({"name":"session_create",
            "description":"Create another independent concurrent session. Defaults to your own identity; other targets require metadata allowed_agent_ids. In Board deployments Fleet places it on a fresh host with capacity and provisions a distinct workspace. The response may be queued; poll session_get. request_id is an optional stable intent key; otherwise the runtime tool-operation ID is used. Never invent a new key to retry an uncertain create.",
            "inputSchema":schema(json!({
                "input":{"type":"string","minLength":1},
                "agent_id":{"type":"string","minLength":1,"maxLength":256,"description":"Target identity; defaults to your own. A different identity requires allowed_agent_ids in your native tool metadata."},
                "task_id":{"type":"integer","minimum":1,"description":"Optional existing Task Board task association; this does not create, reassign or complete a task."},
                "request_id":{"type":"string","minLength":1,"maxLength":128,"description":"Stable key for this create intent. Reuse with identical arguments to recover its original session."}
            }), &["input"])}),
        json!({"name":"session_list",
            "description":"List an authorized identity's independent sessions and Fleet dispatches across hosts. Agent identity and session lifecycle are separate. Inspect queued/failed dispatches as well as live sessions.",
            "inputSchema":schema(json!({
                "agent_id":{"type":"string","minLength":1,"maxLength":256,"description":"Identity to list; defaults to yours. Cross-identity reads require a metadata grant."},
                "offset":{"type":"integer","minimum":0,"default":0},
                "limit":{"type":"integer","minimum":1,"maximum":500,"default":100}
            }), &[]), "annotations":{"readOnlyHint":true}}),
        json!({"name":"session_get",
            "description":"Read one authorized session, placement and status. Optionally read bounded transcript and original tool calls/results to reconcile an uncertain effect before resuming. A queued Fleet dispatch may not yet have a local runtime session.",
            "inputSchema":schema(json!({"session_id":id,
                "include_transcript":{"type":"boolean","default":false},
                "include_tools":{"type":"boolean","default":false},
                "after_sequence":{"type":"integer","minimum":0,"default":0},
                "limit":{"type":"integer","minimum":1,"maximum":1000,"default":100}
            }), &["session_id"]), "annotations":{"readOnlyHint":true}}),
        json!({"name":"session_steer",
            "description":"Durably append input to another authorized session and wake it when appropriate. This is a new intent, not a retry of an unknown tool effect. Stopped sessions remain stopped until resumed.",
            "inputSchema":schema(json!({"session_id":id,"text":{"type":"string","minLength":1}}), &["session_id","text"])}),
        json!({"name":"session_stop",
            "description":"Request a safe stop for a authorized session. By default drain the active tool result before stopping at a turn boundary. cancel=true cancels immediately and may leave a tool outcome unknown; never use immediate cancellation on your own active tool call.",
            "inputSchema":schema(json!({"session_id":id,"cancel":{"type":"boolean","default":false}}), &["session_id"])}),
        json!({"name":"session_resume",
            "description":"Resume a stopped, waiting or failed authorized session from durable transcript and completed tool results. Refuses unresolved tool outcomes; it never replays an unknown effect.",
            "inputSchema":schema(json!({"session_id":id}), &["session_id"])}),
        json!({"name":"session_resolve_tool",
            "description":"Supply the verified ORIGINAL result for one unknown tool call. Inspect session_get(include_tools=true) and external effects first. This records the result without re-executing the tool. Do not fabricate success or request replay. A verified failed MCP tool result uses content plus isError=true.",
            "inputSchema":schema(json!({"session_id":id,"call_id":{"type":"string","minLength":1,"maxLength":256},"result":{}}), &["session_id","call_id","result"])}),
    ]
}

fn rpc_error(id: Value, code: i64, message: &'static str) -> Response {
    Json(json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})).into_response()
}
fn tool_result(value: Value, failed: bool) -> Value {
    json!({"content":[{"type":"text","text":value.to_string()}],"structuredContent":value,"isError":failed})
}
fn arguments<T: DeserializeOwned>(value: Value) -> Result<T, ApiError> {
    if !value.is_object() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "Tool arguments must be an object",
        ));
    }
    serde_json::from_value(value).map_err(|_| {
        ApiError(
            StatusCode::BAD_REQUEST,
            "Unsupported or missing tool arguments",
        )
    })
}
fn allowed(grant: &McpServerConfig, tool: &str) -> bool {
    grant
        .tools_allowlist
        .as_ref()
        .is_none_or(|names| names.iter().any(|name| name == tool))
}
fn can_target(grant: &McpServerConfig, caller: &Session, agent: &str) -> bool {
    caller.agent_id == agent
        || grant
            .allowed_agent_ids
            .iter()
            .any(|allowed| allowed == "*" || allowed == agent)
}
fn target_agent(
    grant: &McpServerConfig,
    caller: &Session,
    requested: Option<String>,
) -> Result<String, ApiError> {
    let agent = requested.unwrap_or_else(|| caller.agent_id.clone());
    nonempty(&agent, 256)?;
    if !can_target(grant, caller, &agent) {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "Target identity is not permitted by lifecycle metadata",
        ));
    }
    Ok(agent)
}
fn valid_url(value: &str) -> Result<reqwest::Url, ApiError> {
    let url = reqwest::Url::parse(value).map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Invalid configured service endpoint",
        )
    })?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Invalid configured service endpoint",
        ));
    }
    Ok(url)
}
fn api_url(base: &str, segments: &[&str]) -> Result<reqwest::Url, ApiError> {
    let mut url = valid_url(base)?;
    let already_api = url.path().trim_end_matches('/').ends_with("/api");
    {
        let mut path = url
            .path_segments_mut()
            .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Invalid service path"))?;
        path.pop_if_empty();
        if !already_api {
            path.push("api");
        }
        path.extend(segments);
    }
    Ok(url)
}

/// Authenticated, stateless Streamable HTTP MCP (JSON response transport).
/// No notification or malformed request is allowed to execute an effect.
pub(super) async fn handle(
    State(app): State<App>,
    Path(caller_id): Path<String>,
    headers: HeaderMap,
    body: Result<Json<Value>, JsonRejection>,
) -> Response {
    // This is a non-browser control endpoint. Disallow ambient browser origins,
    // including DNS-rebinding attempts against unauthenticated loopback installs.
    if app.bearer.is_none() {
        let host = headers
            .get("host")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| reqwest::Url::parse(&format!("http://{value}")).ok());
        let loopback = host
            .as_ref()
            .and_then(|url| url.host_str())
            .is_some_and(|host| {
                host == "localhost"
                    || host
                        .trim_matches(['[', ']'])
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|ip| ip.is_loopback())
            });
        if !loopback {
            return ApiError(
                StatusCode::FORBIDDEN,
                "Loopback control endpoint requires a loopback Host",
            )
            .into_response();
        }
    }
    if headers.contains_key("origin") {
        return ApiError(StatusCode::FORBIDDEN, "Browser origins are not accepted").into_response();
    }
    if let Some(version) = headers.get("mcp-protocol-version")
        && !version
            .to_str()
            .ok()
            .is_some_and(|v| PROTOCOLS.contains(&v))
    {
        return ApiError(StatusCode::BAD_REQUEST, "Unsupported MCP protocol version")
            .into_response();
    }
    let message = match body {
        Ok(Json(value)) => value,
        Err(error) => {
            return (
                error.status(),
                Json(json!({"jsonrpc":"2.0","id":null,
            "error":{"code":-32700,"message":"Invalid JSON request"}})),
            )
                .into_response();
        }
    };
    let id = message.get("id").cloned().unwrap_or(Value::Null);
    if !message.is_object() || message["jsonrpc"] != "2.0" || !message["method"].is_string() {
        return rpc_error(id, -32600, "Invalid Request");
    }
    let method = message["method"].as_str().unwrap();
    if !message.get("params").is_none_or(Value::is_object) {
        return rpc_error(id, -32602, "Invalid params");
    }
    if message.get("id").is_none() {
        return if method.starts_with("notifications/") {
            StatusCode::ACCEPTED.into_response()
        } else {
            rpc_error(Value::Null, -32600, "Request id required")
        };
    }
    if !id.is_string() && !id.is_i64() && !id.is_u64() {
        return rpc_error(Value::Null, -32600, "Invalid request id");
    }
    let rejection = |error: ApiError| {
        if method == "tools/call" {
            Json(json!({"jsonrpc":"2.0","id":id,"result":tool_result(json!({"error":error.1,"http_status":error.0.as_u16()}), true)})).into_response()
        } else {
            error.into_response()
        }
    };
    if let Err(error) = nonempty(&caller_id, 256) {
        return rejection(error);
    }
    let caller = match app.runtime.get_session(&caller_id) {
        Ok(value) => value,
        Err(error) => return rejection(runtime_error(error)),
    };
    let config = match app.runtime.session_agent(&caller_id).await {
        Ok(value) => value,
        Err(error) => return rejection(runtime_error(error)),
    };
    let Some(grant) = config
        .mcp_servers
        .iter()
        .find(|server| server.name == "native")
    else {
        return rejection(ApiError(
            StatusCode::FORBIDDEN,
            "Native lifecycle tools are not granted to this identity",
        ));
    };
    let params = message.get("params").cloned().unwrap_or_else(|| json!({}));
    let result = match method {
        "initialize" => {
            let requested = params["protocolVersion"].as_str().unwrap_or("");
            let version = if PROTOCOLS.contains(&requested) {
                requested
            } else {
                "2025-06-18"
            };
            json!({"protocolVersion":version,"capabilities":{"tools":{"listChanged":false}},
                "serverInfo":{"name":"borg-native-lifecycle","version":env!("CARGO_PKG_VERSION")},
                "instructions":"Caller identity is bound to the native session URL. Metadata controls tool grants. Creates use Fleet in Board mode; poll session_get for provisioning. Inspect original tool results before any uncertain-effect resolution."})
        }
        "ping" => json!({}),
        "tools/list" => {
            if params.get("cursor").is_some_and(|value| !value.is_null()) {
                return rpc_error(id, -32602, "No additional tool pages");
            }
            let tools: Vec<_> = catalog()
                .into_iter()
                .filter(|tool| allowed(grant, tool["name"].as_str().unwrap()))
                .collect();
            json!({"tools":tools})
        }
        "tools/call" => {
            let Some(name) = params["name"].as_str() else {
                return rpc_error(id, -32602, "Tool name required");
            };
            if !allowed(grant, name) || !catalog().iter().any(|tool| tool["name"] == name) {
                tool_result(
                    json!({"error":"Tool is not granted or does not exist","code":"not_permitted"}),
                    true,
                )
            } else {
                let operation = params["_meta"]["borg/operation_id"].as_str();
                match call(&app, &caller, grant, name, params.get("arguments").cloned().unwrap_or_else(|| json!({})), operation).await {
                    Ok(value) => tool_result(value, false),
                    Err(CallError::Known(error)) => tool_result(json!({"error":error.1,"http_status":error.0.as_u16()}), true),
                    // An HTTP failure after dispatch is NOT a known failed tool
                    // result. Let the caller's durable engine mark it unknown.
                    Err(CallError::Uncertain) => return ApiError(StatusCode::BAD_GATEWAY,
                        "Lifecycle outcome is unknown; inspect the original operation before resolving it").into_response(),
                }
            }
        }
        _ => return rpc_error(id, -32601, "Method not found"),
    };
    Json(json!({"jsonrpc":"2.0","id":id,"result":result})).into_response()
}

enum CallError {
    Known(ApiError),
    Uncertain,
}
impl From<ApiError> for CallError {
    fn from(error: ApiError) -> Self {
        Self::Known(error)
    }
}
impl From<anyhow::Error> for CallError {
    fn from(error: anyhow::Error) -> Self {
        Self::Known(runtime_error(error))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Create {
    input: String,
    agent_id: Option<String>,
    task_id: Option<i64>,
    request_id: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct List {
    agent_id: Option<String>,
    #[serde(default)]
    offset: usize,
    #[serde(default = "default_limit")]
    limit: usize,
}
fn default_limit() -> usize {
    100
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Read {
    session_id: String,
    #[serde(default)]
    include_transcript: bool,
    #[serde(default)]
    include_tools: bool,
    #[serde(default)]
    after_sequence: i64,
    #[serde(default = "default_limit")]
    limit: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Target {
    session_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Steer {
    session_id: String,
    text: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Stop {
    session_id: String,
    #[serde(default)]
    cancel: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Resolve {
    session_id: String,
    call_id: String,
    #[serde(deserialize_with = "super::required_value")]
    result: Value,
}

struct Backend {
    board_url: Option<String>,
    board_token: Option<String>,
    runtime_token: Option<String>,
}
impl Backend {
    fn new(app: &App) -> Result<Self, ApiError> {
        let config = app.runtime.config().map_err(runtime_error)?;
        fn token(name: Option<&str>) -> Result<Option<String>, ApiError> {
            name.map(|name| {
                std::env::var(name)
                    .ok()
                    .filter(|v| !v.trim().is_empty())
                    .ok_or(ApiError(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "Configured service authentication is unavailable",
                    ))
            })
            .transpose()
        }
        Ok(Self {
            board_url: config.board.as_ref().map(|board| board.url.clone()),
            board_token: token(
                config
                    .board
                    .as_ref()
                    .and_then(|board| board.bearer_token_env.as_deref()),
            )?,
            runtime_token: token(config.bearer_token_env.as_deref())?,
        })
    }
    async fn dispatches(
        &self,
        app: &App,
        agent: Option<&str>,
        session: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Value>, CallError> {
        let Some(base) = &self.board_url else {
            return Ok(vec![]);
        };
        let mut url = api_url(base, &["fleet", "dispatches"])?;
        url.query_pairs_mut()
            .append_pair("offset", &offset.to_string())
            .append_pair("limit", &limit.to_string());
        if let Some(agent) = agent {
            url.query_pairs_mut().append_pair("agent_id", agent);
        }
        if let Some(session) = session {
            url.query_pairs_mut().append_pair("session_id", session);
        }
        let value = remote(app, url, self.board_token.as_deref(), None).await?;
        let rows = value
            .as_array()
            .or_else(|| value.get("dispatches").and_then(Value::as_array))
            .ok_or(ApiError(
                StatusCode::BAD_GATEWAY,
                "Invalid Fleet dispatch response",
            ))?;
        // Do not trust an optional server-side filter as the authorization check.
        Ok(rows
            .iter()
            .filter(|row| agent.is_none_or(|agent| row["agent_id"] == agent))
            .cloned()
            .collect())
    }
}

async fn remote(
    app: &App,
    url: reqwest::Url,
    token: Option<&str>,
    body: Option<Value>,
) -> Result<Value, CallError> {
    let mut request = if let Some(body) = &body {
        app.http.post(url).json(body)
    } else {
        app.http.get(url)
    };
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let mut response = request.send().await.map_err(|_| {
        if body.is_some() {
            CallError::Uncertain
        } else {
            CallError::Known(ApiError(
                StatusCode::BAD_GATEWAY,
                "Lifecycle service is unreachable",
            ))
        }
    })?;
    let status = response.status();
    if !status.is_success() {
        if body.is_some() && (status.is_server_error() || status.is_redirection()) {
            return Err(CallError::Uncertain);
        }
        return Err(ApiError(StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY), match status.as_u16() {
            404 => "Session or Fleet dispatch not found",
            409 => "Lifecycle conflict; inspect existing session or original request before retrying",
            401 | 403 => "Lifecycle request is not authorized",
            400 | 422 => "Lifecycle service rejected the requested arguments",
            _ => "Lifecycle service is unavailable",
        }).into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| {
        if body.is_some() {
            CallError::Uncertain
        } else {
            CallError::Known(ApiError(
                StatusCode::BAD_GATEWAY,
                "Lifecycle read did not complete",
            ))
        }
    })? {
        if bytes.len().saturating_add(chunk.len()) > MAX_REMOTE_BYTES {
            return Err(if body.is_some() {
                CallError::Uncertain
            } else {
                ApiError(
                    StatusCode::BAD_GATEWAY,
                    "Lifecycle response is too large; request a bounded slice",
                )
                .into()
            });
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| {
        if body.is_some() {
            CallError::Uncertain
        } else {
            ApiError(StatusCode::BAD_GATEWAY, "Lifecycle response is invalid").into()
        }
    })
}

struct Located {
    session: Option<Session>,
    dispatch: Option<Value>,
    remote_url: Option<String>,
}
async fn locate(
    app: &App,
    backend: &Backend,
    caller: &Session,
    grant: &McpServerConfig,
    id: &str,
) -> Result<Located, CallError> {
    nonempty(id, 256)?;
    if let Ok(session) = app.runtime.get_session(id) {
        if !can_target(grant, caller, &session.agent_id) {
            return Err(
                ApiError(StatusCode::NOT_FOUND, "Session not found for this identity").into(),
            );
        }
        return Ok(Located {
            session: Some(session),
            dispatch: None,
            remote_url: None,
        });
    }
    let dispatch = backend
        .dispatches(
            app,
            if grant.allowed_agent_ids.is_empty() {
                Some(caller.agent_id.as_str())
            } else {
                None
            },
            Some(id),
            0,
            2,
        )
        .await?
        .into_iter()
        .find(|row| {
            row["session_id"] == id
                && row["agent_id"]
                    .as_str()
                    .is_some_and(|agent| can_target(grant, caller, agent))
        })
        .ok_or(ApiError(
            StatusCode::NOT_FOUND,
            "Session not found for this identity",
        ))?;
    let base = dispatch["runtime_url"]
        .as_str()
        .ok_or(ApiError(
            StatusCode::BAD_GATEWAY,
            "Fleet dispatch has no runtime endpoint",
        ))?
        .to_owned();
    let session = match remote(
        app,
        api_url(&base, &["sessions", id])?,
        backend.runtime_token.as_deref(),
        None,
    )
    .await
    {
        Ok(value) => {
            let session: Session = serde_json::from_value(value).map_err(|_| {
                ApiError(StatusCode::BAD_GATEWAY, "Invalid runtime session response")
            })?;
            if session.id != id || dispatch["agent_id"] != session.agent_id {
                return Err(ApiError(
                    StatusCode::FORBIDDEN,
                    "Runtime session identity does not match placement",
                )
                .into());
            }
            Some(session)
        }
        Err(CallError::Known(ApiError(StatusCode::NOT_FOUND, _))) => None,
        Err(error) => return Err(error),
    };
    Ok(Located {
        session,
        dispatch: Some(dispatch),
        remote_url: Some(base),
    })
}

async fn call(
    app: &App,
    caller: &Session,
    grant: &McpServerConfig,
    name: &str,
    args: Value,
    operation: Option<&str>,
) -> Result<Value, CallError> {
    let backend = Backend::new(app)?;
    match name {
        "session_create" => {
            let input: Create = arguments(args)?;
            let agent_id = target_agent(grant, caller, input.agent_id)?;
            nonempty(&input.input, MAX_BODY_BYTES)?;
            if input.task_id.is_some_and(|id| id <= 0) {
                return Err(ApiError(StatusCode::BAD_REQUEST, "task_id must be positive").into());
            }
            let key = input.request_id.as_deref().or(operation).ok_or(ApiError(
                StatusCode::BAD_REQUEST,
                "Create requires a stable request_id or runtime operation ID",
            ))?;
            nonempty(key, if input.request_id.is_some() { 128 } else { 512 })?;
            let key = format!("mcp:{}:{key}", caller.id);
            nonempty(&key, 512)?;
            if let Some(base) = &backend.board_url {
                let dispatch = remote(app, api_url(base, &["fleet", "dispatches"])?, backend.board_token.as_deref(),
                    Some(json!({"request_id":key,"agent_id":agent_id,"task_id":input.task_id,"input":input.input,"metadata":{"parent_session_id":caller.id,"request_id":key}}))).await?;
                let dispatch = dispatch.get("dispatch").unwrap_or(&dispatch);
                let id = dispatch["session_id"]
                    .as_str()
                    .ok_or(CallError::Uncertain)?;
                if dispatch["agent_id"] != agent_id {
                    return Err(CallError::Uncertain);
                }
                return Ok(
                    json!({"session_id":id,"agent_id":agent_id,"request_id":key,"parent_session_id":caller.id,"dispatch":dispatch}),
                );
            }
            let id = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, key.as_bytes()).to_string();
            let options = SessionOptions {
                session_id: Some(id),
                metadata: json!({"parent_session_id":caller.id,"request_id":key}),
                ..Default::default()
            };
            let session = app
                .runtime
                .create_session_with_options(
                    &agent_id,
                    input.task_id.map(|id| format!("task_{id}")),
                    &input.input,
                    options,
                )
                .await?;
            drive(&app.runtime, &session);
            Ok(
                json!({"session_id":session.id,"agent_id":session.agent_id,"request_id":key,"parent_session_id":caller.id,"session":session}),
            )
        }
        "session_list" => {
            let input: List = arguments(args)?;
            let agent_id = target_agent(grant, caller, input.agent_id)?;
            if !(1..=500).contains(&input.limit) {
                return Err(
                    ApiError(StatusCode::BAD_REQUEST, "limit must be between 1 and 500").into(),
                );
            }
            let sessions: Vec<_> = app
                .runtime
                .list_sessions()?
                .into_iter()
                .filter(|s| s.agent_id == agent_id)
                .collect();
            let dispatches = backend
                .dispatches(app, Some(&agent_id), None, input.offset, input.limit + 1)
                .await?;
            let more = sessions.len() > input.offset.saturating_add(input.limit)
                || dispatches.len() > input.limit;
            Ok(json!({"agent_id":agent_id,
                "sessions":sessions.into_iter().skip(input.offset).take(input.limit).collect::<Vec<_>>(),
                "dispatches":dispatches.into_iter().take(input.limit).collect::<Vec<_>>(),
                "next_offset":if more { Some(input.offset.saturating_add(input.limit)) } else { None }}))
        }
        "session_get" => {
            let input: Read = arguments(args)?;
            if !(1..=1000).contains(&input.limit) || input.after_sequence < 0 {
                return Err(ApiError(StatusCode::BAD_REQUEST, "Invalid transcript bounds").into());
            }
            let located = locate(app, &backend, caller, grant, &input.session_id).await?;
            let mut result = json!({"session_id":input.session_id,"session":located.session,"dispatch":located.dispatch});
            if located.session.is_some() {
                if input.include_tools {
                    result["tools"] = if let Some(base) = &located.remote_url {
                        remote(
                            app,
                            api_url(base, &["sessions", &input.session_id, "tools"])?,
                            backend.runtime_token.as_deref(),
                            None,
                        )
                        .await?
                    } else {
                        json!(app.runtime.tool_calls(&input.session_id)?)
                    };
                }
                if input.include_transcript {
                    let entries = if let Some(base) = &located.remote_url {
                        let mut url =
                            api_url(base, &["sessions", &input.session_id, "transcript"])?;
                        url.query_pairs_mut()
                            .append_pair("after_sequence", &input.after_sequence.to_string())
                            .append_pair("limit", &input.limit.to_string());
                        remote(app, url, backend.runtime_token.as_deref(), None).await?
                    } else {
                        json!(
                            app.runtime
                                .transcript(&input.session_id)?
                                .into_iter()
                                .filter(|entry| entry.sequence > input.after_sequence)
                                .take(input.limit)
                                .collect::<Vec<_>>()
                        )
                    };
                    result["next_sequence"] = entries
                        .as_array()
                        .and_then(|entries| entries.last())
                        .and_then(|entry| entry.get("sequence"))
                        .cloned()
                        .unwrap_or(json!(input.after_sequence));
                    result["transcript"] = entries;
                }
            }
            Ok(result)
        }
        "session_steer" => {
            let input: Steer = arguments(args)?;
            nonempty(&input.text, MAX_BODY_BYTES)?;
            mutate(
                app,
                &backend,
                caller,
                grant,
                &input.session_id,
                "steer",
                json!({"text":input.text}),
            )
            .await
        }
        "session_stop" => {
            let input: Stop = arguments(args)?;
            if input.cancel && input.session_id == caller.id {
                return Err(ApiError(
                    StatusCode::CONFLICT,
                    "Cannot cancel your own pending tool result; use graceful stop",
                )
                .into());
            }
            mutate(
                app,
                &backend,
                caller,
                grant,
                &input.session_id,
                if input.cancel { "stop" } else { "drain" },
                json!({}),
            )
            .await
        }
        "session_resume" => {
            let input: Target = arguments(args)?;
            mutate(
                app,
                &backend,
                caller,
                grant,
                &input.session_id,
                "resume",
                json!({}),
            )
            .await
        }
        "session_resolve_tool" => {
            let input: Resolve = arguments(args)?;
            nonempty(&input.call_id, 256)?;
            mutate(
                app,
                &backend,
                caller,
                grant,
                &input.session_id,
                "resolve-tool",
                json!({"call_id":input.call_id,"result":input.result}),
            )
            .await
        }
        _ => Err(ApiError(StatusCode::NOT_FOUND, "Unknown lifecycle tool").into()),
    }
}

async fn mutate(
    app: &App,
    backend: &Backend,
    caller: &Session,
    grant: &McpServerConfig,
    id: &str,
    action: &str,
    body: Value,
) -> Result<Value, CallError> {
    let located = locate(app, backend, caller, grant, id).await?;
    if located.session.is_none() {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "Fleet has not provisioned this session; inspect its dispatch before controlling it",
        )
        .into());
    }
    if let Some(base) = &located.remote_url {
        return remote(
            app,
            api_url(base, &["sessions", id, action])?,
            backend.runtime_token.as_deref(),
            Some(body),
        )
        .await;
    }
    let session = match action {
        "steer" => app.runtime.steer(id, body["text"].as_str().unwrap())?,
        "stop" => app.runtime.stop(id)?,
        "drain" => app.runtime.drain_session(id)?,
        "resume" => app.runtime.resume(id)?,
        "resolve-tool" => super::resolve_recorded(
            &app.runtime,
            id,
            body["call_id"].as_str().unwrap(),
            body["result"].clone(),
        )?,
        _ => return Err(ApiError(StatusCode::NOT_FOUND, "Unknown lifecycle operation").into()),
    };
    if !matches!(action, "stop" | "drain") {
        drive(&app.runtime, &session);
    }
    Ok(json!(session))
}
