use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State, rejection::JsonRejection},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use native_runtime::{Runtime, RuntimeConfig, Session, SessionStatus};
use serde::Deserialize;
use serde_json::{Value, json};

const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone)]
struct App {
    runtime: Runtime,
    bearer: Option<Arc<str>>,
}

struct ApiError(StatusCode, &'static str);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error": self.1}))).into_response()
    }
}

fn runtime_error(error: anyhow::Error) -> ApiError {
    let message = error.to_string();
    if message.starts_with("session not found") {
        ApiError(StatusCode::NOT_FOUND, "Session not found")
    } else if message.starts_with("session is already running") {
        ApiError(StatusCode::CONFLICT, "Session is still running")
    } else if message == "unresolved tool calls must be resolved before resume" {
        ApiError(
            StatusCode::CONFLICT,
            "Resolve the original unknown tool result before resuming",
        )
    } else if message == "only an unknown tool call can be explicitly resolved"
        || message == "tool result already committed with different content"
    {
        ApiError(
            StatusCode::CONFLICT,
            "Tool result conflicts with its recorded state",
        )
    } else {
        // Provider, database and tool errors can contain private context.
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Runtime operation failed",
        )
    }
}

fn body<T: serde::de::DeserializeOwned>(
    value: Result<Json<Value>, JsonRejection>,
) -> Result<T, ApiError> {
    let Json(value) = value.map_err(|error| {
        ApiError(
            error.status(),
            "Expected JSON with only the supported fields and values",
        )
    })?;
    if !value.is_object() {
        return Err(ApiError(StatusCode::BAD_REQUEST, "Expected a JSON object"));
    }
    serde_json::from_value(value).map_err(|_| {
        ApiError(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Expected JSON with only the supported fields and values",
        )
    })
}

fn nonempty(value: &str, max: usize) -> Result<(), ApiError> {
    if value.trim().is_empty() || value.len() > max || value.contains('\0') {
        Err(ApiError(
            StatusCode::BAD_REQUEST,
            "A required text field is empty or invalid",
        ))
    } else {
        Ok(())
    }
}

fn session(runtime: &Runtime, id: &str) -> Result<Session, ApiError> {
    nonempty(id, 256)?;
    runtime.get_session(id).map_err(runtime_error)
}

fn drive(runtime: &Runtime, value: &Session) {
    if value.status == SessionStatus::Ready {
        let runtime = runtime.clone();
        let id = value.id.clone();
        tokio::spawn(async move {
            if runtime.run(&id).await.is_err() {
                eprintln!("Session {id}: runtime driver stopped with an error");
            }
        });
    }
}

pub fn resume_ready(runtime: &Runtime) -> anyhow::Result<()> {
    for value in runtime.list_sessions()? {
        drive(runtime, &value);
    }
    Ok(())
}

async fn authorize(
    State(app): State<App>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    if let Some(expected) = &app.bearer {
        let supplied = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        if !supplied.is_some_and(|value| same_token(value.as_bytes(), expected.as_bytes())) {
            return ApiError(StatusCode::UNAUTHORIZED, "Bearer token required").into_response();
        }
    }
    next.run(request).await
}

fn same_token(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    for (index, byte) in right.iter().enumerate() {
        difference |= usize::from(left.get(index).copied().unwrap_or(0) ^ byte);
    }
    difference == 0
}

pub fn router(runtime: Runtime, bearer: Option<String>) -> Router {
    let app = App {
        runtime,
        bearer: bearer.map(Arc::from),
    };
    let api = Router::new()
        .route("/agents", get(agents))
        .route("/sessions", get(sessions).post(create))
        .route("/sessions/{id}", get(read_session))
        .route("/sessions/{id}/transcript", get(transcript))
        .route("/sessions/{id}/tools", get(tools))
        .route("/sessions/{id}/steer", post(steer))
        .route("/sessions/{id}/stop", post(stop))
        .route("/sessions/{id}/resume", post(resume))
        .route("/sessions/{id}/resolve-tool", post(resolve))
        .route_layer(middleware::from_fn_with_state(app.clone(), authorize));
    Router::new()
        .route("/", get(|| async { Html(include_str!("dashboard.html")) }))
        .route(
            "/health",
            get(|| async { Json(json!({"status":"ok","service":"borg-native"})) }),
        )
        .nest("/api", api)
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(app)
}

async fn agents(State(app): State<App>) -> Result<Json<Vec<Value>>, ApiError> {
    app.runtime
        .agents()
        .map_err(runtime_error)?
        .into_iter()
        .map(|agent| {
            let value = serde_json::to_value(agent).map_err(|_| {
                ApiError(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Agent metadata unavailable",
                )
            })?;
            let mut public = serde_json::Map::new();
            for key in [
                "id",
                "model",
                "instructions",
                "reasoning_effort",
                "max_output_tokens",
                "context_window_tokens",
                "max_turns",
            ] {
                if let Some(value) = value.get(key) {
                    public.insert(key.to_owned(), value.clone());
                }
            }
            Ok(Value::Object(public))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

async fn sessions(State(app): State<App>) -> Result<Json<Vec<Session>>, ApiError> {
    app.runtime.list_sessions().map(Json).map_err(runtime_error)
}

async fn read_session(
    State(app): State<App>,
    Path(id): Path<String>,
) -> Result<Json<Session>, ApiError> {
    session(&app.runtime, &id).map(Json)
}

async fn transcript(
    State(app): State<App>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    session(&app.runtime, &id)?;
    app.runtime
        .transcript(&id)
        .map(|value| Json(json!(value)))
        .map_err(runtime_error)
}

async fn tools(State(app): State<App>, Path(id): Path<String>) -> Result<Json<Value>, ApiError> {
    session(&app.runtime, &id)?;
    app.runtime
        .tool_calls(&id)
        .map(|value| Json(json!(value)))
        .map_err(runtime_error)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateSession {
    agent_id: String,
    #[serde(default)]
    task: Option<String>,
    input: String,
}

async fn create(
    State(app): State<App>,
    input: Result<Json<Value>, JsonRejection>,
) -> Result<impl IntoResponse, ApiError> {
    let input: CreateSession = body(input)?;
    nonempty(&input.agent_id, 256)?;
    nonempty(&input.input, MAX_BODY_BYTES)?;
    if let Some(task) = &input.task {
        nonempty(task, 8192)?;
    }
    if !app
        .runtime
        .agents()
        .map_err(runtime_error)?
        .iter()
        .any(|agent| agent.id == input.agent_id)
    {
        return Err(ApiError(StatusCode::BAD_REQUEST, "Unknown agent"));
    }
    let value = app
        .runtime
        .create_session(&input.agent_id, input.task, &input.input)
        .map_err(runtime_error)?;
    drive(&app.runtime, &value);
    Ok((
        StatusCode::CREATED,
        [(header::LOCATION, format!("/api/sessions/{}", value.id))],
        Json(value),
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Steering {
    text: String,
}

async fn steer(
    State(app): State<App>,
    Path(id): Path<String>,
    input: Result<Json<Value>, JsonRejection>,
) -> Result<Json<Session>, ApiError> {
    let input: Steering = body(input)?;
    nonempty(&input.text, MAX_BODY_BYTES)?;
    session(&app.runtime, &id)?;
    let value = app.runtime.steer(&id, &input.text).map_err(runtime_error)?;
    drive(&app.runtime, &value);
    Ok(Json(value))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}

async fn stop(
    State(app): State<App>,
    Path(id): Path<String>,
    input: Result<Json<Value>, JsonRejection>,
) -> Result<Json<Session>, ApiError> {
    let _: Empty = body(input)?;
    session(&app.runtime, &id)?;
    app.runtime.stop(&id).map(Json).map_err(runtime_error)
}

async fn resume(
    State(app): State<App>,
    Path(id): Path<String>,
    input: Result<Json<Value>, JsonRejection>,
) -> Result<Json<Session>, ApiError> {
    let _: Empty = body(input)?;
    session(&app.runtime, &id)?;
    let value = app.runtime.resume(&id).map_err(runtime_error)?;
    drive(&app.runtime, &value);
    Ok(Json(value))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolResolution {
    call_id: String,
    #[serde(deserialize_with = "required_value")]
    result: Value,
}

fn required_value<'de, D: serde::Deserializer<'de>>(input: D) -> Result<Value, D::Error> {
    Value::deserialize(input)
}

async fn resolve(
    State(app): State<App>,
    Path(id): Path<String>,
    input: Result<Json<Value>, JsonRejection>,
) -> Result<Json<Session>, ApiError> {
    let input: ToolResolution = body(input)?;
    nonempty(&input.call_id, 256)?;
    let current = session(&app.runtime, &id)?;
    if current.status != SessionStatus::NeedsResolution {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "Session has no tool result awaiting resolution",
        ));
    }
    let calls = app.runtime.tool_calls(&id).map_err(runtime_error)?;
    if !calls
        .iter()
        .any(|call| call.call_id == input.call_id && call.status == "unknown")
    {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "Unknown tool call is not awaiting resolution",
        ));
    }
    let value = app
        .runtime
        .resolve_tool(&id, &input.call_id, input.result)
        .map_err(runtime_error)?;
    drive(&app.runtime, &value);
    Ok(Json(value))
}

pub fn listen_policy(config: &RuntimeConfig) -> anyhow::Result<(SocketAddr, Option<String>)> {
    let address: SocketAddr = config
        .listen
        .parse()
        .map_err(|_| anyhow::anyhow!("listen must be a numeric IP address and port"))?;
    let token = match &config.bearer_token_env {
        Some(name) => {
            let token = std::env::var(name).map_err(|_| {
                anyhow::anyhow!("configured bearer token environment variable is unavailable")
            })?;
            anyhow::ensure!(
                !token.trim().is_empty() && !token.chars().any(char::is_control),
                "configured bearer token is empty or invalid"
            );
            Some(token)
        }
        None => None,
    };
    anyhow::ensure!(
        address.ip().is_loopback() || token.is_some(),
        "non-loopback listen requires bearer_token_env"
    );
    Ok((address, token))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args_os().skip(1);
    let config_path = match (args.next(), args.next(), args.next()) {
        (Some(flag), Some(path), None) if flag == "--config" => PathBuf::from(path),
        (Some(flag), None, None) if flag == "--help" || flag == "-h" => {
            println!("Usage: borg-native --config FILE\nDefault listen: 127.0.0.1:8787");
            return Ok(());
        }
        _ => anyhow::bail!("Usage: borg-native --config FILE"),
    };
    let config: RuntimeConfig = serde_json::from_slice(&std::fs::read(&config_path)?)?;
    let (address, bearer) = listen_policy(&config)?;
    let runtime = Runtime::open(&config_path)?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    resume_ready(&runtime)?;
    eprintln!("borg-native listening on {}", listener.local_addr()?);
    axum::serve(listener, router(runtime, bearer))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
