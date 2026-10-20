//! Native Fleet's coordination records live in the shared Board, not on a worker.
//!
//! Reserving capacity and an idempotency key is one SQLite transaction. A dispatch
//! never expires into another host: a lost create response is reconciled by its
//! preallocated session UUID on the original runtime. This deliberately favors
//! visible, owned repair over executing a consequential tool twice.

use crate::{api::AppState, db::Pool, events};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{Row, Sqlite, Transaction};

const TERMINAL: &str = "'completed','failed','stopped','cancelled'";

pub async fn init(pool: &Pool) -> anyhow::Result<()> {
    for statement in [
        "CREATE TABLE IF NOT EXISTS fleet_hosts (
            host_id TEXT PRIMARY KEY, capacity INTEGER NOT NULL, enabled INTEGER NOT NULL DEFAULT 1,
            runtime_url TEXT NOT NULL, workspace_root TEXT NOT NULL, revision TEXT NOT NULL DEFAULT '',
            runtime_ready INTEGER NOT NULL DEFAULT 0, ttl_seconds INTEGER NOT NULL DEFAULT 60,
            last_seen INTEGER NOT NULL, metadata TEXT NOT NULL DEFAULT '{}')",
        "CREATE TABLE IF NOT EXISTS fleet_dispatches (
            id INTEGER PRIMARY KEY AUTOINCREMENT, request_id TEXT NOT NULL UNIQUE,
            session_id TEXT NOT NULL UNIQUE, agent_id TEXT NOT NULL REFERENCES agents(id),
            task_id INTEGER REFERENCES tasks(id), host_id TEXT NOT NULL REFERENCES fleet_hosts(host_id),
            runtime_url TEXT NOT NULL, workspace_cwd TEXT NOT NULL,
            generation INTEGER NOT NULL DEFAULT 1, version INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL DEFAULT 'reserved', input TEXT NOT NULL, intent TEXT NOT NULL,
            metadata TEXT NOT NULL DEFAULT '{}', progress TEXT, error TEXT,
            created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)",
        "CREATE INDEX IF NOT EXISTS fleet_dispatch_host ON fleet_dispatches(host_id,status)",
        "CREATE UNIQUE INDEX IF NOT EXISTS fleet_task_active ON fleet_dispatches(task_id)
            WHERE task_id IS NOT NULL AND status NOT IN ('completed','failed','stopped','cancelled')",
    ] {
        sqlx::query(statement).execute(pool).await?;
    }
    Ok(())
}

#[derive(Debug)]
pub struct Error(StatusCode, String);
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error":self.1}))).into_response()
    }
}
impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        Self(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    }
}
impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        Self(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    }
}
type Result<T> = std::result::Result<T, Error>;
fn bad(message: impl Into<String>) -> Error {
    Error(StatusCode::BAD_REQUEST, message.into())
}
fn conflict(message: impl Into<String>) -> Error {
    Error(StatusCode::CONFLICT, message.into())
}
fn missing(message: impl Into<String>) -> Error {
    Error(StatusCode::NOT_FOUND, message.into())
}
fn now() -> i64 {
    chrono::Utc::now().timestamp()
}
fn object() -> Value {
    json!({})
}
fn text(value: &str, field: &str, max: usize) -> Result<()> {
    if value.trim().is_empty() || value.len() > max || value.contains('\0') {
        return Err(bad(format!(
            "{field} must be nonempty, at most {max} bytes, without NUL"
        )));
    }
    Ok(())
}
fn absolute_path(value: &str, field: &str) -> Result<()> {
    text(value, field, 8192)?;
    let path = std::path::Path::new(value);
    if !path.is_absolute()
        || path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err(bad(format!(
            "{field} must be an absolute path without traversal"
        )));
    }
    Ok(())
}
fn http_url(value: &str) -> Result<()> {
    text(value, "runtime_url", 8192)?;
    let url = reqwest::Url::parse(value).map_err(|_| bad("invalid runtime_url"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(bad(
            "runtime_url must be an http(s) base URL without credentials/query/fragment",
        ));
    }
    Ok(())
}
fn is_terminal(status: &str) -> bool {
    matches!(status, "completed" | "failed" | "stopped" | "cancelled")
}
fn valid_status(status: &str) -> bool {
    matches!(
        status,
        "reserved"
            | "provisioning"
            | "launching"
            | "ready"
            | "running"
            | "retrying"
            | "waiting"
            | "stopping"
            | "stopped"
            | "completed"
            | "failed"
            | "needs_resolution"
            | "cancelled"
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Heartbeat {
    pub capacity: u32,
    pub runtime_url: String,
    pub workspace_root: String,
    #[serde(default)]
    pub revision: String,
    pub runtime_ready: bool,
    #[serde(default = "default_ttl")]
    pub ttl_seconds: u32,
    #[serde(default = "object")]
    pub metadata: Value,
}
fn default_ttl() -> u32 {
    60
}

pub async fn heartbeat(
    State(state): State<AppState>,
    Path(host): Path<String>,
    Json(body): Json<Heartbeat>,
) -> Result<Json<Value>> {
    Ok(Json(heartbeat_record(&state.pool, &host, &body).await?))
}
async fn heartbeat_record(pool: &Pool, host: &str, body: &Heartbeat) -> Result<Value> {
    text(host, "host_id", 256)?;
    http_url(&body.runtime_url)?;
    absolute_path(&body.workspace_root, "workspace_root")?;
    if body.capacity > 10000
        || !(5..=3600).contains(&body.ttl_seconds)
        || !body.metadata.is_object()
    {
        return Err(bad(
            "capacity must be <=10000, ttl_seconds 5..3600, metadata an object",
        ));
    }
    // Configuration is a seed, not a periodic overwrite of Board-owned capacity,
    // drain state, freshness policy, or labels. Operator edits survive heartbeats.
    sqlx::query("INSERT INTO fleet_hosts(host_id,capacity,runtime_url,workspace_root,revision,runtime_ready,ttl_seconds,last_seen,metadata)
        VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(host_id) DO UPDATE SET
        runtime_url=excluded.runtime_url,workspace_root=excluded.workspace_root,revision=excluded.revision,
        runtime_ready=excluded.runtime_ready,last_seen=excluded.last_seen")
        .bind(host).bind(body.capacity).bind(body.runtime_url.trim_end_matches('/'))
        .bind(body.workspace_root.trim_end_matches('/')).bind(&body.revision)
        .bind(body.runtime_ready).bind(body.ttl_seconds).bind(now()).bind(body.metadata.to_string())
        .execute(pool).await?;
    let rows = hosts(pool).await?;
    Ok(rows
        .into_iter()
        .find(|h| h["host_id"] == host)
        .expect("just inserted host"))
}

pub async fn list_hosts(State(state): State<AppState>) -> Result<Json<Value>> {
    Ok(Json(json!(hosts(&state.pool).await?)))
}
async fn hosts(pool: &Pool) -> Result<Vec<Value>> {
    let query = format!("SELECT h.*, (SELECT COUNT(*) FROM fleet_dispatches d WHERE d.host_id=h.host_id AND d.status NOT IN ({TERMINAL})) AS active FROM fleet_hosts h ORDER BY h.host_id");
    let rows = sqlx::query(&query).fetch_all(pool).await?;
    rows.iter().map(|row| host_value(row, now())).collect()
}
fn host_value(row: &sqlx::sqlite::SqliteRow, time: i64) -> Result<Value> {
    let last_seen: i64 = row.try_get("last_seen")?;
    let ttl: i64 = row.try_get("ttl_seconds")?;
    let enabled: bool = row.try_get("enabled")?;
    let ready: bool = row.try_get("runtime_ready")?;
    let metadata: Value =
        serde_json::from_str(&row.try_get::<String, _>("metadata")?).unwrap_or_else(|_| object());
    let provisioning_ready = metadata
        .get("provisioning")
        .and_then(|p| p.get("ready"))
        .and_then(Value::as_bool)
        .unwrap_or(true);
    Ok(
        json!({"host_id":row.try_get::<String,_>("host_id")?,"capacity":row.try_get::<i64,_>("capacity")?,
        "enabled":enabled,"runtime_url":row.try_get::<String,_>("runtime_url")?,
        "workspace_root":row.try_get::<String,_>("workspace_root")?,"revision":row.try_get::<String,_>("revision")?,
        "runtime_ready":ready,"ttl_seconds":ttl,"last_seen":last_seen,
        "fresh":enabled && ready && provisioning_ready && time >= last_seen && time - last_seen <= ttl,
        "provisioning_ready":provisioning_ready,
        "active":row.try_get::<i64,_>("active")?,
        "metadata":metadata}),
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostPatch {
    capacity: Option<u32>,
    enabled: Option<bool>,
    ttl_seconds: Option<u32>,
    metadata: Option<Value>,
}
pub async fn update_host(
    State(state): State<AppState>,
    Path(host): Path<String>,
    Json(body): Json<HostPatch>,
) -> Result<Json<Value>> {
    if body.capacity.is_some_and(|c| c > 10000)
        || body.ttl_seconds.is_some_and(|t| !(5..=3600).contains(&t))
        || body.metadata.as_ref().is_some_and(|m| !m.is_object())
    {
        return Err(bad("invalid host policy"));
    }
    let count = sqlx::query("UPDATE fleet_hosts SET capacity=COALESCE(?,capacity),enabled=COALESCE(?,enabled),
        ttl_seconds=COALESCE(?,ttl_seconds),metadata=json_patch(metadata,COALESCE(?,'{}')) WHERE host_id=?")
        .bind(body.capacity).bind(body.enabled).bind(body.ttl_seconds)
        .bind(body.metadata.map(|m| m.to_string())).bind(&host).execute(&state.pool).await?.rows_affected();
    if count == 0 {
        return Err(missing("no fleet host"));
    }
    Ok(Json(
        hosts(&state.pool)
            .await?
            .into_iter()
            .find(|h| h["host_id"] == host)
            .unwrap(),
    ))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Create {
    pub request_id: String,
    pub agent_id: String,
    #[serde(default)]
    pub task_id: Option<i64>,
    #[serde(default)]
    pub input: Option<String>,
    #[serde(default = "object")]
    pub metadata: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Adopt {
    #[serde(flatten)]
    pub create: Create,
    pub session_id: String,
    pub host_id: String,
    pub runtime_url: String,
    pub workspace_cwd: String,
    #[serde(default = "one")]
    pub generation: i64,
    #[serde(default = "running")]
    pub status: String,
}
fn one() -> i64 {
    1
}
fn running() -> String {
    "running".into()
}

pub async fn create_dispatch(
    State(state): State<AppState>,
    Json(body): Json<Create>,
) -> Result<Json<Value>> {
    Ok(Json(reserve(&state.pool, &body, None).await?))
}
pub async fn adopt_dispatch(
    State(state): State<AppState>,
    Json(body): Json<Adopt>,
) -> Result<Json<Value>> {
    Ok(Json(reserve(&state.pool, &body.create, Some(&body)).await?))
}

async fn reserve(pool: &Pool, body: &Create, adopt: Option<&Adopt>) -> Result<Value> {
    text(&body.request_id, "request_id", 512)?;
    text(&body.agent_id, "agent_id", 256)?;
    if !body.metadata.is_object() {
        return Err(bad("metadata must be an object"));
    }
    if let Some(input) = &body.input {
        text(input, "input", 64 * 1024 * 1024)?;
    }
    if let Some(a) = adopt {
        text(&a.session_id, "session_id", 256)?;
        http_url(&a.runtime_url)?;
        absolute_path(&a.workspace_cwd, "workspace_cwd")?;
        if a.generation < 0
            || !valid_status(&a.status)
            || matches!(a.status.as_str(), "reserved" | "provisioning" | "launching")
        {
            return Err(bad("invalid adopted generation or status"));
        }
    }
    let intent = serde_json::to_string(body).expect("serializable create intent");
    let mut tx = pool.begin().await?;
    // A write first also serializes this operation with another process sharing
    // the database (not merely with our single-connection pool).
    sqlx::query("UPDATE fleet_hosts SET last_seen=last_seen WHERE 0")
        .execute(&mut *tx)
        .await?;
    if let Some(row) = sqlx::query("SELECT * FROM fleet_dispatches WHERE request_id=?")
        .bind(&body.request_id)
        .fetch_optional(&mut *tx)
        .await?
    {
        if row.try_get::<String, _>("intent")? != intent {
            return Err(conflict("request_id already has different intent"));
        }
        if let Some(a) = adopt {
            if row.try_get::<String, _>("session_id")? != a.session_id
                || row.try_get::<String, _>("host_id")? != a.host_id
                || row.try_get::<String, _>("runtime_url")? != a.runtime_url.trim_end_matches('/')
                || row.try_get::<String, _>("workspace_cwd")? != a.workspace_cwd
            {
                return Err(conflict("adoption conflicts with the existing reservation"));
            }
        }
        let mut value = dispatch_value(&row)?;
        value["created"] = json!(false);
        tx.commit().await?;
        return Ok(value);
    }
    let agent = sqlx::query("SELECT metadata,lifecycle_intent,retired_at FROM agents WHERE id=?")
        .bind(&body.agent_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| missing("no agent"))?;
    let agent_md: Value =
        serde_json::from_str(&agent.try_get::<String, _>("metadata")?).unwrap_or_else(|_| object());
    if adopt.is_none()
        && (agent_md["harness"] != "native"
            || agent.try_get::<String, _>("lifecycle_intent")? != "run"
            || agent.try_get::<Option<String>, _>("retired_at")?.is_some()
            || agent_md
                .get("lifecycle_intent")
                .is_some_and(|v| matches!(v.as_str(), Some("paused" | "retired"))))
    {
        return Err(conflict("agent is not an enabled native identity"));
    }
    let mut project_id = None;
    let mut task_md = object();
    let input = if let Some(task_id) = body.task_id {
        let task = sqlx::query("SELECT * FROM tasks WHERE id=?")
            .bind(task_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| missing("no task"))?;
        if task.try_get::<Option<String>, _>("assignee")?.as_deref() != Some(&body.agent_id) {
            return Err(conflict("task is not assigned to agent_id"));
        }
        task_md = serde_json::from_str(&task.try_get::<String, _>("metadata")?)
            .unwrap_or_else(|_| object());
        project_id = Some(task.try_get::<i64, _>("project_id")?);
        if adopt.is_none()
            && (!matches!(
                task.try_get::<String, _>("status")?.as_str(),
                "todo" | "in_progress"
            ) || task
                .try_get::<Option<String>, _>("blocked_on_kind")?
                .is_some()
                || task.try_get::<Option<String>, _>("archived_at")?.is_some()
                || task_md["monitor_exempt"] == true
                || task_md["native_dispatch"] == false)
        {
            return Err(conflict(
                "task is not actionable or native dispatch is disabled",
            ));
        }
        let active_query = format!(
            "SELECT * FROM fleet_dispatches WHERE task_id=? AND status NOT IN ({TERMINAL})"
        );
        if let Some(active) = sqlx::query(&active_query)
            .bind(task_id)
            .fetch_optional(&mut *tx)
            .await?
        {
            return Err(conflict(format!(
                "task already has active dispatch {}",
                active.try_get::<i64, _>("id")?
            )));
        }
        body.input.clone().unwrap_or_else(|| format!(
            "Execute Task Board task {task_id} in project {} as {}. Read your current charter and full task, mark it in progress, implement and test useful work, and publish concrete evidence. Use the isolated workspace and coordinate integration on the board. Preserve completed tool results during recovery.",
            project_id.unwrap(), body.agent_id))
    } else {
        body.input
            .clone()
            .ok_or_else(|| bad("input is required without task_id"))?
    };
    let time = now();
    let host_query = format!("SELECT h.*, (SELECT COUNT(*) FROM fleet_dispatches d WHERE d.host_id=h.host_id AND d.status NOT IN ({TERMINAL})) AS active FROM fleet_hosts h ORDER BY h.host_id");
    let host_rows = sqlx::query(&host_query).fetch_all(&mut *tx).await?;
    let mut candidates: Vec<Value> = host_rows
        .iter()
        .map(|r| host_value(r, time))
        .collect::<Result<_>>()?;
    let selected = if let Some(a) = adopt {
        candidates
            .into_iter()
            .find(|h| h["host_id"] == a.host_id)
            .ok_or_else(|| missing("register the adopted host first"))?
    } else {
        candidates.retain(|h| {
            h["fresh"] == true
                && h["capacity"].as_u64().unwrap_or(0) > h["active"].as_u64().unwrap_or(0)
                && placement_allows(&agent_md["placement"], h)
                && placement_allows(&task_md["placement"], h)
        });
        // Ratio comparison without floating point. Stable host-id tie breaking.
        candidates.sort_by(|a, b| {
            (a["active"].as_u64().unwrap() * b["capacity"].as_u64().unwrap())
                .cmp(&(b["active"].as_u64().unwrap() * a["capacity"].as_u64().unwrap()))
                .then_with(|| a["host_id"].as_str().cmp(&b["host_id"].as_str()))
        });
        candidates.into_iter().next().ok_or_else(|| {
            Error(
                StatusCode::SERVICE_UNAVAILABLE,
                "no fresh eligible host capacity".into(),
            )
        })?
    };
    let session_id = if let Some(a) = adopt {
        a.session_id.clone()
    } else {
        new_uuid(&mut tx).await?
    };
    let host_id = selected["host_id"].as_str().unwrap();
    let runtime_url = adopt
        .map(|a| a.runtime_url.trim_end_matches('/').to_owned())
        .unwrap_or_else(|| selected["runtime_url"].as_str().unwrap().into());
    let cwd = adopt.map(|a| a.workspace_cwd.clone()).unwrap_or_else(|| {
        format!(
            "{}/{}",
            selected["workspace_root"]
                .as_str()
                .unwrap()
                .trim_end_matches('/'),
            session_id
        )
    });
    let status = adopt.map(|a| a.status.as_str()).unwrap_or("reserved");
    let generation = adopt.map(|a| a.generation).unwrap_or(1);
    let mut metadata = body.metadata.clone();
    metadata["adopted"] = json!(adopt.is_some());
    metadata["native_attempt"] = json!(task_md
        .get("native_attempt")
        .and_then(Value::as_u64)
        .unwrap_or(1));
    metadata["workspace"] = task_md
        .get("workspace")
        .or_else(|| agent_md.get("workspace"))
        .cloned()
        .unwrap_or_else(object);
    let id: i64 = sqlx::query_scalar("INSERT INTO fleet_dispatches(request_id,session_id,agent_id,task_id,host_id,runtime_url,workspace_cwd,generation,status,input,intent,metadata,created_at,updated_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING id")
        .bind(&body.request_id).bind(&session_id).bind(&body.agent_id).bind(body.task_id).bind(host_id)
        .bind(runtime_url).bind(cwd).bind(generation).bind(status).bind(input).bind(intent).bind(metadata.to_string())
        .bind(time).bind(time).fetch_one(&mut *tx).await?;
    let row = sqlx::query("SELECT * FROM fleet_dispatches WHERE id=?")
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
    let mut value = dispatch_value(&row)?;
    let mut hooks = Vec::new();
    events::emit(
        &mut tx,
        &mut hooks,
        "fleet.dispatch_created",
        Some("fleet"),
        body.task_id,
        project_id,
        None,
        None,
        value.clone(),
        events::Recipients::FromTask,
    )
    .await?;
    if let Some(task_id) = body.task_id {
        stamp_task(&mut tx, task_id, &value).await?;
    }
    tx.commit().await?;
    fire(hooks);
    value["created"] = json!(true);
    Ok(value)
}
fn placement_allows(placement: &Value, host: &Value) -> bool {
    if let Some(pin) = placement.get("host").and_then(Value::as_str) {
        if host["host_id"] != pin {
            return false;
        }
    }
    if let Some(allowed) = placement.get("hosts").and_then(Value::as_array) {
        if !allowed.iter().any(|v| v == &host["host_id"]) {
            return false;
        }
    }
    if let Some(labels) = placement.get("labels").and_then(Value::as_object) {
        if !labels
            .iter()
            .all(|(k, v)| host["metadata"]["labels"].get(k) == Some(v))
        {
            return false;
        }
    }
    true
}
async fn new_uuid(tx: &mut Transaction<'_, Sqlite>) -> Result<String> {
    let mut bytes: Vec<u8> = sqlx::query_scalar("SELECT randomblob(16)")
        .fetch_one(&mut **tx)
        .await?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let mut out = String::new();
    for (i, byte) in bytes.iter().enumerate() {
        if [4, 6, 8, 10].contains(&i) {
            out.push('-');
        }
        out.push_str(&format!("{byte:02x}"));
    }
    Ok(out)
}
fn dispatch_value(row: &sqlx::sqlite::SqliteRow) -> Result<Value> {
    let mut value = object();
    for name in ["id", "generation", "version", "created_at", "updated_at"] {
        value[name] = json!(row.try_get::<i64, _>(name)?);
    }
    value["task_id"] = json!(row.try_get::<Option<i64>, _>("task_id")?);
    for name in [
        "request_id",
        "session_id",
        "agent_id",
        "host_id",
        "runtime_url",
        "workspace_cwd",
        "status",
        "input",
    ] {
        value[name] = json!(row.try_get::<String, _>(name)?);
    }
    for name in ["error", "progress"] {
        value[name] = json!(row.try_get::<Option<String>, _>(name)?);
    }
    value["metadata"] =
        serde_json::from_str(&row.try_get::<String, _>("metadata")?).unwrap_or_else(|_| object());
    Ok(value)
}
async fn stamp_task(tx: &mut Transaction<'_, Sqlite>, task_id: i64, value: &Value) -> Result<()> {
    // A projection for existing Board/UI consumers, never a singleton on agent identity.
    let projection = json!({"native_dispatch":{
        "id":value["id"],"request_id":value["request_id"],"session_id":value["session_id"],
        "agent_id":value["agent_id"],"host_id":value["host_id"],"runtime_url":value["runtime_url"],
        "workspace_cwd":value["workspace_cwd"],"generation":value["generation"],"status":value["status"],
        "error":value["error"],"updated_at":value["updated_at"],"native_attempt":value["metadata"]["native_attempt"]}});
    sqlx::query("UPDATE tasks SET metadata=json_patch(metadata,?) WHERE id=?")
        .bind(projection.to_string())
        .bind(task_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
fn fire(hooks: Vec<events::WebhookDelivery>) {
    events::fire_webhooks(
        hooks,
        crate::WEBHOOK_TIMEOUT
            .get()
            .copied()
            .unwrap_or(std::time::Duration::from_secs(5)),
    );
}

#[derive(Default, Deserialize)]
pub struct Filter {
    host_id: Option<String>,
    agent_id: Option<String>,
    task_id: Option<i64>,
    status: Option<String>,
    request_id: Option<String>,
    session_id: Option<String>,
    limit: Option<u32>,
    offset: Option<u32>,
}
pub async fn list_dispatches(
    State(state): State<AppState>,
    Query(filter): Query<Filter>,
) -> Result<Json<Value>> {
    let rows = sqlx::query("SELECT * FROM fleet_dispatches WHERE (? IS NULL OR host_id=?) AND (? IS NULL OR agent_id=?)
        AND (? IS NULL OR task_id=?) AND (? IS NULL OR status=?) AND (? IS NULL OR request_id=?)
        AND (? IS NULL OR session_id=?) ORDER BY id LIMIT ? OFFSET ?")
        .bind(&filter.host_id).bind(&filter.host_id).bind(&filter.agent_id).bind(&filter.agent_id)
        .bind(filter.task_id).bind(filter.task_id).bind(&filter.status).bind(&filter.status)
        .bind(&filter.request_id).bind(&filter.request_id).bind(&filter.session_id).bind(&filter.session_id)
        .bind(filter.limit.unwrap_or(100).clamp(1,1000)).bind(filter.offset.unwrap_or(0))
        .fetch_all(&state.pool).await?;
    Ok(Json(json!(rows
        .iter()
        .map(dispatch_value)
        .collect::<Result<Vec<_>>>()?)))
}
pub async fn get_dispatch(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<Value>> {
    let row = sqlx::query("SELECT * FROM fleet_dispatches WHERE id=?")
        .bind(id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| missing("no fleet dispatch"))?;
    Ok(Json(dispatch_value(&row)?))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Report {
    pub host_id: String,
    pub generation: i64,
    pub version: i64,
    pub status: String,
    #[serde(default)]
    pub progress: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default = "object")]
    pub metadata: Value,
}
pub async fn report_dispatch(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(body): Json<Report>,
) -> Result<Json<Value>> {
    Ok(Json(report(&state.pool, id, &body).await?))
}
async fn report(pool: &Pool, id: i64, body: &Report) -> Result<Value> {
    if !valid_status(&body.status) || !body.metadata.is_object() {
        return Err(bad("invalid status or metadata"));
    }
    let mut tx = pool.begin().await?;
    sqlx::query("UPDATE fleet_hosts SET last_seen=last_seen WHERE 0")
        .execute(&mut *tx)
        .await?;
    let row = sqlx::query("SELECT * FROM fleet_dispatches WHERE id=?")
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| missing("no fleet dispatch"))?;
    let previous = dispatch_value(&row)?;
    if previous["host_id"] != body.host_id
        || previous["generation"] != body.generation
        || previous["version"] != body.version
    {
        return Err(conflict("stale dispatch host, generation, or version"));
    }
    // Terminal engine sessions can be explicitly resumed, but never replayed by
    // falling back to provisioning/create. Uncertain tools stay visibly held.
    if (is_terminal(previous["status"].as_str().unwrap())
        || previous["status"] == "needs_resolution")
        && matches!(
            body.status.as_str(),
            "reserved" | "provisioning" | "launching"
        )
    {
        return Err(conflict("cannot replay an ended or uncertain session"));
    }
    sqlx::query("UPDATE fleet_dispatches SET status=?,progress=?,error=?,metadata=json_patch(metadata,?),version=version+1,updated_at=? WHERE id=?")
        .bind(&body.status).bind(&body.progress).bind(&body.error).bind(body.metadata.to_string()).bind(now()).bind(id)
        .execute(&mut *tx).await?;
    let row = sqlx::query("SELECT * FROM fleet_dispatches WHERE id=?")
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
    let value = dispatch_value(&row)?;
    let mut hooks = Vec::new();
    let task_id = value["task_id"].as_i64();
    if let Some(task_id) = task_id {
        stamp_task(&mut tx, task_id, &value).await?;
    }
    if previous["status"] != value["status"] || previous["error"] != value["error"] {
        events::emit(
            &mut tx,
            &mut hooks,
            "fleet.dispatch_updated",
            Some("fleet"),
            task_id,
            None,
            None,
            None,
            value.clone(),
            events::Recipients::FromTask,
        )
        .await?;
    }
    tx.commit().await?;
    fire(hooks);
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fixture() -> (tempfile::TempDir, Pool) {
        let dir = tempfile::tempdir().unwrap();
        let pool = crate::db::init(dir.path().join("board.db").to_str().unwrap())
            .await
            .unwrap();
        sqlx::query("INSERT INTO agents(id,metadata,created_at) VALUES('engineer',?,?)")
            .bind(json!({"harness":"native","host":"old-observational-host"}).to_string())
            .bind(events::now_iso())
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO projects(id,name,created_at,updated_at) VALUES(1,'native',?,?)")
            .bind(events::now_iso())
            .bind(events::now_iso())
            .execute(&pool)
            .await
            .unwrap();
        for i in 1..=24 {
            sqlx::query("INSERT INTO tasks(id,project_id,title,assignee,created_at,updated_at) VALUES(?,1,?,'engineer',?,?)")
                .bind(i).bind(format!("task {i}")).bind(events::now_iso()).bind(events::now_iso()).execute(&pool).await.unwrap();
        }
        (dir, pool)
    }
    fn hb(capacity: u32) -> Heartbeat {
        Heartbeat {
            capacity,
            runtime_url: "http://127.0.0.1:8787/api".into(),
            workspace_root: "/workspaces".into(),
            revision: "test-rev".into(),
            runtime_ready: true,
            ttl_seconds: 60,
            metadata: object(),
        }
    }
    async fn four_hosts(pool: &Pool, capacity: u32) {
        for name in ["agent-lab", "agent-lab-2", "agent-lab-3", "agent-lab-4"] {
            heartbeat_record(pool, name, &hb(capacity)).await.unwrap();
        }
    }
    fn task(id: i64) -> Create {
        Create {
            request_id: format!("task:{id}:attempt:1"),
            agent_id: "engineer".into(),
            task_id: Some(id),
            input: None,
            metadata: object(),
        }
    }
    fn report_for(value: &Value, status: &str) -> Report {
        Report {
            host_id: value["host_id"].as_str().unwrap().into(),
            generation: value["generation"].as_i64().unwrap(),
            version: value["version"].as_i64().unwrap(),
            status: status.into(),
            progress: Some("observed runtime".into()),
            error: None,
            metadata: object(),
        }
    }

    #[tokio::test]
    async fn four_concurrent_workers_balance_and_deduplicate_on_shared_board() {
        let (_dir, pool) = fixture().await;
        four_hosts(&pool, 4).await;
        let mut workers = Vec::new();
        for _ in 0..4 {
            let pool = pool.clone();
            workers.push(tokio::spawn(async move {
                for id in 1..=16 {
                    reserve(&pool, &task(id), None).await.unwrap();
                }
            }));
        }
        for worker in workers {
            worker.await.unwrap();
        }
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM fleet_dispatches")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 16);
        for host in hosts(&pool).await.unwrap() {
            assert_eq!(host["active"], 4);
        }
        let rows = sqlx::query("SELECT count(DISTINCT session_id) s, count(DISTINCT workspace_cwd) w FROM fleet_dispatches")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(rows.get::<i64, _>("s"), 16);
        assert_eq!(rows.get::<i64, _>("w"), 16);
        assert_eq!(
            reserve(&pool, &task(17), None).await.unwrap_err().0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn stale_drained_unready_hosts_are_excluded_and_policy_survives_heartbeat() {
        let (_dir, pool) = fixture().await;
        four_hosts(&pool, 8).await;
        sqlx::query("UPDATE fleet_hosts SET last_seen=0 WHERE host_id='agent-lab'")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE fleet_hosts SET enabled=0,capacity=2 WHERE host_id='agent-lab-2'")
            .execute(&pool)
            .await
            .unwrap();
        heartbeat_record(&pool, "agent-lab-2", &hb(8))
            .await
            .unwrap();
        let mut not_ready = hb(8);
        not_ready.runtime_ready = false;
        heartbeat_record(&pool, "agent-lab-3", &not_ready)
            .await
            .unwrap();
        assert_eq!(
            reserve(&pool, &task(1), None).await.unwrap()["host_id"],
            "agent-lab-4"
        );
        let drained = hosts(&pool)
            .await
            .unwrap()
            .into_iter()
            .find(|h| h["host_id"] == "agent-lab-2")
            .unwrap();
        assert_eq!(drained["capacity"], 2);
        assert_eq!(drained["enabled"], false);
    }

    #[tokio::test]
    async fn provisioning_gate_blocks_empty_host_without_clobbering_operator_drain() {
        let (_dir, pool) = fixture().await;
        let mut heartbeat = hb(4);
        heartbeat.metadata = json!({"provisioning":{"ready":false,"checked_at":now()}});
        heartbeat_record(&pool, "bad-route", &heartbeat)
            .await
            .unwrap();
        let host = hosts(&pool).await.unwrap().remove(0);
        assert_eq!(host["runtime_ready"], true);
        assert_eq!(host["provisioning_ready"], false);
        assert_eq!(host["fresh"], false);
        assert_eq!(
            reserve(&pool, &task(1), None).await.unwrap_err().0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        // A process restart/ordinary native heartbeat cannot remove the failed
        // provisioning observation or accidentally reopen manual drain policy.
        heartbeat_record(&pool, "bad-route", &hb(4)).await.unwrap();
        assert_eq!(hosts(&pool).await.unwrap()[0]["fresh"], false);
        sqlx::query("UPDATE fleet_hosts SET enabled=0,metadata=json_set(metadata,'$.provisioning.ready',json('true'))")
            .execute(&pool).await.unwrap();
        let host = hosts(&pool).await.unwrap().remove(0);
        assert_eq!(host["provisioning_ready"], true);
        assert_eq!(host["fresh"], false);
        sqlx::query("UPDATE fleet_hosts SET enabled=1")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            reserve(&pool, &task(1), None).await.unwrap()["host_id"],
            "bad-route"
        );
    }

    #[tokio::test]
    async fn weighted_capacity_and_explicit_placement_not_observed_identity_host() {
        let (_dir, pool) = fixture().await;
        heartbeat_record(&pool, "small", &hb(2)).await.unwrap();
        heartbeat_record(&pool, "large", &hb(6)).await.unwrap();
        for id in 1..=8 {
            reserve(&pool, &task(id), None).await.unwrap();
        }
        let all = hosts(&pool).await.unwrap();
        assert_eq!(
            all.iter().find(|h| h["host_id"] == "small").unwrap()["active"],
            2
        );
        assert_eq!(
            all.iter().find(|h| h["host_id"] == "large").unwrap()["active"],
            6
        );
        sqlx::query("UPDATE fleet_hosts SET capacity=20")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE agents SET metadata=? WHERE id='engineer'")
            .bind(json!({"harness":"native","placement":{"host":"small"}}).to_string())
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            reserve(&pool, &task(9), None).await.unwrap()["host_id"],
            "small"
        );
        sqlx::query("UPDATE tasks SET metadata=? WHERE id=10")
            .bind(json!({"placement":{"hosts":["large"]}}).to_string())
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            reserve(&pool, &task(10), None).await.unwrap_err().0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn idempotent_terminal_retry_never_replays_and_changed_intent_conflicts() {
        let (_dir, pool) = fixture().await;
        four_hosts(&pool, 1).await;
        let first = reserve(&pool, &task(1), None).await.unwrap();
        let id = first["id"].as_i64().unwrap();
        let ended = report(&pool, id, &report_for(&first, "completed"))
            .await
            .unwrap();
        let repeated = reserve(&pool, &task(1), None).await.unwrap();
        assert_eq!(repeated["id"], id);
        assert_eq!(repeated["created"], false);
        assert_eq!(repeated["status"], "completed");
        let mut changed = task(1);
        changed.input = Some("different intent".into());
        assert_eq!(
            reserve(&pool, &changed, None).await.unwrap_err().0,
            StatusCode::CONFLICT
        );
        assert_eq!(
            report(&pool, id, &report_for(&ended, "launching"))
                .await
                .unwrap_err()
                .0,
            StatusCode::CONFLICT
        );
    }

    #[tokio::test]
    async fn version_generation_host_and_active_task_are_fenced() {
        let (_dir, pool) = fixture().await;
        four_hosts(&pool, 2).await;
        let first = reserve(&pool, &task(1), None).await.unwrap();
        let id = first["id"].as_i64().unwrap();
        let mut other = task(1);
        other.request_id = "different-key".into();
        assert_eq!(
            reserve(&pool, &other, None).await.unwrap_err().0,
            StatusCode::CONFLICT
        );
        let update = report_for(&first, "running");
        let updated = report(&pool, id, &update).await.unwrap();
        assert_eq!(
            report(&pool, id, &update).await.unwrap_err().0,
            StatusCode::CONFLICT
        );
        let mut stale = report_for(&updated, "failed");
        stale.generation = 0;
        assert_eq!(
            report(&pool, id, &stale).await.unwrap_err().0,
            StatusCode::CONFLICT
        );
        stale.generation = 1;
        stale.host_id = "other-host".into();
        assert_eq!(
            report(&pool, id, &stale).await.unwrap_err().0,
            StatusCode::CONFLICT
        );
        let raw: String = sqlx::query_scalar("SELECT metadata FROM tasks WHERE id=1")
            .fetch_one(&pool)
            .await
            .unwrap();
        let md: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(md["native_dispatch"]["status"], "running");
        assert_eq!(md["native_dispatch"]["session_id"], first["session_id"]);
    }

    #[tokio::test]
    async fn paused_blocked_exempt_tasks_do_not_launch_and_unknown_tools_hold_capacity() {
        let (_dir, pool) = fixture().await;
        heartbeat_record(&pool, "only", &hb(1)).await.unwrap();
        sqlx::query("UPDATE agents SET lifecycle_intent='paused'")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            reserve(&pool, &task(1), None).await.unwrap_err().0,
            StatusCode::CONFLICT
        );
        sqlx::query("UPDATE agents SET lifecycle_intent='run'")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE tasks SET status='blocked',blocked_on_kind='external' WHERE id=1")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            reserve(&pool, &task(1), None).await.unwrap_err().0,
            StatusCode::CONFLICT
        );
        sqlx::query("UPDATE tasks SET metadata='{\"monitor_exempt\":true}' WHERE id=2")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            reserve(&pool, &task(2), None).await.unwrap_err().0,
            StatusCode::CONFLICT
        );
        let first = reserve(&pool, &task(3), None).await.unwrap();
        let id = first["id"].as_i64().unwrap();
        let uncertain = report(&pool, id, &report_for(&first, "needs_resolution"))
            .await
            .unwrap();
        assert_eq!(hosts(&pool).await.unwrap()[0]["active"], 1);
        assert_eq!(
            reserve(&pool, &task(4), None).await.unwrap_err().0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            report(&pool, id, &report_for(&uncertain, "launching"))
                .await
                .unwrap_err()
                .0,
            StatusCode::CONFLICT
        );
    }

    #[tokio::test]
    async fn adoption_and_database_restart_preserve_live_bootstrap() {
        let (dir, pool) = fixture().await;
        four_hosts(&pool, 2).await;
        let raw = json!({"request_id":"task:1:attempt:1","agent_id":"engineer","task_id":1,
            "session_id":"bootstrap-existing-uuid","host_id":"agent-lab-3","runtime_url":"http://127.0.0.1:17873/api",
            "workspace_cwd":"/home/dev-user/borg-native-workspace","generation":0,"status":"running"});
        let adopted: Adopt = serde_json::from_value(raw).unwrap();
        let first = reserve(&pool, &adopted.create, Some(&adopted))
            .await
            .unwrap();
        assert_eq!(first["metadata"]["adopted"], true);
        assert_eq!(first["session_id"], "bootstrap-existing-uuid");
        assert_eq!(first["runtime_url"], "http://127.0.0.1:17873/api");
        pool.close().await;
        let reopened = crate::db::init(dir.path().join("board.db").to_str().unwrap())
            .await
            .unwrap();
        let repeated = reserve(&reopened, &task(1), None).await.unwrap();
        assert_eq!(repeated["created"], false);
        assert_eq!(repeated["session_id"], first["session_id"]);
        assert_eq!(repeated["workspace_cwd"], first["workspace_cwd"]);
        assert_eq!(repeated["generation"], 0);
        assert_eq!(
            hosts(&reopened)
                .await
                .unwrap()
                .iter()
                .find(|h| h["host_id"] == "agent-lab-3")
                .unwrap()["active"],
            1
        );
    }

    #[tokio::test]
    async fn independent_same_identity_dispatches_have_distinct_session_workspaces() {
        let (_dir, pool) = fixture().await;
        four_hosts(&pool, 1).await;
        let one = Create {
            request_id: "one".into(),
            agent_id: "engineer".into(),
            task_id: None,
            input: Some("one".into()),
            metadata: object(),
        };
        let two = Create {
            request_id: "two".into(),
            input: Some("two".into()),
            ..one.clone()
        };
        let a = reserve(&pool, &one, None).await.unwrap();
        let b = reserve(&pool, &two, None).await.unwrap();
        assert_eq!(a["agent_id"], b["agent_id"]);
        assert_ne!(a["session_id"], b["session_id"]);
        assert_ne!(a["workspace_cwd"], b["workspace_cwd"]);
        assert_ne!(a["host_id"], b["host_id"]);
        assert_eq!(a["session_id"].as_str().unwrap().len(), 36);
    }
    #[tokio::test]
    async fn rest_boundary_exposes_json_rows_and_conflicts() {
        use tower::ServiceExt;
        let (_dir, pool) = fixture().await;
        let (events_tx, _) = tokio::sync::broadcast::channel(16);
        let app = crate::api::router(AppState {
            pool,
            events_tx,
            ipfs_api_url: None,
            shutdown: tokio_util::sync::CancellationToken::new(),
            db_path: String::new(),
            db_snapshot: crate::api::DbSnapshotCfg::disabled(),
            host_auth: Default::default(),
            link_rules: Default::default(),
        });
        async fn call(
            app: axum::Router,
            method: &str,
            path: &str,
            body: Value,
        ) -> (StatusCode, Value) {
            let response = app
                .oneshot(
                    axum::http::Request::builder()
                        .method(method)
                        .uri(path)
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = response.status();
            let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024 * 1024)
                .await
                .unwrap();
            (status, serde_json::from_slice(&bytes).unwrap())
        }
        let (code,host)=call(app.clone(),"POST","/fleet/hosts/h/heartbeat",json!({
            "capacity":2,"runtime_url":"http://127.0.0.1:8787/api","workspace_root":"/ws","runtime_ready":true})).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(host["fresh"], true);
        let body = serde_json::to_value(task(1)).unwrap();
        let (code, first) = call(app.clone(), "POST", "/fleet/dispatches", body.clone()).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(first["created"], true);
        let (_, same) = call(app.clone(), "POST", "/fleet/dispatches", body).await;
        assert_eq!(same["created"], false);
        assert_eq!(same["session_id"], first["session_id"]);
        let (code, rows) = call(
            app.clone(),
            "GET",
            "/fleet/dispatches?host_id=h&limit=10",
            json!(null),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(rows.as_array().unwrap().len(), 1);
        // The broker must not reintroduce Axum's default 2MiB body ceiling
        // (or a smaller model-input allowance) ahead of the 64MiB native API.
        let (code, large) = call(
            app.clone(),
            "POST",
            "/fleet/dispatches",
            json!({
                "request_id":"large-input", "agent_id":"engineer", "input":"x".repeat(3*1024*1024)
            }),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(large["input"].as_str().unwrap().len(), 3 * 1024 * 1024);

        let (code, error) = call(
            app,
            "PATCH",
            &format!("/fleet/dispatches/{}", first["id"]),
            json!({
            "host_id":"other","generation":1,"version":1,"status":"running"}),
        )
        .await;
        assert_eq!(code, StatusCode::CONFLICT);
        assert!(error["error"].as_str().unwrap().contains("stale"));
    }
}
