//! Persistent native harness worker. Board reservations are the only dispatch
//! authority; this process owns neither identities nor conversation state.
//!
//! Native engines and session-workspace MCP services are independently supervised.
//! Restarting Fleet does not kill an inference/tool or discard its result. The
//! worker reconciles by preallocated session UUID before retrying any create.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub runtime_url: String,
    pub workspace_root: PathBuf,
    #[serde(default = "workspace_url")]
    pub workspace_mcp_url: String,
    #[serde(default = "capacity")]
    pub capacity: u32,
    #[serde(default = "poll_seconds")]
    pub poll_seconds: u64,
    #[serde(default = "ttl_seconds")]
    pub ttl_seconds: u64,
    #[serde(default)]
    pub project_id: Option<i64>,
    #[serde(default)]
    pub repo_url: Option<String>,
    /// Existing host SSH config; copied as a repo-local command, never as key bytes.
    #[serde(default)]
    pub git_ssh_config: Option<PathBuf>,
    #[serde(default = "repo_ref")]
    pub repo_ref: String,
    #[serde(default)]
    pub bearer_token_env: Option<String>,
    #[serde(default)]
    pub board_bearer_token_env: Option<String>,
    #[serde(default)]
    pub workspace_bearer_token_env: Option<String>,
    #[serde(default = "http_seconds")]
    pub http_timeout_seconds: u64,
    #[serde(default = "provision_seconds")]
    pub provision_timeout_seconds: u64,
    #[serde(default = "git_probe_interval")]
    pub git_probe_interval_seconds: u64,
    #[serde(default = "git_probe_timeout")]
    pub git_probe_timeout_seconds: u64,
}
fn workspace_url() -> String {
    "http://127.0.0.1:8791/mcp/{session_id}".into()
}
fn capacity() -> u32 {
    4
}
fn poll_seconds() -> u64 {
    5
}
fn ttl_seconds() -> u64 {
    60
}
fn repo_ref() -> String {
    "HEAD".into()
}
fn http_seconds() -> u64 {
    15
}
fn provision_seconds() -> u64 {
    300
}
fn git_probe_interval() -> u64 {
    30
}
fn git_probe_timeout() -> u64 {
    10
}

#[derive(Debug)]
struct Failure {
    status: Option<u16>,
    message: String,
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message.fmt(f)
    }
}
impl Failure {
    fn retryable(&self) -> bool {
        self.status.is_none()
            || self
                .status
                .is_some_and(|s| s >= 500 || matches!(s, 408 | 429))
    }
}

#[derive(Clone)]
struct Http {
    agent: ureq::Agent,
    bearer: Option<String>,
}
impl Http {
    fn new(timeout: u64, key_env: Option<&str>) -> Result<Self, String> {
        let bearer = key_env
            .map(|key| {
                std::env::var(key).map_err(|_| {
                    format!("required credential environment variable {key} is unavailable")
                })
            })
            .transpose()?;
        if bearer.as_ref().is_some_and(|s| s.trim().is_empty()) {
            return Err("configured bearer is empty".into());
        }
        Ok(Self {
            agent: ureq::AgentBuilder::new()
                .timeout(Duration::from_secs(timeout))
                .build(),
            bearer,
        })
    }
    fn request(&self, method: &str, url: &str, body: Option<&Value>) -> Result<Value, Failure> {
        let mut request = self
            .agent
            .request(method, url)
            .set("accept", "application/json");
        if let Some(token) = &self.bearer {
            request = request.set("authorization", &format!("Bearer {token}"));
        }
        let result = if let Some(body) = body {
            request
                .set("content-type", "application/json")
                .send_string(&body.to_string())
        } else {
            request.call()
        };
        let response = result.map_err(|e| match e {
            ureq::Error::Status(code, response) => {
                let message = read_bounded(response.into_reader(), 8192).unwrap_or_default();
                Failure {
                    status: Some(code),
                    message: format!("{method}: HTTP {code}: {message}"),
                }
            }
            // Do not log a complete request URL/header: credentials stay out of logs.
            ureq::Error::Transport(e) => Failure {
                status: None,
                message: format!("{method}: transport failure: {}", e.kind()),
            },
        })?;
        let raw = read_bounded(response.into_reader(), 64 * 1024 * 1024).map_err(|e| Failure {
            status: None,
            message: e,
        })?;
        serde_json::from_str(&raw).map_err(|e| Failure {
            status: None,
            message: format!("{method}: invalid JSON response: {e}"),
        })
    }
    fn array(&self, base: &str, path: &str) -> Result<Vec<Value>, String> {
        self.request("GET", &format!("{base}{path}"), None)
            .map_err(|e| e.to_string())?
            .as_array()
            .cloned()
            .ok_or_else(|| "Board list response is not an array".into())
    }
    fn pages(&self, base: &str, path: &str) -> Result<Vec<Value>, String> {
        let mut rows = Vec::new();
        let sep = if path.contains('?') { '&' } else { '?' };
        loop {
            let value = self
                .request(
                    "GET",
                    &format!("{base}{path}{sep}limit=100&offset={}", rows.len()),
                    None,
                )
                .map_err(|e| e.to_string())?;
            let page = value
                .as_array()
                .ok_or("Board list response is not an array")?;
            let count = page.len();
            rows.extend(page.iter().cloned());
            if count < 100 {
                return Ok(rows);
            }
        }
    }
}
fn read_bounded(reader: impl Read, maximum: u64) -> Result<String, String> {
    let mut text = String::new();
    reader
        .take(maximum + 1)
        .read_to_string(&mut text)
        .map_err(|e| e.to_string())?;
    if text.len() as u64 > maximum {
        return Err("HTTP response exceeds bounded read".into());
    }
    Ok(text)
}
fn encode(value: &str) -> String {
    let mut out = String::new();
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}
fn string<'a>(v: &'a Value, key: &str) -> Result<&'a str, String> {
    v.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing dispatch {key}"))
}
fn number(v: &Value, key: &str) -> Result<i64, String> {
    v.get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| format!("missing dispatch {key}"))
}
fn time() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn terminal(status: &str) -> bool {
    matches!(status, "completed" | "failed" | "stopped" | "cancelled")
}
fn pending(v: &Value) -> bool {
    matches!(
        v["status"].as_str(),
        Some("reserved" | "provisioning" | "launching")
    ) && v["metadata"]["adopted"] != true
}
fn runtime_ready(value: &Value, host: &str) -> bool {
    value["status"] == "ok"
        && value["service"] == "borg-native"
        && value["ready"] == true
        && value["draining"] != true
        && value["host_id"] == host
}
fn session_matches(d: &Value, session: &Value) -> bool {
    if session["id"] != d["session_id"] || session["agent_id"] != d["agent_id"] {
        return false;
    }
    // Old adopted sessions predate placement fields. They are observed only,
    // never recreated. New native dispatches require the complete binding.
    if d["metadata"]["adopted"] == true {
        return true;
    }
    session["host_id"] == d["host_id"]
        && session["generation"] == d["generation"]
        && session["workspace_cwd"] == d["workspace_cwd"]
        && session["task"] == json!(d["task_id"].as_i64().map(|id| format!("task_{id}")))
}
fn native_agent(v: &Value) -> bool {
    v["metadata"]["harness"] == "native"
        && v["retired"] != true
        && v.get("lifecycle_intent")
            .and_then(Value::as_str)
            .unwrap_or("run")
            == "run"
        && !matches!(
            v["metadata"]["lifecycle_intent"].as_str(),
            Some("paused" | "retired")
        )
        && v["metadata"]["native_dispatch"] != false
}
fn actionable(task: &Value) -> bool {
    matches!(task["status"].as_str(), Some("todo" | "in_progress"))
        && task["monitor_exempt"] != true
        && task["effectively_blocked"] != true
        && task["question_blocked"] != true
        && task.get("blocked_on").is_none_or(Value::is_null)
        && task.get("blocked_on_kind").is_none_or(Value::is_null)
        && task["metadata"]["native_dispatch"] != false
}
fn task_attempt(task: &Value) -> u64 {
    task["metadata"]["native_attempt"].as_u64().unwrap_or(1)
}
fn already_dispatched(task: &Value) -> bool {
    let projection = &task["metadata"]["native_dispatch"];
    projection["id"].is_number()
        && task_attempt(task) <= projection["native_attempt"].as_u64().unwrap_or(1)
}
fn task_request(task: &Value, agent: &str) -> Result<Value, String> {
    let id = number(task, "id")?;
    Ok(
        json!({"request_id":format!("task:{id}:attempt:{}",task_attempt(task)),"agent_id":agent,"task_id":id}),
    )
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProvisionHealth {
    config_digest: String,
    ready: bool,
    checked_at: u64,
    last_failure_at: Option<u64>,
    error: Option<String>,
}
impl ProvisionHealth {
    fn load(settings: &Settings) -> Self {
        let config_digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&json!({
            "repo": settings.repo_url, "ref": settings.repo_ref, "ssh_config": settings.git_ssh_config
        })).expect("serializable probe config")));
        let stored = fs::read(
            settings
                .workspace_root
                .join(".native-provisioning-health.json"),
        )
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Self>(&bytes).ok());
        if let Some(previous) = stored.filter(|state| state.config_digest == config_digest) {
            return previous;
        }
        Self {
            config_digest,
            ready: false,
            checked_at: 0,
            last_failure_at: None,
            error: None,
        }
    }
    fn persist(&self, root: &Path) -> Result<(), String> {
        use std::io::Write;
        let temporary = root.join(format!(
            ".native-provisioning-health-{}.tmp",
            std::process::id()
        ));
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temporary)
            .map_err(|e| e.to_string())?;
        file.write_all(&serde_json::to_vec(self).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        fs::rename(temporary, root.join(".native-provisioning-health.json"))
            .map_err(|e| e.to_string())
    }
}

#[derive(Clone)]
struct Worker {
    settings: Settings,
    board_url: String,
    host: String,
    board: Http,
    runtime: Http,
    workspace: Http,
    provision_health: Arc<Mutex<ProvisionHealth>>,
}
impl Worker {
    fn configured() -> Result<Self, String> {
        let settings = crate::config::get()
            .native
            .clone()
            .ok_or("native-worker requires a [native] config section")?;
        if settings.poll_seconds == 0
            || settings.http_timeout_seconds == 0
            || settings.provision_timeout_seconds == 0
            || settings.git_probe_interval_seconds == 0
            || settings.git_probe_timeout_seconds == 0
            || !(5..=3600).contains(&settings.ttl_seconds)
            || settings.capacity > 10000
            || settings.poll_seconds * 2 >= settings.ttl_seconds
        {
            return Err("invalid native poll, timeout, capacity or freshness policy".into());
        }
        if !settings.workspace_root.is_absolute()
            || !settings.workspace_mcp_url.contains("{session_id}")
        {
            return Err("native workspace_root must be absolute; workspace_mcp_url must contain {session_id}".into());
        }
        if settings
            .git_ssh_config
            .as_ref()
            .is_some_and(|p| !p.is_absolute() || !p.is_file())
        {
            return Err(
                "native git_ssh_config must name an existing absolute host SSH config".into(),
            );
        }
        let board = Http::new(
            settings.http_timeout_seconds,
            settings.board_bearer_token_env.as_deref(),
        )?;
        let runtime = Http::new(
            settings.http_timeout_seconds,
            settings.bearer_token_env.as_deref(),
        )?;
        let workspace = Http::new(
            settings.http_timeout_seconds,
            settings.workspace_bearer_token_env.as_deref(),
        )?;
        let provision_health = Arc::new(Mutex::new(ProvisionHealth::load(&settings)));
        Ok(Self {
            provision_health,
            settings,
            board_url: crate::board::Board::base_url().trim_end_matches('/').into(),
            host: crate::this_host(),
            board,
            runtime,
            workspace,
        })
    }
    fn probe_provisioning(&self) -> ProvisionHealth {
        let mut state = self
            .provision_health
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let now = time();
        if state.checked_at > 0
            && now >= state.checked_at
            && now - state.checked_at < self.settings.git_probe_interval_seconds
        {
            return state.clone();
        }
        let mut probe_settings = self.settings.clone();
        probe_settings.provision_timeout_seconds = self.settings.git_probe_timeout_seconds;
        let result = if let Some(repo) = &self.settings.repo_url {
            run_git(
                &probe_settings,
                &["ls-remote", "--", repo, "HEAD"],
                "transport-readiness",
            )
        } else {
            Ok(())
        };
        state.checked_at = time();
        state.ready = result.is_ok();
        state.error = result.err();
        if !state.ready {
            state.last_failure_at = Some(state.checked_at);
        }
        if let Err(error) = state.persist(&self.settings.workspace_root) {
            eprintln!("native provisioning health persistence: {error}");
        }
        state.clone()
    }
    fn publish_provisioning(
        &self,
        state: &std::sync::MutexGuard<'_, ProvisionHealth>,
    ) -> Result<(), String> {
        // Observations only: never overwrite operator capacity or enabled/drain.
        match self.board.request(
            "PATCH",
            &format!("{}/fleet/hosts/{}", self.board_url, encode(&self.host)),
            Some(&json!({"metadata":{"provisioning":&**state}})),
        ) {
            Ok(_) => Ok(()),
            Err(error) if error.status == Some(404) => Ok(()), // initial heartbeat seeds it below
            Err(error) => Err(error.to_string()),
        }
    }
    fn close_provisioning_gate(&self, error: &str) {
        self.close_provisioning_gate_at(error, time());
    }
    fn close_provisioning_gate_at(&self, error: &str, observed_at: u64) {
        let mut state = self
            .provision_health
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        state.ready = false;
        state.checked_at = observed_at;
        state.last_failure_at = Some(state.checked_at);
        state.error = Some(error.into());
        if let Err(error) = state.persist(&self.settings.workspace_root) {
            eprintln!("native provisioning gate persistence: {error}");
        }
        if let Err(error) = self.publish_provisioning(&state) {
            eprintln!("native provisioning gate publication: {error}");
        }
    }
    fn heartbeat(&self) -> Result<(), String> {
        let health = format!(
            "{}/health",
            self.settings
                .runtime_url
                .trim_end_matches('/')
                .trim_end_matches("/api")
        );
        let ready = self
            .runtime
            .request("GET", &health, None)
            .is_ok_and(|v| runtime_ready(&v, &self.host));
        self.probe_provisioning();
        self.publish_heartbeat(ready)
    }
    fn publish_heartbeat(&self, ready: bool) -> Result<(), String> {
        // Keep the observation lock through BOTH writes. A concurrent clone
        // failure cannot publish false/release its reservation and then have an
        // older successful heartbeat overwrite it, even within the same second.
        let provisioning = self
            .provision_health
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        self.publish_provisioning(&provisioning)?;
        let body = json!({"capacity":self.settings.capacity,"runtime_url":self.settings.runtime_url,
            "workspace_root":self.settings.workspace_root,"runtime_ready":ready,"ttl_seconds":self.settings.ttl_seconds,
            "revision":env!("FLEET_BUILD_REV"),"metadata":{"worker":"fleet native-worker","provisioning":&*provisioning}});
        self.board
            .request(
                "POST",
                &format!(
                    "{}/fleet/hosts/{}/heartbeat",
                    self.board_url,
                    encode(&self.host)
                ),
                Some(&body),
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }
    fn dispatches(&self) -> Result<Vec<Value>, String> {
        self.board.pages(
            &self.board_url,
            &format!("/fleet/dispatches?host_id={}", encode(&self.host)),
        )
    }
    fn schedule(&self, only_agent: Option<&str>, apply: bool) -> Result<(), String> {
        let agents = self.board.pages(
            &self.board_url,
            "/agents?retired=false&lifecycle_intent=run",
        )?;
        let eligible: HashSet<&str> = agents
            .iter()
            .filter(|a| native_agent(a))
            .filter_map(|a| a["id"].as_str())
            .filter(|id| only_agent.is_none_or(|wanted| wanted == *id))
            .collect();
        let project = self
            .settings
            .project_id
            .map(|id| format!("&project_id={id}"))
            .unwrap_or_default();
        let mut tasks = Vec::new();
        for status in ["in_progress", "todo"] {
            tasks.extend(
                self.board
                    .array(&self.board_url, &format!("/tasks?status={status}{project}"))?,
            );
        }
        tasks.sort_by_key(|t| {
            (
                match t["priority"].as_str() {
                    Some("high" | "urgent") => 0,
                    Some("low") => 2,
                    _ => 1,
                },
                t["id"].as_i64(),
            )
        });
        for summary in tasks.iter().filter(|t| actionable(t)) {
            if summary["assignee"]
                .as_str()
                .is_none_or(|id| !eligible.contains(id))
            {
                continue;
            }
            // Older REST projections omit BOTH metadata and question blocking.
            // Never infer attempt 1 or runnable work from absent authority fields.
            let full;
            let task = if summary.get("metadata").is_some_and(Value::is_object)
                && summary
                    .get("question_blocked")
                    .is_some_and(Value::is_boolean)
            {
                summary
            } else {
                full = self
                    .board
                    .request(
                        "GET",
                        &format!("{}/tasks/{}", self.board_url, number(summary, "id")?),
                        None,
                    )
                    .map_err(|e| e.to_string())?;
                &full
            };
            if !actionable(task) || already_dispatched(task) {
                continue;
            }
            let Some(agent) = task["assignee"].as_str().filter(|id| eligible.contains(id)) else {
                continue;
            };
            let request = task_request(task, agent)?;
            if !apply {
                println!(
                    "{}",
                    json!({"plan":"native dispatch via Board","request":request})
                );
                continue;
            }
            match self.board.request(
                "POST",
                &format!("{}/fleet/dispatches", self.board_url),
                Some(&request),
            ) {
                Ok(row) => {
                    if row["created"] == true {
                        println!(
                            "{}",
                            json!({"event":"reserved","dispatch_id":row["id"],"session_id":row["session_id"],"host_id":row["host_id"],"task_id":row["task_id"]})
                        );
                    }
                }
                // Capacity exhausted or another scheduler won/intent changed: next
                // tick re-reads Board state; neither warrants local fallback launch.
                Err(e) if matches!(e.status, Some(409 | 503)) => (),
                Err(e) => return Err(format!("reserve task {}: {e}", task["id"])),
            }
        }
        Ok(())
    }
    fn report(
        &self,
        d: &Value,
        status: &str,
        progress: Option<&str>,
        error: Option<&str>,
        metadata: Value,
    ) -> Result<Value, String> {
        self.board
            .request(
                "PATCH",
                &format!("{}/fleet/dispatches/{}", self.board_url, number(d, "id")?),
                Some(&json!({
            "host_id":self.host,"generation":d["generation"],"version":d["version"],"status":status,
            "progress":progress,"error":error,"metadata":metadata})),
            )
            .map_err(|e| e.to_string())
    }
    fn observe(&self, d: &Value) -> Result<bool, String> {
        if d["status"] == "cancelled"
            || (d["status"] == "failed" && d["metadata"]["failure_stage"] == "provisioning")
        {
            return Ok(true); // no engine was created; keep the original repair evidence
        }
        let url = format!(
            "{}/sessions/{}",
            string(d, "runtime_url")?.trim_end_matches('/'),
            encode(string(d, "session_id")?)
        );
        match self.runtime.request("GET", &url, None) {
            Ok(session) => {
                if !session_matches(d, &session) {
                    self.report(
                        d,
                        "needs_resolution",
                        None,
                        Some("native session identity/task/host/generation/workspace does not match reservation"),
                        json!({"observed_at":time()}),
                    )?;
                    return Ok(true);
                }
                let status = string(&session, "status")?;
                if !matches!(
                    status,
                    "ready"
                        | "running"
                        | "retrying"
                        | "waiting"
                        | "stopping"
                        | "stopped"
                        | "completed"
                        | "failed"
                        | "needs_resolution"
                ) {
                    return Err(format!("unknown native session status {status}"));
                }
                self.report(d,status,session["progress"].as_str(),session["error"].as_str(),json!({
                    "observed_at":time(),"runtime":{"updated_at":session["updated_at"],"phase":session["phase"],
                        "step":session["step"],"last_progress_at":session["last_progress_at"],"driver_active":session.get("driver_running").or_else(||session.get("driver_active")).cloned().unwrap_or(Value::Null)},
                    "fleet_revision":env!("FLEET_BUILD_REV"),"launch_retry_at":null,"held_reason":null}))?;
                Ok(true)
            }
            Err(e) if e.status == Some(404) && pending(d) => Ok(false),
            Err(e) => {
                // Observation failure is NOT terminal session failure. Inference may
                // still be live; leave the placement held for owned reconciliation.
                let observation_error = format!("runtime observation unavailable: {e}");
                let error = d["error"]
                    .as_str()
                    .filter(|message| !message.starts_with("runtime observation unavailable:"))
                    .unwrap_or(&observation_error);
                self.report(
                    d, string(d, "status")?, d["progress"].as_str(), Some(error),
                    json!({"observation_error_at":time(),"observation_error":observation_error,"fleet_revision":env!("FLEET_BUILD_REV")}),
                )?;
                Ok(true)
            }
        }
    }
    fn may_start(&self, d: &Value) -> Result<bool, String> {
        if !self
            .provision_health
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .ready
        {
            self.report(
                d,
                string(d, "status")?,
                None,
                None,
                json!({"held_reason":"host default Git transport is not ready"}),
            )?;
            return Ok(false);
        }
        let agent = self
            .board
            .request(
                "GET",
                &format!(
                    "{}/agents/{}",
                    self.board_url,
                    encode(string(d, "agent_id")?)
                ),
                None,
            )
            .map_err(|e| e.to_string())?;
        if !native_agent(&agent) {
            self.report(
                d,
                string(d, "status")?,
                None,
                None,
                json!({"held_reason":"identity is paused, retired or native dispatch disabled"}),
            )?;
            return Ok(false);
        }
        if let Some(task) = d["task_id"].as_i64() {
            let record = self
                .board
                .request("GET", &format!("{}/tasks/{task}", self.board_url), None)
                .map_err(|e| e.to_string())?;
            if terminal(record["status"].as_str().unwrap_or(""))
                || matches!(record["status"].as_str(), Some("done"))
                || record["assignee"] != d["agent_id"]
            {
                self.report(
                    d,
                    "cancelled",
                    Some("task ended or ownership changed before native create"),
                    None,
                    json!({}),
                )?;
                return Ok(false);
            }
            if !actionable(&record) {
                self.report(
                    d,
                    string(d, "status")?,
                    None,
                    None,
                    json!({"held_reason":"task is parked or not actionable"}),
                )?;
                return Ok(false);
            }
        }
        Ok(true)
    }
    fn start(&self, mut d: Value) -> Result<(), String> {
        let result: Result<(), String> = (|| {
            d = self.report(
                &d,
                "provisioning",
                Some("materializing isolated workspace"),
                None,
                json!({"held_reason":null}),
            )?;
            let root = provision(&self.settings, &d)?;
            let workspace_url = self
                .settings
                .workspace_mcp_url
                .replace("{session_id}", string(&d, "session_id")?);
            let probe = json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}});
            match self.workspace.request("POST", &workspace_url, Some(&probe)) {
                Ok(response) if response["result"]["tools"].is_array() => (),
                Ok(_) => {
                    self.retry(&d, "workspace MCP readiness returned no tools")?;
                    return Ok(());
                }
                Err(e) => {
                    self.retry(&d, &format!("workspace MCP unavailable: {e}"))?;
                    return Ok(());
                }
            }
            // Check desired task/identity state again after a potentially long Git
            // clone, before the first runtime side effect.
            if !self.may_start(&d)? {
                return Ok(());
            }
            let payload = create_payload(&self.settings, &d, &root)?;
            d = self.report(
                &d,
                "launching",
                Some("creating idempotent native session"),
                None,
                json!({"workspace_mcp_url":workspace_url}),
            )?;
            let url = format!(
                "{}/sessions",
                string(&d, "runtime_url")?.trim_end_matches('/')
            );
            match self.runtime.request("POST", &url, Some(&payload)) {
                Ok(session) => {
                    if session["id"] != d["session_id"] || session["agent_id"] != d["agent_id"] {
                        self.report(
                            &d,
                            "needs_resolution",
                            None,
                            Some("runtime create returned a different session identity"),
                            json!({}),
                        )?;
                    } else {
                        // Re-read durable state instead of treating an HTTP response
                        // as a license to launch/drive the session a second time.
                        self.observe(&d)?;
                    }
                }
                Err(e) if e.retryable() => {
                    self.retry(
                        &d,
                        &format!("native create outcome needs reconciliation: {e}"),
                    )?;
                }
                Err(e) => {
                    self.report(
                        &d,
                        "needs_resolution",
                        None,
                        Some(&format!("native create rejected: {e}")),
                        json!({}),
                    )?;
                }
            }
            Ok(())
        })();
        if let Err(error) = &result {
            // Provisioning errors cannot hide an owned task. Retain a visible
            // failed dispatch; the recovery monitor creates durable diagnosis.
            if d["status"] == "provisioning" {
                let configured_repo = d["metadata"]["workspace"]["repo"]
                    .as_str()
                    .or(self.settings.repo_url.as_deref());
                if configured_repo == self.settings.repo_url.as_deref()
                    && error.starts_with("workspace git")
                {
                    // Close admission BEFORE releasing the failed reservation.
                    // Native drivers and their completed tool receipts are untouched.
                    self.close_provisioning_gate(error);
                }
                let _ = self.report(
                    &d,
                    "failed",
                    None,
                    Some(error),
                    json!({"failure_stage":"provisioning"}),
                );
            }
        }
        result
    }
    fn retry(&self, d: &Value, error: &str) -> Result<(), String> {
        let attempts = d["metadata"]["launch_attempts"]
            .as_u64()
            .unwrap_or(0)
            .saturating_add(1);
        let delay = 2u64.saturating_pow(attempts.min(6) as u32).min(60);
        self.report(
            d,
            string(d, "status")?,
            d["progress"].as_str(),
            Some(error),
            json!({
            "launch_attempts":attempts,"launch_retry_at":time()+delay}),
        )?;
        Ok(())
    }
    fn reconcile(
        &self,
        jobs: &mut HashMap<i64, JoinHandle<()>>,
        launch: bool,
    ) -> Result<(), String> {
        let finished: Vec<i64> = jobs
            .iter()
            .filter_map(|(id, h)| h.is_finished().then_some(*id))
            .collect();
        for id in finished {
            if jobs.remove(&id).unwrap().join().is_err() {
                eprintln!("native worker job {id} panicked; reservation remains durable");
            }
        }
        let mut rows = self.dispatches()?;
        rows.sort_by_key(|row| terminal(row["status"].as_str().unwrap_or("")));
        for d in rows {
            let id = number(&d, "id")?;
            if jobs.contains_key(&id) {
                continue;
            }
            match self.observe(&d) {
                Ok(false)
                    if launch
                        && d["metadata"]["launch_retry_at"].as_u64().unwrap_or(0) <= time() =>
                {
                    if self.may_start(&d)? {
                        let worker = self.clone();
                        jobs.insert(
                            id,
                            thread::spawn(move || {
                                if let Err(error) = worker.start(d) {
                                    eprintln!("native dispatch {id}: {error}");
                                }
                            }),
                        );
                    }
                }
                Ok(_) => (),
                Err(error) => eprintln!("native dispatch {id}: {error}"),
            }
        }
        Ok(())
    }
}

fn create_payload(settings: &Settings, d: &Value, root: &Path) -> Result<Value, String> {
    Ok(
        json!({"agent_id":string(d,"agent_id")?,"task":d["task_id"].as_i64().map(|id|format!("task_{id}")),
        "input":string(d,"input")?,"session_id":string(d,"session_id")?,"host_id":string(d,"host_id")?,
        "workspace_cwd":root,"workspace_mcp_url":settings.workspace_mcp_url.replace("{session_id}",string(d,"session_id")?),
        "generation":d["generation"],"metadata":{"fleet_dispatch_id":d["id"],"request_id":d["request_id"]}}),
    )
}

pub fn run(once: bool) -> Result<(), String> {
    let worker = Worker::configured()?;
    fs::create_dir_all(&worker.settings.workspace_root).map_err(|e| e.to_string())?;
    if fs::canonicalize(&worker.settings.workspace_root).map_err(|e| e.to_string())?
        != worker.settings.workspace_root
    {
        return Err("workspace_root must be a canonical, non-symlink absolute path".into());
    }
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(
            worker
                .settings
                .workspace_root
                .join(".fleet-native-worker.lock"),
        )
        .map_err(|e| e.to_string())?;
    lock.try_lock()
        .map_err(|_| "another native Fleet worker owns this workspace root")?;
    let mut jobs = HashMap::new();
    loop {
        let result: Result<(), String> = (|| {
            worker.heartbeat()?;
            worker.reconcile(&mut jobs, true)?;
            worker.schedule(None, true)?;
            worker.reconcile(&mut jobs, true)
        })();
        if let Err(error) = result {
            eprintln!("native worker {}: {error}", worker.host);
            if once {
                return Err(error);
            }
        }
        if once {
            for (id, job) in jobs.drain() {
                if job.join().is_err() {
                    return Err(format!("native dispatch {id} panicked"));
                }
            }
            worker.reconcile(&mut jobs, false)?;
            return Ok(());
        }
        thread::sleep(Duration::from_secs(worker.settings.poll_seconds));
    }
}

pub fn spin_up(agent: &str, apply: bool) -> Result<(), String> {
    // This is a broker request, not the legacy tmux/mandate materializer. A
    // persistent worker on the selected host performs the actual native launch.
    Worker::configured()?.schedule(Some(agent), apply)
}

fn safe_uuid(id: &str) -> bool {
    id.len() == 36
        && id.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit() && !b.is_ascii_uppercase()
            }
        })
}
fn provision(settings: &Settings, d: &Value) -> Result<PathBuf, String> {
    let id = string(d, "session_id")?;
    if !safe_uuid(id) {
        return Err("invalid workspace session UUID".into());
    }
    let target = settings.workspace_root.join(id);
    if target.to_str() != Some(string(d, "workspace_cwd")?) {
        return Err("dispatch cwd does not match this host's isolated workspace root".into());
    }
    let spec = &d["metadata"]["workspace"];
    let repo = spec
        .get("repo")
        .and_then(Value::as_str)
        .or(settings.repo_url.as_deref());
    let reference = spec
        .get("ref")
        .and_then(Value::as_str)
        .unwrap_or(&settings.repo_ref);
    if reference.is_empty() || reference.starts_with('-') || reference.chars().any(char::is_control)
    {
        return Err("invalid workspace ref".into());
    }
    let identity = json!({"session_id":id,"dispatch_id":d["id"],"agent_id":d["agent_id"],"repo":repo,"ref":reference,"git_ssh_config":settings.git_ssh_config});
    let manifest = ".fleet-workspace.json";
    if target.exists() || fs::symlink_metadata(&target).is_ok() {
        if fs::symlink_metadata(&target)
            .map_err(|e| e.to_string())?
            .file_type()
            .is_symlink()
        {
            return Err("workspace target is a symlink".into());
        }
        if fs::symlink_metadata(target.join(manifest))
            .map_err(|e| format!("existing workspace has no valid ownership manifest: {e}"))?
            .file_type()
            .is_symlink()
        {
            return Err("workspace ownership manifest is a symlink".into());
        }
        let existing: Value = serde_json::from_str(
            &fs::read_to_string(target.join(manifest))
                .map_err(|e| format!("existing workspace has no valid ownership manifest: {e}"))?,
        )
        .map_err(|e| e.to_string())?;
        if existing != identity {
            return Err(
                "existing workspace belongs to a different dispatch/config; refusing to reset it"
                    .into(),
            );
        }
        return Ok(target);
    }
    fs::create_dir_all(&settings.workspace_root).map_err(|e| e.to_string())?;
    let staging = settings.workspace_root.join(format!(".{id}.provisioning"));
    // Partial clones are never reused blindly or deleted while a previous Git
    // process might still own them. An interrupted provisioning is a repairable
    // failure with evidence, not permission to discard a working tree.
    if staging.exists() || fs::symlink_metadata(&staging).is_ok() {
        return Err(format!(
            "incomplete workspace staging exists at {}; reconcile before retry",
            staging.display()
        ));
    }
    if let Some(repo) = repo {
        if repo.is_empty() || repo.starts_with('-') || repo.chars().any(char::is_control) {
            return Err("invalid workspace repo".into());
        }
        run_git(
            settings,
            &[
                "clone",
                "--no-local",
                "--no-hardlinks",
                "--no-checkout",
                "--",
                repo,
                staging.to_str().ok_or("workspace path is not UTF-8")?,
            ],
            id,
        )?;
        if let Some(command) = git_ssh_command(settings)? {
            // The bootstrap checkout's core.sshCommand does not propagate into
            // service clones. Preserve the configured route for session pushes.
            run_git(
                settings,
                &[
                    "-C",
                    staging.to_str().unwrap(),
                    "config",
                    "core.sshCommand",
                    &command,
                ],
                id,
            )?;
        }
        let commit = resolve_checkout_ref(&staging, reference)?;
        let branch = format!("native/session-{id}");
        run_git(
            settings,
            &[
                "-C",
                staging.to_str().unwrap(),
                "checkout",
                "-b",
                &branch,
                &commit,
            ],
            id,
        )?;
        // Per-session marker is operational metadata, not part of source commits.
        let exclude = staging.join(".git/info/exclude");
        use std::io::Write;
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(exclude)
            .map_err(|e| e.to_string())?;
        writeln!(f, "\n/{manifest}").map_err(|e| e.to_string())?;
    } else {
        fs::create_dir(&staging).map_err(|e| e.to_string())?;
    }
    {
        use std::io::Write;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(staging.join(manifest))
            .map_err(|e| format!("workspace ownership marker must not preexist: {e}"))?;
        file.write_all(&serde_json::to_vec_pretty(&identity).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
    }
    fs::rename(&staging, &target).map_err(|e| e.to_string())?;
    Ok(target)
}
fn resolve_checkout_ref(directory: &Path, reference: &str) -> Result<String, String> {
    // A bare deployment repo may advertise an unborn default HEAD even though
    // origin/main exists. Resolve the requested ref explicitly instead of
    // relying on clone to create a local branch with that name.
    let remote = format!(
        "refs/remotes/origin/{}",
        reference.strip_prefix("refs/heads/").unwrap_or(reference)
    );
    for candidate in [reference, remote.as_str()] {
        let output = Command::new("git")
            .arg("-C")
            .arg(directory)
            .args([
                "rev-parse",
                "--verify",
                "--quiet",
                "--end-of-options",
                &format!("{candidate}^{{commit}}"),
            ])
            .stdin(Stdio::null())
            .output()
            .map_err(|e| e.to_string())?;
        if output.status.success() {
            let commit = String::from_utf8(output.stdout)
                .map_err(|e| e.to_string())?
                .trim()
                .to_owned();
            if matches!(commit.len(), 40 | 64) && commit.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Ok(commit);
            }
        }
    }
    Err(format!(
        "workspace ref {reference:?} is not a fetched commit or origin branch"
    ))
}
fn git_ssh_command(settings: &Settings) -> Result<Option<String>, String> {
    settings
        .git_ssh_config
        .as_ref()
        .map(|p| {
            let path = p.to_str().ok_or("SSH config path is not UTF-8")?;
            if !p.is_absolute() || path.chars().any(char::is_control) {
                return Err("SSH config path must be absolute without control characters".into());
            }
            Ok(format!(
                "ssh -F '{}' -o BatchMode=yes",
                path.replace('\'', "'\\''")
            ))
        })
        .transpose()
}
fn run_git(settings: &Settings, args: &[&str], id: &str) -> Result<(), String> {
    let log = settings.workspace_root.join(format!(".{id}.provision.log"));
    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)
        .map_err(|e| e.to_string())?;
    let stderr = stdout.try_clone().map_err(|e| e.to_string())?;
    let mut cmd = Command::new("git");
    cmd.args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr);
    if let Some(command) = git_ssh_command(settings)? {
        cmd.env("GIT_SSH_COMMAND", command);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    let mut child = cmd.spawn().map_err(|e| format!("cannot start git: {e}"))?;
    let deadline = Instant::now() + Duration::from_secs(settings.provision_timeout_seconds);
    loop {
        match child.try_wait().map_err(|e| e.to_string())? {
            Some(status) if status.success() => return Ok(()),
            Some(status) => {
                return Err(format!(
                    "workspace git exited {status}; inspect {}",
                    log.display()
                ));
            }
            None if Instant::now() >= deadline => {
                #[cfg(unix)]
                {
                    let _ = Command::new("/bin/kill")
                        .args(["-KILL", "--", &format!("-{}", child.id())])
                        .status();
                }
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "workspace git timed out; retained staging and log {} for repair",
                    log.display()
                ));
            }
            None => thread::sleep(Duration::from_millis(100)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "borg-fleet-test-{}-{}-{}",
                std::process::id(),
                time(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn settings(root: &Path) -> Settings {
        Settings {
            runtime_url: "http://127.0.0.1:8787/api".into(),
            workspace_root: root.into(),
            workspace_mcp_url: workspace_url(),
            capacity: 4,
            poll_seconds: 5,
            ttl_seconds: 60,
            project_id: Some(1),
            repo_url: None,
            git_ssh_config: None,
            repo_ref: "HEAD".into(),
            bearer_token_env: None,
            board_bearer_token_env: None,
            workspace_bearer_token_env: None,
            http_timeout_seconds: 15,
            provision_timeout_seconds: 30,
            git_probe_interval_seconds: 30,
            git_probe_timeout_seconds: 10,
        }
    }
    fn dispatch(root: &Path, n: u32) -> Value {
        let id = format!("00000000-0000-4000-8000-{n:012}");
        json!({"id":n,"request_id":format!("task:{n}:attempt:1"),"session_id":id,"agent_id":"engineer","task_id":n,
            "host_id":"lab","runtime_url":"http://127.0.0.1:8787/api","workspace_cwd":root.join(&id),
            "generation":1,"version":1,"status":"reserved","input":"execute assigned work","metadata":{"workspace":{}}})
    }
    fn git(args: &[&str]) {
        let output = Command::new("git").args(args).output().unwrap();
        assert!(
            output.status.success(),
            "git {:?}: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    #[test]
    fn stale_success_cannot_publish_after_failure_even_with_equal_timestamps() {
        use std::io::{BufRead, BufReader, Read, Write};
        use std::net::TcpListener;
        use std::sync::mpsc;
        let tmp = Temp::new();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let writes = Arc::new(Mutex::new(Vec::<bool>::new()));
        let release_server = release.clone();
        let writes_server = writes.clone();
        let server = thread::spawn(move || {
            let mut handlers = Vec::new();
            for index in 0..3 {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let release = release_server.clone();
                let writes = writes_server.clone();
                let entered = entered_tx.clone();
                handlers.push(thread::spawn(move || {
                    let mut reader=BufReader::new(stream.try_clone().unwrap());
                    let mut first=String::new();reader.read_line(&mut first).unwrap();
                    let mut length=0;
                    loop {
                        let mut header=String::new();reader.read_line(&mut header).unwrap();
                        if header=="\r\n" { break; }
                        if let Some((name,value))=header.split_once(':') {
                            if name.eq_ignore_ascii_case("content-length") { length=value.trim().parse::<usize>().unwrap(); }
                        }
                    }
                    let mut body=vec![0;length];reader.read_exact(&mut body).unwrap();
                    let body:Value=serde_json::from_slice(&body).unwrap();
                    assert!(body.get("enabled").is_none(),"observation must not change operator intent");
                    if index==0 {
                        entered.send(()).unwrap();
                        let (lock,wake)=&*release;
                        let mut allowed=lock.lock().unwrap();
                        while !*allowed { allowed=wake.wait(allowed).unwrap(); }
                    }
                    if first.starts_with("PATCH") {
                        assert_eq!(body["metadata"]["provisioning"]["checked_at"],123);
                        writes.lock().unwrap().push(body["metadata"]["provisioning"]["ready"].as_bool().unwrap());
                    }
                    stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").unwrap();
                }));
            }
            for handler in handlers {
                handler.join().unwrap();
            }
        });
        let cfg = settings(&tmp.0);
        let mut state = ProvisionHealth::load(&cfg);
        state.ready = true;
        state.checked_at = 123;
        let client = Http::new(3, None).unwrap();
        let worker = Worker {
            settings: cfg,
            board_url: format!("http://{address}/api"),
            host: "lab".into(),
            board: client.clone(),
            runtime: client.clone(),
            workspace: client,
            provision_health: Arc::new(Mutex::new(state)),
        };
        let heartbeat_worker = worker.clone();
        let heartbeat = thread::spawn(move || heartbeat_worker.publish_heartbeat(true).unwrap());
        entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        let (finished_tx, finished_rx) = mpsc::channel();
        let failing_worker = worker.clone();
        let failure = thread::spawn(move || {
            failing_worker.close_provisioning_gate_at("known transport failure", 123);
            finished_tx.send(()).unwrap();
        });
        // A concurrent HTTP handler could accept the failure now; only the
        // shared observation lock prevents a false release before old success.
        assert!(
            finished_rx
                .recv_timeout(Duration::from_millis(150))
                .is_err()
        );
        {
            let (lock, wake) = &*release;
            *lock.lock().unwrap() = true;
            wake.notify_all();
        }
        heartbeat.join().unwrap();
        failure.join().unwrap();
        server.join().unwrap();
        assert_eq!(*writes.lock().unwrap(), vec![true, false]);
        assert!(!worker.provision_health.lock().unwrap().ready);
        assert!(
            !ProvisionHealth::load(&worker.settings).ready,
            "restart must retain the failed observation"
        );
    }

    #[test]
    fn readiness_requires_native_protocol_identity_not_just_an_open_http_port() {
        let mut health = json!({"status":"ok","service":"borg-native","host_id":"lab","ready":true,"draining":false});
        assert!(runtime_ready(&health, "lab"));
        assert!(!runtime_ready(&health, "other"));
        health["ready"] = json!(false);
        assert!(!runtime_ready(&health, "lab"));
        health["ready"] = json!(true);
        health["draining"] = json!(true);
        assert!(!runtime_ready(&health, "lab"));
        assert!(!runtime_ready(
            &json!({"status":"ok","service":"borg-native"}),
            "lab"
        ));
    }
    #[test]
    fn observed_session_must_match_task_host_generation_and_workspace() {
        let mut d = dispatch(Path::new("/workspaces"), 1);
        let good = json!({"id":d["session_id"],"agent_id":d["agent_id"],"task":"task_1","host_id":d["host_id"],
            "generation":d["generation"],"workspace_cwd":d["workspace_cwd"]});
        assert!(session_matches(&d, &good));
        for key in [
            "host_id",
            "generation",
            "workspace_cwd",
            "task",
            "id",
            "agent_id",
        ] {
            let mut wrong = good.clone();
            wrong[key] = json!("mismatch");
            assert!(!session_matches(&d, &wrong), "{key}");
        }
        d["metadata"]["adopted"] = json!(true);
        assert!(session_matches(
            &d,
            &json!({"id":d["session_id"],"agent_id":d["agent_id"]})
        ));
    }
    #[test]
    fn native_harness_is_distinct_from_legacy_roster_marker_and_respects_intent() {
        let a = json!({"metadata":{"native":true,"harness":"codex"}});
        assert!(!native_agent(&a));
        let mut a = json!({"id":"engineer","metadata":{"harness":"native","host":"elsewhere"},"lifecycle_intent":"run"});
        assert!(native_agent(&a));
        assert!(!crate::watchdog_manages_agent(
            Some(&a["metadata"]),
            "elsewhere",
            false
        ));
        a["lifecycle_intent"] = json!("paused");
        assert!(!native_agent(&a));
        a["lifecycle_intent"] = json!("run");
        a["retired"] = json!(true);
        assert!(!native_agent(&a));
    }
    #[test]
    fn actionable_tasks_respect_external_and_question_waits() {
        let mut t = json!({"id":1,"status":"todo","metadata":{}});
        assert!(actionable(&t));
        t["blocked_on_kind"] = json!("external");
        assert!(!actionable(&t));
        t["blocked_on_kind"] = Value::Null;
        t["question_blocked"] = json!(true);
        assert!(!actionable(&t));
        t["question_blocked"] = json!(false);
        t["monitor_exempt"] = json!(true);
        assert!(!actionable(&t));
        t["monitor_exempt"] = json!(false);
        t["status"] = json!("icebox");
        assert!(!actionable(&t));
    }
    #[test]
    fn ended_attempt_needs_explicit_new_intent_not_an_automatic_storm() {
        let mut t = json!({"id":3,"status":"in_progress","metadata":{"native_dispatch":{"id":4,"status":"failed","native_attempt":1}}});
        assert!(already_dispatched(&t));
        assert_eq!(
            task_request(&t, "engineer").unwrap()["request_id"],
            "task:3:attempt:1"
        );
        t["metadata"]["native_attempt"] = json!(2);
        assert!(!already_dispatched(&t));
        assert_eq!(
            task_request(&t, "engineer").unwrap()["request_id"],
            "task:3:attempt:2"
        );
        assert!(!terminal("retrying"));
        assert!(!terminal("needs_resolution"));
    }
    #[test]
    fn retry_payload_is_identical_after_observation_and_does_not_override_profile() {
        let tmp = Temp::new();
        let cfg = settings(&tmp.0);
        let mut d = dispatch(&tmp.0, 1);
        let first =
            create_payload(&cfg, &d, Path::new(d["workspace_cwd"].as_str().unwrap())).unwrap();
        d["status"] = json!("launching");
        d["version"] = json!(5);
        d["progress"] = json!("lost response");
        let second =
            create_payload(&cfg, &d, Path::new(d["workspace_cwd"].as_str().unwrap())).unwrap();
        assert_eq!(first, second);
        assert!(first.get("model").is_none());
        assert!(first.get("max_turns").is_none());
        assert!(first.get("max_output_tokens").is_none());
        assert_eq!(first["task"], "task_1");
        assert!(
            first["workspace_mcp_url"]
                .as_str()
                .unwrap()
                .ends_with(d["session_id"].as_str().unwrap())
        );
    }
    #[test]
    fn empty_workspaces_are_isolated_idempotent_and_never_reset_user_edits() {
        let tmp = Temp::new();
        let cfg = settings(&tmp.0);
        let a = dispatch(&tmp.0, 1);
        let b = dispatch(&tmp.0, 2);
        let pa = provision(&cfg, &a).unwrap();
        let pb = provision(&cfg, &b).unwrap();
        assert_ne!(pa, pb);
        fs::write(pa.join("only-a"), "completed tool result").unwrap();
        assert!(!pb.join("only-a").exists());
        assert_eq!(provision(&cfg, &a).unwrap(), pa);
        assert_eq!(
            fs::read_to_string(pa.join("only-a")).unwrap(),
            "completed tool result"
        );
        let mut wrong = a;
        wrong["id"] = json!(99);
        assert!(
            provision(&cfg, &wrong)
                .unwrap_err()
                .contains("different dispatch")
        );
    }
    #[test]
    fn git_sessions_have_independent_branches_indexes_and_files() {
        let tmp = Temp::new();
        let source = tmp.0.join("source");
        let root = tmp.0.join("workspaces");
        fs::create_dir(&source).unwrap();
        git(&["init", source.to_str().unwrap()]);
        fs::write(source.join("source.txt"), "base").unwrap();
        git(&["-C", source.to_str().unwrap(), "add", "source.txt"]);
        git(&[
            "-C",
            source.to_str().unwrap(),
            "-c",
            "user.email=fleet-test@example.invalid",
            "-c",
            "user.name=Fleet Test",
            "commit",
            "-m",
            "base",
        ]);
        let mut cfg = settings(&root);
        cfg.repo_url = Some(source.to_str().unwrap().into());
        let ssh_config = tmp.0.join("ssh config");
        fs::write(&ssh_config, "Host *\n  BatchMode yes\n").unwrap();
        cfg.git_ssh_config = Some(ssh_config.clone());
        let a = provision(&cfg, &dispatch(&root, 1)).unwrap();
        let b = provision(&cfg, &dispatch(&root, 2)).unwrap();
        fs::write(a.join("source.txt"), "independent edit").unwrap();
        assert_eq!(fs::read_to_string(b.join("source.txt")).unwrap(), "base");
        assert_eq!(
            fs::read_to_string(source.join("source.txt")).unwrap(),
            "base"
        );
        assert_ne!(
            fs::read_to_string(a.join(".git/HEAD")).unwrap(),
            fs::read_to_string(b.join(".git/HEAD")).unwrap()
        );
        assert!(a.join(".git").is_dir());
        assert!(b.join(".git").is_dir());
        let configured = Command::new("git")
            .args([
                "-C",
                a.to_str().unwrap(),
                "config",
                "--local",
                "core.sshCommand",
            ])
            .output()
            .unwrap();
        assert_eq!(
            String::from_utf8(configured.stdout).unwrap().trim(),
            git_ssh_command(&cfg).unwrap().unwrap()
        );
        let status = Command::new("git")
            .args(["-C", b.to_str().unwrap(), "status", "--porcelain"])
            .output()
            .unwrap();
        assert!(
            status.stdout.is_empty(),
            "ownership manifest must not dirty source commits"
        );
    }
    #[test]
    fn checkout_explicit_main_even_when_bare_remote_head_is_unborn() {
        let tmp = Temp::new();
        let source = tmp.0.join("source");
        let bare = tmp.0.join("bare");
        let root = tmp.0.join("workspaces");
        git(&["init", "-b", "main", source.to_str().unwrap()]);
        fs::write(source.join("proof"), "requested main branch").unwrap();
        git(&["-C", source.to_str().unwrap(), "add", "proof"]);
        git(&[
            "-C",
            source.to_str().unwrap(),
            "-c",
            "user.email=fleet-test@example.invalid",
            "-c",
            "user.name=Fleet Test",
            "commit",
            "-m",
            "base",
        ]);
        git(&[
            "clone",
            "--bare",
            source.to_str().unwrap(),
            bare.to_str().unwrap(),
        ]);
        git(&[
            "-C",
            bare.to_str().unwrap(),
            "symbolic-ref",
            "HEAD",
            "refs/heads/not-created",
        ]);
        let mut cfg = settings(&root);
        cfg.repo_url = Some(bare.to_str().unwrap().into());
        cfg.repo_ref = "main".into();
        let target = provision(&cfg, &dispatch(&root, 1)).unwrap();
        assert_eq!(
            fs::read_to_string(target.join("proof")).unwrap(),
            "requested main branch"
        );
        assert!(resolve_checkout_ref(&target, "refs/heads/main").is_ok());
    }
    #[test]
    fn workspace_refuses_traversal_symlink_and_unowned_partial_tree() {
        let tmp = Temp::new();
        let cfg = settings(&tmp.0);
        let mut d = dispatch(&tmp.0, 1);
        d["session_id"] = json!("../../elsewhere");
        assert!(provision(&cfg, &d).is_err());
        d = dispatch(&tmp.0, 1);
        d["workspace_cwd"] = json!("/different/place");
        assert!(provision(&cfg, &d).is_err());
        d = dispatch(&tmp.0, 1);
        let target = PathBuf::from(d["workspace_cwd"].as_str().unwrap());
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&tmp.0, &target).unwrap();
            assert!(provision(&cfg, &d).unwrap_err().contains("symlink"));
            fs::remove_file(&target).unwrap();
        }
        fs::create_dir(&target).unwrap();
        fs::write(target.join("valuable"), "keep").unwrap();
        assert!(
            provision(&cfg, &d)
                .unwrap_err()
                .contains("ownership manifest")
        );
        assert_eq!(fs::read_to_string(target.join("valuable")).unwrap(), "keep");
    }
    #[test]
    fn startup_adoption_never_recreates_an_unknown_existing_session() {
        let mut d = dispatch(Path::new("/ws"), 1);
        assert!(pending(&d));
        d["metadata"]["adopted"] = json!(true);
        assert!(!pending(&d));
        d["metadata"]["adopted"] = json!(false);
        d["status"] = json!("needs_resolution");
        assert!(!pending(&d));
        d["status"] = json!("failed");
        assert!(!pending(&d));
    }
    #[test]
    fn one_worker_lock_and_url_encoding() {
        let tmp = Temp::new();
        let path = tmp.0.join("lock");
        let a = File::create(&path).unwrap();
        a.try_lock().unwrap();
        let b = File::create(&path).unwrap();
        assert!(b.try_lock().is_err());
        // Other parallel Git tests may have a transient fork-before-exec copy
        // of this descriptor. Explicit unlock tests the lock contract without
        // depending on another thread's exec scheduling.
        a.unlock().unwrap();
        drop(a);
        assert!(b.try_lock().is_ok());
        assert_eq!(
            encode("agent:one/with spaces?"),
            "agent%3Aone%2Fwith%20spaces%3F"
        );
    }
    #[test]
    fn metadata_config_has_no_session_turn_or_model_limits() {
        let cfg: crate::config::Config = toml::from_str(
            r#"
            board_api="http://127.0.0.1:8079/api"
            host="agent-lab-4"
            [native]
            runtime_url="http://127.0.0.1:17874/api"
            workspace_root="/srv/borg/workspaces"
            project_id=1
            capacity=8
            poll_seconds=5
        "#,
        )
        .unwrap();
        let native = cfg.native.unwrap();
        assert_eq!(native.capacity, 8);
        assert_eq!(
            native.workspace_mcp_url,
            "http://127.0.0.1:8791/mcp/{session_id}"
        );
        assert_eq!(native.ttl_seconds, 60);
    }
}
