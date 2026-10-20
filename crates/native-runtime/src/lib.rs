//! Small durable Responses executor. Agent configuration is reusable metadata;
//! each session has independent transcript, control state and tool results.
pub mod mcp;
pub mod model;
pub mod provider;
mod retry;
mod store;
use anyhow::{Context, Result, bail};
pub use model::*;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use store::{Store, ToolRecord};
use tokio::sync::Notify;

#[derive(Clone)]
pub struct Runtime {
    inner: Arc<Inner>,
}
struct Inner {
    config_path: PathBuf,
    store: Store,
    provider: Arc<dyn Provider>,
    tools: Arc<dyn ToolExecutor>,
    running: Mutex<HashSet<String>>,
    run_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    stops: Mutex<HashMap<String, Arc<Notify>>>,
    _database_lock: File,
}
struct RunGuard {
    inner: Arc<Inner>,
    id: String,
}
impl Drop for RunGuard {
    fn drop(&mut self) {
        self.inner.running.lock().unwrap().remove(&self.id);
    }
}
impl Runtime {
    pub fn open(config_path: impl AsRef<Path>) -> Result<Self> {
        Self::with_backends(
            config_path,
            Arc::new(provider::OpenAiProvider::new()?),
            Arc::new(mcp::McpToolExecutor::new()?),
        )
    }
    pub fn with_backends(
        config_path: impl AsRef<Path>,
        provider: Arc<dyn Provider>,
        tools: Arc<dyn ToolExecutor>,
    ) -> Result<Self> {
        let config_path = config_path.as_ref().to_path_buf();
        let config = read_config(&config_path)?;
        let database = if config.database_path.is_absolute() {
            config.database_path.clone()
        } else {
            config_path
                .parent()
                .unwrap_or(Path::new("."))
                .join(&config.database_path)
        };
        if let Some(parent) = database.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let lock_path = database.with_extension("runtime-lock");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)?;
        lock.try_lock()
            .context("another runtime owns this database")?;
        let store = Store::open(&database)?;
        store.recover()?;
        Ok(Self {
            inner: Arc::new(Inner {
                config_path,
                store,
                provider,
                tools,
                running: Mutex::new(HashSet::new()),
                run_locks: Mutex::new(HashMap::new()),
                stops: Mutex::new(HashMap::new()),
                _database_lock: lock,
            }),
        })
    }
    pub fn agents(&self) -> Result<Vec<AgentConfig>> {
        Ok(read_config(&self.inner.config_path)?.agents)
    }
    fn agent(&self, id: &str) -> Result<AgentConfig> {
        self.agents()?
            .into_iter()
            .find(|a| a.id == id)
            .with_context(|| format!("agent not found: {id}"))
    }
    pub fn create_session(
        &self,
        agent_id: &str,
        task: Option<String>,
        input: &str,
    ) -> Result<Session> {
        self.agent(agent_id)?;
        if input.trim().is_empty() {
            bail!("input must not be empty")
        }
        self.inner.store.create(agent_id, task, input)
    }
    pub fn list_sessions(&self) -> Result<Vec<Session>> {
        self.inner.store.list()
    }
    pub fn get_session(&self, id: &str) -> Result<Session> {
        self.inner.store.get(id)
    }
    pub fn transcript(&self, id: &str) -> Result<Vec<TranscriptEntry>> {
        self.inner.store.transcript(id)
    }
    pub fn tool_calls(&self, id: &str) -> Result<Vec<ToolCall>> {
        Ok(self
            .inner
            .store
            .tools(id)?
            .into_iter()
            .map(|r| r.call)
            .collect())
    }
    pub fn steer(&self, id: &str, text: &str) -> Result<Session> {
        if text.trim().is_empty() {
            bail!("input must not be empty")
        }
        self.inner.store.steer(id, text)
    }
    fn notify(&self, id: &str) -> Arc<Notify> {
        self.inner
            .stops
            .lock()
            .unwrap()
            .entry(id.into())
            .or_default()
            .clone()
    }
    pub fn stop(&self, id: &str) -> Result<Session> {
        let running = self.inner.running.lock().unwrap();
        let active = running.contains(id);
        let s = self.inner.store.stop(id, active)?;
        if active {
            self.notify(id).notify_one();
        }
        Ok(s)
    }
    pub fn resume(&self, id: &str) -> Result<Session> {
        let running = self.inner.running.lock().unwrap();
        if running.contains(id) {
            bail!("session is already running; wait for it to stop before resuming")
        }
        // Replace the previous run's stop signal, including any unconsumed permit.
        self.inner
            .stops
            .lock()
            .unwrap()
            .insert(id.into(), Arc::new(Notify::new()));
        self.inner.store.resume(id)
    }
    pub fn resolve_tool(&self, id: &str, call_id: &str, result: Value) -> Result<Session> {
        let running = self.inner.running.lock().unwrap();
        if running.contains(id) {
            bail!("session is already running")
        }
        self.inner.store.tool_result(id, call_id, result, true)
    }
    pub async fn run(&self, id: &str) -> Result<Session> {
        self.get_session(id)?;
        let lock = self
            .inner
            .run_locks
            .lock()
            .unwrap()
            .entry(id.into())
            .or_default()
            .clone();
        // Queue event-driven wakes behind the current run. A steer arriving during
        // terminal commit must not leave a Ready session with no driver.
        let _exclusive = lock.lock().await;
        self.inner.running.lock().unwrap().insert(id.into());
        let _guard = RunGuard {
            inner: self.inner.clone(),
            id: id.into(),
        };
        let s = self.get_session(id)?;
        if matches!(
            s.status,
            SessionStatus::Completed
                | SessionStatus::Failed
                | SessionStatus::NeedsResolution
                | SessionStatus::Stopped
                | SessionStatus::Waiting
        ) {
            return Ok(s);
        }
        let result = self.run_inner(id).await;
        if let Err(error) = &result {
            // Infrastructure errors must be visible even if the HTTP task is detached.
            // If a tool outcome is unknown, preserve that state.
            if let Ok(s) = self.get_session(id) {
                let mut unresolved = s.status == SessionStatus::NeedsResolution;
                if let Ok(records) = self.inner.store.tools(id) {
                    for record in records {
                        unresolved |= matches!(record.call.status.as_str(), "running" | "unknown");
                        if record.call.status == "running" {
                            let _ = self.inner.store.tool_state(id, &record.call.call_id, "unknown", Some("Tool dispatch began but its result could not be durably recorded".into()));
                        }
                    }
                }
                let status = if unresolved {
                    SessionStatus::NeedsResolution
                } else {
                    SessionStatus::Failed
                };
                let _ = self.inner.store.state(
                    id,
                    status,
                    "Runtime error; inspect the error and resume",
                    Some(error.to_string()),
                );
            }
        }
        result
    }
    async fn run_inner(&self, id: &str) -> Result<Session> {
        let stop = self.notify(id);
        let mut turns = 0u64;
        loop {
            if self.inner.store.stopped(id)? {
                return self
                    .inner
                    .store
                    .state(id, SessionStatus::Stopped, "Stopped", None);
            }
            let session = self.get_session(id)?;
            let config = self.agent(&session.agent_id)?;
            // Pending intents were durably committed by a completed model response.
            // Resume these before another model call; Unknown intents are never sent again.
            let intents = self.inner.store.tools(id)?;
            // A cancelled run future may leave a dispatch marker without restarting
            // the process. The exclusive run guard proves its old executor is gone.
            for record in intents.iter().filter(|r| r.call.status == "running") {
                self.inner.store.tool_state(
                    id,
                    &record.call.call_id,
                    "unknown",
                    Some("Prior run ended before the original tool result was recorded".into()),
                )?;
            }
            if intents
                .iter()
                .any(|r| r.call.status == "unknown" || r.call.status == "running")
            {
                return self.inner.store.state(
                    id,
                    SessionStatus::NeedsResolution,
                    "An original tool result must be resolved",
                    Some("unresolved tool outcome".into()),
                );
            }
            for record in intents.into_iter().filter(|r| r.call.status == "pending") {
                if self.inner.store.stopped(id)? {
                    return self.inner.store.state(
                        id,
                        SessionStatus::Stopped,
                        "Stopped before tool dispatch",
                        None,
                    );
                }
                let call = &record.call;
                self.inner.store.state(
                    id,
                    SessionStatus::Running,
                    &format!("Calling {}", call.name),
                    None,
                )?;
                self.inner
                    .store
                    .tool_state(id, &call.call_id, "running", None)?;
                let result = tokio::select! {
                    biased;
                    _=stop.notified()=>Err("Stop requested while a tool result was pending; resolve its original outcome".to_string()),
                    result=self.inner.tools.call(id,&record.servers,&call.name,call.arguments.clone(),&call.operation_id)=>result,
                };
                match result {
                    Ok(value) => {
                        self.inner
                            .store
                            .tool_result(id, &call.call_id, value, false)?;
                    }
                    Err(error) => {
                        self.inner.store.tool_state(
                            id,
                            &call.call_id,
                            "unknown",
                            Some(error.clone()),
                        )?;
                        return self.inner.store.state(
                            id,
                            SessionStatus::NeedsResolution,
                            "Tool outcome unknown; no automatic replay",
                            Some(error),
                        );
                    }
                }
            }
            if self.inner.store.stopped(id)? {
                return self
                    .inner
                    .store
                    .state(id, SessionStatus::Stopped, "Stopped", None);
            }
            if config.max_turns.is_some_and(|limit| turns >= limit) {
                return self.inner.store.state(
                    id,
                    SessionStatus::Waiting,
                    "Configured turn budget reached; resume to continue",
                    None,
                );
            }
            self.inner
                .store
                .state(id, SessionStatus::Running, "Loading configured tools", None)?;
            // Only discovery/initialization is read-only and safe to retry here.
            // Tool calls below retain the single-dispatch/unknown-outcome rule.
            let mut discovery_attempt = 0u64;
            let mut tools = loop {
                let result = tokio::select! {
                    biased;
                    _=stop.notified()=>return self.inner.store.state(id,SessionStatus::Stopped,"Stopped before inference",None),
                    result=self.inner.tools.list_tools(id,&config.mcp_servers)=>result,
                };
                match result {
                    Ok(tools) => break tools,
                    Err(error) => {
                        let Some(retry_after) = mcp::discovery_retry_after(&error) else {
                            return Err(anyhow::Error::msg(error));
                        };
                        discovery_attempt = discovery_attempt.saturating_add(1);
                        if self
                            .wait_retry(
                                id,
                                "tool_discovery",
                                discovery_attempt,
                                &config,
                                &error,
                                retry_after,
                            )
                            .await?
                        {
                            return self.inner.store.state(
                                id,
                                SessionStatus::Stopped,
                                "Stopped during tool discovery retry",
                                None,
                            );
                        }
                    }
                }
            };
            tools.extend(config.provider_tools.clone());
            let transcript = self.transcript(id)?;
            let input_sequence = transcript
                .iter()
                .filter(|e| e.kind == "input")
                .map(|e| e.sequence)
                .max()
                .unwrap_or(0);
            let input = transcript
                .into_iter()
                .filter(|e| matches!(e.kind.as_str(), "input" | "output" | "tool_result"))
                .map(|e| e.item)
                .collect();
            self.inner
                .store
                .record(id, "configuration", &serde_json::to_value(&config)?)?;
            let request = ProviderRequest {
                model: config.model.clone(),
                instructions: format!(
                    "Runtime identity and task context: {}\n\n{}",
                    json!({"agent_id":session.agent_id,"session_id":session.id,"task":session.task}),
                    config.instructions,
                ),
                input,
                tools,
                reasoning_effort: config.reasoning_effort.clone(),
                max_output_tokens: config.max_output_tokens,
                endpoint: config.provider_url.clone(),
                api_key_env: config.api_key_env.clone(),
            };
            let mut retries = 0u64;
            let response = loop {
                if self.inner.store.stopped(id)? {
                    return self.inner.store.state(
                        id,
                        SessionStatus::Stopped,
                        "Stopped before inference",
                        None,
                    );
                }
                self.inner.store.state(
                    id,
                    SessionStatus::Running,
                    "Waiting for model response",
                    None,
                )?;
                let result = tokio::select! {
                    biased;
                    _=stop.notified()=>return self.inner.store.state(id,SessionStatus::Stopped,"Inference cancelled by stop; committed transcript preserved",None),
                    result=self.inner.provider.respond(request.clone())=>result,
                };
                let result = match result {
                    Ok(response) if response.status != "completed" => {
                        self.inner.store.record(id, "incomplete_response", &json!({"id":response.id,"status":response.status,"output":response.output,"usage":response.usage}))?;
                        Err(ProviderError {
                            retryable: response.status == "incomplete",
                            retry_after_ms: None,
                            message: format!(
                                "Provider response {} retained; no tools dispatched",
                                response.status
                            ),
                        })
                    }
                    other => other,
                };
                match result {
                    Ok(response) => break response,
                    Err(error) if error.retryable => {
                        // There is deliberately no retry budget. Numeric legacy
                        // max_retries metadata cannot strand recoverable work.
                        retries = retries.saturating_add(1);
                        if self
                            .wait_retry(
                                id,
                                "inference",
                                retries,
                                &config,
                                &error.message,
                                error.retry_after_ms,
                            )
                            .await?
                        {
                            return self.inner.store.state(
                                id,
                                SessionStatus::Stopped,
                                "Stopped during inference retry",
                                None,
                            );
                        }
                    }
                    Err(error) => {
                        return self.inner.store.state(
                            id,
                            SessionStatus::Failed,
                            "Inference failed; resume after resolving the cause",
                            Some(error.message),
                        );
                    }
                }
            };
            let calls = match self.parse_calls(id, &response.output, &config.mcp_servers) {
                Ok(calls) => calls,
                Err(error) => {
                    self.inner.store.record(id,"invalid_response",&json!({"id":response.id,"output":response.output,"error":error.to_string()}))?;
                    return self.inner.store.state(
                        id,
                        SessionStatus::Failed,
                        "Invalid tool batch retained; no tools dispatched",
                        Some(error.to_string()),
                    );
                }
            };
            let finished = self
                .inner
                .store
                .response(id, &response, calls, input_sequence)?;
            turns += 1;
            if finished {
                return self.get_session(id);
            }
        }
    }
    async fn wait_retry(
        &self,
        id: &str,
        stage: &str,
        attempt: u64,
        config: &AgentConfig,
        error: &str,
        retry_after_ms: Option<u64>,
    ) -> Result<bool> {
        let delay = retry::delay_ms(config, attempt, retry_after_ms);
        self.inner.store.record(
            id,
            "retry",
            &json!({
                "stage": stage, "retry": attempt, "retry_policy": "unlimited",
                "delay_ms": delay, "retry_after_ms": retry_after_ms, "error": error,
            }),
        )?;
        self.inner.store.state(
            id,
            SessionStatus::Retrying,
            &format!("Transient {stage} failure; retry {attempt} (unlimited) in {delay}ms"),
            Some(error.into()),
        )?;
        if self.inner.store.stopped(id)? {
            return Ok(true);
        }
        let stop = self.notify(id);
        tokio::select! {
            biased;
            _=stop.notified()=>Ok(true),
            _=tokio::time::sleep(Duration::from_millis(delay))=>Ok(false),
        }
    }
    fn parse_calls(
        &self,
        id: &str,
        output: &[Value],
        servers: &[McpServerConfig],
    ) -> Result<Vec<ToolRecord>> {
        let mut known: HashSet<String> = self
            .tool_calls(id)?
            .into_iter()
            .map(|t| t.call_id)
            .collect();
        let mut calls = Vec::new();
        for item in output {
            if item.get("type").and_then(Value::as_str) != Some("function_call") {
                continue;
            }
            if item
                .get("status")
                .and_then(Value::as_str)
                .is_some_and(|s| s != "completed")
            {
                bail!("function call is not complete")
            }
            let call_id = item
                .get("call_id")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .context("function call lacks call_id")?;
            if !known.insert(call_id.into()) {
                bail!("duplicate function call_id")
            }
            let name = item
                .get("name")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .context("function call lacks name")?;
            let arguments = serde_json::from_str(
                item.get("arguments")
                    .and_then(Value::as_str)
                    .context("function call arguments must be JSON text")?,
            )?;
            calls.push(ToolRecord {
                call: ToolCall {
                    call_id: call_id.into(),
                    operation_id: format!("{id}:{call_id}"),
                    name: name.into(),
                    arguments,
                    status: "pending".into(),
                    result: None,
                    error: None,
                },
                servers: servers.to_vec(),
            });
        }
        Ok(calls)
    }
}
pub fn read_config(path: impl AsRef<Path>) -> Result<RuntimeConfig> {
    let config: RuntimeConfig = serde_json::from_slice(
        &std::fs::read(path.as_ref()).context("read runtime configuration")?,
    )
    .context("parse runtime configuration")?;
    let mut ids = HashSet::new();
    for agent in &config.agents {
        if agent.id.trim().is_empty() || agent.model.trim().is_empty() || !ids.insert(&agent.id) {
            bail!("agents need unique nonempty ids and models")
        }
        if agent.max_turns == Some(0) || agent.max_output_tokens == Some(0) {
            bail!("configured turn/output limits must be positive or absent")
        }
        if agent.retry_initial_ms == 0
            || agent.retry_max_ms == 0
            || agent.retry_jitter_percent > 100
        {
            bail!("retry delays must be positive and jitter percent at most 100")
        }
        let mut names = HashSet::new();
        for server in &agent.mcp_servers {
            if server.name.is_empty() || !names.insert(&server.name) {
                bail!("MCP servers need unique nonempty names per agent")
            }
        }
    }
    Ok(config)
}
