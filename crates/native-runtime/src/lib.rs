//! Small durable Responses executor. Agent configuration is reusable metadata;
//! each session has independent transcript, control state and tool results.
mod board;
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
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
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
    draining: AtomicBool,
    admission: Mutex<()>,
    reporting: AtomicBool,
    report_lock: tokio::sync::Mutex<()>,
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
        if config.board.is_some() {
            store.queue_current_reports(true)?;
        }
        Ok(Self {
            inner: Arc::new(Inner {
                config_path,
                store,
                provider,
                tools,
                running: Mutex::new(HashSet::new()),
                run_locks: Mutex::new(HashMap::new()),
                stops: Mutex::new(HashMap::new()),
                draining: AtomicBool::new(false),
                admission: Mutex::new(()),
                reporting: AtomicBool::new(false),
                report_lock: tokio::sync::Mutex::new(()),
                _database_lock: lock,
            }),
        })
    }
    pub fn config(&self) -> Result<RuntimeConfig> {
        read_config(&self.inner.config_path)
    }
    /// Local-only compatibility API. Board-enabled callers must use load_agents.
    pub fn agents(&self) -> Result<Vec<AgentConfig>> {
        let config = read_config(&self.inner.config_path)?;
        if config.board.is_some() {
            bail!("Board profiles require async load_agents");
        }
        Ok(config.agents)
    }
    pub async fn load_agents(&self) -> Result<Vec<AgentConfig>> {
        let config = read_config(&self.inner.config_path)?;
        match &config.board {
            Some(board) => board::BoardClient::new(board)?.load_agents().await,
            None => Ok(config.agents),
        }
    }
    pub async fn load_agent(&self, id: &str) -> Result<AgentConfig> {
        let config = read_config(&self.inner.config_path)?;
        match &config.board {
            Some(board) => board::BoardClient::new(board)?.load_agent(id).await,
            None => self.agent(id),
        }
    }
    /// Resolve fresh profile and bind endpoint templates to one durable session.
    pub async fn session_agent(&self, id: &str) -> Result<AgentConfig> {
        let session = self.get_session(id)?;
        let mut agent = self.load_agent(&session.agent_id).await?;
        if let Some(board) = &self.config()?.board {
            board::BoardClient::new(board)?
                .check_generation(&session.id, session.generation)
                .await?;
        }
        for server in &mut agent.mcp_servers {
            // Stable identity is runtime authority, never a shared host env or
            // a model-issued register_agent call. Binding all MCP endpoints also
            // covers aliases of the Board server without trusting a name label.
            server.bound_agent_id = Some(session.agent_id.clone());
            server.url = server
                .url
                .replace(
                    "{runtime_url}",
                    session.runtime_url.as_deref().unwrap_or_default(),
                )
                .replace("{session_id}", &url_component(id))
                .replace("{agent_id}", &url_component(&session.agent_id));
            if server.name == "workspace"
                && let Some(url) = &session.workspace_mcp_url
            {
                server.url = url.clone();
            }
            if server.name == "native"
                && let Some(url) = &session.lifecycle_mcp_url
            {
                server.url = url
                    .replace("{session_id}", &url_component(id))
                    .replace("{agent_id}", &url_component(&session.agent_id));
            }
        }
        // Placement can bind an authorized server, never add a revoked tool grant.
        validate_agent(&agent)?;
        Ok(agent)
    }
    pub async fn create_session_with_options(
        &self,
        agent_id: &str,
        task: Option<String>,
        input: &str,
        mut options: SessionOptions,
    ) -> Result<Session> {
        validate_create(agent_id, input, &options)?;
        let config = read_config(&self.inner.config_path)?;
        if options.host_id.is_none() {
            options.host_id = config.host_id.clone();
        }
        if config.host_id.is_some() && options.host_id != config.host_id {
            bail!("session host_id differs from this runtime host");
        }
        if let Some(session) = self
            .inner
            .store
            .existing_request(agent_id, &task, input, &options)?
        {
            return Ok(self.observe(session));
        }
        self.ensure_accepting()?;
        self.load_agent(agent_id).await?;
        let _admission = self.inner.admission.lock().unwrap();
        self.ensure_accepting()?;
        let session = self.inner.store.create_with_options(
            agent_id,
            task,
            input,
            options,
            config.runtime_url,
        )?;
        Ok(self.observe(session))
    }
    fn ensure_accepting(&self) -> Result<()> {
        if self.is_draining() {
            bail!("runtime is quiescing; no new sessions accepted");
        }
        Ok(())
    }
    pub fn is_draining(&self) -> bool {
        self.inner.draining.load(Ordering::SeqCst)
    }
    /// Cooperative drain: never cancels an in-flight tool or inference. Callers
    /// poll driver_running before stopping the process. Ready work restarts from
    /// committed state after deployment.
    pub fn quiesce(&self) -> Result<Vec<Session>> {
        let _admission = self.inner.admission.lock().unwrap();
        self.inner.draining.store(true, Ordering::SeqCst);
        self.list_sessions()
    }
    pub fn unquiesce(&self) {
        self.inner.draining.store(false, Ordering::SeqCst);
    }
    /// Start the durable reporting worker once. The worker owns only a Weak
    /// runtime between deliveries; dropping a runtime releases its database lock.
    pub fn start_reporting(&self) -> Result<()> {
        let config = read_config(&self.inner.config_path)?;
        let Some(board) = config.board else {
            return Ok(());
        };
        let handle =
            tokio::runtime::Handle::try_current().context("reporting requires an async runtime")?;
        if self.inner.reporting.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let weak = Arc::downgrade(&self.inner);
        handle.spawn(async move {
            loop {
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                let runtime = Runtime { inner };
                let _ = runtime.flush_reports().await;
                drop(runtime);
                tokio::time::sleep(Duration::from_millis(board.report_interval_ms)).await;
            }
        });
        Ok(())
    }
    pub async fn flush_reports(&self) -> Result<usize> {
        let _lock = self.inner.report_lock.lock().await;
        let config = read_config(&self.inner.config_path)?;
        let Some(board) = config.board else {
            return Ok(0);
        };
        let client = board::BoardClient::new(&board)?;
        self.inner.store.queue_current_reports(false)?;
        let mut delivered = 0;
        let mut failed = HashSet::new();
        // Bounded batches keep shutdown and one unavailable identity responsive.
        for (sequence, mut session) in self.inner.store.pending_reports(256)? {
            if failed.contains(&session.id) {
                continue;
            }
            session.driver_running = self.inner.running.lock().unwrap().contains(&session.id);
            match client.report(&session).await {
                Ok(()) => {
                    self.inner.store.acknowledge_report(sequence, &session.id)?;
                    delivered += 1;
                }
                Err(error) => {
                    self.inner
                        .store
                        .report_error(&session.id, &error.to_string())?;
                    failed.insert(session.id);
                }
            }
        }
        Ok(delivered)
    }
    fn observe(&self, mut session: Session) -> Session {
        session.driver_running = self.inner.running.lock().unwrap().contains(&session.id);
        session.report_error = self
            .inner
            .store
            .get_report_error(&session.id)
            .ok()
            .flatten();
        session
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
        let _admission = self.inner.admission.lock().unwrap();
        self.ensure_accepting()?;
        self.agent(agent_id)?;
        if input.trim().is_empty() {
            bail!("input must not be empty")
        }
        self.inner.store.create(agent_id, task, input)
    }
    pub fn list_sessions(&self) -> Result<Vec<Session>> {
        Ok(self
            .inner
            .store
            .list()?
            .into_iter()
            .map(|s| self.observe(s))
            .collect())
    }
    pub fn get_session(&self, id: &str) -> Result<Session> {
        Ok(self.observe(self.inner.store.get(id)?))
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
    /// Explicit placement adoption at an inactive boundary. Prior transcripts
    /// and committed tool receipts/endpoints remain immutable. Unknown effects
    /// must be reconciled before moving a session to another workspace.
    pub fn bind_session(&self, id: &str, mut options: SessionOptions) -> Result<Session> {
        validate_create("binding", "binding", &options)?;
        if options.session_id.as_ref().is_some_and(|other| other != id) {
            bail!("binding cannot change session_id");
        }
        let config = self.config()?;
        if options.host_id.is_none() {
            options.host_id = config.host_id.clone();
        }
        if config.host_id.is_some() && options.host_id != config.host_id {
            bail!("session host_id differs from this runtime host");
        }
        let running = self.inner.running.lock().unwrap();
        if running.contains(id) {
            bail!("session is already running; drain before binding placement");
        }
        self.inner.store.bind(id, options, config.runtime_url)
    }
    /// Graceful per-session stop: mark the durable boundary but do not cancel
    /// the active tool future (including a tool requesting its own stop).
    pub fn drain_session(&self, id: &str) -> Result<Session> {
        let running = self.inner.running.lock().unwrap();
        self.inner.store.stop(id, running.contains(id))
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
        if self.is_draining() {
            return Ok(s);
        }
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
            if self.is_draining() {
                return self.inner.store.state(
                    id,
                    SessionStatus::Ready,
                    "Quiesced at a durable boundary; ready after restart",
                    None,
                );
            }
            let mut profile_attempt = 0u64;
            let fallback_retry: AgentConfig =
                serde_json::from_value(json!({"id": session.agent_id, "instructions":""}))?;
            let config = loop {
                let loaded = tokio::select! {
                    biased;
                    _=stop.notified()=>return self.inner.store.state(id,SessionStatus::Stopped,"Stopped during profile load",None),
                    result=self.session_agent(id)=>result,
                };
                match loaded {
                    Ok(config) => break config,
                    Err(error) => {
                        let Some(hint) = board::read_retry_after(&error) else {
                            return Err(error);
                        };
                        profile_attempt = profile_attempt.saturating_add(1);
                        if self
                            .wait_retry(
                                id,
                                "profile_discovery",
                                profile_attempt,
                                &fallback_retry,
                                &error.to_string(),
                                hint,
                            )
                            .await?
                        {
                            return self.retry_interrupted(id, "Stopped during profile retry");
                        }
                    }
                }
            };
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
            for mut record in intents.into_iter().filter(|r| r.call.status == "pending") {
                // Adopt legacy pending intents' identity without changing their
                // endpoint/arguments/operation id. tool_state persists this same
                // trusted binding before any request can be sent.
                for server in &mut record.servers {
                    server.bound_agent_id = Some(session.agent_id.clone());
                }
                if self.is_draining() {
                    return self.inner.store.state(
                        id,
                        SessionStatus::Ready,
                        "Quiesced before the next tool; committed results preserved",
                        None,
                    );
                }
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
                if let Some((namespace, tool)) = call.name.split_once("__") {
                    let current = config
                        .mcp_servers
                        .iter()
                        .find(|server| server.name == namespace);
                    let original = record
                        .servers
                        .iter()
                        .find(|server| server.name == namespace);
                    let granted = current.zip(original).is_some_and(|(current, original)| {
                        current.url == original.url
                            && current
                                .tools_allowlist
                                .as_ref()
                                .is_none_or(|allowed| allowed.iter().any(|name| name == tool))
                    });
                    if !granted {
                        self.inner.store.tool_result(id, &call.call_id,
                            json!({"isError":true,"content":[{"type":"text","text":"Tool grant or endpoint changed before dispatch; no tool was executed"}]}), false)?;
                        continue;
                    }
                }
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
            if self.is_draining() {
                return self.inner.store.state(
                    id,
                    SessionStatus::Ready,
                    "Quiesced after tool results were committed",
                    None,
                );
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
                            return self
                                .retry_interrupted(id, "Stopped during tool discovery retry");
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
                    json!({"agent_id":session.agent_id,"session_id":session.id,"task":session.task,"host_id":session.host_id,"workspace_cwd":session.workspace_cwd,"generation":session.generation}),
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
                            return self.retry_interrupted(id, "Stopped during inference retry");
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
        let deadline = tokio::time::Instant::now() + Duration::from_millis(delay);
        loop {
            if self.inner.store.stopped(id)? || self.is_draining() {
                return Ok(true);
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(false);
            }
            // A graceful drain never sends cancellation into an active tool;
            // read-only backoff can safely observe its durable flag promptly.
            tokio::select! {
                biased;
                _=stop.notified()=>return Ok(true),
                _=tokio::time::sleep_until(deadline.min(now + Duration::from_secs(1)))=>{},
            }
        }
    }
    fn retry_interrupted(&self, id: &str, progress: &str) -> Result<Session> {
        if self.is_draining() && !self.inner.store.stopped(id)? {
            self.inner.store.state(
                id,
                SessionStatus::Ready,
                "Quiesced during read-only retry",
                None,
            )
        } else {
            self.inner
                .store
                .state(id, SessionStatus::Stopped, progress, None)
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
    if let Some(board) = &config.board {
        board::BoardClient::new(board)?;
    }
    let mut ids = HashSet::new();
    for agent in &config.agents {
        if !ids.insert(&agent.id) {
            bail!("agents need unique nonempty ids and models");
        }
        validate_agent(agent)?;
    }
    Ok(config)
}
pub(crate) fn validate_agent(agent: &AgentConfig) -> Result<()> {
    if agent.id.trim().is_empty() || agent.model.trim().is_empty() {
        bail!("agents need unique nonempty ids and models");
    }
    if agent.max_turns == Some(0) || agent.max_output_tokens == Some(0) {
        bail!("configured turn/output limits must be positive or absent");
    }
    if agent.context_window_tokens == 0
        || agent
            .max_output_tokens
            .is_some_and(|n| u64::from(n) > agent.context_window_tokens)
    {
        bail!("output must fit the shared context window");
    }
    if agent.retry_initial_ms == 0 || agent.retry_max_ms == 0 || agent.retry_jitter_percent > 100 {
        bail!("retry delays must be positive and jitter percent at most 100")
    }
    let mut names = HashSet::new();
    for server in &agent.mcp_servers {
        if server.name.is_empty() || !names.insert(&server.name) {
            bail!("MCP servers need unique nonempty names per agent");
        }
    }
    Ok(())
}
fn validate_create(agent: &str, input: &str, options: &SessionOptions) -> Result<()> {
    if agent.trim().is_empty() || input.trim().is_empty() {
        bail!("input and agent_id must not be empty");
    }
    if options
        .session_id
        .as_ref()
        .is_some_and(|s| s.is_empty() || s.len() > 256 || s.chars().any(char::is_control))
    {
        bail!("invalid session_id");
    }
    if options
        .workspace_cwd
        .as_ref()
        .is_some_and(|p| !p.is_absolute())
    {
        bail!("workspace_cwd must be absolute");
    }
    if options.generation > i64::MAX as u64 {
        bail!("generation exceeds the Board integer range");
    }
    if !options.metadata.is_object() {
        bail!("session metadata must be an object");
    }
    Ok(())
}
fn url_component(value: &str) -> String {
    value
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}
