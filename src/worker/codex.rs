//! Local-stdio adapter qualified against the supplied alpha.10 schema.
use super::process::{self, Control, ManagedProcess};
use crate::limits::{CodingLimits, ExecutionLimits, WorkerLimits};
use crate::protocol::{ExecutionProfile, InferenceSettings};
use crate::governance_completion::{CompletionBoundary, CompletionTarget};
use serde::Serialize;
use serde_json::{Value, json};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    time::Duration,
};
#[path = "codex_charter.rs"]
mod charter_delivery;
#[path="codex_charter_turns.rs"]
mod charter_turns;
#[path="codex_retry.rs"]
pub(super) mod retry;
pub use charter_delivery::Scope as InitialCharterScope;

use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::mpsc,
};

fn validate_inference(settings: &InferenceSettings) -> std::result::Result<(), String> {
    if settings.model != crate::protocol::SELECTED_MODEL
        || settings.reasoning_effort != crate::protocol::SELECTED_REASONING_EFFORT
    {
        return Err("template_inference_unsupported".into());
    }
    Ok(())
}

/// Trusted host values; never supplied by model tool arguments.
#[derive(Debug, Clone)]
pub struct Collaboration {
    pub instruction_source: Option<crate::protocol::RepositoryReadSource>,
    pub controller_url: String,
    pub caller: crate::mcp::Caller,
    pub bridge_binary: PathBuf,
    pub memory: Option<MemoryScope>,
    /// Derived from the immutable accepted task profile by the worker host.
    pub workflow_enabled: bool,
}
#[derive(Debug, Clone)]
pub struct MemoryScope {
    pub agent_id: String,
    pub session_id: String,
}

/// The original worker attempt/session, independent of optional MCP capability.
#[derive(Clone)]
pub struct CompletionScope {
    pub controller_url: String,
    pub caller: crate::mcp::Caller,
    pub session_id: String,
    pub resume: Option<Box<crate::governance_completion::CompletionResume>>,
}

/// A sent turn remains unresolved through its start acknowledgment and process
/// shutdown. Only its exact terminal event closes the execution boundary.
#[derive(Clone, Default)]
pub(super) struct ExecutionLifecycle(Arc<AtomicBool>);
impl ExecutionLifecycle {
    pub(super) fn uncertain(&self) -> bool { self.0.load(Ordering::SeqCst) }
    fn dispatched(&self) { self.0.store(true, Ordering::SeqCst); }
    fn terminal(&self) { self.0.store(false, Ordering::SeqCst); }
}

/// Explicit execution and bridge dispositions are separate from diagnostics.
#[derive(Debug)]
pub struct ExecutionFailure {
    pub message: String,
    pub collaboration_uncertain: bool,
    pub execution_uncertain: bool,
    pub completion_held: bool,
    pub completion_uncertain: bool,
}
impl From<String> for ExecutionFailure {
    fn from(message: String) -> Self {
        Self {
            message,
            collaboration_uncertain: false,
            execution_uncertain: false,
            completion_held: false,
            completion_uncertain: false,
        }
    }
}
impl From<&str> for ExecutionFailure {
    fn from(message: &str) -> Self {
        message.to_owned().into()
    }
}
#[derive(Debug)]
pub enum Event {
    SuppliedOuter { observation: crate::supplied_outer::Observation, acknowledged: tokio::sync::oneshot::Sender<std::result::Result<Option<crate::supplied_outer::Wait>, String>> },
    Started { thread_id: String, turn_id: String },
    Progress(String),
    ReserveCompletionTurn {
        begin_operation_id: String,
        model_requests: u32,
        acknowledged: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    },
    AssociationBarrier {
        thread_id: String,
        initial_turn_id: String,
        acknowledged: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    },
}
#[derive(Debug)]
pub struct Result {
    pub success: bool,
    pub summary: String,
    pub thread_id: String,
    pub turn_id: String,
    pub initial_turn_id: String,
    pub completion_blocked: bool,
    pub completion_uncertain: bool,
    pub observed_usage: ObservedUsage,
}

/// The pinned event exposes token totals, never the number of inner requests.
#[derive(Debug, Default, Clone, Serialize)]
pub struct ObservedUsage {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    model_requests: Option<u32>,
}
impl ObservedUsage {
    fn observe(&mut self, message: &Value, thread_id: &str, turn_id: &str) -> bool {
        if message["method"] != "thread/tokenUsage/updated"
            || message["params"]["threadId"] != thread_id
            || message["params"]["turnId"] != turn_id
        {
            return false;
        }
        let total = &message["params"]["tokenUsage"]["total"];
        let (Some(input), Some(output)) = (
            total["inputTokens"].as_u64(),
            total["outputTokens"].as_u64(),
        ) else {
            return false;
        };
        if input > i64::MAX as u64
            || output > i64::MAX as u64
            || self.input_tokens.is_some_and(|old| input < old)
            || self.output_tokens.is_some_and(|old| output < old)
            || (self.input_tokens == Some(input) && self.output_tokens == Some(output))
        {
            return false;
        }
        self.input_tokens = Some(input);
        self.output_tokens = Some(output);
        true
    }
}

async fn line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    bytes: &mut Vec<u8>,
    frame_bytes: usize,
) -> std::result::Result<Option<Value>, String> {
    loop {
        let chunk = reader.fill_buf().await.map_err(|e| e.to_string())?;
        if chunk.is_empty() {
            if bytes.is_empty() {
                return Ok(None);
            }
            return Err("executor stdout ended inside a message".into());
        }
        let end = chunk.iter().position(|b| *b == b'\n').map(|n| n + 1);
        let count = end.unwrap_or(chunk.len());
        if bytes.len().saturating_add(count) > frame_bytes {
            return Err("executor message exceeds limit (execution.coding.frame_bytes)".into());
        }
        bytes.extend_from_slice(&chunk[..count]);
        reader.consume(count);
        if end.is_some() {
            let value = serde_json::from_slice(bytes)
                .map(Some)
                .map_err(|_| "executor sent invalid JSON".into());
            bytes.clear();
            return value;
        }
    }
}

enum SessionInput {
    Message(Value),
    MemoryBoundary(super::codex_mcp::MemoryBoundary),
}

// Closed diagnostic labels; no supplied method, identifier or payload is logged.
fn rpc_class(method: &str) -> &'static str {
    match method {
        "initialize" => "initialize",
        "command/exec" => "command_exec",
        "thread/start" => "thread_start",
        "thread/read" => "thread_read",
        "thread/resume" => "thread_resume",
        "thread/inject_items" => "thread_inject_items",
        "turn/start" => "turn_start",
        _ => "other_rpc",
    }
}
fn notification_class(message: &Value) -> (usize, &'static str) {
    match message["method"].as_str() {
        Some("turn/completed") => (0, "terminal"),
        Some("item/started" | "item/completed") => (1, "item"),
        Some("thread/tokenUsage/updated") => (2, "usage"),
        Some("error") => (3, "error"),
        _ => (4, "other"),
    }
}
fn progress_notification(message: &Value) -> bool {
    message.as_object().is_some_and(|o| o.len() == 2 && o.contains_key("method") && o.contains_key("params"))
        && message["method"] == "item/agentMessage/delta"
        && message["params"].as_object().is_some_and(|p| p.len() == 4
            && ["threadId", "turnId", "itemId", "delta"].iter().all(|key| p.get(*key).is_some_and(Value::is_string)))
}

pub struct Session {
    limits: CodingLimits,
    process: ManagedProcess,
    reader: BufReader<tokio::process::ChildStdout>,
    frame: Vec<u8>,
    stderr: tokio::task::JoinHandle<std::result::Result<(Vec<u8>, bool), String>>,
    next_id: u64,
    pending: VecDeque<Value>,
    progress: Option<mpsc::Sender<Event>>,
    collaboration: Option<super::codex_mcp::Config>,
    lifecycle: ExecutionLifecycle,
}
// Keep this allowlist local to the supplied executor. Native inference and
// ordinary Git/build children have separate credential requirements.
const CODEX_AUTH_ENVIRONMENT: [&str; 4] = [
    "OPENAI_FEDERATION_RULE_ID",
    "OPENAI_IDENTITY_TOKEN_FILE",
    "OPENAI_WORKLOAD_IDENTITY_CONTEXT",
    "OPENAI_STAGING_FEDERATION_RULE_ID",
];

fn codex_auth_environment() -> std::result::Result<Vec<(String, String)>, String> {
    codex_auth_environment_from(|name| match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(()),
    })
}

fn codex_auth_environment_from(
    mut value: impl FnMut(&str) -> std::result::Result<Option<String>, ()>,
) -> std::result::Result<Vec<(String, String)>, String> {
    let mut environment = Vec::new();
    for name in CODEX_AUTH_ENVIRONMENT {
        if let Some(value) = value(name).map_err(|_| "Codex authentication environment is not valid Unicode")? {
            environment.push((name.into(), value));
        }
    }
    // Preserve partial configuration so Codex rejects it instead of silently
    // falling back to another authentication method.
    Ok(environment)
}

impl Session {
    pub async fn start(
        binary: &Path,
        cwd: &Path,
        state: &Path,
        registry: &Path,
        inference: &InferenceSettings,
        limits: &ExecutionLimits,
        worker_limits: Arc<WorkerLimits>,
        runtime_limits: &crate::limits::RuntimeLimits,
    ) -> std::result::Result<Self, String> {
        validate_inference(inference)?;
        limits.coding.validate()?;
        worker_limits.validate()?;
        super::configured_credentials(runtime_limits)?;
        let auth_environment = codex_auth_environment()?;
        Self::start_with_auth(
            binary,
            cwd,
            state,
            registry,
            inference,
            limits,
            worker_limits,
            Some(&auth_environment),
        )
        .await
    }
    async fn start_with_auth(
        binary: &Path,
        cwd: &Path,
        state: &Path,
        registry: &Path,
        inference: &InferenceSettings,
        limits: &ExecutionLimits,
        worker_limits: Arc<WorkerLimits>,
        inference_auth: Option<&[(String, String)]>,
    ) -> std::result::Result<Self, String> {
        validate_inference(inference)?;
        limits.coding.validate()?;
        worker_limits.validate()?;
        let model = &inference.model;
        process::private_directory(state)?;
        let mut args = vec![
            "app-server".into(),
            "--listen".into(),
            "stdio://".into(),
            "-c".into(),
            format!(
                "model={}",
                serde_json::to_string(model).map_err(|e| e.to_string())?
            ),
            "-c".into(),
            "model_provider=\"openai\"".into(),
            "-c".into(),
            format!(
                "model_reasoning_effort={}",
                serde_json::to_string(&inference.reasoning_effort)
                    .map_err(|_| "invalid selected effort")?
            ),
            // Resolve thread defaults against this task, rather than inheriting
            // unrelated host-user writable roots from system configuration.
            // Managed requirements and workspace-write/tmp/network semantics
            // remain in force; this override only narrows the extra roots.
            "-c".into(),
            format!(
                "sandbox_workspace_write.writable_roots={}",
                serde_json::to_string(&[cwd]).map_err(|e| e.to_string())?
            ),
        ];
        context_args(&mut args, &limits.coding);
        let mut environment = vec![("CODEX_HOME".into(), state.to_string_lossy().into_owned())];
        // Codex owns workload-identity exchange and refresh. Pass the environment's
        // references only to this server; retain task-local state and never select
        // the native worker's API key as a Codex inference credential.
        if let Some(auth) = inference_auth {
            environment.extend_from_slice(auth);
        }
        let mut process =
            ManagedProcess::spawn(binary, &args, cwd, registry, &environment, worker_limits)
                .await?;
        let stdout = process
            .child
            .stdout
            .take()
            .ok_or("missing executor stdout")?;
        let stderr = process
            .child
            .stderr
            .take()
            .ok_or("missing executor stderr")?;
        Ok(Self {
            limits: limits.coding.clone(),
            process,
            reader: BufReader::new(stdout),
            frame: Vec::new(),
            stderr: tokio::spawn(process::read_bounded(stderr, limits.coding.stderr_bytes)),
            next_id: 1,
            pending: VecDeque::new(),
            progress: None,
            collaboration: None,
            lifecycle: ExecutionLifecycle::default(),
        })
    }
    async fn send(&mut self, value: Value) -> std::result::Result<(), String> {
        let mut bytes = serde_json::to_vec(&value).map_err(|e| e.to_string())?;
        if bytes.len() > self.limits.frame_bytes {
            return Err("executor input exceeds limit (execution.coding.frame_bytes)".into());
        }
        bytes.push(b'\n');
        let stdin = self.process.stdin.as_mut().ok_or("executor stdin closed")?;
        // From the first possible write onward, neither an ACK nor reaping the
        // local process proves that this model turn reached a terminal result.
        if value["method"] == "turn/start" { self.lifecycle.dispatched(); }
        stdin.write_all(&bytes)
            .await
            .map_err(|_| "executor request write failed; outcome uncertain".into())
    }
    async fn next(
        &mut self,
        control: &Control,
        events: Option<&mpsc::Sender<Event>>,
    ) -> std::result::Result<Value, String> {
        match self.next_input(control, events, false).await? {
            SessionInput::Message(value) => Ok(value),
            SessionInput::MemoryBoundary(_) => Err("unexpected nested memory boundary".into()),
        }
    }
    // Only framed reads are cancelled by a competing canonical acknowledgment.
    // The retained frame survives that cut; interactive replies and ownership
    // checks run to completion outside select so no partial write is abandoned.
    async fn read_input(
        &mut self,
        control: &Control,
        allow_memory_boundary: bool,
    ) -> std::result::Result<Option<SessionInput>, String> {
        let mut stopped = control.clone();
        let bridge = self.collaboration.clone();
        tokio::select! {
            _=stopped.stopped()=>Err("executor cancelled or deadline reached".into()),
            _=async {match &bridge {Some(config)=>super::codex_mcp::wait_stop(config).await,None=>std::future::pending::<()>().await}} =>Err("collaboration bridge stopped; executor must quiesce".into()),
            gate=async {match bridge.as_ref().filter(|_|allow_memory_boundary) {Some(config)=>super::codex_mcp::wait_memory_boundary(config).await,None=>std::future::pending().await}} => gate.map(|gate|Some(SessionInput::MemoryBoundary(gate))),
            m=line(&mut self.reader, &mut self.frame, self.limits.frame_bytes)=>m.map(|m|m.map(SessionInput::Message)),
        }
    }
    async fn process_input(
        &mut self,
        msg: Value,
        events: Option<&mpsc::Sender<Event>>,
    ) -> std::result::Result<Option<Value>, String> {
        if let Some(config) = &self.collaboration {
            super::codex_mcp::check_stop(config)?;
        }
        if self.collaboration.is_some()
            && msg["method"] == "item/completed"
            && msg["params"]["item"]["type"] == "mcpToolCall"
            && msg["params"]["item"]["server"] == super::codex_mcp::SERVER_NAME
            && !msg["params"]["item"]["error"].is_null()
        {
            return Err("collaboration transport failed; executor must quiesce".into());
        }
        if matches!(msg["method"].as_str(), Some("item/started" | "item/completed" | "turn/completed")) {
            self.process.refresh_ownership()?;
        }
        if msg.get("method").is_some() && msg.get("id").is_some() {
            //= spec/impl/v2/implementation.md#the-supplied-executor-is-qualified-at-its-actual-boundary
            //# An unknown executor request MUST fail without silently granting authority.
            self.send(json!({"id":msg["id"],"error":{"code":-32601,"message":"This worker does not grant interactive requests"}})).await?;
            return Ok(None);
        }
        if msg["method"] == "item/agentMessage/delta" {
            if let (Some(e), Some(s)) = (events.or(self.progress.as_ref()), msg["params"]["delta"].as_str()) {
                let _ = e.try_send(Event::Progress(s.chars().take(self.limits.progress_chars).collect()));
            }
        }
        Ok(Some(msg))
    }
    async fn next_input(
        &mut self,
        control: &Control,
        events: Option<&mpsc::Sender<Event>>,
        allow_memory_boundary: bool,
    ) -> std::result::Result<SessionInput, String> {
        loop {
            match self.read_input(control, allow_memory_boundary).await?.ok_or("executor exited before its result")? {
                SessionInput::MemoryBoundary(gate) => return Ok(SessionInput::MemoryBoundary(gate)),
                SessionInput::Message(msg) => if let Some(msg) = self.process_input(msg, events).await? {
                    return Ok(SessionInput::Message(msg));
                },
            }
        }
    }
    fn defer_notification(&mut self, msg: Value, waiting: &str) -> std::result::Result<(), String> {
        // The pinned agent delta is only best-effort progress, already projected
        // by process_input. Final text, usage, terminal and authority never come
        // from this delta. Unknown/malformed shapes remain in the bounded FIFO.
        if progress_notification(&msg) { return Ok(()); }
        if self.pending.len() >= self.limits.pending_notifications {
            let mut counts = [0usize; 5];
            for item in &self.pending { counts[notification_class(item).0] += 1; }
            return Err(format!("executor notification backlog exceeds bound (execution.coding.pending_notifications); waiting={waiting}; queued={}; incoming={}; terminal={}; item={}; usage={}; error={}; other={}",
                self.pending.len(), notification_class(&msg).1, counts[0], counts[1], counts[2], counts[3], counts[4]));
        }
        self.pending.push_back(msg);
        Ok(())
    }
    async fn wait_with_notifications<T, F, Fut>(
        &mut self,
        make: F,
        control: &Control,
    ) -> std::result::Result<T, String>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, String>>,
    {
        // Called only after the exact acknowledged terminal. EOF cannot grant
        // a retry: the original canonical wait/dispatch still decides, followed
        // by the existing process reaping and retained-thread checks.
        let mut waiting = super::boxed_execution(make);
        let mut stdout_closed = false;
        let mut stopped = control.clone();
        let bridge = self.collaboration.clone();
        loop {
            let input = tokio::select! {
                biased;
                _=stopped.stopped()=>return Err("supplied retry cancelled or deadline reached".into()),
                _=async {match &bridge {Some(config)=>super::codex_mcp::wait_stop(config).await,None=>std::future::pending::<()>().await}} =>return Err("collaboration bridge stopped; executor must quiesce".into()),
                result=&mut waiting=>return result,
                input=self.read_input(control, false), if !stdout_closed=>input?,
            };
            match input {
                None => stdout_closed = true,
                Some(SessionInput::MemoryBoundary(_)) => return Err("unexpected retry memory boundary".into()),
                Some(SessionInput::Message(msg)) => if let Some(msg) = self.process_input(msg, None).await? {
                    if msg.get("method").is_some() { self.defer_notification(msg, "supplied_retry")?; }
                },
            }
        }
    }
    pub async fn request(
        &mut self,
        method: &str,
        params: Value,
        control: &Control,
    ) -> std::result::Result<Value, String> {
        let id = self.next_id;
        self.next_id += 1;
        self.send(json!({"id":id,"method":method,"params":params}))
            .await?;
        loop {
            let msg = self.next(control, None).await?;
            if msg["id"].as_u64() == Some(id) {
                if msg.get("error").is_some() {
                    return Err(format!(
                        "executor {method} refused: {}",
                        msg["error"]["message"]
                            .as_str()
                            .unwrap_or("unspecified error")
                    ));
                }
                return msg
                    .get("result")
                    .cloned()
                    .ok_or_else(|| "executor response has no result".into());
            }
            if msg.get("method").is_some() {
                self.defer_notification(msg, rpc_class(method))?;
            }
        }
    }
    pub async fn initialize(&mut self, control: &Control) -> std::result::Result<Value, String> {
        let response=self.request("initialize",json!({"clientInfo":{"name":"borg_worker","title":"Borg Worker","version":"0.2.0"}}),control).await?;
        self.send(json!({"method":"initialized","params":{}}))
            .await?;
        Ok(response)
    }
    pub async fn preflight_command(
        &mut self,
        cwd: &Path,
        control: &Control,
    ) -> std::result::Result<(), String> {
        // Exercise the pinned command runner before spending a model request.
        // Its reserved socket-directory guard is independent of CODEX_HOME.
        let result = self.request("command/exec", json!({
            "command":["/usr/bin/true"], "cwd":cwd, "timeoutMs":self.limits.preflight_timeout_ms,
            "outputBytesCap":self.limits.preflight_output_bytes,
            "sandboxPolicy":{"type":"workspaceWrite","writableRoots":[cwd],"networkAccess":true}
        }), control).await?;
        if result["exitCode"].as_i64() != Some(0) {
            return Err(format!(
                "executor command preflight failed before model start: {}",
                result["stderr"]
                    .as_str()
                    .unwrap_or("missing successful command outcome")
                    .chars()
                    .take(self.limits.preflight_output_bytes)
                    .collect::<String>()
            ));
        }
        Ok(())
    }
    pub async fn shutdown(mut self) -> std::result::Result<(), String> {
        let outcome = self.process.stop().await;
        self.stderr.abort();
        outcome
    }
}

//= spec/impl/v2/implementation.md#the-supplied-executor-owns-its-inner-loop
//# The supplied executor adapter MUST use the documented interface of its pinned executor release.
// Traceability scope: Historical17.84s execution is not current real-provider/browser journey proof.
//= spec/impl/v2/implementation.md#observable-end-to-end-result
//# The initial system MUST execute a coding task in a workspace through the supplied executor.
pub async fn execute(
    binary: &Path,
    cwd: &Path,
    state: &Path,
    registry: &Path,
    inference: &InferenceSettings,
    limits: &ExecutionLimits,
    instruction: &str,
    control: &Control,
    events: mpsc::Sender<Event>,
) -> std::result::Result<Result, String> {
    execute_with_collaboration(
        binary,
        cwd,
        state,
        registry,
        inference,
        limits,
        instruction,
        control,
        events,
        None,
    )
    .await
    .map_err(|error| error.message)
}

pub async fn execute_with_collaboration(
    binary: &Path,
    cwd: &Path,
    state: &Path,
    registry: &Path,
    inference: &InferenceSettings,
    limits: &ExecutionLimits,
    instruction: &str,
    control: &Control,
    events: mpsc::Sender<Event>,
    collaboration: Option<&Collaboration>,
) -> std::result::Result<Result, ExecutionFailure> {
    execute_with_governance(binary,cwd,state,registry,inference,limits,instruction,control,events,collaboration,None).await
}

pub async fn execute_with_governance(
    binary: &Path, cwd: &Path, state: &Path, registry: &Path,
    inference: &InferenceSettings, limits: &ExecutionLimits, instruction: &str,
    control: &Control, events: mpsc::Sender<Event>,
    collaboration: Option<&Collaboration>, completion_scope: Option<&CompletionScope>,
) -> std::result::Result<Result, ExecutionFailure> {
    execute_with_charter(binary,cwd,state,registry,inference,limits,instruction,control,
        events,collaboration,completion_scope,None).await
}

pub async fn execute_with_charter(
    binary: &Path, cwd: &Path, state: &Path, registry: &Path,
    inference: &InferenceSettings, limits: &ExecutionLimits, instruction: &str,
    control: &Control, events: mpsc::Sender<Event>,
    collaboration: Option<&Collaboration>, completion_scope: Option<&CompletionScope>,
    initial_charter: Option<&InitialCharterScope>,
) -> std::result::Result<Result, ExecutionFailure> {
    execute_with_charter_observed(binary,cwd,state,registry,inference,limits,instruction,
        control,events,collaboration,completion_scope,initial_charter,ExecutionLifecycle::default()).await
}

pub(super) async fn execute_with_charter_observed(
    binary: &Path, cwd: &Path, state: &Path, registry: &Path,
    inference: &InferenceSettings, limits: &ExecutionLimits, instruction: &str,
    control: &Control, events: mpsc::Sender<Event>,
    collaboration: Option<&Collaboration>, completion_scope: Option<&CompletionScope>,
    initial_charter: Option<&InitialCharterScope>, lifecycle: ExecutionLifecycle,
) -> std::result::Result<Result, ExecutionFailure> {
    super::boxed_execution(|| execute_with_profile_observed(binary, cwd, state, registry, inference, limits, instruction,
        control, events, collaboration, completion_scope, initial_charter, lifecycle, None)).await
}

pub(super) async fn execute_with_profile_observed(
    binary: &Path, cwd: &Path, state: &Path, registry: &Path,
    inference: &InferenceSettings, limits: &ExecutionLimits, instruction: &str,
    control: &Control, events: mpsc::Sender<Event>,
    collaboration: Option<&Collaboration>, completion_scope: Option<&CompletionScope>,
    initial_charter: Option<&InitialCharterScope>, lifecycle: ExecutionLifecycle,
    accepted_profile: Option<&ExecutionProfile>,
) -> std::result::Result<Result, ExecutionFailure> {
    let result = async {
        validate_inference(inference)?;
        let credentials = super::configured_credentials(&control.runtime_limits)?;
        let auth_environment = codex_auth_environment()?;
        execute_observed(
            binary,
            cwd,
            state,
            registry,
            inference,
            limits,
            instruction,
            control,
            events,
            &credentials,
            Some(&auth_environment),
            collaboration,
            completion_scope,
            initial_charter,
            lifecycle,
            accepted_profile,
        )
        .await
    }
    .await;
    result.map_err(|mut error: ExecutionFailure| {
        // Also audit early setup/cancellation errors that never started a process.
        if let Some(host) = collaboration {
            let config = super::codex_mcp::Config {
                instruction_source: host.instruction_source.clone(),
                execution_profile: accepted_profile.cloned().filter(|p| matches!(p, ExecutionProfile::CollaborationV2 { .. })).or_else(|| host.workflow_enabled.then_some(ExecutionProfile::CollaborationV2 { publication: None, web_search: None })),
                controller_url: host.controller_url.clone(),
                caller: host.caller.clone(),
                deadline_unix_ms: control.deadline_ms,
                state: state.to_owned(),
                runtime_limits: (*control.runtime_limits).clone(),
                limits: limits.collaboration.clone(),
                worker_limits: Box::new((*control.worker_limits).clone()),
            };
            if matches!(
                super::codex_mcp::outcome_disposition(&config),
                Ok(super::codex_mcp::OutcomeDisposition::Unresolved) | Err(_)
            ) {
                error.collaboration_uncertain = true;
            }
        }
        if collaboration.is_some_and(|host|host.workflow_enabled)
            && !matches!(super::collaboration_notice::unresolved(state,&control.runtime_limits),Ok(false)) {
            error.collaboration_uncertain=true;
        }
        error
    })
}

// The private fixture supplies only synthetic values and no ambient auth.
// Production always collects the finite named configuration before any spawn.
async fn execute_with_credentials(
    binary: &Path,
    cwd: &Path,
    state: &Path,
    registry: &Path,
    inference: &InferenceSettings,
    limits: &ExecutionLimits,
    instruction: &str,
    control: &Control,
    events: mpsc::Sender<Event>,
    credentials: &[String],
    inference_auth: Option<&[(String, String)]>,
) -> std::result::Result<Result, String> {
    execute_with_credentials_and_collaboration(
        binary,
        cwd,
        state,
        registry,
        inference,
        limits,
        instruction,
        control,
        events,
        credentials,
        inference_auth,
        None,
        None,
    )
    .await
    .map_err(|error| error.message)
}

// Only before turn/start or at the owned retained-MCP-result gate. The exact
// canonical append intent and local before-send marker precede executor I/O.
// Traceability scope: caller invokes this before the owned first turn or while the existing retained MCP-result gate is closed. Generic shell/outer asynchronous boundaries remain outside this scope.
//= spec/impl/v2/implementation.md#committed-resolutions-reach-their-consumers
//# A resolution notice MUST be delivered to an active executor at a supported safe input boundary.
async fn deliver_workflow_notice(
    session: &mut Session, host: &mut super::collaboration_notice::Host,
    thread_id: &str, control: &Control,
) -> std::result::Result<(), String> {
    let notice=match host.prepare(control).await? {
        super::collaboration_notice::PrepareDisposition::Notice(notice)=>notice,
        // Continue only the already-required turn. Deferred means an unrelated
        // scan prefix, never permission to omit an admitted continuation.
        super::collaboration_notice::PrepareDisposition::NoChanges
        | super::collaboration_notice::PrepareDisposition::Deferred=>return Ok(()),
    };
    let context=thread_id.to_owned();
    match host.begin_append(&notice.batch_id,&context,control).await? {
        super::collaboration_notice::AppendDisposition::Ready=>{},
        super::collaboration_notice::AppendDisposition::Superseded=>return Ok(()),
        super::collaboration_notice::AppendDisposition::Deferred=>return Err("workflow delivery original append remains unresolved".into()),
    }
    let reply=session.request("thread/inject_items",json!({"threadId":thread_id,"items":[
        {"type":"message","id":format!("borg-workflow-{}",notice.batch_id),"role":"developer","content":[{"type":"input_text","text":notice.text}]}
    ]}),control).await?;
    if reply!=json!({}) {return Err("workflow append reply invalid; original intent retained".into());}
// Traceability scope: only the exact successful inject_items response establishes this context retained the input; unknown replies retain the original intent.
    //= spec/impl/v2/implementation.md#committed-resolutions-reach-their-consumers
    //# A resolution-delivery acknowledgment MUST follow durable retention of the outcome for its receiving continuation.
    host.appended(&notice.batch_id,&context,control).await
}

// Traceability scope: The coding consumer passes its accepted inference settings to startup, thread and turn requests and checks the effective thread settings.
//= spec/impl/v2/implementation.md#definitions-select-execution
//# An execution attempt MUST use the inference settings of its accepted definition revision.
async fn execute_with_credentials_and_collaboration(
    binary: &Path,
    cwd: &Path,
    state: &Path,
    registry: &Path,
    inference: &InferenceSettings,
    limits: &ExecutionLimits,
    instruction: &str,
    control: &Control,
    events: mpsc::Sender<Event>,
    credentials: &[String],
    inference_auth: Option<&[(String, String)]>,
    collaboration: Option<&Collaboration>,
    completion_scope: Option<&CompletionScope>,
) -> std::result::Result<Result, ExecutionFailure> {
    execute_with_credentials_collaboration_and_charter(binary,cwd,state,registry,inference,limits,
        instruction,control,events,credentials,inference_auth,collaboration,completion_scope,None).await
}

async fn execute_with_credentials_collaboration_and_charter(
    binary: &Path,
    cwd: &Path,
    state: &Path,
    registry: &Path,
    inference: &InferenceSettings,
    limits: &ExecutionLimits,
    instruction: &str,
    control: &Control,
    events: mpsc::Sender<Event>,
    credentials: &[String],
    inference_auth: Option<&[(String, String)]>,
    collaboration: Option<&Collaboration>,
    completion_scope: Option<&CompletionScope>,
    initial_charter: Option<&InitialCharterScope>,
) -> std::result::Result<Result, ExecutionFailure> {
    super::boxed_execution(|| execute_observed(binary,cwd,state,registry,inference,limits,instruction,control,events,
        credentials,inference_auth,collaboration,completion_scope,initial_charter,ExecutionLifecycle::default(),None)).await
}

async fn execute_observed(
    binary: &Path, cwd: &Path, state: &Path, registry: &Path,
    inference: &InferenceSettings, limits: &ExecutionLimits, instruction: &str,
    control: &Control, events: mpsc::Sender<Event>, credentials: &[String],
    inference_auth: Option<&[(String, String)]>, collaboration: Option<&Collaboration>,
    completion_scope: Option<&CompletionScope>, initial_charter: Option<&InitialCharterScope>,
    lifecycle: ExecutionLifecycle,
    accepted_profile: Option<&ExecutionProfile>,
) -> std::result::Result<Result, ExecutionFailure> {
    validate_inference(inference)?;
    if initial_charter.is_some() && control.runtime_limits.charter.is_none() {
        return Err("accepted supplied charter policy is absent".into());
    }
    limits.coding.validate()?;
    limits.collaboration.validate()?;
    control.check()?;
    let model = &inference.model;
    let mut workflow = match (collaboration.filter(|h|h.workflow_enabled).and_then(|h|h.memory.as_ref().map(|scope|(h,scope))),control.runtime_limits.workflow.as_ref()) {
        (Some((host,scope)),Some(policy))=>Some(super::collaboration_notice::Host::open(
            crate::client::ApiClient::new(&host.controller_url,control.runtime_limits.clone())?,
            state,&host.caller,&scope.session_id,&scope.agent_id,policy,
        )?),
        _=>None,
    };
    let completion_host = match completion_scope.filter(|_|control.runtime_limits.governance.is_some()) {
        Some(scope) => Some(super::completion::Host::open(
            crate::client::ApiClient::new(&scope.controller_url,control.runtime_limits.clone())?,
            scope.caller.clone(),&scope.session_id,state)?),
        None => None,
    };
    if retry::read(state,control)?.is_some() {
        return Err("retained supplied occurrence requires original recovery; no replacement thread or workspace".into());
    }
    let mut session = Session::start_with_auth(
        binary,
        cwd,
        state,
        registry,
        inference,
        limits,
        control.worker_limits.clone(),
        inference_auth,
    )
    .await?;
    session.lifecycle = lifecycle.clone();
    session.progress = Some(events.clone());
    let mut initial_completion_held = false;
    let mut initial_dispatch_unknown = false;
    let mut completion_started = false;
    let mut pending_completion_held = false;
    // Keep the sequential executor state machine and its large child futures
    // out of each caller's polling frame. Construct inside the existing
    // non-inlined factory; call-site Box::pin still builds a stack temporary.
    let result=super::boxed_execution(|| async {
        session.initialize(control).await?;
        session.preflight_command(cwd,control).await?;
        //= spec/impl/v2/implementation.md#the-supplied-executor-owns-its-inner-loop
        //# An unavailable executor history MUST NOT be reported as a recovered conversation.
        // The pinned schema uses kebab-case SandboxMode; the current web example uses a different spelling.
        let mut thread_config=json!({"model":model,"cwd":cwd,"approvalPolicy":"never","sandbox":"workspace-write","ephemeral":false,"config":{"model_reasoning_effort":inference.reasoning_effort}});
        context_config(&mut thread_config, &limits.coding);
        if let Some(host)=collaboration {
            if !host.bridge_binary.is_absolute() {return Err("collaboration bridge executable must be host absolute path".into());}
            let config=super::codex_mcp::Config{instruction_source:host.instruction_source.clone(),execution_profile:accepted_profile.cloned().filter(|p| matches!(p, ExecutionProfile::CollaborationV2 { .. })).or_else(|| host.workflow_enabled.then_some(ExecutionProfile::CollaborationV2 { publication: None, web_search: None })),controller_url:host.controller_url.clone(),caller:host.caller.clone(),deadline_unix_ms:control.deadline_ms,state:state.to_owned(),runtime_limits:(*control.runtime_limits).clone(),limits:limits.collaboration.clone(),worker_limits:Box::new((*control.worker_limits).clone())};
            config.validate()?;super::codex_mcp::check_stop(&config)?;
            if super::value_contains_credential(&serde_json::to_value(&config).map_err(|_|"bridge configuration encoding failed")?,credentials, &control.worker_limits)? {return Err("bridge identity contains configured credential".into());}
            let config_path=state.join("borg-collaboration-config.json");
            crate::protocol::bounded_json_size(&config,limits.collaboration.config_bytes).map_err(|_|"bridge configuration exceeds retained metadata bound (execution.collaboration.config_bytes)")?;
            process::atomic_json(&config_path,&config)?;
            // Task-scoped stdio only. The key name is inherited for the bridge's
            // finite known-value hygiene; no value is in this retained config.
            thread_config["config"]["mcp_servers"]=json!({(super::codex_mcp::SERVER_NAME):super::codex_mcp::server_config_for_profile(&host.bridge_binary,&config_path,&control.runtime_limits,&limits.collaboration,config.execution_profile.as_ref(),control.deadline_ms.saturating_sub(process::now_ms()).div_ceil(1000).max(1))});
            if workflow.is_some() {super::codex_mcp::enable_memory_gate(&config)?;}
            session.collaboration=Some(config);
        }
        let resume_config=thread_config.clone();
        let thread=match initial_charter {
            Some(scope)=>super::boxed_execution(|| charter_delivery::start_thread(&mut session,scope,state,thread_config,
                inference,control,credentials)).await?,
            None=>session.request("thread/start",thread_config,control).await?,
        };
        session.process.refresh_ownership()?;
        if thread["model"].as_str()!=Some(inference.model.as_str()) || thread["reasoningEffort"].as_str()!=Some(inference.reasoning_effort.as_str()) {return Err("executor effective model or reasoning effort differs from selected profile".into());}
        let thread_id=thread["thread"]["id"].as_str().ok_or("executor did not return a thread id")?.to_owned();
        check_association_ids(&thread_id, None, credentials)?;
        process::atomic_json(&state.join("borg-thread.json"),&json!({"thread_id":thread_id,"cwd":cwd,"turn_start_sent":false}))?;
        let mut pending_completion = None;
        let mut paired_workflow_notice = None;
        let mut initial_input = vec![json!({"type":"text","text":instruction})];
        if let Some(resume)=completion_scope.and_then(|scope|scope.resume.as_ref()) {
            initial_completion_held = true;
            let host=completion_host.as_ref().ok_or("completion resume policy absent")?;
            let required=super::completion::required_events(&resume.evaluation,resume.reset_event.as_ref());
            let notice=if let Some(workflow)=workflow.as_mut() {
                match workflow.prepare_completion(&required,control).await? {
                    super::collaboration_notice::PrepareDisposition::Notice(notice)=>Some(notice),
                    super::collaboration_notice::PrepareDisposition::NoChanges=>None,
                    super::collaboration_notice::PrepareDisposition::Deferred=>return Err("completion initial workflow input deferred".into()),
                }
            } else {None};
            let mut input=host.begin_initial(resume,&thread_id,CompletionTarget::InitialCodexTurn {thread_id:thread_id.clone()},
                notice.as_ref().map(|notice|notice.batch_id.clone()),control).await?
                .ok_or("completion initial input refused")?;
            if let Some(notice)=notice.as_ref() {
                if !matches!(workflow.as_mut().ok_or("completion workflow host absent")?
                    .begin_append(&notice.batch_id,&thread_id,control).await?,super::collaboration_notice::AppendDisposition::Ready) {
                    return Err("completion initial workflow input withheld".into());
                }
                initial_input.push(json!({"type":"text","text":notice.text}));
            }
            initial_input.push(json!({"type":"text","text":input.text()}));
            input.mark_dispatch(&control.runtime_limits)?;
            initial_dispatch_unknown = true;
            pending_completion=Some(input);
            paired_workflow_notice=notice;
        } else if let Some(host)=workflow.as_mut() {
            deliver_workflow_notice(&mut session,host,&thread_id,control).await?;
        }
        process::atomic_json(&state.join("borg-thread.json"),&json!({"thread_id":thread_id,"cwd":cwd,"turn_start_sent":true}))?;
        //= spec/impl/v2/implementation.md#the-supplied-executor-is-qualified-at-its-actual-boundary
        //# A lost start response MUST be reconciled before another executor turn is started for that command.
        // Traceability scope: A failed initial turn/start is reported as uncertain and never resubmitted by this adapter; recovery remains fenced by the original command and unresolved execution.
        //= spec/impl/v2/implementation.md#platform-owned-model-retries
        //# Platform recovery of a supplied executor MUST NOT create another session or repeat an uncertain start merely to implement a provider retry.
        let mut retry_owner=limits.coding.rate_limit_retry.as_ref().map(|policy|retry::Owner::new(state,cwd,&thread_id,policy,json!({
            "caller":completion_scope.map(|s|&s.caller).or_else(||collaboration.map(|s|&s.caller)),
            "session_id":completion_scope.map(|s|&s.session_id),
            "accepted_input_sha256":format!("{}",aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256,
                &serde_json::to_vec(&(inference,limits,instruction)).map_err(|_|"supplied input identity unavailable")?).as_ref().iter().map(|b|format!("{b:02x}")).collect::<String>())
        }),control)).transpose()?;
        if let Some(owner)=retry_owner.as_mut(){owner.before_send(control)?;}
        let params=json!({"threadId":thread_id,"model":model,"effort":inference.reasoning_effort,"cwd":cwd,"approvalPolicy":"never","sandboxPolicy":{"type":"workspaceWrite","writableRoots":[cwd],"networkAccess":true},"input":initial_input});
        let (turn,mut charter_capture)=match initial_charter {
            Some(scope)=>{let (turn,capture)=super::boxed_execution(|| charter_turns::start(&mut session,scope,state,1,&thread_id,params,control,credentials)).await?;(turn,Some(capture))},
            None=>(session.request("turn/start",params,control).await.map_err(|e|format!("turn start outcome uncertain; never resubmitted: {e}"))?,None),
        };
        let mut turn_id=turn["turn"]["id"].as_str().ok_or("turn start outcome uncertain: missing turn id")?.to_owned();
        check_association_ids(&thread_id, Some(&turn_id), credentials)
            .map_err(|_| "turn start outcome uncertain: executor identity contains a configured credential; association withheld")?;
        process::atomic_json(&state.join("borg-thread.json"),&json!({"thread_id":thread_id,"turn_id":turn_id,"cwd":cwd}))?;
        if let Some(input)=pending_completion.as_mut() {
            input.record_turn_start(CompletionBoundary::CodexTurn {thread_id:thread_id.clone(),turn_id:turn_id.clone()},&control.runtime_limits)?;
            completion_started = true;
        }
        initial_dispatch_unknown = false;
        initial_completion_held = false;
        events.send(Event::Started{thread_id:thread_id.clone(),turn_id:turn_id.clone()}).await.map_err(|_|"worker observer closed")?;
        let initial_turn_id = turn_id.clone();
        let mut association_committed = false;
        if let Some(owner)=retry_owner.as_mut() {
            association_barrier(&thread_id,&initial_turn_id,control,&events).await?;
            association_committed=true;
            owner.acknowledged(&turn_id,control,&events).await?;
        }
        let mut completion_in_flight = false;
        // Existing coding admission reports no inner request count (baseline
        // zero). Reserve each additional observed outer turn conservatively.
        let mut reserved_completion_turns = 0u32;
        let mut summary=String::new();
        let mut summary_phase:Option<String>=None;
        let mut observed_usage=ObservedUsage::default();
        loop {
            // Gate selection happens only at the cancellation-safe framed read.
            // It cannot cancel an interactive-error send or discard a prefix.
            let selected=if let Some(message)=session.pending.pop_front(){Ok(SessionInput::Message(message))}else{
                session.next_input(control,Some(&events),workflow.is_some()).await
            };
            let incoming=match selected {
                Err(error)=>Err(error),
                Ok(SessionInput::Message(message))=>Ok(message),
                Ok(SessionInput::MemoryBoundary(gate))=>{
                    if gate.thread_id!=thread_id {return Err("memory boundary thread differs".into());}
                    if paired_workflow_notice.is_none() {
                        if let Some(host)=workflow.as_mut() {deliver_workflow_notice(&mut session,host,&thread_id,control).await?;}
                    }
                    super::codex_mcp::release_memory_boundary(session.collaboration.as_ref().ok_or("memory bridge absent")?,&gate)?;
                    continue;
                }
            };
            let message=match incoming {
                Ok(m)=>m,
                Err(e)=>{
                    // Interrupt is best effort; only the process supervisor supplies stop evidence.
                    let interrupt=json!({"id":session.next_id,"method":"turn/interrupt","params":{"threadId":thread_id,"turnId":turn_id}});
                    let _=tokio::time::timeout(Duration::from_millis(limits.coding.interrupt_timeout_ms),session.send(interrupt)).await;
                    return Err(e);
                }
            };
            if let Some(capture)=charter_capture.as_mut(){capture.item(&message,control)?;}
            if observed_usage.observe(&message,&thread_id,&turn_id) {
                check_association_ids(&thread_id, Some(&turn_id), credentials)?;
                process::atomic_json(&state.join("borg-usage.json"),&json!({"thread_id":thread_id,"turn_id":turn_id,"observed_usage":observed_usage}))?;
            }
            if message["method"]=="item/completed" && message["params"]["threadId"]==thread_id && message["params"]["turnId"]==turn_id && message["params"]["item"]["type"]=="agentMessage" {
                if let Some(text)=message["params"]["item"]["text"].as_str() {
                    summary=text.chars().take(limits.coding.summary_chars).collect();
                    summary_phase=message["params"]["item"]["phase"].as_str().map(str::to_owned);
                }
            }
            if message["method"]=="turn/completed" && message["params"]["threadId"]==thread_id && message["params"]["turn"]["id"]==turn_id {
                let status=message["params"]["turn"]["status"].as_str().ok_or("executor terminal event missing status")?;
                if !matches!(status,"completed"|"failed"|"interrupted") {return Err("unrecognized executor terminal status".into());}
                lifecycle.terminal();
                pending_completion_held=pending_completion.is_some();
                if let (Some(scope),Some(capture))=(initial_charter,charter_capture.take()) {
                    // A commentary item is not a supplied final result. Missing final
                    // metadata stays an audit finding, with the public terminal retained.
                    if let Some(result)=capture.finish(scope,&message,control).await? {
                        summary=result.chars().take(limits.coding.summary_chars).collect();
                    } else if matches!(summary_phase.as_deref(),Some("commentary"|"partial_answer")) {
                        summary.clear();
                    }
                }
                if let Some(owner)=retry_owner.as_mut(){owner.terminal(&message,control,&events,credentials).await?;}
                completion_started = false;
                if crate::supplied_outer::rate_limited(&message["params"]["turn"]) {
                    if let Some(owner)=retry_owner.as_mut() {
                        let cause=owner.rate_cause(control)?;
                        events.send(Event::Progress("Supplied executor rate limited; waiting under accepted retry policy".into())).await.map_err(|_|"worker observer closed")?;
                        reserved_completion_turns=super::boxed_execution(|| session.wait_with_notifications(|| owner.reserve(cause,control,&events),control)).await?;
                        // Reaping the exact old tree is required before a cold
                        // app-server may load the same durable thread. A live
                        // app-server continues without restart or reinjection.
                        if session.process.try_finish_registered()?.is_some() {
                            owner.resume_predecessor(control)?;
                            session.stderr.abort();
                            let bridge=session.collaboration.clone();
                            if let Some(config)=bridge.as_ref(){super::codex_mcp::reopen_after_known_stop(config)?;}
                            session=Session::start_with_auth(binary,cwd,state,registry,inference,limits,
                                control.worker_limits.clone(),inference_auth).await?;
                            session.lifecycle=lifecycle.clone();
                            session.progress=Some(events.clone());
                            session.collaboration=bridge;
                            session.initialize(control).await?;
                            session.preflight_command(cwd,control).await?;
                            let history=session.request("thread/read",json!({"threadId":thread_id,"includeTurns":true}),control).await?;
                            owner.verify_history(&history["thread"],cwd,control)?;
                            let mut params=resume_config.clone();
                            params.as_object_mut().ok_or("supplied resume config invalid")?.remove("ephemeral");
                            params["threadId"]=json!(thread_id);
                            let resumed=session.request("thread/resume",params,control).await?;
                            if resumed["model"]!=inference.model || resumed["reasoningEffort"]!=inference.reasoning_effort {
                                return Err("resumed supplied settings differ from accepted inference".into());
                            }
                            owner.verify_history(&resumed["thread"],cwd,control)?;
                        }
                        let _bridge_boundary=session.collaboration.as_ref().map(super::codex_mcp::continuation_boundary).transpose()?;
                        let previous=turn_id.clone();
                        owner.before_send(control)?;
                        let params=json!({"threadId":thread_id,"model":model,"effort":inference.reasoning_effort,"cwd":cwd,
                            "approvalPolicy":"never","sandboxPolicy":{"type":"workspaceWrite","writableRoots":[cwd],"networkAccess":true},
                            "input":[{"type":"text","text":"Continue the existing task from the retained conversation and workspace after the provider rate-limit failure. Preserve all prior instructions and unresolved completion obligations."}]});
                        let next=if let Some(scope)=initial_charter {
                            let (turn,capture)=super::boxed_execution(|| charter_turns::start(&mut session,scope,state,owner.index(),&thread_id,params,control,credentials)).await?;
                            charter_capture=Some(capture);turn
                        } else {session.request("turn/start",params,control).await?};
                        let next_id=next["turn"]["id"].as_str().ok_or("supplied retry start outcome unknown: missing turn ID")?.to_owned();
                        check_association_ids(&thread_id,Some(&next_id),credentials)?;
                        owner.acknowledged(&next_id,control,&events).await?;
                        if let Some(input)=pending_completion.as_mut() {
                            input.record_supplied_retry(&thread_id,&previous,&next_id,&control.runtime_limits)?;
                            completion_started=true;
                        }
                        turn_id=next_id;summary.clear();summary_phase=None;continue;
                    }
                }
                if summary.is_empty() {summary=format!("Executor turn {status}");}
                if status == "completed" {
                    completion_in_flight = false;
                    if let Some(host) = completion_host.as_ref() {
                        let advance = super::boxed_execution(|| advance_completion(
                            &mut session,host,&mut workflow,&mut pending_completion,&mut paired_workflow_notice,
                            &thread_id,&turn_id,&initial_turn_id,&mut association_committed,&mut completion_in_flight,&mut reserved_completion_turns,
                            cwd,inference,credentials,control,&events,initial_charter,state,&mut charter_capture,&mut retry_owner,
                        )).await;
                        match advance {
                            Ok(CompletionAdvance::Continue(next_turn)) => {
                                completion_started = true;
                                turn_id = next_turn;
                                summary.clear();summary_phase=None;
                                continue;
                            },
                            Ok(CompletionAdvance::Admit) => {},
                            Ok(CompletionAdvance::Held) | Err(_) => {
                                return Ok(Result {success:false,
                                    summary:"Voluntary completion is held; its recorded decision or input requires reconciliation".into(),
                                    thread_id,turn_id,initial_turn_id,completion_blocked:true,
                                    completion_uncertain:completion_in_flight,observed_usage});
                            },
                        }
                    }
                }
                if pending_completion.is_some() {
                    return Ok(Result {success:false,summary:"Completion continuation did not reach a successful consuming boundary".into(),
                        thread_id,turn_id,initial_turn_id,completion_blocked:true,completion_uncertain:false,observed_usage});
                }
                return Ok(Result{success:status=="completed",summary,thread_id,turn_id,initial_turn_id,
                    completion_blocked:false,completion_uncertain:false,observed_usage});
            }
            // Error notifications include willRetry. They are observations, not a terminal result.
        }
    }).await;
    let stopped = session.shutdown().await;
    // Audit after actual process-tree shutdown, including cancellation paths
    // that won before the bridge could persist its static stop marker.
    let mut collaboration_diagnostic = None;
    let collaboration_uncertain = collaboration.is_some_and(|host| {
        let config = super::codex_mcp::Config {
                instruction_source: host.instruction_source.clone(),
                execution_profile: accepted_profile.cloned().filter(|p| matches!(p, ExecutionProfile::CollaborationV2 { .. })).or_else(|| host.workflow_enabled.then_some(ExecutionProfile::CollaborationV2 { publication: None, web_search: None })),
            controller_url: host.controller_url.clone(),
            caller: host.caller.clone(),
            deadline_unix_ms: control.deadline_ms,
            state: state.to_owned(),
            runtime_limits: (*control.runtime_limits).clone(),
            limits: limits.collaboration.clone(),
            worker_limits: Box::new((*control.worker_limits).clone()),
        };
        let disposition = super::codex_mcp::outcome_disposition(&config);
        let unresolved = matches!(
            &disposition,
            Ok(super::codex_mcp::OutcomeDisposition::Unresolved) | Err(_)
        );
        if matches!(disposition, Ok(super::codex_mcp::OutcomeDisposition::Unresolved)) {
            collaboration_diagnostic = super::codex_mcp::diagnostic_reason(&config).ok().flatten();
        }
        unresolved
    });
    if let Err(error) = stopped {
        return Err(ExecutionFailure {
            message: format!("executor stop unverified: {error}"),
            collaboration_uncertain,
            execution_uncertain: lifecycle.uncertain(),
            completion_held:false,
            completion_uncertain:initial_dispatch_unknown || completion_started || (pending_completion_held && lifecycle.uncertain()),
        });
    }
    if collaboration_uncertain {
        return Err(ExecutionFailure {
            message: result
                .err()
                .unwrap_or_else(|| match collaboration_diagnostic {
                    Some(stage) => {
                        format!("collaboration outcome unresolved after executor stop ({stage})")
                    }
                    None => "collaboration outcome unresolved after executor stop".into(),
                }),
            collaboration_uncertain: true,
            execution_uncertain: lifecycle.uncertain(),
            completion_held:initial_completion_held || completion_started || pending_completion_held,
            completion_uncertain:initial_dispatch_unknown || completion_started || (pending_completion_held && lifecycle.uncertain()),
        });
    }
    result.map_err(|message| ExecutionFailure {
        message,
        collaboration_uncertain: false,
        execution_uncertain: lifecycle.uncertain(),
        completion_held:initial_completion_held || completion_started || pending_completion_held,
        completion_uncertain:initial_dispatch_unknown || completion_started || (pending_completion_held && lifecycle.uncertain()),
    })
}

enum CompletionAdvance { Admit, Held, Continue(String) }

/// Called only after the exact current outer turn reports completed. The same
/// thread, task, deadline, accepted configuration and cumulative usage remain.
/// A missing start response never permits another turn/start for this Begin.
async fn advance_completion(
    session: &mut Session,
    host: &super::completion::Host,
    workflow: &mut Option<super::collaboration_notice::Host>,
    pending: &mut Option<super::completion::Input>,
    paired: &mut Option<super::collaboration_notice::Notice>,
    thread_id: &str, turn_id: &str, initial_turn_id: &str,
    association_committed: &mut bool, in_flight: &mut bool, reserved_turns: &mut u32,
    cwd: &Path, inference: &InferenceSettings, credentials: &[String],
    control: &Control, events: &mpsc::Sender<Event>,
    charter: Option<&InitialCharterScope>, state: &Path, capture: &mut Option<charter_turns::Capture>,
    retry_owner: &mut Option<retry::Owner>,
) -> std::result::Result<CompletionAdvance, String> {
    let boundary = CompletionBoundary::CodexTurn {thread_id:thread_id.into(),turn_id:turn_id.into()};
    if !*association_committed {
        let (acknowledged,received) = tokio::sync::oneshot::channel();
        events.send(Event::AssociationBarrier {thread_id:thread_id.into(),initial_turn_id:initial_turn_id.into(),acknowledged})
            .await.map_err(|_|"worker association observer closed")?;
        let mut stopped = control.clone();
        tokio::select! {biased;
            _=stopped.stopped()=>return Err("completion association interrupted".into()),
            result=received=>result.map_err(|_|"worker association acknowledgment absent")??,
        }
        *association_committed = true;
    }
    if let Some(input) = pending.as_mut() {
        if input.delivery_batch_id() != paired.as_ref().map(|notice|notice.batch_id.as_str()) {
            return Err("completion paired input binding differs".into());
        }
        let delivery_ack_operation_id = if let Some(notice) = paired.as_ref() {
            Some(workflow.as_mut().ok_or("completion workflow host absent")?
                .appended_with_receipt(&notice.batch_id,thread_id,control).await?)
        } else { None };
        host.acknowledged(input,boundary.clone(),delivery_ack_operation_id,None,control).await?;
        *pending = None;
        *paired = None;
    }
    let evaluation = match host.evaluate(&boundary,&[],control).await? {
        super::completion::Gate::Admit => return Ok(CompletionAdvance::Admit),
        super::completion::Gate::Held => return Ok(CompletionAdvance::Held),
        super::completion::Gate::Blocked(decision) => decision,
    };
    let required=super::completion::required_events(&evaluation,None);
    let notice = if let Some(workflow) = workflow.as_mut() {
        match workflow.prepare_completion(&required,control).await? {
            super::collaboration_notice::PrepareDisposition::Notice(notice) => Some(notice),
            super::collaboration_notice::PrepareDisposition::NoChanges => None,
            super::collaboration_notice::PrepareDisposition::Deferred => return Ok(CompletionAdvance::Held),
        }
    } else { None };
    let Some(mut input) = host.begin(&boundary,&evaluation.decision,thread_id,
        CompletionTarget::CodexTurn {thread_id:thread_id.into(),previous_turn_id:turn_id.into()},
        notice.as_ref().map(|notice|notice.batch_id.clone()),control).await? else {
        return Ok(CompletionAdvance::Held);
    };
    if let Some(notice) = notice.as_ref() {
        match workflow.as_mut().ok_or("completion workflow host absent")?
            .begin_append(&notice.batch_id,thread_id,control).await? {
            super::collaboration_notice::AppendDisposition::Ready => {},
            super::collaboration_notice::AppendDisposition::Superseded
            | super::collaboration_notice::AppendDisposition::Deferred => return Ok(CompletionAdvance::Held),
        }
    }
    // Both exact recorded bodies are genuine input to one outer turn. The
    // workflow notice does not independently create or count a model request.
    let mut items = vec![json!({"type":"text","text":input.text()})];
    if let Some(notice) = notice.as_ref() {items.push(json!({"type":"text","text":notice.text}));}
    let next_reservation = reserved_turns.checked_add(1).ok_or("coding outer-turn reservation overflow")?;
    if let Some(owner)=retry_owner.as_mut() {
        let reserved=super::boxed_execution(|| session.wait_with_notifications(|| owner.reserve(crate::supplied_outer::Cause::Completion{begin_operation_id:input.feedback().continuation.begin_operation_id.clone()},control,events),control)).await?;
        if reserved!=next_reservation {return Err("shared supplied outer reservation differs".into());}
    }else {
    let (acknowledged,received) = tokio::sync::oneshot::channel();
    events.send(Event::ReserveCompletionTurn {begin_operation_id:input.feedback().continuation.begin_operation_id.clone(),
        model_requests:next_reservation,acknowledged}).await.map_err(|_|"worker reservation observer closed")?;
    let mut stopped = control.clone();
    tokio::select! {biased;
        _=stopped.stopped()=>return Err("completion reservation interrupted".into()),
        result=received=>result.map_err(|_|"worker reservation acknowledgment absent")??,
    }
    *reserved_turns = next_reservation;
    }
    *reserved_turns = next_reservation;
    if let Some(owner)=retry_owner.as_mut(){owner.before_send(control)?;}
    input.mark_dispatch(&control.runtime_limits)?;
    *in_flight = true;
    let params=json!({
        "threadId":thread_id,"model":inference.model,"effort":inference.reasoning_effort,
        "cwd":cwd,"approvalPolicy":"never",
        "sandboxPolicy":{"type":"workspaceWrite","writableRoots":[cwd],"networkAccess":true},
        "input":items,
    });
    let turn=if let Some(scope)=charter {
        let ordinal=next_reservation.checked_add(1).ok_or("supplied outer occurrence overflow")?;
        let (turn,observed)=charter_turns::start(session,scope,state,ordinal,thread_id,params,control,credentials).await?;
        *capture=Some(observed);turn
    }else{session.request("turn/start",params,control).await?};
    let next_turn = turn["turn"]["id"].as_str().ok_or("completion turn start outcome uncertain: missing turn id")?.to_owned();
    check_association_ids(thread_id,Some(&next_turn),credentials)?;
    if let Some(owner)=retry_owner.as_mut(){owner.acknowledged(&next_turn,control,events).await?;}
    input.record_turn_start(CompletionBoundary::CodexTurn {thread_id:thread_id.into(),turn_id:next_turn.clone()},&control.runtime_limits)?;
    // borg-thread.json and Started retain the immutable initial association.
    // Only this exact input journal carries the actual next turn identity.
    *pending = Some(input);
    *paired = notice;
    Ok(CompletionAdvance::Continue(next_turn))
}

// Identity is never rewritten: a matching configured value prevents the owned
// journal/report write. This does not inspect or promise opaque vendor state.
fn check_association_ids(
    thread: &str,
    turn: Option<&str>,
    credentials: &[String],
) -> std::result::Result<(), String> {
    if std::iter::once(thread)
        .chain(turn)
        .any(|id| super::contains_credential(id, credentials))
    {
        return Err(
            "executor identity contains a configured credential; association withheld".into(),
        );
    }
    Ok(())
}

pub fn state_path(root: &Path, attempt_id: &str) -> PathBuf {
    root.join(process::component(attempt_id))
}

#[cfg(test)]
mod credential_tests {
    use super::*;
    use std::{
        os::unix::fs::PermissionsExt,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };
    static SEQUENCE: AtomicU64 = AtomicU64::new(1);
    fn area() -> PathBuf {
        let base =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("artifacts/v2/build-60156282/worker/tests");
        std::fs::create_dir_all(&base).unwrap();
        let root = base.join(format!(
            "credential-peer-{}-{}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        root
    }

    #[test]
    fn codex_auth_environment_is_finite_and_preserves_configuration_errors() {
        let mut read = Vec::new();
        let auth = codex_auth_environment_from(|name| {
            read.push(name.to_owned());
            Ok((name == "OPENAI_IDENTITY_TOKEN_FILE").then(|| "/fixture/identity-token".into()))
        }).unwrap();
        assert_eq!(read, CODEX_AUTH_ENVIRONMENT);
        assert_eq!(auth, vec![("OPENAI_IDENTITY_TOKEN_FILE".into(), "/fixture/identity-token".into())]);
        assert!(codex_auth_environment_from(|_| Ok(None)).unwrap().is_empty());
        assert_eq!(codex_auth_environment_from(|_| Err(())).unwrap_err(),
            "Codex authentication environment is not valid Unicode");
    }

    #[tokio::test]
    async fn codex_server_uses_environment_auth_without_native_api_key() {
        let root = area();
        let state = root.join("state");
        let binary = root.join("auth-peer.py");
        std::fs::write(&binary, r#"#!/usr/bin/python3
import json,os,pathlib,sys
overrides=[sys.argv[i+1] for i,x in enumerate(sys.argv[:-1]) if x=='-c']
assert 'model_provider="openai"' in overrides
assert not any(x.startswith('model_providers.borg_openai.') for x in overrides)
for name in ['OPENAI_FEDERATION_RULE_ID','OPENAI_IDENTITY_TOKEN_FILE','OPENAI_WORKLOAD_IDENTITY_CONTEXT','OPENAI_STAGING_FEDERATION_RULE_ID']:
    assert os.environ[name]=='fixture-'+name
for name in ['OPENAI_API_KEY','CODEX_API_KEY','GITHUB_TOKEN','GH_TOKEN']:
    assert name not in os.environ
assert pathlib.Path(os.environ['CODEX_HOME'])==pathlib.Path.cwd()/'state'
for line in sys.stdin:
    request=json.loads(line)
    if 'id' in request:
        print(json.dumps({'id':request['id'],'result':{}}),flush=True)
"#).unwrap();
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
        let auth = codex_auth_environment_from(|name| Ok(Some(format!("fixture-{name}")))).unwrap();
        let (_cancel, rx) = tokio::sync::watch::channel(false);
        let control = crate::worker::test_control(process::now_ms() + 10_000, rx);
        let registry = root.join("processes");
        let mut session = Session::start_with_auth(
            &binary, &root, &state, &registry, &InferenceSettings::selected(),
            &control.runtime_limits.execution, control.worker_limits.clone(), Some(&auth),
        ).await.unwrap();
        let initialized = session.initialize(&control).await;
        session.shutdown().await.unwrap();
        initialized.unwrap();
        assert!(std::fs::read_dir(&registry).unwrap().all(|entry|
            entry.unwrap().path().extension().is_none_or(|extension| extension != "json")));
    }

    #[tokio::test]
    async fn completion_continuation_keeps_thread_usage_and_initial_association_without_replay() {
        completion_journeys(&["continue","exhausted","lost-start","initial","initial-refused","initial-lost"]).await;
    }
    #[tokio::test]
    async fn retry_and_completion_share_monotone_outer_occurrences_and_reservations() {
        completion_journeys(&["rate-completion","rate-completion-exhausted"]).await;
    }
    async fn completion_journeys(modes:&[&'static str]) {
        use crate::worker::completion::tests as fixture;
        use crate::governance_completion::*;
        use crate::governance::{CompletionDisposition,DecisionPoint,DecisionRef};
        for &mode in modes {
            let root=area(); let state=root.join("state"); process::private_directory(&state).unwrap();
            let binary=root.join("completion-peer.py");
            std::fs::write(root.join("mode"),mode).unwrap();
            std::fs::write(&binary,r#"#!/usr/bin/python3
import json,pathlib,sys
mode=pathlib.Path('mode').read_text()
starts=0
def emit(value): print(json.dumps(value),flush=True)
for line in sys.stdin:
    request=json.loads(line)
    if 'id' not in request: continue
    method=request.get('method')
    if method=='initialize': result={}
    elif method=='command/exec': result={'exitCode':0,'stdout':'','stderr':''}
    elif method=='thread/start':
        result={'thread':{'id':'completion-thread'},'model':'gpt-6-astra','reasoningEffort':'max'}
    elif method=='turn/start':
        starts+=1
        with open('starts.jsonl','a') as output: output.write(json.dumps(request['params'])+'\n')
        if (mode=='lost-start' and starts==2) or (mode=='initial-lost' and starts==1): sys.exit(0)
        turn='turn-'+str(starts)
        emit({'id':request['id'],'result':{'turn':{'id':turn}}})
        emit({'method':'thread/tokenUsage/updated','params':{'threadId':'completion-thread','turnId':turn,'tokenUsage':{'total':{'inputTokens':starts*10,'outputTokens':starts*2}}}})
        if mode.startswith('rate-completion') and starts==2:
            emit({'method':'turn/completed','params':{'threadId':'completion-thread','turn':{'id':turn,'status':'failed','error':{'codexErrorInfo':'rateLimitExceeded','message':'controlled rejection'}}}})
        else:
            emit({'method':'turn/completed','params':{'threadId':'completion-thread','turn':{'id':turn,'status':'completed'}}})
        continue
    else: result={}
    emit({'id':request['id'],'result':result})
"#).unwrap();
            std::fs::set_permissions(&binary,std::fs::Permissions::from_mode(0o700)).unwrap();
            let mut limits=fixture::policy();
            if mode.starts_with("rate-completion") {limits.execution.coding.rate_limit_retry=Some(crate::supplied_outer::RetryPolicy {initial_backoff_ms:1,backoff_multiplier:2,max_backoff_ms:2,jitter_ms:1});}
            if mode=="exhausted" || mode=="initial-refused" {limits.governance.as_mut().unwrap().completion_continuations=0;}
            let first=CompletionBoundary::CodexTurn {thread_id:"completion-thread".into(),turn_id:"turn-1".into()};
            let second=CompletionBoundary::CodexTurn {thread_id:"completion-thread".into(),turn_id:if mode=="rate-completion" {"turn-3"}else{"turn-2"}.into()};
            let mut feedback=if mode.starts_with("initial") {
                fixture::feedback(CompletionBoundary::StoppedResult {report_id:"old-final-candidate".into()},
                    CompletionTarget::InitialCodexTurn {thread_id:"completion-thread".into()})
            } else {fixture::feedback(first.clone(),CompletionTarget::CodexTurn {thread_id:"completion-thread".into(),previous_turn_id:"turn-1".into()})};
            let resume=if mode.starts_with("initial") {
                feedback.continuation.caller.attempt_id="completion-attempt-2".into();
                feedback.continuation.caller.generation=2;
                feedback.continuation.begin_operation_id=begin_id(&feedback.continuation.caller,&feedback.evaluation.decision).unwrap();
                fixture::initial_begin_receipt(&state,&limits,&feedback,mode!="initial-refused");
                Some(CompletionResume {evaluation:feedback.evaluation.clone(),reset_event:None})
            } else {fixture::refused_and_begin(&state,&limits,&feedback,mode!="exhausted");None};
            if mode=="continue" || mode=="initial" || mode=="rate-completion" {
                let completed=if mode=="initial" {first.clone()}else{second.clone()};
                fixture::acknowledge_receipt(&state,&limits,&feedback,completed.clone());
                let caller=feedback.continuation.caller.clone();
                let id=evaluation_id(&caller,&completed).unwrap();
                let mut admitted=feedback.evaluation.clone();
                admitted.boundary=completed.clone();
                admitted.state.subject.attempt_id=caller.attempt_id.clone();
                admitted.state.subject.generation=caller.generation;
                admitted.decision=DecisionRef {operation_id:id.clone(),point:DecisionPoint::Completion};
                admitted.state.disposition=CompletionDisposition::Admit;
                admitted.state.reasons.clear();
                fixture::receipt(&state,&limits,&completed,"evaluate.json",crate::protocol::Operation::EvaluateCompletion(EvaluateCompletion {operation_id:id,caller,boundary:completed.clone(),intervening_requests:vec![]}),
                    crate::protocol::Outcome::Ok {data:crate::protocol::Data::Completion(Box::new(CompletionOutcome::Evaluated(admitted)))});
            }
            let (_cancel,rx)=tokio::sync::watch::channel(false);
            let mut control=crate::worker::test_control(process::now_ms()+10_000,rx);
            control.runtime_limits=Arc::new(limits.clone());
            control.worker_limits=Arc::from(limits.worker.clone());
            let (events,mut received)=mpsc::channel(8);
            let observer=tokio::spawn(async move {
                let mut started=0; let mut barriers=0;let mut outer=Vec::new();
                while let Some(event)=received.recv().await {
                    match event {
                        Event::Started {thread_id,turn_id}=>{assert_eq!(thread_id,"completion-thread");assert_eq!(turn_id,"turn-1");started+=1;},
                        Event::AssociationBarrier {thread_id,initial_turn_id,acknowledged}=>{
                            assert_eq!(thread_id,"completion-thread");assert_eq!(initial_turn_id,"turn-1");barriers+=1;acknowledged.send(Ok(())).unwrap();
                        },
                        Event::ReserveCompletionTurn {model_requests,acknowledged,..}=>{
                            assert_eq!(model_requests,1);acknowledged.send(Ok(())).unwrap();
                        },
                        Event::SuppliedOuter{observation,acknowledged}=>{
                            assert!(mode.starts_with("rate-completion"));
                            if mode=="rate-completion-exhausted" && observation.index()==3 {
                                assert_eq!(observation.phase(),"wait");
                                outer.push((3,"wait"));
                                acknowledged.send(Err("original outer allowance exhausted".into())).unwrap();
                                continue;
                            }
                            let answer=match &observation {
                                crate::supplied_outer::Observation::Waiting{index,cause,..}=>{
                                    assert!(matches!((*index,cause),(2,crate::supplied_outer::Cause::Completion{..})|(3,crate::supplied_outer::Cause::RateLimit{retry_index:1})));
                                    Some(crate::supplied_outer::Wait{index:*index,rate_failures:if *index==3{1}else{0},admitted_at_ms:process::now_ms(),not_before_ms:process::now_ms()})
                                },_=>None,
                            };
                            outer.push((observation.index(),observation.phase()));
                            acknowledged.send(Ok(answer)).unwrap();
                        },
                        Event::Progress(_)=>{},
                    }
                }
                if mode=="rate-completion" {assert_eq!(outer,vec![(1,"ack"),(1,"terminal"),(2,"wait"),(2,"dispatch"),(2,"ack"),(2,"terminal"),(3,"wait"),(3,"dispatch"),(3,"ack"),(3,"terminal")]);}
                if mode=="rate-completion-exhausted" {assert_eq!(outer,vec![(1,"ack"),(1,"terminal"),(2,"wait"),(2,"dispatch"),(2,"ack"),(2,"terminal"),(3,"wait")]);}
                (started,barriers)
            });
            let decision_peer=if mode.starts_with("initial") {None}else {
                Some(fixture::DecisionQueryPeer::start(feedback.evaluation.decision.clone(),
                    fixture::decision_record(&feedback,&limits),&limits).await)
            };
            let controller_url=decision_peer.as_ref().map(|peer|peer.url.clone()).unwrap_or_else(||"http://127.0.0.1:9".into());
            let scope=CompletionScope {controller_url,caller:feedback.continuation.caller.clone(),session_id:"completion-session".into(),resume:resume.map(Box::new)};
            let result=execute_with_credentials_and_collaboration(&binary,&root,&state,&root.join("processes"),
                &InferenceSettings::selected(),&limits.execution,"original task",&control,events,&[],None,None,Some(&scope)).await;
            let observations=observer.await.unwrap();
            if let Some(peer)=decision_peer {peer.finish().await;}
            if mode=="initial-refused" || mode=="initial-lost" {
                let error=result.unwrap_err();
                assert!(error.completion_held);
                assert_eq!(error.completion_uncertain,mode=="initial-lost");
                assert_eq!(error.execution_uncertain,mode=="initial-lost");
                assert_eq!(observations,(0,0));
                if mode=="initial-refused" {assert!(!root.join("starts.jsonl").exists());}
                else {assert_eq!(std::fs::read_to_string(root.join("starts.jsonl")).unwrap().lines().count(),1);}
                continue;
            }
            if mode=="rate-completion-exhausted" {
                let error=result.unwrap_err();
                assert!(error.completion_held,"original input stays held after a known rate terminal");
                assert!(!error.completion_uncertain && !error.execution_uncertain);
                assert_eq!(observations,(1,1));
                assert_eq!(std::fs::read_to_string(root.join("starts.jsonl")).unwrap().lines().count(),2);
                let root=state.join("completion");let mut inputs=Vec::new();
                for caller in std::fs::read_dir(root).unwrap() {for boundary in std::fs::read_dir(caller.unwrap().path()).unwrap() {
                    let p=boundary.unwrap().path().join("input.json");if p.exists(){inputs.push(serde_json::from_slice::<Value>(&std::fs::read(p).unwrap()).unwrap());}
                }}
                assert_eq!(inputs.len(),1);
                assert_eq!(inputs[0]["feedback"]["continuation"]["begin_operation_id"],feedback.continuation.begin_operation_id);
                assert_eq!(inputs[0]["accepted_at"]["turn_id"],"turn-2");
                assert_eq!(inputs[0]["consumed_at"],Value::Null);
                assert_eq!(inputs[0]["started"],true);
                continue;
            }
            let result=result.unwrap();
            assert_eq!(observations,(1,1));
            assert_eq!(result.initial_turn_id,"turn-1");
            assert_eq!(result.thread_id,"completion-thread");
            let starts=std::fs::read_to_string(root.join("starts.jsonl")).unwrap().lines()
                .map(|line|serde_json::from_str::<Value>(line).unwrap()).collect::<Vec<_>>();
            assert_eq!(starts.len(),if mode=="exhausted" || mode=="initial" {1}else if mode=="rate-completion"{3}else{2});
            assert_eq!(starts[0]["input"][0]["text"],"original task");
            if mode=="initial" {
                assert!(starts[0]["input"].as_array().unwrap().iter().any(|item|
                    item["text"].as_str().is_some_and(|text|text.contains("remaining-child"))));
            } else if mode!="exhausted" {
                assert_eq!(starts[1]["threadId"],"completion-thread");
                assert!(starts[1]["input"][0]["text"].as_str().unwrap().contains("remaining-child"));
            }
            assert_eq!(result.success,mode=="continue" || mode=="initial" || mode=="rate-completion");
            assert_eq!(result.completion_blocked,mode!="continue" && mode!="initial" && mode!="rate-completion");
            assert_eq!(result.completion_uncertain,mode=="lost-start");
            assert_eq!(result.observed_usage.model_requests,None);
            assert_eq!(result.observed_usage.input_tokens,Some(if mode=="continue" {20}else if mode=="rate-completion"{30}else{10}));
            let original:Value=serde_json::from_slice(&std::fs::read(state.join("borg-thread.json")).unwrap()).unwrap();
            assert_eq!(original["turn_id"],"turn-1");
            if mode=="rate-completion" {
                assert!(crate::worker::completion::admit_prior_supplied_inputs(&state,&limits).unwrap_err().contains("worker19"));
            }
        }
    }

    #[tokio::test]
    async fn coding_policy_refusals_name_the_exact_metadata_field() {
        let (mut writer, reader) = tokio::io::duplex(64);
        writer.write_all(b"{\"value\":1}\n").await.unwrap();
        let error = line(&mut BufReader::new(reader), &mut Vec::new(), 4).await.unwrap_err();
        assert_eq!(error, "executor message exceeds limit (execution.coding.frame_bytes)");

        for input_bound in [true, false] {
            let root = area();
            let binary = root.join("bounded-peer.py");
            std::fs::write(&binary, r#"#!/usr/bin/python3
import json,sys
for line in sys.stdin:
    request=json.loads(line)
    print(json.dumps({'method':'fixture/one'}),flush=True)
    print(json.dumps({'method':'fixture/two'}),flush=True)
    print(json.dumps({'id':request['id'],'result':{}}),flush=True)
"#).unwrap();
            std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
            let mut limits = (*crate::worker::test_limits()).clone();
            limits.execution.coding.pending_notifications = 1;
            if input_bound { limits.execution.coding.frame_bytes = 16; }
            let mut session = Session::start_with_auth(
                &binary, &root, &root.join("state"), &root.join("processes"),
                &InferenceSettings::selected(), &limits.execution,
                Arc::from(limits.worker.clone()), None,
            ).await.unwrap();
            let (_cancel, rx) = tokio::sync::watch::channel(false);
            let control = crate::worker::test_control(process::now_ms() + 5000, rx);
            let result = if input_bound {
                session.send(json!({"value":"x".repeat(32)})).await
            } else {
                session.request("fixture", json!({}), &control).await.map(|_| ())
            };
            // Termination proof is retained even when the assertion diagnoses a failure.
            session.shutdown().await.unwrap();
            let error = result.unwrap_err();
            assert_eq!(error, if input_bound {
                "executor input exceeds limit (execution.coding.frame_bytes)"
            } else {
                "executor notification backlog exceeds bound (execution.coding.pending_notifications); waiting=other_rpc; queued=1; incoming=other; terminal=0; item=0; usage=0; error=0; other=1"
            });
        }
    }

    #[tokio::test]
    async fn configured_secret_executor_ids_refuse_before_owned_journals_and_started() {
        // Controlled stdio only: no inference key, provider, real Codex or vendor
        // state claim. The production execute path and supervisor are exercised.
        let secret = "fixture-id-secret-731913";
        for mode in ["thread", "turn", "clean"] {
            let root = area();
            let binary = root.join("peer.py");
            std::fs::write(root.join("mode"), mode).unwrap();
            std::fs::write(&binary,r#"#!/usr/bin/python3
import json,pathlib,sys
mode=pathlib.Path('mode').read_text()
secret='fixture-id-secret-731913'
thread=secret if mode=='thread' else 'safe-thread'
turn=secret if mode=='turn' else 'safe-turn'
def emit(value): print(json.dumps(value),flush=True)
for line in sys.stdin:
    request=json.loads(line)
    method=request.get('method')
    if 'id' not in request: continue
    if method=='initialize': result={}
    elif method=='command/exec': result={'exitCode':0,'stdout':'','stderr':''}
    elif method=='thread/start':
        assert request['params']['model']=='gpt-6-astra' and request['params']['config']['model_reasoning_effort']=='max'
        result={'thread':{'id':thread},'model':'gpt-6-astra','reasoningEffort':'max'}
    elif method=='turn/start':
        assert request['params']['model']=='gpt-6-astra' and request['params']['effort']=='max'
        pathlib.Path('turn-started').write_text('yes')
        emit({'method':'thread/tokenUsage/updated','params':{'threadId':thread,'turnId':turn,'tokenUsage':{'total':{'inputTokens':10,'outputTokens':2}}}})
        emit({'method':'turn/completed','params':{'threadId':thread,'turn':{'id':turn,'status':'completed'}}})
        result={'turn':{'id':turn}}
    else: result={}
    emit({'id':request['id'],'result':result})
"#).unwrap();
            std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
            let (cancel, rx) = tokio::sync::watch::channel(false);
            let control = Control {
                deadline_ms: process::now_ms() + 60_000,
                cancel: rx,
                runtime_limits: crate::worker::test_limits(),
                worker_limits: Arc::from(crate::worker::test_limits().worker.clone()),
                validation_runtime: Arc::new(
                    crate::native::ValidationRuntime::new(Arc::from(
                        crate::worker::test_limits().worker.clone(),
                    ))
                    .unwrap(),
                ),
            };
            let (events, mut received) = mpsc::channel(16);
            let state = root.join("state");
            let registry = root.join("processes");
            let result = execute_with_credentials(
                &binary,
                &root,
                &state,
                &registry,
                &InferenceSettings::selected(),
                &crate::worker::test_limits().execution,
                "fixture instruction",
                &control,
                events,
                &[secret.into()],
                None,
            )
            .await;
            let thread_path = state.join("borg-thread.json");
            let usage_path = state.join("borg-usage.json");
            if mode == "clean" {
                let result = result.unwrap();
                assert!(result.success);
                assert!(
                    matches!(received.try_recv().unwrap(),Event::Started {thread_id,turn_id} if thread_id=="safe-thread" && turn_id=="safe-turn")
                );
                let thread: Value =
                    serde_json::from_slice(&std::fs::read(&thread_path).unwrap()).unwrap();
                assert_eq!(thread["thread_id"], "safe-thread");
                assert_eq!(thread["turn_id"], "safe-turn");
                let usage: Value =
                    serde_json::from_slice(&std::fs::read(&usage_path).unwrap()).unwrap();
                assert_eq!(usage["observed_usage"]["input_tokens"], 10);
                assert_eq!(usage["observed_usage"]["output_tokens"], 2);
                assert_eq!(usage["observed_usage"]["model_requests"], Value::Null);
            } else {
                let error = result.unwrap_err();
                assert!(error.contains("association withheld") && !error.contains(secret));
                assert!(received.try_recv().is_err());
                assert!(!usage_path.exists());
                if mode == "thread" {
                    assert!(!root.join("turn-started").exists());
                    assert!(!thread_path.exists());
                } else {
                    assert!(error.contains("turn start outcome uncertain"));
                    assert!(root.join("turn-started").exists());
                    let thread: Value =
                        serde_json::from_slice(&std::fs::read(&thread_path).unwrap()).unwrap();
                    assert_eq!(thread["thread_id"], "safe-thread");
                    assert_eq!(thread["turn_start_sent"], true);
                    assert!(thread.get("turn_id").is_none());
                }
            }
            for path in [&thread_path, &usage_path] {
                if path.exists() {
                    assert!(!std::fs::read_to_string(path).unwrap().contains(secret));
                }
                assert!(!path.with_extension("pending").exists());
            }
            assert!(std::fs::read_dir(registry).unwrap().all(|entry| {
                entry
                    .unwrap()
                    .path()
                    .extension()
                    .is_none_or(|extension| extension != "json")
            }));
            drop(cancel);
        }
    }
    #[tokio::test]
    async fn collaboration_configuration_is_task_scoped_and_transport_failure_quiesces() {
        for mode in [
            "plain",
            "known_refusal",
            "transport_error",
            "unresolved",
            "corrupt",
            "wrong_model",
            "wrong_effort",
        ] {
            let root = area();
            let binary = root.join("peer.py");
            std::fs::write(root.join("mode"), mode).unwrap();
            std::fs::write(&binary,r#"#!/usr/bin/python3
import json,pathlib,sys
mode=pathlib.Path('mode').read_text()
def emit(x): print(json.dumps(x),flush=True)
for line in sys.stdin:
    q=json.loads(line);m=q.get('method')
    if 'id' not in q: continue
    if m=='thread/start':
        pathlib.Path('thread-config.json').write_text(json.dumps(q['params']))
        assert q['params']['model']=='gpt-6-astra' and q['params']['config']['model_reasoning_effort']=='max'
        r={'thread':{'id':'safe-thread'},'model':'gpt-6.1-sol' if mode=='wrong_model' else 'gpt-6-astra','reasoningEffort':'high' if mode=='wrong_effort' else 'max'}
    elif m=='command/exec':r={'exitCode':0,'stdout':'','stderr':''}
    elif m=='turn/start':
        assert q['params']['model']=='gpt-6-astra' and q['params']['effort']=='max'
        pathlib.Path('turn-started').write_text('yes')
        if mode in ('unresolved','corrupt'):
            config=json.loads(pathlib.Path('thread-config.json').read_text())['config']['mcp_servers']['borg_collaboration']
            host=json.loads(pathlib.Path(config['args'][2]).read_text());directory=pathlib.Path(host['state'])/'collaboration-codex';directory.mkdir(mode=0o700)
            journal={'revision':1,'caller':host['caller'],'deadline_unix_ms':host['deadline_unix_ms'],'controller_url':host['controller_url'],'entries':[{'thread_id':'safe-thread','session_id':'safe-session','invocation':{'operation_id':'retained-call','logical_call_id':'exec-controlled','caller':host['caller'],'tool':'collab_memory_set','arguments':{}},'outcome':None,'delivered':False}]}
            path=directory/'calls.json';path.write_text('invalid' if mode=='corrupt' else json.dumps(journal));path.chmod(0o600)
        emit({'id':q['id'],'result':{'turn':{'id':'safe-turn'}}})
        if mode!='plain':
            item={'type':'mcpToolCall','server':'borg_collaboration','id':'exec-controlled','tool':'collab_memory_set','arguments':{},'status':'failed' if mode in ('transport_error','unresolved','corrupt') else 'completed','error':{'message':'controlled transport loss'} if mode in ('transport_error','unresolved','corrupt') else None,'result':None if mode in ('transport_error','unresolved','corrupt') else {'content':[{'type':'text','text':'known committed refusal'}],'isError':True}}
            emit({'method':'item/completed','params':{'threadId':'safe-thread','turnId':'safe-turn','item':item}})
        emit({'method':'turn/completed','params':{'threadId':'safe-thread','turn':{'id':'safe-turn','status':'completed'}}})
        continue
    else:r={}
    emit({'id':q['id'],'result':r})
"#).unwrap();
            std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
            let (_cancel, rx) = tokio::sync::watch::channel(false);
            let control = Control {
                deadline_ms: process::now_ms() + 30_000,
                cancel: rx,
                runtime_limits: crate::worker::test_limits(),
                worker_limits: Arc::from(crate::worker::test_limits().worker.clone()),
                validation_runtime: Arc::new(
                    crate::native::ValidationRuntime::new(Arc::from(
                        crate::worker::test_limits().worker.clone(),
                    ))
                    .unwrap(),
                ),
            };
            let (events, _received) = mpsc::channel(16);
            let state = root.join("state");
            let registry = root.join("processes");
            let host = Collaboration { instruction_source: None,
                controller_url: "http://127.0.0.1:9".into(),
                bridge_binary: PathBuf::from("/host/selected/borg-runtime"),
                memory: None,
                workflow_enabled: false,
                caller: crate::mcp::Caller {
                    task_id: "task-1".into(),
                    attempt_id: "attempt-1".into(),
                    generation: 1,
                    worker_id: "worker-1".into(),
                    incarnation: "incarnation-1".into(),
                },
            };
            let result = execute_with_credentials_and_collaboration(
                &binary,
                &root,
                &state,
                &registry,
                &InferenceSettings::selected(),
                &crate::worker::test_limits().execution,
                "fixture instruction",
                &control,
                events,
                &[],
                None,
                if mode == "plain" { None } else { Some(&host) },
                None,
            )
            .await;
            let config: Value =
                serde_json::from_slice(&std::fs::read(root.join("thread-config.json")).unwrap())
                    .unwrap();
            if mode == "plain" {
                assert_eq!(
                    config["config"]["model_reasoning_effort"],
                    crate::native::REASONING_EFFORT
                );
                assert!(config["config"].get("mcp_servers").is_none());
                assert!(!state.join("borg-collaboration-config.json").exists());
            } else {
                let server = &config["config"]["mcp_servers"][super::super::codex_mcp::SERVER_NAME];
                assert_eq!(server["command"], "/host/selected/borg-runtime");
                assert_eq!(server["required"], true);
                assert_eq!(server["env_vars"], json!([]));
                assert_eq!(server["args"][0], "__collaboration_mcp");
                let retained: super::super::codex_mcp::Config = serde_json::from_slice(
                    &std::fs::read(state.join("borg-collaboration-config.json")).unwrap(),
                )
                .unwrap();
                assert_eq!(retained.caller, host.caller);
                assert_eq!(retained.deadline_unix_ms, control.deadline_ms);
            }
            if matches!(mode, "wrong_model" | "wrong_effort") {
                let error = result.unwrap_err();
                assert!(
                    error
                        .message
                        .contains("effective model or reasoning effort differs")
                );
                assert!(!error.collaboration_uncertain);
                assert!(!root.join("turn-started").exists());
            } else if matches!(mode, "transport_error" | "unresolved" | "corrupt") {
                let failure = result.unwrap_err();
                assert!(failure.message.contains("collaboration transport failed"));
                assert_eq!(
                    failure.collaboration_uncertain,
                    matches!(mode, "unresolved" | "corrupt")
                );
            } else {
                assert!(result.unwrap().success);
            }
            assert!(
                std::fs::read_dir(registry).unwrap().all(|entry| entry
                    .unwrap()
                    .path()
                    .extension()
                    .is_none_or(|ext| ext != "json")),
                "executor process ownership must be actually closed"
            );
        }
    }
    #[tokio::test]
    async fn unsupported_codex_template_model_or_effort_refuses_before_process_or_state() {
        for settings in [
            InferenceSettings {
                model: "gpt-6.1-sol".into(),
                reasoning_effort: "max".into(),
            },
            InferenceSettings {
                model: "gpt-6-astra".into(),
                reasoning_effort: "high".into(),
            },
        ] {
            let root = area();
            let state = root.join("uncreated-state");
            let error = Session::start_with_auth(
                Path::new("/nonexistent-codex"),
                &root,
                &state,
                &root.join("registry"),
                &settings,
                &crate::worker::test_limits().execution,
                Arc::from(crate::worker::test_limits().worker.clone()),
                None,
            )
            .await
            .err()
            .unwrap();
            assert_eq!(error, "template_inference_unsupported");
            assert!(!state.exists());
            assert!(!root.join("registry").exists());
        }
    }
    #[tokio::test]
    async fn effective_executor_model_and_effort_are_checked_before_turn() {
        for mode in ["wrong_model", "wrong_effort"] {
            let root = area();
            let binary = root.join("peer.py");
            std::fs::write(root.join("mode"), mode).unwrap();
            std::fs::write(&binary,r#"#!/usr/bin/python3
import json,pathlib,sys
mode=pathlib.Path('mode').read_text()
assert 'model_reasoning_effort="max"' in sys.argv
assert 'model_context_window=1050000' in sys.argv
for line in sys.stdin:
 q=json.loads(line);m=q.get('method')
 if 'id' not in q:continue
 if m=='thread/start':
  assert q['params']['model']=='gpt-6-astra' and q['params']['config']['model_reasoning_effort']=='max'
  assert q['params']['config']['model_context_window']==1050000
  r={'thread':{'id':'thread-fixture'},'model':'gpt-6.1-sol' if mode=='wrong_model' else 'gpt-6-astra','reasoningEffort':'high' if mode=='wrong_effort' else 'max'}
 elif m=='command/exec':r={'exitCode':0,'stdout':'','stderr':''}
 elif m=='turn/start':pathlib.Path('unexpected-turn').write_text('yes');r={}
 else:r={}
 print(json.dumps({'id':q['id'],'result':r}),flush=True)
"#).unwrap();
            std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
            let (_cancel, rx) = tokio::sync::watch::channel(false);
            let control = Control {
                deadline_ms: process::now_ms() + 30_000,
                cancel: rx,
                runtime_limits: crate::worker::test_limits(),
                worker_limits: Arc::from(crate::worker::test_limits().worker.clone()),
                validation_runtime: Arc::new(
                    crate::native::ValidationRuntime::new(Arc::from(
                        crate::worker::test_limits().worker.clone(),
                    ))
                    .unwrap(),
                ),
            };
            let (events, _received) = mpsc::channel(16);
            let error = execute_with_credentials(
                &binary,
                &root,
                &root.join("state"),
                &root.join("registry"),
                &InferenceSettings::selected(),
                &crate::worker::test_limits().execution,
                "fixture",
                &control,
                events,
                &[],
                None,
            )
            .await
            .unwrap_err();
            assert!(error.contains("effective model or reasoning effort differs"));
            assert!(!root.join("unexpected-turn").exists());
            assert!(
                std::fs::read_dir(root.join("registry")).unwrap().all(|e| e
                    .unwrap()
                    .path()
                    .extension()
                    .is_none_or(|x| x != "json"))
            );
        }
    }
}

#[cfg(test)]
#[tokio::test]
async fn memory_boundary_cancellation_preserves_partial_executor_frame() {
    let (mut writer, reader) = tokio::io::duplex(256);
    let mut reader = BufReader::new(reader);
    let mut frame = Vec::new();
    writer.write_all(b"{\"id\":9,\"res").await.unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            line(
                &mut reader,
                &mut frame,
                crate::worker::test_limits().execution.coding.frame_bytes
            )
        )
        .await
        .is_err()
    );
    assert_eq!(frame, b"{\"id\":9,\"res");
    writer
        .write_all(b"ult\":{}}\n{\"method\":\"next\"}\n")
        .await
        .unwrap();
    assert_eq!(
        line(
            &mut reader,
            &mut frame,
            crate::worker::test_limits().execution.coding.frame_bytes
        )
        .await
        .unwrap(),
        Some(json!({"id":9,"result":{}}))
    );
    assert!(frame.is_empty());
    assert_eq!(
        line(
            &mut reader,
            &mut frame,
            crate::worker::test_limits().execution.coding.frame_bytes
        )
        .await
        .unwrap(),
        Some(json!({"method":"next"}))
    );
}


fn context_args(args: &mut Vec<String>, limits: &crate::limits::CodingLimits) {
    if let Some(window) = limits.model_context_window {
        args.extend(["-c".into(), format!("model_context_window={window}")]);
    }
}
fn context_config(config: &mut Value, limits: &crate::limits::CodingLimits) {
    if let Some(window) = limits.model_context_window {
        config["config"]["model_context_window"] = json!(window);
    }
}
#[cfg(test)]
#[test]
fn accepted_context_window_reaches_both_codex_configuration_sites() {
    let mut coding = crate::limits::RuntimeLimits::seed().unwrap().execution.coding;
    for selected in [None, Some(1_050_000), Some(32_000)] {
        coding.model_context_window = selected;
        coding.validate().unwrap();
        let mut args = vec!["app-server".to_owned()];
        let mut config = json!({"config":{"model_reasoning_effort":"max"}});
        context_args(&mut args, &coding);
        context_config(&mut config, &coding);
        if let Some(window) = selected {
            assert_eq!(args, vec!["app-server".to_owned(), "-c".into(), format!("model_context_window={window}")]);
            assert_eq!(config["config"]["model_context_window"], json!(window));
        } else {
            assert_eq!(args, vec!["app-server"]);
            assert_eq!(config, json!({"config":{"model_reasoning_effort":"max"}}));
        }
    }
}

#[cfg(test)]
#[path = "codex_workflow_join.rs"]
mod workflow_join_tests;

#[cfg(test)]
#[path = "codex_charter_tests.rs"]
mod charter_delivery_tests;

#[cfg(test)]
#[path = "codex_lifecycle_tests.rs"]
mod lifecycle_tests;


#[cfg(test)]
mod metadata_frame_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[tokio::test]
    async fn selected_frame_limit_admits_large_same_thread_second_turn_and_refuses_smaller_bounds() {
        // Controlled stdio exercises the real Session sender and line decoder.
        // It is not a provider context-window or governance continuation claim.
        const FORMER_FRAME_BYTES: usize = 1_048_576;
        const PAYLOAD_BYTES: usize = FORMER_FRAME_BYTES + 4096;
        let request_payload = "q".repeat(PAYLOAD_BYTES);
        let response_payload = "r".repeat(PAYLOAD_BYTES);
        for mode in ["admitted", "outgoing-refused", "incoming-refused"] {
            let root = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("artifacts/v2/build-60156282/worker/tests")
                .join(format!("metadata-frame-{}-{}-{mode}", std::process::id(),
                    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
            std::fs::create_dir_all(&root).unwrap();
            let binary = root.join("peer.py");
            std::fs::write(root.join("mode"), mode).unwrap();
            std::fs::write(&binary, r#"#!/usr/bin/python3
import hashlib,json,pathlib,sys
mode=pathlib.Path('mode').read_text()
former=1048576
payload_bytes=former+4096
turns=0
for raw in sys.stdin.buffer:
    request=json.loads(raw)
    if 'id' not in request: continue
    method=request['method']
    params=request['params']
    text=''
    if method=='initialize': result={}
    elif method=='thread/start':
        assert params['model']=='gpt-6-astra'
        assert params['config']['model_reasoning_effort']=='max'
        result={'thread':{'id':'metadata-frame-thread'},'model':'gpt-6-astra','reasoningEffort':'max'}
    elif method=='turn/start':
        turns+=1
        assert turns<=2 and params['threadId']=='metadata-frame-thread'
        assert params['model']=='gpt-6-astra' and params['effort']=='max'
        text=params['input'][0]['text']
        if turns==1: assert text=='original small turn'
        elif mode=='incoming-refused': assert text=='small continuation input'
        else: assert text=='q'*payload_bytes
        result={'turn':{'id':'metadata-turn-'+str(turns)},'output':('r'*payload_bytes if turns==2 else 'small')}
    else: raise AssertionError('unexpected method '+method)
    response=(json.dumps({'id':request['id'],'result':result},separators=(',',':'))+'\n').encode()
    observation={'id':request['id'],'method':method,'request_wire_bytes':len(raw),
        'input_bytes':len(text.encode()),'input_sha256':hashlib.sha256(text.encode()).hexdigest(),
        'thread_id':params.get('threadId'),'produced_response_wire_bytes':len(response)}
    with open('requests.jsonl','a') as output:
        output.write(json.dumps(observation,separators=(',',':'))+'\n')
    sys.stdout.buffer.write(response)
    sys.stdout.buffer.flush()
"#).unwrap();
            std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
            let mut limits = (*crate::worker::test_limits()).clone();
            limits.execution.coding.frame_bytes = if mode == "admitted" {
                2 * FORMER_FRAME_BYTES
            } else {
                FORMER_FRAME_BYTES
            };
            limits.validate().unwrap();
            let (_cancel, rx) = tokio::sync::watch::channel(false);
            let mut control = crate::worker::test_control(process::now_ms() + 10_000, rx);
            control.runtime_limits = Arc::new(limits.clone());
            control.worker_limits = Arc::from(limits.worker.clone());
            let registry = root.join("processes");
            let mut session = Session::start_with_auth(
                &binary, &root, &root.join("state"), &registry,
                &InferenceSettings::selected(), &limits.execution,
                Arc::from(limits.worker.clone()), None,
            ).await.unwrap();
            let second_text = if mode == "incoming-refused" {
                "small continuation input"
            } else {
                request_payload.as_str()
            };
            let second_params = json!({"threadId":"metadata-frame-thread",
                "input":[{"type":"text","text":second_text}],
                "model":"gpt-6-astra","effort":"max"});
            let second_request_bytes = serde_json::to_vec(&json!({
                "id":4,"method":"turn/start","params":second_params.clone()
            })).unwrap().len();
            // Keep cleanup outside all result assertions, including early refusal.
            let result: std::result::Result<Value, String> = async {
                session.initialize(&control).await?;
                let thread = session.request("thread/start", json!({
                    "model":"gpt-6-astra","config":{"model_reasoning_effort":"max"}
                }), &control).await?;
                if thread["thread"]["id"] != "metadata-frame-thread" {
                    return Err("fixture returned wrong thread".into());
                }
                let first = session.request("turn/start", json!({
                    "threadId":"metadata-frame-thread",
                    "input":[{"type":"text","text":"original small turn"}],
                    "model":"gpt-6-astra","effort":"max"
                }), &control).await?;
                if first != json!({"turn":{"id":"metadata-turn-1"},"output":"small"}) {
                    return Err("fixture returned wrong first turn".into());
                }
                session.request("turn/start", second_params, &control).await
            }.await;
            let shutdown = session.shutdown().await;
            shutdown.unwrap();
            assert!(std::fs::read_dir(&registry).unwrap().all(|entry|
                entry.unwrap().path().extension().is_none_or(|extension| extension != "json")));
            let observations = std::fs::read_to_string(root.join("requests.jsonl")).unwrap()
                .lines().map(|line|serde_json::from_str::<Value>(line).unwrap()).collect::<Vec<_>>();
            assert_eq!(observations.iter().map(|v|v["id"].as_u64().unwrap()).collect::<Vec<_>>(),
                if mode == "outgoing-refused" { vec![1,2,3] } else { vec![1,2,3,4] });
            assert_eq!(observations[0]["method"], "initialize");
            assert_eq!(observations[1]["method"], "thread/start");
            assert_eq!(observations[2]["method"], "turn/start");
            assert_eq!(observations[2]["thread_id"], "metadata-frame-thread");
            if mode == "outgoing-refused" {
                assert!(second_request_bytes > limits.execution.coding.frame_bytes);
                assert_eq!(result.unwrap_err(), "executor input exceeds limit (execution.coding.frame_bytes)");
                // The original small turn exists; the refused second turn never reached the peer.
            } else {
                let last = &observations[3];
                assert_eq!(last["method"], "turn/start");
                assert_eq!(last["thread_id"], "metadata-frame-thread");
                assert_eq!(last["request_wire_bytes"].as_u64().unwrap() as usize, second_request_bytes + 1);
                assert_eq!(last["input_bytes"].as_u64().unwrap() as usize, second_text.len());
                let input_sha256: String = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, second_text.as_bytes())
                    .as_ref().iter().map(|byte| format!("{byte:02x}")).collect();
                assert_eq!(last["input_sha256"], input_sha256);
                let response_bytes = serde_json::to_vec(&json!({"id":4,"result":{
                    "turn":{"id":"metadata-turn-2"},"output":response_payload
                }})).unwrap().len() + 1;
                assert!(response_bytes > FORMER_FRAME_BYTES);
                assert_eq!(last["produced_response_wire_bytes"].as_u64().unwrap() as usize, response_bytes);
                if mode == "admitted" {
                    assert!(second_request_bytes > FORMER_FRAME_BYTES);
                    assert!(second_request_bytes + 1 <= limits.execution.coding.frame_bytes);
                    assert!(response_bytes <= limits.execution.coding.frame_bytes);
                    assert_eq!(result.unwrap(), json!({"turn":{"id":"metadata-turn-2"},"output":response_payload}));
                } else {
                    assert!(second_request_bytes < limits.execution.coding.frame_bytes);
                    assert!(response_bytes > limits.execution.coding.frame_bytes);
                    assert_eq!(result.unwrap_err(), "executor message exceeds limit (execution.coding.frame_bytes)");
                }
                println!("CODING_FRAME_METADATA mode={mode} selected={} request_body={} request_wire={} produced_response_wire={} former={} turns=2",
                    limits.execution.coding.frame_bytes, second_request_bytes, second_request_bytes + 1,
                    response_bytes, FORMER_FRAME_BYTES);
            }
        }
    }
}

async fn association_barrier(thread:&str,turn:&str,control:&Control,events:&mpsc::Sender<Event>)->std::result::Result<(),String> {
    let (acknowledged,received)=tokio::sync::oneshot::channel();
    events.send(Event::AssociationBarrier{thread_id:thread.into(),initial_turn_id:turn.into(),acknowledged}).await.map_err(|_|"worker association observer closed")?;
    let mut stopped=control.clone();
    tokio::select!{biased;_=stopped.stopped()=>Err("initial supplied association interrupted".into()),result=received=>result.map_err(|_|"initial supplied association ACK absent")?,}
}

#[cfg(test)]
#[path = "codex_notification_tests.rs"]
mod notification_tests;
