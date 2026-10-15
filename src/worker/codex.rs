//! Local-stdio adapter qualified against the supplied alpha.10 schema.
use super::process::{self, Control, ManagedProcess};
use crate::limits::{CodingLimits, ExecutionLimits, WorkerLimits};
use crate::protocol::InferenceSettings;
use serde::Serialize;
use serde_json::{Value, json};
use std::sync::Arc;
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    time::Duration,
};
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
    pub controller_url: String,
    pub caller: crate::mcp::Caller,
    pub bridge_binary: PathBuf,
    pub memory: Option<MemoryScope>,
}
#[derive(Debug, Clone)]
pub struct MemoryScope {
    pub agent_id: String,
    pub session_id: String,
}

/// Explicit bridge disposition is kept separate from human diagnostics.
#[derive(Debug)]
pub struct ExecutionFailure {
    pub message: String,
    pub collaboration_uncertain: bool,
}
impl From<String> for ExecutionFailure {
    fn from(message: String) -> Self {
        Self {
            message,
            collaboration_uncertain: false,
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
    Started { thread_id: String, turn_id: String },
    Progress(String),
}
#[derive(Debug)]
pub struct Result {
    pub success: bool,
    pub summary: String,
    pub thread_id: String,
    pub turn_id: String,
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

pub struct Session {
    limits: CodingLimits,
    process: ManagedProcess,
    reader: BufReader<tokio::process::ChildStdout>,
    frame: Vec<u8>,
    stderr: tokio::task::JoinHandle<std::result::Result<(Vec<u8>, bool), String>>,
    next_id: u64,
    pending: VecDeque<Value>,
    collaboration: Option<super::codex_mcp::Config>,
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
        let key = std::env::var("OPENAI_API_KEY").ok();
        Self::start_with_key(
            binary,
            cwd,
            state,
            registry,
            inference,
            limits,
            worker_limits,
            key.as_deref(),
        )
        .await
    }
    async fn start_with_key(
        binary: &Path,
        cwd: &Path,
        state: &Path,
        registry: &Path,
        inference: &InferenceSettings,
        limits: &ExecutionLimits,
        worker_limits: Arc<WorkerLimits>,
        inference_key: Option<&str>,
    ) -> std::result::Result<Self, String> {
        validate_inference(inference)?;
        limits.coding.validate()?;
        worker_limits.validate()?;
        let model = &inference.model;
        process::private_directory(state)?;
        let args = vec![
            "app-server".into(),
            "--listen".into(),
            "stdio://".into(),
            "-c".into(),
            format!(
                "model={}",
                serde_json::to_string(model).map_err(|e| e.to_string())?
            ),
            "-c".into(),
            "model_provider=\"borg_openai\"".into(),
            "-c".into(),
            format!(
                "model_reasoning_effort={}",
                serde_json::to_string(&inference.reasoning_effort)
                    .map_err(|_| "invalid selected effort")?
            ),
            "-c".into(),
            "model_providers.borg_openai.name=\"OpenAI\"".into(),
            "-c".into(),
            "model_providers.borg_openai.base_url=\"https://api.openai.com/v1\"".into(),
            "-c".into(),
            "model_providers.borg_openai.env_key=\"OPENAI_API_KEY\"".into(),
            "-c".into(),
            "model_providers.borg_openai.wire_api=\"responses\"".into(),
            "-c".into(),
            "model_providers.borg_openai.requires_openai_auth=false".into(),
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
        let mut environment = vec![("CODEX_HOME".into(), state.to_string_lossy().into_owned())];
        // The Codex server alone needs its configured inference credential. Ordinary acceptance/Git children do not inherit it.
        if let Some(key) = inference_key {
            environment.push(("OPENAI_API_KEY".into(), key.to_owned()));
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
            collaboration: None,
        })
    }
    async fn send(&mut self, value: Value) -> std::result::Result<(), String> {
        let mut bytes = serde_json::to_vec(&value).map_err(|e| e.to_string())?;
        if bytes.len() > self.limits.frame_bytes {
            return Err("executor input exceeds limit (execution.coding.frame_bytes)".into());
        }
        bytes.push(b'\n');
        self.process
            .stdin
            .as_mut()
            .ok_or("executor stdin closed")?
            .write_all(&bytes)
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
    async fn next_input(
        &mut self,
        control: &Control,
        events: Option<&mpsc::Sender<Event>>,
        allow_memory_boundary: bool,
    ) -> std::result::Result<SessionInput, String> {
        loop {
            let mut stopped = control.clone();
            let bridge = self.collaboration.clone();
            let msg = tokio::select! {
                m=line(&mut self.reader, &mut self.frame, self.limits.frame_bytes)=>m?.ok_or("executor exited before its result")?,
                gate=async {match bridge.as_ref().filter(|_|allow_memory_boundary) {Some(config)=>super::codex_mcp::wait_memory_boundary(config).await,None=>std::future::pending().await}} => return gate.map(SessionInput::MemoryBoundary),
                _=stopped.stopped()=>return Err("executor cancelled or deadline reached".into()),
                _=async {match &bridge {Some(config)=>super::codex_mcp::wait_stop(config).await,None=>std::future::pending::<()>().await}} =>return Err("collaboration bridge stopped; executor must quiesce".into()),
            };
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
            if matches!(
                msg["method"].as_str(),
                Some("item/started" | "item/completed" | "turn/completed")
            ) {
                self.process.refresh_ownership()?;
            }
            if msg.get("method").is_some() && msg.get("id").is_some() {
                //= spec/impl/v2/implementation.md#the-supplied-executor-is-qualified-at-its-actual-boundary
                //# An unknown executor request MUST fail without silently granting authority.
                self.send(json!({"id":msg["id"],"error":{"code":-32601,"message":"This worker does not grant interactive requests"}})).await?;
                continue;
            }
            if msg["method"] == "item/agentMessage/delta" {
                if let (Some(e), Some(s)) = (events, msg["params"]["delta"].as_str()) {
                    let _ = e.try_send(Event::Progress(
                        s.chars().take(self.limits.progress_chars).collect(),
                    ));
                }
            }
            return Ok(SessionInput::Message(msg));
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
                if self.pending.len() >= self.limits.pending_notifications {
                    return Err("executor notification backlog exceeds bound (execution.coding.pending_notifications)".into());
                }
                self.pending.push_back(msg);
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
    let result = async {
        validate_inference(inference)?;
        let credentials = super::configured_credentials(&control.runtime_limits)?;
        let key = std::env::var("OPENAI_API_KEY").ok();
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
            &credentials,
            key.as_deref(),
            collaboration,
        )
        .await
    }
    .await;
    result.map_err(|mut error: ExecutionFailure| {
        // Also audit early setup/cancellation errors that never started a process.
        if let Some(host) = collaboration {
            let config = super::codex_mcp::Config {
                controller_url: host.controller_url.clone(),
                caller: host.caller.clone(),
                deadline_unix_ms: control.deadline_ms,
                state: state.to_owned(),
                runtime_limits: (*control.runtime_limits).clone(),
                limits: limits.collaboration.clone(),
                worker_limits: (*control.worker_limits).clone(),
            };
            if matches!(
                super::codex_mcp::outcome_disposition(&config),
                Ok(super::codex_mcp::OutcomeDisposition::Unresolved) | Err(_)
            ) {
                error.collaboration_uncertain = true;
            }
        }
        error
    })
}

// The private fixture supplies only synthetic values and no inference key.
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
    inference_key: Option<&str>,
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
        inference_key,
        None,
    )
    .await
    .map_err(|error| error.message)
}

// Traceability scope: Supported coding delivery uses pre-turn injection or the retained-result gate below; delivery itself sends no turn interrupt. Arbitrary executor-internal boundaries remain unqualified.
//= spec/impl/v2/implementation.md#shared-memory-changes-reach-sessions
//# Memory-change notification delivery MUST NOT interrupt a tool that has begun execution.
async fn deliver_memory_notice(
    session: &mut Session,
    host: &mut super::memory_notice::Host,
    thread_id: &str,
    control: &Control,
) -> std::result::Result<(), String> {
    let Some(notice) = host.prepare(control).await? else {
        return Ok(());
    };
    let context = format!("codex:{thread_id}");
    if !matches!(
        host.begin_append(&notice.batch_id, &context)?,
        super::memory_notice::AppendDisposition::Ready
    ) {
        return Ok(()); // Exact retained ambiguous batch: never reinject this context.
    }
    let reply=session.request("thread/inject_items",json!({"threadId":thread_id,"items":[
        {"type":"message","id":format!("borg-memory-{}",notice.batch_id),"role":"developer","content":[{"type":"input_text","text":notice.text}]}
    ]}),control).await?;
    if reply != json!({}) {
        return Err("memory append reply invalid; batch retained".into());
    }
    // ACK means appended, not consumed. A lost controller ACK reuses the exact
    // saved mutation; no second executor append is authorized by it.
    host.appended(&notice.batch_id, &context, control).await
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
    inference_key: Option<&str>,
    collaboration: Option<&Collaboration>,
) -> std::result::Result<Result, ExecutionFailure> {
    validate_inference(inference)?;
    limits.coding.validate()?;
    limits.collaboration.validate()?;
    limits.memory_notice.validate()?;
    control.check()?;
    let model = &inference.model;
    let mut memory = match collaboration.and_then(|h| h.memory.as_ref().map(|scope| (h, scope))) {
        Some((host, scope)) => Some(super::memory_notice::Host::open(
            crate::client::ApiClient::new(&host.controller_url, control.runtime_limits.clone())?,
            state,
            &host.caller,
            &scope.session_id,
            &scope.agent_id,
            &limits.memory_notice,
        )?),
        None => None,
    };
    let mut session = Session::start_with_key(
        binary,
        cwd,
        state,
        registry,
        inference,
        limits,
        control.worker_limits.clone(),
        inference_key,
    )
    .await?;
    let result=async {
        session.initialize(control).await?;
        session.preflight_command(cwd,control).await?;
        //= spec/impl/v2/implementation.md#the-supplied-executor-owns-its-inner-loop
        //# An unavailable executor history MUST NOT be reported as a recovered conversation.
        // The pinned schema uses kebab-case SandboxMode; the current web example uses a different spelling.
        let mut thread_config=json!({"model":model,"cwd":cwd,"approvalPolicy":"never","sandbox":"workspace-write","ephemeral":false,"config":{"model_reasoning_effort":inference.reasoning_effort}});
        if let Some(host)=collaboration {
            if !host.bridge_binary.is_absolute() {return Err("collaboration bridge executable must be host absolute path".into());}
            let config=super::codex_mcp::Config{controller_url:host.controller_url.clone(),caller:host.caller.clone(),deadline_unix_ms:control.deadline_ms,state:state.to_owned(),runtime_limits:(*control.runtime_limits).clone(),limits:limits.collaboration.clone(),worker_limits:(*control.worker_limits).clone()};
            config.validate()?;super::codex_mcp::check_stop(&config)?;
            if super::value_contains_credential(&serde_json::to_value(&config).map_err(|_|"bridge configuration encoding failed")?,credentials, &control.worker_limits)? {return Err("bridge identity contains configured credential".into());}
            let config_path=state.join("borg-collaboration-config.json");
            crate::protocol::bounded_json_size(&config,limits.collaboration.config_bytes).map_err(|_|"bridge configuration exceeds retained metadata bound (execution.collaboration.config_bytes)")?;
            process::atomic_json(&config_path,&config)?;
            // Task-scoped stdio only. The key name is inherited for the bridge's
            // finite known-value hygiene; no value is in this retained config.
            thread_config["config"]["mcp_servers"]=json!({(super::codex_mcp::SERVER_NAME):super::codex_mcp::server_config(&host.bridge_binary,&config_path,&control.runtime_limits,&limits.collaboration,control.deadline_ms.saturating_sub(process::now_ms()).div_ceil(1000).max(1))});
            if memory.is_some() {super::codex_mcp::enable_memory_gate(&config)?;}
            session.collaboration=Some(config);
        }
        let thread=session.request("thread/start",thread_config,control).await?;
        session.process.refresh_ownership()?;
        if thread["model"].as_str()!=Some(inference.model.as_str()) || thread["reasoningEffort"].as_str()!=Some(inference.reasoning_effort.as_str()) {return Err("executor effective model or reasoning effort differs from selected profile".into());}
        let thread_id=thread["thread"]["id"].as_str().ok_or("executor did not return a thread id")?.to_owned();
        check_association_ids(&thread_id, None, credentials)?;
        process::atomic_json(&state.join("borg-thread.json"),&json!({"thread_id":thread_id,"cwd":cwd,"turn_start_sent":false}))?;
        if let Some(host)=memory.as_mut() {
            deliver_memory_notice(&mut session,host,&thread_id,control).await?;
        }
        process::atomic_json(&state.join("borg-thread.json"),&json!({"thread_id":thread_id,"cwd":cwd,"turn_start_sent":true}))?;
        //= spec/impl/v2/implementation.md#the-supplied-executor-is-qualified-at-its-actual-boundary
        //# A lost start response MUST be reconciled before another executor turn is started for that command.
        let turn=session.request("turn/start",json!({"threadId":thread_id,"model":model,"effort":inference.reasoning_effort,"cwd":cwd,"approvalPolicy":"never","sandboxPolicy":{"type":"workspaceWrite","writableRoots":[cwd],"networkAccess":true},"input":[{"type":"text","text":instruction}]}),control).await.map_err(|e|format!("turn start outcome uncertain; never resubmitted: {e}"))?;
        let turn_id=turn["turn"]["id"].as_str().ok_or("turn start outcome uncertain: missing turn id")?.to_owned();
        check_association_ids(&thread_id, Some(&turn_id), credentials)
            .map_err(|_| "turn start outcome uncertain: executor identity contains a configured credential; association withheld")?;
        process::atomic_json(&state.join("borg-thread.json"),&json!({"thread_id":thread_id,"turn_id":turn_id,"cwd":cwd}))?;
        events.send(Event::Started{thread_id:thread_id.clone(),turn_id:turn_id.clone()}).await.map_err(|_|"worker observer closed")?;
        let mut summary=String::new();
        let mut observed_usage=ObservedUsage::default();
        loop {
            // Gate selection happens only at the cancellation-safe framed read.
            // It cannot cancel an interactive-error send or discard a prefix.
            let selected=if let Some(message)=session.pending.pop_front(){Ok(SessionInput::Message(message))}else{
                session.next_input(control,Some(&events),memory.is_some()).await
            };
            let incoming=match selected {
                Err(error)=>Err(error),
                Ok(SessionInput::Message(message))=>Ok(message),
                Ok(SessionInput::MemoryBoundary(gate))=>{
                    if gate.thread_id!=thread_id {return Err("memory boundary thread differs".into());}
                    if let Some(host)=memory.as_mut() {deliver_memory_notice(&mut session,host,&thread_id,control).await?;}
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
            if observed_usage.observe(&message,&thread_id,&turn_id) {
                check_association_ids(&thread_id, Some(&turn_id), credentials)?;
                process::atomic_json(&state.join("borg-usage.json"),&json!({"thread_id":thread_id,"turn_id":turn_id,"observed_usage":observed_usage}))?;
            }
            if message["method"]=="item/completed" && message["params"]["item"]["type"]=="agentMessage" {
                if let Some(text)=message["params"]["item"]["text"].as_str() {summary=text.chars().take(limits.coding.summary_chars).collect();}
            }
            if message["method"]=="turn/completed" && message["params"]["threadId"]==thread_id && message["params"]["turn"]["id"]==turn_id {
                let status=message["params"]["turn"]["status"].as_str().ok_or("executor terminal event missing status")?;
                if !matches!(status,"completed"|"failed"|"interrupted") {return Err("unrecognized executor terminal status".into());}
                if summary.is_empty() {summary=format!("Executor turn {status}");}
                return Ok(Result{success:status=="completed",summary,thread_id,turn_id,observed_usage});
            }
            // Error notifications include willRetry. They are observations, not a terminal result.
        }
    }.await;
    let stopped = session.shutdown().await;
    // Audit after actual process-tree shutdown, including cancellation paths
    // that won before the bridge could persist its static stop marker.
    let collaboration_uncertain = collaboration.is_some_and(|host| {
        let config = super::codex_mcp::Config {
            controller_url: host.controller_url.clone(),
            caller: host.caller.clone(),
            deadline_unix_ms: control.deadline_ms,
            state: state.to_owned(),
            runtime_limits: (*control.runtime_limits).clone(),
            limits: limits.collaboration.clone(),
            worker_limits: (*control.worker_limits).clone(),
        };
        matches!(
            super::codex_mcp::outcome_disposition(&config),
            Ok(super::codex_mcp::OutcomeDisposition::Unresolved) | Err(_)
        )
    });
    if let Err(error) = stopped {
        return Err(ExecutionFailure {
            message: format!("executor stop unverified: {error}"),
            collaboration_uncertain,
        });
    }
    if collaboration_uncertain {
        return Err(ExecutionFailure {
            message: result
                .err()
                .unwrap_or_else(|| "collaboration outcome unresolved after executor stop".into()),
            collaboration_uncertain: true,
        });
    }
    result.map_err(|message| ExecutionFailure {
        message,
        collaboration_uncertain: false,
    })
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
            let mut session = Session::start_with_key(
                &binary, &root, &root.join("state"), &root.join("processes"),
                &InferenceSettings::selected(), &limits.execution,
                Arc::new(limits.worker.clone()), None,
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
                "executor notification backlog exceeds bound (execution.coding.pending_notifications)"
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
                worker_limits: Arc::new(crate::worker::test_limits().worker.clone()),
                validation_runtime: Arc::new(
                    crate::native::ValidationRuntime::new(Arc::new(
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
                worker_limits: Arc::new(crate::worker::test_limits().worker.clone()),
                validation_runtime: Arc::new(
                    crate::native::ValidationRuntime::new(Arc::new(
                        crate::worker::test_limits().worker.clone(),
                    ))
                    .unwrap(),
                ),
            };
            let (events, _received) = mpsc::channel(16);
            let state = root.join("state");
            let registry = root.join("processes");
            let host = Collaboration {
                controller_url: "http://127.0.0.1:9".into(),
                bridge_binary: PathBuf::from("/host/selected/borg-runtime"),
                memory: None,
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
                assert_eq!(server["env_vars"], json!(["OPENAI_API_KEY"]));
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
            let error = Session::start_with_key(
                Path::new("/nonexistent-codex"),
                &root,
                &state,
                &root.join("registry"),
                &settings,
                &crate::worker::test_limits().execution,
                Arc::new(crate::worker::test_limits().worker.clone()),
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
for line in sys.stdin:
 q=json.loads(line);m=q.get('method')
 if 'id' not in q:continue
 if m=='thread/start':
  assert q['params']['model']=='gpt-6-astra' and q['params']['config']['model_reasoning_effort']=='max'
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
                worker_limits: Arc::new(crate::worker::test_limits().worker.clone()),
                validation_runtime: Arc::new(
                    crate::native::ValidationRuntime::new(Arc::new(
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
