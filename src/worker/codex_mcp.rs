//! Task-scoped stdio MCP adapter. This is host correlation, not a new auth boundary.
//! The sole bridge owner retains an immutable occurrence before HTTP; lost replies
//! are reconciled by original identity. Only closed read adapters can redispatch
//! after validated absence. No unresolved result reaches the supplied executor.
use super::process;
use crate::read_retention::{CallBody,ReadReference};
use crate::limits::{CollaborationLimits, RuntimeLimits, WorkerLimits};
use crate::{
    mcp::{self, CallResult, Caller, Client, Invocation},
    protocol::*,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    io::{BufRead, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

pub const SERVER_NAME: &str = "borg_collaboration";
const REVISION: u32 = 2;
const READ_REVISION:u32=3;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Exact optional source from the accepted RunAttempt, never model supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruction_source: Option<crate::protocol::RepositoryReadSource>,
    /// Host-derived V2 capability projection. Historical absence is exact V1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_profile: Option<ExecutionProfile>,
    pub runtime_limits: RuntimeLimits,
    pub limits: CollaborationLimits,
    pub worker_limits: WorkerLimits,
    pub controller_url: String,
    pub caller: Caller,
    pub deadline_unix_ms: u64,
    /// Existing per-attempt Codex state; never model supplied.
    pub state: PathBuf,
}
impl Config {
    pub fn validate(&self) -> Result<(), String> {
        self.runtime_limits.validate()?;
        if let Some(source) = &self.instruction_source {
            source.validate_instruction_binding(&self.runtime_limits)?;
        }
        if self.execution_profile.as_ref().is_some_and(|profile|
            !matches!(profile,ExecutionProfile::CollaborationV2{..})) {
            return Err("bridge workflow capability differs from selected metadata".into());
        }
        self.caller.validate(&self.runtime_limits.transport)?;
        self.limits.validate()?;
        self.worker_limits.validate()?;
        if !self.state.is_absolute()
            || self.deadline_unix_ms == 0
            || self.deadline_unix_ms > i64::MAX as u64
        {
            return Err("invalid collaboration bridge configuration".into());
        }
        self.client()?;
        Ok(())
    }
    fn profile(&self) -> ExecutionProfile {
        self.execution_profile.clone().unwrap_or(ExecutionProfile::CollaborationV1 { web_search: None })
    }
    fn client(&self) -> Result<Client,String> {
        Client::new_for_task(&self.controller_url, self.caller.clone(), self.runtime_limits.clone(),
            self.profile(), self.instruction_source.clone())
    }
    fn catalogue(&self) -> Vec<mcp::Tool> {
        let mut selected=self.runtime_limits.clone();
        selected.execution.collaboration.capabilities=self.limits.capabilities.clone();
        mcp::catalogue_for_task(&selected, Some(&self.profile()), self.instruction_source.as_ref())
    }
    pub fn directory(&self) -> PathBuf {
        self.state.join("collaboration-codex")
    }
    pub fn stop_path(&self) -> PathBuf {
        self.directory().join("stopped.json")
    }
}

// A host-held MCP reply is an actual continuation boundary. The original
// retained tool result is not released until the parent has handled this gate.
// Merely seeing an executor item event is insufficient: generic active-turn
// injection can cause an otherwise unnecessary additional model request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct MemoryBoundary {
    revision: u32,
    caller: Caller,
    deadline_unix_ms: u64,
    index: usize,
    pub thread_id: String,
    logical_call_id: String,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MemoryGate {
    caller: Caller,
    deadline_unix_ms: u64,
}

pub(super) fn enable_memory_gate(config: &Config) -> Result<(), String> {
    config.validate()?;
    process::private_directory(&config.directory())?;
    bounded_save(
        &config.directory().join("memory-gate.json"),
        &MemoryGate {
            caller: config.caller.clone(),
            deadline_unix_ms: config.deadline_unix_ms,
        },
        config.limits.gate_bytes,
        "execution.collaboration.gate_bytes",
    )
}
fn memory_gate_enabled(config: &Config) -> Result<bool, String> {
    // The retained filename predates workflow delivery. A memory-only legacy
    // gate must never hold a tool result after automatic memory notices retire.
    if !matches!(config.execution_profile, Some(ExecutionProfile::CollaborationV2 { .. }))
        || config.runtime_limits.workflow.is_none()
    {
        return Ok(false);
    }
    let Some(bytes) = read_private(
        &config.directory().join("memory-gate.json"),
        config.limits.gate_bytes,
        "execution.collaboration.gate_bytes",
    )?
    else {
        return Ok(false);
    };
    let gate: MemoryGate = decode(&bytes, config.limits.gate_bytes)?;
    if gate.caller != config.caller || gate.deadline_unix_ms != config.deadline_unix_ms {
        return Err("memory gate binding differs".into());
    }
    Ok(true)
}
fn read_memory_boundary(config: &Config) -> Result<Option<MemoryBoundary>, String> {
    let Some(bytes) = read_private(
        &config.directory().join("memory-boundary.json"),
        config.limits.boundary_bytes,
        "execution.collaboration.boundary_bytes",
    )?
    else {
        return Ok(None);
    };
    let gate: MemoryBoundary = decode(&bytes, config.limits.boundary_bytes)?;
    if gate.revision != 1
        || gate.caller != config.caller
        || gate.deadline_unix_ms != config.deadline_unix_ms
        || !mcp::valid_identity(
            &gate.thread_id,
            config.runtime_limits.transport.identity_bytes,
        )
        || !mcp::valid_identity(
            &gate.logical_call_id,
            config.runtime_limits.transport.identity_bytes,
        )
    {
        return Err("memory boundary binding differs".into());
    }
    let bytes = read_private(
        &config.directory().join("calls.json"),
        config.limits.journal_bytes,
        "execution.collaboration.journal_bytes",
    )?
    .ok_or("memory boundary journal absent")?;
    let journal: Journal = decode(&bytes, config.limits.journal_bytes)?;
    if !matches!(journal.revision,REVISION|READ_REVISION)
        || journal.caller != config.caller
        || journal.deadline_unix_ms != config.deadline_unix_ms
        || journal.controller_url != config.controller_url
        || journal.runtime_limits != config.runtime_limits
        || journal.limits != config.limits
        || journal.worker_limits != config.worker_limits
        || journal.instruction_source != config.instruction_source
    {
        return Err("memory boundary journal binding differs".into());
    }
    let entry = journal
        .entries
        .get(gate.index)
        .ok_or("memory boundary occurrence absent")?;
    if entry.thread_id != gate.thread_id
        || entry.invocation.logical_call_id != gate.logical_call_id
        || entry.invocation.caller != config.caller
        || entry.outcome.is_none()
    {
        return Err("memory boundary occurrence differs".into());
    }
    Ok(Some(gate))
}
fn boundary_released(config: &Config, gate: &MemoryBoundary) -> Result<bool, String> {
    let Some(bytes) = read_private(
        &config.directory().join("memory-release.json"),
        config.limits.boundary_bytes,
        "execution.collaboration.boundary_bytes",
    )?
    else {
        return Ok(false);
    };
    let released: MemoryBoundary = decode(&bytes, config.limits.boundary_bytes)?;
    if released.caller != config.caller || released.deadline_unix_ms != config.deadline_unix_ms {
        return Err("memory release binding differs".into());
    }
    Ok(released == *gate)
}
pub(super) async fn wait_memory_boundary(config: &Config) -> Result<MemoryBoundary, String> {
    loop {
        check_stop(config)?;
        if process::now_ms() >= config.deadline_unix_ms {
            return Err("memory boundary deadline reached".into());
        }
        if let Some(gate) = read_memory_boundary(config)? {
            if !boundary_released(config, &gate)? {
                return Ok(gate);
            }
        }
        tokio::time::sleep(Duration::from_millis(config.limits.liveness_interval_ms)).await;
    }
}
pub(super) fn release_memory_boundary(
    config: &Config,
    gate: &MemoryBoundary,
) -> Result<(), String> {
    if read_memory_boundary(config)?.as_ref() != Some(gate) {
        return Err("memory boundary changed before release".into());
    }
    bounded_save(
        &config.directory().join("memory-release.json"),
        gate,
        config.limits.boundary_bytes,
        "execution.collaboration.boundary_bytes",
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Entry {
    thread_id: String,
    session_id: String,
    invocation: Invocation,
    outcome: Option<CallBody>,
    delivered: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Journal {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    instruction_source: Option<crate::protocol::RepositoryReadSource>,
    runtime_limits: RuntimeLimits,
    limits: CollaborationLimits,
    worker_limits: WorkerLimits,
    revision: u32,
    caller: Caller,
    deadline_unix_ms: u64,
    controller_url: String,
    entries: Vec<Entry>,
}
fn reference_owner(config:&Config)->Result<bool,String> {
    let Some(root)=config.state.parent().and_then(Path::parent) else {return Ok(false)};
    if config.state!=super::codex::state_path(&root.join("codex"),&config.caller.attempt_id) {return Ok(false)}
    super::handoff::admits_read_references(root,&config.worker_limits)
}
fn check_body(config:&Config,revision:u32,entry:&Entry,body:&CallBody)->Result<(),String> {
    match body {
        CallBody::Inline(value)=>Bridge::<Client>::check_result(&entry.invocation,value,config.limits.complete_result_bytes),
        CallBody::Reference {committed_read_reference:value}=> {
            if revision!=READ_REVISION || !entry.delivered || !reference_owner(config)? {return Err("committed read reference requires delivered journal3/worker20".into())}
            value.validate(&entry.invocation)?;
            bounded_json_size(value,config.limits.complete_result_bytes)?;
            Ok(())
        }
    }
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Stop {
    revision: u32,
    caller: Caller,
    reason: String,
}

fn read_private(path: &Path, bound: usize, field: &str) -> Result<Option<Vec<u8>>, String> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let file = match std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(0x20000 | 0x800)
        .open(path)
    {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("collaboration journal unavailable".into()),
    };
    let meta = file
        .metadata()
        .map_err(|_| "collaboration journal metadata unavailable")?;
    let uid = std::fs::metadata("/proc/self")
        .map_err(|_| "process owner unavailable")?
        .uid();
    if !meta.is_file() || meta.uid() != uid || meta.mode() & 0o077 != 0 {
        return Err("collaboration journal type, owner, mode or bound differs".into());
    }
    if meta.len() > bound as u64 {
        return Err(format!("collaboration journal exceeds bound ({field})"));
    }
    let mut bytes = Vec::new();
    file.take(bound as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "collaboration journal unreadable")?;
    if bytes.len() > bound {
        return Err(format!("collaboration journal exceeds bound ({field})"));
    }
    Ok(Some(bytes))
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8], bound: usize) -> Result<T, String> {
    serde_json::from_value(mcp::strict::parse(bytes, bound)?)
        .map_err(|_| "invalid collaboration journal or frame".into())
}
fn bounded_save(
    path: &Path,
    value: &impl Serialize,
    bound: usize,
    field: &str,
) -> Result<(), String> {
    bounded_json_size(value, bound)
        .map_err(|_| format!("collaboration journal exceeds bound ({field})"))?;
    process::atomic_json(path, value).map_err(|_| "collaboration journal commit failed".into())
}
fn safe_value(
    value: &impl Serialize,
    credentials: &[String],
    limits: &WorkerLimits,
) -> Result<(), String> {
    let value = serde_json::to_value(value).map_err(|_| "collaboration encoding failed")?;
    if super::value_contains_credential(&value, credentials, limits)? {
        Err("collaboration value contains a configured credential".into())
    } else {
        Ok(())
    }
}


// Diagnostic-only record: never an outcome, delivery acknowledgement, or
// authority to retry. One first-failure frame per occurrence uses the existing
// private gate byte limit and never records transport text or response bodies.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum FailureStage {
    Transport,
    HttpStatus,
    ResponseRead,
    ResponseLimit,
    ResponseSyntax,
    ResponseCorrelation,
    MethodRefused,
    ToolEnvelope,
    ControllerReply,
    UncommittedReply,
    ResultLimit,
    ReceiptContract,
    LookupAbsent,
    Unclassified,
}
impl FailureStage {
    fn from_error(error: &str) -> Self {
        match error {
            "MCP transport outcome unknown; reconcile original occurrence" => Self::Transport,
            "MCP HTTP outcome unavailable; reconcile original occurrence" => Self::HttpStatus,
            "MCP response incomplete; reconcile original occurrence" => Self::ResponseRead,
            "MCP response bound; reconcile original occurrence" => Self::ResponseLimit,
            "Invalid or duplicate JSON" => Self::ResponseSyntax,
            "MCP response correlation differs" => Self::ResponseCorrelation,
            "MCP method refused; reconcile original occurrence" => Self::MethodRefused,
            "invalid MCP tool result" | "invalid MCP content" => Self::ToolEnvelope,
            "invalid controller tool reply" | "MCP outcome discriminator differs" => {
                Self::ControllerReply
            }
            _ => Self::Unclassified,
        }
    }
    fn label(self) -> &'static str {
        match self {
            Self::Transport => "mcp_transport",
            Self::HttpStatus => "mcp_http_status",
            Self::ResponseRead => "mcp_response_read",
            Self::ResponseLimit => "mcp_response_limit",
            Self::ResponseSyntax => "mcp_response_syntax",
            Self::ResponseCorrelation => "mcp_response_correlation",
            Self::MethodRefused => "mcp_method_refused",
            Self::ToolEnvelope => "mcp_tool_envelope",
            Self::ControllerReply => "mcp_controller_reply",
            Self::UncommittedReply => "mcp_uncommitted_reply",
            Self::ResultLimit => "mcp_complete_result_limit",
            Self::ReceiptContract => "mcp_receipt_contract",
            Self::LookupAbsent => "mcp_lookup_no_committed_receipt",
            Self::Unclassified => "mcp_unclassified_failure",
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FailureDiagnostic {
    revision: u32,
    caller: Caller,
    operation_id: String,
    logical_call_id: String,
    stage: FailureStage,
}

const JOURNAL_ADMISSION_MESSAGE: &str = "collaboration journal admission bound (execution.collaboration.journal_bytes; reserves execution.collaboration.complete_result_bytes)";

// This record is diagnostic evidence of a call that was never admitted or sent,
// not a controller receipt, a journal entry, or authority to retry another call.
// Keep it separate from diagnostics for dispatched, unresolved occurrences.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AdmissionDisposition {
    NotSent,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AdmissionRefusal {
    revision: u32,
    dispatch: AdmissionDisposition,
    caller: Caller,
    operation_id: String,
    logical_call_id: String,
    thread_id: String,
    session_id: String,
    journal_bytes: usize,
    complete_result_bytes: usize,
}

#[derive(Debug)]
enum InvokeError {
    JournalAdmission(AdmissionRefusal),
    Quiesce(String),
}
impl From<String> for InvokeError {
    fn from(reason: String) -> Self {
        Self::Quiesce(reason)
    }
}
impl From<&str> for InvokeError {
    fn from(reason: &str) -> Self {
        Self::Quiesce(reason.into())
    }
}
impl InvokeError {
    #[cfg(test)]
    fn message(&self) -> &str {
        match self {
            Self::JournalAdmission(_) => JOURNAL_ADMISSION_MESSAGE,
            Self::Quiesce(reason) => reason,
        }
    }
}

pub(super) fn diagnostic_reason(config: &Config) -> Result<Option<&'static str>, String> {
    let Some(bytes) = read_private(
        &config.directory().join("diagnostic.json"),
        config.limits.gate_bytes,
        "execution.collaboration.gate_bytes",
    )?
    else {
        return Ok(None);
    };
    let diagnostic: FailureDiagnostic = decode(&bytes, config.limits.gate_bytes)?;
    if diagnostic.revision != 1 || diagnostic.caller != config.caller {
        return Err("collaboration diagnostic binding differs".into());
    }
    let bytes = read_private(
        &config.directory().join("calls.json"),
        config.limits.journal_bytes,
        "execution.collaboration.journal_bytes",
    )?
    .ok_or("collaboration diagnostic journal absent")?;
    let journal: Journal = decode(&bytes, config.limits.journal_bytes)?;
    if !journal.entries.iter().any(|entry| {
        entry.invocation.caller == diagnostic.caller
            && entry.invocation.operation_id == diagnostic.operation_id
            && entry.invocation.logical_call_id == diagnostic.logical_call_id
            && entry.outcome.is_none()
            && !entry.delivered
    }) {
        return Err("collaboration diagnostic occurrence differs".into());
    }
    Ok(Some(diagnostic.stage.label()))
}
trait Transport {
    async fn call(&self, call: &Invocation) -> Result<CallResult, String>;
    async fn lookup(&self, call: &Invocation) -> Result<Option<CallResult>, String>;
    // An unclassified String error is never evidence of transport loss.
    async fn call_observed(&self, call: &Invocation) -> Result<CallResult, mcp::CallFailure> {
        self.call(call).await.map_err(mcp::CallFailure::Contract)
    }
    async fn lookup_observed(&self, call: &Invocation) -> Result<Option<CallResult>, mcp::CallFailure> {
        self.lookup(call).await.map_err(mcp::CallFailure::Contract)
    }
}
impl Transport for Client {
    async fn call(&self, call: &Invocation) -> Result<CallResult, String> {
        Client::call(self, call).await
    }
    async fn lookup(&self, call: &Invocation) -> Result<Option<CallResult>, String> {
        Client::get_call(self, call).await
    }
    async fn call_observed(&self, call: &Invocation) -> Result<CallResult, mcp::CallFailure> {
        Client::call_observed(self, call).await
    }
    async fn lookup_observed(&self, call: &Invocation) -> Result<Option<CallResult>, mcp::CallFailure> {
        Client::get_call_observed(self, call).await
    }
}
struct Bridge<T> {
    config: Config,
    transport: T,
    journal: Journal,
    path: PathBuf,
    _lock: std::fs::File,
    credentials: Vec<String>,
    cancelled: Arc<AtomicBool>,
}
impl<T: Transport> Bridge<T> {
    fn open(
        config: Config,
        transport: T,
        credentials: Vec<String>,
        cancelled: Arc<AtomicBool>,
    ) -> Result<Self, String> {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        config.validate()?;
        safe_value(&config, &credentials, &config.worker_limits)?;
        let dir = config.directory();
        process::private_directory(&dir)?;
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(0x20000 | 0x800)
            .open(dir.join("owner.lock"))
            .map_err(|_| "collaboration bridge lock unavailable")?;
        let meta = lock
            .metadata()
            .map_err(|_| "collaboration bridge lock unavailable")?;
        if !meta.is_file()
            || meta.mode() & 0o077 != 0
            || meta.uid()
                != std::fs::metadata("/proc/self")
                    .map_err(|_| "process owner unavailable")?
                    .uid()
        {
            return Err("collaboration bridge lock type or owner differs".into());
        }
        lock.try_lock()
            .map_err(|_| "another collaboration bridge owns this attempt")?;
        let path = dir.join("calls.json");
        let journal = match read_private(
            &path,
            config.limits.journal_bytes,
            "execution.collaboration.journal_bytes",
        )? {
            Some(bytes) => decode::<Journal>(&bytes, config.limits.journal_bytes)?,
            None => Journal {
                instruction_source: config.instruction_source.clone(),
                runtime_limits: config.runtime_limits.clone(),
                limits: config.limits.clone(),
                worker_limits: config.worker_limits.clone(),
                revision: REVISION,
                caller: config.caller.clone(),
                deadline_unix_ms: config.deadline_unix_ms,
                controller_url: config.controller_url.clone(),
                entries: vec![],
            },
        };
        if journal.runtime_limits != config.runtime_limits
            || journal.limits != config.limits
            || journal.worker_limits != config.worker_limits
        || journal.instruction_source != config.instruction_source
            || !matches!(journal.revision,REVISION|READ_REVISION)
            || journal.caller != config.caller
            || journal.deadline_unix_ms != config.deadline_unix_ms
            || journal.controller_url != config.controller_url
        {
            return Err("collaboration journal identity or revision differs".into());
        }
        let mut logical = std::collections::BTreeSet::new();
        let mut operations = std::collections::BTreeSet::new();
        for e in &journal.entries {
            e.invocation.validate(&config.runtime_limits)?;
            if e.invocation.caller != config.caller
                || !logical.insert(e.invocation.logical_call_id.clone())
                || !operations.insert(e.invocation.operation_id.clone())
                || e.delivered && e.outcome.is_none()
                || !mcp::valid_identity(
                    &e.thread_id,
                    config.runtime_limits.transport.identity_bytes,
                )
                || !mcp::valid_identity(
                    &e.session_id,
                    config.runtime_limits.transport.identity_bytes,
                )
            {
                return Err("collaboration journal binding differs".into());
            }
            if let Some(outcome) = &e.outcome {
                check_body(&config,journal.revision,e,outcome)?;
            }
        }
        for entry in &journal.entries {
            safe_value(&entry.invocation, &credentials, &config.worker_limits)?;
            safe_value(
                &json!({"thread":entry.thread_id,"session":entry.session_id}),
                &credentials,
                &config.worker_limits,
            )?;
            if let Some(outcome) = &entry.outcome {
                safe_value(outcome, &credentials, &config.worker_limits)?;
            }
        }
        Ok(Self {
            config,
            transport,
            journal,
            path,
            _lock: lock,
            credentials,
            cancelled,
        })
    }
    fn live(&self) -> Result<(), String> {
        if self.cancelled.load(Ordering::SeqCst) {
            return Err("collaboration bridge cancelled".into());
        }
        if process::now_ms() >= self.config.deadline_unix_ms {
            return Err("collaboration bridge deadline reached".into());
        }
        if read_private(
            &self.config.stop_path(),
            self.config.limits.gate_bytes,
            "execution.collaboration.gate_bytes",
        )?
        .is_some()
        {
            return Err("collaboration bridge requires attempt quiescence".into());
        }
        Ok(())
    }
    fn stop(&self, reason: &str) -> Result<(), String> {
        // Static reasons only: no provider diagnostics, arguments or credentials.
        bounded_save(
            &self.config.stop_path(),
            &Stop {
                revision: REVISION,
                caller: self.config.caller.clone(),
                reason: reason.into(),
            },
            self.config.limits.gate_bytes,
            "execution.collaboration.gate_bytes",
        )
    }
    fn save(&self) -> Result<(), String> {
        if self.journal.revision==READ_REVISION && !reference_owner(&self.config)? {return Err("read reference save requires worker20".into())}
        bounded_save(
            &self.path,
            &self.journal,
            self.config.limits.journal_bytes,
            "execution.collaboration.journal_bytes",
        )
    }
    fn compact_delivered(&mut self)->Result<(),String> {
        if !reference_owner(&self.config)? {return Ok(())}
        if let Some(boundary)=read_memory_boundary(&self.config)? {
            if !boundary_released(&self.config,&boundary)? {return Ok(())}
        }
        if super::collaboration_notice::unresolved(&self.config.state,&self.config.runtime_limits)? { return Ok(()) }
        let previous=self.journal.clone();
        let mut changed=false;
        for entry in &mut self.journal.entries {
            if !entry.delivered {continue}
            let Some(original)=entry.outcome.as_ref().and_then(CallBody::inline) else {continue};
            Self::check_result(&entry.invocation,original,self.config.limits.complete_result_bytes)?;
            let Ok(reference)=ReadReference::new(&entry.invocation,original) else {continue};
            let compact=CallBody::Reference {committed_read_reference:reference};
            if serde_json::to_vec(&compact).map_err(|_|"read reference encoding")?.len()
                >=serde_json::to_vec(original).map_err(|_|"read result encoding")?.len() {continue}
            entry.outcome=Some(compact);
            changed=true;
        }
        if changed {
            self.journal.revision=READ_REVISION;
            if let Err(error)=self.save() { self.journal=previous; return Err(error); }
        }
        Ok(())
    }
    fn remember_failure(&self, call: &Invocation, stage: FailureStage) {
        let path = self.config.directory().join("diagnostic.json");
        // Best effort only: failed diagnostic IO cannot change reconciliation.
        let _ = (|| -> Result<(), String> {
            if let Some(bytes) = read_private(
                &path,
                self.config.limits.gate_bytes,
                "execution.collaboration.gate_bytes",
            )? {
                let saved: FailureDiagnostic = decode(&bytes, self.config.limits.gate_bytes)?;
                if saved.revision == 1
                    && saved.caller == call.caller
                    && saved.operation_id == call.operation_id
                    && saved.logical_call_id == call.logical_call_id
                {
                    return Ok(());
                }
            }
            bounded_save(
                &path,
                &FailureDiagnostic {
                    revision: 1,
                    caller: call.caller.clone(),
                    operation_id: call.operation_id.clone(),
                    logical_call_id: call.logical_call_id.clone(),
                    stage,
                },
                self.config.limits.gate_bytes,
                "execution.collaboration.gate_bytes",
            )
        })();
    }
    fn rejected_result_stage(&self, call: &Invocation, result: &CallResult) -> FailureStage {
        let Ok(checked) = mcp::result(call, result.reply.clone()) else {
            return FailureStage::ReceiptContract;
        };
        if checked.requires_quiescence != result.requires_quiescence {
            return FailureStage::ReceiptContract;
        }
        if result.reply.commit.is_none() {
            FailureStage::UncommittedReply
        } else if !matches!(
            result.reply.outcome,
            Outcome::Ok { .. } | Outcome::Refused { .. }
        ) {
            FailureStage::ReceiptContract
        } else if bounded_json_size(result, self.config.limits.complete_result_bytes).is_err() {
            FailureStage::ResultLimit
        } else {
            FailureStage::ReceiptContract
        }
    }
    fn check_result(
        call: &Invocation,
        result: &CallResult,
        result_bytes: usize,
    ) -> Result<(), String> {
        let checked = mcp::result(call, result.reply.clone())?;
        if checked.requires_quiescence != result.requires_quiescence
            || result.reply.commit.is_none()
            || !matches!(
                result.reply.outcome,
                Outcome::Ok { .. } | Outcome::Refused { .. }
            )
        {
            return Err("collaboration outcome is not a bounded committed receipt".into());
        }
        bounded_json_size(result, result_bytes).map_err(|_| {
            "collaboration outcome is not a bounded committed receipt (execution.collaboration.complete_result_bytes)".to_owned()
        })?;
        Ok(())
    }
    async fn until<F, R>(&self, f: F) -> Result<R, String>
    where
        F: std::future::Future<Output = Result<R, String>>,
    {
        self.live()?;
        tokio::pin!(f);
        loop {
            tokio::select! {r=&mut f=>return r,_=tokio::time::sleep(Duration::from_millis(self.config.limits.liveness_interval_ms))=>self.live()?}
        }
    }
    async fn until_observed<F, R>(&self, f: F) -> Result<R, mcp::CallFailure>
    where F: std::future::Future<Output = Result<R, mcp::CallFailure>> {
        self.until(async { Ok(f.await) }).await.map_err(mcp::CallFailure::Contract)?
    }
    fn association(&self, thread: &str) -> Result<(), String> {
        let bytes = read_private(
            &self.config.state.join("borg-thread.json"),
            self.config.limits.frame_bytes,
            "execution.collaboration.frame_bytes",
        )?
        .ok_or("host executor association not retained")?;
        let value = mcp::strict::parse(&bytes, self.config.limits.frame_bytes)?;
        if value.get("thread_id").and_then(Value::as_str) != Some(thread)
            || value.get("turn_start_sent") == Some(&json!(false))
        {
            return Err("MCP thread differs from retained host association".into());
        }
        Ok(())
    }
    // Traceability scope: The coding bridge saves the complete Invocation before transport dispatch; an existing occurrence cannot change tool or arguments.
    //= spec/impl/v2/implementation.md#native-tool-profiles-and-continuations
    //# An execution attempt MUST retain a tool call's exact argument binding before sending a mutation.
    async fn invoke(
        &mut self,
        name: &str,
        args: Value,
        meta: &Value,
    ) -> Result<(Value, usize), InvokeError> {
        self.live()?;
        self.compact_delivered()?;
        let logical = meta
            .get("callId")
            .and_then(Value::as_str)
            .filter(|x| mcp::valid_identity(x, self.config.runtime_limits.transport.identity_bytes))
            .ok_or("MCP logical occurrence absent")?;
        let thread = meta
            .get("threadId")
            .and_then(Value::as_str)
            .filter(|x| mcp::valid_identity(x, self.config.runtime_limits.transport.identity_bytes))
            .ok_or("MCP thread absent")?;
        let session = meta
            .get("sessionId")
            .and_then(Value::as_str)
            .filter(|x| mcp::valid_identity(x, self.config.runtime_limits.transport.identity_bytes))
            .ok_or("MCP session absent")?;
        self.association(thread)?;
        if !self.config.catalogue()
            .iter()
            .any(|t| t.name == name)
            || name == "collab_task_delegate"
        {
            return Err("collaboration tool not applicable to coding task".into());
        }
        safe_value(
            &json!({"logical":logical,"thread":thread,"session":session,"tool":name,"arguments":args}),
            &self.credentials,
            &self.config.worker_limits,
        )?;
        let existing = self
            .journal
            .entries
            .iter()
            .position(|e| e.invocation.logical_call_id == logical);
        let index = if let Some(index) = existing {
            let e = &self.journal.entries[index];
            if e.thread_id != thread
                || e.session_id != session
                || e.invocation.tool != name
                || e.invocation.arguments != args
            {
                return Err("logical collaboration occurrence changed its exact binding".into());
            }
            index
        } else {
            if self.journal.entries.iter().any(|e| !e.delivered) {
                return Err("prior collaboration delivery is unresolved".into());
            }
            let invocation = Invocation {
                operation_id: super::mutations::fresh_id("codex-collab")?,
                logical_call_id: logical.into(),
                caller: self.config.caller.clone(),
                tool: name.into(),
                arguments: args,
            };
            invocation.validate(&self.config.runtime_limits)?;
            let entry = Entry {
                thread_id: thread.into(),
                session_id: session.into(),
                invocation,
                outcome: None,
                delivered: false,
            };
            self.journal.entries.push(entry);
            // The full escaped journal plus complete maximum CallResult must fit
            // before any dispatch. Result is serialized as a value, not a string.
            if bounded_json_size(
                &self.journal,
                self.config
                    .limits
                    .journal_bytes
                    .saturating_sub(self.config.limits.complete_result_bytes),
            )
            .is_err()
            {
                let unsent = self.journal.entries.pop()
                    .ok_or("fresh collaboration intent missing")?;
                return Err(InvokeError::JournalAdmission(AdmissionRefusal {
                    revision: 1,
                    dispatch: AdmissionDisposition::NotSent,
                    caller: unsent.invocation.caller,
                    operation_id: unsent.invocation.operation_id,
                    logical_call_id: unsent.invocation.logical_call_id,
                    thread_id: unsent.thread_id,
                    session_id: unsent.session_id,
                    journal_bytes: self.config.limits.journal_bytes,
                    complete_result_bytes: self.config.limits.complete_result_bytes,
                }));
            }
            self.save()?;
            self.journal.entries.len() - 1
        };
        let call = self.journal.entries[index].invocation.clone();
        if let Some(reference)=self.journal.entries[index].outcome.as_ref().and_then(CallBody::reference).cloned() {
            // An absent reference is never the legacy not-found retry path.
            self.save()?;
            let original=self.until_observed(self.transport.lookup_observed(&call)).await
                .map_err(|e|InvokeError::from(e.into_message()))?.ok_or(crate::read_retention::UNAVAILABLE)?;
            let recovered=reference.restore(&call,original)?;
            Self::check_result(&call,&recovered,self.config.limits.complete_result_bytes)?;
            safe_value(&recovered,&self.credentials,&self.config.worker_limits)?;
            self.journal.entries[index].outcome=Some(recovered.into());
            self.save()?;
        }
        if self.journal.entries[index].outcome.is_none() {
            // Mutations stay lookup-only after their first send. Closed read
            // adapters may repeat only this exact occurrence after absence.
            let first = existing.is_none();
            let mut read_retry = mcp::retryable_read_tool(&call.tool);
            let mut retry_ready = false;
            let mut observed = if first {
                match self.until_observed(self.transport.call_observed(&call)).await {
                    Ok(result) => Some(result),
                    Err(error) => {
                        self.remember_failure(&call, FailureStage::from_error(error.message()));
                        if matches!(&error, mcp::CallFailure::Contract(_)) {
                            return Err(error.into_message().into());
                        }
                        None
                    }
                }
            } else {
                None
            };
            loop {
                if let Some(result) = observed.take() {
                    if Self::check_result(&call, &result, self.config.limits.complete_result_bytes)
                        .is_ok()
                    {
                        safe_value(&result, &self.credentials, &self.config.worker_limits)?;
                        self.journal.entries[index].outcome = Some(result.into());
                        self.save()?;
                        break;
                    }
                    self.remember_failure(&call, self.rejected_result_stage(&call, &result));
                    // A malformed/uncommitted CallResult is not transport retry
                    // authority. Continue original lookup without redispatch.
                    read_retry = false;
                }
                self.live()?;
                let mut redispatched = false;
                observed = match self.until_observed(self.transport.lookup_observed(&call)).await {
                    Ok(Some(result)) => Some(result),
                    Ok(None) => {
                        self.remember_failure(&call, FailureStage::LookupAbsent);
                        if read_retry && retry_ready {
                            // The lookup above happened after the configured
                            // wait. Preserve exact durable intent before send.
                            self.save()?;
                            self.live()?;
                            retry_ready = false;
                            redispatched = true;
                            match self.until_observed(self.transport.call_observed(&call)).await {
                                Ok(result) => Some(result),
                                Err(error) => {
                                    self.remember_failure(&call, FailureStage::from_error(error.message()));
                                    if matches!(&error, mcp::CallFailure::Contract(_)) {
                                        return Err(error.into_message().into());
                                    }
                                    None
                                },
                            }
                        } else {
                            None
                        }
                    }
                    Err(error) => {
                        self.remember_failure(&call, FailureStage::from_error(error.message()));
                        if matches!(&error, mcp::CallFailure::Contract(_)) {
                            return Err(error.into_message().into());
                        }
                        None
                    }
                };
                if redispatched && observed.is_none() {
                    // Resolve a lost reply immediately; only a later fresh
                    // absent lookup after backoff can authorize another send.
                    continue;
                }
                if observed.as_ref().is_none_or(|result| {
                    Self::check_result(&call, result, self.config.limits.complete_result_bytes)
                        .is_err()
                }) {
                    self.until(async {
                        tokio::time::sleep(Duration::from_millis(
                            self.config.limits.lookup_backoff_ms,
                        ))
                        .await;
                        Ok(())
                    })
                    .await?;
                    retry_ready = true;
                }
            }
        }
        let result = self.journal.entries[index]
            .outcome
            .as_ref()
            .and_then(CallBody::inline)
            .ok_or("missing recorded collaboration result")?;
        Self::check_result(&call, result, self.config.limits.complete_result_bytes)?;
        safe_value(result, &self.credentials, &self.config.worker_limits)?;
        // Recovered or in-memory outcomes can follow a failed directory sync.
        // Retry only the same bounded journal write, never the tool dispatch.
        self.save()?;
        self.live()?;
        let text =
            serde_json::to_string(result).map_err(|_| "collaboration result encoding failed")?;
        let value = json!({"content":[{"type":"text","text":text}],"isError":!matches!(result.reply.outcome,Outcome::Ok{..})});
        bounded_json_size(&value, self.config.limits.frame_bytes).map_err(
            |_| "collaboration wire result exceeds bound (execution.collaboration.frame_bytes)",
        )?;
        Ok((value, index))
    }

    // Only the typed pre-save/pre-dispatch branch may return an ordinary MCP
    // tool error. Text from a transport, failed save or recovered occurrence
    // cannot acquire known-unsent status by resembling this message.
    //= spec/impl/v2/implementation.md#limits-are-versioned-metadata
    //# An operation refused by a configured limit MUST identify the limiting metadata field.
    async fn invoke_for_stdio(
        &mut self,
        name: &str,
        args: Value,
        meta: &Value,
    ) -> Result<(Value, Option<usize>), String> {
        match self.invoke(name, args, meta).await {
            Ok((body, index)) => {
                self.hold_memory_boundary(index).await?;
                Ok((body, Some(index)))
            }
            Err(InvokeError::JournalAdmission(refusal)) => {
                // Diagnostic IO is best effort, as for dispatched failures. It
                // cannot change the proven lack of dispatch or hide a pending
                // outcome: prior-undelivered checks run before this branch.
                let _ = bounded_save(
                    &self.config.directory().join("admission.json"),
                    &refusal,
                    self.config.limits.gate_bytes,
                    "execution.collaboration.gate_bytes",
                );
                self.live()?;
                let text = serde_json::to_string(&json!({
                    "code": "collaboration_journal_admission",
                    "dispatch": "not_sent",
                    "message": JOURNAL_ADMISSION_MESSAGE,
                    "journal_bytes": refusal.journal_bytes,
                    "complete_result_bytes": refusal.complete_result_bytes,
                })).map_err(|_| "collaboration admission refusal encoding failed")?;
                let body = json!({"content":[{"type":"text","text":text}],"isError":true});
                bounded_json_size(&body, self.config.limits.frame_bytes)
                    .map_err(|_| "collaboration admission refusal exceeds execution.collaboration.frame_bytes")?;
                Ok((body, None))
            }
            Err(InvokeError::Quiesce(reason)) => Err(reason),
        }
    }
    // Workflow delivery uses this retained-result boundary; the legacy on-disk
    // names are preserved, but memory-only profiles never activate it.
    async fn hold_memory_boundary(&self, index: usize) -> Result<(), String> {
        if !memory_gate_enabled(&self.config)? {
            return Ok(());
        }
        self.live()?;
        let entry = self
            .journal
            .entries
            .get(index)
            .ok_or("memory boundary occurrence absent")?;
        if entry.outcome.is_none() {
            return Err("memory boundary has no retained tool result".into());
        }
        let gate = MemoryBoundary {
            revision: 1,
            caller: self.config.caller.clone(),
            deadline_unix_ms: self.config.deadline_unix_ms,
            index,
            thread_id: entry.thread_id.clone(),
            logical_call_id: entry.invocation.logical_call_id.clone(),
        };
        safe_value(&gate, &self.credentials, &self.config.worker_limits)?;
        bounded_save(
            &self.config.directory().join("memory-boundary.json"),
            &gate,
            self.config.limits.boundary_bytes,
            "execution.collaboration.boundary_bytes",
        )?;
        loop {
            self.live()?;
            if boundary_released(&self.config, &gate)? {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(
                self.config.limits.liveness_interval_ms,
            ))
            .await;
        }
    }
    fn delivered(&mut self, index: usize) -> Result<(), String> {
        if self.journal.entries[index].outcome.is_none() {
            return Err("cannot acknowledge unresolved collaboration result".into());
        }
        self.journal.entries[index].delivered = true;
        self.save()
    }
}

/// Read only after the supervisor has stopped the complete supplied-executor
/// tree. Pipe delivery and model consumption are deliberately not inferred.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeDisposition {
    NoCalls,
    Committed,
    Unresolved,
}
/// A host can inspect delivered originals only while no bridge invocation or
/// pipe delivery owns the boundary. The guard remains held through turn/start.
pub(super) fn continuation_boundary(config:&Config)->Result<std::fs::File,String> {
    check_stop(config)?;
    let lock=outer_boundary_file(config)?;
    lock.try_lock().map_err(|_|"supplied continuation awaits in-flight MCP call or delivery")?;
    if let Some(bytes)=read_private(&config.directory().join("calls.json"),config.limits.journal_bytes,"execution.collaboration.journal_bytes")? {
        let journal:Journal=decode(&bytes,config.limits.journal_bytes)?;
        if journal.entries.iter().any(|entry|!entry.delivered||entry.outcome.is_none()) {
            return Err("supplied continuation awaits original MCP outcome and delivery".into());
        }
    }
    if matches!(inspect_retained_outcomes(config,None,false,None,false)?,OutcomeDisposition::Unresolved) {
        return Err("supplied continuation has unresolved MCP outcome".into());
    }
    Ok(lock)
}

// Only the still-owning adapter calls this after its ManagedProcess proves the
// complete former tree stopped. The static bridge marker is not that proof.
// Exact original calls/deliveries remain intact; no operation is redispatched.
pub(super) fn reopen_after_known_stop(config:&Config)->Result<(),String> {
    let lock=outer_boundary_file(config)?;
    lock.try_lock().map_err(|_|"old collaboration call remains active")?;
    inspect_retained_outcomes(config,None,false,None,false)?;
    if super::collaboration_notice::unresolved(&config.state,&config.runtime_limits)? {
        return Err("supplied resume has an unresolved workflow append".into());
    }
    if let Some(bytes)=read_private(&config.directory().join("calls.json"),config.limits.journal_bytes,"execution.collaboration.journal_bytes")? {
        let journal:Journal=decode(&bytes,config.limits.journal_bytes)?;
        if journal.entries.iter().any(|entry|entry.outcome.is_none()||!entry.delivered) {
            return Err("supplied resume has unresolved original tool delivery".into());
        }
    }
    if let Some(bytes)=read_private(&config.stop_path(),config.limits.gate_bytes,"execution.collaboration.gate_bytes")? {
        let stopped:Stop=decode(&bytes,config.limits.gate_bytes)?;
        if stopped.revision!=REVISION||stopped.caller!=config.caller {return Err("supplied stopped marker identity differs".into());}
        std::fs::remove_file(config.stop_path()).map_err(|_|"settled bridge stop marker unavailable")?;
        std::fs::File::open(config.directory()).and_then(|f|f.sync_all()).map_err(|_|"settled bridge stop marker durability unavailable")?;
    }
    Ok(())
}
fn outer_boundary_file(config:&Config)->Result<std::fs::File,String> {
    use std::os::unix::fs::{MetadataExt,OpenOptionsExt};
    process::private_directory(&config.directory())?;
    let file=std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(false)
        .mode(0o600).custom_flags(0x20000|0x800).open(config.directory().join("outer-boundary.lock"))
        .map_err(|_|"supplied outer boundary unavailable")?;
    let m=file.metadata().map_err(|_|"supplied outer boundary metadata unavailable")?;
    if !m.is_file()||m.mode()&0o077!=0||m.uid()!=std::fs::metadata("/proc/self").map_err(|_|"owner unavailable")?.uid(){return Err("supplied outer boundary owner/type differs".into());}
    Ok(file)
}

pub(super) fn terminal_reads(config:&Config, after:Option<&str>, bound:usize)->Result<Vec<crate::read_retention::ReleaseItem>,String> {
    if !reference_owner(config)? {return Err("terminal read release requires worker20".into())}
    if inspect_retained_outcomes(config,None,false,None,false)?==OutcomeDisposition::Unresolved {return Err("Coding outcomes unresolved".into())}
    if super::collaboration_notice::unresolved(&config.state,&config.runtime_limits)? {return Err("Coding delivery remains unresolved".into())}
    let journal_path=config.directory().join("calls.json");
    if read_private(&journal_path,config.limits.journal_bytes,"execution.collaboration.journal_bytes")?.is_none() {
        // A missing journal is no work only if no retained boundary still
        // requires it. Do not manufacture a new private home/lock for absence.
        if read_memory_boundary(config)?.is_some() {return Err("memory boundary journal absent".into());}
        return Ok(vec![]);
    }
    let lock=outer_boundary_file(config)?;
    lock.try_lock().map_err(|_|"terminal read has an active bridge owner")?;
    if let Some(boundary)=read_memory_boundary(config)? {if !boundary_released(config,&boundary)? {return Err("memory boundary still needs original body".into())}}
    let bytes=read_private(&journal_path,config.limits.journal_bytes,"execution.collaboration.journal_bytes")?
        .ok_or("terminal read journal disappeared before owner lock")?;
    let journal:Journal=decode(&bytes,config.limits.journal_bytes)?;
    let mut selected=Vec::new();
    for entry in &journal.entries {
        if !entry.delivered || entry.outcome.is_none() {return Err("Coding read still has a delivery consumer".into())}
        if after.is_some_and(|id|entry.invocation.operation_id.as_str()<=id) {continue}
        let reference=match entry.outcome.as_ref().unwrap() {
            CallBody::Inline(value)=>match ReadReference::new(&entry.invocation,value) {Ok(v)=>v,Err(_)=>continue},
            CallBody::Reference{committed_read_reference:value}=>value.clone(),
        };
        reference.validate(&entry.invocation)?;
        selected.push(crate::read_retention::ReleaseItem{operation_id:entry.invocation.operation_id.clone(),commit:reference.commit,evidence:reference.evidence});
    }
    selected.sort_by(|a,b|a.operation_id.cmp(&b.operation_id));selected.truncate(bound);Ok(selected)
}

pub fn outcome_disposition(config: &Config) -> Result<OutcomeDisposition, String> {
    inspect_retained_outcomes(config, None, false, None, false)
}

/// Historical outer markers must not silently adopt newly typed Task fields.
/// Uses the same private bounded reader, identity joins and result validation as
/// recovery; this function neither rewrites a record nor resolves an outcome.
pub(super) fn admit_worker18_outcomes(config: &Config) -> Result<(), String> {
    inspect_retained_outcomes(config, Some(super::outcome_requires_worker18), false, None, false).map(|_| ())
}

pub(super) fn admit_retry_outcomes(config:&Config)->Result<(),String> {
    inspect_retained_outcomes(config,Some(crate::supplied_outer::outcome_uses),false,None,false).map(|_|())
}
pub(super) fn admit_retry_outcomes_from_retained_config(mut config:Config)->Result<(),String> {
    let Some(bytes)=read_private(&config.state.join("borg-collaboration-config.json"),config.limits.config_bytes,"execution.collaboration.config_bytes")? else {
        if config.directory().join("calls.json").exists() {return Err("Coding journal lacks its original config".into());}
        return Ok(());
    };
    let retained:Config=decode(&bytes,config.limits.config_bytes)?;
    config.controller_url=retained.controller_url;
    admit_retry_outcomes(&config)
}
pub(super) fn admit_worker19_outcomes(config:&Config)->Result<(),String> {
    if crate::protocol::policy_requires_wire24(&config.runtime_limits)||config.limits.capabilities.is_some() {return Err("Coding accepted identity/task metadata requires worker19".into())}
    inspect_retained_outcomes(config,None,true,None,false).map(|_|())
}
pub(super) fn admit_worker21_outcomes(config: &Config) -> Result<(), String> {
    if mcp::publication::selection(config.execution_profile.as_ref()).is_some() { return Err("Coding publication capability requires worker21".into()); }
    inspect_retained_outcomes(config, Some(crate::protocol::outcome_requires_publication), false, None, false)?;
    if let Some(bytes) = read_private(&config.directory().join("calls.json"), config.limits.journal_bytes, "execution.collaboration.journal_bytes")? {
        let journal: Journal = decode(&bytes, config.limits.journal_bytes)?;
        if journal.entries.iter().any(|e| crate::protocol::requires_publication(&Operation::CallCollaboration { call: e.invocation.clone() })) {
            return Err("original Coding publication call requires worker21, including refusals".into());
        }
    }
    Ok(())
}
pub(super) fn admit_identity_limit_fields_outcomes(config: &Config) -> Result<(), String> {
    use crate::protocol::identity_limit_fields as vocabulary;
    if vocabulary::metadata_uses(&config.runtime_limits) {
        return Err("Coding accepted identity limit field policy requires worker20".into());
    }
    if let Some(bytes) = read_private(&config.state.join("borg-collaboration-config.json"), config.limits.config_bytes, "execution.collaboration.config_bytes")? {
        let retained: Config = decode(&bytes, config.limits.config_bytes)?;
        if vocabulary::metadata_uses(&retained.runtime_limits) {
            return Err("Coding retained identity limit field policy requires worker20".into());
        }
    }
    inspect_retained_outcomes(config, Some(vocabulary::outcome_uses), false, Some(vocabulary::invocation_uses), false).map(|_| ())
}
pub(super) fn admit_identity_limit_fields_from_retained_config(mut config: Config) -> Result<(), String> {
    if let Some(bytes) = read_private(&config.state.join("borg-collaboration-config.json"), config.limits.config_bytes, "execution.collaboration.config_bytes")? {
        let retained: Config = decode(&bytes, config.limits.config_bytes)?;
        config.controller_url = retained.controller_url;
    } else {
        // The accepted command already fences new policy tags. A historical
        // Coding attempt need not have started its MCP bridge at all. No saved
        // endpoint exists in that case, so do not validate the placeholder URL.
        if crate::protocol::identity_limit_fields::metadata_uses(&config.runtime_limits) {
            return Err("Coding accepted identity limit field policy requires worker20".into());
        }
        match std::fs::symlink_metadata(config.directory().join("calls.json")) {
            Ok(_) => return Err("Coding journal lacks its original config".into()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) => return Err("Coding journal inventory unavailable".into()),
        }
    }
    admit_identity_limit_fields_outcomes(&config)
}
pub(super) fn admit_read_pagination_from_retained_config(mut config:Config)->Result<(),String> {
    let Some(bytes)=read_private(&config.state.join("borg-collaboration-config.json"),config.limits.config_bytes,"execution.collaboration.config_bytes")? else {
        return match std::fs::symlink_metadata(config.directory().join("calls.json")) {
            Err(error) if error.kind()==std::io::ErrorKind::NotFound => Ok(()),
            _ => Err("Coding journal lacks its original config".into()),
        };
    };
    let retained:Config=decode(&bytes,config.limits.config_bytes)?;
    config.controller_url=retained.controller_url;
    admit_read_pagination(&config)
}
pub(super) fn admit_read_pagination(config:&Config)->Result<(),String> {
    inspect_retained_outcomes(config,None,false,None,true).map(|_|())
}
fn inspect_retained_outcomes(config: &Config, historical_rejects: Option<fn(&Outcome)->bool>, historical_worker19:bool, historical_requests: Option<fn(&mcp::Invocation)->bool>, historical_pagination: bool) -> Result<OutcomeDisposition, String> {
    config.validate()?;
    // The host reconstructs this scope from the original RunAttempt. A retained
    // bridge configuration cannot silently substitute or drop that source.
    if let Some(bytes) = read_private(&config.state.join("borg-collaboration-config.json"),
        config.limits.config_bytes, "execution.collaboration.config_bytes")? {
        let retained: Config = decode(&bytes, config.limits.config_bytes)?;
        if retained.instruction_source != config.instruction_source || retained.execution_profile != config.execution_profile {
            return Err("retained instruction source differs from accepted command".into());
        }
    }
    let Some(bytes) = read_private(
        &config.directory().join("calls.json"),
        config.limits.journal_bytes,
        "execution.collaboration.journal_bytes",
    )?
    else {
        return Ok(OutcomeDisposition::NoCalls);
    };
    let journal: Journal = decode(&bytes, config.limits.journal_bytes)?;
    if journal.runtime_limits != config.runtime_limits
        || journal.limits != config.limits
        || journal.worker_limits != config.worker_limits
        || journal.instruction_source != config.instruction_source
        || !matches!(journal.revision,REVISION|READ_REVISION)
        || journal.caller != config.caller
        || journal.deadline_unix_ms != config.deadline_unix_ms
        || journal.controller_url != config.controller_url
    {
        return Err("collaboration journal identity or revision differs".into());
    }
    let mut logical = std::collections::BTreeSet::new();
    let mut operations = std::collections::BTreeSet::new();
    let mut unresolved = false;
    for entry in &journal.entries {
        if (historical_worker19 || historical_rejects.is_some() || historical_requests.is_some() || historical_pagination)
            && entry.outcome.as_ref().and_then(CallBody::reference).is_some() {
            return Err("retained read reference requires worker20; historical predicates cannot admit a removed body".into());
        }
        if historical_requests.is_some_and(|rejects| rejects(&entry.invocation)) {
            return Err("original Coding identity limit field request requires worker20, including refusals".into());
        }
        if historical_pagination && (crate::protocol::call_requires_read_pagination(&entry.invocation) || entry.outcome.as_ref().and_then(CallBody::inline).is_some_and(|o|crate::protocol::outcome_requires_read_pagination(&o.reply.outcome))) {return Err("retained Coding pagination requires worker20; original bytes retained".into());}
        if historical_worker19 && (mcp::identity_administration::handles(&entry.invocation.tool) || entry.outcome.as_ref().and_then(CallBody::inline).is_some_and(|o|crate::protocol::outcome_requires_wire24(&o.reply.outcome))) { return Err("original Coding identity/task call requires worker19, including refusals".into()); }
        entry.invocation.validate(&config.runtime_limits)?;
        if historical_worker19 && super::machine_resources::requires(
            &Operation::CallCollaboration { call: entry.invocation.clone() },
            entry.outcome.as_ref().and_then(CallBody::inline).map(|o| &o.reply.outcome)) {
            return Err("machine resource vocabulary in retained Coding journal requires worker19".into());
        }
        if entry.invocation.caller != config.caller
            || !logical.insert(&entry.invocation.logical_call_id)
            || !operations.insert(&entry.invocation.operation_id)
            || !mcp::valid_identity(
                &entry.thread_id,
                config.runtime_limits.transport.identity_bytes,
            )
            || !mcp::valid_identity(
                &entry.session_id,
                config.runtime_limits.transport.identity_bytes,
            )
            || (entry.delivered && entry.outcome.is_none())
        {
            return Err("collaboration journal binding differs".into());
        }
        match &entry.outcome {
            Some(outcome) => {
                check_body(config,journal.revision,entry,outcome)?;
                if historical_rejects.is_some_and(|rejects| outcome.inline().is_none_or(|o|rejects(&o.reply.outcome))) {
                    return Err("retained Coding outcome requires its original worker marker".into());
                }
            }
            None => unresolved = true,
        }
    }
    Ok(if unresolved {
        OutcomeDisposition::Unresolved
    } else if journal.entries.is_empty() {
        OutcomeDisposition::NoCalls
    } else {
        OutcomeDisposition::Committed
    })
}

/// Exact task-owned server configuration. Only the fixed collaboration write
/// catalogue gets the already-authorized per-tool approval; other policies stay.
pub fn server_config(
    binary: &Path,
    config_path: &Path,
    runtime_limits: &RuntimeLimits,
    limits: &CollaborationLimits,
    timeout_seconds: u64,
) -> Value {
    server_config_for_profile(binary,config_path,runtime_limits,limits,None,timeout_seconds)
}

pub fn server_config_for_profile(
    binary: &Path, config_path: &Path, runtime_limits: &RuntimeLimits,
    limits: &CollaborationLimits, profile: Option<&ExecutionProfile>, timeout_seconds: u64,
) -> Value {
    let mut selected_policy=runtime_limits.clone();
    selected_policy.execution.collaboration.capabilities=limits.capabilities.clone();
    let runtime_limits=&selected_policy;
    let selected=match profile {
        Some(profile)=>mcp::catalogue_for_profile(runtime_limits,Some(profile)),
        None=>mcp::catalogue(runtime_limits),
    };
    let enabled = selected.iter().cloned()
        .filter(|t| t.name != "collab_task_delegate")
        .map(|t| t.name)
        .collect::<Vec<_>>();
    let mut tools = serde_json::Map::new();
    for name in [
        "collab_memory_set",
        "collab_task_progress",
        "collab_task_assign",
        "collab_task_cancel",
        "collab_task_retry",
    ] {
        tools.insert(name.into(), json!({"approval_mode":"approve"}));
    }
    if matches!(profile,Some(ExecutionProfile::CollaborationV2{..})) {
        for tool in &selected {
            if tool.name!="collab_task_delegate" && tool.annotations.get("readOnlyHint")==Some(&json!(false)) {
                tools.insert(tool.name.clone(),json!({"approval_mode":"approve"}));
            }
        }
    }
    json!({"command":binary,"args":["__collaboration_mcp","--config",config_path,"--config-bytes",limits.config_bytes.to_string()],"env_vars":["OPENAI_API_KEY"],"required":true,"startup_timeout_sec":(limits.startup_timeout_ms as f64 / 1000.0),"tool_timeout_sec":timeout_seconds,"enabled_tools":enabled,"tools":tools})
}

/// Parent reads only this small static marker while supervising the entire
/// Codex process tree. A marker is never proof that its processes stopped.
pub fn check_stop(config: &Config) -> Result<(), String> {
    if let Some(bytes) = read_private(
        &config.stop_path(),
        config.limits.gate_bytes,
        "execution.collaboration.gate_bytes",
    )? {
        let stopped: Stop = decode(&bytes, config.limits.gate_bytes)?;
        if stopped.revision != REVISION || stopped.caller != config.caller {
            return Err("collaboration stop binding differs".into());
        }
        return Err("collaboration bridge requires executor quiescence".into());
    }
    Ok(())
}
pub async fn wait_stop(config: &Config) {
    loop {
        if check_stop(config).is_err() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(config.limits.liveness_interval_ms)).await;
    }
}

/// Internal fixed-mode entry point only; not a public controller operation.
pub fn run_stdio_config(path: &Path, config_bytes: usize) -> Result<(), String> {
    let bytes = read_private(path, config_bytes, "execution.collaboration.config_bytes")?
        .ok_or("collaboration bridge config absent")?;
    let config: Config = decode(&bytes, config_bytes)?;
    if config.limits.config_bytes != config_bytes {
        return Err("bridge bootstrap bound differs (execution.collaboration.config_bytes)".into());
    }
    config.validate()?;
    let credentials = super::configured_credentials(&config.runtime_limits)?;
    let cancelled = Arc::new(AtomicBool::new(false));
    let client = config.client()?;
    let frame_bytes = config.limits.frame_bytes;
    let channel_items = config.limits.channel_items;
    let mut bridge = Bridge::open(config, client, credentials, cancelled.clone())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| "collaboration bridge runtime unavailable")?;
    let (tx, rx) = std::sync::mpsc::sync_channel(channel_items);
    std::thread::spawn(move || {
        let mut input = std::io::stdin().lock();
        loop {
            let mut frame = Vec::new();
            let result = input
                .by_ref()
                .take(frame_bytes as u64 + 1)
                .read_until(b'\n', &mut frame);
            let item = match result {
                Ok(0) => {
                    cancelled.store(true, Ordering::SeqCst);
                    return;
                }
                Ok(_) if frame.len() > frame_bytes => {
                    Err("invalid bounded MCP stdin frame (execution.collaboration.frame_bytes)")
                }
                Ok(_) if frame.last() == Some(&b'\n') => Ok(frame),
                _ => Err("invalid bounded MCP stdin frame"),
            };
            if item.is_err()
                || item
                    .as_ref()
                    .ok()
                    .and_then(|b| mcp::strict::parse(b, frame_bytes).ok())
                    .is_some_and(|v| {
                        v.get("method").and_then(Value::as_str) == Some("notifications/cancelled")
                    })
            {
                cancelled.store(true, Ordering::SeqCst);
            }
            if tx.send(item).is_err() {
                return;
            }
        }
    });
    let (out_tx, out_rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(channel_items);
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(channel_items);
    std::thread::spawn(move || {
        let mut output = std::io::stdout().lock();
        while let Ok(bytes) = out_rx.recv() {
            let ok = output
                .write_all(&bytes)
                .and_then(|_| output.flush())
                .is_ok();
            if ack_tx.send(ok).is_err() || !ok {
                return;
            }
        }
    });
    let result = (|| -> Result<(), String> {
        loop {
            bridge.live()?;
            let remaining = Duration::from_millis(
                bridge
                    .config
                    .deadline_unix_ms
                    .saturating_sub(process::now_ms()),
            );
            let frame = rx
                .recv_timeout(remaining)
                .map_err(|_| "collaboration input ended or deadline reached")?
                .map_err(str::to_owned)?;
            let value = mcp::strict::parse(&frame, frame_bytes)?;
            let obj = value.as_object().ok_or("invalid MCP request")?;
            if obj.get("jsonrpc") != Some(&json!("2.0"))
                || obj
                    .keys()
                    .any(|k| !["jsonrpc", "id", "method", "params"].contains(&k.as_str()))
            {
                return Err("invalid MCP request".into());
            }
            let method = obj
                .get("method")
                .and_then(Value::as_str)
                .ok_or("MCP method absent")?;
            let params = obj.get("params").cloned().unwrap_or(Value::Null);
            if method == "notifications/initialized" && !obj.contains_key("id") {
                continue;
            }
            if method == "notifications/cancelled" {
                return Err("collaboration request cancelled".into());
            }
            let id = obj
                .get("id")
                .filter(|id| {
                    id.is_i64()
                        || id.as_str().is_some_and(|value| {
                            mcp::valid_identity(
                                value,
                                bridge.config.runtime_limits.transport.identity_bytes,
                            )
                        })
                })
                .cloned()
                .ok_or("invalid MCP transport correlation")?;
            // The complete call/result-delivery window excludes host outer continuation.
            let _outer_boundary = if method == "tools/call" {
                let lock=outer_boundary_file(&bridge.config)?;
                lock.lock().map_err(|_|"supplied outer boundary lock unavailable")?;
                Some(lock)
            } else {None};
            let (body, delivered) = match method {
                "initialize" => {
                    if params.get("protocolVersion").and_then(Value::as_str) != Some(mcp::VERSION) {
                        return Err("unsupported MCP version".into());
                    }
                    (
                        Some(json!({"protocolVersion":mcp::VERSION,"capabilities":{"tools":{"listChanged":false}},"serverInfo":{"name":"borg-collaboration-bridge","version":"1"}})),
                        None,
                    )
                }
                "tools/list" => {
                    let tools = runtime
                        .block_on(bridge.until(bridge.transport.tools()))?
                        .into_iter()
                        .filter(|t| t.name != "collab_task_delegate"
                            && bridge.config.catalogue().iter().any(|selected|selected.name==t.name))
                        .collect::<Vec<_>>();
                    (Some(json!({"tools":tools})), None)
                }
                "tools/call" => {
                    #[derive(Deserialize)]
                    #[serde(deny_unknown_fields)]
                    struct Call {
                        name: String,
                        arguments: Value,
                        #[serde(rename = "_meta")]
                        meta: Value,
                    }
                    let call: Call = serde_json::from_value(params)
                        .map_err(|_| "invalid MCP tool parameters")?;
                    let (body, delivered) = runtime.block_on(
                        bridge.invoke_for_stdio(&call.name, call.arguments, &call.meta),
                    )?;
                    (Some(body), delivered)
                }
                "ping" => (Some(json!({})), None),
                // A valid request for an unadvertised method is a correlated
                // protocol error, not an unresolved tool occurrence. Keep the
                // same bounded flush/ACK path without creating a journal entry.
                _ => (None, None),
            };
            let reply = match body {
                Some(body) => json!({"jsonrpc":"2.0","id":id,"result":body}),
                None => json!({"jsonrpc":"2.0","id":id,
                    "error":{"code":-32601,"message":"Unknown MCP method"}}),
            };
            let mut response = serde_json::to_vec(&reply)
                .map_err(|_| "MCP reply encoding failed")?;
            if response.len() >= frame_bytes {
                return Err(
                    "MCP reply frame exceeds bound (execution.collaboration.frame_bytes)".into(),
                );
            }
            response.push(b'\n');
            out_tx
                .try_send(response)
                .map_err(|_| "MCP output unavailable")?;
            let remaining = Duration::from_millis(
                bridge
                    .config
                    .deadline_unix_ms
                    .saturating_sub(process::now_ms()),
            );
            if !ack_rx
                .recv_timeout(remaining)
                .map_err(|_| "MCP output incomplete before deadline")?
            {
                return Err("MCP output closed".into());
            }
            if let Some(index) = delivered {
                bridge.delivered(index)?;
            }
        }
    })();
    if result.is_err() {
        let _ = bridge.stop("bridge_stopped_or_outcome_unresolved");
    }
    result
}
#[cfg(test)]
#[path = "codex_mcp_tests.rs"]
mod tests;
