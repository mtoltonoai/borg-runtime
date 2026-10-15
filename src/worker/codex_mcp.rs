//! Task-scoped stdio MCP adapter. This is host correlation, not a new auth boundary.
//! The sole bridge owner retains an immutable occurrence before HTTP; lost replies
//! are lookup-only. No unresolved result is returned to the supplied executor.
use super::process;
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
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
        self.caller.validate(&self.runtime_limits.transport)?;
        self.limits.validate()?;
        self.worker_limits.validate()?;
        if !self.state.is_absolute()
            || self.deadline_unix_ms == 0
            || self.deadline_unix_ms > i64::MAX as u64
        {
            return Err("invalid collaboration bridge configuration".into());
        }
        Client::new(
            &self.controller_url,
            self.caller.clone(),
            self.runtime_limits.clone(),
        )?;
        Ok(())
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
    if journal.revision != REVISION
        || journal.caller != config.caller
        || journal.deadline_unix_ms != config.deadline_unix_ms
        || journal.controller_url != config.controller_url
        || journal.runtime_limits != config.runtime_limits
        || journal.limits != config.limits
        || journal.worker_limits != config.worker_limits
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
    outcome: Option<CallResult>,
    delivered: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Journal {
    runtime_limits: RuntimeLimits,
    limits: CollaborationLimits,
    worker_limits: WorkerLimits,
    revision: u32,
    caller: Caller,
    deadline_unix_ms: u64,
    controller_url: String,
    entries: Vec<Entry>,
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

trait Transport {
    async fn call(&self, call: &Invocation) -> Result<CallResult, String>;
    async fn lookup(&self, call: &Invocation) -> Result<Option<CallResult>, String>;
}
impl Transport for Client {
    async fn call(&self, call: &Invocation) -> Result<CallResult, String> {
        Client::call(self, call).await
    }
    async fn lookup(&self, call: &Invocation) -> Result<Option<CallResult>, String> {
        Client::get_call(self, call).await
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
            || journal.revision != REVISION
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
                Self::check_result(&e.invocation, outcome, config.limits.complete_result_bytes)?;
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
        bounded_save(
            &self.path,
            &self.journal,
            self.config.limits.journal_bytes,
            "execution.collaboration.journal_bytes",
        )
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
    ) -> Result<(Value, usize), String> {
        self.live()?;
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
        if !mcp::catalogue(&self.config.runtime_limits)
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
                self.journal.entries.pop();
                return Err("collaboration journal admission bound (execution.collaboration.journal_bytes; reserves execution.collaboration.complete_result_bytes)".into());
            }
            self.save()?;
            self.journal.entries.len() - 1
        };
        let call = self.journal.entries[index].invocation.clone();
        if self.journal.entries[index].outcome.is_none() {
            // New durable intent sends at most once. A recovered intent is
            // lookup-only: absence after a crash is not proof no send occurred.
            let first = existing.is_none();
            let mut observed = if first {
                self.until(self.transport.call(&call)).await.ok()
            } else {
                None
            };
            loop {
                if let Some(result) = observed.take() {
                    if Self::check_result(&call, &result, self.config.limits.complete_result_bytes)
                        .is_ok()
                    {
                        safe_value(&result, &self.credentials, &self.config.worker_limits)?;
                        self.journal.entries[index].outcome = Some(result);
                        self.save()?;
                        break;
                    }
                }
                self.live()?;
                observed = self
                    .until(self.transport.lookup(&call))
                    .await
                    .ok()
                    .flatten();
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
                }
            }
        }
        let result = self.journal.entries[index]
            .outcome
            .as_ref()
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
    // Traceability scope: The coding tool boundary is exposed only after an original tool outcome is retained, so notice delivery cannot preempt that tool.
    //= spec/impl/v2/implementation.md#shared-memory-changes-reach-sessions
    //# Memory-change notification delivery MUST NOT interrupt a tool that has begun execution.
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
pub fn outcome_disposition(config: &Config) -> Result<OutcomeDisposition, String> {
    config.validate()?;
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
        || journal.revision != REVISION
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
        entry.invocation.validate(&config.runtime_limits)?;
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
            Some(outcome) => Bridge::<Client>::check_result(
                &entry.invocation,
                outcome,
                config.limits.complete_result_bytes,
            )?,
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
    let enabled = mcp::catalogue(runtime_limits)
        .into_iter()
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
    let client = Client::new(
        &config.controller_url,
        config.caller.clone(),
        config.runtime_limits.clone(),
    )?;
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
            let (body, delivered) = match method {
                "initialize" => {
                    if params.get("protocolVersion").and_then(Value::as_str) != Some(mcp::VERSION) {
                        return Err("unsupported MCP version".into());
                    }
                    (
                        json!({"protocolVersion":mcp::VERSION,"capabilities":{"tools":{"listChanged":false}},"serverInfo":{"name":"borg-collaboration-bridge","version":"1"}}),
                        None,
                    )
                }
                "tools/list" => {
                    let tools = runtime
                        .block_on(bridge.until(bridge.transport.tools()))?
                        .into_iter()
                        .filter(|t| t.name != "collab_task_delegate")
                        .collect::<Vec<_>>();
                    (json!({"tools":tools}), None)
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
                    let (body, index) =
                        runtime.block_on(bridge.invoke(&call.name, call.arguments, &call.meta))?;
                    runtime.block_on(bridge.hold_memory_boundary(index))?;
                    (body, Some(index))
                }
                "ping" => (json!({}), None),
                _ => return Err("unsupported MCP method".into()),
            };
            let mut response = serde_json::to_vec(&json!({"jsonrpc":"2.0","id":id,"result":body}))
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
