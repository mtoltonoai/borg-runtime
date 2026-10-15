//! CPU validation lives in a short-lived instance of the same trusted binary.
//! No child remains allocated while the provider HTTP request is pending.
use super::{ProviderTokenUsage, UsageObservation, ValidationHost, completed_text, decode, schema};
use crate::worker::process::ManagedProcess;
use serde_json::Value;
use std::{
    io::{Read, Write},
    path::Path,
    process::ExitStatus,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::OwnedSemaphorePermit,
};

const MAGIC: &[u8; 4] = b"NV1\0";
const HEADER: usize = 27;
const REVISION: u8 = 6;
const REPLY_MAGIC: &[u8; 4] = b"NVR\0";
const USAGE_HEADER: usize = 31;
const OUTCOME_HEADER: usize = 5;

/// Identity of the caller-selected value bound. This is a closed part of the
/// same-binary protocol, never an error label supplied by provider content.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum ReplyBound {
    TaskOutput = 0,
    NativeContext = 1,
    TaskOutputWithDecisionEnvelope = 2,
}
impl ReplyBound {
    pub(super) fn field(self) -> &'static str {
        match self {
            Self::TaskOutput => "task_output_bytes",
            Self::NativeContext => "native_context_bytes",
            Self::TaskOutputWithDecisionEnvelope => {
                "task_output_bytes_and_native_decision_envelope_bytes"
            }
        }
    }
    fn from_wire(value: u8) -> Result<Self, String> {
        match value {
            0 => Ok(Self::TaskOutput),
            1 => Ok(Self::NativeContext),
            2 => Ok(Self::TaskOutputWithDecisionEnvelope),
            _ => Err("native_validation_frame_invalid".into()),
        }
    }
    fn reply_error(self) -> String {
        format!("native_validation_reply_bound_{}", self.field())
    }
}

struct Outcome {
    kind: u8,
    length: usize,
}
fn decode_outcome_header(
    header: &[u8; OUTCOME_HEADER],
    limit: usize,
    bound: ReplyBound,
    policy: &crate::limits::NativeLimits,
) -> Result<Outcome, String> {
    let length = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
    match header[0] {
        0 if length <= limit => {}
        1 if length <= policy.helper_error_bytes => {}
        // These fixed discriminators carry no error string or value payload.
        2 | 3 if length == 0 => {}
        0 => return Err(bound.reply_error()),
        1 => return Err("native_validation_reply_bound_native_helper_error_bytes".into()),
        _ => return Err("native_validation_reply_invalid".into()),
    }
    Ok(Outcome {
        kind: header[0],
        length,
    })
}

struct Unreaped {
    child: ManagedProcess,
    _permit: OwnedSemaphorePermit,
    _owner_lock: Option<Arc<std::fs::File>>,
    #[cfg(debug_assertions)]
    _fixture_registry: Option<Arc<super::host::FixtureRegistry>>,
}
static QUARANTINE: Mutex<Vec<Unreaped>> = Mutex::new(Vec::new());

/// Bounded cleanup observation, not a termination acknowledgment for a failed
/// task. Unverified children retain their CPU slots even after a caller drops.
pub fn validation_cleanup_pending() -> usize {
    let mut pending = QUARANTINE.lock().unwrap_or_else(|e| e.into_inner());
    pending.retain_mut(|owned| {
        if matches!(owned.child.try_finish_registered(), Ok(Some(_))) {
            false
        } else {
            let _ = owned.child.request_kill_registered();
            true
        }
    });
    pending.len()
}

#[cfg(debug_assertions)]
#[derive(Clone, Copy)]
enum ProbeKind {
    Before,
    AfterUsage,
    DirectValue,
}
#[cfg(debug_assertions)]
struct Probe {
    program: std::path::PathBuf,
    kind: ProbeKind,
    ready: tokio::sync::oneshot::Sender<u32>,
}
#[cfg(debug_assertions)]
static PROBES: Mutex<Vec<Probe>> = Mutex::new(Vec::new());

/// Debug qualification only: explicitly register a trusted test-binary path.
/// Neither tasks, provider output, NativeConfig nor environment can request it.
/// The next child on this path sends READY and spins until its parent kills it.
#[cfg(debug_assertions)]
pub fn qualification_hold_next_validation(program: &Path) -> tokio::sync::oneshot::Receiver<u32> {
    let (send, receive) = tokio::sync::oneshot::channel();
    PROBES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(Probe {
            program: program.to_owned(),
            kind: ProbeKind::Before,
            ready: send,
        });
    receive
}

/// Debug qualification of cancellation after complete usage metadata. Normal
/// inputs cannot request this, and schema/direct-Value calls cannot consume it.
#[cfg(debug_assertions)]
pub fn qualification_hold_provider_after_usage(
    program: &Path,
) -> tokio::sync::oneshot::Receiver<u32> {
    let (send, receive) = tokio::sync::oneshot::channel();
    PROBES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(Probe {
            program: program.to_owned(),
            kind: ProbeKind::AfterUsage,
            ready: send,
        });
    receive
}

/// Debug qualification of the final original-schema check after a decision.
/// Schema-only/provider calls cannot consume this registration.
#[cfg(debug_assertions)]
pub fn qualification_hold_direct_value_validation(
    program: &Path,
) -> tokio::sync::oneshot::Receiver<u32> {
    let (send, receive) = tokio::sync::oneshot::channel();
    PROBES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(Probe {
            program: program.to_owned(),
            kind: ProbeKind::DirectValue,
            ready: send,
        });
    receive
}

pub(super) struct State {
    worker_limits: Arc<crate::limits::WorkerLimits>,
    child: Option<ManagedProcess>,
    permit: Option<OwnedSemaphorePermit>,
    provider_usage: UsageObservation,
    provider_completed: bool,
    owner_lock: Option<Arc<std::fs::File>>,
    #[cfg(debug_assertions)]
    fixture_registry: Option<Arc<super::host::FixtureRegistry>>,
}
impl Drop for State {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.request_kill_registered();
            let permit = self.permit.take().expect("spawned child owns its CPU slot");
            QUARANTINE
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(Unreaped {
                    child,
                    _permit: permit,
                    _owner_lock: self.owner_lock.take(),
                    #[cfg(debug_assertions)]
                    _fixture_registry: self.fixture_registry.take(),
                });
        }
    }
}
impl State {
    pub(super) fn new(host: &ValidationHost) -> Self {
        Self {
            worker_limits: host.runtime.worker_limits.clone(),
            child: None,
            permit: None,
            provider_usage: UsageObservation::Unknown,
            provider_completed: false,
            owner_lock: host.retain_owner_lock(),
            #[cfg(debug_assertions)]
            fixture_registry: host.retain_fixture_registry(),
        }
    }
    pub(super) fn provider_usage(&self) -> UsageObservation {
        self.provider_usage
    }
    pub(super) fn provider_completed(&self) -> bool {
        self.provider_completed
    }
    //= spec/impl/v2/implementation.md#the-native-executor-is-bounded
    //# The native executor MUST terminate its attempt when the task's execution deadline is reached.
    // Native component scope: local helper kill/reap; remote provider work cannot be proved stopped by local cancellation.
    pub(super) async fn finish(&mut self, kill: bool) -> Result<Option<ExitStatus>, String> {
        let Some(child) = self.child.as_mut() else {
            return Ok(None);
        };
        // Keep ownership and the CPU slot in self across this await. Cancellation
        // of a normal finish therefore leaves the child available to outer cleanup.
        let status = child
            .finish_registered(
                kill,
                tokio::time::Instant::now()
                    + Duration::from_millis(
                        self.worker_limits.native_validation_cleanup_timeout_ms,
                    ),
            )
            .await
            .map_err(|_| "native_validation_cleanup_unverified")?;
        self.child.take();
        self.permit.take();
        Ok(Some(status))
    }

    pub(super) async fn run(
        &mut self,
        host: &ValidationHost,
        schema_value: &Value,
        wire: Option<&[u8]>,
        limit: usize,
        reply_bound: ReplyBound,
        deadline_unix_ms: u64,
        direct_value: bool,
    ) -> Result<Option<Vec<u8>>, String> {
        self.run_mode(
            host,
            schema_value,
            wire,
            limit,
            reply_bound,
            deadline_unix_ms,
            direct_value,
            false,
            false,
        )
        .await
    }

    pub(super) async fn run_tool(
        &mut self,
        host: &ValidationHost,
        wire: &[u8],
        limit: usize,
        reply_bound: ReplyBound,
        deadline_unix_ms: u64,
    ) -> Result<Option<Vec<u8>>, String> {
        self.run_mode(
            host,
            &Value::Bool(true),
            Some(wire),
            limit,
            reply_bound,
            deadline_unix_ms,
            false,
            true,
            false,
        )
        .await
    }

    pub(super) async fn run_loop(
        &mut self,
        host: &ValidationHost,
        validation: &Value,
        wire: Option<&[u8]>,
        limit: usize,
        reply_bound: ReplyBound,
        deadline_unix_ms: u64,
    ) -> Result<Option<Vec<u8>>, String> {
        self.run_mode(
            host,
            validation,
            wire,
            limit,
            reply_bound,
            deadline_unix_ms,
            false,
            false,
            true,
        )
        .await
    }

    async fn run_mode(
        &mut self,
        host: &ValidationHost,
        schema_value: &Value,
        wire: Option<&[u8]>,
        limit: usize,
        reply_bound: ReplyBound,
        deadline_unix_ms: u64,
        direct_value: bool,
        tool_call: bool,
        loop_call: bool,
    ) -> Result<Option<Vec<u8>>, String> {
        let policy = &host.native_limits;
        policy.validate()?;
        schema::measure(
            schema_value,
            policy.schema_bytes,
            policy,
            "native_schema_bytes",
        )?;
        let encoded = serde_json::to_vec(schema_value).map_err(|_| "native_request_encoding")?;
        let payload = wire.unwrap_or_default();
        if limit == 0 || limit > policy.helper_value_bytes {
            return Err("native_validation_frame_bound_native_helper_value_bytes".into());
        }
        if payload.len() > policy.response_wire_bytes {
            return Err("native_validation_frame_bound_native_response_wire_bytes".into());
        }
        // Ordinary task/schema/output admission precedes any host inspection.
        // Missing schema can therefore refuse even when no helper is installed.
        // Every actual spawn still requires a valid trusted program/registry.
        host.check()?;
        let slots = host.runtime.slots.clone();
        loop {
            validation_cleanup_pending();
            tokio::select! {
                permit = slots.clone().acquire_owned() => {
                    self.permit = Some(permit.map_err(|_| "native_validation_unavailable")?);
                    break;
                }
                _ = tokio::time::sleep(Duration::from_millis(self.worker_limits.native_validation_poll_ms)) => {}
            }
        }
        #[cfg(debug_assertions)]
        let probe = {
            let mut probes = PROBES.lock().unwrap_or_else(|e| e.into_inner());
            probes
                .iter()
                .position(|probe| {
                    probe.program == host.program
                        && match probe.kind {
                            ProbeKind::Before => true,
                            ProbeKind::AfterUsage => wire.is_some() && !direct_value,
                            ProbeKind::DirectValue => wire.is_some() && direct_value,
                        }
                })
                .map(|index| probes.swap_remove(index))
        };
        // This synchronous call registers the exact process before returning
        // stdin. Even a post-spawn registration failure retains actual ownership
        // in State before the caller reaches any cancellation/cleanup await.
        self.child = match ManagedProcess::spawn_registered(
            &host.program,
            &host.registry,
            host.runtime.worker_limits.clone(),
        ) {
            Ok(child) => Some(child),
            Err(failure) => {
                let spawned = failure.owner.is_some();
                self.child = failure.owner;
                return Err(if spawned {
                    "native_validation_registration_failed"
                } else {
                    "native_validation_unavailable"
                }
                .into());
            }
        };
        let child = self.child.as_mut().expect("just spawned");
        let mut stdin = child.stdin.take().ok_or("native_validation_unavailable")?;
        let stdout = child
            .child
            .stdout
            .take()
            .ok_or("native_validation_unavailable")?;
        let mut stdout = stdout;
        let mut header = Vec::with_capacity(HEADER);
        header.extend_from_slice(MAGIC);
        header.push(REVISION);
        let mode = if loop_call {
            7
        } else if tool_call {
            5
        } else if direct_value && wire.is_some() {
            3
        } else {
            u8::from(wire.is_some())
        };
        #[cfg(debug_assertions)]
        let mode = match probe.as_ref().map(|p| p.kind) {
            Some(ProbeKind::Before | ProbeKind::DirectValue) => 2,
            Some(ProbeKind::AfterUsage) => {
                if loop_call {
                    8
                } else if tool_call {
                    6
                } else {
                    4
                }
            }
            None => mode,
        };
        header.push(mode);
        header.extend_from_slice(&(limit as u32).to_be_bytes());
        header.extend_from_slice(&deadline_unix_ms.to_be_bytes());
        header.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
        header.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        header.push(reply_bound as u8);
        stdin
            .write_all(&header)
            .await
            .map_err(|_| "native_validation_io")?;
        stdin
            .write_all(&policy.wire())
            .await
            .map_err(|_| "native_validation_io")?;
        stdin
            .write_all(&encoded)
            .await
            .map_err(|_| "native_validation_io")?;
        stdin
            .write_all(payload)
            .await
            .map_err(|_| "native_validation_io")?;
        drop(stdin);
        #[cfg(debug_assertions)]
        let mut probe = probe;
        #[cfg(debug_assertions)]
        if mode == 2 {
            if stdout.read_u8().await.map_err(|_| "native_validation_io")? != 127 {
                return Err("native_validation_reply_invalid".into());
            }
            let _ = probe.take().expect("registered probe").ready.send(
                self.child
                    .as_ref()
                    .and_then(|owner| owner.child.id())
                    .ok_or("native_validation_cleanup_unverified")?,
            );
        }
        let mut usage_header = [0; USAGE_HEADER];
        stdout
            .read_exact(&mut usage_header)
            .await
            .map_err(|_| "native_validation_io")?;
        let (usage, completed) = decode_usage_header(&usage_header)?;
        if wire.is_some() && !direct_value {
            // Retained outside the cancellable CPU future as soon as the full
            // metadata frame arrives, even if result validation later fails.
            self.provider_usage = usage;
            self.provider_completed = completed;
        } else if usage != UsageObservation::Unknown || completed {
            return Err("native_validation_reply_invalid".into());
        }
        #[cfg(debug_assertions)]
        if matches!(mode, 4 | 6 | 8) {
            if stdout.read_u8().await.map_err(|_| "native_validation_io")? != 127 {
                return Err("native_validation_reply_invalid".into());
            }
            let _ = probe.take().expect("registered usage probe").ready.send(
                self.child
                    .as_ref()
                    .and_then(|owner| owner.child.id())
                    .ok_or("native_validation_cleanup_unverified")?,
            );
        }
        let mut outcome_header = [0; OUTCOME_HEADER];
        stdout
            .read_exact(&mut outcome_header)
            .await
            .map_err(|_| "native_validation_io")?;
        let outcome = decode_outcome_header(&outcome_header, limit, reply_bound, policy)?;
        let mut response = vec![0; outcome.length];
        stdout
            .read_exact(&mut response)
            .await
            .map_err(|_| "native_validation_io")?;
        if stdout
            .read(&mut [0])
            .await
            .map_err(|_| "native_validation_io")?
            != 0
        {
            return Err("native_validation_reply_invalid".into());
        }
        let status = self
            .finish(false)
            .await?
            .ok_or("native_validation_cleanup_unverified")?;
        if !status.success() {
            return Err("native_validation_process_failed".into());
        }
        match outcome.kind {
            0 if wire.is_none() && response.is_empty() => Ok(None),
            0 if wire.is_some() => Ok(Some(response)),
            1 => {
                let error = std::str::from_utf8(&response)
                    .map_err(|_| "native_validation_reply_invalid")?;
                if !error.starts_with("native_")
                    || !error.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
                {
                    return Err("native_validation_reply_invalid".into());
                }
                Err(error.to_owned())
            }
            2 => Err("native_validation_reply_bound_native_helper_error_bytes".into()),
            3 => Err(reply_bound.reply_error()),
            _ => Err("native_validation_reply_invalid".into()),
        }
    }
}

fn encode_usage_header(usage: UsageObservation, completed: bool) -> [u8; USAGE_HEADER] {
    let mut header = [0; USAGE_HEADER];
    header[..4].copy_from_slice(REPLY_MAGIC);
    header[4] = REVISION;
    let counts = match usage {
        UsageObservation::Unknown => {
            header[5] = 0;
            [0, 0, 0]
        }
        UsageObservation::Reported(usage) => {
            header[5] = 1;
            [usage.input_tokens, usage.output_tokens, usage.total_tokens]
        }
        UsageObservation::Invalid => {
            header[5] = 2;
            [0, 0, 0]
        }
    };
    for (index, count) in counts.into_iter().enumerate() {
        header[6 + index * 8..14 + index * 8].copy_from_slice(&count.to_be_bytes());
    }
    header[30] = u8::from(completed);
    header
}

fn decode_usage_header(header: &[u8]) -> Result<(UsageObservation, bool), String> {
    if header.len() != USAGE_HEADER || &header[..4] != REPLY_MAGIC || header[4] != REVISION {
        return Err("native_validation_reply_invalid".into());
    }
    let input = u64::from_be_bytes(header[6..14].try_into().unwrap());
    let output = u64::from_be_bytes(header[14..22].try_into().unwrap());
    let total = u64::from_be_bytes(header[22..30].try_into().unwrap());
    if header[30] > 1 {
        return Err("native_validation_reply_invalid".into());
    }
    let usage: Result<UsageObservation, String> = match header[5] {
        0 if [input, output, total] == [0, 0, 0] => Ok(UsageObservation::Unknown),
        1 => ProviderTokenUsage::checked(input, output, total)
            .map(UsageObservation::Reported)
            .ok_or_else(|| "native_validation_reply_invalid".into()),
        2 if [input, output, total] == [0, 0, 0] => Ok(UsageObservation::Invalid),
        _ => Err("native_validation_reply_invalid".into()),
    };
    Ok((usage?, header[30] == 1))
}

struct ReplyWriter<'a, W: Write> {
    output: &'a mut W,
    usage_sent: bool,
    limits: Option<crate::limits::NativeLimits>,
    value_limit: usize,
}
impl<W: Write> ReplyWriter<'_, W> {
    fn usage(&mut self, usage: UsageObservation, completed: bool) -> Result<(), String> {
        if self.usage_sent {
            return Err("native_validation_reply_invalid".into());
        }
        self.output
            .write_all(&encode_usage_header(usage, completed))
            .map_err(|_| "native_validation_io")?;
        self.output.flush().map_err(|_| "native_validation_io")?;
        self.usage_sent = true;
        Ok(())
    }
    fn finish(&mut self, result: Result<Vec<u8>, String>) -> Result<(), String> {
        if !self.usage_sent {
            self.usage(UsageObservation::Unknown, false)?;
        }
        let (kind, payload) = match result {
            Ok(bytes) if bytes.len() <= self.value_limit => (0, bytes),
            Ok(_) => (3, Vec::new()),
            Err(code) => {
                // Malformed internal errors remain protocol failures, even if
                // their text would also exceed the configured error buffer.
                if !code.starts_with("native_")
                    || !code.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
                {
                    return Err("native_validation_reply_invalid".into());
                }
                if self
                    .limits
                    .as_ref()
                    .is_some_and(|p| code.len() > p.helper_error_bytes)
                {
                    (2, Vec::new())
                } else {
                    (1, code.into_bytes())
                }
            }
        };
        self.output
            .write_all(&[kind])
            .map_err(|_| "native_validation_io")?;
        self.output
            .write_all(&(payload.len() as u32).to_be_bytes())
            .map_err(|_| "native_validation_io")?;
        self.output
            .write_all(&payload)
            .map_err(|_| "native_validation_io")?;
        self.output
            .flush()
            .map_err(|_| "native_validation_io".into())
    }
}

/// Internal same-binary entry. It has no provider credential, HTTP client, shell,
/// task submission or persistent state. Its parent owns the hard deadline.
pub fn validation_worker_stdio() -> Result<(), String> {
    let mut output = std::io::stdout().lock();
    let mut reply = ReplyWriter {
        output: &mut output,
        usage_sent: false,
        limits: None,
        value_limit: 0,
    };
    let result = worker(&mut std::io::stdin().lock(), &mut reply);
    reply.finish(result)
}

fn worker(
    input: &mut impl Read,
    reply: &mut ReplyWriter<'_, impl Write>,
) -> Result<Vec<u8>, String> {
    let mut header = [0; HEADER];
    input
        .read_exact(&mut header)
        .map_err(|_| "native_validation_frame_invalid")?;
    if &header[..4] != MAGIC
        || header[4] != REVISION
        || !(matches!(header[5], 0 | 1 | 3 | 5 | 7)
            || (cfg!(debug_assertions) && matches!(header[5], 2 | 4 | 6 | 8)))
    {
        return Err("native_validation_frame_invalid".into());
    }
    let limit = u32::from_be_bytes(header[6..10].try_into().unwrap()) as usize;
    let deadline_ms = u64::from_be_bytes(header[10..18].try_into().unwrap());
    let schema_len = u32::from_be_bytes(header[18..22].try_into().unwrap()) as usize;
    let wire_len = u32::from_be_bytes(header[22..26].try_into().unwrap()) as usize;
    let reply_bound = ReplyBound::from_wire(header[26])?;
    let mut policy_bytes = [0u8; crate::limits::NativeLimits::WIRE_BYTES];
    input
        .read_exact(&mut policy_bytes)
        .map_err(|_| "native_validation_policy_invalid")?;
    let policy = crate::limits::NativeLimits::from_wire(&policy_bytes)?;
    reply.limits = Some(policy.clone());
    reply.value_limit = limit;
    if limit == 0 || limit > policy.helper_value_bytes {
        return Err("native_validation_frame_bound_native_helper_value_bytes".into());
    }
    if schema_len > policy.schema_bytes {
        return Err("native_validation_frame_bound_native_schema_bytes".into());
    }
    if wire_len > policy.response_wire_bytes {
        return Err("native_validation_frame_bound_native_response_wire_bytes".into());
    }
    if header[5] == 0 && wire_len != 0 {
        return Err("native_validation_frame_bound".into());
    }
    if header[5] == 3 && wire_len > limit {
        return Err(format!(
            "native_validation_frame_bound_{}",
            reply_bound.field()
        ));
    }
    let deadline = super::deadline(deadline_ms)?;
    let mut encoded = vec![0; schema_len];
    input
        .read_exact(&mut encoded)
        .map_err(|_| "native_validation_frame_invalid")?;
    let mut wire = vec![0; wire_len];
    input
        .read_exact(&mut wire)
        .map_err(|_| "native_validation_frame_invalid")?;
    if input
        .read(&mut [0])
        .map_err(|_| "native_validation_frame_invalid")?
        != 0
    {
        return Err("native_validation_frame_invalid".into());
    }
    #[cfg(debug_assertions)]
    if header[5] == 2 {
        reply
            .output
            .write_all(&[127])
            .map_err(|_| "native_validation_io")?;
        reply.output.flush().map_err(|_| "native_validation_io")?;
        loop {
            std::hint::spin_loop();
        }
    }
    let schema_value = decode::parse(
        &encoded,
        policy.schema_bytes,
        deadline,
        "native_schema_invalid",
        &policy,
        "native_schema_bytes",
    )?;
    if header[5] == 7 && wire.is_empty() {
        reply.usage(UsageObservation::Unknown, false)?;
        super::loop_step::preflight(&schema_value, &policy)?;
        super::live(deadline)?;
        return Ok(Vec::new());
    }
    if matches!(header[5], 0 | 3) {
        reply.usage(UsageObservation::Unknown, false)?;
        let schema = schema::Schema::compile(&schema_value, &policy)?;
        super::live(deadline)?;
        if header[5] == 0 {
            return Ok(Vec::new());
        }
        let value = decode::parse(
            &wire,
            limit,
            deadline,
            "native_output_not_json",
            &policy,
            reply_bound.field(),
        )?;
        schema.validate(&value, limit, reply_bound.field())?;
        super::live(deadline)?;
        return Ok(wire);
    }
    let wire_limit = policy.response_wire_bytes;
    let envelope = decode::parse(
        &wire,
        wire_limit,
        deadline,
        "native_response_malformed",
        &policy,
        "native_response_wire_bytes",
    )?;
    let loop_input = if matches!(header[5], 7 | 8) {
        Some(super::loop_step::preflight(&schema_value, &policy)?)
    } else {
        None
    };
    let tool = matches!(header[5], 5 | 6)
        .then(|| super::task_tool::from_envelope(&envelope, limit, deadline, &policy));
    reply.usage(
        UsageObservation::from_envelope(&envelope),
        if let Some(input) = &loop_input {
            super::loop_step::completed(&envelope, &input.profile, deadline, &policy)
        } else {
            tool.as_ref().is_some_and(|result| result.is_ok())
                || super::usage::provider_completed(&envelope)
        },
    )?;
    #[cfg(debug_assertions)]
    if matches!(header[5], 4 | 6 | 8) {
        reply
            .output
            .write_all(&[127])
            .map_err(|_| "native_validation_io")?;
        reply.output.flush().map_err(|_| "native_validation_io")?;
        loop {
            std::hint::spin_loop();
        }
    }
    schema::measure(&envelope, wire_limit, &policy, "native_response_wire_bytes")?;
    if let Some(input) = loop_input {
        return super::loop_step::validate_response(&envelope, &input, limit, deadline, &policy);
    }
    if let Some(result) = tool {
        let context = super::task_tool::context(result?);
        return serde_json::to_vec(&context).map_err(|_| "native_tool_context_encoding".into());
    }
    let schema = schema::Schema::compile(&schema_value, &policy)?;
    super::live(deadline)?;
    let text = completed_text(&envelope, limit, reply_bound)?;
    let value = decode::parse(
        text.as_bytes(),
        limit,
        deadline,
        "native_output_not_json",
        &policy,
        reply_bound.field(),
    )?;
    schema.validate(&value, limit, reply_bound.field())?;
    super::live(deadline)?;
    Ok(text.into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_reply_revision_and_counter_admission_are_exact() {
        let known = UsageObservation::Reported(ProviderTokenUsage::checked(17, 9, 26).unwrap());
        let header = encode_usage_header(known, true);
        assert_eq!(decode_usage_header(&header).unwrap(), (known, true));
        for revision in [0, 1, 2, 3, 4, 5, 255] {
            let mut wrong = header;
            wrong[4] = revision;
            assert!(decode_usage_header(&wrong).is_err());
        }
        assert!(decode_usage_header(&header[..29]).is_err());
        let mut inconsistent = header;
        inconsistent[22..30].copy_from_slice(&27u64.to_be_bytes());
        assert!(decode_usage_header(&inconsistent).is_err());
        let mut excessive = header;
        excessive[6..14].copy_from_slice(&u64::MAX.to_be_bytes());
        assert!(decode_usage_header(&excessive).is_err());
        let mut bad_tag = header;
        bad_tag[5] = 3;
        assert!(decode_usage_header(&bad_tag).is_err());
        let mut bad_completion = header;
        bad_completion[30] = 2;
        assert!(decode_usage_header(&bad_completion).is_err());
    }

    #[test]
    fn usage_unknown_invalid_and_reported_zero_are_distinct() {
        for value in [
            UsageObservation::Unknown,
            UsageObservation::Invalid,
            UsageObservation::Reported(ProviderTokenUsage::checked(0, 0, 0).unwrap()),
        ] {
            for completed in [false, true] {
                assert_eq!(
                    decode_usage_header(&encode_usage_header(value, completed)).unwrap(),
                    (value, completed)
                );
            }
        }
        let mut unknown = encode_usage_header(UsageObservation::Unknown, false);
        unknown[6..14].copy_from_slice(&1u64.to_be_bytes());
        assert!(decode_usage_header(&unknown).is_err());
    }
}

#[cfg(test)]
mod helper_diagnostics_tests {
    use super::*;

    fn policy() -> crate::limits::NativeLimits {
        crate::limits::RuntimeLimits::seed()
            .unwrap()
            .execution
            .native
    }

    fn header(kind: u8, length: u32) -> [u8; 5] {
        let mut bytes = [0; 5];
        bytes[0] = kind;
        bytes[1..].copy_from_slice(&length.to_be_bytes());
        bytes
    }

    fn finish(
        policy: crate::limits::NativeLimits,
        value_limit: usize,
        result: Result<Vec<u8>, String>,
    ) -> (Result<(), String>, Vec<u8>) {
        let mut output = Vec::new();
        let result = ReplyWriter {
            output: &mut output,
            // Isolate the outcome frame; the usage frame is tested separately.
            usage_sent: true,
            limits: Some(policy),
            value_limit,
        }
        .finish(result);
        (result, output)
    }

    #[test]
    fn helper_revision_and_bound_sources_are_explicit() {
        assert_eq!(REVISION, 6);
        assert_eq!(HEADER, 27);
        assert_eq!(OUTCOME_HEADER, 5);
        assert_eq!(
            &encode_usage_header(UsageObservation::Unknown, false)[..6],
            b"NVR\0\x06\x00"
        );
        assert_eq!(
            ReplyBound::TaskOutput.reply_error(),
            "native_validation_reply_bound_task_output_bytes"
        );
        assert_eq!(
            ReplyBound::NativeContext.reply_error(),
            "native_validation_reply_bound_native_context_bytes"
        );
        assert_eq!(
            ReplyBound::TaskOutputWithDecisionEnvelope.reply_error(),
            "native_validation_reply_bound_task_output_bytes_and_native_decision_envelope_bytes"
        );
        assert!(ReplyBound::from_wire(u8::MAX).is_err());
    }

    #[test]
    fn success_header_accepts_exact_bound_and_refuses_next_byte_by_source() {
        let policy = policy();
        let exact =
            decode_outcome_header(&header(0, 1), 1, ReplyBound::TaskOutput, &policy).unwrap();
        assert_eq!((exact.kind, exact.length), (0, 1));
        for (source, expected) in [
            (
                ReplyBound::TaskOutput,
                "native_validation_reply_bound_task_output_bytes",
            ),
            (
                ReplyBound::NativeContext,
                "native_validation_reply_bound_native_context_bytes",
            ),
            (
                ReplyBound::TaskOutputWithDecisionEnvelope,
                "native_validation_reply_bound_task_output_bytes_and_native_decision_envelope_bytes",
            ),
        ] {
            assert_eq!(
                decode_outcome_header(&header(0, 2), 1, source, &policy)
                    .err()
                    .as_deref(),
                Some(expected)
            );
        }
    }

    #[test]
    fn error_header_uses_error_cap_independently_of_value_cap() {
        let mut policy = policy();
        policy.helper_error_bytes = 8;
        let exact =
            decode_outcome_header(&header(1, 8), 1, ReplyBound::TaskOutput, &policy).unwrap();
        assert_eq!((exact.kind, exact.length), (1, 8));
        assert_eq!(
            decode_outcome_header(&header(1, 9), 100, ReplyBound::NativeContext, &policy)
                .err()
                .as_deref(),
            Some("native_validation_reply_bound_native_helper_error_bytes")
        );
        policy.helper_error_bytes = 1;
        let tiny =
            decode_outcome_header(&header(1, 1), 1, ReplyBound::TaskOutput, &policy).unwrap();
        assert_eq!((tiny.kind, tiny.length), (1, 1));
        assert_eq!(
            decode_outcome_header(&header(1, 2), 100, ReplyBound::TaskOutput, &policy)
                .err()
                .as_deref(),
            Some("native_validation_reply_bound_native_helper_error_bytes")
        );
    }

    #[test]
    fn overflow_control_headers_are_closed_and_have_no_payload() {
        let policy = policy();
        for kind in [2, 3] {
            let control =
                decode_outcome_header(&header(kind, 0), 1, ReplyBound::TaskOutput, &policy)
                    .unwrap();
            assert_eq!((control.kind, control.length), (kind, 0));
            for length in [1, u32::MAX] {
                assert_eq!(
                    decode_outcome_header(
                        &header(kind, length),
                        1,
                        ReplyBound::TaskOutput,
                        &policy,
                    )
                    .err()
                    .as_deref(),
                    Some("native_validation_reply_invalid")
                );
            }
        }
        for kind in [4, u8::MAX] {
            assert_eq!(
                decode_outcome_header(&header(kind, 0), 1, ReplyBound::TaskOutput, &policy)
                    .err()
                    .as_deref(),
                Some("native_validation_reply_invalid")
            );
        }
    }

    #[test]
    fn writer_preserves_exact_success_and_emits_payload_free_overflow() {
        let (result, bytes) = finish(policy(), 1, Ok(vec![b'x']));
        result.unwrap();
        assert_eq!(bytes, [0, 0, 0, 0, 1, b'x']);
        let (result, bytes) = finish(policy(), 1, Ok(vec![b'x', b'y']));
        result.unwrap();
        assert_eq!(bytes, [3, 0, 0, 0, 0]);
    }

    #[test]
    fn writer_preserves_exact_error_and_reports_error_cap_overflow() {
        let mut policy = policy();
        policy.helper_error_bytes = "native_x".len();
        let (result, bytes) = finish(policy.clone(), 1, Err("native_x".into()));
        result.unwrap();
        let mut expected = header(1, 8).to_vec();
        expected.extend_from_slice(b"native_x");
        assert_eq!(bytes, expected);
        let (result, bytes) = finish(policy.clone(), 1, Err("native_xx".into()));
        result.unwrap();
        assert_eq!(bytes, [2, 0, 0, 0, 0]);
        policy.helper_error_bytes = 1;
        let (result, bytes) = finish(policy, 1, Err("native_x".into()));
        result.unwrap();
        assert_eq!(bytes, [2, 0, 0, 0, 0]);
    }

    #[test]
    fn malformed_error_precedes_error_size_refusal() {
        let mut policy = policy();
        policy.helper_error_bytes = 1;
        for code in [
            "Native_error",
            "native_bad-error",
            "native_bad9",
            "wrong_prefix",
        ] {
            assert!(code.len() > policy.helper_error_bytes);
            let (result, bytes) = finish(policy.clone(), 1, Err(code.into()));
            assert_eq!(
                result.err().as_deref(),
                Some("native_validation_reply_invalid")
            );
            assert!(
                bytes.is_empty(),
                "malformed error must not emit an overflow frame"
            );
        }
    }
}
