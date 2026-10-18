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
const REVISION: u8 = 8;
const REPLY_MAGIC: &[u8; 4] = b"NVR\0";
const USAGE_HEADER: usize = 31;
const OUTCOME_HEADER: usize = 5;

const BODY_CHUNK: usize = 8192;
fn append_body(body: &mut Vec<u8>, bytes: &[u8]) -> Result<(), String> {
    body.try_reserve(bytes.len()).map_err(|_| "native_validation_allocation_failed")?;
    body.extend_from_slice(bytes);
    Ok(())
}
fn read_body(input: &mut impl Read, length: usize) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    let mut chunk = [0u8; BODY_CHUNK];
    while body.len() < length {
        let take = (length - body.len()).min(chunk.len());
        let count = match input.read(&mut chunk[..take]) {
            Ok(count) => count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return Err("native_validation_frame_invalid".into()),
        };
        if count == 0 { return Err("native_validation_frame_invalid".into()); }
        append_body(&mut body, &chunk[..count])?;
    }
    Ok(body)
}
async fn read_async_body(input: &mut (impl tokio::io::AsyncRead + Unpin), length: usize) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    let mut chunk = [0u8; BODY_CHUNK];
    while body.len() < length {
        let take = (length - body.len()).min(chunk.len());
        let count = input.read(&mut chunk[..take]).await.map_err(|_| "native_validation_io")?;
        if count == 0 { return Err("native_validation_io".into()); }
        append_body(&mut body, &chunk[..count])?;
    }
    Ok(body)
}


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
        0 | 4 | 5 if length <= limit => {}
        1 if length <= policy.helper_error_bytes => {}
        // These fixed discriminators carry no error string or value payload.
        2 | 3 if length == 0 => {}
        0 | 4 | 5 => return Err(bound.reply_error()),
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
    provider_rejection: Option<super::ProviderRejection>,
    provider_diagnostic: Option<super::ProviderFailureDiagnostic>,
    format_issue: Option<super::FormatIssue>,
    output_capped: bool,
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
            provider_rejection: None,
            provider_diagnostic: None,
            format_issue: None,
            output_capped: false,
            owner_lock: host.retain_owner_lock(),
            #[cfg(debug_assertions)]
            fixture_registry: host.retain_fixture_registry(),
        }
    }
    pub(super) fn provider_output_capped(&self) -> bool { self.output_capped }
    pub(super) fn format_issue(&self) -> Option<&super::FormatIssue> { self.format_issue.as_ref() }
    pub(super) fn provider_usage(&self) -> UsageObservation {
        self.provider_usage
    }
    pub(super) fn provider_rejection(&self) -> Option<super::ProviderRejection> {
        self.provider_rejection
    }
    pub(super) fn record_rejection(&mut self, rejection: Option<super::ProviderRejection>) {
        self.provider_rejection = rejection;
    }
    pub(super) fn record_diagnostic(&mut self, diagnostic: super::ProviderFailureDiagnostic) {
        self.provider_diagnostic = Some(diagnostic);
    }
    /// Keep the first concrete cause and all observed HTTP facts when later
    /// cleanup replaces the public error. Pending headers alone are not a cause.
    pub(super) fn record_failure(&mut self, class: super::ProviderFailureClass) {
        if self.provider_diagnostic.as_ref().and_then(|v| v.http.as_ref())
            .is_some_and(|v| v.body == super::DiagnosticBodyState::Pending) {
            match class {
                super::ProviderFailureClass::Cancelled => self.diagnostic_body(super::DiagnosticBodyState::Cancelled),
                super::ProviderFailureClass::Deadline => self.diagnostic_body(super::DiagnosticBodyState::TaskDeadline),
                _ => {},
            }
        }
        match self.provider_diagnostic.as_mut() {
            Some(value) if value.class == super::ProviderFailureClass::HttpResponsePending => value.set_class(class),
            None => self.provider_diagnostic = Some(super::ProviderFailureDiagnostic::new(class)),
            Some(_) => {},
        }
    }
    pub(super) fn diagnostic_body(&mut self, body: super::DiagnosticBodyState) {
        if let Some(value) = self.provider_diagnostic.as_mut() {
            if value.http.as_ref().is_some_and(|http| http.body != super::DiagnosticBodyState::Redacted) {
                value.set_body(body);
            }
        }
    }
    pub(super) fn record_result_failure(&mut self, exposure: super::RequestExposure, deadline: tokio::time::Instant) {
        use super::ProviderFailureClass as Class;
        self.record_failure(if tokio::time::Instant::now() >= deadline { Class::Deadline }
            else if exposure == super::RequestExposure::NotStarted { Class::LocalAdmission }
            else { Class::ResponseValidation });
    }
    pub(super) fn finish_diagnostic(&mut self, failed: bool, exposure: super::RequestExposure)
        -> Option<super::ProviderFailureDiagnostic> {
        use super::ProviderFailureClass as Class;
        if self.output_capped { self.record_failure(Class::OutputLimit); }
        else if self.format_issue.is_some() { self.record_failure(Class::ResponseValidation); }
        else if failed {
            self.record_failure(if exposure == super::RequestExposure::NotStarted {
                Class::LocalAdmission
            } else { Class::ResponseValidation });
        } else if self.provider_diagnostic.as_ref().is_none_or(|value| value.class == Class::HttpResponsePending) {
            return None;
        }
        self.provider_diagnostic.take()
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
        let status = match child
            .finish_registered(
                kill,
                tokio::time::Instant::now()
                    + Duration::from_millis(
                        self.worker_limits.native_validation_cleanup_timeout_ms,
                    ),
            )
            .await {
                Ok(status) => status,
                Err(_) => {
                    self.record_failure(super::ProviderFailureClass::ValidationCleanup);
                    return Err("native_validation_cleanup_unverified".into());
                }
            };
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
        self.format_issue = None;
        self.output_capped = false;
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
        let response = read_async_body(&mut stdout, outcome.length).await?;
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
            4 if loop_call && wire.is_some() && completed
                && schema_value.get("repair_enabled").and_then(Value::as_bool) == Some(true) => {
                // Typed repair is admitted only after full payload, EOF and a
                // successful reaped child. A usage header alone cannot grant it.
                let value = decode::parse(&response, limit, super::deadline(deadline_unix_ms)?,
                    "native_validation_reply_invalid", policy, reply_bound.field())?;
                let typed: FormatReply = serde_json::from_value(value).map_err(|_| "native_validation_reply_invalid")?;
                let items = serde_json::to_vec(&typed.items).map_err(|_| "native_validation_reply_invalid")?;
                self.format_issue = Some(typed.issue);
                Ok(Some(items))
            },
            5 if loop_call && wire.is_some() && !completed
                && schema_value.get("repair_enabled").and_then(Value::as_bool)==Some(true) => {
                let value=decode::parse(&response,limit,super::deadline(deadline_unix_ms)?,
                    "native_validation_reply_invalid",policy,reply_bound.field())?;
                if !value.is_array() {return Err("native_validation_reply_invalid".into());}
                self.output_capped=true;Ok(Some(response))
            },
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

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FormatReply { items: Value, issue: super::FormatIssue }

struct ReplyWriter<'a, W: Write> {
    output: &'a mut W,
    usage_sent: bool,
    limits: Option<crate::limits::NativeLimits>,
    value_limit: usize,
    format_reply: bool,
    cap_reply: bool,
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
            Ok(bytes) if bytes.len() <= self.value_limit => (if self.cap_reply {5} else if self.format_reply { 4 } else { 0 }, bytes),
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
        format_reply: false,
        cap_reply: false,
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
    let encoded = read_body(input, schema_len)?;
    let wire = read_body(input, wire_len)?;
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
        let (bytes, issue) = super::loop_step::validate_response_typed(&envelope, &input, limit, deadline, &policy)?;
        if super::loop_step::capped(&envelope,&input,deadline,&policy) {
            reply.cap_reply=true;return Ok(bytes);
        }
        if let Some(issue) = issue {
            let items = decode::parse(&bytes, limit, deadline, "native_tool_context_invalid", &policy, reply_bound.field())?;
            let encoded = serde_json::to_vec(&FormatReply { items, issue }).map_err(|_| "native_validation_reply_invalid")?;
            reply.format_reply = true;
            return Ok(encoded);
        }
        return Ok(bytes);
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
        for revision in [0, 1, 2, 3, 4, 5, 6, 7, 255] {
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
            format_reply: false,
        cap_reply: false,
        }
        .finish(result);
        (result, output)
    }

    #[test]
    fn helper_revision_and_bound_sources_are_explicit() {
        assert_eq!(REVISION, 8);
        assert_eq!(HEADER, 27);
        assert_eq!(OUTCOME_HEADER, 5);
        assert_eq!(
            &encode_usage_header(UsageObservation::Unknown, false)[..6],
            b"NVR\0\x08\x00"
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
        for kind in [6, u8::MAX] {
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


#[cfg(test)]
mod maximum_context_frame_tests {
    use super::*;
    struct ShortBody { bytes: std::io::Cursor<Vec<u8>>, largest_read: usize }
    impl Read for ShortBody {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            self.largest_read = self.largest_read.max(out.len());
            Read::read(&mut self.bytes, out)
        }
    }
    #[test]
    fn advertised_maximum_frame_does_not_allocate_its_declared_length() {
        let mut source=ShortBody { bytes:std::io::Cursor::new(b"abc".to_vec()), largest_read:0 };
        assert_eq!(read_body(&mut source,u32::MAX as usize).unwrap_err(),"native_validation_frame_invalid");
        assert!(source.largest_read <= BODY_CHUNK);
        assert_eq!(read_body(&mut &b"abc"[..],3).unwrap(),b"abc");
        assert_eq!(read_body(&mut &b""[..],0).unwrap(),Vec::<u8>::new());
    }
    #[tokio::test]
    async fn advertised_maximum_reply_reads_only_received_chunks() {
        assert_eq!(read_async_body(&mut &b"abc"[..],u32::MAX as usize).await.unwrap_err(),"native_validation_io");
        assert_eq!(read_async_body(&mut &b"abc"[..],3).await.unwrap(),b"abc");
    }
}

#[cfg(test)]
mod diagnostic_cleanup_source_tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Area(std::path::PathBuf);
    impl Drop for Area {
        fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); }
    }

    // An actual registered child exercises State::finish's error source. This
    // does not claim a provider response or native helper-protocol observation.
    #[tokio::test]
    async fn actual_registry_cleanup_failure_records_cause_before_wrapper_fallback() {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nonce = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
            .unwrap().as_nanos();
        let area = Area(std::env::temp_dir().join(format!("borg-native-cleanup-cause-{}-{nonce}-{}",
            std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed))));
        std::fs::create_dir(&area.0).unwrap();
        let registry = area.0.join("processes");
        let limits = crate::limits::RuntimeLimits::seed().unwrap();
        let worker = Arc::new(limits.worker);
        let runtime = Arc::new(super::super::ValidationRuntime::new(worker.clone()).unwrap());
        let slots = runtime.slots.clone();
        let available_before = slots.available_permits();
        let host = ValidationHost::new("/bin/sleep".into(), registry.clone(),
            Arc::new(limits.execution.native), runtime);
        let mut state = State::new(&host);
        state.permit = Some(slots.clone().acquire_owned().await.unwrap());
        state.child = Some(ManagedProcess::spawn(Path::new("/bin/sleep"), &["60".into()],
            &area.0, &registry, &[], worker.clone()).await.unwrap());
        let pid = state.child.as_ref().and_then(|owner| owner.child.id()).unwrap();
        let record = registry.join(format!("{pid}.json"));
        let backup = area.0.join("original-record.json");
        let original = std::fs::read(&record);
        let mut moved = false;
        let injected = (|| -> std::io::Result<()> {
            std::fs::rename(&record, &backup)?;
            moved = true;
            std::fs::create_dir(&record)
        })();
        // A directory at the exact owned record makes refresh_ownership's
        // atomic rename fail. No cancellation or outer error classifier runs.
        let failed_finish = if injected.is_ok() { Some(state.finish(false).await) } else { None };
        let retained_child = state.child.as_ref().and_then(|owner| owner.child.id()) == Some(pid);
        let retained_permit = state.permit.is_some();
        let available_at_failure = slots.available_permits();
        let diagnostic = state.finish_diagnostic(true, super::super::RequestExposure::NotStarted);

        // Restore and kill/reap before any fault-result assertion can panic.
        let restored = (|| -> std::io::Result<()> {
            if moved {
                if record.is_dir() { std::fs::remove_dir(&record)?; }
                std::fs::rename(&backup, &record)?;
            }
            Ok(())
        })();
        let restored_bytes = std::fs::read(&record);
        let cleanup = state.finish(true).await;
        // Even unexpected restore/cleanup failure must not let an assertion
        // leave this fixture's simple, non-forking child alive or unreaped.
        let fallback_reap = if cleanup.is_err() {
            if let Some(owner) = state.child.as_mut() {
                Some(tokio::time::timeout(
                    Duration::from_millis(worker.native_validation_cleanup_timeout_ms),
                    owner.child.kill(),
                ).await)
            } else { None }
        } else { None };
        let pid_absent = !Path::new(&format!("/proc/{pid}")).exists();
        let state_released = state.child.is_none() && state.permit.is_none();

        assert!(cleanup.is_ok(), "restored cleanup: {cleanup:?}; fallback: {fallback_reap:?}");
        assert!(pid_absent, "registered child was killed and reaped before assertions");
        assert!(injected.is_ok(), "exact record fault setup: {injected:?}");
        assert!(restored.is_ok(), "exact record restoration: {restored:?}");
        assert_eq!(original.unwrap(), restored_bytes.unwrap());
        assert_eq!(failed_finish.unwrap().unwrap_err(), "native_validation_cleanup_unverified");
        assert!(retained_child && retained_permit);
        assert_eq!(available_at_failure + 1, available_before);
        assert_eq!(diagnostic.unwrap().class, super::super::ProviderFailureClass::ValidationCleanup);
        assert!(state_released);
        assert_eq!(slots.available_permits(), available_before);
        assert!(!record.exists());
        assert!(!record.with_extension("pending").exists());
        assert_eq!(state.provider_usage(), UsageObservation::Unknown);
        assert!(!state.provider_completed());
        assert!(state.provider_rejection().is_none());
    }
}
