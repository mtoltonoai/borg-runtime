//! Coding worker for the task-boundary v2 profile.
mod acceptance;
pub mod codex;
pub mod codex_mcp;
pub mod git;
mod handoff;
mod memory_notice;
mod mutations;
mod native_loop;
mod native_tool;
pub mod process;
mod publication;
mod resource;

use crate::{
    client::ApiClient,
    limits::{CodingLimits, RuntimeLimits, WorkerLimits},
    protocol::*,
};
use process::{Control, atomic_json, component};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, watch};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct CommitContext {
    commit: MutationCommit,
    observed_controller_revision: u64,
}
impl CommitContext {
    fn from_reply(reply: &Reply, operation: &Operation) -> Result<Option<Self>, String> {
        reply.validate_for(operation)?;
        Ok(reply.commit.clone().map(|commit| Self {
            commit,
            // validate_for requires this for every stamped reply.
            observed_controller_revision: reply.observed_controller_revision.unwrap(),
        }))
    }
    fn validate(&self, operation: &Operation, outcome: &Outcome) -> Result<(), String> {
        Reply {
            request_id: "retained-context-validation".into(),
            commit: Some(self.commit.clone()),
            observed_controller_revision: Some(self.observed_controller_revision),
            outcome: outcome.clone(),
        }
        .validate_for(operation)
    }
}

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub limits: Arc<RuntimeLimits>,
    /// Operator-installed worker envelope; None is unqualified development mode.
    pub resource_profile: Option<PathBuf>,
    pub controller_url: String,
    pub id: String,
    pub incarnation: String,
    pub workspace_root: PathBuf,
    pub codex_path: PathBuf,
    pub coding_capacity: usize,
    pub native_capacity: usize,
    /// Host configuration only; never selected by task/model input.
    pub native_endpoint: String,
    /// Trusted host executable for the private validator, never task input.
    pub validation_executable: PathBuf,
}

#[derive(Clone)]
struct Reporter {
    resource: Option<Arc<resource::Envelope>>,
    client: ApiClient,
    config: WorkerConfig,
    attempt: Attempt,
    outbox: PathBuf,
    sequence: Arc<AtomicU64>,
    executor_seen: Arc<AtomicBool>,
    provider_uncertain: Arc<AtomicBool>,
    uncertainty_retained: Arc<AtomicBool>,
    output_limit: usize,
    result_limit: usize,
    coding_limits: CodingLimits,
    outbox_lock: Arc<tokio::sync::Mutex<()>>,
}
impl Reporter {
    async fn send(&self, observation: Observation) -> Result<(), String> {
        self.send_acknowledged(observation).await.map(|_| ())
    }
    async fn send_acknowledged(&self, observation: Observation) -> Result<bool, String> {
        let report_id = format!(
            "{}-report-{:020}",
            self.attempt.id,
            self.sequence.fetch_add(1, Ordering::Relaxed)
        );
        self.send_identified(report_id, observation).await
    }
    async fn send_identified(
        &self,
        report_id: String,
        observation: Observation,
    ) -> Result<bool, String> {
        let credentials = configured_credentials(&self.config.limits)?;
        self.send_identified_with_credentials(report_id, observation, &credentials)
            .await
    }
    // Private seam lets tests use synthetic credentials without mutating process
    // environment. Production reads only the fixed provider/token and proxy names.
    async fn send_identified_with_credentials(
        &self,
        report_id: String,
        mut observation: Observation,
        credentials: &[String],
    ) -> Result<bool, String> {
        if [
            report_id.as_str(),
            self.config.id.as_str(),
            self.config.incarnation.as_str(),
            self.attempt.task_id.as_str(),
            self.attempt.id.as_str(),
        ]
        .into_iter()
        .any(|identity| contains_credential(identity, credentials))
            || matches!(&observation, Observation::NativeLoopCompleted { calls, .. }
                if calls.iter().any(|c| contains_credential(&c.operation_id, credentials) || contains_credential(&c.logical_call_id, credentials)))
            || matches!(&observation, Observation::Started { thread_id, turn_id }
                if thread_id.iter().chain(turn_id).any(|id| contains_credential(id, credentials)))
        {
            return Err("report identity contains a configured credential; report withheld".into());
        }
        if let Observation::ResultCandidate {
            checkpoint, output, ..
        } = &observation
        {
            let inspection = output
                .as_ref()
                .map(|value| {
                    value_contains_credential(value, credentials, &self.config.limits.worker)
                })
                .unwrap_or(Ok(false));
            let reason = if checkpoint
                .as_ref()
                .is_some_and(|value| checkpoint_contains_credential(value, credentials))
                || matches!(inspection, Ok(true))
            {
                Some("Computed result contains a configured credential; result withheld".to_owned())
            } else if let Err(reason) = inspection {
                Some(format!("Computed result exceeds bounded credential inspection; result withheld: {reason}"))
            } else {
                None
            };
            if let Some(reason) = reason {
                // Never mutate schema-validated output or source identities into
                // an apparently successful result. Published effects and the
                // controller's selected checkpoint are not changed by this report.
                observation = Observation::ResultCandidate {
                    checks_passed: false,
                    summary: reason.into(),
                    checkpoint: None,
                    validation: vec![],
                    output: None,
                };
            }
        }
        match &mut observation {
            Observation::Progress {
                text,
                output,
                truncated,
            } => {
                *text = scrub_known_characters(
                    std::mem::take(text),
                    self.coding_limits.progress_chars,
                    self.client.limits().controller.max_progress_bytes,
                    credentials,
                );
                *truncated |= output.len() > self.output_limit;
                *output = scrub_known(std::mem::take(output), self.output_limit, credentials);
            }
            Observation::ExecutorOutcome { summary, .. } => {
                *summary = scrub_known(
                    std::mem::take(summary),
                    self.output_limit.min(self.coding_limits.summary_bytes),
                    credentials,
                )
            }
            Observation::ResultCandidate {
                summary,
                validation,
                ..
            } => {
                *summary = scrub_known(
                    std::mem::take(summary),
                    self.output_limit.min(self.coding_limits.summary_bytes),
                    credentials,
                );
                for diagnostic in validation {
                    *diagnostic = scrub_known(
                        std::mem::take(diagnostic),
                        self.config.limits.transport.body_bytes,
                        credentials,
                    );
                }
            }
            Observation::ExecutionUncertain { message } => {
                *message = scrub_known_characters(
                    std::mem::take(message),
                    self.coding_limits.progress_chars,
                    self.client.limits().controller.max_progress_bytes,
                    credentials,
                )
            }
            _ => {}
        }
        if let Observation::ResultCandidate {
            checks_passed: true,
            summary,
            checkpoint,
            validation,
            output,
        } = &observation
        {
            let result = AcceptedResult {
                attempt_id: self.attempt.id.clone(),
                summary: summary.clone(),
                checkpoint: checkpoint.clone(),
                validation: validation.clone(),
                output: output.clone(),
            };
            if self.result_limit == 0
                || self.result_limit >= self.config.limits.transport.body_bytes
                || bounded_json_size(&result, self.result_limit).is_err()
            {
                observation = Observation::ResultCandidate {
                    checks_passed: false,
                    summary: scrub_known(
                        "Computed result exceeds configured result_bytes".into(),
                        self.output_limit.min(self.coding_limits.summary_bytes),
                        credentials,
                    ),
                    checkpoint: None,
                    validation: vec![],
                    output: None,
                };
            }
        }
        if matches!(observation, Observation::ExecutorOutcome { .. }) {
            self.executor_seen.store(true, Ordering::Relaxed);
        }
        let report = Report {
            report_id,
            worker_id: self.config.id.clone(),
            incarnation: self.config.incarnation.clone(),
            task_id: self.attempt.task_id.clone(),
            attempt_id: self.attempt.id.clone(),
            generation: self.attempt.generation,
            observation,
        };
        let path = self
            .outbox
            .join(format!("{}.json", component(&report.report_id)));
        let refused = path.with_extension("refused");
        for retained in [&path, &refused] {
            if retained.exists() {
                let prior: Report =
                    serde_json::from_slice(&std::fs::read(retained).map_err(|e| e.to_string())?)
                        .map_err(|_| "corrupt retained report")?;
                if prior != report {
                    return Err("stable report identity has changed body".into());
                }
            }
        }
        if refused.exists() {
            return Ok(false);
        }
        bounded_json_size(&report, self.config.limits.transport.body_bytes).map_err(|reason| format!("{reason} (transport.body_bytes)"))?;
        atomic_json(&path, &report)?;
        if matches!(report.observation, Observation::ExecutionUncertain { .. }) {
            self.uncertainty_retained.store(true, Ordering::Relaxed);
        }
        let _lock = self.outbox_lock.lock().await;
        flush_outbox_with_credentials(&self.client, &self.outbox, credentials).await?;
        let acknowledged = !path.exists() && !path.with_extension("refused").exists();
        // Transport loss leaves an idempotent local outbox record for the worker connection to replay.
        Ok(acknowledged)
    }
}

async fn native(
    config: &WorkerConfig,
    input: &TaskInput,
    control: &Control,
    reporter: &Reporter,
    continuation: Option<&ChildOutcome>,
    state: &Path,
    owner_lock: Arc<std::fs::File>,
) -> Result<AttemptExit, String> {
    let inference = input.definition.required_inference()?;
    input
        .definition
        .execution_profile
        .as_ref()
        .ok_or("template_profile_unbound")?;
    if crate::execution_profile::collaboration(input) {
        return native_loop::execute(
            config,
            input,
            control,
            reporter,
            continuation,
            state,
            owner_lock,
        )
        .await;
    }
    if crate::task_tool::enabled(input) {
        return native_tool::execute(
            config,
            input,
            control,
            reporter,
            continuation,
            state,
            owner_lock,
        )
        .await;
    }
    control.check()?;
    track_native_launch(state, &reporter.attempt, &control.runtime_limits)?;
    if !reporter
        .send_acknowledged(Observation::Usage {
            model_requests: 1,
            input_tokens: None,
            output_tokens: None,
        })
        .await?
    {
        return Err("native request reservation unacknowledged; no provider request sent".into());
    }
    let current = controller_call(
        &reporter.client,
        Operation::GetTask {
            task_id: reporter.attempt.task_id.clone(),
        },
        control,
    )
    .await?;
    if !matches!(current,Data::Task(ref task) if task.status==TaskStatus::Active&&task.session_id==reporter.attempt.session_id&&task.assigned_agent_id==input.definition.id&&task.input.definition==input.definition&&task.attempt.as_ref().is_some_and(|a|a.id==reporter.attempt.id&&a.session_id==reporter.attempt.session_id&&a.generation==reporter.attempt.generation&&!a.stopped&&!a.uncertain&&a.phase!=AttemptPhase::StopRequested))
    {
        return Err(
            "native reservation did not leave a runnable current attempt; no provider request sent"
                .into(),
        );
    }
    let native_config = crate::native::NativeConfig {
        endpoint: config.native_endpoint.clone(),
        api_key_env: "OPENAI_API_KEY".into(),
        model: inference.model.clone(),
    };
    let mut stopped = control.clone();
    // Native validation owns subprocess cancellation and awaits kill/reap.
    // Dropping its future here could turn an unverified cleanup into a false stop.
    // Request reservation remains consumed even when cancellation wins.
    let host = crate::native::ValidationHost::with_owner_lock(
        config.validation_executable.clone(),
        native_registry(state, &reporter.attempt),
        owner_lock,
        Arc::new(input.execution_limits()?.native.clone()),
        control.validation_runtime.clone(),
    )
    .with_request_attempt(&reporter.attempt);
    let observation = crate::native::execute_step_observed_with_host(
        input,
        &native_config,
        continuation,
        &host,
        stopped.stopped(),
    )
    .await;
    let step = report_native_step_observation(observation, reporter).await?;
    let result = match step {
        crate::native::NativeStep::Complete(result) => result,
        crate::native::NativeStep::DelegateCoding { instruction } => {
            if continuation.is_some() {
                return Err("native continuation cannot delegate again".into());
            }
            return handoff::begin(state, input, reporter, &instruction).await;
        }
    };
    reporter
        .send(Observation::ExecutorOutcome {
            success: true,
            summary: scrub(
                result.summary.clone(),
                reporter.coding_limits.summary_bytes,
                &config.limits,
            ),
        })
        .await?;
    reporter.send(Observation::ProcessTreeStopped).await?;
    reporter
        .send(Observation::ResultCandidate {
            checks_passed: true,
            summary: scrub(
                result.summary,
                reporter.coding_limits.summary_bytes,
                &config.limits,
            ),
            checkpoint: None,
            validation: vec!["native structured output schema: pass".into()],
            output: Some(result.output),
        })
        .await?;
    Ok(AttemptExit::Finished)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttemptExit {
    Finished,
    DelegationPending,
}

// Only coding's actual publish boundary can construct TrackedCheckpoint,
// after the executor completed and its original checkpoint intent committed.
// It remains an error; its uncertainty belongs to that durable external effect.
#[derive(Debug)]
enum AttemptFailure {
    Execution(String),
    TrackedCheckpoint(String),
    CollaborationUncertain(String),
}
impl AttemptFailure {
    fn message(&self) -> &str {
        match self {
            Self::Execution(value)
            | Self::TrackedCheckpoint(value)
            | Self::CollaborationUncertain(value) => value,
        }
    }
    fn execution_uncertain(&self, provider_uncertain: bool) -> bool {
        provider_uncertain
            || matches!(self, Self::CollaborationUncertain(_))
            || self.message().contains("unverified")
            || matches!(self,Self::Execution(value) if value.contains("uncertain"))
    }
}
impl From<codex::ExecutionFailure> for AttemptFailure {
    fn from(value: codex::ExecutionFailure) -> Self {
        if value.collaboration_uncertain {
            Self::CollaborationUncertain(value.message)
        } else {
            Self::Execution(value.message)
        }
    }
}
impl From<String> for AttemptFailure {
    fn from(value: String) -> Self {
        Self::Execution(value)
    }
}
impl From<&str> for AttemptFailure {
    fn from(value: &str) -> Self {
        Self::Execution(value.into())
    }
}
fn receipt_completed(result: &Result<AttemptExit, AttemptFailure>, cleanup_verified: bool) -> bool {
    cleanup_verified
        && !matches!(result, Ok(AttemptExit::DelegationPending))
        && !matches!(result, Err(AttemptFailure::CollaborationUncertain(_)))
        && !matches!(result,Err(error) if error.message().contains("unverified"))
}

async fn report_attempt_failure(
    reporter: &Reporter,
    error: String,
    cleanup_verified: bool,
    uncertain: bool,
) -> bool {
    if uncertain || !cleanup_verified {
        let _ = reporter
            .send(Observation::ExecutionUncertain {
                message: error.clone(),
            })
            .await;
        if !reporter.uncertainty_retained.load(Ordering::Relaxed) {
            // A failed outbox write is not durable uncertainty. Preserve the
            // incomplete launch and withhold terminal reports; restart can
            // reconcile the original attempt without losing provider exposure.
            return false;
        }
    }
    if !reporter.executor_seen.load(Ordering::Relaxed) {
        let _ = reporter
            .send(Observation::ExecutorOutcome {
                success: false,
                summary: error.clone(),
            })
            .await;
    }
    if cleanup_verified {
        let _ = reporter.send(Observation::ProcessTreeStopped).await;
    }
    let _ = reporter
        .send(Observation::ResultCandidate {
            checks_passed: false,
            summary: error,
            checkpoint: None,
            validation: vec![],
            output: None,
        })
        .await;
    true
}

async fn report_native_step_observation(
    observation: crate::native::NativeStepObservation,
    reporter: &Reporter,
) -> Result<crate::native::NativeStep, String> {
    report_native_result(
        observation.result,
        observation.usage,
        observation.provider_outcome,
        reporter,
    )
    .await
}

#[cfg(test)]
async fn report_native_observation(
    observation: crate::native::NativeObservation,
    reporter: &Reporter,
) -> Result<crate::native::NativeResult, String> {
    report_native_result(
        observation.result,
        observation.usage,
        observation.provider_outcome,
        reporter,
    )
    .await
}

// Traceability scope: Preserves typed ProviderOutcome::Unknown independently of error text and local cleanup.
//= spec/impl/v2/implementation.md#interrupted-work-is-reconciled
//# An external effect with an uncertain outcome MUST be reconciled before the platform retries that effect.
async fn report_native_result<T>(
    result: Result<T, String>,
    usage: crate::native::UsageObservation,
    provider_outcome: crate::native::ProviderOutcome,
    reporter: &Reporter,
) -> Result<T, String> {
    // Provider completion and local helper termination are independent. Retain
    // the typed disposition before any report await; failure handling below
    // records ExecutionUncertain even when the error text says only "unknown".
    if provider_outcome == crate::native::ProviderOutcome::Unknown {
        reporter.provider_uncertain.store(true, Ordering::Relaxed);
    }
    if let crate::native::UsageObservation::Reported(usage) = usage {
        if let Err(report_error) = reporter
            .send(Observation::Usage {
                model_requests: 1,
                input_tokens: Some(usage.input_tokens),
                output_tokens: Some(usage.output_tokens),
            })
            .await
        {
            return Err(match result {
                Err(error) => format!("{error}; native usage recording failed: {report_error}"),
                Ok(_) => format!("native usage recording failed: {report_error}"),
            });
        }
    }
    // Unknown/Invalid usage neither fabricates counts nor refunds reservation.
    if reporter.provider_uncertain.load(Ordering::Relaxed) && result.is_ok() {
        Err("native provider outcome remains unknown".into())
    } else {
        result
    }
}

fn configured_credentials(limits: &RuntimeLimits) -> Result<Vec<String>, String> {
    configured_credentials_from(limits, |name| match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(()),
    })
}

// Fixed named inputs only. Decode recognized URL userinfo once; do not guess
// encodings, inspect unrelated environment variables or change child routing.
fn configured_credentials_from(
    limits: &RuntimeLimits,
    mut value: impl FnMut(&str) -> Result<Option<String>, ()>,
) -> Result<Vec<String>, String> {
    fn decoded(value: &str) -> Option<String> {
        let mut bytes = Vec::with_capacity(value.len());
        let mut input = value.bytes();
        while let Some(byte) = input.next() {
            if byte == b'%' {
                let a = char::from(input.next()?).to_digit(16)?;
                let b = char::from(input.next()?).to_digit(16)?;
                bytes.push((a * 16 + b) as u8);
            } else {
                bytes.push(byte);
            } // '+' is literal in URL userinfo.
        }
        String::from_utf8(bytes).ok()
    }
    let mut credentials = Vec::new();
    let mut remaining = limits.transport.body_bytes;
    let mut add = |value: String| -> Result<(), String> {
        if value.is_empty() {
            return Ok(());
        }
        remaining = remaining
            .checked_sub(value.len())
            .ok_or("configured credential inspection exceeds bound (worker.credential_scan_units)")?;
        credentials.push(value);
        Ok(())
    };
    for name in [
        "OPENAI_API_KEY",
        "CODEX_API_KEY",
        "GITHUB_TOKEN",
        "GH_TOKEN",
    ] {
        if let Some(secret) =
            value(name).map_err(|_| "configured credential is not valid Unicode")?
        {
            add(secret)?;
        }
    }
    for name in [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ] {
        let Some(raw) = value(name).map_err(|_| "configured proxy is not valid Unicode")? else {
            continue;
        };
        if raw.len() > limits.transport.body_bytes {
            return Err("configured proxy inspection exceeds bound (transport.body_bytes)".into());
        }
        let parsed = reqwest::Url::parse(&raw)
            .ok()
            .filter(|url| url.host_str().is_some())
            .or_else(|| {
                (!raw.contains("://"))
                    .then(|| reqwest::Url::parse(&format!("http://{raw}")))
                    .and_then(Result::ok)
            });
        let Some(url) = parsed.filter(|url| !url.username().is_empty() || url.password().is_some())
        else {
            // Retain the exact suspicious configured form even when URL parsing
            // cannot establish components; no malformed-input transformation is claimed.
            if raw.contains('@') {
                add(raw)?;
            }
            continue;
        };
        // Preserve recognizable raw spellings as well as the parser's canonical
        // userinfo. URL parsing can percent-encode Unicode or extra ':' bytes.
        let authority = raw
            .split_once("://")
            .map_or(raw.as_str(), |(_, rest)| rest)
            .split(['/', '?', '#'])
            .next()
            .unwrap_or_default();
        if let Some((userinfo, _)) = authority.rsplit_once('@') {
            add(userinfo.to_owned())?;
            if let Some(decoded) = decoded(userinfo) {
                add(decoded)?;
            }
            if let Some((_, password)) = userinfo.split_once(':') {
                add(password.to_owned())?;
                if let Some(decoded) = decoded(password) {
                    add(decoded)?;
                }
            }
        }
        add(raw)?;
        add(url.as_str().to_owned())?;
        let userinfo = match url.password() {
            Some(password) => format!("{}:{password}", url.username()),
            None => url.username().to_owned(),
        };
        add(userinfo.clone())?;
        if let Some(decoded) = decoded(&userinfo) {
            add(decoded)?;
        }
        if let Some(password) = url.password() {
            add(password.to_owned())?;
            if let Some(decoded) = decoded(password) {
                add(decoded)?;
            }
            if password.is_empty() {
                add(url.username().to_owned())?;
                if let Some(decoded) = decoded(url.username()) {
                    add(decoded)?;
                }
            }
        }
    }
    credentials.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
    credentials.dedup();
    Ok(credentials)
}

fn contains_credential(text: &str, credentials: &[String]) -> bool {
    credentials
        .iter()
        .any(|secret| !secret.is_empty() && text.contains(secret))
}

fn scrub(text: String, limit: usize, limits: &RuntimeLimits) -> String {
    match configured_credentials(limits) {
        Ok(credentials) => scrub_known(text, limit, &credentials),
        Err(_) => String::new(), // Do not expose unchecked diagnostics on guard failure.
    }
}

fn scrub_characters(
    text: String,
    character_limit: usize,
    byte_limit: usize,
    limits: &RuntimeLimits,
) -> String {
    match configured_credentials(limits) {
        Ok(credentials) => scrub_known_characters(text, character_limit, byte_limit, &credentials),
        Err(_) => String::new(),
    }
}

fn scrub_known_characters(
    text: String,
    character_limit: usize,
    byte_limit: usize,
    credentials: &[String],
) -> String {
    // Redact before either cut so truncation cannot hide a configured value from
    // the scanner. The accepted controller byte bound and character policy are
    // separate; a character count must not become an implicit UTF-8 byte cap.
    let mut output = scrub_known(text, byte_limit, credentials);
    if let Some((end, _)) = output.char_indices().nth(character_limit) {
        output.truncate(end);
    }
    output
}

// Exact configured-value hygiene only. This cannot identify unknown/transformed
// credentials or establish non-disclosure by opaque executor storage and tools.
fn scrub_known(text: String, limit: usize, credentials: &[String]) -> String {
    fn append(output: &mut String, value: &str, limit: usize) -> bool {
        let mut end = value.len().min(limit.saturating_sub(output.len()));
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        output.push_str(&value[..end]);
        end == value.len()
    }
    let marker = if contains_credential("[credential redacted]", credentials) {
        ""
    } else {
        "[credential redacted]"
    };
    let mut output = String::with_capacity(text.len().min(limit));
    let mut remaining = text.as_str();
    while !remaining.is_empty() && output.len() < limit {
        let found = credentials
            .iter()
            .filter(|secret| !secret.is_empty())
            .filter_map(|secret| remaining.find(secret).map(|at| (at, secret.len())))
            .min_by_key(|(at, length)| (*at, std::cmp::Reverse(*length)));
        let Some((at, length)) = found else {
            append(&mut output, remaining, limit);
            break;
        };
        if !append(&mut output, &remaining[..at], limit) {
            break;
        }
        if !append(&mut output, marker, limit) {
            break;
        }
        remaining = &remaining[at + length..];
    }
    // Replacement boundaries can themselves form another configured value.
    if contains_credential(&output, credentials) {
        output.clear();
    }
    output
}

fn checkpoint_contains_credential(checkpoint: &Checkpoint, credentials: &[String]) -> bool {
    [
        &checkpoint.repository,
        &checkpoint.commit,
        &checkpoint.remote_ref,
        &checkpoint.operation_id,
    ]
    .into_iter()
    .any(|identity| contains_credential(identity, credentials))
}

fn value_contains_credential(
    value: &serde_json::Value,
    credentials: &[String],
    limits: &WorkerLimits,
) -> Result<bool, &'static str> {
    // Inspect without recursive calls or serialization. The admitted native
    // output has its own retained schema bounds; this guard has explicit metadata while
    // refusing pathological internal Values before later result serialization.
    let mut remaining = limits.credential_scan_units;
    let mut pending = vec![(value, 0usize)];
    while let Some((value, depth)) = pending.pop() {
        if depth > limits.credential_scan_depth {
            return Err("result inspection depth exceeded (worker.credential_scan_depth)");
        }
        remaining = remaining
            .checked_sub(1)
            .ok_or("result inspection bound exceeded (worker.credential_scan_units)")?;
        let scalar = match value {
            serde_json::Value::String(value) => Some(std::borrow::Cow::Borrowed(value.as_str())),
            serde_json::Value::Number(value) => Some(std::borrow::Cow::Owned(value.to_string())),
            serde_json::Value::Bool(value) => Some(std::borrow::Cow::Borrowed(if *value {
                "true"
            } else {
                "false"
            })),
            serde_json::Value::Null => Some(std::borrow::Cow::Borrowed("null")),
            serde_json::Value::Array(values) => {
                if values.len().saturating_add(pending.len()) > remaining {
                    return Err("result inspection bound exceeded (worker.credential_scan_units)");
                }
                pending.extend(values.iter().map(|value| (value, depth + 1)));
                None
            }
            serde_json::Value::Object(values) => {
                for (key, value) in values {
                    remaining = remaining
                        .checked_sub(key.len())
                        .ok_or("result inspection bound exceeded (worker.credential_scan_units)")?;
                    if contains_credential(key, credentials) {
                        return Ok(true);
                    }
                    if pending.len() >= remaining {
                        return Err("result inspection bound exceeded (worker.credential_scan_units)");
                    }
                    pending.push((value, depth + 1));
                }
                None
            }
        };
        if let Some(scalar) = scalar {
            remaining = remaining
                .checked_sub(scalar.len())
                .ok_or("result inspection bound exceeded (worker.credential_scan_units)")?;
            if contains_credential(&scalar, credentials) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn report_contains_credential(
    report: &Report,
    credentials: &[String],
    limits: &WorkerLimits,
) -> Result<bool, &'static str> {
    if [
        &report.report_id,
        &report.worker_id,
        &report.incarnation,
        &report.task_id,
        &report.attempt_id,
    ]
    .into_iter()
    .any(|value| contains_credential(value, credentials))
    {
        return Ok(true);
    }
    Ok(match &report.observation {
        Observation::NativeLoopCompleted { calls, .. } => calls.iter().any(|c| {
            contains_credential(&c.operation_id, credentials)
                || contains_credential(&c.logical_call_id, credentials)
        }),
        Observation::Started { thread_id, turn_id } => thread_id
            .iter()
            .chain(turn_id)
            .any(|value| contains_credential(value, credentials)),
        Observation::Progress { text, output, .. } => {
            contains_credential(text, credentials) || contains_credential(output, credentials)
        }
        Observation::ExecutorOutcome { summary, .. } => contains_credential(summary, credentials),
        Observation::ExecutionUncertain { message } => contains_credential(message, credentials),
        Observation::ResultCandidate {
            summary,
            checkpoint,
            validation,
            output,
            ..
        } => {
            contains_credential(summary, credentials)
                || validation
                    .iter()
                    .any(|value| contains_credential(value, credentials))
                || checkpoint
                    .as_ref()
                    .is_some_and(|value| checkpoint_contains_credential(value, credentials))
                || match output {
                    Some(value) => value_contains_credential(value, credentials, limits)?,
                    None => false,
                }
        }
        Observation::Usage { .. } | Observation::ProcessTreeStopped => false,
    })
}

async fn flush_outbox(client: &ApiClient, outbox: &Path) -> Result<(), String> {
    let credentials = configured_credentials(client.limits())?;
    flush_outbox_with_credentials(client, outbox, &credentials).await
}

fn deferred_information(observation: &Observation) -> bool {
    matches!(
        observation,
        Observation::Started { .. } | Observation::Progress { .. } | Observation::Usage { .. }
    )
}

fn read_report(path: &Path, limits: &RuntimeLimits) -> Result<Report, String> {
    use std::io::Read;
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|e| e.to_string())?
        .take((limits.transport.body_bytes + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() > limits.transport.body_bytes {
        return Err("oversized local report record".into());
    }
    serde_json::from_slice(&bytes).map_err(|_| "corrupt worker outbox".into())
}

fn pending_reports(outbox: &Path, limits: &RuntimeLimits) -> Result<bool, String> {
    for entry in std::fs::read_dir(outbox).map_err(|_| "worker outbox unavailable")? {
        let path = entry.map_err(|_| "worker outbox entry unavailable")?.path();
        if path.extension().and_then(|value| value.to_str()) == Some("json")
            && !deferred_information(&read_report(&path, limits)?.observation)
        {
            return Ok(true);
        }
    }
    acceptance::pending(outbox, limits)
}

async fn reports_ready_before_registration(
    client: &ApiClient,
    outbox: &Path,
    outbox_lock: &tokio::sync::Mutex<()>,
) -> Result<(), String> {
    // Reconnection can overlap a still-running attempt's reporter. Serialize
    // read/send/delete with it; release before any handoff invokes Reporter.
    let _lock = outbox_lock.lock().await;
    flush_outbox(client, outbox).await?;
    if pending_reports(outbox, client.limits())? {
        return Err("prior reports or completed acceptance await reconciliation".into());
    }
    Ok(())
}

async fn flush_outbox_with_credentials(
    client: &ApiClient,
    outbox: &Path,
    credentials: &[String],
) -> Result<(), String> {
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(outbox).map_err(|e| e.to_string())? {
        let path = entry.map_err(|e| e.to_string())?.path();
        if path.extension().and_then(|v| v.to_str()) != Some("json") {
            continue;
        }
        if entries.len() >= client.limits().worker.outbox_records {
            return Err("outbox inventory exceeds bound (worker.outbox_records)".into());
        }
        let report = read_report(&path, client.limits())?;
        if report_contains_credential(&report, credentials, &client.limits().worker)
            .map_err(|reason| format!("local report exceeds bounded credential inspection; replay withheld: {reason}"))?
        {
            return Err("local report contains a configured credential; replay withheld".into());
        }
        // Initial start and the native request reservation are separately ACK
        // gated before effects. Deferred informational receipts cannot consume
        // the cleanup dispatch budget or block a verified process stop.
        let priority = if matches!(report.observation, Observation::ExecutionUncertain { .. }) {
            0
        } else if deferred_information(&report.observation) {
            2
        } else {
            1
        };
        entries.push((priority, path));
    }
    entries.sort();
    let mut blocked = BTreeSet::new();
    let mut attempts = 0;
    for (_, path) in entries {
        let report = read_report(&path, client.limits())?;
        if blocked.contains(&report.attempt_id) {
            continue;
        }
        if attempts == client.limits().worker.outbox_batch {
            break;
        }
        attempts += 1;
        if report_contains_credential(&report, credentials, &client.limits().worker)
            .map_err(|reason| format!("local report exceeds bounded credential inspection; replay withheld: {reason}"))?
        {
            // Never sanitize an old body under its existing operation identity.
            // Retain original bytes and refuse before HTTP, rename or deletion.
            return Err("local report contains a configured credential; replay withheld".into());
        }
        match tokio::time::timeout(
            Duration::from_millis(client.limits().worker.outbox_call_timeout_ms),
            client.call(Operation::ObserveWorker {
                report: report.clone(),
            }),
        )
        .await
        {
            Ok(Ok(Reply {
                outcome: Outcome::Ok { .. },
                ..
            })) => {
                std::fs::remove_file(path).map_err(|e| e.to_string())?;
            }
            Ok(Ok(Reply {
                outcome: Outcome::Refused { .. },
                ..
            })) => {
                let intended = Operation::ObserveWorker {
                    report: report.clone(),
                };
                match acceptance::lookup(client, &report.report_id, &intended).await? {
                    acceptance::Lookup::Recorded(Outcome::Ok { .. }, _) => {
                        std::fs::remove_file(path).map_err(|e| e.to_string())?;
                    }
                    acceptance::Lookup::Recorded(
                        ref refusal @ Outcome::Refused { .. },
                        ref context,
                    ) => {
                        atomic_json(
                            &path.with_extension("outcome"),
                            &json!({"outcome": refusal, "context": context}),
                        )?;
                        if matches!(report.observation, Observation::ExecutionUncertain { .. }) {
                            // Do not let a later stop/result overtake an unknown
                            // provider outcome whose recording was refused.
                            blocked.insert(report.attempt_id.clone());
                            continue;
                        }
                        if matches!(report.observation, Observation::ResultCandidate { .. }) {
                            acceptance::retain(outbox, &report, refusal, context, client.limits())?;
                        }
                        std::fs::rename(&path, path.with_extension("refused"))
                            .map_err(|e| e.to_string())?;
                    }
                    // Deferred information stays exact and pending. All other
                    // reports preserve their same-attempt dependency order.
                    _ => {
                        if !deferred_information(&report.observation) {
                            blocked.insert(report.attempt_id.clone());
                        }
                    }
                }
            }
            _ => {
                if !deferred_information(&report.observation) {
                    blocked.insert(report.attempt_id.clone());
                }
            }
        }
    }
    if !blocked.is_empty() || pending_uncertainty(outbox, client.limits())? {
        return Ok(());
    }
    acceptance::flush(client, outbox, credentials).await
}

fn pending_uncertainty(outbox: &Path, limits: &RuntimeLimits) -> Result<bool, String> {
    for entry in std::fs::read_dir(outbox).map_err(|e| e.to_string())? {
        let path = entry.map_err(|e| e.to_string())?.path();
        if path.extension().and_then(|v| v.to_str()) == Some("json")
            && matches!(
                read_report(&path, limits)?.observation,
                Observation::ExecutionUncertain { .. }
            )
        {
            return Ok(true);
        }
    }
    Ok(false)
}

//= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
//# A worker MUST access task state through the controller's application interface.
async fn controller_call(
    client: &ApiClient,
    operation: Operation,
    control: &Control,
) -> Result<Data, String> {
    control.check()?;
    let reply = tokio::time::timeout(
        Duration::from_millis(client.limits().worker.controller_call_timeout_ms),
        client.call(operation),
    )
    .await
    .map_err(|_| "controller operation response timed out; outcome uncertain")??;
    match reply.outcome {
        Outcome::Ok { data } => Ok(data),
        Outcome::Refused { code, .. } => Err(format!("controller refused operation: {code}")),
        Outcome::Pending { .. } | Outcome::Uncertain { .. } => {
            Err("controller operation remains uncertain".into())
        }
    }
}

// Debug-only deterministic process-loss cut. It never runs in release builds,
// never changes Git hooks, and does not simulate publication or settlement.
#[cfg(debug_assertions)]
async fn checkpoint_qualification_cut(
    state: &Path,
    position: &str,
    attempt: &Attempt,
    checkpoint: &Checkpoint,
    control: &Control,
) -> Result<(), String> {
    if std::env::var("BORG_QUALIFICATION_CHECKPOINT_CUT")
        .ok()
        .as_deref()
        != Some(position)
    {
        return Ok(());
    }
    atomic_json(
        &state.join("checkpoint-qualification.json"),
        &json!({"position":position,"attempt":attempt,"checkpoint":checkpoint}),
    )?;
    let mut stopped = control.clone();
    tokio::select! {
        _=stopped.stopped()=>{},
        _=tokio::time::sleep(Duration::from_secs(30))=>{},
    }
    Err("debug checkpoint crash cut ended without process loss".into())
}

fn coding_prompt(
    input: &TaskInput,
    starting_commit: &str,
    credentials: &[String],
) -> Result<String, String> {
    // These arguments previously reached only worker-owned command execution.
    // Refuse known credential values before exposing this data to the executor;
    // do not redact or rewrite the immutable accepted command vectors.
    if input
        .acceptance_commands
        .iter()
        .flatten()
        .any(|argument| contains_credential(argument, credentials))
    {
        return Err("acceptance command contains configured credential".into());
    }
    let commands = serde_json::to_string(&input.acceptance_commands)
        .map_err(|_| "acceptance commands could not be encoded")?;
    if contains_credential(&commands, credentials) {
        return Err("acceptance command contains configured credential".into());
    }
    Ok(format!(
        "{}\n\nWork only in this task checkout. Do not push, publish, open pull requests, deploy, or change credentials. The worker runs the declared acceptance commands and publishes the source checkpoint after your turn. Do not commit credentials, ignored caches, or unrelated files. Starting source checkpoint: {}.\n\nYou may run the declared acceptance commands in this checkout while iterating. Each JSON array below is an executable followed by its exact argument strings; it is data, not a shell command string. Preserve argument boundaries and command order, and use the executable paths as supplied rather than assuming tools are on PATH. These commands do not authorize publication or credential changes. The worker independently reruns them from the acknowledged source checkpoint to decide acceptance.\nDeclared acceptance commands (JSON argv vectors):\n{}",
        input.instruction, starting_commit, commands
    ))
}

// Traceability scope: Workspace path and every owned Git/check invocation derive from the current RunAttempt task/attempt; later attempts get distinct directories.
//= spec/impl/v2/implementation.md#work-starts-from-a-durable-source
//# A workspace mutation MUST be assigned to one current task owner.
// Traceability scope: Supports per-workspace owned build behavior, not four-host representative builds, real Codex concurrency or resource enforcement.
//= spec/impl/v2/implementation.md#initial-four-host-qualification
//# Every coding workspace MUST be able to run repository builds and tests.
// Traceability scope: Historical17.84s execution is not current real-provider/browser journey proof.
//= spec/impl/v2/implementation.md#observable-end-to-end-result
//# The initial system MUST execute a coding task in a workspace through the supplied executor.
// Traceability scope: Positive browser root-result navigation is a separate test gap; validation strings are not protected-check proof. The load oracle asserts exact selected checkpoint and three returned check outcomes; it does not provide protected checker provenance.
//= spec/impl/v2/implementation.md#observable-end-to-end-result
//# The initial system MUST return references to the source changes and validation results produced by a successful task.
async fn coding(
    config: &WorkerConfig,
    attempt: &Attempt,
    input: &TaskInput,
    saved: Option<&Checkpoint>,
    state: &Path,
    control: &Control,
    reporter: &Reporter,
    command: &WorkerCommand,
) -> Result<(), AttemptFailure> {
    let inference = input.definition.required_inference()?.clone();
    let execution_limits = input.execution_limits()?.clone();
    input
        .definition
        .execution_profile
        .as_ref()
        .ok_or("template_profile_unbound")?;
    crate::execution_profile::validate(input)?;
    let repository = input
        .repository
        .as_ref()
        .ok_or("coding task has no repository")?;
    let prompt = coding_prompt(
        input,
        saved
            .map(|c| c.commit.as_str())
            .unwrap_or(&repository.start_commit),
        &configured_credentials(&control.runtime_limits)?,
    )?;
    let registry = state.join("processes").join(component(&attempt.id));
    let workspace = git::checkout_cached(
        &config.workspace_root.join("repositories"),
        &config.workspace_root.join("workspaces"),
        &attempt.id,
        repository,
        saved,
        &registry,
        control,
    )
    .await?;
    let codex_state = codex::state_path(&state.join("codex"), &attempt.id);
    let (events, mut received) = mpsc::channel(execution_limits.coding.progress_queue_items);
    let mut execution = tokio::task::JoinSet::new();
    let collaboration =
        crate::execution_profile::collaboration(input).then(|| codex::Collaboration {
            controller_url: config.controller_url.clone(),
            caller: collaboration_caller(attempt),
            memory: Some(codex::MemoryScope {
                agent_id: input.definition.id.clone(),
                session_id: attempt.session_id.clone(),
            }),
            bridge_binary: config.validation_executable.clone(),
        });
    let (binary, work, state_dir, processes, execution_control) = (
        config.codex_path.clone(),
        workspace.clone(),
        codex_state.clone(),
        registry.clone(),
        control.clone(),
    );
    let coding_limits = execution_limits.coding.clone();
    execution.spawn(async move {
        codex::execute_with_collaboration(
            &binary,
            &work,
            &state_dir,
            &processes,
            &inference,
            &execution_limits,
            &prompt,
            &execution_control,
            events,
            collaboration.as_ref(),
        )
        .await
    });
    let mut recent = String::new();
    let mut recent_truncated = false;
    let output_limit = input
        .limits
        .output_bytes
        .min(coding_limits.command_output_bytes);
    let outcome = loop {
        tokio::select! {
            result=execution.join_next()=>break result.ok_or("executor task disappeared")?.map_err(|_|"executor task failed; outcome uncertain")??,
            event=received.recv()=>match event {
                Some(codex::Event::Started{thread_id,turn_id})=>reporter.send(Observation::Started{thread_id:Some(thread_id),turn_id:Some(turn_id)}).await?,
                Some(codex::Event::Progress(text))=>{
                    recent_truncated|=recent.len().saturating_add(text.len())>output_limit;
                    recent.push_str(&text);recent=scrub(recent,output_limit,&config.limits);
                    reporter.send(Observation::Progress{text:"Codex is working".into(),output:recent.clone(),truncated:recent_truncated}).await?;
                },
                None=>{},
            }
        }
    };
    //= spec/impl/v2/implementation.md#the-supplied-executor-is-qualified-at-its-actual-boundary
    //# A worker MUST record the association between its task attempt and the supplied executor's thread and turn identifiers.
    // A terminal event can win the observer select before its Started event.
    // Persist the exact association before terminal reporting in either order.
    if !reporter
        .send_identified(
            format!("{}-completed-association", attempt.id),
            Observation::Started {
                thread_id: Some(outcome.thread_id.clone()),
                turn_id: Some(outcome.turn_id.clone()),
            },
        )
        .await?
    {
        return Err("completed executor association awaits acknowledgment".into());
    }
    //= spec/impl/v2/implementation.md#the-supplied-executor-owns-its-inner-loop
    //# The adapter MUST record the supplied executor's reported outcome separately from the platform's task acceptance result.
    if !reporter
        .send_identified(
            format!("{}-completed-executor", attempt.id),
            Observation::ExecutorOutcome {
                success: outcome.success,
                summary: scrub(
                    outcome.summary.clone(),
                    reporter.coding_limits.summary_bytes,
                    &config.limits,
                ),
            },
        )
        .await?
    {
        return Err("completed executor outcome awaits acknowledgment".into());
    }
    if !outcome.success {
        reporter.send(Observation::ProcessTreeStopped).await?;
        reporter
            .send(Observation::ResultCandidate {
                checks_passed: false,
                summary: scrub(
                    outcome.summary,
                    reporter.coding_limits.summary_bytes,
                    &config.limits,
                ),
                checkpoint: None,
                validation: vec![],
                output: Some(json!({"observed_usage":outcome.observed_usage})),
            })
            .await?;
        return Ok(());
    }
    let commit = git::commit_source(&workspace, &attempt.id, &registry, control).await?;
    let checkpoint = git::checkpoint(repository, &attempt.task_id, &attempt.id, commit)?;
    //= spec/impl/v2/implementation.md#checkpoint-publication-is-reconciled
    //# A checkpoint publication MUST have a durable operation record before it is sent.
    //= spec/impl/v2/implementation.md#checkpoint-publication-is-reconciled
    //# The publication record MUST identify the repository, exact commit and intended remote reference.
    let mut publication =
        publication::prepare(state, command, reporter, &checkpoint, control).await?;
    //= spec/impl/v2/implementation.md#work-starts-from-a-durable-source
    //# A checkpoint MUST NOT be reported durable before its external storage acknowledges it.
    #[cfg(debug_assertions)]
    checkpoint_qualification_cut(state, "before_publish", attempt, &checkpoint, control).await?;
    publication.mark_started(state, attempt)?;
    git::publish(&workspace, &checkpoint, &registry, control)
        .await
        .map_err(AttemptFailure::TrackedCheckpoint)?;
    #[cfg(debug_assertions)]
    checkpoint_qualification_cut(state, "after_publish", attempt, &checkpoint, control).await?;
    let settlement_id = publication.settlement_id.clone();
    mutations::settle(
        &reporter.client,
        state,
        Operation::SettleOperation {
            settlement_id,
            operation_id: checkpoint.operation_id.clone(),
            task_id: attempt.task_id.clone(),
            attempt_id: attempt.id.clone(),
            generation: attempt.generation,
            outcome: json!({"acknowledged":true}),
            checkpoint: Some(checkpoint.clone()),
        },
        control,
    )
    .await?;
    publication.retire(state, attempt)?;
    // Traceability scope: Fresh validation checkout identifies source; worker validation strings do not themselves identify the executed binary.
    // Execute checks from a fresh acknowledged source checkout. Dirty/ignored
    // inputs and prior local build products cannot make an unpublished change pass.
    let validation_workspace = git::checkout_cached(
        &config.workspace_root.join("repositories"),
        &config.workspace_root.join("validation"),
        &attempt.id,
        repository,
        Some(&checkpoint),
        &registry,
        control,
    )
    .await?;
    let mut validation = Vec::new();
    let mut passed = true;
    for (number, args) in input.acceptance_commands.iter().enumerate() {
        if args.is_empty() {
            return Err("empty acceptance command".into());
        }
        reporter
            .send(Observation::Progress {
                text: format!("Running acceptance command {}", number + 1),
                output: recent.clone(),
                truncated: recent_truncated,
            })
            .await?;
        let result = process::command(
            Path::new(&args[0]),
            &args[1..],
            &validation_workspace,
            &registry,
            control,
            output_limit,
        )
        .await?;
        let unchanged = git::source_unchanged(
            &validation_workspace,
            &checkpoint.commit,
            &registry,
            control,
        )
        .await?;
        passed &= result.success && unchanged;
        validation.push(format!(
            "check {} at {}: {}",
            number + 1,
            checkpoint.commit,
            if result.success && unchanged {
                "pass"
            } else {
                "fail"
            }
        ));
        reporter
            .send(Observation::Progress {
                text: validation.last().cloned().unwrap_or_default(),
                output: scrub(result.text, output_limit, &config.limits),
                truncated: result.truncated,
            })
            .await?;
        if !unchanged {
            return Err("acceptance command changed the checked source revision".into());
        }
    }
    reporter.send(Observation::ProcessTreeStopped).await?;
    //= spec/impl/v2/implementation.md#observable-end-to-end-result
    //# A task MUST NOT be reported successful solely because its executor exited successfully.
    reporter
        .send(Observation::ResultCandidate {
            checks_passed: passed,
            summary: scrub(
                outcome.summary,
                reporter.coding_limits.summary_bytes,
                &config.limits,
            ),
            checkpoint: Some(checkpoint),
            validation,
            output: Some(json!({"observed_usage":outcome.observed_usage})),
        })
        .await?;
    Ok(())
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct NativeTracking {
    format_revision: u32,
    session_id: String,
    task_id: String,
    attempt_id: String,
    generation: u64,
    incarnation: String,
    registry: PathBuf,
    context: process::ProcessContext,
}
fn native_registry(state: &Path, attempt: &Attempt) -> PathBuf {
    state.join("processes").join(component(&attempt.id))
}
fn native_tracking_path(state: &Path, attempt: &Attempt) -> PathBuf {
    state
        .join("native-tracking")
        .join(format!("{}.json", component(&attempt.id)))
}
fn expected_native_tracking(state: &Path, attempt: &Attempt) -> Result<NativeTracking, String> {
    Ok(NativeTracking {
        format_revision: 2,
        session_id: attempt.session_id.clone(),
        task_id: attempt.task_id.clone(),
        attempt_id: attempt.id.clone(),
        generation: attempt.generation,
        incarnation: attempt.worker_incarnation.clone(),
        registry: native_registry(state, attempt),
        context: process::ProcessContext::current()?,
    })
}
fn read_native_tracking(path: &Path, limits: &RuntimeLimits) -> Result<NativeTracking, String> {
    use std::io::Read;
    let file = std::fs::File::open(path).map_err(|_| "native tracking marker unavailable")?;
    let mut bytes = Vec::new();
    file.take((limits.worker.native_tracking_bytes + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| "native tracking marker unreadable")?;
    if bytes.len() > limits.worker.native_tracking_bytes {
        return Err("native tracking marker exceeds bound (worker.native_tracking_bytes)".into());
    }
    serde_json::from_slice(&bytes).map_err(|_| "invalid native tracking marker".into())
}
fn track_native_launch(
    state: &Path,
    attempt: &Attempt,
    limits: &RuntimeLimits,
) -> Result<(), String> {
    let expected = expected_native_tracking(state, attempt)?;
    let path = native_tracking_path(state, attempt);
    if path.exists() {
        if read_native_tracking(&path, limits)? != expected {
            return Err("native tracking identity differs; refusing launch".into());
        }
        return Ok(());
    }
    std::fs::create_dir_all(state.join("native-tracking")).map_err(|e| e.to_string())?;
    std::fs::create_dir_all(native_registry(state, attempt)).map_err(|e| e.to_string())?;
    // Written only in the new-attempt execution path, before any helper or
    // provider request. Startup never manufactures coverage for old receipts.
    bounded_json_size(&expected, limits.worker.native_tracking_bytes).map_err(|reason| format!("{reason} (worker.native_tracking_bytes)"))?;
    atomic_json(&path, &expected)
}
async fn native_registry_reconciled(
    state: &Path,
    attempt: &Attempt,
    limits: &RuntimeLimits,
) -> Result<bool, String> {
    let path = native_tracking_path(state, attempt);
    if !path.exists() {
        return Ok(false);
    }
    let tracking = read_native_tracking(&path, limits)?;
    if tracking != expected_native_tracking(state, attempt)? {
        return Err("native tracking identity/context mismatch; cleanup unverified".into());
    }
    process::reconcile_processes(&tracking.registry, &limits.worker).await?;
    Ok(true)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    command: WorkerCommand,
    completed: bool,
    effects_started: bool,
}
fn validate_command_session(command: &WorkerCommand) -> Result<(), String> {
    if let WorkerCommand::RunAttempt {
        session,
        attempt,
        task,
        ..
    } = command
    {
        session.validate_for(attempt, task)?;
    }
    Ok(())
}
fn read_receipt(path: &Path, limits: &RuntimeLimits) -> Result<Receipt, String> {
    use std::io::Read;
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|_| "command receipt unavailable")?
        .take((limits.worker.command_record_bytes + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| "command receipt unreadable")?;
    if bytes.len() > limits.worker.command_record_bytes {
        return Err("command receipt exceeds bound (worker.command_record_bytes)".into());
    }
    let receipt: Receipt = serde_json::from_slice(&bytes).map_err(|_| "invalid command receipt")?;
    validate_command_session(&receipt.command)?;
    Ok(receipt)
}

struct Active {
    task_id: String,
    generation: u64,
    executor: ExecutorKind,
    cancel: watch::Sender<bool>,
    join: tokio::task::JoinHandle<()>,
}

/// Returns true only for a newly admitted local command. The durable input is
/// compared on replay, so a transport retry cannot silently replace a task.
//= spec/impl/v2/implementation.md#worker-commands-do-not-create-duplicate-execution
//# A launch command MUST bind its attempt, generation, worker incarnation and payload identity.
pub fn record_launch(
    path: &Path,
    command: &WorkerCommand,
    limits: &RuntimeLimits,
) -> Result<bool, String> {
    validate_command_session(command)?;
    if path.exists() {
        let old = read_receipt(path, limits)?;
        if old.command != *command {
            return Err("duplicate command identity has conflicting payload".into());
        }
        return Ok(false);
    }
    let receipt = Receipt {
        command: command.clone(),
        completed: false,
        effects_started: false,
    };
    bounded_json_size(&receipt, limits.worker.command_record_bytes).map_err(|reason| format!("{reason} (worker.command_record_bytes)"))?;
    atomic_json(path, &receipt)?;
    Ok(true)
}

fn command_receipts(
    state: &Path,
    limits: &RuntimeLimits,
) -> Result<Vec<(PathBuf, Receipt)>, String> {
    use std::io::Read;
    let mut records = Vec::new();
    for entry in
        std::fs::read_dir(state.join("commands")).map_err(|_| "command inventory unavailable")?
    {
        let path = entry.map_err(|_| "command inventory unreadable")?.path();
        if path.extension().and_then(|v| v.to_str()) != Some("json") {
            continue;
        }
        if records.len() >= limits.worker.command_records {
            return Err("command inventory exceeds bound (worker.command_records)".into());
        }
        let mut bytes = Vec::new();
        std::fs::File::open(&path)
            .map_err(|_| "command receipt unavailable")?
            .take((limits.worker.command_record_bytes + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| "command receipt unreadable")?;
        if bytes.len() > limits.worker.command_record_bytes {
            return Err("command receipt exceeds bound (worker.command_record_bytes)".into());
        }
        let receipt: Receipt =
            serde_json::from_slice(&bytes).map_err(|_| "invalid command receipt")?;
        validate_command_session(&receipt.command)?;
        records.push((path, receipt));
    }
    records.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(records)
}

fn command_identity(command: &WorkerCommand) -> (&str, &str) {
    match command {
        WorkerCommand::RunAttempt {
            command_id,
            attempt,
            ..
        } => (command_id, &attempt.worker_incarnation),
        WorkerCommand::StopAttempt {
            command_id,
            incarnation,
            ..
        } => (command_id, incarnation),
    }
}

fn recovery_epoch(state: &Path, limits: &RuntimeLimits) -> Result<u64, String> {
    Ok(command_receipts(state, limits)?
        .iter()
        .filter(|(_, r)| r.completed)
        .count() as u64)
}

fn pending_acknowledgments(
    state: &Path,
    incarnation: &str,
    limits: &RuntimeLimits,
) -> Result<Vec<String>, String> {
    // One acknowledgment fits every positive controller reply bound. The
    // durable backlog is retained, including acknowledgments learned later.
    use std::io::Read;
    for (_, receipt) in command_receipts(state, limits)? {
        let (id, inc) = command_identity(&receipt.command);
        if inc != incarnation {
            continue;
        }
        match std::fs::File::open(state.join("acks").join(component(id))) {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take((limits.worker.command_record_bytes + 1) as u64)
                    .read_to_end(&mut bytes)
                    .map_err(|_| "acknowledgment marker unreadable")?;
                if bytes.len() > limits.worker.command_record_bytes
                    || serde_json::from_slice::<String>(&bytes).ok().as_deref() != Some(id)
                {
                    return Err("invalid acknowledgment marker".into());
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(vec![id.to_owned()]);
            }
            Err(_) => return Err("acknowledgment marker unavailable".into()),
        }
    }
    Ok(vec![])
}

fn apply_acknowledgments(
    state: &Path,
    request: &Operation,
    limits: &RuntimeLimits,
) -> Result<(), String> {
    let Operation::AwaitCommands { acknowledged, .. } = request else {
        return Err("expected wait journal".into());
    };
    for id in acknowledged {
        let path = state
            .join("commands")
            .join(format!("{}.json", component(id)));
        let receipt = read_receipt(&path, limits)?;
        if command_identity(&receipt.command).0 != id {
            return Err("acknowledged command identity differs".into());
        }
        atomic_json(&state.join("acks").join(component(id)), id)?;
    }
    Ok(())
}

async fn recover_wait(client: &ApiClient, state: &Path, worker: &str) -> Result<(), String> {
    let path = state.join("mutations/await.json");
    let Some(mut journal) = mutations::read(&path, client.limits())? else {
        return Ok(());
    };
    if journal.consumed {
        return Ok(());
    }
    let Operation::AwaitCommands { worker_id, .. } = &journal.request else {
        return Err("unexpected wait journal operation".into());
    };
    if worker_id != worker {
        return Err("saved wait belongs to another worker".into());
    }
    match mutations::recorded(client, &path, &mut journal).await? {
        Some(Outcome::Ok {
            data: Data::Commands(commands),
        }) => {
            // The exact response is already durable. Materialize commands for
            // recovery only; a restarted worker never executes this old batch.
            apply_acknowledgments(state, &journal.request, client.limits())?;
            for command in commands {
                if matches!(&command, WorkerCommand::RunAttempt { attempt, .. } if attempt.worker_id != worker)
                {
                    return Err("saved command belongs to another worker".into());
                }
                let (id, _) = command_identity(&command);
                record_launch(
                    &state
                        .join("commands")
                        .join(format!("{}.json", component(id))),
                    &command,
                    client.limits(),
                )?;
                if let WorkerCommand::StopAttempt { attempt_id, .. } = &command {
                    atomic_json(&state.join("stops").join(component(attempt_id)), &command)?;
                }
            }
            mutations::consume(&path, &mut journal)
        }
        Some(Outcome::Refused { .. }) => mutations::consume(&path, &mut journal),
        Some(_) => Err("unexpected recorded command reply".into()),
        None => Ok(()), // Pending is retained; registration fences the old incarnation.
    }
}

async fn admit_launch(
    reporter: &Reporter,
    command: &WorkerCommand,
    control: &Control,
    receipt_path: &Path,
) -> Result<bool, String> {
    validate_command_session(command)?;
    let WorkerCommand::RunAttempt {
        session,
        attempt,
        task: input,
        checkpoint,
        continuation,
        ..
    } = command
    else {
        return Err("expected launch".into());
    };
    let start_id = format!("{}-initial-start", attempt.id);
    loop {
        control.check()?;
        if reporter
            .send_identified(
                start_id.clone(),
                Observation::Started {
                    thread_id: None,
                    turn_id: None,
                },
            )
            .await?
        {
            break;
        }
        if reporter
            .outbox
            .join(format!("{}.refused", component(&start_id)))
            .exists()
        {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(
            reporter.config.limits.worker.reconnect_initial_ms,
        ))
        .await;
    }
    // A cached successful report is historical, so read actual current state.
    let current = controller_call(
        &reporter.client,
        Operation::GetTask {
            task_id: attempt.task_id.clone(),
        },
        control,
    )
    .await?;
    let Data::Task(current) = current else {
        return Err("unexpected current task reply".into());
    };
    let mut accepted_input = current.input.clone();
    // The controller binds the original task but allocates the remaining budget
    // into RunAttempt. Every other immutable input field must still match.
    accepted_input.limits.model_requests = input.limits.model_requests;
    let valid = current.session_id == session.id
        && current.assigned_agent_id == session.agent_id
        && current.status == TaskStatus::Active
        && current.attempt.as_ref().is_some_and(|a| {
            a.id == attempt.id
                && a.session_id == session.id
                && a.generation == attempt.generation
                && a.worker_id == reporter.config.id
                && a.worker_incarnation == reporter.config.incarnation
                && !a.stopped
                && !a.uncertain
                && a.phase == AttemptPhase::Running
                && process::now_ms() < a.deadline_unix_ms
        })
        && accepted_input == *input
        && current.selected_checkpoint == *checkpoint
        && current.completed_child == *continuation;
    if !valid {
        return Ok(false);
    }
    input.definition.required_inference()?;
    input
        .definition
        .execution_profile
        .as_ref()
        .ok_or("template_profile_unbound")?;
    control.check()?;
    resource::verify(&reporter.resource)?;
    let mut receipt = read_receipt(receipt_path, reporter.client.limits())?;
    if receipt.command != *command || receipt.completed || receipt.effects_started {
        return Err("launch receipt no longer admits new execution".into());
    }
    receipt.effects_started = true;
    atomic_json(receipt_path, &receipt)?;
    Ok(true)
}

async fn finish_unstarted(reporter: &Reporter) -> bool {
    if reporter
        .send(Observation::ProcessTreeStopped)
        .await
        .is_err()
    {
        return false;
    }
    let current = match tokio::time::timeout(
        Duration::from_millis(reporter.client.limits().worker.handoff_call_timeout_ms),
        reporter.client.call(Operation::GetTask {
            task_id: reporter.attempt.task_id.clone(),
        }),
    )
    .await
    {
        Ok(Ok(Reply {
            outcome: Outcome::Ok {
                data: Data::Task(task),
            },
            ..
        })) => task,
        _ => return false,
    };
    if current.session_id == reporter.attempt.session_id
        && current.assigned_agent_id == current.input.definition.id
        && current.attempt.as_ref().is_some_and(|a| {
            a.id == reporter.attempt.id
                && a.session_id == reporter.attempt.session_id
                && a.generation == reporter.attempt.generation
        })
        && current.status == TaskStatus::Active
    {
        return reporter
            .send(Observation::ResultCandidate {
                checks_passed: false,
                summary:
                    "Attempt did not start because current execution admission was not confirmed"
                        .into(),
                checkpoint: None,
                validation: vec![],
                output: None,
            })
            .await
            .is_ok();
    }
    true
}

async fn reconcile_saved_handoffs(
    config: &WorkerConfig,
    client: &ApiClient,
    state: &Path,
    outbox_lock: &Arc<tokio::sync::Mutex<()>>,
    active: &BTreeMap<String, Active>,
    resource: &Option<Arc<resource::Envelope>>,
) -> Result<(), String> {
    let mut pending = false;
    for entry in std::fs::read_dir(state.join("commands")).map_err(|e| e.to_string())? {
        let path = entry.map_err(|e| e.to_string())?.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let bytes = std::fs::read(&path).map_err(|e| e.to_string())?;
        if bytes.len() > config.limits.worker.command_record_bytes {
            return Err("oversized recovered command".into());
        }
        let mut receipt: Receipt =
            serde_json::from_slice(&bytes).map_err(|_| "corrupt recovered command")?;
        validate_command_session(&receipt.command)?;
        if receipt.completed {
            continue;
        }
        let WorkerCommand::RunAttempt { attempt, task, .. } = &receipt.command else {
            continue;
        };
        if active.contains_key(&attempt.id)
            || (!handoff::path(state, attempt).exists()
                && !(task.definition.executor == ExecutorKind::Native
                    && crate::execution_profile::collaboration(task)))
        {
            continue;
        }
        if attempt.worker_id != config.id {
            return Err("delegation belongs to another worker".into());
        }
        let mut original = config.clone();
        original.incarnation = attempt.worker_incarnation.clone();
        let reporter = Reporter {
            client: ApiClient::new(
                &config.controller_url,
                Arc::new(
                    (*task
                        .resolved_limits
                        .as_ref()
                        .ok_or("execution limits unbound")?
                        .global_metadata)
                        .clone(),
                ),
            )?,
            config: original,
            resource: resource.clone(),
            attempt: attempt.clone(),
            outbox: state.join("outbox"),
            sequence: Arc::new(AtomicU64::new(1)),
            executor_seen: Arc::new(AtomicBool::new(false)),
            provider_uncertain: Arc::new(AtomicBool::new(false)),
            uncertainty_retained: Arc::new(AtomicBool::new(false)),
            output_limit: task.limits.output_bytes,
            result_limit: task.limits.result_bytes,
            coding_limits: task.execution_limits()?.coding.clone(),
            outbox_lock: outbox_lock.clone(),
        };
        if task.definition.executor == ExecutorKind::Native
            && crate::execution_profile::collaboration(task)
        {
            match native_loop::recover_delegate(config, task, &reporter, state).await? {
                Some(AttemptExit::Finished) => {
                    receipt.completed = true;
                    atomic_json(&path, &receipt)?;
                    continue;
                }
                Some(AttemptExit::DelegationPending) => {
                    pending = true;
                    continue;
                }
                None => {}
            }
            if !handoff::path(state, attempt).exists() {
                continue;
            }
        }
        match handoff::reconcile(state, task, &reporter).await? {
            AttemptExit::Finished => {
                receipt.completed = true;
                atomic_json(&path, &receipt)?;
            }
            AttemptExit::DelegationPending => pending = true,
        }
    }
    if pending {
        Err("delegation stop acknowledgment awaits reconciliation".into())
    } else {
        Ok(())
    }
}

fn collaboration_caller(attempt: &Attempt) -> crate::mcp::Caller {
    crate::mcp::Caller {
        task_id: attempt.task_id.clone(),
        attempt_id: attempt.id.clone(),
        generation: attempt.generation,
        worker_id: attempt.worker_id.clone(),
        incarnation: attempt.worker_incarnation.clone(),
    }
}
fn coding_collaboration_closed(
    config: &WorkerConfig,
    input: &TaskInput,
    attempt: &Attempt,
    state: &Path,
) -> Result<bool, String> {
    if input.definition.executor != ExecutorKind::Coding {
        return Ok(true);
    }
    let config = codex_mcp::Config {
        controller_url: config.controller_url.clone(),
        caller: collaboration_caller(attempt),
        deadline_unix_ms: attempt.deadline_unix_ms.min(input.limits.deadline_unix_ms),
        state: codex::state_path(&state.join("codex"), &attempt.id),
        runtime_limits: (*input
            .resolved_limits
            .as_ref()
            .ok_or("execution limits unbound")?
            .global_metadata)
            .clone(),
        limits: input.execution_limits()?.collaboration.clone(),
        worker_limits: config.limits.worker.clone(),
    };
    let disposition = codex_mcp::outcome_disposition(&config)?;
    if !crate::execution_profile::collaboration(input)
        && disposition != codex_mcp::OutcomeDisposition::NoCalls
    {
        return Err("coding collaboration journal differs from accepted profile".into());
    }
    Ok(disposition != codex_mcp::OutcomeDisposition::Unresolved)
}
fn audit_stopped_coding_collaboration(config: &WorkerConfig, state: &Path) -> Result<(), String> {
    // Called only after all recorded process trees have been reconciled, before
    // registration or completed/terminal command shortcuts can discard evidence.
    for entry in std::fs::read_dir(state.join("commands")).map_err(|e| e.to_string())? {
        let path = entry.map_err(|e| e.to_string())?.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let receipt = read_receipt(&path, &config.limits)?;
        if let WorkerCommand::RunAttempt { attempt, task, .. } = &receipt.command {
            if attempt.worker_id != config.id {
                return Err("coding journal belongs to another worker".into());
            }
            if !coding_collaboration_closed(config, task, attempt, state)? {
                return Err(
                    "coding collaboration outcome unresolved; replacement registration refused"
                        .into(),
                );
            }
        }
    }
    Ok(())
}

async fn account_for_interrupted_commands(
    client: &ApiClient,
    state: &Path,
    worker_id: &str,
) -> Result<(), String> {
    let mut paths = std::fs::read_dir(state.join("commands"))
        .map_err(|e| e.to_string())?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .collect::<Vec<_>>();
    paths.sort();
    for path in paths {
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let bytes = std::fs::read(&path).map_err(|e| e.to_string())?;
        if bytes.len() > client.limits().worker.command_record_bytes {
            return Err("oversized recovered command".into());
        }
        let mut receipt: Receipt =
            serde_json::from_slice(&bytes).map_err(|_| "corrupt recovered command")?;
        validate_command_session(&receipt.command)?;
        if receipt.completed {
            continue;
        }
        let WorkerCommand::RunAttempt {
            attempt,
            task: input,
            ..
        } = &receipt.command
        else {
            continue;
        };
        if attempt.worker_id != worker_id {
            return Err("recovered command belongs to another worker".into());
        }
        if handoff::path(state, attempt).exists() {
            return Err("completed delegation decision awaits exact reconciliation".into());
        }
        let reply = tokio::time::timeout(
            Duration::from_millis(client.limits().worker.handoff_call_timeout_ms),
            client.call(Operation::GetTask {
                task_id: attempt.task_id.clone(),
            }),
        )
        .await
        .map_err(|_| "recovery lookup timed out")??;
        let task = match reply.outcome {
            Outcome::Ok {
                data: Data::Task(t),
            } => t,
            _ => {
                return Err(
                    "recovery lookup unavailable; refusing a new worker incarnation".into(),
                );
            }
        };
        if task.session_id != attempt.session_id
            || task
                .attempt
                .as_ref()
                .is_some_and(|a| a.session_id != attempt.session_id)
            || task.input.definition != input.definition
            || task.assigned_agent_id != input.definition.id
        {
            return Err("recovered task session binding differs".into());
        }
        if task
            .attempt
            .as_ref()
            .is_none_or(|a| a.id != attempt.id || a.generation != attempt.generation)
            || matches!(
                task.status,
                TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::Cancelled
            ) && task.attempt.as_ref().is_some_and(|a| a.stopped)
        {
            receipt.completed = true;
            atomic_json(&path, &receipt)?;
            continue;
        }
        if task.attempt.as_ref().is_some_and(|a| a.stopped)
            && acceptance::completed_decision(&state.join("outbox"), attempt, client.limits())?
        {
            // The model/executor already completed. A retained acceptance
            // decision is not an unfinished execution to rerun or relabel.
            receipt.completed = true;
            atomic_json(&path, &receipt)?;
            continue;
        }
        let message="Worker process restarted. Prior owned processes were stopped; unfinished executor effects require reconciliation before a new attempt.".to_owned();
        let native_stop_unverified = receipt.effects_started
            && input.definition.executor == ExecutorKind::Native
            && !task.attempt.as_ref().is_some_and(|a| a.stopped)
            && !native_registry_reconciled(state, attempt, client.limits()).await?;
        let message = if native_stop_unverified {
            "Worker restarted without a completed native decision or durable stopped state. Native helper termination requires reconciliation before replacement registration.".to_owned()
        } else {
            message
        };
        for (kind, observation) in [
            ("uncertain", Observation::ExecutionUncertain { message }),
            ("stopped", Observation::ProcessTreeStopped),
        ] {
            if kind == "uncertain" && !receipt.effects_started {
                continue;
            }
            if kind == "stopped" && native_stop_unverified {
                return Err(
                    "native restart cleanup unverified; refusing replacement registration".into(),
                );
            }
            let report = Report {
                report_id: format!("{}-recovery-{kind}", attempt.id),
                worker_id: worker_id.into(),
                incarnation: attempt.worker_incarnation.clone(),
                task_id: attempt.task_id.clone(),
                attempt_id: attempt.id.clone(),
                generation: attempt.generation,
                observation,
            };
            let reply = tokio::time::timeout(
                Duration::from_millis(client.limits().worker.handoff_call_timeout_ms),
                client.call(Operation::ObserveWorker { report }),
            )
            .await
            .map_err(|_| "recovery report timed out")??;
            if !matches!(reply.outcome, Outcome::Ok { .. }) {
                return Err(
                    "prior worker outcome was not reconciled; refusing replacement registration"
                        .into(),
                );
            }
        }
        if !receipt.effects_started && task.status == TaskStatus::Active {
            let report = Report {
                report_id: format!("{}-recovery-not-started", attempt.id),
                worker_id: worker_id.into(),
                incarnation: attempt.worker_incarnation.clone(),
                task_id: attempt.task_id.clone(),
                attempt_id: attempt.id.clone(),
                generation: attempt.generation,
                observation: Observation::ResultCandidate {
                    checks_passed: false,
                    summary: "Worker restarted before this received command began execution".into(),
                    checkpoint: None,
                    validation: vec![],
                    output: None,
                },
            };
            let reply = tokio::time::timeout(
                Duration::from_millis(client.limits().worker.handoff_call_timeout_ms),
                client.call(Operation::ObserveWorker { report }),
            )
            .await
            .map_err(|_| "unstarted recovery result timed out")??;
            if !matches!(reply.outcome, Outcome::Ok { .. }) {
                return Err("unstarted recovery result awaits reconciliation".into());
            }
        }
        receipt.completed = true;
        atomic_json(&path, &receipt)?;
    }
    Ok(())
}

async fn shutdown(
    active: &mut BTreeMap<String, Active>,
    state: &Path,
    client: &ApiClient,
    outbox_lock: &tokio::sync::Mutex<()>,
    validation_runtime: &Arc<crate::native::ValidationRuntime>,
) -> Result<(), String> {
    for attempt in active.values() {
        let _ = attempt.cancel.send(true);
    }
    // One shared deadline bounds shutdown independently of the resident count.
    let deadline = tokio::time::Instant::now()
        + Duration::from_millis(client.limits().worker.shutdown_join_timeout_ms);
    for (_, mut attempt) in std::mem::take(active) {
        if tokio::time::timeout_at(deadline, &mut attempt.join)
            .await
            .is_err()
        {
            attempt.join.abort();
            let _ = attempt.join.await;
        }
    }
    //= spec/impl/v2/implementation.md#interrupted-work-is-reconciled
    //# The platform MUST NOT report an interrupted command as stopped until its worker confirms termination or the worker is known unable to continue it.
    // Aborted report delivery remains durable in the outbox. No stop claim is
    // manufactured here: attempt-owned reporting follows verified cleanup.
    let mut cleanup_error = None;
    for entry in std::fs::read_dir(state.join("processes")).map_err(|e| e.to_string())? {
        let path = entry.map_err(|e| e.to_string())?.path();
        if path.is_dir() {
            if let Err(error) = process::reconcile_processes(&path, &client.limits().worker).await {
                cleanup_error = Some(error);
            }
        }
    }
    let native_deadline = tokio::time::Instant::now()
        + Duration::from_millis(client.limits().worker.shutdown_native_timeout_ms);
    while crate::native::validation_cleanup_pending() > 0 {
        if tokio::time::Instant::now() >= native_deadline {
            cleanup_error = Some("native_validation_cleanup_unverified".into());
            break;
        }
        tokio::time::sleep(Duration::from_millis(
            client.limits().worker.process_poll_ms,
        ))
        .await;
    }
    let _ = tokio::time::timeout(
        Duration::from_millis(client.limits().worker.shutdown_outbox_timeout_ms),
        async {
            let _lock = outbox_lock.lock().await;
            flush_outbox(client, &state.join("outbox")).await
        },
    )
    .await;
    match cleanup_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

// The owner lock is held by the caller. Persist once; never reinterpret old
// command/process journals under changed startup metadata.
fn retained_worker_limits(
    state: &Path,
    selected: &Arc<RuntimeLimits>,
) -> Result<Arc<RuntimeLimits>, String> {
    use std::io::Read;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let path = state.join("limits.json");
    let opened = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(0x20000 | 0x800)
        .open(&path);
    let mut file = match opened {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            for name in [
                "commands",
                "outbox",
                "processes",
                "codex",
                "mutations",
                "native-tracking",
                "publications",
            ] {
                // The pre-created settlement directory itself carries no evidence.
                let entries = std::fs::read_dir(state.join(name)).map_err(|e| e.to_string())?;
                for entry in entries {
                    let p = entry.map_err(|e| e.to_string())?.path();
                    if name == "mutations"
                        && p.file_name().is_some_and(|n| n == "settlements")
                        && p.is_dir()
                        && std::fs::read_dir(&p)
                            .map_err(|e| e.to_string())?
                            .next()
                            .is_none()
                    {
                        continue;
                    }
                    return Err(
                        "worker state has no retained limits binding; refusing reinterpretation"
                            .into(),
                    );
                }
            }
            atomic_json(&path, &**selected)?;
            return Ok(selected.clone());
        }
        Err(_) => return Err("retained worker limits unavailable".into()),
    };
    let meta = file
        .metadata()
        .map_err(|_| "retained worker limits metadata unavailable")?;
    let uid = std::fs::metadata("/proc/self")
        .map_err(|_| "worker owner unavailable")?
        .uid();
    if !meta.is_file() || meta.nlink() != 1 || meta.uid() != uid || meta.mode() & 0o077 != 0 {
        return Err("retained worker limits type/owner/mode differs".into());
    }
    let size =
        usize::try_from(meta.len()).map_err(|_| "retained limits exceed addressable size")?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| "retained limits allocation unavailable")?;
    file.by_ref()
        .take(meta.len())
        .read_to_end(&mut bytes)
        .map_err(|_| "retained worker limits unreadable")?;
    let mut extra = [0];
    if bytes.len() != size
        || file
            .read(&mut extra)
            .map_err(|_| "retained limits tail unreadable")?
            != 0
    {
        return Err("retained worker limits changed while reading".into());
    }
    Ok(Arc::new(RuntimeLimits::parse(&bytes)?))
}

pub async fn run(mut config: WorkerConfig) -> Result<(), String> {
    // Refuse uninspectable named inputs before registration, state, or helpers.
    config.limits.validate()?;
    configured_credentials(&config.limits)?;
    // Register before any controller/recovery await. A signal received while
    // registering or reconnecting is retained for the whole lifecycle select.
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|e| format!("worker signal handler unavailable: {e}"))?;
    // Qualification probes are host diagnostics. Register before recovery so
    // SIGUSR1 cannot apply its default disposition during an awaited startup.
    let mut clock_probe =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())
            .map_err(|e| format!("worker clock signal handler unavailable: {e}"))?;
    let uid = std::fs::read_to_string("/proc/self/status")
        .map_err(|_| "cannot establish worker privilege level")?
        .lines()
        .find(|l| l.starts_with("Uid:"))
        .and_then(|l| l.split_whitespace().nth(2))
        .and_then(|s| s.parse::<u32>().ok())
        .ok_or("cannot establish worker privilege level")?;
    if uid == 0 {
        return Err("worker refuses administrator execution".into());
    }
    if config.id.is_empty()
        || config.incarnation.is_empty()
        || config.id.len() > config.limits.worker.identity_bytes
        || config.incarnation.len() > config.limits.worker.identity_bytes
    {
        return Err("invalid worker identity".into());
    }
    if config.coding_capacity != config.limits.worker.coding_capacity {
        return Err("worker capacity differs from selected metadata (worker.coding_capacity)".into());
    }
    if config.native_capacity != config.limits.worker.native_capacity {
        return Err("worker capacity differs from selected metadata (worker.native_capacity)".into());
    }
    std::fs::create_dir_all(&config.workspace_root).map_err(|e| e.to_string())?;
    config.workspace_root =
        std::fs::canonicalize(&config.workspace_root).map_err(|e| e.to_string())?;
    let state = config
        .workspace_root
        .join(".borg-worker")
        .join(component(&config.id));
    handoff::initialize_state(&state, &config.limits.worker)?;
    for name in [
        "commands",
        "acks",
        "outbox",
        "processes",
        "codex",
        "stops",
        "delegations",
        "native-tracking",
        "publications",
        "mutations",
        "mutations/settlements",
    ] {
        std::fs::create_dir_all(state.join(name)).map_err(|e| e.to_string())?;
    }
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o700))
            .map_err(|e| e.to_string())?;
    }
    let _lease =
        process::WorkerLease::claim(&state.join("worker-owner.json"), &config.limits.worker)?;
    let selected_limits = config.limits.clone();
    config.limits = retained_worker_limits(&state, &selected_limits)?;
    let policy_changed = *selected_limits != *config.limits;
    let worker_limits = Arc::new(config.limits.worker.clone());
    let validation_runtime = Arc::new(crate::native::ValidationRuntime::new(
        worker_limits.clone(),
    )?);
    let owner_lock = _lease.owner_lock();
    //= spec/impl/v2/implementation.md#worker-commands-do-not-create-duplicate-execution
    //# A restarted worker MUST account for surviving prior-incarnation processes before accepting new work.
    for entry in std::fs::read_dir(state.join("processes")).map_err(|e| e.to_string())? {
        let path = entry.map_err(|e| e.to_string())?.path();
        if path.is_dir() {
            process::reconcile_processes(&path, &config.limits.worker).await?;
        }
    }
    audit_stopped_coding_collaboration(&config, &state)?;
    let _ = command_receipts(&state, &config.limits)?;
    if policy_changed {
        return Err("worker limits differ from retained state policy; retained processes reconciled, new work refused".into());
    }
    let client = ApiClient::new(&config.controller_url, config.limits.clone())?;
    let mut active: BTreeMap<String, Active> = BTreeMap::new();
    let outbox_lock = Arc::new(tokio::sync::Mutex::new(()));
    let (finished, mut completions) = mpsc::channel::<String>(
        config
            .coding_capacity
            .saturating_add(config.native_capacity),
    );
    let mut registered = false;
    let mut recovered = false;
    let mut recovery_diagnosed = false;
    let mut backoff = Duration::from_millis(config.limits.worker.reconnect_initial_ms);
    // Profile refusal occurs after exclusive ownership and actual process reap.
    // The common shutdown path can still flush retained reports and cleanup.
    let envelope = resource::load(config.resource_profile.as_deref(), worker_limits.clone());
    let result = {
        let serving = async {
            let envelope = envelope?;
            if envelope.is_none() {
                eprintln!(
                    "Worker resource envelope unconfigured: development mode, resource limits unqualified"
                );
            }
            loop {
                resource::verify(&envelope)?;
                if !registered {
                    let reports_ready = reports_ready_before_registration(
                        &client,
                        &state.join("outbox"),
                        &outbox_lock,
                    )
                    .await;
                    if reports_ready.is_err() {
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2)
                            .min(Duration::from_millis(config.limits.worker.reconnect_max_ms));
                        continue;
                    }
                    if !recovered {
                        let recovered_result = async {
                            publication::recover(
                                &config,
                                &client,
                                &state,
                                &envelope,
                                &outbox_lock,
                                &validation_runtime,
                            )
                            .await?;
                            mutations::recover_settlements(&client, &state).await?;
                            resource::verify(&envelope)?;
                            recover_wait(&client, &state, &config.id).await?;
                            reconcile_saved_handoffs(
                                &config,
                                &client,
                                &state,
                                &outbox_lock,
                                &active,
                                &envelope,
                            )
                            .await?;
                            account_for_interrupted_commands(&client, &state, &config.id).await
                        }
                        .await;
                        if let Err(error) = recovered_result {
                            if !recovery_diagnosed {
                                eprintln!(
                                    "Worker recovery admission refused: {}",
                                    scrub_characters(
                                        error,
                                        config.limits.execution.coding.progress_chars,
                                        config.limits.controller.max_progress_bytes,
                                        &config.limits
                                    )
                                );
                                recovery_diagnosed = true;
                            }
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2)
                                .min(Duration::from_millis(config.limits.worker.reconnect_max_ms));
                            continue;
                        }
                        recovered = true;
                    }
                    // Recovery itself can enqueue a completed handoff/result.
                    // It must not bypass the same pre-registration drain gate.
                    if pending_reports(&state.join("outbox"), &config.limits)? {
                        continue;
                    }
                    resource::verify(&envelope)?;
                    match mutations::register(
                        &client,
                        &state,
                        Worker {
                            id: config.id.clone(),
                            incarnation: config.incarnation.clone(),
                            coding_capacity: config.coding_capacity,
                            native_capacity: config.native_capacity,
                        },
                    )
                    .await?
                    {
                        true => {
                            registered = true;
                            backoff =
                                Duration::from_millis(config.limits.worker.reconnect_initial_ms);
                        }
                        false => {
                            recovered = false;
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2)
                                .min(Duration::from_millis(config.limits.worker.reconnect_max_ms));
                            continue;
                        }
                    }
                }
                {
                    let _lock = outbox_lock.lock().await;
                    flush_outbox(&client, &state.join("outbox")).await?;
                }
                // A completed decision retries only its durable handoff, never the model.
                resource::verify(&envelope)?;
                let _ = reconcile_saved_handoffs(
                    &config,
                    &client,
                    &state,
                    &outbox_lock,
                    &active,
                    &envelope,
                )
                .await;
                resource::verify(&envelope)?;
                let wait_path = state.join("mutations/await.json");
                let mut wait = match mutations::read(&wait_path, &config.limits)? {
                    Some(journal) if !journal.consumed => journal,
                    _ => mutations::create(
                        &wait_path,
                        Operation::AwaitCommands {
                            operation_id: mutations::fresh_id("await")?,
                            worker_id: config.id.clone(),
                            incarnation: config.incarnation.clone(),
                            acknowledged: pending_acknowledgments(
                                &state,
                                &config.incarnation,
                                &config.limits,
                            )?,
                        },
                        0,
                        &config.limits,
                    )?,
                };
                let Operation::AwaitCommands {
                    worker_id,
                    incarnation,
                    ..
                } = &wait.request
                else {
                    return Err("unexpected wait journal operation".into());
                };
                if worker_id != &config.id {
                    return Err("saved wait belongs to another worker".into());
                }
                let prior_incarnation = incarnation != &config.incarnation;
                let reply = tokio::select! {
                    response=mutations::resolve(&client, &wait_path, &mut wait)=>response?,
                    done=completions.recv()=>{if let Some(id)=done {if let Some(a)=active.remove(&id){let _=a.join.await;}}continue;},
                };
                if prior_incarnation {
                    if reply.is_some() {
                        resource::verify(&envelope)?;
                        recover_wait(&client, &state, &config.id).await?;
                        account_for_interrupted_commands(&client, &state, &config.id).await?;
                    }
                    continue;
                }
                let commands = match reply {
                    Some(Outcome::Ok {
                        data: Data::Commands(commands),
                    }) => {
                        apply_acknowledgments(&state, &wait.request, &config.limits)?;
                        backoff = Duration::from_millis(config.limits.worker.reconnect_initial_ms);
                        commands
                    }
                    Some(Outcome::Refused { code, .. }) => {
                        return Err(format!("worker command stream refused: {code}"));
                    }
                    Some(_) => return Err("unexpected recorded command reply".into()),
                    None => {
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2)
                            .min(Duration::from_millis(config.limits.worker.reconnect_max_ms));
                        registered = false;
                        continue;
                    }
                };
                for command in commands {
                    match command.clone() {
                        WorkerCommand::RunAttempt {
                            command_id,
                            session,
                            attempt,
                            mut task,
                            checkpoint,
                            continuation,
                        } => {
                            //= spec/impl/v2/implementation.md#worker-commands-do-not-create-duplicate-execution
                            //# A worker MUST reject a launch addressed to an earlier incarnation.
                            if attempt.worker_incarnation != config.incarnation
                                || attempt.worker_id != config.id
                            {
                                continue;
                            }
                            session.validate_for(&attempt, &task)?;
                            let receipt_path = state
                                .join("commands")
                                .join(format!("{}.json", component(&command_id)));
                            if receipt_path.exists() {
                                record_launch(&receipt_path, &command, &config.limits)?;
                                //= spec/impl/v2/implementation.md#worker-commands-do-not-create-duplicate-execution
                                //# A duplicate launch in the same worker incarnation MUST return the existing execution or outcome instead of starting another execution.
                                continue;
                            }
                            let executor = task.definition.executor;
                            let capacity = match executor {
                                ExecutorKind::Coding => config.coding_capacity,
                                ExecutorKind::Native => config.native_capacity,
                            };
                            if active.values().filter(|a| a.executor == executor).count()
                                >= capacity
                            {
                                continue;
                            }
                            record_launch(&receipt_path, &command, &config.limits)?;
                            if task.definition.executor == ExecutorKind::Coding {
                                if let Some(child) = &continuation {
                                    task.instruction.push_str("\n\nRecorded child task outcome for this new continuation (not recovered executor history):\n");
                                    task.instruction.push_str(
                                        &serde_json::to_string(&child)
                                            .map_err(|e| e.to_string())?,
                                    );
                                }
                            }
                            // Native receives its original immutable input and a
                            // typed continuation; Control supplies the attempt's
                            // earlier deadline without rewriting that input.
                            let (cancel, receiver) = watch::channel(
                                state.join("stops").join(component(&attempt.id)).exists(),
                            );
                            let control = Control {
                                deadline_ms: attempt
                                    .deadline_unix_ms
                                    .min(task.limits.deadline_unix_ms),
                                cancel: receiver,
                                worker_limits: worker_limits.clone(),
                                runtime_limits: Arc::new(
                                    (*task
                                        .resolved_limits
                                        .as_ref()
                                        .ok_or("execution limits unbound")?
                                        .global_metadata)
                                        .clone(),
                                ),
                                validation_runtime: validation_runtime.clone(),
                            };
                            let reporter = Reporter {
                                client: ApiClient::new(
                                    &config.controller_url,
                                    control.runtime_limits.clone(),
                                )?,
                                config: config.clone(),
                                resource: envelope.clone(),
                                attempt: attempt.clone(),
                                outbox: state.join("outbox"),
                                sequence: Arc::new(AtomicU64::new(1)),
                                executor_seen: Arc::new(AtomicBool::new(false)),
                                provider_uncertain: Arc::new(AtomicBool::new(false)),
                                uncertainty_retained: Arc::new(AtomicBool::new(false)),
                                output_limit: task.limits.output_bytes,
                                result_limit: task.limits.result_bytes,
                                coding_limits: task.execution_limits()?.coding.clone(),
                                outbox_lock: outbox_lock.clone(),
                            };
                            let task_id = attempt.task_id.clone();
                            let generation = attempt.generation;
                            let attempt_id = attempt.id.clone();
                            let cfg = config.clone();
                            let attempt_state = state.clone();
                            let done = finished.clone();
                            let validation_owner = owner_lock.clone();
                            let join = tokio::spawn(async move {
                                match admit_launch(&reporter, &command, &control, &receipt_path)
                                    .await
                                {
                                    Ok(true) => {}
                                    Ok(false) | Err(_) => {
                                        // No workspace/helper/provider action has begun. Keep
                                        // report ordering, but do not invent an executor outcome.
                                        let retained = finish_unstarted(&reporter).await;
                                        if let Ok(mut receipt) =
                                            read_receipt(&receipt_path, &cfg.limits)
                                        {
                                            receipt.completed = retained;
                                            let _ = atomic_json(&receipt_path, &receipt);
                                        }
                                        let _ = done.send(attempt.id).await;
                                        return;
                                    }
                                }
                                // Traceability scope: One accepted executor branch runs; errors enter cleanup/reporting below and do not dispatch the other executor.
                                //= spec/impl/v2/implementation.md#native-tool-profiles-and-continuations
                                //# The platform MUST NOT switch a task's executor automatically after a model or tool failure.
                                let result = match task.definition.executor {
                                    ExecutorKind::Coding => coding(
                                        &cfg,
                                        &attempt,
                                        &task,
                                        checkpoint.as_ref(),
                                        &attempt_state,
                                        &control,
                                        &reporter,
                                        &command,
                                    )
                                    .await
                                    .map(|_| AttemptExit::Finished),
                                    ExecutorKind::Native => native(
                                        &cfg,
                                        &task,
                                        &control,
                                        &reporter,
                                        continuation.as_ref(),
                                        &attempt_state,
                                        validation_owner,
                                    )
                                    .await
                                    .map_err(AttemptFailure::Execution),
                                };
                                let mut cleanup_verified = true;
                                let mut reports_ready = true;
                                let mut collaboration_closed = true;
                                if let Err(error) = &result {
                                    let unverified = error.message().contains("unverified");
                                    let mut uncertain = error.execution_uncertain(
                                        reporter.provider_uncertain.load(Ordering::Relaxed),
                                    );
                                    let error = scrub(
                                        error.message().to_owned(),
                                        reporter.coding_limits.summary_bytes,
                                        &cfg.limits,
                                    );
                                    let cleanup = process::reconcile_processes(
                                        &attempt_state
                                            .join("processes")
                                            .join(component(&attempt.id)),
                                        &cfg.limits.worker,
                                    )
                                    .await;
                                    cleanup_verified = cleanup.is_ok() && !unverified;
                                    if task.definition.executor == ExecutorKind::Coding {
                                        // A reporter failure may have dropped the executor future
                                        // before its own audit. Never infer closure from its text.
                                        collaboration_closed = cleanup_verified
                                            && coding_collaboration_closed(
                                                &cfg,
                                                &task,
                                                &attempt,
                                                &attempt_state,
                                            )
                                            .unwrap_or(false);
                                        uncertain |= !collaboration_closed;
                                    }
                                    reports_ready = report_attempt_failure(
                                        &reporter,
                                        error,
                                        cleanup_verified,
                                        uncertain,
                                    )
                                    .await;
                                }
                                // Only this active execution updates its launch phase;
                                // acknowledgment markers are separate immutable records.
                                if let Ok(mut receipt) = read_receipt(&receipt_path, &cfg.limits) {
                                    receipt.completed = reports_ready
                                        && collaboration_closed
                                        && receipt_completed(&result, cleanup_verified);
                                    let _ = atomic_json(&receipt_path, &receipt);
                                }
                                let _ = done.send(attempt.id).await;
                            });
                            active.insert(
                                attempt_id,
                                Active {
                                    task_id,
                                    generation,
                                    executor,
                                    cancel,
                                    join,
                                },
                            );
                        }
                        WorkerCommand::StopAttempt {
                            command_id,
                            task_id,
                            attempt_id,
                            generation,
                            incarnation,
                        } => {
                            let receipt_path = state
                                .join("commands")
                                .join(format!("{}.json", component(&command_id)));
                            record_launch(&receipt_path, &command, &config.limits)?;
                            if incarnation != config.incarnation {
                                continue;
                            }
                            if let Some(running) = active.get(&attempt_id) {
                                if running.task_id == task_id && running.generation == generation {
                                    atomic_json(
                                        &state.join("stops").join(component(&attempt_id)),
                                        &command,
                                    )?;
                                    let _ = running.cancel.send(true);
                                }
                            } else {
                                atomic_json(
                                    &state.join("stops").join(component(&attempt_id)),
                                    &command,
                                )?;
                            }
                        }
                    }
                }
                mutations::consume(&wait_path, &mut wait)?;
            }
        };
        tokio::pin!(serving);
        let mut clock_probe_open = true;
        loop {
            tokio::select! {
                biased;
                _ = interrupt.recv() => break Ok(()),
                // Poll the same pinned future first: diagnostic traffic cannot
                // replace an outstanding command await or starve ready work.
                result = &mut serving => break result,
                signal = clock_probe.recv(), if clock_probe_open => {
                    if signal.is_some() {
                        validation_runtime.probe_request_clock();
                    } else {
                        // A closed stream cannot become an always-ready loop.
                        clock_probe_open = false;
                    }
                }
            }
        }
    };
    shutdown(
        &mut active,
        &state,
        &client,
        &outbox_lock,
        &validation_runtime,
    )
    .await?;
    result
}

#[cfg(test)]
pub(crate) fn test_limits() -> Arc<RuntimeLimits> {
    Arc::new(RuntimeLimits::seed().expect("explicit test metadata seed"))
}
#[cfg(test)]
pub(crate) fn test_control(deadline_ms: u64, cancel: watch::Receiver<bool>) -> Control {
    let runtime_limits = test_limits();
    let worker_limits = Arc::new(runtime_limits.worker.clone());
    Control {
        deadline_ms,
        cancel,
        validation_runtime: Arc::new(
            crate::native::ValidationRuntime::new(worker_limits.clone()).unwrap(),
        ),
        worker_limits,
        runtime_limits,
    }
}
#[cfg(test)]
mod native_usage_forwarding_tests {
    use super::*;
    use crate::native::{
        NativeObservation, NativeResult, ProviderTokenUsage, RequestExposure, UsageObservation,
    };
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use hyper::{
        Request as HttpRequest, Response as HttpResponse, body::Incoming, service::service_fn,
    };
    use hyper_util::rt::TokioIo;
    use std::{
        convert::Infallible,
        sync::Mutex,
        time::{SystemTime, UNIX_EPOCH},
    };

    type ReportHold = (
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    );
    struct Fixture {
        reporter: Reporter,
        reports: Arc<Mutex<Vec<Report>>>,
        fail_known: Arc<AtomicBool>,
        fail_all: Arc<AtomicBool>,
        hold_next: Arc<Mutex<Option<ReportHold>>>,
        server: tokio::task::JoinHandle<()>,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.server.abort();
        }
    }
    async fn fixture(label: &str) -> Fixture {
        static SEQUENCE: AtomicU64 = AtomicU64::new(1);
        let parent = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("artifacts/v2/build-60156282/worker/native-usage-forwarding-01/tests");
        std::fs::create_dir_all(&parent).unwrap();
        let root = parent.join(format!(
            "{label}-{}-{}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).expect("native usage worker fixture must be new");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let reports = Arc::new(Mutex::new(Vec::new()));
        let fail_known = Arc::new(AtomicBool::new(false));
        let fail_all = Arc::new(AtomicBool::new(false));
        let hold_next = Arc::new(Mutex::new(None::<ReportHold>));
        let holding = hold_next.clone();
        let (recorded, fail) = (reports.clone(), fail_known.clone());
        let reject_all = fail_all.clone();
        let server = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let (recorded, fail) = (recorded.clone(), fail.clone());
                let reject_all = reject_all.clone();
                let holding = holding.clone();
                connections.spawn(async move {
                    let service = service_fn(move |request: HttpRequest<Incoming>| {
                        let (recorded, fail) = (recorded.clone(), fail.clone());
                        let reject_all = reject_all.clone();
                        let holding = holding.clone();
                        async move {
                            let request: Request = serde_json::from_slice(
                                &request.into_body().collect().await.unwrap().to_bytes(),
                            )
                            .unwrap();
                            let Operation::ObserveWorker { report } = request.command else {
                                panic!("fixture accepts usage reports only")
                            };
                            let reject = reject_all.load(Ordering::SeqCst)
                                || (fail.load(Ordering::SeqCst)
                                    && matches!(
                                        report.observation,
                                        Observation::Usage {
                                            input_tokens: Some(_),
                                            ..
                                        }
                                    ));
                            let report_id = report.report_id.clone();
                            recorded.lock().unwrap().push(report);
                            let hold = holding.lock().unwrap().take();
                            if let Some((ready, resume)) = hold {
                                let _ = ready.send(());
                                let _ = resume.await;
                            }
                            if reject {
                                Ok::<_, Infallible>(
                                    HttpResponse::builder()
                                        .status(503)
                                        .body(Full::new(Bytes::new()))
                                        .unwrap(),
                                )
                            } else {
                                Ok(HttpResponse::new(Full::new(Bytes::from(
                                    serde_json::to_vec(&Reply {
                                        request_id: request.request_id,
                                        outcome: Outcome::Ok {
                                            data: Data::Acknowledged,
                                        },
                                        commit: Some(MutationCommit {
                                            operation_id: report_id,
                                            controller_revision: 1,
                                        }),
                                        observed_controller_revision: Some(1),
                                    })
                                    .unwrap(),
                                ))))
                            }
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        let reporter = Reporter {
            client: ApiClient::new(&url, test_limits()).unwrap(),
            resource: None,
            config: WorkerConfig {
                limits: test_limits(),
                resource_profile: None,
                controller_url: url,
                id: "usage-worker".into(),
                incarnation: "one".into(),
                workspace_root: root.clone(),
                codex_path: "/usr/bin/false".into(),
                coding_capacity: 0,
                native_capacity: 1,
                native_endpoint: "http://127.0.0.1:1/v1/responses".into(),
                validation_executable: std::env::current_exe().unwrap(),
            },
            attempt: Attempt {
                session_id: "session-1".into(),
                id: "usage-attempt".into(),
                task_id: "usage-task".into(),
                generation: 1,
                worker_id: "usage-worker".into(),
                worker_incarnation: "one".into(),
                phase: AttemptPhase::Running,
                deadline_unix_ms: process::now_ms() + 60_000,
                stopped: false,
                uncertain: false,
            },
            outbox: root,
            sequence: Arc::new(AtomicU64::new(1)),
            executor_seen: Arc::new(AtomicBool::new(false)),
            provider_uncertain: Arc::new(AtomicBool::new(false)),
            uncertainty_retained: Arc::new(AtomicBool::new(false)),
            output_limit: 4096,
            result_limit: test_limits().task_defaults.result_bytes,
            coding_limits: test_limits().execution.coding.clone(),
            outbox_lock: Arc::new(tokio::sync::Mutex::new(())),
        };
        assert!(
            reporter
                .send_acknowledged(Observation::Usage {
                    model_requests: 1,
                    input_tokens: None,
                    output_tokens: None
                })
                .await
                .unwrap()
        );
        Fixture {
            reporter,
            reports,
            fail_known,
            fail_all,
            hold_next,
            server,
        }
    }

    #[tokio::test]
    async fn reconnect_drain_waits_for_active_reporter_before_touching_outbox() {
        let f = fixture("reconnect-active-reporter").await;
        let (ready, held) = tokio::sync::oneshot::channel();
        let (release, resume) = tokio::sync::oneshot::channel();
        *f.hold_next.lock().unwrap() = Some((ready, resume));
        let reporter = f.reporter.clone();
        let active = tokio::spawn(async move {
            reporter
                .send_identified(
                    "reconnect-active-report".into(),
                    Observation::Progress {
                        text: "active executor report".into(),
                        output: String::new(),
                        truncated: false,
                    },
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), held)
            .await
            .unwrap()
            .unwrap();
        assert!(
            f.reporter.outbox_lock.try_lock().is_err(),
            "active reporter must own the mutex while its ACK is held"
        );
        let path = f
            .reporter
            .outbox
            .join(format!("{}.json", component("reconnect-active-report")));
        assert!(path.exists());
        let reporter = f.reporter.clone();
        let (entered, entering) = tokio::sync::oneshot::channel();
        let mut reconnect = tokio::spawn(async move {
            let _ = entered.send(());
            reports_ready_before_registration(
                &reporter.client,
                &reporter.outbox,
                &reporter.outbox_lock,
            )
            .await
        });
        entering.await.unwrap();
        // The competing drain is actively polled on its own task while the
        // first HTTP ACK is deliberately withheld. An unlocked preimage sends
        // the same report again and deletes the active reporter's file.
        let early = tokio::time::timeout(Duration::from_millis(500), &mut reconnect).await;
        let crossed_active_reporter = early.is_ok();
        let sends_while_held = f.reports.lock().unwrap().len();
        let _ = release.send(());
        let active_result = tokio::time::timeout(Duration::from_secs(2), active)
            .await
            .unwrap()
            .unwrap();
        let drain_result = match early {
            Ok(result) => result.unwrap(),
            Err(_) => tokio::time::timeout(Duration::from_secs(2), reconnect)
                .await
                .unwrap()
                .unwrap(),
        };
        assert!(
            !crossed_active_reporter,
            "reconnect drain bypassed the active reporter: {sends_while_held} observed sends; active result {active_result:?}; drain {drain_result:?}"
        );
        assert_eq!(
            sends_while_held, 2,
            "initial usage plus one active report only"
        );
        assert_eq!(active_result.unwrap(), true);
        drain_result.unwrap();
        assert!(!path.exists());
        assert_eq!(
            f.reports.lock().unwrap().len(),
            2,
            "reconnect must not replay an already-drained report"
        );
    }
    fn observation(success: bool, usage: UsageObservation) -> NativeObservation {
        NativeObservation {
            result: if success {
                Ok(NativeResult {
                    summary: "fixture".into(),
                    output: json!({"fixture":true}),
                    requests: 1,
                })
            } else {
                Err("native_schema_mismatch".into())
            },
            request_exposure: RequestExposure::HttpResponseReceived,
            provider_outcome: crate::native::ProviderOutcome::Completed,
            usage,
        }
    }
    fn known() -> UsageObservation {
        UsageObservation::Reported(ProviderTokenUsage {
            input_tokens: 17,
            output_tokens: 9,
            total_tokens: 26,
        })
    }
    #[tokio::test]
    async fn known_usage_is_forwarded_before_success_or_failure_without_double_charge() {
        for success in [true, false] {
            let f = fixture(if success { "success" } else { "failure" }).await;
            let result =
                report_native_observation(observation(success, known()), &f.reporter).await;
            assert_eq!(result.is_ok(), success);
            if let Err(error) = result {
                assert_eq!(error, "native_schema_mismatch");
            }
            let reports = f.reports.lock().unwrap();
            assert_eq!(reports.len(), 2);
            assert_eq!(
                reports[0].observation,
                Observation::Usage {
                    model_requests: 1,
                    input_tokens: None,
                    output_tokens: None
                }
            );
            assert_eq!(
                reports[1].observation,
                Observation::Usage {
                    model_requests: 1,
                    input_tokens: Some(17),
                    output_tokens: Some(9)
                }
            );
            assert_ne!(reports[0].report_id, reports[1].report_id);
        }
    }
    #[tokio::test]
    async fn unknown_or_invalid_usage_does_not_emit_zeros_or_change_failure() {
        for usage in [UsageObservation::Unknown, UsageObservation::Invalid] {
            let f = fixture("unknown-invalid").await;
            assert!(
                matches!(report_native_observation(observation(false,usage),&f.reporter).await,Err(error) if error=="native_schema_mismatch")
            );
            let reports = f.reports.lock().unwrap();
            assert_eq!(reports.len(), 1);
            assert_eq!(
                reports[0].observation,
                Observation::Usage {
                    model_requests: 1,
                    input_tokens: None,
                    output_tokens: None
                }
            );
        }
    }
    #[tokio::test]
    async fn known_usage_survives_failed_result_and_outbox_replay_keeps_identity() {
        let f = fixture("replay").await;
        f.fail_known.store(true, Ordering::SeqCst);
        assert!(
            matches!(report_native_observation(observation(false,known()),&f.reporter).await,Err(error) if error=="native_schema_mismatch")
        );
        let pending = std::fs::read_dir(&f.reporter.outbox)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("json"))
            .collect::<Vec<_>>();
        assert_eq!(pending.len(), 1);
        let saved: Report = serde_json::from_slice(&std::fs::read(&pending[0]).unwrap()).unwrap();
        f.fail_known.store(false, Ordering::SeqCst);
        flush_outbox(&f.reporter.client, &f.reporter.outbox)
            .await
            .unwrap();
        assert!(!pending[0].exists());
        let reports = f.reports.lock().unwrap();
        assert_eq!(reports.len(), 3);
        assert_eq!(reports[1], saved);
        assert_eq!(reports[2], saved);
        assert_eq!(
            saved.observation,
            Observation::Usage {
                model_requests: 1,
                input_tokens: Some(17),
                output_tokens: Some(9)
            }
        );
    }
    #[tokio::test]
    async fn usage_record_failure_does_not_erase_cleanup_uncertainty() {
        let f = fixture("outbox-failure").await;
        let mut reporter = f.reporter.clone();
        reporter.outbox = reporter.outbox.join("not-a-directory");
        std::fs::write(&reporter.outbox, b"fixture").unwrap();
        let observed = NativeObservation {
            result: Err("native_validation_cleanup_unverified".into()),
            request_exposure: RequestExposure::HttpResponseReceived,
            provider_outcome: crate::native::ProviderOutcome::Completed,
            usage: known(),
        };
        assert!(
            matches!(report_native_observation(observed,&reporter).await,Err(error) if error.contains("native_validation_cleanup_unverified")&&error.contains("native usage recording failed"))
        );
        assert_eq!(f.reports.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn task_recovery_unknown_outcome_survives_failed_uncertainty_write() {
        let f = fixture("unknown-write-failure").await;
        f.reporter.provider_uncertain.store(true, Ordering::Relaxed);
        let sequence = f.reporter.sequence.load(Ordering::Relaxed);
        let id = format!("{}-report-{sequence:020}", f.reporter.attempt.id);
        let blocked = f
            .reporter
            .outbox
            .join(format!("{}.json", component(&id)))
            .with_extension("pending");
        // Only the first uncertainty write fails. Other report paths remain
        // writable, so sending stop/result afterward would expose the defect.
        std::fs::create_dir(&blocked).unwrap();
        let ready =
            report_attempt_failure(&f.reporter, "native_response_incomplete".into(), true, true)
                .await;
        assert!(!ready);
        assert!(!f.reporter.uncertainty_retained.load(Ordering::Relaxed));
        let result: Result<AttemptExit, AttemptFailure> = Err("native_response_incomplete".into());
        assert!(
            !(ready && receipt_completed(&result, true)),
            "launch must remain incomplete"
        );
        assert_eq!(f.reports.lock().unwrap().len(), 1);
        assert_eq!(std::fs::read_dir(&f.reporter.outbox).unwrap().count(), 1);
        std::fs::remove_dir(blocked).unwrap();
        assert!(
            report_attempt_failure(&f.reporter, "native_response_incomplete".into(), true, true)
                .await
        );
        let reports = f.reports.lock().unwrap();
        assert!(matches!(
            reports[1].observation,
            Observation::ExecutionUncertain { .. }
        ));
        assert!(matches!(
            reports[3].observation,
            Observation::ProcessTreeStopped
        ));
    }

    #[tokio::test]
    async fn task_recovery_provider_disposition_is_not_inferred_from_error_text() {
        use crate::native::ProviderOutcome;
        for outcome in [
            ProviderOutcome::NotStarted,
            ProviderOutcome::Unknown,
            ProviderOutcome::Completed,
        ] {
            let f = fixture("provider-disposition").await;
            let observed = NativeObservation {
                result: Err("native_output_schema_mismatch".into()),
                request_exposure: if outcome == ProviderOutcome::NotStarted {
                    RequestExposure::NotStarted
                } else {
                    RequestExposure::HttpResponseReceived
                },
                provider_outcome: outcome,
                usage: UsageObservation::Unknown,
            };
            assert_eq!(
                report_native_observation(observed, &f.reporter)
                    .await
                    .err()
                    .as_deref(),
                Some("native_output_schema_mismatch")
            );
            assert_eq!(
                f.reporter.provider_uncertain.load(Ordering::Relaxed),
                outcome == ProviderOutcome::Unknown
            );
        }
    }

    fn checkpoint() -> Checkpoint {
        Checkpoint {
            registration: crate::repository_catalogue::Revision { repository_id: "fixture-repository".into(), registration_revision: "fixture-revision".into() },
            repository: "https://example.invalid/repository".into(),
            commit: "a".repeat(40),
            remote_ref: "refs/borg/attempt".into(),
            operation_id: "checkpoint-operation".into(),
        }
    }

    fn candidate(output: serde_json::Value) -> Observation {
        Observation::ResultCandidate {
            checks_passed: true,
            summary: "completed".into(),
            checkpoint: Some(checkpoint()),
            validation: vec!["check passed".into()],
            output: Some(output),
        }
    }

    // Exact synthetic configured values only; no environment mutation, provider
    // request, opaque-executor claim or complete credential-disclosure proof.
    #[tokio::test]
    async fn credential_diagnostics_are_redacted_before_outbox_and_replay() {
        let f = fixture("credential-diagnostics").await;
        f.fail_all.store(true, Ordering::SeqCst);
        let secret = "fixture-secret-diagnostic-12345".to_string();
        let credentials = vec![secret.clone()];
        let clean = json!({"nested":[{"answer":42}],"unconfigured":"unknown-token-value"});
        let observation = Observation::ResultCandidate {
            checks_passed: true,
            summary: format!("completed {secret}"),
            checkpoint: Some(checkpoint()),
            validation: vec![format!("check used {secret} and passed")],
            output: Some(clean.clone()),
        };
        assert!(
            !f.reporter
                .send_identified_with_credentials(
                    "credential-diagnostic-report".into(),
                    observation.clone(),
                    &credentials,
                )
                .await
                .unwrap()
        );
        let path = f.reporter.outbox.join(format!(
            "{}.json",
            component("credential-diagnostic-report")
        ));
        let bytes = std::fs::read(&path).unwrap();
        assert!(!String::from_utf8_lossy(&bytes).contains(&secret));
        let saved: Report = serde_json::from_slice(&bytes).unwrap();
        assert!(matches!(&saved.observation, Observation::ResultCandidate {
            checks_passed:true, summary, checkpoint:Some(source), validation, output:Some(value)
        } if summary=="completed [credential redacted]"
            && validation==&vec!["check used [credential redacted] and passed".to_string()]
            && source==&checkpoint() && value==&clean));
        // Same sanitized body/identity can be replayed; different clean output
        // under that pending identity still refuses without replacing its bytes.
        assert!(
            !f.reporter
                .send_identified_with_credentials(
                    saved.report_id.clone(),
                    observation,
                    &credentials,
                )
                .await
                .unwrap()
        );
        assert!(
            f.reporter
                .send_identified_with_credentials(
                    saved.report_id.clone(),
                    candidate(json!({"different":true})),
                    &credentials,
                )
                .await
                .unwrap_err()
                .contains("changed body")
        );
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        f.fail_all.store(false, Ordering::SeqCst);
        flush_outbox_with_credentials(&f.reporter.client, &f.reporter.outbox, &credentials)
            .await
            .unwrap();
        assert!(!path.exists());
        let reports = f.reports.lock().unwrap();
        assert_eq!(reports.len(), 4);
        assert!(reports[1..].iter().all(|report| report == &saved));
        assert!(matches!(
            reports[0].observation,
            Observation::Usage {
                model_requests: 1,
                input_tokens: None,
                output_tokens: None
            }
        ));
    }

    #[tokio::test]
    async fn credential_output_keys_values_and_checkpoint_identities_fail_without_rewriting() {
        let f = fixture("credential-structured").await;
        let secret = "fixture-secret-structured-12345".to_string();
        let credentials = vec![secret.clone()];
        let mut keyed = serde_json::Map::new();
        keyed.insert(format!("key-{secret}"), json!(true));
        let mut cases = vec![
            candidate(json!({"nested":[{"value":format!("prefix-{secret}-suffix")}]})),
            candidate(json!({"nested":[serde_json::Value::Object(keyed)]})),
        ];
        for field in 0..4 {
            let mut source = checkpoint();
            match field {
                0 => source.repository = format!("https://{secret}@example.invalid/repository"),
                1 => source.commit = secret.clone(),
                2 => source.remote_ref = format!("refs/{secret}"),
                _ => source.operation_id = secret.clone(),
            }
            cases.push(Observation::ResultCandidate {
                checks_passed: field != 3,
                summary: "original outcome".into(),
                checkpoint: Some(source),
                validation: vec!["check passed".into()],
                output: Some(json!({"answer":42})),
            });
        }
        for (index, observation) in cases.into_iter().enumerate() {
            assert!(
                f.reporter
                    .send_identified_with_credentials(
                        format!("credential-structured-{index}"),
                        observation,
                        &credentials,
                    )
                    .await
                    .unwrap()
            );
        }
        let reports = f.reports.lock().unwrap();
        assert_eq!(reports.len(), 7);
        for report in &reports[1..] {
            assert!(matches!(&report.observation, Observation::ResultCandidate {
                checks_passed:false, summary, checkpoint:None, validation, output:None
            } if summary.contains("result withheld") && validation.is_empty()));
            assert!(!serde_json::to_string(report).unwrap().contains(&secret));
        }
        assert!(matches!(
            reports[0].observation,
            Observation::Usage {
                model_requests: 1,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn credential_identities_and_old_outbox_refuse_without_write_or_http() {
        let f = fixture("credential-identities").await;
        let secret = "fixture-secret-identity-12345".to_string();
        let credentials = vec![secret.clone()];
        for field in 0..7 {
            let mut reporter = f.reporter.clone();
            let mut id = "identity-report".to_string();
            let mut observation = Observation::ProcessTreeStopped;
            match field {
                0 => id = secret.clone(),
                1 => reporter.config.id = secret.clone(),
                2 => reporter.config.incarnation = secret.clone(),
                3 => reporter.attempt.task_id = secret.clone(),
                4 => reporter.attempt.id = secret.clone(),
                5 => {
                    observation = Observation::Started {
                        thread_id: Some(secret.clone()),
                        turn_id: None,
                    }
                }
                _ => {
                    observation = Observation::Started {
                        thread_id: None,
                        turn_id: Some(secret.clone()),
                    }
                }
            }
            let error = reporter
                .send_identified_with_credentials(id, observation, &credentials)
                .await
                .unwrap_err();
            assert!(error.contains("identity") && !error.contains(&secret));
        }
        assert_eq!(std::fs::read_dir(&f.reporter.outbox).unwrap().count(), 0);
        assert_eq!(f.reports.lock().unwrap().len(), 1);
        let old = Report {
            report_id: "old-report".into(),
            worker_id: "usage-worker".into(),
            incarnation: "one".into(),
            task_id: "usage-task".into(),
            attempt_id: "usage-attempt".into(),
            generation: 1,
            observation: candidate(json!({"old":secret})),
        };
        let path = f.reporter.outbox.join("old-report.json");
        let original = serde_json::to_vec(&old).unwrap();
        std::fs::write(&path, &original).unwrap();
        let error =
            flush_outbox_with_credentials(&f.reporter.client, &f.reporter.outbox, &credentials)
                .await
                .unwrap_err();
        assert!(error.contains("replay withheld") && !error.contains(&secret));
        assert_eq!(std::fs::read(&path).unwrap(), original);
        assert_eq!(f.reports.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn credential_inspection_is_bounded_without_recursion_or_successful_mutation() {
        let f = fixture("credential-inspection-bound").await;
        let credentials = vec!["fixture-secret-bound-12345".into()];
        let mut deep = json!(true);
        for _ in 0..100 {
            deep = json!([deep]);
        }
        for (index, output) in [
            deep,
            json!("x".repeat(test_limits().worker.credential_scan_units + 1)),
        ]
        .into_iter()
        .enumerate()
        {
            assert!(
                f.reporter
                    .send_identified_with_credentials(
                        format!("bounded-inspection-{index}"),
                        candidate(output),
                        &credentials,
                    )
                    .await
                    .unwrap()
            );
        }
        assert!(f.reports.lock().unwrap()[1..].iter().all(|report| matches!(
            &report.observation, Observation::ResultCandidate {
                checks_passed:false, summary, output:None, checkpoint:None, ..
            } if summary.contains("bounded credential inspection")
        )));
        let numeric = vec!["123456789".to_string()];
        assert_eq!(
            value_contains_credential(&json!({"n":123456789}), &numeric, &test_limits().worker),
            Ok(true)
        );
        assert_eq!(
            scrub_known("credential".into(), 1024, &["credential".into()]),
            ""
        );
        assert_eq!(
            scrub_known("αβ-secret-long".into(), 3, &["secret-long".into()]),
            "α"
        );
    }

    #[tokio::test]
    async fn progress_and_uncertainty_use_accepted_character_and_byte_bounds() {
        let f = fixture("progress-character-units").await;
        let cases = [
            (4096, 16384, "🙂".repeat(4097), "🙂".repeat(4096), vec![]),
            (4096, 16384, "a".repeat(4097), "a".repeat(4096), vec![]),
            (5, 20, "🙂".repeat(6), "🙂".repeat(5), vec![]),
            (5, 20, "a".repeat(6), "a".repeat(5), vec![]),
            (8, 7, "🙂".repeat(3), "🙂".into(), vec![]),
            (8, 7, "a".repeat(8), "a".repeat(7), vec![]),
            (32, 128, "🙂secret🙂尾".into(), "🙂[credential redacted]尾".into(), vec!["secret🙂".into()]),
            (2, 128, "🙂secret🙂尾".into(), "🙂[".into(), vec!["secret🙂".into()]),
        ];
        for (index, (characters, bytes, text, expected, credentials)) in cases.into_iter().enumerate() {
            let mut global = (*test_limits()).clone();
            global.controller.max_progress_bytes = bytes;
            let mut coding = global.execution.coding.clone();
            coding.progress_chars = characters;
            let mut input: TaskInput = serde_json::from_value(json!({
                "instruction": "progress metadata fixture",
                "definition": {
                    "id": "progress-agent", "revision": "r1", "executor": "coding",
                    "inference": {"model": "gpt-6-astra", "reasoning_effort": "max"},
                    "execution_profile": {"kind": "collaboration_v1"},
                    "limit_overrides": {"coding": coding}
                },
                "repository": null, "acceptance_commands": [], "output_schema": null,
                "limits": global.task_defaults.at(process::now_ms()).unwrap(),
                "dependencies": []
            })).unwrap();
            // Bind this transport fixture exactly as run() consumes a controller-
            // resolved snapshot. It is not a separate admission qualification.
            input.resolved_limits = Some(global.resolve_task(&input).unwrap());
            let snapshot = input.resolved_limits.as_ref().unwrap();
            let mut reporter = f.reporter.clone();
            reporter.coding_limits = input.execution_limits().unwrap().coding.clone();
            reporter.client = ApiClient::new(
                &reporter.config.controller_url,
                Arc::new((*snapshot.global_metadata).clone()),
            ).unwrap();
            reporter.output_limit = 5;
            // Startup settings deliberately retain the seed; task diagnostics
            // must use the accepted byte/character policy instead.
            assert_eq!(reporter.config.limits.as_ref(), test_limits().as_ref());
            for uncertain in [false, true] {
                let observation = if uncertain {
                    Observation::ExecutionUncertain { message: text.clone() }
                } else {
                    Observation::Progress { text: text.clone(), output: "ééé".into(), truncated: false }
                };
                assert!(reporter.send_identified_with_credentials(
                    format!("character-units-{index}-{uncertain}"), observation, &credentials,
                ).await.unwrap());
                let reports = f.reports.lock().unwrap();
                let reported = match &reports.last().unwrap().observation {
                    Observation::Progress { text, output, truncated } => {
                        assert_eq!(output, "éé");
                        assert!(*truncated);
                        text
                    }
                    Observation::ExecutionUncertain { message } => message,
                    other => panic!("unexpected report: {other:?}"),
                };
                assert_eq!(reported, &expected);
                assert!(reported.chars().count() <= characters);
                assert!(reported.len() <= bytes);
                assert!(!contains_credential(reported, &credentials));
            }
        }
    }

    #[test]
    fn proxy_credential_collection_is_finite_decoded_and_does_not_change_routing() {
        let proxy = "http://proxy-user-fixture:p%40ss%3Aword+%CE%B1@proxy.invalid:8080";
        for name in [
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
        ] {
            let mut read = Vec::new();
            let credentials = configured_credentials_from(&test_limits(), |requested| {
                read.push(requested.to_owned());
                Ok((requested == name).then(|| proxy.to_owned()))
            })
            .unwrap();
            assert_eq!(read.len(), 10);
            assert!(!read.iter().any(|name| name == "UNRELATED_SECRET"));
            for spelling in [
                proxy,
                "proxy-user-fixture:p%40ss%3Aword+%CE%B1",
                "proxy-user-fixture:p@ss:word+α",
                "p%40ss%3Aword+%CE%B1",
                "p@ss:word+α",
            ] {
                assert!(credentials.iter().any(|value| value == spelling));
                assert!(
                    !scrub_known(format!("output {spelling}"), 4096, &credentials)
                        .contains(spelling)
                );
            }
            assert!(!credentials.iter().any(|value| value == "p@ss:word α"));
            assert_eq!(
                proxy,
                "http://proxy-user-fixture:p%40ss%3Aword+%CE%B1@proxy.invalid:8080"
            );
        }
        for (proxy, secret) in [
            (
                "https://proxy-token-fixture@proxy.invalid",
                "proxy-token-fixture",
            ),
            (
                "socks5://:password-fixture@proxy.invalid:1080",
                "password-fixture",
            ),
            (
                "proxy-user-fixture:password-fixture@proxy.invalid:8080",
                "password-fixture",
            ),
            (
                "http://proxy-user-fixture:raw-α:password@proxy.invalid",
                "raw-α:password",
            ),
        ] {
            let credentials = configured_credentials_from(&test_limits(), |name| {
                Ok((name == "ALL_PROXY").then(|| proxy.into()))
            })
            .unwrap();
            assert!(credentials.iter().any(|value| value == proxy));
            assert!(credentials.iter().any(|value| value == secret));
        }
        let clean = configured_credentials_from(&test_limits(), |name| {
            Ok((name == "HTTPS_PROXY").then(|| "https://proxy.invalid:8080".into()))
        })
        .unwrap();
        assert!(clean.is_empty());
        let malformed = "https://proxy-user:password@[invalid";
        let exact = configured_credentials_from(&test_limits(), |name| {
            Ok((name == "https_proxy").then(|| malformed.into()))
        })
        .unwrap();
        assert_eq!(exact, vec![malformed]);
    }

    #[test]
    fn named_credential_collection_refuses_uninspectable_and_overbound_inputs() {
        for invalid_name in [
            "OPENAI_API_KEY",
            "CODEX_API_KEY",
            "GITHUB_TOKEN",
            "GH_TOKEN",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
        ] {
            let error = configured_credentials_from(&test_limits(), |name| {
                if name == invalid_name {
                    Err(())
                } else {
                    Ok(None)
                }
            })
            .unwrap_err();
            assert!(error.contains("not valid Unicode"));
        }
        let error = configured_credentials_from(&test_limits(), |name| {
            Ok((name == "OPENAI_API_KEY")
                .then(|| "x".repeat(test_limits().transport.body_bytes + 1)))
        })
        .unwrap_err();
        assert_eq!(error, "configured credential inspection exceeds bound (worker.credential_scan_units)");
        let error = configured_credentials_from(&test_limits(), |name| {
            Ok((name == "HTTPS_PROXY").then(|| "x".repeat(test_limits().transport.body_bytes + 1)))
        })
        .unwrap_err();
        assert_eq!(error, "configured proxy inspection exceeds bound (transport.body_bytes)");
    }

    #[tokio::test]
    async fn proxy_credentials_are_guarded_in_actual_report_and_retained_outbox_paths() {
        let f = fixture("proxy-credential-paths").await;
        let proxy = "https://proxy-fixture-user:proxy%2Dfixture%2Dpassword@proxy.invalid:8443";
        let password = "proxy-fixture-password";
        let credentials = configured_credentials_from(&test_limits(), |name| {
            Ok((name == "https_proxy").then(|| proxy.into()))
        })
        .unwrap();
        f.fail_all.store(true, Ordering::SeqCst);
        for (id, observation) in [
            (
                "proxy-progress",
                Observation::Progress {
                    text: format!("using {proxy}"),
                    output: format!("password {password}"),
                    truncated: false,
                },
            ),
            (
                "proxy-validation",
                Observation::ResultCandidate {
                    checks_passed: true,
                    summary: "complete".into(),
                    checkpoint: None,
                    validation: vec![format!("checked {password}")],
                    output: Some(json!({"answer":42})),
                },
            ),
            ("proxy-result", candidate(json!({(password): "output"}))),
        ] {
            assert!(
                !f.reporter
                    .send_identified_with_credentials(id.into(), observation, &credentials)
                    .await
                    .unwrap()
            );
            let bytes =
                std::fs::read(f.reporter.outbox.join(format!("{}.json", component(id)))).unwrap();
            let saved: Report = serde_json::from_slice(&bytes).unwrap();
            assert!(
                !report_contains_credential(&saved, &credentials, &test_limits().worker).unwrap()
            );
            if id == "proxy-validation" {
                assert!(
                    matches!(&saved.observation, Observation::ResultCandidate {checks_passed:true, output:Some(value),validation,..} if value == &json!({"answer":42}) && validation == &["checked [credential redacted]".to_string()])
                );
            }
            if id == "proxy-result" {
                assert!(matches!(
                    saved.observation,
                    Observation::ResultCandidate {
                        checks_passed: false,
                        output: None,
                        checkpoint: None,
                        ..
                    }
                ));
            }
        }
        f.fail_all.store(false, Ordering::SeqCst);
        flush_outbox_with_credentials(&f.reporter.client, &f.reporter.outbox, &credentials)
            .await
            .unwrap();
        assert_eq!(std::fs::read_dir(&f.reporter.outbox).unwrap().count(), 0);
        let before = f.reports.lock().unwrap().len();
        let old = Report {
            report_id: "old-proxy-report".into(),
            worker_id: "usage-worker".into(),
            incarnation: "one".into(),
            task_id: "usage-task".into(),
            attempt_id: "usage-attempt".into(),
            generation: 1,
            observation: Observation::Progress {
                text: "old".into(),
                output: proxy.into(),
                truncated: false,
            },
        };
        let path = f.reporter.outbox.join("old-proxy-report.json");
        let original = serde_json::to_vec(&old).unwrap();
        std::fs::write(&path, &original).unwrap();
        let error =
            flush_outbox_with_credentials(&f.reporter.client, &f.reporter.outbox, &credentials)
                .await
                .unwrap_err();
        assert!(error.contains("replay withheld") && !error.contains(password));
        assert_eq!(std::fs::read(path).unwrap(), original);
        assert_eq!(f.reports.lock().unwrap().len(), before);
    }

    #[tokio::test]
    async fn tracked_checkpoint_failure_preserves_independent_unknown_and_cleanup_barriers() {
        use crate::native::ProviderOutcome;
        for mode in ["tracked", "executor", "provider", "unverified", "cleanup"] {
            let f = fixture(&format!("checkpoint-classification-{mode}")).await;
            f.reporter.executor_seen.store(true, Ordering::Relaxed);
            if mode == "provider" {
                let observed = NativeObservation {
                    result: Err("native_response_incomplete".into()),
                    request_exposure: RequestExposure::HttpResponseReceived,
                    provider_outcome: ProviderOutcome::Unknown,
                    usage: UsageObservation::Unknown,
                };
                assert!(
                    report_native_observation(observed, &f.reporter)
                        .await
                        .is_err()
                );
            }
            let failure=match mode {
                "executor"=>AttemptFailure::Execution("executor task failed; outcome uncertain".into()),
                "unverified"=>AttemptFailure::TrackedCheckpoint("checkpoint cleanup unverified".into()),
                _=>AttemptFailure::TrackedCheckpoint("checkpoint publication outcome uncertain; reconcile the original ref before retry".into()),
            };
            let cleanup_verified = mode != "cleanup" && mode != "unverified";
            let uncertain =
                failure.execution_uncertain(f.reporter.provider_uncertain.load(Ordering::Relaxed));
            assert!(
                report_attempt_failure(
                    &f.reporter,
                    failure.message().into(),
                    cleanup_verified,
                    uncertain
                )
                .await
            );
            let reports = f.reports.lock().unwrap();
            let uncertainty = reports
                .iter()
                .position(|r| matches!(r.observation, Observation::ExecutionUncertain { .. }));
            assert_eq!(uncertainty.is_some(), mode != "tracked", "{mode}");
            assert_eq!(
                reports
                    .iter()
                    .any(|r| matches!(r.observation, Observation::ProcessTreeStopped)),
                cleanup_verified,
                "{mode}"
            );
            if let Some(index) = uncertainty {
                let terminal = reports
                    .iter()
                    .position(|report| {
                        matches!(
                            report.observation,
                            Observation::ExecutorOutcome { .. }
                                | Observation::ProcessTreeStopped
                                | Observation::ResultCandidate { .. }
                        )
                    })
                    .expect("failure retains a terminal report");
                assert!(
                    index < terminal,
                    "uncertainty must precede terminal reports"
                );
            }
            assert!(reports.iter().any(|r| matches!(
                r.observation,
                Observation::ResultCandidate {
                    checks_passed: false,
                    ..
                }
            )));
            let result = Err(failure);
            assert_eq!(
                receipt_completed(&result, cleanup_verified),
                cleanup_verified,
                "{mode}"
            );
        }
    }
}

#[cfg(test)]
mod template_inference_tests;

#[cfg(test)]
mod limits_policy_tests {
    use super::*;
    use std::os::unix::fs::symlink;
    struct Area(PathBuf);
    impl Area {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(1);
            let root = std::env::temp_dir().join(format!(
                "borg-worker-limit-policy-{}-{}-{}",
                std::process::id(),
                process::now_ms(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            for name in [
                "commands",
                "outbox",
                "processes",
                "codex",
                "mutations/settlements",
                "native-tracking",
                "publications",
            ] {
                std::fs::create_dir_all(root.join(name)).unwrap();
            }
            Self(root)
        }
    }
    impl Drop for Area {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    #[test]
    // Traceability scope: Lower selected outbox batch metadata preserves the retained worker policy and existing pending evidence bytes.
    //= spec/impl/v2/implementation.md#limits-are-versioned-metadata
    //= type=test
    //# A retained operation MUST NOT be discarded solely because a later limit configuration is lower.
    fn retained_policy_survives_selected_drift_without_rewriting_evidence() {
        let area = Area::new();
        let first = test_limits();
        let _lease =
            process::WorkerLease::claim(&area.0.join("owner.json"), &first.worker).unwrap();
        assert_eq!(*retained_worker_limits(&area.0, &first).unwrap(), *first);
        let original = std::fs::read(area.0.join("limits.json")).unwrap();
        std::fs::write(
            area.0.join("outbox/pending.json"),
            b"retained unresolved observation",
        )
        .unwrap();
        let mut next = (*first).clone();
        next.revision.push_str("-changed");
        next.worker.outbox_batch = 1;
        next.validate().unwrap();
        let retained = retained_worker_limits(&area.0, &Arc::new(next.clone())).unwrap();
        assert_eq!(*retained, *first);
        assert_ne!(*retained, next);
        assert_eq!(std::fs::read(area.0.join("limits.json")).unwrap(), original);
        assert_eq!(
            std::fs::read(area.0.join("outbox/pending.json")).unwrap(),
            b"retained unresolved observation"
        );
    }
    #[test]
    fn missing_or_aliased_binding_never_adopts_nonempty_journals() {
        let area = Area::new();
        let limits = test_limits();
        std::fs::write(area.0.join("outbox/pending.json"), b"original").unwrap();
        assert!(retained_worker_limits(&area.0, &limits).is_err());
        assert!(!area.0.join("limits.json").exists());
        process::atomic_json(&area.0.join("other.json"), &*limits).unwrap();
        symlink(area.0.join("other.json"), area.0.join("limits.json")).unwrap();
        assert!(retained_worker_limits(&area.0, &limits).is_err());
        assert_eq!(
            std::fs::read(area.0.join("outbox/pending.json")).unwrap(),
            b"original"
        );
    }
    #[test]
    fn credential_scan_uses_selected_budget_and_depth() {
        let mut limits = test_limits().worker.clone();
        let value = json!({"nested":[{"safe":"value"}]});
        limits.credential_scan_units = 1;
        assert!(value_contains_credential(&value, &[], &limits).is_err());
        limits.credential_scan_units = 100;
        limits.credential_scan_depth = 1;
        assert!(value_contains_credential(&value, &[], &limits).is_err());
        limits.credential_scan_depth = 4;
        assert_eq!(value_contains_credential(&value, &[], &limits), Ok(false));
        assert_eq!(
            value_contains_credential(&value, &["value".into()], &limits),
            Ok(true)
        );
    }
}
