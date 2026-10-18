//! A completed native decision is replayed as one immutable controller request.
use super::*;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Saved {
    format_revision: u32,
    attempt: Attempt,
    instruction: String,
    request: Operation,
    outcome: Option<Outcome>,
    outcome_context: Option<CommitContext>,
    local_not_sent: bool,
    finished: bool,
}

pub(super) fn path(state: &Path, attempt: &Attempt) -> PathBuf {
    state
        .join("delegations")
        .join(format!("{}.json", component(&attempt.id)))
}

fn request(attempt: &Attempt, input: &TaskInput, instruction: &str) -> Result<Operation, String> {
    Ok(Operation::DelegateTask {
        operation_id: format!("delegate-{}", attempt.id),
        parent_task_id: attempt.task_id.clone(),
        parent_attempt_id: attempt.id.clone(),
        generation: attempt.generation,
        child: crate::delegation::coding_child(input, instruction)?,
    })
}

fn load(path: &Path, attempt: &Attempt, input: &TaskInput) -> Result<Saved, String> {
    let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
    if bytes.len()
        > input
            .resolved_limits
            .as_ref()
            .ok_or("missing limits snapshot")?
            .global_metadata
            .worker
            .command_record_bytes
    {
        return Err("delegation receipt exceeds bound (worker.command_record_bytes)".into());
    }
    let saved: Saved = serde_json::from_slice(&bytes).map_err(|_| "invalid delegation receipt")?;
    if saved.format_revision != 3
        || saved.attempt != *attempt
        || serde_json::to_value(&saved.request).map_err(|e| e.to_string())?
            != serde_json::to_value(request(attempt, input, &saved.instruction)?)
                .map_err(|e| e.to_string())?
    {
        return Err("delegation receipt identity or immutable template mismatch".into());
    }
    match (&saved.outcome, &saved.outcome_context, saved.local_not_sent) {
        (None, None, false) => {}
        (Some(Outcome::Refused { code, message }), None, true)
            if code == "parent_not_current"
                && message == "No new child requested for an inactive parent" => {}
        (Some(outcome), Some(context), false) => context.validate(&saved.request, outcome)?,
        _ => return Err("delegation decision lacks valid committed or local context".into()),
    }
    Ok(saved)
}

// The original command inventory bounds this scan; load validates the saved
// operation, accepted input and committed outcome before marker admission.
pub(super) fn admit_retained_outcomes(state: &Path, limits: &RuntimeLimits) -> Result<(), String> {
    for (_, receipt) in command_receipts(state, limits)? {
        let WorkerCommand::RunAttempt { attempt, task, .. } = &receipt.command else { continue; };
        let path = path(state, attempt);
        if path.exists() {
            let saved = load(&path, attempt, task)?;
            if saved.outcome.as_ref().is_some_and(super::outcome_requires_worker18) {
                return Err("pending_assignment or reassignment in retained delegation outcome requires worker18".into());
            }
        }
    }
    Ok(())
}

pub(super) fn admit_retained_identity(state:&Path,limits:&RuntimeLimits)->Result<(),String> {
    for (_,receipt) in command_receipts(state,limits)? {
        let WorkerCommand::RunAttempt{attempt,task,..}=&receipt.command else{continue};
        let path=path(state,attempt);
        if path.exists() {
            let saved=load(&path,attempt,task)?;
            if crate::protocol::requires_wire24(&saved.request)||saved.outcome.as_ref().is_some_and(crate::protocol::outcome_requires_wire24) {return Err("retained handoff identity/task vocabulary requires worker19".into())}
        }
    }
    Ok(())
}
fn record(
    state: &Path,
    attempt: &Attempt,
    input: &TaskInput,
    instruction: &str,
) -> Result<PathBuf, String> {
    let path = path(state, attempt);
    let new = request(attempt, input, instruction)?;
    if path.exists() {
        let old = load(&path, attempt, input)?;
        if serde_json::to_value(&old.request).map_err(|e| e.to_string())?
            != serde_json::to_value(&new).map_err(|e| e.to_string())?
        {
            return Err("delegation identity has changed body".into());
        }
        return Ok(path);
    }
    let saved = Saved {
        format_revision: 3,
        attempt: attempt.clone(),
        instruction: instruction.into(),
        request: new,
        outcome: None,
        outcome_context: None,
        local_not_sent: false,
        finished: false,
    };
    bounded_json_size(
        &saved,
        input
            .resolved_limits
            .as_ref()
            .ok_or("missing limits snapshot")?
            .global_metadata
            .worker
            .command_record_bytes,
    ).map_err(|reason| format!("{reason} (worker.command_record_bytes)"))?;
    atomic_json(&path, &saved)?;
    Ok(path)
}

pub(super) async fn begin(
    state: &Path,
    input: &TaskInput,
    reporter: &Reporter,
    instruction: &str,
) -> Result<AttemptExit, String> {
    // An existing decision may already have a child. A changed/corrupt replay
    // must not flow into generic parent failure, even when it cannot be read.
    let existing = path(state, &reporter.attempt).exists();
    if let Err(error) = record(state, &reporter.attempt, input, instruction) {
        if !existing {
            return Err(error);
        }
        let _ = reporter
            .send(Observation::Progress {
                text: "Delegation requires reconciliation".into(),
                output: scrub(error, reporter.output_limit, &reporter.config.limits),
                truncated: false,
            })
            .await;
        return Ok(AttemptExit::DelegationPending);
    }
    // After persistence, any transport/local reconciliation error is a distinct
    // pending handoff, never a generic failed parent result.
    match reconcile(state, input, reporter).await {
        Ok(exit) => Ok(exit),
        Err(error) => {
            let _ = reporter
                .send(Observation::Progress {
                    text: "Delegation requires reconciliation".into(),
                    output: scrub(error, reporter.output_limit, &reporter.config.limits),
                    truncated: false,
                })
                .await;
            Ok(AttemptExit::DelegationPending)
        }
    }
}

async fn call(client: &ApiClient, op: Operation) -> Result<Outcome, String> {
    if op.mutation_id().is_some() {
        return Err("mutation call requires committed context".into());
    }
    tokio::time::timeout(
        Duration::from_millis(client.limits().worker.handoff_call_timeout_ms),
        client.call(op),
    )
    .await
    .map_err(|_| "delegation controller response unknown")?
    .map(|reply| reply.outcome)
}

fn verify_outcome(saved: &Saved, outcome: &Outcome) -> Result<(), String> {
    match outcome {
        Outcome::Ok {
            data: Data::Task(child),
        } => {
            let Operation::DelegateTask {
                child: expected, ..
            } = &saved.request
            else {
                return Err("invalid delegation operation".into());
            };
            if child.parent_task_id.as_deref() != Some(saved.attempt.task_id.as_str())
                || child.input != *expected
            {
                return Err("delegation child does not match immutable request".into());
            }
            Ok(())
        }
        Outcome::Refused { code, .. }
            if !matches!(code.as_str(), "operation_conflict" | "store_unavailable") =>
        {
            Ok(())
        }
        _ => Err("delegation outcome remains unresolved".into()),
    }
}

pub(super) async fn reconcile(
    state: &Path,
    input: &TaskInput,
    reporter: &Reporter,
) -> Result<AttemptExit, String> {
    let path = path(state, &reporter.attempt);
    let mut saved = load(&path, &reporter.attempt, input)?;
    if saved.finished {
        return Ok(AttemptExit::Finished);
    }
    let Operation::DelegateTask { operation_id, .. } = &saved.request else {
        return Err("invalid delegation operation".into());
    };
    if saved.outcome.is_none() {
        let outcome = match acceptance::lookup(&reporter.client, operation_id, &saved.request)
            .await?
        {
            acceptance::Lookup::Recorded(outcome, context) => {
                saved.outcome_context = Some(context);
                outcome
            }
            acceptance::Lookup::Absent => {
                let current = call(
                    &reporter.client,
                    Operation::GetTask {
                        task_id: saved.attempt.task_id.clone(),
                    },
                )
                .await?;
                let Outcome::Ok {
                    data: Data::Task(parent),
                } = current
                else {
                    return Err("delegation currentness unknown".into());
                };
                if parent.waiting_child_id.is_some() {
                    return Err("parent has child without matching delegation receipt".into());
                }
                let current = parent.session_id == saved.attempt.session_id
                    && parent.assigned_agent_id == parent.input.definition.id
                    && parent.status == TaskStatus::Active
                    && parent.attempt.as_ref().is_some_and(|a| {
                        a.id == saved.attempt.id
                            && a.session_id == saved.attempt.session_id
                            && a.generation == saved.attempt.generation
                            && !a.stopped
                            && !a.uncertain
                            && a.phase != AttemptPhase::StopRequested
                    })
                    && process::now_ms() < saved.attempt.deadline_unix_ms;
                if current {
                    super::resource::verify(&reporter.resource)?;
                    let reply = tokio::time::timeout(
                        Duration::from_millis(
                            reporter.client.limits().worker.handoff_call_timeout_ms,
                        ),
                        reporter.client.call(saved.request.clone()),
                    )
                    .await
                    .map_err(|_| "delegation controller response unknown")??;
                    let Some(context) = CommitContext::from_reply(&reply, &saved.request)? else {
                        return Err(
                            "delegation outcome is not committed; exact request remains pending"
                                .into(),
                        );
                    };
                    saved.outcome_context = Some(context);
                    reply.outcome
                } else {
                    saved.local_not_sent = true;
                    Outcome::Refused {
                        code: "parent_not_current".into(),
                        message: "No new child requested for an inactive parent".into(),
                    }
                }
            }
            acceptance::Lookup::Unavailable => return Err("delegation lookup unavailable".into()),
        };
        verify_outcome(&saved, &outcome)?;
        saved.outcome = Some(outcome);
        bounded_json_size(
            &saved,
            input
                .resolved_limits
                .as_ref()
                .ok_or("missing limits snapshot")?
                .global_metadata
                .worker
                .command_record_bytes,
        ).map_err(|reason| format!("{reason} (worker.command_record_bytes)"))?;
        atomic_json(&path, &saved)?;
    }
    let outcome = saved.outcome.as_ref().ok_or("delegation outcome missing")?;
    verify_outcome(&saved, outcome)?;
    // NativeStep was returned only after helper cleanup. Recheck retained
    // process ownership before reporting quiescence, including on restart.
    process::reconcile_processes(
        &state.join("processes").join(component(&saved.attempt.id)),
        &reporter.config.limits.worker,
    )
    .await?;
    if crate::native::validation_cleanup_pending() != 0 {
        return Err("native_validation_cleanup_unverified".into());
    }
    let current = call(
        &reporter.client,
        Operation::GetTask {
            task_id: saved.attempt.task_id.clone(),
        },
    )
    .await?;
    let Outcome::Ok {
        data: Data::Task(parent),
    } = current
    else {
        return Err("delegation parent state unknown".into());
    };
    if parent.session_id != saved.attempt.session_id
        || parent.assigned_agent_id != parent.input.definition.id
    {
        return Err("delegation parent session binding differs".into());
    }
    let same = parent.attempt.as_ref().is_some_and(|a| {
        a.id == saved.attempt.id
            && a.session_id == saved.attempt.session_id
            && a.generation == saved.attempt.generation
    });
    if same && !parent.attempt.as_ref().is_some_and(|a| a.stopped) {
        if let Outcome::Refused { code, .. } = outcome {
            let summary = scrub(
                format!("Delegation refused: {code}"),
                reporter
                    .output_limit
                    .min(reporter.coding_limits.summary_bytes),
                &reporter.config.limits,
            );
            if !reporter
                .send_identified(
                    format!("{}-delegation-refused", saved.attempt.id),
                    Observation::ExecutorOutcome {
                        success: false,
                        summary: summary.clone(),
                    },
                )
                .await?
            {
                return Ok(AttemptExit::DelegationPending);
            }
            if !reporter
                .send_identified(
                    format!("{}-delegation-failed", saved.attempt.id),
                    Observation::ResultCandidate {
                        checks_passed: false,
                        summary,
                        checkpoint: None,
                        validation: vec![],
                        output: None,
                    },
                )
                .await?
                && !matches!(
                    parent.status,
                    TaskStatus::Failed | TaskStatus::Cancelled | TaskStatus::Succeeded
                )
            {
                return Ok(AttemptExit::DelegationPending);
            }
        }
        if !reporter
            .send_identified(
                format!("{}-delegation-stopped", saved.attempt.id),
                Observation::ProcessTreeStopped,
            )
            .await?
        {
            return Ok(AttemptExit::DelegationPending);
        }
    }
    saved.finished = true;
    atomic_json(&path, &saved)?;
    Ok(AttemptExit::Finished)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Format {
    format_revision: u32,
}

// Outer revision 15 adds accepted PR request/target and original publication
// phases. Historical absent optional fields keep their original serialization.
// Outer revision 14 adds original-request diagnostic outbox records. Revisions
// 9 through 14 keep their existing readers: admission never rewrites records.
// Outer revision 15 adds explicit requested PR targets and original PR phases.
// Outer16 adds accepted secondary instruction sources to task/bridge records.
// Original absent fields retain exact serialization; no new state namespace.
// Outer17 fences supplied public outer-turn journals and output manifests.
// Outer18 carries optional default-assignment metadata in accepted snapshots.
const CURRENT_STATE_FORMAT: u32 = 19;
const DEFAULT_ASSIGNMENT_STATE_FORMAT: u32 = 18;
const SUPPLIED_STATE_FORMAT: u32 = 17;
const INSTRUCTION_SOURCE_STATE_FORMAT: u32 = 16;
const PULL_REQUEST_STATE_FORMAT: u32 = 15;
const DIAGNOSTIC_STATE_FORMAT: u32 = 14;
const OUTPUT_RETENTION_STATE_FORMAT: u32 = 13;
const OUTPUT_CAP_STATE_FORMAT:u32=12;
const PREVIOUS_STATE_FORMAT: u32 = 11;
const EARLIER_STATE_FORMAT: u32 = 10;
const INSTALLED_STATE_FORMAT: u32 = 9;

fn read_state_format(state: &Path, limits: &WorkerLimits) -> Result<Format, String> {
    use std::io::Read;
    use std::os::unix::fs::OpenOptionsExt;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(0x20000 | 0x800)
        .open(state.join("format.json"))
        .map_err(|e| e.to_string())?;
    let metadata = file.metadata().map_err(|e| e.to_string())?;
    if !metadata.is_file() {
        return Err("unsupported worker state format".into());
    }
    if metadata.len() > limits.state_format_bytes as u64 {
        return Err("unsupported worker state format (worker.state_format_bytes)".into());
    }
    let mut bytes = Vec::new();
    file.take(limits.state_format_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() > limits.state_format_bytes {
        return Err("unsupported worker state format (worker.state_format_bytes)".into());
    }
    let format: Format =
        serde_json::from_slice(&bytes).map_err(|_| "unsupported worker state format")?;
    if !matches!(format.format_revision, INSTALLED_STATE_FORMAT | EARLIER_STATE_FORMAT | PREVIOUS_STATE_FORMAT | OUTPUT_CAP_STATE_FORMAT | OUTPUT_RETENTION_STATE_FORMAT | DIAGNOSTIC_STATE_FORMAT | PULL_REQUEST_STATE_FORMAT | INSTRUCTION_SOURCE_STATE_FORMAT | SUPPLIED_STATE_FORMAT | DEFAULT_ASSIGNMENT_STATE_FORMAT | CURRENT_STATE_FORMAT) {
        return Err("unsupported worker state format; no migration performed".into());
    }
    Ok(format)
}

pub(super) fn admits_instruction_sources(state: &Path, limits: &WorkerLimits) -> Result<bool, String> {
    Ok(matches!(read_state_format(state, limits)?.format_revision, INSTRUCTION_SOURCE_STATE_FORMAT | SUPPLIED_STATE_FORMAT | DEFAULT_ASSIGNMENT_STATE_FORMAT | CURRENT_STATE_FORMAT))
}

pub(super) fn admits_supplied_turn_records(state: &Path, limits: &WorkerLimits) -> Result<bool, String> {
    Ok(matches!(read_state_format(state, limits)?.format_revision, SUPPLIED_STATE_FORMAT | DEFAULT_ASSIGNMENT_STATE_FORMAT | CURRENT_STATE_FORMAT))
}

pub(super) fn admits_default_assignment(state: &Path, limits: &WorkerLimits) -> Result<bool, String> {
    Ok(matches!(read_state_format(state, limits)?.format_revision, DEFAULT_ASSIGNMENT_STATE_FORMAT | CURRENT_STATE_FORMAT))
}

pub(super) fn admits_identity_administration(state:&Path,limits:&WorkerLimits)->Result<bool,String> { Ok(read_state_format(state,limits)?.format_revision==CURRENT_STATE_FORMAT) }

pub(super) fn admits_pull_request_records(state: &Path, limits: &WorkerLimits) -> Result<bool, String> {
    Ok(matches!(read_state_format(state, limits)?.format_revision, PULL_REQUEST_STATE_FORMAT | INSTRUCTION_SOURCE_STATE_FORMAT | SUPPLIED_STATE_FORMAT | DEFAULT_ASSIGNMENT_STATE_FORMAT | CURRENT_STATE_FORMAT))
}

pub(super) fn initialize_state(state: &Path, limits: &WorkerLimits) -> Result<(), String> {
    if state.exists() {
        if !std::fs::symlink_metadata(state)
            .map_err(|e| e.to_string())?
            .file_type()
            .is_dir()
        {
            return Err("worker state is not a directory".into());
        }
        if state.join("format.json").exists() {
            // Compatibility inspection happens before ownership. Retained 9-17
            // markers remain byte-exact until the owned admission boundary.
            read_state_format(state, limits)?;
            return Ok(());
        }
        if std::fs::read_dir(state)
            .map_err(|e| e.to_string())?
            .next()
            .is_some()
        {
            return Err("unmarked nonempty worker state refused; no migration performed".into());
        }
    }
    process::private_directory(state)?;
    write_current_state_format(state, limits)
}

fn write_current_state_format(state: &Path, limits: &WorkerLimits) -> Result<(), String> {
    let format = Format { format_revision: CURRENT_STATE_FORMAT };
    bounded_json_size(&format, limits.state_format_bytes)
        .map_err(|reason| format!("{reason} (worker.state_format_bytes)"))?;
    atomic_json(&state.join("format.json"), &format)
}

/// Call only after the existing local retained-policy, command, resource and
/// stopped-journal admission checks, with this state's owner lease held, before
/// any report replay or recovery request. Inner records keep their own readers
/// and recovery gates; this is only the explicit outer 9-17 -> 18 migration.
/// A failure must exit before the ordinary shutdown report-flushing path.
pub(super) fn finish_state_upgrade(
    state: &Path,
    limits: &RuntimeLimits,
    _lease: &process::WorkerLease,
) -> Result<(), String> {
    read_state_format(state, &limits.worker)?;
    crate::worker::admit_default_assignment(state, limits)?;
    crate::worker::admit_identity_administration(state, limits)?;
    // Also re-persist an observed 18: a prior rename may have succeeded before
    // its directory sync failed. Visibility alone is not a durable barrier.
    write_current_state_format(state, &limits.worker)
        .map_err(|error| format!("worker state format upgrade failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn initialize_state(state: &Path) -> Result<(), String> {
        super::initialize_state(state, &test_limits().worker)
    }
    fn record_launch(path: &Path, command: &WorkerCommand) -> Result<bool, String> {
        crate::worker::record_launch(path, command, &test_limits())
    }
    fn track_native_launch(state: &Path, attempt: &Attempt) -> Result<(), String> {
        crate::worker::track_native_launch(state, attempt, &test_limits())
    }
    async fn native_registry_reconciled(state: &Path, attempt: &Attempt) -> Result<bool, String> {
        crate::worker::native_registry_reconciled(state, attempt, &test_limits()).await
    }

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

    #[derive(Clone, Copy, PartialEq)]
    enum Mode {
        Normal,
        LoseAcknowledgment,
        LoseBeforeCommit,
        Refuse,
        UncommittedCapacity,
        Unavailable,
    }
    struct Store {
        parent: Task,
        operation: Option<(serde_json::Value, Outcome)>,
        sent: Vec<serde_json::Value>,
        reports: Vec<Report>,
        refused_reports: std::collections::BTreeMap<String, (serde_json::Value, Outcome)>,
        mode: Mode,
        commits: BTreeMap<String, MutationCommit>,
        revision: u64,
        receipt_path: PathBuf,
    }
    fn record_commit(store: &mut Store, operation_id: &str) {
        if !store.commits.contains_key(operation_id) {
            store.revision += 1;
            let revision = store.revision;
            store.commits.insert(
                operation_id.into(),
                MutationCommit {
                    operation_id: operation_id.into(),
                    controller_revision: revision,
                },
            );
        }
    }
    fn fixture_reply(store: &Store, request: &Request, outcome: Outcome) -> Reply {
        let original = match (&request.command, &outcome) {
            (
                Operation::GetOperation { operation_id },
                Outcome::Ok {
                    data: Data::Operation(record),
                },
            ) if record.kind == "application_mutation" => Some(operation_id.as_str()),
            _ => request.command.mutation_id(),
        };
        Reply {
            request_id: request.request_id.clone(),
            outcome,
            commit: original.and_then(|id| store.commits.get(id).cloned()),
            observed_controller_revision: Some(store.revision),
        }
    }
    struct Fixture {
        state: PathBuf,
        input: TaskInput,
        reporter: Reporter,
        store: Arc<Mutex<Store>>,
        server: tokio::task::JoinHandle<()>,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.server.abort();
        }
    }
    fn area(label: &str) -> PathBuf {
        static SEQUENCE: AtomicU64 = AtomicU64::new(1);
        let parent = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("artifacts/v2/build-60156282/worker/delegation-01/tests");
        std::fs::create_dir_all(&parent).unwrap();
        let path = parent.join(format!(
            "{label}-{}-{}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).expect("delegation fixture must be new");
        path
    }
    fn task(id: &str, input: TaskInput, attempt: Option<Attempt>) -> Task {
        Task {
            pending_assignment: None,
            session_id: format!("session-{id}"),
            id: id.into(),
            assigned_agent_id: input.definition.id.clone(),
            input,
            status: if attempt.is_some() {
                TaskStatus::Active
            } else {
                TaskStatus::Queued
            },
            revision: 1,
            attempt,
            selected_checkpoint: None,
            result: None,
            blocked_reason: None, native_retry_block: None,
            progress: String::new(),
            recent_output: String::new(),
            output_truncated: false,
            parent_task_id: None,
            waiting_child_id: None,
            completed_child: None,
        }
    }
    async fn fixture(mode: Mode) -> Fixture {
        let state = area("handoff");
        initialize_state(&state).unwrap();
        for d in ["commands", "outbox", "processes", "delegations"] {
            std::fs::create_dir(state.join(d)).unwrap();
        }
        let deadline = process::now_ms() + 60_000;
        let mut input = TaskInput { instruction_source: None,
            pull_request: None,
            resolved_limits: None,
            repository_read: None,
            instruction: "parent original instruction".into(),
            definition: Definition {
                charter: None,
                limit_overrides: None,
                execution_profile: Some(crate::protocol::ExecutionProfile::CollaborationV1 {
                    web_search: None,
                }),
                inference: Some(crate::protocol::InferenceSettings::selected()),
                id: "native".into(),
                revision: "fixed".into(),
                executor: ExecutorKind::Native,
            },
            repository: None,
            acceptance_commands: vec![],
            output_schema: Some(json!({"type":"object"})),
            limits: Limits {
                deadline_unix_ms: deadline,
                model_requests: 4,
                output_bytes: 4096,
                result_bytes: 4096,
                max_delegation_depth: 2,
            },
            dependencies: vec![],
            native_workflow: Some(Box::new(NativeWorkflow::CodingThenContinue {
                child: CodingTaskTemplate { instruction_source: None,
                    pull_request: None,
                    instruction_prefix: "fixed prefix".into(),
                    definition: Definition {
                        charter: None,
                        limit_overrides: None,
                        execution_profile: Some(
                            crate::protocol::ExecutionProfile::CollaborationV1 { web_search: None },
                        ),
                        inference: Some(crate::protocol::InferenceSettings::selected()),
                        id: "coding".into(),
                        revision: "fixed".into(),
                        executor: ExecutorKind::Coding,
                    },
                    repository: Repository {
                        pull_request_target: None,
                        registration: Some(crate::repository_catalogue::Revision {
                            repository_id: "fixture-repository".into(),
                            registration_revision: "fixture-revision".into(),
                        }),
                        url: "/fixture/source.git".into(),
                        start_commit: "a".repeat(40),
                        checkpoint_remote: "/fixture/checkpoints.git".into(),
                    },
                    acceptance_commands: vec![vec!["/usr/bin/true".into()]],
                    dependencies: vec![],
                    limits: Limits {
                        deadline_unix_ms: deadline,
                        model_requests: 2,
                        output_bytes: 4096,
                        result_bytes: 1024,
                        max_delegation_depth: 1,
                    },
                },
            })),
        };
        input.resolved_limits = Some(test_limits().resolve_task(&input).unwrap());
        let attempt = Attempt {
            session_id: "session-parent".into(),
            id: "parent-attempt".into(),
            task_id: "parent".into(),
            generation: 3,
            worker_id: "worker".into(),
            worker_incarnation: "old".into(),
            phase: AttemptPhase::Running,
            deadline_unix_ms: deadline,
            stopped: false,
            uncertain: false,
        };
        let store = Arc::new(Mutex::new(Store {
            parent: task("parent", input.clone(), Some(attempt.clone())),
            operation: None,
            sent: vec![],
            reports: vec![],
            refused_reports: std::collections::BTreeMap::new(),
            mode,
            commits: BTreeMap::new(),
            revision: 1,
            receipt_path: path(&state, &attempt),
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let shared = store.clone();
        let server = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let shared = shared.clone();
                connections.spawn(async move {
                    let service = service_fn(move |http: HttpRequest<Incoming>| {
                        let shared = shared.clone();
                        async move {
                            let request: Request = serde_json::from_slice(
                                &http.into_body().collect().await.unwrap().to_bytes(),
                            )
                            .unwrap();
                            let mut store = shared.lock().unwrap();
                            if store.mode == Mode::Unavailable {
                                return Ok::<_, Infallible>(
                                    HttpResponse::builder()
                                        .status(503)
                                        .body(Full::new(Bytes::new()))
                                        .unwrap(),
                                );
                            }
                            let outcome = match &request.command {
                                Operation::GetTask { task_id } => {
                                    assert_eq!(task_id, "parent");
                                    Outcome::Ok {
                                        data: Data::Task(Box::new(store.parent.clone())),
                                    }
                                }
                                Operation::GetOperation { operation_id } => {
                                    match store
                                        .refused_reports
                                        .get(operation_id)
                                        .or(store.operation.as_ref())
                                    {
                                        Some((intended, outcome)) => Outcome::Ok {
                                            data: Data::Operation(OperationRecord {
                                                id: operation_id.clone(),
                                                task_id: "child".into(),
                                                attempt_id: String::new(),
                                                generation: 0,
                                                kind: "application_mutation".into(),
                                                intended: intended.clone(),
                                                state: "settled".into(),
                                                outcome: Some(
                                                    serde_json::to_value(outcome).unwrap(),
                                                ),
                                            }),
                                        },
                                        None => Outcome::Refused {
                                            code: "not_found".into(),
                                            message: "no receipt".into(),
                                        },
                                    }
                                }
                                Operation::DelegateTask {
                                    parent_task_id,
                                    child,
                                    ..
                                } => {
                                    let body = serde_json::to_value(&request.command).unwrap();
                                    store.sent.push(body.clone());
                                    let durable: serde_json::Value = serde_json::from_slice(
                                        &std::fs::read(&store.receipt_path)
                                            .expect("decision must be durable before send"),
                                    )
                                    .unwrap();
                                    assert_eq!(durable["request"], body);
                                    if store.mode == Mode::LoseBeforeCommit {
                                        store.mode = Mode::Normal;
                                        return Ok(HttpResponse::builder()
                                            .status(503)
                                            .body(Full::new(Bytes::new()))
                                            .unwrap());
                                    }

                                    if store.mode == Mode::UncommittedCapacity {
                                        store.mode = Mode::Normal;
                                        let reply = fixture_reply(
                                            &store,
                                            &request,
                                            Outcome::Refused {
                                                code: "capacity".into(),
                                                message: "uncommitted queue capacity".into(),
                                            },
                                        );
                                        assert!(reply.commit.is_none());
                                        return Ok(HttpResponse::new(Full::new(Bytes::from(
                                            serde_json::to_vec(&reply).unwrap(),
                                        ))));
                                    }
                                    let outcome = if let Some((prior, outcome)) = &store.operation {
                                        if prior == &body {
                                            outcome.clone()
                                        } else {
                                            Outcome::Refused {
                                                code: "operation_conflict".into(),
                                                message: "changed".into(),
                                            }
                                        }
                                    } else if store.mode == Mode::Refuse {
                                        Outcome::Refused {
                                            code: "capacity".into(),
                                            message: "fixture refusal".into(),
                                        }
                                    } else {
                                        let mut child = task("child", child.clone(), None);
                                        child.parent_task_id = Some(parent_task_id.clone());
                                        store.parent.waiting_child_id = Some("child".into());
                                        store.parent.attempt.as_mut().unwrap().phase =
                                            AttemptPhase::StopRequested;
                                        Outcome::Ok {
                                            data: Data::Task(Box::new(child)),
                                        }
                                    };
                                    if store.operation.is_none() {
                                        store.operation = Some((body, outcome.clone()));
                                    }
                                    record_commit(
                                        &mut store,
                                        request.command.mutation_id().unwrap(),
                                    );
                                    if store.mode == Mode::LoseAcknowledgment {
                                        store.mode = Mode::Normal;
                                        return Ok(HttpResponse::builder()
                                            .status(503)
                                            .body(Full::new(Bytes::new()))
                                            .unwrap());
                                    }
                                    outcome
                                }
                                Operation::ObserveWorker { report } => {
                                    store.reports.push(report.clone());
                                    record_commit(&mut store, &report.report_id);
                                    match &report.observation {
                                        Observation::ProcessTreeStopped => {
                                            let a = store.parent.attempt.as_mut().unwrap();
                                            a.stopped = true;
                                            a.phase = AttemptPhase::Stopped;
                                        }
                                        Observation::ExecutionUncertain { .. } => {
                                            store.parent.status = TaskStatus::Failed;
                                            store.parent.attempt.as_mut().unwrap().uncertain = true;
                                        }
                                        Observation::ResultCandidate {
                                            checks_passed: false,
                                            ..
                                        } => {
                                            if matches!(
                                                store.parent.status,
                                                TaskStatus::Cancelled
                                                    | TaskStatus::Succeeded
                                                    | TaskStatus::Failed
                                            ) {
                                                // Match the actual controller: an action refusal
                                                // is a committed receipt, discoverable by exact ID.
                                                let outcome = Outcome::Refused {
                                                    code: "invalid_transition".into(),
                                                    message: "terminal".into(),
                                                };
                                                store.refused_reports.insert(
                                                    report.report_id.clone(),
                                                    (
                                                        serde_json::to_value(&request.command)
                                                            .unwrap(),
                                                        outcome.clone(),
                                                    ),
                                                );
                                                let body = serde_json::to_vec(&fixture_reply(
                                                    &store, &request, outcome,
                                                ))
                                                .unwrap();
                                                return Ok(HttpResponse::new(Full::new(
                                                    Bytes::from(body),
                                                )));
                                            }
                                            store.parent.status = TaskStatus::Failed;
                                        }
                                        _ => {}
                                    }
                                    Outcome::Ok {
                                        data: Data::Task(Box::new(store.parent.clone())),
                                    }
                                }
                                _ => panic!("unexpected handoff fixture operation"),
                            };
                            let body =
                                serde_json::to_vec(&fixture_reply(&store, &request, outcome))
                                    .unwrap();
                            Ok(HttpResponse::new(Full::new(Bytes::from(body))))
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
                id: "worker".into(),
                incarnation: "old".into(),
                workspace_root: state.clone(),
                codex_path: "/usr/bin/false".into(),
                coding_capacity: 1,
                native_capacity: 1,
                native_endpoint: "http://127.0.0.1:1/v1/responses".into(),
                validation_executable: std::env::current_exe().unwrap(),
            },
            attempt: attempt.clone(),
            outbox: state.join("outbox"),
            sequence: Arc::new(AtomicU64::new(1)),
            executor_seen: Arc::new(AtomicBool::new(false)),
            provider_uncertain: Arc::new(AtomicBool::new(false)),
            uncertainty_retained: Arc::new(AtomicBool::new(false)),
            output_limit: input.limits.output_bytes,
            result_limit: input.limits.result_bytes,
            coding_limits: input.execution_limits().unwrap().coding.clone(),
            outbox_lock: Arc::new(tokio::sync::Mutex::new(())),
        };
        record_launch(
            &state
                .join("commands")
                .join(format!("{}.json", component("launch"))),
            &WorkerCommand::RunAttempt {
                command_id: "launch".into(),
                session: crate::protocol::Session {
                    id: attempt.session_id.clone(),
                    agent_id: input.definition.id.clone(),
                    definition: input.definition.clone(),
                    task_id: attempt.task_id.clone(),
                },
                attempt,
                task: input.clone(),
                checkpoint: None,
                continuation: None,
                workflow_continuation: None, completion_continuation: None,
            },
        )
        .unwrap();
        // This fixture represents a prior executor decision, not mere delivery.
        let launch_path = state
            .join("commands")
            .join(format!("{}.json", component("launch")));
        let mut receipt: Receipt =
            serde_json::from_slice(&std::fs::read(&launch_path).unwrap()).unwrap();
        receipt.effects_started = true;
        atomic_json(&launch_path, &receipt).unwrap();
        Fixture {
            state,
            input,
            reporter,
            store,
            server,
        }
    }

    #[tokio::test]
    async fn acknowledged_handoff_stops_without_parent_success_and_replay_is_exact() {
        let f = fixture(Mode::Normal).await;
        assert_eq!(
            begin(&f.state, &f.input, &f.reporter, "do the fixed work")
                .await
                .unwrap(),
            AttemptExit::Finished
        );
        assert_eq!(
            begin(&f.state, &f.input, &f.reporter, "do the fixed work")
                .await
                .unwrap(),
            AttemptExit::Finished
        );
        let store = f.store.lock().unwrap();
        assert_eq!(store.sent.len(), 1);
        let op: Operation = serde_json::from_value(store.sent[0].clone()).unwrap();
        let Operation::DelegateTask { child, .. } = op else {
            panic!("not a delegation")
        };
        assert_eq!(
            child,
            crate::delegation::coding_child(&f.input, "do the fixed work").unwrap()
        );
        assert_eq!(store.parent.status, TaskStatus::Active);
        assert!(store.parent.attempt.as_ref().unwrap().stopped);
        assert_eq!(
            store
                .reports
                .iter()
                .filter(|r| matches!(r.observation, Observation::ProcessTreeStopped))
                .count(),
            1
        );
        assert!(!store.reports.iter().any(|r| matches!(
            r.observation,
            Observation::ExecutorOutcome { .. }
                | Observation::ResultCandidate { .. }
                | Observation::ExecutionUncertain { .. }
        )));
    }
    #[tokio::test]
    async fn lost_acknowledgment_restart_reconciles_same_child_before_generic_uncertainty() {
        let f = fixture(Mode::LoseAcknowledgment).await;
        assert_eq!(
            begin(&f.state, &f.input, &f.reporter, "work once")
                .await
                .unwrap(),
            AttemptExit::DelegationPending
        );
        assert!(
            !f.store
                .lock()
                .unwrap()
                .parent
                .attempt
                .as_ref()
                .unwrap()
                .stopped
        );
        let mut successor = f.reporter.config.clone();
        successor.incarnation = "new".into();
        reconcile_saved_handoffs(
            &successor,
            &f.reporter.client,
            &f.state,
            &f.reporter.outbox_lock,
            &BTreeMap::new(),
            &None,
        )
        .await
        .unwrap();
        account_for_interrupted_commands(&f.reporter.client, &f.state, "worker")
            .await
            .unwrap();
        let saved = load(
            &path(&f.state, &f.reporter.attempt),
            &f.reporter.attempt,
            &f.input,
        )
        .unwrap();
        let context = saved.outcome_context.as_ref().unwrap();
        assert_eq!(
            context.commit.operation_id,
            saved.request.mutation_id().unwrap()
        );
        assert!(
            context.observed_controller_revision > context.commit.controller_revision,
            "lookup must preserve the original delegation commit and its later observed revision"
        );
        let store = f.store.lock().unwrap();
        assert_eq!(store.sent.len(), 1);
        assert_eq!(store.parent.waiting_child_id.as_deref(), Some("child"));
        assert!(store.parent.attempt.as_ref().unwrap().stopped);
        assert!(store.reports.iter().all(|r| !matches!(
            r.observation,
            Observation::ExecutionUncertain { .. } | Observation::ResultCandidate { .. }
        )));
        assert!(
            store
                .reports
                .iter()
                .filter(|r| matches!(r.observation, Observation::ProcessTreeStopped))
                .all(|r| r.incarnation == "old")
        );
    }
    #[tokio::test]
    async fn handoff_preserves_exact_child_registration_and_destination() {
        let f = fixture(Mode::Normal).await;
        let file = record(&f.state, &f.reporter.attempt, &f.input, "original").unwrap();
        let retained = std::fs::read(&file).unwrap();
        for change_registration in [true, false] {
            let mut changed = f.input.clone();
            let Some(workflow) = changed.native_workflow.as_mut() else { panic!("fixture workflow missing") };
            let NativeWorkflow::CodingThenContinue { child } = workflow.as_mut() else { panic!("fixture coding workflow missing") };
            if change_registration {
                child.repository.registration.as_mut().unwrap().registration_revision.push_str("-new");
            } else {
                child.repository.checkpoint_remote.push_str("-other");
            }
            assert!(load(&file, &f.reporter.attempt, &changed).is_err());
            assert!(record(&f.state, &f.reporter.attempt, &changed, "original").is_err());
            assert_eq!(std::fs::read(&file).unwrap(), retained);
        }
        assert!(load(&file, &f.reporter.attempt, &f.input).is_ok());
        assert!(f.store.lock().unwrap().operation.is_none());
    }

    #[tokio::test]
    async fn changed_decision_body_refuses_without_rewriting_original() {
        let f = fixture(Mode::Normal).await;
        let p = record(&f.state, &f.reporter.attempt, &f.input, "original").unwrap();
        let before = std::fs::read(&p).unwrap();
        assert!(record(&f.state, &f.reporter.attempt, &f.input, "changed").is_err());
        assert_eq!(std::fs::read(&p).unwrap(), before);
        assert!(f.store.lock().unwrap().sent.is_empty());
    }
    #[tokio::test]
    async fn missing_controller_receipt_replays_exact_saved_body_without_new_decision() {
        let f = fixture(Mode::LoseBeforeCommit).await;
        assert_eq!(
            begin(&f.state, &f.input, &f.reporter, "one decision")
                .await
                .unwrap(),
            AttemptExit::DelegationPending
        );
        assert_eq!(
            reconcile(&f.state, &f.input, &f.reporter).await.unwrap(),
            AttemptExit::Finished
        );
        let store = f.store.lock().unwrap();
        assert_eq!(store.sent.len(), 2);
        assert_eq!(store.sent[0], store.sent[1]);
        assert_eq!(store.parent.waiting_child_id.as_deref(), Some("child"));
    }

    #[tokio::test]
    async fn failure_to_persist_decision_sends_no_child_request() {
        let f = fixture(Mode::Normal).await;
        std::fs::remove_dir(f.state.join("delegations")).unwrap();
        assert!(
            begin(&f.state, &f.input, &f.reporter, "not persisted")
                .await
                .is_err()
        );
        assert!(f.store.lock().unwrap().sent.is_empty());
        assert!(f.store.lock().unwrap().operation.is_none());
    }

    #[tokio::test]
    async fn unavailable_or_conflicting_handoff_never_reports_parent_failure() {
        let f = fixture(Mode::Unavailable).await;
        assert_eq!(
            begin(&f.state, &f.input, &f.reporter, "work")
                .await
                .unwrap(),
            AttemptExit::DelegationPending
        );
        assert!(
            account_for_interrupted_commands(&f.reporter.client, &f.state, "worker")
                .await
                .is_err()
        );
        {
            let mut store = f.store.lock().unwrap();
            store.mode = Mode::Normal;
            store.operation = Some((
                json!({"operation":"delegate_task","input":"different"}),
                Outcome::Refused {
                    code: "operation_conflict".into(),
                    message: "different".into(),
                },
            ));
        }
        assert!(reconcile(&f.state, &f.input, &f.reporter).await.is_err());
        let store = f.store.lock().unwrap();
        assert!(store.sent.is_empty());
        assert!(!store.reports.iter().any(|r| matches!(
            r.observation,
            Observation::ExecutionUncertain { .. }
                | Observation::ResultCandidate { .. }
                | Observation::ProcessTreeStopped
        )));
    }
    #[tokio::test]
    async fn known_refusal_fails_boundedly_and_cancelled_parent_cannot_create_child() {
        let f = fixture(Mode::Refuse).await;
        assert_eq!(
            begin(&f.state, &f.input, &f.reporter, "refused")
                .await
                .unwrap(),
            AttemptExit::Finished
        );
        {
            let store = f.store.lock().unwrap();
            assert_eq!(store.sent.len(), 1);
            assert_eq!(store.parent.status, TaskStatus::Failed);
            assert!(store.parent.attempt.as_ref().unwrap().stopped);
            assert!(store.parent.waiting_child_id.is_none());
        }
        let cancelled = fixture(Mode::Normal).await;
        cancelled.store.lock().unwrap().parent.status = TaskStatus::Cancelled;
        assert_eq!(
            begin(
                &cancelled.state,
                &cancelled.input,
                &cancelled.reporter,
                "do not send"
            )
            .await
            .unwrap(),
            AttemptExit::Finished
        );
        let saved = load(
            &path(&cancelled.state, &cancelled.reporter.attempt),
            &cancelled.reporter.attempt,
            &cancelled.input,
        )
        .unwrap();
        assert!(saved.local_not_sent);
        assert!(saved.outcome_context.is_none());
        let store = cancelled.store.lock().unwrap();
        assert!(store.sent.is_empty());
        assert_eq!(store.parent.status, TaskStatus::Cancelled);
    }
    #[tokio::test]
    async fn uncommitted_delegation_refusal_remains_pending_without_parent_failure() {
        let f = fixture(Mode::UncommittedCapacity).await;
        assert_eq!(
            begin(&f.state, &f.input, &f.reporter, "work")
                .await
                .unwrap(),
            AttemptExit::DelegationPending
        );
        let first = load(
            &path(&f.state, &f.reporter.attempt),
            &f.reporter.attempt,
            &f.input,
        )
        .unwrap();
        assert!(
            first.outcome.is_none() && first.outcome_context.is_none() && !first.local_not_sent
        );
        {
            let store = f.store.lock().unwrap();
            assert!(store.operation.is_none());
            assert!(store.parent.waiting_child_id.is_none());
            assert!(!store.reports.iter().any(|r| matches!(
                r.observation,
                Observation::ExecutorOutcome { .. }
                    | Observation::ResultCandidate { .. }
                    | Observation::ProcessTreeStopped
            )));
        }
        assert_eq!(
            reconcile(&f.state, &f.input, &f.reporter).await.unwrap(),
            AttemptExit::Finished
        );
        let stored = load(
            &path(&f.state, &f.reporter.attempt),
            &f.reporter.attempt,
            &f.input,
        )
        .unwrap();
        assert!(!stored.local_not_sent);
        let context = stored.outcome_context.as_ref().unwrap();
        assert_eq!(
            context.commit.operation_id,
            first.request.mutation_id().unwrap()
        );
        let store = f.store.lock().unwrap();
        assert_eq!(store.sent.len(), 2);
        assert_eq!(store.sent[0], store.sent[1]);
        assert_eq!(store.parent.waiting_child_id.as_deref(), Some("child"));
        assert!(store.parent.attempt.as_ref().unwrap().stopped);
    }
    #[tokio::test]
    async fn missing_native_decision_refuses_replacement_without_claiming_helper_stop() {
        let f = fixture(Mode::Normal).await;
        assert!(
            account_for_interrupted_commands(&f.reporter.client, &f.state, "worker")
                .await
                .is_err()
        );
        assert!(
            f.store
                .lock()
                .unwrap()
                .reports
                .iter()
                .any(|r| matches!(r.observation, Observation::ExecutionUncertain { .. }))
        );
        assert!(f.store.lock().unwrap().sent.is_empty());
        assert!(
            !f.store
                .lock()
                .unwrap()
                .reports
                .iter()
                .any(|r| matches!(r.observation, Observation::ProcessTreeStopped))
        );
        assert!(
            !f.store
                .lock()
                .unwrap()
                .parent
                .attempt
                .as_ref()
                .unwrap()
                .stopped
        );
    }
    #[tokio::test]
    async fn tracked_empty_registry_releases_local_residency_but_keeps_unknown_outcome() {
        let f = fixture(Mode::Normal).await;
        track_native_launch(&f.state, &f.reporter.attempt).unwrap();
        let marker = std::fs::read(native_tracking_path(&f.state, &f.reporter.attempt)).unwrap();
        account_for_interrupted_commands(&f.reporter.client, &f.state, "worker")
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(native_tracking_path(&f.state, &f.reporter.attempt)).unwrap(),
            marker
        );
        let store = f.store.lock().unwrap();
        assert!(store.parent.attempt.as_ref().unwrap().stopped);
        assert!(store.parent.attempt.as_ref().unwrap().uncertain);
        assert_eq!(store.parent.status, TaskStatus::Failed);
        assert!(store.parent.result.is_none());
        assert!(store.sent.is_empty());
        assert!(
            store
                .reports
                .iter()
                .any(|r| matches!(r.observation, Observation::ExecutionUncertain { .. }))
        );
        assert!(
            store
                .reports
                .iter()
                .any(|r| matches!(r.observation, Observation::ProcessTreeStopped))
        );
        assert!(!store.reports.iter().any(|r| matches!(
            r.observation,
            Observation::Usage {
                model_requests: 0,
                ..
            }
        )));
    }

    #[tokio::test]
    async fn full_escaped_result_is_bounded_before_a_success_report() {
        let f = fixture(Mode::Normal).await;
        let mut reporter = f.reporter.clone();
        reporter.result_limit = 256;
        reporter
            .send(Observation::ResultCandidate {
                checks_passed: true,
                summary: "small summary".into(),
                checkpoint: None,
                validation: vec!["check".into()],
                output: Some(json!({"escaped":"\n".repeat(100)})),
            })
            .await
            .unwrap();
        let store = f.store.lock().unwrap();
        let last = &store.reports.last().unwrap().observation;
        assert!(
            matches!(last,Observation::ResultCandidate{checks_passed:false,output:None,summary,..} if summary.contains("result_bytes"))
        );
    }

    #[tokio::test]
    async fn unverified_native_cleanup_receipt_cannot_bypass_restart_guard() {
        let f = fixture(Mode::Normal).await;
        let p = f
            .state
            .join("commands")
            .join(format!("{}.json", component("launch")));
        let mut receipt: Receipt = serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
        let result = Err(AttemptFailure::Execution(
            "native_validation_cleanup_unverified".to_owned(),
        ));
        receipt.completed = receipt_completed(&result, true);
        assert!(!receipt.completed);
        atomic_json(&p, &receipt).unwrap();
        assert!(
            account_for_interrupted_commands(&f.reporter.client, &f.state, "worker")
                .await
                .is_err()
        );
        let saved: Receipt = serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
        assert!(!saved.completed);
        {
            let mut store = f.store.lock().unwrap();
            assert!(
                !store
                    .reports
                    .iter()
                    .any(|r| matches!(r.observation, Observation::ProcessTreeStopped))
            );
            // A separately committed stop is actual evidence; the local coding
            // inventory alone was deliberately insufficient above.
            store.parent.status = TaskStatus::Failed;
            let attempt = store.parent.attempt.as_mut().unwrap();
            attempt.stopped = true;
            attempt.phase = AttemptPhase::Stopped;
        }
        account_for_interrupted_commands(&f.reporter.client, &f.state, "worker")
            .await
            .unwrap();
        let saved: Receipt = serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
        assert!(saved.completed);
    }
    fn historical_limits() -> Arc<RuntimeLimits> {
        let mut limits = (*test_limits()).clone();
        limits.task_defaults.agent_id = None;
        Arc::new(limits)
    }
    fn historical_state(label: &str, revision: u32) -> (PathBuf, Vec<u8>) {
        assert!(matches!(revision, 9 | 10 | 11 | 12 | 13 | 14 | 15 | 16 | 17 | 18));
        let state = area(label);
        process::private_directory(&state.join("commands")).unwrap();
        process::private_directory(&state.join("mutations")).unwrap();
        let marker = format!("{{\n  \"format_revision\": {revision}\n}}\n").into_bytes();
        std::fs::write(state.join("format.json"), &marker).unwrap();
        (state, marker)
    }

    #[tokio::test]
    async fn instruction_sources_require16_in_original_commands_and_pending_await() {
        let f = fixture(Mode::Normal).await;
        let p = f.state.join("commands").join(format!("{}.json", component("launch")));
        let mut receipt = crate::worker::read_receipt(&p, &test_limits()).unwrap();
        let WorkerCommand::RunAttempt { task, .. } = &mut receipt.command else { unreachable!() };
        let Some(NativeWorkflow::CodingThenContinue { child }) = task.native_workflow.as_deref_mut() else { unreachable!() };
        child.instruction_source = Some(RepositoryReadSource {
            registration: Some(crate::repository_catalogue::Revision { repository_id: "spec".into(), registration_revision: "r1".into() }),
            repository: "/provisioned/spec.git".into(), start_commit: "a".repeat(40),
        });
        atomic_json(&p, &receipt).unwrap();
        let bytes = std::fs::read(&p).unwrap();
        atomic_json(&f.state.join("format.json"), &json!({"format_revision":15})).unwrap();
        let marker = std::fs::read(f.state.join("format.json")).unwrap();
        assert!(crate::worker::admit_instruction_sources(&f.state, &test_limits()).unwrap_err().contains("worker16"));
        assert_eq!(std::fs::read(&p).unwrap(), bytes);
        assert_eq!(std::fs::read(f.state.join("format.json")).unwrap(), marker);
        process::private_directory(&f.state.join("mutations")).unwrap();
        let mut journal = mutations::prepare(Operation::AwaitCommands { operation_id: "original-await".into(), worker_id: "worker".into(), incarnation: "old".into(), acknowledged: vec![] }, 0, &test_limits()).unwrap();
        journal.outcome = Some(Outcome::Ok { data: Data::Commands(vec![receipt.command]) });
        journal.outcome_context = Some(CommitContext { commit: MutationCommit { operation_id: "original-await".into(), controller_revision: 1 }, observed_controller_revision: 1 });
        let jp = f.state.join("mutations/await.json"); mutations::persist(&jp, &journal).unwrap();
        let jb = std::fs::read(&jp).unwrap();
        std::fs::remove_file(&p).unwrap();
        assert!(crate::worker::admit_instruction_sources(&f.state, &test_limits()).unwrap_err().contains("worker16"));
        assert_eq!(std::fs::read(&jp).unwrap(), jb);
        atomic_json(&f.state.join("format.json"), &json!({"format_revision":16})).unwrap();
        crate::worker::admit_instruction_sources(&f.state, &test_limits()).unwrap();
        assert_eq!(std::fs::read(&jp).unwrap(), jb);
    }

    #[test]
    fn supplied_records_require17_after_owned16_upgrade_without_record_rewrite() {
        let (state, marker)=historical_state("supplied16-upgrade",16);let limits=historical_limits();
        process::private_directory(&state.join("commands")).unwrap();
        let command=WorkerCommand::StopAttempt{command_id:"old-command".into(),task_id:"task".into(),attempt_id:"attempt".into(),generation:1,incarnation:"old".into()};
        let path=state.join("commands/original.json");record_launch(&path,&command).unwrap();
        let bytes=std::fs::read(&path).unwrap();initialize_state(&state).unwrap();
        assert_eq!(std::fs::read(state.join("format.json")).unwrap(),marker);
        assert!(admits_instruction_sources(&state,&limits.worker).unwrap());
        assert!(!admits_supplied_turn_records(&state,&limits.worker).unwrap());
        let lease=process::WorkerLease::claim(&state.join("worker-owner.json"),&limits.worker).unwrap();
        crate::worker::read_receipt(&path,&limits).unwrap();
        finish_state_upgrade(&state,&limits,&lease).unwrap();
        assert!(admits_supplied_turn_records(&state,&limits.worker).unwrap());
        assert!(admits_instruction_sources(&state,&limits.worker).unwrap());
        assert_eq!(read_state_format(&state,&limits.worker).unwrap().format_revision,CURRENT_STATE_FORMAT);
        assert_eq!(std::fs::read(&path).unwrap(),bytes);
        initialize_state(&state).unwrap();finish_state_upgrade(&state,&limits,&lease).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(),bytes);
        assert!(matches!(crate::worker::read_receipt(&path,&limits).unwrap().command,WorkerCommand::StopAttempt{command_id,..} if command_id=="old-command"));
    }

    #[test]
    fn historical_format_inspection_is_read_only_and_owned_upgrade_preserves_records() {
        for revision in [9, 10, 11, 12, 13, 14, 15, 16, 17] {
            let (state, original_marker) = historical_state(&format!("format-{revision}-upgrade"), revision);
            let limits = historical_limits();
            process::private_directory(&state.join("commands")).unwrap();
            let command = WorkerCommand::StopAttempt {
                command_id: "retained-stop".into(),
                task_id: "task".into(),
                attempt_id: "attempt".into(),
                generation: 1,
                incarnation: "prior".into(),
            };
            let command_path = state.join("commands/retained.json");
            record_launch(&command_path, &command).unwrap();
            atomic_json(&state.join("limits.json"), &*limits).unwrap();
            let original_command = std::fs::read(&command_path).unwrap();
            let original_limits = std::fs::read(state.join("limits.json")).unwrap();
            initialize_state(&state).unwrap();
            assert_eq!(std::fs::read(state.join("format.json")).unwrap(), original_marker);
            let lease = process::WorkerLease::claim(&state.join("worker-owner.json"), &limits.worker).unwrap();
            // The actual command reader, not the marker, establishes this fixture's
            // retained command shape before the explicit outer-format migration.
            let before = crate::worker::read_receipt(&command_path, &limits).unwrap();
            assert!(!before.completed);
            assert!(!before.effects_started);
            for _ in 0..2 {
                finish_state_upgrade(&state, &limits, &lease).unwrap();
                assert_eq!(read_state_format(&state, &limits.worker).unwrap().format_revision, CURRENT_STATE_FORMAT);
                assert_eq!(std::fs::read(&command_path).unwrap(), original_command);
                assert_eq!(std::fs::read(state.join("limits.json")).unwrap(), original_limits);
                let after = crate::worker::read_receipt(&command_path, &limits).unwrap();
                assert!(!after.completed);
                assert!(!after.effects_started);
                assert_eq!(serde_json::to_value(after.command).unwrap(), serde_json::to_value(&command).unwrap());
            }
        }
    }

    #[test]
    fn invalid_or_unowned_state_cannot_cross_the_upgrade_boundary() {
        let (state, original_marker) = historical_state("format-lock", 10);
        let mut limits = test_limits().worker.clone();
        limits.owner_lock_timeout_ms = 1;
        let lease = process::WorkerLease::claim(&state.join("worker-owner.json"), &limits).unwrap();
        assert!(process::WorkerLease::claim(&state.join("worker-owner.json"), &limits).is_err());
        assert_eq!(std::fs::read(state.join("format.json")).unwrap(), original_marker);
        drop(lease);
        for (i, bytes) in [
            b"{".to_vec(),
            b"{\"format_revision\":10,\"extra\":true}".to_vec(),
            b"{\"format_revision\":10,\"format_revision\":10}".to_vec(),
            b"{\"format_revision\":8}".to_vec(),
            b"{\"format_revision\":19}".to_vec(),
            vec![b' '; limits.state_format_bytes + 1],
        ].into_iter().enumerate() {
            let state = area(&format!("invalid-format-{i}"));
            std::fs::write(state.join("format.json"), &bytes).unwrap();
            let lease = process::WorkerLease::claim(&state.join("worker-owner.json"), &limits).unwrap();
            assert!(initialize_state(&state).is_err());
            let mut metadata = (*historical_limits()).clone();
            metadata.worker = limits.clone();
            assert!(finish_state_upgrade(&state, &metadata, &lease).is_err());
            assert_eq!(std::fs::read(state.join("format.json")).unwrap(), bytes);
            assert!(!state.join("format.pending").exists());
        }
        let state = area("format-symlink");
        let target = state.join("original-marker");
        std::fs::write(&target, &original_marker).unwrap();
        std::os::unix::fs::symlink(&target, state.join("format.json")).unwrap();
        assert!(initialize_state(&state).is_err());
        assert_eq!(std::fs::read(&target).unwrap(), original_marker);
    }

    #[test]
    fn state_upgrade_faults_do_not_claim_durability_and_visible_fourteen_is_resynced() {
        use crate::worker::process::storage_tests::{Point, inject};
        for revision in [9, 10, 11, 12, 13, 14, 15, 16, 17] {
            for point in [Point::Create, Point::Write, Point::FileSync, Point::Rename, Point::DirectorySync] {
                let (state, original) = historical_state(&format!("format-{revision}-upgrade-{point:?}"), revision);
                let marker = state.join("format.json");
                let metadata = historical_limits();
                let limits = metadata.worker.clone();
                let lease = process::WorkerLease::claim(&state.join("worker-owner.json"), &limits).unwrap();
                let cut = inject(&marker, point, 28, if point == Point::Write { 4 } else { 0 });
                assert!(finish_state_upgrade(&state, &metadata, &lease).is_err());
                assert_eq!(cut.hits(), 1);
                if point != Point::DirectorySync {
                    assert_eq!(std::fs::read(&marker).unwrap(), original);
                } else {
                    assert_eq!(read_state_format(&state, &limits).unwrap().format_revision, CURRENT_STATE_FORMAT);
                    // A visible current marker must still traverse the real atomic-json barrier.
                    let second = inject(&marker, Point::DirectorySync, 28, 0);
                    assert!(finish_state_upgrade(&state, &metadata, &lease).is_err());
                    assert_eq!(second.hits(), 1);
                }
                finish_state_upgrade(&state, &metadata, &lease).unwrap();
                assert_eq!(read_state_format(&state, &limits).unwrap().format_revision, CURRENT_STATE_FORMAT);
                assert!(!state.join("format.pending").exists());
            }
        }
    }

    #[tokio::test]
    async fn failed_marker_upgrade_exits_without_outbox_replay_or_registration() {
        use crate::worker::process::storage_tests::{Point, inject};
        for point in [Point::Rename, Point::DirectorySync] {
            let root = area(&format!("format-no-replay-{point:?}"));
            let workspace = root.join("workspace");
            let state = workspace.join(".borg-worker").join(component("worker"));
            process::private_directory(&state).unwrap();
            atomic_json(&state.join("format.json"), &json!({"format_revision":9})).unwrap();
            let original_marker = std::fs::read(state.join("format.json")).unwrap();
            let limits = historical_limits();
            atomic_json(&state.join("limits.json"), &*limits).unwrap();
            process::private_directory(&state.join("outbox")).unwrap();
            let report = Report {
                report_id: "retained-before-upgrade".into(), worker_id: "worker".into(),
                incarnation: "prior".into(), task_id: "task".into(), attempt_id: "attempt".into(),
                generation: 1, observation: Observation::ProcessTreeStopped,
            };
            let report_path = state.join("outbox").join(format!("{}.json", component(&report.report_id)));
            atomic_json(&report_path, &report).unwrap();
            let original_report = std::fs::read(&report_path).unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let marker = state.join("format.json");
            let cut = inject(&marker, point, 28, 0);
            let result = tokio::time::timeout(Duration::from_secs(5), crate::worker::run(WorkerConfig {
                limits: limits.clone(), resource_profile: None,
                controller_url: format!("http://{}", listener.local_addr().unwrap()),
                id: "worker".into(), incarnation: "new".into(), workspace_root: workspace,
                codex_path: PathBuf::from("/must-not-launch"),
                coding_capacity: limits.worker.coding_capacity, native_capacity: limits.worker.native_capacity,
                native_endpoint: "http://127.0.0.1:1/no-provider".into(),
                validation_executable: PathBuf::from("/must-not-launch"),
            })).await.expect("migration error must exit, not enter recovery backoff");
            assert!(result.unwrap_err().contains("worker state format upgrade failed"));
            assert_eq!(cut.hits(), 1);
            assert!(tokio::time::timeout(Duration::from_millis(25), listener.accept()).await.is_err(),
                "neither normal replay nor shutdown may bypass the failed marker barrier");
            assert_eq!(std::fs::read(&report_path).unwrap(), original_report);
            assert!(!state.join("worker-owner.json").exists());
            if point == Point::Rename {
                assert_eq!(std::fs::read(&marker).unwrap(), original_marker);
            } else {
                assert_eq!(read_state_format(&state, &limits.worker).unwrap().format_revision, CURRENT_STATE_FORMAT);
            }
        }
    }

    #[tokio::test]
    async fn state9_failed_local_admission_preserves_marker_records_and_no_controller_send() {
        for fault in ["resource", "policy", "command"] {
            let root = area(&format!("state9-admission-{fault}"));
            let workspace = root.join("workspace");
            let state = workspace.join(".borg-worker").join(component("worker"));
            process::private_directory(&state).unwrap();
            atomic_json(&state.join("format.json"), &json!({"format_revision":9})).unwrap();
            let marker = std::fs::read(state.join("format.json")).unwrap();
            let limits = historical_limits();
            atomic_json(&state.join("limits.json"), &*limits).unwrap();
            let retained_limits = std::fs::read(state.join("limits.json")).unwrap();
            process::private_directory(&state.join("outbox")).unwrap();
            let report = Report {
                report_id: "state9-retained-stop".into(), worker_id: "worker".into(),
                incarnation: "prior".into(), task_id: "task".into(), attempt_id: "attempt".into(),
                generation: 1, observation: Observation::ProcessTreeStopped,
            };
            let report_path = state.join("outbox").join(format!("{}.json", component(&report.report_id)));
            atomic_json(&report_path, &report).unwrap();
            let retained_report = std::fs::read(&report_path).unwrap();
            let mut selected = (*limits).clone();
            let resource_profile = if fault == "resource" {
                let path = root.join("invalid-resource.json");
                std::fs::write(&path, b"{}").unwrap();
                Some(path)
            } else { None };
            if fault == "policy" { selected.revision.push_str("-changed"); }
            if fault == "command" {
                process::private_directory(&state.join("commands")).unwrap();
                std::fs::write(state.join("commands/invalid.json"), b"{}").unwrap();
            }
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let result = tokio::time::timeout(Duration::from_secs(5), Box::pin(crate::worker::run(WorkerConfig {
                limits: Arc::new(selected), resource_profile,
                controller_url: format!("http://{}", listener.local_addr().unwrap()),
                id: "worker".into(), incarnation: "replacement".into(), workspace_root: workspace,
                codex_path: PathBuf::from("/must-not-launch"),
                coding_capacity: limits.worker.coding_capacity, native_capacity: limits.worker.native_capacity,
                native_endpoint: "http://127.0.0.1:1/no-provider".into(),
                validation_executable: PathBuf::from("/must-not-launch"),
            }))).await.expect("local admission refusal must close without recovery");
            assert!(result.is_err(), "{fault} unexpectedly admitted");
            assert!(tokio::time::timeout(Duration::from_millis(25), listener.accept()).await.is_err(),
                "failed local admission cannot replay retained reports during shutdown");
            assert_eq!(std::fs::read(state.join("format.json")).unwrap(), marker);
            assert_eq!(std::fs::read(state.join("limits.json")).unwrap(), retained_limits);
            assert_eq!(std::fs::read(report_path).unwrap(), retained_report);
            if fault == "command" { assert_eq!(std::fs::read(state.join("commands/invalid.json")).unwrap(), b"{}"); }
            assert!(!state.join("worker-owner.json").exists());
        }
    }

    #[test]
    fn worker_format_twelve_refuses_unmarked_or_unsupported_state_without_rewrite() {
        let root = area("formats");
        let old = root.join("old");
        std::fs::create_dir(&old).unwrap();
        std::fs::write(old.join("command.json"), b"historical").unwrap();
        assert!(initialize_state(&old).is_err());
        assert!(!old.join("format.json").exists());
        assert_eq!(
            std::fs::read(old.join("command.json")).unwrap(),
            b"historical"
        );
        std::fs::write(old.join("format.json"), b"{\"format_revision\":1}").unwrap();
        let bytes = std::fs::read(old.join("format.json")).unwrap();
        assert!(initialize_state(&old).is_err());
        assert_eq!(std::fs::read(old.join("format.json")).unwrap(), bytes);
        let format_two = root.join("format-two");
        std::fs::create_dir(&format_two).unwrap();
        std::fs::write(format_two.join("format.json"), b"{\"format_revision\":2}").unwrap();
        let previous = std::fs::read(format_two.join("format.json")).unwrap();
        assert!(initialize_state(&format_two).is_err());
        assert_eq!(
            std::fs::read(format_two.join("format.json")).unwrap(),
            previous
        );
        let format_three = root.join("format-three");
        std::fs::create_dir(&format_three).unwrap();
        std::fs::write(format_three.join("format.json"), b"{\"format_revision\":3}").unwrap();
        let previous = std::fs::read(format_three.join("format.json")).unwrap();
        assert!(initialize_state(&format_three).is_err());
        assert_eq!(
            std::fs::read(format_three.join("format.json")).unwrap(),
            previous
        );
        let format_four = root.join("format-four");
        std::fs::create_dir(&format_four).unwrap();
        std::fs::write(format_four.join("format.json"), b"{\"format_revision\":4}").unwrap();
        let previous = std::fs::read(format_four.join("format.json")).unwrap();
        assert!(initialize_state(&format_four).is_err());
        assert_eq!(
            std::fs::read(format_four.join("format.json")).unwrap(),
            previous
        );
        let format_five = root.join("format-five");
        std::fs::create_dir(&format_five).unwrap();
        std::fs::write(format_five.join("format.json"), b"{\"format_revision\":5}").unwrap();
        let original = std::fs::read(format_five.join("format.json")).unwrap();
        assert!(initialize_state(&format_five).is_err());
        assert_eq!(
            std::fs::read(format_five.join("format.json")).unwrap(),
            original
        );
        for revision in [6, 7, 8, 19] {
            let previous = root.join(format!("format-{revision}"));
            std::fs::create_dir(&previous).unwrap();
            let bytes = serde_json::to_vec(&json!({"format_revision":revision})).unwrap();
            std::fs::write(previous.join("format.json"), &bytes).unwrap();
            std::fs::write(previous.join("command.json"), b"historical").unwrap();
            assert!(initialize_state(&previous).is_err());
            assert_eq!(std::fs::read(previous.join("format.json")).unwrap(), bytes);
            assert_eq!(
                std::fs::read(previous.join("command.json")).unwrap(),
                b"historical"
            );
        }
        let fresh = root.join("fresh");
        initialize_state(&fresh).unwrap();
        initialize_state(&fresh).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &std::fs::read(fresh.join("format.json")).unwrap()
            )
            .unwrap(),
            json!({"format_revision":CURRENT_STATE_FORMAT})
        );
    }

    fn default_assignment_command(metadata: &RuntimeLimits) -> WorkerCommand {
        let mut input = TaskInput {
            instruction_source: None, pull_request: None, resolved_limits: None,
            repository_read: None,
            // A user spelling the metadata key is not a typed selector carrier.
            instruction: "Explain task_defaults.agent_id without changing it".into(),
            definition: Definition {
                charter: None, limit_overrides: None,
                id: "format-native-agent".into(), revision: "r1".into(), executor: ExecutorKind::Native,
                inference: Some(InferenceSettings::selected()),
                execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
            },
            repository: None, acceptance_commands: vec![],
            output_schema: Some(json!({"type":"object","description":"task_defaults.agent_id"})),
            limits: metadata.task_defaults.at(process::now_ms()).unwrap(),
            dependencies: vec![], native_workflow: None,
        };
        input.resolved_limits = Some(metadata.resolve_task(&input).unwrap());
        let attempt = Attempt {
            id: "format-attempt".into(), session_id: "format-session".into(), task_id: "format-task".into(),
            generation: 1, worker_id: "worker".into(), worker_incarnation: "prior".into(),
            phase: AttemptPhase::Assigned, deadline_unix_ms: input.limits.deadline_unix_ms,
            stopped: false, uncertain: false,
        };
        WorkerCommand::RunAttempt {
            command_id: "format-command".into(),
            session: Session { id: attempt.session_id.clone(), agent_id: input.definition.id.clone(),
                definition: input.definition.clone(), task_id: attempt.task_id.clone() },
            attempt, task: input, checkpoint: None, continuation: None,
            workflow_continuation: None, completion_continuation: None,
        }
    }
    fn default_assignment_await(state: &Path, metadata: &RuntimeLimits, command: WorkerCommand) -> PathBuf {
        let mut journal = mutations::prepare(Operation::AwaitCommands {
            operation_id: "format-await".into(), worker_id: "worker".into(), incarnation: "prior".into(), acknowledged: vec![],
        }, 0, metadata).unwrap();
        journal.outcome = Some(Outcome::Ok { data: Data::Commands(vec![command]) });
        journal.outcome_context = Some(CommitContext {
            commit: MutationCommit { operation_id: "format-await".into(), controller_revision: 1 },
            observed_controller_revision: 1,
        });
        let path = state.join("mutations/await.json");
        mutations::persist(&path, &journal).unwrap();
        path
    }

    #[test]
    // Traceability scope: Typed retained metadata and command carriers only; no worker, provider or controller is launched.
    //= spec/impl/v2/implementation.md#limits-are-versioned-metadata
    //= type=test
    //# A retained operation MUST NOT be discarded solely because a later limit configuration is lower.
    fn default_assignment_worker17_upgrade_preserves_missing_selector_carriers() {
        let (state, original_marker) = historical_state("default-assignment-old17", SUPPLIED_STATE_FORMAT);
        let limits = historical_limits();
        let policy_path = state.join("limits.json");
        atomic_json(&policy_path, &*limits).unwrap();
        let command = default_assignment_command(&limits);
        let command_path = state.join("commands/original.json");
        record_launch(&command_path, &command).unwrap();
        let await_path = default_assignment_await(&state, &limits, command.clone());
        let originals: Vec<_> = [policy_path, command_path.clone(), await_path.clone()].into_iter()
            .map(|path| { let bytes = std::fs::read(&path).unwrap(); (path, bytes) }).collect();
        assert!(admits_supplied_turn_records(&state, &limits.worker).unwrap(), "17 keeps its supplied-journal meaning");
        assert!(!admits_default_assignment(&state, &limits.worker).unwrap());
        initialize_state(&state).unwrap();
        assert_eq!(std::fs::read(state.join("format.json")).unwrap(), original_marker);
        let lease = process::WorkerLease::claim(&state.join("worker-owner.json"), &limits.worker).unwrap();
        let retained = crate::worker::retained_worker_limits(&state, &limits).unwrap();
        assert!(retained.task_defaults.agent_id.is_none());
        assert!(crate::worker::same_execution_policy(&retained, &limits));
        assert_eq!(crate::worker::read_receipt(&command_path, &retained).unwrap().command, command);
        assert!(mutations::read(&await_path, &retained).unwrap().unwrap().outcome.is_some());
        finish_state_upgrade(&state, &retained, &lease).unwrap();
        assert_eq!(read_state_format(&state, &limits.worker).unwrap().format_revision, 19);
        assert!(admits_supplied_turn_records(&state, &limits.worker).unwrap());
        assert!(admits_default_assignment(&state, &limits.worker).unwrap());
        for (path, bytes) in originals { assert_eq!(std::fs::read(path).unwrap(), bytes); }
    }

    #[test]
    fn default_assignment_selector_in_each_historical_carrier_refuses_without_rewrite() {
        for carrier in ["limits", "command", "await"] {
            let (state, marker) = historical_state(&format!("default-assignment-{carrier}"), SUPPLIED_STATE_FORMAT);
            let old = historical_limits();
            let mut selected = (*old).clone(); selected.task_defaults.agent_id = Some("triage".into());
            let selected = Arc::new(selected);
            let retained = if carrier == "limits" { selected.clone() } else { old.clone() };
            let policy_path = state.join("limits.json"); atomic_json(&policy_path, &*retained).unwrap();
            let command = default_assignment_command(if carrier == "command" || carrier == "await" { &selected } else { &old });
            let carrier_path = if carrier == "await" {
                default_assignment_await(&state, &retained, command)
            } else {
                let path = state.join("commands/original.json"); record_launch(&path, &command).unwrap(); path
            };
            let policy_bytes = std::fs::read(&policy_path).unwrap();
            let carrier_bytes = std::fs::read(&carrier_path).unwrap();
            let lease = process::WorkerLease::claim(&state.join("worker-owner.json"), &old.worker).unwrap();
            let actual = crate::worker::retained_worker_limits(&state, &retained).unwrap();
            let reason = finish_state_upgrade(&state, &actual, &lease).unwrap_err();
            assert!(reason.contains("task_defaults.agent_id") && reason.contains("worker18"), "{carrier}: {reason}");
            assert_eq!(std::fs::read(state.join("format.json")).unwrap(), marker);
            assert_eq!(std::fs::read(&policy_path).unwrap(), policy_bytes);
            assert_eq!(std::fs::read(&carrier_path).unwrap(), carrier_bytes);
            assert!(!state.join("format.pending").exists());
            // The identical typed carrier is current vocabulary under18; this
            // deliberate fixture marker replacement is not a migration result.
            atomic_json(&state.join("format.json"), &json!({"format_revision":18})).unwrap();
            crate::worker::command_receipts(&state, &actual).unwrap();
            mutations::read(&state.join("mutations/await.json"), &actual).unwrap();
            finish_state_upgrade(&state, &actual, &lease).unwrap();
            assert_eq!(std::fs::read(&policy_path).unwrap(), policy_bytes);
            assert_eq!(std::fs::read(&carrier_path).unwrap(), carrier_bytes);
        }
    }

    #[test]
    fn historical_empty_root_cannot_persist_new_selector_before_marker_upgrade() {
        let (state, marker) = historical_state("default-assignment-empty17", SUPPLIED_STATE_FORMAT);
        for name in ["outbox", "processes", "codex", "mutations/settlements", "native-tracking", "publications", "pull-requests"] {
            process::private_directory(&state.join(name)).unwrap();
        }
        let mut metadata = (*historical_limits()).clone();
        metadata.task_defaults.agent_id = Some("triage".into());
        let metadata = Arc::new(metadata);
        let _lease = process::WorkerLease::claim(&state.join("worker-owner.json"), &metadata.worker).unwrap();
        let reason = crate::worker::retained_worker_limits(&state, &metadata).unwrap_err();
        assert!(reason.contains("task_defaults.agent_id") && reason.contains("worker18"), "{reason}");
        assert!(!state.join("limits.json").exists());
        assert_eq!(std::fs::read(state.join("format.json")).unwrap(), marker);
    }

    #[test]
    // Traceability scope: Original marker and carrier bytes remain unchanged on refusal; current18 accepts the same typed retained carriers, with no executor launched.
    //= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
    //= type=test
    //# A task reassignment MUST retain the task's prior session and execution-input history.
    fn reassignment_in_historical_commands_and_await_refuses_without_default_selector() {
        for carrier in ["command", "await"] {
            let (state, marker) = historical_state(&format!("reassignment-{carrier}"), SUPPLIED_STATE_FORMAT);
            let metadata = historical_limits();
            assert!(metadata.task_defaults.agent_id.is_none());
            let policy = state.join("limits.json"); atomic_json(&policy, &*metadata).unwrap();
            let mut command = default_assignment_command(&metadata);
            let WorkerCommand::RunAttempt { task, .. } = &mut command else { unreachable!() };
            let mut definition = task.definition.clone(); definition.executor = ExecutorKind::Coding; definition.id = "coding".into();
            task.native_workflow = Some(Box::new(NativeWorkflow::Reassign { target: CodingTaskTemplate {
                instruction_source: None, pull_request: None, instruction_prefix: String::new(), definition,
                repository: Repository { registration: None, url: "file:///repo".into(), start_commit: "a".repeat(40),
                    checkpoint_remote: "origin".into(), pull_request_target: None },
                acceptance_commands: vec![vec!["check".into()]], dependencies: task.dependencies.clone(), limits: task.limits.clone(),
            }}));
            let path = if carrier == "await" { default_assignment_await(&state, &metadata, command) }
                else { let path = state.join("commands/original.json"); record_launch(&path, &command).unwrap(); path };
            let policy_bytes = std::fs::read(&policy).unwrap(); let bytes = std::fs::read(&path).unwrap();
            let lease = process::WorkerLease::claim(&state.join("worker-owner.json"), &metadata.worker).unwrap();
            let reason = finish_state_upgrade(&state, &metadata, &lease).unwrap_err();
            assert!(reason.contains("reassign") && reason.contains("worker18"), "{reason}");
            assert_eq!(std::fs::read(state.join("format.json")).unwrap(), marker);
            assert_eq!(std::fs::read(&policy).unwrap(), policy_bytes);
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
            assert!(!state.join("format.pending").exists());
            atomic_json(&state.join("format.json"), &json!({"format_revision":CURRENT_STATE_FORMAT})).unwrap();
            crate::worker::command_receipts(&state, &metadata).unwrap();
            mutations::read(&state.join("mutations/await.json"), &metadata).unwrap();
            finish_state_upgrade(&state, &metadata, &lease).unwrap();
            assert_eq!(std::fs::read(&policy).unwrap(), policy_bytes);
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
        }
    }

    #[test]
    // Traceability scope: A typed saved delegation outcome is the only new carrier; marker and all original records remain byte-exact on refusal.
    //= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
    //= type=test
    //# A task reassignment MUST retain the task's prior session and execution-input history.
    fn pending_assignment_in_saved_outcome_requires_worker18_without_selector_or_reassign() {
        let (state, marker) = historical_state("pending-assignment-saved", SUPPLIED_STATE_FORMAT);
        let metadata = historical_limits();
        assert!(metadata.task_defaults.agent_id.is_none());
        process::private_directory(&state.join("delegations")).unwrap();
        let mut command = default_assignment_command(&metadata);
        let WorkerCommand::RunAttempt { attempt, task: input, .. } = &mut command else { unreachable!() };
        let mut definition = input.definition.clone();
        definition.id = "coding-child".into(); definition.executor = ExecutorKind::Coding;
        input.native_workflow = Some(Box::new(NativeWorkflow::CodingThenContinue { child: CodingTaskTemplate {
            instruction_source: None, pull_request: None, instruction_prefix: String::new(), definition,
            repository: Repository { registration: None, url: "file:///repo".into(), start_commit: "a".repeat(40),
                checkpoint_remote: "origin".into(), pull_request_target: None },
            acceptance_commands: vec![], dependencies: vec![], limits: input.limits.clone(),
        }}));
        assert!(crate::delegation::handoff_template(input).is_none());
        let path = record(&state, attempt, input, "original decision").unwrap();
        let mut saved = load(&path, attempt, input).unwrap();
        let Operation::DelegateTask { child, .. } = &saved.request else { unreachable!() };
        let mut retained = task("child", child.clone(), None);
        retained.parent_task_id = Some(attempt.task_id.clone());
        retained.pending_assignment = Some(PendingAssignment {
            operation_id: "retained-handoff".into(), from_session_id: retained.session_id.clone(),
            session: Session { id: "next-session".into(), task_id: retained.id.clone(),
                agent_id: retained.input.definition.id.clone(), definition: retained.input.definition.clone() },
        });
        saved.outcome = Some(Outcome::Ok { data: Data::Task(Box::new(retained)) });
        saved.outcome_context = Some(CommitContext {
            commit: MutationCommit { operation_id: saved.request.mutation_id().unwrap().to_owned(), controller_revision: 1 },
            observed_controller_revision: 1,
        });
        atomic_json(&path, &saved).unwrap();
        load(&path, attempt, input).unwrap();
        let command_path = state.join("commands/original.json"); record_launch(&command_path, &command).unwrap();
        let policy = state.join("limits.json"); atomic_json(&policy, &*metadata).unwrap();
        let originals: Vec<_> = [&path, &command_path, &policy].into_iter()
            .map(|p| (p.clone(), std::fs::read(p).unwrap())).collect();
        let lease = process::WorkerLease::claim(&state.join("worker-owner.json"), &metadata.worker).unwrap();
        let reason = finish_state_upgrade(&state, &metadata, &lease).unwrap_err();
        assert!(reason.contains("pending_assignment") && reason.contains("worker18"), "{reason}");
        assert_eq!(std::fs::read(state.join("format.json")).unwrap(), marker);
        assert!(!state.join("format.pending").exists());
        for (path, bytes) in &originals { assert_eq!(&std::fs::read(path).unwrap(), bytes); }
        atomic_json(&state.join("format.json"), &json!({"format_revision":CURRENT_STATE_FORMAT})).unwrap();
        finish_state_upgrade(&state, &metadata, &lease).unwrap();
        for (path, bytes) in originals { assert_eq!(std::fs::read(path).unwrap(), bytes); }
    }

    #[test]
    // Traceability scope: Real bounded native and Coding journal readers at pre-upgrade admission, using only local synthetic committed records and no transport or executor.
    //= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
    //= type=test
    //# A task reassignment MUST retain the task's prior session and execution-input history.
    fn pending_assignment_in_native_and_coding_journals_refuses_old_marker_without_rewrite() {
        for coding in [false, true] {
            let (state, marker) = historical_state(if coding {"pending-coding-journal"} else {"pending-native-journal"}, SUPPLIED_STATE_FORMAT);
            let metadata = historical_limits();
            let mut command = default_assignment_command(&metadata);
            if coding {
                let WorkerCommand::RunAttempt { task, session, .. } = &mut command else { unreachable!() };
                task.definition.executor = ExecutorKind::Coding;
                task.resolved_limits = Some(metadata.resolve_task(task).unwrap());
                session.definition = task.definition.clone();
            }
            let WorkerCommand::RunAttempt { attempt, task: input, .. } = &command else { unreachable!() };
            assert!(input.native_workflow.is_none() && metadata.task_defaults.agent_id.is_none());
            let command_path = state.join("commands/original.json"); record_launch(&command_path, &command).unwrap();
            let policy = state.join("limits.json"); atomic_json(&policy, &*metadata).unwrap();
            let config = WorkerConfig {
                limits: metadata.clone(), resource_profile: None, controller_url: "http://127.0.0.1:9".into(),
                id: attempt.worker_id.clone(), incarnation: attempt.worker_incarnation.clone(), workspace_root: state.clone(),
                codex_path: PathBuf::from("/must-not-launch"), coding_capacity: metadata.worker.coding_capacity,
                native_capacity: metadata.worker.native_capacity, native_endpoint: "http://127.0.0.1:9/no-provider".into(),
                validation_executable: PathBuf::from("/must-not-launch"),
            };
            let call = crate::mcp::Invocation {
                operation_id: "retained-task-read".into(), logical_call_id: "retained-logical-read".into(),
                caller: crate::worker::collaboration_caller(attempt), tool: "collab_task_get".into(),
                arguments: json!({"task_id":attempt.task_id}),
            };
            let directory = if coding {
                codex::state_path(&state.join("codex"), &attempt.id).join("collaboration-codex")
            } else { state.join("collaboration").join(component(&attempt.id)) };
            process::private_directory(&directory).unwrap();
            let path = directory.join(if coding {"calls.json"} else {"0000000001.json"});
            let mut retained = task(&attempt.task_id, input.clone(), Some(attempt.clone()));
            retained.session_id = attempt.session_id.clone();
            let persist = |retained: &Task| {
                let outcome = Outcome::Ok { data: Data::Task(Box::new(retained.clone())) };
                let context = CommitContext { commit: MutationCommit { operation_id: call.operation_id.clone(), controller_revision: 1 }, observed_controller_revision: 1 };
                if coding {
                    let reply = Reply { request_id: "retained-read".into(), outcome,
                        commit: Some(context.commit), observed_controller_revision: Some(1) };
                    let result = crate::mcp::result(&call, reply).unwrap();
                    atomic_json(&path, &json!({"instruction_source":null,"runtime_limits":&*metadata,
                        "limits":input.execution_limits().unwrap().collaboration,"worker_limits":metadata.worker,
                        "revision":2,"caller":call.caller,"deadline_unix_ms":attempt.deadline_unix_ms.min(input.limits.deadline_unix_ms),
                        "controller_url":config.controller_url,"entries":[{"thread_id":"thread-original","session_id":"session-original",
                        "invocation":call,"outcome":result,"delivered":true}]})).unwrap();
                } else {
                    let mut journal = mutations::prepare(Operation::CallCollaboration { call: call.clone() }, 0, &metadata).unwrap();
                    journal.outcome = Some(outcome); journal.outcome_context = Some(context); journal.consumed = true;
                    mutations::persist(&path, &journal).unwrap();
                }
            };
            persist(&retained);
            // Absence preserves the old vocabulary and is not synthesized.
            let original = std::fs::read(&path).unwrap();
            assert!(!String::from_utf8_lossy(&original).contains("pending_assignment"));
            crate::worker::admit_default_assignment(&state, &metadata).unwrap();
            if coding { assert!(crate::worker::coding_collaboration_closed(&config, input, attempt, &state).unwrap()); }
            assert_eq!(std::fs::read(&path).unwrap(), original);
            retained.pending_assignment = Some(PendingAssignment { operation_id: "pending-handoff".into(),
                from_session_id: retained.session_id.clone(), session: Session { id: "next-session".into(),
                    task_id: retained.id.clone(), agent_id: retained.input.definition.id.clone(), definition: retained.input.definition.clone() } });
            persist(&retained);
            let originals: Vec<_> = [&path, &command_path, &policy].into_iter()
                .map(|p| (p.clone(), std::fs::read(p).unwrap())).collect();
            let lease = process::WorkerLease::claim(&state.join("worker-owner.json"), &metadata.worker).unwrap();
            let reason = if coding { crate::worker::coding_collaboration_closed(&config, input, attempt, &state).unwrap_err() }
                else { finish_state_upgrade(&state, &metadata, &lease).unwrap_err() };
            assert!(reason.contains("pending_assignment") && reason.contains("worker18"), "{reason}");
            assert_eq!(std::fs::read(state.join("format.json")).unwrap(), marker);
            assert!(!state.join("format.pending").exists());
            for (path, bytes) in &originals { assert_eq!(&std::fs::read(path).unwrap(), bytes); }
            atomic_json(&state.join("format.json"), &json!({"format_revision":CURRENT_STATE_FORMAT})).unwrap();
            finish_state_upgrade(&state, &metadata, &lease).unwrap();
            if coding { assert!(crate::worker::coding_collaboration_closed(&config, input, attempt, &state).unwrap()); }
            for (path, bytes) in originals { assert_eq!(std::fs::read(path).unwrap(), bytes); }
        }
    }

    #[test]
    fn identity_worker18_upgrade_preserves_original_carriers() {
        let (state,marker)=historical_state("identity-old18",18);
        let metadata=historical_limits();
        let policy=state.join("limits.json");atomic_json(&policy,&*metadata).unwrap();
        let command=default_assignment_command(&metadata);
        let path=state.join("commands/original.json");record_launch(&path,&command).unwrap();
        let await_path=default_assignment_await(&state,&metadata,command);
        let originals:Vec<_>=[policy,path,await_path].into_iter().map(|p|{let b=std::fs::read(&p).unwrap();(p,b)}).collect();
        assert!(!admits_identity_administration(&state,&metadata.worker).unwrap());
        initialize_state(&state).unwrap();assert_eq!(std::fs::read(state.join("format.json")).unwrap(),marker);
        let lease=process::WorkerLease::claim(&state.join("worker-owner.json"),&metadata.worker).unwrap();
        finish_state_upgrade(&state,&metadata,&lease).unwrap();
        assert_eq!(read_state_format(&state,&metadata.worker).unwrap().format_revision,19);
        for (p,b) in originals {assert_eq!(std::fs::read(p).unwrap(),b)}
    }
    #[test]
    fn identity_worker18_rejects_new_policy_inputs_and_refused_calls_without_rewrite() {
        for carrier in ["limits","command","await","pending-call","refused-call"] {
            let (state,marker)=historical_state(&format!("identity-{carrier}"),18);
            let old=historical_limits();let mut new=(*old).clone();
            new.task_creation=Some(crate::limits::TaskCreationPolicy{version:1,revision:"creation1".into(),enabled:true});
            let retained=if carrier=="limits" {&new} else {old.as_ref()};
            let policy=state.join("limits.json");atomic_json(&policy,retained).unwrap();
            let command=default_assignment_command(if carrier=="command"{&new}else{&old});
            let command_path=state.join("commands/original.json");record_launch(&command_path,&command).unwrap();
            let mut paths=vec![policy,command_path];
            if carrier=="await"{paths.push(default_assignment_await(&state,&old,default_assignment_command(&new)));}
            if carrier.ends_with("call") {
                let WorkerCommand::RunAttempt{attempt,..}=&command else{unreachable!()};
                let call=crate::mcp::Invocation{operation_id:"identity-original".into(),logical_call_id:"call1".into(),caller:crate::mcp::Caller{task_id:attempt.task_id.clone(),attempt_id:attempt.id.clone(),generation:attempt.generation,worker_id:attempt.worker_id.clone(),incarnation:attempt.worker_incarnation.clone()},tool:"collab_task_create".into(),arguments:json!({"agent_id":null})};
                let mut journal=mutations::prepare(call.operation(),0,&old).unwrap();
                if carrier=="refused-call" {
                    journal.outcome=Some(Outcome::Refused{code:"invalid_input".into(),message:"explicit null identity refused".into()});
                    journal.outcome_context=Some(CommitContext{commit:MutationCommit{operation_id:call.operation_id,controller_revision:1},observed_controller_revision:1});
                }
                let directory=state.join("collaboration").join(component(&attempt.id));process::private_directory(&directory).unwrap();
                let path=directory.join("0000000001.json");mutations::persist(&path,&journal).unwrap();paths.push(path);
            }
            let originals:Vec<_>=paths.into_iter().map(|p|{let b=std::fs::read(&p).unwrap();(p,b)}).collect();
            let lease=process::WorkerLease::claim(&state.join("worker-owner.json"),&old.worker).unwrap();
            let reason=finish_state_upgrade(&state,retained,&lease).unwrap_err();assert!(reason.contains("worker19"),"{carrier}: {reason}");
            assert_eq!(std::fs::read(state.join("format.json")).unwrap(),marker);assert!(!state.join("format.pending").exists());
            for (p,b) in &originals {assert_eq!(std::fs::read(p).unwrap(),*b)}
            // Same typed records are legal under the new marker; this deliberate
            // fixture marker change is not a successful historical migration.
            atomic_json(&state.join("format.json"),&json!({"format_revision":19})).unwrap();
            finish_state_upgrade(&state,retained,&lease).unwrap();
            for (p,b) in originals {assert_eq!(std::fs::read(p).unwrap(),b)}
        }
    }
    #[test]
    fn identity_empty_historical_root_refuses_policy_before_any_metadata_write() {
        for manager in [false,true] {
            let (state,marker)=historical_state(&format!("identity-empty18-{manager}"),18);
            for name in ["outbox","processes","codex","mutations/settlements","native-tracking","publications","pull-requests"] {process::private_directory(&state.join(name)).unwrap();}
            let mut metadata=(*historical_limits()).clone();
            if manager {metadata.identity_administration=Some(crate::limits::IdentityAdministrationPolicy{version:1,revision:"manager1".into(),enabled:true,manager_agent_id:"charter".into(),complete_definitions:true});}
            else {metadata.task_creation=Some(crate::limits::TaskCreationPolicy{version:1,revision:"creation1".into(),enabled:true});}
            let metadata=Arc::new(metadata);let _lease=process::WorkerLease::claim(&state.join("worker-owner.json"),&metadata.worker).unwrap();
            let reason=crate::worker::retained_worker_limits(&state,&metadata).unwrap_err();assert!(reason.contains("worker19"),"{reason}");
            assert!(!state.join("limits.json").exists());assert_eq!(std::fs::read(state.join("format.json")).unwrap(),marker);
        }
    }

}
