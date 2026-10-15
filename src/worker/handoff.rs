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

pub(super) fn initialize_state(state: &Path, limits: &WorkerLimits) -> Result<(), String> {
    if state.exists() {
        if !std::fs::symlink_metadata(state)
            .map_err(|e| e.to_string())?
            .file_type()
            .is_dir()
        {
            return Err("worker state is not a directory".into());
        }
        let marker = state.join("format.json");
        if marker.exists() {
            use std::io::Read;
            use std::os::unix::fs::OpenOptionsExt;
            let file = std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(0x20000 | 0x800)
                .open(&marker)
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
            if format.format_revision != 9 {
                return Err("unsupported worker state format; no migration performed".into());
            }
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
    let format = Format { format_revision: 9 };
    bounded_json_size(&format, limits.state_format_bytes).map_err(|reason| format!("{reason} (worker.state_format_bytes)"))?;
    atomic_json(&state.join("format.json"), &format)
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
            blocked_reason: None,
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
        let mut input = TaskInput {
            resolved_limits: None,
            repository_read: None,
            instruction: "parent original instruction".into(),
            definition: Definition {
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
                child: CodingTaskTemplate {
                    instruction_prefix: "fixed prefix".into(),
                    definition: Definition {
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
    #[test]
    fn worker_format_nine_refuses_unmarked_or_old_state_without_rewrite() {
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
        for revision in [6, 7, 8] {
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
            json!({"format_revision":9})
        );
    }
}
