//! Actual controller/API and local Git qualification of cleanup authority.
//! Executor completion/check results are controlled inputs, not Codex execution.
use super::*;
use crate::controller::{Controller, ControllerConfig};

fn git_fixture(root: &Path, arguments: &[&str]) -> String {
    let output = std::process::Command::new("/usr/bin/git")
        .args(["-c", "core.hooksPath=/dev/null", "-c", "user.name=Cleanup Fixture",
            "-c", "user.email=cleanup@example.invalid"])
        .args(arguments).current_dir(root).output().unwrap();
    assert!(output.status.success(), "Git fixture: {}", String::from_utf8_lossy(&output.stderr));
    String::from_utf8(output.stdout).unwrap().trim().into()
}

fn task_value(reply: Reply) -> Task {
    match reply.outcome {
        Outcome::Ok { data: Data::Task(task) } => *task,
        other => panic!("expected canonical Task: {other:?}"),
    }
}

struct Lab {
    root: PathBuf,
    state: PathBuf,
    workspace: PathBuf,
    config: WorkerConfig,
    client: ApiClient,
    controller: Controller,
    server: tokio::task::JoinHandle<Result<(), String>>,
    command: WorkerCommand,
    attempt: Attempt,
    input: TaskInput,
    control: Control,
    _cancel: watch::Sender<bool>,
    command_path: PathBuf,
}

impl Lab {
    async fn open(label: &str) -> Self {
        Self::with_grace(label, 0).await
    }
    async fn with_grace(label: &str, unsuccessful_grace_ms: u64) -> Self {
        Self::with_retention(label, 0, unsuccessful_grace_ms, 604_800_000).await
    }
    async fn with_retention(label: &str, success_grace_ms: u64, unsuccessful_grace_ms: u64,
        idle_cache_grace_ms: u64) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let root = std::env::temp_dir().join(format!("borg-cleanup-controller-{label}-{}-{}-{}",
            std::process::id(), process::now_ms(), NEXT.fetch_add(1, Ordering::Relaxed)));
        std::fs::create_dir(&root).unwrap();
        let state = root.join("state");
        let seed = root.join("seed");std::fs::create_dir(&seed).unwrap();
        git_fixture(&seed, &["init", "--quiet"]);
        std::fs::write(seed.join("source.txt"), "durable source\n").unwrap();
        git_fixture(&seed, &["add", "source.txt"]);
        git_fixture(&seed, &["commit", "--quiet", "-m", "initial"]);
        let commit = git_fixture(&seed, &["rev-parse", "HEAD"]);
        let remote = root.join("central.git");
        git_fixture(&root, &["clone", "--bare", "--quiet", "--no-hardlinks",
            seed.to_str().unwrap(), remote.to_str().unwrap()]);
        let registration = crate::repository_catalogue::Registration {
            pull_request_target: None,
            repository_id: "cleanup-fixture".into(), registration_revision: "revision-1".into(),
            display_name: "Cleanup fixture".into(), source_endpoint: format!("file://{}", remote.display()),
            checkpoint_endpoint: remote.to_string_lossy().into_owned(),
            source_store_key: "cleanup-source".into(), checkpoint_store_key: "cleanup-checkpoint".into(),
        };
        let mut metadata = RuntimeLimits::seed().unwrap();
        metadata.controller.command_wait_ms = 1;
        // Existing canonical-gate cases select zero grace; the exact-boundary
        // case selects the published 24-hour failed/cancelled retention.
        metadata.retention = Some(crate::limits::RetentionLimits { worker: WorkerRetentionLimits {
            success_grace_ms, failed_grace_ms: unsuccessful_grace_ms, cancelled_grace_ms: unsuccessful_grace_ms,
            idle_cache_grace_ms, batch_entries: 32, filesystem_entries_per_yield: 32,
            operation_timeout_ms: 30_000, retry_delay_ms: 1, record_bytes: 1_048_576,
        }});
        metadata.validate().unwrap();let metadata = Arc::new(metadata);
        handoff::initialize_state(&state, &metadata.worker).unwrap();
        for name in ["commands", "outbox", "processes", "publications", "codex"] {
            process::private_directory(&state.join(name)).unwrap();
        }
        let controller = Controller::open_with_config(root.join("controller.sqlite"), ControllerConfig {
            limits: metadata.clone(), ..ControllerConfig::default()
        }).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(crate::server::serve(controller.clone(), listener, url.clone()));
        let client = ApiClient::new(&url, metadata.clone()).unwrap();
        let config = WorkerConfig {
            limits: metadata.clone(), resource_profile: None, controller_url: url,
            id: "cleanup-worker".into(), incarnation: "cleanup-incarnation".into(),
            workspace_root: root.join("material"), codex_path: "/usr/bin/false".into(),
            coding_capacity: 1, native_capacity: 0, native_endpoint: "http://127.0.0.1:1/v1/responses".into(),
            validation_executable: std::env::current_exe().unwrap(),
        };
        let call = |operation| client.call(operation);
        assert!(matches!(Box::pin(call(Operation::RegisterRepository {
            operation_id: "cleanup-register-repository".into(), registration: registration.clone(),
        })).await.unwrap().outcome, Outcome::Ok { .. }));
        let input = TaskInput { instruction_source: None,
            pull_request: None,
            resolved_limits: None, repository_read: None, instruction: "Controlled cleanup source fixture".into(),
            definition: Definition {
                charter: None,
                id: format!("cleanup-{label}"), revision: "revision-1".into(), executor: ExecutorKind::Coding,
                inference: Some(InferenceSettings::selected()),
                execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }), limit_overrides: None,
            },
            repository: Some(Repository { pull_request_target: None,  registration: Some(registration.binding()), url: String::new(),
                start_commit: commit, checkpoint_remote: String::new() }),
            acceptance_commands: vec![vec!["/usr/bin/true".into()]], output_schema: None,
            limits: Limits { deadline_unix_ms: process::now_ms() + 240_000, model_requests: 10,
                output_bytes: 16_384, result_bytes: metadata.task_defaults.result_bytes, max_delegation_depth: 4 },
            dependencies: vec![], native_workflow: None,
        };
        let submitted = task_value(Box::pin(call(Operation::SubmitTask {
            operation_id: "cleanup-submit".into(), input,
        })).await.unwrap());
        assert!(matches!(Box::pin(call(Operation::RegisterWorker {
            operation_id: "cleanup-register-worker".into(), worker: Worker {
                id: config.id.clone(), incarnation: config.incarnation.clone(), coding_capacity: 1, native_capacity: 0,
            },
        })).await.unwrap().outcome, Outcome::Ok { .. }));
        let reply = Box::pin(call(Operation::AwaitCommands {
            operation_id: "cleanup-first-command".into(), worker_id: config.id.clone(),
            incarnation: config.incarnation.clone(), acknowledged: vec![],
        })).await.unwrap();
        let Outcome::Ok { data: Data::Commands(commands) } = reply.outcome else { panic!("run absent") };
        let command = commands.into_iter().find(|c| matches!(c, WorkerCommand::RunAttempt { .. })).unwrap();
        let WorkerCommand::RunAttempt { attempt, task: input, command_id, .. } = &command else { unreachable!() };
        assert_eq!(attempt.task_id, submitted.id);
        let attempt = attempt.clone();let input = input.clone();
        let command_path = state.join("commands").join(format!("{}.json", component(command_id)));
        atomic_json(&command_path, &Receipt { command: command.clone(), completed: true, effects_started: true }).unwrap();
        let (cancel, receive) = watch::channel(false);
        let validation = Arc::new(crate::native::ValidationRuntime::new(Arc::new(metadata.worker.clone())).unwrap());
        let control = Control { deadline_ms: input.limits.deadline_unix_ms, runtime_limits: metadata.clone(),
            worker_limits: Arc::new(metadata.worker.clone()), validation_runtime: validation, cancel: receive };
        let workspace = Box::pin(git::checkout_cached(&config.workspace_root.join("repositories"),
            &config.workspace_root.join("workspaces"), &attempt.id, input.repository.as_ref().unwrap(), None,
            &state.join("processes").join(component(&attempt.id)), &control)).await.unwrap();
        Self { root, state, workspace, config, client, controller, server, command, attempt, input,
            control, _cancel: cancel, command_path }
    }
    async fn call(&self, operation: Operation) -> Reply { Box::pin(self.client.call(operation)).await.unwrap() }
    async fn task(&self) -> Task { task_value(self.call(Operation::GetTask { task_id: self.attempt.task_id.clone() }).await) }
    async fn report(&self, id: &str, observation: Observation) -> Reply {
        let a = &self.attempt;
        let reply = self.call(Operation::ObserveWorker { report: Report { report_id: id.into(),
            worker_id: a.worker_id.clone(), incarnation: a.worker_incarnation.clone(), task_id: a.task_id.clone(),
            attempt_id: a.id.clone(), generation: a.generation, observation } }).await;
        assert!(matches!(reply.outcome, Outcome::Ok { .. }), "controlled report: {:?}", reply.outcome);reply
    }
    async fn prepare_checkpoint(&self) -> (Checkpoint, publication::Phase) {
        self.report(&format!("{}-completed-association", self.attempt.id), Observation::Started {
            thread_id: Some("controlled-thread".into()), turn_id: Some("controlled-turn".into()),
        }).await;
        self.report(&format!("{}-completed-executor", self.attempt.id), Observation::ExecutorOutcome {
            success: true, summary: "Controlled completion, no supplied executor invocation".into(),
        }).await;
        let reporter = Reporter {
            resource: None, client: self.client.clone(), config: self.config.clone(), attempt: self.attempt.clone(),
            outbox: self.state.join("outbox"), sequence: Arc::new(AtomicU64::new(1)),
            executor_seen: Arc::new(AtomicBool::new(true)), provider_uncertain: Arc::new(AtomicBool::new(false)),
            uncertainty_retained: Arc::new(AtomicBool::new(false)), output_limit: self.input.limits.output_bytes,
            result_limit: self.input.limits.result_bytes, coding_limits: self.input.execution_limits().unwrap().coding.clone(),
            outbox_lock: Arc::new(tokio::sync::Mutex::new(())),
        };
        let cp = git::checkpoint(self.input.repository.as_ref().unwrap(), &self.attempt.task_id, &self.attempt.id,
            self.input.repository.as_ref().unwrap().start_commit.clone()).unwrap();
        let mut phase = Box::pin(publication::prepare(&self.state, &self.command, &reporter, &cp, &self.control)).await.unwrap();
        phase.mark_started(&self.state, &self.attempt).unwrap();
        Box::pin(git::publish(&self.workspace, &cp,
            &self.state.join("processes").join(component(&self.attempt.id)), &self.control)).await.unwrap();
        (cp, phase)
    }
    async fn acknowledge(&self, cp: &Checkpoint, phase: &mut publication::Phase) -> Reply {
        let reply = self.call(Operation::SettleOperation {
            settlement_id: phase.settlement_id.clone(), operation_id: cp.operation_id.clone(),
            task_id: self.attempt.task_id.clone(), attempt_id: self.attempt.id.clone(), generation: self.attempt.generation,
            outcome: json!({"acknowledged":true}), checkpoint: Some(cp.clone()),
        }).await;
        assert!(matches!(reply.outcome, Outcome::Ok { .. }));
        phase.retire(&self.state, &self.attempt).unwrap();reply
    }
    async fn step(&self, scheduler: &mut Scheduler) {
        Box::pin(scheduler.step(&self.config, &self.state, &self.client, &BTreeMap::new(),
            &self.control.validation_runtime)).await;
    }
    fn record(&self) -> Record {
        read_record(&record_path(&self.state, &self.attempt.id),
            self.config.limits.retention.as_ref().unwrap().worker.record_bytes).unwrap().unwrap()
    }
    async fn close(self) {
        let Self { client, controller, server, .. } = self;
        drop(client);server.abort();let _ = server.await;drop(controller);
        tokio::task::yield_now().await;
    }
}

fn assert_deletion_exclusions(line: &str) {
    assert!(line.contains("scope=owned-disposable-worker-copies"));
    assert!(line.contains("excluded=durable-task-results,history,checkpoints,audit-metadata,operation-receipts,recovery-journals,worker-state-and-logs-outside-disposable-paths,other-worktrees,other-repository-caches"));
    assert!(line.contains("excluded-copies-if-present=database-pages,replicas,exports,backups"));
    assert!(line.contains("cache-policy=dependency-holds-and-grace"));
    assert!(line.ends_with("cryptographic-erasure=not-guaranteed"));
    assert!(!line.contains('\n'), "one bounded result line, not retained content");
}

//= spec/impl/v2/implementation.md#deletion-does-not-promise-cryptographic-erasure
//# A deletion result MUST identify the retained-data classes excluded from its guarantee.
#[tokio::test]
async fn cleanup_controller_acknowledged_result_preserves_original_records() {
    //= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
    //# A workspace MUST become eligible for cleanup only after its canonical task is terminal and its bound attempt's execution descendants have stopped.
    //= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
    //# Cleanup MUST preserve durable task results, histories, checkpoints, audit metadata, operation receipts and recovery journals in the initial profile.
    let lab = Lab::open("accepted").await;let mut scheduler = Scheduler::open(&lab.config, &lab.state).unwrap();
    lab.step(&mut scheduler).await;assert!(lab.workspace.exists());assert_eq!(lab.record().reason, Reason::NotTerminal);
    let (cp, mut phase) = lab.prepare_checkpoint().await;lab.acknowledge(&cp, &mut phase).await;
    lab.report("cleanup-success-stop", Observation::ProcessTreeStopped).await;
    lab.report("cleanup-success-result", Observation::ResultCandidate { checks_passed: true,
        summary: "Controlled accepted source".into(), checkpoint: Some(cp.clone()),
        validation: vec!["Controlled check passed".into()], output: None }).await;
    let before = lab.task().await;assert_eq!(before.status, TaskStatus::Succeeded);
    let limits = lab.call(Operation::QueryCollaboration { query: crate::collaboration::Query::Limits { task_id: None } }).await;
    assert!(matches!(limits.outcome, Outcome::Ok { data: Data::Collaboration(crate::collaboration::QueryResult::Limits { global, .. }) }
        if global.retention == lab.config.limits.retention));
    let operation = Operation::GetOperation { operation_id: cp.operation_id.clone() };
    let original = lab.call(operation.clone()).await;
    let result_operation = Operation::GetOperation { operation_id: "cleanup-success-result".into() };
    let original_result = lab.call(result_operation.clone()).await;
    let history_query = Operation::QueryCollaboration { query: crate::collaboration::Query::TaskHistory {
        task_id: lab.attempt.task_id.clone(), after: None, limit: 100,
    }};
    let original_history = lab.call(history_query.clone()).await;
    let receipt = std::fs::read(&lab.command_path).unwrap();
    let phase_path = publication::path(&lab.state, &lab.attempt);let phase_bytes = std::fs::read(&phase_path).unwrap();
    scheduler.notify();lab.step(&mut scheduler).await;
    assert!(!lab.workspace.exists());assert!(lab.record().worktrees.iter().all(|slot|slot.done));
    assert_eq!(lab.record().reason, Reason::CacheGrace);
    assert_eq!(lab.record().disposition, Disposition::Retained,
        "completed worktree disposal with future cache grace is retained, not in-progress removal");
    let record = lab.record();
    let stored = serde_json::to_vec(&record).unwrap();
    let aggregate = result_line(&record, None);
    assert!(aggregate.starts_with(&format!("BORG_CLEANUP Retained CacheGrace attempt={}", lab.attempt.id)));
    assert!(aggregate.contains("repository-cache=cleanup-incomplete"));
    assert_deletion_exclusions(&aggregate);
    assert!(cache_directory(&lab).is_dir(), "worktree disposal must not imply cache disposal");
    for kind in ["workspaces", "validation"] {
        let line = result_line(&record, Some(kind));
        assert!(line.starts_with(&format!("BORG_CLEANUP reclaimed worktree kind={kind} attempt={}", lab.attempt.id)));
        assert!(line.contains("repository-cache=outside-this-worktree-result"));
        assert_deletion_exclusions(&line);
        assert!(line.len() <= lab.attempt.id.len() + kind.len() + 600,
            "disclosure adds only fixed text, never stored source or receipt contents");
    }
    assert_eq!(serde_json::to_vec(&record).unwrap(), stored);
    assert_eq!(std::fs::read(record_path(&lab.state, &lab.attempt.id)).unwrap(), stored);
    assert_eq!(lab.task().await, before);
    let after = lab.call(operation).await;assert_eq!(after.outcome, original.outcome);assert_eq!(after.commit, original.commit);
    let after_result = lab.call(result_operation).await;
    assert_eq!(after_result.outcome, original_result.outcome);assert_eq!(after_result.commit, original_result.commit);
    assert_eq!(lab.call(history_query).await.outcome, original_history.outcome);
    assert_eq!(std::fs::read(&lab.command_path).unwrap(), receipt);assert_eq!(std::fs::read(phase_path).unwrap(), phase_bytes);
    assert_eq!(git_fixture(&lab.root.join("central.git"), &["rev-parse", &cp.remote_ref]), cp.commit);
    lab.close().await;
}

#[tokio::test]
async fn cleanup_controller_late_checkpoint_ack_reclaims_without_new_task() {
    //= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
    //# Workspace cleanup MUST preserve source edits required to complete or reconcile an outstanding checkpoint publication.
    // Scope: This case proves the original outstanding publication holds the workspace until its exact ACK; dirty-source protection is separately exercised by Git tests.
    //= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
    //# Cleanup MUST preserve data required to reconcile an uncertain or unacknowledged operation.
    let lab = Lab::open("late-ack").await;let (cp, mut phase) = lab.prepare_checkpoint().await;
    let current = lab.task().await;
    assert!(matches!(lab.call(Operation::CancelTask { operation_id: "cleanup-cancel".into(),
        task_id: current.id, expected_revision: current.revision }).await.outcome, Outcome::Ok { .. }));
    lab.report("cleanup-cancel-stop", Observation::ProcessTreeStopped).await;
    let mut scheduler = Scheduler::open(&lab.config, &lab.state).unwrap();lab.step(&mut scheduler).await;
    assert!(lab.workspace.exists());assert_eq!(lab.record().reason, Reason::UncertainEffect);
    assert!(lab.task().await.attempt.unwrap().uncertain);
    let waiting = scheduler.deadline().expect("retained item schedules bounded retry");
    lab.acknowledge(&cp, &mut phase).await;
    let before = lab.task().await;assert_eq!(before.status, TaskStatus::Cancelled);assert!(!before.attempt.as_ref().unwrap().uncertain);
    let command_bytes = std::fs::read(&lab.command_path).unwrap();
    // No new task or notify(): the existing recorded retry deadline discovers
    // the late original ACK after the controller has committed it.
    tokio::time::sleep(Duration::from_millis(waiting.saturating_sub(process::now_ms()))).await;
    lab.step(&mut scheduler).await;
    assert!(!lab.workspace.exists());assert!(lab.record().worktrees.iter().all(|slot|slot.done));
    assert_eq!(lab.task().await, before);assert_eq!(std::fs::read(&lab.command_path).unwrap(), command_bytes);
    assert_eq!(std::fs::read_dir(lab.state.join("commands")).unwrap().count(), 1);
    lab.close().await;
}

#[tokio::test]
async fn cleanup_controller_retry_current_attempt_change_holds_old_workspace() {
    //= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
    //# Cleanup MUST preserve data required by an active attempt.
    let lab = Lab::open("retry").await;
    lab.report("cleanup-failed-executor", Observation::ExecutorOutcome { success: false, summary: "Controlled failure".into() }).await;
    lab.report("cleanup-failed-stop", Observation::ProcessTreeStopped).await;
    lab.report("cleanup-failed-result", Observation::ResultCandidate { checks_passed: false,
        summary: "Controlled failed source".into(), checkpoint: None, validation: vec![], output: None }).await;
    let failed = lab.task().await;assert_eq!(failed.status, TaskStatus::Failed);
    assert!(matches!(lab.call(Operation::RetryTask { operation_id: "cleanup-retry".into(), task_id: failed.id,
        expected_revision: failed.revision }).await.outcome, Outcome::Ok { .. }));
    let reply = lab.call(Operation::AwaitCommands { operation_id: "cleanup-successor-command".into(),
        worker_id: lab.config.id.clone(), incarnation: lab.config.incarnation.clone(), acknowledged: vec![] }).await;
    let Outcome::Ok { data: Data::Commands(commands) } = reply.outcome else { panic!("successor absent") };
    assert!(commands.iter().any(|c|matches!(c,WorkerCommand::RunAttempt{attempt,..}if attempt.generation==lab.attempt.generation+1)));
    let before = lab.task().await;let receipt = std::fs::read(&lab.command_path).unwrap();
    let mut scheduler = Scheduler::open(&lab.config, &lab.state).unwrap();lab.step(&mut scheduler).await;
    assert!(lab.workspace.join("source.txt").is_file());assert_eq!(lab.record().reason, Reason::CanonicalBinding);
    assert_eq!(lab.task().await, before);assert_eq!(std::fs::read(&lab.command_path).unwrap(), receipt);
    lab.close().await;
}

#[tokio::test]
async fn cleanup_controller_unsuccessful_dirty_workspace_has_exact_24h_boundary() {
    //= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
    //# Automatic cleanup MUST use per-kind retention settings from worker metadata.
    // Actual controller/API/Git cleanup at controlled observation times, not a
    // sleep or a substitute eligibility predicate. Failed and cancelled edits
    // are disposable only after the chosen grace and all recovery holds clear.
    const GRACE: u64 = 86_400_000;
    for cancelled in [false, true] {
        let lab = Lab::with_grace(if cancelled { "cancelled-24h" } else { "failed-24h" }, GRACE).await;
        std::fs::write(lab.workspace.join("source.txt"), "uncheckpointed tracked edit\n").unwrap();
        std::fs::write(lab.workspace.join("untracked.txt"), "uncheckpointed untracked edit\n").unwrap();
        if cancelled {
            let current = lab.task().await;
            assert!(matches!(lab.call(Operation::CancelTask { operation_id: "cleanup-grace-cancel".into(),
                task_id: current.id, expected_revision: current.revision }).await.outcome, Outcome::Ok { .. }));
            lab.report("cleanup-grace-cancel-stop", Observation::ProcessTreeStopped).await;
        } else {
            lab.report("cleanup-grace-executor", Observation::ExecutorOutcome { success: false, summary: "Controlled failure".into() }).await;
            lab.report("cleanup-grace-failed-stop", Observation::ProcessTreeStopped).await;
            lab.report("cleanup-grace-failed-result", Observation::ResultCandidate { checks_passed: false,
                summary: "Controlled failed source".into(), checkpoint: None, validation: vec![], output: None }).await;
        }
        let before = lab.task().await;
        assert_eq!(before.status, if cancelled { TaskStatus::Cancelled } else { TaskStatus::Failed });
        assert!(before.attempt.as_ref().unwrap().stopped);
        assert!(!before.attempt.as_ref().unwrap().uncertain);
        let command_bytes = std::fs::read(&lab.command_path).unwrap();
        let policy = lab.config.limits.retention.as_ref().unwrap().worker.clone();
        let _scheduler = Scheduler::open(&lab.config, &lab.state).unwrap();
        let safe = process::now_ms();
        for observed in [safe, safe + GRACE - 1] {
            let due = Box::pin(attempt_at(&lab.config, &lab.state, &lab.client, &BTreeMap::new(),
                &lab.control.validation_runtime, &policy, &lab.config.limits.revision, &lab.command_path, observed)).await.unwrap();
            assert_eq!(due, Some(safe + GRACE));
            let record = lab.record();assert_eq!(record.first_safe_ms, Some(safe));assert_eq!(record.reason, Reason::Grace);
            assert_eq!(std::fs::read(lab.workspace.join("source.txt")).unwrap(), b"uncheckpointed tracked edit\n");
            assert_eq!(std::fs::read(lab.workspace.join("untracked.txt")).unwrap(), b"uncheckpointed untracked edit\n");
            assert_eq!(lab.task().await, before);
        }
        Box::pin(attempt_at(&lab.config, &lab.state, &lab.client, &BTreeMap::new(),
            &lab.control.validation_runtime, &policy, &lab.config.limits.revision, &lab.command_path, safe + GRACE)).await.unwrap();
        assert!(!lab.workspace.exists());
        assert!(lab.record().worktrees.iter().all(|slot|slot.done));
        assert_eq!(lab.task().await, before);
        assert_eq!(std::fs::read(&lab.command_path).unwrap(), command_bytes);
        lab.close().await;
    }
}

fn process_snapshot(pid: u32) -> Option<(u64, char)> {
    let text=std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let tail=text.rsplit_once(") ")?.1;let fields=tail.split_whitespace().collect::<Vec<_>>();
    Some((fields.get(19)?.parse().ok()?,fields.first()?.chars().next()?))
}

#[tokio::test]
async fn cleanup_interrupted_real_git_is_reconciled_before_subsequent_work() {
    //= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
    //# Cleanup MUST use bounded work under its configured metadata policy.
    // Scope: real registered Git/child cancellation and the same interrupted
    // cleanup reconciliation gate. The alias is controlled fixture input, not a
    // production removal command or a claim that all Git failures are covered.
    let lab=Lab::open("interrupted-git").await;
    let before=lab.task().await;let command_bytes=std::fs::read(&lab.command_path).unwrap();
    let mut scheduler=Scheduler::open(&lab.config,&lab.state).unwrap();
    scheduler.pending_path=Some(lab.command_path.clone());
    let registry=lab.state.join("processes").join(format!("cleanup-{}",component(&lab.attempt.id)));
    process::private_directory(&registry).unwrap();
    let marker=lab.root.join("cleanup-git-entered");
    let args=vec!["-c".into(),"alias.cleanup-block=!f() { printf '%s' \"$$\" > \"$1\"; exec sleep 60; }; f".into(),
        "cleanup-block".into(),marker.display().to_string()];
    let mut running=Box::pin(process::command(Path::new("/usr/bin/git"),&args,&lab.workspace,&registry,&lab.control,4096));
    tokio::select! {
        result=&mut running=>panic!("blocking Git helper ended before cancellation: {result:?}"),
        _=async {
            let deadline=tokio::time::Instant::now()+Duration::from_secs(5);
            while !marker.exists() {
                assert!(tokio::time::Instant::now()<deadline,"actual Git alias did not enter");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }=>{}
    }
    let records=std::fs::read_dir(&registry).unwrap().map(|e|e.unwrap().path())
        .filter(|p|p.extension().and_then(|s|s.to_str())==Some("json")).collect::<Vec<_>>();
    assert_eq!(records.len(),1,"actual helper must have one durable process root");
    let process_record:serde_json::Value=serde_json::from_slice(&std::fs::read(&records[0]).unwrap()).unwrap();
    let root_pid=u32::try_from(process_record["pid"].as_u64().unwrap()).unwrap();
    let root_start=process_record["start_ticks"].as_u64().unwrap();
    let child_pid: u32=std::fs::read_to_string(&marker).unwrap().parse().unwrap();
    let child_start=process_snapshot(child_pid).expect("blocking child is actually live").0;
    assert_eq!(process_snapshot(root_pid).unwrap().0,root_start);
    // Same cancellation shape as dropping cleanup.step when an actual command
    // wins the worker select. Drop alone cannot discard the retained registry.
    drop(running);
    assert!(scheduler.pending_path.is_some());
    Box::pin(scheduler.reconcile_interrupted(&lab.config,&lab.state)).await.unwrap();
    assert!(scheduler.pending_path.is_none());
    assert!(!process_obligation(&registry).unwrap());
    assert_eq!(std::fs::read_dir(&registry).unwrap().map(|e|e.unwrap().path())
        .filter(|p|p.extension().and_then(|s|s.to_str())==Some("json")).count(),0);
    assert!(registry.join("home/code").is_dir(),"owned HOME is retained, not a process obligation");
    let reaped=tokio::time::Instant::now()+Duration::from_secs(5);
    while process_snapshot(root_pid).is_some_and(|(start,_)|start==root_start) {
        assert!(tokio::time::Instant::now()<reaped,"original Git root was not reaped");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!process_snapshot(child_pid).is_some_and(|(start,state)|start==child_start&&state!='Z'&&state!='X'),
        "original blocking descendant remains live");
    assert_eq!(lab.task().await,before);assert_eq!(std::fs::read(&lab.command_path).unwrap(),command_bytes);
    let next_args=vec!["rev-parse".into(),"HEAD".into()];
    let next=Box::pin(process::command(Path::new("/usr/bin/git"),&next_args,&lab.workspace,&registry,&lab.control,4096)).await.unwrap();
    assert!(next.success);assert_eq!(next.stdout.trim(),lab.input.repository.as_ref().unwrap().start_commit);
    assert!(!process_obligation(&registry).unwrap());
    assert_eq!(lab.task().await,before);assert_eq!(std::fs::read(&lab.command_path).unwrap(),command_bytes);
    lab.close().await;
}


#[tokio::test]
async fn cleanup_controller_disposition_tracks_interrupted_removal_and_recovery() {
    //= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
    //# A cleanup outcome MUST distinguish reclaimed, retained and uncertain resources.
    // Scope: Controlled persisted removal intent and exact filesystem resume discriminate serialized Retained, Uncertain and Reclaimed outcomes.
    //= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
    //# Cleanup MUST preserve durable task results, histories, checkpoints, audit metadata, operation receipts and recovery journals in the initial profile.
    // Actual controller/ACK/Git authority, with one explicit crash cut after the
    // production proof is saved and removal intent is persisted. This controls
    // serialized recovery state; it is not a physical power-loss claim.
    const GRACE: u64 = 1_000;
    let lab=Lab::with_retention("disposition-recovery",GRACE,0,0).await;
    let (cp,mut phase)=lab.prepare_checkpoint().await;lab.acknowledge(&cp,&mut phase).await;
    lab.report("cleanup-disposition-stop",Observation::ProcessTreeStopped).await;
    lab.report("cleanup-disposition-result",Observation::ResultCandidate{checks_passed:true,
        summary:"Controlled disposition source".into(),checkpoint:Some(cp.clone()),
        validation:vec!["Controlled check passed".into()],output:None}).await;
    let before=lab.task().await;assert_eq!(before.status,TaskStatus::Succeeded);
    let original_operation=lab.call(Operation::GetOperation{operation_id:cp.operation_id.clone()}).await;
    let original_command=std::fs::read(&lab.command_path).unwrap();
    let original_phase=std::fs::read(publication::path(&lab.state,&lab.attempt)).unwrap();
    let policy=lab.config.limits.retention.as_ref().unwrap().worker.clone();
    let _scheduler=Scheduler::open(&lab.config,&lab.state).unwrap();
    let safe=process::now_ms();
    let due=Box::pin(attempt_at(&lab.config,&lab.state,&lab.client,&BTreeMap::new(),
        &lab.control.validation_runtime,&policy,&lab.config.limits.revision,&lab.command_path,safe)).await.unwrap();
    assert_eq!(due,Some(safe+GRACE));
    let mut intent=lab.record();assert_eq!(intent.disposition,Disposition::Retained);assert_eq!(intent.reason,Reason::Grace);
    assert!(intent.worktrees[0].proof.is_some());assert!(!intent.worktrees[0].started);assert!(!intent.worktrees[0].done);
    let proof=serde_json::to_value(&intent.worktrees[0].proof).unwrap();
    // The exact legitimate production proof stays bound while modeling a crash
    // after its durable removal intent but before the filesystem call completes.
    intent.worktrees[0].started=true;intent.disposition=Disposition::Removing;
    intent.last_observed_ms=safe+GRACE;
    save(&record_path(&lab.state,&lab.attempt.id),&intent).unwrap();
    assert_eq!(lab.record().disposition,Disposition::Removing);
    let original_path=lab.root.join("retained-original-worktree");
    std::fs::rename(&lab.workspace,&original_path).unwrap();
    let outside=lab.root.join("outside-owned-worktree");std::fs::create_dir(&outside).unwrap();
    std::fs::write(outside.join("sentinel"),"must survive
").unwrap();
    std::os::unix::fs::symlink(&outside,&lab.workspace).unwrap();
    Box::pin(attempt_at(&lab.config,&lab.state,&lab.client,&BTreeMap::new(),
        &lab.control.validation_runtime,&policy,&lab.config.limits.revision,&lab.command_path,safe+GRACE)).await.unwrap();
    let held=lab.record();assert_eq!(held.disposition,Disposition::Uncertain);assert_eq!(held.reason,Reason::Filesystem);
    let held_line=result_line(&held,None);
    assert!(held_line.starts_with(&format!("BORG_CLEANUP Uncertain Filesystem attempt={}",lab.attempt.id)));
    assert!(held_line.contains("repository-cache=cleanup-incomplete"));assert_deletion_exclusions(&held_line);
    assert!(held.worktrees[0].started);assert!(!held.worktrees[0].done);
    assert_eq!(serde_json::to_value(&held.worktrees[0].proof).unwrap(),proof);
    assert_eq!(std::fs::read(outside.join("sentinel")).unwrap(),b"must survive
");
    assert_eq!(std::fs::read(original_path.join("source.txt")).unwrap(),b"durable source
");
    assert_eq!(lab.task().await,before);
    assert!(std::fs::symlink_metadata(&lab.workspace).unwrap().file_type().is_symlink());
    std::fs::remove_file(&lab.workspace).unwrap();std::fs::rename(&original_path,&lab.workspace).unwrap();
    let resumed_now=(safe+GRACE+1).max(process::now_ms());
    let due=Box::pin(attempt_at(&lab.config,&lab.state,&lab.client,&BTreeMap::new(),
        &lab.control.validation_runtime,&policy,&lab.config.limits.revision,&lab.command_path,resumed_now)).await.unwrap();
    assert_eq!(due,None);let finished=lab.record();
    assert_eq!(finished.disposition,Disposition::Reclaimed);assert_eq!(finished.reason,Reason::Complete);
    let finished_line=result_line(&finished,None);
    assert!(finished_line.starts_with(&format!("BORG_CLEANUP Reclaimed Complete attempt={}",lab.attempt.id)));
    assert!(finished_line.contains("repository-cache=cleanup-complete"));assert_deletion_exclusions(&finished_line);
    assert!(finished.worktrees.iter().all(|slot|slot.done));assert!(finished.cache_done);
    assert!(!lab.workspace.exists());assert_eq!(std::fs::read(outside.join("sentinel")).unwrap(),b"must survive
");
    assert_eq!(lab.task().await,before);assert_eq!(std::fs::read(&lab.command_path).unwrap(),original_command);
    assert_eq!(std::fs::read(publication::path(&lab.state,&lab.attempt)).unwrap(),original_phase);
    let after=lab.call(Operation::GetOperation{operation_id:cp.operation_id.clone()}).await;
    assert_eq!(after.outcome,original_operation.outcome);assert_eq!(after.commit,original_operation.commit);
    assert_eq!(git_fixture(&lab.root.join("central.git"),&["rev-parse",&cp.remote_ref]),cp.commit);
    lab.close().await;
}

async fn cache_success(lab:&Lab)->Checkpoint {
    let (checkpoint,mut phase)=lab.prepare_checkpoint().await;
    lab.acknowledge(&checkpoint,&mut phase).await;
    lab.report("cache-acceptance-stopped",Observation::ProcessTreeStopped).await;
    lab.report("cache-acceptance-result",Observation::ResultCandidate{checks_passed:true,
        summary:"Controlled successful cache owner".into(),checkpoint:Some(checkpoint.clone()),
        validation:vec!["Controlled source check".into()],output:None}).await;
    assert_eq!(lab.task().await.status,TaskStatus::Succeeded);
    checkpoint
}
fn cache_directory(lab:&Lab)->PathBuf {
    lab.config.workspace_root.join("repositories").join(git::cleanup::key(lab.input.repository.as_ref().unwrap(),&lab.control).unwrap())
}
async fn cache_observe(lab:&Lab,now:u64)->Option<u64> {
    let policy=&lab.config.limits.retention.as_ref().unwrap().worker;
    Box::pin(attempt_at(&lab.config,&lab.state,&lab.client,&BTreeMap::new(),&lab.control.validation_runtime,
        policy,&lab.config.limits.revision,&lab.command_path,now)).await.unwrap()
}
// Fixture accounting is logical regular-file bytes, not allocated blocks,
// deduplicated object sizes, a production quota or an installed disk metric.
fn cache_fixture_files(root:&Path)->BTreeMap<PathBuf,Vec<u8>> {
    let mut files=BTreeMap::new();let mut pending=vec![root.to_owned()];
    while let Some(path)=pending.pop() {
        let metadata=std::fs::symlink_metadata(&path).unwrap();
        assert!(!metadata.file_type().is_symlink(),"private fixture inventory must not follow links");
        if metadata.is_dir() {
            for entry in std::fs::read_dir(&path).unwrap() {pending.push(entry.unwrap().path());}
        }else{assert!(metadata.is_file());files.insert(path.strip_prefix(root).unwrap().to_owned(),std::fs::read(path).unwrap());}
        assert!(files.len()+pending.len()<4096,"bounded private fixture inventory");
    }
    files
}
fn cache_fixture_bytes(files:&BTreeMap<PathBuf,Vec<u8>>)->u64 {files.values().map(|bytes|bytes.len() as u64).sum()}

#[tokio::test]
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//= type=test
//# Automatic cleanup MUST use per-kind retention settings from worker metadata.
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//= type=test
//# Cleanup MUST preserve durable task results, histories, checkpoints, audit metadata, operation receipts and recovery journals in the initial profile.
async fn cleanup_shared_cache_has_exact_seven_day_boundary_and_retained_byte_accounting() {
    let lab=Lab::open("cache-seven-days").await;
    let checkpoint=cache_success(&lab).await;
    let cache=cache_directory(&lab);
    let validation=Box::pin(git::checkout_cached(&lab.config.workspace_root.join("repositories"),
        &lab.config.workspace_root.join("validation"),&lab.attempt.id,lab.input.repository.as_ref().unwrap(),Some(&checkpoint),
        &lab.state.join("processes").join(component(&lab.attempt.id)),&lab.control)).await.unwrap();
    std::fs::write(cache.join("repository.git/info/exclude"),"target/\n").unwrap();
    for path in [&lab.workspace,&validation] {
        std::fs::create_dir(path.join("target")).unwrap();std::fs::write(path.join("target/build-output.bin"),vec![7;4096]).unwrap();
    }
    let disposable=cache_fixture_bytes(&cache_fixture_files(&lab.workspace))+cache_fixture_bytes(&cache_fixture_files(&validation));
    assert!(disposable>=8192);
    let canonical=lab.task().await;
    let result=lab.call(Operation::GetOperation{operation_id:"cache-acceptance-result".into()}).await;
    let publication=lab.call(Operation::GetOperation{operation_id:checkpoint.operation_id.clone()}).await;
    let history=Operation::QueryCollaboration{query:crate::collaboration::Query::TaskHistory{
        task_id:lab.attempt.task_id.clone(),after:None,limit:100}};
    let history_before=lab.call(history.clone()).await.outcome;
    let receipts=cache_fixture_files(&lab.state.join("commands"));
    let journals=cache_fixture_files(&lab.state.join("publications"));
    let central=cache_fixture_files(&lab.root.join("central.git"));
    let _scheduler=Scheduler::open(&lab.config,&lab.state).unwrap();
    let safe=process::now_ms();let idle=lab.config.limits.retention.as_ref().unwrap().worker.idle_cache_grace_ms;
    assert_eq!(idle,604_800_000,"assert the existing selected policy, never a source fallback");
    let due=cache_observe(&lab,safe).await.unwrap();assert_eq!(due,safe+idle);
    assert!(!lab.workspace.exists()&&!validation.exists());
    assert_eq!(lab.record().disposition,Disposition::Retained);assert_eq!(lab.record().reason,Reason::CacheGrace);
    let retained_cache=cache_fixture_files(&cache);let retained_bytes=cache_fixture_bytes(&retained_cache);assert!(retained_bytes>0);
    assert_eq!(cache_observe(&lab,due-1).await,Some(due));assert_eq!(cache_fixture_files(&cache),retained_cache);
    // Reopening scheduling state does not reset the persisted first-safe time.
    drop(_scheduler);let _restarted=Scheduler::open(&lab.config,&lab.state).unwrap();
    assert_eq!(lab.record().cache_safe_ms,Some(safe));assert_eq!(cache_observe(&lab,due).await,None);
    assert!(!cache.exists());assert!(lab.record().cache_done);assert_eq!(lab.record().disposition,Disposition::Reclaimed);
    assert_eq!(lab.record().policy_revision,lab.config.limits.revision);
    assert_eq!(lab.task().await,canonical);assert_eq!(lab.call(history).await.outcome,history_before);
    let result_after=lab.call(Operation::GetOperation{operation_id:"cache-acceptance-result".into()}).await;
    assert_eq!(result_after.outcome,result.outcome);assert_eq!(result_after.commit,result.commit);
    let publication_after=lab.call(Operation::GetOperation{operation_id:checkpoint.operation_id.clone()}).await;
    assert_eq!(publication_after.outcome,publication.outcome);assert_eq!(publication_after.commit,publication.commit);
    assert_eq!(cache_fixture_files(&lab.state.join("commands")),receipts);
    assert_eq!(cache_fixture_files(&lab.state.join("publications")),journals);
    assert_eq!(cache_fixture_files(&lab.root.join("central.git")),central);
    assert_eq!(git_fixture(&lab.root.join("central.git"),&["rev-parse",&checkpoint.remote_ref]),checkpoint.commit);
    std::fs::write(lab.root.join("cache-byte-accounting.json"),serde_json::to_vec_pretty(&json!({
        "meaning":"logical regular-file bytes in private fixture; not allocated disk or quota",
        "workspace_and_validation_before":disposable,"workspace_and_validation_after":0,
        "cache_before_expiry":retained_bytes,"cache_after_expiry":0,
        "retained_command_bytes":cache_fixture_bytes(&receipts),"retained_publication_bytes":cache_fixture_bytes(&journals),
        "retained_external_checkpoint_bytes":cache_fixture_bytes(&central),"policy_revision":lab.config.limits.revision,
        "first_safe_ms":safe,"expiry_ms":due})).unwrap()).unwrap();
    lab.close().await;
}

#[tokio::test]
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//= type=test
//# Cache cleanup MUST preserve a cache while a worktree, accepted task or unresolved recovery record depends on that local cache.
async fn cleanup_intervening_registered_or_unknown_dependency_restarts_cache_inactivity() {
    for kind in ["registered-worktree","unknown-journal","owned-process","unknown-outbox"] {
        let lab=Lab::open(kind).await;cache_success(&lab).await;
        let _scheduler=Scheduler::open(&lab.config,&lab.state).unwrap();
        let first=process::now_ms();let original_due=cache_observe(&lab,first).await.unwrap();let cache=cache_directory(&lab);
        let marker=std::fs::read(cache.join("last-use.json")).unwrap();
        let foreign=lab.root.join("independent-registered-worktree");
        let unknown=match kind {
            "owned-process"=>lab.state.join("processes").join(component(&lab.attempt.id)).join("unknown-process.json"),
            "unknown-outbox"=>lab.state.join("outbox/unknown-recovery.json"),
            _=>lab.state.join("commands/unknown-recovery.json"),
        };
        let command_before=std::fs::read(&lab.command_path).unwrap();
        if kind=="registered-worktree" {
            // A real Git registration outside the worker command inventory still pins the cache.
            git_fixture(&cache.join("repository.git"),&["worktree","add","--detach",foreign.to_str().unwrap(),&lab.input.repository.as_ref().unwrap().start_commit]);
        }else{std::fs::write(&unknown,b"{unresolved").unwrap();}
        let receipt=read_receipt(&lab.command_path,&lab.config.limits).unwrap();
        let readiness=local_ready(&lab.config,&lab.state,&receipt,&BTreeMap::new());
        if kind=="owned-process" {assert!(!readiness.unwrap(),"owned process entry is a positive local obligation");}
        else if kind=="unknown-outbox" {assert!(readiness.is_err(),"malformed recovery proof is unknown, not absence");}
        else {assert!(readiness.unwrap(),"shared dependency must reach the cache-specific guard");}
        let held=original_due-1;
        assert_eq!(cache_observe(&lab,held).await,Some(held+lab.config.limits.retention.as_ref().unwrap().worker.retry_delay_ms));
        let reason=if kind=="owned-process"||kind=="unknown-outbox" {Reason::LocalRecovery}else{Reason::CacheDependency};
        assert!(cache.exists());assert_eq!(lab.record().reason,reason);
        assert_eq!(lab.record().disposition,Disposition::Retained);assert_eq!(lab.record().cache_safe_ms,None);
        assert_eq!(std::fs::read(cache.join("last-use.json")).unwrap(),marker);
        assert_eq!(std::fs::read(&lab.command_path).unwrap(),command_before);
        if kind=="registered-worktree" {
            assert_eq!(std::fs::read(foreign.join("source.txt")).unwrap(),b"durable source\n");
            // Only the fixture owner resolves its own reference; cleanup never removes it.
            git_fixture(&cache.join("repository.git"),&["worktree","remove",foreign.to_str().unwrap()]);
        }else{assert_eq!(std::fs::read(&unknown).unwrap(),b"{unresolved");std::fs::remove_file(&unknown).unwrap();}
        drop(_scheduler);let _restarted=Scheduler::open(&lab.config,&lab.state).unwrap();
        assert_eq!(lab.record().cache_safe_ms,None,"interrupted proof survives scheduler restart");
        let new_due=cache_observe(&lab,original_due).await.unwrap();
        assert_eq!(new_due,original_due+lab.config.limits.retention.as_ref().unwrap().worker.idle_cache_grace_ms);
        assert_eq!(lab.record().cache_safe_ms,Some(original_due));assert!(cache.exists());
        assert_eq!(cache_observe(&lab,new_due-1).await,Some(new_due));assert!(cache.exists());
        assert_eq!(cache_observe(&lab,new_due).await,None);assert!(!cache.exists());
        lab.close().await;
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//= type=test
//# Cache cleanup MUST preserve a cache while a worktree, accepted task or unresolved recovery record depends on that local cache.
async fn cleanup_reuse_between_observations_invalidates_original_cache_proof() {
    let lab=Lab::open("cache-reuse").await;cache_success(&lab).await;
    let _scheduler=Scheduler::open(&lab.config,&lab.state).unwrap();
    let first=process::now_ms();let due=cache_observe(&lab,first).await.unwrap();let original=lab.record().cache.unwrap();
    let repository=lab.input.repository.as_ref().unwrap();let root=lab.config.workspace_root.join("repositories");
    let workspace_root=lab.config.workspace_root.join("workspaces");let registry=lab.state.join("processes/cache-reuse-fixture");
    let other=Box::pin(git::checkout_cached(&root,&workspace_root,"later-cache-user",repository,None,&registry,&lab.control)).await.unwrap();
    assert!(other.join("source.txt").is_file());
    let proof=Box::pin(git::cleanup::prepare_worktree(&root,&workspace_root,"later-cache-user",repository,&repository.start_commit,false,&registry,&lab.control)).await.unwrap().unwrap();
    Box::pin(git::cleanup::remove_worktree(&proof,&root,&workspace_root,"later-cache-user",repository,&registry,&lab.control,false)).await.unwrap();
    // No cleanup observation happened while this second user existed. The locked
    // use revision alone must invalidate the earlier cache proof and safe time.
    let next_due=cache_observe(&lab,due).await.unwrap();
    assert_eq!(next_due,due+lab.config.limits.retention.as_ref().unwrap().worker.idle_cache_grace_ms);
    assert!(lab.record().cache.as_ref().unwrap()!=&original);assert_eq!(lab.record().cache_safe_ms,Some(due));
    assert!(cache_directory(&lab).exists());assert_eq!(cache_observe(&lab,next_due).await,None);
    lab.close().await;
}

#[tokio::test]
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//= type=test
//# Restart MUST reconcile incomplete cleanup before treating its resources as reclaimed.
async fn cleanup_restart_before_cache_barrier_revalidates_intent_without_claiming_reclaimed() {
    let lab=Lab::open("cache-intent-before-barrier").await;cache_success(&lab).await;
    let scheduler=Scheduler::open(&lab.config,&lab.state).unwrap();
    let first=process::now_ms();let due=cache_observe(&lab,first).await.unwrap();
    // Exact persisted cut between saving Removing and installing the reuse
    // barrier in remove_cache. No deletion or checkpoint acknowledgment is fabricated.
    let mut cut=lab.record();cut.cache_started=true;cut.disposition=Disposition::Removing;
    save(&record_path(&lab.state,&lab.attempt.id),&cut).unwrap();drop(scheduler);
    let cache=cache_directory(&lab);let before=cache_fixture_files(&cache);
    let restarted=Scheduler::open(&lab.config,&lab.state).unwrap();
    assert!(restarted.deadline().is_some());
    let next=cache_observe(&lab,due).await.unwrap();
    assert_eq!(next,due+lab.config.limits.retention.as_ref().unwrap().worker.idle_cache_grace_ms);
    assert!(!lab.record().cache_started&&!lab.record().cache_done);assert_eq!(lab.record().disposition,Disposition::Retained);
    assert_eq!(cache_fixture_files(&cache),before);
    assert_eq!(cache_observe(&lab,next).await,None);assert_eq!(lab.record().disposition,Disposition::Reclaimed);
    assert!(!cache.exists());lab.close().await;
}

#[tokio::test]
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//= type=test
//# Cache cleanup MUST preserve a cache while a worktree, accepted task or unresolved recovery record depends on that local cache.
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//= type=test
//# Cleanup MUST preserve data required by an active attempt.
async fn cleanup_one_shared_cache_survives_active_and_retained_failed_peer() {
    let lab=Lab::with_grace("cache-peer",86_400_000).await;
    let checkpoint=cache_success(&lab).await;
    let _scheduler=Scheduler::open(&lab.config,&lab.state).unwrap();
    let first=process::now_ms();let original_due=cache_observe(&lab,first).await.unwrap();let cache=cache_directory(&lab);
    let mut input=lab.input.clone();input.resolved_limits=None;input.definition.id="cleanup-shared-peer".into();
    let repo=input.repository.as_mut().unwrap();repo.url.clear();repo.checkpoint_remote.clear();
    let submitted=task_value(lab.call(Operation::SubmitTask{operation_id:"cleanup-peer-submit".into(),input}).await);
    let WorkerCommand::RunAttempt{command_id:first_command,..}=&lab.command else{unreachable!()};
    let reply=lab.call(Operation::AwaitCommands{operation_id:"cleanup-peer-command".into(),worker_id:lab.config.id.clone(),
        incarnation:lab.config.incarnation.clone(),acknowledged:vec![first_command.clone()]}).await;
    let Outcome::Ok{data:Data::Commands(commands)}=reply.outcome else{panic!("second command expected")};
    let command=commands.into_iter().find(|command|matches!(command,WorkerCommand::RunAttempt{attempt,..}if attempt.task_id==submitted.id)).unwrap();
    let WorkerCommand::RunAttempt{command_id,task:input,attempt,..}=&command else{unreachable!()};
    let path=lab.state.join("commands").join(format!("{}.json",component(command_id)));
    // A local completed bit is deliberately insufficient: canonical active work
    // still pins the cache even before its working directory exists.
    atomic_json(&path,&Receipt{command:command.clone(),completed:true,effects_started:true}).unwrap();
    let report=|id:&str,observation|Operation::ObserveWorker{report:Report{report_id:id.into(),
        worker_id:attempt.worker_id.clone(),incarnation:attempt.worker_incarnation.clone(),task_id:attempt.task_id.clone(),
        attempt_id:attempt.id.clone(),generation:attempt.generation,observation}};
    assert!(matches!(lab.call(report("cleanup-peer-started",Observation::Started{thread_id:Some("controlled-peer".into()),turn_id:Some("controlled-peer-turn".into())})).await.outcome,Outcome::Ok{..}));
    let held=original_due-1;cache_observe(&lab,held).await;
    assert!(cache.exists());assert_eq!(lab.record().reason,Reason::CacheDependency);assert_eq!(lab.record().cache_safe_ms,None);
    let peer=Box::pin(git::checkout_cached(&lab.config.workspace_root.join("repositories"),&lab.config.workspace_root.join("workspaces"),
        &attempt.id,input.repository.as_ref().unwrap(),None,&lab.state.join("processes").join(component(&attempt.id)),&lab.control)).await.unwrap();
    assert_eq!(git::cleanup::key(input.repository.as_ref().unwrap(),&lab.control).unwrap(),git::cleanup::key(lab.input.repository.as_ref().unwrap(),&lab.control).unwrap());
    assert_ne!(peer,lab.workspace);std::fs::write(peer.join("source.txt"),"peer uncheckpointed edit\n").unwrap();
    std::fs::write(peer.join("new-source.txt"),"peer new source\n").unwrap();
    for (id,observation) in [
        ("cleanup-peer-executor",Observation::ExecutorOutcome{success:false,summary:"Controlled peer failure".into()}),
        ("cleanup-peer-stopped",Observation::ProcessTreeStopped),
        ("cleanup-peer-result",Observation::ResultCandidate{checks_passed:false,summary:"Controlled failed peer".into(),checkpoint:None,validation:vec![],output:None}),
    ] {assert!(matches!(lab.call(report(id,observation)).await.outcome,Outcome::Ok{..}));}
    let failed=task_value(lab.call(Operation::GetTask{task_id:attempt.task_id.clone()}).await);
    assert_eq!(failed.status,TaskStatus::Failed);assert!(failed.attempt.as_ref().unwrap().stopped);
    let receipt_bytes=std::fs::read(&path).unwrap();
    let policy=&lab.config.limits.retention.as_ref().unwrap().worker;
    let failed_safe=held+1;let expired=failed_safe+policy.failed_grace_ms;
    for now in [failed_safe,expired-1] {
        let due=Box::pin(attempt_at(&lab.config,&lab.state,&lab.client,&BTreeMap::new(),&lab.control.validation_runtime,
            policy,&lab.config.limits.revision,&path,now)).await.unwrap();
        assert_eq!(due,Some(expired));
        cache_observe(&lab,now).await;assert_eq!(lab.record().reason,Reason::CacheDependency);
        assert_eq!(std::fs::read(peer.join("source.txt")).unwrap(),b"peer uncheckpointed edit\n");
        assert_eq!(std::fs::read(peer.join("new-source.txt")).unwrap(),b"peer new source\n");assert!(cache.exists());
    }
    let peer_cache_due=Box::pin(attempt_at(&lab.config,&lab.state,&lab.client,&BTreeMap::new(),&lab.control.validation_runtime,
        policy,&lab.config.limits.revision,&path,expired)).await.unwrap().unwrap();
    assert!(!peer.exists());assert!(cache.exists());
    assert_eq!(cache_observe(&lab,expired).await,Some(peer_cache_due));
    assert_eq!(peer_cache_due,expired+policy.idle_cache_grace_ms);
    assert_eq!(cache_observe(&lab,peer_cache_due).await,None);assert!(!cache.exists());
    assert_eq!(std::fs::read(&path).unwrap(),receipt_bytes);
    assert_eq!(task_value(lab.call(Operation::GetTask{task_id:attempt.task_id.clone()}).await),failed);
    assert_eq!(git_fixture(&lab.root.join("central.git"),&["rev-parse",&checkpoint.remote_ref]),checkpoint.commit);
    lab.close().await;
}

#[tokio::test]
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST present accepted task results.
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST link the task's published source checkpoints.
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//= type=test
//# Cleanup MUST preserve durable task results, histories, checkpoints, audit metadata, operation receipts and recovery journals in the initial profile.
async fn cleanup_disposable_cache_does_not_change_browser_result_checkpoint_or_operation_lookup() {
    let lab=Lab::open("cache-product").await;let checkpoint=cache_success(&lab).await;
    let canonical=lab.task().await;let original=lab.call(Operation::GetOperation{operation_id:"cache-acceptance-result".into()}).await;
    let routes=[format!("/tasks/{}",lab.attempt.task_id),format!("/tasks/{}/result",lab.attempt.task_id),
        format!("/tasks/{}/checkpoint",lab.attempt.task_id),"/operations/cache-acceptance-result".into()];
    let _scheduler=Scheduler::open(&lab.config,&lab.state).unwrap();let first=process::now_ms();
    let due=cache_observe(&lab,first).await.unwrap();assert!(cache_directory(&lab).exists());
    for after_expiry in [false,true] {
        if after_expiry {assert_eq!(cache_observe(&lab,due).await,None);assert!(!cache_directory(&lab).exists());}
        for (index,route) in routes.iter().enumerate() {
            let response=Box::pin(crate::product::handle(&lab.controller,crate::product::UiRequest{
                method:"GET".into(),path:route.clone(),body:vec![],origin:None,expected_origin:lab.config.controller_url.clone()})).await;
            assert_eq!(response.status,200,"{route} after_expiry={after_expiry}");
            if index==0 {assert!(response.body.contains("<dt>Status</dt><dd>succeeded</dd>"));}
            if index==1 || index==3 {assert!(response.body.contains("Controlled successful cache owner"));}
            if index==1 || index==2 || index==3 {
                assert!(response.body.contains(&checkpoint.commit));assert!(response.body.contains(&checkpoint.operation_id));
            }
        }
        assert_eq!(lab.task().await,canonical);
        let receipt=lab.call(Operation::GetOperation{operation_id:"cache-acceptance-result".into()}).await;
        assert_eq!(receipt.outcome,original.outcome);assert_eq!(receipt.commit,original.commit);
    }
    lab.close().await;
}

#[tokio::test]
async fn cleanup_controller_unresolved_handoff_alone_holds_eligible_material_and_receipts() {
    //= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
    //# Cleanup MUST preserve data required to reconcile an uncertain or unacknowledged operation.
    // Scope: a Coding command cannot own a native delegation. Its exact local
    // handoff path is therefore an unsupported recovery obligation, regardless
    // of journal contents. This case never invents a native grant or settles it.
    for with_handoff in [false, true] {
        let lab = Lab::with_retention(
            if with_handoff { "handoff-held" } else { "handoff-absent-control" },
            0, 0, 0,
        ).await;
        let checkpoint = cache_success(&lab).await;
        let validation = Box::pin(git::checkout_cached(
            &lab.config.workspace_root.join("repositories"),
            &lab.config.workspace_root.join("validation"),
            &lab.attempt.id, lab.input.repository.as_ref().unwrap(), Some(&checkpoint),
            &lab.state.join("processes").join(component(&lab.attempt.id)), &lab.control,
        )).await.unwrap();
        let cache = cache_directory(&lab);
        std::fs::write(cache.join("repository.git/info/exclude"), "target/\n").unwrap();
        for path in [&lab.workspace, &validation] {
            std::fs::create_dir(path.join("target")).unwrap();
            std::fs::write(path.join("target/output.bin"), b"disposable build output\n").unwrap();
        }
        let before = lab.task().await;
        assert_eq!(before.status, TaskStatus::Succeeded);
        assert!(before.attempt.as_ref().unwrap().stopped);
        assert!(!before.attempt.as_ref().unwrap().uncertain);
        let receipt = read_receipt(&lab.command_path, &lab.config.limits).unwrap();
        assert!(receipt.completed && receipt.effects_started);
        assert!(local_ready(&lab.config, &lab.state, &receipt, &BTreeMap::new()).unwrap());
        let phase_path = publication::path(&lab.state, &lab.attempt);
        let phase = publication::read(&phase_path, &lab.command).unwrap().unwrap();
        let settlement = || Operation::SettleOperation {
            settlement_id: phase.settlement_id.clone(), operation_id: checkpoint.operation_id.clone(),
            task_id: lab.attempt.task_id.clone(), attempt_id: lab.attempt.id.clone(),
            generation: lab.attempt.generation, outcome: json!({"acknowledged": true}),
            checkpoint: Some(checkpoint.clone()),
        };
        let original_settlement = lab.call(settlement()).await;
        assert!(matches!(original_settlement.outcome, Outcome::Ok { .. }));
        let publication_lookup = || Operation::GetOperation { operation_id: checkpoint.operation_id.clone() };
        let result_lookup = || Operation::GetOperation { operation_id: "cache-acceptance-result".into() };
        let history_query = || Operation::QueryCollaboration { query: crate::collaboration::Query::TaskHistory {
            task_id: lab.attempt.task_id.clone(), after: None, limit: 100,
        }};
        let original_publication = lab.call(publication_lookup()).await;
        let original_result = lab.call(result_lookup()).await;
        let original_history = lab.call(history_query()).await;
        let command_bytes = std::fs::read(&lab.command_path).unwrap();
        let phase_bytes = std::fs::read(&phase_path).unwrap();
        let workspace_bytes = cache_fixture_files(&lab.workspace);
        let validation_bytes = cache_fixture_files(&validation);
        let cache_bytes = cache_fixture_files(&cache);
        let journal_path = handoff::path(&lab.state, &lab.attempt);
        assert!(!journal_path.exists());
        let journal = b"{\"unsupported_coding_handoff\":\"unresolved original local bytes\"}\n";
        assert!(journal.len() < lab.config.limits.worker.command_record_bytes);
        let mut scheduler = Scheduler::open(&lab.config, &lab.state).unwrap();
        if with_handoff {
            process::private_directory(journal_path.parent().unwrap()).unwrap();
            std::fs::write(&journal_path, journal).unwrap();
            assert!(!local_ready(&lab.config, &lab.state, &receipt, &BTreeMap::new()).unwrap());
            for reopened in [false, true] {
                if reopened {
                    drop(scheduler);
                    scheduler = Scheduler::open(&lab.config, &lab.state).unwrap();
                }
                let now = process::now_ms();
                assert_eq!(cache_observe(&lab, now).await,
                    Some(now + lab.config.limits.retention.as_ref().unwrap().worker.retry_delay_ms));
                let held = lab.record();
                assert_eq!(held.disposition, Disposition::Retained);
                assert_eq!(held.reason, Reason::LocalRecovery);
                assert_eq!(held.first_safe_ms, None);
                assert_eq!(held.cache_safe_ms, None);
                assert_eq!(held.policy_revision, lab.config.limits.revision);
                assert!(held.worktrees.iter().all(|slot| !slot.started && !slot.done));
                assert!(!held.cache_started && !held.cache_done);
                assert_eq!(std::fs::read(&journal_path).unwrap(), journal);
                assert_eq!(cache_fixture_files(&lab.workspace), workspace_bytes);
                assert_eq!(cache_fixture_files(&validation), validation_bytes);
                assert_eq!(cache_fixture_files(&cache), cache_bytes);
                assert_eq!(std::fs::read(&lab.command_path).unwrap(), command_bytes);
                assert_eq!(std::fs::read(&phase_path).unwrap(), phase_bytes);
                assert_eq!(lab.task().await, before);
                let observed = lab.call(publication_lookup()).await;
                assert_eq!(observed.outcome, original_publication.outcome);
                assert_eq!(observed.commit, original_publication.commit);
                let observed = lab.call(result_lookup()).await;
                assert_eq!(observed.outcome, original_result.outcome);
                assert_eq!(observed.commit, original_result.commit);
                assert_eq!(lab.call(history_query()).await.outcome, original_history.outcome);
            }
            // Only the fixture owner resolves its injected unknown local file.
            // Cleanup must neither delete it nor grant a delegation operation.
            std::fs::remove_file(&journal_path).unwrap();
            assert!(local_ready(&lab.config, &lab.state, &receipt, &BTreeMap::new()).unwrap());
        }
        assert_eq!(cache_observe(&lab, process::now_ms()).await, None);
        let reclaimed = lab.record();
        assert_eq!(reclaimed.disposition, Disposition::Reclaimed);
        assert_eq!(reclaimed.reason, Reason::Complete);
        assert_eq!(reclaimed.policy_revision, lab.config.limits.revision);
        assert!(reclaimed.worktrees.iter().all(|slot| slot.done));
        assert!(reclaimed.cache_done);
        assert!(!lab.workspace.exists() && !validation.exists() && !cache.exists());
        assert_eq!(lab.task().await, before);
        assert_eq!(std::fs::read(&lab.command_path).unwrap(), command_bytes);
        assert_eq!(std::fs::read(&phase_path).unwrap(), phase_bytes);
        let replay = lab.call(settlement()).await;
        assert_eq!(replay.outcome, original_settlement.outcome);
        assert_eq!(replay.commit, original_settlement.commit);
        let observed = lab.call(publication_lookup()).await;
        assert_eq!(observed.outcome, original_publication.outcome);
        assert_eq!(observed.commit, original_publication.commit);
        let observed = lab.call(result_lookup()).await;
        assert_eq!(observed.outcome, original_result.outcome);
        assert_eq!(observed.commit, original_result.commit);
        assert_eq!(lab.call(history_query()).await.outcome, original_history.outcome);
        assert_eq!(git_fixture(&lab.root.join("central.git"), &["rev-parse", &checkpoint.remote_ref]), checkpoint.commit);
        drop(scheduler);
        lab.close().await;
    }
}

#[path = "cleanup_plugin_tests.rs"]
mod plugins;
