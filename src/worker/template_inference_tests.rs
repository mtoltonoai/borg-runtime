use super::*;
fn record_launch(path: &Path, command: &WorkerCommand) -> Result<bool, String> {
    crate::worker::record_launch(path, command, &test_limits())
}
fn read_receipt(path: &Path) -> Result<Receipt, String> {
    crate::worker::read_receipt(path, &test_limits())
}
fn command_receipts(path: &Path) -> Result<Vec<(PathBuf, Receipt)>, String> {
    crate::worker::command_receipts(path, &test_limits())
}
fn track_native_launch(path: &Path, attempt: &Attempt) -> Result<(), String> {
    crate::worker::track_native_launch(path, attempt, &test_limits())
}
async fn native_registry_reconciled(path: &Path, attempt: &Attempt) -> Result<bool, String> {
    crate::worker::native_registry_reconciled(path, attempt, &test_limits()).await
}

use std::os::unix::fs::PermissionsExt;

struct Fixture {
    root: PathBuf,
    state: PathBuf,
    reporter: Reporter,
    control: Control,
    _cancel: watch::Sender<bool>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
fn fixture() -> Fixture {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let root = std::env::temp_dir().join(format!(
        "borg-template-consumer-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let state = root.join("state");
    std::fs::create_dir(&state).unwrap();
    handoff::initialize_state(&state, &test_limits().worker).unwrap();
    std::fs::create_dir(state.join("commands")).unwrap();
    let config = WorkerConfig {
        limits: test_limits(),
        resource_profile: None,
        controller_url: "http://127.0.0.1:9".into(),
        id: "worker".into(),
        incarnation: "new-incarnation".into(),
        workspace_root: root.clone(),
        codex_path: "/nonexistent-codex".into(),
        coding_capacity: 1,
        native_capacity: 1,
        native_endpoint: "http://127.0.0.1:9/v1/responses".into(),
        validation_executable: "/nonexistent-validator".into(),
    };
    let attempt = Attempt {
        session_id: "session-1".into(),
        id: "attempt".into(),
        task_id: "task".into(),
        generation: 1,
        worker_id: "worker".into(),
        worker_incarnation: "old-incarnation".into(),
        phase: AttemptPhase::Running,
        deadline_unix_ms: process::now_ms() + 60_000,
        stopped: false,
        uncertain: false,
    };
    let (_cancel, rx) = watch::channel(false);
    let control = test_control(attempt.deadline_unix_ms, rx);
    let reporter = Reporter {
        client: ApiClient::new(&config.controller_url, test_limits()).unwrap(),
        config,
        resource: None,
        attempt,
        outbox: state.join("outbox"),
        sequence: Arc::new(AtomicU64::new(1)),
        executor_seen: Arc::new(AtomicBool::new(false)),
        provider_uncertain: Arc::new(AtomicBool::new(false)),
        uncertainty_retained: Arc::new(AtomicBool::new(false)),
        output_limit: 4096,
        result_limit: 4096,
        coding_limits: test_limits().execution.coding.clone(),
        outbox_lock: Arc::new(tokio::sync::Mutex::new(())),
    };
    Fixture {
        root,
        state,
        reporter,
        control,
        _cancel,
    }
}
fn input(f: &Fixture, executor: ExecutorKind) -> TaskInput {
    let mut input:TaskInput=serde_json::from_value(json!({"instruction":"fixture","definition":{"id":"definition","revision":"revision","executor":executor,"inference":{"model":"gpt-6-astra","reasoning_effort":"max"},"execution_profile":{"kind":"collaboration_v1"}},"repository":null,"acceptance_commands":[],"output_schema":{"type":"object"},"limits":{"deadline_unix_ms":f.control.deadline_ms,"model_requests":2,"output_bytes":4096,"result_bytes":4096,"max_delegation_depth":1},"dependencies":[]})).unwrap();
    input.resolved_limits = Some(test_limits().resolve_task(&input).unwrap());
    input
}
// Traceability scope: Direct native and coding consumers refuse missing/unsupported settings before any workspace, helper, Codex state or outbox effect.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# An agent definition MUST specify the model used by its executor.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# An agent definition MUST specify the reasoning effort used by its executor.
#[tokio::test]
async fn missing_or_unsupported_template_refuses_both_workers_before_effects() {
    for mode in ["missing", "model", "effort", "profile"] {
        let f = fixture();
        let mut task = input(&f, ExecutorKind::Native);
        match mode {
            "missing" => task.definition.inference = None,
            "model" => task.definition.inference.as_mut().unwrap().model = "gpt-6.1-sol".into(),
            "profile" => task.definition.execution_profile = None,
            _ => task.definition.inference.as_mut().unwrap().reasoning_effort = "high".into(),
        }
        let expected = if mode == "missing" {
            "template_inference_unbound"
        } else if mode == "profile" {
            "template_profile_unbound"
        } else {
            "template_inference_unsupported"
        };
        let lock = Arc::new(std::fs::File::create(f.root.join("test-owner-lock")).unwrap());
        let error = native(
            &f.reporter.config,
            &task,
            &f.control,
            &f.reporter,
            None,
            None,
            &f.state,
            lock,
        )
        .await
        .err()
        .unwrap();
        assert_eq!(error, expected);
        task.definition.executor = ExecutorKind::Coding;
        let command = WorkerCommand::RunAttempt {
            command_id: "command".into(),
            session: crate::protocol::Session {
                id: f.reporter.attempt.session_id.clone(),
                agent_id: task.definition.id.clone(),
                definition: task.definition.clone(),
                task_id: f.reporter.attempt.task_id.clone(),
            },
            attempt: f.reporter.attempt.clone(),
            task: task.clone(),
            checkpoint: None,
            continuation: None,
                workflow_continuation: None, completion_continuation: None,
        };
        let error = coding(
            &f.reporter.config,
            &f.reporter.attempt,
            &task,
            None,
            &f.state,
            &f.control,
            &f.reporter,
            &command,
        )
        .await
        .err()
        .unwrap();
        assert_eq!(error.message(), expected);
        assert!(!f.state.join("native-tracking").exists());
        assert!(!f.root.join("workspaces").exists());
        assert!(!f.state.join("codex").exists());
        assert!(!f.state.join("outbox").exists());
    }
}
#[test]
fn typed_bridge_uncertainty_survives_plain_diagnostic() {
    let failure = AttemptFailure::from(codex::ExecutionFailure {
        message: "bridge stopped".into(),
        collaboration_uncertain: true,
        execution_uncertain: false,
        completion_uncertain: false,
        completion_held: false,
    });
    assert!(failure.execution_uncertain(false));
    assert!(!receipt_completed(&Err(failure), true));
    let known = AttemptFailure::from(codex::ExecutionFailure {
        message: "known refusal".into(),
        collaboration_uncertain: false,
        execution_uncertain: false,
        completion_uncertain: false,
        completion_held: false,
    });
    assert!(!known.execution_uncertain(false));
}
#[tokio::test]
async fn startup_audits_completed_coding_receipt_and_exact_old_caller() {
    let f = fixture();
    let task = input(&f, ExecutorKind::Coding);
    let a = &f.reporter.attempt;
    let command = WorkerCommand::RunAttempt {
        command_id: "command".into(),
        session: crate::protocol::Session {
            id: a.session_id.clone(),
            agent_id: task.definition.id.clone(),
            definition: task.definition.clone(),
            task_id: a.task_id.clone(),
        },
        attempt: a.clone(),
        task: task.clone(),
        checkpoint: None,
        continuation: None,
                workflow_continuation: None, completion_continuation: None,
    };
    atomic_json(
        &f.state.join("commands/command.json"),
        &Receipt {
            command,
            completed: true,
            effects_started: true,
        },
    )
    .unwrap();
    audit_stopped_coding_collaboration(&f.reporter.config, &f.state, &f.reporter.client)
        .await
        .unwrap();
    let caller = collaboration_caller(a);
    let dir = codex::state_path(&f.state.join("codex"), &a.id).join("collaboration-codex");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("calls.json");
    let invocation = json!({"operation_id":"original-operation","logical_call_id":"original-call","caller":caller,"tool":"collab_memory_set","arguments":{}});
    let mut journal = json!({
        "revision": 2,
        "runtime_limits": task.resolved_limits.as_ref().unwrap().global_metadata,
        "limits": task.execution_limits().unwrap().collaboration,
        "worker_limits": f.reporter.config.limits.worker,
        "caller": caller,
        "deadline_unix_ms": a.deadline_unix_ms.min(task.limits.deadline_unix_ms),
        "controller_url": f.reporter.config.controller_url,
        "entries": [{
            "thread_id": "original-thread",
            "session_id": "original-session",
            "invocation": invocation,
            "outcome": null,
            "delivered": false
        }]
    });
    let save = |value: &serde_json::Value| {
        std::fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    };
    save(&journal);
    assert!(!coding_collaboration_closed(&f.reporter.config, &task, a, &f.state).unwrap());
    assert!(
        audit_stopped_coding_collaboration(&f.reporter.config, &f.state, &f.reporter.client)
            .await
            .is_err()
    );
    // Serialize the actual typed Reply so this fixture follows the wire contract.
    journal["entries"][0]["outcome"] = serde_json::to_value(crate::mcp::CallResult {
        reply: Reply {
            request_id: "original-transport".into(),
            outcome: Outcome::Refused {
                code: "invalid_transition".into(),
                message: "known refusal".into(),
            },
            commit: Some(MutationCommit {
                operation_id: "original-operation".into(),
                controller_revision: 1,
            }),
            observed_controller_revision: Some(1),
        },
        requires_quiescence: false,
    })
    .unwrap();
    save(&journal);
    audit_stopped_coding_collaboration(&f.reporter.config, &f.state, &f.reporter.client)
        .await
        .unwrap();
    journal["caller"]["incarnation"] = json!("new-incarnation");
    save(&journal);
    assert!(
        audit_stopped_coding_collaboration(&f.reporter.config, &f.state, &f.reporter.client)
            .await
            .is_err()
    );
    std::fs::write(&path, b"corrupt").unwrap();
    assert!(
        audit_stopped_coding_collaboration(&f.reporter.config, &f.state, &f.reporter.client)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn unbound_configured_child_refuses_before_parent_effects() {
    for mode in ["inference", "profile"] {
        let f = fixture();
        let mut task = input(&f, ExecutorKind::Native);
        let mut definition = input(&f, ExecutorKind::Coding).definition;
        if mode == "inference" {
            definition.inference = None;
        } else {
            definition.execution_profile = None;
        }
        task.native_workflow = Some(Box::new(NativeWorkflow::CodingThenContinue {
            child: CodingTaskTemplate { instruction_source: None,
                pull_request: None,
                instruction_prefix: "child".into(),
                definition,
                repository: Repository {
                    pull_request_target: None,
                    registration: Some(crate::repository_catalogue::Revision {
                        repository_id: "fixture-repository".into(), registration_revision: "fixture-revision".into(),
                    }),
                    url: "file:///nonexistent".into(),
                    start_commit: "a".repeat(40),
                    checkpoint_remote: "origin".into(),
                },
                acceptance_commands: vec![],
                dependencies: vec![],
                limits: task.limits.clone(),
            },
        }));
        let lock = Arc::new(std::fs::File::create(f.root.join("test-owner-lock")).unwrap());
        let error = native(
            &f.reporter.config,
            &task,
            &f.control,
            &f.reporter,
            None,
            None,
            &f.state,
            lock,
        )
        .await
        .err()
        .unwrap();
        assert_eq!(
            error,
            if mode == "inference" {
                "template_inference_unbound"
            } else {
                "template_profile_unbound"
            }
        );
        assert!(!f.state.join("native-tracking").exists());
        assert!(!f.state.join("outbox").exists());
    }
}

fn session_command(f: &Fixture) -> WorkerCommand {
    let task = input(f, ExecutorKind::Native);
    WorkerCommand::RunAttempt {
        command_id: "session-launch".into(),
        session: crate::protocol::Session {
            id: f.reporter.attempt.session_id.clone(),
            agent_id: task.definition.id.clone(),
            definition: task.definition.clone(),
            task_id: f.reporter.attempt.task_id.clone(),
        },
        attempt: f.reporter.attempt.clone(),
        task,
        checkpoint: None,
        continuation: None,
                workflow_continuation: None, completion_continuation: None,
    }
}

#[tokio::test]
async fn changed_session_association_refuses_before_receipt_report_or_launch() {
    for mode in [
        "session",
        "agent",
        "revision",
        "task",
        "attempt_session",
        "task_alias",
        "agent_alias",
        "attempt_alias",
    ] {
        let f = fixture();
        let mut command = session_command(&f);
        let WorkerCommand::RunAttempt {
            session, attempt, ..
        } = &mut command
        else {
            unreachable!()
        };
        match mode {
            "session" => session.id = "other-session".into(),
            "agent" => session.agent_id = "other-agent".into(),
            "revision" => session.definition.revision = "other-revision".into(),
            "task" => session.task_id = "other-task".into(),
            "attempt_session" => attempt.session_id = "other-session".into(),
            "task_alias" => {
                session.id = attempt.task_id.clone();
                attempt.session_id = session.id.clone();
            }
            "agent_alias" => {
                session.id = session.agent_id.clone();
                attempt.session_id = session.id.clone();
            }
            _ => {
                session.id = attempt.id.clone();
                attempt.session_id = session.id.clone();
            }
        }
        let path = f.state.join("commands/session-launch.json");
        assert_eq!(
            record_launch(&path, &command).unwrap_err(),
            "session_binding_mismatch"
        );
        assert_eq!(
            admit_launch(&f.reporter, &command, &f.control, &path)
                .await
                .unwrap_err(),
            "session_binding_mismatch"
        );
        assert!(!path.exists());
        assert!(!f.state.join("outbox").exists());
        assert!(!f.state.join("native-tracking").exists());
        assert!(!f.root.join("workspaces").exists());
    }
}

#[tokio::test]
async fn completed_recovered_command_cannot_hide_changed_session_binding() {
    let f = fixture();
    let command = session_command(&f);
    let path = f.state.join("commands/session-launch.json");
    record_launch(&path, &command).unwrap();
    let mut receipt = read_receipt(&path).unwrap();
    receipt.completed = true;
    let WorkerCommand::RunAttempt { session, .. } = &mut receipt.command else {
        unreachable!()
    };
    session.agent_id = "changed-agent".into();
    atomic_json(&path, &receipt).unwrap();
    let before = std::fs::read(&path).unwrap();
    assert_eq!(
        read_receipt(&path).err().unwrap(),
        "session_binding_mismatch"
    );
    assert_eq!(
        command_receipts(&f.state).err().unwrap(),
        "session_binding_mismatch"
    );
    assert_eq!(
        audit_stopped_coding_collaboration(&f.reporter.config, &f.state, &f.reporter.client)
            .await
            .unwrap_err(),
        "session_binding_mismatch"
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);
}

#[tokio::test]
async fn native_cleanup_marker_cannot_be_reassigned_to_another_session() {
    let f = fixture();
    track_native_launch(&f.state, &f.reporter.attempt).unwrap();
    let path = native_tracking_path(&f.state, &f.reporter.attempt);
    let before = std::fs::read(&path).unwrap();
    let mut changed = f.reporter.attempt.clone();
    changed.session_id = "other-session".into();
    assert!(track_native_launch(&f.state, &changed).is_err());
    assert!(
        native_registry_reconciled(&f.state, &changed)
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(path).unwrap(), before);
}

#[test]
fn coding_prompt_exposes_exact_accepted_commands_without_mutating_task() {
    let f = fixture();
    let mut task = input(&f, ExecutorKind::Coding);
    task.instruction = "Edit the selected source only.".into();
    task.acceptance_commands = vec![
        vec![
            "/toolchain with spaces/bin/rustc".into(),
            "--test".into(),
            "src/main.rs".into(),
            "-o".into(),
            "test binary".into(),
        ],
        vec![
            "./test binary".into(),
            "".into(),
            "quote\"slash\\line\n$() `literal` é".into(),
        ],
    ];
    let accepted = serde_json::to_value(&task).unwrap();
    let prompt = coding_prompt(&task, "accepted-checkpoint", &[]).unwrap();
    assert!(prompt.starts_with(&task.instruction));
    assert!(prompt.contains("Starting source checkpoint: accepted-checkpoint."));
    assert!(prompt.contains("Do not push, publish, open pull requests, deploy, or change credentials."));
    assert!(prompt.contains("Do not commit credentials, ignored caches, or unrelated files."));
    assert!(prompt.contains("data, not a shell command string"));
    assert!(prompt.contains("The worker independently reruns them from the acknowledged source checkpoint to decide acceptance."));
    let encoded = prompt
        .rsplit_once("Declared acceptance commands (JSON argv vectors):\n")
        .unwrap()
        .1;
    let commands: Vec<Vec<String>> = serde_json::from_str(encoded).unwrap();
    assert_eq!(commands, task.acceptance_commands);
    assert_eq!(serde_json::to_value(&task).unwrap(), accepted);
}

#[test]
fn coding_prompt_keeps_an_empty_accepted_command_list_empty() {
    let f = fixture();
    let task = input(&f, ExecutorKind::Coding);
    let prompt = coding_prompt(&task, "source-commit", &[]).unwrap();
    let encoded = prompt
        .rsplit_once("Declared acceptance commands (JSON argv vectors):\n")
        .unwrap()
        .1;
    assert_eq!(encoded, "[]");
    assert!(task.acceptance_commands.is_empty());
}

#[test]
fn coding_prompt_refuses_configured_credentials_without_rewriting_commands() {
    let f = fixture();
    for command in [
        vec!["/private/synthetic-credential/tool".into()],
        vec!["/toolchain/rustc".into(), "--token=synthetic-credential".into()],
    ] {
        let mut task = input(&f, ExecutorKind::Coding);
        task.acceptance_commands = vec![command];
        let accepted = serde_json::to_value(&task).unwrap();
        let error = coding_prompt(&task, "source-commit", &["synthetic-credential".into()])
            .unwrap_err();
        assert_eq!(error, "acceptance command contains configured credential");
        assert!(!error.contains("synthetic-credential"));
        assert_eq!(serde_json::to_value(&task).unwrap(), accepted);
    }
}

#[test]
fn coding_prompt_refuses_a_credential_formed_by_json_escaping() {
    let f = fixture();
    let mut task = input(&f, ExecutorKind::Coding);
    task.acceptance_commands = vec![vec!["/toolchain/compiler".into(), "line\nbreak".into()]];
    let accepted = serde_json::to_value(&task).unwrap();
    let error = coding_prompt(&task, "source-commit", &[r"line\nbreak".into()]).unwrap_err();
    assert_eq!(error, "acceptance command contains configured credential");
    assert_eq!(serde_json::to_value(&task).unwrap(), accepted);
}

#[test]
fn typed_supplied_execution_uncertainty_survives_plain_diagnostic() {
    let failure = AttemptFailure::from(codex::ExecutionFailure {
        message: "executor exited before its result".into(),
        collaboration_uncertain: false,
        execution_uncertain: true,
        completion_uncertain: false,
        completion_held: false,
    });
    assert!(matches!(failure, AttemptFailure::SuppliedExecutorUncertain(_)));
    assert!(failure.execution_uncertain(false));
}
