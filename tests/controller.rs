use borg_runtime::{
    controller::{Controller, ControllerConfig},
    protocol::*,
};
use serde_json::json;
#[path = "controller/host_resources.rs"]
mod host_resources;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

fn metadata() -> borg_runtime::limits::RuntimeLimits { let mut p=borg_runtime::limits::RuntimeLimits::seed().unwrap(); p.execution.native.context_bytes=65_536; p }
fn with_controller_limits(mut config:ControllerConfig, update:impl FnOnce(&mut borg_runtime::limits::ControllerLimits))->ControllerConfig {
    update(&mut Arc::make_mut(&mut config.limits).controller);config
}
fn fixture_repository_registration() -> borg_runtime::repository_catalogue::Registration {
    borg_runtime::repository_catalogue::Registration {
        pull_request_target: None,
        repository_id: "controller-fixture".into(),
        registration_revision: "revision-1".into(),
        display_name: "Controller fixture repository".into(),
        source_endpoint: "https://example.invalid/repo.git".into(),
        checkpoint_endpoint: "origin".into(),
        source_store_key: "controller-source".into(),
        checkpoint_store_key: "controller-checkpoint".into(),
    }
}
fn fixture_repository_binding() -> borg_runtime::repository_catalogue::Revision {
    fixture_repository_registration().binding()
}
async fn register_repository_fixture(c: &Controller, registration: &borg_runtime::repository_catalogue::Registration) {
    let outcome = call(c, Operation::RegisterRepository {
        operation_id: format!("fixture-register-{}-{}", registration.repository_id, registration.registration_revision),
        registration: registration.clone(),
    }).await;
    match outcome {
        Outcome::Ok { data: Data::RepositoryRegistration(entry) } => {
            assert_eq!(entry.repository_id, registration.repository_id);
            assert_eq!(entry.registration_revision, registration.registration_revision);
            assert_eq!(entry.source_endpoint, registration.source_endpoint);
            assert_eq!(entry.checkpoint_endpoint, registration.checkpoint_endpoint);
        }
        other => panic!("explicit fixture registration failed: {other:?}"),
    }
}
// Expected-value projection only. This never edits a submitted request, and it
// accepts only the explicit fixture registration supplied by the test.
fn project_repository_fixture(input: &mut TaskInput, registration: &borg_runtime::repository_catalogue::Registration) {
    let binding = registration.binding();
    let project = |repository: &mut Repository| {
        if repository.registration.as_ref() == Some(&binding) {
            assert!(repository.url.is_empty() || repository.url == registration.source_endpoint);
            assert!(repository.checkpoint_remote.is_empty() || repository.checkpoint_remote == registration.checkpoint_endpoint);
            repository.url = registration.source_endpoint.clone();
            repository.checkpoint_remote = registration.checkpoint_endpoint.clone();
        }
    };
    if let Some(repository) = &mut input.repository { project(repository); }
    if let Some(repository) = &mut input.repository_read {
        if repository.registration.as_ref() == Some(&binding) {
            assert!(repository.repository.is_empty() || repository.repository == registration.source_endpoint);
            repository.repository = registration.source_endpoint.clone();
        }
    }
    if let Some(NativeWorkflow::CodingThenContinue { child, .. }) = input.native_workflow.as_deref_mut() {
        project(&mut child.repository);
    }
}
fn accepted_input(controller:&Controller, input:&TaskInput)->TaskInput {
    let mut accepted=input.clone();
    if accepted.resolved_limits.is_none() { accepted.resolved_limits=Some(controller.limits().resolve_task(input).unwrap()); }
    project_repository_fixture(&mut accepted, &fixture_repository_registration());
    accepted
}

static NEXT: AtomicU64 = AtomicU64::new(1);

#[test]
fn selected_native_sqlite_is_the_linked_engine() {
    assert_eq!(rusqlite::version(), "3.54.0");
}
fn path() -> PathBuf {
    loop {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p = std::env::temp_dir().join(format!(
            "borg-controller-{}-{stamp}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match std::fs::create_dir(&p) {
            Ok(()) => return p.join("task.db"),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => panic!("fresh controller fixture: {e}"),
        }
    }
}
fn config() -> ControllerConfig {
    with_controller_limits(ControllerConfig {
        limits: std::sync::Arc::new(metadata()),
        validation_executable: PathBuf::from(env!("CARGO_BIN_EXE_borg-runtime")),
        ..ControllerConfig::default()
    }, |limits| { limits.command_wait_ms = (Duration::from_millis(20)).as_millis().try_into().unwrap(); })
}
fn future() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 600_000
}
fn native(agent: &str) -> TaskInput {
    TaskInput { instruction_source: None, pull_request: None, resolved_limits: None,
        repository_read: None,
        instruction: "produce one structured answer".into(),
        definition: Definition {charter: None, limit_overrides: None,
            execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
            inference: Some(borg_runtime::protocol::InferenceSettings::selected()),
            id: agent.into(),
            revision: "revision-1".into(),
            executor: ExecutorKind::Native,
        },
        repository: None,
        acceptance_commands: vec![],
        output_schema: Some(
            json!({"type":"object","required":["answer"],"properties":{"answer":{"type":"integer"}},"additionalProperties":false}),
        ),
        limits: Limits {
            deadline_unix_ms: future(),
            model_requests: 10,
            output_bytes: 16_384,
            result_bytes: metadata().task_defaults.result_bytes,
            max_delegation_depth: 4,
        },
        dependencies: vec![],
        native_workflow: None,
    }
}
fn coding(agent: &str) -> TaskInput {
    let mut t = native(agent);
    t.definition = Definition {charter: None, limit_overrides: None,
        execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
        inference: Some(borg_runtime::protocol::InferenceSettings::selected()),
        id: agent.into(),
        revision: "revision-1".into(),
        executor: ExecutorKind::Coding,
    };
    t.repository = Some(Repository {
        pull_request_target: None,
        registration: Some(fixture_repository_binding()),
        url: String::new(),
        start_commit: "a".repeat(40),
        checkpoint_remote: String::new(),
    });
    t.acceptance_commands = vec![vec!["cargo".into(), "test".into()]];
    t.output_schema = None;
    t
}
async fn call(c: &Controller, command: Operation) -> Outcome {
    Box::pin(c.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: format!("request-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
        command,
    }))
    .await
    .outcome
}
fn task(o: Outcome) -> Task {
    match o {
        Outcome::Ok {
            data: Data::Task(t),
        } => *t,
        x => panic!("expected task, got {x:?}"),
    }
}
fn code(o: Outcome) -> String {
    match o {
        Outcome::Refused { code, .. } => code,
        x => panic!("expected refusal, got {x:?}"),
    }
}
async fn submit(c: &Controller, id: &str, input: TaskInput) -> Task {
    // Only this named positive fixture is registered here. Arbitrary inputs and
    // raw call()/handle() paths are never inferred, repaired, or registered.
    let fixed = fixture_repository_binding();
    let repository = input.repository.as_ref().or_else(|| match input.native_workflow.as_deref() {
        Some(NativeWorkflow::CodingThenContinue { child, .. }) => Some(&child.repository),
        _ => None,
    });
    if let Some(repository) = repository.filter(|r| r.registration.as_ref() == Some(&fixed)) {
        assert!(repository.url.is_empty());
        assert!(repository.checkpoint_remote.is_empty());
        register_repository_fixture(c, &fixture_repository_registration()).await;
    }
    task(
        call(
            c,
            Operation::SubmitTask {
                operation_id: id.into(),
                input,
            },
        )
        .await,
    )
}
async fn get(c: &Controller, id: &str) -> Task {
    task(call(c, Operation::GetTask { task_id: id.into() }).await)
}
fn worker(id: &str, inc: &str, native: usize, coding: usize) -> Worker {
    Worker {
        id: id.into(),
        incarnation: inc.into(),
        coding_capacity: coding,
        native_capacity: native,
    }
}
async fn register(c: &Controller, w: &Worker) {
    assert!(matches!(
        call(
            c,
            Operation::RegisterWorker {
                operation_id: format!("register-{}-{}", w.id, w.incarnation),
                worker: w.clone()
            }
        )
        .await,
        Outcome::Ok {
            data: Data::Worker(_)
        }
    ));
}
async fn commands(c: &Controller, w: &Worker, ack: Vec<String>) -> Vec<WorkerCommand> {
    match call(
        c,
        Operation::AwaitCommands {
            operation_id: format!("await-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
            worker_id: w.id.clone(),
            incarnation: w.incarnation.clone(),
            acknowledged: ack,
        },
    )
    .await
    {
        Outcome::Ok {
            data: Data::Commands(v),
        } => v,
        Outcome::Pending { .. } => vec![],
        x => panic!("expected commands {x:?}"),
    }
}
fn run(v: &[WorkerCommand]) -> Attempt {
    v.iter()
        .find_map(|c| match c {
            WorkerCommand::RunAttempt { attempt, .. } => Some(attempt.clone()),
            _ => None,
        })
        .expect("run command")
}
async fn report(c: &Controller, a: &Attempt, id: &str, observation: Observation) -> Outcome {
    call(
        c,
        Operation::ObserveWorker {
            report: Report {
                report_id: id.into(),
                worker_id: a.worker_id.clone(),
                incarnation: a.worker_incarnation.clone(),
                task_id: a.task_id.clone(),
                attempt_id: a.id.clone(),
                generation: a.generation,
                observation,
            },
        },
    )
    .await
}
/// Controlled positive request/response facts through the canonical receipt path.
/// No provider is called: the fixed two-byte body is a fixture observation only.
/// Deliberate malformed/over-budget Usage tests continue to use report unchanged.
async fn controlled_native_request(
    c: &Controller,
    a: &Attempt,
    index: u32,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
) {
    let operation = |report_id, observation| Operation::ObserveWorker {
        report: Report {
            report_id,
            worker_id: a.worker_id.clone(),
            incarnation: a.worker_incarnation.clone(),
            task_id: a.task_id.clone(),
            attempt_id: a.id.clone(),
            generation: a.generation,
            observation,
        },
    };
    let reservation = operation(
        borg_runtime::native_loop_contract::reservation_id(&a.id, index),
        Observation::Usage {
            native_diagnostic_reserved: true,
            model_requests: index,
            input_tokens,
            output_tokens,
        },
    );
    let reserved = Box::pin(c.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: format!(
            "positive-reservation-{}",
            NEXT.fetch_add(1, Ordering::Relaxed)
        ),
        command: reservation.clone(),
    }))
    .await;
    reserved.validate_for(&reservation).unwrap();
    let accepted = task(reserved.outcome.clone());
    assert_eq!(accepted.id, a.task_id);
    assert_eq!(accepted.attempt.as_ref().unwrap().id, a.id);
    let reservation_revision = reserved.commit.as_ref().unwrap().controller_revision;
    let diagnostic = operation(
        borg_runtime::native_loop_contract::diagnostic_id(&a.id, index),
        Observation::NativeRequestDiagnostic {
            request_index: index,
            endpoint: "https://api.openai.com/v1/responses".into(),
            model: accepted
                .input
                .definition
                .required_inference()
                .unwrap()
                .model
                .clone(),
            request_binding: Some(borg_runtime::native::ProviderRequestBinding {
                bytes: 2,
                sha256: "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a".into(),
            }),
            observed_outcome:
                borg_runtime::native_loop_contract::NativeRequestObservedOutcome::Completed,
            failure: None,
        },
    );
    let observed = Box::pin(c.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: format!(
            "positive-diagnostic-{}",
            NEXT.fetch_add(1, Ordering::Relaxed)
        ),
        command: diagnostic.clone(),
    }))
    .await;
    observed.validate_for(&diagnostic).unwrap();
    assert!(
        matches!(
            observed.outcome,
            Outcome::Ok {
                data: Data::Acknowledged
            }
        ),
        "{observed:?}"
    );
    assert!(observed.commit.as_ref().unwrap().controller_revision > reservation_revision);
}

/// Controlled one-response completion, using the actual canonical receipt path.
/// Callers provide the matching stable Usage reservation before this observation.
async fn complete_native_loop(c: &Controller, a: &Attempt) {
    task(
        report(
            c,
            a,
            &borg_runtime::native_loop_contract::completion_id(&a.id),
            Observation::NativeLoopCompleted {
                requests: Vec::new(),
                final_response_index: None,
                model_requests: 1,
                calls: vec![],
            },
        )
        .await,
    );
}
async fn finish_native(c: &Controller, a: &Attempt, label: &str) -> Task {
    task(
        report(
            c,
            a,
            &format!("{label}-started"),
            Observation::Started {
                thread_id: None,
                turn_id: None,
            },
        )
        .await,
    );
    controlled_native_request(c, a, 1, Some(4), Some(2)).await;
    task(
        report(
            c,
            a,
            &borg_runtime::native_loop_contract::completion_id(&a.id),
            Observation::NativeLoopCompleted {
                requests: Vec::new(),
                final_response_index: None,
                model_requests: 1,
                calls: vec![],
            },
        )
        .await,
    );
    task(
        report(
            c,
            a,
            &format!("{label}-executor"),
            Observation::ExecutorOutcome {
                success: true,
                summary: "actual controlled completion".into(),
            },
        )
        .await,
    );
    task(
        report(
            c,
            a,
            &format!("{label}-stopped"),
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    task(
        report(
            c,
            a,
            &format!("{label}-result"),
            Observation::ResultCandidate {
                checks_passed: true,
                summary: "accepted structured answer".into(),
                checkpoint: None,
                validation: vec![borg_runtime::native_loop_contract::evidence(&a.id)],
                output: Some(json!({"answer":42})),
            },
        )
        .await,
    )
}

#[tokio::test]
//= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
//# An accepted task MUST have a durable input record.
//= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
//# A terminal task result MUST be committed before it is reported as final.
//= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
//# Committed platform task state MUST survive an application-process restart while the durable store remains intact.
//= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
//# The platform MUST persist explicitly recorded agent memory in its database.
//= spec/impl/v2/implementation.md#definitions-select-execution
//# An agent identity MUST persist independently of any executor process.
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//# An exact retry of a committed mutating request MUST return its recorded outcome.
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//# Reusing a mutating request's identity with different input MUST fail without changing its recorded outcome.
//= spec/impl/v2/implementation.md#task-state-separates-execution-from-acceptance
//# A succeeded task MUST NOT be reopened by a retry request.
async fn real_sqlite_task_result_history_memory_and_restart() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let input = native("agent-a");
    let t = submit(&c, "submit-a", input.clone()).await;
    assert_eq!(submit(&c, "submit-a", input.clone()).await, t);
    let mut changed = input;
    changed.instruction.push_str(" different");
    assert_eq!(
        code(
            call(
                &c,
                Operation::SubmitTask {
                    operation_id: "submit-a".into(),
                    input: changed
                }
            )
            .await
        ),
        "operation_conflict"
    );
    let w = worker("worker-a", "inc-a", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let memory = call(
        &c,
        Operation::SetMemory {
            operation_id: "memory-a".into(),
            task_id: t.id.clone(),
            attempt_id: a.id.clone(),
            generation: a.generation,
            key: "finding".into(),
            expected_revision: None,
            value: json!({"observed":true}),
        },
    )
    .await;
    assert!(matches!(
        memory,
        Outcome::Ok {
            data: Data::Memory(Some(MemoryValue { revision: 1, .. }))
        }
    ));
    let accepted = finish_native(&c, &a, "first").await;
    assert_eq!(accepted.status, TaskStatus::Succeeded);
    assert_eq!(
        accepted.result.as_ref().unwrap().output,
        Some(json!({"answer":42}))
    );
    drop(c);
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(get(&c, &t.id).await, accepted);
    assert!(matches!(
        call(
            &c,
            Operation::GetMemory {
                agent_id: "agent-a".into(),
                key: "finding".into()
            }
        )
        .await,
        Outcome::Ok {
            data: Data::Memory(Some(MemoryValue { revision: 1, .. }))
        }
    ));
    assert_eq!(
        code(
            call(
                &c,
                Operation::RetryTask {
                    operation_id: "retry-success".into(),
                    task_id: t.id,
                    expected_revision: accepted.revision
                }
            )
            .await
        ),
        "invalid_transition"
    );
}

#[tokio::test]
// Traceability scope: Intact local SQLite persistence, not storage destruction or deployment authentication.
//= spec/impl/v2/implementation.md#observable-end-to-end-result
//= type=test
//# The initial system MUST durably record an accepted task before reporting acceptance.
async fn dropped_acknowledgment_replays_one_durable_submission() {
    use std::{
        future::{Future, poll_fn},
        task::Poll,
    };
    let c = Controller::open_with_config(path(), config()).unwrap();
    let input = native("agent-a");
    let mut request = Box::pin(c.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: "lost-http".into(),
        command: Operation::SubmitTask {
            operation_id: "lost-submit".into(),
            input: input.clone(),
        },
    }));
    poll_fn(|cx| {
        let _ = request.as_mut().poll(cx);
        Poll::Ready(())
    })
    .await;
    drop(request);
    let t = submit(&c, "lost-submit", input).await;
    match call(
        &c,
        Operation::ListTasks {
            after: None,
            limit: 10,
        },
    )
    .await
    {
        Outcome::Ok {
            data: Data::Tasks(v),
        } => assert_eq!(v, vec![t]),
        x => panic!("{x:?}"),
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
//# An execution attempt MUST have a durable identity before it begins.
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//# An attempt assignment MUST commit its worker command in the same transaction.
//= spec/impl/v2/implementation.md#task-state-separates-execution-from-acceptance
//# A retry MUST create a new attempt without changing the task identity.
//= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
//# A cancellation acknowledgment MUST distinguish requested cancellation from verified execution termination.
//= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
//# An automatic retry MUST wait until prior execution is known unable to continue.
//= spec/impl/v2/implementation.md#interrupted-work-is-reconciled
//# A recovery attempt MUST preserve the original task identity.
async fn duplicate_commands_restart_and_incarnation_do_not_create_execution() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let t = submit(&c, "s", native("a")).await;
    let w = worker("w", "old", 1, 0);
    register(&c, &w).await;
    let first = commands(&c, &w, vec![]).await;
    assert_eq!(commands(&c, &w, vec![]).await, first);
    let a = run(&first);
    let new = worker("w", "new", 1, 0);
    assert_eq!(
        code(
            call(
                &c,
                Operation::RegisterWorker {
                    operation_id: format!(
                        "register-request-{}",
                        NEXT.fetch_add(1, Ordering::Relaxed)
                    ),
                    worker: new.clone()
                }
            )
            .await
        ),
        "reconciliation_required"
    );
    drop(c);
    let c = Controller::open_with_config(&p, config()).unwrap();
    register(&c, &w).await;
    assert_eq!(commands(&c, &w, vec![]).await, first);
    let before = get(&c, &t.id).await;
    assert_eq!(before.attempt.as_ref().unwrap().id, a.id);
    let cancelled = task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "cancel".into(),
                task_id: t.id.clone(),
                expected_revision: before.revision,
            },
        )
        .await,
    );
    assert_eq!(
        code(
            call(
                &c,
                Operation::RetryTask {
                    operation_id: "unsafe-retry".into(),
                    task_id: t.id.clone(),
                    expected_revision: cancelled.revision
                }
            )
            .await
        ),
        "reconciliation_required"
    );
    let stopped = task(report(&c, &a, "stopped", Observation::ProcessTreeStopped).await);
    register(&c, &new).await;
    let queued = task(
        call(
            &c,
            Operation::RetryTask {
                operation_id: "safe-retry".into(),
                task_id: t.id.clone(),
                expected_revision: stopped.revision,
            },
        )
        .await,
    );
    assert_eq!(queued.status, TaskStatus::Queued);
    let second = run(&commands(&c, &new, vec![]).await);
    assert_eq!(second.generation, a.generation + 1);
    assert_ne!(second.id, a.id);
    assert_eq!(
        code(
            report(
                &c,
                &a,
                "stale-output",
                Observation::Progress {
                    text: "old".into(),
                    output: "wrong".into(),
                    truncated: false
                }
            )
            .await
        ),
        "stale_generation"
    );
    assert!(get(&c, &t.id).await.recent_output.is_empty());
}

#[tokio::test]
//= spec/impl/v2/implementation.md#definitions-select-execution
//# An agent identity MUST support multiple concurrent sessions.
//# A session MUST have at most one active execution attempt at a time.
//= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
//# A task MUST start only after its prerequisites have succeeded.
//= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
//# Task admission MUST select the oldest eligible task within the requested executor's available capacity.
async fn oldest_eligible_skips_failed_dependency_and_allows_independent_sessions() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let w = worker("w", "i", 4, 0);
    register(&c, &w).await;
    let first = submit(&c, "one", native("same")).await;
    let second = submit(&c, "two", native("same")).await;
    let other = submit(&c, "three", native("other")).await;
    let v = commands(&c, &w, vec![]).await;
    let ids: Vec<_> = v
        .iter()
        .filter_map(|x| match x {
            WorkerCommand::RunAttempt { attempt, .. } => Some(attempt.task_id.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec![first.id.clone(), second.id.clone(), other.id]);
    let original_second = v.iter().find_map(|cmd| match cmd { WorkerCommand::RunAttempt{attempt,..} if attempt.task_id==second.id => Some(attempt.clone()), _=>None }).unwrap();
    assert_ne!(first.session_id,second.session_id);
    let a = run(&v);
    finish_native(&c, &a, "a").await;
    let v = commands(&c, &w, vec![]).await;
    assert!(
        v.iter().any(
            |x| matches!(x,WorkerCommand::RunAttempt{attempt,..} if attempt.task_id==second.id)
        )
    );
    let redelivered: Vec<_> = v.iter().filter_map(|cmd| match cmd { WorkerCommand::RunAttempt{attempt,..} if attempt.task_id==second.id => Some(attempt), _=>None }).collect();
    assert_eq!(redelivered, vec![&original_second], "same session keeps its one original active attempt");
    let mut dependent = native("third");
    dependent.dependencies = vec![second.id.clone()];
    let dep = submit(&c, "dep", dependent).await;
    let current = get(&c, &second.id).await;
    task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "cancel-two".into(),
                task_id: second.id,
                expected_revision: current.revision,
            },
        )
        .await,
    );
    let v = commands(&c, &w, vec![]).await;
    assert!(
        !v.iter()
            .any(|x| matches!(x,WorkerCommand::RunAttempt{attempt,..} if attempt.task_id==dep.id))
    );
    assert!(
        get(&c, &dep.id)
            .await
            .blocked_reason
            .unwrap()
            .contains("Cancelled")
    );
}

#[tokio::test]
//= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
//# Successful completion and cancellation MUST be ordered by the controller's committed task revision.
async fn cancellation_and_success_are_ordered_by_committed_state() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let w = worker("w", "i", 1, 0);
    register(&c, &w).await;
    let t = submit(&c, "s", native("a")).await;
    let a = run(&commands(&c, &w, vec![]).await);
    // Current native admission retains the marked charge and its exact terminal
    // diagnostic before accepting completion. Historical shared helpers stay
    // unchanged; this is controlled evidence through the real receipt path.
    task(report(&c, &a, "success-started", Observation::Started {
        thread_id: None, turn_id: None,
    }).await);
    governance::completed_native_request(&c, &a, Some(4), Some(2)).await;
    complete_native_loop(&c, &a).await;
    task(report(&c, &a, "success-executor", Observation::ExecutorOutcome {
        success: true, summary: "actual controlled completion".into(),
    }).await);
    task(report(&c, &a, "success-stopped", Observation::ProcessTreeStopped).await);
    let success = task(report(&c, &a, "success-result", Observation::ResultCandidate {
        checks_passed: true, summary: "accepted structured answer".into(),
        checkpoint: None,
        validation: vec![borg_runtime::native_loop_contract::evidence(&a.id)],
        output: Some(json!({"answer":42})),
    }).await);
    let answer = task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "late-cancel".into(),
                task_id: t.id,
                expected_revision: success.revision,
            },
        )
        .await,
    );
    assert_eq!(answer, success);
    let t = submit(&c, "s2", native("b")).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let current = get(&c, &t.id).await;
    task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "first-cancel".into(),
                task_id: t.id.clone(),
                expected_revision: current.revision,
            },
        )
        .await,
    );
    task(
        report(
            &c,
            &a,
            "late-executor",
            Observation::ExecutorOutcome {
                success: true,
                summary: "late".into(),
            },
        )
        .await,
    );
    let result = report(
        &c,
        &a,
        "late-result",
        Observation::ResultCandidate {
            checks_passed: true,
            summary: "late".into(),
            checkpoint: None,
            validation: vec![],
            output: Some(json!({"answer":1})),
        },
    )
    .await;
    assert_eq!(code(result), "invalid_transition");
    let cancelled = get(&c, &t.id).await;
    assert_eq!(cancelled.status, TaskStatus::Cancelled);
    assert!(cancelled.result.is_none());
}

#[tokio::test]
//= spec/impl/v2/implementation.md#checkpoint-publication-is-reconciled
//# The publication record MUST identify the repository, exact commit and intended remote reference.
//= spec/impl/v2/implementation.md#work-starts-from-a-durable-source
//# A checkpoint MUST NOT be reported durable before its external storage acknowledges it.
async fn checkpoint_intent_ack_identity_and_atomic_selection() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let t = submit(&c, "s", coding("a")).await;
    let w = worker("w", "i", 0, 1);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let cp = Checkpoint {
        registration: fixture_repository_binding(),
        repository: t
            .input
            .repository
            .as_ref()
            .unwrap()
            .checkpoint_remote
            .clone(),
        commit: "b".repeat(40),
        remote_ref: checkpoint_ref(&t.id, &a.id, "cp"),
        operation_id: "cp".into(),
    };
    // The source clone URL and checkpoint destination are independent. Each
    // branch component binds the actual task, attempt and operation identity.
    for mode in [
        "clone-url",
        "raw-components",
        "wrong-attempt",
        "extra-suffix",
    ] {
        let operation_id = format!("invalid-{mode}");
        let mut candidate = cp.clone();
        candidate.operation_id = operation_id.clone();
        candidate.remote_ref = checkpoint_ref(&t.id, &a.id, &operation_id);
        match mode {
            "clone-url" => candidate.repository = t.input.repository.as_ref().unwrap().url.clone(),
            "raw-components" => {
                candidate.remote_ref =
                    format!("refs/heads/borg/tasks/{}/{}/{}", t.id, a.id, operation_id)
            }
            "wrong-attempt" => {
                candidate.remote_ref = checkpoint_ref(&t.id, "another-attempt", &operation_id)
            }
            "extra-suffix" => candidate.remote_ref.push_str("/other"),
            _ => unreachable!(),
        }
        assert_eq!(code(call(&c, Operation::PrepareOperation {
            operation_id,
            task_id: t.id.clone(),
            attempt_id: a.id.clone(),
            generation: a.generation,
            kind: "checkpoint".into(),
            intended: json!({"registration":candidate.registration,"repository":candidate.repository,"commit":candidate.commit,"remote_ref":candidate.remote_ref}),
        }).await), "invalid_input", "{mode}");
    }
    let prepare = Operation::PrepareOperation {
        operation_id: "cp".into(),
        task_id: t.id.clone(),
        attempt_id: a.id.clone(),
        generation: a.generation,
        kind: "checkpoint".into(),
        intended: json!({"registration":cp.registration,"repository":cp.repository,"commit":cp.commit,"remote_ref":cp.remote_ref}),
    };
    assert!(
        matches!(call(&c,prepare.clone()).await,Outcome::Ok{data:Data::Operation(OperationRecord{state,..})} if state=="pending")
    );
    let settle =
        |settlement_id: &str, checkpoint: Option<Checkpoint>, outcome| Operation::SettleOperation {
            settlement_id: settlement_id.into(),
            operation_id: "cp".into(),
            task_id: t.id.clone(),
            attempt_id: a.id.clone(),
            generation: a.generation,
            outcome,
            checkpoint,
        };
    assert_eq!(
        code(
            call(
                &c,
                settle(
                    "cp-unacknowledged",
                    Some(cp.clone()),
                    json!({"acknowledged":false})
                )
            )
            .await
        ),
        "checkpoint_unsettled"
    );
    assert!(get(&c, &t.id).await.selected_checkpoint.is_none());
    let mut wrong = cp.clone();
    wrong.commit = "c".repeat(40);
    assert_eq!(
        code(
            call(
                &c,
                settle("cp-wrong", Some(wrong), json!({"acknowledged":true}))
            )
            .await
        ),
        "operation_conflict"
    );
    let committed = call(
        &c,
        settle(
            "cp-accepted",
            Some(cp.clone()),
            json!({"acknowledged":true}),
        ),
    )
    .await;
    assert_eq!(
        call(
            &c,
            settle(
                "cp-accepted",
                Some(cp.clone()),
                json!({"acknowledged":true})
            )
        )
        .await,
        committed
    );
    assert_eq!(get(&c, &t.id).await.selected_checkpoint, Some(cp.clone()));
    drop(c);
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(get(&c, &t.id).await.selected_checkpoint, Some(cp.clone()));
    task(
        report(
            &c,
            &a,
            "executor",
            Observation::ExecutorOutcome {
                success: true,
                summary: "changed source".into(),
            },
        )
        .await,
    );
    task(report(&c, &a, "stop", Observation::ProcessTreeStopped).await);
    let accepted = task(
        report(
            &c,
            &a,
            "candidate",
            Observation::ResultCandidate {
                checks_passed: true,
                summary: "verified change".into(),
                checkpoint: Some(cp),
                validation: vec!["cargo test: pass".into()],
                output: None,
            },
        )
        .await,
    );
    assert_eq!(accepted.status, TaskStatus::Succeeded);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#task-state-separates-execution-from-acceptance
//# An executor outcome MUST remain distinct from the task's accepted outcome.
//= spec/impl/v2/implementation.md#observable-end-to-end-result
//# A task MUST NOT be reported successful solely because its executor exited successfully.
async fn native_schema_and_acceptance_are_not_executor_exit_status() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let t = submit(&c, "s", native("a")).await;
    let w = worker("w", "i", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    task(
        report(
            &c,
            &a,
            "executor",
            Observation::ExecutorOutcome {
                success: true,
                summary: "exit zero".into(),
            },
        )
        .await,
    );
    assert_eq!(get(&c, &t.id).await.status, TaskStatus::Active);
    controlled_native_request(&c, &a, 1, None, None).await;
    complete_native_loop(&c, &a).await;
    task(report(&c, &a, "schema-stop", Observation::ProcessTreeStopped).await);
    assert_eq!(
        code(
            report(
                &c,
                &a,
                "bad-schema",
                Observation::ResultCandidate {
                    checks_passed: true,
                    summary: "bad".into(),
                    checkpoint: None,
                    validation: vec![borg_runtime::native_loop_contract::evidence(&a.id)],
                    output: Some(json!({"answer":"not an integer"}))
                }
            )
            .await
        ),
        "schema_validation"
    );
    let failed = task(
        report(
            &c,
            &a,
            "failed-check",
            Observation::ResultCandidate {
                checks_passed: false,
                summary: "failed checks".into(),
                checkpoint: None,
                validation: vec![borg_runtime::native_loop_contract::evidence(&a.id)],
                output: Some(json!({"answer":42})),
            },
        )
        .await,
    );
    assert_eq!(failed.status, TaskStatus::Failed);
    assert!(failed.result.is_none());
}

#[tokio::test]
// Traceability scope: A controlled clock drives the actual restarted controller timer; original task/attempt deadlines persist and an expired retry is refused.
//= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
//= type=test
//# Reconnecting or resuming an attempt MUST NOT reset its execution deadline.
async fn deadline_persists_without_reset_and_produces_stop_command() {
    let p = path();
    let phase = Arc::new(AtomicU64::new(0));
    let read_phase = phase.clone();
    let (idle_sent, idle_sample) = tokio::sync::oneshot::channel();
    let (expiry_sent, expiry_sample) = tokio::sync::oneshot::channel();
    let idle_sent = std::sync::Mutex::new(Some(idle_sent));
    let expiry_sent = std::sync::Mutex::new(Some(expiry_sent));
    let cfg = ControllerConfig {
        clock: Arc::new(move || match read_phase.load(Ordering::SeqCst) {
            0 => 1000,
            1 => {
                // The restarted, otherwise idle owner samples its next timer
                // delay. Keep the original deadline one millisecond ahead.
                read_phase.store(2, Ordering::SeqCst);
                let _ = idle_sent.lock().unwrap().take().unwrap().send(1999u64);
                1999
            }
            2 => {
                // No API request is sent between these samples. This sample
                // therefore comes from expire() after the real recv_timeout.
                read_phase.store(3, Ordering::SeqCst);
                let _ = expiry_sent.lock().unwrap().take().unwrap().send(2001u64);
                2001
            }
            _ => 2001,
        }),
        ..config()
    };
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    // This case qualifies the common controller deadline/stop state machine.
    // Coding setup avoids imposing an unrelated one-second real subprocess
    // deadline on native schema preflight; native timeout has dedicated cases.
    let mut input = coding("a");
    input.limits.deadline_unix_ms = 2000;
    let t = submit(&c, "s", input).await;
    let w = worker("w", "i", 0, 1);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    assert_eq!(a.deadline_unix_ms, 2000);
    drop(c);
    controller_owner_released(&p).await;
    phase.store(1, Ordering::SeqCst);
    let c = Controller::open_with_config(&p, cfg).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), idle_sample)
            .await
            .unwrap()
            .unwrap(),
        1999
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), expiry_sample)
            .await
            .unwrap()
            .unwrap(),
        2001
    );
    // Only now query: the idle timer, rather than this read, started expiry.
    let expired = get(&c, &t.id).await;
    assert_eq!(expired.status, TaskStatus::Failed);
    assert_eq!(expired.input.limits.deadline_unix_ms, 2000);
    let retained_attempt = expired.attempt.as_ref().unwrap();
    assert_eq!(retained_attempt.id, a.id);
    assert_eq!(retained_attempt.generation, a.generation);
    assert_eq!(retained_attempt.deadline_unix_ms, 2000);
    assert_eq!(retained_attempt.phase, AttemptPhase::StopRequested);
    assert!(
        commands(&c, &w, vec![])
            .await
            .iter()
            .any(|c| matches!(c,WorkerCommand::StopAttempt{attempt_id,..} if attempt_id==&a.id))
    );
    let stopped = task(report(&c, &a, "stopped", Observation::ProcessTreeStopped).await);
    assert_eq!(stopped.input.limits.deadline_unix_ms, 2000);
    assert_eq!(
        code(
            call(
                &c,
                Operation::RetryTask {
                    operation_id: "retry".into(),
                    task_id: t.id,
                    expected_revision: stopped.revision
                }
            )
            .await
        ),
        "deadline"
    );
}

#[tokio::test]
//= spec/impl/v2/implementation.md#delegation-is-a-task-handoff
//# A delegation MUST create a separately identified task.
//= spec/impl/v2/implementation.md#delegation-is-a-task-handoff
//# A delegated task MUST retain its parent task association.
async fn delegation_rejects_transitive_cycle_and_keeps_parent_budget() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let p = submit(&c, "parent", native("parent-agent")).await;
    let mut dependent = native("dependent");
    dependent.dependencies = vec![p.id.clone()];
    let d = submit(&c, "dependent", dependent).await;
    let w = worker("w", "i", 2, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let mut child = native("child");
    child.limits.deadline_unix_ms = p.input.limits.deadline_unix_ms;
    child.limits.model_requests = 3;
    child.dependencies = vec![d.id];
    let delegate = |id: &str, child| Operation::DelegateTask {
        operation_id: id.into(),
        parent_task_id: p.id.clone(),
        parent_attempt_id: a.id.clone(),
        generation: a.generation,
        child,
    };
    assert_eq!(
        code(call(&c, delegate("cycle", child.clone())).await),
        "dependency_cycle"
    );
    child.dependencies.clear();
    child.limits.model_requests = 11;
    assert_eq!(
        code(call(&c, delegate("budget", child.clone())).await),
        "capacity"
    );
    child.limits.model_requests = 3;
    let ch = task(call(&c, delegate("child", child.clone())).await);
    assert_eq!(task(call(&c, delegate("child", child)).await), ch);
    assert_eq!(ch.parent_task_id, Some(p.id.clone()));
    let waiting = get(&c, &p.id).await;
    assert_eq!(waiting.waiting_child_id, Some(ch.id));
    assert!(waiting.attempt.unwrap().stopped == false);
}

#[test]
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//# An unsupported stored schema version MUST be refused without rewriting its data.
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//# Corrupt stored task state MUST be refused without rewriting its data.
fn unknown_or_corrupt_schema_is_not_rewritten() {
    // Versions 1 through 11 are explicit unsupported predecessors, never automatic
    // migrations. Preserve their bytes alongside an unknown future version.
    for version in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 999] {
        let p = path();
        {
            let db = rusqlite::Connection::open(&p).unwrap();
            db.execute_batch("CREATE TABLE sentinel(value TEXT); INSERT INTO sentinel VALUES('preserve'); PRAGMA application_id=1112494642;").unwrap();
            db.pragma_update(None, "user_version", version).unwrap();
        }
        let before = std::fs::read(&p).unwrap();
        let error = match Controller::open_with_config(&p, config()) {
            Ok(_) => panic!("unsupported schema was accepted"),
            Err(error) => error,
        };
        assert!(error.contains("migration required"), "{error}");
        assert_eq!(std::fs::read(&p).unwrap(), before);
    }
    let p = path();
    std::fs::write(&p, b"this is not a sqlite database; preserve it").unwrap();
    let before = std::fs::read(&p).unwrap();
    assert!(Controller::open_with_config(&p, config()).is_err());
    assert_eq!(std::fs::read(&p).unwrap(), before);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//# A controller transaction MUST NOT remain open across worker, provider or repository I/O.
async fn waiting_command_read_is_armed_before_new_submission() {
    let c = Controller::open_with_config(
        path(),
        with_controller_limits(ControllerConfig {
            ..config()
        }, |limits| { limits.command_wait_ms = (Duration::from_secs(2)).as_millis().try_into().unwrap(); }),
    )
    .unwrap();
    let w = worker("w", "i", 1, 0);
    register(&c, &w).await;
    let waiter = {
        let c = c.clone();
        let w = w.clone();
        tokio::spawn(async move { commands(&c, &w, vec![]).await })
    };
    tokio::task::yield_now().await;
    let t = submit(&c, "wake", native("a")).await;
    let observed = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run(&observed).task_id, t.id);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#delegation-is-a-task-handoff
//# A child's terminal outcome MUST settle its parent-wait association before the parent resumes from that outcome.
//= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
//# An automatic retry MUST wait until prior execution is known unable to continue.
//= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
//# A task waiting for delegated work MUST release its execution slot.
// Traceability scope: Owned transaction/controlled race coverage, not an exhaustive arbitrary SQLite I/O fault matrix.
//= spec/impl/v2/implementation.md#delegation-is-a-task-handoff
//= type=test
//# A delegation MUST commit its child task and parent-wait association atomically.
async fn child_result_survives_restart_and_resumes_parent_once_without_refund() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let mut input = native("parent");
    input.limits.model_requests = 2;
    let parent = submit(&c, "parent-submit", input).await;
    let w = worker("w", "i", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    controlled_native_request(&c, &a, 1, None, None).await;
    let mut child = native("child");
    child.limits.model_requests = 1;
    child.limits.deadline_unix_ms = parent.input.limits.deadline_unix_ms;
    let child = task(
        call(
            &c,
            Operation::DelegateTask {
                operation_id: "delegate".into(),
                parent_task_id: parent.id.clone(),
                parent_attempt_id: a.id.clone(),
                generation: a.generation,
                child,
            },
        )
        .await,
    );
    let before_stop = commands(&c, &w, vec![]).await;
    assert!(
        !before_stop.iter().any(
            |x| matches!(x,WorkerCommand::RunAttempt{attempt,..} if attempt.task_id==child.id)
        )
    );
    task(report(&c, &a, "parent-quiescent", Observation::ProcessTreeStopped).await);
    let child_attempt = run(&commands(&c, &w, vec![]).await);
    assert_eq!(child_attempt.task_id, child.id);
    let complete = finish_native(&c, &child_attempt, "child").await;
    let ready = get(&c, &parent.id).await;
    assert_eq!(ready.status, TaskStatus::Queued);
    assert!(ready.waiting_child_id.is_none());
    let settled = ready.completed_child.as_ref().unwrap();
    assert_eq!(settled.task_id, child.id);
    assert_eq!(settled.result, complete.result);
    task(
        report(
            &c,
            &a,
            "late-parent-outcome",
            Observation::ExecutorOutcome {
                success: true,
                summary: "old execution".into(),
            },
        )
        .await,
    );
    assert_eq!(
        code(
            report(
                &c,
                &a,
                "late-parent-result",
                Observation::ResultCandidate {
                    checks_passed: true,
                    summary: "must not finish continuation".into(),
                    checkpoint: None,
                    validation: vec![],
                    output: Some(json!({"answer":99}))
                }
            )
            .await
        ),
        "invalid_transition"
    );
    assert_eq!(get(&c, &parent.id).await.status, TaskStatus::Queued);
    drop(c);
    let c = Controller::open_with_config(&p, config()).unwrap();
    let next = commands(&c, &w, vec![]).await;
    let parent_attempt = run(&next);
    assert_eq!(parent_attempt.task_id, parent.id);
    assert_eq!(parent_attempt.generation, a.generation + 1);
    match &next[0] {
        WorkerCommand::RunAttempt {
            task, continuation, ..
        } => {
            assert_eq!(task.limits.model_requests, 0);
            assert_eq!(continuation.as_ref().unwrap(), settled);
        }
        _ => panic!("missing continuation"),
    }
    assert_eq!(commands(&c, &w, vec![]).await, next);
    let original = report(
        &c,
        &child_attempt,
        "child-result",
        Observation::ResultCandidate {
            checks_passed: true,
            summary: "accepted structured answer".into(),
            checkpoint: None,
            validation: vec![borg_runtime::native_loop_contract::evidence(
                &child_attempt.id,
            )],
            output: Some(json!({"answer":42})),
        },
    )
    .await;
    assert_eq!(task(original), complete);
    assert_eq!(
        get(&c, &parent.id).await.attempt.unwrap().id,
        parent_attempt.id
    );
}

#[tokio::test]
//= spec/impl/v2/implementation.md#interrupted-work-is-reconciled
//# An external effect with an uncertain outcome MUST be reconciled before the platform retries that effect.
async fn uncertain_effect_blocks_retry_until_exact_settlement_and_actual_stop() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let t = submit(&c, "s", coding("a")).await;
    let w = worker("w", "i", 0, 1);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let cp = Checkpoint {
        registration: fixture_repository_binding(),
        repository: t
            .input
            .repository
            .as_ref()
            .unwrap()
            .checkpoint_remote
            .clone(),
        commit: "b".repeat(40),
        remote_ref: checkpoint_ref(&t.id, &a.id, "effect"),
        operation_id: "effect".into(),
    };
    assert!(matches!(call(&c,Operation::PrepareOperation{operation_id:"effect".into(),task_id:t.id.clone(),attempt_id:a.id.clone(),generation:a.generation,kind:"checkpoint".into(),intended:json!({"registration":cp.registration,"repository":cp.repository,"commit":cp.commit,"remote_ref":cp.remote_ref})}).await,Outcome::Ok{..}));
    let current = get(&c, &t.id).await;
    task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "cancel".into(),
                task_id: t.id.clone(),
                expected_revision: current.revision,
            },
        )
        .await,
    );
    let stopped = task(report(&c, &a, "stopped", Observation::ProcessTreeStopped).await);
    assert!(stopped.attempt.as_ref().unwrap().uncertain);
    assert_eq!(
        code(
            call(
                &c,
                Operation::RetryTask {
                    operation_id: "blocked-retry".into(),
                    task_id: t.id.clone(),
                    expected_revision: stopped.revision
                }
            )
            .await
        ),
        "reconciliation_required"
    );
    let settle = Operation::SettleOperation {
        settlement_id: "controller-settlement-1".into(),
        operation_id: "effect".into(),
        task_id: t.id.clone(),
        attempt_id: a.id.clone(),
        generation: a.generation,
        outcome: json!({"acknowledged":true}),
        checkpoint: Some(cp),
    };
    assert!(matches!(call(&c, settle).await, Outcome::Ok { .. }));
    let known = get(&c, &t.id).await;
    assert!(!known.attempt.as_ref().unwrap().uncertain);
    assert_eq!(
        task(
            call(
                &c,
                Operation::RetryTask {
                    operation_id: "retry".into(),
                    task_id: t.id,
                    expected_revision: known.revision
                }
            )
            .await
        )
        .status,
        TaskStatus::Queued
    );
}

#[tokio::test]
//= spec/impl/v2/implementation.md#interrupted-work-is-reconciled
//# An external effect with an uncertain outcome MUST be reconciled before the platform retries that effect.
async fn process_stop_does_not_resolve_unobserved_external_execution() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let t = submit(&c, "s", native("a")).await;
    let w = worker("w", "i", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    task(
        report(
            &c,
            &a,
            "unknown",
            Observation::ExecutionUncertain {
                message: "remote start outcome cannot be observed".into(),
            },
        )
        .await,
    );
    let stopped = task(report(&c, &a, "stop", Observation::ProcessTreeStopped).await);
    assert!(stopped.attempt.as_ref().unwrap().uncertain);
    assert_eq!(
        code(
            call(
                &c,
                Operation::RetryTask {
                    operation_id: "retry".into(),
                    task_id: t.id,
                    expected_revision: stopped.revision
                }
            )
            .await
        ),
        "reconciliation_required"
    );
}

#[tokio::test]
async fn acceptance_and_current_memory_reject_stale_writers() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let t = submit(&c, "s", native("a")).await;
    let w = worker("w", "i", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let write = |id: &str, revision, value| Operation::SetMemory {
        operation_id: id.into(),
        task_id: t.id.clone(),
        attempt_id: a.id.clone(),
        generation: a.generation,
        key: "answer".into(),
        expected_revision: revision,
        value,
    };
    assert!(matches!(
        call(&c, write("m1", None, json!(1))).await,
        Outcome::Ok { .. }
    ));
    assert_eq!(
        code(call(&c, write("wrong-revision", None, json!(2))).await),
        "stale_revision"
    );
    assert!(matches!(
        call(&c, write("m2", Some(1), json!(2))).await,
        Outcome::Ok { .. }
    ));
    let before = get(&c, &t.id).await;
    let accepted = finish_native(&c, &a, "finish").await;
    // Cancellation racing after acceptance returns the immutable terminal result,
    // even when it carries the pre-completion revision.
    assert_eq!(
        task(
            call(
                &c,
                Operation::CancelTask {
                    operation_id: "late".into(),
                    task_id: t.id,
                    expected_revision: before.revision
                }
            )
            .await
        ),
        accepted
    );
}

#[tokio::test]
//= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
//# A cancellation acknowledgment MUST distinguish requested cancellation from verified execution termination.
// Traceability scope: No new repair policy for already terminal roots; their existing no-op contract remains explicit.
//= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
//= type=test
//# A cancellation request MUST commit cancellation intent and its stop command atomically.
async fn receipt_exhaustion_reserves_cancellation_and_actual_cleanup() {
    let cfg = with_controller_limits(ControllerConfig {
        ..config()
    }, |limits| { limits.max_receipts = 2; limits.max_cleanup_receipts = 2; });
    let c = Controller::open_with_config(path(), cfg).unwrap();
    let w = worker("w", "i", 1, 0);
    register(&c, &w).await;
    let t = submit(&c, "only-submit", native("a")).await;
    assert!(commands(&c, &w, vec![]).await.is_empty());
    assert_eq!(
        get(&c, &t.id).await.blocked_reason.as_deref(),
        Some("receipt_capacity")
    );
    let cancelled = task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "reserved-cancel".into(),
                task_id: t.id,
                expected_revision: t.revision,
            },
        )
        .await,
    );
    assert_eq!(cancelled.status, TaskStatus::Cancelled);

    let cfg = with_controller_limits(ControllerConfig { // Five terminal + three command-control slots.

        ..config()
    }, |limits| { limits.max_receipts = 4; limits.max_cleanup_receipts = 8; });
    let c = Controller::open_with_config(path(), cfg).unwrap();
    let t = submit(&c, "submit", native("a")).await;
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let running = task(
        report(
            &c,
            &a,
            "started",
            Observation::Started {
                thread_id: None,
                turn_id: None,
            },
        )
        .await,
    );
    assert_eq!(
        code(
            call(
                &c,
                Operation::SubmitTask {
                    operation_id: "overflow".into(),
                    input: native("other")
                }
            )
            .await
        ),
        "capacity"
    );
    let cancel = Operation::CancelTask {
        operation_id: "cancel".into(),
        task_id: t.id,
        expected_revision: running.revision,
    };
    let cancelled = task(call(&c, cancel.clone()).await);
    assert_eq!(cancelled.status, TaskStatus::Cancelled);
    assert_eq!(
        code(
            report(
                &c,
                &a,
                "unreserved-progress",
                Observation::Progress {
                    text: "late".into(),
                    output: String::new(),
                    truncated: false
                }
            )
            .await
        ),
        "capacity"
    );
    let stopped = task(report(&c, &a, "stopped", Observation::ProcessTreeStopped).await);
    assert!(stopped.attempt.as_ref().unwrap().stopped);
    assert_eq!(task(call(&c, cancel).await), cancelled);
    assert_eq!(
        task(report(&c, &a, "stopped", Observation::ProcessTreeStopped).await),
        stopped
    );
}

#[tokio::test]
// Traceability scope: Diagnostic snapshot is not a capacity reservation or connectivity proof; an eligible task can remain queued awaiting a worker command read.
//= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
//= type=test
//# A rejected or deferred admission MUST expose its reason.
async fn insufficient_cleanup_reservation_prevents_assignment() {
    let cfg = with_controller_limits(ControllerConfig {
        ..config()
    }, |limits| { limits.max_cleanup_receipts = 4; });
    let c = Controller::open_with_config(path(), cfg).unwrap();
    let t = submit(&c, "s", native("a")).await;
    let w = worker("w", "i", 1, 0);
    register(&c, &w).await;
    assert!(commands(&c, &w, vec![]).await.is_empty());
    let waiting = get(&c, &t.id).await;
    assert!(waiting.attempt.is_none());
    assert_eq!(waiting.blocked_reason.as_deref(), Some("cleanup_capacity"));
    assert_eq!(
        task(
            call(
                &c,
                Operation::CancelTask {
                    operation_id: "cancel".into(),
                    task_id: t.id,
                    expected_revision: waiting.revision
                }
            )
            .await
        )
        .status,
        TaskStatus::Cancelled
    );
}

#[tokio::test]
//= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
//# An automatic retry MUST wait until relevant uncertain external effects are reconciled.
async fn child_uncertainty_cannot_start_parent_on_another_free_worker() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let parent = submit(&c, "parent", native("p")).await;
    let w = worker("w", "i", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let mut input = native("child");
    input.limits.model_requests = 1;
    input.limits.deadline_unix_ms = parent.input.limits.deadline_unix_ms;
    let child = task(
        call(
            &c,
            Operation::DelegateTask {
                operation_id: "delegate".into(),
                parent_task_id: parent.id.clone(),
                parent_attempt_id: a.id.clone(),
                generation: a.generation,
                child: input,
            },
        )
        .await,
    );
    task(report(&c, &a, "parent-stopped", Observation::ProcessTreeStopped).await);
    let ca = run(&commands(&c, &w, vec![]).await);
    assert_eq!(ca.task_id, child.id);
    task(
        report(
            &c,
            &ca,
            "unknown",
            Observation::ExecutionUncertain {
                message: "unobserved external outcome".into(),
            },
        )
        .await,
    );
    let spare = worker("spare", "i", 1, 0);
    register(&c, &spare).await;
    assert!(commands(&c, &spare, vec![]).await.is_empty());
    let blocked = get(&c, &parent.id).await;
    assert_eq!(
        blocked.blocked_reason.as_deref(),
        Some("child_reconciliation_required")
    );
    assert_eq!(blocked.attempt.unwrap().id, a.id);
    task(report(&c, &ca, "child-stopped", Observation::ProcessTreeStopped).await);
    assert!(commands(&c, &spare, vec![]).await.is_empty());
    assert_eq!(get(&c, &parent.id).await.status, TaskStatus::Queued);
}

#[tokio::test]
async fn retired_worker_incarnation_cannot_register_again_after_restart() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let a = worker("w", "a", 1, 0);
    let b = worker("w", "b", 1, 0);
    register(&c, &a).await;
    register(&c, &b).await;
    assert_eq!(
        code(
            call(
                &c,
                Operation::RegisterWorker {
                    operation_id: "retired-a-before-restart".into(),
                    worker: a.clone()
                }
            )
            .await
        ),
        "stale_incarnation"
    );
    drop(c);
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(
        code(
            call(
                &c,
                Operation::RegisterWorker {
                    operation_id: "retired-a-after-restart".into(),
                    worker: a
                }
            )
            .await
        ),
        "stale_incarnation"
    );
    register(&c, &b).await;
    let t = submit(&c, "s", native("agent")).await;
    let attempt = run(&commands(&c, &b, vec![]).await);
    assert_eq!(attempt.task_id, t.id);
    assert_eq!(attempt.worker_incarnation, "b");
}

#[tokio::test]
async fn oversized_child_handoff_refuses_without_stranding_parent_assignment() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let mut input = native("parent");
    input.instruction = "i".repeat(240_000);
    let parent = submit(&c, "parent", input).await;
    let w = worker("w", "i", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let mut input = native("child");
    input.output_schema = Some(json!({"type":"string"}));
    input.limits.output_bytes = metadata().transport.body_bytes / 8;
    input.limits.model_requests = 1;
    input.limits.deadline_unix_ms = parent.input.limits.deadline_unix_ms;
    let child = task(
        call(
            &c,
            Operation::DelegateTask {
                operation_id: "delegate".into(),
                parent_task_id: parent.id.clone(),
                parent_attempt_id: a.id.clone(),
                generation: a.generation,
                child: input,
            },
        )
        .await,
    );
    task(report(&c, &a, "parent-stop", Observation::ProcessTreeStopped).await);
    let ca = run(&commands(&c, &w, vec![]).await);
    controlled_native_request(&c, &ca, 1, None, None).await;
    complete_native_loop(&c, &ca).await;
    task(
        report(
            &c,
            &ca,
            "child-outcome",
            Observation::ExecutorOutcome {
                success: true,
                summary: "done".into(),
            },
        )
        .await,
    );
    task(report(&c, &ca, "child-stop", Observation::ProcessTreeStopped).await);
    let output = json!("o".repeat(100_000));
    let oversized = Observation::ResultCandidate {
        checks_passed: true,
        summary: "s".repeat(metadata().transport.body_bytes / 8),
        checkpoint: None,
        validation: vec!["v".repeat(4000); 128],
        output: Some(output.clone()),
    };
    assert_eq!(
        code(report(&c, &ca, "too-big", oversized).await),
        "capacity"
    );
    assert_eq!(get(&c, &child.id).await.status, TaskStatus::Active);
    let waiting = get(&c, &parent.id).await;
    assert_eq!(waiting.waiting_child_id, Some(child.id));
    assert!(waiting.completed_child.is_none());
    assert_eq!(waiting.attempt.unwrap().id, a.id);
    // The output is unchanged. Native tasks have no shell validation commands;
    // a smaller diagnostic envelope can be accepted without another model call.
    task(
        report(
            &c,
            &ca,
            "bounded",
            Observation::ResultCandidate {
                checks_passed: true,
                summary: "done".into(),
                checkpoint: None,
                validation: vec![borg_runtime::native_loop_contract::evidence(&ca.id)],
                output: Some(output.clone()),
            },
        )
        .await,
    );
    let resumed = commands(&c, &w, vec![]).await;
    match &resumed[0] {
        WorkerCommand::RunAttempt {
            attempt,
            continuation,
            ..
        } => {
            assert_eq!(attempt.task_id, parent.id);
            assert_eq!(
                continuation
                    .as_ref()
                    .unwrap()
                    .result
                    .as_ref()
                    .unwrap()
                    .output,
                Some(output)
            );
        }
        _ => panic!("expected bounded continuation"),
    }
}

#[tokio::test]
async fn oversized_result_refusal_does_not_withhold_an_unrelated_stop_command() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let mut input = native("parent");
    input.instruction = "i".repeat(240_000);
    let parent = submit(&c, "p", input).await;
    let other = submit(&c, "other", native("other")).await;
    let w = worker("w", "i", 2, 0);
    register(&c, &w).await;
    let initial = commands(&c, &w, vec![]).await;
    let find = |items: &[WorkerCommand], id: &str| {
        items
            .iter()
            .find_map(|x| match x {
                WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == id => {
                    Some(attempt.clone())
                }
                _ => None,
            })
            .unwrap()
    };
    let a = find(&initial, &parent.id);
    let other_a = find(&initial, &other.id);
    let mut input = native("child");
    input.output_schema = Some(json!({"type":"string"}));
    input.limits.output_bytes = metadata().transport.body_bytes / 8;
    input.limits.model_requests = 1;
    input.limits.deadline_unix_ms = parent.input.limits.deadline_unix_ms;
    let child = task(
        call(
            &c,
            Operation::DelegateTask {
                operation_id: "delegate".into(),
                parent_task_id: parent.id.clone(),
                parent_attempt_id: a.id.clone(),
                generation: a.generation,
                child: input,
            },
        )
        .await,
    );
    task(report(&c, &a, "parent-stop", Observation::ProcessTreeStopped).await);
    let ca = find(&commands(&c, &w, vec![]).await, &child.id);
    controlled_native_request(&c, &ca, 1, None, None).await;
    task(
        report(
            &c,
            &ca,
            "outcome",
            Observation::ExecutorOutcome {
                success: true,
                summary: "done".into(),
            },
        )
        .await,
    );
    task(report(&c, &ca, "child-stop", Observation::ProcessTreeStopped).await);
    let refused = code(
        report(
            &c,
            &ca,
            "large-child",
            Observation::ResultCandidate {
                checks_passed: true,
                summary: "s".repeat(80_000),
                checkpoint: None,
                validation: vec!["v".repeat(4000); 128],
                output: Some(json!("o".repeat(128_000))),
            },
        )
        .await,
    );
    assert_eq!(refused, "capacity");
    assert_eq!(get(&c, &child.id).await.status, TaskStatus::Active);
    let ready = get(&c, &parent.id).await;
    assert_eq!(ready.waiting_child_id, Some(child.id));
    let before = get(&c, &other.id).await;
    task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "cancel-other".into(),
                task_id: other.id,
                expected_revision: before.revision,
            },
        )
        .await,
    );
    let delivered = commands(&c, &w, vec![]).await;
    assert!(
        delivered.iter().any(
            |x| matches!(x,WorkerCommand::StopAttempt{attempt_id,..} if attempt_id==&other_a.id)
        )
    );
    assert!(
        !delivered.iter().any(
            |x| matches!(x,WorkerCommand::RunAttempt{attempt,..} if attempt.task_id==parent.id)
        )
    );
    let blocked = get(&c, &parent.id).await;
    assert_eq!(blocked.blocked_reason.as_deref(), Some("waiting_child"));
    assert_eq!(blocked.attempt.unwrap().id, a.id);
}

fn checkpoint_ref(task: &str, attempt: &str, operation: &str) -> String {
    let component = borg_runtime::worker::process::component;
    format!(
        "refs/heads/borg/tasks/{}/{}/{}",
        component(task),
        component(attempt),
        component(operation)
    )
}

#[cfg(debug_assertions)]
#[tokio::test]
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//# An exact retry of a committed mutating request MUST return its recorded outcome.
async fn native_validation_releases_owner_and_ignores_unrelated_progress_revision() {
    let now = future() - 600_000;
    let clock = Arc::new(AtomicU64::new(now));
    let observed = clock.clone();
    let c = Controller::open_with_config(
        path(),
        ControllerConfig {
            clock: Arc::new(move || observed.load(Ordering::SeqCst)),
            ..config()
        },
    )
    .unwrap();
    let t = submit(&c, "validation-task", native("validation-agent")).await;
    let w = worker("validation-worker", "inc-1", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    controlled_native_request(&c, &a, 1, None, None).await;
    complete_native_loop(&c, &a).await;
    task(
        report(
            &c,
            &a,
            "validation-executor",
            Observation::ExecutorOutcome {
                success: true,
                summary: "done".into(),
            },
        )
        .await,
    );
    task(
        report(
            &c,
            &a,
            "current-profile-stopped",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    let candidate = Observation::ResultCandidate {
        checks_passed: true,
        summary: "validated".into(),
        checkpoint: None,
        validation: vec![borg_runtime::native_loop_contract::evidence(&a.id)],
        output: Some(json!({"answer":42})),
    };
    let (ready, release) = c.qualification_pause_next_finalization();
    let running = {
        let (c, a, candidate) = (c.clone(), a.clone(), candidate.clone());
        tokio::spawn(async move { report(&c, &a, "validation-candidate", candidate).await })
    };
    tokio::time::timeout(Duration::from_secs(5), ready)
        .await
        .unwrap()
        .unwrap();
    let progressed = tokio::time::timeout(
        Duration::from_secs(1),
        report(
            &c,
            &a,
            "validation-progress",
            Observation::Progress {
                text: "still current".into(),
                output: String::new(),
                truncated: false,
            },
        ),
    )
    .await
    .expect("DB owner was held during validation");
    let revision = task(progressed).revision;
    release.send(()).unwrap();
    let accepted = task(running.await.unwrap());
    assert_eq!(accepted.status, TaskStatus::Succeeded);
    assert!(accepted.revision > revision);
    clock.store(t.input.limits.deadline_unix_ms + 1, Ordering::SeqCst);
    assert_eq!(
        task(report(&c, &a, "validation-candidate", candidate).await),
        accepted,
        "exact receipt replays after deadline"
    );
    assert_eq!(
        code(
            report(
                &c,
                &a,
                "validation-candidate",
                Observation::ResultCandidate {
                    checks_passed: true,
                    summary: "changed".into(),
                    checkpoint: None,
                    validation: vec![borg_runtime::native_loop_contract::evidence(&a.id)],
                    output: Some(json!({"answer":0}))
                }
            )
            .await
        ),
        "operation_conflict"
    );
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn native_validation_finalization_rechecks_cancel_deadline_and_new_attempt() {
    for mode in ["cancel", "deadline", "new-attempt"] {
        let clock = Arc::new(AtomicU64::new(future() - 600_000));
        let observed = clock.clone();
        let c = Controller::open_with_config(
            path(),
            ControllerConfig {
                clock: Arc::new(move || observed.load(Ordering::SeqCst)),
                ..config()
            },
        )
        .unwrap();
        let t = submit(&c, "task", native("a")).await;
        let w = worker("w", "inc", 1, 0);
        register(&c, &w).await;
        let a = run(&commands(&c, &w, vec![]).await);
        controlled_native_request(&c, &a, 1, None, None).await;
        complete_native_loop(&c, &a).await;
        task(
            report(
                &c,
                &a,
                "outcome",
                Observation::ExecutorOutcome {
                    success: true,
                    summary: "done".into(),
                },
            )
            .await,
        );
        task(
            report(
                &c,
                &a,
                "current-profile-stopped",
                Observation::ProcessTreeStopped,
            )
            .await,
        );
        let (ready, release) = c.qualification_pause_next_finalization();
        let running = {
            let (c, a) = (c.clone(), a.clone());
            tokio::spawn(async move {
                report(
                    &c,
                    &a,
                    "pending-result",
                    Observation::ResultCandidate {
                        checks_passed: true,
                        summary: "candidate".into(),
                        checkpoint: None,
                        validation: vec![borg_runtime::native_loop_contract::evidence(&a.id)],
                        output: Some(json!({"answer":7})),
                    },
                )
                .await
            })
        };
        tokio::time::timeout(Duration::from_secs(5), ready)
            .await
            .unwrap()
            .unwrap();
        if mode == "deadline" {
            clock.store(t.input.limits.deadline_unix_ms + 1, Ordering::SeqCst);
        } else {
            let current = get(&c, &t.id).await;
            task(
                call(
                    &c,
                    Operation::CancelTask {
                        operation_id: "cancel".into(),
                        task_id: t.id.clone(),
                        expected_revision: current.revision,
                    },
                )
                .await,
            );
            if mode == "new-attempt" {
                let stopped =
                    task(report(&c, &a, "stopped", Observation::ProcessTreeStopped).await);
                task(
                    call(
                        &c,
                        Operation::RetryTask {
                            operation_id: "retry".into(),
                            task_id: t.id.clone(),
                            expected_revision: stopped.revision,
                        },
                    )
                    .await,
                );
                let next = run(&commands(&c, &w, vec![]).await);
                assert!(next.generation > a.generation);
            }
        }
        release.send(()).unwrap();
        let result = code(running.await.unwrap());
        assert_eq!(
            result,
            if mode == "new-attempt" {
                "stale_generation"
            } else {
                "invalid_transition"
            },
            "{mode}"
        );
        let current = get(&c, &t.id).await;
        assert!(current.result.is_none());
        if mode == "deadline" {
            assert_eq!(current.status, TaskStatus::Failed);
            assert_eq!(
                current.blocked_reason.as_deref(),
                Some("execution_deadline")
            );
        }
    }
}

#[cfg(debug_assertions)]
#[tokio::test]
// Traceability scope: Owned transaction/controlled race coverage, not an exhaustive arbitrary SQLite I/O fault matrix.
//= spec/impl/v2/implementation.md#delegation-is-a-task-handoff
//= type=test
//# A delegation MUST commit its child task and parent-wait association atomically.
async fn native_schema_preflight_rechecks_delegation_and_replays_refusals() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let parent = submit(&c, "parent", native("parent")).await;
    let w = worker("w", "inc", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let mut child = native("child");
    child.limits.deadline_unix_ms = parent.input.limits.deadline_unix_ms;
    child.limits.model_requests = 1;
    let (ready, release) = c.qualification_pause_next_finalization();
    let running = {
        let c = c.clone();
        let id = parent.id.clone();
        let a = a.clone();
        tokio::spawn(async move {
            call(
                &c,
                Operation::DelegateTask {
                    operation_id: "delegate".into(),
                    parent_task_id: id,
                    parent_attempt_id: a.id,
                    generation: a.generation,
                    child,
                },
            )
            .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), ready)
        .await
        .unwrap()
        .unwrap();
    let current = get(&c, &parent.id).await;
    task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "cancel-parent".into(),
                task_id: parent.id.clone(),
                expected_revision: current.revision,
            },
        )
        .await,
    );
    release.send(()).unwrap();
    assert_eq!(code(running.await.unwrap()), "invalid_transition");
    assert!(get(&c, &parent.id).await.waiting_child_id.is_none());
    let mut unsupported = native("bad");
    unsupported.output_schema = Some(json!({"type":"definitely-not-a-json-type"}));
    let op = Operation::SubmitTask {
        operation_id: "bad-schema".into(),
        input: unsupported,
    };
    let first = call(&c, op.clone()).await;
    assert_eq!(code(first.clone()), "invalid_input");
    assert_eq!(call(&c, op).await, first);
    let (ready, release) = c.qualification_pause_next_finalization();
    let op = Operation::SubmitTask {
        operation_id: "concurrent-same-submit".into(),
        input: native("same"),
    };
    let running = {
        let (c, op) = (c.clone(), op.clone());
        tokio::spawn(async move { call(&c, op).await })
    };
    tokio::time::timeout(Duration::from_secs(5), ready)
        .await
        .unwrap()
        .unwrap();
    let winner = call(&c, op).await;
    assert!(matches!(winner, Outcome::Ok { .. }));
    release.send(()).unwrap();
    assert_eq!(running.await.unwrap(), winner);
}

#[cfg(debug_assertions)]
#[tokio::test]
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//# A controller transaction MUST NOT remain open across worker, provider or repository I/O.
async fn validation_helper_failure_drop_and_timeout_do_not_hold_sql_or_accept() {
    let c = Controller::open_with_config(
        path(),
        ControllerConfig {
            validation_executable: PathBuf::from("/usr/bin/false"),
            ..config()
        },
    )
    .unwrap();
    let op = Operation::SubmitTask {
        operation_id: "bad-helper".into(),
        input: native("bad-helper"),
    };
    let refused = call(&c, op.clone()).await;
    assert_eq!(code(refused.clone()), "validation_unavailable");
    assert_eq!(call(&c, op).await, refused);
    assert_eq!(
        submit(&c, "coding-bypass", coding("coding-bypass"))
            .await
            .status,
        TaskStatus::Queued
    );
    for mode in ["drop", "timeout"] {
        // This is the same compiled helper binary, at a distinct trusted path so
        // the explicit debug probe cannot pause another test's validator.
        let root = path().parent().unwrap().to_path_buf();
        let program = root.join("validator");
        std::fs::copy(env!("CARGO_BIN_EXE_borg-runtime"), &program).unwrap();
        let clock = Arc::new(AtomicU64::new(1000));
        let observed = clock.clone();
        let c = Controller::open_with_config(
            root.join("db"),
            ControllerConfig {
                validation_executable: program.clone(),
                clock: Arc::new(move || observed.load(Ordering::SeqCst)),
                ..config()
            },
        )
        .unwrap();
        let mut input = native(mode);
        input.limits.deadline_unix_ms = if mode == "timeout" { 1200 } else { 6000 };
        let op = Operation::SubmitTask {
            operation_id: "helper-operation".into(),
            input,
        };
        let ready = borg_runtime::native::qualification_hold_next_validation(&program);
        let running = {
            let (c, op) = (c.clone(), op.clone());
            tokio::spawn(async move { call(&c, op).await })
        };
        let pid = tokio::time::timeout(Duration::from_secs(3), ready)
            .await
            .unwrap()
            .unwrap();
        assert!(std::path::Path::new(&format!("/proc/{pid}")).exists());
        let list = tokio::time::timeout(
            Duration::from_secs(1),
            call(
                &c,
                Operation::ListTasks {
                    after: None,
                    limit: 10,
                },
            ),
        )
        .await
        .expect("validation held the owner");
        assert!(matches!(list,Outcome::Ok{data:Data::Tasks(ref values)} if values.is_empty()));
        if mode == "drop" {
            running.abort();
            assert!(running.await.unwrap_err().is_cancelled());
        } else {
            assert_eq!(
                code(
                    tokio::time::timeout(Duration::from_secs(3), running)
                        .await
                        .unwrap()
                        .unwrap()
                ),
                "deadline"
            );
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            while std::path::Path::new(&format!("/proc/{pid}")).exists() {
                borg_runtime::native::validation_cleanup_pending();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("actual helper must be reaped");
        if mode == "drop" {
            assert_eq!(
                code(
                    call(
                        &c,
                        Operation::GetOperation {
                            operation_id: "helper-operation".into()
                        }
                    )
                    .await
                ),
                "not_found"
            );
            assert_eq!(task(call(&c, op).await).status, TaskStatus::Queued);
        } else {
            let replay = call(&c, op).await;
            assert_eq!(code(replay), "deadline");
        }
    }
}

// Local controlled reports exercise the controller's existing cumulative API.
// These values are fixture observations, not measured provider usage or a claim
// that unknown Codex inner calls are bounded by an aggregate token budget.
fn stored_attempt_usage(path: &std::path::Path, attempt: &str) -> (i64, Option<i64>, Option<i64>) {
    // Call only after dropping every Controller for this fixture. The production
    // owner holds an exclusive connection; a test must not bypass that owner.
    let db =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    db.busy_timeout(Duration::from_secs(2)).unwrap();
    db.query_row(
        "SELECT model_requests,input_tokens,output_tokens FROM attempts WHERE id=?1",
        [attempt],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .unwrap()
}

fn stored_usage_observations(path: &std::path::Path, task: &str) -> i64 {
    let db =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    db.busy_timeout(Duration::from_secs(2)).unwrap();
    db.query_row(
        "SELECT count(*) FROM history WHERE task_id=?1 AND kind='worker_observation' AND json_extract(body,'$.kind')='usage'",
        [task],
        |row| row.get(0),
    )
    .unwrap()
}

#[tokio::test]
// Traceability scope: Selected usage/result history paths; function name alone in the older general restart test was not used as evidence.
//= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
//= type=test
//# The platform MUST persist task history in its database.
// Traceability scope: Stored cumulative request/token counts survive controller reopen and exact earlier-report replay without reset or double charge.
//= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
//= type=test
//# Reconnecting or resuming an attempt MUST NOT reset its recorded model-request accounting.
async fn cumulative_usage_upgrades_known_counts_and_rejects_downgrades_durably() {
    // Exact controlled request/terminal facts; these are not provider calls.
    // Diagnostic observations add no Usage rows or request/token increments.
    async fn completed(c: &Controller, a: &Attempt, index: u32) {
        let accepted = get(c, &a.task_id).await;
        let outcome = report(c, a,
            &borg_runtime::native_loop_contract::diagnostic_id(&a.id, index),
            Observation::NativeRequestDiagnostic {
                request_index: index,
                endpoint: "https://api.openai.com/v1/responses".into(),
                model: accepted.input.definition.required_inference().unwrap().model.clone(),
                request_binding: Some(borg_runtime::native::ProviderRequestBinding {
                    bytes: 2,
                    sha256: "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a".into(),
                }),
                observed_outcome: borg_runtime::native_loop_contract::NativeRequestObservedOutcome::Completed,
                failure: None,
            }).await;
        assert!(matches!(outcome, Outcome::Ok { data: Data::Acknowledged }), "{outcome:?}");
    }
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let mut input = native("usage-agent");
    input.limits.model_requests = 3;
    let t = submit(&c, "usage-submit", input).await;
    let w = worker("usage-worker", "inc-1", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let reservation_id = borg_runtime::native_loop_contract::reservation_id(&a.id, 1);
    let increased_id = borg_runtime::native_loop_contract::reservation_id(&a.id, 2);
    let reservation = Observation::Usage {
        native_diagnostic_reserved: true,
        model_requests: 1,
        input_tokens: None,
        output_tokens: None,
    };
    let reserved = report(&c, &a, &reservation_id, reservation.clone()).await;
    assert_eq!(task(reserved.clone()).status, TaskStatus::Active);
    task(
        report(
            &c,
            &a,
            "usage-input-known",
            Observation::Usage {
                native_diagnostic_reserved: false,
                model_requests: 1,
                input_tokens: Some(17),
                output_tokens: None,
            },
        )
        .await,
    );
    let known = Observation::Usage {
        native_diagnostic_reserved: false,
        model_requests: 1,
        input_tokens: Some(17),
        output_tokens: Some(9),
    };
    let known_reply = report(&c, &a, "usage-known", known.clone()).await;
    assert_eq!(task(known_reply.clone()).status, TaskStatus::Active);
    completed(&c, &a, 1).await;
    let increased = Observation::Usage {
        native_diagnostic_reserved: true,
        model_requests: 2,
        input_tokens: Some(20),
        output_tokens: Some(12),
    };
    task(report(&c, &a, &increased_id, increased.clone()).await);
    completed(&c, &a, 2).await;
    // A new observation can repeat the same cumulative snapshot. It is another
    // history event, never another increment to the stored request charge.
    let repeated = Observation::Usage {
        native_diagnostic_reserved: false,
        model_requests: 2,
        input_tokens: Some(20),
        output_tokens: Some(12),
    };
    let latest = task(report(&c, &a, "usage-equal-new-id", repeated).await);
    assert_eq!(latest.status, TaskStatus::Active);
    // A delayed exact outbox/report replay returns its historical reply even
    // after counters became known. It must not replay its earlier SQL update.
    assert_eq!(
        report(&c, &a, &reservation_id, reservation).await,
        reserved
    );
    assert_eq!(
        report(&c, &a, "usage-known", known.clone()).await,
        known_reply
    );
    assert_eq!(get(&c, &t.id).await, latest);
    for (id, model_requests, input_tokens, output_tokens) in [
        ("usage-request-decrease", 1, Some(20), Some(12)),
        ("usage-input-unknown", 2, None, Some(12)),
        ("usage-input-decrease", 2, Some(19), Some(12)),
        ("usage-output-unknown", 2, Some(20), None),
        ("usage-output-decrease", 2, Some(20), Some(11)),
        ("usage-storage-overflow", 2, Some(u64::MAX), Some(12)),
    ] {
        assert_eq!(
            code(
                report(
                    &c,
                    &a,
                    id,
                    Observation::Usage {
                        native_diagnostic_reserved: false,
                        model_requests,
                        input_tokens,
                        output_tokens
                    },
                )
                .await,
            ),
            "invalid_input",
            "{id}",
        );
        assert_eq!(get(&c, &t.id).await, latest, "{id} mutated the task");
    }
    assert_eq!(
        code(report(&c, &a, "usage-known", increased).await),
        "operation_conflict",
    );
    assert_eq!(get(&c, &t.id).await, latest);
    drop(c);
    assert_eq!(stored_attempt_usage(&p, &a.id), (2, Some(20), Some(12)));
    assert_eq!(stored_usage_observations(&p, &t.id), 5);

    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(report(&c, &a, "usage-known", known).await, known_reply);
    assert_eq!(get(&c, &t.id).await, latest);
    drop(c);
    assert_eq!(stored_attempt_usage(&p, &a.id), (2, Some(20), Some(12)));
    assert_eq!(stored_usage_observations(&p, &t.id), 5);
}

#[tokio::test]
// Traceability scope: After controller reopen and a new worker incarnation, remaining allowance excludes prior usage; fresh old-generation reports refuse while original receipts replay.
//= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
//= type=test
//# A task mutation MUST verify its current ownership generation.
//= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
//= type=test
//# Reconnecting or resuming an attempt MUST NOT reset its recorded model-request accounting.
async fn failed_output_retains_usage_and_new_attempt_rejects_stale_accounting() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let mut input = native("usage-failure-agent");
    input.limits.model_requests = 2;
    let t = submit(&c, "usage-failure-submit", input).await;
    let w = worker("usage-failure-worker", "inc-1", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    controlled_native_request(&c, &a, 1, None, None).await;
    let known = Observation::Usage {
        native_diagnostic_reserved: false,
        model_requests: 1,
        input_tokens: Some(17),
        output_tokens: Some(9),
    };
    let original_usage_reply = report(&c, &a, "failure-known-usage", known.clone()).await;
    assert_eq!(
        task(original_usage_reply.clone()).status,
        TaskStatus::Active
    );
    complete_native_loop(&c, &a).await;
    task(
        report(
            &c,
            &a,
            "failure-executor-outcome",
            Observation::ExecutorOutcome {
                success: true,
                summary: "completed controlled response".into(),
            },
        )
        .await,
    );
    task(
        report(
            &c,
            &a,
            "current-profile-stopped",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    let before = get(&c, &t.id).await;
    assert_eq!(
        code(
            report(
                &c,
                &a,
                "schema-rejected-output",
                Observation::ResultCandidate {
                    checks_passed: true,
                    summary: "schema-invalid controlled output".into(),
                    checkpoint: None,
                    validation: vec![borg_runtime::native_loop_contract::evidence(&a.id)],
                    output: Some(json!({"answer":"not an integer"})),
                },
            )
            .await,
        ),
        "schema_validation",
    );
    assert_eq!(get(&c, &t.id).await, before);
    drop(c);
    assert_eq!(stored_attempt_usage(&p, &a.id), (1, Some(17), Some(9)));

    let c = Controller::open_with_config(&p, config()).unwrap();
    let failed = task(
        report(
            &c,
            &a,
            "failed-output-terminal",
            Observation::ResultCandidate {
                checks_passed: false,
                summary: "controlled acceptance failure".into(),
                checkpoint: None,
                validation: vec![borg_runtime::native_loop_contract::evidence(&a.id)],
                output: Some(json!({"answer":"not an integer"})),
            },
        )
        .await,
    );
    assert_eq!(failed.status, TaskStatus::Failed);
    assert!(failed.result.is_none());
    let stopped = task(
        report(
            &c,
            &a,
            "failure-process-stopped",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    let w2 = worker(&w.id, "inc-2", 1, 0);
    register(&c, &w2).await;
    task(
        call(
            &c,
            Operation::RetryTask {
                operation_id: "usage-failure-retry".into(),
                task_id: t.id.clone(),
                expected_revision: stopped.revision,
            },
        )
        .await,
    );
    let next = commands(&c, &w2, vec![]).await;
    let a2 = run(&next);
    assert_ne!(a.id, a2.id);
    assert!(a2.generation > a.generation);
    let remaining = next
        .iter()
        .find_map(|command| match command {
            WorkerCommand::RunAttempt { attempt, task, .. } if attempt.id == a2.id => {
                Some(task.limits.model_requests)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(
        remaining, 1,
        "known usage must not spend the reserved request twice"
    );
    let before = get(&c, &t.id).await;
    // Existing settled report is immutable historical data. It is not permission
    // for a newly submitted report under that old attempt/incarnation.
    assert_eq!(
        report(&c, &a, "failure-known-usage", known.clone()).await,
        original_usage_reply
    );
    assert_eq!(
        code(report(&c, &a, "stale-new-usage-report", known.clone()).await),
        "stale_generation"
    );
    let mut wrong_owner = a2.clone();
    wrong_owner.worker_incarnation = "inc-1".into();
    assert_eq!(
        code(report(&c, &wrong_owner, "wrong-incarnation-usage", known).await),
        "stale_incarnation"
    );
    assert_eq!(get(&c, &t.id).await, before);
    controlled_native_request(&c, &a2, 1, None, None).await;
    let reserved = get(&c, &a2.task_id).await;
    assert_eq!(reserved.status, TaskStatus::Active);
    drop(c);
    assert_eq!(stored_attempt_usage(&p, &a.id), (1, Some(17), Some(9)));
    assert_eq!(stored_attempt_usage(&p, &a2.id), (1, None, None));
    assert_eq!(stored_usage_observations(&p, &t.id), 3);
}

fn configured_native(agent: &str) -> TaskInput {
    let mut input = native(agent);
    input.limits.model_requests = 3;
    let mut child = coding(&format!("{agent}-child"));
    child.limits.model_requests = 1;
    child.limits.deadline_unix_ms = input.limits.deadline_unix_ms;
    child.limits.result_bytes = 4096;
    input.native_workflow = Some(Box::new(NativeWorkflow::CodingThenContinue {
        child: CodingTaskTemplate { instruction_source: None,
            pull_request: None,
            instruction_prefix: "Use the configured repository and checks.".into(),
            definition: child.definition,
            repository: child.repository.unwrap(),
            acceptance_commands: child.acceptance_commands,
            dependencies: child.dependencies,
            limits: child.limits,
        },
    }));
    input
}

fn configured_template_mut(input: &mut TaskInput) -> &mut CodingTaskTemplate {
    match input.native_workflow.as_deref_mut().unwrap() {
        NativeWorkflow::CodingThenContinue { child } => child,
        NativeWorkflow::InspectCurrentTaskThenContinue {} | NativeWorkflow::Reassign { .. } => panic!("expected coding workflow"),
    }
}

async fn finish_controlled_coding(c: &Controller, a: &Attempt, label: &str) -> Task {
    // This is a controller fixture's explicitly supplied acknowledged outcome,
    // not execution of repository checks, Git publication, or Codex calls.
    let current = get(c, &a.task_id).await;
    let operation = format!("{label}-checkpoint");
    let checkpoint = Checkpoint {
        registration: fixture_repository_binding(),
        repository: current
            .input
            .repository
            .as_ref()
            .unwrap()
            .checkpoint_remote
            .clone(),
        commit: "b".repeat(40),
        remote_ref: checkpoint_ref(&a.task_id, &a.id, &operation),
        operation_id: operation.clone(),
    };
    assert!(matches!(call(c, Operation::PrepareOperation {
        operation_id: operation.clone(), task_id: a.task_id.clone(), attempt_id: a.id.clone(), generation: a.generation,
        kind: "checkpoint".into(), intended: json!({"registration":checkpoint.registration,"repository":checkpoint.repository,"commit":checkpoint.commit,"remote_ref":checkpoint.remote_ref}),
    }).await, Outcome::Ok { .. }));
    assert!(matches!(
        call(
            c,
            Operation::SettleOperation {
                settlement_id: format!("{label}-settlement"),
                operation_id: operation,
                task_id: a.task_id.clone(),
                attempt_id: a.id.clone(),
                generation: a.generation,
                outcome: json!({"acknowledged":true}),
                checkpoint: Some(checkpoint.clone()),
            }
        )
        .await,
        Outcome::Ok { .. }
    ));
    task(
        report(
            c,
            a,
            &format!("{label}-executor"),
            Observation::ExecutorOutcome {
                success: true,
                summary: "controlled coding outcome".into(),
            },
        )
        .await,
    );
    task(
        report(
            c,
            a,
            &format!("{label}-stopped"),
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    task(
        report(
            c,
            a,
            &format!("{label}-result"),
            Observation::ResultCandidate {
                checks_passed: true,
                summary: "controlled coding result".into(),
                checkpoint: Some(checkpoint),
                validation: vec!["controlled check passed".into()],
                output: None,
            },
        )
        .await,
    )
}

#[tokio::test]
// Traceability scope: Current explicit private schema, no forever-compatible reader claim.
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# The application MUST identify the schema version of its stored task state.
async fn current_schema_reopens_and_old_wire_has_explicit_original_refusal() {
    let p = path();
    let mut encoded = serde_json::to_value(native("schema-defaults")).unwrap();
    encoded["limits"]
        .as_object_mut()
        .unwrap()
        .remove("result_bytes");
    encoded.as_object_mut().unwrap().remove("native_workflow");
    assert!(serde_json::from_value::<TaskInput>(encoded.clone()).is_err(), "missing result policy must not acquire a Rust default");
    encoded["limits"]["result_bytes"]=json!(metadata().task_defaults.result_bytes);
    let input: TaskInput = serde_json::from_value(encoded).unwrap();
    assert_eq!(input.limits.result_bytes, metadata().task_defaults.result_bytes);
    assert!(input.native_workflow.is_none());
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(
        code(
            c.handle(Request {
                protocol_revision: 3,
                request_id: "old-wire".into(),
                command: Operation::SubmitTask {
                    operation_id: "old-submit".into(),
                    input: input.clone()
                }
            })
            .await
            .outcome
        ),
        "unsupported_version"
    );
    assert_eq!(
        code(
            call(
                &c,
                Operation::GetOperation {
                    operation_id: "old-submit".into()
                }
            )
            .await
        ),
        "not_found"
    );
    let accepted = submit(&c, "new-submit", input).await;
    drop(c);
    {
        let db =
            rusqlite::Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        db.busy_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(
            db.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            24
        );
        assert_eq!(
            db.query_row("SELECT count(*) FROM tasks", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
    }
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(get(&c, &accepted.id).await.input, accepted.input);
    // The existing unknown_or_corrupt_schema_is_not_rewritten test now checks
    // original schema1 AND schema2 bytes, as well as future/corrupt sources.
}

#[tokio::test]
async fn full_accepted_result_bound_counts_escaped_fields_at_exact_boundary() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let mut input = native("result-bound");
    input.output_schema = Some(json!({"type":"string"}));
    input.limits.output_bytes = 2048;
    input.limits.result_bytes = 512;
    let t = submit(&c, "bounded-submit", input).await;
    let w = worker("bounded-worker", "inc", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    controlled_native_request(&c, &a, 1, None, None).await;
    complete_native_loop(&c, &a).await;
    task(
        report(
            &c,
            &a,
            "bounded-executor",
            Observation::ExecutorOutcome {
                success: true,
                summary: "controlled".into(),
            },
        )
        .await,
    );
    task(
        report(
            &c,
            &a,
            "current-profile-stopped",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    let mut result = AcceptedResult {
        pull_request: None,
        attempt_id: a.id.clone(),
        summary: "quoted \"\n\t\\".repeat(2),
        checkpoint: None,
        validation: vec!["escaped \"\\\n".into(), "check two".into()],
        output: Some(json!("quoted output \"\n")),
    };
    result
        .validation
        .push(borg_runtime::native_loop_contract::evidence(&a.id));
    let base = serde_json::to_vec(&result).unwrap().len();
    assert!(base < t.input.limits.result_bytes);
    result
        .summary
        .push_str(&"p".repeat(t.input.limits.result_bytes - base));
    assert_eq!(
        serde_json::to_vec(&result).unwrap().len(),
        t.input.limits.result_bytes
    );
    let candidate = |r: &AcceptedResult| Observation::ResultCandidate {
        checks_passed: true,
        summary: r.summary.clone(),
        checkpoint: r.checkpoint.clone(),
        validation: r.validation.clone(),
        output: r.output.clone(),
    };
    let mut too_big = result.clone();
    too_big.summary.push('!');
    assert_eq!(
        serde_json::to_vec(&too_big).unwrap().len(),
        t.input.limits.result_bytes + 1
    );
    assert_eq!(
        code(report(&c, &a, "one-byte-over", candidate(&too_big)).await),
        "capacity"
    );
    assert!(get(&c, &t.id).await.result.is_none());
    let accepted = task(report(&c, &a, "exact-result-bound", candidate(&result)).await);
    assert_eq!(accepted.status, TaskStatus::Succeeded);
    assert_eq!(accepted.result, Some(result));
}

#[tokio::test]
// Traceability scope: The finite selected task definition profile, not dynamic layered policy inheritance.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# An agent definition MUST identify the executor used for its tasks.
async fn configured_workflow_validates_fixed_child_sources_before_native_dispatch() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    register_repository_fixture(&c, &fixture_repository_registration()).await;
    for (label, expected) in [
        ("zero-result", "invalid_input"),
        ("large-result", "invalid_input"),
        ("child-source", "invalid_input"),
        ("child-check", "invalid_input"),
        ("child-prerequisite", "not_found"),
        ("continuation-envelope", "invalid_input"),
        ("own-revision-conflict", "definition_conflict"),
            ] {
        let mut input = configured_native(label);
        match label {
            "zero-result" => input.limits.result_bytes = 0,
            "large-result" => input.limits.result_bytes = metadata().task_defaults.result_bytes + 1,
            "child-source" => {
                configured_template_mut(&mut input).repository.start_commit = "moving-branch".into()
            }
            "child-check" => configured_template_mut(&mut input).acceptance_commands = vec![vec![]],
            "child-prerequisite" => {
                configured_template_mut(&mut input).dependencies = vec!["missing-task".into()]
            }
            "continuation-envelope" => {
                configured_template_mut(&mut input).limits.result_bytes = metadata().task_defaults.result_bytes
            }
            "own-revision-conflict" => {
                let mut definition = input.definition.clone();
                definition.executor = ExecutorKind::Coding;
                configured_template_mut(&mut input).definition = definition;
            }
            _ => unreachable!(),
        }
        assert_eq!(
            code(
                call(
                    &c,
                    Operation::SubmitTask {
                        operation_id: label.into(),
                        input
                    }
                )
                .await
            ),
            expected,
            "{label}"
        );
    }
    assert!(
        matches!(call(&c, Operation::ListTasks { after: None, limit: 20 }).await, Outcome::Ok { data: Data::Tasks(ref tasks) } if tasks.is_empty())
    );
    // The child agent can become bound while its native parent waits in queue.
    // Refuse native admission on that now-incompatible actual source, exposing
    // a reason without consuming an attempt or starving unrelated work.
    let waiting_input = configured_native("queued-source");
    let child_agent = borg_runtime::delegation::template(&waiting_input)
        .unwrap()
        .definition.id
        .clone();
    let waiting = submit(&c, "queued-source-parent", waiting_input).await;
    let claimed = native(&child_agent); // Same immutable revision, conflicting executor.
    let _claim = submit(&c, "claim-child-agent", claimed).await;
    let queued_worker = worker("queue-worker", "inc", 1, 0);
    register(&c, &queued_worker).await;
    assert!(commands(&c, &queued_worker, vec![]).await.iter().all(|cmd| !matches!(cmd,WorkerCommand::RunAttempt{attempt,..} if attempt.task_id==waiting.id)));
    let blocked = get(&c, &waiting.id).await;
    assert!(blocked.attempt.is_none());
    assert_eq!(
        blocked.blocked_reason.as_deref(),
        Some("configured_child_unavailable:definition_conflict")
    );

    // A future configured child must not depend on the actual waiting parent of
    // this newly delegated native task: that wait edge would create a cycle.
    let parent = submit(&c, "outer", native("outer")).await;
    let w = worker("outer-worker", "inc", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let mut no_depth = configured_native("depth-limited-native");
    no_depth.limits.max_delegation_depth = 1;
    no_depth.limits.deadline_unix_ms = parent.input.limits.deadline_unix_ms;
    configured_template_mut(&mut no_depth)
        .limits
        .max_delegation_depth = 1;
    configured_template_mut(&mut no_depth)
        .limits
        .deadline_unix_ms = parent.input.limits.deadline_unix_ms;
    assert_eq!(
        code(
            call(
                &c,
                Operation::DelegateTask {
                    operation_id: "no-coding-depth".into(),
                    parent_task_id: parent.id.clone(),
                    parent_attempt_id: a.id.clone(),
                    generation: a.generation,
                    child: no_depth
                }
            )
            .await
        ),
        "capacity"
    );
    assert!(get(&c, &parent.id).await.waiting_child_id.is_none());
    let mut nested = configured_native("nested");
    nested.limits.deadline_unix_ms = parent.input.limits.deadline_unix_ms;
    configured_template_mut(&mut nested).limits.deadline_unix_ms =
        parent.input.limits.deadline_unix_ms;
    configured_template_mut(&mut nested).dependencies = vec![parent.id.clone()];
    assert_eq!(
        code(
            call(
                &c,
                Operation::DelegateTask {
                    operation_id: "future-cycle".into(),
                    parent_task_id: parent.id.clone(),
                    parent_attempt_id: a.id.clone(),
                    generation: a.generation,
                    child: nested
                }
            )
            .await
        ),
        "dependency_cycle"
    );
    assert!(get(&c, &parent.id).await.waiting_child_id.is_none());
    assert!(
        matches!(call(&c, Operation::ListTasks { after: None, limit: 20 }).await, Outcome::Ok { data: Data::Tasks(ref tasks) } if tasks.len() == 3)
    );
}

#[tokio::test]
// Traceability scope: Configuration-described bounded behavior only, not a guarantee of model quality or a general programmable policy engine.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# An agent's behavior MUST be described by configuration.
// Traceability scope: Controlled reports exercise actual durable reservation and restart: accepted allowance three leaves one continuation request after one parent debit and one child allowance; original deadline remains exact.
//= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
//= type=test
//# Reconnecting or resuming an attempt MUST NOT reset its execution deadline.
//= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
//= type=test
//# Reconnecting or resuming an attempt MUST NOT reset its recorded model-request accounting.
//= spec/impl/v2/implementation.md#delegation-is-a-task-handoff
//= type=test
//# The task's model-request accounting MUST include platform-mediated requests and configured child-task reservations.
async fn configured_child_matches_template_and_continuation_survives_replay_restart() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let parent = submit(
        &c,
        "configured-parent",
        configured_native("configured-parent"),
    )
    .await;
    let w = worker("configured-worker", "inc", 1, 1);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    controlled_native_request(&c, &a, 1, None, None).await;
    let child =
        borg_runtime::delegation::coding_child(&parent.input, "perform the bounded edit").unwrap();
    let delegate = |id: &str, child: TaskInput| Operation::DelegateTask {
        operation_id: id.into(),
        parent_task_id: parent.id.clone(),
        parent_attempt_id: a.id.clone(),
        generation: a.generation,
        child,
    };
    for mode in [
        "repository",
        "agent",
        "definition",
        "checks",
        "limits",
        "prefix",
        "prerequisite",
    ] {
        let mut different = child.clone();
        match mode {
            "repository" => different
                .repository
                .as_mut()
                .unwrap()
                .url
                .push_str("/other"),
            "agent" => different.definition.id.push_str("-other"),
            "definition" => different.definition.revision.push_str("-other"),
            "checks" => different.acceptance_commands.clear(),
            "limits" => different.limits.result_bytes += 1,
            "prefix" => different.instruction = "unconfigured instruction prefix".into(),
            "prerequisite" => different.dependencies = vec![parent.id.clone()],
            _ => unreachable!(),
        }
        assert_eq!(
            code(call(&c, delegate(&format!("mismatch-{mode}"), different)).await),
            "workflow_mismatch",
            "{mode}"
        );
    }
    let original = delegate("configured-delegate", child.clone());
    let created = task(call(&c, original).await);
    assert_eq!(
        task(call(&c, delegate("configured-delegate", child.clone())).await),
        created
    );
    let mut changed = child.clone();
    changed.instruction.push_str(" different");
    assert_eq!(
        code(call(&c, delegate("configured-delegate", changed)).await),
        "operation_conflict"
    );
    task(report(&c, &a, "decision-stopped", Observation::ProcessTreeStopped).await);
    let child_attempt = run(&commands(&c, &w, vec![]).await);
    assert_eq!(child_attempt.task_id, created.id);
    let complete = finish_controlled_coding(&c, &child_attempt, "configured-child").await;
    assert_eq!(complete.status, TaskStatus::Succeeded);
    drop(c);
    let c = Controller::open_with_config(&p, config()).unwrap();
    // Receipt replay wins even though the original parent is no longer in the
    // delegating phase and the workflow's child is already complete.
    assert_eq!(
        task(call(&c, delegate("configured-delegate", child.clone())).await),
        created
    );
    let continuation = commands(&c, &w, vec![]).await;
    let next = run(&continuation);
    let (remaining, resumed_deadline, recorded) = continuation
        .iter()
        .find_map(|command| match command {
            WorkerCommand::RunAttempt {
                attempt,
                task,
                continuation,
                ..
            } if attempt.id == next.id => Some((
                task.limits.model_requests,
                task.limits.deadline_unix_ms,
                continuation.as_ref().unwrap(),
            )),
            _ => None,
        })
        .unwrap();
    assert_eq!(next.task_id, parent.id);
    assert_eq!(remaining, 1);
    assert_eq!(resumed_deadline, parent.input.limits.deadline_unix_ms);
    assert_eq!(recorded.result, complete.result);
    assert_eq!(
        code(
            call(
                &c,
                Operation::DelegateTask {
                    operation_id: "forbidden-second-child".into(),
                    parent_task_id: parent.id.clone(),
                    parent_attempt_id: next.id.clone(),
                    generation: next.generation,
                    child
                }
            )
            .await
        ),
        "invalid_transition"
    );
    task(
        report(
            &c,
            &next,
            "continuation-executor",
            Observation::ExecutorOutcome {
                success: true,
                summary: "controlled continuation".into(),
            },
        )
        .await,
    );
    let candidate = Observation::ResultCandidate {
        checks_passed: true,
        summary: "final controlled result".into(),
        checkpoint: None,
        validation: vec![borg_runtime::native_loop_contract::evidence(&next.id)],
        output: Some(json!({"answer":42})),
    };
    assert_eq!(
        code(report(&c, &next, "missing-continuation-request", candidate.clone()).await),
        "invalid_transition"
    );
    controlled_native_request(&c, &next, 1, None, None).await;
    complete_native_loop(&c, &next).await;
    task(
        report(
            &c,
            &next,
            "continuation-stop",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    let accepted = task(report(&c, &next, "continuation-result", candidate).await);
    assert_eq!(accepted.status, TaskStatus::Succeeded);
    assert_eq!(accepted.input.limits.model_requests, 3);
}

#[tokio::test]
// Traceability scope: Configured child admission refuses insufficient remaining allowance; spending the continuation prevents another retry. Controlled coding reports are not supplied-executor usage proof.
//= spec/impl/v2/implementation.md#delegation-is-a-task-handoff
//= type=test
//# The task's model-request accounting MUST include platform-mediated requests and configured child-task reservations.
async fn configured_delegation_reserves_last_request_and_cancellation_remains_final() {
    for mode in ["no-room", "cancel", "continuation-spent"] {
        let p = path();
        let c = Controller::open_with_config(&p, config()).unwrap();
        let parent = submit(&c, "parent", configured_native(mode)).await;
        let w = worker("w", "inc", 1, 1);
        register(&c, &w).await;
        let a = run(&commands(&c, &w, vec![]).await);
        // Two physical-request observations require two consecutive original charges.
        for index in 1..=if mode == "no-room" { 2 } else { 1 } {
            controlled_native_request(&c, &a, index, None, None).await;
        }
        let child = borg_runtime::delegation::coding_child(&parent.input, "bounded edit").unwrap();
        let op = || Operation::DelegateTask {
            operation_id: "delegate".into(),
            parent_task_id: parent.id.clone(),
            parent_attempt_id: a.id.clone(),
            generation: a.generation,
            child: child.clone(),
        };
        if mode == "no-room" {
            assert_eq!(code(call(&c, op()).await), "capacity");
            assert!(get(&c, &parent.id).await.waiting_child_id.is_none());
            assert!(
                matches!(call(&c, Operation::ListTasks { after: None, limit: 20 }).await, Outcome::Ok { data: Data::Tasks(ref tasks) } if tasks.len() == 1)
            );
            continue;
        }
        let created = task(call(&c, op()).await);
        if mode == "cancel" {
            let before = get(&c, &parent.id).await;
            let cancelled = task(
                call(
                    &c,
                    Operation::CancelTask {
                        operation_id: "cancel".into(),
                        task_id: parent.id.clone(),
                        expected_revision: before.revision,
                    },
                )
                .await,
            );
            assert_eq!(cancelled.status, TaskStatus::Cancelled);
            assert_eq!(get(&c, &created.id).await.status, TaskStatus::Cancelled);
            drop(c);
            let c = Controller::open_with_config(&p, config()).unwrap();
            assert_eq!(task(call(&c, op()).await), created);
            assert_eq!(get(&c, &created.id).await.status, TaskStatus::Cancelled);
            assert_eq!(get(&c, &parent.id).await.status, TaskStatus::Cancelled);
            assert_eq!(
                code(
                    call(
                        &c,
                        Operation::DelegateTask {
                            operation_id: "fresh-after-cancel".into(),
                            parent_task_id: parent.id.clone(),
                            parent_attempt_id: a.id.clone(),
                            generation: a.generation,
                            child
                        }
                    )
                    .await
                ),
                "invalid_transition"
            );
            continue;
        }
        task(report(&c, &a, "stopped", Observation::ProcessTreeStopped).await);
        let ca = run(&commands(&c, &w, vec![]).await);
        finish_controlled_coding(&c, &ca, "child").await;
        let continuation = commands(&c, &w, vec![]).await;
        let a2 = run(&continuation);
        controlled_native_request(&c, &a2, 1, None, None).await;
        task(
            report(
                &c,
                &a2,
                "failed-executor",
                Observation::ExecutorOutcome {
                    success: false,
                    summary: "controlled final request failure".into(),
                },
            )
            .await,
        );
        task(
            report(
                &c,
                &a2,
                "failed-result",
                Observation::ResultCandidate {
                    checks_passed: false,
                    summary: "controlled failure".into(),
                    checkpoint: None,
                    validation: vec![],
                    output: None,
                },
            )
            .await,
        );
        let stopped = task(
            report(
                &c,
                &a2,
                "continuation-stopped",
                Observation::ProcessTreeStopped,
            )
            .await,
        );
        assert!(stopped.completed_child.is_some());
        assert_eq!(
            code(
                call(
                    &c,
                    Operation::RetryTask {
                        operation_id: "no-reset".into(),
                        task_id: parent.id.clone(),
                        expected_revision: stopped.revision
                    }
                )
                .await
            ),
            "capacity"
        );
        assert_eq!(get(&c, &parent.id).await.input.limits.model_requests, 3);
    }
}

fn controller_sidecar(path: &std::path::Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    name.into()
}

async fn controller_owner_released(path: &std::path::Path) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(controller_sidecar(path, ".controller-lock"))
                .unwrap();
            if file.try_lock().is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("actual previous owner must drain and release");
}

fn actual_process_live(pid: u32) -> bool {
    let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return false;
    };
    let state = stat.rsplit_once(") ").unwrap().1.as_bytes()[0];
    state != b'Z' && state != b'X'
}

#[cfg(debug_assertions)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn helper_owner_excludes_second_controller_after_database_error_and_drop() {
    let p = path();
    let program = p.parent().unwrap().join("held-validator");
    std::fs::copy(env!("CARGO_BIN_EXE_borg-runtime"), &program).unwrap();
    let now = Arc::new(AtomicU64::new(1000));
    let clock = now.clone();
    let settings = ControllerConfig {
        validation_executable: program.clone(),
        clock: Arc::new(move || clock.load(Ordering::SeqCst)),
        ..config()
    };
    let c = Controller::open_with_config(&p, settings.clone()).unwrap();
    let mut input = native("held-owner");
    input.limits.deadline_unix_ms = 60_000;
    let ready = borg_runtime::native::qualification_hold_next_validation(&program);
    let running = {
        let c = c.clone();
        tokio::spawn(async move {
            call(
                &c,
                Operation::SubmitTask {
                    operation_id: "held-submit".into(),
                    input,
                },
            )
            .await
        })
    };
    let pid = tokio::time::timeout(Duration::from_secs(3), ready)
        .await
        .unwrap()
        .unwrap();
    assert!(actual_process_live(pid));
    let error = Controller::open_with_config(&p, settings.clone())
        .err()
        .expect("second owner refused");
    assert!(error.contains("owner lock"), "{error}");
    assert!(
        actual_process_live(pid),
        "refused owner must not reconcile active helper"
    );

    // Exercise the real expire error branch: the configured clock cannot be
    // represented in SQLite. The sole DB thread closes while validation lives.
    now.store(u64::MAX, Ordering::SeqCst);
    assert_eq!(
        code(
            call(
                &c,
                Operation::ListTasks {
                    after: None,
                    limit: 1
                }
            )
            .await
        ),
        "store_unavailable"
    );
    now.store(1000, Ordering::SeqCst);
    assert!(Controller::open_with_config(&p, settings.clone()).is_err());
    assert!(actual_process_live(pid));
    drop(c);
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    // No separate validation or explicit cleanup call triggers this. Startup's
    // bounded observation drains only the already-owned native quarantine.
    let reopened = Controller::open_with_config(&p, settings).unwrap();
    assert!(!actual_process_live(pid));
    assert_eq!(
        code(
            call(
                &reopened,
                Operation::GetOperation {
                    operation_id: "held-submit".into()
                }
            )
            .await
        ),
        "not_found"
    );
    assert_eq!(
        std::fs::read_dir(controller_sidecar(&p, ".native-helpers"))
            .unwrap()
            .filter(|e| e
                .as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|s| s == "json"))
            .count(),
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_lock_survives_last_handle_until_queued_work_drains() {
    use std::sync::{Condvar, Mutex, atomic::AtomicBool};
    let p = path();
    let pause = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(AtomicBool::new(false));
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let settings = {
        let (pause, entered, gate) = (pause.clone(), entered.clone(), gate.clone());
        ControllerConfig {
            clock: Arc::new(move || {
                if pause.swap(false, Ordering::SeqCst) {
                    entered.store(true, Ordering::SeqCst);
                    let (mutex, wake) = &*gate;
                    let mut released = mutex.lock().unwrap();
                    while !*released {
                        released = wake.wait(released).unwrap();
                    }
                }
                1000
            }),
            ..config()
        }
    };
    // Always release the real DB thread even if an assertion fails.
    struct Release(Arc<(Mutex<bool>, Condvar)>);
    impl Drop for Release {
        fn drop(&mut self) {
            *self.0.0.lock().unwrap() = true;
            self.0.1.notify_all();
        }
    }
    let release = Release(gate);
    let c = Controller::open_with_config(&p, settings).unwrap();
    pause.store(true, Ordering::SeqCst);
    let running = {
        let c = c.clone();
        tokio::spawn(async move {
            call(
                &c,
                Operation::ListTasks {
                    after: None,
                    limit: 1,
                },
            )
            .await
        })
    };
    tokio::time::timeout(Duration::from_secs(2), async {
        while !entered.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    drop(c);
    assert!(
        Controller::open_with_config(&p, config())
            .err()
            .unwrap()
            .contains("owner lock")
    );
    drop(release);
    // Releasing the queued operation is not completion of SQLite close. Wait
    // for the real lifetime lock before inspecting files that close may unlink.
    controller_owner_released(&p).await;
    let next = Controller::open_with_config(&p, config()).unwrap();
    assert!(
        matches!(call(&next, Operation::ListTasks { after: None, limit: 1 }).await,
        Outcome::Ok { data: Data::Tasks(values) } if values.is_empty())
    );
}

#[tokio::test]
async fn controller_registry_identity_and_aliases_refuse_without_rewriting() {
    use std::os::unix::fs::symlink;
    for defect in [
        "marker",
        "missing",
        "database_id",
        "lock",
        "copy",
        "symlink",
        "hardlink",
    ] {
        let p = path();
        let c = Controller::open_with_config(&p, config()).unwrap();
        drop(c);
        controller_owner_released(&p).await;
        let registry = controller_sidecar(&p, ".native-helpers");
        let marker = registry.join("owner.binding");
        let mut target = p.clone();
        match defect {
            "marker" => std::fs::write(&marker, b"wrong identity").unwrap(),
            "missing" => {
                std::fs::rename(&registry, p.parent().unwrap().join("retained-registry")).unwrap()
            }
            "database_id" => {
                let db = rusqlite::Connection::open(&p).unwrap();
                db.execute("UPDATE settings SET value='00000000000000000000000000000000' WHERE name='database_id'", []).unwrap();
            }
            "lock" => {
                let lock = controller_sidecar(&p, ".controller-lock");
                // Keep the old inode allocated so reuse cannot hide the mismatch.
                std::fs::rename(&lock, p.parent().unwrap().join("retained-lock")).unwrap();
            }
            "copy" => {
                target = p.parent().unwrap().join("copied.db");
                std::fs::copy(&p, &target).unwrap();
            }
            "symlink" => {
                target = p.parent().unwrap().join("alias.db");
                symlink(&p, &target).unwrap();
            }
            "hardlink" => {
                target = p.parent().unwrap().join("alias.db");
                std::fs::hard_link(&p, &target).unwrap();
            }
            _ => unreachable!(),
        }
        let before = std::fs::read(&p).unwrap();
        let target_before = std::fs::read(&target).unwrap();
        let marker_before = std::fs::read(&marker).ok();
        assert!(
            Controller::open_with_config(&target, config()).is_err(),
            "accepted {defect}"
        );
        assert_eq!(std::fs::read(&p).unwrap(), before, "rewrote {defect}");
        assert_eq!(std::fs::read(&target).unwrap(), target_before);
        assert_eq!(std::fs::read(&marker).ok(), marker_before);
        if defect == "missing" {
            assert!(!registry.exists(), "did not recreate missing registry");
        }
    }
    // A normal relative path resolves to the same persisted identity; accepting
    // it does not accept a filesystem alias or require changing process cwd.
    let p = path();
    let cwd = std::env::current_dir().unwrap();
    let mut relative = PathBuf::new();
    for component in cwd.components() {
        if matches!(component, std::path::Component::Normal(_)) {
            relative.push("..");
        }
    }
    relative.push(p.strip_prefix("/").unwrap());
    let c = Controller::open_with_config(&relative, config()).unwrap();
    drop(c);
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert!(
        matches!(call(&c, Operation::ListTasks { after: None, limit: 1 }).await,
        Outcome::Ok { data: Data::Tasks(values) } if values.is_empty())
    );
}

/// A genuine controller process with a real registered helper, intentionally
/// killed by the acceptance test below. It is never an ordinary suite entry.
#[cfg(debug_assertions)]
#[test]
#[ignore = "requires parent-controlled controller crash fixture"]
fn controller_crash_helper_fixture() {
    let root = PathBuf::from(
        std::env::var_os("BORG_CONTROLLER_CRASH_FIXTURE").expect("controlled fixture root"),
    );
    let program = root.join("validator");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let c = Controller::open_with_config(
            root.join("task.db"),
            ControllerConfig {
                validation_executable: program.clone(),
                ..config()
            },
        )
        .unwrap();
        let ready = borg_runtime::native::qualification_hold_next_validation(&program);
        let _running = tokio::spawn(async move {
            call(
                &c,
                Operation::SubmitTask {
                    operation_id: "crashed-submit".into(),
                    input: native("crash-helper"),
                },
            )
            .await
        });
        let pid = ready.await.unwrap();
        let ready_path = root.join("helper.ready");
        std::fs::write(&ready_path, pid.to_string()).unwrap();
        std::fs::File::open(ready_path).unwrap().sync_all().unwrap();
        std::future::pending::<()>().await;
    });
}

#[cfg(debug_assertions)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_restart_reconciles_actual_orphan_helper_before_readiness() {
    let p = path();
    let root = p.parent().unwrap();
    let harness = root.join("controller-tests");
    let program = root.join("validator");
    std::fs::copy(std::env::current_exe().unwrap(), &harness).unwrap();
    std::fs::copy(env!("CARGO_BIN_EXE_borg-runtime"), &program).unwrap();
    let stdout = std::fs::File::create(root.join("fixture.stdout")).unwrap();
    let stderr = std::fs::File::create(root.join("fixture.stderr")).unwrap();
    let child = std::process::Command::new(harness)
        .args([
            "--exact",
            "controller_crash_helper_fixture",
            "--ignored",
            "--nocapture",
        ])
        .env("BORG_CONTROLLER_CRASH_FIXTURE", root)
        .stdin(std::process::Stdio::null())
        .stdout(stdout)
        .stderr(stderr)
        .spawn()
        .unwrap();
    struct CrashChild(std::process::Child, PathBuf);
    impl Drop for CrashChild {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
            // Failure cleanup only, after the actual fixture owner stopped.
            // The normal acceptance checks below precede this guard. Never
            // remove evidence or call registry recovery to make them pass.
            // This also covers a timeout before helper.ready was observed.
            let Ok(entries) = std::fs::read_dir(&self.1) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_none_or(|s| s != "json") {
                    continue;
                }
                let Ok(metadata) = std::fs::symlink_metadata(&path) else {
                    continue;
                };
                if !metadata.is_file() || metadata.len() > 65_536 {
                    continue;
                }
                let Ok(bytes) = std::fs::read(&path) else {
                    continue;
                };
                let Ok(record) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
                    continue;
                };
                let (Some(pid), Some(start), Some(group)) = (
                    record["pid"].as_u64(),
                    record["start_ticks"].as_u64(),
                    record["process_group"].as_u64(),
                ) else {
                    continue;
                };
                if pid <= 1 || pid > u32::MAX as u64 || group != pid {
                    continue;
                }
                let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
                    continue;
                };
                let Some((_, rest)) = stat.rsplit_once(") ") else {
                    continue;
                };
                let fields: Vec<_> = rest.split_whitespace().collect();
                if fields.get(19).and_then(|v| v.parse::<u64>().ok()) != Some(start)
                    || fields.get(2).and_then(|v| v.parse::<u64>().ok()) != Some(group)
                    || fields.first().is_none_or(|s| matches!(*s, "Z" | "X"))
                {
                    continue;
                }
                let _ = std::process::Command::new("/bin/kill")
                    .args(["-KILL", "--", &pid.to_string()])
                    .status();
            }
        }
    }
    let mut child = CrashChild(child, controller_sidecar(&p, ".native-helpers"));
    let pid: u32 = tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            if let Ok(text) = std::fs::read_to_string(root.join("helper.ready")) {
                if let Ok(pid) = text.parse() {
                    break pid;
                }
            }
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "fixture exited before helper READY"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("actual registered helper readiness");
    assert!(actual_process_live(pid));
    let registry = controller_sidecar(&p, ".native-helpers");
    let records = || {
        std::fs::read_dir(&registry)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|s| s == "json")
            })
            .count()
    };
    assert_eq!(records(), 1);
    // SIGKILL is the controlled crash. No cooperative helper cleanup runs.
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    assert!(actual_process_live(pid));
    assert_eq!(records(), 1);
    let c = Controller::open_with_config(
        &p,
        ControllerConfig {
            validation_executable: program,
            ..config()
        },
    )
    .unwrap();
    assert!(
        !actual_process_live(pid),
        "readiness must follow original helper cleanup"
    );
    assert_eq!(records(), 0);
    assert_eq!(
        code(
            call(
                &c,
                Operation::GetOperation {
                    operation_id: "crashed-submit".into()
                }
            )
            .await
        ),
        "not_found"
    );
    assert!(
        matches!(call(&c, Operation::ListTasks { after: None, limit: 1 }).await,
        Outcome::Ok { data: Data::Tasks(values) } if values.is_empty())
    );
}

#[tokio::test]
async fn controller_startup_refuses_partial_or_unreadable_helper_tracking() {
    let p = path();
    let registry = controller_sidecar(&p, ".native-helpers");
    std::fs::create_dir(&registry).unwrap();
    std::fs::write(registry.join("unclaimed-evidence"), b"retain").unwrap();
    assert!(Controller::open_with_config(&p, config()).is_err());
    assert_eq!(
        std::fs::read(registry.join("unclaimed-evidence")).unwrap(),
        b"retain"
    );
    assert!(!registry.join("owner.binding").exists());
    // A partially initialized fresh root is not automatically emptied or
    // adopted on a subsequent attempt either.
    assert!(Controller::open_with_config(&p, config()).is_err());

    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    drop(c);
    controller_owner_released(&p).await;
    let registry = controller_sidecar(&p, ".native-helpers");
    let invalid = registry.join("unknown.json");
    std::fs::write(&invalid, b"unreadable process identity").unwrap();
    let before = std::fs::read(&p).unwrap();
    assert!(Controller::open_with_config(&p, config()).is_err());
    assert_eq!(std::fs::read(&p).unwrap(), before);
    assert_eq!(
        std::fs::read(&invalid).unwrap(),
        b"unreadable process identity"
    );
    // Only the fixture removes its deliberately invalid record. Production
    // refuses it; no repair API or success acknowledgment is manufactured.
    std::fs::remove_file(invalid).unwrap();
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert!(
        matches!(call(&c, Operation::ListTasks { after: None, limit: 1 }).await,
        Outcome::Ok { data: Data::Tasks(values) } if values.is_empty())
    );
}

async fn accepted_release_task(c: &Controller, label: &str) -> Task {
    let t = submit(c, &format!("{label}-submit"), coding(label)).await;
    let w = worker(&format!("{label}-worker"), "release-incarnation", 0, 1);
    register(c, &w).await;
    let a = run(&commands(c, &w, vec![]).await);
    assert_eq!(a.task_id, t.id);
    finish_controlled_coding(c, &a, label).await
}

fn release_input(t: &Task, operation: &str) -> PrepareReleaseInput {
    // Explicit metadata-only fixture. No package bytes or protected checker are
    // supplied; every accepted candidate must remain Claimed/Unconfigured.
    let checkpoint = t.result.as_ref().unwrap().checkpoint.clone().unwrap();
    PrepareReleaseInput {
        operation_id: operation.into(),
        task_id: t.id.clone(),
        expected_task_revision: t.revision,
        artifact: ReleaseArtifact {
            format: "borg-v2-release/1".into(),
            package_sha256: "1".repeat(64),
            package_bytes: 20_480,
            manifest_sha256: "2".repeat(64),
            source_commit: checkpoint.commit.clone(),
            source_tree: "3".repeat(40),
            object_format: "sha1".into(),
            binary_sha256: "4".repeat(64),
            binary_bytes: 1024,
            target: "x86_64-unknown-linux-gnu".into(),
        },
        checkpoint,
    }
}
fn release(o: Outcome) -> ReleaseCandidate {
    match o {
        Outcome::Ok {
            data: Data::Release(r),
        } => *r,
        other => panic!("release expected: {other:?}"),
    }
}
fn assert_unqualified(r: &ReleaseCandidate) {
    assert_eq!(r.qualification.artifact, ArtifactQualification::Claimed);
    assert_eq!(
        r.qualification.repository_submission,
        ProtectedStatus::Unconfigured
    );
    assert_eq!(r.qualification.checks, ProtectedStatus::Unconfigured);
    assert_eq!(r.qualification.human_rollout, ProtectedStatus::Unconfigured);
}

#[tokio::test]
async fn release_preparation_binds_accepted_source_and_replays_after_deadline_restart() {
    let p = path();
    let now = Arc::new(AtomicU64::new(future() - 600_000));
    let clock = now.clone();
    let cfg = with_controller_limits(ControllerConfig {
        clock: Arc::new(move || clock.load(Ordering::SeqCst)),
        ..config()
    }, |limits| { limits.max_release_candidates = 1; });
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let t = accepted_release_task(&c, "release-durable").await;
    now.store(t.input.limits.deadline_unix_ms + 1, Ordering::SeqCst);
    let input = release_input(&t, "release-original");
    let first = call(&c, Operation::PrepareRelease(input.clone())).await;
    let r = release(first.clone());
    assert_unqualified(&r);
    assert_eq!(r.checkpoint, input.checkpoint);
    assert_eq!(r.artifact, input.artifact);
    assert_eq!(r.task_revision, t.revision);
    assert_eq!(r.accepted_attempt_id, t.result.as_ref().unwrap().attempt_id);
    assert_eq!(
        get(&c, &t.id).await,
        t,
        "preparation does not reopen or mutate execution"
    );
    assert_eq!(
        call(&c, Operation::PrepareRelease(input.clone())).await,
        first
    );
    let mut duplicate = input.clone();
    duplicate.operation_id = "release-duplicate".into();
    assert_eq!(
        call(&c, Operation::PrepareRelease(duplicate.clone())).await,
        first
    );
    let mut changed = input.clone();
    changed.artifact.manifest_sha256 = "5".repeat(64);
    assert_eq!(
        code(call(&c, Operation::PrepareRelease(changed)).await),
        "operation_conflict"
    );
    let mut conflicting_claim = duplicate.clone();
    conflicting_claim.operation_id = "release-new-conflict".into();
    conflicting_claim.artifact.binary_sha256 = "6".repeat(64);
    assert_eq!(
        code(call(&c, Operation::PrepareRelease(conflicting_claim)).await),
        "operation_conflict"
    );
    let mut full = input.clone();
    full.operation_id = "release-full".into();
    full.artifact.package_sha256 = "7".repeat(64);
    assert_eq!(
        code(call(&c, Operation::PrepareRelease(full)).await),
        "capacity"
    );
    drop(c);
    controller_owner_released(&p).await;
    {
        let db =
            rusqlite::Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        assert_eq!(
            db.query_row("SELECT count(*) FROM releases", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            db.query_row(
                "SELECT count(*) FROM history WHERE kind='release_prepared'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
        assert_eq!(db.query_row("SELECT count(*) FROM receipts WHERE id IN ('release-original','release-duplicate')", [], |r| r.get::<_,i64>(0)).unwrap(), 2);
    }
    let c = Controller::open_with_config(&p, cfg).unwrap();
    assert_eq!(call(&c, Operation::PrepareRelease(input)).await, first);
    assert_eq!(call(&c, Operation::PrepareRelease(duplicate)).await, first);
    assert_eq!(
        release(
            call(
                &c,
                Operation::GetRelease {
                    release_id: r.id.clone()
                }
            )
            .await
        ),
        r
    );
    assert!(
        matches!(call(&c, Operation::ListReleases { task_id: t.id.clone(), after: None, limit: 1 }).await,
        Outcome::Ok { data: Data::Releases(values) } if values == vec![r.clone()])
    );
    assert!(
        matches!(call(&c, Operation::ListReleases { task_id: t.id, after: Some(r.id), limit: 1 }).await,
        Outcome::Ok { data: Data::Releases(values) } if values.is_empty())
    );
}

#[tokio::test]
async fn release_preparation_refuses_wrong_source_and_bounded_claims() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let t = accepted_release_task(&c, "release-refusals").await;
    let other = accepted_release_task(&c, "release-other").await;
    for mode in [
        "task",
        "repository",
        "commit",
        "checkpoint",
        "stale",
        "digest",
        "manifest",
        "binary",
        "format",
        "object",
        "tree",
        "target",
        "package-size",
        "binary-size",
    ] {
        let mut input = release_input(&t, &format!("refused-{mode}"));
        match mode {
            "task" => {
                input.task_id = other.id.clone();
                input.expected_task_revision = other.revision;
            }
            "repository" => input.checkpoint.repository = "different-repository".into(),
            "commit" => input.artifact.source_commit = "e".repeat(40),
            "checkpoint" => input.checkpoint.operation_id = "not-original".into(),
            "stale" => input.expected_task_revision -= 1,
            "digest" => input.artifact.package_sha256 = "G".repeat(64),
            "manifest" => input.artifact.manifest_sha256.clear(),
            "binary" => input.artifact.binary_sha256 = "f".repeat(63),
            "format" => input.artifact.format = "borg-v2-release/999".into(),
            "object" => input.artifact.object_format = "sha256".into(),
            "tree" => input.artifact.source_tree = "a".repeat(41),
            "target" => input.artifact.target = "deployment-production".into(),
            "package-size" => input.artifact.package_bytes = u64::MAX,
            "binary-size" => input.artifact.binary_bytes = input.artifact.package_bytes,
            _ => unreachable!(),
        }
        let expected = if mode == "stale" {
            "stale_revision"
        } else if matches!(mode, "task" | "repository" | "commit" | "checkpoint") {
            "source_mismatch"
        } else {
            "invalid_input"
        };
        let first = call(&c, Operation::PrepareRelease(input.clone())).await;
        assert_eq!(code(first.clone()), expected, "{mode}");
        assert_eq!(
            call(&c, Operation::PrepareRelease(input)).await,
            first,
            "refusal receipt replay {mode}"
        );
    }
    let queued = submit(&c, "release-queued-submit", coding("release-queued")).await;
    let mut input = release_input(&t, "release-before-success");
    input.task_id = queued.id;
    input.expected_task_revision = queued.revision;
    assert_eq!(
        code(call(&c, Operation::PrepareRelease(input)).await),
        "invalid_transition"
    );
    assert!(
        matches!(call(&c, Operation::ListReleases { task_id: t.id, after: None, limit: 1 }).await,
        Outcome::Ok { data: Data::Releases(values) } if values.is_empty())
    );
}

#[tokio::test]
async fn release_pages_are_task_scoped_and_ordinary_reports_cannot_qualify() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let t = accepted_release_task(&c, "release-pages").await;
    let other = accepted_release_task(&c, "release-pages-other").await;
    let mut first = release_input(&t, "release-page-one");
    let one = release(call(&c, Operation::PrepareRelease(first.clone())).await);
    first.operation_id = "release-page-two".into();
    first.artifact.package_sha256 = "a".repeat(64);
    let two = release(call(&c, Operation::PrepareRelease(first)).await);
    assert!(
        matches!(call(&c, Operation::ListReleases { task_id: t.id.clone(), after: Some(one.id.clone()), limit: 1 }).await,
        Outcome::Ok { data: Data::Releases(values) } if values == vec![two])
    );
    assert_eq!(
        code(
            call(
                &c,
                Operation::ListReleases {
                    task_id: other.id,
                    after: Some(one.id.clone()),
                    limit: 1
                }
            )
            .await
        ),
        "invalid_input"
    );
    assert_eq!(
        code(
            call(
                &c,
                Operation::ListReleases {
                    task_id: t.id.clone(),
                    after: None,
                    limit: 0
                }
            )
            .await
        ),
        "invalid_input"
    );
    assert_eq!(
        code(
            call(
                &c,
                Operation::ListReleases {
                    task_id: t.id.clone(),
                    after: None,
                    limit: config().max_list_tasks + 1
                }
            )
            .await
        ),
        "invalid_input"
    );
    let a = t.attempt.as_ref().unwrap();
    let observed = report(
        &c,
        a,
        "forged-release-report",
        Observation::ResultCandidate {
            checks_passed: true,
            summary: "protected merge approved; rollout authorized".into(),
            checkpoint: Some(one.checkpoint.clone()),
            validation: vec!["all protected checks pass".into()],
            output: None,
        },
    )
    .await;
    assert!(matches!(observed, Outcome::Refused { .. }));
    // The old generic external-operation path is not a release authority. It
    // cannot reopen this completed attempt or modify any candidate status.
    assert!(matches!(
        call(
            &c,
            Operation::PrepareOperation {
                operation_id: "fake-merge".into(),
                task_id: t.id,
                attempt_id: a.id.clone(),
                generation: a.generation,
                kind: "merge".into(),
                intended: json!({"protected":true,"approved":true})
            }
        )
        .await,
        Outcome::Refused { .. }
    ));
    let reread = release(call(&c, Operation::GetRelease { release_id: one.id }).await);
    assert_unqualified(&reread);
}

#[test]
fn release_wire_rejects_forged_authority_fields() {
    let base = json!({"operation":"prepare_release","input":{
        "operation_id":"release","task_id":"task","expected_task_revision":1,
        "checkpoint":{"registration":fixture_repository_binding(),"repository":"repo","commit":"b".repeat(40),"remote_ref":"ref","operation_id":"cp"},
        "artifact":{"format":"borg-v2-release/1","package_sha256":"1".repeat(64),"package_bytes":20480,
            "manifest_sha256":"2".repeat(64),"source_commit":"b".repeat(40),"source_tree":"3".repeat(40),
            "object_format":"sha1","binary_sha256":"4".repeat(64),"binary_bytes":1024,"target":"x86_64-unknown-linux-gnu"}
    }});
    assert!(serde_json::from_value::<Operation>(base.clone()).is_ok());
    for field in [
        "approved",
        "checks",
        "qualification",
        "human_rollout",
        "target",
        "protected_pass",
    ] {
        let mut forged = base.clone();
        forged["input"][field] = json!(true);
        assert!(
            serde_json::from_value::<Operation>(forged).is_err(),
            "{field}"
        );
    }
    let mut forged = base;
    forged["input"]["artifact"]["verified"] = json!(true);
    assert!(serde_json::from_value::<Operation>(forged).is_err());
    assert!(serde_json::from_value::<ArtifactQualification>(json!("verified")).is_err());
    assert!(serde_json::from_value::<ProtectedStatus>(json!("passed")).is_err());
}

#[tokio::test]
async fn release_original_corruption_and_capacity_refuse_without_rewriting() {
    for mode in ["candidate", "checkpoint", "capacity"] {
        let p = path();
        let c = Controller::open_with_config(&p, config()).unwrap();
        let t = accepted_release_task(&c, mode).await;
        let input = release_input(&t, "release-corruption");
        release(call(&c, Operation::PrepareRelease(input.clone())).await);
        if mode == "capacity" {
            let mut second = input;
            second.operation_id = "second".into();
            second.artifact.package_sha256 = "a".repeat(64);
            release(call(&c, Operation::PrepareRelease(second)).await);
        }
        drop(c);
        controller_owner_released(&p).await;
        {
            let db = rusqlite::Connection::open(&p).unwrap();
            match mode {
                "candidate" => {
                    db.execute(
                        "UPDATE releases SET body=json_set(body,'$.qualification.checks','passed')",
                        [],
                    )
                    .unwrap();
                }
                "checkpoint" => {
                    db.execute(
                        "UPDATE external_operations SET settlement=NULL WHERE id=?1",
                        [&t.selected_checkpoint.as_ref().unwrap().operation_id],
                    )
                    .unwrap();
                }
                _ => {}
            }
        }
        let before = std::fs::read(&p).unwrap();
        let cfg = with_controller_limits(ControllerConfig {
            ..config()
        }, |limits| { limits.max_release_candidates = if mode == "capacity" { 1 } else { 100 }; });
        assert!(Controller::open_with_config(&p, cfg).is_err(), "{mode}");
        assert_eq!(std::fs::read(&p).unwrap(), before, "{mode}");
    }
}

#[tokio::test]
async fn release_uses_original_checkpoint_publication_after_successful_recovery_attempt() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let original = submit(&c, "reused-source-submit", coding("reused-source-agent")).await;
    let w = worker("reused-source-worker", "reused-source-incarnation", 0, 1);
    register(&c, &w).await;
    let first_commands = commands(&c, &w, vec![]).await;
    let publisher = run(&first_commands);
    let cp = Checkpoint {
        registration: fixture_repository_binding(),
        repository: original
            .input
            .repository
            .as_ref()
            .unwrap()
            .checkpoint_remote
            .clone(),
        commit: "b".repeat(40),
        operation_id: "prior-checkpoint".into(),
        remote_ref: checkpoint_ref(&original.id, &publisher.id, "prior-checkpoint"),
    };
    assert!(matches!(call(&c, Operation::PrepareOperation {
        operation_id: cp.operation_id.clone(), task_id: original.id.clone(), attempt_id: publisher.id.clone(),
        generation: publisher.generation, kind: "checkpoint".into(),
        intended: json!({"registration":cp.registration,"repository":cp.repository,"commit":cp.commit,"remote_ref":cp.remote_ref}),
    }).await, Outcome::Ok { .. }));
    assert!(matches!(
        call(
            &c,
            Operation::SettleOperation {
                settlement_id: "controller-settlement-3".into(),
                operation_id: cp.operation_id.clone(),
                task_id: original.id.clone(),
                attempt_id: publisher.id.clone(),
                generation: publisher.generation,
                outcome: json!({"acknowledged":true}),
                checkpoint: Some(cp.clone()),
            }
        )
        .await,
        Outcome::Ok { .. }
    ));
    let current = get(&c, &original.id).await;
    task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "cancel-publisher".into(),
                task_id: original.id.clone(),
                expected_revision: current.revision,
            },
        )
        .await,
    );
    let stopped = task(
        report(
            &c,
            &publisher,
            "publisher-stopped",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    task(
        call(
            &c,
            Operation::RetryTask {
                operation_id: "retry-source".into(),
                task_id: original.id.clone(),
                expected_revision: stopped.revision,
            },
        )
        .await,
    );
    let ack = first_commands
        .iter()
        .map(|command| match command {
            WorkerCommand::RunAttempt { command_id, .. }
            | WorkerCommand::StopAttempt { command_id, .. }
                    | WorkerCommand::CollaborationReady { command_id, .. }
                    | WorkerCommand::PublishRepository { command_id, .. } => command_id.clone(),
        })
        .collect();
    let accepting = run(&commands(&c, &w, ack).await);
    assert_ne!(accepting.id, publisher.id);
    assert!(accepting.generation > publisher.generation);
    assert_eq!(
        get(&c, &original.id).await.selected_checkpoint,
        Some(cp.clone())
    );
    task(
        report(
            &c,
            &accepting,
            "recovery-executor",
            Observation::ExecutorOutcome {
                success: true,
                summary: "validated recovered source".into(),
            },
        )
        .await,
    );
    task(
        report(
            &c,
            &accepting,
            "recovery-stopped",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    let accepted = task(
        report(
            &c,
            &accepting,
            "recovery-result",
            Observation::ResultCandidate {
                checks_passed: true,
                summary: "same acknowledged source".into(),
                checkpoint: Some(cp.clone()),
                validation: vec!["controlled recovery check".into()],
                output: None,
            },
        )
        .await,
    );
    assert_eq!(accepted.status, TaskStatus::Succeeded);
    let prepared = release(
        call(
            &c,
            Operation::PrepareRelease(release_input(&accepted, "recovery-release")),
        )
        .await,
    );
    assert_eq!(prepared.accepted_attempt_id, accepting.id);
    assert_eq!(prepared.checkpoint, cp);
    assert_unqualified(&prepared);
    drop(c);
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(
        release(
            call(
                &c,
                Operation::GetRelease {
                    release_id: prepared.id.clone()
                }
            )
            .await
        ),
        prepared
    );
}

#[tokio::test]
//= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
//# The controller's database MUST reside on local storage.
async fn local_storage_admission_accepts_current_ext4_and_reopens_durable_task() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let t = submit(&c, "local-storage-submit", coding("local-storage-agent")).await;
    drop(c);
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(get(&c, &t.id).await, t);
    // The directory/file admission changes no stored schema or wire format.
    drop(c);
    controller_owner_released(&p).await;
    let db = rusqlite::Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .unwrap();
    assert_eq!(
        db.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        24
    );
}

#[test]
fn local_storage_refusal_precedes_database_or_lock_creation() {
    // /proc/PID is a real procfs directory (unlike the symlink /proc/self).
    // No mount or host setup is required; admission must refuse procfs before
    // a write attempt, not merely fall through to an OS permission error.
    let p = PathBuf::from(format!(
        "/proc/{}/borg-local-storage-{}.db",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    assert!(!p.exists());
    let error = Controller::open_with_config(&p, config())
        .err()
        .expect("procfs refusal");
    assert!(error.contains("recognized local filesystem"), "{error}");
    assert!(!p.exists());
    assert!(!controller_sidecar(&p, ".controller-lock").exists());
    assert!(!controller_sidecar(&p, ".native-helpers").exists());
}

#[test]
fn sqlite_sidecar_aliases_refuse_before_writing_original_files() {
    use std::os::unix::fs::symlink;
    for suffix in ["-wal", "-shm", "-journal"] {
        for kind in ["symlink", "hardlink", "directory"] {
            let p = path();
            std::fs::write(&p, b"preserve original DB bytes").unwrap();
            let sidecar = controller_sidecar(&p, suffix);
            let original = p.parent().unwrap().join("original-sidecar");
            std::fs::write(&original, b"preserve original sidecar").unwrap();
            match kind {
                "symlink" => symlink(&original, &sidecar).unwrap(),
                "hardlink" => std::fs::hard_link(&original, &sidecar).unwrap(),
                "directory" => std::fs::create_dir(&sidecar).unwrap(),
                _ => unreachable!(),
            }
            let error = Controller::open_with_config(&p, config())
                .err()
                .expect("alias refusal");
            assert!(error.contains("sidecar"), "{suffix}/{kind}: {error}");
            assert_eq!(std::fs::read(&p).unwrap(), b"preserve original DB bytes");
            assert_eq!(
                std::fs::read(&original).unwrap(),
                b"preserve original sidecar"
            );
            assert!(!controller_sidecar(&p, ".controller-lock").exists());
        }
    }
}

#[tokio::test]
async fn local_storage_preflight_does_not_release_existing_sqlite_owner_locks() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    // The rejected second owner opens metadata-only descriptors during mount
    // admission. Ordinary file descriptors would release same-process SQLite
    // POSIX locks on close, even though the persistent controller flock survives.
    assert!(Controller::open_with_config(&p, config()).is_err());
    let outcome = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "local_storage_sqlite_lock_contender",
            "--ignored",
            "--nocapture",
        ])
        .env("BORG_LOCAL_STORAGE_LOCK_FIXTURE", &p)
        .output()
        .unwrap();
    assert!(
        outcome.status.success(),
        "same-image external SQLite must remain excluded: {} {}",
        String::from_utf8_lossy(&outcome.stdout),
        String::from_utf8_lossy(&outcome.stderr)
    );
    let t = submit(
        &c,
        "after-local-preflight",
        coding("after-local-preflight-agent"),
    )
    .await;
    assert_eq!(get(&c, &t.id).await, t);
}

/// Same linked SQLite/profile in a separate process, bypassing the controller's
/// flock solely to observe whether the original SQLite lock survived preflight.
#[test]
#[ignore = "requires parent-controlled live SQLite lock fixture"]
fn local_storage_sqlite_lock_contender() {
    let path = std::env::var_os("BORG_LOCAL_STORAGE_LOCK_FIXTURE").expect("parent fixture path");
    let conn = rusqlite::Connection::open_with_flags(
        PathBuf::from(path),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
    )
    .unwrap();
    conn.busy_timeout(Duration::ZERO).unwrap();
    let error = conn
        .execute_batch("BEGIN EXCLUSIVE")
        .expect_err("live owner must retain SQLite exclusion");
    assert!(
        matches!(
            error.sqlite_error_code(),
            Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
        ),
        "{error}"
    );
}

async fn controlled_delegate(
    c: &Controller,
    parent: &Task,
    attempt: &Attempt,
    id: &str,
    agent: &str,
    requests: u32,
) -> Task {
    let mut input = coding(agent);
    input.limits.deadline_unix_ms = parent.input.limits.deadline_unix_ms;
    input.limits.model_requests = requests;
    task(
        call(
            c,
            Operation::DelegateTask {
                operation_id: id.into(),
                parent_task_id: parent.id.clone(),
                parent_attempt_id: attempt.id.clone(),
                generation: attempt.generation,
                child: input,
            },
        )
        .await,
    )
}

#[tokio::test]
// Traceability scope: No new repair policy for already terminal roots; their existing no-op contract remains explicit.
//= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
//= type=test
//# A cancellation request MUST commit cancellation intent and its stop command atomically.
async fn task_recovery_cancel_traverses_settled_child_at_receipt_capacity() {
    for running_grandchild in [false, true] {
        let p = path();
        let cfg = with_controller_limits(ControllerConfig {
            ..config()
        }, |limits| { limits.max_receipts = 24; limits.max_cleanup_receipts = 64; });
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        for agent_id in ["chain-c", "chain-g"] {
            let definition = coding(agent_id).definition;
            assert!(matches!(call(&c, Operation::RegisterAgent {
                operation_id: format!("chain-register-{agent_id}"),
                definition: definition.clone(),
            }).await, Outcome::Ok { data: Data::Agent(agent) } if agent.definition == definition));
        }
        let parent = submit(&c, "chain-parent", coding("chain-p")).await;
        let w = worker("chain-worker", "chain-inc", 0, 1);
        register(&c, &w).await;
        let pa = run(&commands(&c, &w, vec![]).await);
        let child = controlled_delegate(&c, &parent, &pa, "chain-child", "chain-c", 5).await;
        task(
            report(
                &c,
                &pa,
                "chain-parent-stopped",
                Observation::ProcessTreeStopped,
            )
            .await,
        );
        let ca = run(&commands(&c, &w, vec![]).await);
        assert_eq!(ca.task_id, child.id);
        let grandchild =
            controlled_delegate(&c, &child, &ca, "chain-grandchild", "chain-g", 1).await;
        task(
            report(
                &c,
                &ca,
                "chain-child-unknown",
                Observation::ExecutionUncertain {
                    message: "controlled unknown effect while handoff quiesces".into(),
                },
            )
            .await,
        );
        let spare = worker("chain-spare", "spare-inc", 0, 1);
        register(&c, &spare).await;
        let ga = if running_grandchild {
            let a = run(&commands(&c, &spare, vec![]).await);
            assert_eq!(a.task_id, grandchild.id);
            Some(a)
        } else {
            None
        };
        let current = get(&c, &parent.id).await;
        assert!(current.waiting_child_id.is_none());
        assert_eq!(current.completed_child.as_ref().unwrap().task_id, child.id);
        assert_eq!(
            current.completed_child.as_ref().unwrap().status,
            TaskStatus::Failed
        );
        let terminal_child = get(&c, &child.id).await;
        assert!(terminal_child.attempt.as_ref().unwrap().uncertain);
        assert_eq!(
            code(
                call(
                    &c,
                    Operation::CancelTask {
                        operation_id: "stale-chain-cancel".into(),
                        task_id: parent.id.clone(),
                        expected_revision: current.revision - 1
                    }
                )
                .await
            ),
            "stale_revision"
        );
        assert_ne!(get(&c, &grandchild.id).await.status, TaskStatus::Cancelled);
        // Exhaust only normal receipts, without creating unrelated live tasks.
        let mut full = false;
        for index in 0..=cfg.max_receipts {
            let mut malformed = coding("invalid-fill");
            malformed.instruction.clear();
            let outcome = call(
                &c,
                Operation::SubmitTask {
                    operation_id: format!("fill-{index}"),
                    input: malformed,
                },
            )
            .await;
            if code(outcome) == "capacity" {
                full = true;
                break;
            }
        }
        assert!(full);
        let cancel = Operation::CancelTask {
            operation_id: "chain-cancel".into(),
            task_id: parent.id.clone(),
            expected_revision: get(&c, &parent.id).await.revision,
        };
        let cancelled = task(call(&c, cancel.clone()).await);
        assert_eq!(cancelled.status, TaskStatus::Cancelled);
        let stopped_g = get(&c, &grandchild.id).await;
        assert_eq!(
            stopped_g.status,
            TaskStatus::Cancelled,
            "cancellation lost the durable descendant edge"
        );
        let still_terminal = get(&c, &child.id).await;
        assert_eq!(still_terminal.status, terminal_child.status);
        assert_eq!(still_terminal.result, terminal_child.result);
        assert!(still_terminal.attempt.as_ref().unwrap().uncertain);
        if let Some(a) = &ga {
            assert_eq!(
                stopped_g.attempt.as_ref().unwrap().phase,
                AttemptPhase::StopRequested
            );
            assert!(commands(&c, &spare, vec![]).await.iter().any(|command|
                matches!(command, WorkerCommand::StopAttempt { attempt_id, .. } if attempt_id == &a.id)));
        } else {
            assert!(stopped_g.attempt.is_none());
            assert!(commands(&c, &spare, vec![]).await.iter().all(|command|
                !matches!(command, WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == grandchild.id)));
        }
        drop(c);
        let c = Controller::open_with_config(&p, cfg).unwrap();
        assert_eq!(task(call(&c, cancel).await), cancelled);
        assert_eq!(get(&c, &grandchild.id).await.status, TaskStatus::Cancelled);
        assert!(get(&c, &child.id).await.attempt.as_ref().unwrap().uncertain);
        if let Some(a) = &ga {
            task(
                report(
                    &c,
                    a,
                    "chain-grandchild-stopped",
                    Observation::ProcessTreeStopped,
                )
                .await,
            );
        }
        drop(c);
        controller_owner_released(&p).await;
        let db =
            rusqlite::Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        assert_eq!(
            db.query_row(
                "SELECT cleanup FROM receipts WHERE id='chain-cancel'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
        assert_eq!(
            db.query_row(
                "SELECT count(*) FROM history WHERE task_id=?1 AND kind='cancel_requested'",
                [&grandchild.id],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
    }
}

#[tokio::test]
// Traceability scope: Diagnostic snapshot is not a capacity reservation or connectivity proof; an eligible task can remain queued awaiting a worker command read.
//= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
//= type=test
//# A rejected or deferred admission MUST expose its reason.
async fn task_recovery_views_recompute_session_and_registered_executor_capacity() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let w = worker("view-worker", "view-inc", 0, 1);
    register(&c, &w).await;
    let first = submit(&c, "view-first", coding("view-shared")).await;
    let shared = submit(&c, "view-shared", coding("view-shared")).await;
    let other = submit(&c, "view-other", coding("view-other")).await;
    let a = run(&commands(&c, &w, vec![]).await);
    assert_eq!(a.task_id, first.id);
    assert_eq!(
        get(&c, &shared.id).await.blocked_reason.as_deref(),
        Some("executor_capacity")
    );
    assert_eq!(
        get(&c, &other.id).await.blocked_reason.as_deref(),
        Some("executor_capacity")
    );
    let spare = worker("view-spare", "view-spare-inc", 0, 1);
    register(&c, &spare).await;
    let observed = get(&c, &other.id).await;
    assert_eq!(
        observed.blocked_reason, None,
        "another registered compatible worker has room"
    );
    assert!(
        observed.attempt.is_none(),
        "diagnostic read does not reserve or admit"
    );
    if let Outcome::Ok {
        data: Data::Tasks(tasks),
    } = call(
        &c,
        Operation::ListTasks {
            after: None,
            limit: 10,
        },
    )
    .await
    {
        assert_eq!(
            tasks
                .iter()
                .find(|t| t.id == shared.id)
                .unwrap()
                .blocked_reason
                .as_deref(),
            None
        );
        assert_eq!(
            tasks
                .iter()
                .find(|t| t.id == other.id)
                .unwrap()
                .blocked_reason,
            None
        );
    } else {
        panic!("task list expected");
    }
    finish_controlled_coding(&c, &a, "view-done").await;
    let ready = get(&c, &shared.id).await;
    assert_eq!(ready.blocked_reason, None);
    assert!(ready.attempt.is_none());
    let next = run(&commands(&c, &spare, vec![]).await);
    assert_eq!(next.task_id, shared.id, "oldest eligible order is retained");
}

#[tokio::test]
// Traceability scope: Diagnostic snapshot is not a capacity reservation or connectivity proof; an eligible task can remain queued awaiting a worker command read.
//= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
//= type=test
//# A rejected or deferred admission MUST expose its reason.
async fn task_recovery_view_clears_own_effect_and_child_ready_stale_reasons() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let parent = submit(&c, "view-effect-parent", coding("view-effect-p")).await;
    let w = worker("view-effect-worker", "view-effect-inc", 0, 1);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    // This is a generic controlled effect, not typed PR publication.
    let intended = json!({"fixture":"controlled-original"});
    let prepared = call(
        &c,
        Operation::PrepareOperation {
            operation_id: "view-effect".into(),
            task_id: parent.id.clone(),
            attempt_id: a.id.clone(),
            generation: a.generation,
            kind: "merge".into(),
            intended: intended.clone(),
        },
    ).await;
    assert!(matches!(&prepared, Outcome::Ok { data: Data::Operation(effect) }
        if effect.id == "view-effect" && effect.task_id == parent.id
            && effect.attempt_id == a.id && effect.generation == a.generation
            && effect.kind == "merge" && effect.intended == intended
            && effect.state == "pending" && effect.outcome.is_none()),
        "generic effect must retain its exact preparation: {prepared:?}");
    let child = controlled_delegate(&c, &parent, &a, "view-effect-child", "view-effect-c", 1).await;
    task(
        report(
            &c,
            &a,
            "view-effect-parent-stopped",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    let ca = run(&commands(&c, &w, vec![]).await);
    assert_eq!(ca.task_id, child.id);
    finish_controlled_coding(&c, &ca, "view-effect-child-done").await;
    let before = get(&c, &parent.id).await;
    assert_eq!(before.status, TaskStatus::Queued);
    assert_eq!(
        before.blocked_reason.as_deref(),
        Some("reconciliation_required")
    );
    assert!(commands(&c, &w, vec![]).await.is_empty());
    assert!(matches!(
        call(
            &c,
            Operation::SettleOperation {
                settlement_id: "controller-settlement-4".into(),
                operation_id: "view-effect".into(),
                task_id: parent.id.clone(),
                attempt_id: a.id.clone(),
                generation: a.generation,
                outcome: json!({"acknowledged":true}),
                checkpoint: None
            }
        )
        .await,
        Outcome::Ok { .. }
    ));
    drop(c);
    let c = Controller::open_with_config(&p, config()).unwrap();
    let ready = get(&c, &parent.id).await;
    assert_eq!(ready.blocked_reason, None);
    assert_eq!(ready.attempt.as_ref().unwrap().id, a.id);
    let next = run(&commands(&c, &w, vec![]).await);
    assert_eq!(next.task_id, parent.id);
    assert_eq!(next.generation, a.generation + 1);
}

#[tokio::test]
async fn task_recovery_terminal_root_cancel_preserves_existing_noop_contract() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let definition = coding("terminal-child").definition;
    assert!(matches!(call(&c, Operation::RegisterAgent {
        operation_id: "terminal-root-register-child".into(),
        definition: definition.clone(),
    }).await, Outcome::Ok { data: Data::Agent(agent) } if agent.definition == definition));
    let parent = submit(&c, "terminal-root", coding("terminal-root-agent")).await;
    let w = worker("terminal-root-worker", "inc", 0, 1);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let child =
        controlled_delegate(&c, &parent, &a, "terminal-root-child", "terminal-child", 1).await;
    let failed = task(
        report(
            &c,
            &a,
            "terminal-root-unknown",
            Observation::ExecutionUncertain {
                message: "unknown original execution".into(),
            },
        )
        .await,
    );
    let before_child = get(&c, &child.id).await;
    let op = Operation::CancelTask {
        operation_id: "cancel-already-terminal".into(),
        task_id: parent.id,
        expected_revision: 0,
    };
    assert_eq!(task(call(&c, op.clone()).await), failed);
    assert_eq!(task(call(&c, op).await), failed);
    assert_eq!(get(&c, &child.id).await, before_child);
    // This batch traverses historical edges beneath a live root. It does not
    // invent a repair command or reopen an already terminal root.
}

async fn refused_result_fixture(label: &str) -> (PathBuf, Controller, Attempt, Operation, Outcome) {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let task = submit(&c, &format!("{label}-submit"), native(label)).await;
    let w = worker(&format!("{label}-worker"), "inc", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    assert_eq!(a.task_id, task.id);
    controlled_native_request(&c, &a, 1, Some(2), Some(1)).await;
    complete_native_loop(&c, &a).await;
    report(
        &c,
        &a,
        &format!("{label}-executor"),
        Observation::ExecutorOutcome {
            success: true,
            summary: "controlled completed result".into(),
        },
    )
    .await;
    report(
        &c,
        &a,
        &format!("{label}-stopped"),
        Observation::ProcessTreeStopped,
    )
    .await;
    drop(c);
    let c = Controller::open_with_config(
        &p,
        ControllerConfig {
            validation_executable: PathBuf::from("/usr/bin/false"),
            ..config()
        },
    )
    .unwrap();
    let op = Operation::ObserveWorker {
        report: Report {
            report_id: format!("{label}-result"),
            worker_id: a.worker_id.clone(),
            incarnation: a.worker_incarnation.clone(),
            task_id: a.task_id.clone(),
            attempt_id: a.id.clone(),
            generation: a.generation,
            observation: Observation::ResultCandidate {
                checks_passed: true,
                summary: "already completed output".into(),
                checkpoint: None,
                validation: vec![borg_runtime::native_loop_contract::evidence(&a.id)],
                output: Some(json!({"answer":42})),
            },
        },
    };
    let refused = call(&c, op.clone()).await;
    assert_eq!(code(refused.clone()), "validation_unavailable");
    assert_eq!(get(&c, &a.task_id).await.status, TaskStatus::Active);
    (p, c, a, op, refused)
}
fn reconcile_result(id: &str, original: &str) -> Operation {
    Operation::ReconcileResult(ReconcileResultInput {
        operation_id: id.into(),
        original_report_id: original.into(),
    })
}

#[tokio::test]
// Traceability scope: Selected usage/result history paths; function name alone in the older general restart test was not used as evidence.
//= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
//= type=test
//# The platform MUST persist task history in its database.
// Traceability scope: Actual successful/reconciled transaction path; not an exhaustive injected commit-I/O-failure matrix.
//= spec/impl/v2/implementation.md#task-state-separates-execution-from-acceptance
//= type=test
//# An accepted terminal result MUST commit the task state and corresponding history atomically.
async fn task_recovery_result_reconciliation_retries_same_id_without_rewriting_original() {
    let (p, c, a, original, refused) = refused_result_fixture("reconcile").await;
    let op = reconcile_result("acceptance-reconcile", "reconcile-result");
    assert!(
        matches!(call(&c, op.clone()).await, Outcome::Pending { ref operation_id } if operation_id=="acceptance-reconcile")
    );
    assert_eq!(
        code(
            call(
                &c,
                Operation::GetOperation {
                    operation_id: "acceptance-reconcile".into()
                }
            )
            .await
        ),
        "not_found"
    );
    assert_eq!(call(&c, original.clone()).await, refused);
    drop(c);
    let c = Controller::open_with_config(
        &p,
        ControllerConfig {
            validation_executable: PathBuf::from("/usr/bin/false"),
            ..config()
        },
    )
    .unwrap();
    assert!(matches!(
        call(&c, op.clone()).await,
        Outcome::Pending { .. }
    ));
    drop(c);
    controller_owner_released(&p).await;
    let normal_used: usize = {
        let db =
            rusqlite::Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        usize::try_from(
            db.query_row("SELECT count(*) FROM receipts WHERE cleanup=0", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap(),
        )
        .unwrap()
    };
    let bounded = with_controller_limits(ControllerConfig {
        ..config()
    }, |limits| { limits.max_receipts = normal_used; });
    let c = Controller::open_with_config(&p, bounded.clone()).unwrap();

    let accepted = call(&c, op.clone()).await;
    assert_eq!(task(accepted.clone()).status, TaskStatus::Succeeded);
    assert_eq!(
        task(accepted.clone()).result.unwrap().output,
        Some(json!({"answer":42}))
    );
    assert_eq!(call(&c, op.clone()).await, accepted);
    assert_eq!(call(&c, original.clone()).await, refused);
    assert_eq!(
        code(
            call(
                &c,
                reconcile_result("acceptance-reconcile", "different-result")
            )
            .await
        ),
        "operation_conflict"
    );
    assert_eq!(get(&c, &a.task_id).await.attempt.as_ref().unwrap().id, a.id);
    drop(c);
    let c = Controller::open_with_config(&p, bounded).unwrap();
    assert_eq!(call(&c, op).await, accepted);
    assert_eq!(call(&c, original).await, refused);
    drop(c);
    controller_owner_released(&p).await;
    let db = rusqlite::Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .unwrap();
    assert_eq!(
        db.query_row(
            "SELECT cleanup FROM receipts WHERE id='acceptance-reconcile'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
    assert_eq!(db.query_row("SELECT count(*) FROM history WHERE task_id=?1 AND json_extract(body,'$.kind')='result_candidate'", [&a.task_id], |r|r.get::<_,i64>(0)).unwrap(), 1);
}

#[tokio::test]
async fn task_recovery_result_reconciliation_rechecks_stale_cancelled_and_expired_owners() {
    for mode in ["cancel", "stale", "deadline"] {
        let (p, c, a, original, refused) = refused_result_fixture(mode).await;
        drop(c);
        let before = Controller::open_with_config(&p, config()).unwrap();
        let current = get(&before, &a.task_id).await;
        let deadline = current.input.limits.deadline_unix_ms;
        if mode != "deadline" {
            let cancelled = task(
                call(
                    &before,
                    Operation::CancelTask {
                        operation_id: "cancel-for-reconcile".into(),
                        task_id: a.task_id.clone(),
                        expected_revision: current.revision,
                    },
                )
                .await,
            );
            if mode == "stale" {
                task(
                    call(
                        &before,
                        Operation::RetryTask {
                            operation_id: "new-attempt".into(),
                            task_id: a.task_id.clone(),
                            expected_revision: cancelled.revision,
                        },
                    )
                    .await,
                );
                let w = worker(&a.worker_id, &a.worker_incarnation, 1, 0);
                let next = run(&commands(&before, &w, vec![]).await);
                assert!(next.generation > a.generation);
            }
        }
        drop(before);
        let cfg = if mode == "deadline" {
            ControllerConfig {
                clock: Arc::new(move || deadline + 1),
                ..config()
            }
        } else {
            config()
        };
        let c = Controller::open_with_config(&p, cfg).unwrap();
        let op = reconcile_result("permanent-decision", &format!("{mode}-result"));
        let rejected = call(&c, op.clone()).await;
        assert_eq!(
            code(rejected.clone()),
            if mode == "stale" {
                "stale_generation"
            } else {
                "invalid_transition"
            }
        );
        assert_eq!(call(&c, op).await, rejected);
        assert_eq!(call(&c, original).await, refused);
        assert!(get(&c, &a.task_id).await.result.is_none());
    }
}

#[tokio::test]
async fn task_recovery_result_reconciliation_rejects_noninfrastructure_originals() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let t = submit(&c, "permanent-submit", native("permanent-result")).await;
    let w = worker("permanent-worker", "inc", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    controlled_native_request(&c, &a, 1, None, None).await;
    task(
        report(
            &c,
            &a,
            "permanent-executor",
            Observation::ExecutorOutcome {
                success: true,
                summary: "completed".into(),
            },
        )
        .await,
    );
    for (id, value) in [
        ("invalid-output", json!({"answer":"wrong"})),
        ("large-output", json!("x".repeat(20000))),
    ] {
        let candidate = Observation::ResultCandidate {
            checks_passed: true,
            summary: "candidate".into(),
            checkpoint: None,
            validation: vec![],
            output: Some(value),
        };
        assert!(matches!(
            report(&c, &a, id, candidate).await,
            Outcome::Refused { .. }
        ));
        assert_eq!(
            code(call(&c, reconcile_result(&format!("reconcile-{id}"), id)).await),
            "not_reconcilable"
        );
    }
    assert_eq!(
        code(call(&c, reconcile_result("missing-reconcile", "not-present")).await),
        "not_found"
    );
    assert_eq!(
        code(
            call(
                &c,
                reconcile_result("wrong-type-reconcile", "permanent-submit")
            )
            .await
        ),
        "invalid_transition"
    );
    assert_eq!(get(&c, &t.id).await.status, TaskStatus::Active);
}

#[tokio::test]
async fn task_recovery_live_root_stops_terminal_descendant_without_rewriting_result() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let parent = submit(&c, "terminal-child-parent", coding("terminal-child-p")).await;
    let w = worker("terminal-child-worker", "inc", 0, 1);
    register(&c, &w).await;
    let pa = run(&commands(&c, &w, vec![]).await);
    let child = controlled_delegate(
        &c,
        &parent,
        &pa,
        "terminal-child-delegate",
        "terminal-child-c",
        1,
    )
    .await;
    task(
        report(
            &c,
            &pa,
            "terminal-child-parent-stop",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    let ca = run(&commands(&c, &w, vec![]).await);
    let cp = Checkpoint {
        registration: fixture_repository_binding(),
        repository: child
            .input
            .repository
            .as_ref()
            .unwrap()
            .checkpoint_remote
            .clone(),
        commit: "c".repeat(40),
        operation_id: "terminal-child-cp".into(),
        remote_ref: checkpoint_ref(&child.id, &ca.id, "terminal-child-cp"),
    };
    assert!(matches!(call(&c, Operation::PrepareOperation { operation_id: cp.operation_id.clone(), task_id: child.id.clone(), attempt_id: ca.id.clone(), generation: ca.generation,
        kind: "checkpoint".into(), intended: json!({"registration":cp.registration,"repository":cp.repository,"commit":cp.commit,"remote_ref":cp.remote_ref}) }).await, Outcome::Ok { .. }));
    assert!(matches!(
        call(
            &c,
            Operation::SettleOperation {
                settlement_id: "controller-settlement-5".into(),
                operation_id: cp.operation_id.clone(),
                task_id: child.id.clone(),
                attempt_id: ca.id.clone(),
                generation: ca.generation,
                outcome: json!({"acknowledged":true}),
                checkpoint: Some(cp.clone())
            }
        )
        .await,
        Outcome::Ok { .. }
    ));
    task(
        report(
            &c,
            &ca,
            "terminal-child-executor",
            Observation::ExecutorOutcome {
                success: true,
                summary: "controlled finished result".into(),
            },
        )
        .await,
    );
    let completed = task(
        report(
            &c,
            &ca,
            "terminal-child-result",
            Observation::ResultCandidate {
                checks_passed: true,
                summary: "accepted before cleanup report".into(),
                checkpoint: Some(cp),
                validation: vec!["controlled check pass".into()],
                output: None,
            },
        )
        .await,
    );
    assert_eq!(completed.status, TaskStatus::Succeeded);
    assert!(!completed.attempt.as_ref().unwrap().stopped);
    let current = get(&c, &parent.id).await;
    assert_eq!(current.status, TaskStatus::Queued);
    task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "cancel-terminal-descendant-root".into(),
                task_id: parent.id,
                expected_revision: current.revision,
            },
        )
        .await,
    );
    let stopped = get(&c, &child.id).await;
    assert_eq!(stopped.status, TaskStatus::Succeeded);
    assert_eq!(stopped.result, completed.result);
    assert_eq!(stopped.selected_checkpoint, completed.selected_checkpoint);
    assert_eq!(
        stopped.attempt.as_ref().unwrap().phase,
        AttemptPhase::StopRequested
    );
    assert!(!stopped.attempt.as_ref().unwrap().stopped);
    assert!(commands(&c, &w, vec![]).await.iter().any(|command|
        matches!(command, WorkerCommand::StopAttempt { attempt_id, .. } if attempt_id==&ca.id)));
    let cleaned = task(
        report(
            &c,
            &ca,
            "terminal-child-actual-cleanup",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    assert!(cleaned.attempt.as_ref().unwrap().stopped);
    assert_eq!(cleaned.result, completed.result);
}

// These cases use the real local controller and its durable operation path.
// Coding outcomes/cleanup are controlled observations, not Git, Codex or
// provider execution. No native validation helper is needed by these inputs.

#[tokio::test]
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# A task MUST retain the definition revision accepted at submission.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# An execution attempt MUST bind the definition revision it uses.
async fn definition_revision_is_retained_in_original_task_and_successor_commands() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let first_input = coding("revision-agent");
    let mut later_input = first_input.clone();
    later_input.definition.revision = "revision-2".into();
    later_input.instruction = "later definition selection".into();
    let first = submit(&c, "revision-first", first_input.clone()).await;
    let later = submit(&c, "revision-later", later_input.clone()).await;
    drop(c);
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(get(&c, &first.id).await.input, accepted_input(&c, &first_input));
    assert_eq!(get(&c, &later.id).await.input, accepted_input(&c, &later_input));

    let w = worker("revision-worker", "revision-incarnation", 0, 1);
    register(&c, &w).await;
    let initial = commands(&c, &w, vec![]).await;
    let [
        WorkerCommand::RunAttempt {
            attempt,
            task: launched,
            ..
        },
    ] = initial.as_slice()
    else {
        panic!("expected exactly the oldest task's launch: {initial:?}");
    };
    assert_eq!(attempt.task_id, first.id);
    assert_eq!(launched, &accepted_input(&c, &first_input));
    let original = attempt.clone();
    task(
        report(
            &c,
            &original,
            "revision-used",
            Observation::Usage {
                native_diagnostic_reserved: false,
                model_requests: 2,
                input_tokens: None,
                output_tokens: None,
            },
        )
        .await,
    );
    let current = get(&c, &first.id).await;
    task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "revision-cancel".into(),
                task_id: first.id.clone(),
                expected_revision: current.revision,
            },
        )
        .await,
    );
    let stopped = task(
        report(
            &c,
            &original,
            "revision-stopped",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    task(
        call(
            &c,
            Operation::RetryTask {
                operation_id: "revision-retry".into(),
                task_id: first.id.clone(),
                expected_revision: stopped.revision,
            },
        )
        .await,
    );
    drop(c);
    let c = Controller::open_with_config(&p, config()).unwrap();
    let resumed = commands(&c, &w, vec![]).await;
    let [
        WorkerCommand::RunAttempt {
            attempt,
            task: launched,
            ..
        },
    ] = resumed.as_slice()
    else {
        panic!("expected one successor launch: {resumed:?}");
    };
    assert_eq!(attempt.task_id, first.id);
    assert_ne!(attempt.id, original.id);
    assert_eq!(attempt.generation, original.generation + 1);
    let mut expected_launch = accepted_input(&c, &first_input);
    expected_launch.limits.model_requests -= 2;
    assert_eq!(launched, &expected_launch);
    assert_eq!(get(&c, &first.id).await.input, accepted_input(&c, &first_input));
    finish_controlled_coding(&c, attempt, "revision-complete").await;

    let next = commands(&c, &w, vec![]).await;
    let [
        WorkerCommand::RunAttempt {
            attempt,
            task: launched,
            ..
        },
    ] = next.as_slice()
    else {
        panic!("expected the later task's launch: {next:?}");
    };
    assert_eq!(attempt.task_id, later.id);
    assert_eq!(launched, &accepted_input(&c, &later_input));
    assert_eq!(
        get(&c, &first.id).await.input.definition,
        first_input.definition
    );
    assert_eq!(
        get(&c, &later.id).await.input.definition,
        later_input.definition
    );
}

#[tokio::test]
//= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
//= type=test
//# Task dependencies MUST refer to existing tasks.
//= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
//= type=test
//# A task's dependency set MUST remain immutable after submission.
async fn ordinary_prerequisites_refuse_missing_and_remain_original() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    register_repository_fixture(&c, &fixture_repository_registration()).await;
    let mut missing_input = coding("dependency-parent");
    missing_input.dependencies = vec!["nonexistent-prerequisite".into()];
    let missing = Operation::SubmitTask {
        operation_id: "dependency-missing".into(),
        input: missing_input.clone(),
    };
    let refused = call(&c, missing.clone()).await;
    assert_eq!(code(refused.clone()), "not_found");
    assert_eq!(
        call(
            &c,
            Operation::ListTasks {
                after: None,
                limit: 10
            }
        )
        .await,
        Outcome::Ok {
            data: Data::Tasks(vec![])
        }
    );
    drop(c);
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(call(&c, missing).await, refused);
    assert_eq!(
        call(
            &c,
            Operation::ListTasks {
                after: None,
                limit: 10
            }
        )
        .await,
        Outcome::Ok {
            data: Data::Tasks(vec![])
        }
    );

    let prerequisite = submit(
        &c,
        "dependency-prerequisite",
        coding("dependency-target-agent"),
    )
    .await;
    let w = worker("dependency-worker", "dependency-incarnation", 0, 1);
    register(&c, &w).await;
    let prerequisite_attempt = run(&commands(&c, &w, vec![]).await);
    assert_eq!(prerequisite_attempt.task_id, prerequisite.id);
    assert_eq!(
        finish_controlled_coding(&c, &prerequisite_attempt, "dependency-prerequisite-done")
            .await
            .status,
        TaskStatus::Succeeded
    );

    let mut original_input = missing_input;
    original_input.dependencies = vec![prerequisite.id.clone()];
    assert_eq!(
        code(
            call(
                &c,
                Operation::SubmitTask {
                    operation_id: "dependency-missing".into(),
                    input: original_input.clone(),
                }
            )
            .await
        ),
        "operation_conflict"
    );
    let parent = submit(&c, "dependency-valid", original_input.clone()).await;
    let mut changed = original_input.clone();
    changed.dependencies.clear();
    assert_eq!(
        code(
            call(
                &c,
                Operation::SubmitTask {
                    operation_id: "dependency-valid".into(),
                    input: changed,
                }
            )
            .await
        ),
        "operation_conflict"
    );
    assert_eq!(get(&c, &parent.id).await.input, accepted_input(&c, &original_input));
    let assigned = task(
        call(
            &c,
            Operation::AssignTask {
                operation_id: "dependency-assign".into(),
                task_id: parent.id.clone(),
                expected_revision: parent.revision,
                agent_id: parent.assigned_agent_id.clone(),
            },
        )
        .await,
    );
    assert_eq!(assigned.assigned_agent_id, parent.assigned_agent_id);
    assert_eq!(assigned.input, accepted_input(&c, &original_input));
    let parent_attempt = run(&commands(&c, &w, vec![]).await);
    assert_eq!(parent_attempt.task_id, parent.id);

    let mut child_input = coding("dependency-child");
    child_input.dependencies = original_input.dependencies.clone();
    child_input.limits.deadline_unix_ms = original_input.limits.deadline_unix_ms;
    child_input.limits.model_requests = 1;
    let child = task(
        call(
            &c,
            Operation::DelegateTask {
                operation_id: "dependency-delegate".into(),
                parent_task_id: parent.id.clone(),
                parent_attempt_id: parent_attempt.id.clone(),
                generation: parent_attempt.generation,
                child: child_input.clone(),
            },
        )
        .await,
    );
    let waiting = get(&c, &parent.id).await;
    assert_eq!(waiting.waiting_child_id.as_deref(), Some(child.id.as_str()));
    assert_eq!(waiting.input, accepted_input(&c, &original_input));
    assert_eq!(child.input, accepted_input(&c, &child_input));
    assert_eq!(child.parent_task_id.as_deref(), Some(parent.id.as_str()));
    task(
        report(
            &c,
            &parent_attempt,
            "dependency-parent-stopped",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    let child_attempt = run(&commands(&c, &w, vec![]).await);
    assert_eq!(child_attempt.task_id, child.id);
    let completed_child =
        finish_controlled_coding(&c, &child_attempt, "dependency-child-done").await;
    let ready = get(&c, &parent.id).await;
    assert_eq!(ready.status, TaskStatus::Queued);
    assert!(ready.waiting_child_id.is_none());
    assert_eq!(ready.input, accepted_input(&c, &original_input));
    assert_eq!(
        ready.completed_child.as_ref().unwrap().result,
        completed_child.result
    );
    drop(c);
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(get(&c, &parent.id).await.input, accepted_input(&c, &original_input));
    assert_eq!(get(&c, &child.id).await.input, accepted_input(&c, &child_input));
    let continuation_commands = commands(&c, &w, vec![]).await;
    let [
        WorkerCommand::RunAttempt {
            attempt,
            task: resumed_input,
            continuation,
            ..
        },
    ] = continuation_commands.as_slice()
    else {
        panic!("expected one parent continuation: {continuation_commands:?}");
    };
    assert_eq!(attempt.task_id, parent.id);
    assert_eq!(resumed_input.dependencies, original_input.dependencies);
    assert_eq!(continuation, &ready.completed_child);
    let continuation_attempt = attempt.clone();
    let current = get(&c, &parent.id).await;
    task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "dependency-cancel".into(),
                task_id: parent.id.clone(),
                expected_revision: current.revision,
            },
        )
        .await,
    );
    let stopped = task(
        report(
            &c,
            &continuation_attempt,
            "dependency-continuation-stopped",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    let queued = task(
        call(
            &c,
            Operation::RetryTask {
                operation_id: "dependency-retry".into(),
                task_id: parent.id.clone(),
                expected_revision: stopped.revision,
            },
        )
        .await,
    );
    assert_eq!(queued.input, accepted_input(&c, &original_input));
    let retry_commands = commands(&c, &w, vec![]).await;
    let [
        WorkerCommand::RunAttempt {
            attempt,
            task: retry_input,
            ..
        },
    ] = retry_commands.as_slice()
    else {
        panic!("expected one parent retry: {retry_commands:?}");
    };
    assert_eq!(attempt.task_id, parent.id);
    assert_eq!(attempt.generation, continuation_attempt.generation + 1);
    assert_eq!(retry_input.dependencies, original_input.dependencies);
    assert_eq!(get(&c, &parent.id).await.input, accepted_input(&c, &original_input));
}

#[tokio::test]
// Controller source selection only; no assertion that executor history is available.
//= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
//= type=test
//# A recorded executor conversation identifier MUST NOT be treated as a durable checkpoint by itself.
async fn executor_conversation_id_alone_never_selects_recovery_source() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let original_input = coding("conversation-agent");
    let submitted = submit(&c, "conversation-submit", original_input.clone()).await;
    let w = worker("conversation-worker", "conversation-incarnation", 0, 1);
    register(&c, &w).await;
    let first = run(&commands(&c, &w, vec![]).await);
    let started = task(
        report(
            &c,
            &first,
            "conversation-started",
            Observation::Started {
                thread_id: Some("executor-thread-without-checkpoint".into()),
                turn_id: Some("executor-turn-without-checkpoint".into()),
            },
        )
        .await,
    );
    assert_eq!(
        started.attempt.as_ref().unwrap().phase,
        AttemptPhase::Running
    );
    assert!(started.selected_checkpoint.is_none());
    let progressed = task(
        report(
            &c,
            &first,
            "conversation-progress",
            Observation::Progress {
                text: "executor has local progress".into(),
                output: "unpublished workspace changes".into(),
                truncated: false,
            },
        )
        .await,
    );
    assert!(progressed.selected_checkpoint.is_none());
    drop(c);
    controller_owner_released(&p).await;
    {
        let db =
            rusqlite::Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        let association: (Option<String>, Option<String>) = db
            .query_row(
                "SELECT thread_id,turn_id FROM attempts WHERE id=?1",
                [&first.id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            association,
            (
                Some("executor-thread-without-checkpoint".into()),
                Some("executor-turn-without-checkpoint".into()),
            )
        );
    }
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(get(&c, &submitted.id).await, progressed);
    let alleged_operation = "executor-thread-without-checkpoint";
    let alleged_checkpoint = Checkpoint {
        registration: fixture_repository_binding(),
        repository: submitted
            .input
            .repository
            .as_ref()
            .unwrap()
            .checkpoint_remote
            .clone(),
        commit: "b".repeat(40),
        remote_ref: checkpoint_ref(&submitted.id, &first.id, alleged_operation),
        operation_id: alleged_operation.into(),
    };
    assert_eq!(
        code(
            call(
                &c,
                Operation::SettleOperation {
                    settlement_id: "controller-settlement-6".into(),
                    operation_id: alleged_operation.into(),
                    task_id: submitted.id.clone(),
                    attempt_id: first.id.clone(),
                    generation: first.generation,
                    outcome: json!({"acknowledged":true}),
                    checkpoint: Some(alleged_checkpoint),
                }
            )
            .await
        ),
        "not_found"
    );
    assert_eq!(get(&c, &submitted.id).await, progressed);
    task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "conversation-cancel".into(),
                task_id: submitted.id.clone(),
                expected_revision: progressed.revision,
            },
        )
        .await,
    );
    let stopped = task(
        report(
            &c,
            &first,
            "conversation-stopped",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    task(
        call(
            &c,
            Operation::RetryTask {
                operation_id: "conversation-retry".into(),
                task_id: submitted.id.clone(),
                expected_revision: stopped.revision,
            },
        )
        .await,
    );
    drop(c);
    let c = Controller::open_with_config(&p, config()).unwrap();
    let next = commands(&c, &w, vec![]).await;
    let [
        WorkerCommand::RunAttempt {
            attempt,
            task: retry_input,
            checkpoint,
            ..
        },
    ] = next.as_slice()
    else {
        panic!("expected a fresh attempt from original source: {next:?}");
    };
    assert_eq!(attempt.task_id, submitted.id);
    assert_ne!(attempt.id, first.id);
    assert_eq!(attempt.generation, first.generation + 1);
    assert!(checkpoint.is_none());
    assert_eq!(retry_input.repository, accepted_input(&c, &original_input).repository);
    assert_eq!(retry_input, &accepted_input(&c, &original_input));
    assert!(get(&c, &submitted.id).await.selected_checkpoint.is_none());
}

// Read only after the actual controller owner has drained; compare complete
// history rows, not just a count that could miss a rewritten prior observation.
fn oracle_task_history(path: &std::path::Path, task: &str) -> Vec<(i64, i64, String, String)> {
    let db =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    let mut statement = db
        .prepare("SELECT seq,revision,kind,body FROM history WHERE task_id=?1 ORDER BY seq")
        .unwrap();
    let rows = statement
        .query_map([task], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    rows
}

#[tokio::test]
// Uses reachable cleanup/retry states. Pending effects are never bypassed to
// manufacture an old-unsettled/new-generation state; acknowledgments are fixture data.
//= spec/impl/v2/implementation.md#checkpoint-publication-is-reconciled
//= type=test
//# A superseded attempt's publication MUST NOT advance the task's selected checkpoint.
async fn old_publication_cannot_replace_successor_selected_checkpoint() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let submitted = submit(&c, "publication-submit", coding("publication-agent")).await;
    let w = worker("publication-worker", "publication-incarnation", 0, 1);
    register(&c, &w).await;
    let original = run(&commands(&c, &w, vec![]).await);
    let old_checkpoint = Checkpoint {
        registration: fixture_repository_binding(),
        repository: submitted
            .input
            .repository
            .as_ref()
            .unwrap()
            .checkpoint_remote
            .clone(),
        commit: "c".repeat(40),
        remote_ref: checkpoint_ref(&submitted.id, &original.id, "publication-original"),
        operation_id: "publication-original".into(),
    };
    let prepare = Operation::PrepareOperation {
        operation_id: old_checkpoint.operation_id.clone(),
        task_id: submitted.id.clone(),
        attempt_id: original.id.clone(),
        generation: original.generation,
        kind: "checkpoint".into(),
        intended: json!({"registration":old_checkpoint.registration,"repository":old_checkpoint.repository,"commit":old_checkpoint.commit,"remote_ref":old_checkpoint.remote_ref}),
    };
    assert!(matches!(call(&c, prepare).await,
        Outcome::Ok { data: Data::Operation(OperationRecord { state, .. }) } if state == "pending"));
    let current = get(&c, &submitted.id).await;
    task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "publication-cancel".into(),
                task_id: submitted.id.clone(),
                expected_revision: current.revision,
            },
        )
        .await,
    );
    let stopped = task(
        report(
            &c,
            &original,
            "publication-stopped",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    assert!(stopped.attempt.as_ref().unwrap().uncertain);
    assert_eq!(
        code(
            call(
                &c,
                Operation::RetryTask {
                    operation_id: "publication-blocked-before-restart".into(),
                    task_id: submitted.id.clone(),
                    expected_revision: stopped.revision,
                }
            )
            .await
        ),
        "reconciliation_required"
    );
    drop(c);
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(get(&c, &submitted.id).await, stopped);
    assert_eq!(
        code(
            call(
                &c,
                Operation::RetryTask {
                    operation_id: "publication-blocked-after-restart".into(),
                    task_id: submitted.id.clone(),
                    expected_revision: stopped.revision,
                }
            )
            .await
        ),
        "reconciliation_required"
    );
    let not_applied = Operation::SettleOperation {
        settlement_id: "controller-settlement-7".into(),
        operation_id: old_checkpoint.operation_id.clone(),
        task_id: submitted.id.clone(),
        attempt_id: original.id.clone(),
        generation: original.generation,
        outcome: json!({"acknowledged":true,"not_applied":true}),
        checkpoint: None,
    };
    let original_settlement = call(&c, not_applied.clone()).await;
    assert!(matches!(&original_settlement,
        Outcome::Ok { data: Data::Operation(OperationRecord { state, .. }) } if state == "settled"));
    let settled = get(&c, &submitted.id).await;
    assert!(settled.selected_checkpoint.is_none());
    assert!(!settled.attempt.as_ref().unwrap().uncertain);
    task(
        call(
            &c,
            Operation::RetryTask {
                operation_id: "publication-retry".into(),
                task_id: submitted.id.clone(),
                expected_revision: settled.revision,
            },
        )
        .await,
    );
    let successor = run(&commands(&c, &w, vec![]).await);
    assert_eq!(successor.task_id, submitted.id);
    assert_eq!(successor.generation, original.generation + 1);
    let accepted = finish_controlled_coding(&c, &successor, "publication-successor").await;
    assert_eq!(accepted.status, TaskStatus::Succeeded);
    assert_ne!(
        accepted.selected_checkpoint.as_ref().unwrap(),
        &old_checkpoint
    );
    drop(c);
    controller_owner_released(&p).await;
    let history_before = oracle_task_history(&p, &submitted.id);
    assert!(!history_before.is_empty());

    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(call(&c, not_applied.clone()).await, original_settlement);
    assert_eq!(get(&c, &submitted.id).await, accepted);
    assert_eq!(
        code(
            call(
                &c,
                Operation::SettleOperation {
                    settlement_id: "controller-settlement-8".into(),
                    operation_id: old_checkpoint.operation_id.clone(),
                    task_id: submitted.id.clone(),
                    attempt_id: original.id.clone(),
                    generation: original.generation,
                    outcome: json!({"acknowledged":true}),
                    checkpoint: Some(old_checkpoint.clone()),
                }
            )
            .await
        ),
        "operation_conflict"
    );
    let stale_operation = "publication-stale-new-intent";
    assert_eq!(code(call(&c, Operation::PrepareOperation {
        operation_id: stale_operation.into(),
        task_id: submitted.id.clone(),
        attempt_id: original.id.clone(),
        generation: original.generation,
        kind: "checkpoint".into(),
        intended: json!({"registration":old_checkpoint.registration,"repository":old_checkpoint.repository,"commit":old_checkpoint.commit,
            "remote_ref":checkpoint_ref(&submitted.id,&original.id,stale_operation)}),
    }).await), "stale_generation");
    assert_eq!(get(&c, &submitted.id).await, accepted);
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(oracle_task_history(&p, &submitted.id), history_before);
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(get(&c, &submitted.id).await, accepted);
    assert_eq!(call(&c, not_applied).await, original_settlement);
}

#[path = "controller/mutation_replay.rs"]
mod mutation_replay;

#[path = "controller/reply_metadata.rs"]
mod reply_metadata;

#[path = "controller/store_loss.rs"]
mod store_loss;

#[path = "controller/credential_paths.rs"]
mod credential_paths;

#[path = "controller/startup_wal.rs"]
mod startup_wal;

#[path = "controller/native_tool.rs"]
mod native_tool;

#[path = "controller/acceptance_order.rs"]
mod acceptance_order;

#[path = "controller/collaboration_queries.rs"]
mod collaboration_queries;

#[path = "controller/mcp_calls.rs"]
mod mcp_calls;

#[path = "controller/template_inference.rs"]
mod template_inference;


#[path = "controller/repository_calls.rs"]
mod repository_calls;

#[path = "controller/ui_reads.rs"]
mod ui_reads;

#[path = "controller/documents_reviews.rs"]
mod documents_reviews;

#[path = "controller/collaboration_work.rs"]
mod collaboration_work;

#[path = "controller/collaboration_finalizer.rs"]
mod collaboration_finalizer;

#[path = "controller/collaboration_maintenance_provenance.rs"]
mod collaboration_maintenance_provenance;

#[path = "controller/collaboration_startup_migration.rs"]
mod collaboration_startup_migration;

#[path = "controller/governance.rs"]
mod governance;
#[path = "controller/governance_override_invariants.rs"]
mod governance_override_invariants;
#[path = "controller/governance_agent_update_guards.rs"]
mod governance_agent_update_guards;
#[path = "controller/governance_mounts.rs"]
mod governance_mounts;
#[path = "controller/charters.rs"]
mod charters;
#[path = "controller/native_request_evidence.rs"]
mod native_request_evidence;

#[path = "controller/governance_completion.rs"]
mod governance_completion;

#[path = "controller/governance_completion_final_result.rs"]
mod governance_completion_final_result;

#[path = "controller/collaboration_joined_wait.rs"]
mod collaboration_joined_wait;

#[path = "controller/charter_definitions.rs"]
mod charter_definitions;

#[path = "controller/checkpoint_settlement_identity.rs"]
mod checkpoint_settlement_identity;

#[path = "controller/agent_directory.rs"]
mod agent_directory;

#[path = "controller/task_handoffs.rs"]
mod task_handoffs;

#[path = "controller/identity_administration.rs"]
mod identity_administration;
#[path = "controller/identity_limit_fields.rs"]
mod identity_limit_fields;
#[path = "controller/collaboration_limit_fields.rs"]
mod collaboration_limit_fields;
#[path = "controller/codex_rate_limit_recovery.rs"]
mod codex_rate_limit_recovery;

#[path = "controller/identity_limit_fields_compatibility.rs"]
mod identity_limit_fields_compatibility;

#[path = "controller/publication_mcp.rs"]
mod publication_mcp;
