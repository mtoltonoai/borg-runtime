//! Bach schedules request-level interleavings; Bolero supplies bounded inputs and
//! seeds. The resulting traces are replayed against the real Controller/SQLite
//! boundary and compared with small independent semantic models. The controller's
//! OS thread, SQLite internals, Tokio, providers and worker processes do not run
//! under Bach here. These tests are not external-dependency parity, crash-durability
//! evidence, or exhaustive schedule coverage.
use bach::ext::*;
use borg_runtime::{
    controller::{Controller, ControllerConfig},
    protocol::*,
};
use serde_json::json;
use std::{
    cell::RefCell,
    collections::BTreeSet,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

static NEXT: AtomicU64 = AtomicU64::new(1);
const NOW: u64 = 1_000;
const DEADLINE: u64 = 1_000_000;

fn controller() -> Controller {
    let directory = std::env::temp_dir().join(format!(
        "borg-simulation-replay-{}-{}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&directory).unwrap();
    Controller::open_with_config(
        directory.join("tasks.db"),
        ControllerConfig {
            validation_executable: std::path::PathBuf::from(env!("CARGO_BIN_EXE_borg-runtime")),
            limits: {
                let mut p = borg_runtime::limits::RuntimeLimits::seed().unwrap();
                p.controller.command_wait_ms = 5;
                Arc::new(p)
            },
            clock: Arc::new(|| NOW),
            ..ControllerConfig::default()
        },
    )
    .unwrap()
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn input(agent: &str, label: u8) -> TaskInput {
    TaskInput { instruction_source: None,
        pull_request: None,
        resolved_limits: None,
        repository_read: None,
        instruction: format!("answer for input {label}"),
        definition: Definition {
            charter: None,
            limit_overrides: None,
            inference: Some(InferenceSettings::selected()),
            execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
            id: agent.into(),
            revision: "1".into(),
            executor: ExecutorKind::Native,
        },
        repository: None,
        acceptance_commands: vec![],
        output_schema: Some(
            json!({"type":"object","required":["answer"],"properties":{"answer":{"type":"integer"}},"additionalProperties":false}),
        ),
        limits: Limits {
            deadline_unix_ms: DEADLINE,
            model_requests: 16,
            output_bytes: 4096,
            result_bytes: borg_runtime::limits::RuntimeLimits::seed()
                .unwrap()
                .task_defaults
                .result_bytes,
            max_delegation_depth: 2,
        },
        dependencies: vec![],
        native_workflow: None,
    }
}

async fn call(controller: &Controller, command: Operation) -> Outcome {
    controller
        .handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: format!(
                "simulation-request-{}",
                NEXT.fetch_add(1, Ordering::Relaxed)
            ),
            command,
        })
        .await
        .outcome
}
fn task(outcome: Outcome) -> Task {
    match outcome {
        Outcome::Ok {
            data: Data::Task(task),
        } => *task,
        other => panic!("expected task: {other:?}"),
    }
}
fn refused(outcome: Outcome, expected: &str) {
    match outcome {
        Outcome::Refused { code, .. } => assert_eq!(code, expected),
        other => panic!("expected {expected}: {other:?}"),
    }
}
async fn get(controller: &Controller, id: &str) -> Task {
    task(call(controller, Operation::GetTask { task_id: id.into() }).await)
}
async fn submit(controller: &Controller, operation: &str, input: TaskInput) -> Task {
    task(
        call(
            controller,
            Operation::SubmitTask {
                operation_id: operation.into(),
                input: (input).into(),
            },
        )
        .await,
    )
}
fn worker(capacity: usize) -> Worker {
    Worker {
        id: "simulation-worker".into(),
        incarnation: "incarnation-1".into(),
        native_capacity: capacity,
        coding_capacity: 0,
    }
}
async fn register(controller: &Controller, worker: &Worker) {
    assert!(matches!(
        call(
            controller,
            Operation::RegisterWorker {
                operation_id: format!("register-{}-{}", worker.id, worker.incarnation),
                worker: worker.clone()
            }
        )
        .await,
        Outcome::Ok {
            data: Data::Worker(_)
        }
    ));
}
async fn commands(controller: &Controller, worker: &Worker) -> Vec<WorkerCommand> {
    match call(
        controller,
        Operation::AwaitCommands {
            operation_id: format!("command-exchange-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
            worker_id: worker.id.clone(),
            incarnation: worker.incarnation.clone(),
            acknowledged: vec![],
        },
    )
    .await
    {
        Outcome::Ok {
            data: Data::Commands(commands),
        } => commands,
        Outcome::Pending { .. } => vec![],
        other => panic!("expected commands: {other:?}"),
    }
}
fn attempts(commands: &[WorkerCommand]) -> Vec<Attempt> {
    commands
        .iter()
        .filter_map(|command| match command {
            WorkerCommand::RunAttempt { attempt, .. } => Some(attempt.clone()),
            _ => None,
        })
        .collect()
}
async fn observe(
    controller: &Controller,
    attempt: &Attempt,
    suffix: &str,
    observation: Observation,
) -> Outcome {
    let report_id = match &observation {
        Observation::Usage { model_requests, .. } => {
            borg_runtime::native_loop_contract::reservation_id(&attempt.id, *model_requests)
        }
        Observation::NativeLoopCompleted { .. } => {
            borg_runtime::native_loop_contract::completion_id(&attempt.id)
        }
        _ => format!("{}-{suffix}", attempt.id),
    };
    call(
        controller,
        Operation::ObserveWorker {
            report: Report {
                report_id,
                worker_id: attempt.worker_id.clone(),
                incarnation: attempt.worker_incarnation.clone(),
                task_id: attempt.task_id.clone(),
                attempt_id: attempt.id.clone(),
                generation: attempt.generation,
                observation,
            },
        },
    )
    .await
}
async fn prepare_result(controller: &Controller, attempt: &Attempt) {
    task(
        observe(
            controller,
            attempt,
            "started",
            Observation::Started {
                thread_id: None,
                turn_id: None,
            },
        )
        .await,
    );
    task(
        observe(
            controller,
            attempt,
            "usage",
            Observation::Usage {
                native_diagnostic_reserved: false,
                model_requests: 1,
                input_tokens: Some(3),
                output_tokens: Some(2),
            },
        )
        .await,
    );
    // Current accepted profile requires its exact acknowledged reservation and
    // completion receipt. These remain controlled controller observations, not
    // an executed provider loop or worker-process qualification.
    task(
        observe(
            controller,
            attempt,
            "completed",
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
        observe(
            controller,
            attempt,
            "executor",
            Observation::ExecutorOutcome {
                success: true,
                summary: "controlled executor observation".into(),
            },
        )
        .await,
    );
}
fn candidate(attempt: &Attempt) -> Observation {
    Observation::ResultCandidate {
        checks_passed: true,
        summary: "validated candidate".into(),
        checkpoint: None,
        validation: vec![borg_runtime::native_loop_contract::evidence(&attempt.id)],
        output: Some(json!({"answer":42})),
    }
}
async fn finish(controller: &Controller, attempt: &Attempt) {
    prepare_result(controller, attempt).await;
    task(
        observe(
            controller,
            attempt,
            "stopped",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    assert_eq!(
        task(observe(controller, attempt, "result", candidate(attempt)).await).status,
        TaskStatus::Succeeded
    );
}

/// Each actor retains its own message order. Bach controls the ordering between
/// actors. All pure trace construction happens inside the actual Bach executor;
/// replay below makes no claim to simulate the database's internal scheduling.
fn schedule<T: Copy + 'static>(seed: u64, actors: Vec<Vec<(u8, T)>>) -> Vec<T> {
    let trace = Rc::new(RefCell::new(Vec::new()));
    let mut simulation = bach::environment::default::Runtime::new().with_seed(seed);
    simulation.run(|| {
        for actor in actors {
            let trace = trace.clone();
            async move {
                assert!(bach::is_active());
                for (delay, action) in actor {
                    bach::time::sleep(Duration::from_millis(u64::from(delay % 8))).await;
                    bach::task::yield_now().await;
                    trace.borrow_mut().push(action);
                }
            }
            .primary()
            .spawn();
        }
    });
    let answer = trace.borrow().clone();
    answer
}

//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# An exact retry of a committed mutating request MUST return its recorded outcome.
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# Reusing a mutating request's identity with different input MUST fail without changing its recorded outcome.
#[test]
fn bach_generated_duplicate_conflict_traces_preserve_the_first_committed_outcome() {
    let runtime = runtime();
    bolero::check!()
        .with_type::<[u8; 8]>()
        .with_iterations(24)
        .for_each(|bytes| {
            let trace = schedule(
                u64::from_le_bytes(*bytes),
                vec![
                    vec![(bytes[0], 0u8), (bytes[1], 0), (bytes[2], 1)],
                    vec![(bytes[3], 1u8), (bytes[4], 1), (bytes[5], 0)],
                ],
            );
            assert_eq!(trace.len(), 6);
            // Independent oracle: the first successful input owns this operation ID.
            // Later equal inputs replay that immutable outcome; unequal inputs refuse.
            let selected = trace[0];
            runtime.block_on(async {
                let controller = controller();
                let mut original: Option<Task> = None;
                for label in &trace {
                    let outcome = call(
                        &controller,
                        Operation::SubmitTask {
                            operation_id: "one-logical-submit".into(),
                            input: (input("agent", *label)).into(),
                        },
                    )
                    .await;
                    if *label == selected {
                        let observed = task(outcome);
                        if let Some(expected) = &original {
                            assert_eq!(&observed, expected, "trace={trace:?}");
                        } else {
                            original = Some(observed);
                        }
                    } else {
                        refused(outcome, "operation_conflict");
                    }
                }
                let original = original.unwrap();
                assert_eq!(get(&controller, &original.id).await, original);
                match call(
                    &controller,
                    Operation::ListTasks {
                        after: None,
                        limit: 10,
                    },
                )
                .await
                {
                    Outcome::Ok {
                        data: Data::Tasks(tasks),
                    } => assert_eq!(tasks, vec![original]),
                    other => panic!("expected one durable task: {other:?}"),
                }
            });
        });
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TerminalAction {
    Cancel,
    Complete,
}

async fn replay_terminal_order(trace: &[TerminalAction]) {
    let controller = controller();
    let worker = worker(1);
    register(&controller, &worker).await;
    let initial = submit(&controller, "terminal-task", input("agent", 0)).await;
    let old = attempts(&commands(&controller, &worker).await).remove(0);
    prepare_result(&controller, &old).await;
    let expected = if trace[0] == TerminalAction::Cancel {
        TaskStatus::Cancelled
    } else {
        TaskStatus::Succeeded
    };
    for action in trace {
        let current = get(&controller, &initial.id).await;
        match action {
            TerminalAction::Cancel => {
                let result = task(
                    call(
                        &controller,
                        Operation::CancelTask {
                            operation_id: "cancel-original".into(),
                            task_id: initial.id.clone(),
                            expected_revision: current.revision,
                        },
                    )
                    .await,
                );
                assert_eq!(result.status, expected);
                if expected == TaskStatus::Cancelled {
                    assert!(
                        !result.attempt.as_ref().unwrap().stopped,
                        "intent is not termination"
                    );
                    assert_eq!(
                        result.attempt.as_ref().unwrap().phase,
                        AttemptPhase::StopRequested
                    );
                } else {
                    assert_eq!(result, current, "late cancellation cannot replace success");
                }
            }
            TerminalAction::Complete => {
                if expected == TaskStatus::Succeeded {
                    // The success-first trace now includes actual stop evidence
                    // before acceptance. In the cancel-first trace this remains
                    // a late result without termination proof; the existing
                    // retry-before-stop refusal below is still exercised.
                    task(
                        observe(
                            &controller,
                            &old,
                            "stopped",
                            Observation::ProcessTreeStopped,
                        )
                        .await,
                    );
                }
                let result = observe(&controller, &old, "result", candidate(&old)).await;
                if expected == TaskStatus::Cancelled {
                    refused(result, "invalid_transition");
                } else {
                    assert_eq!(task(result).status, TaskStatus::Succeeded);
                }
            }
        }
    }
    let settled = get(&controller, &initial.id).await;
    assert_eq!(settled.status, expected, "trace={trace:?}");
    assert_eq!(settled.result.is_some(), expected == TaskStatus::Succeeded);
    if expected == TaskStatus::Cancelled {
        refused(
            call(
                &controller,
                Operation::RetryTask {
                    operation_id: "too-early-retry".into(),
                    task_id: initial.id.clone(),
                    expected_revision: settled.revision,
                },
            )
            .await,
            "reconciliation_required",
        );
        task(
            observe(
                &controller,
                &old,
                "stopped",
                Observation::ProcessTreeStopped,
            )
            .await,
        );
        let stopped = get(&controller, &initial.id).await;
        let retry = task(
            call(
                &controller,
                Operation::RetryTask {
                    operation_id: "settled-retry".into(),
                    task_id: initial.id.clone(),
                    expected_revision: stopped.revision,
                },
            )
            .await,
        );
        assert_eq!(retry.id, initial.id);
        assert_eq!(retry.session_id, initial.session_id);
        let next = attempts(&commands(&controller, &worker).await).remove(0);
        assert_ne!(next.id, old.id);
        assert_eq!(next.session_id, old.session_id);
        assert_eq!(next.session_id, initial.session_id);
        assert!(next.generation > old.generation);
        assert_eq!(next.deadline_unix_ms, old.deadline_unix_ms);
        let before_stale = get(&controller, &initial.id).await;
        refused(
            observe(
                &controller,
                &old,
                "late-progress",
                Observation::Progress {
                    text: "stale owner".into(),
                    output: "must not survive".into(),
                    truncated: false,
                },
            )
            .await,
            "stale_generation",
        );
        refused(
            call(
                &controller,
                Operation::SetMemory {
                    operation_id: "stale-memory".into(),
                    task_id: old.task_id.clone(),
                    attempt_id: old.id.clone(),
                    generation: old.generation,
                    key: "memory".into(),
                    expected_revision: None,
                    value: json!("stale owner"),
                },
            )
            .await,
            "stale_generation",
        );
        assert_eq!(get(&controller, &initial.id).await, before_stale);
        assert!(matches!(
            call(
                &controller,
                Operation::GetMemory {
                    agent_id: "agent".into(),
                    key: "memory".into()
                }
            )
            .await,
            Outcome::Ok {
                data: Data::Memory(None)
            }
        ));
    } else {
        refused(
            call(
                &controller,
                Operation::RetryTask {
                    operation_id: "cannot-reopen-success".into(),
                    task_id: initial.id,
                    expected_revision: settled.revision,
                },
            )
            .await,
            "invalid_transition",
        );
    }
}

//= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
//= type=test
//# Successful completion and cancellation MUST be ordered by the controller's committed task revision.
//= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
//= type=test
//# An automatic retry MUST wait until prior execution is known unable to continue.
//= spec/impl/v2/implementation.md#worker-commands-do-not-create-duplicate-execution
//= type=test
//# A stale-generation report MUST NOT change the current task result, checkpoint or agent memory.
#[test]
fn bach_terminal_orders_match_committed_status_and_fence_old_generation() {
    let runtime = runtime();
    // Cover both semantic orders independently of randomly generated schedules.
    for trace in [
        [TerminalAction::Cancel, TerminalAction::Complete],
        [TerminalAction::Complete, TerminalAction::Cancel],
    ] {
        runtime.block_on(replay_terminal_order(&trace));
    }
    bolero::check!()
        .with_type::<[u8; 8]>()
        .with_iterations(16)
        .for_each(|bytes| {
            let trace = schedule(
                u64::from_le_bytes(*bytes),
                vec![
                    vec![(bytes[0], TerminalAction::Cancel)],
                    vec![(bytes[1], TerminalAction::Complete)],
                ],
            );
            assert_eq!(trace.len(), 2);
            runtime.block_on(replay_terminal_order(&trace));
        });
}

//= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
//= type=test
//# A task MUST start only after its prerequisites have succeeded.
//= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
//= type=test
//# Task admission MUST select the oldest eligible task within the requested executor's available capacity.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# A session MUST have at most one active execution attempt at a time.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# An agent identity MUST support multiple concurrent sessions.
#[test]
fn bolero_admission_matches_an_independent_queue_model_under_bach_completion_orders() {
    let runtime = runtime();
    let replay = |bytes: &[u8; 8]| {
        runtime.block_on(async {
            let controller = controller();
            let capacity = usize::from(bytes[0] % 3) + 1;
            let worker = worker(capacity);
            register(&controller, &worker).await;
            let mut tasks: Vec<Task> = Vec::new();
            let mut predecessors = Vec::new();
            for i in 0..5 {
                let mut request = input(&format!("agent-{}", bytes[i + 1] % 3), i as u8);
                let dependency = if i != 0 && bytes[i + 1] & 4 != 0 {
                    Some(usize::from(bytes[i + 1]) % i)
                } else {
                    None
                };
                if let Some(index) = dependency {
                    request.dependencies.push(tasks[index].id.clone());
                }
                predecessors.push(dependency);
                tasks.push(submit(&controller, &format!("submit-{i}"), request).await);
            }
            assert_eq!(
                tasks
                    .iter()
                    .map(|t| &t.session_id)
                    .collect::<BTreeSet<_>>()
                    .len(),
                tasks.len()
            );
            for task in &tasks {
                assert_eq!(task.assigned_agent_id, task.input.definition.id);
                assert_ne!(task.session_id, task.assigned_agent_id);
                assert_ne!(task.session_id, task.id);
            }
            let mut completed = BTreeSet::new();
            for round in 0..5 {
                if completed.len() == tasks.len() {
                    break;
                }
                // Model only the observable rule: oldest eligible, distinct session,
                // succeeded prerequisites and remaining capacity. No SQL/receipt
                // implementation or controller transition code is reused.
                let mut selected = Vec::new();
                let mut busy = BTreeSet::new();
                for (i, task) in tasks.iter().enumerate() {
                    if !completed.contains(&i)
                        && predecessors[i].is_none_or(|p| completed.contains(&p))
                        && busy.insert(task.session_id.clone())
                    {
                        selected.push(i);
                        if selected.len() == capacity {
                            break;
                        }
                    }
                }
                assert!(!selected.is_empty());
                let actual = attempts(&commands(&controller, &worker).await);
                assert_eq!(
                    actual.iter().map(|a| a.task_id.clone()).collect::<Vec<_>>(),
                    selected
                        .iter()
                        .map(|i| tasks[*i].id.clone())
                        .collect::<Vec<_>>(),
                    "input={bytes:?} round={round}"
                );
                assert!(actual.len() <= capacity);
                assert_eq!(
                    actual
                        .iter()
                        .map(|a| &a.session_id)
                        .collect::<BTreeSet<_>>()
                        .len(),
                    actual.len()
                );
                for (attempt, selected_index) in actual.iter().zip(&selected) {
                    assert_eq!(attempt.session_id, tasks[*selected_index].session_id);
                }
                if bytes == &[2, 0, 0, 0, 0, 0, 0, 0] && round == 0 {
                    assert_eq!(
                        actual.len(),
                        3,
                        "same-agent sessions must use all three available slots"
                    );
                    assert_eq!(
                        selected
                            .iter()
                            .map(|i| &tasks[*i].assigned_agent_id)
                            .collect::<BTreeSet<_>>()
                            .len(),
                        1
                    );
                }
                let actors = actual
                    .iter()
                    .enumerate()
                    .map(|(i, _)| vec![(bytes[(i + round) % 8], i)])
                    .collect();
                let order = schedule(u64::from_le_bytes(*bytes) ^ round as u64, actors);
                assert_eq!(order.len(), actual.len());
                for i in order {
                    finish(&controller, &actual[i]).await;
                    completed.insert(selected[i]);
                }
            }
            assert_eq!(completed.len(), tasks.len());
            for task in tasks {
                let settled = get(&controller, &task.id).await;
                assert_eq!(settled.status, TaskStatus::Succeeded);
                assert_eq!(settled.session_id, task.session_id);
                assert_eq!(
                    settled.attempt.as_ref().unwrap().session_id,
                    task.session_id
                );
            }
        });
    };
    replay(&[2, 0, 0, 0, 0, 0, 0, 0]);
    bolero::check!()
        .with_type::<[u8; 8]>()
        .with_iterations(16)
        .for_each(replay);
}
