//! Actual controller admission/replay tests; no model, provider or worker process.
use super::*;
use crate::{
    limits::{ExecutionLimitOverrides, RuntimeLimits},
    protocol::*,
};
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(1);
fn path() -> PathBuf {
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "borg-controller-metadata-{}-{stamp}-{n}",
        std::process::id()
    ));
    std::fs::create_dir(&path).unwrap();
    path.join("tasks.db")
}

// Match the existing integration fixture's bounded ownership handoff. Dropping
// Controller closes the command channel; the SQL owner drains asynchronously.
async fn controller_owner_released(path: &std::path::Path) {
    let mut lock_path = path.as_os_str().to_os_string();
    lock_path.push(".controller-lock");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&lock_path)
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

fn coding(limits: &RuntimeLimits, agent: &str) -> TaskInput {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    TaskInput { instruction_source: None,
        pull_request: None,
        resolved_limits: None,
        repository_read: None,
        instruction: "metadata-only admission fixture".into(),
        definition: Definition {
            charter: None,
            limit_overrides: None,
            execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
            inference: Some(InferenceSettings::selected()),
            id: agent.into(),
            revision: "r1".into(),
            executor: ExecutorKind::Coding,
        },
        repository: Some(Repository {
            pull_request_target: None,
            registration: Some(crate::repository_catalogue::Revision {
                repository_id: "metadata-fixture".into(),
                registration_revision: "1".into(),
            }),
            url: String::new(),
            start_commit: "a".repeat(40),
            checkpoint_remote: String::new(),
        }),
        acceptance_commands: vec![],
        output_schema: None,
        limits: limits.task_defaults.at(now).unwrap(),
        dependencies: vec![],
        native_workflow: None,
    }
}
async fn register_fixture(controller: &Controller) {
    let registration = crate::repository_catalogue::Registration {
        pull_request_target: None,
        repository_id: "metadata-fixture".into(),
        registration_revision: "1".into(),
        display_name: "Metadata fixture".into(),
        source_endpoint: "https://example.invalid/repo.git".into(),
        checkpoint_endpoint: "origin".into(),
        source_store_key: "source.git".into(),
        checkpoint_store_key: "checkpoint.git".into(),
    };
    let reply = command(
        controller,
        Operation::RegisterRepository {
            operation_id: "register-metadata-fixture".into(),
            registration,
        },
    )
    .await;
    assert!(matches!(
        reply.outcome,
        Outcome::Ok {
            data: Data::RepositoryRegistration(_)
        }
    ));
}
async fn command(controller: &Controller, operation: Operation) -> Reply {
    controller
        .handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: format!("metadata-request-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
            command: operation,
        })
        .await
}
fn task(reply: &Reply) -> Task {
    match &reply.outcome {
        Outcome::Ok {
            data: Data::Task(t),
        } => (**t).clone(),
        v => panic!("expected task: {v:?}"),
    }
}
fn config(metadata: RuntimeLimits) -> ControllerConfig {
    ControllerConfig {
        limits: Arc::new(metadata),
        ..ControllerConfig::default()
    }
}

#[test]
fn metadata_is_closed_and_missing_policy_does_not_acquire_rust_defaults() {
    let seed = small_context_metadata();
    let mut value = serde_json::to_value(&seed).unwrap();
    value["controller"]
        .as_object_mut()
        .unwrap()
        .remove("queue_capacity");
    assert!(RuntimeLimits::parse(&serde_json::to_vec(&value).unwrap()).is_err());
    let mut value = serde_json::to_value(&seed).unwrap();
    value["controller"]["unregistered_policy"] = serde_json::json!(1);
    assert!(RuntimeLimits::parse(&serde_json::to_vec(&value).unwrap()).is_err());
}
#[tokio::test]
// Traceability scope: Restart and exact original-operation replay preserve the serialized accepted input; a new task acquires later defaults.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# An accepted task MUST retain its resolved execution-limit values.
async fn accepted_snapshot_and_original_receipt_survive_changed_global_defaults() {
    let p = path();
    let mut first = small_context_metadata();
    first.revision = "global-before".into();
    let original = coding(&first, "agent-snapshot");
    let op = Operation::SubmitTask {
        operation_id: "submit-original".into(),
        input: original.clone(),
    };
    let c = Controller::open_with_config(&p, config(first.clone())).unwrap();
    register_fixture(&c).await;
    let committed = command(&c, op.clone()).await;
    let accepted = task(&committed);
    assert!(original.resolved_limits.is_none());
    assert_eq!(
        accepted
            .input
            .resolved_limits
            .as_ref()
            .unwrap()
            .global_metadata
            .as_ref(),
        &first
    );
    let exact = serde_json::to_vec(&accepted.input).unwrap();
    drop(c);
    controller_owner_released(&p).await;
    let mut later = first.clone();
    later.revision = "global-after".into();
    later.execution.native.context_bytes *= 2;
    let c = Controller::open_with_config(&p, config(later.clone())).unwrap();
    register_fixture(&c).await;
    let replay = command(&c, op).await;
    assert_eq!(replay.outcome, committed.outcome);
    assert_eq!(replay.commit, committed.commit);
    assert_eq!(serde_json::to_vec(&task(&replay).input).unwrap(), exact);
    let next = task(
        &command(
            &c,
            Operation::SubmitTask {
                operation_id: "submit-later".into(),
                input: coding(&later, "agent-later"),
            },
        )
        .await,
    );
    assert_eq!(
        next.input
            .resolved_limits
            .as_ref()
            .unwrap()
            .global_metadata
            .as_ref(),
        &later
    );
    assert_eq!(
        accepted
            .input
            .execution_limits()
            .unwrap()
            .native
            .context_bytes,
        first.execution.native.context_bytes
    );
}
#[tokio::test]
// Traceability scope: Accepts an override and refuses different override values under the same agent definition revision.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# An agent definition MUST carry the execution-limit overrides specific to that agent revision.
async fn supplied_snapshot_refuses_and_agent_override_is_exact_immutable_revision_data() {
    let metadata = small_context_metadata();
    let c = Controller::open_with_config(path(), config(metadata.clone())).unwrap();
    register_fixture(&c).await;
    let mut forged = coding(&metadata, "agent-forged");
    forged.resolved_limits = Some(metadata.resolve_task(&forged).unwrap());
    let refusal = command(
        &c,
        Operation::SubmitTask {
            operation_id: "forged".into(),
            input: forged,
        },
    )
    .await;
    assert!(matches!(refusal.outcome,Outcome::Refused{ref code,..} if code=="invalid_input"));
    let mut input = coding(&metadata, "agent-override");
    let mut native = metadata.execution.native.clone();
    native.function_calls_per_response = 2;
    native.context_bytes *= 2;
    input.definition.limit_overrides = Some(Box::new(ExecutionLimitOverrides {
        native: Some(native.clone()),
        ..ExecutionLimitOverrides::default()
    }));
    let accepted = task(
        &command(
            &c,
            Operation::SubmitTask {
                operation_id: "override".into(),
                input: input.clone(),
            },
        )
        .await,
    );
    assert_eq!(&accepted.input.execution_limits().unwrap().native, &native);
    assert_eq!(
        accepted
            .input
            .resolved_limits
            .as_ref()
            .unwrap()
            .global_metadata
            .as_ref(),
        &metadata
    );
    input
        .definition
        .limit_overrides
        .as_mut()
        .unwrap()
        .native
        .as_mut()
        .unwrap()
        .function_calls_per_response = 3;
    let conflict = command(
        &c,
        Operation::SubmitTask {
            operation_id: "same-revision-other-policy".into(),
            input,
        },
    )
    .await;
    assert!(
        matches!(conflict.outcome,Outcome::Refused{ref code,..} if code=="definition_conflict")
    );
}
#[tokio::test]
async fn explicit_admission_metadata_changes_effective_command_bound() {
    let mut selected = small_context_metadata();
    selected.controller.max_acceptance_commands = 1;
    let c = Controller::open_with_config(path(), config(selected.clone())).unwrap();
    register_fixture(&c).await;
    let mut input = coding(&selected, "agent-command-bound");
    input.acceptance_commands = vec![vec!["a".into()], vec!["b".into()]];
    let refusal = command(
        &c,
        Operation::SubmitTask {
            operation_id: "two-commands".into(),
            input: input.clone(),
        },
    )
    .await;
    assert!(matches!(refusal.outcome,Outcome::Refused{ref code,..} if code=="invalid_input"));
    input.acceptance_commands.pop();
    let accepted = command(
        &c,
        Operation::SubmitTask {
            operation_id: "one-command".into(),
            input,
        },
    )
    .await;
    assert_eq!(task(&accepted).input.acceptance_commands.len(), 1);
}

#[tokio::test]
// Traceability scope: A previously committed refusal replays the same outcome and commit even when the later MCP input cap would reject that request.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# A retained operation MUST NOT be discarded solely because a later limit configuration is lower.
async fn retained_call_replay_ignores_new_mcp_policy_and_limits_view_separates_current_from_accepted()
 {
    let p = path();
    let mut first = small_context_metadata();
    first.revision = "mcp-before".into();
    let c = Controller::open_with_config(&p, config(first.clone())).unwrap();
    register_fixture(&c).await;
    let original = task(
        &command(
            &c,
            Operation::SubmitTask {
                operation_id: "mcp-task".into(),
                input: coding(&first, "agent-mcp"),
            },
        )
        .await,
    );
    // A committed refusal is an original outcome too. No worker process or
    // active-call authority is fabricated by this queued-task fixture.
    let call = crate::mcp::Invocation {
        operation_id: "retained-call".into(),
        logical_call_id: "logical-call".into(),
        caller: crate::mcp::Caller {
            task_id: original.id.clone(),
            attempt_id: "absent-attempt".into(),
            generation: 1,
            worker_id: "absent-worker".into(),
            incarnation: "absent-incarnation".into(),
        },
        tool: "collab_memory_set".into(),
        arguments: serde_json::json!({"key":"example","value":"x".repeat(2048)}),
    };
    assert!(call.validate(&first).is_ok());
    let op = Operation::CallCollaboration { call: call.clone() };
    let recorded = command(&c, op.clone()).await;
    assert!(matches!(recorded.outcome, Outcome::Refused { .. }));
    assert!(recorded.commit.is_some());
    drop(c);
    controller_owner_released(&p).await;
    let mut later = first.clone();
    later.revision = "mcp-after".into();
    later.interface.mcp_call_bytes = 512;
    assert!(call.validate(&later).is_err());
    let cfg = config(later.clone());
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    register_fixture(&c).await;
    let replay = command(&c, op).await;
    assert_eq!(replay.outcome, recorded.outcome);
    assert_eq!(replay.commit, recorded.commit);
    // The independent read-only connection must not race a live exclusive SQL
    // owner. Observe actual owner-lock release rather than assuming drop is sync.
    drop(c);
    controller_owner_released(&p).await;
    // Exercise the same borrowed accepted-policy store used by MCP, while the
    // current service configuration intentionally has a distinct revision.
    let sql = rusqlite::Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .unwrap();
    let bound = database::StoreContext::new(&sql, &first);
    let view = collaboration_queries::apply(
        &bound,
        &crate::collaboration::Query::Limits {
            task_id: Some(original.id),
        },
        &cfg,
    )
    .unwrap();
    let crate::collaboration::QueryResult::Limits { global, execution } = view else {
        panic!("limits view missing")
    };
    assert_eq!(*global, later);
    assert_eq!(*execution.unwrap().global_metadata, first);
}

#[test]
fn minimum_complete_result_reserve_matches_actual_encoder_and_overflow_refuses() {
    let mut transport = small_context_metadata().transport;
    for identity_bytes in [1, 17, 256] {
        transport.identity_bytes = identity_bytes;
        let bound = minimum_collaboration_result_bytes(&transport).unwrap();
        let actual = crate::mcp::CallResult {
            requires_quiescence: false,
            reply: Reply {
                request_id: "\\".repeat(identity_bytes),
                commit: Some(MutationCommit {
                    operation_id: "\\".repeat(identity_bytes),
                    controller_revision: i64::MAX as u64,
                }),
                observed_controller_revision: Some(i64::MAX as u64),
                outcome: metadata::complete_call_refusal(),
            },
        };
        assert_eq!(bound, serde_json::to_vec(&actual).unwrap().len());
    }
    transport.identity_bytes = usize::MAX;
    assert!(minimum_collaboration_result_bytes(&transport).is_err());
}

#[tokio::test]
async fn list_tasks_uses_list_page_bytes_independently_of_command_page_bytes() {
    for small_list in [false, true] {
        let mut selected = small_context_metadata();
        if small_list {
            selected.controller.list_page_bytes = 1;
        } else {
            selected.controller.command_page_bytes = 1;
        }
        let c = Controller::open_with_config(path(), config(selected.clone())).unwrap();
        register_fixture(&c).await;
        let accepted = task(
            &command(
                &c,
                Operation::SubmitTask {
                    operation_id: "list-policy-task".into(),
                    input: coding(&selected, "list-policy-agent"),
                },
            )
            .await,
        );
        let listed = command(
            &c,
            Operation::ListTasks {
                after: None,
                limit: 1,
            },
        )
        .await;
        let Outcome::Ok {
            data: Data::Tasks(tasks),
        } = listed.outcome
        else {
            panic!("expected bounded task page: {:?}", listed.outcome);
        };
        if small_list {
            assert!(
                tasks.is_empty(),
                "list metadata must stop an oversized item"
            );
        } else {
            assert_eq!(
                tasks.len(),
                1,
                "command metadata must not truncate task lists"
            );
            assert_eq!(tasks[0].id, accepted.id);
            assert_eq!(tasks[0].input, accepted.input);
        }
    }
}

fn assert_limiting_field(reply: &Reply, expected_code: &str, field: &str) {
    let Outcome::Refused { code, message } = &reply.outcome else {
        panic!("expected refusal: {reply:?}")
    };
    assert_eq!(code, expected_code);
    assert!(message.contains(field), "missing {field} in {message}");
}
#[tokio::test]
async fn admission_refusals_identify_the_exact_limiting_metadata_field() {
    for case in 0..8 {
        let mut selected = small_context_metadata();
        let mut input = coding(&selected, "field-agent");
        let (code, field) = match case {
            0 => {
                selected.controller.max_task_input_bytes = 1;
                ("capacity", "controller.max_task_input_bytes")
            }
            1 => {
                input.limits.output_bytes = selected.controller.max_task_output_bytes + 1;
                ("invalid_input", "controller.max_task_output_bytes")
            }
            2 => {
                input.limits.result_bytes = selected.controller.max_task_result_bytes + 1;
                ("invalid_input", "controller.max_task_result_bytes")
            }
            3 => {
                selected.controller.max_delegation_depth = 1;
                selected.task_defaults.max_delegation_depth = 1;
                input.limits.max_delegation_depth = 2;
                ("invalid_input", "controller.max_delegation_depth")
            }
            4 => {
                selected.controller.max_dependencies = 1;
                input.dependencies = vec!["missing-one".into(), "missing-two".into()];
                ("invalid_input", "controller.max_dependencies")
            }
            5 => {
                selected.controller.max_acceptance_commands = 1;
                input.acceptance_commands = vec![vec!["a".into()], vec!["b".into()]];
                ("invalid_input", "controller.max_acceptance_commands")
            }
            6 => {
                selected.controller.max_acceptance_arguments = 1;
                input.acceptance_commands = vec![vec!["a".into(), "b".into()]];
                ("invalid_input", "controller.max_acceptance_arguments")
            }
            7 => {
                selected.controller.max_acceptance_argument_bytes = 1;
                input.acceptance_commands = vec![vec!["ab".into()]];
                ("invalid_input", "controller.max_acceptance_argument_bytes")
            }
            _ => unreachable!(),
        };
        let c = Controller::open_with_config(path(), config(selected)).unwrap();
        register_fixture(&c).await;
        let op = Operation::SubmitTask {
            operation_id: "field-submit".into(),
            input,
        };
        let original = command(&c, op.clone()).await;
        assert_limiting_field(&original, code, field);
        assert!(original.commit.is_some());
        let replay = command(&c, op).await;
        assert_eq!(replay.outcome, original.outcome);
        assert_eq!(replay.commit, original.commit);
    }
}

fn small_context_metadata() -> RuntimeLimits { let mut p=RuntimeLimits::seed().unwrap(); p.execution.native.context_bytes=65_536; p }

#[tokio::test]
async fn lazy_legacy_queue_still_refuses_full_and_releases_capacity_on_dequeue() {
    let p = path();
    let mut metadata = RuntimeLimits::seed().unwrap();
    metadata.controller.queue_capacity = 1;
    assert!(metadata.controller.store_retry.is_none());
    let mut controller = Controller::open_with_config(&p, config(metadata)).unwrap();
    let (sender, receiver) = mpsc::channel();
    // Own the queue receiver to place a deterministic boundary before dequeue.
    // Dropping the original sender lets the real database owner drain normally.
    let inner = Arc::get_mut(&mut controller.inner).unwrap();
    inner.sender = sender;
    let message = || Message::Revision { reply: oneshot::channel().0 };
    assert!(inner.send(message()).is_ok());
    assert!(matches!(inner.send(message()),Err(mpsc::TrySendError::Full(_))));
    let queued = receiver.try_recv().map(|envelope|envelope.message).unwrap();
    assert!(matches!(queued,Message::Revision {..}));
    assert!(inner.send(message()).is_ok(),"dequeue must release the legacy permit");
    drop(receiver);
    assert!(matches!(inner.send(message()),Err(mpsc::TrySendError::Disconnected(_))));
    drop(controller);
    controller_owner_released(&p).await;
}


// Traceability scope: Small controlled reports exercise actual admission,
// durable result acceptance, helper validation and reopen under the exact
// full selected metadata. This is not a provider or maximum-payload test.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# An accepted task MUST retain its resolved execution-limit values.
#[tokio::test]
async fn maximum_session_profile_small_progress_result_and_restart_remain_durable() {
    use crate::charter::*;

    Box::pin(async {
        let selected = RuntimeLimits::parse(include_bytes!(
            "../../tests/fixtures/maximum-session-controller-metadata-04.json"
        )).unwrap();
        let mut cfg = config(selected.clone());
        cfg.validation_executable = PathBuf::from(std::env::var_os("BORG_TEST_RUNTIME")
            .expect("existing same-build ordinary runtime helper"));
        let p = path();
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        let mut input = coding(&selected, "maximum-profile-native");
        input.definition.executor = ExecutorKind::Native;
        // The selected maximum profile enables charter policy. Supply its
        // original source rather than changing the policy to admit this fixture.
        input.definition.charter = Some(DefinitionCharter::Inline {
            source: CharterSource {
                files: vec![CharterFile {
                    file: "charter.md".into(),
                    text: "# Purpose\n\nThe agent MUST preserve accepted metadata.\n".into(),
                }],
                clauses: vec![CharterClauseRef {
                    file: "charter.md".into(),
                    section: "Purpose".into(),
                    sentence: "The agent MUST preserve accepted metadata.".into(),
                }],
            },
        });
        input.repository = None;
        input.output_schema = Some(serde_json::json!({"type":"object","required":["answer"],
            "properties":{"answer":{"type":"integer"}},"additionalProperties":false}));
        let submit = Operation::SubmitTask { operation_id:"maximum-profile-submit".into(), input };
        let submitted = command(&c, submit.clone()).await;
        let initial = task(&submitted);
        let pinned = serde_json::to_vec(&initial.input).unwrap();
        assert_eq!(*initial.input.resolved_limits.as_ref().unwrap().global_metadata, selected);
        assert_eq!(initial.input.execution_limits().unwrap(), &selected.execution);
        let w = Worker { id:"maximum-profile-worker".into(), incarnation:"original".into(), coding_capacity:0, native_capacity:1 };
        assert!(matches!(command(&c, Operation::RegisterWorker {
            operation_id:"maximum-profile-register".into(), worker:w.clone()
        }).await.outcome, Outcome::Ok { data:Data::Worker(_) }));
        let dispatch = command(&c, Operation::AwaitCommands {
            operation_id:"maximum-profile-await".into(), worker_id:w.id.clone(),
            incarnation:w.incarnation.clone(), acknowledged:vec![]
        }).await;
        let a = match dispatch.outcome {
            Outcome::Ok { data:Data::Commands(commands) } => commands.iter().find_map(|v|match v {
                WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == initial.id => Some(attempt.clone()), _ => None
            }).expect("original Native attempt"), other => panic!("{other:?}")
        };
        let report = |id:String, observation| Operation::ObserveWorker { report:Report {
            report_id:id, worker_id:a.worker_id.clone(), incarnation:a.worker_incarnation.clone(),
            task_id:a.task_id.clone(), attempt_id:a.id.clone(), generation:a.generation, observation
        }};
        task(&command(&c, report("maximum-profile-started".into(), Observation::Started { thread_id:None, turn_id:None })).await);
        let progressed = task(&command(&c, report("maximum-profile-progress".into(), Observation::Progress {
            text:"small progress".into(), output:"small output".into(), truncated:false
        })).await);
        assert_eq!(progressed.progress, "small progress");
        assert_eq!(progressed.recent_output, "small output");
        task(&command(&c, report(crate::native_loop_contract::reservation_id(&a.id,1), Observation::Usage {
            model_requests:1, input_tokens:Some(4), output_tokens:Some(2), native_diagnostic_reserved:true
        })).await);
        let binding = match command(&c, Operation::QueryCharter {
            query: CharterQuery::Session { session_id: a.session_id.clone() },
        }).await.outcome {
            Outcome::Ok { data: Data::CharterQuery(CharterQueryResult::Session(Some(value))) } => value,
            other => panic!("expected accepted charter binding: {other:?}"),
        };
        let caller = crate::mcp::Caller {
            task_id: a.task_id.clone(), attempt_id: a.id.clone(), generation: a.generation,
            worker_id: a.worker_id.clone(), incarnation: a.worker_incarnation.clone(),
        };
        let charter = |id: &str, observation| Operation::ObserveCharter {
            operation_id: id.into(), caller: caller.clone(), observation,
        };
        let begin = charter("maximum-profile-charter-begin", CharterObservation::BeginTurn {
            session_id: a.session_id.clone(), expected_revision: binding.session.revision,
            request_index: 1,
        });
        let begun = command(&c, begin.clone()).await;
        let boundary = match &begun.outcome {
            Outcome::Ok { data: Data::Charter(CharterOutcome::Boundary(value)) } => value.clone(),
            other => panic!("expected original charter boundary: {other:?}"),
        };
        assert_eq!(boundary.effective, binding.charter);
        let diagnostic = command(&c, report(crate::native_loop_contract::diagnostic_id(&a.id,1), Observation::NativeRequestDiagnostic {
            request_index:1, endpoint:"https://api.openai.com/v1/responses".into(),
            model:initial.input.definition.required_inference().unwrap().model.clone(),
            request_binding:Some(crate::native::ProviderRequestBinding { bytes:2,
                sha256:"44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a".into() }),
            observed_outcome:crate::native_loop_contract::NativeRequestObservedOutcome::Completed, failure:None
        })).await;
        assert!(matches!(diagnostic.outcome, Outcome::Ok { data:Data::Acknowledged }), "{diagnostic:?}");
        // These controlled native observations retain the same original request
        // and no-tool reply required by ordinary completion under charter policy.
        let reply = charter("maximum-profile-charter-reply", CharterObservation::Reply {
            turn_id: boundary.turn_id.clone(),
            reply: CharterReplyObservation {
                output: Some(serde_json::json!([{"id":"maximum-profile-output","type":"message",
                    "content":[{"type":"output_text","text":"{\"answer\":42}"}]}])),
                output_reference: None,
                response_id: Some("maximum-profile-controlled-response".into()),
                output_item_ids: vec!["maximum-profile-output".into()],
                tool_call_ids: vec![],
                declaration: Some(serde_json::to_value(DeclaredCharterReferences {
                    revision: boundary.effective.reference.clone(),
                    clauses: boundary.effective.source.clauses.clone(),
                }).unwrap()),
            },
        });
        let replied = command(&c, reply.clone()).await;
        match &replied.outcome {
            Outcome::Ok { data: Data::Charter(CharterOutcome::Reply(value)) } =>
                assert!(value.audit.findings.is_empty()),
            other => panic!("expected retained charter reply: {other:?}"),
        }
        let close = charter("maximum-profile-charter-close", CharterObservation::CompleteTurn {
            turn_id: boundary.turn_id.clone(), results: vec![],
        });
        let closed = command(&c, close.clone()).await;
        assert!(matches!(&closed.outcome, Outcome::Ok {
            data: Data::Charter(CharterOutcome::Turn(value))
        } if value.completed_by.as_deref() == Some("maximum-profile-charter-close")), "{closed:?}");
        task(&command(&c, report(crate::native_loop_contract::completion_id(&a.id), Observation::NativeLoopCompleted {
            model_requests:1, calls:vec![], requests:vec![], final_response_index:None
        })).await);
        task(&command(&c, report("maximum-profile-executor".into(), Observation::ExecutorOutcome {
            success:true, summary:"controlled completion".into()
        })).await);
        task(&command(&c, report("maximum-profile-stopped".into(), Observation::ProcessTreeStopped)).await);
        let result_operation = report("maximum-profile-result".into(), Observation::ResultCandidate {
            checks_passed:true, summary:"small accepted result".into(), checkpoint:None,
            validation:vec![crate::native_loop_contract::evidence(&a.id)], output:Some(serde_json::json!({"answer":42}))
        });
        let result_reply = command(&c, result_operation.clone()).await;
        let accepted = task(&result_reply);
        assert_eq!(accepted.status, TaskStatus::Succeeded);
        assert!(accepted.attempt.as_ref().unwrap().stopped);
        assert!(!accepted.attempt.as_ref().unwrap().uncertain);
        assert_eq!(accepted.result.as_ref().unwrap().output, Some(serde_json::json!({"answer":42})));
        assert_eq!(serde_json::to_vec(&accepted.input).unwrap(), pinned);
        drop(c);
        controller_owner_released(&p).await;
        let c = Controller::open_with_config(&p, cfg).unwrap();
        assert_eq!(task(&command(&c, Operation::GetTask { task_id:accepted.id.clone() }).await), accepted);
        for (operation, original) in [(submit, submitted), (begin, begun), (reply, replied),
            (close, closed), (result_operation, result_reply)] {
            let replay = command(&c, operation).await;
            assert_eq!(replay.outcome, original.outcome);
            assert_eq!(replay.commit, original.commit);
        }
        drop(c);
        controller_owner_released(&p).await;
    }).await;
}
