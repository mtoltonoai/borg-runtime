//! Same-task execution handoffs through the real controller and original receipts.
//! Controlled worker reports do not claim a provider call or physical executor run.
use super::*;
use borg_runtime::{
    charter::{
        CharterClauseRef, CharterFile, CharterQuery, CharterQueryResult, CharterSource,
        DefinitionCharter,
    },
    collaboration::{Query, QueryResult},
    limits::{CharterLimits, DefinitionLimitField, DefinitionUpdatePolicy},
    mcp::{Caller, Invocation},
};

fn handoff_config() -> ControllerConfig {
    let mut cfg = config();
    let limits = Arc::make_mut(&mut cfg.limits);
    limits.charter = Some(CharterLimits {
        record_bytes: 65_536,
        source_bytes: 16_384,
        files: 4,
        clauses: 32,
        metadata_bytes: 8192,
        references: 32,
        reply_items: 32,
        tool_calls: 8,
        reference_depth: 4,
        records: 1024,
        page_items: 4,
        page_bytes: 65_536,
    });
    limits.definition_updates = Some(DefinitionUpdatePolicy {
        revision: "handoff-policy-1".into(),
        allowed_fields: vec![DefinitionLimitField::CodingSummaryChars],
    });
    cfg
}
fn charter(label: &str) -> DefinitionCharter {
    let sentence = format!("The agent MUST preserve {label}.");
    DefinitionCharter::Inline {
        source: CharterSource {
            files: vec![CharterFile {
                file: "charter.md".into(),
                text: format!("# Purpose\n\n{sentence}\n"),
            }],
            clauses: vec![CharterClauseRef {
                file: "charter.md".into(),
                section: "Purpose".into(),
                sentence,
            }],
        },
    }
}
fn configured_input(label: &str) -> (TaskInput, Definition) {
    let mut input = native(&format!("{label}-triage"));
    input.definition.charter = Some(charter("triage-private-source"));
    let mut target = coding(&format!("{label}-engineer"));
    target.definition.charter = Some(charter("engineer-own-source"));
    input.native_workflow = Some(Box::new(NativeWorkflow::Reassign {
        target: CodingTaskTemplate {
            instruction_source: None,
            pull_request: None,
            instruction_prefix: String::new(),
            definition: target.definition.clone(),
            repository: target.repository.unwrap(),
            acceptance_commands: target.acceptance_commands,
            dependencies: input.dependencies.clone(),
            limits: input.limits.clone(),
        },
    }));
    (input, target.definition)
}
async fn reply(c: &Controller, command: Operation) -> Reply {
    let response = Box::pin(c.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: format!("handoff-request-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
        command: command.clone(),
    }))
    .await;
    response.validate_for(&command).unwrap();
    response
}
async fn query(c: &Controller, query: Query) -> QueryResult {
    let r = reply(c, Operation::QueryCollaboration { query }).await;
    assert!(r.commit.is_none());
    match r.outcome {
        Outcome::Ok {
            data: Data::Collaboration(v),
        } => v,
        other => panic!("query: {other:?}"),
    }
}
async fn session_input(c: &Controller, id: &str) -> (Session, TaskInput, bool) {
    match query(
        c,
        Query::SessionInput {
            session_id: id.into(),
        },
    )
    .await
    {
        QueryResult::SessionInput {
            session,
            input,
            current,
        } => (session, *input, current),
        other => panic!("session input: {other:?}"),
    }
}
async fn sessions(c: &Controller) -> Vec<Session> {
    match query(
        c,
        Query::Sessions {
            agent_id: None,
            after: None,
            limit: 10,
        },
    )
    .await
    {
        QueryResult::Sessions(page) => {
            assert!(page.next_cursor.is_none());
            page.items
        }
        other => panic!("sessions: {other:?}"),
    }
}
async fn charter_state(c: &Controller, id: &str) -> CharterQueryResult {
    match call(
        c,
        Operation::QueryCharter {
            query: CharterQuery::Session {
                session_id: id.into(),
            },
        },
    )
    .await
    {
        Outcome::Ok {
            data: Data::CharterQuery(value),
        } => value,
        other => panic!("charter: {other:?}"),
    }
}
async fn registered_input(c: &Controller, label: &str) -> (Task, Definition, Operation, Reply) {
    let (input, target) = configured_input(label);
    register_repository_fixture(c, &fixture_repository_registration()).await;
    let registered = reply(
        c,
        Operation::RegisterAgent {
            operation_id: format!("{label}-register-engineer"),
            definition: target.clone(),
        },
    )
    .await;
    assert!(
        matches!(
            registered.outcome,
            Outcome::Ok {
                data: Data::Agent(_)
            }
        ),
        "{registered:?}"
    );
    // The parent submit helper recognizes the older child workflow only. This
    // explicit request tests the new carrier without silently patching inputs.
    let command = Operation::SubmitTask {
        operation_id: format!("{label}-submit"),
        input,
    };
    let receipt = reply(c, command.clone()).await;
    (task(receipt.outcome.clone()), target, command, receipt)
}
fn launch(
    values: &[WorkerCommand],
    task_id: &str,
) -> (String, Session, Attempt, TaskInput, Option<Checkpoint>) {
    let values: Vec<_> = values
        .iter()
        .filter_map(|v| match v {
            WorkerCommand::RunAttempt {
                command_id,
                session,
                attempt,
                task,
                checkpoint,
                continuation,
                ..
            } if attempt.task_id == task_id => {
                assert!(
                    continuation.is_none(),
                    "same-task assignment is not a child continuation"
                );
                Some((
                    command_id.clone(),
                    session.clone(),
                    attempt.clone(),
                    task.clone(),
                    checkpoint.clone(),
                ))
            }
            _ => None,
        })
        .collect();
    assert_eq!(values.len(), 1, "one original launch for this task");
    values.into_iter().next().unwrap()
}
async fn started(c: &Controller, task_id: &str, label: &str) -> (Worker, Attempt) {
    let w = worker(&format!("{label}-worker"), "original", 1, 1);
    register(c, &w).await;
    let (command, _, a, _, _) = launch(&commands(c, &w, vec![]).await, task_id);
    let acknowledged = commands(c, &w, vec![command]).await;
    assert!(
        acknowledged
            .iter()
            .all(|v| !matches!(v, WorkerCommand::RunAttempt { .. }))
    );
    task(
        report(
            c,
            &a,
            &format!("{label}-started"),
            Observation::Started {
                thread_id: None,
                turn_id: None,
            },
        )
        .await,
    );
    (w, a)
}
fn assign(caller: &Attempt, task: &Task, target: &Definition, id: &str) -> Operation {
    Invocation {
        operation_id: id.into(), logical_call_id: format!("logical-{id}"),
        caller: Caller { task_id: caller.task_id.clone(), attempt_id: caller.id.clone(), generation: caller.generation,
            worker_id: caller.worker_id.clone(), incarnation: caller.worker_incarnation.clone() },
        tool: "collab_task_assign".into(),
        arguments: json!({"task_id":task.id,"expected_revision":task.revision,"agent_id":target.id}),
    }.operation()
}
fn operator_assign(task: &Task, agent: &str, id: &str) -> Operation {
    Operation::AssignTask {
        operation_id: id.into(),
        task_id: task.id.clone(),
        expected_revision: task.revision,
        agent_id: agent.into(),
    }
}
async fn exact_replay(c: &Controller, command: &Operation, original: &Reply) {
    let replay = reply(c, command.clone()).await;
    assert_eq!(replay.commit, original.commit);
    assert_eq!(replay.outcome, original.outcome);
}
async fn assert_original(c: &Controller, id: &str, original: &Reply) {
    let observed = reply(
        c,
        Operation::GetOperation {
            operation_id: id.into(),
        },
    )
    .await;
    assert_eq!(observed.commit, original.commit);
    let Outcome::Ok {
        data: Data::Operation(record),
    } = observed.outcome
    else {
        panic!("original lookup: {observed:?}")
    };
    assert_eq!(record.id, id);
    assert_eq!(
        record.outcome,
        Some(serde_json::to_value(&original.outcome).unwrap())
    );
}
fn no_run(values: &[WorkerCommand]) {
    assert!(
        values
            .iter()
            .all(|v| !matches!(v, WorkerCommand::RunAttempt { .. })),
        "no authority before closure: {values:?}"
    );
}
async fn stopped(c: &Controller, a: &Attempt, id: &str) {
    task(report(c, a, id, Observation::ProcessTreeStopped).await);
}

#[tokio::test]
async fn running_native_self_handoff_waits_for_stop_and_freezes_target_and_task_obligations() {
    let c = Controller::open_with_config(path(), handoff_config()).unwrap();
    let (accepted, target, _, _) = registered_input(&c, "running").await;
    let old_source = charter_state(&c, &accepted.session_id).await;
    let (w, a) = started(&c, &accepted.id, "running").await;
    controlled_native_request(&c, &a, 1, Some(3), Some(2)).await;
    let before = get(&c, &accepted.id).await;
    let command = assign(&a, &before, &target, "running-handoff");
    let original = reply(&c, command.clone()).await;
    let pending = task(original.outcome.clone());
    assert_eq!(pending.id, accepted.id);
    assert_eq!(pending.session_id, accepted.session_id);
    assert_eq!(pending.status, TaskStatus::Blocked);
    assert!(pending.result.is_none());
    assert_eq!(pending.input, accepted.input);
    let reserved = pending
        .pending_assignment
        .as_ref()
        .expect("reserved successor identity")
        .clone();
    assert_eq!(reserved.operation_id, "running-handoff");
    assert_eq!(reserved.from_session_id, accepted.session_id);
    assert_eq!(reserved.session.definition, target);
    assert_ne!(reserved.session.id, accepted.session_id);
    assert_eq!(
        sessions(&c).await.len(),
        1,
        "reserved identity is not active session authority"
    );
    let issued = commands(&c, &w, vec![]).await;
    no_run(&issued);
    assert!(issued.iter().any(|v| matches!(v, WorkerCommand::StopAttempt { attempt_id, generation, .. } if attempt_id==&a.id && *generation==a.generation)));

    // A later selected head must not rewrite an already accepted target.
    let update = reply(
        &c,
        Operation::UpdateDefinition {
            operation_id: "running-new-head".into(),
            update: DefinitionUpdate {
                agent_id: target.id.clone(),
                expected_revision: target.revision.clone(),
                expected_global_revision: c.limits().revision.clone(),
                expected_policy_revision: "handoff-policy-1".into(),
                changes: [(DefinitionLimitField::CodingSummaryChars, 1)]
                    .into_iter()
                    .collect(),
            },
        },
    )
    .await;
    let Outcome::Ok {
        data: Data::DefinitionUpdate(changed),
    } = update.outcome
    else {
        panic!("head update: {update:?}")
    };
    assert_ne!(changed.selected_definition.revision, target.revision);
    // Retirement blocks new admission; it cannot revoke this accepted, frozen
    // successor or grant it authority before the predecessor actually stops.
    let retirement = Operation::RetireAgent {
        operation_id: "running-retire-target".into(),
        agent_id: target.id.clone(),
        expected_revision: changed.selected_definition.revision.clone(),
    };
    let retired = reply(&c, retirement.clone()).await;
    assert!(
        matches!(&retired.outcome,
        Outcome::Ok { data: Data::Agent(agent) } if agent.retired
            && agent.definition == changed.selected_definition),
        "{retired:?}"
    );
    assert_eq!(get(&c, &accepted.id).await, pending);
    exact_replay(&c, &command, &original).await;
    no_run(&commands(&c, &w, vec![]).await);
    stopped(&c, &a, "running-stopped").await;
    let active = get(&c, &accepted.id).await;
    assert_eq!(active.session_id, reserved.session.id);
    assert_eq!(active.assigned_agent_id, target.id);
    assert!(active.pending_assignment.is_none());
    assert!(active.result.is_none());
    assert_eq!(active.status, TaskStatus::Queued);
    assert_eq!(active.input.definition, target);
    assert_eq!(active.input.instruction, accepted.input.instruction);
    assert_eq!(active.input.output_schema, accepted.input.output_schema);
    assert_eq!(active.input.dependencies, accepted.input.dependencies);
    assert_eq!(active.input.limits, accepted.input.limits);
    assert_eq!(
        active
            .input
            .resolved_limits
            .as_ref()
            .unwrap()
            .global_metadata,
        accepted
            .input
            .resolved_limits
            .as_ref()
            .unwrap()
            .global_metadata
    );
    active.input.execution_limits().unwrap();
    let (_, old_input, current) = session_input(&c, &accepted.session_id).await;
    assert!(!current);
    assert_eq!(old_input, accepted.input);
    assert_eq!(charter_state(&c, &accepted.session_id).await, old_source);
    let new_source = charter_state(&c, &active.session_id).await;
    let CharterQueryResult::Session(Some(new_source)) = new_source else {
        panic!("new charter absent")
    };
    assert_eq!(new_source.charter.reference.definition_id, target.id);
    assert_eq!(
        new_source.charter.reference.definition_revision,
        target.revision
    );
    let (_, session, next, input, checkpoint) =
        launch(&commands(&c, &w, vec![]).await, &accepted.id);
    assert_eq!(session, reserved.session);
    assert_eq!(next.session_id, active.session_id);
    assert!(next.generation > a.generation);
    assert_ne!(next.id, a.id);
    assert_eq!(next.deadline_unix_ms, a.deadline_unix_ms);
    assert_eq!(
        input.limits.model_requests,
        accepted.input.limits.model_requests - 1
    );
    assert_eq!(input.output_schema, accepted.input.output_schema);
    assert!(checkpoint.is_none());
    assert!(input.native_workflow.is_none());
    assert_eq!(
        input.repository.as_ref().unwrap().registration,
        Some(fixture_repository_binding())
    );
    assert_eq!(
        input.repository.as_ref().unwrap().url,
        fixture_repository_registration().source_endpoint
    );
    assert_eq!(
        input.repository.as_ref().unwrap().checkpoint_remote,
        fixture_repository_registration().checkpoint_endpoint
    );
    assert_eq!(
        input.acceptance_commands,
        vec![vec!["cargo".to_owned(), "test".to_owned()]]
    );
    exact_replay(&c, &retirement, &retired).await;
    exact_replay(&c, &command, &original).await;
    assert_original(&c, "running-handoff", &original).await;
    assert_eq!(sessions(&c).await.len(), 2);
    assert!(
        matches!(call(&c, Operation::ListTasks { after: None, limit: 10 }).await,
        Outcome::Ok { data: Data::Tasks(tasks) } if tasks.len()==1 && tasks[0].id==accepted.id)
    );
    let mut fresh_input = native("running-fresh-triage");
    fresh_input.definition.charter = Some(charter("fresh-source"));
    let fresh = submit(&c, "running-fresh-submit", fresh_input).await;
    let fresh_assignment = operator_assign(&fresh, &target.id, "running-fresh-handoff");
    let refused = reply(&c, fresh_assignment.clone()).await;
    assert_eq!(code(refused.outcome.clone()), "agent_retired");
    exact_replay(&c, &fresh_assignment, &refused).await;
    assert_eq!(get(&c, &fresh.id).await, fresh);
    assert_eq!(get(&c, &accepted.id).await.session_id, reserved.session.id);
}

#[tokio::test]
async fn handoff_pending_survives_reopen_and_original_receipts_do_not_allocate_again() {
    let p = path();
    let cfg = handoff_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let (accepted, target, submit_command, submitted) = registered_input(&c, "reopen").await;
    let (w, a) = started(&c, &accepted.id, "reopen").await;
    let memory_command = Operation::SetMemory {
        operation_id: "reopen-memory".into(),
        task_id: a.task_id.clone(),
        attempt_id: a.id.clone(),
        generation: a.generation,
        key: "same-key".into(),
        expected_revision: None,
        value: json!({"triage":"private"}),
    };
    let memory = reply(&c, memory_command.clone()).await;
    assert!(matches!(
        memory.outcome,
        Outcome::Ok {
            data: Data::Memory(Some(_))
        }
    ));
    let before = get(&c, &accepted.id).await;
    let command = assign(&a, &before, &target, "reopen-handoff");
    let original = reply(&c, command.clone()).await;
    let pending = task(original.outcome.clone());
    let reserved = pending.pending_assignment.as_ref().unwrap().session.clone();
    drop(c);
    controller_owner_released(&p).await;
    // A typed pending projection is not an exemption from durable handoff proof.
    // Refuse an orphan original intent, then restore its exact retained bytes.
    let sql = rusqlite::Connection::open(&p).unwrap();
    let original_body: String = sql.query_row("SELECT body FROM tasks WHERE id=?1", [&accepted.id], |r| r.get(0)).unwrap();
    let intent: (String, String, String, String, Option<String>, String) = sql.query_row(
        "SELECT operation_id,task_id,from_session_id,state,to_session_id,body FROM task_assignments WHERE task_id=?1",
        [&accepted.id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).unwrap();
    assert_eq!(sql.execute("DELETE FROM task_assignments WHERE operation_id=?1", [&intent.0]).unwrap(), 1);
    drop(sql);
    assert!(Controller::open_with_config(&p, cfg.clone()).is_err(), "orphan pending projection must refuse reopen");
    let sql = rusqlite::Connection::open(&p).unwrap();
    assert_eq!(sql.query_row("SELECT body FROM tasks WHERE id=?1", [&accepted.id], |r| r.get::<_,String>(0)).unwrap(), original_body);
    sql.execute("INSERT INTO task_assignments(operation_id,task_id,from_session_id,state,to_session_id,body) VALUES(?1,?2,?3,?4,?5,?6)",
        rusqlite::params![intent.0,intent.1,intent.2,intent.3,intent.4,intent.5]).unwrap();
    // Removing only the optional projection preserves canonical historical field
    // ordering and still leaves an unowned Blocked task, which must be refused.
    let field = format!(",\"pending_assignment\":{}", serde_json::to_string(pending.pending_assignment.as_ref().unwrap()).unwrap());
    assert_eq!(original_body.matches(&field).count(), 1);
    let no_projection = original_body.replacen(&field, "", 1);
    sql.execute("UPDATE tasks SET body=?2 WHERE id=?1", rusqlite::params![accepted.id,no_projection]).unwrap();
    drop(sql);
    let refusal = Controller::open_with_config(&p, cfg.clone()).err().expect("unowned Blocked task must refuse reopen");
    assert!(refusal.contains("invalid retained native retry block"), "{refusal}");
    let sql = rusqlite::Connection::open(&p).unwrap();
    assert_eq!(sql.query_row("SELECT body FROM tasks WHERE id=?1", [&accepted.id], |r| r.get::<_,String>(0)).unwrap(), no_projection);
    sql.execute("UPDATE tasks SET body=?2 WHERE id=?1", rusqlite::params![accepted.id,original_body]).unwrap();
    drop(sql);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    exact_replay(&c, &command, &original).await;
    assert_original(&c, "reopen-handoff", &original).await;
    assert_eq!(get(&c, &accepted.id).await, pending);
    no_run(&commands(&c, &w, vec![]).await);
    stopped(&c, &a, "reopen-stopped").await;
    let (_, _, next, _, _) = launch(&commands(&c, &w, vec![]).await, &accepted.id);
    assert_eq!(next.session_id, reserved.id);
    task(
        report(
            &c,
            &next,
            "reopen-engineer-started",
            Observation::Started {
                thread_id: None,
                turn_id: None,
            },
        )
        .await,
    );
    assert!(matches!(
        call(
            &c,
            Operation::GetMemory {
                agent_id: target.id.clone(),
                key: "same-key".into()
            }
        )
        .await,
        Outcome::Ok {
            data: Data::Memory(None)
        }
    ));
    let new_memory = Operation::SetMemory {
        operation_id: "reopen-engineer-memory".into(),
        task_id: next.task_id.clone(),
        attempt_id: next.id.clone(),
        generation: next.generation,
        key: "same-key".into(),
        expected_revision: None,
        value: json!({"engineer":"own"}),
    };
    let new_memory_receipt = reply(&c, new_memory.clone()).await;
    assert!(
        matches!(new_memory_receipt.outcome, Outcome::Ok { data: Data::Memory(Some(ref v)) } if v.agent_id==target.id && v.revision==1)
    );
    let old_snapshot = session_input(&c, &accepted.session_id).await;
    let new_snapshot = session_input(&c, &reserved.id).await;
    let history = query(
        &c,
        Query::TaskHistory {
            task_id: accepted.id.clone(),
            after: None,
            limit: 10,
        },
    )
    .await;
    let current = get(&c, &accepted.id).await;
    let all_sessions = sessions(&c).await;
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg).unwrap();
    assert_eq!(get(&c, &accepted.id).await, current);
    assert_eq!(sessions(&c).await, all_sessions);
    assert_eq!(session_input(&c, &accepted.session_id).await, old_snapshot);
    assert_eq!(session_input(&c, &reserved.id).await, new_snapshot);
    assert_eq!(
        query(
            &c,
            Query::TaskHistory {
                task_id: accepted.id.clone(),
                after: None,
                limit: 10
            }
        )
        .await,
        history
    );
    for (op, receipt) in [
        (&submit_command, &submitted),
        (&command, &original),
        (&memory_command, &memory),
        (&new_memory, &new_memory_receipt),
    ] {
        exact_replay(&c, op, receipt).await;
    }
    assert!(
        matches!(call(&c, Operation::GetMemory { agent_id: accepted.assigned_agent_id.clone(), key: "same-key".into() }).await,
        Outcome::Ok { data: Data::Memory(Some(v)) } if v.value==json!({"triage":"private"}) && v.revision==1)
    );
    assert!(
        matches!(call(&c, Operation::GetMemory { agent_id: target.id, key: "same-key".into() }).await,
        Outcome::Ok { data: Data::Memory(Some(v)) } if v.value==json!({"engineer":"own"}) && v.revision==1)
    );
    assert_eq!(get(&c, &accepted.id).await, current);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
//= type=test
//# A reassigned session MUST NOT execute until relevant uncertain external effects are reconciled.
async fn handoff_cannot_activate_unknown_execution_or_unsettled_external_effect() {
    for unknown in [false, true] {
        let c = Controller::open_with_config(path(), handoff_config()).unwrap();
        let label = if unknown { "unknown" } else { "effect" };
        let (accepted, target, _, _) = registered_input(&c, label).await;
        let (w, a) = started(&c, &accepted.id, label).await;
        if !unknown {
            let prepared = reply(
                &c,
                Operation::PrepareOperation {
                    operation_id: "held-effect".into(),
                    task_id: a.task_id.clone(),
                    attempt_id: a.id.clone(),
                    generation: a.generation,
                    kind: "merge".into(),
                    intended: json!({"fixture":"no external action performed"}),
                },
            )
            .await;
            assert!(matches!(
                prepared.outcome,
                Outcome::Ok {
                    data: Data::Operation(_)
                }
            ));
        }
        let before = get(&c, &accepted.id).await;
        let command = assign(&a, &before, &target, "held-handoff");
        let original = reply(&c, command.clone()).await;
        let pending = task(original.outcome.clone());
        if unknown {
            task(
                report(
                    &c,
                    &a,
                    "held-unknown",
                    Observation::ExecutionUncertain {
                        message: "controlled unknown original execution".into(),
                    },
                )
                .await,
            );
        }
        stopped(&c, &a, "held-stopped").await;
        let held = get(&c, &accepted.id).await;
        assert_eq!(held.session_id, accepted.session_id);
        assert!(held.attempt.as_ref().unwrap().uncertain);
        assert!(held.result.is_none());
        assert_eq!(sessions(&c).await.len(), 1);
        no_run(&commands(&c, &w, vec![]).await);
        exact_replay(&c, &command, &original).await;
        assert_eq!(sessions(&c).await.len(), 1);
        if !unknown {
            let settled = reply(
                &c,
                Operation::SettleOperation {
                    settlement_id: "held-not-applied".into(),
                    operation_id: "held-effect".into(),
                    task_id: a.task_id.clone(),
                    attempt_id: a.id.clone(),
                    generation: a.generation,
                    outcome: json!({"acknowledged":true,"not_applied":true}),
                    checkpoint: None,
                },
            )
            .await;
            assert!(
                matches!(settled.outcome, Outcome::Ok { data: Data::Operation(ref record) } if record.state=="settled")
            );
            let after = get(&c, &accepted.id).await;
            assert_eq!(
                after.session_id,
                pending.pending_assignment.unwrap().session.id
            );
            assert_eq!(sessions(&c).await.len(), 2);
            let (_, _, next, _, _) = launch(&commands(&c, &w, vec![]).await, &accepted.id);
            assert!(next.generation > a.generation);
        }
    }
}

#[tokio::test]
async fn handoff_commit_failure_rolls_back_reserved_session_and_same_original_retries_once() {
    let p = path();
    let failure = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut cfg = handoff_config();
    cfg.qualification_fail_next_commit = Some(failure.clone());
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let (accepted, target, _, _) = registered_input(&c, "commit").await;
    let command = operator_assign(&accepted, &target.id, "commit-handoff");
    let original_sessions = sessions(&c).await;
    failure.store(true, Ordering::SeqCst);
    let failed = reply(&c, command.clone()).await;
    assert!(!failure.load(Ordering::SeqCst));
    assert_eq!(code(failed.outcome), "store_unavailable");
    assert!(failed.commit.is_none());
    assert_eq!(get(&c, &accepted.id).await, accepted);
    assert_eq!(sessions(&c).await, original_sessions);
    assert_eq!(
        code(
            reply(
                &c,
                Operation::GetOperation {
                    operation_id: "commit-handoff".into()
                }
            )
            .await
            .outcome
        ),
        "not_found"
    );
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg).unwrap();
    let saved = reply(&c, command.clone()).await;
    let current = task(saved.outcome.clone());
    assert_ne!(current.session_id, accepted.session_id);
    assert_eq!(current.id, accepted.id);
    assert_eq!(sessions(&c).await.len(), 2);
    exact_replay(&c, &command, &saved).await;
    assert_original(&c, "commit-handoff", &saved).await;
    assert_eq!(get(&c, &accepted.id).await, current);
    assert_eq!(sessions(&c).await.len(), 2);
}

#[tokio::test]
async fn assignment_same_agent_is_noop_and_missing_cross_executor_scope_is_refused() {
    let c = Controller::open_with_config(path(), handoff_config()).unwrap();
    let mut input = native("unconfigured-triage");
    input.definition.charter = Some(charter("native-source"));
    let accepted = submit(&c, "unconfigured-submit", input).await;
    let mut engineer = coding("unconfigured-engineer").definition;
    engineer.charter = Some(charter("coding-source"));
    assert!(matches!(
        call(
            &c,
            Operation::RegisterAgent {
                operation_id: "unconfigured-register".into(),
                definition: engineer.clone()
            }
        )
        .await,
        Outcome::Ok {
            data: Data::Agent(_)
        }
    ));
    let same = operator_assign(&accepted, &accepted.assigned_agent_id, "same-agent");
    let no_op = reply(&c, same.clone()).await;
    assert_eq!(task(no_op.outcome.clone()), accepted);
    exact_replay(&c, &same, &no_op).await;
    let missing = operator_assign(&accepted, &engineer.id, "missing-scope");
    let refused = reply(&c, missing.clone()).await;
    assert_eq!(code(refused.outcome.clone()), "handoff_not_configured");
    exact_replay(&c, &missing, &refused).await;
    assert_eq!(get(&c, &accepted.id).await, accepted);
    assert_eq!(sessions(&c).await.len(), 1);
    let conflict = operator_assign(&accepted, &engineer.id, "same-agent");
    assert_eq!(
        code(reply(&c, conflict).await.outcome),
        "operation_conflict"
    );
}

#[tokio::test]
async fn superseded_session_reports_memory_and_new_effects_cannot_mutate_current_owner() {
    let c = Controller::open_with_config(path(), handoff_config()).unwrap();
    let (accepted, target, _, _) = registered_input(&c, "stale").await;
    let (w, old) = started(&c, &accepted.id, "stale").await;
    let before = get(&c, &accepted.id).await;
    let command = assign(&old, &before, &target, "stale-handoff");
    let saved = reply(&c, command.clone()).await;
    task(saved.outcome.clone());
    stopped(&c, &old, "stale-stopped").await;
    let (_, _, current, _, _) = launch(&commands(&c, &w, vec![]).await, &accepted.id);
    task(
        report(
            &c,
            &current,
            "stale-new-started",
            Observation::Started {
                thread_id: None,
                turn_id: None,
            },
        )
        .await,
    );
    let retained = get(&c, &accepted.id).await;
    for (id, observation) in [
        (
            "late-progress",
            Observation::Progress {
                text: "old private text".into(),
                output: "old output".into(),
                truncated: false,
            },
        ),
        (
            "late-result",
            Observation::ResultCandidate {
                checks_passed: true,
                summary: "not accepted".into(),
                checkpoint: None,
                validation: vec![],
                output: Some(json!({"answer":1})),
            },
        ),
    ] {
        assert_eq!(
            code(report(&c, &old, id, observation).await),
            "stale_generation"
        );
    }
    let stale_memory = Operation::SetMemory {
        operation_id: "late-memory".into(),
        task_id: old.task_id.clone(),
        attempt_id: old.id.clone(),
        generation: old.generation,
        key: "late".into(),
        expected_revision: None,
        value: json!("not copied"),
    };
    assert_eq!(
        code(reply(&c, stale_memory).await.outcome),
        "stale_generation"
    );
    let stale_effect = Operation::PrepareOperation {
        operation_id: "late-effect".into(),
        task_id: old.task_id.clone(),
        attempt_id: old.id.clone(),
        generation: old.generation,
        kind: "merge".into(),
        intended: json!({"fixture":"no external action performed"}),
    };
    assert_eq!(
        code(reply(&c, stale_effect).await.outcome),
        "stale_generation"
    );
    assert_eq!(get(&c, &accepted.id).await, retained);
    assert!(matches!(
        call(
            &c,
            Operation::GetMemory {
                agent_id: target.id,
                key: "late".into()
            }
        )
        .await,
        Outcome::Ok {
            data: Data::Memory(None)
        }
    ));
    exact_replay(&c, &command, &saved).await;
    assert_eq!(get(&c, &accepted.id).await, retained);
}

#[tokio::test]
async fn coding_successor_cannot_drop_original_output_schema_or_declared_checks() {
    let c = Controller::open_with_config(path(), handoff_config()).unwrap();
    let (accepted, target, _, _) = registered_input(&c, "result").await;
    let assigned = task(
        reply(&c, operator_assign(&accepted, &target.id, "result-handoff"))
            .await
            .outcome,
    );
    let (w, a) = started(&c, &accepted.id, "result").await;
    let repo = assigned.input.repository.as_ref().unwrap();
    let cp = Checkpoint {
        registration: repo.registration.clone().unwrap(),
        repository: repo.checkpoint_remote.clone(),
        commit: "b".repeat(40),
        remote_ref: checkpoint_ref(&a.task_id, &a.id, "result-checkpoint"),
        operation_id: "result-checkpoint".into(),
    };
    let prepared = reply(&c, Operation::PrepareOperation { operation_id: cp.operation_id.clone(), task_id: a.task_id.clone(), attempt_id: a.id.clone(), generation: a.generation,
        kind: "checkpoint".into(), intended: json!({"registration":cp.registration,"repository":cp.repository,"commit":cp.commit,"remote_ref":cp.remote_ref}) }).await;
    assert!(matches!(
        prepared.outcome,
        Outcome::Ok {
            data: Data::Operation(_)
        }
    ));
    let settled = reply(
        &c,
        Operation::SettleOperation {
            settlement_id: "result-checkpoint-ack".into(),
            operation_id: cp.operation_id.clone(),
            task_id: a.task_id.clone(),
            attempt_id: a.id.clone(),
            generation: a.generation,
            outcome: json!({"acknowledged":true}),
            checkpoint: Some(cp.clone()),
        },
    )
    .await;
    assert!(matches!(
        settled.outcome,
        Outcome::Ok {
            data: Data::Operation(_)
        }
    ));
    task(
        report(
            &c,
            &a,
            "result-executor",
            Observation::ExecutorOutcome {
                success: true,
                summary: "controlled executor outcome".into(),
            },
        )
        .await,
    );
    stopped(&c, &a, "result-stopped").await;
    let original = get(&c, &accepted.id).await;
    assert!(original.result.is_none());
    assert_eq!(
        code(
            report(
                &c,
                &a,
                "result-invalid-schema",
                Observation::ResultCandidate {
                    checks_passed: true,
                    summary: "invalid product result".into(),
                    checkpoint: Some(cp.clone()),
                    validation: vec!["cargo test passed".into()],
                    output: Some(json!({"wrong":1})),
                }
            )
            .await
        ),
        "schema_validation"
    );
    assert_eq!(
        code(
            report(
                &c,
                &a,
                "result-missing-check",
                Observation::ResultCandidate {
                    checks_passed: true,
                    summary: "missing accepted check".into(),
                    checkpoint: Some(cp.clone()),
                    validation: vec![],
                    output: Some(json!({"answer":1})),
                }
            )
            .await
        ),
        "invalid_input"
    );
    assert_eq!(get(&c, &accepted.id).await, original);
    let finished = task(
        report(
            &c,
            &a,
            "result-valid",
            Observation::ResultCandidate {
                checks_passed: true,
                summary: "same task complete".into(),
                checkpoint: Some(cp.clone()),
                validation: vec!["cargo test passed".into()],
                output: Some(json!({"answer":1})),
            },
        )
        .await,
    );
    assert_eq!(finished.id, accepted.id);
    assert_eq!(finished.status, TaskStatus::Succeeded);
    assert_eq!(finished.result.unwrap().checkpoint, Some(cp));
    assert_eq!(sessions(&c).await.len(), 2);
    no_run(&commands(&c, &w, vec![]).await);
}

#[tokio::test]
async fn same_task_coding_handoff_keeps_original_acknowledged_checkpoint_and_history() {
    let p = path();
    let cfg = handoff_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let mut input = coding("checkpoint-engineer");
    input.definition.charter = Some(charter("checkpoint-source"));
    let mut target = coding("checkpoint-reviewer").definition;
    target.charter = Some(charter("reviewer-source"));
    assert!(matches!(
        call(
            &c,
            Operation::RegisterAgent {
                operation_id: "checkpoint-register-target".into(),
                definition: target.clone()
            }
        )
        .await,
        Outcome::Ok {
            data: Data::Agent(_)
        }
    ));
    let accepted = submit(&c, "checkpoint-submit", input).await;
    let (w, old) = started(&c, &accepted.id, "checkpoint").await;
    let repo = accepted.input.repository.as_ref().unwrap();
    let checkpoint = Checkpoint {
        registration: repo.registration.clone().unwrap(),
        repository: repo.checkpoint_remote.clone(),
        commit: "c".repeat(40),
        remote_ref: checkpoint_ref(&old.task_id, &old.id, "original-publication"),
        operation_id: "original-publication".into(),
    };
    let prepare = Operation::PrepareOperation {
        operation_id: checkpoint.operation_id.clone(),
        task_id: old.task_id.clone(),
        attempt_id: old.id.clone(),
        generation: old.generation,
        kind: "checkpoint".into(),
        intended: json!({"registration":checkpoint.registration,"repository":checkpoint.repository,"commit":checkpoint.commit,"remote_ref":checkpoint.remote_ref}),
    };
    let prepared = reply(&c, prepare.clone()).await;
    assert!(matches!(
        prepared.outcome,
        Outcome::Ok {
            data: Data::Operation(_)
        }
    ));
    let settle = Operation::SettleOperation {
        settlement_id: "original-publication-ack".into(),
        operation_id: checkpoint.operation_id.clone(),
        task_id: old.task_id.clone(),
        attempt_id: old.id.clone(),
        generation: old.generation,
        outcome: json!({"acknowledged":true}),
        checkpoint: Some(checkpoint.clone()),
    };
    let settled = reply(&c, settle.clone()).await;
    assert!(matches!(
        settled.outcome,
        Outcome::Ok {
            data: Data::Operation(_)
        }
    ));
    let before = get(&c, &accepted.id).await;
    assert_eq!(before.selected_checkpoint, Some(checkpoint.clone()));
    let assignment = operator_assign(&before, &target.id, "checkpoint-handoff");
    let original = reply(&c, assignment.clone()).await;
    let pending = task(original.outcome.clone());
    assert_eq!(pending.selected_checkpoint, Some(checkpoint.clone()));
    no_run(&commands(&c, &w, vec![]).await);
    stopped(&c, &old, "checkpoint-stopped").await;
    let current = get(&c, &accepted.id).await;
    assert_eq!(current.selected_checkpoint, Some(checkpoint.clone()));
    assert_eq!(current.input.repository, accepted.input.repository);
    assert_eq!(
        current.input.acceptance_commands,
        accepted.input.acceptance_commands
    );
    assert_eq!(current.input.limits, accepted.input.limits);
    assert!(current.result.is_none());
    let history = query(
        &c,
        Query::TaskHistory {
            task_id: accepted.id.clone(),
            after: None,
            limit: 10,
        },
    )
    .await;
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg).unwrap();
    assert_eq!(get(&c, &accepted.id).await, current);
    assert_eq!(
        query(
            &c,
            Query::TaskHistory {
                task_id: accepted.id.clone(),
                after: None,
                limit: 10
            }
        )
        .await,
        history
    );
    exact_replay(&c, &prepare, &prepared).await;
    exact_replay(&c, &settle, &settled).await;
    exact_replay(&c, &assignment, &original).await;
    let (_, session, next, execution, selected) =
        launch(&commands(&c, &w, vec![]).await, &accepted.id);
    assert_ne!(session.id, accepted.session_id);
    assert_eq!(session.definition, target);
    assert_eq!(selected, Some(checkpoint));
    assert_eq!(execution.repository, accepted.input.repository);
    assert_eq!(next.task_id, old.task_id);
    assert!(next.generation > old.generation);
}
