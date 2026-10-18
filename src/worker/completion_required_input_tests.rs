//! Child of memory_notice_tests: reuse its real Controller/HTTP ScanFixture.
//! These tests exercise the existing Host and original delivery receipts. No
//! executor is launched and no provider-consumption claim is made.
use super::*;
use crate::collaboration::{
    notifications::DeliveryObservation,
    questions::{PoseQuestion, Question, QuestionMutation, QuestionOption},
    work::{WorkMutation, WorkQuery, WorkQueryResult},
    workflow::{ActivityDetail, PrincipalRef, QuestionChange, RecordRef, WorkflowActivity},
};
use crate::worker::collaboration_notice::{Host as WorkflowHost, PrepareDisposition};

fn occurrence(label: &str) -> String { mutations::fresh_id(label).unwrap() }

async fn fixture(steps: usize) -> ScanFixture {
    std::env::var_os("BORG_TEST_RUNTIME").expect("explicit library-test runtime binding");
    ScanFixture::configured(1, |limits| {
        // Complete no-charter V2 catalogue: 48,174-byte validation envelope,
        // 4,387 decoder admissions including keys, depth 19. Fixture metadata
        // is selected before validation and ordinary task acceptance.
        limits.execution.native.schema_bytes = 49_152;
        limits.execution.native.json_nodes = 8_192;
        limits.execution.native.json_depth = 24;
        limits.workflow = Some(crate::limits::WorkflowLimits {
            record_bytes:65_536, text_bytes:4096, options:8, records:128,
            revisions:1024, page_items:8, page_bytes:65_536, waits_per_task:8,
            graph_scan_nodes:256, events_per_mutation:16, event_bytes:16_384,
            notice_bytes:16_384, pending_delivery_bytes:131_072, delivery_scan_rows:32,
        });
        limits.governance = crate::worker::completion::tests::policy().governance;
        limits.governance.as_mut().unwrap().scan_rows = steps;
    }).await
}

async fn request(f: &ScanFixture, command: Operation) -> Reply {
    let reply = f.client.call(command.clone()).await.unwrap();
    reply.validate_for(&command).unwrap();
    reply
}

async fn native_reader(f: &ScanFixture) -> (Task, Caller) {
    let input = TaskInput { instruction_source: None,
        pull_request: None,
        resolved_limits:None, repository_read:None,
        instruction:"Controlled completion scan; no executor is launched".into(),
        definition:Definition {
            charter:None, limit_overrides:None,
            execution_profile:Some(ExecutionProfile::CollaborationV2 { web_search:None }),
            inference:Some(InferenceSettings::selected()),
            id:"completion-scan-native-agent".into(), revision:"one".into(), executor:ExecutorKind::Native,
        },
        repository:None, acceptance_commands:vec![], output_schema:Some(json!({"type":"integer"})),
        limits:Limits { deadline_unix_ms:f.control.deadline_ms, model_requests:4,
            output_bytes:4096, result_bytes:f.limits.task_defaults.result_bytes, max_delegation_depth:1 },
        dependencies:vec![], native_workflow:None,
    };
    let task = match request(f, Operation::SubmitTask { operation_id:occurrence("completion-submit"), input }).await.outcome {
        Outcome::Ok { data:Data::Task(task) } => *task,
        other => panic!("native task expected: {other:?}"),
    };
    assert!(matches!(request(f, Operation::RegisterWorker { operation_id:occurrence("completion-register"),
        worker:Worker { id:"completion-scan-worker".into(), incarnation:"one".into(), coding_capacity:0, native_capacity:1 },
    }).await.outcome, Outcome::Ok { .. }));
    let commands = match request(f, Operation::AwaitCommands {
        operation_id:occurrence("completion-claim"), worker_id:"completion-scan-worker".into(),
        incarnation:"one".into(), acknowledged:vec![],
    }).await.outcome {
        Outcome::Ok { data:Data::Commands(commands) } => commands,
        other => panic!("ordinary admission expected: {other:?}"),
    };
    let attempt = commands.into_iter().find_map(|command| match command {
        WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == task.id => Some(attempt),
        _ => None,
    }).expect("one genuine V2 attempt");
    f.report(&attempt, Observation::Started { thread_id:None, turn_id:None }).await;
    let actor = Caller { task_id:task.id.clone(), attempt_id:attempt.id, generation:attempt.generation,
        worker_id:attempt.worker_id, incarnation:attempt.worker_incarnation };
    (task, actor)
}

fn workflow_host(f: &ScanFixture, task: &Task, actor: &Caller) -> WorkflowHost {
    WorkflowHost::open(f.client.clone(), &f.area.0.join("workflow-worker"),
        actor, &task.session_id, &task.assigned_agent_id, f.limits.workflow.as_ref().unwrap()).unwrap()
}

fn question(task: &Task, recipient: PrincipalRef) -> PoseQuestion {
    PoseQuestion { record:RecordRef::Task { task_id:task.id.clone() }, recipient,
        prompt:"Choose the retained option".into(), options:vec![QuestionOption {
            id:"keep".into(), label:"Keep".into(), explanation:"Keep the original selection".into(),
        }], recommended_option_id:"keep".into() }
}

async fn pose(f: &ScanFixture, task: &Task, actor: Option<&Caller>, recipient: PrincipalRef) -> Question {
    let posed = question(task, recipient);
    let operation_id = occurrence("completion-question");
    let command = match actor {
        Some(actor) => crate::mcp::Invocation { operation_id:operation_id.clone(),
            logical_call_id:format!("logical-{operation_id}"), caller:actor.clone(),
            tool:"collab_question_pose".into(), arguments:json!({"question":posed}),
        }.operation(),
        None => Operation::MutateQuestion { operation_id, mutation:QuestionMutation::Pose { question:posed } },
    };
    match request(f, command).await.outcome {
        Outcome::Ok { data:Data::QuestionMutation(value) } => value.question.question,
        other => panic!("original question expected: {other:?}"),
    }
}

async fn activity(f: &ScanFixture, question: &Question, terminal: bool) -> WorkflowActivity {
    let mut cursor = None;
    for _ in 0..f.limits.workflow.as_ref().unwrap().records {
        let page = match request(f, Operation::QueryWorkflowActivity { cursor:cursor.take(), limit:8 }).await.outcome {
            Outcome::Ok { data:Data::WorkflowActivity(page) } => page,
            other => panic!("canonical event page expected: {other:?}"),
        };
        if let Some(event) = page.items.into_iter().find(|event|
            event.subject == (RecordRef::Question { question_id:question.id.clone() })
                && matches!(&event.detail, ActivityDetail::Question(change)
                    if matches!(change, QuestionChange::Closed { .. }) == terminal)) {
            return event;
        }
        cursor = page.next_cursor;
        if cursor.is_none() { break; }
    }
    panic!("original question event absent")
}

async fn close(f: &ScanFixture, question: &Question) -> WorkflowActivity {
    assert!(matches!(request(f, Operation::MutateQuestion {
        operation_id:occurrence("completion-question-close"), mutation:QuestionMutation::Cancel {
            question_id:question.id.clone(), expected_revision:question.revision, reason:"Original required input".into(),
        },
    }).await.outcome, Outcome::Ok { data:Data::QuestionMutation(_) }));
    activity(f, question, true).await
}

async fn observe(f: &ScanFixture, actor: &Caller) -> DeliveryObservation {
    match request(f, Operation::ObserveCollaborationDelivery { caller:actor.clone(), limit:8 }).await.outcome {
        Outcome::Ok { data:Data::CollaborationDelivery(view) } => view,
        other => panic!("actual workflow delivery expected: {other:?}"),
    }
}

fn scan_cap(f: &ScanFixture) -> usize {
    f.limits.workflow.as_ref().unwrap().revisions.saturating_add(1).saturating_mul(2)
        .min(f.limits.governance.as_ref().unwrap().scan_rows)
}

fn producer_sends(f: &ScanFixture) -> Vec<Operation> { f.sends.lock().unwrap().clone() }

fn no_input_begin(sends: &[Operation]) {
    assert!(!sends.iter().any(|op| matches!(op,
        Operation::BeginCollaborationDelivery { .. } | Operation::BeginCompletionContinuation(_)
        | Operation::AcknowledgeCompletionInput(_))));
}

#[tokio::test]
async fn completion_scan_crosses_empty_ordinary_lane_but_ordinary_prepare_remains_one_scan() {
    for completion in [false, true] {
        let f = fixture(32).await;
        let (task, actor) = native_reader(&f).await;
        let question = pose(&f, &task, Some(&actor), PrincipalRef::TrustedHostOperator).await;
        let terminal = close(&f, &question).await;
        let before = observe(&f, &actor).await;
        let prefix = before.available.as_ref().expect("ordinary scan offer");
        assert!(!prefix.has_input()); assert!(!prefix.has_more_work()); assert!(prefix.routing.is_none());
        assert!(scan_cap(&f) >= 2);
        f.sends.lock().unwrap().clear();
        let mut host = workflow_host(&f, &task, &actor);
        let outcome = if completion { host.prepare_completion(&[terminal.id.clone()], &f.control).await }
            else { host.prepare(&f.control).await }.unwrap();
        let sends = producer_sends(&f);
        no_input_begin(&sends);
        assert_eq!(sends.iter().filter(|op| matches!(op, Operation::AcknowledgeCollaborationDelivery { .. })).count(), 1,
            "only the actual empty ordinary prefix is ACKed");
        let after = observe(&f, &actor).await;
        assert_eq!(after.acknowledged_cursor, prefix.to_cursor);
        if completion {
            let PrepareDisposition::Notice(notice) = outcome else { panic!("required routing input was not selected: {outcome:?}") };
            let pending = after.pending.expect("one exact retained input batch");
            assert_eq!(notice.batch_id, pending.batch_id); assert!(pending.delivery.is_none());
            assert!(pending.page.contains_event(&terminal.id));
            assert_eq!(pending.page.routing.as_ref().unwrap().items, vec![terminal.clone()]);
            let text:serde_json::Value = serde_json::from_str(&notice.text).unwrap();
            assert_eq!(text["events"][0]["id"], serde_json::to_value(&terminal.id).unwrap());
            assert_eq!(sends.iter().filter(|op| matches!(op, Operation::PrepareCollaborationDelivery { .. })).count(), 2);
        } else {
            assert_eq!(outcome, PrepareDisposition::NoChanges);
            assert!(after.pending.is_none()); assert!(after.routing_turn);
            assert!(after.available.as_ref().unwrap().contains_event(&terminal.id),
                "NoChanges described the completed ordinary page, not the owed routing lane");
            assert_eq!(sends.iter().filter(|op| matches!(op, Operation::PrepareCollaborationDelivery { .. })).count(), 1);
        }
    }
}

#[tokio::test]
async fn completion_scan_preserves_unrelated_nonempty_batch_without_ack_or_begin() {
    let f = fixture(32).await;
    let (task, actor) = native_reader(&f).await;
    let advisory = pose(&f, &task, None, PrincipalRef::Agent { agent_id:task.assigned_agent_id.clone() }).await;
    let advisory_event = activity(&f, &advisory, false).await;
    let question = pose(&f, &task, Some(&actor), PrincipalRef::TrustedHostOperator).await;
    let terminal = close(&f, &question).await;
    let before = observe(&f, &actor).await;
    assert!(before.available.as_ref().unwrap().contains_event(&advisory_event.id));
    assert!(!before.available.as_ref().unwrap().contains_event(&terminal.id));
    f.sends.lock().unwrap().clear();
    let mut host = workflow_host(&f, &task, &actor);
    assert_eq!(host.prepare_completion(&[terminal.id.clone()], &f.control).await.unwrap(), PrepareDisposition::Deferred);
    let sends = producer_sends(&f); no_input_begin(&sends);
    assert!(!sends.iter().any(|op| matches!(op, Operation::AcknowledgeCollaborationDelivery { .. })));
    let after = observe(&f, &actor).await;
    assert_eq!(after.acknowledged_cursor, before.acknowledged_cursor);
    let pending = after.pending.unwrap();
    assert!(pending.page.contains_event(&advisory_event.id)); assert!(!pending.page.contains_event(&terminal.id));
    assert!(pending.delivery.is_none());
    drop(host);
    let mut reopened = workflow_host(&f, &task, &actor);
    assert_eq!(reopened.prepare_completion(&[terminal.id], &f.control).await.unwrap(), PrepareDisposition::Deferred);
    assert_eq!(observe(&f, &actor).await.pending, Some(pending));
    assert!(!producer_sends(&f).iter().any(|op| matches!(op, Operation::AcknowledgeCollaborationDelivery { .. })));
}

#[tokio::test]
async fn completion_scan_stops_when_a_previously_eligible_route_no_longer_makes_progress() {
    let f = fixture(32).await;
    let (task, actor) = native_reader(&f).await;
    // Host-originated routing follows the linked record owner, so an actual
    // owner change can remove this caller's eligibility after observation.
    let question = pose(&f, &task, None, PrincipalRef::TrustedHostOperator).await;
    let terminal = close(&f, &question).await;
    let mut host = workflow_host(&f, &task, &actor);
    assert_eq!(host.prepare(&f.control).await.unwrap(), PrepareDisposition::NoChanges);
    assert!(observe(&f, &actor).await.available.unwrap().contains_event(&terminal.id));
    let revision = match request(&f, Operation::QueryWork { query:WorkQuery::Get { task_id:task.id.clone() } }).await.outcome {
        Outcome::Ok { data:Data::WorkQuery(WorkQueryResult::Task(view)) } => view.coordination_revision,
        other => panic!("work revision expected: {other:?}"),
    };
    assert!(matches!(request(&f, Operation::MutateWork { operation_id:occurrence("completion-owner"),
        mutation:WorkMutation::ChangeOwner { task_id:task.id.clone(), expected_revision:revision,
            owner:PrincipalRef::TrustedHostOperator },
    }).await.outcome, Outcome::Ok { data:Data::WorkMutation(_) }));
    f.sends.lock().unwrap().clear();
    let outcome = tokio::time::timeout(Duration::from_secs(5), host.prepare_completion(&[terminal.id], &f.control))
        .await.expect("finite progress/cap stop").unwrap();
    assert_eq!(outcome, PrepareDisposition::Deferred);
    let sends = producer_sends(&f); no_input_begin(&sends);
    let prepared = sends.iter().filter(|op| matches!(op, Operation::PrepareCollaborationDelivery { .. })).count();
    assert!(prepared <= scan_cap(&f), "preparation steps, not aggregate SQL rows, are capped");
    assert!(observe(&f, &actor).await.pending.is_none(), "no unrepresented input may be retained as consumed");
    assert_eq!(activity(&f, &question, true).await.detail, terminal.detail);
}

#[tokio::test]
async fn completion_scan_bound_preserves_required_suffix_without_a_second_prepare() {
    let f = fixture(1).await;
    let (task, actor) = native_reader(&f).await;
    let question = pose(&f, &task, Some(&actor), PrincipalRef::TrustedHostOperator).await;
    let terminal = close(&f, &question).await;
    assert_eq!(scan_cap(&f), 1);
    f.sends.lock().unwrap().clear();
    let mut host = workflow_host(&f, &task, &actor);
    assert_eq!(host.prepare_completion(&[terminal.id.clone()], &f.control).await.unwrap(), PrepareDisposition::Deferred);
    let sends = producer_sends(&f); no_input_begin(&sends);
    assert_eq!(sends.iter().filter(|op| matches!(op, Operation::PrepareCollaborationDelivery { .. })).count(), 1);
    assert_eq!(sends.iter().filter(|op| matches!(op, Operation::AcknowledgeCollaborationDelivery { .. })).count(), 1);
    let after = observe(&f, &actor).await;
    assert!(after.pending.is_none()); assert!(after.routing_turn);
    assert!(after.available.unwrap().contains_event(&terminal.id));
}

#[tokio::test]
async fn reset_selector_does_not_substitute_an_old_required_page_for_the_actual_reset_event() {
    use crate::governance::CompletionReason;
    use crate::governance_completion::{CompletionBoundary, CompletionTarget};
    let f = fixture(32).await;
    let (task, actor) = native_reader(&f).await;
    let old_question = pose(&f, &task, Some(&actor), PrincipalRef::TrustedHostOperator).await;
    let old = close(&f, &old_question).await;
    let mut host = workflow_host(&f, &task, &actor);
    let PrepareDisposition::Notice(old_notice) = host.prepare_completion(&[old.id.clone()], &f.control).await.unwrap()
        else { panic!("original old input must be pending before the later event") };
    let old_pending = observe(&f, &actor).await.pending.unwrap();
    assert!(old_pending.page.contains_event(&old.id));
    let reset_question = pose(&f, &task, Some(&actor), PrincipalRef::TrustedHostOperator).await;
    let reset = close(&f, &reset_question).await;
    assert!(!old_pending.page.contains_event(&reset.id));
    // The existing selector fixture supplies a typed retained evaluation only;
    // original rule/decision authority is tested through Controller separately.
    // Both membership discriminators above are genuine immutable Controller events.
    let mut feedback = crate::worker::completion::tests::feedback(
        CompletionBoundary::NativeResponse { request_index:1 },
        CompletionTarget::NativeRequest { request_index:2 });
    feedback.evaluation.state.reasons = vec![CompletionReason::UnconsumedInput { event_id:old.id.clone() }];
    let original = feedback.evaluation.clone();
    assert_eq!(crate::worker::completion::required_events(&feedback.evaluation, None), vec![old.id]);
    let required = crate::worker::completion::required_events(&feedback.evaluation, Some(&reset.id));
    assert_eq!(required, vec![reset.id]);
    f.sends.lock().unwrap().clear();
    assert_eq!(host.prepare_completion(&required, &f.control).await.unwrap(), PrepareDisposition::Deferred);
    let sends = producer_sends(&f); no_input_begin(&sends);
    assert!(!sends.iter().any(|op| matches!(op, Operation::AcknowledgeCollaborationDelivery { .. })));
    let pending = observe(&f, &actor).await.pending.unwrap();
    assert_eq!(pending, old_pending); assert_eq!(pending.batch_id, old_notice.batch_id);
    assert_eq!(feedback.evaluation, original);
}
