//! Source-only join under tests/controller.rs. Mutations cross real Controller
//! or browser-product boundaries; SQL only reads their durable evidence.
use super::*;
use borg_runtime::collaboration::{questions::*, work::*, workflow::*};
use borg_runtime::{mcp::{Caller, Invocation}, product::{self, UiRequest}};

fn workflow_config() -> ControllerConfig {
    let mut cfg = config();
    let limits = Arc::make_mut(&mut cfg.limits);
    limits.workflow = Some(borg_runtime::limits::WorkflowLimits {
        record_bytes: 32_768, text_bytes: 4_096, options: 8,
        records: 512, revisions: 4_096, page_items: 8, page_bytes: 32_768,
        waits_per_task: 8, graph_scan_nodes: 512, events_per_mutation: 64,
        event_bytes: 8_192, notice_bytes: 8_192,
        pending_delivery_bytes: 32_768, delivery_scan_rows: 64,
    });
    limits.validate().unwrap();
    cfg
}
fn workflow_native(agent: &str) -> TaskInput {
    let mut input = native(agent);
    input.definition.execution_profile = Some(ExecutionProfile::CollaborationV2 { web_search: None });
    input
}
fn request(command: Operation) -> Request {
    Request { protocol_revision: WIRE_REVISION,
        request_id: format!("provenance-request-{}", NEXT.fetch_add(1, Ordering::Relaxed)), command }
}
async fn reply(c: &Controller, command: Operation) -> Reply {
    let r = c.handle(request(command.clone())).await;
    r.validate_for(&command).unwrap();
    r
}
async fn activity(c: &Controller) -> Vec<WorkflowActivity> {
    let mut events = vec![];
    let mut cursor = None;
    for _ in 0..16 {
        let page = match call(c, Operation::QueryWorkflowActivity { cursor: cursor.take(), limit: 8 }).await {
            Outcome::Ok { data: Data::WorkflowActivity(page) } => page,
            other => panic!("activity page expected: {other:?}"),
        };
        events.extend(page.items);
        cursor = page.next_cursor;
        if cursor.is_none() { break; }
    }
    assert!(cursor.is_none(), "finite attribution fixture fits its read bound");
    events
}
fn closed(events: &[WorkflowActivity], task_id: &str) -> WorkflowActivity {
    let found = events.iter().filter(|event| matches!(&event.detail,
        ActivityDetail::Task(TaskChange::Closed { resolution }) if resolution.task_id == task_id
    )).collect::<Vec<_>>();
    assert_eq!(found.len(), 1, "exactly one first terminal occurrence");
    (*found[0]).clone()
}
fn host_actor(path: TrustedHostPath) -> RecordedActor {
    RecordedActor { principal: PrincipalRef::TrustedHostOperator,
        provenance: ActorProvenance::TrustedHost { path } }
}
fn receipt_count(path: &std::path::Path) -> i64 {
    let db = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    db.query_row("SELECT count(*) FROM receipts", [], |r| r.get(0)).unwrap()
}
fn persisted_event(path: &std::path::Path, id: &EventId) -> WorkflowActivity {
    let db = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let body: String = db.query_row("SELECT body FROM workflow_events WHERE product_revision=?1 AND ordinal=?2",
        rusqlite::params![i64::try_from(id.product_revision).unwrap(), i64::try_from(id.ordinal).unwrap()], |r| r.get(0)).unwrap();
    serde_json::from_str(&body).unwrap()
}
async fn question(c: &Controller, id: &str) -> QuestionView {
    match call(c, Operation::QueryQuestion { query: QuestionQuery::Get { question_id: id.into() } }).await {
        Outcome::Ok { data: Data::QuestionQuery(QuestionQueryResult::Question(view)) } => view,
        other => panic!("question expected: {other:?}"),
    }
}
async fn work(c: &Controller, id: &str) -> WorkView {
    match call(c, Operation::QueryWork { query: WorkQuery::Get { task_id: id.into() } }).await {
        Outcome::Ok { data: Data::WorkQuery(WorkQueryResult::Task(view)) } => view,
        other => panic!("work expected: {other:?}"),
    }
}
async fn pose(c: &Controller, id: &str, task: &Task, recipient: PrincipalRef) -> Question {
    let mutation = QuestionMutation::Pose { question: PoseQuestion {
        record: RecordRef::Task { task_id: task.id.clone() }, recipient,
        prompt: "Choose one option. Recommended: keep.".into(),
        options: vec![
            QuestionOption { id: "keep".into(), label: "Keep".into(), explanation: "Keep the current choice".into() },
            QuestionOption { id: "change".into(), label: "Change".into(), explanation: "Use the alternative".into() },
        ], recommended_option_id: "keep".into(),
    }};
    match call(c, Operation::MutateQuestion { operation_id: id.into(), mutation }).await {
        Outcome::Ok { data: Data::QuestionMutation(value) } => value.question.question,
        other => panic!("posed question expected: {other:?}"),
    }
}
fn encode_form(value: &str) -> String {
    value.bytes().map(|b| if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
        (b as char).to_string()
    } else { format!("%{b:02X}") }).collect()
}
fn form(fields: &[(&str, String)]) -> Vec<u8> {
    fields.iter().map(|(key, value)| format!("{}={}", encode_form(key), encode_form(value)))
        .collect::<Vec<_>>().join("&").into_bytes()
}
async fn browser(c: &Controller, path: &str, fields: &[(&str, String)]) -> product::UiResponse {
    product::handle(c, UiRequest { method: "POST".into(), path: path.into(), body: form(fields),
        origin: Some("http://local.invalid".into()), expected_origin: "http://local.invalid".into() }).await
}

#[tokio::test]
async fn actual_expiry_records_maintenance_without_a_receipt_or_recipient_authority() {
    let p = path();
    let clock = Arc::new(AtomicU64::new(1_000));
    let source_clock = clock.clone();
    let mut cfg = workflow_config();
    cfg.clock = Arc::new(move || source_clock.load(Ordering::SeqCst));
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let mut input = workflow_native("expiry-source-owner");
    input.limits.deadline_unix_ms = 2_000;
    let source = submit(&c, "maintenance-source", input).await;
    let mut input = workflow_native("expiry-recipient");
    input.limits.deadline_unix_ms = 10_000;
    let waiter = submit(&c, "maintenance-waiter", input).await;
    let q = pose(&c, "maintenance-agent-question", &source,
        PrincipalRef::Agent { agent_id: waiter.assigned_agent_id.clone() }).await;
    let wait = call(&c, Operation::MutateWork { operation_id: "maintenance-wait".into(),
        mutation: WorkMutation::AddWait { task_id: waiter.id.clone(), expected_revision: 0,
            resolver: ResolverRef::Task { task_id: source.id.clone() } } }).await;
    assert!(matches!(wait, Outcome::Ok { data: Data::WorkMutation(_) }));
    drop(c);
    controller_owner_released(&p).await;
    let receipts_before = receipt_count(&p);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    clock.store(3_000, Ordering::SeqCst);
    // GetTask wakes the actual controller owner. Its expiry transaction executes
    // before this read; no test calls a finalizer or inserts a terminal record.
    let observed = reply(&c, Operation::GetTask { task_id: source.id.clone() }).await;
    assert!(observed.commit.is_none(), "maintenance does not invent a mutation receipt for a read");
    let expired = task(observed.outcome);
    assert_eq!(expired.status, TaskStatus::Failed);
    assert_eq!(expired.blocked_reason.as_deref(), Some("execution_deadline"));
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(receipt_count(&p), receipts_before);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let events = activity(&c).await;
    let event = closed(&events, &source.id);
    let maintenance = host_actor(TrustedHostPath::ControllerMaintenance);
    assert_eq!(event.actor, maintenance);
    assert_eq!(event.routing.as_ref().unwrap().requested_by, maintenance);
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(persisted_event(&p, &event.id), event);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let view = work(&c, &waiter.id).await;
    assert_eq!(view.open_waits, 0);
    assert_eq!(view.waits.items.len(), 1);
    match &view.waits.items[0].status {
        WaitStatus::Resolved { resolution, actor, .. } => {
            assert_eq!(actor, &maintenance);
            assert_eq!(resolution.event_id, event.id);
        },
        other => panic!("expiry must resolve the exact Task wait: {other:?}"),
    }
    let audit = events.iter().filter(|e| matches!(&e.detail,
        ActivityDetail::Wait(WaitChange::Resolved { wait, .. }) if wait.task_id == waiter.id
    )).collect::<Vec<_>>();
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].actor, maintenance);
    assert_eq!(audit[0].id.product_revision, event.id.product_revision);
    assert_eq!(question(&c, &q.id).await.question, q);
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert_eq!(activity(&c).await, events);
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(persisted_event(&p, &event.id), event);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert_eq!(work(&c, &waiter.id).await, view);
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(receipt_count(&p), receipts_before);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    // The persisted maintenance principal is attribution, not a reusable answer
    // credential. The current caller still must be the explicit recipient.
    let answer = QuestionAnswer { option_id: "keep".into(), explanation: "Current recipient decision".into() };
    let refused = call(&c, Operation::MutateQuestion { operation_id: "maintenance-host-answer".into(),
        mutation: QuestionMutation::Answer { question_id: q.id.clone(), expected_revision: q.revision,
            answer: answer.clone() } }).await;
    assert_eq!(code(refused), "recipient_mismatch");
    let denied = browser(&c, &format!("/questions/{}/answer", q.id), &[
        ("operation_id", "maintenance-browser-answer".into()), ("expected_revision", q.revision.to_string()),
        ("option_id", "keep".into()), ("explanation", "No inherited maintenance authority".into()),
    ]).await;
    assert_eq!(denied.status, 409, "{}", denied.body);
    assert!(denied.body.contains("recipient_mismatch"));
    assert_eq!(question(&c, &q.id).await.question, q);
    assert_eq!(activity(&c).await, events);
    // Positive control: ordinary admission plus a current bound recipient can
    // answer after expiry; the old maintenance record is not used as its actor.
    let worker = worker("maintenance-recipient-worker", "inc", 1, 0);
    register(&c, &worker).await;
    let a = run(&commands(&c, &worker, vec![]).await);
    assert_eq!(a.task_id, waiter.id);
    task(report(&c, &a, "maintenance-recipient-start", Observation::Started { thread_id: None, turn_id: None }).await);
    let caller = Caller { task_id: a.task_id.clone(), attempt_id: a.id.clone(), generation: a.generation,
        worker_id: a.worker_id.clone(), incarnation: a.worker_incarnation.clone() };
    let invocation = Invocation { operation_id: "maintenance-recipient-answer".into(),
        logical_call_id: "maintenance-recipient-answer-logical".into(), caller,
        tool: "collab_question_answer".into(),
        arguments: json!({"question_id":q.id,"expected_revision":q.revision,"answer":answer}) };
    let answer = match call(&c, invocation.operation()).await {
        Outcome::Ok { data: Data::QuestionMutation(value) } => value.new_answer.unwrap(),
        other => panic!("current direct recipient answer expected: {other:?}"),
    };
    assert_eq!(answer.author.principal, PrincipalRef::Agent { agent_id: waiter.assigned_agent_id });
    assert!(matches!(answer.author.provenance, ActorProvenance::BoundAgentSession { ref attempt_id, .. } if attempt_id == &a.id));
    assert_eq!(closed(&activity(&c).await, &source.id), event);
}

#[tokio::test]
async fn explicit_cancellation_and_validated_report_retain_their_actual_provenance() {
    let p = path();
    let cfg = workflow_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let direct = submit(&c, "attribution-direct-task", workflow_native("attribution-direct-owner")).await;
    task(call(&c, Operation::CancelTask { operation_id: "attribution-direct-cancel".into(),
        task_id: direct.id.clone(), expected_revision: direct.revision }).await);
    let browser_task = submit(&c, "attribution-browser-task", workflow_native("attribution-browser-owner")).await;
    let response = browser(&c, &format!("/tasks/{}/cancel", browser_task.id), &[
        ("operation_id", "attribution-browser-cancel".into()), ("expected_revision", browser_task.revision.to_string()),
    ]).await;
    assert_eq!(response.status, 303, "{}", response.body);
    let reported = submit(&c, "attribution-report-task", workflow_native("attribution-report-owner")).await;
    let w = worker("attribution-report-worker", "inc", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    assert_eq!(a.task_id, reported.id);
    task(report(&c, &a, "attribution-report-start", Observation::Started { thread_id: None, turn_id: None }).await);
    let before = activity(&c).await;
    let mut stale = a.clone();
    stale.generation += 1;
    let rejected = report(&c, &stale, "attribution-stale-report", Observation::ExecutionUncertain { message: "Rejected stale caller".into() }).await;
    assert!(matches!(rejected, Outcome::Refused { .. }));
    assert_eq!(activity(&c).await, before);
    let failed = task(report(&c, &a, "attribution-valid-report", Observation::ExecutionUncertain { message: "Current worker lost execution certainty".into() }).await);
    assert_eq!(failed.status, TaskStatus::Failed);
    assert_eq!(failed.attempt.as_ref().unwrap().phase, AttemptPhase::StopRequested);
    let events = activity(&c).await;
    assert_eq!(closed(&events, &direct.id).actor, host_actor(TrustedHostPath::OperatorApi));
    assert_eq!(closed(&events, &browser_task.id).actor, host_actor(TrustedHostPath::Browser));
    let event = closed(&events, &reported.id);
    let expected = RecordedActor { principal: PrincipalRef::Agent { agent_id: reported.assigned_agent_id.clone() },
        provenance: ActorProvenance::BoundAgentSession { agent_id: reported.assigned_agent_id,
            task_id: reported.id, session_id: reported.session_id, attempt_id: a.id,
            generation: a.generation, worker_id: a.worker_id, incarnation: a.worker_incarnation } };
    assert_eq!(event.actor, expected);
    assert_eq!(event.routing.as_ref().unwrap().requested_by, expected);
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(persisted_event(&p, &event.id), event);
}

#[tokio::test]
async fn work_and_question_boundaries_reject_caller_supplied_maintenance_selectors() {
    let p = path();
    let cfg = workflow_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let t = submit(&c, "selector-task", workflow_native("selector-owner")).await;
    let q = pose(&c, "selector-question", &t, PrincipalRef::TrustedHostOperator).await;
    let wait = Operation::MutateWork { operation_id: "selector-valid-wait".into(),
        mutation: WorkMutation::AddWait { task_id: t.id.clone(), expected_revision: 0,
            resolver: ResolverRef::Question { question_id: q.id.clone() } } };
    let cancel = Operation::MutateQuestion { operation_id: "selector-valid-cancel".into(),
        mutation: QuestionMutation::Cancel { question_id: q.id.clone(), expected_revision: q.revision,
            reason: "The current operator closes this question".into() } };
    let actor = host_actor(TrustedHostPath::ControllerMaintenance);
    let before = activity(&c).await;
    drop(c);
    controller_owner_released(&p).await;
    let receipts = receipt_count(&p);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    for selector in ["actor", "provenance", "actor_provenance", "invocation_path"] {
        let value = match selector {
            "actor" => serde_json::to_value(&actor).unwrap(),
            "invocation_path" => serde_json::to_value(TrustedHostPath::ControllerMaintenance).unwrap(),
            _ => serde_json::to_value(&actor.provenance).unwrap(),
        };
        // Full Request deserialization reaches the actual closed mutation DTO;
        // there is no caller-authoritative RecordedActor field at this boundary.
        let mut raw = serde_json::to_value(request(wait.clone())).unwrap();
        raw["input"]["mutation"]["input"].as_object_mut().unwrap().insert(selector.into(), value.clone());
        assert!(serde_json::from_value::<Request>(raw).is_err(), "Work accepted {selector}");
        let mut raw = serde_json::to_value(request(cancel.clone())).unwrap();
        raw["input"]["mutation"].as_object_mut().unwrap().insert(selector.into(), value.clone());
        assert!(serde_json::from_value::<Request>(raw).is_err(), "Question accepted {selector}");
        let forged = serde_json::to_string(&value).unwrap();
        let refused = browser(&c, &format!("/tasks/{}/coordination/waits", t.id), &[
            ("operation_id", format!("selector-work-{selector}")), ("expected_revision", "0".into()),
            ("resolver_kind", "question".into()), ("resolver_id", q.id.clone()), (selector, forged.clone()),
        ]).await;
        assert_eq!(refused.status, 400, "{}", refused.body);
        let refused = browser(&c, &format!("/questions/{}/cancel", q.id), &[
            ("operation_id", format!("selector-question-{selector}")), ("expected_revision", q.revision.to_string()),
            ("reason", "Forged attribution must not be used".into()), (selector, forged),
        ]).await;
        assert_eq!(refused.status, 400, "{}", refused.body);
    }
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(receipt_count(&p), receipts, "rejected boundary fields never reach a mutation receipt");
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert_eq!(activity(&c).await, before);
    assert_eq!(work(&c, &t.id).await.coordination_revision, 0);
    assert_eq!(question(&c, &q.id).await.question, q);
    // The identical legitimate operations remain usable without any selector.
    let accepted = call(&c, wait).await;
    assert!(matches!(accepted, Outcome::Ok { data: Data::WorkMutation(_) }));
    let waiting = work(&c, &t.id).await;
    assert_eq!(waiting.waits.items[0].created_by, host_actor(TrustedHostPath::OperatorApi));
    let accepted = browser(&c, &format!("/questions/{}/cancel", q.id), &[
        ("operation_id", "selector-valid-browser-cancel".into()), ("expected_revision", q.revision.to_string()),
        ("reason", "The current operator closes this question".into()),
    ]).await;
    assert_eq!(accepted.status, 303, "{}", accepted.body);
    let ended = work(&c, &t.id).await;
    match &ended.waits.items[0].status {
        WaitStatus::Resolved { actor, .. } => assert_eq!(actor, &host_actor(TrustedHostPath::Browser)),
        other => panic!("legitimate browser terminal transition expected: {other:?}"),
    }
}
