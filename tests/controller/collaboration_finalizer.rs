//! Finite finalizer regressions joined under tests/controller.rs.
//! Valid state is produced by Controller. SQL reads atomic-write evidence and
//! injects explicit offline corruption only after the actual owner shuts down.
use super::*;
use borg_runtime::collaboration::{questions::*, work::*, workflow::*};
use std::collections::BTreeMap;

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
    input.definition.execution_profile = Some(ExecutionProfile::CollaborationV2 { publication: None, web_search: None });
    input
}
async fn reply(c: &Controller, command: Operation) -> Reply {
    let r = c.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: format!("finalizer-request-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
        command: command.clone(),
    }).await;
    r.validate_for(&command).unwrap();
    r
}
async fn work(c: &Controller, id: &str) -> WorkView {
    match call(c, Operation::QueryWork { query: WorkQuery::Get { task_id: id.into() } }).await {
        Outcome::Ok { data: Data::WorkQuery(WorkQueryResult::Task(view)) } => view,
        other => panic!("work view expected: {other:?}"),
    }
}
async fn question(c: &Controller, id: &str) -> QuestionView {
    match call(c, Operation::QueryQuestion { query: QuestionQuery::Get { question_id: id.into() } }).await {
        Outcome::Ok { data: Data::QuestionQuery(value) } => match *value {
            QuestionQueryResult::Question(view) => view,
            other => panic!("question view expected: {other:?}"),
        },
        other => panic!("question expected: {other:?}"),
    }
}
async fn activity(c: &Controller) -> Vec<WorkflowActivity> {
    let mut all = Vec::new();
    let mut cursor = None;
    for _ in 0..32 {
        let page = match call(c, Operation::QueryWorkflowActivity { cursor: cursor.take(), limit: 8 }).await {
            Outcome::Ok { data: Data::WorkflowActivity(page) } => page,
            other => panic!("workflow activity expected: {other:?}"),
        };
        all.extend(page.items);
        cursor = page.next_cursor;
        if cursor.is_none() { break; }
    }
    assert!(cursor.is_none(), "finite fixture activity fits its explicit read bound");
    all
}
async fn setup(c: &Controller, question_resolver: bool) -> (Task, Question, [Task; 2]) {
    let source = submit(c, "finalizer-source", workflow_native("finalizer-source-owner")).await;
    let posed = PoseQuestion {
        record: RecordRef::Task { task_id: source.id.clone() },
        recipient: PrincipalRef::TrustedHostOperator,
        prompt: "Choose one option. Recommended: keep.".into(),
        options: vec![
            QuestionOption { id: "keep".into(), label: "Keep".into(), explanation: "Keep the existing choice".into() },
            QuestionOption { id: "change".into(), label: "Change".into(), explanation: "Use the stated alternative".into() },
        ],
        recommended_option_id: "keep".into(),
    };
    let q = match call(c, Operation::MutateQuestion {
        operation_id: "finalizer-question".into(), mutation: QuestionMutation::Pose { question: posed },
    }).await {
        Outcome::Ok { data: Data::QuestionMutation(value) } => value.question.question,
        other => panic!("posed question expected: {other:?}"),
    };
    let waiters = [
        submit(c, "finalizer-waiter-one", workflow_native("finalizer-waiter-one-owner")).await,
        submit(c, "finalizer-waiter-two", workflow_native("finalizer-waiter-two-owner")).await,
    ];
    for (index, waiter) in waiters.iter().enumerate() {
        let resolver = if question_resolver {
            ResolverRef::Question { question_id: q.id.clone() }
        } else { ResolverRef::Task { task_id: source.id.clone() } };
        let outcome = call(c, Operation::MutateWork {
            operation_id: format!("finalizer-wait-{index}"), mutation: WorkMutation::AddWait {
                task_id: waiter.id.clone(), expected_revision: 0, resolver,
            },
        }).await;
        assert!(matches!(outcome, Outcome::Ok { data: Data::WorkMutation(_) }), "{outcome:?}");
        assert_eq!(work(c, &waiter.id).await.open_waits, 1);
    }
    (source, q, waiters)
}
fn terminal_operation(id: &str, source: &Task, q: &Question, question_resolver: bool) -> Operation {
    if question_resolver {
        Operation::MutateQuestion { operation_id: id.into(), mutation: QuestionMutation::Cancel {
            question_id: q.id.clone(), expected_revision: q.revision, reason: "Finalizer terminal cause".into(),
        }}
    } else {
        Operation::CancelTask { operation_id: id.into(), task_id: source.id.clone(), expected_revision: source.revision }
    }
}

// A closed, finite projection excludes controller receipt counters: ordinary
// capacity refusal legitimately records one refusal receipt. All domain state,
// event destinations, session delivery and worker commands must remain exact.
fn durable_projection(path: &std::path::Path) -> BTreeMap<String, Vec<String>> {
    let db = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut projection = BTreeMap::new();
    for (name, sql) in [
        ("tasks", "SELECT json_array(id,status,active,body) FROM tasks ORDER BY id"),
        ("history", "SELECT json_array(seq,task_id,revision,kind,body) FROM history ORDER BY seq"),
        ("questions", "SELECT json_array(id,body) FROM workflow_questions ORDER BY id"),
        ("answers", "SELECT json_array(id,body) FROM workflow_answers ORDER BY id"),
        ("question_actions", "SELECT json_array(question_id,question_revision,body) FROM workflow_question_actions ORDER BY question_id,question_revision"),
        ("work_heads", "SELECT json_array(task_id,revision,body) FROM workflow_task_state ORDER BY task_id"),
        ("waits", "SELECT json_array(id,task_id,resolver_kind,resolver_id,closed,body) FROM workflow_waits ORDER BY id"),
        ("wait_closures", "SELECT json_array(wait_id,body) FROM workflow_wait_closures ORDER BY wait_id"),
        ("admitted_inputs", "SELECT json_array(wait_id,task_id,attempt_id,body,input_ack) FROM workflow_wait_consumptions ORDER BY wait_id,attempt_id"),
        ("events_and_destinations", "SELECT json_array(product_revision,ordinal,body,targets) FROM workflow_events ORDER BY product_revision,ordinal"),
        ("session_delivery", "SELECT json_array(id,body,workflow_delivery) FROM sessions ORDER BY id"),
        ("commands", "SELECT json_array(id,acknowledged,body,delivery_operation_id) FROM commands ORDER BY id"),
    ] {
        let mut statement = db.prepare(sql).unwrap();
        let rows = statement.query_map([], |row| row.get::<_, String>(0)).unwrap()
            .collect::<Result<Vec<_>, _>>().unwrap();
        projection.insert(name.into(), rows);
    }
    projection
}
fn receipt_count(path: &std::path::Path, id: &str) -> i64 {
    let db = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    db.query_row("SELECT count(*) FROM receipts WHERE id=?1", [id], |row| row.get(0)).unwrap()
}
fn destinations(path: &std::path::Path, event: &EventId) -> Vec<DeliveryScope> {
    let db = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let raw: String = db.query_row("SELECT targets FROM workflow_events WHERE product_revision=?1 AND ordinal=?2",
        rusqlite::params![i64::try_from(event.product_revision).unwrap(), i64::try_from(event.ordinal).unwrap()], |row| row.get(0)).unwrap();
    // The canonical event's immutable routing field chooses the exact stored
    // envelope. Audit-only events retain the original array representation.
    let body: String = db.query_row("SELECT body FROM workflow_events WHERE product_revision=?1 AND ordinal=?2",
        rusqlite::params![i64::try_from(event.product_revision).unwrap(), i64::try_from(event.ordinal).unwrap()], |row| row.get(0)).unwrap();
    let canonical: WorkflowActivity = serde_json::from_str(&body).unwrap();
    assert_eq!(&canonical.id, event);
    if canonical.routing.is_some() {
        let envelope: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let object = envelope.as_object().unwrap();
        assert_eq!(object.len(), 2);
        assert!(object.contains_key("concrete") && object.contains_key("route"));
        serde_json::from_value(object["concrete"].clone()).unwrap()
    } else {
        serde_json::from_str(&raw).unwrap()
    }
}
fn closed_event(events: &[WorkflowActivity], source: &Task, q: &Question, question_resolver: bool) -> WorkflowActivity {
    let found = events.iter().filter(|event| match &event.detail {
        ActivityDetail::Task(TaskChange::Closed { resolution }) => !question_resolver && resolution.task_id == source.id,
        ActivityDetail::Question(QuestionChange::Closed { resolution: QuestionResolution::Cancelled { question_id, .. } }) =>
            question_resolver && question_id == &q.id,
        _ => false,
    }).collect::<Vec<_>>();
    assert_eq!(found.len(), 1, "one first terminal occurrence, including after replay");
    (*found[0]).clone()
}
fn closed_outcome(event: &WorkflowActivity) -> ResolverOutcome {
    match &event.detail {
        ActivityDetail::Task(TaskChange::Closed { resolution }) => ResolverOutcome::Task(resolution.clone()),
        ActivityDetail::Question(QuestionChange::Closed { resolution }) => ResolverOutcome::Question(resolution.clone()),
        other => panic!("terminal activity expected: {other:?}"),
    }
}
async fn assert_wait_cascade(c: &Controller, destinations: &BTreeMap<EventId, Vec<DeliveryScope>>, waiters: &[Task; 2], closed: &WorkflowActivity) {
    let events = activity(c).await;
    let same_revision = events.iter().filter(|event| event.id.product_revision == closed.id.product_revision).collect::<Vec<_>>();
    assert_eq!(same_revision.len(), 3, "one terminal event and two typed unblock activities");
    assert!(same_revision.windows(2).all(|pair| pair[0].id < pair[1].id));
    let expected_outcome = closed_outcome(closed);
    for waiter in waiters {
        let view = work(c, &waiter.id).await;
        assert_eq!(view.open_waits, 0);
        assert_eq!(view.waits.items.len(), 1);
        assert_eq!(view.task.input, waiter.input);
        assert_eq!(view.task.session_id, waiter.session_id);
        assert!(view.task.result.is_none());
        let wait = &view.waits.items[0];
        let resolution = match &wait.status {
            WaitStatus::Resolved { resolution, .. } => resolution,
            other => panic!("resolved wait expected: {other:?}"),
        };
        assert_eq!(resolution.wait, wait.reference);
        assert_eq!(resolution.resolver, wait.resolver);
        assert_eq!(resolution.event_id, closed.id, "both waiters retain the original terminal occurrence");
        assert_eq!(resolution.outcome, expected_outcome);
        let unblock = same_revision.iter().filter(|event| matches!(&event.detail,
            ActivityDetail::Wait(WaitChange::Resolved { wait: reference, resolver, outcome })
                if reference == &wait.reference && resolver == &wait.resolver && outcome == &expected_outcome
        )).collect::<Vec<_>>();
        assert_eq!(unblock.len(), 1);
        assert_ne!(unblock[0].id, closed.id);
        let scope = DeliveryScope {
            task_id: waiter.id.clone(), session_id: waiter.session_id.clone(), agent_id: waiter.assigned_agent_id.clone(),
        };
        assert!(destinations.get(&closed.id).unwrap().contains(&scope),
            "the original terminal occurrence targets the existing waiting task/session");
        assert!(!destinations.get(&unblock[0].id).unwrap().contains(&scope),
            "the queryable audit sibling must not duplicate terminal input to its waiter");
    }
}
async fn assert_original_receipt(c: &Controller, id: &str, original: &Reply) {
    let lookup = reply(c, Operation::GetOperation { operation_id: id.into() }).await;
    let record = match lookup.outcome {
        Outcome::Ok { data: Data::Operation(record) } => record,
        other => panic!("original operation receipt expected: {other:?}"),
    };
    assert_eq!(serde_json::from_value::<Outcome>(record.outcome.unwrap()).unwrap(), original.outcome);
    assert_eq!(lookup.commit, original.commit);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//= type=test
//# A resolved wait MUST retain the exact terminal outcome occurrence that resolved it.
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//= type=test
//# An unblock activity MUST carry the typed outcome of each wait it ends.
async fn first_terminal_task_event_resolves_two_waiters_once_and_survives_replay_reopen() {
    let p = path();
    let cfg = workflow_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let (source, q, waiters) = setup(&c, false).await;
    let terminal = terminal_operation("finalizer-terminal-task", &source, &q, false);
    let committed = reply(&c, terminal.clone()).await;
    let closed_task = task(committed.outcome.clone());
    assert_eq!(closed_task.status, TaskStatus::Cancelled);
    let events = activity(&c).await;
    let closed = closed_event(&events, &source, &q, false);
    let actual = match &closed.detail {
        ActivityDetail::Task(TaskChange::Closed { resolution }) => resolution,
        _ => unreachable!(),
    };
    assert_eq!(actual.task_id, source.id);
    assert_eq!(actual.terminal_task_revision, closed_task.revision);
    assert_eq!(actual.status, TerminalTaskStatus::Cancelled);
    assert!(actual.accepted_result.is_none());
    assert_eq!(actual.summary, closed_task.blocked_reason.as_deref().unwrap_or_default());
    assert_eq!(question(&c, &q.id).await.question, q, "task cancellation does not rewrite an open question");
    let cascade_events = activity(&c).await;
    drop(c);
    controller_owner_released(&p).await;
    let cascade_destinations = cascade_events.iter().map(|event|
        (event.id.clone(), destinations(&p, &event.id))).collect::<BTreeMap<_, _>>();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert_wait_cascade(&c, &cascade_destinations, &waiters, &closed).await;
    drop(c);
    controller_owner_released(&p).await;
    let after = durable_projection(&p);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let replay = reply(&c, terminal.clone()).await;
    assert_eq!(replay.outcome, committed.outcome);
    assert_eq!(replay.commit, committed.commit);
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(durable_projection(&p), after);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert_original_receipt(&c, "finalizer-terminal-task", &committed).await;
    // A fresh no-op cancellation is not another terminal transition either.
    task(call(&c, Operation::CancelTask {
        operation_id: "finalizer-already-terminal".into(), task_id: source.id.clone(), expected_revision: closed_task.revision,
    }).await);
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(durable_projection(&p), after);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert_eq!(activity(&c).await, events);
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(durable_projection(&p), after);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert_eq!(closed_event(&activity(&c).await, &source, &q, false), closed);
    let cascade_events = activity(&c).await;
    drop(c);
    controller_owner_released(&p).await;
    let cascade_destinations = cascade_events.iter().map(|event|
        (event.id.clone(), destinations(&p, &event.id))).collect::<BTreeMap<_, _>>();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert_wait_cascade(&c, &cascade_destinations, &waiters, &closed).await;
    let replay = reply(&c, terminal).await;
    assert_eq!(replay.outcome, committed.outcome);
    assert_eq!(replay.commit, committed.commit);
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(receipt_count(&p, "finalizer-terminal-task"), 1);
}

async fn bounded_terminal_rollback(question_resolver: bool, event_bound: bool) {
    let p = path();
    let mut cfg = workflow_config();
    let policy = Arc::make_mut(&mut cfg.limits).workflow.as_mut().unwrap();
    if event_bound { policy.events_per_mutation = 2; } else { policy.revisions = 4; }
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let (source, q, waiters) = setup(&c, question_resolver).await;
    // Setup has only the question pose and two single-event AddWait operations.
    // The terminal cascade needs a Closed event plus two new unblock events;
    // the shared event limit is hit after the domain's two-wait bound permits it.
    assert_eq!(activity(&c).await.len(), 3);
    drop(c);
    controller_owner_released(&p).await;
    let before = durable_projection(&p);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let source_before = get(&c, &source.id).await;
    let question_before = question(&c, &q.id).await;
    let waiter_before = [work(&c, &waiters[0].id).await, work(&c, &waiters[1].id).await];
    let operation = terminal_operation("finalizer-capacity", &source, &q, question_resolver);
    let refused = reply(&c, operation.clone()).await;
    assert_eq!(code(refused.outcome.clone()), "capacity");
    assert!(refused.commit.is_some(), "normal refusal keeps the original refusal receipt");
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(durable_projection(&p), before, "the terminal domain mutation, cascade and delivery must all roll back");
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert_eq!(get(&c, &source.id).await, source_before);
    assert_eq!(question(&c, &q.id).await, question_before);
    for (waiter, original) in waiters.iter().zip(&waiter_before) {
        assert_eq!(&work(&c, &waiter.id).await, original);
        assert_eq!(original.open_waits, 1);
    }
    assert_original_receipt(&c, "finalizer-capacity", &refused).await;
    let replay = reply(&c, operation).await;
    assert_eq!(replay.outcome, refused.outcome);
    assert_eq!(replay.commit, refused.commit);
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(durable_projection(&p), before);
    assert_eq!(receipt_count(&p, "finalizer-capacity"), 1);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//= type=test
//# A resolver's terminal transition MUST commit its typed outcome, affected wait resolutions and delivery associations atomically.
async fn shared_event_bound_rolls_back_task_and_question_terminal_cascades() {
    for question_resolver in [false, true] { bounded_terminal_rollback(question_resolver, true).await; }
}

#[tokio::test]
async fn global_immutable_capacity_rolls_back_task_and_question_terminal_cascades() {
    for question_resolver in [false, true] { bounded_terminal_rollback(question_resolver, false).await; }
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn terminal_commit_cut_leaves_no_receipt_and_same_identity_applies_exactly_once() {
    for question_resolver in [false, true] {
        let p = path();
        let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut cfg = workflow_config();
        cfg.qualification_fail_next_commit = Some(fail.clone());
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        let (source, q, waiters) = setup(&c, question_resolver).await;
        drop(c);
        controller_owner_released(&p).await;
        let before = durable_projection(&p);
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        let terminal = terminal_operation("finalizer-commit-cut", &source, &q, question_resolver);
        fail.store(true, Ordering::SeqCst);
        let failed = reply(&c, terminal.clone()).await;
        assert!(!fail.load(Ordering::SeqCst), "actual outer COMMIT hook consumed");
        assert_eq!(code(failed.outcome), "store_unavailable");
        assert!(failed.commit.is_none());
        drop(c);
        controller_owner_released(&p).await;
        assert_eq!(durable_projection(&p), before);
        assert_eq!(receipt_count(&p, "finalizer-commit-cut"), 0);
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        let missing = reply(&c, Operation::GetOperation { operation_id: "finalizer-commit-cut".into() }).await;
        assert!(matches!(missing.outcome, Outcome::Refused { .. }));
        // Retry the original operation and expected revision after known rollback.
        let committed = reply(&c, terminal.clone()).await;
        assert!(matches!(committed.outcome, Outcome::Ok { .. }), "{committed:?}");
        assert!(committed.commit.is_some());
        let closed = closed_event(&activity(&c).await, &source, &q, question_resolver);
        let cascade_events = activity(&c).await;
        drop(c);
        controller_owner_released(&p).await;
        let cascade_destinations = cascade_events.iter().map(|event|
            (event.id.clone(), destinations(&p, &event.id))).collect::<BTreeMap<_, _>>();
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        assert_wait_cascade(&c, &cascade_destinations, &waiters, &closed).await;
        drop(c);
        controller_owner_released(&p).await;
        let after = durable_projection(&p);
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        let replay = reply(&c, terminal).await;
        assert_eq!(replay.outcome, committed.outcome);
        assert_eq!(replay.commit, committed.commit);
        drop(c);
        controller_owner_released(&p).await;
        assert_eq!(durable_projection(&p), after);
        assert_eq!(receipt_count(&p, "finalizer-commit-cut"), 1);
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        assert_original_receipt(&c, "finalizer-commit-cut", &committed).await;
    }
}

// The terminal history is independent of the current task head after RetryTask.
// The finite query deliberately retains the exact serialized body and SQL key.
fn terminal_history(path: &std::path::Path, task_id: &str) -> Vec<(i64, u64, String)> {
    let db = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut statement = db.prepare(
        "SELECT seq,revision,body FROM history WHERE task_id=?1 AND kind='workflow_task_closed' ORDER BY seq LIMIT 4"
    ).unwrap();
    let rows = statement.query_map([task_id], |row| Ok((row.get(0)?, u64::try_from(row.get::<_, i64>(1)?).unwrap(), row.get(2)?))).unwrap()
        .collect::<std::result::Result<Vec<_>, _>>().unwrap();
    rows
}
fn task_resolution(event: &WorkflowActivity) -> TaskResolution {
    match &event.detail {
        ActivityDetail::Task(TaskChange::Closed { resolution }) => resolution.clone(),
        other => panic!("Task terminal occurrence expected: {other:?}"),
    }
}
fn assert_terminal_history(path: &std::path::Path, event: &WorkflowActivity) {
    let resolution = task_resolution(event);
    let rows = terminal_history(path, &resolution.task_id).into_iter()
        .filter(|(_, revision, _)| *revision == resolution.terminal_task_revision).collect::<Vec<_>>();
    assert_eq!(rows.len(), 1);
    assert_eq!(serde_json::from_str::<TaskResolution>(&rows[0].2).unwrap(), resolution);
    assert_eq!(rows[0].2, serde_json::to_string(&resolution).unwrap(), "bare canonical TaskResolution is retained");
}

#[tokio::test]
async fn retry_reopen_preserves_original_task_closed_history_and_receipt() {
    let p = path();
    let cfg = workflow_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let (source, q, waiters) = setup(&c, false).await;
    let terminal = terminal_operation("history-original-cancel", &source, &q, false);
    let original = reply(&c, terminal.clone()).await;
    let cancelled = task(original.outcome.clone());
    let first_events = activity(&c).await;
    let first = closed_event(&first_events, &source, &q, false);
    drop(c);
    controller_owner_released(&p).await;
    assert_terminal_history(&p, &first);
    let first_history = terminal_history(&p, &source.id);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert_eq!(first_history.len(), 1);
    let queued = task(call(&c, Operation::RetryTask { operation_id: "history-retry".into(),
        task_id: source.id.clone(), expected_revision: cancelled.revision }).await);
    assert_eq!(queued.status, TaskStatus::Queued);
    assert!(queued.revision > task_resolution(&first).terminal_task_revision);
    assert_eq!(queued.session_id, source.session_id);
    assert_eq!(queued.input, source.input);
    assert_eq!(activity(&c).await, first_events);
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(terminal_history(&p, &source.id), first_history);
    let before_reopen = durable_projection(&p);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert_eq!(get(&c, &source.id).await, queued);
    assert_eq!(activity(&c).await, first_events);
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(terminal_history(&p, &source.id), first_history);
    assert_eq!(durable_projection(&p), before_reopen);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let cascade_events = activity(&c).await;
    drop(c);
    controller_owner_released(&p).await;
    let cascade_destinations = cascade_events.iter().map(|event|
        (event.id.clone(), destinations(&p, &event.id))).collect::<BTreeMap<_, _>>();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert_wait_cascade(&c, &cascade_destinations, &waiters, &first).await;
    let replay = reply(&c, terminal.clone()).await;
    assert_eq!(replay.outcome, original.outcome);
    assert_eq!(replay.commit, original.commit);
    assert_eq!(get(&c, &source.id).await, queued, "original receipt replay does not recancel the retried task");
    assert_original_receipt(&c, "history-original-cancel", &original).await;
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(terminal_history(&p, &source.id), first_history);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    // A later genuine terminal transition has another revision and occurrence;
    // uniqueness is by exact task/revision, not one lifetime row per task.
    let second = task(call(&c, Operation::CancelTask { operation_id: "history-second-cancel".into(),
        task_id: source.id.clone(), expected_revision: queued.revision }).await);
    let events = activity(&c).await;
    let terminal_events = events.iter().filter(|event| matches!(&event.detail,
        ActivityDetail::Task(TaskChange::Closed { resolution }) if resolution.task_id == source.id
    )).cloned().collect::<Vec<_>>();
    assert_eq!(terminal_events.len(), 2);
    assert_eq!(terminal_events[0], first);
    assert_eq!(task_resolution(&terminal_events[1]).terminal_task_revision, second.revision);
    assert_ne!(terminal_events[1].id, first.id);
    drop(c);
    controller_owner_released(&p).await;
    assert_terminal_history(&p, &terminal_events[1]);
    let rows = terminal_history(&p, &source.id);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], first_history[0]);
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert_eq!(activity(&c).await, events);
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(terminal_history(&p, &source.id), rows);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let cascade_events = activity(&c).await;
    drop(c);
    controller_owner_released(&p).await;
    let cascade_destinations = cascade_events.iter().map(|event|
        (event.id.clone(), destinations(&p, &event.id))).collect::<BTreeMap<_, _>>();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert_wait_cascade(&c, &cascade_destinations, &waiters, &first).await;
    let replay = reply(&c, terminal).await;
    assert_eq!(replay.outcome, original.outcome);
    assert_eq!(replay.commit, original.commit);
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(receipt_count(&p, "history-original-cancel"), 1);
    assert_eq!(terminal_history(&p, &source.id), rows);
}

// Actual controller mutations produce the valid store. Deliberate corruption
// occurs only after its real database owner has shut down and released the lock.
async fn retained_terminal_history_fixture(succeeded: bool) -> (PathBuf, ControllerConfig, WorkflowActivity) {
    let p = path();
    let cfg = workflow_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let (source, q, _) = setup(&c, false).await;
    if succeeded {
        let w = worker("history-accepted-worker", "inc", 1, 0);
        register(&c, &w).await;
        let a = run(&commands(&c, &w, vec![]).await);
        assert_eq!(a.task_id, source.id);
        let accepted = finish_native(&c, &a, "history-accepted").await;
        assert_eq!(accepted.status, TaskStatus::Succeeded);
    } else {
        let cancelled = task(call(&c, terminal_operation("history-fixture-cancel", &source, &q, false)).await);
        let queued = task(call(&c, Operation::RetryTask { operation_id: "history-fixture-retry".into(),
            task_id: source.id.clone(), expected_revision: cancelled.revision }).await);
        assert_eq!(queued.status, TaskStatus::Queued);
    }
    let event = closed_event(&activity(&c).await, &source, &q, false);
    drop(c);
    controller_owner_released(&p).await;
    assert_terminal_history(&p, &event);
    (p, cfg, event)
}

// Full retained bytes, including database, WAL/SHM, lock and helper-registry
// binding files. No hashes or selected-row projection can conceal a rewrite.
fn history_fixture_bytes(path: &std::path::Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &std::path::Path, here: &std::path::Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(here).unwrap() {
            let path = entry.unwrap().path();
            let metadata = std::fs::symlink_metadata(&path).unwrap();
            if metadata.is_dir() { visit(root, &path, out); }
            else {
                assert!(metadata.is_file());
                out.insert(path.strip_prefix(root).unwrap().to_owned(), std::fs::read(path).unwrap());
            }
        }
    }
    let root = path.parent().unwrap();
    let mut bytes = BTreeMap::new();
    visit(root, root, &mut bytes);
    bytes
}
async fn assert_history_reopen_refused_unchanged(path: &std::path::Path, cfg: ControllerConfig, defect: &str) {
    let before = history_fixture_bytes(path);
    for _ in 0..2 {
        assert!(Controller::open_with_config(path, cfg.clone()).is_err(), "corrupt terminal occurrence admitted: {defect}");
        controller_owner_released(path).await;
        assert_eq!(history_fixture_bytes(path), before, "refused startup rewrote retained bytes: {defect}");
    }
}

// Keep every mutable copy mutually consistent so startup must authenticate the
// terminal claim against immutable history rather than just compare its copies.
fn replace_resolution_copies(db: &rusqlite::Connection, original: &WorkflowActivity, forged: &TaskResolution) {
    let original_resolution = task_resolution(original);
    let events = {
        let mut statement = db.prepare("SELECT body FROM workflow_events WHERE product_revision=?1 ORDER BY ordinal").unwrap();
        let rows = statement.query_map([i64::try_from(original.id.product_revision).unwrap()], |r| r.get::<_, String>(0)).unwrap()
            .map(|row| serde_json::from_str::<WorkflowActivity>(&row.unwrap()).unwrap()).collect::<Vec<_>>();
        rows
    };
    assert_eq!(events.len(), 3);
    for mut event in events {
        match &mut event.detail {
            ActivityDetail::Task(TaskChange::Closed { resolution }) => {
                assert_eq!(*resolution, original_resolution);
                *resolution = forged.clone();
            },
            ActivityDetail::Wait(WaitChange::Resolved { outcome: ResolverOutcome::Task(resolution), .. }) => {
                assert_eq!(*resolution, original_resolution);
                *resolution = forged.clone();
            },
            other => panic!("only the original terminal occurrence and its two audit copies: {other:?}"),
        }
        assert_eq!(db.execute("UPDATE workflow_events SET body=?3 WHERE product_revision=?1 AND ordinal=?2",
            rusqlite::params![i64::try_from(event.id.product_revision).unwrap(), i64::try_from(event.id.ordinal).unwrap(), serde_json::to_string(&event).unwrap()]).unwrap(), 1);
    }
    let waits = {
        let mut statement = db.prepare("SELECT body FROM workflow_waits WHERE resolver_kind='task' AND resolver_id=?1 AND closed=1 ORDER BY id").unwrap();
        let rows = statement.query_map([&original_resolution.task_id], |r| r.get::<_, String>(0)).unwrap()
            .map(|row| serde_json::from_str::<WorkflowWait>(&row.unwrap()).unwrap()).collect::<Vec<_>>();
        rows
    };
    assert_eq!(waits.len(), 2);
    for mut wait in waits {
        match &mut wait.status {
            WaitStatus::Resolved { resolution, .. } => {
                assert_eq!(resolution.event_id, original.id);
                assert_eq!(resolution.outcome, ResolverOutcome::Task(original_resolution.clone()));
                resolution.outcome = ResolverOutcome::Task(forged.clone());
            },
            other => panic!("original closed wait expected: {other:?}"),
        }
        assert_eq!(db.execute("UPDATE workflow_waits SET body=?2 WHERE id=?1",
            rusqlite::params![wait.reference.wait_id, serde_json::to_string(&wait).unwrap()]).unwrap(), 1);
        assert_eq!(db.execute("UPDATE workflow_wait_closures SET body=?2 WHERE wait_id=?1",
            rusqlite::params![wait.reference.wait_id, serde_json::to_string(&wait.status).unwrap()]).unwrap(), 1);
    }
    let associations: i64 = db.query_row("SELECT count(*) FROM workflow_wait_consumptions", [], |r| r.get(0)).unwrap();
    assert_eq!(associations, 0, "fixture has not admitted the waiting tasks");
}

#[tokio::test]
async fn startup_rejects_rewritten_terminal_events_even_with_matching_wait_copies() {
    for defect in ["status", "summary", "accepted_result"] {
        let (p, cfg, event) = retained_terminal_history_fixture(defect == "accepted_result").await;
        let original = task_resolution(&event);
        let history_before = terminal_history(&p, &original.task_id);
        let mut forged = original.clone();
        match defect {
            "status" => { assert_eq!(forged.status, TerminalTaskStatus::Cancelled); forged.status = TerminalTaskStatus::Failed; },
            "summary" => forged.summary = "This is not the recorded original terminal summary".into(),
            "accepted_result" => {
                assert_eq!(forged.status, TerminalTaskStatus::Succeeded);
                assert!(forged.accepted_result.take().is_some(), "reference comes from actual accepted native completion");
            },
            _ => unreachable!(),
        }
        let before = durable_projection(&p);
        let db = rusqlite::Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE).unwrap();
        db.execute_batch("BEGIN IMMEDIATE").unwrap();
        replace_resolution_copies(&db, &event, &forged);
        db.execute_batch("COMMIT").unwrap();
        db.close().unwrap();
        assert_eq!(terminal_history(&p, &original.task_id), history_before, "immutable occurrence was not forged");
        let after = durable_projection(&p);
        assert_eq!(after["tasks"], before["tasks"], "current Task is deliberately untouched");
        assert_eq!(after["history"], before["history"]);
        assert_ne!(after["events_and_destinations"], before["events_and_destinations"]);
        assert_history_reopen_refused_unchanged(&p, cfg, defect).await;
    }
}

#[tokio::test]
async fn startup_refuses_missing_duplicate_or_corrupt_terminal_history_without_repair() {
    for defect in ["missing", "duplicate", "malformed", "task_id", "terminal_revision", "summary", "over_bound"] {
        let (p, cfg, event) = retained_terminal_history_fixture(false).await;
        let original = task_resolution(&event);
        let rows = terminal_history(&p, &original.task_id);
        assert_eq!(rows.len(), 1);
        let (sequence, revision, _) = &rows[0];
        assert_eq!(*revision, original.terminal_task_revision);
        let before = durable_projection(&p);
        let db = rusqlite::Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE).unwrap();
        match defect {
            "missing" => { assert_eq!(db.execute("DELETE FROM history WHERE seq=?1", [sequence]).unwrap(), 1); },
            "duplicate" => {
                assert_eq!(db.execute("INSERT INTO history(task_id,revision,kind,body) SELECT task_id,revision,kind,body FROM history WHERE seq=?1",
                    [sequence]).unwrap(), 1);
            },
            _ => {
                let mut forged = original.clone();
                let raw = match defect {
                    "malformed" => "{\"task_id\":".to_owned(),
                    "task_id" => { forged.task_id.push_str("-different"); serde_json::to_string(&forged).unwrap() },
                    "terminal_revision" => { forged.terminal_task_revision += 1; serde_json::to_string(&forged).unwrap() },
                    "summary" => { forged.summary = "Different retained historical outcome".into(); serde_json::to_string(&forged).unwrap() },
                    "over_bound" => {
                        forged.summary = "x".repeat(cfg.limits.workflow.as_ref().unwrap().event_bytes + 1);
                        serde_json::to_string(&forged).unwrap()
                    },
                    _ => unreachable!(),
                };
                assert_eq!(db.execute("UPDATE history SET body=?2 WHERE seq=?1", rusqlite::params![sequence, raw]).unwrap(), 1);
            },
        }
        db.close().unwrap();
        let after = durable_projection(&p);
        assert_ne!(after["history"], before["history"]);
        for key in ["tasks", "events_and_destinations", "waits", "wait_closures", "admitted_inputs", "session_delivery"] {
            assert_eq!(after[key], before[key], "only the immutable occurrence was corrupted: {defect}/{key}");
        }
        assert_history_reopen_refused_unchanged(&p, cfg, defect).await;
    }
}

// Observe only a released controller's real database. The cardinalities below
// are fixture bounds (four receipts, two versions), not production policy.
#[cfg(debug_assertions)]
fn records_revision_projection(path: &std::path::Path) -> (u64, u64, BTreeMap<String, Vec<String>>) {
    let mut rows = durable_projection(path);
    let db = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let product: String = db.query_row("SELECT value FROM settings WHERE name='workflow_revision'", [], |r| r.get(0)).unwrap();
    let product = product.parse::<u64>().unwrap();
    let controller: i64 = db.query_row("SELECT value FROM counters WHERE kind='controller_revision'", [], |r| r.get(0)).unwrap();
    for (key, count_sql, maximum, sql) in [
        ("documents", "SELECT count(*) FROM workflow_documents", 1i64,
         "SELECT json_array(id,revision,created_product_revision,body) FROM workflow_documents ORDER BY id LIMIT 2"),
        ("versions", "SELECT count(*) FROM workflow_document_versions", 2,
         "SELECT json_array(document_id,version,body) FROM workflow_document_versions ORDER BY document_id,version LIMIT 3"),
        ("receipts", "SELECT count(*) FROM receipts", 4,
         "SELECT json_array(id,request,outcome,cleanup,controller_revision,governance) FROM receipts ORDER BY controller_revision LIMIT 5"),
    ] {
        let count: i64 = db.query_row(count_sql, [], |r| r.get(0)).unwrap();
        assert!(count <= maximum, "fixture inventory exceeded: {key}/{count}");
        let values = db.prepare(sql).unwrap().query_map([], |r| r.get::<_, String>(0)).unwrap()
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(values.len(), usize::try_from(count).unwrap());
        rows.insert(key.into(), values);
    }
    assert!(rows["tasks"].is_empty(), "document fixtures never fabricate task subjects");
    assert!(rows["history"].is_empty());
    db.close().unwrap();
    (product, u64::try_from(controller).unwrap(), rows)
}

#[cfg(debug_assertions)]
#[tokio::test]

// Traceability scope: Receipt noise and failed COMMIT consume no collaboration revision; retry, replay and reopen preserve exact ordering.
//= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
//= type=test
//# A committed collaboration change MUST receive the next collaboration revision in commit order.

async fn workflow_revision_tracks_only_committed_collaboration_changes_across_receipt_noise_and_commit_cut() {
    use borg_runtime::collaboration::documents::{
        DocumentContent, DocumentMutation, DocumentMutationOutcome, DocumentQuery,
        DocumentQueryResult, DocumentView,
    };
    fn view(outcome: &Outcome) -> DocumentView {
        match outcome {
            Outcome::Ok { data: Data::DocumentMutation(DocumentMutationOutcome::Document { view }) } => view.clone(),
            other => panic!("document mutation expected: {other:?}"),
        }
    }
    async fn read(c: &Controller, document_id: &str, version: Option<u64>) -> DocumentView {
        match call(c, Operation::QueryDocument { query: DocumentQuery::Get { document_id: document_id.into(), version } }).await {
            Outcome::Ok { data: Data::DocumentQuery(DocumentQueryResult::Document(view)) } => view,
            other => panic!("document read expected: {other:?}"),
        }
    }

    let p = path();
    let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut cfg = workflow_config();
    cfg.qualification_fail_next_commit = Some(fail.clone());
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert!(activity(&c).await.is_empty());
    drop(c);
    controller_owner_released(&p).await;
    let (product_base, receipt_base, initial) = records_revision_projection(&p);
    assert!(initial["receipts"].is_empty());

    let registration = Operation::RegisterWorker {
        operation_id: "records-revision-register".into(),
        worker: worker("records-revision-worker", "records-revision-incarnation", 1, 0),
    };
    let missing_task = Operation::CancelTask {
        operation_id: "records-revision-missing-task".into(),
        task_id: "records-revision-no-task".into(), expected_revision: 1,
    };
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let registered = reply(&c, registration.clone()).await;
    assert!(matches!(&registered.outcome, Outcome::Ok { data: Data::Worker(_) }));
    assert_eq!(registered.commit.as_ref().unwrap().controller_revision, receipt_base + 1);
    let refused = reply(&c, missing_task.clone()).await;
    assert_eq!(code(refused.outcome.clone()), "not_found");
    assert_eq!(refused.commit.as_ref().unwrap().controller_revision, receipt_base + 2);
    assert!(activity(&c).await.is_empty());
    drop(c);
    controller_owner_released(&p).await;
    let noise = records_revision_projection(&p);
    assert_eq!((noise.0, noise.1), (product_base, receipt_base + 2));
    assert_eq!(noise.2["receipts"].len(), 2);
    assert!(noise.2["documents"].is_empty());
    assert!(noise.2["versions"].is_empty());

    let create = Operation::MutateDocument {
        operation_id: "records-revision-create".into(),
        mutation: DocumentMutation::Create {
            content: DocumentContent { title: "Revision ordering".into(), body: "First immutable version".into() },
            supplied_content_id: None, responsible_owner: PrincipalRef::TrustedHostOperator,
        },
    };
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let created = reply(&c, create.clone()).await;
    let first = view(&created.outcome);
    assert_eq!(created.commit.as_ref().unwrap().controller_revision, receipt_base + 3);
    assert_eq!(first.document.created_product_revision, product_base + 1);
    assert_eq!(first.document.changed_product_revision, product_base + 1);
    assert_eq!(first.version.product_revision, product_base + 1);
    assert_eq!(first.version.operation_id, "records-revision-create");
    let first_events = activity(&c).await;
    assert_eq!(first_events.len(), 1);
    assert_eq!(first_events[0].id.product_revision, product_base + 1);
    assert_eq!(first_events[0].id.ordinal, 0);
    assert_eq!(first_events[0].subject, RecordRef::Document { document_id: first.document.id.clone() });
    assert!(matches!(&first_events[0].detail, ActivityDetail::Document(DocumentChange::Created { version, .. }) if version == &first.version.reference));
    assert_ne!(first_events[0].id.product_revision, created.commit.as_ref().unwrap().controller_revision);
    drop(c);
    controller_owner_released(&p).await;
    let before_cut = records_revision_projection(&p);
    assert_eq!((before_cut.0, before_cut.1), (product_base + 1, receipt_base + 3));
    assert_eq!(before_cut.2["receipts"].len(), 3);

    let append = Operation::MutateDocument {
        operation_id: "records-revision-append".into(),
        mutation: DocumentMutation::AppendVersion {
            document_id: first.document.id.clone(), expected_revision: first.document.revision,
            content: DocumentContent { title: "Revision ordering".into(), body: "Second immutable version".into() },
            supplied_content_id: None,
        },
    };
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    fail.store(true, Ordering::SeqCst);
    let failed = reply(&c, append.clone()).await;
    assert!(!fail.load(Ordering::SeqCst), "the actual outer COMMIT hook was consumed");
    assert_eq!(code(failed.outcome.clone()), "store_unavailable");
    assert!(failed.commit.is_none());
    assert!(failed.observed_controller_revision.is_none());
    assert_eq!(read(&c, &first.document.id, None).await, first);
    assert_eq!(activity(&c).await, first_events);
    assert_eq!(code(reply(&c, Operation::GetOperation { operation_id: "records-revision-append".into() }).await.outcome), "not_found");
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(records_revision_projection(&p), before_cut,
        "failed COMMIT cannot publish a record, event, receipt, or either counter");

    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert_eq!(code(reply(&c, Operation::GetOperation { operation_id: "records-revision-append".into() }).await.outcome), "not_found");
    let committed = reply(&c, append.clone()).await;
    let second = view(&committed.outcome);
    assert_ne!(committed.request_id, failed.request_id);
    assert_eq!(committed.commit.as_ref().unwrap().operation_id, "records-revision-append");
    assert_eq!(committed.commit.as_ref().unwrap().controller_revision, receipt_base + 4);
    assert_eq!(second.document.id, first.document.id);
    assert_eq!(second.document.created_product_revision, product_base + 1);
    assert_eq!(second.document.changed_product_revision, product_base + 2);
    assert_eq!(second.version.product_revision, product_base + 2);
    assert_eq!(second.version.reference.version, first.version.reference.version + 1);
    assert_eq!(second.version.operation_id, "records-revision-append");
    assert_eq!(read(&c, &first.document.id, Some(1)).await.version, first.version);
    let events = activity(&c).await;
    assert_eq!(events.len(), 2);
    assert_eq!(events[0], first_events[0]);
    assert_eq!(events[1].id.database_id, events[0].id.database_id);
    assert_eq!(events[1].id.product_revision, product_base + 2);
    assert_eq!(events[1].id.ordinal, 0);
    assert_eq!(events[1].subject, RecordRef::Document { document_id: first.document.id.clone() });
    assert_eq!(events[1].subject_revision, second.document.revision);
    assert!(matches!(&events[1].detail, ActivityDetail::Document(DocumentChange::VersionAdded { previous, version, .. }) if previous == &first.version.reference && version == &second.version.reference));
    assert_ne!(events[1].id.product_revision, committed.commit.as_ref().unwrap().controller_revision);
    drop(c);
    controller_owner_released(&p).await;
    let after_commit = records_revision_projection(&p);
    assert_eq!((after_commit.0, after_commit.1), (product_base + 2, receipt_base + 4));
    assert_eq!(after_commit.2["receipts"].len(), 4);
    assert_eq!(after_commit.2["documents"].len(), 1);
    assert_eq!(after_commit.2["versions"].len(), 2);

    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    for (operation, original) in [
        (&registration, &registered), (&missing_task, &refused), (&create, &created), (&append, &committed),
    ] {
        let replay = reply(&c, operation.clone()).await;
        assert_ne!(replay.request_id, original.request_id);
        assert_eq!(replay.outcome, original.outcome);
        assert_eq!(replay.commit, original.commit);
        assert_original_receipt(&c, operation.mutation_id().unwrap(), original).await;
    }
    assert_eq!(read(&c, &first.document.id, None).await, second);
    assert_eq!(activity(&c).await, events);
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(records_revision_projection(&p), after_commit, "replays and reads consume neither counter");
    let c = Controller::open_with_config(&p, cfg).unwrap();
    assert_eq!(read(&c, &first.document.id, None).await, second);
    assert_eq!(activity(&c).await, events);
    assert_original_receipt(&c, "records-revision-append", &committed).await;
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(records_revision_projection(&p), after_commit, "reopen preserves the complete committed projection");
}
