//! Actual owner maintenance under linked SQLite contention. Numeric timings are
//! fixture policy only. Native admission uses the genuine BORG_TEST_RUNTIME
//! helper; no provider, process-completion report or accepted state is invented.
use super::*;
use crate::controller::database::StoredTask;

fn connection(lab: &Lab) -> Connection {
    let sql = Connection::open(&lab.path).unwrap();
    // Observation may briefly overlap the owner's successful exclusive COMMIT;
    // only the real owner connection uses busy_timeout(0) for retry evidence.
    sql.busy_timeout(Duration::from_secs(1)).unwrap();
    sql
}

fn hold_reader(lab: &Lab) -> Connection {
    let sql = connection(lab);
    sql.execute_batch("BEGIN").unwrap();
    assert!(sql_count(&sql, "tasks") > 0);
    sql
}

fn stored_task(sql: &Connection, id: &str) -> Task {
    let body: String = sql.query_row("SELECT body FROM tasks WHERE id=?1", [id], |r| r.get(0)).unwrap();
    serde_json::from_str::<StoredTask>(&body).unwrap().task
}

fn deadline_events(sql: &Connection, id: &str) -> i64 {
    sql.query_row("SELECT count(*) FROM history WHERE task_id=?1 AND kind='deadline'", [id], |r| r.get(0)).unwrap()
}

async fn submit(lab: &Lab, name: &str, deadline: u64) -> (Request, Reply, Task) {
    let mut input = TaskInput { instruction_source: None,
        pull_request: None,
        resolved_limits: None, repository_read: None,
        instruction: "produce one structured answer".into(),
        definition: Definition { charter: None, limit_overrides: None,
            execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
            inference: Some(InferenceSettings::selected()), id: format!("agent-{name}"),
            revision: "1".into(), executor: ExecutorKind::Native },
        repository: None, acceptance_commands: vec![],
        output_schema: Some(serde_json::json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false})),
        limits: lab.controller().limits().task_defaults.at(lab.clock.load(Ordering::SeqCst)).unwrap(),
        dependencies: vec![], native_workflow: None,
    };
    input.limits.deadline_unix_ms = deadline;
    let original = request(&format!("submit-{name}"), Operation::SubmitTask {
        operation_id: format!("original-{name}"), input: (input).into(),
    });
    let reply = tokio::time::timeout(Duration::from_secs(10),
        Box::pin(lab.controller().handle(original.clone()))).await.expect("genuine helper admission must finish");
    let accepted_task = match &reply.outcome {
        Outcome::Ok { data: Data::Task(task) } => (**task).clone(),
        _ => panic!("actual native task was not admitted: {reply:?}"),
    };
    // A reply may include derived admission diagnostics absent from the stored
    // task body. Retain actual canonical bytes as the no-change oracle.
    let task = stored_task(&connection(lab), &accepted_task.id);
    assert!(task.input.resolved_limits.is_some());
    assert_eq!(task.status, TaskStatus::Queued);
    assert_eq!(task.input.limits.deadline_unix_ms, deadline);
    (original, reply, task)
}

async fn revision_read(lab: &Lab) -> Result<u64, String> {
    // The real UI revision message does not itself require COMMIT in DELETE
    // mode. A normal Get/List transaction would share the reader's lock issue.
    let (reply, response) = oneshot::channel();
    if lab.controller().inner.send(Message::Revision { reply }).is_err() {
        panic!("maintenance owner unexpectedly unavailable");
    }
    tokio::time::timeout(Duration::from_secs(1), response).await
        .expect("revision read must not wait for maintenance backoff").unwrap()
}

async fn park_expiry(lab: &Lab, deadline: u64) -> usize {
    let before = lab.seen.rollbacks.load(Ordering::SeqCst);
    lab.clock.store(deadline, Ordering::SeqCst);
    revision_read(lab).await.unwrap();
    until("real expiry COMMIT must fail and explicitly roll back", ||
        lab.seen.rollbacks.load(Ordering::SeqCst) > before).await;
    lab.seen.rollbacks.load(Ordering::SeqCst)
}

fn commands(reply: Reply) -> Vec<WorkerCommand> {
    match reply.outcome {
        Outcome::Ok { data: Data::Commands(commands) } => commands,
        other => panic!("expected committed command exchange: {other:?}"),
    }
}

fn await_commands(id: &str, worker: &Worker, acknowledged: Vec<String>) -> Request {
    request(id, Operation::AwaitCommands { operation_id: id.into(),
        worker_id: worker.id.clone(), incarnation: worker.incarnation.clone(), acknowledged })
}

#[tokio::test]
async fn expiry_rollback_preserves_replay_and_recovers_future_deadline_without_new_messages() {
    let mut lab = Lab::open(8, 20, 2_000);
    let first_deadline = lab.clock.load(Ordering::SeqCst) + 60_000;
    let (original, accepted, first) = Box::pin(submit(&lab, "expire-first", first_deadline)).await;
    let (_, _, later) = Box::pin(submit(&lab, "expire-later", first_deadline + 100)).await;
    let reader = hold_reader(&lab);
    let receipts = sql_count(&reader, "receipts");
    let committed_revision = revision(&reader);
    park_expiry(&lab, first_deadline).await;

    let mut replay = original;
    replay.request_id = "replayed-original-transport".into();
    let replay = receive(lab.enqueue(replay, TrustedHostPath::OperatorApi)).await;
    assert_eq!(replay.request_id, "replayed-original-transport");
    assert_eq!(replay.commit, accepted.commit);
    assert_eq!(replay.outcome, accepted.outcome);
    assert_eq!(revision_read(&lab).await.unwrap(), u64::try_from(committed_revision).unwrap());
    let fresh = receive(lab.enqueue(request("fresh-effect", register("fresh-effect-original")),
        TrustedHostPath::OperatorApi)).await;
    refused(&fresh, "store_unavailable");
    assert_eq!(sql_count(&reader, "receipts"), receipts);
    assert_eq!(sql_count(&reader, "repositories"), 0);
    assert_eq!(stored_task(&reader, &first.id), first);
    assert_eq!(deadline_events(&reader, &first.id), 0);

    reader.execute_batch("ROLLBACK").unwrap();
    // From this unlock through both terminal observations, only read-only SQL
    // and the fixture clock are used. No controller message can drive expiry.
    until("parked expiry must retry after the lock is released", ||
        stored_task(&reader, &first.id).status == TaskStatus::Failed).await;
    assert_eq!(stored_task(&reader, &later.id).status, TaskStatus::Queued);
    lab.clock.store(later.input.limits.deadline_unix_ms, Ordering::SeqCst);
    until("owner must recover and service the next future deadline while idle", ||
        stored_task(&reader, &later.id).status == TaskStatus::Failed).await;
    for task in [&first, &later] {
        let expired = stored_task(&reader, &task.id);
        assert_eq!(expired.blocked_reason.as_deref(), Some("execution_deadline"));
        assert_eq!(expired.input, task.input);
        assert_eq!(expired.revision, task.revision + 1);
        assert!(expired.attempt.is_none());
        assert_eq!(deadline_events(&reader, &task.id), 1);
    }
    assert_eq!(revision(&reader), committed_revision + 2);
    assert_eq!(sql_count(&reader, "receipts"), receipts);
    assert_eq!(sql_count(&reader, "attempts"), 0);
    lab.close();
}

#[tokio::test]
async fn deadline_read_contention_retries_before_idle_expiry_without_new_operation() {
    let mut lab = Lab::open(4, 20, 2_000);
    let deadline = lab.clock.load(Ordering::SeqCst) + 60_000;
    let (_, _, task) = Box::pin(submit(&lab, "deadline-read", deadline)).await;
    let blocker = connection(&lab);
    blocker.busy_timeout(Duration::from_secs(1)).unwrap();
    blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();
    let receipts = sql_count(&blocker, "receipts");
    let committed_revision = revision(&blocker);
    let rollbacks = lab.seen.rollbacks.load(Ordering::SeqCst);
    lab.clock.store(deadline, Ordering::SeqCst);
    assert_eq!(revision_read(&lab).await.unwrap_err(), "durable revision unavailable");
    assert_eq!(lab.seen.rollbacks.load(Ordering::SeqCst), rollbacks,
        "deadline SELECT failed before any transaction; do not label it a rollback");
    assert!(!lab.owner.as_ref().unwrap().is_finished());
    blocker.execute_batch("ROLLBACK").unwrap();
    until("deadline read retry must lead to expiry without an incoming command", ||
        stored_task(&blocker, &task.id).status == TaskStatus::Failed).await;
    assert_eq!(deadline_events(&blocker, &task.id), 1);
    assert_eq!(sql_count(&blocker, "receipts"), receipts);
    assert_eq!(revision(&blocker), committed_revision + 1);
    lab.close();
}

#[tokio::test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A transient failure of a platform-owned durable-store transaction MUST be retried with metadata-controlled backoff while retaining the original operation identity.
async fn overdue_maintenance_keeps_backoff_under_wakeups_and_stops_on_shutdown() {
    let mut lab = Lab::open(4, 60_000, 120_000);
    let deadline = lab.clock.load(Ordering::SeqCst) + 60_000;
    let (_, _, task) = Box::pin(submit(&lab, "shutdown-expiry", deadline)).await;
    let reader = hold_reader(&lab);
    let receipts = sql_count(&reader, "receipts");
    let committed_revision = revision(&reader);
    let parked_rollbacks = park_expiry(&lab, deadline).await;
    for _ in 0..16 {
        assert_eq!(revision_read(&lab).await.unwrap(), u64::try_from(committed_revision).unwrap());
    }
    assert_eq!(lab.seen.rollbacks.load(Ordering::SeqCst), parked_rollbacks,
        "an overdue task's zero delay must not bypass the selected maintenance backoff");
    lab.close();
    assert_eq!(lab.seen.rollbacks.load(Ordering::SeqCst), parked_rollbacks);
    reader.execute_batch("ROLLBACK").unwrap();
    assert_eq!(stored_task(&reader, &task.id), task);
    assert_eq!(deadline_events(&reader, &task.id), 0);
    assert_eq!(sql_count(&reader, "receipts"), receipts);
    assert_eq!(revision(&reader), committed_revision);
}

#[tokio::test]
async fn maintenance_exhaustion_closes_owner_without_refreshing_its_phase_lifetime() {
    let mut lab = Lab::open(4, 40, 400);
    let deadline = lab.clock.load(Ordering::SeqCst) + 60_000;
    let (_, _, task) = Box::pin(submit(&lab, "exhaust-expiry", deadline)).await;
    let reader = hold_reader(&lab);
    let receipts = sql_count(&reader, "receipts");
    let committed_revision = revision(&reader);
    let before = lab.seen.rollbacks.load(Ordering::SeqCst);
    park_expiry(&lab, deadline).await;
    until("one fixed maintenance service lifetime must exhaust under sustained contention", ||
        lab.owner.as_ref().unwrap().is_finished()).await;
    let after = lab.seen.rollbacks.load(Ordering::SeqCst);
    assert!(after >= before + 2, "real failed COMMIT attempts must share the original lifetime");
    lab.close();
    reader.execute_batch("ROLLBACK").unwrap();
    assert_eq!(stored_task(&reader, &task.id), task);
    assert_eq!(deadline_events(&reader, &task.id), 0);
    assert_eq!(sql_count(&reader, "receipts"), receipts);
    assert_eq!(revision(&reader), committed_revision);
    assert_eq!(lab.seen.rollbacks.load(Ordering::SeqCst), after);
}

#[tokio::test]
async fn pending_expiry_allows_stop_delivery_and_ack_but_never_admits_fresh_run() {
    let mut lab = Lab::open(8, 60_000, 120_000);
    let now = lab.clock.load(Ordering::SeqCst);
    let (_, _, active) = Box::pin(submit(&lab, "control-active", now + 120_000)).await;
    let worker = Worker { id: "maintenance-worker".into(), incarnation: "one".into(),
        coding_capacity: 0, native_capacity: 2 };
    let registered = receive(lab.enqueue(request("register-worker", Operation::RegisterWorker {
        operation_id: "register-worker-original".into(), worker: worker.clone(),
    }), TrustedHostPath::OperatorApi)).await;
    assert!(matches!(registered.outcome, Outcome::Ok { data: Data::Worker(_) }));
    let started = commands(receive(lab.enqueue(await_commands("start-original", &worker, vec![]),
        TrustedHostPath::OperatorApi)).await);
    let attempt = match started.as_slice() {
        [WorkerCommand::RunAttempt { attempt, .. }] => attempt.clone(),
        _ => panic!("one actual admission expected: {started:?}"),
    };
    assert_eq!(attempt.task_id, active.id);
    // Admit only the first task before creating the remaining tasks. One spare
    // native slot makes the later no-Run result discriminate the maintenance
    // gate from ordinary worker-capacity exhaustion.
    let (_, _, overdue) = Box::pin(submit(&lab, "control-overdue", now + 60_000)).await;
    let (_, _, queued) = Box::pin(submit(&lab, "control-queued", now + 120_000)).await;
    let reader = hold_reader(&lab);
    let current = stored_task(&reader, &active.id);
    let overdue = stored_task(&reader, &overdue.id);
    let queued = stored_task(&reader, &queued.id);
    let parked_rollbacks = park_expiry(&lab, overdue.input.limits.deadline_unix_ms).await;
    reader.execute_batch("ROLLBACK").unwrap();
    // The real SQL lock is now gone, but the same maintenance episode remains
    // parked until its selected backoff. Controls must work in this interval.
    let cancelled = receive(lab.enqueue(request("cancel-active", Operation::CancelTask {
        operation_id: "cancel-active-original".into(), task_id: active.id.clone(),
        expected_revision: current.revision,
    }), TrustedHostPath::OperatorApi)).await;
    let stopped = match cancelled.outcome {
        Outcome::Ok { data: Data::Task(task) } => task,
        other => panic!("protective cancellation must remain available: {other:?}"),
    };
    assert_eq!(stopped.status, TaskStatus::Cancelled);
    assert_eq!(stopped.attempt.as_ref().unwrap().phase, AttemptPhase::StopRequested);
    assert!(!stopped.attempt.as_ref().unwrap().stopped,
        "command delivery does not invent physical completion");
    let delivered = commands(receive(lab.enqueue(await_commands("stop-delivery", &worker, vec![]),
        TrustedHostPath::OperatorApi)).await);
    let stop_id = match delivered.as_slice() {
        [WorkerCommand::StopAttempt { command_id, attempt_id, .. }] if attempt_id == &attempt.id => command_id.clone(),
        _ => panic!("only the original Stop may be delivered: {delivered:?}"),
    };
    let acknowledged = commands(receive(lab.enqueue(await_commands("stop-ack", &worker, vec![stop_id.clone()]),
        TrustedHostPath::OperatorApi)).await);
    assert!(acknowledged.is_empty());
    let ack: bool = reader.query_row("SELECT acknowledged FROM commands WHERE id=?1", [&stop_id], |r| r.get(0)).unwrap();
    assert!(ack);
    let pending = receive(lab.enqueue(await_commands("no-fresh-run", &worker, vec![]),
        TrustedHostPath::OperatorApi)).await;
    assert!(matches!(&pending.outcome, Outcome::Pending { operation_id } if operation_id == "no-fresh-run"));
    assert!(pending.commit.is_none());
    for task in [&overdue, &queued] {
        assert_eq!(stored_task(&reader, &task.id), *task);
        assert_eq!(deadline_events(&reader, &task.id), 0);
    }
    assert_eq!(sql_count(&reader, "attempts"), 1);
    // Pending command lookup explicitly rolls back; none of those rollbacks
    // may be mistaken for a successful maintenance retry or terminal event.
    assert!(lab.seen.rollbacks.load(Ordering::SeqCst) >= parked_rollbacks);
    lab.close();
}
