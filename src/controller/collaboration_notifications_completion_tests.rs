//! Actual controller-produced records, followed by query-only helper reads.
//! Include as a child of collaboration_notifications; author-only, never run.
use super::*;
use crate::{
    collaboration::{documents::*, questions::*, work::*},
    controller::Controller,
    limits::RuntimeLimits,
    mcp::Caller,
};
use serde_json::json;
use std::{path::{Path, PathBuf}, sync::{Arc, atomic::{AtomicU64, Ordering}}, time::{Duration, SystemTime, UNIX_EPOCH}};

static NEXT: AtomicU64 = AtomicU64::new(1);
fn now() -> u64 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64 }
fn unique() -> String { format!("completion-input-{}-{}-{}", std::process::id(), now(), NEXT.fetch_add(1, Ordering::Relaxed)) }
fn selected() -> RuntimeLimits {
    let mut p = RuntimeLimits::seed().unwrap();
    p.controller.command_wait_ms = 5;
    p.execution.native.context_bytes = 65_536;
    p.workflow = Some(WorkflowLimits { record_bytes: 32_768, text_bytes: 4_096, options: 8, records: 512,
        revisions: 4_096, page_items: 8, page_bytes: 32_768, waits_per_task: 8, graph_scan_nodes: 512,
        events_per_mutation: 64, event_bytes: 8_192, notice_bytes: 8_192,
        pending_delivery_bytes: 32_768, delivery_scan_rows: 64 });
    p.validate().unwrap(); p
}
fn input(agent: &str, v2: bool) -> TaskInput {
    let p = RuntimeLimits::seed().unwrap();
    TaskInput { instruction_source: None, pull_request: None,  instruction: "Return a bounded integer".into(),
        definition: Definition { id: agent.into(), revision: if v2 { "completion-v2" } else { "completion-v1" }.into(),
            executor: ExecutorKind::Native, inference: Some(InferenceSettings::selected()), limit_overrides: None, charter: None,
            execution_profile: Some(if v2 { ExecutionProfile::CollaborationV2 { publication: None, web_search: None } }
                else { ExecutionProfile::CollaborationV1 { web_search: None } }) },
        repository: None, repository_read: None, acceptance_commands: vec![], output_schema: Some(json!({"type":"integer"})),
        limits: Limits { deadline_unix_ms: now() + 600_000, model_requests: 32, output_bytes: 16_384,
            result_bytes: p.task_defaults.result_bytes, max_delegation_depth: 4 },
        dependencies: vec![], native_workflow: None, resolved_limits: None }
}
struct Lab { path: PathBuf, config: ControllerConfig, controller: Option<Controller> }
impl Lab {
    fn new(p: RuntimeLimits) -> Self {
        let root = std::env::temp_dir().join(unique()); std::fs::create_dir(&root).unwrap();
        let path = root.join("tasks.sqlite");
        // Existing library-test runtime binding; never substitute the test
        // harness executable for the real controller validation helper.
        let config = ControllerConfig { limits: Arc::new(p),
            validation_executable: PathBuf::from(std::env::var("BORG_TEST_RUNTIME").expect("explicit library-test runtime binding")),
            ..ControllerConfig::default() };
        let controller = Some(Controller::open_with_config(&path, config.clone()).unwrap());
        Self { path, config, controller }
    }
    fn c(&self) -> &Controller { self.controller.as_ref().unwrap() }
    async fn inspect<T>(&mut self, check: impl FnOnce(&Connection<'_>) -> T) -> T {
        drop(self.controller.take()); released(&self.path).await;
        let sql = rusqlite::Connection::open_with_flags(&self.path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        sql.execute_batch("PRAGMA query_only=ON").unwrap();
        let tx = Connection::new(&sql, &self.config.limits);
        let result = check(&tx);
        drop(tx); drop(sql);
        self.controller = Some(Controller::open_with_config(&self.path, self.config.clone()).unwrap());
        result
    }
}
async fn released(path: &Path) {
    let mut lock = path.as_os_str().to_owned(); lock.push(".controller-lock");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let file = std::fs::OpenOptions::new().read(true).write(true).open(PathBuf::from(&lock)).unwrap();
            if file.try_lock().is_ok() { break; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.expect("previous controller released its SQLite owner lock");
}
async fn call(c: &Controller, command: Operation) -> Reply {
    let reply = c.handle(Request { protocol_revision: WIRE_REVISION, request_id: unique(), command: command.clone() }).await;
    reply.validate_for(&command).unwrap(); reply
}
fn task(reply: Reply) -> Task {
    match reply.outcome { Outcome::Ok { data: Data::Task(t) } => *t, other => panic!("task: {other:?}") }
}
fn delivery(reply: Reply) -> DeliveryObservation {
    match reply.outcome { Outcome::Ok { data: Data::CollaborationDelivery(v) } => v, other => panic!("delivery: {other:?}") }
}
async fn report(c: &Controller, caller: &Caller, observation: Observation) -> Task {
    task(call(c, Operation::ObserveWorker { report: Report { report_id: unique(), task_id: caller.task_id.clone(),
        attempt_id: caller.attempt_id.clone(), generation: caller.generation, worker_id: caller.worker_id.clone(),
        incarnation: caller.incarnation.clone(), observation } }).await)
}
async fn admit(c: &Controller, queued: &Task) -> (Task, Caller) {
    let worker = Worker { id: unique(), incarnation: "inc".into(), coding_capacity: 0, native_capacity: 1 };
    assert!(matches!(call(c, Operation::RegisterWorker { operation_id: unique(), worker: worker.clone() }).await.outcome, Outcome::Ok { .. }));
    let reply = call(c, Operation::AwaitCommands { operation_id: unique(), worker_id: worker.id, incarnation: worker.incarnation, acknowledged: vec![] }).await;
    let Outcome::Ok { data: Data::Commands(commands) } = reply.outcome else { panic!("commands: {reply:?}") };
    let a = commands.into_iter().find_map(|command| match command {
        WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == queued.id => Some(attempt), _ => None,
    }).expect("ordinary admission of the selected task");
    let caller = Caller { task_id: a.task_id, attempt_id: a.id, generation: a.generation, worker_id: a.worker_id, incarnation: a.worker_incarnation };
    (report(c, &caller, Observation::Started { thread_id: None, turn_id: None }).await, caller)
}
async fn actor(c: &Controller, agent: &str, v2: bool) -> (Task, Caller) {
    let queued = task(call(c, Operation::SubmitTask { operation_id: unique(), input: input(agent, v2) }).await);
    admit(c, &queued).await
}
async fn document(c: &Controller, owner: &str) -> DocumentView {
    let reply = call(c, Operation::MutateDocument { operation_id: unique(), mutation: DocumentMutation::Create {
        content: DocumentContent { title: "Completion input".into(), body: "Exact routing target".into() }, supplied_content_id: None,
        responsible_owner: PrincipalRef::Agent { agent_id: owner.into() } } }).await;
    match reply.outcome { Outcome::Ok { data: Data::DocumentMutation(DocumentMutationOutcome::Document { view }) } => view, other => panic!("document: {other:?}") }
}
async fn owner(c: &Controller, doc: &DocumentView, agent: &str) {
    let reply = call(c, Operation::MutateDocument { operation_id: unique(), mutation: DocumentMutation::SetResponsibleOwner {
        document_id: doc.document.id.clone(), expected_revision: doc.document.revision,
        responsible_owner: PrincipalRef::Agent { agent_id: agent.into() } } }).await;
    assert!(matches!(reply.outcome, Outcome::Ok { data: Data::DocumentMutation(_) }), "{reply:?}");
}
async fn pose(c: &Controller, record: RecordRef, recipient: PrincipalRef) -> Question {
    let reply = call(c, Operation::MutateQuestion { operation_id: unique(), mutation: QuestionMutation::Pose { question: PoseQuestion {
        record, recipient, prompt: "Choose the existing option".into(),
        options: vec![QuestionOption { id: "keep".into(), label: "Keep".into(), explanation: "Keep".into() },
            QuestionOption { id: "change".into(), label: "Change".into(), explanation: "Change".into() }], recommended_option_id: "keep".into(),
    } } }).await;
    match reply.outcome { Outcome::Ok { data: Data::QuestionMutation(v) } => v.question.question, other => panic!("question: {other:?}") }
}
async fn events(c: &Controller) -> Vec<WorkflowActivity> {
    let mut out = vec![]; let mut cursor = None;
    for _ in 0..32 {
        let reply = call(c, Operation::QueryWorkflowActivity { cursor: cursor.take(), limit: 8 }).await;
        let Outcome::Ok { data: Data::WorkflowActivity(page) } = reply.outcome else { panic!("activity: {reply:?}") };
        out.extend(page.items); cursor = page.next_cursor;
        if cursor.is_none() { return out; }
    }
    panic!("fixture exceeded its finite event inventory")
}
async fn answer(c: &Controller, question: &Question) -> WorkflowActivity {
    let reply = call(c, Operation::MutateQuestion { operation_id: unique(), mutation: QuestionMutation::Answer {
        question_id: question.id.clone(), expected_revision: question.revision,
        answer: QuestionAnswer { option_id: "keep".into(), explanation: "Original resolution".into() } } }).await;
    assert!(matches!(reply.outcome, Outcome::Ok { data: Data::QuestionMutation(_) }), "{reply:?}");
    let mut closed = events(c).await.into_iter().filter(|e| e.subject == RecordRef::Question { question_id: question.id.clone() }
        && matches!(&e.detail, ActivityDetail::Question(QuestionChange::Closed { .. })));
    let event = closed.next().expect("actual first terminal event"); assert!(closed.next().is_none()); event
}
async fn observe(c: &Controller, caller: &Caller) -> DeliveryObservation {
    delivery(call(c, Operation::ObserveCollaborationDelivery { caller: caller.clone(), limit: 8 }).await)
}
async fn prepare(c: &Controller, caller: &Caller, view: &DeliveryObservation) -> DeliveryBatch {
    delivery(call(c, Operation::PrepareCollaborationDelivery { operation_id: unique(), caller: caller.clone(),
        expected_cursor: view.acknowledged_cursor.clone(), expected_routing_cursor: view.routing_cursor.clone(),
        through: view.available.as_ref().unwrap().through.clone(), limit: 8 }).await).pending.unwrap()
}
fn begin(caller: &Caller, batch: &DeliveryBatch, context: &str) -> Operation {
    Operation::BeginCollaborationDelivery { operation_id: unique(), caller: caller.clone(), batch_id: batch.batch_id.clone(), context_binding: context.into() }
}
fn ack(caller: &Caller, batch: &DeliveryBatch, context: &str) -> Operation {
    Operation::AcknowledgeCollaborationDelivery { operation_id: unique(), caller: caller.clone(), batch_id: batch.batch_id.clone(), context_binding: context.into() }
}
async fn settle(c: &Controller, caller: &Caller, batch: &DeliveryBatch) {
    let context = unique();
    if batch.page.has_input() {
        let begun = delivery(call(c, begin(caller, batch, &context)).await);
        assert!(begun.pending.as_ref().unwrap().delivery.is_some());
    }
    assert!(delivery(call(c, ack(caller, batch, &context)).await).pending.is_none());
}
async fn seek_route(c: &Controller, caller: &Caller, event: &EventId) -> DeliveryBatch {
    for _ in 0..32 {
        let view = observe(c, caller).await; assert!(view.pending.is_none());
        let routed = view.available.as_ref().expect("owed event remains available").routing.as_ref()
            .is_some_and(|r| r.items.iter().any(|e| &e.id == event));
        let batch = prepare(c, caller, &view).await;
        if routed { return batch; }
        settle(c, caller, &batch).await;
    }
    panic!("finite fixture did not reach original routed input")
}
fn read_inputs(tx: &Connection<'_>, task_id: &str, scan_rows: usize) -> Result<CompletionInputs> {
    let current = load(tx, task_id)?.task;
    let accepted = &current.input.resolved_limits.as_ref().expect("real admission bound metadata").global_metadata;
    let view = tx.with_limits(accepted);
    completion_inputs(&view, &current, now(), scan_rows)
}
fn exact_input<'a>(read: &'a CompletionInputs, event: &WorkflowActivity) -> &'a CompletionInput {
    let matched: Vec<_> = read.items.iter().filter(|item| item.event.id == event.id).collect();
    assert_eq!(matched.len(), 1, "one original EventId, including when several waits reference it");
    assert_eq!(matched[0].event, *event); matched[0]
}

#[tokio::test]
async fn completion_scan_finds_terminal_behind_both_cursors_and_refuses_partial_inventory() {
    let mut policy = selected(); policy.workflow.as_mut().unwrap().delivery_scan_rows = 1;
    let mut lab = Lab::new(policy);
    actor(lab.c(), "old-v1", false).await;
    let (reader, caller) = actor(lab.c(), "next-v2", true).await;
    let doc = document(lab.c(), "old-v1").await;
    let q = pose(lab.c(), RecordRef::Document { document_id: doc.document.id.clone() }, PrincipalRef::TrustedHostOperator).await;
    let original = answer(lab.c(), &q).await;
    // A later event keeps routing.to_cursor non-None after the original event.
    pose(lab.c(), RecordRef::Document { document_id: doc.document.id.clone() }, PrincipalRef::TrustedHostOperator).await;
    let mut passed = false;
    for _ in 0..32 {
        let view = observe(lab.c(), &caller).await;
        if view.acknowledged_cursor.position.as_ref() >= Some(&original.id) && view.routing_cursor.as_ref() >= Some(&original.id) {
            passed = true; break;
        }
        let batch = prepare(lab.c(), &caller, &view).await;
        assert!(!batch.page.has_input(), "old V1 owner leaves the resolution owed");
        settle(lab.c(), &caller, &batch).await;
    }
    assert!(passed, "both independent scans passed the original EventId");
    owner(lab.c(), &doc, "next-v2").await;
    let count = events(lab.c()).await.len(); assert!(count > 1);
    lab.inspect(|tx| {
        assert!(matches!(read_inputs(tx, &reader.id, 0), Err(Fault::Refused("invalid_input", _))));
        assert!(matches!(read_inputs(tx, &reader.id, count - 1), Err(Fault::Refused("capacity", _))), "a matching early row cannot conceal an incomplete scan");
        let read = read_inputs(tx, &reader.id, count).unwrap();
        assert!(!read.pending_begin); assert_eq!(read.items.len(), 1);
        assert!(!exact_input(&read, &original).consumed);
    }).await;
}

#[tokio::test]
async fn completion_own_begin_survives_owner_change_and_only_exact_ack_consumes() {
    let mut lab = Lab::new(selected());
    let (reader, caller) = actor(lab.c(), "reader", true).await;
    actor(lab.c(), "later-v1-owner", false).await;
    let doc = document(lab.c(), "reader").await;
    let q = pose(lab.c(), RecordRef::Document { document_id: doc.document.id.clone() }, PrincipalRef::TrustedHostOperator).await;
    let original = answer(lab.c(), &q).await;
    let batch = seek_route(lab.c(), &caller, &original.id).await;
    let begun = delivery(call(lab.c(), begin(&caller, &batch, "original-context")).await);
    assert!(begun.pending.as_ref().unwrap().delivery.is_some());
    owner(lab.c(), &doc, "later-v1-owner").await;
    lab.inspect(|tx| {
        let read = read_inputs(tx, &reader.id, 64).unwrap();
        assert!(read.pending_begin); assert!(!exact_input(&read, &original).consumed);
    }).await;
    let operation = ack(&caller, &batch, "original-context");
    let committed = call(lab.c(), operation.clone()).await;
    assert!(delivery(committed.clone()).pending.is_none());
    lab.inspect(|tx| {
        let read = read_inputs(tx, &reader.id, 64).unwrap();
        assert!(!read.pending_begin); assert!(exact_input(&read, &original).consumed);
    }).await;
    let replay = call(lab.c(), operation).await;
    assert_eq!(replay.outcome, committed.outcome); assert_eq!(replay.commit, committed.commit);
}

#[tokio::test]
async fn completion_advisory_only_begin_remains_a_fence_without_required_terminal_input() {
    let mut lab = Lab::new(selected());
    let (reader, caller) = actor(lab.c(), "advisory", true).await;
    pose(lab.c(), RecordRef::Task { task_id: reader.id.clone() }, PrincipalRef::Agent { agent_id: reader.assigned_agent_id.clone() }).await;
    let view = observe(lab.c(), &caller).await;
    let batch = prepare(lab.c(), &caller, &view).await;
    assert_eq!(batch.page.items.len(), 1);
    assert!(matches!(&batch.page.items[0].detail, ActivityDetail::Question(QuestionChange::Posed { .. })));
    assert!(delivery(call(lab.c(), begin(&caller, &batch, "advisory-context")).await).pending.unwrap().delivery.is_some());
    lab.inspect(|tx| {
        let read = read_inputs(tx, &reader.id, 64).unwrap();
        assert!(read.items.is_empty()); assert!(read.pending_begin);
    }).await;
    assert!(delivery(call(lab.c(), ack(&caller, &batch, "advisory-context")).await).pending.is_none());
    lab.inspect(|tx| {
        let read = read_inputs(tx, &reader.id, 64).unwrap();
        assert!(read.items.is_empty()); assert!(!read.pending_begin);
    }).await;
}

#[tokio::test]
async fn completion_foreign_route_ack_cannot_discharge_concrete_waiter_input() {
    let mut lab = Lab::new(selected());
    let (routing_task, routing_caller) = actor(lab.c(), "route-owner", true).await;
    let waiting = task(call(lab.c(), Operation::SubmitTask { operation_id: unique(), input: input("waiter", true) }).await);
    let doc = document(lab.c(), "route-owner").await;
    let q = pose(lab.c(), RecordRef::Document { document_id: doc.document.id.clone() }, PrincipalRef::TrustedHostOperator).await;
    let added = call(lab.c(), Operation::MutateWork { operation_id: unique(), mutation: WorkMutation::AddWait {
        task_id: waiting.id.clone(), expected_revision: 0, resolver: ResolverRef::Question { question_id: q.id.clone() } } }).await;
    assert!(matches!(added.outcome, Outcome::Ok { data: Data::WorkMutation(_) }), "{added:?}");
    let original = answer(lab.c(), &q).await;
    let route = seek_route(lab.c(), &routing_caller, &original.id).await;
    settle(lab.c(), &routing_caller, &route).await;
    let (waiter, caller) = admit(lab.c(), &waiting).await;
    let view = observe(lab.c(), &caller).await;
    assert_eq!(view.available.as_ref().unwrap().continuation.as_ref().unwrap().resolved_waits[0].event_id, original.id);
    lab.inspect(|tx| {
        let route = read_inputs(tx, &routing_task.id, 64).unwrap();
        assert!(exact_input(&route, &original).consumed);
        let concrete = read_inputs(tx, &waiter.id, 64).unwrap();
        assert!(!concrete.pending_begin); assert_eq!(concrete.items.len(), 1, "resolved-wait audit events are not extra required inputs");
        assert!(!exact_input(&concrete, &original).consumed, "another session's route ACK is not this waiter's input ACK");
    }).await;
    let batch = prepare(lab.c(), &caller, &observe(lab.c(), &caller).await).await;
    settle(lab.c(), &caller, &batch).await;
    lab.inspect(|tx| {
        let concrete = read_inputs(tx, &waiter.id, 64).unwrap();
        assert!(!concrete.pending_begin); assert!(exact_input(&concrete, &original).consumed);
    }).await;
}

#[tokio::test]
async fn completion_stopped_attempt_refuses_instead_of_proving_empty_input() {
    let mut lab = Lab::new(selected());
    let (reader, caller) = actor(lab.c(), "stopped", true).await;
    lab.inspect(|tx| { let read = read_inputs(tx, &reader.id, 64).unwrap(); assert!(read.items.is_empty()); assert!(!read.pending_begin); }).await;
    let stopped = report(lab.c(), &caller, Observation::ProcessTreeStopped).await;
    assert_eq!(stopped.status, TaskStatus::Active); assert!(stopped.attempt.as_ref().unwrap().stopped);
    lab.inspect(|tx| assert!(matches!(read_inputs(tx, &reader.id, 64), Err(Fault::Refused("reconciliation_required", _))), "stopped routing ineligibility is not a complete empty proof")).await;
}

fn read_stopped_inputs(tx: &Connection<'_>, task_id: &str, scan_rows: usize) -> Result<CompletionInputs> {
    let current = load(tx, task_id)?.task;
    let accepted = &current.input.resolved_limits.as_ref().expect("real admission bound metadata").global_metadata;
    let view = tx.with_limits(accepted);
    completion_stopped_inputs(&view, &current, now(), scan_rows)
}

#[tokio::test]
async fn completion_stopped_projection_requires_exact_current_stopped_nonterminal_task() {
    let mut lab = Lab::new(selected());
    let (reader, caller) = actor(lab.c(), "stopped-proof", true).await;
    lab.inspect(|tx| {
        assert!(matches!(read_stopped_inputs(tx, &reader.id, 64), Err(Fault::Refused("reconciliation_required", _))));
    }).await;
    let stopped = report(lab.c(), &caller, Observation::ProcessTreeStopped).await;
    assert_eq!(stopped.attempt.as_ref().unwrap().phase, AttemptPhase::Stopped);
    assert!(!stopped.attempt.as_ref().unwrap().uncertain);
    lab.inspect(|tx| {
        let current = load(tx, &reader.id).unwrap().task;
        assert_eq!(current, stopped);
        let read = completion_stopped_inputs(tx, &current, now(), 64).unwrap();
        assert!(read.items.is_empty()); assert!(!read.pending_begin);
        assert!(matches!(completion_stopped_inputs(tx, &current, now(), 0), Err(Fault::Refused("invalid_input", _))));
        let mut stale = current.clone(); stale.revision += 1;
        assert!(matches!(completion_stopped_inputs(tx, &stale, now(), 64), Err(Fault::Refused("stale_revision", _))));
        let mut replaced = current.clone(); replaced.attempt.as_mut().unwrap().worker_incarnation = "different-incarnation".into();
        assert!(matches!(completion_stopped_inputs(tx, &replaced, now(), 64), Err(Fault::Refused("stale_revision", _))));
        let mut uncertain = current.clone(); uncertain.attempt.as_mut().unwrap().uncertain = true;
        assert!(matches!(completion_stopped_inputs(tx, &uncertain, now(), 64), Err(Fault::Refused("reconciliation_required", _))));
    }).await;
    let cancelled = task(call(lab.c(), Operation::CancelTask { operation_id: unique(), task_id: stopped.id, expected_revision: stopped.revision }).await);
    assert_eq!(cancelled.status, TaskStatus::Cancelled);
    lab.inspect(|tx| {
        assert!(matches!(read_stopped_inputs(tx, &cancelled.id, 64), Err(Fault::Refused("reconciliation_required", _))));
    }).await;
}

#[tokio::test]
async fn completion_stopped_projection_preserves_owned_unknown_route_begin_after_owner_change() {
    let mut lab = Lab::new(selected());
    let (reader, caller) = actor(lab.c(), "stopped-claim", true).await;
    actor(lab.c(), "other-v1-owner", false).await;
    let doc = document(lab.c(), "stopped-claim").await;
    let q = pose(lab.c(), RecordRef::Document { document_id: doc.document.id.clone() }, PrincipalRef::TrustedHostOperator).await;
    let original = answer(lab.c(), &q).await;
    let batch = seek_route(lab.c(), &caller, &original.id).await;
    assert!(delivery(call(lab.c(), begin(&caller, &batch, "stopped-original-context")).await).pending.unwrap().delivery.is_some());
    let stopped = report(lab.c(), &caller, Observation::ProcessTreeStopped).await;
    assert!(stopped.attempt.as_ref().unwrap().stopped);
    assert!(!stopped.attempt.as_ref().unwrap().uncertain);
    owner(lab.c(), &doc, "other-v1-owner").await;
    let count = events(lab.c()).await.len(); assert!(count > 1);
    lab.inspect(|tx| {
        assert!(matches!(read_stopped_inputs(tx, &reader.id, count - 1), Err(Fault::Refused("capacity", _))));
        let read = read_stopped_inputs(tx, &reader.id, count).unwrap();
        assert!(read.pending_begin);
        assert!(!exact_input(&read, &original).consumed);
    }).await;
}

#[tokio::test]
async fn completion_stopped_projection_keeps_advisory_begin_as_an_input_fence() {
    let mut lab = Lab::new(selected());
    let (reader, caller) = actor(lab.c(), "stopped-advisory", true).await;
    pose(lab.c(), RecordRef::Task { task_id: reader.id.clone() }, PrincipalRef::Agent { agent_id: reader.assigned_agent_id.clone() }).await;
    let batch = prepare(lab.c(), &caller, &observe(lab.c(), &caller).await).await;
    assert!(batch.page.has_input());
    assert!(delivery(call(lab.c(), begin(&caller, &batch, "stopped-advisory-context")).await).pending.unwrap().delivery.is_some());
    let stopped = report(lab.c(), &caller, Observation::ProcessTreeStopped).await;
    assert!(!stopped.attempt.as_ref().unwrap().uncertain);
    lab.inspect(|tx| {
        let read = read_stopped_inputs(tx, &reader.id, 64).unwrap();
        assert!(read.items.is_empty()); assert!(read.pending_begin);
    }).await;
}

#[tokio::test]
async fn completion_stopped_projection_keeps_actual_ack_but_excludes_new_unclaimed_routes() {
    let mut lab = Lab::new(selected());
    let (reader, caller) = actor(lab.c(), "shared-agent", true).await;
    let doc = document(lab.c(), "shared-agent").await;
    let first = pose(lab.c(), RecordRef::Document { document_id: doc.document.id.clone() }, PrincipalRef::TrustedHostOperator).await;
    let consumed = answer(lab.c(), &first).await;
    let batch = seek_route(lab.c(), &caller, &consumed.id).await;
    settle(lab.c(), &caller, &batch).await;
    let stopped = report(lab.c(), &caller, Observation::ProcessTreeStopped).await;
    assert!(!stopped.attempt.as_ref().unwrap().uncertain);
    // The same agent's new active session can receive this later route. It was
    // never concretely addressed to or claimed by the original stopped session.
    let (successor, _) = actor(lab.c(), "shared-agent", true).await;
    let later = pose(lab.c(), RecordRef::Document { document_id: doc.document.id.clone() }, PrincipalRef::TrustedHostOperator).await;
    let unclaimed = answer(lab.c(), &later).await;
    assert!(unclaimed.id > consumed.id);
    lab.inspect(|tx| {
        let stopped = read_stopped_inputs(tx, &reader.id, 64).unwrap();
        assert!(!stopped.pending_begin);
        assert_eq!(stopped.items.len(), 1);
        assert!(exact_input(&stopped, &consumed).consumed);
        assert!(stopped.items.iter().all(|item| item.event.id != unclaimed.id));
        let active = read_inputs(tx, &successor.id, 64).unwrap();
        assert!(!exact_input(&active, &unclaimed).consumed, "later route remains owed to a real eligible scope");
    }).await;
}
