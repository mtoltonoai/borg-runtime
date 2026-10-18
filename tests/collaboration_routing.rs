//! Actual controller transactions for one retained resolution routing obligation.
//! Reuses the DELIVERY02 Controller/controlled-worker fixture pattern; this file
//! does not launch a worker, provider, host, browser, or a second test runner.
//! Begin/ACK here exercise controller ownership, not proof of consumer append.
use borg_runtime::{
    collaboration::{documents::*, notifications::*, questions::*, workflow::*, Query, QueryResult},
    controller::{Controller, ControllerConfig},
    limits::{RuntimeLimits, WorkflowLimits},
    mcp::{Caller, Invocation},
    protocol::*,
};
use serde_json::{json, Value};
use std::{path::{Path, PathBuf}, sync::{Arc, atomic::{AtomicU64, Ordering}}, time::{Duration, SystemTime, UNIX_EPOCH}};

static NEXT: AtomicU64 = AtomicU64::new(1);
fn unique() -> String {
    format!("routing-{}-{}-{}", std::process::id(), SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(), NEXT.fetch_add(1, Ordering::Relaxed))
}
fn limits() -> RuntimeLimits {
    let mut p = RuntimeLimits::seed().unwrap();
    p.execution.native.context_bytes = 65_536;
    p.controller.command_wait_ms = 5;
    p.workflow = Some(WorkflowLimits {
        record_bytes: 32_768, text_bytes: 4_096, options: 8, records: 512,
        revisions: 4_096, page_items: 8, page_bytes: 32_768, waits_per_task: 8,
        graph_scan_nodes: 512, events_per_mutation: 64, event_bytes: 8_192,
        notice_bytes: 8_192, pending_delivery_bytes: 32_768, delivery_scan_rows: 64,
    });
    p.validate().unwrap(); p
}
fn input(agent: &str, profile: ExecutionProfile) -> TaskInput {
    let revision = match &profile {
        ExecutionProfile::CollaborationV1 { .. } => "routing-collaboration-v1",
        ExecutionProfile::CollaborationV2 { .. } => "routing-collaboration-v2",
    };
    TaskInput { instruction_source: None,
        pull_request: None,
        instruction: "Return a bounded structured value".into(),
        definition: Definition { charter: None, id: agent.into(), revision: revision.into(),
            executor: ExecutorKind::Native, inference: Some(InferenceSettings::selected()),
            execution_profile: Some(profile), limit_overrides: None },
        repository: None, repository_read: None, acceptance_commands: vec![],
        output_schema: Some(json!({"type":"integer"})),
        limits: Limits { deadline_unix_ms: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64 + 600_000,
            model_requests: 32, output_bytes: 16_384,
            result_bytes: RuntimeLimits::seed().unwrap().task_defaults.result_bytes, max_delegation_depth: 4 },
        dependencies: vec![], native_workflow: None, resolved_limits: None,
    }
}
struct Lab { root: PathBuf, c: Controller, config: ControllerConfig }
impl Lab {
    fn new() -> Self { Self::with(limits()) }
    fn with(p: RuntimeLimits) -> Self {
        let root = std::env::temp_dir().join(unique());
        std::fs::create_dir(&root).unwrap();
        let config = ControllerConfig { limits: Arc::new(p),
            validation_executable: PathBuf::from(env!("CARGO_BIN_EXE_borg-runtime")), ..ControllerConfig::default() };
        let c = Controller::open_with_config(&root.join("tasks.sqlite"), config.clone()).unwrap();
        Self { root, c, config }
    }
    async fn restart(self) -> Self {
        let Self { root, c, config } = self;
        drop(c); released(&root.join("tasks.sqlite")).await;
        let c = Controller::open_with_config(&root.join("tasks.sqlite"), config.clone()).unwrap();
        Self { root, c, config }
    }
}
async fn released(path: &Path) {
    let mut name = path.as_os_str().to_owned(); name.push(".controller-lock");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let file = std::fs::OpenOptions::new().read(true).write(true).open(PathBuf::from(&name)).unwrap();
            if file.try_lock().is_ok() { break; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.expect("previous controller owner released");
}
async fn call(c: &Controller, command: Operation) -> Reply {
    let reply = c.handle(Request { protocol_revision: WIRE_REVISION, request_id: unique(), command: command.clone() }).await;
    reply.validate_for(&command).unwrap(); reply
}
fn task(reply: Reply) -> Task {
    match reply.outcome { Outcome::Ok { data: Data::Task(t) } => *t, other => panic!("task: {other:?}") }
}
fn delivery(reply: &Reply) -> DeliveryObservation {
    match &reply.outcome { Outcome::Ok { data: Data::CollaborationDelivery(v) } => v.clone(), other => panic!("delivery: {other:?}") }
}
fn refused(reply: &Reply, code: &str) {
    assert!(matches!(&reply.outcome, Outcome::Refused { code: actual, .. } if actual == code), "{reply:?}");
}
fn changed(reply: &Reply) -> QuestionMutationOutcome {
    assert!(reply.commit.is_some(), "{reply:?}");
    match &reply.outcome { Outcome::Ok { data: Data::QuestionMutation(v) } => v.clone(), other => panic!("question: {other:?}") }
}
async fn actor(c: &Controller, agent: &str) -> (Task, Caller) {
    actor_profile(c, agent, ExecutionProfile::CollaborationV2 { web_search: None }).await
}
async fn actor_profile(c: &Controller, agent: &str, profile: ExecutionProfile) -> (Task, Caller) {
    let t = task(call(c, Operation::SubmitTask { operation_id: unique(), input: input(agent, profile) }).await);
    let w = Worker { id: unique(), incarnation: "inc".into(), coding_capacity: 0, native_capacity: 1 };
    assert!(matches!(call(c, Operation::RegisterWorker { operation_id: unique(), worker: w.clone() }).await.outcome, Outcome::Ok { .. }));
    let reply = call(c, Operation::AwaitCommands { operation_id: unique(), worker_id: w.id, incarnation: w.incarnation, acknowledged: vec![] }).await;
    let Outcome::Ok { data: Data::Commands(commands) } = reply.outcome else { panic!("commands: {reply:?}") };
    let a = commands.into_iter().find_map(|command| match command {
        WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == t.id => Some(attempt), _ => None,
    }).expect("one ordinary admitted attempt");
    let caller = Caller { task_id: a.task_id, attempt_id: a.id, generation: a.generation, worker_id: a.worker_id, incarnation: a.worker_incarnation };
    report(c, &caller, Observation::Started { thread_id: None, turn_id: None }).await;
    (t, caller)
}
async fn report(c: &Controller, caller: &Caller, observation: Observation) -> Task {
    task(call(c, Operation::ObserveWorker { report: Report { report_id: unique(),
        worker_id: caller.worker_id.clone(), incarnation: caller.incarnation.clone(), task_id: caller.task_id.clone(),
        attempt_id: caller.attempt_id.clone(), generation: caller.generation, observation } }).await)
}
async fn cancel(c: &Controller, caller: &Caller) {
    let t = task(call(c, Operation::GetTask { task_id: caller.task_id.clone() }).await);
    task(call(c, Operation::CancelTask { operation_id: unique(), task_id: t.id, expected_revision: t.revision }).await);
    let stopped = report(c, caller, Observation::ProcessTreeStopped).await;
    assert_eq!(stopped.status, TaskStatus::Cancelled);
    assert!(stopped.attempt.as_ref().unwrap().stopped);
}
async fn inventory(c: &Controller) -> (Vec<Task>, Vec<Session>) {
    let reply = call(c, Operation::ListTasks { after: None, limit: 32 }).await;
    let Outcome::Ok { data: Data::Tasks(tasks) } = reply.outcome else { panic!("tasks: {reply:?}") };
    let reply = call(c, Operation::QueryCollaboration { query: Query::Sessions { agent_id: None, after: None, limit: 32 } }).await;
    let Outcome::Ok { data: Data::Collaboration(QueryResult::Sessions(sessions)) } = reply.outcome else { panic!("sessions: {reply:?}") };
    assert!(sessions.next_cursor.is_none()); (tasks, sessions.items)
}
async fn document(c: &Controller, owner: &str) -> DocumentView {
    let reply = call(c, Operation::MutateDocument { operation_id: unique(), mutation: DocumentMutation::Create {
        content: DocumentContent { title: "Retained routing target".into(), body: "Immutable question linkage".into() },
        supplied_content_id: None, responsible_owner: PrincipalRef::Agent { agent_id: owner.into() },
    } }).await;
    match reply.outcome { Outcome::Ok { data: Data::DocumentMutation(DocumentMutationOutcome::Document { view }) } => view, other => panic!("document: {other:?}") }
}
async fn owner(c: &Controller, doc: &DocumentView, agent: &str) -> DocumentView {
    let reply = call(c, Operation::MutateDocument { operation_id: unique(), mutation: DocumentMutation::SetResponsibleOwner {
        document_id: doc.document.id.clone(), expected_revision: doc.document.revision,
        responsible_owner: PrincipalRef::Agent { agent_id: agent.into() },
    } }).await;
    match reply.outcome { Outcome::Ok { data: Data::DocumentMutation(DocumentMutationOutcome::Document { view }) } => view, other => panic!("owner: {other:?}") }
}
fn posed(doc: &DocumentView) -> PoseQuestion {
    PoseQuestion { record: RecordRef::Document { document_id: doc.document.id.clone() },
        recipient: PrincipalRef::TrustedHostOperator, prompt: "Choose the retained option. Recommended: keep.".into(),
        options: vec![QuestionOption { id: "keep".into(), label: "Keep".into(), explanation: "Keep".into() },
            QuestionOption { id: "change".into(), label: "Change".into(), explanation: "Change".into() }],
        recommended_option_id: "keep".into() }
}
async fn question(c: &Controller, doc: &DocumentView, asker: Option<&Caller>) -> Question {
    let q = posed(doc);
    let operation = match asker {
        None => Operation::MutateQuestion { operation_id: unique(), mutation: QuestionMutation::Pose { question: q } },
        Some(caller) => Invocation { operation_id: unique(), logical_call_id: unique(), caller: caller.clone(),
            tool: "collab_question_pose".into(), arguments: json!({"question":q}) }.operation(),
    };
    changed(&call(c, operation).await).question.question
}
async fn activity(c: &Controller) -> Vec<WorkflowActivity> {
    let mut result = vec![]; let mut cursor = None;
    for _ in 0..32 {
        let reply = call(c, Operation::QueryWorkflowActivity { cursor: cursor.take(), limit: 8 }).await;
        let Outcome::Ok { data: Data::WorkflowActivity(page) } = reply.outcome else { panic!("activity: {reply:?}") };
        result.extend(page.items); cursor = page.next_cursor;
        if cursor.is_none() { return result; }
    }
    panic!("fixture activity exceeded its explicit finite bound")
}
async fn answer(c: &Controller, q: &Question) -> (Operation, Reply, WorkflowActivity) {
    let operation = Operation::MutateQuestion { operation_id: unique(), mutation: QuestionMutation::Answer {
        question_id: q.id.clone(), expected_revision: q.revision,
        answer: QuestionAnswer { option_id: "keep".into(), explanation: "One original resolution".into() },
    } };
    let receipt = call(c, operation.clone()).await;
    changed(&receipt);
    let events: Vec<_> = activity(c).await.into_iter().filter(|e| e.subject == RecordRef::Question { question_id: q.id.clone() }
        && matches!(&e.detail, ActivityDetail::Question(QuestionChange::Closed { .. }))).collect();
    assert_eq!(events.len(), 1);
    let event = events.into_iter().next().unwrap();
    let routing = event.routing.as_ref().expect("actual terminal question retains its immutable routing intent");
    assert_eq!(routing.requested_by, q.poser);
    assert_eq!(routing.linked_record, q.posed.record);
    (operation, receipt, event)
}
async fn observe(c: &Controller, caller: &Caller) -> DeliveryObservation {
    delivery(&call(c, Operation::ObserveCollaborationDelivery { caller: caller.clone(), limit: 8 }).await)
}
fn prepare_request(caller: &Caller, view: &DeliveryObservation, limit: usize) -> Operation {
    Operation::PrepareCollaborationDelivery { operation_id: unique(), caller: caller.clone(),
        expected_cursor: view.acknowledged_cursor.clone(), expected_routing_cursor: view.routing_cursor.clone(),
        through: view.available.as_ref().expect("one bounded offered page").through.clone(), limit }
}
async fn prepare(c: &Controller, caller: &Caller, view: &DeliveryObservation) -> (Operation, Reply, DeliveryBatch) {
    let request = prepare_request(caller, view, 8);
    let reply = call(c, request.clone()).await;
    let batch = delivery(&reply).pending.expect("prepared batch retained");
    assert_eq!(Some(batch.batch_id.as_str()), request.mutation_id());
    assert_eq!(Some(&batch.page), view.available.as_ref());
    (request, reply, batch)
}
fn begin(caller: &Caller, batch: &DeliveryBatch, context: &str) -> Operation {
    Operation::BeginCollaborationDelivery { operation_id: unique(), caller: caller.clone(), batch_id: batch.batch_id.clone(), context_binding: context.into() }
}
fn ack(caller: &Caller, batch: &DeliveryBatch, context: &str) -> Operation {
    Operation::AcknowledgeCollaborationDelivery { operation_id: unique(), caller: caller.clone(), batch_id: batch.batch_id.clone(), context_binding: context.into() }
}
fn routed(page: &DeliveryPage, event: &EventId) -> bool {
    page.routing.as_ref().is_some_and(|route| route.items.iter().any(|item| &item.id == event))
}
async fn settle(c: &Controller, caller: &Caller, batch: &DeliveryBatch) -> DeliveryObservation {
    let context = unique();
    if batch.page.has_input() {
        let begun = delivery(&call(c, begin(caller, batch, &context)).await);
        assert!(begun.superseded.is_none());
        assert!(begun.pending.as_ref().unwrap().delivery.is_some());
    }
    delivery(&call(c, ack(caller, batch, &context)).await)
}
async fn seek(c: &Controller, caller: &Caller, event: &WorkflowActivity) -> (Operation, Reply, DeliveryBatch) {
    for _ in 0..32 {
        let view = observe(c, caller).await;
        assert!(view.pending.is_none());
        let page = view.available.as_ref().expect("retained resolution remains discoverable at a bounded boundary");
        let found = routed(page, &event.id);
        let prepared = prepare(c, caller, &view).await;
        if found {
            assert!(prepared.2.page.items.is_empty() && prepared.2.page.continuation.is_none());
            assert_eq!(prepared.2.page.from_cursor, prepared.2.page.to_cursor);
            assert_eq!(prepared.2.page.routing.as_ref().unwrap().items.iter().find(|e| e.id == event.id), Some(event));
            return prepared;
        }
        settle(c, caller, &prepared.2).await;
    }
    panic!("original routed event not reached within the finite fixture history")
}
async fn scan_without(c: &Controller, caller: &Caller, forbidden: &EventId) {
    // Exercise successive ordinary and routing boundaries, including a complete
    // routing wrap. Each production operation still has its configured row bound.
    let mut wraps = 0;
    for _ in 0..32 {
        let view = observe(c, caller).await;
        assert!(view.pending.is_none());
        let Some(page) = view.available.as_ref() else { return; };
        assert!(!page.items.iter().any(|e| &e.id == forbidden));
        assert!(!routed(page, forbidden), "one event-level obligation must not be reassigned");
        let wrapped = page.routing.as_ref().is_some_and(|r| r.to_cursor.is_none() && !r.has_more);
        let (_, _, batch) = prepare(c, caller, &view).await;
        settle(c, caller, &batch).await;
        if wrapped { wraps += 1; if wraps == 2 { return; } }
    }
    panic!("bounded fixture routing scan did not complete")
}
async fn exact_receipt(c: &Controller, operation: &Operation, original: &Reply) {
    let replay = call(c, operation.clone()).await;
    assert_eq!(replay.outcome, original.outcome); assert_eq!(replay.commit, original.commit);
    let lookup = call(c, Operation::GetOperation { operation_id: operation.mutation_id().unwrap().into() }).await;
    let Outcome::Ok { data: Data::Operation(record) } = lookup.outcome else { panic!("receipt: {lookup:?}") };
    assert_eq!(serde_json::from_value::<Outcome>(record.outcome.unwrap()).unwrap(), original.outcome);
    assert_eq!(lookup.commit, original.commit);
}

#[tokio::test]
async fn routing_intent_behind_ordinary_cursor_is_discovered_after_responsible_owner_changes() {
    let lab = Lab::new();
    actor_profile(&lab.c, "old-v1-owner", ExecutionProfile::CollaborationV1 { web_search: None }).await;
    let (_, next) = actor(&lab.c, "next-owner").await;
    let doc = document(&lab.c, "old-v1-owner").await;
    let q = question(&lab.c, &doc, None).await;
    let (answer_op, answer_receipt, original) = answer(&lab.c, &q).await;
    let unchanged = inventory(&lab.c).await;
    scan_without(&lab.c, &next, &original.id).await;
    let advanced = observe(&lab.c, &next).await;
    assert!(advanced.acknowledged_cursor.position.as_ref().is_some_and(|p| p >= &original.id));
    assert_eq!(inventory(&lab.c).await, unchanged, "scanning cannot create a task/session or alter accepted execution");
    owner(&lab.c, &doc, "next-owner").await;
    let (_, _, batch) = seek(&lab.c, &next, &original).await;
    assert_eq!(batch.page.from_cursor, advanced.acknowledged_cursor);
    assert!(batch.page.routing.as_ref().unwrap().items.iter().any(|e| e.id <= batch.page.from_cursor.position.clone().unwrap()));
    settle(&lab.c, &next, &batch).await;
    exact_receipt(&lab.c, &answer_op, &answer_receipt).await;
    assert_eq!(activity(&lab.c).await.iter().find(|e| e.id == original.id), Some(&original));
    scan_without(&lab.c, &next, &original.id).await;
}

#[tokio::test]
async fn routing_without_eligible_owner_waits_for_an_ordinary_admitted_v2_session() {
    let lab = Lab::new();
    let (_, v1) = actor_profile(&lab.c, "delayed-owner", ExecutionProfile::CollaborationV1 { web_search: None }).await;
    let doc = document(&lab.c, "delayed-owner").await;
    let q = question(&lab.c, &doc, None).await;
    let (answer_op, answer_receipt, original) = answer(&lab.c, &q).await;
    let before = inventory(&lab.c).await;
    for _ in 0..2 {
        refused(&call(&lab.c, Operation::ObserveCollaborationDelivery { caller: v1.clone(), limit: 8 }).await, "not_configured");
        exact_receipt(&lab.c, &answer_op, &answer_receipt).await;
        assert_eq!(inventory(&lab.c).await, before);
    }
    let lab = lab.restart().await;
    assert_eq!(inventory(&lab.c).await, before);
    let (task, eligible) = actor(&lab.c, "delayed-owner").await;
    let after_admission = inventory(&lab.c).await;
    assert_eq!(after_admission.0.len(), before.0.len() + 1);
    assert_eq!(after_admission.1.len(), before.1.len() + 1);
    assert_eq!(task.assigned_agent_id, "delayed-owner");
    let (_, _, batch) = seek(&lab.c, &eligible, &original).await;
    settle(&lab.c, &eligible, &batch).await;
    assert_eq!(inventory(&lab.c).await, after_admission);
    exact_receipt(&lab.c, &answer_op, &answer_receipt).await;
}

#[tokio::test]
async fn routing_retired_agent_requester_falls_back_to_current_document_owner() {
    let lab = Lab::new();
    let (_, requester) = actor(&lab.c, "retired-requester").await;
    let (_, current) = actor(&lab.c, "document-owner").await;
    let doc = document(&lab.c, "document-owner").await;
    let q = question(&lab.c, &doc, Some(&requester)).await;
    cancel(&lab.c, &requester).await;
    let (_, _, event) = answer(&lab.c, &q).await;
    assert!(matches!(&event.routing.as_ref().unwrap().requested_by.provenance,
        ActorProvenance::BoundAgentSession { task_id, .. } if task_id == &requester.task_id));
    let before = inventory(&lab.c).await;
    let (_, _, batch) = seek(&lab.c, &current, &event).await;
    settle(&lab.c, &current, &batch).await;
    assert_eq!(inventory(&lab.c).await, before);
}

#[tokio::test]
async fn routing_owner_change_before_begin_commits_no_append_and_preserves_original_event() {
    let lab = Lab::new();
    let (_, old) = actor(&lab.c, "owner-before").await;
    let (_, next) = actor(&lab.c, "owner-after").await;
    let doc = document(&lab.c, "owner-before").await;
    let q = question(&lab.c, &doc, None).await;
    let (answer_op, answer_receipt, original) = answer(&lab.c, &q).await;
    let (old_prepare, old_prepared, old_batch) = seek(&lab.c, &old, &original).await;
    owner(&lab.c, &doc, "owner-after").await;
    let before = observe(&lab.c, &old).await;
    let begin_old = begin(&old, &old_batch, "old-owner-no-append");
    let superseded_reply = call(&lab.c, begin_old.clone()).await;
    let superseded = delivery(&superseded_reply);
    assert!(superseded_reply.commit.is_some()); assert!(superseded.pending.is_none());
    assert_eq!(superseded.acknowledged_cursor, before.acknowledged_cursor);
    assert_eq!(superseded.superseded, Some(DeliverySupersession { batch_id: old_batch.batch_id.clone(),
        operation_id: begin_old.mutation_id().unwrap().into(), caller: old.clone(), context_binding: "old-owner-no-append".into() }));
    assert!(matches!(call(&lab.c, ack(&old, &old_batch, "old-owner-no-append")).await.outcome, Outcome::Refused { .. }));
    exact_receipt(&lab.c, &old_prepare, &old_prepared).await;
    exact_receipt(&lab.c, &begin_old, &superseded_reply).await;
    let (_, _, new_batch) = seek(&lab.c, &next, &original).await;
    settle(&lab.c, &next, &new_batch).await;
    exact_receipt(&lab.c, &answer_op, &answer_receipt).await;
    assert_eq!(activity(&lab.c).await.iter().find(|e| e.id == original.id), Some(&original));
    scan_without(&lab.c, &old, &original.id).await;
}

#[tokio::test]
async fn routing_owner_change_after_unknown_begin_cannot_reassign_one_original_claim() {
    let lab = Lab::new();
    let (_, old) = actor(&lab.c, "claimed-owner").await;
    let (_, next) = actor(&lab.c, "replacement-owner").await;
    let doc = document(&lab.c, "claimed-owner").await;
    let q = question(&lab.c, &doc, None).await;
    let (answer_op, answer_receipt, original) = answer(&lab.c, &q).await;
    let (_, _, batch) = seek(&lab.c, &old, &original).await;
    let intent = begin(&old, &batch, "unknown-original-append");
    let committed = call(&lab.c, intent.clone()).await;
    let locked = delivery(&committed).pending.unwrap();
    assert!(locked.delivery.is_some());
    // The controller committed Begin; the consumer's append result is unknown.
    // No ACK is manufactured here and an owner change cannot erase this fence.
    owner(&lab.c, &doc, "replacement-owner").await;
    let lab = lab.restart().await;
    assert_eq!(observe(&lab.c, &old).await.pending, Some(locked.clone()));
    exact_receipt(&lab.c, &intent, &committed).await;
    scan_without(&lab.c, &next, &original.id).await;
    assert!(matches!(call(&lab.c, begin(&next, &batch, "replacement-context")).await.outcome, Outcome::Refused { .. }));
    assert!(matches!(call(&lab.c, ack(&next, &batch, "replacement-context")).await.outcome, Outcome::Refused { .. }));
    assert_eq!(observe(&lab.c, &old).await.pending, Some(locked));
    exact_receipt(&lab.c, &answer_op, &answer_receipt).await;
}

#[tokio::test]
async fn routing_two_eligible_sessions_race_for_one_event_level_begin_and_ack() {
    let lab = Lab::new();
    let (_, first) = actor(&lab.c, "shared-owner").await;
    let (_, second) = actor(&lab.c, "shared-owner").await;
    assert_ne!(first.task_id, second.task_id);
    let doc = document(&lab.c, "shared-owner").await;
    let q = question(&lab.c, &doc, None).await;
    let (_, _, original) = answer(&lab.c, &q).await;
    let (_, _, a) = seek(&lab.c, &first, &original).await;
    let (_, _, b) = seek(&lab.c, &second, &original).await;
    let ia = begin(&first, &a, "claim-a"); let ib = begin(&second, &b, "claim-b");
    let (ra, rb) = tokio::join!(call(&lab.c, ia.clone()), call(&lab.c, ib.clone()));
    let da = delivery(&ra); let db = delivery(&rb);
    let claims = [&da, &db].into_iter().filter(|v| v.pending.as_ref().is_some_and(|p| p.delivery.is_some())).count();
    assert_eq!(claims, 1, "alternatives are one obligation, not fanout copies");
    let (winner, batch, context, loser, losing_batch, loser_reply) = if da.pending.is_some() {
        (&first, &a, "claim-a", &second, &b, &db)
    } else { (&second, &b, "claim-b", &first, &a, &da) };
    assert!(loser_reply.pending.is_none() && loser_reply.superseded.is_some());
    assert!(matches!(call(&lab.c, ack(loser, losing_batch, "not-the-claim")).await.outcome, Outcome::Refused { .. }));
    let acknowledge = ack(winner, batch, context);
    let ack_receipt = call(&lab.c, acknowledge.clone()).await;
    let acknowledged = delivery(&ack_receipt);
    assert!(acknowledged.pending.is_none());
    exact_receipt(&lab.c, &ia, &ra).await; exact_receipt(&lab.c, &ib, &rb).await;
    exact_receipt(&lab.c, &acknowledge, &ack_receipt).await;
    scan_without(&lab.c, &first, &original.id).await;
    scan_without(&lab.c, &second, &original.id).await;
}

#[tokio::test]
async fn routing_restart_and_lost_ack_recover_exact_receipts_without_fresh_delivery() {
    let lab = Lab::new();
    let (_, caller) = actor(&lab.c, "receipt-owner").await;
    let (_, alternative) = actor(&lab.c, "receipt-owner").await;
    let doc = document(&lab.c, "receipt-owner").await;
    let q = question(&lab.c, &doc, None).await;
    let (answer_op, answer_receipt, original) = answer(&lab.c, &q).await;
    let (prepare_op, prepare_receipt, batch) = seek(&lab.c, &caller, &original).await;
    let lab = lab.restart().await;
    exact_receipt(&lab.c, &prepare_op, &prepare_receipt).await;
    assert_eq!(observe(&lab.c, &caller).await.pending, Some(batch.clone()));
    let begin_op = begin(&caller, &batch, "retained-context");
    let begin_receipt = call(&lab.c, begin_op.clone()).await;
    let begun = delivery(&begin_receipt).pending.unwrap();
    let lab = lab.restart().await;
    assert_eq!(observe(&lab.c, &caller).await.pending, Some(begun));
    exact_receipt(&lab.c, &begin_op, &begin_receipt).await;
    let ack_op = ack(&caller, &batch, "retained-context");
    let ack_receipt = call(&lab.c, ack_op.clone()).await;
    assert!(delivery(&ack_receipt).pending.is_none());
    // Simulate only loss of the committed reply, not a second executor append.
    let lab = lab.restart().await;
    let before = inventory(&lab.c).await;
    for (op, receipt) in [(&answer_op, &answer_receipt), (&prepare_op, &prepare_receipt), (&begin_op, &begin_receipt), (&ack_op, &ack_receipt)] {
        exact_receipt(&lab.c, op, receipt).await;
    }
    assert!(observe(&lab.c, &caller).await.pending.is_none());
    assert!(matches!(call(&lab.c, begin(&caller, &batch, "fresh-resend-is-forbidden")).await.outcome, Outcome::Refused { .. }));
    scan_without(&lab.c, &caller, &original.id).await;
    scan_without(&lab.c, &alternative, &original.id).await;
    assert_eq!(inventory(&lab.c).await, before);
}

#[tokio::test]
async fn routing_zero_and_stale_cursor_refusals_preserve_original_batch_and_receipts() {
    let lab = Lab::new();
    let (_, caller) = actor(&lab.c, "cursor-owner").await;
    let doc = document(&lab.c, "cursor-owner").await;
    let q = question(&lab.c, &doc, None).await;
    let (answer_op, answer_receipt, original) = answer(&lab.c, &q).await;
    let before = observe(&lab.c, &caller).await;
    refused(&call(&lab.c, Operation::ObserveCollaborationDelivery { caller: caller.clone(), limit: 0 }).await, "invalid_input");
    refused(&call(&lab.c, prepare_request(&caller, &before, 0)).await, "invalid_input");
    assert_eq!(observe(&lab.c, &caller).await, before);
    let (_, _, batch) = seek(&lab.c, &caller, &original).await;
    let exact = observe(&lab.c, &caller).await;
    let wrong = Operation::PrepareCollaborationDelivery { operation_id: unique(), caller: caller.clone(),
        expected_cursor: exact.acknowledged_cursor.clone(), expected_routing_cursor: Some(original.id.clone()),
        through: original.id.clone(), limit: 8 };
    assert_ne!(exact.routing_cursor, Some(original.id.clone()));
    refused(&call(&lab.c, wrong).await, "stale_revision");
    assert_eq!(observe(&lab.c, &caller).await, exact);
    exact_receipt(&lab.c, &answer_op, &answer_receipt).await;
    settle(&lab.c, &caller, &batch).await;
}

#[tokio::test]
async fn routing_one_row_scan_reaches_each_original_intent_without_skipping_or_fanout() {
    let mut selected = limits(); selected.workflow.as_mut().unwrap().delivery_scan_rows = 1;
    selected.validate().unwrap(); let lab = Lab::with(selected);
    let (_, caller) = actor(&lab.c, "scan-owner").await;
    let doc = document(&lab.c, "scan-owner").await;
    let mut originals = vec![];
    for _ in 0..3 {
        let q = question(&lab.c, &doc, None).await;
        originals.push(answer(&lab.c, &q).await);
    }
    let before = inventory(&lab.c).await;
    let mut delivered = vec![]; let mut scans = 0;
    for _ in 0..64 {
        let view = observe(&lab.c, &caller).await;
        let Some(page) = view.available.as_ref() else { break; };
        if let Some(route) = &page.routing {
            scans += 1;
            assert!(route.items.len() <= 1, "routing shares the explicit one-row scan bound");
            for event in &route.items {
                assert!(originals.iter().any(|(_, _, original)| original == event));
                assert!(!delivered.contains(&event.id)); delivered.push(event.id.clone());
            }
        }
        let (_, _, batch) = prepare(&lab.c, &caller, &view).await;
        settle(&lab.c, &caller, &batch).await;
        if delivered.len() == originals.len() { break; }
    }
    assert!(scans >= 3);
    assert_eq!(delivered, originals.iter().map(|(_, _, event)| event.id.clone()).collect::<Vec<_>>());
    assert_eq!(inventory(&lab.c).await, before);
    for (op, receipt, event) in originals {
        exact_receipt(&lab.c, &op, &receipt).await;
        scan_without(&lab.c, &caller, &event.id).await;
    }
}

#[tokio::test]
async fn routing_first_event_exceeding_page_bytes_refuses_without_claim_or_lost_receipt() {
    let mut lab = Lab::new();
    actor_profile(&lab.c, "bounded-route-owner", ExecutionProfile::CollaborationV1 { web_search: None }).await;
    let doc = document(&lab.c, "bounded-route-owner").await;
    let q = question(&lab.c, &doc, None).await;
    let (answer_op, answer_receipt, original) = answer(&lab.c, &q).await;
    // Retain the real event first, then ordinarily admit the V2 recipient with
    // explicit smaller metadata. This isolates delivery from activity-query and
    // producer response bounds; no stored bytes or accepted snapshot are edited.
    let mut selected = (*lab.config.limits).clone();
    selected.workflow.as_mut().unwrap().page_bytes = serde_json::to_vec(&original).unwrap().len() + 128;
    selected.validate().unwrap(); lab.config.limits = Arc::new(selected);
    let lab = lab.restart().await;
    let (_, caller) = actor(&lab.c, "bounded-route-owner").await;
    let before = inventory(&lab.c).await;
    let mut response = call(&lab.c, Operation::ObserveCollaborationDelivery { caller: caller.clone(), limit: 8 }).await;
    // A bounded ordinary prefix may fit before the independent routing page.
    // Acknowledge only input-free pages; no routed input may pass this bound.
    for _ in 0..8 {
        if matches!(&response.outcome, Outcome::Refused { .. }) { break; }
        let view = delivery(&response);
        assert!(!view.available.as_ref().unwrap().has_input());
        let (_, _, batch) = prepare(&lab.c, &caller, &view).await;
        settle(&lab.c, &caller, &batch).await;
        response = call(&lab.c, Operation::ObserveCollaborationDelivery { caller: caller.clone(), limit: 8 }).await;
    }
    refused(&response, "response_bound");
    assert!(matches!(&response.outcome, Outcome::Refused { message, .. } if message.contains("workflow.page_bytes")));
    assert_eq!(inventory(&lab.c).await, before);
    exact_receipt(&lab.c, &answer_op, &answer_receipt).await;
    let lab = lab.restart().await;
    exact_receipt(&lab.c, &answer_op, &answer_receipt).await;
    refused(&call(&lab.c, Operation::ObserveCollaborationDelivery { caller, limit: 8 }).await, "response_bound");
    assert_eq!(inventory(&lab.c).await, before);
}

#[tokio::test]
async fn routing_retains_exact_previously_acknowledged_work_input_without_second_append() {
    use borg_runtime::collaboration::work::WorkMutation;
    let lab = Lab::new();
    actor_profile(&lab.c, "initial-ineligible-owner", ExecutionProfile::CollaborationV1 { web_search: None }).await;
    let doc = document(&lab.c, "initial-ineligible-owner").await;
    let q = question(&lab.c, &doc, None).await;
    let waiting = task(call(&lab.c, Operation::SubmitTask { operation_id: unique(),
        input: input("work-recipient", ExecutionProfile::CollaborationV2 { web_search: None }) }).await);
    assert!(waiting.attempt.is_none());
    let added = call(&lab.c, Operation::MutateWork { operation_id: unique(), mutation: WorkMutation::AddWait {
        task_id: waiting.id.clone(), expected_revision: 0, resolver: ResolverRef::Question { question_id: q.id.clone() },
    } }).await;
    let Outcome::Ok { data: Data::WorkMutation(added) } = added.outcome else { panic!("add wait: {added:?}") };
    assert_eq!(added.view.open_waits, 1);
    let (answer_op, answer_receipt, original) = answer(&lab.c, &q).await;

    // Ordinary existing admission delivers this concrete wait outcome even
    // though the document owner's independent routing alternative is ineligible.
    let worker = Worker { id: unique(), incarnation: "inc".into(), coding_capacity: 0, native_capacity: 1 };
    assert!(matches!(call(&lab.c, Operation::RegisterWorker { operation_id: unique(), worker: worker.clone() }).await.outcome, Outcome::Ok { .. }));
    let reply = call(&lab.c, Operation::AwaitCommands { operation_id: unique(), worker_id: worker.id,
        incarnation: worker.incarnation, acknowledged: vec![] }).await;
    let Outcome::Ok { data: Data::Commands(commands) } = reply.outcome else { panic!("admission: {reply:?}") };
    let attempt = commands.into_iter().find_map(|command| match command {
        WorkerCommand::RunAttempt { attempt, .. }
            if attempt.task_id == waiting.id => Some(attempt),
        _ => None,
    }).expect("existing worker performs ordinary admission of the resolved task");
    let caller = Caller { task_id: attempt.task_id, attempt_id: attempt.id, generation: attempt.generation,
        worker_id: attempt.worker_id, incarnation: attempt.worker_incarnation };
    report(&lab.c, &caller, Observation::Started { thread_id: None, turn_id: None }).await;
    let offer = observe(&lab.c, &caller).await;
    let continuation = offer.available.as_ref().unwrap().continuation.clone()
        .expect("delivery resolves the exact admitted WORK association");
    assert_eq!(continuation.task_id, waiting.id);
    assert_eq!(continuation.session_id, waiting.session_id);
    assert_eq!(continuation.resolved_waits.len(), 1);
    assert_eq!(continuation.resolved_waits[0].event_id, original.id);
    let (_, _, input_batch) = prepare(&lab.c, &caller, &offer).await;
    assert_eq!(input_batch.page.continuation.as_ref(), Some(&continuation));
    assert!(input_batch.page.routing.is_none());
    let begin_op = begin(&caller, &input_batch, "work-original-context");
    let begin_receipt = call(&lab.c, begin_op.clone()).await;
    assert!(delivery(&begin_receipt).pending.as_ref().unwrap().delivery.is_some());
    // Admission is not ACK. Commit this exact input ACK before changing owner.
    let ack_op = ack(&caller, &input_batch, "work-original-context");
    let ack_receipt = call(&lab.c, ack_op.clone()).await;
    assert!(delivery(&ack_receipt).pending.is_none());
    owner(&lab.c, &doc, "work-recipient").await;
    let before = inventory(&lab.c).await;

    let mut retained_page = None;
    for _ in 0..16 {
        let view = observe(&lab.c, &caller).await;
        let page = view.available.as_ref().expect("the newly eligible retained routing obligation remains scannable");
        assert!(!page.items.iter().any(|event| event.id == original.id));
        if let Some(route) = &page.routing {
            assert!(route.items.is_empty(), "the exact WORK input was already acknowledged, so it is not appended again");
            assert!(!page.has_input());
            assert!(route.from_cursor.as_ref().is_none_or(|from| from < &original.id));
            assert!(route.to_cursor.as_ref().is_none_or(|to| to >= &original.id));
            retained_page = Some(prepare(&lab.c, &caller, &view).await);
            break;
        }
        let (_, _, prefix) = prepare(&lab.c, &caller, &view).await;
        settle(&lab.c, &caller, &prefix).await;
    }
    let (route_prepare, route_receipt, route_batch) = retained_page.expect("one bounded routing scan covers the original occurrence");
    assert!(route_batch.delivery.is_none() && !route_batch.page.has_input());
    assert_eq!(inventory(&lab.c).await, before);

    // targets.retained is deliberately not a client-selected DTO. Inspect this
    // one canonical row only after releasing the EXCLUSIVE controller owner.
    let Lab { root, c, config } = lab;
    drop(c); released(&root.join("tasks.sqlite")).await;
    let db = rusqlite::Connection::open_with_flags(root.join("tasks.sqlite"), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let (body, targets): (String, String) = db.query_row(
        "SELECT body,targets FROM workflow_events WHERE product_revision=?1 AND ordinal=?2",
        rusqlite::params![i64::try_from(original.id.product_revision).unwrap(), i64::from(original.id.ordinal)],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ).unwrap();
    assert_eq!(serde_json::from_str::<WorkflowActivity>(&body).unwrap(), original);
    let envelope: Value = serde_json::from_str(&targets).unwrap();
    assert!(envelope["route"].is_null(), "no event-level Begin claim is synthesized for previously retained input");
    assert_eq!(envelope["retained"], json!({
        "operation_id": route_prepare.mutation_id().unwrap(),
        "scope": route_batch.page.from_cursor.scope,
        "caller": caller,
        "context_binding": "work-original-context",
    }));
    let requests = {
        let mut statement = db.prepare("SELECT request FROM receipts ORDER BY controller_revision LIMIT 257").unwrap();
        let rows = statement.query_map([], |row| row.get::<_, String>(0)).unwrap();
        rows.map(|row| serde_json::from_str::<Operation>(&row.unwrap()).unwrap()).collect::<Vec<_>>()
    };
    assert!(requests.len() <= 256, "finite fixture receipt audit bound");
    assert!(!requests.iter().any(|operation| matches!(operation,
        Operation::BeginCollaborationDelivery { batch_id, .. }
        | Operation::AcknowledgeCollaborationDelivery { batch_id, .. } if batch_id == &route_batch.batch_id)),
        "retaining the original proof requires neither a new Begin nor an input ACK");
    drop(db);
    let c = Controller::open_with_config(&root.join("tasks.sqlite"), config.clone()).unwrap();
    let lab = Lab { root, c, config };
    assert_eq!(observe(&lab.c, &caller).await.pending, Some(route_batch.clone()));
    exact_receipt(&lab.c, &route_prepare, &route_receipt).await;
    // Only the empty scan-prefix ACK follows, bound to its own batch ID. This
    // is not a second executor-input ACK for the original WORK continuation.
    let prefix_ack = ack(&caller, &route_batch, &route_batch.batch_id);
    let prefix_receipt = call(&lab.c, prefix_ack.clone()).await;
    assert!(delivery(&prefix_receipt).pending.is_none());
    for (op, receipt) in [(&answer_op, &answer_receipt), (&begin_op, &begin_receipt),
        (&ack_op, &ack_receipt), (&route_prepare, &route_receipt), (&prefix_ack, &prefix_receipt)] {
        exact_receipt(&lab.c, op, receipt).await;
    }
    scan_without(&lab.c, &caller, &original.id).await;
    assert_eq!(inventory(&lab.c).await, before);
}


// Startup validates retained terminal payloads against the original controller
// occurrence, not merely valid IDs and subject revisions. These tests alone
// corrupt a canonical event body, after releasing the EXCLUSIVE controller.
mod terminal_payload_corruption {
    use super::*;

    async fn cancelled_task(lab: &Lab) -> (Task, Operation, Reply, WorkflowActivity) {
        let submitted = task(call(&lab.c, Operation::SubmitTask {
            operation_id: unique(),
            input: input("terminal-payload-owner", ExecutionProfile::CollaborationV2 { web_search: None }),
        }).await);
        assert!(submitted.attempt.is_none(), "queued fixture needs no worker or executor");
        let operation = Operation::CancelTask {
            operation_id: unique(), task_id: submitted.id.clone(), expected_revision: submitted.revision,
        };
        let receipt = call(&lab.c, operation.clone()).await;
        let cancelled = task(receipt.clone());
        assert_eq!(cancelled.status, TaskStatus::Cancelled);
        assert!(cancelled.attempt.is_none() && cancelled.result.is_none());
        let mut events = activity(&lab.c).await.into_iter().filter(|event| {
            event.subject == (RecordRef::Task { task_id: submitted.id.clone() })
                && matches!(&event.detail, ActivityDetail::Task(TaskChange::Closed { .. }))
        }).collect::<Vec<_>>();
        assert_eq!(events.len(), 1, "one actual first terminal task occurrence");
        let original = events.pop().unwrap();
        let ActivityDetail::Task(TaskChange::Closed { resolution }) = &original.detail else { unreachable!() };
        assert_eq!(resolution.task_id, cancelled.id);
        assert_eq!(resolution.terminal_task_revision, cancelled.revision);
        assert_eq!(resolution.status, TerminalTaskStatus::Cancelled);
        assert!(resolution.accepted_result.is_none());
        assert_eq!(resolution.summary, cancelled.blocked_reason.as_deref().unwrap_or_default());
        (cancelled, operation, receipt, original)
    }

    async fn reject_changed_payload(
        lab: Lab,
        original_operation: Operation,
        original_receipt: Reply,
        original: WorkflowActivity,
        corrupted: WorkflowActivity,
        expected_error: &str,
    ) {
        assert_ne!(corrupted.detail, original.detail);
        let mut outer_only = corrupted.clone();
        outer_only.detail = original.detail.clone();
        assert_eq!(outer_only, original, "only the semantic detail changes; IDs, subject, revisions, actor and routing remain exact");
        let encoded = serde_json::to_string(&corrupted).unwrap();
        assert_eq!(serde_json::from_str::<WorkflowActivity>(&encoded).unwrap(), corrupted);
        assert!(encoded.len() <= lab.config.limits.workflow.as_ref().unwrap().event_bytes,
            "the payload remains valid, canonical and within its configured representation bound");

        // Prove this exact original fixture is accepted at startup before any
        // corruption. Replay and lookup preserve its original operation receipt.
        let lab = lab.restart().await;
        assert_eq!(activity(&lab.c).await.into_iter().find(|event| event.id == original.id), Some(original.clone()));
        exact_receipt(&lab.c, &original_operation, &original_receipt).await;
        let Lab { root, c, config } = lab;
        let path = root.join("tasks.sqlite");
        drop(c);
        released(&path).await;

        let db = rusqlite::Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE).unwrap();
        let key_revision = i64::try_from(original.id.product_revision).unwrap();
        let key_ordinal = i64::from(original.id.ordinal);
        let (old_body, old_targets): (String, String) = db.query_row(
            "SELECT body,targets FROM workflow_events WHERE product_revision=?1 AND ordinal=?2",
            rusqlite::params![key_revision, key_ordinal],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).unwrap();
        assert_eq!(old_body, serde_json::to_string(&original).unwrap());
        assert_eq!(db.execute(
            "UPDATE workflow_events SET body=?3 WHERE product_revision=?1 AND ordinal=?2 AND body=?4",
            rusqlite::params![key_revision, key_ordinal, &encoded, &old_body],
        ).unwrap(), 1, "alter only this original canonical event body");
        let (stored_body, stored_targets): (String, String) = db.query_row(
            "SELECT body,targets FROM workflow_events WHERE product_revision=?1 AND ordinal=?2",
            rusqlite::params![key_revision, key_ordinal],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).unwrap();
        assert_eq!(stored_body, encoded);
        assert_eq!(stored_targets, old_targets, "no target/claim, receipt or domain row is repaired to match the corruption");
        drop(db);

        let original_corrupted_bytes = std::fs::read(&path).unwrap();
        match Controller::open_with_config(&path, config) {
            Err(error) => assert_eq!(error,
                "invalid stored collaboration state; original bytes retained",
                "public startup refusal for {expected_error}"),
            Ok(controller) => {
                drop(controller);
                panic!("startup accepted a canonical event whose terminal payload differs from its original occurrence");
            }
        }
        assert_eq!(std::fs::read(&path).unwrap(), original_corrupted_bytes,
            "rejected startup must preserve the exact corrupted store, not repair it");
    }

    #[tokio::test]
    async fn startup_rejects_terminal_task_status_changed_at_same_occurrence() {
        let lab = Lab::new();
        let (_, operation, receipt, original) = cancelled_task(&lab).await;
        let mut corrupted = original.clone();
        let ActivityDetail::Task(TaskChange::Closed { resolution }) = &mut corrupted.detail else { unreachable!() };
        resolution.status = TerminalTaskStatus::Failed;
        reject_changed_payload(lab, operation, receipt, original, corrupted,
            "task resolution differs from original terminal occurrence").await;
    }

    #[tokio::test]
    async fn startup_rejects_terminal_task_summary_changed_at_same_occurrence() {
        let lab = Lab::new();
        let (_, operation, receipt, original) = cancelled_task(&lab).await;
        let mut corrupted = original.clone();
        let ActivityDetail::Task(TaskChange::Closed { resolution }) = &mut corrupted.detail else { unreachable!() };
        resolution.summary.push_str(" (different retained summary)");
        reject_changed_payload(lab, operation, receipt, original, corrupted,
            "task resolution differs from original terminal occurrence").await;
    }

    #[tokio::test]
    async fn startup_rejects_answered_question_option_changed_at_same_occurrence() {
        let lab = Lab::new();
        // Register an actual agent definition/session by ordinary submission;
        // this queued fixture never creates a worker or performs execution.
        task(call(&lab.c, Operation::SubmitTask {
            operation_id: unique(),
            input: input("question-payload-owner", ExecutionProfile::CollaborationV2 { web_search: None }),
        }).await);
        let doc = document(&lab.c, "question-payload-owner").await;
        let q = question(&lab.c, &doc, None).await;
        let (operation, receipt, original) = answer(&lab.c, &q).await;
        assert!(q.posed.options.iter().any(|option| option.id == "change"));
        let mut corrupted = original.clone();
        let ActivityDetail::Question(QuestionChange::Closed {
            resolution: QuestionResolution::Answered { selected_option_id, .. },
        }) = &mut corrupted.detail else { unreachable!() };
        assert_eq!(selected_option_id.as_str(), "keep");
        *selected_option_id = "change".into();
        reject_changed_payload(lab, operation, receipt, original, corrupted,
            "workflow question closure differs from its original immutable occurrence").await;
    }


    #[tokio::test]
    async fn startup_rejects_decided_review_verdict_changed_at_same_occurrence() {
        let lab = Lab::new();
        task(call(&lab.c, Operation::SubmitTask {
            operation_id: unique(),
            input: input("review-payload-owner", ExecutionProfile::CollaborationV2 { web_search: None }),
        }).await);
        let doc = document(&lab.c, "review-payload-owner").await;
        let target = ReviewTarget::DocumentVersion { version: doc.version.reference.clone() };
        let request = call(&lab.c, Operation::MutateReview {
            operation_id: unique(), mutation: ReviewMutation::Request {
                target: target.clone(), reviewer: PrincipalRef::TrustedHostOperator,
                summary: "Assess this exact retained document version".into(),
            },
        }).await;
        let review = match request.outcome {
            Outcome::Ok { data: Data::ReviewMutation(ReviewMutationOutcome::Review(view)) } => view.review,
            other => panic!("requested review: {other:?}"),
        };
        let operation = Operation::MutateReview {
            operation_id: unique(), mutation: ReviewMutation::Decide {
                review_id: review.id.clone(), expected_revision: review.revision,
                verdict: ReviewVerdict::Approved, explanation: "The exact version meets the stated condition".into(),
            },
        };
        let receipt = call(&lab.c, operation.clone()).await;
        assert!(receipt.commit.is_some());
        let decided = match &receipt.outcome {
            Outcome::Ok { data: Data::ReviewMutation(ReviewMutationOutcome::Review(view)) } => view,
            other => panic!("decided review: {other:?}"),
        };
        assert_eq!(decided.review.status, ReviewStatus::Decided);
        let decision = decided.decision.as_ref().expect("actual immutable decision committed");
        assert_eq!(decision.verdict, ReviewVerdict::Approved);
        let mut events = activity(&lab.c).await.into_iter().filter(|event| {
            event.subject == (RecordRef::Review { review_id: review.id.clone() })
                && matches!(&event.detail, ActivityDetail::Review(ReviewChange::Closed { .. }))
        }).collect::<Vec<_>>();
        assert_eq!(events.len(), 1, "one actual review terminal occurrence");
        let original = events.pop().unwrap();
        assert_eq!(original.subject_revision, decided.review.revision);
        assert_eq!(original.detail, ActivityDetail::Review(ReviewChange::Closed {
            resolution: ReviewResolution::Decided {
                review_id: review.id, decision_id: decision.id.clone(), target, verdict: ReviewVerdict::Approved,
            },
        }));
        let mut corrupted = original.clone();
        let ActivityDetail::Review(ReviewChange::Closed {
            resolution: ReviewResolution::Decided { verdict, .. },
        }) = &mut corrupted.detail else { unreachable!() };
        *verdict = ReviewVerdict::Rejected;
        reject_changed_payload(lab, operation, receipt, original, corrupted,
            "workflow review closure differs from its original immutable occurrence").await;
    }
}

#[path = "collaboration_routing/joined.rs"]
mod collaboration_routing_joined;

// Gate: V2_COLLAB_DELIVERY / DELIVERY, concurrent cancellation.
// This real-controller component fixture supplies the receiving ACK; it does
// not claim an actual executor append. Existing native/Codex cases own that proof.
//= spec/impl/v2/implementation.md#committed-resolutions-reach-their-consumers
//# A committed resolution delivery MUST carry an identity that deduplicates its delivery retries.
//= spec/impl/v2/implementation.md#committed-resolutions-reach-their-consumers
//# A resolution notice MUST NOT reopen a terminal task.
#[tokio::test]
async fn routing_original_ack_and_cancellation_keep_both_committed_orders_after_reopen() {
    use std::{future::Future, sync::{Mutex, mpsc}, task::Poll};
    use tokio::sync::oneshot;

    // Read only after releasing the controller's sole database owner. Compare
    // the original event/claim, session cursor/pending body and receipt count.
    fn closed_projection(path: &Path, session: &str, event: &EventId,
        operations: &[&Operation]) -> (String, String, String, i64, i64, i64) {
        let db = rusqlite::Connection::open_with_flags(path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let delivery: String = db.query_row(
            "SELECT workflow_delivery FROM sessions WHERE id=?1", [session], |r| r.get(0)).unwrap();
        let (body, targets): (String, String) = db.query_row(
            "SELECT body,targets FROM workflow_events WHERE product_revision=?1 AND ordinal=?2",
            rusqlite::params![i64::try_from(event.product_revision).unwrap(), i64::from(event.ordinal)], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
        for operation in operations {
            let count: i64 = db.query_row("SELECT count(*) FROM receipts WHERE id=?1",
                [operation.mutation_id().unwrap()], |r| r.get(0)).unwrap();
            assert_eq!(count, 1, "one original mutation receipt, including a refused ACK");
        }
        let tasks = db.query_row("SELECT count(*) FROM tasks", [], |r| r.get(0)).unwrap();
        let sessions = db.query_row("SELECT count(*) FROM sessions", [], |r| r.get(0)).unwrap();
        let receipts = db.query_row("SELECT count(*) FROM receipts", [], |r| r.get(0)).unwrap();
        (delivery, body, targets, tasks, sessions, receipts)
    }

    for ack_first in [true, false] {
        // The existing injected clock holds the database owner before either
        // operation can commit. It preserves time; no production hook is added.
        type Gate = (oneshot::Sender<()>, mpsc::Receiver<()>);
        let gate = Arc::new(Mutex::new(None::<Gate>));
        let clock_gate = gate.clone();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let root = std::env::temp_dir().join(unique());
        std::fs::create_dir(&root).unwrap();
        let config = ControllerConfig {
            limits: Arc::new(limits()),
            validation_executable: PathBuf::from(env!("CARGO_BIN_EXE_borg-runtime")),
            clock: Arc::new(move || {
                if let Some((entered, release)) = clock_gate.lock().unwrap().take() {
                    entered.send(()).expect("race owner barrier observed");
                    release.recv_timeout(Duration::from_secs(5)).expect("bounded race owner release");
                }
                now
            }),
            ..ControllerConfig::default()
        };
        let c = Controller::open_with_config(root.join("tasks.sqlite"), config.clone()).unwrap();
        let lab = Lab { root, c, config };
        let (_, caller) = actor(&lab.c, "cancellation-race-owner").await;
        let doc = document(&lab.c, "cancellation-race-owner").await;
        let q = question(&lab.c, &doc, Some(&caller)).await;
        let (answer_op, answer_receipt, original) = answer(&lab.c, &q).await;
        let (prepare_op, prepare_receipt, batch) = seek(&lab.c, &caller, &original).await;
        let context = "original-cancellation-race-input";
        let begin_op = begin(&caller, &batch, context);
        let begin_receipt = call(&lab.c, begin_op.clone()).await;
        let begun = delivery(&begin_receipt).pending.unwrap();
        assert!(begun.delivery.is_some());
        let before = task(call(&lab.c, Operation::GetTask { task_id: caller.task_id.clone() }).await);
        let before_inventory = inventory(&lab.c).await;
        assert_eq!(before_inventory.0.len(), 1);
        assert_eq!(before_inventory.1.len(), 1);
        let acknowledge = ack(&caller, &batch, context);
        let cancellation = Operation::CancelTask { operation_id: unique(),
            task_id: before.id.clone(), expected_revision: before.revision };
        let (first_op, second_op) = if ack_first {
            (acknowledge.clone(), cancellation.clone())
        } else { (cancellation.clone(), acknowledge.clone()) };
        let (entered, arrived) = oneshot::channel();
        let (release, resume) = mpsc::channel();
        assert!(gate.lock().unwrap().replace((entered, resume)).is_none());
        let mut first = Box::pin(call(&lab.c, first_op));
        let mut second = Box::pin(call(&lab.c, second_op));
        // Polling through dispatch's synchronous enqueue to its pending reply
        // proves submission. Both calls are outstanding before owner release.
        std::future::poll_fn(|cx| {
            assert!(first.as_mut().poll(cx).is_pending()); Poll::Ready(())
        }).await;
        tokio::time::timeout(Duration::from_secs(5), arrived).await.unwrap().unwrap();
        std::future::poll_fn(|cx| {
            assert!(second.as_mut().poll(cx).is_pending()); Poll::Ready(())
        }).await;
        release.send(()).unwrap();
        let (first_reply, second_reply) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(first, second)
        }).await.expect("both original queued mutations completed");
        assert!(first_reply.commit.as_ref().unwrap().controller_revision
            < second_reply.commit.as_ref().unwrap().controller_revision);
        let (ack_receipt, cancel_receipt) = if ack_first {
            (first_reply, second_reply)
        } else { (second_reply, first_reply) };
        let cancelled = task(cancel_receipt.clone());
        assert_eq!(cancelled.status, TaskStatus::Cancelled);
        assert_eq!(cancelled.input, before.input);
        assert_eq!(cancelled.session_id, before.session_id);
        assert_eq!(cancelled.attempt.as_ref().unwrap().phase, AttemptPhase::StopRequested);
        assert!(!cancelled.attempt.as_ref().unwrap().stopped,
            "cancellation receipt is not physical termination evidence");
        if ack_first {
            let acknowledged = delivery(&ack_receipt);
            assert!(acknowledged.pending.is_none());
            assert_eq!(acknowledged.acknowledged_cursor, batch.page.to_cursor);
        } else {
            refused(&ack_receipt, "invalid_transition");
        }
        // A fresh delivery operation cannot use the terminal caller. Replaying
        // the original receipt below is distinct from authorizing a new append.
        refused(&call(&lab.c, begin(&caller, &batch, "forbidden-new-input")).await,
            "invalid_transition");
        refused(&call(&lab.c, ack(&caller, &batch, context)).await, "invalid_transition");
        let stopped = report(&lab.c, &caller, Observation::ProcessTreeStopped).await;
        assert_eq!(stopped.status, TaskStatus::Cancelled);
        assert!(stopped.attempt.as_ref().unwrap().stopped);
        assert_eq!(stopped.input, before.input);
        let terminal_inventory = inventory(&lab.c).await;
        assert_eq!(terminal_inventory.1, before_inventory.1);
        assert_eq!(terminal_inventory.0, vec![stopped.clone()]);
        let closed_events = activity(&lab.c).await.into_iter().filter(|event|
            event.subject == original.subject && matches!(&event.detail,
                ActivityDetail::Question(QuestionChange::Closed { .. }))).collect::<Vec<_>>();
        assert_eq!(closed_events, vec![original.clone()]);

        let Lab { root, c, config } = lab;
        let db_path = root.join("tasks.sqlite");
        drop(c); released(&db_path).await;
        let operations = [&answer_op, &prepare_op, &begin_op, &acknowledge, &cancellation];
        let durable = closed_projection(&db_path, &before.session_id, &original.id, &operations);
        let state: Value = serde_json::from_str(&durable.0).unwrap();
        let event: WorkflowActivity = serde_json::from_str(&durable.1).unwrap();
        let targets: Value = serde_json::from_str(&durable.2).unwrap();
        assert_eq!(event, original);
        assert_eq!(targets["route"]["batch_id"], json!(batch.batch_id));
        assert_eq!(targets["route"]["scope"], json!(batch.page.from_cursor.scope));
        assert_eq!(targets["route"]["intent"], json!(begun.delivery.as_ref().unwrap()));
        assert_eq!(targets["route"]["acknowledged"], json!(ack_first));
        assert!(targets.get("retained").is_none());
        if ack_first {
            assert!(state["pending"].is_null());
            assert_eq!(state["last_ack_context"], json!(context));
        } else {
            assert_eq!(serde_json::from_value::<DeliveryBatch>(state["pending"].clone()).unwrap(), begun);
            assert_eq!(state["acknowledged_cursor"], json!(batch.page.from_cursor));
        }
        assert_eq!((durable.3, durable.4), (1, 1));
        let reopened = Controller::open_with_config(&db_path, config).unwrap();
        for (operation, receipt) in [(&answer_op, &answer_receipt), (&prepare_op, &prepare_receipt),
            (&begin_op, &begin_receipt), (&acknowledge, &ack_receipt), (&cancellation, &cancel_receipt)] {
            exact_receipt(&reopened, operation, receipt).await;
        }
        assert_eq!(inventory(&reopened).await, terminal_inventory);
        let repeated_closed = activity(&reopened).await.into_iter().filter(|event|
            event.subject == original.subject && matches!(&event.detail,
                ActivityDetail::Question(QuestionChange::Closed { .. }))).collect::<Vec<_>>();
        assert_eq!(repeated_closed, vec![original.clone()]);
        drop(reopened); released(&db_path).await;
        assert_eq!(closed_projection(&db_path, &before.session_id, &original.id, &operations), durable);
    }
}
