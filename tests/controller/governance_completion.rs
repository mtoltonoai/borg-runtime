//! Actual Controller completion boundaries, receipt transactions and delivery.
//! Controlled reports qualify Controller state; they do not claim that a model
//! consumed input. The native/Codex producer suite owns that separate proof.
use super::*;
use super::governance::{governance_config, promote};
use borg_runtime::{
    collaboration::{notifications::*, questions::*, work::*, workflow::*},
    governance::*,
    governance_completion::*,
    mcp::{Caller, Invocation},
};
use std::collections::BTreeMap;

#[path = "governance_task_read.rs"]
mod task_read;

fn id(label: &str) -> String {
    format!("completion-{label}-{}", NEXT.fetch_add(1, Ordering::Relaxed))
}
async fn reply(c: &Controller, command: Operation) -> Reply {
    let value = Box::pin(c.handle(Request {
        protocol_revision: WIRE_REVISION, request_id: id("request"), command: command.clone(),
    })).await;
    value.validate_for(&command).unwrap();
    value
}
fn same_original(actual: &Reply, original: &Reply) {
    assert_eq!(actual.outcome, original.outcome);
    assert_eq!(actual.commit, original.commit);
    assert!(actual.commit.is_some());
}
fn caller(a: &Attempt) -> Caller {
    Caller { task_id: a.task_id.clone(), attempt_id: a.id.clone(), generation: a.generation,
        worker_id: a.worker_id.clone(), incarnation: a.worker_incarnation.clone() }
}
async fn reserve(c: &Controller, a: &Attempt, index: u32) {
    // Current positive charge, through its original canonical identity. A charge
    // alone does not assert a terminal provider observation or consume input.
    task(report(c, a, &borg_runtime::native_loop_contract::reservation_id(&a.id, index),
        Observation::Usage { native_diagnostic_reserved: true, model_requests: index,
            input_tokens: Some(u64::from(index) * 4), output_tokens: Some(u64::from(index) * 2) }).await);
}
async fn active(c: &Controller, label: &str) -> (Task, Attempt, Caller, Worker) {
    let mut input = native(&id(label));
    input.definition.execution_profile = Some(ExecutionProfile::CollaborationV2 { publication: None, web_search: None });
    let queued = submit(c, &id("submit"), input).await;
    let worker = worker(&id("worker"), "inc", 1, 0);
    register(c, &worker).await;
    let a = commands(c, &worker, vec![]).await.into_iter().find_map(|command| match command {
        WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == queued.id => Some(attempt),
        _ => None,
    }).expect("ordinary admission of this exact task");
    task(report(c, &a, &id("started"), Observation::Started { thread_id: None, turn_id: None }).await);
    reserve(c, &a, 1).await;
    (get(c, &queued.id).await, a.clone(), caller(&a), worker)
}
fn evaluation(caller: &Caller, index: u32) -> Operation {
    let boundary = CompletionBoundary::NativeResponse { request_index: index };
    Operation::EvaluateCompletion(EvaluateCompletion {
        operation_id: evaluation_id(caller, &boundary).unwrap(), caller: caller.clone(), boundary,
        intervening_requests: vec![],
    })
}
fn decision_ref(operation: &Operation) -> DecisionRef {
    let Operation::EvaluateCompletion(value) = operation else { panic!("completion operation required") };
    DecisionRef { operation_id: value.operation_id.clone(), point: DecisionPoint::Completion }
}
async fn audit(c: &Controller, operation: &Operation) -> DecisionRecord {
    match call(c, Operation::QueryGovernance { query: GovernanceQuery::Decision { decision: decision_ref(operation) } }).await {
        Outcome::Ok { data: Data::Governance(result) } => match *result {
            GovernanceResult::Decision(record) => record,
            other => panic!("completion decision expected: {other:?}"),
        },
        other => panic!("completion audit expected: {other:?}"),
    }
}
fn completion_state(record: &DecisionRecord) -> CompletionDecision {
    match &record.detail {
        DecisionDetail::Completion(state) => state.clone(),
        other => panic!("completion detail expected: {other:?}"),
    }
}
async fn evaluated(c: &Controller, actor: &Caller, index: u32, blocked: bool) -> (Operation, Reply, CompletionDecision) {
    let operation = evaluation(actor, index);
    let value = reply(c, operation.clone()).await;
    let record = audit(c, &operation).await;
    assert_eq!(record.mode, DecisionMode::Enforcing);
    assert_eq!(record.id, decision_ref(&operation));
    assert_eq!(record.controller_revision, value.commit.as_ref().unwrap().controller_revision);
    assert!(matches!(&record.actor.provenance, ActorProvenance::BoundAgentSession {
        task_id, attempt_id, generation, worker_id, incarnation, ..
    } if task_id == &actor.task_id && attempt_id == &actor.attempt_id
        && generation == &actor.generation && worker_id == &actor.worker_id && incarnation == &actor.incarnation));
    let state = completion_state(&record);
    if blocked {
        assert_eq!(code(value.outcome.clone()), "governance_completion_blocked");
        assert_eq!(record.actual_outcome, ActualDecisionOutcome::GovernanceBlocked);
        assert!(matches!(state.disposition, CompletionDisposition::Blocked { .. }));
        assert_eq!(get(c, &actor.task_id).await.blocked_reason,
            Some(format!("governance_completion_blocked:{}", record.id.operation_id)));
    } else {
        assert_eq!(record.actual_outcome, ActualDecisionOutcome::Applied);
        assert_eq!(state.disposition, CompletionDisposition::Admit);
        assert!(state.reasons.is_empty());
        match &value.outcome {
            Outcome::Ok { data: Data::Completion(CompletionOutcome::Evaluated(actual)) } => {
                assert_eq!(actual.decision, record.id); assert_eq!(actual.state, state);
                assert_eq!(actual.boundary, CompletionBoundary::NativeResponse { request_index: index });
            },
            other => panic!("admitted original completion expected: {other:?}"),
        }
    }
    (operation, value, state)
}
fn pose_input(task_id: &str) -> PoseQuestion {
    PoseQuestion { record: RecordRef::Task { task_id: task_id.into() },
        recipient: PrincipalRef::TrustedHostOperator, prompt: "Choose the existing option".into(),
        options: vec![
            QuestionOption { id: "keep".into(), label: "Keep".into(), explanation: "Keep the existing choice".into() },
            QuestionOption { id: "change".into(), label: "Change".into(), explanation: "Use the alternative".into() },
        ], recommended_option_id: "keep".into() }
}
async fn tool(c: &Controller, actor: &Caller, name: &str, arguments: serde_json::Value) -> Reply {
    let occurrence = id("tool");
    reply(c, Invocation { operation_id: occurrence.clone(), logical_call_id: format!("logical-{occurrence}"),
        caller: actor.clone(), tool: name.into(), arguments }.operation()).await
}
async fn pose(c: &Controller, actor: &Caller) -> Question {
    let value = tool(c, actor, "collab_question_pose", json!({"question":pose_input(&actor.task_id)})).await;
    match value.outcome {
        Outcome::Ok { data: Data::QuestionMutation(value) } => {
            assert!(matches!(&value.question.question.poser.provenance,
                ActorProvenance::BoundAgentSession { task_id, .. } if task_id == &actor.task_id));
            value.question.question
        },
        other => panic!("original caller question expected: {other:?}"),
    }
}
async fn events(c: &Controller) -> Vec<WorkflowActivity> {
    let mut all = Vec::new(); let mut cursor = None;
    for _ in 0..32 {
        match call(c, Operation::QueryWorkflowActivity { cursor: cursor.take(), limit: 8 }).await {
            Outcome::Ok { data: Data::WorkflowActivity(page) } => {
                all.extend(page.items); cursor = page.next_cursor;
                if cursor.is_none() { return all; }
            },
            other => panic!("canonical workflow activity expected: {other:?}"),
        }
    }
    panic!("finite completion fixture exceeded its event read bound")
}
async fn close_question(c: &Controller, question: &Question, self_actor: Option<&Caller>) -> WorkflowActivity {
    let reason = "Original completion input";
    let value = if let Some(actor) = self_actor {
        tool(c, actor, "collab_question_cancel", json!({"question_id":question.id,
            "expected_revision":question.revision,"reason":reason})).await
    } else {
        reply(c, Operation::MutateQuestion { operation_id: id("question-close"), mutation: QuestionMutation::Cancel {
            question_id: question.id.clone(), expected_revision: question.revision, reason: reason.into(),
        }}).await
    };
    assert!(matches!(value.outcome, Outcome::Ok { data: Data::QuestionMutation(_) }), "{value:?}");
    let mut terminal = events(c).await.into_iter().filter(|event|
        event.subject == (RecordRef::Question { question_id: question.id.clone() })
            && matches!(&event.detail, ActivityDetail::Question(QuestionChange::Closed { .. })));
    let event = terminal.next().expect("one original terminal occurrence");
    assert!(terminal.next().is_none());
    event
}
fn has_input(state: &CompletionDecision, event: &EventId) -> bool {
    state.reasons.iter().any(|reason| matches!(reason, CompletionReason::UnconsumedInput { event_id } if event_id == event))
}
fn delivery(value: Reply) -> DeliveryObservation {
    match value.outcome {
        Outcome::Ok { data: Data::CollaborationDelivery(view) } => view,
        other => panic!("delivery observation expected: {other:?}"),
    }
}
async fn observe(c: &Controller, actor: &Caller) -> DeliveryObservation {
    delivery(reply(c, Operation::ObserveCollaborationDelivery { caller: actor.clone(), limit: 8 }).await)
}
async fn prepare(c: &Controller, actor: &Caller, view: &DeliveryObservation) -> DeliveryBatch {
    delivery(reply(c, Operation::PrepareCollaborationDelivery {
        operation_id: id("prepare"), caller: actor.clone(), expected_cursor: view.acknowledged_cursor.clone(),
        expected_routing_cursor: view.routing_cursor.clone(),
        through: view.available.as_ref().expect("finite available page").through.clone(), limit: 8,
    }).await).pending.expect("original prepared batch")
}
// The ordinary scan can expose an empty prefix before the independent routed
// page. Commit only those empty scan advances; no input or Begin is invented.
async fn observe_required_input(c: &Controller, actor: &Caller) -> DeliveryObservation {
    for _ in 0..c.limits().workflow.as_ref().unwrap().delivery_scan_rows {
        let observed = observe(c, actor).await;
        assert!(observed.pending.is_none(), "the fixture must not replace an existing input");
        let page = observed.available.as_ref().expect("required input remains in the canonical inventory");
        if page.has_input() { return observed; }
        let batch = prepare(c, actor, &observed).await;
        assert!(!batch.page.has_input());
        assert!(batch.delivery.is_none());
        let advanced = delivery(ack_delivery(c, actor, &batch, "fixture-empty-prefix").await);
        assert!(advanced.pending.is_none());
        assert_ne!((advanced.acknowledged_cursor, advanced.routing_cursor),
            (observed.acknowledged_cursor, observed.routing_cursor),
            "an empty scan must advance a cursor, not claim executor input");
    }
    panic!("bounded fixture scan did not reach the required input");
}
fn batch_has(batch: &DeliveryBatch, event: &EventId) -> bool {
    batch.page.items.iter().any(|item| &item.id == event)
        || batch.page.routing.as_ref().is_some_and(|routing| routing.items.iter().any(|item| &item.id == event))
}
async fn begin_delivery(c: &Controller, actor: &Caller, batch: &DeliveryBatch, context: &str) -> Reply {
    reply(c, Operation::BeginCollaborationDelivery { operation_id: id("delivery-begin"),
        caller: actor.clone(), batch_id: batch.batch_id.clone(), context_binding: context.into() }).await
}
async fn ack_delivery(c: &Controller, actor: &Caller, batch: &DeliveryBatch, context: &str) -> Reply {
    reply(c, Operation::AcknowledgeCollaborationDelivery { operation_id: id("delivery-ack"),
        caller: actor.clone(), batch_id: batch.batch_id.clone(), context_binding: context.into() }).await
}
fn begin_completion(actor: &Caller, decision: &DecisionRef, next: u32, batch: Option<String>) -> Operation {
    Operation::BeginCompletionContinuation(BeginCompletionContinuation {
        operation_id: begin_id(actor, decision).unwrap(), caller: actor.clone(), decision: decision.clone(),
        context_binding: id("input-context"), target: CompletionTarget::NativeRequest { request_index: next },
        delivery_batch_id: batch,
    })
}

// Read only after the actual Controller owner releases its SQLite lock. The
// projection includes the original receipt/audit and every domain touched by
// these fixtures; no successful policy, shadow or input state is fabricated.
fn durable(path: &std::path::Path) -> BTreeMap<String, Vec<String>> {
    let db = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut result = BTreeMap::new();
    for (name, sql) in [
        ("tasks", "SELECT json_array(id,status,active,body) FROM tasks ORDER BY id"),
        ("attempts", "SELECT json_array(id,body,model_requests) FROM attempts ORDER BY id"),
        ("history", "SELECT json_array(seq,task_id,revision,kind,body) FROM history ORDER BY seq"),
        ("receipts", "SELECT json_array(id,request,outcome,cleanup,controller_revision,governance) FROM receipts ORDER BY id"),
        ("counters", "SELECT json_array(kind,value) FROM counters ORDER BY kind"),
        ("settings", "SELECT json_array(name,value) FROM settings ORDER BY name"),
        ("questions", "SELECT json_array(id,body) FROM workflow_questions ORDER BY id"),
        ("question_actions", "SELECT json_array(question_id,question_revision,body) FROM workflow_question_actions ORDER BY question_id,question_revision"),
        ("work", "SELECT json_array(task_id,revision,body) FROM workflow_task_state ORDER BY task_id"),
        ("waits", "SELECT json_array(id,task_id,resolver_kind,resolver_id,closed,body) FROM workflow_waits ORDER BY id"),
        ("input", "SELECT json_array(wait_id,task_id,attempt_id,body,input_ack) FROM workflow_wait_consumptions ORDER BY wait_id,attempt_id"),
        ("events", "SELECT json_array(product_revision,ordinal,body,targets) FROM workflow_events ORDER BY product_revision,ordinal"),
        ("sessions", "SELECT json_array(id,body,workflow_delivery) FROM sessions ORDER BY id"),
        ("commands", "SELECT json_array(id,acknowledged,body,delivery_operation_id) FROM commands ORDER BY id"),
    ] {
        result.insert(name.into(), db.prepare(sql).unwrap().query_map([], |row| row.get::<_, String>(0)).unwrap()
            .collect::<Result<Vec<_>, _>>().unwrap());
    }
    result
}
fn stored_hold(path: &std::path::Path, task_id: &str) -> Option<CompletionHold> {
    let db = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let body: String = db.query_row("SELECT body FROM tasks WHERE id=?1", [task_id], |row| row.get(0)).unwrap();
    let value: serde_json::Value = serde_json::from_str(&body).unwrap();
    value.get("completion_hold").filter(|value| !value.is_null())
        .map(|value| serde_json::from_value(value.clone()).unwrap())
}
pub(super) fn stored_completion_reason(path: &std::path::Path, task_id: &str) -> Option<String> {
    let db = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let raw: String = db.query_row("SELECT body FROM tasks WHERE id=?1", [task_id], |row| row.get(0)).unwrap();
    let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
    serde_json::from_value(value["task"]["blocked_reason"].clone()).unwrap()
}

#[tokio::test]
async fn absent_or_unpromoted_policy_returns_not_enforced_without_hold_or_audit() {
    for selected in [false, true] {
        let p = path(); let mut cfg = governance_config();
        if !selected { Arc::make_mut(&mut cfg.limits).governance = None; }
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        let (before, _a, actor, _w) = active(&c, "not-enforced").await;
        let operation = evaluation(&actor, 1);
        let original = reply(&c, operation.clone()).await;
        assert_eq!(original.outcome, Outcome::Ok { data: Data::Completion(CompletionOutcome::NotEnforced) });
        assert!(original.commit.is_some());
        assert_eq!(get(&c, &before.id).await, before, "no synthetic completion hold or task revision");
        same_original(&reply(&c, operation.clone()).await, &original);
        drop(c); controller_owner_released(&p).await;
        assert!(stored_hold(&p, &before.id).is_none());
        let db = rusqlite::Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let audit: Option<String> = db.query_row("SELECT governance FROM receipts WHERE id=?1",
            [&decision_ref(&operation).operation_id], |row| row.get(0)).unwrap();
        assert!(audit.is_none()); drop(db);
        let c = Controller::open_with_config(&p, cfg).unwrap();
        same_original(&reply(&c, operation).await, &original);
        assert_eq!(get(&c, &before.id).await, before);
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
//= type=test
//# A voluntary completion decision MUST evaluate the current canonical required work for its task.

// Traceability scope: Optional questions do not block; an explicit required wait fences the live completion caller without authorizing a new Run.
//= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
//= type=test
//# A voluntary completion decision MUST refuse completion while required work remains unfinished.

async fn optional_unanswered_question_admits_but_explicit_wait_fences_the_live_caller() {
    let c = Controller::open_with_config(path(), governance_config()).unwrap();
    promote(&c, "optional-policy", true).await;
    let (before, a, actor, w) = active(&c, "optional").await;
    let question = pose(&c, &actor).await;
    let (_, _, admitted) = evaluated(&c, &actor, 1, false).await;
    assert_eq!(admitted.consecutive_count, 0);
    reserve(&c, &a, 2).await;
    let waiting = match call(&c, Operation::MutateWork { operation_id: id("explicit-wait"), mutation: WorkMutation::AddWait {
        task_id: before.id.clone(), expected_revision: 0, resolver: ResolverRef::Question { question_id: question.id.clone() },
    }}).await {
        Outcome::Ok { data: Data::WorkMutation(value) } => value.view,
        other => panic!("actual explicit wait expected: {other:?}"),
    };
    assert_eq!(waiting.open_waits, 1); assert_eq!(waiting.task.status, TaskStatus::Queued);
    let current = waiting.task.attempt.as_ref().unwrap();
    assert_eq!(current.phase, AttemptPhase::StopRequested); assert!(!current.stopped);
    assert!(!current.uncertain); assert_eq!(waiting.task.input, before.input);
    let projected = get(&c, &before.id).await;
    let mut expected_projection = waiting.task.clone();
    expected_projection.blocked_reason = Some("prior_execution_active".into());
    assert_eq!(projected, expected_projection, "GetTask projects the active admission fence without changing the wait");
    let operation = evaluation(&actor, 2);
    let refused = reply(&c, operation.clone()).await;
    assert_eq!(code(refused.outcome), "invalid_transition");
    assert_eq!(get(&c, &before.id).await, projected);
    assert_eq!(code(call(&c, Operation::QueryGovernance { query: GovernanceQuery::Decision {
        decision: decision_ref(&operation),
    }}).await), "not_found", "the existing caller fence precedes governance assessment");
    assert!(!commands(&c, &w, vec![]).await.iter().any(|command|
        matches!(command, WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == before.id && attempt.id != a.id)));
}

#[tokio::test]
//= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
//= type=test
//# A voluntary completion decision MUST refuse completion while an applicable required input remains unconsumed.

// Traceability scope: Canonical required input blocks completion through Observe, Prepare and Begin; exact ACK alone discharges that input.
//= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
//= type=test
//# A voluntary completion decision MUST refuse completion while required work remains unfinished.

async fn exact_terminal_input_is_required_through_observe_prepare_and_begin_until_ack() {
    let mut cfg = governance_config();
    Arc::make_mut(&mut cfg.limits).governance.as_mut().unwrap().completion_continuations = 6;
    let c = Controller::open_with_config(path(), cfg).unwrap();
    promote(&c, "input-policy", true).await;
    let (before, a, actor, _w) = active(&c, "input").await;
    let question = pose(&c, &actor).await;
    let terminal = close_question(&c, &question, None).await;
    let observed = observe_required_input(&c, &actor).await;
    let (_, _, first) = evaluated(&c, &actor, 1, true).await;
    assert!(has_input(&first, &terminal.id)); assert_eq!(first.consecutive_count, 1);
    let batch = prepare(&c, &actor, &observed).await;
    assert!(batch_has(&batch, &terminal.id)); assert!(batch.delivery.is_none());
    assert_eq!(observe(&c, &actor).await.acknowledged_cursor, observed.acknowledged_cursor);
    reserve(&c, &a, 2).await;
    let (_, _, prepared) = evaluated(&c, &actor, 2, true).await;
    assert!(has_input(&prepared, &terminal.id)); assert_eq!(prepared.consecutive_count, 2);
    let context = id("actual-input");
    let begun = delivery(begin_delivery(&c, &actor, &batch, &context).await);
    assert!(begun.pending.as_ref().unwrap().delivery.is_some());
    assert_eq!(begun.acknowledged_cursor, observed.acknowledged_cursor);
    reserve(&c, &a, 3).await;
    let (_, _, unknown) = evaluated(&c, &actor, 3, true).await;
    assert!(has_input(&unknown, &terminal.id));
    assert!(unknown.reasons.contains(&CompletionReason::EvaluationUnavailable { code: CompletionFailure::Unavailable }));
    assert!(matches!(unknown.disposition, CompletionDisposition::Blocked { continuation_eligible: false, exhausted: false }));
    let acknowledged = delivery(ack_delivery(&c, &actor, &batch, &context).await);
    assert!(acknowledged.pending.is_none());
    reserve(&c, &a, 4).await;
    let (_, _, admitted) = evaluated(&c, &actor, 4, false).await;
    assert_eq!(admitted.consecutive_count, unknown.consecutive_count, "input ACK is not an independent reset");
    assert!(admitted.independent_reset_event.is_none());

    // A later independent outcome takes the paired completion-input path.
    // Both ACK operations bind the same original batch/context and a genuine
    // next-response reservation; Begin alone did not acknowledge either input.
    let later_question = pose(&c, &actor).await;
    let later_terminal = close_question(&c, &later_question, None).await;
    reserve(&c, &a, 5).await;
    let (later_decision, _, later_state) = evaluated(&c, &actor, 5, true).await;
    assert_eq!(later_state.independent_reset_event, Some(later_terminal.id.clone()));
    let later_view = observe_required_input(&c, &actor).await;
    let later_batch = prepare(&c, &actor, &later_view).await;
    assert!(batch_has(&later_batch, &later_terminal.id));
    let begin = begin_completion(&actor, &decision_ref(&later_decision), 6, Some(later_batch.batch_id.clone()));
    let begun = reply(&c, begin.clone()).await;
    let continuation = match &begun.outcome {
        Outcome::Ok { data: Data::Completion(CompletionOutcome::Begun(feedback)) } => feedback.continuation.clone(),
        other => panic!("paired completion input expected: {other:?}"),
    };
    assert!(continuation.acknowledged.is_none());
    let c_begin = delivery(begin_delivery(&c, &actor, &later_batch, &continuation.context_binding).await);
    assert!(c_begin.pending.unwrap().delivery.is_some());
    reserve(&c, &a, 6).await;
    let workflow_ack = ack_delivery(&c, &actor, &later_batch, &continuation.context_binding).await;
    let workflow_ack_id = workflow_ack.commit.as_ref().unwrap().operation_id.clone();
    assert!(delivery(workflow_ack).pending.is_none());
    let consumed_at = CompletionBoundary::NativeResponse { request_index: 6 };
    let acknowledgement = Operation::AcknowledgeCompletionInput(AcknowledgeCompletionInput {
        operation_id: acknowledgement_id(&actor, &continuation.begin_operation_id).unwrap(),
        caller: actor.clone(), begin_operation_id: continuation.begin_operation_id.clone(), consumed_at: consumed_at.clone(),
        delivery_ack_operation_id: Some(workflow_ack_id.clone()),
        native_input: Some(NativeCompletionInput { request_binding: completion_binding(), rejections: vec![] }),
    });
    let acknowledged = reply(&c, acknowledgement.clone()).await;
    let expected_ack = CompletionInputAck {
        operation_id: acknowledgement_id(&actor, &continuation.begin_operation_id).unwrap(), consumed_at,
        delivery_ack_operation_id: Some(workflow_ack_id),
        native_input: Some(NativeCompletionInput { request_binding: completion_binding(), rejections: vec![] }),
    };
    let mut expected = continuation; expected.acknowledged = Some(expected_ack);
    assert_eq!(acknowledged.outcome, Outcome::Ok { data: Data::Completion(CompletionOutcome::InputAcknowledged(expected)) });
    same_original(&reply(&c, acknowledgement).await, &acknowledged);
    same_original(&reply(&c, begin).await, &begun);
    let (_, _, consumed) = evaluated(&c, &actor, 6, false).await;
    assert_eq!(consumed.consecutive_count, later_state.consecutive_count);
    assert!(consumed.independent_reset_event.is_none());
    let after = get(&c, &before.id).await;
    assert_eq!(after.input, before.input); assert_eq!(after.status, TaskStatus::Active);
    assert_eq!(after.attempt.as_ref().unwrap().id, a.id); assert!(!after.attempt.unwrap().stopped);
}

#[cfg(debug_assertions)]
#[tokio::test]
//= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
//= type=test
//# A refused voluntary completion MUST record a bounded reason identifying the remaining required work or input.
async fn commit_cut_rolls_back_original_refusal_hold_and_audit_then_same_identity_commits_once() {
    let p = path(); let failed_commit = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut cfg = governance_config(); cfg.qualification_fail_next_commit = Some(failed_commit.clone());
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    promote(&c, "atomic-policy", true).await;
    let (before, _a, actor, _w) = active(&c, "atomic").await;
    let question = pose(&c, &actor).await;
    let terminal = close_question(&c, &question, None).await;
    let observed = observe(&c, &actor).await;
    let prepared = prepare(&c, &actor, &observed).await;
    let operation = evaluation(&actor, 1);
    drop(c); controller_owner_released(&p).await;
    let initial = durable(&p); assert!(stored_hold(&p, &before.id).is_none());
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    failed_commit.store(true, Ordering::SeqCst);
    let failed = reply(&c, operation.clone()).await;
    assert!(!failed_commit.load(Ordering::SeqCst), "real COMMIT cut was consumed");
    assert_eq!(code(failed.outcome), "store_unavailable"); assert!(failed.commit.is_none());
    assert_eq!(get(&c, &before.id).await, before);
    assert_eq!(observe(&c, &actor).await.pending, Some(prepared));
    assert_eq!(code(call(&c, Operation::GetOperation { operation_id: decision_ref(&operation).operation_id.clone() }).await), "not_found");
    assert_eq!(code(call(&c, Operation::QueryGovernance { query: GovernanceQuery::Decision { decision: decision_ref(&operation) } }).await), "not_found");
    drop(c); controller_owner_released(&p).await;
    assert_eq!(durable(&p), initial, "failed receipt transaction leaves every projected row exact");
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let (retried, original, state) = evaluated(&c, &actor, 1, true).await;
    assert_eq!(serde_json::to_value(&retried).unwrap(), serde_json::to_value(&operation).unwrap());
    assert_eq!(state.consecutive_count, 1); assert!(has_input(&state, &terminal.id));
    let committed_task = get(&c, &before.id).await;
    assert_eq!(committed_task.revision, before.revision + 1);
    let record = audit(&c, &operation).await;
    for _ in 0..2 { same_original(&reply(&c, operation.clone()).await, &original); }
    assert_eq!(get(&c, &before.id).await, committed_task);
    drop(c); controller_owner_released(&p).await;
    let hold = stored_hold(&p, &before.id).unwrap();
    assert_eq!(hold.decision, record.id); assert_eq!(hold.state, state); assert!(hold.continuation.is_none());
    assert_eq!(Some(hold.observed_product_revision), record.observed_product_revision);
    let committed = durable(&p);
    let c = Controller::open_with_config(&p, cfg).unwrap();
    same_original(&reply(&c, operation.clone()).await, &original);
    assert_eq!(audit(&c, &operation).await, record);
    drop(c); controller_owner_released(&p).await;
    assert_eq!(durable(&p), committed, "reopen and original replay create no duplicate hold, count or audit");
}

#[tokio::test]
//= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
//= type=test
//# Consecutive continuations caused by refused voluntary completion MUST be bounded by versioned metadata.
//= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
//= type=test
//# The consecutive blocked-completion count MUST reset only on an independently originated event that changes the required work or applicable inputs.
//= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
//= type=test
//# Exhausting the blocked-completion bound MUST leave the task visibly blocked without authorizing another model turn.
async fn bounded_count_ignores_self_and_unrelated_events_and_resets_once_for_independent_required_input() {
    let p = path(); let cfg = governance_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    promote(&c, "count-policy", true).await;
    let (_before, a, actor, _w) = active(&c, "count").await;
    let first_question = pose(&c, &actor).await;
    let first_event = close_question(&c, &first_question, Some(&actor)).await;
    let (_, _, first) = evaluated(&c, &actor, 1, true).await;
    assert_eq!(first.consecutive_count, 1); assert!(has_input(&first, &first_event.id));
    reserve(&c, &a, 2).await;
    let (_, _, second) = evaluated(&c, &actor, 2, true).await;
    assert_eq!(second.consecutive_count, 2); assert!(second.independent_reset_event.is_none());
    task(report(&c, &a, &id("self-progress"), Observation::Progress {
        text: "Self progress is not resolved required work".into(), output: String::new(), truncated: false,
    }).await);
    let self_question = pose(&c, &actor).await;
    let self_event = close_question(&c, &self_question, Some(&actor)).await;
    reserve(&c, &a, 3).await;
    let (exhausted_op, _, exhausted) = evaluated(&c, &actor, 3, true).await;
    assert!(has_input(&exhausted, &self_event.id)); assert_eq!(exhausted.consecutive_count, 3);
    assert!(exhausted.independent_reset_event.is_none());
    assert_eq!(exhausted.disposition, CompletionDisposition::Blocked { continuation_eligible: false, exhausted: true });
    assert_eq!(code(reply(&c, begin_completion(&actor, &decision_ref(&exhausted_op), 4, None)).await.outcome), "governance_completion_blocked");
    let unrelated = submit(&c, &id("unrelated-submit"), native(&id("unrelated-agent"))).await;
    task(call(&c, Operation::CancelTask { operation_id: id("unrelated-cancel"),
        task_id: unrelated.id, expected_revision: unrelated.revision }).await);
    reserve(&c, &a, 4).await;
    let (still_op, still_reply, still_exhausted) = evaluated(&c, &actor, 4, true).await;
    assert_eq!(still_exhausted.consecutive_count, 3); assert!(still_exhausted.independent_reset_event.is_none());
    let before_restart = get(&c, &a.task_id).await;
    drop(c); controller_owner_released(&p).await;
    assert_eq!(stored_hold(&p, &a.task_id).unwrap().state, still_exhausted);
    let retained = durable(&p);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    same_original(&reply(&c, still_op).await, &still_reply);
    assert_eq!(get(&c, &a.task_id).await, before_restart);
    drop(c); controller_owner_released(&p).await;
    assert_eq!(durable(&p), retained, "restart and original replay do not reset the exhausted count");
    let c = Controller::open_with_config(&p, cfg).unwrap();
    let independent_question = pose(&c, &actor).await;
    let independent = close_question(&c, &independent_question, None).await;
    reserve(&c, &a, 5).await;
    let (reset_op, reset_reply, reset) = evaluated(&c, &actor, 5, true).await;
    assert_eq!(reset.consecutive_count, 1); assert_eq!(reset.independent_reset_event, Some(independent.id));
    assert_eq!(reset.disposition, CompletionDisposition::Blocked { continuation_eligible: true, exhausted: false });
    same_original(&reply(&c, reset_op).await, &reset_reply);
    reserve(&c, &a, 6).await;
    let (_, _, next) = evaluated(&c, &actor, 6, true).await;
    assert_eq!(next.consecutive_count, 2); assert!(next.independent_reset_event.is_none());
}

#[tokio::test]
//= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
//= type=test
//# Failure of the completion check MUST leave the task visibly blocked without authorizing another model turn.
async fn reason_and_scan_bound_failures_commit_a_visible_typed_block_without_continuation() {
    for scan_overflow in [false, true] {
        let p = path(); let mut cfg = governance_config();
        let bounds = Arc::make_mut(&mut cfg.limits).governance.as_mut().unwrap();
        if scan_overflow { bounds.scan_rows = 32; } else { bounds.reason_items = 1; }
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        promote(&c, "failure-policy", true).await;
        let (before, _a, actor, _w) = active(&c, "failure").await;
        let first = pose(&c, &actor).await; close_question(&c, &first, None).await;
        if scan_overflow {
            // Only one required terminal input, followed by advisory questions:
            // failure proves the complete scan bound, not reason_items overflow.
            for _ in 0..33 { pose(&c, &actor).await; }
        } else {
            let second = pose(&c, &actor).await; close_question(&c, &second, None).await;
        }
        let (operation, original, state) = evaluated(&c, &actor, 1, true).await;
        assert_eq!(state.reasons, vec![CompletionReason::EvaluationUnavailable { code: CompletionFailure::BoundExceeded }]);
        assert_eq!(state.consecutive_count, 1);
        assert_eq!(state.disposition, CompletionDisposition::Blocked { continuation_eligible: false, exhausted: false });
        let bound = cfg.limits.governance.as_ref().unwrap();
        assert!(state.reasons.len() <= bound.reason_items);
        assert!(serde_json::to_vec(&state.reasons).unwrap().len() <= bound.reason_bytes);
        assert_eq!(code(reply(&c, begin_completion(&actor, &decision_ref(&operation), 2, None)).await.outcome), "governance_completion_blocked");
        same_original(&reply(&c, operation.clone()).await, &original);
        let current = get(&c, &before.id).await;
        assert_eq!(current.input, before.input); assert_eq!(current.status, TaskStatus::Active);
        drop(c); controller_owner_released(&p).await;
        let hold = stored_hold(&p, &before.id).unwrap();
        assert_eq!(hold.state, state); assert!(hold.continuation.is_none());
        let c = Controller::open_with_config(&p, cfg).unwrap();
        same_original(&reply(&c, operation).await, &original);
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
//= type=test
//# An authorized cancellation or emergency stop MUST NOT wait for voluntary completion admission.
async fn unknown_begin_allows_only_original_input_and_operator_cancellation_bypasses_the_hold() {
    let p = path(); let cfg = governance_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    promote(&c, "unknown-policy", true).await;
    let (before, a, actor, _w) = active(&c, "unknown").await;
    let question = pose(&c, &actor).await; close_question(&c, &question, None).await;
    let (original_evaluation, _, state) = evaluated(&c, &actor, 1, true).await;
    assert_eq!(state.disposition, CompletionDisposition::Blocked { continuation_eligible: true, exhausted: false });
    let observed = observe_required_input(&c, &actor).await;
    let batch = prepare(&c, &actor, &observed).await;
    let begin = begin_completion(&actor, &decision_ref(&original_evaluation), 2, Some(batch.batch_id.clone()));
    let original = reply(&c, begin.clone()).await;
    let feedback = match &original.outcome {
        Outcome::Ok { data: Data::Completion(CompletionOutcome::Begun(feedback)) } => feedback.clone(),
        other => panic!("original retained continuation expected: {other:?}"),
    };
    assert!(feedback.continuation.acknowledged.is_none());
    assert_eq!(feedback.evaluation.state, state);
    same_original(&reply(&c, begin.clone()).await, &original);
    let mut changed_context = begin.clone();
    let Operation::BeginCompletionContinuation(value) = &mut changed_context else { unreachable!() };
    value.context_binding = id("substituted-context");
    assert_eq!(code(reply(&c, changed_context).await.outcome), "operation_conflict");
    let mut second_identity = begin.clone();
    let Operation::BeginCompletionContinuation(value) = &mut second_identity else { unreachable!() };
    value.operation_id = id("second-begin");
    assert_eq!(code(reply(&c, second_identity).await.outcome), "operation_conflict");
    assert_eq!(code(begin_delivery(&c, &actor, &batch, "unrelated-context").await.outcome), "delivery_pending");
    let exact_context = &feedback.continuation.context_binding;
    let delivery_begin = delivery(begin_delivery(&c, &actor, &batch, exact_context).await);
    assert!(delivery_begin.pending.unwrap().delivery.is_some());
    // A genuine reservation for the next response is still not its ACK.
    reserve(&c, &a, 2).await;
    let next = evaluation(&actor, 2);
    assert_eq!(code(reply(&c, next.clone()).await.outcome), "delivery_pending");
    assert_eq!(code(call(&c, Operation::QueryGovernance { query: GovernanceQuery::Decision { decision: decision_ref(&next) } }).await), "not_found");
    let held_task = get(&c, &before.id).await;
    let cancelled = task(call(&c, Operation::CancelTask { operation_id: id("cancel-held"),
        task_id: before.id.clone(), expected_revision: held_task.revision }).await);
    assert_eq!(cancelled.status, TaskStatus::Cancelled); assert_eq!(cancelled.input, before.input);
    let stopped = cancelled.attempt.as_ref().unwrap();
    assert_eq!(stopped.id, a.id); assert_eq!(stopped.phase, AttemptPhase::StopRequested);
    assert!(!stopped.stopped); assert!(!stopped.uncertain);
    same_original(&reply(&c, begin.clone()).await, &original);
    drop(c); controller_owner_released(&p).await;
    let hold = stored_hold(&p, &before.id).unwrap();
    assert_eq!(hold.state, state); assert_eq!(hold.continuation, Some(feedback.continuation));
    let c = Controller::open_with_config(&p, cfg).unwrap();
    same_original(&reply(&c, begin).await, &original);
    assert_eq!(get(&c, &before.id).await, cancelled);
}

#[tokio::test]
async fn original_receipt_survives_stale_caller_but_new_boundary_and_rebound_identity_are_refused() {
    let c = Controller::open_with_config(path(), governance_config()).unwrap();
    promote(&c, "stale-policy", true).await;
    let (before, a, actor, _w) = active(&c, "stale").await;
    let question = pose(&c, &actor).await; close_question(&c, &question, None).await;
    let (operation, original, state) = evaluated(&c, &actor, 1, true).await;
    reserve(&c, &a, 2).await;
    let mut stale = actor.clone(); stale.incarnation = "other-incarnation".into();
    let stale_operation = evaluation(&stale, 2);
    assert_eq!(code(reply(&c, stale_operation.clone()).await.outcome), "stale_owner");
    assert_eq!(code(call(&c, Operation::QueryGovernance { query: GovernanceQuery::Decision { decision: decision_ref(&stale_operation) } }).await), "not_found");
    let mut changed_body = operation.clone();
    let Operation::EvaluateCompletion(value) = &mut changed_body else { unreachable!() };
    value.caller = stale;
    assert_eq!(code(reply(&c, changed_body).await.outcome), "operation_conflict");
    let current = get(&c, &before.id).await;
    task(call(&c, Operation::CancelTask { operation_id: id("stale-cancel"), task_id: before.id.clone(), expected_revision: current.revision }).await);
    task(report(&c, &a, &id("stale-stopped"), Observation::ProcessTreeStopped).await);
    let terminal = get(&c, &before.id).await;
    same_original(&reply(&c, operation.clone()).await, &original);
    assert_eq!(completion_state(&audit(&c, &operation).await), state);
    assert_eq!(code(reply(&c, evaluation(&actor, 2)).await.outcome), "invalid_transition");
    assert_eq!(get(&c, &before.id).await, terminal);
}

// Successor coverage: actual stop/admission and exact consuming request proof.
fn completion_binding() -> borg_runtime::native::ProviderRequestBinding {
    // SHA-256 of the two controlled serialized request bytes `{}`. This is a
    // host observation fixture, not a claim that a provider request was run.
    borg_runtime::native::ProviderRequestBinding { bytes: 2,
        sha256: "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a".into() }
}
fn completion_report(a: &Attempt, report_id: String, observation: Observation) -> Operation {
    Operation::ObserveWorker { report: Report { report_id, task_id: a.task_id.clone(),
        attempt_id: a.id.clone(), generation: a.generation, worker_id: a.worker_id.clone(),
        incarnation: a.worker_incarnation.clone(), observation } }
}
async fn host_question(c: &Controller, task_id: &str) -> Question {
    match call(c, Operation::MutateQuestion { operation_id: id("host-question"),
        mutation: QuestionMutation::Pose { question: pose_input(task_id) } }).await {
        Outcome::Ok { data: Data::QuestionMutation(value) } => value.question.question,
        other => panic!("original host question expected: {other:?}"),
    }
}
async fn required_question(c: &Controller, task_id: &str) -> (Question, WaitRef) {
    let question = host_question(c, task_id).await;
    let view = match call(c, Operation::QueryWork { query: WorkQuery::Get { task_id: task_id.into() } }).await {
        Outcome::Ok { data: Data::WorkQuery(WorkQueryResult::Task(view)) } => view,
        other => panic!("coherent work view expected: {other:?}"),
    };
    let value = match call(c, Operation::MutateWork { operation_id: id("required-wait"),
        mutation: WorkMutation::AddWait { task_id: task_id.into(), expected_revision: view.coordination_revision,
            resolver: ResolverRef::Question { question_id: question.id.clone() } } }).await {
        Outcome::Ok { data: Data::WorkMutation(value) } => value,
        other => panic!("real required wait expected: {other:?}"),
    };
    assert_eq!(value.view.open_waits, 1);
    assert_eq!(value.view.task.blocked_reason.as_deref(), Some("workflow_wait"));
    (question, value.changed_wait.unwrap())
}
async fn completion_attempts(c: &Controller, task_id: &str) -> Vec<borg_runtime::collaboration::AttemptView> {
    use borg_runtime::collaboration::{Query, QueryResult};
    match call(c, Operation::QueryCollaboration { query: Query::TaskAttempts {
        task_id: task_id.into(), after: None, limit: 8,
    }}).await {
        Outcome::Ok { data: Data::Collaboration(QueryResult::TaskAttempts(page)) } => {
            assert!(page.next_cursor.is_none()); page.items
        },
        other => panic!("bounded original attempt history expected: {other:?}"),
    }
}
async fn no_completion_resume(c: &Controller, w: &Worker, task_id: &str, original_attempt: &str) {
    let issued = commands(c, w, vec![]).await;
    assert!(!issued.iter().any(|command| matches!(command,
        WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == task_id)), "{issued:?}");
    let attempts = completion_attempts(c, task_id).await;
    assert_eq!(attempts.len(), 1, "a hold cannot allocate a hidden successor attempt");
    assert_eq!(attempts[0].attempt.id, original_attempt);
}
fn completion_ack(actor: &Caller, begin: &CompletionContinuation, index: u32,
    native_input: Option<NativeCompletionInput>) -> Operation {
    Operation::AcknowledgeCompletionInput(AcknowledgeCompletionInput {
        operation_id: acknowledgement_id(actor, &begin.begin_operation_id).unwrap(), caller: actor.clone(),
        begin_operation_id: begin.begin_operation_id.clone(),
        consumed_at: CompletionBoundary::NativeResponse { request_index: index },
        delivery_ack_operation_id: None, native_input,
    })
}
async fn rejected_completion_request(c: &Controller, a: &Attempt, index: u32,
    kind: borg_runtime::native::RejectionKind,
    retry_after: borg_runtime::native::RetryAfter) -> NativeCompletionRejection {
    let operation_id = borg_runtime::native_loop_contract::rejection_id(&a.id, index);
    let accepted = get(c, &a.task_id).await;
    task(report(c, a, &operation_id, Observation::NativeRequestRejected {
        request_index: index, request_binding: completion_binding(),
        endpoint: "https://api.openai.com/v1/responses".into(),
        model: accepted.input.definition.required_inference().unwrap().model.clone(),
        rejection: borg_runtime::native::ProviderRejection {
            kind, retry_after,
        }, diagnostic: "controlled complete provider rejection".into(),
    }).await);
    NativeCompletionRejection { request_index: index, operation_id }
}

#[tokio::test]
async fn exhausted_physical_stop_resumes_only_for_original_independent_required_resolution() {
    let p = path(); let cfg = governance_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    promote(&c, "stopped-reset-policy", true).await;
    let (accepted, a, actor, w) = active(&c, "stopped-reset").await;
    let first = pose(&c, &actor).await;
    close_question(&c, &first, Some(&actor)).await;
    evaluated(&c, &actor, 1, true).await;
    reserve(&c, &a, 2).await; evaluated(&c, &actor, 2, true).await;
    reserve(&c, &a, 3).await;
    let (decision, original, exhausted) = evaluated(&c, &actor, 3, true).await;
    assert_eq!(exhausted.consecutive_count, 3);
    assert_eq!(exhausted.disposition, CompletionDisposition::Blocked { continuation_eligible: false, exhausted: true });
    let immutable_audit = audit(&c, &decision).await;
    // These later events cannot revive an exhausted task. The self terminal is
    // actually addressed to this task, so origin (not irrelevance) excludes it.
    let self_question = pose(&c, &actor).await;
    let self_terminal = close_question(&c, &self_question, Some(&actor)).await;
    task(report(&c, &a, &id("resume-self-progress"), Observation::Progress {
        text: "Own progress does not reset completion".into(), output: String::new(), truncated: false,
    }).await);
    let unrelated = submit(&c, &id("resume-unrelated"), native(&id("unrelated-owner"))).await;
    task(call(&c, Operation::CancelTask { operation_id: id("resume-unrelated-cancel"),
        task_id: unrelated.id, expected_revision: unrelated.revision }).await);
    let stop = completion_report(&a, id("exhausted-physical-stop"), Observation::ProcessTreeStopped);
    let stopped_reply = reply(&c, stop.clone()).await;
    let stopped = task(stopped_reply.outcome.clone());
    assert_eq!(stopped.status, TaskStatus::Queued);
    assert_eq!(stopped.blocked_reason, Some("governance_completion_blocked".into()),
        "queued-task view projects the current admission refusal");
    assert!(stopped.attempt.as_ref().unwrap().stopped);
    assert_eq!(stopped.attempt.as_ref().unwrap().phase, AttemptPhase::Stopped);
    assert!(!stopped.attempt.as_ref().unwrap().uncertain);
    no_completion_resume(&c, &w, &accepted.id, &a.id).await;
    drop(c); controller_owner_released(&p).await;
    let held = stored_hold(&p, &accepted.id).unwrap();
    assert_eq!(held.decision, decision_ref(&decision));
    assert_eq!(stored_completion_reason(&p, &accepted.id),
        Some(format!("governance_completion_blocked:{}", held.decision.operation_id)),
        "the original typed hold marker remains in canonical storage");
    assert_eq!(held.state, exhausted); assert!(held.continuation.is_none());
    assert!(self_terminal.id.product_revision > held.observed_product_revision);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    no_completion_resume(&c, &w, &accepted.id, &a.id).await;
    same_original(&reply(&c, stop.clone()).await, &stopped_reply);
    same_original(&reply(&c, decision.clone()).await, &original);

    let (required, wait) = required_question(&c, &accepted.id).await;
    no_completion_resume(&c, &w, &accepted.id, &a.id).await;
    let resolution = close_question(&c, &required, None).await;
    assert!(resolution.id.product_revision > held.observed_product_revision);
    assert!(matches!(resolution.actor.provenance, ActorProvenance::TrustedHost { path: TrustedHostPath::OperatorApi }));
    let view = match call(&c, Operation::QueryWork { query: WorkQuery::Get { task_id: accepted.id.clone() } }).await {
        Outcome::Ok { data: Data::WorkQuery(WorkQueryResult::Task(view)) } => view,
        other => panic!("resolved required wait expected: {other:?}"),
    };
    assert_eq!(view.open_waits, 0);
    assert_eq!(view.task.blocked_reason.as_deref(), Some("workflow_waits_ended"));
    let resolved = view.waits.items.iter().find(|item| item.reference == wait).unwrap();
    assert!(matches!(&resolved.status, WaitStatus::Resolved { resolution: exact, .. }
        if exact.event_id == resolution.id));
    drop(c); controller_owner_released(&p).await;
    assert_eq!(stored_hold(&p, &accepted.id).unwrap(), held,
        "the resolver does not rewrite the old completion decision, count or watermark");
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let admission = Operation::AwaitCommands { operation_id: id("reset-admission"),
        worker_id: w.id.clone(), incarnation: w.incarnation.clone(), acknowledged: vec![] };
    let issued = reply(&c, admission.clone()).await;
    let Outcome::Ok { data: Data::Commands(commands) } = &issued.outcome else { panic!("real reset admission expected: {issued:?}") };
    let (next, resume) = commands.iter().find_map(|command| match command {
        WorkerCommand::RunAttempt { attempt, task, session, completion_continuation: Some(resume), .. }
            if attempt.task_id == accepted.id => {
                assert_eq!(session.id, accepted.session_id);
                let mut remaining = accepted.input.clone(); remaining.limits.model_requests -= 3;
                assert_eq!(*task, remaining); Some((attempt.clone(), resume.clone()))
            },
        _ => None,
    }).expect("ordinary command carries its exact original completion reason");
    assert_ne!(next.id, a.id); assert!(next.generation > a.generation);
    assert_eq!(next.deadline_unix_ms, a.deadline_unix_ms);
    assert_eq!(resume.evaluation.decision, decision_ref(&decision));
    assert_eq!(resume.evaluation.state, exhausted);
    assert_eq!(resume.evaluation.boundary, CompletionBoundary::NativeResponse { request_index: 3 });
    assert_eq!(resume.reset_event, Some(resolution.id.clone()));
    assert_eq!(audit(&c, &decision).await, immutable_audit);
    same_original(&reply(&c, admission).await, &issued);
    let history = completion_attempts(&c, &accepted.id).await;
    assert_eq!(history.len(), 2);
    assert_eq!(history.iter().map(|item| item.reported_model_requests).sum::<u32>(), 3);
    assert!(history.iter().find(|item| item.attempt.id == a.id).unwrap().attempt.stopped);
    drop(c); controller_owner_released(&p).await;
    assert_eq!(stored_hold(&p, &accepted.id).unwrap(), held, "admission is neither reset nor input ACK");
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    task(report(&c, &next, &id("reset-started"), Observation::Started { thread_id: None, turn_id: None }).await);
    let new_actor = caller(&next);
    let reset_view = observe_required_input(&c, &new_actor).await;
    let reset_batch = prepare(&c, &new_actor, &reset_view).await;
    assert!(reset_batch.page.contains_event(&resolution.id));
    assert!(reset_batch.page.continuation.as_ref().unwrap().resolved_waits.iter()
        .any(|resolved| resolved.event_id == resolution.id));
    let begin = Operation::BeginCompletionContinuation(BeginCompletionContinuation {
        operation_id: begin_id(&new_actor, &resume.evaluation.decision).unwrap(), caller: new_actor.clone(),
        decision: resume.evaluation.decision.clone(), context_binding: id("reset-original-input"),
        target: CompletionTarget::InitialNativeRequest { request_index: 1 },
        delivery_batch_id: Some(reset_batch.batch_id.clone()),
    });
    let begun = reply(&c, begin.clone()).await;
    let continuation = match &begun.outcome {
        Outcome::Ok { data: Data::Completion(CompletionOutcome::Begun(value)) } => {
            assert_eq!(value.evaluation, resume.evaluation); value.continuation.clone()
        }, other => panic!("original reset witness permits exactly this initial input: {other:?}"),
    };
    assert!(continuation.acknowledged.is_none());
    assert_eq!(observe(&c, &new_actor).await.pending, Some(reset_batch.clone()));
    let delivery_begin = delivery(begin_delivery(&c, &new_actor, &reset_batch, &continuation.context_binding).await);
    assert!(delivery_begin.pending.as_ref().unwrap().delivery.is_some());
    reserve(&c, &next, 1).await;
    let workflow_ack = ack_delivery(&c, &new_actor, &reset_batch, &continuation.context_binding).await;
    let workflow_ack_id = workflow_ack.commit.as_ref().unwrap().operation_id.clone();
    assert!(delivery(workflow_ack).pending.is_none());
    let mut ack = completion_ack(&new_actor, &continuation, 1,
        Some(NativeCompletionInput { request_binding: completion_binding(), rejections: vec![] }));
    let Operation::AcknowledgeCompletionInput(value) = &mut ack else { unreachable!() };
    value.delivery_ack_operation_id = Some(workflow_ack_id);
    let acknowledged = reply(&c, ack.clone()).await;
    assert!(matches!(acknowledged.outcome, Outcome::Ok { data: Data::Completion(CompletionOutcome::InputAcknowledged(_)) }));
    assert_eq!(audit(&c, &decision).await, immutable_audit);
    let (new_decision, new_reply, reset) = evaluated(&c, &new_actor, 1, true).await;
    assert_eq!(reset.independent_reset_event, Some(resolution.id.clone()));
    assert_eq!(reset.consecutive_count, 1);
    assert!(!has_input(&reset, &resolution.id), "the actual paired workflow ACK consumes the reset occurrence");
    assert!(has_input(&reset, &self_terminal.id), "other original required input remains unconsumed");
    assert_eq!(reset.disposition, CompletionDisposition::Blocked { continuation_eligible: true, exhausted: false });
    same_original(&reply(&c, begin).await, &begun);
    same_original(&reply(&c, ack).await, &acknowledged);
    same_original(&reply(&c, new_decision).await, &new_reply);
    assert_eq!(audit(&c, &decision).await, immutable_audit);
    assert_eq!(get(&c, &accepted.id).await.input, accepted.input);
}

#[tokio::test]
async fn stopped_completion_resume_preserves_unknown_effect_cancel_deadline_and_native_block_fences() {
    for mode in ["unknown_begin", "pending_effect", "cancelled", "deadline", "native_retry_block"] {
        let p = path(); let mut cfg = governance_config();
        let clock = Arc::new(std::sync::atomic::AtomicU64::new((cfg.clock)()));
        let read = clock.clone(); cfg.clock = Arc::new(move || read.load(Ordering::SeqCst));
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        promote(&c, &format!("resume-fence-{mode}"), true).await;
        let (accepted, a, actor, w) = active(&c, mode).await;
        let question = pose(&c, &actor).await; close_question(&c, &question, None).await;
        let (decision, original, state) = evaluated(&c, &actor, 1, true).await;
        assert_eq!(state.disposition, CompletionDisposition::Blocked { continuation_eligible: true, exhausted: false });
        let immutable_audit = audit(&c, &decision).await;
        let mut unknown = None;
        if mode == "unknown_begin" {
            let view = observe_required_input(&c, &actor).await;
            let batch = prepare(&c, &actor, &view).await;
            let begin = begin_completion(&actor, &decision_ref(&decision), 2, Some(batch.batch_id.clone()));
            let r = reply(&c, begin.clone()).await;
            assert!(matches!(r.outcome, Outcome::Ok { data: Data::Completion(CompletionOutcome::Begun(_)) }));
            unknown = Some((begin, r));
        } else if mode == "pending_effect" {
            // A generic pending effect tests the resume fence, without PR publication.
            let operation_id = id("unknown-effect");
            let intended = json!({"controlled":"original pending effect"});
            let prepared = reply(&c, Operation::PrepareOperation { operation_id: operation_id.clone(),
                task_id: accepted.id.clone(), attempt_id: a.id.clone(), generation: a.generation,
                kind: "merge".into(), intended: intended.clone() }).await;
            assert!(matches!(&prepared.outcome, Outcome::Ok { data: Data::Operation(effect) }
                if effect.id == operation_id && effect.task_id == accepted.id
                    && effect.attempt_id == a.id && effect.generation == a.generation
                    && effect.kind == "merge" && effect.intended == intended
                    && effect.state == "pending" && effect.outcome.is_none()),
                "generic pending effect must retain its exact preparation: {prepared:?}");
        } else if mode == "native_retry_block" {
            reserve(&c, &a, 2).await;
            let rejected = rejected_completion_request(&c, &a, 2, borg_runtime::native::RejectionKind::RateLimited, borg_runtime::native::RetryAfter::Invalid).await;
            let blocked = task(report(&c, &a, &id("actual-native-block"), Observation::NativeRetryBlocked {
                block: borg_runtime::native::NativeRetryBlock { request_index: 2,
                    source_operation_id: rejected.operation_id, reason: borg_runtime::native::RetryBlockReason::InvalidRetryAfter },
            }).await);
            assert_eq!(blocked.status, TaskStatus::Blocked);
            assert!(blocked.native_retry_block.is_some());
        }
        let stop = completion_report(&a, id("fenced-physical-stop"), Observation::ProcessTreeStopped);
        let stopped_reply = reply(&c, stop.clone()).await;
        let stopped = task(stopped_reply.outcome.clone());
        assert!(stopped.attempt.as_ref().unwrap().stopped);
        assert_eq!(stopped.attempt.as_ref().unwrap().phase, AttemptPhase::Stopped);
        if mode == "native_retry_block" {
            assert_eq!(stopped.status, TaskStatus::Blocked, "typed completion cannot clear another blocked lifecycle");
            assert!(stopped.native_retry_block.is_some());
            // Independent activity cannot waive the explicit non-governance block.
            let later = host_question(&c, &accepted.id).await; close_question(&c, &later, None).await;
        } else {
            let (required, _) = required_question(&c, &accepted.id).await;
            if mode == "cancelled" {
                let current = get(&c, &accepted.id).await;
                let cancelled = task(call(&c, Operation::CancelTask { operation_id: id("cancel-held-resume"),
                    task_id: current.id, expected_revision: current.revision }).await);
                assert_eq!(cancelled.status, TaskStatus::Cancelled);
            }
            close_question(&c, &required, None).await;
            if mode == "deadline" { clock.store(accepted.input.limits.deadline_unix_ms + 1, Ordering::SeqCst); }
        }
        no_completion_resume(&c, &w, &accepted.id, &a.id).await;
        let retained = get(&c, &accepted.id).await;
        assert_eq!(retained.input, accepted.input);
        if mode == "pending_effect" { assert!(retained.attempt.as_ref().unwrap().uncertain); }
        if mode == "cancelled" { assert_eq!(retained.status, TaskStatus::Cancelled); }
        if mode == "deadline" { assert_eq!(retained.status, TaskStatus::Failed); }
        if mode == "native_retry_block" {
            assert_eq!(retained.status, TaskStatus::Blocked); assert_eq!(retained.native_retry_block, stopped.native_retry_block);
        }
        same_original(&reply(&c, decision.clone()).await, &original);
        same_original(&reply(&c, stop.clone()).await, &stopped_reply);
        assert_eq!(audit(&c, &decision).await, immutable_audit);
        drop(c); controller_owner_released(&p).await;
        let hold = stored_hold(&p, &accepted.id).unwrap();
        assert_eq!(hold.decision, decision_ref(&decision)); assert_eq!(hold.state, state);
        if mode == "unknown_begin" { assert!(hold.continuation.as_ref().unwrap().acknowledged.is_none()); }
        let c = Controller::open_with_config(&p, cfg).unwrap();
        no_completion_resume(&c, &w, &accepted.id, &a.id).await;
        same_original(&reply(&c, decision).await, &original);
        if let Some((begin, r)) = unknown { same_original(&reply(&c, begin).await, &r); }
        assert_eq!(get(&c, &accepted.id).await, retained);
    }
}

#[tokio::test]
async fn native_completion_ack_requires_same_binding_and_every_original_rejection_in_order() {
    // Fresh canonical ACK identities prevent an earlier refusal receipt from
    // masking validation of a later variant. No provider or request is run.
    for mode in ["exact", "missing_binding", "missing_rejection", "changed_binding", "reversed_rejections", "foreign_identity",
        "invalid_request", "context_limit", "policy_refused", "quota_exhausted", "authentication_failed"] {
        let p = path(); let cfg = governance_config();
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        promote(&c, &format!("native-ack-{mode}"), true).await;
        let (accepted, a, actor, _w) = active(&c, mode).await;
        let question = pose(&c, &actor).await;
        let terminal = close_question(&c, &question, None).await;
        let (decision, _, state) = evaluated(&c, &actor, 1, true).await;
        let view = observe_required_input(&c, &actor).await;
        let batch = prepare(&c, &actor, &view).await;
        assert!(batch.page.contains_event(&terminal.id));
        let begin = begin_completion(&actor, &decision_ref(&decision), 2, Some(batch.batch_id.clone()));
        let begun = reply(&c, begin.clone()).await;
        let continuation = match &begun.outcome {
            Outcome::Ok { data: Data::Completion(CompletionOutcome::Begun(value)) } => value.continuation.clone(),
            other => panic!("one fixed original input expected: {other:?}"),
        };
        assert_eq!(continuation.target, CompletionTarget::NativeRequest { request_index: 2 });
        assert!(continuation.acknowledged.is_none());
        assert_eq!(observe(&c, &actor).await.pending, Some(batch.clone()));
        let workflow_begin = delivery(begin_delivery(&c, &actor, &batch, &continuation.context_binding).await);
        assert!(workflow_begin.pending.as_ref().unwrap().delivery.is_some());
        use borg_runtime::native::RejectionKind;
        let first_kind = match mode {
            "invalid_request" => RejectionKind::InvalidRequest,
            "context_limit" => RejectionKind::ContextLimit,
            "policy_refused" => RejectionKind::PolicyRefused,
            "quota_exhausted" => RejectionKind::QuotaExhausted,
            "authentication_failed" => RejectionKind::AuthenticationFailed,
            _ => RejectionKind::RateLimited,
        };
        reserve(&c, &a, 2).await;
        let first = rejected_completion_request(&c, &a, 2, first_kind, borg_runtime::native::RetryAfter::Absent).await;
        // Direct trusted Usage receipts deliberately exercise the ACK consumer
        // independently of AcquireNativeRequest's earlier retry-class fence.
        // A known fatal rejection remains durable evidence, never input ACK.
        reserve(&c, &a, 3).await;
        let second = rejected_completion_request(&c, &a, 3, RejectionKind::RateLimited, borg_runtime::native::RetryAfter::Absent).await;
        reserve(&c, &a, 4).await;
        let exact_input = NativeCompletionInput { request_binding: completion_binding(), rejections: vec![first, second] };
        let mut evidence = Some(exact_input.clone());
        match mode {
            "missing_binding" => evidence = None,
            "missing_rejection" => { evidence.as_mut().unwrap().rejections.remove(0); },
            "changed_binding" => evidence.as_mut().unwrap().request_binding.sha256 = "0".repeat(64),
            "reversed_rejections" => evidence.as_mut().unwrap().rejections.reverse(),
            "foreign_identity" => evidence.as_mut().unwrap().rejections[0].operation_id = id("unrelated-rejection"),
            _ => {},
        }
        let workflow_ack = ack_delivery(&c, &actor, &batch, &continuation.context_binding).await;
        let workflow_ack_id = workflow_ack.commit.as_ref().unwrap().operation_id.clone();
        assert!(delivery(workflow_ack).pending.is_none());
        let before = get(&c, &accepted.id).await;
        let mut ack = completion_ack(&actor, &continuation, 4, evidence.clone());
        let Operation::AcknowledgeCompletionInput(value) = &mut ack else { unreachable!() };
        value.delivery_ack_operation_id = Some(workflow_ack_id.clone());
        let original = reply(&c, ack.clone()).await;
        assert!(original.commit.is_some());
        let mut expected = continuation.clone();
        if mode == "exact" {
            expected.acknowledged = Some(CompletionInputAck {
                operation_id: ack.mutation_id().unwrap().into(),
                consumed_at: CompletionBoundary::NativeResponse { request_index: 4 },
                delivery_ack_operation_id: Some(workflow_ack_id), native_input: Some(exact_input.clone()),
            });
            assert_eq!(original.outcome, Outcome::Ok { data: Data::Completion(CompletionOutcome::InputAcknowledged(expected.clone())) });
        } else {
            assert_eq!(code(original.outcome.clone()), if mode == "missing_rejection" { "capacity" } else { "source_mismatch" });
            assert_eq!(get(&c, &accepted.id).await, before, "rejected evidence cannot acknowledge or replace the fixed input");
        }
        same_original(&reply(&c, ack.clone()).await, &original);
        same_original(&reply(&c, begin.clone()).await, &begun);
        drop(c); controller_owner_released(&p).await;
        let hold = stored_hold(&p, &accepted.id).unwrap();
        assert_eq!(hold.state, state); assert_eq!(hold.continuation, Some(expected));
        let c = Controller::open_with_config(&p, cfg).unwrap();
        same_original(&reply(&c, ack).await, &original);
        same_original(&reply(&c, begin).await, &begun);
    }
}

#[tokio::test]
async fn codex_completion_ack_uses_no_native_request_evidence() {
    for native_evidence in [false, true] {
        let p = path(); let cfg = governance_config();
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        promote(&c, "codex-ack-policy", true).await;
        let mut input = coding(&id("codex-ack-agent"));
        input.definition.execution_profile = Some(ExecutionProfile::CollaborationV2 { publication: None, web_search: None });
        let accepted = submit(&c, &id("codex-ack-submit"), input).await;
        let w = worker(&id("codex-ack-worker"), "inc", 0, 1); register(&c, &w).await;
        let a = run(&commands(&c, &w, vec![]).await); assert_eq!(a.task_id, accepted.id);
        let thread = id("codex-thread"); let first_turn = id("codex-first-turn");
        task(report(&c, &a, &id("codex-started"), Observation::Started {
            thread_id: Some(thread.clone()), turn_id: Some(first_turn.clone()),
        }).await);
        let actor = caller(&a);
        let question = pose(&c, &actor).await;
        let terminal = close_question(&c, &question, None).await;
        let boundary = CompletionBoundary::CodexTurn { thread_id: thread.clone(), turn_id: first_turn.clone() };
        let decision = Operation::EvaluateCompletion(EvaluateCompletion {
            operation_id: evaluation_id(&actor, &boundary).unwrap(), caller: actor.clone(), boundary,
            intervening_requests: vec![],
        });
        assert_eq!(code(reply(&c, decision.clone()).await.outcome), "governance_completion_blocked");
        let state = completion_state(&audit(&c, &decision).await);
        assert_eq!(state.disposition, CompletionDisposition::Blocked { continuation_eligible: true, exhausted: false });
        let view = observe_required_input(&c, &actor).await;
        let batch = prepare(&c, &actor, &view).await;
        assert!(batch.page.contains_event(&terminal.id));
        let begin = Operation::BeginCompletionContinuation(BeginCompletionContinuation {
            operation_id: begin_id(&actor, &decision_ref(&decision)).unwrap(), caller: actor.clone(),
            decision: decision_ref(&decision), context_binding: id("codex-original-input"),
            target: CompletionTarget::CodexTurn { thread_id: thread.clone(), previous_turn_id: first_turn },
            delivery_batch_id: Some(batch.batch_id.clone()),
        });
        let begun = reply(&c, begin.clone()).await;
        let continuation = match &begun.outcome {
            Outcome::Ok { data: Data::Completion(CompletionOutcome::Begun(value)) } => value.continuation.clone(),
            other => panic!("original Codex continuation expected: {other:?}"),
        };
        assert!(continuation.acknowledged.is_none());
        assert_eq!(observe(&c, &actor).await.pending, Some(batch.clone()));
        let workflow_begin = delivery(begin_delivery(&c, &actor, &batch, &continuation.context_binding).await);
        assert!(workflow_begin.pending.as_ref().unwrap().delivery.is_some());
        let consumed_at = CompletionBoundary::CodexTurn { thread_id: thread, turn_id: id("codex-next-turn") };
        let workflow_ack = ack_delivery(&c, &actor, &batch, &continuation.context_binding).await;
        let workflow_ack_id = workflow_ack.commit.as_ref().unwrap().operation_id.clone();
        assert!(delivery(workflow_ack).pending.is_none());
        let ack = Operation::AcknowledgeCompletionInput(AcknowledgeCompletionInput {
            operation_id: acknowledgement_id(&actor, &continuation.begin_operation_id).unwrap(), caller: actor.clone(),
            begin_operation_id: continuation.begin_operation_id.clone(), consumed_at: consumed_at.clone(),
            delivery_ack_operation_id: Some(workflow_ack_id.clone()),
            native_input: native_evidence.then(|| NativeCompletionInput { request_binding: completion_binding(), rejections: vec![] }),
        });
        let before = get(&c, &accepted.id).await;
        let original = reply(&c, ack.clone()).await;
        let mut expected = continuation;
        if native_evidence {
            assert_eq!(code(original.outcome.clone()), "source_mismatch");
            assert_eq!(get(&c, &accepted.id).await, before);
        } else {
            expected.acknowledged = Some(CompletionInputAck { operation_id: ack.mutation_id().unwrap().into(),
                consumed_at, delivery_ack_operation_id: Some(workflow_ack_id), native_input: None });
            assert_eq!(original.outcome, Outcome::Ok { data: Data::Completion(CompletionOutcome::InputAcknowledged(expected.clone())) });
        }
        same_original(&reply(&c, ack.clone()).await, &original);
        drop(c); controller_owner_released(&p).await;
        let hold = stored_hold(&p, &accepted.id).unwrap();
        assert_eq!(hold.state, state); assert_eq!(hold.continuation, Some(expected));
        let c = Controller::open_with_config(&p, cfg).unwrap();
        same_original(&reply(&c, ack).await, &original);
        same_original(&reply(&c, begin).await, &begun);
    }
}

#[path = "governance_completion_startup.rs"]
mod startup_acceptance;

// Measure the fixture boundary without polling either request future. The real
// behavioral cases retain their controller operations and assertions unchanged.
#[tokio::test]
async fn completion_fixture_dispatch_keeps_controller_future_off_parent_stack() {
    async fn unboxed(c: &Controller, command: Operation) -> Reply {
        let value = c.handle(Request {
            protocol_revision: WIRE_REVISION, request_id: id("request"), command: command.clone(),
        }).await;
        value.validate_for(&command).unwrap();
        value
    }
    let p = path();
    let c = Controller::open_with_config(&p, governance_config()).unwrap();
    let command = Operation::QueryGovernance { query: GovernanceQuery::State };
    let before = unboxed(&c, command.clone());
    let after = reply(&c, command);
    let before_bytes = std::mem::size_of_val(&before);
    let after_bytes = std::mem::size_of_val(&after);
    eprintln!("completion fixture dispatch bytes: unboxed={before_bytes}, boxed={after_bytes}");
    assert!(after_bytes < before_bytes);
    drop((before, after));
    drop(c); controller_owner_released(&p).await;
}

#[path = "governance_completion_required_input.rs"]
mod governance_completion_required_input;
