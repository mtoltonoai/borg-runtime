//! The original final ResultCandidate rechecks the exact pre-stop input view.
//! No provider/worker process is run: real Controller operations and controlled
//! observations qualify durable ordering and ordinary admission only.
use super::*;
use super::governance::{governance_config, promote};
use borg_runtime::{
    collaboration::{questions::*, workflow::*, AttemptView, Query, QueryResult},
    governance::*, governance_completion::*, mcp::Caller,
};
use std::collections::BTreeMap;

fn id(label: &str) -> String {
    format!("final-completion-{label}-{}", NEXT.fetch_add(1, Ordering::Relaxed))
}
async fn reply(c: &Controller, command: Operation) -> Reply {
    let value = Box::pin(c.handle(Request { protocol_revision: WIRE_REVISION,
        request_id: id("request"), command: command.clone() })).await;
    value.validate_for(&command).unwrap(); value
}
fn same_original(actual: &Reply, original: &Reply) {
    assert_eq!(actual.outcome, original.outcome); assert_eq!(actual.commit, original.commit);
    assert!(actual.commit.is_some());
}
fn report_operation(a: &Attempt, report_id: &str, observation: Observation) -> Operation {
    Operation::ObserveWorker { report: Report { report_id: report_id.into(), task_id: a.task_id.clone(),
        attempt_id: a.id.clone(), generation: a.generation, worker_id: a.worker_id.clone(),
        incarnation: a.worker_incarnation.clone(), observation } }
}
fn final_candidate(a: &Attempt) -> Operation {
    report_operation(a, &id("result"), Observation::ResultCandidate {
        checks_passed: true, summary: "Accepted structured answer".into(), checkpoint: None,
        validation: vec![borg_runtime::native_loop_contract::evidence(&a.id)],
        output: Some(json!({"answer":42})),
    })
}
fn reference(operation: &Operation) -> DecisionRef {
    DecisionRef { operation_id: operation.mutation_id().unwrap().into(), point: DecisionPoint::Completion }
}
async fn audit(c: &Controller, operation: &Operation) -> DecisionRecord {
    match call(c, Operation::QueryGovernance { query: GovernanceQuery::Decision { decision: reference(operation) } }).await {
        Outcome::Ok { data: Data::Governance(value) } => match *value {
            GovernanceResult::Decision(record) => record,
            other => panic!("completion decision expected: {other:?}"),
        },
        other => panic!("completion audit expected: {other:?}"),
    }
}
fn state(record: &DecisionRecord) -> CompletionDecision {
    let DecisionDetail::Completion(value) = &record.detail else { panic!("completion detail expected") };
    value.clone()
}
async fn attempts(c: &Controller, task_id: &str) -> Vec<AttemptView> {
    match call(c, Operation::QueryCollaboration { query: Query::TaskAttempts {
        task_id: task_id.into(), after: None, limit: 8,
    }}).await {
        Outcome::Ok { data: Data::Collaboration(QueryResult::TaskAttempts(page)) } => {
            assert!(page.next_cursor.is_none()); page.items
        },
        other => panic!("finite actual attempt history expected: {other:?}"),
    }
}
async fn active_admitted(c: &Controller) -> (Task, Attempt, Worker, Question, Operation, Reply) {
    let mut input = native(&id("agent"));
    input.definition.execution_profile = Some(ExecutionProfile::CollaborationV2 { web_search: None });
    let queued = submit(c, &id("submit"), input).await;
    let w = worker(&id("worker"), "inc", 1, 0); register(c, &w).await;
    let issued = commands(c, &w, vec![]).await;
    let a = run(&issued); assert_eq!(a.task_id, queued.id);
    task(report(c, &a, &id("started"), Observation::Started { thread_id: None, turn_id: None }).await);
    let command_ids = issued.iter().filter_map(|command| match command {
        WorkerCommand::RunAttempt { command_id, .. } => Some(command_id.clone()), _ => None,
    }).collect();
    assert!(!commands(c, &w, command_ids).await.iter().any(|command| matches!(command, WorkerCommand::RunAttempt { .. })));
    // The final result needs the current marked charge and its original exact
    // Completed diagnostic. These are controlled facts, not a provider run.
    controlled_native_request(c, &a, 1, Some(4), Some(2)).await;
    let question = match call(c, Operation::MutateQuestion { operation_id: id("question"), mutation: QuestionMutation::Pose {
        question: PoseQuestion { record: RecordRef::Task { task_id: queued.id.clone() },
            recipient: PrincipalRef::TrustedHostOperator, prompt: "Choose the existing option".into(),
            options: vec![
                QuestionOption { id: "keep".into(), label: "Keep".into(), explanation: "Keep the existing choice".into() },
                QuestionOption { id: "change".into(), label: "Change".into(), explanation: "Use the alternative".into() },
            ], recommended_option_id: "keep".into() },
    }}).await {
        Outcome::Ok { data: Data::QuestionMutation(value) } => value.question.question,
        other => panic!("optional host question expected: {other:?}"),
    };
    let caller = Caller { task_id: queued.id.clone(), attempt_id: a.id.clone(), generation: a.generation,
        worker_id: a.worker_id.clone(), incarnation: a.worker_incarnation.clone() };
    let boundary = CompletionBoundary::NativeResponse { request_index: 1 };
    let evaluation = Operation::EvaluateCompletion(EvaluateCompletion {
        operation_id: evaluation_id(&caller, &boundary).unwrap(), caller, boundary, intervening_requests: vec![],
    });
    let admitted = reply(c, evaluation.clone()).await;
    let Outcome::Ok { data: Data::Completion(CompletionOutcome::Evaluated(value)) } = &admitted.outcome else {
        panic!("actual active completion admission expected: {admitted:?}")
    };
    assert_eq!(value.state.disposition, CompletionDisposition::Admit); assert!(value.state.reasons.is_empty());
    assert_eq!(value.state.consecutive_count, 0);
    (get(c, &queued.id).await, a, w, question, evaluation, admitted)
}
async fn closed_input(c: &Controller, question: &Question) -> WorkflowActivity {
    let value = reply(c, Operation::MutateQuestion { operation_id: id("question-close"), mutation: QuestionMutation::Cancel {
        question_id: question.id.clone(), expected_revision: question.revision, reason: "New input after active completion admission".into(),
    }}).await;
    assert!(matches!(value.outcome, Outcome::Ok { data: Data::QuestionMutation(_) }), "{value:?}");
    let mut found = Vec::new(); let mut cursor = None;
    for _ in 0..32 {
        let page = match call(c, Operation::QueryWorkflowActivity { cursor: cursor.take(), limit: 8 }).await {
            Outcome::Ok { data: Data::WorkflowActivity(page) } => page,
            other => panic!("workflow activity expected: {other:?}"),
        };
        found.extend(page.items.into_iter().filter(|event|
            event.subject == (RecordRef::Question { question_id: question.id.clone() })
                && matches!(&event.detail, ActivityDetail::Question(QuestionChange::Closed { .. }))));
        cursor = page.next_cursor;
        if cursor.is_none() { break; }
    }
    assert!(cursor.is_none()); assert_eq!(found.len(), 1); found.remove(0)
}
async fn stop(c: &Controller, a: &Attempt) -> (Task, Operation, Reply) {
    complete_native_loop(c, a).await;
    task(report(c, a, &id("executor"), Observation::ExecutorOutcome {
        success: true, summary: "Actual controlled native exit".into(),
    }).await);
    let operation = report_operation(a, &id("stopped"), Observation::ProcessTreeStopped);
    let original = reply(c, operation.clone()).await;
    let stopped = task(original.outcome.clone());
    assert_eq!(stopped.status, TaskStatus::Active);
    let current = stopped.attempt.as_ref().unwrap();
    assert_eq!(current.id, a.id); assert_eq!(current.phase, AttemptPhase::Stopped);
    assert!(current.stopped); assert!(!current.uncertain);
    (stopped, operation, original)
}
fn assert_blocked(record: &DecisionRecord, original: &Reply, event: &EventId, stopped: &Task) {
    assert_eq!(record.mode, DecisionMode::Enforcing);
    assert_eq!(record.actual_outcome, ActualDecisionOutcome::GovernanceBlocked);
    assert_eq!(record.controller_revision, original.commit.as_ref().unwrap().controller_revision);
    let current = state(record);
    assert_eq!(current.subject.task_id, stopped.id);
    assert_eq!(current.subject.task_revision, stopped.revision);
    assert_eq!(current.subject.attempt_id, stopped.attempt.as_ref().unwrap().id);
    assert!(matches!(&record.actor.provenance, ActorProvenance::BoundAgentSession { task_id, attempt_id, generation, .. }
        if task_id == &stopped.id && attempt_id == &stopped.attempt.as_ref().unwrap().id
            && *generation == stopped.attempt.as_ref().unwrap().generation));
    assert!(record.observed_product_revision.unwrap() >= event.product_revision);
    assert_eq!(current.disposition, CompletionDisposition::Blocked { continuation_eligible: true, exhausted: false });
    assert_eq!(current.reasons, vec![CompletionReason::UnconsumedInput { event_id: event.clone() }]);
    assert_eq!(current.consecutive_count, 1); assert_eq!(current.independent_reset_event, Some(event.clone()));
}
fn durable(path: &std::path::Path) -> BTreeMap<String, Vec<String>> {
    let db = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut all = BTreeMap::new();
    for (name, sql) in [
        ("tasks", "SELECT json_array(id,status,active,body) FROM tasks ORDER BY id"),
        ("attempts", "SELECT json_array(id,body,executor_outcome,model_requests,input_tokens,output_tokens) FROM attempts ORDER BY id"),
        ("history", "SELECT json_array(seq,task_id,revision,kind,body) FROM history ORDER BY seq"),
        ("receipts", "SELECT json_array(id,request,outcome,controller_revision,governance) FROM receipts ORDER BY id"),
        ("counters", "SELECT json_array(kind,value) FROM counters ORDER BY kind"),
        ("events", "SELECT json_array(product_revision,ordinal,body,targets) FROM workflow_events ORDER BY product_revision,ordinal"),
        ("delivery", "SELECT json_array(id,body,workflow_delivery) FROM sessions ORDER BY id"),
        ("commands", "SELECT json_array(id,acknowledged,body,delivery_operation_id) FROM commands ORDER BY id"),
    ] {
        all.insert(name.into(), db.prepare(sql).unwrap().query_map([], |row| row.get::<_, String>(0)).unwrap()
            .collect::<Result<Vec<_>, _>>().unwrap());
    }
    all
}
fn hold(path: &std::path::Path, task_id: &str) -> CompletionHold {
    let db = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let raw: String = db.query_row("SELECT body FROM tasks WHERE id=?1", [task_id], |row| row.get(0)).unwrap();
    let task: serde_json::Value = serde_json::from_str(&raw).unwrap();
    serde_json::from_value(task["completion_hold"].clone()).unwrap()
}

#[tokio::test]
//= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
//= type=test
//# A voluntary completion decision MUST refuse completion while an applicable required input remains unconsumed.
async fn late_input_and_final_result_follow_the_original_commit_order() {
    for input_first in [true, false] {
        let p = path(); let cfg = governance_config();
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        promote(&c, "final-order-policy", true).await;
        let (accepted, a, _w, question, evaluation, admitted) = active_admitted(&c).await;
        let early = audit(&c, &evaluation).await;
        let terminal = if input_first { Some(closed_input(&c, &question).await) } else { None };
        // Stop captures the real active input projection before changing phase.
        // No Prepare, Begin, ACK, explicit wait or SQL destination is invented.
        let (stopped, stop_operation, stop_reply) = stop(&c, &a).await;
        let candidate = final_candidate(&a);
        let original = reply(&c, candidate.clone()).await;
        let final_record = audit(&c, &candidate).await;
        let current = get(&c, &accepted.id).await;
        if let Some(terminal) = &terminal {
            assert!(early.observed_product_revision.unwrap() < terminal.id.product_revision);
            assert_eq!(code(original.outcome.clone()), "governance_completion_blocked");
            assert_blocked(&final_record, &original, &terminal.id, &stopped);
            assert_eq!(current.status, TaskStatus::Queued); assert!(current.result.is_none());
            assert_eq!(current.blocked_reason, None,
                "the stopped typed hold permits a fresh ordinary continuation, not the refused final result");
            assert!(current.attempt.as_ref().unwrap().stopped);
        } else {
            assert_eq!(task(original.outcome.clone()).status, TaskStatus::Succeeded);
            assert_eq!(current.status, TaskStatus::Succeeded); assert!(current.result.is_some());
            assert_eq!(state(&final_record).disposition, CompletionDisposition::Admit);
            assert_eq!(final_record.actual_outcome, ActualDecisionOutcome::Applied);
            closed_input(&c, &question).await;
            assert_eq!(get(&c, &accepted.id).await, current, "later input cannot revoke an already committed terminal result");
        }
        assert_eq!(current.id, accepted.id); assert_eq!(current.session_id, accepted.session_id);
        assert_eq!(current.input, accepted.input);
        assert_eq!(current.attempt.as_ref().unwrap().id, a.id);
        same_original(&reply(&c, evaluation.clone()).await, &admitted);
        same_original(&reply(&c, stop_operation.clone()).await, &stop_reply);
        same_original(&reply(&c, candidate.clone()).await, &original);
        assert_eq!(audit(&c, &evaluation).await, early);
        assert_eq!(get(&c, &accepted.id).await, current);
        drop(c); controller_owner_released(&p).await;
        let before = durable(&p);
        if terminal.is_some() {
            let retained = hold(&p, &accepted.id);
            assert_eq!(retained.decision, reference(&candidate));
            assert_eq!(retained.state, state(&final_record));
            assert_eq!(Some(retained.observed_product_revision), final_record.observed_product_revision);
            assert!(retained.continuation.is_none());
            assert_eq!(super::governance_completion::stored_completion_reason(&p, &accepted.id),
                Some(format!("governance_completion_blocked:{}", retained.decision.operation_id)),
                "the eligible public projection must not erase the original stored hold");
        }
        let c = Controller::open_with_config(&p, cfg).unwrap();
        same_original(&reply(&c, candidate.clone()).await, &original);
        same_original(&reply(&c, stop_operation).await, &stop_reply);
        assert_eq!(audit(&c, &candidate).await, final_record);
        drop(c); controller_owner_released(&p).await;
        assert_eq!(durable(&p), before, "restart/replay preserves both original stop evidence and final receipt");
    }
}

#[cfg(debug_assertions)]
#[tokio::test]
//= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
//= type=test
//# A continuation after refused voluntary completion MUST preserve the task's original identity, accepted settings, remaining deadline, accounting and unresolved effects.
async fn final_result_commit_cut_retries_once_and_admits_same_task_with_exact_recorded_reason() {
    let p = path(); let probe = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut cfg = governance_config(); cfg.qualification_fail_next_commit = Some(probe.clone());
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    promote(&c, "final-atomic-policy", true).await;
    let (accepted, a, w, question, evaluation, _) = active_admitted(&c).await;
    let terminal = closed_input(&c, &question).await;
    let (stopped, _, _) = stop(&c, &a).await;
    let candidate = final_candidate(&a);
    let early = audit(&c, &evaluation).await;
    let previous_attempts = attempts(&c, &accepted.id).await;
    assert_eq!(previous_attempts.len(), 1); assert_eq!(previous_attempts[0].reported_model_requests, 1);
    drop(c); controller_owner_released(&p).await;
    let before = durable(&p); let original_hold = hold(&p, &accepted.id);
    assert_eq!(original_hold.decision, reference(&evaluation));
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    probe.store(true, Ordering::SeqCst);
    let failed = reply(&c, candidate.clone()).await;
    assert!(!probe.load(Ordering::SeqCst)); assert_eq!(code(failed.outcome), "store_unavailable");
    assert!(failed.commit.is_none()); assert_eq!(get(&c, &accepted.id).await, stopped);
    assert_eq!(audit(&c, &evaluation).await, early);
    assert_eq!(code(call(&c, Operation::GetOperation { operation_id: reference(&candidate).operation_id.clone() }).await), "not_found");
    assert_eq!(code(call(&c, Operation::QueryGovernance { query: GovernanceQuery::Decision { decision: reference(&candidate) } }).await), "not_found");
    drop(c); controller_owner_released(&p).await;
    assert_eq!(durable(&p), before); assert_eq!(hold(&p, &accepted.id), original_hold);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let original = reply(&c, candidate.clone()).await;
    assert_eq!(code(original.outcome.clone()), "governance_completion_blocked");
    let final_record = audit(&c, &candidate).await;
    assert_blocked(&final_record, &original, &terminal.id, &stopped);
    let queued = get(&c, &accepted.id).await;
    assert_eq!(queued.status, TaskStatus::Queued); assert_eq!(queued.revision, stopped.revision + 1);
    assert_eq!(queued.input, accepted.input); assert!(queued.result.is_none());
    assert_eq!(attempts(&c, &accepted.id).await, previous_attempts);
    same_original(&reply(&c, candidate.clone()).await, &original);
    drop(c); controller_owner_released(&p).await;
    let retained = hold(&p, &accepted.id);
    assert_eq!(retained.decision, reference(&candidate)); assert_eq!(retained.state, state(&final_record));
    assert!(retained.continuation.is_none(), "a stopped refusal is not an input ACK");
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    same_original(&reply(&c, candidate).await, &original);
    let admission = Operation::AwaitCommands { operation_id: id("resume-commands"), worker_id: w.id.clone(),
        incarnation: w.incarnation.clone(), acknowledged: vec![] };
    let issued = reply(&c, admission.clone()).await;
    let Outcome::Ok { data: Data::Commands(commands) } = &issued.outcome else { panic!("ordinary continuation command expected: {issued:?}") };
    let matching: Vec<_> = commands.iter().filter(|command| matches!(command,
        WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == accepted.id)).collect();
    assert_eq!(matching.len(), 1);
    let WorkerCommand::RunAttempt { attempt: next, task: input, session, completion_continuation, .. } = matching[0] else { unreachable!() };
    assert_ne!(next.id, a.id); assert!(next.generation > a.generation);
    assert_eq!(next.task_id, accepted.id); assert_eq!(next.session_id, accepted.session_id);
    assert_eq!(session.id, accepted.session_id); assert_eq!(next.deadline_unix_ms, a.deadline_unix_ms);
    let mut remaining_input = accepted.input.clone(); remaining_input.limits.model_requests -= 1;
    assert_eq!(*input, remaining_input, "ordinary admission carries only the real remaining request allowance");
    let resume = completion_continuation.as_ref().expect("exact stopped final-decision input");
    assert_eq!(resume.evaluation.decision, final_record.id);
    assert!(resume.reset_event.is_none(), "the already eligible final hold needs no independent reset witness");
    assert_eq!(resume.evaluation.state, state(&final_record));
    assert_eq!(resume.evaluation.boundary, CompletionBoundary::StoppedResult { report_id: final_record.id.operation_id.clone() });
    let current = get(&c, &accepted.id).await;
    assert_eq!(current.id, accepted.id); assert_eq!(current.session_id, accepted.session_id);
    assert_eq!(current.input, accepted.input); assert_eq!(current.attempt.as_ref().unwrap().id, next.id);
    let recorded = attempts(&c, &accepted.id).await;
    assert_eq!(recorded.len(), 2);
    assert_eq!(recorded.iter().map(|view| view.reported_model_requests).sum::<u32>(), 1);
    assert_eq!(recorded.iter().find(|view| view.attempt.id == a.id).unwrap(), &previous_attempts[0]);
    assert_eq!(recorded.iter().find(|view| view.attempt.id == next.id).unwrap().reported_model_requests, 0);
    same_original(&reply(&c, admission).await, &issued);
    assert_eq!(attempts(&c, &accepted.id).await, recorded, "original command replay cannot admit a second continuation");
    drop(c); controller_owner_released(&p).await;
    let after_admission = hold(&p, &accepted.id);
    assert_eq!(after_admission.state, retained.state);
    assert!(after_admission.continuation.is_none(), "ordinary admission alone never acknowledges model input");
}

#[tokio::test]
async fn successful_executor_exit_cannot_bypass_failed_required_completion_check() {
    let p = path(); let cfg = governance_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    promote(&c, "required-final-check-policy", true).await;
    let (accepted, a, _worker, _optional_question, evaluation, early) = active_admitted(&c).await;
    assert_eq!(accepted.status, TaskStatus::Active); assert!(accepted.result.is_none());
    let (stopped, stop_operation, stop_reply) = stop(&c, &a).await;
    assert_eq!(stopped.status, TaskStatus::Active); assert!(stopped.result.is_none(),
        "successful exit and physical stop are not platform acceptance");
    let before_attempts = attempts(&c, &accepted.id).await;
    let mut candidate = final_candidate(&a);
    let Operation::ObserveWorker { report } = &mut candidate else { unreachable!() };
    let Observation::ResultCandidate { checks_passed, .. } = &mut report.observation else { unreachable!() };
    *checks_passed = false;
    let refusal = reply(&c, candidate.clone()).await;
    assert_eq!(code(refusal.outcome.clone()), "governance_completion_blocked");
    let record = audit(&c, &candidate).await;
    assert_eq!(record.actual_outcome, ActualDecisionOutcome::GovernanceBlocked);
    assert_eq!(record.controller_revision, refusal.commit.as_ref().unwrap().controller_revision);
    assert_eq!(record.id, reference(&candidate));
    let blocked = state(&record);
    assert_eq!(blocked.reasons, vec![CompletionReason::AcceptanceCheck {
        check:CompletionCheck::NativeOutput, index:0 }]);
    assert_eq!(blocked.consecutive_count, 1); assert!(blocked.independent_reset_event.is_none());
    assert_eq!(blocked.disposition, CompletionDisposition::Blocked { continuation_eligible:true, exhausted:false });
    let held = get(&c, &accepted.id).await;
    assert_eq!(held.status, TaskStatus::Queued); assert_eq!(held.input, accepted.input);
    assert!(held.result.is_none()); assert_eq!(attempts(&c, &accepted.id).await, before_attempts);
    assert_eq!(held.blocked_reason, None, "the public queued view projects eligibility for ordinary continuation, not the stored completion marker");
    let lookup = reply(&c, Operation::GetOperation { operation_id:record.id.operation_id.clone() }).await;
    let Outcome::Ok { data:Data::Operation(saved) } = lookup.outcome else { panic!("original refusal lookup expected") };
    assert_eq!(saved.intended, serde_json::to_value(&candidate).unwrap());
    assert_eq!(saved.outcome, Some(serde_json::to_value(&refusal.outcome).unwrap()));
    assert_eq!(lookup.commit, refusal.commit);
    same_original(&reply(&c, candidate.clone()).await, &refusal);
    same_original(&reply(&c, stop_operation.clone()).await, &stop_reply);
    same_original(&reply(&c, evaluation.clone()).await, &early);
    drop(c); controller_owner_released(&p).await;
    assert_eq!(super::governance_completion::stored_completion_reason(&p, &accepted.id),
        Some(format!("governance_completion_blocked:{}", record.id.operation_id)));
    let retained = hold(&p, &accepted.id);
    assert_eq!(retained.decision, record.id); assert_eq!(retained.state, blocked);
    assert!(retained.continuation.is_none(), "public admission eligibility is not a model-input ACK");
    let before = durable(&p);
    let c = Controller::open_with_config(&p, cfg).unwrap();
    same_original(&reply(&c, candidate.clone()).await, &refusal);
    same_original(&reply(&c, stop_operation).await, &stop_reply);
    same_original(&reply(&c, evaluation).await, &early);
    assert_eq!(audit(&c, &candidate).await, record);
    assert_eq!(get(&c, &accepted.id).await, held);
    drop(c); controller_owner_released(&p).await;
    assert_eq!(durable(&p), before);
}
