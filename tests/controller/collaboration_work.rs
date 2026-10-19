//! Join under tests/controller.rs; all changes use the real controller API.
//! Controlled worker reports qualify state transitions, not worker processes or providers.
use super::*;
use borg_runtime::collaboration::{
    documents::*, notifications::*, questions::*, work::*, workflow::*, Page, Query, QueryResult,
};
use borg_runtime::mcp::Caller;

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
fn workflow_coding(agent: &str) -> TaskInput {
    let mut input = coding(agent);
    input.definition.execution_profile = Some(ExecutionProfile::CollaborationV2 { publication: None, web_search: None });
    input
}
async fn reply(c: &Controller, command: Operation) -> Reply {
    let r = c.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: format!("work-request-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
        command: command.clone(),
    }).await;
    r.validate_for(&command).unwrap();
    r
}
fn work_change(r: &Reply) -> WorkMutationOutcome {
    assert!(r.commit.is_some(), "{r:?}");
    match &r.outcome {
        Outcome::Ok { data: Data::WorkMutation(out) } => out.clone(),
        x => panic!("committed work mutation expected: {x:?}"),
    }
}
async fn mutate(c: &Controller, id: &str, mutation: WorkMutation) -> Reply {
    reply(c, Operation::MutateWork { operation_id: id.into(), mutation }).await
}
async fn work(c: &Controller, id: &str) -> WorkView {
    let r = reply(c, Operation::QueryWork { query: WorkQuery::Get { task_id: id.into() } }).await;
    assert!(r.commit.is_none(), "read must not mutate: {r:?}");
    assert!(r.observed_controller_revision.is_some());
    match r.outcome {
        Outcome::Ok { data: Data::WorkQuery(WorkQueryResult::Task(view)) } => view,
        x => panic!("work view expected: {x:?}"),
    }
}
async fn wait_page(c: &Controller, id: &str, after: Option<String>, limit: usize) -> Page<WorkflowWait> {
    match call(c, Operation::QueryWork { query: WorkQuery::Waits { task_id: id.into(), after, limit } }).await {
        Outcome::Ok { data: Data::WorkQuery(WorkQueryResult::Waits(page)) } => page,
        x => panic!("wait page expected: {x:?}"),
    }
}
async fn add(c: &Controller, id: &str, task: &str, revision: u64, resolver: ResolverRef) -> WorkMutationOutcome {
    work_change(&mutate(c, id, WorkMutation::AddWait {
        task_id: task.into(), expected_revision: revision, resolver,
    }).await)
}
fn pose_input(task_id: &str) -> PoseQuestion {
    PoseQuestion {
        record: RecordRef::Task { task_id: task_id.into() },
        recipient: PrincipalRef::TrustedHostOperator,
        prompt: "Choose one option. Recommended: keep.".into(),
        options: vec![
            QuestionOption { id: "keep".into(), label: "Keep".into(), explanation: "Preserve the current choice".into() },
            QuestionOption { id: "change".into(), label: "Change".into(), explanation: "Use the stated alternative".into() },
        ],
        recommended_option_id: "keep".into(),
    }
}
async fn question_mutation(c: &Controller, id: &str, mutation: QuestionMutation) -> QuestionMutationOutcome {
    match call(c, Operation::MutateQuestion { operation_id: id.into(), mutation }).await {
        Outcome::Ok { data: Data::QuestionMutation(out) } => *out,
        x => panic!("question mutation expected: {x:?}"),
    }
}
async fn pose(c: &Controller, id: &str, task_id: &str) -> Question {
    question_mutation(c, id, QuestionMutation::Pose { question: pose_input(task_id) }).await.question.question
}
async fn question(c: &Controller, id: &str) -> QuestionView {
    match call(c, Operation::QueryQuestion { query: QuestionQuery::Get { question_id: id.into() } }).await {
        Outcome::Ok { data: Data::QuestionQuery(value) } => match *value {
            QuestionQueryResult::Question(view) => view,
            other => panic!("question view expected: {other:?}"),
        },
        x => panic!("question view expected: {x:?}"),
    }
}
async fn cancel_question(c: &Controller, id: &str, q: &Question, reason: &str) -> QuestionMutationOutcome {
    question_mutation(c, id, QuestionMutation::Cancel {
        question_id: q.id.clone(), expected_revision: q.revision, reason: reason.into(),
    }).await
}
fn resolution(wait: &WorkflowWait) -> &ResolvedWait {
    match &wait.status {
        WaitStatus::Resolved { resolution, .. } => resolution,
        x => panic!("exact resolved wait expected: {x:?}"),
    }
}
fn assert_no_run_for(commands: &[WorkerCommand], task_id: &str) {
    assert!(!commands.iter().any(|c| matches!(c,
        WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == task_id
    )), "unexpected task execution: {commands:?}");
}
async fn native_occupied(c: &Controller, worker_id: &str) -> u64 {
    match call(c, Operation::QueryCollaboration { query: Query::Worker { worker_id: worker_id.into() } }).await {
        Outcome::Ok { data: Data::Collaboration(QueryResult::Worker(view)) } => view.native_occupied,
        x => panic!("worker view expected: {x:?}"),
    }
}
async fn start_and_charge(c: &Controller, a: &Attempt, label: &str, model_requests: u32) {
    task(report(c, a, &format!("{label}-started"), Observation::Started { thread_id: None, turn_id: None }).await);
    controlled_native_request(c, a, model_requests, Some(4), Some(2)).await;
}

#[tokio::test]
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//= type=test
//# The first open collaboration wait MUST retain the task's prior workflow state and assignee.
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//= type=test
//# Ending the last collaboration wait on a nonterminal task MUST restore its recorded workflow state and assignee.
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//= type=test
//# A resolved wait MUST retain the exact terminal outcome occurrence that resolved it.

// Traceability scope: Two real question wait identities and their exact terminal outcomes survive replay and reopen.
//= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
//= type=test
//# Every collaboration record MUST have a durable identity.
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//= type=test
//# A collaboration wait MUST identify a concrete task, question or review resolver.

async fn two_negative_question_outcomes_restore_only_after_last_wait_and_replay_after_restart() {
    let p = path();
    let cfg = workflow_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let t = submit(&c, "two-submit", workflow_native("two-owner")).await;
    let q1 = pose(&c, "two-question-one", &t.id).await;
    let q2 = pose(&c, "two-question-two", &t.id).await;
    let before = work(&c, &t.id).await;
    assert_eq!(before.coordination_revision, 0);
    let first_op = Operation::MutateWork { operation_id: "two-wait-one".into(), mutation: WorkMutation::AddWait {
        task_id: t.id.clone(), expected_revision: 0, resolver: ResolverRef::Question { question_id: q1.id.clone() },
    }};
    let first_reply = reply(&c, first_op.clone()).await;
    let first = work_change(&first_reply);
    let prior = ResumeState { status: t.status, assigned_agent_id: t.assigned_agent_id.clone() };
    assert_eq!(first.view.state, WorkState::Waiting { prior: prior.clone() });
    let second = add(&c, "two-wait-two", &t.id, first.view.coordination_revision,
        ResolverRef::Question { question_id: q2.id.clone() }).await;
    assert_eq!(second.view.open_waits, 2);
    let cancelled = cancel_question(&c, "two-cancel-one", &q1, "Need withdrawn").await;
    let one_left = work(&c, &t.id).await;
    assert!(one_left.coordination_blocked);
    assert_eq!(one_left.open_waits, 1);
    assert_eq!(one_left.state, WorkState::Waiting { prior: prior.clone() });
    let page_one = wait_page(&c, &t.id, None, 1).await;
    let page_two = wait_page(&c, &t.id, page_one.next_cursor.clone(), 1).await;
    assert!(page_one.next_cursor.is_some());
    assert!(page_two.next_cursor.is_none());
    assert_eq!(page_one.items.len(), 1);
    assert_eq!(page_two.items.len(), 1);
    assert_ne!(page_one.items[0].reference, page_two.items[0].reference);
    let closed = one_left.waits.items.iter().find(|w| w.reference == first.changed_wait.clone().unwrap()).unwrap();
    let exact = resolution(closed).clone();
    assert_eq!(exact.outcome, ResolverOutcome::Question(QuestionResolution::Cancelled {
        question_id: q1.id.clone(), action_revision: cancelled.question.question.revision, reason: "Need withdrawn".into(),
    }));
    let original_receipt = reply(&c, Operation::GetOperation { operation_id: "two-wait-one".into() }).await;
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg).unwrap();
    assert_eq!(work(&c, &t.id).await, one_left);
    let replay = reply(&c, first_op).await;
    assert_eq!(replay.outcome, first_reply.outcome);
    assert_eq!(replay.commit, first_reply.commit);
    let lookup = reply(&c, Operation::GetOperation { operation_id: "two-wait-one".into() }).await;
    assert_eq!(lookup.outcome, original_receipt.outcome);
    assert_eq!(lookup.commit, original_receipt.commit);
    let replaced = question_mutation(&c, "two-supersede-two", QuestionMutation::Supersede {
        question_id: q2.id.clone(), expected_revision: q2.revision, reason: "Refined decision".into(),
        replacement: pose_input(&t.id),
    }).await;
    let ended = work(&c, &t.id).await;
    assert_eq!(ended.open_waits, 0);
    assert!(!ended.coordination_blocked);
    assert_eq!(ended.state, WorkState::Runnable { state: prior });
    assert_eq!(ended.task.input, t.input);
    assert_eq!(ended.task.session_id, t.session_id);
    assert_eq!(ended.task.assigned_agent_id, t.assigned_agent_id);
    assert_ne!(ended.task.status, TaskStatus::Succeeded);
    assert!(ended.task.result.is_none());
    let retained = ended.waits.items.iter().find(|w| w.reference == exact.wait).unwrap();
    assert_eq!(resolution(retained), &exact);
    let second_wait = ended.waits.items.iter().find(|w| w.reference == second.changed_wait.clone().unwrap()).unwrap();
    assert_eq!(resolution(second_wait).outcome, ResolverOutcome::Question(QuestionResolution::Superseded {
        question_id: q2.id, replacement_question_id: replaced.replacement.unwrap().question.id,
        reason: "Refined decision".into(),
    }));
    // Replaying the terminal operation must not append another resolution.
    cancel_question(&c, "two-cancel-one", &q1, "Need withdrawn").await;
    assert_eq!(work(&c, &t.id).await, ended);
}

#[tokio::test]

// Traceability scope: Changing responsible owner preserves task/session/accepted input; it assigns no partition authority.
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# Task ownership MUST remain distinct from session and partition ownership.

async fn responsible_owner_transfer_preserves_assignee_accepted_input_and_open_question() {
    let c = Controller::open_with_config(path(), workflow_config()).unwrap();
    let t = submit(&c, "owner-original", workflow_native("original-owner")).await;
    submit(&c, "owner-next", workflow_native("next-owner")).await;
    let q = pose(&c, "owner-question", &t.id).await;
    let waiting = add(&c, "owner-wait", &t.id, 0, ResolverRef::Question { question_id: q.id.clone() }).await;
    let changed = work_change(&mutate(&c, "owner-transfer", WorkMutation::ChangeOwner {
        task_id: t.id.clone(), expected_revision: waiting.view.coordination_revision,
        owner: PrincipalRef::Agent { agent_id: "next-owner".into() },
    }).await);
    assert_eq!(changed.view.task.input, t.input);
    assert_eq!(changed.view.task.assigned_agent_id, t.assigned_agent_id);
    assert_eq!(changed.view.task.session_id, t.session_id);
    assert_eq!(changed.view.open_waits, 1);
    let observed = question(&c, &q.id).await;
    assert_eq!(observed.question, q);
    assert!(observed.active_answer.is_none());
    assert_eq!(observed.current_responsible_owner, changed.view.responsible_owner);
    let ended = work_change(&mutate(&c, "owner-remove-wait", WorkMutation::RemoveWait {
        task_id: t.id.clone(), expected_revision: changed.view.coordination_revision,
        wait_id: waiting.changed_wait.unwrap().wait_id, reason: "Continue independently".into(),
    }).await);
    assert_eq!(ended.view.responsible_owner, PrincipalRef::Agent { agent_id: "next-owner".into() });
    assert_eq!(ended.view.state, WorkState::Runnable { state: ResumeState {
        status: t.status, assigned_agent_id: t.assigned_agent_id,
    }});
    assert_eq!(ended.view.task.input, t.input);
    assert_eq!(question(&c, &q.id).await.question, q);
    assert!(matches!(&ended.view.waits.items[0].status, WaitStatus::Removed { reason, actor, operation_id, product_revision }
        if reason == "Continue independently" && actor.principal == PrincipalRef::TrustedHostOperator
        && *operation_id == "owner-remove-wait" && *product_revision == ended.product_revision));
}

#[tokio::test]
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//= type=test
//# A wait on a resolver that is already closed MUST be refused.
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//= type=test
//# A wait that would close a cycle through waits and parent links MUST be refused.

// Traceability scope: The real Request decoder rejects agent, team and external-party wait kinds; valid concrete resolvers remain separate.
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//= type=test
//# A wait on an agent, team or external party MUST be refused.

async fn closed_resolvers_and_task_wait_cycles_refuse_without_new_waits() {
    let c = Controller::open_with_config(path(), workflow_config()).unwrap();
    let a = submit(&c, "cycle-a", workflow_native("cycle-a")).await;
    let b = submit(&c, "cycle-b", workflow_native("cycle-b")).await;
    add(&c, "cycle-a-b", &a.id, 0, ResolverRef::Task { task_id: b.id.clone() }).await;
    let before = work(&c, &b.id).await;
    let refused = mutate(&c, "cycle-b-a", WorkMutation::AddWait {
        task_id: b.id.clone(), expected_revision: 0, resolver: ResolverRef::Task { task_id: a.id.clone() },
    }).await;
    assert_eq!(code(refused.outcome), "cycle");
    assert_eq!(work(&c, &b.id).await, before);
    let q = pose(&c, "closed-question", &b.id).await;
    cancel_question(&c, "close-question", &q, "No decision needed").await;
    assert_eq!(code(mutate(&c, "closed-question-wait", WorkMutation::AddWait {
        task_id: b.id.clone(), expected_revision: 0,
        resolver: ResolverRef::Question { question_id: q.id },
    }).await.outcome), "resolver_closed");
    let terminal = submit(&c, "closed-task", workflow_native("closed-task")).await;
    task(call(&c, Operation::CancelTask { operation_id: "close-task".into(), task_id: terminal.id.clone(), expected_revision: terminal.revision }).await);
    assert_eq!(code(mutate(&c, "closed-task-wait", WorkMutation::AddWait {
        task_id: b.id.clone(), expected_revision: 0,
        resolver: ResolverRef::Task { task_id: terminal.id },
    }).await.outcome), "resolver_closed");
    assert_eq!(work(&c, &b.id).await, before);
    for kind in ["agent", "team", "external_party"] {
        // Unsupported resolver identities are rejected by the real request decoder.
        let mut bad = serde_json::to_value(Request {
            protocol_revision: WIRE_REVISION, request_id: "unsupported-resolver".into(),
            command: Operation::MutateWork { operation_id: "unsupported-resolver".into(), mutation: WorkMutation::AddWait {
                task_id: b.id.clone(), expected_revision: 0,
                resolver: ResolverRef::Task { task_id: a.id.clone() },
            }},
        }).unwrap();
        assert!(serde_json::from_value::<Request>(bad.clone()).is_ok());
        *bad.pointer_mut("/input/mutation/input/resolver").unwrap() = json!({"kind":kind,"id":"someone"});
        assert!(serde_json::from_value::<Request>(bad).is_err());
    }
}

#[tokio::test]
async fn existing_delegation_parent_edge_participates_in_wait_cycle_check() {
    let c = Controller::open_with_config(path(), workflow_config()).unwrap();
    let parent = submit(&c, "parent-cycle-parent", workflow_native("parent-cycle-owner")).await;
    let w = worker("parent-cycle-worker", "inc", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let mut child_input = workflow_native("parent-cycle-child");
    child_input.limits.deadline_unix_ms = parent.input.limits.deadline_unix_ms;
    child_input.limits.model_requests = 2;
    let child = task(call(&c, Operation::DelegateTask {
        operation_id: "parent-cycle-delegate".into(), parent_task_id: parent.id.clone(),
        parent_attempt_id: a.id.clone(), generation: a.generation, child: (child_input).into(),
    }).await);
    assert_eq!(child.parent_task_id.as_deref(), Some(parent.id.as_str()));
    let before = work(&c, &child.id).await;
    assert_eq!(code(mutate(&c, "parent-cycle-close", WorkMutation::AddWait {
        task_id: child.id.clone(), expected_revision: 0,
        resolver: ResolverRef::Task { task_id: parent.id.clone() },
    }).await.outcome), "cycle");
    assert_eq!(work(&c, &child.id).await, before);
    assert_eq!(get(&c, &parent.id).await.waiting_child_id.as_deref(), Some(child.id.as_str()));
}

#[tokio::test]
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//= type=test
//# A task with an open collaboration wait MUST be ineligible for a new execution attempt.
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//= type=test
//# A task suspended on a collaboration wait MUST release its execution slot after its current execution has stopped.
async fn wait_quiesces_one_attempt_releases_slot_only_on_stop_and_resumes_with_remaining_budget() {
    let c = Controller::open_with_config(path(), workflow_config()).unwrap();
    let mut input = workflow_native("same-agent");
    input.limits.model_requests = 3;
    let t = submit(&c, "slot-original", input).await;
    let w = worker("slot-worker", "inc", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    start_and_charge(&c, &a, "slot-original", 1).await;
    let active = get(&c, &t.id).await;
    let q = pose(&c, "slot-question", &t.id).await;
    let waiting = add(&c, "slot-wait", &t.id, 0, ResolverRef::Question { question_id: q.id.clone() }).await;
    assert_eq!(waiting.view.state, WorkState::Waiting { prior: ResumeState {
        status: active.status, assigned_agent_id: active.assigned_agent_id,
    }});
    let retained = waiting.view.task.attempt.as_ref().unwrap();
    assert_eq!(retained.id, a.id);
    assert_eq!(retained.phase, AttemptPhase::StopRequested);
    assert!(!retained.stopped);
    let peer = submit(&c, "slot-peer", workflow_native("same-agent")).await;
    let held = commands(&c, &w, vec![]).await;
    assert!(held.iter().any(|c| matches!(c, WorkerCommand::StopAttempt { attempt_id, .. } if attempt_id == &a.id)));
    assert_no_run_for(&held, &peer.id);
    assert_eq!(native_occupied(&c, &w.id).await, 1);
    task(report(&c, &a, "slot-stopped", Observation::ProcessTreeStopped).await);
    assert_eq!(native_occupied(&c, &w.id).await, 0);
    let released = commands(&c, &w, vec![]).await;
    assert_no_run_for(&released, &t.id);
    assert_eq!(run(&released).task_id, peer.id, "waits do not pause every session of an agent");
    let spare = worker("slot-spare", "inc", 1, 0);
    register(&c, &spare).await;
    assert_no_run_for(&commands(&c, &spare, vec![]).await, &t.id);
    cancel_question(&c, "slot-resolve", &q, "Proceed without decision").await;
    let admitted = commands(&c, &spare, vec![]).await;
    let (next, budget) = admitted.iter().find_map(|command| match command {
        WorkerCommand::RunAttempt { attempt, task, .. } if attempt.task_id == t.id => Some((attempt, task.limits.model_requests)),
        _ => None,
    }).expect("eligible task has a fresh continuation attempt");
    assert_ne!(next.id, a.id);
    assert!(next.generation > a.generation);
    assert_eq!(next.session_id, a.session_id);
    assert_eq!(next.deadline_unix_ms, a.deadline_unix_ms);
    assert_eq!(budget, 2);
    assert_eq!(get(&c, &t.id).await.input, t.input);
}

#[tokio::test]
async fn resolving_before_process_stop_never_resurrects_or_replaces_the_live_attempt() {
    let c = Controller::open_with_config(path(), workflow_config()).unwrap();
    let t = submit(&c, "live-submit", workflow_native("live-owner")).await;
    let w = worker("live-worker", "inc", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let q = pose(&c, "live-question", &t.id).await;
    add(&c, "live-wait", &t.id, 0, ResolverRef::Question { question_id: q.id.clone() }).await;
    cancel_question(&c, "live-resolve", &q, "Decision withdrawn").await;
    let spare = worker("live-spare", "inc", 1, 0);
    register(&c, &spare).await;
    assert_no_run_for(&commands(&c, &spare, vec![]).await, &t.id);
    let retained = work(&c, &t.id).await;
    assert_eq!(retained.open_waits, 0);
    let attempt = retained.task.attempt.unwrap();
    assert_eq!(attempt.id, a.id);
    assert_eq!(attempt.phase, AttemptPhase::StopRequested);
    assert!(!attempt.stopped);
    assert_eq!(native_occupied(&c, &w.id).await, 1);
    task(report(&c, &a, "live-stop", Observation::ProcessTreeStopped).await);
    let next = run(&commands(&c, &spare, vec![]).await);
    assert_ne!(next.id, a.id);
    assert_eq!(next.session_id, a.session_id);
}

#[tokio::test]
async fn cancelled_task_stays_terminal_after_wait_resolution_and_reopen() {
    let p = path();
    let cfg = workflow_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let t = submit(&c, "cancel-submit", workflow_native("cancel-owner")).await;
    let q = pose(&c, "cancel-question", &t.id).await;
    add(&c, "cancel-wait", &t.id, 0, ResolverRef::Question { question_id: q.id.clone() }).await;
    let current = get(&c, &t.id).await;
    task(call(&c, Operation::CancelTask { operation_id: "cancel-task".into(), task_id: t.id.clone(), expected_revision: current.revision }).await);
    cancel_question(&c, "cancel-resolve", &q, "Wait has ended" ).await;
    let terminal = work(&c, &t.id).await;
    assert_eq!(terminal.state, WorkState::Terminal { status: TaskStatus::Cancelled });
    assert_eq!(terminal.task.status, TaskStatus::Cancelled);
    assert!(terminal.task.result.is_none());
    assert_eq!(terminal.open_waits, 0);
    assert_eq!(terminal.task.input, t.input);
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg).unwrap();
    assert_eq!(work(&c, &t.id).await, terminal);
    let w = worker("cancel-worker", "inc", 1, 0);
    register(&c, &w).await;
    assert_no_run_for(&commands(&c, &w, vec![]).await, &t.id);
    assert_eq!(work(&c, &t.id).await.task.status, TaskStatus::Cancelled);
}

#[tokio::test]
async fn task_terminal_resolution_does_not_satisfy_failed_immutable_prerequisite() {
    let c = Controller::open_with_config(path(), workflow_config()).unwrap();
    let prerequisite = submit(&c, "dependency-source", workflow_native("dependency-source")).await;
    let mut input = workflow_native("dependency-target");
    input.dependencies.push(prerequisite.id.clone());
    let t = submit(&c, "dependency-target", input).await;
    add(&c, "dependency-wait", &t.id, 0, ResolverRef::Task { task_id: prerequisite.id.clone() }).await;
    let closed = task(call(&c, Operation::CancelTask {
        operation_id: "dependency-cancel".into(), task_id: prerequisite.id.clone(), expected_revision: prerequisite.revision,
    }).await);
    let unblocked = work(&c, &t.id).await;
    assert_eq!(unblocked.open_waits, 0);
    let terminal = match &resolution(&unblocked.waits.items[0]).outcome {
        ResolverOutcome::Task(outcome) => outcome,
        other => panic!("task outcome expected: {other:?}"),
    };
    assert_eq!(terminal.task_id, prerequisite.id);
    assert_eq!(terminal.terminal_task_revision, closed.revision);
    assert_eq!(terminal.status, TerminalTaskStatus::Cancelled);
    assert!(terminal.accepted_result.is_none());
    let w = worker("dependency-worker", "inc", 1, 0);
    register(&c, &w).await;
    assert_no_run_for(&commands(&c, &w, vec![]).await, &t.id);
    let observed = get(&c, &t.id).await;
    assert_eq!(observed.input, t.input);
    assert_eq!(observed.status, TaskStatus::Queued);
    assert!(observed.blocked_reason.unwrap().starts_with("prerequisite:"));
    assert!(observed.result.is_none());
}

#[tokio::test]
async fn wait_resolution_preserves_pending_effect_uncertainty_until_exact_settlement() {
    let c = Controller::open_with_config(path(), workflow_config()).unwrap();
    let t = submit(&c, "effect-submit", workflow_coding("effect-owner")).await;
    let w = worker("effect-worker", "inc", 0, 1);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let cp = Checkpoint {
        registration: fixture_repository_binding(),
        repository: t.input.repository.as_ref().unwrap().checkpoint_remote.clone(),
        commit: "b".repeat(40), remote_ref: checkpoint_ref(&t.id, &a.id, "work-effect"),
        operation_id: "work-effect".into(),
    };
    assert!(matches!(call(&c, Operation::PrepareOperation {
        operation_id: cp.operation_id.clone(), task_id: t.id.clone(), attempt_id: a.id.clone(),
        generation: a.generation, kind: "checkpoint".into(), intended: json!({
            "registration":cp.registration,"repository":cp.repository,"commit":cp.commit,"remote_ref":cp.remote_ref,
        }),
    }).await, Outcome::Ok { .. }));
    let q = pose(&c, "effect-question", &t.id).await;
    add(&c, "effect-wait", &t.id, 0, ResolverRef::Question { question_id: q.id.clone() }).await;
    task(report(&c, &a, "effect-stop", Observation::ProcessTreeStopped).await);
    cancel_question(&c, "effect-resolve", &q, "No decision remains").await;
    assert_no_run_for(&commands(&c, &w, vec![]).await, &t.id);
    let retained = work(&c, &t.id).await;
    assert_eq!(retained.open_waits, 0);
    let stopped = retained.task.attempt.unwrap();
    assert_eq!(stopped.id, a.id);
    assert!(stopped.stopped && stopped.uncertain);
    assert_eq!(retained.task.blocked_reason.as_deref(), Some("reconciliation_required"));
    assert!(matches!(call(&c, Operation::SettleOperation {
        settlement_id: "work-effect-settlement".into(), operation_id: cp.operation_id.clone(),
        task_id: t.id.clone(), attempt_id: a.id.clone(), generation: a.generation,
        outcome: json!({"acknowledged":true}), checkpoint: Some(cp),
    }).await, Outcome::Ok { .. }));
    let next = run(&commands(&c, &w, vec![]).await);
    assert_ne!(next.id, a.id);
    assert_eq!(next.session_id, a.session_id);
    assert_eq!(get(&c, &t.id).await.input, t.input);
}

#[tokio::test]
async fn resolving_wait_does_not_refresh_expired_deadline_or_exhausted_budget() {
    for mode in ["deadline", "budget"] {
        let clock = Arc::new(AtomicU64::new(future() - 300_000));
        let mut cfg = workflow_config();
        let read = clock.clone();
        cfg.clock = Arc::new(move || read.load(Ordering::SeqCst));
        let c = Controller::open_with_config(path(), cfg).unwrap();
        let mut input = workflow_native(mode);
        if mode == "budget" { input.limits.model_requests = 1; }
        let t = submit(&c, "guard-submit", input).await;
        let w = worker("guard-worker", "inc", 1, 0);
        register(&c, &w).await;
        let a = run(&commands(&c, &w, vec![]).await);
        start_and_charge(&c, &a, "guard", 1).await;
        let q = pose(&c, "guard-question", &t.id).await;
        add(&c, "guard-wait", &t.id, 0, ResolverRef::Question { question_id: q.id.clone() }).await;
        task(report(&c, &a, "guard-stop", Observation::ProcessTreeStopped).await);
        if mode == "deadline" { clock.store(t.input.limits.deadline_unix_ms + 1, Ordering::SeqCst); }
        cancel_question(&c, "guard-resolve", &q, "Wait ended").await;
        assert_no_run_for(&commands(&c, &w, vec![]).await, &t.id);
        let observed = work(&c, &t.id).await;
        assert_eq!(observed.open_waits, 0);
        assert_eq!(observed.task.input, t.input);
        assert_eq!(observed.task.attempt.as_ref().unwrap().id, a.id);
        assert_eq!(observed.task.attempt.as_ref().unwrap().deadline_unix_ms, a.deadline_unix_ms);
        assert_eq!(observed.task.status, TaskStatus::Failed);
        assert_eq!(observed.task.blocked_reason.as_deref(), Some(if mode == "deadline" {
            "execution_deadline"
        } else { "model_request_budget_exhausted" }));
        assert!(observed.task.result.is_none());
    }
}

#[tokio::test]

// Traceability scope: A real review resolver closes its exact typed wait; this does not accept the task result.
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//= type=test
//# A collaboration wait MUST identify a concrete task, question or review resolver.

async fn review_rejection_resolves_exact_review_wait_without_task_acceptance() {
    let c = Controller::open_with_config(path(), workflow_config()).unwrap();
    let t = submit(&c, "review-task", workflow_native("review-owner")).await;
    let doc = match call(&c, Operation::MutateDocument { operation_id: "review-doc".into(), mutation: DocumentMutation::Create {
        content: DocumentContent { title: "Review source".into(), body: "Immutable source".into() },
        supplied_content_id: None, responsible_owner: PrincipalRef::TrustedHostOperator,
    }}).await {
        Outcome::Ok { data: Data::DocumentMutation(DocumentMutationOutcome::Document { view }) } => view,
        x => panic!("document expected: {x:?}"),
    };
    let target = ReviewTarget::DocumentVersion { version: doc.version.reference };
    let review = match call(&c, Operation::MutateReview { operation_id: "review-request".into(), mutation: ReviewMutation::Request {
        target: target.clone(), reviewer: PrincipalRef::TrustedHostOperator, summary: "Assess exact document".into(),
    }}).await {
        Outcome::Ok { data: Data::ReviewMutation(value) } => match *value {
            ReviewMutationOutcome::Review(view) => view.review,
        },
        x => panic!("review expected: {x:?}"),
    };
    add(&c, "review-wait", &t.id, 0, ResolverRef::Review { review_id: review.id.clone() }).await;
    let decision = match call(&c, Operation::MutateReview { operation_id: "review-reject".into(), mutation: ReviewMutation::Decide {
        review_id: review.id.clone(), expected_revision: review.revision,
        verdict: ReviewVerdict::Rejected, explanation: "Required condition is absent".into(),
    }}).await {
        Outcome::Ok { data: Data::ReviewMutation(value) } => match *value {
            ReviewMutationOutcome::Review(view) => view.decision.unwrap(),
        },
        x => panic!("review decision expected: {x:?}"),
    };
    let view = work(&c, &t.id).await;
    assert_eq!(view.open_waits, 0);
    assert_eq!(resolution(&view.waits.items[0]).outcome, ResolverOutcome::Review(ReviewResolution::Decided {
        review_id: review.id.clone(), decision_id: decision.id, target, verdict: ReviewVerdict::Rejected,
    }));
    assert_eq!(view.task.input, t.input);
    assert_ne!(view.task.status, TaskStatus::Succeeded);
    assert!(view.task.result.is_none());
    assert_eq!(code(mutate(&c, "review-closed-wait", WorkMutation::AddWait {
        task_id: t.id.clone(), expected_revision: view.coordination_revision,
        resolver: ResolverRef::Review { review_id: review.id },
    }).await.outcome), "resolver_closed");
    assert_eq!(work(&c, &t.id).await, view);
}

#[tokio::test]
async fn wait_capacity_cas_and_operation_conflict_leave_committed_wait_unchanged() {
    let mut cfg = workflow_config();
    Arc::make_mut(&mut cfg.limits).workflow.as_mut().unwrap().waits_per_task = 1;
    let c = Controller::open_with_config(path(), cfg).unwrap();
    let t = submit(&c, "bounds-task", workflow_native("bounds-owner")).await;
    let q1 = pose(&c, "bounds-q-one", &t.id).await;
    let q2 = pose(&c, "bounds-q-two", &t.id).await;
    let first = add(&c, "bounds-first", &t.id, 0, ResolverRef::Question { question_id: q1.id.clone() }).await;
    let before = work(&c, &t.id).await;
    assert_eq!(code(mutate(&c, "bounds-overflow", WorkMutation::AddWait {
        task_id: t.id.clone(), expected_revision: before.coordination_revision,
        resolver: ResolverRef::Question { question_id: q2.id.clone() },
    }).await.outcome), "capacity");
    assert_eq!(work(&c, &t.id).await, before);
    assert_eq!(code(mutate(&c, "bounds-first", WorkMutation::AddWait {
        task_id: t.id.clone(), expected_revision: 0, resolver: ResolverRef::Question { question_id: q2.id },
    }).await.outcome), "operation_conflict");
    let stale = mutate(&c, "bounds-stale", WorkMutation::RemoveWait {
        task_id: t.id.clone(), expected_revision: 0, wait_id: first.changed_wait.unwrap().wait_id,
        reason: "Stale view".into(),
    }).await;
    assert_eq!(code(stale.outcome), "revision_conflict");
    assert_eq!(work(&c, &t.id).await, before);
    assert_eq!(question(&c, &q1.id).await.question.status, QuestionStatus::Open);
}

#[cfg(debug_assertions)]
#[tokio::test]
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//= type=test
//# A requested collaboration wait MUST be committed before initiating the current attempt's quiescence.
// Scope: question, wait and operation receipt rollback. The joined delivery
// suite separately checks the delivery association in this same transaction.
async fn commit_failure_rolls_back_wait_stop_command_and_terminal_resolution() {
    let failed_commit = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut cfg = workflow_config();
    cfg.qualification_fail_next_commit = Some(failed_commit.clone());
    let c = Controller::open_with_config(path(), cfg).unwrap();
    let t = submit(&c, "rollback-task", workflow_native("rollback-owner")).await;
    let w = worker("rollback-worker", "inc", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let q = pose(&c, "rollback-question", &t.id).await;
    let before = work(&c, &t.id).await;
    failed_commit.store(true, Ordering::SeqCst);
    let failed = mutate(&c, "rollback-add", WorkMutation::AddWait {
        task_id: t.id.clone(), expected_revision: 0, resolver: ResolverRef::Question { question_id: q.id.clone() },
    }).await;
    assert!(!failed_commit.load(Ordering::SeqCst), "actual COMMIT failure hook was consumed");
    assert_eq!(code(failed.outcome), "store_unavailable");
    assert!(failed.commit.is_none());
    assert_eq!(work(&c, &t.id).await, before);
    assert!(!commands(&c, &w, vec![]).await.iter().any(|cmd| matches!(cmd,
        WorkerCommand::StopAttempt { attempt_id, .. } if attempt_id == &a.id
    )));
    add(&c, "rollback-add-known", &t.id, 0, ResolverRef::Question { question_id: q.id.clone() }).await;
    let waiting = work(&c, &t.id).await;
    failed_commit.store(true, Ordering::SeqCst);
    let failed = reply(&c, Operation::MutateQuestion { operation_id: "rollback-resolve".into(), mutation: QuestionMutation::Cancel {
        question_id: q.id.clone(), expected_revision: q.revision, reason: "Failed commit".into(),
    }}).await;
    assert!(!failed_commit.load(Ordering::SeqCst));
    assert_eq!(code(failed.outcome), "store_unavailable");
    assert!(failed.commit.is_none());
    assert_eq!(question(&c, &q.id).await.question, q);
    assert_eq!(work(&c, &t.id).await, waiting);
    let lookup = reply(&c, Operation::GetOperation { operation_id: "rollback-resolve".into() }).await;
    assert!(!matches!(lookup.outcome, Outcome::Ok { data: Data::Operation(_) }));
    cancel_question(&c, "rollback-resolve-known", &q, "Known committed resolution").await;
    let ended = work(&c, &t.id).await;
    assert_eq!(ended.open_waits, 0);
    assert_eq!(ended.waits.items.len(), 1);
    assert!(matches!(ended.waits.items[0].status, WaitStatus::Resolved { .. }));
}

fn caller_for(attempt: &Attempt) -> Caller {
    Caller {
        task_id: attempt.task_id.clone(), attempt_id: attempt.id.clone(),
        generation: attempt.generation, worker_id: attempt.worker_id.clone(),
        incarnation: attempt.worker_incarnation.clone(),
    }
}
fn delivery(r: &Reply) -> DeliveryObservation {
    match &r.outcome {
        Outcome::Ok { data: Data::CollaborationDelivery(value) } => value.as_ref().clone(),
        other => panic!("delivery observation expected: {other:?}"),
    }
}
async fn observe_delivery(c: &Controller, caller: &Caller, limit: usize) -> DeliveryObservation {
    let r = reply(c, Operation::ObserveCollaborationDelivery { caller: caller.clone(), limit }).await;
    assert!(r.commit.is_none(), "Observe must not claim input consumption");
    delivery(&r)
}
// Read-only evidence of writes made by actual controller operations. This test
// never inserts a wait, event, delivery state, receipt, admission or ACK directly.
fn input_associations(path: &std::path::Path, task_id: &str) -> Vec<(ResolvedWait, String, Option<serde_json::Value>)> {
    let db = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut statement = db.prepare(
        "SELECT body,attempt_id,input_ack FROM workflow_wait_consumptions WHERE task_id=?1 ORDER BY wait_id,attempt_id"
    ).unwrap();
    let rows = statement.query_map([task_id], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, Option<String>>(2)?)))
        .unwrap().map(|row| {
            let (body, attempt, ack) = row.unwrap();
            (serde_json::from_str(&body).unwrap(), attempt, ack.map(|raw| serde_json::from_str(&raw).unwrap()))
        }).collect();
    rows
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn admitted_continuation_survives_short_scan_and_only_exact_acked_ids_are_suppressed() {
    let p = path();
    let failed_commit = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut cfg = workflow_config();
    let policy = Arc::make_mut(&mut cfg.limits).workflow.as_mut().unwrap();
    policy.page_items = 2;
    policy.delivery_scan_rows = 1;
    cfg.qualification_fail_next_commit = Some(failed_commit.clone());
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    // The first task is oldest for ordinary admission. The first workflow event
    // is unrelated, so its one-row delivery prefix cannot contain either wait.
    let t = submit(&c, "prefix-task", workflow_native("prefix-owner")).await;
    let other = submit(&c, "prefix-other-task", workflow_native("prefix-other-owner")).await;
    let unrelated = pose(&c, "prefix-unrelated-event", &other.id).await;
    let q1 = pose(&c, "prefix-question-one", &t.id).await;
    let q2 = pose(&c, "prefix-question-two", &t.id).await;
    let one = add(&c, "prefix-wait-one", &t.id, 0, ResolverRef::Question { question_id: q1.id.clone() }).await;
    add(&c, "prefix-wait-two", &t.id, one.view.coordination_revision,
        ResolverRef::Question { question_id: q2.id.clone() }).await;
    cancel_question(&c, "prefix-resolve-one", &q1, "First requirement withdrawn").await;
    cancel_question(&c, "prefix-resolve-two", &q2, "Second requirement withdrawn").await;
    let ended = work(&c, &t.id).await;
    assert_eq!(ended.open_waits, 0);
    let mut exact = ended.waits.items.iter().map(|wait| resolution(wait).clone()).collect::<Vec<_>>();
    exact.sort_by(|a, b| a.event_id.cmp(&b.event_id).then(a.wait.wait_id.cmp(&b.wait.wait_id)));
    assert_eq!(exact.len(), 2);
    drop(c);
    controller_owner_released(&p).await;
    assert!(input_associations(&p, &t.id).is_empty());
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let w = worker("prefix-worker", "inc", 1, 0);
    register(&c, &w).await;
    let emitted = commands(&c, &w, vec![]).await;
    let (attempt, continuation) = emitted.iter().find_map(|command| match command {
        WorkerCommand::RunAttempt { attempt, workflow_continuation: Some(continuation), .. }
            if attempt.task_id == t.id => Some((attempt.clone(), continuation.clone())),
        _ => None,
    }).expect("ordinary admission carries the exact required continuation");
    assert_eq!(continuation, WorkflowContinuation {
        task_id: t.id.clone(), session_id: t.session_id.clone(), resolved_waits: exact.clone(),
    });
    drop(c);
    controller_owner_released(&p).await;
    let associations = input_associations(&p, &t.id);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert_eq!(associations.len(), 2);
    for (resolution, admitted, ack) in &associations {
        assert!(exact.contains(resolution));
        assert_eq!(admitted, &attempt.id);
        assert!(ack.is_none(), "a RunAttempt association is not an input ACK");
    }
    task(report(&c, &attempt, "prefix-started", Observation::Started { thread_id: None, turn_id: None }).await);
    let caller = caller_for(&attempt);
    let observed = observe_delivery(&c, &caller, 1).await;
    assert!(observed.pending.is_none());
    assert!(observed.acknowledged_cursor.position.is_none());
    let offered = observed.available.as_ref().unwrap();
    assert_eq!(offered.continuation.as_ref(), Some(&continuation));
    assert!(offered.items.is_empty(), "the scanned first event belongs to the unrelated task");
    assert!(offered.has_more);
    let first_event = match call(&c, Operation::QueryWorkflowActivity { cursor: None, limit: 1 }).await {
        Outcome::Ok { data: Data::WorkflowActivity(page) } => page.items.into_iter().next().unwrap(),
        other => panic!("activity page expected: {other:?}"),
    };
    assert_eq!(first_event.subject, RecordRef::Question { question_id: unrelated.id });
    assert_eq!(offered.to_cursor.position.as_ref(), Some(&first_event.id));
    assert!(exact.iter().all(|resolution| resolution.event_id > first_event.id));
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(input_associations(&p, &t.id), associations);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let prepared = reply(&c, Operation::PrepareCollaborationDelivery {
        operation_id: "prefix-prepare".into(), caller: caller.clone(),
        expected_cursor: observed.acknowledged_cursor.clone(), expected_routing_cursor: observed.routing_cursor.clone(), through: offered.through.clone(), limit: 1,
    }).await;
    let batch = delivery(&prepared).pending.unwrap();
    assert_eq!(batch.page, *offered);
    assert_eq!(batch.page.continuation.as_ref(), Some(&continuation));
    assert!(batch.delivery.is_none());
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(input_associations(&p, &t.id), associations);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let context = "prefix-original-native-context";
    let begun = reply(&c, Operation::BeginCollaborationDelivery {
        operation_id: "prefix-begin".into(), caller: caller.clone(),
        batch_id: batch.batch_id.clone(), context_binding: context.into(),
    }).await;
    let intended = delivery(&begun);
    assert_eq!(intended.acknowledged_cursor, observed.acknowledged_cursor);
    assert_eq!(intended.pending.as_ref().unwrap().page.continuation.as_ref(), Some(&continuation));
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(input_associations(&p, &t.id), associations, "Begin records intent, not input consumption");
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    failed_commit.store(true, Ordering::SeqCst);
    let failed = reply(&c, Operation::AcknowledgeCollaborationDelivery {
        operation_id: "prefix-ack-cut".into(), caller: caller.clone(),
        batch_id: batch.batch_id.clone(), context_binding: context.into(),
    }).await;
    assert!(!failed_commit.load(Ordering::SeqCst), "actual ACK COMMIT hook consumed");
    assert_eq!(code(failed.outcome), "store_unavailable");
    assert!(failed.commit.is_none());
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(input_associations(&p, &t.id), associations);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert_eq!(observe_delivery(&c, &caller, 1).await, intended);
    // This is the controlled host ACK API boundary. Consumer tests separately
    // establish that a real executor retained the corresponding input first.
    let acknowledged = reply(&c, Operation::AcknowledgeCollaborationDelivery {
        operation_id: "prefix-ack-known".into(), caller: caller.clone(),
        batch_id: batch.batch_id.clone(), context_binding: context.into(),
    }).await;
    let settled = delivery(&acknowledged);
    assert!(settled.pending.is_none());
    assert_eq!(settled.acknowledged_cursor, batch.page.to_cursor);
    drop(c);
    controller_owner_released(&p).await;
    let acknowledged_rows = input_associations(&p, &t.id);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert_eq!(acknowledged_rows.len(), associations.len());
    for ((resolution, admitted, ack), (original, original_attempt, _)) in acknowledged_rows.iter().zip(&associations) {
        assert_eq!(resolution, original);
        assert_eq!(admitted, original_attempt);
        assert_eq!(ack.as_ref(), Some(&json!({"caller":caller,"context_binding":context})));
    }
    // This independent, directly addressed question is created after the
    // continuation ACK. Suppression of exact terminal IDs must preserve it.
    let mut followup_input = pose_input(&t.id);
    followup_input.recipient = PrincipalRef::Agent { agent_id: t.assigned_agent_id.clone() };
    let followup = question_mutation(&c, "prefix-independent-followup", QuestionMutation::Pose {
        question: followup_input,
    }).await.question.question;
    // WaitResolved siblings remain canonical audit events, but sending them to
    // this waiter would repeat outcomes already retained in its continuation.
    let mut cursor = None;
    let mut sibling_ids = vec![];
    let mut followup_ids = vec![];
    for _ in 0..32 {
        let page = match call(&c, Operation::QueryWorkflowActivity { cursor: cursor.take(), limit: 2 }).await {
            Outcome::Ok { data: Data::WorkflowActivity(page) } => page,
            other => panic!("activity expected: {other:?}"),
        };
        sibling_ids.extend(page.items.iter().filter_map(|event| match &event.detail {
            ActivityDetail::Wait(WaitChange::Resolved { wait, .. }) if wait.task_id == t.id => Some(event.id.clone()),
            _ => None,
        }));
        followup_ids.extend(page.items.iter().filter_map(|event| match &event.detail {
            ActivityDetail::Question(QuestionChange::Posed { .. })
                if event.subject == (RecordRef::Question { question_id: followup.id.clone() }) => Some(event.id.clone()),
            _ => None,
        }));
        cursor = page.next_cursor;
        if cursor.is_none() { break; }
    }
    assert!(cursor.is_none());
    assert_eq!(sibling_ids.len(), 2);
    assert!(sibling_ids.iter().all(|id| exact.iter().any(|r| r.event_id.product_revision == id.product_revision && r.event_id != *id)));
    assert_eq!(followup_ids.len(), 1);
    assert!(exact.iter().all(|resolution| resolution.event_id < followup_ids[0]));
    // This pose is the last workflow mutation. Routing uses an independent
    // cursor and may repeat empty scans after a complete pass, so completion
    // means both finite scans reached this exact head, not available == None.
    let drain_through = followup_ids[0].clone();
    let mut seen = vec![];
    let mut ordinary_pages = 0;
    let mut routing_pages = 0;
    let mut drained = false;
    for index in 0..64 {
        let observed = observe_delivery(&c, &caller, 1).await;
        assert!(observed.pending.is_none());
        let Some(page) = observed.available else {
            assert_eq!(observed.acknowledged_cursor.position.as_ref(), Some(&drain_through));
            assert!(observed.routing_cursor.is_none());
            drained = true; break;
        };
        assert_eq!(page.through, drain_through, "both bounded scans use the same fixed committed head");
        assert!(page.continuation.is_none(), "ACKed required input is not offered again");
        let input = page.items.iter().chain(page.routing.iter().flat_map(|route| route.items.iter()));
        let input = input.collect::<Vec<_>>();
        assert!(input.len() <= 1, "the original one-item page bound applies to both lanes");
        assert!(input.iter().all(|event| !exact.iter().any(|r| r.event_id == event.id)));
        assert!(input.iter().all(|event| !sibling_ids.contains(&event.id)),
            "wait audit events cannot repeat the acknowledged terminal input in either lane");
        seen.extend(input.iter().map(|event| event.id.clone()));
        let prepared = reply(&c, Operation::PrepareCollaborationDelivery {
            operation_id: format!("prefix-drain-prepare-{index}"), caller: caller.clone(),
            expected_cursor: observed.acknowledged_cursor.clone(), expected_routing_cursor: observed.routing_cursor.clone(), through: page.through.clone(), limit: 1,
        }).await;
        let pending = delivery(&prepared).pending.unwrap();
        assert_eq!(pending.page, page);
        let context = format!("prefix-drain-context-{index}");
        if pending.page.has_input() {
            let begun = delivery(&reply(&c, Operation::BeginCollaborationDelivery {
                operation_id: format!("prefix-drain-begin-{index}"), caller: caller.clone(),
                batch_id: pending.batch_id.clone(), context_binding: context.clone(),
            }).await);
            let intent = begun.pending.as_ref().unwrap().delivery.as_ref().unwrap();
            assert_eq!(intent.caller, caller);
            assert_eq!(intent.context_binding, context);
        }
        let acknowledged = delivery(&reply(&c, Operation::AcknowledgeCollaborationDelivery {
            operation_id: format!("prefix-drain-ack-{index}"), caller: caller.clone(),
            batch_id: pending.batch_id, context_binding: context,
        }).await);
        assert!(acknowledged.pending.is_none());
        assert_eq!(acknowledged.acknowledged_cursor, pending.page.to_cursor);
        if let Some(route) = &pending.page.routing {
            routing_pages += 1;
            assert_eq!(pending.page.from_cursor, pending.page.to_cursor,
                "routing ACK leaves the ordinary scan cursor unchanged");
            assert_eq!(route.from_cursor, observed.routing_cursor);
            assert_eq!(acknowledged.routing_cursor, route.to_cursor);
            assert!(!acknowledged.routing_turn);
            if let Some(to) = &route.to_cursor {
                assert!(route.has_more);
                assert!(route.from_cursor.as_ref().is_none_or(|from| to > from),
                    "the bounded routing scan advances in original event order");
            } else {
                assert!(!route.has_more, "None completes the routing scan through the fixed head");
                assert!(route.from_cursor.is_some() || pending.page.has_input(),
                    "an empty scan cannot report progress without a cursor change");
                if acknowledged.acknowledged_cursor.position.as_ref() == Some(&drain_through) {
                    drained = true; break;
                }
            }
        } else {
            ordinary_pages += 1;
            assert!(pending.page.to_cursor.position > pending.page.from_cursor.position,
                "the bounded ordinary scan advances in original event order");
            assert_eq!(acknowledged.routing_cursor, observed.routing_cursor);
            assert!(acknowledged.routing_turn);
        }
    }
    assert!(ordinary_pages > 0 && routing_pages > 0, "both independent cursor lanes were exercised");
    assert!(drained, "both bounded scans reach the same fixed committed event head");
    for id in sibling_ids { assert!(!seen.contains(&id), "audit-only unblock input was delivered twice"); }
    for id in followup_ids { assert!(seen.contains(&id), "exact terminal suppression swallowed independent targeted activity"); }
    let unique = seen.iter().collect::<std::collections::BTreeSet<_>>();
    assert_eq!(unique.len(), seen.len());
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(input_associations(&p, &t.id), acknowledged_rows);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert_eq!(get(&c, &t.id).await.input, t.input);
}

async fn work_actor(c: &Controller, agent: &str) -> (Task, Attempt, Caller) {
    let t = submit(c, &format!("{agent}-actor-task"), workflow_native(agent)).await;
    let w = worker(&format!("{agent}-actor-worker"), "inc", 1, 0);
    register(c, &w).await;
    let a = run(&commands(c, &w, vec![]).await);
    assert_eq!(a.task_id, t.id);
    task(report(c, &a, &format!("{agent}-actor-start"), Observation::Started { thread_id: None, turn_id: None }).await);
    let caller = caller_for(&a);
    (t, a, caller)
}
async fn work_rpc_result(c: &Controller, caller: &Caller, id: &str, tool: &str, arguments: serde_json::Value) -> borg_runtime::mcp::CallResult {
    let invocation = borg_runtime::mcp::Invocation {
        operation_id: id.into(), logical_call_id: format!("logical-{id}"),
        caller: caller.clone(), tool: tool.into(), arguments: arguments.clone(),
    };
    let body = serde_json::to_vec(&json!({
        "jsonrpc":"2.0", "id":7, "method":"tools/call", "params":{
            "name":tool, "arguments":arguments,
            "_meta":{"borg":{"operation_id":id,"logical_call_id":format!("logical-{id}")}},
        },
    })).unwrap();
    let header = serde_json::to_string(caller).unwrap();
    let (status, body) = borg_runtime::mcp::handle(c, Some(&header), Some(borg_runtime::mcp::VERSION), &body).await;
    assert_eq!(status, 200);
    let rpc: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let raw = rpc["result"]["content"][0]["text"].as_str().unwrap_or_else(|| panic!("MCP tool result expected: {rpc}"));
    let result: borg_runtime::mcp::CallResult = serde_json::from_str(raw).unwrap();
    result.reply.validate_for(&invocation.operation()).unwrap();
    result
}
async fn work_rpc(c: &Controller, caller: &Caller, id: &str, tool: &str, arguments: serde_json::Value) -> Reply {
    work_rpc_result(c, caller, id, tool, arguments).await.reply
}

#[tokio::test]
async fn v2_mcp_work_mutations_require_current_record_owner_and_current_caller() {
    let c = Controller::open_with_config(path(), workflow_config()).unwrap();
    let (_owner_task, owner_attempt, owner) = work_actor(&c, "mcp-work-owner").await;
    let (foreign_task, _foreign_attempt, foreign) = work_actor(&c, "mcp-work-foreign").await;
    let t = submit(&c, "mcp-work-record", workflow_native("mcp-work-owner")).await;
    let q = pose(&c, "mcp-work-question", &t.id).await;
    let before = work(&c, &t.id).await;
    let read = work_rpc(&c, &owner, "mcp-work-get", "collab_task_coordination_get", json!({"task_id":t.id})).await;
    assert!(matches!(read.outcome, Outcome::Ok { data: Data::WorkQuery(WorkQueryResult::Task(ref view)) } if view == &before));
    let wait_args = json!({"task_id":t.id,"expected_revision":0,"resolver":{"kind":"question","question_id":q.id}});
    assert_eq!(code(work_rpc(&c, &foreign, "mcp-work-foreign-wait", "collab_task_wait", wait_args.clone()).await.outcome), "not_owner");
    assert_eq!(work(&c, &t.id).await, before);
    let added_call = work_rpc_result(&c, &owner, "mcp-work-add", "collab_task_wait", wait_args).await;
    assert!(!added_call.requires_quiescence, "waiting another owned task must not stop this calling session");
    let added = work_change(&added_call.reply);
    let recorded = &added.view.waits.items[0];
    assert_eq!(recorded.created_by.principal, PrincipalRef::Agent { agent_id: "mcp-work-owner".into() });
    assert_eq!(recorded.created_by.provenance, ActorProvenance::BoundAgentSession {
        agent_id: "mcp-work-owner".into(), session_id: owner_attempt.session_id,
        task_id: owner.task_id.clone(), attempt_id: owner.attempt_id.clone(), generation: owner.generation,
        worker_id: owner.worker_id.clone(), incarnation: owner.incarnation.clone(),
    });
    let changed = work_change(&work_rpc(&c, &owner, "mcp-work-transfer", "collab_task_owner", json!({
        "task_id":t.id,"expected_revision":added.view.coordination_revision,
        "owner":{"kind":"agent","agent_id":"mcp-work-foreign"},
    })).await);
    assert_eq!(changed.view.task.input, t.input);
    assert_eq!(changed.view.task.assigned_agent_id, t.assigned_agent_id);
    assert_eq!(question(&c, &q.id).await.question, q);
    let remove_args = json!({"task_id":t.id,"expected_revision":changed.view.coordination_revision,
        "wait_id":added.changed_wait.unwrap().wait_id,"reason":"Current record owner removes the wait"});
    assert_eq!(code(work_rpc(&c, &owner, "mcp-work-retired-owner", "collab_task_unwait", remove_args.clone()).await.outcome), "not_owner");
    let mut stale = foreign.clone();
    stale.generation += 1;
    assert_eq!(code(work_rpc(&c, &stale, "mcp-work-stale-generation", "collab_task_unwait", remove_args.clone()).await.outcome), "stale_generation");
    stale = foreign.clone();
    stale.incarnation = "other-incarnation".into();
    assert_eq!(code(work_rpc(&c, &stale, "mcp-work-stale-incarnation", "collab_task_unwait", remove_args.clone()).await.outcome), "stale_owner");
    assert_eq!(work(&c, &t.id).await, changed.view);
    let removed_reply = work_rpc(&c, &foreign, "mcp-work-remove", "collab_task_unwait", remove_args.clone()).await;
    let removed = work_change(&removed_reply);
    assert_eq!(removed.view.open_waits, 0);
    let listed = work_rpc(&c, &foreign, "mcp-work-waits", "collab_task_waits", json!({"task_id":t.id,"after":null,"limit":1})).await;
    assert!(matches!(listed.outcome, Outcome::Ok { data: Data::WorkQuery(WorkQueryResult::Waits(ref page)) }
        if page.items == removed.view.waits.items && page.next_cursor.is_none()));
    let current = get(&c, &foreign_task.id).await;
    task(call(&c, Operation::CancelTask { operation_id: "mcp-work-cancel-caller".into(), task_id: foreign_task.id, expected_revision: current.revision }).await);
    let retired = work_rpc(&c, &foreign, "mcp-work-retired-caller", "collab_task_owner", json!({
        "task_id":t.id,"expected_revision":removed.view.coordination_revision,"owner":{"kind":"trusted_host_operator"},
    })).await;
    assert!(matches!(retired.outcome, Outcome::Refused { .. }));
    assert_eq!(work(&c, &t.id).await, removed.view);
    let replay = work_rpc(&c, &foreign, "mcp-work-remove", "collab_task_unwait", remove_args).await;
    assert_eq!(replay.outcome, removed_reply.outcome);
    assert_eq!(replay.commit, removed_reply.commit);
}

#[tokio::test]
async fn v2_mcp_successful_self_wait_requests_quiescence_and_replays_original_receipt() {
    let c = Controller::open_with_config(path(), workflow_config()).unwrap();
    let (t, attempt, caller) = work_actor(&c, "mcp-self-wait").await;
    let q = pose(&c, "mcp-self-question", &t.id).await;
    let arguments = json!({"task_id":t.id,"expected_revision":0,
        "resolver":{"kind":"question","question_id":q.id}});
    let original = work_rpc_result(&c, &caller, "mcp-self-wait-original", "collab_task_wait", arguments.clone()).await;
    assert!(original.requires_quiescence, "successful self-wait must stop the calling execution");
    let changed = work_change(&original.reply);
    assert_eq!(changed.view.task.id, caller.task_id);
    assert_eq!(changed.view.open_waits, 1);
    assert!(changed.changed_wait.is_some());
    let retained = changed.view.task.attempt.as_ref().unwrap();
    assert_eq!(retained.id, attempt.id);
    assert_eq!(retained.phase, AttemptPhase::StopRequested);
    assert!(!retained.stopped);
    let before_replay = work(&c, &t.id).await;
    let replay = work_rpc_result(&c, &caller, "mcp-self-wait-original", "collab_task_wait", arguments).await;
    assert!(replay.requires_quiescence);
    assert_eq!(replay.reply.outcome, original.reply.outcome);
    assert_eq!(replay.reply.commit, original.reply.commit);
    assert_eq!(work(&c, &t.id).await, before_replay);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//= type=test
//# A wait that would close a cycle through waits and parent links MUST be refused.
async fn transitive_wait_cycle_refuses_without_changing_tasks_waits_or_commands_and_replays_exactly() {
    // Capture only the original component domain rows, after releasing the owner.
    // A recorded refusal may add its own receipt; it cannot alter these rows.
    let projection = |path: &std::path::Path| -> Vec<Vec<String>> {
        let db = rusqlite::Connection::open_with_flags(
            path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        ).unwrap();
        [
            "SELECT json_array(id,status,active,body) FROM tasks ORDER BY id",
            "SELECT json_array(id,task_id,resolver_kind,resolver_id,closed,body) FROM workflow_waits ORDER BY id",
            "SELECT json_array(task_id,revision,body) FROM workflow_task_state ORDER BY task_id",
            "SELECT json_array(id,acknowledged,body,delivery_operation_id) FROM commands ORDER BY id",
        ].into_iter().map(|sql| {
            let mut statement = db.prepare(sql).unwrap();
            let rows = statement.query_map([], |row| row.get::<_, String>(0)).unwrap()
                .collect::<Result<Vec<_>, _>>().unwrap();
            rows
        }).collect()
    };
    let p = path();
    let cfg = workflow_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let first = submit(&c, "transitive-first", workflow_native("transitive-first-owner")).await;
    let middle = submit(&c, "transitive-middle", workflow_native("transitive-middle-owner")).await;
    let last = submit(&c, "transitive-last", workflow_native("transitive-last-owner")).await;
    add(&c, "transitive-first-middle", &first.id, 0,
        ResolverRef::Task { task_id: middle.id.clone() }).await;
    add(&c, "transitive-middle-last", &middle.id, 0,
        ResolverRef::Task { task_id: last.id.clone() }).await;
    let before = [work(&c, &first.id).await, work(&c, &middle.id).await, work(&c, &last.id).await];
    assert_eq!([before[0].open_waits, before[1].open_waits, before[2].open_waits], [1, 1, 0]);
    assert_eq!(before[0].waits.items[0].resolver, ResolverRef::Task { task_id: middle.id.clone() });
    assert_eq!(before[1].waits.items[0].resolver, ResolverRef::Task { task_id: last.id.clone() });
    let closing = Operation::MutateWork {
        operation_id: "transitive-last-first".into(),
        mutation: WorkMutation::AddWait {
            task_id: last.id.clone(), expected_revision: before[2].coordination_revision,
            resolver: ResolverRef::Task { task_id: first.id.clone() },
        },
    };
    drop(c);
    controller_owner_released(&p).await;
    let original_rows = projection(&p);
    assert_eq!(original_rows[0].len(), 3);
    assert_eq!(original_rows[1].len(), 2);
    assert!(original_rows[3].is_empty(), "no worker or execution command belongs to this fixture");

    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    // The proposed edge must walk both existing edges to reach its own task.
    let refused = reply(&c, closing.clone()).await;
    assert_eq!(code(refused.outcome.clone()), "cycle");
    assert_eq!([work(&c, &first.id).await, work(&c, &middle.id).await, work(&c, &last.id).await], before);
    let lookup = reply(&c, Operation::GetOperation { operation_id: "transitive-last-first".into() }).await;
    let record = match &lookup.outcome {
        Outcome::Ok { data: Data::Operation(record) } => record,
        other => panic!("original cycle refusal receipt expected: {other:?}"),
    };
    assert_eq!(serde_json::from_value::<Outcome>(record.outcome.clone().unwrap()).unwrap(), refused.outcome);
    assert_eq!(lookup.commit, refused.commit);
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(projection(&p), original_rows);

    let c = Controller::open_with_config(&p, cfg).unwrap();
    let replay = reply(&c, closing).await;
    assert_eq!(replay.outcome, refused.outcome);
    assert_eq!(replay.commit, refused.commit);
    let replay_lookup = reply(&c, Operation::GetOperation { operation_id: "transitive-last-first".into() }).await;
    assert_eq!(replay_lookup.outcome, lookup.outcome);
    assert_eq!(replay_lookup.commit, lookup.commit);
    assert_eq!([work(&c, &first.id).await, work(&c, &middle.id).await, work(&c, &last.id).await], before);
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(projection(&p), original_rows);
}
