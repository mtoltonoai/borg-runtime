//! Real controller transactions and original receipts; no executor/provider work.
//! Retirement gates new selections while retaining accepted tasks and their records.
use super::*;
use borg_runtime::collaboration::{Query, QueryResult};

async fn directory_reply(c: &Controller, command: Operation) -> Reply {
    let reply = Box::pin(c.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: format!("directory-request-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
        command: command.clone(),
    })).await;
    reply.validate_for(&command).unwrap();
    reply
}

fn agent(reply: &Reply) -> AgentSummary {
    match &reply.outcome {
        Outcome::Ok { data: Data::Agent(value) } => value.clone(),
        other => panic!("agent summary expected: {other:?}"),
    }
}

async fn selected(c: &Controller, id: &str) -> AgentSummary {
    agent(&directory_reply(c, Operation::GetAgent { agent_id: id.into() }).await)
}

async fn agents(c: &Controller, after: Option<String>, limit: usize) -> Vec<AgentSummary> {
    let reply = directory_reply(c, Operation::ListAgents { after, limit }).await;
    assert!(reply.commit.is_none());
    match reply.outcome {
        Outcome::Ok { data: Data::Agents(values) } => values,
        other => panic!("directory page expected: {other:?}"),
    }
}

async fn query(c: &Controller, query: Query) -> QueryResult {
    let reply = directory_reply(c, Operation::QueryCollaboration { query }).await;
    assert!(reply.commit.is_none());
    match reply.outcome {
        Outcome::Ok { data: Data::Collaboration(value) } => value,
        other => panic!("collaboration read expected: {other:?}"),
    }
}

fn registration(operation_id: &str, definition: &Definition) -> Operation {
    Operation::RegisterAgent { operation_id: operation_id.into(), definition: definition.clone() }
}

fn retirement(operation_id: &str, definition: &Definition) -> Operation {
    Operation::RetireAgent { operation_id: operation_id.into(), agent_id: definition.id.clone(),
        expected_revision: definition.revision.clone() }
}

async fn exact_replay(c: &Controller, command: &Operation, original: &Reply) {
    let replay = directory_reply(c, command.clone()).await;
    assert_eq!(replay.outcome, original.outcome);
    assert_eq!(replay.commit, original.commit, "replay retains the original operation revision");
    assert_eq!(serde_json::to_vec(&replay.outcome).unwrap(), serde_json::to_vec(&original.outcome).unwrap());
}

#[tokio::test]
//= spec/impl/v2/implementation.md#agent-identity-lifecycle
//= type=test
//# The controller MUST support explicit agent registration without creating a task.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# An agent identity MUST persist independently of any executor process.
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# Reusing a mutating request's identity with different input MUST fail without changing its recorded outcome.
async fn registration_is_taskless_and_replays_without_replacing_an_existing_identity() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let definition = native("directory-registered").definition;
    let command = registration("directory-register", &definition);
    let original = directory_reply(&c, command.clone()).await;
    assert!(original.commit.is_some());
    assert_eq!(agent(&original).definition, definition);
    assert!(!agent(&original).retired);
    assert_eq!(agents(&c, None, 10).await, vec![agent(&original)]);
    assert!(matches!(call(&c, Operation::ListTasks { after: None, limit: 10 }).await,
        Outcome::Ok { data: Data::Tasks(values) } if values.is_empty()));
    assert!(matches!(call(&c, Operation::ListAgentTasks { agent_id: definition.id.clone(), after: None, limit: 10 }).await,
        Outcome::Ok { data: Data::Tasks(values) } if values.is_empty()));
    assert!(matches!(query(&c, Query::Sessions { agent_id: None, after: None, limit: 10 }).await,
        QueryResult::Sessions(page) if page.items.is_empty() && page.next_cursor.is_none()));
    exact_replay(&c, &command, &original).await;
    let mut other = definition.clone();
    other.revision = "revision-2".into();
    assert_eq!(code(directory_reply(&c, registration("directory-register", &other)).await.outcome), "operation_conflict");
    for (id, body) in [("directory-duplicate", &definition), ("directory-replace", &other)] {
        assert_eq!(code(directory_reply(&c, registration(id, body)).await.outcome), "already_exists");
    }
    assert_eq!(selected(&c, &definition.id).await, agent(&original));
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, config()).unwrap();
    exact_replay(&c, &command, &original).await;
    assert_eq!(selected(&c, &definition.id).await, agent(&original));
    assert!(matches!(query(&c, Query::Sessions { agent_id: None, after: None, limit: 10 }).await,
        QueryResult::Sessions(page) if page.items.is_empty()));
}

#[tokio::test]
//= spec/impl/v2/implementation.md#agent-identity-lifecycle
//= type=test
//# Retiring an agent identity MUST remove it from the active agent directory.
//= spec/impl/v2/implementation.md#agent-identity-lifecycle
//= type=test
//# A retired agent identity MUST remain resolvable for historical reads.
async fn retirement_keeps_a_directory_cursor_and_never_resurrects_the_identity() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let mut definitions = vec![];
    for id in ["directory-a", "directory-b", "directory-c"] {
        let definition = native(id).definition;
        let reply = directory_reply(&c, registration(&format!("register-{id}"), &definition)).await;
        assert_eq!(agent(&reply).definition, definition);
        definitions.push(definition);
    }
    let first = agents(&c, None, 1).await;
    assert_eq!(first[0].id, "directory-a");
    let original_register = directory_reply(&c, registration("register-directory-a", &definitions[0])).await;
    let stale = Operation::RetireAgent { operation_id: "retire-stale".into(), agent_id: first[0].id.clone(),
        expected_revision: "not-the-selected-revision".into() };
    let refused = directory_reply(&c, stale.clone()).await;
    assert_eq!(code(refused.outcome.clone()), "configuration_conflict");
    assert!(!selected(&c, &first[0].id).await.retired);
    let command = retirement("retire-directory-a", &definitions[0]);
    let retired = directory_reply(&c, command.clone()).await;
    assert!(agent(&retired).retired);
    assert_eq!(agent(&retired).definition, definitions[0]);
    assert_eq!(selected(&c, &first[0].id).await, agent(&retired));
    let next = agents(&c, Some(first[0].id.clone()), 1).await;
    assert_eq!(next[0].id, "directory-b");
    assert_eq!(agents(&c, None, 10).await.iter().map(|a| a.id.as_str()).collect::<Vec<_>>(),
        vec!["directory-b", "directory-c"]);
    exact_replay(&c, &command, &retired).await;
    // The old registration remains an original active-summary receipt, not a
    // current directory read and not permission to resurrect its retired ID.
    exact_replay(&c, &registration("register-directory-a", &definitions[0]), &original_register).await;
    exact_replay(&c, &stale, &refused).await;
    assert_eq!(code(directory_reply(&c, retirement("retire-again", &definitions[0])).await.outcome), "agent_retired");
    let mut replacement = definitions[0].clone();
    replacement.revision = "replacement-revision".into();
    for (id, body) in [("reregister-same", &definitions[0]), ("reregister-new", &replacement)] {
        assert_eq!(code(directory_reply(&c, registration(id, body)).await.outcome), "agent_retired");
    }
    assert_eq!(code(directory_reply(&c, Operation::RetireAgent {
        operation_id: "retire-directory-a".into(), agent_id: definitions[1].id.clone(),
        expected_revision: definitions[1].revision.clone(),
    }).await.outcome), "operation_conflict");
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, config()).unwrap();
    exact_replay(&c, &command, &retired).await;
    exact_replay(&c, &registration("register-directory-a", &definitions[0]), &original_register).await;
    exact_replay(&c, &stale, &refused).await;
    assert_eq!(selected(&c, &first[0].id).await, agent(&retired));
    assert_eq!(agents(&c, Some(first[0].id.clone()), 1).await[0].id, "directory-b");
    assert_eq!(code(directory_reply(&c, registration("reregister-after-restart", &replacement)).await.outcome), "agent_retired");
}

#[tokio::test]
//= spec/impl/v2/implementation.md#agent-identity-lifecycle
//= type=test
//# A retired agent identity MUST be refused for new work.
//= spec/impl/v2/implementation.md#agent-identity-lifecycle
//= type=test
//# Agent retirement MUST preserve retained historical records.
//= spec/impl/v2/implementation.md#agent-identity-lifecycle
//= type=test
//# Agent retirement MUST NOT cancel already accepted work.
//= spec/impl/v2/implementation.md#agent-identity-lifecycle
//= type=test
//# Task submission MUST NOT reactivate a retired agent identity.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# Every session MUST carry the agent identity it runs as.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# An agent's memories MUST persist across every session of that agent identity.
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# An exact retry of a committed mutating request MUST return its recorded outcome.
async fn retirement_preserves_accepted_sessions_memory_history_and_original_receipts_on_restart() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let input = native("directory-history");
    let registered = registration("history-register", &input.definition);
    let register_receipt = directory_reply(&c, registered.clone()).await;
    let submit_command = Operation::SubmitTask { operation_id: "history-submit".into(), input: input.clone() };
    let submit_receipt = directory_reply(&c, submit_command.clone()).await;
    let accepted = task(submit_receipt.outcome.clone());
    let queued = submit(&c, "history-second-session", input.clone()).await;
    assert_ne!(accepted.session_id, queued.session_id);
    let sessions_query = Query::Sessions { agent_id: Some(input.definition.id.clone()), after: None, limit: 10 };
    let sessions = query(&c, sessions_query.clone()).await;
    let QueryResult::Sessions(page) = &sessions else { panic!("session page expected") };
    assert_eq!(page.items.len(), 2);
    assert!(page.next_cursor.is_none());
    assert!(page.items.iter().all(|session| session.agent_id == input.definition.id
        && session.definition == input.definition));
    let w = worker("directory-history-worker", "incarnation-1", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    assert_eq!(a.task_id, accepted.id);
    task(report(&c, &a, "history-started", Observation::Started { thread_id: None, turn_id: None }).await);
    // The command poll also inspected the second task and committed one
    // admission_deferred revision while this worker's sole slot became occupied.
    // Compare retirement against that actual post-setup state, not its older
    // submission receipt; no accepted input, session or attempt may change.
    let mut expected_queued = queued.clone();
    expected_queued.revision += 1;
    let queued = get(&c, &queued.id).await;
    assert_eq!(queued, expected_queued);
    assert_eq!(queued.status, TaskStatus::Queued);
    assert!(queued.attempt.is_none());
    assert_eq!(queued.blocked_reason.as_deref(), Some("executor_capacity"));
    let write = |operation_id: &str, expected_revision, value| Operation::SetMemory {
        operation_id: operation_id.into(), task_id: accepted.id.clone(), attempt_id: a.id.clone(),
        generation: a.generation, key: "retained-key".into(), expected_revision, value,
    };
    let memory_command = write("history-memory-before", None, json!({"revision":"before retirement"}));
    let memory_receipt = directory_reply(&c, memory_command.clone()).await;
    assert!(matches!(&memory_receipt.outcome, Outcome::Ok { data: Data::Memory(Some(value)) }
        if value.revision == 1 && value.agent_id == input.definition.id));
    let current = get(&c, &accepted.id).await;
    let history_query = Query::TaskHistory { task_id: accepted.id.clone(), after: None, limit: 10 };
    let history_before = query(&c, history_query.clone()).await;
    let retire_command = retirement("history-retire", &input.definition);
    let retired = directory_reply(&c, retire_command.clone()).await;
    assert!(agent(&retired).retired);
    assert_eq!(get(&c, &accepted.id).await, current, "retirement must not cancel accepted work");
    assert_eq!(get(&c, &queued.id).await, queued);
    assert_eq!(query(&c, sessions_query.clone()).await, sessions);
    assert_eq!(query(&c, history_query.clone()).await, history_before);
    assert!(agents(&c, None, 10).await.is_empty());

    assert_eq!(code(directory_reply(&c, Operation::SubmitTask {
        operation_id: "history-new-submit".into(), input: input.clone(),
    }).await.outcome), "agent_retired");
    // Re-selecting the already accepted identity creates no new work or session,
    // even after retirement. A genuinely new assignment still refuses.
    let no_op = Operation::AssignTask {
        operation_id: "history-same-agent-noop".into(), task_id: queued.id.clone(),
        expected_revision: queued.revision, agent_id: input.definition.id.clone(),
    };
    let no_op_receipt = directory_reply(&c, no_op.clone()).await;
    assert_eq!(task(no_op_receipt.outcome.clone()), queued);
    exact_replay(&c, &no_op, &no_op_receipt).await;
    let other_input = native("directory-history-other");
    let other_registered = directory_reply(&c, registration("history-register-other", &other_input.definition)).await;
    assert_eq!(agent(&other_registered).definition, other_input.definition);
    let other = submit(&c, "history-other-task", other_input).await;
    assert_eq!(code(directory_reply(&c, Operation::AssignTask {
        operation_id: "history-new-assignment".into(), task_id: other.id.clone(),
        expected_revision: other.revision, agent_id: input.definition.id.clone(),
    }).await.outcome), "agent_retired");
    assert_eq!(get(&c, &other.id).await, other);
    assert_eq!(code(directory_reply(&c, Operation::UpdateDefinition {
        operation_id: "history-new-definition".into(), update: DefinitionUpdate {
            agent_id: input.definition.id.clone(), expected_revision: input.definition.revision.clone(),
            expected_global_revision: c.limits().revision.clone(),
            expected_policy_revision: c.limits().definition_updates.as_ref().unwrap().revision.clone(),
            changes: std::collections::BTreeMap::from([(
                borg_runtime::limits::DefinitionLimitField::NativeContextBytes,
                c.limits().execution.native.context_bytes + 1,
            )]),
        },
    }).await.outcome), "agent_retired");
    assert_eq!(get(&c, &queued.id).await, queued);
    assert_eq!(query(&c, sessions_query.clone()).await, sessions, "refusals create no new session");

    // Accepted session authority continues to use its original attempt and
    // memory CAS; no new session or executor work is manufactured here.
    let later_command = write("history-memory-after", Some(1), json!({"revision":"after retirement"}));
    let later_receipt = directory_reply(&c, later_command.clone()).await;
    assert!(matches!(&later_receipt.outcome, Outcome::Ok { data: Data::Memory(Some(value)) }
        if value.revision == 2 && value.agent_id == input.definition.id));
    let memory_query = Operation::GetMemory { agent_id: input.definition.id.clone(), key: "retained-key".into() };
    assert_eq!(directory_reply(&c, memory_query.clone()).await.outcome, later_receipt.outcome);
    let current = get(&c, &accepted.id).await;
    task(call(&c, Operation::CancelTask { operation_id: "history-cancel".into(),
        task_id: current.id.clone(), expected_revision: current.revision }).await);
    let stopped = task(report(&c, &a, "history-stopped", Observation::ProcessTreeStopped).await);
    assert_eq!(stopped.status, TaskStatus::Cancelled);
    assert!(stopped.attempt.as_ref().unwrap().stopped);
    // Verified stop releases capacity. This read recomputes only the queued
    // diagnostic; it does not assign an attempt or advance its durable revision.
    let mut expected_queued_after_stop = queued.clone();
    expected_queued_after_stop.blocked_reason = None;
    let queued_after_stop = get(&c, &queued.id).await;
    assert_eq!(queued_after_stop, expected_queued_after_stop);
    let history = query(&c, history_query.clone()).await;
    let QueryResult::TaskHistory(page) = &history else { panic!("task history expected") };
    assert!(!page.items.is_empty());
    assert!(page.next_cursor.is_none(), "fixture compares the complete bounded history");
    let attempts_query = Query::TaskAttempts { task_id: accepted.id.clone(), after: None, limit: 10 };
    let attempts = query(&c, attempts_query.clone()).await;
    let tasks_query = Operation::ListAgentTasks { agent_id: input.definition.id.clone(), after: None, limit: 10 };
    let historical_tasks = directory_reply(&c, tasks_query.clone()).await.outcome;
    assert!(matches!(&historical_tasks, Outcome::Ok { data: Data::Tasks(values) } if values.len() == 2));
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(selected(&c, &input.definition.id).await, agent(&retired));
    assert_eq!(get(&c, &accepted.id).await, stopped);
    assert_eq!(get(&c, &queued.id).await, queued_after_stop);
    assert_eq!(query(&c, sessions_query).await, sessions);
    assert_eq!(query(&c, history_query).await, history);
    assert_eq!(query(&c, attempts_query).await, attempts);
    assert_eq!(directory_reply(&c, tasks_query).await.outcome, historical_tasks);
    assert_eq!(directory_reply(&c, memory_query).await.outcome, later_receipt.outcome);
    for (command, original) in [(&registered, &register_receipt), (&submit_command, &submit_receipt),
        (&memory_command, &memory_receipt), (&later_command, &later_receipt), (&retire_command, &retired)] {
        exact_replay(&c, command, original).await;
    }
    assert_eq!(get(&c, &accepted.id).await, stopped, "replayed submit does not reopen accepted work");
}

#[tokio::test]
//= spec/impl/v2/implementation.md#agent-identity-lifecycle
//= type=test
//# A retired agent identity MUST be refused for new work.
async fn retired_recipient_refuses_new_questions_and_delegation_but_keeps_existing_question() {
    use borg_runtime::collaboration::{questions::*, workflow::{PrincipalRef, RecordRef}};
    let p = path();
    let mut cfg = config();
    // Reuse the existing controller workflow fixture policy; no provider runs.
    Arc::make_mut(&mut cfg.limits).workflow = governance::governance_config().limits.workflow.clone();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let parent = submit(&c, "directory-parent-submit", native("directory-parent")).await;
    let recipient = native("directory-recipient").definition;
    let registered = directory_reply(&c, registration("directory-recipient-register", &recipient)).await;
    assert!(!agent(&registered).retired);
    let posed = PoseQuestion {
        record: RecordRef::Task { task_id: parent.id.clone() },
        recipient: PrincipalRef::Agent { agent_id: recipient.id.clone() },
        prompt: "Choose the retained option.".into(),
        options: vec![QuestionOption { id: "keep".into(), label: "Keep".into(), explanation: "Keep this choice".into() }],
        recommended_option_id: "keep".into(),
    };
    let question_command = Operation::MutateQuestion { operation_id: "directory-question-before".into(),
        mutation: QuestionMutation::Pose { question: posed.clone() } };
    let question_receipt = directory_reply(&c, question_command.clone()).await;
    let question = match &question_receipt.outcome {
        Outcome::Ok { data: Data::QuestionMutation(value) } => value.question.clone(),
        other => panic!("accepted question expected: {other:?}"),
    };
    let retired = directory_reply(&c, retirement("directory-recipient-retire", &recipient)).await;
    assert!(agent(&retired).retired);
    assert_eq!(code(directory_reply(&c, Operation::MutateQuestion {
        operation_id: "directory-question-after".into(), mutation: QuestionMutation::Pose { question: posed },
    }).await.outcome), "agent_retired");
    let question_query = Operation::QueryQuestion { query: QuestionQuery::Get { question_id: question.question.id.clone() } };
    let question_read = directory_reply(&c, question_query.clone()).await;
    assert!(matches!(&question_read.outcome, Outcome::Ok {
        data: Data::QuestionQuery(QuestionQueryResult::Question(value))
    } if value == &question));
    exact_replay(&c, &question_command, &question_receipt).await;

    let w = worker("directory-parent-worker", "incarnation-1", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    assert_eq!(a.task_id, parent.id);
    task(report(&c, &a, "directory-parent-started", Observation::Started { thread_id: None, turn_id: None }).await);
    let mut child = native(&recipient.id);
    child.limits.deadline_unix_ms = parent.input.limits.deadline_unix_ms;
    child.limits.model_requests = 3;
    let delegate = |id: &str, child| Operation::DelegateTask { operation_id: id.into(),
        parent_task_id: parent.id.clone(), parent_attempt_id: a.id.clone(), generation: a.generation, child };
    let before = get(&c, &parent.id).await;
    assert_eq!(code(directory_reply(&c, delegate("directory-delegate-retired", child.clone())).await.outcome), "agent_retired");
    assert_eq!(get(&c, &parent.id).await, before, "refused delegation must not reserve work or stop its parent");
    assert!(matches!(query(&c, Query::Sessions { agent_id: Some(recipient.id.clone()), after: None, limit: 10 }).await,
        QueryResult::Sessions(page) if page.items.is_empty()));
    // The same bounded input can delegate to an active identity, proving that
    // the negative reaches retirement rather than an unrelated budget fence.
    child.definition = native("directory-active-child").definition;
    let delegated = task(directory_reply(&c, delegate("directory-delegate-active", child)).await.outcome);
    assert_eq!(delegated.parent_task_id.as_deref(), Some(parent.id.as_str()));
    let waiting = get(&c, &parent.id).await;
    assert_eq!(waiting.waiting_child_id.as_deref(), Some(delegated.id.as_str()));
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg).unwrap();
    assert_eq!(selected(&c, &recipient.id).await, agent(&retired));
    assert_eq!(directory_reply(&c, question_query).await.outcome, question_read.outcome);
    exact_replay(&c, &question_command, &question_receipt).await;
    assert_eq!(get(&c, &parent.id).await, waiting);
    assert_eq!(get(&c, &delegated.id).await, delegated);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
//= type=test
//# Admission MUST enforce configured queue and resource bounds.
async fn registered_agents_share_the_implicit_submission_capacity_without_refusal_residue() {
    let p = path();
    let cfg = with_controller_limits(config(), |limits| limits.max_tasks = 2);
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    for id in ["directory-capacity-a", "directory-capacity-b"] {
        let definition = native(id).definition;
        assert!(!agent(&directory_reply(&c, registration(&format!("register-{id}"), &definition)).await).retired);
    }
    let directory = agents(&c, None, 10).await;
    assert_eq!(directory.len(), 2);
    let unknown = native("directory-capacity-unknown");
    let command = Operation::SubmitTask { operation_id: "capacity-implicit-new".into(), input: unknown.clone() };
    let refused = directory_reply(&c, command.clone()).await;
    assert_eq!(code(refused.outcome.clone()), "capacity");
    if let Outcome::Refused { message, .. } = &refused.outcome {
        assert!(message.contains("controller.max_tasks"), "limiting field is explicit: {message}");
    }
    assert_eq!(agents(&c, None, 10).await, directory);
    assert_eq!(code(directory_reply(&c, Operation::GetAgent { agent_id: unknown.definition.id.clone() }).await.outcome), "not_found");
    assert_eq!(code(directory_reply(&c, Operation::QueryCollaboration { query: Query::Definition {
        definition_id: unknown.definition.id.clone(), revision: unknown.definition.revision.clone(),
    } }).await.outcome), "not_found");
    assert!(matches!(call(&c, Operation::ListTasks { after: None, limit: 10 }).await,
        Outcome::Ok { data: Data::Tasks(values) } if values.is_empty()));
    assert!(matches!(query(&c, Query::Sessions { agent_id: None, after: None, limit: 10 }).await,
        QueryResult::Sessions(page) if page.items.is_empty()));
    // Directory capacity limits new identities; an already registered identity
    // still has the independent task capacity needed for its first session.
    let accepted = submit(&c, "capacity-existing-submit", native("directory-capacity-a")).await;
    assert_eq!(accepted.assigned_agent_id, "directory-capacity-a");
    assert_eq!(agents(&c, None, 10).await, directory);
    exact_replay(&c, &command, &refused).await;
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg).unwrap();
    assert_eq!(agents(&c, None, 10).await, directory);
    assert_eq!(get(&c, &accepted.id).await, accepted);
    exact_replay(&c, &command, &refused).await;
    assert_eq!(code(directory_reply(&c, Operation::GetAgent { agent_id: unknown.definition.id }).await.outcome), "not_found");
}

#[cfg(debug_assertions)]
#[tokio::test]
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# An exact retry of a committed mutating request MUST return its recorded outcome.
async fn agent_lifecycle_actual_commit_failures_leave_no_partial_state_or_receipt() {
    fn closed_counts(p: &std::path::Path, agent_id: &str, operation_id: &str) -> (i64, i64, i64, i64) {
        let db = rusqlite::Connection::open_with_flags(p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        (
            db.query_row("SELECT count(*) FROM definitions WHERE id=?1", [agent_id], |row| row.get(0)).unwrap(),
            db.query_row("SELECT count(*) FROM agents WHERE id=?1", [agent_id], |row| row.get(0)).unwrap(),
            db.query_row("SELECT count(*) FROM agent_retirements WHERE id=?1", [agent_id], |row| row.get(0)).unwrap(),
            db.query_row("SELECT count(*) FROM receipts WHERE id=?1", [operation_id], |row| row.get(0)).unwrap(),
        )
    }
    let p = path();
    let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut cfg = config();
    cfg.qualification_fail_next_commit = Some(fail.clone());
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let definition = native("directory-commit-failure").definition;
    let register_command = registration("directory-register-commit", &definition);
    fail.store(true, Ordering::SeqCst);
    let failed = directory_reply(&c, register_command.clone()).await;
    assert!(!fail.load(Ordering::SeqCst), "the actual commit hook must be consumed");
    assert_eq!(code(failed.outcome), "store_unavailable");
    assert!(failed.commit.is_none());
    assert!(agents(&c, None, 10).await.is_empty());
    assert_eq!(code(directory_reply(&c, Operation::GetAgent { agent_id: definition.id.clone() }).await.outcome), "not_found");
    assert_eq!(code(directory_reply(&c, Operation::GetOperation { operation_id: "directory-register-commit".into() }).await.outcome), "not_found");
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(closed_counts(&p, &definition.id, "directory-register-commit"), (0, 0, 0, 0));

    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let registered = directory_reply(&c, register_command.clone()).await;
    assert_eq!(agent(&registered).definition, definition);
    assert!(!agent(&registered).retired);
    assert!(registered.commit.is_some());
    let retire_command = retirement("directory-retire-commit", &definition);
    fail.store(true, Ordering::SeqCst);
    let failed = directory_reply(&c, retire_command.clone()).await;
    assert!(!fail.load(Ordering::SeqCst));
    assert_eq!(code(failed.outcome), "store_unavailable");
    assert!(failed.commit.is_none());
    assert_eq!(selected(&c, &definition.id).await, agent(&registered));
    assert_eq!(agents(&c, None, 10).await, vec![agent(&registered)]);
    assert_eq!(code(directory_reply(&c, Operation::GetOperation { operation_id: "directory-retire-commit".into() }).await.outcome), "not_found");
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(closed_counts(&p, &definition.id, "directory-retire-commit"), (1, 1, 0, 0));

    let c = Controller::open_with_config(&p, cfg).unwrap();
    let retired = directory_reply(&c, retire_command.clone()).await;
    assert!(agent(&retired).retired);
    assert!(retired.commit.is_some());
    assert!(agents(&c, None, 10).await.is_empty());
    exact_replay(&c, &register_command, &registered).await;
    exact_replay(&c, &retire_command, &retired).await;
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(closed_counts(&p, &definition.id, "directory-retire-commit"), (1, 1, 1, 1));
}

#[tokio::test]
//= spec/impl/v2/implementation.md#agent-identity-lifecycle
//= type=test
//# Retiring an agent identity MUST remove it from the active agent directory.
//= spec/impl/v2/implementation.md#agent-identity-lifecycle
//= type=test
//# A retired agent identity MUST remain resolvable for historical reads.
async fn bound_agent_directory_filters_before_paging_and_exposes_no_lifecycle_mutation_tool() {
    use borg_runtime::mcp::{self, Caller, Invocation, ListPage};
    fn page(reply: &Reply) -> ListPage {
        match &reply.outcome {
            Outcome::Ok { data: Data::CollaborationPage(value) } => value.clone(),
            other => panic!("bound directory page expected: {other:?}"),
        }
    }
    let c = Controller::open_with_config(path(), config()).unwrap();
    let t = submit(&c, "directory-mcp-submit", native("directory-mcp-z-caller")).await;
    let w = worker("directory-mcp-worker", "incarnation-1", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    assert_eq!(a.task_id, t.id);
    task(report(&c, &a, "directory-mcp-started", Observation::Started { thread_id: None, turn_id: None }).await);
    let actor = Caller { task_id: t.id.clone(), attempt_id: a.id.clone(), generation: a.generation,
        worker_id: a.worker_id.clone(), incarnation: a.worker_incarnation.clone() };
    let invoke = |id: &str, tool: &str, arguments| Operation::CallCollaboration { call: Invocation {
        operation_id: id.into(), logical_call_id: format!("logical-{id}"), caller: actor.clone(),
        tool: tool.into(), arguments,
    } };
    let mut definitions = vec![];
    for id in ["directory-mcp-a", "directory-mcp-b", "directory-mcp-c", "directory-mcp-d"] {
        let definition = native(id).definition;
        assert!(!agent(&directory_reply(&c, registration(&format!("register-{id}"), &definition)).await).retired);
        definitions.push(definition);
    }
    for index in [0, 2] {
        let definition = &definitions[index];
        assert!(agent(&directory_reply(&c, retirement(&format!("retire-{}", definition.id), definition)).await).retired);
    }
    let first_command = invoke("directory-mcp-page-1", "collab_agents_list", json!({"cursor":null,"limit":1}));
    let first_receipt = directory_reply(&c, first_command.clone()).await;
    let first = page(&first_receipt);
    assert_eq!(first.items.len(), 1, "a retired prefix must not consume the page limit");
    assert_eq!(first.items[0]["id"], "directory-mcp-b");
    assert!(first.next_cursor.is_some());
    let retired = directory_reply(&c, retirement("directory-mcp-retire-cursor", &definitions[1])).await;
    assert!(agent(&retired).retired);
    let second = page(&directory_reply(&c, invoke("directory-mcp-page-2", "collab_agents_list",
        json!({"cursor":first.next_cursor,"limit":1}))).await);
    assert_eq!(second.items.len(), 1, "retired cursor and subsequent retired row remain ordering positions");
    assert_eq!(second.items[0]["id"], "directory-mcp-d");
    assert!(second.next_cursor.is_some());
    let third = page(&directory_reply(&c, invoke("directory-mcp-page-3", "collab_agents_list",
        json!({"cursor":second.next_cursor,"limit":1}))).await);
    assert_eq!(third.items.len(), 1);
    assert_eq!(third.items[0]["id"], "directory-mcp-z-caller");
    assert!(third.next_cursor.is_none());
    let all = page(&directory_reply(&c, invoke("directory-mcp-page-current", "collab_agents_list",
        json!({"cursor":null,"limit":2}))).await);
    assert_eq!(all.items.iter().map(|value| value["id"].as_str().unwrap()).collect::<Vec<_>>(),
        vec!["directory-mcp-d", "directory-mcp-z-caller"]);
    assert!(all.next_cursor.is_none());
    exact_replay(&c, &first_command, &first_receipt).await;
    let historical = directory_reply(&c, invoke("directory-mcp-read-retired", "collab_agent_get",
        json!({"agent_id":definitions[1].id}))).await;
    assert_eq!(agent(&historical), agent(&retired));

    let directory_before = agents(&c, None, 10).await;
    let task_before = get(&c, &t.id).await;
    let new_definition = native("directory-mcp-forbidden-new").definition;
    assert!(matches!(t.input.definition.execution_profile.as_ref(),
        Some(ExecutionProfile::CollaborationV1 { .. })));
    let catalogue = mcp::catalogue_for_profile(c.limits(), t.input.definition.execution_profile.as_ref());
    for (tool, arguments) in [
        ("collab_agent_register", json!({"definition":new_definition})),
        ("collab_agent_retire", json!({"agent_id":definitions[3].id,"expected_revision":definitions[3].revision})),
    ] {
        assert!(catalogue.iter().all(|entry| entry.name != tool));
        let refused = directory_reply(&c, invoke(&format!("directory-forbidden-{tool}"), tool, arguments)).await;
        assert_eq!(code(refused.outcome.clone()), "invalid_input");
        assert!(matches!(&refused.outcome, Outcome::Refused { message, .. }
            if message == "identity/task tools require V2"));
        assert_eq!(agents(&c, None, 10).await, directory_before);
        assert_eq!(get(&c, &t.id).await, task_before);
    }
    assert_eq!(code(directory_reply(&c, Operation::GetAgent { agent_id: new_definition.id }).await.outcome), "not_found");
    assert!(!selected(&c, &definitions[3].id).await.retired);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#agent-identity-lifecycle
//= type=test
//# A retired agent identity MUST be refused for new work.
//= spec/impl/v2/implementation.md#agent-identity-lifecycle
//= type=test
//# Agent retirement MUST preserve retained historical records.
//= spec/impl/v2/implementation.md#agent-identity-lifecycle
//= type=test
//# A retired agent identity MUST remain resolvable for historical reads.
async fn retired_document_and_review_principals_remain_historical_but_refuse_new_ownership() {
    use borg_runtime::collaboration::{documents::*, work::*, workflow::{PrincipalRef, ReviewTarget}};
    fn document(reply: &Reply) -> DocumentView {
        match &reply.outcome {
            Outcome::Ok { data: Data::DocumentMutation(DocumentMutationOutcome::Document { view }) } => view.clone(),
            other => panic!("document expected: {other:?}"),
        }
    }
    fn review(reply: &Reply) -> ReviewView {
        match &reply.outcome {
            Outcome::Ok { data: Data::ReviewMutation(ReviewMutationOutcome::Review(view)) } => view.clone(),
            other => panic!("review expected: {other:?}"),
        }
    }
    let p = path();
    let mut cfg = config();
    Arc::make_mut(&mut cfg.limits).workflow = governance::governance_config().limits.workflow.clone();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let definition = native("directory-record-principal").definition;
    let registered = directory_reply(&c, registration("directory-record-register", &definition)).await;
    assert!(!agent(&registered).retired);
    let principal = PrincipalRef::Agent { agent_id: definition.id.clone() };
    let content = DocumentContent { title: "Retained ownership".into(), body: "Exact historical document bytes".into() };
    let create = |operation_id: &str, responsible_owner| Operation::MutateDocument {
        operation_id: operation_id.into(), mutation: DocumentMutation::Create {
            content: content.clone(), supplied_content_id: None, responsible_owner,
        },
    };
    let original_document_command = create("directory-record-document", principal.clone());
    let original_document = directory_reply(&c, original_document_command.clone()).await;
    let historical_document = document(&original_document);
    assert_eq!(historical_document.document.responsible_owner, principal);
    let other_document = document(&directory_reply(&c,
        create("directory-record-other-document", PrincipalRef::TrustedHostOperator)).await);
    let target = ReviewTarget::DocumentVersion { version: historical_document.version.reference.clone() };
    let original_review_command = Operation::MutateReview { operation_id: "directory-record-review".into(),
        mutation: ReviewMutation::Request { target: target.clone(), reviewer: principal.clone(), summary: "Review the exact retained version".into() } };
    let original_review = directory_reply(&c, original_review_command.clone()).await;
    let historical_review = review(&original_review);
    assert_eq!(historical_review.review.reviewer, principal);
    assert_eq!(historical_review.current_responsible_owner, principal);
    let accepted = submit(&c, "directory-record-work", native("directory-record-task-owner")).await;
    let work_query = Operation::QueryWork { query: WorkQuery::Get { task_id: accepted.id.clone() } };
    let original_work = directory_reply(&c, work_query.clone()).await;
    let coordination_revision = match &original_work.outcome {
        Outcome::Ok { data: Data::WorkQuery(WorkQueryResult::Task(view)) } => {
            assert_ne!(view.responsible_owner, principal);
            view.coordination_revision
        },
        other => panic!("work view expected: {other:?}"),
    };
    let queries = vec![
        Operation::QueryDocument { query: DocumentQuery::Get { document_id: historical_document.document.id.clone(), version: Some(historical_document.version.reference.version) } },
        Operation::QueryDocument { query: DocumentQuery::Get { document_id: other_document.document.id.clone(), version: None } },
        Operation::QueryDocument { query: DocumentQuery::List { after: None, limit: 8 } },
        Operation::QueryReview { query: ReviewQuery::Get { review_id: historical_review.review.id.clone() } },
        Operation::QueryReview { query: ReviewQuery::List { record: None, status: None, after: None, limit: 8 } },
        work_query,
    ];
    let mut retained_reads = vec![];
    for query in queries {
        let reply = directory_reply(&c, query.clone()).await;
        assert!(matches!(&reply.outcome, Outcome::Ok { .. }), "{reply:?}");
        assert!(reply.commit.is_none());
        retained_reads.push((query, reply.outcome));
    }
    let retired = directory_reply(&c, retirement("directory-record-retire", &definition)).await;
    assert!(agent(&retired).retired);
    let refused_commands = [
        create("directory-record-new-document", principal.clone()),
        Operation::MutateDocument { operation_id: "directory-record-new-document-owner".into(),
            mutation: DocumentMutation::SetResponsibleOwner { document_id: other_document.document.id.clone(),
                expected_revision: other_document.document.revision, responsible_owner: principal.clone() } },
        Operation::MutateReview { operation_id: "directory-record-new-review".into(),
            mutation: ReviewMutation::Request { target, reviewer: principal.clone(), summary: "Fresh request to retired reviewer".into() } },
        Operation::MutateWork { operation_id: "directory-record-new-work-owner".into(),
            mutation: WorkMutation::ChangeOwner { task_id: accepted.id.clone(), expected_revision: coordination_revision,
                owner: principal.clone() } },
    ];
    for command in refused_commands {
        assert_eq!(code(directory_reply(&c, command).await.outcome), "agent_retired");
    }
    assert_eq!(get(&c, &accepted.id).await, accepted);
    for (query, original) in &retained_reads {
        assert_eq!(&directory_reply(&c, query.clone()).await.outcome, original,
            "retirement and refused new selections preserve original records and inventories");
    }
    exact_replay(&c, &original_document_command, &original_document).await;
    exact_replay(&c, &original_review_command, &original_review).await;
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg).unwrap();
    assert_eq!(selected(&c, &definition.id).await, agent(&retired));
    assert_eq!(get(&c, &accepted.id).await, accepted);
    for (query, original) in retained_reads {
        assert_eq!(directory_reply(&c, query).await.outcome, original);
    }
    exact_replay(&c, &original_document_command, &original_document).await;
    exact_replay(&c, &original_review_command, &original_review).await;
}
