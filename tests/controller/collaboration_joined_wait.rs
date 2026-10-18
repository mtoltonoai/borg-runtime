//! Actual MCP -> Controller receipt -> stop command -> wait resolution join.
//! Controlled worker observations qualify durable state, not executor processes.
#![cfg(debug_assertions)]
use super::*;
use borg_runtime::{
    collaboration::{questions::*, work::*, workflow::*, AttemptView, Query, QueryResult},
    mcp::{self, CallResult, Caller, Invocation},
};

fn joined_config() -> ControllerConfig {
    let mut cfg = config();
    let limits = Arc::make_mut(&mut cfg.limits);
    limits.workflow = Some(borg_runtime::limits::WorkflowLimits {
        record_bytes: 32_768, text_bytes: 4_096, options: 8,
        records: 512, revisions: 4_096, page_items: 8, page_bytes: 32_768,
        waits_per_task: 8, graph_scan_nodes: 512, events_per_mutation: 64,
        event_bytes: 8_192, notice_bytes: 8_192,
        pending_delivery_bytes: 32_768, delivery_scan_rows: 64,
    });
    limits.validate().unwrap(); cfg
}
// Keep construction of these large fixture futures outside their callers'
// polling frames; the actual Controller/MCP routes and replies are unchanged.
#[inline(never)]
fn reply<'a>(c: &'a Controller, command: Operation)
    -> std::pin::Pin<Box<impl std::future::Future<Output = Reply> + 'a>> {
    Box::pin(async move {
        let value = c.handle(Request { protocol_revision: WIRE_REVISION,
            request_id: format!("joined-wait-request-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
            command: command.clone() }).await;
        value.validate_for(&command).unwrap(); value
    })
}
#[inline(never)]
fn rpc_wait<'a>(c: &'a Controller, invocation: &'a Invocation, caller_header: &'a str, body: &'a [u8])
    -> std::pin::Pin<Box<impl std::future::Future<Output = CallResult> + 'a>> {
    Box::pin(async move {
        let (status, bytes) = mcp::handle(c, Some(caller_header), Some(mcp::VERSION), body).await;
        assert_eq!(status, 200);
        let rpc: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(rpc["id"], json!(7)); assert!(rpc.get("error").is_none(), "{rpc}");
        let raw = rpc["result"]["content"][0]["text"].as_str().expect("actual MCP tool result");
        let result: CallResult = serde_json::from_str(raw).unwrap();
        result.reply.validate_for(&invocation.operation()).unwrap(); result
    })
}
async fn lookup(c: &Controller, invocation: &Invocation) -> Reply {
    reply(c, Operation::GetCollaborationCall { caller: invocation.caller.clone(),
        operation_id: invocation.operation_id.clone() }).await
}
async fn work(c: &Controller, task_id: &str) -> WorkView {
    let value = reply(c, Operation::QueryWork { query: WorkQuery::Get { task_id: task_id.into() } }).await;
    assert!(value.commit.is_none());
    match value.outcome {
        Outcome::Ok { data: Data::WorkQuery(WorkQueryResult::Task(view)) } => view,
        other => panic!("coherent work query expected: {other:?}"),
    }
}
async fn events(c: &Controller) -> Vec<WorkflowActivity> {
    let mut all = Vec::new(); let mut cursor = None;
    for _ in 0..8 {
        match call(c, Operation::QueryWorkflowActivity { cursor: cursor.take(), limit: 8 }).await {
            Outcome::Ok { data: Data::WorkflowActivity(page) } => {
                all.extend(page.items); cursor = page.next_cursor;
                if cursor.is_none() { return all; }
            }, other => panic!("canonical workflow activity expected: {other:?}"),
        }
    }
    panic!("finite joined-wait fixture exceeded its activity bound")
}
async fn attempts(c: &Controller, task_id: &str) -> Vec<AttemptView> {
    match call(c, Operation::QueryCollaboration { query: Query::TaskAttempts {
        task_id: task_id.into(), after: None, limit: 8,
    }}).await {
        Outcome::Ok { data: Data::Collaboration(QueryResult::TaskAttempts(page)) } => {
            assert!(page.next_cursor.is_none()); page.items
        }, other => panic!("bounded original attempt history expected: {other:?}"),
    }
}
async fn occupied(c: &Controller, worker_id: &str) -> u64 {
    match call(c, Operation::QueryCollaboration { query: Query::Worker { worker_id: worker_id.into() } }).await {
        Outcome::Ok { data: Data::Collaboration(QueryResult::Worker(view)) } => view.native_occupied,
        other => panic!("worker occupancy expected: {other:?}"),
    }
}
fn same_original(actual: &CallResult, original: &CallResult) {
    assert_eq!(actual.requires_quiescence, original.requires_quiescence);
    assert_eq!(actual.reply.outcome, original.reply.outcome);
    assert_eq!(actual.reply.commit, original.reply.commit);
    assert!(actual.reply.commit.is_some());
}
fn no_run(commands: &[WorkerCommand], task_id: &str) {
    assert!(!commands.iter().any(|command| matches!(command,
        WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == task_id)), "{commands:?}");
}

#[tokio::test]
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//= type=test
//# A requested collaboration wait MUST be committed before initiating the current attempt's quiescence.
async fn mcp_self_wait_commit_cut_retries_one_original_then_answers_and_resumes_once() {
    let p = path(); let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut cfg = joined_config(); cfg.qualification_fail_next_commit = Some(fail.clone());
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let mut input = native("joined-wait-owner");
    input.definition.execution_profile = Some(ExecutionProfile::CollaborationV2 { web_search: None });
    input.limits.model_requests = 3;
    let accepted = submit(&c, "joined-wait-submit", input).await;
    let w = worker("joined-wait-worker", "inc", 1, 0); register(&c, &w).await;
    let issued = commands(&c, &w, vec![]).await;
    let a = run(&issued); assert_eq!(a.task_id, accepted.id);
    task(report(&c, &a, "joined-wait-started", Observation::Started { thread_id: None, turn_id: None }).await);
    // Controlled positive request facts; no provider request is performed.
    controlled_native_request(&c, &a, 1, Some(4), Some(2)).await;
    // Acknowledge the original Run so later command queries discriminate a new
    // stop or new attempt from ordinary redelivery of the initial command.
    let run_ids = issued.iter().filter_map(|command| match command {
        WorkerCommand::RunAttempt { command_id, .. } => Some(command_id.clone()), _ => None,
    }).collect();
    no_run(&commands(&c, &w, run_ids).await, &accepted.id);
    let question = match call(&c, Operation::MutateQuestion { operation_id: "joined-wait-question".into(),
        mutation: QuestionMutation::Pose { question: PoseQuestion {
            record: RecordRef::Task { task_id: accepted.id.clone() }, recipient: PrincipalRef::TrustedHostOperator,
            prompt: "Choose one option. Recommended: keep.".into(),
            options: vec![
                QuestionOption { id: "keep".into(), label: "Keep".into(), explanation: "Preserve the current choice".into() },
                QuestionOption { id: "change".into(), label: "Change".into(), explanation: "Use the stated alternative".into() },
            ], recommended_option_id: "keep".into(),
        }} }).await {
        Outcome::Ok { data: Data::QuestionMutation(value) } => value.question.question,
        other => panic!("original optional question expected: {other:?}"),
    };
    let caller = Caller { task_id: accepted.id.clone(), attempt_id: a.id.clone(), generation: a.generation,
        worker_id: a.worker_id.clone(), incarnation: a.worker_incarnation.clone() };
    let invocation = Invocation { operation_id: "joined-wait-original".into(), logical_call_id: "joined-wait-logical-original".into(),
        caller: caller.clone(), tool: "collab_task_wait".into(),
        arguments: json!({"task_id":accepted.id,"expected_revision":0,
            "resolver":{"kind":"question","question_id":question.id}}),
    };
    let header = serde_json::to_string(&caller).unwrap();
    let body = serde_json::to_vec(&json!({"jsonrpc":"2.0","id":7,"method":"tools/call","params":{
        "name":invocation.tool,"arguments":invocation.arguments,
        "_meta":{"borg":{"operation_id":invocation.operation_id,"logical_call_id":invocation.logical_call_id}},
    }})).unwrap();
    let before = work(&c, &accepted.id).await;
    assert_eq!(before.coordination_revision, 0); assert_eq!(before.open_waits, 0);
    assert_eq!(before.task.status, TaskStatus::Active);
    assert_eq!(before.task.attempt.as_ref().unwrap().phase, AttemptPhase::Running);
    let before_events = events(&c).await;
    let before_attempts = attempts(&c, &accepted.id).await;
    assert_eq!(before_attempts.len(), 1); assert_eq!(before_attempts[0].reported_model_requests, 1);

    fail.store(true, Ordering::SeqCst);
    let failed = rpc_wait(&c, &invocation, &header, &body).await;
    assert!(!fail.load(Ordering::SeqCst), "the actual original receipt COMMIT hook was consumed");
    assert_eq!(code(failed.reply.outcome.clone()), "store_unavailable");
    assert!(failed.reply.commit.is_none());
    assert!(!failed.requires_quiescence, "rolled-back wait cannot instruct the executor to stop");
    let absent = lookup(&c, &invocation).await;
    assert_eq!(code(absent.outcome), "not_found"); assert!(absent.commit.is_none());
    assert_eq!(work(&c, &accepted.id).await, before);
    assert_eq!(events(&c).await, before_events);
    assert_eq!(attempts(&c, &accepted.id).await, before_attempts);
    let after_failure = commands(&c, &w, vec![]).await;
    no_run(&after_failure, &accepted.id);
    assert!(!after_failure.iter().any(|command| matches!(command,
        WorkerCommand::StopAttempt { attempt_id, .. } if attempt_id == &a.id)), "failed COMMIT leaked a StopAttempt");
    assert_eq!(occupied(&c, &w.id).await, 1);
    drop(c); controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert_eq!(work(&c, &accepted.id).await, before);
    assert_eq!(events(&c).await, before_events);
    let absent = lookup(&c, &invocation).await;
    assert_eq!(code(absent.outcome), "not_found"); assert!(absent.commit.is_none());

    // Exact original JSON-RPC bytes, call identity and expected revision retry.
    let original = rpc_wait(&c, &invocation, &header, &body).await;
    assert!(original.requires_quiescence);
    assert_eq!(original.reply.commit.as_ref().unwrap().operation_id, invocation.operation_id);
    let change = match &original.reply.outcome {
        Outcome::Ok { data: Data::WorkMutation(value) } => value.clone(),
        other => panic!("same original wait must commit after rollback: {other:?}"),
    };
    let wait = change.changed_wait.clone().unwrap();
    assert_eq!(change.view.task.id, caller.task_id);
    assert_eq!(change.view.open_waits, 1); assert_eq!(change.view.waits.items.len(), 1);
    assert_eq!(change.view.coordination_revision, 1); assert!(change.view.coordination_blocked);
    assert_eq!(change.view.state, WorkState::Waiting { prior: ResumeState {
        status: before.task.status, assigned_agent_id: before.task.assigned_agent_id.clone(),
    }});
    assert_eq!(change.view.waits.items[0].reference, wait);
    assert_eq!(change.view.waits.items[0].operation_id, invocation.operation_id);
    assert_eq!(change.view.task.input, accepted.input);
    let stopping = change.view.task.attempt.as_ref().unwrap();
    assert_eq!(stopping.id, a.id); assert_eq!(stopping.phase, AttemptPhase::StopRequested);
    assert!(!stopping.stopped && !stopping.uncertain);
    let committed_events = events(&c).await;
    assert_eq!(committed_events.iter().filter(|event| matches!(&event.detail,
        ActivityDetail::Wait(WaitChange::Added { wait: actual, .. }) if actual == &wait)).count(), 1);
    let waiting = work(&c, &accepted.id).await;
    same_original(&rpc_wait(&c, &invocation, &header, &body).await, &original);
    same_original(&mcp::recovered(&invocation, lookup(&c, &invocation).await).unwrap(), &original);
    assert_eq!(work(&c, &accepted.id).await, waiting);
    assert_eq!(events(&c).await, committed_events);
    let stop_commands = commands(&c, &w, vec![]).await;
    no_run(&stop_commands, &accepted.id);
    let stops = stop_commands.iter().filter(|command| matches!(command,
        WorkerCommand::StopAttempt { attempt_id, .. } if attempt_id == &a.id)).collect::<Vec<_>>();
    assert_eq!(stops.len(), 1);
    assert!(matches!(stops[0], WorkerCommand::StopAttempt { command_id, task_id, generation, incarnation, .. }
        if command_id == &format!("stop-{}",a.id) && task_id == &accepted.id
            && generation == &a.generation && incarnation == &a.worker_incarnation));
    assert_eq!(occupied(&c, &w.id).await, 1, "stop intent has not freed the executing slot");
    // AwaitCommands records why a fresh attempt cannot yet be admitted. Bind
    // restart to that actual committed projection, while proving it changed
    // neither the original wait nor the execution identity.
    let after_commands = work(&c, &accepted.id).await;
    let mut expected_after_commands = waiting.clone();
    expected_after_commands.task.revision += 1;
    expected_after_commands.task.blocked_reason = Some("prior_execution_active".into());
    assert_eq!(after_commands, expected_after_commands);
    let waiting = after_commands;
    drop(c); controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    same_original(&rpc_wait(&c, &invocation, &header, &body).await, &original);
    assert_eq!(work(&c, &accepted.id).await, waiting);
    assert_eq!(events(&c).await, committed_events);

    // The intended host answers before the physical stop report. Resolution
    // alone must not revive the old attempt or allocate a fresh one early.
    let answer_op = Operation::MutateQuestion { operation_id: "joined-wait-answer".into(),
        mutation: QuestionMutation::Answer { question_id: question.id.clone(), expected_revision: question.revision,
            answer: QuestionAnswer { option_id: "keep".into(), explanation: "Keep the existing choice".into() } } };
    let answer_reply = reply(&c, answer_op.clone()).await;
    let answer = match &answer_reply.outcome {
        Outcome::Ok { data: Data::QuestionMutation(value) } => {
            assert_eq!(value.question.question.status, QuestionStatus::Answered);
            value.new_answer.clone().unwrap()
        }, other => panic!("actual intended-recipient answer expected: {other:?}"),
    };
    let ended = work(&c, &accepted.id).await;
    assert_eq!(ended.open_waits, 0); assert_eq!(ended.waits.items.len(), 1);
    let exact = match &ended.waits.items[0].status {
        WaitStatus::Resolved { resolution, .. } => resolution.clone(),
        other => panic!("original answered outcome must resolve the exact wait: {other:?}"),
    };
    assert_eq!(exact.wait, wait);
    assert_eq!(exact.outcome, ResolverOutcome::Question(QuestionResolution::Answered {
        question_id: question.id.clone(), answer: AnswerRef { question_id: question.id.clone(), answer_id: answer.id.clone() },
        selected_option_id: "keep".into(),
    }));
    let terminal = events(&c).await.into_iter().filter(|event| matches!(&event.detail,
        ActivityDetail::Question(QuestionChange::Closed { resolution })
            if ResolverOutcome::Question(resolution.clone()) == exact.outcome)).collect::<Vec<_>>();
    assert_eq!(terminal.len(), 1); assert_eq!(terminal[0].id, exact.event_id);
    assert_eq!(ended.task.attempt.as_ref().unwrap().id, a.id);
    assert_eq!(ended.task.attempt.as_ref().unwrap().phase, AttemptPhase::StopRequested);
    assert!(!ended.task.attempt.as_ref().unwrap().stopped);
    no_run(&commands(&c, &w, vec![]).await, &accepted.id);
    assert_eq!(occupied(&c, &w.id).await, 1);
    assert_eq!(attempts(&c, &accepted.id).await.len(), 1);

    let stopped = task(report(&c, &a, "joined-wait-stopped", Observation::ProcessTreeStopped).await);
    assert!(stopped.attempt.as_ref().unwrap().stopped);
    assert_eq!(stopped.attempt.as_ref().unwrap().phase, AttemptPhase::Stopped);
    assert_eq!(occupied(&c, &w.id).await, 0);
    let admission = Operation::AwaitCommands { operation_id: "joined-wait-resume".into(),
        worker_id: w.id.clone(), incarnation: w.incarnation.clone(), acknowledged: vec![] };
    let resumed = reply(&c, admission.clone()).await;
    let Outcome::Ok { data: Data::Commands(emitted) } = &resumed.outcome else { panic!("ordinary continuation admission expected: {resumed:?}") };
    let runs = emitted.iter().filter(|command| matches!(command,
        WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == accepted.id)).collect::<Vec<_>>();
    assert_eq!(runs.len(), 1);
    let WorkerCommand::RunAttempt { command_id, session, attempt: next, task: remaining,
        workflow_continuation: Some(continuation), .. } = runs[0] else { panic!("exact required continuation expected") };
    assert_ne!(next.id, a.id); assert!(next.generation > a.generation);
    assert_eq!(next.session_id, a.session_id); assert_eq!(session.id, accepted.session_id);
    assert_eq!(next.deadline_unix_ms, a.deadline_unix_ms);
    let mut expected_input = accepted.input.clone(); expected_input.limits.model_requests -= 1;
    assert_eq!(*remaining, expected_input);
    assert_eq!(*continuation, WorkflowContinuation { task_id: accepted.id.clone(), session_id: accepted.session_id.clone(), resolved_waits: vec![exact.clone()] });
    let current = get(&c, &accepted.id).await;
    assert_eq!(current.input, accepted.input); assert_eq!(current.attempt.as_ref().unwrap().id, next.id);
    let history = attempts(&c, &accepted.id).await;
    assert_eq!(history.len(), 2);
    assert_eq!(history.iter().map(|view| view.reported_model_requests).sum::<u32>(), 1);
    assert!(history.iter().find(|view| view.attempt.id == a.id).unwrap().attempt.stopped);
    let repeated = reply(&c, admission).await;
    assert_eq!(repeated.outcome, resumed.outcome); assert_eq!(repeated.commit, resumed.commit);
    // Historical quiescence remains true on the original wait receipt, but its
    // replay cannot install another wait or stop this newly admitted attempt.
    same_original(&rpc_wait(&c, &invocation, &header, &body).await, &original);
    assert_eq!(get(&c, &accepted.id).await, current);
    let drained = commands(&c, &w, vec![command_id.clone()]).await;
    no_run(&drained, &accepted.id);
    assert!(!drained.iter().any(|command| matches!(command,
        WorkerCommand::StopAttempt { attempt_id, .. } if attempt_id == &next.id)));
    assert_eq!(attempts(&c, &accepted.id).await, history);
    let final_events = events(&c).await;
    drop(c); controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg).unwrap();
    same_original(&rpc_wait(&c, &invocation, &header, &body).await, &original);
    same_original(&mcp::recovered(&invocation, lookup(&c, &invocation).await).unwrap(), &original);
    let repeated_answer = reply(&c, answer_op).await;
    assert_eq!(repeated_answer.outcome, answer_reply.outcome); assert_eq!(repeated_answer.commit, answer_reply.commit);
    assert_eq!(events(&c).await, final_events);
    assert_eq!(attempts(&c, &accepted.id).await, history);
    assert_eq!(get(&c, &accepted.id).await, current);
    assert_eq!(work(&c, &accepted.id).await.waits.items.len(), 1);
}
