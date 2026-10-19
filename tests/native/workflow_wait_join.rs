//! Nested child of native/workflow_join.rs; uses the actual owned controller,
//! native worker and controlled loopback Responses peer. No synthetic reports.
use super::*;
use borg_runtime::charter::*;
use borg_runtime::collaboration::questions::QuestionAnswer;
use borg_runtime::collaboration::work::{WaitStatus, WorkQuery, WorkQueryResult, WorkView};
use borg_runtime::collaboration::workflow::{
    AnswerRef, QuestionResolution, ResolverOutcome, ResolverRef, WaitChange, WorkflowContinuation,
};

async fn joined_work(api: &ApiClient, task_id: &str) -> WorkView {
    let operation = Operation::QueryWork {
        query: WorkQuery::Get { task_id: task_id.into() },
    };
    let reply = api.call(operation.clone()).await.unwrap();
    reply.validate_for(&operation).unwrap();
    let Outcome::Ok { data: Data::WorkQuery(WorkQueryResult::Task(view)) } = reply.outcome
    else { panic!("actual work query: {reply:?}") };
    view
}

fn joined_json(path: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

// Call only after both owned processes have exited and released SQLite ownership.
fn joined_receipt(connection: &rusqlite::Connection, id: &str) -> (Operation, Outcome, u64) {
    let (request, outcome, revision): (String, String, i64) = connection.query_row(
        "SELECT request,outcome,controller_revision FROM receipts WHERE id=?1",
        [id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ).unwrap();
    assert!(revision > 0);
    (serde_json::from_str(&request).unwrap(), serde_json::from_str(&outcome).unwrap(), revision as u64)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]

// Traceability scope: An actual stopped native task receives one fresh ordinary RunAttempt and its exact committed resolution input.
//= spec/impl/v2/implementation.md#committed-resolutions-reach-their-consumers
//= type=test
//# A committed resolution that makes a waiting task eligible MUST wake it through the existing platform command mechanism.

async fn native_self_wait_stops_then_answers_into_one_exact_fresh_attempt_input() {
    Box::pin(self_wait_scenario(false)).await;
}

// The same real wait/resume journey must close its original charter turn before
// physical stop, without granting the new attempt authority over the old reply.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chartered_self_wait_closes_original_turn_before_same_session_resume() {
    Box::pin(self_wait_scenario(true)).await;
}

fn wait_charter() -> CharterSource {
    let sentence = "The agent MUST retain the waiting task purpose.";
    CharterSource {
        files: vec![CharterFile { file: "charter.md".into(), text: format!("# Purpose\n\n{sentence}\n") }],
        clauses: vec![CharterClauseRef { file: "charter.md".into(), section: "Purpose".into(), sentence: sentence.into() }],
    }
}

async fn self_wait_scenario(with_charter: bool) {
    let mut lab = workflow_lab(if with_charter { "workflow-charter-self-wait-join" } else { "workflow-native-self-wait-join" });
    // Same explicit current V2 sizing as workflow_retry's positive fixture;
    // this scenario has no identity/publication capability catalogue.
    let mut selected = (*lab.limits).clone();
    selected.execution.native.function_count = 67;
    selected.execution.native.schema_bytes = 65_536;
    let tools = borg_runtime::mcp::catalogue_for_profile(&selected,
        Some(&ExecutionProfile::CollaborationV2 { publication: None, web_search: None }));
    assert_eq!(tools.len(), 67);
    assert_eq!(tools.iter().filter(|tool| tool.name == "collab_read_continue").count(), 1);
    selected.validate().unwrap();
    lab.limits = Arc::new(selected);
    std::fs::write(lab.root.join("limits.json"), serde_json::to_vec_pretty(&*lab.limits).unwrap()).unwrap();
    if with_charter {
        let mut limits = (*lab.limits).clone();
        limits.charter = Some(borg_runtime::limits::CharterLimits {
            record_bytes: 262_144, source_bytes: 65_536, files: 8, clauses: 32,
            metadata_bytes: 16_384, references: 32, reply_items: 32, tool_calls: 8,
            reference_depth: 8, records: 1024, page_items: 32, page_bytes: 262_144,
        });
        limits.validate().unwrap();
        lab.limits = Arc::new(limits);
        std::fs::write(lab.root.join("limits.json"), serde_json::to_vec_pretty(&*lab.limits).unwrap()).unwrap();
    }
    let mut controller = lab.controller("controller").await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let mut input = lab.input(2);
    input.definition.execution_profile = Some(ExecutionProfile::CollaborationV2 { publication: None, web_search: None });
    input.definition.revision = "workflow-self-wait-v2".into();
    if with_charter { input.definition.charter = Some(DefinitionCharter::Inline { source: wait_charter() }); }
    let charter_reference = CharterSourceRef { definition_id: input.definition.id.clone(), definition_revision: input.definition.revision.clone() };
    let task = submit(&api, input).await;
    assert_eq!(task.input.resolved_limits.as_ref().unwrap().global_metadata.workflow.as_ref(),
        lab.limits.workflow.as_ref());

    let first_entered = Arc::new(tokio::sync::Notify::new());
    let first_release = Arc::new(tokio::sync::Notify::new());
    let next_entered = Arc::new(tokio::sync::Notify::new());
    let next_release = Arc::new(tokio::sync::Notify::new());
    let wait_arguments = Arc::new(Mutex::new(None::<Value>));
    let final_released = Arc::new(AtomicBool::new(false));
    let peer = Peer::start_async({
        let first_entered = first_entered.clone();
        let first_release = first_release.clone();
        let next_entered = next_entered.clone();
        let next_release = next_release.clone();
        let wait_arguments = wait_arguments.clone();
        let final_released = final_released.clone();
        let charter_reference = charter_reference.clone();
        move |index, _request| {
            let first_entered = first_entered.clone();
            let first_release = first_release.clone();
            let next_entered = next_entered.clone();
            let next_release = next_release.clone();
            let wait_arguments = wait_arguments.clone();
            let final_released = final_released.clone();
            let charter_reference = charter_reference.clone();
            async move {
                if index == 0 {
                    first_entered.notify_one();
                    first_release.notified().await;
                    let arguments = wait_arguments.lock().unwrap().clone().expect("actual question posed");
                    Some(function(index, "collab_task_wait", arguments))
                } else {
                    assert_eq!(index, 1, "self-wait and its resolution permit exactly one fresh required request");
                    next_entered.notify_one();
                    next_release.notified().await;
                    final_released.store(true, Ordering::SeqCst);
                    if with_charter {
                        Some(envelope(json!([{"type":"message","role":"assistant","status":"completed",
                            "content":[{"type":"output_text","text":json!({"charter_references":{
                                "revision":charter_reference,"clauses":wait_charter().clauses},"result":7}).to_string()}]}])))
                    } else { Some(final_value()) }
                }
            }
        }
    }).await;
    let mut worker = lab.worker(&lab.url, &peer.endpoint, "workflow-self-wait-reader");
    tokio::time::timeout(Duration::from_secs(30), first_entered.notified()).await
        .expect("first actual native request absent");
    let active = get(&api, &task.id).await;
    assert_eq!(active.status, TaskStatus::Active);
    let first_attempt = active.attempt.clone().unwrap();
    assert_eq!(first_attempt.phase, AttemptPhase::Running);
    assert!(!first_attempt.stopped && !first_attempt.uncertain);
    assert_eq!(active.input, task.input);
    let initial = joined_work(&api, &task.id).await;
    assert_eq!(initial.coordination_revision, 0);
    assert_eq!(initial.open_waits, 0);

    // The host asks its own question while the actual model response is held.
    // The later native MCP call supplies only the concrete resolver reference.
    let pose = Operation::MutateQuestion {
        operation_id: "pose-native-self-wait-question".into(),
        mutation: QuestionMutation::Pose { question: PoseQuestion {
            record: RecordRef::Task { task_id: task.id.clone() },
            recipient: PrincipalRef::TrustedHostOperator,
            prompt: "Choose the continuation value.".into(),
            options: vec![
                QuestionOption { id: "keep".into(), label: "Keep".into(), explanation: "Return integer seven".into() },
                QuestionOption { id: "change".into(), label: "Change".into(), explanation: "Reconsider the value".into() },
            ],
            recommended_option_id: "keep".into(),
        }},
    };
    let posed = api.call(pose.clone()).await.unwrap();
    posed.validate_for(&pose).unwrap();
    assert!(posed.commit.is_some());
    let Outcome::Ok { data: Data::QuestionMutation(changed) } = posed.outcome
    else { panic!("question pose failed: {posed:?}") };
    let question = changed.question.question;
    assert_eq!(question.posed.recipient, PrincipalRef::TrustedHostOperator);
    let arguments = json!({"task_id":task.id,"expected_revision":initial.coordination_revision,
        "resolver":{"kind":"question","question_id":question.id}});
    *wait_arguments.lock().unwrap() = Some(arguments.clone());
    assert_eq!(peer.count(), 1);
    assert!(!notice_directory(&lab, &task).join("acknowledge.json").exists());
    first_release.notify_one();

    let waiting = until(&api, &task.id, |task| task.status == TaskStatus::Queued
        && task.attempt.as_ref().is_some_and(|attempt| attempt.phase == AttemptPhase::Stopped && attempt.stopped)).await;
    let stopped = waiting.attempt.as_ref().unwrap();
    assert_eq!(stopped.id, first_attempt.id);
    assert!(!stopped.uncertain);
    assert_eq!(waiting.input, task.input);
    assert_eq!(waiting.session_id, task.session_id);
    assert!(waiting.result.is_none());
    let blocked = joined_work(&api, &task.id).await;
    assert_eq!(blocked.task.status, TaskStatus::Queued);
    assert_eq!(blocked.open_waits, 1);
    assert!(blocked.coordination_blocked);
    assert_eq!(blocked.waits.items.len(), 1);
    assert!(blocked.waits.next_cursor.is_none());
    let wait = blocked.waits.items[0].clone();
    assert_eq!(wait.status, WaitStatus::Open);
    assert_eq!(wait.resolver, ResolverRef::Question { question_id: question.id.clone() });
    assert_eq!(reported_requests(&api, &task.id, &first_attempt.id).await, 1);
    assert_eq!(peer.count(), 1, "no continuation POST while the wait remains open after physical stop");
    assert!(!notice_directory(&lab, &task).join("acknowledge.json").exists());

    // quiesce() retains this exact successful MCP result before reporting Stop.
    let wait_journal_path = lab.state().join("collaboration")
        .join(process::component(&first_attempt.id)).join("0000000001.json");
    let retained_wait = joined_json(&wait_journal_path);
    let wait_operation: Operation = serde_json::from_value(retained_wait["request"].clone()).unwrap();
    let Operation::CallCollaboration { call } = &wait_operation else { panic!("wait journal: {retained_wait}") };
    assert_eq!(call.tool, "collab_task_wait");
    assert_eq!(call.arguments, arguments);
    assert_eq!(call.caller.task_id, task.id);
    assert_eq!(call.caller.attempt_id, first_attempt.id);
    assert_eq!(call.operation_id, wait.operation_id);
    let wait_outcome: Outcome = serde_json::from_value(retained_wait["outcome"].clone()).unwrap();
    let Outcome::Ok { data: Data::WorkMutation(committed_wait) } = &wait_outcome
    else { panic!("wait was not committed: {retained_wait}") };
    assert_eq!(committed_wait.changed_wait.as_ref(), Some(&wait.reference));
    assert_eq!(committed_wait.view.open_waits, 1);
    assert_eq!(committed_wait.view.task.attempt.as_ref().unwrap().phase, AttemptPhase::StopRequested);
    assert!(!committed_wait.view.task.attempt.as_ref().unwrap().stopped);

    let closed_charter = if with_charter {
        let operation = Operation::QueryCharter { query: CharterQuery::Turn {
            turn_id: format!("{}-charter-turn-1", first_attempt.id),
        }};
        let reply = api.call(operation.clone()).await.unwrap();
        reply.validate_for(&operation).unwrap();
        let Outcome::Ok { data: Data::CharterQuery(result) } = reply.outcome
        else { panic!("original self-wait charter turn unavailable: {reply:?}") };
        let CharterQueryResult::Turn(turn) = *result else { panic!("original self-wait charter turn unavailable") };
        assert_eq!(turn.boundary.caller, call.caller);
        assert_eq!(turn.boundary.session_id, task.session_id);
        assert_eq!(turn.boundary.request_index, 1);
        assert_eq!(turn.boundary.effective, ResolvedCharter { reference: charter_reference.clone(), source: wait_charter() });
        assert_eq!(turn.reply.as_ref().unwrap().observation.tool_call_ids, vec![call.logical_call_id.clone()]);
        assert_eq!(turn.completed_by, Some(format!("{}-closed", turn.boundary.turn_id)));
        assert_eq!(turn.delivered_results, vec![CharterResultReceipt::Collaboration {
            call_id: call.logical_call_id.clone(), operation_id: call.operation_id.clone(),
        }]);
        Some(turn)
    } else { None };

    // Answer only after the real worker has acknowledged physical stop.
    let answer_operation = Operation::MutateQuestion {
        operation_id: "answer-native-self-wait-question".into(),
        mutation: QuestionMutation::Answer {
            question_id: question.id.clone(), expected_revision: question.revision,
            answer: QuestionAnswer { option_id: "keep".into(), explanation: "Use the original requested integer.".into() },
        },
    };
    let answered = api.call(answer_operation.clone()).await.unwrap();
    answered.validate_for(&answer_operation).unwrap();
    let answer_commit = answered.commit.clone().expect("original answer transaction");
    let Outcome::Ok { data: Data::QuestionMutation(changed) } = &answered.outcome
    else { panic!("answer failed: {answered:?}") };
    let answer = changed.new_answer.as_ref().unwrap();
    let expected_outcome = QuestionResolution::Answered {
        question_id: question.id.clone(),
        answer: AnswerRef { question_id: question.id.clone(), answer_id: answer.id.clone() },
        selected_option_id: "keep".into(),
    };
    tokio::time::timeout(Duration::from_secs(30), next_entered.notified()).await
        .expect("fresh native attempt did not reach its actual required input");
    assert!(!final_released.load(Ordering::SeqCst));
    let resumed = get(&api, &task.id).await;
    let fresh = resumed.attempt.clone().unwrap();
    assert_eq!(resumed.status, TaskStatus::Active);
    assert_eq!(fresh.phase, AttemptPhase::Running);
    assert!(!fresh.stopped && !fresh.uncertain);
    assert_ne!(fresh.id, first_attempt.id);
    assert_eq!(fresh.generation, first_attempt.generation + 1);
    assert_eq!(fresh.task_id, task.id);
    assert_eq!(fresh.session_id, task.session_id);
    assert_eq!(resumed.assigned_agent_id, task.assigned_agent_id);
    assert_eq!(resumed.input, task.input, "the accepted execution input remains immutable");
    assert_eq!(fresh.deadline_unix_ms, first_attempt.deadline_unix_ms);
    assert_eq!(fresh.deadline_unix_ms, task.input.limits.deadline_unix_ms);
    assert_eq!(reported_requests(&api, &task.id, &fresh.id).await, 1);
    assert_eq!(peer.count(), 2);
    let resolved = joined_work(&api, &task.id).await;
    assert_eq!(resolved.open_waits, 0);
    assert!(!resolved.coordination_blocked);
    assert_eq!(resolved.waits.items.len(), 1);
    let WaitStatus::Resolved { resolution, .. } = &resolved.waits.items[0].status
    else { panic!("actual wait was not resolved: {resolved:?}") };
    assert_eq!(resolution.wait, wait.reference);
    assert_eq!(resolution.outcome, ResolverOutcome::Question(expected_outcome.clone()));
    let continuation = WorkflowContinuation {
        task_id: task.id.clone(), session_id: task.session_id.clone(),
        resolved_waits: vec![resolution.clone()],
    };
    let activity = api.call(Operation::QueryWorkflowActivity { cursor: None, limit: 8 }).await.unwrap();
    let Outcome::Ok { data: Data::WorkflowActivity(page) } = activity.outcome
    else { panic!("activity query: {activity:?}") };
    assert!(page.next_cursor.is_none());
    let closed: Vec<_> = page.items.iter().filter(|event| matches!(&event.detail,
        ActivityDetail::Question(QuestionChange::Closed { resolution }) if resolution == &expected_outcome)).collect();
    assert_eq!(closed.len(), 1, "the host answer has one original terminal occurrence");
    assert_eq!(resolution.event_id, closed[0].id);
    assert_eq!(page.items.iter().filter(|event| matches!(&event.detail,
        ActivityDetail::Wait(WaitChange::Resolved { wait: reference, .. }) if reference == &wait.reference)).count(), 1);

    // This is the second physical POST, held before a response can acknowledge
    // the append. Admission and Begin must carry the full original resolution.
    let observation = observe_delivery(&api, &resumed).await;
    let batch = observation.pending.as_ref().expect("real Prepare and Begin before input POST");
    assert!(observation.acknowledged_cursor.position.is_none());
    assert_eq!(batch.page.continuation.as_ref(), Some(&continuation));
    let intent = batch.delivery.as_ref().expect("actual Begin intent");
    assert_eq!(intent.caller.attempt_id, fresh.id);
    assert_eq!(intent.context_binding, fresh.id);
    assert!(!notice_directory(&lab, &task).join("acknowledge.json").exists(), "POST and Begin are not input ACK");
    let before = mirror(&lab, &task);
    assert_eq!(before["pending"]["delivery"]["appended"], false);
    let exact_text = before["pending"]["text"].as_str().unwrap();
    let requests = peer.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    if with_charter {
        let encoded_source = serde_json::to_string(&json!({"revision":charter_reference,
            "source":serde_json::to_string(&wait_charter()).unwrap()})).unwrap();
        for (index, request) in requests.iter().enumerate() {
            let items = request["input"].as_array().unwrap();
            let charter_inputs: Vec<_> = items.iter().filter(|item| item["role"] == "developer"
                && item["content"].as_str().is_some_and(|text| text.starts_with("Effective agent charter. "))).collect();
            assert_eq!(charter_inputs.len(), 1);
            assert!(charter_inputs[0]["content"].as_str().unwrap().contains(&encoded_source));
            assert_eq!(items.iter().filter(|item| item["role"] == "user" && item["content"] == task.input.instruction).count(), 1);
            assert_eq!(items.iter().filter(|item| item["role"] == "developer").count(), index + 1);
        }
        let delivered_inputs: Vec<_> = requests[1]["input"].as_array().unwrap().iter()
            .filter(|item| item["role"] == "developer" && item["content"] == exact_text).collect();
        assert_eq!(delivered_inputs.len(), 1);
        let operation = Operation::QueryCharter { query: CharterQuery::Turn {
            turn_id: format!("{}-charter-turn-1", fresh.id),
        }};
        let reply = api.call(operation.clone()).await.unwrap();
        reply.validate_for(&operation).unwrap();
        let Outcome::Ok { data: Data::CharterQuery(result) } = reply.outcome
        else { panic!("fresh self-wait charter turn unavailable: {reply:?}") };
        let CharterQueryResult::Turn(turn) = *result else { panic!("fresh self-wait charter turn unavailable") };
        assert_eq!(turn.boundary.caller.attempt_id, fresh.id);
        assert_eq!(turn.boundary.caller.generation, fresh.generation);
        assert_eq!(turn.boundary.session_id, task.session_id);
        assert_eq!(turn.boundary.effective, closed_charter.as_ref().unwrap().boundary.effective);
        assert!(turn.reply.is_none() && turn.completed_by.is_none());
    } else {
        assert!(!requests[0]["input"].as_array().unwrap().iter().any(|item| item["role"] == "developer"));
        let delivered_inputs: Vec<_> = requests[1]["input"].as_array().unwrap().iter()
            .filter(|item| item["role"] == "developer").collect();
        assert_eq!(delivered_inputs.len(), 1);
        assert_eq!(delivered_inputs[0]["content"], exact_text);
        assert_eq!(requests[1]["input"][0]["content"], task.input.instruction);
    }
    assert!(!requests[1]["input"].as_array().unwrap().iter().any(|item| item["type"] == "function_call_output"),
        "the old wait tool is closed by quiescence, not replayed as a fresh attempt's tool output");
    let delivered: Value = serde_json::from_str(exact_text).unwrap();
    assert_eq!(delivered["type"], "collaboration_activity");
    assert_eq!(delivered["batch_id"], batch.batch_id);
    assert_eq!(delivered["scope"]["session_id"], task.session_id);
    assert_eq!(delivered["continuation"], serde_json::to_value(&continuation).unwrap());
    let delivered_events: Vec<WorkflowActivity> = serde_json::from_value(delivered["events"].clone()).unwrap();
    assert!(delivered_events.iter().all(|event| event.id != resolution.event_id),
        "the original Closed event is represented once by its exact continuation reference");
    assert!(!delivered_events.iter().any(|event| matches!(&event.detail,
        ActivityDetail::Wait(WaitChange::Resolved { wait: reference, .. }) if reference == &wait.reference)),
        "the retained audit sibling does not duplicate the answer as another input");
    assert_eq!(joined_json(&wait_journal_path)["consumed"], true);

    next_release.notify_one();
    let terminal = until(&api, &task.id, |task| task.status == TaskStatus::Succeeded
        && task.attempt.as_ref().is_some_and(|attempt| attempt.stopped)).await;
    assert!(final_released.load(Ordering::SeqCst));
    assert_eq!(terminal.attempt.as_ref().unwrap().id, fresh.id);
    assert_eq!(terminal.result.as_ref().unwrap().output, Some(json!(7)));
    assert_eq!(terminal.input, task.input);
    assert_eq!(peer.count(), 2, "no notice-only turn or third attempt request");
    worker.stop().await;
    controller.stop().await;

    // Only now inspect SQLite. The actual worker's original ACK must agree with
    // its mirror, receipt, admitted wait association and exact Begin context.
    assert_eq!(lab.usage(&first_attempt.id).0, 1);
    assert_eq!(lab.usage(&fresh.id).0, 1);
    assert_eq!(ack_count(&lab, &task.id), 1);
    let after = delivery_state(&lab, &task.session_id).unwrap();
    assert!(after["pending"].is_null());
    assert_eq!(after["acknowledged_cursor"], serde_json::to_value(&batch.page.to_cursor).unwrap());
    let mirror_after = mirror(&lab, &task);
    assert!(mirror_after["pending"].is_null());
    assert_eq!(mirror_after["acknowledged_cursor"], after["acknowledged_cursor"]);
    let ack_journal = joined_json(&notice_directory(&lab, &task).join("acknowledge.json"));
    assert_eq!(ack_journal["consumed"], true);
    let ack_operation: Operation = serde_json::from_value(ack_journal["request"].clone()).unwrap();
    let Operation::AcknowledgeCollaborationDelivery { operation_id: ack_id, caller, batch_id, context_binding } = &ack_operation
    else { panic!("ACK journal: {ack_journal}") };
    assert_eq!(caller, &intent.caller);
    assert_eq!(batch_id, &batch.batch_id);
    assert_eq!(context_binding, &fresh.id);
    let connection = rusqlite::Connection::open_with_flags(lab.root.join("tasks.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let (stored_ack, ack_outcome, ack_revision) = joined_receipt(&connection, ack_id);
    assert_eq!(serde_json::to_value(&stored_ack).unwrap(), serde_json::to_value(&ack_operation).unwrap());
    assert_eq!(serde_json::to_value(&ack_outcome).unwrap(), ack_journal["outcome"]);
    assert_eq!(ack_journal["outcome_context"]["commit"], json!({"operation_id":ack_id,"controller_revision":ack_revision}));
    let Outcome::Ok { data: Data::CollaborationDelivery(acked) } = ack_outcome
    else { panic!("input ACK did not commit") };
    assert!(acked.pending.is_none());
    assert_eq!(acked.acknowledged_cursor, batch.page.to_cursor);
    let (stored_begin, begin_outcome, begin_revision) = joined_receipt(&connection, &intent.operation_id);
    assert_eq!(serde_json::to_value(&stored_begin).unwrap(), serde_json::to_value(Operation::BeginCollaborationDelivery {
        operation_id: intent.operation_id.clone(), caller: intent.caller.clone(),
        batch_id: batch.batch_id.clone(), context_binding: fresh.id.clone(),
    }).unwrap());
    let Outcome::Ok { data: Data::CollaborationDelivery(begun) } = begin_outcome
    else { panic!("Begin did not commit") };
    assert_eq!(begun.pending.as_ref(), Some(batch));
    assert!(begun.acknowledged_cursor.position.is_none());
    let (stored_wait, stored_wait_outcome, wait_revision) = joined_receipt(&connection, &wait.operation_id);
    assert_eq!(serde_json::to_value(&stored_wait).unwrap(), serde_json::to_value(&wait_operation).unwrap());
    assert_eq!(stored_wait_outcome, wait_outcome);
    assert_eq!(retained_wait["outcome_context"]["commit"],
        json!({"operation_id":wait.operation_id,"controller_revision":wait_revision}));
    let stop_id = format!("{}-collaboration-workflow-wait-stopped", first_attempt.id);
    let (stop_operation, stop_outcome, stop_revision) = joined_receipt(&connection, &stop_id);
    let Operation::ObserveWorker { report } = stop_operation else { panic!("physical Stop receipt is not a worker report") };
    assert_eq!(report.attempt_id, first_attempt.id);
    assert_eq!(report.observation, Observation::ProcessTreeStopped);
    assert!(matches!(stop_outcome, Outcome::Ok { .. }));
    let (stored_answer, stored_answer_outcome, answer_revision) = joined_receipt(&connection, &answer_commit.operation_id);
    assert_eq!(serde_json::to_value(&stored_answer).unwrap(), serde_json::to_value(&answer_operation).unwrap());
    assert_eq!(stored_answer_outcome, answered.outcome);
    assert_eq!(answer_revision, answer_commit.controller_revision);
    assert!(wait_revision < stop_revision && stop_revision < answer_revision
        && answer_revision < begin_revision && begin_revision < ack_revision,
        "original receipt order must prove wait commit, physical stop, answer, Begin, then ACK");

    if let Some(turn) = &closed_charter {
        let closure_id = turn.completed_by.as_ref().unwrap();
        let (closure_operation, closure_outcome, closure_revision) = joined_receipt(&connection, closure_id);
        let Operation::ObserveCharter { operation_id, caller, observation } = closure_operation
        else { panic!("self-wait closure is not its original charter observation") };
        assert_eq!(&operation_id, closure_id);
        assert_eq!(caller, call.caller, "only the original caller closes the wait reply");
        assert_eq!(observation, CharterObservation::CompleteTurn {
            turn_id: turn.boundary.turn_id.clone(), results: turn.delivered_results.clone(),
        });
        let Outcome::Ok { data: Data::Charter(result) } = closure_outcome
        else { panic!("original self-wait closure did not commit") };
        let CharterOutcome::Turn(stored_turn) = *result else { panic!("original self-wait closure did not commit") };
        assert_eq!(&stored_turn, turn);
        assert!(wait_revision < closure_revision && closure_revision < stop_revision,
            "original wait result and charter closure must precede physical stop and successor admission");
        assert_eq!(connection.query_row("SELECT count(*) FROM receipts WHERE id=?1",
            [format!("{}-charter-prior-closed", fresh.id)], |row| row.get::<_, i64>(0)).unwrap(), 0,
            "the fresh caller must not try to close the prior caller's charter turn");
    }

    let mut association_query = connection.prepare(
        "SELECT attempt_id,body,input_ack FROM workflow_wait_consumptions WHERE task_id=?1 ORDER BY attempt_id LIMIT 3").unwrap();
    let associations = association_query.query_map([&task.id], |row|
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, Option<String>>(2)?)))
        .unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(associations.len(), 1);
    assert_eq!(associations[0].0, fresh.id);
    assert_eq!(serde_json::from_str::<Value>(&associations[0].1).unwrap(), serde_json::to_value(resolution).unwrap());
    assert_eq!(serde_json::from_str::<Value>(associations[0].2.as_ref().expect("actual input ACK")).unwrap(),
        json!({"caller":caller,"context_binding":context_binding}));
    let mut command_query = connection.prepare("SELECT body FROM commands WHERE task_id=?1 ORDER BY seq LIMIT 16").unwrap();
    let commands = command_query.query_map([&task.id], |row| row.get::<_, String>(0)).unwrap()
        .map(|row| serde_json::from_str::<WorkerCommand>(&row.unwrap()).unwrap()).collect::<Vec<_>>();
    assert!(commands.len() < 16, "finite fixture command inventory");
    let runs = commands.iter().filter_map(|command| match command {
        WorkerCommand::RunAttempt { session, attempt, task, workflow_continuation, .. } =>
            Some((session, attempt, task, workflow_continuation)),
        _ => None,
    }).collect::<Vec<_>>();
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0].1.id, first_attempt.id);
    assert!(runs[0].3.is_none());
    assert_eq!(runs[1].1.id, fresh.id);
    assert_eq!(runs[1].0.id, task.session_id);
    assert_eq!(runs[1].3.as_ref(), Some(&continuation));
    assert_eq!(runs[0].2.limits.model_requests, 2);
    assert_eq!(runs[1].2.limits.model_requests, 1, "ordinary fresh admission retains the debit of request one");
    let mut expected_command_input = (*runs[0].2).clone();
    expected_command_input.limits.model_requests = 1;
    assert_eq!(runs[1].2, &expected_command_input, "only the remaining request allowance changes in the Run copy");
    assert_eq!(connection.query_row("SELECT count(*) FROM sessions", [], |row| row.get::<_, i64>(0)).unwrap(), 1);
    assert_eq!(connection.query_row("SELECT count(*) FROM attempts WHERE task_id=?1", [&task.id], |row| row.get::<_, i64>(0)).unwrap(), 2);
}
