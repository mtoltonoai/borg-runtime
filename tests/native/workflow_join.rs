//! Include as a child module of tests/native_collaboration.rs. This reuses its
//! actual controller/worker process and local Responses peer fixtures.
//! An actual question is posed while required request one is held. Its committed
//! event joins required request two, after the real MCP tool result is retained.
use super::*;
use borg_runtime::collaboration::questions::{PoseQuestion, QuestionMutation, QuestionOption};
use borg_runtime::collaboration::workflow::{ActivityDetail, PrincipalRef, QuestionChange, RecordRef, WorkflowActivity};
use borg_runtime::limits::WorkflowLimits;

const EVENT_TEXT: &str = "Exact workflow event: first line\nsecond line \"quoted\" λ <script>untrusted text</script>";

fn workflow_policy() -> WorkflowLimits {
    WorkflowLimits {
        record_bytes: 65_536, text_bytes: 4096, options: 8, records: 128,
        revisions: 1024, page_items: 8, page_bytes: 65_536, waits_per_task: 8,
        graph_scan_nodes: 256, events_per_mutation: 16, event_bytes: 16_384,
        notice_bytes: 16_384, pending_delivery_bytes: 131_072, delivery_scan_rows: 32,
    }
}

fn workflow_lab(label: &str) -> Lab {
    let mut lab = Lab::new(label);
    let mut limits = (*lab.limits).clone();
    limits.workflow = Some(workflow_policy());
    // The complete V2 validation envelope is 48,175 bytes and 4,387 decoder
    // admissions (including object keys). These are explicit fixture bounds;
    // the production seed is unchanged.
    limits.execution.native.schema_bytes = 49_152;
    limits.execution.native.json_nodes = 8_192;
    limits.validate().unwrap();
    lab.limits = Arc::new(limits);
    std::fs::write(lab.root.join("limits.json"), serde_json::to_vec_pretty(&*lab.limits).unwrap()).unwrap();
    lab
}

async fn pose_event(api: &ApiClient, origin: &str, task: &Task) -> WorkflowActivity {
    let operation = Operation::MutateQuestion {
        operation_id: "pose-native-workflow-question".into(),
        mutation: QuestionMutation::Pose { question: PoseQuestion {
            record: RecordRef::Task { task_id: task.id.clone() },
            recipient: PrincipalRef::Agent { agent_id: task.assigned_agent_id.clone() },
            prompt: EVENT_TEXT.into(),
            options: vec![
                QuestionOption { id: "keep".into(), label: "Keep".into(), explanation: "Keep the existing choice".into() },
                QuestionOption { id: "change".into(), label: "Change".into(), explanation: "Use the explicit alternative".into() },
            ],
            recommended_option_id: "keep".into(),
        }},
    };
    let reply = api.call(operation.clone()).await.unwrap();
    reply.validate_for(&operation).unwrap();
    let _commit = reply.commit.as_ref().expect("actual question transaction must commit");
    let Outcome::Ok { data: Data::QuestionMutation(changed) } = reply.outcome else { panic!("question pose failed: {reply:?}"); };
    let question = changed.question.question;
    let read = api.call(Operation::QueryWorkflowActivity { cursor: None, limit: 8 }).await.unwrap();
    let Outcome::Ok { data: Data::WorkflowActivity(page) } = read.outcome else { panic!("workflow query failed: {read:?}"); };
    assert_eq!(page.items.len(), 1, "one actual question event, no synthetic provider event");
    assert!(page.next_cursor.is_none());
    let event = page.items.into_iter().next().unwrap();
    assert_eq!(event.id.product_revision, question.created_product_revision);
    assert_eq!(question.changed_product_revision, question.created_product_revision);
    assert_eq!(event.subject, RecordRef::Question { question_id: question.id });
    assert_eq!(event.subject_revision, question.revision);
    let ActivityDetail::Question(QuestionChange::Posed { linked_record, recipient, prompt, recommended_option_id }) = &event.detail else { panic!("unexpected canonical event: {event:?}"); };
    assert_eq!(linked_record, &RecordRef::Task { task_id: task.id.clone() });
    assert_eq!(recipient, &PrincipalRef::Agent { agent_id: task.assigned_agent_id.clone() });
    assert_eq!(prompt.text, question.posed.prompt);
    assert!(prompt.text.contains(EVENT_TEXT));
    assert!(!prompt.truncated);
    assert_eq!(recommended_option_id, "keep");
    assert_eq!(page.through.as_ref(), Some(&event.id));
    // The actual browser route reads this same canonical event. This particular
    // posed-question payload has no numeric nested references; only its EventId
    // and subject revision need the documented decimal-string projection.
    let response = reqwest::Client::builder().no_proxy().build().unwrap()
        .get(format!("{origin}/ui/workflow-activity")).send().await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let browser: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    let projected_id = json!({
        "database_id":event.id.database_id,
        "product_revision":event.id.product_revision.to_string(),
        "ordinal":event.id.ordinal.to_string(),
    });
    assert_eq!(browser, json!({
        "items":[{"id":projected_id,"subject":event.subject,
            "subject_revision":event.subject_revision.to_string(),"actor":event.actor,
            "detail":event.detail,"truncated":event.truncated}],
        "through":projected_id,"cursor":serde_json::to_string(&event.id).unwrap(),"has_more":false,
    }));
    event
}

async fn observe_delivery(api: &ApiClient, task: &Task) -> borg_runtime::collaboration::notifications::DeliveryObservation {
    let attempt=task.attempt.as_ref().unwrap();
    let operation=Operation::ObserveCollaborationDelivery {
        caller:borg_runtime::mcp::Caller {task_id:task.id.clone(),attempt_id:attempt.id.clone(),
            generation:attempt.generation,worker_id:attempt.worker_id.clone(),incarnation:attempt.worker_incarnation.clone()},
        limit:8,
    };
    let reply=api.call(operation.clone()).await.unwrap();
    reply.validate_for(&operation).unwrap();
    let Outcome::Ok {data:Data::CollaborationDelivery(observation)}=reply.outcome else {panic!("delivery observation: {reply:?}")};
    *observation
}

async fn reported_requests(api: &ApiClient, task_id: &str, attempt_id: &str) -> u32 {
    let reply=api.call(Operation::QueryCollaboration {
        query:borg_runtime::collaboration::Query::TaskAttempts {task_id:task_id.into(),after:None,limit:8},
    }).await.unwrap();
    let Outcome::Ok {data:Data::Collaboration(borg_runtime::collaboration::QueryResult::TaskAttempts(page))}=reply.outcome else {panic!("attempt query: {reply:?}")};
    page.items.into_iter().find(|row|row.attempt.id==attempt_id).expect("exact current attempt").reported_model_requests
}

fn delivery_state(lab: &Lab, session: &str) -> Option<Value> {
    let connection = rusqlite::Connection::open_with_flags(
        lab.root.join("tasks.sqlite"), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    ).unwrap();
    let raw: Option<String> = connection.query_row(
        "SELECT workflow_delivery FROM sessions WHERE id=?1", [session], |row| row.get(0),
    ).unwrap();
    raw.map(|raw| serde_json::from_str(&raw).unwrap())
}

fn workflow_receipts(lab: &Lab, task: &str) -> Vec<Operation> {
    let connection = rusqlite::Connection::open_with_flags(
        lab.root.join("tasks.sqlite"), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    ).unwrap();
    let mut statement = connection.prepare("SELECT request FROM receipts ORDER BY controller_revision").unwrap();
    let rows = statement.query_map([], |row| row.get::<_, String>(0)).unwrap();
    rows.map(|row| serde_json::from_str::<Operation>(&row.unwrap()).unwrap()).filter(|operation| {
        match operation {
            Operation::PrepareCollaborationDelivery { caller, .. }
            | Operation::BeginCollaborationDelivery { caller, .. }
            | Operation::AcknowledgeCollaborationDelivery { caller, .. } => caller.task_id == task,
            _ => false,
        }
    }).collect()
}

fn ack_count(lab: &Lab, task: &str) -> usize {
    workflow_receipts(lab, task).iter().filter(|op| matches!(op, Operation::AcknowledgeCollaborationDelivery { .. })).count()
}

fn notice_directory(lab: &Lab, task: &Task) -> PathBuf {
    let mut path = lab.state().join("collaboration-notices");
    for (tag, value) in [("task", &task.id), ("session", &task.session_id), ("agent", &task.assigned_agent_id)] {
        path.push(tag);
        let value = process::component(value);
        for chunk in value.as_bytes().chunks(96) { path.push(std::str::from_utf8(chunk).unwrap()); }
    }
    path
}

fn mirror(lab: &Lab, task: &Task) -> Value {
    serde_json::from_slice(&std::fs::read(notice_directory(lab, task).join("delivery.json")).unwrap()).unwrap()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReplyMode { Completed, Unknown, Stop }

async fn native_join(mode: ReplyMode) {
    let label = match mode { ReplyMode::Completed => "workflow-native-complete", ReplyMode::Unknown => "workflow-native-unknown", ReplyMode::Stop => "workflow-native-stop" };
    let lab = workflow_lab(label);
    let mut controller = lab.controller("controller").await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let mut input = lab.input(2);
    input.definition.execution_profile = Some(ExecutionProfile::CollaborationV2 { publication: None, web_search: None });
    input.definition.revision = "workflow-consumer-v2".into();
    let task = submit(&api, input).await;
    let accepted = task.input.resolved_limits.as_ref().expect("actual controller acceptance snapshot");
    assert_eq!(accepted.global_metadata.workflow.as_ref(), lab.limits.workflow.as_ref());
    assert!(matches!(task.input.definition.execution_profile, Some(ExecutionProfile::CollaborationV2 { .. })));

    let first_entered = Arc::new(tokio::sync::Notify::new());
    let first_release = Arc::new(tokio::sync::Notify::new());
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let body_released = Arc::new(AtomicBool::new(false));
    let peer = Peer::start_async({
        let first_entered = first_entered.clone(); let first_release = first_release.clone();
        let entered = entered.clone(); let release = release.clone(); let body_released = body_released.clone();
        move |index, _request| {
            let first_entered = first_entered.clone(); let first_release = first_release.clone();
            let entered = entered.clone(); let release = release.clone(); let body_released = body_released.clone();
            async move {
                if index == 0 {
                    first_entered.notify_one();
                    first_release.notified().await;
                    return Some(function(index, "collab_memory_get", json!({"key":"workflow-join-boundary"})));
                }
                assert_eq!(index, 1, "workflow input cannot cause another model request");
                entered.notify_one();
                release.notified().await;
                if mode == ReplyMode::Completed {
                    body_released.store(true, Ordering::SeqCst);
                    Some(final_value())
                } else { None }
            }
        }
    }).await;
    let mut worker = lab.worker(&lab.url, &peer.endpoint, "workflow-reader");
    tokio::time::timeout(Duration::from_secs(30), first_entered.notified()).await.expect("first actual native HTTP request absent");
    let first_request = peer.requests.lock().unwrap()[0].clone();
    assert!(!first_request["input"].as_array().unwrap().iter().any(|v| v["role"] == "developer"));
    let active = get(&api, &task.id).await;
    assert_eq!(active.status, TaskStatus::Active);
    let event = pose_event(&api, &lab.url, &active).await;
    assert_eq!(peer.count(), 1, "posing the event cannot begin a model turn");
    assert!(!notice_directory(&lab, &task).join("acknowledge.json").exists());
    first_release.notify_one();
    tokio::time::timeout(Duration::from_secs(30), entered.notified()).await.expect("required native tool continuation absent");
    assert!(!body_released.load(Ordering::SeqCst));
    assert_eq!(peer.count(), 2);

    let running = get(&api, &task.id).await;
    let attempt = running.attempt.as_ref().unwrap();
    let context = attempt.id.clone();
    let observation = observe_delivery(&api, &running).await;
    assert!(observation.pending.is_some(), "actual Prepare and Begin must precede HTTP input");
    let before = json!({"acknowledged_cursor":observation.acknowledged_cursor,
        "pending":observation.pending,"last_ack_context":null});
    assert!(before["acknowledged_cursor"]["position"].is_null());
    assert_eq!(before["pending"]["page"]["items"], json!([event.clone()]));
    assert_eq!(before["pending"]["delivery"]["context_binding"], context);
    assert_eq!(before["pending"]["delivery"]["caller"]["attempt_id"], attempt.id);
    assert!(!notice_directory(&lab, &task).join("acknowledge.json").exists(), "HTTP submission alone must not ACK workflow delivery");

    let durable_before = mirror(&lab, &task);
    assert_eq!(durable_before["pending"]["delivery"]["appended"], false);
    let exact_text = durable_before["pending"]["text"].as_str().unwrap();
    let request = peer.requests.lock().unwrap()[1].clone();
    let developer: Vec<_> = request["input"].as_array().unwrap().iter().filter(|v| v["role"] == "developer").collect();
    assert_eq!(developer.len(), 1, "one canonical workflow append at this required boundary");
    assert_eq!(developer[0]["content"], exact_text, "request must carry the exact retained Host text");
    let delivered: Value = serde_json::from_str(exact_text).unwrap();
    assert_eq!(delivered["type"], "collaboration_activity");
    assert_eq!(delivered["events"], json!([event.clone()]));
    assert_eq!(delivered.get("continuation"), Some(&Value::Null), "a posed question alone does not resolve a wait");
    assert_eq!(delivered["events"][0]["detail"]["change"]["prompt"], serde_json::to_value(&event).unwrap()["detail"]["change"]["prompt"]);
    assert_eq!(delivered["batch_id"], before["pending"]["batch_id"]);
    assert_eq!(delivered["scope"]["session_id"], task.session_id);
    assert_eq!(request["input"][0]["content"], task.input.instruction);
    assert_eq!(reported_requests(&api, &task.id, &attempt.id).await, 2, "only the initial request and required tool continuation may be reserved");
    let original: Value = serde_json::from_slice(&std::fs::read(
        lab.state().join("collaboration").join(process::component(&attempt.id)).join("0000000001.json")
    ).unwrap()).unwrap();
    assert_eq!(original["consumed"], true, "tool result must be retained before workflow input joins its continuation");
    let outputs: Vec<_> = request["input"].as_array().unwrap().iter().filter(|v| v["type"] == "function_call_output").collect();
    assert_eq!(outputs.len(), 1);
    let tool_result: Value = serde_json::from_str(outputs[0]["output"].as_str().unwrap()).unwrap();
    assert_eq!(tool_result["outcome"], original["outcome"]);
    assert_eq!(tool_result["commit"], original["outcome_context"]["commit"]);
    assert_eq!(outputs[0]["call_id"], "call-0");

    if mode == ReplyMode::Stop {
        let reply = api.call(Operation::CancelTask {
            operation_id: "cancel-workflow-input-in-flight".into(),
            task_id: task.id.clone(), expected_revision: running.revision,
        }).await.unwrap();
        assert!(matches!(reply.outcome, Outcome::Ok { .. }), "{reply:?}");
        // No response bytes are released; the actual worker observes cancellation.
        until(&api, &task.id, |t| t.status == TaskStatus::Cancelled && t.attempt.as_ref().is_some_and(|a| a.stopped)).await;
        release.notify_one();
    } else { release.notify_one(); }

    let terminal = if mode == ReplyMode::Completed {
        let value = until(&api, &task.id, |t| t.status == TaskStatus::Succeeded).await;
        assert!(body_released.load(Ordering::SeqCst));
        assert_eq!(value.result.as_ref().unwrap().output, Some(json!(7)));
        value
    } else if mode == ReplyMode::Unknown {
        let value = until(&api, &task.id, |t| t.status == TaskStatus::Failed && t.attempt.as_ref().is_some_and(|a| a.stopped && a.uncertain)).await;
        assert!(value.result.is_none());
        value
    } else { get(&api, &task.id).await };
    assert_eq!(peer.count(), 2, "notice delivery must not create a separate turn or retry");
    worker.stop().await;
    controller.stop().await;

    let after = delivery_state(&lab, &task.session_id).unwrap();
    let durable_after = mirror(&lab, &task);
    if mode == ReplyMode::Completed {
        assert_eq!(ack_count(&lab, &task.id), 1);
        assert!(after["pending"].is_null());
        assert_eq!(after["acknowledged_cursor"]["position"], serde_json::to_value(&event.id).unwrap());
        assert!(durable_after["pending"].is_null());
        let ack: Value = serde_json::from_slice(&std::fs::read(notice_directory(&lab, &task).join("acknowledge.json")).unwrap()).unwrap();
        assert_eq!(ack["consumed"], true);
        assert_eq!(ack["request"]["input"]["batch_id"], before["pending"]["batch_id"]);
        assert_eq!(ack["request"]["input"]["context_binding"], context);
    } else {
        assert_eq!(ack_count(&lab, &task.id), 0);
        assert_eq!(after, before, "stop or unknown response must preserve the exact canonical pending batch/intent");
        assert_eq!(durable_after, durable_before, "an unobserved append must remain unresolved in the durable mirror");
        assert!(!notice_directory(&lab, &task).join("acknowledge.json").exists());
    }
    assert_eq!(lab.usage(&terminal.attempt.as_ref().unwrap().id).0, 2);
    let connection = rusqlite::Connection::open_with_flags(lab.root.join("tasks.sqlite"), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let sessions: i64 = connection.query_row("SELECT count(*) FROM sessions", [], |row| row.get(0)).unwrap();
    assert_eq!(sessions, 1, "delivery must not create another session");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_workflow_exact_input_acknowledges_only_after_complete_response() {
    native_join(ReplyMode::Completed).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_workflow_unknown_response_retains_original_pending_input() {
    native_join(ReplyMode::Unknown).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_workflow_stop_before_response_retains_original_pending_input() {
    native_join(ReplyMode::Stop).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_v1_does_not_append_workflow_events_even_when_metadata_is_enabled() {
    let lab = workflow_lab("workflow-native-v1");
    let mut controller = lab.controller("controller").await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let task = submit(&api, lab.input(2)).await;
    assert!(matches!(task.input.definition.execution_profile, Some(ExecutionProfile::CollaborationV1 { .. })));
    assert!(task.input.resolved_limits.as_ref().unwrap().global_metadata.workflow.is_some());
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let peer = Peer::start_async({
        let entered = entered.clone(); let release = release.clone();
        move |index, request| {
            let entered = entered.clone(); let release = release.clone();
            async move {
                assert!(!request["input"].as_array().unwrap().iter().any(|v| v["role"] == "developer"));
                if index == 0 {
                    entered.notify_one(); release.notified().await;
                    Some(function(index, "collab_memory_get", json!({"key":"workflow-join-boundary"})))
                } else { assert_eq!(index, 1); Some(final_value()) }
            }
        }
    }).await;
    let mut worker = lab.worker(&lab.url, &peer.endpoint, "workflow-v1-reader");
    tokio::time::timeout(Duration::from_secs(30), entered.notified()).await.expect("first native V1 request absent");
    let active = get(&api, &task.id).await;
    pose_event(&api, &lab.url, &active).await;
    release.notify_one();
    let complete = until(&api, &task.id, |t| t.status == TaskStatus::Succeeded).await;
    assert_eq!(complete.result.as_ref().unwrap().output, Some(json!(7)));
    assert_eq!(peer.count(), 2);
    worker.stop().await;
    controller.stop().await;
    assert!(workflow_receipts(&lab, &task.id).is_empty());
    assert!(delivery_state(&lab, &task.session_id).is_none());
    assert!(!notice_directory(&lab, &task).exists());
}

/// A bounded unrelated prefix is acknowledged as scanned, not as delivered
/// input. Later ordinary events remain owed for a later supported boundary;
/// they cannot fail this already-required request or manufacture another turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_workflow_unrelated_scan_prefix_defers_without_failing_required_request() {
    let mut lab=workflow_lab("workflow-native-unrelated-prefix");
    let mut limits=(*lab.limits).clone();
    limits.workflow.as_mut().unwrap().delivery_scan_rows=1;
    limits.validate().unwrap();
    lab.limits=Arc::new(limits);
    std::fs::write(lab.root.join("limits.json"),serde_json::to_vec_pretty(&*lab.limits).unwrap()).unwrap();
    let mut controller=lab.controller("controller").await;
    let api=ApiClient::new(&lab.url,lab.limits.clone()).unwrap();
    let mut foreign_input=lab.input(1);
    foreign_input.definition.execution_profile=Some(ExecutionProfile::CollaborationV2{ publication: None,web_search:None});
    foreign_input.definition.revision="workflow-unrelated-v2".into();
    let foreign=submit_identified(&api,foreign_input,"submit-unrelated-workflow-task").await;
    for index in 0..2 {
        let operation=Operation::MutateQuestion {
            operation_id:format!("pose-unrelated-question-{index}"),
            mutation:QuestionMutation::Pose {question:PoseQuestion {
                record:RecordRef::Task{task_id:foreign.id.clone()},
                recipient:PrincipalRef::TrustedHostOperator,
                prompt:format!("Unrelated bounded-prefix question {index}"),
                options:vec![QuestionOption{id:"keep".into(),label:"Keep".into(),explanation:"Keep".into()},
                    QuestionOption{id:"change".into(),label:"Change".into(),explanation:"Change".into()}],
                recommended_option_id:"keep".into(),
            }},
        };
        let reply=api.call(operation.clone()).await.unwrap();
        reply.validate_for(&operation).unwrap();
        assert!(matches!(reply.outcome,Outcome::Ok{data:Data::QuestionMutation(_)}),"{reply:?}");
    }
    let current_foreign=get(&api,&foreign.id).await;
    let cancel=api.call(Operation::CancelTask{operation_id:"cancel-unrelated-before-worker".into(),
        task_id:foreign.id.clone(),expected_revision:current_foreign.revision}).await.unwrap();
    assert!(matches!(cancel.outcome,Outcome::Ok{..}),"{cancel:?}");
    assert!(get(&api,&foreign.id).await.attempt.is_none(),"the unrelated task must never dispatch");
    let read=api.call(Operation::QueryWorkflowActivity{cursor:None,limit:8}).await.unwrap();
    let Outcome::Ok{data:Data::WorkflowActivity(events)}=read.outcome else {panic!("workflow query: {read:?}")};
    assert!(events.items.len()>=2 && events.next_cursor.is_none());
    let first=events.items[0].id.clone();
    let second=events.items[1].id.clone();
    let mut input=lab.input(1);
    input.definition.execution_profile=Some(ExecutionProfile::CollaborationV2{ publication: None,web_search:None});
    input.definition.revision="workflow-required-request-v2".into();
    let task=submit_identified(&api,input,"submit-required-after-unrelated-prefix").await;
    assert_eq!(task.input.resolved_limits.as_ref().unwrap().global_metadata.workflow.as_ref().unwrap().delivery_scan_rows,1);
    let entered=Arc::new(tokio::sync::Notify::new());
    let release=Arc::new(tokio::sync::Notify::new());
    let peer=Peer::start_async({let entered=entered.clone();let release=release.clone();move|index,request|{
        let entered=entered.clone();let release=release.clone();async move {
            assert_eq!(index,0,"bounded catch-up must not start an extra request");
            assert!(!request["input"].as_array().unwrap().iter().any(|item|item["role"]=="developer"));
            entered.notify_one();release.notified().await;Some(final_value())
        }
    }}).await;
    let mut worker=lab.worker(&lab.url,&peer.endpoint,"workflow-unrelated-reader");
    tokio::time::timeout(Duration::from_secs(30),entered.notified()).await.expect("an unrelated prefix must not fail the required model request");
    let active=get(&api,&task.id).await;
    let observation=observe_delivery(&api,&active).await;
    assert_eq!(observation.acknowledged_cursor.position.as_ref(),Some(&first));
    assert!(observation.pending.is_none());
    let still_available=observation.available.expect("later ordinary events remain unscanned");
    assert_eq!(still_available.from_cursor.position.as_ref(),Some(&first));
    assert_eq!(still_available.to_cursor.position.as_ref(),Some(&first));
    let routing=still_available.routing.as_ref().expect("independent bounded routing scan gets its fair boundary");
    assert_eq!(routing.from_cursor,None);assert_eq!(routing.to_cursor.as_ref(),Some(&first));
    assert!(routing.items.is_empty() && routing.has_more);
    assert!(still_available.items.is_empty() && still_available.continuation.is_none());
    assert!(still_available.to_cursor.position.as_ref()<Some(&second),"later ordinary event remains unscanned");
    assert_eq!(peer.count(),1);
    release.notify_one();
    let complete=until(&api,&task.id,|t|t.status==TaskStatus::Succeeded).await;
    assert_eq!(complete.result.as_ref().unwrap().output,Some(json!(7)));
    worker.stop().await;controller.stop().await;
    let retained=delivery_state(&lab,&task.session_id).unwrap();
    assert_eq!(retained["acknowledged_cursor"]["position"],serde_json::to_value(&first).unwrap());
    assert!(retained["pending"].is_null());
    let receipts=workflow_receipts(&lab,&task.id);
    assert_eq!(receipts.iter().filter(|op|matches!(op,Operation::PrepareCollaborationDelivery{..})).count(),1);
    assert_eq!(receipts.iter().filter(|op|matches!(op,Operation::AcknowledgeCollaborationDelivery{..})).count(),1);
    assert!(!receipts.iter().any(|op|matches!(op,Operation::BeginCollaborationDelivery{..})));
    assert_eq!(lab.usage(&complete.attempt.as_ref().unwrap().id).0,1);
    assert_eq!(peer.count(),1);
}

/// The normal second request consumes the concrete posed event and advances
/// the ordinary prefix. The already committed terminal routing obligation then
/// joins the third required tool continuation under the same task/attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_workflow_routes_terminal_event_behind_prefix_at_next_required_boundary() {
    let lab = workflow_lab("workflow-native-routed-terminal");
    let mut controller = lab.controller("controller").await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let mut input = lab.input(3);
    input.definition.execution_profile = Some(ExecutionProfile::CollaborationV2 { publication: None, web_search: None });
    input.definition.revision = "workflow-native-routed-v2".into();
    let task = submit(&api, input).await;
    let first_entered = Arc::new(tokio::sync::Notify::new());
    let first_release = Arc::new(tokio::sync::Notify::new());
    let routed_entered = Arc::new(tokio::sync::Notify::new());
    let routed_release = Arc::new(tokio::sync::Notify::new());
    let peer = Peer::start_async({
        let first_entered=first_entered.clone(); let first_release=first_release.clone();
        let routed_entered=routed_entered.clone(); let routed_release=routed_release.clone();
        move |index, _| {
            let first_entered=first_entered.clone(); let first_release=first_release.clone();
            let routed_entered=routed_entered.clone(); let routed_release=routed_release.clone();
            async move {
                match index {
                    0 => { first_entered.notify_one(); first_release.notified().await;
                        Some(function(index,"collab_memory_get",json!({"key":"first-required-boundary"}))) }
                    1 => Some(function(index,"collab_memory_get",json!({"key":"second-required-boundary"}))),
                    2 => { routed_entered.notify_one(); routed_release.notified().await; Some(final_value()) }
                    _ => panic!("routing created an additional model request"),
                }
            }
        }
    }).await;
    let mut worker=lab.worker(&lab.url,&peer.endpoint,"workflow-routed-reader");
    tokio::time::timeout(Duration::from_secs(30),first_entered.notified()).await.unwrap();
    let active=get(&api,&task.id).await;
    let posed=pose_event(&api,&lab.url,&active).await;
    let RecordRef::Question{question_id}=&posed.subject else{panic!("question event expected")};
    let close=Operation::MutateQuestion{operation_id:"cancel-routed-native-question".into(),
        mutation:QuestionMutation::Cancel{question_id:question_id.clone(),expected_revision:posed.subject_revision,
            reason:"The original requester no longer needs this choice".into()}};
    let closed_receipt=api.call(close.clone()).await.unwrap();
    closed_receipt.validate_for(&close).unwrap();
    assert!(matches!(closed_receipt.outcome,Outcome::Ok{data:Data::QuestionMutation(_)}));
    let read=api.call(Operation::QueryWorkflowActivity{cursor:None,limit:8}).await.unwrap();
    let Outcome::Ok{data:Data::WorkflowActivity(page)}=read.outcome else{panic!("activity query: {read:?}")};
    let closed:Vec<_>=page.items.into_iter().filter(|event|event.subject==posed.subject
        && matches!(&event.detail,ActivityDetail::Question(QuestionChange::Closed{..}))).collect();
    assert_eq!(closed.len(),1);let closed=closed.into_iter().next().unwrap();
    assert!(closed.routing.is_some(),"one immutable original resolution routing intent");
    // The existing browser route exposes this same immutable terminal event,
    // including routing attribution, with only its documented number encoding.
    fn decimal_numbers(value:&mut Value) {
        match value {
            Value::Number(number)=>*value=Value::String(number.to_string()),
            Value::Array(items)=>for item in items { decimal_numbers(item); },
            Value::Object(fields)=>for item in fields.values_mut() { decimal_numbers(item); },
            _=>{},
        }
    }
    let mut expected_browser=serde_json::to_value(&closed).unwrap();decimal_numbers(&mut expected_browser);
    let response=reqwest::Client::builder().no_proxy().build().unwrap()
        .get(format!("{}/ui/workflow-activity",lab.url)).send().await.unwrap();
    assert_eq!(response.status(),reqwest::StatusCode::OK);
    let browser:Value=serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    let browser_event=browser["items"].as_array().unwrap().iter().find(|item|item["id"]==expected_browser["id"]).unwrap();
    assert_eq!(browser_event,&expected_browser);
    assert_eq!(peer.count(),1,"resolution alone cannot start a model request");
    first_release.notify_one();
    tokio::time::timeout(Duration::from_secs(30),routed_entered.notified()).await.unwrap();
    assert_eq!(peer.count(),3);
    let running=get(&api,&task.id).await;
    let attempt=running.attempt.as_ref().unwrap();
    let observed=observe_delivery(&api,&running).await;
    let batch=observed.pending.as_ref().expect("routed input remains pending until complete response");
    let route=batch.page.routing.as_ref().expect("third boundary scans owed routing independently");
    assert_eq!(route.items,vec![closed.clone()]);
    assert!(batch.page.items.is_empty() && batch.page.continuation.is_none());
    assert_eq!(batch.page.from_cursor,batch.page.to_cursor);
    assert!(observed.acknowledged_cursor.position.as_ref()>=Some(&closed.id),"ordinary prefix already passed the original event");
    assert_eq!(batch.delivery.as_ref().unwrap().context_binding,attempt.id);
    let durable=mirror(&lab,&task);
    assert_eq!(durable["pending"]["delivery"]["appended"],false);
    let request=peer.requests.lock().unwrap()[2].clone();
    let notices:Vec<Value>=request["input"].as_array().unwrap().iter().filter(|value|value["role"]=="developer")
        .map(|value|serde_json::from_str(value["content"].as_str().unwrap()).unwrap()).collect();
    assert_eq!(notices.len(),2,"one prior concrete event plus one routed original event");
    assert_eq!(notices[1]["events"],json!([closed.clone()]));
    assert_eq!(notices[1]["batch_id"],batch.batch_id);
    assert_eq!(reported_requests(&api,&task.id,&attempt.id).await,3);
    routed_release.notify_one();
    let terminal=until(&api,&task.id,|t|t.status==TaskStatus::Succeeded).await;
    assert_eq!(terminal.attempt.as_ref().unwrap().id,attempt.id);
    assert_eq!(terminal.result.as_ref().unwrap().output,Some(json!(7)));
    worker.stop().await;controller.stop().await;
    assert_eq!(peer.count(),3);
    assert_eq!(ack_count(&lab,&task.id),2);
    assert!(delivery_state(&lab,&task.session_id).unwrap()["pending"].is_null());
    assert!(mirror(&lab,&task)["pending"].is_null());
    assert_eq!(lab.usage(&attempt.id).0,3);
}

#[path = "workflow_wait_join.rs"]
mod workflow_wait_join;

#[path = "workflow_side_effect_resolution.rs"]
mod workflow_side_effect_resolution;

#[path = "workflow_owner_routing.rs"]
mod workflow_owner_routing;
#[path = "delegation_question_review.rs"]
mod delegation_question_review;

#[path = "workflow_retry.rs"]
mod workflow_retry;

#[path = "workflow_recovery.rs"]
mod workflow_recovery;

#[path = "workflow_model_deadline.rs"]
mod workflow_model_deadline;
