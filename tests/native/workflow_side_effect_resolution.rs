//! Actual worker/controller/MCP with controlled local Responses and reply barriers.
//! The side effect has committed while its original tool future remains in flight.
//! No external provider or supplied-executor-internal tool boundary is qualified.
use super::*;
use borg_runtime::collaboration::questions::{Question, QuestionAnswer, QuestionStatus};
use borg_runtime::collaboration::workflow::{ActorProvenance, AnswerRef, QuestionResolution, TrustedHostPath};
use borg_runtime::governance::{ActualDecisionOutcome, ControlDecision, ControlScope, DecisionDetail, DecisionMode, DecisionPoint, DecisionRef, GovernanceMutation, GovernanceQuery, GovernanceResult};
use borg_runtime::limits::GovernanceLimits;

#[derive(Clone, Copy, PartialEq, Eq)]
enum HeldResolutionMode { Continue, OperatorCancel, GovernanceStop, GovernancePauseThenStop }

impl HeldResolutionMode {
    fn governance(self) -> bool {
        matches!(self, Self::GovernanceStop | Self::GovernancePauseThenStop)
    }
}

async fn governance_change(api: &ApiClient, id: &str, mutation: GovernanceMutation) -> (Operation, Reply) {
    let operation = Operation::MutateGovernance { operation_id: id.into(), mutation };
    let reply = api.call(operation.clone()).await.unwrap();
    reply.validate_for(&operation).unwrap();
    assert_eq!(reply.commit.as_ref().unwrap().operation_id, id);
    assert!(matches!(&reply.outcome, Outcome::Ok { data: Data::Governance(_) }));
    let replay = api.call(operation.clone()).await.unwrap();
    replay.validate_for(&operation).unwrap();
    assert_eq!(replay.outcome, reply.outcome);
    assert_eq!(replay.commit, reply.commit);
    (operation, reply)
}

async fn governance_pause(api: &ApiClient, id: &str) -> (Operation, Reply) {
    let reason = "operator admission hold while original work remains owned";
    let pair = governance_change(api, id, GovernanceMutation::Pause {
        scope: ControlScope::AllTasks, reason: reason.into(),
    }).await;
    let Outcome::Ok { data: Data::Governance(value) } = &pair.1.outcome else { unreachable!() };
    let GovernanceResult::Hold(hold) = value.as_ref() else { panic!("pause must retain its exact hold") };
    assert_eq!(hold.operation_id, id);
    assert_eq!(hold.scope, ControlScope::AllTasks);
    assert_eq!(hold.reason, reason);
    assert_eq!(hold.actor.principal, PrincipalRef::TrustedHostOperator);
    assert_eq!(hold.actor.provenance, ActorProvenance::TrustedHost { path: TrustedHostPath::OperatorApi });
    pair
}

async fn actual_worker_poll(proxy: &Proxy, incarnation: &str, worker: &mut Process, limits: &RuntimeLimits) {
    // An empty original Await may legitimately occupy the selected command
    // long-poll window. Use its existing client deadline, not a shorter fixture timeout.
    let until = tokio::time::Instant::now() + Duration::from_millis(limits.transport.client_timeout_ms);
    while !proxy.await_replies.lock().unwrap().iter().any(|(id, current)| id == "loop-worker" && current == incarnation) {
        assert!(worker.child.try_wait().unwrap().is_none(), "actual worker exited before command response");
        assert!(tokio::time::Instant::now() < until, "actual worker command poll did not complete");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}


async fn open_operator_question(api: &ApiClient, task: &Task) -> Question {
    let operation = Operation::MutateQuestion {
        operation_id: "pose-before-side-effect".into(),
        mutation: QuestionMutation::Pose { question: PoseQuestion {
            record: RecordRef::Task { task_id: task.id.clone() },
            recipient: PrincipalRef::TrustedHostOperator,
            prompt: "Choose the recorded outcome while the original write is in flight".into(),
            options: vec![
                QuestionOption { id: "keep".into(), label: "Keep".into(), explanation: "Retain the write".into() },
                QuestionOption { id: "change".into(), label: "Change".into(), explanation: "Record the alternative".into() },
            ],
            recommended_option_id: "keep".into(),
        }},
    };
    let reply = api.call(operation.clone()).await.unwrap();
    reply.validate_for(&operation).unwrap();
    assert!(reply.commit.is_some());
    let Outcome::Ok { data: Data::QuestionMutation(outcome) } = reply.outcome else {
        panic!("question pose: {reply:?}");
    };
    outcome.question.question
}

async fn answer_held_question(api: &ApiClient, question: &Question) -> WorkflowActivity {
    let operation = Operation::MutateQuestion {
        operation_id: "answer-during-side-effect".into(),
        mutation: QuestionMutation::Answer {
            question_id: question.id.clone(), expected_revision: question.revision,
            answer: QuestionAnswer { option_id: "change".into(), explanation: "Exact committed terminal resolution".into() },
        },
    };
    let reply = api.call(operation.clone()).await.unwrap();
    reply.validate_for(&operation).unwrap();
    assert_eq!(reply.commit.as_ref().unwrap().operation_id, "answer-during-side-effect");
    let Outcome::Ok { data: Data::QuestionMutation(outcome) } = reply.outcome else {
        panic!("question answer: {reply:?}");
    };
    assert_eq!(outcome.question.question.status, QuestionStatus::Answered);
    assert_eq!(outcome.question.question.posed, question.posed);
    let answer = outcome.new_answer.unwrap();
    assert_eq!(answer.operation_id, "answer-during-side-effect");
    let read = api.call(Operation::QueryWorkflowActivity { cursor: None, limit: 8 }).await.unwrap();
    let Outcome::Ok { data: Data::WorkflowActivity(page) } = read.outcome else {
        panic!("workflow activity: {read:?}");
    };
    assert_eq!(page.items.len(), 2, "only the real pose and answer transactions");
    assert!(page.next_cursor.is_none());
    let closed = page.items.last().unwrap().clone();
    assert_eq!(closed.subject, RecordRef::Question { question_id: question.id.clone() });
    assert_eq!(closed.id.product_revision, answer.product_revision);
    assert_eq!(closed.actor, answer.author);
    assert_eq!(closed.detail, ActivityDetail::Question(QuestionChange::Closed {
        resolution: QuestionResolution::Answered {
            question_id: question.id.clone(),
            answer: AnswerRef { question_id: question.id.clone(), answer_id: answer.id },
            selected_option_id: "change".into(),
        },
    }));
    let routing = closed.routing.as_ref().expect("terminal resolution has a retained routing obligation");
    assert_eq!(routing.linked_record, question.posed.record);
    assert_eq!(routing.requested_by, question.poser);
    closed
}

fn held_call_result(held: &HeldToolReply) -> borg_runtime::mcp::CallResult {
    let original = held.original.lock().unwrap().clone().unwrap();
    serde_json::from_str(original["result"]["content"][0]["text"].as_str().unwrap()).unwrap()
}

async fn stored_effect(api: &ApiClient, original: &borg_runtime::mcp::CallResult,
    invocation: &borg_runtime::mcp::Invocation) -> OperationRecord {
    let operation = Operation::GetOperation { operation_id: invocation.operation_id.clone() };
    let reply = api.call(operation.clone()).await.unwrap();
    reply.validate_for(&operation).unwrap();
    assert_eq!(reply.commit, original.reply.commit);
    let Outcome::Ok { data: Data::Operation(record) } = reply.outcome else {
        panic!("original effect lookup: {reply:?}");
    };
    assert_eq!(record.intended, serde_json::to_value(invocation.operation()).unwrap());
    assert_eq!(record.outcome, Some(serde_json::to_value(&original.reply.outcome).unwrap()));
    assert_eq!(memory(api).await, original.reply.outcome, "canonical memory stays at the one committed revision");
    record
}

// Active checks use the controller API and the actual worker's exact journals.
// The complete SQLite inventories are checked only after the owner is reaped.
async fn retained_delivery_request(api: &ApiClient, bytes: &[u8]) -> Operation {
    let journal: Value = serde_json::from_slice(bytes).unwrap();
    assert_eq!(journal["consumed"], true);
    let request: Operation = serde_json::from_value(journal["request"].clone()).unwrap();
    let operation_id = match &request {
        Operation::PrepareCollaborationDelivery { operation_id, .. }
        | Operation::AcknowledgeCollaborationDelivery { operation_id, .. } => operation_id,
        _ => panic!("baseline must contain original scan Prepare and ACK"),
    };
    let lookup = Operation::GetOperation { operation_id: operation_id.clone() };
    let reply = api.call(lookup.clone()).await.unwrap();
    reply.validate_for(&lookup).unwrap();
    assert_eq!(serde_json::to_value(&reply.commit).unwrap(), journal["outcome_context"]["commit"]);
    let Outcome::Ok { data: Data::Operation(record) } = reply.outcome else {
        panic!("original delivery receipt: {reply:?}");
    };
    assert_eq!(record.intended, journal["request"]);
    assert_eq!(record.outcome.as_ref(), Some(&journal["outcome"]));
    let outcome: Outcome = serde_json::from_value(journal["outcome"].clone()).unwrap();
    assert!(matches!(outcome, Outcome::Ok { data: Data::CollaborationDelivery(_) }));
    request
}

fn released_inventory_owner(lab: &Lab) -> std::fs::File {
    // Process::stop is bounded and waits for process exit. Require actual OS
    // owner release before opening SQLite, with no sleep or changed lock mode.
    let lock = std::fs::OpenOptions::new().read(true).write(true)
        .open(lab.root.join("tasks.sqlite.controller-lock")).unwrap();
    lock.try_lock().expect("reaped controller must release its database owner lock");
    lock
}

fn effect_occurrences(lab: &Lab, task_id: &str) -> Vec<borg_runtime::mcp::Invocation> {
    let db = rusqlite::Connection::open_with_flags(lab.root.join("tasks.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut statement = db.prepare("SELECT request FROM receipts ORDER BY controller_revision").unwrap();
    let rows = statement.query_map([], |row| row.get::<_, String>(0)).unwrap();
    rows.filter_map(|row| match serde_json::from_str::<Operation>(&row.unwrap()).unwrap() {
        Operation::CallCollaboration { call } if call.caller.task_id == task_id && call.tool == "collab_memory_set" => Some(call),
        _ => None,
    }).collect()
}

async fn held_resolution(mode: HeldResolutionMode) {
    let label = match mode {
        HeldResolutionMode::Continue => "workflow-held-write",
        HeldResolutionMode::OperatorCancel => "workflow-held-write-cancel",
        HeldResolutionMode::GovernanceStop => "workflow-held-write-governance-stop",
        HeldResolutionMode::GovernancePauseThenStop => "workflow-held-write-governance-pause-stop",
    };
    let mut lab = workflow_lab(label);
    if mode.governance() {
        // Reuse the existing canonical governance fixture policy, with no
        // promotion or change to any production/default metadata.
        let limits = Arc::make_mut(&mut lab.limits);
        limits.governance = Some(GovernanceLimits {
            record_bytes: 65_536, reason_items: 16, reason_bytes: 4_096,
            scan_rows: 512, rule_versions: 16, configuration_versions: 16,
            shadow_evidence_items: 16, completion_continuations: 2, override_max_ms: 10_000,
        });
        limits.validate().unwrap();
        std::fs::write(lab.root.join("limits.json"), serde_json::to_vec_pretty(limits).unwrap()).unwrap();
    }
    let mut controller = lab.controller("controller").await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let mut input = lab.input(2);
    input.definition.execution_profile = Some(ExecutionProfile::CollaborationV2 { web_search: None });
    input.definition.revision = "workflow-held-write-v2".into();
    let task = submit(&api, input).await;
    let accepted_input = task.input.clone();
    assert_eq!(accepted_input.resolved_limits.as_ref().unwrap().global_metadata.workflow.as_ref(), lab.limits.workflow.as_ref());
    // This real operator question predates the worker. Its empty ordinary scan
    // is acknowledged at request one, selecting the routing lane next. The later
    // terminal event can therefore join the next required tool continuation.
    let question = open_operator_question(&api, &task).await;
    let held = Arc::new(HeldToolReply::new("collab_memory_set"));
    let proxy = Proxy::start(lab.url.clone(), None, false, lab.limits.clone()).await;
    *proxy.held_reply.lock().unwrap() = Some(held.clone());
    let next_entered = Arc::new(tokio::sync::Notify::new());
    let next_release = Arc::new(tokio::sync::Notify::new());
    let response_released = Arc::new(AtomicBool::new(false));
    let peer = Peer::start_async({
        let held = held.clone(); let entered = next_entered.clone();
        let release = next_release.clone(); let response_released = response_released.clone();
        move |index, _request| {
            let held = held.clone(); let entered = entered.clone();
            let release = release.clone(); let response_released = response_released.clone();
            async move {
                match index {
                    0 => Some(function(index, "collab_memory_set", json!({
                        "key":"note", "expected_revision":null,
                        "value":{"effect":"original-held-write","sequence":1},
                    }))),
                    1 => {
                        assert!(mode == HeldResolutionMode::Continue, "operator cancellation must prevent continuation");
                        assert!(held.released.load(Ordering::SeqCst), "resolution cannot bypass the original tool reply");
                        entered.notify_one(); release.notified().await;
                        response_released.store(true, Ordering::SeqCst);
                        Some(final_value())
                    }
                    _ => panic!("resolution created an extra model request"),
                }
            }
        }
    }).await;
    let initial_pause = if mode == HeldResolutionMode::GovernancePauseThenStop {
        Some(governance_pause(&api, "governance-pause-before-admission").await)
    } else { None };
    let mut worker = lab.worker(&proxy.url, &peer.endpoint, label);
    if let Some((_, pause)) = initial_pause {
        // This actual worker has one free native slot. A completed command poll
        // under Pause cannot create an attempt or a provider request.
        actual_worker_poll(&proxy, label, &mut worker, &lab.limits).await;
        let queued = get(&api, &task.id).await;
        assert_eq!(queued.status, TaskStatus::Queued);
        assert!(queued.attempt.is_none());
        assert_eq!(queued.input, accepted_input);
        assert_eq!(peer.count(), 0);
        assert_eq!(held.calls.load(Ordering::SeqCst), 0);
        let (_, released) = governance_change(&api, "governance-release-before-admission", GovernanceMutation::ReleaseHold {
            scope: ControlScope::AllTasks,
            expected_operation_id: pause.commit.unwrap().operation_id,
            reason: "allow the original task through ordinary worker admission".into(),
        }).await;
        assert!(matches!(released.outcome, Outcome::Ok {data: Data::Governance(value)}
            if *value == GovernanceResult::Released(ControlScope::AllTasks)));
        // The real admission below is the positive counterpart to the held poll.
    }
    tokio::time::timeout(Duration::from_secs(10), held.entered.notified()).await.expect("committed tool response never reached barrier");
    let active = get(&api, &task.id).await;
    let attempt = active.attempt.as_ref().unwrap().clone();
    assert_eq!(active.status, TaskStatus::Active);
    assert_eq!(attempt.phase, AttemptPhase::Running);
    let original = held_call_result(&held);
    assert!(!original.requires_quiescence);
    assert!(matches!(&original.reply.outcome, Outcome::Ok { data: Data::Memory(Some(value)) }
        if value.revision == 1 && value.value == json!({"effect":"original-held-write","sequence":1})));
    let tool_path = lab.state().join("collaboration").join(process::component(&attempt.id)).join("0000000001.json");
    let tool_before = std::fs::read(&tool_path).unwrap();
    let pending: Value = serde_json::from_slice(&tool_before).unwrap();
    assert!(pending["outcome"].is_null());
    assert_eq!(pending["consumed"], false);
    let invocation: borg_runtime::mcp::Invocation = serde_json::from_value(pending["request"]["input"]["call"].clone()).unwrap();
    assert_eq!(invocation.tool, "collab_memory_set");
    assert_eq!(original.reply.commit.as_ref().unwrap().operation_id, invocation.operation_id);
    let original_record = stored_effect(&api, &original, &invocation).await;
    let notice_path = notice_directory(&lab, &task);
    let prepare_path = notice_path.join("prepare.json");
    let begin_path = notice_path.join("begin.json");
    let ack_path = notice_path.join("acknowledge.json");
    let scan_prepare = std::fs::read(&prepare_path).unwrap();
    let scan_ack = std::fs::read(&ack_path).unwrap();
    let baseline = vec![retained_delivery_request(&api, &scan_prepare).await,
        retained_delivery_request(&api, &scan_ack).await];
    assert!(matches!(&baseline[0], Operation::PrepareCollaborationDelivery { .. }));
    let Operation::AcknowledgeCollaborationDelivery { batch_id: scan_batch, context_binding: scan_context, .. } = &baseline[1]
        else { panic!("original empty-scan ACK absent"); };
    assert!(matches!(&baseline[0], Operation::PrepareCollaborationDelivery { operation_id, .. } if operation_id == scan_batch));
    assert!(!begin_path.exists(), "an empty scan has no Begin input intent");
    let baseline_delivery = observe_delivery(&api, &active).await;
    assert!(baseline_delivery.pending.is_none() && baseline_delivery.available.is_none());
    assert_eq!(baseline_delivery.acknowledged_cursor.position.as_ref().unwrap().product_revision, question.created_product_revision);
    assert!(baseline_delivery.routing_turn);
    let baseline_mirror = mirror(&lab, &task);
    let initial_request = peer.requests.lock().unwrap()[0].clone();
    assert!(!initial_request["input"].as_array().unwrap().iter().any(|item| item["role"] == "developer"),
        "an unrelated scan ACK is not model input");

    let active_pause = if mode == HeldResolutionMode::GovernancePauseThenStop {
        Some(governance_pause(&api, "governance-pause-held-write").await)
    } else { None };
    let closed = answer_held_question(&api, &question).await;
    let owed = observe_delivery(&api, &active).await;
    assert!(owed.pending.is_none());
    let available = owed.available.as_ref().expect("exact resolution remains owed while tool reply is held");
    assert!(available.items.is_empty() && available.continuation.is_none());
    assert_eq!(available.routing.as_ref().unwrap().items, vec![closed.clone()]);
    // Bounded scheduling opportunity, matching the existing held-MCP fixture.
    // The cancellation mode below proves the actual stop path is still live.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let still = get(&api, &task.id).await;
    assert_eq!(still.status, TaskStatus::Active);
    assert_eq!(still.attempt.as_ref(), Some(&attempt));
    assert_eq!(still.input, accepted_input);
    assert_eq!(still.session_id, task.session_id);
    assert!(!held.released.load(Ordering::SeqCst));
    assert_eq!(held.calls.load(Ordering::SeqCst), 1);
    assert_eq!(peer.count(), 1, "a terminal resolution cannot initiate a request");
    assert_eq!(reported_requests(&api, &task.id, &attempt.id).await, 1);
    assert_eq!(std::fs::read(&tool_path).unwrap(), tool_before, "resolution cannot cancel/reconcile/consume the in-flight call");
    assert_eq!(observe_delivery(&api, &still).await, owed,
        "no resolution Prepare, Begin or ACK before the tool completes");
    assert_eq!(std::fs::read(&prepare_path).unwrap(), scan_prepare);
    assert!(!begin_path.exists());
    assert_eq!(std::fs::read(&ack_path).unwrap(), scan_ack);
    assert_eq!(mirror(&lab, &task), baseline_mirror);
    assert_eq!(stored_effect(&api, &original, &invocation).await, original_record);

    if mode != HeldResolutionMode::Continue {
        let control = if mode.governance() {
            let pair = governance_change(&api, "governance-stop-held-write", GovernanceMutation::Stop {
                task_id: task.id.clone(), expected_revision: still.revision,
                reason: "operator stop while original tool reply is held".into(),
            }).await;
            assert!(matches!(&pair.1.outcome, Outcome::Ok {data: Data::Governance(value)}
                if value.as_ref() == &GovernanceResult::StopRequested {task_id:task.id.clone()}));
            Some(pair)
        } else {
            let cancel = Operation::CancelTask { operation_id: "operator-cancel-held-write".into(),
                task_id: task.id.clone(), expected_revision: still.revision };
            let reply = api.call(cancel.clone()).await.unwrap();
            reply.validate_for(&cancel).unwrap();
            assert!(matches!(reply.outcome, Outcome::Ok { .. }));
            None
        };
        let stopped = tokio::time::timeout(Duration::from_secs(5), until(&api, &task.id, |task|
            task.status == TaskStatus::Cancelled && task.attempt.as_ref().is_some_and(|attempt| attempt.stopped)))
            .await.expect("operator cancellation must stop the worker before releasing the tool reply");
        assert!(!held.released.load(Ordering::SeqCst));
        assert!(stopped.result.is_none());
        assert_eq!(stopped.attempt.as_ref().unwrap().id, attempt.id);
        assert_eq!(stopped.attempt.as_ref().unwrap().phase, AttemptPhase::Stopped);
        assert!(stopped.attempt.as_ref().unwrap().uncertain,
            "physical stop cannot establish delivery of the held tool response");
        assert_eq!(stopped.input, accepted_input);
        assert_eq!(stopped.session_id, task.session_id);
        assert_eq!(peer.count(), 1);
        assert_eq!(std::fs::read(&prepare_path).unwrap(), scan_prepare);
        assert!(!begin_path.exists());
        assert_eq!(std::fs::read(&ack_path).unwrap(), scan_ack);
        assert_eq!(mirror(&lab, &task), baseline_mirror);
        // Cancellation interrupts the in-flight HTTP future before receipt
        // retention. The canonical commit below proves the one external effect;
        // it does not establish that this worker received its held response.
        let retained_tool = std::fs::read(&tool_path).unwrap();
        assert_eq!(retained_tool, tool_before,
            "stop preserves the exact original unresolved invocation");
        let unresolved: Value = serde_json::from_slice(&retained_tool).unwrap();
        assert!(unresolved["outcome"].is_null());
        assert!(unresolved["outcome_context"].is_null());
        assert_eq!(unresolved["consumed"], false, "cancelled result was never supplied to a model continuation");
        assert_eq!(stored_effect(&api, &original, &invocation).await, original_record);
        assert_eq!(held.calls.load(Ordering::SeqCst), 1, "cancellation must not repeat the side effect");
        if let Some((operation, original_stop)) = &control {
            let replay = api.call(operation.clone()).await.unwrap();
            replay.validate_for(operation).unwrap();
            assert_eq!(replay.commit, original_stop.commit);
            assert_eq!(replay.outcome, original_stop.outcome);
            let decision = Operation::QueryGovernance { query: GovernanceQuery::Decision {
                decision: DecisionRef {operation_id:"governance-stop-held-write".into(),point:DecisionPoint::EmergencyControl},
            }};
            let audited = api.call(decision.clone()).await.unwrap();
            audited.validate_for(&decision).unwrap();
            let Outcome::Ok {data:Data::Governance(value)} = audited.outcome else {panic!("original emergency audit absent")};
            let GovernanceResult::Decision(record) = *value else {panic!("wrong audit")};
            assert_eq!(record.controller_revision, original_stop.commit.as_ref().unwrap().controller_revision);
            assert_eq!(record.mode, DecisionMode::Enforcing);
            assert_eq!(record.actual_outcome, ActualDecisionOutcome::Applied);
            assert_eq!(record.detail, DecisionDetail::EmergencyControl(ControlDecision::StopRequested {task_id:task.id.clone()}));
            assert_eq!(record.actor.principal, PrincipalRef::TrustedHostOperator);
            assert_eq!(record.actor.provenance, ActorProvenance::TrustedHost {path:TrustedHostPath::OperatorApi});
            assert_eq!(record.id, DecisionRef {operation_id:"governance-stop-held-write".into(),point:DecisionPoint::EmergencyControl});
            if let Some((pause, original_pause)) = &active_pause {
                let replay = api.call(pause.clone()).await.unwrap();
                replay.validate_for(pause).unwrap();
                assert_eq!(replay.commit, original_pause.commit);
                assert_eq!(replay.outcome, original_pause.outcome);
            }
        }
        held.release.notify_one();
        worker.stop().await;
        if mode.governance() {
            let before_registration = proxy.registrations.load(Ordering::SeqCst);
            let mut replacement = lab.worker(&proxy.url, &peer.endpoint, "governance-stopped-replacement");
            actual_worker_poll(&proxy, "governance-stopped-replacement", &mut replacement, &lab.limits).await;
            assert!(proxy.registrations.load(Ordering::SeqCst) > before_registration);
            let retained = get(&api, &task.id).await;
            assert_eq!(retained.status, TaskStatus::Cancelled);
            assert_eq!(retained.attempt.as_ref().unwrap().id, attempt.id);
            assert!(retained.attempt.as_ref().unwrap().stopped && retained.attempt.as_ref().unwrap().uncertain);
            assert_eq!(retained.input, accepted_input);
            assert_eq!(retained.session_id, task.session_id);
            assert_eq!(std::fs::read(&tool_path).unwrap(), tool_before,
                "replacement cannot infer delivery from the canonical effect receipt");
            assert_eq!(stored_effect(&api, &original, &invocation).await, original_record);
            assert_eq!(peer.count(), 1);
            assert_eq!(held.calls.load(Ordering::SeqCst), 1);
            replacement.stop().await;
        }
        controller.stop().await;
        let _owner = released_inventory_owner(&lab);
        assert_eq!(std::fs::read(&tool_path).unwrap(), tool_before,
            "late reply release and shutdown cannot resolve or consume the stopped invocation");
        assert_eq!(serde_json::to_value(workflow_receipts(&lab, &task.id)).unwrap(), serde_json::to_value(&baseline).unwrap(),
            "the entire receipt inventory contains only the two original scan operations");
        assert_eq!(ack_count(&lab, &task.id), 1, "operator cancellation cannot acknowledge the terminal input");
        let retained = delivery_state(&lab, &task.session_id).unwrap();
        // Derive the complete persisted-state expectation from the canonical
        // pre-resolution observation and the exact committed scan ACK context.
        let mut expected = serde_json::to_value(&baseline_delivery).unwrap();
        let fields = expected.as_object_mut().unwrap();
        fields.remove("available");
        fields.insert("last_ack_context".into(), json!(scan_context));
        assert_eq!(retained, expected);
        assert_eq!(effect_occurrences(&lab, &task.id), vec![invocation]);
        assert_eq!(lab.usage(&attempt.id), (1, Some(5), Some(2)));
        return;
    }

    held.release.notify_one();
    tokio::time::timeout(Duration::from_secs(30), next_entered.notified()).await.expect("ordinary tool continuation absent");
    let running = get(&api, &task.id).await;
    assert_eq!(running.attempt.as_ref().unwrap().id, attempt.id);
    let observed = observe_delivery(&api, &running).await;
    let batch = observed.pending.as_ref().expect("actual routed input pending until provider response");
    assert_eq!(batch.page.routing.as_ref().unwrap().items, vec![closed.clone()]);
    assert!(batch.page.items.is_empty() && batch.page.continuation.is_none());
    assert_eq!(batch.page.from_cursor, batch.page.to_cursor);
    assert_eq!(batch.delivery.as_ref().unwrap().caller, invocation.caller);
    assert_eq!(batch.delivery.as_ref().unwrap().context_binding, attempt.id);
    let durable = mirror(&lab, &task);
    assert_eq!(durable["pending"]["delivery"]["appended"], false);
    let exact_text = durable["pending"]["text"].as_str().unwrap();
    let request = peer.requests.lock().unwrap()[1].clone();
    let items = request["input"].as_array().unwrap();
    let outputs: Vec<_> = items.iter().enumerate().filter(|(_, item)| item["type"] == "function_call_output").collect();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].1["call_id"], invocation.logical_call_id);
    let output: Value = serde_json::from_str(outputs[0].1["output"].as_str().unwrap()).unwrap();
    assert_eq!(output, json!({"operation_id":invocation.operation_id,"logical_call_id":invocation.logical_call_id,
        "outcome":original.reply.outcome,"commit":original.reply.commit}));
    let notices: Vec<_> = items.iter().enumerate().filter_map(|(position, item)| {
        if item["role"] != "developer" { return None; }
        let text = item["content"].as_str()?;
        let notice: Value = serde_json::from_str(text).ok()?;
        (notice["type"] == "collaboration_activity").then_some((position, text, notice))
    }).collect();
    assert_eq!(notices.len(), 1);
    assert!(notices[0].0 > outputs[0].0, "exact resolution follows the retained original tool result");
    assert_eq!(notices[0].1, exact_text);
    assert_eq!(notices[0].2["events"], json!([closed]));
    assert_eq!(notices[0].2["batch_id"], batch.batch_id);
    assert_eq!(notices[0].2["scope"]["session_id"], task.session_id);
    assert!(notices[0].2["continuation"].is_null());
    let consumed: Value = serde_json::from_slice(&std::fs::read(&tool_path).unwrap()).unwrap();
    assert_eq!(consumed["consumed"], true);
    assert_eq!(consumed["outcome"], serde_json::to_value(&original.reply.outcome).unwrap());
    assert_eq!(consumed["outcome_context"]["commit"], serde_json::to_value(&original.reply.commit).unwrap());
    assert!(!response_released.load(Ordering::SeqCst));
    assert_eq!(std::fs::read(&ack_path).unwrap(), scan_ack, "dispatch cannot create or consume a new input ACK");
    assert_eq!(observed.acknowledged_cursor, baseline_delivery.acknowledged_cursor);
    assert_eq!(observed.routing_cursor, baseline_delivery.routing_cursor);
    assert_eq!(observed.routing_turn, baseline_delivery.routing_turn);
    assert_eq!(reported_requests(&api, &task.id, &attempt.id).await, 2);
    let batch_id = batch.batch_id.clone();
    next_release.notify_one();
    let terminal = until(&api, &task.id, |task| task.status == TaskStatus::Succeeded
        && task.attempt.as_ref().is_some_and(|attempt| attempt.stopped)).await;
    assert_eq!(terminal.result.as_ref().unwrap().output, Some(json!(7)));
    assert_eq!(terminal.attempt.as_ref().unwrap().id, attempt.id);
    assert_eq!(terminal.attempt.as_ref().unwrap().deadline_unix_ms, attempt.deadline_unix_ms);
    assert_eq!(terminal.input, accepted_input);
    assert_eq!(terminal.session_id, task.session_id);
    assert_eq!(stored_effect(&api, &original, &invocation).await, original_record);
    assert_eq!(held.calls.load(Ordering::SeqCst), 1);
    assert_eq!(peer.count(), 2);
    worker.stop().await; controller.stop().await;
    let _owner = released_inventory_owner(&lab);
    assert_eq!(effect_occurrences(&lab, &task.id), vec![invocation]);
    let receipts = workflow_receipts(&lab, &task.id);
    assert_eq!(serde_json::to_value(&receipts[..baseline.len()]).unwrap(), serde_json::to_value(&baseline).unwrap());
    let added = &receipts[baseline.len()..];
    assert_eq!(added.len(), 3, "one Prepare, Begin and input ACK for the original terminal event");
    assert!(matches!(&added[0], Operation::PrepareCollaborationDelivery { operation_id, .. } if operation_id == &batch_id));
    assert!(matches!(&added[1], Operation::BeginCollaborationDelivery { batch_id: id, context_binding, .. }
        if id == &batch_id && context_binding == &attempt.id));
    assert!(matches!(&added[2], Operation::AcknowledgeCollaborationDelivery { batch_id: id, context_binding, .. }
        if id == &batch_id && context_binding == &attempt.id));
    assert_eq!(ack_count(&lab, &task.id), 2, "one earlier scan ACK and exactly one actual input ACK");
    assert!(delivery_state(&lab, &task.session_id).unwrap()["pending"].is_null());
    assert!(mirror(&lab, &task)["pending"].is_null());
    assert_eq!(lab.usage(&attempt.id), (2, Some(10), Some(4)));
}

// Traceability scope: controlled native Responses plus actual worker/controller/MCP;
// the original committed side-effecting tool response stays in flight until release.
//= spec/impl/v2/implementation.md#committed-resolutions-reach-their-consumers
//= type=test
//# A resolution notice MUST NOT cancel a running side-effecting tool.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_terminal_resolution_preserves_running_side_effect_until_original_reply() {
    held_resolution(HeldResolutionMode::Continue).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_operator_cancel_interrupts_same_held_side_effect_without_reexecution() {
    held_resolution(HeldResolutionMode::OperatorCancel).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_governance_stop_interrupts_held_effect_and_replacement_preserves_uncertainty() {
    held_resolution(HeldResolutionMode::GovernanceStop).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_governance_pause_blocks_admission_preserves_running_effect_until_explicit_stop() {
    held_resolution(HeldResolutionMode::GovernancePauseThenStop).await;
}
