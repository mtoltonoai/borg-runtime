//! One actual Controller/worker conjunction: required tool continuation is
//! throttled once, retaining its original tool result and unacknowledged notice.
//! Local HTTP only; existing workflow Lab/Proxy and bounded status-peer pattern.
use super::*;
use borg_runtime::{
    limits::{NativeCircuitLimits, NativeRetryLimits},
    native::{ProviderRequestBinding, RejectionKind, RetryAfter},
    native_loop_contract as contract,
};

async fn original(api: &ApiClient, id: &str) -> (Reply, OperationRecord) {
    let operation = Operation::GetOperation { operation_id: id.into() };
    let reply = api.call(operation.clone()).await.unwrap();
    reply.validate_for(&operation).unwrap();
    assert_eq!(reply.commit.as_ref().unwrap().operation_id, id);
    let Outcome::Ok { data: Data::Operation(row) } = &reply.outcome else {
        panic!("original canonical receipt absent: {reply:?}");
    };
    assert_eq!(row.id, id);
    assert!(matches!(serde_json::from_value::<Outcome>(row.outcome.clone().unwrap()).unwrap(), Outcome::Ok { .. }));
    let row = row.clone();
    (reply, row)
}

fn report(row: &OperationRecord) -> Observation {
    let Operation::ObserveWorker { report } = serde_json::from_value(row.intended.clone()).unwrap() else {
        panic!("expected canonical worker observation");
    };
    report.observation
}

struct RetryPeer {
    endpoint: String,
    bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    charges: Arc<Mutex<Vec<u64>>>,
    retry_delay: Arc<Mutex<Option<Duration>>>,
    owner: Option<tokio::task::JoinHandle<()>>,
}
impl RetryPeer {
    async fn start(api: ApiClient, task: Task, policy: Arc<RuntimeLimits>,
        second_entered: Arc<tokio::sync::Notify>, second_release: Arc<tokio::sync::Notify>,
        third_entered: Arc<tokio::sync::Notify>, third_release: Arc<tokio::sync::Notify>, refusal: Value) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
        let bodies = Arc::new(Mutex::new(Vec::new())); let captured = bodies.clone();
        let charges = Arc::new(Mutex::new(Vec::new())); let debits = charges.clone();
        let retry_delay = Arc::new(Mutex::new(None)); let delay = retry_delay.clone();
        let owner = tokio::spawn(async move {
            let mut rejected_at = None;
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let bytes = tokio::time::timeout(Duration::from_secs(10), async {
                    let mut head = Vec::new();
                    while !head.ends_with(b"\r\n\r\n") {
                        head.push(stream.read_u8().await.unwrap());
                        assert!(head.len() <= policy.transport.http_header_bytes);
                    }
                    let head = String::from_utf8(head).unwrap();
                    assert!(head.starts_with("POST /v1/responses "));
                    let length = head.lines().find_map(|line| { let (k, v) = line.split_once(':')?;
                        k.eq_ignore_ascii_case("content-length").then(||v.trim().parse::<usize>().unwrap()) }).unwrap();
                    assert!(length <= policy.execution.native.request_bytes);
                    let mut bytes = vec![0; length]; stream.read_exact(&mut bytes).await.unwrap(); bytes
                }).await.expect("bounded local request body");
                let request: Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(request["model"], "gpt-6-astra");
                assert_eq!(request["reasoning"], json!({"effort":"max"}));
                let index = { let mut all = captured.lock().unwrap(); let i = all.len();
                    assert!(i < 3, "no fourth POST is authorized"); all.push(bytes); i };
                let current = get(&api, &task.id).await; let attempt = current.attempt.as_ref().unwrap();
                assert_eq!(current.status, TaskStatus::Active); assert!(!attempt.stopped && !attempt.uncertain);
                assert_eq!(current.input, task.input); assert_eq!(current.session_id, task.session_id);
                assert_eq!(attempt.deadline_unix_ms, task.input.limits.deadline_unix_ms);
                let request_index = u32::try_from(index + 1).unwrap();
                let id = contract::reservation_id(&attempt.id, request_index);
                let (charged, row) = original(&api, &id).await;
                let Operation::ObserveWorker { report: observed } = serde_json::from_value(row.intended).unwrap() else { panic!("original debit"); };
                assert_eq!(observed.report_id, id); assert_eq!(observed.task_id, task.id);
                assert_eq!(observed.attempt_id, attempt.id); assert_eq!(observed.generation, attempt.generation);
                assert_eq!(observed.worker_id, attempt.worker_id); assert_eq!(observed.incarnation, attempt.worker_incarnation);
                let (expected_input, expected_output) = if index == 0 { (None, None) } else { (Some(5), Some(2)) };
                assert!(matches!(observed.observation, Observation::Usage { model_requests,
                    native_diagnostic_reserved: true, input_tokens, output_tokens }
                    if model_requests == request_index && input_tokens == expected_input && output_tokens == expected_output));
                debits.lock().unwrap().push(charged.commit.unwrap().controller_revision);
                let (status, extra, body) = match index {
                    0 => (200, "", function(0, "collab_memory_get", json!({"key":"workflow-retry-original"}))),
                    1 => {
                        second_entered.notify_one(); second_release.notified().await;
                        rejected_at = Some(tokio::time::Instant::now());
                        (429, "retry-after-ms: 5\r\n", refusal.clone())
                    }
                    2 => {
                        *delay.lock().unwrap() = Some(rejected_at.unwrap().elapsed());
                        { let all = captured.lock().unwrap();
                            assert_eq!(all[1], all[2], "retry must retain exact original continuation HTTP bytes"); }
                        third_entered.notify_one(); third_release.notified().await;
                        (200, "", final_value())
                    }
                    _ => unreachable!(),
                };
                let body = serde_json::to_vec(&body).unwrap();
                stream.write_all(format!("HTTP/1.1 {status} fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{extra}Connection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
                stream.write_all(&body).await.unwrap(); stream.shutdown().await.unwrap();
            }
        });
        Self { endpoint, bodies, charges, retry_delay, owner: Some(owner) }
    }
    fn count(&self) -> usize { self.bodies.lock().unwrap().len() }
    async fn close(&mut self) {
        let owner = self.owner.take().unwrap(); owner.abort();
        if let Err(error) = owner.await { assert!(error.is_cancelled(), "local peer panicked: {error}"); }
    }
}
impl Drop for RetryPeer { fn drop(&mut self) { if let Some(owner) = &self.owner { owner.abort(); } } }

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn known_429_tool_continuation_retains_original_call_and_defers_notice_ack() {
    exercise_retry("workflow-native-retry", json!({"error":{"code":"rate_limit_exceeded","type":"rate_limit_error"}}), true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_down_tool_continuation_retains_original_call_and_defers_notice_ack() {
    exercise_retry("workflow-native-slow-down", json!({"error":{"code":"slow_down","type":"rate_limit_error"}}), true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tokens_tool_continuation_retains_original_call_and_defers_notice_ack() {
    exercise_retry("workflow-native-tokens", json!({"error":{"code":"rate_limit_exceeded","type":"tokens","message":"synthetic quota refusal","param":null}}), true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_throttle_shape_preserves_partial_tool_state_without_retry() {
    exercise_retry("workflow-native-unknown-throttle", json!({"error":{"code":"rate_limit_exceeded","type":"requests","message":"PRIVATE_PROMPT","param":null}}), false).await;
}

async fn exercise_retry(label: &str, refusal: Value, expect_retry: bool) {
    let mut lab = workflow_lab(label);
    let mut policy = (*lab.limits).clone();
    // The shared workflow fixture predates the machine-resource tools and read continuation.
    // Select the current complete V2 catalogue within explicit local bounds;
    // production metadata and the original retry/effect oracles stay unchanged.
    policy.execution.native.function_count = 67;
    policy.execution.native.schema_bytes = 65_536;
    assert_eq!(borg_runtime::mcp::catalogue_for_profile(&policy,
        Some(&ExecutionProfile::CollaborationV2 { publication: None, web_search: None })).len(), 67);
    assert_eq!(borg_runtime::mcp::catalogue_for_profile(&policy,
        Some(&ExecutionProfile::CollaborationV2 { publication: None, web_search: None })).iter()
        .filter(|tool| tool.name == "collab_read_continue").count(), 1);
    // Same explicit bounded local retry selection as charter_retry's peer; no
    // production/default metadata selection or actual provider call is changed.
    policy.native_retry = Some(NativeRetryLimits { initial_backoff_ms: 5, backoff_multiplier: 2,
        max_backoff_ms: 20, jitter_ms: 1, circuit: NativeCircuitLimits { failure_threshold: 4,
            open_ms: 50, max_entries: 16 }, auth_cooldown_ms: 150, auth_max_entries: 16,
        coordination_record_bytes: 32_768 });
    policy.validate().unwrap(); lab.limits = Arc::new(policy);
    std::fs::write(lab.root.join("limits.json"), serde_json::to_vec_pretty(&*lab.limits).unwrap()).unwrap();
    let mut controller = lab.controller("workflow-retry-controller").await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let mut input = lab.input(3);
    input.definition.execution_profile = Some(ExecutionProfile::CollaborationV2 { publication: None, web_search: None });
    input.definition.revision = "workflow-retry-v2".into();
    let task = submit(&api, input).await;
    assert_eq!(task.input.resolved_limits.as_ref().unwrap().global_metadata.native_retry, lab.limits.native_retry);
    assert_fixture_native_profile_fits(&lab.limits, &task.input, 67);
    let second_entered = Arc::new(tokio::sync::Notify::new()); let second_release = Arc::new(tokio::sync::Notify::new());
    let third_entered = Arc::new(tokio::sync::Notify::new()); let third_release = Arc::new(tokio::sync::Notify::new());
    let mut peer = RetryPeer::start(api.clone(), task.clone(), lab.limits.clone(), second_entered.clone(),
        second_release.clone(), third_entered.clone(), third_release.clone(), refusal).await;
    let proxy = Proxy::start(lab.url.clone(), None, false, lab.limits.clone()).await;
    let held = Arc::new(HeldToolReply::new("collab_memory_get"));
    *proxy.held_reply.lock().unwrap() = Some(held.clone());
    let mut worker = lab.worker(&proxy.url, &peer.endpoint, "workflow-retry-worker");
    tokio::time::timeout(Duration::from_secs(20), held.entered.notified()).await.expect("original tool not reached");
    let active = get(&api, &task.id).await; assert_eq!(active.status, TaskStatus::Active);
    let event = pose_event(&api, &lab.url, &active).await;
    assert_eq!(peer.count(), 1, "event must not create a model turn while original tool reply is held");
    assert!(!notice_directory(&lab, &task).join("acknowledge.json").exists());
    held.release.notify_one();
    tokio::time::timeout(Duration::from_secs(30), second_entered.notified()).await.expect("original required continuation absent");
    let running = get(&api, &task.id).await; let attempt = running.attempt.as_ref().unwrap().clone();
    let before = observe_delivery(&api, &running).await;
    let before_json = serde_json::to_value(&before).unwrap();
    assert!(before.pending.is_some());
    assert_eq!(before_json["pending"]["page"]["items"], json!([event.clone()]));
    assert_eq!(before_json["pending"]["delivery"]["context_binding"], attempt.id);
    assert!(before_json["acknowledged_cursor"]["position"].is_null());
    let durable = mirror(&lab, &task);
    assert_eq!(durable["pending"]["delivery"]["appended"], false);
    let journal_path = lab.state().join("collaboration").join(process::component(&attempt.id)).join("0000000001.json");
    let tool_bytes = std::fs::read(&journal_path).unwrap();
    let tool: Value = serde_json::from_slice(&tool_bytes).unwrap(); assert_eq!(tool["consumed"], true);
    let request: Value = serde_json::from_slice(&peer.bodies.lock().unwrap()[1]).unwrap();
    let values = request["input"].as_array().unwrap();
    let notices: Vec<_> = values.iter().filter(|v| v["role"] == "developer").collect();
    assert_eq!(notices.len(), 1); assert_eq!(notices[0]["content"], durable["pending"]["text"]);
    let outputs: Vec<_> = values.iter().filter(|v| v["type"] == "function_call_output").collect();
    assert_eq!(outputs.len(), 1); assert_eq!(outputs[0]["call_id"], "call-0");
    let observed: Value = serde_json::from_str(outputs[0]["output"].as_str().unwrap()).unwrap();
    assert_eq!(observed["outcome"], tool["outcome"]); assert_eq!(observed["commit"], tool["outcome_context"]["commit"]);
    assert_eq!(reported_requests(&api, &task.id, &attempt.id).await, 2);
    assert!(!notice_directory(&lab, &task).join("acknowledge.json").exists());
    second_release.notify_one();
    if !expect_retry {
        let terminal = until(&api, &task.id, |t| t.status == TaskStatus::Failed && t.attempt.as_ref().is_some_and(|a| a.stopped && a.uncertain)).await;
        assert!(terminal.result.is_none());
        assert_eq!(terminal.input, task.input);
        assert_eq!(terminal.session_id, task.session_id);
        assert_eq!(terminal.attempt.as_ref().unwrap().id, attempt.id);
        assert_eq!(peer.count(), 2, "unknown response cannot authorize another POST");
        assert_eq!(held.calls.load(Ordering::SeqCst), 1);
        assert_eq!(std::fs::read(&journal_path).unwrap(), tool_bytes);
        assert_eq!(mirror(&lab, &task), durable);
        assert!(!notice_directory(&lab, &task).join("acknowledge.json").exists());
        let (_, diagnostic) = original(&api, &contract::diagnostic_id(&attempt.id, 2)).await;
        let Observation::NativeRequestDiagnostic { request_binding, observed_outcome, failure, .. } = report(&diagnostic) else { panic!("original unknown diagnostic"); };
        assert_eq!(observed_outcome, contract::NativeRequestObservedOutcome::Unknown);
        assert_eq!(failure.unwrap().http.unwrap().category, borg_runtime::native::CategoryDiagnostic::Absent);
        let bytes = peer.bodies.lock().unwrap()[1].clone();
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &bytes);
        assert_eq!(request_binding, Some(ProviderRequestBinding { bytes: u64::try_from(bytes.len()).unwrap(), sha256: digest.as_ref().iter().map(|b| format!("{b:02x}")).collect() }));
        let rejection = api.call(Operation::GetOperation { operation_id: contract::rejection_id(&attempt.id, 2) }).await.unwrap();
        assert!(matches!(rejection.outcome, Outcome::Refused { ref code, .. } if code == "not_found"));
        worker.stop().await;
        // The final acceptance-refusal Progress may follow stopped; capture the
        // settled public task after stopping the worker before asserting reopen.
        let stable = get(&api, &task.id).await;
        assert_eq!(stable.input, task.input);
        assert_eq!(stable.session_id, task.session_id);
        assert_eq!(stable.attempt, terminal.attempt);
        assert_eq!(stable.status, TaskStatus::Failed);
        assert!(stable.result.is_none());
        controller.stop().await;
        assert_eq!(ack_count(&lab, &task.id), 0);
        assert_eq!(lab.usage(&attempt.id), (2, Some(5), Some(2)));
        {
            let db = rusqlite::Connection::open_with_flags(lab.root.join("tasks.sqlite"), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
            // Delivery observation requires an active caller. After terminal
            // failure, inspect its retained canonical state only with both
            // owners stopped; an Unknown response must not acknowledge it.
            let raw: String = db.query_row("SELECT workflow_delivery FROM sessions WHERE id=?1",
                [&task.session_id], |row| row.get(0)).unwrap();
            let retained: Value = serde_json::from_str(&raw).unwrap();
            for field in ["pending", "acknowledged_cursor", "routing_cursor", "superseded"] {
                assert_eq!(retained[field], before_json[field]);
            }
            assert_eq!(retained["routing_turn"].as_bool().unwrap_or(false),
                before_json["routing_turn"].as_bool().unwrap_or(false));
            assert!(retained["last_ack_context"].is_null());
            let mut query = db.prepare("SELECT body FROM history WHERE task_id=?1 AND kind='worker_observation' ORDER BY seq").unwrap();
            let rows: Vec<String> = query.query_map([&task.id], |row| row.get(0)).unwrap().collect::<Result<_, _>>().unwrap();
            let uncertainty = rows.iter().map(|body| serde_json::from_str::<Value>(body).unwrap())
                .find(|value| value["kind"] == "execution_uncertain").expect("retained uncertainty event");
            let message = uncertainty["message"].as_str().unwrap();
            assert!(message.contains("classification_refused=unsupported_type"));
            assert!(!message.contains("PRIVATE_PROMPT"));
        }
        let mut controller = lab.controller("workflow-unknown-restart").await;
        assert_eq!(get(&api, &task.id).await, stable);
        assert_eq!(original(&api, &contract::diagnostic_id(&attempt.id, 2)).await.1, diagnostic);
        assert_eq!(peer.count(), 2);
        controller.stop().await;
        assert_eq!(std::fs::read(&journal_path).unwrap(), tool_bytes);
        peer.close().await;
        return;
    }
    tokio::time::timeout(Duration::from_secs(30), third_entered.notified()).await.expect("authorized retry absent");
    assert_eq!(peer.count(), 3); assert!(peer.retry_delay.lock().unwrap().unwrap() >= Duration::from_millis(5));
    assert_eq!(held.calls.load(Ordering::SeqCst), 1, "retained call must not be dispatched again");
    assert_eq!(std::fs::read(&journal_path).unwrap(), tool_bytes);
    assert_eq!(mirror(&lab, &task), durable);
    assert_eq!(serde_json::to_value(observe_delivery(&api, &running).await).unwrap(), before_json);
    assert!(!notice_directory(&lab, &task).join("acknowledge.json").exists(), "429 cannot acknowledge notice input");
    assert_eq!(reported_requests(&api, &task.id, &attempt.id).await, 3);
    let rejection_id = contract::rejection_id(&attempt.id, 2);
    let (rejected_reply, rejected) = original(&api, &rejection_id).await;
    let Observation::NativeRequestRejected { request_index, endpoint, model, rejection, request_binding, .. } = report(&rejected) else { panic!("exact known429 rejection missing"); };
    assert_eq!(request_index, 2); assert_eq!(endpoint, peer.endpoint); assert_eq!(model, "gpt-6-astra");
    assert_eq!(rejection.kind, RejectionKind::RateLimited); assert_eq!(rejection.retry_after, RetryAfter::DelayMs(5));
    let bytes = peer.bodies.lock().unwrap()[1].clone();
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &bytes);
    assert_eq!(request_binding, ProviderRequestBinding { bytes: u64::try_from(bytes.len()).unwrap(),
        sha256: digest.as_ref().iter().map(|b| format!("{b:02x}")).collect() });
    let charges = peer.charges.lock().unwrap().clone(); assert_eq!(charges.len(), 3);
    let rejection_revision = rejected_reply.commit.as_ref().unwrap().controller_revision;
    assert!(charges[0] < charges[1] && charges[1] < rejection_revision && rejection_revision < charges[2]);
    third_release.notify_one();
    let terminal = until(&api, &task.id, |t| t.status == TaskStatus::Succeeded && t.attempt.as_ref().is_some_and(|a| a.stopped)).await;
    assert_eq!(terminal.input, task.input); assert_eq!(terminal.session_id, task.session_id);
    assert_eq!(terminal.attempt.as_ref().unwrap().id, attempt.id); assert!(!terminal.attempt.as_ref().unwrap().uncertain);
    assert_eq!(terminal.result.as_ref().unwrap().output, Some(json!(7)));
    assert_eq!(peer.count(), 3); assert_eq!(held.calls.load(Ordering::SeqCst), 1);
    let completion_id = contract::completion_id(&attempt.id);
    let completion = original(&api, &completion_id).await;
    let Observation::NativeLoopCompleted { model_requests, calls, requests, final_response_index } = report(&completion.1) else { panic!("original completion proof"); };
    assert_eq!(model_requests, 3); assert_eq!(final_response_index.unwrap_or(model_requests), 3); assert_eq!(calls.len(), 1);
    assert_eq!(requests, vec![contract::NativeRequestEvidence::ConfirmedRejection { request_index: 2, operation_id: rejection_id.clone() }]);
    assert_eq!(calls[0].request_index, 1); assert_eq!(calls[0].call_index, 1); assert_eq!(calls[0].logical_call_id, "call-0");
    assert_eq!(calls[0].operation_id, format!("native-collab-{}-1", attempt.id));
    worker.stop().await;
    let stable = get(&api, &task.id).await; assert_eq!(stable, terminal);
    let ack_path = notice_directory(&lab, &task).join("acknowledge.json");
    let ack_bytes = std::fs::read(&ack_path).unwrap();
    let ack: Value = serde_json::from_slice(&ack_bytes).unwrap();
    assert_eq!(ack["consumed"], true);
    let ack_request: Operation = serde_json::from_value(ack["request"].clone()).unwrap();
    let Operation::AcknowledgeCollaborationDelivery { operation_id: ack_id, caller, batch_id, context_binding } = &ack_request else { panic!("original delivery ACK request"); };
    assert_eq!(serde_json::to_value(caller).unwrap(), before_json["pending"]["delivery"]["caller"]);
    assert_eq!(json!(batch_id), before_json["pending"]["batch_id"]); assert_eq!(context_binding, &attempt.id);
    let ack_record = original(&api, ack_id).await.1;
    assert_eq!(ack_record.intended, ack["request"]); assert_eq!(ack_record.outcome, Some(ack["outcome"].clone()));
    controller.stop().await;
    let state = delivery_state(&lab, &task.session_id).unwrap();
    assert!(state["pending"].is_null()); assert_eq!(state["acknowledged_cursor"]["position"], json!(event.id));
    assert_eq!(ack_count(&lab, &task.id), 1); assert!(mirror(&lab, &task)["pending"].is_null());
    assert_eq!(lab.usage(&attempt.id), (3, Some(10), Some(4)));
    let mut controller = lab.controller("workflow-retry-restart").await;
    assert_eq!(get(&api, &task.id).await, stable);
    assert_eq!(original(&api, &rejection_id).await.1, rejected);
    assert_eq!(original(&api, &completion_id).await.1, completion.1);
    assert_eq!(original(&api, ack_id).await.1, ack_record);
    assert_eq!(peer.count(), 3); controller.stop().await;
    assert_eq!(lab.usage(&attempt.id), (3, Some(10), Some(4)));
    assert_eq!(std::fs::read(&ack_path).unwrap(), ack_bytes);
    assert_eq!(delivery_state(&lab, &task.session_id).unwrap(), state); assert_eq!(ack_count(&lab, &task.id), 1);
    assert_eq!(std::fs::read(&journal_path).unwrap(), tool_bytes); peer.close().await;
}
