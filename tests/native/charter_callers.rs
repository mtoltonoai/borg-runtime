//! Actual local worker/controller callers. These cases add negative/restart and
//! final-compaction coverage; they do not assert Codex inner-reply observability.
use super::*;
use borg_runtime::{
    limits::{NativeCircuitLimits, NativeRetryLimits},
    native_loop_contract as contract,
};

fn configure(lab: &mut Lab) {
    let policy = Arc::make_mut(&mut lab.limits);
    policy.charter = Some(limits());
    policy.execution.native.context_bytes = 262_144;
    configure_native_schema(policy);
    policy.native_retry = Some(NativeRetryLimits {
        initial_backoff_ms: 5,
        backoff_multiplier: 2,
        max_backoff_ms: 20,
        jitter_ms: 1,
        circuit: NativeCircuitLimits {
            failure_threshold: 4,
            open_ms: 50,
            max_entries: 16,
        },
        auth_cooldown_ms: 150,
        auth_max_entries: 16,
        coordination_record_bytes: 32_768,
    });
    save_limits(lab);
}
fn save_limits(lab: &Lab) {
    lab.limits.validate().unwrap();
    std::fs::write(
        lab.root.join("limits.json"),
        serde_json::to_vec_pretty(&*lab.limits).unwrap(),
    )
    .unwrap();
}
fn input(lab: &Lab, requests: u32) -> TaskInput {
    let mut input = lab.input(requests);
    input.definition.revision = "r1".into();
    input.definition.charter = Some(source("r1"));
    input
}
async fn receipt(api: &ApiClient, id: &str) -> Option<OperationRecord> {
    match api
        .call(Operation::GetOperation {
            operation_id: id.into(),
        })
        .await
        .unwrap()
        .outcome
    {
        Outcome::Ok {
            data: Data::Operation(row),
        } => Some(row),
        Outcome::Refused { code, .. } if code == "not_found" => None,
        other => panic!("receipt {id}: {other:?}"),
    }
}
async fn wait_receipt(api: &ApiClient, id: &str) -> OperationRecord {
    let end = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(row) = receipt(api, id).await {
            return row;
        }
        assert!(tokio::time::Instant::now() < end, "receipt absent: {id}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
async fn turns(api: &ApiClient, session: &str) -> Vec<CharterTurn> {
    match api
        .call(Operation::QueryCharter {
            query: CharterQuery::Turns {
                session_id: session.into(),
                after: None,
                limit: 10,
            },
        })
        .await
        .unwrap()
        .outcome
    {
        Outcome::Ok {
            data: Data::CharterQuery(result),
        } => {
            let CharterQueryResult::Turns(page) = *result else { panic!("turns: {result:?}") };
            assert!(
                page.next_cursor.is_none(),
                "fixture exceeded one bounded page"
            );
            page.items
        }
        other => panic!("turns: {other:?}"),
    }
}
struct HttpReply {
    status: u16,
    extra: &'static str,
    body: Value,
}
impl HttpReply {
    fn ok(body: Value) -> Option<Self> {
        Some(Self {
            status: 200,
            extra: "",
            body,
        })
    }
    fn rejected(extra: &'static str) -> Option<Self> {
        Some(Self {
            status: 429,
            extra,
            body: json!({"error":{"code":"rate_limit_exceeded","type":"rate_limit_error"}}),
        })
    }
}
struct BoundedPeer {
    endpoint: String,
    bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    owner: Option<tokio::task::JoinHandle<()>>,
}
impl BoundedPeer {
    async fn start<F, Fut>(
        api: ApiClient,
        task_id: String,
        policy: Arc<RuntimeLimits>,
        maximum: usize,
        reply: F,
    ) -> Self
    where
        F: Fn(usize, Value) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Option<HttpReply>> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let captured = bodies.clone();
        let owner = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut head = Vec::new();
                while !head.ends_with(b"\r\n\r\n") {
                    assert!(head.len() < policy.transport.http_header_bytes);
                    head.push(stream.read_u8().await.unwrap());
                }
                let head = String::from_utf8(head).unwrap();
                assert!(head.starts_with("POST /v1/responses "));
                let length = head
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                assert!(length <= policy.execution.native.request_bytes);
                let mut body = vec![0; length];
                stream.read_exact(&mut body).await.unwrap();
                let request: Value = serde_json::from_slice(&body).unwrap();
                assert_eq!(request["model"], "gpt-6-astra");
                assert_eq!(request["reasoning"], json!({"effort":"max"}));
                let index = {
                    let mut all = captured.lock().unwrap();
                    let index = all.len();
                    assert!(index < maximum, "unexpected extra physical POST");
                    all.push(body);
                    index
                };
                let task = get(&api, &task_id).await;
                let attempt = task.attempt.as_ref().unwrap();
                let physical = u32::try_from(index + 1).unwrap();
                let row = receipt(&api, &contract::reservation_id(&attempt.id, physical))
                    .await
                    .unwrap();
                assert!(matches!(
                    serde_json::from_value::<Outcome>(row.outcome.unwrap()).unwrap(),
                    Outcome::Ok { .. }
                ));
                let Operation::ObserveWorker { report } =
                    serde_json::from_value(row.intended).unwrap()
                else {
                    panic!("debit body")
                };
                assert_eq!(report.attempt_id, attempt.id);
                assert_eq!(report.generation, attempt.generation);
                assert!(
                    matches!(report.observation, Observation::Usage { model_requests, .. } if model_requests == physical)
                );
                if let Some(response) = reply(index, request).await {
                    let bytes = serde_json::to_vec(&response.body).unwrap();
                    assert!(bytes.len() <= policy.transport.body_bytes);
                    stream.write_all(format!("HTTP/1.1 {} fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{}Connection: close\r\n\r\n",
                        response.status, bytes.len(), response.extra).as_bytes()).await.unwrap();
                    stream.write_all(&bytes).await.unwrap();
                }
                stream.shutdown().await.unwrap();
            }
        });
        Self {
            endpoint,
            bodies,
            owner: Some(owner),
        }
    }
    fn count(&self) -> usize {
        self.bodies.lock().unwrap().len()
    }
    async fn close(&mut self) {
        let owner = self.owner.take().unwrap();
        owner.abort();
        if let Err(error) = owner.await {
            assert!(error.is_cancelled(), "peer failed: {error}");
        }
    }
}
impl Drop for BoundedPeer {
    fn drop(&mut self) {
        if let Some(owner) = &self.owner {
            owner.abort();
        }
    }
}

async fn registered(lab: &Lab, api: &ApiClient, incarnation: &str, worker: &mut Process) {
    let end = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            worker.child.try_wait().unwrap().is_none(),
            "replacement exited"
        );
        if let Ok(bytes) = std::fs::read(lab.state().join("mutations/registration.json")) {
            let saved: Value = serde_json::from_slice(&bytes).unwrap();
            let operation: Operation = serde_json::from_value(saved["request"].clone()).unwrap();
            if let Operation::RegisterWorker {
                operation_id,
                worker: selected,
            } = operation
            {
                if selected.incarnation == incarnation {
                    if let Some(record) = receipt(api, &operation_id).await {
                        if matches!(serde_json::from_value::<Outcome>(record.outcome.unwrap()).unwrap(),
                            Outcome::Ok { data: Data::Worker(actual) } if actual == selected)
                        {
                            return;
                        }
                    }
                }
            }
        }
        assert!(
            tokio::time::Instant::now() < end,
            "durable replacement registration absent"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
fn completed_command(lab: &Lab, attempt_id: &str) {
    let mut found = false;
    for entry in std::fs::read_dir(lab.state().join("commands")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let saved: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let command: WorkerCommand = serde_json::from_value(saved["command"].clone()).unwrap();
        if matches!(command, WorkerCommand::RunAttempt { ref attempt, .. } if attempt.id == attempt_id)
        {
            found = true;
            assert_eq!(
                saved["completed"], true,
                "original run command remained resumable"
            );
            assert_eq!(saved["effects_started"], true);
        }
    }
    assert!(found, "original durable run command missing");
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    Cancel,
    Deadline,
    Unknown,
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_rejection_then_cancellation_has_no_successor_or_fabricated_charter_reply() {
    ordinary_fault(Fault::Cancel).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_rejection_delay_crossing_deadline_retains_original_and_blocks() {
    ordinary_fault(Fault::Deadline).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_ordinary_reply_survives_fresh_worker_without_resend() {
    ordinary_fault(Fault::Unknown).await;
}
async fn ordinary_fault(fault: Fault) {
    let mut lab = Lab::new("charter-ordinary-boundaries");
    configure(&mut lab);
    let mut controller = lab.controller("ordinary-controller").await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let mut input = input(&lab, 3);
    if fault == Fault::Deadline {
        input.limits.deadline_unix_ms = process::now_ms() + 30_000;
    }
    let task = submit(&api, input).await;
    let mut peer = BoundedPeer::start(
        api.clone(),
        task.id.clone(),
        lab.limits.clone(),
        1,
        move |index, request| async move {
            assert_eq!(index, 0);
            assert!(request.get("tools").is_some());
            assert!(request["input"].to_string().contains("r1"));
            match fault {
                Fault::Cancel => HttpReply::rejected("retry-after-ms: 10000\r\n"),
                Fault::Deadline => HttpReply::rejected("retry-after-ms: 60000\r\n"),
                Fault::Unknown => None,
            }
        },
    )
    .await;
    let proxy = Proxy::start(lab.url.clone(), None, false, lab.limits.clone()).await;
    let mut worker = lab.worker(&proxy.url, &peer.endpoint, "ordinary-first");
    if fault == Fault::Cancel {
        let issued = until(&api, &task.id, |t| t.attempt.is_some()).await;
        wait_receipt(
            &api,
            &contract::rejection_id(&issued.attempt.as_ref().unwrap().id, 1),
        )
        .await;
        let current = get(&api, &task.id).await;
        assert!(matches!(
            api.call(Operation::CancelTask {
                operation_id: "cancel-after-original-rejection".into(),
                task_id: task.id.clone(),
                expected_revision: current.revision
            })
            .await
            .unwrap()
            .outcome,
            Outcome::Ok { .. }
        ));
    }
    let stopped = until(&api, &task.id, |t| {
        t.attempt.as_ref().is_some_and(|a| a.stopped)
            && match fault {
                Fault::Cancel => t.status == TaskStatus::Cancelled,
                Fault::Deadline => t.status == TaskStatus::Blocked,
                Fault::Unknown => t.attempt.as_ref().unwrap().uncertain,
            }
    })
    .await;
    let attempt = stopped.attempt.as_ref().unwrap();
    assert_eq!(peer.count(), 1);
    assert_eq!(live_usage(&api,attempt).await.0, 1);
    assert!(stopped.result.is_none());
    assert!(
        receipt(&api, &contract::reservation_id(&attempt.id, 2))
            .await
            .is_none()
    );
    assert!(
        receipt(&api, &contract::completion_id(&attempt.id))
            .await
            .is_none()
    );
    let original = turns(&api, &task.session_id).await;
    assert_eq!(original.len(), 1);
    assert_eq!(original[0].boundary.request_index, 1);
    assert_eq!(original[0].boundary.effective.reference, reference("r1"));
    assert!(original[0].reply.is_none() && original[0].completed_by.is_none());
    if fault == Fault::Unknown {
        assert!(
            receipt(&api, &contract::rejection_id(&attempt.id, 1))
                .await
                .is_none()
        );
    } else {
        let row = receipt(&api, &contract::rejection_id(&attempt.id, 1))
            .await
            .unwrap();
        let Operation::ObserveWorker { report } = serde_json::from_value(row.intended).unwrap()
        else {
            panic!("rejection")
        };
        let Observation::NativeRequestRejected {
            request_binding,
            request_index,
            ..
        } = report.observation
        else {
            panic!("typed rejection")
        };
        assert_eq!(request_index, 1);
        let all = peer.bodies.lock().unwrap();
        let bytes = &all[0];
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes);
        assert_eq!(request_binding.bytes, bytes.len() as u64);
        assert_eq!(
            request_binding.sha256,
            digest
                .as_ref()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        );
    }
    if fault == Fault::Deadline {
        let row = receipt(&api, &format!("{}-native-retry-block-1", attempt.id))
            .await
            .unwrap();
        let Operation::ObserveWorker { report } = serde_json::from_value(row.intended).unwrap()
        else {
            panic!("block")
        };
        let Observation::NativeRetryBlocked { block } = report.observation else {
            panic!("typed block")
        };
        assert_eq!(
            block.reason,
            borg_runtime::native::RetryBlockReason::RetryDelayUnavailable
        );
        assert_eq!(
            block.source_operation_id,
            contract::rejection_id(&attempt.id, 1)
        );
    }
    worker.stop().await;
    controller.stop().await;
    let mut controller = lab.controller("ordinary-restart").await;
    assert_eq!(turns(&api, &task.session_id).await, original);
    if fault == Fault::Unknown {
        let registration_count = proxy.registrations.load(Ordering::SeqCst);
        let mut replacement = lab.worker(&proxy.url, &peer.endpoint, "ordinary-replacement");
        let end = tokio::time::Instant::now() + Duration::from_secs(10);
        while proxy.registrations.load(Ordering::SeqCst) <= registration_count {
            assert!(
                tokio::time::Instant::now() < end,
                "replacement did not register"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        registered(&lab, &api, "ordinary-replacement", &mut replacement).await;
        completed_command(&lab, &attempt.id);
        let current = get(&api, &task.id).await;
        assert_eq!(current.attempt.as_ref().unwrap().id, attempt.id);
        assert!(current.attempt.as_ref().unwrap().uncertain);
        assert!(
            matches!(api.call(Operation::RetryTask { operation_id: "unknown-no-authorized-resend".into(),
            task_id: task.id.clone(), expected_revision: current.revision }).await.unwrap().outcome,
            Outcome::Refused { code, .. } if code == "reconciliation_required")
        );
        assert_eq!(turns(&api, &task.session_id).await, original);
        assert!(
            receipt(&api, &contract::reservation_id(&attempt.id, 2))
                .await
                .is_none()
        );
        // The listener remains live through the restart barrier and observation
        // interval; a resend fails its exact maximum, rather than hitting a dead port.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(peer.count(), 1);
        replacement.stop().await;
    }
    assert_eq!(peer.count(), 1);
    controller.stop().await;
    peer.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn charter_does_not_reopen_unbound_or_retired_native_admission() {
    let mut lab = Lab::new("charter-retired-admission");
    configure(&mut lab);
    let mut controller = lab.controller("retired-controller").await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    for retired in [false, true] {
        let mut value = input(&lab, 2);
        if retired {
            value.native_workflow =
                Some(Box::new(NativeWorkflow::InspectCurrentTaskThenContinue {}));
        } else {
            value.definition.execution_profile = None;
        }
        let reply = api
            .call(Operation::SubmitTask {
                operation_id: format!("refuse-{retired}"),
                input: (value).into(),
            })
            .await
            .unwrap();
        match &reply.outcome {
            Outcome::Refused { code, message } if retired => {
                assert_eq!(code, "invalid_input");
                assert_eq!(
                    message,
                    "collaboration_profile_conflicts_with_legacy_inspection"
                );
            }
            Outcome::Refused { code, .. } => assert_eq!(code, "template_profile_unbound"),
            _ => panic!("legacy path unexpectedly admitted: {reply:?}"),
        }
    }
    controller.stop().await;
}

#[path = "charter_final_compaction.rs"]
mod final_compaction;

#[path = "charter_idle.rs"]
mod idle;
#[path = "charter_retry_resume.rs"]
mod retry_resume;
