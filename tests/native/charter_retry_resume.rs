//! Confirmed no-reply rejection may resume only through canonical explicit
//! RetryTask. The existing uncertain-outcome refusal case is selected separately.
use super::idle::{attempts, settled_run};
use super::*;

struct ResumePeer {
    endpoint: String,
    bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    charges: Arc<Mutex<Vec<OperationRecord>>>,
    owner: Option<tokio::task::JoinHandle<()>>,
}
impl ResumePeer {
    async fn start(
        api: ApiClient,
        task: Task,
        policy: Arc<RuntimeLimits>,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let saved = bodies.clone();
        let charges = Arc::new(Mutex::new(Vec::new()));
        let debits = charges.clone();
        let owner = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let bytes = tokio::time::timeout(Duration::from_secs(10), async {
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
                    let mut bytes = vec![0; length];
                    stream.read_exact(&mut bytes).await.unwrap();
                    bytes
                })
                .await
                .expect("bounded local request body");
                let request: Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(request["model"], "gpt-6-astra");
                assert_eq!(request["reasoning"], json!({"effort":"max"}));
                assert!(request.get("tools").is_some());
                assert!(request["input"].to_string().contains("r1"));
                let index = {
                    let mut all = saved.lock().unwrap();
                    let index = all.len();
                    assert!(index < 2, "no third physical POST is permitted");
                    all.push(bytes);
                    index
                };
                let current = get(&api, &task.id).await;
                assert_eq!(current.input, task.input);
                assert_eq!(current.session_id, task.session_id);
                let attempt = current.attempt.as_ref().unwrap();
                assert!(!attempt.stopped && !attempt.uncertain);
                let id = contract::reservation_id(&attempt.id, 1);
                let charge = receipt(&api, &id).await.unwrap();
                let Operation::ObserveWorker { report } =
                    serde_json::from_value(charge.intended.clone()).unwrap()
                else {
                    panic!("original reservation body")
                };
                assert_eq!(report.report_id, id);
                assert_eq!(report.task_id, task.id);
                assert_eq!(report.attempt_id, attempt.id);
                assert_eq!(report.generation, attempt.generation);
                assert_eq!(report.worker_id, attempt.worker_id);
                assert_eq!(report.incarnation, attempt.worker_incarnation);
                assert!(matches!(
                    report.observation,
                    Observation::Usage {
                        model_requests: 1,
                        native_diagnostic_reserved: true,
                        ..
                    }
                ));
                assert!(matches!(
                    serde_json::from_value::<Outcome>(charge.outcome.clone().unwrap()).unwrap(),
                    Outcome::Ok { .. }
                ));
                debits.lock().unwrap().push(charge);
                let (status, extra, body) = if index == 0 {
                    (
                        429,
                        "retry-after-ms: 10000\r\n",
                        json!({"error":{"code":"rate_limit_exceeded","type":"rate_limit_error"}}),
                    )
                } else {
                    entered.notify_one();
                    release.notified().await;
                    (200, "", final_reply("r1", json!(7)))
                };
                let body = serde_json::to_vec(&body).unwrap();
                assert!(body.len() <= policy.transport.body_bytes);
                stream.write_all(format!("HTTP/1.1 {status} fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{extra}Connection: close\r\n\r\n", body.len()).as_bytes()).await.unwrap();
                stream.write_all(&body).await.unwrap();
                stream.shutdown().await.unwrap();
            }
        });
        Self {
            endpoint,
            bodies,
            charges,
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
            assert!(error.is_cancelled(), "local peer failed: {error}");
        }
    }
}
impl Drop for ResumePeer {
    fn drop(&mut self) {
        if let Some(owner) = &self.owner {
            owner.abort();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn confirmed_rejection_then_explicit_retry_preserves_old_turn_and_starts_new_occurrence() {
    Box::pin(resume()).await;
}
async fn resume() {
    let mut lab = Lab::new("charter-confirmed-rejection-resume");
    configure(&mut lab);
    let mut controller = lab.controller("retry-controller").await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let task = submit(&api, input(&lab, 3)).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let mut peer = ResumePeer::start(
        api.clone(),
        task.clone(),
        lab.limits.clone(),
        entered.clone(),
        release.clone(),
    )
    .await;
    let proxy = Proxy::start(lab.url.clone(), None, false, lab.limits.clone()).await;
    let mut worker = lab.worker(&proxy.url, &peer.endpoint, "retry-native-worker");
    let active = until(&api, &task.id, |t| t.attempt.is_some()).await;
    let first = active.attempt.as_ref().unwrap().clone();
    let rejection_id = contract::rejection_id(&first.id, 1);
    let rejection = wait_receipt(&api, &rejection_id).await;
    let original_operation: Operation = serde_json::from_value(rejection.intended.clone()).unwrap();
    let Operation::ObserveWorker { report } = &original_operation else {
        panic!("original rejection")
    };
    assert_eq!(report.attempt_id, first.id);
    assert_eq!(report.generation, first.generation);
    let Observation::NativeRequestRejected {
        request_index: 1,
        request_binding,
        rejection: rejected,
        ..
    } = &report.observation
    else {
        panic!("typed original rejection")
    };
    assert!(rejected.kind.same_input_retryable());
    let bytes = peer.bodies.lock().unwrap()[0].clone();
    assert_eq!(request_binding.bytes, bytes.len() as u64);
    assert_eq!(
        request_binding.sha256,
        aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &bytes)
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    assert!(matches!(
        serde_json::from_value::<Outcome>(rejection.outcome.clone().unwrap()).unwrap(),
        Outcome::Ok { .. }
    ));
    let current = get(&api, &task.id).await;
    assert!(matches!(
        api.call(Operation::CancelTask {
            operation_id: "retry-cancel-after-confirmed-rejection".into(),
            task_id: task.id.clone(),
            expected_revision: current.revision,
        })
        .await
        .unwrap()
        .outcome,
        Outcome::Ok { .. }
    ));
    let stopped = until(&api, &task.id, |t| {
        t.status == TaskStatus::Cancelled
            && t.attempt
                .as_ref()
                .is_some_and(|a| a.stopped && !a.uncertain)
    })
    .await;
    settled_run(&lab, stopped.attempt.as_ref().unwrap(), &mut worker).await;
    let stopped = get(&api, &task.id).await;
    let original_turns = turns(&api, &task.session_id).await;
    assert_eq!(original_turns.len(), 1);
    let original_turn = original_turns[0].clone();
    assert_eq!(original_turn.boundary.caller.attempt_id, first.id);
    assert_eq!(original_turn.boundary.request_index, 1);
    assert!(original_turn.reply.is_none() && original_turn.completed_by.is_none());
    assert_eq!(peer.count(), 1);
    assert_eq!(
        live_usage(&api, stopped.attempt.as_ref().unwrap()).await.0,
        1
    );
    assert!(
        receipt(&api, &contract::reservation_id(&first.id, 2))
            .await
            .is_none()
    );
    let retry = api
        .call(Operation::RetryTask {
            operation_id: "retry-explicit-confirmed-request".into(),
            task_id: task.id.clone(),
            expected_revision: stopped.revision,
        })
        .await
        .unwrap();
    assert!(matches!(
        retry.outcome,
        Outcome::Ok {
            data: Data::Task(_)
        }
    ));
    assert!(retry.commit.is_some());
    tokio::time::timeout(Duration::from_secs(30), entered.notified())
        .await
        .expect("canonical retry did not reach its first real POST");
    let resumed = get(&api, &task.id).await;
    let fresh = resumed.attempt.as_ref().unwrap().clone();
    assert_eq!(resumed.status, TaskStatus::Active);
    assert_eq!(resumed.id, task.id);
    assert_eq!(resumed.session_id, task.session_id);
    assert_eq!(resumed.assigned_agent_id, task.assigned_agent_id);
    assert_eq!(resumed.input, task.input);
    assert_ne!(fresh.id, first.id);
    assert_eq!(fresh.generation, first.generation + 1);
    assert_eq!(fresh.worker_id, first.worker_id);
    assert_eq!(fresh.worker_incarnation, first.worker_incarnation);
    assert_eq!(fresh.deadline_unix_ms, first.deadline_unix_ms);
    assert!(!fresh.stopped && !fresh.uncertain);
    let now = turns(&api, &task.session_id).await;
    assert_eq!(now.len(), 2);
    assert_eq!(
        now.iter()
            .find(|t| t.boundary.caller.attempt_id == first.id),
        Some(&original_turn)
    );
    let next = now
        .iter()
        .find(|t| t.boundary.caller.attempt_id == fresh.id)
        .unwrap();
    assert_eq!(next.boundary.request_index, 1);
    assert_eq!(next.boundary.effective.reference, reference("r1"));
    assert!(next.reply.is_none() && next.completed_by.is_none());
    assert!(
        receipt(&api, &format!("{}-charter-prior-closed", fresh.id))
            .await
            .is_none(),
        "no fabricated old-turn closure"
    );
    assert_eq!(receipt(&api, &rejection_id).await, Some(rejection.clone()));
    // Exact original replay remains audit of that occurrence, even after its
    // caller has been superseded. It must not alter the fresh task or turn.
    let replay = api.call(original_operation).await.unwrap();
    assert_eq!(replay.commit.as_ref().unwrap().operation_id, rejection_id);
    assert_eq!(
        replay.outcome,
        serde_json::from_value::<Outcome>(rejection.outcome.clone().unwrap()).unwrap()
    );
    assert_eq!(get(&api, &task.id).await, resumed);
    assert_eq!(turns(&api, &task.session_id).await, now);
    assert_eq!(peer.count(), 2);
    release.notify_one();
    let done = until(&api, &task.id, |t| {
        t.status == TaskStatus::Succeeded && t.attempt.as_ref().is_some_and(|a| a.stopped)
    })
    .await;
    assert_eq!(done.result.as_ref().unwrap().output, Some(json!(7)));
    assert_eq!(done.input, task.input);
    assert_eq!(done.session_id, task.session_id);
    let charged = attempts(&api, &task.id).await;
    assert_eq!(charged.len(), 2);
    assert!(charged.iter().all(|a| a.reported_model_requests == 1));
    assert_eq!(peer.charges.lock().unwrap().len(), 2);
    assert_eq!(peer.count(), 2);
    let final_turns = turns(&api, &task.session_id).await;
    assert_eq!(
        final_turns
            .iter()
            .find(|t| t.boundary.caller.attempt_id == first.id),
        Some(&original_turn)
    );
    assert_eq!(receipt(&api, &rejection_id).await, Some(rejection.clone()));
    std::fs::write(lab.root.join("confirmed-retry-proof.json"), serde_json::to_vec_pretty(&json!({
        "accepted":task,"stopped":stopped,"original_turn":original_turn,"original_rejection":rejection,
        "retry_receipt":retry,"resumed":resumed,"replay":replay,"done":done,
        "attempts":charged,"charges":peer.charges.lock().unwrap().clone(),"turns":final_turns,
    })).unwrap()).unwrap();
    worker.stop().await;
    controller.stop().await;
    let mut controller = lab.controller("retry-controller-reopen").await;
    assert_eq!(get(&api, &task.id).await, done);
    assert_eq!(turns(&api, &task.session_id).await, final_turns);
    assert_eq!(receipt(&api, &rejection_id).await, Some(rejection));
    assert_eq!(peer.count(), 2);
    controller.stop().await;
    peer.close().await;
}
