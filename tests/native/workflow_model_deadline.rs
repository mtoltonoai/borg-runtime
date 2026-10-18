//! Actual Controller/native worker deadline while the first model POST is held.
//! Synthetic loopback provider only; no response or tool result is fabricated.
use super::*;
use borg_runtime::{native::ProviderFailureClass, native_loop_contract as ids};

async fn original_record(api: &ApiClient, id: &str) -> (OperationRecord, MutationCommit) {
    let op = Operation::GetOperation {
        operation_id: id.into(),
    };
    let reply = api.call(op.clone()).await.unwrap();
    reply.validate_for(&op).unwrap();
    let Outcome::Ok {
        data: Data::Operation(record),
    } = reply.outcome
    else {
        panic!("original request receipt: {reply:?}");
    };
    assert_eq!(record.id, id);
    let commit = reply.commit.unwrap();
    assert_eq!(commit.operation_id, id);
    (record, commit)
}

async fn no_original_record(api: &ApiClient, id: &str) {
    let op = Operation::GetOperation {
        operation_id: id.into(),
    };
    let reply = api.call(op.clone()).await.unwrap();
    reply.validate_for(&op).unwrap();
    assert!(reply.commit.is_none());
    assert!(matches!(reply.outcome, Outcome::Refused { code, .. } if code == "not_found"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// Traceability scope: actual task/worker/model request timeout under its accepted
// deadline, retained Unknown diagnostic, and unchanged outcome after restart.
//= spec/impl/v2/implementation.md#the-native-executor-is-bounded
//= type=test
//# The native executor MUST terminate its attempt when the task's execution deadline is reached.
async fn held_model_request_expires_as_unknown_without_continuation_and_survives_restart() {
    let lab = Lab::new("native-held-model-deadline");
    let mut controller = lab.controller("controller-one").await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let released = Arc::new(AtomicBool::new(false));
    let peer = Peer::start_async({
        let entered = entered.clone();
        let release = release.clone();
        let released = released.clone();
        move |index, _| {
            let entered = entered.clone();
            let release = release.clone();
            let released = released.clone();
            async move {
                assert_eq!(
                    index, 0,
                    "unknown model POST must not be repeated or continued"
                );
                entered.notify_one();
                tokio::time::timeout(Duration::from_secs(90), release.notified())
                    .await
                    .expect("held model fixture was not released");
                released.store(true, Ordering::SeqCst);
                None // The fixture never supplies a completed model response.
            }
        }
    })
    .await;
    let proxy = Proxy::start(lab.url.clone(), None, false, lab.limits.clone()).await;
    let mut input = lab.input(3);
    input.limits.deadline_unix_ms = process::now_ms() + 15_000;
    let task = submit(&api, input).await;
    let accepted = task.input.clone();
    let mut worker = lab.worker(&proxy.url, &peer.endpoint, "deadline-first");
    tokio::time::timeout(Duration::from_secs(10), entered.notified())
        .await
        .expect("actual first native POST never reached the held peer");
    let received_at = process::now_ms();
    assert!(received_at < accepted.limits.deadline_unix_ms);
    let active = get(&api, &task.id).await;
    assert_eq!(active.status, TaskStatus::Active);
    assert_eq!(active.input, accepted);
    let attempt = active.attempt.as_ref().unwrap().clone();
    assert_eq!(attempt.phase, AttemptPhase::Running);
    assert_eq!(attempt.deadline_unix_ms, accepted.limits.deadline_unix_ms);
    assert_eq!(live_usage(&api, &attempt).await, (1, None, None));
    assert_eq!(peer.count(), 1);
    assert!(!released.load(Ordering::SeqCst));

    // No cancellation command or process signal is issued before this actual
    // deadline outcome. The worker daemon must remain alive after its task ends.
    let terminal = until(&api, &task.id, |t| {
        t.status == TaskStatus::Failed
            && t.attempt.as_ref().is_some_and(|a| a.stopped && a.uncertain)
    })
    .await;
    let stopped_at = process::now_ms();
    assert!(stopped_at >= accepted.limits.deadline_unix_ms);
    assert!(
        !released.load(Ordering::SeqCst),
        "peer closure cannot cause the timeout outcome"
    );
    assert!(worker.child.try_wait().unwrap().is_none());
    assert_eq!(terminal.input, accepted);
    assert!(terminal.result.is_none());
    assert_eq!(peer.count(), 1);
    assert_eq!(
        live_usage(&api, terminal.attempt.as_ref().unwrap()).await,
        (1, None, None)
    );
    worker.stop().await;
    // Join all original reporting before taking the exact restart baseline.
    let settled = get(&api, &task.id).await;
    assert_eq!(settled.status, TaskStatus::Failed);
    assert_eq!(settled.input, accepted);
    assert!(settled.result.is_none());
    assert!(
        settled
            .attempt
            .as_ref()
            .is_some_and(|a| a.id == attempt.id && a.stopped && a.uncertain)
    );
    let charge = original_record(&api, &ids::reservation_id(&attempt.id, 1)).await;
    let diagnostic = original_record(&api, &ids::diagnostic_id(&attempt.id, 1)).await;
    assert!(charge.1.controller_revision < diagnostic.1.controller_revision);
    let Operation::ObserveWorker {
        report: charge_report,
    } = serde_json::from_value(charge.0.intended.clone()).unwrap()
    else {
        panic!("charge report required")
    };
    assert_eq!(charge_report.attempt_id, attempt.id);
    assert!(matches!(
        charge_report.observation,
        Observation::Usage {
            native_diagnostic_reserved: true,
            model_requests: 1,
            input_tokens: None,
            output_tokens: None,
        }
    ));
    let Operation::ObserveWorker { report } =
        serde_json::from_value(diagnostic.0.intended.clone()).unwrap()
    else {
        panic!("diagnostic report required")
    };
    assert_eq!(report.task_id, task.id);
    assert_eq!(report.attempt_id, attempt.id);
    assert_eq!(report.generation, attempt.generation);
    assert_eq!(report.worker_id, attempt.worker_id);
    assert_eq!(report.incarnation, attempt.worker_incarnation);
    let Observation::NativeRequestDiagnostic {
        request_index,
        endpoint,
        model,
        request_binding,
        observed_outcome,
        failure,
    } = &report.observation
    else {
        panic!("native terminal diagnostic required");
    };
    assert_eq!(*request_index, 1);
    assert_eq!(endpoint, &peer.endpoint);
    assert_eq!(
        model,
        &accepted.definition.required_inference().unwrap().model
    );
    request_binding
        .as_ref()
        .expect("actual exposed request has its original binding")
        .validate(accepted.execution_limits().unwrap().native.request_bytes)
        .unwrap();
    assert_eq!(
        *observed_outcome,
        ids::NativeRequestObservedOutcome::Unknown
    );
    let failure = failure
        .as_ref()
        .expect("deadline retains its original failure class");
    // Control::stopped also expires at this deadline. The native biased select
    // can observe that supplied future or its own deadline timer first. Preserve
    // the exact observed class; neither classification proves provider success.
    assert!(matches!(
        failure.class,
        ProviderFailureClass::Cancelled | ProviderFailureClass::Deadline
    ));
    assert!(failure.http.is_none(), "the peer sent no response headers");
    assert_eq!(
        diagnostic.0.outcome,
        Some(
            serde_json::to_value(Outcome::Ok {
                data: Data::Acknowledged
            })
            .unwrap()
        )
    );
    no_original_record(&api, &ids::reservation_id(&attempt.id, 2)).await;
    no_original_record(&api, &ids::completion_id(&attempt.id)).await;
    let tool_dir = lab
        .state()
        .join("collaboration")
        .join(process::component(&attempt.id));
    assert!(
        !tool_dir.exists(),
        "a held model response cannot manufacture a tool occurrence"
    );
    std::fs::write(
        lab.root.join("observed-model-deadline.json"),
        serde_json::to_vec_pretty(&json!({
            "accepted_deadline_unix_ms":accepted.limits.deadline_unix_ms,
            "first_post_received_unix_ms":received_at,"stopped_observed_unix_ms":stopped_at,
            "charge":charge,"diagnostic":diagnostic,"task":settled,
            "request":peer.requests.lock().unwrap()[0].clone(),
            "response_released":released.load(Ordering::SeqCst),"operator_cancel_issued":false,
        }))
        .unwrap(),
    )
    .unwrap();
    controller.stop().await;

    let mut controller = lab.controller("controller-two").await;
    assert_eq!(get(&api, &task.id).await, settled);
    assert_eq!(
        original_record(&api, &ids::reservation_id(&attempt.id, 1)).await,
        charge
    );
    assert_eq!(
        original_record(&api, &ids::diagnostic_id(&attempt.id, 1)).await,
        diagnostic
    );
    let mut replacement = lab.worker(&proxy.url, &peer.endpoint, "deadline-second");
    let until_poll = tokio::time::Instant::now()
        + Duration::from_millis(
            lab.limits.controller.command_wait_ms + lab.limits.transport.client_timeout_ms,
        );
    while !proxy
        .await_replies
        .lock()
        .unwrap()
        .iter()
        .any(|(id, inc)| id == "loop-worker" && inc == "deadline-second")
    {
        assert!(replacement.child.try_wait().unwrap().is_none());
        assert!(
            tokio::time::Instant::now() < until_poll,
            "replacement command poll missing"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(get(&api, &task.id).await, settled);
    assert_eq!(
        original_record(&api, &ids::diagnostic_id(&attempt.id, 1)).await,
        diagnostic
    );
    assert_eq!(peer.count(), 1);
    assert!(!released.load(Ordering::SeqCst));
    assert!(!tool_dir.exists());
    no_original_record(&api, &ids::reservation_id(&attempt.id, 2)).await;
    let retry = api
        .call(Operation::RetryTask {
            operation_id: "retry-unknown-model-deadline".into(),
            task_id: task.id.clone(),
            expected_revision: settled.revision,
        })
        .await
        .unwrap();
    assert!(
        matches!(retry.outcome, Outcome::Refused { code, .. } if code == "reconciliation_required")
    );
    assert_eq!(get(&api, &task.id).await, settled);
    assert_eq!(
        live_usage(&api, settled.attempt.as_ref().unwrap()).await,
        (1, None, None)
    );
    replacement.stop().await;
    controller.stop().await;
    assert_eq!(lab.usage(&attempt.id), (1, None, None));
    assert_eq!(peer.count(), 1);
    release.notify_one();
}
