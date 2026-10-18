//! Actual worker loss after a physical circuit-probe POST. The provider is a
//! controlled loopback peer; no report, permit, charge or stored row is seeded.
use super::*;
use borg_runtime::native_retry::{NativeRequestAdmission, NativeRequestOutcome,
    NativeRequestSettlement, NativeSettlementSource, NativeWaitReason};

fn health(record: &OperationRecord) -> NativeRequestSettlement {
    match serde_json::from_value::<Outcome>(record.outcome.clone().unwrap()).unwrap() {
        Outcome::Ok { data: Data::NativeRequestSettlement(value) } => value,
        other => panic!("missing actual native health settlement: {other:?}"),
    }
}

async fn archived_circuit_wait(lab: &Lab, task: &Task)
    -> (Operation, Reply, OperationRecord, u64, u64) {
    let attempt = task.attempt.as_ref().unwrap();
    let directory = lab.state_for(&attempt.worker_id).join("native-retry")
        .join(process::component(&attempt.id));
    let mut selected = None;
    // The real worker archives each original Wait before acquiring again. Read
    // its exact receipt after success, so the 50ms fixture interval need not be
    // caught by a scheduler-dependent test sleep or a fabricated clock.
    for (index, entry) in std::fs::read_dir(directory).unwrap().enumerate() {
        assert!(index < lab.limits.controller.max_receipts);
        let path = entry.unwrap().path();
        if !path.file_name().unwrap().to_string_lossy().starts_with("0000000001-wait-") {
            continue;
        }
        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.len() <= lab.limits.worker.mutation_record_bytes);
        let saved: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(saved["consumed"], true);
        let request: Operation = serde_json::from_value(saved["request"].clone()).unwrap();
        let Operation::AcquireNativeRequest { operation_id, caller, request_index, .. } = &request
            else { panic!("wait archive did not retain an acquisition") };
        assert_eq!(caller.task_id, task.id);
        assert_eq!(caller.attempt_id, attempt.id);
        assert_eq!(caller.worker_id, attempt.worker_id);
        assert_eq!(caller.incarnation, attempt.worker_incarnation);
        assert_eq!(caller.generation, attempt.generation);
        assert_eq!(*request_index, 1);
        let (reply, record) = receipt(&lab.api, operation_id).await;
        assert_eq!(record.intended, serde_json::to_value(&request).unwrap());
        assert_eq!(record.outcome.as_ref().unwrap(), &saved["outcome"]);
        assert_eq!(saved["outcome_context"]["commit"], serde_json::to_value(&reply.commit).unwrap());
        let Outcome::Ok { data: Data::NativeRequestAdmission(NativeRequestAdmission::Wait {
            reason: NativeWaitReason::CircuitOpen, retry_at_ms: Some(retry_at), generation,
        }) } = serde_json::from_value(record.outcome.clone().unwrap()).unwrap()
            else { panic!("independent request did not wait on the unknown probe cooldown") };
        assert!(selected.replace((request, reply, record, retry_at, generation)).is_none(),
            "the unchanged cooldown must not create repeated mutating Wait receipts");
    }
    selected.expect("independent worker request omitted the original cooldown Wait")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_probe_loss_restart_cools_down_and_admits_only_a_distinct_request() {
    let lab = Lab::new("native-unknown-circuit-probe");
    assert_eq!(lab.limits.native_retry.as_ref().unwrap().circuit.failure_threshold, 4);
    assert_eq!(lab.limits.native_retry.as_ref().unwrap().circuit.open_ms, 50);
    let mut controller = lab.controller("controller-one").await;
    let (send, receive) = tokio::sync::oneshot::channel();
    let mut peer = Peer::start_inner(&lab, None, Replies::CircuitProbeLoss, Some(send)).await;
    let mut first = lab.worker(&peer.endpoint, "worker-one");
    let mut originals = Vec::new();
    let mut opened_at = None;
    for index in 0..4 {
        let label = format!("unknown-open-{index}");
        let instruction = format!("Unknown circuit opening request {index}. Return JSON with result equal to ok.");
        let submitted = lab.submit_named(4, &label, &instruction).await;
        let unknown = until_task(&lab.api, &submitted.id, |task|
            task.attempt.as_ref().is_some_and(|attempt| attempt.stopped && attempt.uncertain)).await;
        assert_eq!(unknown.input, submitted.input);
        assert!(unknown.result.is_none());
        let attempt = unknown.attempt.as_ref().unwrap();
        let charged = charged_once(&lab.api, attempt, 1).await;
        let settled = receipt(&lab.api, &format!("{}-native-health-1", attempt.id)).await;
        let value = health(&settled.1);
        assert_eq!(value.outcome, NativeRequestOutcome::Unknown);
        assert_eq!(value.source, NativeSettlementSource::Reported);
        assert!(!value.permit.circuit.probe);
        assert!(!value.permit.auth_probe);
        assert_eq!(value.permit.circuit.caller.attempt_id, attempt.id);
        assert!(committed(&charged.0) < committed(&settled.0));
        assert!(lookup(&lab.api, &rejection_id(&attempt.id, 1)).await.is_none(),
            "EOF cannot manufacture a known rejection or401 cooldown");
        assert!(lookup(&lab.api, &contract::reservation_id(&attempt.id, 2)).await.is_none());
        assert!(lookup(&lab.api, &contract::completion_id(&attempt.id)).await.is_none());
        if index < 3 { assert_eq!(value.circuit_retry_at_ms, None); }
        else { opened_at = value.circuit_retry_at_ms; }
        originals.push((unknown, charged, settled));
    }
    let opened_at = opened_at.expect("four actual unknown POSTs must open this selected circuit");
    let submitted = lab.submit_named(4, "unknown-held-probe",
        "Unknown circuit held probe. Return JSON with result equal to ok.").await;
    let mut stream = tokio::time::timeout(WAIT, receive).await
        .expect("real circuit probe never reached the HTTP peer")
        .expect("peer closed before handing off the probe response");
    let probed = get(&lab.api, &submitted.id).await;
    let attempt = probed.attempt.as_ref().unwrap().clone();
    assert!(!attempt.stopped);
    let (original_acquire, grant_reply, grant_record, NativeRequestAdmission::Granted { permit }) =
        acquisition(&lab, &probed).await.unwrap() else { panic!("physical probe has no exact original permit") };
    assert!(permit.circuit.probe);
    assert!(!permit.auth_probe, "this is a circuit probe, not a401 credential probe");
    assert_eq!(permit.circuit.key.endpoint, peer.endpoint);
    assert_eq!(permit.request_index, 1);
    let charged = charged_once(&lab.api, &attempt, 1).await;
    assert!(committed(&grant_reply) < committed(&charged.0));
    {
        let seen = peer.seen.lock().unwrap();
        assert_eq!(seen.len(), 5);
        assert_eq!(seen[4].task_id, submitted.id);
        assert!(seen[4].received_unix_ms >= opened_at);
        assert_eq!(seen[4].charge_revision, committed(&charged.0));
        assert!(seen[4].replied.is_none());
    }
    assert!(lookup(&lab.api, &format!("{}-native-health-1", attempt.id)).await.is_none());
    first.hard_loss();
    // The original POST reached the peer, but no response is delivered. Local
    // kill, socket close and later health recovery do not prove it had no effect.
    let _ = stream.shutdown().await;
    drop(stream);
    controller.stop().await;
    let mut controller = lab.controller("controller-two").await;
    let grant_replay = lab.api.call(original_acquire.clone()).await.unwrap();
    assert_eq!(grant_replay.commit, grant_reply.commit);
    assert_eq!(grant_replay.outcome, serde_json::from_value::<Outcome>(grant_record.outcome.clone().unwrap()).unwrap());
    peer.quiet(5).await;
    let mut replacement = lab.worker(&peer.endpoint, "worker-two");
    lab.registered("worker-two", &mut replacement).await;
    let recovered = until_task(&lab.api, &submitted.id, |task|
        task.attempt.as_ref().is_some_and(|a| a.stopped && a.uncertain)).await;
    assert_eq!(recovered.input, submitted.input);
    assert_eq!(recovered.attempt.as_ref().unwrap().id, attempt.id);
    assert_eq!(recovered.attempt.as_ref().unwrap().generation, attempt.generation);
    assert_eq!(recovered.attempt.as_ref().unwrap().deadline_unix_ms, attempt.deadline_unix_ms);
    assert!(recovered.result.is_none());
    lab.completed_command(&attempt.id);
    assert_eq!(receipt(&lab.api, &contract::reservation_id(&attempt.id, 1)).await.1, charged.1);
    assert!(lookup(&lab.api, &rejection_id(&attempt.id, 1)).await.is_none());
    assert!(lookup(&lab.api, &contract::reservation_id(&attempt.id, 2)).await.is_none());
    peer.quiet(5).await;

    let independent_input = lab.submit_named(4, "unknown-independent-probe",
        "Independent circuit recovery request. Return JSON with result equal to ok.").await;
    let independent = until_task(&lab.api, &independent_input.id, |task| task.status == TaskStatus::Succeeded).await;
    assert_eq!(independent.input, independent_input.input);
    let next_attempt = independent.attempt.as_ref().unwrap();
    assert!(next_attempt.stopped && !next_attempt.uncertain);
    assert_eq!(independent.result.as_ref().unwrap().output, Some(json!({"result":"ok"})));
    let (wait_request, wait_reply, wait_record, retry_at, generation) = archived_circuit_wait(&lab, &independent).await;
    let (_, next_grant_reply, _, NativeRequestAdmission::Granted { permit: next_permit }) =
        acquisition(&lab, &independent).await.unwrap() else { panic!("independent POST has no exact grant") };
    assert!(next_permit.circuit.probe);
    assert!(!next_permit.auth_probe);
    assert_eq!(next_permit.circuit.generation, generation);
    assert!(generation > permit.circuit.generation);
    assert_ne!(next_permit.circuit.operation_id, permit.circuit.operation_id);
    assert_ne!(next_permit.circuit.caller.attempt_id, attempt.id);
    assert_ne!(next_permit.circuit.caller.task_id, submitted.id);
    let next_charge = charged_once(&lab.api, next_attempt, 1).await;
    assert!(committed(&wait_reply) < committed(&next_grant_reply));
    assert!(committed(&next_grant_reply) < committed(&next_charge.0));
    let wait_replay = lab.api.call(wait_request.clone()).await.unwrap();
    assert_eq!(wait_replay.commit, wait_reply.commit);
    assert_eq!(wait_replay.outcome, serde_json::from_value::<Outcome>(wait_record.outcome.clone().unwrap()).unwrap());
    {
        let seen = peer.seen.lock().unwrap();
        assert_eq!(seen.len(), 6);
        assert_eq!(seen[5].task_id, independent.id);
        assert!(seen[5].received_unix_ms >= retry_at, "independent POST preceded the original unknown-probe cooldown");
        assert_eq!(seen[5].charge_revision, committed(&next_charge.0));
        assert_eq!(seen.iter().filter(|value| value.task_id == submitted.id).count(), 1);
    }
    // Read back the already-derived immutable settlement through its public
    // original-permit interface. FencedOwner and the actual Wait operation ID
    // prove the worker's acquisition caused this; this call cannot seed it.
    let settle_original = Operation::SettleNativeRequest {
        operation_id: "readback-fenced-unknown-probe".into(), caller: permit.circuit.caller.clone(),
        acquire_operation_id: permit.circuit.operation_id.clone(), outcome: NativeRequestOutcome::Unknown,
    };
    let settled_reply = lab.api.call(settle_original).await.unwrap();
    let Outcome::Ok { data: Data::NativeRequestSettlement(settled) } = settled_reply.outcome
        else { panic!("original unknown settlement unavailable") };
    assert_eq!(settled.source, NativeSettlementSource::FencedOwner);
    assert_eq!(settled.outcome, NativeRequestOutcome::Unknown);
    assert_eq!(settled.permit, permit);
    assert_eq!(settled.circuit_retry_at_ms, Some(retry_at));
    let Operation::AcquireNativeRequest { operation_id: wait_id, .. } = wait_request else { unreachable!() };
    assert_eq!(settled.operation_id, wait_id);
    let unchanged = get(&lab.api, &submitted.id).await;
    assert_eq!(unchanged, recovered, "shared health recovery changed the original uncertain task");
    let retry = lab.api.call(Operation::RetryTask { operation_id: "retry-unknown-original-probe".into(),
        task_id: submitted.id.clone(), expected_revision: unchanged.revision }).await.unwrap();
    assert!(matches!(retry.outcome, Outcome::Refused { code, .. } if code == "reconciliation_required"));
    for (task, charge, settlement) in &originals {
        let a = task.attempt.as_ref().unwrap();
        let retained = get(&lab.api, &task.id).await;
        assert_eq!(retained.input, task.input);
        let retained_attempt = retained.attempt.as_ref().unwrap();
        assert_eq!(retained_attempt.id, a.id);
        assert_eq!(retained_attempt.generation, a.generation);
        assert_eq!(retained_attempt.deadline_unix_ms, a.deadline_unix_ms);
        assert!(retained_attempt.stopped && retained_attempt.uncertain);
        assert!(retained.result.is_none());
        assert_eq!(receipt(&lab.api, &contract::reservation_id(&a.id, 1)).await.1, charge.1);
        assert_eq!(receipt(&lab.api, &format!("{}-native-health-1", a.id)).await.1, settlement.1);
        assert_eq!(peer.seen.lock().unwrap().iter().filter(|value| value.task_id == task.id).count(), 1);
    }
    for a in [&attempt, next_attempt] {
        assert!(lookup(&lab.api, &contract::reservation_id(&a.id, 2)).await.is_none());
    }
    assert!(lookup(&lab.api, &contract::completion_id(&attempt.id)).await.is_none());
    peer.quiet(6).await;
    replacement.stop().await;
    controller.stop().await;
    let mut controller = lab.controller("controller-three").await;
    assert_eq!(get(&lab.api, &submitted.id).await, recovered);
    assert_eq!(get(&lab.api, &independent.id).await, independent);
    let replay = lab.api.call(original_acquire).await.unwrap();
    assert_eq!(replay.commit, grant_reply.commit);
    assert_eq!(replay.outcome, serde_json::from_value::<Outcome>(grant_record.outcome.unwrap()).unwrap());
    peer.quiet(6).await;
    controller.stop().await;
    peer.close().await;
}
