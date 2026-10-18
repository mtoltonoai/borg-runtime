//! Root integration discriminator: independent Stop delivery is not admission.
//! Actual Controller requests only; no fabricated policy or receipt rows.
use super::*;
use super::governance::{governance_config, governance_reply};
use borg_runtime::governance::{GovernanceMutation, GovernanceResult};

#[tokio::test]
async fn governance_stop_delivery_and_ack_commit_while_ordinary_clock_is_unavailable() {
    let clock = Arc::new(AtomicU64::new(future() - 600_000));
    let mut cfg = governance_config();
    let source = clock.clone();
    cfg.clock = Arc::new(move || source.load(Ordering::SeqCst));
    let c = Controller::open_with_config(path(), cfg).unwrap();
    let active = submit(&c, "control-live-submit", native("control-live")).await;
    let w = worker("control-worker", "control-incarnation", 1, 0);
    register(&c, &w).await;
    let issued = commands(&c, &w, vec![]).await;
    let attempt = run(&issued);
    assert_eq!(attempt.task_id, active.id);
    task(report(&c, &attempt, "control-started", Observation::Started {
        thread_id: None, turn_id: None,
    }).await);
    let run_ids = issued.iter().filter_map(|command| match command {
        WorkerCommand::RunAttempt { command_id, .. } => Some(command_id.clone()),
        _ => None,
    }).collect();
    assert!(commands(&c, &w, run_ids).await.is_empty());
    let queued = submit(&c, "control-queued-submit", native("control-queued")).await;
    assert_eq!(queued.status, TaskStatus::Queued);
    assert!(queued.attempt.is_none());
    let current = get(&c, &active.id).await;

    // This exact ordinary-policy failure is intentionally independent of Stop.
    // The unchanged ordinary expiry path refuses clock zero; no error is caught.
    clock.store(0, Ordering::SeqCst);
    // GetTask recomputes admission reasons. Compare under the same clock rather
    // than treating a legitimate read projection as a persisted mutation.
    let queued_observed = get(&c, &queued.id).await;
    assert_eq!(serde_json::to_value(&queued_observed.input).unwrap(), serde_json::to_value(&queued.input).unwrap());
    assert!(queued_observed.attempt.is_none());
    let queued_before = serde_json::to_value(queued_observed).unwrap();
    let stop = Operation::MutateGovernance {
        operation_id: "control-stop".into(),
        mutation: GovernanceMutation::Stop {
            task_id: current.id.clone(), expected_revision: current.revision,
            reason: "stop independent of unavailable ordinary governance".into(),
        },
    };
    let stopped = governance_reply(&c, stop.clone()).await;
    assert!(stopped.commit.is_some());
    assert!(matches!(&stopped.outcome, Outcome::Ok { data: Data::Governance(value) }
        if matches!(value.as_ref(), GovernanceResult::StopRequested { task_id } if task_id == &current.id)));

    let delivery = Operation::AwaitCommands {
        operation_id: "control-deliver-stop".into(), worker_id: w.id.clone(),
        incarnation: w.incarnation.clone(), acknowledged: vec![],
    };
    let delivered = governance_reply(&c, delivery.clone()).await;
    assert!(delivered.commit.is_some(), "Stop delivery must commit: {delivered:?}");
    let stop_ids: Vec<String> = match &delivered.outcome {
        Outcome::Ok { data: Data::Commands(items) } => {
            assert_eq!(items.len(), 1);
            items.iter().map(|command| match command {
                WorkerCommand::StopAttempt { command_id, task_id, attempt_id, .. } => {
                    assert_eq!(task_id, &current.id);
                    assert_eq!(attempt_id, &attempt.id);
                    command_id.clone()
                }
                other => panic!("control exchange admitted ordinary work: {other:?}"),
            }).collect()
        }
        other => panic!("Stop delivery expected: {other:?}"),
    };
    let acknowledgment = Operation::AwaitCommands {
        operation_id: "control-ack-stop".into(), worker_id: w.id.clone(),
        incarnation: w.incarnation.clone(), acknowledged: stop_ids,
    };
    let acknowledged = governance_reply(&c, acknowledgment.clone()).await;
    assert!(acknowledged.commit.is_some(), "Stop ACK must commit: {acknowledged:?}");
    assert!(matches!(&acknowledged.outcome, Outcome::Ok { data: Data::Commands(items) } if items.is_empty()));
    for (request, original) in [(stop, stopped), (delivery, delivered), (acknowledgment, acknowledged)] {
        let replay = governance_reply(&c, request).await;
        assert_eq!(replay.commit, original.commit);
        assert_eq!(replay.outcome, original.outcome);
    }
    assert_eq!(serde_json::to_value(get(&c, &queued.id).await).unwrap(), queued_before,
        "control-only exchanges must not alter queued input, attempt, holds or retry state");
    let stop_requested = get(&c, &active.id).await;
    assert!(!stop_requested.attempt.as_ref().unwrap().stopped,
        "command delivery/ACK is not a ProcessTreeStopped observation");

    // No new control progress: ordinary policy checks must still run and refuse.
    let ordinary = governance_reply(&c, Operation::AwaitCommands {
        operation_id: "control-ordinary-still-blocked".into(), worker_id: w.id,
        incarnation: w.incarnation, acknowledged: vec![],
    }).await;
    assert!(matches!(&ordinary.outcome, Outcome::Refused { code, .. } if code == "governance_clock_unavailable"));
    assert_eq!(serde_json::to_value(get(&c, &queued.id).await).unwrap(), queued_before);
    assert_eq!(serde_json::to_value(get(&c, &active.id).await).unwrap(), serde_json::to_value(stop_requested).unwrap());
}
