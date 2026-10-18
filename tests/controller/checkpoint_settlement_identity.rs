//! Canonical Controller settlement checks. These tests do not claim a Git push
//! or provider acknowledgement; they exercise the recorded acknowledgement input.
use super::*;

async fn exchange(c: &Controller, command: Operation) -> Reply {
    let reply = Box::pin(c.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: format!(
            "checkpoint-request-{}",
            NEXT.fetch_add(1, Ordering::Relaxed)
        ),
        command: command.clone(),
    }))
    .await;
    reply.validate_for(&command).unwrap();
    reply
}

async fn lookup(c: &Controller, id: &str) -> Reply {
    exchange(
        c,
        Operation::GetOperation {
            operation_id: id.into(),
        },
    )
    .await
}

fn record(reply: &Reply) -> &OperationRecord {
    match &reply.outcome {
        Outcome::Ok {
            data: Data::Operation(record),
        } => record,
        other => panic!("expected canonical operation, got {other:?}"),
    }
}

fn settlement(t: &Task, a: &Attempt, id: &str, cp: &Checkpoint) -> Operation {
    Operation::SettleOperation {
        settlement_id: id.into(),
        operation_id: cp.operation_id.clone(),
        task_id: t.id.clone(),
        attempt_id: a.id.clone(),
        generation: a.generation,
        outcome: json!({"acknowledged": true}),
        checkpoint: Some(cp.clone()),
    }
}

async fn original_refusal(c: &Controller, op: &Operation, first: &Reply) {
    let Operation::SettleOperation { settlement_id, .. } = op else {
        unreachable!()
    };
    assert_eq!(code(first.outcome.clone()), "operation_conflict");
    assert_eq!(first.commit.as_ref().unwrap().operation_id, *settlement_id);
    let replay = exchange(c, op.clone()).await;
    assert_eq!(replay.outcome, first.outcome);
    assert_eq!(replay.commit, first.commit);
    let saved = lookup(c, settlement_id).await;
    assert_eq!(saved.commit, first.commit);
    let saved = record(&saved);
    assert_eq!(saved.id, *settlement_id);
    assert_eq!(saved.kind, "application_mutation");
    assert_eq!(saved.state, "settled");
    assert_eq!(saved.intended, serde_json::to_value(op).unwrap());
    assert_eq!(
        saved.outcome,
        Some(serde_json::to_value(&first.outcome).unwrap())
    );
}

async fn unchanged(c: &Controller, before: &Task, pending: &OperationRecord) {
    assert_eq!(get(c, &before.id).await, *before);
    assert!(before.selected_checkpoint.is_none());
    assert_eq!(record(&lookup(c, &pending.id).await), pending);
    assert_eq!(pending.state, "pending");
    assert!(pending.outcome.is_none());
}

async fn mismatched_settlements(label: &str, registration: bool) {
    let p = path();
    println!("checkpoint fixture {label}: {}", p.display());
    let c = Controller::open_with_config(&p, config()).unwrap();
    let submitted = submit(&c, "submit", coding("coding-agent")).await;
    let w = worker("worker", "incarnation", 0, 1);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let repo = submitted.input.repository.as_ref().unwrap();
    assert_ne!(repo.url, repo.checkpoint_remote);
    let cp = Checkpoint {
        registration: repo.registration.clone().unwrap(),
        repository: repo.checkpoint_remote.clone(),
        commit: "b".repeat(40),
        remote_ref: checkpoint_ref(&submitted.id, &a.id, "publication"),
        operation_id: "publication".into(),
    };
    let prepare = Operation::PrepareOperation {
        operation_id: cp.operation_id.clone(),
        task_id: submitted.id.clone(),
        attempt_id: a.id.clone(),
        generation: a.generation,
        kind: "checkpoint".into(),
        intended: json!({"registration":cp.registration,"repository":cp.repository,
                         "commit":cp.commit,"remote_ref":cp.remote_ref}),
    };
    let prepared = exchange(&c, prepare.clone()).await;
    let pending = record(&prepared).clone();
    assert_eq!(pending.id, cp.operation_id);
    assert_eq!(pending.task_id, submitted.id);
    assert_eq!(pending.attempt_id, a.id);
    assert_eq!(pending.generation, a.generation);
    assert_eq!(pending.kind, "checkpoint");
    let before = get(&c, &submitted.id).await;
    unchanged(&c, &before, &pending).await;

    let mut candidates = vec![cp.clone(), cp.clone()];
    if registration {
        candidates[0].registration.registration_revision = "revision-2".into();
        candidates[1].registration.repository_id = "different-repository".into();
    } else {
        candidates[0].repository = "https://example.invalid/other-checkpoints.git".into();
        candidates[1].repository = repo.url.clone();
    }
    let mut refusals = Vec::new();
    for (index, candidate) in candidates.iter().enumerate() {
        let id = format!("refused-{index}");
        let op = settlement(&before, &a, &id, candidate);
        let reply = exchange(&c, op.clone()).await;
        original_refusal(&c, &op, &reply).await;
        unchanged(&c, &before, &pending).await;
        // Even a now-correct body cannot overwrite this original refusal.
        assert_eq!(
            code(
                exchange(&c, settlement(&before, &a, &id, &cp))
                    .await
                    .outcome
            ),
            "operation_conflict"
        );
        original_refusal(&c, &op, &reply).await;
        unchanged(&c, &before, &pending).await;
        refusals.push((op, reply));
    }
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, config()).unwrap();
    unchanged(&c, &before, &pending).await;
    for (op, reply) in &refusals {
        original_refusal(&c, op, reply).await;
    }
    let prepared_replay = exchange(&c, prepare).await;
    assert_eq!(prepared_replay.outcome, prepared.outcome);
    assert_eq!(prepared_replay.commit, prepared.commit);
    unchanged(&c, &before, &pending).await;

    // The untouched original remains settleable; the negative branches did not
    // consume its settlement reservation, alter its intent, or select a tuple.
    let valid = settlement(&before, &a, "accepted", &cp);
    let committed = exchange(&c, valid.clone()).await;
    let settled = record(&committed).clone();
    let mut expected_settled = pending.clone();
    expected_settled.state = "settled".into();
    expected_settled.outcome = Some(json!({"acknowledged": true}));
    assert_eq!(settled, expected_settled);
    let selected = get(&c, &before.id).await;
    assert_eq!(selected.selected_checkpoint, Some(cp.clone()));
    assert_eq!(selected.input, before.input);
    for (op, reply) in &refusals {
        original_refusal(&c, op, reply).await;
    }
    for (index, candidate) in candidates.iter().enumerate() {
        let op = settlement(&before, &a, &format!("after-accepted-{index}"), candidate);
        let reply = exchange(&c, op.clone()).await;
        original_refusal(&c, &op, &reply).await;
        assert_eq!(get(&c, &before.id).await, selected);
        assert_eq!(record(&lookup(&c, &cp.operation_id).await), &settled);
    }
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(get(&c, &before.id).await, selected);
    assert_eq!(record(&lookup(&c, &cp.operation_id).await), &settled);
    let replay = exchange(&c, valid).await;
    assert_eq!(replay.outcome, committed.outcome);
    assert_eq!(replay.commit, committed.commit);
    assert_eq!(get(&c, &before.id).await, selected);
    for (op, reply) in &refusals {
        original_refusal(&c, op, reply).await;
    }
    assert_eq!(get(&c, &before.id).await, selected);
    drop(c);
    controller_owner_released(&p).await;
}

#[tokio::test]
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//# A checkpoint publication MUST identify its accepted repository registration revision.
async fn wrong_registration_cannot_settle_or_select_original_checkpoint() {
    mismatched_settlements("registration", true).await;
}

#[tokio::test]
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//# An accepted repository association MUST distinguish the source endpoint from the checkpoint destination.
//= spec/impl/v2/implementation.md#checkpoint-publication-is-reconciled
//# The publication record MUST identify the repository, exact commit and intended remote reference.
async fn wrong_destination_cannot_settle_or_select_original_checkpoint() {
    mismatched_settlements("destination", false).await;
}
