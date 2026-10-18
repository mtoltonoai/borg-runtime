//! R1: restart only the Controller while two actual executor turns remain held.
//! The existing controlled stdio peer is not a pinned Codex/provider service.
use super::*;
use borg_runtime::collaboration::{AttemptView, Query, QueryResult};

async fn history(lab: &Lab, id: &str) -> Vec<AttemptView> {
    match lab.client.call(Operation::QueryCollaboration {
        query: Query::TaskAttempts { task_id: id.into(), after: None, limit: 4 },
    }).await.unwrap().outcome {
        Outcome::Ok { data: Data::Collaboration(QueryResult::TaskAttempts(page)) } => {
            assert!(page.next_cursor.is_none()); page.items
        }
        other => panic!("actual attempt history expected: {other:?}"),
    }
}

// The initial start marks Running before the executor's actual thread/turn
// association report commits. The held Gate alone does not fence that report.
async fn associated(lab: &Lab, attempt: &Attempt, gate: &Gate) -> Reply {
    let operation_id = format!("{}-report-{:020}", attempt.id, 1);
    let expected = Operation::ObserveWorker { report: Report {
        report_id: operation_id.clone(), worker_id: attempt.worker_id.clone(),
        incarnation: attempt.worker_incarnation.clone(), task_id: attempt.task_id.clone(),
        attempt_id: attempt.id.clone(), generation: attempt.generation,
        observation: Observation::Started {
            thread_id: Some(gate.observed["thread"].as_str().unwrap().into()),
            turn_id: Some(gate.observed["turn"].as_str().unwrap().into()),
        },
    } };
    let end = Instant::now() + TIMEOUT;
    loop {
        let reply = lab.client.call(Operation::GetOperation { operation_id: operation_id.clone() }).await.unwrap();
        match &reply.outcome {
            Outcome::Ok { data: Data::Operation(record) } => {
                assert_eq!(record.kind, "application_mutation");
                assert_eq!(record.state, "settled");
                assert_eq!(record.intended, serde_json::to_value(&expected).unwrap());
                let outcome: Outcome = serde_json::from_value(record.outcome.clone().unwrap()).unwrap();
                assert!(matches!(outcome, Outcome::Ok { data: Data::Task(_) }));
                assert_eq!(reply.commit.as_ref().unwrap().operation_id, operation_id);
                return reply;
            }
            Outcome::Refused { code, .. } if code == "not_found" => {}
            other => panic!("original executor association lookup failed: {other:?}"),
        }
        assert!(Instant::now() < end, "actual executor association did not commit");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

fn live(gate: &Gate) -> (u32, u64) {
    let pid = u32::try_from(gate.observed["pid"].as_u64().unwrap()).unwrap();
    let (start, state) = observed_process(pid).expect("held executor must exist");
    assert!(!matches!(state, 'Z' | 'X'), "executor exited before controller cut");
    (pid, start)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_restart_preserves_two_live_executor_turns_and_original_attempts() {
    let mut lab = Lab::new("controller-restart-live-executors").await;
    let first = lab.submit("live-first", "live-agent-a").await;
    let second = lab.submit("live-second", "live-agent-b").await;
    let origin = lab.origin.clone();
    lab.start_worker("worker-a", "live-one", &origin);
    lab.start_worker("worker-b", "live-one", &origin);
    let one = lab.gate().await;
    let two = lab.gate().await;
    let live_before = [live(&one), live(&two)];
    assert_ne!(live_before[0].0, live_before[1].0);
    let mut before = Vec::new();
    let mut associations = Vec::new();
    for submitted in [&first, &second] {
        let running = lab.until(&submitted.id, "canonical started executor", |task| {
            task.status == TaskStatus::Active && task.attempt.as_ref().is_some_and(|attempt|
                attempt.phase == AttemptPhase::Running && !attempt.stopped && !attempt.uncertain)
        }).await;
        let attempt = running.attempt.as_ref().unwrap();
        let gate = [&one, &two].into_iter().find(|gate|
            gate.observed["cwd"].as_str().unwrap().contains(&format!("/{}/", attempt.worker_id)))
            .expect("each actual worker owns one held executor");
        associations.push(associated(&lab, attempt, gate).await);
        let current = lab.get(&running.id).await;
        assert_eq!(current.attempt, running.attempt);
        assert_eq!(current.status, TaskStatus::Active);
        let running = current;
        assert_eq!(gate.observed["before"], "before\n");
        assert_eq!(running.input, submitted.input);
        assert!(running.result.is_none() && running.selected_checkpoint.is_none());
        let attempts = history(&lab, &running.id).await;
        assert_eq!(attempts.len(), 1);
        before.push((running, attempts, gate.observed.clone()));
    }
    assert_ne!(before[0].0.attempt.as_ref().unwrap().worker_id,
        before[1].0.attempt.as_ref().unwrap().worker_id);
    save(lab.root.join("live-before-controller-cut.json"), &json!({
        "executors":[one.observed,two.observed], "pid_start_ticks":live_before,
        "tasks":before.iter().map(|(task, _, _)| task).collect::<Vec<_>>(),
    }));

    // Gate sockets and both worker processes remain owned and unreleased.
    lab.restart_controller().await;
    for (pid, start) in live_before {
        let (same_start, state) = observed_process(pid).expect("controller restart lost live executor");
        assert_eq!(same_start, start, "PID reuse cannot prove continuity");
        assert!(!matches!(state, 'Z' | 'X'));
    }
    for worker in &mut lab.workers {
        assert!(worker.child.try_wait().unwrap().is_none());
    }
    for (old, old_history, _) in &before {
        let current = lab.get(&old.id).await;
        assert_eq!(current, *old, "controller restart alone must not replace the live attempt");
        let current_history = history(&lab, &old.id).await;
        assert_eq!(current_history.len(), 1);
        assert_eq!(current_history, *old_history, "restart preserves all original attempt accounting");
    }
    for original in &associations {
        let replay = lab.client.call(Operation::GetOperation {
            operation_id: original.commit.as_ref().unwrap().operation_id.clone(),
        }).await.unwrap();
        assert_eq!(replay.outcome, original.outcome);
        assert_eq!(replay.commit, original.commit);
    }
    assert_eq!(lab.starts.len(), 2);
    one.release().await;
    two.release().await;
    let mut accepted = Vec::new();
    for (old, old_history, observed) in &before {
        let finished = lab.successful(&old.id).await;
        let original = old.attempt.as_ref().unwrap();
        let final_attempt = finished.attempt.as_ref().unwrap();
        assert_eq!(final_attempt.id, original.id);
        assert_eq!(final_attempt.generation, original.generation);
        assert_eq!(final_attempt.worker_id, original.worker_id);
        assert_eq!(final_attempt.worker_incarnation, original.worker_incarnation);
        assert_eq!(final_attempt.deadline_unix_ms, original.deadline_unix_ms);
        assert_eq!(finished.session_id, old.session_id);
        assert_eq!(finished.input, old.input);
        assert!(final_attempt.stopped && !final_attempt.uncertain);
        let final_history = history(&lab, &old.id).await;
        assert_eq!(final_history.len(), 1);
        assert_eq!(final_history[0].reported_model_requests, old_history[0].reported_model_requests);
        let checkpoint = finished.selected_checkpoint.as_ref().unwrap();
        let receipt = lab.client.call(Operation::GetOperation { operation_id: checkpoint.operation_id.clone() }).await.unwrap();
        assert!(matches!(&receipt.outcome, Outcome::Ok { data: Data::Operation(record) }
            if record.state == "settled" && record.task_id == old.id && record.attempt_id == original.id));
        let association = lab.client.call(Operation::GetOperation {
            operation_id: format!("{}-completed-association", original.id),
        }).await.unwrap();
        let Outcome::Ok { data: Data::Operation(record) } = association.outcome else {
            panic!("completed executor association receipt missing");
        };
        let Operation::ObserveWorker { report } = serde_json::from_value(record.intended).unwrap() else {
            panic!("association receipt is not the original worker report");
        };
        assert_eq!(report.attempt_id, original.id);
        assert_eq!(report.generation, original.generation);
        assert_eq!(report.worker_id, original.worker_id);
        assert_eq!(report.incarnation, original.worker_incarnation);
        assert_eq!(report.observation, Observation::Started {
            thread_id: Some(observed["thread"].as_str().unwrap().into()),
            turn_id: Some(observed["turn"].as_str().unwrap().into()),
        });
        accepted.push(finished);
    }
    assert_eq!(lab.starts.len(), 2);
    assert!(tokio::time::timeout(Duration::from_millis(100), lab.gate.accept()).await.is_err(),
        "no duplicate executor may hide beyond the two observed gates");
    for (pid, start) in live_before {
        assert!(observed_process(pid).is_none_or(|(current, state)| current != start || matches!(state, 'Z' | 'X')));
    }
    save(lab.root.join("live-controller-restart-proof.json"), &json!({
        "original":before.iter().map(|(task, _, _)| task).collect::<Vec<_>>(),
        "accepted":accepted, "executor_starts":lab.starts, "pid_start_ticks":live_before,
        "scope":"actual Controller restart while two controlled executor processes are demonstrably live; no pinned Codex/provider claim",
    }));
    lab.close();
}
