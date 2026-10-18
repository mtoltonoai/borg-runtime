//! Exact ordering/failure cases using the existing real database, validator,
//! finalization gate, and controlled worker observations. No provider is used.
use super::*;
use rusqlite::Connection;

async fn exchange(c: &Controller, command: Operation) -> Reply {
    let result = c
        .handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: format!("order-request-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
            command: command.clone(),
        })
        .await;
    result.validate_for(&command).unwrap();
    result
}
async fn position(c: &Controller) -> u64 {
    exchange(
        c,
        Operation::ListTasks {
            after: None,
            limit: 1,
        },
    )
    .await
    .observed_controller_revision
    .unwrap()
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
fn candidate(a: &Attempt, id: &str) -> Operation {
    Operation::ObserveWorker {
        report: Report {
            report_id: id.into(),
            worker_id: a.worker_id.clone(),
            incarnation: a.worker_incarnation.clone(),
            task_id: a.task_id.clone(),
            attempt_id: a.id.clone(),
            generation: a.generation,
            observation: Observation::ResultCandidate {
                checks_passed: true,
                summary: "completed controlled native result".into(),
                checkpoint: None,
                validation: vec![borg_runtime::native_loop_contract::evidence(&a.id)],
                output: Some(json!({"answer":42})),
            },
        },
    }
}
async fn completed_before_acceptance(c: &Controller, a: &Attempt, label: &str) {
    controlled_native_request(c, a, 1, Some(2), Some(1)).await;
    complete_native_loop(c, a).await;
    task(
        report(
            c,
            a,
            &format!("{label}-executor"),
            Observation::ExecutorOutcome {
                success: true,
                summary: "controlled result already produced".into(),
            },
        )
        .await,
    );
    task(
        report(
            c,
            a,
            &format!("{label}-stopped"),
            Observation::ProcessTreeStopped,
        )
        .await,
    );
}
fn same_receipt(a: &Reply, b: &Reply) {
    assert_eq!(a.commit, b.commit);
    assert_eq!(a.outcome, b.outcome);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_validation_capacity_reconciles_same_id_once_and_rechecks_cancellation() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let mut submitted = Vec::new();
    for index in 0..6 {
        submitted.push(
            submit(
                &c,
                &format!("capacity-submit-{index}"),
                native(&format!("capacity-agent-{index}")),
            )
            .await,
        );
    }
    let w = worker("capacity-worker", "inc", 6, 0);
    register(&c, &w).await;
    let attempts: Vec<Attempt> = commands(&c, &w, vec![])
        .await
        .into_iter()
        .filter_map(|command| match command {
            WorkerCommand::RunAttempt { attempt, .. } => Some(attempt),
            _ => None,
        })
        .collect();
    assert_eq!(attempts.len(), 6);
    for (index, a) in attempts.iter().enumerate() {
        completed_before_acceptance(&c, a, &format!("capacity-{index}")).await;
    }
    // Four real validation results hold the existing four permits. No fake
    // capacity token, extra runtime limit, or open SQL transaction is supplied.
    let mut held = Vec::new();
    for (index, a) in attempts.iter().take(4).enumerate() {
        let (ready, release) = c.qualification_pause_next_finalization();
        let request = candidate(a, &format!("holder-result-{index}"));
        let owner = c.clone();
        let running = tokio::spawn(async move { exchange(&owner, request).await });
        tokio::time::timeout(Duration::from_secs(5), ready)
            .await
            .unwrap()
            .unwrap();
        held.push((release, running));
    }
    let mut originals = Vec::new();
    for index in 4..6 {
        let request = candidate(&attempts[index], &format!("capacity-original-{index}"));
        let reply = exchange(&c, request.clone()).await;
        assert!(
            matches!(&reply.outcome,Outcome::Refused{code,message} if code=="capacity"&&message=="controller native validation capacity exhausted: controller.validation_parallelism")
        );
        assert!(reply.commit.is_some());
        same_receipt(&exchange(&c, request.clone()).await, &reply);
        let reconcile = reconcile_result(
            &format!("capacity-reconcile-{index}"),
            &format!("capacity-original-{index}"),
        );
        let before = position(&c).await;
        let pending = exchange(&c, reconcile).await;
        assert!(matches!(pending.outcome, Outcome::Pending { .. }));
        assert!(pending.commit.is_none());
        assert_eq!(position(&c).await, before);
        assert_eq!(
            code(
                lookup(&c, &format!("capacity-reconcile-{index}"))
                    .await
                    .outcome
            ),
            "not_found"
        );
        originals.push((request, reply));
    }
    let cancelled = get(&c, &attempts[5].task_id).await;
    task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "cancel-capacity-waiter".into(),
                task_id: cancelled.id,
                expected_revision: cancelled.revision,
            },
        )
        .await,
    );
    let (release, running) = held.pop().unwrap();
    release.send(()).unwrap();
    assert_eq!(
        task(running.await.unwrap().outcome).status,
        TaskStatus::Succeeded
    );
    let request = reconcile_result("capacity-reconcile-4", "capacity-original-4");
    let accepted = exchange(&c, request.clone()).await;
    assert_eq!(task(accepted.outcome.clone()).status, TaskStatus::Succeeded);
    same_receipt(&exchange(&c, request.clone()).await, &accepted);
    let cancelled = exchange(
        &c,
        reconcile_result("capacity-reconcile-5", "capacity-original-5"),
    )
    .await;
    assert_eq!(code(cancelled.outcome.clone()), "invalid_transition");
    assert!(cancelled.commit.is_some());
    for (release, running) in held {
        release.send(()).unwrap();
        assert_eq!(
            task(running.await.unwrap().outcome).status,
            TaskStatus::Succeeded
        );
    }
    for (original, reply) in &originals {
        same_receipt(&exchange(&c, original.clone()).await, reply);
    }
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, config()).unwrap();
    same_receipt(&exchange(&c, request).await, &accepted);
    same_receipt(
        &exchange(
            &c,
            reconcile_result("capacity-reconcile-5", "capacity-original-5"),
        )
        .await,
        &cancelled,
    );
    for (original, reply) in &originals {
        same_receipt(&exchange(&c, original.clone()).await, reply);
    }
    assert_eq!(
        get(&c, &attempts[4].task_id).await.attempt.unwrap().id,
        attempts[4].id
    );
    assert!(get(&c, &attempts[5].task_id).await.result.is_none());
    drop(c);
    controller_owner_released(&p).await;
    let db = Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    assert_eq!(db.query_row("SELECT count(*) FROM history WHERE task_id=?1 AND json_extract(body,'$.kind')='result_candidate'",[&attempts[4].task_id],|r|r.get::<_,i64>(0)).unwrap(),1);
    assert_eq!(
        db.query_row(
            "SELECT model_requests FROM attempts WHERE id=?1",
            [&attempts[4].id],
            |r| r.get::<_, u32>(0)
        )
        .unwrap(),
        1
    );
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn reconciliation_commit_hook_failure_stays_pending_until_same_id_restart() {
    let (p, c, a, original, refused) = refused_result_fixture("commit-reconcile").await;
    let original_reply = exchange(&c, original.clone()).await;
    assert_eq!(original_reply.outcome, refused);
    let before = position(&c).await;
    drop(c);
    controller_owner_released(&p).await;
    let history = oracle_task_history(&p, &a.task_id);
    let commit_probe = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut cfg = config();
    cfg.qualification_fail_next_commit = Some(commit_probe.clone());
    let c = Controller::open_with_config(&p, cfg).unwrap();
    let request = reconcile_result("commit-reconcile-retry", "commit-reconcile-result");
    for _ in 0..2 {
        // Arm only after valid startup/setup. SQLite consumes this at COMMIT,
        // proving the failed transaction reached its real commit boundary.
        commit_probe.store(true, Ordering::SeqCst);
        let pending = exchange(&c, request.clone()).await;
        assert!(!commit_probe.load(Ordering::SeqCst));
        assert!(
            matches!(pending.outcome,Outcome::Pending{ref operation_id} if operation_id=="commit-reconcile-retry")
        );
        assert!(pending.commit.is_none());
        assert_eq!(position(&c).await, before);
        assert_eq!(
            code(lookup(&c, "commit-reconcile-retry").await.outcome),
            "not_found"
        );
        same_receipt(&exchange(&c, original.clone()).await, &original_reply);
        assert!(get(&c, &a.task_id).await.result.is_none());
    }
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(oracle_task_history(&p, &a.task_id), history);
    let db = Connection::open(&p).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM receipts WHERE id='commit-reconcile-retry'",
            [],
            |r| r.get::<_, i64>(0),
        )
        .unwrap(),
        0
    );
    assert!(!commit_probe.load(Ordering::SeqCst));
    drop(db);
    let c = Controller::open_with_config(&p, config()).unwrap();
    let accepted = exchange(&c, request.clone()).await;
    assert_eq!(task(accepted.outcome.clone()).status, TaskStatus::Succeeded);
    assert_eq!(
        accepted.commit.as_ref().unwrap().controller_revision,
        before + 1
    );
    same_receipt(&exchange(&c, request.clone()).await, &accepted);
    same_receipt(&exchange(&c, original.clone()).await, &original_reply);
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, config()).unwrap();
    same_receipt(&exchange(&c, request).await, &accepted);
    same_receipt(&exchange(&c, original).await, &original_reply);
}

#[tokio::test]
async fn child_settles_before_parent_stop_without_overlapping_successor_across_restart() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    // This native parent has no repository; its later explicit coding child
    // still needs the fixed operator registration before direct delegation.
    register_repository_fixture(&c, &fixture_repository_registration()).await;
    let mut input = native("early-child-parent");
    input.limits.model_requests = 5;
    let parent = submit(&c, "early-parent", input).await;
    let pw = worker("parent-worker", "inc", 1, 0);
    register(&c, &pw).await;
    let pa = run(&commands(&c, &pw, vec![]).await);
    controlled_native_request(&c, &pa, 1, Some(2), Some(1)).await;
    let child = controlled_delegate(&c, &parent, &pa, "early-delegate", "early-child", 1).await;
    let cw = worker("child-worker", "inc", 0, 1);
    register(&c, &cw).await;
    let ca = run(&commands(&c, &cw, vec![]).await);
    assert_eq!(ca.task_id, child.id);
    let completed = finish_controlled_coding(&c, &ca, "early-child-complete").await;
    let waiting = get(&c, &parent.id).await;
    assert_eq!(waiting.status, TaskStatus::Active);
    assert!(waiting.waiting_child_id.is_none());
    assert!(!waiting.attempt.as_ref().unwrap().stopped);
    assert_eq!(
        waiting.completed_child.as_ref().unwrap().result,
        completed.result
    );
    let spare = worker("spare-worker", "inc", 1, 0);
    register(&c, &spare).await;
    assert!(commands(&c, &spare, vec![]).await.is_empty());
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(get(&c, &parent.id).await, waiting);
    assert!(commands(&c, &spare, vec![]).await.is_empty());
    let stopped = report(
        &c,
        &pa,
        "parent-final-stop",
        Observation::ProcessTreeStopped,
    )
    .await;
    let queued = task(stopped.clone());
    assert_eq!(queued.status, TaskStatus::Queued);
    assert_eq!(queued.completed_child, waiting.completed_child);
    assert_eq!(
        report(
            &c,
            &pa,
            "parent-final-stop",
            Observation::ProcessTreeStopped
        )
        .await,
        stopped
    );
    let offered = commands(&c, &spare, vec![]).await;
    let successor = run(&offered);
    assert_eq!(successor.task_id, parent.id);
    assert_ne!(successor.id, pa.id);
    assert_eq!(successor.generation, pa.generation + 1);
    let (command_id, task_input, continuation) = offered
        .iter()
        .find_map(|cmd| match cmd {
            WorkerCommand::RunAttempt {
                command_id,
                attempt,
                task,
                continuation,
                ..
            } if attempt.id == successor.id => Some((command_id, task, continuation)),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        task_input.limits.deadline_unix_ms,
        parent.input.limits.deadline_unix_ms
    );
    assert_eq!(task_input.limits.model_requests, 3);
    assert_eq!(continuation, &waiting.completed_child);
    assert!(
        !commands(&c, &spare, vec![command_id.clone()])
            .await
            .iter()
            .any(|cmd| matches!(cmd, WorkerCommand::RunAttempt { .. }))
    );
    assert_eq!(get(&c, &parent.id).await.attempt.unwrap().id, successor.id);
}

#[tokio::test]
async fn native_malformed_overage_and_exhausted_reservation_preserve_exact_debits_and_owner() {
    // Controller-only admission/receipt proof. No provider is started, so this
    // does not claim to observe physical provider non-execution.
    fn retained(
        p: &std::path::Path,
        a: &Attempt,
    ) -> (
        (i64, Option<i64>, Option<i64>),
        Vec<(i64, String, String, i64)>,
        Vec<String>,
    ) {
        // Every call is after Controller drop and owner-release confirmation.
        let db = Connection::open_with_flags(p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
        let usage = db.query_row(
            "SELECT model_requests,input_tokens,output_tokens FROM attempts WHERE id=?1",
            [&a.id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        ).unwrap();
        let credits = db.prepare(
            "SELECT request_index,reservation_id,diagnostic_id,consumed FROM native_request_diagnostic_credits WHERE attempt_id=?1 ORDER BY request_index",
        ).unwrap().query_map([&a.id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))
            .unwrap().collect::<rusqlite::Result<Vec<_>>>().unwrap();
        let commands = db.prepare("SELECT body FROM commands WHERE task_id=?1 ORDER BY seq")
            .unwrap().query_map([&a.task_id], |r| r.get(0))
            .unwrap().collect::<rusqlite::Result<Vec<_>>>().unwrap();
        (usage, credits, commands)
    }
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let mut input = native("over-budget-agent");
    input.limits.model_requests = 2;
    let submitted = submit(&c, "over-budget-submit", input).await;
    let w = worker("over-budget-worker", "inc", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let initial = get(&c, &submitted.id).await;
    assert_eq!(initial.status, TaskStatus::Active);
    assert_eq!(initial.attempt.as_ref().unwrap().id, a.id);
    let operation = |report_id: &str, observation| Operation::ObserveWorker {
        report: Report {
            report_id: report_id.into(),
            worker_id: a.worker_id.clone(),
            incarnation: a.worker_incarnation.clone(),
            task_id: a.task_id.clone(),
            attempt_id: a.id.clone(),
            generation: a.generation,
            observation,
        },
    };
    // Preserve the exact original0737 malformed Native packet, including its
    // unmarked 0->3 cumulative jump, original ID and reported token values.
    let malformed = operation("over-budget-observed", Observation::Usage {
        native_diagnostic_reserved: false,
        model_requests: 3,
        input_tokens: Some(30),
        output_tokens: Some(9),
    });
    let refused = exchange(&c, malformed.clone()).await;
    assert_eq!(refused.outcome, Outcome::Refused {
        code: "source_mismatch".into(),
        message: "new native charge requires exactly one marked original Usage reservation".into(),
    });
    assert!(refused.commit.is_some());
    same_receipt(&exchange(&c, malformed.clone()).await, &refused);
    assert_eq!(get(&c, &submitted.id).await, initial);
    drop(c);
    controller_owner_released(&p).await;
    let uncharged = retained(&p, &a);
    assert_eq!(uncharged.0, (0, None, None));
    assert!(uncharged.1.is_empty());
    assert_eq!(uncharged.2.len(), 1);
    assert!(matches!(serde_json::from_str::<WorkerCommand>(&uncharged.2[0]).unwrap(),
        WorkerCommand::RunAttempt {attempt,..} if attempt == a));

    let c = Controller::open_with_config(&p, config()).unwrap();
    same_receipt(&exchange(&c, malformed.clone()).await, &refused);
    assert_eq!(get(&c, &submitted.id).await, initial);
    controlled_native_request(&c, &a, 1, Some(10), Some(3)).await;
    controlled_native_request(&c, &a, 2, Some(20), Some(6)).await;
    let charged = get(&c, &submitted.id).await;
    assert_eq!(charged.status, TaskStatus::Active);
    assert_eq!(charged.attempt.as_ref().unwrap().id, a.id);
    let first_id = borg_runtime::native_loop_contract::reservation_id(&a.id, 1);
    let first = lookup(&c, &first_id).await;
    let first_operation = operation(&first_id, Observation::Usage {
        native_diagnostic_reserved: true,
        model_requests: 1,
        input_tokens: Some(10),
        output_tokens: Some(3),
    });
    // Exact earlier receipt replay may not roll cumulative usage back to one.
    let first_replay = exchange(&c, first_operation.clone()).await;
    let Outcome::Ok {data: Data::Operation(first_record)} = &first.outcome else {
        panic!("original reservation lookup: {first:?}");
    };
    assert_eq!(first_record.intended, serde_json::to_value(&first_operation).unwrap());
    assert_eq!(first_record.outcome, Some(serde_json::to_value(&first_replay.outcome).unwrap()));
    assert_eq!(first.commit, first_replay.commit);
    let third = operation(&borg_runtime::native_loop_contract::reservation_id(&a.id, 3),
        Observation::Usage {
            native_diagnostic_reserved: true,
            model_requests: 3,
            input_tokens: Some(30),
            output_tokens: Some(9),
        });
    let exhausted = exchange(&c, third.clone()).await;
    assert_eq!(exhausted.outcome, Outcome::Refused {
        code: "capacity".into(),
        message: "native diagnostic reservation exceeds original model-request allowance".into(),
    });
    assert!(exhausted.commit.is_some());
    same_receipt(&exchange(&c, third.clone()).await, &exhausted);
    same_receipt(&exchange(&c, malformed.clone()).await, &refused);
    assert_eq!(get(&c, &submitted.id).await, charged);
    drop(c);
    controller_owner_released(&p).await;
    let exhausted_state = retained(&p, &a);
    assert_eq!(exhausted_state.0, (2, Some(20), Some(6)));
    assert_eq!(exhausted_state.1, (1..=2).map(|index| (
        i64::from(index),
        borg_runtime::native_loop_contract::reservation_id(&a.id, index),
        borg_runtime::native_loop_contract::diagnostic_id(&a.id, index),
        1,
    )).collect::<Vec<_>>());
    assert_eq!(exhausted_state.2, uncharged.2);

    let c = Controller::open_with_config(&p, config()).unwrap();
    same_receipt(&exchange(&c, malformed).await, &refused);
    same_receipt(&exchange(&c, third).await, &exhausted);
    same_receipt(&exchange(&c, first_operation).await, &first_replay);
    assert_eq!(get(&c, &submitted.id).await, charged);
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(retained(&p, &a), exhausted_state);
}

// Defensive Controller-consumer regression with injected cumulative Usage.
// These are controlled non-native observations, not measured Codex inner calls.
// The current supplied-executor adapter does not emit this overage: inner request
// counts remain unknown and its platform outer-turn reservations precheck bounds.
#[tokio::test]
async fn controlled_non_native_observed_overage_commits_exact_total_and_stop_without_refund_or_retry_reset() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let mut input = coding("over-budget-agent");
    input.limits.model_requests = 2;
    let submitted = submit(&c, "over-budget-submit", input).await;
    let w = worker("over-budget-worker", "inc", 0, 1);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let usage = Observation::Usage {
        native_diagnostic_reserved: false,
        model_requests: 3,
        input_tokens: Some(30),
        output_tokens: Some(9),
    };
    let original = report(&c, &a, "over-budget-observed", usage.clone()).await;
    let failed = task(original.clone());
    assert_eq!(failed.status, TaskStatus::Failed);
    assert_eq!(
        failed.blocked_reason.as_deref(),
        Some("model_request_budget_exceeded")
    );
    assert_eq!(
        failed.attempt.as_ref().unwrap().phase,
        AttemptPhase::StopRequested
    );
    assert!(!failed.attempt.as_ref().unwrap().stopped);
    let stop = commands(&c, &w, vec![]).await;
    assert_eq!(stop.iter().filter(|command|matches!(command,WorkerCommand::StopAttempt{attempt_id,..} if attempt_id==&a.id)).count(),1);
    assert_eq!(
        report(&c, &a, "over-budget-observed", usage.clone()).await,
        original
    );
    assert_eq!(
        code(
            report(
                &c,
                &a,
                "over-budget-decrease",
                Observation::Usage {
                    native_diagnostic_reserved: false,
                    model_requests: 2,
                    input_tokens: Some(30),
                    output_tokens: Some(9)
                }
            )
            .await
        ),
        "invalid_input"
    );
    task(
        report(
            &c,
            &a,
            "over-budget-stopped",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    let stopped = get(&c, &submitted.id).await;
    assert!(stopped.attempt.as_ref().unwrap().stopped);
    assert_eq!(
        code(
            call(
                &c,
                Operation::RetryTask {
                    operation_id: "over-budget-retry".into(),
                    task_id: stopped.id.clone(),
                    expected_revision: stopped.revision
                }
            )
            .await
        ),
        "capacity"
    );
    drop(c);
    controller_owner_released(&p).await;
    let db = Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT model_requests,input_tokens,output_tokens FROM attempts WHERE id=?1",
            [&a.id],
            |r| Ok((
                r.get::<_, u32>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?
            ))
        )
        .unwrap(),
        (3, 30, 9)
    );
    drop(db);
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(
        report(&c, &a, "over-budget-observed", usage).await,
        original
    );
    let current = get(&c, &submitted.id).await;
    assert_eq!(current.attempt.as_ref().unwrap().id, a.id);
    assert_eq!(current.status, TaskStatus::Failed);
    assert_eq!(
        code(
            call(
                &c,
                Operation::RetryTask {
                    operation_id: "over-budget-retry-after-restart".into(),
                    task_id: current.id,
                    expected_revision: current.revision
                }
            )
            .await
        ),
        "capacity"
    );
    assert!(
        !commands(&c, &w, vec![])
            .await
            .iter()
            .any(|command| matches!(command, WorkerCommand::RunAttempt { .. }))
    );
}

#[tokio::test]
async fn settling_tracked_checkpoint_never_clears_independent_execution_uncertainty() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let t = submit(&c, "unknown-plus-effect", coding("independent-unknown")).await;
    let w = worker("independent-worker", "inc", 0, 1);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let operation_id = "independent-checkpoint";
    let cp = Checkpoint {
        registration: fixture_repository_binding(),
        repository: t
            .input
            .repository
            .as_ref()
            .unwrap()
            .checkpoint_remote
            .clone(),
        commit: "b".repeat(40),
        remote_ref: checkpoint_ref(&t.id, &a.id, operation_id),
        operation_id: operation_id.into(),
    };
    assert!(matches!(call(&c,Operation::PrepareOperation{operation_id:operation_id.into(),task_id:t.id.clone(),attempt_id:a.id.clone(),generation:a.generation,kind:"checkpoint".into(),intended:json!({"registration":cp.registration,"repository":cp.repository,"commit":cp.commit,"remote_ref":cp.remote_ref})}).await,Outcome::Ok{..}));
    task(
        report(
            &c,
            &a,
            "independent-unknown-report",
            Observation::ExecutionUncertain {
                message: "controlled independent executor/provider outcome remains unknown".into(),
            },
        )
        .await,
    );
    task(
        report(
            &c,
            &a,
            "independent-local-stop",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    let settle = Operation::SettleOperation {
        settlement_id: "independent-not-applied".into(),
        operation_id: operation_id.into(),
        task_id: t.id.clone(),
        attempt_id: a.id.clone(),
        generation: a.generation,
        outcome: json!({"acknowledged":true,"not_applied":true}),
        checkpoint: None,
    };
    let settled = exchange(&c, settle.clone()).await;
    assert!(
        matches!(&settled.outcome,Outcome::Ok{data:Data::Operation(record)} if record.state=="settled")
    );
    let current = get(&c, &t.id).await;
    assert!(current.attempt.as_ref().unwrap().uncertain);
    assert!(current.result.is_none());
    assert!(current.selected_checkpoint.is_none());
    assert_eq!(
        code(
            call(
                &c,
                Operation::RetryTask {
                    operation_id: "independent-retry-refused".into(),
                    task_id: t.id.clone(),
                    expected_revision: current.revision
                }
            )
            .await
        ),
        "reconciliation_required"
    );
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, config()).unwrap();
    same_receipt(&exchange(&c, settle).await, &settled);
    let current = get(&c, &t.id).await;
    assert!(current.attempt.as_ref().unwrap().uncertain);
    assert_eq!(
        code(
            call(
                &c,
                Operation::RetryTask {
                    operation_id: "independent-retry-after-restart".into(),
                    task_id: t.id,
                    expected_revision: current.revision
                }
            )
            .await
        ),
        "reconciliation_required"
    );
}
