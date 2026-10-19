//! Controller metadata tests use real SQLite transactions and controlled worker
//! observations. They do not execute a provider, repository effect, or deployment.
use super::*;
use rusqlite::Connection;

async fn exchange(c: &Controller, correlation: &str, command: Operation) -> Reply {
    let reply = c
        .handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: correlation.into(),
            command: command.clone(),
        })
        .await;
    assert_eq!(reply.request_id, correlation);
    reply
        .validate_for(&command)
        .expect("producer reply must validate for the actual operation");
    assert!(serde_json::to_vec(&reply).unwrap().len() <= metadata().transport.body_bytes);
    reply
}
async fn position(c: &Controller) -> u64 {
    let reply = exchange(
        c,
        "position",
        Operation::ListTasks {
            after: None,
            limit: 1,
        },
    )
    .await;
    assert!(reply.commit.is_none());
    assert!(matches!(reply.outcome, Outcome::Ok { .. }));
    reply.observed_controller_revision.unwrap()
}
fn stamp(reply: &Reply, operation: &str, revision: u64) {
    assert_eq!(
        reply.commit,
        Some(MutationCommit {
            operation_id: operation.into(),
            controller_revision: revision,
        })
    );
}
async fn lookup(c: &Controller, id: &str) -> Reply {
    exchange(
        c,
        "lookup",
        Operation::GetOperation {
            operation_id: id.into(),
        },
    )
    .await
}
fn app_record(reply: &Reply, command: &Operation, outcome: &Outcome) {
    let Outcome::Ok {
        data: Data::Operation(record),
    } = &reply.outcome
    else {
        panic!("{reply:?}")
    };
    assert_eq!(record.kind, "application_mutation");
    assert_eq!(record.intended, serde_json::to_value(command).unwrap());
    assert_eq!(record.outcome, Some(serde_json::to_value(outcome).unwrap()));
}
fn sql_position(db: &Connection) -> i64 {
    db.query_row(
        "SELECT value FROM counters WHERE kind='controller_revision'",
        [],
        |r| r.get(0),
    )
    .unwrap()
}

#[tokio::test]
async fn original_commit_survives_replay_lookup_restart_and_recorded_refusal() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(position(&c).await, 0);
    register_repository_fixture(&c, &fixture_repository_registration()).await;
    assert_eq!(position(&c).await, 1);
    let submit = Operation::SubmitTask {
        operation_id: "metadata-submit".into(),
        input: (coding("metadata-agent")).into(),
    };
    let accepted = exchange(&c, "first", submit.clone()).await;
    stamp(&accepted, "metadata-submit", 2);
    assert_eq!(accepted.observed_controller_revision, Some(2));
    let t = task(accepted.outcome.clone());
    assert_eq!(t.revision, 1);
    let reg = Operation::RegisterWorker {
        operation_id: "metadata-register".into(),
        worker: worker("metadata-worker", "inc", 0, 1),
    };
    stamp(&exchange(&c, "register", reg).await, "metadata-register", 3);
    let read = exchange(
        &c,
        "read",
        Operation::GetTask {
            task_id: t.id.clone(),
        },
    )
    .await;
    assert!(read.commit.is_none());
    assert_eq!(read.observed_controller_revision, Some(3));
    assert_eq!(
        task(read.outcome).revision,
        1,
        "task and controller positions are distinct"
    );
    let replay = exchange(&c, &"\\".repeat(256), submit.clone()).await;
    assert_eq!(replay.commit, accepted.commit);
    assert_eq!(replay.outcome, accepted.outcome);
    assert_eq!(replay.observed_controller_revision, Some(3));
    let mut changed = submit.clone();
    if let Operation::SubmitTask { input, .. } = &mut changed {
        input.instruction.push('!');
    }
    let conflict = exchange(&c, "conflict", changed).await;
    assert_eq!(code(conflict.outcome), "operation_conflict");
    assert!(conflict.commit.is_none());
    assert_eq!(position(&c).await, 3);
    let cancel = Operation::CancelTask {
        operation_id: "metadata-stale".into(),
        task_id: t.id.clone(),
        expected_revision: 0,
    };
    let refusal = exchange(&c, "stale", cancel.clone()).await;
    assert_eq!(code(refusal.outcome.clone()), "stale_revision");
    stamp(&refusal, "metadata-stale", 4);
    assert_eq!(position(&c).await, 4);
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, config()).unwrap();
    let replay = exchange(&c, "after-restart", cancel.clone()).await;
    assert_eq!(replay.commit, refusal.commit);
    assert_eq!(replay.outcome, refusal.outcome);
    for (id, op, original) in [
        ("metadata-submit", &submit, &accepted),
        ("metadata-stale", &cancel, &refusal),
    ] {
        let found = lookup(&c, id).await;
        assert_eq!(found.commit, original.commit);
        assert_eq!(found.observed_controller_revision, Some(4));
        app_record(&found, op, &original.outcome);
    }
    // An older original stamp is valid after a newer observation; it is history.
    let replay = exchange(&c, "late-old-stamp", submit).await;
    stamp(&replay, "metadata-submit", 2);
    assert_eq!(replay.observed_controller_revision, Some(4));
}

#[tokio::test]
async fn mutable_effect_lookup_never_inherits_prepare_or_settlement_commit() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    submit(&c, "effect-submit", coding("effect-agent")).await;
    let w = worker("effect-worker", "inc", 0, 1);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let prepare = Operation::PrepareOperation {
        operation_id: "effect-intent".into(),
        task_id: a.task_id.clone(),
        attempt_id: a.id.clone(),
        generation: a.generation,
        kind: "merge".into(),
        intended: json!({"controlled":"effect"}),
    };
    let original = exchange(&c, "prepare", prepare.clone()).await;
    let start = original.commit.as_ref().unwrap().controller_revision;
    let pending = lookup(&c, "effect-intent").await;
    assert!(pending.commit.is_none());
    assert_eq!(pending.observed_controller_revision, Some(start));
    let settle = |id: &str, outcome| Operation::SettleOperation {
        settlement_id: id.into(),
        operation_id: "effect-intent".into(),
        task_id: a.task_id.clone(),
        attempt_id: a.id.clone(),
        generation: a.generation,
        outcome,
        checkpoint: None,
    };
    let uncertain_op = settle("effect-uncertain", json!({"acknowledged":false}));
    let uncertain = exchange(&c, "uncertain", uncertain_op.clone()).await;
    stamp(&uncertain, "effect-uncertain", start + 1);
    let final_op = settle("effect-final", json!({"acknowledged":true}));
    let final_reply = exchange(&c, "final", final_op.clone()).await;
    stamp(&final_reply, "effect-final", start + 2);
    let current = lookup(&c, "effect-intent").await;
    assert!(current.commit.is_none());
    assert_eq!(current.observed_controller_revision, Some(start + 2));
    let Outcome::Ok {
        data: Data::Operation(record),
    } = &current.outcome
    else {
        panic!("{current:?}")
    };
    assert_eq!(record.state, "settled");
    assert_eq!(record.outcome, Some(json!({"acknowledged":true})));
    for (id, op, original) in [
        ("effect-uncertain", &uncertain_op, &uncertain),
        ("effect-final", &final_op, &final_reply),
    ] {
        let found = lookup(&c, id).await;
        assert_eq!(found.commit, original.commit);
        app_record(&found, op, &original.outcome);
    }
    let replay = exchange(&c, "old-prepare", prepare).await;
    assert_eq!(replay.outcome, original.outcome);
    assert_eq!(replay.commit, original.commit);
    assert_eq!(replay.observed_controller_revision, Some(start + 2));
    // A new receipt for an equivalent registration still receives its own stamp.
    let same = exchange(
        &c,
        "same-registration",
        Operation::RegisterWorker {
            operation_id: "effect-register-again".into(),
            worker: w,
        },
    )
    .await;
    stamp(&same, "effect-register-again", start + 3);
}

#[tokio::test]
async fn uncommitted_capacity_and_idle_wait_have_no_stamp_or_counter_churn() {
    let p = path();
    let c = Controller::open_with_config(
        &p,
        with_controller_limits(ControllerConfig {
            ..config()
        }, |limits| { limits.max_receipts = 2; }),
    )
    .unwrap();
    register_repository_fixture(&c, &fixture_repository_registration()).await;
    let w = worker("idle-worker", "inc", 0, 1);
    register(&c, &w).await;
    let wait = Operation::AwaitCommands {
        operation_id: "idle-wait".into(),
        worker_id: w.id,
        incarnation: w.incarnation,
        acknowledged: vec![],
    };
    for _ in 0..2 {
        let reply = exchange(&c, "idle", wait.clone()).await;
        assert!(
            matches!(reply.outcome, Outcome::Pending { ref operation_id } if operation_id == "idle-wait")
        );
        assert!(reply.commit.is_none());
        assert_eq!(position(&c).await, 2);
    }
    let refused = exchange(
        &c,
        "full",
        Operation::SubmitTask {
            operation_id: "full-submit".into(),
            input: (coding("full-agent")).into(),
        },
    )
    .await;
    assert_eq!(code(refused.outcome), "capacity");
    assert!(refused.commit.is_none());
    for id in ["idle-wait", "full-submit"] {
        let absent = lookup(&c, id).await;
        assert_eq!(code(absent.outcome), "not_found");
        assert!(absent.commit.is_none());
        assert_eq!(absent.observed_controller_revision, Some(2));
    }
    drop(c);
    controller_owner_released(&p).await;
    let db = Connection::open(&p).unwrap();
    assert_eq!(sql_position(&db), 2);
    assert_eq!(
        db.query_row("SELECT count(*) FROM receipts", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn recorded_validation_refusal_has_original_stamp_but_temporary_reconcile_does_not() {
    let (p, c, _a, original, refused) = refused_result_fixture("metadata-validator").await;
    let before = position(&c).await;
    let recorded = exchange(&c, "original-validation-refusal", original.clone()).await;
    assert_eq!(recorded.outcome, refused);
    stamp(&recorded, "metadata-validator-result", before);
    let reconciliation =
        reconcile_result("metadata-validation-reconcile", "metadata-validator-result");
    let pending = exchange(&c, "temporary", reconciliation.clone()).await;
    assert!(matches!(pending.outcome, Outcome::Pending { .. }));
    assert!(pending.commit.is_none());
    assert_eq!(position(&c).await, before);
    assert_eq!(
        code(lookup(&c, "metadata-validation-reconcile").await.outcome),
        "not_found"
    );
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, config()).unwrap();
    let accepted = exchange(&c, "recovered-validator", reconciliation.clone()).await;
    assert_eq!(task(accepted.outcome.clone()).status, TaskStatus::Succeeded);
    stamp(&accepted, "metadata-validation-reconcile", before + 1);
    let old = exchange(&c, "old-refusal", original).await;
    assert_eq!(old.outcome, refused);
    assert_eq!(old.commit, recorded.commit);
    assert_eq!(old.observed_controller_revision, Some(before + 1));
    let again = exchange(&c, "reconcile-replay", reconciliation).await;
    assert_eq!(again.commit, accepted.commit);
    assert_eq!(again.outcome, accepted.outcome);
}

#[tokio::test]
async fn expiry_transaction_advances_once_without_fabricating_a_mutation_receipt() {
    let p = path();
    let clock = Arc::new(AtomicU64::new(1000));
    let source = clock.clone();
    let c = Controller::open_with_config(
        &p,
        ControllerConfig {
            clock: Arc::new(move || source.load(Ordering::SeqCst)),
            ..config()
        },
    )
    .unwrap();
    register_repository_fixture(&c, &fixture_repository_registration()).await;
    let mut first = coding("expiry-one");
    first.limits.deadline_unix_ms = 2000;
    let original = Operation::SubmitTask {
        operation_id: "expiry-one".into(),
        input: (first).into(),
    };
    let one = exchange(&c, "first", original.clone()).await;
    let mut second = coding("expiry-two");
    second.limits.deadline_unix_ms = 2500;
    let two = submit(&c, "expiry-two", second).await;
    assert_eq!(position(&c).await, 3);
    clock.store(3000, Ordering::SeqCst);
    for id in [task(one.outcome.clone()).id, two.id] {
        let t = get(&c, &id).await;
        assert_eq!(t.status, TaskStatus::Failed);
        assert_eq!(t.blocked_reason.as_deref(), Some("execution_deadline"));
    }
    assert_eq!(position(&c).await, 4);
    assert_eq!(position(&c).await, 4);
    let replay = exchange(&c, "expired-original", original).await;
    assert_eq!(replay.outcome, one.outcome);
    stamp(&replay, "expiry-one", 2);
    assert_eq!(replay.observed_controller_revision, Some(4));
    drop(c);
    controller_owner_released(&p).await;
    let db = Connection::open(&p).unwrap();
    assert_eq!(sql_position(&db), 4);
    assert_eq!(
        db.query_row("SELECT count(*) FROM receipts", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        3
    );
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn actual_commit_hook_failure_rolls_back_receipt_state_and_revision() {
    let p = path();
    let commit_probe = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut cfg = config();
    cfg.qualification_fail_next_commit = Some(commit_probe.clone());
    let c = Controller::open_with_config(&p, cfg).unwrap();
    register_repository_fixture(&c, &fixture_repository_registration()).await;
    let command = Operation::SubmitTask {
        operation_id: "commit-failed".into(),
        input: (coding("commit-agent")).into(),
    };
    // Refuse the real owned connection's COMMIT, after valid startup. The
    // consumed flag distinguishes an actual COMMIT from pre-transaction refusal.
    commit_probe.store(true, Ordering::SeqCst);
    assert_eq!(position(&c).await, 1);
    assert!(
        commit_probe.load(Ordering::SeqCst),
        "read/maintenance must not consume the write fault"
    );
    let reply = exchange(&c, "commit-failed", command.clone()).await;
    assert!(!commit_probe.load(Ordering::SeqCst));
    assert_eq!(code(reply.outcome), "store_unavailable");
    assert!(reply.commit.is_none());
    assert!(reply.observed_controller_revision.is_none());
    assert_eq!(position(&c).await, 1);
    assert_eq!(code(lookup(&c, "commit-failed").await.outcome), "not_found");
    drop(c);
    controller_owner_released(&p).await;
    let db = Connection::open(&p).unwrap();
    assert_eq!(sql_position(&db), 1);
    for table in ["receipts", "tasks", "history", "agents", "definitions"] {
        assert_eq!(
            db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            i64::from(table == "receipts"),
            "{table}"
        );
    }
    drop(db);
    // A normal restart accepts the original exact operation once; the failed
    // transaction did not consume its identity, revision, or task allocation.
    let c = Controller::open_with_config(&p, config()).unwrap();
    let accepted = exchange(&c, "commit-recovered", command.clone()).await;
    assert!(matches!(accepted.outcome, Outcome::Ok { .. }));
    stamp(&accepted, "commit-failed", 2);
    let replay = exchange(&c, "commit-replay", command).await;
    assert_eq!(replay.commit, accepted.commit);
    assert_eq!(replay.outcome, accepted.outcome);
    assert_eq!(position(&c).await, 2);
}

#[tokio::test]
async fn controller_revision_overflow_refuses_new_commit_but_preserves_original_replay() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let op = Operation::RegisterWorker {
        operation_id: "overflow-original".into(),
        worker: worker("original", "inc", 0, 1),
    };
    let original = exchange(&c, "original", op.clone()).await;
    drop(c);
    controller_owner_released(&p).await;
    let db = Connection::open(&p).unwrap();
    db.execute(
        "UPDATE counters SET value=?1 WHERE kind='controller_revision'",
        [i64::MAX],
    )
    .unwrap();
    drop(db);
    let c = Controller::open_with_config(&p, config()).unwrap();
    let old = exchange(&c, "old", op).await;
    assert_eq!(old.commit, original.commit);
    assert_eq!(old.outcome, original.outcome);
    assert_eq!(old.observed_controller_revision, Some(i64::MAX as u64));
    let refused = exchange(
        &c,
        "overflow",
        Operation::RegisterWorker {
            operation_id: "overflow-new".into(),
            worker: worker("new", "inc", 0, 1),
        },
    )
    .await;
    assert_eq!(code(refused.outcome), "store_unavailable");
    assert!(refused.commit.is_none());
    assert_eq!(position(&c).await, i64::MAX as u64);
    drop(c);
    controller_owner_released(&p).await;
    let db = Connection::open(&p).unwrap();
    assert_eq!(sql_position(&db), i64::MAX);
    assert_eq!(
        db.query_row("SELECT count(*) FROM workers", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        db.query_row("SELECT count(*) FROM receipts", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn corrupt_controller_revision_associations_refuse_reopen_without_repair() {
    for corrupt in [
        "DELETE FROM counters WHERE kind='controller_revision'",
        "UPDATE counters SET value=-1 WHERE kind='controller_revision'",
        "UPDATE counters SET value=0 WHERE kind='controller_revision'",
        "PRAGMA ignore_check_constraints=ON; UPDATE receipts SET controller_revision=0",
        "UPDATE receipts SET request=replace(request,'integrity-original','integrity-changed')",
    ] {
        let p = path();
        let c = Controller::open_with_config(&p, config()).unwrap();
        let original = exchange(
            &c,
            "integrity",
            Operation::RegisterWorker {
                operation_id: "integrity-original".into(),
                worker: worker("integrity-worker", "inc", 0, 1),
            },
        )
        .await;
        stamp(&original, "integrity-original", 1);
        drop(c);
        controller_owner_released(&p).await;
        let db = Connection::open(&p).unwrap();
        db.execute_batch(corrupt).unwrap();
        db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        drop(db);
        let before = std::fs::read(&p).unwrap();
        assert!(
            Controller::open_with_config(&p, config()).is_err(),
            "{corrupt}"
        );
        assert_eq!(std::fs::read(&p).unwrap(), before, "{corrupt}");
    }
}

#[tokio::test]
async fn over_bound_lookup_rolls_back_cancellation_receipt_counter_and_cleanup_debit() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let original = submit(&c, "boundary-submit", coding("boundary-agent")).await;
    let cancel = Operation::CancelTask {
        operation_id: "boundary-cancel".into(),
        task_id: original.id.clone(),
        expected_revision: original.revision,
    };
    // Deliberately oversized stored-field fixture. This is a negative wire-bound
    // test, not a claim that a provider may emit unbounded progress.
    let mut predicted = original.clone();
    predicted.status = TaskStatus::Cancelled;
    predicted.revision += 1;
    predicted.blocked_reason = Some("cancellation_requested".into());
    let wrapper = |task: Task| {
        let outcome = Outcome::Ok {
            data: Data::Task(Box::new(task)),
        };
        Outcome::Ok {
            data: Data::Operation(OperationRecord {
                id: "boundary-cancel".into(),
                task_id: original.id.clone(),
                attempt_id: String::new(),
                generation: 0,
                kind: "application_mutation".into(),
                intended: serde_json::to_value(&cancel).unwrap(),
                state: "settled".into(),
                outcome: Some(serde_json::to_value(&outcome).unwrap()),
            }),
        }
    };
    let target = metadata().transport.body_bytes - 1024 + 1;
    let empty_size = serde_json::to_vec(&wrapper(predicted.clone()))
        .unwrap()
        .len();
    let progress = "x".repeat(target - empty_size);
    predicted.progress = progress.clone();
    let predicted_outcome = Outcome::Ok {
        data: Data::Task(Box::new(predicted.clone())),
    };
    assert!(serde_json::to_vec(&predicted_outcome).unwrap().len() <= metadata().transport.body_bytes - 1024);
    let lookup_outcome = wrapper(predicted);
    assert_eq!(serde_json::to_vec(&lookup_outcome).unwrap().len(), target);
    let maximum_lookup = Reply {
        request_id: "\\".repeat(256),
        commit: Some(MutationCommit {
            operation_id: "boundary-cancel".into(),
            controller_revision: i64::MAX as u64,
        }),
        observed_controller_revision: Some(i64::MAX as u64),
        outcome: lookup_outcome,
    };
    assert!(
        serde_json::to_vec(&maximum_lookup).unwrap().len() <= metadata().transport.body_bytes,
        "discriminates the legacy Outcome ceiling from the full-wire ceiling"
    );
    drop(c);
    controller_owner_released(&p).await;
    let db = Connection::open(&p).unwrap();
    let original_body: String = db
        .query_row("SELECT body FROM tasks WHERE id=?1", [&original.id], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(original_body.matches("\"progress\":\"\"").count(), 1);
    let body = original_body.replacen(
        "\"progress\":\"\"",
        &format!("\"progress\":{}", serde_json::to_string(&progress).unwrap()),
        1,
    );
    db.execute(
        "UPDATE tasks SET body=?2 WHERE id=?1",
        rusqlite::params![original.id, body],
    )
    .unwrap();
    let cleanup: i64 = db
        .query_row(
            "SELECT cleanup FROM tasks WHERE id=?1",
            [&original.id],
            |r| r.get(0),
        )
        .unwrap();
    let history: i64 = db
        .query_row("SELECT count(*) FROM history", [], |r| r.get(0))
        .unwrap();
    drop(db);
    let c = Controller::open_with_config(&p, config()).unwrap();
    let refusal = exchange(&c, "short-initial-correlation", cancel).await;
    assert_eq!(code(refusal.outcome), "capacity");
    assert!(refusal.commit.is_none());
    assert_eq!(
        code(lookup(&c, "boundary-cancel").await.outcome),
        "not_found"
    );
    drop(c);
    controller_owner_released(&p).await;
    let db = Connection::open(&p).unwrap();
    assert_eq!(sql_position(&db), 2);
    assert_eq!(
        db.query_row("SELECT count(*) FROM receipts", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        2
    );
    assert_eq!(
        db.query_row("SELECT body FROM tasks WHERE id=?1", [&original.id], |r| {
            r.get::<_, String>(0)
        })
        .unwrap(),
        body
    );
    assert_eq!(
        db.query_row(
            "SELECT cleanup FROM tasks WHERE id=?1",
            [&original.id],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        cleanup
    );
    assert_eq!(
        db.query_row("SELECT count(*) FROM history", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        history
    );
}

#[tokio::test]
async fn large_ack_exchange_keeps_lookup_and_maximum_correlation_replay_bounded() {
    let p = path();
    let cfg = with_controller_limits(ControllerConfig {
        ..config()
    }, |limits| { limits.max_commands_per_reply = 128; });
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let w = worker(&"\\".repeat(256), &"\"".repeat(256), 0, 128);
    let registered = exchange(
        &c,
        "register-large-worker",
        Operation::RegisterWorker {
            operation_id: "large-worker-registration".into(),
            worker: w.clone(),
        },
    )
    .await;
    assert!(matches!(
        registered.outcome,
        Outcome::Ok {
            data: Data::Worker(_)
        }
    ));
    for n in 0..128 {
        submit(
            &c,
            &format!("small-submit-{n}"),
            coding(&format!("small-{n:03}")),
        )
        .await;
    }
    // The accepted metadata snapshot is part of every Run. Fetch bounded
    // delivery pages instead of assuming all 128 Runs fit one byte envelope.
    // Keep all 128 distinct original command IDs for the large ACK below.
    let mut old = commands(&c, &w, vec![]).await;
    while old.len() < 128 {
        assert!(!old.is_empty(), "the first bounded page must make progress");
        let acknowledged = old
            .iter()
            .map(|command| match command {
                WorkerCommand::RunAttempt { command_id, .. } => command_id.clone(),
                _ => panic!("expected original Run"),
            })
            .collect();
        let next = commands(&c, &w, acknowledged).await;
        assert!(!next.is_empty(), "each bounded page must make progress");
        old.extend(next);
        assert!(old.len() <= 128);
    }
    assert_eq!(old.len(), 128);
    let mut acknowledgments = Vec::new();
    for (n, command) in old.iter().enumerate() {
        let WorkerCommand::RunAttempt {
            attempt,
            command_id,
            ..
        } = command
        else {
            panic!("expected original Run")
        };
        acknowledgments.push(command_id.clone());
        task(
            report(
                &c,
                attempt,
                &format!("large-stopped-{n}"),
                Observation::ProcessTreeStopped,
            )
            .await,
        );
        let failed = task(
            report(
                &c,
                attempt,
                &format!("large-failed-{n}"),
                Observation::ResultCandidate {
                    checks_passed: false,
                    summary: "controlled received-only cleanup".into(),
                    checkpoint: None,
                    validation: vec![],
                    output: None,
                },
            )
            .await,
        );
        assert_eq!(failed.status, TaskStatus::Failed);
    }
    assert_eq!(
        acknowledgments.iter().collect::<std::collections::BTreeSet<_>>().len(),
        128,
        "the final ACK still contains the maximum number of distinct commands"
    );
    // Derive the body from the actual command representation. Four commands
    // would fit the old array limit, but not their original ACK/lookup envelope.
    let mut sample = old[0].clone();
    let WorkerCommand::RunAttempt { task: input, .. } = &mut sample else {
        unreachable!()
    };
    input.instruction.clear();
    let overhead = serde_json::to_vec(&sample).unwrap().len();
    let desired_command_size = (metadata().transport.body_bytes - 4096 - 32 - 5) / 4;
    let instruction_len = desired_command_size.checked_sub(overhead).unwrap();
    let mut queued = Vec::new();
    for n in 0..4 {
        let mut input = coding(&format!("large-{n:03}"));
        input.instruction = "x".repeat(instruction_len);
        assert!(serde_json::to_vec(&input).unwrap().len() <= metadata().transport.body_bytes / 4);
        queued.push(submit(&c, &format!("large-submit-{n}"), input).await);
    }
    let operation = Operation::AwaitCommands {
        operation_id: "large-page".into(),
        worker_id: w.id.clone(),
        incarnation: w.incarnation.clone(),
        acknowledged: acknowledgments,
    };
    let hypothetical: Vec<_> = queued
        .iter()
        .map(|t| {
            let a = Attempt {
                id: format!("attempt-{}-1", t.id),
                session_id: t.session_id.clone(),
                task_id: t.id.clone(),
                generation: 1,
                worker_id: w.id.clone(),
                worker_incarnation: w.incarnation.clone(),
                phase: AttemptPhase::Assigned,
                deadline_unix_ms: t.input.limits.deadline_unix_ms,
                stopped: false,
                uncertain: false,
            };
            WorkerCommand::RunAttempt {
                command_id: format!("run-{}", a.id),
                session: (Session { id: t.session_id.clone(), agent_id:t.input.definition.id.clone(), definition:t.input.definition.clone(), task_id:t.id.clone() }).into(),
                attempt: a,
                task: (t.input.clone()).into(),
                checkpoint: None,
                continuation: None,
                workflow_continuation: None, completion_continuation: None,
            }
        })
        .collect();
    assert!(serde_json::to_vec(&hypothetical).unwrap().len() <= metadata().transport.body_bytes - 4096);
    let all = Outcome::Ok {
        data: Data::Commands(hypothetical),
    };
    let lookup_all = Outcome::Ok {
        data: Data::Operation(OperationRecord {
            id: "large-page".into(),
            task_id: String::new(),
            attempt_id: String::new(),
            generation: 0,
            kind: "application_mutation".into(),
            intended: serde_json::to_value(&operation).unwrap(),
            state: "settled".into(),
            outcome: Some(serde_json::to_value(&all).unwrap()),
        }),
    };
    assert!(
        serde_json::to_vec(&lookup_all).unwrap().len() > metadata().transport.body_bytes,
        "the actual large unique ACK request makes the four-command lookup unreturnable"
    );
    let accepted = exchange(&c, "initial-short", operation.clone()).await;
    let Outcome::Ok {
        data: Data::Commands(delivered),
    } = &accepted.outcome
    else {
        panic!("{accepted:?}")
    };
    assert_eq!(
        delivered.len(),
        3,
        "only commands included in the bounded receipt may be admitted"
    );
    let admitted: Vec<_> = delivered
        .iter()
        .map(|cmd| match cmd {
            WorkerCommand::RunAttempt { attempt, .. } => attempt.task_id.clone(),
            _ => panic!("expected Run"),
        })
        .collect();
    for t in &queued {
        let current = get(&c, &t.id).await;
        if admitted.contains(&t.id) {
            assert_eq!(current.status, TaskStatus::Active);
        } else {
            assert_eq!(current.status, TaskStatus::Queued);
            assert!(current.attempt.is_none());
        }
    }
    let maximal = "\\".repeat(256);
    let read = exchange(
        &c,
        &maximal,
        Operation::GetOperation {
            operation_id: "large-page".into(),
        },
    )
    .await;
    assert_eq!(read.commit, accepted.commit);
    app_record(&read, &operation, &accepted.outcome);
    let replay = exchange(&c, &maximal, operation.clone()).await;
    assert_eq!(replay.commit, accepted.commit);
    assert_eq!(replay.outcome, accepted.outcome);
    let ack: Vec<_> = delivered
        .iter()
        .map(|cmd| match cmd {
            WorkerCommand::RunAttempt { command_id, .. } => command_id.clone(),
            _ => unreachable!(),
        })
        .collect();
    let remainder = commands(&c, &w, ack).await;
    assert_eq!(remainder.len(), 1);
    assert!(!admitted.contains(&run(&remainder).task_id));
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg).unwrap();
    let replay = exchange(&c, &maximal, operation).await;
    assert_eq!(replay.commit, accepted.commit);
    assert_eq!(replay.outcome, accepted.outcome);
    assert!(replay.observed_controller_revision > accepted.observed_controller_revision);
}
