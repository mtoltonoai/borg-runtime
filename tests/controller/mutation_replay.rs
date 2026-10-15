use super::*;

#[tokio::test]
async fn fresh_schema_eleven_preserves_credit_columns_and_reopens() {
    let p = path();
    let c = Controller::open_with_config(&p, config())
        .expect("fresh schema must satisfy its startup validator");
    drop(c);
    controller_owner_released(&p).await;
    {
        let db =
            rusqlite::Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        assert_eq!(
            db.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            12
        );
        for (query, expected) in [
            (
                "SELECT type,\"notnull\",dflt_value FROM pragma_table_info('attempts') WHERE name='control_remaining'",
                ("INTEGER".to_string(), 1_i64, Some("0".to_string())),
            ),
            (
                "SELECT type,\"notnull\",dflt_value FROM pragma_table_info('commands') WHERE name='delivery_operation_id'",
                ("TEXT".to_string(), 0_i64, None),
            ),
            (
                "SELECT type,\"notnull\",dflt_value FROM pragma_table_info('external_operations') WHERE name='finalization_reserved'",
                ("INTEGER".to_string(), 1_i64, Some("0".to_string())),
            ),
        ] {
            let actual: (String, i64, Option<String>) = db
                .query_row(query, [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .unwrap();
            assert_eq!(actual, expected);
        }
        for query in [
            "SELECT coalesce(sum(control_remaining),0) FROM attempts",
            "SELECT coalesce(sum(finalization_reserved),0) FROM external_operations",
            "SELECT coalesce(sum(cleanup),0) FROM tasks",
            "SELECT count(*) FROM receipts",
        ] {
            assert_eq!(db.query_row(query, [], |r| r.get::<_, i64>(0)).unwrap(), 0);
        }
    }
    let c = Controller::open_with_config(&p, config()).expect("unchanged empty schema must reopen");
    assert_eq!(
        call(
            &c,
            Operation::ListTasks {
                after: None,
                limit: 1
            }
        )
        .await,
        Outcome::Ok {
            data: Data::Tasks(vec![])
        }
    );
}

fn poll(id: &str, w: &Worker, acknowledged: Vec<String>) -> Operation {
    Operation::AwaitCommands {
        operation_id: id.into(),
        worker_id: w.id.clone(),
        incarnation: w.incarnation.clone(),
        acknowledged,
    }
}
fn registration(id: &str, w: &Worker) -> Operation {
    Operation::RegisterWorker {
        operation_id: id.into(),
        worker: w.clone(),
    }
}
fn command_ids(outcome: &Outcome) -> Vec<String> {
    match outcome {
        Outcome::Ok {
            data: Data::Commands(commands),
        } => commands
            .iter()
            .map(|c| match c {
                WorkerCommand::RunAttempt { command_id, .. }
                | WorkerCommand::StopAttempt { command_id, .. } => command_id.clone(),
            })
            .collect(),
        other => panic!("expected recorded command response: {other:?}"),
    }
}
fn effect(a: &Attempt, operation: &str) -> Operation {
    Operation::PrepareOperation {
        operation_id: operation.into(),
        task_id: a.task_id.clone(),
        attempt_id: a.id.clone(),
        generation: a.generation,
        kind: "merge".into(),
        intended: json!({"controlled":"original effect"}),
    }
}
fn settle(a: &Attempt, operation: &str, mutation: &str, outcome: serde_json::Value) -> Operation {
    Operation::SettleOperation {
        settlement_id: mutation.into(),
        operation_id: operation.into(),
        task_id: a.task_id.clone(),
        attempt_id: a.id.clone(),
        generation: a.generation,
        outcome,
        checkpoint: None,
    }
}

#[derive(Debug, PartialEq)]
struct Ledger {
    normal: i64,
    used: i64,
    tasks: i64,
    controls: i64,
    effects: i64,
}
impl Ledger {
    fn total(&self) -> i64 {
        self.used + self.tasks + self.controls + self.effects
    }
}
async fn restart(
    p: &std::path::Path,
    c: Controller,
    cfg: &ControllerConfig,
) -> (Controller, Ledger) {
    drop(c);
    controller_owner_released(p).await;
    let ledger = {
        let db =
            rusqlite::Connection::open_with_flags(p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        let n = |sql| db.query_row(sql, [], |r| r.get::<_, i64>(0)).unwrap();
        Ledger {
            normal: n("SELECT count(*) FROM receipts WHERE cleanup=0"),
            used: n("SELECT count(*) FROM receipts WHERE cleanup=1"),
            tasks: n("SELECT coalesce(sum(cleanup),0) FROM tasks"),
            controls: n("SELECT coalesce(sum(control_remaining),0) FROM attempts"),
            effects: n("SELECT coalesce(sum(finalization_reserved),0) FROM external_operations"),
        }
    };
    assert!(ledger.total() <= cfg.max_cleanup_receipts as i64);
    (
        Controller::open_with_config(p, cfg.clone()).unwrap(),
        ledger,
    )
}

#[tokio::test]
// Historical registration outcomes do not reinstall a retired incarnation.
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# An exact retry of a committed mutating request MUST return its recorded outcome.
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# Reusing a mutating request's identity with different input MUST fail without changing its recorded outcome.
async fn registration_replay_is_historical_not_reinstallation() {
    let p = path();
    let cfg = config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let a = worker("registered", "a", 0, 1);
    let b = worker("registered", "b", 0, 1);
    let original = registration("register-a", &a);
    let accepted = call(&c, original.clone()).await;
    assert_eq!(
        accepted,
        Outcome::Ok {
            data: Data::Worker(a.clone())
        }
    );
    assert_eq!(
        call(&c, registration("register-b", &b)).await,
        Outcome::Ok {
            data: Data::Worker(b.clone())
        }
    );
    let (c, before) = restart(&p, c, &cfg).await;
    assert_eq!(call(&c, original.clone()).await, accepted);
    let (c, replayed) = restart(&p, c, &cfg).await;
    assert_eq!(replayed, before);
    let mut different = a.clone();
    different.coding_capacity = 2;
    assert_eq!(
        code(call(&c, registration("register-a", &different)).await),
        "operation_conflict"
    );
    assert_eq!(
        code(call(&c, registration("new-retired-request", &a)).await),
        "stale_incarnation"
    );
    let t = submit(&c, "registered-task", coding("registered-agent")).await;
    let commands = commands(&c, &b, vec![]).await;
    let current = run(&commands);
    assert_eq!(current.task_id, t.id);
    assert_eq!(current.worker_incarnation, "b");
    assert_eq!(call(&c, original).await, accepted);
    assert_eq!(get(&c, &t.id).await.attempt.unwrap(), current);
}

#[tokio::test]
async fn pending_waits_do_not_churn_receipts_and_ack_empty_is_committed() {
    let p = path();
    let cfg = config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let w = worker("waiter", "inc", 0, 1);
    register(&c, &w).await;
    let idle = poll("idle-exchange", &w, vec![]);
    for _ in 0..3 {
        assert_eq!(
            call(&c, idle.clone()).await,
            Outcome::Pending {
                operation_id: "idle-exchange".into()
            }
        );
    }
    assert_eq!(
        code(
            call(
                &c,
                Operation::GetOperation {
                    operation_id: "idle-exchange".into()
                }
            )
            .await
        ),
        "not_found"
    );
    let (c, ledger) = restart(&p, c, &cfg).await;
    assert_eq!(ledger.normal, 1);
    assert_eq!(ledger.total(), 0);
    assert!(matches!(
        call(&c, idle.clone()).await,
        Outcome::Pending { .. }
    ));
    let submitted = submit(&c, "wait-submission", coding("wait-agent")).await;
    let assigned = call(&c, idle.clone()).await;
    let ids = command_ids(&assigned);
    assert_eq!(ids.len(), 1);
    assert_eq!(
        get(&c, &submitted.id).await.attempt.as_ref().unwrap().phase,
        AttemptPhase::Assigned
    );
    assert_eq!(call(&c, idle.clone()).await, assigned);
    assert_eq!(
        code(call(&c, poll("idle-exchange", &w, ids.clone())).await),
        "operation_conflict"
    );
    let mut long_wait = cfg.clone();
    Arc::make_mut(&mut long_wait.limits).controller.command_wait_ms = (Duration::from_secs(30)).as_millis().try_into().unwrap();
    let (c, _) = restart(&p, c, &long_wait).await;
    let ack = poll("ack-empty", &w, ids);
    let empty = tokio::time::timeout(Duration::from_secs(2), call(&c, ack.clone()))
        .await
        .expect("committed empty must not wait for the 30-second timeout");
    assert_eq!(
        empty,
        Outcome::Ok {
            data: Data::Commands(vec![])
        }
    );
    let (c, _) = restart(&p, c, &long_wait).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), call(&c, ack))
            .await
            .unwrap(),
        empty
    );
    assert_eq!(call(&c, idle).await, assigned);
}

#[tokio::test]
async fn full_normal_pool_retains_exact_stop_ack_and_effect_finalization() {
    let p = path();
    let cfg = with_controller_limits(ControllerConfig {
        ..config()
    }, |limits| { limits.max_receipts = 5; limits.max_cleanup_receipts = 9; });
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let w = worker("cleanup", "inc", 0, 1);
    register_repository_fixture(&c, &fixture_repository_registration()).await; // normal 1: fixture setup
    register(&c, &w).await; // normal 2
    let t = submit(&c, "cleanup-submit", coding("cleanup-agent")).await; // normal 3
    let initial = call(&c, poll("cleanup-assign", &w, vec![])).await; // normal 4
    let a = get(&c, &t.id).await.attempt.unwrap();
    assert!(matches!(
        call(&c, effect(&a, "cleanup-effect")).await,
        Outcome::Ok { .. }
    )); // normal 5
    let (c, full) = restart(&p, c, &cfg).await;
    assert_eq!(
        full,
        Ledger {
            normal: 5,
            used: 0,
            tasks: 5,
            controls: 3,
            effects: 1
        }
    );
    // A retained ordinary report has no committed receipt at exhaustion. It
    // must not become a controller-side sequence gate ahead of real cleanup.
    let started = Observation::Started {
        thread_id: None,
        turn_id: None,
    };
    let original_started = report(&c, &a, "cleanup-started-pending", started.clone()).await;
    assert_eq!(code(original_started.clone()), "capacity");
    assert_eq!(
        code(
            call(
                &c,
                Operation::GetOperation {
                    operation_id: "cleanup-started-pending".into(),
                }
            )
            .await
        ),
        "not_found"
    );
    assert_eq!(
        get(&c, &t.id).await.attempt.unwrap().phase,
        AttemptPhase::Assigned
    );
    let current = get(&c, &t.id).await;
    task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "cleanup-cancel".into(),
                task_id: t.id.clone(),
                expected_revision: current.revision,
            },
        )
        .await,
    );
    let delivery = poll("cleanup-stop-delivery", &w, vec![]);
    let delivered = call(&c, delivery.clone()).await;
    let ids = command_ids(&delivered);
    assert_eq!(ids.len(), 1);
    assert!(matches!(&delivered, Outcome::Ok { data: Data::Commands(v) }
        if v.iter().all(|c| matches!(c, WorkerCommand::StopAttempt { attempt_id, .. } if attempt_id == &a.id))));
    let (c, offered) = restart(&p, c, &cfg).await;
    assert_eq!(
        offered,
        Ledger {
            normal: 5,
            used: 2,
            tasks: 4,
            controls: 2,
            effects: 1
        }
    );
    assert_eq!(call(&c, delivery).await, delivered);
    for id in ["duplicate-read-a", "duplicate-read-b"] {
        assert_eq!(
            call(&c, poll(id, &w, vec![])).await,
            Outcome::Pending {
                operation_id: id.into()
            }
        );
    }
    // Run was suppressed by cancellation. Its duplicate ACK earns no credit.
    assert!(matches!(
        call(&c, poll("suppressed-run-ack", &w, command_ids(&initial))).await,
        Outcome::Pending { .. }
    ));
    let mut mixed = ids.clone();
    mixed.push("foreign-command".into());
    assert!(matches!(
        call(&c, poll("mixed-ack", &w, mixed)).await,
        Outcome::Refused { .. }
    ));
    let (c, untouched) = restart(&p, c, &cfg).await;
    assert_eq!(untouched, offered);
    let ack = poll("cleanup-stop-ack", &w, ids.clone());
    let empty = call(&c, ack.clone()).await;
    assert_eq!(
        empty,
        Outcome::Ok {
            data: Data::Commands(vec![])
        }
    );
    assert_eq!(call(&c, ack).await, empty);
    assert!(matches!(
        call(&c, poll("duplicate-stop-ack", &w, ids)).await,
        Outcome::Pending { .. }
    ));
    let (c, acknowledged) = restart(&p, c, &cfg).await;
    assert_eq!(
        acknowledged,
        Ledger {
            normal: 5,
            used: 3,
            tasks: 4,
            controls: 1,
            effects: 1
        }
    );
    assert!(matches!(
        call(
            &c,
            settle(
                &a,
                "cleanup-effect",
                "unfunded-uncertainty",
                json!({"acknowledged":false})
            )
        )
        .await,
        Outcome::Refused { .. }
    ));
    let mut wrong_owner = a.clone();
    wrong_owner.generation += 1;
    assert!(matches!(
        call(
            &c,
            settle(
                &wrong_owner,
                "cleanup-effect",
                "wrong-final-owner",
                json!({"acknowledged":true})
            )
        )
        .await,
        Outcome::Refused { .. }
    ));
    let (c, still_reserved) = restart(&p, c, &cfg).await;
    assert_eq!(still_reserved, acknowledged);
    let final_request = settle(
        &a,
        "cleanup-effect",
        "cleanup-final",
        json!({"acknowledged":true,"not_applied":true}),
    );
    let final_outcome = call(&c, final_request.clone()).await;
    assert!(
        matches!(&final_outcome, Outcome::Ok { data: Data::Operation(r) } if r.state == "settled")
    );
    assert_eq!(call(&c, final_request).await, final_outcome);
    let (c, settled) = restart(&p, c, &cfg).await;
    assert_eq!(
        settled,
        Ledger {
            normal: 5,
            used: 4,
            tasks: 4,
            controls: 1,
            effects: 0
        }
    );
    let stopped = task(report(&c, &a, "cleanup-stopped", Observation::ProcessTreeStopped).await);
    assert!(stopped.attempt.unwrap().stopped);
    let (c, stopped_ledger) = restart(&p, c, &cfg).await;
    assert_eq!(
        stopped_ledger,
        Ledger {
            normal: 5,
            used: 5,
            tasks: 3,
            controls: 0,
            effects: 0
        }
    );
    assert_eq!(
        report(&c, &a, "cleanup-started-pending", started).await,
        original_started
    );
    assert_eq!(
        code(
            call(
                &c,
                Operation::GetOperation {
                    operation_id: "cleanup-started-pending".into(),
                }
            )
            .await
        ),
        "not_found"
    );
    assert!(get(&c, &t.id).await.attempt.unwrap().stopped);
}

#[tokio::test]
// Each settlement observation has its own immutable receipt; the external
// effect continues under its separate original identity.
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# An exact retry of a committed mutating request MUST return its recorded outcome.
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# Reusing a mutating request's identity with different input MUST fail without changing its recorded outcome.
async fn settlement_receipts_remain_independent_of_later_effect_resolution() {
    let p = path();
    let cfg = config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let w = worker("settlement", "inc", 0, 1);
    register(&c, &w).await;
    let t = submit(&c, "settlement-submit", coding("settlement-agent")).await;
    let a = run(&commands(&c, &w, vec![]).await);
    assert_eq!(a.task_id, t.id);
    assert!(matches!(
        call(&c, effect(&a, "effect-original")).await,
        Outcome::Ok { .. }
    ));
    let uncertain = settle(
        &a,
        "effect-original",
        "observation-u",
        json!({"acknowledged":false,"message":"unknown"}),
    );
    let u = call(&c, uncertain.clone()).await;
    assert!(matches!(&u, Outcome::Ok { data: Data::Operation(r) } if r.state == "uncertain"));
    let final_request = settle(
        &a,
        "effect-original",
        "observation-f",
        json!({"acknowledged":true,"not_applied":true}),
    );
    let f = call(&c, final_request.clone()).await;
    assert!(matches!(&f, Outcome::Ok { data: Data::Operation(r) } if r.state == "settled"));
    assert_eq!(call(&c, uncertain.clone()).await, u);
    assert_eq!(call(&c, final_request.clone()).await, f);
    let before = get(&c, &t.id).await;
    let equivalent = settle(
        &a,
        "effect-original",
        "observation-equivalent",
        json!({"acknowledged":true,"not_applied":true}),
    );
    assert_eq!(call(&c, equivalent).await, f);
    assert_eq!(get(&c, &t.id).await, before);
    assert_eq!(
        code(
            call(
                &c,
                settle(
                    &a,
                    "effect-original",
                    "observation-u",
                    json!({"acknowledged":true})
                )
            )
            .await
        ),
        "operation_conflict"
    );
    assert_eq!(
        code(
            call(
                &c,
                settle(
                    &a,
                    "effect-original",
                    "observation-different-final",
                    json!({"acknowledged":true,"different":true})
                )
            )
            .await
        ),
        "operation_conflict"
    );
    drop(c);
    controller_owner_released(&p).await;
    {
        let db =
            rusqlite::Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        let original: String = db
            .query_row(
                "SELECT settlement FROM external_operations WHERE id='effect-original'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(original, serde_json::to_string(&final_request).unwrap());
    }
    let c = Controller::open_with_config(&p, cfg).unwrap();
    assert_eq!(call(&c, uncertain).await, u);
    assert_eq!(call(&c, final_request).await, f);
    assert_eq!(get(&c, &t.id).await, before);
}

#[tokio::test]
async fn exact_credit_bound_precedes_assignment_and_external_intent() {
    for capacity in [7, 8, 9] {
        let p = path();
        let cfg = with_controller_limits(ControllerConfig {
            ..config()
        }, |limits| { limits.max_cleanup_receipts = capacity; });
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        let w = worker("bound", "inc", 0, 1);
        register(&c, &w).await;
        let t = submit(&c, "bound-submit", coding("bound-agent")).await;
        let offered = commands(&c, &w, vec![]).await;
        if capacity == 7 {
            assert!(offered.is_empty());
            let queued = get(&c, &t.id).await;
            assert!(queued.attempt.is_none());
            assert_eq!(queued.blocked_reason.as_deref(), Some("cleanup_capacity"));
        } else {
            let a = run(&offered);
            let intent = effect(&a, "bound-effect");
            let outcome = call(&c, intent.clone()).await;
            if capacity == 8 {
                assert_eq!(code(outcome.clone()), "capacity");
                assert_eq!(call(&c, intent).await, outcome);
                let current = get(&c, &t.id).await;
                assert!(!current.attempt.unwrap().uncertain);
            } else {
                assert!(
                    matches!(outcome, Outcome::Ok { data: Data::Operation(r) } if r.state == "pending")
                );
            }
        }
        let (_, ledger) = restart(&p, c, &cfg).await;
        assert_eq!(ledger.effects, if capacity == 9 { 1 } else { 0 });
        assert_eq!(ledger.controls, if capacity == 7 { 0 } else { 3 });
        assert_eq!(ledger.tasks, if capacity == 7 { 1 } else { 5 });
    }
}

#[tokio::test]
async fn byte_bound_keeps_undeliverable_attempt_queued_until_actual_ack() {
    let p = path();
    let cfg = config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let w = worker("pages", "inc", 0, 5);
    register(&c, &w).await;
    let mut submitted = Vec::new();
    for n in 0..5 {
        let mut input = coding(&format!("page-agent-{n}"));
        input.instruction = "x".repeat(240_000);
        submitted.push(submit(&c, &format!("page-submit-{n}"), input).await);
    }
    let response = call(&c, poll("page-delivery", &w, vec![])).await;
    let delivered = command_ids(&response);
    assert_eq!(delivered.len(), 4);
    let fifth = get(&c, &submitted[4].id).await;
    assert_eq!(fifth.status, TaskStatus::Queued);
    assert!(fifth.attempt.is_none());
    assert_eq!(
        fifth.blocked_reason.as_deref(),
        Some("command_delivery_capacity")
    );
    drop(c);
    controller_owner_released(&p).await;
    {
        let db =
            rusqlite::Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        let mut q = db
            .prepare("SELECT id,delivery_operation_id FROM commands ORDER BY seq")
            .unwrap();
        let rows = q
            .query_map([], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows.len(), 4);
        for (id, marker) in rows {
            assert!(delivered.contains(&id));
            assert_eq!(marker.as_deref(), Some("page-delivery"));
        }
        assert_eq!(
            db.query_row(
                "SELECT count(*) FROM attempts WHERE task_id=?1",
                [&fifth.id],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
    }
    let c = Controller::open_with_config(&p, cfg).unwrap();
    assert_eq!(call(&c, poll("page-delivery", &w, vec![])).await, response);
    assert_eq!(get(&c, &fifth.id).await, fifth);
    let next_request = poll("page-ack-and-next", &w, delivered);
    let next = call(&c, next_request.clone()).await;
    let Outcome::Ok {
        data: Data::Commands(commands),
    } = &next
    else {
        panic!("{next:?}");
    };
    let [WorkerCommand::RunAttempt { attempt, .. }] = commands.as_slice() else {
        panic!("{commands:?}");
    };
    assert_eq!(attempt.task_id, fifth.id);
    assert_eq!(attempt.generation, 1);
    assert_eq!(get(&c, &fifth.id).await.attempt.as_ref(), Some(attempt));
    assert_eq!(call(&c, next_request).await, next);
}

#[tokio::test]
async fn existing_run_and_stop_page_never_strands_an_unreturned_new_attempt() {
    let p = path();
    let cfg = with_controller_limits(ControllerConfig {
        ..config()
    }, |limits| { limits.max_commands_per_reply = 1; });
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let w = worker("one-page", "inc", 0, 2);
    register(&c, &w).await;
    let a = submit(&c, "one-page-a", coding("one-page-agent-a")).await;
    let first = call(&c, poll("one-page-first", &w, vec![])).await;
    let a_attempt = get(&c, &a.id).await.attempt.unwrap();
    assert_eq!(command_ids(&first).len(), 1);
    let b = submit(&c, "one-page-b", coding("one-page-agent-b")).await;
    let repeated = call(&c, poll("one-page-old-run", &w, vec![])).await;
    assert_eq!(repeated, first);
    let queued = get(&c, &b.id).await;
    assert_eq!(queued.status, TaskStatus::Queued);
    assert!(queued.attempt.is_none());
    assert_eq!(
        queued.blocked_reason.as_deref(),
        Some("command_delivery_capacity")
    );
    let (c, ledger) = restart(&p, c, &cfg).await;
    assert_eq!(ledger.controls, 3); // Only A was admitted; B owns no attempt credit.
    assert_eq!(get(&c, &b.id).await, queued);
    assert_eq!(
        call(&c, poll("one-page-old-run", &w, vec![])).await,
        repeated
    );
    // Current cancellation suppresses A's Run, but its unacknowledged Stop
    // still occupies the sole response slot. B must remain unassigned.
    let current = get(&c, &a.id).await;
    task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "one-page-cancel-a".into(),
                task_id: a.id.clone(),
                expected_revision: current.revision,
            },
        )
        .await,
    );
    let stop = call(&c, poll("one-page-stop", &w, vec![])).await;
    assert!(matches!(&stop, Outcome::Ok { data: Data::Commands(v) }
        if matches!(v.as_slice(), [WorkerCommand::StopAttempt { attempt_id, .. }] if attempt_id == &a_attempt.id)));
    assert!(get(&c, &b.id).await.attempt.is_none());
    assert!(!get(&c, &a.id).await.attempt.unwrap().stopped);
    let next_request = poll("one-page-ack-stop", &w, command_ids(&stop));
    let next = call(&c, next_request.clone()).await;
    let Outcome::Ok {
        data: Data::Commands(v),
    } = &next
    else {
        panic!("{next:?}");
    };
    let [WorkerCommand::RunAttempt { attempt, .. }] = v.as_slice() else {
        panic!("{v:?}");
    };
    assert_eq!(attempt.task_id, b.id);
    assert_eq!(attempt.generation, 1);
    assert_eq!(get(&c, &b.id).await.attempt.as_ref(), Some(attempt));
    assert_eq!(call(&c, next_request).await, next);
    // The original response remains historical; it does not undo cancellation.
    assert_eq!(call(&c, poll("one-page-first", &w, vec![])).await, first);
    assert_eq!(get(&c, &a.id).await.status, TaskStatus::Cancelled);
}

#[tokio::test]
// Traceability scope: Real controller transactions reject pre-admission/pre-successor revisions while replaying an original cancellation without changing the current generation.
//= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
//= type=test
//# A task mutation MUST verify its current ownership generation.
async fn operator_revision_fences_new_generation_while_original_receipt_replays() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let submitted = submit(&c, "generation-submit", coding("generation-agent")).await;
    let w = worker("generation", "inc", 0, 1);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    assert_eq!(
        code(
            call(
                &c,
                Operation::CancelTask {
                    operation_id: "stale-before-assignment".into(),
                    task_id: submitted.id.clone(),
                    expected_revision: submitted.revision
                }
            )
            .await
        ),
        "stale_revision"
    );
    let assigned = get(&c, &submitted.id).await;
    let cancellation = Operation::CancelTask {
        operation_id: "generation-cancel".into(),
        task_id: submitted.id.clone(),
        expected_revision: assigned.revision,
    };
    let original = call(&c, cancellation.clone()).await;
    let stopped = task(
        report(
            &c,
            &a,
            "generation-stopped",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    let queued = task(
        call(
            &c,
            Operation::RetryTask {
                operation_id: "generation-retry".into(),
                task_id: submitted.id.clone(),
                expected_revision: stopped.revision,
            },
        )
        .await,
    );
    let next = run(&commands(&c, &w, vec![]).await);
    assert_eq!(next.generation, a.generation + 1);
    let current = get(&c, &submitted.id).await;
    assert_eq!(
        code(
            call(
                &c,
                Operation::CancelTask {
                    operation_id: "stale-before-successor".into(),
                    task_id: submitted.id.clone(),
                    expected_revision: queued.revision
                }
            )
            .await
        ),
        "stale_revision"
    );
    assert_eq!(call(&c, cancellation).await, original);
    assert_eq!(get(&c, &submitted.id).await, current);
}

#[tokio::test]
async fn malformed_credit_and_delivery_bindings_refuse_unchanged_at_startup() {
    for mode in ["delivery", "undelivered", "control", "effect", "owner"] {
        let p = path();
        let c = Controller::open_with_config(&p, config()).unwrap();
        let t = submit(&c, "integrity-submit", coding("integrity-agent")).await;
        if mode != "owner" {
            let w = worker("integrity", "inc", 0, 1);
            register(&c, &w).await;
            let a = run(&commands(&c, &w, vec![]).await);
            if mode == "effect" {
                assert!(matches!(
                    call(&c, effect(&a, "integrity-effect")).await,
                    Outcome::Ok { .. }
                ));
            }
        }
        drop(c);
        controller_owner_released(&p).await;
        {
            let db = rusqlite::Connection::open(&p).unwrap();
            match mode {
                "delivery" => {
                    db.execute(
                        "UPDATE commands SET delivery_operation_id='missing-receipt'",
                        [],
                    )
                    .unwrap();
                }
                "undelivered" => {
                    db.execute("UPDATE commands SET delivery_operation_id=NULL", [])
                        .unwrap();
                }
                "control" => {
                    db.execute_batch("PRAGMA ignore_check_constraints=ON; UPDATE attempts SET control_remaining=4;").unwrap();
                }
                "effect" => {
                    db.execute("UPDATE external_operations SET finalization_reserved=0", [])
                        .unwrap();
                }
                "owner" => {
                    // Preserve canonical field ordering while making the
                    // no-attempt generation invariant inconsistent.
                    let original: String = db
                        .query_row("SELECT body FROM tasks WHERE id=?1", [&t.id], |r| r.get(0))
                        .unwrap();
                    assert!(original.contains("\"generation\":0"));
                    let altered = original.replacen("\"generation\":0", "\"generation\":1", 1);
                    db.execute(
                        "UPDATE tasks SET body=?2 WHERE id=?1",
                        rusqlite::params![t.id, altered],
                    )
                    .unwrap();
                }
                _ => unreachable!(),
            }
            db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                .unwrap();
        }
        let before = std::fs::read(&p).unwrap();
        assert!(
            Controller::open_with_config(&p, config()).is_err(),
            "{mode}"
        );
        assert_eq!(std::fs::read(&p).unwrap(), before, "{mode}");
    }
}

#[tokio::test]
async fn batched_stop_transitions_charge_one_receipt_without_cross_owner_theft() {
    let p = path();
    let cfg = with_controller_limits(ControllerConfig {
        ..config()
    }, |limits| { limits.max_receipts = 6; limits.max_cleanup_receipts = 16; });
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let w = worker("batch-credit", "inc", 0, 2);
    register(&c, &w).await;
    let first = submit(&c, "batch-first", coding("batch-first-agent")).await;
    let second = submit(&c, "batch-second", coding("batch-second-agent")).await;
    let assigned = commands(&c, &w, vec![]).await;
    assert_eq!(assigned.len(), 2);
    let mut invalid = coding("batch-fill");
    invalid.instruction.clear();
    assert_eq!(
        code(
            call(
                &c,
                Operation::SubmitTask {
                    operation_id: "batch-fill".into(),
                    input: invalid
                }
            )
            .await
        ),
        "invalid_input"
    );
    for (id, t) in [("batch-cancel-a", &first), ("batch-cancel-b", &second)] {
        let current = get(&c, &t.id).await;
        task(
            call(
                &c,
                Operation::CancelTask {
                    operation_id: id.into(),
                    task_id: t.id.clone(),
                    expected_revision: current.revision,
                },
            )
            .await,
        );
    }
    let delivered = call(&c, poll("batch-delivery", &w, vec![])).await;
    let ids = command_ids(&delivered);
    assert_eq!(ids.len(), 2);
    assert!(matches!(&delivered, Outcome::Ok { data: Data::Commands(v) }
        if v.iter().all(|c| matches!(c, WorkerCommand::StopAttempt { .. }))));
    let (c, offered) = restart(&p, c, &cfg).await;
    assert_eq!(
        offered,
        Ledger {
            normal: 6,
            used: 3,
            tasks: 8,
            controls: 5,
            effects: 0
        }
    );
    let mut other_incarnation = w.clone();
    other_incarnation.incarnation = "foreign".into();
    assert!(matches!(
        call(&c, poll("foreign-ack", &other_incarnation, ids.clone())).await,
        Outcome::Refused { .. }
    ));
    let (c, untouched) = restart(&p, c, &cfg).await;
    assert_eq!(untouched, offered);
    let acknowledgment = poll("batch-ack", &w, ids);
    let outcome = call(&c, acknowledgment.clone()).await;
    assert_eq!(
        outcome,
        Outcome::Ok {
            data: Data::Commands(vec![])
        }
    );
    let (c, acknowledged) = restart(&p, c, &cfg).await;
    assert_eq!(
        acknowledged,
        Ledger {
            normal: 6,
            used: 4,
            tasks: 8,
            controls: 4,
            effects: 0
        }
    );
    assert_eq!(call(&c, acknowledgment).await, outcome);
    let (_, replayed) = restart(&p, c, &cfg).await;
    assert_eq!(replayed, acknowledged);
}

fn received_only_candidate(checks_passed: bool) -> Observation {
    Observation::ResultCandidate {
        checks_passed,
        summary: "received command was stopped without executor execution".into(),
        checkpoint: None,
        validation: vec![],
        output: None,
    }
}

#[tokio::test]
// This is a platform failure after a controlled ProcessTreeStopped input, not a
// fabricated executor exit or a positive repository/provider result.
//= spec/impl/v2/implementation.md#task-state-separates-execution-from-acceptance
//= type=test
//# An executor outcome MUST remain distinct from the task's accepted outcome.
async fn received_only_stopped_failure_keeps_executor_absent_and_allows_fresh_retry() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let submitted = submit(&c, "received-only-submit", coding("received-only-agent")).await;
    let w = worker("received-only", "inc", 0, 1);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let assigned = get(&c, &submitted.id).await;
    assert_eq!(
        assigned.attempt.as_ref().unwrap().phase,
        AttemptPhase::Assigned
    );
    let premature = report(
        &c,
        &a,
        "received-only-before-stop",
        received_only_candidate(false),
    )
    .await;
    assert_eq!(code(premature.clone()), "invalid_transition");
    assert_eq!(get(&c, &submitted.id).await, assigned);
    let stopped = task(
        report(
            &c,
            &a,
            "received-only-stopped",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    assert_eq!(stopped.status, TaskStatus::Active);
    assert!(stopped.attempt.as_ref().unwrap().stopped);
    assert!(!stopped.attempt.as_ref().unwrap().uncertain);
    assert_eq!(
        code(
            report(
                &c,
                &a,
                "received-only-false-success",
                received_only_candidate(true)
            )
            .await
        ),
        "invalid_transition"
    );
    assert_eq!(get(&c, &submitted.id).await, stopped);
    assert_eq!(
        report(
            &c,
            &a,
            "received-only-before-stop",
            received_only_candidate(false)
        )
        .await,
        premature
    );
    let failed = task(
        report(
            &c,
            &a,
            "received-only-failure",
            received_only_candidate(false),
        )
        .await,
    );
    assert_eq!(failed.status, TaskStatus::Failed);
    assert_eq!(failed.blocked_reason.as_deref(), Some("acceptance_failed"));
    assert!(failed.result.is_none());
    assert!(failed.selected_checkpoint.is_none());
    drop(c);
    controller_owner_released(&p).await;
    {
        let db =
            rusqlite::Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        let executor: Option<String> = db
            .query_row(
                "SELECT executor_outcome FROM attempts WHERE id=?1",
                [&a.id],
                |r| r.get(0),
            )
            .unwrap();
        assert!(executor.is_none());
        let body: String = db
            .query_row("SELECT body FROM tasks WHERE id=?1", [&submitted.id], |r| {
                r.get(0)
            })
            .unwrap();
        assert!(
            serde_json::from_str::<serde_json::Value>(&body).unwrap()["executor_success"].is_null()
        );
        assert_eq!(db.query_row("SELECT count(*) FROM history WHERE task_id=?1 AND kind='worker_observation' AND json_extract(body,'$.kind')='executor_outcome'", [&submitted.id], |r| r.get::<_,i64>(0)).unwrap(), 0);
    }
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(get(&c, &submitted.id).await, failed);
    assert_eq!(
        task(
            report(
                &c,
                &a,
                "received-only-failure",
                received_only_candidate(false)
            )
            .await
        ),
        failed
    );
    let queued = task(
        call(
            &c,
            Operation::RetryTask {
                operation_id: "received-only-retry".into(),
                task_id: submitted.id.clone(),
                expected_revision: failed.revision,
            },
        )
        .await,
    );
    assert_eq!(queued.status, TaskStatus::Queued);
    let next = run(&commands(&c, &w, vec![]).await);
    assert_eq!(next.task_id, submitted.id);
    assert_ne!(next.id, a.id);
    assert_eq!(next.generation, a.generation + 1);
    let current = get(&c, &submitted.id).await;
    assert_eq!(
        task(
            report(
                &c,
                &a,
                "received-only-failure",
                received_only_candidate(false)
            )
            .await
        ),
        failed
    );
    assert_eq!(
        code(
            report(
                &c,
                &a,
                "received-only-stale-new-report",
                received_only_candidate(false)
            )
            .await
        ),
        "stale_generation"
    );
    assert_eq!(get(&c, &submitted.id).await, current);
}

#[tokio::test]
async fn received_only_failure_cannot_override_uncertainty_or_cancellation() {
    for uncertain in [false, true] {
        let p = path();
        let c = Controller::open_with_config(&p, config()).unwrap();
        let t = submit(
            &c,
            "received-refusal-submit",
            coding("received-refusal-agent"),
        )
        .await;
        let w = worker("received-refusal", "inc", 0, 1);
        register(&c, &w).await;
        let a = run(&commands(&c, &w, vec![]).await);
        if uncertain {
            assert!(matches!(
                call(&c, effect(&a, "received-unknown-effect")).await,
                Outcome::Ok { .. }
            ));
        } else {
            let current = get(&c, &t.id).await;
            task(
                call(
                    &c,
                    Operation::CancelTask {
                        operation_id: "received-cancel".into(),
                        task_id: t.id.clone(),
                        expected_revision: current.revision,
                    },
                )
                .await,
            );
        }
        let stopped = task(
            report(
                &c,
                &a,
                "received-refusal-stopped",
                Observation::ProcessTreeStopped,
            )
            .await,
        );
        assert!(stopped.attempt.as_ref().unwrap().stopped);
        assert_eq!(stopped.attempt.as_ref().unwrap().uncertain, uncertain);
        let outcome = report(
            &c,
            &a,
            "received-refused-failure",
            received_only_candidate(false),
        )
        .await;
        assert_eq!(
            code(outcome.clone()),
            if uncertain {
                "reconciliation_required"
            } else {
                "invalid_transition"
            }
        );
        assert_eq!(get(&c, &t.id).await, stopped);
        drop(c);
        let c = Controller::open_with_config(&p, config()).unwrap();
        assert_eq!(
            report(
                &c,
                &a,
                "received-refused-failure",
                received_only_candidate(false)
            )
            .await,
            outcome
        );
        assert_eq!(get(&c, &t.id).await, stopped);
    }
}
