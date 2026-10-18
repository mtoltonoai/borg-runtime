//! Real controller receipts and startup admission with controlled public Codex
//! facts. These cases do not claim a provider call or app-server qualification.
use super::*;
use borg_runtime::{
    collaboration::{AttemptView, Query, QueryResult},
    supplied_outer::{self, Cause, Observation as Outer, RetryPolicy, Wait},
};
use std::collections::BTreeMap;

const THREAD: &str = "controlled-supplied-thread";
const TURN: &str = "controlled-turn-1";

fn retry_config() -> (ControllerConfig, Arc<AtomicU64>, RetryPolicy) {
    let now = Arc::new(AtomicU64::new(1_000));
    let clock = now.clone();
    let mut cfg = config();
    cfg.clock = Arc::new(move || clock.load(Ordering::SeqCst));
    let policy = RetryPolicy {
        initial_backoff_ms: 10,
        backoff_multiplier: 2,
        max_backoff_ms: 40,
        jitter_ms: 3,
    };
    Arc::make_mut(&mut cfg.limits)
        .execution
        .coding
        .rate_limit_retry = Some(policy.clone());
    cfg.limits.validate().unwrap();
    (cfg, now, policy)
}

async fn setup(c: &Controller) -> Attempt {
    let mut input = coding("supplied-retry-agent");
    input.limits.deadline_unix_ms = 100_000;
    input.limits.model_requests = 3;
    assert!(
        input.definition.charter.is_none(),
        "non-chartered admission is intentional"
    );
    let original = submit(c, "supplied-retry-submit", input).await;
    let w = worker("supplied-worker", "original-incarnation", 0, 1);
    register(c, &w).await;
    let a = run(&commands(c, &w, vec![]).await);
    assert_eq!(a.task_id, original.id);
    task(
        report(
            c,
            &a,
            "supplied-started",
            Observation::Started {
                thread_id: Some(THREAD.into()),
                turn_id: Some(TURN.into()),
            },
        )
        .await,
    );
    task(
        report(
            c,
            &a,
            "supplied-original-usage",
            Observation::Usage {
                model_requests: 0,
                input_tokens: Some(11),
                output_tokens: Some(7),
                native_diagnostic_reserved: false,
            },
        )
        .await,
    );
    a
}

fn operation(a: &Attempt, observation: Outer) -> Operation {
    Operation::ObserveWorker {
        report: Report {
            report_id: supplied_outer::id(&a.id, observation.index(), observation.phase()),
            worker_id: a.worker_id.clone(),
            incarnation: a.worker_incarnation.clone(),
            task_id: a.task_id.clone(),
            attempt_id: a.id.clone(),
            generation: a.generation,
            observation: Observation::SuppliedOuter { observation },
        },
    }
}
async fn send(c: &Controller, op: Operation) -> Reply {
    let reply = Box::pin(c.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: format!("supplied-request-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
        command: op.clone(),
    }))
    .await;
    reply.validate_for(&op).unwrap();
    reply
}
fn ack(index: u32, turn: &str) -> Outer {
    Outer::Acknowledged {
        index,
        thread_id: THREAD.into(),
        turn_id: turn.into(),
    }
}
fn terminal(index: u32, turn: &str, status: &str, class: &str) -> Outer {
    Outer::Terminal {
        index,
        thread_id: THREAD.into(),
        turn_id: turn.into(),
        terminal: json!({"id":turn,"status":status,"error":{
            "codexErrorInfo":class,
            "message":"rate limit; wait 999 days (untrusted diagnostic, never the timer)"
        }}),
    }
}
fn waiting(index: u32, turn: &str, retry_index: u32) -> Outer {
    Outer::Waiting {
        index,
        thread_id: THREAD.into(),
        previous_turn_id: turn.into(),
        cause: Cause::RateLimit { retry_index },
    }
}
fn dispatch(index: u32, turn: &str, retry_index: u32) -> Outer {
    Outer::Dispatch {
        index,
        thread_id: THREAD.into(),
        previous_turn_id: turn.into(),
        cause: Cause::RateLimit { retry_index },
    }
}
fn accepted(reply: &Reply) {
    assert!(
        matches!(
            &reply.outcome,
            Outcome::Ok {
                data: Data::Task(_)
            }
        ),
        "{reply:?}"
    );
    assert!(reply.commit.is_some());
}
fn wait(reply: &Reply) -> Wait {
    assert!(reply.commit.is_some());
    match &reply.outcome {
        Outcome::Ok {
            data: Data::SuppliedOuterWait(value),
        } => value.clone(),
        other => panic!("expected canonical wait, got {other:?}"),
    }
}
async fn attempts(c: &Controller, a: &Attempt) -> AttemptView {
    match call(
        c,
        Operation::QueryCollaboration {
            query: Query::TaskAttempts {
                task_id: a.task_id.clone(),
                after: None,
                limit: 2,
            },
        },
    )
    .await
    {
        Outcome::Ok {
            data: Data::Collaboration(QueryResult::TaskAttempts(page)),
        } => {
            assert_eq!(
                page.items.len(),
                1,
                "continuation must not allocate another attempt"
            );
            let view = page.items.into_iter().next().unwrap();
            assert_eq!(view.attempt.id, a.id);
            assert_eq!(view.attempt.session_id, a.session_id);
            assert_eq!(view.attempt.worker_incarnation, a.worker_incarnation);
            view
        }
        other => panic!("expected actual attempt inventory: {other:?}"),
    }
}
async fn charge_is(c: &Controller, a: &Attempt, n: u32) {
    let value = attempts(c, a).await;
    assert_eq!(value.reported_model_requests, n);
    assert_eq!(
        (value.input_tokens, value.output_tokens),
        (Some(11), Some(7))
    );
    assert!(
        value.executor_outcome.is_none(),
        "outer failure is not an attempt outcome"
    );
}
async fn replay(c: &Controller, op: &Operation, original: &Reply) {
    let repeated = send(c, op.clone()).await;
    assert_eq!(repeated.outcome, original.outcome);
    assert_eq!(repeated.commit, original.commit);
    let Operation::ObserveWorker { report } = op else {
        panic!()
    };
    let lookup = send(
        c,
        Operation::GetOperation {
            operation_id: report.report_id.clone(),
        },
    )
    .await;
    assert_eq!(lookup.commit, original.commit);
    let Outcome::Ok {
        data: Data::Operation(record),
    } = lookup.outcome
    else {
        panic!("original receipt missing")
    };
    assert_eq!(record.intended, serde_json::to_value(op).unwrap());
    assert_eq!(
        serde_json::from_value::<Outcome>(record.outcome.unwrap()).unwrap(),
        original.outcome
    );
}

#[tokio::test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# Platform recovery of a supplied executor MUST NOT create another session or repeat an uncertain start merely to implement a provider retry.
//= spec/impl/v2/implementation.md#cancellation-commits-intent-before-interruption
//# Reconnecting or resuming an attempt MUST NOT reset its recorded model-request accounting.
async fn only_exact_acknowledged_rate_limit_terminal_admits_next_occurrence() {
    let p = path();
    let (cfg, clock, policy) = retry_config();
    let c = Controller::open_with_config(&p, cfg).unwrap();
    let a = setup(&c).await;
    accepted(&send(&c, operation(&a, ack(1, TURN))).await);
    accepted(
        &send(
            &c,
            operation(&a, terminal(1, TURN, "failed", "rateLimitExceeded")),
        )
        .await,
    );
    for (index, previous, next_turn) in [
        (2, TURN, "controlled-turn-2"),
        (3, "controlled-turn-2", "controlled-turn-3"),
    ] {
        let now = clock.load(Ordering::SeqCst);
        let op = operation(&a, waiting(index, previous, index - 1));
        let original = send(&c, op.clone()).await;
        let expected = Wait {
            index,
            rate_failures: index - 1,
            admitted_at_ms: now,
            not_before_ms: now
                + policy
                    .delay_ms(
                        index - 1,
                        supplied_outer::jitter_sample(THREAD, index),
                        now,
                        100_000,
                    )
                    .unwrap(),
        };
        assert_eq!(wait(&original), expected);
        replay(&c, &op, &original).await;
        charge_is(&c, &a, index - 2).await;
        clock.store(expected.not_before_ms, Ordering::SeqCst);
        let op = operation(&a, dispatch(index, previous, index - 1));
        let admitted = send(&c, op.clone()).await;
        accepted(&admitted);
        replay(&c, &op, &admitted).await;
        charge_is(&c, &a, index - 1).await;
        accepted(&send(&c, operation(&a, ack(index, next_turn))).await);
        accepted(
            &send(
                &c,
                operation(
                    &a,
                    terminal(index, next_turn, "failed", "rateLimitExceeded"),
                ),
            )
            .await,
        );
    }
    let current = get(&c, &a.task_id).await;
    assert_eq!(current.session_id, a.session_id);
    assert_eq!(current.status, TaskStatus::Active);
    assert!(current.result.is_none());
    drop(c);
    controller_owner_released(&p).await;

    // Refusals retain their original phase IDs. Each negative cut therefore
    // gets an independent real attempt; no correction replaces a refused body.
    for cut in [
        "missing_ack",
        "missing_terminal",
        "wrong_thread",
        "wrong_turn",
        "quota",
        "context",
        "auth",
        "generic",
        "interrupted",
        "early_dispatch",
        "reused_ack",
        "stale_incarnation",
    ] {
        let p = path();
        let (cfg, clock, _) = retry_config();
        let c = Controller::open_with_config(&p, cfg).unwrap();
        let a = setup(&c).await;
        let mut rejected;
        let expected;
        if cut == "missing_ack" {
            rejected = operation(&a, terminal(1, TURN, "failed", "rateLimitExceeded"));
            expected = "reconciliation_required";
        } else {
            accepted(&send(&c, operation(&a, ack(1, TURN))).await);
            if cut == "missing_terminal" {
                rejected = operation(&a, waiting(2, TURN, 1));
                expected = "reconciliation_required";
            } else if cut == "wrong_thread" || cut == "wrong_turn" {
                let mut value = terminal(1, TURN, "failed", "rateLimitExceeded");
                if let Outer::Terminal {
                    thread_id, turn_id, ..
                } = &mut value
                {
                    if cut == "wrong_thread" {
                        *thread_id = "foreign-thread".into();
                    } else {
                        *turn_id = "foreign-turn".into();
                    }
                }
                rejected = operation(&a, value);
                expected = "source_mismatch";
            } else {
                let class = match cut {
                    "quota" => "usageLimitExceeded",
                    "context" => "contextWindowExceeded",
                    "auth" => "unauthorized",
                    "generic" => "other",
                    _ => "rateLimitExceeded",
                };
                let status = if cut == "interrupted" {
                    "interrupted"
                } else {
                    "failed"
                };
                accepted(&send(&c, operation(&a, terminal(1, TURN, status, class))).await);
                if ["quota", "context", "auth", "generic", "interrupted"].contains(&cut) {
                    rejected = operation(&a, waiting(2, TURN, 1));
                    expected = "reconciliation_required";
                } else if cut == "stale_incarnation" {
                    rejected = operation(&a, waiting(2, TURN, 1));
                    if let Operation::ObserveWorker { report } = &mut rejected {
                        report.incarnation = "replacement-incarnation".into();
                    }
                    expected = "stale_incarnation";
                } else {
                    let due = wait(&send(&c, operation(&a, waiting(2, TURN, 1))).await);
                    rejected = operation(&a, dispatch(2, TURN, 1));
                    if cut == "early_dispatch" {
                        clock.store(due.not_before_ms - 1, Ordering::SeqCst);
                        expected = "invalid_transition";
                    } else {
                        clock.store(due.not_before_ms, Ordering::SeqCst);
                        accepted(&send(&c, rejected).await);
                        rejected = operation(&a, ack(2, TURN));
                        expected = "source_mismatch";
                    }
                }
            }
        }
        let before = get(&c, &a.task_id).await;
        let usage = attempts(&c, &a).await;
        let refused = send(&c, rejected.clone()).await;
        assert_eq!(code(refused.outcome.clone()), expected, "cut {cut}");
        assert!(
            refused.commit.is_some(),
            "retain original refusal at cut {cut}"
        );
        replay(&c, &rejected, &refused).await;
        assert_eq!(get(&c, &a.task_id).await, before, "cut {cut}");
        assert_eq!(attempts(&c, &a).await, usage, "cut {cut}");
        drop(c);
        controller_owner_released(&p).await;
    }
}

fn files(p: &std::path::Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut result = BTreeMap::new();
    for entry in std::fs::read_dir(p.parent().unwrap()).unwrap() {
        let entry = entry.unwrap();
        let meta = std::fs::symlink_metadata(entry.path()).unwrap();
        assert!(meta.is_file());
        assert!(meta.len() <= 64 * 1024 * 1024, "bounded private fixture");
        result.insert(
            entry.file_name().into(),
            std::fs::read(entry.path()).unwrap(),
        );
        assert!(result.len() <= 16, "bounded private fixture");
    }
    result
}
fn stamp_negative_old_marker(p: &std::path::Path) {
    let sql = rusqlite::Connection::open(p).unwrap();
    assert_eq!(
        sql.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        25
    );
    // Deliberate incompatible-marker corruption of a current producer, never
    // evidence of a schema24 executable or valid historical upgrade.
    sql.pragma_update(None, "user_version", 24).unwrap();
}
async fn refuses_old_bytes(p: &std::path::Path, cfg: ControllerConfig) {
    stamp_negative_old_marker(p);
    let before = files(p);
    for _ in 0..2 {
        let error = match Controller::open_with_config(p, cfg.clone()) {
            Ok(_) => panic!("old marker admitted new supplied vocabulary"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("supplied") && error.contains("schema25"),
            "wrong refusal: {error}"
        );
        controller_owner_released(p).await;
        assert_eq!(
            files(p),
            before,
            "failed startup must preserve database and sidecars"
        );
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//# An exact retry of a committed mutating request MUST return its recorded outcome.
//# A change to stored task state representation MUST have an explicit migration disposition.
async fn retry_receipts_reopen_exactly_and_old_vocabulary_refuses_before_replay() {
    let p = path();
    let (cfg, clock, _) = retry_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let a = setup(&c).await;
    let mut originals = Vec::new();
    for value in [
        ack(1, TURN),
        terminal(1, TURN, "failed", "rateLimitExceeded"),
        waiting(2, TURN, 1),
    ] {
        let op = operation(&a, value);
        let reply = send(&c, op.clone()).await;
        if matches!(
            &reply.outcome,
            Outcome::Ok {
                data: Data::SuppliedOuterWait(_)
            }
        ) {
            clock.store(wait(&reply).not_before_ms, Ordering::SeqCst);
        } else {
            accepted(&reply);
        }
        originals.push((op, reply));
    }
    let op = operation(&a, dispatch(2, TURN, 1));
    let admitted = send(&c, op.clone()).await;
    accepted(&admitted);
    originals.push((op, admitted));
    let before = get(&c, &a.task_id).await;
    charge_is(&c, &a, 1).await;
    drop(c);
    controller_owner_released(&p).await;
    for _ in 0..2 {
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        assert_eq!(get(&c, &a.task_id).await, before);
        for (op, reply) in &originals {
            replay(&c, op, reply).await;
        }
        charge_is(&c, &a, 1).await;
        assert_eq!(get(&c, &a.task_id).await, before);
        drop(c);
        controller_owner_released(&p).await;
    }
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let cancelled = task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "supplied-cancel".into(),
                task_id: a.task_id.clone(),
                expected_revision: before.revision,
            },
        )
        .await,
    );
    for (op, reply) in &originals {
        replay(&c, op, reply).await;
    }
    assert_eq!(
        get(&c, &a.task_id).await,
        cancelled,
        "historical grant replay cannot reopen execution"
    );
    charge_is(&c, &a, 1).await;
    drop(c);
    controller_owner_released(&p).await;
    // Corrupt only the causal association in a separate closed copy. The
    // original receipts above and the old-vocabulary negative below stay exact.
    let corrupt = path();
    for (name, bytes) in files(&p) {
        std::fs::write(corrupt.parent().unwrap().join(name), bytes).unwrap();
    }
    {
        let sql = rusqlite::Connection::open(&corrupt).unwrap();
        let id = supplied_outer::id(&a.id, 2, "dispatch");
        let raw: String = sql
            .query_row("SELECT request FROM receipts WHERE id=?1", [&id], |r| {
                r.get(0)
            })
            .unwrap();
        let mut request: Operation = serde_json::from_str(&raw).unwrap();
        let Operation::ObserveWorker { report } = &mut request else {
            panic!("report")
        };
        let Observation::SuppliedOuter {
            observation: Outer::Dispatch {
                previous_turn_id, ..
            },
        } = &mut report.observation
        else {
            panic!("dispatch")
        };
        *previous_turn_id = "corrupt-prior-turn".into();
        sql.execute(
            "UPDATE receipts SET request=?2 WHERE id=?1",
            rusqlite::params![id, serde_json::to_string(&request).unwrap()],
        )
        .unwrap();
    }
    let corrupted_bytes = files(&corrupt);
    for _ in 0..2 {
        let error = match Controller::open_with_config(&corrupt, cfg.clone()) {
            Ok(_) => panic!("corrupt dispatch admitted"),
            Err(e) => e.to_string(),
        };
        assert!(error.contains("supplied dispatch differs"), "{error}");
        controller_owner_released(&corrupt).await;
        assert_eq!(files(&corrupt), corrupted_bytes);
    }
    refuses_old_bytes(&p, cfg).await;

    // Accepted metadata alone fences old readers, without a SuppliedOuter report.
    let p = path();
    let (cfg, _, _) = retry_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    setup(&c).await;
    drop(c);
    controller_owner_released(&p).await;
    refuses_old_bytes(&p, cfg).await;

    // A real refused new report is still new stored vocabulary even when no
    // task, retry policy, successful outer fact, or charged dispatch exists.
    let p = path();
    let cfg = config();
    assert!(cfg.limits.execution.coding.rate_limit_retry.is_none());
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let op = Operation::ObserveWorker {
        report: Report {
            report_id: supplied_outer::id("absent-attempt", 1, "ack"),
            worker_id: "absent-worker".into(),
            incarnation: "absent-incarnation".into(),
            task_id: "absent-task".into(),
            attempt_id: "absent-attempt".into(),
            generation: 1,
            observation: Observation::SuppliedOuter {
                observation: ack(1, TURN),
            },
        },
    };
    let original = send(&c, op.clone()).await;
    assert!(matches!(&original.outcome, Outcome::Refused { .. }));
    assert!(original.commit.is_some());
    replay(&c, &op, &original).await;
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    replay(&c, &op, &original).await;
    drop(c);
    controller_owner_released(&p).await;
    refuses_old_bytes(&p, cfg).await;
}
