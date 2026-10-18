//! Task-scoped reads of actual retained completion receipts. Controlled worker
//! reports establish canonical stop; no worker process or provider is claimed.
use super::*;

fn read_operation(task_id: &str) -> Operation {
    Operation::QueryGovernance { query: GovernanceQuery::TaskCompletion { task_id: task_id.into() } }
}
fn held_record(value: &Reply, expected_task: &str) -> Option<DecisionRecord> {
    assert!(value.commit.is_none(), "a locator is not a mutation receipt");
    match &value.outcome {
        Outcome::Ok { data: Data::Governance(result) } => match result.as_ref() {
            GovernanceResult::TaskCompletion { task_id, decision } if task_id == expected_task =>
                decision.as_deref().cloned(),
            other => panic!("task-scoped projection expected: {other:?}"),
        },
        other => panic!("task-scoped read expected: {other:?}"),
    }
}

struct HeldFixture {
    controller: Controller, cfg: ControllerConfig, task: Task, empty: Task,
    original_operation: Operation, original_reply: Reply, record: DecisionRecord,
    stop_operation: Operation, stop_reply: Reply,
}
async fn held_fixture(p: &std::path::Path) -> HeldFixture {
    let mut cfg = governance_config();
    // Existing admitted policy: zero continuations exhausts on the first block.
    // It creates the real physical-stop transition without synthetic hold rows.
    Arc::make_mut(&mut cfg.limits).governance.as_mut().unwrap().completion_continuations = 0;
    cfg.limits.validate().unwrap();
    let controller = Controller::open_with_config(p, cfg.clone()).unwrap();
    // Use the existing real registration, shadow evidence and promotion path;
    // bootstrap deliberately observes completion without enforcing a hold.
    promote(&controller, "locator-policy", true).await;
    let mut input = coding(&id("locator-codex"));
    input.definition.execution_profile = Some(ExecutionProfile::CollaborationV2 { web_search: None });
    let accepted = submit(&controller, &id("locator-submit"), input).await;
    let worker = worker(&id("locator-worker"), "inc-1", 0, 1);
    register(&controller, &worker).await;
    let a = run(&commands(&controller, &worker, vec![]).await);
    assert_eq!(a.task_id, accepted.id);
    let thread_id = id("locator-thread"); let turn_id = id("locator-turn");
    task(report(&controller, &a, &id("locator-started"), Observation::Started {
        thread_id: Some(thread_id.clone()), turn_id: Some(turn_id.clone()),
    }).await);
    let actor = caller(&a);
    let question = pose(&controller, &actor).await;
    let event = close_question(&controller, &question, Some(&actor)).await;
    let boundary = CompletionBoundary::CodexTurn { thread_id, turn_id };
    let original_operation = Operation::EvaluateCompletion(EvaluateCompletion {
        operation_id: evaluation_id(&actor, &boundary).unwrap(), caller: actor,
        boundary, intervening_requests: vec![],
    });
    let original_reply = reply(&controller, original_operation.clone()).await;
    assert_eq!(code(original_reply.outcome.clone()), "governance_completion_blocked");
    let record = audit(&controller, &original_operation).await;
    let state = completion_state(&record);
    assert_eq!(state.consecutive_count, 1);
    assert_eq!(state.disposition, CompletionDisposition::Blocked { continuation_eligible: false, exhausted: true });
    assert!(has_input(&state, &event.id));
    assert_eq!(held_record(&reply(&controller, read_operation(&accepted.id)).await, &accepted.id), Some(record.clone()));
    let stop_operation = completion_report(&a, id("locator-stopped"), Observation::ProcessTreeStopped);
    let stop_reply = reply(&controller, stop_operation.clone()).await;
    let stopped = task(stop_reply.outcome.clone());
    assert_eq!(stopped.status, TaskStatus::Queued);
    assert_eq!(stopped.blocked_reason.as_deref(), Some("governance_completion_blocked"));
    let attempt = stopped.attempt.as_ref().unwrap();
    assert!(attempt.stopped && !attempt.uncertain);
    assert_eq!(attempt.phase, AttemptPhase::Stopped);
    no_completion_resume(&controller, &worker, &accepted.id, &a.id).await;
    // The same locator must distinguish an actual task with no hold.
    let empty = submit(&controller, &id("locator-empty"), coding(&id("locator-empty-agent"))).await;
    HeldFixture { controller, cfg, task: stopped, empty, original_operation, original_reply, record,
        stop_operation, stop_reply }
}

#[tokio::test]
async fn task_completion_locator_survives_generic_stop_and_restart_without_reinterpreting_original() {
    let p = path(); let f = Box::pin(held_fixture(&p)).await;
    let before = reply(&f.controller, read_operation(&f.task.id)).await;
    assert_eq!(held_record(&before, &f.task.id), Some(f.record.clone()));
    let none = reply(&f.controller, read_operation(&f.empty.id)).await;
    assert_eq!(held_record(&none, &f.empty.id), None);
    assert_eq!(none.observed_controller_revision, before.observed_controller_revision);
    let absent = reply(&f.controller, read_operation("missing-task")).await;
    assert_eq!(code(absent.outcome), "not_found");
    let invalid = reply(&f.controller, read_operation("")).await;
    assert_eq!(code(invalid.outcome), "invalid_input");
    // Browser provenance is crate-private and remains covered by the existing
    // product governance boundary tests; this external case uses OperatorApi.
    assert_eq!(get(&f.controller, &f.task.id).await, f.task);
    drop(f.controller); controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, f.cfg).unwrap();
    let after = reply(&c, read_operation(&f.task.id)).await;
    assert_eq!(after.outcome, before.outcome);
    assert_eq!(get(&c, &f.task.id).await, f.task);
    assert_eq!(held_record(&reply(&c, read_operation(&f.empty.id)).await, &f.empty.id), None);
    same_original(&reply(&c, f.original_operation).await, &f.original_reply);
    same_original(&reply(&c, f.stop_operation).await, &f.stop_reply);
    drop(c); controller_owner_released(&p).await;
}

#[tokio::test]
async fn task_completion_reply_is_bound_to_query_kind_task_and_completion_subject() {
    let p = path(); let f = Box::pin(held_fixture(&p)).await;
    let op = read_operation(&f.task.id);
    let actual = reply(&f.controller, op.clone()).await;
    for wrong in [read_operation(&f.empty.id), Operation::QueryGovernance { query: GovernanceQuery::State },
        Operation::QueryGovernance { query: GovernanceQuery::Decision { decision: f.record.id.clone() } },
        Operation::QueryGovernance { query: GovernanceQuery::Rule { rule: f.record.rule.clone() } },
        Operation::QueryGovernance { query: GovernanceQuery::Configuration { revision: f.record.configuration.governance_revision.clone() } }] {
        assert!(actual.validate_for(&wrong).is_err());
    }
    for change in [0, 1] {
        let mut record = f.record.clone();
        if change == 0 { record.id.point = DecisionPoint::EmergencyControl; }
        else if let DecisionDetail::Completion(detail) = &mut record.detail { detail.subject.task_id = f.empty.id.clone(); }
        let mut bad = actual.clone();
        bad.outcome = Outcome::Ok { data: Data::Governance(Box::new(GovernanceResult::TaskCompletion {
            task_id: f.task.id.clone(), decision: Some(Box::new(record)),
        })) };
        assert!(bad.validate_for(&op).is_err());
    }
    let legacy = reply(&f.controller, Operation::QueryGovernance { query: GovernanceQuery::Decision { decision: f.record.id.clone() } }).await;
    assert!(legacy.validate_for(&op).is_err(), "legacy response cannot pretend to implement the locator");
    assert_eq!(WIRE_REVISION, 21);
    for raw in [r#"{"kind":"state"}"#, r#"{"kind":"decision","decision":{"operation_id":"original","point":"completion"}}"#,
        r#"{"kind":"rule","rule":{"id":"rule","revision":"1"}}"#, r#"{"kind":"configuration","revision":"v1"}"#] {
        let old: GovernanceQuery = serde_json::from_str(raw).unwrap();
        assert_eq!(serde_json::to_string(&old).unwrap(), raw);
    }
    let raw = format!(r#"{{"kind":"task_completion","task_id":"{}","decision_id":"untrusted"}}"#, f.task.id);
    assert!(serde_json::from_str::<GovernanceQuery>(&raw).is_err());
    drop(f.controller); controller_owner_released(&p).await;
}

#[tokio::test]
async fn corrupt_retained_completion_cannot_restart_as_an_empty_task_decision() {
    let p = path(); let f = Box::pin(held_fixture(&p)).await;
    drop(f.controller); controller_owner_released(&p).await;
    let sql = rusqlite::Connection::open(&p).unwrap();
    assert_eq!(sql.execute("UPDATE receipts SET governance=NULL WHERE id=?1", [&f.record.id.operation_id]).unwrap(), 1);
    drop(sql);
    assert!(Controller::open_with_config(&p, f.cfg).is_err(), "a missing referenced decision must not become valid absence");
}

#[tokio::test]
#[ignore = "explicit fresh artifact directory for the supervised production browser cohort"]
async fn seed_canonical_stopped_governance_for_browser() {
    let dir = PathBuf::from(std::env::var_os("BORG_GOVERNANCE_TASK_FIXTURE").expect("explicit fixture output required"));
    std::fs::create_dir(&dir).expect("fixture output must be fresh");
    let p = dir.join("tasks.sqlite"); let f = Box::pin(held_fixture(&p)).await;
    let view = reply(&f.controller, read_operation(&f.task.id)).await;
    assert_eq!(held_record(&view, &f.task.id), Some(f.record.clone()));
    let snapshot = json!({"scope":"real Controller mutations and controlled reports, no worker/provider execution",
        "task":f.task,"empty_task":f.empty,"decision":f.record,"locator_reply":view,
        "evaluation_operation":f.original_operation,"evaluation_reply":f.original_reply,
        "stop_operation":f.stop_operation,"stop_reply":f.stop_reply});
    std::fs::write(dir.join("limits.json"), serde_json::to_vec_pretty(f.cfg.limits.as_ref()).unwrap()).unwrap();
    drop(f.controller); controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, f.cfg).unwrap();
    assert_eq!(held_record(&reply(&c, read_operation(snapshot["task"]["id"].as_str().unwrap())).await,
        snapshot["task"]["id"].as_str().unwrap()).unwrap(), serde_json::from_value(snapshot["decision"].clone()).unwrap());
    drop(c); controller_owner_released(&p).await;
    std::fs::write(dir.join("observed.json"), serde_json::to_vec_pretty(&snapshot).unwrap()).unwrap();
}
