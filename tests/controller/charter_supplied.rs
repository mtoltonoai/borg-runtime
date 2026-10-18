//! Actual Controller/receipt tests for initial outer-context delivery. Scripted
//! executor I/O belongs to the consumer suite; this file fabricates no model turn.
use super::*;

async fn send(c: &Controller, command: Operation) -> Reply {
    let reply = Box::pin(c.handle(Request { protocol_revision: WIRE_REVISION,
        request_id: "supplied-charter-test".into(), command: command.clone() })).await;
    reply.validate_for(&command).unwrap(); reply
}
async fn coding_setup(c: &Controller, label: &str) -> (Task, Attempt, Caller) {
    let mut input = coding(label);
    input.definition.charter = Some(DefinitionCharter::Inline { source: source(label) });
    let t = submit(c, &format!("{label}-submit"), input).await;
    let w = worker(&format!("{label}-worker"), "one", 0, 1);
    register(c, &w).await;
    let a = run(&commands(c, &w, vec![]).await);
    assert_eq!(a.phase, AttemptPhase::Assigned);
    let caller = Caller { task_id: t.id.clone(), attempt_id: a.id.clone(), generation: a.generation,
        worker_id: a.worker_id.clone(), incarnation: a.worker_incarnation.clone() };
    (t, a, caller)
}
async fn intent(c: &Controller, caller: &Caller, id: &str) -> Operation {
    let before = session(c, &get(c, &caller.task_id).await.session_id).await;
    let source = before.session.pending.clone().unwrap_or_else(|| before.session.effective.clone());
    let CharterQueryResult::Source(resolved) = query(c, CharterQuery::Source { source: source.clone() }).await else { panic!() };
    Operation::ObserveCharter { operation_id: id.into(), caller: caller.clone(),
        observation: CharterObservation::BeginSuppliedContext { session_id: before.session.session_id,
            expected_revision: before.session.revision, source, request_id: 2,
            developer_instructions: supplied_context_instructions(&resolved).unwrap() } }
}
fn delivery(reply: &Reply) -> CharterSuppliedContext {
    match outcome(reply) { CharterOutcome::SuppliedContext(v) => v, other => panic!("{other:?}") }
}
async fn read(c: &Controller, id: &str) -> CharterSuppliedContext {
    match query(c, CharterQuery::SuppliedContext { delivery_id: id.into() }).await {
        CharterQueryResult::SuppliedContext(v) => v, other => panic!("{other:?}")
    }
}
fn ack(caller: &Caller, id: &str, delivery_id: &str) -> Operation {
    Operation::ObserveCharter { operation_id: id.into(), caller: caller.clone(), observation:
        CharterObservation::AckSuppliedContext { delivery_id: delivery_id.into(), request_id: 2,
            response: json!({"thread":{"id":"actual-scripted-thread","items":[]},
                "model":SELECTED_MODEL,"reasoningEffort":SELECTED_REASONING_EFFORT,"cwd":"/controlled"}) } }
}

#[tokio::test]
async fn supplied_initial_intent_ack_replay_and_newer_pending_are_distinct_from_model_turns() {
    let p = path(); let cfg = charter_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let (t, _, caller) = coding_setup(&c, "supplied-initial").await;
    let accepted = get(&c, &t.id).await;
    let target = other_source(&c, "supplied-pending-before").await;
    let before = session(&c, &t.session_id).await;
    outcome(&set(&c, "supplied-set-before", &before.session, &target).await);
    let operation = intent(&c, &caller, "supplied-original").await;
    let committed = send(&c, operation.clone()).await;
    let begun = delivery(&committed);
    assert_eq!(begun.effective.reference, target);
    assert!(begun.acknowledgment.is_none());
    assert_eq!(send(&c, operation.clone()).await.commit, committed.commit);
    let newer = other_source(&c, "supplied-pending-after").await;
    let before = session(&c, &t.session_id).await;
    outcome(&set(&c, "supplied-set-after", &before.session, &newer).await);
    let binding = session(&c, &t.session_id).await;
    let operation_ack = ack(&caller, "supplied-original-ack", &begun.delivery_id);
    let accepted_ack = send(&c, operation_ack.clone()).await;
    let complete = delivery(&accepted_ack);
    assert_eq!(complete.acknowledgment.as_ref().unwrap().thread_id, "actual-scripted-thread");
    assert_eq!(complete.effective, begun.effective);
    assert_eq!(session(&c, &t.session_id).await, binding);
    assert_eq!(binding.session.pending, Some(newer));
    assert_eq!(get(&c, &t.id).await, accepted);
    assert!(matches!(query(&c, CharterQuery::Turns { session_id: t.session_id.clone(), after: None, limit: 4 }).await,
        CharterQueryResult::Turns(page) if page.items.is_empty()));
    assert_eq!(code(call(&c, Operation::GetOperation { operation_id: ids::reservation_id(&caller.attempt_id, 1) }).await), "not_found");
    drop(c); controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg).unwrap();
    assert_eq!(read(&c, &begun.delivery_id).await, complete);
    assert_eq!(send(&c, operation_ack).await.commit, accepted_ack.commit);
    assert_eq!(send(&c, operation).await.commit, committed.commit);
    assert_eq!(get(&c, &t.id).await, accepted);
}

#[tokio::test]
async fn supplied_unknown_start_cannot_be_replaced_and_late_ack_does_not_regrant_work() {
    let p = path(); let cfg = charter_config(); let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let (t, _, caller) = coding_setup(&c, "supplied-unknown").await;
    let operation = intent(&c, &caller, "supplied-unknown-original").await;
    let original = send(&c, operation.clone()).await;
    let initial = delivery(&original);
    drop(c); controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg).unwrap();
    assert_eq!(read(&c, &initial.delivery_id).await, initial);
    assert_eq!(send(&c, operation).await.commit, original.commit);
    let another = intent(&c, &caller, "supplied-unknown-another").await;
    assert_eq!(code(send(&c, another).await.outcome), "reconciliation_required");
    let revision = get(&c, &t.id).await.revision;
    task(call(&c, Operation::CancelTask { operation_id: "supplied-cancel".into(), task_id: t.id.clone(), expected_revision: revision }).await);
    let cancelled = get(&c, &t.id).await;
    let binding = session(&c, &t.session_id).await;
    assert!(delivery(&send(&c, ack(&caller, "supplied-late-ack", &initial.delivery_id)).await).acknowledgment.is_some());
    assert_eq!(get(&c, &t.id).await, cancelled);
    assert_eq!(session(&c, &t.session_id).await, binding);
    let another = intent(&c, &caller, "supplied-after-cancel").await;
    assert_eq!(code(send(&c, another).await.outcome), "invalid_transition");
}

#[tokio::test]
async fn supplied_source_caller_frame_and_actual_ack_identity_are_fenced() {
    let c = Controller::open_with_config(path(), charter_config()).unwrap();
    let (t, _, caller) = coding_setup(&c, "supplied-fences").await;
    let original = intent(&c, &caller, "supplied-fences-original").await;
    let before = session(&c, &t.session_id).await;
    for defect in 0..5 {
        let mut operation = original.clone();
        let Operation::ObserveCharter { operation_id, caller, observation: CharterObservation::BeginSuppliedContext { expected_revision, source, developer_instructions, .. } } = &mut operation else { panic!() };
        *operation_id = format!("supplied-bad-{defect}");
        match defect {
            0 => *expected_revision += 1,
            1 => source.definition_revision = "wrong-source".into(),
            2 => developer_instructions.push_str("changed"),
            3 => caller.incarnation = "foreign-incarnation".into(),
            _ => *developer_instructions = "x".repeat(c.limits().execution.coding.frame_bytes + 1),
        }
        let refused = send(&c, operation).await.outcome;
        if defect < 4 {
            assert_eq!(code(refused), ["stale_revision", "source_mismatch", "source_mismatch", "stale_owner"][defect]);
        } else { assert!(matches!(refused, Outcome::Refused { .. })); }
        assert_eq!(session(&c, &t.session_id).await, before);
    }
    let begun = delivery(&send(&c, original.clone()).await);
    let mut changed = original;
    if let Operation::ObserveCharter { observation: CharterObservation::BeginSuppliedContext { request_id, .. }, .. } = &mut changed { *request_id = 3; }
    assert!(matches!(send(&c, changed).await.outcome, Outcome::Refused { .. }));
    for defect in 0..4 {
        let mut operation = ack(&caller, &format!("supplied-ack-bad-{defect}"), &begun.delivery_id);
        let Operation::ObserveCharter { caller, observation: CharterObservation::AckSuppliedContext { request_id, response, .. }, .. } = &mut operation else { panic!() };
        match defect { 0 => *request_id = 3, 1 => response["thread"]["id"] = json!(null),
            2 => response["model"] = json!("different"), _ => caller.incarnation = "foreign".into() }
        assert!(matches!(send(&c, operation).await.outcome, Outcome::Refused { .. }));
        assert_eq!(read(&c, &begun.delivery_id).await, begun);
    }
}

#[tokio::test]
async fn supplied_initial_gate_rejects_native_and_already_started_coding_without_a_turn() {
    let c = Controller::open_with_config(path(), charter_config()).unwrap();
    let (native_task, _, native_caller) = setup(&c, "supplied-native-refusal").await;
    let before = session(&c, &native_task.session_id).await;
    let operation = intent(&c, &native_caller, "supplied-native-attempt").await;
    assert_eq!(code(send(&c, operation).await.outcome), "unsupported_charter_boundary");
    assert_eq!(session(&c, &native_task.session_id).await, before);
    let (t, a, caller) = coding_setup(&c, "supplied-started-refusal").await;
    let before = session(&c, &t.session_id).await;
    task(report(&c, &a, "supplied-already-started", Observation::Started {
        thread_id: Some("prior-actual-thread".into()), turn_id: Some("prior-actual-turn".into()) }).await);
    let operation = intent(&c, &caller, "supplied-after-started").await;
    assert_eq!(code(send(&c, operation).await.outcome), "invalid_transition");
    assert_eq!(session(&c, &t.session_id).await, before);
    assert_eq!(code(call(&c, Operation::QueryCharter { query: CharterQuery::SuppliedContext {
        delivery_id: "supplied-after-started".into() } }).await), "not_found");
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn supplied_failed_commit_leaves_no_intent_and_original_id_remains_usable() {
    let p = path(); let mut cfg = charter_config(); let hook = Arc::new(std::sync::atomic::AtomicBool::new(false));
    cfg.qualification_fail_next_commit = Some(hook.clone());
    let c = Controller::open_with_config(&p, cfg).unwrap(); let (t, _, caller) = coding_setup(&c, "supplied-rollback").await;
    let operation = intent(&c, &caller, "supplied-rollback-original").await;
    let before = session(&c, &t.session_id).await;
    hook.store(true, Ordering::SeqCst);
    let result = send(&c, operation.clone()).await;
    assert_eq!(code(result.outcome), "store_unavailable"); assert!(result.commit.is_none());
    assert!(!hook.load(Ordering::SeqCst)); assert_eq!(session(&c, &t.session_id).await, before);
    assert_eq!(code(call(&c, Operation::QueryCharter { query: CharterQuery::SuppliedContext { delivery_id: "supplied-rollback-original".into() } }).await), "not_found");
    let result = send(&c, operation.clone()).await; delivery(&result);
    assert_eq!(send(&c, operation).await.commit, result.commit);
}

#[tokio::test]
async fn supplied_missing_or_changed_ack_and_intent_rows_refuse_reopen_without_repair() {
    for defect in 0..3 {
        let p = path(); let cfg = charter_config(); let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        let (_, _, caller) = coding_setup(&c, "supplied-corrupt").await;
        let operation = intent(&c, &caller, "supplied-stored").await;
        delivery(&send(&c, operation).await);
        let exact = delivery(&send(&c, ack(&caller, "supplied-stored-ack", "supplied-stored")).await);
        drop(c); controller_owner_released(&p).await;
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap(); assert_eq!(read(&c, "supplied-stored").await, exact);
        drop(c); controller_owner_released(&p).await;
        let sql = rusqlite::Connection::open(&p).unwrap();
        match defect {
            0 => { sql.execute("DELETE FROM charter_closures WHERE request_id='supplied-stored'", []).unwrap(); }
            1 => { let mut a = exact.acknowledgment.clone().unwrap(); a.thread_id = "changed-thread".into(); a.response["thread"]["id"] = json!("changed-thread");
                sql.execute("UPDATE charter_closures SET body=?1 WHERE request_id='supplied-stored'", [serde_json::to_string(&a).unwrap()]).unwrap(); }
            _ => { sql.execute("DELETE FROM charter_requests WHERE id='supplied-stored'", []).unwrap(); }
        }
        drop(sql); let before = stored_bytes(&p);
        assert!(Controller::open_with_config(&p, cfg).is_err(), "defect {defect}");
        assert_eq!(stored_bytes(&p), before, "startup rewrote defect {defect}");
    }
}

#[path="charter_supplied_turns.rs"]
mod outer_turns;
