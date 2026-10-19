//! Prospective helper tests on real in-memory SQLite. No test was run in authoring.
//! These deliberately inserted typed records test read proof/guards, not a claim
//! that the runtime performed acceptance. D's controller tests cover real ingress.
use super::*;
use crate::protocol::*;
use rusqlite::Connection;

fn policy() -> crate::limits::RuntimeLimits {
    let mut p = crate::limits::RuntimeLimits::seed().unwrap();
    p.workflow = Some(WorkflowLimits {
        record_bytes: 8192, text_bytes: 2048, options: 8, records: 128, revisions: 1024,
        page_items: 4, page_bytes: 16384, waits_per_task: 8, graph_scan_nodes: 128,
        events_per_mutation: 16, event_bytes: 4096, notice_bytes: 4096,
        pending_delivery_bytes: 16384, delivery_scan_rows: 64,
    });
    p
}
fn actor() -> RecordedActor {
    RecordedActor { principal: PrincipalRef::TrustedHostOperator,
        provenance: ActorProvenance::TrustedHost { path: TrustedHostPath::OperatorApi } }
}
fn sql() -> Connection {
    let c = Connection::open_in_memory().unwrap();
    c.execute_batch(include_str!("schema12.sql")).unwrap();
    c.execute("INSERT INTO settings(name,value) VALUES('workflow_revision','0')", []).unwrap();
    c
}
fn empty_capacity_tables(c: &Connection) {
    // Real count implementation; reduced fixture rows are not a schema assertion.
    for table in HEAD_TABLES.iter().chain(IMMUTABLE_TABLES) {
        c.execute_batch(&format!("CREATE TABLE {table}(id TEXT)")).unwrap();
    }
}
fn host_context<'a>(tx: &'a StoreContext<'a>, host: &'a RecordedActor, p: &'a crate::limits::RuntimeLimits) -> Context<'a, 'a> {
    Context { tx, actor: host, operation_id: "fixture-op", product_revision: 1, now_ms: 10, limits: p.workflow.as_ref().unwrap() }
}
fn refused_code<T>(value: Result<T>) -> (&'static str, String) {
    match value { Err(Fault::Refused(code, text)) => (code, text), _ => panic!("expected refusal") }
}
fn fixture_task(tx: &StoreContext<'_>) -> database::StoredTask {
    let definition = Definition { charter: None, limit_overrides: None,
        execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
        inference: Some(InferenceSettings::selected()), id: "agent".into(), revision: "definition-1".into(), executor: ExecutorKind::Native };
    let mut input = TaskInput { instruction_source: None, pull_request: None,  resolved_limits: None, repository_read: None,
        instruction: "bounded fixture".into(), definition: definition.clone(), repository: None,
        acceptance_commands: vec![], output_schema: None,
        limits: Limits { deadline_unix_ms: 1000, model_requests: 1, output_bytes: 1024,
            result_bytes: tx.limits.task_defaults.result_bytes, max_delegation_depth: 1 },
        dependencies: vec![], native_workflow: None };
    input.resolved_limits = Some(tx.limits.resolve_task(&input).unwrap());
    tx.execute("INSERT INTO agents(id,definition) VALUES(?1,?2)", params![definition.id, encode(&definition).unwrap()]).unwrap();
    tx.execute("INSERT INTO definitions(id,revision,body) VALUES(?1,?2,?3)", params![definition.id, definition.revision, encode(&definition).unwrap()]).unwrap();
    let session = Session { id: "session".into(), agent_id: definition.id.clone(), definition,
        task_id: "task".into() };
    super::super::sessions::insert(tx, &session, 1).unwrap();
    let t = Task { id: "task".into(), session_id: "session".into(), input: input.into(),
        assigned_agent_id: "agent".into(), status: TaskStatus::Queued, revision: 1,
        attempt: None, selected_checkpoint: None, result: None, blocked_reason: None, pending_assignment: None, native_retry_block: None,
        progress: String::new(), recent_output: String::new(), output_truncated: false,
        parent_task_id: None, waiting_child_id: None, completed_child: None };
    let stored = database::StoredTask { task: t, generation: 0, attempt_counter: 0, depth: 0,
        delegated_model_requests: 0, executor_success: None, unresolved_execution: false, cleanup_remaining: 0, completion_hold: None };
    database::insert(tx, &stored, 1).unwrap();
    stored
}
fn assigned(tx: &StoreContext<'_>, t: &mut database::StoredTask, number: u64) -> Attempt {
    let a = Attempt { id: format!("attempt-{number}"), task_id: t.task.id.clone(), session_id: t.task.session_id.clone(),
        generation: number, worker_id: "worker".into(), worker_incarnation: format!("inc-{number}"),
        phase: AttemptPhase::Assigned, deadline_unix_ms: t.task.input.limits.deadline_unix_ms,
        stopped: false, uncertain: false };
    t.task.status = TaskStatus::Active; t.generation = number; t.attempt_counter = number; t.task.attempt = Some(a.clone());
    database::save(tx, t, "attempt_assigned", &a).unwrap();
    a
}
fn result(a: &Attempt) -> AcceptedResult {
    AcceptedResult { pull_request: None,  attempt_id: a.id.clone(), summary: "same bytes".into(), checkpoint: None,
        validation: vec!["controlled".into()], output: Some(serde_json::json!({"answer":42})) }
}
fn observation(r: &AcceptedResult) -> Observation {
    Observation::ResultCandidate { checks_passed: true, summary: r.summary.clone(), checkpoint: r.checkpoint.clone(),
        validation: r.validation.clone(), output: r.output.clone() }
}
fn accept_fixture(tx: &StoreContext<'_>, t: &mut database::StoredTask, r: &AcceptedResult) -> (u64, u64) {
    t.task.status = TaskStatus::Succeeded; t.task.result = Some(Box::new(r.clone())); t.executor_success = Some(true);
    database::save(tx, t, "worker_observation", &observation(r)).unwrap();
    let seq: i64 = tx.query_row("SELECT MAX(seq) FROM history", [], |r| r.get(0)).unwrap();
    (seq as u64, t.task.revision)
}

#[test]
fn product_revision_is_canonical_bounded_and_read_only() {
    let p = policy(); let c = sql(); let tx = StoreContext::new(&c, &p);
    assert_eq!(product_revision(&tx).unwrap(), 0);
    for raw in ["-1", "+1", "01", "1 ", "", "9223372036854775808", "1junk", "0\n", "1\0junk", "0\0"] {
        c.execute("UPDATE settings SET value=?1 WHERE name='workflow_revision'", [raw]).unwrap();
        assert!(product_revision(&tx).is_err(), "{raw:?}");
        assert_eq!(c.query_row("SELECT value FROM settings WHERE name='workflow_revision'", [], |r| r.get::<_, String>(0)).unwrap(), raw);
    }
    c.execute("UPDATE settings SET value='9223372036854775807' WHERE name='workflow_revision'", []).unwrap();
    assert_eq!(product_revision(&tx).unwrap(), i64::MAX as u64);
    c.execute("UPDATE settings SET value=x'31' WHERE name='workflow_revision'", []).unwrap();
    assert!(product_revision(&tx).is_err()); // A digit-looking BLOB is not canonical TEXT.
    c.execute("UPDATE settings SET value='123456789012345678901junk' WHERE name='workflow_revision'", []).unwrap();
    assert!(product_revision(&tx).is_err()); // Full length is checked, never a valid prefix.
    c.execute("DELETE FROM settings WHERE name='workflow_revision'", []).unwrap();
    assert!(product_revision(&tx).is_err());
}

#[test]
fn malformed_and_configured_identity_bounds_remain_distinct() {
    let mut p = policy(); p.transport.identity_bytes = 8;
    let c = sql(); let tx = StoreContext::new(&c, &p);
    assert_eq!(refused_code(identity(&tx, "x\n")).0, "invalid_input");
    assert_eq!(refused_code(identity(&tx, "123456789")).1, "Configured limit exceeded: transport.identity_bytes");
    let target = ReviewTarget::DocumentVersion { version: DocumentVersionRef { document_id: "doc".into(), version: 1, content_id: "123456789".into() } };
    assert_eq!(refused_code(validate_target_shape(&tx, &target)).1, "Configured limit exceeded: transport.identity_bytes");
}

#[test]
fn global_capacity_is_cross_domain_and_fail_closed() {
    let mut p = policy(); p.workflow.as_mut().unwrap().records = 2; p.workflow.as_mut().unwrap().revisions = 2;
    let c = sql(); empty_capacity_tables(&c); let tx = StoreContext::new(&c, &p); let host = actor(); let ctx = host_context(&tx, &host, &p);
    c.execute("INSERT INTO workflow_questions VALUES('q')", []).unwrap();
    c.execute("INSERT INTO workflow_documents VALUES('d')", []).unwrap();
    assert!(ensure_capacity(&ctx, 0, 0).is_ok());
    assert_eq!(refused_code(ensure_capacity(&ctx, 1, 0)).1, "Configured limit exceeded: workflow.records");
    c.execute("INSERT INTO workflow_answers VALUES('a')", []).unwrap();
    c.execute("INSERT INTO workflow_events VALUES('e')", []).unwrap();
    assert_eq!(refused_code(ensure_capacity(&ctx, 0, 1)).1, "Configured limit exceeded: workflow.revisions");
    assert!(ensure_capacity(&ctx, usize::MAX, 0).is_err());
    c.execute_batch("DROP TABLE workflow_wait_consumptions").unwrap();
    // A missing required table remains a storage failure; the current owner
    // preserves the SQLite error variant for transaction classification.
    assert!(matches!(ensure_capacity(&ctx, 0, 0), Err(Fault::Sqlite(_))));
}

#[test]
fn exact_acceptance_occurrence_survives_later_mutable_revision() {
    let p = policy(); let c = sql(); let tx = StoreContext::new(&c, &p);
    let mut t = fixture_task(&tx); let a = assigned(&tx, &mut t, 1); let r = result(&a);
    let occurrence = accept_fixture(&tx, &mut t, &r);
    t.task.attempt.as_mut().unwrap().stopped = true;
    t.task.attempt.as_mut().unwrap().phase = AttemptPhase::Stopped;
    database::save(&tx, &mut t, "worker_observation", &Observation::ProcessTreeStopped).unwrap();
    assert!(t.task.revision > occurrence.1);
    let host = actor(); let q = QueryContext { tx: &tx, actor: &host, now_ms: 9999, limits: p.workflow.as_ref().unwrap() };
    let ReviewTarget::AcceptedTaskResult { result: target } = accepted_target(&q, "task", AcceptedReviewScope::Result).unwrap() else { panic!("result target") };
    assert_eq!((target.acceptance_history_sequence, target.terminal_task_revision), occurrence);
    assert_eq!(target.attempt_id, a.id);
    assert!(accepted_target(&q, "task", AcceptedReviewScope::Checkpoint).is_err());
}

#[test]
fn repeated_candidate_bytes_from_earlier_attempt_do_not_rebase_acceptance() {
    let p = policy(); let c = sql(); let tx = StoreContext::new(&c, &p);
    let mut t = fixture_task(&tx); let a1 = assigned(&tx, &mut t, 1); let r1 = result(&a1);
    t.task.status = TaskStatus::Failed;
    database::save(&tx, &mut t, "worker_observation", &observation(&r1)).unwrap();
    // These deliberately injected attempts belong to separate logical requests,
    // each with its own terminal buffer; this helper is not retry admission.
    drop(tx);
    let tx = StoreContext::new(&c, &p);
    let a2 = assigned(&tx, &mut t, 2); let r2 = result(&a2);
    assert_eq!(encode(&observation(&r1)).unwrap(), encode(&observation(&r2)).unwrap());
    let occurrence = accept_fixture(&tx, &mut t, &r2);
    assert_eq!(accepted_occurrence(&tx, &t.task, &r2).unwrap(), occurrence);
    c.execute("UPDATE history SET body='{}' WHERE seq=?1", [occurrence.0 as i64]).unwrap();
    assert!(accepted_occurrence(&tx, &t.task, &r2).is_err());
}

#[test]
fn duplicate_acceptance_or_assignment_never_selects_arbitrary_history() {
    let p = policy(); let c = sql(); let tx = StoreContext::new(&c, &p);
    let mut t = fixture_task(&tx); let a = assigned(&tx, &mut t, 1); let r = result(&a);
    accept_fixture(&tx, &mut t, &r);
    database::history(&tx, &t.task, "worker_observation", &observation(&r)).unwrap();
    assert!(accepted_occurrence(&tx, &t.task, &r).is_err());
    c.execute("DELETE FROM history WHERE seq=(SELECT MAX(seq) FROM history)", []).unwrap();
    database::history(&tx, &t.task, "attempt_assigned", &a).unwrap();
    assert!(accepted_occurrence(&tx, &t.task, &r).is_err());
}

#[test]
fn historical_actor_retains_binding_after_stop_and_successor_attempt() {
    let p = policy(); let c = sql(); let tx = StoreContext::new(&c, &p);
    let mut t = fixture_task(&tx); let a = assigned(&tx, &mut t, 1);
    let old = RecordedActor { principal: PrincipalRef::Agent { agent_id: "agent".into() },
        provenance: ActorProvenance::BoundAgentSession { agent_id: "agent".into(), session_id: a.session_id.clone(), task_id: a.task_id.clone(),
            attempt_id: a.id.clone(), generation: a.generation, worker_id: a.worker_id.clone(), incarnation: a.worker_incarnation.clone() } };
    t.task.attempt.as_mut().unwrap().stopped = true;
    t.task.attempt.as_mut().unwrap().phase = AttemptPhase::Stopped;
    database::save(&tx, &mut t, "worker_observation", &Observation::ProcessTreeStopped).unwrap();
    let _ = assigned(&tx, &mut t, 2);
    assert!(validate_recorded_actor(&tx, &old).is_ok()); // No worker registration/currentness manufactured.
    let mut wrong = old.clone();
    if let ActorProvenance::BoundAgentSession { incarnation, .. } = &mut wrong.provenance { *incarnation = "different".into(); }
    assert!(validate_recorded_actor(&tx, &wrong).is_err());
    let mut wrong = old; wrong.principal = PrincipalRef::TrustedHostOperator;
    assert!(validate_recorded_actor(&tx, &wrong).is_err());
}

#[test]
fn task_touch_uses_coordination_revision_and_never_rewrites_accepted_task() {
    let p = policy(); let c = sql();
    c.execute_batch("CREATE TABLE workflow_task_state(task_id TEXT PRIMARY KEY, revision INTEGER NOT NULL, body TEXT NOT NULL)").unwrap();
    for table in HEAD_TABLES.iter().chain(IMMUTABLE_TABLES).filter(|t| **t != "workflow_task_state") {
        c.execute_batch(&format!("CREATE TABLE {table}(id TEXT)")).unwrap();
    }
    let tx = StoreContext::new(&c, &p); let mut t = fixture_task(&tx); let a = assigned(&tx, &mut t, 1); let r = result(&a);
    accept_fixture(&tx, &mut t, &r);
    let before: String = c.query_row("SELECT body FROM tasks WHERE id='task'", [], |r| r.get(0)).unwrap();
    let host = actor(); let ctx = host_context(&tx, &host, &p); let record = RecordRef::Task { task_id: "task".into() };
    assert_eq!(record_owner_revision(&tx, &record).unwrap().revision, 0);
    assert_eq!(stage_record_change(&ctx, &record, 0).unwrap(), 1);
    assert_eq!(refused_code(stage_record_change(&ctx, &record, 1)).0, "duplicate_record_change");
    assert_eq!(c.query_row("SELECT body FROM tasks WHERE id='task'", [], |r| r.get::<_, String>(0)).unwrap(), before);
}

#[test]
fn pending_reviews_cursor_binds_viewer_high_water_and_complete_page_bytes() {
    let p = policy(); let c = sql(); let tx = StoreContext::new(&c, &p);
    c.execute_batch("CREATE TABLE workflow_task_state(task_id TEXT PRIMARY KEY,revision INTEGER,body TEXT);
        CREATE TABLE workflow_reviews(id TEXT PRIMARY KEY,revision INTEGER,created_product_revision INTEGER,reviewer TEXT,status TEXT,body TEXT);
        CREATE TABLE workflow_review_decisions(id TEXT,review_id TEXT,body TEXT);
        CREATE TABLE workflow_review_withdrawals(review_id TEXT,body TEXT);").unwrap();
    let mut t = fixture_task(&tx); let a = assigned(&tx, &mut t, 1); let r = result(&a);
    accept_fixture(&tx, &mut t, &r);
    let host = actor(); let q = QueryContext { tx: &tx, actor: &host, now_ms: 9999, limits: p.workflow.as_ref().unwrap() };
    let target = accepted_target(&q, "task", AcceptedReviewScope::Result).unwrap();
    let add = |n: u64| {
        let review = Review { id: format!("review-{n}"), revision: 1, target: target.clone(),
            reviewer: PrincipalRef::TrustedHostOperator, summary: format!("Review {n}"), requested_by: host.clone(),
            created_product_revision: n, changed_product_revision: n, status: ReviewStatus::Open, decision_id: None };
        c.execute("INSERT INTO workflow_reviews VALUES(?1,1,?2,?3,'open',?4)",
            params![review.id, integer(n).unwrap(), encode(&review.reviewer).unwrap(), encode(&review).unwrap()]).unwrap();
        c.execute("UPDATE settings SET value=?1 WHERE name='workflow_revision'", [n.to_string()]).unwrap();
    };
    add(1); add(2); add(3);
    let first = pending_reviews(&q, &host.principal, None, 1).unwrap();
    assert_eq!(first.items[0].review.id, "review-1");
    add(4);
    let second = pending_reviews(&q, &host.principal, first.next_cursor.as_deref(), 1).unwrap();
    let third = pending_reviews(&q, &host.principal, second.next_cursor.as_deref(), 1).unwrap();
    assert_eq!(second.items[0].review.id, "review-2");
    assert_eq!(third.items[0].review.id, "review-3");
    assert!(third.next_cursor.is_none()); // Later creation is outside the retained high-water.
    assert!(pending_reviews(&q, &PrincipalRef::Agent { agent_id: "agent".into() }, first.next_cursor.as_deref(), 1).is_err());
    let mut future_cursor: ReviewCursor = decode(first.next_cursor.as_deref().unwrap()).unwrap();
    future_cursor.through = 5;
    assert!(pending_reviews(&q, &host.principal, Some(&encode(&future_cursor).unwrap()), 1).is_err());
    let mut small = q.limits.clone();
    small.page_bytes = encode(&first).unwrap().len();
    let narrow = QueryContext { tx: &tx, actor: &host, now_ms: 9999, limits: &small };
    let bounded = pending_reviews(&narrow, &host.principal, None, 4).unwrap();
    assert_eq!(bounded.items.len(), 1);
    assert!(bounded.next_cursor.is_some());
    assert!(encode(&bounded).unwrap().len() <= small.page_bytes);
}


#[test]
fn review_owner_bounds_index_before_body_decode_and_refuses_corruption_without_repair() {
    let p = policy(); let c = sql(); let tx = StoreContext::new(&c, &p);
    c.execute_batch("CREATE TABLE workflow_documents(id TEXT PRIMARY KEY,revision INTEGER,created_product_revision INTEGER,body TEXT);
        CREATE TABLE workflow_reviews(id TEXT PRIMARY KEY,revision INTEGER,created_product_revision INTEGER,reviewer TEXT,status TEXT,body TEXT);").unwrap();
    let host = actor();
    let version = DocumentVersionRef { document_id: "doc".into(), version: 1, content_id: "content".into() };
    let document = Document { id: "doc".into(), revision: 1, current: version.clone(),
        responsible_owner: host.principal.clone(), created_product_revision: 1, changed_product_revision: 1 };
    let review = Review { id: "review".into(), revision: 2,
        target: ReviewTarget::DocumentVersion { version }, reviewer: host.principal.clone(),
        summary: "Review".into(), requested_by: host.clone(), created_product_revision: 2,
        changed_product_revision: 3, status: ReviewStatus::Open, decision_id: None };
    let body = encode(&review).unwrap(); let reviewer = encode(&review.reviewer).unwrap();
    c.execute("INSERT INTO workflow_documents VALUES('doc',1,1,?1)", [encode(&document).unwrap()]).unwrap();
    c.execute("INSERT INTO workflow_reviews VALUES('review',2,2,?1,'open',?2)", params![reviewer, body]).unwrap();
    let record = RecordRef::Review { review_id: "review".into() };
    let owner = record_owner_revision(&tx, &record).unwrap();
    assert_eq!(owner.owner, host.principal);
    assert_eq!(owner.revision, 2); // The review's revision, not its target's revision.
    let bound = p.workflow.as_ref().unwrap().record_bytes;
    let oversized = "x".repeat(bound + 1);
    let bound_message = "stored review index exceeds configured workflow.record_bytes";
    let mismatch_message = "review ownership head mismatch";
    // The corrupt-body cases distinguish the early bound check from decoding
    // the canonical body first. This fixture deliberately injects invalid rows.
    for (index_reviewer, state, raw, expected) in [
        (oversized.as_str(), "open", body.as_str(), bound_message),
        (reviewer.as_str(), oversized.as_str(), body.as_str(), bound_message),
        (oversized.as_str(), "open", "{", bound_message),
        (reviewer.as_str(), oversized.as_str(), "{", bound_message),
        ("{}", "open", body.as_str(), mismatch_message),
        (reviewer.as_str(), "open\0ignored", body.as_str(), mismatch_message),
    ] {
        c.execute("UPDATE workflow_reviews SET reviewer=?1,status=?2,body=?3 WHERE id='review'",
            params![index_reviewer, state, raw]).unwrap();
        let before: (String, String, String) = c.query_row(
            "SELECT reviewer,status,body FROM workflow_reviews WHERE id='review'", [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
        let changes: i64 = c.query_row("SELECT total_changes()", [], |r| r.get(0)).unwrap();
        match record_owner_revision(&tx, &record) {
            Err(Fault::Store(message)) => assert_eq!(message, expected),
            _ => panic!("expected exact stored review refusal"),
        }
        let after: (String, String, String) = c.query_row(
            "SELECT reviewer,status,body FROM workflow_reviews WHERE id='review'", [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
        assert_eq!(after, before);
        assert_eq!(c.query_row("SELECT total_changes()", [], |r| r.get::<_, i64>(0)).unwrap(), changes);
    }
}


#[test]
fn changed_product_revision_uses_requested_head_and_actual_work_header() {
    use crate::collaboration::questions::{PoseQuestion, Question, QuestionOption, QuestionStatus};
    let p = policy(); let c = sql(); let tx = StoreContext::new(&c, &p); let host = actor();
    c.execute_batch("CREATE TABLE workflow_task_state(task_id TEXT PRIMARY KEY,revision INTEGER,body TEXT);
        CREATE TABLE workflow_documents(id TEXT PRIMARY KEY,revision INTEGER,created_product_revision INTEGER,body TEXT);
        CREATE TABLE workflow_reviews(id TEXT PRIMARY KEY,revision INTEGER,created_product_revision INTEGER,reviewer TEXT,status TEXT,body TEXT);
        CREATE TABLE workflow_questions(id TEXT PRIMARY KEY,revision INTEGER,created_product_revision INTEGER,recipient TEXT,status TEXT,record_kind TEXT,record_id TEXT,body TEXT);").unwrap();
    for table in HEAD_TABLES.iter().chain(IMMUTABLE_TABLES).filter(|t| ![
        "workflow_task_state", "workflow_documents", "workflow_reviews", "workflow_questions"].contains(&**t)) {
        c.execute_batch(&format!("CREATE TABLE {table}(id TEXT)")).unwrap();
    }
    let _ = fixture_task(&tx);
    let task_ref = RecordRef::Task { task_id: "task".into() };
    assert_eq!(record_changed_product_revision(&tx, &task_ref).unwrap(), 0);
    // The requested pending revision is exactly one beyond the committed head.
    c.execute("UPDATE settings SET value='6' WHERE name='workflow_revision'", []).unwrap();
    let mut ctx = host_context(&tx, &host, &p); ctx.product_revision = 7;
    assert_eq!(stage_record_change(&ctx, &task_ref, 0).unwrap(), 1);
    assert_eq!(record_owner_revision(&tx, &task_ref).unwrap().revision, 1);
    assert_eq!(record_changed_product_revision(&tx, &task_ref).unwrap(), 7);

    let version = DocumentVersionRef { document_id: "doc".into(), version: 1, content_id: "content".into() };
    let document = Document { id: "doc".into(), revision: 2, current: version.clone(),
        responsible_owner: host.principal.clone(), created_product_revision: 1, changed_product_revision: 4 };
    let review = Review { id: "review".into(), revision: 3,
        target: ReviewTarget::DocumentVersion { version }, reviewer: host.principal.clone(),
        summary: "Review".into(), requested_by: host.clone(), created_product_revision: 2,
        changed_product_revision: 9, status: ReviewStatus::Open, decision_id: None };
    let mut question = Question { id: "question".into(), revision: 5,
        created_product_revision: 3, changed_product_revision: 12,
        posed: PoseQuestion { record: RecordRef::Review { review_id: "review".into() }, recipient: host.principal.clone(),
            prompt: "Choose. Recommended option: yes".into(), recommended_option_id: "yes".into(),
            options: vec![QuestionOption { id: "yes".into(), label: "Yes".into(), explanation: "Proceed".into() },
                QuestionOption { id: "no".into(), label: "No".into(), explanation: "Stop".into() }] },
        poser: host.clone(), status: QuestionStatus::Open, latest_answer_id: None, active_answer_id: None,
        superseded_by: None, supersedes: None };
    c.execute("INSERT INTO workflow_documents VALUES('doc',2,1,?1)", [encode(&document).unwrap()]).unwrap();
    c.execute("INSERT INTO workflow_reviews VALUES('review',3,2,?1,'open',?2)",
        params![encode(&review.reviewer).unwrap(), encode(&review).unwrap()]).unwrap();
    c.execute("INSERT INTO workflow_questions VALUES('question',5,3,?1,'open','review','review',?2)",
        params![encode(&question.posed.recipient).unwrap(), encode(&question).unwrap()]).unwrap();
    let changes: i64 = c.query_row("SELECT total_changes()", [], |r| r.get(0)).unwrap();
    for (record, revision, changed) in [
        (RecordRef::Document { document_id: "doc".into() }, 2, 4),
        (RecordRef::Review { review_id: "review".into() }, 3, 9),
        (RecordRef::Question { question_id: "question".into() }, 5, 12),
    ] {
        let owner = record_owner_revision(&tx, &record).unwrap();
        assert_eq!(owner.owner, host.principal);
        assert_eq!(owner.revision, revision);
        assert_eq!(record_changed_product_revision(&tx, &record).unwrap(), changed);
    }
    assert_eq!(c.query_row("SELECT total_changes()", [], |r| r.get::<_, i64>(0)).unwrap(), changes);
    // A field in a corrupt canonical head is not accepted as an independent index.
    question.changed_product_revision = 2;
    c.execute("UPDATE workflow_questions SET body=?1 WHERE id='question'", [encode(&question).unwrap()]).unwrap();
    assert!(matches!(record_changed_product_revision(&tx, &RecordRef::Question { question_id: "question".into() }), Err(Fault::Store(_))));
    assert!(record_changed_product_revision(&tx, &RecordRef::Review { review_id: "missing".into() }).is_err());
}
