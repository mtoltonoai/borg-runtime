//! Joined product-route fixture against the actual controller and durable store.
//! Uses parent Fixture/ui/form helpers; no browser engine, provider or host is simulated.
//! Source-only packet: this test has not been compiled or executed by its author.
use super::*;
use borg_runtime::collaboration::{documents::*, questions::*, work::*, workflow::*};
use borg_runtime::limits::WorkflowLimits;
use serde_json::Value;

fn joined_config() -> ControllerConfig {
    let mut cfg = config();
    let limits = std::sync::Arc::make_mut(&mut cfg.limits);
    limits.workflow = Some(WorkflowLimits {
        record_bytes: 262_144.min(limits.transport.body_bytes), text_bytes: 4096,
        options: 8, records: 256, revisions: 4096, page_items: 4,
        page_bytes: 262_144.min(limits.controller.collaboration_page_bytes),
        waits_per_task: 16, graph_scan_nodes: 256, events_per_mutation: 64,
        event_bytes: 16_384, notice_bytes: 16_384,
        pending_delivery_bytes: 65_536.min(limits.transport.body_bytes), delivery_scan_rows: 256,
    });
    limits.validate().unwrap();
    cfg
}

async fn joined_fixture() -> Fixture {
    let root = std::env::temp_dir().join(format!("borg-product-joined-{}-{}-{}",
        std::process::id(), now(), NEXT.fetch_add(1, Ordering::Relaxed)));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("tasks.sqlite");
    let controller = Controller::open_with_config(&path, joined_config()).unwrap();
    Fixture { controller, path }
}

// Parse only this product's rendered form framing. Values are never re-created
// from assumed revisions or treated as authority outside the real POST handler.
fn unescape(value: &str) -> String {
    value.replace("&quot;", "\"").replace("&#39;", "'")
        .replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
}
fn rendered_form<'a>(page: &'a UiResponse, action: &str) -> &'a str {
    assert_eq!(page.status, 200, "{}", page.body);
    page.body.split(&format!("<form method=\"post\" action=\"{action}\">"))
        .nth(1).expect("real action form").split("</form>").next().unwrap()
}
fn hidden_value(rendered: &str, name: &str) -> String {
    unescape(rendered.split(&format!("<input type=\"hidden\" name=\"{name}\" value=\""))
        .nth(1).expect("rendered hidden field").split('"').next().unwrap())
}
fn mutation_fields(page: &UiResponse, action: &str) -> Vec<(&'static str, String)> {
    let rendered = rendered_form(page, action);
    vec![("operation_id", hidden_value(rendered, "operation_id")),
        ("expected_revision", hidden_value(rendered, "expected_revision"))]
}
fn work_receipt(page: &UiResponse) -> WorkMutationOutcome {
    assert_eq!(page.status, 200, "{}", page.body);
    assert!(page.location.is_none());
    assert!(page.body.contains("data-live-refresh=\"false\""));
    assert!(!page.body.contains("method=\"post\""));
    let encoded = page.body.split("<pre id=\"coordination-receipt\">").nth(1)
        .expect("original coordination receipt").split("</pre>").next().unwrap();
    serde_json::from_str(&unescape(encoded)).unwrap()
}
async fn work(c: &Controller, id: &str) -> WorkView {
    let Data::WorkQuery(WorkQueryResult::Task(view)) = call(c,
        Operation::QueryWork { query: WorkQuery::Get { task_id: id.into() } }).await
        else { panic!("work view expected") };
    view
}
async fn document(c: &Controller, id: &str, version: Option<u64>) -> DocumentView {
    let Data::DocumentQuery(DocumentQueryResult::Document(view)) = call(c,
        Operation::QueryDocument { query: DocumentQuery::Get { document_id: id.into(), version } }).await
        else { panic!("document view expected") };
    view
}
async fn question(c: &Controller, id: &str) -> QuestionView {
    let Data::QuestionQuery(result) = call(c,
        Operation::QueryQuestion { query: QuestionQuery::Get { question_id: id.into() } }).await
        else { panic!("question view expected") };
    let QuestionQueryResult::Question(view) = *result else { panic!("question view expected") };
    view
}
async fn review(c: &Controller, id: &str) -> ReviewView {
    let Data::ReviewQuery(result) = call(c,
        Operation::QueryReview { query: ReviewQuery::Get { review_id: id.into() } }).await
        else { panic!("review view expected") };
    let ReviewQueryResult::Review(view) = *result else { panic!("review view expected") };
    view
}
async fn original_receipt(c: &Controller, id: &str) -> (Reply, Outcome) {
    let reply = Box::pin(c.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: format!("joined-receipt-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
        command: Operation::GetOperation { operation_id: id.into() },
    })).await;
    assert_eq!(reply.commit.as_ref().expect("original commit").operation_id, id);
    let Outcome::Ok { data: Data::Operation(record) } = &reply.outcome else { panic!("original operation expected: {reply:?}") };
    assert_eq!(record.id, id);
    let outcome = serde_json::from_value(record.outcome.clone().expect("retained original outcome")).unwrap();
    (reply, outcome)
}
async fn activities(c: &Controller) -> Vec<WorkflowActivity> {
    let mut all = Vec::new(); let mut cursor = None;
    for _ in 0..16 {
        let Data::WorkflowActivity(page) = call(c, Operation::QueryWorkflowActivity { cursor, limit: 4 }).await
            else { panic!("workflow activity expected") };
        all.extend(page.items); cursor = page.next_cursor;
        if cursor.is_none() { return all; }
    }
    panic!("joined fixture exceeded its explicit activity-page bound")
}
async fn browser_activities(c: &Controller) -> Vec<Value> {
    let mut all = Vec::new(); let mut cursor = None::<String>;
    for _ in 0..16 {
        let path = cursor.as_ref().map(|value|format!("/ui/workflow-activity?cursor={}", encode(value)))
            .unwrap_or_else(||"/ui/workflow-activity".into());
        let page = ui(c, "GET", &path, vec![]).await;
        assert_eq!(page.status, 200, "{}", page.body);
        let value: Value = serde_json::from_str(&page.body).unwrap();
        all.extend(value["items"].as_array().expect("browser activity items").iter().cloned());
        if value["has_more"] == false { return all; }
        let next = value["cursor"].as_str().expect("opaque continuation cursor").to_owned();
        assert_ne!(cursor.as_ref(), Some(&next), "activity cursor must advance");
        cursor = Some(next);
    }
    panic!("joined browser activity exceeded its explicit page bound")
}
fn browser_actor(actor: &RecordedActor) {
    assert_eq!(actor.principal, PrincipalRef::TrustedHostOperator);
    assert_eq!(actor.provenance, ActorProvenance::TrustedHost { path: TrustedHostPath::Browser });
}
async fn owner_released(path: &std::path::Path) {
    let mut lock_path = path.as_os_str().to_os_string(); lock_path.push(".controller-lock");
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let lock = std::fs::OpenOptions::new().read(true).write(true).open(&lock_path).unwrap();
            if lock.try_lock().is_ok() { break; }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }).await.expect("the actual owner releases the lock before reopening");
}

#[tokio::test]
async fn browser_join_keeps_wait_resolution_refs_and_original_receipts_after_later_edits_and_restart() {
    Box::pin(joined_scenario()).await;
}
async fn joined_scenario() {
    let f = joined_fixture().await;
    let made = ui(&f.controller, "POST", "/tasks", native_submission("joined-waiting", now()+3_600_000, None)).await;
    assert_eq!(made.status, 303, "{}", made.body);
    let waiting = task(&f.controller, task_id(&made)).await;
    let made = ui(&f.controller, "POST", "/tasks", native_submission("joined-resolver", now()+3_600_000, None)).await;
    assert_eq!(made.status, 303, "{}", made.body);
    let resolver = task(&f.controller, task_id(&made)).await;
    let work_path = format!("/tasks/{}/coordination", encode(&waiting.id));

    let page = ui(&f.controller, "GET", "/documents/new", vec![]).await;
    let made = ui(&f.controller, "POST", "/documents", form(&[
        ("operation_id", hidden_value(rendered_form(&page, "/documents"), "operation_id")),
        ("title", "Joined reviewed source".into()), ("body", "Original <scope> & evidence".into()),
        ("owner_agent_id", String::new()),
    ])).await;
    assert_eq!(made.status, 303, "{}", made.body);
    let doc_path = made.location.unwrap(); let doc_id = doc_path.strip_prefix("/documents/").unwrap().to_owned();
    let original_doc = document(&f.controller, &doc_id, None).await;
    browser_actor(&original_doc.version.author);
    let target = ReviewTarget::DocumentVersion { version: original_doc.version.reference.clone() };
    let page = ui(&f.controller, "GET", &doc_path, vec![]).await;
    let rendered = rendered_form(&page, "/reviews");
    let exact_target = hidden_value(rendered, "target");
    assert_eq!(serde_json::from_str::<ReviewTarget>(&exact_target).unwrap(), target);
    let made = ui(&f.controller, "POST", "/reviews", form(&[
        ("operation_id", hidden_value(rendered, "operation_id")), ("target", exact_target),
        ("reviewer_agent_id", String::new()), ("summary", "Assess the retained original version".into()),
    ])).await;
    assert_eq!(made.status, 303, "{}", made.body);
    let review_path = made.location.unwrap(); let review_id = review_path.strip_prefix("/reviews/").unwrap().to_owned();
    let requested_review = review(&f.controller, &review_id).await;
    browser_actor(&requested_review.review.requested_by);

    let page = ui(&f.controller, "GET", "/questions/new", vec![]).await;
    let rendered = rendered_form(&page, "/questions");
    let made = ui(&f.controller, "POST", "/questions", form(&[
        ("operation_id", hidden_value(rendered, "operation_id")), ("option_count", hidden_value(rendered, "option_count")),
        ("record_kind", "task".into()), ("record_id", waiting.id.clone()), ("recipient_kind", "operator".into()),
        ("recipient_agent_id", String::new()), ("prompt", "Which scope should this waiting task use? Recommend original.".into()),
        ("option_1_label", "Original".into()), ("option_1_explanation", "Preserve current scope".into()),
        ("option_2_label", "Narrow".into()), ("option_2_explanation", "Use the smaller scope".into()),
        ("recommended_option_id", "option-1".into()),
    ])).await;
    assert_eq!(made.status, 303, "{}", made.body);
    let question_path = made.location.unwrap(); let question_id = question_path.strip_prefix("/questions/").unwrap().to_owned();
    let posed = question(&f.controller, &question_id).await;
    browser_actor(&posed.question.poser);

    let waits_path = format!("{work_path}/waits");
    let mut original_wait = None;
    for (kind, id) in [("task", resolver.id.as_str()), ("question", question_id.as_str()), ("review", review_id.as_str())] {
        let page = ui(&f.controller, "GET", &work_path, vec![]).await;
        let mut fields = mutation_fields(&page, &waits_path);
        let operation_id = fields[0].1.clone();
        fields.extend([("resolver_kind", kind.into()), ("resolver_id", id.into())]);
        let body = form(&fields);
        let outcome = work_receipt(&ui(&f.controller, "POST", &waits_path, body.clone()).await);
        assert!(outcome.changed_wait.is_some());
        original_wait = Some((operation_id, body, outcome));
    }
    let (wait_operation, wait_body, wait_receipt) = original_wait.unwrap();
    assert_eq!(wait_receipt.view.open_waits, 3);
    assert!(wait_receipt.view.coordination_blocked);
    let (wait_original, _) = original_receipt(&f.controller, &wait_operation).await;
    let page = ui(&f.controller, "GET", &work_path, vec![]).await;
    for expected in ["Blocked by resolver waits", "Task:", "Question:", "Review:", &resolver.id, &question_id, &review_id] {
        assert!(page.body.contains(expected), "missing {expected}: {}", page.body);
    }

    // A later head cannot silently move an already requested exact-version review.
    let page = ui(&f.controller, "GET", &doc_path, vec![]).await;
    let version_path = format!("{doc_path}/versions");
    let mut fields = mutation_fields(&page, &version_path);
    fields.extend([("title", "Later head".into()), ("body", "Different current evidence".into())]);
    let edited = ui(&f.controller, "POST", &version_path, form(&fields)).await;
    assert_eq!(edited.status, 303, "{}", edited.body);
    let later_doc = document(&f.controller, &doc_id, None).await;
    assert_ne!(later_doc.version.reference, original_doc.version.reference);
    assert_eq!(document(&f.controller, &doc_id, Some(original_doc.version.reference.version)).await.version, original_doc.version);
    assert_eq!(review(&f.controller, &review_id).await.review.target, target);

    let answer_path = format!("{question_path}/answer");
    let page = ui(&f.controller, "GET", &question_path, vec![]).await;
    let mut answer_fields = mutation_fields(&page, &answer_path);
    let answer_operation = answer_fields[0].1.clone();
    answer_fields.extend([("option_id", "option-2".into()), ("explanation", "Narrow <scope> & retain provenance".into())]);
    let answer_body = form(&answer_fields);
    let answered = ui(&f.controller, "POST", &answer_path, answer_body.clone()).await;
    assert_eq!(answered.status, 303, "{}", answered.body);
    let (answer_original, answer_outcome) = original_receipt(&f.controller, &answer_operation).await;
    let Outcome::Ok { data: Data::QuestionMutation(answered) } = answer_outcome else { panic!("answer receipt expected") };
    let answer = answered.new_answer.unwrap();
    assert_eq!(answer.operation_id, answer_operation); browser_actor(&answer.author);
    assert_eq!(answer.answer.option_id, "option-2");
    assert_eq!(work(&f.controller, &waiting.id).await.open_waits, 2);

    let decide_path = format!("{review_path}/decide");
    let page = ui(&f.controller, "GET", &review_path, vec![]).await;
    assert!(page.body.contains(&format!("href=\"{doc_path}?version=1\"")));
    let mut decision_fields = mutation_fields(&page, &decide_path);
    let decision_operation = decision_fields[0].1.clone();
    decision_fields.extend([("verdict", "changes_requested".into()), ("explanation", "Preserve this negative review of version one".into())]);
    let decision_body = form(&decision_fields);
    let decided = ui(&f.controller, "POST", &decide_path, decision_body.clone()).await;
    assert_eq!(decided.status, 303, "{}", decided.body);
    let (decision_original, decision_outcome) = original_receipt(&f.controller, &decision_operation).await;
    let Outcome::Ok { data: Data::ReviewMutation(result) } = decision_outcome else { panic!("review decision receipt expected") };
    let ReviewMutationOutcome::Review(decided) = *result else { panic!("review decision receipt expected") };
    let decision = decided.decision.unwrap();
    assert_eq!(decision.operation_id, decision_operation); assert_eq!(decision.target, target);
    assert_eq!(decision.verdict, ReviewVerdict::ChangesRequested); browser_actor(&decision.reviewer);
    assert_eq!(work(&f.controller, &waiting.id).await.open_waits, 1);

    let resolver_path = format!("/tasks/{}", encode(&resolver.id));
    let cancel_path = format!("{resolver_path}/cancel");
    let page = ui(&f.controller, "GET", &resolver_path, vec![]).await;
    let fields = mutation_fields(&page, &cancel_path); let cancel_operation = fields[0].1.clone();
    let cancelled = ui(&f.controller, "POST", &cancel_path, form(&fields)).await;
    assert_eq!(cancelled.status, 303, "{}", cancelled.body);
    let released = work(&f.controller, &waiting.id).await;
    assert_eq!(released.open_waits, 0); assert!(!released.coordination_blocked);
    assert!(matches!(released.state, WorkState::Runnable { .. }));
    assert_eq!(released.task.input, waiting.input); assert_eq!(released.task.session_id, waiting.session_id);
    assert_eq!(released.task.assigned_agent_id, waiting.assigned_agent_id);
    assert_eq!(released.task.status, TaskStatus::Queued);
    assert!(released.task.attempt.is_none() && released.task.result.is_none());
    let page = ui(&f.controller, "GET", &work_path, vec![]).await;
    assert!(page.body.contains("Execution still depends on ordinary admission"));
    assert!(page.body.contains("changes_requested"));
    assert!(!page.body.contains("Request retry"));

    let events = activities(&f.controller).await;
    let browser_events = browser_activities(&f.controller).await;
    for wait in &released.waits.items {
        browser_actor(&wait.created_by);
        let WaitStatus::Resolved { resolution, actor, operation_id } = &wait.status else { panic!("all three waits must retain their exact resolutions") };
        browser_actor(actor);
        let event = events.iter().find(|event| event.id == resolution.event_id).expect("exact terminal event");
        browser_actor(&event.actor);
        let visible = browser_events.iter().find(|value| value["id"]["database_id"] == event.id.database_id
            && value["id"]["product_revision"] == event.id.product_revision.to_string()
            && value["id"]["ordinal"] == event.id.ordinal.to_string()).expect("same durable occurrence in browser activity");
        assert_eq!(visible["actor"]["provenance"]["path"], "browser");
        match (&resolution.outcome, &event.detail) {
            (ResolverOutcome::Question(value), ActivityDetail::Question(QuestionChange::Closed { resolution: actual })) => {
                assert_eq!(value, actual); assert_eq!(operation_id, &answer_operation);
                assert_eq!(value, &QuestionResolution::Answered { question_id: question_id.clone(),
                    answer: AnswerRef { question_id: question_id.clone(), answer_id: answer.id.clone() }, selected_option_id: "option-2".into() });
                assert_eq!(visible["detail"]["change"]["resolution"]["answer"]["answer_id"], answer.id);
                assert_eq!(event.routing.as_ref().unwrap().linked_record, posed.question.posed.record);
                assert_eq!(event.routing.as_ref().unwrap().requested_by, posed.question.poser);
            },
            (ResolverOutcome::Review(value), ActivityDetail::Review(ReviewChange::Closed { resolution: actual })) => {
                assert_eq!(value, actual); assert_eq!(operation_id, &decision_operation);
                assert_eq!(value, &ReviewResolution::Decided { review_id: review_id.clone(), decision_id: decision.id.clone(),
                    target: target.clone(), verdict: ReviewVerdict::ChangesRequested });
                assert_eq!(visible["detail"]["change"]["resolution"]["target"]["version"]["content_id"], original_doc.version.reference.content_id);
            },
            (ResolverOutcome::Task(value), ActivityDetail::Task(TaskChange::Closed { resolution: actual })) => {
                assert_eq!(value, actual); assert_eq!(operation_id, &cancel_operation);
                assert_eq!(value.task_id, resolver.id); assert_eq!(value.status, TerminalTaskStatus::Cancelled);
                assert!(value.accepted_result.is_none());
                assert_eq!(visible["detail"]["change"]["resolution"]["status"], "cancelled");
            },
            other => panic!("wait and terminal event disagree: {other:?}"),
        }
        assert!(events.iter().any(|other| other.id.product_revision == event.id.product_revision
            && other.subject == RecordRef::Task { task_id: waiting.id.clone() }
            && matches!(&other.detail, ActivityDetail::Wait(WaitChange::Resolved { wait: id, outcome, .. })
                if id == &wait.reference && outcome == &resolution.outcome)), "resolution fanout must share the original product transaction");
    }
    assert_eq!(released.waits.items.len(), 3);

    // A later answer changes current effect, not any wait's immutable resolution.
    let page = ui(&f.controller, "GET", &question_path, vec![]).await;
    let replace_path = format!("{question_path}/replace-answer");
    let mut fields = mutation_fields(&page, &replace_path);
    fields.push(("expected_answer_id", hidden_value(rendered_form(&page, &replace_path), "expected_answer_id")));
    fields.extend([("option_id", "option-1".into()), ("explanation", "Current effect now differs".into()), ("reason", "New evidence after the original wait closed".into())]);
    let replaced = ui(&f.controller, "POST", &replace_path, form(&fields)).await;
    assert_eq!(replaced.status, 303, "{}", replaced.body);
    let current_question = question(&f.controller, &question_id).await;
    assert_ne!(current_question.active_answer.as_ref().unwrap().id, answer.id);
    assert_eq!(current_question.active_answer.as_ref().unwrap().answer.option_id, "option-1");
    assert_eq!(work(&f.controller, &waiting.id).await.waits, released.waits);
    let events_after_edit = activities(&f.controller).await;

    let database = f.path.clone(); drop(f); owner_released(&database).await;
    let reopened = Controller::open_with_config(&database, joined_config()).unwrap();
    for (id, original) in [(&wait_operation, &wait_original), (&answer_operation, &answer_original), (&decision_operation, &decision_original)] {
        let (lookup, _) = original_receipt(&reopened, id).await;
        assert_eq!(lookup.commit, original.commit); assert_eq!(lookup.outcome, original.outcome);
    }
    // Exact original forms replay even though their CAS revisions are now stale.
    assert_eq!(work_receipt(&ui(&reopened, "POST", &waits_path, wait_body).await), wait_receipt);
    assert_eq!(work_receipt(&ui(&reopened, "GET", &format!("/operations/{}", encode(&wait_operation)), vec![]).await), wait_receipt);
    for (path, body, operation) in [(&answer_path, answer_body, &answer_operation), (&decide_path, decision_body, &decision_operation)] {
        let replay = ui(&reopened, "POST", path, body).await;
        assert_eq!(replay.status, 303, "{}", replay.body);
        let lookup = ui(&reopened, "GET", &format!("/operations/{}", encode(operation)), vec![]).await;
        assert_eq!(lookup.status, 200, "{}", lookup.body); assert!(lookup.body.contains(operation));
    }
    assert_eq!(activities(&reopened).await, events_after_edit, "receipt replay must not emit another resolution");
    assert_eq!(work(&reopened, &waiting.id).await.waits, released.waits);
    assert_eq!(question(&reopened, &question_id).await, current_question);
    assert_eq!(document(&reopened, &doc_id, None).await.version, later_doc.version);
    assert_eq!(document(&reopened, &doc_id, Some(1)).await.version, original_doc.version);
    assert_eq!(review(&reopened, &review_id).await.decision.unwrap(), decision);
}
