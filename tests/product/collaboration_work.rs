//! Include beneath tests/product.rs to exercise the real browser/controller path.
//! Source-only preparation: no provider, remote host or test run is claimed.
use super::*;
use borg_runtime::collaboration::{work::*, workflow::*, questions::*};
use borg_runtime::limits::WorkflowLimits;

fn coordination_config() -> ControllerConfig {
    let mut cfg = config();
    let limits = std::sync::Arc::make_mut(&mut cfg.limits);
    limits.workflow = Some(WorkflowLimits {
        record_bytes: 262_144.min(limits.transport.body_bytes), text_bytes: 4096,
        options: 8, records: 256, revisions: 4096,
        page_items: 20.min(limits.controller.collaboration_page_items),
        page_bytes: 262_144.min(limits.controller.collaboration_page_bytes),
        waits_per_task: 16, graph_scan_nodes: 256, events_per_mutation: 64,
        event_bytes: 16_384, notice_bytes: 16_384,
        pending_delivery_bytes: 65_536.min(limits.transport.body_bytes),
        delivery_scan_rows: 256,
    });
    limits.validate().unwrap();
    cfg
}

async fn coordination_fixture() -> Fixture {
    let root = std::env::temp_dir().join(format!("borg-product-coordination-{}-{}-{}", std::process::id(), now(), NEXT.fetch_add(1, Ordering::Relaxed)));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("tasks.sqlite");
    let controller = Controller::open_with_config(&path, coordination_config()).unwrap();
    register_fixture_repository(&controller).await;
    Fixture { controller, path }
}

async fn coordination_seed(c: &Controller, operation: &str) -> Task {
    let made = ui(c, "POST", "/tasks", native_submission(operation, now() + 3_600_000, None)).await;
    assert_eq!(made.status, 303, "{}", made.body);
    task(c, task_id(&made)).await
}

async fn coordination_exchange(c: &Controller, command: Operation) -> Reply {
    // Match the parent fixture: preserve the request while boxing its large future.
    Box::pin(c.handle(Request { protocol_revision: WIRE_REVISION, request_id: format!("coordination-read-{}", NEXT.fetch_add(1, Ordering::Relaxed)), command })).await
}

async fn coordination_owner_released(path: &std::path::Path) {
    let mut lock_path = path.as_os_str().to_os_string();
    lock_path.push(".controller-lock");
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let lock = std::fs::OpenOptions::new().read(true).write(true).open(&lock_path).unwrap();
            if lock.try_lock().is_ok() { break; }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }).await.expect("the real controller owner must release before restart");
}

async fn coordination_view(c: &Controller, id: &str) -> WorkView {
    match call(c, Operation::QueryWork { query: WorkQuery::Get { task_id: id.into() } }).await {
        Data::WorkQuery(WorkQueryResult::Task(view)) => view,
        other => panic!("expected work view, got {other:?}"),
    }
}

fn coordination_route(id: &str) -> String { format!("/tasks/{}/coordination", encode(id)) }

fn wait_form(operation: &str, revision: u64, kind: &str, id: &str) -> Vec<u8> {
    form(&[("operation_id", operation.into()), ("expected_revision", revision.to_string()), ("resolver_kind", kind.into()), ("resolver_id", id.into())])
}

fn owner_form(operation: &str, revision: u64, owner: &PrincipalRef) -> Vec<u8> {
    let (kind, agent) = match owner { PrincipalRef::TrustedHostOperator => ("operator", ""), PrincipalRef::Agent { agent_id } => ("agent", agent_id.as_str()) };
    form(&[("operation_id", operation.into()), ("expected_revision", revision.to_string()), ("owner_kind", kind.into()), ("owner_agent_id", agent.into())])
}

fn decode_receipt(response: &UiResponse) -> WorkMutationOutcome {
    assert_eq!(response.status, 200, "{}", response.body);
    assert!(response.location.is_none());
    assert!(response.body.contains("data-live-refresh=\"false\""));
    assert!(!response.body.contains("method=\"post\""));
    let raw = response.body.split("<pre id=\"coordination-receipt\">").nth(1).expect("exact receipt").split("</pre>").next().unwrap();
    let json = raw.replace("&quot;", "\"").replace("&#39;", "'").replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&");
    serde_json::from_str(&json).unwrap()
}

async fn coordination_question(c: &Controller, id: &str, operation: &str) -> Question {
    let mutation = QuestionMutation::Pose { question: PoseQuestion {
        record: RecordRef::Task { task_id: id.into() }, recipient: PrincipalRef::TrustedHostOperator,
        prompt: "Proceed with the optional change? Recommend hold while facts remain unclear.".into(),
        options: vec![QuestionOption { id: "hold".into(), label: "Hold".into(), explanation: "Keep the existing behavior".into() }, QuestionOption { id: "proceed".into(), label: "Proceed".into(), explanation: "Use the proposed behavior".into() }],
        recommended_option_id: "hold".into(),
    } };
    match call(c, Operation::MutateQuestion { operation_id: operation.into(), mutation }).await {
        Data::QuestionMutation(value) => value.question.question,
        other => panic!("expected question outcome, got {other:?}"),
    }
}

#[tokio::test]
async fn coordination_browser_shows_multiple_typed_waits_and_negative_resolution_without_claiming_execution() {
    let f = coordination_fixture().await;
    let waiting = coordination_seed(&f.controller, "work-waiting").await;
    let resolver = coordination_seed(&f.controller, "work-resolver").await;
    let question = coordination_question(&f.controller, &waiting.id, "work-question").await;
    let path = coordination_route(&waiting.id);
    let first = coordination_view(&f.controller, &waiting.id).await;
    let one = ui(&f.controller, "POST", &format!("{path}/waits"), wait_form("work-add-task", first.coordination_revision, "task", &resolver.id)).await;
    let one = decode_receipt(&one);
    let two = ui(&f.controller, "POST", &format!("{path}/waits"), wait_form("work-add-question", one.view.coordination_revision, "question", &question.id)).await;
    let two = decode_receipt(&two);
    assert_eq!(two.view.open_waits, 2);
    let page = ui(&f.controller, "GET", &path, vec![]).await;
    assert_eq!(page.status, 200, "{}", page.body);
    for expected in ["Blocked by resolver waits", "Task:", "Question:", &resolver.id, &question.id] { assert!(page.body.contains(expected), "{}", page.body); }

    call(&f.controller, Operation::MutateQuestion { operation_id: "work-cancel-question".into(), mutation: QuestionMutation::Cancel { question_id: question.id.clone(), expected_revision: question.revision, reason: "<script>negative outcome</script>".into() } }).await;
    let partial = coordination_view(&f.controller, &waiting.id).await;
    assert_eq!(partial.open_waits, 1);
    assert!(partial.coordination_blocked);
    let page = ui(&f.controller, "GET", &path, vec![]).await;
    assert!(page.body.contains("&lt;script&gt;negative outcome&lt;/script&gt;"));
    assert!(!page.body.contains("<script>negative outcome"));
    assert!(page.body.contains("Blocked by resolver waits"));
    call(&f.controller, Operation::CancelTask { operation_id: "work-cancel-resolver".into(), task_id: resolver.id.clone(), expected_revision: task(&f.controller, &resolver.id).await.revision }).await;
    let resolved = coordination_view(&f.controller, &waiting.id).await;
    assert_eq!(resolved.open_waits, 0);
    assert!(!resolved.coordination_blocked);
    assert!(matches!(resolved.state, WorkState::Runnable { .. }));
    assert_eq!(resolved.task.input, waiting.input);
    assert_eq!(resolved.task.session_id, waiting.session_id);
    assert_eq!(resolved.task.attempt, waiting.attempt);
    assert_eq!(resolved.task.status, TaskStatus::Queued);
    assert!(resolved.task.result.is_none());
    let page = ui(&f.controller, "GET", &path, vec![]).await;
    assert!(page.body.contains("Execution still depends on ordinary admission"));
    assert!(!page.body.contains("Request retry"));
    assert!(resolved.waits.items.iter().all(|wait| matches!(&wait.status, WaitStatus::Resolved { .. })));
}

#[tokio::test]
async fn coordination_owner_forms_keep_accepted_identity_and_exact_receipts_after_restart() {
    let f = coordination_fixture().await;
    let accepted = coordination_seed(&f.controller, "work-owner-task").await;
    let question = coordination_question(&f.controller, &accepted.id, "work-owner-question").await;
    let initial = coordination_view(&f.controller, &accepted.id).await;
    let path = coordination_route(&accepted.id);
    let original_owner = PrincipalRef::TrustedHostOperator;
    let original_body = owner_form("work-owner-original", initial.coordination_revision, &original_owner);
    let first_page = ui(&f.controller, "POST", &format!("{path}/owner"), original_body.clone()).await;
    let original = decode_receipt(&first_page);
    let Data::QuestionQuery(result) = call(&f.controller, Operation::QueryQuestion { query: QuestionQuery::Get { question_id: question.id.clone() } }).await else { panic!("expected linked question"); };
    let QuestionQueryResult::Question(question_view) = *result else { panic!("expected linked question"); };
    assert_eq!(question_view.question, question);
    assert_eq!(question_view.question.status, QuestionStatus::Open);
    assert_eq!(question_view.current_responsible_owner, original_owner);
    let first = coordination_exchange(&f.controller, Operation::GetOperation { operation_id: "work-owner-original".into() }).await;
    assert!(first.commit.is_some());
    let latest_owner = PrincipalRef::Agent { agent_id: accepted.assigned_agent_id.clone() };
    let later = ui(&f.controller, "POST", &format!("{path}/owner"), owner_form("work-owner-later", original.view.coordination_revision, &latest_owner)).await;
    let later = decode_receipt(&later);
    assert_eq!(later.view.responsible_owner, latest_owner);
    let Data::QuestionQuery(result) = call(&f.controller, Operation::QueryQuestion { query: QuestionQuery::Get { question_id: question.id.clone() } }).await else { panic!("expected linked question"); };
    let QuestionQueryResult::Question(question_view) = *result else { panic!("expected linked question"); };
    assert_eq!(question_view.question, question);
    assert_eq!(question_view.current_responsible_owner, latest_owner);
    let unchanged = task(&f.controller, &accepted.id).await;
    assert_eq!(unchanged.input, accepted.input);
    assert_eq!(unchanged.assigned_agent_id, accepted.assigned_agent_id);
    assert_eq!(unchanged.session_id, accepted.session_id);
    let stale = ui(&f.controller, "POST", &format!("{path}/owner"), owner_form("work-owner-stale", original.view.coordination_revision, &original_owner)).await;
    assert_eq!(stale.status, 409, "{}", stale.body);
    assert_eq!(coordination_view(&f.controller, &accepted.id).await.responsible_owner, latest_owner);
    for response in [
        ui(&f.controller, "POST", &format!("{path}/owner"), original_body.clone()).await,
        ui(&f.controller, "GET", "/operations/work-owner-original", vec![]).await,
    ] { assert_eq!(decode_receipt(&response), original); }
    let conflict = ui(&f.controller, "POST", &format!("{path}/owner"), owner_form("work-owner-original", later.view.coordination_revision, &latest_owner)).await;
    assert_eq!(conflict.status, 409, "{}", conflict.body);
    assert!(conflict.body.contains("operation_conflict"));
    let cfg = coordination_config();
    let db = f.path.clone();
    drop(f);
    coordination_owner_released(&db).await;
    let reopened = Controller::open_with_config(&db, cfg).unwrap();
    let lookup = coordination_exchange(&reopened, Operation::GetOperation { operation_id: "work-owner-original".into() }).await;
    assert_eq!(lookup.commit, first.commit);
    assert_eq!(lookup.outcome, first.outcome);
    assert_eq!(decode_receipt(&ui(&reopened, "GET", "/operations/work-owner-original", vec![]).await), original);
    assert_eq!(decode_receipt(&ui(&reopened, "POST", &format!("{path}/owner"), original_body).await), original);
}

#[tokio::test]
async fn coordination_browser_refuses_untrusted_and_malformed_forms_before_claiming_operation() {
    let f = coordination_fixture().await;
    let waiting = coordination_seed(&f.controller, "work-form-task").await;
    let resolver = coordination_seed(&f.controller, "work-form-resolver").await;
    let view = coordination_view(&f.controller, &waiting.id).await;
    let path = format!("{}/waits", coordination_route(&waiting.id));
    let body = wait_form("work-origin", view.coordination_revision, "task", &resolver.id);
    for origin in [None, Some("null"), Some("https://other.invalid")] {
        let response = handle(&f.controller, UiRequest { method: "POST".into(), path: path.clone(), body: body.clone(), origin: origin.map(str::to_owned), expected_origin: ORIGIN.into() }).await;
        assert_eq!(response.status, 403, "{}", response.body);
    }
    let unknown = [body.clone(), b"&actor=operator".to_vec()].concat();
    let duplicate = [body.clone(), b"&expected_revision=0".to_vec()].concat();
    let malformed = [body.clone(), b"&reason=%GG".to_vec()].concat();
    for malformed_body in [unknown, duplicate, malformed, wait_form("work-origin", view.coordination_revision, "agent", &resolver.assigned_agent_id)] {
        let response = ui(&f.controller, "POST", &path, malformed_body).await;
        assert_eq!(response.status, 400, "{}", response.body);
    }
    assert_eq!(ui(&f.controller, "POST", &format!("{path}?resolver_kind=task"), body.clone()).await.status, 400);
    assert_eq!(ui(&f.controller, "PUT", &path, body.clone()).await.status, 405);
    let missing = coordination_exchange(&f.controller, Operation::GetOperation { operation_id: "work-origin".into() }).await;
    assert!(matches!(missing.outcome, Outcome::Refused { ref code, .. } if code == "not_found"));
    assert_eq!(coordination_view(&f.controller, &waiting.id).await, view);
    let valid = ui(&f.controller, "POST", &path, body).await;
    assert_eq!(decode_receipt(&valid).view.open_waits, 1);
}

#[tokio::test]
async fn coordination_wait_removal_keeps_original_reason_and_cancelled_task_terminal() {
    let f = coordination_fixture().await;
    let waiting = coordination_seed(&f.controller, "work-removal-task").await;
    let resolver = coordination_seed(&f.controller, "work-removal-resolver").await;
    let path = coordination_route(&waiting.id);
    let initial = coordination_view(&f.controller, &waiting.id).await;
    let added = decode_receipt(&ui(&f.controller, "POST", &format!("{path}/waits"), wait_form("work-removal-add", initial.coordination_revision, "task", &resolver.id)).await);
    let wait = added.changed_wait.unwrap();
    let remove_path = format!("{path}/waits/{}/remove", encode(&wait.wait_id));
    let body = form(&[("operation_id", "work-remove".into()), ("expected_revision", added.view.coordination_revision.to_string()), ("reason", "No longer required <reason>".into())]);
    let removed = decode_receipt(&ui(&f.controller, "POST", &remove_path, body.clone()).await);
    assert_eq!(removed.view.open_waits, 0);
    assert!(removed.view.waits.items.iter().any(|wait| matches!(&wait.status, WaitStatus::Removed { reason, .. } if reason == "No longer required <reason>")));
    assert_eq!(decode_receipt(&ui(&f.controller, "POST", &remove_path, body).await), removed);
    let reopened = decode_receipt(&ui(&f.controller, "POST", &format!("{path}/waits"), wait_form("work-add-before-cancel", removed.view.coordination_revision, "task", &resolver.id)).await);
    call(&f.controller, Operation::CancelTask { operation_id: "work-cancel-waiting".into(), task_id: waiting.id.clone(), expected_revision: reopened.view.task.revision }).await;
    call(&f.controller, Operation::CancelTask { operation_id: "work-cancel-last-resolver".into(), task_id: resolver.id.clone(), expected_revision: task(&f.controller, &resolver.id).await.revision }).await;
    let final_view = coordination_view(&f.controller, &waiting.id).await;
    assert_eq!(final_view.task.status, TaskStatus::Cancelled);
    assert_eq!(final_view.task.input, waiting.input);
    assert_eq!(final_view.task.session_id, waiting.session_id);
    assert!(final_view.task.attempt.is_none());
    let page = ui(&f.controller, "GET", &path, vec![]).await;
    assert!(page.body.contains("Terminal: cancelled"));
    assert!(page.body.contains("does not restart it"));
    assert!(!page.body.contains("Add a resolver wait"));
    assert!(!page.body.contains("Request retry"));
}
