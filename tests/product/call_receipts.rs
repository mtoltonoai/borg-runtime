//! Product-route reads of real local collaboration receipts. The caller comes
//! from an actual registered worker's controller-issued attempt; no provider,
//! worker process, fabricated commit or supplied task authority is exercised.
use super::*;
use borg_runtime::collaboration::{CallRecord, Query, QueryResult};
use borg_runtime::mcp::{Caller, Invocation};

const CALL_VIEW_NOW: u64 = 1_800_000_000_000;

struct CallsFixture {
    fixture: Fixture,
    own: Task,
    other: Task,
    caller: Caller,
}

async fn calls_fixture(mut cfg: ControllerConfig) -> CallsFixture {
    cfg.clock = std::sync::Arc::new(|| CALL_VIEW_NOW);
    // The runner's fresh artifact root separates executions even when their
    // process namespaces reuse the same PID and fixture counter.
    let parent = std::env::var_os("BORG_TEST_ARTIFACT")
        .map(PathBuf::from).unwrap_or_else(std::env::temp_dir);
    let root = parent.join(format!(
        "borg-call-view-{}-{}-{}", std::process::id(), now(), NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("tasks.sqlite");
    let controller = Controller::open_with_config(&path, cfg).unwrap();
    let first = ui(&controller, "POST", "/tasks", native_submission(
        "call-view-own-submission", CALL_VIEW_NOW + 3_600_000, None,
    )).await;
    let own = task(&controller, task_id(&first)).await;
    let second = ui(&controller, "POST", "/tasks", native_submission(
        "call-view-other-submission", CALL_VIEW_NOW + 3_600_000, None,
    )).await;
    let other = task(&controller, task_id(&second)).await;
    assert_ne!(own.id, other.id);
    assert_ne!(own.session_id, other.session_id);
    // Same-agent tasks make the ownership check discriminate exact task scope.
    assert_eq!(own.assigned_agent_id, other.assigned_agent_id);
    let worker = Worker {
        id: "call-view-worker".into(), incarnation: "one".into(),
        coding_capacity: 0, native_capacity: 2,
    };
    call(&controller, Operation::RegisterWorker {
        operation_id: "call-view-register".into(), worker: worker.clone(),
    }).await;
    let Data::Commands(commands) = call(&controller, Operation::AwaitCommands {
        operation_id: "call-view-assignment".into(), worker_id: worker.id,
        incarnation: worker.incarnation, acknowledged: vec![],
    }).await else { panic!("actual controller assignment") };
    let attempt = commands.iter().find_map(|command| match command {
        WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == own.id => Some(attempt.clone()),
        _ => None,
    }).expect("the submitted task has a controller-issued attempt");
    report(&controller, &attempt, Observation::Started { thread_id: None, turn_id: None }).await;
    let caller = Caller {
        task_id: attempt.task_id.clone(), attempt_id: attempt.id.clone(), generation: attempt.generation,
        worker_id: attempt.worker_id.clone(), incarnation: attempt.worker_incarnation.clone(),
    };
    let own = task(&controller, &own.id).await;
    let other = task(&controller, &other.id).await;
    assert_eq!(own.attempt.as_ref().unwrap().id, caller.attempt_id);
    CallsFixture { fixture: Fixture { controller, path }, own, other, caller }
}

fn invocation(caller: &Caller, id: &str, tool: &str, arguments: serde_json::Value) -> Invocation {
    Invocation {
        operation_id: id.into(), logical_call_id: format!("logical-{id}"),
        caller: caller.clone(), tool: tool.into(), arguments,
    }
}

async fn exchange(controller: &Controller, command: Operation) -> Reply {
    let result = controller.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: format!("call-view-read-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
        command: command.clone(),
    }).await;
    result.validate_for(&command).unwrap();
    result
}

fn route(task_id: &str, operation_id: Option<&str>) -> String {
    let base = format!("/tasks/{}/calls", encode(task_id));
    operation_id.map(|id| format!("{base}?operation_id={}", encode(id))).unwrap_or(base)
}

async fn projection(controller: &Controller, task_id: &str, operation_id: &str) -> CallRecord {
    let wrapper = exchange(controller, Operation::QueryCollaboration {
        query: Query::Call { task_id: task_id.into(), operation_id: operation_id.into() },
    }).await;
    // This is a read wrapper. The original commit belongs to the nested result.
    assert!(wrapper.commit.is_none(), "{wrapper:?}");
    match wrapper.outcome {
        Outcome::Ok { data: Data::Collaboration(QueryResult::Call(record)) } => *record,
        other => panic!("expected original call projection: {other:?}"),
    }
}

fn rendered_record(response: &UiResponse) -> CallRecord {
    assert_eq!(response.status, 200, "{}", response.body);
    let (_, rest) = response.body.split_once("<h2>Original call and recorded outcome</h2><pre>")
        .expect("record must be presented as the original call");
    let (escaped, _) = rest.split_once("</pre>").unwrap();
    // Decode the text content, keeping a literal entity in the original JSON
    // literal. Ampersands are decoded last so entities are not decoded twice.
    let text = escaped.replace("&quot;", "\"").replace("&#39;", "'")
        .replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&");
    serde_json::from_str(&text).expect("the view contains the canonical CallRecord shape")
}

async fn read_snapshot(controller: &Controller, own: &str, other: &str) -> (Task, Task, u64) {
    let first = task(controller, own).await;
    let second = task(controller, other).await;
    let wrapper = exchange(controller, Operation::GetTask { task_id: own.into() }).await;
    (first, second, wrapper.observed_controller_revision.unwrap())
}

#[tokio::test]
async fn call_receipt_route_displays_original_record_after_later_memory_change_and_task_cancel() {
    let f = calls_fixture(config()).await;
    let c = &f.fixture.controller;
    let original = invocation(&f.caller, "call-original/\"&é", "collab_memory_set", serde_json::json!({
        "key": "call-view-note", "expected_revision": null,
        "value": {"version": 1, "text": "<script>display-only</script> & literal &lt; quote\""},
    }));
    let committed = exchange(c, original.operation()).await;
    assert!(matches!(&committed.outcome, Outcome::Ok { data: Data::Memory(Some(value)) } if value.revision == 1), "{committed:?}");
    assert_eq!(committed.commit.as_ref().unwrap().operation_id, original.operation_id);
    let later = invocation(&f.caller, "call-later-memory", "collab_memory_set", serde_json::json!({
        "key": "call-view-note", "expected_revision": 1, "value": {"version": 2},
    }));
    let later = exchange(c, later.operation()).await;
    assert!(matches!(later.outcome, Outcome::Ok { data: Data::Memory(Some(_)) }));
    let expected = projection(c, &f.own.id, &original.operation_id).await;
    assert_eq!(expected.invocation, original);
    assert_eq!(expected.invocation.caller.task_id, f.own.id);
    assert_eq!(expected.result.reply.commit, committed.commit);
    assert_eq!(expected.result.reply.outcome, committed.outcome);
    assert!(!expected.result.requires_quiescence);
    let before = read_snapshot(c, &f.own.id, &f.other.id).await;
    let memory_before = call(c, Operation::GetMemory {
        agent_id: f.own.assigned_agent_id.clone(), key: "call-view-note".into(),
    }).await;
    assert!(matches!(&memory_before, Data::Memory(Some(value)) if value.revision == 2));
    let page = ui(c, "GET", &route(&f.own.id, Some(&original.operation_id)), vec![]).await;
    assert_eq!(rendered_record(&page), expected);
    assert!(page.body.contains(&format!("href=\"/tasks/{}\"", encode(&f.own.id))));
    assert!(page.body.contains(&format!("href=\"/tasks/{}/history\"", encode(&f.own.id))));
    assert!(page.body.contains(&format!("href=\"/operations/{}\">Original operation receipt</a>", encode(&original.operation_id))));
    assert!(page.body.contains("This never executes or retries the call."));
    assert!(page.body.contains("&lt;script&gt;display-only&lt;/script&gt;"));
    assert!(!page.body.contains("<script>display-only"));
    assert_eq!(read_snapshot(c, &f.own.id, &f.other.id).await, before);
    assert_eq!(call(c, Operation::GetMemory {
        agent_id: f.own.assigned_agent_id.clone(), key: "call-view-note".into(),
    }).await, memory_before);

    // Historical inspection does not need the old caller to remain executable.
    let current = task(c, &f.own.id).await;
    call(c, Operation::CancelTask {
        operation_id: "call-view-cancel".into(), task_id: current.id.clone(), expected_revision: current.revision,
    }).await;
    let before = read_snapshot(c, &f.own.id, &f.other.id).await;
    let historical = ui(c, "GET", &route(&f.own.id, Some(&original.operation_id)), vec![]).await;
    let retained = rendered_record(&historical);
    assert_eq!(retained.invocation, original);
    assert_eq!(retained.result.reply.commit, committed.commit);
    assert_eq!(retained.result.reply.outcome, committed.outcome);
    assert_eq!(read_snapshot(c, &f.own.id, &f.other.id).await, before);
}

#[tokio::test]
async fn call_receipt_route_shows_a_committed_refusal_as_a_readable_original_outcome() {
    let f = calls_fixture(config()).await;
    let c = &f.fixture.controller;
    let rejected = invocation(&f.caller, "call-refused-memory", "collab_memory_set", serde_json::json!({
        "key": "never-created", "expected_revision": 7, "value": "must-not-be-written",
    }));
    let original = exchange(c, rejected.operation()).await;
    assert!(matches!(&original.outcome, Outcome::Refused { code, .. } if code == "stale_revision"), "{original:?}");
    assert!(original.commit.is_some());
    let before = read_snapshot(c, &f.own.id, &f.other.id).await;
    let page = ui(c, "GET", &route(&f.own.id, Some(&rejected.operation_id)), vec![]).await;
    // HTTP200 describes the successful observation, not success of that call.
    let observed = rendered_record(&page);
    assert_eq!(observed.invocation, rejected);
    assert_eq!(observed.result.reply.outcome, original.outcome);
    assert_eq!(observed.result.reply.commit, original.commit);
    assert!(page.body.contains("stale_revision"));
    assert!(page.body.contains(&format!("href=\"/operations/{}\"", rejected.operation_id)));
    assert_eq!(call(c, Operation::GetMemory {
        agent_id: f.own.assigned_agent_id.clone(), key: "never-created".into(),
    }).await, Data::Memory(None));
    assert_eq!(read_snapshot(c, &f.own.id, &f.other.id).await, before);
}

#[tokio::test]
async fn call_receipt_route_refuses_absent_cross_task_non_call_and_outside_catalogue_receipts() {
    let f = calls_fixture(config()).await;
    let c = &f.fixture.controller;
    let own = invocation(&f.caller, "call-own-only", "collab_memory_set", serde_json::json!({
        "key": "private-call-key", "expected_revision": null, "value": "private-call-result-marker",
    }));
    let original = exchange(c, own.operation()).await;
    assert!(original.commit.is_some());
    assert!(matches!(original.outcome, Outcome::Ok { .. }));
    // The catalogue rejects this real invocation and the controller records that
    // refusal. No direct store insertion is used to manufacture an old call.
    let excluded = invocation(&f.caller, "call-excluded-tool", "not_a_catalogue_tool", serde_json::json!({}));
    let rejected = exchange(c, excluded.operation()).await;
    assert!(rejected.commit.is_some());
    assert!(matches!(rejected.outcome, Outcome::Refused { .. }));
    let stored = exchange(c, Operation::GetOperation { operation_id: excluded.operation_id.clone() }).await;
    let Outcome::Ok { data: Data::Operation(record) } = stored.outcome else { panic!("actual excluded-tool receipt") };
    assert_eq!(record.intended, serde_json::to_value(excluded.operation()).unwrap());
    let before = read_snapshot(c, &f.own.id, &f.other.id).await;
    for (task_id, operation_id) in [
        (f.own.id.as_str(), "call-absent"),
        (f.other.id.as_str(), own.operation_id.as_str()),
        (f.own.id.as_str(), "call-view-own-submission"),
        (f.own.id.as_str(), excluded.operation_id.as_str()),
        ("task-not-present", own.operation_id.as_str()),
    ] {
        let page = ui(c, "GET", &route(task_id, Some(operation_id)), vec![]).await;
        assert_eq!(page.status, 404, "{task_id}/{operation_id}: {}", page.body);
        assert!(page.body.contains("not_found"));
        for withheld in ["Original call and recorded outcome", "private-call-result-marker", "private-call-key"] {
            assert!(!page.body.contains(withheld), "{task_id}/{operation_id}: {}", page.body);
        }
        assert!(!page.body.contains(&format!("href=\"/operations/{}\"", encode(operation_id))));
    }
    assert_eq!(read_snapshot(c, &f.own.id, &f.other.id).await, before);
    let retained = projection(c, &f.own.id, &own.operation_id).await;
    assert_eq!(retained.result.reply.outcome, original.outcome);
    assert_eq!(retained.result.reply.commit, original.commit);
}

#[tokio::test]
async fn call_receipt_form_and_invalid_queries_keep_read_status_and_make_no_new_operation() {
    let f = calls_fixture(config()).await;
    let c = &f.fixture.controller;
    let base = route(&f.own.id, None);
    let before = read_snapshot(c, &f.own.id, &f.other.id).await;
    for uri in [base.clone(), format!("{base}?operation_id=")] {
        let page = ui(c, "GET", &uri, vec![]).await;
        assert_eq!(page.status, 200, "{}", page.body);
        assert!(page.body.contains(&format!("<form method=\"get\" action=\"{base}\">")));
        assert!(page.body.contains("name=\"operation_id\""));
        assert!(!page.body.contains("Original call and recorded outcome"));
        assert!(!page.body.contains("Original operation receipt</a>"));
        assert!(!page.body.contains("method=\"post\""));
    }
    let too_long = "x".repeat(c.limits().transport.identity_bytes + 1);
    for (uri, status) in [
        (format!("{base}?operation_id=missing&unexpected=value"), 400),
        (format!("{base}?operation_id=%ZZ"), 400),
        (format!("{base}?operation_id=a&operation_id=b"), 400),
        (format!("{base}?=value"), 400),
        (route(&f.own.id, Some("\n")), 409),
        (route(&f.own.id, Some(&too_long)), 409),
        ("/tasks/%00/calls".into(), 400),
        ("/tasks/%ZZ/calls".into(), 400),
        ("/tasks//calls".into(), 400),
    ] {
        let page = ui(c, "GET", &uri, vec![]).await;
        assert_eq!(page.status, status, "{uri}: {}", page.body);
        assert!(!page.body.contains("Original call and recorded outcome"));
        if status == 409 { assert!(page.body.contains("invalid_input"), "{}", page.body); }
    }
    let action_id = "call-view-is-not-a-mutation";
    let post = ui(c, "POST", &base, form(&[
        ("operation_id", action_id.into()), ("expected_revision", f.own.revision.to_string()),
    ])).await;
    assert_eq!(post.status, 404, "{}", post.body);
    let absent = exchange(c, Operation::GetOperation { operation_id: action_id.into() }).await;
    assert!(matches!(absent.outcome, Outcome::Refused { ref code, .. } if code == "not_found"));
    assert!(absent.commit.is_none());
    assert_eq!(read_snapshot(c, &f.own.id, &f.other.id).await, before);
}

#[tokio::test]
async fn bounded_call_projection_refuses_display_without_changing_its_real_stored_outcome() {
    let mut cfg = config();
    let limits = std::sync::Arc::make_mut(&mut cfg.limits);
    limits.controller.collaboration_page_bytes = 1;
    if let Some(workflow) = limits.workflow.as_mut() {
        workflow.page_bytes = 1;
    }
    cfg.limits.validate().unwrap();
    let f = calls_fixture(cfg).await;
    let c = &f.fixture.controller;
    let invocation = invocation(&f.caller, "call-view-too-large", "collab_memory_set", serde_json::json!({
        "key": "bounded-call", "expected_revision": null, "value": "real-committed-value",
    }));
    let original = exchange(c, invocation.operation()).await;
    assert!(matches!(original.outcome, Outcome::Ok { data: Data::Memory(Some(_)) }), "{original:?}");
    assert!(original.commit.is_some());
    let before = read_snapshot(c, &f.own.id, &f.other.id).await;
    let page = ui(c, "GET", &route(&f.own.id, Some(&invocation.operation_id)), vec![]).await;
    assert_eq!(page.status, 409, "{}", page.body);
    assert!(page.body.contains("response_bound"));
    assert!(!page.body.contains("real-committed-value"));
    assert!(!page.body.contains("Original call and recorded outcome"));
    let stored = exchange(c, Operation::GetOperation { operation_id: invocation.operation_id.clone() }).await;
    let Outcome::Ok { data: Data::Operation(record) } = stored.outcome else { panic!("actual original receipt") };
    assert_eq!(record.intended, serde_json::to_value(invocation.operation()).unwrap());
    let outcome: Outcome = serde_json::from_value(record.outcome.unwrap()).unwrap();
    assert_eq!(outcome, original.outcome);
    assert_eq!(read_snapshot(c, &f.own.id, &f.other.id).await, before);
}
