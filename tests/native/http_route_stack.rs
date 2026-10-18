//! Actual normal controller HTTP routes before any worker or provider exists.
//! Reuses the owned native test process; preserves the seed runtime stack policy.
use super::*;

fn section_json(body: &str, heading: &str) -> Value {
    // Same rendered-metadata decoder as the existing product limits test.
    let encoded = body.split_once(heading).expect("metadata section").1
        .split_once("<pre>").unwrap().1.split_once("</pre>").unwrap().0;
    let decoded = encoded.replace("&quot;", "\"").replace("&#39;", "'")
        .replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&");
    serde_json::from_str(&decoded).expect("rendered metadata JSON")
}

async fn bounded_body(mut response: reqwest::Response, bound: usize) -> String {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.unwrap() {
        assert!(body.len().checked_add(chunk.len()).is_some_and(|n| n <= bound),
            "HTTP response exceeds selected product.response_bytes");
        body.extend_from_slice(&chunk);
    }
    String::from_utf8(body).unwrap()
}

async fn page(http: &reqwest::Client, lab: &Lab, path: &str) -> String {
    let response = http.get(format!("{}{path}", lab.url)).send().await.unwrap();
    assert_eq!(response.status(), 200, "GET {path}");
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["referrer-policy"], "same-origin");
    bounded_body(response, lab.limits.product.response_bytes).await
}

fn task_bounds(body: &str, task: &Task) {
    let section = body.split_once("<h2>Accepted task limits</h2>").unwrap().1
        .split_once("</dl>").unwrap().0;
    for (label, value) in [
        ("Deadline (Unix milliseconds)", task.input.limits.deadline_unix_ms.to_string()),
        ("Model requests", task.input.limits.model_requests.to_string()),
        ("Output bytes", task.input.limits.output_bytes.to_string()),
        ("Complete result bytes", task.input.limits.result_bytes.to_string()),
        ("Maximum delegation depth", task.input.limits.max_delegation_depth.to_string()),
    ] {
        assert!(section.contains(&format!("<dt>{label}</dt><dd>{value}</dd>")), "{label}");
    }
    let accepted = task.input.resolved_limits.as_ref().unwrap();
    assert!(section.contains(&format!("<dt>Accepted global metadata revision</dt><dd>{}</dd>", accepted.global_revision)));
    assert!(section.contains(&format!("<dt>Accepted agent revision</dt><dd>{}</dd>", accepted.agent_revision)));
}

// Traceability scope: A real default-stack controller serves retained metadata
// and ordinary task routes before any attempt; this is not a provider test.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# The browser interface MUST expose the effective limits applicable to displayed work.
#[tokio::test]
async fn actual_http_limits_and_task_routes_before_any_attempt_preserve_original_state() {
    let lab = Lab::new("http-route-default-stack");
    assert!(lab.limits.runtime.thread_stack_bytes.is_none());
    assert_eq!(serde_json::to_value(&lab.limits.runtime).unwrap(),
        serde_json::to_value(&RuntimeLimits::seed().unwrap().runtime).unwrap());
    let input = lab.input(1);
    let remaining = input.limits.deadline_unix_ms.checked_sub(process::now_ms()).unwrap();
    let journey = async {
        let mut controller = lab.controller("controller").await;
        let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
        let accepted = submit_identified(&api, input, "http-route-submit").await;
        assert_eq!(accepted.status, TaskStatus::Queued);
        assert!(accepted.attempt.is_none());
        let http = reqwest::Client::builder().no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_millis(lab.limits.transport.client_connect_timeout_ms))
            .timeout(Duration::from_millis(lab.limits.transport.client_timeout_ms))
            .build().unwrap();

        // This is deliberately the first browser route, before any live attempt.
        // Only the controller has been launched; no worker or model peer exists.
        let selected_path = format!("/limits?task_id={}", accepted.id);
        let selected = page(&http, &lab, &selected_path).await;
        assert_eq!(section_json(&selected, "<h2>Current global metadata</h2>"), json!(&*lab.limits));
        assert_eq!(section_json(&selected, "<h2>Recorded task execution metadata</h2>"),
            json!(accepted.input.resolved_limits.as_ref().unwrap()));
        task_bounds(&selected, &accepted);
        std::fs::write(lab.root.join("selected-metadata.html"), &selected).unwrap();

        let global = page(&http, &lab, "/limits").await;
        assert_eq!(section_json(&global, "<h2>Current global metadata</h2>"), json!(&*lab.limits));
        assert!(!global.contains("<h2>Recorded task execution metadata</h2>"));
        assert!(!global.contains("<h2>Accepted task limits</h2>"));
        std::fs::write(lab.root.join("global-metadata.html"), &global).unwrap();

        // Fixed representatives of remaining ordinary route groups. No adaptive
        // route sweep, alternate caller, task creation or mutation is hidden here.
        let routes = [
            ("/tasks".to_string(), accepted.id.clone()),
            (format!("/tasks/{}", accepted.id), accepted.id.clone()),
            (format!("/tasks/new?agent_id={}", accepted.assigned_agent_id), accepted.assigned_agent_id.clone()),
            (format!("/agents/{}", accepted.assigned_agent_id), accepted.assigned_agent_id.clone()),
            (format!("/sessions/{}", accepted.session_id), accepted.session_id.clone()),
            ("/operations/http-route-submit".to_string(), "http-route-submit".to_string()),
        ];
        for (path, identity) in &routes {
            assert!(page(&http, &lab, path).await.contains(identity), "GET {path} lost identity");
        }
        assert_eq!(get(&api, &accepted.id).await, accepted, "reads changed accepted task");

        let cancel_path = format!("/tasks/{}/cancel", accepted.id);
        let refused_id = "http-route-invalid-origin";
        let response = http.post(format!("{}{cancel_path}", lab.url))
            .header("origin", "http://foreign.invalid")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(format!("operation_id={refused_id}&expected_revision={}", accepted.revision))
            .send().await.unwrap();
        assert_eq!(response.status(), 403);
        let refused_body = bounded_body(response, lab.limits.product.response_bytes).await;
        assert!(refused_body.contains("configured same origin"));
        assert_eq!(get(&api, &accepted.id).await, accepted, "foreign origin mutated task");
        let missing_op = Operation::GetOperation { operation_id: refused_id.into() };
        let missing = api.call(missing_op.clone()).await.unwrap();
        missing.validate_for(&missing_op).unwrap();
        assert!(matches!(&missing.outcome, Outcome::Refused { code, .. } if code == "not_found"));
        assert!(missing.commit.is_none());

        let cancel_id = "http-route-cancel";
        let intended = Operation::CancelTask { operation_id: cancel_id.into(),
            task_id: accepted.id.clone(), expected_revision: accepted.revision };
        let response = http.post(format!("{}{cancel_path}", lab.url))
            .header("origin", &lab.url)
            .header("content-type", "application/x-www-form-urlencoded")
            .body(format!("operation_id={cancel_id}&expected_revision={}", accepted.revision))
            .send().await.unwrap();
        assert_eq!(response.status(), 303);
        assert_eq!(response.headers()["location"].to_str().unwrap(), format!("/tasks/{}", accepted.id));
        assert!(bounded_body(response, lab.limits.product.response_bytes).await.contains(cancel_id));
        let cancelled = get(&api, &accepted.id).await;
        assert_eq!(cancelled.status, TaskStatus::Cancelled);
        assert!(cancelled.attempt.is_none());
        assert_eq!(cancelled.input, accepted.input);
        assert_eq!(cancelled.session_id, accepted.session_id);
        assert_eq!(cancelled.assigned_agent_id, accepted.assigned_agent_id);

        let lookup = Operation::GetOperation { operation_id: cancel_id.into() };
        let reply = api.call(lookup.clone()).await.unwrap();
        reply.validate_for(&lookup).unwrap();
        let Outcome::Ok { data: Data::Operation(record) } = &reply.outcome else { panic!("original cancel receipt: {reply:?}") };
        assert_eq!(record.id, cancel_id);
        assert_eq!(record.task_id, accepted.id);
        assert_eq!(record.kind, "application_mutation");
        assert_eq!(record.state, "settled");
        assert_eq!(record.intended, json!(&intended));
        assert_eq!(record.outcome, Some(json!(Outcome::Ok { data: Data::Task(Box::new(cancelled.clone())) })));
        let commit = reply.commit.as_ref().unwrap();
        assert_eq!(commit.operation_id, cancel_id);
        assert!(commit.controller_revision > 0);
        assert!(reply.observed_controller_revision.unwrap() >= commit.controller_revision);
        let original = page(&http, &lab, &format!("/operations/{cancel_id}")).await;
        assert!(original.contains(cancel_id));
        assert!(original.contains("Original committed controller revision"));
        assert!(original.contains(&commit.controller_revision.to_string()));
        assert_eq!(get(&api, &accepted.id).await, cancelled, "receipt read changed task");
        assert!(controller.child.try_wait().unwrap().is_none(), "controller died after browser routes");

        std::fs::write(lab.root.join("http-route-evidence.json"), serde_json::to_vec_pretty(&json!({
            "selected_path":selected_path,"remaining_get_routes":routes,
            "accepted":accepted,"cancelled":cancelled,"original_cancel_receipt":reply,
            "invalid_origin_status":403,"worker_started":false,"provider_started":false,
            "runtime_policy":&lab.limits.runtime
        })).unwrap()).unwrap();
        controller.stop().await;
        drop(controller); // Existing owner records exact terminal exit and reaping.
        let db = rusqlite::Connection::open_with_flags(lab.root.join("tasks.sqlite"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        for (table, expected) in [("tasks",1_i64),("sessions",1),("attempts",0),("workers",0)] {
            assert_eq!(db.query_row(&format!("SELECT count(*) FROM {table}"), [],
                |row| row.get::<_,i64>(0)).unwrap(), expected, "{table}");
        }
        let (request, outcome, revision): (String,String,i64) = db.query_row(
            "SELECT request,outcome,controller_revision FROM receipts WHERE id=?1", [cancel_id],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?))).unwrap();
        assert_eq!(serde_json::from_str::<Value>(&request).unwrap(), json!(&intended));
        assert_eq!(serde_json::from_str::<Value>(&outcome).unwrap(), record.outcome.clone().unwrap());
        assert_eq!(u64::try_from(revision).unwrap(), commit.controller_revision);
        assert_eq!(db.query_row("SELECT count(*) FROM receipts WHERE id=?1", [refused_id],
            |row| row.get::<_,i64>(0)).unwrap(), 0);
    };
    tokio::time::timeout(Duration::from_millis(remaining), Box::pin(journey))
        .await.expect("bounded controller-only HTTP route regression");
}
