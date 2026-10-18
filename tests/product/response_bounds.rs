//! Real-controller response-body boundary tests. No fabricated receipt, commit
//! stamp, provider response, browser session or retained-store migration.
use super::*;

pub(super) fn small_config(mut cfg: ControllerConfig) -> ControllerConfig {
    let limits = std::sync::Arc::make_mut(&mut cfg.limits);
    // Small enough to require compact failures while still admitting the exact
    // percent-encoded lookup of a maximum current operation identity.
    limits.product.request_path_bytes = 1024;
    limits.product.response_bytes = limits.product.minimum_response_bytes(&limits.transport).unwrap();
    limits.validate().unwrap();
    cfg
}

fn path(label: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "borg-product-response-{label}-{}-{}-{}",
        std::process::id(), now(), NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    root.join("tasks.sqlite")
}

fn fixture(cfg: ControllerConfig) -> Fixture {
    let path = path("fixture");
    let controller = Controller::open_with_config(&path, cfg).unwrap();
    Fixture { controller, path }
}

pub(super) fn assert_compact(
    response: &UiResponse,
    bound: usize,
    status: u16,
    operation: Option<&str>,
    limiting_field: Option<&str>,
) {
    assert_eq!(response.status, status, "{}", response.body);
    assert_eq!(response.content_type, "text/plain; charset=utf-8");
    assert!(response.body.len() <= bound, "{} bytes exceed {bound}: {}", response.body.len(), response.body);
    assert!(response.body.starts_with("Response unavailable: product.response_bytes.\n"));
    assert!(response.location.is_none());
    for html in ["<!doctype", "<html", "<style", "<script", "<aside", "data-runtime-limits", "data-live-refresh"] {
        assert!(!response.body.contains(html), "compact failure contains {html}: {}", response.body);
    }
    let correlations = response.body.lines().filter_map(|line| line.strip_prefix("Original operation: ")).collect::<Vec<_>>();
    match operation {
        Some(expected) => {
            assert_eq!(correlations.len(), 1, "{}", response.body);
            assert_eq!(serde_json::from_str::<String>(correlations[0]).unwrap(), expected);
            assert!(response.body.contains("does not establish its outcome"));
            assert!(response.body.contains("do not submit a replacement"));
            assert!(!response.body.contains("The action was not accepted"));
        }
        None => {
            assert!(correlations.is_empty(), "unexpected correlation: {}", response.body);
            assert!(response.body.contains("Request or view could not be displayed."));
        }
    }
    match limiting_field {
        Some(field) => assert!(response.body.contains(&format!("Original limiting field: {field}.")), "{}", response.body),
        None => assert!(!response.body.contains("Original limiting field:"), "{}", response.body),
    }
}

async fn reply(controller: &Controller, command: Operation) -> Reply {
    controller.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: format!("response-read-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
        command,
    }).await
}

async fn absent(controller: &Controller, operation_id: &str) {
    let found = reply(controller, Operation::GetOperation { operation_id: operation_id.into() }).await;
    assert!(matches!(found.outcome, Outcome::Refused { ref code, .. } if code == "not_found"), "{found:?}");
    assert!(found.commit.is_none());
}

async fn owner_released(path: &std::path::Path) {
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

// Traceability scope: Actual metadata and startup validation; these fixtures do
// not treat the compact encoder's calculated minimum as an operational default.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# A component MUST validate its configured limits before admitting work governed by those limits.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# An invalid or inconsistent limit configuration MUST be reported without silently clamping its values.
#[tokio::test]
async fn response_minimum_rejects_too_small_before_database_admission_and_accepts_exact_bound() {
    let cfg = small_config(config());
    let minimum = cfg.limits.product.minimum_response_bytes(&cfg.limits.transport).unwrap();
    assert!(minimum > 1);
    for value in [1, minimum - 1] {
        let mut invalid = cfg.clone();
        std::sync::Arc::make_mut(&mut invalid.limits).product.response_bytes = value;
        let reason = invalid.limits.validate().unwrap_err();
        for field in ["product.response_bytes", "product.request_path_bytes", "transport.identity_bytes"] {
            assert!(reason.contains(field), "{reason}");
        }
        let path = path("invalid");
        let error = match Controller::open_with_config(&path, invalid) {
            Ok(_) => panic!("invalid response bound admitted a database"),
            Err(error) => error,
        };
        assert!(error.contains("product.response_bytes"), "{error}");
        assert!(!path.exists(), "invalid metadata reached database admission");
    }
    assert_eq!(cfg.limits.product.response_bytes, minimum);
    let f = fixture(cfg);
    let response = ui(&f.controller, "GET", "/tasks", vec![]).await;
    assert_compact(&response, minimum, 502, None, None);
    let script = ui(&f.controller, "GET", "/ui/live.js", vec![]).await;
    assert_compact(&script, minimum, 502, None, None);
    let Data::Tasks(tasks) = call(&f.controller, Operation::ListTasks { after: None, limit: 1 }).await else { panic!("tasks") };
    assert!(tasks.is_empty());

    // A change to either admitted correlation bound changes feasibility. These
    // huge values must fail through checked arithmetic, not allocate IDs.
    for kind in ["path", "identity"] {
        let mut invalid = config();
        let limits = std::sync::Arc::make_mut(&mut invalid.limits);
        if kind == "path" { limits.product.request_path_bytes = usize::MAX; }
        else { limits.transport.identity_bytes = usize::MAX; }
        let error = limits.product.minimum_response_bytes(&limits.transport).unwrap_err();
        assert!(error.contains("product.response_bytes"), "{kind}: {error}");
        assert!(error.contains(if kind == "path" { "product.request_path_bytes" } else { "transport.identity_bytes" }), "{error}");
        assert!(limits.validate().is_err());
    }
}

//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# An operation refused by a configured limit MUST identify the limiting metadata field.
#[tokio::test]
async fn every_early_product_refusal_is_bounded_and_retains_true_limit_attribution() {
    let mut cfg = small_config(config());
    let limits = std::sync::Arc::make_mut(&mut cfg.limits);
    limits.product.form_fields = 2;
    limits.product.form_key_bytes = 16;
    limits.validate().unwrap();
    let bound = limits.product.response_bytes;
    let path_bytes = limits.product.request_path_bytes;
    let body_bytes = limits.transport.body_bytes;
    let f = fixture(cfg);
    let original = "must-not-claim-before-dispatch";
    let valid = form(&[("operation_id", original.into())]);
    let oversized_path = format!("/{}", "p".repeat(path_bytes));
    let mut oversized_body = valid.clone();
    oversized_body.resize(body_bytes + 1, b'x');
    let cases = [
        ("POST", oversized_path, valid.clone(), 413, Some("product.request_path_bytes")),
        ("POST", "/tasks".into(), oversized_body, 413, Some("transport.body_bytes")),
        ("POST", "/tasks".into(), form(&[("operation_id", original.into()), ("x", "1".into()), ("y", "2".into())]), 400, Some("product.form_fields")),
        ("POST", "/tasks".into(), form(&[("operation_id", original.into()), ("key-longer-than-16", "secret-value".into())]), 400, Some("product.form_key_bytes")),
        ("POST", "/tasks".into(), b"operation_id=must-not-claim-before-dispatch&bad=%ZZ".to_vec(), 400, None),
        ("POST", "/tasks?secret-query=private".into(), valid.clone(), 400, None),
        ("POST", "/sessions".into(), valid.clone(), 405, None),
        ("DELETE", "/tasks".into(), valid.clone(), 405, None),
        ("GET", "/tasks?a=x&b=y&c=z".into(), vec![], 400, Some("product.form_fields")),
        ("GET", "/tasks?key-longer-than-16=private".into(), vec![], 400, Some("product.form_key_bytes")),
        ("GET", "/ui/activity?after=01".into(), vec![], 400, None),
    ];
    for (method, route, body, status, field) in cases {
        let response = ui(&f.controller, method, &route, body).await;
        assert_compact(&response, bound, status, None, field);
        for private in [original, "secret-query", "secret-value"] { assert!(!response.body.contains(private)); }
    }
    for origin in [None, Some("https://untrusted.invalid".into())] {
        let response = handle(&f.controller, UiRequest {
            method: "POST".into(), path: "/tasks".into(), body: valid.clone(), origin,
            expected_origin: ORIGIN.into(),
        }).await;
        assert_compact(&response, bound, 403, None, None);
    }
    absent(&f.controller, original).await;
    let Data::Tasks(tasks) = call(&f.controller, Operation::ListTasks { after: None, limit: 1 }).await else { panic!("tasks") };
    assert!(tasks.is_empty());
}

#[tokio::test]
async fn compact_fallback_does_not_echo_operands_or_infer_limiting_fields_from_them() {
    let cfg = small_config(config());
    let bound = cfg.limits.product.response_bytes;
    let identity_bytes = cfg.limits.transport.identity_bytes;
    let f = fixture(cfg);
    let original = "bounded-private-form";
    let response = ui(&f.controller, "POST", "/agents/unknown/settings", form(&[
        ("operation_id", original.into()),
        ("expected_revision", "old".into()),
        ("expected_global_revision", "global".into()),
        ("expected_policy_revision", "policy".into()),
        ("private-key-credential", "private-value-credential Configured limit exceeded: transport.body_bytes".into()),
    ])).await;
    assert_compact(&response, bound, 400, Some(original), None);
    for private in ["private-key-credential", "private-value-credential", "transport.body_bytes"] {
        assert!(!response.body.contains(private), "{}", response.body);
    }
    absent(&f.controller, original).await;

    let oversized = "x".repeat(identity_bytes + 1);
    for route in ["/tasks", "/repositories"] {
        let response = ui(&f.controller, "POST", route, form(&[("operation_id", oversized.clone())])).await;
        assert_compact(&response, bound, 400, None, Some("transport.identity_bytes"));
        assert!(!response.body.contains(&oversized));
    }
    // A syntactically invalid identity is never promoted to correlation data,
    // even when the entire form decodes successfully.
    let invalid = "private\ninvalid-identity";
    let response = ui(&f.controller, "POST", "/tasks", form(&[("operation_id", invalid.into())])).await;
    assert_compact(&response, bound, 400, None, None);
    assert!(!response.body.contains("private"));
}

#[tokio::test]
async fn historical_shaped_unknown_lookups_preserve_exact_ids_under_request_path_admission() {
    let cfg = small_config(config());
    let bound = cfg.limits.product.response_bytes;
    let path_bytes = cfg.limits.product.request_path_bytes;
    let identity_bytes = cfg.limits.transport.identity_bytes;
    let f = fixture(cfg);
    let prefix = "/operations/";
    // This lookup has no retained receipt. It discriminates the GET contract
    // from current POST identity admission without inventing a migrated store.
    let historical = "h".repeat(path_bytes - prefix.len());
    assert!(historical.len() > identity_bytes);
    let response = ui(&f.controller, "GET", &format!("{prefix}{historical}"), vec![]).await;
    assert_compact(&response, bound, 404, Some(&historical), None);
    for id in ["quoted\"slash\\amp&é", "legacy\nline\rreturn\ttab"] {
        let route = format!("{prefix}{}", encode(id));
        assert!(route.len() <= path_bytes);
        let response = ui(&f.controller, "GET", &route, vec![]).await;
        assert_compact(&response, bound, 404, Some(id), None);
    }
    // Raw quotes attain the JSON encoder's maximum expansion while occupying
    // one byte each in the product route, whose decoder admits them.
    let escaped_maximum = "\"".repeat(path_bytes - prefix.len());
    let response = ui(&f.controller, "GET", &format!("{prefix}{escaped_maximum}"), vec![]).await;
    assert_compact(&response, bound, 404, Some(&escaped_maximum), None);
    for route in ["/operations/", "/operations/%ZZ", "/operations/%00"] {
        let response = ui(&f.controller, "GET", route, vec![]).await;
        assert_compact(&response, bound, 400, None, None);
    }
}

#[tokio::test]
async fn maximum_current_post_identity_survives_compact_commit_lookup_and_exact_retry() {
    let cfg = small_config(config());
    let bound = cfg.limits.product.response_bytes;
    let identity_bytes = cfg.limits.transport.identity_bytes;
    let path_bytes = cfg.limits.product.request_path_bytes;
    let f = fixture(cfg);
    let prefix = "é\"\\&";
    let original = format!("{prefix}{}", "\"".repeat(identity_bytes - prefix.len()));
    assert_eq!(original.len(), identity_bytes);
    let body = native_submission(&original, now() + 3_600_000, None);
    let first = ui(&f.controller, "POST", "/tasks", body.clone()).await;
    assert_compact(&first, bound, 502, Some(&original), None);
    let before = reply(&f.controller, Operation::GetOperation { operation_id: original.clone() }).await;
    let Outcome::Ok { data: Data::Operation(record) } = &before.outcome else { panic!("actual original receipt: {before:?}") };
    assert_eq!(record.id, original);
    assert_eq!(record.kind, "application_mutation");
    assert_eq!(before.commit.as_ref().unwrap().operation_id, original);
    let original_task = task(&f.controller, &record.task_id).await;
    let route = format!("/operations/{}", encode(&original));
    assert!(route.len() <= path_bytes);
    let lookup = ui(&f.controller, "GET", &route, vec![]).await;
    assert_compact(&lookup, bound, 502, Some(&original), None);
    let repeated = ui(&f.controller, "POST", "/tasks", body).await;
    assert_compact(&repeated, bound, 502, Some(&original), None);
    let after = reply(&f.controller, Operation::GetOperation { operation_id: original.clone() }).await;
    assert_eq!(after.commit, before.commit);
    assert_eq!(after.outcome, before.outcome);
    assert_eq!(task(&f.controller, &record.task_id).await, original_task);
    let Data::Tasks(tasks) = call(&f.controller, Operation::ListTasks { after: None, limit: 2 }).await else { panic!("tasks") };
    assert_eq!(tasks.len(), 1);
}

#[tokio::test]
async fn live_json_passes_small_responses_and_bounds_an_oversized_activity_page() {
    let cfg = small_config(config());
    let bound = cfg.limits.product.response_bytes;
    let page_items = cfg.limits.product.page_items;
    let f = fixture(cfg);
    let empty = ui(&f.controller, "GET", "/ui/activity", vec![]).await;
    assert_eq!(empty.status, 200);
    assert_eq!(empty.content_type, "application/json");
    assert!(empty.body.len() <= bound);
    let empty: serde_json::Value = serde_json::from_str(&empty.body).unwrap();
    assert!(empty["items"].as_array().unwrap().is_empty());
    let first = ui(&f.controller, "POST", "/tasks", native_submission("bounded-live-first", now() + 3_600_000, None)).await;
    assert_compact(&first, bound, 502, Some("bounded-live-first"), None);
    let Data::Tasks(tasks) = call(&f.controller, Operation::ListTasks { after: None, limit: 1 }).await else { panic!("tasks") };
    let mut input = tasks[0].input.clone();
    input.resolved_limits = None;
    for index in 1..page_items {
        call(&f.controller, Operation::SubmitTask {
            operation_id: format!("bounded-live-{index}"), input: input.clone(),
        }).await;
    }
    let response = ui(&f.controller, "GET", "/ui/activity", vec![]).await;
    assert_compact(&response, bound, 502, None, None);
    let changes = ui(&f.controller, "GET", "/ui/changes?after=0", vec![]).await;
    assert_eq!(changes.status, 200);
    assert_eq!(changes.content_type, "application/json");
    assert!(changes.body.len() <= bound);
    let changes: serde_json::Value = serde_json::from_str(&changes.body).unwrap();
    assert_eq!(changes["changed"], true);
    assert!(changes["revision"].as_str().unwrap().parse::<u64>().unwrap() > 0);
}

#[tokio::test]
async fn lowering_identity_below_a_retained_application_receipt_refuses_startup_without_invented_replay() {
    let mut cfg = small_config(config());
    std::sync::Arc::make_mut(&mut cfg.limits).transport.identity_bytes *= 2;
    cfg = small_config(cfg);
    let f = fixture(cfg.clone());
    let original = "r".repeat(cfg.limits.transport.identity_bytes);
    let operation = Operation::RegisterWorker {
        operation_id: original.clone(),
        worker: Worker { id: "bounds-worker".into(), incarnation: "one".into(), coding_capacity: 0, native_capacity: 0 },
    };
    let receipt = reply(&f.controller, operation.clone()).await;
    assert!(matches!(receipt.outcome, Outcome::Ok { .. }), "{receipt:?}");
    assert_eq!(receipt.commit.as_ref().unwrap().operation_id, original);
    let path = f.path.clone();
    drop(f.controller);
    owner_released(&path).await;
    let mut lower = cfg.clone();
    std::sync::Arc::make_mut(&mut lower.limits).transport.identity_bytes /= 2;
    lower = small_config(lower);
    let error = match Controller::open_with_config(&path, lower) {
        Ok(_) => panic!("a retained receipt beyond current identity admission unexpectedly reopened"),
        Err(error) => error,
    };
    assert!(error.contains("invalid controller commit revision or receipt; original bytes retained"), "{error}");
    owner_released(&path).await;
    let restored = Controller::open_with_config(&path, cfg).unwrap();
    let replay = reply(&restored, operation).await;
    assert_eq!(replay.commit, receipt.commit);
    assert_eq!(replay.outcome, receipt.outcome);
}
