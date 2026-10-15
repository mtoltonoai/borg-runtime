//! Real private HTTP adapter checks, including query propagation to the UI.
use borg_runtime::{
    client::ApiClient,
    controller::{Controller, ControllerConfig},
    protocol::*,
    server,
};
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::test]
async fn malformed_body_is_not_reported_as_a_metadata_limit() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let root = std::env::temp_dir().join(format!(
        "borg-http-body-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    let controller = Controller::open_with_config(
        root.join("db"),
        ControllerConfig {
            validation_executable: env!("CARGO_BIN_EXE_borg-runtime").into(),
            ..ControllerConfig::default()
        },
    )
    .unwrap();
    let body_bound = controller.limits().transport.body_bytes;
    let listener = server::listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let origin = format!("http://{address}");
    let serve = tokio::spawn(server::serve(controller, listener, origin.clone()));
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(format!(
        "POST /api/operations HTTP/1.1\r\nHost: {address}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\nzz\r\n"
    ).as_bytes()).await.unwrap();
    let mut bytes = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        stream.read_to_end(&mut bytes),
    )
    .await
    .unwrap()
    .unwrap();
    let reply = String::from_utf8(bytes).unwrap();
    assert!(reply.starts_with("HTTP/1.1 400"), "{reply}");
    assert!(
        reply.contains("Invalid or interrupted request body"),
        "{reply}"
    );
    assert!(!reply.contains("transport.body_bytes"), "{reply}");
    let response = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .post(format!("{origin}/api/operations"))
        .body(vec![b'x'; body_bound + 1])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 413);
    assert!(
        response
            .text()
            .await
            .unwrap()
            .contains("transport.body_bytes")
    );
    serve.abort();
    let _ = serve.await;
}

#[tokio::test]
async fn explicit_header_count_metadata_replaces_the_library_default() {
    use borg_runtime::limits::RuntimeLimits;
    use std::sync::Arc;
    let root = std::env::temp_dir().join(format!(
        "borg-header-count-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    let mut limits = RuntimeLimits::seed().unwrap();
    limits.transport.http_header_count = 140;
    let controller = Controller::open_with_config(
        root.join("db"),
        ControllerConfig {
            limits: Arc::new(limits),
            validation_executable: env!("CARGO_BIN_EXE_borg-runtime").into(),
            ..ControllerConfig::default()
        },
    )
    .unwrap();
    let listener = server::listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let serve = tokio::spawn(server::serve(controller, listener, origin.clone()));
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    for (count, status) in [(120, 200), (160, 431)] {
        let mut request = client.get(format!("{origin}/health"));
        for i in 0..count {
            request = request.header(format!("x-fixture-{i}"), "1");
        }
        let reply = request.send().await.unwrap();
        assert_eq!(reply.status().as_u16(), status);
    }
    serve.abort();
    let _ = serve.await;
}

// Owned listener boundary only: authentication and SSH forwarding are deployment concerns.
//= spec/impl/v2/implementation.md#transport-and-access-controls-remain
//= type=test
//# An endpoint relying on the host's authenticated access MUST NOT be exposed directly on an untrusted network.
#[tokio::test]
async fn listener_refuses_non_loopback_addresses_before_binding() {
    for address in ["0.0.0.0:0", "[::]:0", "192.0.2.1:0", "[2001:db8::1]:0"] {
        let result = server::listen(address.parse().unwrap()).await;
        assert_eq!(
            result.unwrap_err(),
            "only private loopback listeners are supported by this adapter",
            "address: {address}"
        );
    }
}

#[tokio::test]
// Traceability scope: Current HTTP tests and older browser runs are different subjects; complete browser-created coding remains open.
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The collaboration product MUST provide a browser user interface.
async fn browser_pagination_and_private_boundary_use_the_actual_http_adapter() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let root = std::env::temp_dir().join(format!("borg-http-{}-{now}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let controller = Controller::open_with_config(
        root.join("tasks.sqlite"),
        ControllerConfig {
            validation_executable: std::path::PathBuf::from(env!("CARGO_BIN_EXE_borg-runtime")),
            ..ControllerConfig::default()
        },
    )
    .unwrap();
    let listener = server::listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let client = ApiClient::new(&origin, std::sync::Arc::new(controller.limits().clone())).unwrap();
    let serve = tokio::spawn(server::serve(controller, listener, origin.clone()));
    let mut ids = Vec::new();
    let mut sessions = std::collections::BTreeSet::new();
    for i in 0..51 {
        let input = TaskInput {
            resolved_limits: None,
            repository_read: None,
            instruction: format!("page-fixture-{i:02}"),
            definition: Definition {
                limit_overrides: None,
                inference: Some(InferenceSettings::selected()),
                execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
                id: format!("agent-{i}"),
                revision: "1".into(),
                executor: ExecutorKind::Native,
            },
            repository: None,
            acceptance_commands: vec![],
            output_schema: Some(serde_json::json!({"type":"string"})),
            limits: Limits {
                deadline_unix_ms: now + 60_000,
                model_requests: 1,
                output_bytes: 1024,
                result_bytes: borg_runtime::limits::RuntimeLimits::seed()
                    .unwrap()
                    .task_defaults
                    .result_bytes,
                max_delegation_depth: 0,
            },
            dependencies: vec![],
            native_workflow: None,
        };
        match client
            .call(Operation::SubmitTask {
                operation_id: format!("http-submit-{i}"),
                input,
            })
            .await
            .unwrap()
            .outcome
        {
            Outcome::Ok {
                data: Data::Task(t),
            } => {
                assert!(t.input.resolved_limits.is_some());
                t.input.execution_limits().unwrap();
                assert_eq!(t.assigned_agent_id, t.input.definition.id);
                assert_ne!(t.session_id, t.id);
                assert!(sessions.insert(t.session_id.clone()));
                ids.push(t.id.clone());
            }
            other => panic!("submit failed: {other:?}"),
        }
    }
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let response = http.get(format!("{origin}/tasks")).send().await.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["referrer-policy"], "same-origin");
    assert!(
        response.headers()["content-security-policy"]
            .to_str()
            .unwrap()
            .contains("default-src 'none'")
    );
    let page = response.text().await.unwrap();
    assert!(page.contains("<a href=\"/agents/agent-0\">agent-0</a></td>"));
    assert!(!page.contains("<a href=\"/agents/agent-50\">agent-50</a></td>"));
    let cursor = page
        .split("href=\"/tasks?after=")
        .nth(1)
        .unwrap()
        .split('"')
        .next()
        .unwrap();
    let next = http
        .get(format!("{origin}/tasks?after={cursor}"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(next.contains("<a href=\"/agents/agent-50\">agent-50</a></td>"));
    assert!(!next.contains("<a href=\"/agents/agent-0\">agent-0</a></td>"));
    let refused = http
        .post(format!("{origin}/tasks/new?after={cursor}"))
        .header("origin", &origin)
        .body("operation_id=bad-query")
        .send()
        .await
        .unwrap();
    assert!(refused.status().is_client_error());
    let refused = http
        .get(format!("{origin}/tasks"))
        .header("host", "foreign.example")
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), 400);
    let refused = http
        .post(format!("{origin}/api/operations"))
        .header("origin", "https://foreign.example")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), 403);
    let unchanged = client
        .call(Operation::GetTask {
            task_id: ids[0].clone(),
        })
        .await
        .unwrap();
    assert!(
        matches!(unchanged.outcome, Outcome::Ok { data: Data::Task(t) } if t.status == TaskStatus::Queued)
    );
    serve.abort();
    let _ = serve.await;
}
