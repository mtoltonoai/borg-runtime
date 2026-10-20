//! Real HTTP routes and durable session state with deterministic model/tool peers.
#[allow(dead_code)]
#[path = "../src/main.rs"]
mod service;

use native_runtime::*;
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Barrier, Semaphore};

type ResponseFuture = BoxFuture<'static, Result<ProviderResponse, ProviderError>>;
struct FakeProvider(Box<dyn Fn(ProviderRequest) -> ResponseFuture + Send + Sync>);
impl Provider for FakeProvider {
    fn respond(
        &self,
        request: ProviderRequest,
    ) -> BoxFuture<'_, Result<ProviderResponse, ProviderError>> {
        (self.0)(request)
    }
}
fn provider(
    f: impl Fn(ProviderRequest) -> ResponseFuture + Send + Sync + 'static,
) -> Arc<dyn Provider> {
    Arc::new(FakeProvider(Box::new(f)))
}
fn answer(text: &str) -> ProviderResponse {
    ProviderResponse {
        id: Some("fixture-response".into()),
        status: "completed".into(),
        output: vec![
            json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":text}]}),
        ],
        usage: Some(json!({"input_tokens":1,"output_tokens":1})),
    }
}
fn latest_input(request: &ProviderRequest) -> String {
    request
        .input
        .iter()
        .rev()
        .find(|v| v["role"] == "user")
        .and_then(|v| v["content"].as_str())
        .unwrap()
        .to_owned()
}
struct FakeTools {
    calls: AtomicUsize,
}
impl ToolExecutor for FakeTools {
    fn list_tools<'a>(
        &'a self,
        _: &'a str,
        _: &'a [McpServerConfig],
    ) -> BoxFuture<'a, Result<Vec<Value>, String>> {
        Box::pin(async {
            Ok(vec![
                json!({"type":"function","name":"fixture_tool","description":"Controlled tool","parameters":{"type":"object"}}),
            ])
        })
    }
    fn call<'a>(
        &'a self,
        _: &'a str,
        _: &'a [McpServerConfig],
        _: &'a str,
        _: Value,
        _: &'a str,
    ) -> BoxFuture<'a, Result<Value, String>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err("Tool connection closed before its result was observed".into()) })
    }
}
fn fixture(peer: Arc<dyn Provider>, tools: Arc<dyn ToolExecutor>) -> (tempfile::TempDir, Runtime) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.json");
    std::fs::write(&path, json!({
        "database_path":"state.sqlite",
        "agents":[{"id":"engineer","instructions":"Finish the requested work.","api_key_env":"PRIVATE_TEST_KEY_REFERENCE"}]
    }).to_string()).unwrap();
    let runtime = Runtime::with_backends(path, peer, tools).unwrap();
    (dir, runtime)
}
fn tools() -> Arc<FakeTools> {
    Arc::new(FakeTools {
        calls: AtomicUsize::new(0),
    })
}
struct Server {
    url: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn server(runtime: Runtime, token: Option<String>) -> Server {
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", socket.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(socket, service::router(runtime, token))
            .await
            .unwrap();
    });
    Server { url, task }
}
async fn create(client: &reqwest::Client, server: &Server, text: &str) -> Value {
    let response = client.post(format!("{}/api/sessions", server.url))
        .json(&json!({"agent_id":"engineer","input":text,"task":format!("https://tasks.example/{text}")}))
        .send().await.unwrap();
    assert_eq!(response.status(), 201);
    response.json().await.unwrap()
}
async fn state(runtime: &Runtime, id: &str, expected: SessionStatus) -> Session {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let value = runtime.get_session(id).unwrap();
            if value.status == expected {
                return value;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("Expected {expected:?}; got {:?}", runtime.get_session(id)))
}

#[tokio::test]
async fn http_validation_authentication_and_full_size_input() {
    let (_dir, runtime) = fixture(
        provider(|_| Box::pin(async { Ok(answer("done")) })),
        tools(),
    );
    let server = server(runtime.clone(), Some("test-token".into())).await;
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .get(format!("{}/health", server.url))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client
            .get(format!("{}/api/agents", server.url))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let response = client
        .get(format!("{}/api/agents", server.url))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap();
    let agents: Value = response.json().await.unwrap();
    assert_eq!(agents[0]["model"], "gpt-6-astra");
    assert_eq!(agents[0]["max_output_tokens"], 128_000);
    assert_eq!(agents[0]["context_window_tokens"], 1_050_000);
    assert!(agents[0].get("provider_url").is_none());
    assert!(agents[0].get("api_key_env").is_none());
    for (body, status) in [
        (json!({"agent_id":"unknown","input":"work"}), 400),
        (json!({"agent_id":"engineer","input":" "}), 400),
        (
            json!({"agent_id":"engineer","input":"work","api_key":"must-not-be-reflected"}),
            422,
        ),
        (json!({"agent_id":"engineer"}), 422),
        (json!(["engineer", null, "work"]), 400),
    ] {
        let response = client
            .post(format!("{}/api/sessions", server.url))
            .bearer_auth("test-token")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        assert!(
            !response
                .text()
                .await
                .unwrap()
                .contains("must-not-be-reflected")
        );
    }
    assert!(runtime.list_sessions().unwrap().is_empty());
    // Exceeds Axum's default 2 MiB body limit without reducing the model allowance.
    let input = "x".repeat(2 * 1024 * 1024 + 100);
    let response = client
        .post(format!("{}/api/sessions", server.url))
        .bearer_auth("test-token")
        .json(&json!({"agent_id":"engineer","input":input}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    let created: Value = response.json().await.unwrap();
    state(
        &runtime,
        created["id"].as_str().unwrap(),
        SessionStatus::Completed,
    )
    .await;
    let response = client
        .get(format!("{}/api/sessions/absent", server.url))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 404);
    let mut config: RuntimeConfig =
        serde_json::from_value(json!({"database_path":"unused","agents":[]})).unwrap();
    assert_eq!(
        service::listen_policy(&config).unwrap().0.to_string(),
        "127.0.0.1:8787"
    );
    config.listen = "0.0.0.0:8787".into();
    assert!(service::listen_policy(&config).is_err());
}

#[tokio::test]
async fn two_sessions_of_one_agent_run_independently_and_in_parallel() {
    let barrier = Arc::new(Barrier::new(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let peer = provider({
        let calls = calls.clone();
        move |request| {
            let barrier = barrier.clone();
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                barrier.wait().await;
                Ok(answer(&format!("finished {}", latest_input(&request))))
            })
        }
    });
    let (_dir, runtime) = fixture(peer, tools());
    let server = server(runtime.clone(), None).await;
    let client = reqwest::Client::new();
    let first = create(&client, &server, "first").await;
    let second = create(&client, &server, "second").await;
    assert_ne!(first["id"], second["id"]);
    assert_eq!(first["agent_id"], second["agent_id"]);
    for (value, own, other) in [(first, "first", "second"), (second, "second", "first")] {
        let id = value["id"].as_str().unwrap();
        let completed = state(&runtime, id, SessionStatus::Completed).await;
        assert_eq!(completed.turns, 1);
        let response = client
            .get(format!("{}/api/sessions/{id}/transcript", server.url))
            .send()
            .await
            .unwrap();
        let transcript: Value = response.json().await.unwrap();
        assert!(transcript.to_string().contains(&format!("finished {own}")));
        assert!(
            !transcript
                .to_string()
                .contains(&format!("finished {other}"))
        );
        assert_eq!(
            completed.task.as_deref(),
            Some(format!("https://tasks.example/{own}").as_str())
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn stop_resume_and_steer_preserve_session_and_committed_inputs() {
    let entered = Arc::new(Semaphore::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let peer = provider({
        let entered = entered.clone();
        let calls = calls.clone();
        move |request| {
            let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
            let entered = entered.clone();
            Box::pin(async move {
                if first {
                    entered.add_permits(1);
                    std::future::pending::<()>().await;
                }
                Ok(answer(&latest_input(&request)))
            })
        }
    });
    let (_dir, runtime) = fixture(peer, tools());
    let server = server(runtime.clone(), None).await;
    let client = reqwest::Client::new();
    let created = create(&client, &server, "initial").await;
    let id = created["id"].as_str().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert_eq!(
        client
            .post(format!("{}/api/sessions/{id}/stop", server.url))
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    state(&runtime, id, SessionStatus::Stopped).await;
    assert_eq!(
        client
            .post(format!("{}/api/sessions/{id}/resume", server.url))
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    state(&runtime, id, SessionStatus::Completed).await;
    assert_eq!(
        client
            .post(format!("{}/api/sessions/{id}/steer", server.url))
            .json(&json!({"text":"follow-up"}))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    state(&runtime, id, SessionStatus::Completed).await;
    let inputs: Vec<_> = runtime
        .transcript(id)
        .unwrap()
        .into_iter()
        .filter(|e| e.kind == "input")
        .map(|e| e.item["content"].clone())
        .collect();
    assert_eq!(inputs, vec![json!("initial"), json!("follow-up")]);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn unknown_tool_requires_explicit_result_without_another_tool_dispatch() {
    let peer = provider(|request| {
        Box::pin(async move {
            if request
                .input
                .iter()
                .any(|v| v["type"] == "function_call_output")
            {
                return Ok(answer("resolved"));
            }
            Ok(ProviderResponse {
                id: Some("tool-response".into()),
                status: "completed".into(),
                usage: None,
                output: vec![
                    json!({"type":"function_call","name":"fixture_tool","call_id":"original-call","arguments":"{}"}),
                ],
            })
        })
    });
    let tools = tools();
    let (_dir, runtime) = fixture(peer, tools.clone());
    let server = server(runtime.clone(), None).await;
    let client = reqwest::Client::new();
    let created = create(&client, &server, "tool").await;
    let id = created["id"].as_str().unwrap();
    state(&runtime, id, SessionStatus::NeedsResolution).await;
    let response = client
        .get(format!("{}/api/sessions/{id}/tools", server.url))
        .send()
        .await
        .unwrap();
    let recorded: Value = response.json().await.unwrap();
    assert_eq!(recorded[0]["call_id"], "original-call");
    assert_eq!(recorded[0]["status"], "unknown");
    let missing_result = client
        .post(format!("{}/api/sessions/{id}/resolve-tool", server.url))
        .json(&json!({"call_id":"original-call"}))
        .send()
        .await
        .unwrap();
    assert_eq!(missing_result.status(), 422);
    assert_eq!(runtime.get_session(id).unwrap().status, SessionStatus::NeedsResolution);
    assert_eq!(
        client
            .post(format!("{}/api/sessions/{id}/resume", server.url))
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    let observed = json!({"observed":"original external result"});
    let response = client
        .post(format!("{}/api/sessions/{id}/resolve-tool", server.url))
        .json(&json!({"call_id":"original-call","result":observed}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    state(&runtime, id, SessionStatus::Completed).await;
    assert_eq!(tools.calls.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.tool_calls(id).unwrap()[0].result, Some(observed));
}

#[tokio::test]
async fn startup_drives_ready_sessions_and_leaves_stopped_sessions_parked() {
    let calls = Arc::new(AtomicUsize::new(0));
    let peer = provider({
        let calls = calls.clone();
        move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(answer("startup")) })
        }
    });
    let (_dir, runtime) = fixture(peer, tools());
    let ready = runtime
        .create_session("engineer", None, "ready before listen")
        .unwrap();
    let parked = runtime
        .create_session("engineer", None, "parked before listen")
        .unwrap();
    runtime.stop(&parked.id).unwrap();
    service::resume_ready(&runtime).unwrap();
    state(&runtime, &ready.id, SessionStatus::Completed).await;
    assert_eq!(
        runtime.get_session(&parked.id).unwrap().status,
        SessionStatus::Stopped
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
