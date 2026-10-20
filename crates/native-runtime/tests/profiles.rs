//! Profile authority, dispatch idempotency, same-identity isolation and reporting
//! against real HTTP Board boundaries, with deterministic model/tool peers.
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::get,
};
use native_runtime::*;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Barrier, Notify, Semaphore};

#[derive(Default)]
struct BoardState {
    identity: Mutex<Value>,
    runtime: Mutex<Value>,
    tools: Mutex<Value>,
    sessions: Mutex<HashMap<String, Value>>,
    catalog: Mutex<HashMap<String, Value>>,
    read_fail: AtomicBool,
    write_fail: AtomicBool,
    reads: AtomicUsize,
}
struct Board {
    url: String,
    state: Arc<BoardState>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Board {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Board {
    async fn new() -> Self {
        let state = Arc::new(BoardState::default());
        *state.identity.lock().unwrap() = json!({"id":"engineer","charter":"Board charter v1","lifecycle_intent":"run","metadata":{"role":"engineering","model":"board-model"}});
        *state.runtime.lock().unwrap() = json!({"role":"engineering","role_version":1,"agent_version":0,"entries":[{"id":"profile","enabled":true,"payload":{"retry_initial_ms":1,"retry_max_ms":2}}]});
        *state.tools.lock().unwrap() = json!({"role_version":1,"agent_version":0,"entries":[
            {"id":"workspace","enabled":true,"payload":{"kind":"mcp","name":"workspace","url":"http://unused/{session_id}","tools_allowlist":["write"]}},
            {"id":"native","enabled":true,"payload":{"kind":"mcp","name":"native","url":"{runtime_url}/mcp/sessions/{session_id}"}}
        ]});
        async fn identity(
            State(s): State<Arc<BoardState>>,
            Path(id): Path<String>,
        ) -> Result<Json<Value>, StatusCode> {
            s.reads.fetch_add(1, Ordering::SeqCst);
            if s.read_fail.load(Ordering::SeqCst) {
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
            if id != "engineer" {
                return Err(StatusCode::NOT_FOUND);
            }
            Ok(Json(s.identity.lock().unwrap().clone()))
        }
        async fn config(
            State(s): State<Arc<BoardState>>,
            Path((_id, kind)): Path<(String, String)>,
        ) -> Result<Json<Value>, StatusCode> {
            if s.read_fail.load(Ordering::SeqCst) {
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
            Ok(Json(if kind == "runtime" {
                s.runtime.lock().unwrap().clone()
            } else {
                s.tools.lock().unwrap().clone()
            }))
        }
        async fn state_read(
            State(s): State<Arc<BoardState>>,
            Path(id): Path<String>,
        ) -> Result<Json<Value>, StatusCode> {
            if s.read_fail.load(Ordering::SeqCst) {
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
            Ok(Json(
                s.sessions
                    .lock()
                    .unwrap()
                    .get(&id)
                    .cloned()
                    .unwrap_or(Value::Null),
            ))
        }
        async fn state_write(
            State(s): State<Arc<BoardState>>,
            Path(id): Path<String>,
            Json(body): Json<Value>,
        ) -> Result<Json<Value>, StatusCode> {
            if s.write_fail.load(Ordering::SeqCst) {
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
            let mut states = s.sessions.lock().unwrap();
            if let Some(old) = states.get(&id) {
                if old["generation"].as_u64() > body["generation"].as_u64() {
                    return Err(StatusCode::FORBIDDEN);
                }
                if old["generation"] == body["generation"] {
                    assert!(old["step"].as_u64() <= body["step"].as_u64());
                }
            }
            states.insert(id, body.clone());
            Ok(Json(body))
        }
        async fn catalog(
            State(s): State<Arc<BoardState>>,
            Json(body): Json<Value>,
        ) -> Result<Json<Value>, StatusCode> {
            if s.write_fail.load(Ordering::SeqCst) {
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
            s.catalog.lock().unwrap().insert(
                body["entry_id"].as_str().unwrap().into(),
                body["payload"].clone(),
            );
            Ok(Json(body))
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/api/agents/{id}", get(identity))
            .route("/api/agents/{id}/config/{kind}", get(config).post(catalog))
            .route(
                "/api/sessions/{id}/state",
                get(state_read).post(state_write),
            )
            .with_state(state.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self { url, state, task }
    }
}
fn config_file(dir: &tempfile::TempDir, board: &str) -> PathBuf {
    let path = dir.path().join("runtime.json");
    std::fs::write(&path,serde_json::to_vec(&json!({"database_path":dir.path().join("sessions.sqlite"),"board":{"url":board,"timeout_ms":1000},"host_id":"lab-test","runtime_url":"http://runtime-test","agents":[]})).unwrap()).unwrap();
    path
}
fn options(id: &str, root: &str) -> SessionOptions {
    SessionOptions {
        session_id: Some(id.into()),
        workspace_cwd: Some(root.into()),
        workspace_mcp_url: Some(format!("http://workspace-test/mcp/{id}")),
        generation: 1,
        ..Default::default()
    }
}
fn answer() -> ProviderResponse {
    ProviderResponse {
        id: None,
        status: "completed".into(),
        usage: None,
        output: vec![
            json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"finished"}]}),
        ],
    }
}
struct RecordingProvider {
    requests: Mutex<Vec<ProviderRequest>>,
    barrier: Option<Barrier>,
}
impl RecordingProvider {
    fn new(parallel: bool) -> Self {
        Self {
            requests: Mutex::new(vec![]),
            barrier: parallel.then(|| Barrier::new(2)),
        }
    }
}
impl Provider for RecordingProvider {
    fn respond(
        &self,
        request: ProviderRequest,
    ) -> BoxFuture<'_, Result<ProviderResponse, ProviderError>> {
        Box::pin(async move {
            let count = {
                let mut r = self.requests.lock().unwrap();
                r.push(request);
                r.len()
            };
            if count <= 2
                && let Some(barrier) = &self.barrier
            {
                barrier.wait().await;
            }
            Ok(answer())
        })
    }
}
#[derive(Default)]
struct RecordingTools {
    discovered: Mutex<Vec<(String, Vec<McpServerConfig>)>>,
}
impl ToolExecutor for RecordingTools {
    fn list_tools<'a>(
        &'a self,
        id: &'a str,
        servers: &'a [McpServerConfig],
    ) -> BoxFuture<'a, Result<Vec<Value>, String>> {
        Box::pin(async move {
            self.discovered
                .lock()
                .unwrap()
                .push((id.into(), servers.to_vec()));
            Ok(vec![])
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
        Box::pin(async { panic!("no tool expected") })
    }
}

#[tokio::test]
async fn board_only_new_identity_hot_reload_parallel_sessions_and_endpoint_binding() {
    let board = Board::new().await;
    let dir = tempfile::tempdir().unwrap();
    let config = config_file(&dir, &board.url);
    let original = std::fs::read(&config).unwrap();
    let provider = Arc::new(RecordingProvider::new(true));
    let tools = Arc::new(RecordingTools::default());
    let runtime = Runtime::with_backends(&config, provider.clone(), tools.clone()).unwrap();
    let first = runtime
        .create_session_with_options(
            "engineer",
            Some("task-one".into()),
            "input-one",
            options("session-one", "/tmp/one"),
        )
        .await
        .unwrap();
    let second = runtime
        .create_session_with_options(
            "engineer",
            Some("task-two".into()),
            "input-two",
            options("session-two", "/tmp/two"),
        )
        .await
        .unwrap();
    let (one, two) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(runtime.run(&first.id), runtime.run(&second.id))
    })
    .await
    .unwrap();
    for result in [one, two] {
        assert_eq!(result.unwrap().status, SessionStatus::Completed);
    }
    let requests = provider.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    for request in requests.iter() {
        assert_eq!(request.model, "board-model");
        assert_eq!(request.max_output_tokens, Some(128000));
        assert_eq!(request.reasoning_effort.as_deref(), Some("max"));
        assert!(request.instructions.contains("Board charter v1"));
        assert_eq!(request.input.len(), 1);
    }
    drop(requests);
    let profile = runtime.session_agent(&first.id).await.unwrap();
    assert_eq!(profile.context_window_tokens, 1050000);
    assert_eq!(profile.max_turns, None);
    assert_eq!(profile.max_retries, None);
    assert_eq!(
        profile.profile_source.as_ref().unwrap()["authority"],
        "board"
    );
    let discovered = tools.discovered.lock().unwrap().clone();
    for (id, servers) in discovered.iter() {
        assert_eq!(servers[0].url, format!("http://workspace-test/mcp/{id}"));
        assert_eq!(
            servers[1].url,
            format!("http://runtime-test/mcp/sessions/{id}")
        );
    }
    drop(discovered);
    board.state.identity.lock().unwrap()["metadata"]["model"] = json!("changed-model");
    board.state.identity.lock().unwrap()["charter"] = json!("Board charter v2");
    runtime.steer(&first.id, "followup-one").unwrap();
    runtime.run(&first.id).await.unwrap();
    let requests = provider.requests.lock().unwrap().clone();
    let updated = requests.last().unwrap();
    assert_eq!(updated.model, "changed-model");
    assert!(updated.instructions.contains("Board charter v2"));
    assert!(
        !serde_json::to_string(&updated.input)
            .unwrap()
            .contains("input-two")
    );
    assert_eq!(
        std::fs::read(&config).unwrap(),
        original,
        "host JSON must not be edited"
    );
}

#[tokio::test]
async fn create_idempotency_survives_concurrency_board_outage_and_quiescence() {
    let board = Board::new().await;
    let dir = tempfile::tempdir().unwrap();
    let config = config_file(&dir, &board.url);
    let provider = Arc::new(RecordingProvider::new(false));
    let runtime = Runtime::with_backends(
        &config,
        provider.clone(),
        Arc::new(RecordingTools::default()),
    )
    .unwrap();
    let opt = options("reserved-uuid", "/tmp/work");
    let (one, two) = tokio::join!(
        runtime.create_session_with_options("engineer", None, "same", opt.clone()),
        runtime.create_session_with_options("engineer", None, "same", opt.clone())
    );
    assert_eq!(one.unwrap().id, two.unwrap().id);
    assert_eq!(runtime.list_sessions().unwrap().len(), 1);
    runtime.run("reserved-uuid").await.unwrap();
    board.state.read_fail.store(true, Ordering::SeqCst);
    runtime.quiesce().unwrap();
    assert_eq!(
        runtime
            .create_session_with_options("engineer", None, "same", opt.clone())
            .await
            .unwrap()
            .status,
        SessionStatus::Completed
    );
    let error = runtime
        .create_session_with_options("engineer", None, "different", opt)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("conflict"));
    assert_eq!(
        runtime
            .transcript("reserved-uuid")
            .unwrap()
            .iter()
            .filter(|e| e.kind == "input")
            .count(),
        1
    );
    assert_eq!(provider.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn report_outbox_survives_restart_and_never_collapses_same_identity_sessions() {
    let board = Board::new().await;
    let dir = tempfile::tempdir().unwrap();
    let config = config_file(&dir, &board.url);
    let provider = Arc::new(RecordingProvider::new(false));
    let tools = Arc::new(RecordingTools::default());
    let runtime = Runtime::with_backends(&config, provider.clone(), tools.clone()).unwrap();
    for id in ["one", "two"] {
        runtime
            .create_session_with_options(
                "engineer",
                Some(id.into()),
                id,
                options(id, &format!("/tmp/{id}")),
            )
            .await
            .unwrap();
        runtime.run(id).await.unwrap();
    }
    board.state.write_fail.store(true, Ordering::SeqCst);
    assert_eq!(runtime.flush_reports().await.unwrap(), 0);
    assert!(runtime.get_session("one").unwrap().report_error.is_some());
    let before = runtime.get_session("one").unwrap();
    drop(runtime);
    board.state.write_fail.store(false, Ordering::SeqCst);
    let runtime = Runtime::with_backends(&config, provider, tools).unwrap();
    assert!(runtime.flush_reports().await.unwrap() > 0);
    assert!(runtime.get_session("one").unwrap().report_error.is_none());
    assert_eq!(runtime.get_session("one").unwrap().step, before.step);
    let catalog = board.state.catalog.lock().unwrap().clone();
    assert_eq!(catalog.len(), 2);
    assert_eq!(catalog["one"]["task"], "one");
    assert_eq!(catalog["two"]["task"], "two");
    assert_eq!(catalog["one"]["host_id"], "lab-test");
    assert_eq!(catalog["one"]["phase"], "idle");
    drop(catalog);
    assert_eq!(
        runtime.flush_reports().await.unwrap(),
        0,
        "terminal sessions need no perpetual heartbeat"
    );
}

#[tokio::test]
async fn superseded_generation_blocks_execution_before_any_model_or_tool_dispatch() {
    let board = Board::new().await;
    let dir = tempfile::tempdir().unwrap();
    let config = config_file(&dir, &board.url);
    let provider = Arc::new(RecordingProvider::new(false));
    let tools = Arc::new(RecordingTools::default());
    let runtime = Runtime::with_backends(&config, provider.clone(), tools.clone()).unwrap();
    runtime
        .create_session_with_options("engineer", None, "work", options("fenced", "/tmp/fenced"))
        .await
        .unwrap();
    board
        .state
        .sessions
        .lock()
        .unwrap()
        .insert("fenced".into(), json!({"generation":2,"step":0}));
    assert!(
        runtime
            .run("fenced")
            .await
            .unwrap_err()
            .to_string()
            .contains("superseded")
    );
    assert!(provider.requests.lock().unwrap().is_empty());
    assert!(tools.discovered.lock().unwrap().is_empty());
}

struct ToolProvider {
    calls: AtomicUsize,
}
impl Provider for ToolProvider {
    fn respond(
        &self,
        _: ProviderRequest,
    ) -> BoxFuture<'_, Result<ProviderResponse, ProviderError>> {
        Box::pin(async move {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(ProviderResponse {
                    id: None,
                    status: "completed".into(),
                    usage: None,
                    output: vec![
                        json!({"type":"function_call","call_id":"write-once","name":"workspace__write","arguments":"{}"}),
                    ],
                })
            } else {
                Ok(answer())
            }
        })
    }
}
struct GatedTool {
    entered: Notify,
    release: Semaphore,
    calls: AtomicUsize,
}
impl ToolExecutor for GatedTool {
    fn list_tools<'a>(
        &'a self,
        _: &'a str,
        _: &'a [McpServerConfig],
    ) -> BoxFuture<'a, Result<Vec<Value>, String>> {
        Box::pin(async { Ok(vec![]) })
    }
    fn call<'a>(
        &'a self,
        _: &'a str,
        _: &'a [McpServerConfig],
        _: &'a str,
        _: Value,
        _: &'a str,
    ) -> BoxFuture<'a, Result<Value, String>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
            Ok(json!({"receipt":"committed-original-effect"}))
        })
    }
}
#[tokio::test]
async fn graceful_drain_and_quiesce_preserve_active_tool_receipts_without_replay() {
    for global in [false, true] {
        let board = Board::new().await;
        let dir = tempfile::tempdir().unwrap();
        let config = config_file(&dir, &board.url);
        let provider = Arc::new(ToolProvider {
            calls: AtomicUsize::new(0),
        });
        let tool = Arc::new(GatedTool {
            entered: Notify::new(),
            release: Semaphore::new(0),
            calls: AtomicUsize::new(0),
        });
        let runtime = Runtime::with_backends(&config, provider.clone(), tool.clone()).unwrap();
        runtime
            .create_session_with_options(
                "engineer",
                None,
                "write",
                options("drained", "/tmp/drain"),
            )
            .await
            .unwrap();
        let r = runtime.clone();
        let driver = tokio::spawn(async move { r.run("drained").await });
        tokio::time::timeout(Duration::from_secs(5), tool.entered.notified())
            .await
            .unwrap();
        let active = runtime.get_session("drained").unwrap();
        assert!(active.driver_running);
        assert_eq!(active.phase, "awaiting-tool-result");
        if global {
            runtime.quiesce().unwrap();
        } else {
            runtime.drain_session("drained").unwrap();
        }
        assert_eq!(runtime.tool_calls("drained").unwrap()[0].status, "running");
        tool.release.add_permits(1);
        let drained = tokio::time::timeout(Duration::from_secs(5), driver)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            drained.status,
            if global {
                SessionStatus::Ready
            } else {
                SessionStatus::Stopped
            }
        );
        assert_eq!(
            runtime.tool_calls("drained").unwrap()[0].result,
            Some(json!({"receipt":"committed-original-effect"}))
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert!(!runtime.get_session("drained").unwrap().driver_running);
        runtime.unquiesce();
        if !global {
            runtime.resume("drained").unwrap();
        }
        assert_eq!(
            runtime.run("drained").await.unwrap().status,
            SessionStatus::Completed
        );
        assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn board_read_outage_retries_without_fake_progress_or_fallback_to_host_identity() {
    let board = Board::new().await;
    let dir = tempfile::tempdir().unwrap();
    let config = config_file(&dir, &board.url);
    let provider = Arc::new(RecordingProvider::new(false));
    let runtime = Runtime::with_backends(
        &config,
        provider.clone(),
        Arc::new(RecordingTools::default()),
    )
    .unwrap();
    let created = runtime
        .create_session_with_options(
            "engineer",
            None,
            "survive Board outage",
            options("outage", "/tmp/outage"),
        )
        .await
        .unwrap();
    board.state.read_fail.store(true, Ordering::SeqCst);
    let r = runtime.clone();
    let driver = tokio::spawn(async move { r.run("outage").await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while runtime.get_session("outage").unwrap().status != SessionStatus::Retrying {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let waiting = runtime.get_session("outage").unwrap();
    assert_eq!(waiting.step, 0);
    assert_eq!(waiting.last_progress_at, created.last_progress_at);
    assert!(waiting.driver_running);
    assert!(provider.requests.lock().unwrap().is_empty());
    board.state.read_fail.store(false, Ordering::SeqCst);
    let result = tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result.status, SessionStatus::Completed);
    assert_eq!(result.step, 1);
    assert_eq!(provider.requests.lock().unwrap()[0].model, "board-model");
}

#[tokio::test]
#[ignore = "requires BORG_TEST_TASK_BOARD_URL; creates a uniquely named test-only identity/role, then pauses it"]
async fn real_board_role_profiles_new_identity_parallel_sessions_and_reporting() {
    let url = std::env::var("BORG_TEST_TASK_BOARD_URL")
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    let client = reqwest::Client::new();
    let suffix = uuid::Uuid::new_v4().to_string();
    let id = format!("native-profile-test-{suffix}");
    let role = format!("native-profile-role-{suffix}");
    // Not a Fleet launch candidate; its native sessions run only in this isolated
    // test database with a deterministic provider. No API model spend or task.
    client.post(format!("{url}/api/agents")).json(&json!({"agent_id":id,"charter":"real Board charter","metadata":{"role":role,"harness":"native-test-fixture","test_only":true}})).send().await.unwrap().error_for_status().unwrap();
    client.post(format!("{url}/api/roles/{role}/config/runtime")).json(&json!({"entry_id":"profile","enabled":true,"payload":{"model":"real-role-v1","max_retries":null}})).send().await.unwrap().error_for_status().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let config = config_file(&dir, &url);
    let original = std::fs::read(&config).unwrap();
    let provider = Arc::new(RecordingProvider::new(true));
    let runtime = Runtime::with_backends(
        &config,
        provider.clone(),
        Arc::new(RecordingTools::default()),
    )
    .unwrap();
    let first_id = format!("{suffix}-one");
    let second_id = format!("{suffix}-two");
    runtime
        .create_session_with_options(
            &id,
            None,
            "first real-Board input",
            options(&first_id, "/tmp/real-one"),
        )
        .await
        .unwrap();
    runtime
        .create_session_with_options(
            &id,
            None,
            "second real-Board input",
            options(&second_id, "/tmp/real-two"),
        )
        .await
        .unwrap();
    let (one, two) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(runtime.run(&first_id), runtime.run(&second_id))
    })
    .await
    .unwrap();
    assert_eq!(one.unwrap().status, SessionStatus::Completed);
    assert_eq!(two.unwrap().status, SessionStatus::Completed);
    let profile = runtime.load_agent(&id).await.unwrap();
    assert_eq!(profile.model, "real-role-v1");
    assert_eq!(profile.max_output_tokens, Some(128000));
    assert_eq!(profile.context_window_tokens, 1050000);
    assert_eq!(profile.max_turns, None);
    let source = profile.profile_source.unwrap();
    assert_eq!(source["role"], role);
    assert_eq!(source["runtime_role_version"], 1);
    client
        .post(format!("{url}/api/roles/{role}/config/runtime"))
        .json(&json!({"entry_id":"profile","payload":{"model":"real-role-v2","max_retries":null}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    runtime
        .steer(&first_id, "consume live role change")
        .unwrap();
    runtime.run(&first_id).await.unwrap();
    assert_eq!(
        provider.requests.lock().unwrap().last().unwrap().model,
        "real-role-v2"
    );
    assert!(runtime.flush_reports().await.unwrap() > 0);
    let catalog: Value = client
        .get(format!("{url}/api/agents/{id}/config/native_session"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let entries = catalog["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().any(|entry| entry["id"] == first_id));
    assert!(entries.iter().any(|entry| entry["id"] == second_id));
    let state: Value = client
        .get(format!("{url}/api/sessions/{first_id}/state"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(state["phase"], "idle");
    assert_eq!(state["generation"], 1);
    assert_eq!(state["step"], 3);
    assert_eq!(std::fs::read(&config).unwrap(), original);
    client.post(format!("{url}/api/agents/{id}/lifecycle-intent")).json(&json!({"intent":"paused","intent_by":"native-core","reason":"isolated native profile integration test complete"})).send().await.unwrap().error_for_status().unwrap();
    eprintln!(
        "real Board profile test passed: identity={id}, sessions={first_id},{second_id}, role_versions=1->2, host_config_unchanged=true"
    );
}

#[tokio::test]
async fn explicit_binding_preserves_transcript_and_rejects_generation_rollback() {
    let board = Board::new().await;
    let dir = tempfile::tempdir().unwrap();
    let config = config_file(&dir, &board.url);
    let runtime = Runtime::with_backends(
        &config,
        Arc::new(RecordingProvider::new(false)),
        Arc::new(RecordingTools::default()),
    )
    .unwrap();
    runtime
        .create_session_with_options(
            "engineer",
            None,
            "original",
            options("adopt", "/tmp/original"),
        )
        .await
        .unwrap();
    runtime.run("adopt").await.unwrap();
    let before = serde_json::to_value(runtime.transcript("adopt").unwrap()).unwrap();
    let mut updated = options("adopt", "/tmp/adopted");
    updated.generation = 2;
    let session = runtime.bind_session("adopt", updated).unwrap();
    assert_eq!(session.generation, 2);
    assert_eq!(session.workspace_cwd, Some("/tmp/adopted".into()));
    assert_eq!(
        serde_json::to_value(runtime.transcript("adopt").unwrap()).unwrap(),
        before
    );
    assert!(
        runtime
            .bind_session("adopt", options("adopt", "/tmp/old"))
            .unwrap_err()
            .to_string()
            .contains("decrease")
    );
    assert_eq!(
        runtime.get_session("adopt").unwrap().status,
        SessionStatus::Completed
    );
}
