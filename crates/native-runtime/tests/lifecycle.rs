//! Real native MCP HTTP plus durable runtime, deterministic model responses, and
//! the actual Python workspace service. No provider account or credentials.
#[allow(dead_code)]
#[path = "../src/main.rs"]
mod service;

use native_runtime::{mcp::McpToolExecutor, *};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Barrier;

type Reply = BoxFuture<'static, Result<ProviderResponse, ProviderError>>;
struct FakeProvider(Box<dyn Fn(ProviderRequest) -> Reply + Send + Sync>);
impl Provider for FakeProvider {
    fn respond(
        &self,
        request: ProviderRequest,
    ) -> BoxFuture<'_, Result<ProviderResponse, ProviderError>> {
        (self.0)(request)
    }
}
fn provider(f: impl Fn(ProviderRequest) -> Reply + Send + Sync + 'static) -> Arc<dyn Provider> {
    Arc::new(FakeProvider(Box::new(f)))
}
fn answer(text: &str) -> ProviderResponse {
    ProviderResponse {
        id: None,
        status: "completed".into(),
        usage: None,
        output: vec![
            json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":text}]}),
        ],
    }
}
fn tool(name: &str, call_id: &str, args: Value) -> ProviderResponse {
    ProviderResponse {
        id: None,
        status: "completed".into(),
        usage: None,
        output: vec![
            json!({"type":"function_call","name":name,"call_id":call_id,"arguments":args.to_string()}),
        ],
    }
}
fn input(request: &ProviderRequest) -> &str {
    request
        .input
        .iter()
        .rev()
        .find(|v| v["role"] == "user")
        .unwrap()["content"]
        .as_str()
        .unwrap()
}
fn results(request: &ProviderRequest) -> Vec<Value> {
    request
        .input
        .iter()
        .filter(|v| v["type"] == "function_call_output")
        .map(|v| serde_json::from_str(v["output"].as_str().unwrap()).unwrap())
        .collect()
}

struct Server {
    url: String,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl Drop for Server {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
impl Server {
    async fn stop(mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}
async fn listener() -> (tokio::net::TcpListener, String) {
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", socket.local_addr().unwrap());
    (socket, url)
}
fn serve(socket: tokio::net::TcpListener, runtime: Runtime, token: Option<String>) -> Server {
    let url = format!("http://{}", socket.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(socket, service::router(runtime, token))
            .await
            .unwrap();
    });
    Server {
        url,
        task: Some(task),
    }
}
fn config(path: &Path, url: &str) {
    std::fs::write(path, json!({"database_path":"state.sqlite", "host_id":"fixture-host", "agents":[
        {"id":"engineer","instructions":"Use configured tools.","retry_initial_ms":1,"retry_max_ms":2,"retry_jitter_percent":0,
         "mcp_servers":[{"name":"native","url":format!("{url}/mcp/sessions/{{session_id}}")}]},
        {"id":"other","instructions":"Other identity private context.","mcp_servers":[]}
    ]}).to_string()).unwrap();
}
fn fixture(url: &str, peer: Arc<dyn Provider>) -> (tempfile::TempDir, PathBuf, Runtime) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.json");
    config(&path, url);
    let runtime =
        Runtime::with_backends(&path, peer, Arc::new(McpToolExecutor::new().unwrap())).unwrap();
    (dir, path, runtime)
}
async fn wait(runtime: &Runtime, id: &str, status: SessionStatus) -> Session {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let session = runtime.get_session(id).unwrap();
            if session.status == status {
                return session;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("expected {status:?}; got {:?}", runtime.get_session(id)))
}
async fn invoke(runtime: &Runtime, caller: &str, name: &str, args: Value, op: &str) -> Value {
    let servers = runtime.session_agent(caller).await.unwrap().mcp_servers;
    McpToolExecutor::new()
        .unwrap()
        .call(caller, &servers, &format!("native__{name}"), args, op)
        .await
        .unwrap()
}
fn value(result: &Value) -> Value {
    assert_eq!(result["isError"], false, "{result:#}");
    let content: Value =
        serde_json::from_str(result["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(content, result["structuredContent"]);
    content
}

#[tokio::test]
async fn native_parent_calls_its_own_mcp_to_create_parallel_same_identity_child_and_idempotently_recovers_it()
 {
    let barrier = Arc::new(Barrier::new(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let peer = provider({
        let calls = calls.clone();
        move |request| {
            let barrier = barrier.clone();
            let calls = calls.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                assert!(
                    request
                        .tools
                        .iter()
                        .any(|v| v["name"] == "native__session_create")
                );
                if input(&request) == "parent" && results(&request).is_empty() {
                    return Ok(tool(
                        "native__session_create",
                        "delegate",
                        json!({"input":"child","request_id":"intent-one"}),
                    ));
                }
                if input(&request) == "parent" {
                    assert!(value(&results(&request)[0])["session_id"].is_string());
                }
                barrier.wait().await; // Proves the child runs before its parent terminates.
                Ok(answer(&format!("finished {}", input(&request))))
            })
        }
    });
    let (socket, url) = listener().await;
    let (_dir, path, runtime) = fixture(&url, peer.clone());
    let server = serve(socket, runtime.clone(), None);
    let parent = runtime.create_session("engineer", None, "parent").unwrap();
    let run = tokio::spawn({
        let runtime = runtime.clone();
        let id = parent.id.clone();
        async move { runtime.run(&id).await.unwrap() }
    });
    wait(&runtime, &parent.id, SessionStatus::Completed).await;
    run.await.unwrap();
    let receipt = value(
        runtime.tool_calls(&parent.id).unwrap()[0]
            .result
            .as_ref()
            .unwrap(),
    );
    let child_id = receipt["session_id"].as_str().unwrap().to_owned();
    assert_ne!(child_id, parent.id);
    assert_eq!(receipt["parent_session_id"], parent.id);
    assert_eq!(receipt["agent_id"], "engineer");
    let child = wait(&runtime, &child_id, SessionStatus::Completed).await;
    assert_eq!(child.agent_id, parent.agent_id);
    assert_eq!(runtime.list_sessions().unwrap().len(), 2);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert!(
        !serde_json::to_string(&runtime.transcript(&child_id).unwrap())
            .unwrap()
            .contains("finished parent")
    );
    let retried = invoke(
        &runtime,
        &parent.id,
        "session_create",
        json!({"input":"child","request_id":"intent-one"}),
        "different-call-same-intent",
    )
    .await;
    assert_eq!(value(&retried)["session_id"], child_id);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let changed = invoke(
        &runtime,
        &parent.id,
        "session_create",
        json!({"input":"DIFFERENT","request_id":"intent-one"}),
        "different-intent",
    )
    .await;
    assert_eq!(changed["isError"], true);
    assert_eq!(runtime.list_sessions().unwrap().len(), 2);
    let old_health: Value = reqwest::Client::new()
        .get(format!("{url}/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    server.stop().await;
    drop(runtime);
    let recovered =
        Runtime::with_backends(&path, peer, Arc::new(McpToolExecutor::new().unwrap())).unwrap();
    assert_eq!(
        recovered.get_session(&child_id).unwrap().status,
        SessionStatus::Completed
    );
    assert_eq!(
        recovered.tool_calls(&parent.id).unwrap()[0]
            .result
            .as_ref()
            .map(value),
        Some(receipt)
    );
    let (new_socket, new_url) = listener().await;
    let mut cfg: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    cfg["agents"][0]["mcp_servers"][0]["url"] =
        json!(format!("{new_url}/mcp/sessions/{{session_id}}"));
    std::fs::write(&path, cfg.to_string()).unwrap();
    let _new_server = serve(new_socket, recovered.clone(), None);
    let retry_after_restart = invoke(
        &recovered,
        &parent.id,
        "session_create",
        json!({"input":"child","request_id":"intent-one"}),
        "after-restart",
    )
    .await;
    assert_eq!(value(&retry_after_restart)["session_id"], child_id);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let new_health: Value = reqwest::Client::new()
        .get(format!("{new_url}/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_ne!(old_health["instance_id"], new_health["instance_id"]);
}

#[tokio::test]
async fn lifecycle_controls_and_read_slices_preserve_original_inputs_and_unknown_result() {
    let (socket, url) = listener().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let peer = provider({
        let calls = calls.clone();
        move |request| {
            let calls = calls.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(answer(input(&request)))
            })
        }
    });
    let (_dir, _path, runtime) = fixture(&url, peer);
    let _server = serve(socket, runtime.clone(), None);
    let caller = runtime
        .create_session("engineer", None, "controller")
        .unwrap();
    let target = runtime.create_session("engineer", None, "initial").unwrap();
    value(
        &invoke(
            &runtime,
            &caller.id,
            "session_stop",
            json!({"session_id":target.id}),
            "stop",
        )
        .await,
    );
    assert_eq!(
        runtime.get_session(&target.id).unwrap().status,
        SessionStatus::Stopped
    );
    value(
        &invoke(
            &runtime,
            &caller.id,
            "session_steer",
            json!({"session_id":target.id,"text":"follow-up"}),
            "steer",
        )
        .await,
    );
    assert_eq!(
        runtime.get_session(&target.id).unwrap().status,
        SessionStatus::Stopped
    );
    value(
        &invoke(
            &runtime,
            &caller.id,
            "session_resume",
            json!({"session_id":target.id}),
            "resume",
        )
        .await,
    );
    wait(&runtime, &target.id, SessionStatus::Completed).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let read = value(&invoke(&runtime, &caller.id, "session_get", json!({"session_id":target.id,"include_transcript":true,"include_tools":true,"limit":1}), "read").await);
    assert_eq!(read["transcript"].as_array().unwrap().len(), 1);
    assert_eq!(read["transcript"][0]["item"]["content"], "initial");
    let next = value(&invoke(&runtime, &caller.id, "session_get", json!({"session_id":target.id,"include_transcript":true,"after_sequence":read["next_sequence"],"limit":1}), "next").await);
    assert_eq!(next["transcript"][0]["item"]["content"], "follow-up");
    let listed = value(
        &invoke(
            &runtime,
            &caller.id,
            "session_list",
            json!({"limit":1}),
            "list",
        )
        .await,
    );
    assert_eq!(listed["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(listed["next_offset"], 1);
    let missing = invoke(
        &runtime,
        &caller.id,
        "session_resolve_tool",
        json!({"session_id":target.id,"call_id":"not-a-call"}),
        "missing-result",
    )
    .await;
    assert_eq!(missing["isError"], true);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn metadata_permissions_identity_binding_auth_and_notifications_fail_before_effects() {
    let (socket, url) = listener().await;
    let (_dir, path, runtime) = fixture(&url, provider(|_| Box::pin(async { Ok(answer("done")) })));
    let server = serve(socket, runtime.clone(), Some("fixture-token".into()));
    let caller = runtime.create_session("engineer", None, "caller").unwrap();
    let other = runtime
        .create_session("other", None, "other-private-value")
        .unwrap();
    let endpoint = format!("{}/mcp/sessions/{}", server.url, caller.id);
    let http = reqwest::Client::new();
    let ping = json!({"jsonrpc":"2.0","id":1,"method":"ping"});
    assert_eq!(
        http.post(&endpoint)
            .json(&ping)
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        http.post(&endpoint)
            .bearer_auth("fixture-token")
            .header("Origin", "https://untrusted.example")
            .json(&ping)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let rpc = |name: &str, args: Value| json!({"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":name,"arguments":args,"_meta":{"borg/operation_id":"identity-test"}}});
    let response: Value = http
        .post(&endpoint)
        .bearer_auth("fixture-token")
        .json(&rpc(
            "session_get",
            json!({"session_id":other.id,"include_transcript":true}),
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(response["result"]["isError"], true);
    assert!(!response.to_string().contains("other-private-value"));
    let mut disabled: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    disabled["agents"][0]["mcp_servers"][0]["tools_allowlist"] = json!(["session_list"]);
    std::fs::write(&path, disabled.to_string()).unwrap();
    let catalog: Value = http
        .post(&endpoint)
        .bearer_auth("fixture-token")
        .json(&json!({"jsonrpc":"2.0","id":8,"method":"tools/list"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(catalog["result"]["tools"].as_array().unwrap().len(), 1);
    assert_eq!(catalog["result"]["tools"][0]["name"], "session_list");
    let denied: Value = http
        .post(&endpoint)
        .bearer_auth("fixture-token")
        .json(&rpc("session_create", json!({"input":"not-permitted"})))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(denied["result"]["isError"], true);
    let mut notification = rpc("session_create", json!({"input":"must-not-execute"}));
    notification.as_object_mut().unwrap().remove("id");
    let rejected: Value = http
        .post(&endpoint)
        .bearer_auth("fixture-token")
        .json(&notification)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(rejected["error"]["code"], -32600);
    assert_eq!(runtime.list_sessions().unwrap().len(), 2);
}

struct WorkspaceProcess {
    process: Child,
    url: String,
}
impl WorkspaceProcess {
    fn start(root: &Path) -> Self {
        let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tooling/workspace_mcp.py");
        let mut process = Command::new("python3")
            .arg(script)
            .args(["--session-root"])
            .arg(root)
            .args(["--port", "0"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut line = String::new();
        BufReader::new(process.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let ready: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(ready["mode"], "sessions");
        Self {
            process,
            url: ready["listening"].as_str().unwrap().to_owned(),
        }
    }
}
impl Drop for WorkspaceProcess {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

#[tokio::test]
async fn real_mcp_session_bindings_override_identity_workspace_and_preserve_filesystem_isolation() {
    let barrier = Arc::new(Barrier::new(2));
    let peer = provider(move |request| {
        let barrier = barrier.clone();
        Box::pin(async move {
            match results(&request).len() {
                0 => {
                    barrier.wait().await;
                    Ok(tool(
                        "workspace__workspace_write",
                        "write",
                        json!({"path":"marker.txt","content":input(&request)}),
                    ))
                }
                1 => {
                    assert_eq!(results(&request)[0]["isError"], false);
                    Ok(tool(
                        "workspace__workspace_run",
                        "shell",
                        json!({"command":"printf '%s\\n' \"$PWD\"; cat marker.txt"}),
                    ))
                }
                _ => {
                    assert_eq!(results(&request)[1]["isError"], false);
                    Ok(answer("isolated workspace verified"))
                }
            }
        })
    });
    let (socket, url) = listener().await;
    let (dir, path, mut runtime) = fixture(&url, peer.clone());
    drop(runtime);
    let mut cfg: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    cfg["agents"][0]["mcp_servers"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"workspace","url":"http://127.0.0.1:1/wrong-global-workspace"}));
    std::fs::write(&path, cfg.to_string()).unwrap();
    runtime =
        Runtime::with_backends(&path, peer, Arc::new(McpToolExecutor::new().unwrap())).unwrap();
    let roots = dir.path().join("worktrees");
    std::fs::create_dir(&roots).unwrap();
    let helper = WorkspaceProcess::start(&roots);
    let _server = serve(socket, runtime.clone(), None);
    let mut sessions = Vec::new();
    for marker in ["one", "two"] {
        let id = uuid::Uuid::new_v4().to_string();
        let cwd = roots.join(&id);
        std::fs::create_dir(&cwd).unwrap();
        let session = runtime
            .create_session_with_options(
                "engineer",
                None,
                marker,
                SessionOptions {
                    session_id: Some(id.clone()),
                    workspace_cwd: Some(cwd),
                    workspace_mcp_url: Some(format!("{}/{{session_id}}", helper.url)),
                    host_id: Some("fixture-host".into()),
                    generation: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        sessions.push(session);
    }
    let a = runtime.run(&sessions[0].id);
    let b = runtime.run(&sessions[1].id);
    let (a, b) = tokio::time::timeout(Duration::from_secs(8), async { tokio::join!(a, b) })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "workspace sessions did not finish: {:?}",
                runtime.list_sessions()
            )
        });
    assert_eq!(a.unwrap().status, SessionStatus::Completed);
    assert_eq!(b.unwrap().status, SessionStatus::Completed);
    for (session, marker) in sessions.iter().zip(["one", "two"]) {
        assert_eq!(
            std::fs::read_to_string(roots.join(&session.id).join("marker.txt")).unwrap(),
            marker
        );
        let call = runtime.tool_calls(&session.id).unwrap().remove(1);
        let result: Value =
            serde_json::from_str(call.result.unwrap()["content"][0]["text"].as_str().unwrap())
                .unwrap();
        assert!(
            result["output"]
                .as_str()
                .unwrap()
                .contains(roots.join(&session.id).to_str().unwrap())
        );
        assert!(result["output"].as_str().unwrap().ends_with(marker));
        let configured = runtime.session_agent(&session.id).await.unwrap();
        assert_eq!(
            configured
                .mcp_servers
                .iter()
                .find(|s| s.name == "workspace")
                .unwrap()
                .url,
            format!("{}/{}", helper.url, session.id)
        );
    }
    assert!(!roots.join("marker.txt").exists());
}

#[tokio::test]
async fn self_graceful_stop_records_its_tool_receipt_before_parking_and_health_tracks_quiesce() {
    let peer = provider(|request| {
        Box::pin(async move {
            if results(&request).is_empty() {
                let context: Value = serde_json::from_str(
                    request
                        .instructions
                        .split_once(": ")
                        .unwrap()
                        .1
                        .lines()
                        .next()
                        .unwrap(),
                )
                .unwrap();
                Ok(tool(
                    "native__session_stop",
                    "self-stop",
                    json!({"session_id":context["session_id"]}),
                ))
            } else {
                Ok(answer("resumed with original stop receipt"))
            }
        })
    });
    let (socket, url) = listener().await;
    let (_dir, _path, runtime) = fixture(&url, peer);
    let _server = serve(socket, runtime.clone(), None);
    let caller = runtime
        .create_session("engineer", None, "stop gracefully")
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(8), runtime.run(&caller.id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.status, SessionStatus::Stopped);
    let receipts = runtime.tool_calls(&caller.id).unwrap();
    assert_eq!(receipts[0].status, "succeeded");
    value(receipts[0].result.as_ref().unwrap());
    let http = reqwest::Client::new();
    let health: Value = http
        .get(format!("{url}/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["host_id"], "fixture-host");
    assert_eq!(health["ready"], true);
    assert!(health["instance_id"].is_string());
    assert_eq!(
        http.post(format!("{url}/api/quiesce"))
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let drained: Value = http
        .get(format!("{url}/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(drained["instance_id"], health["instance_id"]);
    assert_eq!(drained["ready"], false);
    assert_eq!(
        http.post(format!("{url}/api/unquiesce"))
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    runtime.resume(&caller.id).unwrap();
    assert_eq!(
        runtime.run(&caller.id).await.unwrap().status,
        SessionStatus::Completed
    );
    assert_eq!(runtime.tool_calls(&caller.id).unwrap().len(), 1);
}

#[tokio::test]
async fn unknown_external_effect_is_resolved_through_mcp_without_replaying_it() {
    use axum::{
        Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::post,
    };
    async fn effect(
        State(count): State<Arc<AtomicUsize>>,
        Json(request): Json<Value>,
    ) -> axum::response::Response {
        let result = match request["method"].as_str().unwrap() {
            "initialize" => {
                json!({"protocolVersion":"2025-06-18","capabilities":{"tools":{}},"serverInfo":{"name":"effect-fixture","version":"1"}})
            }
            "notifications/initialized" => return StatusCode::ACCEPTED.into_response(),
            "tools/list" => {
                json!({"tools":[{"name":"once","inputSchema":{"type":"object","properties":{}}}]})
            }
            "tools/call" => {
                // The external side effect committed, but its original response
                // was lost. The calling runtime must never infer a safe replay.
                count.fetch_add(1, Ordering::SeqCst);
                return StatusCode::BAD_GATEWAY.into_response();
            }
            _ => panic!("unexpected method"),
        };
        Json(json!({"jsonrpc":"2.0","id":request["id"],"result":result})).into_response()
    }
    let effects = Arc::new(AtomicUsize::new(0));
    let (effect_socket, effect_url) = listener().await;
    let app = Router::new()
        .route("/mcp", post(effect))
        .with_state(effects.clone());
    let effect_task = tokio::spawn(async move {
        axum::serve(effect_socket, app).await.unwrap();
    });
    let _effect_server = Server {
        url: effect_url.clone(),
        task: Some(effect_task),
    };
    let peer = provider(|request| {
        Box::pin(async move {
            if results(&request).is_empty() {
                Ok(tool("effect__once", "original", json!({})))
            } else {
                assert_eq!(results(&request)[0]["observed"], "original-effect-receipt");
                Ok(answer("reconciled"))
            }
        })
    });
    let (socket, url) = listener().await;
    let (_dir, path, mut runtime) = fixture(&url, peer.clone());
    drop(runtime);
    let mut cfg: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    cfg["agents"][0]["mcp_servers"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"effect","url":format!("{effect_url}/mcp")}));
    std::fs::write(&path, cfg.to_string()).unwrap();
    runtime =
        Runtime::with_backends(&path, peer, Arc::new(McpToolExecutor::new().unwrap())).unwrap();
    let _server = serve(socket, runtime.clone(), None);
    let caller = runtime
        .create_session("engineer", None, "controller")
        .unwrap();
    let target = runtime
        .create_session("engineer", None, "do effect")
        .unwrap();
    assert_eq!(
        runtime.run(&target.id).await.unwrap().status,
        SessionStatus::NeedsResolution
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    let read = value(
        &invoke(
            &runtime,
            &caller.id,
            "session_get",
            json!({"session_id":target.id,"include_tools":true}),
            "inspect",
        )
        .await,
    );
    assert_eq!(read["tools"][0]["status"], "unknown");
    assert_eq!(read["tools"][0]["call_id"], "original");
    assert_eq!(
        invoke(
            &runtime,
            &caller.id,
            "session_resume",
            json!({"session_id":target.id}),
            "unsafe-resume"
        )
        .await["isError"],
        true
    );
    let observed = json!({"observed":"original-effect-receipt"});
    value(
        &invoke(
            &runtime,
            &caller.id,
            "session_resolve_tool",
            json!({"session_id":target.id,"call_id":"original","result":observed}),
            "verified-resolution",
        )
        .await,
    );
    wait(&runtime, &target.id, SessionStatus::Completed).await;
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(
        runtime.tool_calls(&target.id).unwrap()[0].result,
        Some(observed.clone())
    );
    value(
        &invoke(
            &runtime,
            &caller.id,
            "session_resolve_tool",
            json!({"session_id":target.id,"call_id":"original","result":observed}),
            "same-resolution",
        )
        .await,
    );
    assert_eq!(invoke(&runtime, &caller.id, "session_resolve_tool", json!({"session_id":target.id,"call_id":"original","result":{"invented":"conflicting"}}), "conflict").await["isError"], true);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(
        runtime
            .transcript(&target.id)
            .unwrap()
            .iter()
            .filter(|entry| entry.kind == "resolution")
            .count(),
        1
    );
}

#[derive(Clone)]
struct FleetFixture {
    caller_url: String,
    target_url: String,
    rows: Arc<std::sync::Mutex<Vec<Value>>>,
    posts: Arc<AtomicUsize>,
    lose_reply: bool,
    allowed_agents: Arc<std::sync::Mutex<Vec<String>>>,
}
async fn fleet_fixture(
    axum::extract::State(state): axum::extract::State<FleetFixture>,
    request: axum::extract::Request,
) -> axum::response::Response {
    use axum::{
        Json,
        http::{Method, StatusCode},
        response::IntoResponse,
    };
    let method = request.method().clone();
    let uri = request.uri().to_string();
    let parsed = reqwest::Url::parse(&format!("http://fixture{uri}")).unwrap();
    let path = parsed.path();
    if method == Method::GET && path.starts_with("/api/sessions/") && path.ends_with("/state") {
        return Json(Value::Null).into_response();
    }
    if method == Method::GET && path == "/api/agents/engineer" {
        return Json(json!({"id":"engineer","charter":"Use metadata-selected tools.","retired":false,"lifecycle_intent":"run",
            "metadata":{"role":"fixture","harness":"native"}})).into_response();
    }
    if method == Method::GET && path == "/api/agents/engineer/config/tool" {
        return Json(json!({"agent_id":"engineer","count":1,"agent_version":1,"role_version":0,"entries":[{
            "id":"native","enabled":true,"position":0,"source":"agent","scope":null,
            "payload":{"kind":"mcp","name":"native","url":format!("{}/mcp/sessions/{{session_id}}",state.caller_url),"allowed_agent_ids":*state.allowed_agents.lock().unwrap()}}]})).into_response();
    }
    if method == Method::GET && path.starts_with("/api/agents/engineer/config/") {
        return Json(json!({"agent_id":"engineer","count":0,"entries":[],"agent_version":0,"role_version":0})).into_response();
    }
    if method == Method::GET && path == "/api/fleet/dispatches" {
        let query: std::collections::HashMap<_, _> = parsed.query_pairs().into_owned().collect();
        let offset: usize = query.get("offset").map(|s| s.parse().unwrap()).unwrap_or(0);
        let limit: usize = query
            .get("limit")
            .map(|s| s.parse().unwrap())
            .unwrap_or(100);
        let rows: Vec<_> = state
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|row| {
                ["agent_id", "session_id"]
                    .iter()
                    .all(|key| query.get(*key).is_none_or(|value| row[*key] == *value))
            })
            .skip(offset)
            .take(limit)
            .cloned()
            .collect();
        return Json(json!(rows)).into_response();
    }
    if method == Method::POST && path == "/api/fleet/dispatches" {
        state.posts.fetch_add(1, Ordering::SeqCst);
        let bytes = axum::body::to_bytes(request.into_body(), 64 * 1024 * 1024)
            .await
            .unwrap();
        let input: Value = serde_json::from_slice(&bytes).unwrap();
        assert!(input["agent_id"].is_string());
        assert!(input["metadata"]["parent_session_id"].is_string());
        let mut rows = state.rows.lock().unwrap();
        if let Some(row) = rows
            .iter()
            .find(|row| row["request_id"] == input["request_id"])
        {
            if row["input"] != input["input"] {
                return StatusCode::CONFLICT.into_response();
            }
            return Json(row.clone()).into_response();
        }
        let row = json!({"id":rows.len()+1,"session_id":uuid::Uuid::new_v4().to_string(),
            "request_id":input["request_id"],"agent_id":input["agent_id"],"task_id":input["task_id"],
            "input":input["input"],"metadata":input["metadata"],"host_id":"other-host",
            "runtime_url":format!("{}/api",state.target_url),"workspace_cwd":"/fixture/fleet-owned",
            "generation":1,"status":"reserved","created":true});
        rows.push(row.clone());
        if state.lose_reply {
            return StatusCode::BAD_GATEWAY.into_response();
        }
        return (StatusCode::CREATED, Json(row)).into_response();
    }
    panic!("unexpected fixture request {method} {uri}");
}
async fn board_fixture(
    caller_url: &str,
    target_url: &str,
    lose_reply: bool,
) -> (Server, FleetFixture) {
    let (socket, url) = listener().await;
    let state = FleetFixture {
        caller_url: caller_url.into(),
        target_url: target_url.into(),
        rows: Arc::new(std::sync::Mutex::new(vec![])),
        posts: Arc::new(AtomicUsize::new(0)),
        lose_reply,
        allowed_agents: Arc::new(std::sync::Mutex::new(Vec::new())),
    };
    let app = axum::Router::new()
        .fallback(fleet_fixture)
        .with_state(state.clone());
    let task = tokio::spawn(async move {
        axum::serve(socket, app).await.unwrap();
    });
    (
        Server {
            url,
            task: Some(task),
        },
        state,
    )
}
fn enable_board(path: &Path, board_url: &str) {
    let mut cfg: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    cfg["board"] = json!({"url":board_url});
    std::fs::write(path, cfg.to_string()).unwrap();
}

#[tokio::test]
async fn lifecycle_routes_board_creation_and_controls_to_the_actual_other_runtime() {
    let (a_socket, a_url) = listener().await;
    let (_a_dir, a_path, a) = fixture(
        &a_url,
        provider(|_| Box::pin(async { Ok(answer("local")) })),
    );
    let (b_socket, b_url) = listener().await;
    let (_b_dir, b_path, b) = fixture(
        &b_url,
        provider(|request| Box::pin(async move { Ok(answer(input(&request))) })),
    );
    let mut b_config: Value = serde_json::from_slice(&std::fs::read(&b_path).unwrap()).unwrap();
    b_config["host_id"] = json!("other-host");
    std::fs::write(&b_path, b_config.to_string()).unwrap();
    let _a_server = serve(a_socket, a.clone(), None);
    let _b_server = serve(b_socket, b.clone(), None);
    let caller = a.create_session("engineer", None, "controller").unwrap();
    let (board, broker) = board_fixture(&a_url, &b_url, false).await;
    enable_board(&a_path, &board.url);
    let receipt = value(
        &invoke(
            &a,
            &caller.id,
            "session_create",
            json!({"input":"remote work","request_id":"remote-intent"}),
            "remote-create",
        )
        .await,
    );
    let child_id = receipt["session_id"].as_str().unwrap();
    assert_eq!(receipt["dispatch"]["host_id"], "other-host");
    assert_eq!(broker.posts.load(Ordering::SeqCst), 1);
    assert_eq!(a.list_sessions().unwrap().len(), 1); // Never silently bypasses Fleet.
    assert!(b.list_sessions().unwrap().is_empty());
    let queued = value(
        &invoke(
            &a,
            &caller.id,
            "session_get",
            json!({"session_id":child_id}),
            "queued",
        )
        .await,
    );
    assert_eq!(queued["session"], Value::Null);
    assert_eq!(queued["dispatch"]["status"], "reserved");
    // The Fleet provisioning boundary, represented by a second real runtime.
    b.create_session_with_options(
        "engineer",
        None,
        "remote work",
        SessionOptions {
            session_id: Some(child_id.into()),
            host_id: Some("other-host".into()),
            generation: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    value(
        &invoke(
            &a,
            &caller.id,
            "session_stop",
            json!({"session_id":child_id}),
            "remote-stop",
        )
        .await,
    );
    assert_eq!(
        b.get_session(child_id).unwrap().status,
        SessionStatus::Stopped
    );
    value(
        &invoke(
            &a,
            &caller.id,
            "session_steer",
            json!({"session_id":child_id,"text":"remotely steered"}),
            "remote-steer",
        )
        .await,
    );
    value(
        &invoke(
            &a,
            &caller.id,
            "session_resume",
            json!({"session_id":child_id}),
            "remote-resume",
        )
        .await,
    );
    wait(&b, child_id, SessionStatus::Completed).await;
    let read = value(
        &invoke(
            &a,
            &caller.id,
            "session_get",
            json!({"session_id":child_id,"include_tools":true,"include_transcript":true,"limit":1}),
            "remote-read",
        )
        .await,
    );
    assert_eq!(read["session"]["id"], child_id);
    assert_eq!(read["transcript"].as_array().unwrap().len(), 1);
    assert_eq!(read["transcript"][0]["item"]["content"], "remote work");
    assert_eq!(a.list_sessions().unwrap().len(), 1);
    assert_eq!(b.list_sessions().unwrap().len(), 1);
    let listed = value(&invoke(&a, &caller.id, "session_list", json!({}), "remote-list").await);
    assert_eq!(listed["dispatches"][0]["session_id"], child_id);
    broker.allowed_agents.lock().unwrap().push("other".into());
    let other_receipt = value(
        &invoke(
            &a,
            &caller.id,
            "session_create",
            json!({"agent_id":"other","input":"permitted worker","request_id":"worker-intent"}),
            "worker-create",
        )
        .await,
    );
    let other_id = other_receipt["session_id"].as_str().unwrap();
    assert_eq!(other_receipt["agent_id"], "other");
    b.create_session_with_options(
        "other",
        None,
        "permitted worker",
        SessionOptions {
            session_id: Some(other_id.into()),
            host_id: Some("other-host".into()),
            generation: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    value(
        &invoke(
            &a,
            &caller.id,
            "session_stop",
            json!({"session_id":other_id}),
            "worker-stop",
        )
        .await,
    );
    value(
        &invoke(
            &a,
            &caller.id,
            "session_resume",
            json!({"session_id":other_id}),
            "worker-resume",
        )
        .await,
    );
    wait(&b, other_id, SessionStatus::Completed).await;
    assert_eq!(
        value(
            &invoke(
                &a,
                &caller.id,
                "session_get",
                json!({"session_id":other_id}),
                "worker-read"
            )
            .await
        )["session"]["agent_id"],
        "other"
    );
    broker.allowed_agents.lock().unwrap().clear();
    assert_eq!(
        invoke(
            &a,
            &caller.id,
            "session_get",
            json!({"session_id":other_id}),
            "revoked-worker-read"
        )
        .await["isError"],
        true
    );
}

#[tokio::test]
async fn lost_fleet_mutation_reply_is_unknown_and_never_automatically_resent() {
    let (socket, url) = listener().await;
    let (_dir, path, runtime) = fixture(
        &url,
        provider(|_| {
            Box::pin(async {
                Ok(tool(
                    "native__session_create",
                    "lost-create",
                    json!({"input":"one child","request_id":"durable-intent"}),
                ))
            })
        }),
    );
    let _server = serve(socket, runtime.clone(), None);
    let caller = runtime
        .create_session("engineer", None, "delegate")
        .unwrap();
    let inspector = runtime.create_session("engineer", None, "inspect").unwrap();
    let (board, broker) = board_fixture(&url, &url, true).await;
    enable_board(&path, &board.url);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(8), runtime.run(&caller.id))
            .await
            .unwrap()
            .unwrap()
            .status,
        SessionStatus::NeedsResolution
    );
    assert_eq!(broker.posts.load(Ordering::SeqCst), 1);
    assert_eq!(broker.rows.lock().unwrap().len(), 1);
    assert_eq!(runtime.tool_calls(&caller.id).unwrap()[0].status, "unknown");
    let listed = value(
        &invoke(
            &runtime,
            &inspector.id,
            "session_list",
            json!({}),
            "read-only-reconcile",
        )
        .await,
    );
    assert_eq!(listed["dispatches"].as_array().unwrap().len(), 1);
    assert_eq!(
        listed["dispatches"][0]["metadata"]["parent_session_id"],
        caller.id
    );
    assert_eq!(
        invoke(
            &runtime,
            &inspector.id,
            "session_resume",
            json!({"session_id":caller.id}),
            "no-unsafe-replay"
        )
        .await["isError"],
        true
    );
    assert_eq!(broker.posts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn native_create_http_accepts_immutable_fleet_binding_and_rejects_conflicting_retries() {
    let (socket, url) = listener().await;
    let (dir, path, runtime) = fixture(&url, provider(|_| Box::pin(async { Ok(answer("done")) })));
    let mut cfg: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    cfg["runtime_url"] = json!(format!("{url}/api/"));
    std::fs::write(&path, cfg.to_string()).unwrap();
    let _server = serve(socket, runtime.clone(), None);
    let cwd = dir.path().join("fleet-owned-worktree");
    std::fs::create_dir(&cwd).unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let intent = json!({"agent_id":"engineer","input":"one intent","task":"task_5","session_id":id,
        "host_id":"fixture-host","workspace_cwd":cwd,"workspace_mcp_url":"http://127.0.0.1:8791/mcp/{session_id}",
        "lifecycle_mcp_url":"{runtime_url}/mcp/sessions/{session_id}","generation":4,"metadata":{"dispatch_id":42}});
    let http = reqwest::Client::new();
    let response = http
        .post(format!("{url}/api/sessions"))
        .json(&intent)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    let created: Value = response.json().await.unwrap();
    assert_eq!(created["id"], id);
    assert_eq!(created["host_id"], "fixture-host");
    assert_eq!(created["workspace_cwd"], json!(cwd));
    assert_eq!(created["generation"], 4);
    wait(&runtime, &id, SessionStatus::Completed).await;
    let mut retry = intent.clone();
    retry.as_object_mut().unwrap().remove("session_id");
    retry["request_id"] = json!(id);
    assert!(
        http.post(format!("{url}/api/sessions"))
            .json(&retry)
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    assert_eq!(runtime.list_sessions().unwrap().len(), 1);
    retry["session_id"] = json!(id);
    assert_eq!(
        http.post(format!("{url}/api/sessions"))
            .json(&retry)
            .send()
            .await
            .unwrap()
            .status(),
        422
    );
    let mut changed = intent.clone();
    changed["input"] = json!("different intent");
    assert_eq!(
        http.post(format!("{url}/api/sessions"))
            .json(&changed)
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    let mut invalid = intent;
    invalid["metadata"] = json!("not-an-object");
    assert_eq!(
        http.post(format!("{url}/api/sessions"))
            .json(&invalid)
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert_eq!(runtime.list_sessions().unwrap().len(), 1);
    let profiles: Value = http
        .get(format!("{url}/api/agents"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(profiles[0]["max_retries"], Value::Null);
    assert_eq!(profiles[0]["max_output_tokens"], 128000);
    assert_eq!(profiles[0]["context_window_tokens"], 1050000);
    assert!(profiles[0].get("api_key_env").is_none());
    let before = json!(runtime.transcript(&id).unwrap());
    let rebound = http
        .post(format!("{url}/api/sessions/{id}/bind"))
        .json(&json!({"generation":5,"metadata":{"adopted":true}}))
        .send()
        .await
        .unwrap();
    assert_eq!(rebound.status(), 200);
    let rebound: Value = rebound.json().await.unwrap();
    assert_eq!(rebound["generation"], 5);
    assert_eq!(rebound["workspace_cwd"], json!(cwd));
    assert_eq!(json!(runtime.transcript(&id).unwrap()), before);
    assert_eq!(
        http.post(format!("{url}/api/sessions/{id}/bind"))
            .json(&json!({"generation":4}))
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    assert_eq!(
        http.post(format!("{url}/api/sessions/{id}/bind"))
            .json(&json!({"generation":5,"session_id":"different-id"}))
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
}

#[tokio::test]
async fn explicit_target_grants_and_wildcard_apply_without_changing_caller_identity() {
    let (socket, url) = listener().await;
    let (_dir, path, runtime) = fixture(&url, provider(|_| Box::pin(async { Ok(answer("done")) })));
    let _server = serve(socket, runtime.clone(), None);
    let caller = runtime.create_session("engineer", None, "manager").unwrap();
    let args = json!({"agent_id":"other","input":"worker","request_id":"targeted-intent"});
    assert_eq!(
        invoke(
            &runtime,
            &caller.id,
            "session_create",
            args.clone(),
            "ungranted"
        )
        .await["isError"],
        true
    );
    assert_eq!(runtime.list_sessions().unwrap().len(), 1);
    let mut cfg: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    cfg["agents"][0]["mcp_servers"][0]["allowed_agent_ids"] = json!(["other"]);
    std::fs::write(&path, cfg.to_string()).unwrap();
    let receipt = value(&invoke(&runtime, &caller.id, "session_create", args, "granted").await);
    let other_id = receipt["session_id"].as_str().unwrap();
    assert_eq!(receipt["agent_id"], "other");
    assert_eq!(receipt["parent_session_id"], caller.id);
    wait(&runtime, other_id, SessionStatus::Completed).await;
    let listed = value(
        &invoke(
            &runtime,
            &caller.id,
            "session_list",
            json!({"agent_id":"other"}),
            "worker-list",
        )
        .await,
    );
    assert_eq!(listed["sessions"][0]["id"], other_id);
    assert_eq!(
        runtime.get_session(&caller.id).unwrap().agent_id,
        "engineer"
    );
    cfg["agents"]
        .as_array_mut()
        .unwrap()
        .push(json!({"id":"new-worker","instructions":"New identity after runtime startup."}));
    std::fs::write(&path, cfg.to_string()).unwrap();
    let new_args =
        json!({"agent_id":"new-worker","input":"new worker","request_id":"new-identity-intent"});
    assert_eq!(
        invoke(
            &runtime,
            &caller.id,
            "session_create",
            new_args.clone(),
            "not-in-subset"
        )
        .await["isError"],
        true
    );
    cfg["agents"][0]["mcp_servers"][0]["allowed_agent_ids"] = json!(["*"]);
    std::fs::write(&path, cfg.to_string()).unwrap();
    let new_receipt = value(
        &invoke(
            &runtime,
            &caller.id,
            "session_create",
            new_args,
            "wildcard-grant",
        )
        .await,
    );
    assert_eq!(new_receipt["agent_id"], "new-worker");
    wait(
        &runtime,
        new_receipt["session_id"].as_str().unwrap(),
        SessionStatus::Completed,
    )
    .await;
    cfg["agents"][0]["mcp_servers"][0]["allowed_agent_ids"] = json!([]);
    std::fs::write(&path, cfg.to_string()).unwrap();
    assert_eq!(
        invoke(
            &runtime,
            &caller.id,
            "session_get",
            json!({"session_id":other_id}),
            "revoked"
        )
        .await["isError"],
        true
    );
    assert_eq!(
        runtime.get_session(&caller.id).unwrap().agent_id,
        "engineer"
    );
}
