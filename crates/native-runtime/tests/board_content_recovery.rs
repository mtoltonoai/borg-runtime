//! Real Board -> MCP -> durable native-loop regression for the October 20 stall.
//! Run against a freshly built, isolated Board binary, never a production Board:
//! BORG_TEST_BOARD_BINARY=$PWD/components/task-board/target/debug/task-board \
//!   cargo test --locked --test board_content_recovery -- --ignored
use native_runtime::{
    BoxFuture, Provider, ProviderError, ProviderRequest, ProviderResponse, Runtime, SessionStatus,
    mcp::McpToolExecutor,
};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct CommentProvider {
    task_id: i64,
    body: String,
    calls: AtomicUsize,
    results: Mutex<Vec<Value>>,
}
impl Provider for CommentProvider {
    fn respond(
        &self,
        request: ProviderRequest,
    ) -> BoxFuture<'_, Result<ProviderResponse, ProviderError>> {
        Box::pin(async move {
            let index = self.calls.fetch_add(1, Ordering::SeqCst);
            let output = if index == 0 {
                vec![
                    json!({"type":"function_call", "id":"fc-comment", "call_id":"comment",
                    "name":"board__comment_task", "arguments":json!({
                        "task_id":self.task_id,"body":self.body
                    }).to_string()}),
                ]
            } else {
                assert_eq!(index, 1, "no tool replay or repeat recovery turn");
                let result = request
                    .input
                    .iter()
                    .find(|item| item["type"] == "function_call_output")
                    .expect("model receives a durable tool result");
                self.results.lock().unwrap().push(
                    serde_json::from_str(result["output"].as_str().expect("tool result JSON text"))
                        .unwrap(),
                );
                vec![
                    json!({"type":"message", "id":"message-done", "role":"assistant",
                    "content":[{"type":"output_text","text":"Original outcome received; continued."}]}),
                ]
            };
            Ok(ProviderResponse {
                id: Some(format!("response-{index}")),
                output,
                usage: None,
                status: "completed".into(),
            })
        })
    }
}

async fn session(base: &str, task_id: i64, body: &str) -> (SessionStatus, Value, usize) {
    let directory = tempfile::tempdir().unwrap();
    let config = directory.path().join("runtime.json");
    std::fs::write(
        &config,
        serde_json::to_vec(&json!({
            "database_path":directory.path().join("native.sqlite"),
            "agents":[{"id":"test-engineer","instructions":"Run the supplied test.",
                "mcp_servers":[{"name":"board","url":format!("{base}/mcp"),
                    "tools_allowlist":["comment_task"]}]}]
        }))
        .unwrap(),
    )
    .unwrap();
    let provider = Arc::new(CommentProvider {
        task_id,
        body: body.into(),
        calls: AtomicUsize::new(0),
        results: Mutex::new(Vec::new()),
    });
    let runtime = Runtime::with_backends(
        &config,
        provider.clone(),
        Arc::new(McpToolExecutor::with_timeout(Duration::from_secs(5)).unwrap()),
    )
    .unwrap();
    let created = runtime
        .create_session(
            "test-engineer",
            Some(format!("task_{task_id}")),
            "Test comment.",
        )
        .unwrap();
    let finished = tokio::time::timeout(Duration::from_secs(20), runtime.run(&created.id))
        .await
        .unwrap()
        .unwrap();
    let calls = runtime.tool_calls(&created.id).unwrap();
    assert_eq!(calls.len(), 1, "exactly one original tool intent");
    let tool = serde_json::to_value(&calls[0]).unwrap();
    if finished.status == SessionStatus::Completed {
        assert_eq!(tool["status"], "succeeded");
        assert_eq!(provider.results.lock().unwrap().len(), 1);
        assert_eq!(
            provider.results.lock().unwrap()[0],
            tool["result"],
            "same persisted result reaches model"
        );
    } else {
        assert_eq!(tool["status"], "unknown");
        assert!(
            runtime.resume(&created.id).is_err(),
            "unknown effect is never auto-replayed"
        );
    }
    (finished.status, tool, provider.calls.load(Ordering::SeqCst))
}

#[tokio::test]
#[ignore = "requires BORG_TEST_BOARD_BINARY; spawns an isolated real Board"]
async fn board_comments_continue_native_sessions_without_weakening_unknown_effects() {
    let binary = PathBuf::from(std::env::var("BORG_TEST_BOARD_BINARY").expect("Board binary path"));
    let directory = tempfile::tempdir().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let config = directory.path().join("board.toml");
    std::fs::write(
        &config,
        format!(
            "db_path = {:?}\nhost = \"127.0.0.1\"\nport = {port}\n",
            directory.path().join("board.sqlite").to_str().unwrap()
        ),
    )
    .unwrap();
    let mut board = tokio::process::Command::new(binary)
        .arg("--config")
        .arg(&config)
        .env_remove("LISTEN_FDS")
        .env_remove("LISTEN_PID")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            assert!(
                board.try_wait().unwrap().is_none(),
                "Board exited before readiness"
            );
            if let Ok(response) = client.get(format!("{base}/api/health")).send().await {
                if response.status().is_success() {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let project: Value = client
        .post(format!("{base}/api/projects"))
        .json(&json!({"name":"Content recovery regression"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let task: Value = client
        .post(format!("{base}/api/tasks"))
        .json(&json!({"project_id":project["id"],"title":"Original comment receipt"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let task_id = task["id"].as_i64().unwrap();
    let text = "GET /api/sessions – PM has the exact receipt";
    let (status, tool, turns) = session(&base, task_id, text).await;
    assert_eq!(status, SessionStatus::Completed);
    assert_eq!(turns, 2);
    assert_ne!(tool["result"]["isError"], true);
    client
        .post(format!("{base}/api/system/content-policy"))
        .json(&json!({"enforce_style":true}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let (status, tool, turns) = session(&base, task_id, text).await;
    assert_eq!(
        status,
        SessionStatus::Completed,
        "definite refusal does not strand native driver"
    );
    assert_eq!(turns, 2);
    assert_eq!(tool["result"]["isError"], true);
    let diagnostic: Value =
        serde_json::from_str(tool["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(diagnostic["committed"], false);
    assert!(
        diagnostic["message"]
            .as_str()
            .unwrap()
            .contains("non-ASCII")
    );
    let receipt: Value = client
        .get(format!("{base}/api/tasks/{task_id}"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        receipt["comment_count"], 1,
        "accepted original exists once; refusal never committed"
    );
    let (status, _, turns) = session(&base, -999999, "valid ascii").await;
    assert_eq!(
        status,
        SessionStatus::NeedsResolution,
        "generic internal error still requires reconciliation"
    );
    assert_eq!(turns, 1);
    board.kill().await.unwrap();
    board.wait().await.unwrap();
}
