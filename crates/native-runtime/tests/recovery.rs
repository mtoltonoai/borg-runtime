use native_runtime::{
    BoxFuture, McpServerConfig, Provider, ProviderError, ProviderRequest, ProviderResponse, Runtime,
    SessionStatus, ToolExecutor,
};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tempfile::TempDir;
use tokio::sync::{Barrier, Notify};

struct Fixture {
    _directory: TempDir,
    config: PathBuf,
}

impl Fixture {
    fn new(max_retries: u32) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let config = directory.path().join("config.json");
        std::fs::write(
            &config,
            serde_json::to_vec(&json!({
                "database_path": directory.path().join("runtime.sqlite"),
                "agents": [{
                    "id": "engineer",
                    "model": "mock-model",
                    "instructions": "Use the supplied tool when needed.",
                    "max_turns": 1,
                    "max_retries": max_retries,
                    "retry_initial_ms": 1
                }]
            }))
            .unwrap(),
        )
        .unwrap();
        Self {
            _directory: directory,
            config,
        }
    }

    fn runtime(&self, provider: Arc<dyn Provider>, tools: Arc<dyn ToolExecutor>) -> Runtime {
        Runtime::with_backends(&self.config, provider, tools).unwrap()
    }
}

fn completed(text: &str) -> ProviderResponse {
    ProviderResponse {
        id: Some(format!("response-{text}")),
        output: vec![json!({
            "id": format!("message-{text}"),
            "type": "message",
            "role": "assistant",
            "content": [{"type": "output_text", "text": text}]
        })],
        usage: Some(json!({"input_tokens": 3, "output_tokens": 2})),
        status: "completed".into(),
    }
}

fn reasoning() -> Value {
    json!({
        "id": "reasoning-original",
        "type": "reasoning",
        "encrypted_content": "opaque-test-payload-must-survive",
        "summary": [{"type": "summary_text", "text": "A synthetic public summary."}],
        "future_provider_field": {"must": ["survive", 17]}
    })
}

fn tool_turn() -> ProviderResponse {
    ProviderResponse {
        id: Some("response-tool".into()),
        output: vec![
            reasoning(),
            json!({
                "id": "function-original",
                "type": "function_call",
                "call_id": "call-original",
                "name": "record",
                "arguments": "{\"value\":\"sentinel\"}"
            }),
        ],
        usage: None,
        status: "completed".into(),
    }
}

struct ScriptProvider {
    replies: Mutex<VecDeque<Result<ProviderResponse, ProviderError>>>,
    requests: Mutex<Vec<ProviderRequest>>,
}

impl ScriptProvider {
    fn new(replies: Vec<Result<ProviderResponse, ProviderError>>) -> Self {
        Self {
            replies: Mutex::new(replies.into()),
            requests: Mutex::new(Vec::new()),
        }
    }
}

impl Provider for ScriptProvider {
    fn respond(
        &self,
        request: ProviderRequest,
    ) -> BoxFuture<'_, Result<ProviderResponse, ProviderError>> {
        Box::pin(async move {
            self.requests.lock().unwrap().push(request);
            self.replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("runtime issued an unexpected extra provider request")
        })
    }
}

#[derive(Debug, Clone)]
struct ToolInvocation {
    session_id: String,
    operation_id: String,
    arguments: Value,
}

#[derive(Default)]
struct RecordingTools {
    invocations: Mutex<Vec<ToolInvocation>>,
    ambiguous_failure: bool,
}

impl ToolExecutor for RecordingTools {
    fn list_tools<'a>(
        &'a self,
        _session_id: &'a str,
        _servers: &'a [McpServerConfig],
    ) -> BoxFuture<'a, Result<Vec<Value>, String>> {
        Box::pin(async {
            Ok(vec![json!({
                "type": "function",
                "name": "record",
                "description": "Record a test value.",
                "parameters": {
                    "type": "object",
                    "properties": {"value": {"type": "string"}},
                    "required": ["value"],
                    "additionalProperties": false
                }
            })])
        })
    }

    fn call<'a>(
        &'a self,
        session_id: &'a str,
        _servers: &'a [McpServerConfig],
        name: &'a str,
        arguments: Value,
        operation_id: &'a str,
    ) -> BoxFuture<'a, Result<Value, String>> {
        Box::pin(async move {
            assert_eq!(name, "record");
            self.invocations.lock().unwrap().push(ToolInvocation {
                session_id: session_id.into(),
                operation_id: operation_id.into(),
                arguments,
            });
            if self.ambiguous_failure {
                Err("tool may have committed; its response was lost".into())
            } else {
                Ok(json!({"recorded": "sentinel"}))
            }
        })
    }
}

struct ConcurrentProvider {
    rendezvous: Barrier,
    requests: Mutex<Vec<ProviderRequest>>,
}

impl Provider for ConcurrentProvider {
    fn respond(
        &self,
        request: ProviderRequest,
    ) -> BoxFuture<'_, Result<ProviderResponse, ProviderError>> {
        Box::pin(async move {
            let input = serde_json::to_string(&request.input).unwrap();
            let answer = if input.contains("session-alpha") {
                assert!(!input.contains("session-beta"));
                "answer-alpha"
            } else {
                assert!(input.contains("session-beta"));
                "answer-beta"
            };
            self.requests.lock().unwrap().push(request);
            self.rendezvous.wait().await;
            Ok(completed(answer))
        })
    }
}

struct DropSignal(Arc<AtomicBool>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct PendingProvider {
    entered: Notify,
    dropped: Arc<AtomicBool>,
    calls: AtomicUsize,
}

struct GatedProvider {
    entered: Notify,
    release: Notify,
    calls: AtomicUsize,
}

impl Provider for GatedProvider {
    fn respond(
        &self,
        _request: ProviderRequest,
    ) -> BoxFuture<'_, Result<ProviderResponse, ProviderError>> {
        Box::pin(async move {
            assert_eq!(
                self.calls.fetch_add(1, Ordering::SeqCst),
                0,
                "concurrent drivers must not duplicate the model request"
            );
            self.entered.notify_one();
            self.release.notified().await;
            Ok(completed("one-shared-execution"))
        })
    }
}

impl Provider for PendingProvider {
    fn respond(
        &self,
        _request: ProviderRequest,
    ) -> BoxFuture<'_, Result<ProviderResponse, ProviderError>> {
        Box::pin(async move {
            let _drop_signal = DropSignal(self.dropped.clone());
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            std::future::pending().await
        })
    }
}

#[tokio::test]
async fn two_sessions_of_one_agent_reach_inference_concurrently_and_stay_isolated() {
    let fixture = Fixture::new(0);
    let provider = Arc::new(ConcurrentProvider {
        rendezvous: Barrier::new(2),
        requests: Mutex::new(Vec::new()),
    });
    let tools = Arc::new(RecordingTools::default());
    let runtime = fixture.runtime(provider.clone(), tools.clone());
    let alpha = runtime
        .create_session("engineer", Some("task-alpha".into()), "session-alpha")
        .unwrap();
    let beta = runtime
        .create_session("engineer", Some("task-beta".into()), "session-beta")
        .unwrap();
    assert_ne!(alpha.id, beta.id);
    assert_eq!(runtime.list_sessions().unwrap().len(), 2);

    // Both responses wait for the other request. Serializing by agent instead
    // of by session deadlocks this barrier and fails the bounded test.
    let (alpha_run, beta_run) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(runtime.run(&alpha.id), runtime.run(&beta.id))
    })
    .await
    .expect("same-agent sessions must be able to run simultaneously");
    assert_eq!(alpha_run.unwrap().status, SessionStatus::Completed);
    assert_eq!(beta_run.unwrap().status, SessionStatus::Completed);
    {
        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        for request in requests.iter() {
            let (own, other) = if serde_json::to_string(&request.input)
                .unwrap()
                .contains("session-alpha")
            {
                (&alpha, &beta)
            } else {
                (&beta, &alpha)
            };
            let (header, instructions) = request
                .instructions
                .split_once("\n\n")
                .expect("runtime context must precede the agent instructions");
            let identity: Value = serde_json::from_str(
                header
                    .strip_prefix("Runtime identity and task context: ")
                    .expect("runtime identity prefix must be explicit"),
            )
            .unwrap();
            assert_eq!(
                identity,
                json!({
                    "agent_id": "engineer",
                    "session_id": own.id,
                    "task": own.task
                })
            );
            assert_eq!(instructions, "Use the supplied tool when needed.");
            assert!(!request.instructions.contains(&other.id));
            assert!(!request.instructions.contains(other.task.as_deref().unwrap()));
        }
    }
    assert!(tools.invocations.lock().unwrap().is_empty());

    for (session, own_answer, other_answer) in [
        (&alpha, "answer-alpha", "answer-beta"),
        (&beta, "answer-beta", "answer-alpha"),
    ] {
        let transcript = serde_json::to_value(runtime.transcript(&session.id).unwrap()).unwrap();
        let encoded = transcript.to_string();
        assert!(encoded.contains(own_answer));
        assert!(!encoded.contains(other_answer));
        assert_eq!(runtime.get_session(&session.id).unwrap().task, session.task);
    }
}

#[tokio::test]
async fn raw_reasoning_survives_tool_execution_reopen_and_the_next_model_input() {
    let fixture = Fixture::new(0);
    let provider = Arc::new(ScriptProvider::new(vec![
        Ok(tool_turn()),
        Ok(completed("finished")),
    ]));
    let tools = Arc::new(RecordingTools::default());
    let runtime = fixture.runtime(provider.clone(), tools.clone());
    let session = runtime
        .create_session("engineer", None, "Record a sentinel, then summarize.")
        .unwrap();

    // The explicit one-turn fixture budget pauses after the committed tool
    // result, permitting a real database reopen before the second model call.
    assert_eq!(
        runtime.run(&session.id).await.unwrap().status,
        SessionStatus::Waiting
    );
    assert_eq!(provider.requests.lock().unwrap().len(), 1);
    assert_eq!(tools.invocations.lock().unwrap().len(), 1);
    let before = serde_json::to_value(runtime.transcript(&session.id).unwrap()).unwrap();
    drop(runtime);

    let reopened = fixture.runtime(provider.clone(), tools.clone());
    assert_eq!(
        serde_json::to_value(reopened.transcript(&session.id).unwrap()).unwrap(),
        before,
        "recovery must preserve complete transcript entries, not rendered summaries"
    );
    reopened.resume(&session.id).unwrap();
    assert_eq!(
        reopened.run(&session.id).await.unwrap().status,
        SessionStatus::Completed
    );
    assert_eq!(tools.invocations.lock().unwrap().len(), 1);
    let requests = provider.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    let input = &requests[1].input;
    let reasoning_index = input.iter().position(|item| item == &reasoning()).unwrap();
    let call_index = input
        .iter()
        .position(|item| item["type"] == "function_call")
        .unwrap();
    let result_index = input
        .iter()
        .position(|item| item["type"] == "function_call_output")
        .unwrap();
    assert!(reasoning_index < call_index && call_index < result_index);
    assert_eq!(input[call_index]["call_id"], "call-original");
    assert_eq!(input[result_index]["call_id"], "call-original");
    assert_eq!(
        serde_json::from_str::<Value>(input[result_index]["output"].as_str().unwrap()).unwrap(),
        json!({"recorded": "sentinel"})
    );
    assert_eq!(input.iter().filter(|item| *item == &reasoning()).count(), 1);
}

#[tokio::test]
async fn ambiguous_tool_and_interrupted_intent_require_resolution_without_replay() {
    for interrupted_before_error_was_saved in [false, true] {
        let fixture = Fixture::new(0);
        let provider = Arc::new(ScriptProvider::new(vec![
            Ok(tool_turn()),
            Ok(completed("resolved")),
        ]));
        let tools = Arc::new(RecordingTools {
            ambiguous_failure: true,
            ..Default::default()
        });
        let runtime = fixture.runtime(provider.clone(), tools.clone());
        let session = runtime
            .create_session("engineer", Some("original-task".into()), "Record once.")
            .unwrap();
        assert_eq!(
            runtime.run(&session.id).await.unwrap().status,
            SessionStatus::NeedsResolution
        );
        let original = tools.invocations.lock().unwrap()[0].clone();
        assert_eq!(original.session_id, session.id);
        assert_eq!(original.arguments, json!({"value": "sentinel"}));
        assert!(!original.operation_id.is_empty());
        let calls = runtime.tool_calls(&session.id).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].status, "unknown");
        assert_eq!(calls[0].operation_id, original.operation_id);
        assert!(calls[0].result.is_none());
        drop(runtime);

        if interrupted_before_error_was_saved {
            // Reconstruct the durable crash boundary after dispatch but before
            // an error/result was saved. Keep the original call, arguments and
            // operation ID; only the unfinished "running" intent survives.
            let database = fixture._directory.path().join("runtime.sqlite");
            let mut connection = rusqlite::Connection::open(database).unwrap();
            let transaction = connection.transaction().unwrap();
            for (table, query, update) in [
                (
                    "session",
                    "SELECT body FROM sessions WHERE id=?1",
                    "UPDATE sessions SET body=?2 WHERE id=?1",
                ),
                (
                    "tool",
                    "SELECT body FROM tool_calls WHERE session_id=?1",
                    "UPDATE tool_calls SET body=?2 WHERE session_id=?1",
                ),
            ] {
                let body: String = transaction
                    .query_row(query, [&session.id], |row| row.get(0))
                    .unwrap();
                let mut value: Value = serde_json::from_str(&body).unwrap();
                let record = if table == "tool" {
                    &mut value["call"]
                } else {
                    &mut value
                };
                record["status"] = json!("running");
                record["error"] = Value::Null;
                transaction
                    .execute(update, rusqlite::params![session.id, value.to_string()])
                    .unwrap();
            }
            transaction.commit().unwrap();
        }

        let reopened = fixture.runtime(provider.clone(), tools.clone());
        assert_eq!(
            reopened.get_session(&session.id).unwrap().status,
            SessionStatus::NeedsResolution
        );
        assert!(reopened.resume(&session.id).is_err());
        assert_eq!(
            reopened.run(&session.id).await.unwrap().status,
            SessionStatus::NeedsResolution
        );
        assert_eq!(provider.requests.lock().unwrap().len(), 1);
        assert_eq!(tools.invocations.lock().unwrap().len(), 1);
        let recovered = reopened.tool_calls(&session.id).unwrap();
        assert_eq!(recovered[0].status, "unknown");
        assert_eq!(recovered[0].operation_id, original.operation_id);
        assert_eq!(recovered[0].arguments, original.arguments);

        let result = json!({"recorded": "sentinel", "reconciled": true});
        reopened
            .resolve_tool(&session.id, "call-original", result.clone())
            .unwrap();
        // Retrying the same resolution is idempotent; changing the committed
        // original result must fail instead of silently rewriting evidence.
        reopened
            .resolve_tool(&session.id, "call-original", result.clone())
            .unwrap();
        assert!(
            reopened
                .resolve_tool(&session.id, "call-original", json!({"different": true}))
                .is_err()
        );
        reopened.resume(&session.id).unwrap();
        assert_eq!(
            reopened.run(&session.id).await.unwrap().status,
            SessionStatus::Completed
        );
        assert_eq!(tools.invocations.lock().unwrap().len(), 1);
        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        let outputs: Vec<_> = requests[1]
            .input
            .iter()
            .filter(|item| item["type"] == "function_call_output")
            .collect();
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0]["call_id"], "call-original");
        assert_eq!(
            serde_json::from_str::<Value>(outputs[0]["output"].as_str().unwrap()).unwrap(),
            result
        );
        assert_eq!(
            reopened
                .transcript(&session.id)
                .unwrap()
                .iter()
                .filter(|entry| entry.item["type"] == "function_call_output")
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn transient_provider_failure_exhausts_a_finite_retry_budget() {
    let fixture = Fixture::new(2);
    let provider = Arc::new(ScriptProvider::new(
        (0..3)
            .map(|_| {
                Err(ProviderError {
                    message: "temporary provider failure".into(),
                    retryable: true,
                })
            })
            .collect(),
    ));
    let tools = Arc::new(RecordingTools::default());
    let runtime = fixture.runtime(provider.clone(), tools.clone());
    let session = runtime.create_session("engineer", None, "Try once.").unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), runtime.run(&session.id))
        .await
        .expect("retry budget must terminate")
        .unwrap();
    assert_eq!(result.status, SessionStatus::Failed);
    let requests = provider.requests.lock().unwrap();
    assert_eq!(requests.len(), 3, "initial request plus exactly two retries");
    assert!(requests.windows(2).all(|pair| pair[0].input == pair[1].input));
    assert!(tools.invocations.lock().unwrap().is_empty());
    assert!(runtime.tool_calls(&session.id).unwrap().is_empty());
    let transcript = runtime.transcript(&session.id).unwrap();
    assert_eq!(transcript.iter().filter(|entry| entry.kind == "retry").count(), 2);
    assert!(!transcript.iter().any(|entry| entry.kind == "output"));
    drop(requests);
    // Calling the driver again is not implicit permission to renew a failed run.
    assert_eq!(
        runtime.run(&session.id).await.unwrap().status,
        SessionStatus::Failed
    );
    assert_eq!(provider.requests.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn stop_cancels_inflight_inference_and_persists_a_stopped_session() {
    let fixture = Fixture::new(0);
    let provider = Arc::new(PendingProvider {
        entered: Notify::new(),
        dropped: Arc::new(AtomicBool::new(false)),
        calls: AtomicUsize::new(0),
    });
    let tools = Arc::new(RecordingTools::default());
    let runtime = fixture.runtime(provider.clone(), tools.clone());
    let session = runtime.create_session("engineer", None, "Wait for stop.").unwrap();
    let driver = {
        let runtime = runtime.clone();
        let id = session.id.clone();
        tokio::spawn(async move { runtime.run(&id).await })
    };
    tokio::time::timeout(Duration::from_secs(5), provider.entered.notified())
        .await
        .expect("provider future must have been polled before stop");
    runtime.stop(&session.id).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .expect("stop must cancel pending inference")
        .unwrap()
        .unwrap();
    assert_eq!(result.status, SessionStatus::Stopped);
    assert!(provider.dropped.load(Ordering::SeqCst));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert!(tools.invocations.lock().unwrap().is_empty());
    assert!(
        !runtime
            .transcript(&session.id)
            .unwrap()
            .iter()
            .any(|entry| entry.kind == "output")
    );
    drop(runtime);
    let reopened = fixture.runtime(provider, tools);
    assert_eq!(
        reopened.get_session(&session.id).unwrap().status,
        SessionStatus::Stopped
    );
}

#[tokio::test]
async fn incomplete_responses_retry_committed_input_without_tools_and_exhaust_finitely() {
    for eventually_completes in [true, false] {
        let fixture = Fixture::new(2);
        let partial = ProviderResponse {
            id: Some("response-incomplete".into()),
            output: vec![
                reasoning(),
                json!({
                    "id": "partial-function",
                    "type": "function_call",
                    "call_id": "partial-call",
                    "name": "record",
                    "status": "in_progress",
                    "arguments": "{\"value\":"
                }),
            ],
            usage: Some(json!({"input_tokens": 3, "output_tokens": 1})),
            status: "incomplete".into(),
        };
        let replies = if eventually_completes {
            vec![Ok(partial.clone()), Ok(completed("complete-after-retry"))]
        } else {
            vec![Ok(partial.clone()), Ok(partial.clone()), Ok(partial.clone())]
        };
        let provider = Arc::new(ScriptProvider::new(replies));
        let tools = Arc::new(RecordingTools::default());
        let runtime = fixture.runtime(provider.clone(), tools.clone());
        let session = runtime
            .create_session("engineer", None, "Only complete responses may execute tools.")
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(5), runtime.run(&session.id))
            .await
            .expect("incomplete-response retries must have a finite budget")
            .unwrap();
        assert_eq!(
            outcome.status,
            if eventually_completes {
                SessionStatus::Completed
            } else {
                SessionStatus::Failed
            }
        );
        assert_eq!(outcome.turns, if eventually_completes { 1 } else { 0 });
        assert!(tools.invocations.lock().unwrap().is_empty());
        assert!(runtime.tool_calls(&session.id).unwrap().is_empty());

        let expected_requests = if eventually_completes { 2 } else { 3 };
        {
            let requests = provider.requests.lock().unwrap();
            assert_eq!(requests.len(), expected_requests);
            assert!(requests.windows(2).all(|pair| pair[0].input == pair[1].input));
            assert!(
                requests
                    .iter()
                    .all(|request| request.input.iter().all(|item| item != &reasoning()
                        && item["call_id"] != "partial-call")),
                "partial output is diagnostic evidence, not committed model input"
            );
        }
        let transcript = runtime.transcript(&session.id).unwrap();
        let diagnostics: Vec<_> = transcript
            .iter()
            .filter(|entry| entry.kind == "incomplete_response")
            .map(|entry| entry.item.clone())
            .collect();
        assert_eq!(diagnostics.len(), if eventually_completes { 1 } else { 3 });
        for diagnostic in &diagnostics {
            assert_eq!(diagnostic["id"], "response-incomplete");
            assert_eq!(diagnostic["status"], "incomplete");
            assert_eq!(diagnostic["output"], json!(partial.output));
            assert_eq!(diagnostic["usage"], json!(partial.usage));
        }
        assert_eq!(
            transcript.iter().filter(|entry| entry.kind == "retry").count(),
            expected_requests - 1
        );
        assert!(
            !transcript.iter().any(|entry| entry.kind == "output"
                && (entry.item == reasoning() || entry.item["call_id"] == "partial-call"))
        );
        drop(runtime);

        let reopened = fixture.runtime(provider.clone(), tools.clone());
        let retained: Vec<_> = reopened
            .transcript(&session.id)
            .unwrap()
            .into_iter()
            .filter(|entry| entry.kind == "incomplete_response")
            .map(|entry| entry.item)
            .collect();
        assert_eq!(retained, diagnostics);
        // Neither a completed run nor exhausted retries are implicitly renewed.
        assert_eq!(
            reopened.run(&session.id).await.unwrap().status,
            outcome.status
        );
        assert_eq!(provider.requests.lock().unwrap().len(), expected_requests);
        assert!(tools.invocations.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn simultaneous_drivers_of_one_session_queue_and_share_one_provider_request() {
    let fixture = Fixture::new(0);
    let provider = Arc::new(GatedProvider {
        entered: Notify::new(),
        release: Notify::new(),
        calls: AtomicUsize::new(0),
    });
    let tools = Arc::new(RecordingTools::default());
    let runtime = fixture.runtime(provider.clone(), tools.clone());
    let session = runtime
        .create_session("engineer", None, "Run this session once.")
        .unwrap();
    let first = {
        let runtime = runtime.clone();
        let id = session.id.clone();
        tokio::spawn(async move { runtime.run(&id).await })
    };
    tokio::time::timeout(Duration::from_secs(5), provider.entered.notified())
        .await
        .expect("first driver must enter inference");

    let second = runtime.run(&session.id);
    tokio::pin!(second);
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut second)
            .await
            .is_err(),
        "second driver must wait for the active run, not return its Running snapshot"
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    provider.release.notify_one();
    let first_result = tokio::time::timeout(Duration::from_secs(5), first)
        .await
        .expect("first driver must finish after its provider is released")
        .unwrap()
        .unwrap();
    let second_result = tokio::time::timeout(Duration::from_secs(5), &mut second)
        .await
        .expect("queued driver must observe the completed run")
        .unwrap();
    assert_eq!(first_result.status, SessionStatus::Completed);
    assert_eq!(second_result.status, SessionStatus::Completed);
    assert_eq!(first_result.id, second_result.id);
    assert_eq!(first_result.turns, 1);
    assert_eq!(second_result.turns, 1);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert!(tools.invocations.lock().unwrap().is_empty());
    assert_eq!(
        runtime
            .transcript(&session.id)
            .unwrap()
            .iter()
            .filter(|entry| entry.kind == "output" && entry.item["type"] == "message")
            .count(),
        1
    );
}
