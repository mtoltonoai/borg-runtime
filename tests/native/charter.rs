//! Actual controlled Responses transport across all three native producers.
//! These rendering/capture fixtures are not controller BeginTurn permission;
//! the separate controller/worker cases qualify durable boundary adoption.
use super::*;
use borg_runtime::native::{CharterExchange, CharterInput, NativeLoopProfile, NativeLoopStep,
    ValidationHost, execute_step_observed_with_host, execute_loop_step,
    request_current_task, execute_charter_compaction};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

fn host(exchange: Arc<CharterExchange>) -> ValidationHost {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!("borg-charter-native-{}-{}-{}",
        std::process::id(), now_ms(), NEXT.fetch_add(1, Ordering::Relaxed)));
    std::fs::create_dir(&root).unwrap();
    ValidationHost::new(std::path::PathBuf::from(env!("CARGO_BIN_EXE_borg-runtime")), root,
        validation_policy(), validation_runtime()).with_charter(exchange)
}
fn exchange(changed: bool) -> Arc<CharterExchange> {
    Arc::new(CharterExchange::new(CharterInput {
        revision: json!({"definition_id":"agent","definition_revision":"revision-2"}),
        source: "# Purpose\n\nThe agent MUST preserve the exact task instruction.\n".into(), changed,
    }, &metadata().execution.native).unwrap())
}
fn declaration() -> Value {
    json!({"revision":{"definition_id":"agent","definition_revision":"revision-2"},
        "clauses":[{"file":"charter.md","section":"Purpose","sentence":"The agent MUST preserve the exact task instruction."}]})
}
fn wrapped(result: Value, references: Value) -> Value {
    let mut value = with_usage(envelope(&json!({"charter_references":references,"result":result}).to_string()));
    value["id"] = json!("actual-response-17");
    value["output"][0]["id"] = json!("actual-message-19");
    value
}

#[tokio::test]
//= spec/impl/v2/implementation.md#definitions-carry-reusable-charters
//= type=test
//# The native executor MUST deliver the effective charter to its model.
async fn plain_and_loop_producers_preserve_actual_reply_and_separate_charter_from_task() {
    for is_loop in [false, true] {
        let input = bind(task());
        let original = input.clone();
        let exchange = exchange(false);
        let original_reply = wrapped(json!({"result":"good"}), declaration());
        let peer = Fixture::start(200, serde_json::to_vec(&original_reply).unwrap(), Duration::ZERO, "").await;
        let host = host(exchange.clone());
        let cfg = config(peer.endpoint.clone());
        let output = if is_loop {
            let value = execute_loop_step(&input, &cfg, &NativeLoopProfile::default(), None, &host,
                async { Ok(()) }, std::future::pending()).await;
            match value.result.unwrap() { NativeLoopStep::Complete { result, .. } => result.output,
                _ => panic!("unexpected tool") }
        } else {
            let value = execute_step_observed_with_host(&input, &cfg, None, &host, std::future::pending()).await;
            match value.result.unwrap() { NativeStep::Complete(result) => result.output,
                _ => panic!("unexpected delegation") }
        };
        assert_eq!(output, json!({"result":"good"}));
        let request = peer.received.await.unwrap();
        assert_eq!(request["input"][1]["content"], input.instruction);
        assert!(request["input"][0]["content"].as_str().unwrap().contains("revision-2"));
        assert_eq!(request["reasoning"]["effort"], input.definition.required_inference().unwrap().reasoning_effort);
        assert_eq!(input, original);
        let capture = exchange.take_reply().unwrap().unwrap();
        assert_eq!(capture.response_id.as_deref(), Some("actual-response-17"));
        assert_eq!(capture.declared, Some(declaration()));
        assert_eq!(capture.original_output, Some(original_reply["output"].clone()));
        assert!(capture.call_ids.is_empty());
    }
}

#[tokio::test]
async fn fixed_tool_captures_real_call_without_changing_arguments_or_inventing_metadata() {
    let mut input = task();
    input.native_workflow = Some(Box::new(NativeWorkflow::InspectCurrentTaskThenContinue {}));
    input.limits.model_requests = 2;
    let input = bind(input);
    let exchange = exchange(false);
    let reply = with_usage(json!({"id":"actual-tool-reply","status":"completed","output":[
        {"type":"function_call","id":"actual-item","status":"completed","call_id":"actual-call",
        "name":"get_current_task","arguments":"{}"}]}));
    let peer = Fixture::start(200, serde_json::to_vec(&reply).unwrap(), Duration::ZERO, "").await;
    let value = request_current_task(&input, &config(peer.endpoint.clone()), &host(exchange.clone()),
        async { Ok(()) }, std::future::pending()).await;
    assert_eq!(value.result.unwrap().call_id(), "actual-call");
    let request = peer.received.await.unwrap();
    assert_eq!(request["tools"][0]["parameters"]["properties"], json!({}));
    let capture = exchange.take_reply().unwrap().unwrap();
    assert_eq!(capture.call_ids, ["actual-call"]);
    assert_eq!(capture.declared, None);
    assert_eq!(capture.original_output, Some(reply["output"].clone()));
}

#[tokio::test]
async fn malformed_references_do_not_change_valid_task_result_or_create_feedback() {
    let input = bind(task());
    let exchange = exchange(false);
    let peer = Fixture::start(200, serde_json::to_vec(&wrapped(json!({"result":"good"}), json!(17))).unwrap(), Duration::ZERO, "").await;
    let value = execute_step_observed_with_host(&input, &config(peer.endpoint.clone()), None,
        &host(exchange.clone()), std::future::pending()).await;
    assert!(matches!(value.result, Ok(NativeStep::Complete(_))));
    assert_eq!(exchange.take_reply().unwrap().unwrap().declared, Some(json!(17)));
    let _one_request = peer.received.await.unwrap();
}

#[tokio::test]
async fn local_capture_refusal_preserves_actual_reported_usage() {
    let input=bind(task());let exchange=exchange(false);
    let credential=std::env::var("HOME").unwrap();
    let first=credential.chars().next().unwrap();
    let escaped=format!("\\u{:04x}{}",first as u32,&credential[first.len_utf8()..]);
    let text=format!(r#"{{"charter_references":{{"invalid":"{escaped}"}},"result":{{"result":"good"}}}}"#);
    let original=with_usage(envelope(&text));
    assert!(!original.to_string().contains(&credential));
    let peer=Fixture::start(200,serde_json::to_vec(&original).unwrap(),Duration::ZERO,"").await;
    let value=execute_step_observed_with_host(&input,&config(peer.endpoint.clone()),None,
        &host(exchange.clone()),std::future::pending()).await;
    assert_eq!(value.usage,reported());
    assert_eq!(value.provider_outcome,ProviderOutcome::Completed);
    assert!(value.result.is_err());
    assert_eq!(exchange.take_failure().unwrap().as_deref(),Some("native_output_contains_credential"));
    assert!(exchange.take_reply().unwrap().is_none());
    let _one_post=peer.received.await.unwrap();
}

#[tokio::test]
//= spec/impl/v2/implementation.md#native-charter-changes-preserve-prompt-continuity
//= type=test
//# A native charter change MUST be applied as a mid-conversation instruction.
//= spec/impl/v2/implementation.md#native-charter-changes-preserve-prompt-continuity
//= type=test
//# A native compaction that follows a charter change MUST write its summary under the new charter.
async fn real_summary_uses_selected_inference_without_tools_and_retains_new_source() {
    let input = bind(task());
    let exchange = exchange(true);
    let peer = Fixture::start(200, serde_json::to_vec(&wrapped(json!({"summary":"One task and its actual result remain."}), declaration())).unwrap(), Duration::ZERO, "").await;
    let context = json!([{"role":"user","content":"actual prior task"},
        {"type":"function_call_output","call_id":"actual-call","output":"actual committed result"}]);
    let value = execute_charter_compaction(&input, &config(peer.endpoint.clone()), &context,
        &host(exchange.clone()), async { Ok(()) }, std::future::pending()).await;
    assert_eq!(value.result.unwrap().summary, "One task and its actual result remain.");
    let request = peer.received.await.unwrap();
    assert!(request.get("tools").is_none());
    assert_eq!(request["input"][1], context[1]);
    assert!(request["input"].as_array().unwrap().last().unwrap()["content"].as_str().unwrap().contains("Charter change"));
    assert_eq!(request["model"], input.definition.required_inference().unwrap().model);
    assert_eq!(request["reasoning"]["effort"], input.definition.required_inference().unwrap().reasoning_effort);
    assert_eq!(request["max_output_tokens"], input.execution_limits().unwrap().native.max_output_tokens);
    assert_eq!(exchange.take_reply().unwrap().unwrap().declared, Some(declaration()));
}
