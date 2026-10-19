//! Controlled Responses peers only. These exercise the native primitive, not
//! MCP authority, durable worker orchestration or provider interoperability.
use super::*;
use borg_runtime::native::{
    NativeFunction, NativeLoopCall, NativeLoopObservation, NativeLoopProfile, NativeLoopReply,
    NativeLoopStep, NativeWebSearch, ValidationHost, ValidationRuntime, execute_loop_step,
};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

fn input() -> TaskInput {
    let mut input = task();
    input.limits.deadline_unix_ms = now_ms() + 30_000;
    input.limits.model_requests = 3;
    bind(input)
}
fn profile() -> NativeLoopProfile {
    NativeLoopProfile {
        functions: vec![NativeFunction {
            name: "collab_read".into(),
            description: "Read a scoped synthetic record".into(),
            parameters: json!({"type":"object","properties":{"record":{"type":"string"}},"required":["record"],"additionalProperties":false}),
        }],
        web_search: None,
    }
}
fn host() -> ValidationHost {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let path = std::env::temp_dir().join(format!(
        "borg-native-loop-{}-{}-{}",
        std::process::id(),
        now_ms(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&path).unwrap();
    let limits = metadata();
    ValidationHost::new(
        PathBuf::from(env!("CARGO_BIN_EXE_borg-runtime")),
        path,
        Arc::new(limits.execution.native),
        Arc::new(ValidationRuntime::new(Arc::from(limits.worker)).unwrap()),
    )
}
fn tool(id: &str) -> Value {
    json!({"id":format!("fc_{id}"),"type":"function_call","status":"completed","call_id":id,"name":"collab_read","arguments":"{\"record\":\"alpha\"}"})
}
fn call_response(id: &str) -> Value {
    with_usage(json!({"status":"completed","output":[
        {"type":"reasoning","summary":[],"encrypted_content":"opaque encrypted reasoning"},
        {"type":"message","role":"assistant","status":"completed","phase":"commentary","content":[{"type":"output_text","text":"Checking the record","annotations":[]}]},
        tool(id)
    ]}))
}
fn final_response() -> Value {
    with_usage(envelope("{\"result\":\"good\"}"))
}
fn call(observation: NativeLoopObservation) -> NativeLoopCall {
    assert_eq!(observation.provider_outcome, ProviderOutcome::Completed);
    assert_eq!(observation.usage, reported());
    match observation.result.unwrap() {
        NativeLoopStep::Call(call) => call,
        _ => panic!("expected tool"),
    }
}
async fn run(
    input: &TaskInput,
    profile: &NativeLoopProfile,
    prior: Option<(&NativeLoopCall, NativeLoopReply<'_>)>,
    envelope: Value,
) -> (NativeLoopObservation, Value) {
    let peer = Fixture::start(
        200,
        serde_json::to_vec(&envelope).unwrap(),
        Duration::ZERO,
        "",
    )
    .await;
    let result = require_send(execute_loop_step(
        input,
        &config(peer.endpoint.clone()),
        profile,
        prior,
        &host(),
        async { Ok(()) },
        std::future::pending(),
    ))
    .await;
    let request = peer.received.await.unwrap();
    assert_eq!(request["model"], "gpt-6-astra");
    assert_eq!(request["reasoning"], json!({"effort":"max"}));
    assert_eq!(
        request["max_output_tokens"],
        input.execution_limits().unwrap().native.max_output_tokens
    );
    (result, request)
}

// Traceability scope: Controlled helper transport: two configured calls across three requests preserve exact context and outputs. Actual worker repeated dispatch and durable receipts are covered by the collaboration integration cases, not this primitive fixture.
//= spec/impl/v2/implementation.md#native-tool-profiles-and-continuations
//= type=test
//# The native executor MUST support repeated configured tool calls within the task's finite execution bounds.
#[tokio::test]
async fn three_steps_preserve_exact_context_calls_results_and_schema() {
    let input = input();
    let profile = profile();
    let first_envelope = call_response("first");
    let (first, request) = run(&input, &profile, None, first_envelope.clone()).await;
    assert_eq!(request["store"], false);
    assert_eq!(request["stream"], false);
    assert_eq!(request["tool_choice"], "auto");
    assert_eq!(request["parallel_tool_calls"], false);
    assert_eq!(
        request["tools"][0]["parameters"],
        profile.functions[0].parameters
    );
    assert_eq!(
        request["text"]["format"]["schema"],
        input.output_schema.clone().unwrap()
    );
    let first = call(first);
    assert_eq!(first.name(), "collab_read");
    assert_eq!(first.arguments(), &json!({"record":"alpha"}));
    assert_eq!(first.request_index(), 1);
    let output_one = "{\"record\":\"actual first observation\",\"revision\":11}";
    let (second, request) = run(
        &input,
        &profile,
        Some((
            &first,
            NativeLoopReply {
                call_id: "first",
                output: output_one,
            },
        )),
        call_response("second"),
    )
    .await;
    assert_eq!(request["input"][1], first_envelope["output"][0]);
    assert_eq!(request["input"][2]["phase"], "commentary");
    assert_eq!(request["input"][3], first_envelope["output"][2]);
    assert_eq!(
        request["input"][4],
        json!({"type":"function_call_output","call_id":"first","output":output_one})
    );
    let second = call(second);
    assert_eq!(second.request_index(), 2);
    let (complete, request) = run(
        &input,
        &profile,
        Some((
            &second,
            NativeLoopReply {
                call_id: "second",
                output: "second actual result",
            },
        )),
        final_response(),
    )
    .await;
    assert_eq!(request["input"][4]["output"], output_one);
    assert_eq!(
        request["input"].as_array().unwrap().last().unwrap()["call_id"],
        "second"
    );
    assert_eq!(request["tools"].as_array().unwrap().len(), 1);
    assert_eq!(complete.provider_outcome, ProviderOutcome::Completed);
    match complete.result.unwrap() {
        NativeLoopStep::Complete {
            result,
            output_items,
            ..
        } => {
            assert_eq!(result.output, json!({"result":"good"}));
            assert_eq!(result.requests, 1);
            assert_eq!(output_items, final_response()["output"]);
        }
        _ => panic!("expected final"),
    }
}

#[tokio::test]
async fn ordinary_task_can_finish_first_and_no_tool_enables_coding() {
    let input = input();
    assert!(input.native_workflow.is_none());
    for profile in [profile(), NativeLoopProfile::default()] {
        let (value, request) = run(&input, &profile, None, final_response()).await;
        assert!(matches!(value.result, Ok(NativeLoopStep::Complete { .. })));
        assert!(!request.to_string().contains("delegate"));
    }
}

// Traceability scope: Controlled helper transport: changed task/profile/reply association and exhausted request allowance refuse before the next POST. This is the negative continuation boundary, not durable worker orchestration.
//= spec/impl/v2/implementation.md#native-tool-profiles-and-continuations
//= type=test
//# The native executor MUST support repeated configured tool calls within the task's finite execution bounds.
#[tokio::test]
async fn exact_reply_task_profile_and_remaining_request_budget_guard_next_post() {
    let mut input = input();
    input.limits.model_requests = 1;
    let input = bind(input);
    let original = profile();
    let (first, _) = run(&input, &original, None, call_response("first")).await;
    let first = call(first);
    for case in 0..4 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cfg = config(format!(
            "http://{}/v1/responses",
            listener.local_addr().unwrap()
        ));
        let mut changed_input = input.clone();
        let mut changed_profile = original.clone();
        if case == 1 {
            changed_input.instruction.push('!');
        }
        if case == 2 {
            changed_profile.functions[0].description.push('!');
        }
        let changed_input = bind(changed_input);
        let id = if case == 3 { "other" } else { "first" };
        let result = execute_loop_step(
            &changed_input,
            &cfg,
            &changed_profile,
            Some((
                &first,
                NativeLoopReply {
                    call_id: id,
                    output: "actual result",
                },
            )),
            &host(),
            async { panic!("must refuse before ownership callback") },
            std::future::pending(),
        )
        .await;
        assert_eq!(result.request_exposure, RequestExposure::NotStarted);
        assert_eq!(result.provider_outcome, ProviderOutcome::NotStarted);
        assert!(result.result.is_err());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn configured_schema_validation_rejects_invalid_arguments_and_final_result() {
    for args in [
        "{\"record\":7}",
        "{\"record\":\"a\",\"authority\":\"root\"}",
        "{\"record\":\"a\",\"record\":\"b\"}",
        "not json",
    ] {
        let mut body = call_response("first");
        body["output"][2]["arguments"] = json!(args);
        let (result, _) = run(&input(), &profile(), None, body).await;
        assert!(result.result.is_err());
        assert_eq!(result.provider_outcome, ProviderOutcome::Completed);
        assert_eq!(result.usage, reported());
    }
    let (result, _) = run(
        &input(),
        &profile(),
        None,
        with_usage(envelope("{\"result\":7}")),
    )
    .await;
    assert_eq!(
        result.result.err().unwrap(),
        "native_output_schema_mismatch"
    );
    assert_eq!(result.provider_outcome, ProviderOutcome::Completed);
}

#[tokio::test]
async fn unconfigured_unknown_multiple_or_partial_calls_never_escape() {
    for case in 0..6 {
        let mut body = call_response("first");
        match case {
            0 => body["output"][2]["name"] = json!("shell"),
            1 => body["output"][2]["status"] = json!("in_progress"),
            2 => body["output"].as_array_mut().unwrap().push(tool("second")),
            3 => body["output"][2]["endpoint"] = json!("http://unselected"),
            4 => body["output"]
                .as_array_mut()
                .unwrap()
                .push(json!({"type":"computer_call","status":"completed"})),
            _ => body["output"][2]["call_id"] = json!("bad\nidentity"),
        }
        let (result, _) = run(&input(), &profile(), None, body).await;
        assert!(result.result.is_err());
        assert_eq!(result.provider_outcome, ProviderOutcome::Unknown);
        assert_eq!(result.usage, reported());
    }
}

fn search_response() -> Value {
    let mut body = final_response();
    body["output"].as_array_mut().unwrap().insert(1, json!({
        "type":"web_search_call", "id":"search_first", "status":"completed",
        "action":{"type":"search","queries":["synthetic query"],"domains":["example.org"],"sources":[{"type":"url","url":"https://example.org/source"}]}
    }));
    body["output"][2]["phase"] = json!("final_answer");
    body["output"][2]["content"][0]["annotations"] = json!([{"type":"url_citation","url":"https://example.org/source","title":"Synthetic source","start_index":0,"end_index":4}]);
    body
}

// Traceability scope: Controlled helper transport: an absent hosted-search selection is not advertised and its returned call is refused; enabled search remains bounded. Accepted-definition binding is exercised separately by actual worker integration.
//= spec/impl/v2/implementation.md#native-tool-profiles-and-continuations
//= type=test
//# The native executor MUST refuse a provider tool that is absent from the task's accepted configuration.
#[tokio::test]
async fn hosted_search_is_opt_in_bounded_and_retains_annotations() {
    let (disabled, request) = run(&input(), &profile(), None, search_response()).await;
    assert!(disabled.result.is_err());
    assert_eq!(disabled.provider_outcome, ProviderOutcome::Unknown);
    assert!(request.get("max_tool_calls").is_none());
    assert!(
        request["tools"]
            .as_array()
            .unwrap()
            .iter()
            .all(|v| v["type"] != "web_search")
    );
    let mut enabled = profile();
    enabled.web_search = Some(NativeWebSearch { max_tool_calls: 1 });
    let (result, request) = run(&input(), &enabled, None, search_response()).await;
    assert_eq!(request["max_tool_calls"], 1);
    assert_eq!(
        request["include"],
        json!([
            "reasoning.encrypted_content",
            "web_search_call.action.sources"
        ])
    );
    match result.result.unwrap() {
        NativeLoopStep::Complete {
            result,
            output_items,
            ..
        } => {
            assert_eq!(result.output, json!({"result":"good"}));
            assert_eq!(output_items, search_response()["output"]);
        }
        _ => panic!("expected final"),
    }
    for case in 0..5 {
        let mut body = search_response();
        match case {
            0 => body["output"][1]["action"]["type"] = json!("download_and_execute"),
            1 => body["output"][1]["status"] = json!("searching"),
            2 => {
                let mut extra = body["output"][1].clone();
                extra["id"] = json!("search_second");
                body["output"].as_array_mut().unwrap().push(extra);
            }
            3 => body["output"][1]["action"]["unexpected"] = json!(true),
            _ => body["output"][2]["content"][0]["annotations"][0]["type"] = json!("file_citation"),
        }
        let (result, _) = run(&input(), &enabled, None, body).await;
        assert!(result.result.is_err());
        assert_eq!(result.provider_outcome, ProviderOutcome::Unknown);
    }
}

#[tokio::test]
async fn malformed_profile_context_and_expired_input_refuse_before_post() {
    for case in 0..7 {
        let mut input = input();
        let mut profile = profile();
        let schema_bytes = input.execution_limits().unwrap().native.schema_bytes;
        match case {
            0 => profile.functions.push(profile.functions[0].clone()),
            1 => {
                profile.functions[0].parameters =
                    json!({"type":"object","properties":{"x":{"$ref":"https://unselected/schema"}}})
            }
            2 => input.limits.model_requests = 0,
            3 => input.limits.deadline_unix_ms = now_ms().saturating_sub(1),
            4 => profile.web_search = Some(NativeWebSearch { max_tool_calls: 0 }),
            5 => {
                profile.functions[0].parameters["description"] = json!("x".repeat(schema_bytes + 1))
            }
            _ => {
                input.output_schema.as_mut().unwrap()["description"] =
                    json!("x".repeat(schema_bytes + 1))
            }
        }
        let input = bind(input);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cfg = config(format!(
            "http://{}/v1/responses",
            listener.local_addr().unwrap()
        ));
        let result = execute_loop_step(
            &input,
            &cfg,
            &profile,
            None,
            &host(),
            async { panic!("invalid input must not reach before-send") },
            std::future::pending(),
        )
        .await;
        assert_eq!(result.request_exposure, RequestExposure::NotStarted);
        assert!(result.result.is_err());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }
    let input = input();
    let profile = profile();
    let (first, _) = run(&input, &profile, None, call_response("first")).await;
    let first = call(first);
    let output = "x".repeat(input.execution_limits().unwrap().native.context_bytes);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cfg = config(format!(
        "http://{}/v1/responses",
        listener.local_addr().unwrap()
    ));
    let result = execute_loop_step(
        &input,
        &cfg,
        &profile,
        Some((
            &first,
            NativeLoopReply {
                call_id: "first",
                output: &output,
            },
        )),
        &host(),
        async { panic!("bounded context must refuse") },
        std::future::pending(),
    )
    .await;
    assert_eq!(result.request_exposure, RequestExposure::NotStarted);
    assert!(result.result.is_err());
}

#[tokio::test]
async fn currentness_refusal_and_cancellation_never_bypass_helper_cleanup() {
    let input = input();
    let profile = profile();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cfg = config(format!(
        "http://{}/v1/responses",
        listener.local_addr().unwrap()
    ));
    let result = execute_loop_step(
        &input,
        &cfg,
        &profile,
        None,
        &host(),
        async { Err("current-owner-refused".into()) },
        std::future::pending(),
    )
    .await;
    assert_eq!(result.result.err().unwrap(), "current-owner-refused");
    assert_eq!(result.request_exposure, RequestExposure::NotStarted);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    let h = host();
    let ready = borg_runtime::native::qualification_hold_provider_after_usage(
        std::path::Path::new(env!("CARGO_BIN_EXE_borg-runtime")),
    );
    let peer = Fixture::start(
        200,
        serde_json::to_vec(&call_response("first")).unwrap(),
        Duration::ZERO,
        "",
    )
    .await;
    let result = execute_loop_step(
        &input,
        &config(peer.endpoint.clone()),
        &profile,
        None,
        &h,
        async { Ok(()) },
        async {
            ready.await.unwrap();
        },
    )
    .await;
    assert_eq!(result.result.err().unwrap(), "native_cancelled");
    assert_eq!(result.usage, reported());
    assert_eq!(result.provider_outcome, ProviderOutcome::Completed);
    assert_eq!(borg_runtime::native::validation_cleanup_pending(), 0);
}

#[tokio::test]
async fn response_shape_metadata_limits_survive_helper_validation() {
    let input = input();
    let policy = &input.execution_limits().unwrap().native;
    for case in 0..3 {
        let mut body = call_response("first");
        let expected = match case {
            0 => {
                body["output"][2]["id"] = json!("x".repeat(policy.call_id_bytes + 1));
                "native_tool_call_identity_invalid_native_call_id_bytes"
            }
            1 => {
                body["output"][2]["call_id"] = json!("x".repeat(policy.call_id_bytes + 1));
                "native_tool_call_identity_invalid_native_call_id_bytes"
            }
            _ => {
                body["output"].as_array_mut().unwrap().push(tool("second"));
                "native_tool_call_count_native_function_calls_per_response"
            }
        };
        let (result, _) = run(&input, &profile(), None, body).await;
        assert_eq!(result.result.err().as_deref(), Some(expected));
        assert_eq!(result.provider_outcome, ProviderOutcome::Unknown);
        assert_eq!(result.usage, reported());
    }
}

#[tokio::test]
async fn incomplete_envelope_keeps_disposition_before_shape_limit() {
    let mut body = call_response("first");
    body["status"] = json!("incomplete");
    body["output"].as_array_mut().unwrap().push(tool("second"));
    let (result, _) = run(&input(), &profile(), None, body).await;
    assert_eq!(
        result.result.err().as_deref(),
        Some("native_provider_response_not_completed")
    );
    assert_eq!(result.provider_outcome, ProviderOutcome::Unknown);
    assert_eq!(result.usage, reported());
}

#[tokio::test]
async fn incomplete_wire_and_complete_refusal_keep_different_outcomes() {
    let mut refusal = final_response();
    refusal["output"][1]["content"][0] = json!({"type":"refusal","refusal":"declined"});
    let (result, _) = run(&input(), &profile(), None, refusal).await;
    assert_eq!(result.result.err().unwrap(), "native_provider_refused");
    assert_eq!(result.provider_outcome, ProviderOutcome::Completed);
    assert_eq!(result.usage, reported());
    let peer = Fixture::start(
        200,
        b"{\"status\":\"completed\",\"output\":[".to_vec(),
        Duration::ZERO,
        "",
    )
    .await;
    let result = execute_loop_step(
        &input(),
        &config(peer.endpoint.clone()),
        &profile(),
        None,
        &host(),
        async { Ok(()) },
        std::future::pending(),
    )
    .await;
    assert!(result.result.is_err());
    assert_eq!(result.provider_outcome, ProviderOutcome::Unknown);
    assert_eq!(result.usage, UsageObservation::Unknown);
}

#[tokio::test]
async fn reused_call_identity_refuses_and_commentary_is_preserved_before_final() {
    let input = input();
    let profile = profile();
    let (first, _) = run(&input, &profile, None, call_response("first")).await;
    let first = call(first);
    let (repeat, _) = run(
        &input,
        &profile,
        Some((
            &first,
            NativeLoopReply {
                call_id: "first",
                output: "recorded",
            },
        )),
        call_response("first"),
    )
    .await;
    assert_eq!(
        repeat.result.err().unwrap(),
        "native_tool_call_identity_invalid"
    );
    assert_eq!(repeat.provider_outcome, ProviderOutcome::Completed);
    let mut final_value = final_response();
    let commentary = call_response("ignored")["output"][1].clone();
    final_value["output"]
        .as_array_mut()
        .unwrap()
        .insert(1, commentary);
    final_value["output"][2]["phase"] = json!("final_answer");
    let (complete, _) = run(&input, &profile, None, final_value.clone()).await;
    match complete.result.unwrap() {
        NativeLoopStep::Complete { output_items, .. } => {
            assert_eq!(output_items, final_value["output"])
        }
        _ => panic!("expected final"),
    }
}

#[tokio::test]
async fn hosted_search_citation_offsets_are_bounded_in_original_unicode_text() {
    let mut enabled = profile();
    enabled.web_search = Some(NativeWebSearch { max_tool_calls: 1 });
    for text in ["{\"result\":\"ascii\"}", "{\"result\":\"é🦀\"}"] {
        let count = text.chars().count();
        let mut body = search_response();
        body["output"][2]["content"][0]["text"] = json!(text);
        body["output"][2]["content"][0]["annotations"][0]["end_index"] = json!(count);
        let (valid, _) = run(&input(), &enabled, None, body.clone()).await;
        match valid.result.unwrap() {
            NativeLoopStep::Complete { output_items, .. } => {
                assert_eq!(output_items[2]["content"][0]["text"], text)
            }
            _ => panic!("expected final"),
        }
        body["output"][2]["content"][0]["annotations"][0]["end_index"] = json!(count + 1);
        let (invalid, _) = run(&input(), &enabled, None, body).await;
        assert!(invalid.result.is_err());
        assert_eq!(invalid.provider_outcome, ProviderOutcome::Unknown);
    }
}

#[tokio::test]
async fn loop_astra_selection_refuses_previous_model_before_network() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = config(format!(
        "http://{}/v1/responses",
        listener.local_addr().unwrap()
    ));
    cfg.model = "gpt-6.1-sol".into();
    let out = execute_loop_step(
        &input(),
        &cfg,
        &profile(),
        None,
        &host(),
        async { Ok(()) },
        std::future::pending(),
    )
    .await;
    assert_eq!(
        out.result.err().as_deref(),
        Some("native_config_template_mismatch")
    );
    assert_eq!(out.request_exposure, RequestExposure::NotStarted);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn no_memory_notice_preserves_original_task_and_exact_tool_result() {
    use borg_runtime::native::execute_loop_step_with_notice;
    let input = input();
    let profile = profile();
    let before = input.clone();
    let (first, _) = run(&input, &profile, None, call_response("before-memory")).await;
    let first = call(first);
    let result_text = "{\"key\":\"note\",\"revision\":1,\"value\":\"old observation\"}";
    let peer = Fixture::start(
        200,
        serde_json::to_vec(&call_response("after-memory")).unwrap(),
        Duration::ZERO,
        "",
    )
    .await;
    let value = execute_loop_step_with_notice(
        &input,
        &config(peer.endpoint.clone()),
        &profile,
        Some((
            &first,
            NativeLoopReply {
                call_id: "before-memory",
                output: result_text,
            },
        )),
        None,
        &host(),
        async { Ok(()) },
        std::future::pending(),
    )
    .await;
    let request = peer.received.await.unwrap();
    assert_eq!(input, before);
    assert_eq!(request["input"][0]["content"], input.instruction);
    let items = request["input"].as_array().unwrap();
    assert_eq!(
        items[items.len() - 1],
        json!({"type":"function_call_output","call_id":"before-memory","output":result_text})
    );
    assert!(items.iter().all(|item| item["role"] != "developer"));
    let second = call(value);
    // Same original TaskInput/profile still passes the next step association.
    let (done, next) = run(
        &input,
        &profile,
        Some((
            &second,
            NativeLoopReply {
                call_id: "after-memory",
                output: "canonical reread",
            },
        )),
        final_response(),
    )
    .await;
    assert!(done.result.is_ok());
    assert_eq!(
        next["input"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|x| x["role"] == "developer")
            .count(),
        0
    );
}

#[tokio::test]
async fn retired_memory_notice_refuses_before_reservation_or_http() {
    use borg_runtime::native::execute_loop_step_with_notice;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cfg = config(format!(
        "http://{}/v1/responses",
        listener.local_addr().unwrap()
    ));
    let input = input();
    for notice in ["old pending notice".to_owned(), "x".repeat(input.execution_limits().unwrap().native.memory_notice_bytes + 1)] {
    let out = execute_loop_step_with_notice(
        &input,
        &cfg,
        &profile(),
        None,
        Some(&notice),
        &host(),
        async { panic!("bounded context must refuse before reservation") },
        std::future::pending(),
    )
    .await;
    assert_eq!(
        out.result.err().as_deref(),
        Some("native_memory_notifications_disabled")
    );
    assert_eq!(out.request_exposure, RequestExposure::NotStarted);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    }
}

#[tokio::test]
async fn closed_shape_diagnostics_survive_helper_without_changing_usage_or_disposition() {
    for case in 0..4 {
        let mut body = final_response();
        let expected = match case {
            0 => {
                body["output"][0]["DO_NOT_RETAIN"] = json!("DO_NOT_RETAIN");
                "native_response_shape_reasoning_fields"
            }
            1 => {
                body["output"][1]["content"] = json!([]);
                "native_response_shape_message_content_count"
            }
            2 => {
                let duplicate = body["output"][1].clone();
                body["output"].as_array_mut().unwrap().push(duplicate);
                "native_response_shape_final_message_count"
            }
            _ => {
                body["output"][1]["content"][0]["annotations"] = Value::Null;
                "native_response_shape_annotations"
            }
        };
        let (result, _) = run(&input(), &profile(), None, body).await;
        assert_eq!(result.result.err().as_deref(), Some(expected));
        assert_eq!(
            result.request_exposure,
            RequestExposure::HttpResponseReceived
        );
        assert_eq!(result.provider_outcome, ProviderOutcome::Unknown);
        assert_eq!(result.usage, reported());
    }
}

#[tokio::test]
async fn reasoning_content_survives_helper_and_exact_continuation_without_selecting_it_as_result() {
    for content in [
        None,
        Some(json!([])),
        Some(json!([{"type":"reasoning_text","text":"SYNTHETIC_PRIVATE_REASONING"}])),
        Some(json!([
            {"type":"reasoning_text","text":"SYNTHETIC_PRIVATE_REASONING one"},
            {"type":"reasoning_text","text":"SYNTHETIC_PRIVATE_REASONING two"}
        ])),
    ] {
        let input = input();
        let profile = profile();
        let mut first_envelope = call_response("typed-reasoning");
        if let Some(content) = content {
            first_envelope["output"][0]["content"] = content;
        }
        let original_items = first_envelope["output"].clone();
        let (first, _) = run(&input, &profile, None, first_envelope).await;
        assert_eq!(
            first.request_exposure,
            RequestExposure::HttpResponseReceived
        );
        let first = call(first);
        let retained = first.retained_items().as_array().unwrap();
        assert_eq!(
            &retained[1..],
            original_items.as_array().unwrap().as_slice()
        );

        let mut final_envelope = final_response();
        final_envelope["output"][0]["content"] = json!([
            {"type":"reasoning_text","text":"SYNTHETIC_FINAL_PRIVATE_REASONING"}
        ]);
        let final_items = final_envelope["output"].clone();
        let original_receipt = "{\"record\":\"exact synthetic observation\",\"revision\":11}";
        let (final_step, request) = run(
            &input,
            &profile,
            Some((
                &first,
                NativeLoopReply {
                    call_id: first.call_id(),
                    output: original_receipt,
                },
            )),
            final_envelope,
        )
        .await;
        let next_items = request["input"].as_array().unwrap();
        assert_eq!(
            &next_items[1..1 + original_items.as_array().unwrap().len()],
            original_items.as_array().unwrap().as_slice()
        );
        assert_eq!(
            next_items.last().unwrap(),
            &json!({"type":"function_call_output","call_id":first.call_id(),"output":original_receipt})
        );
        assert_eq!(
            final_step.request_exposure,
            RequestExposure::HttpResponseReceived
        );
        assert_eq!(final_step.provider_outcome, ProviderOutcome::Completed);
        assert_eq!(final_step.usage, reported());
        match final_step.result.unwrap() {
            NativeLoopStep::Complete {
                result,
                output_items,
                ..
            } => {
                assert_eq!(output_items, final_items);
                assert_eq!(result.output, json!({"result":"good"}));
                assert!(!result.summary.contains("SYNTHETIC_"));
                assert_eq!(result.requests, 1);
            }
            _ => panic!("expected final assistant result"),
        }
    }
}

#[tokio::test]
async fn malformed_reasoning_content_helper_preserves_usage_and_hides_text() {
    for (content, expected) in [
        (Value::Null, "native_response_shape_reasoning_content"),
        (
            json!([{"type":"reasoning_text","text":"DO_NOT_RETAIN","DO_NOT_RETAIN":true}]),
            "native_response_shape_reasoning_content_fields",
        ),
        (
            json!([{"type":"DO_NOT_RETAIN","text":"DO_NOT_RETAIN"}]),
            "native_response_shape_reasoning_content_type",
        ),
        (
            json!([{"type":"reasoning_text","text":{"DO_NOT_RETAIN":true}}]),
            "native_response_shape_reasoning_content_text",
        ),
    ] {
        let mut body = final_response();
        body["output"][0]["content"] = content;
        let (observed, _) = run(&input(), &profile(), None, body).await;
        assert_eq!(observed.result.err().as_deref(), Some(expected));
        assert_eq!(
            observed.request_exposure,
            RequestExposure::HttpResponseReceived
        );
        assert_eq!(observed.provider_outcome, ProviderOutcome::Unknown);
        assert_eq!(observed.usage, reported());
    }
}

// Controlled provider fixture: actual final context + recorded host feedback,
// not controller policy qualification or an unknown provider replay.
#[tokio::test]
async fn completion_continuation_appends_actual_final_items_and_feedback_without_fake_call() {
    let input=input(); let profile=profile(); let envelope=final_response();
    let (first,_)=run(&input,&profile,None,envelope.clone()).await;
    let NativeLoopStep::Complete {continuation,..}=first.result.unwrap() else {panic!("final expected")};
    assert_eq!(continuation.request_index(),1);
    let peer=Fixture::start(200,serde_json::to_vec(&final_response()).unwrap(),Duration::ZERO,"").await;
    let feedback="Recorded completion refusal: remaining child task child-7";
    let response=borg_runtime::native::execute_loop_step_after_completion(
        &input,&config(peer.endpoint.clone()),&profile,&continuation,feedback,None,None,&host(),
        async {Ok(())},std::future::pending(),
    ).await;
    assert!(matches!(response.result,Ok(NativeLoopStep::Complete {..})));
    let request=peer.received.await.unwrap();
    let items=request["input"].as_array().unwrap();
    assert_eq!(items[0],json!({"role":"user","content":input.instruction}));
    let original=envelope["output"].as_array().unwrap();
    assert_eq!(&items[1..1+original.len()],original.as_slice());
    assert_eq!(items.last().unwrap(),&json!({"role":"developer","content":feedback}));
    assert!(items.iter().all(|item|item["type"]!="function_call" && item["type"]!="function_call_output"));
}

#[tokio::test]
async fn completion_continuation_binds_task_and_refuses_exhausted_or_changed_input_before_post() {
    let mut input=input(); input.limits.model_requests=1; let input=bind(input);
    let profile=profile();
    let (first,_)=run(&input,&profile,None,final_response()).await;
    let NativeLoopStep::Complete {continuation,..}=first.result.unwrap() else {panic!("final expected")};
    for changed in [false,true] {
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cfg=config(format!("http://{}/v1/responses",listener.local_addr().unwrap()));
        let mut candidate=input.clone(); if changed {candidate.instruction.push('!');}
        let candidate=bind(candidate);
        let response=borg_runtime::native::execute_loop_step_after_completion(
            &candidate,&cfg,&profile,&continuation,"recorded reason",None,None,&host(),
            async {Ok(())},std::future::pending(),
        ).await;
        assert_eq!(response.request_exposure,RequestExposure::NotStarted);
        assert!(response.result.is_err());
        assert!(tokio::time::timeout(Duration::from_millis(20),listener.accept()).await.is_err());
    }
}

#[tokio::test]
async fn final_compaction_keeps_original_response_index_and_accounts_for_separate_request() {
    let input=input(); let profile=profile();
    let (first,_)=run(&input,&profile,None,final_response()).await;
    let NativeLoopStep::Complete {continuation,..}=first.result.unwrap() else {panic!("final expected")};
    assert!(continuation.after_compaction(3,Some("summary")).is_err());
    let compacted=continuation.after_compaction(2,Some("canonical current-source summary")).unwrap();
    assert_eq!(compacted.request_index(),1);
    assert_eq!(compacted.next_request_index().unwrap(),3);
    let context=compacted.compaction_input().unwrap();
    assert_eq!(context.as_array().unwrap().len(),2);
    assert!(context[1]["content"].as_str().unwrap().contains("canonical current-source summary"));
    assert!(context.as_array().unwrap().iter().all(|item|item["type"]!="function_call_output"));
}

#[tokio::test]
async fn successor_initial_request_carries_original_instruction_and_recorded_reason_once() {
    let input=input();let profile=profile();
    let peer=Fixture::start(200,serde_json::to_vec(&final_response()).unwrap(),Duration::ZERO,"").await;
    let response=borg_runtime::native::execute_loop_step_with_completion_input(
        &input,&config(peer.endpoint.clone()),&profile,"Recorded stopped completion decision: remaining-child",
        None,None,&host(),async {Ok(())},std::future::pending(),
    ).await;
    let NativeLoopStep::Complete {continuation,..}=response.result.unwrap() else {panic!("final expected")};
    assert_eq!(continuation.request_index(),1);
    let request=peer.received.await.unwrap();let items=request["input"].as_array().unwrap();
    assert_eq!(items[0],json!({"role":"user","content":input.instruction}));
    assert_eq!(items[1],json!({"role":"developer","content":"Recorded stopped completion decision: remaining-child"}));
    assert_eq!(items.len(),2);
}


// Source-only primitive fixtures: the worker/controller own original rejection
// receipts and retry admission; these verify exact real request bytes at the API.
#[tokio::test]
async fn completion_retry_advances_physical_index_with_identical_actual_input() {
    for initial in [false,true] {
        let input=input();let profile=profile();let base=host();
        let previous=if initial {None} else {
            let (first,_)=run(&input,&profile,None,final_response()).await;
            let NativeLoopStep::Complete {continuation,..}=first.result.unwrap() else {panic!("final expected")};
            Some(continuation)
        };
        let first=if initial {1}else{2};
        let reason="Original recorded completion refusal: child-7 remains";
        let rejected=Fixture::start(429,serde_json::to_vec(&json!({"error":{"code":"rate_limit_exceeded","type":"rate_limit_error"}})).unwrap(),Duration::ZERO,"").await;
        let request_host=base.for_request(None);
        let response=if let Some(prior)=previous.as_ref() {
            borg_runtime::native::execute_loop_step_after_completion_at(&input,&config(rejected.endpoint.clone()),&profile,
                prior,reason,None,None,Some(first),&request_host,async {Ok(())},std::future::pending()).await
        } else {
            borg_runtime::native::execute_loop_step_with_completion_input_at(&input,&config(rejected.endpoint.clone()),&profile,
                reason,None,None,Some(first),&request_host,async {Ok(())},std::future::pending()).await
        };
        assert!(matches!(response.provider_outcome,ProviderOutcome::Rejected(_)));
        assert!(response.result.is_err());
        let original=request_host.request_binding().unwrap().unwrap();
        let original_body=rejected.received.await.unwrap();
        let peer=Fixture::start(200,serde_json::to_vec(&final_response()).unwrap(),Duration::ZERO,"").await;
        let retry_host=base.for_request(Some(original.clone()));
        let response=if let Some(prior)=previous.as_ref() {
            borg_runtime::native::execute_loop_step_after_completion_at(&input,&config(peer.endpoint.clone()),&profile,
                prior,reason,None,None,Some(first+1),&retry_host,async {Ok(())},std::future::pending()).await
        } else {
            borg_runtime::native::execute_loop_step_with_completion_input_at(&input,&config(peer.endpoint.clone()),&profile,
                reason,None,None,Some(first+1),&retry_host,async {Ok(())},std::future::pending()).await
        };
        assert_eq!(response.provider_outcome,ProviderOutcome::Completed);
        let NativeLoopStep::Complete {continuation,..}=response.result.unwrap() else {panic!("final expected")};
        assert_eq!(continuation.request_index(),first+1);
        assert_eq!(retry_host.request_binding().unwrap(),Some(original));
        assert_eq!(peer.received.await.unwrap(),original_body);
    }
}

#[tokio::test]
async fn completion_retry_changed_reason_is_rejected_before_post() {
    let input=input();let profile=profile();let base=host();
    let rejected=Fixture::start(429,serde_json::to_vec(&json!({"error":{"code":"rate_limit_exceeded"}})).unwrap(),Duration::ZERO,"").await;
    let first=base.for_request(None);
    let response=borg_runtime::native::execute_loop_step_with_completion_input_at(&input,&config(rejected.endpoint.clone()),&profile,
        "original reason",None,None,Some(1),&first,async {Ok(())},std::future::pending()).await;
    assert!(matches!(response.provider_outcome,ProviderOutcome::Rejected(_)));
    let original=first.request_binding().unwrap().unwrap();
    let _=rejected.received.await.unwrap();
    let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cfg=config(format!("http://{}/v1/responses",listener.local_addr().unwrap()));
    let retry=base.for_request(Some(original));
    let response=borg_runtime::native::execute_loop_step_with_completion_input_at(&input,&cfg,&profile,
        "changed reason",None,None,Some(2),&retry,async {Ok(())},std::future::pending()).await;
    assert_eq!(response.provider_outcome,ProviderOutcome::NotStarted);
    assert_eq!(response.request_exposure,RequestExposure::NotStarted);
    assert_eq!(response.result.err().as_deref(),Some("native_retry_original_input_changed_no_post"));
    assert!(retry.request_binding().unwrap().is_none());
    assert!(tokio::time::timeout(Duration::from_millis(20),listener.accept()).await.is_err());
}
