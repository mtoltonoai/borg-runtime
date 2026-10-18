//! Pure validation and retained-context tests, mounted only under loop_step.
//! These fixtures do not claim helper EOF/reaping, a canonical receipt, or
//! provider interoperability; those boundaries belong to the actual worker tests.
use super::*;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

fn fixture(enabled: bool) -> (TaskInput, ValidationInput, NativeLimits) {
    let mut limits = crate::limits::RuntimeLimits::seed().unwrap();
    limits.execution.native.context_bytes = 65_536;
    // Explicit fixture policy, not new production defaults or a retry count.
    limits.native_retry = enabled.then_some(crate::limits::NativeRetryLimits {
        initial_backoff_ms: 5,
        backoff_multiplier: 2,
        max_backoff_ms: 20,
        jitter_ms: 1,
        circuit: crate::limits::NativeCircuitLimits {
            failure_threshold: 2,
            open_ms: 50,
            max_entries: 8,
        },
        auth_cooldown_ms: 80,
        auth_max_entries: 8,
        coordination_record_bytes: 65_536,
    });
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let mut task: TaskInput = serde_json::from_value(json!({
        "instruction": "Return the synthetic integer answer without changing the schema.",
        "definition": {
            "id": "output-cap-fixture",
            "revision": "accepted-1",
            "executor": "native",
            "inference": crate::protocol::InferenceSettings::selected(),
            "execution_profile": {"kind": "collaboration_v1"}
        },
        "repository": null,
        "acceptance_commands": [],
        "dependencies": [],
        "output_schema": {
            "type": "object", "properties": {"answer": {"type": "integer"}},
            "required": ["answer"], "additionalProperties": false
        },
        "limits": limits.task_defaults.at(now).unwrap()
    })).unwrap();
    task.limits.output_bytes = 1024;
    task.limits.model_requests = 4;
    task.resolved_limits = Some(limits.resolve_task(&task).unwrap());
    let policy = task.execution_limits().unwrap().native.clone();
    let input = ValidationInput {
        schema: task.output_schema.clone().unwrap(),
        profile: NativeLoopProfile {
            functions: vec![NativeFunction {
                name: "read_record".into(),
                description: "Read a synthetic record".into(),
                parameters: json!({
                    "type": "object", "properties": {"key": {"type": "integer"}},
                    "required": ["key"], "additionalProperties": false
                }),
            }],
            web_search: None,
        },
        output_bytes: task.limits.output_bytes,
        repair_enabled: task.resolved_limits.as_ref().unwrap()
            .global_metadata.native_retry.is_some(),
        output_prefix: None,
        joined_charter: false,
    };
    let input = preflight(&serde_json::to_value(input).unwrap(), &policy).unwrap();
    assert_eq!(input.repair_enabled, enabled);
    (task, input, policy)
}

fn capped_reply(items: Value) -> Value {
    json!({
        "status": "incomplete", "error": null,
        "incomplete_details": {"reason": "max_output_tokens"},
        "output": items
    })
}

fn message(text: &str, status: &str) -> Value {
    json!({
        "type": "message", "id": "message-one", "role": "assistant",
        "status": status, "phase": "final_answer",
        "content": [{"type": "output_text", "text": text, "annotations": []}]
    })
}

fn completed_reply(text: &str) -> Value {
    json!({"status": "completed", "error": null, "output": [message(text, "completed")]})
}

fn reasoning() -> Value {
    json!({
        "type": "reasoning", "id": "reasoning-one", "status": "incomplete",
        "summary": [], "encrypted_content": "SYNTHETIC_OPAQUE_CONTEXT",
        "content": [{"type": "reasoning_text", "text": "synthetic retained reasoning"}]
    })
}

fn partial_call(arguments: &str) -> Value {
    json!({
        "type": "function_call", "id": "function-one", "call_id": "original-call",
        "status": "incomplete", "name": "read_record", "arguments": arguments
    })
}

fn validate(
    envelope: &Value, input: &ValidationInput, policy: &NativeLimits,
) -> Result<(Vec<u8>, Option<super::super::FormatIssue>), String> {
    validate_response_typed(envelope, input, policy.context_bytes, deadline(), policy)
}

#[test]
fn exact_cap_terminal_accepts_empty_reasoning_and_partial_text_without_a_final_result() {
    let (_, input, policy) = fixture(true);
    for items in [
        json!([]),
        json!([reasoning()]),
        json!([{"type":"message","role":"assistant","status":"incomplete","content":[]}]),
        json!([message("", "incomplete")]),
        json!([reasoning(), message("{\"answer\":", "incomplete")]),
    ] {
        let envelope = capped_reply(items.clone());
        assert!(capped(&envelope, &input, deadline(), &policy));
        assert!(!completed(&envelope, &input.profile, deadline(), &policy));
        let (encoded, issue) = validate(&envelope, &input, &policy).unwrap();
        assert_eq!(serde_json::from_slice::<Value>(&encoded).unwrap(), items);
        assert!(issue.is_none(), "capped partial text is not a completed format failure");
    }
    // A normal complete answer keeps its original classification.
    let final_reply = completed_reply("{\"answer\":7}");
    assert!(!capped(&final_reply, &input, deadline(), &policy));
    assert!(completed(&final_reply, &input.profile, deadline(), &policy));
    assert!(validate(&final_reply, &input, &policy).unwrap().1.is_none());
}

#[test]
fn cap_requires_accepted_retry_policy_and_the_exact_terminal_reason() {
    let (_, disabled, policy) = fixture(false);
    let original = capped_reply(json!([]));
    assert!(!capped(&original, &disabled, deadline(), &policy));
    assert!(validate(&original, &disabled, &policy).is_err());
    let (_, input, policy) = fixture(true);
    let mut rejected = Vec::new();
    for status in ["in_progress", "queued", "failed", "cancelled"] {
        let mut value = original.clone(); value["status"] = json!(status); rejected.push(value);
    }
    for details in [
        Value::Null, json!({}), json!({"reason":"content_filter"}),
        json!({"reason":"other"}), json!({"reason":7}),
        json!({"reason":"max_output_tokens","extra":true}),
    ] {
        let mut value = original.clone(); value["incomplete_details"] = details; rejected.push(value);
    }
    let mut missing = original.clone(); missing.as_object_mut().unwrap().remove("status"); rejected.push(missing);
    let mut missing = original.clone(); missing.as_object_mut().unwrap().remove("incomplete_details"); rejected.push(missing);
    let mut missing = original.clone(); missing.as_object_mut().unwrap().remove("output"); rejected.push(missing);
    let mut wrong_output = original.clone(); wrong_output["output"] = Value::Null; rejected.push(wrong_output);
    let mut error = original.clone(); error["error"] = json!({"code":"synthetic_error"}); rejected.push(error);
    rejected.push(capped_reply(json!([{
        "type":"message","role":"assistant","status":"incomplete",
        "content":[{"type":"refusal","refusal":"synthetic refusal"}]
    }])));
    rejected.push(capped_reply(json!([message("{", "in_progress")])));
    for envelope in rejected {
        assert!(!capped(&envelope, &input, deadline(), &policy));
        assert!(validate(&envelope, &input, &policy).is_err());
    }
}

#[test]
fn retained_prefix_and_new_suffix_are_joined_exactly_before_schema_validation() {
    let (_, mut input, policy) = fixture(true);
    let first = capped_reply(json!([message("{", "incomplete")]));
    validate(&first, &input, &policy).unwrap();
    let first_text = cap_shape(&first, &input, deadline(), &policy).unwrap().text.unwrap();
    input.output_prefix = Some(joined_text(None, first_text, input.output_bytes).unwrap());
    let second = capped_reply(json!([message("\"answer\":", "incomplete")]));
    validate(&second, &input, &policy).unwrap();
    let second_text = cap_shape(&second, &input, deadline(), &policy).unwrap().text.unwrap();
    input.output_prefix = Some(joined_text(input.output_prefix.as_deref(), second_text, input.output_bytes).unwrap());
    assert_eq!(input.output_prefix.as_deref(), Some("{\"answer\":"));
    let final_reply = completed_reply("7}");
    let input = preflight(&serde_json::to_value(input).unwrap(), &policy).unwrap();
    let (encoded, issue) = validate(&final_reply, &input, &policy).unwrap();
    assert!(issue.is_none());
    assert_eq!(serde_json::from_slice::<Value>(&encoded).unwrap(), final_reply["output"],
        "the retained provider items remain the original suffix, not rewritten joined output");
    let joined = joined_text(input.output_prefix.as_deref(), "7}", input.output_bytes).unwrap();
    assert_eq!(joined, "{\"answer\":7}");
    assert_eq!(decode::parse_final(joined.as_bytes(), input.output_bytes, deadline(), &policy, "task_output_bytes")
        .unwrap().unwrap(), json!({"answer":7}));
    let (_, without_prefix, _) = fixture(true);
    assert!(matches!(validate(&final_reply, &without_prefix, &policy).unwrap().1,
        Some(super::super::FormatIssue::InvalidJson {..})));

    let bad = completed_reply("\"wrong\"}");
    assert!(!capped(&bad, &input, deadline(), &policy));
    assert!(matches!(validate(&bad, &input, &policy).unwrap().1,
        Some(super::super::FormatIssue::Schema {rule:super::super::SchemaRule::Type})));
    assert_eq!(validate_response(&bad, &input, policy.context_bytes, deadline(), &policy).unwrap_err(),
        "native_output_schema_mismatch");
}

#[test]
fn prefix_bounds_and_preflight_refuse_without_truncation_or_losing_the_original_schema() {
    let (_, mut input, policy) = fixture(true);
    input.output_prefix = Some("{\"answer\":".into());
    input.output_bytes = "{\"answer\":7}".len();
    let final_reply = completed_reply("7}");
    let original_schema = input.schema.clone();
    assert!(validate(&final_reply, &input, &policy).unwrap().1.is_none());
    input.output_bytes -= 1;
    assert_eq!(validate(&final_reply, &input, &policy).unwrap_err(),
        "native_output_cap_prefix_bound_task_output_bytes");
    assert_eq!(input.schema, original_schema);
    assert_eq!(input.output_prefix.as_deref(), Some("{\"answer\":"));
    assert_eq!(joined_text(Some("λ"), "z", 3).unwrap(), "λz");
    assert_eq!(joined_text(Some("λ"), "z", 2).unwrap_err(),
        "native_output_cap_prefix_bound_task_output_bytes");

    let mut invalid = serde_json::to_value(&input).unwrap();
    invalid["repair_enabled"] = json!(false);
    assert_eq!(preflight(&invalid, &policy).err().as_deref(), Some("native_output_cap_prefix_invalid"));
    invalid["repair_enabled"] = json!(true);
    invalid["output_prefix"] = json!("x".repeat(input.output_bytes.max(policy.context_bytes) + 1));
    assert_eq!(preflight(&invalid, &policy).err().as_deref(), Some("native_output_cap_prefix_invalid"));
    invalid["output_prefix"] = Value::Null;
    invalid["joined_charter"] = json!(true);
    assert_eq!(preflight(&invalid, &policy).err().as_deref(), Some("native_output_cap_prefix_invalid"));
    invalid["joined_charter"] = json!(false);
    invalid["schema"] = json!({"type":"not-a-schema-type"});
    assert!(preflight(&invalid, &policy).is_err());
}

#[test]
fn partial_calls_become_exact_quoted_context_without_tool_results_or_accounting_changes() {
    let (task, input, policy) = fixture(true);
    for arguments in ["{\"key\":", "{\"key\":7}"] {
        let call = partial_call(arguments);
        let items = json!([reasoning(), call]);
        let envelope = capped_reply(items.clone());
        let (encoded, issue) = validate(&envelope, &input, &policy).unwrap();
        assert!(issue.is_none());
        assert_eq!(serde_json::from_slice::<Value>(&encoded).unwrap(), items);
        let original_context = json!([{"role":"user","content":task.instruction}]);
        // Private test construction isolates the real context transformation.
        // It is not an assertion that this fixture crossed the CPU/receipt gate.
        let retained = NativeLoopCap {
            retained: NativeLoopFinal {
                task: task.clone(), profile: input.profile.clone(), context: original_context.clone(),
                output_items: items.clone(), request_index: 1, charged_through: 1, compacted: false,
            },
            prefix: None,
        };
        let next = retained.next_context().unwrap();
        assert_eq!(retained.output_items(), &items);
        assert_eq!(retained.request_index(), 1);
        assert_eq!(next.task, task);
        assert_eq!(next.profile, input.profile);
        assert_eq!(next.context, original_context);
        assert_eq!(next.request_index(), 1);
        assert_eq!(next.next_request_index().unwrap(), 2);
        assert!(!next.compacted);
        assert_eq!(next.output_items[0], items[0]);
        assert_eq!(next.output_items[1], json!({
            "role":"assistant",
            "content":format!("Original undispatched output-cap function item: {}", serde_json::to_string(&call).unwrap())
        }));
        let context = next.compaction_input().unwrap();
        assert_eq!(context, json!([original_context[0], items[0], next.output_items[1]]));
        assert!(context.as_array().unwrap().iter().all(|item|
            !matches!(item.get("type").and_then(Value::as_str), Some("function_call" | "function_call_output"))));
        assert!(context.as_array().unwrap().iter().all(|item| item.get("call_id").is_none()));
    }
    let malformed_completed = json!({"status":"completed","output":[{
        "type":"function_call","status":"completed","call_id":"original-call",
        "name":"read_record","arguments":"{\"key\":"
    }]});
    assert_eq!(validate(&malformed_completed, &input, &policy).unwrap_err(), "native_tool_arguments_invalid");
}

#[test]
fn capped_calls_keep_profile_identity_and_final_text_conflict_guards() {
    let (_, mut input, policy) = fixture(true);
    let original = capped_reply(json!([partial_call("{\"key\":")]));
    let mut unknown = original.clone(); unknown["output"][0]["name"] = json!("not_configured");
    assert_eq!(cap_shape(&unknown, &input, deadline(), &policy).err().as_deref(), Some("native_tool_unconfigured"));
    assert!(validate(&unknown, &input, &policy).is_err());
    let mut bad_id = original.clone(); bad_id["output"][0]["call_id"] = json!("");
    assert!(cap_shape(&bad_id, &input, deadline(), &policy).is_err());
    assert!(validate(&bad_id, &input, &policy).is_err());
    let mut extra = original.clone(); extra["output"][0]["result"] = json!({"fabricated":true});
    assert!(validate(&extra, &input, &policy).is_err());
    input.output_prefix = Some("{\"answer\":".into());
    assert_eq!(cap_shape(&original, &input, deadline(), &policy).err().as_deref(),
        Some("native_output_cap_final_text_call_conflict"));
    assert!(validate(&original, &input, &policy).is_err());
    let complete_call = json!({"status":"completed","output":[{
        "type":"function_call","status":"completed","call_id":"original-call",
        "name":"read_record","arguments":"{\"key\":7}"
    }]});
    assert_eq!(validate(&complete_call, &input, &policy).unwrap_err(),
        "native_output_cap_final_text_call_conflict");
}

#[test]
fn partial_wire_and_local_structural_bounds_cannot_become_cap_evidence() {
    let (_, input, policy) = fixture(true);
    let envelope = capped_reply(json!([reasoning(), message("{", "incomplete")]));
    let wire = serde_json::to_vec(&envelope).unwrap();
    let parsed = decode::parse(&wire, policy.response_wire_bytes, deadline(), "native_response_malformed",
        &policy, "native_response_wire_bytes").unwrap();
    assert!(capped(&parsed, &input, deadline(), &policy));
    for invalid in [wire[..wire.len()-1].to_vec(), [wire.as_slice(), b" trailing"].concat(),
        b"{\"status\":\"incomplete\",\"status\":\"incomplete\",\"output\":[]}".to_vec()] {
        assert!(decode::parse(&invalid, policy.response_wire_bytes, deadline(), "native_response_malformed",
            &policy, "native_response_wire_bytes").is_err());
    }
    assert_eq!(decode::parse(&wire, wire.len()-1, deadline(), "native_response_malformed",
        &policy, "native_response_wire_bytes").unwrap_err(), "native_json_byte_bound_native_response_wire_bytes");
    let (bytes, nodes) = schema::measure(&envelope["output"], policy.context_bytes, &policy, "native_context_bytes").unwrap();
    assert_eq!(validate_response_typed(&envelope, &input, bytes-1, deadline(), &policy).unwrap_err(),
        "native_json_byte_bound_native_context_bytes");
    let mut bounded = policy.clone(); bounded.json_nodes = nodes-1;
    assert_eq!(validate(&envelope, &input, &bounded).unwrap_err(), "native_json_complexity_bound_native_json_nodes");
    bounded = policy.clone(); bounded.json_depth = 1;
    assert_eq!(validate(&envelope, &input, &bounded).unwrap_err(), "native_json_complexity_bound_native_json_depth");
}

#[test]
fn expired_deadline_refuses_even_an_empty_capped_output() {
    let (_, input, policy) = fixture(true);
    for items in [json!([]), json!([reasoning()]), json!([message("{", "incomplete")])] {
        let envelope = capped_reply(items);
        let expired = Instant::now();
        assert!(!capped(&envelope, &input, expired, &policy));
        assert!(validate_response_typed(&envelope, &input, policy.context_bytes, expired, &policy).is_err());
    }
}

#[test]
fn capped_hosted_search_keeps_its_original_completed_item_contract() {
    let (_,mut input,policy)=fixture(true);
    input.profile.web_search=Some(NativeWebSearch {max_tool_calls:1});
    let mut envelope=capped_reply(json!([{"type":"web_search_call","id":"search-one","status":"incomplete","action":{"type":"search","query":"synthetic"}}]));
    assert!(validate(&envelope,&input,&policy).is_err());
    envelope["output"][0]["status"]=json!("completed");
    assert!(validate(&envelope,&input,&policy).is_ok());
}
