//! Candidate data admission only: no provider request or deployed policy claim.
use super::*;

fn depth(value: &Value) -> usize {
    match value {
        Value::Array(items) => items.iter().map(depth).max().map_or(0, |n| n + 1),
        Value::Object(items) => items.values().map(depth).max().map_or(0, |n| n + 1),
        _ => 0,
    }
}

#[test]
fn exact_candidate_charter_catalogue_and_initial_probe_fit_selected_bounds() {
    // Byte-exact pending activation data. This is not an invented live task;
    // the ordinary probe instruction/schema are shared with identity_support.
    let limits: crate::limits::RuntimeLimits = serde_json::from_str(include_str!(
        "testdata/activation-controller-limits.pending.json"
    )).unwrap();
    let definition: crate::protocol::Definition = serde_json::from_str(include_str!(
        "testdata/activation-charter-definition.pending.json"
    )).unwrap();
    limits.validate().unwrap();
    let execution = limits.resolve_definition(&definition).unwrap();
    let policy = &execution.native;
    assert_eq!(policy, &limits.execution.native);
    assert_eq!(policy.function_count, 73);
    let mut rendered = limits.clone();
    rendered.execution = execution.clone();
    let tools = crate::mcp::catalogue_for_profile(&rendered, definition.execution_profile.as_ref());
    let profile = NativeLoopProfile {
        functions: tools.into_iter().map(|t| NativeFunction {
            name: t.name, description: t.description, parameters: t.input_schema,
        }).collect(),
        web_search: None,
    };
    assert_eq!(profile.functions.len(), 73);
    selected(&profile, policy).unwrap();
    let output_schema = json!({"type":"integer"});
    let validation = serde_json::to_value(ValidationInput {
        schema: output_schema.clone(), profile: profile.clone(),
        output_bytes: limits.task_defaults.output_bytes,
        repair_enabled: limits.native_retry.is_some(), output_prefix: None, joined_charter: false,
    }).unwrap();
    let validation_size = schema::measure(&validation, policy.schema_bytes, policy, "native_schema_bytes").unwrap();
    preflight(&validation, policy).unwrap();
    let inference = definition.required_inference().unwrap();
    let body = json!({
        "model":inference.model,"reasoning":{"effort":inference.reasoning_effort},
        "input":[{"role":"user","content":"Bound caller fixture"}],"store":false,"stream":false,
        "include":["reasoning.encrypted_content"],"max_output_tokens":policy.max_output_tokens,
        "tools":profile.functions.iter().map(|f| json!({"type":"function","name":f.name,
            "description":f.description,"parameters":f.parameters,"strict":false})).collect::<Vec<_>>(),
        "tool_choice":"auto","parallel_tool_calls":policy.function_calls_per_response>1,
        "text":{"format":response_format(&output_schema).unwrap()}
    });
    let Some(crate::charter::DefinitionCharter::Inline { source }) = &definition.charter else {
        panic!("exact candidate inline charter");
    };
    let reference = json!({"definition_id":definition.id,"definition_revision":definition.revision});
    let source_json = serde_json::to_string(source).unwrap();
    let exact_charter = serde_json::to_string(&json!({"revision":reference,"source":source_json})).unwrap();
    let exchange = crate::native::CharterExchange::new(crate::native::CharterInput {
        revision: reference, source: source_json, changed: false,
    }, policy).unwrap();
    let request = exchange.prepare(&body, policy).unwrap();
    assert_eq!(request["tools"], body["tools"]);
    assert_eq!(request["input"][1]["content"], "Bound caller fixture");
    assert!(request["input"][0]["content"].as_str().unwrap().contains(&exact_charter));
    let context = schema::measure(&request["input"], policy.context_bytes, policy, "native_context_bytes").unwrap();
    let request_size = schema::measure(&request, policy.request_bytes, policy, "native_request_bytes").unwrap();
    let profile_size = schema::measure(&serde_json::to_value(&profile).unwrap(), policy.schema_bytes, policy, "native_schema_bytes").unwrap();
    println!("candidate_activation_admission {}", json!({
        "scope":"exact pending metadata and charter with fixed ordinary probe; no provider or live task",
        "functions":profile.functions.len(),"profile_bytes":profile_size.0,"profile_nodes":profile_size.1,
        "validation_bytes":validation_size.0,"validation_nodes":validation_size.1,"validation_depth":depth(&validation),
        "context_bytes":context.0,"context_nodes":context.1,"context_depth":depth(&request["input"]),
        "request_bytes":request_size.0,"request_nodes":request_size.1,"request_depth":depth(&request),
        "selected_function_count":policy.function_count,"selected_schema_bytes":policy.schema_bytes,
        "selected_json_nodes":policy.json_nodes,"selected_json_depth":policy.json_depth,
        "selected_context_bytes":policy.context_bytes,"selected_request_bytes":policy.request_bytes
    }));
}
