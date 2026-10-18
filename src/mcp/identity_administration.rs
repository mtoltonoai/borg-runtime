//! One shared catalogue and typed decoder for both executors.
use super::Tool;
use crate::{
    charter::*,
    limits::{CollaborationCapabilities, RuntimeLimits},
    protocol::*,
};
use serde_json::{Value, json};

pub fn handles(name: &str) -> bool {
    matches!(
        name,
        "collab_task_create"
            | "collab_agent_register"
            | "collab_agent_retire"
            | "collab_definition_replace"
            | "collab_charter_publish"
            | "collab_charter_select"
            | "collab_charter_set_pending"
    )
}
fn object(properties: Value, required: &[&str]) -> Value {
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}
fn all(properties: Value) -> Value {
    let keys: Vec<_> = properties
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    object(properties.clone(), &keys)
}
fn nullable(value: Value) -> Value {
    json!({"anyOf":[value,{"type":"null"}]})
}
fn numeric_group(value: Value) -> Value {
    let fields = value
        .as_object()
        .unwrap()
        .iter()
        .filter(|(key, _)| key.as_str() != "capabilities")
        .map(|(key, value)| {
            (
                key.clone(),
                if value.is_null() {
                    nullable(json!({"type":"integer","minimum":1}))
                } else {
                    json!({"type":"integer","minimum":0})
                },
            )
        })
        .collect::<serde_json::Map<_, _>>();
    all(Value::Object(fields))
}
pub fn catalogue(
    limits: &RuntimeLimits,
    capabilities: Option<&CollaborationCapabilities>,
) -> Vec<Tool> {
    let Some(c) = capabilities else {
        return vec![];
    };
    let id = json!({"type":"string","minLength":1,"maxLength":limits.transport.identity_bytes,"x-borg-maxUtf8Bytes":limits.transport.identity_bytes});
    let text =
        json!({"type":"string","minLength":1,"maxLength":limits.controller.max_task_input_bytes});
    let reference = all(json!({"definition_id":id,"definition_revision":id}));
    let charter = json!({"oneOf":[all(json!({"kind":{"const":"inline"},"source":all(json!({
        "files":{"type":"array","minItems":1,"items":all(json!({"file":id,"text":text}))},
        "clauses":{"type":"array","minItems":1,"items":all(json!({"file":id,"section":id,"sentence":text}))}
    }))})),all(json!({"kind":{"const":"reference"},"source":reference}))]});
    let caps = all(
        json!({"version":{"const":1},"task_creation":{"type":"boolean"},"identity_administration":{"type":"boolean"}}),
    );
    let mut collaboration =
        numeric_group(serde_json::to_value(&limits.execution.collaboration).unwrap());
    collaboration["properties"]["capabilities"] = nullable(caps);
    let overrides = all(json!({
        "native":nullable(numeric_group(serde_json::to_value(&limits.execution.native).unwrap())),
        "coding":nullable(numeric_group(serde_json::to_value(&limits.execution.coding).unwrap())),
        "collaboration":nullable(collaboration),
        "memory_notice":nullable(numeric_group(serde_json::to_value(&limits.execution.memory_notice).unwrap()))
    }));
    let profile = object(
        json!({"kind":{"enum":["collaboration_v1","collaboration_v2"]},
        "web_search":nullable(all(json!({"max_tool_calls":{"type":"integer","minimum":1,"maximum":u32::MAX}})))}),
        &["kind"],
    );
    let definition = object(
        json!({"id":id,"revision":id,"executor":{"enum":["coding","native"]},
        "inference":all(json!({"model":{"type":"string","minLength":1},"reasoning_effort":{"type":"string","minLength":1}})),
        "execution_profile":profile,"limit_overrides":nullable(overrides),"charter":nullable(charter.clone())}),
        &[
            "id",
            "revision",
            "executor",
            "inference",
            "execution_profile",
        ],
    );
    let mut result = Vec::new();
    let mut push =
        |name: &str, description: &str, schema: Value| {
            result.push(Tool {name:name.into(),description:description.into(),input_schema:schema,
        annotations:json!({"readOnlyHint":false,"idempotentHint":false,"openWorldHint":false})})
        };
    if c.task_creation && limits.task_creation.as_ref().is_some_and(|p| p.enabled) {
        let source = all(
            json!({"registration":all(json!({"repository_id":id,"registration_revision":id})),"start_commit":{"type":"string","pattern":"^(?:[0-9a-f]{40}|[0-9a-f]{64})$"}}),
        );
        let workload = object(
            json!({"deadline_ms":{"type":"integer","minimum":1},"model_requests":{"type":"integer","minimum":1,"maximum":u32::MAX},
            "output_bytes":{"type":"integer","minimum":1},"result_bytes":{"type":"integer","minimum":1},"max_delegation_depth":{"type":"integer","minimum":0,"maximum":u32::MAX}}),
            &[],
        );
        push(
            "collab_task_create",
            "Create one independent ordinary task for an existing active agent; omission selects current task defaults. Creation is not execution or completion. Reconcile an unknown outcome using its original call identity.",
            object(
                json!({"agent_id":id,"expected_agent_revision":id,"instruction":text,
                "dependencies":{"type":"array","maxItems":limits.controller.max_dependencies,"items":id},
                "acceptance_commands":{"type":"array","maxItems":limits.controller.max_acceptance_commands,"items":{"type":"array","minItems":1,"maxItems":limits.controller.max_acceptance_arguments,"items":{"type":"string","maxLength":limits.controller.max_acceptance_argument_bytes}}},
                "output_schema":{},"workload":workload,"repository":source,"repository_read":source,"instruction_source":source}),
                &[
                    "instruction",
                    "dependencies",
                    "acceptance_commands",
                    "output_schema",
                ],
            ),
        );
    }
    if c.identity_administration
        && limits
            .identity_administration
            .as_ref()
            .is_some_and(|p| p.enabled)
    {
        for (name, description, fields) in [
            (
                "collab_agent_register",
                "Register an immutable agent identity without a task. Only the current configured bound manager can administer identities.",
                json!({"definition":definition}),
            ),
            (
                "collab_agent_retire",
                "Retire an active identity for future work, preserving accepted work and exact originals. Retiring the manager requires trusted operator recovery.",
                json!({"agent_id":id,"expected_revision":id}),
            ),
            (
                "collab_definition_replace",
                "Select a complete validated immutable definition revision for future tasks under head/global/policy/governance CAS. Does not alter accepted sessions or policy.",
                json!({"definition":definition,"expected_revision":id}),
            ),
            (
                "collab_charter_publish",
                "Publish a same-agent immutable charter source without selecting it or waking a session.",
                json!({"agent_id":id,"expected_definition_revision":id,"charter":charter}),
            ),
            (
                "collab_charter_select",
                "Select a published charter for future tasks without changing existing session delivery.",
                json!({"agent_id":id,"expected_definition_revision":id,"source":reference}),
            ),
            (
                "collab_charter_set_pending",
                "Record a pending session charter; safe-boundary delivery and acknowledgment remain separate.",
                json!({"session_id":id,"expected_revision":{"type":"integer","minimum":1,"maximum":i64::MAX},"target":reference}),
            ),
        ] {
            let mut fields = fields.as_object().unwrap().clone();
            for key in [
                "expected_global_revision",
                "expected_policy_revision",
                "expected_governance_revision",
            ] {
                fields.insert(key.into(), id.clone());
            }
            push(name, description, all(Value::Object(fields)));
        }
    }
    result
}
pub fn validate(
    name: &str,
    args: &Value,
    limits: &RuntimeLimits,
    c: Option<&CollaborationCapabilities>,
) -> Result<(), String> {
    let schema = catalogue(limits, c)
        .into_iter()
        .find(|t| t.name == name)
        .ok_or("tool is not in the accepted collaboration capability selection")?
        .input_schema;
    if !jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .build(&schema)
        .map_err(|_| "invalid identity/task tool schema")?
        .is_valid(args)
    {
        return Err("arguments differ from the closed identity/task creation schema".into());
    }
    if name == "collab_task_create" {
        serde_json::from_value::<TaskCreate>(args.clone()).map_err(|e| e.to_string())?;
    } else {
        parse(name, args)?;
    }
    Ok(())
}
/// Also used by retained readers; this decoder never authorizes its caller.
pub fn parse(name: &str, args: &Value) -> Result<IdentityAdministrationRequest, String> {
    let mut a = args
        .as_object()
        .ok_or("identity arguments must be an object")?
        .clone();
    let mut take = |key: &str| -> Result<String, String> {
        a.remove(key)
            .and_then(|v| v.as_str().map(str::to_owned))
            .ok_or_else(|| format!("missing {key}"))
    };
    let global = take("expected_global_revision")?;
    let policy = take("expected_policy_revision")?;
    let governance = take("expected_governance_revision")?;
    let kind = match name {
        "collab_agent_register" => "register",
        "collab_agent_retire" => "retire",
        "collab_definition_replace" => "replace",
        "collab_charter_publish" => "publish",
        "collab_charter_select" => "select_for_future_tasks",
        "collab_charter_set_pending" => "set_pending",
        _ => return Err("unknown identity tool".into()),
    };
    a.insert("kind".into(), json!(kind));
    let mutation = if name.starts_with("collab_charter_") {
        if kind != "set_pending" {
            a.insert("expected_global_revision".into(), json!(global));
            a.insert("expected_governance_revision".into(), json!(governance));
        }
        IdentityMutation::Charter {
            mutation: serde_json::from_value::<CharterMutation>(Value::Object(a))
                .map_err(|e| e.to_string())?,
        }
    } else {
        serde_json::from_value(Value::Object(a)).map_err(|e| e.to_string())?
    };
    Ok(IdentityAdministrationRequest {
        expected_global_revision: global,
        expected_policy_revision: policy,
        expected_governance_revision: governance,
        mutation,
    })
}
