//! Explicit task configuration selects the decision protocol. Model JSON alone
//! never enables a workflow, changes a coding template, or proves child commit.
use super::{NativeResult, ProviderOutcome, RequestExposure, UsageObservation};
use crate::{
    delegation,
    protocol::{ChildOutcome, TaskInput},
};
use serde_json::{Value, json};

pub enum NativeStep {
    Complete(NativeResult),
    DelegateCoding { instruction: String },
}
pub struct NativeStepObservation {
    pub result: Result<NativeStep, String>,
    pub request_exposure: RequestExposure,
    pub usage: UsageObservation,
    pub provider_outcome: ProviderOutcome,
}
impl NativeStepObservation {
    pub(super) fn not_started(code: impl Into<String>) -> Self {
        Self {
            result: Err(code.into()),
            request_exposure: RequestExposure::NotStarted,
            usage: UsageObservation::Unknown,
            provider_outcome: ProviderOutcome::NotStarted,
        }
    }
}

pub(super) struct Plan {
    pub instruction: String,
    pub response_schema: Value,
    pub response_bound: usize,
    pub decision: bool,
}
pub(super) fn plan(
    input: &TaskInput,
    schema: &Value,
    continuation: Option<&ChildOutcome>,
) -> Result<Plan, String> {
    if crate::task_tool::enabled(input) {
        return Err("native_task_tool_consumer_required".into());
    }
    // Continuation carries the remaining request budget, not the immutable
    // initial reservation for a child plus both native requests.
    if continuation.is_none() {
        delegation::validate_workflow(input)?;
    }
    let decision = input.native_workflow.is_some() && continuation.is_none();
    let instruction = if let Some(child) = continuation {
        // The worker supplies the exact committed outcome; this function does
        // not infer it from model output or claim a commit from public fields.
        delegation::continuation_instruction(input, child)?
    } else if decision {
        delegation::decision_instruction(input)?
    } else {
        input.instruction.clone()
    };
    let response_bound = if decision {
        input
            .limits
            .output_bytes
            .checked_add(input.execution_limits()?.native.decision_envelope_bytes)
            .ok_or("native_output_limit_invalid_native_output_bytes")?
    } else {
        input.limits.output_bytes
    };
    let response_schema = if decision {
        // Do not embed the original result schema here: that would relocate its
        // root-local references. A complete value is validated separately.
        // Responses requires an explicit object root for json_schema output.
        // Keep the existing non-strict branches and local closed-shape checks;
        // declaring their common root type changes no accepted decision value.
        json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","oneOf":[
            {"type":"object","properties":{"kind":{"const":"complete"},"result":true},"required":["kind","result"],"additionalProperties":false},
            {"type":"object","properties":{"kind":{"const":"delegate"},"instruction":{"type":"string","minLength":1,"maxLength":input.limits.output_bytes}},"required":["kind","instruction"],"additionalProperties":false}
        ]})
    } else {
        schema.clone()
    };
    Ok(Plan {
        instruction,
        response_schema,
        response_bound,
        decision,
    })
}

pub(super) enum Decision {
    Complete(Value),
    Delegate(String),
}
pub(super) fn decode_decision(value: Value, output_bound: usize) -> Result<Decision, String> {
    let Value::Object(mut object) = value else {
        return Err("native_decision_shape".into());
    };
    if object.len() != 2 {
        return Err("native_decision_shape".into());
    }
    let Some(Value::String(kind)) = object.remove("kind") else {
        return Err("native_decision_shape".into());
    };
    match kind.as_str() {
        "complete" => object
            .remove("result")
            .map(Decision::Complete)
            .ok_or_else(|| "native_decision_shape".into()),
        "delegate" => match object.remove("instruction") {
            Some(Value::String(instruction)) if instruction.len() > output_bound => {
                Err("native_delegation_instruction_bound_task_output_bytes".into())
            }
            Some(Value::String(instruction)) if !instruction.is_empty() => {
                Ok(Decision::Delegate(instruction))
            }
            _ => Err("native_delegation_instruction_bound".into()),
        },
        _ => Err("native_decision_shape".into()),
    }
}
