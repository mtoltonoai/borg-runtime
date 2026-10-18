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
    pub diagnostic: Option<super::ProviderFailureDiagnostic>,
}
impl NativeStepObservation {
    pub(super) fn not_started(code: impl Into<String>) -> Self {
        Self {
            result: Err(code.into()),
            request_exposure: RequestExposure::NotStarted,
            usage: UsageObservation::Unknown,
            provider_outcome: ProviderOutcome::NotStarted,
            diagnostic: Some(super::ProviderFailureDiagnostic::new(super::ProviderFailureClass::LocalAdmission)),
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
    // Traceability scope: Native planning keeps the current accepted instruction and result schema for Reassign; no child decision or continuation replaces that input.
    //= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
    //# A task reassignment MUST retain the task's prior session and execution-input history.
    let decision = delegation::template(input).is_some() && continuation.is_none();
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

#[cfg(test)]
mod reassignment_tests {
    use super::*;
    use crate::protocol::*;

    #[test]
    // Traceability scope: Planner-only discriminator for original instruction/schema and absence of a child continuation; durable lineage is tested by controller cases.
    //= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
    //= type=test
    //# A task reassignment MUST retain the task's prior session and execution-input history.
    fn reassignment_uses_original_result_schema_and_never_decision_or_child_continuation() {
        let metadata = crate::limits::RuntimeLimits::seed().unwrap();
        let mut input = TaskInput {
            instruction_source: None, pull_request: None, resolved_limits: None, repository_read: None,
            instruction: "Return the original result".into(),
            definition: Definition { charter: None, id: "triage".into(), revision: "r1".into(),
                executor: ExecutorKind::Native, inference: Some(InferenceSettings::selected()),
                execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }), limit_overrides: None },
            repository: None, acceptance_commands: vec![], output_schema: Some(json!({"type":"string"})),
            limits: metadata.task_defaults.at(0).unwrap(), dependencies: vec![], native_workflow: None,
        };
        let mut coding = input.definition.clone(); coding.id = "coding".into(); coding.executor = ExecutorKind::Coding;
        input.native_workflow = Some(Box::new(NativeWorkflow::Reassign { target: CodingTaskTemplate {
            instruction_source: None, pull_request: None, instruction_prefix: String::new(), definition: coding,
            repository: Repository { registration: None, url: "file:///repo".into(), start_commit: "a".repeat(40),
                checkpoint_remote: "origin".into(), pull_request_target: None },
            acceptance_commands: vec![vec!["check".into()]], dependencies: input.dependencies.clone(), limits: input.limits.clone(),
        }}));
        input.resolved_limits = Some(metadata.resolve_task(&input).unwrap());
        let plan = plan(&input, input.output_schema.as_ref().unwrap(), None).unwrap();
        assert!(!plan.decision);
        assert_eq!(plan.instruction, input.instruction);
        assert_eq!(plan.response_schema, *input.output_schema.as_ref().unwrap());
        assert_eq!(plan.response_bound, input.limits.output_bytes);
        let child = ChildOutcome { task_id: "unrelated-child".into(), status: TaskStatus::Failed,
            result: None, summary: "No child belongs to reassignment".into() };
        assert_eq!(super::plan(&input, input.output_schema.as_ref().unwrap(), Some(&child)).err().unwrap(),
            "native_reassignment_has_no_child_continuation");
    }
}
