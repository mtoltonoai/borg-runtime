use borg_runtime::{delegation::*, protocol::*};
use serde_json::json;

fn metadata() -> borg_runtime::limits::RuntimeLimits {
    borg_runtime::limits::RuntimeLimits::seed().unwrap()
}

fn parent() -> TaskInput {
    let mut input = TaskInput {
        resolved_limits: None,
        repository_read: None,
        instruction: "Use a coding child when source changes are needed.".into(),
        definition: Definition {
            limit_overrides: None,
            inference: Some(InferenceSettings::selected()),
            execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
            id: "native".into(),
            revision: "1".into(),
            executor: ExecutorKind::Native,
        },
        repository: None,
        acceptance_commands: vec![],
        output_schema: Some(json!({"type":"string"})),
        limits: Limits {
            deadline_unix_ms: 10_000,
            model_requests: 3,
            output_bytes: 4096,
            result_bytes: metadata().task_defaults.result_bytes,
            max_delegation_depth: 2,
        },
        dependencies: vec![],
        native_workflow: Some(Box::new(NativeWorkflow::CodingThenContinue {
            child: CodingTaskTemplate {
                instruction_prefix: "Preserve the configured repository and checks.".into(),
                definition: Definition {
                    limit_overrides: None,
                    inference: Some(InferenceSettings::selected()),
                    execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
                    id: "coding".into(),
                    revision: "2".into(),
                    executor: ExecutorKind::Coding,
                },
                repository: Repository {
                    registration: Some(borg_runtime::repository_catalogue::Revision {
                        repository_id: "fixture-repository".into(),
                        registration_revision: "1".into(),
                    }),
                    url: "/configured/source.git".into(),
                    start_commit: "a".repeat(40),
                    checkpoint_remote: "/configured/checkpoint.git".into(),
                },
                acceptance_commands: vec![vec!["/bin/true".into()]],
                dependencies: vec!["existing-input".into()],
                limits: Limits {
                    deadline_unix_ms: 9_000,
                    model_requests: 1,
                    output_bytes: 2048,
                    result_bytes: 8192,
                    max_delegation_depth: 1,
                },
            },
        })),
    };
    input.resolved_limits = Some(metadata().resolve_task(&input).unwrap());
    input
}

#[test]
// Traceability scope: Configuration-described bounded behavior only, not a guarantee of model quality or a general programmable policy engine.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# An agent's behavior MUST be described by configuration.
fn model_instruction_cannot_replace_configured_execution_fields() {
    let p = parent();
    validate_workflow(&p).unwrap();
    let instruction = r#"{"repository":"untrusted","provider":"untrusted","model_requests":99}"#;
    let child = coding_child(&p, instruction).unwrap();
    let configured = template(&p).unwrap();
    assert_eq!(child.repository.as_ref().unwrap(), &configured.repository);
    assert_eq!(child.definition, configured.definition);
    assert_eq!(child.definition.id, configured.definition.id);
    assert_ne!(child.definition.id, p.definition.id);
    assert_eq!(child.acceptance_commands, configured.acceptance_commands);
    assert_eq!(child.dependencies, configured.dependencies);
    assert_eq!(child.limits, configured.limits);
    assert_eq!(
        child.instruction,
        format!("{}\n\n{instruction}", configured.instruction_prefix)
    );
    assert!(child.native_workflow.is_none());
    assert!(child.output_schema.is_none());
    assert!(coding_child(&p, "").is_err());
    assert!(coding_child(&p, &"a".repeat(p.limits.output_bytes + 1)).is_err());
}

#[test]
fn configured_workflow_leaves_initial_and_continuation_request_capacity() {
    let mut p = parent();
    validate_workflow(&p).unwrap();
    p.limits.model_requests = 2;
    assert_eq!(
        validate_workflow(&p).unwrap_err(),
        "native_workflow_budget_invalid"
    );
    p.limits.model_requests = u32::MAX;
    let NativeWorkflow::CodingThenContinue { child } = p.native_workflow.as_deref_mut().unwrap()
    else {
        panic!("expected coding workflow")
    };
    child.limits.model_requests = u32::MAX;
    assert!(validate_workflow(&p).is_err());
    let mut p = parent();
    let NativeWorkflow::CodingThenContinue { child } = p.native_workflow.as_deref_mut().unwrap()
    else {
        panic!("expected coding workflow")
    };
    child.limits.deadline_unix_ms = p.limits.deadline_unix_ms + 1;
    assert!(validate_workflow(&p).is_err());
}

fn outcome() -> ChildOutcome {
    ChildOutcome {
        task_id: "child-task".into(),
        status: TaskStatus::Succeeded,
        summary: "quoted \"value\"\n\u{0001}".into(),
        result: Some(AcceptedResult {
            attempt_id: "child-attempt".into(),
            summary: "quoted \"value\"\n\u{0001}".into(),
            checkpoint: Some(Checkpoint {
                registration: borg_runtime::repository_catalogue::Revision {
                    repository_id: "fixture-repository".into(),
                    registration_revision: "1".into(),
                },
                repository: "repo\\with\"quotes".into(),
                commit: "b".repeat(40),
                remote_ref: "refs/heads/borg/tasks/fixed".into(),
                operation_id: "checkpoint-op".into(),
            }),
            validation: vec!["check 1: pass".into(), "check 2: pass".into()],
            output: Some(
                json!({"observed_usage":{"input_tokens":17,"output_tokens":9,"model_requests":null}}),
            ),
        }),
    }
}

#[test]
fn complete_result_bound_counts_escaped_metadata_and_every_field() {
    let mut p = parent();
    let child = outcome();
    let result = child.result.as_ref().unwrap();
    let encoded = serde_json::to_vec(result).unwrap();
    p.limits.result_bytes = encoded.len();
    assert!(accepted_result_fits(&p.limits, result));
    p.limits.result_bytes -= 1;
    assert!(!accepted_result_fits(&p.limits, result));
}

#[test]
fn continuation_preserves_complete_committed_outcome_without_truncation() {
    let p = parent();
    let child = outcome();
    let prompt = continuation_instruction(&p, &child).unwrap();
    assert!(prompt.starts_with(CONTINUATION_INSTRUCTION));
    let encoded = prompt.rsplit_once(CHILD_OUTCOME_SEPARATOR).unwrap().1;
    assert_eq!(
        serde_json::from_str::<ChildOutcome>(encoded).unwrap(),
        child
    );
    let mut oversized = child.clone();
    oversized
        .result
        .as_mut()
        .unwrap()
        .validation
        .push("x".repeat(8192));
    assert!(continuation_instruction(&p, &oversized).is_err());
    let mut pending = child.clone();
    pending.status = TaskStatus::Active;
    assert!(continuation_instruction(&p, &pending).is_err());
    let mut no_result = child;
    no_result.result = None;
    assert!(continuation_instruction(&p, &no_result).is_err());
}

#[test]
fn prompt_admission_counts_original_instruction_and_full_child_capacity() {
    let mut p = parent();
    validate_workflow(&p).unwrap();
    p.instruction = "x".repeat(metadata().execution.native.context_bytes - 4096);
    assert_eq!(
        validate_workflow(&p).unwrap_err(),
        "native_continuation_bound"
    );
    assert!(continuation_instruction(&p, &outcome()).is_ok());
    p.instruction = "x".repeat(metadata().execution.native.context_bytes);
    assert!(continuation_instruction(&p, &outcome()).is_err());
    assert!(decision_instruction(&p).is_err());
}

#[test]
fn workflow_configuration_is_closed_and_missing_policy_requires_explicit_metadata() {
    let p = parent();
    let mut wire = serde_json::to_value(&p).unwrap();
    wire.as_object_mut().unwrap().remove("native_workflow");
    wire["limits"]
        .as_object_mut()
        .unwrap()
        .remove("result_bytes");
    assert!(
        serde_json::from_value::<TaskInput>(wire.clone()).is_err(),
        "missing policy cannot acquire an executable default"
    );
    wire["limits"]["result_bytes"] = json!(metadata().task_defaults.result_bytes);
    let plain: TaskInput = serde_json::from_value(wire).unwrap();
    assert!(plain.native_workflow.is_none());
    assert_eq!(
        plain.limits.result_bytes,
        metadata().task_defaults.result_bytes
    );
    assert!(coding_child(&plain, "ordinary data").is_err());
    let mut wire = serde_json::to_value(&p).unwrap();
    wire["native_workflow"]["provider"] = json!("not-a-configured-field");
    assert!(serde_json::from_value::<TaskInput>(wire).is_err());
}

#[test]
fn decision_prompt_carries_original_root_schema_without_rewriting_references() {
    let mut p = parent();
    let schema =
        json!({"$defs":{"value":{"const":{"$ref":"literal-data"}}},"$ref":"#/$defs/value"});
    p.output_schema = Some(schema.clone());
    let prompt = decision_instruction(&p).unwrap();
    let original = prompt.rsplit_once(DECISION_SCHEMA_SEPARATOR).unwrap().1;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(original).unwrap(),
        schema
    );
    p.output_schema = Some(json!({"const":"x".repeat(metadata().execution.native.context_bytes)}));
    assert_eq!(
        decision_instruction(&p).unwrap_err(),
        "native_instruction_bound"
    );
}
