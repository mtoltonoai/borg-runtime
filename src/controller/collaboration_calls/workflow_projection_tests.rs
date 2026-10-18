//! Finite known-repository metadata projection guard. Synthetic DTO inputs only;
//! this is not arbitrary-secret detection, stored-secret sanitization or admission.
use super::*;
use crate::collaboration::{Page, work::*, workflow::PrincipalRef};

fn sample() -> WorkView {
    let definition = Definition {
        charter: None,
        id: "projection-agent".into(), revision: "r1".into(), executor: ExecutorKind::Native,
        inference: Some(InferenceSettings::selected()), limit_overrides: None,
        execution_profile: Some(ExecutionProfile::CollaborationV2 { web_search: None }),
    };
    let limits = Limits { deadline_unix_ms: 999_999, model_requests: 2, output_bytes: 1024, result_bytes: 4096, max_delegation_depth: 1 };
    let task = Task {
        id: "projection-task".into(), session_id: "projection-session".into(),
        input: TaskInput { instruction_source: None, pull_request: None,  resolved_limits: None, repository_read: None,
            instruction: "Synthetic finite projection test".into(), definition,
            repository: None, acceptance_commands: vec![], output_schema: None,
            limits, dependencies: vec![], native_workflow: None },
        assigned_agent_id: "projection-agent".into(), status: TaskStatus::Queued, revision: 1,
        attempt: None, selected_checkpoint: None, result: None, blocked_reason: None, pending_assignment: None, native_retry_block: None,
        progress: String::new(), recent_output: String::new(), output_truncated: false,
        parent_task_id: None, waiting_child_id: None, completed_child: None,
    };
    WorkView { state: WorkState::Runnable { state: ResumeState { status: task.status, assigned_agent_id: task.assigned_agent_id.clone() } },
        task, coordination_revision: 0, responsible_owner: PrincipalRef::TrustedHostOperator,
        open_waits: 0, coordination_blocked: false, waits: Page { items: vec![], next_cursor: None } }
}
fn projections(view: WorkView) -> [Data; 2] {
    [Data::WorkQuery(WorkQueryResult::Task(view.clone())),
        Data::WorkMutation(WorkMutationOutcome { view, changed_wait: None, product_revision: 1 })]
}
fn repository() -> Repository {
    Repository { pull_request_target: None,  registration: None, url: "https://example.invalid/source.git".into(),
        start_commit: "a".repeat(40), checkpoint_remote: "origin".into() }
}

#[test]
// Scope: known repository identity operands in the two newly nested MCP results.
//= spec/impl/v2/implementation.md#shared-collaboration-tool-interface
//= type=test
//# Collaboration tool responses MUST exclude credential values.
fn nested_work_query_and_mutation_refuse_known_credential_operands_without_echo() {
    const MARKER: &str = "SYNTHETIC_NESTED_PROJECTION_CREDENTIAL";
    let token_url = format!("https://fixture-user:{MARKER}@example.invalid/source.git");
    let metadata = crate::limits::RuntimeLimits::seed().unwrap();
    for operand in 0..5 {
        let mut view = sample();
        match operand {
            0 | 1 => {
                let mut source = repository();
                if operand == 0 { source.url = token_url.clone(); } else { source.checkpoint_remote = token_url.clone(); }
                view.task.input.repository = Some(source);
            }
            2 => view.task.input.repository_read = Some(RepositoryReadSource {
                registration: None, repository: token_url.clone(), start_commit: "a".repeat(40),
            }),
            3 | 4 => {
                let mut source = repository();
                if operand == 3 { source.url = token_url.clone(); } else { source.checkpoint_remote = token_url.clone(); }
                let mut definition = view.task.input.definition.clone(); definition.executor = ExecutorKind::Coding;
                view.task.input.native_workflow = Some(Box::new(NativeWorkflow::CodingThenContinue {
                    child: CodingTaskTemplate { instruction_source: None, pull_request: None,  instruction_prefix: "Synthetic child".into(), definition,
                        repository: source, acceptance_commands: vec![], dependencies: vec![], limits: view.task.input.limits.clone() },
                }));
            }
            _ => unreachable!(),
        }
        for data in projections(view) {
            assert!(serde_json::to_string(&data).unwrap().contains(MARKER));
            match bounded(&metadata, (data, false)) {
                Err(Fault::Refused(code, message)) => {
                    assert_eq!(code, "credential_metadata");
                    assert_eq!(message, "credential-bearing metadata is not exposed");
                    assert!(!message.contains(MARKER));
                }
                other => panic!("nested operand {operand} must be refused: {other:?}"),
            }
        }
    }
}

#[test]
fn clean_nested_work_projection_preserves_exact_dto_and_changed_flag() {
    let metadata = crate::limits::RuntimeLimits::seed().unwrap();
    let mut view = sample(); view.task.input.repository = Some(repository());
    for data in projections(view) {
        for changed in [false, true] {
            assert_eq!(bounded(&metadata, (data.clone(), changed)).unwrap(), (data.clone(), changed));
        }
    }
}

#[test]
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# An operation refused by a configured limit MUST identify the limiting metadata field.
fn nested_work_projection_checks_complete_mcp_data_envelope_before_returning() {
    let mut metadata = crate::limits::RuntimeLimits::seed().unwrap();
    for data in projections(sample()) {
        let bytes = serde_json::to_vec(&data).unwrap().len();
        metadata.interface.mcp_data_bytes = bytes;
        assert_eq!(bounded(&metadata, (data.clone(), false)).unwrap(), (data.clone(), false));
        metadata.interface.mcp_data_bytes = bytes - 1;
        match bounded(&metadata, (data, false)) {
            Err(Fault::Refused(code, message)) => {
                assert_eq!(code, "response_bound"); assert!(message.contains("interface.mcp_data_bytes"));
            }
            other => panic!("complete envelope must be bounded: {other:?}"),
        }
    }
}
