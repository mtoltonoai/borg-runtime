use super::*;
use crate::{limits::RuntimeLimits, mcp, protocol::ExecutionProfile};

#[test]
fn closed_read_list_matches_current_canonical_adapters_and_never_name_shapes() {
    let limits = RuntimeLimits::seed().unwrap();
    let profile = ExecutionProfile::CollaborationV2 { web_search: None };
    let tools = mcp::catalogue_for_profile(&limits, Some(&profile));
    let mut reads = 0;
    for tool in tools {
        // Only this test uses the local catalogue annotation as an independent
        // inventory oracle. Production dispatch never trusts an annotation.
        let read = tool.annotations["readOnlyHint"] == true;
        assert_eq!(retryable_read_tool(&tool.name), read, "{}", tool.name);
        reads += usize::from(read);
    }
    assert_eq!(reads, 40);
    for name in ["collab_fake_get", "collab_task_get_extra", "COLLAB_TASK_GET",
        "collab_definition_update", "collab_memory_set", "collab_task_delegate",
        "collab_question_answer", "collab_document_version_create", "collab_review_decide",
        "collab_task_owner", "__host_lookup"] {
        assert!(!retryable_read_tool(name), "{name}");
    }
    assert!(retryable_read_tool("collab_question_get"));
    assert!(mcp::validate_arguments_for_profile("collab_question_get",
        &serde_json::json!({"question_id":"q"}), &limits,
        Some(&ExecutionProfile::CollaborationV1 { web_search: None })).is_err(),
        "read recovery eligibility never grants profile authority");
}
