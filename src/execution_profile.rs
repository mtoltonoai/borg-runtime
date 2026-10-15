//! Immutable accepted collaboration selection. Missing profiles retain legacy
//! task bytes for decoding; this module never supplies an implicit default.
use crate::protocol::*;

pub fn collaboration(input: &TaskInput) -> bool {
    matches!(
        input.definition.execution_profile,
        Some(ExecutionProfile::CollaborationV1 { .. })
    )
}
fn selected(profile: Option<&ExecutionProfile>, executor: ExecutorKind) -> Result<(), String> {
    match profile {
        None => Ok(()),
        Some(ExecutionProfile::CollaborationV1 { web_search }) => {
            if web_search.as_ref().is_some_and(|s| s.max_tool_calls == 0) {
                return Err("native_web_search_limit_invalid".into());
            }
            if executor == ExecutorKind::Coding && web_search.is_some() {
                return Err("coding_profile_has_native_provider_settings".into());
            }
            Ok(())
        }
    }
}
pub fn validate(input: &TaskInput) -> Result<(), String> {
    selected(
        input.definition.execution_profile.as_ref(),
        input.definition.executor,
    )?;
    if collaboration(input)
        && matches!(
            input.native_workflow.as_deref(),
            Some(NativeWorkflow::InspectCurrentTaskThenContinue {})
        )
    {
        return Err("collaboration_profile_conflicts_with_legacy_inspection".into());
    }
    if let Some(NativeWorkflow::CodingThenContinue { child }) = input.native_workflow.as_deref() {
        child.definition.required_inference()?;
        child
            .definition
            .execution_profile
            .as_ref()
            .ok_or("template_profile_unbound")?;
        selected(
            child.definition.execution_profile.as_ref(),
            child.definition.executor,
        )?;
    }
    Ok(())
}
