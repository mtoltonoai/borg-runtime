//! Immutable accepted collaboration selection. Missing profiles retain legacy
//! task bytes for decoding; this module never supplies an implicit default.
use crate::protocol::*;

pub fn collaboration(input: &TaskInput) -> bool {
    matches!(
        input.definition.execution_profile,
        Some(ExecutionProfile::CollaborationV1 { .. } | ExecutionProfile::CollaborationV2 { .. })
    )
}
fn selected(profile: Option<&ExecutionProfile>, executor: ExecutorKind) -> Result<(), String> {
    match profile {
        None => Ok(()),
        Some(ExecutionProfile::CollaborationV1 { web_search } | ExecutionProfile::CollaborationV2 { web_search }) => {
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
pub(crate) fn validate_definition(definition: &Definition) -> Result<(), String> {
    selected(definition.execution_profile.as_ref(), definition.executor)
}
pub fn validate(input: &TaskInput) -> Result<(), String> {
    selected(
        input.definition.execution_profile.as_ref(),
        input.definition.executor,
    )?;
    if input.instruction_source.is_some()
        && (input.definition.executor != ExecutorKind::Coding || !collaboration(input))
    {
        return Err("instruction_source_requires_coding_collaboration_profile".into());
    }
    if collaboration(input)
        && matches!(
            input.native_workflow.as_deref(),
            Some(NativeWorkflow::InspectCurrentTaskThenContinue {})
        )
    {
        return Err("collaboration_profile_conflicts_with_legacy_inspection".into());
    }
    // Traceability scope: Both configured work scopes validate their explicit target definition; only canonical reassignment selects and binds the successor session.
    //= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
    //# A reassigned session MUST bind the selected target agent's accepted definition revision.
    if let Some(child) = crate::delegation::configured_template(input) {
        if child.instruction_source.is_some() && child.definition.executor != ExecutorKind::Coding {
            return Err("instruction_source_requires_coding_collaboration_profile".into());
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v2_keeps_native_search_and_coding_restrictions() {
        for profile in [
            ExecutionProfile::CollaborationV1 { web_search: None },
            ExecutionProfile::CollaborationV2 { web_search: None },
        ] {
            assert!(selected(Some(&profile), ExecutorKind::Native).is_ok());
            assert!(selected(Some(&profile), ExecutorKind::Coding).is_ok());
        }
        for profile in [
            ExecutionProfile::CollaborationV1 { web_search: Some(NativeSearchProfile { max_tool_calls: 1 }) },
            ExecutionProfile::CollaborationV2 { web_search: Some(NativeSearchProfile { max_tool_calls: 1 }) },
        ] {
            assert!(selected(Some(&profile), ExecutorKind::Native).is_ok());
            assert_eq!(selected(Some(&profile), ExecutorKind::Coding).unwrap_err(), "coding_profile_has_native_provider_settings");
        }
        for profile in [
            ExecutionProfile::CollaborationV1 { web_search: Some(NativeSearchProfile { max_tool_calls: 0 }) },
            ExecutionProfile::CollaborationV2 { web_search: Some(NativeSearchProfile { max_tool_calls: 0 }) },
        ] {
            assert_eq!(selected(Some(&profile), ExecutorKind::Native).unwrap_err(), "native_web_search_limit_invalid");
        }
        assert!(selected(None, ExecutorKind::Native).is_ok());
    }
}
