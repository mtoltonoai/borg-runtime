//! Operator draft defaults only. Accepted TaskInput and stored limits retain
//! their original typed representation and explicitly supplied values.
use crate::limits::RuntimeLimits;
use crate::protocol::*;
use serde::{Deserialize, Deserializer};

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum WorkflowDraft {
    CodingThenContinue { child: ChildDraft },
    Reassign { target: CodingTaskTemplate },
    InspectCurrentTaskThenContinue {},
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChildDraft {
    #[serde(default)]
    pull_request: Option<crate::pull_request::Request>,
    #[serde(default)]
    instruction_source: Option<RepositoryReadSource>,
    instruction_prefix: String,
    definition: Definition,
    repository: Repository,
    acceptance_commands: Vec<Vec<String>>,
    dependencies: Vec<String>,
    limits: LimitsDraft,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LimitsDraft {
    deadline_unix_ms: u64,
    // Only omission defaults. Explicit null, duplicate fields and out-of-range
    // values keep the same refusal behavior as the actual Limits type.
    #[serde(default, deserialize_with = "explicit_requests")]
    model_requests: Option<u32>,
    output_bytes: usize,
    #[serde(default, deserialize_with = "explicit_result_bytes")]
    result_bytes: Option<usize>,
    max_delegation_depth: u32,
}
fn explicit_result_bytes<'de, D: Deserializer<'de>>(d: D) -> Result<Option<usize>, D::Error> {
    usize::deserialize(d).map(Some)
}
fn explicit_requests<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u32>, D::Error> {
    u32::deserialize(d).map(Some)
}

pub(super) fn native_workflow(
    value: &str,
    parent_requests: u32,
    legacy_receipt_only: bool,
    metadata: &RuntimeLimits,
) -> Result<Option<Box<NativeWorkflow>>, String> {
    let draft = serde_json::from_str::<Option<WorkflowDraft>>(value)
        .map_err(|_| "Invalid native workflow JSON or unknown configuration field".to_string())?;
    let Some(draft) = draft else { return Ok(None) };
    let workflow = match draft {
        WorkflowDraft::InspectCurrentTaskThenContinue {} => {
            if legacy_receipt_only {
                // Read compatibility only; fresh controller admission refuses
                // an unbound definition before any attempt can be launched.
                return Ok(Some(Box::new(
                    NativeWorkflow::InspectCurrentTaskThenContinue {},
                )));
            }
            return Err("Use the normal collaboration tools to inspect the current task; inspect_current_task_then_continue is no longer accepted by this form".into());
        }
        WorkflowDraft::Reassign { target } => {
            validate_configured_target(&target, metadata)?;
            if target.definition.executor != ExecutorKind::Coding || !target.instruction_prefix.is_empty() {
                return Err("Reassignment requires a coding target and an empty instruction_prefix".into());
            }
            NativeWorkflow::Reassign { target }
        }
        WorkflowDraft::CodingThenContinue { child } => {
            if let Some(source) = &child.instruction_source {
                validate_instruction_source(source, &child.definition, metadata)?;
            }
            let registration = child
                .repository
                .registration
                .as_ref()
                .ok_or("Configured child requires an explicit repository registration")?;
            if !crate::mcp::valid_identity(
                &registration.repository_id,
                metadata.transport.identity_bytes,
            ) || !crate::mcp::valid_identity(
                &registration.registration_revision,
                metadata.transport.identity_bytes,
            ) || !child.repository.url.is_empty()
                || !child.repository.checkpoint_remote.is_empty()
                || child.repository.pull_request_target.is_some()
            {
                return Err("Configured child must select a bounded repository ID and revision with empty server-owned endpoints".into());
            }
            let requests = match child.limits.model_requests {
                Some(value) => value,
                // One parent decision and one parent continuation are required by
                // this workflow shape; this subtraction is not a policy ceiling.
                None => parent_requests
                    .checked_sub(2)
                    .filter(|value| *value > 0)
                    .ok_or("native_workflow_budget_invalid")?,
            };
            NativeWorkflow::CodingThenContinue {
                child: CodingTaskTemplate {
                    pull_request: child.pull_request,
                    instruction_source: child.instruction_source,
                    instruction_prefix: child.instruction_prefix,
                    definition: child.definition,
                    repository: child.repository,
                    acceptance_commands: child.acceptance_commands,
                    dependencies: child.dependencies,
                    limits: Limits {
                        deadline_unix_ms: child.limits.deadline_unix_ms,
                        model_requests: requests,
                        output_bytes: child.limits.output_bytes,
                        result_bytes: child
                            .limits
                            .result_bytes
                            .unwrap_or(metadata.task_defaults.result_bytes),
                        max_delegation_depth: child.limits.max_delegation_depth,
                    },
                },
            }
        }
    };
    Ok(Some(Box::new(workflow)))
}

/// Editable illustration only; no workflow is submitted unless the operator
/// explicitly supplies the JSON control. Numeric values mirror this draft.
// Traceability scope: The editable example mirrors existing task limits; it supplies no implicit accepted target or deadline.
//= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
//# A task reassignment MUST preserve the task's accepted deadline.
pub(super) fn reassignment_help(fields: &super::Form) -> String {
    let value = |name: &str| fields.get(name).and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .filter(serde_json::Value::is_number).unwrap_or_else(|| serde_json::Value::String(format!("copy {name}")));
    let dependencies: Vec<_> = fields.get("dependencies").map(|raw| raw.split([',', '\n', '\r'])
        .map(str::trim).filter(|v| !v.is_empty()).collect()).unwrap_or_default();
    let example = serde_json::json!({"kind":"reassign", "target": {
        "definition":{"id":"coding-agent-id", "revision":"accepted-revision", "executor":"coding",
            "inference":{"model":SELECTED_MODEL,"reasoning_effort":SELECTED_REASONING_EFFORT},
            "execution_profile":{"kind":"collaboration_v1"}},
        "repository":{"registration":{"repository_id":"registered-repository-id","registration_revision":"immutable-revision"},
            "start_commit":"replace-with-exact-commit"},
        "instruction_prefix":"", "acceptance_commands":[["your-check-command"]], "dependencies":dependencies,
        "limits":{"deadline_unix_ms":value("deadline_unix_ms"), "model_requests":value("model_requests"),
            "output_bytes":value("output_bytes"), "result_bytes":value("result_bytes"), "max_delegation_depth":value("max_delegation_depth")}
    }});
    format!("<details><summary>Reassignment example</summary><p>Paste this into the workflow field and replace the coding definition, repository registration, exact commit and checks with your accepted choices. Copy the complete definition from the agent page, including any charter or limit overrides. Keep limits and dependencies equal to this task if you change the draft. This example is not submitted automatically.</p><pre>{}</pre></details>",
        super::escape(&serde_json::to_string_pretty(&example).unwrap_or_default()))
}

/// Reassignment targets are fully explicit; no child-budget defaults apply.
pub(super) fn validate_configured_target(target: &CodingTaskTemplate, metadata: &RuntimeLimits) -> Result<(), String> {
    let registration = target.repository.registration.as_ref()
        .ok_or("Configured work requires an explicit repository registration")?;
    if !crate::mcp::valid_identity(&registration.repository_id, metadata.transport.identity_bytes)
        || !crate::mcp::valid_identity(&registration.registration_revision, metadata.transport.identity_bytes)
        || !target.repository.url.is_empty() || !target.repository.checkpoint_remote.is_empty()
        || target.repository.pull_request_target.is_some()
    {
        return Err("Configured work must select a bounded repository ID and revision with empty server-owned endpoints".into());
    }
    if let Some(source) = &target.instruction_source {
        validate_instruction_source(source, &target.definition, metadata)?;
    }
    Ok(())
}

/// Structural browser input only. The controller resolves the registration and
/// checks current admission policy after looking up the original operation.
pub(super) fn validate_instruction_source(
    source: &RepositoryReadSource,
    definition: &Definition,
    metadata: &RuntimeLimits,
) -> Result<(), String> {
    if definition.executor != ExecutorKind::Coding
        || !matches!(definition.execution_profile,
            Some(ExecutionProfile::CollaborationV1 { .. } | ExecutionProfile::CollaborationV2 { .. }))
    {
        return Err("An instruction source requires a coding task with an explicit collaboration profile".into());
    }
    let registration = source.registration.as_ref()
        .ok_or("An instruction source requires an explicit repository registration")?;
    if !crate::mcp::valid_identity(&registration.repository_id, metadata.transport.identity_bytes)
        || !crate::mcp::valid_identity(&registration.registration_revision, metadata.transport.identity_bytes)
        || !source.repository.is_empty()
    {
        return Err("An instruction source requires a bounded repository ID and revision with an empty server-owned endpoint".into());
    }
    if !matches!(source.start_commit.len(), 40 | 64)
        || !source.start_commit.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("An instruction source requires an exact lowercase commit".into());
    }
    Ok(())
}


/// Only a structurally decoded draft whose known repository operands contain no
/// credential is eligible for validation redisplay. This does not scan arbitrary
/// text for secrets and never changes an accepted request or a stored record.
// Traceability scope: refused browser native-workflow drafts, not arbitrary secrets.
//= spec/impl/v2/implementation.md#shared-product-state
//# The browser interface MUST present the information exposed through the shared agent interface.
pub(super) fn may_redisplay_native_workflow(value: &str) -> bool {
    match serde_json::from_str::<Option<WorkflowDraft>>(value) {
        Ok(None | Some(WorkflowDraft::InspectCurrentTaskThenContinue {})) => true,
        Ok(Some(WorkflowDraft::CodingThenContinue { child })) => {
            !crate::credential::repository_contains_credentials(&child.repository.url)
                && !crate::credential::repository_contains_credentials(&child.repository.checkpoint_remote)
                && child.repository.pull_request_target.is_none()
                && child.instruction_source.as_ref().is_none_or(|source| source.repository.is_empty())
        }
        Ok(Some(WorkflowDraft::Reassign { target })) => {
            !crate::credential::repository_contains_credentials(&target.repository.url)
                && !crate::credential::repository_contains_credentials(&target.repository.checkpoint_remote)
                && target.repository.pull_request_target.is_none()
                && target.instruction_source.as_ref().is_none_or(|source| source.repository.is_empty())
        }
        // An undecodable known field cannot be safely projected. Other submitted
        // form values remain available for correction; no request is retried.
        Err(_) => false,
    }
}

#[cfg(test)]
mod reassignment_tests {
    use super::*;

    #[test]
    // Traceability scope: Browser decoding preserves explicit target fields and full limits without child-budget defaults; canonical admission owns equality and session binding.
    //= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
    //= type=test
    //# A task reassignment MUST preserve the task's accepted deadline.
    //= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
    //= type=test
    //# A reassigned session MUST bind the selected target agent's accepted definition revision.
    fn reassignment_target_is_explicit_without_child_defaults_or_private_redisplay() {
        let metadata = RuntimeLimits::seed().unwrap();
        let target = CodingTaskTemplate {
            instruction_source: None, pull_request: None, instruction_prefix: String::new(),
            definition: Definition { charter: None, id: "coding".into(), revision: "r1".into(),
                executor: ExecutorKind::Coding, inference: Some(InferenceSettings::selected()),
                execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }), limit_overrides: None },
            repository: Repository { registration: Some(crate::repository_catalogue::Revision {
                repository_id: "repo".into(), registration_revision: "r1".into() }),
                url: String::new(), start_commit: "a".repeat(40), checkpoint_remote: String::new(), pull_request_target: None },
            acceptance_commands: vec![vec!["check".into()]], dependencies: vec!["prerequisite".into()],
            limits: metadata.task_defaults.at(0).unwrap(),
        };
        let expected = NativeWorkflow::Reassign { target };
        let value = serde_json::to_value(&expected).unwrap();
        let decoded = native_workflow(&value.to_string(), 1, false, &metadata).unwrap().unwrap();
        assert_eq!(*decoded, expected, "the child decision/continuation allowance must not alter a handoff");
        assert!(may_redisplay_native_workflow(&value.to_string()));
        for field in ["model_requests", "result_bytes"] {
            let mut omitted = value.clone(); omitted["target"]["limits"].as_object_mut().unwrap().remove(field);
            assert!(native_workflow(&omitted.to_string(), 1, false, &metadata).is_err(), "{field}");
        }
        let mut prefixed = value.clone(); prefixed["target"]["instruction_prefix"] = "changed work".into();
        assert!(native_workflow(&prefixed.to_string(), 1, false, &metadata).is_err());
        let mut private = value.clone(); private["target"]["repository"]["url"] = "https://user:secret@example.com/repo".into();
        assert!(native_workflow(&private.to_string(), 1, false, &metadata).is_err());
        assert!(!may_redisplay_native_workflow(&private.to_string()));
    }
}
