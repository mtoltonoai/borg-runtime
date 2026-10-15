//! Operator draft defaults only. Accepted TaskInput and stored limits retain
//! their original typed representation and explicitly supplied values.
use crate::limits::RuntimeLimits;
use crate::protocol::*;
use serde::{Deserialize, Deserializer};

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum WorkflowDraft {
    CodingThenContinue { child: ChildDraft },
    InspectCurrentTaskThenContinue {},
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChildDraft {
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
        WorkflowDraft::CodingThenContinue { child } => {
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
