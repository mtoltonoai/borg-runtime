//! Successor vocabulary carried by existing operations, receipts and snapshots.
use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct IdentityAdministrationRequest {
    pub expected_global_revision: String,
    pub expected_policy_revision: String,
    pub expected_governance_revision: String,
    pub mutation: IdentityMutation,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum IdentityMutation {
    Register {
        definition: Definition,
    },
    Retire {
        agent_id: String,
        expected_revision: String,
    },
    Replace {
        expected_revision: String,
        definition: Definition,
    },
    Charter {
        mutation: crate::charter::CharterMutation,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct IdentityAdministrationOutcome {
    pub actor: crate::collaboration::workflow::RecordedActor,
    pub policy: crate::limits::IdentityAdministrationPolicy,
    pub global_revision: String,
    pub governance_revision: String,
    pub comparison_rule: String,
    pub before: Option<Definition>,
    pub before_limits: Option<crate::limits::ExecutionLimits>,
    pub after_limits: Option<crate::limits::ExecutionLimits>,
    pub classification: ConfigurationChangeClass,
    /// A manager may remove its own usable selection, but cannot appoint one.
    pub manager_unusable_for_future_tasks: bool,
    pub effect: IdentityEffect,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum IdentityEffect {
    Registered(AgentSummary),
    Retired(AgentSummary),
    Replaced(AgentSummary),
    Charter(crate::charter::CharterOutcome),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskRepositorySelection {
    pub registration: crate::repository_catalogue::Revision,
    pub start_commit: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct TaskWorkload {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_requests: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_delegation_depth: Option<u32>,
}
fn present_string<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    String::deserialize(d).map(Some)
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TaskCreate {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_string"
    )]
    pub agent_id: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_string"
    )]
    pub expected_agent_revision: Option<String>,
    pub instruction: String,
    pub dependencies: Vec<String>,
    pub acceptance_commands: Vec<Vec<String>>,
    pub output_schema: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workload: Option<TaskWorkload>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository: Option<TaskRepositorySelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository_read: Option<TaskRepositorySelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruction_source: Option<TaskRepositorySelection>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TaskCreationProvenance {
    pub operation_id: String,
    pub logical_call_id: String,
    pub creator: crate::collaboration::workflow::RecordedActor,
    pub policy: crate::limits::TaskCreationPolicy,
    pub global_revision: String,
    pub governance_revision: String,
    pub default_agent_id: Option<String>,
    pub accepted_at_unix_ms: u64,
}

/// Typed reader fences: strings in instructions/results are not format markers.
pub fn policy_requires_wire24(p: &crate::limits::RuntimeLimits) -> bool {
    p.identity_administration.is_some()
        || p.task_creation.is_some()
        || p.execution.collaboration.capabilities.is_some()
}
pub fn definition_requires_wire24(d: &Definition) -> bool {
    d.limit_overrides
        .as_ref()
        .and_then(|o| o.collaboration.as_ref())
        .is_some_and(|c| c.capabilities.is_some())
}
pub fn input_requires_wire24(t: &TaskInput) -> bool {
    definition_requires_wire24(&t.definition)
        || t.resolved_limits.as_ref().is_some_and(|s| {
            policy_requires_wire24(&s.global_metadata)
                || s.values.collaboration.capabilities.is_some()
        })
        || crate::delegation::configured_template(t)
            .is_some_and(|c| definition_requires_wire24(&c.definition))
}
pub fn requires_wire24(op: &Operation) -> bool {
    match op {
        Operation::AdministerIdentity { .. } => true,
        Operation::CallCollaboration { call } => {
            crate::mcp::identity_administration::handles(&call.tool)
        }
        Operation::RegisterAgent { definition, .. } => definition_requires_wire24(definition),
        Operation::SubmitTask { input, .. } | Operation::DelegateTask { child: input, .. } => {
            input_requires_wire24(input)
        }
        Operation::SubmitTaskDraft { input, .. } => input
            .definition
            .as_ref()
            .is_some_and(definition_requires_wire24),
        _ => false,
    }
}
pub fn outcome_requires_wire24(outcome: &Outcome) -> bool {
    let Outcome::Ok { data } = outcome else {
        return false;
    };
    match data {
        Data::IdentityAdministration(_) => true,
        Data::Task(t) => input_requires_wire24(&t.input),
        Data::Tasks(ts) => ts.iter().any(|t| input_requires_wire24(&t.input)),
        Data::Agent(a) => definition_requires_wire24(&a.definition),
        Data::Agents(a) => a.iter().any(|a| definition_requires_wire24(&a.definition)),
        Data::DefinitionUpdate(v) => definition_requires_wire24(&v.selected_definition),
        Data::Commands(cs) => cs
            .iter()
            .any(|c| matches!(c,WorkerCommand::RunAttempt{task,..} if input_requires_wire24(task))),
        Data::Operation(v) => {
            serde_json::from_value::<Operation>(v.intended.clone())
                .is_ok_and(|o| requires_wire24(&o))
                || v.outcome.as_ref().is_some_and(|v| {
                    serde_json::from_value::<Outcome>(v.clone())
                        .is_ok_and(|o| outcome_requires_wire24(&o))
                })
        }
        Data::Collaboration(crate::collaboration::QueryResult::Limits { global, execution }) => {
            policy_requires_wire24(global)
                || execution.as_ref().is_some_and(|s| {
                    policy_requires_wire24(&s.global_metadata)
                        || s.values.collaboration.capabilities.is_some()
                })
        }
        Data::Collaboration(crate::collaboration::QueryResult::Call(v)) => {
            outcome_requires_wire24(&v.result.reply.outcome)
        }
        Data::WorkMutation(v) => input_requires_wire24(&v.view.task.input),
        Data::WorkQuery(crate::collaboration::work::WorkQueryResult::Task(v)) => {
            input_requires_wire24(&v.task.input)
        }
        _ => false,
    }
}
