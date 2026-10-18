//! Typed vocabulary fences, not numeric limit checks or policy enrollment.
//! Ordinary coding.frame_bytes/collaboration.journal_bytes values predate this
//! change. Only their new DefinitionLimitField names require the successor.
use super::*;
use crate::{governance::*, limits::{DefinitionLimitField, DefinitionUpdatePolicy, RuntimeLimits}};

pub(crate) fn field_uses(field: &DefinitionLimitField) -> bool {
    matches!(field, DefinitionLimitField::CodingFrameBytes | DefinitionLimitField::CollaborationJournalBytes)
}

pub(crate) fn policy_uses(policy: &DefinitionUpdatePolicy) -> bool {
    policy.allowed_fields.iter().any(field_uses)
}

pub(crate) fn metadata_uses(limits: &RuntimeLimits) -> bool {
    limits.definition_updates.as_ref().is_some_and(policy_uses)
}

pub(crate) fn input_uses(input: &TaskInput) -> bool {
    input.resolved_limits.as_ref().is_some_and(|s| metadata_uses(&s.global_metadata))
}

pub(crate) fn command_uses(command: &WorkerCommand) -> bool {
    matches!(command, WorkerCommand::RunAttempt { task, .. } if input_uses(task))
}

pub(crate) fn configuration_uses(configuration: &ConfigurationVersion) -> bool {
    configuration.widening_fields.iter().any(field_uses)
}

pub(crate) fn decision_uses(record: &DecisionRecord) -> bool {
    record.configuration_metadata.as_ref().is_some_and(|p| metadata_uses(p))
        || matches!(&record.detail, DecisionDetail::DefinitionLimits(d)
            if d.comparisons.iter().any(|c| field_uses(&c.field)))
}

pub(crate) fn invocation_uses(call: &crate::mcp::Invocation) -> bool {
    // Inspect the closed field-key position even in a refused malformed call.
    // Instruction text, unknown tools and arbitrary result JSON are not tags.
    call.tool == "collab_definition_update"
        && call.arguments.get("changes").and_then(Value::as_object).is_some_and(|changes|
            changes.keys().any(|key| matches!(key.as_str(), "coding.frame_bytes" | "collaboration.journal_bytes")))
}

pub(crate) fn operation_uses(operation: &Operation) -> bool {
    match operation {
        Operation::UpdateDefinition { update, .. } => update.changes.keys().any(field_uses),
        Operation::CallCollaboration { call } => invocation_uses(call),
        Operation::SubmitTask { input, .. } | Operation::DelegateTask { child: input, .. } => input_uses(input),
        Operation::MutateGovernance { mutation: GovernanceMutation::RegisterConfiguration { configuration }, .. } => configuration_uses(configuration),
        _ => false,
    }
}

pub(crate) fn outcome_uses(outcome: &Outcome) -> bool {
    let Outcome::Ok { data } = outcome else { return false; };
    match data {
        Data::DefinitionUpdate(value) => policy_uses(&value.policy) || value.changed_fields.iter().any(field_uses),
        Data::Task(task) => input_uses(&task.input),
        Data::Tasks(tasks) => tasks.iter().any(|t| input_uses(&t.input)),
        Data::Commands(commands) => commands.iter().any(command_uses),
        Data::WorkMutation(value) => input_uses(&value.view.task.input),
        Data::WorkQuery(crate::collaboration::work::WorkQueryResult::Task(value)) => input_uses(&value.task.input),
        Data::Collaboration(crate::collaboration::QueryResult::SessionInput { input, .. }) => input_uses(input),
        Data::Collaboration(crate::collaboration::QueryResult::Limits { global, execution }) =>
            metadata_uses(global) || execution.as_ref().is_some_and(|s| metadata_uses(&s.global_metadata)),
        Data::Collaboration(crate::collaboration::QueryResult::Call(record)) =>
            invocation_uses(&record.invocation) || outcome_uses(&record.result.reply.outcome),
        Data::Operation(record) if record.kind == "application_mutation" => {
            serde_json::from_value::<Operation>(record.intended.clone()).is_ok_and(|op| operation_uses(&op))
                || record.outcome.as_ref().is_some_and(|v|
                    serde_json::from_value::<Outcome>(v.clone()).is_ok_and(|o| outcome_uses(&o)))
        }
        Data::Governance(value) => match value.as_ref() {
            GovernanceResult::Configuration(c) => configuration_uses(c),
            GovernanceResult::Decision(d) => decision_uses(d),
            GovernanceResult::TaskCompletion { decision, .. } => decision.as_ref().is_some_and(|d| decision_uses(d)),
            _ => false,
        },
        _ => false,
    }
}
