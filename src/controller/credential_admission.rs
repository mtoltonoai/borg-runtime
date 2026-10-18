//! A finite typed-input guard, not arbitrary-secret discovery. Reject before
//! the receipt engine can persist even a refused request's repository operand.
use crate::{credential::repository_contains_credentials, protocol::*};

fn repository(value: &Repository) -> bool {
    repository_contains_credentials(&value.url)
        || repository_contains_credentials(&value.checkpoint_remote)
}

fn task(value: &TaskInput) -> bool {
    value.repository.as_ref().is_some_and(repository)
        || value
            .repository_read
            .as_ref()
            .is_some_and(|source| repository_contains_credentials(&source.repository))
        || crate::delegation::configured_template(value).is_some_and(|child| repository(&child.repository))
}

fn checkpoint(value: &Checkpoint) -> bool {
    repository_contains_credentials(&value.repository)
        || repository_contains_credentials(&value.remote_ref)
}

pub(super) fn rejected(operation: &Operation) -> bool {
    match operation {
        Operation::RegisterRepository { registration, .. }
        | Operation::ReviseRepository { registration, .. } => {
            repository_contains_credentials(&registration.source_endpoint)
                || repository_contains_credentials(&registration.checkpoint_endpoint)
        }
        Operation::SubmitTaskDraft { input, .. } => {
            input.repository.as_ref().is_some_and(repository)
                || input.repository_read.as_ref().is_some_and(|source| repository_contains_credentials(&source.repository))
                || input.instruction_source.as_ref().is_some_and(|source| repository_contains_credentials(&source.repository))
                || matches!(input.native_workflow.as_deref(), Some(NativeWorkflow::CodingThenContinue { child } | NativeWorkflow::Reassign { target: child }) if repository(&child.repository))
        }
        Operation::SubmitTask { input, .. } => task(input),
        Operation::DelegateTask { child, .. } => task(child),
        Operation::PrepareOperation { kind, intended, .. } => {
            if kind == "checkpoint"
                && ["repository", "remote_ref"].iter().any(|field| {
                    intended
                        .get(*field)
                        .and_then(serde_json::Value::as_str)
                        .is_none()
                })
            {
                // A later SQL type refusal would retain the malformed field,
                // including any URL nested in it. Reject this known shape at
                // the same pre-dispatch boundary; opaque intents stay opaque.
                return true;
            }
            // The checkpoint producer supplies these named fields. Checking
            // them on other external intents does not invent a new schema.
            ["repository", "remote_ref"].iter().any(|field| {
                intended
                    .get(*field)
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(repository_contains_credentials)
            })
        }
        Operation::SettleOperation {
            checkpoint: value, ..
        } => value.as_ref().is_some_and(checkpoint),
        Operation::ObserveWorker { report } => match &report.observation {
            Observation::ResultCandidate {
                checkpoint: value, ..
            } => value.as_ref().is_some_and(checkpoint),
            _ => false,
        },
        Operation::PrepareRelease(value) => checkpoint(&value.checkpoint),
        _ => false,
    }
}
