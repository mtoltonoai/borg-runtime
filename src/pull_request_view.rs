//! Public copies of known PR-bearing records. Canonical worker inputs stay exact.
//! User text, schemas, tool arguments and arbitrary JSON are never field-filtered.
use crate::{protocol::*, pull_request as pr};
use serde_json::{Value, json};

pub(crate) fn input(value: &TaskInput) -> TaskInput {
    let mut value = value.clone();
    if let Some(repository) = &mut value.repository { repository.pull_request_target = None; }
    if let Some(NativeWorkflow::CodingThenContinue { child } | NativeWorkflow::Reassign { target: child }) = value.native_workflow.as_deref_mut() {
        child.repository.pull_request_target = None;
    }
    value
}
pub(crate) fn task(value: &Task) -> Task {
    let mut value = value.clone();
    value.input = Box::new(input(&value.input));
    value
}
pub(crate) fn work(value: &crate::collaboration::work::WorkMutationOutcome) -> crate::collaboration::work::WorkMutationOutcome {
    let mut value = value.clone();
    value.view.task = Box::new(task(&value.view.task));
    value
}
fn encode(value: &impl serde::Serialize) -> Result<Value, String> {
    serde_json::to_value(value).map_err(|_| "public record encoding failed".into())
}
fn decode<T: serde::de::DeserializeOwned>(value: &Value) -> Result<T, String> {
    serde_json::from_value(value.clone()).map_err(|_| "typed public record unavailable".into())
}
fn head(value: &pr::HeadIntent) -> Value {
    json!({"binding":value.binding,"target_revision":value.target.revision,
        "head_operation_id":value.head_operation_id,"head_ref":value.head_ref,
        "private_target_omitted":true})
}
fn head_proof(value: &pr::HeadProof) -> Value {
    json!({"operation_id":value.operation_id,"target_revision":value.target_revision,
        "remote_ref":value.remote_ref,"commit":value.commit,"private_remote_omitted":true})
}
fn intent(kind: &str, value: &Value) -> Result<Value, String> {
    match kind {
        pr::HEAD_KIND => Ok(head(&decode::<pr::HeadIntent>(value)?)),
        pr::CREATE_KIND => {
            let value: pr::Intent = decode(value)?;
            Ok(json!({"head":head(&value.head),"head_proof":head_proof(&value.head_proof),
                "title":value.title,"body":value.body,"marker":value.marker}))
        }
        _ => Ok(value.clone()),
    }
}
fn effect_outcome(kind: &str, value: &Value) -> Result<Value, String> {
    match kind {
        pr::HEAD_KIND => match decode::<pr::HeadOutcome>(value)? {
            pr::HeadOutcome::Applied(value) => Ok(json!({"acknowledged":value.acknowledged,"proof":head_proof(&value.proof)})),
            pr::HeadOutcome::NotApplied(value) => encode(&value),
        },
        pr::CREATE_KIND => encode(&decode::<pr::CreateOutcome>(value)?),
        _ => Ok(value.clone()),
    }
}
fn command(value: &mut Operation) -> Result<(), String> {
    match value {
        Operation::SubmitTaskDraft { input: value, .. } => {
            if let Some(repository)=&mut value.repository { repository.pull_request_target=None; }
            if let Some(NativeWorkflow::CodingThenContinue { child } | NativeWorkflow::Reassign { target: child })=value.native_workflow.as_deref_mut() { child.repository.pull_request_target=None; }
        }
        Operation::SubmitTask { input: value, .. } | Operation::DelegateTask { child: value, .. } => *value = Box::new(input(value)),
        Operation::RegisterRepository { registration, .. } | Operation::ReviseRepository { registration, .. } => {
            registration.pull_request_target = None;
        }
        Operation::PrepareOperation { kind, intended, .. } => *intended = intent(kind, intended)?,
        // This command has no effect-kind discriminator. Omit its opaque
        // payload rather than guess from user JSON keys. Its typed committed
        // external record below still exposes the original safe result.
        Operation::SettleOperation { outcome, .. } => {
            *outcome = json!({"opaque_effect_payload_omitted":true});
        }
        _ => {}
    }
    Ok(())
}
pub(crate) fn record(value: &OperationRecord) -> Result<OperationRecord, String> {
    let mut value = value.clone();
    if value.kind == "application_mutation" {
        let mut request: Operation = decode(&value.intended)?;
        command(&mut request)?;
        value.intended = encode(&request)?;
        if let Some(saved) = &value.outcome {
            let mut outcome: Outcome = decode(saved)?;
            if let Outcome::Ok { data: value } = &mut outcome { *value = data(value.clone())?; }
            value.outcome = Some(encode(&outcome)?);
        }
    } else {
        value.intended = intent(&value.kind, &value.intended)?;
        if let Some(saved) = &value.outcome { value.outcome = Some(effect_outcome(&value.kind, saved)?); }
    }
    Ok(value)
}
/// Call this before the collaboration outcome is committed, so original replay
/// and later call lookup expose the same public bytes and commit identity.
pub(crate) fn data(mut value: Data) -> Result<Data, String> {
    match &mut value {
        Data::Task(value) => **value = task(value),
        Data::Tasks(values) => { for value in values { *value = task(value); } }
        Data::WorkMutation(value) => *value = work(value),
        Data::WorkQuery(crate::collaboration::work::WorkQueryResult::Task(value)) => value.task = Box::new(task(&value.task)),
        Data::Commands(values) => {
            for value in values {
                if let WorkerCommand::RunAttempt { task: value, .. } = value { *value = Box::new(input(value)); }
            }
        }
        // Model call lookup can contain a previous typed outcome. Decode only
        // that closed receipt type; do not traverse user-owned arbitrary JSON.
        Data::Operation(value) => *value = record(value)?,
        _ => {}
    }
    Ok(value)
}
