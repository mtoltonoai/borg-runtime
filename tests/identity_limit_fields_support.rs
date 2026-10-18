//! Explicit fixture selection, never deployed metadata or identity enrollment.
#![allow(dead_code)]
#[path = "identity_support.rs"]
mod support;
pub use support::*;
use borg_runtime::{controller::Controller, governance::*, limits::*, protocol::*};
use serde_json::{Value, json};

pub const FIELDS: [DefinitionLimitField; 2] = [DefinitionLimitField::CodingFrameBytes, DefinitionLimitField::CollaborationJournalBytes];

pub fn policy() -> RuntimeLimits {
    let mut p = support::policy();
    p.revision = "identity-limit-fields-fixture-1".into();
    // This task explicitly qualifies inheritance of the deployed four-field
    // policy. The shipped seed and historical fixtures remain unchanged.
    p.execution.coding.rate_limit_retry = Some(borg_runtime::supplied_outer::RetryPolicy {
        initial_backoff_ms: 3000, backoff_multiplier: 2, max_backoff_ms: 60000, jitter_ms: 1000,
    });
    let updates = p.definition_updates.as_mut().unwrap();
    updates.revision = "identity-limit-fields-policy-1".into();
    updates.allowed_fields.extend(FIELDS);
    p.validate().unwrap();
    p
}

pub fn numeric_args(p: &RuntimeLimits, d: &Definition, changes: Value) -> Value {
    json!({"agent_id":d.id,"expected_revision":d.revision,"expected_global_revision":p.revision,
        "expected_policy_revision":p.definition_updates.as_ref().unwrap().revision,"changes":changes})
}

pub fn changes(fields: &[(DefinitionLimitField, usize)]) -> Value {
    serde_json::to_value(fields.iter().copied().collect::<std::collections::BTreeMap<_, _>>()).unwrap()
}

pub fn updated(r: &Reply) -> DefinitionUpdateOutcome {
    match &r.outcome { Outcome::Ok { data: Data::DefinitionUpdate(v) } => (**v).clone(), other => panic!("update outcome: {other:?}") }
}

pub fn replaced(r: &Reply) -> IdentityAdministrationOutcome {
    match &r.outcome { Outcome::Ok { data: Data::IdentityAdministration(v) } => (**v).clone(), other => panic!("replacement outcome: {other:?}") }
}

pub fn replacement(p: &RuntimeLimits, before: &Definition, fields: &[(DefinitionLimitField, usize)]) -> Definition {
    let mut after = p.resolve_definition(before).unwrap();
    for (field, value) in fields { field.set(&mut after, *value); }
    let mut d = before.clone();
    d.revision = id("replacement");
    let overrides = d.limit_overrides.get_or_insert_with(Default::default);
    for (field, _) in fields { field.materialize(overrides, &after); }
    d
}

pub async fn inherited(c: &Controller, p: &RuntimeLimits, name: &str) -> Definition {
    let mut d = definition(p, name, ExecutorKind::Coding, false);
    d.limit_overrides = None;
    ok(api(c, Operation::RegisterAgent { operation_id: id("register-inherited"), definition: d.clone() }).await);
    d
}

pub async fn audit(c: &Controller, operation_id: &str) -> DecisionRecord {
    match ok(api(c, Operation::QueryGovernance { query: GovernanceQuery::Decision {
        decision: DecisionRef { operation_id: operation_id.into(), point: DecisionPoint::DefinitionLimits }
    }}).await) {
        Data::Governance(v) => match *v { GovernanceResult::Decision(d) => d, other => panic!("decision: {other:?}") },
        other => panic!("governance result: {other:?}"),
    }
}

pub fn assert_retry_and_context(values: &ExecutionLimits) {
    assert_eq!(values.coding.model_context_window, Some(1_050_000));
    let retry = values.coding.rate_limit_retry.as_ref().expect("complete inherited retry policy");
    assert_eq!((retry.initial_backoff_ms, retry.backoff_multiplier, retry.max_backoff_ms, retry.jitter_ms), (3000, 2, 60000, 1000));
}
