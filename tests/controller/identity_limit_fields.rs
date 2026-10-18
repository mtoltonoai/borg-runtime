//! Eight-oracle field follow-up, using canonical operations and real SQLite.
#[path = "../identity_limit_fields_support.rs"]
mod support;
use support::*;
use borg_runtime::{controller::Controller, governance::*, limits::*, mcp, protocol::*};
use serde_json::{Value, json};
use std::sync::{Arc, atomic::Ordering};

async fn fixture(label: &str) -> Harness {
    Harness::with_policy(label, "charter", ExecutorKind::Native, true, policy()).await
}

async fn replace(h: &Harness, before: &Definition, next: Definition) -> Reply {
    h.call("collab_definition_replace", admin_args(&h.cfg.limits,
        json!({"expected_revision":before.revision,"definition":next}))).await
}

#[tokio::test]
async fn identity_limit_fields_numeric_directions_commit_and_retain_exact_comparison() {
    let h = fixture("directions").await;
    let original = h.current().await;
    let mut before = selected(&h.c, "charter").await.definition;
    for (delta, expected) in [(1isize, ConfigurationChangeClass::Widening), (-2, ConfigurationChangeClass::Narrowing), (0, ConfigurationChangeClass::Neutral)] {
        let prior = h.cfg.limits.resolve_definition(&before).unwrap();
        let edits: Vec<_> = FIELDS.iter().map(|&f| (f, f.value(&prior).unwrap().checked_add_signed(delta).unwrap())).collect();
        let call = invocation(&h.caller, "collab_definition_update", numeric_args(&h.cfg.limits, &before, changes(&edits)));
        let reply = h.invoke(&call).await;
        let outcome = updated(&reply);
        assert_eq!(outcome.classification, expected);
        assert_eq!(outcome.changed_fields, FIELDS);
        assert_eq!(outcome.previous_revision, before.revision);
        assert_eq!(outcome.policy, h.cfg.limits.definition_updates.clone().unwrap());
        assert_eq!(outcome.effective_for, ConfigurationEffect::FutureSubmissions);
        let mut effective = prior.clone();
        for (f, v) in edits { f.set(&mut effective, v); }
        assert_eq!(h.cfg.limits.resolve_definition(&outcome.selected_definition).unwrap(), effective);
        assert_retry_and_context(&effective);
        assert_eq!(outcome.selected_definition.executor, before.executor);
        assert_eq!(outcome.selected_definition.inference, before.inference);
        assert_eq!(outcome.selected_definition.execution_profile, before.execution_profile);
        assert_eq!(effective.collaboration.capabilities, prior.collaboration.capabilities);
        let evidence = audit(&h.c, &call.operation_id).await;
        assert_eq!(evidence.actor, bound_actor(&h));
        assert_eq!(evidence.controller_revision, reply.commit.as_ref().unwrap().controller_revision);
        assert_eq!(evidence.configuration_metadata.as_deref(), Some(h.cfg.limits.as_ref()));
        let DecisionDetail::DefinitionLimits(d) = evidence.detail else { panic!("comparison") };
        assert!(d.allowed);
        assert_eq!(d.comparisons.len(), 2);
        for comparison in d.comparisons {
            assert_eq!(comparison.before, comparison.field.value(&prior));
            assert_eq!(Some(comparison.after), comparison.field.value(&effective));
            assert_eq!(comparison.rule, ComparisonRule::ResourceAllowance);
            assert_eq!(comparison.change, match expected {
                ConfigurationChangeClass::Widening => EffectiveChange::Widening,
                ConfigurationChangeClass::Narrowing => EffectiveChange::Narrowing,
                _ => EffectiveChange::Neutral,
            });
        }
        let replay = h.invoke(&call).await;
        assert_eq!(replay.outcome, reply.outcome);
        assert_eq!(replay.commit, reply.commit);
        before = outcome.selected_definition;
    }
    assert_eq!(h.current().await.input, original.input);
    assert_eq!(h.current().await.status, original.status);
    h.close().await;
}

#[tokio::test]
async fn identity_limit_fields_complete_replacement_both_numeric_directions_stay_widening() {
    let h = fixture("replace-directions").await;
    let original = inherited(&h.c, &h.cfg.limits, "complete-values").await;
    let mut before = original.clone();
    for field in FIELDS {
        for delta in [1isize, -2] {
            let prior = h.cfg.limits.resolve_definition(&before).unwrap();
            let value = field.value(&prior).unwrap().checked_add_signed(delta).unwrap();
            let next = replacement(&h.cfg.limits, &before, &[(field, value)]);
            let reply = replace(&h, &before, next.clone()).await;
            let outcome = replaced(&reply);
            assert_eq!(outcome.classification, ConfigurationChangeClass::Widening);
            assert_eq!(outcome.before, Some(before));
            assert_eq!(outcome.before_limits, Some(prior.clone()));
            let mut expected = prior;
            field.set(&mut expected, value);
            assert_eq!(outcome.after_limits, Some(expected.clone()));
            assert_retry_and_context(&expected);
            assert!(matches!(outcome.effect, IdentityEffect::Replaced(ref a) if a.definition == next));
            assert_eq!(selected(&h.c, &next.id).await.definition, next);
            before = next;
        }
    }
    let old = api(&h.c, Operation::QueryCollaboration { query: borg_runtime::collaboration::Query::Definition {
        definition_id: original.id.clone(), revision: original.revision.clone()
    }}).await;
    assert!(matches!(ok(old), Data::Collaboration(borg_runtime::collaboration::QueryResult::Definition(d)) if d == original));
    let cfg = h.cfg.clone(); let h = h.reopen(cfg).await;
    assert_eq!(selected(&h.c, &before.id).await.definition, before);
    h.close().await;
}

#[tokio::test]
async fn identity_limit_fields_unselected_and_unknown_changes_refuse() {
    let h = Harness::new("unselected-fields", "charter", ExecutorKind::Native, true).await;
    let before = selected(&h.c, "charter").await.definition;
    let effective = h.cfg.limits.resolve_definition(&before).unwrap();
    for field in FIELDS {
        for delta in [-1isize, 0, 1] {
            let value = field.value(&effective).unwrap().checked_add_signed(delta).unwrap();
            assert_eq!(refused(h.call("collab_definition_update", numeric_args(&h.cfg.limits, &before, changes(&[(field, value)]))).await), "configuration_policy");
        }
        let next = replacement(&h.cfg.limits, &before, &[(field, field.value(&effective).unwrap() + 1)]);
        assert_eq!(refused(replace(&h, &before, next).await), "governance_configuration_blocked");
    }
    for fields in [json!({"coding.stderr_bytes":1}), json!({"collaboration.frame_bytes":1}), json!({"unknown":1})] {
        assert_eq!(refused(h.call("collab_definition_update", numeric_args(&h.cfg.limits, &before, fields)).await), "invalid_input");
    }
    let mut next = before.clone(); next.revision = id("unknown-change");
    next.limit_overrides.as_mut().unwrap().coding = Some(effective.coding.clone());
    next.limit_overrides.as_mut().unwrap().coding.as_mut().unwrap().stderr_bytes -= 1;
    assert_eq!(refused(replace(&h, &before, next).await), "governance_configuration_blocked");
    assert_eq!(selected(&h.c, "charter").await.definition, before);
    h.close().await;
}

#[tokio::test]
async fn identity_limit_fields_touched_groups_inherit_current_retry_and_preserve_old_snapshots() {
    let mut old = policy();
    old.revision = "identity-limit-fields-old-task-max2".into();
    old.task_defaults.model_requests = 2;
    old.execution.coding.rate_limit_retry = None;
    let h = Harness::with_policy("whole-group", "charter", ExecutorKind::Native, true, old).await;
    let before = inherited(&h.c, &h.cfg.limits, "inherited-groups").await;
    let accepted = h.current().await.input;
    assert!(accepted.execution_limits().unwrap().coding.rate_limit_retry.is_none());
    let mut cfg = h.cfg.clone();
    let current = Arc::make_mut(&mut cfg.limits);
    current.revision = "identity-limit-fields-current-max".into();
    current.task_defaults.model_requests = u32::MAX;
    current.execution.coding.rate_limit_retry = policy().execution.coding.rate_limit_retry;
    current.execution.coding.pending_notifications += 1;
    current.execution.collaboration.lookup_backoff_ms += 1;
    current.validate().unwrap();
    let h = h.reopen(cfg).await;
    let prior = h.cfg.limits.resolve_definition(&before).unwrap();
    assert_retry_and_context(&prior);
    let edits = [(FIELDS[0], prior.coding.frame_bytes + 1), (FIELDS[1], prior.collaboration.journal_bytes + 1)];
    let result = h.call("collab_definition_update", numeric_args(&h.cfg.limits, &before, changes(&edits))).await;
    let updated = updated(&result);
    let mut expected = prior.clone();
    for (field, value) in edits { field.set(&mut expected, value); }
    assert_eq!(h.cfg.limits.resolve_definition(&updated.selected_definition).unwrap(), expected);
    let groups = updated.selected_definition.limit_overrides.as_ref().unwrap();
    assert_eq!(groups.coding.as_ref(), Some(&expected.coding));
    assert_eq!(groups.collaboration.as_ref(), Some(&expected.collaboration));
    assert!(groups.native.is_none() && groups.memory_notice.is_none());
    assert_eq!(h.current().await.input, accepted);
    // Replacing complete groups is also built from today's resolved values.
    let candidate = replacement(&h.cfg.limits, &updated.selected_definition, &[(FIELDS[0], prior.coding.frame_bytes), (FIELDS[1], prior.collaboration.journal_bytes)]);
    let outcome = replaced(&replace(&h, &updated.selected_definition, candidate).await);
    assert_eq!(outcome.after_limits, Some(prior));
    assert_eq!(h.current().await.input, accepted);
    h.close().await;
}

#[tokio::test]
async fn identity_limit_fields_full_replace_still_requires_widening_rule_for_decrease() {
    let h = fixture("conservative-rule").await;
    let mut subject = super::native("identity-limit-fields-narrowing-subject");
    subject.definition.execution_profile = Some(ExecutionProfile::CollaborationV2 { web_search: None, publication: None });
    subject.definition.charter = Some(charter("identity-limit-fields-narrowing-subject"));
    let selection = super::governance::promote_with_subject(&h.c, "identity-limit-fields-narrowing", false, subject).await.configuration;
    let before = selected(&h.c, "charter").await.definition;
    let values = h.cfg.limits.resolve_definition(&before).unwrap();
    let edits = [(FIELDS[0], values.coding.frame_bytes - 1), (FIELDS[1], values.collaboration.journal_bytes - 1)];
    let mut args = admin_args(&h.cfg.limits, json!({"expected_revision":before.revision,"definition":replacement(&h.cfg.limits, &before, &edits)}));
    args["expected_governance_revision"] = json!(selection.revision);
    assert_eq!(refused(h.call("collab_definition_replace", args).await), "governance_configuration_blocked");
    assert_eq!(selected(&h.c, "charter").await.definition, before);
    let reply = h.call("collab_definition_update", numeric_args(&h.cfg.limits, &before, changes(&edits))).await;
    let decrease = updated(&reply);
    assert_eq!(decrease.classification, ConfigurationChangeClass::Narrowing);
    let increase = h.call("collab_definition_update", numeric_args(&h.cfg.limits, &decrease.selected_definition,
        changes(&[(FIELDS[0], values.coding.frame_bytes), (FIELDS[1], values.collaboration.journal_bytes)]))).await;
    assert_eq!(refused(increase), "governance_configuration_blocked");
    h.close().await;
}

#[tokio::test]
async fn identity_limit_fields_cas_manager_provenance_replay_and_real_commit_rollback() {
    let h = fixture("cas-rollback").await;
    let before = selected(&h.c, "charter").await.definition;
    let value = h.cfg.limits.resolve_definition(&before).unwrap().coding.frame_bytes + 1;
    let args = numeric_args(&h.cfg.limits, &before, changes(&[(FIELDS[0], value)]));
    for key in ["expected_revision", "expected_global_revision", "expected_policy_revision"] {
        let mut stale = args.clone(); stale[key] = json!("stale");
        assert_eq!(refused(h.call("collab_definition_update", stale).await), "configuration_conflict");
    }
    let next = replacement(&h.cfg.limits, &before, &[(FIELDS[0], value)]);
    let base = admin_args(&h.cfg.limits, json!({"expected_revision":before.revision,"definition":next}));
    for key in ["expected_revision", "expected_global_revision", "expected_policy_revision", "expected_governance_revision"] {
        let mut stale = base.clone(); stale[key] = json!("stale");
        assert_eq!(refused(h.call("collab_definition_replace", stale).await), "configuration_conflict");
    }
    let mut forged = invocation(&h.caller, "collab_definition_update", args.clone());
    forged.caller.generation += 1;
    assert!(matches!(h.invoke(&forged).await.outcome, Outcome::Refused { .. }));
    let call = invocation(&h.caller, "collab_definition_update", args);
    h.fail.store(true, Ordering::SeqCst);
    let failed = h.invoke(&call).await;
    assert_eq!(refused(failed), "store_unavailable");
    assert_eq!(selected(&h.c, "charter").await.definition, before);
    assert_eq!(refused(api(&h.c, Operation::GetOperation { operation_id: call.operation_id.clone() }).await), "not_found");
    let result = h.invoke(&call).await;
    let changed = updated(&result);
    let conflict = invocation(&h.caller, "collab_definition_update", call.arguments.clone());
    let refusal = h.invoke(&conflict).await;
    assert_eq!(refused(refusal.clone()), "configuration_conflict");
    let mut reused = call.clone(); reused.arguments["changes"]["coding.frame_bytes"] = json!(value + 1);
    assert_eq!(refused(h.invoke(&reused).await), "operation_conflict");
    // Retire the old current manager authority via explicit fixture metadata.
    let mut cfg = h.cfg.clone();
    let p = Arc::make_mut(&mut cfg.limits);
    p.revision = "identity-limit-fields-reconfigured-manager".into();
    p.identity_administration.as_mut().unwrap().manager_agent_id = "triage".into();
    p.identity_administration.as_mut().unwrap().revision = "manager-policy-2".into();
    let h = h.reopen(cfg).await;
    for (original, expected) in [(&call, &result), (&conflict, &refusal)] {
        let replay = h.invoke(original).await;
        assert_eq!(replay.outcome, expected.outcome); assert_eq!(replay.commit, expected.commit);
    }
    assert_eq!(refused(h.call("collab_definition_update", numeric_args(&h.cfg.limits, &changed.selected_definition,
        changes(&[(FIELDS[0], value - 1)]))).await), "identity_manager_required");
    assert_eq!(selected(&h.c, "charter").await.definition, changed.selected_definition);
    h.close().await;
}

#[tokio::test]
async fn identity_limit_fields_complete_validation_and_checked_output_extent_without_allocation() {
    let h = fixture("checked-extents").await;
    let before = inherited(&h.c, &h.cfg.limits, "extent-coding").await;
    let values = h.cfg.limits.resolve_definition(&before).unwrap();
    for (field, value) in [(FIELDS[0], 0), (FIELDS[1], 0), (FIELDS[0], usize::MAX),
        (FIELDS[1], usize::MAX), (FIELDS[1], values.collaboration.complete_result_bytes), (FIELDS[0], isize::MAX as usize)] {
        let result = h.call("collab_definition_update", numeric_args(&h.cfg.limits, &before, changes(&[(field, value)]))).await;
        let Outcome::Refused { code, message } = result.outcome else { panic!("invalid complete extent admitted") };
        assert!(matches!(code.as_str(), "invalid_input" | "invalid_limits"), "{code}: {message}");
        if field == FIELDS[0] && value == isize::MAX as usize { assert!(message.contains("supplied original output extent overflow"), "{message}"); }
        assert_eq!(refused(replace(&h, &before, replacement(&h.cfg.limits, &before, &[(field, value)])).await), "invalid_limits");
    }
    // Full replacement still validates unrelated cross-field invariants.
    let mut invalid = replacement(&h.cfg.limits, &before, &[(FIELDS[0], values.coding.frame_bytes + 1)]);
    let mut collaboration = values.collaboration.clone();
    collaboration.boundary_bytes = collaboration.frame_bytes + 1;
    invalid.limit_overrides.as_mut().unwrap().collaboration = Some(collaboration);
    assert_eq!(refused(replace(&h, &before, invalid).await), "invalid_limits");
    assert_eq!(selected(&h.c, &before.id).await.definition, before);
    let big = u32::MAX as usize + 1;
    let result = h.call("collab_definition_update", numeric_args(&h.cfg.limits, &before,
        changes(&[(FIELDS[0], big), (FIELDS[1], big)]))).await;
    let selected = updated(&result).selected_definition;
    let effective = h.cfg.limits.resolve_definition(&selected).unwrap();
    assert_eq!(effective.coding.frame_bytes, big);
    assert_eq!(effective.collaboration.journal_bytes, big);
    assert_retry_and_context(&effective);
    // Only small definition/receipt JSON was created, not a buffer of this size.
    assert!(serde_json::to_vec(&result).unwrap().len() < 16_384);
    h.close().await;
}
