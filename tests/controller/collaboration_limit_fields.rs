//! Complete-definition collaboration allowances retain their existing policy boundary.
#[path = "../identity_support.rs"]
mod support;

use borg_runtime::{collaboration, limits::*, mcp::Invocation, protocol::*};
use serde_json::{Value, json};
use support::*;

type Edit = fn(&mut CollaborationLimits);

fn edits() -> [(&'static str, Edit); 6] {
    [
        ("config_bytes", |v| v.config_bytes += 1),
        ("frame_bytes", |v| v.frame_bytes += 1),
        ("channel_items", |v| v.channel_items += 1),
        ("startup_timeout_ms", |v| v.startup_timeout_ms += 1),
        ("gate_bytes", |v| v.gate_bytes += 1),
        ("boundary_bytes", |v| v.boundary_bytes += 1),
    ]
}

fn replacement(before: &Definition, edit: impl FnOnce(&mut CollaborationLimits)) -> Definition {
    let mut next = before.clone();
    next.revision = id("collaboration-replacement");
    edit(
        next.limit_overrides
            .as_mut()
            .unwrap()
            .collaboration
            .as_mut()
            .unwrap(),
    );
    next
}

fn replace_args(h: &Harness, before: &Definition, next: &Definition) -> Value {
    admin_args(
        &h.cfg.limits,
        json!({"expected_revision":before.revision,"definition":next}),
    )
}

fn replace_call(h: &Harness, before: &Definition, next: &Definition) -> Invocation {
    invocation(
        &h.caller,
        "collab_definition_replace",
        replace_args(h, before, next),
    )
}

fn replaced(reply: &Reply) -> &IdentityAdministrationOutcome {
    match &reply.outcome {
        Outcome::Ok {
            data: Data::IdentityAdministration(outcome),
        } => outcome,
        other => panic!("expected complete-definition replacement: {other:?}"),
    }
}

async fn assert_immutable_definition(h: &Harness, definition: &Definition) {
    let reply = api(
        &h.c,
        Operation::QueryCollaboration {
            query: collaboration::Query::Definition {
                definition_id: definition.id.clone(),
                revision: definition.revision.clone(),
            },
        },
    )
    .await;
    assert!(
        matches!(ok(reply), Data::Collaboration(collaboration::QueryResult::Definition(d)) if &d == definition)
    );
}

async fn assert_refused_without_replacement(
    h: &Harness,
    before: &Definition,
    next: &Definition,
    args: Value,
    code: &str,
) {
    let call = invocation(&h.caller, "collab_definition_replace", args);
    let first = h.invoke(&call).await;
    assert_eq!(refused(first.clone()), code);
    let replay = h.invoke(&call).await;
    assert_eq!(replay.outcome, first.outcome);
    assert_eq!(replay.commit, first.commit);
    assert_eq!(selected(&h.c, &before.id).await.definition, *before);
    assert_immutable_definition(h, before).await;
    assert_eq!(
        refused(
            api(
                &h.c,
                Operation::QueryCollaboration {
                    query: collaboration::Query::Definition {
                        definition_id: next.id.clone(),
                        revision: next.revision.clone(),
                    },
                },
            )
            .await,
        ),
        "not_found"
    );
}

#[tokio::test]
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# A task MUST retain the definition revision accepted at submission.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# An accepted task MUST retain its resolved execution-limit values.
async fn collaboration_limit_fields_each_and_combined_replace_preserve_snapshots_and_originals() {
    let h = Harness::new(
        "collaboration-allowances",
        "charter",
        ExecutorKind::Native,
        true,
    )
    .await;
    let accepted = h.current().await;
    let mut history = Vec::new();
    let mut originals = Vec::new();
    let mut heads = Vec::new();
    for agent in ["charter", "triage"] {
        let mut before = selected(&h.c, agent).await.definition;
        let capabilities = before
            .limit_overrides
            .as_ref()
            .unwrap()
            .collaboration
            .as_ref()
            .unwrap()
            .capabilities
            .as_ref()
            .unwrap();
        assert!(capabilities.task_creation);
        assert_eq!(capabilities.identity_administration, agent == "charter");
        history.push(before.clone());
        let mut cases = edits().to_vec();
        // The production handoff changes these six allowances together with the
        // already-selected complete-result field, using one complete definition.
        cases.push(("combined", |v| {
            for (_, edit) in edits() {
                edit(v);
            }
            v.complete_result_bytes += 1;
        }));
        for (label, edit) in cases {
            let next = replacement(&before, edit);
            let prior = h.cfg.limits.resolve_definition(&before).unwrap();
            let mut expected = prior.clone();
            edit(&mut expected.collaboration);
            let call = replace_call(&h, &before, &next);
            let reply = h.invoke(&call).await;
            let outcome = replaced(&reply);
            assert_eq!(
                outcome.classification,
                ConfigurationChangeClass::Widening,
                "{label}"
            );
            assert_eq!(outcome.actor, bound_actor(&h));
            assert_eq!(
                outcome.policy,
                h.cfg.limits.identity_administration.clone().unwrap()
            );
            assert_eq!(outcome.global_revision, h.cfg.limits.revision);
            assert_eq!(outcome.governance_revision, h.cfg.limits.revision);
            assert_eq!(outcome.before.as_ref(), Some(&before));
            assert_eq!(outcome.before_limits.as_ref(), Some(&prior));
            assert_eq!(outcome.after_limits.as_ref(), Some(&expected));
            assert!(!outcome.manager_unusable_for_future_tasks);
            assert!(
                matches!(&outcome.effect, IdentityEffect::Replaced(agent) if agent.definition == next)
            );
            assert_eq!(selected(&h.c, &next.id).await.definition, next);
            assert_eq!(next.executor, before.executor);
            assert_eq!(next.inference, before.inference);
            assert_eq!(next.execution_profile, before.execution_profile);
            assert_eq!(next.charter, before.charter);
            let groups = next.limit_overrides.as_ref().unwrap();
            assert!(
                groups.native.is_none()
                    && groups.coding.is_none()
                    && groups.memory_notice.is_none()
            );
            assert_eq!(
                expected.collaboration.capabilities,
                prior.collaboration.capabilities
            );
            // Reversing only the changed group and revision reproduces the entire
            // predecessor, including explicit null groups and all unrelated fields.
            let mut restored = next.clone();
            restored.revision = before.revision.clone();
            restored.limit_overrides.as_mut().unwrap().collaboration = before
                .limit_overrides
                .as_ref()
                .unwrap()
                .collaboration
                .clone();
            assert_eq!(restored, before);
            assert_eq!(h.current().await.input, accepted.input);
            assert_eq!(h.current().await.status, accepted.status);
            let replay = h.invoke(&call).await;
            assert_eq!(replay.outcome, reply.outcome);
            assert_eq!(replay.commit, reply.commit);
            originals.push((call, reply));
            history.push(next.clone());
            before = next;
        }
        heads.push(before);
    }
    let cfg = h.cfg.clone();
    let h = h.reopen(cfg).await;
    for definition in &history {
        assert_immutable_definition(&h, definition).await;
    }
    for (call, original) in originals {
        let replay = h.invoke(&call).await;
        assert_eq!(replay.outcome, original.outcome);
        assert_eq!(replay.commit, original.commit);
        let mut changed = call;
        changed.arguments["definition"]["revision"] = json!(id("duplicate-content-conflict"));
        assert_eq!(refused(h.invoke(&changed).await), "operation_conflict");
    }
    for head in heads {
        assert_eq!(selected(&h.c, &head.id).await.definition, head);
    }
    assert_eq!(h.current().await.input, accepted.input);
    assert_eq!(h.current().await.status, accepted.status);
    h.close().await;
}

#[tokio::test]
async fn collaboration_limit_fields_complete_definition_policy_refuses_without_mutation() {
    let mut p = policy();
    p.identity_administration
        .as_mut()
        .unwrap()
        .complete_definitions = false;
    let h = Harness::with_policy(
        "collaboration-disabled",
        "charter",
        ExecutorKind::Native,
        true,
        p,
    )
    .await;
    let before = selected(&h.c, "charter").await.definition;
    let accepted = h.current().await.input;
    for (_, edit) in edits() {
        let next = replacement(&before, edit);
        assert_refused_without_replacement(
            &h,
            &before,
            &next,
            replace_args(&h, &before, &next),
            "configuration_policy",
        )
        .await;
    }
    assert_eq!(h.current().await.input, accepted);
    h.close().await;
}

#[tokio::test]
//= spec/impl/v2/implementation.md#configuration-changes-preserve-governance
//= type=test
//# A widening configuration change MUST be refused unless the authenticated invocation is trusted for that configured class of change.
async fn collaboration_limit_fields_current_widening_rule_refuses_without_mutation() {
    let h = Harness::new(
        "collaboration-widening-rule",
        "charter",
        ExecutorKind::Native,
        true,
    )
    .await;
    let mut subject = super::native("collaboration-widening-subject");
    subject.definition.execution_profile = Some(ExecutionProfile::CollaborationV2 {
        publication: None,
        web_search: None,
    });
    subject.definition.charter = Some(charter("collaboration-widening-subject"));
    let selection =
        super::governance::promote_with_subject(&h.c, "collaboration-widening", false, subject)
            .await
            .configuration;
    let before = selected(&h.c, "charter").await.definition;
    let accepted = h.current().await.input;
    for (_, edit) in edits() {
        let next = replacement(&before, edit);
        let mut args = replace_args(&h, &before, &next);
        args["expected_governance_revision"] = json!(selection.revision);
        assert_refused_without_replacement(
            &h,
            &before,
            &next,
            args,
            "governance_configuration_blocked",
        )
        .await;
    }
    assert_eq!(h.current().await.input, accepted);
    h.close().await;
}

#[tokio::test]
async fn collaboration_limit_fields_do_not_bypass_existing_selected_field_checks() {
    let mut p = policy();
    p.definition_updates
        .as_mut()
        .unwrap()
        .allowed_fields
        .retain(|field| *field != DefinitionLimitField::NativeContextBytes);
    let h = Harness::with_policy(
        "collaboration-excluded-existing",
        "charter",
        ExecutorKind::Native,
        true,
        p,
    )
    .await;
    let before = selected(&h.c, "charter").await.definition;
    let mut next = replacement(&before, |v| {
        for (_, edit) in edits() {
            edit(v);
        }
    });
    let mut native = h.cfg.limits.resolve_definition(&before).unwrap().native;
    native.context_bytes += 1;
    next.limit_overrides.as_mut().unwrap().native = Some(native);
    assert_refused_without_replacement(
        &h,
        &before,
        &next,
        replace_args(&h, &before, &next),
        "governance_configuration_blocked",
    )
    .await;
    h.close().await;
}

#[tokio::test]
async fn collaboration_limit_fields_do_not_classify_lookup_or_liveness_changes() {
    let h = Harness::new(
        "collaboration-unclassified",
        "charter",
        ExecutorKind::Native,
        true,
    )
    .await;
    let before = selected(&h.c, "charter").await.definition;
    let unrelated: [Edit; 2] = [
        |v| v.lookup_backoff_ms += 1,
        |v| v.liveness_interval_ms += 1,
    ];
    for edit in unrelated {
        let next = replacement(&before, |v| {
            for (_, allowance) in edits() {
                allowance(v);
            }
            edit(v);
        });
        assert_refused_without_replacement(
            &h,
            &before,
            &next,
            replace_args(&h, &before, &next),
            "governance_configuration_blocked",
        )
        .await;
    }
    h.close().await;
}

#[tokio::test]
async fn collaboration_limit_fields_replacement_retains_all_four_revision_checks() {
    let h = Harness::new(
        "collaboration-revisions",
        "charter",
        ExecutorKind::Native,
        true,
    )
    .await;
    let before = selected(&h.c, "charter").await.definition;
    for key in [
        "expected_revision",
        "expected_global_revision",
        "expected_policy_revision",
        "expected_governance_revision",
    ] {
        let next = replacement(&before, |v| v.config_bytes += 1);
        let mut args = replace_args(&h, &before, &next);
        args[key] = json!("stale");
        assert_refused_without_replacement(&h, &before, &next, args, "configuration_conflict")
            .await;
    }
    h.close().await;
}
