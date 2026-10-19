//! Actual bound-agent configuration calls and canonical controller guards.
//! The current update contract has no ancestor-composition or safety-mode field.
//! Unknown-field refusals below prove that closed contract, not an inheritance
//! engine or a runtime decision on an otherwise representable governance edit.
use super::*;
use borg_runtime::collaboration::workflow::{ActorProvenance, PrincipalRef};
use borg_runtime::governance::*;
use borg_runtime::governance_completion::{
    self, CompletionBoundary, CompletionOutcome, EvaluateCompletion,
};
use borg_runtime::mcp::{Caller, Invocation};

async fn original(c: &Controller, operation: Operation) -> Reply {
    governance::governance_reply(c, operation).await
}

async fn query(c: &Controller, query: GovernanceQuery) -> GovernanceResult {
    match call(c, Operation::QueryGovernance { query }).await {
        Outcome::Ok {
            data: Data::Governance(value),
        } => *value,
        other => panic!("expected governance query: {other:?}"),
    }
}

async fn selected(c: &Controller, agent_id: &str) -> Definition {
    match call(
        c,
        Operation::GetAgent {
            agent_id: agent_id.into(),
        },
    )
    .await
    {
        Outcome::Ok {
            data: Data::Agent(value),
        } => value.definition,
        other => panic!("expected selected definition: {other:?}"),
    }
}

async fn setup(
    label: &str,
    allow_widening: bool,
) -> (Controller, Task, Attempt, Caller, ConfigurationVersion) {
    let c = Controller::open_with_config(path(), governance::governance_config()).unwrap();
    let promotion = governance::promote(&c, &format!("{label}-policy"), allow_widening).await;
    let mut input = native(label);
    input.definition.execution_profile =
        Some(ExecutionProfile::CollaborationV2 { publication: None, web_search: None });
    let queued = submit(&c, &format!("{label}-submit"), input).await;
    let worker = worker(&format!("{label}-worker"), "original-incarnation", 1, 0);
    register(&c, &worker).await;
    let issued = commands(&c, &worker, vec![]).await;
    let attempt = run(&issued);
    assert_eq!(attempt.task_id, queued.id);
    let acknowledged = issued
        .iter()
        .filter_map(|value| match value {
            WorkerCommand::RunAttempt { command_id, .. } => Some(command_id.clone()),
            _ => None,
        })
        .collect();
    assert!(
        commands(&c, &worker, acknowledged)
            .await
            .iter()
            .all(|value| !matches!(value, WorkerCommand::RunAttempt { .. }))
    );
    let task = task(
        report(
            &c,
            &attempt,
            &format!("{label}-started"),
            Observation::Started {
                thread_id: None,
                turn_id: None,
            },
        )
        .await,
    );
    let caller = Caller {
        task_id: task.id.clone(),
        attempt_id: attempt.id.clone(),
        generation: attempt.generation,
        worker_id: attempt.worker_id.clone(),
        incarnation: attempt.worker_incarnation.clone(),
    };
    (c, task, attempt, caller, promotion.configuration)
}

fn arguments(c: &Controller, definition: &Definition, context_bytes: usize) -> serde_json::Value {
    json!({"agent_id": definition.id, "expected_revision": definition.revision,
        "expected_global_revision": c.limits().revision,
        "expected_policy_revision": c.limits().definition_updates.as_ref().unwrap().revision,
        "changes": {"native.context_bytes": context_bytes}})
}

fn agent_update(caller: &Caller, id: &str, arguments: serde_json::Value) -> Operation {
    Invocation {
        operation_id: id.into(),
        logical_call_id: format!("logical-{id}"),
        caller: caller.clone(),
        tool: "collab_definition_update".into(),
        arguments,
    }
    .operation()
}

fn changed(reply: &Reply) -> &DefinitionUpdateOutcome {
    match &reply.outcome {
        Outcome::Ok {
            data: Data::DefinitionUpdate(value),
        } => value,
        other => panic!("expected permitted limit update: {other:?}"),
    }
}

async fn assert_selection(c: &Controller, configuration: &ConfigurationVersion) {
    assert_eq!(
        query(c, GovernanceQuery::State).await,
        GovernanceResult::State(GovernanceState {
            selected_revision: configuration.revision.clone(),
            temporary_override: None,
        })
    );
    assert_eq!(
        query(
            c,
            GovernanceQuery::Configuration {
                revision: configuration.revision.clone()
            }
        )
        .await,
        GovernanceResult::Configuration(configuration.clone())
    );
}

#[tokio::test]
//= spec/impl/v2/implementation.md#configuration-changes-preserve-governance
//= type=test
//# An agent-originated configuration change MUST NOT weaken governance applicable to its session, definition or ancestor definition.
// Scoped to the current closed definition-limit operation and global selection.
// Ancestor composition is absent; its unknown-field refusal is not an ancestry proof.
async fn agent_update_cannot_replace_selected_governance_or_escape_enforced_comparison() {
    let (c, task_before, _attempt, caller, configuration) = setup("agent-rule", false).await;
    let definition = task_before.input.definition.clone();
    let accepted = serde_json::to_vec(&task_before.input).unwrap();
    let context = c
        .limits()
        .resolve_definition(&definition)
        .unwrap()
        .native
        .context_bytes;
    let mut invalid = Vec::new();
    for (field, value) in [
        (
            "governance",
            json!({"selected_revision":"unqualified-agent-choice"}),
        ),
        (
            "definition_update_policy",
            json!({"allow_configured_widening":true}),
        ),
        (
            "ancestor_definition",
            json!({"id":"claimed-ancestor","governance":null}),
        ),
    ] {
        let mut args = arguments(&c, &definition, context);
        args[field] = value;
        let operation = agent_update(&caller, &format!("agent-closed-{field}"), args);
        let refused = original(&c, operation.clone()).await;
        assert_eq!(code(refused.outcome.clone()), "invalid_input", "{field}");
        assert_eq!(selected(&c, &definition.id).await, definition);
        assert_eq!(get(&c, &task_before.id).await, task_before);
        assert_selection(&c, &configuration).await;
        invalid.push((operation, refused));
    }
    // This request IS in the supported wire vocabulary and configured field
    // class. Its refusal must come from actual selected-rule enforcement.
    let widening = agent_update(
        &caller,
        "agent-real-widening",
        arguments(&c, &definition, context + 1),
    );
    let refused = original(&c, widening.clone()).await;
    assert_eq!(
        code(refused.outcome.clone()),
        "governance_configuration_blocked"
    );
    assert!(refused.commit.is_some());
    assert_eq!(selected(&c, &definition.id).await, definition);
    let GovernanceResult::Decision(audit) = query(
        &c,
        GovernanceQuery::Decision {
            decision: DecisionRef {
                operation_id: "agent-real-widening".into(),
                point: DecisionPoint::DefinitionLimits,
            },
        },
    )
    .await
    else {
        panic!("expected enforcing comparison audit")
    };
    assert_eq!(audit.mode, DecisionMode::Enforcing);
    assert_eq!(audit.rule, configuration.definition_limits);
    assert_eq!(
        audit.configuration.governance_revision,
        configuration.revision
    );
    assert_eq!(
        audit.actual_outcome,
        ActualDecisionOutcome::GovernanceBlocked
    );
    assert_eq!(
        audit.actor.principal,
        PrincipalRef::Agent {
            agent_id: definition.id.clone()
        }
    );
    assert_eq!(
        audit.actor.provenance,
        ActorProvenance::BoundAgentSession {
            agent_id: definition.id.clone(),
            session_id: task_before.session_id.clone(),
            task_id: caller.task_id.clone(),
            attempt_id: caller.attempt_id.clone(),
            generation: caller.generation,
            worker_id: caller.worker_id.clone(),
            incarnation: caller.incarnation.clone(),
        }
    );
    let DecisionDetail::DefinitionLimits(comparison) = &audit.detail else {
        panic!("expected definition decision")
    };
    assert!(!comparison.allowed);
    assert_eq!(
        comparison.reason,
        ConfigurationReason::WideningNotConfigured
    );
    assert_eq!(comparison.comparisons.len(), 1);
    assert_eq!(comparison.comparisons[0].before, Some(context));
    assert_eq!(comparison.comparisons[0].after, context + 1);
    assert_eq!(comparison.comparisons[0].change, EffectiveChange::Widening);

    // An unchanged permitted value really commits through this same agent path.
    let permitted = original(
        &c,
        agent_update(
            &caller,
            "agent-neutral-control",
            arguments(&c, &definition, context),
        ),
    )
    .await;
    let change = changed(&permitted);
    assert!(permitted.commit.is_some());
    assert_eq!(change.classification, ConfigurationChangeClass::Neutral);
    assert_eq!(change.effective_for, ConfigurationEffect::FutureSubmissions);
    assert_ne!(change.selected_definition.revision, definition.revision);
    assert_eq!(
        selected(&c, &definition.id).await,
        change.selected_definition
    );
    assert_eq!(
        serde_json::to_vec(&get(&c, &task_before.id).await.input).unwrap(),
        accepted
    );
    assert_selection(&c, &configuration).await;
    for (operation, first) in invalid
        .into_iter()
        .chain(std::iter::once((widening, refused)))
    {
        let replay = original(&c, operation).await;
        assert_eq!(replay.outcome, first.outcome);
        assert_eq!(replay.commit, first.commit);
    }
    assert_eq!(
        query(
            &c,
            GovernanceQuery::Decision {
                decision: audit.id.clone()
            }
        )
        .await,
        GovernanceResult::Decision(audit)
    );
}

async fn mandatory_validation_stays_enforcing(mode: &str) {
    let (c, initial, attempt, caller, configuration) = setup(mode, true).await;
    let definition = initial.input.definition.clone();
    let accepted = serde_json::to_vec(&initial.input).unwrap();
    let context = c
        .limits()
        .resolve_definition(&definition)
        .unwrap()
        .native
        .context_bytes;
    let value = if mode == "disable" {
        json!({"enabled":false})
    } else {
        json!({"mode":"shadow"})
    };
    let mut top_level = arguments(&c, &definition, context + 1);
    top_level["governance"] = json!({"mandatory_validation":value});
    let mut field_patch = arguments(&c, &definition, context + 1);
    field_patch["changes"] = if mode == "disable" {
        json!({"governance.mandatory_validation.enabled":0})
    } else {
        json!({"governance.mandatory_validation.shadow":1})
    };
    let mut refusals = Vec::new();
    for (shape, args) in [("top", top_level), ("field", field_patch)] {
        let operation = agent_update(&caller, &format!("{mode}-{shape}"), args);
        let refused = original(&c, operation.clone()).await;
        assert_eq!(code(refused.outcome.clone()), "invalid_input");
        assert_eq!(selected(&c, &definition.id).await, definition);
        assert_eq!(get(&c, &initial.id).await, initial);
        assert_selection(&c, &configuration).await;
        refusals.push((operation, refused));
    }
    // The ordinary rule allows this real widening. The safety check must still
    // enforce its original accepted schema; the task has no hot-reloaded input.
    let permitted = original(
        &c,
        agent_update(
            &caller,
            &format!("{mode}-permitted"),
            arguments(&c, &definition, context + 1),
        ),
    )
    .await;
    let change = changed(&permitted);
    assert_eq!(change.classification, ConfigurationChangeClass::Widening);
    assert_eq!(change.effective_for, ConfigurationEffect::FutureSubmissions);
    assert_eq!(
        selected(&c, &definition.id).await,
        change.selected_definition
    );
    assert_eq!(
        serde_json::to_vec(&get(&c, &initial.id).await.input).unwrap(),
        accepted
    );
    assert_selection(&c, &configuration).await;

    governance::completed_native_request(&c, &attempt, None, None).await;
    complete_native_loop(&c, &attempt).await;
    let boundary = CompletionBoundary::NativeResponse { request_index: 1 };
    let admitted = call(
        &c,
        Operation::EvaluateCompletion(EvaluateCompletion {
            operation_id: governance_completion::evaluation_id(&caller, &boundary).unwrap(),
            caller,
            boundary,
            intervening_requests: vec![],
        }),
    )
    .await;
    assert!(
        matches!(&admitted, Outcome::Ok { data: Data::Completion(completion) }
        if matches!(completion.as_ref(), CompletionOutcome::Evaluated(value)
            if value.state.disposition == CompletionDisposition::Admit)),
        "{admitted:?}"
    );
    task(
        report(
            &c,
            &attempt,
            &format!("{mode}-executor"),
            Observation::ExecutorOutcome {
                success: true,
                summary: "controlled completion".into(),
            },
        )
        .await,
    );
    task(
        report(
            &c,
            &attempt,
            &format!("{mode}-stopped"),
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    let before = get(&c, &initial.id).await;
    let candidate = |id: &str, output| Operation::ObserveWorker {
        report: Report {
            report_id: id.into(),
            worker_id: attempt.worker_id.clone(),
            incarnation: attempt.worker_incarnation.clone(),
            task_id: attempt.task_id.clone(),
            attempt_id: attempt.id.clone(),
            generation: attempt.generation,
            observation: Observation::ResultCandidate {
                checks_passed: true,
                summary: "same candidate".into(),
                checkpoint: None,
                validation: vec![borg_runtime::native_loop_contract::evidence(&attempt.id)],
                output: Some(output),
            },
        },
    };
    let bad = candidate(
        &format!("{mode}-bad-schema"),
        json!({"answer":"not an integer"}),
    );
    let refused = original(&c, bad.clone()).await;
    assert_eq!(code(refused.outcome.clone()), "schema_validation");
    assert_eq!(get(&c, &initial.id).await, before);
    assert!(before.result.is_none());
    let valid = task(
        call(
            &c,
            candidate(&format!("{mode}-valid-schema"), json!({"answer":42})),
        )
        .await,
    );
    assert_eq!(valid.status, TaskStatus::Succeeded);
    assert_eq!(
        valid.result.as_ref().unwrap().output,
        Some(json!({"answer":42}))
    );
    assert_selection(&c, &configuration).await;
    for (operation, first) in refusals.into_iter().chain(std::iter::once((bad, refused))) {
        let replay = original(&c, operation).await;
        assert_eq!(replay.outcome, first.outcome);
        assert_eq!(replay.commit, first.commit);
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#decision-rules-have-versions-and-evidence
//= type=test
//# A mandatory safety check MUST NOT be disabled or converted to shadow evaluation by governance configuration.
// Disable is not a supported update field; actual schema rejection remains mandatory.
async fn agent_update_cannot_disable_mandatory_validation() {
    mandatory_validation_stays_enforcing("disable").await;
}

#[tokio::test]
//= spec/impl/v2/implementation.md#decision-rules-have-versions-and-evidence
//= type=test
//# A mandatory safety check MUST NOT be disabled or converted to shadow evaluation by governance configuration.
// Shadow is not a supported update mode; actual schema rejection remains mandatory.
async fn agent_update_cannot_shadow_mandatory_validation() {
    mandatory_validation_stays_enforcing("shadow").await;
}
