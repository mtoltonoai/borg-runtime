//! Operator-only source publication and future-task head selection. No task is
//! manufactured to store source; session adoption remains a separate mutation.
use super::{charters, database::*, governance, metadata};
use crate::{charter::*, governance::*, protocol::*};
use rusqlite::{OptionalExtension, params};

fn bad(message: &str) -> Fault {
    Fault::Store(message.into())
}
fn identity(tx: &StoreContext<'_>, value: &str) -> Result<()> {
    if valid_id(tx.limits, value) {
        Ok(())
    } else {
        refuse("invalid_input", "invalid charter definition identity")
    }
}
fn reference(d: &Definition) -> CharterSourceRef {
    CharterSourceRef {
        definition_id: d.id.clone(),
        definition_revision: d.revision.clone(),
    }
}
fn source(tx: &StoreContext<'_>, d: &Definition) -> Result<Option<CharterSourceRef>> {
    let l = tx
        .limits
        .charter
        .as_ref()
        .ok_or_else(|| Fault::Refused("not_configured", "charter policy is absent".into()))?;
    d.charter
        .as_ref()
        .map(|_| charters::resolve(tx, &reference(d), l).map(|v| v.reference))
        .transpose()
}
fn head(tx: &StoreContext<'_>, id: &str) -> Result<Definition> {
    identity(tx, id)?;
    let raw: Option<String> = tx
        .query_row(
            "SELECT CAST(substr(CAST(definition AS BLOB),1,?2) AS TEXT) FROM agents WHERE id=?1",
            params![
                id,
                sql_bound(tx.limits.transport.body_bytes.saturating_add(1))?
            ],
            |r| r.get(0),
        )
        .optional()?;
    let raw = raw.ok_or_else(|| Fault::Refused("not_found", "agent not found".into()))?;
    if raw.len() > tx.limits.transport.body_bytes {
        return Err(bad("agent definition exceeds body bound"));
    }
    let d: Definition = decode(&raw)?;
    if d.id != id || charters::definition(tx, &reference(&d))? != d {
        return Err(bad("selected definition differs from immutable source"));
    }
    Ok(d)
}
fn operands(input: &CharterMutation) -> Result<(&str, &str, &str, &str, CharterDefinitionEffect)> {
    match input {
        CharterMutation::Publish {
            agent_id,
            expected_definition_revision,
            expected_global_revision,
            expected_governance_revision,
            ..
        } => Ok((
            agent_id,
            expected_definition_revision,
            expected_global_revision,
            expected_governance_revision,
            CharterDefinitionEffect::PublicationOnly,
        )),
        CharterMutation::SelectForFutureTasks {
            agent_id,
            expected_definition_revision,
            expected_global_revision,
            expected_governance_revision,
            ..
        } => Ok((
            agent_id,
            expected_definition_revision,
            expected_global_revision,
            expected_governance_revision,
            CharterDefinitionEffect::FutureSubmissions,
        )),
        _ => Err(bad("not a charter definition mutation")),
    }
}
fn candidate(
    tx: &StoreContext<'_>,
    input: &CharterMutation,
    before: &Definition,
    revision: &str,
) -> Result<(Definition, CharterSourceRef)> {
    let l = tx
        .limits
        .charter
        .as_ref()
        .ok_or_else(|| Fault::Refused("not_configured", "charter policy is absent".into()))?;
    let mut d = before.clone();
    d.revision = revision.into();
    let target = match input {
        CharterMutation::Publish {
            charter: DefinitionCharter::Inline { source },
            ..
        } => {
            d.charter = Some(DefinitionCharter::Inline {
                source: source.clone(),
            });
            reference(&d)
        }
        CharterMutation::Publish {
            charter: DefinitionCharter::Reference { source },
            ..
        }
        | CharterMutation::SelectForFutureTasks { source, .. } => {
            let original = charters::resolve(tx, source, l)?.reference;
            d.charter = Some(DefinitionCharter::Reference {
                source: original.clone(),
            });
            original
        }
        _ => return Err(bad("not a charter definition mutation")),
    };
    d.required_inference().map_err(|code| {
        Fault::Refused(code, "selected definition inference is unavailable".into())
    })?;
    if d.execution_profile.is_none() {
        return refuse(
            "template_profile_unbound",
            "selected definition has no collaboration profile",
        );
    }
    charters::validate_definition(tx, &d)?;
    let before_limits = tx
        .limits
        .resolve_definition(before)
        .map_err(|e| Fault::Refused("invalid_limits", e))?;
    let after_limits = tx
        .limits
        .resolve_definition(&d)
        .map_err(|e| Fault::Refused("invalid_limits", e))?;
    let mut restored = d.clone();
    restored.revision = before.revision.clone();
    restored.charter = before.charter.clone();
    if restored != *before || before_limits != after_limits {
        return Err(bad(
            "charter change altered non-charter definition or effective limits",
        ));
    }
    super::bounded_json_size(&d, tx.limits.transport.body_bytes).map_err(|_| {
        Fault::Refused(
            "input_bound",
            "definition exceeds transport.body_bytes".into(),
        )
    })?;
    Ok((d, target))
}
fn decision(
    tx: &StoreContext<'_>,
    before: &Definition,
    after: &Definition,
    target: CharterSourceRef,
    effect: CharterDefinitionEffect,
) -> Result<CharterConfigurationDecision> {
    let prior = source(tx, before)?;
    let change = if prior.as_ref() == Some(&target) {
        EffectiveChange::Neutral
    } else {
        EffectiveChange::Widening
    };
    Ok(CharterConfigurationDecision {
        before: reference(before),
        proposed: reference(after),
        source_before: prior,
        source_after: target,
        effect,
        comparison: ComparisonRule::ConservativeWidening,
        change,
    })
}
fn revision(tx: &StoreContext<'_>, id: &str) -> Result<String> {
    // Existing receipt/task capacities bound prior authored collision entries.
    let cap = tx
        .limits
        .controller
        .max_receipts
        .checked_add(tx.limits.controller.max_cleanup_receipts)
        .and_then(|v| v.checked_add(tx.limits.controller.max_tasks))
        .ok_or_else(|| bad("definition revision scan bound overflow"))?;
    for _ in 0..=cap {
        let value = format!("charter-definition-{}", next(tx, "definition_update")?);
        identity(tx, &value)?;
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM definitions WHERE id=?1 AND revision=?2)",
            params![id, value],
            |r| r.get(0),
        )?;
        if !exists {
            return Ok(value);
        }
    }
    refuse(
        "capacity",
        "definition revision collision scan exceeds existing receipt/task capacities",
    )
}
pub(super) fn apply(
    ctx: &charters::Context<'_, '_>,
    input: &CharterMutation,
) -> Result<CharterOutcome> {
    let tx = ctx.tx;
    let actor = super::identity_administration::authorize(tx)?;
    let selected = governance::selection(tx)?;
    let (agent, expected, global, governance_revision, effect) = operands(input)?;
    for id in [
        agent,
        expected,
        global,
        governance_revision,
        ctx.operation_id,
    ] {
        identity(tx, id)?;
    }
    if global != tx.limits.revision || governance_revision != selected.revision {
        return refuse(
            "configuration_conflict",
            "current global or governance revision differs",
        );
    }
    super::agents::require_active(tx, agent)?;
    let before = head(tx, agent)?;
    if before.revision != expected {
        return refuse(
            "configuration_conflict",
            "selected definition revision differs",
        );
    }
    let fresh = revision(tx, agent)?;
    let (definition, target) = candidate(tx, input, &before, &fresh)?;
    let audit = decision(tx, &before, &definition, target.clone(), effect)?;
    governance::stage_charter(tx, ctx.operation_id, actor, audit)?;
    let result = CharterOutcome::Definition(CharterDefinitionOutcome {
        previous_revision: before.revision.clone(),
        definition: definition.clone(),
        source: target,
        effect,
    });
    // The exact proposed reply and original-operation lookup must fit before
    // rows are applied. An ordinary refusal retains only the staged proposal.
    metadata::receipt_bound(
        tx.limits,
        &Operation::MutateCharter {
            operation_id: ctx.operation_id.into(),
            mutation: input.clone(),
        },
        &Outcome::Ok {
            data: Data::Charter(result.clone()),
        },
    )?;
    let encoded = encode(&definition)?;
    tx.execute(
        "INSERT INTO definitions(id,revision,body) VALUES(?1,?2,?3)",
        params![agent, fresh, encoded],
    )?;
    if effect == CharterDefinitionEffect::FutureSubmissions {
        let n = tx.execute(
            "UPDATE agents SET definition=?2 WHERE id=?1 AND definition=?3",
            params![agent, encoded, encode(&before)?],
        )?;
        if n != 1 {
            return Err(bad(
                "selected charter definition changed inside owner transaction",
            ));
        }
    }
    Ok(result)
}
/// Historical evidence uses the original captured metadata supplied by the
/// governance reader. A mandatory refusal retains a proposal, not applied rows.
pub(super) fn validate_decision(
    tx: &StoreContext<'_>,
    record: &DecisionRecord,
    audit: &CharterConfigurationDecision,
    request: &Operation,
    outcome: &Outcome,
) -> Result<bool> {
    let projected=super::identity_administration::project(metadata::Recorded{request:request.clone(),outcome:outcome.clone(),commit:MutationCommit{operation_id:record.id.operation_id.clone(),controller_revision:record.controller_revision}})?;
    let request=&projected.request; let outcome=&projected.outcome;
    let Operation::MutateCharter { mutation, .. } = request else {
        return Ok(false);
    };
    let (agent, expected, global, governance_revision, effect) = operands(mutation)?;
    if agent != audit.before.definition_id
        || expected != audit.before.definition_revision
        || global != record.configuration.global_revision
        || governance_revision != record.configuration.governance_revision
        || global != tx.limits.revision
        || record.configuration.accepted_global_revision.is_some()
        || audit.effect != effect
        || audit.proposed.definition_id != agent
    {
        return Ok(false);
    }
    let selected = governance::read_configuration(tx, governance_revision)?;
    if selected.global_revision != global
        || selected.definition_update_policy_revision
            != record.configuration.definition_update_policy_revision
    {
        return Ok(false);
    }
    let before = charters::definition(tx, &audit.before)?;
    let (after, target) = candidate(tx, mutation, &before, &audit.proposed.definition_revision)?;
    if decision(tx, &before, &after, target.clone(), effect)? != *audit {
        return Ok(false);
    }
    match outcome {
        Outcome::Ok {
            data: Data::Charter(CharterOutcome::Definition(value)),
        } => Ok(value
            == &CharterDefinitionOutcome {
                previous_revision: expected.into(),
                definition: after.clone(),
                source: target,
                effect,
            }
            && charters::definition(tx, &reference(&after))? == after),
        Outcome::Refused { .. } => Ok(true),
        _ => Ok(false),
    }
}
/// Reverse membership prevents deleting the new definition while keeping the
/// successful original receipt. Audit lookup validates original metadata/body.
pub(super) fn validate_membership(
    tx: &StoreContext<'_>,
    original: &metadata::Recorded,
) -> Result<()> {
    let record = governance::read_decision(
        tx,
        &DecisionRef {
            operation_id: original.commit.operation_id.clone(),
            point: DecisionPoint::CharterConfiguration,
        },
    )?;
    if !matches!(record.actual_outcome, ActualDecisionOutcome::Applied) {
        return Err(bad(
            "successful charter definition lacks its original audit",
        ));
    }
    Ok(())
}

/// Selection history includes later limit-only successors. Publication cannot
/// hide a rolled-back head, and a later genuine head update is not corruption.
pub(super) fn validate_selected_heads(tx: &StoreContext<'_>) -> Result<()> {
    let cap = tx
        .limits
        .controller
        .max_receipts
        .checked_add(tx.limits.controller.max_cleanup_receipts)
        .ok_or_else(|| bad("receipt scan overflow"))?;
    let mut q=tx.prepare("SELECT CAST(substr(CAST(id AS BLOB),1,?1) AS TEXT) FROM receipts ORDER BY controller_revision LIMIT ?2")?;
    let mut rows = q.query(params![
        sql_bound(tx.limits.transport.identity_bytes.saturating_add(1))?,
        sql_bound(cap.saturating_add(1))?
    ])?;
    let mut latest = std::collections::BTreeMap::new();
    let mut charter_agents = std::collections::BTreeSet::new();
    let mut count = 0usize;
    while let Some(row) = rows.next()? {
        count += 1;
        if count > cap {
            return Err(bad("receipt inventory exceeds configured bound"));
        }
        let id: String = row.get(0)?;
        identity(tx, &id)?;
        let original = super::identity_administration::project(charters::receipt(tx, &id)?)?;
        let (before, after, charter) = match original.outcome {
            Outcome::Ok {
                data: Data::Charter(CharterOutcome::Definition(value)),
            } => {
                let before = CharterSourceRef {
                    definition_id: value.definition.id.clone(),
                    definition_revision: value.previous_revision,
                };
                let after = if value.effect == CharterDefinitionEffect::FutureSubmissions {
                    reference(&value.definition)
                } else {
                    before.clone()
                };
                (before, after, true)
            }
            Outcome::Ok {
                data: Data::DefinitionUpdate(value),
            } => {
                let before = CharterSourceRef {
                    definition_id: value.selected_definition.id.clone(),
                    definition_revision: value.previous_revision,
                };
                (before, reference(&value.selected_definition), false)
            }
            Outcome::Ok { data:Data::IdentityAdministration(value) } => {
                if let IdentityEffect::Replaced(agent)=&value.effect {
                    let prior=value.before.as_ref().ok_or_else(||bad("replacement predecessor absent"))?;
                    (reference(prior),reference(&agent.definition),true)
                } else {continue}
            }
            _ => continue,
        };
        if latest
            .get(&before.definition_id)
            .is_some_and(|prior| prior != &before)
        {
            return Err(bad(
                "definition transition predecessor differs from original selected head",
            ));
        }
        if charter {
            charter_agents.insert(before.definition_id.clone());
        }
        latest.insert(before.definition_id, after);
        // At most one head per successfully scanned original receipt.
        if latest.len() > cap {
            return Err(bad(
                "selected definition inventory exceeds enclosing receipt bound",
            ));
        }
    }
    for agent in charter_agents {
        let expected = latest
            .get(&agent)
            .ok_or_else(|| bad("charter selected head absent"))?;
        if reference(&head(tx, &agent)?) != *expected {
            return Err(bad(
                "selected charter head differs from latest original committed selection",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn charter_operator_boundary_and_old_wire_refuse_before_any_effect() {
        let p = std::env::temp_dir().join(format!(
            "charter-boundary-{}-{}",
            std::process::id(),
            crate::worker::process::now_ms()
        ));
        let mut limits = crate::limits::RuntimeLimits::seed().unwrap();
        limits.charter = Some(crate::limits::CharterLimits {
            record_bytes: 65536,
            source_bytes: 16384,
            files: 4,
            clauses: 32,
            metadata_bytes: 8192,
            references: 32,
            reply_items: 32,
            tool_calls: 8,
            reference_depth: 4,
            records: 1024,
            page_items: 4,
            page_bytes: 65536,
        });
        limits.governance = Some(crate::limits::GovernanceLimits {
            record_bytes: 65536,
            reason_items: 16,
            reason_bytes: 4096,
            scan_rows: 512,
            rule_versions: 16,
            configuration_versions: 16,
            shadow_evidence_items: 16,
            completion_continuations: 2,
            override_max_ms: 10000,
        });
        let c = super::super::Controller::open_with_config(
            &p,
            super::super::ControllerConfig {
                limits: std::sync::Arc::new(limits),
                ..Default::default()
            },
        )
        .unwrap();
        let command = Operation::MutateCharter {
            operation_id: "operator-only".into(),
            mutation: CharterMutation::SelectForFutureTasks {
                agent_id: "missing".into(),
                expected_definition_revision: "1".into(),
                expected_global_revision: c.limits().revision.clone(),
                expected_governance_revision: c.limits().revision.clone(),
                source: CharterSourceRef {
                    definition_id: "source".into(),
                    definition_revision: "1".into(),
                },
            },
        };
        let request = Request {
            protocol_revision: WIRE_REVISION,
            request_id: "browser".into(),
            command: command.clone(),
        };
        let refused = Box::pin(c.handle_browser(request.clone())).await;
        assert!(
            matches!(refused.outcome,Outcome::Refused{ref code,..} if code=="governance_operator_required")
        );
        let mut old_command = command.clone();
        if let Operation::MutateCharter { operation_id, .. } = &mut old_command {
            *operation_id = "old-wire".into();
        }
        let old = Box::pin(c.handle(Request {
            protocol_revision: 16,
            request_id: "old-reader".into(),
            command: old_command,
        }))
        .await;
        assert!(matches!(old.outcome,Outcome::Refused{ref code,..} if code=="unsupported_version"));
        assert!(old.commit.is_none());
        let absent = Box::pin(c.handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: "lookup".into(),
            command: Operation::GetOperation {
                operation_id: "old-wire".into(),
            },
        }))
        .await;
        assert!(matches!(absent.outcome,Outcome::Refused{ref code,..} if code=="not_found"));
        // Publication/selection are not part of either model-writable catalogue.
        for profile in [
            ExecutionProfile::CollaborationV1 { web_search: None },
            ExecutionProfile::CollaborationV2 { web_search: None },
        ] {
            let catalogue = crate::mcp::catalogue_for_profile(c.limits(), Some(&profile));
            assert!(!catalogue.iter().any(|tool| tool.name.contains("charter")));
        }
    }
}
