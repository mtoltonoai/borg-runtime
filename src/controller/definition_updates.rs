//! A closed change class in the existing collaboration/receipt transaction.
//! No independent proposal store, global hot reload or authority mutation.
use super::{ControllerConfig, database::*};
use crate::{limits::ExecutionLimitOverrides, protocol::*};
use rusqlite::{OptionalExtension, params};

//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//# A definition-limit change MUST compare the expected selected definition revision before applying a new immutable revision.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//# A definition-limit change MUST compare the expected active global metadata and change-policy revisions before application.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//# A definition-limit change MUST be restricted to the fields allowed by the configured change policy.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//# A definition-limit change MUST validate the complete resulting effective configuration before commit.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//# A permitted definition-limit change MUST atomically commit its immutable definition, selected head and durable operation outcome.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//# A definition-limit change MUST preserve the executor, inference settings and tool capabilities of its predecessor.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//# A configuration change outcome MUST distinguish future-task selection from settings retained by already accepted tasks.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//# A definition-limit change MUST NOT grant host administration, credentials or protected release authority.
//= spec/impl/v2/implementation.md#configuration-changes-preserve-governance
//# A governed configuration change MUST compare its effective before and after values composed with the same active defaults.
//= spec/impl/v2/implementation.md#configuration-changes-preserve-governance
//# An agent-originated configuration change MUST NOT weaken governance applicable to its session, definition or ancestor definition.
//= spec/impl/v2/implementation.md#decision-rules-have-versions-and-evidence
//# A mandatory safety check MUST NOT be disabled or converted to shadow evaluation by governance configuration.
pub(super) fn apply(tx: &StoreContext<'_>, input: &DefinitionUpdate, cfg: &ControllerConfig, operation_id: &str, actor: &crate::collaboration::workflow::RecordedActor) -> Result<(Data, bool)> {
    // This is today's controller policy, never the caller's accepted historical
    // defaults. The outer collaboration handler retains that caller's fences,
    // deadline and complete-response limits through finalization.
    let current = cfg.limits.as_ref();
    current.validate().map_err(|reason| Fault::Store(reason))?;
    for identity in [&input.agent_id, &input.expected_revision, &input.expected_global_revision, &input.expected_policy_revision] {
        if !valid_id(tx.limits, identity) { return refuse("invalid_input", "invalid definition-update identity: transport.identity_bytes"); }
    }
    if input.expected_global_revision != current.revision {
        return refuse("configuration_conflict", "active global metadata revision differs");
    }
    let Some(policy) = current.definition_updates.as_ref() else {
        return refuse("configuration_disabled", "definition_updates is not configured");
    };
    if input.expected_policy_revision != policy.revision {
        return refuse("configuration_conflict", "active definition_updates.revision differs");
    }
    if input.changes.is_empty() { return refuse("invalid_input", "definition change is empty"); }
    for (field, value) in &input.changes {
        if !policy.allowed_fields.contains(field) {
            return refuse("configuration_policy", format!("field {} is outside definition_updates.allowed_fields", encode(field)?));
        }
        if *value == 0 { return refuse("invalid_limits", "definition resource allowances must be positive"); }
    }
    super::agents::require_active(tx, &input.agent_id)?;
    let prior: Option<String> = tx.query_row("SELECT definition FROM agents WHERE id=?1", [&input.agent_id], |r|r.get(0)).optional()?;
    let Some(prior) = prior else { return refuse("not_found", "agent not found"); };
    let original: Definition = decode(&prior)?;
    if original.id != input.agent_id { return Err(Fault::Store("agent definition identity differs".into())); }
    if original.revision != input.expected_revision {
        return refuse("configuration_conflict", "selected definition revision differs");
    }
    original.required_inference().map_err(|code| Fault::Refused(code, "selected definition inference is unavailable".into()))?;
    if original.execution_profile.is_none() { return refuse("template_profile_unbound", "selected definition has no collaboration profile"); }
    let before = current.resolve_definition(&original).map_err(|reason| Fault::Refused("invalid_limits", reason))?;
    let mut after = before.clone();
    let mut decreased = false;
    let mut increased = false;
    let mut previous_unspecified = false;
    for (field, value) in &input.changes {
        if let Some(previous) = field.value(&before) {
            decreased |= *value < previous;
            increased |= *value > previous;
        } else {
            previous_unspecified = true;
        }
        field.set(&mut after, *value);
    }
    // An unknown executor default cannot be assigned a numeric comparison.
    // This class does not deny possible known widening in other changed fields.
    let classification = if previous_unspecified { ConfigurationChangeClass::PreviousUnspecified } else if increased { ConfigurationChangeClass::Widening } else if decreased { ConfigurationChangeClass::Narrowing } else { ConfigurationChangeClass::Neutral };
    let mut definition = original.clone();
    // A limit-only successor reuses complete authoritative charter source.
    // This changes no effective charter and grants no charter update authority.
    let charter_tx = tx.with_limits(current);
    if original.charter.is_some() {
        let limits = current.charter.as_ref().ok_or_else(|| Fault::Refused("not_configured", "charter reader policy is required".into()))?;
        let source = super::charters::resolve(&charter_tx, &crate::charter::CharterSourceRef {
            definition_id: original.id.clone(), definition_revision: original.revision.clone(),
        }, limits)?;
        definition.charter = Some(crate::charter::DefinitionCharter::Reference { source: source.reference });
    }
    super::charters::validate_definition(&charter_tx, &definition)?;
    let mut overrides = definition.limit_overrides.take().map(|v|*v).unwrap_or_else(ExecutionLimitOverrides::default);
    for field in input.changes.keys() { field.materialize(&mut overrides, &after); }
    definition.limit_overrides = Some(Box::new(overrides));
    let actual = current.resolve_definition(&definition).map_err(|reason| Fault::Refused("invalid_limits", reason))?;
    if actual != after { return Err(Fault::Store("definition patch changed an unrequested effective value".into())); }
    // Caller-authored task revisions may already use the generated spelling.
    // Skip those finite existing entries rather than reusing their identity.
    definition.revision = loop {
        let candidate = format!("definition-{}", next(tx, "definition_update")?);
        if !valid_id(tx.limits, &candidate) { return refuse("invalid_limits", "generated revision exceeds transport.identity_bytes"); }
        let exists: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM definitions WHERE id=?1 AND revision=?2)", params![definition.id, candidate], |r|r.get(0))?;
        if !exists { break candidate; }
    };
    if current.governance.is_some() {
        let decision=super::governance::compare_definition(tx,operation_id,actor,&original.id,&original.revision,
            Some(definition.revision.clone()),&before,&after,&input.changes)?;
        // Bootstrap records the exact effective comparison without replacing any
        // existing check. Only the qualified selection adds this enforcement.
        if !decision.allowed&&super::governance::completion_enforced(tx)? {
            return refuse("governance_configuration_blocked","selected governance rule refused the effective configuration change");
        }
    }
    let body = encode(&definition)?;
    tx.execute("INSERT INTO definitions(id,revision,body) VALUES(?1,?2,?3)", params![definition.id, definition.revision, body])?;
    let updated = tx.execute("UPDATE agents SET definition=?2 WHERE id=?1 AND definition=?3", params![definition.id, body, prior])?;
    if updated != 1 { return Err(Fault::Store("selected definition changed inside owned transaction".into())); }
    // The existing Database::process appends the original request/outcome and
    // commits this transaction only after all caller/response checks succeed.
    Ok((Data::DefinitionUpdate(Box::new(DefinitionUpdateOutcome {
        previous_revision: original.revision,
        selected_definition: definition,
        global_revision: current.revision.clone(),
        policy: policy.clone(),
        changed_fields: input.changes.keys().copied().collect(),
        classification,
        effective_for: ConfigurationEffect::FutureSubmissions,
    })), true))
}
