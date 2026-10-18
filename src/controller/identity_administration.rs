//! Exclusive bound-agent identity administration in the canonical transaction.
//! Host/SSH access to OperatorApi remains trusted; this is not host isolation.
use super::{ControllerConfig, database::*};
use crate::{
    collaboration::workflow::{ActorProvenance, PrincipalRef, RecordedActor},
    mcp::Caller,
    protocol::*,
};
use rusqlite::{OptionalExtension, params};

pub(super) fn bound_actor(task: &Task, caller: &Caller) -> RecordedActor {
    RecordedActor {
        principal: PrincipalRef::Agent {
            agent_id: task.assigned_agent_id.clone(),
        },
        provenance: ActorProvenance::BoundAgentSession {
            agent_id: task.assigned_agent_id.clone(),
            session_id: task.session_id.clone(),
            task_id: caller.task_id.clone(),
            attempt_id: caller.attempt_id.clone(),
            generation: caller.generation,
            worker_id: caller.worker_id.clone(),
            incarnation: caller.incarnation.clone(),
        },
    }
}
pub(super) fn authorize_actor(tx: &StoreContext<'_>, actor: &RecordedActor) -> Result<()> {
    if actor.principal == PrincipalRef::TrustedHostOperator && tx.bound_actor.is_none() {
        if super::governance::operator(tx)? != *actor {
            return refuse("identity_manager_required", "operator provenance differs");
        }
        return Ok(());
    }
    if tx.bound_actor.as_ref() != Some(actor) {
        return refuse(
            "identity_manager_required",
            "identity administration requires the canonical bound actor",
        );
    }
    let root = tx.governance_limits;
    let p = root
        .identity_administration
        .as_ref()
        .filter(|p| p.enabled)
        .ok_or_else(|| {
            Fault::Refused(
                "identity_manager_required",
                "identity administration selection is disabled or missing".into(),
            )
        })?;
    if actor.principal
        != (PrincipalRef::Agent {
            agent_id: p.manager_agent_id.clone(),
        })
    {
        return refuse(
            "identity_manager_required",
            "only the current configured bound manager may administer identities",
        );
    }
    let manager = super::agents::selected_active_definition(tx, &p.manager_agent_id)?;
    let usable = matches!(
        manager.execution_profile,
        Some(ExecutionProfile::CollaborationV2 { .. })
    ) && root
        .resolve_definition(&manager)
        .map_err(|e| Fault::Refused("invalid_limits", e))?
        .collaboration
        .capabilities
        .as_ref()
        .is_some_and(|c| c.identity_administration);
    if !usable {
        return refuse(
            "identity_manager_required",
            "selected manager has no usable administration profile; trusted operator recovery required",
        );
    }
    // Validate the recorded session/definition/attempt lineage, not a claimed name.
    super::collaboration_workflow::validate_recorded_actor(tx, actor)?;
    Ok(())
}
pub(super) fn authorize(tx: &StoreContext<'_>) -> Result<RecordedActor> {
    let actor = match &tx.bound_actor {
        Some(actor) => actor.clone(),
        None => super::governance::operator(tx)?,
    };
    authorize_actor(tx, &actor)?;
    Ok(actor)
}
/// Fresh agent work cannot introduce identities or immutable revisions. An
/// already accepted exact template is a historical input, never operator work.
pub(super) fn admit_definition(
    tx: &StoreContext<'_>,
    definition: &Definition,
    accepted: Option<&TaskInput>,
) -> Result<()> {
    if let Some(parent) = accepted {
        if crate::delegation::configured_template(parent)
            .is_some_and(|t| t.definition == *definition)
        {
            return Ok(());
        }
        return refuse(
            "definition_provenance",
            "definition differs from its preaccepted template",
        );
    }
    if tx.bound_actor.is_none() {
        return Ok(());
    }
    let known:Option<String>=tx.query_row("SELECT d.body FROM definitions d JOIN agents a ON a.id=d.id WHERE d.id=?1 AND d.revision=?2",params![definition.id,definition.revision],|r|r.get(0)).optional()?;
    match known {
        Some(body) if decode::<Definition>(&body)? == *definition => Ok(()),
        _ => refuse(
            "identity_manager_required",
            "register or revise the identity through the configured manager mechanism before fresh admission",
        ),
    }
}
fn validate_candidate(
    tx: &StoreContext<'_>,
    d: &Definition,
) -> Result<crate::limits::ExecutionLimits> {
    if !valid_id(tx.limits, &d.id) || !valid_id(tx.limits, &d.revision) {
        return refuse("invalid_input", "invalid definition identity");
    }
    super::operations::require_definition(d)?;
    crate::execution_profile::validate_definition(d)
        .map_err(|e| Fault::Refused("invalid_input", e))?;
    let effective = tx
        .limits
        .resolve_definition(d)
        .map_err(|e| Fault::Refused("invalid_limits", e))?;
    super::definition_updates::validate_supplied_output(tx.limits, d, &effective)?;
    if effective.collaboration.capabilities.is_some()
        && !matches!(
            d.execution_profile,
            Some(ExecutionProfile::CollaborationV2 { .. })
        )
    {
        return refuse(
            "invalid_input",
            "new collaboration capabilities require an explicit V2 profile",
        );
    }
    super::charters::validate_definition(tx, d)?;
    super::bounded_json_size(d, tx.limits.controller.max_task_input_bytes).map_err(|_| {
        Fault::Refused(
            "capacity",
            "definition exceeds controller.max_task_input_bytes".into(),
        )
    })?;
    Ok(effective)
}
pub(super) fn apply(
    tx: &StoreContext<'_>,
    operation_id: &str,
    input: &IdentityAdministrationRequest,
    cfg: &ControllerConfig,
    now: u64,
) -> Result<(Data, bool)> {
    let actor = authorize(tx)?;
    let current = tx.governance_limits;
    let policy = current.identity_administration.as_ref().ok_or_else(|| {
        Fault::Refused(
            "configuration_disabled",
            "identity_administration is absent".into(),
        )
    })?;
    let selection = super::governance::selection(tx)?;
    if input.expected_global_revision != current.revision
        || input.expected_policy_revision != policy.revision
        || input.expected_governance_revision != selection.revision
    {
        return refuse(
            "configuration_conflict",
            "current global, identity policy or governance revision differs",
        );
    }
    let tx = tx.with_limits(current);
    let mut before = None;
    let mut before_limits = None;
    let mut after_limits = None;
    let mut classification = ConfigurationChangeClass::Neutral;
    let effect = match &input.mutation {
        IdentityMutation::Register { definition } => {
            if !policy.complete_definitions {
                return refuse(
                    "configuration_policy",
                    "complete identity definitions are disabled",
                );
            }
            after_limits = Some(validate_candidate(&tx, definition)?);
            classification = ConfigurationChangeClass::Widening;
            require_widening(&tx)?;
            super::agents::register(&tx, definition, cfg)?;
            IdentityEffect::Registered(super::agents::agent(&tx, &definition.id)?)
        }
        IdentityMutation::Retire {
            agent_id,
            expected_revision,
        } => {
            before = Some(super::agents::selected_active_definition(&tx, agent_id)?);
            before_limits = Some(
                current
                    .resolve_definition(before.as_ref().unwrap())
                    .map_err(|e| Fault::Refused("invalid_limits", e))?,
            );
            let (Data::Agent(agent), _) = super::agents::apply(
                &tx,
                &Operation::RetireAgent {
                    operation_id: operation_id.into(),
                    agent_id: agent_id.clone(),
                    expected_revision: expected_revision.clone(),
                },
                cfg,
                now,
            )?
            else {
                return Err(Fault::Store("retirement outcome differs".into()));
            };
            classification = ConfigurationChangeClass::Narrowing;
            IdentityEffect::Retired(agent)
        }
        IdentityMutation::Replace {
            definition,
            expected_revision,
        } => {
            if !policy.complete_definitions {
                return refuse(
                    "configuration_policy",
                    "complete identity definitions are disabled",
                );
            }
            let old = super::agents::selected_active_definition(&tx, &definition.id)?;
            if &old.revision != expected_revision {
                return refuse(
                    "configuration_conflict",
                    "selected definition revision differs",
                );
            }
            let exists: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM definitions WHERE id=?1 AND revision=?2)",
                params![definition.id, definition.revision],
                |r| r.get(0),
            )?;
            if exists {
                return refuse(
                    "definition_conflict",
                    "replacement requires a fresh immutable definition revision",
                );
            }
            before_limits = Some(
                current
                    .resolve_definition(&old)
                    .map_err(|e| Fault::Refused("invalid_limits", e))?,
            );
            after_limits = Some(validate_candidate(&tx, definition)?);
            validate_effective_change(
                &tx,
                before_limits.as_ref().unwrap(),
                after_limits.as_ref().unwrap(),
            )?;
            let mut comparison = definition.clone();
            comparison.revision = old.revision.clone();
            if comparison != old {
                classification = ConfigurationChangeClass::Widening;
                require_widening(&tx)?;
            }
            // Full definitions cannot contain policy/governance/credential or
            // registration fields; all effective groups use existing validation.
            let encoded = encode(definition)?;
            tx.execute(
                "INSERT INTO definitions(id,revision,body) VALUES(?1,?2,?3)",
                params![definition.id, definition.revision, encoded],
            )?;
            if tx.execute(
                "UPDATE agents SET definition=?2 WHERE id=?1 AND definition=?3",
                params![definition.id, encoded, encode(&old)?],
            )? != 1
            {
                return Err(Fault::Store(
                    "definition head changed inside owner transaction".into(),
                ));
            }
            before = Some(old);
            IdentityEffect::Replaced(super::agents::agent(&tx, &definition.id)?)
        }
        IdentityMutation::Charter { mutation } => {
            let l = current
                .charter
                .as_ref()
                .ok_or_else(|| Fault::Refused("not_configured", "charter policy absent".into()))?;
            let result = super::charters::mutate(
                &super::charters::Context {
                    tx: &tx,
                    operation_id,
                    now_ms: now,
                    limits: l,
                },
                mutation,
            )?;
            IdentityEffect::Charter(result)
        }
    };
    let manager_unusable_for_future_tasks = match &effect {
        IdentityEffect::Retired(a) => a.id == policy.manager_agent_id,
        IdentityEffect::Replaced(a) => {
            a.id == policy.manager_agent_id
                && (!matches!(
                    a.definition.execution_profile,
                    Some(ExecutionProfile::CollaborationV2 { .. })
                ) || after_limits
                    .as_ref()
                    .and_then(|v| v.collaboration.capabilities.as_ref())
                    .is_none_or(|c| !c.identity_administration))
        }
        _ => false,
    };
    let outcome = IdentityAdministrationOutcome {
        actor,
        policy: policy.clone(),
        global_revision: current.revision.clone(),
        governance_revision: selection.revision,
        comparison_rule: "identity-definition-revision@1".into(),
        before,
        before_limits,
        after_limits,
        classification,
        manager_unusable_for_future_tasks,
        effect,
    };
    Ok((Data::IdentityAdministration(Box::new(outcome)), true))
}
// The full typed definition operation is not an escape from the existing
// governed resource-field selection. Extra Native fields are the explicit
// complete NativeLimits class; other unknown effective changes fail closed.
fn validate_effective_change(
    tx: &StoreContext<'_>,
    before: &crate::limits::ExecutionLimits,
    after: &crate::limits::ExecutionLimits,
) -> Result<()> {
    use crate::limits::DefinitionLimitField::*;
    let selected = super::governance::selection(tx)?;
    let mut known = before.clone();
    for field in [
        NativeContextBytes,
        NativeRequestBytes,
        NativeHelperValueBytes,
        NativeResponseWireBytes,
        CodingModelContextWindow,
        NativeOutputBytes,
        NativeResultBytes,
        NativeMaxOutputTokens,
        CodingCommandOutputBytes,
        CodingSummaryChars,
        CodingSummaryBytes,
        CollaborationCompleteResultBytes,
        CodingFrameBytes,
        CollaborationJournalBytes,
    ] {
        let old = field.value(before);
        let new = field.value(after);
        if old == new {
            continue;
        }
        let Some(new) = new else {
            return refuse(
                "governance_configuration_blocked",
                "removing a specified resource allowance has no sound comparison",
            );
        };
        if old.is_none_or(|old| new > old) && !selected.widening_fields.contains(&field) {
            return refuse(
                "governance_configuration_blocked",
                "full definition widens a field outside the active governance selection",
            );
        }
        field.set(&mut known, new);
    }
    // Every supported extra Native value is an execution/encoding allowance;
    // explicit assignments make future fields fail the whole-value comparison.
    macro_rules! native_fields {($($field:ident),+)=>{$(known.native.$field=after.native.$field;)+}}
    native_fields!(
        input_bytes,
        schema_bytes,
        json_depth,
        json_nodes,
        schema_expansion,
        validation_work,
        regex_bytes,
        regex_dfa_bytes,
        function_count,
        function_name_bytes,
        function_description_bytes,
        call_id_bytes,
        function_calls_per_response,
        memory_notice_bytes,
        endpoint_bytes,
        api_key_env_bytes,
        api_key_bytes,
        http_connect_timeout_ms,
        error_body_bytes,
        error_read_timeout_ms,
        decode_check_bytes,
        helper_error_bytes,
        task_tool_observation_bytes,
        summary_bytes,
        decision_envelope_bytes,
        continuation_envelope_bytes,
        task_tool_identity_bytes
    );
    // Version-one capabilities are a closed set of ordinary collaboration
    // mechanisms. They cannot carry governance, repositories or host authority.
    known.collaboration.capabilities = after.collaboration.capabilities.clone();
    if &known != after {
        return refuse(
            "governance_configuration_blocked",
            "unclassified effective definition change",
        );
    }
    Ok(())
}
fn require_widening(tx: &StoreContext<'_>) -> Result<()> {
    let selected = super::governance::selection(tx)?;
    let rule = super::governance::read_rule(tx, &selected.definition_limits)?;
    if !matches!(
        rule.body,
        crate::governance::RuleBody::DefinitionLimits {
            allow_configured_widening: true
        }
    ) {
        return refuse(
            "governance_configuration_blocked",
            "current governance refuses widening identity configuration",
        );
    }
    Ok(())
}

pub(super) fn request(operation: &Operation) -> Result<Option<IdentityAdministrationRequest>> {
    match operation {
        Operation::AdministerIdentity { input, .. } => Ok(Some(input.clone())),
        Operation::CallCollaboration { call }
            if crate::mcp::identity_administration::handles(&call.tool)
                && call.tool != "collab_task_create" =>
        {
            crate::mcp::identity_administration::parse(&call.tool, &call.arguments)
                .map(Some)
                .map_err(|e| Fault::Store(e))
        }
        _ => Ok(None),
    }
}
/// A reader projection only. The stored original request and receipt are never
/// rewritten or replaced by these inner operation shapes.
pub(super) fn project(
    mut original: super::metadata::Recorded,
) -> Result<super::metadata::Recorded> {
    if matches!(&original.outcome, Outcome::Refused { .. }) {
        return Ok(original);
    }
    if let Some(input) = request(&original.request)? {
        let id = original.commit.operation_id.clone();
        original.request = match input.mutation {
            IdentityMutation::Register { definition } => Operation::RegisterAgent {
                operation_id: id,
                definition,
            },
            IdentityMutation::Retire {
                agent_id,
                expected_revision,
            } => Operation::RetireAgent {
                operation_id: id,
                agent_id,
                expected_revision,
            },
            IdentityMutation::Charter { mutation } => Operation::MutateCharter {
                operation_id: id,
                mutation,
            },
            IdentityMutation::Replace { .. } => original.request,
        };
        if let Outcome::Ok {
            data: Data::IdentityAdministration(value),
        } = original.outcome
        {
            original.outcome = Outcome::Ok {
                data: match value.effect {
                    IdentityEffect::Registered(a) | IdentityEffect::Retired(a) => Data::Agent(a),
                    IdentityEffect::Charter(v) => Data::Charter(v),
                    _ => Data::IdentityAdministration(value),
                },
            };
        }
    }
    Ok(original)
}
pub(super) fn validate_existing(
    tx: &StoreContext<'_>,
    cfg: &ControllerConfig,
    version: i64,
) -> Result<()> {
    let mut q = tx.prepare("SELECT id FROM receipts ORDER BY controller_revision")?;
    let ids = q
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if ids.len() > cfg.max_receipts.saturating_add(cfg.max_cleanup_receipts) {
        return Err(Fault::Store(
            "identity receipt inventory exceeds bound".into(),
        ));
    }
    for id in ids {
        let Some(r) = super::metadata::scan_inline(tx, &id)? else { continue };
        if version < 25
            && (crate::protocol::requires_wire24(&r.request)
                || crate::protocol::outcome_requires_wire24(&r.outcome))
        {
            return Err(Fault::Store(
                "historical schema contains identity/task creation vocabulary".into(),
            ));
        }
        if let Outcome::Ok {
            data: Data::IdentityAdministration(value),
        } = &r.outcome
        {
            let request = request(&r.request)?.ok_or_else(|| {
                Fault::Store("identity outcome lacks its original request".into())
            })?;
            if value.policy.revision != request.expected_policy_revision
                || value.global_revision != request.expected_global_revision
                || value.governance_revision != request.expected_governance_revision
                || value.comparison_rule != "identity-definition-revision@1"
            {
                return Err(Fault::Store(
                    "identity original configuration differs".into(),
                ));
            }
            if let Operation::CallCollaboration { call } = &r.request {
                let ActorProvenance::BoundAgentSession { session_id, .. } = &value.actor.provenance
                else {
                    return Err(Fault::Store("identity original actor is not bound".into()));
                };
                let task = super::sessions::historical_task(tx, &call.caller.task_id, session_id)?;
                if bound_actor(&task, &call.caller) != value.actor
                    || value.actor.principal
                        != (PrincipalRef::Agent {
                            agent_id: value.policy.manager_agent_id.clone(),
                        })
                {
                    return Err(Fault::Store(
                        "identity original actor/provenance differs".into(),
                    ));
                }
                super::collaboration_workflow::validate_recorded_actor(tx, &value.actor)?;
            }
            let definition = match &value.effect {
                IdentityEffect::Registered(a)
                | IdentityEffect::Retired(a)
                | IdentityEffect::Replaced(a) => Some(&a.definition),
                _ => None,
            };
            if let Some(d) = definition {
                let raw: String = tx.query_row(
                    "SELECT body FROM definitions WHERE id=?1 AND revision=?2",
                    params![d.id, d.revision],
                    |r| r.get(0),
                )?;
                if decode::<Definition>(&raw)? != *d {
                    return Err(Fault::Store("identity original definition differs".into()));
                }
            }
        }
        if matches!(&r.request,Operation::CallCollaboration{call} if call.tool=="collab_task_create")
        {
            super::task_creation::validate_original(tx, &r)?;
        }
    }
    if version < 25 {
        for (table, column) in [
            ("tasks", "body"),
            ("commands", "body"),
            ("definitions", "body"),
        ] {
            let mut q = tx.prepare(&format!("SELECT {column} FROM {table}"))?;
            for row in q.query_map([], |r| r.get::<_, String>(0))? {
                let raw = row?;
                let selected = match table {
                    "tasks" => crate::protocol::input_requires_wire24(
                        &decode::<StoredTask>(&raw)?.task.input,
                    ),
                    "commands" => {
                        matches!(decode::<WorkerCommand>(&raw)?,WorkerCommand::RunAttempt{task,..} if crate::protocol::input_requires_wire24(&task))
                    }
                    _ => crate::protocol::definition_requires_wire24(&decode::<Definition>(&raw)?),
                };
                if selected {
                    return Err(Fault::Store(
                        "historical schema contains new accepted policy/profile".into(),
                    ));
                }
            }
        }
    }
    Ok(())
}
