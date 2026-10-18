//! Stable directory identities with receipt-backed registration and retirement.
//! Tombstones gate only new selections; accepted sessions and history remain intact.
use super::database::StoreContext as Connection;
use super::{ControllerConfig, bounded_json_size, database::*};
use crate::protocol::*;
use rusqlite::{OptionalExtension, params};
//= spec/impl/v2/implementation.md#agent-identity-lifecycle
//# A retired agent identity MUST remain resolvable for historical reads.
//= spec/impl/v2/implementation.md#agent-identity-lifecycle
//# Agent retirement MUST preserve retained historical records.
pub(super) fn agent(tx: &Connection, id: &str) -> Result<AgentSummary> {
    let limits = tx.limits;
    if !valid_id(limits, id) {
        return refuse("invalid_input", "invalid agent identity");
    }
    let body: Option<String> = tx
        .query_row("SELECT definition FROM agents WHERE id=?1", [id], |r| {
            r.get(0)
        })
        .optional()?;
    let Some(body) = body else {
        return refuse("not_found", "agent not found");
    };
    let definition: Definition = decode(&body)?;
    if definition.id != id {
        return Err(Fault::Store("agent definition identity differs".into()));
    }
    Ok(AgentSummary {
        id: id.into(),
        definition,
        retired: retired(tx, id)?,
    })
}
pub(super) fn selected_active_definition(tx: &Connection, id: &str) -> Result<Definition> {
    let selected = agent(tx, id)?;
    require_active(tx, id)?;
    Ok(selected.definition)
}

pub(super) fn apply(
    tx: &Connection,
    op: &Operation,
    cfg: &ControllerConfig,
    now: u64,
) -> Result<(Data, bool)> {
    match op {
        Operation::RegisterAgent { definition, .. } => {
            super::identity_administration::authorize(tx)?;
            register(tx, definition, cfg)?;
            Ok((Data::Agent(agent(tx, &definition.id)?), true))
        }
        //= spec/impl/v2/implementation.md#agent-identity-lifecycle
        //# Agent retirement MUST NOT cancel already accepted work.
        Operation::RetireAgent {
            operation_id,
            agent_id,
            expected_revision,
        } => {
            super::identity_administration::authorize(tx)?;
            if !valid_id(tx.limits, expected_revision) {
                return refuse(
                    "invalid_input",
                    "invalid expected selected definition revision",
                );
            }
            let selected = agent(tx, agent_id)?;
            require_active(tx, agent_id)?;
            if selected.definition.revision != *expected_revision {
                return refuse(
                    "configuration_conflict",
                    "selected definition revision differs",
                );
            }
            tx.execute("INSERT INTO agent_retirements(id,definition_revision,operation_id) VALUES(?1,?2,?3)",
                params![agent_id, expected_revision, operation_id])?;
            Ok((Data::Agent(agent(tx, agent_id)?), true))
        }
        Operation::GetAgent { agent_id } => Ok((Data::Agent(agent(tx, agent_id)?), false)),
        //= spec/impl/v2/implementation.md#agent-identity-lifecycle
        //# Retiring an agent identity MUST remove it from the active agent directory.
        Operation::ListAgents { after, limit } => {
            if *limit == 0 {
                return refuse("invalid_input", "invalid page bound");
            }
            if *limit > cfg.max_list_tasks {
                return refuse(
                    "invalid_input",
                    "invalid page bound: controller.max_list_tasks",
                );
            }
            if let Some(id) = after {
                agent(tx, id)?;
            }
            let mut q = tx
                .prepare("SELECT id FROM agents WHERE id>?1 AND NOT EXISTS(SELECT 1 FROM agent_retirements r WHERE r.id=agents.id) ORDER BY id COLLATE BINARY LIMIT ?2")?;
            let ids = q.query_map(
                params![after.as_deref().unwrap_or(""), *limit as i64],
                |r| r.get::<_, String>(0),
            )?;
            let mut values = vec![];
            for id in ids {
                values.push(agent(tx, &id?)?);
                if bounded_json_size(&values, cfg.list_page_bytes).is_err() {
                    values.pop();
                    break;
                }
            }
            Ok((Data::Agents(values), false))
        }
        Operation::ListAgentTasks {
            agent_id,
            after,
            limit,
        } => {
            agent(tx, agent_id)?;
            if *limit == 0 {
                return refuse("invalid_input", "invalid page bound");
            }
            if *limit > cfg.max_list_tasks {
                return refuse(
                    "invalid_input",
                    "invalid page bound: controller.max_list_tasks",
                );
            }
            // A retained task cursor remains usable after reassignment away from this
            // agent. It is an ordering position, not an authorization or membership claim.
            let seq = match after {
                None => 0,
                Some(id) => tx
                    .query_row("SELECT seq FROM tasks WHERE id=?1", [id], |r| {
                        r.get::<_, i64>(0)
                    })
                    .optional()?
                    .ok_or_else(|| Fault::Refused("invalid_input", "unknown task cursor".into()))?,
            };
            let mut q =
                tx.prepare("SELECT id FROM tasks WHERE agent=?1 AND seq>?2 ORDER BY seq LIMIT ?3")?;
            let ids = q.query_map(params![agent_id, seq, *limit as i64], |r| {
                r.get::<_, String>(0)
            })?;
            let mut values = vec![];
            for id in ids {
                let t = load(tx, &id?)?;
                if t.task.assigned_agent_id != *agent_id {
                    return Err(Fault::Store("agent task index mismatch".into()));
                }
                values.push(super::operations::view(tx, t, cfg, now)?);
                if bounded_json_size(&values, cfg.list_page_bytes).is_err() {
                    values.pop();
                    break;
                }
            }
            Ok((Data::Tasks(values), false))
        }
        _ => unreachable!("agent directory routing"),
    }
}

/// Absent historical identities may still be admitted by the existing task path.
/// An existing tombstone is permanent, including when the selected definition is
/// supplied explicitly. Original receipt replay occurs before these checks.
//= spec/impl/v2/implementation.md#agent-identity-lifecycle
//# A retired agent identity MUST be refused for new work.
//= spec/impl/v2/implementation.md#agent-identity-lifecycle
//# Task submission MUST NOT reactivate a retired agent identity.
pub(super) fn require_active(tx: &Connection<'_>, id: &str) -> Result<()> {
    if retired(tx, id)? {
        return refuse(
            "agent_retired",
            "agent identity is retired; historical sessions remain readable",
        );
    }
    Ok(())
}
fn retired(tx: &Connection<'_>, id: &str) -> Result<bool> {
    let version: i64 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if version < super::application_schema::AGENT_DIRECTORY_VERSION {
        return Ok(false);
    }
    tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM agent_retirements WHERE id=?1)",
        [id],
        |r| r.get(0),
    )
    .map_err(Into::into)
}
pub(super) fn require_directory_capacity(
    tx: &Connection<'_>,
    cfg: &ControllerConfig,
) -> Result<()> {
    let count: i64 = tx.query_row("SELECT count(*) FROM agents", [], |r| r.get(0))?;
    if unsigned(count)? >= cfg.max_tasks as u64 {
        return refuse("capacity", "agent directory exceeds controller.max_tasks");
    }
    Ok(())
}
//= spec/impl/v2/implementation.md#agent-identity-lifecycle
//# The controller MUST support explicit agent registration without creating a task.
pub(super) fn register(tx: &Connection<'_>, definition: &Definition, cfg: &ControllerConfig) -> Result<()> {
    if !valid_id(tx.limits, &definition.id) || !valid_id(tx.limits, &definition.revision) {
        return refuse("invalid_input", "invalid agent definition identity");
    }
    require_active(tx, &definition.id)?;
    let present: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM agents WHERE id=?1)",
        [&definition.id],
        |r| r.get(0),
    )?;
    if present {
        return refuse(
            "already_exists",
            "agent identity already exists; use its selected revision",
        );
    }
    super::operations::require_definition(definition)?;
    crate::execution_profile::validate_definition(definition)
        .map_err(|e| Fault::Refused("invalid_input", e))?;
    cfg.limits
        .resolve_definition(definition)
        .map_err(|e| Fault::Refused("invalid_limits", e))?;
    super::charters::validate_definition(tx, definition)?;
    if bounded_json_size(definition, cfg.limits.controller.max_task_input_bytes).is_err() {
        return refuse(
            "capacity",
            "definition exceeds controller.max_task_input_bytes",
        );
    }
    // Before explicit registration every identity required a task. Preserve that
    // existing configured directory ceiling without introducing a new number.
    require_directory_capacity(tx, cfg)?;
    let body = encode(definition)?;
    let original: Option<String> = tx
        .query_row(
            "SELECT body FROM definitions WHERE id=?1 AND revision=?2",
            params![definition.id, definition.revision],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(original) = original {
        if original != body {
            return refuse(
                "definition_conflict",
                "definition revision changed its meaning",
            );
        }
    } else {
        tx.execute(
            "INSERT INTO definitions(id,revision,body) VALUES(?1,?2,?3)",
            params![definition.id, definition.revision, body],
        )?;
    }
    tx.execute(
        "INSERT INTO agents(id,definition) VALUES(?1,?2)",
        params![definition.id, body],
    )?;
    Ok(())
}

/// Runs before any historical schema upgrade. New operation vocabulary cannot
/// be smuggled into an old marker, including a recorded refused operation.
/// New tombstones are joined to their original successful receipt both ways.
pub(super) fn validate_existing(
    tx: &Connection<'_>,
    cfg: &ControllerConfig,
    version: i64,
) -> Result<()> {
    let modern = version >= super::application_schema::AGENT_DIRECTORY_VERSION;
    let count: i64 = tx.query_row("SELECT count(*) FROM agents", [], |r| r.get(0))?;
    if unsigned(count)? > cfg.max_tasks as u64 {
        return Err(Fault::Store(
            "agent directory exceeds configured bound".into(),
        ));
    }
    if modern {
        let mut q = tx.prepare(
            "SELECT id,definition_revision,operation_id FROM agent_retirements ORDER BY id",
        )?;
        let mut rows = q.query([])?;
        let mut count = 0usize;
        while let Some(row) = rows.next()? {
            count += 1;
            if count > cfg.max_tasks {
                return Err(Fault::Store(
                    "retired agent inventory exceeds configured bound".into(),
                ));
            }
            let (id, revision, operation): (String, String, String) =
                (row.get(0)?, row.get(1)?, row.get(2)?);
            if [&id, &revision, &operation]
                .iter()
                .any(|v| !valid_id(tx.limits, v))
            {
                return Err(Fault::Store("invalid retired agent identity".into()));
            }
            let selected = agent(tx, &id)?;
            if selected.definition.revision != revision || !selected.retired {
                return Err(Fault::Store(
                    "retired agent selected revision differs".into(),
                ));
            }
            let record = super::metadata::read(tx, &operation)?
                .ok_or_else(|| Fault::Store("retired agent receipt absent".into()))?;
            let record = super::identity_administration::project(record)?;
            if !matches!(
                &record.request,
                Operation::RetireAgent { operation_id, agent_id, expected_revision }
                    if operation_id == &operation && agent_id == &id && expected_revision == &revision
            ) || record.outcome
                != (Outcome::Ok {
                    data: Data::Agent(selected),
                })
            {
                return Err(Fault::Store(
                    "retired agent original receipt differs".into(),
                ));
            }
        }
    }
    let mut q = tx.prepare("SELECT id FROM receipts ORDER BY id")?;
    let mut rows = q.query([])?;
    while let Some(row) = rows.next()? {
        let id: String = row.get(0)?;
        let record = super::metadata::read(tx, &id)?
            .ok_or_else(|| Fault::Store("agent directory receipt disappeared".into()))?;
        let record=super::identity_administration::project(record)?;
        let new_op = matches!(
            &record.request,
            Operation::RegisterAgent { .. } | Operation::RetireAgent { .. }
        );
        let new_data = matches!(&record.outcome, Outcome::Ok { data: Data::Agent(a) } if a.retired)
            || matches!(&record.outcome, Outcome::Ok { data: Data::Agents(a) } if a.iter().any(|a|a.retired));
        if !modern && (new_op || new_data) {
            return Err(Fault::Store(
                "historical schema contains agent lifecycle vocabulary".into(),
            ));
        }
        match (&record.request, &record.outcome) {
            (
                Operation::RegisterAgent {
                    operation_id,
                    definition,
                },
                Outcome::Ok {
                    data: Data::Agent(original),
                },
            ) => {
                let immutable: String = tx.query_row(
                    "SELECT body FROM definitions WHERE id=?1 AND revision=?2",
                    params![definition.id, definition.revision],
                    |r| r.get(0),
                )?;
                if operation_id != &id
                    || original.id != definition.id
                    || original.definition != *definition
                    || original.retired
                    || decode::<Definition>(&immutable)? != *definition
                    || agent(tx, &definition.id).is_err()
                {
                    return Err(Fault::Store(
                        "registered agent original receipt differs".into(),
                    ));
                }
            }
            (
                Operation::RetireAgent {
                    operation_id,
                    agent_id,
                    expected_revision,
                },
                Outcome::Ok {
                    data: Data::Agent(original),
                },
            ) => {
                let selected = agent(tx, agent_id)?;
                let recorded: String = tx.query_row(
                    "SELECT operation_id FROM agent_retirements WHERE id=?1",
                    [agent_id],
                    |r| r.get(0),
                )?;
                if operation_id != &id
                    || recorded != id
                    || selected != *original
                    || !selected.retired
                    || &selected.definition.revision != expected_revision
                {
                    return Err(Fault::Store(
                        "retirement receipt lacks its exact tombstone".into(),
                    ));
                }
            }
            (
                Operation::RegisterAgent { .. } | Operation::RetireAgent { .. },
                Outcome::Ok { .. },
            ) => {
                return Err(Fault::Store(
                    "agent lifecycle receipt outcome kind differs".into(),
                ));
            }
            _ => (),
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "agent_directory_tests.rs"]
mod tests;
