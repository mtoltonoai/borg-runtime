//! Read-only directory over authoritative identities and current assignments.
use super::{ControllerConfig, bounded_json_size, database::*};
use crate::protocol::*;
use super::database::StoreContext as Connection;
use rusqlite::{ OptionalExtension, params};
fn agent(tx: &Connection, id: &str) -> Result<AgentSummary> {
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
    })
}
pub(super) fn apply(
    tx: &Connection,
    op: &Operation,
    cfg: &ControllerConfig,
    now: u64,
) -> Result<(Data, bool)> {
    match op {
        Operation::GetAgent { agent_id } => Ok((Data::Agent(agent(tx, agent_id)?), false)),
        Operation::ListAgents { after, limit } => {
            if *limit == 0 { return refuse("invalid_input", "invalid page bound"); }
            if *limit > cfg.max_list_tasks {
                return refuse("invalid_input", "invalid page bound: controller.max_list_tasks");
            }
            if let Some(id) = after {
                agent(tx, id)?;
            }
            let mut q = tx
                .prepare("SELECT id FROM agents WHERE id>?1 ORDER BY id COLLATE BINARY LIMIT ?2")?;
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
            if *limit == 0 { return refuse("invalid_input", "invalid page bound"); }
            if *limit > cfg.max_list_tasks {
                return refuse("invalid_input", "invalid page bound: controller.max_list_tasks");
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
        _ => unreachable!("agent read routing"),
    }
}
