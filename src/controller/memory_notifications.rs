//! A bounded view of committed memory history plus one durable delivery prefix.
//! Model visibility belongs to consumers; these acknowledgments do not prove it.
use super::database::StoreContext as Connection;
use super::{ControllerConfig, database::*};
use crate::{collaboration::*, protocol::*};
use rusqlite::{OptionalExtension, params};
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Event {
    key: String,
    revision: u64,
    operation_id: String,
}
struct State {
    session: Session,
    start: u64,
    cursor: AgentMemoryCursor,
    pending: Option<AgentMemoryBatch>,
}
fn head(tx: &Connection) -> Result<u64> {
    unsigned(tx.query_row("SELECT COALESCE(MAX(seq),0) FROM history", [], |r| r.get(0))?)
}
fn cursor(tx: &Connection, s: &Session, sequence: u64) -> Result<AgentMemoryCursor> {
    let database_id: String = tx.query_row(
        "SELECT value FROM settings WHERE name='database_id'",
        [],
        |r| r.get(0),
    )?;
    if database_id.len() != 32
        || !database_id
            .bytes()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    {
        return Err(Fault::Store(
            "invalid memory cursor database identity".into(),
        ));
    }
    Ok(AgentMemoryCursor {
        database_id,
        agent_id: s.agent_id.clone(),
        session_id: s.id.clone(),
        sequence,
    })
}
fn limit(cfg: &ControllerConfig, n: usize) -> Result<usize> {
    if n == 0 {
        return refuse("invalid_input", "invalid memory observation limit");
    }
    if n > cfg.max_list_tasks {
        return refuse(
            "invalid_input",
            "invalid memory observation limit: controller.max_list_tasks",
        );
    }
    if n > cfg.memory_notice_items {
        return refuse(
            "invalid_input",
            "invalid memory observation limit: controller.memory_notice_items",
        );
    }
    Ok(n)
}
fn state(tx: &Connection, s: Session) -> Result<State> {
    let limits = tx.limits;
    let (start,ack,pending):(i64,i64,Option<String>)=tx.query_row(
        &format!("SELECT memory_start_seq,memory_cursor_seq,CAST(substr(CAST(memory_pending AS BLOB),1,{memory_probe}) AS TEXT) FROM sessions WHERE id=?1", memory_probe=limits.controller.memory_pending_bytes.saturating_add(1)),
        [&s.id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    let (start, ack) = (unsigned(start)?, unsigned(ack)?);
    if start > ack || ack > head(tx)? {
        return Err(Fault::Store("invalid memory delivery cursor".into()));
    }
    let pending = pending
        .map(|v| {
            if v.len() > limits.controller.memory_pending_bytes {
                return Err(Fault::Store("memory batch exceeds bound".into()));
            }
            decode::<AgentMemoryBatch>(&v)
        })
        .transpose()?;
    let st = State {
        cursor: cursor(tx, &s, ack)?,
        session: s,
        start,
        pending,
    };
    if let Some(batch) = &st.pending {
        validate_batch(tx, &st, batch)?;
    }
    Ok(st)
}
// Traceability scope: Committed history exposes only changed key/revision to other sessions with the same agent identity; own-session and foreign-agent events are filtered.
//= spec/impl/v2/implementation.md#shared-memory-changes-reach-sessions
//# A committed memory update MUST make its changed key and committed revision available to other active sessions of the same agent.
fn event_notice(
    limits: &crate::limits::RuntimeLimits,
    tx: &Connection,
    s: &Session,
    task_id: &str,
    body: &str,
) -> Result<Option<AgentMemoryNotice>> {
    if body.len() > limits.controller.memory_notice_event_bytes || !valid_id(limits, task_id) {
        return Err(Fault::Store("memory event exceeds bound".into()));
    }
    let event: Event =
        serde_json::from_str(body).map_err(|_| Fault::Store("invalid memory event".into()))?;
    if !valid_id(limits, &event.key)
        || !valid_id(limits, &event.operation_id)
        || event.revision == 0
        || event.revision > i64::MAX as u64
    {
        return Err(Fault::Store(
            "invalid memory event identity/revision".into(),
        ));
    }
    let source: Option<(String, String)> = tx
        .query_row(
            "SELECT agent,session FROM tasks WHERE id=?1",
            [task_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let Some((agent, session_id)) = source else {
        return Err(Fault::Store("memory event task missing".into()));
    };
    if agent != s.agent_id || session_id == s.id {
        return Ok(None);
    }
    let source = super::sessions::get(tx, &session_id)?;
    if source.agent_id != agent || source.task_id != task_id {
        return Err(Fault::Store("memory event origin differs".into()));
    }
    Ok(Some(AgentMemoryNotice {
        key: event.key,
        revision: event.revision,
    }))
}
fn page(
    tx: &Connection,
    s: &Session,
    from: &AgentMemoryCursor,
    through: u64,
    n: usize,
) -> Result<Option<AgentMemoryPage>> {
    let limits = tx.limits;
    if from.sequence > through || through > head(tx)? {
        return refuse("invalid_input", "memory history window is invalid");
    }
    if from.sequence == through {
        return Ok(None);
    }
    let scan = n
        .saturating_mul(limits.controller.memory_notice_scan_factor)
        .max(limits.controller.memory_notice_scan_min)
        .min(limits.controller.memory_notice_scan_max);
    let mut query=tx.prepare(&format!("SELECT seq,substr(task_id,1,{identity_probe}),substr(kind,1,{kind_probe}),CASE WHEN kind='memory_recorded' THEN CAST(substr(CAST(body AS BLOB),1,{event_probe}) AS TEXT) ELSE '' END FROM history WHERE seq>?1 AND seq<=?2 ORDER BY seq LIMIT ?3", event_probe=limits.controller.memory_notice_event_bytes.saturating_add(1), identity_probe=limits.transport.identity_bytes.saturating_add(1), kind_probe=limits.controller.history_kind_bytes.saturating_add(1)))?;
    let mut rows = query.query(params![
        integer(from.sequence)?,
        integer(through)?,
        scan as i64
    ])?;
    let mut notices: BTreeMap<String, u64> = BTreeMap::new();
    let mut after = from.sequence;
    let mut examined = 0usize;
    let mut exhausted = false;
    while examined < scan {
        let Some(row) = rows.next()? else {
            exhausted = true;
            break;
        };
        let sequence = unsigned(row.get(0)?)?;
        let task_id: String = row.get(1)?;
        let kind: String = row.get(2)?;
        let proposed = if kind == "memory_recorded" {
            event_notice(limits, tx, s, &task_id, &row.get::<_, String>(3)?)?
        } else {
            None
        };
        if let Some(notice) = proposed {
            if !notices.contains_key(&notice.key) && notices.len() == n {
                break;
            }
            let old = notices.get(&notice.key).copied();
            notices.insert(notice.key.clone(), old.unwrap_or(0).max(notice.revision));
            let candidate = make_page(from, sequence, through, &notices);
            if bounded_json_size(&candidate, limits.controller.memory_pending_page_bytes).is_err() {
                if let Some(v) = old {
                    notices.insert(notice.key, v);
                } else {
                    notices.remove(&notice.key);
                }
                break;
            }
        }
        after = sequence;
        examined += 1;
    }
    if exhausted {
        after = through;
    }
    if after == from.sequence {
        return refuse(
            "capacity",
            "next memory notice cannot fit the page bound: controller.memory_pending_page_bytes",
        );
    }
    Ok(Some(make_page(from, after, through, &notices)))
}
fn make_page(
    from: &AgentMemoryCursor,
    after: u64,
    through: u64,
    values: &BTreeMap<String, u64>,
) -> AgentMemoryPage {
    let mut to = from.clone();
    to.sequence = after;
    AgentMemoryPage {
        from_cursor: from.clone(),
        to_cursor: to,
        through_sequence: through,
        notices: values
            .iter()
            .map(|(key, revision)| AgentMemoryNotice {
                key: key.clone(),
                revision: *revision,
            })
            .collect(),
        has_more: after < through,
    }
}
fn validate_batch(tx: &Connection, st: &State, b: &AgentMemoryBatch) -> Result<()> {
    let limits = tx.limits;
    let p = &b.page;
    let expected = cursor(tx, &st.session, p.to_cursor.sequence)?;
    if !valid_id(limits, &b.batch_id)
        || p.from_cursor != st.cursor
        || p.to_cursor != expected
        || p.to_cursor.sequence <= st.cursor.sequence
        || p.to_cursor.sequence > p.through_sequence
        || p.through_sequence > head(tx)?
        || p.has_more != (p.to_cursor.sequence < p.through_sequence)
        || p.notices.len() > limits.controller.memory_notice_items
    {
        return Err(Fault::Store("invalid retained memory batch binding".into()));
    }
    // Reconstruct the complete included prefix, with origin filtering before
    // coalescing. A fabricated skipped event or notice cannot survive restart.
    let rebuilt = page(
        tx,
        &st.session,
        &st.cursor,
        p.to_cursor.sequence,
        limits.controller.memory_notice_items,
    )?
    .ok_or_else(|| Fault::Store("empty retained memory batch".into()))?;
    if rebuilt.to_cursor != p.to_cursor || rebuilt.has_more || rebuilt.notices != p.notices {
        return Err(Fault::Store(
            "retained memory batch differs from committed history".into(),
        ));
    }
    Ok(())
}
pub(super) fn validate_existing(tx: &Connection, s: &Session) -> Result<()> {
    state(tx, s.clone()).map(|_| ())
}
fn observation(
    limits: &crate::limits::RuntimeLimits,
    st: State,
    available: Option<AgentMemoryPage>,
) -> Result<Data> {
    let value = AgentMemoryObservation {
        start_sequence: st.start,
        acknowledged_cursor: st.cursor,
        pending: st.pending,
        available,
    };
    bounded_json_size(&value, limits.controller.memory_pending_bytes).map_err(|_| {
        Fault::Refused(
            "capacity",
            "memory observation exceeds bound: controller.memory_pending_bytes".into(),
        )
    })?;
    Ok(Data::AgentMemory(value))
}
// Traceability scope: Observe, prepare and acknowledge resolve an existing current task/session and update only its notification state; they allocate no session.
//= spec/impl/v2/implementation.md#shared-memory-changes-reach-sessions
//# Memory-change notices MUST NOT create a session.
pub(super) fn apply(
    tx: &Connection,
    op: &Operation,
    cfg: &ControllerConfig,
    now: u64,
) -> Result<(Data, bool)> {
    let limits = tx.limits;
    let caller = match op {
        Operation::ObserveAgentMemory { caller, .. }
        | Operation::PrepareAgentMemory { caller, .. }
        | Operation::AcknowledgeAgentMemory { caller, .. } => caller,
        _ => unreachable!(),
    };
    let task = super::collaboration_calls::caller(tx, caller, now)?;
    let session = super::sessions::get(tx, &task.task.session_id)?;
    let mut st = state(tx, session)?;
    let result = match op {
        Operation::ObserveAgentMemory { limit: n, .. } => {
            let n = limit(cfg, *n)?;
            let available = if st.pending.is_none() {
                page(tx, &st.session, &st.cursor, head(tx)?, n)?
            } else {
                None
            };
            (observation(limits, st, available)?, false)
        }
        Operation::PrepareAgentMemory {
            expected_cursor,
            through_sequence,
            limit: n,
            ..
        } => {
            let n = limit(cfg, *n)?;
            if expected_cursor != &st.cursor {
                return refuse(
                    "stale_revision",
                    "memory acknowledged cursor changed or scope differs",
                );
            }
            if st.pending.is_some() {
                (observation(limits, st, None)?, false)
            } else {
                let available = page(tx, &st.session, &st.cursor, *through_sequence, n)?
                    .ok_or_else(|| {
                        Fault::Refused("no_changes", "memory window has no unaccounted rows".into())
                    })?;
                let sequence = next(tx, "memory_batch")?;
                let batch = AgentMemoryBatch {
                    batch_id: AgentMemoryBatch::identifier(&st.cursor.database_id, sequence),
                    page: available,
                };
                bounded_json_size(&batch, limits.controller.memory_pending_bytes).map_err(
                    |_| {
                        Fault::Refused(
                            "capacity",
                            "memory pending batch exceeds bound: controller.memory_pending_bytes"
                                .into(),
                        )
                    },
                )?;
                if tx.execute("UPDATE sessions SET memory_pending=?2 WHERE id=?1 AND memory_pending IS NULL AND memory_cursor_seq=?3",params![st.session.id,encode(&batch)?,integer(st.cursor.sequence)?])?!=1 {return Err(Fault::Store("memory prepare binding changed".into()));}
                st.pending = Some(batch);
                (observation(limits, st, None)?, true)
            }
        }
        Operation::AcknowledgeAgentMemory { batch_id, .. } => {
            if !valid_id(limits, batch_id) {
                return refuse("invalid_input", "invalid memory batch identity");
            }
            let Some(batch) = st.pending.as_ref() else {
                return refuse("not_found", "session has no pending memory batch");
            };
            if &batch.batch_id != batch_id {
                return refuse(
                    "batch_mismatch",
                    "acknowledgment differs from pending memory batch",
                );
            }
            let next_cursor = batch.page.to_cursor.clone();
            if tx.execute("UPDATE sessions SET memory_cursor_seq=?2,memory_pending=NULL WHERE id=?1 AND memory_cursor_seq=?3",params![st.session.id,integer(next_cursor.sequence)?,integer(st.cursor.sequence)?])?!=1 {return Err(Fault::Store("memory acknowledgment binding changed".into()));}
            st.cursor = next_cursor;
            st.pending = None;
            (observation(limits, st, None)?, true)
        }
        _ => unreachable!(),
    };
    let final_now = (cfg.clock)();
    if final_now < now {
        return refuse(
            "clock_unavailable",
            "clock moved backwards during memory observation",
        );
    }
    super::collaboration_calls::caller(tx, caller, final_now)?;
    Ok(result)
}
