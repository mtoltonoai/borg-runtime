//! Read-only validation of historical shared-memory notification state.
//! Fresh automatic delivery operations are retired; stored memory is separate.
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
// Historical pending batches are validated against the original filtered audit
// prefix. Validation does not expose or deliver fresh memory notifications.
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
    let record=super::metadata::read(tx,&event.operation_id)?.ok_or_else(||Fault::Store("memory event original receipt absent".into()))?;
    let attempt_id=match &record.request {
        Operation::SetMemory { task_id:owner,attempt_id,.. } if owner==task_id => attempt_id,
        Operation::CallCollaboration { call } if call.caller.task_id==task_id && call.tool=="collab_memory_set" => &call.caller.attempt_id,
        _=>return Err(Fault::Store("memory event original mutation differs".into())),
    };
    if !matches!(record.outcome,Outcome::Ok{..}){return Err(Fault::Store("memory event original mutation was refused".into()))}
    let raw:String=tx.query_row("SELECT body FROM attempts WHERE id=?1 AND task_id=?2",params![attempt_id,task_id],|r|r.get(0))?;
    let attempt:Attempt=decode(&raw)?;
    let source=super::sessions::get(tx,&attempt.session_id)?;
    let agent=source.agent_id.clone();
    let session_id=source.id.clone();
    if agent != s.agent_id || session_id == s.id {return Ok(None)}
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
/// The notification protocol remains decodable for historical receipts only.
/// The database owner resolves an existing exact receipt before this handler.
/// Fresh requests never scan history, prepare a batch, or acknowledge delivery.
pub(super) fn apply(
    tx: &Connection,
    op: &Operation,
    _cfg: &ControllerConfig,
    now: u64,
) -> Result<(Data, bool)> {
    let caller = match op {
        Operation::ObserveAgentMemory { caller, .. }
        | Operation::PrepareAgentMemory { caller, .. }
        | Operation::AcknowledgeAgentMemory { caller, .. } => caller,
        _ => unreachable!(),
    };
    super::collaboration_calls::caller(tx, caller, now)?;
    refuse("disabled", "automatic shared-memory change notifications are retired")
}
