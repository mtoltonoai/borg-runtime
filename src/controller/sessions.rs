//! Canonical agent instantiations. State lives in the associated task/attempts;
//! the immutable session link does not create a second execution state machine.
use super::{ControllerConfig, database::*};
use crate::protocol::*;
use super::database::StoreContext as Connection;
use rusqlite::{ OptionalExtension, params};

pub(super) fn get(tx: &Connection, id: &str) -> Result<Session> {
    let limits = tx.limits;
    if !valid_id(limits, id) {
        return refuse("invalid_input", "invalid session identity");
    }
    let row: Option<(String,String,String)> = tx.query_row(
        &format!("SELECT agent,task_id,CAST(substr(CAST(body AS BLOB),1,{body_probe}) AS TEXT) FROM sessions WHERE id=?1", body_probe=limits.transport.body_bytes.saturating_add(1)),
        [id], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    let Some((agent, task_id, body)) = row else {
        return refuse("not_found", "session not found");
    };
    if body.len() > tx.limits.transport.body_bytes {
        return Err(Fault::Store("session body exceeds bound".into()));
    }
    let session: Session = decode(&body)?;
    if session.id != id
        || session.agent_id != agent
        || session.task_id != task_id
        || session.agent_id != session.definition.id
        || session.id == session.task_id
        || session.id == session.agent_id
        || !valid_id(limits, &task_id)
        || !valid_id(limits, &agent)
    {
        return Err(Fault::Store("session/index identity mismatch".into()));
    }
    session
        .definition
        .required_inference()
        .map_err(|_| Fault::Store("session inference binding invalid".into()))?;
    if session.definition.execution_profile.is_none() {
        return Err(Fault::Store("session tool profile absent".into()));
    }
    let definition: String = tx.query_row(
        "SELECT body FROM definitions WHERE id=?1 AND revision=?2",
        params![agent, session.definition.revision],
        |r| r.get(0),
    )?;
    if decode::<Definition>(&definition)? != session.definition {
        return Err(Fault::Store("session definition revision differs".into()));
    }
    Ok(session)
}

pub(super) fn insert(tx: &Connection, session: &Session, seq: u64) -> Result<()> {
    let limits = tx.limits;
    if !valid_id(limits, &session.id)
        || !valid_id(limits, &session.task_id)
        || session.agent_id != session.definition.id
        || session.id == session.task_id
        || session.id == session.agent_id
    {
        return Err(Fault::Store("invalid new session binding".into()));
    }
    let memory_start: i64 = tx.query_row("SELECT COALESCE(MAX(seq),0) FROM history",[],|r|r.get(0))?;
    unsigned(memory_start)?;
    tx.execute(
        "INSERT INTO sessions(id,seq,agent,task_id,body,memory_start_seq,memory_cursor_seq,memory_pending) VALUES(?1,?2,?3,?4,?5,?6,?6,NULL)",
        params![
            session.id,
            integer(seq)?,
            session.agent_id,
            session.task_id,
            encode(session)?,
            memory_start
        ],
    )?;
    Ok(())
}
// Traceability scope: Each session links its own durable task and fenced attempts; validation refuses cross-session execution state.
//= spec/impl/v2/implementation.md#definitions-select-execution
//# A session MUST retain its own execution state.
pub(super) fn validate_task(tx: &Connection, task: &Task) -> Result<()> {
    let session = get(tx, &task.session_id)?;
    if session.task_id != task.id
        || session.agent_id != task.assigned_agent_id
        || session.definition != task.input.definition
    {
        return Err(Fault::Store("task/session binding differs".into()));
    }
    if let Some(attempt) = &task.attempt {
        session
            .validate_for(attempt, &task.input)
            .map_err(|_| Fault::Store("attempt/session binding differs".into()))?;
    }
    Ok(())
}
pub(super) fn validate_existing(
    conn: &Connection,
    cfg: &ControllerConfig,
) -> std::result::Result<(), String> {
    let result = (|| -> Result<()> {
        let count: i64 = conn.query_row("SELECT count(*) FROM sessions", [], |r| r.get(0))?;
        let tasks: i64 = conn.query_row("SELECT count(*) FROM tasks", [], |r| r.get(0))?;
        if count != tasks || unsigned(count)? > cfg.max_tasks as u64 {
            return Err(Fault::Store("session/task count mismatch".into()));
        }
        let mut q = conn.prepare("SELECT id FROM sessions ORDER BY seq")?;
        let mut rows = q.query([])?;
        while let Some(row) = rows.next()? {
            let session = get(conn, &row.get::<_, String>(0)?)?;
            super::memory_notifications::validate_existing(conn, &session)?;
            let task = load(conn, &session.task_id)?;
            if task.task.session_id != session.id {
                return Err(Fault::Store("session task back-link differs".into()));
            }
        }
        Ok(())
    })();
    result.map_err(|_| "corrupt session/task association; original bytes retained".into())
}
