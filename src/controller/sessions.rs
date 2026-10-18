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
    if input(tx, &session)? != task.input { return Err(Fault::Store("current task differs from immutable session input".into())); }
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
    workflow_schema: bool,
) -> std::result::Result<(), String> {
    let result = (|| -> Result<()> {
        let count: i64 = conn.query_row("SELECT count(*) FROM sessions", [], |r| r.get(0))?;
        let tasks: i64 = conn.query_row("SELECT count(*) FROM tasks", [], |r| r.get(0))?;
        if count < tasks || unsigned(count)? > bound(cfg)? as u64
            || (!lineage_schema(conn)? && count != tasks) {
            return Err(Fault::Store("session/task count mismatch".into()));
        }
        let mut q = conn.prepare("SELECT id FROM sessions ORDER BY seq")?;
        let mut rows = q.query([])?;
        while let Some(row) = rows.next()? {
            let session = get(conn, &row.get::<_, String>(0)?)?;
            super::memory_notifications::validate_existing(conn, &session)?;
            if workflow_schema {
                super::collaboration_notifications::validate_existing(conn, &session)?;
            }
            let task = load(conn, &session.task_id)?;
            if task.task.id != session.task_id { return Err(Fault::Store("session task back-link differs".into())); }
            let _ = input(conn, &session)?;
        }
        Ok(())
    })();
    result.map_err(|_| "corrupt session/task association; original bytes retained".into())
}

pub(super) fn bound(cfg: &ControllerConfig) -> Result<usize> {
    cfg.max_tasks.checked_add(cfg.max_receipts)
        .ok_or_else(|| Fault::Store("session retention bound overflow".into()))
}
fn lineage_schema(tx: &Connection<'_>) -> Result<bool> {
    let version: i64=tx.query_row("PRAGMA user_version",[],|r|r.get(0))?;
    Ok(version >= super::application_schema::DEFAULT_ASSIGNMENT_VERSION)
}
pub(super) fn is_successor(tx: &Connection<'_>, id: &str) -> Result<bool> {
    if !lineage_schema(tx)? {return Ok(false)}
    Ok(tx.query_row("SELECT EXISTS(SELECT 1 FROM session_inputs WHERE session_id=?1 AND previous_session_id IS NOT NULL)",[id],|r|r.get(0))?)
}
/// Immutable controller-resolved input for this exact instantiation. Older
/// stores have one session/task and are read without adding or rewriting rows.
pub(super) fn input(tx: &Connection<'_>, session: &Session) -> Result<TaskInput> {
    let maximum=tx.limits.transport.body_bytes;
    let value=if lineage_schema(tx)? {
        let row:Option<(String,String)>=tx.query_row(
            "SELECT task_id,CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM session_inputs WHERE session_id=?1",
            params![session.id,sql_bound(maximum.saturating_add(1))?],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        let (task,raw)=row.ok_or_else(||Fault::Store("session accepted input absent".into()))?;
        if task!=session.task_id || raw.len()>maximum {return Err(Fault::Store("session input identity/bound differs".into()))}
        decode::<TaskInput>(&raw)?
    } else {
        let raw:String=tx.query_row("SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM tasks WHERE id=?1",params![session.task_id,sql_bound(maximum.saturating_add(1))?],|r|r.get(0))?;
        if raw.len()>maximum{return Err(Fault::Store("historical task input exceeds bound".into()))}
        let task:StoredTask=decode(&raw)?;
        if task.task.id!=session.task_id||task.task.session_id!=session.id {return Err(Fault::Store("historical session/input differs".into()))}
        task.task.input
    };
    if value.definition!=session.definition {return Err(Fault::Store("session input definition differs".into()))}
    value.execution_limits().map_err(Fault::Store)?;
    Ok(value)
}
pub(super) fn bind_input(tx:&Connection<'_>,session:&Session,input:&TaskInput,previous:Option<&str>,operation:Option<&str>)->Result<()> {
    if input.definition!=session.definition || previous.is_some()!=operation.is_some(){return Err(Fault::Store("new session input lineage differs".into()))}
    input.execution_limits().map_err(Fault::Store)?;
    let raw=encode(input)?;
    if raw.len()>tx.limits.transport.body_bytes{return refuse("capacity","session input exceeds transport.body_bytes")}
    tx.execute("INSERT INTO session_inputs(session_id,task_id,body,previous_session_id,operation_id) VALUES(?1,?2,?3,?4,?5)",params![session.id,session.task_id,raw,previous,operation])?;
    Ok(())
}
pub(super) fn historical_task(tx:&Connection<'_>,task_id:&str,session_id:&str)->Result<Task> {
    let session=get(tx,session_id)?;
    if session.task_id!=task_id {return Err(Fault::Store("historical session belongs to another task".into()))}
    let mut task=load(tx,task_id)?.task;
    if task.session_id!=session_id { task.pending_assignment=None; }
    task.input=input(tx,&session)?;
    task.session_id=session.id;
    task.assigned_agent_id=session.agent_id;
    if task.attempt.as_ref().is_some_and(|a|a.session_id!=session_id){task.attempt=None;task.result=None;}
    Ok(task)
}
pub(super) fn initial_input(tx:&Connection<'_>,task_id:&str)->Result<TaskInput> {
    let id:String=tx.query_row("SELECT id FROM sessions WHERE task_id=?1 ORDER BY seq LIMIT 1",[task_id],|r|r.get(0))?;
    input(tx,&get(tx,&id)?)
}
