use crate::model::*;
use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

pub(crate) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ToolRecord {
    pub call: ToolCall,
    pub servers: Vec<McpServerConfig>,
}
pub(crate) struct Store {
    conn: Mutex<Connection>,
}
impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;
          CREATE TABLE IF NOT EXISTS sessions(id TEXT PRIMARY KEY, body TEXT NOT NULL, stop_requested INTEGER NOT NULL DEFAULT 0);
          CREATE TABLE IF NOT EXISTS transcript(sequence INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL REFERENCES sessions(id), kind TEXT NOT NULL, item TEXT NOT NULL, created_at INTEGER NOT NULL);
          CREATE INDEX IF NOT EXISTS transcript_session ON transcript(session_id, sequence);
          CREATE TABLE IF NOT EXISTS tool_calls(session_id TEXT NOT NULL REFERENCES sessions(id), call_id TEXT NOT NULL, body TEXT NOT NULL, PRIMARY KEY(session_id,call_id));")?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
    pub fn recover(&self) -> Result<()> {
        let mut c = self.conn.lock().unwrap();
        let tx = c.transaction()?;
        let sessions = {
            let mut q = tx.prepare("SELECT body FROM sessions")?;
            q.query_map([], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        for body in sessions {
            let mut session: Session = serde_json::from_str(&body)?;
            let records = Self::tools_at(&tx, &session.id)?;
            let mut unknown = false;
            let previous_status = session.status;
            for mut record in records {
                if record.call.status == "running" {
                    record.call.status = "unknown".into();
                    record.call.error = Some("Runtime stopped before the tool result was durably recorded; resolve the original call explicitly".into());
                    Self::put_tool(&tx, &session.id, &record)?;
                }
                unknown |= record.call.status == "unknown";
            }
            if unknown {
                session.status = SessionStatus::NeedsResolution;
                session.progress = "An interrupted tool needs its original result resolved".into();
            } else if matches!(
                session.status,
                SessionStatus::Running | SessionStatus::Retrying | SessionStatus::Stopping
            ) {
                let stopped: bool = tx.query_row(
                    "SELECT stop_requested FROM sessions WHERE id=?",
                    [&session.id],
                    |r| r.get(0),
                )?;
                session.status = if stopped {
                    SessionStatus::Stopped
                } else {
                    SessionStatus::Ready
                };
                session.progress = "Recovered from the committed transcript".into();
            }
            if unknown || session.status != previous_status {
                Self::put_session(&tx, &mut session)?;
            }
        }
        tx.commit()?;
        Ok(())
    }
    pub fn create(&self, agent: &str, task: Option<String>, input: &str) -> Result<Session> {
        let mut c = self.conn.lock().unwrap();
        let tx = c.transaction()?;
        let stamp = now();
        let session = Session {
            id: uuid::Uuid::new_v4().to_string(),
            agent_id: agent.into(),
            task,
            status: SessionStatus::Ready,
            progress: "Ready".into(),
            error: None,
            turns: 0,
            created_at: stamp,
            updated_at: stamp,
        };
        tx.execute(
            "INSERT INTO sessions(id,body) VALUES(?,?)",
            params![session.id, serde_json::to_string(&session)?],
        )?;
        Self::append(
            &tx,
            &session.id,
            "input",
            &json!({"role":"user","content":input}),
        )?;
        tx.commit()?;
        Ok(session)
    }
    pub fn get(&self, id: &str) -> Result<Session> {
        Self::get_at(&self.conn.lock().unwrap(), id)
    }
    fn get_at(c: &Connection, id: &str) -> Result<Session> {
        let body: Option<String> = c
            .query_row("SELECT body FROM sessions WHERE id=?", [id], |r| r.get(0))
            .optional()?;
        serde_json::from_str(&body.with_context(|| format!("session not found: {id}"))?)
            .context("invalid session record")
    }
    pub fn list(&self) -> Result<Vec<Session>> {
        let c = self.conn.lock().unwrap();
        let mut q = c.prepare("SELECT body FROM sessions ORDER BY rowid")?;
        let raw = q
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        raw.into_iter()
            .map(|v| Ok(serde_json::from_str(&v)?))
            .collect()
    }
    fn put_session(c: &Connection, s: &mut Session) -> Result<()> {
        s.updated_at = now();
        c.execute(
            "UPDATE sessions SET body=? WHERE id=?",
            params![serde_json::to_string(s)?, s.id],
        )?;
        Ok(())
    }
    pub fn state(
        &self,
        id: &str,
        status: SessionStatus,
        progress: &str,
        error: Option<String>,
    ) -> Result<Session> {
        let mut c = self.conn.lock().unwrap();
        let tx = c.transaction()?;
        let mut s = Self::get_at(&tx, id)?;
        s.status = status;
        s.progress = progress.into();
        s.error = error;
        Self::put_session(&tx, &mut s)?;
        tx.commit()?;
        Ok(s)
    }
    pub fn stopped(&self, id: &str) -> Result<bool> {
        Ok(self.conn.lock().unwrap().query_row(
            "SELECT stop_requested FROM sessions WHERE id=?",
            [id],
            |r| r.get(0),
        )?)
    }
    pub fn stop(&self, id: &str, active: bool) -> Result<Session> {
        let mut c = self.conn.lock().unwrap();
        let tx = c.transaction()?;
        let mut s = Self::get_at(&tx, id)?;
        tx.execute("UPDATE sessions SET stop_requested=1 WHERE id=?", [id])?;
        if s.status != SessionStatus::NeedsResolution {
            s.status = if active {
                SessionStatus::Stopping
            } else {
                SessionStatus::Stopped
            };
        }
        s.progress = "Stop requested".into();
        Self::put_session(&tx, &mut s)?;
        tx.commit()?;
        Ok(s)
    }
    pub fn resume(&self, id: &str) -> Result<Session> {
        let mut c = self.conn.lock().unwrap();
        let tx = c.transaction()?;
        let mut s = Self::get_at(&tx, id)?;
        if Self::tools_at(&tx, id)?
            .iter()
            .any(|t| t.call.status == "unknown")
        {
            bail!("unresolved tool calls must be resolved before resume")
        }
        tx.execute("UPDATE sessions SET stop_requested=0 WHERE id=?", [id])?;
        s.status = SessionStatus::Ready;
        s.error = None;
        s.progress = "Ready to resume committed work".into();
        Self::put_session(&tx, &mut s)?;
        tx.commit()?;
        Ok(s)
    }
    pub fn steer(&self, id: &str, text: &str) -> Result<Session> {
        let mut c = self.conn.lock().unwrap();
        let tx = c.transaction()?;
        let mut s = Self::get_at(&tx, id)?;
        Self::append(&tx, id, "input", &json!({"role":"user","content":text}))?;
        if !matches!(
            s.status,
            SessionStatus::Running
                | SessionStatus::Retrying
                | SessionStatus::Stopping
                | SessionStatus::Stopped
                | SessionStatus::NeedsResolution
        ) {
            s.status = SessionStatus::Ready;
        }
        s.progress = "New input saved".into();
        Self::put_session(&tx, &mut s)?;
        tx.commit()?;
        Ok(s)
    }
    fn append(c: &Connection, id: &str, kind: &str, item: &Value) -> Result<()> {
        c.execute(
            "INSERT INTO transcript(session_id,kind,item,created_at) VALUES(?,?,?,?)",
            params![
                id,
                kind,
                serde_json::to_string(item)?,
                i64::try_from(now())?
            ],
        )?;
        Ok(())
    }
    pub fn record(&self, id: &str, kind: &str, item: &Value) -> Result<()> {
        Self::append(&self.conn.lock().unwrap(), id, kind, item)
    }
    pub fn transcript(&self, id: &str) -> Result<Vec<TranscriptEntry>> {
        let c = self.conn.lock().unwrap();
        Self::get_at(&c, id)?;
        let mut q=c.prepare("SELECT sequence,kind,item,created_at FROM transcript WHERE session_id=? ORDER BY sequence")?;
        let raw = q
            .query_map([id], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        raw.into_iter()
            .map(|(sequence, kind, item, created_at)| {
                Ok(TranscriptEntry {
                    sequence,
                    kind,
                    item: serde_json::from_str(&item)?,
                    created_at: created_at.try_into()?,
                })
            })
            .collect()
    }
    pub fn response(
        &self,
        id: &str,
        response: &ProviderResponse,
        calls: Vec<ToolRecord>,
        input_sequence: i64,
    ) -> Result<bool> {
        let mut c = self.conn.lock().unwrap();
        let tx = c.transaction()?;
        let mut s = Self::get_at(&tx, id)?;
        // The entire model output and every call intent commit before any tool can run.
        for output in &response.output {
            Self::append(&tx, id, "output", output)?;
        }
        Self::append(
            &tx,
            id,
            "response",
            &json!({"id":response.id,"status":response.status,"usage":response.usage}),
        )?;
        let has_calls = !calls.is_empty();
        for call in calls {
            tx.execute(
                "INSERT INTO tool_calls(session_id,call_id,body) VALUES(?,?,?)",
                params![id, call.call.call_id, serde_json::to_string(&call)?],
            )?;
        }
        s.turns += 1;
        let mut finished = false;
        if !has_calls {
            let newer: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM transcript WHERE session_id=? AND kind='input' AND sequence>?)", params![id, input_sequence], |r| r.get(0))?;
            let stop: bool = tx.query_row(
                "SELECT stop_requested FROM sessions WHERE id=?",
                [id],
                |r| r.get(0),
            )?;
            s.status = if stop {
                SessionStatus::Stopped
            } else if newer {
                SessionStatus::Running
            } else {
                SessionStatus::Completed
            };
            s.progress = if stop {
                "Stopped"
            } else if newer {
                "Processing new input"
            } else {
                "Completed"
            }
            .into();
            s.error = None;
            finished = !newer || stop;
        }
        Self::put_session(&tx, &mut s)?;
        tx.commit()?;
        Ok(finished)
    }
    fn tools_at(c: &Connection, id: &str) -> Result<Vec<ToolRecord>> {
        let mut q = c.prepare("SELECT body FROM tool_calls WHERE session_id=? ORDER BY rowid")?;
        let raw = q
            .query_map([id], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        raw.into_iter()
            .map(|v| Ok(serde_json::from_str(&v)?))
            .collect()
    }
    pub fn tools(&self, id: &str) -> Result<Vec<ToolRecord>> {
        let c = self.conn.lock().unwrap();
        Self::get_at(&c, id)?;
        Self::tools_at(&c, id)
    }
    fn put_tool(c: &Connection, id: &str, record: &ToolRecord) -> Result<()> {
        c.execute(
            "UPDATE tool_calls SET body=? WHERE session_id=? AND call_id=?",
            params![serde_json::to_string(record)?, id, record.call.call_id],
        )?;
        Ok(())
    }
    pub fn tool_state(
        &self,
        id: &str,
        call_id: &str,
        status: &str,
        error: Option<String>,
    ) -> Result<()> {
        let mut c = self.conn.lock().unwrap();
        let tx = c.transaction()?;
        let mut r = Self::tools_at(&tx, id)?
            .into_iter()
            .find(|t| t.call.call_id == call_id)
            .context("tool call not found")?;
        r.call.status = status.into();
        r.call.error = error;
        Self::put_tool(&tx, id, &r)?;
        tx.commit()?;
        Ok(())
    }
    pub fn tool_result(
        &self,
        id: &str,
        call_id: &str,
        result: Value,
        explicit: bool,
    ) -> Result<Session> {
        let mut c = self.conn.lock().unwrap();
        let tx = c.transaction()?;
        let mut s = Self::get_at(&tx, id)?;
        let mut r = Self::tools_at(&tx, id)?
            .into_iter()
            .find(|t| t.call.call_id == call_id)
            .context("tool call not found")?;
        if r.call.status == "succeeded" {
            if r.call.result.as_ref() == Some(&result) {
                return Ok(s);
            }
            bail!("tool result already committed with different content")
        }
        if explicit && r.call.status != "unknown" {
            bail!("only an unknown tool call can be explicitly resolved")
        }
        if !explicit && r.call.status != "running" {
            bail!("tool call is not running")
        }
        r.call.status = "succeeded".into();
        r.call.result = Some(result.clone());
        r.call.error = None;
        Self::put_tool(&tx, id, &r)?;
        Self::append(
            &tx,
            id,
            "tool_result",
            &json!({"type":"function_call_output","call_id":call_id,"output":serde_json::to_string(&result)?}),
        )?;
        if explicit {
            Self::append(
                &tx,
                id,
                "resolution",
                &json!({"call_id":call_id,"operation_id":r.call.operation_id}),
            )?;
            if !Self::tools_at(&tx, id)?
                .iter()
                .any(|t| t.call.status == "unknown")
            {
                let stopped: bool = tx.query_row(
                    "SELECT stop_requested FROM sessions WHERE id=?",
                    [id],
                    |r| r.get(0),
                )?;
                s.status = if stopped {
                    SessionStatus::Stopped
                } else {
                    SessionStatus::Ready
                };
                s.error = None;
                s.progress = "Original tool result resolved".into();
            }
        }
        Self::put_session(&tx, &mut s)?;
        tx.commit()?;
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn final_reply() -> ProviderResponse {
        ProviderResponse {
            id: Some("final".into()),
            status: "completed".into(),
            usage: None,
            output: vec![
                json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"Finished"}]}),
            ],
        }
    }

    #[test]
    fn final_response_commit_survives_crash_without_restarting_inference() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let store = Store::open(&path).unwrap();
        let session = store.create("agent", None, "work").unwrap();
        store
            .state(&session.id, SessionStatus::Running, "Inference", None)
            .unwrap();
        let seq = store
            .transcript(&session.id)
            .unwrap()
            .last()
            .unwrap()
            .sequence;
        assert!(
            store
                .response(&session.id, &final_reply(), vec![], seq)
                .unwrap()
        );
        // Crash boundary: no executor finalization call occurs after response commit.
        drop(store);
        let reopened = Store::open(&path).unwrap();
        reopened.recover().unwrap();
        assert_eq!(
            reopened.get(&session.id).unwrap().status,
            SessionStatus::Completed
        );
        assert_eq!(
            reopened
                .transcript(&session.id)
                .unwrap()
                .iter()
                .filter(|e| e.kind == "output")
                .count(),
            1
        );
    }

    #[test]
    fn input_received_during_inference_survives_final_response_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let store = Store::open(&path).unwrap();
        let session = store.create("agent", None, "first").unwrap();
        store
            .state(&session.id, SessionStatus::Running, "Inference", None)
            .unwrap();
        let seq = store
            .transcript(&session.id)
            .unwrap()
            .last()
            .unwrap()
            .sequence;
        store.steer(&session.id, "second").unwrap();
        assert!(
            !store
                .response(&session.id, &final_reply(), vec![], seq)
                .unwrap()
        );
        drop(store);
        let reopened = Store::open(&path).unwrap();
        reopened.recover().unwrap();
        assert_eq!(
            reopened.get(&session.id).unwrap().status,
            SessionStatus::Ready
        );
        assert!(
            reopened
                .transcript(&session.id)
                .unwrap()
                .iter()
                .any(|e| e.item.get("content") == Some(&json!("second")))
        );
    }
}
