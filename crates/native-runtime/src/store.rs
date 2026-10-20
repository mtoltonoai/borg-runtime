use crate::model::*;
use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{Mutex, MutexGuard},
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
    // WAL readers remain observable while an external deployment writer owns
    // the write lock. In-memory SQLite uses its single connection instead.
    reader: Option<Mutex<Connection>>,
}
fn wait_for_writer(attempt: i32) -> bool {
    // SQLite retries the same lock acquisition/statement, never a model or tool
    // helper. There is no time budget that can discard an already received tool
    // result. Diagnostics expose prolonged contention without database contents.
    if attempt == 0 || attempt % 600 == 0 {
        eprintln!(
            "native storage waiting for SQLite writer lock (attempt {attempt}); preserving pending commit"
        );
    }
    let delay = 1u64 << (attempt.max(0) as u32).min(6);
    std::thread::sleep(std::time::Duration::from_millis(delay));
    true
}
impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.busy_handler(Some(wait_for_writer))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;
          CREATE TABLE IF NOT EXISTS sessions(id TEXT PRIMARY KEY, body TEXT NOT NULL, stop_requested INTEGER NOT NULL DEFAULT 0);
          CREATE TABLE IF NOT EXISTS transcript(sequence INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL REFERENCES sessions(id), kind TEXT NOT NULL, item TEXT NOT NULL, created_at INTEGER NOT NULL);
          CREATE INDEX IF NOT EXISTS transcript_session ON transcript(session_id, sequence);
          CREATE TABLE IF NOT EXISTS tool_calls(session_id TEXT NOT NULL REFERENCES sessions(id), call_id TEXT NOT NULL, body TEXT NOT NULL, PRIMARY KEY(session_id,call_id));
          CREATE TABLE IF NOT EXISTS session_requests(session_id TEXT PRIMARY KEY REFERENCES sessions(id), body TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS board_reports(sequence INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL REFERENCES sessions(id), body TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS board_report_errors(session_id TEXT PRIMARY KEY REFERENCES sessions(id), error TEXT NOT NULL);")?;
        let reader = if path == Path::new(":memory:") {
            None
        } else {
            let reader =
                Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
            reader.busy_handler(Some(wait_for_writer))?;
            Some(Mutex::new(reader))
        };
        Ok(Self {
            conn: Mutex::new(conn),
            reader,
        })
    }
    fn read_connection(&self) -> MutexGuard<'_, Connection> {
        self.reader.as_ref().unwrap_or(&self.conn).lock().unwrap()
    }
    pub fn recover(&self) -> Result<()> {
        let mut c = self.conn.lock().unwrap();
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let sessions = {
            let mut q = tx.prepare("SELECT body FROM sessions")?;
            q.query_map([], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        for body in sessions {
            let mut session: Session = serde_json::from_str(&body)?;
            let legacy = session.last_progress_at == 0;
            if legacy {
                session.last_progress_at = session.updated_at.max(session.created_at);
            }
            session.driver_running = false;
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
            if unknown || legacy || session.status != previous_status {
                Self::put_session(&tx, &mut session)?;
            }
        }
        tx.commit()?;
        Ok(())
    }
    pub fn create(&self, agent: &str, task: Option<String>, input: &str) -> Result<Session> {
        self.create_with_options(agent, task, input, SessionOptions::default(), None)
    }
    pub fn existing_request(
        &self,
        agent: &str,
        task: &Option<String>,
        input: &str,
        options: &SessionOptions,
    ) -> Result<Option<Session>> {
        let Some(id) = &options.session_id else {
            return Ok(None);
        };
        let request = serde_json::to_string(
            &json!({"agent_id":agent,"task":task,"input":input,"options":options}),
        )?;
        let c = self.read_connection();
        let prior: Option<String> = c
            .query_row(
                "SELECT body FROM session_requests WHERE session_id=?",
                [id],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(prior) = prior {
            if prior != request {
                bail!("session create conflict: idempotency key has different intent");
            }
            return Ok(Some(Self::get_at(&c, id)?));
        }
        Ok(None)
    }
    pub fn create_with_options(
        &self,
        agent: &str,
        task: Option<String>,
        input: &str,
        options: SessionOptions,
        runtime_url: Option<String>,
    ) -> Result<Session> {
        let mut c = self.conn.lock().unwrap();
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let id = options
            .session_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let request = serde_json::to_string(
            &json!({"agent_id": agent,"task":task,"input":input,"options":options}),
        )?;
        let prior: Option<String> = tx
            .query_row(
                "SELECT body FROM session_requests WHERE session_id=?",
                [&id],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(prior) = prior {
            if prior != request {
                bail!("session create conflict: idempotency key has different intent");
            }
            return Self::get_at(&tx, &id);
        }
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM sessions WHERE id=?)",
            [&id],
            |r| r.get(0),
        )?;
        if exists {
            bail!("session create conflict: existing session has no matching create receipt");
        }
        let stamp = now();
        let session = Session {
            id,
            agent_id: agent.into(),
            task,
            status: SessionStatus::Ready,
            progress: "Ready".into(),
            error: None,
            turns: 0,
            created_at: stamp,
            updated_at: stamp,
            generation: options.generation,
            host_id: options.host_id,
            runtime_url,
            workspace_cwd: options.workspace_cwd,
            workspace_mcp_url: options.workspace_mcp_url,
            lifecycle_mcp_url: options.lifecycle_mcp_url,
            metadata: options.metadata,
            phase: "idle".into(),
            step: 0,
            last_progress_at: stamp,
            driver_running: false,
            failure_kind: None,
            report_error: None,
        };
        tx.execute(
            "INSERT INTO sessions(id,body) VALUES(?,?)",
            params![session.id, serde_json::to_string(&session)?],
        )?;
        tx.execute(
            "INSERT INTO session_requests(session_id,body) VALUES(?,?)",
            params![session.id, request],
        )?;
        Self::queue_report(&tx, &session)?;
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
        Self::get_at(&self.read_connection(), id)
    }
    fn get_at(c: &Connection, id: &str) -> Result<Session> {
        let body: Option<String> = c
            .query_row("SELECT body FROM sessions WHERE id=?", [id], |r| r.get(0))
            .optional()?;
        serde_json::from_str(&body.with_context(|| format!("session not found: {id}"))?)
            .context("invalid session record")
    }
    pub fn list(&self) -> Result<Vec<Session>> {
        let c = self.read_connection();
        let mut q = c.prepare("SELECT body FROM sessions ORDER BY rowid")?;
        let raw = q
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        raw.into_iter()
            .map(|v| Ok(serde_json::from_str(&v)?))
            .collect()
    }
    fn advance(s: &mut Session) {
        s.step = s.step.saturating_add(1);
        s.last_progress_at = now();
    }
    fn put_session(c: &Connection, s: &mut Session) -> Result<()> {
        s.updated_at = now();
        s.phase = match s.status {
            SessionStatus::Ready | SessionStatus::Completed => "idle",
            SessionStatus::Stopping => s.phase.as_str(),
            SessionStatus::Waiting | SessionStatus::Stopped => "suspended",
            SessionStatus::Failed | SessionStatus::NeedsResolution => "blocked",
            SessionStatus::Retrying => "awaiting-model",
            SessionStatus::Running
                if s.progress.starts_with("Calling ")
                    || s.progress == "Loading configured tools" =>
            {
                "awaiting-tool-result"
            }
            SessionStatus::Running => "awaiting-model",
        }
        .into();
        if s.progress.starts_with("Quiesced") {
            s.phase = "suspended".into();
        }
        s.failure_kind = match s.status {
            SessionStatus::NeedsResolution => Some("uncertain_tool_outcome".into()),
            SessionStatus::Failed if s.progress.starts_with("Inference failed") => {
                Some("inference_failed".into())
            }
            SessionStatus::Failed => Some("runtime_error".into()),
            _ => None,
        };
        c.execute(
            "UPDATE sessions SET body=? WHERE id=?",
            params![serde_json::to_string(s)?, s.id],
        )?;
        Self::queue_report(c, s)?;
        Ok(())
    }
    fn queue_report(c: &Connection, s: &Session) -> Result<()> {
        c.execute(
            "INSERT INTO board_reports(session_id,body) VALUES(?,?)",
            params![s.id, serde_json::to_string(s)?],
        )?;
        Ok(())
    }
    pub fn pending_reports(&self, limit: usize) -> Result<Vec<(i64, Session)>> {
        let c = self.read_connection();
        let mut q =
            c.prepare("SELECT sequence,body FROM board_reports ORDER BY sequence LIMIT ?")?;
        let raw = q
            .query_map([limit as i64], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        raw.into_iter()
            .map(|(seq, body)| Ok((seq, serde_json::from_str(&body)?)))
            .collect()
    }
    pub fn acknowledge_report(&self, sequence: i64, id: &str) -> Result<()> {
        let mut c = self.conn.lock().unwrap();
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute("DELETE FROM board_reports WHERE sequence=?", [sequence])?;
        tx.execute("DELETE FROM board_report_errors WHERE session_id=?", [id])?;
        tx.commit()?;
        Ok(())
    }
    pub fn report_error(&self, id: &str, error: &str) -> Result<()> {
        self.conn.lock().unwrap().execute("INSERT INTO board_report_errors(session_id,error) VALUES(?,?) ON CONFLICT(session_id) DO UPDATE SET error=excluded.error", params![id,error])?;
        Ok(())
    }
    pub fn get_report_error(&self, id: &str) -> Result<Option<String>> {
        Ok(self
            .read_connection()
            .query_row(
                "SELECT error FROM board_report_errors WHERE session_id=?",
                [id],
                |r| r.get(0),
            )
            .optional()?)
    }
    pub fn queue_current_reports(&self, include_terminal: bool) -> Result<()> {
        let mut c = self.conn.lock().unwrap();
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let raw = {
            let mut q = tx.prepare(
                "SELECT body FROM sessions WHERE id NOT IN (SELECT session_id FROM board_reports) AND (? OR json_extract(body,'$.status') IN ('ready','running','retrying','stopping'))",
            )?;
            q.query_map([include_terminal], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        for body in raw {
            Self::queue_report(&tx, &serde_json::from_str(&body)?)?;
        }
        tx.commit()?;
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
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut s = Self::get_at(&tx, id)?;
        s.status = status;
        s.progress = progress.into();
        s.error = error;
        Self::put_session(&tx, &mut s)?;
        tx.commit()?;
        Ok(s)
    }
    pub fn bind(
        &self,
        id: &str,
        options: SessionOptions,
        runtime_url: Option<String>,
    ) -> Result<Session> {
        let mut c = self.conn.lock().unwrap();
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut s = Self::get_at(&tx, id)?;
        if Self::tools_at(&tx, id)?
            .iter()
            .any(|r| matches!(r.call.status.as_str(), "running" | "unknown"))
        {
            bail!("resolve unknown tool outcomes before binding placement");
        }
        if options.generation < s.generation {
            bail!("cannot decrease session generation");
        }
        if options.generation > s.generation {
            tx.execute("DELETE FROM board_reports WHERE session_id=?", [id])?;
        }
        s.generation = options.generation;
        if options.host_id.is_some() {
            s.host_id = options.host_id;
        }
        if options.workspace_cwd.is_some() {
            s.workspace_cwd = options.workspace_cwd;
        }
        if options.workspace_mcp_url.is_some() {
            s.workspace_mcp_url = options.workspace_mcp_url;
        }
        if options.lifecycle_mcp_url.is_some() {
            s.lifecycle_mcp_url = options.lifecycle_mcp_url;
        }
        if runtime_url.is_some() {
            s.runtime_url = runtime_url;
        }
        if let (Some(current), Some(extra)) =
            (s.metadata.as_object_mut(), options.metadata.as_object())
        {
            current.extend(extra.clone());
        }
        Self::put_session(&tx, &mut s)?;
        tx.commit()?;
        Ok(s)
    }
    pub fn stopped(&self, id: &str) -> Result<bool> {
        Ok(self.read_connection().query_row(
            "SELECT stop_requested FROM sessions WHERE id=?",
            [id],
            |r| r.get(0),
        )?)
    }
    pub fn stop(&self, id: &str, active: bool) -> Result<Session> {
        let mut c = self.conn.lock().unwrap();
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
        Self::advance(&mut s);
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
        let c = self.read_connection();
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
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
        Self::advance(&mut s);
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
        let c = self.read_connection();
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
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut r = Self::tools_at(&tx, id)?
            .into_iter()
            .find(|t| t.call.call_id == call_id)
            .context("tool call not found")?;
        r.call.status = status.into();
        r.call.error = error;
        if status == "running" {
            let agent = Self::get_at(&tx, id)?.agent_id;
            for server in &mut r.servers {
                server.bound_agent_id = Some(agent.clone());
            }
        }
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
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
        Self::advance(&mut s);
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
