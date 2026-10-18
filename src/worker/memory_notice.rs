//! Read-only decoder for retained historical memory-notice mirrors.
//! This module is test-only; the runtime has no automatic delivery host.
use super::*;
use crate::collaboration::{
    AgentMemoryBatch, AgentMemoryCursor, AgentMemoryPage,
};
use crate::limits::MemoryNoticeLimits;
use crate::mcp::Caller;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Delivery {
    caller: Caller,
    context: String,
    appended: bool,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Pending {
    batch: AgentMemoryBatch,
    text: String,
    delivery: Option<Delivery>,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Mirror {
    limits: MemoryNoticeLimits,
    revision: u32,
    start_sequence: u64,
    acknowledged_cursor: AgentMemoryCursor,
    pending: Option<Pending>,
}

/// Qualification-only decoding of already-copied bytes. This does not call
/// Host::open, read configured credentials, observe/prepare/acknowledge memory,
/// or persist any record. The retained historical Mirror validator is reused.
#[cfg(test)]
pub(super) fn validate_retained_mirror_copy(
    bytes: &[u8],
    task: &Task,
    runtime: &crate::limits::RuntimeLimits,
    relative_scope: &Path,
    copied_file: &Path,
) -> Result<serde_json::Value, String> {
    let attempt = task.attempt.as_ref().ok_or("retained memory attempt missing")?;
    if !matches!(task.status, TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::Cancelled)
        || !attempt.stopped || attempt.phase != AttemptPhase::Stopped
        || attempt.task_id != task.id || attempt.session_id != task.session_id
    {
        return Err("retained memory caller is not canonically stopped".into());
    }
    if relative_scope != scope_directory(Path::new(""), &task.session_id) {
        return Err("retained memory session path differs".into());
    }
    let limits = task.input.execution_limits()?.memory_notice.clone();
    if bytes.len() > limits.mirror_bytes {
        return Err("retained memory mirror exceeds accepted bound".into());
    }
    let copied = read_private(copied_file, limits.mirror_bytes, "execution.memory_notice.mirror_bytes")?
        .ok_or("retained memory copy missing")?;
    if copied != bytes {
        return Err("retained memory copied bytes changed".into());
    }
    let mirror: Mirror = serde_json::from_slice(bytes).map_err(|_| "invalid retained mirror")?;
    let host = Host {
        limits,
        client: ApiClient::new("http://127.0.0.1:1", Arc::new(runtime.clone()))?,
        caller: collaboration_caller(attempt),
        session_id: task.session_id.clone(),
        agent_id: task.assigned_agent_id.clone(),
        directory: PathBuf::new(),
        credentials: Vec::new(),
        mirror: None,
    };
    host.validate_mirror(&mirror)?;
    let value = serde_json::to_value(&mirror).map_err(|_| "retained mirror encoding")?;
    if value != serde_json::from_slice::<serde_json::Value>(bytes).map_err(|_| "retained mirror JSON")? {
        return Err("retained mirror codec would change data".into());
    }
    Ok(value)
}

/// Historical validator only: no mutation, controller call, or executor append.
pub(super) struct Host {
    limits: MemoryNoticeLimits,
    client: ApiClient,
    caller: Caller,
    session_id: String,
    agent_id: String,
    directory: PathBuf,
    credentials: Vec<String>,
    mirror: Option<Mirror>,
}

impl Host {
    fn open_with_credentials(
        client: ApiClient,
        state: &Path,
        caller: &Caller,
        session_id: &str,
        agent_id: &str,
        limits: &MemoryNoticeLimits,
        credentials: Vec<String>,
    ) -> Result<Self, String> {
        caller.validate(&client.limits().transport)?;
        limits.validate()?;
        if !crate::mcp::valid_identity(session_id, client.limits().transport.identity_bytes)
            || !crate::mcp::valid_identity(agent_id, client.limits().transport.identity_bytes)
        {
            return Err("invalid host memory scope".into());
        }
        let mut host = Self {
            limits: limits.clone(),
            client,
            caller: caller.clone(),
            session_id: session_id.into(),
            agent_id: agent_id.into(),
            directory: scope_directory(state, session_id),
            credentials,
            mirror: None,
        };
        host.safe(&json!({"caller":caller,"session_id":session_id,"agent_id":agent_id}))?;
        let path = host.directory.join("delivery.json");
        if let Some(bytes) = read_private(
            &path,
            limits.mirror_bytes,
            "execution.memory_notice.mirror_bytes",
        )? {
            let mirror: Mirror =
                serde_json::from_slice(&bytes).map_err(|_| "invalid memory delivery mirror")?;
            host.validate_mirror(&mirror)?;
            host.mirror = Some(mirror);
        }
        Ok(host)
    }

    fn safe(&self, value: &impl Serialize) -> Result<(), String> {
        bounded_json_size(value, self.client.limits().transport.body_bytes)
            .map_err(|_| "memory notification data exceeds bound (transport.body_bytes)")?;
        let value =
            serde_json::to_value(value).map_err(|_| "memory notification encoding failed")?;
        if value_contains_credential(&value, &self.credentials, &self.client.limits().worker)
            .map_err(|reason| {
                format!("memory notification credential inspection exceeds bound: {reason}")
            })?
        {
            return Err("memory notification contains a configured credential".into());
        }
        Ok(())
    }

    fn cursor(&self, cursor: &AgentMemoryCursor) -> Result<(), String> {
        if !crate::mcp::valid_identity(
            &cursor.database_id,
            self.client.limits().transport.identity_bytes,
        ) || cursor.session_id != self.session_id
            || cursor.agent_id != self.agent_id
            || cursor.sequence > i64::MAX as u64
        {
            return Err("memory notification scope differs".into());
        }
        if let Some(mirror) = &self.mirror {
            if cursor.database_id != mirror.acknowledged_cursor.database_id {
                return Err("memory notification database differs".into());
            }
        }
        Ok(())
    }

    fn page(&self, page: &AgentMemoryPage) -> Result<(), String> {
        self.cursor(&page.from_cursor)?;
        self.cursor(&page.to_cursor)?;
        if page.from_cursor.database_id != page.to_cursor.database_id
            || page.to_cursor.sequence <= page.from_cursor.sequence
            || page.to_cursor.sequence > page.through_sequence
            || page.through_sequence > i64::MAX as u64
            || page.has_more != (page.to_cursor.sequence < page.through_sequence)
        {
            return Err("invalid bounded memory notification page".into());
        }
        if page.notices.len() > self.limits.keys {
            return Err(
                "invalid bounded memory notification page (execution.memory_notice.keys)".into(),
            );
        }
        let mut keys = BTreeSet::new();
        for notice in &page.notices {
            if !crate::mcp::valid_identity(
                &notice.key,
                self.client.limits().transport.identity_bytes,
            ) || notice.revision == 0
                || notice.revision > i64::MAX as u64
                || !keys.insert(&notice.key)
            {
                return Err("invalid memory notification hint".into());
            }
        }
        self.safe(page)
    }

    fn encode(&self, batch: &AgentMemoryBatch) -> Result<String, String> {
        self.page(&batch.page)?;
        if !crate::mcp::valid_identity(
            &batch.batch_id,
            self.client.limits().transport.identity_bytes,
        ) {
            return Err("invalid memory notification batch identity".into());
        }
        let text = serde_json::to_string(&json!({
            "type":"agent_memory_hints",
            "batch_id":batch.batch_id,
            "instruction":"Other sessions changed these shared agent memory keys. These are invalidation hints, not memory values. Call collab_memory_get for the current canonical value before relying on cached content.",
            "notices":batch.page.notices,
        })).map_err(|_| "memory notification encoding failed")?;
        // Bound its escaped representation as well as the model-visible bytes.
        bounded_json_size(&text, self.limits.notice_bytes).map_err(
            |_| "encoded memory notification exceeds bound (execution.memory_notice.notice_bytes)",
        )?;
        self.safe(&text)?;
        Ok(text)
    }

    fn validate_mirror(&self, mirror: &Mirror) -> Result<(), String> {
        if mirror.limits != self.limits
            || mirror.revision != 1
            || mirror.start_sequence > mirror.acknowledged_cursor.sequence
        {
            return Err("unsupported memory delivery mirror".into());
        }
        self.cursor(&mirror.acknowledged_cursor)?;
        if let Some(pending) = &mirror.pending {
            if pending.batch.page.from_cursor != mirror.acknowledged_cursor
                || pending.text != self.encode(&pending.batch)?
            {
                return Err("memory delivery mirror differs from exact batch".into());
            }
            if let Some(delivery) = &pending.delivery {
                delivery.caller.validate(&self.client.limits().transport)?;
                if delivery.caller.task_id != self.caller.task_id
                    || !crate::mcp::valid_identity(
                        &delivery.context,
                        self.client.limits().transport.identity_bytes,
                    )
                {
                    return Err("memory delivery context differs".into());
                }
            }
        }
        self.safe(mirror)
    }

}

fn scope_directory(state: &Path, session_id: &str) -> PathBuf {
    let encoded = component(session_id);
    let mut path = state.join("memory-notices");
    // Keep every filename below NAME_MAX without truncating identity or hashing.
    for chunk in encoded.as_bytes().chunks(96) {
        path.push(std::str::from_utf8(chunk).expect("hexadecimal component"));
    }
    path
}

fn read_private(path: &Path, limit: usize, field: &str) -> Result<Option<Vec<u8>>, String> {
    // Linux O_NONBLOCK | O_NOFOLLOW: a FIFO cannot stall the worker, and a final
    // symlink cannot redirect validation to a different file than the read.
    let file = match std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(0x800 | 0x20000)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("memory notification record unavailable".into()),
    };
    let metadata = file
        .metadata()
        .map_err(|_| "memory notification record metadata unavailable")?;
    let owner = std::fs::metadata("/proc/self")
        .map_err(|_| "memory notification owner unavailable")?
        .uid();
    if !metadata.is_file()
        || metadata.uid() != owner
        || metadata.nlink() != 1
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        return Err("memory notification record is not a bounded private regular file".into());
    }
    if metadata.len() > limit as u64 {
        return Err(format!(
            "memory notification record exceeds bound ({field})"
        ));
    }
    let mut bytes = Vec::new();
    file.take((limit + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| "memory notification record unreadable")?;
    if bytes.len() > limit {
        return Err(format!(
            "memory notification record exceeds bound ({field})"
        ));
    }
    Ok(Some(bytes))
}

#[cfg(test)]
#[path = "memory_notice_tests.rs"]
mod tests;

/// Test fixture for an already-retained, unacknowledged historical notice.
/// Runtime consumers must leave these bytes inert and unchanged.
pub(super) fn retain_fixture(state: &Path, caller: &Caller, session_id: &str,
    agent_id: &str, limits: Arc<crate::limits::RuntimeLimits>) -> (PathBuf, Vec<u8>) {
    let host=Host::open_with_credentials(ApiClient::new("http://127.0.0.1:1",limits.clone()).unwrap(),
        state,caller,session_id,agent_id,&limits.execution.memory_notice,vec![]).unwrap();
    let from=AgentMemoryCursor {database_id:"historical-database".into(),agent_id:agent_id.into(),session_id:session_id.into(),sequence:1};
    let mut to=from.clone();to.sequence=2;
    let batch=AgentMemoryBatch {batch_id:"historical-memory-batch".into(),page:AgentMemoryPage {
        from_cursor:from.clone(),to_cursor:to,through_sequence:2,
        notices:vec![crate::collaboration::AgentMemoryNotice {key:"other-session-key".into(),revision:1}],has_more:false,
    }};
    let mirror=Mirror {limits:host.limits.clone(),revision:1,start_sequence:1,acknowledged_cursor:from,
        pending:Some(Pending {text:host.encode(&batch).unwrap(),batch,
            delivery:Some(Delivery {caller:caller.clone(),context:"historical-context".into(),appended:false})})};
    host.validate_mirror(&mirror).unwrap();
    process::private_directory(&host.directory).unwrap();
    let path=host.directory.join("delivery.json");atomic_json(&path,&mirror).unwrap();
    let bytes=std::fs::read(&path).unwrap();(path,bytes)
}
