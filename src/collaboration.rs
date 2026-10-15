//! Read projections over the existing collaboration/controller store.
//! These values are observations, never a grant, launch command or protected check.
use crate::{protected_release::Check, protocol::*};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "query",
    content = "input",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Query {
    Repositories {
        after: Option<String>,
        limit: usize,
    },
    RepositoryRegistration {
        repository_id: String,
        registration_revision: Option<String>,
    },
    Limits {
        task_id: Option<String>,
    },
    Session {
        session_id: String,
    },
    Sessions {
        agent_id: Option<String>,
        after: Option<String>,
        limit: usize,
    },
    Activity {
        after: Option<u64>,
        limit: usize,
    },
    Tasks {
        agent_id: Option<String>,
        parent_task_id: Option<String>,
        after: Option<String>,
        limit: usize,
    },
    Call {
        task_id: String,
        operation_id: String,
    },
    Repository {
        task_id: String,
        request: crate::repository_view::Request,
    },
    Definition {
        definition_id: String,
        revision: String,
    },
    MemoryKeys {
        agent_id: String,
        after: Option<String>,
        limit: usize,
    },
    Workers {
        after: Option<String>,
        limit: usize,
    },
    Worker {
        worker_id: String,
    },
    TaskHistory {
        task_id: String,
        after: Option<String>,
        limit: usize,
    },
    TaskAttempts {
        task_id: String,
        after: Option<String>,
        limit: usize,
    },
    TaskCheckpoints {
        task_id: String,
        after: Option<String>,
        limit: usize,
    },
    ReleaseChecks {
        release_id: String,
        after: Option<String>,
        limit: usize,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Page<T> {
    pub items: Vec<T>,
    /// None is the end observed by this query, not a snapshot across later pages.
    pub next_cursor: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MemoryKey {
    pub key: String,
    pub revision: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WorkerCapacity {
    pub worker: Worker,
    /// Controller-known not-stopped ownership for this registered incarnation.
    pub coding_occupied: u64,
    pub native_occupied: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct HistoryEntry {
    pub sequence: u64,
    pub task_revision: u64,
    pub kind: String,
    pub body: Value,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExecutorOutcomeView {
    pub success: bool,
    pub summary: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AttemptView {
    pub attempt: Attempt,
    pub executor_outcome: Option<ExecutorOutcomeView>,
    pub reported_model_requests: u32,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CheckpointPublication {
    pub operation_id: String,
    pub attempt_id: String,
    pub generation: u64,
    pub checkpoint: Checkpoint,
    pub state: String,
    pub outcome: Option<CheckpointOutcome>,
    pub selected: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CheckpointOutcome {
    pub acknowledged: Option<bool>,
    pub not_applied: Option<bool>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReleaseCheck {
    pub operation_id: String,
    pub producer_id: String,
    pub controller_revision: u64,
    pub check: Check,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum QueryResult {
    Repositories(Page<crate::repository_catalogue::Entry>),
    RepositoryRegistration(crate::repository_catalogue::Entry),
    Limits {
        global: Box<crate::limits::RuntimeLimits>,
        execution: Option<Box<crate::limits::LimitSnapshot>>,
    },
    Session(Session),
    Sessions(Page<Session>),
    Activity(ActivityPage),
    Tasks(crate::mcp::ListPage),
    Call(Box<CallRecord>),
    Repository(Box<crate::repository_view::Output>),
    Definition(Definition),
    MemoryKeys(Page<MemoryKey>),
    Workers(Page<WorkerCapacity>),
    Worker(WorkerCapacity),
    TaskHistory(Page<HistoryEntry>),
    TaskAttempts(Page<AttemptView>),
    TaskCheckpoints(Page<CheckpointPublication>),
    ReleaseChecks(Page<ReleaseCheck>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ActivityEntry {
    pub sequence: u64,
    pub task_id: String,
    pub task_revision: u64,
    pub kind: String,
    pub body: Value,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ActivityPage {
    pub items: Vec<ActivityEntry>,
    pub cursor: u64,
    pub has_more: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CallRecord {
    pub invocation: crate::mcp::Invocation,
    pub result: crate::mcp::CallResult,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Change {
    pub revision: u64,
    pub changed: bool,
    pub reset: bool,
}

// Host-only typed memory observations; no model-selected agent or session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentMemoryCursor {
    pub database_id: String,
    pub agent_id: String,
    pub session_id: String,
    pub sequence: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentMemoryNotice {
    pub key: String,
    pub revision: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentMemoryPage {
    pub from_cursor: AgentMemoryCursor,
    pub to_cursor: AgentMemoryCursor,
    pub through_sequence: u64,
    pub notices: Vec<AgentMemoryNotice>,
    pub has_more: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentMemoryBatch {
    pub batch_id: String,
    pub page: AgentMemoryPage,
}
impl AgentMemoryBatch {
    /// Existing durable identity encoding, shared by allocation and size probing.
    /// The decimal padding is part of the persisted wire representation, not a
    /// configurable policy bound. Callers retain their existing range checks.
    pub(crate) fn identifier(database_id: &str, sequence: u64) -> String {
        format!("memory-batch-{database_id}-{sequence:020}")
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentMemoryObservation {
    pub start_sequence: u64,
    pub acknowledged_cursor: AgentMemoryCursor,
    pub pending: Option<AgentMemoryBatch>,
    pub available: Option<AgentMemoryPage>,
}
