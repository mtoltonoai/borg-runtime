//! Mutable task coordination, separate from immutable execution prerequisites.
use super::{Page, workflow::*};
use crate::protocol::{Task, TaskStatus};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag="action", content="input", rename_all="snake_case", deny_unknown_fields)]
pub enum WorkMutation {
    AddWait { task_id: String, expected_revision: u64, resolver: ResolverRef },
    RemoveWait { task_id: String, expected_revision: u64, wait_id: String, reason: String },
    ChangeOwner { task_id: String, expected_revision: u64, owner: PrincipalRef },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag="query", content="input", rename_all="snake_case", deny_unknown_fields)]
pub enum WorkQuery {
    Get { task_id: String },
    Waits { task_id: String, after: Option<String>, limit: usize },
}
/// Logical state saved before the first wait. The assignee is the accepted
/// execution identity, not the independently mutable responsible record owner.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ResumeState {
    pub status: TaskStatus,
    pub assigned_agent_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag="kind", rename_all="snake_case", deny_unknown_fields)]
pub enum WorkState {
    Runnable { state: ResumeState },
    Waiting { prior: ResumeState },
    Terminal { status: TaskStatus },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag="kind", rename_all="snake_case", deny_unknown_fields)]
pub enum WaitStatus {
    Open,
    Resolved { resolution: ResolvedWait, actor: RecordedActor, operation_id: String },
    Removed { reason: String, actor: RecordedActor, operation_id: String, product_revision: u64 },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WorkflowWait {
    pub reference: WaitRef,
    pub resolver: ResolverRef,
    pub created_by: RecordedActor,
    pub operation_id: String,
    pub product_revision: u64,
    pub status: WaitStatus,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WorkView {
    /// One transaction's execution observation; no separate GetTask race.
    pub task: Box<Task>,
    /// Zero is an existing task with no coordination mutation yet.
    pub coordination_revision: u64,
    pub responsible_owner: PrincipalRef,
    pub state: WorkState,
    pub open_waits: usize,
    /// These are coordination blockers only, never an admission/launch promise.
    pub coordination_blocked: bool,
    pub waits: Page<WorkflowWait>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WorkMutationOutcome {
    pub view: WorkView,
    pub changed_wait: Option<WaitRef>,
    pub product_revision: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag="kind", content="value", rename_all="snake_case", deny_unknown_fields)]
pub enum WorkQueryResult { Task(WorkView), Waits(Page<WorkflowWait>) }
