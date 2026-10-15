//! Shared v2 task contract. No executor-owned result is an accepted task result.
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const WIRE_REVISION: u32 = 8;
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorKind {
    Coding,
    Native,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Queued,
    Active,
    Succeeded,
    Failed,
    Cancelled,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttemptPhase {
    Assigned,
    Running,
    StopRequested,
    Stopped,
}
/// The only currently selected inference settings. These values are explicit
/// definition data, not defaults for older definitions that omit the binding.
pub const SELECTED_MODEL: &str = "gpt-6-astra";
pub const SELECTED_REASONING_EFFORT: &str = "max";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InferenceSettings {
    pub model: String,
    pub reasoning_effort: String,
}
impl InferenceSettings {
    /// For a producer explicitly constructing a new selected definition.
    pub fn selected() -> Self {
        Self {
            model: SELECTED_MODEL.into(),
            reasoning_effort: SELECTED_REASONING_EFFORT.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
// Traceability scope: The serialized definition carries overrides; controller submission also makes its complete body immutable for each definition revision.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//# An agent definition MUST carry the execution-limit overrides specific to that agent revision.
pub struct Definition {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit_overrides: Option<Box<crate::limits::ExecutionLimitOverrides>>,
    /// Sole immutable provider/tool capability selection for this revision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_profile: Option<ExecutionProfile>,
    /// None preserves historical bytes and means unbound, never a model fallback.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inference: Option<InferenceSettings>,
    pub id: String,
    pub revision: String,
    pub executor: ExecutorKind,
}
impl Definition {
    /// Consumer launch and controller admission must both call this check.
    // Traceability scope: Admission and both consumers require explicit model and effort; historical absence decodes but never supplies a default.
    //= spec/impl/v2/implementation.md#definitions-select-execution
    //# An agent definition MUST specify the model used by its executor.
    //= spec/impl/v2/implementation.md#definitions-select-execution
    //# An agent definition MUST specify the reasoning effort used by its executor.
    pub fn required_inference(&self) -> Result<&InferenceSettings, &'static str> {
        let settings = self
            .inference
            .as_ref()
            .ok_or("template_inference_unbound")?;
        if settings.model != SELECTED_MODEL
            || settings.reasoning_effort != SELECTED_REASONING_EFFORT
        {
            return Err("template_inference_unsupported");
        }
        Ok(settings)
    }
}
/// One controller-issued instantiation of an immutable agent revision. Execution
/// state remains in its associated task and fenced attempts, not a second copy.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Session {
    pub id: String,
    pub agent_id: String,
    pub definition: Definition,
    pub task_id: String,
}
impl Session {
    /// Worker launch checks this exact accepted association before any effects.
    /// A retry changes the attempt, never the session or its definition.
    pub fn validate_for(&self, attempt: &Attempt, input: &TaskInput) -> Result<(), &'static str> {
        let bound = input
            .resolved_limits
            .as_ref()
            .ok_or("execution_limits_unbound")?
            .global_metadata
            .transport
            .identity_bytes;
        let valid = |s: &str| !s.is_empty() && s.len() <= bound && !s.chars().any(char::is_control);
        if !valid(&self.id)
            || !valid(&self.task_id)
            || !valid(&self.agent_id)
            || self.id == self.task_id
            || self.id == self.agent_id
            || self.id == attempt.id
            || self.agent_id != self.definition.id
            || self.definition != input.definition
            || attempt.session_id != self.id
            || attempt.task_id != self.task_id
        {
            return Err("session_binding_mismatch");
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AgentSummary {
    pub id: String,
    pub definition: Definition,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Repository {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registration: Option<crate::repository_catalogue::Revision>,
    /// Server-owned accepted endpoint projection; external submissions leave it empty.
    #[serde(default)]
    pub url: String,
    pub start_commit: String,
    #[serde(default)]
    pub checkpoint_remote: String,
}
/// Accepted read-only source identity. The host mapping, never this value,
/// selects a local bare path. This grants no checkpoint publication or commands.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RepositoryReadSource {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registration: Option<crate::repository_catalogue::Revision>,
    /// Server-owned accepted source endpoint.
    #[serde(default)]
    pub repository: String,
    pub start_commit: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub deadline_unix_ms: u64,
    pub model_requests: u32,
    pub output_bytes: usize,
    /// Bound for the complete accepted result, including checks and checkpoint.
    /// Distinct from the output-value and streamed-output bound.
    pub result_bytes: usize,
    pub max_delegation_depth: u32,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CodingTaskTemplate {
    pub instruction_prefix: String,
    pub definition: Definition,
    pub repository: Repository,
    pub acceptance_commands: Vec<Vec<String>>,
    pub dependencies: Vec<String>,
    pub limits: Limits,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeWorkflow {
    CodingThenContinue { child: CodingTaskTemplate },
    InspectCurrentTaskThenContinue {},
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
// Traceability scope: Configuration-described bounded behavior only, not a guarantee of model quality or a general programmable policy engine.
//= spec/impl/v2/implementation.md#definitions-select-execution
//# An agent's behavior MUST be described by configuration.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//# A task MUST have at most one primary repository association.
pub struct TaskInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_limits: Option<Box<crate::limits::LimitSnapshot>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository_read: Option<RepositoryReadSource>,

    pub instruction: String,
    pub definition: Definition,
    pub repository: Option<Repository>,
    pub acceptance_commands: Vec<Vec<String>>,
    pub output_schema: Option<Value>,
    pub limits: Limits,
    pub dependencies: Vec<String>,
    /// Explicit immutable task configuration, never inferred from model output.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_workflow: Option<Box<NativeWorkflow>>,
}
impl TaskInput {
    pub fn execution_limits(&self) -> Result<&crate::limits::ExecutionLimits, String> {
        let snapshot = self
            .resolved_limits
            .as_ref()
            .ok_or("execution limits are not bound at admission")?;
        snapshot.validate_for(self)?;
        Ok(&snapshot.values)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Checkpoint {
    pub registration: crate::repository_catalogue::Revision,
    pub repository: String,
    pub commit: String,
    pub remote_ref: String,
    pub operation_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AcceptedResult {
    pub attempt_id: String,
    pub summary: String,
    pub checkpoint: Option<Checkpoint>,
    pub validation: Vec<String>,
    pub output: Option<Value>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Attempt {
    pub id: String,
    pub session_id: String,
    pub task_id: String,
    pub generation: u64,
    pub worker_id: String,
    pub worker_incarnation: String,
    pub phase: AttemptPhase,
    pub deadline_unix_ms: u64,
    pub stopped: bool,
    pub uncertain: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Task {
    pub id: String,
    pub session_id: String,
    pub input: TaskInput,
    pub assigned_agent_id: String,
    pub status: TaskStatus,
    pub revision: u64,
    pub attempt: Option<Attempt>,
    pub selected_checkpoint: Option<Checkpoint>,
    pub result: Option<AcceptedResult>,
    pub blocked_reason: Option<String>,
    pub progress: String,
    pub recent_output: String,
    pub output_truncated: bool,
    pub parent_task_id: Option<String>,
    pub waiting_child_id: Option<String>,
    pub completed_child: Option<ChildOutcome>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChildOutcome {
    pub task_id: String,
    pub status: TaskStatus,
    pub result: Option<AcceptedResult>,
    pub summary: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Worker {
    pub id: String,
    pub incarnation: String,
    pub coding_capacity: usize,
    pub native_capacity: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkerCommand {
    RunAttempt {
        command_id: String,
        session: Session,
        attempt: Attempt,
        task: TaskInput,
        checkpoint: Option<Checkpoint>,
        continuation: Option<ChildOutcome>,
    },
    StopAttempt {
        command_id: String,
        task_id: String,
        attempt_id: String,
        generation: u64,
        incarnation: String,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Observation {
    NativeLoopCompleted {
        model_requests: u32,
        calls: Vec<NativeCallEvidence>,
    },
    Started {
        thread_id: Option<String>,
        turn_id: Option<String>,
    },
    Progress {
        text: String,
        output: String,
        truncated: bool,
    },
    ExecutorOutcome {
        success: bool,
        summary: String,
    },
    ProcessTreeStopped,
    ExecutionUncertain {
        message: String,
    },
    Usage {
        model_requests: u32,
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
    },
    ResultCandidate {
        checks_passed: bool,
        summary: String,
        checkpoint: Option<Checkpoint>,
        validation: Vec<String>,
        output: Option<Value>,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Report {
    pub report_id: String,
    pub worker_id: String,
    pub incarnation: String,
    pub task_id: String,
    pub attempt_id: String,
    pub generation: u64,
    pub observation: Observation,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OperationRecord {
    pub id: String,
    pub task_id: String,
    pub attempt_id: String,
    pub generation: u64,
    pub kind: String,
    pub intended: Value,
    pub state: String,
    pub outcome: Option<Value>,
}
/// Artifact identity reported during preparation. The controller binds it to an
/// accepted source checkpoint; these fields do not prove protected validation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReleaseArtifact {
    pub format: String,
    pub package_sha256: String,
    pub package_bytes: u64,
    pub manifest_sha256: String,
    pub source_commit: String,
    pub source_tree: String,
    pub object_format: String,
    pub binary_sha256: String,
    pub binary_bytes: u64,
    /// Compiler/platform target, not a deployment target or rollout scope.
    pub target: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactQualification {
    Claimed,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProtectedStatus {
    Unconfigured,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReleaseQualification {
    pub artifact: ArtifactQualification,
    pub repository_submission: ProtectedStatus,
    pub checks: ProtectedStatus,
    pub human_rollout: ProtectedStatus,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReleaseCandidate {
    pub id: String,
    pub task_id: String,
    pub task_revision: u64,
    pub accepted_attempt_id: String,
    pub checkpoint: Checkpoint,
    pub artifact: ReleaseArtifact,
    pub revision: u64,
    pub qualification: ReleaseQualification,
}
/// Ordinary preparation input deliberately has no acceptance, approval or
/// rollout fields. Unknown fields are rejected, not interpreted as authority.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PrepareReleaseInput {
    pub operation_id: String,
    pub task_id: String,
    pub expected_task_revision: u64,
    pub checkpoint: Checkpoint,
    pub artifact: ReleaseArtifact,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReconcileResultInput {
    pub operation_id: String,
    pub original_report_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryValue {
    pub agent_id: String,
    pub key: String,
    pub revision: u64,
    pub value: Value,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", content = "input", rename_all = "snake_case")]
pub enum Operation {
    RegisterRepository {
        operation_id: String,
        registration: crate::repository_catalogue::Registration,
    },
    ReviseRepository {
        operation_id: String,
        expected_generation: u64,
        registration: crate::repository_catalogue::Registration,
    },
    SetRepositoryEnabled {
        operation_id: String,
        repository_id: String,
        expected_generation: u64,
        enabled_for_new_tasks: bool,
    },
    ListRepositories {
        after: Option<String>,
        limit: usize,
    },
    GetRepository {
        repository_id: String,
        registration_revision: Option<String>,
    },
    // Proposed additions to protocol::Operation. Exact current caller required.
    ObserveAgentMemory {
        caller: crate::mcp::Caller,
        limit: usize,
    },
    PrepareAgentMemory {
        operation_id: String,
        caller: crate::mcp::Caller,
        expected_cursor: crate::collaboration::AgentMemoryCursor,
        through_sequence: u64,
        limit: usize,
    },
    AcknowledgeAgentMemory {
        operation_id: String,
        caller: crate::mcp::Caller,
        batch_id: String,
    },
    // Data::AgentMemory(crate::collaboration::AgentMemoryObservation)
    // Only Prepare and Acknowledge have mutation_id; Observe never writes.
    CallCollaboration {
        call: crate::mcp::Invocation,
    },
    GetCollaborationCall {
        caller: crate::mcp::Caller,
        operation_id: String,
    },
    QueryCollaboration {
        query: crate::collaboration::Query,
    },
    PrepareRelease(PrepareReleaseInput),
    GetProtectedRelease {
        release_id: String,
    },
    GetRelease {
        release_id: String,
    },
    ListReleases {
        task_id: String,
        after: Option<String>,
        limit: usize,
    },
    SubmitTask {
        operation_id: String,
        input: TaskInput,
    },
    ListAgents {
        after: Option<String>,
        limit: usize,
    },
    GetAgent {
        agent_id: String,
    },
    ListAgentTasks {
        agent_id: String,
        after: Option<String>,
        limit: usize,
    },
    GetTask {
        task_id: String,
    },
    ListTasks {
        after: Option<String>,
        limit: usize,
    },
    AssignTask {
        operation_id: String,
        task_id: String,
        expected_revision: u64,
        agent_id: String,
    },
    CancelTask {
        operation_id: String,
        task_id: String,
        expected_revision: u64,
    },
    RetryTask {
        operation_id: String,
        task_id: String,
        expected_revision: u64,
    },
    DelegateTask {
        operation_id: String,
        parent_task_id: String,
        parent_attempt_id: String,
        generation: u64,
        child: TaskInput,
    },
    RegisterWorker {
        operation_id: String,
        worker: Worker,
    },
    ObserveWorker {
        report: Report,
    },
    /// Re-evaluate acceptance of an immutable, previously refused result.
    /// Carries no replacement result and never starts executor/provider work.
    ReconcileResult(ReconcileResultInput),
    AwaitCommands {
        operation_id: String,
        worker_id: String,
        incarnation: String,
        acknowledged: Vec<String>,
    },
    PrepareOperation {
        operation_id: String,
        task_id: String,
        attempt_id: String,
        generation: u64,
        kind: String,
        intended: Value,
    },
    SettleOperation {
        settlement_id: String,
        operation_id: String,
        task_id: String,
        attempt_id: String,
        generation: u64,
        outcome: Value,
        checkpoint: Option<Checkpoint>,
    },
    GetOperation {
        operation_id: String,
    },
    GetMemory {
        agent_id: String,
        key: String,
    },
    SetMemory {
        operation_id: String,
        task_id: String,
        attempt_id: String,
        generation: u64,
        key: String,
        expected_revision: Option<u64>,
        value: Value,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub protocol_revision: u32,
    pub request_id: String,
    #[serde(flatten)]
    pub command: Operation,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Data {
    RepositoryRegistration(crate::repository_catalogue::Entry),
    RepositoryRegistrations(crate::collaboration::Page<crate::repository_catalogue::Entry>),
    AgentMemory(crate::collaboration::AgentMemoryObservation),
    Repository(Box<crate::repository_view::Output>),
    Collaboration(crate::collaboration::QueryResult),
    CollaborationPage(crate::mcp::ListPage),
    ProtectedRelease(Box<crate::protected_release::StageView>),
    Release(Box<ReleaseCandidate>),
    Releases(Vec<ReleaseCandidate>),
    Agent(AgentSummary),
    Agents(Vec<AgentSummary>),
    Task(Box<Task>),
    Tasks(Vec<Task>),
    Worker(Worker),
    Commands(Vec<WorkerCommand>),
    Operation(OperationRecord),
    Memory(Option<MemoryValue>),
    Acknowledged,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Outcome {
    Ok {
        data: Data,
    },
    Refused {
        code: String,
        message: String,
    },
    Pending {
        operation_id: String,
    },
    Uncertain {
        operation_id: String,
        message: String,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MutationCommit {
    pub operation_id: String,
    pub controller_revision: u64,
}

impl Operation {
    pub fn mutation_id(&self) -> Option<&str> {
        match self {
            Self::RegisterRepository { operation_id, .. }
            | Self::ReviseRepository { operation_id, .. }
            | Self::SetRepositoryEnabled { operation_id, .. } => Some(operation_id),
            Self::CallCollaboration { call } => Some(&call.operation_id),
            Self::PrepareRelease(input) => Some(&input.operation_id),
            Self::ReconcileResult(input) => Some(&input.operation_id),
            Self::PrepareAgentMemory { operation_id, .. }
            | Self::AcknowledgeAgentMemory { operation_id, .. }
            | Self::SubmitTask { operation_id, .. }
            | Self::AssignTask { operation_id, .. }
            | Self::CancelTask { operation_id, .. }
            | Self::RetryTask { operation_id, .. }
            | Self::DelegateTask { operation_id, .. }
            | Self::PrepareOperation { operation_id, .. }
            | Self::SetMemory { operation_id, .. }
            | Self::RegisterWorker { operation_id, .. }
            | Self::AwaitCommands { operation_id, .. } => Some(operation_id),
            Self::ObserveWorker { report } => Some(&report.report_id),
            Self::SettleOperation { settlement_id, .. } => Some(settlement_id),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Reply {
    pub request_id: String,
    pub commit: Option<MutationCommit>,
    pub observed_controller_revision: Option<u64>,
    #[serde(flatten)]
    pub outcome: Outcome,
}
impl Reply {
    pub fn ok(id: impl Into<String>, data: Data) -> Self {
        Self {
            request_id: id.into(),
            commit: None,
            observed_controller_revision: None,
            outcome: Outcome::Ok { data },
        }
    }
    pub fn refused(
        id: impl Into<String>,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            request_id: id.into(),
            commit: None,
            observed_controller_revision: None,
            outcome: Outcome::Refused {
                code: code.into(),
                message: message.into(),
            },
        }
    }

    /// Validate the supplied controller context, not fresh execution authority.
    /// The original receipt revision and a later read revision are distinct.
    pub fn validate_for(&self, operation: &Operation) -> Result<(), String> {
        if let Outcome::Pending { operation_id } | Outcome::Uncertain { operation_id, .. } =
            &self.outcome
        {
            let requested = operation.mutation_id().or_else(|| match operation {
                Operation::GetOperation { operation_id }
                | Operation::GetCollaborationCall { operation_id, .. } => {
                    Some(operation_id.as_str())
                }
                _ => None,
            });
            if requested != Some(operation_id.as_str()) {
                return Err("unresolved outcome identity differs from request".into());
            }
        }
        if self
            .observed_controller_revision
            .is_some_and(|revision| revision > i64::MAX as u64)
        {
            return Err("unsupported observed controller revision".into());
        }
        let receipt_lookup = match (&self.outcome, operation) {
            (
                Outcome::Ok {
                    data: Data::Operation(record),
                },
                Operation::GetOperation { operation_id }
                | Operation::GetCollaborationCall { operation_id, .. },
            ) if record.kind == "application_mutation" => {
                if record.id != *operation_id || record.state != "settled" {
                    return Err("mutation lookup identity or state differs".into());
                }
                let original: Operation = serde_json::from_value(record.intended.clone())
                    .map_err(|_| "invalid original mutation in lookup")?;
                if original.mutation_id() != Some(operation_id.as_str())
                    || serde_json::to_value(&original).map_err(|_| "invalid original mutation")?
                        != record.intended
                {
                    return Err("mutation lookup original identity differs".into());
                }
                let outcome: Outcome = serde_json::from_value(
                    record
                        .outcome
                        .clone()
                        .ok_or("mutation lookup lacks outcome")?,
                )
                .map_err(|_| "invalid original mutation outcome")?;
                if !matches!(outcome, Outcome::Ok { .. } | Outcome::Refused { .. }) {
                    return Err("mutation lookup outcome is not committed".into());
                }
                if Some(serde_json::to_value(&outcome).map_err(|_| "invalid original outcome")?)
                    != record.outcome
                {
                    return Err("mutation lookup outcome contains unexpected fields".into());
                }
                Some(operation_id.as_str())
            }
            _ => None,
        };
        if let Some(commit) = &self.commit {
            if commit.operation_id.is_empty()
                || commit.operation_id.chars().any(char::is_control)
                || commit.controller_revision == 0
                || commit.controller_revision > i64::MAX as u64
                || !self
                    .observed_controller_revision
                    .is_some_and(|revision| revision >= commit.controller_revision)
                || !matches!(self.outcome, Outcome::Ok { .. } | Outcome::Refused { .. })
                || operation.mutation_id().or(receipt_lookup) != Some(commit.operation_id.as_str())
            {
                return Err("invalid committed mutation context".into());
            }
        } else if receipt_lookup.is_some()
            || (operation.mutation_id().is_some() && matches!(self.outcome, Outcome::Ok { .. }))
        {
            return Err("successful mutation lacks its committed context".into());
        }
        if matches!(self.outcome, Outcome::Ok { .. }) && self.observed_controller_revision.is_none()
        {
            return Err("successful reply lacks observed controller revision".into());
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn committed_context_is_original_and_separate_from_observed_revision() {
        let operation = Operation::CancelTask {
            operation_id: "cancel-1".into(),
            task_id: "task-1".into(),
            expected_revision: 2,
        };
        let mut reply = Reply::refused("transport-9", "stale_revision", "recorded refusal");
        reply.commit = Some(MutationCommit {
            operation_id: "cancel-1".into(),
            controller_revision: 3,
        });
        reply.observed_controller_revision = Some(19);
        assert!(reply.validate_for(&operation).is_ok());
        assert_eq!(
            serde_json::from_slice::<Reply>(&serde_json::to_vec(&reply).unwrap()).unwrap(),
            reply
        );
        for revision in [0, i64::MAX as u64 + 1] {
            let mut bad = reply.clone();
            bad.commit.as_mut().unwrap().controller_revision = revision;
            assert!(bad.validate_for(&operation).is_err());
        }
        let mut bad = reply.clone();
        bad.commit.as_mut().unwrap().operation_id = "other".into();
        assert!(bad.validate_for(&operation).is_err());
        bad = reply.clone();
        bad.observed_controller_revision = Some(2);
        assert!(bad.validate_for(&operation).is_err());
        bad = reply.clone();
        bad.outcome = Outcome::Pending {
            operation_id: "cancel-1".into(),
        };
        assert!(bad.validate_for(&operation).is_err());
        // A temporary refusal has no durable stamp. Its caller must reconcile,
        // rather than treating the absence as evidence that a prior send failed.
        assert!(
            Reply::refused("r", "capacity", "queue full")
                .validate_for(&operation)
                .is_ok()
        );
        let mut missing = Reply::ok("r", Data::Acknowledged);
        missing.observed_controller_revision = Some(19);
        assert!(missing.validate_for(&operation).is_err());
    }

    #[test]
    fn receipt_lookup_context_cannot_be_borrowed_by_live_effect_state() {
        let original = Operation::CancelTask {
            operation_id: "cancel-1".into(),
            task_id: "task-1".into(),
            expected_revision: 2,
        };
        let query = Operation::GetOperation {
            operation_id: "cancel-1".into(),
        };
        let record = OperationRecord {
            id: "cancel-1".into(),
            task_id: "task-1".into(),
            attempt_id: String::new(),
            generation: 0,
            kind: "application_mutation".into(),
            intended: serde_json::to_value(original).unwrap(),
            state: "settled".into(),
            outcome: Some(
                serde_json::to_value(Outcome::Refused {
                    code: "stale_revision".into(),
                    message: "original".into(),
                })
                .unwrap(),
            ),
        };
        let mut reply = Reply::ok("lookup", Data::Operation(record));
        reply.observed_controller_revision = Some(8);
        assert!(reply.validate_for(&query).is_err());
        reply.commit = Some(MutationCommit {
            operation_id: "cancel-1".into(),
            controller_revision: 4,
        });
        assert!(reply.validate_for(&query).is_ok());
        let mut extra = reply.clone();
        let Outcome::Ok {
            data: Data::Operation(ref mut record),
        } = extra.outcome
        else {
            unreachable!()
        };
        record
            .outcome
            .as_mut()
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert(
                "commit".into(),
                serde_json::json!({"operation_id":"other","controller_revision":2}),
            );
        assert!(extra.validate_for(&query).is_err());
        let Outcome::Ok {
            data: Data::Operation(ref mut record),
        } = reply.outcome
        else {
            unreachable!()
        };
        record.kind = "checkpoint".into();
        assert!(reply.validate_for(&query).is_err());
        reply.commit = None;
        assert!(reply.validate_for(&query).is_ok());
        reply.observed_controller_revision = None;
        assert!(reply.validate_for(&query).is_err());
    }

    #[test]
    fn pending_identity_cannot_retarget_a_mutation_or_a_read() {
        let operation = Operation::CancelTask {
            operation_id: "original".into(),
            task_id: "task".into(),
            expected_revision: 1,
        };
        for outcome in [
            Outcome::Pending {
                operation_id: "other".into(),
            },
            Outcome::Uncertain {
                operation_id: "other".into(),
                message: "unknown".into(),
            },
        ] {
            let mut reply = Reply {
                request_id: "transport".into(),
                commit: None,
                observed_controller_revision: None,
                outcome,
            };
            assert!(reply.validate_for(&operation).is_err());
            reply.outcome = Outcome::Pending {
                operation_id: "original".into(),
            };
            assert!(reply.validate_for(&operation).is_ok());
            assert!(
                reply
                    .validate_for(&Operation::GetTask {
                        task_id: "original".into()
                    })
                    .is_err()
            );
        }
    }

    #[test]
    fn shared_wire_roundtrip() {
        let r = Request {
            protocol_revision: WIRE_REVISION,
            request_id: "request-1".into(),
            command: Operation::GetTask {
                task_id: "task-1".into(),
            },
        };
        let value = serde_json::to_vec(&r).unwrap();
        let back: Request = serde_json::from_slice(&value).unwrap();
        assert_eq!(back.request_id, r.request_id);
    }
    #[test]
    fn unknown_operation_refuses() {
        assert!(serde_json::from_str::<Request>(r#"{"protocol_revision":1,"request_id":"r","operation":"grant_everything","input":{}}"#).is_err());
    }
    #[test]
    fn result_reconciliation_accepts_only_original_receipt_identity() {
        let request = serde_json::json!({
            "protocol_revision": WIRE_REVISION,
            "request_id": "request",
            "operation": "reconcile_result",
            "input": {"operation_id":"reconcile-once", "original_report_id":"original-result"}
        });
        let parsed: Request = serde_json::from_value(request.clone()).unwrap();
        assert!(
            matches!(parsed.command, Operation::ReconcileResult(ref input)
            if input.operation_id == "reconcile-once" && input.original_report_id == "original-result")
        );
        assert_eq!(serde_json::to_value(parsed).unwrap(), request);
        for field in [
            "report",
            "output",
            "checks_passed",
            "approved",
            "generation",
        ] {
            let mut replaced = request.clone();
            replaced["input"][field] = serde_json::json!(true);
            assert!(serde_json::from_value::<Request>(replaced).is_err());
        }
    }

    #[test]
    // Traceability scope: Existing decoder negatives remove required Register/Await/Settle mutation IDs; transport correlation and the original effect ID do not substitute.
    //= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
    //= type=test
    //# A mutating request MUST carry an operation identity.
    fn lifecycle_mutations_require_their_own_identity() {
        for (operation, input, field) in [
            (
                "register_worker",
                serde_json::json!({
                    "operation_id":"register-once", "worker":{
                        "id":"worker", "incarnation":"incarnation", "coding_capacity":1, "native_capacity":1
                    }
                }),
                "operation_id",
            ),
            (
                "await_commands",
                serde_json::json!({
                    "operation_id":"exchange-once", "worker_id":"worker", "incarnation":"incarnation", "acknowledged":[]
                }),
                "operation_id",
            ),
            (
                "settle_operation",
                serde_json::json!({
                    "settlement_id":"observation-once", "operation_id":"original-effect",
                    "task_id":"task", "attempt_id":"attempt", "generation":1,
                    "outcome":{"acknowledged":false}, "checkpoint":null
                }),
                "settlement_id",
            ),
        ] {
            let value = serde_json::json!({
                "protocol_revision":WIRE_REVISION, "request_id":"transport-correlation",
                "operation":operation, "input":input
            });
            let parsed: Request = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(serde_json::to_value(parsed).unwrap(), value);
            let mut missing = value.clone();
            missing["input"].as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<Request>(missing).is_err(),
                "{operation}"
            );
        }
        let value = serde_json::json!({
            "protocol_revision":WIRE_REVISION, "request_id":"transport-correlation",
            "operation":"settle_operation", "input":{
                "settlement_id":"observation-once", "operation_id":"original-effect",
                "task_id":"task", "attempt_id":"attempt", "generation":1,
                "outcome":{"acknowledged":false}, "checkpoint":null
            }
        });
        let parsed: Request = serde_json::from_value(value).unwrap();
        assert!(
            matches!(parsed.command, Operation::SettleOperation { settlement_id, operation_id, .. }
            if settlement_id == "observation-once" && operation_id == "original-effect")
        );
    }
}

pub fn bounded_json_size<T: serde::Serialize>(value: &T, max: usize) -> Result<usize, String> {
    struct Count {
        bytes: usize,
        max: usize,
    }
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let n = self
                .bytes
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("size overflow"))?;
            if n > self.max {
                return Err(std::io::Error::other("JSON exceeds bound"));
            }
            self.bytes = n;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count { bytes: 0, max };
    serde_json::to_writer(&mut count, value).map_err(|e| e.to_string())?;
    Ok(count.bytes)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeSearchProfile {
    pub max_tool_calls: u32,
}
/// Accepted immutable tool profile. Absence is the original legacy workflow.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutionProfile {
    CollaborationV1 {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        web_search: Option<NativeSearchProfile>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeCallEvidence {
    pub call_index: u32,
    /// One-based model request which returned this single owned function call.
    pub request_index: u32,
    pub operation_id: String,
    pub logical_call_id: String,
}
