//! Shared v2 task contract. No executor-owned result is an accepted task result.
#[path = "protocol_identity.rs"]
mod identity;
pub use identity::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
pub use crate::native_loop_contract::NativeRequestEvidence;

pub const WIRE_REVISION: u32 = 24;
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
    /// Durable native retry hold, never automatically scheduled.
    Blocked,
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
    /// Original immutable charter source; absent historical definitions stay absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub charter: Option<crate::charter::DefinitionCharter>,
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
/// Closed request carried inside the ordinary collaboration occurrence.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DefinitionUpdate {
    pub agent_id: String,
    pub expected_revision: String,
    pub expected_global_revision: String,
    pub expected_policy_revision: String,
    pub changes: std::collections::BTreeMap<crate::limits::DefinitionLimitField, usize>,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConfigurationChangeClass { Neutral, Narrowing, Widening, PreviousUnspecified }
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConfigurationEffect { FutureSubmissions }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DefinitionUpdateOutcome {
    pub previous_revision: String,
    pub selected_definition: Definition,
    pub global_revision: String,
    pub policy: crate::limits::DefinitionUpdatePolicy,
    pub changed_fields: Vec<crate::limits::DefinitionLimitField>,
    pub classification: ConfigurationChangeClass,
    pub effective_for: ConfigurationEffect,
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
    /// Retirement disables new use, not historical identity or accepted work.
    #[serde(default, skip_serializing_if = "agent_is_active")]
    pub retired: bool,
}
fn agent_is_active(retired: &bool) -> bool { !*retired }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Repository {
    /// Server-copied immutable target; external submissions leave it absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pull_request_target: Option<crate::pull_request::Target>,
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
impl RepositoryReadSource {
    /// Shape validation for an already resolved instruction binding. This does
    /// not resolve a catalogue head, authorize a caller, or acquire material.
    pub fn validate_instruction_binding(
        &self,
        limits: &crate::limits::RuntimeLimits,
    ) -> Result<(), String> {
        let binding = self.registration.as_ref().ok_or("instruction_source_registration_missing")?;
        for identity in [&binding.repository_id, &binding.registration_revision] {
            if identity.is_empty() || identity.len() > limits.transport.identity_bytes
                || identity.chars().any(char::is_control)
            {
                return Err("instruction_source_identity_invalid: transport.identity_bytes".into());
            }
        }
        if self.repository.is_empty() || self.repository.chars().any(char::is_control)
            || crate::credential::repository_contains_credentials(&self.repository)
        {
            return Err("instruction_source_endpoint_invalid".into());
        }
        if self.repository.len() > limits.controller.max_repository_identity_bytes {
            return Err("instruction_source_endpoint_bound: controller.max_repository_identity_bytes".into());
        }
        if self.repository.len() > limits.repository.endpoint_bytes {
            return Err("instruction_source_endpoint_bound: repository.endpoint_bytes".into());
        }
        if !matches!(self.start_commit.len(), 40 | 64)
            || !self.start_commit.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err("instruction_source_commit_invalid".into());
        }
        Ok(())
    }
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
    /// Immutable secondary read scope, resolved when the parent is accepted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruction_source: Option<RepositoryReadSource>,
    /// Explicit requested result; absence retains checkpoint-only behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pull_request: Option<crate::pull_request::Request>,
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
    /// Explicit same-task handoff scope; never creates a delegated child.
    Reassign { target: CodingTaskTemplate },
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
    /// Optional Coding instruction tree; distinct from its primary repository.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruction_source: Option<RepositoryReadSource>,
    /// Explicit requested result; never inferred from the instruction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pull_request: Option<crate::pull_request::Request>,
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
/// Root submission draft. Omission chooses configured routing; explicit null is invalid.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TaskDraft {
    /// Optional Coding instruction tree; distinct from its primary repository.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruction_source: Option<RepositoryReadSource>,
    /// Explicit requested result; never inferred from the instruction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pull_request: Option<crate::pull_request::Request>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository_read: Option<RepositoryReadSource>,

    pub instruction: String,
    #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "present_definition")]
    pub definition: Option<Definition>,
    pub repository: Option<Repository>,
    pub acceptance_commands: Vec<Vec<String>>,
    pub output_schema: Option<Value>,
    pub limits: Limits,
    pub dependencies: Vec<String>,
    /// Explicit immutable task configuration, never inferred from model output.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_workflow: Option<Box<NativeWorkflow>>,
}
impl TaskDraft {
    /// Admission supplies the exact selected head; accepted tasks never retain an omission.
    pub(crate) fn resolve(&self, definition: Definition) -> TaskInput {
        TaskInput {
            resolved_limits: None,
            instruction_source: self.instruction_source.clone(),
            pull_request: self.pull_request.clone(),
            repository_read: self.repository_read.clone(),
            instruction: self.instruction.clone(),
            definition,
            repository: self.repository.clone(),
            acceptance_commands: self.acceptance_commands.clone(),
            output_schema: self.output_schema.clone(),
            limits: self.limits.clone(),
            dependencies: self.dependencies.clone(),
            native_workflow: self.native_workflow.clone(),
        }
    }
}

fn present_definition<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Definition>, D::Error> {
    Definition::deserialize(d).map(Some)
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
    /// Controller-derived reference to the exact settled original PR operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pull_request: Option<crate::pull_request::ResultReference>,
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
#[serde(deny_unknown_fields)]
pub struct PendingAssignment {
    pub operation_id: String,
    pub from_session_id: String,
    /// Reserved identity only. No session authority exists until activation.
    pub session: Session,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Task {
    pub id: String,
    pub session_id: String,
    pub input: TaskInput,
    pub assigned_agent_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_assignment: Option<PendingAssignment>,
    pub status: TaskStatus,
    pub revision: u64,
    pub attempt: Option<Attempt>,
    pub selected_checkpoint: Option<Checkpoint>,
    pub result: Option<AcceptedResult>,
    pub blocked_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_retry_block: Option<crate::native::NativeRetryBlock>,
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        workflow_continuation: Option<crate::collaboration::workflow::WorkflowContinuation>,
        /// Original held decision carried by a fresh ordinary attempt; not input ACK.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        completion_continuation: Option<crate::governance_completion::CompletionResume>,
    },
    CollaborationReady {
        command_id: String,
        caller: crate::mcp::Caller,
        through: crate::collaboration::workflow::EventId,
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
    /// Immutable facts for one acknowledged native request reservation. This
    /// report does not authorize retry or change the current attempt's owner.
    NativeRequestDiagnostic {
        request_index: u32,
        endpoint: String,
        model: String,
        request_binding: Option<crate::native::ProviderRequestBinding>,
        observed_outcome: crate::native_loop_contract::NativeRequestObservedOutcome,
        failure: Option<crate::native::ProviderFailureDiagnostic>,
    },
    NativeRequestRejected {
        request_index: u32, endpoint: String, model: String,
        rejection: crate::native::ProviderRejection, diagnostic: String,
        request_binding: crate::native::ProviderRequestBinding,
    },
    NativeOutputCapped {
        request_index: u32,
        undispatched_calls: u32,
        request_binding: crate::native::ProviderRequestBinding,
        output_binding: crate::native::ProviderRequestBinding,
    },
    NativeFormatFailure {
        request_index: u32,
        issue: crate::native::FormatIssue,
        request_binding: crate::native::ProviderRequestBinding,
        output_binding: crate::native::ProviderRequestBinding,
    },
    NativeRetryBlocked { block: crate::native::NativeRetryBlock },
    NativeLoopCompleted {
        model_requests: u32,
        calls: Vec<NativeCallEvidence>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        requests: Vec<NativeRequestEvidence>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        final_response_index: Option<u32>,
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
        /// Present only on the original new native request charge. Historical
        /// absent/false receipts keep their bytes and acquire no synthetic credit.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        native_diagnostic_reserved: bool,
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
    AdministerIdentity { operation_id: String, input: IdentityAdministrationRequest },
    /// Ephemeral trusted-host observation without a task mutation identity.
    /// Never exposed in the model tool catalogue.
    ReportMachineResources { sample: crate::machine_resources::Sample },
    /// Private host coordination, never advertised as MCP tools.
    AcquireNativeRequest {
        operation_id: String, caller: crate::mcp::Caller, request_index: u32,
        key: crate::native_retry::CircuitKey, credential_configuration: String,
    },
    SettleNativeRequest {
        operation_id: String, caller: crate::mcp::Caller,
        acquire_operation_id: String, outcome: crate::native_retry::NativeRequestOutcome,
    },
    /// Readiness hint for an original Wait; never a grant or mutation receipt.
    ObserveNativeRequest { caller: crate::mcp::Caller, acquire_operation_id: String },
    /// Established operator invocation only; not part of an agent tool catalogue.
    MutateGovernance { operation_id: String, mutation: crate::governance::GovernanceMutation },
    QueryGovernance { query: crate::governance::GovernanceQuery },
    /// Existing trusted host path; not an agent MCP operation.
    MutateCharter { operation_id: String, mutation: crate::charter::CharterMutation },
    /// Original occurrence audit is not new execution authority.
    ObserveCharter { operation_id: String, caller: crate::mcp::Caller, observation: crate::charter::CharterObservation },
    QueryCharter { query: crate::charter::CharterQuery },
    EvaluateCompletion(crate::governance_completion::EvaluateCompletion),
    BeginCompletionContinuation(crate::governance_completion::BeginCompletionContinuation),
    AcknowledgeCompletionInput(crate::governance_completion::AcknowledgeCompletionInput),
    MutateQuestion { operation_id: String, mutation: crate::collaboration::questions::QuestionMutation },
    QueryQuestion { query: crate::collaboration::questions::QuestionQuery },
    MutateDocument { operation_id: String, mutation: crate::collaboration::documents::DocumentMutation },
    QueryDocument { query: crate::collaboration::documents::DocumentQuery },
    MutateReview { operation_id: String, mutation: crate::collaboration::documents::ReviewMutation },
    QueryReview { query: crate::collaboration::documents::ReviewQuery },
    MutateWork { operation_id: String, mutation: crate::collaboration::work::WorkMutation },
    QueryWork { query: crate::collaboration::work::WorkQuery },
    /// Trusted-host operator action; shares the exact definition-update transaction.
    UpdateDefinition {
        operation_id: String,
        update: DefinitionUpdate,
    },
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
    ObserveCollaborationDelivery { caller: crate::mcp::Caller, limit: usize },
    PrepareCollaborationDelivery {
        operation_id: String, caller: crate::mcp::Caller,
        expected_cursor: crate::collaboration::workflow::DeliveryCursor,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_routing_cursor: Option<crate::collaboration::workflow::EventId>,
        through: crate::collaboration::workflow::EventId, limit: usize,
    },
    BeginCollaborationDelivery {
        operation_id: String, caller: crate::mcp::Caller,
        batch_id: String, context_binding: String,
    },
    AcknowledgeCollaborationDelivery {
        operation_id: String, caller: crate::mcp::Caller,
        batch_id: String, context_binding: String,
    },
    QueryWorkflowActivity { cursor: Option<String>, limit: usize },
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
    SubmitTaskDraft {
        operation_id: String,
        input: TaskDraft,
    },
    RegisterAgent {
        operation_id: String,
        definition: Definition,
    },
    RetireAgent {
        operation_id: String,
        agent_id: String,
        expected_revision: String,
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
    IdentityAdministration(Box<IdentityAdministrationOutcome>),
    MachineResourceReport(crate::machine_resources::ReportDisposition),
    NativeRequestAdmission(crate::native_retry::NativeRequestAdmission),
    NativeRequestSettlement(crate::native_retry::NativeRequestSettlement),
    NativeRequestReadiness { acquire_operation_id: String, changed: bool },
    Governance(Box<crate::governance::GovernanceResult>),
    Charter(crate::charter::CharterOutcome),
    CharterQuery(crate::charter::CharterQueryResult),
    Completion(crate::governance_completion::CompletionOutcome),
    CollaborationDelivery(crate::collaboration::notifications::DeliveryObservation),
    WorkflowActivity(crate::collaboration::workflow::WorkflowActivityPage),
    QuestionMutation(crate::collaboration::questions::QuestionMutationOutcome),
    QuestionQuery(crate::collaboration::questions::QuestionQueryResult),
    DocumentMutation(crate::collaboration::documents::DocumentMutationOutcome),
    DocumentQuery(crate::collaboration::documents::DocumentQueryOutcome),
    ReviewMutation(crate::collaboration::documents::ReviewMutationOutcome),
    ReviewQuery(crate::collaboration::documents::ReviewQueryOutcome),
    WorkQuery(crate::collaboration::work::WorkQueryResult),
    WorkMutation(crate::collaboration::work::WorkMutationOutcome),
    RepositoryRegistration(crate::repository_catalogue::Entry),
    RepositoryRegistrations(crate::collaboration::Page<crate::repository_catalogue::Entry>),
    AgentMemory(crate::collaboration::AgentMemoryObservation),
    Repository(Box<crate::repository_view::Output>),
    Collaboration(crate::collaboration::QueryResult),
    CollaborationPage(crate::mcp::ListPage),
    ProtectedRelease(Box<crate::protected_release::StageView>),
    Release(Box<ReleaseCandidate>),
    Releases(Vec<ReleaseCandidate>),
    DefinitionUpdate(Box<DefinitionUpdateOutcome>),
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
            Self::AcquireNativeRequest { operation_id, .. }
            | Self::SettleNativeRequest { operation_id, .. }
            | Self::MutateGovernance { operation_id, .. }
            | Self::MutateCharter { operation_id, .. }
            | Self::ObserveCharter { operation_id, .. }
            | Self::MutateQuestion { operation_id, .. }
            | Self::MutateDocument { operation_id, .. }
            | Self::MutateReview { operation_id, .. }
            | Self::MutateWork { operation_id, .. }
            | Self::UpdateDefinition { operation_id, .. }
            | Self::AdministerIdentity { operation_id, .. }
            | Self::RegisterAgent { operation_id, .. }
            | Self::RetireAgent { operation_id, .. }
            | Self::RegisterRepository { operation_id, .. }
            | Self::ReviseRepository { operation_id, .. }
            | Self::SetRepositoryEnabled { operation_id, .. } => Some(operation_id),
            Self::EvaluateCompletion(value) => Some(&value.operation_id),
            Self::BeginCompletionContinuation(value) => Some(&value.operation_id),
            Self::AcknowledgeCompletionInput(value) => Some(&value.operation_id),
            Self::CallCollaboration { call } => Some(&call.operation_id),
            Self::PrepareRelease(input) => Some(&input.operation_id),
            Self::ReconcileResult(input) => Some(&input.operation_id),
            Self::PrepareAgentMemory { operation_id, .. }
            | Self::AcknowledgeAgentMemory { operation_id, .. }
            | Self::PrepareCollaborationDelivery { operation_id, .. }
            | Self::BeginCollaborationDelivery { operation_id, .. }
            | Self::AcknowledgeCollaborationDelivery { operation_id, .. }
            | Self::SubmitTask { operation_id, .. }
            | Self::SubmitTaskDraft { operation_id, .. }
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
        if let Outcome::Ok { data } = &self.outcome {
            if !native_request_response_matches(operation, data) {
                return Err("native request response differs from original operation".into());
            }
            let valid = match (operation, data) {
                (Operation::ReportMachineResources { .. }, Data::MachineResourceReport(_)) => true,
                (Operation::ReportMachineResources { .. }, _) | (_, Data::MachineResourceReport(_)) => false,
                (Operation::RegisterAgent { definition, .. }, Data::Agent(agent)) =>
                    agent.id == definition.id && agent.definition == *definition && !agent.retired,
                (Operation::RetireAgent { agent_id, expected_revision, .. }, Data::Agent(agent)) =>
                    agent.id == *agent_id && agent.definition.revision == *expected_revision && agent.retired,
                (Operation::RegisterAgent { .. } | Operation::RetireAgent { .. }, _) => false,
                (Operation::MutateCharter { mutation, .. }, Data::Charter(value)) => match (mutation,value) {
                    (crate::charter::CharterMutation::Publish { agent_id, expected_definition_revision, .. }, crate::charter::CharterOutcome::Definition(value)) =>
                        value.effect==crate::charter::CharterDefinitionEffect::PublicationOnly && value.definition.id==*agent_id && value.previous_revision==*expected_definition_revision,
                    (crate::charter::CharterMutation::SelectForFutureTasks { agent_id, expected_definition_revision, .. }, crate::charter::CharterOutcome::Definition(value)) =>
                        value.effect==crate::charter::CharterDefinitionEffect::FutureSubmissions && value.definition.id==*agent_id && value.previous_revision==*expected_definition_revision,
                    (crate::charter::CharterMutation::SetPending { .. },crate::charter::CharterOutcome::Session(_)) => true,
                    _=>false,
                },
                (Operation::ObserveCharter { .. }, Data::Charter(crate::charter::CharterOutcome::Definition(_))) => false,
                (Operation::ObserveCharter { .. }, Data::Charter(_))
                | (Operation::QueryCharter { .. }, Data::CharterQuery(_)) => true,
                (Operation::MutateCharter { .. } | Operation::ObserveCharter { .. } | Operation::QueryCharter { .. }, _)
                | (_, Data::Charter(_) | Data::CharterQuery(_)) => false,
                _ => true,
            };
            if !valid { return Err("charter result differs from operation".into()); }
        }
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
        if let Outcome::Ok { data } = &self.outcome {
            match (operation, data) {
                (Operation::MutateGovernance { .. } | Operation::QueryGovernance { .. }, Data::Governance(value))
                    if governance_response_matches(operation, value) => (),
                (Operation::MutateGovernance { .. } | Operation::QueryGovernance { .. }, _)
                | (_, Data::Governance(_)) => return Err("governance response differs from its original request".into()),
                _ => (),
            }
        }
        if matches!(self.outcome, Outcome::Ok { .. }) && self.observed_controller_revision.is_none()
        {
            return Err("successful reply lacks observed controller revision".into());
        }
        Ok(())
    }
}

// Repeated acquisition/settlement may return an older canonical occurrence.
// Compare its immutable subject, not its ID with the new request envelope.
fn native_request_response_matches(operation: &Operation, data: &Data) -> bool {
    use crate::native_retry::NativeRequestAdmission;
    match (operation, data) {
        (Operation::AcquireNativeRequest { caller, request_index, key, credential_configuration, .. },
            Data::NativeRequestAdmission(NativeRequestAdmission::Granted { permit })) =>
            &permit.circuit.caller == caller && permit.request_index == *request_index
                && &permit.circuit.key == key
                && &permit.credential_configuration == credential_configuration,
        (Operation::AcquireNativeRequest { .. },
            Data::NativeRequestAdmission(NativeRequestAdmission::Wait { .. })) => true,
        (Operation::SettleNativeRequest { caller, acquire_operation_id, outcome, .. },
            Data::NativeRequestSettlement(settlement)) =>
            &settlement.permit.circuit.caller == caller
                && &settlement.permit.circuit.operation_id == acquire_operation_id
                && &settlement.outcome == outcome,
        (Operation::ObserveNativeRequest { acquire_operation_id, .. },
            Data::NativeRequestReadiness { acquire_operation_id: original, .. }) =>
            acquire_operation_id == original,
        (Operation::AcquireNativeRequest { .. } | Operation::SettleNativeRequest { .. }
            | Operation::ObserveNativeRequest { .. }, _)
        | (_, Data::NativeRequestAdmission(_) | Data::NativeRequestSettlement(_)
            | Data::NativeRequestReadiness { .. }) => false,
        _ => true,
    }
}

// Exact typed response association, not a new authority decision. Current
// transport/receipt bounds and committed mutation context remain independent.
fn governance_response_matches(operation: &Operation, value: &crate::governance::GovernanceResult) -> bool {
    use crate::governance::{GovernanceMutation as M, GovernanceQuery as Q, GovernanceResult as V};
    use crate::collaboration::workflow::{ActorProvenance, PrincipalRef, TrustedHostPath};
    let operator = |actor: &crate::collaboration::workflow::RecordedActor| {
        actor.principal == PrincipalRef::TrustedHostOperator
            && actor.provenance == (ActorProvenance::TrustedHost { path: TrustedHostPath::OperatorApi })
    };
    match (operation, value) {
        (Operation::MutateGovernance { mutation: M::RegisterRule { rule }, .. }, V::Rule(observed)) => observed == rule,
        (Operation::MutateGovernance { mutation: M::RegisterConfiguration { configuration }, .. }, V::Configuration(observed)) => observed == configuration,
        (Operation::MutateGovernance { mutation: M::EvaluateShadow { original, candidate, configuration_revision }, .. }, V::Shadow(observed)) =>
            &observed.original == original && &observed.candidate == candidate && &observed.configuration_revision == configuration_revision,
        (Operation::MutateGovernance { mutation: M::Promote { configuration_revision, .. }, .. }, V::State(observed)) =>
            &observed.selected_revision == configuration_revision && observed.temporary_override.is_none(),
        (Operation::MutateGovernance { operation_id, mutation: M::Pause { scope, reason } }, V::Hold(observed)) =>
            &observed.operation_id == operation_id && &observed.scope == scope && &observed.reason == reason && operator(&observed.actor),
        (Operation::MutateGovernance { mutation: M::ReleaseHold { scope, .. }, .. }, V::Released(observed)) => observed == scope,
        (Operation::MutateGovernance { mutation: M::Stop { task_id, .. }, .. }, V::StopRequested { task_id: observed }) => observed == task_id,
        (Operation::MutateGovernance { operation_id, mutation: M::Override { expected_revision, configuration_revision, expires_at_ms, reason } }, V::State(observed)) =>
            &observed.selected_revision == configuration_revision && observed.temporary_override.as_ref().is_some_and(|original|
                &original.operation_id == operation_id && &original.prior_revision == expected_revision
                && &original.replacement_revision == configuration_revision && original.expires_at_ms == *expires_at_ms
                && &original.reason == reason && operator(&original.actor)),
        (Operation::MutateGovernance { mutation: M::ReconcileExpiry { .. }, .. }, V::State(observed)) => observed.temporary_override.is_none(),
        (Operation::QueryGovernance { query: Q::Decision { decision } }, V::Decision(observed)) => &observed.id == decision,
        (Operation::QueryGovernance { query: Q::TaskCompletion { task_id } }, V::TaskCompletion { task_id: observed, decision }) =>
            observed == task_id && decision.as_ref().is_none_or(|record|
                record.id.point == crate::governance::DecisionPoint::Completion
                    && matches!(&record.detail, crate::governance::DecisionDetail::Completion(detail)
                        if &detail.subject.task_id == task_id)),
        (Operation::QueryGovernance { query: Q::Rule { rule } }, V::Rule(observed)) => &observed.reference == rule,
        (Operation::QueryGovernance { query: Q::Configuration { revision } }, V::Configuration(observed)) => &observed.revision == revision,
        (Operation::QueryGovernance { query: Q::State }, V::State(_)) => true,
        _ => false,
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
    CollaborationV2 {
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
