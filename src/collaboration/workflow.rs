//! Shared collaboration workflow identities, outcomes and activity records.
//! Actor values below are durable records, never caller-authoritative mutation inputs.
use serde::{Deserialize, Serialize};
use crate::protocol::Checkpoint;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]

// Traceability scope: The closed recipient schema admits the supported agent and trusted-host principals.
//= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
//# A collaboration request MUST refuse unsupported recipient kinds.

pub enum PrincipalRef {
    Agent { agent_id: String },
    // One existing trusted-host operator scope, not a named authenticated person.
    TrustedHostOperator,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActorProvenance {
    BoundAgentSession {
        agent_id: String, session_id: String, task_id: String,
        attempt_id: String, generation: u64, worker_id: String, incarnation: String,
    },
    TrustedHost { path: TrustedHostPath },
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TrustedHostPath { Browser, OperatorApi, ControllerMaintenance }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecordedActor {
    pub principal: PrincipalRef,
    pub provenance: ActorProvenance,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RecordRef {
    Task { task_id: String },
    Question { question_id: String },
    Document { document_id: String },
    Review { review_id: String },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DocumentVersionRef {
    pub document_id: String,
    pub version: u64,
    pub content_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AnswerRef { pub question_id: String, pub answer_id: String }

/// Short spelling aliases the single shared document-version representation.
pub type DocVersionRef = DocumentVersionRef;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AcceptedResultRef {
    pub task_id: String,
    pub attempt_id: String,
    // Revision that first committed this exact accepted result, not the current
    // task revision after later observations or collaboration activity.
    pub terminal_task_revision: u64,
    // Existing canonical history occurrence that committed acceptance. Root
    // checks the exact stored AcceptedResult bytes at that occurrence; no new
    // digest dependency or field on AcceptedResult is required for this reference.
    pub acceptance_history_sequence: u64,
    // Exact accepted result.checkpoint, including registration, repository,
    // commit, remote_ref and publication operation_id; None for no checkpoint.
    pub checkpoint: Option<Checkpoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReviewTarget {
    DocumentVersion { version: DocumentVersionRef },
    AcceptedTaskResult { result: AcceptedResultRef },
    // Explicit checkpoint-focused review; requires result.checkpoint.is_some().
    // The whole accepted result remains bound, rather than only a moving ref.
    AcceptedTaskCheckpoint { result: AcceptedResultRef },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReviewVerdict {
    Approved,
    ChangesRequested,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum QuestionResolution {
    Answered { question_id: String, answer: AnswerRef, selected_option_id: String },
    Cancelled { question_id: String, action_revision: u64, reason: String },
    Superseded { question_id: String, replacement_question_id: String, reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReviewResolution {
    Decided {
        review_id: String,
        decision_id: String,
        target: ReviewTarget,
        verdict: ReviewVerdict,
    },
    Withdrawn {
        review_id: String,
        target: ReviewTarget,
        action_revision: u64,
        reason: String,
    },
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag="kind", rename_all="snake_case", deny_unknown_fields)]

// Traceability scope: Unsupported agent, team and external-party wait kinds cannot decode into this closed type.
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//# A wait on an agent, team or external party MUST be refused.

pub enum ResolverRef {
    Task { task_id: String },
    Question { question_id: String },
    /// A document review is this same resolver with a DocumentVersion target.
    Review { review_id: String },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all="snake_case")]
pub enum TerminalTaskStatus { Succeeded, Failed, Cancelled }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TaskResolution {
    pub task_id: String,
    pub terminal_task_revision: u64,
    pub status: TerminalTaskStatus,
    pub accepted_result: Option<AcceptedResultRef>,
    pub summary: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag="kind", content="value", rename_all="snake_case", deny_unknown_fields)]
pub enum ResolverOutcome {
    Task(TaskResolution), Question(QuestionResolution), Review(ReviewResolution),
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct EventId {
    pub database_id: String,
    pub product_revision: u64,
    pub ordinal: u32,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WaitRef { pub task_id: String, pub wait_id: String }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ResolvedWait {
    pub wait: WaitRef,
    pub resolver: ResolverRef,
    pub event_id: EventId,
    pub outcome: ResolverOutcome,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecordOwnerRevision { pub owner: PrincipalRef, pub revision: u64 }
/// Text projection, not a replacement for an exact durable record reference.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BoundedText { pub text: String, pub truncated: bool }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag="kind", rename_all="snake_case", deny_unknown_fields)]
pub enum QuestionChange {
    Posed { linked_record: RecordRef, recipient: PrincipalRef, prompt: BoundedText, recommended_option_id: String },
    Closed { resolution: QuestionResolution },
    AnswerReplaced { previous: AnswerRef, current: AnswerRef, selected_option_id: String, reason: BoundedText },
    AnswerWithdrawn { answer: AnswerRef, reason: BoundedText },
    MarkedMoot { answer: AnswerRef, caused_by: AnswerRef, reason: BoundedText },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag="kind", rename_all="snake_case", deny_unknown_fields)]
pub enum DocumentChange {
    Created { version: DocumentVersionRef, responsible_owner: PrincipalRef, title: BoundedText, body: BoundedText },
    VersionAdded { previous: DocumentVersionRef, version: DocumentVersionRef, title: BoundedText, body: BoundedText },
    OwnerChanged { previous: PrincipalRef, current: PrincipalRef },
    CommentAdded { comment_id: String, body: BoundedText },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag="kind", rename_all="snake_case", deny_unknown_fields)]
pub enum ReviewChange {
    Requested { target: ReviewTarget, reviewer: PrincipalRef, summary: BoundedText },
    Closed { resolution: ReviewResolution },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag="kind", rename_all="snake_case", deny_unknown_fields)]
pub enum WaitChange {
    Added { wait: WaitRef, resolver: ResolverRef },
    Removed { wait: WaitRef, resolver: ResolverRef, reason: BoundedText },
    Resolved { wait: WaitRef, resolver: ResolverRef, outcome: ResolverOutcome },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag="kind", rename_all="snake_case", deny_unknown_fields)]
pub enum TaskChange {
    OwnerChanged { previous: PrincipalRef, current: PrincipalRef },
    Closed { resolution: TaskResolution },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag="kind", content="change", rename_all="snake_case", deny_unknown_fields)]
pub enum ActivityDetail {
    Task(TaskChange), Question(QuestionChange), Document(DocumentChange), Review(ReviewChange), Wait(WaitChange),
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResolutionRouting {
    pub requested_by: RecordedActor,
    pub linked_record: RecordRef,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WorkflowActivity {
    pub id: EventId,
    //= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
    //# A collaboration change activity MUST identify its typed record subject.
    pub subject: RecordRef,
    pub subject_revision: u64,
    pub actor: RecordedActor,
    pub detail: ActivityDetail,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing: Option<ResolutionRouting>,
    pub truncated: bool,
}
/// Controller-only intent returned to the sole transaction owner. Not tool input.
#[derive(Debug, Clone, PartialEq)]
pub enum WorkflowEffect {
    Activity { subject: RecordRef, subject_revision: u64, detail: ActivityDetail },
    Resolve { resolution: ResolverOutcome, requested_by: RecordedActor, linked_record: RecordRef },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeliveryScope { pub task_id: String, pub session_id: String, pub agent_id: String }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeliveryCursor {
    pub database_id: String,
    pub scope: DeliveryScope,
    /// None is before the first product event; no fabricated positive revision.
    pub position: Option<EventId>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WorkflowActivityPage {
    pub items: Vec<WorkflowActivity>,
    pub through: Option<EventId>,
    pub next_cursor: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WorkflowContinuation {
    pub task_id: String,
    pub session_id: String,
    pub resolved_waits: Vec<ResolvedWait>,
}
