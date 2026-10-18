//! Versioned document and exact-target review domain records.
//! Shared actors, targets, verdicts and resolver identities live in workflow.
use serde::{Deserialize,Serialize};
use super::workflow::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DocumentContent {
    pub title: String,
    // Bounded text, escaped on display; not executable markup or a block engine.
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Document {
    pub id: String,
    pub revision: u64,
    pub current: DocumentVersionRef,
    pub responsible_owner: PrincipalRef,
    pub created_product_revision: u64,
    pub changed_product_revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DocumentVersion {
    pub reference: DocumentVersionRef,
    pub content: DocumentContent,
    pub author: RecordedActor,
    pub product_revision: u64,
    pub operation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecordedComment {
    pub id: String,
    pub record: RecordRef,
    pub body: String,
    pub author: RecordedActor,
    pub product_revision: u64,
    pub operation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DocumentMutation {
    Create {
        content: DocumentContent,
        supplied_content_id: Option<String>,
        responsible_owner: PrincipalRef,
    },
    AppendVersion {
        document_id: String,
        expected_revision: u64,
        content: DocumentContent,
        supplied_content_id: Option<String>,
    },
    SetResponsibleOwner {
        document_id: String,
        expected_revision: u64,
        responsible_owner: PrincipalRef,
    },
    PostComment {
        record: RecordRef,
        expected_revision: u64,
        body: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DocumentView {
    pub document: Document,
    pub version: DocumentVersion,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DocumentMutationOutcome {
    Document { view: DocumentView },
    Comment { comment: RecordedComment, record_revision: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DocumentQuery {
    Get { document_id: String, version: Option<u64> },
    List { after: Option<String>, limit: usize },
    Versions { document_id: String, after: Option<String>, limit: usize },
    Comments { record: RecordRef, after: Option<String>, limit: usize },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case", deny_unknown_fields)]
pub enum DocumentQueryOutcome {
    Document(DocumentView),
    Documents(crate::collaboration::Page<Document>),
    Versions(crate::collaboration::Page<DocumentVersion>),
    Comments(crate::collaboration::Page<RecordedComment>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReviewStatus { Open, Decided, Withdrawn }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Review {
    pub id: String,
    pub revision: u64,
    pub target: ReviewTarget,
    pub reviewer: PrincipalRef,
    pub summary: String,
    pub requested_by: RecordedActor,
    pub created_product_revision: u64,
    pub changed_product_revision: u64,
    pub status: ReviewStatus,
    pub decision_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RecordedReviewDecision {
    pub id: String,
    pub review_id: String,
    pub target: ReviewTarget,
    pub verdict: ReviewVerdict,
    pub explanation: String,
    pub reviewer: RecordedActor,
    pub product_revision: u64,
    pub operation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RecordedReviewWithdrawal {
    pub review_id: String,
    pub target: ReviewTarget,
    pub reason: String,
    pub actor: RecordedActor,
    pub product_revision: u64,
    pub operation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReviewView {
    pub review: Review,
    pub decision: Option<RecordedReviewDecision>,
    pub withdrawal: Option<RecordedReviewWithdrawal>,
    // Derived from target document/task's current collaboration owner. Not an
    // edit of immutable requested_by, target, reviewer or accepted execution.
    pub current_responsible_owner: PrincipalRef,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReviewMutation {
    Request { target: ReviewTarget, reviewer: PrincipalRef, summary: String },
    Decide {
        review_id: String,
        expected_revision: u64,
        verdict: ReviewVerdict,
        explanation: String,
    },
    Withdraw { review_id: String, expected_revision: u64, reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AcceptedReviewScope { Result, Checkpoint }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReviewQuery {
    Get { review_id: String },
    List {
        record: Option<RecordRef>,
        status: Option<ReviewStatus>,
        after: Option<String>,
        limit: usize,
    },
    // Produce exact controller-owned selector; a subsequent Request still checks
    // it in its own transaction. This query grants no acceptance or authority.
    AcceptedTarget { task_id: String, scope: AcceptedReviewScope },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReviewMutationOutcome { Review(ReviewView) }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReviewQueryOutcome {
    Review(ReviewView),
    Reviews(crate::collaboration::Page<Review>),
    AcceptedTarget(ReviewTarget),
}

// Root protocol names the query payloads Result; aliases preserve one wire shape.
pub type DocumentQueryResult = DocumentQueryOutcome;
pub type ReviewQueryResult = ReviewQueryOutcome;
