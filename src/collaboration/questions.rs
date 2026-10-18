//! Immutable posed questions, append-only answers and bounded decision projections.
//! Actor provenance is produced by the canonical controller, never tool arguments.
use serde::{Deserialize, Serialize};
use super::{Page, workflow::{PrincipalRef, RecordedActor, RecordRef, AnswerRef, ReviewTarget}};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QuestionOption {
    pub id: String,
    pub label: String,
    pub explanation: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PoseQuestion {
    pub record: RecordRef,
    pub recipient: PrincipalRef,
    // Accepted prompt includes the controller-composed exact recommendation line.
    pub prompt: String,
    pub options: Vec<QuestionOption>,
    pub recommended_option_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QuestionAnswer {
    pub option_id: String,
    pub explanation: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum QuestionMutation {
    Pose { question: PoseQuestion },
    Answer { question_id: String, expected_revision: u64, answer: QuestionAnswer },
    Cancel { question_id: String, expected_revision: u64, reason: String },
    Supersede {
        question_id: String, expected_revision: u64, reason: String,
        replacement: PoseQuestion,
    },
    SupersedeAnswer {
        question_id: String, expected_revision: u64, expected_answer_id: String,
        answer: QuestionAnswer, reason: String,
    },
    WithdrawAnswer {
        question_id: String, expected_revision: u64, expected_answer_id: String,
        reason: String,
    },
    MarkMoot {
        question_id: String, expected_revision: u64, expected_answer_id: String,
        caused_by: AnswerRef, reason: String,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QuestionStatus { Open, Answered, Cancelled, Superseded, Moot }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Question {
    pub id: String,
    pub revision: u64,
    pub created_product_revision: u64,
    pub changed_product_revision: u64,
    pub posed: PoseQuestion,
    pub poser: RecordedActor,
    pub status: QuestionStatus,
    // Latest immutable answer remains identifiable after its effect is withdrawn.
    pub latest_answer_id: Option<String>,
    pub active_answer_id: Option<String>,
    pub superseded_by: Option<String>,
    pub supersedes: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecordedAnswer {
    pub id: String,
    pub question_id: String,
    pub answer: QuestionAnswer,
    pub author: RecordedActor,
    pub replaces: Option<String>,
    pub product_revision: u64,
    pub operation_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum QuestionAction {
    Cancelled { reason: String },
    Superseded { replacement_question_id: String, reason: String },
    AnswerReplaced { previous_answer_id: String, answer_id: String, reason: String },
    AnswerWithdrawn { answer_id: String, reason: String },
    MarkedMoot { answer_id: String, caused_by: AnswerRef, reason: String },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecordedQuestionAction {
    pub question_id: String,
    pub question_revision: u64,
    pub product_revision: u64,
    pub actor: RecordedActor,
    pub operation_id: String,
    pub action: QuestionAction,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QuestionView {
    pub question: Question,
    pub active_answer: Option<RecordedAnswer>,
    // Read dynamically from the linked record. Changing it never rewrites posed.
    pub current_responsible_owner: PrincipalRef,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QuestionMutationOutcome {
    pub question: QuestionView,
    pub new_answer: Option<RecordedAnswer>,
    pub replacement: Option<QuestionView>,
    pub action: Option<RecordedQuestionAction>,
}

// Root's common query result wraps these; Page uses existing bounded projection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum QuestionQuery {
    Get { question_id: String },
    List { record: Option<RecordRef>, status: Option<QuestionStatus>, after: Option<String>, limit: usize },
    Answers { question_id: String, after: Option<String>, limit: usize },
    Actions { question_id: String, after: Option<String>, limit: usize },
    // Agent tools bind viewer from the caller. A trusted-host read may inspect a
    // selected recipient's queue, without acquiring that recipient's write rights.
    DecisionQueue { viewer: PrincipalRef, after: Option<String>, limit: usize },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionQueueItem {
    Question {
        question_id: String, revision: u64, created_product_revision: u64,
        record: RecordRef, recipient: PrincipalRef, prompt: String,
        recommended_option_id: String,
    },
    // Projection of the document/review owner's canonical record.
    Review {
        review_id: String, revision: u64, created_product_revision: u64,
        recipient: PrincipalRef, target: ReviewTarget, summary: String,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DecisionQueuePage {
    pub viewer: PrincipalRef,
    pub items: Vec<DecisionQueueItem>,
    pub through_product_revision: u64,
    pub observed_product_revision: u64,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case", deny_unknown_fields)]
pub enum QuestionQueryResult {
    Question(QuestionView),
    Questions(Page<QuestionView>),
    Answers(Page<RecordedAnswer>),
    Actions(Page<RecordedQuestionAction>),
    DecisionQueue(DecisionQueuePage),
}

impl QuestionMutation {
    pub fn expected_record(&self) -> Option<(&str, u64)> {
        match self {
            Self::Pose { .. } => None,
            Self::Answer { question_id, expected_revision, .. }
            | Self::Cancel { question_id, expected_revision, .. }
            | Self::Supersede { question_id, expected_revision, .. }
            | Self::SupersedeAnswer { question_id, expected_revision, .. }
            | Self::WithdrawAnswer { question_id, expected_revision, .. }
            | Self::MarkMoot { question_id, expected_revision, .. } => Some((question_id, *expected_revision)),
        }
    }
}
