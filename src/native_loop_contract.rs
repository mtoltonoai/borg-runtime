//! Shared, data-only identifiers for the explicit collaboration loop receipts.
//! Names identify expected receipts; controller validation still checks their
//! exact owner, bodies, outcome, commit order and accepted profile.
pub fn reservation_id(attempt: &str, index: u32) -> String {
    format!("{attempt}-native-request-{index}")
}
pub fn usage_id(attempt: &str, index: u32) -> String {
    format!("{attempt}-native-usage-{index}")
}
/// One original terminal observation consumes the request's diagnostic credit.
/// Constructing its identity grants no authority and proves no provider outcome.
pub fn diagnostic_id(attempt: &str, index: u32) -> String {
    format!("{attempt}-native-diagnostic-{index}")
}

/// Provider disposition observed by the original request owner. Local validation,
/// cleanup and task acceptance remain separate. Retry delay is retained only in
/// its screened diagnostic field, rather than duplicated in this disposition.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeRequestObservedOutcome {
    NotStarted,
    Unknown,
    KnownRejected { rejection_kind: crate::native::RejectionKind },
    Completed,
    OutputCapped,
}
impl From<crate::native::ProviderOutcome> for NativeRequestObservedOutcome {
    fn from(value: crate::native::ProviderOutcome) -> Self {
        use crate::native::ProviderOutcome;
        match value {
            ProviderOutcome::NotStarted => Self::NotStarted,
            ProviderOutcome::Unknown => Self::Unknown,
            ProviderOutcome::Rejected(value) => Self::KnownRejected { rejection_kind: value.kind },
            ProviderOutcome::Completed => Self::Completed,
            ProviderOutcome::OutputCapped => Self::OutputCapped,
        }
    }
}
pub fn call_id(attempt: &str, index: u32) -> String {
    format!("native-collab-{attempt}-{index}")
}
pub fn completion_id(attempt: &str) -> String {
    format!("{attempt}-native-loop-completed")
}
pub fn evidence(attempt: &str) -> String {
    format!("native collaboration loop: {}", completion_id(attempt))
}

/// Stable occurrence identity; the first call preserves the original contract.
pub fn call_operation_id(attempt: &str, request_index: u32, call_index: u32) -> String {
    if call_index == 1 {
        call_id(attempt, request_index)
    } else {
        format!("{}-call-{call_index}", call_id(attempt, request_index))
    }
}

/// A charged non-tool request is identified by its original canonical fact.
/// This value is data. Controller validation resolves the original record;
/// constructing a variant grants no permission and proves no provider outcome.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeRequestEvidence {
    Compaction { request_index: u32, operation_id: String },
    ConfirmedRejection { request_index: u32, operation_id: String },
    FormatRepair { request_index: u32, operation_id: String },
    OutputCap { request_index: u32, operation_id: String },
    CompletionContinuation {
        evidence: crate::governance_completion::NativeCompletionContinuationEvidence,
    },
}
impl NativeRequestEvidence {
    pub fn request_index(&self) -> u32 {
        match self {
            Self::Compaction { request_index, .. }
            | Self::ConfirmedRejection { request_index, .. }
            | Self::FormatRepair { request_index, .. }
            | Self::OutputCap { request_index, .. } => *request_index,
            Self::CompletionContinuation { evidence } => evidence.request_index,
        }
    }
}

/// One retained known-rejection report for this original charged request.
pub fn rejection_id(attempt: &str, index: u32) -> String {
    format!("{attempt}-native-rejection-{index}")
}

/// The original completed-format observation, never a provider rejection.
pub fn format_failure_id(attempt: &str, index: u32) -> String {
    format!("{attempt}-native-format-{index}")
}

/// A validated terminal output cap, distinct from incomplete transport.
pub fn output_cap_id(attempt: &str, index: u32) -> String {
    format!("{attempt}-native-output-cap-{index}")
}
