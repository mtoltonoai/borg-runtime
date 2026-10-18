//! Closed resource-allowance change class. Numeric policy bounds stay in the
//! existing limit groups; this metadata selects fields, not a hidden ceiling.
use super::{ExecutionLimits, ExecutionLimitOverrides, TransportLimits};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum DefinitionLimitField {
    #[serde(rename = "native.context_bytes")]
    NativeContextBytes,
    #[serde(rename = "native.request_bytes")]
    NativeRequestBytes,
    #[serde(rename = "native.helper_value_bytes")]
    NativeHelperValueBytes,
    #[serde(rename = "native.response_wire_bytes")]
    NativeResponseWireBytes,
    #[serde(rename = "coding.model_context_window")]
    CodingModelContextWindow,
    #[serde(rename = "native.output_bytes")]
    NativeOutputBytes,
    #[serde(rename = "native.result_bytes")]
    NativeResultBytes,
    #[serde(rename = "native.max_output_tokens")]
    NativeMaxOutputTokens,
    #[serde(rename = "coding.command_output_bytes")]
    CodingCommandOutputBytes,
    #[serde(rename = "coding.summary_chars")]
    CodingSummaryChars,
    #[serde(rename = "coding.summary_bytes")]
    CodingSummaryBytes,
    #[serde(rename = "collaboration.complete_result_bytes")]
    CollaborationCompleteResultBytes,
}
impl DefinitionLimitField {
    pub fn value(self, values: &ExecutionLimits) -> Option<usize> {
        Some(match self {
            Self::CodingModelContextWindow => return values.coding.model_context_window,
            Self::NativeRequestBytes => values.native.request_bytes,
            Self::NativeHelperValueBytes => values.native.helper_value_bytes,
            Self::NativeResponseWireBytes => values.native.response_wire_bytes,
            Self::NativeContextBytes => values.native.context_bytes,
            Self::NativeOutputBytes => values.native.output_bytes,
            Self::NativeResultBytes => values.native.result_bytes,
            Self::NativeMaxOutputTokens => values.native.max_output_tokens,
            Self::CodingCommandOutputBytes => values.coding.command_output_bytes,
            Self::CodingSummaryChars => values.coding.summary_chars,
            Self::CodingSummaryBytes => values.coding.summary_bytes,
            Self::CollaborationCompleteResultBytes => values.collaboration.complete_result_bytes,
        })
    }
    pub fn set(self, values: &mut ExecutionLimits, value: usize) {
        match self {
            Self::CodingModelContextWindow => values.coding.model_context_window = Some(value),
            Self::NativeRequestBytes => values.native.request_bytes = value,
            Self::NativeHelperValueBytes => values.native.helper_value_bytes = value,
            Self::NativeResponseWireBytes => values.native.response_wire_bytes = value,
            Self::NativeContextBytes => values.native.context_bytes = value,
            Self::NativeOutputBytes => values.native.output_bytes = value,
            Self::NativeResultBytes => values.native.result_bytes = value,
            Self::NativeMaxOutputTokens => values.native.max_output_tokens = value,
            Self::CodingCommandOutputBytes => values.coding.command_output_bytes = value,
            Self::CodingSummaryChars => values.coding.summary_chars = value,
            Self::CodingSummaryBytes => values.coding.summary_bytes = value,
            Self::CollaborationCompleteResultBytes => values.collaboration.complete_result_bytes = value,
        }
    }
    /// Preserve untouched groups; touched groups freeze their complete current
    /// effective values because the established representation is group-based.
    pub fn materialize(self, overrides: &mut ExecutionLimitOverrides, values: &ExecutionLimits) {
        match self {
            Self::NativeRequestBytes | Self::NativeHelperValueBytes | Self::NativeResponseWireBytes | Self::NativeContextBytes | Self::NativeOutputBytes | Self::NativeResultBytes | Self::NativeMaxOutputTokens => overrides.native = Some(values.native.clone()),
            Self::CodingModelContextWindow | Self::CodingCommandOutputBytes | Self::CodingSummaryChars | Self::CodingSummaryBytes => overrides.coding = Some(values.coding.clone()),
            Self::CollaborationCompleteResultBytes => overrides.collaboration = Some(values.collaboration.clone()),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DefinitionUpdatePolicy {
    pub revision: String,
    pub allowed_fields: Vec<DefinitionLimitField>,
}
impl DefinitionUpdatePolicy {
    pub fn validate(&self, transport: &TransportLimits) -> Result<(), String> {
        if self.revision.is_empty() || self.revision.len() > transport.identity_bytes || self.revision.chars().any(char::is_control) {
            return Err("definition_updates.revision is invalid".into());
        }
        let unique: std::collections::BTreeSet<_> = self.allowed_fields.iter().copied().collect();
        if unique.len() != self.allowed_fields.len() {
            return Err("definition_updates.allowed_fields contains duplicates".into());
        }
        Ok(())
    }
}
