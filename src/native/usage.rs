//! Observations from a complete provider envelope, separate from budget charge
//! and platform acceptance. Missing or malformed counts are never zero usage.
use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderTokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
}
impl ProviderTokenUsage {
    pub(super) fn checked(input: u64, output: u64, total: u64) -> Option<Self> {
        if [input, output, total].iter().any(|n| *n > i64::MAX as u64)
            || input.checked_add(output) != Some(total)
        {
            return None;
        }
        Some(Self {
            input_tokens: input,
            output_tokens: output,
            total_tokens: total,
        })
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum UsageObservation {
    #[default]
    Unknown,
    Reported(ProviderTokenUsage),
    Invalid,
}
impl UsageObservation {
    pub fn diagnostic(&self) -> Option<&'static str> {
        match self {
            Self::Invalid => Some("native_usage_invalid"),
            _ => None,
        }
    }

    pub(super) fn from_envelope(envelope: &Value) -> Self {
        let Some(value) = envelope.get("usage").filter(|v| !v.is_null()) else {
            return Self::Unknown;
        };
        let Some(object) = value.as_object() else {
            return Self::Invalid;
        };
        let (Some(input), Some(output), Some(total)) = (
            object.get("input_tokens").and_then(Value::as_u64),
            object.get("output_tokens").and_then(Value::as_u64),
            object.get("total_tokens").and_then(Value::as_u64),
        ) else {
            return Self::Invalid;
        };
        ProviderTokenUsage::checked(input, output, total).map_or(Self::Invalid, Self::Reported)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RequestExposure {
    #[default]
    NotStarted,
    /// Set conservatively before polling the single POST. This does not prove
    /// that the provider executed or billed a model request.
    MayHaveReachedProvider,
    /// An HTTP response was received, not necessarily a model completion.
    HttpResponseReceived,
}

/// A diagnostic category is not proof that repeating a provider request is safe.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderErrorCategory {
    RateLimited, Overloaded, InvalidRequest, ContextLimit, PolicyRefused,
    QuotaExhausted, AuthenticationFailed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectionKind { RateLimited, InvalidRequest, ContextLimit, PolicyRefused, QuotaExhausted, AuthenticationFailed }
impl RejectionKind {
    pub fn same_input_retryable(self)->bool {matches!(self,Self::RateLimited)}
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case", deny_unknown_fields)]
pub enum RetryAfter { Absent, DelayMs(u64), Invalid }
/// Produced only from a complete selected rejection envelope, never error text.
/// Known refusal does not refund the reservation or independently admit a retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRejection {
    pub kind: RejectionKind,
    pub retry_after: RetryAfter,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all="snake_case")]
pub enum RetryBlockReason {
    RequestAllowance,
    Deadline,
    RetryPolicyDisabled,
    InvalidRetryAfter,
    RetryDelayUnavailable,
    CoordinationDelayUnavailable,
    InvalidRequest,
    ContextLimit,
    PolicyRefused,
    QuotaExhausted,
    AuthenticationFailed,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeRetryBlock {
    pub request_index: u32,
    /// Exact canonical rejection or reservation receipt; never free-form error text.
    pub source_operation_id: String,
    pub reason: RetryBlockReason,
}

/// Provider disposition is separate from local validation, cleanup, token usage
/// and controller acceptance. It never authorizes retry or refunds reservation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderOutcome {
    /// Complete validated output-cap terminal; partial output is not a final result.
    OutputCapped,
    NotStarted,
    /// Complete selected rejection: no accepted model output; retry still needs admission.
    Rejected(ProviderRejection),
    /// Sending may have begun but no complete selected provider outcome was
    /// observed. HTTP headers, status codes and partial bodies are insufficient.
    Unknown,
    /// A fully decoded completed assistant text/refusal or configured tool-call envelope was observed.
    /// Its output can still be invalid, refused, over budget or locally cancelled.
    Completed,
}

/// Examine the complete bounded envelope, not just its top-level status or a
/// prefix ending at a refusal. Unknown actions/parts and malformed later fields
/// never produce a known-completion marker. Output JSON/schema checks follow.
pub(super) fn provider_completed(envelope: &Value) -> bool {
    if envelope.get("status").and_then(Value::as_str) != Some("completed")
        || envelope.get("error").is_some_and(|value| !value.is_null())
    {
        return false;
    }
    let Some(items) = envelope.get("output").and_then(Value::as_array) else {
        return false;
    };
    let mut messages = 0;
    for item in items {
        match item.get("type").and_then(Value::as_str) {
            Some("reasoning") => {}
            Some("message") => {
                messages += 1;
                if messages != 1
                    || item.get("role").and_then(Value::as_str) != Some("assistant")
                    || item.get("status").and_then(Value::as_str) != Some("completed")
                {
                    return false;
                }
                let Some(parts) = item.get("content").and_then(Value::as_array) else {
                    return false;
                };
                if parts.is_empty() {
                    return false;
                }
                for part in parts {
                    let key = match part.get("type").and_then(Value::as_str) {
                        Some("output_text") => "text",
                        Some("refusal") => "refusal",
                        _ => return false,
                    };
                    if part.get(key).and_then(Value::as_str).is_none() {
                        return false;
                    }
                }
            }
            _ => return false,
        }
    }
    messages == 1
}

/// No Debug/Serialize: this contains task output. The worker must persist known
/// usage before taking result's success/failure branch, using its existing outbox.
pub struct NativeObservation {
    pub result: Result<super::NativeResult, String>,
    pub request_exposure: RequestExposure,
    pub usage: UsageObservation,
    pub provider_outcome: ProviderOutcome,
    /// Observed failure facts, independent of bounded display text and disposition.
    pub diagnostic: Option<super::ProviderFailureDiagnostic>,
}
impl NativeObservation {
    pub(super) fn not_started(code: impl Into<String>) -> Self {
        Self {
            result: Err(code.into()),
            request_exposure: RequestExposure::NotStarted,
            usage: UsageObservation::Unknown,
            provider_outcome: ProviderOutcome::NotStarted,
            diagnostic: Some(super::ProviderFailureDiagnostic::new(super::ProviderFailureClass::LocalAdmission)),
        }
    }
}

#[cfg(test)]
mod rejection_kind_tests {
    use super::*;

    #[test]
    fn complete_known_rejections_keep_their_distinct_wire_kind() {
        for (kind,label) in [(RejectionKind::RateLimited,"rate_limited"),
            (RejectionKind::InvalidRequest,"invalid_request"),
            (RejectionKind::ContextLimit,"context_limit"),
            (RejectionKind::PolicyRefused,"policy_refused"),
            (RejectionKind::QuotaExhausted,"quota_exhausted"),
            (RejectionKind::AuthenticationFailed,"authentication_failed")] {
            let value=ProviderRejection {kind,retry_after:RetryAfter::Invalid};
            let json=serde_json::to_value(value).unwrap();
            assert_eq!(json["kind"],label);
            assert_eq!(serde_json::from_value::<ProviderRejection>(json).unwrap(),value);
            assert_eq!(kind.same_input_retryable(),kind==RejectionKind::RateLimited);
        }
        assert!(serde_json::from_str::<RejectionKind>("\"overloaded\"").is_err());
    }
}
