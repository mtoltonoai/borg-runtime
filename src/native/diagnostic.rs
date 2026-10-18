//! Source-selected provider facts, independent of a display-summary byte limit.
//! These observations never authorize retry or prove an ambiguous POST absent.
use super::{ProviderErrorCategory, RetryAfter};
use crate::limits::NativeLimits;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderFailureClass {
    LocalAdmission,
    HttpResponsePending,
    TransportUnknown,
    ConnectTimeout,
    Deadline,
    Cancelled,
    ResponseRead,
    ResponseBound,
    ResponseValidation,
    HttpRefused,
    ValidationCleanup,
    OutputLimit,
    Redacted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticBodyState {
    Pending,
    Complete,
    Empty,
    InvalidJson,
    DuplicateKey,
    DepthBound,
    NodeBound,
    ReadFailed,
    AdvertisedBound,
    StreamBound,
    ReadTimeout,
    TaskDeadline,
    Cancelled,
    Redacted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum NumericDiagnostic {
    Absent,
    Value(u64),
    Invalid,
    Duplicate,
    SourceBound,
    Redacted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum RetryHeaderDiagnostic {
    Absent,
    Seconds(u64),
    DateUnixMs(u64),
    Invalid,
    Duplicate,
    SourceBound,
    Redacted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum HttpStatusDiagnostic {
    Value(u16),
    Redacted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum CategoryDiagnostic {
    Absent,
    Value(ProviderErrorCategory),
    Redacted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum SchedulerRetryDiagnostic {
    Value(RetryAfter),
    Redacted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaDimension {
    TokensPerMinute,
    RequestsPerMinute,
}

/// Exact digits from the already-selected complete quota-message grammar.
/// Fractions are never rounded, parsed as floats or used for retry scheduling.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaValues {
    pub dimension: QuotaDimension,
    pub limit: String,
    pub used: String,
    pub requested: String,
    pub wait_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum QuotaDiagnostic {
    Absent,
    Unrecognized,
    InvalidType,
    Redacted,
    Value(QuotaValues),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpDiagnostic {
    pub status: HttpStatusDiagnostic,
    pub body: DiagnosticBodyState,
    pub category: CategoryDiagnostic,
    /// Original accepted scheduling policy, not the broader diagnostic parser.
    pub scheduler_retry_after: SchedulerRetryDiagnostic,
    pub retry_after: RetryHeaderDiagnostic,
    pub retry_after_ms: NumericDiagnostic,
    pub requests_limit: NumericDiagnostic,
    pub requests_remaining: NumericDiagnostic,
    pub tokens_limit: NumericDiagnostic,
    pub tokens_remaining: NumericDiagnostic,
    pub requests_reset_ms: NumericDiagnostic,
    pub tokens_reset_ms: NumericDiagnostic,
    pub quota: QuotaDiagnostic,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderFailureDiagnostic {
    pub class: ProviderFailureClass,
    pub http: Option<HttpDiagnostic>,
}

impl ProviderFailureDiagnostic {
    pub fn new(class: ProviderFailureClass) -> Self {
        Self { class, http: None }
    }

    pub fn http(class: ProviderFailureClass, facts: HttpDiagnostic) -> Self {
        Self {
            class,
            http: Some(facts),
        }
    }

    pub fn set_class(&mut self, class: ProviderFailureClass) {
        self.class = class;
    }

    pub fn set_body(&mut self, body: DiagnosticBodyState) {
        if let Some(http) = &mut self.http {
            http.body = body;
        }
    }

    /// Screen normalized values as well as raw-source screening in the parser.
    /// The worker must still apply its complete configured-credential scalar
    /// scan to the final report before journaling; this adapter knows one key.
    pub fn redact_credential(&mut self, key: &str) {
        if serde_json::to_string(&self.class).is_ok_and(|text| text.contains(key)) {
            self.class = ProviderFailureClass::Redacted;
        }
        let Some(http) = &mut self.http else {
            return;
        };
        if matches!(http.status,HttpStatusDiagnostic::Value(value) if value.to_string().contains(key))
        {
            http.status = HttpStatusDiagnostic::Redacted;
        }
        if serde_json::to_string(&http.body).is_ok_and(|text| text.contains(key)) {
            http.body = DiagnosticBodyState::Redacted;
        }
        if matches!(http.category,CategoryDiagnostic::Value(value) if serde_json::to_string(&value).is_ok_and(|text|text.contains(key)))
        {
            http.category = CategoryDiagnostic::Redacted;
        }
        if matches!(http.scheduler_retry_after,SchedulerRetryDiagnostic::Value(RetryAfter::DelayMs(value)) if value.to_string().contains(key))
        {
            http.scheduler_retry_after = SchedulerRetryDiagnostic::Redacted;
        }
        if matches!(http.retry_after,RetryHeaderDiagnostic::Seconds(value)|RetryHeaderDiagnostic::DateUnixMs(value) if value.to_string().contains(key))
        {
            http.retry_after = RetryHeaderDiagnostic::Redacted;
        }
        for value in [
            &mut http.retry_after_ms,
            &mut http.requests_limit,
            &mut http.requests_remaining,
            &mut http.tokens_limit,
            &mut http.tokens_remaining,
            &mut http.requests_reset_ms,
            &mut http.tokens_reset_ms,
        ] {
            if matches!(*value,NumericDiagnostic::Value(value) if value.to_string().contains(key)) {
                *value = NumericDiagnostic::Redacted;
            }
        }
        if let QuotaDiagnostic::Value(quota) = &http.quota {
            if [&quota.limit, &quota.used, &quota.requested]
                .into_iter()
                .any(|v| v.contains(key))
                || quota.wait_ms.to_string().contains(key)
                || serde_json::to_string(&quota.dimension).is_ok_and(|text| text.contains(key))
            {
                http.quota = QuotaDiagnostic::Redacted;
            }
        }
    }

    /// Conservative serialized DTO upper bound derived from its actual serde
    /// shape and the source-byte bound, without allocating the source-sized
    /// quota strings. Full Report/receipt/outbox admission is a separate join.
    pub fn maximum_encoded_bytes(policy: &NativeLimits) -> Result<usize, String> {
        let maximum = Self::http(
            ProviderFailureClass::HttpResponsePending,
            HttpDiagnostic {
                status: HttpStatusDiagnostic::Value(999),
                body: DiagnosticBodyState::AdvertisedBound,
                category: CategoryDiagnostic::Value(ProviderErrorCategory::AuthenticationFailed),
                scheduler_retry_after: SchedulerRetryDiagnostic::Value(RetryAfter::DelayMs(
                    u64::MAX,
                )),
                retry_after: RetryHeaderDiagnostic::DateUnixMs(u64::MAX),
                retry_after_ms: NumericDiagnostic::Value(u64::MAX),
                requests_limit: NumericDiagnostic::Value(u64::MAX),
                requests_remaining: NumericDiagnostic::Value(u64::MAX),
                tokens_limit: NumericDiagnostic::Value(u64::MAX),
                tokens_remaining: NumericDiagnostic::Value(u64::MAX),
                requests_reset_ms: NumericDiagnostic::Value(u64::MAX),
                tokens_reset_ms: NumericDiagnostic::Value(u64::MAX),
                quota: QuotaDiagnostic::Value(QuotaValues {
                    dimension: QuotaDimension::RequestsPerMinute,
                    limit: String::new(),
                    used: String::new(),
                    requested: String::new(),
                    wait_ms: u64::MAX,
                }),
            },
        );
        let overhead = serde_json::to_vec(&maximum)
            .map_err(|_| "native diagnostic bound encoding failed")?
            .len();
        overhead
            .checked_add(policy.error_body_bytes)
            .ok_or_else(|| "native diagnostic bound overflow".into())
    }

    /// Shape/source bounds only; original request, receipt and provider outcome
    /// consistency belong to the controller's exact original-occurrence join.
    //= spec/impl/v2/implementation.md#platform-owned-model-retries
    //# A failed platform-owned provider request MUST retain bounded diagnostics sufficient to identify the observed failure class and available provider retry or quota information.
    pub fn validate(&self, policy: &NativeLimits) -> Result<(), String> {
        if let Some(http) = &self.http {
            if matches!(http.status, HttpStatusDiagnostic::Value(status) if !(100..=999).contains(&status))
            {
                return Err("native diagnostic HTTP status invalid".into());
            }
            if let QuotaDiagnostic::Value(quota) = &http.quota {
                let mut bytes = 0usize;
                for value in [&quota.limit, &quota.used, &quota.requested] {
                    if !quota_number(value) {
                        return Err("native diagnostic quota number invalid".into());
                    }
                    bytes = bytes
                        .checked_add(value.len())
                        .ok_or("native diagnostic quota size overflow")?;
                }
                if bytes > policy.error_body_bytes {
                    return Err("native diagnostic quota exceeds native.error_body_bytes".into());
                }
            }
        }
        Ok(())
    }
}

pub(super) fn quota_number(value: &str) -> bool {
    let whole = |v: &str| {
        !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()) && v.parse::<u64>().is_ok()
    };
    match value.split_once('.') {
        Some((integer, fraction)) => {
            whole(integer) && !fraction.is_empty() && fraction.bytes().all(|b| b.is_ascii_digit())
        }
        None => whole(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sample() -> ProviderFailureDiagnostic {
        ProviderFailureDiagnostic::http(
            ProviderFailureClass::HttpResponsePending,
            HttpDiagnostic {
                status: HttpStatusDiagnostic::Value(503),
                body: DiagnosticBodyState::Pending,
                category: CategoryDiagnostic::Absent,
                scheduler_retry_after: SchedulerRetryDiagnostic::Value(RetryAfter::Absent),
                retry_after: RetryHeaderDiagnostic::Absent,
                retry_after_ms: NumericDiagnostic::Absent,
                requests_limit: NumericDiagnostic::Absent,
                requests_remaining: NumericDiagnostic::Absent,
                tokens_limit: NumericDiagnostic::Absent,
                tokens_remaining: NumericDiagnostic::Absent,
                requests_reset_ms: NumericDiagnostic::Absent,
                tokens_reset_ms: NumericDiagnostic::Absent,
                quota: QuotaDiagnostic::Absent,
            },
        )
    }
    #[test]
    fn bounded_shape_roundtrips_exact_lexemes_and_refuses_unknown_fields() {
        let mut policy = crate::limits::RuntimeLimits::seed()
            .unwrap()
            .execution
            .native;
        let mut facts = sample();
        let quota = QuotaValues {
            dimension: QuotaDimension::TokensPerMinute,
            limit: "1.000000000000000000000000000000000000000001".into(),
            used: "0000".into(),
            requested: u64::MAX.to_string(),
            wait_ms: u64::MAX,
        };
        policy.error_body_bytes = quota.limit.len() + quota.used.len() + quota.requested.len();
        facts.http.as_mut().unwrap().quota = QuotaDiagnostic::Value(quota);
        facts.validate(&policy).unwrap();
        let bytes = serde_json::to_vec(&facts).unwrap();
        assert_eq!(
            serde_json::from_slice::<ProviderFailureDiagnostic>(&bytes).unwrap(),
            facts
        );
        assert!(bytes.len() <= ProviderFailureDiagnostic::maximum_encoded_bytes(&policy).unwrap());
        policy.error_body_bytes -= 1;
        assert!(facts
            .validate(&policy)
            .unwrap_err()
            .contains("native.error_body_bytes"));
        let mut encoded = serde_json::to_value(&facts).unwrap();
        encoded["http"]["provider_message"] = "PRIVATE".into();
        assert!(serde_json::from_value::<ProviderFailureDiagnostic>(encoded).is_err());
        for invalid in [
            "",
            "-1",
            "1.",
            ".1",
            "1e3",
            "18446744073709551616",
            "1.0 private",
        ] {
            if let QuotaDiagnostic::Value(quota) = &mut facts.http.as_mut().unwrap().quota {
                quota.limit = invalid.into();
            }
            assert!(facts.validate(&policy).is_err(), "{invalid}");
        }
    }
    #[test]
    fn serialized_bound_covers_source_sized_quota_and_every_closed_variant() {
        let mut policy = crate::limits::RuntimeLimits::seed()
            .unwrap()
            .execution
            .native;
        for source_bytes in [1usize, 3, 32, 8192] {
            policy.error_body_bytes = source_bytes;
            let bound = ProviderFailureDiagnostic::maximum_encoded_bytes(&policy).unwrap();
            let mut facts = sample();
            let http = facts.http.as_mut().unwrap();
            let numeric = NumericDiagnostic::Value(u64::MAX);
            http.retry_after_ms = numeric;
            http.requests_limit = numeric;
            http.requests_remaining = numeric;
            http.tokens_limit = numeric;
            http.tokens_remaining = numeric;
            http.requests_reset_ms = numeric;
            http.tokens_reset_ms = numeric;
            http.status = HttpStatusDiagnostic::Value(999);
            http.scheduler_retry_after =
                SchedulerRetryDiagnostic::Value(RetryAfter::DelayMs(u64::MAX));
            http.retry_after = RetryHeaderDiagnostic::DateUnixMs(u64::MAX);
            http.category = CategoryDiagnostic::Value(ProviderErrorCategory::AuthenticationFailed);
            if source_bytes >= 3 {
                http.quota = QuotaDiagnostic::Value(QuotaValues {
                    dimension: QuotaDimension::RequestsPerMinute,
                    limit: if source_bytes >= 5 {
                        format!("0.{}", "1".repeat(source_bytes - 4))
                    } else {
                        "0".into()
                    },
                    used: "0".into(),
                    requested: "0".into(),
                    wait_ms: u64::MAX,
                });
            }
            for class in [
                ProviderFailureClass::LocalAdmission,
                ProviderFailureClass::HttpResponsePending,
                ProviderFailureClass::TransportUnknown,
                ProviderFailureClass::ConnectTimeout,
                ProviderFailureClass::Deadline,
                ProviderFailureClass::Cancelled,
                ProviderFailureClass::ResponseRead,
                ProviderFailureClass::ResponseBound,
                ProviderFailureClass::ResponseValidation,
                ProviderFailureClass::HttpRefused,
                ProviderFailureClass::ValidationCleanup,
                ProviderFailureClass::OutputLimit,
                ProviderFailureClass::Redacted,
            ] {
                facts.class = class;
                for body in [
                    DiagnosticBodyState::Pending,
                    DiagnosticBodyState::Complete,
                    DiagnosticBodyState::Empty,
                    DiagnosticBodyState::InvalidJson,
                    DiagnosticBodyState::DuplicateKey,
                    DiagnosticBodyState::DepthBound,
                    DiagnosticBodyState::NodeBound,
                    DiagnosticBodyState::ReadFailed,
                    DiagnosticBodyState::AdvertisedBound,
                    DiagnosticBodyState::StreamBound,
                    DiagnosticBodyState::ReadTimeout,
                    DiagnosticBodyState::TaskDeadline,
                    DiagnosticBodyState::Cancelled,
                    DiagnosticBodyState::Redacted,
                ] {
                    facts.http.as_mut().unwrap().body = body;
                    facts.validate(&policy).unwrap();
                    assert!(
                        serde_json::to_vec(&facts).unwrap().len() <= bound,
                        "{source_bytes} {class:?} {body:?}"
                    );
                }
            }
            let before = bound;
            policy.error_body_bytes = source_bytes + 1;
            assert_eq!(
                ProviderFailureDiagnostic::maximum_encoded_bytes(&policy).unwrap(),
                before + 1
            );
        }
        policy.error_body_bytes = usize::MAX;
        assert!(ProviderFailureDiagnostic::maximum_encoded_bytes(&policy).is_err());
    }
    #[test]
    //= spec/impl/v2/implementation.md#platform-owned-model-retries
    //= type=test
    //# Retained provider diagnostics MUST exclude configured credentials and submitted prompt content.
    fn scalar_redaction_preserves_explicit_unknown_instead_of_zero_or_absence() {
        let mut facts = sample();
        let http = facts.http.as_mut().unwrap();
        http.status = HttpStatusDiagnostic::Value(429);
        http.retry_after = RetryHeaderDiagnostic::Seconds(429);
        http.retry_after_ms = NumericDiagnostic::Value(429);
        http.requests_limit = NumericDiagnostic::Value(429);
        http.requests_remaining = NumericDiagnostic::Value(429);
        http.tokens_limit = NumericDiagnostic::Value(429);
        http.tokens_remaining = NumericDiagnostic::Value(429);
        http.requests_reset_ms = NumericDiagnostic::Value(429);
        http.tokens_reset_ms = NumericDiagnostic::Value(429);
        http.scheduler_retry_after = SchedulerRetryDiagnostic::Value(RetryAfter::DelayMs(429));
        http.quota = QuotaDiagnostic::Value(QuotaValues {
            dimension: QuotaDimension::RequestsPerMinute,
            limit: "429.00".into(),
            used: "1".into(),
            requested: "2".into(),
            wait_ms: 3,
        });
        facts.redact_credential("429");
        let http = facts.http.as_ref().unwrap();
        assert_eq!(http.status, HttpStatusDiagnostic::Redacted);
        assert_eq!(http.retry_after, RetryHeaderDiagnostic::Redacted);
        assert_eq!(
            http.scheduler_retry_after,
            SchedulerRetryDiagnostic::Redacted
        );
        assert_eq!(http.quota, QuotaDiagnostic::Redacted);
        for value in [
            http.retry_after_ms,
            http.requests_limit,
            http.requests_remaining,
            http.tokens_limit,
            http.tokens_remaining,
            http.requests_reset_ms,
            http.tokens_reset_ms,
        ] {
            assert_eq!(value, NumericDiagnostic::Redacted);
        }
        assert!(!serde_json::to_string(&facts).unwrap().contains("429"));
    }
}
