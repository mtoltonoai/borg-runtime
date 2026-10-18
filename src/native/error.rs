//! HTTP refusal diagnostics are finite labels, never arbitrary provider text.
//! Selected bounded response headers supplement labels without retaining prose.
//! Only a complete selected refusal can establish a known rejection. Categories
//! are separate from that proof and never authorize replay of an ambiguous POST.
use super::diagnostic::*;
use super::{decode, ProviderErrorCategory, ProviderRejection, RejectionKind, RetryAfter};
use crate::limits::NativeLimits;
use serde_json::Value;
use std::time::{Duration, SystemTime};
use tokio::time::Instant;

/// Private request-local capture: the scheduler value is never reconstructed
/// from a redacted diagnostic or a later wall-clock observation.
pub(super) struct HeaderObservation {
    pub facts: HttpDiagnostic,
    retry_after: RetryAfter,
}

pub(super) struct ErrorObservation {
    pub diagnostic: String,
    pub category: Option<ProviderErrorCategory>,
    pub rejection: Option<ProviderRejection>,
    pub facts: ProviderFailureDiagnostic,
}

pub(super) async fn diagnostic(
    response: &mut reqwest::Response,
    key: &str,
    deadline: Instant,
    policy: &NativeLimits,
) -> String {
    let initial = head(response, key, policy);
    inspect(response, key, deadline, policy, initial)
        .await
        .diagnostic
}

//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A provider quota exhaustion MUST be distinguished from transient throttling.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# Retained provider diagnostics MUST exclude configured credentials and submitted prompt content.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A failed platform-owned provider request MUST retain bounded diagnostics sufficient to identify the observed failure class and available provider retry or quota information.
pub(super) async fn inspect(
    response: &mut reqwest::Response,
    key: &str,
    deadline: Instant,
    policy: &NativeLimits,
    initial: HeaderObservation,
) -> ErrorObservation {
    let mut fields = header_diagnostics(response.headers(), key, policy.summary_bytes);
    let retry_after = initial.retry_after;
    let mut initial = initial.facts;
    if retry_after == RetryAfter::Invalid {
        fields.push(("retry_after_state", "invalid".into()));
    }
    let status = response.status().as_u16();
    let class = match status {
        401 | 403 => "native_provider_authentication_failed",
        429 => "native_provider_rate_limited",
        500..=599 => "native_provider_unavailable",
        300..=399 => "native_provider_redirect_refused",
        _ => "native_provider_request_refused",
    };
    let mut result = format!("{class} status={status}");
    let unknown = |result, fields: &[(&str, String)], initial: HttpDiagnostic, class| {
        let mut facts = ProviderFailureDiagnostic::http(class, initial);
        facts.redact_credential(key);
        ErrorObservation {
            diagnostic: finish(result, fields, policy.summary_bytes),
            category: None,
            rejection: None,
            facts,
        }
    };
    let read_deadline = Instant::now()
        .checked_add(Duration::from_millis(policy.error_read_timeout_ms))
        .unwrap_or(deadline)
        .min(deadline);
    let body = match tokio::time::timeout_at(read_deadline, body(response, policy.error_body_bytes))
        .await
    {
        Ok(Ok(body)) => body,
        Ok(Err(state)) => {
            fields.push(("error_body", state.label().into()));
            initial.body = state.body();
            return unknown(result, &fields, initial, state.class());
        }
        Err(_) => {
            fields.push((
                "error_body",
                if read_deadline == deadline {
                    "timed_out_task_deadline"
                } else {
                    "timed_out_native_error_read_timeout_ms"
                }
                .into(),
            ));
            initial.body = if read_deadline == deadline {
                DiagnosticBodyState::TaskDeadline
            } else {
                DiagnosticBodyState::ReadTimeout
            };
            return unknown(result, &fields, initial, ProviderFailureClass::Deadline);
        }
    };
    // Reuse bounded JSON admission, including duplicate-key refusal. Diagnostics
    // may be absent for truncated, oversized, malformed or slow error bodies.
    let value = match decode::parse(
        &body,
        policy.error_body_bytes,
        read_deadline,
        "native_error_not_json",
        policy,
        "native_error_body_bytes",
    ) {
        Ok(value) => value,
        Err(code) => {
            let state = if body.is_empty() {
                "empty"
            } else {
                match code.as_str() {
                    "native_json_duplicate_key" => "duplicate_key",
                    "native_json_complexity_bound_native_json_depth" => "bound_native_json_depth",
                    "native_json_complexity_bound_native_json_nodes" => "bound_native_json_nodes",
                    "native_deadline_exceeded" => "deadline_exceeded",
                    _ => "invalid_json",
                }
            };
            fields.push(("error_body", state.into()));
            initial.body = match state {
                "empty" => DiagnosticBodyState::Empty,
                "duplicate_key" => DiagnosticBodyState::DuplicateKey,
                "bound_native_json_depth" => DiagnosticBodyState::DepthBound,
                "bound_native_json_nodes" => DiagnosticBodyState::NodeBound,
                "deadline_exceeded" => DiagnosticBodyState::TaskDeadline,
                _ => DiagnosticBodyState::InvalidJson,
            };
            let class = if initial.body == DiagnosticBodyState::TaskDeadline {
                ProviderFailureClass::Deadline
            } else {
                ProviderFailureClass::HttpRefused
            };
            return unknown(result, &fields, initial, class);
        }
    };
    fields.push(("error_body", "complete".into()));
    initial.body = DiagnosticBodyState::Complete;
    let classification = classify(status, &value);
    if let Err(reason) = classification {
        // A fixed source label explains refusal without retaining any provider
        // field names, strings or prose. This is diagnostic only, never proof.
        // The existing all-or-bounded summary policy still applies.
        if !reason.contains(key) {
            fields.insert(0, ("classification_refused", reason.into()));
        }
    }
    let Some(error) = value.get("error").and_then(Value::as_object) else {
        return unknown(result, &fields, initial, ProviderFailureClass::HttpRefused);
    };
    for (field, labels) in [("type", TYPES), ("code", CODES), ("param", PARAMS)] {
        if let Some(candidate) = error.get(field).and_then(Value::as_str) {
            // Select a source literal, not an arbitrary provider string matching
            // a character pattern. Never retain provider messages or URLs.
            if let Some(label) = labels
                .iter()
                .find(|label| **label == candidate && !label.contains(key))
            {
                result.push(' ');
                result.push_str(field);
                result.push('=');
                result.push_str(label);
            }
        }
    }
    fields.extend(message_diagnostics(error.get("message"), key));
    initial.quota = quota_diagnostic(error.get("message"), key);
    let category = classification.ok();
    initial.category = category
        .map(CategoryDiagnostic::Value)
        .unwrap_or(CategoryDiagnostic::Absent);
    if let Some(category) = category {
        fields.push(("category", category_label(category).into()));
    }
    let rejection = category
        .and_then(rejection_kind)
        .map(|kind| ProviderRejection { kind, retry_after });
    let mut facts = ProviderFailureDiagnostic::http(ProviderFailureClass::HttpRefused, initial);
    facts.redact_credential(key);
    ErrorObservation {
        diagnostic: finish(result, &fields, policy.summary_bytes),
        category,
        rejection,
        facts,
    }
}

// This mapping is reached only after the complete bounded, duplicate-free,
// closed error envelope and compatible status/type/code checks below. A known
// rejection is not a same-input retry grant or evidence of zero provider usage.
fn rejection_kind(category: ProviderErrorCategory) -> Option<RejectionKind> {
    Some(match category {
        ProviderErrorCategory::RateLimited => RejectionKind::RateLimited,
        ProviderErrorCategory::InvalidRequest => RejectionKind::InvalidRequest,
        ProviderErrorCategory::ContextLimit => RejectionKind::ContextLimit,
        ProviderErrorCategory::PolicyRefused => RejectionKind::PolicyRefused,
        ProviderErrorCategory::QuotaExhausted => RejectionKind::QuotaExhausted,
        ProviderErrorCategory::AuthenticationFailed => RejectionKind::AuthenticationFailed,
        // A server_is_overloaded label alone does not establish that a POST was
        // not accepted. No selected documented no-acceptance envelope exists
        // here, so overload and generic 5xx remain Unknown provider outcomes.
        ProviderErrorCategory::Overloaded => return None,
    })
}

fn category_label(category: ProviderErrorCategory) -> &'static str {
    match category {
        ProviderErrorCategory::RateLimited => "rate_limited",
        ProviderErrorCategory::Overloaded => "overloaded",
        ProviderErrorCategory::InvalidRequest => "invalid_request",
        ProviderErrorCategory::ContextLimit => "context_limit",
        ProviderErrorCategory::PolicyRefused => "policy_refused",
        ProviderErrorCategory::QuotaExhausted => "quota_exhausted",
        ProviderErrorCategory::AuthenticationFailed => "authentication_failed",
    }
}

// This runs only after bounded, duplicate-free admission of the complete body.
// Closed keys and compatible status/code/type prevent a recognized substring or
// an error object hidden in a different envelope from becoming retry proof.
#[cfg(test)]
fn category(status: u16, value: &Value) -> Option<ProviderErrorCategory> {
    classify(status, value).ok()
}

fn classify(status: u16, value: &Value) -> Result<ProviderErrorCategory, &'static str> {
    use ProviderErrorCategory::*;
    let root = value.as_object().ok_or("invalid_root")?;
    if root.keys().any(|key| key != "error") {
        return Err("unexpected_root_keys");
    }
    let error = root.get("error").ok_or("missing_error")?
        .as_object().ok_or("invalid_error")?;
    if error
        .keys()
        .any(|key| !["message", "type", "param", "code"].contains(&key.as_str()))
    {
        return Err("unexpected_error_keys");
    }
    if error.get("message").is_some_and(|value| !value.is_string())
        || error
            .get("param")
            .is_some_and(|value| !value.is_null() && !value.is_string())
        || error.get("type").is_some_and(|value| !value.is_null() && !value.is_string())
    {
        return Err("invalid_field_type");
    }
    let code = match error.get("code") {
        None | Some(Value::Null) => None,
        Some(Value::String(code)) => Some(code.as_str()),
        _ => return Err("invalid_field_type"),
    };
    let compatible_type = |expected: &[&str]| {
        error.get("type").is_none_or(|value| {
            value.is_null() || value.as_str().is_some_and(|kind| expected.contains(&kind))
        })
    };
    let category = match code {
        // The provider documents this exact HTTP refusal before a response
        // stream begins. Unlike the historical rate_limit_exceeded envelope,
        // the newly admitted slow_down code requires its documented type.
        // https://developers.openai.com/api/docs/guides/rate-limits
        Some("slow_down")
            if status == 429 && error.get("type").and_then(Value::as_str) == Some("rate_limit_error") =>
        {
            Some(RateLimited)
        }
        // Firsthand Responses.create HTTP example in the upstream SDK issue:
        // https://github.com/openai/openai-node/issues/1477
        // This is a selected observed compatibility shape, not a claim about
        // any previously retained Unknown response whose body was discarded.
        Some("rate_limit_exceeded")
            if status == 429 && (compatible_type(&["rate_limit_error"])
                || (error.get("type").and_then(Value::as_str) == Some("tokens")
                    && error.get("message").is_some_and(Value::is_string)
                    && error.get("param") == Some(&Value::Null))) =>
        {
            Some(RateLimited)
        }
        Some("server_is_overloaded")
            if matches!(status, 429 | 500..=599)
                && compatible_type(&["server_error", "service_unavailable_error"]) =>
        {
            Some(Overloaded)
        }
        Some("context_length_exceeded")
            if matches!(status, 400 | 413 | 422) && compatible_type(&["invalid_request_error"]) =>
        {
            Some(ContextLimit)
        }
        Some("content_policy_violation" | "policy_violation")
            if matches!(status, 400 | 403 | 422)
                && compatible_type(&["invalid_request_error", "permission_error"]) =>
        {
            Some(PolicyRefused)
        }
        Some(
            "insufficient_quota"
            | "credit_balance_exhausted"
            | "organization_spend_limit_exceeded"
            | "project_spend_limit_exceeded"
            | "organization_usage_limit_exceeded",
        ) if matches!(status, 402 | 403 | 429)
            && compatible_type(&[
                "insufficient_quota",
                "invalid_request_error",
                "rate_limit_error",
            ]) =>
        {
            Some(QuotaExhausted)
        }
        Some("invalid_api_key")
            if matches!(status, 401 | 403)
                && compatible_type(&[
                    "authentication_error",
                    "permission_error",
                    "invalid_request_error",
                ]) =>
        {
            Some(AuthenticationFailed)
        }
        Some(
            "invalid_json_schema"
            | "invalid_value"
            | "invalid_type"
            | "invalid_request_error"
            | "missing_required_parameter"
            | "unknown_parameter"
            | "unsupported_parameter"
            | "unsupported_value"
            | "model_not_found",
        ) if matches!(status, 400 | 404 | 422)
            && compatible_type(&["invalid_request_error", "not_found_error"]) =>
        {
            Some(InvalidRequest)
        }
        None => match error.get("type").and_then(Value::as_str) {
            Some("authentication_error") if matches!(status, 401 | 403) => {
                Some(AuthenticationFailed)
            }
            Some("permission_error") if status == 403 => Some(PolicyRefused),
            Some("invalid_request_error") if matches!(status, 400 | 404 | 422) => {
                Some(InvalidRequest)
            }
            Some("insufficient_quota") if matches!(status, 402 | 403 | 429) => Some(QuotaExhausted),
            _ => None,
        },
        _ => None,
    };
    category.ok_or_else(|| {
        if error.get("type").and_then(Value::as_str).is_some_and(|kind| !TYPES.contains(&kind)) {
            "unsupported_type"
        } else if code.is_some_and(|code| !CODES.contains(&code)) {
            "unsupported_code"
        } else if code.is_none() && error.get("type").is_none_or(Value::is_null) {
            "missing_discriminator"
        } else {
            "status_or_type_mismatch"
        }
    })
}

#[derive(Clone, Copy)]
enum BodyFailure {
    AdvertisedBound,
    StreamBound,
    ReadFailed,
}
impl BodyFailure {
    fn label(self) -> &'static str {
        match self {
            Self::AdvertisedBound => "advertised_bound_native_error_body_bytes",
            Self::StreamBound => "stream_bound_native_error_body_bytes",
            Self::ReadFailed => "read_failed",
        }
    }
    fn body(self) -> DiagnosticBodyState {
        match self {
            Self::AdvertisedBound => DiagnosticBodyState::AdvertisedBound,
            Self::StreamBound => DiagnosticBodyState::StreamBound,
            Self::ReadFailed => DiagnosticBodyState::ReadFailed,
        }
    }
    fn class(self) -> ProviderFailureClass {
        match self {
            Self::AdvertisedBound | Self::StreamBound => ProviderFailureClass::ResponseBound,
            Self::ReadFailed => ProviderFailureClass::ResponseRead,
        }
    }
}
async fn body(response: &mut reqwest::Response, limit: usize) -> Result<Vec<u8>, BodyFailure> {
    if response.content_length().is_some_and(|n| n > limit as u64) {
        return Err(BodyFailure::AdvertisedBound);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| BodyFailure::ReadFailed)?
    {
        if body
            .len()
            .checked_add(chunk.len())
            .is_none_or(|bytes| bytes > limit)
        {
            return Err(BodyFailure::StreamBound);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

const TYPES: &[&str] = &[
    "invalid_request_error",
    "authentication_error",
    "permission_error",
    "not_found_error",
    "rate_limit_error",
    "tokens",
    "insufficient_quota",
    "server_error",
    "service_unavailable_error",
];
const CODES: &[&str] = &[
    "invalid_json_schema",
    "invalid_value",
    "invalid_type",
    "invalid_request_error",
    "missing_required_parameter",
    "unknown_parameter",
    "unsupported_parameter",
    "unsupported_value",
    "model_not_found",
    "invalid_api_key",
    "insufficient_quota",
    "rate_limit_exceeded",
    "context_length_exceeded",
    "content_policy_violation",
    "policy_violation",
    "credit_balance_exhausted",
    "organization_spend_limit_exceeded",
    "project_spend_limit_exceeded",
    "organization_usage_limit_exceeded",
    "slow_down",
    "server_is_overloaded",
];
const PARAMS: &[&str] = &[
    "model",
    "input",
    "max_output_tokens",
    "text",
    "text.format",
    "text.format.type",
    "text.format.name",
    "text.format.schema",
    "text.format.strict",
    "tools",
    "tool_choice",
    "store",
    "stream",
    "service_tier",
];

// This is an operational projection, not retry authorization. Capture only
// source-selected fields; an unknown value never becomes provider prose in logs.
fn unique_header<'a>(
    headers: &'a reqwest::header::HeaderMap,
    name: &'static str,
    key: &str,
    limit: usize,
) -> Option<&'a str> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?;
    if values.next().is_some() || value.as_bytes().len() > limit {
        return None;
    }
    let value = value.to_str().ok()?;
    if value.is_empty() || value.contains(key) {
        return None;
    }
    Some(value)
}

// Capture fixed selected header observations synchronously, before any body await.
// The display and scheduling bounds remain independent from source admission.
pub(super) fn head(
    response: &reqwest::Response,
    key: &str,
    policy: &NativeLimits,
) -> HeaderObservation {
    capture_headers(
        response.status().as_u16(),
        response.headers(),
        key,
        policy,
        SystemTime::now(),
    )
}

#[derive(Clone, Copy)]
enum HeaderFailure {
    Invalid,
    Duplicate,
    SourceBound,
    Redacted,
}
fn selected_header<'a>(
    headers: &'a reqwest::header::HeaderMap,
    name: &'static str,
    key: &str,
    limit: usize,
) -> Result<Option<&'a str>, HeaderFailure> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(HeaderFailure::Duplicate);
    }
    if value.as_bytes().len() > limit {
        return Err(HeaderFailure::SourceBound);
    }
    let value = value.to_str().map_err(|_| HeaderFailure::Invalid)?;
    if value.contains(key) {
        return Err(HeaderFailure::Redacted);
    }
    if value.is_empty() {
        return Err(HeaderFailure::Invalid);
    }
    Ok(Some(value))
}

fn numeric_header(
    headers: &reqwest::header::HeaderMap,
    name: &'static str,
    key: &str,
    limit: usize,
    parse: fn(&str) -> Option<u64>,
) -> NumericDiagnostic {
    match selected_header(headers, name, key, limit) {
        Ok(None) => NumericDiagnostic::Absent,
        Ok(Some(value)) => parse(value)
            .map(NumericDiagnostic::Value)
            .unwrap_or(NumericDiagnostic::Invalid),
        Err(HeaderFailure::Invalid) => NumericDiagnostic::Invalid,
        Err(HeaderFailure::Duplicate) => NumericDiagnostic::Duplicate,
        Err(HeaderFailure::SourceBound) => NumericDiagnostic::SourceBound,
        Err(HeaderFailure::Redacted) => NumericDiagnostic::Redacted,
    }
}

fn capture_headers(
    status: u16,
    headers: &reqwest::header::HeaderMap,
    key: &str,
    policy: &NativeLimits,
    observed_wall: SystemTime,
) -> HeaderObservation {
    let limit = policy.error_body_bytes;
    let scheduler_retry_after = retry_after(headers, key, policy.summary_bytes, observed_wall);
    let retry_header = match selected_header(headers, "retry-after", key, limit) {
        Ok(None) => RetryHeaderDiagnostic::Absent,
        Ok(Some(value)) => {
            if let Some(seconds) = decimal(value) {
                RetryHeaderDiagnostic::Seconds(seconds)
            } else if let Some(ms) = httpdate::parse_http_date(value)
                .ok()
                .and_then(|date| date.duration_since(SystemTime::UNIX_EPOCH).ok())
                .and_then(|duration| duration.as_secs().checked_mul(1_000))
            {
                RetryHeaderDiagnostic::DateUnixMs(ms)
            } else {
                RetryHeaderDiagnostic::Invalid
            }
        }
        Err(HeaderFailure::Invalid) => RetryHeaderDiagnostic::Invalid,
        Err(HeaderFailure::Duplicate) => RetryHeaderDiagnostic::Duplicate,
        Err(HeaderFailure::SourceBound) => RetryHeaderDiagnostic::SourceBound,
        Err(HeaderFailure::Redacted) => RetryHeaderDiagnostic::Redacted,
    };
    let numeric = |name| numeric_header(headers, name, key, limit, decimal);
    let mut facts = ProviderFailureDiagnostic::http(
        ProviderFailureClass::HttpResponsePending,
        HttpDiagnostic {
            status: HttpStatusDiagnostic::Value(status),
            body: DiagnosticBodyState::Pending,
            category: CategoryDiagnostic::Absent,
            scheduler_retry_after: SchedulerRetryDiagnostic::Value(scheduler_retry_after),
            retry_after: retry_header,
            retry_after_ms: numeric("retry-after-ms"),
            requests_limit: numeric("x-ratelimit-limit-requests"),
            requests_remaining: numeric("x-ratelimit-remaining-requests"),
            tokens_limit: numeric("x-ratelimit-limit-tokens"),
            tokens_remaining: numeric("x-ratelimit-remaining-tokens"),
            requests_reset_ms: numeric_header(
                headers,
                "x-ratelimit-reset-requests",
                key,
                limit,
                reset_millis,
            ),
            tokens_reset_ms: numeric_header(
                headers,
                "x-ratelimit-reset-tokens",
                key,
                limit,
                reset_millis,
            ),
            quota: QuotaDiagnostic::Absent,
        },
    );
    facts.redact_credential(key);
    HeaderObservation {
        facts: facts
            .http
            .expect("header capture always constructs HTTP facts"),
        retry_after: scheduler_retry_after,
    }
}

#[cfg(test)]
fn header_facts(
    status: u16,
    headers: &reqwest::header::HeaderMap,
    key: &str,
    policy: &NativeLimits,
    observed_wall: SystemTime,
) -> HttpDiagnostic {
    capture_headers(status, headers, key, policy, observed_wall).facts
}

fn quota_diagnostic(value: Option<&Value>, key: &str) -> QuotaDiagnostic {
    let Some(value) = value else {
        return QuotaDiagnostic::Absent;
    };
    let Some(message) = value.as_str() else {
        return QuotaDiagnostic::InvalidType;
    };
    if message.contains(key) {
        return QuotaDiagnostic::Redacted;
    }
    let Some(values) = quota_message(message) else {
        return QuotaDiagnostic::Unrecognized;
    };
    // The existing closed grammar returns exactly these five fields, preserving
    // source digits. Do not capture arbitrary message/model/organization text.
    let get = |field| {
        values
            .iter()
            .find(|(name, _)| *name == field)
            .map(|(_, value)| value.as_str())
    };
    let Some(dimension) = get("quota_dimension").and_then(|v| match v {
        "tokens_per_minute" => Some(QuotaDimension::TokensPerMinute),
        "requests_per_minute" => Some(QuotaDimension::RequestsPerMinute),
        _ => None,
    }) else {
        return QuotaDiagnostic::Unrecognized;
    };
    let (Some(limit), Some(used), Some(requested), Some(wait_ms)) = (
        get("quota_limit"),
        get("quota_used"),
        get("quota_requested"),
        get("quota_wait_ms").and_then(decimal),
    ) else {
        return QuotaDiagnostic::Unrecognized;
    };
    if wait_ms.to_string().contains(key) {
        return QuotaDiagnostic::Redacted;
    }
    QuotaDiagnostic::Value(QuotaValues {
        dimension,
        limit: limit.into(),
        used: used.into(),
        requested: requested.into(),
        wait_ms,
    })
}

// A supplied malformed, duplicated, oversized or credential-containing timing
// header is not absence. Retain the refusal proof, but forbid a guessed retry.
fn retry_after(
    headers: &reqwest::header::HeaderMap,
    key: &str,
    limit: usize,
    observed_wall: SystemTime,
) -> RetryAfter {
    let read = |name| {
        if !headers.contains_key(name) {
            return Ok(None);
        }
        unique_header(headers, name, key, limit).map(Some).ok_or(())
    };
    let (Ok(seconds_or_date), Ok(milliseconds)) = (read("retry-after"), read("retry-after-ms"))
    else {
        return RetryAfter::Invalid;
    };
    let delay = match seconds_or_date {
        None => None,
        Some(value) => match retry_after_millis(value, observed_wall) {
            Some(value) => Some(value),
            None => return RetryAfter::Invalid,
        },
    };
    let millis = match milliseconds {
        None => None,
        Some(value) => match decimal(value) {
            Some(value) => Some(value),
            None => return RetryAfter::Invalid,
        },
    };
    match (delay, millis) {
        (None, None) => RetryAfter::Absent,
        (Some(value), None) | (None, Some(value)) => RetryAfter::DelayMs(value),
        // Neither selected provider minimum may be shortened by the other.
        (Some(left), Some(right)) => RetryAfter::DelayMs(left.max(right)),
    }
}

fn retry_after_millis(value: &str, observed_wall: SystemTime) -> Option<u64> {
    if value.bytes().all(|byte| byte.is_ascii_digit()) {
        return decimal(value)?.checked_mul(1_000);
    }
    // `httpdate` is already pinned in Cargo.lock through hyper. Integration
    // declares that exact locked package directly; no hand-written date grammar.
    let target = httpdate::parse_http_date(value).ok()?;
    let duration = target
        .duration_since(observed_wall)
        .unwrap_or(Duration::ZERO);
    duration
        .as_secs()
        .checked_mul(1_000)?
        .checked_add(u64::from(duration.subsec_nanos()).div_ceil(1_000_000))
}

fn fingerprint(value: &str) -> String {
    let hash = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, value.as_bytes());
    hash.as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn message_diagnostics(value: Option<&Value>, key: &str) -> Vec<(&'static str, String)> {
    let Some(value) = value else {
        return Vec::new();
    };
    let Some(message) = value.as_str() else {
        return vec![("provider_message_state", "invalid_type".into())];
    };
    // Check the decoded string, so JSON unicode escapes cannot bypass the key
    // guard. Even when it contains no key, arbitrary prose can echo task input.
    if message.contains(key) {
        return vec![
            ("provider_message_state", "redacted".into()),
            ("provider_message_bytes", message.len().to_string()),
        ];
    }
    let projection = quota_message(message);
    let mut out = vec![
        (
            "provider_message_state",
            if projection.is_some() {
                "projected"
            } else {
                "fingerprinted"
            }
            .into(),
        ),
        ("provider_message_bytes", message.len().to_string()),
        ("provider_message_sha256", fingerprint(message)),
    ];
    if let Some(projection) = projection {
        out.extend(projection);
    }
    out
}

// Recognize one complete quota-message grammar. Model/organization identifiers
// are consumed and discarded. Unknown prose gets only length/fingerprint above;
// these observations never establish rejection or select a retry delay.
fn quota_message(message: &str) -> Option<Vec<(&'static str, String)>> {
    let rest = message.strip_prefix("Rate limit reached for ")?;
    let (model, rest) = rest.split_once(" in organization ")?;
    if model.is_empty() {
        return None;
    }
    let (organization, rest) = rest.split_once(" on ")?;
    if organization.is_empty() {
        return None;
    }
    let (dimension, rest) = if let Some(rest) = rest.strip_prefix("tokens per min (TPM): Limit ") {
        ("tokens_per_minute", rest)
    } else if let Some(rest) = rest.strip_prefix("requests per min (RPM): Limit ") {
        ("requests_per_minute", rest)
    } else {
        return None;
    };
    let (limit, rest) = rest.split_once(", Used ")?;
    let (used, rest) = rest.split_once(", Requested ")?;
    let (requested, rest) = rest.split_once(". Please try again in ")?;
    let wait = rest
        .strip_suffix(". Visit https://platform.openai.com/account/rate-limits to learn more.")?;
    let number = |value: &str| {
        if let Some((whole, fraction)) = value.split_once('.') {
            decimal(whole).is_some()
                && !fraction.is_empty()
                && fraction.bytes().all(|byte| byte.is_ascii_digit())
        } else {
            decimal(value).is_some()
        }
    };
    if ![limit, used, requested].into_iter().all(number) {
        return None;
    }
    let wait = reset_millis(wait)?;
    Some(vec![
        ("quota_dimension", dimension.into()),
        ("quota_limit", limit.into()),
        ("quota_used", used.into()),
        ("quota_requested", requested.into()),
        ("quota_wait_ms", wait.to_string()),
    ])
}

fn decimal(value: &str) -> Option<u64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

// Reset headers use a sequence of numeric duration components, e.g. 1m2.5s.
// Values are observations only. Checked arithmetic rejects unsupported precision
// or representations instead of changing the retry schedule by rounding.
fn reset_millis(mut value: &str) -> Option<u64> {
    if value.is_empty() {
        return None;
    }
    let mut total = 0u64;
    let mut previous_unit = u64::MAX;
    while !value.is_empty() {
        let end = value
            .bytes()
            .take_while(|byte| byte.is_ascii_digit())
            .count();
        if end == 0 {
            return None;
        }
        let whole = decimal(&value[..end])?;
        value = &value[end..];
        let mut fraction = 0u64;
        let mut denominator = 1u64;
        if let Some(rest) = value.strip_prefix('.') {
            let end = rest
                .bytes()
                .take_while(|byte| byte.is_ascii_digit())
                .count();
            if end == 0 {
                return None;
            }
            fraction = decimal(&rest[..end])?;
            denominator = 10u64.checked_pow(u32::try_from(end).ok()?)?;
            value = &rest[end..];
        }
        let (unit, rest) = if let Some(rest) = value.strip_prefix("ms") {
            (1, rest)
        } else if let Some(rest) = value.strip_prefix('s') {
            (1_000, rest)
        } else if let Some(rest) = value.strip_prefix('m') {
            (60_000, rest)
        } else if let Some(rest) = value.strip_prefix('h') {
            (3_600_000, rest)
        } else if let Some(rest) = value.strip_prefix('d') {
            (86_400_000, rest)
        } else {
            return None;
        };
        if unit >= previous_unit {
            return None;
        }
        previous_unit = unit;
        let fractional_millis = fraction.checked_mul(unit)?;
        if fractional_millis % denominator != 0 {
            return None;
        }
        total = total
            .checked_add(whole.checked_mul(unit)?)?
            .checked_add(fractional_millis / denominator)?;
        value = rest;
    }
    Some(total)
}

fn header_diagnostics(
    headers: &reqwest::header::HeaderMap,
    key: &str,
    limit: usize,
) -> Vec<(&'static str, String)> {
    let mut out = Vec::new();
    if let Some(value) = unique_header(headers, "x-request-id", key, limit) {
        // This exact opaque identifier representation can be handed to provider
        // support. Other representations are correlated by fingerprint only.
        if value
            .strip_prefix("req_")
            .is_some_and(|id| id.len() == 32 && id.bytes().all(|byte| byte.is_ascii_hexdigit()))
        {
            out.push(("request_id", value.to_owned()));
        } else {
            out.push(("request_id_sha256", fingerprint(value)));
        }
    }
    if let Some(value) = unique_header(headers, "retry-after", key, limit) {
        if let Some(seconds) = decimal(value) {
            out.push(("retry_after_seconds", seconds.to_string()));
        } else if let Ok(date) = httpdate::parse_http_date(value) {
            if let Ok(duration) = date.duration_since(SystemTime::UNIX_EPOCH) {
                if let Some(millis) = duration.as_secs().checked_mul(1_000) {
                    out.push(("retry_after_unix_ms", millis.to_string()));
                }
            }
        } else {
            out.push(("retry_after_format", "unparsed".into()));
            out.push(("retry_after_sha256", fingerprint(value)));
        }
    }
    for (header, field) in [
        ("retry-after-ms", "retry_after_ms"),
        ("x-ratelimit-limit-requests", "rate_limit_requests_limit"),
        (
            "x-ratelimit-remaining-requests",
            "rate_limit_requests_remaining",
        ),
        ("x-ratelimit-limit-tokens", "rate_limit_tokens_limit"),
        (
            "x-ratelimit-remaining-tokens",
            "rate_limit_tokens_remaining",
        ),
    ] {
        if let Some(value) = unique_header(headers, header, key, limit).and_then(decimal) {
            out.push((field, value.to_string()));
        }
    }
    for (header, field) in [
        ("x-ratelimit-reset-requests", "rate_limit_requests_reset_ms"),
        ("x-ratelimit-reset-tokens", "rate_limit_tokens_reset_ms"),
    ] {
        if let Some(value) = unique_header(headers, header, key, limit).and_then(reset_millis) {
            out.push((field, value.to_string()));
        }
    }
    out
}

fn finish(mut result: String, headers: &[(&str, String)], limit: usize) -> String {
    if headers.is_empty() {
        return result;
    }
    for (field, value) in headers {
        let size = result
            .len()
            .checked_add(field.len())
            .and_then(|n| n.checked_add(value.len()))
            .and_then(|n| n.checked_add(2));
        if size.is_none_or(|size| size > limit) {
            return "native_http_diagnostic_bound_native_summary_bytes".into();
        }
        result.push(' ');
        result.push_str(field);
        result.push('=');
        result.push_str(value);
    }
    result
}

#[cfg(test)]
mod header_tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue};

    #[test]
    fn reset_observations_are_exact_checked_duration_values() {
        for (text, expected) in [
            ("1m2.5s", 62_500),
            ("20ms", 20),
            ("1h2m3s4ms", 3_723_004),
            ("0s", 0),
        ] {
            assert_eq!(reset_millis(text), Some(expected));
        }
        for value in [
            "",
            "1",
            "1s2m",
            "1s2s",
            "-1s",
            "1.0001s",
            "1e3s",
            "1s private",
            "18446744073709551615d",
        ] {
            assert_eq!(reset_millis(value), None, "{value}");
        }
    }

    #[test]
    fn duplicate_secret_malformed_and_over_bound_headers_are_not_retained() {
        let mut headers = HeaderMap::new();
        headers.append(
            "x-request-id",
            HeaderValue::from_static("req_0123456789abcdef0123456789abcdef"),
        );
        headers.append(
            "x-request-id",
            HeaderValue::from_static("req_fedcba9876543210fedcba9876543210"),
        );
        headers.insert("retry-after", HeaderValue::from_static("configured-key"));
        headers.insert(
            "x-ratelimit-limit-tokens",
            HeaderValue::from_static("12 secret"),
        );
        headers.insert(
            "x-ratelimit-remaining-tokens",
            HeaderValue::from_static("18446744073709551616"),
        );
        assert!(header_diagnostics(&headers, "configured-key", 1_024).is_empty());
        headers.clear();
        headers.insert(
            "x-request-id",
            HeaderValue::from_static("unrecognized-opaque-id"),
        );
        assert!(header_diagnostics(&headers, "configured-key", 1).is_empty());
        let values = header_diagnostics(&headers, "configured-key", 1_024);
        assert_eq!(values[0].0, "request_id_sha256");
        assert_eq!(values[0].1.len(), 64);
        assert!(!values[0].1.contains("unrecognized-opaque-id"));
    }

    #[test]
    fn complete_header_diagnostic_fits_the_accepted_summary_bound_or_names_it() {
        let base = "native_provider_rate_limited status=429".to_string();
        let fields = [("retry_after_seconds", "2".to_string())];
        let exact = base.len() + " retry_after_seconds=2".len();
        assert_eq!(
            finish(base.clone(), &fields, exact),
            format!("{base} retry_after_seconds=2")
        );
        assert_eq!(
            finish(base, &fields, exact - 1),
            "native_http_diagnostic_bound_native_summary_bytes"
        );
    }

    #[test]
    fn retry_after_preserves_absence_and_invalidity_and_honors_both_minima() {
        let now = SystemTime::UNIX_EPOCH;
        let mut headers = HeaderMap::new();
        assert_eq!(
            retry_after(&headers, "configured-key", 1024, now),
            RetryAfter::Absent
        );
        headers.insert("retry-after", HeaderValue::from_static("2"));
        assert_eq!(
            retry_after(&headers, "configured-key", 1024, now),
            RetryAfter::DelayMs(2000)
        );
        headers.insert("retry-after-ms", HeaderValue::from_static("2501"));
        assert_eq!(
            retry_after(&headers, "configured-key", 1024, now),
            RetryAfter::DelayMs(2501)
        );
        headers.insert("retry-after-ms", HeaderValue::from_static("1"));
        assert_eq!(
            retry_after(&headers, "configured-key", 1024, now),
            RetryAfter::DelayMs(2000)
        );
        headers.append("retry-after", HeaderValue::from_static("2"));
        assert_eq!(
            retry_after(&headers, "configured-key", 1024, now),
            RetryAfter::Invalid
        );
        for invalid in ["-1", "1.5", "NaN", "18446744073709552", "configured-key"] {
            headers.clear();
            headers.insert("retry-after", HeaderValue::from_str(invalid).unwrap());
            assert_eq!(
                retry_after(&headers, "configured-key", 1024, now),
                RetryAfter::Invalid,
                "{invalid}"
            );
        }
        headers.clear();
        headers.insert("retry-after", HeaderValue::from_static("123"));
        assert_eq!(
            retry_after(&headers, "configured-key", 2, now),
            RetryAfter::Invalid
        );
        headers.insert("retry-after", HeaderValue::from_static("0"));
        assert_eq!(
            retry_after(&headers, "configured-key", 1024, now),
            RetryAfter::DelayMs(0)
        );
        headers.insert("retry-after-ms", HeaderValue::from_static("bad"));
        assert_eq!(
            retry_after(&headers, "configured-key", 1024, now),
            RetryAfter::Invalid
        );
    }

    #[test]
    fn retry_after_http_dates_round_up_and_past_dates_mean_zero() {
        let target = SystemTime::UNIX_EPOCH + Duration::from_secs(784_111_777);
        for text in [
            "Sun, 06 Nov 1994 08:49:37 GMT",
            "Sunday, 06-Nov-94 08:49:37 GMT",
            "Sun Nov  6 08:49:37 1994",
        ] {
            assert_eq!(
                retry_after_millis(text, target - Duration::from_nanos(1)),
                Some(1)
            );
            assert_eq!(
                retry_after_millis(text, target - Duration::from_millis(1001)),
                Some(1001)
            );
            assert_eq!(retry_after_millis(text, target), Some(0));
            assert_eq!(
                retry_after_millis(text, target + Duration::from_secs(1)),
                Some(0)
            );
        }
        assert_eq!(
            retry_after_millis("Mon, 06 Nov 1994 08:49:37 GMT", target),
            None
        );
        assert_eq!(
            retry_after_millis("Sun, 31 Feb 1994 08:49:37 GMT", target),
            None
        );
    }

    #[test]
    fn structured_categories_do_not_replace_closed_rejection_proof() {
        use ProviderErrorCategory::*;
        for (status, code, kind) in [
            (429, "rate_limit_exceeded", RateLimited),
            (503, "server_is_overloaded", Overloaded),
            (400, "context_length_exceeded", ContextLimit),
            (400, "invalid_json_schema", InvalidRequest),
            (403, "content_policy_violation", PolicyRefused),
            (429, "insufficient_quota", QuotaExhausted),
            (401, "invalid_api_key", AuthenticationFailed),
        ] {
            assert_eq!(
                category(status, &serde_json::json!({"error":{"code":code}})),
                Some(kind)
            );
        }
        let rate = serde_json::json!({"error":{"code":"rate_limit_exceeded", "type":"rate_limit_error", "param":null, "message":"safe fixture"}});
        assert_eq!(category(429, &rate), Some(RateLimited));
        let mut conflict = rate.clone();
        conflict["error"]["type"] = "insufficient_quota".into();
        assert_eq!(category(429, &conflict), None);
        let mut extra = rate.clone();
        extra["output"] = serde_json::json!([]);
        assert_eq!(category(429, &extra), None);
        let mut extra = rate.clone();
        extra["error"]["unknown"] = true.into();
        assert_eq!(category(429, &extra), None);
        let mut malformed = rate.clone();
        malformed["error"]["message"] = 17.into();
        assert_eq!(category(429, &malformed), None);
        assert_eq!(category(500, &rate), None);
        assert_eq!(
            category(500, &serde_json::json!({"error":{"code":"server_error"}})),
            None
        );
        assert_eq!(
            category(
                429,
                &serde_json::json!({"error":{"type":"rate_limit_error"}})
            ),
            None
        );
        assert_eq!(
            category(
                429,
                &serde_json::json!({"error":{"code":"insufficient_quota", "type":"rate_limit_error"}})
            ),
            Some(QuotaExhausted)
        );
    }

    #[test]
    fn selected_throttling_envelopes_and_refusal_reasons_are_closed() {
        use serde_json::json;
        use ProviderErrorCategory::RateLimited;
        for value in [
            json!({"error":{"type":"rate_limit_error","code":"slow_down"}}),
            json!({"error":{"message":"synthetic quota detail","type":"tokens","param":null,"code":"rate_limit_exceeded"}}),
        ] {
            assert_eq!(classify(429, &value), Ok(RateLimited));
            for status in [200, 400, 401, 403, 500, 503] {
                assert_eq!(classify(status, &value), Err("status_or_type_mismatch"));
            }
        }
        for (value, reason) in [
            (json!([]), "invalid_root"),
            (json!({}), "missing_error"),
            (json!({"error":null}), "invalid_error"),
            (json!({"error":{"code":"slow_down","type":"rate_limit_error"},"output":[]}), "unexpected_root_keys"),
            (json!({"error":{"code":"slow_down","type":"rate_limit_error","PRIVATE_KEY":true}}), "unexpected_error_keys"),
            (json!({"error":{"code":"slow_down","type":17}}), "invalid_field_type"),
            (json!({"error":{"code":17,"type":"rate_limit_error"}}), "invalid_field_type"),
            (json!({"error":{"code":"rate_limit_exceeded","type":"requests"}}), "unsupported_type"),
            (json!({"error":{"code":"PRIVATE_CODE","type":"rate_limit_error"}}), "unsupported_code"),
            (json!({"error":{}}), "missing_discriminator"),
            (json!({"error":{"code":"slow_down"}}), "status_or_type_mismatch"),
            (json!({"error":{"code":"slow_down","type":"tokens"}}), "status_or_type_mismatch"),
            (json!({"error":{"code":"slow_down","type":"insufficient_quota"}}), "status_or_type_mismatch"),
            (json!({"error":{"code":"rate_limit_exceeded","type":"tokens"}}), "status_or_type_mismatch"),
            (json!({"error":{"code":"rate_limit_exceeded","type":"tokens","message":"safe","param":"input"}}), "status_or_type_mismatch"),
        ] {
            assert_eq!(classify(429, &value), Err(reason));
            assert_eq!(category(429, &value), None);
        }
    }

    #[test]
    fn only_selected_transient_rejection_permits_same_input_retry() {
        use ProviderErrorCategory::*;
        for (status, code, category, kind) in [
            (
                429,
                "rate_limit_exceeded",
                RateLimited,
                RejectionKind::RateLimited,
            ),
            (
                400,
                "invalid_json_schema",
                InvalidRequest,
                RejectionKind::InvalidRequest,
            ),
            (
                413,
                "context_length_exceeded",
                ContextLimit,
                RejectionKind::ContextLimit,
            ),
            (
                403,
                "content_policy_violation",
                PolicyRefused,
                RejectionKind::PolicyRefused,
            ),
            (
                429,
                "insufficient_quota",
                QuotaExhausted,
                RejectionKind::QuotaExhausted,
            ),
            (
                401,
                "invalid_api_key",
                AuthenticationFailed,
                RejectionKind::AuthenticationFailed,
            ),
        ] {
            let parsed =
                super::category(status, &serde_json::json!({"error":{"code":code}})).unwrap();
            assert_eq!(parsed, category);
            assert_eq!(rejection_kind(parsed), Some(kind));
            assert_eq!(
                kind.same_input_retryable(),
                kind == RejectionKind::RateLimited
            );
        }
        assert_eq!(rejection_kind(Overloaded), None);
        for status in 500..=599 {
            for code in [
                "server_is_overloaded",
                "invalid_json_schema",
                "context_length_exceeded",
                "content_policy_violation",
                "insufficient_quota",
                "invalid_api_key",
                "rate_limit_exceeded",
            ] {
                assert_eq!(
                    super::category(status, &serde_json::json!({"error":{"code":code}}))
                        .and_then(rejection_kind),
                    None
                );
            }
        }
    }

    #[test]
    fn selected_nontransient_proof_rejects_conflicting_status_type_and_envelope() {
        for (status, code) in [
            (400, "invalid_json_schema"),
            (413, "context_length_exceeded"),
            (403, "content_policy_violation"),
            (429, "insufficient_quota"),
            (401, "invalid_api_key"),
        ] {
            let base = serde_json::json!({"error":{"code":code}});
            assert!(category(status, &base).and_then(rejection_kind).is_some());
            let mut conflict = base.clone();
            conflict["error"]["type"] = "server_error".into();
            let mut extra = base.clone();
            extra["output"] = serde_json::json!([]);
            let mut future = base.clone();
            future["error"]["future_field"] = true.into();
            let mut malformed = base.clone();
            malformed["error"]["message"] = 17.into();
            for value in [conflict, extra, future, malformed] {
                assert_eq!(category(status, &value).and_then(rejection_kind), None);
            }
            assert_eq!(category(200, &base).and_then(rejection_kind), None);
        }
    }

    #[test]
    fn only_exact_quota_grammar_projects_numbers_and_decoded_credentials_are_redacted() {
        let message = "Rate limit reached for PRIVATE_MODEL in organization PRIVATE_ORG on tokens per min (TPM): Limit 30000.000, Used 29374.000, Requested 684. Please try again in 1.5s. Visit https://platform.openai.com/account/rate-limits to learn more.";
        let fields = message_diagnostics(Some(&Value::String(message.into())), "configured-key");
        assert!(fields.contains(&("quota_dimension", "tokens_per_minute".into())));
        assert!(fields.contains(&("quota_limit", "30000.000".into())));
        assert!(fields.contains(&("quota_used", "29374.000".into())));
        assert!(fields.contains(&("quota_requested", "684".into())));
        assert!(fields.contains(&("quota_wait_ms", "1500".into())));
        for (_, value) in &fields {
            assert!(!value.contains("PRIVATE_MODEL") && !value.contains("PRIVATE_ORG"));
        }
        assert!(quota_message(&format!("{message} reflected prompt")).is_none());
        let decoded: Value = serde_json::from_str(r#""configured-\u006bey""#).unwrap();
        assert_eq!(
            message_diagnostics(Some(&decoded), "configured-key"),
            vec![
                ("provider_message_state", "redacted".into()),
                ("provider_message_bytes", "14".into())
            ]
        );
        let unknown = message_diagnostics(
            Some(&Value::String("DO_NOT_COPY_PROMPT".into())),
            "configured-key",
        );
        assert_eq!(
            unknown[0],
            ("provider_message_state", "fingerprinted".into())
        );
        assert_eq!(unknown[1], ("provider_message_bytes", "18".into()));
        assert_eq!(unknown[2].0, "provider_message_sha256");
        assert_eq!(unknown[2].1.len(), 64);
        assert!(!unknown
            .iter()
            .any(|(_, value)| value.contains("DO_NOT_COPY_PROMPT")));
    }

    #[test]
    fn typed_headers_use_source_bound_without_expanding_scheduling_admission() {
        let mut policy = crate::limits::RuntimeLimits::seed()
            .unwrap()
            .execution
            .native;
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", HeaderValue::from_static("12"));
        headers.insert("retry-after-ms", HeaderValue::from_static("15000"));
        headers.insert(
            "x-ratelimit-limit-tokens",
            HeaderValue::from_static("30000"),
        );
        headers.insert(
            "x-ratelimit-reset-tokens",
            HeaderValue::from_static("1m2.5s"),
        );
        policy.summary_bytes = 1;
        let small = header_facts(
            429,
            &headers,
            "configured-key",
            &policy,
            SystemTime::UNIX_EPOCH,
        );
        assert_eq!(small.retry_after, RetryHeaderDiagnostic::Seconds(12));
        assert_eq!(small.retry_after_ms, NumericDiagnostic::Value(15000));
        assert_eq!(small.tokens_limit, NumericDiagnostic::Value(30000));
        assert_eq!(small.tokens_reset_ms, NumericDiagnostic::Value(62500));
        assert_eq!(
            small.scheduler_retry_after,
            SchedulerRetryDiagnostic::Value(RetryAfter::Invalid)
        );
        policy.summary_bytes = 8192;
        let mut large = header_facts(
            429,
            &headers,
            "configured-key",
            &policy,
            SystemTime::UNIX_EPOCH,
        );
        assert_eq!(
            large.scheduler_retry_after,
            SchedulerRetryDiagnostic::Value(RetryAfter::DelayMs(15000))
        );
        large.scheduler_retry_after = small.scheduler_retry_after;
        assert_eq!(
            large, small,
            "only the intentionally separate scheduling observation depends on summary admission"
        );
        policy.error_body_bytes = 1;
        let bound = header_facts(
            429,
            &headers,
            "configured-key",
            &policy,
            SystemTime::UNIX_EPOCH,
        );
        assert_eq!(bound.retry_after, RetryHeaderDiagnostic::SourceBound);
        assert_eq!(bound.tokens_limit, NumericDiagnostic::SourceBound);
        assert_eq!(bound.requests_limit, NumericDiagnostic::Absent);
    }

    #[test]
    fn typed_header_states_keep_invalid_duplicate_redacted_and_absent_distinct() {
        let mut policy = crate::limits::RuntimeLimits::seed()
            .unwrap()
            .execution
            .native;
        policy.error_body_bytes = 32;
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", HeaderValue::from_static("nonsense"));
        headers.insert("retry-after-ms", HeaderValue::from_static("configured-key"));
        headers.append("x-ratelimit-limit-tokens", HeaderValue::from_static("5"));
        headers.append("x-ratelimit-limit-tokens", HeaderValue::from_static("5"));
        headers.insert(
            "x-ratelimit-remaining-tokens",
            HeaderValue::from_static("18446744073709551616"),
        );
        headers.insert(
            "x-ratelimit-limit-requests",
            HeaderValue::from_str(&"7".repeat(33)).unwrap(),
        );
        let facts = header_facts(
            503,
            &headers,
            "configured-key",
            &policy,
            SystemTime::UNIX_EPOCH,
        );
        assert_eq!(facts.status, HttpStatusDiagnostic::Value(503));
        assert_eq!(facts.retry_after, RetryHeaderDiagnostic::Invalid);
        assert_eq!(facts.retry_after_ms, NumericDiagnostic::Redacted);
        assert_eq!(facts.tokens_limit, NumericDiagnostic::Duplicate);
        assert_eq!(facts.tokens_remaining, NumericDiagnostic::Invalid);
        assert_eq!(facts.requests_limit, NumericDiagnostic::SourceBound);
        assert_eq!(facts.requests_remaining, NumericDiagnostic::Absent);
        assert_eq!(facts.category, CategoryDiagnostic::Absent);
        assert_eq!(facts.body, DiagnosticBodyState::Pending);
        assert!(!serde_json::to_string(&facts)
            .unwrap()
            .contains("configured-key"));
    }

    #[test]
    //= spec/impl/v2/implementation.md#platform-owned-model-retries
    //= type=test
    //# Retained provider diagnostics MUST exclude configured credentials and submitted prompt content.
    fn typed_normalized_numbers_are_screened_before_retention() {
        let policy = crate::limits::RuntimeLimits::seed()
            .unwrap()
            .execution
            .native;
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", HeaderValue::from_static("2"));
        headers.insert("x-ratelimit-reset-tokens", HeaderValue::from_static("2s"));
        headers.insert("retry-after-ms", HeaderValue::from_static("0002000"));
        let facts = header_facts(429, &headers, "2000", &policy, SystemTime::UNIX_EPOCH);
        assert_eq!(
            facts.scheduler_retry_after,
            SchedulerRetryDiagnostic::Value(RetryAfter::Invalid),
            "raw credential-containing ms source remains scheduler invalid"
        );
        assert_eq!(facts.retry_after_ms, NumericDiagnostic::Redacted);
        assert_eq!(
            facts.tokens_reset_ms,
            NumericDiagnostic::Redacted,
            "normalized 2s is also credential material"
        );
        headers.remove("retry-after-ms");
        let facts = header_facts(429, &headers, "2000", &policy, SystemTime::UNIX_EPOCH);
        assert_eq!(
            facts.scheduler_retry_after,
            SchedulerRetryDiagnostic::Redacted
        );
        assert_eq!(facts.retry_after, RetryHeaderDiagnostic::Seconds(2));
        assert_eq!(
            retry_after(
                &headers,
                "2000",
                policy.summary_bytes,
                SystemTime::UNIX_EPOCH
            ),
            RetryAfter::DelayMs(2000),
            "diagnostic redaction never rewrites actual scheduling semantics"
        );
        assert!(!serde_json::to_string(&facts).unwrap().contains("2000"));
        headers.insert(
            "x-ratelimit-limit-requests",
            HeaderValue::from_static("00042"),
        );
        let facts = header_facts(429, &headers, "42", &policy, SystemTime::UNIX_EPOCH);
        assert_eq!(facts.requests_limit, NumericDiagnostic::Redacted);
        assert_eq!(
            facts.status,
            HttpStatusDiagnostic::Redacted,
            "numeric status scalar also screened"
        );
    }

    #[test]
    fn quota_projection_retains_exact_bounded_fraction_and_rejects_prose_or_rounding() {
        let fraction = "7".repeat(1024);
        let message=format!("Rate limit reached for PRIVATE_MODEL in organization PRIVATE_ORG on tokens per min (TPM): Limit 30000.{fraction}, Used 29374.000, Requested 684. Please try again in 1.5s. Visit https://platform.openai.com/account/rate-limits to learn more.");
        let projected = quota_diagnostic(Some(&Value::String(message.clone())), "configured-key");
        assert_eq!(
            projected,
            QuotaDiagnostic::Value(QuotaValues {
                dimension: QuotaDimension::TokensPerMinute,
                limit: format!("30000.{fraction}"),
                used: "29374.000".into(),
                requested: "684".into(),
                wait_ms: 1500
            })
        );
        let bytes = serde_json::to_vec(&projected).unwrap();
        assert_eq!(
            serde_json::from_slice::<QuotaDiagnostic>(&bytes).unwrap(),
            projected
        );
        for omitted in [
            "PRIVATE_MODEL",
            "PRIVATE_ORG",
            "Rate limit reached",
            "https://",
        ] {
            assert!(!std::str::from_utf8(&bytes).unwrap().contains(omitted));
        }
        assert_eq!(
            quota_diagnostic(
                Some(&Value::String(format!("{message} PRIVATE_PROMPT"))),
                "configured-key"
            ),
            QuotaDiagnostic::Unrecognized
        );
        assert_eq!(
            quota_diagnostic(Some(&Value::String(message)), "1500"),
            QuotaDiagnostic::Redacted,
            "normalized wait numeric scalar is screened"
        );
        assert_eq!(
            quota_diagnostic(
                Some(&Value::String("PRIVATE_PROMPT configured-key".into())),
                "configured-key"
            ),
            QuotaDiagnostic::Redacted
        );
        assert_eq!(
            quota_diagnostic(Some(&Value::Number(3.into())), "configured-key"),
            QuotaDiagnostic::InvalidType
        );
    }
    #[test]
    fn date_retry_redaction_never_recomputes_the_captured_scheduling_delay() {
        let policy = crate::limits::RuntimeLimits::seed()
            .unwrap()
            .execution
            .native;
        let target = SystemTime::UNIX_EPOCH + Duration::from_secs(784_111_777);
        let observed = target - Duration::from_secs(2);
        let mut headers = HeaderMap::new();
        headers.insert(
            "retry-after",
            HeaderValue::from_static("Sun, 06 Nov 1994 08:49:37 GMT"),
        );
        let captured = capture_headers(429, &headers, "2000", &policy, observed);
        assert_eq!(
            captured.facts.scheduler_retry_after,
            SchedulerRetryDiagnostic::Redacted
        );
        assert_eq!(captured.retry_after, RetryAfter::DelayMs(2000));
        // A later wall-clock value would change the delay if privacy triggered
        // a reparse. inspect consumes captured.retry_after directly instead.
        assert_eq!(
            retry_after(
                &headers,
                "2000",
                policy.summary_bytes,
                target + Duration::from_secs(1)
            ),
            RetryAfter::DelayMs(0)
        );
        assert_eq!(captured.retry_after, RetryAfter::DelayMs(2000));
        assert!(!serde_json::to_string(&captured.facts)
            .unwrap()
            .contains("2000"));
    }
}
