//! HTTP refusal diagnostics are finite labels, never provider text. Reading an
//! error body cannot turn a refused request into a retry or completed outcome.
use super::decode;
use crate::limits::NativeLimits;
use serde_json::Value;
use std::time::Duration;
use tokio::time::Instant;

pub(super) async fn diagnostic(
    response: &mut reqwest::Response,
    key: &str,
    deadline: Instant,
    policy: &NativeLimits,
) -> String {
    let status = response.status().as_u16();
    let class = match status {
        401 | 403 => "native_provider_authentication_failed",
        429 => "native_provider_rate_limited",
        500..=599 => "native_provider_unavailable",
        300..=399 => "native_provider_redirect_refused",
        _ => "native_provider_request_refused",
    };
    let mut result = format!("{class} status={status}");
    let read_deadline = Instant::now()
        .checked_add(Duration::from_millis(policy.error_read_timeout_ms))
        .unwrap_or(deadline)
        .min(deadline);
    let Ok(Some(body)) =
        tokio::time::timeout_at(read_deadline, body(response, policy.error_body_bytes)).await
    else {
        return result;
    };
    // Reuse bounded JSON admission, including duplicate-key refusal. Diagnostics
    // may be absent for truncated, oversized, malformed or slow error bodies.
    let Ok(value) = decode::parse(
        &body,
        policy.error_body_bytes,
        read_deadline,
        "native_error_not_json",
        policy,
        "native_error_body_bytes",
    ) else {
        return result;
    };
    let Some(error) = value.get("error").and_then(Value::as_object) else {
        return result;
    };
    for (field, labels) in [("type", TYPES), ("code", CODES), ("param", PARAMS)] {
        if let Some(candidate) = error.get(field).and_then(Value::as_str) {
            // Select a source literal, not an arbitrary provider string matching
            // a character pattern. Never retain messages, IDs, headers or URLs.
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
    result
}

async fn body(response: &mut reqwest::Response, limit: usize) -> Option<Vec<u8>> {
    if response.content_length().is_some_and(|n| n > limit as u64) {
        return None;
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.ok()? {
        if body
            .len()
            .checked_add(chunk.len())
            .is_none_or(|bytes| bytes > limit)
        {
            return None;
        }
        body.extend_from_slice(&chunk);
    }
    Some(body)
}

const TYPES: &[&str] = &[
    "invalid_request_error",
    "authentication_error",
    "permission_error",
    "not_found_error",
    "rate_limit_error",
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
