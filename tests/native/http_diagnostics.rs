//! Join under tests/native.rs to exercise the actual request/error path.
//! A loopback fixture is the only server; no provider or credential access.
use super::*;
use borg_runtime::native::{ProviderRejection, RejectionKind, RetryAfter};

fn selected_rejection_cases() -> [(u16, &'static str, &'static str, RejectionKind); 6] {
    [
        (
            429,
            "rate_limit_exceeded",
            "rate_limited",
            RejectionKind::RateLimited,
        ),
        (
            400,
            "invalid_json_schema",
            "invalid_request",
            RejectionKind::InvalidRequest,
        ),
        (
            400,
            "context_length_exceeded",
            "context_limit",
            RejectionKind::ContextLimit,
        ),
        (
            403,
            "content_policy_violation",
            "policy_refused",
            RejectionKind::PolicyRefused,
        ),
        (
            429,
            "insufficient_quota",
            "quota_exhausted",
            RejectionKind::QuotaExhausted,
        ),
        (
            401,
            "invalid_api_key",
            "authentication_failed",
            RejectionKind::AuthenticationFailed,
        ),
    ]
}

#[tokio::test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# Retained provider diagnostics MUST exclude configured credentials and submitted prompt content.
async fn native_http_refusal_retains_bounded_request_quota_and_retry_headers() {
    let body =
        json!({"error":{"message":"DO_NOT_COPY_PROVIDER_PROMPT", "code":"rate_limit_exceeded"}});
    let fixture = Fixture::start(429, serde_json::to_vec(&body).unwrap(), Duration::ZERO,
        "x-request-id: req_0123456789abcdef0123456789abcdef\r\nretry-after: 2\r\nretry-after-ms: 2500\r\nx-ratelimit-limit-requests: 60\r\nx-ratelimit-remaining-requests: 0\r\nx-ratelimit-limit-tokens: 30000\r\nx-ratelimit-remaining-tokens: 12\r\nx-ratelimit-reset-requests: 1m2.5s\r\nx-ratelimit-reset-tokens: 20ms\r\n").await;
    let result = observed(&task(), &config(fixture.endpoint.clone())).await;
    fixture.finish().await;
    assert_eq!(result.result.err().as_deref(), Some(concat!(
        "native_provider_rate_limited status=429 code=rate_limit_exceeded",
        " request_id=req_0123456789abcdef0123456789abcdef retry_after_seconds=2 retry_after_ms=2500",
        " rate_limit_requests_limit=60 rate_limit_requests_remaining=0",
        " rate_limit_tokens_limit=30000 rate_limit_tokens_remaining=12",
        " rate_limit_requests_reset_ms=62500 rate_limit_tokens_reset_ms=20",
        " error_body=complete provider_message_state=fingerprinted provider_message_bytes=27 provider_message_sha256=dab4b9e6042d5417bf233db01cf7a81d07aac95e18fb201bd5db6224c190d82b category=rate_limited"
    )));
    assert_eq!(
        result.request_exposure,
        RequestExposure::HttpResponseReceived
    );
    assert_eq!(
        result.provider_outcome,
        ProviderOutcome::Rejected(ProviderRejection {
            kind: RejectionKind::RateLimited,
            retry_after: RetryAfter::DelayMs(2500)
        })
    );
    assert_eq!(result.usage, UsageObservation::Unknown);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# Retained provider diagnostics MUST exclude configured credentials and submitted prompt content.
async fn native_http_diagnostic_headers_do_not_echo_unknown_text_or_configured_key() {
    let credential = std::env::var("HOME").unwrap(); // Existing synthetic fixture key, never a provider secret.
    let headers = format!("x-request-id: arbitrary-private-request-identifier\r\nretry-after: {credential}\r\nx-ratelimit-limit-requests: 7 private\r\nx-ratelimit-remaining-requests: 8\r\nx-ratelimit-remaining-requests: 9\r\nx-extra-private: DO_NOT_COPY_HEADER\r\n");
    let fixture = Fixture::start(
        429,
        b"DO_NOT_COPY_PROVIDER_BODY".to_vec(),
        Duration::ZERO,
        &headers,
    )
    .await;
    let result = observed(&task(), &config(fixture.endpoint.clone())).await;
    fixture.finish().await;
    let diagnostic = result
        .result
        .err()
        .expect("expected confirmed HTTP rejection");
    assert!(diagnostic.starts_with("native_provider_rate_limited status=429 request_id_sha256="));
    let fingerprint = diagnostic
        .split_once("request_id_sha256=")
        .unwrap()
        .1
        .split_whitespace()
        .next()
        .unwrap();
    assert_eq!(fingerprint.len(), 64);
    assert!(fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit()));
    for excluded in [
        credential.as_str(),
        "arbitrary-private-request-identifier",
        "DO_NOT_COPY_HEADER",
        "DO_NOT_COPY_PROVIDER_BODY",
        "rate_limit_requests",
        "retry_after_seconds",
        "retry_after_ms",
    ] {
        assert!(
            !diagnostic.contains(excluded),
            "unexpected retained field/value"
        );
    }
}

#[tokio::test]
async fn native_http_headers_survive_a_stalled_error_body_without_extending_deadline() {
    let fixture = Fixture::start_response(
        429,
        vec![b'x'; 64],
        Duration::ZERO,
        "x-request-id: req_0123456789abcdef0123456789abcdef\r\nretry-after: 3\r\n",
        0,
        Duration::from_secs(30),
        true,
    )
    .await;
    let before = tokio::time::Instant::now();
    let result = observed(&task(), &config(fixture.endpoint.clone())).await;
    fixture.finish().await;
    assert_eq!(result.result.err().as_deref(), Some("native_provider_rate_limited status=429 request_id=req_0123456789abcdef0123456789abcdef retry_after_seconds=3 error_body=timed_out_native_error_read_timeout_ms"));
    assert_eq!(result.provider_outcome, ProviderOutcome::Unknown);
    assert!(
        before.elapsed() < Duration::from_secs(5),
        "existing error-body timeout and task deadline remain in force"
    );
}

#[tokio::test]
async fn native_http_rejection_requires_the_complete_selected_closed_envelope() {
    for (status, body) in [
        (
            429,
            br#"{"error":{"code":"rate_limit_exceeded","code":"rate_limit_exceeded"}}"#.as_slice(),
        ),
        (
            429,
            br#"{"error":{"code":"rate_limit_exceeded"},"output":[]}"#.as_slice(),
        ),
        (
            429,
            br#"{"error":{"code":"rate_limit_exceeded","unknown":true}}"#.as_slice(),
        ),
        (
            429,
            br#"{"error":{"code":"rate_limit_exceeded","type":"insufficient_quota"}}"#.as_slice(),
        ),
        (
            429,
            br#"{"error":{"code":"rate_limit_exceeded","message":17}}"#.as_slice(),
        ),
        (429, br#"{"error":{"type":"rate_limit_error"}}"#.as_slice()),
        (
            500,
            br#"{"error":{"code":"rate_limit_exceeded"}}"#.as_slice(),
        ),
        (
            503,
            br#"{"error":{"code":"server_is_overloaded"}}"#.as_slice(),
        ),
        (500, br#"{"error":{"code":"server_error"}}"#.as_slice()),
    ] {
        let fixture =
            Fixture::start(status, body.to_vec(), Duration::ZERO, "retry-after: 0\r\n").await;
        let result = observed(&task(), &config(fixture.endpoint.clone())).await;
        fixture.finish().await;
        assert!(result.result.is_err());
        assert_eq!(
            result.provider_outcome,
            ProviderOutcome::Unknown,
            "status={status}, body fixture remains unknown"
        );
        assert_eq!(
            result.request_exposure,
            RequestExposure::HttpResponseReceived
        );
        assert_eq!(result.usage, UsageObservation::Unknown);
    }
}

#[tokio::test]
async fn native_http_malformed_selected_rejections_remain_unknown_for_every_class() {
    for (status, code, _, _) in selected_rejection_cases() {
        for body in [
            serde_json::to_vec(&json!({"error":{"code":code,"message":17}})).unwrap(),
            format!(r#"{{"error":{{"code":"{code}","code":"{code}"}}}}"#).into_bytes(),
        ] {
            let fixture = Fixture::start(status, body, Duration::ZERO, "retry-after: 0\r\n").await;
            let result = observed(&task(), &config(fixture.endpoint.clone())).await;
            fixture.finish().await;
            assert!(result.result.is_err());
            assert_eq!(
                result.provider_outcome,
                ProviderOutcome::Unknown,
                "malformed {code}"
            );
            assert_eq!(
                result.request_exposure,
                RequestExposure::HttpResponseReceived
            );
            assert_eq!(result.usage, UsageObservation::Unknown);
        }
    }
}

#[tokio::test]
async fn native_http_refusal_proof_is_independent_of_retry_after_validity() {
    for (header, expected) in [
        ("", RetryAfter::Absent),
        ("retry-after: 2\r\n", RetryAfter::DelayMs(2000)),
        (
            "retry-after: Sun, 06 Nov 1994 08:49:37 GMT\r\n",
            RetryAfter::DelayMs(0),
        ),
        ("retry-after: invalid\r\n", RetryAfter::Invalid),
        ("retry-after: 1\r\nretry-after: 1\r\n", RetryAfter::Invalid),
        (
            "retry-after-ms: 18446744073709551616\r\n",
            RetryAfter::Invalid,
        ),
    ] {
        let fixture = Fixture::start(
            429,
            br#"{"error":{"code":"rate_limit_exceeded"}}"#.to_vec(),
            Duration::ZERO,
            header,
        )
        .await;
        let result = observed(&task(), &config(fixture.endpoint.clone())).await;
        fixture.finish().await;
        assert_eq!(
            result.provider_outcome,
            ProviderOutcome::Rejected(ProviderRejection {
                kind: RejectionKind::RateLimited,
                retry_after: expected,
            })
        );
        assert_eq!(
            result.usage,
            UsageObservation::Unknown,
            "known refusal does not refund or invent usage"
        );
        assert!(result
            .result
            .err()
            .expect("expected confirmed HTTP rejection")
            .ends_with("category=rate_limited"));
    }
}

#[tokio::test]
async fn native_http_selected_refusals_at_body_bound_are_known_but_over_bound_are_not() {
    let input = task();
    let limit = input.execution_limits().unwrap().native.error_body_bytes;
    for (status, code, _, kind) in selected_rejection_cases() {
        let mut bytes = serde_json::to_vec(&json!({"error":{"code":code}})).unwrap();
        assert!(bytes.len() < limit);
        bytes.resize(limit, b' ');
        let fixture = Fixture::start(status, bytes.clone(), Duration::ZERO, "").await;
        let result = observed(&task(), &config(fixture.endpoint.clone())).await;
        fixture.finish().await;
        assert_eq!(
            result.provider_outcome,
            ProviderOutcome::Rejected(ProviderRejection {
                kind,
                retry_after: RetryAfter::Absent,
            }),
            "complete {code} at exact bound"
        );
        assert_eq!(result.usage, UsageObservation::Unknown);
        bytes.push(b' ');
        let fixture = Fixture::start(status, bytes, Duration::ZERO, "").await;
        let result = observed(&task(), &config(fixture.endpoint.clone())).await;
        fixture.finish().await;
        assert_eq!(
            result.provider_outcome,
            ProviderOutcome::Unknown,
            "oversized {code}"
        );
        assert!(result
            .result
            .err()
            .expect("expected confirmed HTTP rejection")
            .ends_with("error_body=advertised_bound_native_error_body_bytes"));
        assert_eq!(
            result.request_exposure,
            RequestExposure::HttpResponseReceived
        );
        assert_eq!(result.usage, UsageObservation::Unknown);
    }
}

#[tokio::test]
async fn native_http_selected_refusals_with_lost_body_suffix_stay_unknown() {
    for (status, code, _, _) in selected_rejection_cases() {
        let bytes = serde_json::to_vec(&json!({"error":{"code":code}})).unwrap();
        let fixture = Fixture::start_response(
            status,
            bytes,
            Duration::ZERO,
            "retry-after: 0\r\n",
            1,
            Duration::ZERO,
            true,
        )
        .await;
        let result = observed(&task(), &config(fixture.endpoint.clone())).await;
        fixture.finish().await;
        assert_eq!(
            result.provider_outcome,
            ProviderOutcome::Unknown,
            "incomplete {code}"
        );
        assert!(result
            .result
            .err()
            .expect("expected confirmed HTTP rejection")
            .ends_with("retry_after_seconds=0 error_body=read_failed"));
        assert_eq!(
            result.request_exposure,
            RequestExposure::HttpResponseReceived
        );
        assert_eq!(result.usage, UsageObservation::Unknown);
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# Retained provider diagnostics MUST exclude configured credentials and submitted prompt content.
async fn native_http_escaped_configured_credential_is_redacted_after_json_decode() {
    let credential = std::env::var("HOME").unwrap();
    let escaped: String = credential
        .encode_utf16()
        .map(|unit| format!("\\u{unit:04x}"))
        .collect();
    let bytes = format!(r#"{{"error":{{"message":"{escaped}","code":"rate_limit_exceeded"}}}}"#)
        .into_bytes();
    let fixture = Fixture::start(429, bytes, Duration::ZERO, "").await;
    let result = observed(&task(), &config(fixture.endpoint.clone())).await;
    fixture.finish().await;
    let diagnostic = result
        .result
        .err()
        .expect("expected confirmed HTTP rejection");
    assert_eq!(diagnostic, format!("native_provider_rate_limited status=429 code=rate_limit_exceeded error_body=complete provider_message_state=redacted provider_message_bytes={} category=rate_limited", credential.len()));
    assert!(!diagnostic.contains(&credential));
    assert!(!diagnostic.contains("provider_message_sha256"));
}

#[tokio::test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A platform-owned model request rejected as invalid MUST NOT be retried.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A context-window rejection MUST NOT be retried with unchanged input.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A provider policy refusal MUST be reported as a policy block.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A provider quota exhaustion MUST be distinguished from transient throttling.
async fn native_http_structured_categories_remain_distinct_from_retry_authorization() {
    for (status, code, category, kind) in selected_rejection_cases() {
        let body =
            serde_json::to_vec(&json!({"error":{"code":code,"message":"PRIVATE_PROVIDER_DETAIL"}}))
                .unwrap();
        let fixture = Fixture::start(status, body, Duration::ZERO, "").await;
        let result = observed(&task(), &config(fixture.endpoint.clone())).await;
        fixture.finish().await;
        assert_eq!(
            result.provider_outcome,
            ProviderOutcome::Rejected(ProviderRejection {
                kind,
                retry_after: RetryAfter::Absent,
            })
        );
        assert_eq!(
            kind.same_input_retryable(),
            kind == RejectionKind::RateLimited
        );
        assert_eq!(
            result.request_exposure,
            RequestExposure::HttpResponseReceived
        );
        assert_eq!(result.usage, UsageObservation::Unknown);
        let diagnostic = result
            .result
            .err()
            .expect("expected confirmed HTTP rejection");
        assert!(diagnostic.ends_with(&format!("category={category}")));
        assert!(!diagnostic.contains("PRIVATE_PROVIDER_DETAIL"));
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A provider quota exhaustion MUST be distinguished from transient throttling.
async fn native_http_quota_codes_with_compatible_types_are_known_nontransient_refusals() {
    for code in [
        "insufficient_quota",
        "credit_balance_exhausted",
        "organization_spend_limit_exceeded",
        "project_spend_limit_exceeded",
        "organization_usage_limit_exceeded",
    ] {
        let body =
            serde_json::to_vec(&json!({"error":{"code":code,"type":"rate_limit_error"}})).unwrap();
        let fixture = Fixture::start(429, body, Duration::ZERO, "retry-after: 0\r\n").await;
        let result = observed(&task(), &config(fixture.endpoint.clone())).await;
        fixture.finish().await;
        assert_eq!(
            result.provider_outcome,
            ProviderOutcome::Rejected(ProviderRejection {
                kind: RejectionKind::QuotaExhausted,
                retry_after: RetryAfter::DelayMs(0),
            })
        );
        assert!(!RejectionKind::QuotaExhausted.same_input_retryable());
        assert_eq!(result.usage, UsageObservation::Unknown);
        assert!(result
            .result
            .err()
            .expect("expected confirmed HTTP rejection")
            .ends_with("category=quota_exhausted"));
    }
}

#[tokio::test]
async fn native_http_selected_type_only_refusals_are_known_without_retry_authorization() {
    for (status, error_type, kind) in [
        (
            401,
            "authentication_error",
            RejectionKind::AuthenticationFailed,
        ),
        (403, "permission_error", RejectionKind::PolicyRefused),
        (400, "invalid_request_error", RejectionKind::InvalidRequest),
        (429, "insufficient_quota", RejectionKind::QuotaExhausted),
    ] {
        for error in [
            json!({"type":error_type}),
            json!({"code":null,"type":error_type}),
        ] {
            let body = serde_json::to_vec(&json!({"error":error})).unwrap();
            let fixture =
                Fixture::start(status, body, Duration::ZERO, "retry-after: invalid\r\n").await;
            let result = observed(&task(), &config(fixture.endpoint.clone())).await;
            fixture.finish().await;
            assert_eq!(
                result.provider_outcome,
                ProviderOutcome::Rejected(ProviderRejection {
                    kind,
                    retry_after: RetryAfter::Invalid,
                })
            );
            assert!(!kind.same_input_retryable());
            assert_eq!(
                result.request_exposure,
                RequestExposure::HttpResponseReceived
            );
            assert_eq!(result.usage, UsageObservation::Unknown);
        }
    }
}

#[tokio::test]
async fn native_http_overload_category_does_not_prove_rejection() {
    for status in [429, 500, 503] {
        let fixture = Fixture::start(
            status,
            br#"{"error":{"code":"server_is_overloaded"}}"#.to_vec(),
            Duration::ZERO,
            "retry-after: 0\r\n",
        )
        .await;
        let result = observed(&task(), &config(fixture.endpoint.clone())).await;
        fixture.finish().await;
        assert_eq!(result.provider_outcome, ProviderOutcome::Unknown);
        assert_eq!(result.usage, UsageObservation::Unknown);
        assert!(result
            .result
            .err()
            .expect("expected confirmed HTTP rejection")
            .ends_with("category=overloaded"));
    }
}

fn input_with_summary_bytes(bytes: usize) -> TaskInput {
    let mut input = task();
    let mut native = input.execution_limits().unwrap().native.clone();
    native.summary_bytes = bytes;
    input.definition.limit_overrides =
        Some(Box::new(borg_runtime::limits::ExecutionLimitOverrides {
            native: Some(native),
            coding: None,
            collaboration: None,
            memory_notice: None,
        }));
    bind(input)
}

//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A failed platform-owned provider request MUST retain bounded diagnostics sufficient to identify the observed failure class and available provider retry or quota information.
#[tokio::test]
async fn typed_http_facts_survive_summary_loss_without_changing_rejection_or_retry_admission() {
    use borg_runtime::native::{
        CategoryDiagnostic, DiagnosticBodyState, HttpStatusDiagnostic, NumericDiagnostic,
        ProviderErrorCategory, ProviderFailureClass, QuotaDiagnostic, QuotaDimension, QuotaValues,
        RetryHeaderDiagnostic, SchedulerRetryDiagnostic,
    };
    let fraction = "7".repeat(1500);
    let message=format!("Rate limit reached for PRIVATE_MODEL in organization PRIVATE_ORG on tokens per min (TPM): Limit 30000.{fraction}, Used 29374.000, Requested 684. Please try again in 1.5s. Visit https://platform.openai.com/account/rate-limits to learn more.");
    let body =
        serde_json::to_vec(&json!({"error":{"code":"rate_limit_exceeded","message":message}}))
            .unwrap();
    let mut original = None;
    for summary in [1usize, 64, 8192] {
        let fixture=Fixture::start(429,body.clone(),Duration::ZERO,"retry-after: 12\r\nretry-after-ms: 15000\r\nx-ratelimit-limit-tokens: 30000\r\nx-ratelimit-remaining-tokens: 12\r\nx-ratelimit-reset-requests: 1m2.5s\r\n").await;
        let input = input_with_summary_bytes(summary);
        let result = observed(&input, &config(fixture.endpoint.clone())).await;
        fixture.finish().await;
        let schedule = if summary == 1 {
            RetryAfter::Invalid
        } else {
            RetryAfter::DelayMs(15000)
        };
        assert_eq!(
            result.provider_outcome,
            ProviderOutcome::Rejected(ProviderRejection {
                kind: RejectionKind::RateLimited,
                retry_after: schedule
            })
        );
        let diagnostic = result
            .diagnostic
            .expect("typed diagnostic independent of display");
        assert_eq!(diagnostic.class, ProviderFailureClass::HttpRefused);
        diagnostic
            .validate(&input.execution_limits().unwrap().native)
            .unwrap();
        assert!(
            serde_json::to_vec(&diagnostic).unwrap().len()
                <= borg_runtime::native::ProviderFailureDiagnostic::maximum_encoded_bytes(
                    &input.execution_limits().unwrap().native
                )
                .unwrap()
        );
        let mut facts = diagnostic.http.unwrap();
        assert_eq!(facts.status, HttpStatusDiagnostic::Value(429));
        assert_eq!(facts.body, DiagnosticBodyState::Complete);
        assert_eq!(
            facts.category,
            CategoryDiagnostic::Value(ProviderErrorCategory::RateLimited)
        );
        assert_eq!(
            facts.scheduler_retry_after,
            SchedulerRetryDiagnostic::Value(schedule)
        );
        assert_eq!(facts.retry_after, RetryHeaderDiagnostic::Seconds(12));
        assert_eq!(facts.retry_after_ms, NumericDiagnostic::Value(15000));
        assert_eq!(facts.tokens_limit, NumericDiagnostic::Value(30000));
        assert_eq!(facts.tokens_remaining, NumericDiagnostic::Value(12));
        assert_eq!(facts.requests_reset_ms, NumericDiagnostic::Value(62500));
        assert_eq!(
            facts.quota,
            QuotaDiagnostic::Value(QuotaValues {
                dimension: QuotaDimension::TokensPerMinute,
                limit: format!("30000.{fraction}"),
                used: "29374.000".into(),
                requested: "684".into(),
                wait_ms: 1500
            })
        );
        facts.scheduler_retry_after = SchedulerRetryDiagnostic::Value(RetryAfter::Absent);
        if let Some(original) = &original {
            assert_eq!(&facts, original);
        } else {
            original = Some(facts);
        }
        let display = result.result.err().expect("expected native refusal");
        if summary <= 64 {
            assert_eq!(display, "native_http_diagnostic_bound_native_summary_bytes");
        }
    }
}

//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A failed platform-owned provider request MUST retain bounded diagnostics sufficient to identify the observed failure class and available provider retry or quota information.
#[tokio::test]
async fn typed_http_partial_body_keeps_headers_without_inventing_rejection() {
    use borg_runtime::native::{
        CategoryDiagnostic, DiagnosticBodyState, NumericDiagnostic, ProviderFailureClass,
        RetryHeaderDiagnostic,
    };
    let body = br#"{"error":{"code":"rate_limit_exceeded"}}"#.to_vec();
    for missing in [1usize, 0] {
        let fixture = Fixture::start_response(
            429,
            body.clone(),
            Duration::ZERO,
            "retry-after: 12\r\nx-ratelimit-limit-tokens: 30000\r\n",
            missing,
            Duration::ZERO,
            true,
        )
        .await;
        let result = observed(
            &input_with_summary_bytes(1),
            &config(fixture.endpoint.clone()),
        )
        .await;
        fixture.finish().await;
        let diagnostic = result.diagnostic.unwrap();
        let facts = diagnostic.http.unwrap();
        assert_eq!(facts.retry_after, RetryHeaderDiagnostic::Seconds(12));
        assert_eq!(facts.tokens_limit, NumericDiagnostic::Value(30000));
        if missing == 1 {
            assert_eq!(result.provider_outcome, ProviderOutcome::Unknown);
            assert_eq!(diagnostic.class, ProviderFailureClass::ResponseRead);
            assert_eq!(facts.body, DiagnosticBodyState::ReadFailed);
            assert_eq!(facts.category, CategoryDiagnostic::Absent);
        } else {
            assert!(matches!(
                result.provider_outcome,
                ProviderOutcome::Rejected(_)
            ));
            assert_eq!(diagnostic.class, ProviderFailureClass::HttpRefused);
            assert_eq!(facts.body, DiagnosticBodyState::Complete);
        }
        assert!(result.result.is_err());
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# Retained provider diagnostics MUST exclude configured credentials and submitted prompt content.
async fn typed_http_facts_do_not_copy_decoded_credentials_or_arbitrary_provider_text() {
    use borg_runtime::native::{NumericDiagnostic, QuotaDiagnostic, RetryHeaderDiagnostic};
    let credential = std::env::var("HOME").unwrap();
    let escaped: String = credential
        .encode_utf16()
        .map(|unit| format!("\\u{unit:04x}"))
        .collect();
    let body = format!(r#"{{"error":{{"code":"rate_limit_exceeded","message":"{escaped}"}}}}"#)
        .into_bytes();
    let headers=format!("retry-after: {credential}\r\nx-ratelimit-limit-tokens: PRIVATE_PROMPT\r\nx-extra: PRIVATE_EXTRA\r\n");
    let fixture = Fixture::start(429, body, Duration::ZERO, &headers).await;
    let result = observed(
        &input_with_summary_bytes(1),
        &config(fixture.endpoint.clone()),
    )
    .await;
    fixture.finish().await;
    let facts = result.diagnostic.unwrap();
    let encoded = serde_json::to_string(&facts).unwrap();
    for omitted in [
        credential.as_str(),
        "PRIVATE_PROMPT",
        "PRIVATE_EXTRA",
        "provider_message_sha256",
    ] {
        assert!(!encoded.contains(omitted));
    }
    let http = facts.http.unwrap();
    assert_eq!(http.retry_after, RetryHeaderDiagnostic::Redacted);
    assert_eq!(http.tokens_limit, NumericDiagnostic::Invalid);
    assert_eq!(http.quota, QuotaDiagnostic::Redacted);
    assert_eq!(
        result.provider_outcome,
        ProviderOutcome::Rejected(ProviderRejection {
            kind: RejectionKind::RateLimited,
            retry_after: RetryAfter::Invalid
        })
    );
}

#[tokio::test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A provider quota exhaustion MUST be distinguished from transient throttling.
async fn native_selected_throttle_shapes_use_original_header_delay_without_message_authority() {
    for (body, kind) in [
        (json!({"error":{"message":"PRIVATE_PROMPT try again in 999s", "type":"rate_limit_error", "param":null, "code":"slow_down"}}), RejectionKind::RateLimited),
        (json!({"error":{"message":"PRIVATE_PROMPT try again in 999s", "type":"tokens", "param":null, "code":"rate_limit_exceeded"}}), RejectionKind::RateLimited),
        (json!({"error":{"message":"PRIVATE_PROMPT retry now", "type":"insufficient_quota", "param":null, "code":"insufficient_quota"}}), RejectionKind::QuotaExhausted),
    ] {
        let fixture = Fixture::start(429, serde_json::to_vec(&body).unwrap(), Duration::ZERO,
            "retry-after: 1\r\nretry-after-ms: 23\r\n").await;
        let result = observed(&task(), &config(fixture.endpoint.clone())).await;
        fixture.finish().await;
        assert_eq!(result.provider_outcome, ProviderOutcome::Rejected(ProviderRejection {
            kind, retry_after: RetryAfter::DelayMs(1000),
        }));
        assert_eq!(result.request_exposure, RequestExposure::HttpResponseReceived);
        assert_eq!(result.usage, UsageObservation::Unknown);
        let text = result.result.unwrap_err();
        assert!(!text.contains("PRIVATE_PROMPT"));
        assert!(!text.contains("999s"));
        assert!(!text.contains("classification_refused"));
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# Retained provider diagnostics MUST exclude configured credentials and submitted prompt content.
async fn native_unknown_throttle_shapes_explain_only_fixed_refusal_reason() {
    let credential = std::env::var("HOME").unwrap();
    for (body, reason) in [
        (json!({"error":{"code":"rate_limit_exceeded","type":"requests","message":credential,"param":null}}), "unsupported_type"),
        (json!({"error":{"code":"slow_down","type":"insufficient_quota"}}), "status_or_type_mismatch"),
        (json!({"error":{"code":"slow_down","type":"rate_limit_error"},"PRIVATE_ROOT_FIELD":true}), "unexpected_root_keys"),
        (json!({"error":{"code":"slow_down","type":"rate_limit_error","PRIVATE_ERROR_FIELD":true}}), "unexpected_error_keys"),
        (json!({"error":{"code":"slow_down","type":42}}), "invalid_field_type"),
        (json!({"error":{"code":"slow_down"}}), "status_or_type_mismatch"),
        (json!({"error":{"code":"rate_limit_exceeded","type":"tokens"}}), "status_or_type_mismatch"),
        (json!({"error":{"code":"rate_limit_exceeded","type":"tokens","message":"PRIVATE_PROMPT","param":"PRIVATE_PARAM"}}), "status_or_type_mismatch"),
        (json!({"error":{"code":"PRIVATE_CODE","type":"rate_limit_error","message":"PRIVATE_PROMPT"}}), "unsupported_code"),
        (json!({"error":{}}), "missing_discriminator"),
        (json!({"error":null}), "invalid_error"),
    ] {
        let fixture = Fixture::start(429, serde_json::to_vec(&body).unwrap(), Duration::ZERO, "retry-after: 0\r\n").await;
        let result = observed(&task(), &config(fixture.endpoint.clone())).await;
        fixture.finish().await;
        assert_eq!(result.provider_outcome, ProviderOutcome::Unknown);
        assert_eq!(result.usage, UsageObservation::Unknown);
        let facts = result.diagnostic.as_ref().unwrap();
        assert_eq!(facts.http.as_ref().unwrap().category, borg_runtime::native::CategoryDiagnostic::Absent);
        let text = result.result.unwrap_err();
        assert!(text.contains(&format!("classification_refused={reason}")), "{text}");
        for private in [credential.as_str(), "PRIVATE_ROOT_FIELD", "PRIVATE_ERROR_FIELD", "PRIVATE_PROMPT", "PRIVATE_PARAM", "PRIVATE_CODE"] {
            assert!(!text.contains(private), "provider text leaked");
            assert!(!serde_json::to_string(facts).unwrap().contains(private));
        }
    }
    for body in [
        br#"{"error":{"code":"slow_down","type":"rate_limit_error","type":"rate_limit_error"}}"#.as_slice(),
        br#"{"error":{"code":"rate_limit_exceeded","type":"tokens","message":"safe","param":null,"param":null}}"#.as_slice(),
        br#"{"error":{"code":"slow_down","type":"rate_limit_error"}"#.as_slice(),
    ] {
        let fixture = Fixture::start(429, body.to_vec(), Duration::ZERO, "retry-after: 0\r\n").await;
        let result = observed(&task(), &config(fixture.endpoint.clone())).await;
        fixture.finish().await;
        assert_eq!(result.provider_outcome, ProviderOutcome::Unknown);
        assert_eq!(result.usage, UsageObservation::Unknown);
        assert!(!result.result.unwrap_err().contains("category=rate_limited"));
    }
}
