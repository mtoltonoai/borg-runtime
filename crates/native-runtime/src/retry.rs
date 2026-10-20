//! One unbounded retry policy with bounded, jittered sleeps. No action replay.
use crate::AgentConfig;
use reqwest::header::{HeaderMap, RETRY_AFTER};
use std::time::SystemTime;

pub(crate) fn retry_after_ms(headers: &HeaderMap) -> Option<u64> {
    let text = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
    if let Ok(seconds) = text.parse::<u64>() {
        return Some(seconds.saturating_mul(1000));
    }
    let when = httpdate::parse_http_date(text).ok()?;
    Some(
        when.duration_since(SystemTime::now())
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64,
    )
}

pub(crate) fn delay_ms(config: &AgentConfig, attempt: u64, retry_after: Option<u64>) -> u64 {
    let random = uuid::Uuid::new_v4().as_u128() as u64;
    delay_with_random(config, attempt, retry_after, random)
}
fn delay_with_random(
    config: &AgentConfig,
    attempt: u64,
    retry_after: Option<u64>,
    random: u64,
) -> u64 {
    let cap = config.retry_max_ms.max(1);
    let factor = 1u64
        .checked_shl(attempt.saturating_sub(1).min(63) as u32)
        .unwrap_or(u64::MAX);
    let base = config
        .retry_initial_ms
        .saturating_mul(factor)
        .min(cap)
        .max(1);
    let spread = (u128::from(base) * u128::from(config.retry_jitter_percent.min(100)) / 100) as u64;
    let lower = base.saturating_sub(spread).max(1);
    let upper = base.saturating_add(spread).min(cap).max(lower);
    let jittered = lower.saturating_add(random % upper.saturating_sub(lower).saturating_add(1));
    // The local exponential/jitter delay is capped, but a server cooldown is
    // a floor, even when longer. Operator stop still interrupts the whole wait.
    jittered.min(cap).max(retry_after.unwrap_or(0)).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn null_default_is_unlimited_and_backoff_is_bounded_with_jitter_and_hints() {
        let config: AgentConfig =
            serde_json::from_value(json!({"id":"test","instructions":"test"})).unwrap();
        assert_eq!(config.max_retries, None);
        assert_eq!(config.max_turns, None);
        assert_eq!(config.max_output_tokens, Some(128000));
        assert_eq!(config.context_window_tokens, 1050000);
        assert_eq!(delay_with_random(&config, 1, None, 0), 400);
        assert_eq!(delay_with_random(&config, 1, None, 200), 600);
        assert_eq!(delay_with_random(&config, 1, Some(2000), 0), 2000);
        for attempt in [1, 2, 20, 64, u32::MAX as u64 + 1, u64::MAX] {
            for random in [0, 42, u64::MAX] {
                let delay = delay_with_random(&config, attempt, None, random);
                assert!((1..=60000).contains(&delay));
                assert_eq!(
                    delay_with_random(&config, attempt, Some(300000), random),
                    300000
                );
            }
        }
    }
    #[test]
    fn retry_after_supports_seconds_and_http_dates_without_exposing_header_text() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, "17".parse().unwrap());
        assert_eq!(retry_after_ms(&headers), Some(17000));
        headers.insert(
            RETRY_AFTER,
            "Sun, 06 Nov 1994 08:49:37 GMT".parse().unwrap(),
        );
        assert_eq!(retry_after_ms(&headers), Some(0));
        let later = SystemTime::now() + std::time::Duration::from_secs(60);
        headers.insert(RETRY_AFTER, httpdate::fmt_http_date(later).parse().unwrap());
        assert!((58000..=60000).contains(&retry_after_ms(&headers).unwrap()));
        headers.insert(RETRY_AFTER, "invalid-private-value".parse().unwrap());
        assert_eq!(retry_after_ms(&headers), None);
    }
}
