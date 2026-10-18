//! Optional accepted timing policy for retrying a confirmed provider rejection.
//! Request budgets and the original task deadline remain the retry bounds.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeRetryLimits {
    pub initial_backoff_ms: u64,
    pub backoff_multiplier: u32,
    pub max_backoff_ms: u64,
    pub jitter_ms: u64,
    pub circuit: NativeCircuitLimits,
    pub auth_cooldown_ms: u64,
    pub auth_max_entries: usize,
    pub coordination_record_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeCircuitLimits {
    pub failure_threshold: u32,
    pub open_ms: u64,
    pub max_entries: usize,
}

impl NativeRetryLimits {
    pub fn validate(&self) -> Result<(), String> {
        if self.circuit.failure_threshold == 0 {
            return Err("native_retry.circuit.failure_threshold must be positive".into());
        }
        for (field, value) in [("circuit.open_ms", self.circuit.open_ms), ("auth_cooldown_ms", self.auth_cooldown_ms)] {
            if value == 0 || std::time::Instant::now().checked_add(std::time::Duration::from_millis(value)).is_none()
                || std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).ok()
                    .and_then(|now| u64::try_from(now.as_millis()).ok()).and_then(|now| now.checked_add(value)).is_none() {
                return Err(format!("native_retry.{field} exceeds positive clock representation"));
            }
        }
        for (field, value) in [("circuit.max_entries", self.circuit.max_entries), ("auth_max_entries", self.auth_max_entries),
            ("coordination_record_bytes", self.coordination_record_bytes)] {
            if value == 0 || value > isize::MAX as usize {
                return Err(format!("native_retry.{field} exceeds positive addressable representation"));
            }
        }
        for (field, value) in [
            ("initial_backoff_ms", self.initial_backoff_ms),
            ("max_backoff_ms", self.max_backoff_ms),
            ("jitter_ms", self.jitter_ms),
        ] {
            if value == 0 {
                return Err(format!("native_retry.{field} must be positive"));
            }
        }
        if self.backoff_multiplier == 0 {
            return Err("native_retry.backoff_multiplier must be at least one".into());
        }
        if self.initial_backoff_ms > self.max_backoff_ms {
            return Err("native_retry.initial_backoff_ms exceeds max_backoff_ms".into());
        }
        let maximum_delay = self
            .max_backoff_ms
            .checked_add(self.jitter_ms)
            .ok_or("native_retry.max_backoff_ms plus jitter_ms exceeds clock representation")?;
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(maximum_delay))
            .is_none()
            || std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .and_then(|now| u64::try_from(now.as_millis()).ok())
                .and_then(|now| now.checked_add(maximum_delay))
                .is_none()
        {
            return Err(
                "native_retry.max_backoff_ms plus jitter_ms exceeds clock representation".into(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Deliberately local test values, not metadata defaults.
    fn policy() -> NativeRetryLimits {
        NativeRetryLimits {
            initial_backoff_ms: 10,
            backoff_multiplier: 2,
            max_backoff_ms: 80,
            jitter_ms: 7,
            circuit: NativeCircuitLimits { failure_threshold: 2, open_ms: 50, max_entries: 8 },
            auth_cooldown_ms: 80, auth_max_entries: 8, coordination_record_bytes: 65_536,
        }
    }

    #[test]
    fn positive_timing_policy_round_trips_without_inventing_a_retry_count() {
        let value = policy();
        value.validate().unwrap();
        let json = serde_json::to_value(&value).unwrap();
        assert_eq!(json.as_object().unwrap().len(), 8);
        assert_eq!(serde_json::from_value::<NativeRetryLimits>(json).unwrap(), value);
        let mut unknown = serde_json::to_value(policy()).unwrap();
        unknown["max_retries"] = serde_json::json!(3);
        assert!(serde_json::from_value::<NativeRetryLimits>(unknown).is_err());
    }

    #[test]
    fn zero_or_reversed_timings_refuse() {
        for field in ["initial_backoff_ms", "max_backoff_ms", "jitter_ms"] {
            let mut json = serde_json::to_value(policy()).unwrap();
            json[field] = serde_json::json!(0);
            let invalid: NativeRetryLimits = serde_json::from_value(json).unwrap();
            assert!(invalid.validate().unwrap_err().contains(field));
        }
        let mut invalid = policy();
        invalid.backoff_multiplier = 0;
        assert!(invalid.validate().unwrap_err().contains("backoff_multiplier"));
        invalid = policy();
        invalid.initial_backoff_ms = invalid.max_backoff_ms + 1;
        assert!(invalid.validate().unwrap_err().contains("initial_backoff_ms"));
    }

    #[test]
    fn one_multiplier_is_valid_and_combined_clock_overflow_refuses() {
        let mut value = policy();
        value.backoff_multiplier = 1;
        value.validate().unwrap();
        value.jitter_ms = u64::MAX;
        assert!(value.validate().unwrap_err().contains("clock representation"));
    }

    #[test]
    fn coordination_requires_explicit_positive_behavior_and_storage_bounds() {
        for path in [vec!["circuit", "failure_threshold"], vec!["circuit", "open_ms"],
            vec!["circuit", "max_entries"], vec!["auth_cooldown_ms"], vec!["auth_max_entries"],
            vec!["coordination_record_bytes"]] {
            let mut json = serde_json::to_value(policy()).unwrap();
            let mut field = &mut json;
            for component in &path { field = &mut field[*component]; }
            *field = serde_json::json!(0);
            let value: NativeRetryLimits = serde_json::from_value(json).unwrap();
            assert!(value.validate().unwrap_err().contains(&path.join(".")));
        }
        let mut value = policy();
        value.auth_cooldown_ms = u64::MAX;
        assert!(value.validate().unwrap_err().contains("auth_cooldown_ms"));
        let mut json = serde_json::to_value(policy()).unwrap();
        json.as_object_mut().unwrap().remove("circuit");
        assert!(serde_json::from_value::<NativeRetryLimits>(json).is_err());
    }
}
