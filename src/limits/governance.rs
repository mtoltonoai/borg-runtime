//! Explicit bounds for the selected finite governance mechanisms.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GovernanceLimits {
    pub record_bytes: usize,
    pub reason_items: usize,
    pub reason_bytes: usize,
    pub scan_rows: usize,
    pub rule_versions: usize,
    pub configuration_versions: usize,
    pub shadow_evidence_items: usize,
    /// Zero denies any additional continuation; it does not admit completion.
    pub completion_continuations: usize,
    pub override_max_ms: u64,
}

impl GovernanceLimits {
    pub fn validate(&self, current: &super::RuntimeLimits) -> Result<(), String> {
        for (name, value) in [
            ("record_bytes", self.record_bytes), ("reason_items", self.reason_items),
            ("reason_bytes", self.reason_bytes), ("scan_rows", self.scan_rows),
            ("rule_versions", self.rule_versions),
            ("configuration_versions", self.configuration_versions),
            ("shadow_evidence_items", self.shadow_evidence_items),
        ] {
            if value == 0 || value > isize::MAX as usize {
                return Err(format!("governance.{name} is outside the addressable positive range"));
            }
        }
        if self.record_bytes > current.transport.body_bytes
            || self.reason_bytes > self.record_bytes
            || self.completion_continuations > u32::MAX as usize {
            return Err("governance record, reason or continuation bound exceeds its enclosing representation".into());
        }
        let failure = vec![crate::governance::CompletionReason::EvaluationUnavailable {
            code: crate::governance::CompletionFailure::BoundExceeded,
        }];
        let failure_bytes = serde_json::to_vec(&failure).map_err(|e|e.to_string())?.len();
        if self.reason_bytes < failure_bytes {
            return Err("governance.reason_bytes cannot retain its minimal typed failure reason".into());
        }
        if self.override_max_ms == 0
            || std::time::Instant::now().checked_add(std::time::Duration::from_millis(self.override_max_ms)).is_none() {
            return Err("governance.override_max_ms is outside the clock range".into());
        }
        Ok(())
    }
}
