//! Explicit authority and scheduling bounds for disposable worker data only.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RetentionLimits {
    pub worker: WorkerRetentionLimits,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkerRetentionLimits {
    pub success_grace_ms: u64,
    pub failed_grace_ms: u64,
    pub cancelled_grace_ms: u64,
    pub idle_cache_grace_ms: u64,
    pub batch_entries: usize,
    pub filesystem_entries_per_yield: usize,
    pub operation_timeout_ms: u64,
    pub retry_delay_ms: u64,
    pub record_bytes: usize,
}

impl RetentionLimits {
    pub fn validate(&self) -> Result<(), String> {
        self.worker.validate()
    }
}

impl WorkerRetentionLimits {
    pub fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("success_grace_ms", self.success_grace_ms),
            ("failed_grace_ms", self.failed_grace_ms),
            ("cancelled_grace_ms", self.cancelled_grace_ms),
            ("idle_cache_grace_ms", self.idle_cache_grace_ms),
            ("operation_timeout_ms", self.operation_timeout_ms),
            ("retry_delay_ms", self.retry_delay_ms),
        ] {
            if std::time::Instant::now()
                .checked_add(std::time::Duration::from_millis(value))
                .is_none()
                || std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .ok()
                    .and_then(|now| u64::try_from(now.as_millis()).ok())
                    .and_then(|now| now.checked_add(value))
                    .is_none()
            {
                return Err(format!(
                    "retention.worker.{field} exceeds clock representation"
                ));
            }
        }
        for (field, value) in [
            ("operation_timeout_ms", self.operation_timeout_ms),
            ("retry_delay_ms", self.retry_delay_ms),
        ] {
            if value == 0 {
                return Err(format!("retention.worker.{field} must be positive"));
            }
        }
        for (field, value) in [
            ("batch_entries", self.batch_entries),
            (
                "filesystem_entries_per_yield",
                self.filesystem_entries_per_yield,
            ),
            ("record_bytes", self.record_bytes),
        ] {
            if value == 0 || value > isize::MAX as usize {
                return Err(format!(
                    "retention.worker.{field} exceeds positive addressable representation"
                ));
            }
        }
        Ok(())
    }
}
