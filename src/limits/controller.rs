//! Explicit controller policy metadata. Seed values live only in the JSON metadata file.
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerLimits {
    pub queue_capacity: usize,
    pub max_tasks: usize,
    pub max_release_candidates: usize,
    pub max_receipts: usize,
    pub max_cleanup_receipts: usize,
    pub max_workers: usize,
    pub max_dependencies: usize,
    pub max_commands_per_reply: usize,
    pub max_list_tasks: usize,
    pub max_worker_capacity: usize,
    pub worker_waiters: usize,
    pub ui_waiters: usize,
    pub validation_parallelism: usize,
    pub command_wait_ms: u64,
    pub validation_timeout_ms: u64,
    pub sqlite_busy_timeout_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store_retry: Option<StoreRetryLimits>,
    pub max_task_input_bytes: usize,
    pub max_task_output_bytes: usize,
    pub max_task_result_bytes: usize,
    pub max_delegation_depth: usize,
    pub max_acceptance_commands: usize,
    pub max_acceptance_arguments: usize,
    pub max_acceptance_argument_bytes: usize,
    pub max_repository_identity_bytes: usize,
    pub max_progress_bytes: usize,
    pub max_validation_strings: usize,
    pub max_validation_string_bytes: usize,
    pub max_checkpoint_ref_bytes: usize,
    pub command_page_bytes: usize,
    pub run_command_bytes: usize,
    pub list_page_bytes: usize,
    pub outcome_bytes: usize,
    pub collaboration_page_bytes: usize,
    pub collaboration_item_bytes: usize,
    pub collaboration_page_items: usize,
    pub collaboration_cursor_bytes: usize,
    pub collaboration_progress_bytes: usize,
    pub collaboration_child_instruction_bytes: usize,
    pub memory_value_bytes: usize,
    pub effect_value_bytes: usize,
    pub history_kind_bytes: usize,
    pub memory_notice_event_bytes: usize,
    pub memory_notice_scan_min: usize,
    pub memory_notice_scan_factor: usize,
    pub memory_notice_scan_max: usize,
    pub memory_notice_items: usize,
    pub memory_pending_bytes: usize,
    pub memory_pending_page_bytes: usize,
    pub release_binary_bytes: u64,
    pub release_package_bytes: u64,
    pub protected_envelope_reserve_bytes: usize,
    pub protected_default_records: usize,
    pub protected_check_results: usize,
    pub protected_observation_bytes: usize,
    pub owner_binding_bytes: usize,
    pub owner_lock_wait_ms: u64,
    pub owner_lock_retry_ms: u64,
    pub fdinfo_bytes: usize,
    pub fdinfo_lines: usize,
    pub mountinfo_bytes: usize,
    pub mountinfo_mounts: usize,
    pub mountinfo_line_bytes: usize,
    pub mountinfo_fields: usize,
}

/// Current controller-service policy for one identified SQL phase. It neither
/// alters an accepted task deadline nor authorizes replay of external work.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreRetryLimits {
    pub backoff_ms: u64,
    pub operation_timeout_ms: u64,
}
impl StoreRetryLimits {
    pub fn validate(&self) -> Result<(), String> {
        if self.backoff_ms == 0 || self.backoff_ms >= self.operation_timeout_ms {
            return Err("controller.store_retry requires 0 < backoff_ms < operation_timeout_ms".into());
        }
        for n in [self.backoff_ms, self.operation_timeout_ms] {
            std::time::Instant::now().checked_add(std::time::Duration::from_millis(n))
                .ok_or("controller.store_retry exceeds clock representation")?;
        }
        Ok(())
    }
}

impl ControllerLimits {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(retry) = &self.store_retry { retry.validate()?; }
        if self.history_kind_bytes == 0 { return Err("controller.history_kind_bytes must be positive".into()); }
        if self.queue_capacity == 0 { return Err("controller.queue_capacity must be positive".into()); }
        if self.max_tasks == 0 { return Err("controller.max_tasks must be positive".into()); }
        if self.max_release_candidates == 0 { return Err("controller.max_release_candidates must be positive".into()); }
        if self.max_receipts == 0 { return Err("controller.max_receipts must be positive".into()); }
        if self.max_cleanup_receipts == 0 { return Err("controller.max_cleanup_receipts must be positive".into()); }
        if self.max_workers == 0 { return Err("controller.max_workers must be positive".into()); }
        if self.max_commands_per_reply == 0 { return Err("controller.max_commands_per_reply must be positive".into()); }
        if self.max_list_tasks == 0 { return Err("controller.max_list_tasks must be positive".into()); }
        if self.max_worker_capacity == 0 { return Err("controller.max_worker_capacity must be positive".into()); }
        if self.worker_waiters == 0 { return Err("controller.worker_waiters must be positive".into()); }
        if self.ui_waiters == 0 { return Err("controller.ui_waiters must be positive".into()); }
        if self.validation_parallelism == 0 { return Err("controller.validation_parallelism must be positive".into()); }
        if self.command_wait_ms == 0 { return Err("controller.command_wait_ms must be positive".into()); }
        if self.validation_timeout_ms == 0 { return Err("controller.validation_timeout_ms must be positive".into()); }
        if self.sqlite_busy_timeout_ms == 0 { return Err("controller.sqlite_busy_timeout_ms must be positive".into()); }
        if self.max_task_input_bytes == 0 { return Err("controller.max_task_input_bytes must be positive".into()); }
        if self.max_task_output_bytes == 0 { return Err("controller.max_task_output_bytes must be positive".into()); }
        if self.max_task_result_bytes == 0 { return Err("controller.max_task_result_bytes must be positive".into()); }
        if self.max_acceptance_commands == 0 { return Err("controller.max_acceptance_commands must be positive".into()); }
        if self.max_acceptance_arguments == 0 { return Err("controller.max_acceptance_arguments must be positive".into()); }
        if self.max_acceptance_argument_bytes == 0 { return Err("controller.max_acceptance_argument_bytes must be positive".into()); }
        if self.max_repository_identity_bytes == 0 { return Err("controller.max_repository_identity_bytes must be positive".into()); }
        if self.max_progress_bytes == 0 { return Err("controller.max_progress_bytes must be positive".into()); }
        if self.max_validation_strings == 0 { return Err("controller.max_validation_strings must be positive".into()); }
        if self.max_validation_string_bytes == 0 { return Err("controller.max_validation_string_bytes must be positive".into()); }
        if self.max_checkpoint_ref_bytes == 0 { return Err("controller.max_checkpoint_ref_bytes must be positive".into()); }
        if self.command_page_bytes == 0 { return Err("controller.command_page_bytes must be positive".into()); }
        if self.run_command_bytes == 0 { return Err("controller.run_command_bytes must be positive".into()); }
        if self.list_page_bytes == 0 { return Err("controller.list_page_bytes must be positive".into()); }
        if self.outcome_bytes == 0 { return Err("controller.outcome_bytes must be positive".into()); }
        if self.collaboration_page_bytes == 0 { return Err("controller.collaboration_page_bytes must be positive".into()); }
        if self.collaboration_item_bytes == 0 { return Err("controller.collaboration_item_bytes must be positive".into()); }
        if self.collaboration_page_items == 0 { return Err("controller.collaboration_page_items must be positive".into()); }
        if self.collaboration_cursor_bytes == 0 { return Err("controller.collaboration_cursor_bytes must be positive".into()); }
        if self.collaboration_progress_bytes == 0 { return Err("controller.collaboration_progress_bytes must be positive".into()); }
        if self.collaboration_child_instruction_bytes == 0 { return Err("controller.collaboration_child_instruction_bytes must be positive".into()); }
        if self.memory_value_bytes == 0 { return Err("controller.memory_value_bytes must be positive".into()); }
        if self.effect_value_bytes == 0 { return Err("controller.effect_value_bytes must be positive".into()); }
        if self.memory_notice_event_bytes == 0 { return Err("controller.memory_notice_event_bytes must be positive".into()); }
        if self.memory_notice_scan_min == 0 { return Err("controller.memory_notice_scan_min must be positive".into()); }
        if self.memory_notice_scan_factor == 0 { return Err("controller.memory_notice_scan_factor must be positive".into()); }
        if self.memory_notice_scan_max == 0 { return Err("controller.memory_notice_scan_max must be positive".into()); }
        if self.memory_notice_items == 0 { return Err("controller.memory_notice_items must be positive".into()); }
        if self.memory_pending_bytes == 0 { return Err("controller.memory_pending_bytes must be positive".into()); }
        if self.memory_pending_page_bytes == 0 { return Err("controller.memory_pending_page_bytes must be positive".into()); }
        if self.release_binary_bytes == 0 { return Err("controller.release_binary_bytes must be positive".into()); }
        if self.release_package_bytes == 0 { return Err("controller.release_package_bytes must be positive".into()); }
        if self.protected_envelope_reserve_bytes == 0 { return Err("controller.protected_envelope_reserve_bytes must be positive".into()); }
        if self.protected_default_records == 0 { return Err("controller.protected_default_records must be positive".into()); }
        if self.protected_check_results == 0 { return Err("controller.protected_check_results must be positive".into()); }
        if self.protected_observation_bytes == 0 { return Err("controller.protected_observation_bytes must be positive".into()); }
        if self.owner_binding_bytes == 0 { return Err("controller.owner_binding_bytes must be positive".into()); }
        if self.owner_lock_wait_ms == 0 { return Err("controller.owner_lock_wait_ms must be positive".into()); }
        if self.owner_lock_retry_ms == 0 { return Err("controller.owner_lock_retry_ms must be positive".into()); }
        if self.fdinfo_bytes == 0 { return Err("controller.fdinfo_bytes must be positive".into()); }
        if self.fdinfo_lines == 0 { return Err("controller.fdinfo_lines must be positive".into()); }
        if self.mountinfo_bytes == 0 { return Err("controller.mountinfo_bytes must be positive".into()); }
        if self.mountinfo_mounts == 0 { return Err("controller.mountinfo_mounts must be positive".into()); }
        if self.mountinfo_line_bytes == 0 { return Err("controller.mountinfo_line_bytes must be positive".into()); }
        if self.mountinfo_fields == 0 { return Err("controller.mountinfo_fields must be positive".into()); }
        if self.queue_capacity > tokio::sync::Semaphore::MAX_PERMITS
            || self.worker_waiters > tokio::sync::Semaphore::MAX_PERMITS
            || self.ui_waiters > tokio::sync::Semaphore::MAX_PERMITS
            || self.validation_parallelism > tokio::sync::Semaphore::MAX_PERMITS {
            return Err("controller concurrency exceeds the semaphore representation".into());
        }
        for value in [self.max_tasks, self.max_release_candidates, self.max_receipts,
            self.max_cleanup_receipts,self.max_workers,self.max_dependencies,self.max_commands_per_reply,
            self.max_list_tasks,self.max_worker_capacity,self.collaboration_page_items,
            self.memory_notice_scan_max,self.memory_notice_items] {
            i64::try_from(value).map_err(|_| "controller limit exceeds SQLite integer representation")?;
            value.checked_add(1).ok_or("controller bounded-reader lookahead overflow")?;
        }
        if self.memory_notice_scan_min > self.memory_notice_scan_max
            || self.memory_pending_page_bytes > self.memory_pending_bytes
            || self.release_binary_bytes >= self.release_package_bytes {
            return Err("controller limits have inconsistent nested bounds".into());
        }
        for value in [self.owner_binding_bytes, self.fdinfo_bytes, self.mountinfo_bytes,
            self.mountinfo_mounts,self.mountinfo_line_bytes,self.mountinfo_fields,self.history_kind_bytes,
            self.memory_notice_event_bytes,self.memory_pending_bytes,self.collaboration_cursor_bytes] {
            value.checked_add(1).ok_or("controller read bound overflow")?;
        }
        for value in [self.command_wait_ms,self.validation_timeout_ms,self.sqlite_busy_timeout_ms,self.owner_lock_wait_ms,self.owner_lock_retry_ms] {
            std::time::Instant::now().checked_add(std::time::Duration::from_millis(value)).ok_or("controller timeout exceeds clock representation")?;
        }
        self.max_receipts.checked_add(self.max_cleanup_receipts).and_then(|v|v.checked_add(self.max_tasks)).and_then(|v|v.checked_mul(2)).and_then(|v|i64::try_from(v).ok()).ok_or("controller startup inventory bound overflow")?;
        Ok(())
    }
}

#[cfg(test)]
mod store_retry_tests {
    use super::*;

    #[test]
    fn absent_store_policy_round_trips_without_an_added_field() {
        let limits = crate::limits::RuntimeLimits::seed().unwrap().controller;
        assert!(limits.store_retry.is_none());
        let bytes = serde_json::to_vec(&limits).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(value.get("store_retry").is_none());
        let retained: ControllerLimits = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(serde_json::to_vec(&retained).unwrap(), bytes);
    }

    #[test]
    fn explicit_store_policy_is_closed_positive_and_clock_bounded() {
        let policy = StoreRetryLimits { backoff_ms: 10, operation_timeout_ms: 100 };
        policy.validate().unwrap();
        let value = serde_json::to_value(&policy).unwrap();
        assert_eq!(value, serde_json::json!({"backoff_ms":10,"operation_timeout_ms":100}));
        assert!(serde_json::from_value::<StoreRetryLimits>(serde_json::json!({
            "backoff_ms":10,"operation_timeout_ms":100,"retry_count":3
        })).is_err());
        for (backoff_ms, operation_timeout_ms) in [(0,100),(10,0),(10,10),(100,10)] {
            assert!(StoreRetryLimits { backoff_ms, operation_timeout_ms }.validate().is_err());
        }
        let largest = StoreRetryLimits { backoff_ms: 1, operation_timeout_ms: u64::MAX };
        assert_eq!(largest.validate().is_ok(), std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(u64::MAX)).is_some());
        let mut current = crate::limits::RuntimeLimits::seed().unwrap();
        current.controller.store_retry = Some(policy.clone());
        current.controller.validate().unwrap();
        let restored: crate::limits::RuntimeLimits = serde_json::from_slice(&serde_json::to_vec(&current).unwrap()).unwrap();
        assert_eq!(restored.controller.store_retry, Some(policy));
        assert!(restored.native_retry.is_none(), "store scheduling does not require provider policy");
    }
}
