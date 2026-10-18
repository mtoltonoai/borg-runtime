//! Explicit metadata limits used by worker startup and accepted execution.
//! No Default implementation or implicit policy values: seed data is external.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkerLimits {
    pub state_format_bytes: usize,
    pub native_tracking_bytes: usize,
    pub controller_call_timeout_ms: u64,
    pub coding_capacity: usize,
    pub native_capacity: usize,
    pub max_total_capacity: usize,
    pub native_validation_capacity: usize,
    pub native_observation_queue_records: usize,
    pub native_observation_proc_bytes: usize,
    pub native_validation_poll_ms: u64,
    pub native_validation_cleanup_timeout_ms: u64,
    pub identity_bytes: usize,
    pub process_record_bytes: usize,
    pub process_registry_records: usize,
    pub tracked_descendants: usize,
    pub process_census_entries: usize,
    pub identity_read_attempts: usize,
    pub identity_retry_us: u64,
    pub owner_lock_timeout_ms: u64,
    pub owner_lock_retry_ms: u64,
    pub spawn_ready_timeout_ms: u64,
    pub spawn_marker_bytes: usize,
    pub process_stop_timeout_ms: u64,
    pub process_poll_ms: u64,
    pub stream_drain_timeout_ms: u64,
    pub recovery_checks: usize,
    pub recovery_interval_ms: u64,
    pub shutdown_join_timeout_ms: u64,
    pub shutdown_native_timeout_ms: u64,
    pub shutdown_outbox_timeout_ms: u64,
    pub reconnect_initial_ms: u64,
    pub reconnect_max_ms: u64,
    pub outbox_records: usize,
    pub outbox_batch: usize,
    pub outbox_call_timeout_ms: u64,
    pub command_records: usize,
    pub command_record_bytes: usize,
    pub mutation_record_bytes: usize,
    pub publication_record_bytes: usize,
    pub acceptance_call_timeout_ms: u64,
    pub handoff_call_timeout_ms: u64,
    pub mutation_call_timeout_ms: u64,
    pub publication_call_timeout_ms: u64,
    pub publication_recovery_timeout_ms: u64,
    pub git_output_bytes: usize,
    pub git_cache_identity_bytes: usize,
    pub repository_identity_bytes: usize,
    pub resource_profile_bytes: usize,
    pub resource_proc_bytes: usize,
    pub resource_lines: usize,
    pub resource_line_bytes: usize,
    pub resource_control_bytes: usize,
    pub resource_path_bytes: usize,
    pub credential_scan_units: usize,
    pub credential_scan_depth: usize,
}
impl WorkerLimits {
    pub fn validate(&self) -> Result<(), String> {
        if self.state_format_bytes == 0 || self.state_format_bytes > isize::MAX as usize {
            return Err("worker.state_format_bytes exceeds positive addressable bound".into());
        }
        if self.native_tracking_bytes > isize::MAX as usize {
            return Err(
                "WorkerLimits.native_tracking_bytes exceeds addressable representation".into(),
            );
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.controller_call_timeout_ms,
            ))
            .is_none()
        {
            return Err(
                "WorkerLimits.controller_call_timeout_ms exceeds clock representation".into(),
            );
        }
        if self.coding_capacity > isize::MAX as usize {
            return Err("WorkerLimits.coding_capacity exceeds addressable representation".into());
        }
        if self.native_capacity > isize::MAX as usize {
            return Err("WorkerLimits.native_capacity exceeds addressable representation".into());
        }
        if self.max_total_capacity > isize::MAX as usize {
            return Err(
                "WorkerLimits.max_total_capacity exceeds addressable representation".into(),
            );
        }
        if self.native_validation_capacity > isize::MAX as usize {
            return Err(
                "WorkerLimits.native_validation_capacity exceeds addressable representation".into(),
            );
        }
        if self.native_observation_queue_records > isize::MAX as usize {
            return Err(
                "WorkerLimits.native_observation_queue_records exceeds addressable representation"
                    .into(),
            );
        }
        if self.native_observation_proc_bytes > isize::MAX as usize {
            return Err(
                "WorkerLimits.native_observation_proc_bytes exceeds addressable representation"
                    .into(),
            );
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.native_validation_poll_ms,
            ))
            .is_none()
        {
            return Err(
                "WorkerLimits.native_validation_poll_ms exceeds clock representation".into(),
            );
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.native_validation_cleanup_timeout_ms,
            ))
            .is_none()
        {
            return Err(
                "WorkerLimits.native_validation_cleanup_timeout_ms exceeds clock representation"
                    .into(),
            );
        }
        if self.identity_bytes > isize::MAX as usize {
            return Err("WorkerLimits.identity_bytes exceeds addressable representation".into());
        }
        if self.process_record_bytes > isize::MAX as usize {
            return Err(
                "WorkerLimits.process_record_bytes exceeds addressable representation".into(),
            );
        }
        if self.process_registry_records > isize::MAX as usize {
            return Err(
                "WorkerLimits.process_registry_records exceeds addressable representation".into(),
            );
        }
        if self.tracked_descendants > isize::MAX as usize {
            return Err(
                "WorkerLimits.tracked_descendants exceeds addressable representation".into(),
            );
        }
        if self.process_census_entries > isize::MAX as usize {
            return Err(
                "WorkerLimits.process_census_entries exceeds addressable representation".into(),
            );
        }
        if self.identity_read_attempts > isize::MAX as usize {
            return Err(
                "WorkerLimits.identity_read_attempts exceeds addressable representation".into(),
            );
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(self.owner_lock_timeout_ms))
            .is_none()
        {
            return Err("WorkerLimits.owner_lock_timeout_ms exceeds clock representation".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(self.owner_lock_retry_ms))
            .is_none()
        {
            return Err("WorkerLimits.owner_lock_retry_ms exceeds clock representation".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.spawn_ready_timeout_ms,
            ))
            .is_none()
        {
            return Err("WorkerLimits.spawn_ready_timeout_ms exceeds clock representation".into());
        }
        if self.spawn_marker_bytes > isize::MAX as usize {
            return Err(
                "WorkerLimits.spawn_marker_bytes exceeds addressable representation".into(),
            );
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.process_stop_timeout_ms,
            ))
            .is_none()
        {
            return Err("WorkerLimits.process_stop_timeout_ms exceeds clock representation".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(self.process_poll_ms))
            .is_none()
        {
            return Err("WorkerLimits.process_poll_ms exceeds clock representation".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.stream_drain_timeout_ms,
            ))
            .is_none()
        {
            return Err("WorkerLimits.stream_drain_timeout_ms exceeds clock representation".into());
        }
        if self.recovery_checks > isize::MAX as usize {
            return Err("WorkerLimits.recovery_checks exceeds addressable representation".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(self.recovery_interval_ms))
            .is_none()
        {
            return Err("WorkerLimits.recovery_interval_ms exceeds clock representation".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.shutdown_join_timeout_ms,
            ))
            .is_none()
        {
            return Err(
                "WorkerLimits.shutdown_join_timeout_ms exceeds clock representation".into(),
            );
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.shutdown_native_timeout_ms,
            ))
            .is_none()
        {
            return Err(
                "WorkerLimits.shutdown_native_timeout_ms exceeds clock representation".into(),
            );
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.shutdown_outbox_timeout_ms,
            ))
            .is_none()
        {
            return Err(
                "WorkerLimits.shutdown_outbox_timeout_ms exceeds clock representation".into(),
            );
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(self.reconnect_initial_ms))
            .is_none()
        {
            return Err("WorkerLimits.reconnect_initial_ms exceeds clock representation".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(self.reconnect_max_ms))
            .is_none()
        {
            return Err("WorkerLimits.reconnect_max_ms exceeds clock representation".into());
        }
        if self.outbox_records > isize::MAX as usize {
            return Err("WorkerLimits.outbox_records exceeds addressable representation".into());
        }
        if self.outbox_batch > isize::MAX as usize {
            return Err("WorkerLimits.outbox_batch exceeds addressable representation".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.outbox_call_timeout_ms,
            ))
            .is_none()
        {
            return Err("WorkerLimits.outbox_call_timeout_ms exceeds clock representation".into());
        }
        if self.command_records > isize::MAX as usize {
            return Err("WorkerLimits.command_records exceeds addressable representation".into());
        }
        if self.command_record_bytes > isize::MAX as usize {
            return Err(
                "WorkerLimits.command_record_bytes exceeds addressable representation".into(),
            );
        }
        if self.mutation_record_bytes > isize::MAX as usize {
            return Err(
                "WorkerLimits.mutation_record_bytes exceeds addressable representation".into(),
            );
        }
        if self.publication_record_bytes > isize::MAX as usize {
            return Err(
                "WorkerLimits.publication_record_bytes exceeds addressable representation".into(),
            );
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.acceptance_call_timeout_ms,
            ))
            .is_none()
        {
            return Err(
                "WorkerLimits.acceptance_call_timeout_ms exceeds clock representation".into(),
            );
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.handoff_call_timeout_ms,
            ))
            .is_none()
        {
            return Err("WorkerLimits.handoff_call_timeout_ms exceeds clock representation".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.mutation_call_timeout_ms,
            ))
            .is_none()
        {
            return Err(
                "WorkerLimits.mutation_call_timeout_ms exceeds clock representation".into(),
            );
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.publication_call_timeout_ms,
            ))
            .is_none()
        {
            return Err(
                "WorkerLimits.publication_call_timeout_ms exceeds clock representation".into(),
            );
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.publication_recovery_timeout_ms,
            ))
            .is_none()
        {
            return Err(
                "WorkerLimits.publication_recovery_timeout_ms exceeds clock representation".into(),
            );
        }
        if self.git_output_bytes > isize::MAX as usize {
            return Err("WorkerLimits.git_output_bytes exceeds addressable representation".into());
        }
        if self.git_cache_identity_bytes > isize::MAX as usize {
            return Err(
                "WorkerLimits.git_cache_identity_bytes exceeds addressable representation".into(),
            );
        }
        if self.repository_identity_bytes > isize::MAX as usize {
            return Err(
                "WorkerLimits.repository_identity_bytes exceeds addressable representation".into(),
            );
        }
        if self.resource_profile_bytes > isize::MAX as usize {
            return Err(
                "WorkerLimits.resource_profile_bytes exceeds addressable representation".into(),
            );
        }
        if self.resource_proc_bytes > isize::MAX as usize {
            return Err(
                "WorkerLimits.resource_proc_bytes exceeds addressable representation".into(),
            );
        }
        if self.resource_lines > isize::MAX as usize {
            return Err("WorkerLimits.resource_lines exceeds addressable representation".into());
        }
        if self.resource_line_bytes > isize::MAX as usize {
            return Err(
                "WorkerLimits.resource_line_bytes exceeds addressable representation".into(),
            );
        }
        if self.resource_control_bytes > isize::MAX as usize {
            return Err(
                "WorkerLimits.resource_control_bytes exceeds addressable representation".into(),
            );
        }
        if self.resource_path_bytes > isize::MAX as usize {
            return Err(
                "WorkerLimits.resource_path_bytes exceeds addressable representation".into(),
            );
        }
        if self.credential_scan_units > isize::MAX as usize {
            return Err(
                "WorkerLimits.credential_scan_units exceeds addressable representation".into(),
            );
        }
        if self.credential_scan_depth > isize::MAX as usize {
            return Err(
                "WorkerLimits.credential_scan_depth exceeds addressable representation".into(),
            );
        }
        if self.native_tracking_bytes == 0 || self.controller_call_timeout_ms == 0 {
            return Err("worker tracking/controller bounds must be positive".into());
        }
        if self.max_total_capacity == 0 {
            return Err("WorkerLimits.max_total_capacity must be positive".into());
        }
        if self.identity_bytes == 0 {
            return Err("WorkerLimits.identity_bytes must be positive".into());
        }
        if self.process_record_bytes == 0 {
            return Err("WorkerLimits.process_record_bytes must be positive".into());
        }
        if self.process_registry_records == 0 {
            return Err("WorkerLimits.process_registry_records must be positive".into());
        }
        if self.tracked_descendants == 0 {
            return Err("WorkerLimits.tracked_descendants must be positive".into());
        }
        if self.process_census_entries == 0 {
            return Err("WorkerLimits.process_census_entries must be positive".into());
        }
        if self.identity_read_attempts == 0 {
            return Err("WorkerLimits.identity_read_attempts must be positive".into());
        }
        if self.identity_retry_us == 0 {
            return Err("WorkerLimits.identity_retry_us must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_micros(self.identity_retry_us))
            .is_none()
        {
            return Err("WorkerLimits.identity_retry_us exceeds this clock representation".into());
        }
        if self.owner_lock_timeout_ms == 0 {
            return Err("WorkerLimits.owner_lock_timeout_ms must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(self.owner_lock_timeout_ms))
            .is_none()
        {
            return Err(
                "WorkerLimits.owner_lock_timeout_ms exceeds this clock representation".into(),
            );
        }
        if self.owner_lock_retry_ms == 0 {
            return Err("WorkerLimits.owner_lock_retry_ms must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(self.owner_lock_retry_ms))
            .is_none()
        {
            return Err(
                "WorkerLimits.owner_lock_retry_ms exceeds this clock representation".into(),
            );
        }
        if self.spawn_ready_timeout_ms == 0 {
            return Err("WorkerLimits.spawn_ready_timeout_ms must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.spawn_ready_timeout_ms,
            ))
            .is_none()
        {
            return Err(
                "WorkerLimits.spawn_ready_timeout_ms exceeds this clock representation".into(),
            );
        }
        if self.spawn_marker_bytes == 0 {
            return Err("WorkerLimits.spawn_marker_bytes must be positive".into());
        }
        if self.process_stop_timeout_ms == 0 {
            return Err("WorkerLimits.process_stop_timeout_ms must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.process_stop_timeout_ms,
            ))
            .is_none()
        {
            return Err(
                "WorkerLimits.process_stop_timeout_ms exceeds this clock representation".into(),
            );
        }
        if self.stream_drain_timeout_ms == 0 {
            return Err("WorkerLimits.stream_drain_timeout_ms must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.stream_drain_timeout_ms,
            ))
            .is_none()
        {
            return Err(
                "WorkerLimits.stream_drain_timeout_ms exceeds this clock representation".into(),
            );
        }
        if self.recovery_checks == 0 {
            return Err("WorkerLimits.recovery_checks must be positive".into());
        }
        if self.recovery_interval_ms == 0 {
            return Err("WorkerLimits.recovery_interval_ms must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(self.recovery_interval_ms))
            .is_none()
        {
            return Err(
                "WorkerLimits.recovery_interval_ms exceeds this clock representation".into(),
            );
        }
        if self.shutdown_join_timeout_ms == 0 {
            return Err("WorkerLimits.shutdown_join_timeout_ms must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.shutdown_join_timeout_ms,
            ))
            .is_none()
        {
            return Err(
                "WorkerLimits.shutdown_join_timeout_ms exceeds this clock representation".into(),
            );
        }
        if self.shutdown_native_timeout_ms == 0 {
            return Err("WorkerLimits.shutdown_native_timeout_ms must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.shutdown_native_timeout_ms,
            ))
            .is_none()
        {
            return Err(
                "WorkerLimits.shutdown_native_timeout_ms exceeds this clock representation".into(),
            );
        }
        if self.shutdown_outbox_timeout_ms == 0 {
            return Err("WorkerLimits.shutdown_outbox_timeout_ms must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.shutdown_outbox_timeout_ms,
            ))
            .is_none()
        {
            return Err(
                "WorkerLimits.shutdown_outbox_timeout_ms exceeds this clock representation".into(),
            );
        }
        if self.reconnect_initial_ms == 0 {
            return Err("WorkerLimits.reconnect_initial_ms must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(self.reconnect_initial_ms))
            .is_none()
        {
            return Err(
                "WorkerLimits.reconnect_initial_ms exceeds this clock representation".into(),
            );
        }
        if self.reconnect_max_ms == 0 {
            return Err("WorkerLimits.reconnect_max_ms must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(self.reconnect_max_ms))
            .is_none()
        {
            return Err("WorkerLimits.reconnect_max_ms exceeds this clock representation".into());
        }
        if self.outbox_records == 0 {
            return Err("WorkerLimits.outbox_records must be positive".into());
        }
        if self.outbox_batch == 0 {
            return Err("WorkerLimits.outbox_batch must be positive".into());
        }
        if self.outbox_call_timeout_ms == 0 {
            return Err("WorkerLimits.outbox_call_timeout_ms must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.outbox_call_timeout_ms,
            ))
            .is_none()
        {
            return Err(
                "WorkerLimits.outbox_call_timeout_ms exceeds this clock representation".into(),
            );
        }
        if self.command_records == 0 {
            return Err("WorkerLimits.command_records must be positive".into());
        }
        if self.command_record_bytes == 0 {
            return Err("WorkerLimits.command_record_bytes must be positive".into());
        }
        if self.mutation_record_bytes == 0 {
            return Err("WorkerLimits.mutation_record_bytes must be positive".into());
        }
        if self.publication_record_bytes == 0 {
            return Err("WorkerLimits.publication_record_bytes must be positive".into());
        }
        if self.acceptance_call_timeout_ms == 0 {
            return Err("WorkerLimits.acceptance_call_timeout_ms must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.acceptance_call_timeout_ms,
            ))
            .is_none()
        {
            return Err(
                "WorkerLimits.acceptance_call_timeout_ms exceeds this clock representation".into(),
            );
        }
        if self.handoff_call_timeout_ms == 0 {
            return Err("WorkerLimits.handoff_call_timeout_ms must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.handoff_call_timeout_ms,
            ))
            .is_none()
        {
            return Err(
                "WorkerLimits.handoff_call_timeout_ms exceeds this clock representation".into(),
            );
        }
        if self.mutation_call_timeout_ms == 0 {
            return Err("WorkerLimits.mutation_call_timeout_ms must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.mutation_call_timeout_ms,
            ))
            .is_none()
        {
            return Err(
                "WorkerLimits.mutation_call_timeout_ms exceeds this clock representation".into(),
            );
        }
        if self.publication_call_timeout_ms == 0 {
            return Err("WorkerLimits.publication_call_timeout_ms must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.publication_call_timeout_ms,
            ))
            .is_none()
        {
            return Err(
                "WorkerLimits.publication_call_timeout_ms exceeds this clock representation".into(),
            );
        }
        if self.publication_recovery_timeout_ms == 0 {
            return Err("WorkerLimits.publication_recovery_timeout_ms must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.publication_recovery_timeout_ms,
            ))
            .is_none()
        {
            return Err(
                "WorkerLimits.publication_recovery_timeout_ms exceeds this clock representation"
                    .into(),
            );
        }
        if self.git_output_bytes == 0 {
            return Err("WorkerLimits.git_output_bytes must be positive".into());
        }
        if self.git_cache_identity_bytes == 0 {
            return Err("WorkerLimits.git_cache_identity_bytes must be positive".into());
        }
        if self.repository_identity_bytes == 0 {
            return Err("WorkerLimits.repository_identity_bytes must be positive".into());
        }
        if self.resource_profile_bytes == 0 {
            return Err("WorkerLimits.resource_profile_bytes must be positive".into());
        }
        if self.resource_proc_bytes == 0 {
            return Err("WorkerLimits.resource_proc_bytes must be positive".into());
        }
        if self.resource_lines == 0 {
            return Err("WorkerLimits.resource_lines must be positive".into());
        }
        if self.resource_line_bytes == 0 {
            return Err("WorkerLimits.resource_line_bytes must be positive".into());
        }
        if self.resource_control_bytes == 0 {
            return Err("WorkerLimits.resource_control_bytes must be positive".into());
        }
        if self.resource_path_bytes == 0 {
            return Err("WorkerLimits.resource_path_bytes must be positive".into());
        }
        if self.credential_scan_units == 0 {
            return Err("WorkerLimits.credential_scan_units must be positive".into());
        }
        if self.credential_scan_depth == 0 {
            return Err("WorkerLimits.credential_scan_depth must be positive".into());
        }
        if self.process_poll_ms == 0 {
            return Err("WorkerLimits.process_poll_ms must be positive".into());
        }
        if self.native_validation_capacity == 0 {
            return Err("WorkerLimits.native_validation_capacity must be positive".into());
        }
        if self.native_validation_poll_ms == 0 {
            return Err("WorkerLimits.native_validation_poll_ms must be positive".into());
        }
        if self.native_validation_cleanup_timeout_ms == 0 {
            return Err(
                "WorkerLimits.native_validation_cleanup_timeout_ms must be positive".into(),
            );
        }
        if self.native_observation_queue_records == 0 || self.native_observation_proc_bytes == 0 {
            return Err("native observation bounds must be positive".into());
        }
        if self.max_total_capacity > tokio::sync::Semaphore::MAX_PERMITS {
            return Err("worker completion queue capacity exceeds library representation".into());
        }
        if self.native_validation_capacity > tokio::sync::Semaphore::MAX_PERMITS {
            return Err("native validation capacity exceeds semaphore representation".into());
        }
        let total = self
            .coding_capacity
            .checked_add(self.native_capacity)
            .ok_or("worker capacity overflows this target")?;
        if total == 0 || total > self.max_total_capacity {
            return Err("worker capacity exceeds configured total".into());
        }
        if self.reconnect_initial_ms > self.reconnect_max_ms {
            return Err("worker reconnect interval ordering differs".into());
        }
        if self.owner_lock_retry_ms > self.owner_lock_timeout_ms {
            return Err("worker owner retry interval exceeds timeout".into());
        }
        if self.outbox_batch > self.outbox_records {
            return Err("worker outbox batch exceeds inventory bound".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CodingLimits {
    /// Absent historical policy disables platform outer-turn rate recovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit_retry: Option<crate::supplied_outer::RetryPolicy>,
    /// None retains the historical executor-selected window; bytes are not tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_context_window: Option<usize>,
    pub frame_bytes: usize,
    pub pending_notifications: usize,
    pub progress_queue_items: usize,
    pub progress_chars: usize,
    pub summary_chars: usize,
    pub summary_bytes: usize,
    pub stderr_bytes: usize,
    pub command_output_bytes: usize,
    pub preflight_timeout_ms: u64,
    pub preflight_output_bytes: usize,
    pub interrupt_timeout_ms: u64,
}
impl CodingLimits {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(policy) = &self.rate_limit_retry { policy.validate()?; }
        if self.model_context_window.is_some_and(|n| n == 0 || n > super::SELECTED_MODEL_CONTEXT_TOKENS) {
            return Err("coding.model_context_window exceeds the selected model supported context window".into());
        }
        if self.progress_queue_items > tokio::sync::Semaphore::MAX_PERMITS {
            return Err("coding progress queue exceeds library representation".into());
        }
        if self.frame_bytes > isize::MAX as usize {
            return Err("CodingLimits.frame_bytes exceeds addressable representation".into());
        }
        if self.pending_notifications > isize::MAX as usize {
            return Err(
                "CodingLimits.pending_notifications exceeds addressable representation".into(),
            );
        }
        if self.progress_queue_items > isize::MAX as usize {
            return Err(
                "CodingLimits.progress_queue_items exceeds addressable representation".into(),
            );
        }
        if self.progress_chars > isize::MAX as usize {
            return Err("CodingLimits.progress_chars exceeds addressable representation".into());
        }
        if self.summary_chars > isize::MAX as usize {
            return Err("CodingLimits.summary_chars exceeds addressable representation".into());
        }
        if self.summary_bytes > isize::MAX as usize {
            return Err("CodingLimits.summary_bytes exceeds addressable representation".into());
        }
        if self.stderr_bytes > isize::MAX as usize {
            return Err("CodingLimits.stderr_bytes exceeds addressable representation".into());
        }
        if self.command_output_bytes > isize::MAX as usize {
            return Err(
                "CodingLimits.command_output_bytes exceeds addressable representation".into(),
            );
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(self.preflight_timeout_ms))
            .is_none()
        {
            return Err("CodingLimits.preflight_timeout_ms exceeds clock representation".into());
        }
        if self.preflight_output_bytes > isize::MAX as usize {
            return Err(
                "CodingLimits.preflight_output_bytes exceeds addressable representation".into(),
            );
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(self.interrupt_timeout_ms))
            .is_none()
        {
            return Err("CodingLimits.interrupt_timeout_ms exceeds clock representation".into());
        }
        if self.frame_bytes == 0 {
            return Err("CodingLimits.frame_bytes must be positive".into());
        }
        if self.pending_notifications == 0 {
            return Err("CodingLimits.pending_notifications must be positive".into());
        }
        if self.progress_queue_items == 0 {
            return Err("CodingLimits.progress_queue_items must be positive".into());
        }
        if self.progress_chars == 0 {
            return Err("CodingLimits.progress_chars must be positive".into());
        }
        if self.summary_chars == 0 {
            return Err("CodingLimits.summary_chars must be positive".into());
        }
        if self.summary_bytes == 0 {
            return Err("CodingLimits.summary_bytes must be positive".into());
        }
        if self.stderr_bytes == 0 {
            return Err("CodingLimits.stderr_bytes must be positive".into());
        }
        if self.command_output_bytes == 0 {
            return Err("CodingLimits.command_output_bytes must be positive".into());
        }
        if self.preflight_timeout_ms == 0 {
            return Err("CodingLimits.preflight_timeout_ms must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(self.preflight_timeout_ms))
            .is_none()
        {
            return Err(
                "CodingLimits.preflight_timeout_ms exceeds this clock representation".into(),
            );
        }
        if self.preflight_output_bytes == 0 {
            return Err("CodingLimits.preflight_output_bytes must be positive".into());
        }
        if self.interrupt_timeout_ms == 0 {
            return Err("CodingLimits.interrupt_timeout_ms must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(self.interrupt_timeout_ms))
            .is_none()
        {
            return Err(
                "CodingLimits.interrupt_timeout_ms exceeds this clock representation".into(),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CollaborationLimits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<super::CollaborationCapabilities>,
    pub config_bytes: usize,
    pub frame_bytes: usize,
    pub channel_items: usize,
    pub journal_bytes: usize,
    pub complete_result_bytes: usize,
    pub startup_timeout_ms: u64,
    pub lookup_backoff_ms: u64,
    pub liveness_interval_ms: u64,
    pub gate_bytes: usize,
    pub boundary_bytes: usize,
}
impl CollaborationLimits {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(capabilities) = &self.capabilities { capabilities.validate()?; }
        if self.config_bytes > isize::MAX as usize {
            return Err(
                "CollaborationLimits.config_bytes exceeds addressable representation".into(),
            );
        }
        if self.frame_bytes > isize::MAX as usize {
            return Err(
                "CollaborationLimits.frame_bytes exceeds addressable representation".into(),
            );
        }
        if self.channel_items > isize::MAX as usize {
            return Err(
                "CollaborationLimits.channel_items exceeds addressable representation".into(),
            );
        }
        if self.journal_bytes > isize::MAX as usize {
            return Err(
                "CollaborationLimits.journal_bytes exceeds addressable representation".into(),
            );
        }
        if self.complete_result_bytes > isize::MAX as usize {
            return Err(
                "CollaborationLimits.complete_result_bytes exceeds addressable representation"
                    .into(),
            );
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(self.startup_timeout_ms))
            .is_none()
        {
            return Err(
                "CollaborationLimits.startup_timeout_ms exceeds clock representation".into(),
            );
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(self.lookup_backoff_ms))
            .is_none()
        {
            return Err(
                "CollaborationLimits.lookup_backoff_ms exceeds clock representation".into(),
            );
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(self.liveness_interval_ms))
            .is_none()
        {
            return Err(
                "CollaborationLimits.liveness_interval_ms exceeds clock representation".into(),
            );
        }
        if self.gate_bytes > isize::MAX as usize {
            return Err("CollaborationLimits.gate_bytes exceeds addressable representation".into());
        }
        if self.boundary_bytes > isize::MAX as usize {
            return Err(
                "CollaborationLimits.boundary_bytes exceeds addressable representation".into(),
            );
        }
        if self.config_bytes == 0 {
            return Err("CollaborationLimits.config_bytes must be positive".into());
        }
        if self.frame_bytes == 0 {
            return Err("CollaborationLimits.frame_bytes must be positive".into());
        }
        if self.channel_items == 0 {
            return Err("CollaborationLimits.channel_items must be positive".into());
        }
        if self.journal_bytes == 0 {
            return Err("CollaborationLimits.journal_bytes must be positive".into());
        }
        if self.complete_result_bytes == 0 {
            return Err("CollaborationLimits.complete_result_bytes must be positive".into());
        }
        if self.startup_timeout_ms == 0 {
            return Err("CollaborationLimits.startup_timeout_ms must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(self.startup_timeout_ms))
            .is_none()
        {
            return Err(
                "CollaborationLimits.startup_timeout_ms exceeds this clock representation".into(),
            );
        }
        if self.lookup_backoff_ms == 0 {
            return Err("CollaborationLimits.lookup_backoff_ms must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(self.lookup_backoff_ms))
            .is_none()
        {
            return Err(
                "CollaborationLimits.lookup_backoff_ms exceeds this clock representation".into(),
            );
        }
        if self.liveness_interval_ms == 0 {
            return Err("CollaborationLimits.liveness_interval_ms must be positive".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(self.liveness_interval_ms))
            .is_none()
        {
            return Err(
                "CollaborationLimits.liveness_interval_ms exceeds this clock representation".into(),
            );
        }
        if self.gate_bytes == 0 {
            return Err("CollaborationLimits.gate_bytes must be positive".into());
        }
        if self.boundary_bytes == 0 {
            return Err("CollaborationLimits.boundary_bytes must be positive".into());
        }
        if self.complete_result_bytes >= self.journal_bytes {
            return Err(
                "collaboration result reserve must leave room for the retained intent".into(),
            );
        }
        if self.boundary_bytes > self.frame_bytes || self.gate_bytes > self.frame_bytes {
            return Err("collaboration boundary exceeds configured frame".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MemoryNoticeLimits {
    pub keys: usize,
    pub notice_bytes: usize,
    pub mirror_bytes: usize,
    pub empty_prefix_pages: usize,
}
impl MemoryNoticeLimits {
    pub fn validate(&self) -> Result<(), String> {
        if self.keys > isize::MAX as usize {
            return Err("MemoryNoticeLimits.keys exceeds addressable representation".into());
        }
        if self.notice_bytes > isize::MAX as usize {
            return Err(
                "MemoryNoticeLimits.notice_bytes exceeds addressable representation".into(),
            );
        }
        if self.mirror_bytes > isize::MAX as usize {
            return Err(
                "MemoryNoticeLimits.mirror_bytes exceeds addressable representation".into(),
            );
        }
        if self.empty_prefix_pages > isize::MAX as usize {
            return Err(
                "MemoryNoticeLimits.empty_prefix_pages exceeds addressable representation".into(),
            );
        }
        if self.keys == 0 {
            return Err("MemoryNoticeLimits.keys must be positive".into());
        }
        if self.notice_bytes == 0 {
            return Err("MemoryNoticeLimits.notice_bytes must be positive".into());
        }
        if self.mirror_bytes == 0 {
            return Err("MemoryNoticeLimits.mirror_bytes must be positive".into());
        }
        if self.empty_prefix_pages == 0 {
            return Err("MemoryNoticeLimits.empty_prefix_pages must be positive".into());
        }
        if self.notice_bytes >= self.mirror_bytes {
            return Err("memory notice must leave room for retained mirror metadata".into());
        }
        Ok(())
    }
}
