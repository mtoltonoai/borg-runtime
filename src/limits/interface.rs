//! Explicit interface policy. All shipped numeric values live in metadata.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InterfaceLimits {
    pub mcp_call_bytes: usize,
    pub mcp_data_bytes: usize,
    pub mcp_caller_header_bytes: usize,
    pub protected_config_bytes: usize,
    pub protected_request_bytes: usize,
    pub protected_reply_bytes: usize,
    pub protected_identity_bytes: usize,
    pub protected_operation_id_bytes: usize,
    pub protected_socket_path_bytes: usize,
    pub protected_connections: usize,
    pub protected_exchange_timeout_ms: u64,
    pub protected_call_timeout_ms: u64,
    pub protected_git_stdout_bytes: usize,
    pub protected_git_timeout_ms: u64,
    pub protected_reflog_records: usize,
}

impl InterfaceLimits {
    pub fn validate(&self) -> Result<(), String> {
        if self.mcp_call_bytes == 0 || self.mcp_call_bytes > isize::MAX as usize {
            return Err("interface.mcp_call_bytes exceeds the positive addressable range".into());
        }
        if self.mcp_data_bytes == 0 || self.mcp_data_bytes > isize::MAX as usize {
            return Err("interface.mcp_data_bytes exceeds the positive addressable range".into());
        }
        if self.mcp_caller_header_bytes == 0 || self.mcp_caller_header_bytes > isize::MAX as usize {
            return Err(
                "interface.mcp_caller_header_bytes exceeds the positive addressable range".into(),
            );
        }
        if self.protected_config_bytes == 0 || self.protected_config_bytes > isize::MAX as usize {
            return Err(
                "interface.protected_config_bytes exceeds the positive addressable range".into(),
            );
        }
        if self.protected_request_bytes == 0 || self.protected_request_bytes > isize::MAX as usize {
            return Err(
                "interface.protected_request_bytes exceeds the positive addressable range".into(),
            );
        }
        if self.protected_reply_bytes == 0 || self.protected_reply_bytes > isize::MAX as usize {
            return Err(
                "interface.protected_reply_bytes exceeds the positive addressable range".into(),
            );
        }
        // This is the encoded size of the closed, fixed refusal vocabulary,
        // not a second policy cap or a bound derived from caller input.
        if self.protected_reply_bytes < crate::protected_release::minimum_limit_refusal_bytes()? {
            return Err(
                "interface.protected_reply_bytes cannot encode the fixed limit-refusal frame"
                    .into(),
            );
        }
        if self.protected_identity_bytes == 0 || self.protected_identity_bytes > isize::MAX as usize
        {
            return Err(
                "interface.protected_identity_bytes exceeds the positive addressable range".into(),
            );
        }
        if self.protected_operation_id_bytes == 0
            || self.protected_operation_id_bytes > isize::MAX as usize
        {
            return Err(
                "interface.protected_operation_id_bytes exceeds the positive addressable range"
                    .into(),
            );
        }
        if self.protected_socket_path_bytes == 0
            || self.protected_socket_path_bytes > isize::MAX as usize
        {
            return Err(
                "interface.protected_socket_path_bytes exceeds the positive addressable range"
                    .into(),
            );
        }
        if self.protected_connections == 0 || self.protected_connections > isize::MAX as usize {
            return Err(
                "interface.protected_connections exceeds the positive addressable range".into(),
            );
        }
        if self.protected_exchange_timeout_ms == 0
            || std::time::Instant::now()
                .checked_add(std::time::Duration::from_millis(
                    self.protected_exchange_timeout_ms,
                ))
                .is_none()
        {
            return Err(
                "interface.protected_exchange_timeout_ms exceeds the positive clock range".into(),
            );
        }
        if self.protected_call_timeout_ms == 0
            || std::time::Instant::now()
                .checked_add(std::time::Duration::from_millis(
                    self.protected_call_timeout_ms,
                ))
                .is_none()
        {
            return Err(
                "interface.protected_call_timeout_ms exceeds the positive clock range".into(),
            );
        }
        if self.protected_git_stdout_bytes == 0
            || self.protected_git_stdout_bytes > isize::MAX as usize
        {
            return Err(
                "interface.protected_git_stdout_bytes exceeds the positive addressable range"
                    .into(),
            );
        }
        if self.protected_git_timeout_ms == 0
            || std::time::Instant::now()
                .checked_add(std::time::Duration::from_millis(
                    self.protected_git_timeout_ms,
                ))
                .is_none()
        {
            return Err(
                "interface.protected_git_timeout_ms exceeds the positive clock range".into(),
            );
        }
        if self.protected_reflog_records == 0 || self.protected_reflog_records > isize::MAX as usize
        {
            return Err(
                "interface.protected_reflog_records exceeds the positive addressable range".into(),
            );
        }
        // These are representation constraints, not adjustable policy defaults.
        if self.protected_request_bytes > u32::MAX as usize
            || self.protected_reply_bytes > u32::MAX as usize
        {
            return Err("protected interface byte bounds exceed the u32 wire frame".into());
        }
        if self
            .protected_operation_id_bytes
            .checked_add("finish:".len())
            .is_none_or(|n| n > self.protected_identity_bytes)
        {
            return Err("protected completion identity cannot fit its configured bound".into());
        }
        if self.protected_connections > tokio::sync::Semaphore::MAX_PERMITS {
            return Err("protected connection bound exceeds semaphore representation".into());
        }
        if u64::try_from(self.protected_git_stdout_bytes)
            .ok()
            .and_then(|n| n.checked_add(1))
            .is_none()
        {
            return Err(
                "protected git observation bound cannot include its overflow sentinel".into(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn protected_reply_limit_must_fit_the_exact_fixed_refusal_frame() {
        let mut limits = crate::limits::RuntimeLimits::seed().unwrap().interface;
        let minimum = crate::protected_release::minimum_limit_refusal_bytes().unwrap();
        assert!(minimum > 0);
        limits.protected_reply_bytes = minimum - 1;
        assert!(
            limits
                .validate()
                .unwrap_err()
                .contains("interface.protected_reply_bytes")
        );
        limits.protected_reply_bytes = minimum;
        limits.validate().unwrap();
    }
}
