//! Accepted native execution policy. Numeric defaults live in the metadata seed.
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeLimits {
    pub context_bytes: usize,
    pub input_bytes: usize,
    pub output_bytes: usize,
    pub result_bytes: usize,
    pub schema_bytes: usize,
    pub json_depth: usize,
    pub json_nodes: usize,
    pub schema_expansion: usize,
    pub validation_work: usize,
    pub regex_bytes: usize,
    pub regex_dfa_bytes: usize,
    pub request_bytes: usize,
    pub response_wire_bytes: usize,
    pub helper_value_bytes: usize,
    pub max_output_tokens: usize,
    pub function_count: usize,
    pub function_name_bytes: usize,
    pub function_description_bytes: usize,
    pub call_id_bytes: usize,
    pub function_calls_per_response: usize,
    pub memory_notice_bytes: usize,
    pub endpoint_bytes: usize,
    pub api_key_env_bytes: usize,
    pub api_key_bytes: usize,
    pub http_connect_timeout_ms: u64,
    pub error_body_bytes: usize,
    pub error_read_timeout_ms: u64,
    pub decode_check_bytes: usize,
    pub helper_error_bytes: usize,
    pub task_tool_observation_bytes: usize,
    pub summary_bytes: usize,
    pub decision_envelope_bytes: usize,
    pub continuation_envelope_bytes: usize,
    pub task_tool_identity_bytes: usize,
}
impl NativeLimits {
    /// Encoding capacity for an original parsed response, not another provider
    /// allowance. Strings/keys never grow beyond their JSON wire encoding;
    /// each finite serde_json number takes at most 24 canonical bytes. The
    /// accepted decoder counts keys and values against json_nodes. Raw HTTP
    /// bytes, nodes, depth and every enclosing storage bound stay unchanged.
    pub(crate) fn canonical_output_bytes(&self) -> Result<usize, String> {
        self.json_nodes.checked_mul(24)
            .and_then(|overhead| self.response_wire_bytes.checked_add(overhead))
            .filter(|bytes| *bytes <= isize::MAX as usize)
            .ok_or_else(|| "native canonical output extent exceeds addressable representation".into())
    }

    pub fn validate(&self) -> Result<(), String> {
        if self
            .json_depth
            .checked_add(2)
            .is_none_or(|n| n > super::SERDE_JSON_RECURSION_CAPACITY)
        {
            return Err(
                "native.json_depth exceeds supported complete-document parser capacity".into(),
            );
        }
        if self.context_bytes == 0 {
            return Err("NativeLimits.context_bytes must be positive".into());
        }
        if self.input_bytes == 0 {
            return Err("NativeLimits.input_bytes must be positive".into());
        }
        if self.output_bytes == 0 {
            return Err("NativeLimits.output_bytes must be positive".into());
        }
        if self.result_bytes == 0 {
            return Err("NativeLimits.result_bytes must be positive".into());
        }
        if self.schema_bytes == 0 {
            return Err("NativeLimits.schema_bytes must be positive".into());
        }
        if self.json_depth == 0 {
            return Err("NativeLimits.json_depth must be positive".into());
        }
        if self.json_nodes == 0 {
            return Err("NativeLimits.json_nodes must be positive".into());
        }
        if self.schema_expansion == 0 {
            return Err("NativeLimits.schema_expansion must be positive".into());
        }
        if self.validation_work == 0 {
            return Err("NativeLimits.validation_work must be positive".into());
        }
        if self.regex_bytes == 0 {
            return Err("NativeLimits.regex_bytes must be positive".into());
        }
        if self.regex_dfa_bytes == 0 {
            return Err("NativeLimits.regex_dfa_bytes must be positive".into());
        }
        if self.request_bytes == 0 {
            return Err("NativeLimits.request_bytes must be positive".into());
        }
        if self.response_wire_bytes == 0 {
            return Err("NativeLimits.response_wire_bytes must be positive".into());
        }
        if self.helper_value_bytes == 0 {
            return Err("NativeLimits.helper_value_bytes must be positive".into());
        }
        if self.max_output_tokens == 0 {
            return Err("NativeLimits.max_output_tokens must be positive".into());
        }
        if self.function_count == 0 {
            return Err("NativeLimits.function_count must be positive".into());
        }
        if self.function_name_bytes == 0 {
            return Err("NativeLimits.function_name_bytes must be positive".into());
        }
        if self.function_description_bytes == 0 {
            return Err("NativeLimits.function_description_bytes must be positive".into());
        }
        if self.call_id_bytes == 0 {
            return Err("NativeLimits.call_id_bytes must be positive".into());
        }
        if self.function_calls_per_response == 0 {
            return Err("NativeLimits.function_calls_per_response must be positive".into());
        }
        if self.memory_notice_bytes == 0 {
            return Err("NativeLimits.memory_notice_bytes must be positive".into());
        }
        if self.endpoint_bytes == 0 {
            return Err("NativeLimits.endpoint_bytes must be positive".into());
        }
        if self.api_key_env_bytes == 0 {
            return Err("NativeLimits.api_key_env_bytes must be positive".into());
        }
        if self.api_key_bytes == 0 {
            return Err("NativeLimits.api_key_bytes must be positive".into());
        }
        if self.http_connect_timeout_ms == 0 {
            return Err("NativeLimits.http_connect_timeout_ms must be positive".into());
        }
        if self.error_body_bytes == 0 {
            return Err("NativeLimits.error_body_bytes must be positive".into());
        }
        if self.error_read_timeout_ms == 0 {
            return Err("NativeLimits.error_read_timeout_ms must be positive".into());
        }
        if self.decode_check_bytes == 0 {
            return Err("NativeLimits.decode_check_bytes must be positive".into());
        }
        if self.helper_error_bytes == 0 {
            return Err("NativeLimits.helper_error_bytes must be positive".into());
        }
        if self.task_tool_observation_bytes == 0 {
            return Err("NativeLimits.task_tool_observation_bytes must be positive".into());
        }
        if self.summary_bytes == 0 {
            return Err("NativeLimits.summary_bytes must be positive".into());
        }
        if self.decision_envelope_bytes == 0 {
            return Err("NativeLimits.decision_envelope_bytes must be positive".into());
        }
        if self.continuation_envelope_bytes == 0 {
            return Err("NativeLimits.continuation_envelope_bytes must be positive".into());
        }
        if self.task_tool_identity_bytes == 0 {
            return Err("NativeLimits.task_tool_identity_bytes must be positive".into());
        }
        if self.context_bytes > u32::MAX as usize {
            return Err("NativeLimits.context_bytes exceeds helper frame representation".into());
        }
        if self.input_bytes > u32::MAX as usize {
            return Err("NativeLimits.input_bytes exceeds helper frame representation".into());
        }
        if self.output_bytes > u32::MAX as usize {
            return Err("NativeLimits.output_bytes exceeds helper frame representation".into());
        }
        if self.result_bytes > u32::MAX as usize {
            return Err("NativeLimits.result_bytes exceeds helper frame representation".into());
        }
        if self.schema_bytes > u32::MAX as usize {
            return Err("NativeLimits.schema_bytes exceeds helper frame representation".into());
        }
        if self.regex_bytes > u32::MAX as usize {
            return Err("NativeLimits.regex_bytes exceeds helper frame representation".into());
        }
        if self.regex_dfa_bytes > u32::MAX as usize {
            return Err("NativeLimits.regex_dfa_bytes exceeds helper frame representation".into());
        }
        if self.request_bytes > u32::MAX as usize {
            return Err("NativeLimits.request_bytes exceeds helper frame representation".into());
        }
        if self.response_wire_bytes > u32::MAX as usize {
            return Err(
                "NativeLimits.response_wire_bytes exceeds helper frame representation".into(),
            );
        }
        if self.helper_value_bytes > u32::MAX as usize {
            return Err(
                "NativeLimits.helper_value_bytes exceeds helper frame representation".into(),
            );
        }
        if self.function_name_bytes > u32::MAX as usize {
            return Err(
                "NativeLimits.function_name_bytes exceeds helper frame representation".into(),
            );
        }
        if self.function_description_bytes > u32::MAX as usize {
            return Err(
                "NativeLimits.function_description_bytes exceeds helper frame representation"
                    .into(),
            );
        }
        if self.call_id_bytes > u32::MAX as usize {
            return Err("NativeLimits.call_id_bytes exceeds helper frame representation".into());
        }
        if self.memory_notice_bytes > u32::MAX as usize {
            return Err(
                "NativeLimits.memory_notice_bytes exceeds helper frame representation".into(),
            );
        }
        if self.endpoint_bytes > u32::MAX as usize {
            return Err("NativeLimits.endpoint_bytes exceeds helper frame representation".into());
        }
        if self.api_key_env_bytes > u32::MAX as usize {
            return Err(
                "NativeLimits.api_key_env_bytes exceeds helper frame representation".into(),
            );
        }
        if self.api_key_bytes > u32::MAX as usize {
            return Err("NativeLimits.api_key_bytes exceeds helper frame representation".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                self.http_connect_timeout_ms,
            ))
            .is_none()
        {
            return Err("NativeLimits.http_connect_timeout_ms exceeds clock representation".into());
        }
        if self.error_body_bytes > u32::MAX as usize {
            return Err("NativeLimits.error_body_bytes exceeds helper frame representation".into());
        }
        if std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(self.error_read_timeout_ms))
            .is_none()
        {
            return Err("NativeLimits.error_read_timeout_ms exceeds clock representation".into());
        }
        if self.decode_check_bytes > u32::MAX as usize {
            return Err(
                "NativeLimits.decode_check_bytes exceeds helper frame representation".into(),
            );
        }
        if self.helper_error_bytes > u32::MAX as usize {
            return Err(
                "NativeLimits.helper_error_bytes exceeds helper frame representation".into(),
            );
        }
        if self.task_tool_observation_bytes > u32::MAX as usize {
            return Err(
                "NativeLimits.task_tool_observation_bytes exceeds helper frame representation"
                    .into(),
            );
        }
        if self.decision_envelope_bytes > u32::MAX as usize {
            return Err(
                "NativeLimits.decision_envelope_bytes exceeds helper frame representation".into(),
            );
        }
        if self.continuation_envelope_bytes > u32::MAX as usize {
            return Err(
                "NativeLimits.continuation_envelope_bytes exceeds helper frame representation"
                    .into(),
            );
        }
        if self.task_tool_identity_bytes > u32::MAX as usize {
            return Err(
                "NativeLimits.task_tool_identity_bytes exceeds helper frame representation".into(),
            );
        }
        if self.function_calls_per_response > u32::MAX as usize {
            return Err("native call count exceeds receipt representation".into());
        }
        if self
            .output_bytes
            .checked_add(self.decision_envelope_bytes)
            .is_none_or(|v| v > self.helper_value_bytes)
            || self.context_bytes > self.helper_value_bytes
        {
            return Err(
                "native helper value bound cannot carry configured output or context".into(),
            );
        }
        if self.helper_value_bytes > self.response_wire_bytes {
            return Err("native response wire bound cannot carry helper value".into());
        }
        Ok(())
    }
    pub(crate) const WIRE_BYTES: usize = 34 * std::mem::size_of::<u64>();
    pub(crate) fn wire(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::WIRE_BYTES);
        bytes.extend_from_slice(&(self.context_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.input_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.output_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.result_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.schema_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.json_depth as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.json_nodes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.schema_expansion as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.validation_work as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.regex_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.regex_dfa_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.request_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.response_wire_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.helper_value_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.max_output_tokens as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.function_count as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.function_name_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.function_description_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.call_id_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.function_calls_per_response as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.memory_notice_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.endpoint_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.api_key_env_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.api_key_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.http_connect_timeout_ms as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.error_body_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.error_read_timeout_ms as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.decode_check_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.helper_error_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.task_tool_observation_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.summary_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.decision_envelope_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.continuation_envelope_bytes as u64).to_be_bytes());
        bytes.extend_from_slice(&(self.task_tool_identity_bytes as u64).to_be_bytes());
        bytes
    }
    pub(crate) fn from_wire(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != Self::WIRE_BYTES {
            return Err("native_validation_policy_invalid".into());
        }
        let mut fields = bytes.chunks_exact(std::mem::size_of::<u64>());
        let mut next = || -> Result<u64, String> {
            Ok(u64::from_be_bytes(
                fields
                    .next()
                    .ok_or("native_validation_policy_invalid")?
                    .try_into()
                    .map_err(|_| "native_validation_policy_invalid")?,
            ))
        };
        let value = Self {
            context_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            input_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            output_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            result_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            schema_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            json_depth: usize::try_from(next()?).map_err(|_| "native_validation_policy_invalid")?,
            json_nodes: usize::try_from(next()?).map_err(|_| "native_validation_policy_invalid")?,
            schema_expansion: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            validation_work: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            regex_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            regex_dfa_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            request_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            response_wire_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            helper_value_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            max_output_tokens: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            function_count: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            function_name_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            function_description_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            call_id_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            function_calls_per_response: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            memory_notice_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            endpoint_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            api_key_env_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            api_key_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            http_connect_timeout_ms: next()?,
            error_body_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            error_read_timeout_ms: next()?,
            decode_check_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            helper_error_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            task_tool_observation_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            summary_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            decision_envelope_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            continuation_envelope_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
            task_tool_identity_bytes: usize::try_from(next()?)
                .map_err(|_| "native_validation_policy_invalid")?,
        };
        value.validate()?;
        Ok(value)
    }
}

#[cfg(test)]
mod canonical_output_extent_tests {
    #[test]
    fn canonical_extent_is_checked_and_counts_encoding_not_more_provider_bytes() {
        let mut p=crate::limits::RuntimeLimits::seed().unwrap().execution.native;
        p.response_wire_bytes=u32::MAX as usize;p.json_nodes=8192;
        assert_eq!(p.canonical_output_bytes().unwrap(),4_295_163_903);
        assert_eq!(p.canonical_output_bytes().unwrap().div_ceil(524_288),8193);
        assert!(p.canonical_output_bytes().unwrap()+1<=p.response_wire_bytes+25*p.json_nodes);
        assert_eq!(p.response_wire_bytes,u32::MAX as usize);
        p.json_nodes=usize::MAX;assert!(p.canonical_output_bytes().is_err());
        p.json_nodes=1;p.response_wire_bytes=usize::MAX;
        assert!(p.canonical_output_bytes().is_err());
    }
}
