//! Versioned operational policy. Values are data, never hidden consumer defaults.
/// Selected gpt-6-astra context window: input, output and reasoning tokens together.
/// Source: https://developers.openai.com/api/docs/models/gpt-6-astra (2026-10-15).
pub const SELECTED_MODEL_CONTEXT_TOKENS: usize = 1_050_000;
mod charter;
pub use charter::CharterLimits;
mod controller;
mod interface;
mod native;
mod native_retry;
pub use native_retry::{NativeRetryLimits, NativeCircuitLimits};
mod product;
mod worker;
mod definition_updates;
mod retention;
mod workflow;
mod governance;
pub use governance::GovernanceLimits;
pub use workflow::WorkflowLimits;
pub use retention::{RetentionLimits, WorkerRetentionLimits};
pub use definition_updates::{DefinitionLimitField, DefinitionUpdatePolicy};
pub use controller::{ControllerLimits, StoreRetryLimits};
pub use interface::InterfaceLimits;
pub use native::NativeLimits;
pub use product::{ProductLimits, RepositoryLimits};
use serde::{Deserialize, Serialize};
use std::{io::Read, path::Path};
pub use worker::{CodingLimits, CollaborationLimits, MemoryNoticeLimits, WorkerLimits};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TransportLimits {
    pub body_bytes: usize,
    pub identity_bytes: usize,
    pub http_connections: usize,
    pub http_header_bytes: usize,
    pub http_header_count: usize,
    pub http_header_timeout_ms: u64,
    pub http_body_timeout_ms: u64,
    pub client_connect_timeout_ms: u64,
    pub client_timeout_ms: u64,
}
impl TransportLimits {
    pub fn validate(&self) -> Result<(), String> {
        for (name, n) in [
            ("body_bytes", self.body_bytes),
            ("identity_bytes", self.identity_bytes),
            ("http_connections", self.http_connections),
            ("http_header_bytes", self.http_header_bytes),
            ("http_header_count", self.http_header_count),
        ] {
            if n == 0 || n > isize::MAX as usize {
                return Err(format!(
                    "transport.{name} is outside the addressable positive range"
                ));
            }
        }
        for (name, n) in [
            ("http_header_timeout_ms", self.http_header_timeout_ms),
            ("http_body_timeout_ms", self.http_body_timeout_ms),
            ("client_connect_timeout_ms", self.client_connect_timeout_ms),
            ("client_timeout_ms", self.client_timeout_ms),
        ] {
            if n == 0
                || std::time::Instant::now()
                    .checked_add(std::time::Duration::from_millis(n))
                    .is_none()
            {
                return Err(format!("transport.{name} is outside the clock range"));
            }
        }
        if self.http_connections > tokio::sync::Semaphore::MAX_PERMITS {
            return Err("transport.http_connections exceeds semaphore representation".into());
        }
        if self.http_header_count > HTTP1_MAXIMUM_HEADER_COUNT {
            return Err("transport.http_header_count exceeds HTTP header representation".into());
        }
        if self.identity_bytes >= self.body_bytes {
            return Err("transport.identity_bytes cannot fill transport.body_bytes".into());
        }
        if self.client_connect_timeout_ms > self.client_timeout_ms {
            return Err("transport.client_connect_timeout_ms exceeds client_timeout_ms".into());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskDefaults {
    /// Root routing only; accepted tasks retain the selected explicit definition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    pub deadline_ms: u64,
    pub model_requests: u32,
    pub output_bytes: usize,
    pub result_bytes: usize,
    pub max_delegation_depth: u32,
}
impl TaskDefaults {
    pub fn at(&self, now: u64) -> Result<crate::protocol::Limits, String> {
        Ok(crate::protocol::Limits {
            deadline_unix_ms: now
                .checked_add(self.deadline_ms)
                .ok_or("task_defaults.deadline_ms overflows timestamp")?,
            model_requests: self.model_requests,
            output_bytes: self.output_bytes,
            result_bytes: self.result_bytes,
            max_delegation_depth: self.max_delegation_depth,
        })
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExecutionLimits {
    pub native: NativeLimits,
    pub coding: CodingLimits,
    pub collaboration: CollaborationLimits,
    pub memory_notice: MemoryNoticeLimits,
}
impl ExecutionLimits {
    pub fn validate(&self) -> Result<(), String> {
        self.native.validate()?;
        self.coding.validate()?;
        self.collaboration.validate()?;
        self.memory_notice.validate()?;
        if self.memory_notice.notice_bytes > self.native.context_bytes {
            return Err("memory_notice.notice_bytes exceeds native.context_bytes".into());
        }
        Ok(())
    }
}
/// Complete group overrides. Missing groups inherit the exact selected global revision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct ExecutionLimitOverrides {
    pub native: Option<NativeLimits>,
    pub coding: Option<CodingLimits>,
    pub collaboration: Option<CollaborationLimits>,
    pub memory_notice: Option<MemoryNoticeLimits>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeLimits {
    /// Historical absence neither selects nor enforces governance rules.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub governance: Option<GovernanceLimits>,
    /// Historical absence does not enable charter operations or synthesize a source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub charter: Option<CharterLimits>,
    /// Future accepted tasks only; historical absence enables no provider retry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_retry: Option<NativeRetryLimits>,
    /// Historical absence enables no new collaboration workflow operations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow: Option<WorkflowLimits>,
    /// Historical absence grants no automatic deletion authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<RetentionLimits>,
    /// Absent in historical metadata: no configuration writes are enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition_updates: Option<DefinitionUpdatePolicy>,
    pub runtime: RuntimeThreadLimits,
    pub constraints: RepresentationConstraints,
    pub schema_version: u32,
    pub revision: String,
    pub controller: ControllerLimits,
    pub transport: TransportLimits,
    pub worker: WorkerLimits,
    pub product: ProductLimits,
    pub repository: RepositoryLimits,
    pub interface: InterfaceLimits,
    pub execution: ExecutionLimits,
    pub task_defaults: TaskDefaults,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LimitSnapshot {
    pub schema_version: u32,
    pub global_revision: String,
    /// Exact selected source data, not a weak hash or a pointer to mutable defaults.
    pub global_metadata: Box<RuntimeLimits>,
    pub agent_revision: String,
    pub values: ExecutionLimits,
}
impl RuntimeLimits {
    /// Explicit shipped seed data. Maintained startup loads the selected metadata file.
    // Traceability scope: Selected byte framing and model-token defaults; actual load qualification is separate.
    //= spec/impl/v2/implementation.md#maximum-context-defaults
    //# Default context limits MUST use the maximum supported by the selected model and executor.
    pub fn seed() -> Result<Self, String> {
        Self::parse(include_bytes!("../../metadata/limits.json"))
    }
    pub fn parse(bytes: &[u8]) -> Result<Self, String> {
        let policy: Self =
            serde_json::from_slice(bytes).map_err(|e| format!("invalid limits metadata: {e}"))?;
        policy.validate()?;
        Ok(policy)
    }
    pub fn load(path: &Path) -> Result<Self, String> {
        let mut f = std::fs::File::open(path).map_err(|e| format!("limits metadata open: {e}"))?;
        let meta = f.metadata().map_err(|e| e.to_string())?;
        if !meta.is_file() {
            return Err("limits metadata must be a regular file".into());
        }
        let size =
            usize::try_from(meta.len()).map_err(|_| "limits metadata exceeds addressable input")?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| "cannot allocate limits metadata")?;
        f.by_ref()
            .take(meta.len())
            .read_to_end(&mut bytes)
            .map_err(|e| e.to_string())?;
        let mut extra = [0];
        if bytes.len() != size || f.read(&mut extra).map_err(|e| e.to_string())? != 0 {
            return Err("limits metadata changed during load".into());
        }
        Self::parse(&bytes)
    }
    //= spec/impl/v2/implementation.md#limits-are-versioned-metadata
    //# An invalid or inconsistent limit configuration MUST be reported without silently clamping its values.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != 1 {
            return Err("unsupported limits metadata schema".into());
        }
        self.transport.validate()?;
        if let Some(charter) = &self.charter {
            charter.validate(self)?;
        }
        if let Some(retry) = &self.native_retry { retry.validate()?; }
        if let Some(retention) = &self.retention {
            retention.validate()?;
        }
        self.runtime.validate()?;
        self.constraints.validate()?;
        if self.transport.http_header_bytes < self.constraints.http1_minimum_buffer_bytes {
            return Err(
                "transport.http_header_bytes below supported HTTP buffer representation".into(),
            );
        }
        if self.revision.is_empty()
            || self.revision.len() > self.transport.identity_bytes
            || self.revision.chars().any(char::is_control)
        {
            return Err("invalid limits metadata revision".into());
        }
        if let Some(policy) = &self.definition_updates {
            policy.validate(&self.transport)?;
        }
        self.controller.validate()?;
        self.worker.validate()?;
        self.product.validate()?;
        self.product.validate_response(&self.transport)?;
        self.repository.validate()?;
        self.interface.validate()?;
        self.execution.validate()?;
        if let Some(workflow) = &self.workflow {
            workflow.validate(self)?;
        }
        if let Some(governance) = &self.governance {
            governance.validate(self)?;
        }
        self.validate_execution_transport(&self.execution)?;
        if self.product.page_items > self.controller.max_list_tasks
            || self.product.page_items > self.controller.collaboration_page_items
        {
            return Err("product.page_items exceeds controller page capacity".into());
        }
        if self.interface.mcp_call_bytes > self.transport.body_bytes
            || self.interface.mcp_data_bytes > self.transport.body_bytes
            || self.interface.mcp_caller_header_bytes > self.transport.http_header_bytes
        {
            return Err("interface limits cannot fit the transport limits".into());
        }
        if self.task_defaults.agent_id.as_ref().is_some_and(|id| id.is_empty()
            || id.len() > self.transport.identity_bytes || id.chars().any(char::is_control)) {
            return Err("invalid task_defaults.agent_id: transport.identity_bytes".into());
        }
        if self.task_defaults.deadline_ms == 0
            || self.task_defaults.model_requests == 0
            || self.task_defaults.output_bytes == 0
            || self.task_defaults.result_bytes == 0
        {
            return Err("task_defaults must declare positive finite values".into());
        }
        if self.task_defaults.output_bytes > self.controller.max_task_output_bytes
            || self.task_defaults.result_bytes > self.controller.max_task_result_bytes
            || usize::try_from(self.task_defaults.max_delegation_depth).map_err(|_| "task_defaults.max_delegation_depth exceeds representation")? > self.controller.max_delegation_depth
        {
            return Err("task_defaults exceed controller task admission maxima".into());
        }
        if self.task_defaults.output_bytes > self.task_defaults.result_bytes
            || self.task_defaults.result_bytes >= self.transport.body_bytes
        {
            return Err("task_defaults output/result cannot fit transport.body_bytes".into());
        }
        Ok(())
    }
    // Traceability scope: Starts from explicit global execution metadata and replaces only agent override groups that are present.
    //= spec/impl/v2/implementation.md#limits-are-versioned-metadata
    //# Global metadata MUST provide explicit defaults for limits without a more specific configured value.
    fn effective(&self, input: &crate::protocol::TaskInput) -> Result<ExecutionLimits, String> {
        self.resolve_definition(&input.definition)
    }
    /// Complete validation for a future definition without inventing a task.
    pub fn resolve_definition(&self, definition: &crate::protocol::Definition) -> Result<ExecutionLimits, String> {
        let mut v = self.execution.clone();
        if let Some(o) = &definition.limit_overrides {
            if let Some(x) = &o.native {
                v.native = x.clone();
            }
            if let Some(x) = &o.coding {
                v.coding = x.clone();
            }
            if let Some(x) = &o.collaboration {
                v.collaboration = x.clone();
            }
            if let Some(x) = &o.memory_notice {
                v.memory_notice = x.clone();
            }
        }
        v.validate()?;
        self.validate_execution_transport(&v)?;
        Ok(v)
    }
    fn validate_execution_transport(&self, v: &ExecutionLimits) -> Result<(), String> {
        let minimum = crate::controller::minimum_collaboration_result_bytes(&self.transport)?;
        if v.collaboration.complete_result_bytes < minimum {
            return Err("collaboration.complete_result_bytes cannot retain a bounded refusal".into());
        }
        if v.collaboration.complete_result_bytes > self.transport.body_bytes {
            return Err("collaboration.complete_result_bytes exceeds transport.body_bytes".into());
        }
        if v.native.call_id_bytes > self.transport.identity_bytes {
            return Err("native.call_id_bytes exceeds transport.identity_bytes".into());
        }
        Ok(())
    }
    // Traceability scope: Both source revisions and the complete selected global metadata are retained in the task snapshot.
    //= spec/impl/v2/implementation.md#limits-are-versioned-metadata
    //# An accepted task MUST identify the metadata revisions used to resolve its execution limits.
    pub fn resolve_task(
        &self,
        input: &crate::protocol::TaskInput,
    ) -> Result<Box<LimitSnapshot>, String> {
        self.validate()?;
        Ok(Box::new(LimitSnapshot {
            schema_version: 1,
            global_revision: self.revision.clone(),
            global_metadata: Box::new(self.clone()),
            agent_revision: input.definition.revision.clone(),
            values: self.effective(input)?,
        }))
    }
}
impl LimitSnapshot {
    pub fn validate_for(&self, input: &crate::protocol::TaskInput) -> Result<(), String> {
        if self.schema_version != 1
            || self.global_revision != self.global_metadata.revision
            || self.agent_revision != input.definition.revision
        {
            return Err("execution limits revision binding mismatch".into());
        }
        self.global_metadata.validate()?;
        if self.values != self.global_metadata.effective(input)? {
            return Err("execution limits values differ from their recorded sources".into());
        }
        Ok(())
    }
}

/// Dependency representation facts, validated against the compiled mechanisms.
/// These describe supported encoding/library capacity rather than operator policy.
pub const SERDE_JSON_RECURSION_CAPACITY: usize = 128;
/// Hyper 1.11.1 reserves an http 1.5.0 HeaderMap for the received header count.
/// Its raw maximum is 32768 buckets with 3/4 usable capacity; reserve(24577)
/// exceeds that representation. This is a dependency fact, not selected policy.
pub const HTTP1_MAXIMUM_HEADER_COUNT: usize = 24576;
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
// Traceability scope: Representation invariants and explicitly unknown external constraints are separate from configurable policy groups.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//# The runtime MUST distinguish configured policy limits from non-configurable representation and external-service constraints.
pub struct RepresentationConstraints {
    pub serde_json_complete_document_containers: usize,
    pub http1_minimum_buffer_bytes: usize,
    pub http1_maximum_header_count: usize,
    pub native_helper_length_bits: u32,
    pub task_model_request_bits: u32,
    pub durable_unsigned_value_bits: u32,
    pub provider_model_context_tokens: Option<u64>,
    pub provider_requests_per_minute: Option<u64>,
    pub codex_inner_tool_calls: Option<u64>,
}
impl RepresentationConstraints {
    fn validate(&self) -> Result<(), String> {
        if self.serde_json_complete_document_containers != SERDE_JSON_RECURSION_CAPACITY - 1
            || self.http1_minimum_buffer_bytes != 8192
            || self.http1_maximum_header_count != HTTP1_MAXIMUM_HEADER_COUNT
            || self.native_helper_length_bits != u32::BITS
            || self.task_model_request_bits != u32::BITS
            || self.durable_unsigned_value_bits != i64::BITS - 1
        {
            return Err("metadata representation facts differ from this executable".into());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeThreadLimits {
    pub worker_threads: Option<usize>,
    pub blocking_threads: usize,
    pub thread_stack_bytes: Option<usize>,
}
impl RuntimeThreadLimits {
    fn validate(&self) -> Result<(), String> {
        if self.worker_threads == Some(0)
            || self.blocking_threads == 0
            || self.thread_stack_bytes == Some(0)
        {
            return Err(
                "runtime thread policy must be positive or explicit automatic selection".into(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn task() -> crate::protocol::TaskInput {
        let p = RuntimeLimits::seed().unwrap();
        crate::protocol::TaskInput { instruction_source: None,
            pull_request: None,
            resolved_limits: None,
            repository_read: None,
            instruction: "inspect".into(),
            definition: crate::protocol::Definition {
                charter: None,
                limit_overrides: None,
                execution_profile: None,
                inference: Some(crate::protocol::InferenceSettings::selected()),
                id: "agent-a".into(),
                revision: "rev-a".into(),
                executor: crate::protocol::ExecutorKind::Native,
            },
            repository: None,
            acceptance_commands: vec![],
            output_schema: Some(serde_json::json!({"type":"object"})),
            limits: p.task_defaults.at(1).unwrap(),
            dependencies: vec![],
            native_workflow: None,
        }
    }
    #[test]
    fn explicit_metadata_seed_validates() {
        RuntimeLimits::seed().unwrap().validate().unwrap();
    }
    #[test]
    // Traceability scope: No agent override: changed global defaults affect only a new resolution, while the retained task values remain exact.
    //= spec/impl/v2/implementation.md#limits-are-versioned-metadata
    //= type=test
    //# Global metadata MUST provide explicit defaults for limits without a more specific configured value.
    //= spec/impl/v2/implementation.md#limits-are-versioned-metadata
    //= type=test
    //# Changing default metadata MUST NOT silently change the limits of an already accepted task.
    fn task_keeps_exact_defaults_after_global_edit() {
        let mut old = RuntimeLimits::seed().unwrap();
        let mut input = task();
        input.resolved_limits = Some(old.resolve_task(&input).unwrap());
        let retained = input.execution_limits().unwrap().clone();
        old.revision = "rev-new".into();
        old.execution.native.context_bytes /= 2;
        assert_eq!(*input.execution_limits().unwrap(), retained);
        assert_ne!(old.resolve_task(&input).unwrap().values, retained);
    }
    #[test]
    fn agent_override_survives_exact_snapshot_binding() {
        let p = RuntimeLimits::seed().unwrap();
        let mut input = task();
        let mut n = p.execution.native.clone();
        n.function_calls_per_response = 3;
        input.definition.limit_overrides = Some(Box::new(ExecutionLimitOverrides {
            native: Some(n.clone()),
            ..Default::default()
        }));
        input.resolved_limits = Some(p.resolve_task(&input).unwrap());
        assert_eq!(input.execution_limits().unwrap().native, n);
        input
            .resolved_limits
            .as_mut()
            .unwrap()
            .values
            .native
            .function_calls_per_response = 4;
        assert!(input.execution_limits().is_err());
    }
    #[test]
    fn missing_unknown_and_duplicate_metadata_do_not_default() {
        let seed = include_bytes!("../../metadata/limits.json");
        let mut value: serde_json::Value = serde_json::from_slice(seed).unwrap();
        value.as_object_mut().unwrap().remove("transport");
        assert!(RuntimeLimits::parse(&serde_json::to_vec(&value).unwrap()).is_err());
        let mut value: serde_json::Value = serde_json::from_slice(seed).unwrap();
        value["typo"] = serde_json::json!(1);
        assert!(RuntimeLimits::parse(&serde_json::to_vec(&value).unwrap()).is_err());
        let raw = std::str::from_utf8(seed)
            .unwrap()
            .replacen("{", "{\"schema_version\":1,", 1);
        assert!(RuntimeLimits::parse(raw.as_bytes()).is_err());
    }
    #[test]
    fn cross_field_limit_cannot_be_silently_clamped() {
        let mut p = RuntimeLimits::seed().unwrap();
        p.interface.mcp_call_bytes = p.transport.body_bytes + 1;
        assert!(p.validate().unwrap_err().contains("interface"));
        let mut p = RuntimeLimits::seed().unwrap();
        p.transport.http_header_bytes = 1;
        assert!(p.validate().unwrap_err().contains("http_header_bytes"));
    }
    #[test]
    // Traceability scope: Checks the selected provider context fact separately from still-unknown request and executor-inner quotas; no byte/token conversion.
    //= spec/impl/v2/implementation.md#limits-are-versioned-metadata
    //= type=test
    //# The runtime MUST distinguish configured policy limits from non-configurable representation and external-service constraints.
    fn unknown_external_limits_stay_explicitly_unknown() {
        let p = RuntimeLimits::seed().unwrap();
        assert_eq!(p.constraints.provider_model_context_tokens, Some(SELECTED_MODEL_CONTEXT_TOKENS as u64));
        assert!(p.constraints.provider_requests_per_minute.is_none());
        assert!(p.constraints.codex_inner_tool_calls.is_none());
    }
    #[test]
    fn agent_call_ids_must_fit_their_retained_transport() {
        let p = RuntimeLimits::seed().unwrap();
        let mut input = task();
        let mut native = p.execution.native.clone();
        native.call_id_bytes = p.transport.identity_bytes + 1;
        input.definition.limit_overrides = Some(Box::new(ExecutionLimitOverrides {
            native: Some(native),
            ..Default::default()
        }));
        assert!(
            p.resolve_task(&input)
                .unwrap_err()
                .contains("native.call_id_bytes")
        );
        let mut global = p.clone();
        global.execution.native.call_id_bytes = global.transport.identity_bytes + 1;
        assert!(
            global
                .validate()
                .unwrap_err()
                .contains("native.call_id_bytes")
        );
    }
    #[test]
    fn browser_page_size_must_fit_both_controller_projections() {
        let mut p = RuntimeLimits::seed().unwrap();
        p.controller.max_list_tasks = p.product.page_items - 1;
        assert!(p.validate().unwrap_err().contains("product.page_items"));
        let mut p = RuntimeLimits::seed().unwrap();
        p.product.page_items = p.controller.collaboration_page_items + 1;
        p.controller.max_list_tasks = p.product.page_items;
        assert!(p.validate().unwrap_err().contains("product.page_items"));
    }
    #[test]
    fn accepted_result_bound_must_retain_a_refusal() {
        let mut p = RuntimeLimits::seed().unwrap();
        p.execution.collaboration.complete_result_bytes =
            crate::controller::minimum_collaboration_result_bytes(&p.transport).unwrap() - 1;
        assert!(p.validate().unwrap_err().contains("collaboration.complete_result_bytes"));
    }
    #[test]
    fn task_defaults_fit_controller_admission_before_startup() {
        for field in ["output", "result", "depth"] {
            let mut p = RuntimeLimits::seed().unwrap();
            match field {
                "output" => p.controller.max_task_output_bytes = p.task_defaults.output_bytes - 1,
                "result" => p.controller.max_task_result_bytes = p.task_defaults.result_bytes - 1,
                _ => p.task_defaults.max_delegation_depth = u32::try_from(p.controller.max_delegation_depth).unwrap() + 1,
            }
            assert!(p.validate().unwrap_err().contains("task_defaults exceed controller"));
        }
    }
    #[test]
    fn transport_concurrency_refuses_unrepresentable_semaphore_count() {
        let mut p = RuntimeLimits::seed().unwrap();
        p.transport.http_connections = tokio::sync::Semaphore::MAX_PERMITS + 1;
        assert!(p.validate().unwrap_err().contains("transport.http_connections"));
    }
    #[test]
    fn http_header_count_is_metadata_with_a_checked_dependency_representation() {
        // Check the pinned dependency itself, including its load-factor reserve
        // behavior, so an upgrade cannot silently invalidate the descriptor.
        let mut headers = hyper::HeaderMap::new();
        assert!(headers.try_reserve(HTTP1_MAXIMUM_HEADER_COUNT).is_ok());
        let mut too_many = hyper::HeaderMap::new();
        assert!(
            too_many
                .try_reserve(HTTP1_MAXIMUM_HEADER_COUNT + 1)
                .is_err()
        );

        let mut p = RuntimeLimits::seed().unwrap();
        let selected = p.transport.http_header_count;
        assert_eq!(
            p.constraints.http1_maximum_header_count,
            HTTP1_MAXIMUM_HEADER_COUNT
        );
        for count in [selected, HTTP1_MAXIMUM_HEADER_COUNT] {
            p.transport.http_header_count = count;
            p.validate().unwrap();
            assert_eq!(p.transport.http_header_count, count);
        }
        for count in [HTTP1_MAXIMUM_HEADER_COUNT + 1, isize::MAX as usize] {
            p.transport.http_header_count = count;
            assert!(
                p.validate()
                    .unwrap_err()
                    .contains("transport.http_header_count")
            );
        }
        p.transport.http_header_count = selected;
        p.constraints.http1_maximum_header_count -= 1;
        assert!(
            p.validate()
                .unwrap_err()
                .contains("metadata representation facts")
        );
    }
    #[test]
    fn metadata_attachments_are_indirect_and_keep_the_same_json_shape() {
        let p = RuntimeLimits::seed().unwrap();
        let mut input = task();
        let overrides = ExecutionLimitOverrides { native: Some(p.execution.native.clone()), ..Default::default() };
        input.definition.limit_overrides = Some(Box::new(overrides.clone()));
        input.resolved_limits = Some(p.resolve_task(&input).unwrap());
        let value = serde_json::to_value(&input).unwrap();
        assert_eq!(value["definition"]["limit_overrides"], serde_json::to_value(overrides).unwrap());
        assert_eq!(value["resolved_limits"], serde_json::to_value(input.resolved_limits.as_deref().unwrap()).unwrap());
        let decoded: crate::protocol::TaskInput = serde_json::from_value(value).unwrap();
        assert_eq!(decoded, input);
        assert_eq!(std::mem::size_of::<Option<Box<LimitSnapshot>>>(), std::mem::size_of::<usize>());
        assert_eq!(std::mem::size_of::<Option<Box<ExecutionLimitOverrides>>>(), std::mem::size_of::<usize>());
    }
}


#[cfg(test)]
mod maximum_context_tests {
    use super::*;
    //= spec/impl/v2/implementation.md#maximum-context-defaults
    //= type=test
    //# Default context limits MUST use the maximum supported by the selected model and executor.
    #[test]
    fn maximum_context_defaults_use_framing_and_model_units_separately() {
        let p=RuntimeLimits::seed().unwrap();
        p.validate().unwrap();
        let n=&p.execution.native;
        assert_eq!([n.context_bytes,n.request_bytes,n.helper_value_bytes,n.response_wire_bytes],[u32::MAX as usize;4]);
        assert_eq!(p.execution.coding.model_context_window,Some(SELECTED_MODEL_CONTEXT_TOKENS));
        assert_eq!(p.constraints.provider_model_context_tokens,Some(SELECTED_MODEL_CONTEXT_TOKENS as u64));
        assert_eq!(n.max_output_tokens,16_384);
        let mut wrong=n.clone();wrong.context_bytes=u32::MAX as usize+1;
        assert!(wrong.validate().unwrap_err().contains("context_bytes"));
        let mut coding=p.execution.coding.clone();coding.model_context_window=Some(SELECTED_MODEL_CONTEXT_TOKENS+1);
        assert!(coding.validate().unwrap_err().contains("model_context_window"));
        coding.model_context_window=Some(0);assert!(coding.validate().is_err());
    }
    #[test]
    fn absent_historical_coding_window_roundtrips_without_acquiring_default() {
        let p=RuntimeLimits::seed().unwrap();
        let mut prior=serde_json::to_value(&p.execution.coding).unwrap();
        prior.as_object_mut().unwrap().remove("model_context_window");
        let encoded=serde_json::to_vec(&prior).unwrap();
        let decoded:CodingLimits=serde_json::from_slice(&encoded).unwrap();
        decoded.validate().unwrap();
        assert_eq!(decoded.model_context_window,None);
        assert_eq!(serde_json::to_value(&decoded).unwrap(),prior);
        assert_eq!(serde_json::to_vec(&serde_json::to_value(&decoded).unwrap()).unwrap(),encoded);
        let retained=include_str!("../../tests/fixtures/definition-update-old-worker-limits.json");
        let value:serde_json::Value=serde_json::from_str(retained).unwrap();
        let metadata:RuntimeLimits=serde_json::from_str(retained).unwrap();
        metadata.validate().unwrap();
        assert_eq!(metadata.execution.coding.model_context_window,None);
        assert_eq!(serde_json::to_value(&metadata).unwrap(),value);
    }
}
