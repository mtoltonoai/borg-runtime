//! One bounded Responses step for a host-selected collaboration tool catalogue.
//! This adapter does not call tools or grant authority. The worker must reserve
//! each request and retain/reconcile the actual MCP result before continuing.
use super::*;
use crate::protocol::ExecutionProfile;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

use crate::limits::NativeLimits;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeFunction {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeWebSearch {
    /// Provider-hosted calls within one response, not model POSTs or MCP calls.
    pub max_tool_calls: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeLoopProfile {
    pub functions: Vec<NativeFunction>,
    pub web_search: Option<NativeWebSearch>,
}
impl NativeLoopProfile {
    /// Same pure carrier validation used before helper/provider dispatch.
    pub(crate) fn validate_selection(&self, input: &TaskInput) -> Result<(), String> {
        let policy = &input.execution_limits()?.native;
        selected(self, policy)?;
        let schema = input.output_schema.as_ref().ok_or("native_output_schema_missing")?;
        let validation = serde_json::to_value(ValidationInput {
            schema: schema.clone(), profile: self.clone(), output_bytes: input.limits.output_bytes,
            repair_enabled: input.resolved_limits.as_ref().is_some_and(|v| v.global_metadata.native_retry.is_some()),
            output_prefix: None, joined_charter: false,
        }).map_err(|_| "native_request_encoding")?;
        schema::measure(&validation, policy.schema_bytes, policy, "native_schema_bytes")?;
        Ok(())
    }
}

// Responses accepts a smaller generation-schema language than the accepted
// Draft202012 contract. Only the closed conditional-allOf form is projected;
// no structural constraint is removed. This is guidance, not an equivalent
// validator. ValidationInput and the durable TaskInput retain the exact schema,
// and validate_response still checks the full contract before acceptance.
fn response_format(schema: &Value) -> Result<Value, String> {
    let conditional = schema.get("type").and_then(Value::as_str) == Some("object")
        && schema
            .get("allOf")
            .and_then(Value::as_array)
            .is_some_and(|clauses| {
                !clauses.is_empty()
                    && clauses.iter().all(|clause| {
                        clause.as_object().is_some_and(|c| {
                            c.contains_key("if")
                                && c.contains_key("then")
                                && c.keys()
                                    .all(|key| matches!(key.as_str(), "if" | "then" | "else"))
                        })
                    })
            });
    if schema.get("allOf").is_some() && !conditional {
        return Err("native_provider_schema_projection_unsupported".into());
    }
    let mut guidance = schema.clone();
    if conditional {
        guidance.as_object_mut().unwrap().remove("allOf");
        explicit_scalar_types(&mut guidance)?;
    }
    Ok(
        json!({"type":"json_schema", "name":"borg_native_result", "strict":false, "schema":guidance}),
    )
}

// Equivalent scalar type spelling for enum/const leaves in the projected
// guidance. Visit schema positions only, never application property names or
// literal enum/const values. Mixed or nonscalar sets are left unchanged.
fn explicit_scalar_types(schema: &mut Value) -> Result<(), String> {
    fn scalar(value: &Value) -> Option<&'static str> {
        match value {
            Value::Null => Some("null"),
            Value::Bool(_) => Some("boolean"),
            Value::Number(_) => Some("number"),
            Value::String(_) => Some("string"),
            _ => None,
        }
    }
    let Some(object) = schema.as_object_mut() else {
        return Err("native_provider_schema_projection_unsupported".into());
    };
    // Closed schema positions only: an unhandled applicator could contain a
    // reference into the removed clauses. Reject it rather than produce a
    // dangling reference or silently strip more of the validation contract.
    if object.keys().any(|key| {
        !matches!(
            key.as_str(),
            "type"
                | "properties"
                | "required"
                | "additionalProperties"
                | "items"
                | "enum"
                | "const"
                | "description"
                | "title"
                | "minimum"
                | "maximum"
                | "exclusiveMinimum"
                | "exclusiveMaximum"
                | "multipleOf"
                | "minLength"
                | "maxLength"
                | "pattern"
                | "format"
                | "minItems"
                | "maxItems"
                | "anyOf"
        )
    }) {
        return Err("native_provider_schema_projection_unsupported".into());
    }
    if !object.contains_key("type") {
        let inferred = object.get("const").and_then(scalar).or_else(|| {
            let values = object.get("enum")?.as_array()?;
            let first = scalar(values.first()?)?;
            values
                .iter()
                .all(|value| scalar(value) == Some(first))
                .then_some(first)
        });
        if let Some(kind) = inferred {
            object.insert("type".into(), json!(kind));
        }
    }
    if let Some(children) = object.get_mut("properties").and_then(Value::as_object_mut) {
        for child in children.values_mut() {
            explicit_scalar_types(child)?;
        }
    }
    if let Some(child) = object.get_mut("items") {
        explicit_scalar_types(child)?;
    }
    if let Some(child) = object
        .get_mut("additionalProperties")
        .filter(|v| v.is_object())
    {
        explicit_scalar_types(child)?;
    }
    if let Some(children) = object.get_mut("anyOf").and_then(Value::as_array_mut) {
        for child in children {
            explicit_scalar_types(child)?;
        }
    }
    Ok(())
}

/// Private fields prevent a provider/model value from manufacturing retained
/// context. This is not a durable receipt: the worker owns that boundary.
#[derive(Clone)]
pub struct NativeLoopCall {
    pending: Vec<(String, String, Value)>,
    call_index: u32,
    task: TaskInput,
    profile: NativeLoopProfile,
    context: Value,
    request_index: u32,
    charged_through: u32,
    compacted: bool,
    call_id: String,
    name: String,
    arguments: Value,
}
impl NativeLoopCall {
    pub fn has_following_calls(&self) -> bool {
        !self.pending.is_empty()
    }
    pub fn call_index(&self) -> u32 {
        self.call_index
    }
    /// Advance an already validated response batch after its current durable
    /// tool receipt. This never starts a provider request or changes its count.
    pub fn next_call(&self, output: &str) -> Result<Option<Self>, String> {
        if self.pending.is_empty() {
            return Ok(None);
        }
        let policy = &self.task.execution_limits()?.native;
        let mut next = self.clone();
        let appended =
            json!({"type":"function_call_output", "call_id":self.call_id,"output":output});
        let old = schema::measure(
            &next.context,
            policy.context_bytes,
            policy,
            "native_context_bytes",
        )?
        .0;
        let additional = schema::measure(
            &appended,
            policy.context_bytes,
            policy,
            "native_context_bytes",
        )?
        .0;
        if old
            .checked_add(additional)
            .and_then(|v| v.checked_add(1))
            .is_none_or(|v| v > policy.context_bytes)
        {
            return Err("native_tool_context_bound_native_context_bytes".into());
        }
        next.context
            .as_array_mut()
            .ok_or("native_tool_context_invalid")?
            .push(appended);
        let (id, name, args) = next.pending.remove(0);
        next.call_id = id;
        next.name = name;
        next.arguments = args;
        next.call_index = self
            .call_index
            .checked_add(1)
            .ok_or("native_tool_call_count_native_function_calls_per_response")?;
        Ok(Some(next))
    }
    pub fn call_id(&self) -> &str {
        &self.call_id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn arguments(&self) -> &Value {
        &self.arguments
    }
    pub fn request_index(&self) -> u32 {
        self.request_index
    }
    // Kept separate from sending so the exact continuation append can be
    // exercised without a provider. The caller checks task/profile/call identity.
    fn tool_output_context(&self, output: &str, policy: &NativeLimits) -> Result<Value, String> {
        if output.len() > policy.context_bytes {
            return Err("native_tool_result_bound_native_context_bytes".into());
        }
        // Charge serialized framing before cloning/appending accumulated
        // context. JSON escaping is included, and nothing is truncated.
        // Traceability scope: After the caller verifies task/profile/call identity, this append preserves the corresponding tool output under the accepted context bound.
        //= spec/impl/v2/implementation.md#native-tool-profiles-and-continuations
        //# A native model continuation MUST use the recorded outcome of its corresponding tool call.
        let appended = json!({"type":"function_call_output","call_id":self.call_id,"output":output});
        let old = schema::measure(
            &self.context,
            policy.context_bytes,
            policy,
            "native_context_bytes",
        )?
        .0;
        let next = schema::measure(
            &appended,
            policy.context_bytes,
            policy,
            "native_context_bytes",
        )?
        .0;
        if old.saturating_add(next).saturating_add(1) > policy.context_bytes {
            return Err("native_tool_context_bound_native_context_bytes".into());
        }
        let mut context = self.context.clone();
        context
            .as_array_mut()
            .ok_or("native_tool_context_invalid")?
            .push(appended);
        Ok(context)
    }
    /// Render the exact completed tool result for a separately reserved
    /// compaction. Receipt/current-owner verification remains with the worker.
    pub fn compaction_input(&self, output: &str) -> Result<Value, String> {
        if !self.pending.is_empty() {
            return Err("native_compaction_calls_pending".into());
        }
        let policy = &self.task.execution_limits()?.native;
        let appended =
            json!({"type":"function_call_output","call_id":self.call_id,"output":output});
        let old = schema::measure(
            &self.context,
            policy.context_bytes,
            policy,
            "native_context_bytes",
        )?
        .0;
        let add = schema::measure(
            &appended,
            policy.context_bytes,
            policy,
            "native_context_bytes",
        )?
        .0;
        if old
            .checked_add(add)
            .and_then(|n| n.checked_add(1))
            .is_none_or(|n| n > policy.context_bytes)
        {
            return Err("native_compaction_context_bound_native_context_bytes".into());
        }
        let mut context = self.context.clone();
        context
            .as_array_mut()
            .ok_or("native_tool_context_invalid")?
            .push(appended);
        Ok(context)
    }
    /// Advance only accounting after the host has committed the original known
    /// rejection. This is not a reply, compaction, permission or retry decision.
    pub fn after_rejected_request(&self, request_index: u32) -> Result<Self, String> {
        if !self.pending.is_empty()
            || request_index
                != self
                    .charged_through
                    .checked_add(1)
                    .ok_or("native_request_overflow")?
            || request_index >= self.task.limits.model_requests
        {
            return Err("native_rejected_request_accounting_mismatch".into());
        }
        let mut next = self.clone();
        next.charged_through = request_index;
        Ok(next)
    }
    /// Called only after the canonical compaction reply is committed for the
    /// effective source. Preserve original call identity separately from the
    /// charged compaction request; the next request consumes the summary once.
    pub fn after_compaction(
        &self,
        request_index: u32,
        summary: Option<&str>,
    ) -> Result<Self, String> {
        let policy = &self.task.execution_limits()?.native;
        if !self.pending.is_empty()
            || request_index
                != self
                    .charged_through
                    .checked_add(1)
                    .ok_or("native_request_overflow")?
            || request_index >= self.task.limits.model_requests
            || summary.is_some_and(|v| v.is_empty() || v.len() > policy.summary_bytes)
        {
            return Err("native_compaction_continuation_mismatch".into());
        }
        let mut next = self.clone();
        if let Some(summary) = summary {
            let context = json!([{"role":"user","content":self.task.instruction},
                {"role":"developer","content":format!("Derived conversation summary; not charter authority: {summary}")}]);
            schema::measure(
                &context,
                policy.context_bytes,
                policy,
                "native_context_bytes",
            )?;
            next.context = context;
            next.compacted = true;
        }
        next.charged_through = request_index;
        Ok(next)
    }
    pub fn retained_items(&self) -> &Value {
        &self.context
    }
}

/// A validated final response retained for an explicitly admitted continuation.
/// It cannot be manufactured from a tool call or model-supplied identifier.
#[derive(Clone)]
pub struct NativeLoopFinal {
    task: TaskInput,
    profile: NativeLoopProfile,
    context: Value,
    output_items: Value,
    request_index: u32,
    charged_through: u32,
    compacted: bool,
}
impl NativeLoopFinal {
    pub fn request_index(&self) -> u32 {
        self.request_index
    }
    pub fn next_request_index(&self) -> Result<u32, String> {
        self.charged_through
            .checked_add(1)
            .ok_or_else(|| "native_request_overflow".into())
    }
    pub fn compaction_input(&self) -> Result<Value, String> {
        let policy = &self.task.execution_limits()?.native;
        let old = schema::measure(
            &self.context,
            policy.context_bytes,
            policy,
            "native_context_bytes",
        )?
        .0;
        let add = schema::measure(
            &self.output_items,
            policy.context_bytes,
            policy,
            "native_context_bytes",
        )?
        .0;
        if !self.compacted
            && old
                .checked_add(add)
                .is_none_or(|n| n > policy.context_bytes)
        {
            return Err("native_completion_context_bound_native_context_bytes".into());
        }
        let mut context = self.context.clone();
        if !self.compacted {
            context
                .as_array_mut()
                .ok_or("native_completion_context_invalid")?
                .extend(
                    self.output_items
                        .as_array()
                        .ok_or("native_completion_items_invalid")?
                        .iter()
                        .cloned(),
                );
        }
        schema::measure(
            &context,
            policy.context_bytes,
            policy,
            "native_context_bytes",
        )?;
        Ok(context)
    }
    /// Accounting-only transition after the exact original rejection receipt;
    /// preserve the actual final response index and all original context bytes.
    pub fn after_rejected_request(&self, request_index: u32) -> Result<Self, String> {
        if request_index != self.next_request_index()?
            || request_index >= self.task.limits.model_requests
        {
            return Err("native_rejected_request_accounting_mismatch".into());
        }
        let mut next = self.clone();
        next.charged_through = request_index;
        Ok(next)
    }
    /// The worker supplies only a committed canonical current-source summary.
    /// An obsolete compaction still advances accounting, retaining old context.
    pub fn after_compaction(
        &self,
        request_index: u32,
        summary: Option<&str>,
    ) -> Result<Self, String> {
        let policy = &self.task.execution_limits()?.native;
        if request_index != self.next_request_index()?
            || request_index >= self.task.limits.model_requests
            || summary.is_some_and(|v| v.is_empty() || v.len() > policy.summary_bytes)
        {
            return Err("native_compaction_continuation_mismatch".into());
        }
        let mut next = self.clone();
        if let Some(summary) = summary {
            next.context = json!([{"role":"user","content":self.task.instruction},
                {"role":"developer","content":format!("Derived conversation summary; not charter authority: {summary}")}]);
            schema::measure(
                &next.context,
                policy.context_bytes,
                policy,
                "native_context_bytes",
            )?;
            next.compacted = true;
        }
        next.charged_through = request_index;
        Ok(next)
    }
}

/// A completed final-text response that failed the declared format. This type
/// is created only after the trusted validation helper's full typed response.
/// Its private retained layout is never exposed as a validated final response.
#[derive(Clone)]
pub struct NativeLoopRepair {
    retained: NativeLoopFinal,
    issue: super::FormatIssue,
}
impl NativeLoopRepair {
    pub fn request_index(&self) -> u32 {
        self.retained.request_index
    }
    pub fn issue(&self) -> &super::FormatIssue {
        &self.issue
    }
    pub fn output_items(&self) -> &Value {
        &self.retained.output_items
    }
}

/// A complete provider terminal at the output cap, never a final result or a
/// dispatched call. Original items remain available for canonical binding.
#[derive(Clone)]
pub struct NativeLoopCap {
    retained: NativeLoopFinal,
    prefix: Option<String>,
}
impl NativeLoopCap {
    pub fn request_index(&self) -> u32 {
        self.retained.request_index
    }
    pub fn output_items(&self) -> &Value {
        &self.retained.output_items
    }
    fn next_context(&self) -> Result<NativeLoopFinal, String> {
        let policy = &self.retained.task.execution_limits()?.native;
        let mut next = self.retained.clone();
        let mut items = Vec::new();
        for item in self
            .retained
            .output_items
            .as_array()
            .ok_or("native_output_cap_items_invalid")?
        {
            if item.get("type").and_then(Value::as_str) == Some("function_call") {
                // These bytes were observed, but no function was dispatched.
                // Quoting the partial call as data never fabricates a result or
                // presents an incomplete function call as a completed input.
                let original =
                    serde_json::to_string(item).map_err(|_| "native_output_cap_encoding")?;
                items.push(json!({"role":"assistant","content":format!("Original undispatched output-cap function item: {original}")}));
            } else {
                items.push(item.clone());
            }
        }
        next.output_items = Value::Array(items);
        schema::measure(
            &next.output_items,
            policy.context_bytes,
            policy,
            "native_context_bytes",
        )?;
        Ok(next)
    }
}
const OUTPUT_CAP_FEEDBACK: &str = "The previous response stopped at its output cap. Continue from its retained partial output. No client function in the capped response was executed: issue any required functions as complete configured calls, never assume results. If final-answer text has already begun, emit only its remaining suffix; the host joins it to the exact retained prefix before validating the original task schema.";
fn joined_text(prefix: Option<&str>, text: &str, limit: usize) -> Result<String, String> {
    let bytes = prefix
        .map_or(0, str::len)
        .checked_add(text.len())
        .ok_or("native_output_cap_prefix_bound_task_output_bytes")?;
    if bytes > limit {
        return Err("native_output_cap_prefix_bound_task_output_bytes".into());
    }
    let mut joined = String::new();
    joined
        .try_reserve_exact(bytes)
        .map_err(|_| "native_output_cap_allocation_failed")?;
    if let Some(prefix) = prefix {
        joined.push_str(prefix);
    }
    joined.push_str(text);
    Ok(joined)
}
fn joined_limit(output: usize, charter: bool, policy: &NativeLimits) -> Result<usize, String> {
    if charter {
        output
            .checked_add(policy.decision_envelope_bytes)
            .map(|n| n.min(output.max(policy.context_bytes)))
            .ok_or_else(|| "native_output_cap_assembly_bound".into())
    } else {
        Ok(output)
    }
}
fn final_value(value: Value, joined_charter: bool) -> Value {
    if joined_charter
        && value.as_object().is_some_and(|o| {
            o.len() == 2 && o.contains_key("result") && o.contains_key("charter_references")
        })
    {
        value["result"].clone()
    } else {
        value
    }
}

/// Supplied by the host only after its actual tool result/receipt is retained.
/// Matching this ID proves association, not controller authority or persistence.
pub struct NativeLoopReply<'a> {
    pub call_id: &'a str,
    pub output: &'a str,
}

pub enum NativeLoopStep {
    OutputCap(NativeLoopCap),
    FormatRepair(NativeLoopRepair),
    Call(NativeLoopCall),
    Complete {
        result: NativeResult,
        /// Exact bounded provider items for host validation, including private
        /// reasoning. Public progress excludes reasoning items and retains
        /// citation observations without treating them as facts or authority.
        output_items: Value,
        continuation: NativeLoopFinal,
    },
}
pub struct NativeLoopObservation {
    pub result: Result<NativeLoopStep, String>,
    pub request_exposure: RequestExposure,
    pub usage: UsageObservation,
    pub provider_outcome: ProviderOutcome,
    pub diagnostic: Option<ProviderFailureDiagnostic>,
}

/// Executes one POST only. No hidden request retry, MCP invocation, Codex
/// fallback, compaction or provider-history recovery occurs here. Callers pass
/// the original task (whose allowance is already the remaining attempt budget)
/// and the identical selected profile at every step. `before_send` is the
/// worker's final current-owner check after durable request reservation.
pub async fn execute_loop_step<
    F: Future<Output = ()> + Send,
    G: Future<Output = Result<(), String>> + Send,
>(
    input: &TaskInput,
    config: &NativeConfig,
    profile: &NativeLoopProfile,
    prior: Option<(&NativeLoopCall, NativeLoopReply<'_>)>,
    host: &ValidationHost,
    before_send: G,
    cancelled: F,
) -> NativeLoopObservation {
    execute_loop_step_with_notice(
        input,
        config,
        profile,
        prior,
        None,
        host,
        before_send,
        cancelled,
    )
    .await
}

/// A host-only key/revision hint is appended to the already-required request.
/// It does not mutate TaskInput, tool results, the profile, or the request budget.
/// A successful response proves submission, not that the model used the hint.
pub async fn execute_loop_step_with_notice<
    F: Future<Output = ()> + Send,
    G: Future<Output = Result<(), String>> + Send,
>(
    input: &TaskInput,
    config: &NativeConfig,
    profile: &NativeLoopProfile,
    prior: Option<(&NativeLoopCall, NativeLoopReply<'_>)>,
    host_notice: Option<&str>,
    host: &ValidationHost,
    before_send: G,
    cancelled: F,
) -> NativeLoopObservation {
    execute_loop_step_with_host_inputs(
        input,
        config,
        profile,
        prior,
        host_notice,
        None,
        host,
        before_send,
        cancelled,
    )
    .await
}

/// Distinct workflow input obeys its accepted policy and the same aggregate context budget.
pub async fn execute_loop_step_with_host_inputs<
    F: Future<Output = ()> + Send,
    G: Future<Output = Result<(), String>> + Send,
>(
    input: &TaskInput,
    config: &NativeConfig,
    profile: &NativeLoopProfile,
    prior: Option<(&NativeLoopCall, NativeLoopReply<'_>)>,
    host_notice: Option<&str>,
    workflow_notice: Option<&str>,
    host: &ValidationHost,
    before_send: G,
    cancelled: F,
) -> NativeLoopObservation {
    execute_loop_step_with_host_inputs_at(
        input,
        config,
        profile,
        prior,
        host_notice,
        workflow_notice,
        None,
        host,
        before_send,
        cancelled,
    )
    .await
}

/// Physical index advances only after the worker records a confirmed rejection.
pub async fn execute_loop_step_with_host_inputs_at<
    F: Future<Output = ()> + Send,
    G: Future<Output = Result<(), String>> + Send,
>(
    input: &TaskInput,
    config: &NativeConfig,
    profile: &NativeLoopProfile,
    prior: Option<(&NativeLoopCall, NativeLoopReply<'_>)>,
    host_notice: Option<&str>,
    workflow_notice: Option<&str>,
    request_index: Option<u32>,
    host: &ValidationHost,
    before_send: G,
    cancelled: F,
) -> NativeLoopObservation {
    execute_loop_step_inner(
        input,
        config,
        profile,
        prior,
        None,
        None,
        None,
        host_notice,
        workflow_notice,
        request_index,
        host,
        before_send,
        cancelled,
    )
    .await
}

/// Genuine host feedback follows the actual final response in the same task
/// context. This spends one ordinary reserved request and never fabricates a
/// function call or function result.
pub async fn execute_loop_step_after_completion<
    F: Future<Output = ()> + Send,
    G: Future<Output = Result<(), String>> + Send,
>(
    input: &TaskInput,
    config: &NativeConfig,
    profile: &NativeLoopProfile,
    prior: &NativeLoopFinal,
    feedback: &str,
    host_notice: Option<&str>,
    workflow_notice: Option<&str>,
    host: &ValidationHost,
    before_send: G,
    cancelled: F,
) -> NativeLoopObservation {
    execute_loop_step_after_completion_at(
        input,
        config,
        profile,
        prior,
        feedback,
        host_notice,
        workflow_notice,
        None,
        host,
        before_send,
        cancelled,
    )
    .await
}

/// Physical index advances only after the worker records a confirmed rejection.
pub async fn execute_loop_step_after_completion_at<
    F: Future<Output = ()> + Send,
    G: Future<Output = Result<(), String>> + Send,
>(
    input: &TaskInput,
    config: &NativeConfig,
    profile: &NativeLoopProfile,
    prior: &NativeLoopFinal,
    feedback: &str,
    host_notice: Option<&str>,
    workflow_notice: Option<&str>,
    request_index: Option<u32>,
    host: &ValidationHost,
    before_send: G,
    cancelled: F,
) -> NativeLoopObservation {
    execute_loop_step_inner(
        input,
        config,
        profile,
        None,
        Some((prior, feedback)),
        None,
        None,
        host_notice,
        workflow_notice,
        request_index,
        host,
        before_send,
        cancelled,
    )
    .await
}

/// A new logical request follows the exact completed invalid response and
/// fixed, value-free validation feedback. This is never a same-body retry.
pub async fn execute_loop_step_after_format_at<
    F: Future<Output = ()> + Send,
    G: Future<Output = Result<(), String>> + Send,
>(
    input: &TaskInput,
    config: &NativeConfig,
    profile: &NativeLoopProfile,
    prior: &NativeLoopRepair,
    host_notice: Option<&str>,
    workflow_notice: Option<&str>,
    request_index: Option<u32>,
    host: &ValidationHost,
    before_send: G,
    cancelled: F,
) -> NativeLoopObservation {
    let feedback = prior.issue.feedback();
    execute_loop_step_inner(
        input,
        config,
        profile,
        None,
        Some((&prior.retained, &feedback)),
        None,
        None,
        host_notice,
        workflow_notice,
        request_index,
        host,
        before_send,
        cancelled,
    )
    .await
}

pub async fn execute_loop_step_after_output_cap_at<
    F: Future<Output = ()> + Send,
    G: Future<Output = Result<(), String>> + Send,
>(
    input: &TaskInput,
    config: &NativeConfig,
    profile: &NativeLoopProfile,
    prior: &NativeLoopCap,
    host_notice: Option<&str>,
    workflow_notice: Option<&str>,
    request_index: Option<u32>,
    host: &ValidationHost,
    before_send: G,
    cancelled: F,
) -> NativeLoopObservation {
    let retained = match prior.next_context() {
        Ok(v) => v,
        Err(error) => {
            return NativeLoopObservation {
                result: Err(error),
                request_exposure: RequestExposure::NotStarted,
                usage: UsageObservation::Unknown,
                provider_outcome: ProviderOutcome::NotStarted,
                diagnostic: Some(ProviderFailureDiagnostic::new(
                    ProviderFailureClass::LocalAdmission,
                )),
            };
        }
    };
    execute_loop_step_inner(
        input,
        config,
        profile,
        None,
        Some((&retained, OUTPUT_CAP_FEEDBACK)),
        prior.prefix.as_deref(),
        None,
        host_notice,
        workflow_notice,
        request_index,
        host,
        before_send,
        cancelled,
    )
    .await
}

/// First genuine input of an ordinarily admitted successor attempt. The
/// immutable task remains unchanged; recorded refusal is a distinct host input.
pub async fn execute_loop_step_with_completion_input<
    F: Future<Output = ()> + Send,
    G: Future<Output = Result<(), String>> + Send,
>(
    input: &TaskInput,
    config: &NativeConfig,
    profile: &NativeLoopProfile,
    feedback: &str,
    host_notice: Option<&str>,
    workflow_notice: Option<&str>,
    host: &ValidationHost,
    before_send: G,
    cancelled: F,
) -> NativeLoopObservation {
    execute_loop_step_with_completion_input_at(
        input,
        config,
        profile,
        feedback,
        host_notice,
        workflow_notice,
        None,
        host,
        before_send,
        cancelled,
    )
    .await
}

/// Physical index advances only after the worker records a confirmed rejection.
pub async fn execute_loop_step_with_completion_input_at<
    F: Future<Output = ()> + Send,
    G: Future<Output = Result<(), String>> + Send,
>(
    input: &TaskInput,
    config: &NativeConfig,
    profile: &NativeLoopProfile,
    feedback: &str,
    host_notice: Option<&str>,
    workflow_notice: Option<&str>,
    request_index: Option<u32>,
    host: &ValidationHost,
    before_send: G,
    cancelled: F,
) -> NativeLoopObservation {
    execute_loop_step_inner(
        input,
        config,
        profile,
        None,
        None,
        None,
        Some(feedback),
        host_notice,
        workflow_notice,
        request_index,
        host,
        before_send,
        cancelled,
    )
    .await
}

async fn execute_loop_step_inner<
    F: Future<Output = ()> + Send,
    G: Future<Output = Result<(), String>> + Send,
>(
    input: &TaskInput,
    config: &NativeConfig,
    profile: &NativeLoopProfile,
    prior: Option<(&NativeLoopCall, NativeLoopReply<'_>)>,
    completion: Option<(&NativeLoopFinal, &str)>,
    output_prefix: Option<&str>,
    initial_feedback: Option<&str>,
    host_notice: Option<&str>,
    workflow_notice: Option<&str>,
    request_index: Option<u32>,
    host: &ValidationHost,
    before_send: G,
    cancelled: F,
) -> NativeLoopObservation {
    let deadline = match super::deadline(input.limits.deadline_unix_ms) {
        Ok(value) => value,
        Err(error) => {
            return NativeLoopObservation {
                result: Err(error),
                request_exposure: RequestExposure::NotStarted,
                usage: UsageObservation::Unknown,
                provider_outcome: ProviderOutcome::NotStarted,
                diagnostic: Some(ProviderFailureDiagnostic::new(
                    ProviderFailureClass::Deadline,
                )),
            };
        }
    };
    let mut cpu = cpu::State::new(host);
    let mut exposure = RequestExposure::NotStarted;
    let mut interrupted = None;
    let mut result = {
        let work = before(
            input,
            config,
            profile,
            prior,
            completion,
            output_prefix,
            initial_feedback,
            host_notice,
            workflow_notice,
            request_index,
            host,
            before_send,
            deadline,
            &mut cpu,
            &mut exposure,
        );
        tokio::pin!(work, cancelled);
        tokio::select! { biased;
            _ = &mut cancelled => { interrupted = Some(ProviderFailureClass::Cancelled); Err("native_cancelled".into()) },
            _ = tokio::time::sleep_until(deadline) => { interrupted = Some(ProviderFailureClass::Deadline); Err("native_deadline_exceeded".into()) },
            result = &mut work => result,
        }
    };
    if let Some(class) = interrupted {
        cpu.record_failure(class);
    }
    if result.is_err() {
        cpu.record_result_failure(exposure, deadline);
    }
    if let Err(error) = cpu.finish(true).await {
        cpu.record_failure(ProviderFailureClass::ValidationCleanup);
        result = Err(error);
    }
    let diagnostic = cpu.finish_diagnostic(result.is_err(), exposure);
    NativeLoopObservation {
        result,
        diagnostic,
        request_exposure: exposure,
        usage: cpu.provider_usage(),
        provider_outcome: if exposure == RequestExposure::NotStarted {
            ProviderOutcome::NotStarted
        } else if let Some(rejection) = cpu.provider_rejection() {
            ProviderOutcome::Rejected(rejection)
        } else if cpu.provider_output_capped() {
            ProviderOutcome::OutputCapped
        } else if cpu.provider_completed() {
            ProviderOutcome::Completed
        } else {
            ProviderOutcome::Unknown
        },
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ValidationInput {
    pub schema: Value,
    pub profile: NativeLoopProfile,
    pub output_bytes: usize,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub repair_enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub joined_charter: bool,
}

fn valid_id(value: &str, policy: &NativeLimits) -> bool {
    !value.is_empty() && value.len() <= policy.call_id_bytes && !value.chars().any(char::is_control)
}
fn selected(profile: &NativeLoopProfile, policy: &NativeLimits) -> Result<(), String> {
    // The complete encoded catalogue shares the existing schema byte/node
    // bounds; count and string limits additionally make admission explicit.
    if profile.functions.len() > policy.function_count {
        return Err("native_tool_profile_bound_native_function_count".into());
    }
    if profile
        .web_search
        .as_ref()
        .is_some_and(|p| p.max_tool_calls == 0)
    {
        return Err("native_tool_profile_invalid".into());
    }
    let mut names = BTreeSet::new();
    for function in &profile.functions {
        if function.name.len() > policy.function_name_bytes {
            return Err("native_tool_profile_bound_native_function_name_bytes".into());
        }
        if function.description.len() > policy.function_description_bytes {
            return Err("native_tool_profile_bound_native_function_description_bytes".into());
        }
        if function.name.is_empty()
            || !function
                .name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
            || !names.insert(function.name.as_str())
            || function.parameters.get("type").and_then(Value::as_str) != Some("object")
        {
            return Err("native_tool_profile_invalid".into());
        }
    }
    crate::protocol::bounded_json_size(profile, policy.schema_bytes)
        .map_err(|_| "native_tool_profile_bound_native_schema_bytes")?;
    for function in &profile.functions {
        schema::measure(
            &function.parameters,
            policy.schema_bytes,
            policy,
            "native_schema_bytes",
        )?;
    }
    Ok(())
}

// Traceability scope: Native request and continuation mechanism: checks the remaining request allowance, deadline, selected profile and retained-context bounds. The worker owns durable reservations, repeated MCP dispatch and receipt provenance.
//= spec/impl/v2/implementation.md#native-tool-profiles-and-continuations
//# The native executor MUST support repeated configured tool calls within the task's finite execution bounds.
async fn before<G: Future<Output = Result<(), String>> + Send>(
    input: &TaskInput,
    config: &NativeConfig,
    profile: &NativeLoopProfile,
    prior: Option<(&NativeLoopCall, NativeLoopReply<'_>)>,
    completion: Option<(&NativeLoopFinal, &str)>,
    output_prefix: Option<&str>,
    initial_feedback: Option<&str>,
    host_notice: Option<&str>,
    workflow_notice: Option<&str>,
    request_index: Option<u32>,
    host: &ValidationHost,
    before_send: G,
    deadline: Instant,
    cpu: &mut cpu::State,
    exposure: &mut RequestExposure,
) -> Result<NativeLoopStep, String> {
    let policy = &input.execution_limits()?.native;
    if input.definition.charter.is_some() && host.charter.is_none() {
        return Err("native_charter_consumer_required".into());
    }
    policy.validate()?;
    if *policy != *host.native_limits {
        return Err("native_validation_policy_mismatch".into());
    }
    if input.definition.executor != ExecutorKind::Native {
        return Err("native_executor_required".into());
    }
    if input.repository.is_some() || !input.acceptance_commands.is_empty() {
        return Err("native_has_no_workspace_or_shell".into());
    }
    if input.instruction.is_empty() || input.instruction.len() > policy.context_bytes {
        return Err("native_instruction_bound_native_context_bytes".into());
    }
    if input.limits.output_bytes == 0 || input.limits.output_bytes > policy.output_bytes {
        return Err("native_output_limit_invalid_native_output_bytes".into());
    }
    if input.limits.result_bytes == 0 || input.limits.result_bytes > policy.result_bytes {
        return Err("native_result_limit_invalid_native_result_bytes".into());
    }
    let inference = input
        .definition
        .required_inference()
        .map_err(str::to_owned)?;
    input
        .definition
        .execution_profile
        .as_ref()
        .ok_or("template_profile_unbound")?;
    if config.model != inference.model {
        return Err("native_config_template_mismatch".into());
    }
    selected(profile, policy)?;
    crate::protocol::bounded_json_size(input, policy.input_bytes)
        .map_err(|_| "native_input_bound_native_input_bytes")?;
    let schema = input
        .output_schema
        .as_ref()
        .ok_or("native_output_schema_missing")?;
    schema::measure(schema, policy.schema_bytes, policy, "native_schema_bytes")?;
    let url = endpoint(&config.endpoint, policy)?;
    let (mut context, index) = match (prior, completion) {
        (None, None) => (json!([{"role":"user","content":input.instruction}]), 1),
        (Some(_), Some(_)) => return Err("native_continuation_kind_conflict".into()),
        (None, Some((final_response, feedback))) => {
            if final_response.task != *input
                || final_response.profile != *profile
                || feedback.is_empty()
                || feedback.len() > policy.context_bytes
            {
                return Err("native_completion_observation_mismatch".into());
            }
            let mut context = final_response.compaction_input()?;
            context
                .as_array_mut()
                .ok_or("native_completion_context_invalid")?
                .push(json!({"role":"developer","content":feedback}));
            schema::measure(
                &context,
                policy.context_bytes,
                policy,
                "native_context_bytes",
            )?;
            (context, final_response.next_request_index()?)
        }
        (Some((call, reply)), None) => {
            if call.task != *input
                || call.profile != *profile
                || reply.call_id != call.call_id
                || !call.pending.is_empty()
            {
                return Err("native_tool_observation_mismatch".into());
            }
            let index = call
                .charged_through
                .checked_add(1)
                .ok_or("native_request_budget_exhausted_task_model_requests")?;
            if call.compacted {
                schema::measure(
                    &call.context,
                    policy.context_bytes,
                    policy,
                    "native_context_bytes",
                )?;
                (call.context.clone(), index)
            } else {
                (call.tool_output_context(reply.output, policy)?, index)
            }
        }
    };
    if let Some(feedback) = initial_feedback {
        if index != 1 || feedback.is_empty() || feedback.len() > policy.context_bytes {
            return Err("native_initial_completion_input_mismatch".into());
        }
        context
            .as_array_mut()
            .ok_or("native_completion_context_invalid")?
            .push(json!({"role":"developer","content":feedback}));
    }
    let index = match request_index {
        Some(actual) if actual >= index => actual,
        Some(_) => return Err("native_physical_request_index_precedes_context".into()),
        None => index,
    };
    if index > input.limits.model_requests {
        return Err("native_request_budget_exhausted_task_model_requests".into());
    }
    // Retain the legacy argument/codec for historical data, but do not append
    // retired automatic memory hints to a newly admitted model request.
    if host_notice.is_some() {
        return Err("native_memory_notifications_disabled".into());
    }
    if let Some(notice) = workflow_notice {
        if !matches!(
            input.definition.execution_profile,
            Some(ExecutionProfile::CollaborationV2 { .. })
        ) {
            return Err("native_workflow_profile_not_configured".into());
        }
        let workflow = input
            .resolved_limits
            .as_ref()
            .and_then(|s| s.global_metadata.workflow.as_ref())
            .ok_or("native_workflow_delivery_disabled")?;
        if notice.is_empty() || notice.len() > workflow.notice_bytes {
            return Err("native_workflow_notice_bound_workflow_notice_bytes".into());
        }
        context
            .as_array_mut()
            .ok_or("native_tool_context_invalid")?
            .push(json!({"role":"developer","content":notice}));
    }
    schema::measure(
        &context,
        policy.context_bytes,
        policy,
        "native_context_bytes",
    )?;
    let validation = serde_json::to_value(ValidationInput {
        output_prefix: output_prefix.map(str::to_owned),
        joined_charter: output_prefix.is_some() && input.definition.charter.is_some(),
        schema: schema.clone(),
        profile: profile.clone(),
        output_bytes: input.limits.output_bytes,
        repair_enabled: input
            .resolved_limits
            .as_ref()
            .is_some_and(|v| v.global_metadata.native_retry.is_some()),
    })
    .map_err(|_| "native_request_encoding")?;
    schema::measure(
        &validation,
        policy.schema_bytes,
        policy,
        "native_schema_bytes",
    )?;
    let bound = input.limits.output_bytes.max(policy.context_bytes);
    let reply_bound = if input.limits.output_bytes >= policy.context_bytes {
        cpu::ReplyBound::TaskOutput
    } else {
        cpu::ReplyBound::NativeContext
    };
    let bound_field = reply_bound.field();
    cpu.run_loop(
        host,
        &validation,
        None,
        bound,
        reply_bound,
        input.limits.deadline_unix_ms,
    )
    .await?;
    let mut tools = profile
        .functions
        .iter()
        .map(|function| {
            json!({
                "type":"function", "name":function.name, "description":function.description,
                "parameters":function.parameters, "strict":false
            })
        })
        .collect::<Vec<_>>();
    if profile.web_search.is_some() {
        tools.push(json!({"type":"web_search"}));
    }
    // Traceability scope: The native wire request uses the accepted definition settings checked above; it does not select a worker default.
    //= spec/impl/v2/implementation.md#definitions-select-execution
    //# An execution attempt MUST use the inference settings of its accepted definition revision.
    let format = response_format(&schema)?;
    let mut body = json!({
        "model":inference.model,"reasoning":{"effort":inference.reasoning_effort}, "input":context, "store":false, "stream":false,
        "include":["reasoning.encrypted_content"], "max_output_tokens":policy.max_output_tokens,
        "tools":tools, "tool_choice":"auto", "parallel_tool_calls":policy.function_calls_per_response > 1,
        "text":{"format":format}
    });
    if let Some(search) = &profile.web_search {
        body["max_tool_calls"] = json!(search.max_tool_calls);
        body["include"]
            .as_array_mut()
            .unwrap()
            .push(json!("web_search_call.action.sources"));
    }
    before_send.await?;
    live(deadline)?;
    let (wire, key) =
        send_request_observed(url, config, &body, bound, deadline, exposure, host, cpu).await?;
    let encoded = cpu
        .run_loop(
            host,
            &validation,
            Some(&wire),
            bound,
            reply_bound,
            input.limits.deadline_unix_ms,
        )
        .await?
        .ok_or("native_validation_reply_invalid")?;
    let items = decode::parse(
        &encoded,
        bound,
        deadline,
        "native_tool_context_invalid",
        policy,
        bound_field,
    )?;
    if contains_credential(&items, &key) {
        return Err("native_output_contains_credential".into());
    }
    if cpu.provider_output_capped() {
        let found = shape_with_mode(&items, profile, deadline, policy, true)?;
        if found.refused
            || !input
                .resolved_limits
                .as_ref()
                .is_some_and(|v| v.global_metadata.native_retry.is_some())
        {
            return Err("native_output_cap_reply_invalid".into());
        }
        let prefix = match found.text {
            Some(text) => Some(joined_text(
                output_prefix,
                text,
                input.limits.output_bytes.max(policy.context_bytes),
            )?),
            None => output_prefix.map(str::to_owned),
        }
        .filter(|value| !value.is_empty());
        if prefix.as_ref().is_some_and(|value| value.contains(&key)) {
            return Err("native_output_contains_credential".into());
        }
        return Ok(NativeLoopStep::OutputCap(NativeLoopCap {
            retained: NativeLoopFinal {
                task: input.clone(),
                profile: profile.clone(),
                context,
                output_items: items,
                request_index: index,
                charged_through: index,
                compacted: false,
            },
            prefix,
        }));
    }
    let shape = shape(&items, profile, deadline, policy)?;
    if let Some(issue) = cpu.format_issue().cloned() {
        if !shape.calls.is_empty()
            || shape.refused
            || shape.text.is_none()
            || !input
                .resolved_limits
                .as_ref()
                .is_some_and(|v| v.global_metadata.native_retry.is_some())
        {
            return Err("native_format_reply_invalid".into());
        }
        return Ok(NativeLoopStep::FormatRepair(NativeLoopRepair {
            retained: NativeLoopFinal {
                task: input.clone(),
                profile: profile.clone(),
                context,
                output_items: items,
                request_index: index,
                charged_through: index,
                compacted: false,
            },
            issue,
        }));
    }
    if !shape.calls.is_empty() {
        let mut calls = Vec::new();
        for (id, name, args) in shape.calls {
            if context
                .as_array()
                .ok_or("native_tool_context_invalid")?
                .iter()
                .any(|item| {
                    item.get("type").and_then(Value::as_str) == Some("function_call")
                        && item.get("call_id").and_then(Value::as_str) == Some(id)
                })
            {
                return Err("native_tool_call_identity_invalid".into());
            }
            let arguments = decode::parse(
                args.as_bytes(),
                policy.context_bytes,
                deadline,
                "native_tool_arguments_invalid",
                policy,
                "native_context_bytes",
            )?;
            calls.push((id.to_owned(), name.to_owned(), arguments));
        }
        let old = schema::measure(
            &context,
            policy.context_bytes,
            policy,
            "native_context_bytes",
        )?
        .0;
        let addition =
            schema::measure(&items, policy.context_bytes, policy, "native_context_bytes")?.0;
        if old
            .checked_add(addition)
            .is_none_or(|v| v > policy.context_bytes)
        {
            return Err("native_tool_context_bound_native_context_bytes".into());
        }
        context
            .as_array_mut()
            .ok_or("native_tool_context_invalid")?
            .extend(
                items
                    .as_array()
                    .ok_or(ShapeFault::ContinuationItems.code())?
                    .iter()
                    .cloned(),
            );
        schema::measure(
            &context,
            policy.context_bytes,
            policy,
            "native_context_bytes",
        )?;
        let (call_id, name, arguments) = calls.remove(0);
        Ok(NativeLoopStep::Call(NativeLoopCall {
            task: input.clone(),
            profile: profile.clone(),
            context,
            request_index: index,
            charged_through: index,
            compacted: false,
            call_id,
            name,
            arguments,
            pending: calls,
            call_index: 1,
        }))
    } else {
        let raw_limit = joined_limit(
            input.limits.output_bytes,
            output_prefix.is_some() && input.definition.charter.is_some(),
            policy,
        )?;
        let text = joined_text(
            output_prefix,
            shape.text.ok_or(ShapeFault::FinalTextMissing.code())?,
            raw_limit,
        )?;
        let output = decode::parse(
            text.as_bytes(),
            raw_limit,
            deadline,
            "native_output_not_json",
            policy,
            "task_output_bytes",
        )?;
        if contains_credential(&output, &key) {
            return Err("native_output_contains_credential".into());
        }
        Ok(NativeLoopStep::Complete {
            result: NativeResult {
                summary: "Native output validated against the task schema".into(),
                output: final_value(
                    output,
                    output_prefix.is_some() && input.definition.charter.is_some(),
                ),
                requests: 1,
            },
            continuation: NativeLoopFinal {
                task: input.clone(),
                profile: profile.clone(),
                context,
                output_items: items.clone(),
                request_index: index,
                charged_through: index,
                compacted: false,
            },
            output_items: items,
        })
    }
}

struct Shape<'a> {
    calls: Vec<(&'a str, &'a str, &'a str)>,
    text: Option<&'a str>,
    refused: bool,
}
// Closed structural diagnostics: values, unknown field names, provider text,
// identifiers and credentials never become part of an error code.
#[derive(Clone, Copy)]
enum ShapeFault {
    AnnotationEnd,
    AnnotationFields,
    AnnotationRange,
    AnnotationStart,
    AnnotationTitle,
    AnnotationType,
    AnnotationUrl,
    Annotations,
    CallsWithRefusal,
    ContentType,
    ContinuationItems,
    FinalMessageCount,
    FinalTextMissing,
    FunctionArguments,
    FunctionCallId,
    FunctionFields,
    FunctionName,
    ItemIdInvalidOrDuplicate,
    ItemIdType,
    ItemType,
    MessageContent,
    MessageContentCount,
    MessageFields,
    MessagePhase,
    MessageRole,
    OutputArray,
    OutputMissing,
    ReasoningContent,
    ReasoningContentFields,
    ReasoningContentText,
    ReasoningContentType,
    ReasoningEncryptedContent,
    ReasoningFields,
    ReasoningSummary,
    ReasoningSummaryFields,
    ReasoningSummaryText,
    ReasoningSummaryType,
    RefusalFields,
    RefusalText,
    SearchActionFields,
    SearchActionMissing,
    SearchActionText,
    SearchActionType,
    SearchDomains,
    SearchFields,
    SearchId,
    SearchIdInvalid,
    SearchQueries,
    SearchSourceFields,
    SearchSourceType,
    SearchSourceUrl,
    SearchSources,
    TerminalMissing,
    TextFields,
    TextLogprobs,
    TextValue,
    UnsupportedContentType,
}
impl ShapeFault {
    fn code(self) -> &'static str {
        match self {
            Self::AnnotationEnd => "native_response_shape_annotation_end",
            Self::AnnotationFields => "native_response_shape_annotation_fields",
            Self::AnnotationRange => "native_response_shape_annotation_range",
            Self::AnnotationStart => "native_response_shape_annotation_start",
            Self::AnnotationTitle => "native_response_shape_annotation_title",
            Self::AnnotationType => "native_response_shape_annotation_type",
            Self::AnnotationUrl => "native_response_shape_annotation_url",
            Self::Annotations => "native_response_shape_annotations",
            Self::CallsWithRefusal => "native_response_shape_calls_with_refusal",
            Self::ContentType => "native_response_shape_content_type",
            Self::ContinuationItems => "native_response_shape_continuation_items",
            Self::FinalMessageCount => "native_response_shape_final_message_count",
            Self::FinalTextMissing => "native_response_shape_final_text_missing",
            Self::FunctionArguments => "native_response_shape_function_arguments",
            Self::FunctionCallId => "native_response_shape_function_call_id",
            Self::FunctionFields => "native_response_shape_function_fields",
            Self::FunctionName => "native_response_shape_function_name",
            Self::ItemIdInvalidOrDuplicate => "native_response_shape_item_id_invalid_or_duplicate",
            Self::ItemIdType => "native_response_shape_item_id_type",
            Self::ItemType => "native_response_shape_item_type",
            Self::MessageContent => "native_response_shape_message_content",
            Self::MessageContentCount => "native_response_shape_message_content_count",
            Self::MessageFields => "native_response_shape_message_fields",
            Self::MessagePhase => "native_response_shape_message_phase",
            Self::MessageRole => "native_response_shape_message_role",
            Self::OutputArray => "native_response_shape_output_array",
            Self::OutputMissing => "native_response_shape_output_missing",
            Self::ReasoningContent => "native_response_shape_reasoning_content",
            Self::ReasoningContentFields => "native_response_shape_reasoning_content_fields",
            Self::ReasoningContentText => "native_response_shape_reasoning_content_text",
            Self::ReasoningContentType => "native_response_shape_reasoning_content_type",
            Self::ReasoningEncryptedContent => "native_response_shape_reasoning_encrypted_content",
            Self::ReasoningFields => "native_response_shape_reasoning_fields",
            Self::ReasoningSummary => "native_response_shape_reasoning_summary",
            Self::ReasoningSummaryFields => "native_response_shape_reasoning_summary_fields",
            Self::ReasoningSummaryText => "native_response_shape_reasoning_summary_text",
            Self::ReasoningSummaryType => "native_response_shape_reasoning_summary_type",
            Self::RefusalFields => "native_response_shape_refusal_fields",
            Self::RefusalText => "native_response_shape_refusal_text",
            Self::SearchActionFields => "native_response_shape_search_action_fields",
            Self::SearchActionMissing => "native_response_shape_search_action_missing",
            Self::SearchActionText => "native_response_shape_search_action_text",
            Self::SearchActionType => "native_response_shape_search_action_type",
            Self::SearchDomains => "native_response_shape_search_domains",
            Self::SearchFields => "native_response_shape_search_fields",
            Self::SearchId => "native_response_shape_search_id",
            Self::SearchIdInvalid => "native_response_shape_search_id_invalid",
            Self::SearchQueries => "native_response_shape_search_queries",
            Self::SearchSourceFields => "native_response_shape_search_source_fields",
            Self::SearchSourceType => "native_response_shape_search_source_type",
            Self::SearchSourceUrl => "native_response_shape_search_source_url",
            Self::SearchSources => "native_response_shape_search_sources",
            Self::TerminalMissing => "native_response_shape_terminal_missing",
            Self::TextFields => "native_response_shape_text_fields",
            Self::TextLogprobs => "native_response_shape_text_logprobs",
            Self::TextValue => "native_response_shape_text_value",
            Self::UnsupportedContentType => "native_response_shape_unsupported_content_type",
        }
    }
}

fn keys(value: &Value, allowed: &[&str], fault: ShapeFault) -> Result<(), String> {
    if value
        .as_object()
        .is_none_or(|o| o.keys().any(|key| !allowed.contains(&key.as_str())))
    {
        return Err(fault.code().into());
    }
    Ok(())
}
fn text_field<'a>(value: &'a Value, key: &str, fault: ShapeFault) -> Result<&'a str, String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| fault.code().into())
}
fn optional_strings(value: &Value, key: &str, fault: ShapeFault) -> Result<(), String> {
    if value.get(key).is_some_and(|v| {
        !v.is_null()
            && v.as_array()
                .is_none_or(|a| a.iter().any(|v| !v.is_string()))
    }) {
        return Err(fault.code().into());
    }
    Ok(())
}
fn search_action(action: &Value) -> Result<(), String> {
    let fields: &[&str] = match text_field(action, "type", ShapeFault::SearchActionType)? {
        "search" => &["type", "query", "queries", "domains", "sources"],
        "open_page" => &["type", "url"],
        "find_in_page" => &["type", "url", "pattern"],
        _ => return Err("native_tool_unconfigured".into()),
    };
    keys(action, fields, ShapeFault::SearchActionFields)?;
    for field in ["query", "url", "pattern"] {
        if action
            .get(field)
            .is_some_and(|v| !v.is_null() && !v.is_string())
        {
            return Err(ShapeFault::SearchActionText.code().into());
        }
    }
    optional_strings(action, "queries", ShapeFault::SearchQueries)?;
    optional_strings(action, "domains", ShapeFault::SearchDomains)?;
    if let Some(sources) = action.get("sources").filter(|v| !v.is_null()) {
        for source in sources.as_array().ok_or(ShapeFault::SearchSources.code())? {
            keys(source, &["type", "url"], ShapeFault::SearchSourceFields)?;
            if source.get("type").and_then(Value::as_str) != Some("url") {
                return Err(ShapeFault::SearchSourceType.code().into());
            }
            text_field(source, "url", ShapeFault::SearchSourceUrl)?;
        }
    }
    Ok(())
}
fn annotations(part: &Value) -> Result<(), String> {
    // Positions are Unicode scalar character offsets in the original text,
    // not Rust UTF-8 byte offsets. No annotation rewrites or text slicing.
    let characters = text_field(part, "text", ShapeFault::TextValue)?
        .chars()
        .count() as u64;
    if let Some(annotations) = part.get("annotations") {
        for annotation in annotations
            .as_array()
            .ok_or(ShapeFault::Annotations.code())?
        {
            keys(
                annotation,
                &["type", "url", "title", "start_index", "end_index"],
                ShapeFault::AnnotationFields,
            )?;
            if annotation.get("type").and_then(Value::as_str) != Some("url_citation") {
                return Err(ShapeFault::AnnotationType.code().into());
            }
            text_field(annotation, "url", ShapeFault::AnnotationUrl)?;
            text_field(annotation, "title", ShapeFault::AnnotationTitle)?;
            let start = annotation
                .get("start_index")
                .and_then(Value::as_u64)
                .ok_or(ShapeFault::AnnotationStart.code())?;
            let end = annotation
                .get("end_index")
                .and_then(Value::as_u64)
                .ok_or(ShapeFault::AnnotationEnd.code())?;
            if start > end || end > characters {
                return Err(ShapeFault::AnnotationRange.code().into());
            }
        }
    }
    Ok(())
}

// Traceability scope: Response validation rejects functions and hosted search absent from the host-selected profile. The worker derives that profile from the accepted task; this primitive does not independently authorize a profile.
//= spec/impl/v2/implementation.md#native-tool-profiles-and-continuations
//# The native executor MUST refuse a provider tool that is absent from the task's accepted configuration.
fn shape_with_mode<'a>(
    items: &'a Value,
    profile: &NativeLoopProfile,
    deadline: Instant,
    policy: &NativeLimits,
    capped: bool,
) -> Result<Shape<'a>, String> {
    let mut found = Shape {
        calls: Vec::new(),
        text: None,
        refused: false,
    };
    let mut final_messages = 0;
    let mut searches = 0u32;
    let mut ids = BTreeSet::new();
    for item in items.as_array().ok_or(ShapeFault::OutputArray.code())? {
        live(deadline)?;
        if let Some(id) = item.get("id") {
            let id = id.as_str().ok_or(ShapeFault::ItemIdType.code())?;
            if id.len() > policy.call_id_bytes {
                return Err("native_tool_call_identity_invalid_native_call_id_bytes".into());
            }
            if !valid_id(id, policy) || !ids.insert(id) {
                return Err(ShapeFault::ItemIdInvalidOrDuplicate.code().into());
            }
        }
        match text_field(item, "type", ShapeFault::ItemType)? {
            "reasoning" => {
                keys(
                    item,
                    &[
                        "type",
                        "id",
                        "status",
                        "summary",
                        "encrypted_content",
                        "content",
                    ],
                    ShapeFault::ReasoningFields,
                )?;
                if item.get("status").is_some_and(|v| {
                    !v.is_null()
                        && v.as_str() != Some("completed")
                        && !(capped && v.as_str() == Some("incomplete"))
                }) {
                    return Err("native_provider_response_not_completed".into());
                }
                if item
                    .get("encrypted_content")
                    .is_some_and(|v| !v.is_null() && !v.is_string())
                {
                    return Err(ShapeFault::ReasoningEncryptedContent.code().into());
                }
                if let Some(summary) = item.get("summary") {
                    for part in summary
                        .as_array()
                        .ok_or(ShapeFault::ReasoningSummary.code())?
                    {
                        keys(part, &["type", "text"], ShapeFault::ReasoningSummaryFields)?;
                        if part.get("type").and_then(Value::as_str) != Some("summary_text") {
                            return Err(ShapeFault::ReasoningSummaryType.code().into());
                        }
                        text_field(part, "text", ShapeFault::ReasoningSummaryText)?;
                    }
                }
                // Responses reasoning items may carry optional typed content.
                // Retain it only in the original bounded model context; it is
                // never selected as final assistant text or put in diagnostics.
                if let Some(content) = item.get("content") {
                    for part in content
                        .as_array()
                        .ok_or(ShapeFault::ReasoningContent.code())?
                    {
                        live(deadline)?;
                        keys(part, &["type", "text"], ShapeFault::ReasoningContentFields)?;
                        if part.get("type").and_then(Value::as_str) != Some("reasoning_text") {
                            return Err(ShapeFault::ReasoningContentType.code().into());
                        }
                        text_field(part, "text", ShapeFault::ReasoningContentText)?;
                    }
                }
            }
            "message" => {
                keys(
                    item,
                    &["type", "id", "status", "role", "content", "phase"],
                    ShapeFault::MessageFields,
                )?;
                if item.get("role").and_then(Value::as_str) != Some("assistant") {
                    return Err(ShapeFault::MessageRole.code().into());
                }
                if item.get("status").and_then(Value::as_str) != Some("completed")
                    && !(capped && item.get("status").and_then(Value::as_str) == Some("incomplete"))
                {
                    return Err("native_provider_response_not_completed".into());
                }
                if item.get("phase").is_some_and(|v| {
                    !v.is_null() && !matches!(v.as_str(), Some("commentary" | "final_answer"))
                }) {
                    return Err(ShapeFault::MessagePhase.code().into());
                }
                let parts = item
                    .get("content")
                    .and_then(Value::as_array)
                    .ok_or(ShapeFault::MessageContent.code())?;
                if capped && parts.is_empty() {
                    continue;
                }
                if parts.len() != 1 {
                    return Err(ShapeFault::MessageContentCount.code().into());
                }
                let part = &parts[0];
                match text_field(part, "type", ShapeFault::ContentType)? {
                    "output_text" => {
                        keys(
                            part,
                            &["type", "text", "annotations", "logprobs"],
                            ShapeFault::TextFields,
                        )?;
                        annotations(part)?;
                        if part
                            .get("logprobs")
                            .is_some_and(|v| !v.is_null() && !v.is_array())
                        {
                            return Err(ShapeFault::TextLogprobs.code().into());
                        }
                        let text = text_field(part, "text", ShapeFault::TextValue)?;
                        if item.get("phase").and_then(Value::as_str) != Some("commentary") {
                            final_messages += 1;
                            if final_messages > 1 {
                                return Err(ShapeFault::FinalMessageCount.code().into());
                            }
                            found.text = Some(text);
                        }
                    }
                    "refusal" => {
                        keys(part, &["type", "refusal"], ShapeFault::RefusalFields)?;
                        text_field(part, "refusal", ShapeFault::RefusalText)?;
                        found.refused = true;
                    }
                    _ => return Err(ShapeFault::UnsupportedContentType.code().into()),
                }
            }
            "function_call" => {
                keys(
                    item,
                    &["type", "id", "status", "call_id", "name", "arguments"],
                    ShapeFault::FunctionFields,
                )?;
                if found.calls.len() >= policy.function_calls_per_response {
                    return Err("native_tool_call_count_native_function_calls_per_response".into());
                }
                if item.get("status").and_then(Value::as_str) != Some("completed")
                    && !(capped && item.get("status").and_then(Value::as_str) == Some("incomplete"))
                {
                    return Err("native_provider_response_not_completed".into());
                }
                let name = text_field(item, "name", ShapeFault::FunctionName)?;
                if !profile.functions.iter().any(|f| f.name == name) {
                    return Err("native_tool_unconfigured".into());
                }
                let id = text_field(item, "call_id", ShapeFault::FunctionCallId)?;
                if id.len() > policy.call_id_bytes {
                    return Err("native_tool_call_identity_invalid_native_call_id_bytes".into());
                }
                if !valid_id(id, policy) {
                    return Err("native_tool_call_identity_invalid".into());
                }
                if found.calls.iter().any(|(old, _, _)| *old == id) {
                    return Err("native_tool_call_identity_invalid".into());
                }
                found.calls.push((
                    id,
                    name,
                    text_field(item, "arguments", ShapeFault::FunctionArguments)?,
                ));
            }
            "web_search_call" => {
                keys(
                    item,
                    &["type", "id", "status", "action"],
                    ShapeFault::SearchFields,
                )?;
                let configured = profile
                    .web_search
                    .as_ref()
                    .ok_or("native_tool_unconfigured")?;
                searches = searches
                    .checked_add(1)
                    .ok_or("native_tool_call_count_web_search_max_tool_calls")?;
                if searches > configured.max_tool_calls {
                    return Err("native_tool_call_count_web_search_max_tool_calls".into());
                }
                if item.get("status").and_then(Value::as_str) != Some("completed") {
                    return Err("native_provider_response_not_completed".into());
                }
                if !valid_id(text_field(item, "id", ShapeFault::SearchId)?, policy) {
                    return Err(ShapeFault::SearchIdInvalid.code().into());
                }
                search_action(
                    item.get("action")
                        .ok_or(ShapeFault::SearchActionMissing.code())?,
                )?;
            }
            _ => return Err("native_tool_unconfigured".into()),
        }
    }
    if !capped && found.calls.is_empty() && found.text.is_none() && !found.refused {
        return Err(ShapeFault::TerminalMissing.code().into());
    }
    if !found.calls.is_empty() && found.refused {
        return Err(ShapeFault::CallsWithRefusal.code().into());
    }
    Ok(found)
}

fn shape<'a>(
    items: &'a Value,
    profile: &NativeLoopProfile,
    deadline: Instant,
    policy: &NativeLimits,
) -> Result<Shape<'a>, String> {
    shape_with_mode(items, profile, deadline, policy, false)
}
pub(super) fn capped(
    envelope: &Value,
    input: &ValidationInput,
    deadline: Instant,
    policy: &NativeLimits,
) -> bool {
    cap_shape(envelope, input, deadline, policy).is_ok()
}
fn cap_shape<'a>(
    envelope: &'a Value,
    input: &ValidationInput,
    deadline: Instant,
    policy: &NativeLimits,
) -> Result<Shape<'a>, String> {
    live(deadline)?;
    if !input.repair_enabled
        || envelope.get("status").and_then(Value::as_str) != Some("incomplete")
        || envelope.get("error").is_some_and(|v| !v.is_null())
    {
        return Err("native_output_cap_terminal_invalid".into());
    }
    let details = envelope
        .get("incomplete_details")
        .and_then(Value::as_object)
        .ok_or("native_output_cap_terminal_invalid")?;
    if details.len() != 1
        || details.get("reason").and_then(Value::as_str) != Some("max_output_tokens")
    {
        return Err("native_output_cap_terminal_invalid".into());
    }
    let found = shape_with_mode(
        envelope
            .get("output")
            .ok_or("native_output_cap_items_invalid")?,
        &input.profile,
        deadline,
        policy,
        true,
    )?;
    if found.refused {
        return Err("native_provider_refused".into());
    }
    if input.output_prefix.is_some() && !found.calls.is_empty() {
        return Err("native_output_cap_final_text_call_conflict".into());
    }
    if let Some(text) = found.text {
        joined_text(
            input.output_prefix.as_deref(),
            text,
            input.output_bytes.max(policy.context_bytes),
        )?;
    }
    Ok(found)
}

/// Helper-side profile preflight: real selected JSON Schemas are compiled in
/// the existing killable CPU helper, not on an async executor thread.
pub(super) fn preflight(value: &Value, policy: &NativeLimits) -> Result<ValidationInput, String> {
    let input: ValidationInput =
        serde_json::from_value(value.clone()).map_err(|_| "native_tool_profile_invalid")?;
    selected(&input.profile, policy)?;
    if input.output_bytes == 0 || input.output_bytes > policy.output_bytes {
        return Err("native_output_limit_invalid_native_output_bytes".into());
    }
    if input.output_prefix.as_ref().is_some_and(|p| {
        !input.repair_enabled || p.len() > input.output_bytes.max(policy.context_bytes)
    }) || (input.joined_charter && input.output_prefix.is_none())
    {
        return Err("native_output_cap_prefix_invalid".into());
    }
    schema::Schema::compile(&input.schema, policy)?;
    for function in &input.profile.functions {
        schema::Schema::compile(&function.parameters, policy)?;
    }
    Ok(input)
}
fn completed_shape<'a>(
    envelope: &'a Value,
    profile: &NativeLoopProfile,
    deadline: Instant,
    policy: &NativeLimits,
) -> Result<Shape<'a>, String> {
    if envelope.get("status").and_then(Value::as_str) != Some("completed")
        || envelope.get("error").is_some_and(|v| !v.is_null())
    {
        return Err("native_provider_response_not_completed".into());
    }
    shape(
        envelope
            .get("output")
            .ok_or("native_provider_response_not_completed")?,
        profile,
        deadline,
        policy,
    )
}
pub(super) fn completed(
    envelope: &Value,
    profile: &NativeLoopProfile,
    deadline: Instant,
    policy: &NativeLimits,
) -> bool {
    // This flag classifies provider disposition only. Validation retains the
    // original error, including the exact metadata field that refused a bound.
    completed_shape(envelope, profile, deadline, policy).is_ok()
}
pub(super) fn validate_response(
    envelope: &Value,
    input: &ValidationInput,
    limit: usize,
    deadline: Instant,
    policy: &NativeLimits,
) -> Result<Vec<u8>, String> {
    let (bytes, issue) = validate_response_typed(envelope, input, limit, deadline, policy)?;
    match issue {
        Some(issue) => Err(issue.stable_error().into()),
        None => Ok(bytes),
    }
}

pub(super) fn validate_response_typed(
    envelope: &Value,
    input: &ValidationInput,
    limit: usize,
    deadline: Instant,
    policy: &NativeLimits,
) -> Result<(Vec<u8>, Option<super::FormatIssue>), String> {
    let found = if capped(envelope, input, deadline, policy) {
        cap_shape(envelope, input, deadline, policy)?
    } else {
        completed_shape(envelope, &input.profile, deadline, policy)?
    };
    let is_cap = envelope.get("status").and_then(Value::as_str) == Some("incomplete");
    let items = envelope
        .get("output")
        .ok_or(ShapeFault::OutputMissing.code())?;
    schema::measure(
        items,
        limit,
        policy,
        if input.output_bytes >= policy.context_bytes {
            "task_output_bytes"
        } else {
            "native_context_bytes"
        },
    )?;
    if found.refused {
        return Err("native_provider_refused".into());
    }
    let mut issue = None;
    if is_cap {
        return Ok((
            serde_json::to_vec(items).map_err(|_| "native_output_cap_encoding")?,
            None,
        ));
    }
    if !found.calls.is_empty() {
        if input.output_prefix.is_some() {
            return Err("native_output_cap_final_text_call_conflict".into());
        }
        for (_, name, args) in found.calls {
            let function = input
                .profile
                .functions
                .iter()
                .find(|f| f.name == name)
                .ok_or("native_tool_unconfigured")?;
            let args = decode::parse(
                args.as_bytes(),
                policy.context_bytes,
                deadline,
                "native_tool_arguments_invalid",
                policy,
                "native_context_bytes",
            )?;
            schema::Schema::compile(&function.parameters, policy)?.validate(
                &args,
                policy.context_bytes,
                "native_context_bytes",
            )?;
            schema::measure(items, policy.context_bytes, policy, "native_context_bytes")?;
        }
    } else {
        let raw_limit = joined_limit(input.output_bytes, input.joined_charter, policy)?;
        let text = joined_text(
            input.output_prefix.as_deref(),
            found.text.ok_or(ShapeFault::FinalTextMissing.code())?,
            raw_limit,
        )?;
        if input.repair_enabled {
            match decode::parse_final(
                text.as_bytes(),
                raw_limit,
                deadline,
                policy,
                "task_output_bytes",
            )? {
                Ok(output) => {
                    issue = schema::Schema::compile(&input.schema, policy)?.final_issue(
                        &final_value(output, input.joined_charter),
                        input.output_bytes,
                        "task_output_bytes",
                    )?
                }
                Err(found) => issue = Some(found),
            }
        } else {
            let output = decode::parse(
                text.as_bytes(),
                input.output_bytes,
                deadline,
                "native_output_not_json",
                policy,
                "task_output_bytes",
            )?;
            schema::Schema::compile(&input.schema, policy)?.validate(
                &output,
                input.output_bytes,
                "task_output_bytes",
            )?;
        }
    }
    live(deadline)?;
    let bytes = serde_json::to_vec(items).map_err(|_| "native_tool_context_encoding")?;
    Ok((bytes, issue))
}

#[cfg(test)]
mod policy_tests {
    use super::*;

    fn policy() -> NativeLimits {
        serde_json::from_str(include_str!("../../limits-native-seed.json")).unwrap()
    }

    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }

    fn profile() -> NativeLoopProfile {
        NativeLoopProfile {
            functions: vec![
                NativeFunction {
                    name: "inspect_record".into(),
                    description: "Inspect a record by key.".into(),
                    parameters: json!({
                        "type": "object",
                        "properties": {"key": {"type": "string"}},
                        "required": ["key"],
                        "additionalProperties": false
                    }),
                },
                NativeFunction {
                    name: "read_version".into(),
                    description: "Read a positive record version.".into(),
                    parameters: json!({
                        "type": "object",
                        "properties": {"version": {"type": "integer", "minimum": 1}},
                        "required": ["version"],
                        "additionalProperties": false
                    }),
                },
            ],
            web_search: None,
        }
    }

    fn envelope(second_arguments: Value) -> Value {
        json!({
            "status": "completed",
            "error": null,
            "output": [
                {
                    "type": "function_call",
                    "id": "item-first",
                    "status": "completed",
                    "call_id": "logical-first",
                    "name": "inspect_record",
                    "arguments": r#"{"key":"alpha"}"#
                },
                {
                    "type": "function_call",
                    "id": "item-second",
                    "status": "completed",
                    "call_id": "logical-second",
                    "name": "read_version",
                    "arguments": serde_json::to_string(&second_arguments).unwrap()
                }
            ]
        })
    }

    fn validation(profile: NativeLoopProfile, policy: &NativeLimits) -> ValidationInput {
        preflight(
            &serde_json::to_value(ValidationInput {
                output_prefix: None,
                joined_charter: false,
                schema: json!({
                    "type": "object",
                    "properties": {"result": {"type": "string"}},
                    "required": ["result"],
                    "additionalProperties": false
                }),
                profile,
                output_bytes: policy.output_bytes,
                repair_enabled: false,
            })
            .unwrap(),
            policy,
        )
        .unwrap()
    }

    #[test]
    fn configured_two_call_response_retains_provider_order() {
        let mut policy = policy();
        policy.function_calls_per_response = 2;
        let input = validation(profile(), &policy);
        let envelope = envelope(json!({"version": 1}));
        let found = shape(&envelope["output"], &input.profile, deadline(), &policy).unwrap();
        assert_eq!(
            found.calls,
            vec![
                ("logical-first", "inspect_record", r#"{"key":"alpha"}"#),
                ("logical-second", "read_version", r#"{"version":1}"#),
            ]
        );
        let encoded =
            validate_response(&envelope, &input, policy.context_bytes, deadline(), &policy)
                .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&encoded).unwrap(),
            envelope["output"]
        );
    }

    #[test]
    fn configured_one_call_limit_rejects_a_two_call_response() {
        let mut policy = policy();
        policy.function_calls_per_response = 1;
        let profile = profile();
        let envelope = envelope(json!({"version": 1}));
        assert_eq!(
            shape(&envelope["output"], &profile, deadline(), &policy)
                .err()
                .as_deref(),
            Some("native_tool_call_count_native_function_calls_per_response")
        );
        assert!(!completed(&envelope, &profile, deadline(), &policy));
        let input = validation(profile, &policy);
        assert_eq!(
            validate_response(&envelope, &input, policy.context_bytes, deadline(), &policy)
                .unwrap_err(),
            "native_tool_call_count_native_function_calls_per_response"
        );
    }

    #[test]
    fn batch_rejects_duplicate_logical_ids_with_distinct_item_ids() {
        let mut policy = policy();
        policy.function_calls_per_response = 2;
        let profile = profile();
        let mut envelope = envelope(json!({"version": 1}));
        envelope["output"][1]["call_id"] = json!("logical-first");
        assert_eq!(
            shape(&envelope["output"], &profile, deadline(), &policy)
                .err()
                .as_deref(),
            Some("native_tool_call_identity_invalid")
        );
        assert!(!completed(&envelope, &profile, deadline(), &policy));
    }

    #[test]
    fn batch_validates_second_call_against_its_selected_schema() {
        let mut policy = policy();
        policy.function_calls_per_response = 2;
        let input = validation(profile(), &policy);
        let mut envelope = envelope(json!({"version": "invalid"}));
        // This is a complete two-call provider response; the second arguments
        // fail only when the adapter validates the selected function schema.
        assert!(completed(&envelope, &input.profile, deadline(), &policy));
        assert_eq!(
            validate_response(&envelope, &input, policy.context_bytes, deadline(), &policy,)
                .unwrap_err(),
            "native_output_schema_mismatch"
        );
        envelope["output"].as_array_mut().unwrap().truncate(1);
        validate_response(&envelope, &input, policy.context_bytes, deadline(), &policy).unwrap();
    }

    fn tool_input_fixture(context_bytes: usize) -> (NativeLoopCall, String) {
        let limits = crate::limits::RuntimeLimits::seed().unwrap();
        let mut native = limits.execution.native.clone();
        native.context_bytes = context_bytes;
        native.function_calls_per_response = 2;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let mut task: TaskInput = serde_json::from_value(json!({
            "instruction": "Read the synthetic retained tool result.",
            "definition": {
                "id": "tool-input-agent", "revision": "tool-input-r1", "executor": "native",
                "inference": crate::protocol::InferenceSettings::selected(),
                "execution_profile": {"kind": "collaboration_v1"},
                "limit_overrides": {"native": native}
            },
            "repository": null, "acceptance_commands": [],
            "output_schema": {"type": "object", "properties": {"result": {"type": "string"}},
                "required": ["result"], "additionalProperties": false},
            "limits": limits.task_defaults.at(now).unwrap(), "dependencies": []
        }))
        .unwrap();
        task.limits.output_bytes = 65_536;
        task.resolved_limits = Some(limits.resolve_task(&task).unwrap());
        let call = NativeLoopCall {
            pending: vec![(
                "logical-second".into(),
                "read_version".into(),
                json!({"version": 1}),
            )],
            call_index: 1,
            task,
            profile: profile(),
            context: envelope(json!({"version": 1}))["output"].clone(),
            request_index: 1,
            charged_through: 1,
            compacted: false,
            call_id: "logical-first".into(),
            name: "inspect_record".into(),
            arguments: json!({"key": "alpha"}),
        };
        // Complete synthetic envelope, including strings that require escaping
        // again inside function_call_output. This is not historical evidence.
        let mut value = json!({
            "operation_id": "synthetic-tool-operation", "logical_call_id": "logical-first",
            "outcome": {"status": "ok", "data": {"kind": "memory", "value": {
                "agent_id": "tool-input-agent", "key": "fixture", "revision": 1,
                "value": "synthetic \"quoted\" \\ input\n\u{03bb}".repeat(256)
            }}},
            "commit": {"operation_id": "synthetic-tool-operation", "controller_revision": 7}
        });
        let padding = 95_028 - serde_json::to_vec(&value).unwrap().len();
        let payload = value["outcome"]["data"]["value"]["value"]
            .as_str()
            .unwrap()
            .to_owned();
        value["outcome"]["data"]["value"]["value"] = json!(payload + &"x".repeat(padding));
        let result = serde_json::to_string(&value).unwrap();
        assert_eq!(result.len(), 95_028);
        (call, result)
    }

    #[test]
    // Traceability scope: Component-only serial and continuation context construction retains a complete synthetic envelope; no provider or historical receipt is used.
    //= spec/impl/v2/implementation.md#native-tool-profiles-and-continuations
    //= type=test
    //# A native model continuation MUST use the recorded outcome of its corresponding tool call.
    fn large_tool_input_survives_serial_and_continuation_appends_unchanged() {
        let (first, result) = tool_input_fixture(262_144);
        assert_eq!(first.task.limits.output_bytes, 65_536);
        let original = first.retained_items().clone();
        let second = first.next_call(&result).unwrap().unwrap();
        let policy = &second.task.execution_limits().unwrap().native;
        let context = second.tool_output_context(&result, policy).unwrap();
        let mut expected = original.clone();
        for call_id in ["logical-first", "logical-second"] {
            expected.as_array_mut().unwrap().push(json!({
                "type": "function_call_output", "call_id": call_id, "output": result
            }));
        }
        assert_eq!(context, expected);
        // Check after the full JSON context has been encoded and decoded too.
        let round_trip: Value =
            serde_json::from_slice(&serde_json::to_vec(&context).unwrap()).unwrap();
        for index in [2, 3] {
            assert_eq!(
                round_trip[index]["output"].as_str().unwrap().as_bytes(),
                result.as_bytes()
            );
        }
        assert_eq!(first.retained_items(), &original);
        assert_eq!(second.retained_items().as_array().unwrap().len(), 3);
        assert_eq!(second.task.limits.output_bytes, 65_536);
        assert_eq!(second.request_index(), first.request_index());
        assert_eq!(second.charged_through, first.charged_through);
        assert!(second.pending.is_empty());
    }

    #[test]
    fn tool_input_context_counts_json_framing_and_accumulated_history() {
        let (first, result) = tool_input_fixture(262_144);
        let appended =
            json!({"type": "function_call_output", "call_id": first.call_id(), "output": result});
        let framed_bytes = serde_json::to_vec(&appended).unwrap().len();
        let accumulated_bytes = serde_json::to_vec(first.retained_items()).unwrap().len();
        let exact_bound = accumulated_bytes + framed_bytes + 1;
        assert!(framed_bytes > result.len());
        for (bound, reason) in [
            (result.len(), "native_json_byte_bound_native_context_bytes"),
            (
                framed_bytes,
                "native_tool_context_bound_native_context_bytes",
            ),
            (
                exact_bound - 1,
                "native_tool_context_bound_native_context_bytes",
            ),
        ] {
            let (call, same_result) = tool_input_fixture(bound);
            assert_eq!(same_result, result);
            let policy = &call.task.execution_limits().unwrap().native;
            assert_eq!(call.next_call(&result).err().unwrap(), reason);
            assert_eq!(
                call.tool_output_context(&result, policy).unwrap_err(),
                reason
            );
            assert_eq!(call.task.limits.output_bytes, 65_536);
        }
        let (call, _) = tool_input_fixture(exact_bound);
        let policy = &call.task.execution_limits().unwrap().native;
        let second = call.next_call(&result).unwrap().unwrap();
        let context = call.tool_output_context(&result, policy).unwrap();
        assert_eq!(second.retained_items(), &context);
        assert_eq!(serde_json::to_vec(&context).unwrap().len(), exact_bound);
        assert_eq!(
            second.tool_output_context(&result, policy).unwrap_err(),
            "native_tool_context_bound_native_context_bytes"
        );
        let (call, _) = tool_input_fixture(result.len() - 1);
        assert_eq!(
            call.tool_output_context(&result, &call.task.execution_limits().unwrap().native)
                .unwrap_err(),
            "native_tool_result_bound_native_context_bytes"
        );
    }

    #[test]
    fn large_tool_input_does_not_raise_final_output_or_change_its_schema() {
        let (call, _) = tool_input_fixture(262_144);
        let policy = &call.task.execution_limits().unwrap().native;
        let input = ValidationInput {
            output_prefix: None,
            joined_charter: false,
            repair_enabled: false,
            schema: call.task.output_schema.clone().unwrap(),
            profile: call.profile.clone(),
            output_bytes: call.task.limits.output_bytes,
        };
        assert_eq!(input.output_bytes, 65_536);
        let final_envelope = |value: Value| {
            json!({
                "status": "completed", "error": null,
                "output": [{"type": "message", "id": "synthetic-final", "role": "assistant",
                    "status": "completed", "content": [{"type": "output_text",
                        "text": serde_json::to_string(&value).unwrap(), "annotations": []}]}]
            })
        };
        let overhead = serde_json::to_vec(&json!({"result": ""})).unwrap().len();
        let at_bound = final_envelope(json!({"result": "x".repeat(65_536 - overhead)}));
        assert_eq!(
            at_bound["output"][0]["content"][0]["text"]
                .as_str()
                .unwrap()
                .len(),
            65_536
        );
        validate_response(&at_bound, &input, policy.context_bytes, deadline(), policy).unwrap();
        let oversized = final_envelope(json!({"result": "x".repeat(65_537 - overhead)}));
        assert_eq!(
            validate_response(&oversized, &input, policy.context_bytes, deadline(), policy)
                .unwrap_err(),
            "native_output_cap_prefix_bound_task_output_bytes"
        );
        assert_eq!(
            validate_response(
                &final_envelope(json!({"result": 1})),
                &input,
                policy.context_bytes,
                deadline(),
                policy
            )
            .unwrap_err(),
            "native_output_schema_mismatch"
        );
        assert_eq!(call.task.limits.output_bytes, 65_536);
    }

    #[tokio::test]
    async fn serial_calls_retain_receipts_before_provider_continuation() {
        use std::sync::Arc;

        let mut limits = crate::limits::RuntimeLimits::seed().unwrap();
        limits.execution.native.function_calls_per_response = 2;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let mut input: TaskInput = serde_json::from_value(json!({
            "instruction": "Inspect the record and its version.",
            "definition": {
                "id": "serial-call-agent",
                "revision": "serial-call-revision",
                "executor": "native",
                "inference": crate::protocol::InferenceSettings::selected(),
                "execution_profile": {"kind": "collaboration_v1"}
            },
            "repository": null,
            "acceptance_commands": [],
            "output_schema": {"type": "object"},
            "limits": limits.task_defaults.at(now).unwrap(),
            "dependencies": []
        }))
        .unwrap();
        input.resolved_limits = Some(limits.resolve_task(&input).unwrap());
        let profile = profile();
        let first = NativeLoopCall {
            pending: vec![(
                "logical-second".into(),
                "read_version".into(),
                json!({"version": 1}),
            )],
            call_index: 1,
            task: input.clone(),
            profile: profile.clone(),
            context: envelope(json!({"version": 1}))["output"].clone(),
            request_index: 1,
            charged_through: 1,
            compacted: false,
            call_id: "logical-first".into(),
            name: "inspect_record".into(),
            arguments: json!({"key": "alpha"}),
        };
        assert_eq!(first.call_index(), 1);
        assert_eq!(first.pending.len(), 1);
        let second = first
            .next_call("actual first retained receipt")
            .unwrap()
            .unwrap();
        assert_eq!(second.call_index(), 2);
        assert_eq!(second.request_index(), first.request_index());
        assert_eq!(second.call_id(), "logical-second");
        assert_eq!(second.name(), "read_version");
        assert_eq!(second.arguments(), &json!({"version": 1}));
        assert_eq!(
            second.retained_items().as_array().unwrap().last().unwrap(),
            &json!({
                "type": "function_call_output",
                "call_id": "logical-first",
                "output": "actual first retained receipt"
            })
        );
        assert_eq!(first.retained_items().as_array().unwrap().len(), 2);
        assert!(second.pending.is_empty());
        assert!(
            second
                .next_call("actual second retained receipt")
                .unwrap()
                .is_none()
        );

        let host = ValidationHost::new(
            "/unused-program".into(),
            "/unused-registry".into(),
            Arc::new(limits.execution.native),
            Arc::new(ValidationRuntime::new(Arc::from(limits.worker)).unwrap()),
        );
        let config = NativeConfig {
            endpoint: "http://127.0.0.1:1/v1/responses".into(),
            api_key_env: "UNUSED_PENDING_CALL_KEY".into(),
            model: crate::protocol::SELECTED_MODEL.into(),
        };
        let observed = execute_loop_step(
            &input,
            &config,
            &profile,
            Some((
                &first,
                NativeLoopReply {
                    call_id: first.call_id(),
                    output: "actual first retained receipt",
                },
            )),
            &host,
            async { Err("pending call reached provider send".into()) },
            std::future::pending(),
        )
        .await;
        assert_eq!(
            observed.result.err().as_deref(),
            Some("native_tool_observation_mismatch")
        );
        assert_eq!(observed.request_exposure, RequestExposure::NotStarted);
        assert_eq!(observed.provider_outcome, ProviderOutcome::NotStarted);
    }
}

#[cfg(test)]
mod shape_diagnostic_tests {
    use super::*;

    fn policy() -> NativeLimits {
        crate::limits::RuntimeLimits::seed()
            .unwrap()
            .execution
            .native
    }
    fn profile() -> NativeLoopProfile {
        NativeLoopProfile {
            functions: Vec::new(),
            web_search: None,
        }
    }
    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }
    fn items() -> Value {
        json!([
            {"type":"reasoning","id":"rs_test","summary":[],"encrypted_content":"synthetic"},
            {"type":"message","id":"msg_test","role":"assistant","status":"completed","phase":"final_answer",
                "content":[{"type":"output_text","text":"{\"ok\":true}","annotations":[],"logprobs":[]}]}
        ])
    }
    fn changed(edit: impl FnOnce(&mut Value)) -> Value {
        let mut value = items();
        edit(&mut value);
        value
    }

    #[test]
    fn documented_reasoning_content_is_optional_and_retains_exact_items() {
        let policy = policy();
        let validation = ValidationInput {
            output_prefix: None,
            joined_charter: false,
            schema: json!({"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false}),
            profile: profile(),
            output_bytes: policy.output_bytes,
            repair_enabled: false,
        };
        for content in [
            None,
            Some(json!([])),
            Some(json!([{"type":"reasoning_text","text":"SYNTHETIC_PRIVATE_REASONING"}])),
            Some(json!([
                {"type":"reasoning_text","text":"SYNTHETIC_PRIVATE_REASONING one"},
                {"type":"reasoning_text","text":""},
                {"type":"reasoning_text","text":"SYNTHETIC_PRIVATE_REASONING two \u{03bb}"}
            ])),
        ] {
            let mut output = items();
            if let Some(content) = content {
                output[0]["content"] = content;
            }
            let envelope = json!({"status":"completed","error":null,"output":output});
            assert!(completed(
                &envelope,
                &validation.profile,
                deadline(),
                &policy
            ));
            let encoded = validate_response(
                &envelope,
                &validation,
                policy.context_bytes,
                deadline(),
                &policy,
            )
            .unwrap();
            assert_eq!(serde_json::from_slice::<Value>(&encoded).unwrap(), output);
            let found = shape(&output, &validation.profile, deadline(), &policy).unwrap();
            assert_eq!(found.text, Some("{\"ok\":true}"));
            assert!(found.calls.is_empty());
            assert!(!found.refused);
        }
    }

    #[test]
    fn reasoning_content_obeys_existing_context_complexity_and_deadline_bounds() {
        let policy = policy();
        let output = changed(|items| {
            items[0]["content"] = json!([
                {"type":"reasoning_text","text":"synthetic bounded reasoning one"},
                {"type":"reasoning_text","text":"synthetic bounded reasoning two"}
            ]);
        });
        let (bytes, nodes) = schema::measure(
            &output,
            policy.context_bytes,
            &policy,
            "native_context_bytes",
        )
        .unwrap();
        let validation = ValidationInput {
            output_prefix: None,
            joined_charter: false,
            schema: json!(true),
            profile: profile(),
            output_bytes: "{\"ok\":true}".len(),
            repair_enabled: false,
        };
        let envelope = json!({"status":"completed","error":null,"output":output});
        validate_response(
            &envelope,
            &validation,
            policy.context_bytes,
            deadline(),
            &policy,
        )
        .unwrap();
        let mut bounded = policy.clone();
        bounded.context_bytes = bytes - 1;
        assert_eq!(
            validate_response(
                &envelope,
                &validation,
                bounded.context_bytes,
                deadline(),
                &bounded
            )
            .unwrap_err(),
            "native_json_byte_bound_native_context_bytes"
        );
        let mut bounded = policy.clone();
        bounded.json_nodes = nodes - 1;
        assert_eq!(
            validate_response(
                &envelope,
                &validation,
                bounded.context_bytes,
                deadline(),
                &bounded
            )
            .unwrap_err(),
            "native_json_complexity_bound_native_json_nodes"
        );
        let mut bounded = policy.clone();
        bounded.json_depth = 1;
        assert_eq!(
            validate_response(
                &envelope,
                &validation,
                bounded.context_bytes,
                deadline(),
                &bounded
            )
            .unwrap_err(),
            "native_json_complexity_bound_native_json_depth"
        );
        assert_eq!(
            validate_response(
                &envelope,
                &validation,
                policy.context_bytes,
                Instant::now() - Duration::from_millis(1),
                &policy
            )
            .unwrap_err(),
            "native_deadline_exceeded"
        );
    }

    #[test]
    fn malformed_reasoning_content_refuses_with_closed_diagnostics() {
        let policy = policy();
        let validation = ValidationInput {
            output_prefix: None,
            joined_charter: false,
            schema: json!(true),
            profile: profile(),
            output_bytes: policy.output_bytes,
            repair_enabled: false,
        };
        for (content, expected) in [
            (Value::Null, ShapeFault::ReasoningContent),
            (json!(false), ShapeFault::ReasoningContent),
            (json!("DO_NOT_RETAIN"), ShapeFault::ReasoningContent),
            (
                json!({"type":"reasoning_text","text":"DO_NOT_RETAIN"}),
                ShapeFault::ReasoningContent,
            ),
            (json!([null]), ShapeFault::ReasoningContentFields),
            (json!(["DO_NOT_RETAIN"]), ShapeFault::ReasoningContentFields),
            (json!([{}]), ShapeFault::ReasoningContentType),
            (
                json!([{"type":"reasoning_text","text":"DO_NOT_RETAIN","DO_NOT_RETAIN":true}]),
                ShapeFault::ReasoningContentFields,
            ),
            (
                json!([{"text":"DO_NOT_RETAIN"}]),
                ShapeFault::ReasoningContentType,
            ),
            (
                json!([{"type":null,"text":"DO_NOT_RETAIN"}]),
                ShapeFault::ReasoningContentType,
            ),
            (
                json!([{"type":7,"text":"DO_NOT_RETAIN"}]),
                ShapeFault::ReasoningContentType,
            ),
            (
                json!([{"type":"summary_text","text":"DO_NOT_RETAIN"}]),
                ShapeFault::ReasoningContentType,
            ),
            (
                json!([{"type":"reasoning_text"}]),
                ShapeFault::ReasoningContentText,
            ),
            (
                json!([{"type":"reasoning_text","text":null}]),
                ShapeFault::ReasoningContentText,
            ),
            (
                json!([{"type":"reasoning_text","text":7}]),
                ShapeFault::ReasoningContentText,
            ),
            (
                json!([{"type":"reasoning_text","text":["DO_NOT_RETAIN"]}]),
                ShapeFault::ReasoningContentText,
            ),
            (
                json!([{"type":"reasoning_text","text":{"DO_NOT_RETAIN":true}}]),
                ShapeFault::ReasoningContentText,
            ),
            (
                json!([{"type":"reasoning_text","text":"valid first part"},{"type":"output_text","text":"DO_NOT_RETAIN"}]),
                ShapeFault::ReasoningContentType,
            ),
        ] {
            let output = changed(|items| items[0]["content"] = content);
            let envelope = json!({"status":"completed","error":null,"output":output});
            let error = validate_response(
                &envelope,
                &validation,
                policy.context_bytes,
                deadline(),
                &policy,
            )
            .unwrap_err();
            assert_eq!(error, expected.code());
            assert!(!error.contains("DO_NOT_RETAIN"));
            assert!(error.len() <= policy.helper_error_bytes);
            assert!(!completed(
                &envelope,
                &validation.profile,
                deadline(),
                &policy
            ));
        }
    }

    #[test]
    fn structural_refusals_identify_the_check_without_content() {
        let cases = [
            (json!(null), ShapeFault::OutputArray),
            (changed(|x| x[0]["id"] = json!(7)), ShapeFault::ItemIdType),
            (
                changed(|x| x[1]["id"] = json!("rs_test")),
                ShapeFault::ItemIdInvalidOrDuplicate,
            ),
            (
                changed(|x| x[0]["type"] = json!(false)),
                ShapeFault::ItemType,
            ),
            (
                changed(|x| x[0]["DO_NOT_RETAIN"] = json!("DO_NOT_RETAIN")),
                ShapeFault::ReasoningFields,
            ),
            (
                changed(|x| x[0]["encrypted_content"] = json!({"DO_NOT_RETAIN":1})),
                ShapeFault::ReasoningEncryptedContent,
            ),
            (
                changed(|x| x[0]["summary"] = Value::Null),
                ShapeFault::ReasoningSummary,
            ),
            (
                changed(|x| {
                    x[0]["summary"] =
                        json!([{"type":"summary_text","text":"synthetic","DO_NOT_RETAIN":true}])
                }),
                ShapeFault::ReasoningSummaryFields,
            ),
            (
                changed(|x| x[0]["summary"] = json!([{"type":"DO_NOT_RETAIN","text":"synthetic"}])),
                ShapeFault::ReasoningSummaryType,
            ),
            (
                changed(|x| x[0]["summary"] = json!([{"type":"summary_text","text":7}])),
                ShapeFault::ReasoningSummaryText,
            ),
            (
                changed(|x| x[1]["DO_NOT_RETAIN"] = json!(true)),
                ShapeFault::MessageFields,
            ),
            (
                changed(|x| x[1]["role"] = json!("DO_NOT_RETAIN")),
                ShapeFault::MessageRole,
            ),
            (
                changed(|x| x[1]["phase"] = json!("DO_NOT_RETAIN")),
                ShapeFault::MessagePhase,
            ),
            (
                changed(|x| x[1]["content"] = Value::Null),
                ShapeFault::MessageContent,
            ),
            (
                changed(|x| x[1]["content"] = json!([])),
                ShapeFault::MessageContentCount,
            ),
            (
                changed(|x| x[1]["content"][0]["type"] = json!(7)),
                ShapeFault::ContentType,
            ),
            (
                changed(|x| x[1]["content"][0]["type"] = json!("DO_NOT_RETAIN")),
                ShapeFault::UnsupportedContentType,
            ),
            (
                changed(|x| x[1]["content"][0]["DO_NOT_RETAIN"] = json!(true)),
                ShapeFault::TextFields,
            ),
            (
                changed(|x| x[1]["content"][0]["text"] = json!(7)),
                ShapeFault::TextValue,
            ),
            (
                changed(|x| x[1]["content"][0]["annotations"] = Value::Null),
                ShapeFault::Annotations,
            ),
            (
                changed(|x| x[1]["content"][0]["logprobs"] = json!(7)),
                ShapeFault::TextLogprobs,
            ),
            (
                changed(|x| {
                    let mut duplicate = x[1].clone();
                    duplicate["id"] = json!("msg_other");
                    x.as_array_mut().unwrap().push(duplicate);
                }),
                ShapeFault::FinalMessageCount,
            ),
            (
                changed(|x| x[1]["phase"] = json!("commentary")),
                ShapeFault::TerminalMissing,
            ),
            (
                changed(|x| x[1]["content"][0] = json!({"type":"refusal","refusal":7})),
                ShapeFault::RefusalText,
            ),
        ];
        let policy = policy();
        let validation = ValidationInput {
            output_prefix: None,
            joined_charter: false,
            schema: json!(true),
            profile: profile(),
            output_bytes: policy.output_bytes,
            repair_enabled: false,
        };
        for (output, fault) in cases {
            let envelope = json!({"status":"completed","error":null,"output":output});
            let error = validate_response(
                &envelope,
                &validation,
                policy.context_bytes,
                deadline(),
                &policy,
            )
            .unwrap_err();
            assert_eq!(error, fault.code());
            assert!(error.starts_with("native_response_shape_"));
            assert!(error.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'));
            assert!(!error.contains("DO_NOT_RETAIN"));
            assert!(error.len() <= policy.helper_error_bytes);
            assert!(!completed(
                &envelope,
                &validation.profile,
                deadline(),
                &policy
            ));
        }
    }

    #[test]
    fn annotation_and_search_diagnostics_are_closed() {
        let valid = json!({"type":"url_citation","url":"https://example.invalid","title":"synthetic","start_index":0,"end_index":1});
        for (field, value, expected) in [
            ("DO_NOT_RETAIN", json!(true), ShapeFault::AnnotationFields),
            ("type", json!("DO_NOT_RETAIN"), ShapeFault::AnnotationType),
            ("url", json!(7), ShapeFault::AnnotationUrl),
            ("title", json!(7), ShapeFault::AnnotationTitle),
            ("start_index", json!(-1), ShapeFault::AnnotationStart),
            ("end_index", json!(-1), ShapeFault::AnnotationEnd),
            ("end_index", json!(9), ShapeFault::AnnotationRange),
        ] {
            let mut annotation = valid.clone();
            annotation[field] = value;
            let part = json!({"text":"x","annotations":[annotation]});
            assert_eq!(annotations(&part).unwrap_err(), expected.code());
        }
        for (action, expected) in [
            (json!({"type":7}), ShapeFault::SearchActionType),
            (
                json!({"type":"search","DO_NOT_RETAIN":true}),
                ShapeFault::SearchActionFields,
            ),
            (
                json!({"type":"search","query":7}),
                ShapeFault::SearchActionText,
            ),
            (
                json!({"type":"search","queries":[7]}),
                ShapeFault::SearchQueries,
            ),
            (
                json!({"type":"search","domains":[7]}),
                ShapeFault::SearchDomains,
            ),
            (
                json!({"type":"search","sources":7}),
                ShapeFault::SearchSources,
            ),
            (
                json!({"type":"search","sources":[{"type":"url","url":"synthetic","DO_NOT_RETAIN":true}]}),
                ShapeFault::SearchSourceFields,
            ),
            (
                json!({"type":"search","sources":[{"type":"DO_NOT_RETAIN","url":"synthetic"}]}),
                ShapeFault::SearchSourceType,
            ),
            (
                json!({"type":"search","sources":[{"type":"url","url":7}]}),
                ShapeFault::SearchSourceUrl,
            ),
        ] {
            assert_eq!(search_action(&action).unwrap_err(), expected.code());
        }
    }

    #[test]
    fn diagnostic_change_preserves_acceptance_and_validation_order() {
        let policy = policy();
        let validation = ValidationInput {
            output_prefix: None,
            joined_charter: false,
            schema: json!(true),
            profile: profile(),
            output_bytes: policy.output_bytes,
            repair_enabled: false,
        };
        let output = items();
        let mut envelope = json!({"status":"completed","error":null,"output":output});
        assert!(completed(
            &envelope,
            &validation.profile,
            deadline(),
            &policy
        ));
        let bytes = validate_response(
            &envelope,
            &validation,
            policy.context_bytes,
            deadline(),
            &policy,
        )
        .unwrap();
        assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), output);
        envelope["status"] = json!("incomplete");
        envelope["output"] = json!(null);
        assert_eq!(
            validate_response(
                &envelope,
                &validation,
                policy.context_bytes,
                deadline(),
                &policy
            )
            .unwrap_err(),
            "native_provider_response_not_completed"
        );
        envelope["status"] = json!("completed");
        envelope["output"] = changed(|x| {
            x[1]["role"] = json!("DO_NOT_RETAIN");
            x[1]["phase"] = json!("DO_NOT_RETAIN");
        });
        assert_eq!(
            validate_response(
                &envelope,
                &validation,
                policy.context_bytes,
                deadline(),
                &policy
            )
            .unwrap_err(),
            ShapeFault::MessageRole.code()
        );
    }
}

#[cfg(test)]
#[path = "activation_admission_tests.rs"]
mod activation_admission_tests;

#[cfg(test)]
mod provider_schema_tests {
    use super::*;

    fn diagnostic() -> (Value, ValidationInput, NativeLimits) {
        let source: Value =
            serde_json::from_str(include_str!("../../metadata/operational-diagnostic.json"))
                .unwrap();
        let limits = crate::limits::RuntimeLimits::seed().unwrap();
        let definition: crate::protocol::Definition =
            serde_json::from_value(source["definition"].clone()).unwrap();
        let selected = limits.resolve_definition(&definition).unwrap();
        // Retain the complete v4 input rendered from the exact catalogue,
        // renderer and limits named by its acceptance binding. Later catalogue
        // additions must not change this historical 19,746-byte fixture or be
        // mistaken for qualification of the current catalogue under v4 limits.
        let fixture: Value = serde_json::from_str(include_str!(
            "testdata/operational-diagnostic-v4-validation.json"
        ))
        .unwrap();
        for key in [
            "catalogue_source_sha256",
            "catalogue_renderer_sha256",
            "global_metadata_sha256",
        ] {
            assert_eq!(fixture["source_bindings"][key], source["schema_binding"][key]);
        }
        let validation: ValidationInput =
            serde_json::from_value(fixture["validation_input"].clone()).unwrap();
        assert_eq!(validation.schema, source["output_schema"]);
        assert_eq!(validation.output_bytes, 65_536);
        (source, validation, selected.native)
    }
    fn report() -> Value {
        json!({"disposition":"no_action","origin_task_id":"origin","target_agent_id":"agent",
            "reason":"No justified configured-limit change","change":null,
            "measurement":{"status":"not_applied","baseline":null,"later":null,
                "comparison":"not_comparable","causal_improvement_claim":false,"limitations":[]}})
    }
    fn envelope(report: &Value) -> Value {
        json!({"status":"completed","error":null,"output":[{
            "type":"message","id":"msg-report","role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":serde_json::to_string(report).unwrap(),"annotations":[]}]
        }]})
    }
    fn supported_shape(schema: &Value) {
        let Some(object) = schema.as_object() else {
            panic!("diagnostic guidance schema must be an object");
        };
        for key in [
            "allOf",
            "not",
            "dependentRequired",
            "dependentSchemas",
            "if",
            "then",
            "else",
        ] {
            assert!(
                !object.contains_key(key),
                "unsupported provider keyword {key}"
            );
        }
        assert!(
            object.contains_key("type"),
            "diagnostic guidance leaves use explicit types"
        );
        if let Some(properties) = object.get("properties").and_then(Value::as_object) {
            assert_eq!(object.get("additionalProperties"), Some(&json!(false)));
            let required = object["required"].as_array().unwrap();
            assert_eq!(required.len(), properties.len());
            for (key, value) in properties {
                assert!(required.contains(&json!(key)));
                supported_shape(value);
            }
        }
        if let Some(items) = object.get("items") {
            supported_shape(items);
        }
    }

    #[test]
    fn diagnostic_generation_guidance_preserves_structure_and_exact_accepted_schema_bound() {
        let (source, input, policy) = diagnostic();
        let before = input.schema.clone();
        let serialized = serde_json::to_value(&input).unwrap();
        assert_eq!(serde_json::to_vec(&serialized).unwrap().len(), 19_746);
        assert_eq!(policy.schema_bytes, 19_746);
        assert_eq!(source["schema_binding"]["schema_bytes"], json!(19_746));
        let format = response_format(&input.schema).unwrap();
        assert_eq!(format["type"], json!("json_schema"));
        assert_eq!(format["strict"], json!(false));
        let guidance = &format["schema"];
        supported_shape(guidance);
        assert_eq!(guidance["required"], before["required"]);
        assert_eq!(
            guidance["additionalProperties"],
            before["additionalProperties"]
        );
        assert_eq!(
            input.schema, before,
            "the accepted contract is never rewritten"
        );
        assert_eq!(input.schema["allOf"].as_array().unwrap().len(), 6);
        let validator = jsonschema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .build(guidance)
            .unwrap();
        assert!(validator.is_valid(&report()));
        let mut missing = report();
        missing.as_object_mut().unwrap().remove("change");
        assert!(!validator.is_valid(&missing));
        let mut extra = report();
        extra["unexpected"] = json!(true);
        assert!(!validator.is_valid(&extra));
    }

    #[test]
    fn guidance_valid_contradictions_are_refused_by_actual_final_validation() {
        let (_, input, policy) = diagnostic();
        let format = response_format(&input.schema).unwrap();
        let validator = jsonschema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .build(&format["schema"])
            .unwrap();
        let original = report();
        assert!(
            validate_response(
                &envelope(&original),
                &input,
                policy.context_bytes,
                Instant::now() + Duration::from_secs(5),
                &policy
            )
            .is_ok()
        );
        let mut missing_change = original.clone();
        missing_change["disposition"] = json!("measured");
        let mut wrong_state = original.clone();
        wrong_state["measurement"]["status"] = json!("incomplete_evidence");
        for contradictory in [missing_change, wrong_state] {
            assert!(
                validator.is_valid(&contradictory),
                "guidance is deliberately not the acceptance oracle"
            );
            assert_eq!(
                validate_response(
                    &envelope(&contradictory),
                    &input,
                    policy.context_bytes,
                    Instant::now() + Duration::from_secs(5),
                    &policy
                )
                .unwrap_err(),
                "native_output_schema_mismatch"
            );
        }
        assert_eq!(input.schema["allOf"].as_array().unwrap().len(), 6);
    }

    #[test]
    fn simple_schema_is_unchanged_and_unexpected_projection_constructs_refuse() {
        let simple = json!({"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false});
        assert_eq!(response_format(&simple).unwrap()["schema"], simple);
        for schema in [
            json!({"type":"object","allOf":[{"required":["ok"]}],"properties":{"ok":{"type":"boolean"}}}),
            json!({"type":"object","allOf":[{"if":true,"then":true,"required":["ok"]}]}),
            json!({"type":"object","allOf":[{"if":true,"then":true}],"properties":{"ok":{"$ref":"#/allOf/0/then"}}}),
            json!({"type":"object","allOf":[{"if":true,"then":true}],"properties":{"ok":{"allOf":[{"type":"boolean"}]}}}),
            json!({"type":"object","allOf":[{"if":true,"then":{"type":"string"}}],"patternProperties":{".*":{"$ref":"#/allOf/0/then"}}}),
            json!({"type":"object","allOf":[{"if":true,"then":true}],"unevaluatedProperties":{"$ref":"#/allOf/0/then"}}),
            json!({"type":"object","allOf":[{"if":true,"then":true}],"properties":{"rows":{"type":"array","contains":{"$ref":"#/allOf/0/then"}}}}),
            json!({"type":"object","allOf":[{"if":true,"then":true}],"properties":{"rows":{"type":"array","prefixItems":[{"$ref":"#/allOf/0/then"}]}}}),
            json!({"type":"object","allOf":[{"if":true,"then":true}],"propertyNames":{"$ref":"#/allOf/0/then"}}),
        ] {
            assert_eq!(
                response_format(&schema).unwrap_err(),
                "native_provider_schema_projection_unsupported"
            );
        }
    }

    #[test]
    fn scalar_types_do_not_guess_mixed_enums_or_transform_literal_values() {
        let mut schema = json!({"type":"object","properties":{
            "allOf":{"enum":["if","then"]},"mixed":{"enum":[true,1]},
            "numeric":{"enum":[1,1.5]},"null_only":{"const":null},
            "literal":{"const":{"enum":[true]}}
        }});
        explicit_scalar_types(&mut schema).unwrap();
        assert_eq!(schema["properties"]["allOf"]["type"], json!("string"));
        assert!(schema["properties"]["mixed"].get("type").is_none());
        assert_eq!(schema["properties"]["numeric"]["type"], json!("number"));
        assert_eq!(schema["properties"]["null_only"]["type"], json!("null"));
        assert_eq!(
            schema["properties"]["literal"],
            json!({"const":{"enum":[true]}})
        );
    }
}

#[cfg(test)]
mod format_repair_tests {
    use super::*;
    fn input() -> (ValidationInput, NativeLimits) {
        let policy = crate::limits::RuntimeLimits::seed()
            .unwrap()
            .execution
            .native;
        (
            ValidationInput {
                output_prefix: None,
                joined_charter: false,
                schema: json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false}),
                profile: NativeLoopProfile {
                    functions: vec![],
                    web_search: None,
                },
                output_bytes: 1024,
                repair_enabled: true,
            },
            policy,
        )
    }
    fn reply(text: &str) -> Value {
        json!({"status":"completed","output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":text}]}]})
    }
    #[test]
    fn only_completed_final_format_mismatch_yields_typed_repair() {
        let (input, p) = input();
        let deadline = Instant::now() + Duration::from_secs(5);
        for (text, want) in [
            ("bad", 0),
            ("{\"answer\":\"bad\"}", 1),
            ("{\"answer\":1,\"answer\":2}", 2),
        ] {
            let envelope = reply(text);
            let (items, issue) =
                validate_response_typed(&envelope, &input, p.context_bytes, deadline, &p).unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&items).unwrap(),
                envelope["output"]
            );
            assert!(match (want, issue) {
                (0, Some(super::super::FormatIssue::InvalidJson { .. })) => true,
                (
                    1,
                    Some(super::super::FormatIssue::Schema {
                        rule: super::super::SchemaRule::Type,
                    }),
                ) => true,
                (2, Some(super::super::FormatIssue::DuplicateKey {})) => true,
                _ => false,
            });
        }
        assert!(
            validate_response_typed(
                &reply("{\"answer\":1}"),
                &input,
                p.context_bytes,
                deadline,
                &p
            )
            .unwrap()
            .1
            .is_none()
        );
    }
    #[test]
    fn wire_refusal_bounds_and_historical_disabled_policy_never_become_format_repair() {
        let (mut input, p) = input();
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut incomplete = reply("bad");
        incomplete["status"] = json!("incomplete");
        assert!(
            validate_response_typed(&incomplete, &input, p.context_bytes, deadline, &p).is_err()
        );
        let refusal = json!({"status":"completed","output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"refusal","refusal":"no"}]}]});
        assert!(validate_response_typed(&refusal, &input, p.context_bytes, deadline, &p).is_err());
        input.output_bytes = 1;
        assert!(
            validate_response_typed(&reply("bad"), &input, p.context_bytes, deadline, &p)
                .unwrap_err()
                .contains("bound")
        );
        input.output_bytes = 1024;
        input.repair_enabled = false;
        assert_eq!(
            validate_response_typed(&reply("bad"), &input, p.context_bytes, deadline, &p)
                .unwrap_err(),
            "native_output_not_json"
        );
        assert!(
            validate_response_typed(&reply("bad"), &input, p.context_bytes, Instant::now(), &p)
                .is_err()
        );
    }
}

#[cfg(test)]
#[path = "output_cap_tests.rs"]
mod output_cap_tests;

#[cfg(test)]
#[path = "charter_profile_admission_tests.rs"]
mod charter_profile_admission_tests;
