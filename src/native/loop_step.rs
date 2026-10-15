//! One bounded Responses step for a host-selected collaboration tool catalogue.
//! This adapter does not call tools or grant authority. The worker must reserve
//! each request and retain/reconcile the actual MCP result before continuing.
use super::*;
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
    call_id: String,
    name: String,
    arguments: Value,
}
impl NativeLoopCall {
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
    pub fn retained_items(&self) -> &Value {
        &self.context
    }
}

/// Supplied by the host only after its actual tool result/receipt is retained.
/// Matching this ID proves association, not controller authority or persistence.
pub struct NativeLoopReply<'a> {
    pub call_id: &'a str,
    pub output: &'a str,
}

pub enum NativeLoopStep {
    Call(NativeLoopCall),
    Complete {
        result: NativeResult,
        /// Exact bounded provider items for host validation, including private
        /// reasoning. Public progress excludes reasoning items and retains
        /// citation observations without treating them as facts or authority.
        output_items: Value,
    },
}
pub struct NativeLoopObservation {
    pub result: Result<NativeLoopStep, String>,
    pub request_exposure: RequestExposure,
    pub usage: UsageObservation,
    pub provider_outcome: ProviderOutcome,
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
    let deadline = match super::deadline(input.limits.deadline_unix_ms) {
        Ok(value) => value,
        Err(error) => {
            return NativeLoopObservation {
                result: Err(error),
                request_exposure: RequestExposure::NotStarted,
                usage: UsageObservation::Unknown,
                provider_outcome: ProviderOutcome::NotStarted,
            };
        }
    };
    let mut cpu = cpu::State::new(host);
    let mut exposure = RequestExposure::NotStarted;
    let mut result = {
        let work = before(
            input,
            config,
            profile,
            prior,
            host_notice,
            host,
            before_send,
            deadline,
            &mut cpu,
            &mut exposure,
        );
        tokio::pin!(work, cancelled);
        tokio::select! { biased;
            _ = &mut cancelled => Err("native_cancelled".into()),
            _ = tokio::time::sleep_until(deadline) => Err("native_deadline_exceeded".into()),
            result = &mut work => result,
        }
    };
    if let Err(error) = cpu.finish(true).await {
        result = Err(error);
    }
    NativeLoopObservation {
        result,
        request_exposure: exposure,
        usage: cpu.provider_usage(),
        provider_outcome: if exposure == RequestExposure::NotStarted {
            ProviderOutcome::NotStarted
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
    host_notice: Option<&str>,
    host: &ValidationHost,
    before_send: G,
    deadline: Instant,
    cpu: &mut cpu::State,
    exposure: &mut RequestExposure,
) -> Result<NativeLoopStep, String> {
    let policy = &input.execution_limits()?.native;
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
    let (mut context, index) = match prior {
        None => (json!([{"role":"user","content":input.instruction}]), 1),
        Some((call, reply)) => {
            if call.task != *input
                || call.profile != *profile
                || reply.call_id != call.call_id
                || !call.pending.is_empty()
            {
                return Err("native_tool_observation_mismatch".into());
            }
            let index = call
                .request_index
                .checked_add(1)
                .ok_or("native_request_budget_exhausted_task_model_requests")?;
            if reply.output.len() > policy.context_bytes {
                return Err("native_tool_result_bound_native_context_bytes".into());
            }
            // Charge serialized framing before cloning/appending accumulated
            // context. JSON escaping is included, and nothing is truncated.
            // Traceability scope: The helper verifies the prior task/profile/call identity and appends that corresponding recorded tool output unchanged.
            //= spec/impl/v2/implementation.md#native-tool-profiles-and-continuations
            //# A native model continuation MUST use the recorded outcome of its corresponding tool call.
            let appended = json!({"type":"function_call_output","call_id":reply.call_id,"output":reply.output});
            let old = schema::measure(
                &call.context,
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
            let mut context = call.context.clone();
            context
                .as_array_mut()
                .ok_or("native_tool_context_invalid")?
                .push(appended);
            (context, index)
        }
    };
    if index > input.limits.model_requests {
        return Err("native_request_budget_exhausted_task_model_requests".into());
    }
    if let Some(notice) = host_notice {
        if notice.is_empty() || notice.len() > policy.memory_notice_bytes {
            return Err("native_memory_notice_bound_native_memory_notice_bytes".into());
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
        schema: schema.clone(),
        profile: profile.clone(),
        output_bytes: input.limits.output_bytes,
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
    let mut body = json!({
        "model":inference.model,"reasoning":{"effort":inference.reasoning_effort}, "input":context, "store":false, "stream":false,
        "include":["reasoning.encrypted_content"], "max_output_tokens":policy.max_output_tokens,
        "tools":tools, "tool_choice":"auto", "parallel_tool_calls":policy.function_calls_per_response > 1,
        "text":{"format":{"type":"json_schema","name":"borg_native_result","strict":false,"schema":schema}}
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
    let (wire, key) = send_request(url, config, &body, bound, deadline, exposure, host).await?;
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
    let shape = shape(&items, profile, deadline, policy)?;
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
            call_id,
            name,
            arguments,
            pending: calls,
            call_index: 1,
        }))
    } else {
        let text = shape.text.ok_or(ShapeFault::FinalTextMissing.code())?;
        let output = decode::parse(
            text.as_bytes(),
            input.limits.output_bytes,
            deadline,
            "native_output_not_json",
            policy,
            "task_output_bytes",
        )?;
        Ok(NativeLoopStep::Complete {
            result: NativeResult {
                summary: "Native output validated against the task schema".into(),
                output,
                requests: 1,
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
fn shape<'a>(
    items: &'a Value,
    profile: &NativeLoopProfile,
    deadline: Instant,
    policy: &NativeLimits,
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
                if item
                    .get("status")
                    .is_some_and(|v| !v.is_null() && v.as_str() != Some("completed"))
                {
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
                if item.get("status").and_then(Value::as_str) != Some("completed") {
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
                if item.get("status").and_then(Value::as_str) != Some("completed") {
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
    if found.calls.is_empty() && found.text.is_none() && !found.refused {
        return Err(ShapeFault::TerminalMissing.code().into());
    }
    if !found.calls.is_empty() && found.refused {
        return Err(ShapeFault::CallsWithRefusal.code().into());
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
    let found = completed_shape(envelope, &input.profile, deadline, policy)?;
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
    if !found.calls.is_empty() {
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
        let output = decode::parse(
            found
                .text
                .ok_or(ShapeFault::FinalTextMissing.code())?
                .as_bytes(),
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
    live(deadline)?;
    serde_json::to_vec(items).map_err(|_| "native_tool_context_encoding".into())
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
                schema: json!({
                    "type": "object",
                    "properties": {"result": {"type": "string"}},
                    "required": ["result"],
                    "additionalProperties": false
                }),
                profile,
                output_bytes: policy.output_bytes,
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
            Arc::new(ValidationRuntime::new(Arc::new(limits.worker)).unwrap()),
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
            schema: json!({"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false}),
            profile: profile(),
            output_bytes: policy.output_bytes,
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
            schema: json!(true),
            profile: profile(),
            output_bytes: "{\"ok\":true}".len(),
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
            schema: json!(true),
            profile: profile(),
            output_bytes: policy.output_bytes,
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
            schema: json!(true),
            profile: profile(),
            output_bytes: policy.output_bytes,
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
            schema: json!(true),
            profile: profile(),
            output_bytes: policy.output_bytes,
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
