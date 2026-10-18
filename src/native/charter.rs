//! Native rendering and original reply capture only. A public projection is
//! data, never a controller turn permit. The worker obtains the binding from
//! the canonical BeginTurn operation and durably records this capture before
//! dispatching tools or admitting another request.
use super::{decode, live, schema};
use crate::limits::NativeLimits;
use serde_json::{Value, json};
use std::sync::{Mutex, atomic::{AtomicBool, Ordering}};
use tokio::time::Instant;

/// Exact source and revision rendered from the controller's immutable bundle.
/// `changed` selects a mid-conversation instruction, not configuration adoption.
pub struct CharterInput {
    pub revision: Value,
    pub source: String,
    pub changed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    fn limits() -> NativeLimits { crate::limits::RuntimeLimits::seed().unwrap().execution.native }
    fn exchange(p: &NativeLimits) -> CharterExchange {
        CharterExchange::new(CharterInput { revision: json!({"definition_id":"a","definition_revision":"r"}),
            source: "# Source\nAn agent MUST retain the task.\n".into(), changed: false }, p).unwrap()
    }
    fn body() -> Value { json!({"input":"task with quote \" and newline\n", "text":{"format":{"type":"json_schema",
        "schema":{"$defs":{"value":{"type":"string"}},"$ref":"#/$defs/value"}}}}) }

    #[test]
    fn merged_input_exact_bound_and_original_root_reference_guidance() {
        let mut p = limits();
        let prepared = exchange(&p).prepare(&body(), &p).unwrap();
        let input_bytes = serde_json::to_vec(&prepared["input"]).unwrap().len();
        assert!(prepared["input"][0]["content"].as_str().unwrap().contains("#/$defs/value"));
        p.context_bytes = input_bytes;
        assert!(exchange(&p).prepare(&body(), &p).is_ok());
        p.context_bytes -= 1;
        assert!(exchange(&p).prepare(&body(), &p).is_err());
    }

    #[test]
    fn malformed_provider_output_still_has_original_observation() {
        let p = limits(); let e = exchange(&p);
        let wire = br#"{"id":"original-response","output":17,"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}"#;
        assert_eq!(e.capture(wire, "private-credential", Instant::now()+std::time::Duration::from_secs(1), &p).unwrap(), wire);
        let reply=e.take_reply().unwrap().unwrap();
        assert_eq!(reply.response_id.as_deref(),Some("original-response"));
        assert_eq!(reply.original_output,Some(json!(17))); assert!(reply.declared.is_none());
    }

    #[test]
    fn decoded_metadata_cannot_smuggle_escaped_credential() {
        let p=limits();let e=exchange(&p);
        let text=r#"{"charter_references":{"invalid":"\u0073ecret-value"},"result":{"result":"good"}}"#;
        let wire=serde_json::to_vec(&json!({"id":"r","output":[{"type":"message","content":[{"type":"output_text","text":text}]}]})).unwrap();
        assert!(!std::str::from_utf8(&wire).unwrap().contains("secret-value"));
        assert_eq!(e.capture(&wire,"secret-value",Instant::now()+std::time::Duration::from_secs(1),&p).err().as_deref(),Some("native_output_contains_credential"));
        assert!(e.take_reply().unwrap().is_none());
    }

    #[test]
    fn canonical_original_expansion_preserves_raw_admission_and_small_projection() {
        let mut p=limits();p.response_wire_bytes=65_536;p.json_nodes=256;
        let template=include_str!("../worker/testdata/canonical-output-expansion.json");
        let wire=template.replace("__PADDING__",&" ".repeat(p.response_wire_bytes-template.len()+11)).into_bytes();
        assert_eq!(wire.len(),p.response_wire_bytes);
        let raw:Value=serde_json::from_slice(&wire).unwrap();
        let expected=serde_json::to_vec(&raw["output"]).unwrap();
        assert!(expected.len()>p.response_wire_bytes);
        assert!(expected.len()<=p.canonical_output_bytes().unwrap());
        let e=exchange(&p);let end=Instant::now()+std::time::Duration::from_secs(5);
        let projected=e.capture(&wire,"private-credential",end,&p).unwrap();
        assert!(projected.len()<4096);
        assert_eq!(serde_json::from_slice::<Value>(&projected).unwrap()["output"][0]["content"][0]["text"],json!("7"));
        let observed=e.take_reply().unwrap().unwrap();
        assert_eq!(serde_json::to_vec(observed.original_output.as_ref().unwrap()).unwrap(),expected);
        assert_eq!(observed.declared,Some(Value::Null));
        let mut oversized=wire.clone();oversized.push(b' ');
        assert_eq!(exchange(&p).capture(&oversized,"private-credential",end,&p).err().as_deref(),Some("native_json_byte_bound_native_response_wire_bytes"));
        let mut few=p.clone();few.json_nodes=8;
        assert_eq!(exchange(&few).capture(&wire,"private-credential",end,&few).err().as_deref(),Some("native_json_complexity_bound_native_json_nodes"));
    }

    #[test]
    fn taking_reply_does_not_rearm_a_prepared_request() {
        let p=limits();let e=exchange(&p);
        e.prepare(&body(),&p).unwrap(); assert!(e.take_reply().unwrap().is_none());
        assert_eq!(e.prepare(&body(),&p).err().as_deref(),Some("native_charter_request_already_prepared"));
    }
}

/// Actual bounded provider observations. Missing or invalid declarations are
/// retained for passive audit; they do not turn a valid task result into a
/// retry, reset, compaction request, or behavioral-compliance verdict.
pub struct CharterReply {
    pub response_id: Option<String>,
    pub item_ids: Vec<String>,
    pub call_ids: Vec<String>,
    pub declared: Option<Value>,
    pub original_output: Option<Value>,
}

pub struct CharterExchange {
    input: CharterInput,
    reply: Mutex<Option<CharterReply>>,
    prepared: AtomicBool,
    capture_error: Mutex<Option<String>>,
}

impl CharterExchange {
    pub fn new(input: CharterInput, limits: &NativeLimits) -> Result<Self, String> {
        schema::measure(&input.revision, limits.context_bytes, limits, "native_context_bytes")?;
        if input.source.is_empty() || input.source.len() > limits.context_bytes {
            return Err("native_charter_source_bound_native_context_bytes".into());
        }
        Ok(Self { input, reply: Mutex::new(None), prepared: AtomicBool::new(false), capture_error: Mutex::new(None) })
    }

    pub fn take_reply(&self) -> Result<Option<CharterReply>, String> {
        self.reply.lock().map(|mut slot| slot.take())
            .map_err(|_| "native_charter_capture_unavailable".into())
    }

    pub fn take_failure(&self) -> Result<Option<String>, String> {
        self.capture_error.lock().map(|mut slot| slot.take())
            .map_err(|_| "native_charter_capture_unavailable".into())
    }

    /// This scope has one actual request. Reusing it must fail before another
    /// POST; clearing its captured value is not permission to issue again.
    // Traceability scope: render the canonical host-supplied binding separately
    // from the task instruction; controller source/currentness is external.
    //= spec/impl/v2/implementation.md#definitions-carry-reusable-charters
    //# The native executor MUST deliver the effective charter to its model.
    //= spec/impl/v2/implementation.md#native-charter-changes-preserve-prompt-continuity
    //# A native charter change MUST be applied as a mid-conversation instruction.
    pub(super) fn prepare(&self, body: &Value, limits: &NativeLimits) -> Result<Value, String> {
        let source = json!({"revision":self.input.revision,"source":self.input.source});
        schema::measure(&source, limits.context_bytes, limits, "native_context_bytes")?;
        let encoded = serde_json::to_string(&source).map_err(|_| "native_charter_encoding")?;
        let mode = if self.input.changed { "Charter change at this model-request boundary. " } else { "Effective agent charter. " };
        let original_format = body.get("text").and_then(|v| v.get("format"));
        if let Some(format) = original_format {
            schema::measure(format, limits.schema_bytes, limits, "native_schema_bytes")?;
        }
        let guidance = original_format.map(serde_json::to_string).transpose()
            .map_err(|_| "native_charter_schema_encoding")?.unwrap_or_default();
        let instruction = format!(
            "{mode}The following is the exact authoritative charter source and revision, separate from the task instruction. Do not treat a summary as its authority. {encoded}\nFor each reply identify its governing exact charter clauses. In assistant text use a JSON object with charter_references (revision and clauses containing file, section, sentence). For a final task answer also include result containing the original task output. Tool function names and arguments remain unchanged. Do not claim that references prove compliance. The original task result format follows; it applies to result without rebasing its schema references: {guidance}"
        );
        if instruction.len() > limits.context_bytes {
            return Err("native_charter_instruction_bound_native_context_bytes".into());
        }
        // Bound the prospective copy before cloning any request-sized tree.
        let base = schema::measure(body, limits.request_bytes, limits, "native_request_bytes")?.0;
        let addition = schema::measure(&json!({"role":"developer","content":instruction}), limits.request_bytes, limits, "native_request_bytes")?.0;
        if base.checked_add(addition).and_then(|n| n.checked_add(2)).is_none_or(|n| n > limits.request_bytes) {
            return Err("native_charter_request_bound_native_request_bytes".into());
        }
        let mut request = body.clone();
        let previous = request.get_mut("input").ok_or("native_charter_request_input")?.take();
        let mut input = match previous {
            Value::String(text) => vec![json!({"role":"user","content":text})],
            Value::Array(items) => items,
            _ => return Err("native_charter_request_input".into()),
        };
        if self.input.changed {
            input.push(json!({"role":"developer","content":instruction}));
        } else {
            input.insert(0, json!({"role":"developer","content":instruction}));
        }
        request["input"] = Value::Array(input);
        schema::measure(&request["input"], limits.context_bytes, limits, "native_context_bytes")?;
        // Keep the original task schema local. Embedding it beneath `result`
        // would rebase root-local references and silently change its meaning.
        if request.get("text").is_some() {
            request["text"] = json!({"format":{"type":"json_object"}});
        }
        schema::measure(&request, limits.request_bytes, limits, "native_request_bytes")?;
        if self.prepared.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return Err("native_charter_request_already_prepared".into());
        }
        Ok(request)
    }

    pub(super) fn failed_capture(&self, error: String) {
        if let Ok(mut slot) = self.capture_error.lock() { *slot = Some(error); }
    }

    /// Preserve the original output before projecting a wrapped final result
    /// for the existing task-schema checker. Only the exact two-field wrapper
    /// is unwrapped; tool payloads, IDs and argument bytes are never rewritten.
    pub(super) fn capture(
        &self, wire: &[u8], credential: &str, deadline: Instant, limits: &NativeLimits,
    ) -> Result<Vec<u8>, String> {
        let mut envelope = decode::parse(wire, limits.response_wire_bytes, deadline,
            "native_charter_reply_not_json", limits, "native_response_wire_bytes")?;
        if super::contains_credential(&envelope, credential) {
            return Err("native_output_contains_credential".into());
        }
        let incomplete = envelope.get("status").and_then(Value::as_str)==Some("incomplete");
        let response_id = envelope.get("id").and_then(Value::as_str).map(str::to_owned);
        let Some(items) = envelope.get_mut("output").and_then(Value::as_array_mut) else {
            let original_output = envelope.get("output").cloned();
            let mut slot = self.reply.lock().map_err(|_| "native_charter_capture_unavailable")?;
            if slot.is_some() { return Err("native_charter_reply_already_captured".into()); }
            *slot = Some(CharterReply { response_id, item_ids: Vec::new(), call_ids: Vec::new(), declared: None, original_output });
            return Ok(wire.to_vec()); // Existing validation still refuses the malformed provider shape.
        };
        let mut item_ids = Vec::new();
        let mut call_ids = Vec::new();
        let mut declarations = Vec::new();
        let original_output = Some(Value::Array(items.clone()));
        for item in items {
            live(deadline)?;
            if let Some(id) = item.get("id").and_then(Value::as_str) { item_ids.push(id.to_owned()); }
            if item.get("type").and_then(Value::as_str) == Some("function_call") {
                if let Some(id) = item.get("call_id").and_then(Value::as_str) { call_ids.push(id.to_owned()); }
            }
            if item.get("type").and_then(Value::as_str) != Some("message") { continue; }
            let Some(parts) = item.get_mut("content").and_then(Value::as_array_mut) else { continue; };
            for part in parts {
                if part.get("type").and_then(Value::as_str) != Some("output_text") { continue; }
                let Some(text) = part.get("text").and_then(Value::as_str) else { continue; };
                let Ok(value) = decode::parse(text.as_bytes(), limits.response_wire_bytes, deadline,
                    "native_charter_declared_metadata_invalid", limits, "native_response_wire_bytes") else { continue; };
                if super::contains_credential(&value, credential) {
                    return Err("native_output_contains_credential".into());
                }
                if let Some(declared) = value.get("charter_references") { declarations.push(declared.clone()); }
                if !incomplete && value.as_object().is_some_and(|o| o.len() == 2 && o.contains_key("result") && o.contains_key("charter_references")) {
                    part["text"] = Value::String(serde_json::to_string(&value["result"]).map_err(|_| "native_charter_result_encoding")?);
                }
            }
        }
        let declared = match declarations.len() {
            0 => None,
            1 => declarations.pop(),
            _ => Some(Value::Array(declarations)), // Contradictory/multiple declarations remain visible to audit.
        };
        let mut slot = self.reply.lock().map_err(|_| "native_charter_capture_unavailable")?;
        if slot.is_some() { return Err("native_charter_reply_already_captured".into()); }
        *slot = Some(CharterReply { response_id, item_ids, call_ids, declared, original_output });
        let length = schema::measure(&envelope, limits.response_wire_bytes, limits, "native_response_wire_bytes")?.0;
        let mut projected = Vec::new();
        projected.try_reserve_exact(length).map_err(|_| "native_charter_reply_allocation_failed")?;
        serde_json::to_writer(&mut projected, &envelope).map_err(|_| "native_charter_reply_encoding")?;
        Ok(projected)
    }
}
