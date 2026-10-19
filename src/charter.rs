//! Charter data carries source and audit identity, never execution authority.
//! Root registers these types in the existing host operation/receipt layer.
use crate::mcp::Caller;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct CharterSourceRef { pub definition_id: String, pub definition_revision: String }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag="kind", rename_all="snake_case", deny_unknown_fields)]
pub enum DefinitionCharter {
    /// These complete source bytes belong to this immutable Definition revision.
    Inline { source: CharterSource },
    /// Reuse the original retained definition's complete source identity.
    Reference { source: CharterSourceRef },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CharterFile { pub file: String, pub text: String }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct CharterClauseRef {
    pub file: String,
    /// Exact heading text in the supported authored Markdown source; not a path.
    pub section: String,
    /// Exact sentence, with source line wrapping joined by one space.
    pub sentence: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CharterSource { pub files: Vec<CharterFile>, pub clauses: Vec<CharterClauseRef> }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResolvedCharter { pub reference: CharterSourceRef, pub source: CharterSource }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeclaredCharterReferences { pub revision: CharterSourceRef, pub clauses: Vec<CharterClauseRef> }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag="kind", rename_all="snake_case", deny_unknown_fields)]
pub enum CharterAuditFinding {
    MissingMetadata, MalformedMetadata, EmptyApplicabilityUnspecified,
    RevisionMismatch, UnknownClause { index: usize }, DuplicateClause { index: usize },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CharterAudit {
    /// Empty means tuple resolution succeeded, never behavioral compliance.
    pub findings: Vec<CharterAuditFinding>,
}
/// Exact deterministic JSON bytes of one original output Value. This is an
/// occurrence-bound locator, never permission to execute or infer a reply.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CharterOutputReference {
    pub request_id: String,
    pub bytes: u64,
    pub sha256: String,
    pub chunk_bytes: u32,
    pub chunks: u32,
}
impl CharterOutputReference {
    pub fn validate(&self, maximum: usize, chunk_maximum: usize) -> Result<(), String> {
        if self.request_id.is_empty() || self.bytes == 0 || self.bytes > maximum as u64
            || self.chunk_bytes == 0 || self.chunk_bytes as usize > chunk_maximum
            || self.chunks == 0 || u64::from(self.chunks) != self.bytes.div_ceil(u64::from(self.chunk_bytes))
            || self.sha256.len() != 64 || !self.sha256.bytes().all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v)) {
            return Err("invalid original charter output reference".into());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CharterOutputChunk {
    pub reference: CharterOutputReference,
    pub index: u32,
    /// Lowercase hexadecimal preserves arbitrary UTF-8 boundaries exactly.
    pub data: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CharterOutputChunkReceipt {
    pub reference: CharterOutputReference,
    pub index: u32,
    pub sha256: String,
    pub bytes: u32,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CharterReplyObservation {
    /// Exact original provider output field. None is absent; Some(Null) is explicit null.
    /// This is bounded reply content, not the complete transport envelope.
    #[serde(default, skip_serializing_if="Option::is_none", deserialize_with="present_value")]
    pub output: Option<Value>,
    /// Present only when output is retained losslessly in bounded existing-store
    /// chunks. Both fields absent means absent output; a chunk encoding `null`
    /// preserves explicit null. Historical inline rows are never rewritten.
    #[serde(default, skip_serializing_if="Option::is_none")]
    pub output_reference: Option<CharterOutputReference>,
    /// Actual provider ID if present; host occurrence identity remains separate.
    pub response_id: Option<String>,
    pub output_item_ids: Vec<String>,
    /// Exact ordered provider tool-call IDs, including all calls in one reply.
    pub tool_call_ids: Vec<String>,
    /// Preserve original parsed value, including strings/numbers/wrong shapes.
    /// Missing metadata is None and never replaced by inferred governing clauses.
    #[serde(default, skip_serializing_if="Option::is_none", deserialize_with="present_value")]
    pub declaration: Option<Value>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CharterSession {
    pub session_id: String,
    pub revision: u64,
    pub initial: CharterSourceRef,
    pub effective: CharterSourceRef,
    pub pending: Option<CharterSourceRef>,
    pub last_turn_id: Option<String>,
    pub compaction_due: Option<CharterSourceRef>,
    pub compaction_in_flight: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EffectiveCharter { pub session: CharterSession, pub charter: ResolvedCharter }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CharterBoundary {
    /// The canonical BeginTurn operation ID; never a fabricated provider ID.
    pub turn_id: String,
    pub session_id: String,
    pub caller: Caller,
    /// One-based actual charged model request count, including compactions.
    pub request_index: u32,
    pub binding_revision: u64,
    pub effective: ResolvedCharter,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CharterReplyRecord {
    pub reply_id: String,
    pub turn_id: String,
    pub effective: CharterSourceRef,
    pub observation: CharterReplyObservation,
    pub audit: CharterAudit,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag="kind", rename_all="snake_case", deny_unknown_fields)]
pub enum CharterResultReceipt {
    Collaboration { call_id: String, operation_id: String },
    CurrentTask { call_id: String, report_id: String },
}
impl CharterResultReceipt { pub fn call_id(&self)->&str { match self { Self::Collaboration{call_id,..}|Self::CurrentTask{call_id,..}=>call_id } } }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CharterTurn {
    pub boundary: CharterBoundary,
    pub reply: Option<CharterReplyRecord>,
    pub completed_by: Option<String>,
    pub delivered_results: Vec<CharterResultReceipt>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all="snake_case")]
pub enum CompactionDisposition { CurrentSourceSummary, ObsoleteSourceSummary, NoAcceptedSummary }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CharterCompaction {
    pub run_id: String,
    pub session_id: String,
    pub caller: Caller,
    pub request_index: u32,
    pub effective: ResolvedCharter,
    pub reply: Option<CharterReplyRecord>,
    pub summary: Option<String>,
    pub disposition: Option<CompactionDisposition>,
}
/// An initial supplied-executor context, not a model request or model reply.
/// The original intent survives an unknown thread/start response. Its existence
/// never authorizes another start; recovery queries the same operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CharterSuppliedContext {
    pub delivery_id: String,
    pub session_id: String,
    pub caller: Caller,
    pub binding_revision: u64,
    pub effective: ResolvedCharter,
    pub request_id: u64,
    pub developer_instructions: String,
    pub acknowledgment: Option<CharterSuppliedAcknowledgment>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CharterSuppliedAcknowledgment {
    pub operation_id: String,
    pub request_id: u64,
    pub thread_id: String,
    /// Exact bounded thread/start result, not a model response or compliance proof.
    pub response: Value,
}
/// A public supplied outer turn, never an internal model request or native charge.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CharterSuppliedBoundary {
    pub turn_id: String,
    pub delivery_id: String,
    pub session_id: String,
    pub caller: Caller,
    pub outer_index: u32,
    pub effective: ResolvedCharter,
    pub request_id: u64,
    pub thread_id: String,
    /// The schema sent in this exact public turn/start. Product TaskInput schema
    /// remains separate and is never moved under a different JSON reference root.
    pub output_schema: Value,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CharterSuppliedTurnAck {
    pub operation_id: String,
    pub request_id: u64,
    pub turn_id: String,
    pub response: Value,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CharterSuppliedTerminal {
    pub thread_id: String,
    pub turn_id: String,
    pub status: String,
    /// Exact public error field; absent and explicit null remain distinct.
    #[serde(default, skip_serializing_if="Option::is_none", deserialize_with="present_value")]
    pub error: Option<Value>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CharterSuppliedTurn {
    pub boundary: CharterSuppliedBoundary,
    pub acknowledgment: Option<CharterSuppliedTurnAck>,
    pub reply: Option<CharterReplyRecord>,
    pub terminal: Option<CharterSuppliedTerminal>,
}
/// Only final-message carriage is constrained. The result is still ordinary
/// supplied-executor text; original task-result validation is not replaced.
/// Null/malformed/missing declarations in actual events remain passive findings.
pub fn supplied_result_schema() -> Value {
    serde_json::json!({"type":"object","additionalProperties":false,
        "properties":{"result":{"type":"string"},"charter_references":{"anyOf":[
            {"type":"null"},{"type":"object","additionalProperties":false,
             "properties":{"revision":{"type":"object","additionalProperties":false,
                "properties":{"definition_id":{"type":"string"},"definition_revision":{"type":"string"}},
                "required":["definition_id","definition_revision"]},
             "clauses":{"type":"array","items":{"type":"object","additionalProperties":false,
                "properties":{"file":{"type":"string"},"section":{"type":"string"},"sentence":{"type":"string"}},
                "required":["file","section","sentence"]}}},"required":["revision","clauses"]}]}},
        "required":["result","charter_references"]})
}
pub fn supplied_result_instructions() -> &'static str {
    "For this public outer turn, return your final answer in result and your genuinely applicable delivered-charter references in charter_references. Use null when no declaration is available. References are passive audit metadata, never proof of compliance. Do not invent references or claim visibility of hidden internal model replies."
}
/// Aggregate of at most reply_items complete public item frames plus one
/// terminal frame. Canonical finite JSON numbers can expand to 24 bytes; the
/// extra factor bounds parsed encoding without enlarging any received frame.
/// This derives from accepted limits, not a new policy default.
pub(crate) fn supplied_frame_bytes(frame_bytes: usize) -> usize {
    // A derived maximum beyond the addressable byte extent cannot exclude a
    // representable value. Keep checking the actual serialization at that
    // extent instead of rejecting small values under a large configured cap.
    frame_bytes.saturating_mul(25).min(isize::MAX as usize)
}
pub(crate) fn supplied_output_bytes(frame_bytes: usize, reply_items: usize) -> Result<usize, String> {
    Ok(supplied_frame_bytes(frame_bytes).saturating_add(1)
        .saturating_mul(reply_items.saturating_add(1)).saturating_add(64)
        .min(isize::MAX as usize))
}

/// One deterministic source projection shared by the trusted producer and store.
/// It asks for references without manufacturing any model-declared metadata.
pub fn supplied_context_instructions(charter: &ResolvedCharter) -> Result<String, serde_json::Error> {
    Ok(format!("The following authoritative charter governs this context. Preserve its exact source revision and clause identities. Include the applicable charter references in each model reply; references are audit information, not proof of compliance.\n{}", serde_json::to_string(charter)?))
}
// Serde's ordinary Option<Value> decoder collapses explicit null into absence.
// Missing fields use default None; a present field, including null, stays Some.
fn present_value<'de,D:serde::Deserializer<'de>>(d:D)->Result<Option<Value>,D::Error>{Value::deserialize(d).map(Some)}
/// Trusted configuration route only; never added to the agent MCP catalogue.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag="kind", rename_all="snake_case", deny_unknown_fields)]
pub enum CharterMutation {
    Publish { agent_id: String, expected_definition_revision: String, expected_global_revision: String, expected_governance_revision: String, charter: DefinitionCharter },
    SelectForFutureTasks { agent_id: String, expected_definition_revision: String, expected_global_revision: String, expected_governance_revision: String, source: CharterSourceRef },
    SetPending { session_id: String, expected_revision: u64, target: CharterSourceRef },
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all="snake_case")]
pub enum CharterDefinitionEffect { PublicationOnly, FutureSubmissions }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CharterDefinitionOutcome {
    pub previous_revision: String,
    pub definition: crate::protocol::Definition,
    pub source: CharterSourceRef,
    pub effect: CharterDefinitionEffect,
}
/// Host worker observation route: new requests require a current Caller; late
/// audit requires the exact retained original occurrence and grants no execution.
/// There is no model-supplied safe-boundary flag or caller identity in metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag="kind", rename_all="snake_case", deny_unknown_fields)]
pub enum CharterObservation {
    OutputChunk { chunk: CharterOutputChunk },
    BeginSuppliedContext { session_id: String, expected_revision: u64, source: CharterSourceRef, request_id: u64, developer_instructions: String },
    AckSuppliedContext { delivery_id: String, request_id: u64, response: Value },
    BeginSuppliedTurn { delivery_id: String, outer_index: u32, request_id: u64, thread_id: String, output_schema: Value },
    AckSuppliedTurn { turn_id: String, request_id: u64, response: Value },
    SuppliedReply { turn_id: String, terminal: CharterSuppliedTerminal, reply: CharterReplyObservation },
    BeginTurn { session_id: String, expected_revision: u64, request_index: u32 },
    Reply { turn_id: String, reply: CharterReplyObservation },
    CompleteTurn { turn_id: String, results: Vec<CharterResultReceipt> },
    BeginCompaction { session_id: String, expected_revision: u64, request_index: u32 },
    CompactionReply { run_id: String, reply: CharterReplyObservation, summary: Option<String> },
    /// Reconcile the exact committed provider rejection; it is not a model reply.
    CompactionRejected { run_id: String, report_id: String },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag="kind", content="value", rename_all="snake_case", deny_unknown_fields)]
pub enum CharterOutcome { Definition(CharterDefinitionOutcome), Session(EffectiveCharter), Boundary(CharterBoundary), Reply(CharterReplyRecord), Turn(CharterTurn), Compaction(CharterCompaction), SuppliedContext(CharterSuppliedContext), SuppliedTurn(CharterSuppliedTurn), OutputChunk(CharterOutputChunkReceipt) }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag="kind", rename_all="snake_case", deny_unknown_fields)]
pub enum CharterQuery {
    OutputChunk { request_id: String, index: u32 },
    SuppliedContext { delivery_id: String },
    SuppliedTurn { turn_id: String },
    Source { source: CharterSourceRef }, Session { session_id: String },
    Turn { turn_id: String }, Compaction { run_id: String },
    Turns { session_id: String, after: Option<String>, limit: usize },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag="kind", content="value", rename_all="snake_case", deny_unknown_fields)]
pub enum CharterQueryResult {
    OutputChunk(CharterOutputChunk),
    SuppliedContext(CharterSuppliedContext),
    SuppliedTurn(CharterSuppliedTurn),
    Source(ResolvedCharter), Session(Option<EffectiveCharter>), Turn(CharterTurn),
    Compaction(CharterCompaction), Turns(crate::collaboration::Page<CharterTurn>),
}
