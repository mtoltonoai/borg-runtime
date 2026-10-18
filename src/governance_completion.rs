//! Closed worker/controller completion exchanges. They carry original receipt
//! identities and genuine executor boundaries, never caller-selected authority.
use crate::{governance::{CompletionDecision, DecisionRef}, mcp::Caller};
use serde::{Deserialize,Serialize};

#[derive(Debug,Clone,Serialize,Deserialize,PartialEq,Eq)]
#[serde(tag="kind",rename_all="snake_case",deny_unknown_fields)]
pub enum CompletionBoundary {
    NativeResponse {request_index:u32},
    CodexTurn {thread_id:String,turn_id:String},
    /// Controller-owned final candidate boundary; never accepted by EvaluateCompletion.
    StoppedResult {report_id:String},
}

#[derive(Debug,Clone,Serialize,Deserialize,PartialEq,Eq)]
#[serde(tag="kind",rename_all="snake_case",deny_unknown_fields)]
pub enum CompletionTarget {
    NativeRequest {request_index:u32},
    InitialNativeRequest {request_index:u32},
    InitialCodexTurn {thread_id:String},
    /// The next turn ID is not known before the one original turn/start.
    CodexTurn {thread_id:String,previous_turn_id:String},
}

#[derive(Debug,Clone,Serialize,Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
pub struct EvaluateCompletion {
    pub operation_id:String,
    pub caller:Caller,
    pub boundary:CompletionBoundary,
    #[serde(default,skip_serializing_if="Vec::is_empty")]
    pub intervening_requests:Vec<crate::native_loop_contract::NativeRequestEvidence>,
}
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
pub struct BeginCompletionContinuation {
    pub operation_id:String,
    pub caller:Caller,
    pub decision:DecisionRef,
    pub context_binding:String,
    pub target:CompletionTarget,
    /// Optional exact existing workflow batch included in this same actual
    /// input. It is not a waiver for another pending append or caller.
    pub delivery_batch_id:Option<String>,
}
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
pub struct AcknowledgeCompletionInput {
    pub operation_id:String,
    pub caller:Caller,
    pub begin_operation_id:String,
    pub consumed_at:CompletionBoundary,
    /// Exact successful original workflow ACK when this input carried its batch.
    pub delivery_ack_operation_id:Option<String>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub native_input:Option<NativeCompletionInput>,
}

/// Initial input of a new ordinarily admitted attempt after verified prior
/// stop. The previous exact decision remains the reason; admission is not ACK.
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
pub struct CompletionResume {
    pub evaluation:CompletionEvaluation,
    /// Original immutable required-input event; admission is neither reset nor ACK.
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub reset_event:Option<crate::collaboration::workflow::EventId>,
}

/// Exact retained input intent. Unknown dispatch never creates another Begin.
/// Acknowledgement is supplied only by the supported actual consumer boundary.
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
pub struct CompletionContinuation {
    pub begin_operation_id:String,
    pub caller:Caller,
    pub decision:DecisionRef,
    pub context_binding:String,
    pub target:CompletionTarget,
    pub delivery_batch_id:Option<String>,
    pub acknowledged:Option<CompletionInputAck>,
}
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
pub struct CompletionInputAck {
    pub operation_id:String,
    pub consumed_at:CompletionBoundary,
    /// Exact successful original workflow ACK when this input carried its batch.
    pub delivery_ack_operation_id:Option<String>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub native_input:Option<NativeCompletionInput>,
}

#[derive(Debug,Clone,Serialize,Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
pub struct CompletionEvaluation {
    pub decision:DecisionRef,
    pub state:CompletionDecision,
    pub boundary:CompletionBoundary,
}
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
pub struct CompletionFeedback {
    pub evaluation:CompletionEvaluation,
    pub continuation:CompletionContinuation,
}
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq,Eq)]
#[serde(tag="kind",content="value",rename_all="snake_case",deny_unknown_fields)]
pub enum CompletionOutcome {
    /// No selected enforcing rule; this preserves ordinary behavior and never
    /// authorizes a governance-caused continuation or a fabricated shadow run.
    NotEnforced,
    Evaluated(CompletionEvaluation),
    Begun(CompletionFeedback),
    InputAcknowledged(CompletionContinuation),
}

/// Canonical rejected POSTs before the actual consuming response. Every proof
/// keeps the fixed Begin target and the same exact serialized request binding.
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeCompletionInput {
    pub request_binding:crate::native::ProviderRequestBinding,
    pub rejections:Vec<NativeCompletionRejection>,
}
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeCompletionRejection {
    pub request_index:u32,
    pub operation_id:String,
}

/// Evidence for a genuine no-tool transition in the existing native loop.
/// The initial rejected final response and next actual response remain distinct.
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeCompletionContinuationEvidence {
    pub request_index:u32,
    pub decision:DecisionRef,
    pub begin_operation_id:String,
    pub acknowledgement_operation_id:String,
}

fn identity<T:Serialize>(kind:&str,value:&T)->Result<String,serde_json::Error> {
    let bytes=serde_json::to_vec(&(kind,value))?;
    let hash=aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256,&bytes);
    Ok(format!("completion-{kind}-{}",hash.as_ref().iter().map(|b|format!("{b:02x}")).collect::<String>()))
}
/// One original decision per exact actual executor boundary, even if a client
/// attempts to substitute another operation ID. These are identities, not grants.
pub fn evaluation_id(caller:&Caller,boundary:&CompletionBoundary)->Result<String,serde_json::Error> {
    identity("evaluate",&(caller,boundary))
}
pub fn begin_id(caller:&Caller,decision:&DecisionRef)->Result<String,serde_json::Error> {
    identity("begin",&(caller,decision))
}
pub fn acknowledgement_id(caller:&Caller,begin_operation_id:&str)->Result<String,serde_json::Error> {
    identity("ack",&(caller,begin_operation_id))
}
