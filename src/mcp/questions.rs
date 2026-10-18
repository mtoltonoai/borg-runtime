//! Closed question tool adapter. Caller provenance and original occurrence are
//! supplied by the existing controller call transaction, never these arguments.
use super::Tool;
use crate::{collaboration::{questions::*,workflow::*},limits::RuntimeLimits};
use serde::Deserialize;
use serde_json::{Value,json};
#[derive(Debug,Deserialize)]
#[serde(tag="tool",content="arguments",deny_unknown_fields)]
enum Args {
    #[serde(rename="collab_question_pose")] Pose{question:PoseQuestion},
    #[serde(rename="collab_question_answer")] Answer{question_id:String,expected_revision:u64,answer:QuestionAnswer},
    #[serde(rename="collab_question_cancel")] Cancel{question_id:String,expected_revision:u64,reason:String},
    #[serde(rename="collab_question_supersede")] Supersede{question_id:String,expected_revision:u64,reason:String,replacement:PoseQuestion},
    #[serde(rename="collab_answer_replace")] Replace{question_id:String,expected_revision:u64,expected_answer_id:String,answer:QuestionAnswer,reason:String},
    #[serde(rename="collab_answer_withdraw")] Withdraw{question_id:String,expected_revision:u64,expected_answer_id:String,reason:String},
    #[serde(rename="collab_answer_mark_moot")] Moot{question_id:String,expected_revision:u64,expected_answer_id:String,caused_by:AnswerRef,reason:String},
    #[serde(rename="collab_question_get")] Get{question_id:String},
    #[serde(rename="collab_questions_list")] List{record:Option<RecordRef>,status:Option<QuestionStatus>,after:Option<String>,limit:usize},
    #[serde(rename="collab_question_answers")] Answers{question_id:String,after:Option<String>,limit:usize},
    #[serde(rename="collab_question_actions")] Actions{question_id:String,after:Option<String>,limit:usize},
    #[serde(rename="collab_decisions_list")] Decisions{after:Option<String>,limit:usize},
}
pub(crate) enum Command {Mutation(QuestionMutation),Query(QuestionQuery)}
pub(crate) const NAMES:[&str;12]=["collab_question_pose","collab_question_answer","collab_question_cancel","collab_question_supersede","collab_answer_replace","collab_answer_withdraw","collab_answer_mark_moot","collab_question_get","collab_questions_list","collab_question_answers","collab_question_actions","collab_decisions_list"];
pub(crate) fn handles(name:&str)->bool{NAMES.contains(&name)}
/// Structural adapter only. The domain checks current configured UTF-8, record,
/// page and identity limits inside the original receipt transaction. No agent
/// argument selects the viewer or author; Decisions uses established caller.
pub(crate) fn parse(name:&str,arguments:&Value,actor:&RecordedActor)->Result<Command,String>{
    let args:Args=serde_json::from_value(json!({"tool":name,"arguments":arguments})).map_err(|_|"question tool arguments differ from closed schema")?;
    Ok(match args {
        Args::Pose{question}=>Command::Mutation(QuestionMutation::Pose{question}),
        Args::Answer{question_id,expected_revision,answer}=>Command::Mutation(QuestionMutation::Answer{question_id,expected_revision,answer}),
        Args::Cancel{question_id,expected_revision,reason}=>Command::Mutation(QuestionMutation::Cancel{question_id,expected_revision,reason}),
        Args::Supersede{question_id,expected_revision,reason,replacement}=>Command::Mutation(QuestionMutation::Supersede{question_id,expected_revision,reason,replacement}),
        Args::Replace{question_id,expected_revision,expected_answer_id,answer,reason}=>Command::Mutation(QuestionMutation::SupersedeAnswer{question_id,expected_revision,expected_answer_id,answer,reason}),
        Args::Withdraw{question_id,expected_revision,expected_answer_id,reason}=>Command::Mutation(QuestionMutation::WithdrawAnswer{question_id,expected_revision,expected_answer_id,reason}),
        Args::Moot{question_id,expected_revision,expected_answer_id,caused_by,reason}=>Command::Mutation(QuestionMutation::MarkMoot{question_id,expected_revision,expected_answer_id,caused_by,reason}),
        Args::Get{question_id}=>Command::Query(QuestionQuery::Get{question_id}),
        Args::List{record,status,after,limit}=>Command::Query(QuestionQuery::List{record,status,after,limit}),
        Args::Answers{question_id,after,limit}=>Command::Query(QuestionQuery::Answers{question_id,after,limit}),
        Args::Actions{question_id,after,limit}=>Command::Query(QuestionQuery::Actions{question_id,after,limit}),
        Args::Decisions{after,limit}=>Command::Query(QuestionQuery::DecisionQueue{viewer:actor.principal.clone(),after,limit}),
    })
}
fn object(fields:Vec<(&str,Value)>)->Value {let required=fields.iter().map(|(k,_)|Value::String((*k).into())).collect::<Vec<_>>();let properties=fields.into_iter().map(|(k,v)|(k.into(),v)).collect::<serde_json::Map<_,_>>();json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})}
fn nullable(value:Value)->Value {json!({"anyOf":[value,{"type":"null"}]})}
fn text(bytes:usize,field:&str)->Value {json!({"type":"string","x-borg-maxUtf8Bytes":bytes,"x-borg-limit-field":field})}
fn tagged(kind:&str,name:&str,value:Value)->Value {object(vec![("kind",json!({"type":"string","enum":[kind]})),(name,value)])}
/// Extend the catalogue AFTER the existing flat-property legacy limits loop.
/// These nested fields retain their own metadata mapping; they are not all IDs.
pub(crate) fn catalogue(limits:&RuntimeLimits)->Vec<Tool>{
    let Some(w)=&limits.workflow else{return vec![]};let id=text(limits.transport.identity_bytes,"transport.identity_bytes");let words=text(w.text_bytes,"workflow.text_bytes");let after=nullable(text(limits.controller.collaboration_cursor_bytes,"controller.collaboration_cursor_bytes"));let count=json!({"type":"integer","minimum":1,"x-borg-maximum":w.page_items,"x-borg-limit-field":"workflow.page_items"});let rev=json!({"type":"integer","minimum":1,"maximum":i64::MAX});
    let record=json!({"anyOf":[tagged("task","task_id",id.clone()),tagged("question","question_id",id.clone()),tagged("document","document_id",id.clone()),tagged("review","review_id",id.clone())]});
    let principal=json!({"anyOf":[tagged("agent","agent_id",id.clone()),object(vec![("kind",json!({"type":"string","enum":["trusted_host_operator"]}))])]});
    let option=object(vec![("id",id.clone()),("label",words.clone()),("explanation",words.clone())]);
    let pose=object(vec![("record",record.clone()),("recipient",principal),("prompt",words.clone()),("options",json!({"type":"array","items":option,"minItems":1,"x-borg-maxItems":w.options,"x-borg-limit-field":"workflow.options"})),("recommended_option_id",id.clone())]);
    let answer=object(vec![("option_id",id.clone()),("explanation",words.clone())]);let reference=object(vec![("question_id",id.clone()),("answer_id",id.clone())]);
    let schemas=vec![
        object(vec![("question",pose.clone())]),
        object(vec![("question_id",id.clone()),("expected_revision",rev.clone()),("answer",answer.clone())]),
        object(vec![("question_id",id.clone()),("expected_revision",rev.clone()),("reason",words.clone())]),
        object(vec![("question_id",id.clone()),("expected_revision",rev.clone()),("reason",words.clone()),("replacement",pose)]),
        object(vec![("question_id",id.clone()),("expected_revision",rev.clone()),("expected_answer_id",id.clone()),("answer",answer),("reason",words.clone())]),
        object(vec![("question_id",id.clone()),("expected_revision",rev.clone()),("expected_answer_id",id.clone()),("reason",words.clone())]),
        object(vec![("question_id",id.clone()),("expected_revision",rev),("expected_answer_id",id.clone()),("caused_by",reference),("reason",words)]),
        object(vec![("question_id",id.clone())]),
        object(vec![("record",nullable(record)),("status",nullable(json!({"type":"string","enum":["open","answered","cancelled","superseded","moot"]}))),("after",after.clone()),("limit",count.clone())]),
        object(vec![("question_id",id.clone()),("after",after.clone()),("limit",count.clone())]),
        object(vec![("question_id",id),("after",after.clone()),("limit",count.clone())]),
        object(vec![("after",after),("limit",count)]),
    ];
    NAMES.into_iter().zip(schemas).enumerate().map(|(i,(name,input_schema))|Tool{name:name.into(),description:match i{0=>"Pose one routed question linked to an existing record; the controller appends the selected option ID and label to the immutable prompt. For an operator recipient, offer choices about what should happen. Do not describe steps for the operator to perform by hand.",1=>"Answer as this bound session's own agent identity; no impersonation or default answer.",2=>"Cancel an open question with an attributed reason.",3=>"Replace an open question with a linked new question under this caller. For an operator recipient, offer choices about what should happen. Do not describe steps for the operator to perform by hand.",4=>"Append a replacement for your own recorded answer; preserve the original.",5=>"Withdraw your own answer's effect without deleting its record.",6=>"Remove an answer's product effect, citing the answer that made it moot. This grants no authority.",7=>"Read question, effective answer and current responsible record owner.",8=>"Read bounded questions; continuation is query-scoped and does not freeze later lifecycle changes.",9=>"Read immutable answer history.",10=>"Read attributed cancellation, supersession and answer-effect history.",_=>"Read all supported open questions and pending reviews owed by this bound caller, with bounded continuation."}.into(),input_schema,annotations:json!({"readOnlyHint":i>=7,"destructiveHint":false,"idempotentHint":false,"openWorldHint":false})}).collect()
}
/// Root routes only these names here before its legacy flat-field validator.
/// Structural failures are malformed input. Configured byte/count refusals are
/// produced by the domain with exact workflow.* fields in the durable outcome.
pub(crate) fn validate_arguments(name:&str,arguments:&Value,limits:&RuntimeLimits)->Result<(),String>{
    let tool=catalogue(limits).into_iter().find(|t|t.name==name).ok_or("question tools are disabled")?;
    let validator=jsonschema::options().with_draft(jsonschema::Draft::Draft202012).build(&tool.input_schema).map_err(|_|"invalid question tool schema")?;
    if !validator.is_valid(arguments){return Err("question tool arguments differ from closed schema".into())}Ok(())
}
