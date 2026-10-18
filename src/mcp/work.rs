//! Closed adapter for task coordination; actor authority is supplied by the
//! current controller call context, never by an argument or a selected owner.
use super::Tool;
use crate::{collaboration::{work::*,workflow::*},limits::RuntimeLimits};
use serde::Deserialize;
use serde_json::{Value,json};

#[derive(Debug,Deserialize)]
#[serde(tag="tool",content="arguments",deny_unknown_fields)]
enum Args {
    #[serde(rename="collab_task_coordination_get")] Get {task_id:String},
    #[serde(rename="collab_task_waits")] Waits {task_id:String,after:Option<String>,limit:usize},
    #[serde(rename="collab_task_wait")] Add {task_id:String,expected_revision:u64,resolver:ResolverRef},
    #[serde(rename="collab_task_unwait")] Remove {task_id:String,expected_revision:u64,wait_id:String,reason:String},
    #[serde(rename="collab_task_owner")] Owner {task_id:String,expected_revision:u64,owner:PrincipalRef},
}
pub(crate) enum Command {Mutation(WorkMutation),Query(WorkQuery)}
pub(crate) const NAMES:[&str;5]=["collab_task_coordination_get","collab_task_waits","collab_task_wait","collab_task_unwait","collab_task_owner"];
pub(crate) fn handles(name:&str)->bool {NAMES.contains(&name)}
pub(crate) fn parse(name:&str,arguments:&Value)->Result<Command,String> {
    let input:Args=serde_json::from_value(json!({"tool":name,"arguments":arguments})).map_err(|_|"task coordination arguments differ from closed schema")?;
    Ok(match input {
        Args::Get{task_id}=>Command::Query(WorkQuery::Get{task_id}),
        Args::Waits{task_id,after,limit}=>Command::Query(WorkQuery::Waits{task_id,after,limit}),
        Args::Add{task_id,expected_revision,resolver}=>Command::Mutation(WorkMutation::AddWait{task_id,expected_revision,resolver}),
        Args::Remove{task_id,expected_revision,wait_id,reason}=>Command::Mutation(WorkMutation::RemoveWait{task_id,expected_revision,wait_id,reason}),
        Args::Owner{task_id,expected_revision,owner}=>Command::Mutation(WorkMutation::ChangeOwner{task_id,expected_revision,owner}),
    })
}
/// Historical disposition of this exact successful call. A worker may manage
/// another owned task without suspending its own execution.
pub(crate) fn requires_quiescence(call:&super::Invocation,outcome:&crate::protocol::Outcome)->bool {
    let Ok(Command::Mutation(WorkMutation::AddWait{task_id,..}))=parse(&call.tool,&call.arguments) else{return false};
    if task_id!=call.caller.task_id {return false}
    matches!(outcome,crate::protocol::Outcome::Ok{data:crate::protocol::Data::WorkMutation(result)}
        if result.view.task.id==task_id && result.view.open_waits>0
        && result.view.coordination_blocked
        && result.changed_wait.as_ref().is_some_and(|wait|wait.task_id==task_id))
}
fn object(fields:Vec<(&str,Value)>)->Value {
    let required=fields.iter().map(|(name,_)|Value::String((*name).into())).collect::<Vec<_>>();
    let properties=fields.into_iter().map(|(name,value)|(name.into(),value)).collect::<serde_json::Map<_,_>>();
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}
fn tagged(kind:&str,field:&str,value:Value)->Value {object(vec![("kind",json!({"type":"string","enum":[kind]})),(field,value)])}
fn text(bytes:usize,field:&str)->Value {json!({"type":"string","x-borg-maxUtf8Bytes":bytes,"x-borg-limit-field":field})}
pub(crate) fn catalogue(limits:&RuntimeLimits)->Vec<Tool> {
    let Some(policy)=limits.workflow.as_ref() else{return vec![]};
    let id=text(limits.transport.identity_bytes,"transport.identity_bytes");
    let revision=json!({"type":"integer","minimum":0,"maximum":i64::MAX});
    let resolver=json!({"anyOf":[tagged("task","task_id",id.clone()),tagged("question","question_id",id.clone()),tagged("review","review_id",id.clone())]});
    let owner=json!({"anyOf":[tagged("agent","agent_id",id.clone()),object(vec![("kind",json!({"type":"string","enum":["trusted_host_operator"]}))])]});
    let schemas=vec![
        object(vec![("task_id",id.clone())]),
        object(vec![("task_id",id.clone()),("after",json!({"anyOf":[id.clone(),{"type":"null"}]})),("limit",json!({"type":"integer","minimum":1,"x-borg-maximum":policy.page_items,"x-borg-limit-field":"workflow.page_items"}))]),
        object(vec![("task_id",id.clone()),("expected_revision",revision.clone()),("resolver",resolver)]),
        object(vec![("task_id",id.clone()),("expected_revision",revision.clone()),("wait_id",id.clone()),("reason",text(policy.text_bytes,"workflow.text_bytes"))]),
        object(vec![("task_id",id),("expected_revision",revision),("owner",owner)]),
    ];
    let descriptions=[
        "Read coherent task execution state, current responsible owner and separate workflow waits. This is not a launch permission.",
        "Read bounded task wait history with exact resolver outcomes and continuation.",
        "Record a wait on an open concrete task, question or review. Waiting on the calling task quiesces its execution; coordinating another owned task does not. Immutable execution prerequisites stay unchanged.",
        "Remove one open wait with an attributed reason. Removal does not grant permission or reconcile uncertain effects.",
        "Change this record's responsible owner while retaining accepted execution identity and all open questions.",
    ];
    NAMES.into_iter().zip(schemas).zip(descriptions).enumerate().map(|(i,((name,input_schema),description))|Tool {
        name:name.into(),description:description.into(),input_schema,
        annotations:json!({"readOnlyHint":i<2,"destructiveHint":false,"idempotentHint":false,"openWorldHint":false}),
    }).collect()
}
pub(crate) fn validate_arguments(name:&str,arguments:&Value,limits:&RuntimeLimits)->Result<(),String> {
    let tool=catalogue(limits).into_iter().find(|tool|tool.name==name).ok_or("task coordination tools are disabled")?;
    let validator=jsonschema::options().with_draft(jsonschema::Draft::Draft202012).build(&tool.input_schema).map_err(|_|"invalid task coordination schema")?;
    if !validator.is_valid(arguments) {return Err("task coordination arguments differ from closed schema".into())}
    Ok(())
}
