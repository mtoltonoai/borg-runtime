//! Metadata-rendered document/review tools. Root adds these only to the explicit
//! new accepted workflow profile; no existing task profile acquires them.
use super::Tool;
use crate::limits::RuntimeLimits;
use serde_json::{json,Value};
pub(crate) fn owns(name:&str)->bool {
    matches!(name,"collab_document_create"|"collab_document_version_create"|"collab_document_owner_set"|"collab_document_get"|"collab_documents_list"|"collab_document_versions_list"|"collab_comment_post"|"collab_comments_list"|"collab_review_create"|"collab_review_get"|"collab_reviews_list"|"collab_review_target_get"|"collab_review_decide"|"collab_review_withdraw")
}
// Configurable policy bounds are descriptive here and are enforced by the
// canonical domain operation, which returns the exact limiting metadata field.
// Structural errors, including fixed representation widths, remain schema errors.
fn string(bytes:usize,nonempty:bool,field:&str)->Value {json!({"type":"string","minLength":if nonempty{1}else{0},"x-borg-maxUtf8Bytes":bytes,"x-borg-limit-field":field})}
fn integer()->Value {json!({"type":"integer","minimum":1,"maximum":i64::MAX})}
// Traceability scope: Task comment CAS begins at the separate coordination
// revision zero; positive document/review mutation revisions stay unchanged.
//= spec/impl/v2/implementation.md#collaboration-surfaces-preserve-existing-authority
//# The shared agent interface MUST expose every supported collaboration operation permitted to its current caller.
fn comment_revision()->Value {json!({"type":"integer","minimum":0,"maximum":i64::MAX})}
fn nullable(schema:Value)->Value {json!({"anyOf":[schema,{"type":"null"}]})}
fn object(fields:Value)->Value {
    let required:Vec<_>=fields.as_object().expect("fixed schema fields").keys().cloned().collect();
    json!({"type":"object","properties":fields,"required":required,"additionalProperties":false})
}
fn kind(value:&str)->Value {json!({"type":"string","enum":[value]})}
fn principal(id:&Value)->Value {json!({"oneOf":[object(json!({"kind":kind("trusted_host_operator")})),object(json!({"kind":kind("agent"),"agent_id":id}))]})}
fn record(id:&Value)->Value {json!({"oneOf":[object(json!({"kind":kind("task"),"task_id":id})),object(json!({"kind":kind("question"),"question_id":id})),object(json!({"kind":kind("document"),"document_id":id})),object(json!({"kind":kind("review"),"review_id":id}))]})}
pub(super) fn catalogue(limits:&RuntimeLimits)->Vec<Tool> {
    let Some(p)=limits.workflow.as_ref() else{return vec![]};
    let id=string(limits.transport.identity_bytes,true,"transport.identity_bytes");let text=string(p.text_bytes,false,"workflow.text_bytes");let content=object(json!({"title":string(p.text_bytes,true,"workflow.text_bytes"),"body":text}));
    let principal=principal(&id);let record=record(&id);let cursor=nullable(string(limits.controller.collaboration_cursor_bytes,true,"controller.collaboration_cursor_bytes"));let limit=json!({"type":"integer","minimum":1,"x-borg-maximum":p.page_items,"x-borg-limit-field":"workflow.page_items"});
    let registration=object(json!({"repository_id":id,"registration_revision":id}));
    // Existing Checkpoint's canonical shape, never a moving ref alone.
    let checkpoint=object(json!({"registration":registration,"repository":string(limits.transport.body_bytes,true,"transport.body_bytes"),"commit":id,"remote_ref":string(limits.transport.body_bytes,true,"transport.body_bytes"),"operation_id":id}));
    let accepted=object(json!({"task_id":id,"attempt_id":id,"terminal_task_revision":integer(),"acceptance_history_sequence":integer(),"checkpoint":nullable(checkpoint)}));
    let version=object(json!({"document_id":id,"version":integer(),"content_id":id}));
    let target=json!({"oneOf":[object(json!({"kind":kind("document_version"),"version":version})),object(json!({"kind":kind("accepted_task_result"),"result":accepted})),object(json!({"kind":kind("accepted_task_checkpoint"),"result":accepted}))]});
    let declarations=[
        ("collab_document_create","Create a bounded immutable initial document version and its current head",false,object(json!({"content":content,"supplied_content_id":nullable(id.clone()),"responsible_owner":principal}))),
        ("collab_document_version_create","Append one immutable version under the current expected document revision",false,object(json!({"document_id":id,"expected_revision":integer(),"content":content,"supplied_content_id":nullable(id.clone())}))),
        ("collab_document_owner_set","Change collaboration responsibility without changing accepted execution identity",false,object(json!({"document_id":id,"expected_revision":integer(),"responsible_owner":principal}))),
        ("collab_document_get","Read the current head or one exact retained document version",true,object(json!({"document_id":id,"version":nullable(integer())}))),
        ("collab_documents_list","Page canonical document heads",true,object(json!({"cursor":cursor,"limit":limit}))),
        ("collab_document_versions_list","Page immutable versions of one exact document",true,object(json!({"document_id":id,"cursor":cursor,"limit":limit}))),
        ("collab_comment_post","Post an immutable attributed comment under the subject's expected revision",false,object(json!({"record":record,"expected_revision":comment_revision(),"body":string(p.text_bytes,true,"workflow.text_bytes")}))),
        ("collab_comments_list","Page canonical comments for one typed record",true,object(json!({"record":record,"cursor":cursor,"limit":limit}))),
        ("collab_review_create","Request a direct product review of an exact immutable document version or accepted task result/checkpoint",false,object(json!({"target":target,"reviewer":principal,"summary":string(p.text_bytes,true,"workflow.text_bytes")}))),
        ("collab_review_get","Read a review and its immutable outcome",true,object(json!({"review_id":id}))),
        ("collab_reviews_list","Page canonical reviews, optionally filtered by subject and status",true,object(json!({"record":nullable(record.clone()),"status":nullable(json!({"type":"string","enum":["open","decided","withdrawn"]})),"cursor":cursor,"limit":limit}))),
        ("collab_review_target_get","Get the controller's exact accepted result/checkpoint selector for a subsequent review request",true,object(json!({"task_id":id,"scope":json!({"type":"string","enum":["result","checkpoint"]})}))),
        ("collab_review_decide","Record an immutable product decision only as the recorded direct reviewer; grants no protected authority",false,object(json!({"review_id":id,"expected_revision":integer(),"verdict":json!({"type":"string","enum":["approved","changes_requested","rejected"]}),"explanation":text}))),
        ("collab_review_withdraw","Withdraw an open review with the authorized current caller and a recorded reason",false,object(json!({"review_id":id,"expected_revision":integer(),"reason":string(p.text_bytes,true,"workflow.text_bytes")}))),
    ];
    declarations.into_iter().map(|(name,description,read,input_schema)|Tool{name:name.into(),description:description.into(),input_schema,annotations:json!({"readOnlyHint":read,"idempotentHint":false,"openWorldHint":false})}).collect()
}

/// Root routes these names here before its legacy flat-field validator, retaining
/// the original current-actor/profile/receipt path for the canonical operation.
/// Nested configured byte/count bounds are not misreported as malformed schema.
pub(crate) fn validate_arguments(name:&str,arguments:&Value,limits:&RuntimeLimits)->Result<(),String> {
    let tool=catalogue(limits).into_iter().find(|tool|tool.name==name).ok_or("document/review tools are disabled")?;
    let validator=jsonschema::options().with_draft(jsonschema::Draft::Draft202012).build(&tool.input_schema)
        .map_err(|_|"invalid document/review tool schema")?;
    if !validator.is_valid(arguments){return Err("document or review arguments differ from closed schema".into())}
    Ok(())
}
