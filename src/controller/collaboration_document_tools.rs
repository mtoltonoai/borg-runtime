//! Closed document/review argument decoding. Root preserves the validated current
//! MCP actor when dispatching this canonical Operation; it must never re-enter as
//! TrustedHostOperator. Envelope operation_id is not a model argument.
use super::database::{Fault,Result};
use crate::{collaboration::{documents::*,workflow::*},mcp::Invocation,protocol::Operation};
use serde::Deserialize;
#[derive(Deserialize)]
#[serde(tag="tool",content="arguments",deny_unknown_fields)]
enum Args {
    #[serde(rename="collab_document_create")]
    Create {content:DocumentContent,supplied_content_id:Option<String>,responsible_owner:PrincipalRef},
    #[serde(rename="collab_document_version_create")]
    Version {document_id:String,expected_revision:u64,content:DocumentContent,supplied_content_id:Option<String>},
    #[serde(rename="collab_document_owner_set")]
    Owner {document_id:String,expected_revision:u64,responsible_owner:PrincipalRef},
    #[serde(rename="collab_document_get")]
    Document {document_id:String,version:Option<u64>},
    #[serde(rename="collab_documents_list")]
    Documents {cursor:Option<String>,limit:usize},
    #[serde(rename="collab_document_versions_list")]
    Versions {document_id:String,cursor:Option<String>,limit:usize},
    #[serde(rename="collab_comment_post")]
    Comment {record:RecordRef,expected_revision:u64,body:String},
    #[serde(rename="collab_comments_list")]
    Comments {record:RecordRef,cursor:Option<String>,limit:usize},
    #[serde(rename="collab_review_create")]
    ReviewCreate {target:ReviewTarget,reviewer:PrincipalRef,summary:String},
    #[serde(rename="collab_review_get")]
    Review {review_id:String},
    #[serde(rename="collab_reviews_list")]
    Reviews {record:Option<RecordRef>,status:Option<ReviewStatus>,cursor:Option<String>,limit:usize},
    #[serde(rename="collab_review_target_get")]
    Target {task_id:String,scope:AcceptedReviewScope},
    #[serde(rename="collab_review_decide")]
    Decide {review_id:String,expected_revision:u64,verdict:ReviewVerdict,explanation:String},
    #[serde(rename="collab_review_withdraw")]
    Withdraw {review_id:String,expected_revision:u64,reason:String},
}
pub(super) fn operation(call:&Invocation)->Result<Option<Operation>> {
    if !crate::mcp::documents_reviews::owns(&call.tool){return Ok(None)}
    let args:Args=serde_json::from_value(serde_json::json!({"tool":call.tool,"arguments":call.arguments}))
        .map_err(|_|Fault::Refused("invalid_input","document or review arguments differ from the closed schema".into()))?;
    let id=call.operation_id.clone();
    let document=|mutation|Operation::MutateDocument{operation_id:id.clone(),mutation};
    let review=|mutation|Operation::MutateReview{operation_id:id.clone(),mutation};
    Ok(Some(match args {
        Args::Create{content,supplied_content_id,responsible_owner}=>document(DocumentMutation::Create{content,supplied_content_id,responsible_owner}),
        Args::Version{document_id,expected_revision,content,supplied_content_id}=>document(DocumentMutation::AppendVersion{document_id,expected_revision,content,supplied_content_id}),
        Args::Owner{document_id,expected_revision,responsible_owner}=>document(DocumentMutation::SetResponsibleOwner{document_id,expected_revision,responsible_owner}),
        Args::Document{document_id,version}=>Operation::QueryDocument{query:DocumentQuery::Get{document_id,version}},
        Args::Documents{cursor,limit}=>Operation::QueryDocument{query:DocumentQuery::List{after:cursor,limit}},
        Args::Versions{document_id,cursor,limit}=>Operation::QueryDocument{query:DocumentQuery::Versions{document_id,after:cursor,limit}},
        Args::Comment{record,expected_revision,body}=>document(DocumentMutation::PostComment{record,expected_revision,body}),
        Args::Comments{record,cursor,limit}=>Operation::QueryDocument{query:DocumentQuery::Comments{record,after:cursor,limit}},
        Args::ReviewCreate{target,reviewer,summary}=>review(ReviewMutation::Request{target,reviewer,summary}),
        Args::Review{review_id}=>Operation::QueryReview{query:ReviewQuery::Get{review_id}},
        Args::Reviews{record,status,cursor,limit}=>Operation::QueryReview{query:ReviewQuery::List{record,status,after:cursor,limit}},
        Args::Target{task_id,scope}=>Operation::QueryReview{query:ReviewQuery::AcceptedTarget{task_id,scope}},
        Args::Decide{review_id,expected_revision,verdict,explanation}=>review(ReviewMutation::Decide{review_id,expected_revision,verdict,explanation}),
        Args::Withdraw{review_id,expected_revision,reason}=>review(ReviewMutation::Withdraw{review_id,expected_revision,reason}),
    }))
}
