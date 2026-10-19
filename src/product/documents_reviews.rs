//! Forms and views over canonical documents/reviews. Root routes them through the
//! same handlers as MCP, after existing host/same-origin request validation.
use super::*;
use crate::collaboration::{documents::*,workflow::*,Page};

pub(super) enum Mutation {
    Document {operation_id:String,request:DocumentMutation},
    Review {operation_id:String,request:ReviewMutation},
}
fn revision(f:&Form)->Result<u64,String> {
    let n=comment_revision(f)?;
    if n==0 {return Err("Invalid expected revision".into())}
    Ok(n)
}
// Traceability scope: The first task comment uses coordination revision zero;
// canonical record-specific CAS remains authoritative for every subject kind.
//= spec/impl/v2/implementation.md#collaboration-surfaces-preserve-existing-authority
//# The browser interface MUST expose every supported collaboration operation permitted to its caller.
fn comment_revision(f:&Form)->Result<u64,String> {
    let raw=required(f,"expected_revision")?;
    let n:u64=raw.parse().map_err(|_|"Invalid expected revision")?;
    if n>i64::MAX as u64 || n.to_string()!=raw {return Err("Invalid expected revision".into())}
    Ok(n)
}
fn principal(f:&Form,key:&str)->PrincipalRef {
    match f.get(key).filter(|s|!s.is_empty()) {
        Some(id)=>PrincipalRef::Agent{agent_id:id.clone()},
        None=>PrincipalRef::TrustedHostOperator,
    }
}
fn route_id(raw:&str,limits:&RuntimeLimits)->Result<String,String> {
    let id=decode(raw,false)?;
    if !crate::mcp::valid_identity(&id,limits.transport.identity_bytes){return Err("Invalid record identity".into())}
    Ok(id)
}
fn identity_field(f:&Form,limits:&RuntimeLimits)->Result<String,String> {
    let id=required(f,"operation_id")?;
    if !crate::mcp::valid_identity(id,limits.transport.identity_bytes){return Err("Invalid operation identity".into())}
    Ok(id.into())
}
pub(super) fn parse_post(path:&str,f:&Form,limits:&RuntimeLimits)->Result<Option<Mutation>,String> {
    if path=="/documents" {
        fields_only(f,&["operation_id","title","body","owner_agent_id"])?;
        return Ok(Some(Mutation::Document{operation_id:identity_field(f,limits)?,request:DocumentMutation::Create{
            content:DocumentContent{title:required(f,"title")?.into(),body:f.get("body").cloned().unwrap_or_default()},supplied_content_id:None,responsible_owner:principal(f,"owner_agent_id")}}));
    }
    if let Some((raw,action))=path.strip_prefix("/documents/").and_then(|s|s.split_once('/')) {
        if !["versions","owner","comments"].contains(&action){return Ok(None)}
        let document_id=route_id(raw,limits)?;
        let operation_id=identity_field(f,limits)?;
        let expected_revision=if action=="comments"{comment_revision(f)?}else{revision(f)?};
        let request=match action {
            "versions"=>{fields_only(f,&["operation_id","expected_revision","title","body"])?;DocumentMutation::AppendVersion{document_id,expected_revision,content:DocumentContent{title:required(f,"title")?.into(),body:f.get("body").cloned().unwrap_or_default()},supplied_content_id:None}},
            "owner"=>{fields_only(f,&["operation_id","expected_revision","owner_agent_id"])?;DocumentMutation::SetResponsibleOwner{document_id,expected_revision,responsible_owner:principal(f,"owner_agent_id")}},
            "comments"=>{fields_only(f,&["operation_id","expected_revision","body"])?;DocumentMutation::PostComment{record:RecordRef::Document{document_id},expected_revision,body:required(f,"body")?.into()}},
            _=>unreachable!(),
        };
        return Ok(Some(Mutation::Document{operation_id,request}));
    }
    for (prefix,kind) in [("/questions/","question"),("/tasks/","task")] {
        if let Some(raw)=path.strip_prefix(prefix).and_then(|s|s.strip_suffix("/comments")).filter(|s|!s.contains('/')) {
            fields_only(f,&["operation_id","expected_revision","body"])?;
            let id=route_id(raw,limits)?;
            let record=if kind=="question"{RecordRef::Question{question_id:id}}else{RecordRef::Task{task_id:id}};
            return Ok(Some(Mutation::Document{operation_id:identity_field(f,limits)?,request:DocumentMutation::PostComment{record,expected_revision:comment_revision(f)?,body:required(f,"body")?.into()}}));
        }
    }
    if path=="/reviews" {
        fields_only(f,&["operation_id","target","reviewer_agent_id","summary"])?;
        let raw=required(f,"target")?;
        let value=crate::mcp::strict::parse(raw.as_bytes(),limits.transport.body_bytes)?;
        let target:ReviewTarget=serde_json::from_value(value.clone()).map_err(|_|"Invalid exact review subject")?;
        if serde_json::to_value(&target).map_err(|_|"Invalid review subject")?!=value{return Err("Unknown or ignored review subject field".into())}
        return Ok(Some(Mutation::Review{operation_id:identity_field(f,limits)?,request:ReviewMutation::Request{target,reviewer:principal(f,"reviewer_agent_id"),summary:required(f,"summary")?.into()}}));
    }
    if let Some((raw,action))=path.strip_prefix("/reviews/").and_then(|s|s.split_once('/')) {
        if !["decide","withdraw","comments"].contains(&action){return Ok(None)}
        let review_id=route_id(raw,limits)?;
        let operation_id=identity_field(f,limits)?;
        let expected_revision=if action=="comments"{comment_revision(f)?}else{revision(f)?};
        if action=="comments" {
            fields_only(f,&["operation_id","expected_revision","body"])?;
            return Ok(Some(Mutation::Document{operation_id,request:DocumentMutation::PostComment{record:RecordRef::Review{review_id},expected_revision,body:required(f,"body")?.into()}}));
        }
        let request=if action=="decide" {
            fields_only(f,&["operation_id","expected_revision","verdict","explanation"])?;
            let verdict=match required(f,"verdict")?{"approved"=>ReviewVerdict::Approved,"changes_requested"=>ReviewVerdict::ChangesRequested,"rejected"=>ReviewVerdict::Rejected,_=>return Err("Invalid review verdict".into())};
            ReviewMutation::Decide{review_id,expected_revision,verdict,explanation:f.get("explanation").cloned().unwrap_or_default()}
        } else {
            fields_only(f,&["operation_id","expected_revision","reason"])?;
            ReviewMutation::Withdraw{review_id,expected_revision,reason:required(f,"reason")?.into()}
        };
        return Ok(Some(Mutation::Review{operation_id,request}));
    }
    Ok(None)
}
// Read-only canonical DTOs already passed the domain record/page bounds. The
// product finalizer bounds the complete escaped response; no private SQL row or
// controller storage configuration is exposed here.
//= spec/impl/v2/implementation.md#shared-product-state
//# The browser interface MUST present the information exposed through the shared agent interface.
fn exact_details(id:&str,label:&str,value:&impl serde::Serialize)->String {
    let encoded=serde_json::to_string(value).expect("canonical collaboration DTO serializes");
    format!("<details id=\"{}\"><summary>{}</summary><pre>{}</pre></details>",escape(id),escape(label),escape(&encoded))
}
fn exact_target_link(target:&ReviewTarget)->String {
    let (href,label)=match target {
        ReviewTarget::DocumentVersion{version}=>(format!("/documents/{}?version={}",encode(&version.document_id),version.version),"Read the reviewed document version"),
        ReviewTarget::AcceptedTaskResult{result}=>(format!("{}/result",task_path(&result.task_id)),"Read the accepted task result"),
        ReviewTarget::AcceptedTaskCheckpoint{result}=>(format!("{}/checkpoint",task_path(&result.task_id)),"Read the accepted task checkpoint"),
    };
    format!("<p><a href=\"{}\">{}</a></p>",escape(&href),label)
}
fn actor(actor:&RecordedActor)->String {
    match &actor.provenance {
        ActorProvenance::TrustedHost{path:_}=>"Trusted-host operator".into(),
        ActorProvenance::BoundAgentSession{agent_id,session_id,..}=>format!("Agent {} in session {}",escape(agent_id),escape(session_id)),
    }
}
fn recipient(principal:&PrincipalRef)->String {
    match principal {PrincipalRef::TrustedHostOperator=>"Trusted-host operator".into(),PrincipalRef::Agent{agent_id}=>format!("Agent {}",escape(agent_id))}
}
fn text_area(name:&str,label:&str,value:&str)->String {
    format!("<label for=\"{}\">{}</label><textarea id=\"{}\" name=\"{}\" rows=\"10\">{}</textarea>",escape(name),escape(label),escape(name),escape(name),escape(value))
}
fn mutation_start(route:&str,revision:Option<u64>)->String {
    let mut s=format!("<form method=\"post\" action=\"{}\">{}",escape(route),hidden("operation_id",&identity()));
    if let Some(revision)=revision{s.push_str(&hidden("expected_revision",&revision.to_string()));}
    s
}
fn principal_field(name:&str,label:&str)->String {
    format!("{}<p>Leave blank for the trusted-host operator; otherwise enter an existing agent ID.</p>",field(name,label,"","text",false))
}
fn next_page(body:&mut String,route:&str,cursor:Option<&str>){
    if let Some(cursor)=cursor{let join=if route.contains('?'){"&"}else{"?"};let _=write!(body,"<p><a href=\"{}{join}after={}\">Next page</a></p>",escape(route),encode(cursor));}
}
// Shared by the two existing canonical record/status list projections.
//= spec/impl/v2/implementation.md#collaboration-surfaces-preserve-existing-authority
//# The browser interface MUST expose every supported collaboration operation permitted to its caller.
pub(super) fn list_record_filter(fields:&Form)->Result<Option<RecordRef>,String> {
    let kind=fields.get("record_kind").filter(|value|!value.is_empty());
    let id=fields.get("record_id").filter(|value|!value.is_empty());
    match (kind,id) {
        (None,None)=>Ok(None),
        (Some(kind),Some(id))=>Ok(Some(match kind.as_str() {
            "task"=>RecordRef::Task{task_id:id.clone()},"question"=>RecordRef::Question{question_id:id.clone()},
            "document"=>RecordRef::Document{document_id:id.clone()},"review"=>RecordRef::Review{review_id:id.clone()},
            _=>return Err("Unsupported linked record kind".into()),
        })),
        _=>Err("A record filter requires both kind and ID".into()),
    }
}
pub(super) fn list_filter_base(route:&str,fields:&Form)->String {
    let query=["record_kind","record_id","status"].into_iter().filter_map(|key|
        fields.get(key).filter(|value|!value.is_empty()).map(|value|format!("{key}={}",encode(value))))
        .collect::<Vec<_>>().join("&");
    if query.is_empty(){route.into()}else{format!("{route}?{query}")}
}
pub(super) fn list_filter_form(route:&str,fields:&Form,statuses:&[(&str,&str)])->String {
    let mut body=format!("<form method=\"get\" action=\"{}\"><label>Linked record kind<select name=\"record_kind\">",escape(route));
    for(kind,label)in [("","Any record"),("task","Task"),("question","Question"),("document","Document"),("review","Review")] {
        let _=write!(body,"<option value=\"{kind}\" {}>{label}</option>",if fields.get("record_kind").map(String::as_str).unwrap_or("")==kind{"selected"}else{""});
    }
    body.push_str("</select></label>");body.push_str(&field("record_id","Linked record ID",fields.get("record_id").map(String::as_str).unwrap_or(""),"text",false));
    body.push_str("<label>Status<select name=\"status\"><option value=\"\">Any status</option>");
    for(value,label)in statuses {let _=write!(body,"<option value=\"{}\" {}>{}</option>",escape(value),if fields.get("status").map(String::as_str)==Some(*value){"selected"}else{""},escape(label));}
    body.push_str("</select></label><button>Filter records</button></form>");body
}
pub(super) fn new_document()->UiResponse {
    let mut body=String::from("<p><a href=\"/documents\">Documents</a></p>");
    body.push_str(&mutation_start("/documents",None));
    body.push_str(&field("title","Title","","text",true));
    body.push_str(&text_area("body","Content",""));
    body.push_str(&principal_field("owner_agent_id","Responsible owner"));
    body.push_str("<button>Create document</button></form>");
    page(200,"New document",body)
}
pub(super) fn document_list(documents:&Page<Document>)->UiResponse {
    let mut body=String::from("<p><a href=\"/documents/new\">New document</a></p><ul>");
    for document in &documents.items {let _=write!(body,"<li><a href=\"/documents/{}\">{}</a> — version {}, owner {}</li>",encode(&document.id),escape(&document.id),document.current.version,recipient(&document.responsible_owner));}
    body.push_str("</ul>");next_page(&mut body,"/documents",documents.next_cursor.as_deref());
    page(200,"Documents",body)
}
pub(super) fn document(view:&DocumentView)->UiResponse {
    let doc=&view.document;let version=&view.version;
    let route=format!("/documents/{}",encode(&doc.id));
    let mut body=format!("<p><a href=\"/documents\">Documents</a> · <a href=\"{route}/versions\">All versions</a> · <a href=\"{route}/comments\">Comments</a></p><p>Version {}. Recorded by {}. Responsible owner: {}.</p><pre>{}</pre>",version.reference.version,actor(&version.author),recipient(&doc.responsible_owner),escape(&version.content.body));
    body.push_str(&exact_details("document-record","Exact document version, identity and provenance",view));
    body.push_str(&review_request_form(&ReviewTarget::DocumentVersion{version:version.reference.clone()}));
    if doc.current==version.reference {
        body.push_str("<h2>Create a new version</h2>");body.push_str(&mutation_start(&format!("{route}/versions"),Some(doc.revision)));
        body.push_str(&field("title","Title",&version.content.title,"text",true));body.push_str(&text_area("body","Content",&version.content.body));body.push_str("<button>Save new version</button></form>");
        body.push_str("<h2>Responsible owner</h2>");body.push_str(&mutation_start(&format!("{route}/owner"),Some(doc.revision)));body.push_str(&principal_field("owner_agent_id","New responsible owner"));body.push_str("<button>Update owner</button></form>");
    } else {body.push_str("<p>This is a retained historical version. Open the current version to edit.</p>");}
    body.push_str(&comment_form(&format!("{route}/comments"),doc.revision));
    page(200,&version.content.title,body)
}
pub(super) fn versions(document_id:&str,versions:&Page<DocumentVersion>)->UiResponse {
    let route=format!("/documents/{}",encode(document_id));
    let mut body=format!("<p><a href=\"{route}\">Current document</a></p><ol>");
    for version in &versions.items {let _=write!(body,"<li><a href=\"{route}?version={}\">Version {}: {}</a> — {}</li>",version.reference.version,version.reference.version,escape(&version.content.title),actor(&version.author));}
    body.push_str("</ol>");next_page(&mut body,&format!("{route}/versions"),versions.next_cursor.as_deref());page(200,"Document versions",body)
}
pub(super) fn comment_form(route:&str,revision:u64)->String {
    format!("<h2>Add comment</h2>{}{}<button>Post comment</button></form>",mutation_start(route,Some(revision)),text_area("body","Comment",""))
}
pub(super) fn comments(route:&str,comments:&Page<RecordedComment>)->UiResponse {
    let mut body=String::new();
    for comment in &comments.items {let _=write!(body,"<article><p>{}</p><pre>{}</pre>{}</article>",actor(&comment.author),escape(&comment.body),exact_details(&format!("comment-{}",comment.id),"Exact immutable comment and provenance",comment));}
    next_page(&mut body,route,comments.next_cursor.as_deref());page(200,"Comments",body)
}
fn target_label(target:&ReviewTarget)->String {
    match target {
        ReviewTarget::DocumentVersion{version}=>format!("Document {} version {}",escape(&version.document_id),version.version),
        ReviewTarget::AcceptedTaskResult{result}=>format!("Accepted result of task {} (attempt {}, acceptance revision {})",escape(&result.task_id),escape(&result.attempt_id),result.terminal_task_revision),
        ReviewTarget::AcceptedTaskCheckpoint{result}=>format!("Accepted checkpoint of task {} (attempt {}, acceptance revision {})",escape(&result.task_id),escape(&result.attempt_id),result.terminal_task_revision),
    }
}
pub(super) fn review_request_form(target:&ReviewTarget)->String {
    let mut body=format!("<h2>Request review</h2><p>{}</p>",target_label(target));
    body.push_str(&exact_target_link(target));
    body.push_str(&exact_details("review-target","Exact reviewed subject",target));
    body.push_str(&mutation_start("/reviews",None));
    body.push_str(&hidden("target",&serde_json::to_string(target).expect("typed review target serializes")));
    body.push_str(&principal_field("reviewer_agent_id","Direct reviewer"));
    body.push_str(&text_area("summary","What should the reviewer assess?",""));
    body.push_str("<button>Request review</button></form>");body
}
pub(super) fn review_list(reviews:&Page<Review>,fields:&Form)->UiResponse {
    let mut body=String::from("<p><a href=\"/decisions\">Decision queue</a></p>");
    body.push_str(&list_filter_form("/reviews",fields,&[("open","Open"),("decided","Decided"),("withdrawn","Withdrawn")]));
    body.push_str("<ul>");
    for review in &reviews.items {let _=write!(body,"<li><a href=\"/reviews/{}\">{}</a> — {}</li>",encode(&review.id),target_label(&review.target),recipient(&review.reviewer));}
    body.push_str("</ul>");next_page(&mut body,&list_filter_base("/reviews",fields),reviews.next_cursor.as_deref());page(200,"Reviews",body)
}
pub(super) fn review(view:&ReviewView)->UiResponse {
    let review=&view.review;let route=format!("/reviews/{}",encode(&review.id));
    let mut body=format!("<p><a href=\"/reviews\">Reviews</a> · <a href=\"/decisions\">Decision queue</a></p><h2>{}</h2><p>Requested by {}. Direct reviewer: {}. Responsible owner: {}.</p><pre>{}</pre>",target_label(&review.target),actor(&review.requested_by),recipient(&review.reviewer),recipient(&view.current_responsible_owner),escape(&review.summary));
    body.push_str(&exact_target_link(&review.target));
    body.push_str(&exact_details("review-record","Exact review subject, outcome and provenance",view));
    if let Some(decision)=&view.decision {
        let verdict=match decision.verdict{ReviewVerdict::Approved=>"Approved",ReviewVerdict::ChangesRequested=>"Changes requested",ReviewVerdict::Rejected=>"Rejected"};
        let _=write!(body,"<p>{verdict} by {}.</p><pre>{}</pre>",actor(&decision.reviewer),escape(&decision.explanation));
    } else if let Some(withdrawal)=&view.withdrawal {let _=write!(body,"<p>Withdrawn by {}.</p><pre>{}</pre>",actor(&withdrawal.actor),escape(&withdrawal.reason));}
    else if review.status==ReviewStatus::Open {
        if review.reviewer==PrincipalRef::TrustedHostOperator {
            body.push_str(&mutation_start(&format!("{route}/decide"),Some(review.revision)));
            body.push_str("<label for=\"verdict\">Decision</label><select id=\"verdict\" name=\"verdict\"><option value=\"approved\">Approve this product result</option><option value=\"changes_requested\">Request changes</option><option value=\"rejected\">Reject</option></select>");
            body.push_str(&text_area("explanation","Explanation",""));body.push_str("<button>Record decision</button></form>");
        } else {body.push_str("<p>This review is owed by the recorded agent. This operator view cannot answer as that agent.</p>");}
        body.push_str(&mutation_start(&format!("{route}/withdraw"),Some(review.revision)));body.push_str(&text_area("reason","Withdrawal reason",""));body.push_str("<button>Withdraw review</button></form>");
    }
    body.push_str("<p>These are product review outcomes. They do not authorize deployment, host changes or credential access.</p>");
    body.push_str(&comment_form(&format!("{route}/comments"),review.revision));page(200,"Review",body)
}


fn record_path(record:&RecordRef)->String {
    match record {RecordRef::Task{task_id}=>task_path(task_id),RecordRef::Question{question_id}=>format!("/questions/{}",encode(question_id)),RecordRef::Document{document_id}=>format!("/documents/{}",encode(document_id)),RecordRef::Review{review_id}=>format!("/reviews/{}",encode(review_id))}
}
fn committed(operation_id:&str,route:String)->UiResponse {
    let mut r=page(303,"Recorded collaboration change",format!("<p>Operation <code>{}</code> committed.</p><p><a href=\"{}\">View current record</a></p>",escape(operation_id),escape(&route)));
    r.location=Some(route);r
}
// Root's existing mutation_reply_for checks original receipt identity/commit before
// dispatching these success renderers. Pending/uncertain/refused paths stay shared.
pub(super) fn document_committed(operation_id:&str,outcome:&DocumentMutationOutcome)->UiResponse {
    let route=match outcome {DocumentMutationOutcome::Document{view}=>format!("/documents/{}",encode(&view.document.id)),DocumentMutationOutcome::Comment{comment,..}=>record_path(&comment.record)};
    committed(operation_id,route)
}
pub(super) fn review_committed(operation_id:&str,outcome:&ReviewMutationOutcome)->UiResponse {
    let ReviewMutationOutcome::Review(view)=outcome;
    committed(operation_id,format!("/reviews/{}",encode(&view.review.id)))
}
pub(super) async fn post(controller:&Controller,path:&str,fields:&Form)->Option<UiResponse> {
    let parsed=match parse_post(path,fields,controller.limits()){Ok(v)=>v,Err(e)=>return Some(error(400,&e))};
    let (operation_id,operation,fallback)=match parsed? {
        Mutation::Document{operation_id,request}=>(operation_id.clone(),Operation::MutateDocument{operation_id,mutation:request},"/documents"),
        Mutation::Review{operation_id,request}=>(operation_id.clone(),Operation::MutateReview{operation_id,mutation:request},"/reviews"),
    };
    // Root's product call entry establishes Browser provenance; no form field can.
    let reply=call(controller,operation).await;
    Some(mutation_reply_for(reply,&operation_id,None,fallback))
}
fn query_fields(query:&str,allowed:&[&str],limits:&RuntimeLimits)->Result<Form,UiResponse> {
    let f=parse_form(query.as_bytes(),limits).map_err(|e|error(400,&e))?;
    fields_only(&f,allowed).map_err(|e|error(400,&e))?;Ok(f)
}
fn after(f:&Form)->Option<String>{f.get("after").filter(|v|!v.is_empty()).cloned()}
async fn document_query(controller:&Controller,query:DocumentQuery)->Result<DocumentQueryResult,UiResponse> {
    let r=call(controller,Operation::QueryDocument{query}).await;
    match r.outcome {Outcome::Ok{data:Data::DocumentQuery(value)}=>Ok(value),_=>Err(not_task(r))}
}
async fn review_query(controller:&Controller,query:ReviewQuery)->Result<Box<ReviewQueryResult>,UiResponse> {
    let r=call(controller,Operation::QueryReview{query}).await;
    match r.outcome {Outcome::Ok{data:Data::ReviewQuery(value)}=>Ok(value),_=>Err(not_task(r))}
}
pub(super) async fn get(controller:&Controller,path:&str,raw_query:&str)->Option<UiResponse> {
    match get_inner(controller,path,raw_query).await {Ok(r)=>r,Err(r)=>Some(r)}
}
async fn get_inner(controller:&Controller,path:&str,raw_query:&str)->Result<Option<UiResponse>,UiResponse> {
    let relevant=path=="/documents"||path.starts_with("/documents/")||path=="/reviews"||path.starts_with("/reviews/")
        || ((path.starts_with("/questions/")||path.starts_with("/tasks/"))&&(path.ends_with("/comments")||path.ends_with("/review-target")));
    if !relevant{return Ok(None)}
    let limits=controller.limits();
    let Some(policy)=limits.workflow.as_ref() else{return Err(error(409,"Collaboration workflows are disabled by current metadata"))};
    let count=policy.page_items.min(limits.product.page_items);
    if path=="/documents/new"{query_fields(raw_query,&[],limits)?;return Ok(Some(new_document()))}
    if path=="/documents" {
        let f=query_fields(raw_query,&["after"],limits)?;
        let DocumentQueryResult::Documents(p)=document_query(controller,DocumentQuery::List{after:after(&f),limit:count}).await? else{return Err(error(502,"Unexpected document list"))};
        return Ok(Some(document_list(&p)))
    }
    if path=="/reviews" {
        let f=query_fields(raw_query,&["after","record_kind","record_id","status"],limits)?;
        let record=list_record_filter(&f).map_err(|e|error(400,&e))?;
        let status=match f.get("status").map(String::as_str).filter(|value|!value.is_empty()) {
            None=>None,Some("open")=>Some(ReviewStatus::Open),Some("decided")=>Some(ReviewStatus::Decided),Some("withdrawn")=>Some(ReviewStatus::Withdrawn),
            _=>return Err(error(400,"Unsupported review status")),
        };
        let result=review_query(controller,ReviewQuery::List{record,status,after:after(&f),limit:count}).await?;
        let ReviewQueryResult::Reviews(p)=result.as_ref() else{return Err(error(502,"Unexpected review list"))};
        return Ok(Some(review_list(p,&f)))
    }
    for (prefix,kind) in [("/documents/","document"),("/reviews/","review"),("/questions/","question"),("/tasks/","task")] {
        let Some(tail)=path.strip_prefix(prefix) else{continue};
        let (raw,action)=tail.split_once('/').unwrap_or((tail,""));
        let id=route_id(raw,limits).map_err(|e|error(400,&e))?;
        let record=match kind {"document"=>RecordRef::Document{document_id:id.clone()},"review"=>RecordRef::Review{review_id:id.clone()},"question"=>RecordRef::Question{question_id:id.clone()},_=>RecordRef::Task{task_id:id.clone()}};
        if action=="comments" {
            let f=query_fields(raw_query,&["after"],limits)?;
            let DocumentQueryResult::Comments(p)=document_query(controller,DocumentQuery::Comments{record,after:after(&f),limit:count}).await? else{return Err(error(502,"Unexpected comments"))};
            return Ok(Some(comments(path,&p)))
        }
        if kind=="task"&&action=="review-target" {
            let f=query_fields(raw_query,&["scope"],limits)?;
            let scope=match f.get("scope").map(String::as_str).unwrap_or("result"){"result"=>AcceptedReviewScope::Result,"checkpoint"=>AcceptedReviewScope::Checkpoint,_=>return Err(error(400,"Unsupported accepted review scope"))};
            let result=review_query(controller,ReviewQuery::AcceptedTarget{task_id:id,scope}).await?;
            let ReviewQueryResult::AcceptedTarget(target)=result.as_ref() else{return Err(error(502,"Unexpected accepted subject"))};
            return Ok(Some(page(200,"Request result review",review_request_form(target))))
        }
        if kind=="document"&&action=="versions" {
            let f=query_fields(raw_query,&["after","cursor","max_bytes"],limits)?;
            let response=boxed_route(||super::read_pages::public_list(controller,crate::read_page::ReadTool::Versions,
                serde_json::json!({"document_id":id,"cursor":after(&f),"limit":count}),&f)).await?;
            return Ok(Some(response))
        }
        if kind=="document"&&action.is_empty() {
            let f=query_fields(raw_query,&["version","part","cursor","max_bytes"],limits)?;
            let version=match f.get("version"){None=>None,Some(value)=>{let n:u64=value.parse().map_err(|_|error(400,"Invalid document version"))?;if n==0||n.to_string()!=*value{return Err(error(400,"Invalid document version"))}Some(n)}};
            let part=match f.get("part").map(String::as_str).unwrap_or("record") {"record"=>crate::read_page::DocumentPart::Record,"body"=>crate::read_page::DocumentPart::Body,_=>return Err(error(400,"Invalid document part"))};
            let (cursor,max_bytes)=super::read_pages::options(&f)?;
            let read=boxed_route(||super::read_pages::query(controller,crate::read_page::Request {selection:crate::read_page::Selection::Document{document_id:id.clone(),version,part},cursor,max_bytes})).await?;
            if read.complete && read.offset==0 && part==crate::read_page::DocumentPart::Record && version.is_none() && !f.contains_key("cursor") && !f.contains_key("max_bytes") {
                let view:DocumentView=serde_json::from_str(&read.content).map_err(|_|error(502,"Unexpected document projection"))?;
                let full=document(&view);
                if full.body.len()<=limits.product.response_bytes {return Ok(Some(full));}
            }
            return Ok(Some(super::read_pages::render(&read)))
        }
        if kind=="review"&&action.is_empty() {
            query_fields(raw_query,&[],limits)?;
            let result=review_query(controller,ReviewQuery::Get{review_id:id}).await?;
            let ReviewQueryResult::Review(view)=result.as_ref() else{return Err(error(502,"Unexpected review"))};
            return Ok(Some(review(view)))
        }
        return Ok(None)
    }
    Ok(None)
}
