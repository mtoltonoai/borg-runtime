//! Browser questions use the same canonical mutations and original receipts as MCP.
//! The root dispatcher performs its existing origin/body checks before post().
use super::*;
use crate::collaboration::{questions::*,workflow::*};

pub(super) fn route(path:&str)->bool { path=="/questions" || path=="/decisions" || path.starts_with("/questions/") }
fn question_path(id:&str)->String {format!("/questions/{}",encode(id))}
fn actor_label(actor:&RecordedActor)->String {match &actor.principal {PrincipalRef::TrustedHostOperator=>"Trusted host operator".into(),PrincipalRef::Agent{agent_id}=>format!("Agent {agent_id}")}}
fn principal_label(actor:&PrincipalRef)->String {match actor {PrincipalRef::TrustedHostOperator=>"Trusted host operator".into(),PrincipalRef::Agent{agent_id}=>format!("Agent {agent_id}")}}
fn raw_json(value:&impl serde::Serialize)->String {format!("<pre>{}</pre>",escape(&serde_json::to_string_pretty(value).unwrap_or_default()))}
fn area(name:&str,label:&str,value:&str,required:bool)->String {format!("<label>{}<textarea name=\"{}\" {}>{}</textarea></label>",escape(label),escape(name),if required{"required"}else{""},escape(value))}
fn nav()->String {"<p><a href=\"/questions\">Questions</a> · <a href=\"/decisions\">Decision queue</a> · <a href=\"/questions/new\">Pose a question</a></p>".into()}
fn continuation(body:&mut String,base:&str,after:Option<&str>) {if let Some(after)=after {let join=if base.contains('?'){"&"}else{"?"};let _=write!(body,"<p><a href=\"{}{join}after={}\">Next page</a></p>",escape(base),encode(after));}}
async fn query(controller:&Controller,query:QuestionQuery)->Result<Box<QuestionQueryResult>,UiResponse> {
    let reply=call(controller,Operation::QueryQuestion{query}).await;
    match reply.outcome {Outcome::Ok{data:Data::QuestionQuery(value)}=>Ok(value),_=>Err(not_task(reply))}
}
fn page_limit(controller:&Controller)->Result<usize,UiResponse> {controller.limits().workflow.as_ref().map(|p|p.page_items.min(controller.limits().product.page_items)).ok_or_else(||error(409,"Collaboration workflows are disabled"))}
fn record(fields:&Form)->Result<RecordRef,String> {
    let id=required(fields,"record_id")?.to_owned();Ok(match required(fields,"record_kind")? {
        "task"=>RecordRef::Task{task_id:id},"question"=>RecordRef::Question{question_id:id},"document"=>RecordRef::Document{document_id:id},"review"=>RecordRef::Review{review_id:id},_=>return Err("Unknown linked record kind".into())
    })
}
fn record_parts(record:&RecordRef)->(&str,&str) {match record {RecordRef::Task{task_id}=>("task",task_id),RecordRef::Question{question_id}=>("question",question_id),RecordRef::Document{document_id}=>("document",document_id),RecordRef::Review{review_id}=>("review",review_id)}}
fn pose(fields:&Form,limits:&RuntimeLimits)->Result<PoseQuestion,String> {
    let count=number(fields,"option_count")?;
    let policy=limits.workflow.as_ref().ok_or("Collaboration workflows are disabled")?;
    if count==0 || count>policy.options as u64 {return Err("Option count exceeds workflow.options".into())}
    let mut allowed=vec!["operation_id".to_owned(),"expected_revision".into(),"reason".into(),"record_kind".into(),"record_id".into(),"recipient_kind".into(),"recipient_agent_id".into(),"prompt".into(),"recommended_option_id".into(),"option_count".into()];
    let mut options=vec![];
    for n in 1..=count {let label=format!("option_{n}_label");let explanation=format!("option_{n}_explanation");options.push(QuestionOption{id:format!("option-{n}"),label:required(fields,&label)?.into(),explanation:fields.get(&explanation).cloned().unwrap_or_default()});allowed.push(label);allowed.push(explanation);}
    fields_only(fields,&allowed.iter().map(String::as_str).collect::<Vec<_>>())?;
    let recipient=match required(fields,"recipient_kind")? {"operator"=>{if fields.get("recipient_agent_id").is_some_and(|v|!v.is_empty()){return Err("Operator recipient has no agent ID".into())}PrincipalRef::TrustedHostOperator},"agent"=>PrincipalRef::Agent{agent_id:required(fields,"recipient_agent_id")?.into()},_=>return Err("Unknown recipient kind".into())};
    Ok(PoseQuestion{record:record(fields)?,recipient,prompt:required(fields,"prompt")?.into(),options,recommended_option_id:required(fields,"recommended_option_id")?.into()})
}
/// Only the exact canonical trailing recommendation is generated display text.
/// Keep every byte of the submitted body, and never mutate the stored original.
fn editable_prompt(posed:&PoseQuestion)->&str {
    let Some(option)=posed.options.iter().find(|option|option.id==posed.recommended_option_id)else{return &posed.prompt};
    let suffix=format!("\n\nRecommended option: {} — {}.",
        serde_json::to_string(&option.id).expect("string serializes"),
        serde_json::to_string(&option.label).expect("string serializes"));
    posed.prompt.strip_suffix(&suffix).unwrap_or(&posed.prompt)
}
fn pose_form(controller:&Controller,posed:Option<&Question>,count:usize)->UiResponse {
    let Some(policy)=controller.limits().workflow.as_ref()else{return error(409,"Collaboration workflows are disabled")};
    if count==0 || count>policy.options{return error(400,"Option count exceeds workflow.options")}
    let action=posed.map(|q|format!("{}/supersede",question_path(&q.id))).unwrap_or_else(||"/questions".into());
    let value=posed.map(|q|&q.posed);let mut body=nav();
    body.push_str("<p>Ask one decision and select its direct recipient and recommended option. For an operator recipient, offer choices about what should happen. Do not describe steps for the operator to perform by hand. The saved prompt includes the selected recommendation. An answer records a product decision and grants no deployment or host authority.</p>");
    let _=write!(body,"<form method=\"post\" action=\"{}\">",escape(&action));body.push_str(&hidden("operation_id",&identity()));body.push_str(&hidden("option_count",&count.to_string()));
    if let Some(q)=posed {body.push_str(&hidden("expected_revision",&q.revision.to_string()));body.push_str(&area("reason","Reason for replacing this question","",true));}
    let (kind,id)=value.map(|q|record_parts(&q.record)).unwrap_or(("task",""));
    if posed.is_some(){body.push_str(&hidden("record_kind",kind));body.push_str(&hidden("record_id",id));}else{body.push_str("<label>Linked record<select name=\"record_kind\"><option value=\"task\">Task</option><option value=\"document\">Document</option><option value=\"question\">Question</option><option value=\"review\">Review</option></select></label>");body.push_str(&field("record_id","Record ID",id,"text",true));}
    let agent=value.and_then(|q|match &q.recipient{PrincipalRef::Agent{agent_id}=>Some(agent_id.as_str()),_=>None});
    let _=write!(body,"<label>Recipient<select name=\"recipient_kind\"><option value=\"operator\" {}>Trusted host operator</option><option value=\"agent\" {}>Agent</option></select></label>",if agent.is_none(){"selected"}else{""},if agent.is_some(){"selected"}else{""});
    body.push_str(&field("recipient_agent_id","Recipient agent ID (only for Agent)",agent.unwrap_or(""),"text",false));
    body.push_str(&area("prompt","Question and recommendation",value.map(editable_prompt).unwrap_or(""),true));
    for n in 1..=count {let option=value.and_then(|q|q.options.get(n-1));let _=write!(body,"<fieldset><legend>Option {n}</legend>");body.push_str(&field(&format!("option_{n}_label"),"Option",option.map(|q|q.label.as_str()).unwrap_or(""),"text",true));body.push_str(&area(&format!("option_{n}_explanation"),"Explanation",option.map(|q|q.explanation.as_str()).unwrap_or(""),false));body.push_str("</fieldset>");}
    body.push_str("<label>Recommended option<select name=\"recommended_option_id\" required><option value=\"\">Select a recommendation</option>");
    for n in 1..=count{let key=format!("option-{n}");let _=write!(body,"<option value=\"{key}\" {}>Option {n}</option>",if value.is_some_and(|q|q.options.get(n-1).is_some_and(|option|q.recommended_option_id==option.id)){"selected"}else{""});}body.push_str("</select></label><button>Save question</button></form>");
    if posed.is_none(){body.push_str("<h2>Change number of options</h2><form method=\"get\" action=\"/questions/new\">");body.push_str(&field("option_count","Number of options",&count.to_string(),"number",true));body.push_str("<button>Start a new form</button></form>");}
    page(200,if posed.is_some(){"Replace open question"}else{"Pose a question"},body)
}
fn action_form(body:&mut String,q:&Question,action:&str,label:&str,answer:Option<&str>,options:bool) {
    let _=write!(body,"<form method=\"post\" action=\"{}/{}\"><fieldset><legend>{}</legend>",question_path(&q.id),action,escape(label));body.push_str(&hidden("operation_id",&identity()));body.push_str(&hidden("expected_revision",&q.revision.to_string()));
    if let Some(answer)=answer {body.push_str(&hidden("expected_answer_id",answer));}
    if options {body.push_str("<label>Answer<select name=\"option_id\" required><option value=\"\">Select an option</option>");for option in &q.posed.options{let _=write!(body,"<option value=\"{}\">{}</option>",escape(&option.id),escape(&option.label));}body.push_str("</select></label>");body.push_str(&area("explanation","Answer explanation","",false));}
    if action!="answer"{body.push_str(&area("reason","Reason","",true));}
    if action=="moot"{body.push_str(&field("caused_by_question_id","Question containing the later answer","","text",true));body.push_str(&field("caused_by_answer_id","Later answer ID","","text",true));}
    let _=write!(body,"<button>{}</button></fieldset></form>",escape(label));
}
async fn question_view(controller:&Controller,id:&str,after:Option<String>,actions:bool)->Result<UiResponse,UiResponse> {
    let result=query(controller,QuestionQuery::Get{question_id:id.into()}).await?;
    let QuestionQueryResult::Question(v)=result.as_ref() else{return Err(error(502,"Unexpected question projection"))};
    let q=&v.question;let mut body=nav();body.push_str("<p>This product decision does not authorize deployment, credentials or host changes.</p><dl>");term(&mut body,"Question",&q.id);term(&mut body,"Revision",&q.revision.to_string());term(&mut body,"Status",&format!("{:?}",q.status));term(&mut body,"Poser",&actor_label(&q.poser));term(&mut body,"Recipient",&principal_label(&q.posed.recipient));term(&mut body,"Current responsible owner",&principal_label(&v.current_responsible_owner));body.push_str("</dl><h2>Question</h2><pre>");body.push_str(&escape(&q.posed.prompt));body.push_str("</pre>");
    //= spec/impl/v2/implementation.md#shared-product-state
    //# The browser interface MUST present the information exposed through the shared agent interface.
    body.push_str("<details id=\"question-record\"><summary>Exact question, current effect and provenance</summary>");body.push_str(&raw_json(v));body.push_str("</details>");
    body.push_str("<ul>");
    for option in &q.posed.options {let _=write!(body,"<li><strong>{}</strong>{}<p>{}</p></li>",escape(&option.label),if option.id==q.posed.recommended_option_id{" (recommended)"}else{""},escape(&option.explanation));}body.push_str("</ul>");
    for(label,id)in[("Replaces",&q.supersedes),("Replaced by",&q.superseded_by)]{if let Some(id)=id{let _=write!(body,"<p>{label}: <a href=\"{}\">{}</a></p>",question_path(id),escape(id));}}
    body.push_str("<h2>Current answer effect</h2>");if let Some(answer)=&v.active_answer {body.push_str(&raw_json(answer));}else{body.push_str("<p>No effective answer.</p>");}
    if q.status==QuestionStatus::Open {
        if q.posed.recipient==PrincipalRef::TrustedHostOperator {action_form(&mut body,q,"answer","Record operator answer",None,true);}else{body.push_str("<p>Only this recipient's bound agent session can answer. The operator cannot answer as that agent.</p>");}
        action_form(&mut body,q,"cancel","Cancel open question",None,false);let _=write!(body,"<p><a href=\"{}/supersede\">Replace this open question</a></p>",question_path(id));
    }
    if q.status==QuestionStatus::Answered {if let Some(answer)=q.latest_answer_id.as_deref(){if q.posed.recipient==PrincipalRef::TrustedHostOperator {action_form(&mut body,q,"replace-answer","Replace answer",Some(answer),true);if q.active_answer_id.is_some(){action_form(&mut body,q,"withdraw-answer","Withdraw answer effect",Some(answer),false);}}if q.active_answer_id.is_some(){action_form(&mut body,q,"moot","Mark answer moot",Some(answer),false);}}}
    let _=write!(body,"<h2>Immutable history</h2><p><a href=\"{}\">Answers</a> · <a href=\"{}?history=actions\">Actions</a></p>",question_path(id),question_path(id));
    let query_value=if actions{QuestionQuery::Actions{question_id:id.into(),after,limit:page_limit(controller)?}}else{QuestionQuery::Answers{question_id:id.into(),after,limit:page_limit(controller)?}};
    match query(controller,query_value).await?.as_ref() {QuestionQueryResult::Answers(p)=>{for item in &p.items{body.push_str(&raw_json(item));}continuation(&mut body,&question_path(id),p.next_cursor.as_deref());},QuestionQueryResult::Actions(p)=>{for item in &p.items{body.push_str(&raw_json(item));}continuation(&mut body,&format!("{}?history=actions",question_path(id)),p.next_cursor.as_deref());},_=>return Err(error(502,"Unexpected history projection"))}
    let comment_route=format!("{}/comments",question_path(id));
    let _=write!(body,"<h2>Comments</h2><p><a href=\"{}\">Read immutable comments</a></p>",comment_route);
    body.push_str(&super::documents_reviews::comment_form(&comment_route,q.revision));
    Ok(page(200,"Question",body))
}
pub(super) async fn get(controller:&Controller,path:&str,raw_query:&str)->UiResponse {
    match get_inner(controller,path,raw_query).await{Ok(r)|Err(r)=>r}
}
async fn get_inner(controller:&Controller,path:&str,raw_query:&str)->Result<UiResponse,UiResponse> {
    let fields=parse_form(raw_query.as_bytes(),controller.limits()).map_err(|e|error(400,&e))?;
    if path=="/questions/new"{fields_only(&fields,&["option_count"]).map_err(|e|error(400,&e))?;let count=fields.get("option_count").map(|s|s.parse::<usize>()).transpose().map_err(|_|error(400,"Invalid option count"))?.unwrap_or(2);return Ok(pose_form(controller,None,count));}
    if path=="/decisions" {
        fields_only(&fields,&["after","recipient_agent_id"]).map_err(|e|error(400,&e))?;
        let viewer=fields.get("recipient_agent_id").filter(|v|!v.is_empty()).map(|agent_id|PrincipalRef::Agent{agent_id:agent_id.clone()}).unwrap_or(PrincipalRef::TrustedHostOperator);
        let result=query(controller,QuestionQuery::DecisionQueue{viewer:viewer.clone(),after:fields.get("after").cloned(),limit:page_limit(controller)?}).await?;
        let QuestionQueryResult::DecisionQueue(p)=result.as_ref() else{return Err(error(502,"Unexpected decision queue"))};
        let mut body=nav();let _=write!(body,"<p>Decisions owed by {}. The queue is observed at product revision {}; later changes can remove resolved items. Product decisions confer no protected authority.</p>",escape(&principal_label(&viewer)),p.observed_product_revision);
        body.push_str("<form method=\"get\" action=\"/decisions\">");body.push_str(&field("recipient_agent_id","Agent ID (blank for trusted host operator)",fields.get("recipient_agent_id").map(String::as_str).unwrap_or(""),"text",false));body.push_str("<button>View queue</button></form><ul>");
        for item in &p.items {match item {DecisionQueueItem::Question{question_id,prompt,..}=>{let _=write!(body,"<li>Question <a href=\"{}\">{}</a><p>{}</p></li>",question_path(question_id),escape(question_id),escape(prompt));},DecisionQueueItem::Review{review_id,summary,..}=>{let _=write!(body,"<li>Review <a href=\"/reviews/{}\">{}</a><p>{}</p></li>",encode(review_id),escape(review_id),escape(summary));}}}body.push_str("</ul>");
        let base=fields.get("recipient_agent_id").map(|id|format!("/decisions?recipient_agent_id={}",encode(id))).unwrap_or_else(||"/decisions".into());continuation(&mut body,&base,p.next_cursor.as_deref());return Ok(page(200,"Decision queue",body));
    }
    if path=="/questions" {
        fields_only(&fields,&["after","record_kind","record_id","status"]).map_err(|e|error(400,&e))?;
        let record=super::documents_reviews::list_record_filter(&fields).map_err(|e|error(400,&e))?;
        let status=match fields.get("status").map(String::as_str).filter(|value|!value.is_empty()) {
            None=>None,Some("open")=>Some(QuestionStatus::Open),Some("answered")=>Some(QuestionStatus::Answered),
            Some("cancelled")=>Some(QuestionStatus::Cancelled),Some("superseded")=>Some(QuestionStatus::Superseded),Some("moot")=>Some(QuestionStatus::Moot),
            _=>return Err(error(400,"Unsupported question status")),
        };
        let result=query(controller,QuestionQuery::List{record,status,after:fields.get("after").cloned(),limit:page_limit(controller)?}).await?;
        let QuestionQueryResult::Questions(p)=result.as_ref() else{return Err(error(502,"Unexpected questions list"))};
        let mut body=nav();body.push_str(&super::documents_reviews::list_filter_form("/questions",&fields,&[("open","Open"),("answered","Answered"),("cancelled","Cancelled"),("superseded","Superseded"),("moot","Moot")]));
        body.push_str("<ul>");for v in &p.items{let q=&v.question;let _=write!(body,"<li><a href=\"{}\">{}</a> — {:?}: {}</li>",question_path(&q.id),escape(&q.id),q.status,escape(&q.posed.prompt));}body.push_str("</ul>");
        continuation(&mut body,&super::documents_reviews::list_filter_base("/questions",&fields),p.next_cursor.as_deref());return Ok(page(200,"Questions",body));
    }
    let raw=path.strip_prefix("/questions/").ok_or_else(||error(404,"Unknown question route"))?;
    let (raw_id,sub)=raw.split_once('/').unwrap_or((raw,""));let id=decode(raw_id,false).map_err(|e|error(400,&e))?;
    if sub=="supersede" {if !fields.is_empty(){return Err(error(400,"Unexpected replacement query"))}let result=query(controller,QuestionQuery::Get{question_id:id}).await?;let QuestionQueryResult::Question(v)=result.as_ref() else{return Err(error(502,"Unexpected question"))};if v.question.status!=QuestionStatus::Open{return Err(error(409,"Only an open question can be replaced"))}return Ok(pose_form(controller,Some(&v.question),v.question.posed.options.len()));}
    if !sub.is_empty(){return Err(error(404,"Unknown question route"))}
    fields_only(&fields,&["after","history"]).map_err(|e|error(400,&e))?;let actions=match fields.get("history").map(String::as_str){None|Some("answers")=>false,Some("actions")=>true,_=>return Err(error(400,"Unknown history view"))};question_view(controller,&id,fields.get("after").cloned(),actions).await
}
fn parse_mutation(path:&str,fields:&Form,limits:&RuntimeLimits)->Result<QuestionMutation,String> {
    if path=="/questions" {if fields.contains_key("expected_revision")||fields.contains_key("reason"){return Err("New question cannot carry a previous revision or replacement reason".into())}return Ok(QuestionMutation::Pose{question:pose(fields,limits)?})}
    let (raw_id,action)=path.strip_prefix("/questions/").and_then(|s|s.split_once('/')).ok_or("Unknown question action")?;let question_id=decode(raw_id,false)?;let expected_revision=number(fields,"expected_revision")?;
    if action=="supersede"{return Ok(QuestionMutation::Supersede{question_id,expected_revision,reason:required(fields,"reason")?.into(),replacement:pose(fields,limits)?})}
    let mut allowed=vec!["operation_id","expected_revision"];
    match action {"answer"=>allowed.extend(["option_id","explanation"]),"cancel"=>allowed.push("reason"),"replace-answer"=>allowed.extend(["expected_answer_id","option_id","explanation","reason"]),"withdraw-answer"=>allowed.extend(["expected_answer_id","reason"]),"moot"=>allowed.extend(["expected_answer_id","caused_by_question_id","caused_by_answer_id","reason"]),_=>return Err("Unknown question action".into())};fields_only(fields,&allowed)?;
    let answer=||->Result<QuestionAnswer,String>{Ok(QuestionAnswer{option_id:required(fields,"option_id")?.into(),explanation:fields.get("explanation").cloned().unwrap_or_default()})};
    Ok(match action {
        "answer"=>QuestionMutation::Answer{question_id,expected_revision,answer:answer()?},
        "cancel"=>QuestionMutation::Cancel{question_id,expected_revision,reason:required(fields,"reason")?.into()},
        "replace-answer"=>QuestionMutation::SupersedeAnswer{question_id,expected_revision,expected_answer_id:required(fields,"expected_answer_id")?.into(),answer:answer()?,reason:required(fields,"reason")?.into()},
        "withdraw-answer"=>QuestionMutation::WithdrawAnswer{question_id,expected_revision,expected_answer_id:required(fields,"expected_answer_id")?.into(),reason:required(fields,"reason")?.into()},
        "moot"=>QuestionMutation::MarkMoot{question_id,expected_revision,expected_answer_id:required(fields,"expected_answer_id")?.into(),caused_by:AnswerRef{question_id:required(fields,"caused_by_question_id")?.into(),answer_id:required(fields,"caused_by_answer_id")?.into()},reason:required(fields,"reason")?.into()},
        _=>return Err("Unknown question action".into()),
    })
}
pub(super) async fn post(controller:&Controller,path:&str,fields:Form,operation_id:&str)->UiResponse {
    let mutation=match parse_mutation(path,&fields,controller.limits()){Ok(m)=>m,Err(e)=>return error(400,&e)};
    let reply=call(controller,Operation::MutateQuestion{operation_id:operation_id.into(),mutation}).await;
    mutation_reply_for(reply,operation_id,None,"/questions")
}
pub(super) fn committed(operation_id:&str,outcome:&QuestionMutationOutcome)->UiResponse {
    let q=&outcome.question.question;let path=question_path(&q.id);let mut body=nav();let _=write!(body,"<p>Recorded question revision {} under operation <code>{}</code>. This is a product decision, not protected release authority.</p><p><a href=\"{}\">View question and answer history</a> · <a href=\"/operations/{}\">Original receipt</a></p>",q.revision,escape(operation_id),path,encode(operation_id));let mut response=page(303,"Question change recorded",body);response.location=Some(path);response
}
