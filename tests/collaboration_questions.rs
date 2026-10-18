//! Actual controller transactions and browser/MCP adapters; controlled agents.
//! No provider, worker process, host or installed-release qualification.
use borg_runtime::{controller::{Controller,ControllerConfig},protocol::*,collaboration::{questions::*,documents::*,workflow::*},limits::{RuntimeLimits,WorkflowLimits},mcp::{Caller,Invocation},product::{self,UiRequest}};
use serde_json::{json,Value};
use std::{path::PathBuf,sync::{Arc,atomic::{AtomicU64,Ordering}},time::{SystemTime,UNIX_EPOCH}};
static NEXT:AtomicU64=AtomicU64::new(1);
fn unique()->String{format!("questions-{}-{}-{}",std::process::id(),SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),NEXT.fetch_add(1,Ordering::Relaxed))}
fn limits()->RuntimeLimits {
    let mut p=RuntimeLimits::seed().unwrap();p.execution.native.context_bytes=65_536;p.controller.command_wait_ms=5;
    p.workflow=Some(WorkflowLimits{record_bytes:32_768,text_bytes:4_096,options:8,records:512,revisions:4_096,page_items:8,page_bytes:32_768,waits_per_task:8,graph_scan_nodes:512,events_per_mutation:64,event_bytes:8_192,notice_bytes:8_192,pending_delivery_bytes:32_768,delivery_scan_rows:64});p.validate().unwrap();p
}
fn config(p:RuntimeLimits)->ControllerConfig{ControllerConfig{limits:Arc::new(p),validation_executable:PathBuf::from(env!("CARGO_BIN_EXE_borg-runtime")),..ControllerConfig::default()}}
fn input(agent:&str)->TaskInput {
    TaskInput{ instruction_source: None, pull_request: None, instruction:"Return a bounded structured value".into(),definition:Definition{charter: None, id:agent.into(),revision:"v1".into(),executor:ExecutorKind::Native,inference:Some(InferenceSettings::selected()),execution_profile:Some(ExecutionProfile::CollaborationV2{ publication: None,web_search:None}),limit_overrides:None},repository:None,repository_read:None,acceptance_commands:vec![],output_schema:Some(json!({"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false})),limits:Limits{deadline_unix_ms:SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()as u64+600_000,model_requests:32,output_bytes:16_384,result_bytes:RuntimeLimits::seed().unwrap().task_defaults.result_bytes,max_delegation_depth:4},dependencies:vec![],native_workflow:None,resolved_limits:None}
}
struct Lab{root:PathBuf,c:Controller,config:ControllerConfig}
impl Lab{fn new()->Self{Self::with(limits())}fn with(p:RuntimeLimits)->Self{let root=std::env::temp_dir().join(unique());std::fs::create_dir(&root).unwrap();let config=config(p);let c=Controller::open_with_config(&root.join("tasks.sqlite"),config.clone()).unwrap();Self{root,c,config}}async fn restart(self)->Self{let Self{root,c,config}=self;drop(c);released(&root.join("tasks.sqlite")).await;let c=Controller::open_with_config(&root.join("tasks.sqlite"),config.clone()).unwrap();Self{root,c,config}}}
async fn released(path:&std::path::Path){let mut name=path.as_os_str().to_owned();name.push(".controller-lock");tokio::time::timeout(std::time::Duration::from_secs(3),async{loop{let file=std::fs::OpenOptions::new().read(true).write(true).open(PathBuf::from(&name)).unwrap();if file.try_lock().is_ok(){break}tokio::time::sleep(std::time::Duration::from_millis(5)).await;}}).await.expect("previous controller owner released");}
async fn call(c:&Controller,command:Operation)->Reply{let reply=Box::pin(c.handle(Request{protocol_revision:WIRE_REVISION,request_id:unique(),command:command.clone()})).await;reply.validate_for(&command).unwrap();reply}
fn task(reply:Reply)->Task{match reply.outcome{Outcome::Ok{data:Data::Task(t)}=>*t,x=>panic!("expected task: {x:?}")}}
fn changed(reply:&Reply)->QuestionMutationOutcome{assert!(reply.commit.is_some(),"{reply:?}");match &reply.outcome{Outcome::Ok{data:Data::QuestionMutation(v)}=>v.clone(),x=>panic!("expected committed question: {x:?}")}}
fn refused(reply:&Reply,code:&str){assert!(matches!(&reply.outcome,Outcome::Refused{code:c,..}if c==code),"{reply:?}");}
async fn mutate(c:&Controller,op:&str,mutation:QuestionMutation)->Reply{call(c,Operation::MutateQuestion{operation_id:op.into(),mutation}).await}
async fn query(c:&Controller,query:QuestionQuery)->QuestionQueryResult{match call(c,Operation::QueryQuestion{query}).await.outcome{Outcome::Ok{data:Data::QuestionQuery(q)}=>q,x=>panic!("expected question query: {x:?}")}}
async fn get(c:&Controller,id:&str)->QuestionView{match query(c,QuestionQuery::Get{question_id:id.into()}).await{QuestionQueryResult::Question(v)=>v,x=>panic!("{x:?}")}}
async fn submit(c:&Controller,agent:&str)->Task{task(call(c,Operation::SubmitTask{operation_id:unique(),input:input(agent)}).await)}
fn pose_for(record:RecordRef,recipient:PrincipalRef)->PoseQuestion{PoseQuestion{record,recipient,prompt:"Choose one option. Recommended: keep.".into(),options:vec![QuestionOption{id:"keep".into(),label:"Keep".into(),explanation:"Preserve the current choice".into()},QuestionOption{id:"change".into(),label:"Change".into(),explanation:"Use the stated alternative".into()}],recommended_option_id:"keep".into()}}
async fn pose(c:&Controller,task:&Task,recipient:PrincipalRef)->Question{changed(&mutate(c,&unique(),QuestionMutation::Pose{question:pose_for(RecordRef::Task{task_id:task.id.clone()},recipient)}).await).question.question}
fn answer(q:&Question,option:&str)->QuestionMutation{QuestionMutation::Answer{question_id:q.id.clone(),expected_revision:q.revision,answer:QuestionAnswer{option_id:option.into(),explanation:"Explicit decision".into()}}}
async fn queue(c:&Controller,viewer:PrincipalRef,after:Option<String>,limit:usize)->DecisionQueuePage{match query(c,QuestionQuery::DecisionQueue{viewer,after,limit}).await{QuestionQueryResult::DecisionQueue(v)=>v,x=>panic!("{x:?}")}}
async fn actor(c:&Controller,agent:&str)->(Task,Caller){actor_profile(c,agent,ExecutionProfile::CollaborationV2{ publication: None,web_search:None}).await}
async fn actor_profile(c:&Controller,agent:&str,profile:ExecutionProfile)->(Task,Caller){
    let mut data=input(agent);data.definition.execution_profile=Some(profile);let t=task(call(c,Operation::SubmitTask{operation_id:unique(),input:data}).await);let w=Worker{id:unique(),incarnation:"inc".into(),coding_capacity:0,native_capacity:1};
    assert!(matches!(call(c,Operation::RegisterWorker{operation_id:unique(),worker:w.clone()}).await.outcome,Outcome::Ok{..}));
    let commands=call(c,Operation::AwaitCommands{operation_id:unique(),worker_id:w.id.clone(),incarnation:w.incarnation.clone(),acknowledged:vec![]}).await;
    let Outcome::Ok{data:Data::Commands(commands)}=commands.outcome else{panic!("{commands:?}")};
    let a=commands.into_iter().find_map(|command|match command{WorkerCommand::RunAttempt{attempt,..}if attempt.task_id==t.id=>Some(attempt),_=>None}).expect("own admitted attempt");
    task(call(c,Operation::ObserveWorker{report:Report{report_id:unique(),worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone(),task_id:t.id.clone(),attempt_id:a.id.clone(),generation:a.generation,observation:Observation::Started{thread_id:None,turn_id:None}}}).await);
    (t,Caller{task_id:a.task_id,attempt_id:a.id,generation:a.generation,worker_id:a.worker_id,incarnation:a.worker_incarnation})
}
async fn tool(c:&Controller,caller:&Caller,id:&str,name:&str,args:Value)->Reply{let invocation=Invocation{operation_id:id.into(),logical_call_id:format!("logical-{id}"),caller:caller.clone(),tool:name.into(),arguments:args};call(c,invocation.operation()).await}
async fn browser(c:&Controller,method:&str,path:&str,body:String,origin:&str)->product::UiResponse{Box::pin(product::handle(c,UiRequest{method:method.into(),path:path.into(),body:body.into_bytes(),origin:Some(origin.into()),expected_origin:"http://local.invalid".into()})).await}
fn enc(s:&str)->String{s.bytes().map(|b|if b.is_ascii_alphanumeric()||b"-._~".contains(&b){(b as char).to_string()}else{format!("%{b:02X}")}).collect()}
fn form(fields:&[(&str,String)])->String{fields.iter().map(|(k,v)|format!("{}={}",enc(k),enc(v))).collect::<Vec<_>>().join("&")}

#[tokio::test]
//= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
//= type=test
//# A blocking question MUST take no default answer.
//= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
//= type=test
//# A posted question's prompt, options and typed decision fields MUST be immutable.
//= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
//= type=test
//# A recorded answer MUST be immutable.

// Traceability scope: Question and answer identities and original receipts survive replay and owner reopen.
//= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
//= type=test
//# Every collaboration record MUST have a durable identity.

async fn question_records_immutable_answer_and_original_receipt_across_restart(){
    let lab=Lab::new();let t=submit(&lab.c,"owner").await;let q=pose(&lab.c,&t,PrincipalRef::TrustedHostOperator).await;
    assert!(get(&lab.c,&q.id).await.active_answer.is_none());
    let op="answer-original";let command=answer(&q,"keep");let first=mutate(&lab.c,op,command.clone()).await;let out=changed(&first);
    assert_eq!(out.question.question.posed,q.posed);assert_eq!(out.question.question.poser,q.poser);assert_eq!(out.new_answer.as_ref().unwrap().author.principal,PrincipalRef::TrustedHostOperator);
    let original=call(&lab.c,Operation::GetOperation{operation_id:op.into()}).await;
    let lab=lab.restart().await;let replay=mutate(&lab.c,op,command).await;assert_eq!(replay.outcome,first.outcome);assert_eq!(replay.commit,first.commit);
    let lookup=call(&lab.c,Operation::GetOperation{operation_id:op.into()}).await;assert_eq!(lookup.outcome,original.outcome);assert_eq!(lookup.commit,original.commit);
    refused(&mutate(&lab.c,op,answer(&q,"change")).await,"operation_conflict");
    let QuestionQueryResult::Answers(answers)=query(&lab.c,QuestionQuery::Answers{question_id:q.id,after:None,limit:8}).await else{panic!()};assert_eq!(answers.items,vec![out.new_answer.unwrap()]);assert!(answers.next_cursor.is_none());
}

#[tokio::test]
//= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
//= type=test
//# A question answer MUST match the direct recipient recorded on the question to the caller context established by the controller.
//= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
//= type=test
//# An answer replacement or withdrawal MUST be accepted only from the question's recorded direct recipient.
//= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
//= type=test
//# An application-supplied actor label MUST NOT be recorded as authenticated-human identity.
//= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
//= type=test
//# The controller MUST derive a collaboration mutation's actor provenance from its invocation context.
async fn question_recipient_race_rejects_foreign_actor_and_browser_impersonation(){
    let lab=Lab::new();let(t,caller)=actor(&lab.c,"recipient").await;let(_,other)=actor(&lab.c,"other").await;let(_,caller2)=actor(&lab.c,"recipient").await;let q=pose(&lab.c,&t,PrincipalRef::Agent{agent_id:"recipient".into()}).await;
    let args=json!({"question_id":q.id,"expected_revision":q.revision,"answer":{"option_id":"keep","explanation":"recipient"}});
    refused(&tool(&lab.c,&other,"foreign-answer","collab_question_answer",args.clone()).await,"recipient_mismatch");
    refused(&mutate(&lab.c,"host-answer",answer(&q,"keep")).await,"recipient_mismatch");
    let forged=browser(&lab.c,"POST",&format!("/questions/{}/answer",q.id),form(&[("operation_id","forged".into()),("expected_revision",q.revision.to_string()),("option_id","keep".into()),("actor","recipient".into())]),"http://local.invalid").await;assert_eq!(forged.status,400);
    let(a,b)=tokio::join!(tool(&lab.c,&caller,"race-a","collab_question_answer",args.clone()),tool(&lab.c,&caller2,"race-b","collab_question_answer",args));
    assert_eq!([&a,&b].into_iter().filter(|r|matches!(r.outcome,Outcome::Ok{..})).count(),1);
    assert_eq!([&a,&b].into_iter().filter(|r|matches!(&r.outcome,Outcome::Refused{code,..}if code=="stale_revision")).count(),1);
    let view=get(&lab.c,&q.id).await;let recorded=view.active_answer.unwrap();assert_eq!(recorded.author.principal,PrincipalRef::Agent{agent_id:"recipient".into()});assert!(matches!(recorded.author.provenance,ActorProvenance::BoundAgentSession{ref task_id,..}if task_id==&caller.task_id || task_id==&caller2.task_id));
    refused(&tool(&lab.c,&other,"foreign-withdraw","collab_answer_withdraw",json!({"question_id":q.id,"expected_revision":view.question.revision,"expected_answer_id":recorded.id,"reason":"cannot impersonate recipient"})).await,"recipient_mismatch");
    refused(&tool(&lab.c,&other,"foreign-replace","collab_answer_replace",json!({"question_id":q.id,"expected_revision":view.question.revision,"expected_answer_id":recorded.id,"answer":{"option_id":"change","explanation":"foreign replacement"},"reason":"cannot replace the recipient's answer"})).await,"recipient_mismatch");
    let unchanged=get(&lab.c,&q.id).await;assert_eq!(unchanged.question,view.question);assert_eq!(unchanged.active_answer,Some(recorded));
    let page=browser(&lab.c,"GET",&format!("/questions/{}",q.id),String::new(),"http://local.invalid").await;assert_eq!(page.status,200);assert!(page.body.contains("Agent recipient"));assert!(!page.body.contains("Record operator answer"));
}

#[tokio::test]
//= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
//= type=test
//# A question cancellation or supersession MUST preserve the original poser.
//= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
//= type=test
//# A replacement question MUST record the acting poser and the question it supersedes.
//= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
//= type=test
//# A question cancellation or supersession MUST record its acting caller context and reason.
async fn retired_asker_question_can_be_cancelled_or_superseded_without_rewriting_poser(){
    let lab=Lab::new();let(t,caller)=actor(&lab.c,"asker").await;
    let posed=pose_for(RecordRef::Task{task_id:t.id.clone()},PrincipalRef::TrustedHostOperator);
    let first=changed(&tool(&lab.c,&caller,"agent-pose","collab_question_pose",json!({"question":posed})).await).question.question;
    let second=changed(&tool(&lab.c,&caller,"agent-pose-two","collab_question_pose",json!({"question":posed})).await).question.question;
    let current=task(call(&lab.c,Operation::GetTask{task_id:t.id.clone()}).await);task(call(&lab.c,Operation::CancelTask{operation_id:"retire-asker".into(),task_id:t.id.clone(),expected_revision:current.revision}).await);
    let cancelled=changed(&mutate(&lab.c,"close-retired",QuestionMutation::Cancel{question_id:first.id.clone(),expected_revision:first.revision,reason:"Asker ended".into()}).await);
    assert_eq!(cancelled.question.question.poser,first.poser);assert_eq!(cancelled.question.question.status,QuestionStatus::Cancelled);
    let expected_actor=RecordedActor{principal:PrincipalRef::TrustedHostOperator,provenance:ActorProvenance::TrustedHost{path:TrustedHostPath::OperatorApi}};
    let cancellation=cancelled.action.as_ref().unwrap();assert_eq!(cancellation.actor,expected_actor);assert_eq!(cancellation.question_id,first.id);assert_eq!(cancellation.question_revision,first.revision+1);assert_eq!(cancellation.product_revision,cancelled.question.question.changed_product_revision);assert_eq!(cancellation.operation_id,"close-retired");assert_eq!(cancellation.action,QuestionAction::Cancelled{reason:"Asker ended".into()});
    let mut replacement=second.posed.clone();replacement.prompt="New decision. Recommended: change.".into();replacement.recommended_option_id="change".into();
    let replaced=changed(&mutate(&lab.c,"replace-retired",QuestionMutation::Supersede{question_id:second.id.clone(),expected_revision:second.revision,reason:"Refined question".into(),replacement}).await);
    let new=replaced.replacement.unwrap().question;assert_eq!(new.supersedes.as_deref(),Some(second.id.as_str()));assert_eq!(new.poser.principal,PrincipalRef::TrustedHostOperator);assert_eq!(replaced.question.question.poser,second.poser);assert_eq!(replaced.question.question.superseded_by.as_deref(),Some(new.id.as_str()));
    let supersession=replaced.action.as_ref().unwrap();assert_eq!(supersession.actor,expected_actor);assert_eq!(new.poser,expected_actor);assert_eq!(supersession.question_id,second.id);assert_eq!(supersession.question_revision,second.revision+1);assert_eq!(supersession.product_revision,replaced.question.question.changed_product_revision);assert_eq!(supersession.operation_id,"replace-retired");assert_eq!(supersession.action,QuestionAction::Superseded{replacement_question_id:new.id.clone(),reason:"Refined question".into()});
    let lab=lab.restart().await;
    for (id,expected) in [(&first.id,cancellation),(&second.id,supersession)] {let QuestionQueryResult::Actions(history)=query(&lab.c,QuestionQuery::Actions{question_id:id.clone(),after:None,limit:8}).await else{panic!()};assert_eq!(history.items,vec![expected.clone()]);}
    let observed=queue(&lab.c,PrincipalRef::TrustedHostOperator,None,8).await;assert_eq!(observed.items.len(),1);assert!(matches!(&observed.items[0],DecisionQueueItem::Question{question_id,..}if question_id==&new.id));
}

#[tokio::test]
//= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
//= type=test
//# A moot mark MUST NOT create a product decision effect.
//= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
//= type=test
//# A moot mark MUST remove the marked answer's product effect.
//= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
//= type=test
//# A moot mark MUST record its acting caller context and the answer that made the decision moot.
//= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
//= type=test
//# A recorded answer MUST be immutable.
//= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
//= type=test
//# An answer replacement or withdrawal MUST be accepted only from the question's recorded direct recipient.
//= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
//= type=test
//# An answer replacement or withdrawal MUST retain the original answer record.
//= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
//= type=test
//# An answered question MUST NOT be cancelled or superseded.
async fn answer_replacement_withdrawal_and_moot_preserve_original_records_and_never_reopen(){
    let lab=Lab::new();let t=submit(&lab.c,"owner").await;let q=pose(&lab.c,&t,PrincipalRef::TrustedHostOperator).await;
    let a=changed(&mutate(&lab.c,"first-answer",answer(&q,"keep")).await);let original=a.new_answer.unwrap();let mut current=a.question.question;
    refused(&mutate(&lab.c,"cancel-answered",QuestionMutation::Cancel{question_id:q.id.clone(),expected_revision:current.revision,reason:"invalid".into()}).await,"resolver_closed");
    refused(&mutate(&lab.c,"supersede-answered",QuestionMutation::Supersede{question_id:q.id.clone(),expected_revision:current.revision,reason:"invalid".into(),replacement:q.posed.clone()}).await,"resolver_closed");
    let replacement=changed(&mutate(&lab.c,"replace-answer",QuestionMutation::SupersedeAnswer{question_id:q.id.clone(),expected_revision:current.revision,expected_answer_id:original.id.clone(),answer:QuestionAnswer{option_id:"change".into(),explanation:"New evidence".into()},reason:"Changed judgment".into()}).await);let next=replacement.new_answer.unwrap();assert_eq!(next.replaces.as_deref(),Some(original.id.as_str()));current=replacement.question.question;
    refused(&mutate(&lab.c,"stale-answer",QuestionMutation::WithdrawAnswer{question_id:q.id.clone(),expected_revision:current.revision,expected_answer_id:original.id.clone(),reason:"Wrong answer head".into()}).await,"stale_answer");
    let withdrawn=changed(&mutate(&lab.c,"withdraw",QuestionMutation::WithdrawAnswer{question_id:q.id.clone(),expected_revision:current.revision,expected_answer_id:next.id.clone(),reason:"No longer effective".into()}).await);assert!(withdrawn.question.active_answer.is_none());assert_eq!(withdrawn.question.question.status,QuestionStatus::Answered);current=withdrawn.question.question;
    let restored=changed(&mutate(&lab.c,"replace-withdrawn",QuestionMutation::SupersedeAnswer{question_id:q.id.clone(),expected_revision:current.revision,expected_answer_id:next.id.clone(),answer:QuestionAnswer{option_id:"keep".into(),explanation:"New explicit answer".into()},reason:"New decision".into()}).await);let active=restored.new_answer.unwrap();
    let q2=pose(&lab.c,&t,PrincipalRef::TrustedHostOperator).await;let cause=changed(&mutate(&lab.c,"other-answer",answer(&q2,"change")).await).new_answer.unwrap();
    let cause_ref=AnswerRef{question_id:q2.id.clone(),answer_id:cause.id.clone()};
    let moot=changed(&mutate(&lab.c,"mark-moot",QuestionMutation::MarkMoot{question_id:q.id.clone(),expected_revision:restored.question.question.revision,expected_answer_id:active.id.clone(),caused_by:cause_ref.clone(),reason:"Other decision removed need".into()}).await);assert_eq!(moot.question.question.status,QuestionStatus::Moot);assert!(moot.question.active_answer.is_none());assert!(moot.new_answer.is_none());
    let recorded=moot.action.as_ref().unwrap();assert_eq!(recorded.actor,RecordedActor{principal:PrincipalRef::TrustedHostOperator,provenance:ActorProvenance::TrustedHost{path:TrustedHostPath::OperatorApi}});assert_eq!(recorded.question_id,q.id);assert_eq!(recorded.question_revision,restored.question.question.revision+1);assert_eq!(recorded.product_revision,moot.question.question.changed_product_revision);assert_eq!(recorded.operation_id,"mark-moot");assert_eq!(recorded.action,QuestionAction::MarkedMoot{answer_id:active.id.clone(),caused_by:cause_ref,reason:"Other decision removed need".into()});
    let lab=lab.restart().await;let QuestionQueryResult::Actions(actions)=query(&lab.c,QuestionQuery::Actions{question_id:q.id.clone(),after:None,limit:8}).await else{panic!()};assert_eq!(actions.items.last(),Some(recorded));assert_eq!(get(&lab.c,&q2.id).await.active_answer,Some(cause));assert_eq!(get(&lab.c,&q.id).await,moot.question);
    let QuestionQueryResult::Answers(history)=query(&lab.c,QuestionQuery::Answers{question_id:q.id.clone(),after:None,limit:8}).await else{panic!()};assert_eq!(history.items,vec![original,next,active]);
    assert!(queue(&lab.c,PrincipalRef::TrustedHostOperator,None,8).await.items.is_empty());
}

async fn create_document(c:&Controller,owner:PrincipalRef)->DocumentView {
    let r=call(c,Operation::MutateDocument{operation_id:unique(),mutation:DocumentMutation::Create{content:DocumentContent{title:"Decision document".into(),body:"Exact version one".into()},supplied_content_id:None,responsible_owner:owner}}).await;
    match r.outcome{Outcome::Ok{data:Data::DocumentMutation(DocumentMutationOutcome::Document{view})}=>view,x=>panic!("{x:?}")}
}

#[tokio::test]
//= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
//= type=test
//# An open question MUST remain cancellable or supersedable by its current responsible record owner when the asker is inactive.
async fn current_record_owner_closes_inactive_askers_questions_with_bound_context() {
    let lab=Lab::new();let(asker,asker_call)=actor(&lab.c,"inactive-asker").await;
    let(owner,owner_call)=actor(&lab.c,"current-owner").await;
    let principal=PrincipalRef::Agent{agent_id:owner.assigned_agent_id.clone()};
    let expected_actor=RecordedActor{principal:principal.clone(),provenance:ActorProvenance::BoundAgentSession{
        agent_id:owner.assigned_agent_id.clone(),session_id:owner.session_id.clone(),task_id:owner_call.task_id.clone(),
        attempt_id:owner_call.attempt_id.clone(),generation:owner_call.generation,worker_id:owner_call.worker_id.clone(),incarnation:owner_call.incarnation.clone()}};
    let doc=create_document(&lab.c,PrincipalRef::Agent{agent_id:asker.assigned_agent_id.clone()}).await;
    let posed=pose_for(RecordRef::Document{document_id:doc.document.id.clone()},PrincipalRef::TrustedHostOperator);
    let first=changed(&tool(&lab.c,&asker_call,"owner-first-pose","collab_question_pose",json!({"question":posed})).await).question.question;
    let second=changed(&tool(&lab.c,&asker_call,"owner-second-pose","collab_question_pose",json!({"question":posed})).await).question.question;
    let moved=call(&lab.c,Operation::MutateDocument{operation_id:"transfer-question-responsibility".into(),mutation:DocumentMutation::SetResponsibleOwner{
        document_id:doc.document.id,expected_revision:doc.document.revision,responsible_owner:principal.clone()}}).await;
    assert!(matches!(moved.outcome,Outcome::Ok{..}));
    let current=task(call(&lab.c,Operation::GetTask{task_id:asker.id.clone()}).await);
    task(call(&lab.c,Operation::CancelTask{operation_id:"make-asker-inactive".into(),task_id:asker.id.clone(),expected_revision:current.revision}).await);
    refused(&tool(&lab.c,&asker_call,"inactive-asker-cannot-close","collab_question_cancel",json!({"question_id":first.id,"expected_revision":first.revision,"reason":"Old caller cannot start new work"})).await,"invalid_transition");
    for original in [&first,&second] {let view=get(&lab.c,&original.id).await;assert_eq!(view.question,*original);assert_eq!(view.current_responsible_owner,principal);assert!(view.active_answer.is_none());}
    let cancelled=changed(&tool(&lab.c,&owner_call,"owner-cancel","collab_question_cancel",json!({"question_id":first.id,"expected_revision":first.revision,"reason":"Responsible owner closes obsolete question"})).await);
    let action=cancelled.action.as_ref().unwrap();assert_eq!(action.actor,expected_actor);assert_eq!(action.action,QuestionAction::Cancelled{reason:"Responsible owner closes obsolete question".into()});assert_eq!(cancelled.question.question.poser,first.poser);assert_eq!(cancelled.question.question.status,QuestionStatus::Cancelled);
    let replaced=changed(&tool(&lab.c,&owner_call,"owner-supersede","collab_question_supersede",json!({"question_id":second.id,"expected_revision":second.revision,"reason":"Responsible owner refines question","replacement":second.posed})).await);
    let replacement=&replaced.replacement.as_ref().unwrap().question;let replacement_action=replaced.action.as_ref().unwrap();
    assert_eq!(replacement.poser,expected_actor);assert_eq!(replacement.supersedes.as_deref(),Some(second.id.as_str()));assert_eq!(replaced.question.question.poser,second.poser);assert_eq!(replaced.question.question.status,QuestionStatus::Superseded);assert_eq!(replacement_action.actor,expected_actor);assert_eq!(replacement_action.action,QuestionAction::Superseded{replacement_question_id:replacement.id.clone(),reason:"Responsible owner refines question".into()});
    assert!(cancelled.new_answer.is_none());assert!(replaced.new_answer.is_none());
    let lab=lab.restart().await;
    for (id,expected) in [(&first.id,action),(&second.id,replacement_action)] {let QuestionQueryResult::Actions(history)=query(&lab.c,QuestionQuery::Actions{question_id:id.clone(),after:None,limit:8}).await else{panic!()};assert_eq!(history.items,vec![expected.clone()]);}
    let pending=queue(&lab.c,PrincipalRef::TrustedHostOperator,None,8).await;assert!(matches!(&pending.items[..],[DecisionQueueItem::Question{question_id,..}]if question_id==&replacement.id));
    assert_eq!(task(call(&lab.c,Operation::GetTask{task_id:owner.id}).await).input,owner.input);assert_eq!(task(call(&lab.c,Operation::GetTask{task_id:asker.id}).await).input,asker.input);
}

#[tokio::test]
async fn operator_question_wire_shape_rejects_multiple_answers_without_deciding() {
    // The wire format represents one selected option. This does not interpret
    // arbitrary prompt prose or establish the full one-decision requirement.
    let lab=Lab::new();let t=submit(&lab.c,"single-answer-shape").await;
    let q=pose(&lab.c,&t,PrincipalRef::TrustedHostOperator).await;
    for malformed in [
        json!({"option_id":["keep","change"],"explanation":"two selections"}),
        json!({"option_id":"keep","option_ids":["keep","change"],"explanation":"extra selections"}),
    ] {assert!(serde_json::from_value::<QuestionAnswer>(malformed).is_err());}
    let response=browser(&lab.c,"POST",&format!("/questions/{}/answer",q.id),form(&[("operation_id","two-browser-answers".into()),("expected_revision",q.revision.to_string()),("option_id","keep".into()),("option_id","change".into()),("explanation","two selections".into())]),"http://local.invalid").await;
    assert_eq!(response.status,400);assert_eq!(get(&lab.c,&q.id).await.question,q);assert!(get(&lab.c,&q.id).await.active_answer.is_none());
    let answer=changed(&mutate(&lab.c,"one-explicit-answer",answer(&q,"keep")).await);assert_eq!(answer.new_answer.as_ref().unwrap().answer.option_id,"keep");
    let QuestionQueryResult::Answers(history)=query(&lab.c,QuestionQuery::Answers{question_id:q.id,after:None,limit:8}).await else{panic!()};assert_eq!(history.items,vec![answer.new_answer.unwrap()]);
}
async fn request_review(c:&Controller,target:ReviewTarget,reviewer:PrincipalRef)->ReviewView {
    match call(c,Operation::MutateReview{operation_id:unique(),mutation:ReviewMutation::Request{target,reviewer,summary:"Review this exact version".into()}}).await.outcome{Outcome::Ok{data:Data::ReviewMutation(ReviewMutationOutcome::Review(v))}=>v,x=>panic!("{x:?}")}
}
#[tokio::test]
//= spec/impl/v2/implementation.md#the-supported-decision-queue-is-complete
//= type=test
//# The decision queue MUST include every open question directly routed to its viewer.
//= spec/impl/v2/implementation.md#the-supported-decision-queue-is-complete
//= type=test
//# The decision queue MUST include every open review directly routed to its viewer.
//= spec/impl/v2/implementation.md#the-supported-decision-queue-is-complete
//= type=test
//# A bounded decision-queue page MUST provide a continuation when more matching decisions remain.
async fn decision_queue_merges_all_supported_kinds_and_cursor_scopes_without_false_empty_page(){
    let lab=Lab::new();let(t,_)=actor(&lab.c,"owner").await;let q1=pose(&lab.c,&t,PrincipalRef::TrustedHostOperator).await;
    let doc=create_document(&lab.c,PrincipalRef::TrustedHostOperator).await;let review=request_review(&lab.c,ReviewTarget::DocumentVersion{version:doc.version.reference},PrincipalRef::TrustedHostOperator).await;
    let q2=pose(&lab.c,&t,PrincipalRef::TrustedHostOperator).await;
    let first=queue(&lab.c,PrincipalRef::TrustedHostOperator,None,1).await;assert!(matches!(&first.items[..],[DecisionQueueItem::Question{question_id,..}]if question_id==&q1.id));let cursor=first.next_cursor.unwrap();
    changed(&mutate(&lab.c,"close-first",answer(&q1,"keep")).await);let later=pose(&lab.c,&t,PrincipalRef::TrustedHostOperator).await;
    let second=queue(&lab.c,PrincipalRef::TrustedHostOperator,Some(cursor.clone()),1).await;assert_eq!(second.through_product_revision,first.through_product_revision);assert!(second.observed_product_revision>second.through_product_revision);assert!(matches!(&second.items[..],[DecisionQueueItem::Review{review_id,..}]if review_id==&review.review.id));
    let third=queue(&lab.c,PrincipalRef::TrustedHostOperator,second.next_cursor,1).await;assert!(matches!(&third.items[..],[DecisionQueueItem::Question{question_id,..}]if question_id==&q2.id));assert!(third.next_cursor.is_none());
    let refreshed=queue(&lab.c,PrincipalRef::TrustedHostOperator,None,8).await;assert!(refreshed.items.iter().any(|v|matches!(v,DecisionQueueItem::Question{question_id,..}if question_id==&later.id)));
    refused(&call(&lab.c,Operation::QueryQuestion{query:QuestionQuery::DecisionQueue{viewer:PrincipalRef::Agent{agent_id:"owner".into()},after:Some(cursor),limit:1}}).await,"invalid_input");
    let (_,agent)=actor(&lab.c,"queue-agent").await;let args=json!({"after":null,"limit":8,"viewer":{"kind":"trusted_host_operator"}});refused(&tool(&lab.c,&agent,"forged-viewer","collab_decisions_list",args).await,"invalid_input");
}

fn notice_delivery(reply: &Reply) -> borg_runtime::collaboration::notifications::DeliveryObservation {
    match &reply.outcome {
        Outcome::Ok { data: Data::CollaborationDelivery(value) } => value.clone(),
        other => panic!("canonical notice delivery expected: {other:?}"),
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#the-supported-decision-queue-is-complete
//= type=test
//# A notice outside the decision queue MUST NOT substitute for an owed decision's queue entry.
async fn acknowledged_question_and_review_notices_preserve_owed_queue_entries_until_decided() {
    let lab=Lab::new();let (accepted,caller)=actor(&lab.c,"notice-recipient").await;
    let viewer=PrincipalRef::Agent{agent_id:accepted.assigned_agent_id.clone()};
    let q=pose(&lab.c,&accepted,viewer.clone()).await;
    let document=create_document(&lab.c,PrincipalRef::TrustedHostOperator).await;
    let review=request_review(&lab.c,ReviewTarget::DocumentVersion{version:document.version.reference},viewer.clone()).await;
    let owed=queue(&lab.c,viewer.clone(),None,8).await;
    assert!(matches!(&owed.items[..],[DecisionQueueItem::Question{question_id,..},DecisionQueueItem::Review{review_id,..}]
        if question_id==&q.id && review_id==&review.review.id));
    assert!(owed.next_cursor.is_none());

    let observed=notice_delivery(&call(&lab.c,Operation::ObserveCollaborationDelivery{caller:caller.clone(),limit:8}).await);
    assert!(observed.pending.is_none());
    let page=observed.available.as_ref().expect("both open-record notices reach their actual recipient");
    assert_eq!(page.items.len(),2,"the unrelated document-owner notice is not delivered to this agent");
    assert!(page.continuation.is_none() && page.routing.is_none());
    assert!(page.items.iter().any(|event|event.subject==RecordRef::Question{question_id:q.id.clone()}
        && matches!(&event.detail,ActivityDetail::Question(QuestionChange::Posed{recipient,..}) if recipient==&viewer)));
    assert!(page.items.iter().any(|event|event.subject==RecordRef::Review{review_id:review.review.id.clone()}
        && matches!(&event.detail,ActivityDetail::Review(ReviewChange::Requested{target,reviewer,..})
            if target==&review.review.target && reviewer==&viewer)));
    let prepare=Operation::PrepareCollaborationDelivery{operation_id:"notice-prepare".into(),caller:caller.clone(),
        expected_cursor:observed.acknowledged_cursor.clone(),expected_routing_cursor:observed.routing_cursor.clone(),
        through:page.through.clone(),limit:8};
    let prepared=call(&lab.c,prepare.clone()).await;
    let batch=notice_delivery(&prepared).pending.unwrap();
    assert_eq!(batch.page,*page);assert!(batch.delivery.is_none());
    assert_eq!(queue(&lab.c,viewer.clone(),None,8).await,owed);
    let acknowledge=Operation::AcknowledgeCollaborationDelivery{operation_id:"notice-ack".into(),caller:caller.clone(),
        batch_id:batch.batch_id.clone(),context_binding:"notice-original-context".into()};
    let unproved=Operation::AcknowledgeCollaborationDelivery{operation_id:"notice-unproved-ack".into(),caller:caller.clone(),
        batch_id:batch.batch_id.clone(),context_binding:"notice-original-context".into()};
    refused(&call(&lab.c,unproved).await,"delivery_pending");
    assert_eq!(queue(&lab.c,viewer.clone(),None,8).await,owed);
    let begin=Operation::BeginCollaborationDelivery{operation_id:"notice-begin".into(),caller:caller.clone(),
        batch_id:batch.batch_id.clone(),context_binding:"notice-original-context".into()};
    let begun=call(&lab.c,begin.clone()).await;
    assert_eq!(notice_delivery(&begun).pending.as_ref().unwrap().delivery.as_ref().unwrap().context_binding,"notice-original-context");
    refused(&call(&lab.c,Operation::AcknowledgeCollaborationDelivery{operation_id:"notice-wrong-context".into(),
        caller:caller.clone(),batch_id:batch.batch_id.clone(),context_binding:"notice-other-context".into()}).await,"delivery_pending");
    // This is the controller's protocol ACK after a consumer retains the notice;
    // it does not claim that a provider or supplied executor ran in this fixture.
    let acknowledged=call(&lab.c,acknowledge.clone()).await;
    let delivered=notice_delivery(&acknowledged);
    assert!(delivered.pending.is_none());assert_eq!(delivered.acknowledged_cursor,batch.page.to_cursor);
    assert_ne!(delivered.acknowledged_cursor,observed.acknowledged_cursor);
    assert_eq!(queue(&lab.c,viewer.clone(),None,8).await,owed);
    assert_eq!(get(&lab.c,&q.id).await.question,q);
    match call(&lab.c,Operation::QueryReview{query:ReviewQuery::Get{review_id:review.review.id.clone()}}).await.outcome {
        Outcome::Ok{data:Data::ReviewQuery(ReviewQueryResult::Review(value))}=>assert_eq!(value,review),other=>panic!("{other:?}"),
    }
    assert_eq!(task(call(&lab.c,Operation::GetTask{task_id:accepted.id.clone()}).await).input,accepted.input);

    let lab=lab.restart().await;
    for (operation,original) in [(prepare,&prepared),(begin,&begun),(acknowledge.clone(),&acknowledged)] {
        let replay=call(&lab.c,operation).await;
        assert_eq!(replay.outcome,original.outcome);assert_eq!(replay.commit,original.commit);
    }
    let after_restart=notice_delivery(&call(&lab.c,Operation::ObserveCollaborationDelivery{caller:caller.clone(),limit:8}).await);
    assert!(after_restart.pending.is_none());assert_eq!(after_restart.acknowledged_cursor,batch.page.to_cursor);
    assert!(after_restart.available.as_ref().is_none_or(|available|!available.has_input()),"an acknowledged notice is not delivered again");
    assert_eq!(queue(&lab.c,viewer.clone(),None,8).await,owed,"durable notice consumption does not answer either decision");

    refused(&mutate(&lab.c,"notice-foreign-answer",answer(&q,"keep")).await,"recipient_mismatch");
    assert_eq!(queue(&lab.c,viewer.clone(),None,8).await,owed);
    let answer_arguments=json!({"question_id":q.id,"expected_revision":q.revision,
        "answer":{"option_id":"keep","explanation":"Explicitly answered after reading the notice"}});
    let answered=tool(&lab.c,&caller,"notice-answer","collab_question_answer",answer_arguments.clone()).await;
    assert_eq!(changed(&answered).question.question.status,QuestionStatus::Answered);
    let remaining=queue(&lab.c,viewer.clone(),None,8).await;
    assert!(matches!(&remaining.items[..],[DecisionQueueItem::Review{review_id,..}] if review_id==&review.review.id));
    let replay=call(&lab.c,acknowledge.clone()).await;
    assert_eq!(replay.outcome,acknowledged.outcome);assert_eq!(replay.commit,acknowledged.commit);
    assert_eq!(queue(&lab.c,viewer.clone(),None,8).await,remaining,"old notice replay cannot remove the still-open review or restore the answered question");
    let review_arguments=json!({"review_id":review.review.id,"expected_revision":review.review.revision,
        "verdict":"approved","explanation":"Explicit exact-version review after reading the notice"});
    let decided=tool(&lab.c,&caller,"notice-review","collab_review_decide",review_arguments.clone()).await;
    match &decided.outcome {Outcome::Ok{data:Data::ReviewMutation(ReviewMutationOutcome::Review(value))}=>{
        assert_eq!(value.review.status,ReviewStatus::Decided);assert_eq!(value.decision.as_ref().unwrap().target,review.review.target);
    },other=>panic!("{other:?}")}
    assert!(queue(&lab.c,viewer.clone(),None,8).await.items.is_empty());
    let lab=lab.restart().await;
    for (operation,original) in [(acknowledge,&acknowledged),
        (Invocation{operation_id:"notice-answer".into(),logical_call_id:"logical-notice-answer".into(),caller:caller.clone(),tool:"collab_question_answer".into(),arguments:answer_arguments}.operation(),&answered),
        (Invocation{operation_id:"notice-review".into(),logical_call_id:"logical-notice-review".into(),caller:caller.clone(),tool:"collab_review_decide".into(),arguments:review_arguments}.operation(),&decided)] {
        let replay=call(&lab.c,operation).await;assert_eq!(replay.outcome,original.outcome);assert_eq!(replay.commit,original.commit);
    }
    assert!(queue(&lab.c,viewer,None,8).await.items.is_empty());
    assert_eq!(task(call(&lab.c,Operation::GetTask{task_id:accepted.id}).await).input,accepted.input);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
//= type=test
//# A record-owner change MUST preserve its open questions.
async fn record_owner_change_preserves_question_recipient_poser_and_accepted_execution(){
    let lab=Lab::new();let t=submit(&lab.c,"original-owner").await;submit(&lab.c,"next-owner").await;
    let doc=create_document(&lab.c,PrincipalRef::Agent{agent_id:"original-owner".into()}).await;
    let q=changed(&mutate(&lab.c,"document-question",QuestionMutation::Pose{question:pose_for(RecordRef::Document{document_id:doc.document.id.clone()},PrincipalRef::TrustedHostOperator)}).await).question.question;
    let changed_owner=call(&lab.c,Operation::MutateDocument{operation_id:"change-owner".into(),mutation:DocumentMutation::SetResponsibleOwner{document_id:doc.document.id,expected_revision:doc.document.revision,responsible_owner:PrincipalRef::Agent{agent_id:"next-owner".into()}}}).await;assert!(matches!(changed_owner.outcome,Outcome::Ok{..}));
    let observed=get(&lab.c,&q.id).await;assert_eq!(observed.question,q);assert_eq!(observed.current_responsible_owner,PrincipalRef::Agent{agent_id:"next-owner".into()});assert_eq!(observed.question.status,QuestionStatus::Open);
    assert_eq!(task(call(&lab.c,Operation::GetTask{task_id:t.id.clone()}).await).input,t.input);
}

#[tokio::test]
async fn comments_touch_only_question_revision_and_browser_origin_refusal_is_no_effect(){
    let lab=Lab::new();let t=submit(&lab.c,"owner").await;let q=pose(&lab.c,&t,PrincipalRef::TrustedHostOperator).await;
    let path=format!("/questions/{}/comments",q.id);let fields=form(&[("operation_id","comment-one".into()),("expected_revision",q.revision.to_string()),("body","<script>plain comment</script>".into())]);
    let foreign=browser(&lab.c,"POST",&path,fields.clone(),"http://other.invalid").await;assert_eq!(foreign.status,403);assert_eq!(get(&lab.c,&q.id).await.question,q);
    let response=browser(&lab.c,"POST",&path,fields,"http://local.invalid").await;assert_eq!(response.status,303,"{}",response.body);
    let v=get(&lab.c,&q.id).await;assert_eq!(v.question.posed,q.posed);assert_eq!(v.question.poser,q.poser);assert_eq!(v.question.revision,q.revision+1);assert!(v.active_answer.is_none());
    refused(&mutate(&lab.c,"stale-after-comment",answer(&q,"keep")).await,"stale_revision");
    let reply=call(&lab.c,Operation::QueryDocument{query:DocumentQuery::Comments{record:RecordRef::Question{question_id:q.id.clone()},after:None,limit:8}}).await;
    let Outcome::Ok{data:Data::DocumentQuery(DocumentQueryOutcome::Comments(comments))}=reply.outcome else{panic!("{reply:?}")};assert_eq!(comments.items.len(),1);assert_eq!(comments.items[0].body,"<script>plain comment</script>");assert!(matches!(comments.items[0].author.provenance,ActorProvenance::TrustedHost{path:TrustedHostPath::Browser}));
    let page=browser(&lab.c,"GET",&path,String::new(),"http://local.invalid").await;assert_eq!(page.status,200);assert!(!page.body.contains("<script>plain"));assert!(page.body.contains("&lt;script&gt;plain"));
}

#[tokio::test]
async fn invalid_replacement_and_capacity_refusal_leave_original_question_and_queue_intact(){
    let mut p=limits();p.workflow.as_mut().unwrap().records=1;let lab=Lab::with(p);let t=submit(&lab.c,"owner").await;let q=pose(&lab.c,&t,PrincipalRef::TrustedHostOperator).await;
    let mut bad=q.posed.clone();bad.recommended_option_id="missing".into();refused(&mutate(&lab.c,"bad-replacement",QuestionMutation::Supersede{question_id:q.id.clone(),expected_revision:q.revision,reason:"refine".into(),replacement:bad}).await,"invalid_input");assert_eq!(get(&lab.c,&q.id).await.question,q);
    refused(&mutate(&lab.c,"capacity-replacement",QuestionMutation::Supersede{question_id:q.id.clone(),expected_revision:q.revision,reason:"refine".into(),replacement:q.posed.clone()}).await,"capacity");assert_eq!(get(&lab.c,&q.id).await.question,q);
    let pending=queue(&lab.c,PrincipalRef::TrustedHostOperator,None,8).await;assert_eq!(pending.items.len(),1);assert!(pending.next_cursor.is_none());
    let QuestionQueryResult::Actions(actions)=query(&lab.c,QuestionQuery::Actions{question_id:q.id,after:None,limit:8}).await else{panic!()};assert!(actions.items.is_empty());
}

#[tokio::test]
async fn oversized_first_queue_item_refuses_instead_of_hiding_an_owed_decision(){
    let mut p=limits();p.workflow.as_mut().unwrap().page_bytes=512;let lab=Lab::with(p);let t=submit(&lab.c,"owner").await;let mut posed=pose_for(RecordRef::Task{task_id:t.id},PrincipalRef::TrustedHostOperator);posed.prompt=format!("Recommended: keep. {}","x".repeat(1024));let q=changed(&mutate(&lab.c,"large-question",QuestionMutation::Pose{question:posed}).await).question.question;
    let reply=call(&lab.c,Operation::QueryQuestion{query:QuestionQuery::DecisionQueue{viewer:PrincipalRef::TrustedHostOperator,after:None,limit:8}}).await;refused(&reply,"response_bound");assert!(matches!(&reply.outcome,Outcome::Refused{message,..}if message.contains("workflow.page_bytes")));assert_eq!(get(&lab.c,&q.id).await.question,q);
}

#[tokio::test]
async fn question_tools_are_closed_and_disabled_without_explicit_workflow_metadata(){
    let v2=ExecutionProfile::CollaborationV2{ publication: None,web_search:None};
    let mut p=limits();p.workflow=None;let lab=Lab::with(p);let t=submit(&lab.c,"owner").await;
    // Test the explicit V2 projection: absence of workflow metadata must still
    // withhold added tools. catalogue() alone deliberately remains V1-only.
    assert!(!borg_runtime::mcp::catalogue_for_profile(lab.c.limits(),Some(&v2)).iter().any(|tool|tool.name=="collab_question_pose"));
    let disabled=mutate(&lab.c,"disabled",QuestionMutation::Pose{question:pose_for(RecordRef::Task{task_id:t.id},PrincipalRef::TrustedHostOperator)}).await;
    refused(&disabled,"workflow_disabled");
    assert!(matches!(&disabled.outcome,Outcome::Refused{message,..}if message=="workflow policy is not selected"));
    let p=limits();let tools=borg_runtime::mcp::catalogue_for_profile(&p,Some(&v2));let question_names=tools.iter().filter(|t|t.name.starts_with("collab_question")||t.name.starts_with("collab_answer")||t.name=="collab_decisions_list").map(|t|t.name.as_str()).collect::<std::collections::BTreeSet<_>>();
    let expected=["collab_question_pose","collab_question_answer","collab_question_cancel","collab_question_supersede","collab_answer_replace","collab_answer_withdraw","collab_answer_mark_moot","collab_question_get","collab_questions_list","collab_question_answers","collab_question_actions","collab_decisions_list"].into_iter().collect::<std::collections::BTreeSet<_>>();
    assert_eq!(question_names,expected);
    for name in question_names {let tool=tools.iter().find(|t|t.name==name).unwrap();assert_eq!(tool.input_schema["additionalProperties"],false);assert!(tool.input_schema["properties"].get("actor").is_none());assert!(tool.input_schema["properties"].get("caller").is_none());}
    let valid=json!({"after":null,"limit":1});
    assert!(borg_runtime::mcp::validate_arguments_for_profile("collab_decisions_list",&valid,&p,Some(&v2)).is_ok());
    assert_eq!(borg_runtime::mcp::validate_arguments_for_profile("collab_decisions_list",&valid,lab.c.limits(),Some(&v2)).unwrap_err(),"collaboration workflow tools are not configured");
    assert_eq!(borg_runtime::mcp::validate_arguments_for_profile("collab_decisions_list",&json!({"after":null,"limit":1,"viewer":{"kind":"trusted_host_operator"}}),&p,Some(&v2)).unwrap_err(),"question tool arguments differ from closed schema");
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn sqlite_commit_failure_rolls_back_answer_activity_and_queue_membership(){
    let mut lab=Lab::new();let t=submit(&lab.c,"owner").await;let q=pose(&lab.c,&t,PrincipalRef::TrustedHostOperator).await;
    let fail=Arc::new(std::sync::atomic::AtomicBool::new(false));lab.config.qualification_fail_next_commit=Some(fail.clone());let lab=lab.restart().await;
    fail.store(true,Ordering::SeqCst);let failed=mutate(&lab.c,"commit-cut",answer(&q,"keep")).await;assert!(!fail.load(Ordering::SeqCst),"actual COMMIT hook consumed");refused(&failed,"store_unavailable");assert!(failed.commit.is_none());
    assert_eq!(get(&lab.c,&q.id).await.question,q);assert!(get(&lab.c,&q.id).await.active_answer.is_none());assert_eq!(queue(&lab.c,PrincipalRef::TrustedHostOperator,None,8).await.items.len(),1);
    let QuestionQueryResult::Answers(p)=query(&lab.c,QuestionQuery::Answers{question_id:q.id.clone(),after:None,limit:8}).await else{panic!()};assert!(p.items.is_empty());
    let original=call(&lab.c,Operation::GetOperation{operation_id:"commit-cut".into()}).await;assert!(!matches!(original.outcome,Outcome::Ok{data:Data::Operation(_)}));
    // A fresh occurrence is allowed only after this controlled rollback is known.
    let applied=changed(&mutate(&lab.c,"after-known-rollback",answer(&q,"keep")).await);assert_eq!(applied.question.question.revision,q.revision+1);
}

async fn rpc(c:&Controller,caller:&Caller,method:&str,params:Value)->Value {
    let body=serde_json::to_vec(&json!({"jsonrpc":"2.0","id":7,"method":method,"params":params})).unwrap();let header=serde_json::to_string(caller).unwrap();let(status,body)=borg_runtime::mcp::handle(c,Some(&header),Some(borg_runtime::mcp::VERSION),&body).await;assert_eq!(status,200);serde_json::from_slice(&body).unwrap()
}
fn rpc_result(value:Value)->Reply {let raw=value["result"]["content"][0]["text"].as_str().unwrap_or_else(||panic!("expected MCP tool result: {value}"));serde_json::from_str::<borg_runtime::mcp::CallResult>(raw).unwrap().reply}
fn rpc_params(op:&str,name:&str,arguments:Value)->Value{json!({"name":name,"arguments":arguments,"_meta":{"borg":{"operation_id":op,"logical_call_id":format!("logical-{op}")}}})}
#[tokio::test]
//= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
//= type=test
//# An application-supplied actor label MUST NOT be recorded as authenticated-human identity.
//= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
//= type=test
//# The controller MUST derive a collaboration mutation's actor provenance from its invocation context.
async fn actual_mcp_adapter_and_browser_share_one_question_answer_and_original_occurrence(){
    let lab=Lab::new();let(t,caller)=actor(&lab.c,"asking-agent").await;let posed=pose_for(RecordRef::Task{task_id:t.id},PrincipalRef::TrustedHostOperator);
    let params=rpc_params("rpc-pose","collab_question_pose",json!({"question":posed}));let first=rpc_result(rpc(&lab.c,&caller,"tools/call",params.clone()).await);let q=changed(&first).question.question;assert_eq!(q.poser.principal,PrincipalRef::Agent{agent_id:"asking-agent".into()});
    let response=browser(&lab.c,"POST",&format!("/questions/{}/answer",q.id),form(&[("operation_id","browser-answer".into()),("expected_revision",q.revision.to_string()),("option_id","keep".into()),("explanation","One explicit operator decision".into())]),"http://local.invalid").await;assert_eq!(response.status,303,"{}",response.body);
    let read=rpc_result(rpc(&lab.c,&caller,"tools/call",rpc_params("rpc-read","collab_question_get",json!({"question_id":q.id}))).await);
    let Outcome::Ok{data:Data::QuestionQuery(QuestionQueryResult::Question(v))}=read.outcome else{panic!("{read:?}")};assert_eq!(v,get(&lab.c,&q.id).await);assert!(matches!(v.active_answer.unwrap().author.provenance,ActorProvenance::TrustedHost{path:TrustedHostPath::Browser}));
    let replay=rpc_result(rpc(&lab.c,&caller,"tools/call",params).await);assert_eq!(replay.outcome,first.outcome);assert_eq!(replay.commit,first.commit);
    let lookup=rpc_result(rpc(&lab.c,&caller,"borg/calls/get",json!({"call_id":"rpc-pose"})).await);assert_eq!(lookup.commit,first.commit);let Outcome::Ok{data:Data::Operation(original)}=lookup.outcome else{panic!()};assert_eq!(serde_json::from_value::<Outcome>(original.outcome.unwrap()).unwrap(),first.outcome);
}

#[tokio::test]
async fn v1_never_gains_question_tools_when_workflow_metadata_is_enabled(){
    let lab=Lab::new();let original_profile=ExecutionProfile::CollaborationV1{web_search:None};let before=serde_json::to_vec(&original_profile).unwrap();assert_eq!(before,br#"{"kind":"collaboration_v1"}"#);
    let(t,caller)=actor_profile(&lab.c,"legacy-agent",original_profile).await;let q=pose(&lab.c,&t,PrincipalRef::TrustedHostOperator).await;
    let posed=q.posed.clone();let cases=[
        ("collab_question_pose",json!({"question":posed})),
        ("collab_question_answer",json!({"question_id":q.id,"expected_revision":q.revision,"answer":{"option_id":"keep","explanation":"explicit"}})),
        ("collab_question_cancel",json!({"question_id":q.id,"expected_revision":q.revision,"reason":"explicit"})),
        ("collab_question_supersede",json!({"question_id":q.id,"expected_revision":q.revision,"reason":"explicit","replacement":posed})),
        ("collab_answer_replace",json!({"question_id":q.id,"expected_revision":q.revision,"expected_answer_id":"not-yet-an-answer","answer":{"option_id":"keep","explanation":"explicit"},"reason":"explicit"})),
        ("collab_answer_withdraw",json!({"question_id":q.id,"expected_revision":q.revision,"expected_answer_id":"not-yet-an-answer","reason":"explicit"})),
        ("collab_answer_mark_moot",json!({"question_id":q.id,"expected_revision":q.revision,"expected_answer_id":"not-yet-an-answer","caused_by":{"question_id":q.id,"answer_id":"absent"},"reason":"explicit"})),
        ("collab_question_get",json!({"question_id":q.id})),
        ("collab_questions_list",json!({"record":null,"status":null,"after":null,"limit":1})),
        ("collab_question_answers",json!({"question_id":q.id,"after":null,"limit":1})),
        ("collab_question_actions",json!({"question_id":q.id,"after":null,"limit":1})),
        ("collab_decisions_list",json!({"after":null,"limit":1})),
    ];
    let v2=ExecutionProfile::CollaborationV2{ publication: None,web_search:None};
    for(index,(name,args))in cases.into_iter().enumerate(){
        // Every argument object is structurally valid for V2. The exact V1
        // refusal must precede domain lookup/mutation, even for absent answers.
        assert!(borg_runtime::mcp::validate_arguments_for_profile(name,&args,lab.c.limits(),Some(&v2)).is_ok(),"{name}");
        let id=format!("v1-denied-{index}");
        let result=tool(&lab.c,&caller,&id,name,args.clone()).await;
        refused(&result,"invalid_input");
        assert!(matches!(&result.outcome,Outcome::Refused{message,..}if message=="collaboration workflow tools are not configured"));
        assert_eq!(result.commit.as_ref().unwrap().operation_id,id);
        let replay=tool(&lab.c,&caller,&id,name,args).await;
        assert_eq!(replay.outcome,result.outcome);assert_eq!(replay.commit,result.commit);
        let lookup=call(&lab.c,Operation::GetOperation{operation_id:id.clone()}).await;
        assert_eq!(lookup.commit,result.commit);
        let Outcome::Ok{data:Data::Operation(original)}=lookup.outcome else{panic!("{lookup:?}")};
        assert_eq!(original.id,id);
        assert_eq!(serde_json::from_value::<Outcome>(original.outcome.unwrap()).unwrap(),result.outcome);
    }
    let catalogue=rpc(&lab.c,&caller,"tools/list",json!({})).await;let tools=catalogue["result"]["tools"].as_array().unwrap();assert!(!tools.iter().any(|t|t["name"].as_str().is_some_and(|n|n.starts_with("collab_question")||n.starts_with("collab_answer")||n=="collab_decisions_list")));
    assert_eq!(get(&lab.c,&q.id).await.question,q);let observed=task(call(&lab.c,Operation::GetTask{task_id:t.id}).await);assert_eq!(serde_json::to_vec(observed.input.definition.execution_profile.as_ref().unwrap()).unwrap(),before);assert_eq!(observed.input,t.input);
}


#[tokio::test]
//= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
//= type=test
//# A question's prompt MUST state the recommended option.
async fn posted_prompt_contains_one_deterministic_recommendation_and_composition_respects_bounds(){
    let lab=Lab::new();let t=submit(&lab.c,"recommendation-owner").await;
    let mut raw=pose_for(RecordRef::Task{task_id:t.id.clone()},PrincipalRef::TrustedHostOperator);
    raw.prompt="Choose one option.".into();
    let expected="Choose one option.\n\nRecommended option: \"keep\" — \"Keep\".";
    let command=QuestionMutation::Pose{question:raw.clone()};
    let committed=mutate(&lab.c,"composed-recommendation",command.clone()).await;
    let q=changed(&committed).question.question;
    assert_eq!(q.posed.prompt,expected);assert_eq!(q.posed.options,raw.options);
    assert_eq!(raw.prompt,"Choose one option.");
    let replay=mutate(&lab.c,"composed-recommendation",command).await;
    assert_eq!(replay.outcome,committed.outcome);assert_eq!(replay.commit,committed.commit);
    let page=queue(&lab.c,PrincipalRef::TrustedHostOperator,None,8).await;
    assert!(matches!(&page.items[..],[DecisionQueueItem::Question{prompt,..}]if prompt==expected));
    let same=changed(&mutate(&lab.c,"canonical-prompt-once",QuestionMutation::Pose{question:q.posed.clone()}).await).question.question;
    assert_eq!(same.posed.prompt,expected);
    let mut wrong=q.posed.clone();wrong.recommended_option_id="change".into();
    refused(&mutate(&lab.c,"different-reserved-recommendation",QuestionMutation::Pose{question:wrong}).await,"invalid_input");
    let mut full=raw;full.prompt="x".repeat(lab.c.limits().workflow.as_ref().unwrap().text_bytes);
    let refused_size=mutate(&lab.c,"composed-prompt-too-large",QuestionMutation::Pose{question:full}).await;
    refused(&refused_size,"input_bound");
    assert!(matches!(&refused_size.outcome,Outcome::Refused{message,..}if message.contains("workflow.text_bytes")));
    assert_eq!(queue(&lab.c,PrincipalRef::TrustedHostOperator,None,8).await.items.len(),2);
    let lab=lab.restart().await;assert_eq!(get(&lab.c,&q.id).await.question,q);
}

#[tokio::test]
async fn reopening_refuses_rolled_back_answer_head_and_resurrected_or_inconsistent_effect_history(){
    for corrupt in ["rolled_head","withdrawn_effect","replacement_lineage","replacement_action","missing_action"] {
        let lab=Lab::new();let t=submit(&lab.c,"lineage-owner").await;let q=pose(&lab.c,&t,PrincipalRef::TrustedHostOperator).await;
        let initial=changed(&mutate(&lab.c,"lineage-answer",answer(&q,"keep")).await);let a=initial.new_answer.unwrap();
        let replacement=changed(&mutate(&lab.c,"lineage-replacement",QuestionMutation::SupersedeAnswer{question_id:q.id.clone(),expected_revision:initial.question.question.revision,expected_answer_id:a.id.clone(),answer:QuestionAnswer{option_id:"change".into(),explanation:"Revised decision".into()},reason:"New evidence".into()}).await);
        let b=replacement.new_answer.unwrap();let action=replacement.action.unwrap();
        let withdrawn=changed(&mutate(&lab.c,"lineage-withdrawal",QuestionMutation::WithdrawAnswer{question_id:q.id.clone(),expected_revision:replacement.question.question.revision,expected_answer_id:b.id.clone(),reason:"Effect removed".into()}).await).question.question;
        let comment=call(&lab.c,Operation::MutateDocument{operation_id:"lineage-comment".into(),mutation:DocumentMutation::PostComment{record:RecordRef::Question{question_id:q.id.clone()},expected_revision:withdrawn.revision,body:"History remains visible".into()}}).await;
        assert!(matches!(comment.outcome,Outcome::Ok{..}));
        let before=get(&lab.c,&q.id).await;assert_eq!(before.question.latest_answer_id.as_deref(),Some(b.id.as_str()));assert!(before.active_answer.is_none());
        // The legitimate chain including an unrelated comment reopens first.
        let lab=lab.restart().await;assert_eq!(get(&lab.c,&q.id).await,before);
        let Lab{root,c,config}=lab;drop(c);let path=root.join("tasks.sqlite");released(&path).await;
        let sql=rusqlite::Connection::open(&path).unwrap();
        match corrupt {
            "rolled_head"|"withdrawn_effect"=>{
                let mut altered=before.question.clone();
                if corrupt=="rolled_head"{altered.latest_answer_id=Some(a.id.clone());altered.active_answer_id=Some(a.id.clone());}
                else{altered.active_answer_id=Some(b.id.clone());}
                assert_eq!(sql.execute("UPDATE workflow_questions SET body=?2 WHERE id=?1",rusqlite::params![q.id,serde_json::to_string(&altered).unwrap()]).unwrap(),1);
            }
            "replacement_lineage"=>{
                let mut altered=b.clone();altered.replaces=None;
                assert_eq!(sql.execute("UPDATE workflow_answers SET body=?2 WHERE id=?1",rusqlite::params![b.id,serde_json::to_string(&altered).unwrap()]).unwrap(),1);
            }
            "replacement_action"=>{
                let mut altered=action.clone();
                if let QuestionAction::AnswerReplaced{answer_id,..}=&mut altered.action{*answer_id=a.id.clone();}else{panic!("replacement action expected")}
                assert_eq!(sql.execute("UPDATE workflow_question_actions SET body=?3 WHERE question_id=?1 AND question_revision=?2",rusqlite::params![q.id,i64::try_from(action.question_revision).unwrap(),serde_json::to_string(&altered).unwrap()]).unwrap(),1);
            }
            "missing_action"=>{assert_eq!(sql.execute("DELETE FROM workflow_question_actions WHERE question_id=?1 AND question_revision=?2",rusqlite::params![q.id,i64::try_from(action.question_revision).unwrap()]).unwrap(),1);}
            _=>unreachable!(),
        }
        drop(sql);
        assert!(Controller::open_with_config(&path,config).is_err(),"must refuse corrupted {corrupt}, never restore an old effect");
    }
}


#[tokio::test]
async fn browser_supersedes_custom_mcp_options_with_changed_recommendation_and_preserves_original_prompt(){
    let lab=Lab::new();let(t,caller)=actor(&lab.c,"custom-question-poser").await;
    let raw=PoseQuestion{record:RecordRef::Task{task_id:t.id.clone()},recipient:PrincipalRef::TrustedHostOperator,
        prompt:"Choose a direction. The phrase Recommended option: is a field label.".into(),
        options:vec![QuestionOption{id:"keep-current".into(),label:"Keep".into(),explanation:"Keep the current direction".into()},QuestionOption{id:"change-path".into(),label:"Change".into(),explanation:"Choose another direction".into()}],
        recommended_option_id:"keep-current".into()};
    let params=rpc_params("custom-mcp-question","collab_question_pose",json!({"question":raw}));
    let original=rpc_result(rpc(&lab.c,&caller,"tools/call",params.clone()).await);
    let q=changed(&original).question.question;let saved_prompt=q.posed.prompt.clone();let saved_posed=serde_json::to_vec(&q.posed).unwrap();
    let route=format!("/questions/{}/supersede",q.id);
    let editor=browser(&lab.c,"GET",&route,String::new(),"http://local.invalid").await;
    assert_eq!(editor.status,200,"{}",editor.body);
    let prompt=editor.body.split("<textarea name=\"prompt\" required>").nth(1).expect("replacement prompt field").split("</textarea>").next().unwrap().to_owned();
    assert_eq!(prompt,raw.prompt,"strip only the exact generated suffix, preserving the body");
    assert!(editor.body.contains("<option value=\"option-1\" selected>Option 1</option>"),"map original custom ID to the matching editable option");
    let hidden_value=|name:&str|->String {let prefix=format!("name=\"{name}\" value=\"");editor.body.split(&prefix).nth(1).expect("hidden form field").split('\"').next().unwrap().to_owned()};
    assert_eq!(hidden_value("record_id"),t.id);assert_eq!(hidden_value("expected_revision"),q.revision.to_string());
    let body=form(&[("operation_id",hidden_value("operation_id")),("expected_revision",hidden_value("expected_revision")),("option_count",hidden_value("option_count")),
        ("record_kind",hidden_value("record_kind")),("record_id",hidden_value("record_id")),("reason","Choose a revised direction".into()),
        ("recipient_kind","operator".into()),("recipient_agent_id",String::new()),("prompt",prompt.clone()),
        ("option_1_label","Keep current direction".into()),("option_1_explanation","Preserve it".into()),
        ("option_2_label","New direction".into()),("option_2_explanation","Use revised evidence".into()),("recommended_option_id","option-2".into())]);
    let response=browser(&lab.c,"POST",&route,body.clone(),"http://local.invalid").await;
    assert_eq!(response.status,303,"{}",response.body);
    let old=get(&lab.c,&q.id).await.question;
    assert_eq!(old.status,QuestionStatus::Superseded);assert_eq!(old.posed.prompt,saved_prompt);assert_eq!(serde_json::to_vec(&old.posed).unwrap(),saved_posed);assert_eq!(old.poser,q.poser);
    let replacement=get(&lab.c,old.superseded_by.as_deref().unwrap()).await.question;
    assert_eq!(replacement.supersedes.as_deref(),Some(q.id.as_str()));assert_eq!(replacement.posed.recommended_option_id,"option-2");
    assert_eq!(replacement.posed.options[1].label,"New direction");
    assert_eq!(replacement.posed.prompt,format!("{prompt}\n\nRecommended option: \"option-2\" — \"New direction\"."));
    assert!(matches!(&replacement.poser.provenance,ActorProvenance::TrustedHost{path:TrustedHostPath::Browser}));
    let replay=browser(&lab.c,"POST",&route,body,"http://local.invalid").await;assert_eq!(replay.status,303);
    assert_eq!(get(&lab.c,&replacement.id).await.question,replacement);
    let original_replay=rpc_result(rpc(&lab.c,&caller,"tools/call",params).await);
    assert_eq!(original_replay.outcome,original.outcome);assert_eq!(original_replay.commit,original.commit);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# Corrupt stored task state MUST be refused without rewriting its data.
async fn reopening_refuses_oversized_question_indexes_with_unchanged_canonical_body(){
    for column in ["recipient","status","record_kind","record_id"] {
        let lab=Lab::new();let t=submit(&lab.c,"index-bound-owner").await;
        let q=pose(&lab.c,&t,PrincipalRef::TrustedHostOperator).await;
        let saved=get(&lab.c,&q.id).await;
        let lab=lab.restart().await;assert_eq!(get(&lab.c,&q.id).await,saved);
        let maximum=lab.config.limits.workflow.as_ref().unwrap().record_bytes;
        let Lab{root,c,config}=lab;drop(c);let path=root.join("tasks.sqlite");released(&path).await;
        let sql=rusqlite::Connection::open(&path).unwrap();
        let body:String=sql.query_row("SELECT body FROM workflow_questions WHERE id=?1",[&q.id],|r|r.get(0)).unwrap();
        assert_eq!(body,serde_json::to_string(&saved.question).unwrap());
        // column is this test's closed four-name source list, never caller SQL.
        let original:String=sql.query_row(&format!("SELECT {column} FROM workflow_questions WHERE id=?1"),[&q.id],|r|r.get(0)).unwrap();
        let oversized=format!("{original}\0{}","x".repeat(maximum+1));
        assert_eq!(sql.execute(&format!("UPDATE workflow_questions SET {column}=?2 WHERE id=?1"),rusqlite::params![q.id,oversized]).unwrap(),1);
        let after:String=sql.query_row("SELECT body FROM workflow_questions WHERE id=?1",[&q.id],|r|r.get(0)).unwrap();assert_eq!(after,body);
        sql.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();drop(sql);
        let retained=||["","-wal","-shm"].into_iter().map(|suffix|{
            let mut name=path.as_os_str().to_owned();name.push(suffix);let file=PathBuf::from(name);
            let bytes=match std::fs::read(&file){Ok(value)=>Some(value),Err(error)if error.kind()==std::io::ErrorKind::NotFound=>None,Err(error)=>panic!("read retained {file:?}: {error}")};
            (file,bytes)
        }).collect::<Vec<_>>();
        let before=retained();
        assert!(Controller::open_with_config(&path,config).is_err(),"oversized {column} admitted");
        assert_eq!(retained(),before,"startup rewrote corrupt {column}");
    }
}

#[tokio::test]
async fn competing_question_resolutions_keep_one_original_outcome_across_restart() {
    // The adapter drops each completed canonical response before the consumer
    // receives it. GetOperation supplies the first usable outcome; no TCP loss
    // or worker/provider execution is claimed by this controlled boundary.
    async fn discard_response(c: &Controller, operation: Operation) {
        let _ = Box::pin(c.handle(Request {
            protocol_revision: WIRE_REVISION, request_id: unique(), command: operation,
        })).await;
    }
    async fn original(c: &Controller, operation: &Operation) -> (Reply, Outcome) {
        let Operation::MutateQuestion { operation_id, .. } = operation else { panic!() };
        let reply = call(c, Operation::GetOperation { operation_id: operation_id.clone() }).await;
        let Outcome::Ok { data: Data::Operation(record) } = &reply.outcome else { panic!("{reply:?}") };
        assert_eq!(&record.id, operation_id);
        assert_eq!(record.state, "settled");
        assert_eq!(record.intended, serde_json::to_value(operation).unwrap());
        assert_eq!(&reply.commit.as_ref().unwrap().operation_id, operation_id);
        let outcome = serde_json::from_value(record.outcome.clone().unwrap()).unwrap();
        (reply, outcome)
    }

    // Each pair starts in a state in which either competing operation is valid.
    // Answer/Cancel resolve an open question; Withdraw/Moot remove an active
    // answer's effect. A four-way race would mix incompatible preconditions.
    for answered in [false, true] {
        let lab = Lab::new();
        let t = submit(&lab.c, "resolution-race-owner").await;
        let posed = pose(&lab.c, &t, PrincipalRef::TrustedHostOperator).await;
        let mut before = get(&lab.c, &posed.id).await;
        let mut cause = None;
        if answered {
            before = changed(&mutate(&lab.c, "race-initial-answer", answer(&posed, "keep")).await).question;
            let other = pose(&lab.c, &t, PrincipalRef::TrustedHostOperator).await;
            let recorded = changed(&mutate(&lab.c, "race-causing-answer", answer(&other, "change")).await).new_answer.unwrap();
            cause = Some((other.id, recorded));
        }
        let pair = if let Some((cause_question, cause_answer)) = &cause {
            let current_answer = before.active_answer.as_ref().unwrap();
            [
                Operation::MutateQuestion { operation_id: "race-withdraw".into(), mutation: QuestionMutation::WithdrawAnswer {
                    question_id: posed.id.clone(), expected_revision: before.question.revision,
                    expected_answer_id: current_answer.id.clone(), reason: "Withdraw the original answer effect".into(),
                }},
                Operation::MutateQuestion { operation_id: "race-moot".into(), mutation: QuestionMutation::MarkMoot {
                    question_id: posed.id.clone(), expected_revision: before.question.revision,
                    expected_answer_id: current_answer.id.clone(),
                    caused_by: AnswerRef { question_id: cause_question.clone(), answer_id: cause_answer.id.clone() },
                    reason: "The recorded second answer removed the need".into(),
                }},
            ]
        } else {
            [
                Operation::MutateQuestion { operation_id: "race-answer".into(), mutation: answer(&posed, "keep") },
                Operation::MutateQuestion { operation_id: "race-cancel".into(), mutation: QuestionMutation::Cancel {
                    question_id: posed.id.clone(), expected_revision: posed.revision, reason: "Owner cancels the open decision".into(),
                }},
            ]
        };
        tokio::join!(discard_response(&lab.c, pair[0].clone()), discard_response(&lab.c, pair[1].clone()));
        let first = original(&lab.c, &pair[0]).await;
        let second = original(&lab.c, &pair[1]).await;
        let originals = [first, second];
        assert_eq!(originals.iter().filter(|(_, outcome)| matches!(outcome, Outcome::Ok { .. })).count(), 1);
        assert_eq!(originals.iter().filter(|(_, outcome)| matches!(outcome, Outcome::Refused { code, .. } if code == "stale_revision")).count(), 1);
        let winner = originals.iter().find_map(|(_, outcome)| match outcome {
            Outcome::Ok { data: Data::QuestionMutation(value) } => Some(value.clone()), _ => None,
        }).unwrap();
        let after = get(&lab.c, &posed.id).await;
        assert_eq!(after, winner.question);
        assert_eq!(after.question.revision, before.question.revision + 1);
        assert_eq!(after.question.posed, posed.posed);
        assert_eq!(after.question.poser, posed.poser);
        let QuestionQueryResult::Answers(answers) = query(&lab.c, QuestionQuery::Answers { question_id: posed.id.clone(), after: None, limit: 8 }).await else { panic!() };
        let QuestionQueryResult::Actions(actions) = query(&lab.c, QuestionQuery::Actions { question_id: posed.id.clone(), after: None, limit: 8 }).await else { panic!() };
        assert!(answers.next_cursor.is_none()); assert!(actions.next_cursor.is_none());
        if let Some((cause_question, cause_answer)) = &cause {
            assert!(after.active_answer.is_none());
            assert!(winner.new_answer.is_none());
            assert_eq!(answers.items, vec![before.active_answer.clone().unwrap()]);
            let action = winner.action.clone().unwrap();
            assert_eq!(actions.items, vec![action.clone()]);
            assert_eq!(action.actor.principal, PrincipalRef::TrustedHostOperator);
            match &action.action {
                QuestionAction::AnswerWithdrawn { answer_id, .. } => {
                    assert_eq!(answer_id, &answers.items[0].id);
                    assert_eq!(after.question.status, QuestionStatus::Answered);
                }
                QuestionAction::MarkedMoot { answer_id, caused_by, .. } => {
                    assert_eq!(answer_id, &answers.items[0].id);
                    assert_eq!(caused_by, &AnswerRef { question_id: cause_question.clone(), answer_id: cause_answer.id.clone() });
                    assert_eq!(after.question.status, QuestionStatus::Moot);
                }
                other => panic!("unexpected winning action: {other:?}"),
            }
            assert_eq!(get(&lab.c, cause_question).await.active_answer, Some(cause_answer.clone()));
        } else if let Some(answer) = &winner.new_answer {
            assert_eq!(after.question.status, QuestionStatus::Answered);
            assert_eq!(answers.items, vec![answer.clone()]);
            assert!(actions.items.is_empty());
        } else {
            assert_eq!(after.question.status, QuestionStatus::Cancelled);
            assert!(answers.items.is_empty());
            assert_eq!(actions.items, vec![winner.action.clone().unwrap()]);
        }
        assert!(queue(&lab.c, PrincipalRef::TrustedHostOperator, None, 8).await.items.is_empty());
        let lab = lab.restart().await;
        assert_eq!(get(&lab.c, &posed.id).await, after);
        assert_eq!(query(&lab.c, QuestionQuery::Answers { question_id: posed.id.clone(), after: None, limit: 8 }).await, QuestionQueryResult::Answers(answers));
        assert_eq!(query(&lab.c, QuestionQuery::Actions { question_id: posed.id.clone(), after: None, limit: 8 }).await, QuestionQueryResult::Actions(actions));
        for (operation, (saved, outcome)) in pair.iter().zip(&originals) {
            let replay = call(&lab.c, operation.clone()).await;
            assert_eq!(&replay.outcome, outcome);
            assert_eq!(replay.commit, saved.commit);
            let (lookup, original_outcome) = original(&lab.c, operation).await;
            assert_eq!(lookup.outcome, saved.outcome); assert_eq!(lookup.commit, saved.commit);
            assert_eq!(&original_outcome, outcome);
        }
        assert_eq!(get(&lab.c, &posed.id).await, after);
        assert!(queue(&lab.c, PrincipalRef::TrustedHostOperator, None, 8).await.items.is_empty());
    }
}

#[tokio::test]

// Traceability scope: Bound MCP rejects team/person/external recipients durably without product effects; a supported recipient remains valid.
//= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
//= type=test
//# A collaboration request MUST refuse unsupported recipient kinds.

async fn unsupported_recipient_kinds_refuse_through_bound_mcp_without_product_effects() {
    let lab = Lab::new();
    let (t, caller) = actor(&lab.c, "unsupported-recipient-agent").await;
    let posed = pose_for(RecordRef::Task { task_id: t.id.clone() }, PrincipalRef::TrustedHostOperator);
    let original_task = task(call(&lab.c, Operation::GetTask { task_id: t.id.clone() }).await);
    let baseline_queue = queue(&lab.c, PrincipalRef::TrustedHostOperator, None, 8).await;
    let list = QuestionQuery::List { record: Some(RecordRef::Task { task_id: t.id.clone() }), status: None, after: None, limit: 8 };
    let baseline_questions = query(&lab.c, list.clone()).await;
    let activity = Operation::QueryWorkflowActivity { cursor: None, limit: 8 };
    let baseline_activity = call(&lab.c, activity.clone()).await.outcome;
    let mut last_controller_revision = call(&lab.c, Operation::GetTask { task_id: t.id.clone() }).await.observed_controller_revision.unwrap();

    for kind in ["team", "person", "external_party"] {
        let id = format!("unsupported-recipient-{kind}");
        let mut arguments = json!({"question": posed});
        arguments["question"]["recipient"] = json!({"kind": kind});
        let invocation = Invocation {
            operation_id: id.clone(), logical_call_id: format!("logical-{id}"), caller: caller.clone(),
            tool: "collab_question_pose".into(), arguments: arguments.clone(),
        };
        let params = rpc_params(&id, "collab_question_pose", arguments);
        let rejected = rpc_result(rpc(&lab.c, &caller, "tools/call", params.clone()).await);
        assert!(matches!(&rejected.outcome, Outcome::Refused { code, message }
            if code == "invalid_input" && message == "question tool arguments differ from closed schema"), "{rejected:?}");
        let commit = rejected.commit.as_ref().expect("canonical structural refusal is durable");
        assert_eq!(commit.operation_id, id);
        assert!(commit.controller_revision > last_controller_revision);
        last_controller_revision = commit.controller_revision;

        // The nested schema fails inside the canonical action transaction. It
        // records the original refusal, but creates no question/product effect.
        let direct = call(&lab.c, Operation::GetOperation { operation_id: id.clone() }).await;
        let bound = rpc_result(rpc(&lab.c, &caller, "borg/calls/get", json!({"call_id": id})).await);
        assert_eq!(direct.commit, rejected.commit); assert_eq!(bound.commit, rejected.commit);
        assert_eq!(direct.outcome, bound.outcome);
        let Outcome::Ok { data: Data::Operation(record) } = &direct.outcome else { panic!("{direct:?}") };
        assert_eq!(record.id, id); assert_eq!(record.state, "settled");
        assert_eq!(record.intended, serde_json::to_value(invocation.operation()).unwrap());
        assert_eq!(serde_json::from_value::<Outcome>(record.outcome.clone().unwrap()).unwrap(), rejected.outcome);
        let replay = rpc_result(rpc(&lab.c, &caller, "tools/call", params).await);
        assert_eq!(replay.outcome, rejected.outcome); assert_eq!(replay.commit, rejected.commit);
        assert_eq!(task(call(&lab.c, Operation::GetTask { task_id: t.id.clone() }).await), original_task);
        assert_eq!(query(&lab.c, list.clone()).await, baseline_questions);
        assert_eq!(queue(&lab.c, PrincipalRef::TrustedHostOperator, None, 8).await, baseline_queue);
        assert_eq!(call(&lab.c, activity.clone()).await.outcome, baseline_activity);
    }
    // A distinct, supported recipient input goes through the same actual MCP
    // adapter and bound caller. Invalid recipients did not poison this route.
    let params = rpc_params("supported-recipient-control", "collab_question_pose", json!({"question": posed}));
    let committed = rpc_result(rpc(&lab.c, &caller, "tools/call", params.clone()).await);
    let question = changed(&committed).question.question;
    assert_eq!(question.posed.recipient, PrincipalRef::TrustedHostOperator);
    assert_eq!(question.poser.principal, PrincipalRef::Agent { agent_id: t.assigned_agent_id.clone() });
    assert!(matches!(&question.poser.provenance, ActorProvenance::BoundAgentSession { task_id, session_id, .. }
        if task_id == &caller.task_id && session_id == &t.session_id));
    assert_eq!(question.created_product_revision, baseline_queue.observed_product_revision + 1);
    let replay = rpc_result(rpc(&lab.c, &caller, "tools/call", params).await);
    assert_eq!(replay.outcome, committed.outcome); assert_eq!(replay.commit, committed.commit);
    let lookup = rpc_result(rpc(&lab.c, &caller, "borg/calls/get", json!({"call_id":"supported-recipient-control"})).await);
    assert_eq!(lookup.commit, committed.commit);
    let Outcome::Ok { data: Data::Operation(record) } = lookup.outcome else { panic!() };
    assert_eq!(record.id, "supported-recipient-control");
    assert_eq!(serde_json::from_value::<Outcome>(record.outcome.unwrap()).unwrap(), committed.outcome);
    let QuestionQueryResult::Questions(page) = query(&lab.c, list).await else { panic!() };
    assert_eq!(page.items, vec![get(&lab.c, &question.id).await]); assert!(page.next_cursor.is_none());
}


#[tokio::test]
async fn operator_option_authoring_guidance_reaches_browser_and_bound_agent_catalogue() {
    // This checks guidance delivery, not arbitrary natural-language compliance.
    // The retained controlled worker/browser journey separately supplies a real
    // outcome-choice example; no text classifier or automatic refusal is added.
    let guidance = "For an operator recipient, offer choices about what should happen. Do not describe steps for the operator to perform by hand.";
    let lab = Lab::new();
    let (t, caller) = actor(&lab.c, "operator-option-author").await;
    let listed = rpc(&lab.c, &caller, "tools/list", json!({})).await;
    let tools = listed["result"]["tools"].as_array().expect("bound V2 catalogue");
    for name in ["collab_question_pose", "collab_question_supersede"] {
        let tool = tools.iter().find(|tool| tool["name"].as_str() == Some(name)).unwrap();
        assert!(tool["description"].as_str().unwrap().ends_with(guidance), "{name}");
    }
    let question = pose_for(RecordRef::Task { task_id: t.id }, PrincipalRef::TrustedHostOperator);
    let params = rpc_params("operator-option-editor-source", "collab_question_pose", json!({"question": question}));
    let original = rpc_result(rpc(&lab.c, &caller, "tools/call", params.clone()).await);
    let posed = changed(&original).question.question;
    for route in ["/questions/new".to_owned(), format!("/questions/{}/supersede", posed.id)] {
        let response = browser(&lab.c, "GET", &route, String::new(), "http://local.invalid").await;
        assert_eq!(response.status, 200, "{}", response.body);
        assert!(response.body.contains(guidance), "{route}");
    }
    assert_eq!(get(&lab.c, &posed.id).await.question, posed);
    let replay = rpc_result(rpc(&lab.c, &caller, "tools/call", params).await);
    assert_eq!(replay.outcome, original.outcome);
    assert_eq!(replay.commit, original.commit);
}
