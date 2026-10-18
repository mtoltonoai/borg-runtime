//! Actual controller transactions and browser/MCP adapters; controlled agents.
//! No provider, worker process, host or installed-release qualification.
use borg_runtime::{controller::{Controller,ControllerConfig},protocol::*,collaboration::{questions::*,notifications::*,workflow::*},limits::{RuntimeLimits,WorkflowLimits},mcp::{Caller,Invocation},product::{self,UiRequest}};
use serde_json::{json,Value};
use borg_runtime::collaboration::work::*;
use std::{path::PathBuf,sync::{Arc,atomic::{AtomicU64,Ordering}},time::{SystemTime,UNIX_EPOCH}};
static NEXT:AtomicU64=AtomicU64::new(1);
fn unique()->String{format!("delivery-{}-{}-{}",std::process::id(),SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),NEXT.fetch_add(1,Ordering::Relaxed))}
fn limits()->RuntimeLimits {
    let mut p=RuntimeLimits::seed().unwrap();p.execution.native.context_bytes=65_536;p.controller.command_wait_ms=5;
    p.workflow=Some(WorkflowLimits{record_bytes:32_768,text_bytes:4_096,options:8,records:512,revisions:4_096,page_items:8,page_bytes:32_768,waits_per_task:8,graph_scan_nodes:512,events_per_mutation:64,event_bytes:8_192,notice_bytes:8_192,pending_delivery_bytes:32_768,delivery_scan_rows:64});p.validate().unwrap();p
}
fn config(p:RuntimeLimits)->ControllerConfig{ControllerConfig{limits:Arc::new(p),validation_executable:PathBuf::from(env!("CARGO_BIN_EXE_borg-runtime")),..ControllerConfig::default()}}
fn input(agent:&str)->TaskInput {
    TaskInput{ instruction_source: None, pull_request: None, instruction:"Return a bounded structured value".into(),definition:Definition{charter: None, id:agent.into(),revision:"v1".into(),executor:ExecutorKind::Native,inference:Some(InferenceSettings::selected()),execution_profile:Some(ExecutionProfile::CollaborationV2{web_search:None}),limit_overrides:None},repository:None,repository_read:None,acceptance_commands:vec![],output_schema:Some(json!({"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false})),limits:Limits{deadline_unix_ms:SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()as u64+600_000,model_requests:32,output_bytes:16_384,result_bytes:RuntimeLimits::seed().unwrap().task_defaults.result_bytes,max_delegation_depth:4},dependencies:vec![],native_workflow:None,resolved_limits:None}
}
struct Lab{root:PathBuf,c:Controller,config:ControllerConfig}
impl Lab{fn new()->Self{Self::with(limits())}fn with(p:RuntimeLimits)->Self{let root=std::env::temp_dir().join(unique());std::fs::create_dir(&root).unwrap();let config=config(p);let c=Controller::open_with_config(&root.join("tasks.sqlite"),config.clone()).unwrap();Self{root,c,config}}async fn restart(self)->Self{let Self{root,c,config}=self;drop(c);released(&root.join("tasks.sqlite")).await;let c=Controller::open_with_config(&root.join("tasks.sqlite"),config.clone()).unwrap();Self{root,c,config}}}
async fn released(path:&std::path::Path){let mut name=path.as_os_str().to_owned();name.push(".controller-lock");tokio::time::timeout(std::time::Duration::from_secs(3),async{loop{let file=std::fs::OpenOptions::new().read(true).write(true).open(PathBuf::from(&name)).unwrap();if file.try_lock().is_ok(){break}tokio::time::sleep(std::time::Duration::from_millis(5)).await;}}).await.expect("previous controller owner released");}
async fn call(c:&Controller,command:Operation)->Reply{let reply=c.handle(Request{protocol_revision:WIRE_REVISION,request_id:unique(),command:command.clone()}).await;reply.validate_for(&command).unwrap();reply}
fn task(reply:Reply)->Task{match reply.outcome{Outcome::Ok{data:Data::Task(t)}=>*t,x=>panic!("expected task: {x:?}")}}
fn changed(reply:&Reply)->QuestionMutationOutcome{assert!(reply.commit.is_some(),"{reply:?}");match &reply.outcome{Outcome::Ok{data:Data::QuestionMutation(v)}=>v.clone(),x=>panic!("expected committed question: {x:?}")}}
fn refused(reply:&Reply,code:&str){assert!(matches!(&reply.outcome,Outcome::Refused{code:c,..}if c==code),"{reply:?}");}
async fn mutate(c:&Controller,op:&str,mutation:QuestionMutation)->Reply{call(c,Operation::MutateQuestion{operation_id:op.into(),mutation}).await}
fn pose_for(record:RecordRef,recipient:PrincipalRef)->PoseQuestion{PoseQuestion{record,recipient,prompt:"Choose one option. Recommended: keep.".into(),options:vec![QuestionOption{id:"keep".into(),label:"Keep".into(),explanation:"Preserve the current choice".into()},QuestionOption{id:"change".into(),label:"Change".into(),explanation:"Use the stated alternative".into()}],recommended_option_id:"keep".into()}}
fn answer(q:&Question,option:&str)->QuestionMutation{QuestionMutation::Answer{question_id:q.id.clone(),expected_revision:q.revision,answer:QuestionAnswer{option_id:option.into(),explanation:"Explicit decision".into()}}}
async fn actor(c:&Controller,agent:&str)->(Task,Caller){actor_profile(c,agent,ExecutionProfile::CollaborationV2{web_search:None}).await}
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


fn delivery(reply:&Reply)->DeliveryObservation {match &reply.outcome{Outcome::Ok{data:Data::CollaborationDelivery(v)}=>v.clone(),x=>panic!("delivery: {x:?}")}}
async fn observe(c:&Controller,caller:&Caller)->DeliveryObservation{delivery(&call(c,Operation::ObserveCollaborationDelivery{caller:caller.clone(),limit:8}).await)}
async fn event(c:&Controller,t:&Task,caller:&Caller)->Question {
    let q=changed(&tool(c,caller,&unique(),"collab_question_pose",json!({"question":pose_for(RecordRef::Task{task_id:t.id.clone()},PrincipalRef::Agent{agent_id:t.assigned_agent_id.clone()})})).await).question.question;
    changed(&tool(c,caller,&unique(),"collab_question_answer",json!({"question_id":q.id,"expected_revision":q.revision,"answer":{"option_id":"keep","explanation":"Explicit decision"}})).await);q
}
async fn prepare(c:&Controller,caller:&Caller,view:&DeliveryObservation)->(Operation,Reply,DeliveryBatch){
    let page=view.available.as_ref().expect("committed event available");
    let request=Operation::PrepareCollaborationDelivery{expected_routing_cursor:view.routing_cursor.clone(),operation_id:unique(),caller:caller.clone(),expected_cursor:view.acknowledged_cursor.clone(),through:page.through.clone(),limit:8};
    let result=call(c,request.clone()).await;let batch=delivery(&result).pending.unwrap();assert_eq!(Some(batch.batch_id.as_str()),request.mutation_id());(request,result,batch)
}
fn begin(caller:&Caller,b:&DeliveryBatch,context:&str)->Operation{Operation::BeginCollaborationDelivery{operation_id:unique(),caller:caller.clone(),batch_id:b.batch_id.clone(),context_binding:context.into()}}
fn ack(caller:&Caller,b:&DeliveryBatch,context:&str)->Operation{Operation::AcknowledgeCollaborationDelivery{operation_id:unique(),caller:caller.clone(),batch_id:b.batch_id.clone(),context_binding:context.into()}}
async fn activity(c:&Controller,cursor:Option<String>,limit:usize)->WorkflowActivityPage{match call(c,Operation::QueryWorkflowActivity{cursor,limit}).await.outcome{Outcome::Ok{data:Data::WorkflowActivity(v)}=>v,x=>panic!("activity: {x:?}")}}

//= spec/impl/v2/implementation.md#committed-resolutions-reach-their-consumers
//# Pending resolution delivery MUST remain recoverable after a controller restart.
//= spec/impl/v2/implementation.md#committed-resolutions-reach-their-consumers
//# A committed resolution delivery MUST carry an identity that deduplicates its delivery retries.
#[tokio::test]
async fn workflow_prepare_restart_begin_and_lost_ack_preserve_one_original_batch(){
    let lab=Lab::new();let(t,caller)=actor(&lab.c,"delivery-agent").await;let q=event(&lab.c,&t,&caller).await;
    let view=observe(&lab.c,&caller).await;assert!(view.pending.is_none());assert!(view.available.as_ref().unwrap().items.iter().any(|e|e.subject==RecordRef::Question{question_id:q.id.clone()}));
    let (prepare_request,prepared,b)=prepare(&lab.c,&caller,&view).await;assert!(b.delivery.is_none());assert!(!b.page.items.is_empty());
    let before_task=task(call(&lab.c,Operation::GetTask{task_id:t.id.clone()}).await);
    let lab=lab.restart().await;assert_eq!(observe(&lab.c,&caller).await.pending,Some(b.clone()));
    let replay=call(&lab.c,prepare_request).await;assert_eq!(replay.outcome,prepared.outcome);assert_eq!(replay.commit,prepared.commit);
    let intent=begin(&caller,&b,"native:original-attempt");let intended=call(&lab.c,intent.clone()).await;let exact=delivery(&intended).pending.unwrap();assert_eq!(exact.delivery.as_ref().unwrap().operation_id,intent.mutation_id().unwrap());
    // This is the host retention acknowledgement after an actual consumer would
    // have retained input; the consumer JOIN cases prove that separate boundary.
    let acknowledge=ack(&caller,&b,"native:original-attempt");let committed=call(&lab.c,acknowledge.clone()).await;let settled=delivery(&committed);assert!(settled.pending.is_none());assert_eq!(settled.acknowledged_cursor,b.page.to_cursor);
    let lab=lab.restart().await;let lookup=call(&lab.c,Operation::GetOperation{operation_id:acknowledge.mutation_id().unwrap().into()}).await;let Outcome::Ok{data:Data::Operation(record)}=lookup.outcome else{panic!("{lookup:?}")};assert_eq!(serde_json::from_value::<Outcome>(record.outcome.unwrap()).unwrap(),committed.outcome);assert_eq!(lookup.commit,committed.commit);
    assert_eq!(observe(&lab.c,&caller).await.acknowledged_cursor,b.page.to_cursor);assert_eq!(task(call(&lab.c,Operation::GetTask{task_id:t.id}).await),before_task);
}

#[tokio::test]
async fn workflow_append_intent_refuses_changed_context_foreign_caller_and_unproved_ack(){
    let lab=Lab::new();let(t,caller)=actor(&lab.c,"intent-owner").await;let(_,foreign)=actor(&lab.c,"foreign").await;event(&lab.c,&t,&caller).await;
    let (_,_,b)=prepare(&lab.c,&caller,&observe(&lab.c,&caller).await).await;
    refused(&call(&lab.c,ack(&caller,&b,"context-one")).await,"delivery_pending");assert_eq!(observe(&lab.c,&caller).await.pending,Some(b.clone()));
    let original=begin(&caller,&b,"context-one");let committed=call(&lab.c,original.clone()).await;let expected=delivery(&committed).pending.unwrap();
    refused(&call(&lab.c,begin(&caller,&b,"context-two")).await,"delivery_pending");
    refused(&call(&lab.c,ack(&caller,&b,"context-two")).await,"delivery_pending");
    assert!(matches!(call(&lab.c,ack(&foreign,&b,"context-one")).await.outcome,Outcome::Refused{..}));
    let lab=lab.restart().await;assert_eq!(observe(&lab.c,&caller).await.pending,Some(expected));let repeated=call(&lab.c,original).await;assert_eq!(repeated.commit,committed.commit);assert_eq!(repeated.outcome,committed.outcome);
}

#[tokio::test]
async fn workflow_activity_and_session_delivery_use_same_committed_ids_without_cross_session_inputs(){
    let lab=Lab::new();let(a,ca)=actor(&lab.c,"a").await;let(b,cb)=actor(&lab.c,"b").await;let qa=event(&lab.c,&a,&ca).await;let qb=event(&lab.c,&b,&cb).await;
    let oa=observe(&lab.c,&ca).await;let ob=observe(&lab.c,&cb).await;
    let ia=&oa.available.as_ref().unwrap().items;let ib=&ob.available.as_ref().unwrap().items;
    assert!(ia.iter().any(|e|e.subject==RecordRef::Question{question_id:qa.id.clone()}));assert!(!ia.iter().any(|e|e.subject==RecordRef::Question{question_id:qb.id.clone()}));
    assert!(ib.iter().any(|e|e.subject==RecordRef::Question{question_id:qb.id.clone()}));assert!(!ib.iter().any(|e|e.subject==RecordRef::Question{question_id:qa.id.clone()}));
    let mut all=vec![];let mut cursor=None;
    for _ in 0..32 {let page=activity(&lab.c,cursor.take(),1).await;all.extend(page.items);cursor=page.next_cursor;if cursor.is_none(){break;}}
    assert!(cursor.is_none());let ids=all.iter().map(|e|e.id.clone()).collect::<std::collections::BTreeSet<_>>();assert_eq!(ids.len(),all.len());
    for e in ia.iter().chain(ib.iter()){assert_eq!(all.iter().find(|x|x.id==e.id),Some(e));}
    let mut wrong=oa.acknowledged_cursor.clone();wrong.scope.session_id=b.session_id;
    refused(&call(&lab.c,Operation::PrepareCollaborationDelivery{expected_routing_cursor:None,operation_id:unique(),caller:ca,expected_cursor:wrong,through:oa.available.unwrap().through,limit:8}).await,"stale_revision");
}

#[tokio::test]
async fn workflow_v1_and_retired_callers_cannot_begin_append_or_advance_input_cursor(){
    let lab=Lab::new();let(t,v1)=actor_profile(&lab.c,"old-profile",ExecutionProfile::CollaborationV1{web_search:None}).await;
    refused(&call(&lab.c,Operation::ObserveCollaborationDelivery{caller:v1,limit:8}).await,"not_configured");assert_eq!(task(call(&lab.c,Operation::GetTask{task_id:t.id}).await).input,t.input);
    let(t,caller)=actor(&lab.c,"cancelled-delivery").await;event(&lab.c,&t,&caller).await;let(_,_,b)=prepare(&lab.c,&caller,&observe(&lab.c,&caller).await).await;
    let current=task(call(&lab.c,Operation::GetTask{task_id:t.id.clone()}).await);task(call(&lab.c,Operation::CancelTask{operation_id:unique(),task_id:t.id.clone(),expected_revision:current.revision}).await);
    for request in [begin(&caller,&b,"retired"),ack(&caller,&b,"retired")] {assert!(matches!(call(&lab.c,request).await.outcome,Outcome::Refused{..}));}
    let Selfless{root,config}=close(lab).await;let db=rusqlite::Connection::open(root.join("tasks.sqlite")).unwrap();let raw:String=db.query_row("SELECT workflow_delivery FROM sessions WHERE id=?1",[&t.session_id],|r|r.get(0)).unwrap();let state:Value=serde_json::from_str(&raw).unwrap();assert_eq!(state["pending"],serde_json::to_value(b).unwrap());drop(db);drop(config);
}
struct Selfless{root:PathBuf,config:ControllerConfig}
async fn close(lab:Lab)->Selfless{let Lab{root,c,config}=lab;drop(c);released(&root.join("tasks.sqlite")).await;Selfless{root,config}}

#[cfg(debug_assertions)]
#[tokio::test]
async fn workflow_commit_failure_preserves_original_cursor_pending_and_receipt_absence(){
    let mut lab=Lab::new();let(t,caller)=actor(&lab.c,"rollback").await;event(&lab.c,&t,&caller).await;
    let fail=Arc::new(std::sync::atomic::AtomicBool::new(false));lab.config.qualification_fail_next_commit=Some(fail.clone());let lab=lab.restart().await;let view=observe(&lab.c,&caller).await;
    let op=Operation::PrepareCollaborationDelivery{expected_routing_cursor:None,operation_id:unique(),caller:caller.clone(),expected_cursor:view.acknowledged_cursor.clone(),through:view.available.as_ref().unwrap().through.clone(),limit:8};
    fail.store(true,Ordering::SeqCst);let result=call(&lab.c,op.clone()).await;refused(&result,"store_unavailable");assert!(!fail.load(Ordering::SeqCst));assert!(result.commit.is_none());assert_eq!(observe(&lab.c,&caller).await,view);
    assert!(matches!(call(&lab.c,Operation::GetOperation{operation_id:op.mutation_id().unwrap().into()}).await.outcome,Outcome::Refused{..}));
}

async fn observe_attempt(c:&Controller,caller:&Caller,observation:Observation)->Task {
    task(call(c,Operation::ObserveWorker{report:Report{report_id:unique(),worker_id:caller.worker_id.clone(),incarnation:caller.incarnation.clone(),task_id:caller.task_id.clone(),attempt_id:caller.attempt_id.clone(),generation:caller.generation,observation}}).await)
}
async fn successor(c:&Controller,old:&Caller)->Caller {
    let t=task(call(c,Operation::GetTask{task_id:old.task_id.clone()}).await);
    task(call(c,Operation::CancelTask{operation_id:unique(),task_id:t.id.clone(),expected_revision:t.revision}).await);
    let stopped=observe_attempt(c,old,Observation::ProcessTreeStopped).await;
    assert!(stopped.attempt.as_ref().unwrap().stopped && !stopped.attempt.as_ref().unwrap().uncertain);
    let w=Worker{id:old.worker_id.clone(),incarnation:unique(),coding_capacity:0,native_capacity:1};
    assert!(matches!(call(c,Operation::RegisterWorker{operation_id:unique(),worker:w.clone()}).await.outcome,Outcome::Ok{..}));
    task(call(c,Operation::RetryTask{operation_id:unique(),task_id:t.id.clone(),expected_revision:stopped.revision}).await);
    let r=call(c,Operation::AwaitCommands{operation_id:unique(),worker_id:w.id.clone(),incarnation:w.incarnation.clone(),acknowledged:vec![]}).await;
    let Outcome::Ok{data:Data::Commands(commands)}=r.outcome else{panic!("{r:?}")};
    let a=commands.into_iter().find_map(|c|match c{WorkerCommand::RunAttempt{attempt,..}if attempt.task_id==t.id=>Some(attempt),_=>None}).expect("ordinary replacement attempt");
    let caller=Caller{task_id:a.task_id,attempt_id:a.id,generation:a.generation,worker_id:a.worker_id,incarnation:a.worker_incarnation};
    observe_attempt(c,&caller,Observation::Started{thread_id:None,turn_id:None}).await;caller
}

#[tokio::test]
async fn workflow_preintent_supersession_and_begin_are_serialized_in_both_commit_orders(){
    for begin_wins in [true,false] {
        let lab=Lab::new();let(t,old)=actor(&lab.c,"preintent-race").await;event(&lab.c,&t,&old).await;
        let (old_request,old_receipt,old_batch)=prepare(&lab.c,&old,&observe(&lab.c,&old).await).await;
        let next=successor(&lab.c,&old).await;let offered=observe(&lab.c,&next).await;
        assert_eq!(offered.acknowledged_cursor,old_batch.page.from_cursor);
        assert_eq!(offered.pending.as_ref().unwrap(),&old_batch);assert!(offered.available.is_some());
        let replacement=Operation::PrepareCollaborationDelivery{expected_routing_cursor:None,operation_id:unique(),caller:next.clone(),expected_cursor:offered.acknowledged_cursor.clone(),through:offered.available.as_ref().unwrap().through.clone(),limit:8};
        let original_intent=begin(&next,&old_batch,"new-current-context");
        if begin_wins {
            let committed=call(&lab.c,original_intent.clone()).await;let locked=delivery(&committed).pending.unwrap();
            refused(&call(&lab.c,replacement).await,"delivery_pending");
            let lookup=call(&lab.c,Operation::GetOperation{operation_id:original_intent.mutation_id().unwrap().into()}).await;assert_eq!(lookup.commit,committed.commit);
            assert_eq!(observe(&lab.c,&next).await.pending,Some(locked));
        } else {
            let committed=call(&lab.c,replacement.clone()).await;let new_batch=delivery(&committed).pending.unwrap();
            assert_ne!(new_batch.batch_id,old_batch.batch_id);assert_eq!(new_batch.page.from_cursor,old_batch.page.from_cursor);assert!(new_batch.delivery.is_none());
            refused(&call(&lab.c,original_intent).await,"batch_mismatch");
            let replay=call(&lab.c,replacement).await;assert_eq!(replay.commit,committed.commit);assert_eq!(replay.outcome,committed.outcome);
        }
        let retained=call(&lab.c,old_request).await;assert_eq!(retained.commit,old_receipt.commit);assert_eq!(retained.outcome,old_receipt.outcome);
        assert_eq!(observe(&lab.c,&next).await.acknowledged_cursor,old_batch.page.from_cursor);
    }
}

#[tokio::test]
async fn workflow_delivery_uses_page_bytes_below_pending_bytes_without_losing_the_second_event(){
    let mut selected=limits();selected.workflow.as_mut().unwrap().page_bytes=4_096;
    selected.validate().unwrap();
    let lab=Lab::with(selected);let(t,caller)=actor(&lab.c,"bounded-page").await;
    let mut questions=vec![];
    for _ in 0..2 {
        let mut question=pose_for(RecordRef::Task{task_id:t.id.clone()},PrincipalRef::Agent{agent_id:t.assigned_agent_id.clone()});
        question.prompt="p".repeat(2_048);
        questions.push(changed(&mutate(&lab.c,&unique(),QuestionMutation::Pose{question}).await).question.question);
    }
    let selected=lab.config.limits.workflow.as_ref().unwrap();
    let first=observe(&lab.c,&caller).await;let offered=first.available.as_ref().unwrap();
    assert!(selected.page_bytes<selected.pending_delivery_bytes);
    assert_eq!(offered.items.len(),1);assert!(offered.has_more);
    assert_eq!(offered.items[0].subject,RecordRef::Question{question_id:questions[0].id.clone()});
    assert!(serde_json::to_vec(offered).unwrap().len()<=selected.page_bytes);
    let first_activity=activity(&lab.c,None,1).await;
    let second_activity=activity(&lab.c,first_activity.next_cursor,1).await;
    assert_eq!(second_activity.items.len(),1);
    let mut too_large=offered.clone();too_large.items.push(second_activity.items[0].clone());
    too_large.to_cursor.position=Some(too_large.through.clone());too_large.has_more=false;
    let size=serde_json::to_vec(&too_large).unwrap().len();
    assert!(size>selected.page_bytes && size<selected.pending_delivery_bytes,"the old pending-only bound admits both events");
    let (_,_,first_batch)=prepare(&lab.c,&caller,&first).await;
    assert_eq!(&first_batch.page,offered);
    let lab=lab.restart().await;
    assert_eq!(observe(&lab.c,&caller).await.pending,Some(first_batch.clone()));
    delivery(&call(&lab.c,begin(&caller,&first_batch,"bounded-page-first")).await);
    let acknowledged=delivery(&call(&lab.c,ack(&caller,&first_batch,"bounded-page-first")).await);
    assert_eq!(acknowledged.acknowledged_cursor,first_batch.page.to_cursor);
    let second=observe(&lab.c,&caller).await;let offered=second.available.as_ref().unwrap();
    assert_eq!(offered.items,second_activity.items);assert!(!offered.has_more);
    assert!(serde_json::to_vec(offered).unwrap().len()<=lab.config.limits.workflow.as_ref().unwrap().page_bytes);
    let (_,_,second_batch)=prepare(&lab.c,&caller,&second).await;
    delivery(&call(&lab.c,begin(&caller,&second_batch,"bounded-page-second")).await);
    let acknowledged=delivery(&call(&lab.c,ack(&caller,&second_batch,"bounded-page-second")).await);
    assert_eq!(acknowledged.acknowledged_cursor.position,Some(second_batch.page.through));
    assert!(observe(&lab.c,&caller).await.available.is_none());
}

#[tokio::test]
async fn workflow_initial_cursor_continuation_survives_reopen_and_exact_ack_without_skipping_first_event(){
    let mut selected=limits();selected.workflow.as_mut().unwrap().page_bytes=6_144;
    // Leave explicit room for the canonical recommendation suffix; page-fit
    // inequalities below still measure the complete accepted event and refs.
    selected.workflow.as_mut().unwrap().text_bytes=8_192;
    selected.validate().unwrap();let lab=Lab::with(selected);
    let (t,original_caller)=actor(&lab.c,"initial-continuation").await;
    // This first event fits alone but not beside all four required resolutions.
    // It must remain unscanned while the full admitted continuation is delivered.
    // Bind the large concrete event to this existing session before AddWait
    // stops its original attempt. Its cursor is still unacknowledged (None).
    let mut question=pose_for(RecordRef::Task{task_id:t.id.clone()},PrincipalRef::Agent{agent_id:t.assigned_agent_id.clone()});
    question.prompt="p".repeat(4_096);
    let first_question=changed(&mutate(&lab.c,&unique(),QuestionMutation::Pose{question}).await).question.question;
    let mut revision=0;let mut questions=vec![];
    for _ in 0..4 {
        let question=pose_for(RecordRef::Task{task_id:t.id.clone()},PrincipalRef::TrustedHostOperator);
        let q=changed(&mutate(&lab.c,&unique(),QuestionMutation::Pose{question}).await).question.question;
        let added=call(&lab.c,Operation::MutateWork{operation_id:unique(),mutation:WorkMutation::AddWait{
            task_id:t.id.clone(),expected_revision:revision,resolver:ResolverRef::Question{question_id:q.id.clone()},
        }}).await;
        let Outcome::Ok{data:Data::WorkMutation(added)}=added.outcome else{panic!("{added:?}")};
        revision=added.view.coordination_revision;questions.push(q);
    }
    let stopped=observe_attempt(&lab.c,&original_caller,Observation::ProcessTreeStopped).await;
    assert!(stopped.attempt.as_ref().unwrap().stopped && !stopped.attempt.as_ref().unwrap().uncertain);
    for q in &questions {changed(&mutate(&lab.c,&unique(),answer(q,"keep")).await);}
    let worker=Worker{id:unique(),incarnation:"inc".into(),coding_capacity:0,native_capacity:1};
    assert!(matches!(call(&lab.c,Operation::RegisterWorker{operation_id:unique(),worker:worker.clone()}).await.outcome,Outcome::Ok{..}));
    let commands=call(&lab.c,Operation::AwaitCommands{operation_id:unique(),worker_id:worker.id,incarnation:worker.incarnation,acknowledged:vec![]}).await;
    let Outcome::Ok{data:Data::Commands(commands)}=commands.outcome else{panic!("{commands:?}")};
    let attempt=commands.into_iter().find_map(|command|match command{
        WorkerCommand::RunAttempt{attempt,..}if attempt.task_id==t.id=>Some(attempt),_=>None,
    }).expect("ordinary admission retains the resolved task");
    let caller=Caller{task_id:attempt.task_id,attempt_id:attempt.id,generation:attempt.generation,worker_id:attempt.worker_id,incarnation:attempt.worker_incarnation};
    observe_attempt(&lab.c,&caller,Observation::Started{thread_id:None,turn_id:None}).await;
    let view=observe(&lab.c,&caller).await;let offered=view.available.as_ref().unwrap();
    let continuation=offered.continuation.clone().expect("exact admitted wait association is retained by delivery");
    assert_eq!(continuation.resolved_waits.len(),4);
    assert_eq!(continuation.task_id,t.id);assert_eq!(continuation.session_id,t.session_id);
    for question in &questions {
        assert_eq!(continuation.resolved_waits.iter().filter(|resolved|resolved.resolver==ResolverRef::Question{question_id:question.id.clone()}).count(),1);
    }
    assert!(view.acknowledged_cursor.position.is_none());
    assert_eq!(offered.from_cursor,offered.to_cursor);assert!(offered.items.is_empty());assert!(offered.has_more);
    assert_eq!(offered.continuation.as_ref(),Some(&continuation));
    let first=activity(&lab.c,None,1).await.items.remove(0);
    assert_eq!(first.subject,RecordRef::Question{question_id:first_question.id});
    let mut joined=offered.clone();joined.items.push(first.clone());joined.to_cursor.position=Some(first.id.clone());
    let bound=lab.config.limits.workflow.as_ref().unwrap().page_bytes;
    assert!(serde_json::to_vec(offered).unwrap().len()<=bound);
    assert!(serde_json::to_vec(&joined).unwrap().len()>bound);
    joined.continuation=None;assert!(serde_json::to_vec(&joined).unwrap().len()<=bound);
    let (prepare_request,prepared,batch)=prepare(&lab.c,&caller,&view).await;
    assert_eq!(batch.page,*offered);
    let lab=lab.restart().await;
    assert_eq!(observe(&lab.c,&caller).await.pending,Some(batch.clone()));
    let replay=call(&lab.c,prepare_request).await;assert_eq!(replay.outcome,prepared.outcome);assert_eq!(replay.commit,prepared.commit);
    refused(&call(&lab.c,ack(&caller,&batch,"initial-continuation-context")).await,"delivery_pending");
    let begun=delivery(&call(&lab.c,begin(&caller,&batch,"initial-continuation-context")).await);
    let lab=lab.restart().await;assert_eq!(observe(&lab.c,&caller).await,begun);
    let acknowledge=ack(&caller,&batch,"initial-continuation-context");let committed=call(&lab.c,acknowledge.clone()).await;
    let settled=delivery(&committed);assert!(settled.pending.is_none());assert_eq!(settled.acknowledged_cursor,view.acknowledged_cursor);
    let lab=lab.restart().await;
    let replay=call(&lab.c,acknowledge.clone()).await;assert_eq!(replay.outcome,committed.outcome);assert_eq!(replay.commit,committed.commit);
    let lookup=call(&lab.c,Operation::GetOperation{operation_id:acknowledge.mutation_id().unwrap().into()}).await;
    let Outcome::Ok{data:Data::Operation(record)}=lookup.outcome else{panic!("{lookup:?}")};
    assert_eq!(serde_json::from_value::<Outcome>(record.outcome.unwrap()).unwrap(),committed.outcome);
    let next=observe(&lab.c,&caller).await;assert_eq!(next.acknowledged_cursor,view.acknowledged_cursor);
    let next=next.available.unwrap();assert!(next.continuation.is_none());assert_eq!(next.items.first(),Some(&first));
    assert!(next.items.iter().all(|event|!continuation.resolved_waits.iter().any(|wait|wait.event_id==event.id)));
}
