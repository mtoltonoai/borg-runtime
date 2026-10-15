//! Clean-state canonical agent/session qualification through real controller SQL.
//! Worker reports are controlled inputs; no provider, Codex or host qualification.
use borg_runtime::{controller::{Controller,ControllerConfig},protocol::*,collaboration::{Query,QueryResult},mcp::{Caller,Invocation}};
use serde_json::json;
use std::{path::{Path,PathBuf},sync::{Arc,atomic::{AtomicU64,AtomicBool,Ordering}},time::Duration};
static NEXT:AtomicU64=AtomicU64::new(1);
fn dir()->PathBuf{let p=std::env::temp_dir().join(format!("borg-agent-sessions-{}-{}-{}",std::process::id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),NEXT.fetch_add(1,Ordering::SeqCst)));std::fs::create_dir(&p).unwrap();p.join("task.db")}
fn cfg()->ControllerConfig{let mut policy=borg_runtime::limits::RuntimeLimits::seed().unwrap();policy.controller.command_wait_ms=10;ControllerConfig{limits:Arc::new(policy),validation_executable:env!("CARGO_BIN_EXE_borg-runtime").into(),..Default::default()}}
fn input(agent:&str)->TaskInput{TaskInput{ resolved_limits: None,repository_read:None,instruction:"controlled session work".into(),definition:Definition{ limit_overrides: None,id:agent.into(),revision:"v1".into(),executor:ExecutorKind::Native,inference:Some(InferenceSettings::selected()),execution_profile:Some(ExecutionProfile::CollaborationV1{web_search:None})},repository:None,acceptance_commands:vec![],output_schema:Some(json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false})),limits:Limits{deadline_unix_ms:now()+600_000,model_requests:20,output_bytes:4096,result_bytes:borg_runtime::limits::RuntimeLimits::seed().unwrap().task_defaults.result_bytes,max_delegation_depth:4},dependencies:vec![],native_workflow:None}}
fn now()->u64{std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64}
async fn x(c:&Controller,command:Operation)->Reply{let r=c.handle(Request{protocol_revision:WIRE_REVISION,request_id:format!("request-{}",NEXT.fetch_add(1,Ordering::SeqCst)),command:command.clone()}).await;r.validate_for(&command).unwrap();r}
fn task(r:Reply)->Task{match r.outcome{Outcome::Ok{data:Data::Task(t)}=>*t,o=>panic!("expected task {o:?}")}}
fn refused(r:Reply,code:&str){assert!(matches!(r.outcome,Outcome::Refused{code:ref c,..} if c==code),"{r:?}");}
async fn submit(c:&Controller,id:&str,t:TaskInput)->Task{task(x(c,Operation::SubmitTask{operation_id:id.into(),input:t}).await)}
async fn get(c:&Controller,id:&str)->Task{task(x(c,Operation::GetTask{task_id:id.into()}).await)}
async fn session(c:&Controller,id:&str)->Session{match x(c,Operation::QueryCollaboration{query:Query::Session{session_id:id.into()}}).await.outcome{Outcome::Ok{data:Data::Collaboration(QueryResult::Session(s))}=>s,o=>panic!("{o:?}")}}
async fn sessions(c:&Controller,agent:Option<&str>,after:Option<String>,limit:usize)->borg_runtime::collaboration::Page<Session>{match x(c,Operation::QueryCollaboration{query:Query::Sessions{agent_id:agent.map(Into::into),after,limit}}).await.outcome{Outcome::Ok{data:Data::Collaboration(QueryResult::Sessions(s))}=>s,o=>panic!("{o:?}")}}
fn worker(n:usize,c:usize)->Worker{Worker{id:"worker".into(),incarnation:"incarnation-1".into(),native_capacity:n,coding_capacity:c}}
async fn register(c:&Controller,w:&Worker){assert!(matches!(x(c,Operation::RegisterWorker{operation_id:format!("register-{}-{}",w.id,w.incarnation),worker:w.clone()}).await.outcome,Outcome::Ok{data:Data::Worker(_)}));}
async fn commands(c:&Controller,w:&Worker,ack:Vec<String>)->Vec<WorkerCommand>{match x(c,Operation::AwaitCommands{operation_id:format!("await-{}",NEXT.fetch_add(1,Ordering::SeqCst)),worker_id:w.id.clone(),incarnation:w.incarnation.clone(),acknowledged:ack}).await.outcome{Outcome::Ok{data:Data::Commands(v)}=>v,Outcome::Pending{..}=>vec![],o=>panic!("{o:?}")}}
fn launch(v:&[WorkerCommand],id:&str)->(String,Session,Attempt,TaskInput){v.iter().find_map(|c|match c{WorkerCommand::RunAttempt{command_id,session,attempt,task,..} if attempt.task_id==id=>Some((command_id.clone(),session.clone(),attempt.clone(),task.clone())),_=>None}).unwrap_or_else(||panic!("no launch for {id}: {v:?}"))}
async fn report(c:&Controller,a:&Attempt,id:&str,observation:Observation)->Reply{x(c,Operation::ObserveWorker{report:Report{report_id:id.into(),task_id:a.task_id.clone(),attempt_id:a.id.clone(),generation:a.generation,worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone(),observation}}).await}
async fn start(c:&Controller,a:&Attempt,label:&str){task(report(c,a,&format!("{label}-started"),Observation::Started{thread_id:None,turn_id:None}).await);}
fn actor(a:&Attempt)->Caller{Caller{task_id:a.task_id.clone(),attempt_id:a.id.clone(),generation:a.generation,worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone()}}
fn invoke(a:&Attempt,id:&str,tool:&str,args:serde_json::Value)->Operation{Operation::CallCollaboration{call:Invocation{operation_id:id.into(),logical_call_id:format!("logical-{id}"),caller:actor(a),tool:tool.into(),arguments:args}}}
async fn failure(c:&Controller,a:&Attempt,label:&str)->Task{task(report(c,a,&format!("{label}-executor"),Observation::ExecutorOutcome{success:false,summary:"controlled failed executor".into()}).await);task(report(c,a,&format!("{label}-stopped"),Observation::ProcessTreeStopped).await);task(report(c,a,&format!("{label}-candidate"),Observation::ResultCandidate{summary:"controlled failure".into(),checkpoint:None,checks_passed:false,validation:vec![],output:None}).await)}
async fn closed(p:&Path){tokio::time::timeout(Duration::from_secs(3),async{loop{let f=std::fs::OpenOptions::new().read(true).write(true).open(p.with_extension("db.controller-lock")).unwrap();if f.try_lock().is_ok(){break}tokio::time::sleep(Duration::from_millis(5)).await}}).await.unwrap();}

use borg_runtime::collaboration::{AgentMemoryObservation,AgentMemoryPage};
fn memory(r:Reply)->AgentMemoryObservation{match r.outcome{Outcome::Ok{data:Data::AgentMemory(v)}=>v,o=>panic!("memory response: {o:?}")}}
async fn observe(c:&Controller,a:&Attempt,n:usize)->AgentMemoryObservation{memory(x(c,Operation::ObserveAgentMemory{caller:actor(a),limit:n}).await)}
fn prepare(a:&Attempt,id:&str,p:&AgentMemoryPage,n:usize)->Operation{Operation::PrepareAgentMemory{operation_id:id.into(),caller:actor(a),expected_cursor:p.from_cursor.clone(),through_sequence:p.through_sequence,limit:n}}
async fn acknowledge(c:&Controller,a:&Attempt,id:&str,batch:&str)->Reply{x(c,Operation::AcknowledgeAgentMemory{operation_id:id.into(),caller:actor(a),batch_id:batch.into()}).await}
async fn clear(c:&Controller,a:&Attempt){for _ in 0..100{let state=observe(c,a,100).await;let batch=if let Some(b)=state.pending{b}else if let Some(p)=state.available{memory(x(c,prepare(a,&format!("clear-{}",NEXT.fetch_add(1,Ordering::SeqCst)),&p,100)).await).pending.unwrap()}else{return};assert!(batch.page.notices.is_empty(),"unexpected notice during fixture baseline");memory(acknowledge(c,a,&format!("ack-{}",NEXT.fetch_add(1,Ordering::SeqCst)),&batch.batch_id).await);}panic!("unbounded fixture clear")}
async fn write(c:&Controller,a:&Attempt,id:&str,key:&str,expected:Option<u64>)->Reply{x(c,invoke(a,id,"collab_memory_set",json!({"key":key,"expected_revision":expected,"value":{"data":"not-a-notice"}}))).await}
async fn actors(c:&Controller)->(Task,Attempt,Task,Attempt,Attempt){let ta=submit(c,"actor-a",input("shared")).await;let tb=submit(c,"actor-b",input("shared")).await;let tc=submit(c,"actor-c",input("unrelated")).await;let w=worker(3,0);register(c,&w).await;let v=commands(c,&w,vec![]).await;let (_,_,a,_)=launch(&v,&ta.id);let (_,_,b,_)=launch(&v,&tb.id);let (_,_,other,_)=launch(&v,&tc.id);start(c,&a,"a").await;start(c,&b,"b").await;start(c,&other,"c").await;(ta,a,tb,b,other)}
// Traceability scope: Real controller writes expose the committed key/revision to other same-agent sessions, exclude own/foreign writes and omit memory values.
//= spec/impl/v2/implementation.md#shared-memory-changes-reach-sessions
//= type=test
//# A committed memory update MUST make its changed key and committed revision available to other active sessions of the same agent.
#[tokio::test]
async fn memory_notices_origin_filter_precedes_coalescing_and_values_never_flow(){
 let c=Controller::open_with_config(dir(),cfg()).unwrap();let (_,a,_,b,other)=actors(&c).await;clear(&c,&a).await;clear(&c,&b).await;
 assert!(matches!(write(&c,&a,"a-k1","k",None).await.outcome,Outcome::Ok{..}));assert!(matches!(write(&c,&b,"b-k2","k",Some(1)).await.outcome,Outcome::Ok{..}));assert!(matches!(write(&c,&a,"a-k3","k",Some(2)).await.outcome,Outcome::Ok{..}));write(&c,&other,"other-secret","foreign",None).await;
 let pa=observe(&c,&a,100).await.available.unwrap();let pb=observe(&c,&b,100).await.available.unwrap();assert_eq!(pa.notices,vec![borg_runtime::collaboration::AgentMemoryNotice{key:"k".into(),revision:2}]);assert_eq!(pb.notices[0].revision,3);assert_eq!(pb.notices.len(),1);assert!(!serde_json::to_string(&pa).unwrap().contains("not-a-notice"));
}
#[tokio::test]
async fn memory_notices_new_session_starts_at_creation_not_inventory(){
 let c=Controller::open_with_config(dir(),cfg()).unwrap();let ta=submit(&c,"first",input("shared")).await;let w=worker(2,0);register(&c,&w).await;let v=commands(&c,&w,vec![]).await;let (_,_,a,_)=launch(&v,&ta.id);start(&c,&a,"first").await;write(&c,&a,"old-memory","old",None).await;
 let tb=submit(&c,"second",input("shared")).await;write(&c,&a,"after-instantiation","new",None).await;let v=commands(&c,&w,vec![]).await;let (_,_,b,_)=launch(&v,&tb.id);start(&c,&b,"second").await;let o=observe(&c,&b,100).await;assert!(o.start_sequence>0);let p=o.available.unwrap();assert_eq!(p.notices.len(),1);assert_eq!(p.notices[0].key,"new");
}
#[tokio::test]
async fn memory_pending_and_ack_original_receipts_survive_loss_restart_and_new_worker(){
 let p=dir();let c=Controller::open_with_config(&p,cfg()).unwrap();let (_,a,tb,b,_)=actors(&c).await;clear(&c,&b).await;write(&c,&a,"write-first","key",None).await;let page=observe(&c,&b,100).await.available.unwrap();let op=prepare(&b,"prepare-original",&page,100);let prepared=x(&c,op.clone()).await;let pending=memory(prepared.clone()).pending.unwrap();drop(prepared.clone());
 write(&c,&a,"write-after-pending","key",Some(1)).await;assert_eq!(observe(&c,&b,100).await.pending,Some(pending.clone()));let replay=x(&c,op.clone()).await;assert_eq!(replay.outcome,prepared.outcome);assert_eq!(replay.commit,prepared.commit);
 let failed=failure(&c,&b,"receiver-failure").await;assert_eq!(failed.status,TaskStatus::Failed);let retried=task(x(&c,Operation::RetryTask{operation_id:"receiver-retry".into(),task_id:tb.id.clone(),expected_revision:failed.revision}).await);assert_eq!(retried.session_id,tb.session_id);
 let mut new=worker(1,0);new.id="replacement-worker".into();new.incarnation="replacement".into();register(&c,&new).await;let offered=commands(&c,&new,vec![]).await;let (_,_,successor,_)=launch(&offered,&tb.id);start(&c,&successor,"replacement").await;assert_ne!(successor.id,b.id);
 refused(x(&c,Operation::ObserveAgentMemory{caller:actor(&b),limit:100}).await,"stale_generation");refused(acknowledge(&c,&b,"stale-worker-ack",&pending.batch_id).await,"stale_generation");drop(c);closed(&p).await;let c=Controller::open_with_config(&p,cfg()).unwrap();assert_eq!(observe(&c,&successor,100).await.pending,Some(pending.clone()));let ack=Operation::AcknowledgeAgentMemory{operation_id:"ack-original".into(),caller:actor(&successor),batch_id:pending.batch_id.clone()};let ar=x(&c,ack.clone()).await;let done=memory(ar.clone());assert!(done.pending.is_none());assert_eq!(done.acknowledged_cursor,pending.page.to_cursor);let replay=x(&c,ack).await;assert_eq!(replay.outcome,ar.outcome);assert_eq!(replay.commit,ar.commit);let later=observe(&c,&successor,100).await.available.unwrap();assert!(later.notices.iter().any(|n|n.key=="key"&&n.revision==2));
}
#[tokio::test]
async fn memory_observation_empty_poll_and_ack_do_not_create_history_wake_loop(){
 let c=Controller::open_with_config(dir(),cfg()).unwrap();let (_,_,_,b,_)=actors(&c).await;clear(&c,&b).await;let op=Operation::ObserveAgentMemory{caller:actor(&b),limit:100};let first=x(&c,op.clone()).await;assert!(first.commit.is_none());assert!(memory(first.clone()).available.is_none());let revision=first.observed_controller_revision.unwrap();for _ in 0..5{let r=x(&c,op.clone()).await;assert_eq!(r.observed_controller_revision,Some(revision));assert!(r.commit.is_none());assert!(memory(r).available.is_none());}let change=c.wait_for_change(revision).await.unwrap();assert!(!change.changed);assert_eq!(change.revision,revision);
}
#[tokio::test]
async fn memory_scan_bounds_raw_rows_and_item_overflow_never_skip_notices(){
 let c=Controller::open_with_config(dir(),cfg()).unwrap();let (_,a,_,b,other)=actors(&c).await;clear(&c,&b).await;
 for n in 0..140{task(report(&c,&other,&format!("noise-{n}"),Observation::Progress{text:"bounded unrelated event".into(),output:String::new(),truncated:false}).await);if n==65{write(&c,&a,"middle-write","first",None).await;}}
 write(&c,&a,"last-first","first",Some(1)).await;write(&c,&a,"last-second","second",None).await;let mut observed=std::collections::BTreeMap::new();let mut pages=0;
 loop{let o=observe(&c,&b,1).await;let Some(p)=o.available else{break};assert!(p.to_cursor.sequence-p.from_cursor.sequence<=64);assert!(p.notices.len()<=1);for n in &p.notices{let v=observed.entry(n.key.clone()).or_insert(0);*v=(*v).max(n.revision);}let pending=memory(x(&c,prepare(&b,&format!("scan-prepare-{pages}"),&p,1)).await).pending.unwrap();assert_eq!(pending.page,p);memory(acknowledge(&c,&b,&format!("scan-ack-{pages}"),&pending.batch_id).await);pages+=1;assert!(pages<10);}
 assert!(pages>=3);assert_eq!(observed.get("first"),Some(&2));assert_eq!(observed.get("second"),Some(&1));
}
#[tokio::test]
async fn memory_prepare_commit_rollback_and_stale_cas_emit_no_phantom_delivery(){
 let p=dir();let probe=Arc::new(AtomicBool::new(false));let mut config=cfg();config.qualification_fail_next_commit=Some(probe.clone());let c=Controller::open_with_config(&p,config).unwrap();let (_,a,_,b,_)=actors(&c).await;clear(&c,&b).await;
 probe.store(true,Ordering::SeqCst);refused(write(&c,&a,"rolled-back","x",None).await,"store_unavailable");assert!(observe(&c,&b,100).await.available.is_none());write(&c,&a,"committed","x",None).await;let page=observe(&c,&b,100).await.available.unwrap();refused(write(&c,&a,"stale-write","x",None).await,"stale_revision");assert_eq!(observe(&c,&b,100).await.available,Some(page.clone()));let op=prepare(&b,"rolled-prepare",&page,100);probe.store(true,Ordering::SeqCst);refused(x(&c,op.clone()).await,"store_unavailable");let unchanged=observe(&c,&b,100).await;assert!(unchanged.pending.is_none());assert_eq!(unchanged.available,Some(page));let prepared=x(&c,op.clone()).await;assert!(memory(prepared.clone()).pending.is_some());let replay=x(&c,op).await;assert_eq!(replay.commit,prepared.commit);assert_eq!(replay.outcome,prepared.outcome);
}
#[tokio::test]
async fn memory_scope_future_cursor_deadline_cancel_and_wrong_batch_refuse(){
 let clock=Arc::new(AtomicU64::new(now()));let mut config=cfg();let cl=clock.clone();config.clock=Arc::new(move||cl.load(Ordering::SeqCst));let c=Controller::open_with_config(dir(),config).unwrap();let (_,a,tb,b,_)=actors(&c).await;clear(&c,&b).await;write(&c,&a,"fence-memory","key",None).await;let p=observe(&c,&b,100).await.available.unwrap();
 for (n,field) in ["database","agent","session"].into_iter().enumerate(){let mut page=p.clone();match field{"database"=>page.from_cursor.database_id="wrong".into(),"agent"=>page.from_cursor.agent_id="wrong".into(),_=>page.from_cursor.session_id="wrong".into()};refused(x(&c,prepare(&b,&format!("wrong-{n}"),&page,100)).await,"stale_revision");}
 let mut future=p.clone();future.through_sequence=u64::MAX;refused(x(&c,prepare(&b,"future-window",&future,100)).await,"invalid_input");refused(x(&c,Operation::ObserveAgentMemory{caller:actor(&b),limit:0}).await,"invalid_input");let prepared=memory(x(&c,prepare(&b,"valid-pending",&p,100)).await);let batch=prepared.pending.unwrap();refused(acknowledge(&c,&b,"wrong-batch-ack","not-the-batch").await,"batch_mismatch");
 let current=get(&c,&tb.id).await;task(x(&c,Operation::CancelTask{operation_id:"cancel-receiver".into(),task_id:tb.id,expected_revision:current.revision}).await);assert!(matches!(acknowledge(&c,&b,"cancelled-ack",&batch.batch_id).await.outcome,Outcome::Refused{..}));clock.store(a.deadline_unix_ms+1,Ordering::SeqCst);assert!(matches!(x(&c,Operation::ObserveAgentMemory{caller:actor(&a),limit:100}).await.outcome,Outcome::Refused{..}));
}
#[tokio::test]
async fn memory_corrupt_pending_batch_and_cursor_refuse_without_repair(){
 for field in ["notice","cursor","start"]{let p=dir();let c=Controller::open_with_config(&p,cfg()).unwrap();let (_,a,tb,b,_)=actors(&c).await;clear(&c,&b).await;write(&c,&a,"corrupt-test-write","key",None).await;let page=observe(&c,&b,100).await.available.unwrap();memory(x(&c,prepare(&b,"corrupt-test-prepare",&page,100)).await);drop(c);closed(&p).await;let db=rusqlite::Connection::open(&p).unwrap();match field{"notice"=>{let raw:String=db.query_row("SELECT memory_pending FROM sessions WHERE id=?1",[&tb.session_id],|r|r.get(0)).unwrap();let mut batch:borg_runtime::collaboration::AgentMemoryBatch=serde_json::from_str(&raw).unwrap();batch.page.notices[0].revision+=1;db.execute("UPDATE sessions SET memory_pending=?2 WHERE id=?1",rusqlite::params![tb.session_id,serde_json::to_string(&batch).unwrap()]).unwrap();},"cursor"=>{db.execute("UPDATE sessions SET memory_cursor_seq=-1 WHERE id=?1",[&tb.session_id]).unwrap();},_=>{db.execute("UPDATE sessions SET memory_start_seq=9223372036854775807 WHERE id=?1",[&tb.session_id]).unwrap();}}drop(db);let before=std::fs::read(&p).unwrap();assert!(Controller::open_with_config(&p,cfg()).is_err());assert_eq!(std::fs::read(&p).unwrap(),before);}
}

#[tokio::test]
async fn memory_concurrent_prepare_ack_receipts_and_failed_ack_preserve_exact_pending(){
 let probe=Arc::new(AtomicBool::new(false));let mut config=cfg();config.qualification_fail_next_commit=Some(probe.clone());let c=Controller::open_with_config(dir(),config).unwrap();let (_,a,_,b,_)=actors(&c).await;clear(&c,&b).await;
 write(&c,&a,"concurrent-memory","key",None).await;let page=observe(&c,&b,100).await.available.unwrap();let op=prepare(&b,"concurrent-prepare",&page,100);
 let (one,two)=tokio::join!(x(&c,op.clone()),x(&c,op.clone()));assert_eq!(one.commit,two.commit);assert_eq!(one.outcome,two.outcome);let pending=memory(one.clone()).pending.unwrap();
 let mut changed=op.clone();if let Operation::PrepareAgentMemory{limit,..}=&mut changed{*limit=99;}refused(x(&c,changed).await,"operation_conflict");
 let different=prepare(&b,"separate-prepare-occurrence",&page,100);let more=x(&c,different).await;assert_eq!(memory(more).pending,Some(pending.clone()));
 write(&c,&a,"concurrent-after-pending","key",Some(1)).await;let ack=Operation::AcknowledgeAgentMemory{operation_id:"concurrent-ack".into(),caller:actor(&b),batch_id:pending.batch_id.clone()};
 probe.store(true,Ordering::SeqCst);refused(x(&c,ack.clone()).await,"store_unavailable");assert!(!probe.load(Ordering::SeqCst));let still=observe(&c,&b,100).await;assert_eq!(still.pending,Some(pending.clone()));assert_eq!(still.acknowledged_cursor,page.from_cursor);
 let (one,two)=tokio::join!(x(&c,ack.clone()),x(&c,ack.clone()));assert_eq!(one.commit,two.commit);assert_eq!(one.outcome,two.outcome);let done=memory(one);assert!(done.pending.is_none());assert_eq!(done.acknowledged_cursor,pending.page.to_cursor);
 refused(x(&c,prepare(&b,"stale-cursor-after-ack",&page,100)).await,"stale_revision");refused(acknowledge(&c,&b,"fresh-id-old-batch",&pending.batch_id).await,"not_found");
 let later=observe(&c,&b,100).await.available.unwrap();assert!(later.notices.iter().any(|n|n.key=="key"&&n.revision==2));
}
