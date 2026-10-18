//! The retired fixed Inspect flow is not an admissible clean-state profile.
//! Current collaboration receipt/usage acceptance is also qualified in mcp_calls.
use super::*;
use borg_runtime::task_tool::{self as contract,ToolObservation};
async fn setup()->(Controller,Task,Attempt){let c=Controller::open_with_config(path(),config()).unwrap();let t=submit(&c,"inspect-replacement",native("inspect-agent")).await;let w=worker("inspect-worker","inc",1,0);register(&c,&w).await;let a=run(&commands(&c,&w,vec![]).await);task(report(&c,&a,"started",Observation::Started{thread_id:None,turn_id:None}).await);(c,t,a)}
async fn projection(c:&Controller,a:&Attempt)->ToolObservation{let reply=c.handle(Request{protocol_revision:WIRE_REVISION,request_id:"projection".into(),command:Operation::GetTask{task_id:a.task_id.clone()}}).await;// Deliberately synthesize the retired marker as ordinary progress data;
// it is not an observed current-profile tool receipt and must grant no acceptance.
let observed=reply.observed_controller_revision.unwrap();let t=task(reply.outcome);ToolObservation{call_id:"fixed-fixture".into(),task:contract::CurrentTaskProjection{task_id:t.id,agent_id:t.assigned_agent_id,definition:t.input.definition,status:t.status,observed_controller_revision:observed,deadline_unix_ms:a.deadline_unix_ms,attempt_id:a.id.clone(),generation:a.generation}}}
fn progress(v:&ToolObservation)->Observation{Observation::Progress{text:contract::PROGRESS_TEXT.into(),output:contract::encode(v,16384,&metadata().execution.native).unwrap(),truncated:false}}
#[tokio::test]
async fn native_tool_fixed_workflow_refuses_new_profile_before_dispatch(){
 let c=Controller::open_with_config(path(),config()).unwrap();
 for n in [1,2]{let mut t=native(&format!("inspect-{n}"));t.native_workflow=Some(Box::new(NativeWorkflow::InspectCurrentTaskThenContinue{}));t.limits.model_requests=n;let op=Operation::SubmitTask{operation_id:format!("fixed-{n}"),input:t};let first=call(&c,op.clone()).await;assert!(matches!(first,Outcome::Refused{..}));assert_eq!(call(&c,op).await,first);}
 assert!(matches!(call(&c,Operation::ListTasks{after:None,limit:10}).await,Outcome::Ok{data:Data::Tasks(v)} if v.is_empty()));
}
#[tokio::test]
async fn native_tool_progress_marker_cannot_replace_current_loop_completion(){
 let (c,_,a)=setup().await;let v=projection(&c,&a).await;assert_eq!(code(report(&c,&a,&contract::report_id(&a.id),progress(&v)).await),"invalid_transition");
 for n in [1,2]{controlled_native_request(&c,&a,n,None,None).await;}
 task(report(&c,&a,"executor",Observation::ExecutorOutcome{success:true,summary:"controlled".into()}).await);task(report(&c,&a,"stopped",Observation::ProcessTreeStopped).await);
 let outcome=report(&c,&a,"candidate",Observation::ResultCandidate{checks_passed:true,summary:"must not accept fixed marker".into(),checkpoint:None,validation:vec![contract::evidence(&a.id)],output:Some(json!({"answer":1}))}).await;
 assert_eq!(code(outcome),"source_mismatch");assert!(get(&c,&a.task_id).await.result.is_none());
}
#[tokio::test]
async fn native_tool_progress_replay_is_immutable_and_cancelled_session_cannot_publish(){
 let (c,_,a)=setup().await;let mut v=projection(&c,&a).await;let id=contract::report_id(&a.id);let first=report(&c,&a,&id,progress(&v)).await;assert_eq!(code(first.clone()),"invalid_transition");assert_eq!(report(&c,&a,&id,progress(&v)).await,first);v.call_id="changed".into();assert_eq!(code(report(&c,&a,&id,progress(&v)).await),"operation_conflict");
 let t=get(&c,&a.task_id).await;task(call(&c,Operation::CancelTask{operation_id:"cancel".into(),task_id:t.id,expected_revision:t.revision}).await);
 // Cleanup diagnostics remain reportable after cancellation; they are not acceptance.
 task(report(&c,&a,"after-cancel",progress(&v)).await);
 assert_eq!(code(report(&c,&a,"cancelled-candidate",Observation::ResultCandidate{checks_passed:true,summary:"must remain cancelled".into(),checkpoint:None,validation:vec![contract::evidence(&a.id)],output:Some(json!({"answer":1}))}).await),"invalid_transition");
 let final_task=get(&c,&a.task_id).await;assert_eq!(final_task.status,TaskStatus::Cancelled);assert!(final_task.result.is_none());
}
#[tokio::test]
async fn native_tool_unbound_definition_refusal_replays_without_creating_task(){
 let c=Controller::open_with_config(path(),config()).unwrap();
 for (field,code_expected) in [("inference","template_inference_unbound"),("profile","template_profile_unbound")]{let mut t=native(field);if field=="inference"{t.definition.inference=None}else{t.definition.execution_profile=None};let op=Operation::SubmitTask{operation_id:format!("unbound-{field}"),input:t};let first=call(&c,op.clone()).await;assert_eq!(code(first.clone()),code_expected);assert_eq!(call(&c,op).await,first);}
 assert!(matches!(call(&c,Operation::ListTasks{after:None,limit:10}).await,Outcome::Ok{data:Data::Tasks(v)} if v.is_empty()));
}
