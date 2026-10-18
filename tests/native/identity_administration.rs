//! Controlled Native Responses and the real Coding stdio MCP bridge share the
//! actual controller. No provider, installed Codex, deployment or review vote claim.
#[path = "../identity_support.rs"]
mod support;
use support::*;
use borg_runtime::{protocol::*,mcp::{self,Client,CallResult,Tool},native::{self,NativeFunction,NativeLoopProfile,NativeLoopStep,ValidationHost,ValidationRuntime,CharterExchange,CharterInput},worker::{codex_mcp,process}};
use serde_json::{json,Value};
use borg_runtime::charter::*;
use std::{sync::Arc,time::Duration,process::Stdio};
use tokio::io::{AsyncBufReadExt,AsyncWriteExt,BufReader};

struct Server{url:String,join:tokio::task::JoinHandle<Result<(),String>>}
impl Server {
    async fn new(h:&Harness)->Self {let listener=borg_runtime::server::listen("127.0.0.1:0".parse().unwrap()).await.unwrap();let url=format!("http://{}",listener.local_addr().unwrap());let c=h.c.clone();let origin=url.clone();let join=tokio::spawn(async move{borg_runtime::server::serve(c,listener,origin).await});Self{url,join}}
    async fn close(self){self.join.abort();assert!(self.join.await.unwrap_err().is_cancelled());}
}
fn client(h:&Harness,s:&Server)->Client {Client::new_for_profile(&s.url,h.caller.clone(),(*h.original.input.resolved_limits.as_ref().unwrap().global_metadata).clone(),h.original.input.definition.execution_profile.clone().unwrap()).unwrap()}
async fn native_call(h:&Harness,c:&Client,tools:&[Tool],tool:&str,args:Value,index:u32)->CallResult {
    let profile=NativeLoopProfile{functions:tools.iter().map(|t|NativeFunction{name:t.name.clone(),description:t.description.clone(),parameters:t.input_schema.clone()}).collect(),web_search:None};
    let response=json!({"id":format!("controlled-response-{index}"),"status":"completed","usage":{"input_tokens":5,"output_tokens":3,"total_tokens":8},"output":[{"type":"function_call","id":format!("item-{index}"),"status":"completed","call_id":format!("native-call-{index}"),"name":tool,"arguments":args.to_string()}]});
    let peer=super::Fixture::start(200,serde_json::to_vec(&response).unwrap(),Duration::ZERO,"").await;
    let input=h.current().await.input;
    let bounds=input.execution_limits().unwrap().native.clone();let path=h.root.join(id("native-helper"));std::fs::create_dir(&path).unwrap();
    let caller=&h.caller;
    ok(api(&h.c,Operation::ObserveWorker{report:Report{report_id:borg_runtime::native_loop_contract::reservation_id(&caller.attempt_id,index),worker_id:caller.worker_id.clone(),incarnation:caller.incarnation.clone(),task_id:caller.task_id.clone(),attempt_id:caller.attempt_id.clone(),generation:caller.generation,observation:Observation::Usage{model_requests:index,input_tokens:None,output_tokens:None,native_diagnostic_reserved:true}}}).await);
    let Data::CharterQuery(CharterQueryResult::Session(Some(effective)))=ok(api(&h.c,Operation::QueryCharter{query:CharterQuery::Session{session_id:h.original.session_id.clone()}}).await) else{panic!("accepted charter session")};
    let turn_id=id("native-boundary");
    let Data::Charter(CharterOutcome::Boundary(boundary))=ok(api(&h.c,Operation::ObserveCharter{operation_id:turn_id.clone(),caller:caller.clone(),observation:CharterObservation::BeginTurn{session_id:h.original.session_id.clone(),expected_revision:effective.session.revision,request_index:index}}).await) else{panic!("actual native turn admission")};
    let source=&boundary.effective.source;
    let exchange=Arc::new(CharterExchange::new(CharterInput{revision:serde_json::to_value(&boundary.effective.reference).unwrap(),source:serde_json::to_string(source).unwrap(),changed:false},&bounds).unwrap());
    let host=ValidationHost::new(env!("CARGO_BIN_EXE_borg-runtime").into(),path,Arc::new(bounds),Arc::new(ValidationRuntime::new(Arc::new(h.cfg.limits.worker.clone())).unwrap())).with_charter(exchange.clone());
    let observation=native::execute_loop_step(&input,&native::NativeConfig{endpoint:peer.endpoint.clone(),api_key_env:"HOME".into(),model:SELECTED_MODEL.into()},&profile,None,&host,async{Ok(())},std::future::pending()).await;
    let request=peer.received.await.unwrap();peer.handle.await.unwrap();
    assert_eq!(request["tools"].as_array().unwrap().iter().filter(|t|t["type"]=="function").count(),tools.len());
    let expected=tools.iter().find(|t|t.name==tool).unwrap();assert_eq!(request["tools"].as_array().unwrap().iter().find(|t|t["name"]==tool).unwrap()["parameters"],expected.input_schema);
    let NativeLoopStep::Call(call)=observation.result.unwrap() else{panic!("actual native tool call required")};assert_eq!(call.name(),tool);assert_eq!(call.arguments(),&args);
    let original=mcp::Invocation{operation_id:borg_runtime::native_loop_contract::call_operation_id(&h.caller.attempt_id,index,1),logical_call_id:call.call_id().into(),caller:h.caller.clone(),tool:call.name().into(),arguments:call.arguments().clone()};
    let captured=exchange.take_reply().unwrap().unwrap();
    assert_eq!(captured.call_ids,vec![call.call_id().to_string()]);assert!(captured.original_output.is_some());
    ok(api(&h.c,Operation::ObserveCharter{operation_id:id("native-reply"),caller:caller.clone(),observation:CharterObservation::Reply{turn_id:turn_id.clone(),reply:CharterReplyObservation{output:captured.original_output,output_reference:None,response_id:captured.response_id,output_item_ids:captured.item_ids,tool_call_ids:captured.call_ids,declaration:captured.declared}}}).await);
    let result=c.call(&original).await.unwrap();assert!(result.reply.commit.is_some());
    assert_eq!(c.call(&original).await.unwrap(),result);
    let lookup=api(&h.c,Operation::GetCollaborationCall{caller:h.caller.clone(),operation_id:original.operation_id.clone()}).await;
    assert_eq!(mcp::recovered(&original,lookup).unwrap().reply.outcome,result.reply.outcome);
    let closed=ok(api(&h.c,Operation::ObserveCharter{operation_id:id("native-complete-turn"),caller:caller.clone(),observation:CharterObservation::CompleteTurn{turn_id,results:vec![CharterResultReceipt::Collaboration{call_id:call.call_id().into(),operation_id:original.operation_id}]}}).await);
    assert!(matches!(closed,Data::Charter(CharterOutcome::Turn(ref turn)) if turn.completed_by.is_some()));
    assert_eq!(native::validation_cleanup_pending(),0,"actual Native helper process closure");
    result
}
struct CodingPeer {
    child:tokio::process::Child,input:Option<tokio::process::ChildStdin>,output:BufReader<tokio::process::ChildStdout>,config:codex_mcp::Config,sequence:u64,
}
impl CodingPeer {
    async fn start(h:&Harness,s:&Server)->Self {
        let task=h.current().await;let state=h.root.join("coding-bridge");process::private_directory(&state).unwrap();
        process::atomic_json(&state.join("borg-thread.json"),&json!({"thread_id":"controlled-coding-thread","turn_start_sent":true})).unwrap();
        let config=codex_mcp::Config{instruction_source:task.input.instruction_source.clone(),execution_profile:task.input.definition.execution_profile.clone(),runtime_limits:(*task.input.resolved_limits.as_ref().unwrap().global_metadata).clone(),limits:task.input.execution_limits().unwrap().collaboration.clone(),worker_limits:h.cfg.limits.worker.clone(),controller_url:s.url.clone(),caller:h.caller.clone(),deadline_unix_ms:task.input.limits.deadline_unix_ms,state};
        let config_path=config.state.join("bridge.json");process::atomic_json(&config_path,&config).unwrap();let log=std::fs::File::create(config.state.join("process.log")).unwrap();
        let mut child=tokio::process::Command::new(env!("CARGO_BIN_EXE_borg-runtime")).args(["__collaboration_mcp","--config"]).arg(&config_path).arg("--config-bytes").arg(config.limits.config_bytes.to_string()).env_clear().env("PATH","/usr/bin:/bin").env("HOME",&config.state).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(log).kill_on_drop(true).spawn().unwrap();
        let input=child.stdin.take();let output=BufReader::new(child.stdout.take().unwrap());let mut peer=Self{child,input,output,config,sequence:0};
        let init=peer.rpc("initialize",json!({"protocolVersion":mcp::VERSION,"capabilities":{},"clientInfo":{"name":"controlled-coding","version":"1"}})).await;assert_eq!(init["protocolVersion"],mcp::VERSION);peer
    }
    async fn rpc(&mut self,method:&str,params:Value)->Value {
        self.sequence+=1;let mut frame=serde_json::to_vec(&json!({"jsonrpc":"2.0","id":self.sequence,"method":method,"params":params})).unwrap();frame.push(b'\n');
        self.input.as_mut().unwrap().write_all(&frame).await.unwrap();self.input.as_mut().unwrap().flush().await.unwrap();
        let mut line=Vec::new();let n=tokio::time::timeout(Duration::from_secs(20),self.output.read_until(b'\n',&mut line)).await.unwrap().unwrap();assert!(n>0,"bridge closed before response; inspect {}",self.config.state.display());
        let response:Value=serde_json::from_slice(&line).unwrap();assert!(response.get("error").is_none(),"{response:?}");assert_eq!(response["id"],self.sequence);response["result"].clone()
    }
    async fn call(&mut self,name:&str,args:Value,logical:&str)->CallResult {
        let reply=self.rpc("tools/call",json!({"name":name,"arguments":args,"_meta":{"callId":logical,"threadId":"controlled-coding-thread","sessionId":"controlled-coding-session"}})).await;
        let outcome:CallResult=serde_json::from_str(reply["content"][0]["text"].as_str().unwrap()).unwrap();self.rpc("ping",json!({})).await;
        let journal:Value=serde_json::from_slice(&std::fs::read(self.config.directory().join("calls.json")).unwrap()).unwrap();let entry=journal["entries"].as_array().unwrap().iter().find(|e|e["invocation"]["logical_call_id"]==logical).unwrap();assert_eq!(entry["delivered"],true);assert_eq!(entry["outcome"],serde_json::to_value(&outcome).unwrap());outcome
    }
    async fn close(mut self){drop(self.input.take());let status=tokio::time::timeout(Duration::from_secs(20),self.child.wait()).await.unwrap().unwrap();assert_eq!(status.code(),Some(1),"bridge EOF is a recorded stop, not task success");let stop:Value=serde_json::from_slice(&std::fs::read(self.config.stop_path()).unwrap()).unwrap();assert_eq!(stop["caller"],serde_json::to_value(&self.config.caller).unwrap());}
}
#[tokio::test]
async fn native_and_coding_task_creation_share_real_mcp_contract(){
    let native=Harness::new("native-task-create","triage",ExecutorKind::Native,false).await;let ns=Server::new(&native).await;let nc=client(&native,&ns);let nt=nc.tools().await.unwrap();assert_eq!(nt.len(),65);
    let before=native.current().await;let result=native_call(&native,&nc,&nt,"collab_task_create",create_args(None),1).await;let created=task(result.reply);assert_eq!(created.assigned_agent_id,"triage");assert_ne!(created.id,before.id);assert_eq!(native.current().await.input,before.input);assert_eq!(native.current().await.status,before.status);assert_eq!(native.current().await.waiting_child_id,before.waiting_child_id);
    let coding=Harness::new("coding-task-create","engineer",ExecutorKind::Coding,false).await;let cs=Server::new(&coding).await;let mut peer=CodingPeer::start(&coding,&cs).await;
    let catalogue:Vec<Tool>=serde_json::from_value(peer.rpc("tools/list",json!({})).await["tools"].clone()).unwrap();assert_eq!(nt,catalogue);
    let mut args=create_args(Some("engineer"));args["repository"]=json!(coding.source);args["instruction_source"]=json!(coding.source);args["output_schema"]=Value::Null;args["acceptance_commands"]=json!([["/usr/bin/true"]]);
    let cbefore=coding.current().await;let result=peer.call("collab_task_create",args.clone(),"coding-task-create").await;let ct=task(result.reply.clone());assert_eq!(ct.input.definition.executor,ExecutorKind::Coding);assert!(ct.input.instruction_source.is_some());assert_eq!(coding.current().await,cbefore);
    assert_eq!(peer.call("collab_task_create",args,"coding-task-create").await,result);assert_eq!(coding.tasks().await.len(),2);
    peer.close().await;cs.close().await;ns.close().await;drop(nc);native.close().await;coding.close().await;
}
#[tokio::test]
async fn charter_creates_designer_and_requests_review_through_mcp(){
    let h=Harness::new("charter-designer","charter",ExecutorKind::Native,true).await;let server=Server::new(&h).await;let client=client(&h,&server);let tools=client.tools().await.unwrap();assert_eq!(tools.len(),71);
    let args=admin_args(&h.cfg.limits,json!({"definition":definition(&h.cfg.limits,"ui-designer",ExecutorKind::Native,false)}));
    let registered=native_call(&h,&client,&tools,"collab_agent_register",args,1).await;assert!(matches!(registered.reply.outcome,Outcome::Ok{data:Data::IdentityAdministration(ref r)} if matches!(&r.effect,IdentityEffect::Registered(a) if a.id=="ui-designer")));
    let mut args=create_args(Some("ui-designer"));args["instruction"]=json!("Review the human collaboration workflow for usability using this exact known source. Creation is not review completion or a release approval.");args["repository_read"]=json!(h.source);args["expected_agent_revision"]=json!("1");
    let review=task(native_call(&h,&client,&tools,"collab_task_create",args,2).await.reply);assert_eq!(review.assigned_agent_id,"ui-designer");assert_eq!(review.status,TaskStatus::Queued);assert!(review.result.is_none()&&review.parent_task_id.is_none());
    let doc=native_call(&h,&client,&tools,"collab_document_create",json!({"content":{"title":"Review scope","body":"Inspect the human collaboration workflow."},"supplied_content_id":null,"responsible_owner":{"kind":"agent","agent_id":"ui-designer"}}),3).await;assert!(matches!(doc.reply.outcome,Outcome::Ok{data:Data::DocumentMutation(_)}));
    assert_eq!(h.tasks().await.len(),2);let Data::Collaboration(borg_runtime::collaboration::QueryResult::TaskHistory(history))=ok(api(&h.c,Operation::QueryCollaboration{query:borg_runtime::collaboration::Query::TaskHistory{task_id:review.id.clone(),after:None,limit:16}}).await) else{panic!("canonical creation history")};let provenance:TaskCreationProvenance=serde_json::from_value(history.items.into_iter().find(|e|e.kind=="created_by").unwrap().body).unwrap();assert_eq!(provenance.operation_id,borg_runtime::native_loop_contract::call_operation_id(&h.caller.attempt_id,2,1));
    server.close().await;drop(client);let cfg=h.cfg.clone();let h=h.reopen(cfg).await;assert_eq!(selected(&h.c,"ui-designer").await.id,"ui-designer");h.close().await;
}
