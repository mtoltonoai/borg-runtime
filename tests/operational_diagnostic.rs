//! Ordinary diagnostic configuration through real controller/SQLite/HTTP/MCP.
//! Report/model choices are controlled fixtures, not live model or worker claims.
//! The canonical receipt oracle checks facts; schema acceptance alone does not.
use borg_runtime::{
    collaboration::{AttemptView, QueryResult},
    controller::{Controller, ControllerConfig},
    limits::{ExecutionLimitOverrides, RuntimeLimits},
    mcp::{Caller, Client, Invocation},
    native_loop_contract as ids,
    protocol::*,
    server,
};
use serde_json::{Value, json};
use std::{path::PathBuf, sync::{Arc, atomic::{AtomicU64, Ordering}}};
static NEXT: AtomicU64 = AtomicU64::new(1);
fn unique(label: &str) -> String { format!("{label}-{}", NEXT.fetch_add(1, Ordering::Relaxed)) }
fn profile() -> Value { serde_json::from_str(include_str!("../metadata/operational-diagnostic.json")).unwrap() }
fn task(reply: &Reply) -> Task {
    match &reply.outcome { Outcome::Ok { data:Data::Task(t) } => *t.clone(), other => panic!("task: {other:?}") }
}
fn selected(reply: &Reply) -> Definition {
    match &reply.outcome { Outcome::Ok { data:Data::Agent(a) } => a.definition.clone(), other => panic!("agent: {other:?}") }
}
fn applied(reply: &Reply) -> DefinitionUpdateOutcome {
    match &reply.outcome { Outcome::Ok { data:Data::DefinitionUpdate(v) } => *v.clone(), other => panic!("apply: {other:?}") }
}
fn refusal(reply:&Reply)->(&str,&str) {
    match &reply.outcome { Outcome::Refused{code,message}=>(code,message), other=>panic!("refusal: {other:?}") }
}
struct Lab {
    controller: Controller,
    worker: Worker,
    path: PathBuf,
    config: ControllerConfig,
    base: String,
    server: tokio::task::JoinHandle<Result<(),String>>,
}
impl Lab {
    async fn new() -> Self {
        let dir=std::env::temp_dir().join(format!("borg-operational-diagnostic-{}-{}",std::process::id(),unique("db")));
        std::fs::create_dir(&dir).unwrap();
        let mut cfg=ControllerConfig{validation_executable:PathBuf::from(env!("CARGO_BIN_EXE_borg-runtime")),..ControllerConfig::default()};
        Arc::make_mut(&mut cfg.limits).controller.command_wait_ms=10;
        Self::open(dir.join("tasks.sqlite"),cfg,true).await
    }
    async fn open(path:PathBuf,cfg:ControllerConfig,register:bool)->Self {
        let controller=Controller::open_with_config(&path,cfg.clone()).unwrap();
        let listener=server::listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let base=format!("http://{}",listener.local_addr().unwrap());
        let serving=tokio::spawn(server::serve(controller.clone(),listener,base.clone()));
        let lab=Self{controller,path,config:cfg,worker:Worker{id:"controlled-worker".into(),incarnation:"one".into(),native_capacity:2,coding_capacity:0},base,server:serving};
        if register { let r=lab.x(Operation::RegisterWorker{operation_id:unique("register"),worker:lab.worker.clone()}).await;
        assert!(matches!(r.outcome,Outcome::Ok{data:Data::Worker(_)})); }
        lab
    }
    async fn x(&self, command:Operation)->Reply {
        let reply=Box::pin(self.controller.handle(Request{protocol_revision:WIRE_REVISION,request_id:unique("request"),command:command.clone()})).await;
        reply.validate_for(&command).unwrap();reply
    }
    async fn get(&self,id:&str)->Task { task(&self.x(Operation::GetTask{task_id:id.into()}).await) }
    async fn submit(&self,input:impl Into<Box<TaskInput>>)->Task {
        task(&self.x(Operation::SubmitTask{operation_id:unique("submit"),input: (input).into()}).await)
    }
    async fn start(&self,t:&Task)->Actor {
        let r=self.x(Operation::AwaitCommands{operation_id:unique("await"),worker_id:self.worker.id.clone(),incarnation:self.worker.incarnation.clone(),acknowledged:vec![]}).await;
        let a=match r.outcome { Outcome::Ok{data:Data::Commands(cs)}=>cs.into_iter().find_map(|c|match c{WorkerCommand::RunAttempt{attempt,..} if attempt.task_id==t.id=>Some(attempt),_=>None}).expect("requested task command"),x=>panic!("commands: {x:?}")};
        self.report(&a,unique("started"),Observation::Started{thread_id:None,turn_id:None}).await;
        let caller=Caller{task_id:t.id.clone(),attempt_id:a.id.clone(),generation:a.generation,worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone()};
        let client=Client::new(&self.base,caller.clone(),self.controller.limits().clone()).unwrap();
        assert!(client.tools().await.unwrap().iter().any(|t|t.name=="collab_definition_update"));
        Actor{attempt:a,caller,client,index:0,calls:vec![]}
    }
    async fn report(&self,a:&Attempt,id:String,observation:Observation)->Task {
        task(&self.report_reply(a,id,observation).await)
    }
    async fn report_reply(&self,a:&Attempt,id:String,observation:Observation)->Reply {
        self.x(Operation::ObserveWorker{report:Report{report_id:id,worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone(),task_id:a.task_id.clone(),attempt_id:a.id.clone(),generation:a.generation,observation}}).await
    }
    async fn memory(&self,agent:&str)->Option<MemoryValue> {
        match self.x(Operation::GetMemory{agent_id:agent.into(),key:"diagnostic-payload".into()}).await.outcome {Outcome::Ok{data:Data::Memory(v)}=>v,x=>panic!("memory: {x:?}")}
    }
}
impl Drop for Lab { fn drop(&mut self){self.server.abort();} }
struct Actor { attempt:Attempt, caller:Caller, client:Client, index:u32, calls:Vec<NativeCallEvidence> }
impl Actor {
    // Every invocation here is a controlled completed model response choosing
    // a tool or the final answer. Tool refusals and failed task outcomes below
    // remain their own original facts; no failed provider response is relabelled.
    async fn completed_request(&mut self,lab:&Lab) {
        self.index+=1;
        let reserved=lab.report_reply(&self.attempt,ids::reservation_id(&self.attempt.id,self.index),Observation::Usage{native_diagnostic_reserved:true,model_requests:self.index,input_tokens:None,output_tokens:None}).await;
        let accepted=task(&reserved);
        assert_eq!(accepted.id,self.attempt.task_id);
        assert_eq!(accepted.attempt.as_ref().unwrap().id,self.attempt.id);
        let observed=lab.report_reply(&self.attempt,ids::diagnostic_id(&self.attempt.id,self.index),Observation::NativeRequestDiagnostic{
            request_index:self.index,
            endpoint:"https://api.openai.com/v1/responses".into(),
            model:accepted.input.definition.required_inference().unwrap().model.clone(),
            // Fixed synthetic request body b"{}"; this fixture calls no provider.
            request_binding:Some(borg_runtime::native::ProviderRequestBinding{
                bytes:2,sha256:"44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a".into(),
            }),
            observed_outcome:ids::NativeRequestObservedOutcome::Completed,
            failure:None,
        }).await;
        assert!(matches!(observed.outcome,Outcome::Ok{data:Data::Acknowledged}),"{observed:?}");
        assert!(observed.commit.as_ref().unwrap().controller_revision>reserved.commit.as_ref().unwrap().controller_revision);
    }
    async fn call(&mut self,lab:&Lab,tool:&str,args:Value)->(Invocation,Reply) {
        self.completed_request(lab).await;
        let call=Invocation{operation_id:ids::call_operation_id(&self.attempt.id,self.index,1),logical_call_id:format!("controlled-function-{}",self.index),caller:self.caller.clone(),tool:tool.into(),arguments:args};
        let r=self.client.call(&call).await.unwrap();assert!(!r.requires_quiescence);
        assert!(r.reply.commit.is_some(),"every exercised valid occurrence is settled");
        self.calls.push(NativeCallEvidence{call_index:1,request_index:self.index,operation_id:call.operation_id.clone(),logical_call_id:call.logical_call_id.clone()});
        (call,r.reply)
    }
    async fn finish(&mut self,lab:&Lab,success:bool,summary:&str,output:Option<Value>)->Task {
        if success {
            self.completed_request(lab).await;
            lab.report(&self.attempt,ids::completion_id(&self.attempt.id),Observation::NativeLoopCompleted{requests:Vec::new(),final_response_index:Some(self.index),model_requests:self.index,calls:self.calls.clone()}).await;
        }
        lab.report(&self.attempt,unique("executor"),Observation::ExecutorOutcome{success,summary:summary.into()}).await;
        lab.report(&self.attempt,unique("stopped"),Observation::ProcessTreeStopped).await;
        lab.report(&self.attempt,unique("candidate"),Observation::ResultCandidate{checks_passed:success,summary:summary.into(),checkpoint:None,validation:if success{vec![ids::evidence(&self.attempt.id)]}else{vec![]},output}).await
    }
}
fn input(limits:&RuntimeLimits,definition:Definition,instruction:String,schema:Value)->TaskInput {
    TaskInput{ instruction_source: None, pull_request: None, resolved_limits:None,repository_read:None,instruction,definition,repository:None,acceptance_commands:vec![],output_schema:Some(schema),limits:limits.task_defaults.at(borg_runtime::worker::process::now_ms()).unwrap(),dependencies:vec![],native_workflow:None}
}
fn workload(limits:&RuntimeLimits)->TaskInput {
    let mut collaboration=limits.execution.collaboration.clone();collaboration.complete_result_bytes=2048;
    input(limits,Definition{charter: None, id:"bounded-result-consumer".into(),revision:"revision-1".into(),executor:ExecutorKind::Native,inference:Some(InferenceSettings::selected()),execution_profile:Some(ExecutionProfile::CollaborationV1{web_search:None}),limit_overrides:Some(Box::new(ExecutionLimitOverrides{collaboration:Some(collaboration),..Default::default()}))},"Write the configured diagnostic-payload once, then answer 1. This is an independent controlled workload, never a retry of an uncertain task.".into(),json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false}))
}
fn diagnostic(limits:&RuntimeLimits,origin:&Task)->TaskInput {
    let p=profile();
    input(limits,serde_json::from_value(p["definition"].clone()).unwrap(),format!("{}\nOPERATIONAL_CONTEXT\n{}",p["instruction"].as_str().unwrap(),json!({"origin_task_id":origin.id,"target_agent_id":origin.assigned_agent_id,"later_task_ids":[]})),p["output_schema"].clone())
}
fn payload()->Value {json!({"key":"diagnostic-payload","expected_revision":null,"value":"x".repeat(8192)})}
fn observation(t:&Task,view:&AttemptView,stamp:u64)->Value {
    json!({"task_id":t.id,"task_revision":t.revision,"definition_revision":t.input.definition.revision,"observed_controller_revision":stamp,"status":t.status,"reported_model_requests":view.reported_model_requests,"input_tokens":view.input_tokens,"output_tokens":view.output_tokens})
}
async fn attempts(lab:&Lab,actor:&mut Actor,id:&str)->(AttemptView,u64) {
    let (_,reply)=actor.call(lab,"collab_task_attempts_list",json!({"task_id":id,"cursor":null,"limit":lab.controller.limits().controller.collaboration_page_items})).await;
    let stamp=reply.observed_controller_revision.unwrap();
    match reply.outcome{Outcome::Ok{data:Data::Collaboration(QueryResult::TaskAttempts(page))}=>{assert!(page.next_cursor.is_none());assert_eq!(page.items.len(),1);(page.items[0].clone(),stamp)},x=>panic!("attempt evidence: {x:?}")}
}
fn report(change:&DefinitionUpdateOutcome,commit:&MutationCommit,baseline:Value,later:Option<Value>,status:&str)->Value {
    json!({"disposition":if later.is_some(){"measured"}else{"applied_unmeasured"},"origin_task_id":baseline["task_id"],"target_agent_id":change.selected_definition.id,"reason":"Actual complete-result refusal; restore the current configured default for later independent work.","change":{"operation_id":commit.operation_id,"controller_revision":commit.controller_revision,"previous_revision":change.previous_revision,"selected_revision":change.selected_definition.revision,"global_revision":change.global_revision,"policy_revision":change.policy.revision,"changed_fields":change.changed_fields,"classification":change.classification,"effective_for":change.effective_for},"measurement":{"status":status,"baseline":baseline,"later":later,"comparison":if later.is_some(){"success_after_failure"}else{"not_comparable"},"causal_improvement_claim":false,"limitations":["Controlled report producer and model choices; real controller/SQLite/MCP, no live model or real worker inference."]}})
}
// These are test citations only. The ordinary profile is explicitly bound as
// configuration, not relabelled as Rust implementation by this fixture.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# The shared agent interface MUST support diagnosis of configuration refusals using existing bounded operational records.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# Evaluation of a configuration change MUST identify its committed change outcome and the definition revisions of the observed work.
#[tokio::test]
async fn ordinary_diagnostic_applies_then_measures_actual_later_work() {
    let lab=Lab::new().await;
    let origin=lab.submit(workload(lab.controller.limits())).await;
    let original_input=origin.input.clone();
    let mut original=lab.start(&origin).await;
    let (failed_call,failed_reply)=original.call(&lab,"collab_memory_set",payload()).await;
    assert_eq!(refusal(&failed_reply).0,"response_bound");
    assert!(refusal(&failed_reply).1.contains("execution.collaboration.complete_result_bytes"));
    assert!(lab.memory(&origin.assigned_agent_id).await.is_none());
    let failed=original.finish(&lab,false,refusal(&failed_reply).1,None).await;
    assert_eq!(failed.status,TaskStatus::Failed);
    let diagnostic_task=lab.submit(diagnostic(lab.controller.limits(),&origin)).await;
    let diagnostic_input=diagnostic_task.input.clone();
    let mut agent=lab.start(&diagnostic_task).await;
    let (_,r)=agent.call(&lab,"collab_task_get",json!({"task_id":origin.id})).await;
    let observed_origin=task(&r);assert_eq!(observed_origin.input,original_input);
    let (baseline_attempt,baseline_stamp)=attempts(&lab,&mut agent,&origin.id).await;
    let failure=baseline_attempt.executor_outcome.as_ref().unwrap();assert!(!failure.success);assert_eq!(failure.summary,refusal(&failed_reply).1);
    let (_,history)=agent.call(&lab,"collab_task_history_list",json!({"task_id":origin.id,"cursor":null,"limit":lab.controller.limits().controller.collaboration_page_items})).await;
    match history.outcome {Outcome::Ok{data:Data::Collaboration(QueryResult::TaskHistory(page))}=>{assert!(page.next_cursor.is_none());assert!(page.items.iter().any(|e|e.kind=="worker_observation"));},x=>panic!("history: {x:?}")}
    let (_,current)=agent.call(&lab,"collab_limits_get",json!({})).await;
    let global=match current.outcome{Outcome::Ok{data:Data::Collaboration(QueryResult::Limits{global,execution})}=>{assert_eq!(execution,diagnostic_input.resolved_limits);*global},x=>panic!("limits: {x:?}")};
    let (_,head)=agent.call(&lab,"collab_agent_get",json!({"agent_id":origin.assigned_agent_id})).await;
    let predecessor=selected(&head);assert_eq!(predecessor,original_input.definition);
    let (_,old)=agent.call(&lab,"collab_definition_get",json!({"definition_id":predecessor.id,"revision":predecessor.revision})).await;
    assert!(matches!(&old.outcome,Outcome::Ok{data:Data::Collaboration(QueryResult::Definition(d))}if d==&predecessor));
    let policy=global.definition_updates.as_ref().unwrap();
    let (update,reply)=agent.call(&lab,"collab_definition_update",json!({"agent_id":predecessor.id,"expected_revision":predecessor.revision,"expected_global_revision":global.revision,"expected_policy_revision":policy.revision,"changes":{"collaboration.complete_result_bytes":global.execution.collaboration.complete_result_bytes}})).await;
    let changed=applied(&reply);let commit=reply.commit.clone().unwrap();
    assert_eq!(changed.previous_revision,predecessor.revision);assert_eq!(changed.effective_for,ConfigurationEffect::FutureSubmissions);
    let (_,receipt)=agent.call(&lab,"collab_call_get",json!({"call_id":update.operation_id})).await;
    match &receipt.outcome {Outcome::Ok{data:Data::Operation(record)}=>{assert_eq!(record.id,update.operation_id);assert_eq!(record.intended,serde_json::to_value(update.operation()).unwrap());assert_eq!(record.outcome,Some(serde_json::to_value(&reply.outcome).unwrap()));},x=>panic!("receipt: {x:?}")}
    assert_ne!(receipt.commit,reply.commit,"lookup has its own stamp, never the original mutation stamp");
    let (_,head)=agent.call(&lab,"collab_agent_get",json!({"agent_id":origin.assigned_agent_id})).await;assert_eq!(selected(&head),changed.selected_definition);
    // The local qualification driver admits separate later work. The diagnostic
    // task has no workload creation/retry permission and never submits this.
    let mut later_input=original_input.clone();later_input.definition=changed.selected_definition.clone();later_input.resolved_limits=None;
    let later=lab.submit(later_input).await;assert_ne!(later.id,origin.id);
    let mut consumer=lab.start(&later).await;
    let (_,success)=consumer.call(&lab,"collab_memory_set",payload()).await;
    assert!(matches!(success.outcome,Outcome::Ok{data:Data::Memory(Some(_))}));
    assert_eq!(lab.memory(&origin.assigned_agent_id).await.unwrap().value,json!("x".repeat(8192)));
    let completed=consumer.finish(&lab,true,"same controlled workload accepted",Some(json!({"answer":1}))).await;
    assert_eq!(completed.status,TaskStatus::Succeeded);
    let (_,seen)=agent.call(&lab,"collab_task_get",json!({"task_id":later.id})).await;
    let observed_later=task(&seen);let (later_attempt,later_stamp)=attempts(&lab,&mut agent,&later.id).await;
    assert_eq!(observed_later.input.instruction,original_input.instruction);assert_eq!(observed_later.input.output_schema,original_input.output_schema);assert_eq!(observed_later.input.repository,original_input.repository);
    assert_eq!(observed_later.input.definition,changed.selected_definition);assert!(later_attempt.executor_outcome.as_ref().unwrap().success);
    let result=report(&changed,&commit,observation(&observed_origin,&baseline_attempt,baseline_stamp),Some(observation(&observed_later,&later_attempt,later_stamp)),"observed");
    let validator=jsonschema::options().with_draft(jsonschema::Draft::Draft202012).build(&profile()["output_schema"]).unwrap();assert!(validator.is_valid(&result));
    assert_eq!(result["change"]["controller_revision"],json!(commit.controller_revision));assert_eq!(result["measurement"]["baseline"]["definition_revision"],json!(predecessor.revision));assert_eq!(result["measurement"]["later"]["definition_revision"],json!(changed.selected_definition.revision));
    let done=agent.finish(&lab,true,"observed outcome, no causality claim",Some(result.clone())).await;
    assert_eq!(done.status,TaskStatus::Succeeded);assert_eq!(done.result.unwrap().output,Some(result));
    assert_eq!(lab.get(&origin.id).await.input,original_input);assert_eq!(lab.get(&origin.id).await.status,TaskStatus::Failed);assert_eq!(lab.get(&diagnostic_task.id).await.input,diagnostic_input);
    let recovered=original.client.get_call(&failed_call).await.unwrap().unwrap();assert_eq!(recovered.reply.outcome,failed_reply.outcome);assert_eq!(recovered.reply.commit,failed_reply.commit);
}
#[test]
fn diagnostic_schema_refuses_contradictory_measurement_claims() {
    let p=profile();let validator=jsonschema::options().with_draft(jsonschema::Draft::Draft202012).build(&p["output_schema"]).unwrap();
    let no_action=json!({"disposition":"no_action","origin_task_id":"origin","target_agent_id":"agent","reason":"No justified configured-limit change","change":null,"measurement":{"status":"not_applied","baseline":null,"later":null,"comparison":"not_comparable","causal_improvement_claim":false,"limitations":[]}});
    assert!(validator.is_valid(&no_action));
    let mut bad=no_action.clone();bad["disposition"]=json!("measured");assert!(!validator.is_valid(&bad));
    let mut bad=no_action.clone();bad["measurement"]["status"]=json!("incomplete_evidence");assert!(!validator.is_valid(&bad));
    let mut bad=no_action.clone();bad["measurement"]["causal_improvement_claim"]=json!(true);assert!(!validator.is_valid(&bad));
}

async fn failed_origin(lab:&Lab)->(Task,Actor,Invocation,Reply) {
    let original=lab.submit(workload(lab.controller.limits())).await;
    let mut actor=lab.start(&original).await;
    let (call,refused)=actor.call(lab,"collab_memory_set",payload()).await;
    assert_eq!(refusal(&refused).0,"response_bound");
    let terminal=actor.finish(lab,false,refusal(&refused).1,None).await;
    assert_eq!(terminal.status,TaskStatus::Failed);assert!(lab.memory(&original.assigned_agent_id).await.is_none());
    (terminal,actor,call,refused)
}
fn change_args(lab:&Lab,origin:&Task)->Value {
    let limits=lab.controller.limits();
    json!({"agent_id":origin.assigned_agent_id,"expected_revision":origin.input.definition.revision,"expected_global_revision":limits.revision,"expected_policy_revision":limits.definition_updates.as_ref().unwrap().revision,"changes":{"collaboration.complete_result_bytes":limits.execution.collaboration.complete_result_bytes}})
}
fn unapplied(origin:&Task,disposition:&str,reason:&str)->Value {
    json!({"disposition":disposition,"origin_task_id":origin.id,"target_agent_id":origin.assigned_agent_id,"reason":reason,"change":null,"measurement":{"status":"not_applied","baseline":null,"later":null,"comparison":"not_comparable","causal_improvement_claim":false,"limitations":["No application or later-work improvement claimed."]}})
}
async fn assert_head(lab:&Lab,definition:&Definition) {
    let head=lab.x(Operation::GetAgent{agent_id:definition.id.clone()}).await;
    assert_eq!(selected(&head),*definition);
}
#[tokio::test]
async fn diagnostic_stale_selected_global_and_policy_end_without_change() {
    for field in ["expected_revision","expected_global_revision","expected_policy_revision"] {
        let lab=Lab::new().await;let (origin,_,_,_)=failed_origin(&lab).await;
        let old=origin.input.clone();let diagnostic_task=lab.submit(diagnostic(lab.controller.limits(),&origin)).await;
        let mut actor=lab.start(&diagnostic_task).await;
        let mut args=change_args(&lab,&origin);args[field]=json!("stale-exact-binding");
        let (call,reply)=actor.call(&lab,"collab_definition_update",args).await;
        assert_eq!(refusal(&reply).0,"configuration_conflict");assert!(reply.commit.is_some());
        let receipt=actor.client.get_call(&call).await.unwrap().unwrap();assert_eq!(receipt.reply.outcome,reply.outcome);assert_eq!(receipt.reply.commit,reply.commit);
        assert_head(&lab,&old.definition).await;assert!(lab.memory(&origin.assigned_agent_id).await.is_none());
        let done=actor.finish(&lab,true,"stale binding: one refusal, no fresh application attempt",Some(unapplied(&origin,"refused",refusal(&reply).1))).await;
        assert_eq!(done.status,TaskStatus::Succeeded);assert_eq!(actor.calls.iter().filter(|c|c.operation_id==call.operation_id).count(),1);
        assert_eq!(lab.get(&origin.id).await.input,old);assert_eq!(lab.get(&origin.id).await.status,TaskStatus::Failed);
    }
}
#[tokio::test]
async fn diagnostic_uncertain_origin_is_no_action_and_never_retried() {
    let lab=Lab::new().await;let origin=lab.submit(workload(lab.controller.limits())).await;
    let original=lab.start(&origin).await;
    let uncertain=lab.report(&original.attempt,unique("uncertain"),Observation::ExecutionUncertain{message:"original external result is not known; do not repeat".into()}).await;
    assert!(uncertain.attempt.as_ref().unwrap().uncertain);let before=lab.get(&origin.id).await;
    let d=lab.submit(diagnostic(lab.controller.limits(),&origin)).await;let mut actor=lab.start(&d).await;
    let (_,r)=actor.call(&lab,"collab_task_get",json!({"task_id":origin.id})).await;assert!(task(&r).attempt.unwrap().uncertain);
    let (view,_)=attempts(&lab,&mut actor,&origin.id).await;assert!(view.attempt.uncertain);
    let done=actor.finish(&lab,true,"uncertain original is not a diagnosed policy refusal",Some(unapplied(&origin,"no_action","Original attempt remains uncertain; no retry, configuration change or measurement."))).await;
    assert_eq!(done.status,TaskStatus::Succeeded);assert_eq!(lab.get(&origin.id).await,before);assert_head(&lab,&origin.input.definition).await;assert!(lab.memory(&origin.assigned_agent_id).await.is_none());
}
#[tokio::test]
async fn diagnostic_absent_or_incomparable_later_work_stays_applied_unmeasured() {
    for missing in ["no_later_task","incomparable_revision","missing_observation"] {
        let lab=Lab::new().await;let (origin,_,_,_)=failed_origin(&lab).await;
        let d=lab.submit(diagnostic(lab.controller.limits(),&origin)).await;let mut actor=lab.start(&d).await;
        let (baseline,stamp)=attempts(&lab,&mut actor,&origin.id).await;
        let (_,reply)=actor.call(&lab,"collab_definition_update",change_args(&lab,&origin)).await;let changed=applied(&reply);let commit=reply.commit.unwrap();
        if missing!="no_later_task" {
            let mut next=origin.input.clone();next.resolved_limits=None;
            if missing=="missing_observation"{next.definition=changed.selected_definition.clone();}
            let later=lab.submit(next).await;
            let (_,seen)=actor.call(&lab,"collab_task_get",json!({"task_id":later.id})).await;let seen=task(&seen);
            if missing=="incomparable_revision" {assert_ne!(seen.input.definition.revision,changed.selected_definition.revision);} else {assert_eq!(seen.input.definition,changed.selected_definition);assert_eq!(seen.status,TaskStatus::Queued);assert!(seen.result.is_none());}
        } else {
            let (_,page)=actor.call(&lab,"collab_tasks_list",json!({"cursor":null,"limit":lab.controller.limits().controller.collaboration_page_items,"agent_id":origin.assigned_agent_id,"parent_task_id":null})).await;
            assert!(matches!(page.outcome,Outcome::Ok{..}));
        }
        let output=report(&changed,&commit,observation(&origin,&baseline,stamp),None,missing);
        assert_eq!(output["disposition"],"applied_unmeasured");assert!(output["measurement"]["later"].is_null());
        let done=actor.finish(&lab,true,"applied; no comparable observed later completion",Some(output)).await;
        assert_eq!(done.status,TaskStatus::Succeeded);assert_head(&lab,&changed.selected_definition).await;assert_eq!(lab.get(&origin.id).await.input,origin.input);assert!(lab.memory(&origin.assigned_agent_id).await.is_none());
    }
}
// Controlled report choices exercise canonical evidence retention and result
// acceptance. They do not claim that a model follows the diagnostic instruction.
#[tokio::test]
async fn diagnostic_failed_and_truncated_later_evidence_never_claims_success() {
    for truncated in [false,true] {
        let lab=Lab::new().await;
        let (origin,_,_,_)=failed_origin(&lab).await;
        let d=lab.submit(diagnostic(lab.controller.limits(),&origin)).await;
        let mut actor=lab.start(&d).await;
        let (baseline,baseline_stamp)=attempts(&lab,&mut actor,&origin.id).await;
        let (_,reply)=actor.call(&lab,"collab_definition_update",change_args(&lab,&origin)).await;
        let changed=applied(&reply);let commit=reply.commit.unwrap();
        let mut input=origin.input.clone();
        input.definition=changed.selected_definition.clone();input.resolved_limits=None;
        let later=lab.submit(input).await;
        assert_ne!(later.id,origin.id);
        let mut consumer=lab.start(&later).await;
        let retained="bounded but incomplete later evidence";
        if truncated {
            assert!(retained.len()<=later.input.limits.output_bytes);
            let progress=lab.report(&consumer.attempt,unique("truncated-later"),Observation::Progress{
                text:"controlled later output snapshot".into(),output:retained.into(),truncated:true,
            }).await;
            assert!(progress.output_truncated);assert_eq!(progress.recent_output,retained);
        }
        let summary="controlled later execution failed";
        let failed=consumer.finish(&lab,false,summary,None).await;
        assert_eq!(failed.status,TaskStatus::Failed);
        let (_,seen)=actor.call(&lab,"collab_task_get",json!({"task_id":later.id})).await;
        let observed=task(&seen);
        assert_eq!(observed,failed);
        assert_eq!(observed.input.definition,changed.selected_definition);
        assert_eq!(observed.output_truncated,truncated);
        if truncated {assert_eq!(observed.recent_output,retained);}
        let (later_attempt,later_stamp)=attempts(&lab,&mut actor,&later.id).await;
        let outcome=later_attempt.executor_outcome.as_ref().unwrap();
        assert!(!outcome.success);assert_eq!(outcome.summary,summary);
        assert_eq!(later_attempt.attempt,observed.attempt.clone().unwrap());
        assert_eq!(baseline.input_tokens,None);assert_eq!(baseline.output_tokens,None);
        assert_eq!(later_attempt.input_tokens,None);assert_eq!(later_attempt.output_tokens,None);
        let mut output=if truncated {
            report(&changed,&commit,observation(&origin,&baseline,baseline_stamp),None,"incomplete_evidence")
        } else {
            let mut measured=report(&changed,&commit,observation(&origin,&baseline,baseline_stamp),
                Some(observation(&observed,&later_attempt,later_stamp)),"observed");
            measured["measurement"]["comparison"]=json!("failure_after_failure");
            measured
        };
        if truncated {
            output["measurement"]["limitations"].as_array_mut().unwrap().push(json!(
                format!("Required later output is truncated: task {} revision {} observed controller revision {}",observed.id,observed.revision,later_stamp)));
            assert_eq!(output["disposition"],"applied_unmeasured");
            assert!(output["measurement"]["later"].is_null());
        } else {
            assert_eq!(output["disposition"],"measured");
            assert_eq!(output["measurement"]["later"]["status"],"failed");
            assert!(output["measurement"]["later"]["input_tokens"].is_null());
            assert!(output["measurement"]["later"]["output_tokens"].is_null());
        }
        assert_ne!(output["measurement"]["comparison"],"success_after_failure");
        assert_eq!(output["measurement"]["causal_improvement_claim"],false);
        let validator=jsonschema::options().with_draft(jsonschema::Draft::Draft202012)
            .build(&profile()["output_schema"]).unwrap();
        assert!(validator.is_valid(&output),"{output}");
        let done=actor.finish(&lab,true,"observed failure or incomplete evidence",Some(output.clone())).await;
        assert_eq!(done.status,TaskStatus::Succeeded);assert_eq!(done.result.unwrap().output,Some(output));
        assert_head(&lab,&changed.selected_definition).await;
        assert_eq!(lab.get(&origin.id).await,origin);
        assert_eq!(lab.get(&later.id).await,observed);
        assert!(lab.memory(&origin.assigned_agent_id).await.is_none());
    }
}
async fn released(path:&std::path::Path) {
    let mut name=path.as_os_str().to_owned();name.push(".controller-lock");
    tokio::time::timeout(std::time::Duration::from_secs(3),async{
        loop {
            let file=std::fs::OpenOptions::new().read(true).write(true).open(PathBuf::from(&name)).unwrap();
            if file.try_lock().is_ok(){break;}
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }).await.expect("previous controller owner released");
}
#[tokio::test]
async fn diagnostic_restart_recovers_original_apply_without_repeating_origin() {
    let lab=Lab::new().await;let (origin,original,failed_call,failed_reply)=failed_origin(&lab).await;
    let d=lab.submit(diagnostic(lab.controller.limits(),&origin)).await;let mut actor=lab.start(&d).await;
    let (call,first)=actor.call(&lab,"collab_definition_update",change_args(&lab,&origin)).await;
    let changed=applied(&first);let path=lab.path.clone();let cfg=lab.config.clone();let old_origin=origin.input.clone();let old_d=d.input.clone();
    // The real HTTP server gives each keep-alive connection its own controller
    // clone. Close both client pools before releasing the owner, as a process
    // shutdown would; stopping only the accept-loop leaves connections alive.
    let caller=actor.caller.clone();let original_caller=original.caller.clone();
    drop(actor);drop(original);drop(lab);released(&path).await;
    let lab=Lab::open(path,cfg,false).await;
    let client=Client::new(&lab.base,caller,lab.controller.limits().clone()).unwrap();
    let recovered=client.get_call(&call).await.unwrap().unwrap();assert_eq!(recovered.reply.outcome,first.outcome);assert_eq!(recovered.reply.commit,first.commit);
    let exact=client.call(&call).await.unwrap();assert_eq!(exact.reply.outcome,first.outcome);assert_eq!(exact.reply.commit,first.commit);
    assert_head(&lab,&changed.selected_definition).await;assert_eq!(lab.get(&origin.id).await.input,old_origin);assert_eq!(lab.get(&origin.id).await.status,TaskStatus::Failed);assert_eq!(lab.get(&d.id).await.input,old_d);
    let original_client=Client::new(&lab.base,original_caller,lab.controller.limits().clone()).unwrap();
    let failure=original_client.get_call(&failed_call).await.unwrap().unwrap();assert_eq!(failure.reply.outcome,failed_reply.outcome);assert_eq!(failure.reply.commit,failed_reply.commit);assert!(lab.memory(&origin.assigned_agent_id).await.is_none());
    // Exact replay is readback of the same logical change, not a fresh diagnosis.
    let mut rewritten=call.clone();rewritten.arguments["changes"]["collaboration.complete_result_bytes"]=json!(lab.controller.limits().execution.collaboration.complete_result_bytes+1);
    assert_eq!(refusal(&client.call(&rewritten).await.unwrap().reply).0,"operation_conflict");
    assert_head(&lab,&changed.selected_definition).await;
}
#[tokio::test]
async fn independent_diagnostic_caller_cannot_verify_another_call_receipt() {
    let lab=Lab::new().await;let (origin,_,_,_)=failed_origin(&lab).await;
    let d=lab.submit(diagnostic(lab.controller.limits(),&origin)).await;let mut actor=lab.start(&d).await;
    let (call,first)=actor.call(&lab,"collab_definition_update",change_args(&lab,&origin)).await;let changed=applied(&first);
    let mut second_input=diagnostic(lab.controller.limits(),&origin);second_input.definition.id="independent-diagnostic".into();
    let second=lab.submit(second_input).await;let mut other=lab.start(&second).await;
    let (_,lookup)=other.call(&lab,"collab_call_get",json!({"call_id":call.operation_id})).await;
    assert_eq!(refusal(&lookup).0,"caller_mismatch");
    let output=unapplied(&origin,"no_action","Previous caller's commit is not independently visible; external asserted result is not canonical verification.");
    let done=other.finish(&lab,true,"receipt visibility insufficient; no second change",Some(output)).await;assert_eq!(done.status,TaskStatus::Succeeded);assert_head(&lab,&changed.selected_definition).await;
}
