//! Admission/preflight qualification with exact proposed metadata as test data.
//! The absent credential is a deliberate stop after helper validation and full
//! charter request construction; no BeginTurn, POST or activation is claimed.
use super::*;
use crate::{charter::*, controller::{Controller, ControllerConfig}, limits::RuntimeLimits, protocol::*};
use std::{path::{Path, PathBuf}, sync::{Arc, atomic::{AtomicBool, Ordering}}, time::Duration};

fn helper() -> PathBuf {
    let path = PathBuf::from(std::env::var_os("BORG_CHARTER_PROFILE_HELPER").expect("qualified ordinary helper image required"));
    assert!(path.is_absolute() && path.is_file()); path
}
fn policies() -> (RuntimeLimits, RuntimeLimits) {
    let old = RuntimeLimits::parse(include_bytes!("../worker/testdata/retained-startup-envelope.json")).unwrap();
    let new = RuntimeLimits::parse(include_bytes!("../worker/testdata/proposed-controller-envelope.json")).unwrap();
    assert!(old.charter.is_none() && old.native_retry.is_none());
    assert_eq!(new.execution.native.schema_bytes, 65_536);
    assert_eq!(new.execution.native.json_nodes, 8192);
    assert_eq!(old.execution.native.context_bytes,65_536);
    assert_eq!(old.execution.native.response_wire_bytes,6_358_528);
    for p in [&new] {
        assert_eq!(p.execution.native.context_bytes, u32::MAX as usize);
        assert_eq!(p.execution.native.request_bytes, u32::MAX as usize);
        assert_eq!(p.execution.native.response_wire_bytes, u32::MAX as usize);
        assert_eq!(p.execution.native.helper_value_bytes, u32::MAX as usize);
    }
    (old, new)
}
fn root(label: &str) -> PathBuf {
    let p=std::env::temp_dir().join(format!("{label}-{}-{}",std::process::id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    std::fs::create_dir(&p).unwrap(); p
}
fn open(path: &Path, limits: &RuntimeLimits) -> Controller {
    Controller::open_with_config(path, ControllerConfig {limits:Arc::new(limits.clone()), validation_executable:helper(), ..ControllerConfig::default()}).unwrap()
}
async fn release(path: &Path) {
    let mut name=path.as_os_str().to_os_string();name.push(".controller-lock");
    tokio::time::timeout(Duration::from_secs(3),async {
        loop {
            let file=std::fs::OpenOptions::new().read(true).write(true).open(PathBuf::from(&name)).unwrap();
            if file.try_lock().is_ok() {break}
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.expect("previous controller drained");
}
async fn call(c: &Controller, command: Operation) -> Reply {
    let reply=Box::pin(c.handle(Request {protocol_revision:WIRE_REVISION, request_id:"charter-profile-preflight".into(), command:command.clone()})).await;
    reply.validate_for(&command).unwrap(); reply
}
fn task(reply: Reply) -> Task {
    match reply.outcome {Outcome::Ok{data:Data::Task(t)}=>*t, other=>panic!("expected task: {other:?}")}
}
async fn get(c: &Controller, id: &str) -> Task {task(call(c,Operation::GetTask{task_id:id.into()}).await)}
async fn submit(c:&Controller,id:&str,input:TaskInput)->Task {task(call(c,Operation::SubmitTask{operation_id:id.into(),input}).await)}
async fn effective(c:&Controller,t:&Task)->EffectiveCharter {
    match call(c,Operation::QueryCharter{query:CharterQuery::Session{session_id:t.session_id.clone()}}).await.outcome {
        Outcome::Ok{data:Data::CharterQuery(CharterQueryResult::Session(Some(v)))}=>v, other=>panic!("session: {other:?}")
    }
}
fn input(label:&str,p:&RuntimeLimits)->TaskInput {
    TaskInput { instruction_source: None, pull_request: None, resolved_limits:None,repository_read:None,instruction:"Return the integer answer 7.".into(),
        definition:Definition{id:label.into(),revision:"revision-1".into(),executor:ExecutorKind::Native,
            inference:Some(InferenceSettings::selected()),execution_profile:Some(ExecutionProfile::CollaborationV1{web_search:None}),limit_overrides:None,charter:None},
        repository:None,acceptance_commands:vec![],output_schema:Some(json!({"type":"object","required":["answer"],"properties":{"answer":{"type":"integer"}},"additionalProperties":false})),
        limits:Limits{deadline_unix_ms:crate::worker::process::now_ms()+p.task_defaults.deadline_ms,model_requests:p.task_defaults.model_requests,output_bytes:p.task_defaults.output_bytes,result_bytes:p.task_defaults.result_bytes,max_delegation_depth:p.task_defaults.max_delegation_depth},
        dependencies:vec![],native_workflow:None}
}
fn maximum_source(p:&RuntimeLimits)->CharterSource {
    let l=p.charter.as_ref().unwrap();
    let clauses=(0..l.clauses).map(|n|CharterClauseRef{file:"c.md".into(),section:"Purpose".into(),sentence:format!("Agent MUST keep {n:03}.")}).collect::<Vec<_>>();
    let text=format!("# Purpose\n\n{}\n\n",clauses.iter().map(|c|c.sentence.as_str()).collect::<Vec<_>>().join("\n\n"));
    let mut source=CharterSource{files:vec![CharterFile{file:"c.md".into(),text}],clauses};
    let bytes=serde_json::to_vec(&source).unwrap().len();assert!(bytes<l.source_bytes);
    source.files[0].text.push_str(&"x".repeat(l.source_bytes-bytes));
    assert_eq!(serde_json::to_vec(&source).unwrap().len(),l.source_bytes);source
}
fn profile(t:&TaskInput)->NativeLoopProfile {
    NativeLoopProfile{functions:crate::mcp::catalogue_for_profile(&t.resolved_limits.as_ref().unwrap().global_metadata,t.definition.execution_profile.as_ref()).into_iter().map(|v|NativeFunction{name:v.name,description:v.description,parameters:v.input_schema}).collect(),web_search:None}
}
fn validation(t:&TaskInput,p:&NativeLoopProfile)->Value {
    serde_json::to_value(ValidationInput{schema:t.output_schema.clone().unwrap(),profile:p.clone(),output_bytes:t.limits.output_bytes,repair_enabled:t.resolved_limits.as_ref().unwrap().global_metadata.native_retry.is_some(),output_prefix:None,joined_charter:false}).unwrap()
}
fn key_nodes(v:&Value)->usize {match v {Value::Array(a)=>1+a.iter().map(key_nodes).sum::<usize>(),Value::Object(o)=>1+o.iter().map(|(_,v)|1+key_nodes(v)).sum::<usize>(),_=>1}}
fn exchange(source:&ResolvedCharter,p:&NativeLimits)->CharterExchange {
    CharterExchange::new(CharterInput{revision:serde_json::to_value(&source.reference).unwrap(),source:serde_json::to_string(&source.source).unwrap(),changed:false},p).unwrap()
}
// A projection from the identical production request fields supplements the
// actual constructor/helper execution below. It is not a captured provider POST.
fn request_projection(t:&TaskInput,p:&NativeLoopProfile,source:&ResolvedCharter)->Value {
    let n=&t.execution_limits().unwrap().native;let i=t.definition.inference.as_ref().unwrap();
    let body=json!({"model":i.model,"reasoning":{"effort":i.reasoning_effort},"input":[{"role":"user","content":t.instruction}],"store":false,"stream":false,"include":["reasoning.encrypted_content"],"max_output_tokens":n.max_output_tokens,
        "tools":p.functions.iter().map(|f|json!({"type":"function","name":f.name,"description":f.description,"parameters":f.parameters,"strict":false})).collect::<Vec<_>>(),"tool_choice":"auto","parallel_tool_calls":n.function_calls_per_response>1,"text":{"format":response_format(t.output_schema.as_ref().unwrap()).unwrap()}});
    exchange(source,n).prepare(&body,n).unwrap()
}
async fn preflight(t:&TaskInput,source:Option<&ResolvedCharter>,old:&RuntimeLimits,dir:&Path,expected:&str,reached:bool) {
    let p=profile(t);let n=&t.execution_limits().unwrap().native;
    let mut host=ValidationHost::new(helper(),dir.to_path_buf(),Arc::new(n.clone()),Arc::new(ValidationRuntime::new(Arc::new(old.worker.clone())).unwrap()));
    let capture=source.map(|source|Arc::new(exchange(source,n)));
    if let Some(c)=&capture {host=host.with_charter(c.clone())}
    const MISSING:&str="BORG_CHARTER_PROFILE_PREFLIGHT_NO_CREDENTIAL";
    assert!(std::env::var_os(MISSING).is_none(),"fixture requires absent sentinel, never a credential");
    let cfg=NativeConfig{endpoint:"http://127.0.0.1:9/v1/responses".into(),api_key_env:MISSING.into(),model:MODEL.into()};
    let before=AtomicBool::new(false);
    let r=Box::pin(execute_loop_step(t,&cfg,&p,None,&host,async {before.store(true,Ordering::SeqCst);Ok(())},std::future::pending())).await;
    assert_eq!(r.result.err().as_deref(),Some(expected));assert_eq!(before.load(Ordering::SeqCst),reached);
    assert_eq!(r.request_exposure,RequestExposure::NotStarted);assert_eq!(r.usage,UsageObservation::Unknown);
    if let Some(c)=capture {assert!(c.take_reply().unwrap().is_none());assert!(c.take_failure().unwrap().is_none())}
}

#[tokio::test]
#[ignore="requires qualified ordinary helper8 image via BORG_CHARTER_PROFILE_HELPER"]
async fn global_v2_maximum_charter_reuse_admits_and_preflights_without_changing_old_task() {
    let (old,new)=policies();let dir=root("charter-global");let db=dir.join("state.sqlite");
    let c=open(&db,&old);let t=submit(&c,"old-submit",input("historical-v1",&old)).await;let historical=get(&c,&t.id).await;
    drop(c);release(&db).await;let c=open(&db,&new);assert_eq!(get(&c,&t.id).await,historical);
    let rejected=call(&c,Operation::SubmitTask{operation_id:"old-unchartered-reuse".into(),input:input("historical-v1",&new)}).await;
    assert!(matches!(rejected.outcome,Outcome::Refused{ref code,..} if code=="charter_required"));
    let source=maximum_source(&new);let mut seed=input("charter-source",&new);seed.definition.charter=Some(DefinitionCharter::Inline{source:source.clone()});
    let seed=submit(&c,"source-submit",seed).await;let original=effective(&c,&seed).await;
    let mut next=input("global-v2-charter",&new);next.definition.execution_profile=Some(ExecutionProfile::CollaborationV2{web_search:None});next.definition.charter=Some(DefinitionCharter::Reference{source:original.charter.reference.clone()});
    let t=submit(&c,"global-submit",next.clone()).await;let accepted=get(&c,&t.id).await;let bound=effective(&c,&t).await;
    assert_eq!(bound.charter,original.charter);assert_eq!(bound.session.initial,original.charter.reference);assert_eq!(bound.session.revision,1);
    assert_eq!(t.input.definition,next.definition);assert_eq!(t.input.execution_limits().unwrap().native,new.execution.native);
    assert_eq!(t.input.limits.model_requests,u32::MAX);
    let p=profile(&t.input);assert_eq!(p.functions.len(),64);selected(&p,&new.execution.native).unwrap();
    let v=validation(&t.input,&p);let projected=request_projection(&t.input,&p,&bound.charter);
    let encoded=serde_json::to_vec(&projected).unwrap();
    decode::parse(&encoded,new.execution.native.request_bytes,Instant::now()+Duration::from_secs(5),"fixture_invalid",&new.execution.native,"native_request_bytes").unwrap();
    assert!(projected["input"][0]["content"].as_str().unwrap().contains(&serde_json::to_string(&serde_json::to_string(&source).unwrap()).unwrap()));
    println!("GLOBAL_CHARTER_PROFILE {}",json!({"functions":p.functions.len(),"source_bytes":serde_json::to_vec(&source).unwrap().len(),"clauses":source.clauses.len(),"validation_bytes":serde_json::to_vec(&v).unwrap().len(),"validation_key_nodes":key_nodes(&v),"request_projection_bytes":encoded.len(),"request_projection_key_nodes":key_nodes(&projected),"schema_bound":new.execution.native.schema_bytes,"nodes_bound":new.execution.native.json_nodes}));
    preflight(&t.input,Some(&bound.charter),&old,&dir.join("native"),"native_api_key_unavailable",true).await;
    let mut conflict=next.clone();conflict.definition.charter=Some(DefinitionCharter::Inline{source:source.clone()});
    assert!(matches!(call(&c,Operation::SubmitTask{operation_id:"same-definition-different-body".into(),input:conflict}).await.outcome,Outcome::Refused{ref code,..} if code=="definition_conflict"));
    let mut excessive=input("charter-too-large",&new);let mut oversized=source;oversized.files[0].text.push('x');excessive.definition.charter=Some(DefinitionCharter::Inline{source:oversized});
    assert!(matches!(call(&c,Operation::SubmitTask{operation_id:"source-one-over".into(),input:excessive}).await.outcome,Outcome::Refused{ref code,..} if code=="input_bound"));
    assert_eq!(get(&c,&historical.id).await,historical);assert_eq!(get(&c,&t.id).await,accepted);assert_eq!(effective(&c,&t).await,bound);
    drop(c);release(&db).await;
}

#[tokio::test]
#[ignore="requires qualified ordinary helper8 image via BORG_CHARTER_PROFILE_HELPER"]
async fn diagnostic_v4_keeps_v1_override_and_names_minimal_future_definition_delta() {
    let (old,new)=policies();let dir=root("charter-diagnostic");let db=dir.join("state.sqlite");let c=open(&db,&old);
    let diagnostic:Value=serde_json::from_str(include_str!("../../metadata/operational-diagnostic.json")).unwrap();
    let mut original=input("ignored",&old);original.definition=serde_json::from_value(diagnostic["definition"].clone()).unwrap();original.output_schema=Some(diagnostic["output_schema"].clone());
    let historical=submit(&c,"diagnostic-original",original.clone()).await;let historical=get(&c,&historical.id).await;
    let v=validation(&historical.input,&profile(&historical.input));assert_eq!(serde_json::to_vec(&v).unwrap().len(),19_746);
    preflight(&historical.input,None,&old,&dir.join("original-native"),"native_api_key_unavailable",true).await;
    drop(c);release(&db).await;let c=open(&db,&new);assert_eq!(get(&c,&historical.id).await,historical);
    let mut source_input=input("diagnostic-source",&new);source_input.definition.charter=Some(DefinitionCharter::Inline{source:maximum_source(&new)});
    let source_task=submit(&c,"diagnostic-source-submit",source_input).await;let source=effective(&c,&source_task).await.charter;
    let mut proposed=original.clone();proposed.definition.revision="limits-feedback-v4-charter-proposal".into();proposed.definition.charter=Some(DefinitionCharter::Reference{source:source.reference.clone()});
    let frozen=submit(&c,"diagnostic-preserved-override",proposed.clone()).await;let frozen=get(&c,&frozen.id).await;
    assert_eq!(frozen.input.definition.execution_profile,Some(ExecutionProfile::CollaborationV1{web_search:None}));
    assert_eq!(frozen.input.execution_limits().unwrap().native,historical.input.execution_limits().unwrap().native);
    let measured=validation(&frozen.input,&profile(&frozen.input));let exact=serde_json::to_vec(&measured).unwrap().len();assert_eq!(exact,19_768);assert!(key_nodes(&measured)<=4096);
    preflight(&frozen.input,Some(&source),&old,&dir.join("incompatible-native"),"native_json_byte_bound_native_schema_bytes",false).await;
    // A separate proposed future immutable revision, not UpdateDefinition, not
    // a global override and not a migration of either accepted original input.
    proposed.definition.revision="limits-feedback-v4-charter-sized-proposal".into();
    proposed.definition.limit_overrides.as_mut().unwrap().native.as_mut().unwrap().schema_bytes=exact;
    let future=submit(&c,"diagnostic-sized-proposal",proposed.clone()).await;let future=get(&c,&future.id).await;
    let mut expected=historical.input.execution_limits().unwrap().native.clone();expected.schema_bytes=exact;
    assert_eq!(future.input.execution_limits().unwrap().native,expected);
    assert_eq!(future.input.definition.execution_profile,historical.input.definition.execution_profile);
    preflight(&future.input,Some(&source),&old,&dir.join("future-native"),"native_api_key_unavailable",true).await;
    assert!(serde_json::from_value::<crate::limits::DefinitionLimitField>(json!("native.schema_bytes")).is_err());
    println!("DIAGNOSTIC_PROFILE {}",json!({"profile":diagnostic["profile_revision"],"original_schema_bytes":19746,"future_validation_bytes":exact,"delta":exact-19746,"validation_key_nodes":key_nodes(&measured),"execution_profile":"collaboration_v1","selected":false}));
    assert_eq!(get(&c,&historical.id).await,historical);assert_eq!(get(&c,&frozen.id).await,frozen);assert_eq!(get(&c,&future.id).await,future);
    drop(c);release(&db).await;
}

#[path = "charter_definition_admission_tests.rs"]
mod definition_admission;
