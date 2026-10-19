//! Actual consumer/stdio/HTTP joining with explicit synthetic controller replies.
//! Common Controller authority/CAS/reopen tests are in controller/charter_supplied.
//! No Codex binary, model provider or production authority is represented here.
use super::*;
use crate::charter::*;
use crate::limits::{RuntimeLimits, CharterLimits};
use crate::protocol::{Data, Operation, Reply, Request, MutationCommit};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, service::service_fn, Request as HttpRequest, Response as HttpResponse};
use hyper_util::rt::TokioIo;
use std::os::unix::fs::PermissionsExt;
use std::sync::{Mutex, atomic::{AtomicU64, Ordering}};
use tokio::sync::watch;

const THREAD: &str = "supplied-original-thread";
fn caller() -> crate::mcp::Caller {
    crate::mcp::Caller { task_id:"supplied-task".into(), attempt_id:"supplied-attempt".into(),
        generation:1, worker_id:"supplied-worker".into(), incarnation:"one".into() }
}
fn source(label: &str) -> ResolvedCharter {
    let sentence = format!("The agent MUST preserve {label}.");
    ResolvedCharter { reference:CharterSourceRef {definition_id:"supplied-agent".into(),definition_revision:label.into()},
        source:CharterSource {files:vec![CharterFile {file:"charter.md".into(),text:format!("# Purpose\n\n{sentence}\n")}],
            clauses:vec![CharterClauseRef {file:"charter.md".into(),section:"Purpose".into(),sentence}]} }
}
struct Peer {
    source: ResolvedCharter,
    pending: bool,
    intent: Option<CharterSuppliedContext>,
    outer: Option<CharterSuppliedTurn>,
    operations: Vec<Operation>,
    mode: String,
}
impl Peer {
    fn reply(&mut self, request: Request, root: &Path) -> Reply {
        let command = request.command;
        self.operations.push(command.clone());
        let original = source("original");
        let data = match &command {
            Operation::GetOperation {..} => return Reply::refused(request.request_id,"not_found","controlled original absent"),
            Operation::QueryCharter {query:CharterQuery::Session {session_id}} => {
                assert_eq!(session_id,"supplied-session");
                Data::CharterQuery(Box::new(CharterQueryResult::Session(Some(EffectiveCharter {
                    session:CharterSession {session_id:session_id.clone(),revision:1,
                        initial:original.reference.clone(),effective:original.reference.clone(),
                        pending:self.pending.then(||self.source.reference.clone()),last_turn_id:None,
                        compaction_due:None,compaction_in_flight:None},charter:original,
                }))))
            }
            Operation::QueryCharter {query:CharterQuery::Source {source}} => {
                assert_eq!(source,&self.source.reference);
                Data::CharterQuery(Box::new(CharterQueryResult::Source(self.source.clone())))
            }
            Operation::ObserveCharter {operation_id,caller:actual,observation:CharterObservation::BeginSuppliedContext {
                session_id,expected_revision,source:reference,request_id,developer_instructions }} => {
                assert_eq!(actual,&caller()); assert_eq!(*expected_revision,1);
                assert_eq!(reference,&self.source.reference);
                assert_eq!(developer_instructions,&supplied_context_instructions(&self.source).unwrap());
                assert!(self.intent.is_none(),"one actual original intent");
                if self.mode=="refuse_intent" { return Reply::refused(request.request_id,"stale_revision","controlled CAS refusal"); }
                let intent=CharterSuppliedContext {delivery_id:operation_id.clone(),session_id:session_id.clone(),
                    caller:actual.clone(),binding_revision:2,effective:self.source.clone(),request_id:*request_id,
                    developer_instructions:developer_instructions.clone(),acknowledgment:None};
                self.intent=Some(intent.clone());
                std::fs::write(root.join("intent-ack"),b"original acknowledged").unwrap();
                Data::Charter(Box::new(CharterOutcome::SuppliedContext(intent)))
            }
            Operation::ObserveCharter {operation_id,caller:actual,observation:CharterObservation::AckSuppliedContext {
                delivery_id,request_id,response }} => {
                assert_eq!(actual,&caller());
                let mut result=self.intent.clone().expect("original intent before ACK");
                assert_eq!(delivery_id,&result.delivery_id);assert_eq!(*request_id,result.request_id);
                let actual_response:Value=serde_json::from_slice(&std::fs::read(root.join("response.json")).unwrap()).unwrap();
                assert_eq!(response,&actual_response,"exact original result, not a synthesized model response");
                if self.mode=="refuse_ack" {return Reply::refused(request.request_id,"source_mismatch","controlled ACK refusal");}
                result.acknowledgment=Some(CharterSuppliedAcknowledgment {operation_id:operation_id.clone(),request_id:*request_id,
                    thread_id:THREAD.into(),response:response.clone()});
                std::fs::write(root.join("context-ack"),b"original result durably acknowledged").unwrap();
                self.intent=Some(result.clone());
                Data::Charter(Box::new(CharterOutcome::SuppliedContext(result)))
            }
            Operation::QueryCharter{query:CharterQuery::SuppliedContext{delivery_id}}=>{
                let v=self.intent.clone().unwrap();assert_eq!(delivery_id,&v.delivery_id);
                Data::CharterQuery(Box::new(CharterQueryResult::SuppliedContext(v)))
            }
            Operation::ObserveCharter{operation_id,caller,observation:CharterObservation::BeginSuppliedTurn{delivery_id,outer_index,request_id,thread_id,output_schema}}=>{
                let c=self.intent.as_ref().unwrap();assert!(c.acknowledgment.is_some());assert_eq!(output_schema,&supplied_result_schema());
                let v=CharterSuppliedTurn{boundary:CharterSuppliedBoundary{turn_id:operation_id.clone(),delivery_id:delivery_id.clone(),
                    session_id:c.session_id.clone(),caller:caller.clone(),outer_index:*outer_index,effective:c.effective.clone(),
                    request_id:*request_id,thread_id:thread_id.clone(),output_schema:output_schema.clone()},acknowledgment:None,reply:None,terminal:None};
                self.outer=Some(v.clone());Data::Charter(Box::new(CharterOutcome::SuppliedTurn(v)))
            }
            Operation::ObserveCharter{operation_id,observation:CharterObservation::AckSuppliedTurn{turn_id,request_id,response},..}=>{
                let v=self.outer.as_mut().unwrap();assert_eq!(turn_id,&v.boundary.turn_id);
                v.acknowledgment=Some(CharterSuppliedTurnAck{operation_id:operation_id.clone(),request_id:*request_id,turn_id:response["turn"]["id"].as_str().unwrap().into(),response:response.clone()});
                Data::Charter(Box::new(CharterOutcome::SuppliedTurn(v.clone())))
            }
            Operation::ObserveCharter{observation:CharterObservation::OutputChunk{chunk},..}=>{
                let bytes:Vec<u8>=chunk.data.as_bytes().chunks_exact(2).map(|p|u8::from_str_radix(std::str::from_utf8(p).unwrap(),16).unwrap()).collect();
                let sha=aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256,&bytes).as_ref().iter().map(|b|format!("{b:02x}")).collect();
                Data::Charter(Box::new(CharterOutcome::OutputChunk(CharterOutputChunkReceipt{reference:chunk.reference.clone(),index:chunk.index,sha256:sha,bytes:bytes.len() as u32})))
            }
            Operation::ObserveCharter{operation_id,observation:CharterObservation::SuppliedReply{turn_id,reply,..},..}=>{
                assert!(reply.declaration.is_none(),"this original stub has no final declaration");
                Data::Charter(Box::new(CharterOutcome::Reply(CharterReplyRecord{reply_id:operation_id.clone(),turn_id:turn_id.clone(),
                    effective:self.source.reference.clone(),observation:reply.clone(),audit:CharterAudit{findings:vec![CharterAuditFinding::MissingMetadata]}})))
            }
            other=>panic!("unexpected controlled operation {other:?}"),
        };
        let mut reply=Reply::ok(request.request_id,data);
        reply.observed_controller_revision=Some(17);
        if let Some(id)=command.mutation_id() {reply.commit=Some(MutationCommit {operation_id:id.into(),controller_revision:17});}
        reply.validate_for(&command).unwrap();reply
    }
}
struct Fixture {
    root:PathBuf,binary:PathBuf,runtime:Arc<RuntimeLimits>,scope:InitialCharterScope,
    peer:Arc<Mutex<Peer>>,server:tokio::task::JoinHandle<()>,
}
impl Fixture {
    async fn new(mode:&str,pending:bool)->Self {
        static NEXT:AtomicU64=AtomicU64::new(1);
        let root=std::env::temp_dir().join(format!("borg-codex-charter-{}-{}-{}",std::process::id(),process::now_ms(),NEXT.fetch_add(1,Ordering::Relaxed)));
        process::private_directory(&root).unwrap();
        let mut runtime=(*crate::worker::test_limits()).clone();
        runtime.charter=Some(CharterLimits {record_bytes:262144,source_bytes:65536,files:8,clauses:32,
            metadata_bytes:16384,references:32,reply_items:32,tool_calls:8,reference_depth:8,
            records:1024,page_items:8,page_bytes:65536});
        runtime.validate().unwrap();let runtime=Arc::new(runtime);
        crate::worker::handoff::initialize_state(&root.join("state"),&runtime.worker).unwrap();
        let binary=root.join("executor.py");std::fs::write(&binary,EXECUTOR).unwrap();
        std::fs::set_permissions(&binary,std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::write(root.join("mode"),mode).unwrap();
        let peer=Arc::new(Mutex::new(Peer {source:source(if pending {"pending"} else {"original"}),pending,
            intent:None,outer:None,operations:Vec::new(),mode:mode.into()}));
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url=format!("http://{}",listener.local_addr().unwrap());
        let p=peer.clone();let r=root.clone();
        let server=tokio::spawn(async move {
            let mut connections=tokio::task::JoinSet::new();
            loop {
                let (stream,_)=listener.accept().await.unwrap();let p=p.clone();let r=r.clone();
                connections.spawn(async move {
                    let service=service_fn(move |request:HttpRequest<Incoming>| {
                        let p=p.clone();let r=r.clone();async move {
                            let request:Request=serde_json::from_slice(&request.into_body().collect().await.map_err(std::io::Error::other)?.to_bytes()).unwrap();
                            let reply=p.lock().unwrap().reply(request,&r);
                            Ok::<_,std::io::Error>(HttpResponse::new(Full::new(Bytes::from(serde_json::to_vec(&reply).unwrap()))))
                        }
                    });
                    let _=hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream),service).await;
                });
            }
        });
        let worker_state=root.join("state");
        Self {root,binary,runtime,scope:InitialCharterScope {worker_state,controller_url:url,caller:caller(),session_id:"supplied-session".into()},peer,server}
    }
    fn control(&self)->(watch::Sender<bool>,Control) {
        let (send,cancel)=watch::channel(false);let worker_limits: Arc<crate::limits::WorkerLimits>=Arc::from(self.runtime.worker.clone());
        (send,Control {deadline_ms:process::now_ms()+30_000,cancel,worker_limits:worker_limits.clone(),runtime_limits:self.runtime.clone(),
            validation_runtime:Arc::new(crate::native::ValidationRuntime::new(worker_limits).unwrap())})
    }
    fn state(&self)->PathBuf {self.root.join("state")}
    async fn session(&self)->Session {
        Session::start_with_auth(&self.binary,&self.root,&self.state(),&self.root.join("processes"),
            &InferenceSettings::selected(),&self.runtime.execution,Arc::from(self.runtime.worker.clone()),None).await.unwrap()
    }
    fn config(&self)->Value {json!({"model":crate::protocol::SELECTED_MODEL,"cwd":self.root,
        "approvalPolicy":"never","sandbox":"workspace-write","ephemeral":false,
        "baseInstructions":"Existing base instructions stay intact.",
        "config":{"model_reasoning_effort":crate::protocol::SELECTED_REASONING_EFFORT,
            "mcp_servers":{"controlled":{"command":"existing-borg-bridge","args":["unchanged"]}}}})}
    fn requests(&self)->Vec<Value> {
        match std::fs::read_to_string(self.root.join("requests.jsonl")) {Ok(s)=>s.lines().map(|line|serde_json::from_str(line).unwrap()).collect(),Err(_)=>Vec::new()}
    }
    fn count(&self,method:&str)->usize {self.requests().iter().filter(|r|r["method"]==method).count()}
    fn ack_count(&self)->usize {self.peer.lock().unwrap().operations.iter().filter(|o|matches!(o,Operation::ObserveCharter {observation:CharterObservation::AckSuppliedContext {..},..})).count()}
    fn closed(&self) {assert!(std::fs::read_dir(self.root.join("processes")).unwrap().all(|e|e.unwrap().path().extension().is_none_or(|s|s!="json")));}
}
impl Drop for Fixture {fn drop(&mut self){self.server.abort();let _=std::fs::remove_dir_all(&self.root);}}
const EXECUTOR:&str=r#"#!/usr/bin/python3
import json,pathlib,sys,time
root=pathlib.Path.cwd();mode=(root/'mode').read_text()
def emit(value): print(json.dumps(value,separators=(',',':')),flush=True)
for line in sys.stdin:
    request=json.loads(line)
    with (root/'requests.jsonl').open('a') as f:f.write(json.dumps(request,separators=(',',':'))+'\n')
    if 'id' not in request:continue
    method=request.get('method')
    if method=='thread/start':
        if mode!='legacy':
            assert (root/'intent-ack').exists()
            marker=json.loads((root/'state/supplied-charter/dispatch.json').read_text())
            assert marker['request_id']==request['id'] and marker['thread_id'] is None
        (root/'start-observed').write_text('actual stdio start observed')
        if mode in ('eof','cancel','malformed'):
            while not (root/'release').exists():time.sleep(0.005)
            if mode=='eof':sys.exit(0)
        result={'thread':{'id':'supplied-original-thread','items':[]},'model':request['params']['model'],'reasoningEffort':request['params']['config']['model_reasoning_effort'],'controlled_extra':{'preserve':'exact result'}}
        if mode=='malformed':result['thread']={}
        (root/'response.json').write_text(json.dumps(result))
    elif method=='turn/start':
        if mode!='legacy':assert (root/'context-ack').exists(), 'no first turn before canonical charter ACK'
        emit({'id':request['id'],'result':{'turn':{'id':'supplied-turn'}}})
        emit({'method':'turn/completed','params':{'threadId':'supplied-original-thread','turn':{'id':'supplied-turn','status':'completed'}}})
        continue
    elif method=='command/exec':result={'exitCode':0,'stdout':'','stderr':''}
    else:result={}
    emit({'id':request['id'],'result':result})
"#;
async fn observed(root:&Path) {
    tokio::time::timeout(Duration::from_secs(5),async {while !root.join("start-observed").exists(){tokio::time::sleep(Duration::from_millis(5)).await;}}).await.unwrap();
}

#[tokio::test]
async fn initial_charter_exact_source_pending_and_executor_settings_are_joined_before_ack() {
    let f=Fixture::new("success",true).await;let (_cancel,c)=f.control();let mut s=f.session().await;
    let before=f.config();let result=charter_delivery::start_thread(&mut s,&f.scope,&f.state(),before.clone(),&InferenceSettings::selected(),&c,&[]).await.unwrap();
    assert_eq!(result,serde_json::from_slice::<Value>(&std::fs::read(f.root.join("response.json")).unwrap()).unwrap());
    let request=f.requests().into_iter().find(|r|r["method"]=="thread/start").unwrap();
    let mut expected=before;expected["developerInstructions"]=json!(supplied_context_instructions(&source("pending")).unwrap());
    assert_eq!(request["params"],expected);assert_eq!(f.ack_count(),1);assert_eq!(f.count("turn/start"),0);
    assert!(f.peer.lock().unwrap().operations.iter().all(|o|!matches!(o,Operation::ObserveWorker {..})));
    s.shutdown().await.unwrap();f.closed();
}

#[tokio::test]
async fn full_coding_consumer_acknowledges_initial_charter_before_its_first_turn() {
    let f=Fixture::new("success",false).await;let (_cancel,c)=f.control();let (events,_rx)=mpsc::channel(16);
    let result=execute_with_credentials_collaboration_and_charter(&f.binary,&f.root,&f.state(),&f.root.join("processes"),
        &InferenceSettings::selected(),&f.runtime.execution,"Original task only.",&c,events,&[],None,None,None,Some(&f.scope)).await.unwrap();
    assert!(result.success);assert_eq!(f.count("thread/start"),1);assert_eq!(f.count("turn/start"),1);assert_eq!(f.ack_count(),1);
    let request=f.requests().into_iter().find(|r|r["method"]=="turn/start").unwrap();
    assert_eq!(request["params"]["input"],json!([{"type":"text","text":"Original task only."},{"type":"text","text":supplied_result_instructions()}]));assert_eq!(request["params"]["outputSchema"],supplied_result_schema());f.closed();
}

async fn uncertain(mode:&str) {
    let f=Fixture::new(mode,false).await;let (cancel,c)=f.control();let mut s=f.session().await;
    let state=f.state();let config=f.config();let inference=InferenceSettings::selected();
    let mut start=Box::pin(charter_delivery::start_thread(&mut s,&f.scope,&state,config,&inference,&c,&[]));
    tokio::select! {r=&mut start=>panic!("start returned before controlled reply {r:?}"),_=observed(&f.root)=>{}}
    assert_eq!(f.ack_count(),0);
    if mode=="cancel"{cancel.send(true).unwrap();}else{std::fs::write(f.root.join("release"),b"release").unwrap();}
    assert!(start.await.unwrap_err().contains("uncertain"));s.shutdown().await.unwrap();
    let original=std::fs::read(f.state().join("supplied-charter/dispatch.json")).unwrap();
    let (_cancel,fresh)=f.control();let mut s=f.session().await;
    let error=charter_delivery::start_thread(&mut s,&f.scope,&f.state(),f.config(),&inference,&fresh,&[]).await.unwrap_err();
    assert!(error.contains("no second thread/start"));assert_eq!(f.count("thread/start"),1);assert_eq!(f.count("turn/start"),0);assert_eq!(f.ack_count(),0);
    assert_eq!(std::fs::read(f.state().join("supplied-charter/dispatch.json")).unwrap(),original);s.shutdown().await.unwrap();f.closed();
}
#[tokio::test] async fn unknown_initial_response_never_resends(){uncertain("eof").await;}
#[tokio::test] async fn cancellation_after_actual_send_never_resends(){uncertain("cancel").await;}
#[tokio::test] async fn malformed_initial_result_never_becomes_an_ack_or_second_start(){uncertain("malformed").await;}

#[tokio::test]
async fn canonical_ack_refusal_retains_original_thread_and_withholds_first_turn() {
    let f=Fixture::new("refuse_ack",false).await;let (_cancel,c)=f.control();let (events,_rx)=mpsc::channel(16);
    let error=execute_with_credentials_collaboration_and_charter(&f.binary,&f.root,&f.state(),&f.root.join("processes"),
        &InferenceSettings::selected(),&f.runtime.execution,"Task.",&c,events,&[],None,None,None,Some(&f.scope)).await.unwrap_err();
    assert!(error.message.contains("uncertain"));assert_eq!(f.count("thread/start"),1);assert_eq!(f.count("turn/start"),0);assert_eq!(f.ack_count(),1);
    assert!(!f.root.join("context-ack").exists());f.closed();
}

#[tokio::test]
async fn source_credential_frame_and_cas_refusals_happen_before_executor_send() {
    for mode in ["credential","bound","refuse_intent"] {
        let f=Fixture::new(mode,false).await;let (_cancel,c)=f.control();let mut s=f.session().await;let mut config=f.config();
        let credentials=if mode=="credential"{vec!["The agent MUST preserve original.".to_owned()]}else{vec![]};
        if mode=="bound"{config["preserved"]=json!("x".repeat(f.runtime.execution.coding.frame_bytes));}
        assert!(charter_delivery::start_thread(&mut s,&f.scope,&f.state(),config,&InferenceSettings::selected(),&c,&credentials).await.is_err());
        assert_eq!(f.count("thread/start"),0);assert_eq!(f.ack_count(),0);assert!(!f.state().join("supplied-charter/dispatch.json").exists());
        s.shutdown().await.unwrap();f.closed();
    }
}

#[tokio::test]
async fn absent_charter_preserves_legacy_thread_and_task_fields() {
    let f=Fixture::new("legacy",false).await;let (_cancel,c)=f.control();let (events,_rx)=mpsc::channel(16);
    let result=execute_with_credentials_collaboration_and_charter(&f.binary,&f.root,&f.state(),&f.root.join("processes"),
        &InferenceSettings::selected(),&f.runtime.execution,"Original task only.",&c,events,&[],None,None,None,None).await.unwrap();
    assert!(result.success);assert!(f.peer.lock().unwrap().operations.is_empty());
    let request=f.requests().into_iter().find(|r|r["method"]=="thread/start").unwrap();
    let mut expected=json!({"model":crate::protocol::SELECTED_MODEL,"cwd":f.root,"approvalPolicy":"never",
        "sandbox":"workspace-write","ephemeral":false,"config":{"model_reasoning_effort":crate::protocol::SELECTED_REASONING_EFFORT}});
    context_config(&mut expected,&f.runtime.execution.coding);assert_eq!(request["params"],expected);
    assert!(!f.state().join("supplied-charter").exists());f.closed();
}

#[path = "codex_charter_admission_tests.rs"]
mod admission_join;
