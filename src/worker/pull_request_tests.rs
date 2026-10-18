//! Real controller receipts over a bounded loopback fixture, with controlled
//! provider facts. No fixture contacts GitHub, invokes Git, or claims a provider
//! performed the represented effect. HTTP provider discrimination is separate.
use super::*;
use crate::charter::{CharterClauseRef, CharterFile, CharterSource, DefinitionCharter};
use crate::controller::{Controller, ControllerConfig};
use crate::worker::mutations::fresh_id;
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{Request as HttpRequest, Response, body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;
use std::sync::Mutex;

fn selected_limits() -> Arc<RuntimeLimits> {
    // Exact selected metadata, not a runtime default or a relaxed test policy.
    const PATH: &str = "/home/dev-user/borg-runtime/artifacts/v2/parallel-integration-o1p7h0uc/deployment-metadata-candidate-01/readiness-06/default-selection-saved-01/controller-limits.SELECTED.json";
    let bytes = std::fs::read(PATH).expect("explicit retained selected metadata fixture");
    let hash: String = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &bytes).as_ref().iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(hash, "b820356ea1eaef6fb61ac1aa212a1b33cdfa0699a7bc6b230898480fdcc65c76");
    Arc::new(serde_json::from_slice(&bytes).unwrap())
}
fn target() -> contract::Target {
    contract::Target { revision:"fixture-target-r1".into(), api_base:"https://api.github.com".into(),
        owner:"fixture-owner".into(), repository:"fixture-repository".into(), repository_id:17,
        head_remote:"https://github.com/fixture-owner/fixture-repository.git".into(), base_branch:"main".into(), credential_name:"GITHUB_TOKEN".into() }
}
fn data(reply: Reply) -> Data {
    match reply.outcome { Outcome::Ok{data} => data, other => panic!("expected original controller success: {other:?}") }
}
async fn direct(c: &Controller, command: Operation) -> Reply {
    Box::pin(c.handle(Request{protocol_revision:WIRE_REVISION,request_id:fresh_id("pr-fixture-request").unwrap(),command})).await
}
fn effect_record(reply: Reply) -> OperationRecord {
    match data(reply) { Data::Operation(r)=>r, other=>panic!("expected effect: {other:?}") }
}
#[derive(Default)]
struct Probe { seen:Vec<Operation>, drop_prepare:Option<String>, dropped:Option<Reply>, deny_lookup:Option<String>,
    drop_final:Option<String>, dropped_final:Option<(Report,Reply)> }
struct Fixture {
    state:PathBuf, command:WorkerCommand, phase:Phase, controller:Controller,
    client:ApiClient, config:WorkerConfig, limits:Arc<RuntimeLimits>, probe:Arc<Mutex<Probe>>,
    server:tokio::task::JoinHandle<()>, _lease:process::WorkerLease,
}
impl Drop for Fixture { fn drop(&mut self) { self.server.abort(); } }
impl Fixture {
    async fn new() -> Self {
        Self::with_output_limit(4096,false).await
    }
    async fn with_output_limit(output_bytes:usize, publication:bool) -> Self {
        let limits=selected_limits();
        let root=std::env::temp_dir().join(fresh_id("borg-pr-worker").unwrap());
        process::private_directory(&root).unwrap();
        let state=root.join("worker"); handoff::initialize_state(&state,&limits.worker).unwrap();
        for name in ["commands","acks","outbox","processes","stops","mutations/settlements","delegations","native-tracking","pull-requests"] {
            process::private_directory(&state.join(name)).unwrap();
        }
        let lease=process::WorkerLease::claim(&state.join("worker-owner.json"),&limits.worker).unwrap();
        let controller=Controller::open_with_config(root.join("controller.sqlite"),ControllerConfig {
            limits:limits.clone(), clock:Arc::new(||100), ..ControllerConfig::default()
        }).unwrap();
        let registration=crate::repository_catalogue::Registration { repository_id:"fixture-source".into(),registration_revision:"r1".into(),
            display_name:"Controlled PR fixture".into(),source_endpoint:"/fixture/source.git".into(),checkpoint_endpoint:"/fixture/checkpoint.git".into(),
            source_store_key:"source.git".into(),checkpoint_store_key:"checkpoint.git".into(),pull_request_target:Some(target()) };
        data(direct(&controller,Operation::RegisterRepository{operation_id:"register-source".into(),registration:registration.clone()}).await);
        let sentence="The agent MUST preserve original operation identities.";
        let charter=DefinitionCharter::Inline { source:CharterSource {
            files:vec![CharterFile{file:"charter.md".into(),text:format!("# Purpose\n\n{sentence}\n")}],
            clauses:vec![CharterClauseRef{file:"charter.md".into(),section:"Purpose".into(),sentence:sentence.into()}],
        }};
        let input=TaskInput { instruction_source: None, pull_request:Some(contract::Request{title:"Controlled change".into(),body:"Original requested body".into()}),resolved_limits:None,repository_read:None,
            instruction:"Controlled coding execution".into(),definition:Definition{id:"fixture-agent".into(),revision:"r1".into(),executor:ExecutorKind::Coding,
                inference:Some(InferenceSettings::selected()),execution_profile:Some(if publication {ExecutionProfile::CollaborationV2{web_search:None,publication:Some(crate::mcp::publication::Selection::V1{
                    repository_id:registration.repository_id.clone(),registration_revision:registration.registration_revision.clone(),target_revision:target().revision,
                    actions:vec![crate::mcp::publication::Action::PublishBranch],branch_namespace:"borg/mcp-fixture".into()})}}else{ExecutionProfile::CollaborationV1{web_search:None}}),charter:Some(charter),limit_overrides:None},
            repository:Some(Repository{registration:Some(registration.binding()),url:String::new(),checkpoint_remote:String::new(),start_commit:"a".repeat(40),pull_request_target:None}),
            acceptance_commands:vec![vec!["controlled-check".into()]],output_schema:None,
            limits:Limits{deadline_unix_ms:10_000,model_requests:2,output_bytes,result_bytes:limits.task_defaults.result_bytes,max_delegation_depth:1},dependencies:vec![],native_workflow:None };
        data(direct(&controller,Operation::SubmitTask{operation_id:"submit".into(),input}).await);
        data(direct(&controller,Operation::RegisterWorker{operation_id:"register-worker".into(),worker:Worker{id:"worker".into(),incarnation:"one".into(),coding_capacity:1,native_capacity:0}}).await);
        let Data::Commands(commands)=data(direct(&controller,Operation::AwaitCommands{operation_id:"launch".into(),worker_id:"worker".into(),incarnation:"one".into(),acknowledged:vec![]}).await) else {panic!("commands")};
        let command=commands.into_iter().find(|v|matches!(v,WorkerCommand::RunAttempt{..})).unwrap();
        let (attempt,input,_)=context(&command).unwrap();
        let report=|id:&str,observation|Operation::ObserveWorker{report:Report{report_id:id.into(),worker_id:attempt.worker_id.clone(),incarnation:attempt.worker_incarnation.clone(),
            task_id:attempt.task_id.clone(),attempt_id:attempt.id.clone(),generation:attempt.generation,observation}};
        data(direct(&controller,report("started",Observation::Started{thread_id:Some("fixture-thread".into()),turn_id:Some("fixture-turn".into())})).await);
        data(direct(&controller,report("executor",Observation::ExecutorOutcome{success:true,summary:"Controlled executor result".into()})).await);
        let cp=git::checkpoint(input.repository.as_ref().unwrap(),&attempt.task_id,&attempt.id,"c".repeat(40)).unwrap();
        effect_record(direct(&controller,Operation::PrepareOperation{operation_id:cp.operation_id.clone(),task_id:attempt.task_id.clone(),attempt_id:attempt.id.clone(),generation:attempt.generation,kind:"checkpoint".into(),
            intended:json!({"registration":cp.registration,"repository":cp.repository,"remote_ref":cp.remote_ref,"commit":cp.commit})}).await);
        effect_record(direct(&controller,Operation::SettleOperation{settlement_id:"checkpoint-settlement".into(),operation_id:cp.operation_id.clone(),task_id:attempt.task_id.clone(),attempt_id:attempt.id.clone(),generation:attempt.generation,
            outcome:json!({"acknowledged":true}),checkpoint:Some(cp.clone())}).await);
        let b=Bounds::from_limits(&limits);
        let head=contract::prepare_head(&target(),input.repository.as_ref().unwrap(),attempt,&cp,&format!("pr-create-{}",digest(&(attempt.id.as_str(),attempt.generation)).unwrap()),&b).unwrap();
        let phase=Phase{revision:1,command_sha256:digest(&command).unwrap(),head_effect:Effect::new(contract::head_record(&head,&b).unwrap()).unwrap(),
            final_report_id:format!("pr-result-{}",digest(&head.binding.operation_id).unwrap()),head,create:None,create_effect:None,
            candidate:Observation::ResultCandidate{checks_passed:true,summary:"Controlled checked result".into(),checkpoint:Some(cp),validation:vec!["controlled check".into()],output:None},
            final_observation:None,final_report:None,final_commit:None,retired:false};
        record_launch(&state.join("commands/launch.json"),&command,&limits).unwrap();
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url=format!("http://{}",listener.local_addr().unwrap());
        let probe=Arc::new(Mutex::new(Probe::default()));
        let (owner,captured,retained_state,retained_command,body_bound)=(controller.clone(),probe.clone(),state.clone(),command.clone(),limits.transport.body_bytes);
        let server=tokio::spawn(async move {
            let mut connections=tokio::task::JoinSet::new();
            loop {
                let (stream,_)=listener.accept().await.unwrap();
                let (owner,captured,state,command)=(owner.clone(),captured.clone(),retained_state.clone(),retained_command.clone());
                connections.spawn(async move {
                    let service=service_fn(move |request:HttpRequest<Incoming>| {
                        let (owner,captured,state,command)=(owner.clone(),captured.clone(),state.clone(),command.clone());
                        async move {
                            let bytes=Limited::new(request.into_body(),body_bound).collect().await.map_err(std::io::Error::other)?.to_bytes();
                            let request:Request=serde_json::from_slice(&bytes).unwrap();
                            let (drop_ack,drop_final)={
                                let mut probe=captured.lock().unwrap();
                                assert!(probe.seen.len()<128,"bounded fixture request inventory");
                                probe.seen.push(request.command.clone());
                                if matches!(&request.command,Operation::GetOperation{operation_id} if probe.deny_lookup.as_ref()==Some(operation_id)) {
                                    return Err(std::io::Error::other("controlled original lookup unavailable"));
                                }
                                let selected=matches!(&request.command,Operation::PrepareOperation{operation_id,..} if probe.drop_prepare.as_ref()==Some(operation_id));
                                if selected {
                                    let phase=read(&state,&command).unwrap().expect("phase must exist before controller Prepare");
                                    assert_eq!(serde_json::to_value(phase.head_effect.request()).unwrap(),serde_json::to_value(&request.command).unwrap());
                                    assert!(phase.head_effect.prepare_commit.is_none());
                                    probe.drop_prepare=None;
                                }
                                let final_report=match &request.command {
                                    Operation::ObserveWorker{report} if probe.drop_final.as_ref()==Some(&report.report_id)=>{
                                        let phase=read(&state,&command).unwrap().expect("normalized phase precedes final controller write");
                                        assert_eq!(phase.final_report.as_ref(),Some(report));
                                        assert!(phase.final_commit.is_none() && !phase.retired);
                                        probe.drop_final=None;
                                        Some(report.clone())
                                    },_=>None,
                                };
                                (selected,final_report)
                            };
                            let reply=Box::pin(owner.handle(request)).await;
                            if drop_ack { captured.lock().unwrap().dropped=Some(reply); return Err(std::io::Error::other("controlled loss after real Prepare commit")); }
                            if let Some(report)=drop_final {
                                let mut probe=captured.lock().unwrap();probe.deny_lookup=Some(report.report_id.clone());probe.dropped_final=Some((report,reply));
                                return Err(std::io::Error::other("controlled loss after real normalized final commit"));
                            }
                            Ok::<_,std::io::Error>(Response::new(Full::new(Bytes::from(serde_json::to_vec(&reply).unwrap()))))
                        }
                    });
                    let _=hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream),service).await;
                });
            }
        });
        let config=WorkerConfig{limits:limits.clone(),resource_profile:None,controller_url:url.clone(),id:"worker".into(),incarnation:"one".into(),workspace_root:state.clone(),codex_path:"/fixture/not-executed".into(),
            coding_capacity:1,native_capacity:0,native_endpoint:"http://127.0.0.1:1".into(),validation_executable:std::env::current_exe().unwrap()};
        Self{state,command,phase,controller,client:ApiClient::new(&url,limits.clone()).unwrap(),config,limits,probe,server,_lease:lease}
    }
    fn control(&self)->(Control,watch::Sender<bool>) {
        let (sender,cancel)=watch::channel(false);
        (Control{deadline_ms:process::now_ms()+60_000,cancel,runtime_limits:self.limits.clone(),worker_limits:Arc::new(self.limits.worker.clone()),
            validation_runtime:Arc::new(crate::native::ValidationRuntime::new(Arc::new(self.limits.worker.clone())).unwrap())},sender)
    }
    fn completed(&self) {
        let path=self.state.join("commands/launch.json");
        let mut receipt=read_receipt(&path,&self.limits).unwrap();receipt.completed=true;receipt.effects_started=true;atomic_json(&path,&receipt).unwrap();
    }
    async fn recover(&self) -> Result<(),String> {
        recover(&self.config,&self.state,&None,&Arc::new(tokio::sync::Mutex::new(())),
            &Arc::new(crate::native::ValidationRuntime::new(Arc::new(self.limits.worker.clone())).unwrap())).await
    }
    async fn current(&self) -> Task {
        match data(direct(&self.controller,Operation::GetTask{task_id:context(&self.command).unwrap().0.task_id.clone()}).await) {
            Data::Task(task)=>*task,other=>panic!("task: {other:?}")
        }
    }
    async fn commit_create(&self,phase:&mut Phase) {
        let (control,_sender)=self.control();phase.save(&self.state,&self.command).unwrap();
        prepare_effect(&self.client,&mut phase.head_effect,&control).await.unwrap();phase.head_effect.may_have_started=true;
        phase.head_effect.outcome=Some(head_outcome(&phase.head).unwrap());phase.save(&self.state,&self.command).unwrap();
        settle_effect(&self.client,&self.state,&mut phase.head_effect,&control).await.unwrap();
        let (_,input,_)=context(&self.command).unwrap();let b=Bounds::from_limits(&self.limits);
        let intent=contract::prepare_pull_request(&phase.head,phase.head_effect.settled.as_ref().unwrap(),input.pull_request.as_ref().unwrap(),&b).unwrap();
        phase.create_effect=Some(Effect::new(contract::intent_record(&intent,&b).unwrap()).unwrap());phase.create=Some(intent.clone());phase.save(&self.state,&self.command).unwrap();
        prepare_effect(&self.client,phase.create_effect.as_mut().unwrap(),&control).await.unwrap();phase.create_effect.as_mut().unwrap().may_have_started=true;
        let target=&intent.head.target;let branch=|name:String,commit:String|contract::BranchProof{repository_id:target.repository_id,owner:target.owner.clone(),repository:target.repository.clone(),branch:name,commit};
        let proof=contract::Proof{operation_id:intent.head.binding.operation_id.clone(),provider_id:111,number:23,url:format!("https://github.com/{}/{}/pull/23",target.owner,target.repository),
            head:branch(intent.head.head_ref.strip_prefix("refs/heads/").unwrap().into(),intent.head.binding.checkpoint.commit.clone()),base:branch(target.base_branch.clone(),"d".repeat(40)),title:intent.title.clone(),body:intent.body.clone(),marker:intent.marker.clone()};
        phase.create_effect.as_mut().unwrap().outcome=Some(json!(contract::Settlement{acknowledged:true,proof}));phase.save(&self.state,&self.command).unwrap();
        settle_effect(&self.client,&self.state,phase.create_effect.as_mut().unwrap(),&control).await.unwrap();phase.save(&self.state,&self.command).unwrap();
    }
}

#[tokio::test]
async fn phase_precedes_real_prepare_ack_loss_and_completed_marker_does_not_hide_negative_recovery() {
    let f=Fixture::new().await;let (control,_sender)=f.control();let mut phase=f.phase.clone();
    phase.save(&f.state,&f.command).unwrap();
    f.probe.lock().unwrap().drop_prepare=Some(phase.head_effect.intended.id.clone());
    assert!(prepare_effect(&f.client,&mut phase.head_effect,&control).await.is_err());
    let committed=f.probe.lock().unwrap().dropped.clone().expect("real original commit reached before ACK cut");
    assert!(committed.commit.is_some());
    assert!(read(&f.state,&f.command).unwrap().unwrap().head_effect.prepare_commit.is_none());
    f.completed();f.recover().await.unwrap();
    let ended=read(&f.state,&f.command).unwrap().unwrap();
    assert_eq!(ended.head_effect.prepare_commit,committed.commit);
    assert_eq!(ended.head_effect.outcome,Some(json!({"acknowledged":true,"not_applied":true})));
    assert!(!ended.head_effect.may_have_started);assert!(ended.create.is_none());assert!(ended.retired);
    assert_eq!(f.current().await.status,TaskStatus::Failed);assert!(f.current().await.result.is_none());
    assert!(read_receipt(&f.state.join("commands/launch.json"),&f.limits).unwrap().completed);
    assert!(cleanup_verified(&f.client,&f.state,&f.command).await.unwrap());
    let probe=f.probe.lock().unwrap();
    assert!(probe.seen.iter().all(|op|!matches!(op,Operation::PrepareOperation{kind,..} if kind==contract::CREATE_KIND)));
    assert!(probe.seen.iter().filter_map(|op|match op{Operation::PrepareOperation{operation_id,..}=>Some(operation_id),_=>None}).all(|id|id==&ended.head_effect.intended.id));
}

#[tokio::test]
async fn possible_send_with_unavailable_original_lookup_keeps_unknown_and_never_settles_or_finishes() {
    let f=Fixture::new().await;let (control,_sender)=f.control();let mut phase=f.phase.clone();phase.save(&f.state,&f.command).unwrap();
    prepare_effect(&f.client,&mut phase.head_effect,&control).await.unwrap();
    phase.head_effect.may_have_started=true;phase.save(&f.state,&f.command).unwrap();f.completed();
    f.probe.lock().unwrap().deny_lookup=Some(phase.head_effect.intended.id.clone());
    let before=std::fs::read(path(&f.state,context(&f.command).unwrap().0)).unwrap();
    assert!(f.recover().await.is_err());
    assert_eq!(std::fs::read(path(&f.state,context(&f.command).unwrap().0)).unwrap(),before);
    assert!(!read_receipt(&f.state.join("commands/launch.json"),&f.limits).unwrap().completed);
    assert!(!cleanup_ready(&f.state,&f.command).unwrap());
    let probe=f.probe.lock().unwrap();
    assert!(probe.seen.iter().all(|op|!matches!(op,Operation::SettleOperation{..}|Operation::ObserveWorker{..})));
    let mut false_negative=phase.clone();false_negative.head_effect.outcome=Some(json!({"acknowledged":true,"not_applied":true}));
    assert!(false_negative.validate(&f.command).is_err(),"possible send never becomes proven not-applied");
}

#[tokio::test]
async fn canonical_settled_create_survives_lost_local_ack_and_completed_launch_to_exact_result_and_cleanup() {
    let f=Fixture::new().await;let (control,_sender)=f.control();let mut phase=f.phase.clone();phase.save(&f.state,&f.command).unwrap();
    prepare_effect(&f.client,&mut phase.head_effect,&control).await.unwrap();phase.head_effect.may_have_started=true;
    phase.head_effect.outcome=Some(head_outcome(&phase.head).unwrap());phase.save(&f.state,&f.command).unwrap();
    settle_effect(&f.client,&f.state,&mut phase.head_effect,&control).await.unwrap();
    let (_,input,_)=context(&f.command).unwrap();let b=Bounds::from_limits(&f.limits);
    let intent=contract::prepare_pull_request(&phase.head,phase.head_effect.settled.as_ref().unwrap(),input.pull_request.as_ref().unwrap(),&b).unwrap();
    phase.create_effect=Some(Effect::new(contract::intent_record(&intent,&b).unwrap()).unwrap());phase.create=Some(intent.clone());phase.save(&f.state,&f.command).unwrap();
    prepare_effect(&f.client,phase.create_effect.as_mut().unwrap(),&control).await.unwrap();phase.create_effect.as_mut().unwrap().may_have_started=true;phase.save(&f.state,&f.command).unwrap();
    let target=&intent.head.target;let branch=|name:String,commit:String|contract::BranchProof{repository_id:target.repository_id,owner:target.owner.clone(),repository:target.repository.clone(),branch:name,commit};
    let proof=contract::Proof{operation_id:intent.head.binding.operation_id.clone(),provider_id:111,number:23,url:format!("https://github.com/{}/{}/pull/23",target.owner,target.repository),
        head:branch(intent.head.head_ref.strip_prefix("refs/heads/").unwrap().into(),intent.head.binding.checkpoint.commit.clone()),base:branch(target.base_branch.clone(),"d".repeat(40)),
        title:intent.title.clone(),body:intent.body.clone(),marker:intent.marker.clone()};
    let expected=contract::validate_proof(&intent,&proof,&b).unwrap();
    let mut committed=phase.create_effect.as_ref().unwrap().clone();committed.outcome=Some(json!(contract::Settlement{acknowledged:true,proof}));
    let original=direct(&f.controller,committed.settlement().unwrap()).await;effect_record(original.clone());
    assert!(read(&f.state,&f.command).unwrap().unwrap().create_effect.unwrap().outcome.is_none(),"canonical commit outlives local ACK");
    f.completed();f.recover().await.unwrap();
    let ended=read(&f.state,&f.command).unwrap().unwrap();assert!(ended.retired);
    assert_eq!(ended.create_effect.as_ref().unwrap().settled,Some(effect_record(original.clone())));
    let current=f.current().await;assert_eq!(current.status,TaskStatus::Succeeded);
    assert_eq!(current.result.as_ref().unwrap().pull_request,Some(expected));
    assert_eq!(current.result.as_ref().unwrap().checkpoint,Some(intent.head.binding.checkpoint));
    assert!(cleanup_verified(&f.client,&f.state,&f.command).await.unwrap());
    let replay=direct(&f.controller,committed.settlement().unwrap()).await;
    assert_eq!(replay.outcome,original.outcome);assert_eq!(replay.commit,original.commit);
    let count=f.probe.lock().unwrap().seen.len();f.recover().await.unwrap();
    assert_eq!(f.probe.lock().unwrap().seen.len(),count,"retired recovery performs no new controller call");
    assert_eq!(f.current().await,current);
}

#[tokio::test]
async fn selected_phase_reservation_is_complete_typed_size_not_http_response_ceiling() {
    let f=Fixture::new().await;
    assert_eq!(f.limits.worker.publication_record_bytes,3*1024*1024);assert_eq!(f.limits.transport.body_bytes,64*1024*1024);
    preflight_phase(&f.phase,&f.command).unwrap();
    // An independently too-small phase policy still refuses before any send.
    let mut narrow=f.command.clone();
    if let WorkerCommand::RunAttempt{task,..}=&mut narrow {
        let mut selected=task.resolved_limits.as_ref().unwrap().global_metadata.as_ref().clone();
        selected.worker.publication_record_bytes=1;
        task.resolved_limits.as_mut().unwrap().global_metadata=Box::new(selected);
    }
    context(&narrow).unwrap();
    assert_eq!(preflight_phase(&f.phase,&narrow).unwrap_err(),"PR complete future phase exceeds worker.publication_record_bytes");
    assert!(f.probe.lock().unwrap().seen.is_empty(),"sizing causes no transport or external effect");
}

#[tokio::test]
async fn normalized_success_and_abort_reports_survive_final_ack_loss_under_small_accepted_output_limit() {
    for success in [false,true] {
        let f=Fixture::with_output_limit(64,false).await;let mut phase=f.phase.clone();
        if let Observation::ResultCandidate{summary,..}=&mut phase.candidate { *summary="original successful summary ".repeat(12); }
        if success { f.commit_create(&mut phase).await; } else { phase.save(&f.state,&f.command).unwrap(); }
        f.probe.lock().unwrap().drop_final=Some(phase.final_report_id.clone());
        assert!(f.recover().await.is_err(),"real final commit ACK and immediate lookup are deliberately unavailable");
        let (report,original)=f.probe.lock().unwrap().dropped_final.clone().expect("actual normalized final mutation committed");
        let committed=original.commit.clone().expect("real controller commit");
        let retained=read(&f.state,&f.command).unwrap().unwrap();
        assert!(!retained.retired);assert!(retained.final_commit.is_none());assert_eq!(retained.final_report.as_ref(),Some(&report));
        let Observation::ResultCandidate{summary:raw,..}=retained.final_observation.as_ref().unwrap() else {panic!("original candidate")};
        let Observation::ResultCandidate{summary:normalized,checks_passed,..}=&report.observation else {panic!("normalized candidate")};
        assert!(raw.len()>64);assert!(normalized.len()<=64);assert_ne!(raw,normalized);assert_eq!(*checks_passed,success);
        let expected=if success {TaskStatus::Succeeded}else{TaskStatus::Failed};
        let current=f.current().await;assert_eq!(current.status,expected);
        if success {assert_eq!(&current.result.as_ref().unwrap().summary,normalized);} else {assert!(current.result.is_none());}
        let before=f.probe.lock().unwrap().seen.len();f.probe.lock().unwrap().deny_lookup=None;
        f.recover().await.unwrap();
        let ended=read(&f.state,&f.command).unwrap().unwrap();assert!(ended.retired);assert_eq!(ended.final_commit,Some(committed.clone()));assert_eq!(ended.final_report,Some(report.clone()));
        {
            let probe=f.probe.lock().unwrap();
            assert!(probe.seen[before..].iter().all(|op|matches!(op,Operation::GetOperation{operation_id} if operation_id==&report.report_id)),"recovery reads only the exact normalized final receipt; no new effect or final mutation");
        }
        let replay=direct(&f.controller,Operation::ObserveWorker{report:report.clone()}).await;
        assert_eq!(replay.outcome,original.outcome);assert_eq!(replay.commit,Some(committed));
        flush_outbox(&f.client,&f.state.join("outbox")).await.unwrap();
        assert!(cleanup_verified(&f.client,&f.state,&f.command).await.unwrap());
        assert_eq!(f.current().await,current);
    }
}

#[tokio::test]
async fn publication_mcp_settlement_coexists_with_legacy_phase_without_accepting_foreign_or_changed_facts() {
    use crate::mcp::{publication as p, Caller, Invocation};
    let f=Fixture::with_output_limit(4096,true).await;
    let mut legacy=f.phase.clone();
    legacy.head_effect.prepare_commit=Some(MutationCommit{operation_id:legacy.head_effect.intended.id.clone(),controller_revision:1});
    legacy.head_effect.outcome=Some(json!({"acknowledged":true,"not_applied":true}));
    legacy.save(&f.state,&f.command).unwrap();
    let WorkerCommand::RunAttempt{attempt,task,session,..}=&f.command else{unreachable!()};
    let caller=Caller{task_id:attempt.task_id.clone(),attempt_id:attempt.id.clone(),generation:attempt.generation,worker_id:attempt.worker_id.clone(),incarnation:attempt.worker_incarnation.clone()};
    let selected=p::selection(task.definition.execution_profile.as_ref()).unwrap().clone();
    // This fixture tests strict local retained-record association only. The
    // synthetic accepted-source reference is not a canonical acceptance claim
    // or permission to execute a provider operation.
    let repository=task.repository.as_ref().unwrap().clone();
    let checkpoint=git::checkpoint(&repository,"controlled-source","controlled-source-attempt","d".repeat(40)).unwrap();
    let source=p::Source{task_id:"controlled-source".into(),session_id:"controlled-source-session".into(),task_revision:9,
        accepted_result:crate::collaboration::workflow::AcceptedResultRef{task_id:"controlled-source".into(),attempt_id:"controlled-source-attempt".into(),terminal_task_revision:9,acceptance_history_sequence:11,checkpoint:Some(checkpoint.clone())},
        result_sha256:"b".repeat(64),checkpoint_attempt_id:"controlled-source-attempt".into(),checkpoint_generation:1,repository};
    let invocation=Invocation{operation_id:"coexisting-publication".into(),logical_call_id:"coexisting-call".into(),caller:caller.clone(),tool:"collab_repository_publish".into(),arguments:json!({
        "source_task_id":source.task_id,"expected_source_revision":source.task_revision,"checkpoint_operation_id":checkpoint.operation_id,
        "repository_id":selected.registration().repository_id,"registration_revision":selected.registration().registration_revision,"target_revision":selected.target_revision()})};
    let id=p::publication_id(&invocation.operation_id).unwrap();
    let head=contract::prepare_publication_head(p::Binding{publication_id:id.clone(),admission_operation_id:invocation.operation_id.clone(),
        requester:p::Requester{caller,session_id:session.id.clone()},source,selection:selected},target(),&Bounds::from_limits(&f.limits)).unwrap();
    let request=p::Command{invocation,session:session.clone(),attempt:attempt.clone(),input:task.clone(),head,create:None};
    request.validate(&id).unwrap();
    let mut effect=Effect::new(request.effect(&f.limits).unwrap()).unwrap();
    effect.prepare_commit=Some(MutationCommit{operation_id:effect.intended.id.clone(),controller_revision:2});
    effect.outcome=Some(json!({"acknowledged":true,"not_applied":true}));
    let operation=effect.settlement().unwrap();
    let command=WorkerCommand::PublishRepository{command_id:id.clone(),request:Box::new(request)};
    let command_path=f.state.join("commands").join(format!("{}.json",component(&id)));
    record_launch(&command_path,&command,&f.limits).unwrap();
    process::private_directory(&f.state.join("publication-requests")).unwrap();
    let phase_path=f.state.join("publication-requests").join(format!("{}.json",component(&id)));
    atomic_json(&phase_path,&json!({"revision":1,"command_sha256":p::digest(&command).unwrap(),"effect":effect,"unknown_recorded":false,"retired":false})).unwrap();
    let original=std::fs::read(&phase_path).unwrap();
    validate_saved_settlement(&f.state,&f.command,&operation).unwrap();
    let mut changed=operation.clone();let Operation::SettleOperation{outcome,..}=&mut changed else{unreachable!()};*outcome=json!({"acknowledged":false});
    assert!(validate_saved_settlement(&f.state,&f.command,&changed).unwrap_err().contains("publication original settlement changed"));
    let mut foreign=operation.clone();let Operation::SettleOperation{operation_id,..}=&mut foreign else{unreachable!()};*operation_id="pr-foreign".into();
    assert!(validate_saved_settlement(&f.state,&f.command,&foreign).unwrap_err().contains("identity differs"));
    let mut legacy_changed=legacy.head_effect.settlement().unwrap();let Operation::SettleOperation{outcome,..}=&mut legacy_changed else{unreachable!()};*outcome=json!({"acknowledged":false});
    assert_eq!(validate_saved_settlement(&f.state,&f.command,&legacy_changed).unwrap_err(),"PR saved settlement changed");
    assert_eq!(std::fs::read(&phase_path).unwrap(),original);
    std::fs::remove_file(&phase_path).unwrap();
    assert!(validate_saved_settlement(&f.state,&f.command,&operation).unwrap_err().contains("publication settlement phase absent"));
}
