//! Real controller transactions, immutable proof joins and reader barriers.
//! Worker/Git/provider effects are exercised separately by recovery/publication_mcp.
use super::*;
use borg_runtime::{mcp::{self, publication as p, Caller, Invocation}, pull_request as pr};
use serde_json::Value;

fn selection(reg: &borg_runtime::repository_catalogue::Registration, actions: Vec<p::Action>) -> p::Selection {
    p::Selection::V1 { repository_id: reg.repository_id.clone(), registration_revision: reg.registration_revision.clone(),
        target_revision: reg.pull_request_target.as_ref().unwrap().revision.clone(), actions, branch_namespace: "borg/publication-fixture".into() }
}
fn profile(selected: Option<p::Selection>) -> ExecutionProfile { ExecutionProfile::CollaborationV2 { web_search: None, publication: selected } }
async fn fixture(c: &Controller, root: &std::path::Path, executor: ExecutorKind, enabled: bool) -> (Task, Task, Caller, Worker, borg_runtime::repository_catalogue::Registration) {
    let reg = borg_runtime::repository_catalogue::Registration { repository_id: "publication-fixture".into(), registration_revision: "one".into(), display_name: "Local publication fixture".into(),
        source_endpoint: root.join("source.git").to_string_lossy().into(), checkpoint_endpoint: root.join("checkpoints.git").to_string_lossy().into(),
        source_store_key: "source.git".into(), checkpoint_store_key: "checkpoints.git".into(),
        pull_request_target: Some(pr::Target { revision: "target-one".into(), api_base: "http://127.0.0.1:9".into(), owner: "fixture".into(), repository: "fixture".into(), repository_id: 71,
            head_remote: root.join("provider.git").to_string_lossy().into(), base_branch: "main".into(), credential_name: "GITHUB_TOKEN".into() }) };
    register_repository_fixture(c, &reg).await;
    let mut input = coding("publication-source");
    input.repository.as_mut().unwrap().registration = Some(reg.binding());
    let source = submit(c, "publication-source-submit", input).await;
    let source_worker = worker("publication-source-worker", "one", 0, 1); register(c, &source_worker).await;
    let a = run(&commands(c, &source_worker, vec![]).await);
    assert_eq!(a.task_id, source.id);
    task(report(c, &a, "publication-source-start", Observation::Started { thread_id: Some("source-thread".into()), turn_id: Some("source-turn".into()) }).await);
    let cp = Checkpoint { registration: reg.binding(), repository: reg.checkpoint_endpoint.clone(), commit: "b".repeat(40),
        remote_ref: checkpoint_ref(&a.task_id, &a.id, "publication-source-checkpoint"), operation_id: "publication-source-checkpoint".into() };
    assert!(matches!(call(c, Operation::PrepareOperation { operation_id: cp.operation_id.clone(), task_id: a.task_id.clone(), attempt_id: a.id.clone(), generation: a.generation, kind: "checkpoint".into(),
        intended: json!({"registration":cp.registration,"repository":cp.repository,"commit":cp.commit,"remote_ref":cp.remote_ref}) }).await, Outcome::Ok { .. }));
    assert!(matches!(call(c, Operation::SettleOperation { settlement_id: "publication-source-settle".into(), operation_id: cp.operation_id.clone(), task_id: a.task_id.clone(), attempt_id: a.id.clone(), generation: a.generation,
        outcome: json!({"acknowledged":true}), checkpoint: Some(cp.clone()) }).await, Outcome::Ok { .. }));
    task(report(c, &a, "publication-source-outcome", Observation::ExecutorOutcome { success: true, summary: "controlled source checks".into() }).await);
    task(report(c, &a, "publication-source-stop", Observation::ProcessTreeStopped).await);
    let source = task(report(c, &a, "publication-source-result", Observation::ResultCandidate { checks_passed: true, summary: "accepted source".into(), checkpoint: Some(cp), validation: vec!["controlled check passed".into()], output: None }).await);
    assert_eq!(source.status, TaskStatus::Succeeded);
    let mut input = if executor == ExecutorKind::Native { native("publication-requester") } else { coding("publication-requester") };
    if let Some(repo) = &mut input.repository { repo.registration = Some(reg.binding()); }
    input.dependencies = vec![source.id.clone()];
    input.definition.execution_profile = Some(profile(enabled.then(||selection(&reg, vec![p::Action::PublishBranch, p::Action::OpenPullRequest]))));
    if executor == ExecutorKind::Native && enabled {
        let mut native=c.limits().execution.native.clone();native.function_count=mcp::catalogue_for_profile(c.limits(),input.definition.execution_profile.as_ref()).len();
        let profile=borg_runtime::native::NativeLoopProfile{functions:mcp::catalogue_for_profile(c.limits(),input.definition.execution_profile.as_ref()).into_iter().map(|t|borg_runtime::native::NativeFunction{name:t.name,description:t.description,parameters:t.input_schema}).collect(),web_search:None};
        let carrier=json!({"schema":input.output_schema,"profile":profile,"output_bytes":input.limits.output_bytes});
        native.schema_bytes=native.schema_bytes.max(serde_json::to_vec(&carrier).unwrap().len());
        // Existing complete Native test-definition profile from identity_support;
        // the larger provider request includes its catalogue and context framing.
        native.json_nodes=65_536;
        input.definition.limit_overrides=Some(Box::new(borg_runtime::limits::ExecutionLimitOverrides{native:Some(native),..Default::default()}));
    }
    let requester = submit(c, "publication-requester-submit", input).await;
    let w = worker("publication-requester-worker", "one", usize::from(executor==ExecutorKind::Native), usize::from(executor==ExecutorKind::Coding)); register(c, &w).await;
    let attempt = run(&commands(c, &w, vec![]).await);
    assert_eq!(attempt.task_id, requester.id);
    let requester = task(report(c, &attempt, "publication-requester-start", Observation::Started { thread_id: None, turn_id: None }).await);
    let caller = Caller { task_id: requester.id.clone(), attempt_id: attempt.id, generation: attempt.generation, worker_id: w.id.clone(), incarnation: w.incarnation.clone() };
    (source, requester, caller, w, reg)
}
fn invocation(caller: &Caller, id: &str, tool: &str, arguments: Value) -> Invocation {
    Invocation { operation_id: id.into(), logical_call_id: format!("logical-{id}"), caller: caller.clone(), tool: tool.into(), arguments }
}
fn publish(caller: &Caller, source: &Task, reg: &borg_runtime::repository_catalogue::Registration, id: &str) -> Invocation {
    invocation(caller,id,"collab_repository_publish",json!({"source_task_id":source.id,"expected_source_revision":source.revision,"checkpoint_operation_id":source.selected_checkpoint.as_ref().unwrap().operation_id,
        "repository_id":reg.repository_id,"registration_revision":reg.registration_revision,"target_revision":reg.pull_request_target.as_ref().unwrap().revision}))
}
async fn invoke(c: &Controller, call: &Invocation) -> Reply {
    let r = c.handle(Request { protocol_revision: WIRE_REVISION, request_id: "publication-test".into(), command: call.operation() }).await;
    r.validate_for(&call.operation()).unwrap(); r
}
fn view(reply: &Reply) -> &p::View { let Outcome::Ok { data: Data::Publication(v) } = &reply.outcome else { panic!("publication response: {reply:?}"); }; v }
async fn get_view(c: &Controller, caller: &Caller, id: &str) -> p::View {
    view(&invoke(c, &invocation(caller, &format!("get-{}",NEXT.fetch_add(1,Ordering::Relaxed)),"collab_publication_get",json!({"publication_id":id}))).await).clone()
}
fn publication_command(commands: &[WorkerCommand]) -> &p::Command {
    commands.iter().find_map(|c| if let WorkerCommand::PublishRepository { request, .. } = c { Some(request.as_ref()) } else { None }).expect("owned publication command")
}
async fn settle(c: &Controller, request: &p::Command, outcome: Value, id: &str) -> OperationRecord {
    let record=request.effect(c.limits()).unwrap();
    let r=call(c, Operation::SettleOperation { settlement_id:id.into(),operation_id:record.id,task_id:record.task_id,attempt_id:record.attempt_id,generation:record.generation,outcome,checkpoint:None }).await;
    let Outcome::Ok {data:Data::Operation(record)}=r else {panic!("settlement {r:?}")};record
}
fn head_proof(head: &pr::HeadIntent) -> pr::HeadProof {
    pr::HeadProof { operation_id:head.head_operation_id.clone(), target_revision:head.target.revision.clone(), remote:head.target.head_remote.clone(), remote_ref:head.head_ref.clone(), commit:head.binding.checkpoint.commit.clone() }
}

#[tokio::test]
async fn publication_mcp_both_executors_share_catalogue_and_absent_capability_refuses_forgery() {
    for executor in [ExecutorKind::Native,ExecutorKind::Coding] { for enabled in [false,true] {
        let db=path();let c=Controller::open_with_config(&db,config()).unwrap();
        let (source,requester,caller,worker,reg)=fixture(&c,db.parent().unwrap(),executor,enabled).await;
        let listener=borg_runtime::server::listen("127.0.0.1:0".parse().unwrap()).await.unwrap();let origin=format!("http://{}",listener.local_addr().unwrap());
        let server=tokio::spawn(borg_runtime::server::serve(c.clone(),listener,origin.clone()));
        let client=mcp::Client::new_for_profile(&origin,caller.clone(),c.limits().clone(),requester.input.definition.execution_profile.clone().unwrap()).unwrap();
        let tools=client.tools().await.unwrap();
        assert_eq!(tools.iter().filter(|t|p::handles(&t.name)).count(),if enabled {3}else{0});
        assert!(!tools.iter().any(|t|t.name.contains("deploy")||t.name.contains("merge")||t.name=="collab_repository_register"));
        let call=publish(&caller,&source,&reg,"publication-original");
        if enabled {
            let reply=client.call(&call).await.unwrap().reply;assert_eq!(view(&reply).branch.state,p::State::Pending);
            let cmds=commands(&c,&worker,vec![]).await;let request=publication_command(&cmds);
            assert_eq!(request.input,requester.input);assert_eq!(request.head.binding.task_id,source.id);
            assert_eq!(request.head.publication.as_ref().unwrap().requester.caller,caller);
            assert_ne!(request.attempt.id,request.head.binding.attempt_id);
        } else {
            // Direct canonical calls cannot bypass tool hiding.
            assert_eq!(code(invoke(&c,&call).await.outcome),"publication_policy");
            assert!(commands(&c,&worker,vec![]).await.iter().all(|c|!matches!(c,WorkerCommand::PublishRepository{..})));
        }
        assert_eq!(get(&c,&source.id).await,source);
        server.abort();let _=server.await;
    }}
}

#[tokio::test]
async fn publication_mcp_original_replay_survives_definition_catalogue_heads_cancellation_and_reopen() {
    let db=path();let cfg=config();let c=Controller::open_with_config(&db,cfg.clone()).unwrap();
    let (source,requester,caller,w,reg)=fixture(&c,db.parent().unwrap(),ExecutorKind::Native,true).await;
    let original_input=serde_json::to_vec(&requester.input).unwrap();let original=publish(&caller,&source,&reg,"publication-original");
    let first=invoke(&c,&original).await;assert_eq!(view(&first).branch.state,p::State::Pending);
    let mut next=reg.clone();next.registration_revision="two".into();next.pull_request_target.as_mut().unwrap().revision="target-two".into();
    assert!(matches!(call(&c,Operation::ReviseRepository{operation_id:"catalogue-head-change".into(),expected_generation:1,registration:next}).await,Outcome::Ok{..}));
    let update=DefinitionUpdate{agent_id:requester.input.definition.id.clone(),expected_revision:requester.input.definition.revision.clone(),expected_global_revision:c.limits().revision.clone(),expected_policy_revision:c.limits().definition_updates.as_ref().unwrap().revision.clone(),changes:std::collections::BTreeMap::from([(borg_runtime::limits::DefinitionLimitField::NativeContextBytes,80_000)])};
    assert!(matches!(call(&c,Operation::UpdateDefinition{operation_id:"definition-head-change".into(),update}).await,Outcome::Ok{..}));
    assert_eq!(serde_json::to_vec(&get(&c,&requester.id).await.input).unwrap(),original_input);
    let cmds=commands(&c,&w,vec![]).await;let request=publication_command(&cmds).clone();
    let current=get(&c,&requester.id).await;task(call(&c,Operation::CancelTask{operation_id:"cancel-publication-requester".into(),task_id:current.id,expected_revision:current.revision}).await);
    let settled=settle(&c,&request,json!(pr::HeadSettlement{acknowledged:true,proof:head_proof(&request.head)}),"late-original-fact").await;
    assert_eq!(settled.state,"settled");assert_eq!(get_view(&c,&caller,&view(&first).publication_id).await.branch.state,p::State::Completed);
    let replay=invoke(&c,&original).await;assert_eq!(replay.outcome,first.outcome);assert_eq!(replay.commit,first.commit);
    let mut conflict=original.clone();conflict.arguments["expected_source_revision"]=json!(source.revision+1);assert_eq!(code(invoke(&c,&conflict).await.outcome),"operation_conflict");
    assert_eq!(get(&c,&source.id).await,source);drop(c);controller_owner_released(&db).await;
    let c=Controller::open_with_config(&db,cfg).unwrap();let replay=invoke(&c,&original).await;assert_eq!(replay.outcome,first.outcome);assert_eq!(replay.commit,first.commit);
    assert_eq!(get(&c,&source.id).await,source);
}

#[tokio::test]
async fn publication_mcp_source_target_action_and_envelope_refusals_precede_effects() {
    let db=path();let c=Controller::open_with_config(&db,config()).unwrap();let(source,requester,caller,w,reg)=fixture(&c,db.parent().unwrap(),ExecutorKind::Native,true).await;
    let original=publish(&caller,&source,&reg,"unused");
    for (index,(field,value)) in [("source_task_id",json!(requester.id)),("expected_source_revision",json!(source.revision+1)),("checkpoint_operation_id",json!("foreign-checkpoint")),("repository_id",json!("foreign-repo")),("registration_revision",json!("two")),("target_revision",json!("wrong"))].into_iter().enumerate() {
        let mut call=original.clone();call.operation_id=format!("bad-{index}");call.logical_call_id=format!("logical-bad-{index}");call.arguments[field]=value;
        assert!(matches!(invoke(&c,&call).await.outcome,Outcome::Refused{..}),"{field}");
    }
    let mut huge=original.clone();huge.operation_id="oversized-call".into();huge.arguments["source_task_id"]=json!("x".repeat(c.limits().transport.identity_bytes+1));assert!(matches!(invoke(&c,&huge).await.outcome,Outcome::Refused{..}));
    let mut caller_bad=original.clone();caller_bad.operation_id="stale-caller".into();caller_bad.caller.generation+=1;assert!(matches!(invoke(&c,&caller_bad).await.outcome,Outcome::Refused{..}));
    let foreign=submit(&c,"foreign-source-task",native("foreign-source-agent")).await;
    let mut unaccepted=original.clone();unaccepted.operation_id="unaccepted-source".into();unaccepted.arguments["source_task_id"]=json!(foreign.id);unaccepted.arguments["expected_source_revision"]=json!(foreign.revision);assert!(matches!(invoke(&c,&unaccepted).await.outcome,Outcome::Refused{..}));
    assert!(commands(&c,&w,vec![]).await.iter().all(|c|!matches!(c,WorkerCommand::PublishRepository{..})));
    assert_eq!(get(&c,&source.id).await,source);
    let accepted=invoke(&c,&publish(&caller,&source,&reg,"correct-after-refusals")).await;assert_eq!(view(&accepted).branch.state,p::State::Pending);
    let open=invocation(&caller,"premature-pr","collab_pull_request_open",json!({"publication_id":view(&accepted).publication_id,"title":"Change","body":"Original"}));
    assert_eq!(code(invoke(&c,&open).await.outcome),"publication_pending");
    assert!(get_view(&c,&caller,&view(&accepted).publication_id).await.pull_request.is_none());
}

#[tokio::test]
async fn publication_mcp_pending_unknown_and_exact_provider_evidence_remain_distinct() {
    let db=path();let cfg=config();let c=Controller::open_with_config(&db,cfg.clone()).unwrap();let(source,_,caller,w,reg)=fixture(&c,db.parent().unwrap(),ExecutorKind::Coding,true).await;
    let admitted=invoke(&c,&publish(&caller,&source,&reg,"branch-admit")).await;let id=view(&admitted).publication_id.clone();
    let cmds=commands(&c,&w,vec![]).await;let request=publication_command(&cmds).clone();
    settle(&c,&request,json!({"acknowledged":false}),"branch-unknown").await;
    assert_eq!(get_view(&c,&caller,&id).await.branch.state,p::State::Unknown);
    settle(&c,&request,json!(pr::HeadSettlement{acknowledged:true,proof:head_proof(&request.head)}),"branch-known").await;
    let opening=invocation(&caller,"open-admit","collab_pull_request_open",json!({"publication_id":id,"title":"Exact title","body":"Exact body"}));let first=invoke(&c,&opening).await;assert_eq!(view(&first).pull_request.as_ref().unwrap().state,p::State::Pending);
    let cmds=commands(&c,&w,vec![id.clone()]).await;let create=cmds.iter().find_map(|cmd|match cmd{WorkerCommand::PublishRepository{request,..} if request.create.is_some()=>Some(request.as_ref().clone()),_=>None}).unwrap();
    settle(&c,&create,json!({"acknowledged":false}),"create-unknown").await;assert_eq!(get_view(&c,&caller,&id).await.pull_request.unwrap().state,p::State::Unknown);
    let intent=create.create.as_ref().unwrap();let t=&intent.head.target;let branch=|name:String,commit:String|pr::BranchProof{repository_id:t.repository_id,owner:t.owner.clone(),repository:t.repository.clone(),branch:name,commit};
    let proof=pr::Proof{operation_id:intent.head.binding.operation_id.clone(),provider_id:91,number:17,url:format!("{}/{}/{}/pull/17",t.api_base,t.owner,t.repository),
        head:branch(intent.head.head_ref.strip_prefix("refs/heads/").unwrap().into(),source.selected_checkpoint.as_ref().unwrap().commit.clone()),base:branch("main".into(),"c".repeat(40)),title:intent.title.clone(),body:intent.body.clone(),marker:intent.marker.clone()};
    settle(&c,&create,json!(pr::Settlement{acknowledged:true,proof:proof.clone()}),"create-known").await;
    let observed=get_view(&c,&caller,&id).await;let reference=observed.pull_request.unwrap().pull_request.unwrap();assert_eq!(reference.head,proof.head);assert_eq!(reference.base,proof.base);assert_eq!(reference.provider_id,91);
    let replay=invoke(&c,&opening).await;assert_eq!(replay.outcome,first.outcome);assert_eq!(replay.commit,first.commit);
    drop(c);controller_owner_released(&db).await;Controller::open_with_config(&db,cfg).unwrap();
}

#[tokio::test]
async fn publication_mcp_schema25_upgrade_preserves_bytes_and_unmarked_new_records_refuse() {
    let db=path();let cfg=config();let c=Controller::open_with_config(&db,cfg.clone()).unwrap();let t=submit(&c,"old-record",native("historical")).await;drop(c);controller_owner_released(&db).await;
    let raw=rusqlite::Connection::open(&db).unwrap();let before:String=raw.query_row("SELECT request FROM receipts WHERE id='old-record'",[],|r|r.get(0)).unwrap();raw.pragma_update(None,"user_version",25).unwrap();drop(raw);
    let c=Controller::open_with_config(&db,cfg.clone()).unwrap();assert_eq!(get(&c,&t.id).await,t);drop(c);controller_owner_released(&db).await;
    let raw=rusqlite::Connection::open(&db).unwrap();assert_eq!(raw.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),28);assert_eq!(raw.query_row("SELECT request FROM receipts WHERE id='old-record'",[],|r|r.get::<_,String>(0)).unwrap(),before);drop(raw);
    let db=path();let c=Controller::open_with_config(&db,cfg.clone()).unwrap();let(source,_,caller,_,reg)=fixture(&c,db.parent().unwrap(),ExecutorKind::Native,true).await;
    let reply=invoke(&c,&publish(&caller,&source,&reg,"new-publication")).await;assert_eq!(view(&reply).branch.state,p::State::Pending);drop(c);controller_owner_released(&db).await;
    for version in [25,26] {
        let raw=rusqlite::Connection::open(&db).unwrap();raw.pragma_update(None,"user_version",version).unwrap();let original:String=raw.query_row("SELECT body FROM commands WHERE json_extract(body,'$.kind')='publish_repository'",[],|r|r.get(0)).unwrap();drop(raw);
        assert!(Controller::open_with_config(&db,cfg.clone()).is_err());let raw=rusqlite::Connection::open(&db).unwrap();assert_eq!(raw.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),version);assert_eq!(raw.query_row("SELECT body FROM commands WHERE json_extract(body,'$.kind')='publish_repository'",[],|r|r.get::<_,String>(0)).unwrap(),original);
    }
    let raw=rusqlite::Connection::open(&db).unwrap();raw.pragma_update(None,"user_version",27).unwrap();drop(raw);
    let c=Controller::open_with_config(&db,cfg).unwrap();drop(c);controller_owner_released(&db).await;
    let raw=rusqlite::Connection::open(&db).unwrap();assert_eq!(raw.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),28);
}

#[tokio::test]
async fn publication_mcp_reserved_command_and_settlement_capacity_survives_receipt_exhaustion() {
    let db=path();let mut cfg=config();Arc::make_mut(&mut cfg.limits).controller.max_receipts=50;
    let c=Controller::open_with_config(&db,cfg.clone()).unwrap();let(source,_,caller,w,reg)=fixture(&c,db.parent().unwrap(),ExecutorKind::Native,true).await;
    let admitted=invoke(&c,&publish(&caller,&source,&reg,"bounded-publication")).await;assert_eq!(view(&admitted).branch.state,p::State::Pending);
    for n in 0..60 {
        let reply=invoke(&c,&invocation(&caller,&format!("fill-{n}"),"not_a_tool",json!({}))).await;
        if reply.commit.is_none() { assert_eq!(code(reply.outcome),"capacity");break; }
    }
    let cmds=commands(&c,&w,vec![]).await;let request=publication_command(&cmds).clone();
    let id=view(&admitted).publication_id.clone();
    // These original command receipts consume their own reservation, never the
    // requesting attempt's Run/Stop cleanup allowance.
    assert!(commands(&c,&w,vec![id]).await.is_empty());
    let record=settle(&c,&request,json!(pr::HeadSettlement{acknowledged:true,proof:head_proof(&request.head)}),"reserved-settlement").await;
    assert_eq!(record.state,"settled");assert_eq!(get(&c,&source.id).await,source);
    drop(c);controller_owner_released(&db).await;Controller::open_with_config(&db,cfg).unwrap();
}

#[tokio::test]
async fn publication_mcp_missing_target_disallowed_action_and_small_worker_envelope_refuse() {
    let db=path();let mut cfg=config();Arc::make_mut(&mut cfg.limits).worker.publication_record_bytes=1;
    let c=Controller::open_with_config(&db,cfg).unwrap();let(source,_,caller,w,reg)=fixture(&c,db.parent().unwrap(),ExecutorKind::Native,true).await;
    assert_eq!(code(invoke(&c,&publish(&caller,&source,&reg,"tiny-publication-envelope")).await.outcome),"capacity");
    assert!(commands(&c,&w,vec![]).await.iter().all(|c|!matches!(c,WorkerCommand::PublishRepository{..})));
    let mut input=native("missing-publication-target");input.definition.execution_profile=Some(profile(Some(p::Selection::V1{repository_id:reg.repository_id.clone(),registration_revision:reg.registration_revision.clone(),target_revision:"unregistered-target".into(),actions:vec![p::Action::PublishBranch],branch_namespace:"borg/fixture".into()})));
    assert_eq!(code(call(&c,Operation::SubmitTask{operation_id:"missing-target-task".into(),input}).await),"publication_configuration");
    let selected=selection(&reg,vec![p::Action::PublishBranch]);
    assert!(p::validate("collab_pull_request_open",&json!({"publication_id":"x","title":"title","body":"body"}),c.limits(),Some(&selected)).is_err());
    assert_eq!(get(&c,&source.id).await,source);
}

#[tokio::test]
async fn publication_mcp_stopped_unacknowledged_owner_requires_original_admission_effect_and_session() {
    for tamper in ["none", "admission", "effect", "session"] {
        let db=path();let cfg=config();let c=Controller::open_with_config(&db,cfg.clone()).unwrap();
        let (source,requester,caller,w,reg)=fixture(&c,db.parent().unwrap(),ExecutorKind::Coding,true).await;
        let original=publish(&caller,&source,&reg,"stopped-publication-original");
        let first=invoke(&c,&original).await;assert_eq!(view(&first).branch.state,p::State::Pending);
        let cmds=commands(&c,&w,vec![]).await;let request=publication_command(&cmds).clone();
        let current=get(&c,&requester.id).await;
        task(call(&c,Operation::CancelTask{operation_id:"stop-publication-owner".into(),task_id:current.id,expected_revision:current.revision}).await);
        task(report(&c,&request.attempt,"stopped-publication-owner",Observation::ProcessTreeStopped).await);
        assert!(get(&c,&requester.id).await.attempt.as_ref().unwrap().stopped);
        drop(c);controller_owner_released(&db).await;
        let raw=rusqlite::Connection::open(&db).unwrap();
        let command_id=&view(&first).publication_id;
        let (ack,body):(i64,String)=raw.query_row("SELECT acknowledged,body FROM commands WHERE id=?1",[command_id],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
        assert_eq!(ack,0,"only the original publication remains unacknowledged");
        match tamper {
            "none"=>{},
            "admission"=>{assert_eq!(raw.execute("DELETE FROM receipts WHERE id=?1",[&original.operation_id]).unwrap(),1);},
            "effect"=>{let id=request.effect(&cfg.limits).unwrap().id;assert_eq!(raw.execute("DELETE FROM external_operations WHERE id=?1",[id]).unwrap(),1);},
            "session"=>{let mut v:Value=serde_json::from_str(&body).unwrap();v["request"]["session"]["id"]=json!("foreign-session");assert_eq!(raw.execute("UPDATE commands SET body=?1 WHERE id=?2",rusqlite::params![v.to_string(),command_id]).unwrap(),1);},
            _=>unreachable!(),
        }
        drop(raw);let before=std::fs::read(&db).unwrap();
        if tamper=="none" {
            let c=Controller::open_with_config(&db,cfg).unwrap();let replay=invoke(&c,&original).await;
            assert_eq!(replay.commit,first.commit);assert_eq!(replay.outcome,first.outcome);assert_eq!(get(&c,&source.id).await,source);
        } else {
            assert!(Controller::open_with_config(&db,cfg).is_err(),"stopped owner cannot bypass {tamper} binding");
            assert_eq!(std::fs::read(&db).unwrap(),before,"refusal must retain original damaged evidence");
        }
    }
}
