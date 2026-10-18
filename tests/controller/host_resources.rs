//! Real controller/SQLite and MCP consumers with controlled clocks and reports.
//! These fixtures do not assert installed telemetry or four-host deployment.
use super::*;
use borg_runtime::{collaboration::{Query,QueryResult},machine_resources::*};
use rusqlite::Connection;

fn profile()->Profile {serde_json::from_str(include_str!("../../metadata/machine-resources.example.json")).unwrap()}
fn settings()->(ControllerConfig,Arc<AtomicU64>,Arc<AtomicU64>) {
    let mono=Arc::new(AtomicU64::new(100));let wall=Arc::new(AtomicU64::new(1_000_000));
    let mut cfg=config();let m=mono.clone();let w=wall.clone();
    cfg.machine_resources=Some(borg_runtime::controller::machine_resources::Config {profile:Arc::new(profile()),monotonic_ms:Arc::new(move||m.load(Ordering::SeqCst))});
    cfg.clock=Arc::new(move||w.load(Ordering::SeqCst));
    (cfg,mono,wall)
}
async fn registered(c:&Controller) {
    for m in profile().machines.into_iter().filter(|m|m.role==Role::Worker) {register(c,&worker(m.worker_id.as_deref().unwrap(),"inc-1",1,1)).await;}
}
fn sample(index:usize,sequence:u64)->Sample {
    let p=profile();let m=&p.machines[index];
    Sample {machine_id:m.machine_id.clone(),role:m.role,worker:m.worker_id.as_ref().map(|id|WorkerBinding{worker_id:id.clone(),incarnation:"inc-1".into()}),
        producer_epoch:"11111111-1111-1111-1111-111111111111:2".into(),sequence,observed_at_unix_ms:50,sample_window_ms:Some(5000),
        cpu:Metric::Available{value:CpuUsage{used_core_equivalents:1.0,total_online_logical_cores:4,percent:25.0,busy_ticks:25,total_ticks:100,window_ms:5000}},
        memory:Metric::Available{value:MemoryUsage{used_bytes:3072,total_bytes:4096,available_bytes:1024}},
        filesystems:vec![Filesystem{scope_labels:m.storage_labels.clone(),space:Metric::Available{value:DiskUsage{used_bytes:286720,total_bytes:409600,free_bytes:122880,available_bytes:81920}}}]}
}
async fn exchange(c:&Controller,operation:Operation)->Reply {
    let reply=c.handle(Request {protocol_revision:WIRE_REVISION,request_id:"host-resources-test".into(),command:operation.clone()}).await;
    reply.validate_for(&operation).unwrap();reply
}
async fn send(c:&Controller,sample:Sample)->Reply {exchange(c,Operation::ReportMachineResources{sample}).await}
async fn view(c:&Controller,index:usize)->View {
    match exchange(c,Operation::QueryCollaboration{query:Query::Machine{machine_id:profile().machines[index].machine_id.clone()}}).await.outcome {
        Outcome::Ok{data:Data::Collaboration(QueryResult::Machine(v))}=>v,other=>panic!("{other:?}"),
    }
}
// Read through the database owner while it is live. Direct SQLite checks
// below happen only after controller_owner_released confirms closure.
async fn snapshot(c:&Controller)->(u64,Vec<Task>,Vec<borg_runtime::collaboration::HistoryEntry>) {
    let reply=exchange(c,Operation::ListTasks{after:None,limit:100}).await;
    let revision=reply.observed_controller_revision.unwrap();
    let Outcome::Ok{data:Data::Tasks(tasks)}=reply.outcome else{panic!("{reply:?}")};
    let mut history=Vec::new();
    for task in &tasks {
        let Outcome::Ok{data:Data::Collaboration(QueryResult::TaskHistory(page))}=exchange(c,Operation::QueryCollaboration{query:Query::TaskHistory{task_id:task.id.clone(),after:None,limit:100}}).await.outcome else{panic!("history read refused")};
        assert!(page.next_cursor.is_none(),"controlled fixture history is fully observed");
        history.extend(page.items);
    }
    (revision,tasks,history)
}
#[tokio::test]
async fn host_resources_latest_identity_ordering_and_receipt_budget() {
    let (mut cfg,mono,_)=settings();Arc::make_mut(&mut cfg.limits).controller.max_receipts=4;
    let p=path();let c=Controller::open_with_config(&p,cfg).unwrap();registered(&c).await;
    let first=sample(1,1);
    let accepted=send(&c,first.clone()).await;assert!(accepted.commit.is_none());
    assert_eq!(accepted.outcome,Outcome::Ok{data:Data::MachineResourceReport(ReportDisposition::Replaced)});
    mono.store(1100,Ordering::SeqCst);
    assert_eq!(send(&c,first.clone()).await.outcome,Outcome::Ok{data:Data::MachineResourceReport(ReportDisposition::Duplicate)});
    assert_eq!(view(&c,1).await.receipt_age_ms,Some(1000));
    let mut conflict=first.clone();conflict.observed_at_unix_ms+=1;
    assert_eq!(code(send(&c,conflict).await.outcome),"sample_conflict");
    let mut bad=first.clone();bad.sequence=2;bad.memory=Metric::Available{value:MemoryUsage{total_bytes:1,used_bytes:2,available_bytes:0}};
    assert_eq!(code(send(&c,bad).await.outcome),"memory_consistency");
    bad=sample(1,2);bad.machine_id="not-configured".into();assert_eq!(code(send(&c,bad).await.outcome),"machine_identity");
    bad=sample(1,2);bad.producer_epoch="22222222-2222-2222-2222-222222222222:3".into();assert_eq!(code(send(&c,bad).await.outcome),"producer_epoch");
    assert!(matches!(send(&c,sample(1,3)).await.outcome,Outcome::Ok{..}));
    assert_eq!(code(send(&c,sample(1,2)).await.outcome),"older_sample");
    let mut restarted=sample(1,1);restarted.producer_epoch="11111111-1111-1111-1111-111111111111:3".into();
    assert!(matches!(send(&c,restarted).await.outcome,Outcome::Ok{..}),"same-incarnation process restart can restart sample sequence after advancing kernel epoch");
    assert_eq!(code(send(&c,sample(1,999)).await.outcome),"producer_epoch","delayed predecessor cannot reclaim the slot");
    register(&c,&worker("agent-lab-2","inc-2",1,1)).await;
    assert_eq!(view(&c,1).await.freshness,Freshness::Unobserved);
    let mut successor=sample(1,1);successor.worker.as_mut().unwrap().incarnation="inc-2".into();successor.producer_epoch="22222222-2222-2222-2222-222222222222:1".into();
    assert!(matches!(send(&c,successor.clone()).await.outcome,Outcome::Ok{..}));
    assert_eq!(code(send(&c,sample(1,9999)).await.outcome),"stale_worker");
    assert_eq!(code(call(&c,Operation::RegisterAgent{operation_id:"receipt-full".into(),definition:native("fill").definition}).await),"capacity");
    let before=snapshot(&c).await;assert_eq!(before.0,4);
    for sequence in 2..=128 {successor.sequence=sequence;assert!(matches!(send(&c,successor.clone()).await.outcome,Outcome::Ok{..}));}
    assert_eq!(view(&c,1).await.sample.unwrap().sequence,128);
    assert_eq!(snapshot(&c).await,before,"telemetry must not churn task receipts/revisions/history even when saturated");
    let old=c.handle(Request{protocol_revision:23,request_id:"old-wire".into(),command:Operation::ReportMachineResources{sample:sample(0,1)}}).await;
    assert_eq!(code(old.outcome),"unsupported_version");assert_eq!(view(&c,0).await.freshness,Freshness::Unobserved);
    drop(c);controller_owner_released(&p).await;
    let sql=Connection::open(&p).unwrap();
    assert_eq!(sql.query_row("SELECT count(*) FROM receipts",[],|r|r.get::<_,i64>(0)).unwrap(),4);
    assert_eq!(sql.query_row("SELECT value FROM counters WHERE kind='controller_revision'",[],|r|r.get::<_,u64>(0)).unwrap(),before.0);
}
#[tokio::test]
async fn host_resources_controller_workers_fresh_stale_and_restart() {
    let (cfg,mono,wall)=settings();let p=path();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();registered(&c).await;
    for index in 0..4 {assert_eq!(view(&c,index).await.freshness,Freshness::Unobserved);let mut s=sample(index,1);s.observed_at_unix_ms=u64::MAX;assert!(matches!(send(&c,s).await.outcome,Outcome::Ok{..}));}
    mono.store(15099,Ordering::SeqCst);wall.store(1,Ordering::SeqCst);
    for index in 0..4 {let v=view(&c,index).await;assert_eq!(v.freshness,Freshness::Fresh);assert_eq!(v.receipt_age_ms,Some(14999));assert_eq!(v.received_at_unix_ms,Some(1_000_000));}
    mono.store(15100,Ordering::SeqCst);
    for index in 0..4 {assert_eq!(view(&c,index).await.freshness,Freshness::Stale);}
    let mut failed=sample(2,2);failed.memory=Metric::Unavailable{reason:Reason::ReadUnavailable};
    send(&c,failed.clone()).await;
    let v=view(&c,2).await;assert_eq!(v.freshness,Freshness::Fresh);assert_eq!(v.sample.unwrap().memory,failed.memory);
    mono.store(30100,Ordering::SeqCst);send(&c,failed).await;assert_eq!(view(&c,2).await.freshness,Freshness::Stale,"duplicate does not renew receiver time");
    let stored=snapshot(&c).await;drop(c);controller_owner_released(&p).await;
    let restarted=Controller::open_with_config(&p,cfg).unwrap();
    for index in 0..4 {let v=view(&restarted,index).await;assert_eq!(v.freshness,Freshness::Unobserved);assert!(v.sample.is_none());assert!(v.received_at_unix_ms.is_none());}
    assert_eq!(snapshot(&restarted).await,stored,"restart does not restore fabricated host samples");
}
async fn actor(c:&Controller,id:&str,coding_task:bool)->borg_runtime::mcp::Caller {
    let t=submit(c,id,if coding_task{coding(id)}else{native(id)}).await;
    let mut attempt=None;
    for index in 1..4 {for command in commands(c,&worker(&profile().machines[index].machine_id,"inc-1",1,1),vec![]).await {
        if let WorkerCommand::RunAttempt{attempt:a,..}=command {if a.task_id==t.id {attempt=Some(a);}}
    }}
    let a=attempt.expect("controlled task admitted");
    assert!(matches!(report(c,&a,&format!("{id}-started"),Observation::Started{thread_id:None,turn_id:None}).await,Outcome::Ok{..}));
    borg_runtime::mcp::Caller{task_id:a.task_id,attempt_id:a.id,generation:a.generation,worker_id:a.worker_id,incarnation:a.worker_incarnation}
}
async fn mcp(c:&Controller,actor:&borg_runtime::mcp::Caller,id:&str,tool:&str,args:serde_json::Value)->borg_runtime::mcp::CallResult {
    let body=serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":tool,"arguments":args,"_meta":{"borg":{"operation_id":id,"logical_call_id":id}}}})).unwrap();
    let (status,bytes)=borg_runtime::mcp::handle(c,Some(&serde_json::to_string(actor).unwrap()),Some(borg_runtime::mcp::VERSION),&body).await;
    assert_eq!(status,200);let rpc:serde_json::Value=serde_json::from_slice(&bytes).unwrap();
    serde_json::from_str(rpc["result"]["content"][0]["text"].as_str().unwrap_or_else(||panic!("{rpc}"))).unwrap()
}
#[tokio::test]
async fn host_resources_shared_mcp_projection_redaction_and_no_mutations() {
    let (cfg,mono,_)=settings();let p=path();let c=Controller::open_with_config(&p,cfg).unwrap();registered(&c).await;
    let native=actor(&c,"machine-native",false).await;let coding=actor(&c,"machine-coding",true).await;
    for index in 0..4 {send(&c,sample(index,1)).await;}
    let before=snapshot(&c).await;
    let n=mcp(&c,&native,"native-machine-read","collab_machines_list",json!({"cursor":null,"limit":4})).await;
    let cod=mcp(&c,&coding,"coding-machine-read","collab_machines_list",json!({"cursor":null,"limit":4})).await;
    assert_eq!(n.reply.outcome,cod.reply.outcome);assert!(!n.requires_quiescence&&!cod.requires_quiescence);
    let Outcome::Ok{data:Data::Collaboration(QueryResult::Machines(page))}=&n.reply.outcome else{panic!("{n:?}")};assert_eq!(page.items.len(),4);assert!(page.next_cursor.is_none());assert_eq!(page.items[0].machine.role,Role::Controller);
    let one=mcp(&c,&native,"one-machine","collab_machine_get",json!({"machine_id":"agent-lab"})).await;
    assert_eq!(one.reply.outcome,Outcome::Ok{data:Data::Collaboration(QueryResult::Machine(page.items[0].clone()))});
    let first=exchange(&c,Operation::QueryCollaboration{query:Query::Machines{after:None,limit:1}}).await;
    let Outcome::Ok{data:Data::Collaboration(QueryResult::Machines(first))}=first.outcome else{panic!()};assert_eq!(first.items.len(),1);
    let second=exchange(&c,Operation::QueryCollaboration{query:Query::Machines{after:first.next_cursor.clone(),limit:1}}).await;
    let Outcome::Ok{data:Data::Collaboration(QueryResult::Machines(second))}=second.outcome else{panic!()};assert_eq!(second.items[0].machine.machine_id,"agent-lab-2");
    assert_eq!(code(exchange(&c,Operation::QueryCollaboration{query:Query::Machines{after:None,limit:0}}).await.outcome),"invalid_input");
    assert_eq!(code(exchange(&c,Operation::QueryCollaboration{query:Query::Workers{after:first.next_cursor,limit:1}}).await.outcome),"invalid_input");
    let tools=borg_runtime::mcp::catalogue(c.limits());
    for tool in tools.iter().filter(|t|t.name.contains("machine")) {assert_eq!(tool.annotations["readOnlyHint"],true);}
    assert_eq!(tools.iter().filter(|t|t.name.contains("machine")).count(),2);
    assert!(borg_runtime::mcp::validate_arguments("report_machine_resources",&json!({}),c.limits()).is_err());
    let raw=serde_json::to_string(&n).unwrap();for secret in ["/var/lib/borg","/usr/bin/stat","credential","command_line","environment"] {assert!(!raw.contains(secret),"{secret}");}
    mono.store(20000,Ordering::SeqCst);send(&c,sample(0,2)).await;
    let replay=mcp(&c,&native,"native-machine-read","collab_machines_list",json!({"cursor":null,"limit":4})).await;
    assert_eq!(replay.reply.commit,n.reply.commit);assert_eq!(replay.reply.outcome,n.reply.outcome,"original MCP read receipt must not be freshly sampled or re-aged");
    assert_eq!(snapshot(&c).await.1,before.1);assert_eq!(snapshot(&c).await.2,before.2,"model reads do not mutate task history");
}
fn shape(c:&Connection)->Vec<(String,String,Option<String>)> {
    c.prepare("SELECT type,name,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name").unwrap().query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap().map(Result::unwrap).collect()
}
fn receipts(c:&Connection)->Vec<(String,String,String)> {
    c.prepare("SELECT id,request,outcome FROM receipts ORDER BY id").unwrap().query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap().map(Result::unwrap).collect()
}
fn prior24()->Connection {
    let c=Connection::open_in_memory().unwrap();
    for sql in [include_str!("../../src/controller/schema12.sql"),include_str!("../../src/controller/schema13-from12.sql"),include_str!("../../src/controller/schema14-from13.sql"),include_str!("../../src/controller/schema17-from16.sql"),include_str!("../../src/controller/schema18-from17.sql"),include_str!("../../src/controller/schema23-from22.sql"),include_str!("../../src/controller/schema24-from23.sql")] {c.execute_batch(sql).unwrap();}
    c.pragma_update(None,"user_version",24).unwrap();c
}
#[tokio::test]
async fn host_resources_prior_schema_new_vocabulary_refused() {
    let (cfg,_,_)=settings();let p=path();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();registered(&c).await;
    drop(c);controller_owner_released(&p).await;
    let sql=Connection::open(&p).unwrap();assert_eq!(shape(&sql),shape(&prior24()),"independently constructed historical24 SQL remains exact");
    let originals=receipts(&sql);drop(sql);
    let sql=Connection::open(&p).unwrap();sql.pragma_update(None,"user_version",24).unwrap();drop(sql);
    let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    drop(c);controller_owner_released(&p).await;
    let sql=Connection::open(&p).unwrap();assert_eq!(sql.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),25);assert_eq!(receipts(&sql),originals);assert_eq!(shape(&sql),shape(&prior24()));drop(sql);
    let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let caller=actor(&c,"historical-telemetry-read",false).await;
    let result=mcp(&c,&caller,"new-machine-outcome","collab_machines_list",json!({"cursor":null,"limit":4})).await;assert!(matches!(result.reply.outcome,Outcome::Ok{..}));
    drop(c);controller_owner_released(&p).await;
    let sql=Connection::open(&p).unwrap();sql.pragma_update(None,"user_version",24).unwrap();let originals=receipts(&sql);drop(sql);
    let before=std::fs::read(&p).unwrap();
    let refused=Controller::open_with_config(&p,cfg);assert!(matches!(refused,Err(ref error) if error.contains("schema25")),"new typed outcome must refuse before upgrade");
    assert_eq!(std::fs::read(&p).unwrap(),before);let sql=Connection::open(&p).unwrap();assert_eq!(receipts(&sql),originals);assert_eq!(sql.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),24);
}
