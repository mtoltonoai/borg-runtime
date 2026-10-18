//! Real controller transactions with controlled, explicitly identified worker
//! reports. Filesystem custody is exercised separately by the cleanup producer.
use super::*;
use std::sync::atomic::AtomicBool;
use borg_runtime::read_retention::{ReadEvidence, ReleaseItem, TerminalReadRelease, TranscriptCustody};

struct RetainedFixture {
    c: Controller,
    cfg: ControllerConfig,
    path: PathBuf,
    clock: Arc<AtomicU64>,
    task: Task,
    attempt: Attempt,
    actor: Caller,
    command_id: String,
}
async fn retained_fixture(label: &str) -> RetainedFixture {
    let mut input=profiled(label);
    input.definition.execution_profile=Some(ExecutionProfile::CollaborationV2 {web_search:None});
    retained_fixture_with(label,input,config(),None).await
}
async fn retained_fixture_with(label:&str,input:TaskInput,mut cfg:ControllerConfig,registration:Option<borg_runtime::repository_catalogue::Registration>)->RetainedFixture {
    let mut limits=(*cfg.limits).clone();
    limits.retention=Some(borg_runtime::limits::RetentionLimits {worker:borg_runtime::limits::WorkerRetentionLimits {
        success_grace_ms:100,failed_grace_ms:100,cancelled_grace_ms:100,idle_cache_grace_ms:1000,
        batch_entries:8,filesystem_entries_per_yield:8,operation_timeout_ms:1000,retry_delay_ms:1,record_bytes:1_048_576,
    }});
    cfg.limits=Arc::new(limits);
    cfg.qualification_fail_next_commit=Some(Arc::new(AtomicBool::new(false)));
    let clock=Arc::new(AtomicU64::new(future()-300_000));let now=clock.clone();cfg.clock=Arc::new(move||now.load(Ordering::SeqCst));
    let path=path();let c=Controller::open_with_config(&path,cfg.clone()).unwrap();
    if let Some(registration)=registration { register_repository_fixture(&c,&registration).await; }
    let task=submit(&c,&format!("{label}-submit"),input).await;
    let w=worker(&format!("{label}-worker"),"inc",1,0);register(&c,&w).await;
    let commands=commands(&c,&w,vec![]).await;
    let (command_id,attempt)=commands.into_iter().find_map(|cmd|match cmd {
        WorkerCommand::RunAttempt {command_id,attempt,..} if attempt.task_id==task.id=>Some((command_id,attempt)),_=>None,
    }).unwrap();
    let actor=Caller {task_id:task.id.clone(),attempt_id:attempt.id.clone(),generation:attempt.generation,
        worker_id:attempt.worker_id.clone(),incarnation:attempt.worker_incarnation.clone()};
    self::task(report(&c,&attempt,&format!("{label}-started"),Observation::Started {thread_id:None,turn_id:None}).await);
    RetainedFixture {c,cfg,path,clock,task,attempt,actor,command_id}
}
async fn exchange(c:&Controller,command:Operation)->Reply {
    let reply=c.handle(Request {protocol_revision:WIRE_REVISION,request_id:"read-retention-fixture".into(),command:command.clone()}).await;
    reply.validate_for(&command).unwrap();reply
}
async fn large_read(f:&RetainedFixture,id:&str)->(Invocation,Reply) {
    let set=invocation(&f.actor,&format!("{id}-set"),"collab_memory_set",json!({"key":id,"expected_revision":null,"value":"immutable original ".repeat(4096)}));
    assert!(matches!(invoke(&f.c,&set).await.outcome,Outcome::Ok{..}));
    let read=invocation(&f.actor,id,"collab_memory_get",json!({"key":id}));let reply=invoke(&f.c,&read).await;
    assert!(matches!(reply.outcome,Outcome::Ok {data:Data::Memory(Some(_))}));(read,reply)
}
async fn close_cancelled(f:&RetainedFixture) {
    let current=get(&f.c,&f.task.id).await;
    self::task(call_fn(&f.c,Operation::CancelTask {operation_id:format!("{}-cancel",f.task.id),task_id:current.id,expected_revision:current.revision}).await);
    self::task(report(&f.c,&f.attempt,&format!("{}-stop",f.task.id),Observation::ProcessTreeStopped).await);
    let closed=get(&f.c,&f.task.id).await;
    assert_eq!(closed.status,TaskStatus::Cancelled);assert!(closed.attempt.as_ref().unwrap().stopped);assert!(!closed.attempt.as_ref().unwrap().uncertain);
}
async fn release_plan(f:&RetainedFixture,calls:&[(Invocation,Reply)],id:&str)->TerminalReadRelease {
    let current=get(&f.c,&f.task.id).await;
    let bytes=serde_json::to_vec(calls).unwrap();
    TerminalReadRelease {operation_id:id.into(),caller:f.actor.clone(),command_id:f.command_id.clone(),session_id:current.session_id,
        terminal_revision:current.revision,first_safe_operation_id:None,policy_revision:f.cfg.limits.revision.clone(),policy:f.cfg.limits.retention.as_ref().unwrap().worker.clone(),
        custody:TranscriptCustody::NativeJournal {manifest_sha256:borg_runtime::read_retention::digest(&bytes),bytes:bytes.len() as u64,files:calls.len() as u64},
        custody_operation_id:id.into(),reads:calls.iter().map(|(call,reply)|ReleaseItem {operation_id:call.operation_id.clone(),commit:reply.commit.clone().unwrap(),evidence:ReadEvidence::from_outcome(call,&reply.outcome).unwrap()}).collect()}
}
async fn custody_and_safe(f:&RetainedFixture,plan:&TerminalReadRelease)->TerminalReadRelease {
    self::task(report(&f.c,&f.attempt,&plan.custody_operation_id,Observation::ReadTranscriptCustody {proof:Box::new(plan.clone())}).await);
    let mut safe=plan.clone();safe.operation_id=format!("{}-safe",plan.operation_id);
    let reply=exchange(&f.c,Operation::ReleaseTerminalReads(safe.clone())).await;
    let Outcome::Ok {data:Data::TerminalReadRelease(first)}=reply.outcome else {panic!("first-safe refused: {reply:?}")};
    assert!(first.released.is_empty());assert_eq!(first.first_safe_ms,f.clock.load(Ordering::SeqCst));
    f.clock.store(first.eligible_at_ms,Ordering::SeqCst);
    let mut release=plan.clone();release.operation_id=format!("{}-release",plan.operation_id);release.first_safe_operation_id=Some(safe.operation_id);release
}
async fn release(f:&RetainedFixture,plan:&TerminalReadRelease)->Reply {
    exchange(&f.c,Operation::ReleaseTerminalReads(plan.clone())).await
}

#[tokio::test]
async fn terminal_release_checks_real_closure_and_transcript_custody() {
    let f=retained_fixture("release-closure").await;let read=large_read(&f,"read-closure").await;
    let active=release_plan(&f,std::slice::from_ref(&read),"active-custody").await;
    assert_eq!(code(release(&f,&active).await.outcome),"read_release_held");
    close_cancelled(&f).await;
    let plan=release_plan(&f,std::slice::from_ref(&read),"closed-custody").await;
    let mut missing_custody=plan.clone();missing_custody.operation_id="closed-release-without-custody".into();
    assert_eq!(code(release(&f,&missing_custody).await.outcome),"read_release_held","valid-looking hash/counts without original worker custody are not authority");
    let mut foreign=plan.clone();foreign.caller.incarnation="foreign".into();
    assert!(matches!(report(&f.c,&f.attempt,"wrong-custody",Observation::ReadTranscriptCustody{proof:Box::new(foreign)}).await,Outcome::Refused{..}));
    let release_plan=custody_and_safe(&f,&plan).await;
    let expected=get(&f.c,&f.task.id).await;
    let result=release(&f,&release_plan).await;
    assert!(matches!(&result.outcome,Outcome::Ok{data:Data::TerminalReadRelease(r)} if r.released==vec![read.0.operation_id.clone()]&&r.body_bytes_after<r.body_bytes_before));
    assert_eq!(get(&f.c,&f.task.id).await,expected,"custody/release must never revive the stopped task");
}

#[tokio::test]
async fn terminal_release_commit_failure_replay_and_changed_batch() {
    let f=retained_fixture("release-cut").await;let read=large_read(&f,"read-cut").await;close_cancelled(&f).await;
    let plan=release_plan(&f,std::slice::from_ref(&read),"cut-custody").await;let plan=custody_and_safe(&f,&plan).await;
    f.cfg.qualification_fail_next_commit.as_ref().unwrap().store(true,Ordering::SeqCst);
    let failed=release(&f,&plan).await;assert!(failed.commit.is_none());
    assert_eq!(mcp::recovered(&read.0,lookup(&f.c,&read.0).await).unwrap().reply.outcome,read.1.outcome);
    let first=release(&f,&plan).await;assert!(matches!(first.outcome,Outcome::Ok{..}));
    let again=release(&f,&plan).await;assert_eq!(again.commit,first.commit);assert_eq!(again.outcome,first.outcome);
    let mut changed=plan.clone();changed.reads[0].evidence.outcome_sha256="0".repeat(64);
    assert_eq!(code(release(&f,&changed).await.outcome),"operation_conflict");
    let reference=lookup(&f.c,&read.0).await;let Outcome::Ok{data:Data::Operation(record)}=reference.outcome else {panic!("reference expected")};
    assert_eq!(record.kind,"committed_read_reference");assert_eq!(record.state,borg_runtime::read_retention::UNAVAILABLE);
}

#[tokio::test]
async fn released_read_stays_distinct_from_missing_operation() {
    let f=retained_fixture("cold-read").await;
    reserve(&f.c,&f.attempt,1).await;
    let mut read=invocation(&f.actor,&loop_ids::call_id(&f.attempt.id,1),"collab_task_get",json!({"task_id":f.task.id}));
    read.logical_call_id="actual-consumed-native-call".into();
    let original=invoke(&f.c,&read).await;reserve(&f.c,&f.attempt,2).await;
    self::task(report(&f.c,&f.attempt,&loop_ids::completion_id(&f.attempt.id),Observation::NativeLoopCompleted {
        requests:vec![],final_response_index:None,model_requests:2,calls:vec![NativeCallEvidence {request_index:1,call_index:1,operation_id:read.operation_id.clone(),logical_call_id:read.logical_call_id.clone()}],
    }).await);
    close_cancelled(&f).await;
    let plan=release_plan(&f,&[(read.clone(),original.clone())],"cold-custody").await;let plan=custody_and_safe(&f,&plan).await;
    assert!(matches!(release(&f,&plan).await.outcome,Outcome::Ok{..}));
    let terminal=get(&f.c,&f.task.id).await;
    let RetainedFixture{c,cfg,path,..}=f;drop(c);controller_owner_released(&path).await;
    let c=Controller::open_with_config(&path,cfg).unwrap();
    assert_eq!(get(&c,&terminal.id).await,terminal,"canonical Native loop consumer validates the compact original at cold open");
    let replay=invoke(&c,&read).await;
    assert_eq!(replay.commit,original.commit);
    assert!(matches!(replay.outcome,Outcome::Ok{data:Data::OriginalReadBodyUnavailable(_)}));
    let mut missing=read.clone();missing.operation_id="no-such-read".into();
    assert_eq!(code(lookup(&c,&missing).await.outcome),"not_found");
    assert_eq!(mcp::recovered(&read,lookup(&c,&read).await).unwrap_err(),borg_runtime::read_retention::UNAVAILABLE);
}

#[tokio::test]
async fn fresh_volatile_observation_never_rewrites_original_delivery() {
    let f=retained_fixture("fresh-read").await;let (old,first)=large_read(&f,"volatile-read").await;
    let old_reference=borg_runtime::read_retention::ReadReference::new(&old,&mcp::result(&old,first.clone()).unwrap()).unwrap();
    let set=invocation(&f.actor,"volatile-change","collab_memory_set",json!({"key":"volatile-read","expected_revision":1,"value":"changed state"}));
    assert!(matches!(invoke(&f.c,&set).await.outcome,Outcome::Ok{..}));
    let mut fresh=old.clone();fresh.operation_id="explicit-fresh-observation".into();fresh.logical_call_id="new-observation".into();
    let result=invoke(&f.c,&fresh).await;assert_ne!(first.outcome,result.outcome);assert_ne!(first.commit,result.commit);
    assert!(old_reference.restore(&old,mcp::result(&fresh,result.clone()).unwrap()).is_err());
    assert_eq!(invoke(&f.c,&old).await.outcome,first.outcome);
    assert_eq!(ReadEvidence::from_outcome(&old,&first.outcome).unwrap(),old_reference.evidence);
    close_cancelled(&f).await;fresh.operation_id="fresh-after-stop".into();fresh.logical_call_id="fresh-after-stop".into();
    assert!(matches!(invoke(&f.c,&fresh).await.outcome,Outcome::Refused{..}),"fresh observation uses current admission");
}


#[tokio::test]
async fn immutable_reference_keeps_actual_resolved_source() {
    let repo=super::super::repository_calls::Repo::new();
    let f=retained_fixture_with("immutable-release",repo.input("immutable-release"),repo.config(),
        Some(super::super::repository_calls::read_repository_registration())).await;
    let call=invocation(&f.actor,"immutable-body","collab_repository_read",json!({"source":"start","path":"waiting.txt","start_line":1,"max_lines":1}));
    let original=invoke(&f.c,&call).await;
    let Outcome::Ok{data:Data::Repository(body)}=&original.outcome else {panic!("original immutable read failed: {original:?}")};
    assert_eq!(body.source.commit,repo.old);
    let evidence=ReadEvidence::from_outcome(&call,&original.outcome).unwrap();
    close_cancelled(&f).await;
    let plan=release_plan(&f,&[(call.clone(),original.clone())],"immutable-custody").await;let plan=custody_and_safe(&f,&plan).await;
    assert!(matches!(release(&f,&plan).await.outcome,Outcome::Ok{data:Data::TerminalReadRelease(ref value)} if value.released==vec![call.operation_id.clone()]));
    let recovered=mcp::recovered(&call,lookup(&f.c,&call).await).unwrap();
    assert_eq!(recovered.reply.outcome,original.outcome);assert_eq!(recovered.reply.commit,original.commit);
    evidence.verify(&call,&recovered.reply.outcome).unwrap();
    let reference=borg_runtime::read_retention::ReadReference::new(&call,&mcp::result(&call,original.clone()).unwrap()).unwrap();
    let mut changed_commit=recovered.clone();
    changed_commit.reply.commit.as_mut().unwrap().controller_revision+=1;
    assert!(reference.restore(&call,changed_commit).is_err(),"original commit revision cannot change during reconstruction");
    assert_eq!(reference.restore(&call,recovered).unwrap().reply,original);
    let retained=exchange(&f.c,Operation::GetOperation {operation_id:call.operation_id.clone()}).await;
    assert_eq!(retained.commit,original.commit);
    assert!(matches!(retained.outcome,Outcome::Ok{data:Data::Operation(ref record)} if record.kind=="committed_read_reference"),
        "ordinary GetOperation must retain the compact original, not the reconstructed body");
    let mut foreign=call.clone();foreign.caller.incarnation="other-owner".into();
    assert_eq!(code(lookup(&f.c,&foreign).await.outcome),"caller_mismatch");
    // The bare repository already has a newer HEAD. Original recovery is tied
    // to the resolved old commit/blob/range, with no global revision equality.
    assert_ne!(repo.old,repo.new);
    let hidden=repo.root.join("temporarily-unavailable.git");std::fs::rename(&repo.bare,&hidden).unwrap();
    assert_eq!(mcp::recovered(&call,lookup(&f.c,&call).await).unwrap_err(),borg_runtime::read_retention::UNAVAILABLE);
    std::fs::rename(hidden,&repo.bare).unwrap();
    assert_eq!(mcp::recovered(&call,lookup(&f.c,&call).await).unwrap().reply.outcome,original.outcome);
}

#[tokio::test]
async fn compact_read_versions_refuse_old_or_malformed_shapes() {
    let f=retained_fixture("compact-compat").await;let read=large_read(&f,"compat-read").await;close_cancelled(&f).await;
    let plan=release_plan(&f,std::slice::from_ref(&read),"compat-custody").await;let plan=custody_and_safe(&f,&plan).await;
    assert!(matches!(release(&f,&plan).await.outcome,Outcome::Ok{..}));
    let RetainedFixture{c,path,cfg,..}=f;drop(c);controller_owner_released(&path).await;
    let db=rusqlite::Connection::open(&path).unwrap();
    let before:String=db.query_row("SELECT outcome FROM receipts WHERE id=?1",[&read.0.operation_id],|row|row.get(0)).unwrap();
    db.pragma_update(None,"user_version",25).unwrap();drop(db);
    assert!(Controller::open_with_config(&path,cfg.clone()).is_err());
    let db=rusqlite::Connection::open(&path).unwrap();assert_eq!(db.query_row("PRAGMA user_version",[],|row|row.get::<_,i64>(0)).unwrap(),25);
    assert_eq!(db.query_row("SELECT outcome FROM receipts WHERE id=?1",[&read.0.operation_id],|row|row.get::<_,String>(0)).unwrap(),before);
    db.pragma_update(None,"user_version",26).unwrap();
    let mut malformed:serde_json::Value=serde_json::from_str(&before).unwrap();malformed["committed_read_reference"]["release_operation_id"]=json!("absent-original-release");
    db.execute("UPDATE receipts SET outcome=?1 WHERE id=?2",rusqlite::params![serde_json::to_string(&malformed).unwrap(),read.0.operation_id]).unwrap();drop(db);
    assert!(Controller::open_with_config(&path,cfg).is_err(),"cold scanners cannot treat malformed compact receipts as absent unrelated rows");
}

#[path = "read_retention_pages.rs"]
mod pages;
