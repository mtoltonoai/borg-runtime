//! Controlled canonical receipts through the real Bridge and durable journal.
use super::*;

fn retained_setup() -> (Area, Config, Fake, Arc<AtomicBool>) {
    let (area, mut cfg, mut fake, cancel) = setup(Mode::LargeRead);
    let state=area.0.join("worker-state");
    super::super::super::handoff::initialize_state(&state, &cfg.worker_limits).unwrap();
    cfg.state = super::super::super::codex::state_path(&state.join("codex"), &cfg.caller.attempt_id);
    process::private_directory(&cfg.state).unwrap();
    process::atomic_json(&cfg.state.join("borg-thread.json"), &json!({"thread_id":"thread-1","turn_id":"turn-1"})).unwrap();
    fake.journal = cfg.directory().join("calls.json");
    (area, cfg, fake, cancel)
}
async fn read(bridge: &mut Bridge<Fake>, id: &str) -> (Value, usize) {
    bridge.invoke("collab_memory_get", json!({"key":"fixture"}), &meta(id)).await.unwrap()
}

#[tokio::test]
async fn delivered_read_reference_reopens_with_original_envelope() {
    let (_area,cfg,fake,cancel)=retained_setup();
    let mut bridge=Bridge::open(cfg.clone(),fake.clone(),vec![],cancel.clone()).unwrap();
    let (wire,index)=read(&mut bridge,"original").await;
    let original=bridge.journal.entries[index].clone();
    bridge.delivered(index).unwrap();
    let before=std::fs::read(&bridge.path).unwrap();
    bridge.compact_delivered().unwrap();
    assert!(std::fs::metadata(&bridge.path).unwrap().len() < before.len() as u64);
    let entry=&bridge.journal.entries[index];
    assert_eq!(entry.invocation,original.invocation);
    assert_eq!((&entry.thread_id,&entry.session_id),(&original.thread_id,&original.session_id));
    assert!(entry.delivered && entry.outcome.as_ref().unwrap().reference().is_some());
    let invocation=entry.invocation.clone();
    drop(bridge);
    fake.state.lock().unwrap().receipts.get_mut(&invocation.operation_id).unwrap().reply.observed_controller_revision=Some(9);
    let mut reopened=Bridge::open(cfg,fake.clone(),vec![],cancel).unwrap();
    let (again,same)=read(&mut reopened,"original").await;
    assert_eq!(same,index); assert_eq!(again,wire);
    assert_eq!(reopened.journal.entries[index].outcome,original.outcome);
    let disk:Journal=decode(&std::fs::read(&reopened.path).unwrap(),reopened.config.limits.journal_bytes).unwrap();
    assert!(disk.entries[index].outcome.as_ref().unwrap().inline().is_some(),"exact original persisted before active delivery");
    assert_eq!(fake.state.lock().unwrap().calls,1);
    assert_eq!(fake.state.lock().unwrap().lookups,1);
}

#[tokio::test]
async fn unresolved_undelivered_boundary_and_write_payloads_remain_inline() {
    let (_area,cfg,fake,cancel)=retained_setup();
    let mut bridge=Bridge::open(cfg.clone(),fake,vec![],cancel).unwrap();
    let (_,index)=read(&mut bridge,"not-delivered").await;
    let before=bridge.journal.clone();bridge.compact_delivered().unwrap();assert_eq!(bridge.journal,before);
    bridge.journal.entries[index].delivered=true;
    // A durable boundary names this exact still-consumed output.
    let gate=MemoryBoundary {revision:1,caller:cfg.caller.clone(),thread_id:"thread-1".into(),
        deadline_unix_ms:cfg.deadline_unix_ms,index,logical_call_id:"not-delivered".into()};
    bounded_save(&cfg.directory().join("memory-boundary.json"),&gate,cfg.limits.boundary_bytes,"boundary").unwrap();
    bridge.save().unwrap();let held=bridge.journal.clone();bridge.compact_delivered().unwrap();assert_eq!(bridge.journal,held);
    std::fs::remove_file(cfg.directory().join("memory-boundary.json")).unwrap();
    bridge.journal.entries[index].invocation.tool="collab_memory_set".into();
    bridge.journal.entries[index].invocation.arguments=args();
    bridge.save().unwrap();let write=bridge.journal.clone();bridge.compact_delivered().unwrap();assert_eq!(bridge.journal,write,"changed=false does not classify writes");
    bridge.journal.entries[index].outcome=None;bridge.journal.entries[index].delivered=false;
    bridge.save().unwrap();let pending=std::fs::read(&bridge.path).unwrap();bridge.compact_delivered().unwrap();assert_eq!(std::fs::read(&bridge.path).unwrap(),pending);
}

#[tokio::test]
async fn reference_lookup_corruption_is_not_absence_or_send_authority() {
    for corruption in 0..4 {
        let (_area,cfg,fake,cancel)=retained_setup();let mut bridge=Bridge::open(cfg,fake.clone(),vec![],cancel).unwrap();
        let (_,index)=read(&mut bridge,"reference").await;bridge.delivered(index).unwrap();bridge.compact_delivered().unwrap();
        let saved=std::fs::read(&bridge.path).unwrap();let id=bridge.journal.entries[index].invocation.operation_id.clone();
        let mut state=fake.state.lock().unwrap();
        if corruption==0 {state.receipts.remove(&id);} else {
            let result=state.receipts.get_mut(&id).unwrap();match corruption {
                1=>result.reply.commit.as_mut().unwrap().controller_revision+=1,
                2=>result.reply.outcome=Outcome::Ok {data:Data::Acknowledged},
                _=>result.requires_quiescence=true,
            }
        }drop(state);
        assert!(bridge.invoke("collab_memory_get",json!({"key":"fixture"}),&meta("reference")).await.is_err());
        assert_eq!(fake.state.lock().unwrap().calls,1);
        assert_eq!(std::fs::read(&bridge.path).unwrap(),saved);
        assert!(bridge.journal.entries[index].outcome.as_ref().unwrap().reference().is_some());
    }
}

#[tokio::test]
async fn journal_reference_save_failure_preserves_barriers() {
    let (_area,cfg,fake,cancel)=retained_setup();let mut bridge=Bridge::open(cfg,fake.clone(),vec![],cancel).unwrap();
    let (_,index)=read(&mut bridge,"save-failure").await;bridge.delivered(index).unwrap();
    let before=std::fs::read(&bridge.path).unwrap();let original=bridge.journal.clone();
    let _fault=process::storage_tests::inject(&bridge.path,process::storage_tests::Point::FileSync,5,0);
    assert!(bridge.compact_delivered().is_err());
    assert_eq!(bridge.journal,original);assert_eq!(std::fs::read(&bridge.path).unwrap(),before);
    assert_eq!(fake.state.lock().unwrap().calls,1);
}
