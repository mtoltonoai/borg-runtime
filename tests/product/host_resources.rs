//! Typed controller/UI parity; actual browser coordination is covered by the Node fixture.
use super::*;
use borg_runtime::{collaboration::{Query, QueryResult}, machine_resources::*};
use std::sync::Arc;

async fn fixture() -> (Fixture, Arc<AtomicU64>) {
    let root = std::env::temp_dir().join(format!("borg-machine-product-{}-{}-{}", std::process::id(), now(), NEXT.fetch_add(1,Ordering::Relaxed)));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("tasks.sqlite");
    let mut profile: Profile = serde_json::from_str(include_str!("../../metadata/machine-resources.example.json")).unwrap();
    profile.machines[0].label = "Controller <unsafe> & data".into();
    let mono = Arc::new(AtomicU64::new(100)); let clock=mono.clone();
    let mut cfg=config();
    cfg.machine_resources=Some(borg_runtime::controller::machine_resources::Config{profile:Arc::new(profile),monotonic_ms:Arc::new(move||clock.load(Ordering::SeqCst))});
    let controller=Controller::open_with_config(&path,cfg).unwrap();
    (Fixture{controller,path},mono)
}
fn sample(sequence:u64)->Sample {
    Sample{machine_id:"agent-lab".into(),role:Role::Controller,worker:None,producer_epoch:"11111111-1111-1111-1111-111111111111:1".into(),sequence,
        observed_at_unix_ms:1000,sample_window_ms:Some(5000),
        cpu:Metric::Available{value:CpuUsage{used_core_equivalents:1.0,total_online_logical_cores:4,percent:25.0,busy_ticks:25,total_ticks:100,window_ms:5000}},
        memory:Metric::Available{value:MemoryUsage{used_bytes:3072,total_bytes:4096,available_bytes:1024}},
        filesystems:vec![Filesystem{scope_labels:vec!["root".into(),"borg-state".into(),"workspaces".into()],space:Metric::Available{value:DiskUsage{used_bytes:100,total_bytes:400,free_bytes:300,available_bytes:200}}}]}
}
#[tokio::test]
async fn host_resources_machine_rows_units_stale_and_shared_parity() {
    let (f,mono)=fixture().await;
    let unobserved=ui(&f.controller,"GET","/machines",vec![]).await;
    assert_eq!(unobserved.status,200);assert!(unobserved.body.contains("Unobserved"));
    call(&f.controller,Operation::ReportMachineResources{sample:sample(1)}).await;
    let Data::Collaboration(QueryResult::Machine(view))=call(&f.controller,Operation::QueryCollaboration{query:Query::Machine{machine_id:"agent-lab".into()}}).await else {panic!()};
    assert_eq!(view.freshness,Freshness::Fresh);
    let response=ui(&f.controller,"GET","/machines/agent-lab",vec![]).await;
    assert_eq!(response.status,200);
    for text in ["Controller &lt;unsafe&gt; &amp; data","1.000 core-equivalents / 4 online logical cores","25.00%","3072 / 4096 bytes","1024 available bytes","100 / 400 bytes","200 service-available bytes; 300 free bytes","CPU window: 5000 ms","data-machine-refresh-ms=\"5000\"","root, borg-state, workspaces"] {assert!(response.body.contains(text),"missing {text}");}
    for secret in ["Controller <unsafe>","/var/lib/borg","/usr/bin/stat","producer_epoch"] {assert!(!response.body.contains(secret),"{secret}");}
    mono.store(15100,Ordering::SeqCst);
    let stale=ui(&f.controller,"GET","/machines/agent-lab",vec![]).await;
    assert!(stale.body.contains("Stale · retained sample"));assert!(stale.body.contains("data-machine-age-ms=\"15000\""));
    let mut failed=sample(2);failed.cpu=Metric::WarmingUp{reason:Reason::CounterReset};failed.sample_window_ms=None;failed.memory=Metric::Unavailable{reason:Reason::ReadUnavailable};
    call(&f.controller,Operation::ReportMachineResources{sample:failed}).await;
    let unavailable=ui(&f.controller,"GET","/machines",vec![]).await;
    assert!(unavailable.body.contains("Warming up · counter_reset"));assert!(unavailable.body.contains("Unavailable · read_unavailable"));
    assert_eq!(ui(&f.controller,"GET","/machines/unknown",vec![]).await.status,404);
    let workers=ui(&f.controller,"GET","/workers",vec![]).await;
    assert!(workers.body.contains("/machines"),"machine measurements must be discoverable from worker capacity");
}
#[tokio::test]
async fn host_resources_refresh_without_task_changes_preserves_input() {
    let (f,_)=fixture().await;
    let before=ui(&f.controller,"GET","/ui/changes?after=0",vec![]).await;
    let first=ui(&f.controller,"GET","/machines",vec![]).await;
    call(&f.controller,Operation::ReportMachineResources{sample:sample(1)}).await;
    let second=ui(&f.controller,"GET","/machines",vec![]).await;
    let after=ui(&f.controller,"GET","/ui/changes?after=0",vec![]).await;
    assert_eq!(before.body,after.body,"ephemeral observations do not fabricate task change events");
    assert_ne!(first.body,second.body,"a page read sees the newer sample independently of task revisions");
    assert!(second.body.contains("id=\"machine-resources\""));
    assert!(second.body.contains("data-machine-refresh-ms=\"5000\""));
    // Actual dirty-input preservation, unavailable/reconnect, timer ageing and
    // single-fetch assertions run against live.js in host_resources_live.cjs.
    let script=ui(&f.controller,"GET","/ui/live.js",vec![]).await;
    assert_eq!(script.status,200);assert_eq!(script.body,include_str!("../../src/product/live.js"));
}
