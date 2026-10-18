//! Owned worker process and controlled HTTP peer; no model, host service or deployment.
use super::*;
use borg_runtime::machine_resources::{Profile,Producer,WorkerBinding};

fn profile(root:&Path)->Profile {
    let mut p:Profile=serde_json::from_str(include_str!("../../metadata/machine-resources.example.json")).unwrap();
    p.local_machine_id="agent-lab-2".into();
    for scope in &mut p.storage_scopes {scope.path=root.into();}
    let bytes=std::fs::read("/usr/bin/stat").unwrap();
    p.stat_helper.sha256=aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256,&bytes).as_ref().iter().map(|n|format!("{n:02x}")).collect();
    p.bounds.sample_interval_ms=80;p.bounds.collection_timeout_ms=60;p.bounds.cleanup_timeout_ms=50;
    p.bounds.report_timeout_ms=1000;p.bounds.stale_after_ms=240;p.bounds.max_window_ms=5000;
    p.validate().unwrap();p
}
#[tokio::test]
async fn host_resources_pending_await_stop_and_failed_report_independent() {
    use bytes::Bytes;
    use http_body_util::{BodyExt,Full};
    use hyper::{Request as HttpRequest,Response as HttpResponse,body::Incoming,service::service_fn};
    use hyper_util::rt::TokioIo;
    use std::{convert::Infallible,process::Stdio};
    let root=area("machine-pending-await");
    let p=profile(&root);let profile_path=root.join("host-profile.json");process::atomic_json(&profile_path,&p).unwrap();
    let selected=write_limits(&root,&limits());
    let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let url=format!("http://{}",listener.local_addr().unwrap());
    let waits=Arc::new(AtomicU64::new(0));let reports=Arc::new(AtomicU64::new(0));let registrations=Arc::new(AtomicU64::new(0));
    let w=waits.clone();let r=reports.clone();let g=registrations.clone();
    let server=tokio::spawn(async move {
        let mut peers=tokio::task::JoinSet::new();
        loop {let (socket,_)=listener.accept().await.unwrap();let w=w.clone();let r=r.clone();let g=g.clone();
            peers.spawn(async move {let service=service_fn(move|request:HttpRequest<Incoming>|{let w=w.clone();let r=r.clone();let g=g.clone();async move{
                let q:Request=serde_json::from_slice(&request.into_body().collect().await.unwrap().to_bytes()).unwrap();
                let reply=match q.command {
                    Operation::GetOperation{..}=>Reply::refused(q.request_id,"not_found","fixture original absent"),
                    Operation::RegisterWorker{operation_id,worker}=>{g.fetch_add(1,Ordering::SeqCst);Reply{request_id:q.request_id,outcome:Outcome::Ok{data:Data::Worker(worker)},commit:Some(MutationCommit{operation_id,controller_revision:1}),observed_controller_revision:Some(1)}},
                    Operation::AwaitCommands{..}=>{w.fetch_add(1,Ordering::SeqCst);std::future::pending::<Reply>().await},
                    Operation::ReportMachineResources{sample}=>{assert_eq!(sample.worker.as_ref().unwrap().worker_id,"agent-lab-2");r.fetch_add(1,Ordering::SeqCst);return Ok::<_,Infallible>(HttpResponse::builder().status(503).body(Full::new(Bytes::new())).unwrap());},
                    other=>panic!("unexpected fixture operation {other:?}"),
                };
                Ok::<_,Infallible>(HttpResponse::new(Full::new(Bytes::from(serde_json::to_vec(&reply).unwrap()))))
            }});let _=hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(socket),service).await;});
        }
    });
    let mut child=tokio::process::Command::new(env!("CARGO_BIN_EXE_borg-runtime")).args(["worker","--limits",selected.to_str().unwrap(),"--controller",&url,"--id","agent-lab-2","--incarnation","one","--workspace-root",root.to_str().unwrap(),"--codex","/usr/bin/false","--host-observation-profile",profile_path.to_str().unwrap()])
        .env_clear().env("PATH","/usr/bin:/bin").env("HOME",&root).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::piped()).kill_on_drop(true).spawn().unwrap();
    let state=root.join(".borg-worker").join(process::component("agent-lab-2"));let journal=state.join("mutations/await.json");
    tokio::time::timeout(Duration::from_secs(5),async{while waits.load(Ordering::SeqCst)!=1 || reports.load(Ordering::SeqCst)<2 {tokio::time::sleep(Duration::from_millis(10)).await;}}).await.expect("worker must report independently while its original await is held");
    let original=std::fs::read(&journal).unwrap();let prior=reports.load(Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(3),async{while reports.load(Ordering::SeqCst)<prior+2 {tokio::time::sleep(Duration::from_millis(10)).await;}}).await.unwrap();
    assert_eq!(std::fs::read(&journal).unwrap(),original,"observation failure must not replace, cancel or consume the original command journal");
    assert_eq!(waits.load(Ordering::SeqCst),1);assert_eq!(registrations.load(Ordering::SeqCst),1);
    let pid=child.id().unwrap();assert!(tokio::process::Command::new("/bin/kill").args(["-INT","--",&pid.to_string()]).status().await.unwrap().success());
    let status=tokio::time::timeout(Duration::from_secs(3),child.wait()).await.expect("stop has priority over observations").unwrap();
    assert!(status.success(),"worker must gracefully stop: {status}");assert!(!state.join("worker-owner.json").exists());
    let count=reports.load(Ordering::SeqCst);tokio::time::sleep(Duration::from_millis(160)).await;assert_eq!(reports.load(Ordering::SeqCst),count,"shutdown joined producer and reporter");
    server.abort();let _=server.await;
}
#[tokio::test]
async fn host_resources_producer_shutdown_single_inflight_and_lost_host() {
    let root=area("machine-producer");let p=Arc::new(profile(&root));
    let active=Arc::new(AtomicU64::new(0));let maximum=Arc::new(AtomicU64::new(0));
    let release=Arc::new(tokio::sync::Notify::new());let (send,mut seen)=tokio::sync::mpsc::unbounded_channel();
    let a=active.clone();let m=maximum.clone();let held=release.clone();
    struct Flight(Arc<AtomicU64>);impl Drop for Flight {fn drop(&mut self){self.0.fetch_sub(1,Ordering::SeqCst);}}
    let producer=Producer::start(p.clone(),Some(WorkerBinding{worker_id:"agent-lab-2".into(),incarnation:"one".into()}),borg_runtime::machine_resources::producer_epoch(p.bounds.kernel_read_bytes).unwrap(),move|sample|{
        let a=a.clone();let m=m.clone();let held=held.clone();let send=send.clone();async move{
            let current=a.fetch_add(1,Ordering::SeqCst)+1;m.fetch_max(current,Ordering::SeqCst);let _flight=Flight(a);send.send(sample.sequence).unwrap();held.notified().await;
        }
    }).unwrap();
    let first=tokio::time::timeout(Duration::from_secs(3),seen.recv()).await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;assert!(seen.try_recv().is_err());assert_eq!(maximum.load(Ordering::SeqCst),1);
    release.notify_one();let newest=tokio::time::timeout(Duration::from_secs(3),seen.recv()).await.unwrap().unwrap();assert!(newest>first+1,"slow report keeps only the latest pending sample");
    tokio::time::timeout(Duration::from_secs(2),producer.shutdown()).await.unwrap().unwrap();assert_eq!(active.load(Ordering::SeqCst),0);
    let closed=seen.recv().await;assert!(closed.is_none(),"stopped/lost producer emits no fabricated fresh sample");
}
