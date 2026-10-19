//! Real controller and worker processes, local Git, and a controlled coding peer.
//! No provider calls; cleanup is reached only through the ordinary worker loop.
use super::*;
use borg_runtime::{client::ApiClient, collaboration::{Query, QueryResult}};
use serde_json::{Value, json};
use std::{collections::BTreeMap, os::unix::fs::PermissionsExt, process::{Child, Command, Stdio}, sync::Mutex};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request as HttpRequest, Response, body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;

// Same owned process/finite signal-and-reap pattern as worker::task_recovery.
struct Process { child: Child, closed: PathBuf }
impl Process {
    fn start(image: &Path, args: &[String], root: &Path, label: &str) -> Self {
        let log = std::fs::File::create(root.join(format!("{label}.log"))).unwrap();
        let child = Command::new(image).args(args).env_clear()
            .env("PATH", "/usr/bin:/bin").env("HOME", root)
            .env("OPENAI_API_KEY", "synthetic-cleanup-fixture-key")
            .stdin(Stdio::null()).stdout(log.try_clone().unwrap()).stderr(log).spawn().unwrap();
        Self { child, closed: root.join(format!("{label}-closed.json")) }
    }
    fn live(&mut self) { assert!(self.child.try_wait().unwrap().is_none(), "fixture process exited early"); }
    async fn stop(&mut self) {
        if self.child.try_wait().unwrap().is_none() {
            assert!(Command::new("/usr/bin/kill").args(["-INT", &self.child.id().to_string()]).status().unwrap().success());
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() { assert!(status.success(), "fixture shutdown: {status}");break; }
            assert!(tokio::time::Instant::now() < deadline, "fixture shutdown timed out");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = Command::new("/usr/bin/kill").args(["-INT", &self.child.id().to_string()]).status();
            let until = std::time::Instant::now() + Duration::from_secs(5);
            while self.child.try_wait().ok().flatten().is_none() && std::time::Instant::now() < until {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        let _ = self.child.kill();let waited = self.child.wait();
        let _ = std::fs::write(&self.closed, serde_json::to_vec(&json!({"pid":self.child.id(),
            "reaped":waited.is_ok(),"exit_code":waited.as_ref().ok().and_then(|s|s.code())})).unwrap());
    }
}

struct CleanupReadGate {
    task_id: String,
    held: AtomicU64,
    worker_state: PathBuf,
    evidence: PathBuf,
    pending_commands: Mutex<Vec<String>>,
    uncommitted_wait: Mutex<Option<String>>,
    released: std::sync::atomic::AtomicBool,
    wake: tokio::sync::Notify,
}

// Match the actual completed local command and retired publication before
// holding a terminal GetTask. Admission/finalization reads are not cleanup.
fn completed_cleanup_command(gate: &CleanupReadGate, task: &Task) -> Option<Value> {
    let attempt=task.attempt.as_ref()?;
    let phase:Value=serde_json::from_slice(&std::fs::read(gate.worker_state.join("publications")
        .join(format!("{}.json",process::component(&attempt.id)))).ok()?).ok()?;
    if phase["retired_for_checks"]!=true {return None;}
    let command_id=phase["command"]["command_id"].as_str()?;
    let receipt:Value=serde_json::from_slice(&std::fs::read(gate.worker_state.join("commands")
        .join(format!("{}.json",process::component(command_id)))).ok()?).ok()?;
    (receipt["completed"]==true && receipt["effects_started"]==true
        && receipt["command"]==phase["command"]
        && receipt["command"]["attempt"]["id"]==attempt.id
        && receipt["command"]["attempt"]["task_id"]==task.id).then_some(receipt)
}

// A single original checkpoint settlement is delayed, never replaced or retried
// by this fixture. All replies still come from the actual controller.
struct AckProxy {
    url: String,
    pending: Arc<Mutex<Option<Operation>>>,
    sends: Arc<AtomicU64>,
    release: Arc<tokio::sync::Semaphore>,
    owner: tokio::task::JoinHandle<()>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}
impl AckProxy {
    async fn start(upstream: String, bound: usize) -> Self {
        Self::start_with_cleanup_read(upstream, bound, None).await
    }
    async fn start_with_cleanup_read(upstream: String, bound: usize, cleanup: Option<Arc<CleanupReadGate>>) -> Self {
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url=format!("http://{}",listener.local_addr().unwrap());
        let pending=Arc::new(Mutex::new(None));let sends=Arc::new(AtomicU64::new(0));
        let release=Arc::new(tokio::sync::Semaphore::new(0));
        let (p,n,g)=(pending.clone(),sends.clone(),release.clone());
        let (shutdown,mut shutdown_rx)=tokio::sync::oneshot::channel();
        let owner=tokio::spawn(async move {
            let client=reqwest::Client::builder().no_proxy().redirect(reqwest::redirect::Policy::none()).build().unwrap();
            let mut connections=tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _=&mut shutdown_rx=>{
                        connections.abort_all();
                        while let Some(joined)=connections.join_next().await {
                            if let Err(error)=joined {
                                assert!(error.is_cancelled(),"proxy connection task failed: {error}");
                            }
                        }
                        break;
                    }
                    accepted=listener.accept()=>{
                        let (stream,_)=accepted.unwrap();let (client,upstream,p,n,g,cleanup)=(client.clone(),upstream.clone(),p.clone(),n.clone(),g.clone(),cleanup.clone());
                        connections.spawn(async move {
                            let service=service_fn(move |request:HttpRequest<Incoming>| {
                                let (client,upstream,p,n,g,cleanup)=(client.clone(),upstream.clone(),p.clone(),n.clone(),g.clone(),cleanup.clone());
                                async move {
                                    let route=request.uri().path().to_owned();assert!(matches!(route.as_str(),"/api/operations"|"/mcp"));
                                    let mut headers=reqwest::header::HeaderMap::new();
                                    for name in ["content-type","accept","mcp-protocol-version","x-borg-caller"] {
                                        if let Some(value)=request.headers().get(name) {headers.insert(name,value.clone());}
                                    }
                                    let bytes=request.into_body().collect().await.unwrap().to_bytes();assert!(bytes.len()<=bound);
                                    let mut cleanup_lookup=false;let mut await_operation=None;
                                    if route=="/api/operations" {
                                        let request:Request=serde_json::from_slice(&bytes).unwrap();
                                        await_operation=match &request.command {
                                            Operation::AwaitCommands{operation_id,acknowledged,..}=>Some((operation_id.clone(),acknowledged.is_empty())),
                                            _=>None,
                                        };
                                        cleanup_lookup=cleanup.as_ref().is_some_and(|gate| matches!(&request.command,Operation::GetTask{task_id} if task_id==&gate.task_id));
                                        if cleanup.is_none() && matches!(&request.command,Operation::SettleOperation {checkpoint:Some(_),..}) {
                                            assert_eq!(n.fetch_add(1,Ordering::SeqCst),0,"original settlement must not repeat");
                                            *p.lock().unwrap()=Some(request.command.clone());
                                            tokio::time::timeout(Duration::from_secs(20),g.acquire()).await.unwrap().unwrap().forget();
                                        }
                                    }
                                    let response=client.post(format!("{upstream}{route}")).headers(headers).body(bytes.to_vec()).send().await.map_err(std::io::Error::other)?;
                                    let status=response.status();let content_type=response.headers().get("content-type").cloned();
                                    let body=response.bytes().await.map_err(std::io::Error::other)?;assert!(body.len()<=bound);
                                    if let Some((expected,no_ack))=await_operation {
                                        if let Some(gate)=cleanup.as_ref().filter(|gate|gate.held.load(Ordering::SeqCst)>0&&!gate.released.load(Ordering::SeqCst)) {
                                            let reply:Reply=serde_json::from_slice(&body).unwrap();
                                            if let Outcome::Pending{operation_id}=&reply.outcome {
                                                assert_eq!(operation_id,&expected);assert!(reply.commit.is_none());
                                                let mut current=gate.uncommitted_wait.lock().unwrap();
                                                if let Some(original)=current.as_ref(){assert_eq!(original,operation_id,"idle wait identity changed before a committed reply");}
                                                *current=Some(operation_id.clone());
                                                if no_ack {
                                                    let mut pending=gate.pending_commands.lock().unwrap();
                                                    // Preserve the first complete three-Pending oracle.
                                                    if pending.len()<3 {
                                                        pending.push(operation_id.clone());
                                                        std::fs::write(gate.evidence.join("cleanup-pending-waits.json"),serde_json::to_vec(&json!({
                                                            "original_pending_operations":*pending,"held_cleanup_reads":gate.held.load(Ordering::SeqCst)
                                                        })).unwrap()).unwrap();
                                                    }
                                                }
                                            } else if let Some(commit)=reply.commit.as_ref() {
                                                assert_eq!(commit.operation_id,expected);
                                                let mut current=gate.uncommitted_wait.lock().unwrap();
                                                if let Some(original)=current.as_ref(){assert_eq!(original,&expected,"committed reply changed the pending wait identity");}
                                                // A settled wait (including StopAttempt delivery) permits
                                                // the worker's next logical AwaitCommands identity.
                                                *current=None;
                                                let mut pending=gate.pending_commands.lock().unwrap();
                                                if pending.len()<3 {pending.clear();}
                                            }
                                        }
                                    }
                                    if cleanup_lookup {
                                        let reply:Reply=serde_json::from_slice(&body).unwrap();
                                        let gate=cleanup.as_ref().unwrap();
                                        let completed=match &reply.outcome {
                                            Outcome::Ok{data:Data::Task(task)} if task.id==gate.task_id && task.status==TaskStatus::Succeeded=>completed_cleanup_command(gate,task),
                                            _=>None,
                                        };
                                        if let Some(completed)=completed.filter(|_|!gate.released.load(Ordering::SeqCst)) {
                                            gate.held.fetch_add(1,Ordering::SeqCst);
                                            std::fs::write(gate.evidence.join("cleanup-held-command.json"),serde_json::to_vec(&completed).unwrap()).unwrap();
                                            tokio::time::timeout(Duration::from_secs(20),async {
                                                loop {let wait=gate.wake.notified();if gate.released.load(Ordering::SeqCst) {break;}wait.await;}
                                            }).await.expect("cleanup response gate expired");
                                        }
                                    }
                                    let mut reply=Response::builder().status(status);if let Some(value)=content_type {reply=reply.header("content-type",value);}
                                    Ok::<_,std::io::Error>(reply.body(Full::new(Bytes::from(body))).unwrap())
                                }
                            });
                            let _=hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream),service).await;
                        });
                    }
                    joined=connections.join_next(),if !connections.is_empty()=>{
                        joined.expect("proxy connection task exists").expect("proxy connection task failed");
                    }
                }
            }
        });
        Self {url,pending,sends,release,owner,shutdown:Some(shutdown)}
    }
    async fn stop(&mut self) {
        if let Some(shutdown)=self.shutdown.take() {let _=shutdown.send(());}
        (&mut self.owner).await.expect("proxy owner task failed");
    }
}
impl Drop for AckProxy {fn drop(&mut self) {self.owner.abort();}}

async fn call(api: &ApiClient, op: Operation) -> Reply { Box::pin(api.call(op)).await.unwrap() }
async fn task(api: &ApiClient, id: &str) -> Task {
    match call(api, Operation::GetTask { task_id: id.into() }).await.outcome {
        Outcome::Ok { data: Data::Task(task) } => *task,
        other => panic!("canonical task: {other:?}"),
    }
}
async fn ready(api: &ApiClient, owner: &mut Process) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        owner.live();
        if matches!(api.call(Operation::ListTasks {after:None,limit:1}).await,
            Ok(Reply { outcome:Outcome::Ok{..},.. })) { break; }
        assert!(tokio::time::Instant::now() < deadline, "controller did not become ready");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
fn journal_bytes(state: &Path, attempt: &Attempt, command: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut paths = vec![command.to_owned(),state.join("publications").join(format!("{}.json",process::component(&attempt.id)))];
    let codex = codex::state_path(&state.join("codex"), &attempt.id);
    for name in ["borg-thread.json", "borg-collaboration-config.json", "borg-usage.json"] { paths.push(codex.join(name)); }
    paths.into_iter().map(|p| {let bytes=std::fs::read(&p).unwrap();(p,bytes)}).collect()
}
fn same_journals(expected: &BTreeMap<PathBuf, Vec<u8>>) {
    for (path, bytes) in expected { assert_eq!(&std::fs::read(path).unwrap(),bytes,"retained {}",path.display()); }
}

#[tokio::test]
async fn worker_loop_automatically_reclaims_accepted_material_and_restart_preserves_receipts() {
    //= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
    //# A worker MUST reclaim eligible disposable workspaces and local repository caches without an operator command.
    //= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
    //# Cleanup MUST preserve durable task results, histories, checkpoints, audit metadata, operation receipts and recovery journals in the initial profile.
    let root=area("automatic-cleanup-lifecycle");
    let mut policy=(*limits()).clone();
    policy.worker.coding_capacity=1;policy.worker.native_capacity=0;
    policy.controller.command_wait_ms=25;
    policy.retention=Some(borg_runtime::limits::RetentionLimits{worker:borg_runtime::limits::WorkerRetentionLimits{
        success_grace_ms:0,failed_grace_ms:86_400_000,cancelled_grace_ms:86_400_000,
        idle_cache_grace_ms:604_800_000,batch_entries:32,filesystem_entries_per_yield:32,
        operation_timeout_ms:30_000,retry_delay_ms:10,record_bytes:1_048_576,
    }});
    policy.validate().unwrap();let policy=Arc::new(policy);let selected=write_limits(&root,&policy);
    let image=root.join("runtime");retained_images::materialize(Path::new(env!("CARGO_BIN_EXE_borg-runtime")),&image,false);
    let mut repository=repository(&root);
    let seed=root.join("seed");std::fs::write(seed.join(".gitignore"),"target/\n").unwrap();
    run_git(&seed,&["add",".gitignore"]);run_git(&seed,&["commit","-qm","ignore disposable build outputs"]);
    repository.start_commit=run_git(&seed,&["rev-parse","HEAD"]);
    run_git(&seed,&["push",&repository.url,"HEAD:refs/heads/cleanup-fixture"]);
    let remote=root.join("remote.git");
    let registration=borg_runtime::repository_catalogue::Registration {
        pull_request_target: None,
        repository_id:"cleanup-lifecycle".into(),registration_revision:"fixture-1".into(),
        display_name:"Controlled local cleanup lifecycle".into(),source_endpoint:format!("file://{}",remote.display()),
        checkpoint_endpoint:remote.to_string_lossy().into_owned(),source_store_key:"cleanup-source".into(),checkpoint_store_key:"cleanup-checkpoint".into(),
    };
    let fake=root.join("controlled-codex");
    let script=r#"#!/usr/bin/python3
import json,os,pathlib,sys
root=pathlib.Path(__file__).parent
for line in sys.stdin:
 m=json.loads(line); method=m.get('method')
 def send(v): print(json.dumps(v),flush=True)
 if method=='initialize': send({'id':m['id'],'result':{'userAgent':'controlled-cleanup-peer'}})
 elif method=='command/exec': send({'id':m['id'],'result':{'exitCode':0,'stdout':'','stderr':''}})
 elif method=='thread/start':
  home=pathlib.Path(os.environ['CODEX_HOME'])
  (home/'.tmp/plugins/.git/objects').mkdir(parents=True)
  (home/'.tmp/plugins/.git/objects/fixture.pack').write_bytes(b'reconstructible plugin fixture')
  (home/'sessions').mkdir()
  (home/'sessions/fixture-transcript.jsonl').write_text('retained transcript fixture\n')
  (home/'fixture-vendor.sqlite').write_bytes(b'opaque vendor state fixture')
  assert m['params']['model']=='gpt-6-astra' and m['params']['config']['model_reasoning_effort']=='max'
  send({'id':m['id'],'result':{'thread':{'id':'cleanup-thread'},'model':'gpt-6-astra','reasoningEffort':'max'}})
 elif method=='thread/inject_items': send({'id':m['id'],'result':{}})
 elif method=='turn/start':
  with (root/'turns.txt').open('a') as f: f.write('one\n'); f.flush()
  pathlib.Path('answer.txt').write_text('after\n')
  pathlib.Path('target').mkdir(exist_ok=True); pathlib.Path('target/executor.bin').write_text('disposable executor build\n')
  send({'id':m['id'],'result':{'turn':{'id':'cleanup-turn','status':'inProgress'}}})
  send({'method':'thread/tokenUsage/updated','params':{'threadId':'cleanup-thread','turnId':'cleanup-turn','tokenUsage':{'total':{'inputTokens':10,'outputTokens':2}}}})
  send({'method':'item/completed','params':{'item':{'type':'agentMessage','text':'controlled source ready'}}})
  send({'method':'turn/completed','params':{'threadId':'cleanup-thread','turn':{'id':'cleanup-turn','status':'completed'}}})
 elif 'id' in m: raise AssertionError('unexpected fixture method '+str(method))
"#;
    std::fs::write(&fake,script).unwrap();std::fs::set_permissions(&fake,std::fs::Permissions::from_mode(0o700)).unwrap();
    let listener=std::net::TcpListener::bind("127.0.0.1:0").unwrap();let address=listener.local_addr().unwrap().to_string();drop(listener);
    let origin=format!("http://{address}");let api=ApiClient::new(&origin,policy.clone()).unwrap();
    let controller_args=vec!["controller".into(),"--limits".into(),selected.display().to_string(),"--db".into(),root.join("tasks.sqlite").display().to_string(),"--listen".into(),address,"--origin".into(),origin.clone()];
    let mut controller=Process::start(&image,&controller_args,&root,"controller-1");ready(&api,&mut controller).await;
    assert!(matches!(call(&api,Operation::RegisterRepository{operation_id:"cleanup-register".into(),registration:registration.clone()}).await.outcome,Outcome::Ok{..}));
    let acceptance=r#"import pathlib,sys,time
root=pathlib.Path(sys.argv[1]); assert pathlib.Path('answer.txt').read_text()=='after\n'
pathlib.Path('target').mkdir(exist_ok=True);pathlib.Path('target/acceptance.bin').write_text('disposable acceptance build\n')
(root/'acceptance-ready').write_text('ready')
until=time.monotonic()+30
while not (root/'release-acceptance').exists():
 assert time.monotonic()<until,'fixture acceptance release expired'
 time.sleep(.01)
"#;
    let input=TaskInput { instruction_source: None,
        pull_request: None,
        resolved_limits:None,repository_read:None,instruction:"Update answer.txt to after, then return. This is a controlled local cleanup acceptance.".into(),
        definition:Definition{charter: None, id:"cleanup-agent".into(),revision:"fixture-1".into(),executor:ExecutorKind::Coding,inference:Some(InferenceSettings::selected()),
            execution_profile:Some(ExecutionProfile::CollaborationV1{web_search:None}),limit_overrides:None},
        repository:Some(Repository{ pull_request_target: None, registration:Some(registration.binding()),url:String::new(),start_commit:repository.start_commit.clone(),checkpoint_remote:String::new()}),
        acceptance_commands:vec![vec!["/usr/bin/python3".into(),"-c".into(),acceptance.into(),root.display().to_string()]],output_schema:None,
        limits:Limits{deadline_unix_ms:process::now_ms()+120_000,model_requests:2,output_bytes:16_384,result_bytes:policy.task_defaults.result_bytes,max_delegation_depth:4},
        dependencies:vec![],native_workflow:None,
    };
    let submitted=call(&api,Operation::SubmitTask{operation_id:"cleanup-submit".into(),input: (input).into()}).await;
    let Outcome::Ok{data:Data::Task(submitted)}=submitted.outcome else {panic!("submit refused")};
    let mut proxy=AckProxy::start(origin.clone(),policy.transport.body_bytes).await;
    let worker_root=root.join("worker");let worker_id="automatic-cleanup-worker";
    let worker_args=|incarnation:&str|vec!["worker".into(),"--limits".into(),selected.display().to_string(),"--controller".into(),proxy.url.clone(),
        "--id".into(),worker_id.into(),"--incarnation".into(),incarnation.into(),"--workspace-root".into(),worker_root.display().to_string(),
        "--codex".into(),fake.display().to_string(),"--coding-capacity".into(),"1".into(),"--native-capacity".into(),"0".into()];
    let mut worker=Process::start(&image,&worker_args("incarnation-1"),&root,"worker-1");
    let deadline=tokio::time::Instant::now()+Duration::from_secs(60);
    let original_settlement=loop {
        worker.live();
        if let Some(op)=proxy.pending.lock().unwrap().clone() {break op;}
        let observed=task(&api,&submitted.id).await;
        assert!(!matches!(observed.status,TaskStatus::Failed|TaskStatus::Cancelled),"before publication ACK: {observed:?}");
        assert!(tokio::time::Instant::now()<deadline,"original settlement was not dispatched");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let Operation::SettleOperation {operation_id: original_publication,checkpoint:Some(original_checkpoint),..}=&original_settlement else {panic!("exact original checkpoint settlement required")};
    let pending_task=task(&api,&submitted.id).await;
    let pending_work=worker_root.join("workspaces").join(process::component(&pending_task.attempt.as_ref().unwrap().id));
    assert!(pending_work.join("answer.txt").is_file());assert!(pending_work.join("target/executor.bin").is_file());
    assert_ne!(pending_task.status,TaskStatus::Succeeded);
    let pending_receipt=call(&api,Operation::GetOperation{operation_id:original_publication.clone()}).await;
    assert!(matches!(pending_receipt.outcome,Outcome::Ok{data:Data::Operation(record)} if record.state=="pending" && record.outcome.is_none()));
    assert_eq!(run_git(&remote,&["rev-parse",&original_checkpoint.remote_ref]),original_checkpoint.commit);
    // Release only the held original acknowledgement. No task or cleanup request
    // is submitted; the worker continues its normal publication/acceptance loop.
    proxy.release.add_permits(1);
    while !root.join("acceptance-ready").exists() {
        worker.live();controller.live();
        let observed=task(&api,&submitted.id).await;
        assert!(!matches!(observed.status,TaskStatus::Failed|TaskStatus::Cancelled),"before acceptance: {observed:?}");
        assert!(tokio::time::Instant::now()<deadline,"acceptance barrier not reached");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let in_flight=task(&api,&submitted.id).await;let attempt=in_flight.attempt.as_ref().unwrap().clone();
    let work=worker_root.join("workspaces").join(process::component(&attempt.id));
    let checked=worker_root.join("validation").join(process::component(&attempt.id));
    assert!(work.join("target/executor.bin").is_file());assert!(checked.join("target/acceptance.bin").is_file());
    assert_ne!(in_flight.status,TaskStatus::Succeeded);
    let state=worker_root.join(".borg-worker").join(process::component(worker_id));
    let command=std::fs::read_dir(state.join("commands")).unwrap().map(|e|e.unwrap().path()).find(|p|{
        let v:Value=serde_json::from_slice(&std::fs::read(p).unwrap()).unwrap();v["command"]["kind"]=="run_attempt"
    }).unwrap();
    let saved_command:Value=serde_json::from_slice(&std::fs::read(&command).unwrap()).unwrap();
    assert_eq!(saved_command["command"]["attempt"]["id"],attempt.id);
    let cleanup=state.join("cleanup").join(format!("{}.json",process::component(&attempt.id)));
    let codex_home=state.join("codex").join(process::component(&attempt.id));
    let plugin_tree=codex_home.join(".tmp/plugins");
    let plugin_progress=state.join("cleanup-codex").join(format!("{}.json",process::component(&attempt.id)));
    assert!(plugin_tree.join(".git/objects/fixture.pack").is_file(), "held acceptance retains attempt plugins");
    let transcript_bytes=std::fs::read(codex_home.join("sessions/fixture-transcript.jsonl")).unwrap();
    let retained_transcript=state.join("codex-transcripts").join(process::component(&attempt.id)).join("sessions/fixture-transcript.jsonl");
    assert!(codex_home.join("fixture-vendor.sqlite").is_file());
    // Executor has stopped at the existing acceptance barrier. These original
    // home journals are stable before cleanup; command/publication receipts
    // legitimately finish settling below and keep their later baseline.
    let home_journals=["borg-thread.json","borg-usage.json","borg-collaboration-config.json"]
        .map(|name|(codex_home.join(name),std::fs::read(codex_home.join(name)).unwrap()));

    std::fs::write(root.join("release-acceptance"),"release").unwrap();
    let accepted=loop {
        worker.live();let observed=task(&api,&submitted.id).await;
        if observed.status==TaskStatus::Succeeded {break observed;}
        assert!(!matches!(observed.status,TaskStatus::Failed|TaskStatus::Cancelled),"acceptance failed: {observed:?}");
        assert!(tokio::time::Instant::now()<deadline,"accepted result absent");tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let cp=accepted.selected_checkpoint.clone().unwrap();
    assert_eq!(&cp,original_checkpoint);assert_eq!(proxy.sends.load(Ordering::SeqCst),1);
    assert!(accepted.attempt.as_ref().unwrap().stopped);assert!(!accepted.attempt.as_ref().unwrap().uncertain);
    let checkpoint_op=call(&api,Operation::GetOperation{operation_id:cp.operation_id.clone()}).await;
    assert!(matches!(&checkpoint_op.outcome,Outcome::Ok{data:Data::Operation(record)} if record.state=="settled" && record.outcome.as_ref().is_some_and(|v|v["acknowledged"]==true)));
    let history=Operation::QueryCollaboration{query:Query::TaskHistory{task_id:accepted.id.clone(),after:None,limit:100}};
    let original_history=call(&api,history.clone()).await;
    loop {
        worker.live();
        let complete=std::fs::read(&cleanup).ok().and_then(|b|serde_json::from_slice::<Value>(&b).ok())
            .is_some_and(|v|v["worktrees"].as_array().is_some_and(|slots|slots.len()==2&&slots.iter().all(|s|s["done"]==true)));
        let plugins_complete=std::fs::read(&plugin_progress).ok().and_then(|b|serde_json::from_slice::<Value>(&b).ok())
            .is_some_and(|v|v["done"]==true);
        if complete&&plugins_complete&&!work.exists()&&!checked.exists()&&!plugin_tree.exists() {break;}
        assert!(tokio::time::Instant::now()<deadline,"worker loop did not reclaim acknowledged material");tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let retained=journal_bytes(&state,&attempt,&command);
    for (path,bytes) in &home_journals {assert_eq!(&std::fs::read(path).unwrap(),bytes,"original Borg home journal changed during copyout");}
    assert_eq!(std::fs::read(&retained_transcript).unwrap(),transcript_bytes);
    assert!(!codex_home.join("fixture-vendor.sqlite").exists());
    assert!(!codex_home.join("sessions").exists());
    let plugin_progress_bytes=std::fs::read(&plugin_progress).unwrap();
    assert_eq!(task(&api,&accepted.id).await,accepted);
    assert_eq!(call(&api,history.clone()).await.outcome,original_history.outcome);
    assert_eq!(run_git(&remote,&["rev-parse",&cp.remote_ref]),cp.commit);
    assert_eq!(std::fs::read_to_string(root.join("turns.txt")).unwrap(),"one\n");
    worker.stop().await;controller.stop().await;
    let mut controller2=Process::start(&image,&controller_args,&root,"controller-2");ready(&api,&mut controller2).await;
    let mut worker2=Process::start(&image,&worker_args("incarnation-2"),&root,"worker-2");
    let restart_deadline=tokio::time::Instant::now()+Duration::from_secs(30);
    loop {
        worker2.live();let reply=call(&api,Operation::QueryCollaboration{query:Query::Worker{worker_id:worker_id.into()}}).await;
        if matches!(reply.outcome,Outcome::Ok{data:Data::Collaboration(QueryResult::Worker(view))} if view.worker.incarnation=="incarnation-2") {break;}
        assert!(tokio::time::Instant::now()<restart_deadline,"restarted worker did not complete registration");tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(task(&api,&accepted.id).await,accepted);same_journals(&retained);
    let after=call(&api,Operation::GetOperation{operation_id:cp.operation_id.clone()}).await;
    assert_eq!(after.outcome,checkpoint_op.outcome);assert_eq!(after.commit,checkpoint_op.commit);
    assert_eq!(call(&api,history).await.outcome,original_history.outcome);
    assert!(!work.exists()&&!checked.exists());assert_eq!(run_git(&remote,&["rev-parse",&cp.remote_ref]),cp.commit);
    assert_eq!(std::fs::read_to_string(root.join("turns.txt")).unwrap(),"one\n");
    let attempts=call(&api,Operation::QueryCollaboration{query:Query::TaskAttempts{task_id:accepted.id.clone(),after:None,limit:100}}).await;
    assert!(matches!(attempts.outcome,Outcome::Ok{data:Data::Collaboration(QueryResult::TaskAttempts(page))} if page.items.len()==1&&page.next_cursor.is_none()));
    worker2.stop().await;controller2.stop().await;same_journals(&retained);
    for (path,bytes) in &home_journals {assert_eq!(&std::fs::read(path).unwrap(),bytes);}
    assert!(!plugin_tree.exists());assert_eq!(std::fs::read(&plugin_progress).unwrap(),plugin_progress_bytes);
    assert_eq!(std::fs::read(&retained_transcript).unwrap(),transcript_bytes);
    assert!(!codex_home.join("fixture-vendor.sqlite").exists());
    assert!(!codex_home.join("sessions").exists());
    assert_eq!(proxy.sends.load(Ordering::SeqCst),1);proxy.stop().await;
}

fn proc_start(pid: u32) -> Option<String> {
    std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()
        .and_then(|s|s.rsplit_once(") ").and_then(|(_,tail)|tail.split_whitespace().nth(19)).map(str::to_owned))
}

#[tokio::test]
async fn worker_cleanup_read_cannot_block_another_active_attempt_stop() {
    //= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
    //# Cleanup MUST use bounded work under its configured metadata policy.
    // Scope: real worker control-loop responsiveness while one canonical cleanup
    // read is deliberately held. This does not establish every cleanup latency.
    let root=area("automatic-cleanup-stop-liveness");
    let mut policy=(*limits()).clone();policy.worker.coding_capacity=2;policy.worker.native_capacity=0;
    policy.controller.command_wait_ms=25;
    policy.retention=Some(borg_runtime::limits::RetentionLimits{worker:borg_runtime::limits::WorkerRetentionLimits{
        success_grace_ms:0,failed_grace_ms:86_400_000,cancelled_grace_ms:86_400_000,
        idle_cache_grace_ms:604_800_000,batch_entries:32,filesystem_entries_per_yield:32,
        operation_timeout_ms:30_000,retry_delay_ms:10,record_bytes:1_048_576,
    }});
    policy.validate().unwrap();let policy=Arc::new(policy);let selected=write_limits(&root,&policy);
    let image=root.join("runtime");retained_images::materialize(Path::new(env!("CARGO_BIN_EXE_borg-runtime")),&image,false);
    let repository=repository(&root);let remote=root.join("remote.git");
    let registration=borg_runtime::repository_catalogue::Registration {
        pull_request_target: None,
        repository_id:"cleanup-stop".into(),registration_revision:"fixture-1".into(),display_name:"Controlled cleanup stop".into(),
        source_endpoint:format!("file://{}",remote.display()),checkpoint_endpoint:remote.to_string_lossy().into_owned(),
        source_store_key:"cleanup-stop-source".into(),checkpoint_store_key:"cleanup-stop-checkpoint".into(),
    };
    let fake=root.join("controlled-codex");
    let script=r#"#!/usr/bin/python3
import json,os,pathlib,sys,time
root=pathlib.Path(__file__).parent
thread='cleanup-'+pathlib.Path.cwd().name
for line in sys.stdin:
 m=json.loads(line);method=m.get('method')
 def send(v):print(json.dumps(v),flush=True)
 if method=='initialize':send({'id':m['id'],'result':{'userAgent':'controlled-cleanup-peer'}})
 elif method=='command/exec':send({'id':m['id'],'result':{'exitCode':0,'stdout':'','stderr':''}})
 elif method=='thread/start':send({'id':m['id'],'result':{'thread':{'id':thread},'model':'gpt-6-astra','reasoningEffort':'max'}})
 elif method=='thread/inject_items':send({'id':m['id'],'result':{}})
 elif method=='turn/start':
  slow='slow-cleanup' in m['params']['input'][0]['text']
  label='slow' if slow else 'fast'
  (root/(label+'-started')).write_text(str(os.getpid()))
  send({'id':m['id'],'result':{'turn':{'id':'turn-'+label,'status':'inProgress'}}})
  if slow:continue
  until=time.monotonic()+30
  while not (root/'allow-fast-completion').exists():
   assert time.monotonic()<until,'fast completion barrier expired'
   time.sleep(.01)
  pathlib.Path('answer.txt').write_text('after\n')
  send({'method':'item/completed','params':{'item':{'type':'agentMessage','text':'controlled fast completion'}}})
  send({'method':'turn/completed','params':{'threadId':thread,'turn':{'id':'turn-fast','status':'completed'}}})
 elif method=='turn/interrupt':send({'id':m['id'],'result':{}})
 elif 'id' in m:raise AssertionError('unexpected method '+str(method))
"#;
    std::fs::write(&fake,script).unwrap();std::fs::set_permissions(&fake,std::fs::Permissions::from_mode(0o700)).unwrap();
    let listener=std::net::TcpListener::bind("127.0.0.1:0").unwrap();let address=listener.local_addr().unwrap().to_string();drop(listener);
    let origin=format!("http://{address}");let api=ApiClient::new(&origin,policy.clone()).unwrap();
    let controller_args=vec!["controller".into(),"--limits".into(),selected.display().to_string(),"--db".into(),root.join("tasks.sqlite").display().to_string(),
        "--listen".into(),address,"--origin".into(),origin.clone()];
    let mut controller=Process::start(&image,&controller_args,&root,"controller");ready(&api,&mut controller).await;
    assert!(matches!(call(&api,Operation::RegisterRepository{operation_id:"cleanup-stop-register".into(),registration:registration.clone()}).await.outcome,Outcome::Ok{..}));
    let make=|label:&str|TaskInput { instruction_source: None,
        pull_request: None,
        resolved_limits:None,repository_read:None,instruction:format!("{label}-cleanup controlled task"),
        definition:Definition{charter: None, id:format!("cleanup-{label}"),revision:"fixture-1".into(),executor:ExecutorKind::Coding,inference:Some(InferenceSettings::selected()),
            execution_profile:Some(ExecutionProfile::CollaborationV1{web_search:None}),limit_overrides:None},
        repository:Some(Repository{ pull_request_target: None, registration:Some(registration.binding()),url:String::new(),start_commit:repository.start_commit.clone(),checkpoint_remote:String::new()}),
        acceptance_commands:vec![vec!["/usr/bin/true".into()]],output_schema:None,
        limits:Limits{deadline_unix_ms:process::now_ms()+120_000,model_requests:2,output_bytes:16_384,result_bytes:policy.task_defaults.result_bytes,max_delegation_depth:4},
        dependencies:vec![],native_workflow:None,
    };
    let Outcome::Ok{data:Data::Task(fast)}=call(&api,Operation::SubmitTask{operation_id:"cleanup-fast-submit".into(),input:(make("fast")).into()}).await.outcome else{panic!("fast submit refused")};
    let Outcome::Ok{data:Data::Task(slow)}=call(&api,Operation::SubmitTask{operation_id:"cleanup-slow-submit".into(),input:(make("slow")).into()}).await.outcome else{panic!("slow submit refused")};
    let worker_root=root.join("worker");let worker_id="cleanup-liveness-worker";
    let gate=Arc::new(CleanupReadGate{task_id:fast.id.clone(),held:AtomicU64::new(0),
        worker_state:worker_root.join(".borg-worker").join(process::component(worker_id)),evidence:root.clone(),
        pending_commands:Mutex::new(Vec::new()),uncommitted_wait:Mutex::new(None),released:std::sync::atomic::AtomicBool::new(false),wake:tokio::sync::Notify::new()});
    let mut proxy=AckProxy::start_with_cleanup_read(origin.clone(),policy.transport.body_bytes,Some(gate.clone())).await;
    let args=vec!["worker".into(),"--limits".into(),selected.display().to_string(),"--controller".into(),proxy.url.clone(),"--id".into(),worker_id.into(),
        "--incarnation".into(),"incarnation-1".into(),"--workspace-root".into(),worker_root.display().to_string(),"--codex".into(),fake.display().to_string(),
        "--coding-capacity".into(),"2".into(),"--native-capacity".into(),"0".into()];
    let mut worker=Process::start(&image,&args,&root,"worker");
    let deadline=tokio::time::Instant::now()+Duration::from_secs(60);
    while !root.join("fast-started").exists()||!root.join("slow-started").exists() {
        worker.live();assert!(tokio::time::Instant::now()<deadline,"both controlled attempts did not start");tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let active=task(&api,&slow.id).await;assert_eq!(active.status,TaskStatus::Active);assert!(!active.attempt.as_ref().unwrap().stopped);
    let pid:u32=std::fs::read_to_string(root.join("slow-started")).unwrap().parse().unwrap();let process_identity=proc_start(pid).expect("controlled slow executor is live");
    std::fs::write(root.join("allow-fast-completion"),"release").unwrap();
    while gate.held.load(Ordering::SeqCst)==0 {
        worker.live();assert!(tokio::time::Instant::now()<deadline,"cleanup did not request terminal task");tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The real idle protocol returns uncommitted Pending for the same original
    // wait. Neither these replies nor short cleanup retries may starve the other
    // future, invent a new wait identity, or restart the held cleanup read.
    let pending_deadline=tokio::time::Instant::now()+Duration::from_secs(5);
    while gate.pending_commands.lock().unwrap().len()<3 {
        worker.live();assert!(tokio::time::Instant::now()<pending_deadline,"Pending AwaitCommands responses made no progress beside cleanup");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let original_pending=gate.pending_commands.lock().unwrap().clone();
    assert_eq!(original_pending.len(),3);assert!(original_pending.iter().all(|id|id==&original_pending[0]));
    assert_eq!(gate.held.load(Ordering::SeqCst),1,"Pending command replies restarted cleanup");
    let accepted=task(&api,&fast.id).await;assert_eq!(accepted.status,TaskStatus::Succeeded);
    let original_attempt=accepted.attempt.as_ref().unwrap();assert!(original_attempt.stopped);
    let work=worker_root.join("workspaces").join(process::component(&original_attempt.id));assert!(work.exists());
    assert!(!gate.released.load(Ordering::SeqCst));
    let current=task(&api,&slow.id).await;assert_eq!(current.status,TaskStatus::Active);
    assert_eq!(current.attempt.as_ref().unwrap().id,active.attempt.as_ref().unwrap().id);
    let cancelled=call(&api,Operation::CancelTask{operation_id:"cleanup-cancel-active".into(),task_id:slow.id.clone(),expected_revision:current.revision}).await;
    assert!(matches!(cancelled.outcome,Outcome::Ok{..}));
    // The cleanup read remains held longer than this finite stop oracle. A
    // worker awaiting cleanup in the chosen select branch cannot pass this.
    let stop_deadline=tokio::time::Instant::now()+Duration::from_secs(10);
    loop {
        worker.live();assert!(!gate.released.load(Ordering::SeqCst));
        let observed=task(&api,&slow.id).await;
        if observed.status==TaskStatus::Cancelled && observed.attempt.as_ref().is_some_and(|a|a.stopped&&a.phase==AttemptPhase::Stopped) {break;}
        assert!(tokio::time::Instant::now()<stop_deadline,"held cleanup read blocked actual StopAttempt/process acknowledgment");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_ne!(proc_start(pid).as_deref(),Some(process_identity.as_str()),"original executor must be reaped before cleanup reply release");
    assert!(!gate.released.load(Ordering::SeqCst));assert!(work.exists());
    gate.released.store(true,Ordering::SeqCst);gate.wake.notify_waiters();
    let finish=tokio::time::Instant::now()+Duration::from_secs(30);
    let checked=worker_root.join("validation").join(process::component(&original_attempt.id));
    let record=worker_root.join(".borg-worker").join(process::component(worker_id)).join("cleanup").join(format!("{}.json",process::component(&original_attempt.id)));
    loop {
        worker.live();
        let resumed=std::fs::read(&record).ok().and_then(|b|serde_json::from_slice::<Value>(&b).ok())
            .is_some_and(|v|v["worktrees"].as_array().is_some_and(|slots|slots.len()==2&&slots.iter().all(|slot|slot["done"]==true)));
        if resumed&&!work.exists()&&!checked.exists(){break;}
        assert!(tokio::time::Instant::now()<finish,"cleanup did not resume after stop handling");tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(task(&api,&fast.id).await,accepted);
    let failed_material=worker_root.join("workspaces").join(process::component(&active.attempt.as_ref().unwrap().id));
    assert!(failed_material.exists(),"cancelled workspace retains selected24h grace");
    worker.stop().await;controller.stop().await;proxy.stop().await;
}
