//! Actual local controller/worker/Git join. GitHub is a bounded loopback peer;
//! no provider, credentials, external repository or merge action is exercised.
use super::*;
use borg_runtime::pull_request as pr;
use tokio::sync::watch;

const TOKEN: &str="controlled-pr-fixture-credential";
struct ProviderPeer {
    url:String, state:Arc<Mutex<ProviderState>>, posts:mpsc::Receiver<Value>,
    release:watch::Sender<bool>, join:tokio::task::JoinHandle<()>,
}
#[derive(Default)]
struct ProviderState { posts:usize, reads:usize, proof:Option<Value> }
impl Drop for ProviderPeer {fn drop(&mut self){self.join.abort();}}
impl ProviderPeer {
    async fn new(root:PathBuf, head_remote:PathBuf, cut:bool, client:ApiClient)->Self {
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url=format!("http://{}",listener.local_addr().unwrap());
        let state=Arc::new(Mutex::new(ProviderState::default()));let shared=state.clone();
        let (tx,posts)=mpsc::channel(2);let (release,_)=watch::channel(!cut);let permit=release.clone();let api=url.clone();
        let join=tokio::spawn(async move{
            let mut connections=tokio::task::JoinSet::new();
            loop {tokio::select!{
                accepted=listener.accept()=>{
                    let (stream,_)=accepted.unwrap();let (state,tx,permit,root,remote,client,api)=(shared.clone(),tx.clone(),permit.clone(),root.clone(),head_remote.clone(),client.clone(),api.clone());
                    connections.spawn(async move{
                        let service=service_fn(move |request:HttpRequest<Incoming>|{
                            let (state,tx,permit,root,remote,client,api)=(state.clone(),tx.clone(),permit.clone(),root.clone(),remote.clone(),client.clone(),api.clone());
                            async move{
                                assert_eq!(request.headers().get("authorization").unwrap(),format!("Bearer {TOKEN}").as_str());
                                let method=request.method().clone();let path=request.uri().path().to_owned();let query=request.uri().query().unwrap_or("").to_owned();
                                let bytes=request.into_body().collect().await.map_err(io::Error::other)?.to_bytes();assert!(bytes.len()<131072);
                                let (status,value)=if method==hyper::Method::GET && path=="/repos/fixture-owner/fixture-repo" {
                                    (200,json!({"id":71,"name":"fixture-repo","owner":{"login":"fixture-owner"}}))
                                } else if method==hyper::Method::GET && path.starts_with("/repos/fixture-owner/fixture-repo/git/ref/heads/") {
                                    let branch=path.strip_prefix("/repos/fixture-owner/fixture-repo/git/ref/heads/").unwrap();
                                    let reference=format!("refs/heads/{branch}");let commit=git(&root,&["--git-dir",remote.to_str().unwrap(),"rev-parse",&reference]);
                                    (200,json!({"ref":reference,"object":{"type":"commit","sha":commit}}))
                                } else if method==hyper::Method::POST && path=="/repos/fixture-owner/fixture-repo/pulls" {
                                    let body:Value=serde_json::from_slice(&bytes).unwrap();
                                    let task:Task=task(client.call(Operation::GetTask{task_id:fs::read_to_string(root.join("requested-task-id")).unwrap()}).await.unwrap());
                                    let attempt=task.attempt.as_ref().unwrap();let phase_path=root.join("pr-worker/.borg-worker").join(borg_runtime::worker::process::component("pr-worker"))
                                        .join("pull-requests").join(format!("{}.json",borg_runtime::worker::process::component(&attempt.id)));
                                    let phase:Value=serde_json::from_slice(&fs::read(&phase_path).unwrap()).unwrap();
                                    assert_eq!(phase["create_effect"]["may_have_started"],true,"send barrier precedes POST");
                                    assert!(phase["create_effect"]["prepare_commit"]["controller_revision"].as_u64().unwrap()>0);
                                    let id=phase["create_effect"]["intended"]["id"].as_str().unwrap();
                                    let Outcome::Ok{data:Data::Operation(original)}=client.call(Operation::GetOperation{operation_id:id.into()}).await.unwrap().outcome else{panic!("canonical prepare absent")};
                                    assert_eq!(serde_json::to_value(&original).unwrap(),phase["create_effect"]["intended"]);
                                    assert_eq!(body,pr::create_body(&serde_json::from_value::<pr::Intent>(original.intended.clone()).unwrap(),&pr::Bounds::from_limits(client.limits())).unwrap());
                                    let cp=task.selected_checkpoint.as_ref().unwrap();let reference=format!("refs/heads/{}",body["head"].as_str().unwrap());
                                    assert_eq!(git(&root,&["--git-dir",remote.to_str().unwrap(),"rev-parse",&reference]),cp.commit);
                                    assert_ne!(cp.repository,remote.to_str().unwrap(),"provider head is a separate real destination");
                                    let repo=json!({"id":71,"name":"fixture-repo","owner":{"login":"fixture-owner"}});
                                    let proof=json!({"id":901,"number":17,"html_url":format!("{api}/fixture-owner/fixture-repo/pull/17"),"title":body["title"],"body":body["body"],
                                        "head":{"ref":body["head"],"sha":cp.commit,"repo":repo},"base":{"ref":"main","sha":git(&root,&["--git-dir",remote.to_str().unwrap(),"rev-parse","refs/heads/main"]),"repo":repo}});
                                    {let mut s=state.lock().unwrap();s.posts+=1;assert_eq!(s.posts,1,"unknown create must never be repeated");s.proof=Some(proof.clone());}
                                    save(root.join("provider-original-create.json"),&json!({"body":body,"canonical_prepare":original,"phase":phase,"proof":proof}));
                                    tx.send(proof.clone()).await.map_err(io::Error::other)?;
                                    if cut {return Err(io::Error::other("controlled provider response lost after original creation"));}
                                    (201,proof)
                                } else if method==hyper::Method::GET && path=="/repos/fixture-owner/fixture-repo/pulls" {
                                    assert!(query.contains("state=all"));assert!(query.contains("base=main"));
                                    let mut gate=permit.subscribe();while !*gate.borrow_and_update(){gate.changed().await.map_err(io::Error::other)?;}
                                    let mut s=state.lock().unwrap();s.reads+=1;(200,json!([s.proof.clone().expect("original create before lookup")]))
                                } else {panic!("unexpected bounded peer request");};
                                Ok::<_,io::Error>(HttpResponse::builder().status(status).header("content-type","application/json").body(Full::new(Bytes::from(serde_json::to_vec(&value).unwrap()))).unwrap())
                            }
                        });
                        let _=hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream),service).await;
                    });
                },Some(done)=connections.join_next()=>done.unwrap(),
            }}
        });Self{url,state,posts,release,join}
    }
}
fn worker(lab:&mut Lab,incarnation:&str)->usize {
    let origin=lab.origin.clone();let workspace=lab.root.join("pr-worker");let fixture=lab.fixture.clone();
    let process=lab.spawn_env(format!("pr-worker-{incarnation}"),&["worker","--controller",&origin,"--id","pr-worker","--incarnation",incarnation,"--workspace-root",workspace.to_str().unwrap(),"--codex",fixture.to_str().unwrap(),"--coding-capacity","1","--native-capacity","0"],&[("GITHUB_TOKEN",TOKEN)],"limits.json");
    lab.workers.push(process);lab.workers.len()-1
}
async fn joined(cut:bool) {
    let mut lab=Lab::new(if cut{"pr-create-lost-response"}else{"pr-create-success"}).await;
    let head_remote=lab.root.join("provider-head.git");git(&lab.root,&["clone","--bare","--quiet",lab.repository.url.as_str(),head_remote.to_str().unwrap()]);
    git(&lab.root,&["--git-dir",head_remote.to_str().unwrap(),"update-ref","refs/heads/main",&lab.repository.start_commit]);
    let mut peer=ProviderPeer::new(lab.root.clone(),head_remote.clone(),cut,lab.client.clone()).await;
    let target=pr::Target{revision:"provider-target-one".into(),api_base:peer.url.clone(),owner:"fixture-owner".into(),repository:"fixture-repo".into(),repository_id:71,head_remote:head_remote.to_string_lossy().into(),base_branch:"main".into(),credential_name:"GITHUB_TOKEN".into()};
    let registration=borg_runtime::repository_catalogue::Registration{repository_id:"pr-fixture-repository".into(),registration_revision:"one".into(),display_name:"Controlled PR publication".into(),source_endpoint:lab.repository.url.clone(),checkpoint_endpoint:lab.repository.checkpoint_remote.clone(),source_store_key:"remote.git".into(),checkpoint_store_key:"remote.git".into(),pull_request_target:Some(target.clone())};
    assert!(matches!(lab.client.call(Operation::RegisterRepository{operation_id:"register-pr-fixture".into(),registration:registration.clone()}).await.unwrap().outcome,Outcome::Ok{..}));
    lab.repository.registration=Some(registration.binding());
    let mut input=lab.input("pr-agent");input.pull_request=Some(pr::Request{title:"Controlled requested change".into(),body:"Exact initial task request.".into()});
    let submitted=task(lab.client.call(Operation::SubmitTask{operation_id:"submit-requested-pr".into(),input: (input).into()}).await.unwrap());
    assert_eq!(submitted.input.repository.as_ref().unwrap().pull_request_target,Some(target));
    fs::write(lab.root.join("requested-task-id"),&submitted.id).unwrap();
    let original_worker=worker(&mut lab,"original");let gate=lab.gate().await;let observed=gate.observed.clone();gate.release().await;
    let proof=tokio::time::timeout(TIMEOUT,peer.posts.recv()).await.unwrap().unwrap();
    let before=lab.get(&submitted.id).await;let attempt=before.attempt.as_ref().unwrap().clone();
    let checkpoint=before.selected_checkpoint.clone().unwrap();
    if cut {
        lab.workers[original_worker].kill();lab.restart_controller().await;
        assert_eq!(lab.get(&submitted.id).await.attempt.as_ref().unwrap().id,attempt.id);
        // Retain release across the restart gap, when no lookup receiver may exist.
        peer.release.send_replace(true);worker(&mut lab,"replacement");
    }
    let done=lab.successful(&submitted.id).await;
    assert_eq!(done.attempt.as_ref().unwrap().id,attempt.id);assert_eq!(done.selected_checkpoint.as_ref(),Some(&checkpoint));
    let result=done.result.as_ref().unwrap().pull_request.as_ref().unwrap();assert_eq!(result.number,17);assert_eq!(result.provider_id,901);
    assert_eq!(result.head.commit,checkpoint.commit);assert_eq!(result.checkpoint_operation_id,checkpoint.operation_id);
    let record=match lab.client.call(Operation::GetOperation{operation_id:result.operation_id.clone()}).await.unwrap().outcome{Outcome::Ok{data:Data::Operation(r)}=>r,other=>panic!("{other:?}")};
    assert_eq!(pr::reference_from_record(&record,&pr::Bounds::from_limits(&lab.limits)).unwrap(),*result);
    assert_eq!(record.outcome.as_ref().unwrap()["proof"]["body"],proof["body"]);
    let head=match lab.client.call(Operation::GetOperation{operation_id:result.head_operation_id.clone()}).await.unwrap().outcome{Outcome::Ok{data:Data::Operation(r)}=>r,other=>panic!("{other:?}")};assert_eq!(head.state,"settled");
    assert_eq!(peer.state.lock().unwrap().posts,1);if cut{assert!(peer.state.lock().unwrap().reads>=1);}
    assert!(tokio::time::timeout(Duration::from_millis(200),lab.gate.accept()).await.is_err(),"recovery must not reenter executor");
    assert_eq!(lab.starts.len(),1);assert_eq!(lab.starts[0],observed);
    lab.close();let calls=peer.state.lock().unwrap();save(lab.root.join("pr-complete.json"),&json!({"task":done,"checkpoint":checkpoint,"create":record,"head":head,"provider_posts":calls.posts,"provider_gets":calls.reads,"controlled_provider_only":true}));drop(calls);
    peer.join.abort();let _=(&mut peer.join).await;
}
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn requested_pull_request_uses_distinct_head_and_typed_original_result(){joined(false).await;}
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn lost_pull_request_response_restarts_by_original_lookup_without_second_create(){joined(true).await;}
