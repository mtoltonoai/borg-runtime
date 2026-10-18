//! Actual shared Native/Coding consumers, controller/worker processes, local
//! Git and a controlled loopback PR provider. No live publication is performed.
use super::*;
use borg_runtime::{mcp::{self,publication as p,Caller,Invocation},pull_request as pr};
use tokio::sync::watch;

const TOKEN:&str="publication-fixture-secret-never-in-model";
struct Provider { url:String,state:Arc<Mutex<ProviderState>>,posts:mpsc::Receiver<Value>,release:watch::Sender<bool>,join:tokio::task::JoinHandle<()> }
#[derive(Default)] struct ProviderState { creates:usize,lookups:usize,proof:Option<Value> }
impl Drop for Provider {fn drop(&mut self){self.join.abort();}}
impl Provider {
    async fn start(root:PathBuf,remote:PathBuf,lose_create:bool)->Self {
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();let url=format!("http://{}",listener.local_addr().unwrap());let api=url.clone();
        let state=Arc::new(Mutex::new(ProviderState::default()));let shared=state.clone();let(tx,posts)=mpsc::channel(2);let(release,_)=watch::channel(!lose_create);let released=release.clone();
        let join=tokio::spawn(async move {let mut connections=tokio::task::JoinSet::new();loop{tokio::select!{
            accepted=listener.accept()=>{let(stream,_)=accepted.unwrap();let(root,remote,api,state,tx,released)=(root.clone(),remote.clone(),api.clone(),shared.clone(),tx.clone(),released.clone());connections.spawn(async move {
                let service=service_fn(move|request:HttpRequest<Incoming>|{let(root,remote,api,state,tx,released)=(root.clone(),remote.clone(),api.clone(),state.clone(),tx.clone(),released.clone());async move {
                    assert_eq!(request.headers().get("authorization").unwrap(),&format!("Bearer {TOKEN}"));
                    let method=request.method().clone();let path=request.uri().path().to_owned();let bytes=request.into_body().collect().await.unwrap().to_bytes();
                    let(status,body)=if path=="/repos/fixture/publication" {(200,json!({"id":71,"name":"publication","owner":{"login":"fixture"}}))}
                    else if path.starts_with("/repos/fixture/publication/git/ref/heads/") {
                        let branch=path.strip_prefix("/repos/fixture/publication/git/ref/heads/").unwrap();let reference=format!("refs/heads/{branch}");let sha=git(&root,&["--git-dir",remote.to_str().unwrap(),"rev-parse",&reference]);
                        (200,json!({"ref":reference,"object":{"type":"commit","sha":sha}}))
                    } else if method==hyper::Method::POST && path=="/repos/fixture/publication/pulls" {
                        let body:Value=serde_json::from_slice(&bytes).unwrap();assert!(!body.to_string().contains(TOKEN));assert!(body["body"].as_str().unwrap().contains("<!-- borg-pr-operation:"));
                        let head=git(&root,&["--git-dir",remote.to_str().unwrap(),"rev-parse",&format!("refs/heads/{}",body["head"].as_str().unwrap())]);let base=git(&root,&["--git-dir",remote.to_str().unwrap(),"rev-parse","refs/heads/main"]);
                        let repo=json!({"id":71,"name":"publication","owner":{"login":"fixture"}});
                        let proof=json!({"id":901,"number":17,"html_url":format!("{api}/fixture/publication/pull/17"),"title":body["title"],"body":body["body"],"head":{"ref":body["head"],"sha":head,"repo":repo},"base":{"ref":"main","sha":base,"repo":repo}});
                        {let mut s=state.lock().unwrap();s.creates+=1;assert_eq!(s.creates,1,"original create must never repeat");s.proof=Some(proof.clone());}
                        save(root.join("publication-provider-create.json"),&json!({"request":body,"proof":proof}));tx.send(proof.clone()).await.unwrap();
                        if lose_create {return Err(io::Error::other("controlled lost PR acknowledgment after creation"));}
                        (201,proof)
                    } else if method==hyper::Method::GET && path=="/repos/fixture/publication/pulls" {
                        let mut gate=released.subscribe();while !*gate.borrow_and_update(){gate.changed().await.map_err(io::Error::other)?;}
                        let mut s=state.lock().unwrap();s.lookups+=1;(200,json!([s.proof.clone().expect("original create precedes reconciliation")]))
                    } else {panic!("unexpected configured provider request {method} {path}")};
                    Ok::<_,io::Error>(HttpResponse::builder().status(status).header("content-type","application/json").body(Full::new(Bytes::from(body.to_string()))).unwrap())
                }});
                let _=hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream),service).await;
            });},Some(result)=connections.join_next()=>result.unwrap(),
        }}});Self{url,state,posts,release,join}
    }
}
fn args(source:&Task)->Value {
    let cp=source.selected_checkpoint.as_ref().unwrap();let target=source.input.repository.as_ref().unwrap().pull_request_target.as_ref().unwrap();
    json!({"source_task_id":source.id,"expected_source_revision":source.revision,"checkpoint_operation_id":cp.operation_id,"repository_id":cp.registration.repository_id,"registration_revision":cp.registration.registration_revision,"target_revision":target.revision})
}
fn selected(source:&Task)->ExecutionProfile {
    let cp=source.selected_checkpoint.as_ref().unwrap();ExecutionProfile::CollaborationV2{web_search:None,publication:Some(p::Selection::V1{
        repository_id:cp.registration.repository_id.clone(),registration_revision:cp.registration.registration_revision.clone(),target_revision:source.input.repository.as_ref().unwrap().pull_request_target.as_ref().unwrap().revision.clone(),
        actions:vec![p::Action::PublishBranch,p::Action::OpenPullRequest],branch_namespace:"borg/mcp-fixture".into()})}
}
async fn source(lab:&mut Lab,remote:PathBuf,api:&str,head_remote:Option<String>)->Task {
    let reg=borg_runtime::repository_catalogue::Registration{repository_id:"publication-source".into(),registration_revision:"one".into(),display_name:"Isolated publication fixture".into(),source_endpoint:lab.repository.url.clone(),checkpoint_endpoint:lab.repository.checkpoint_remote.clone(),source_store_key:"remote.git".into(),checkpoint_store_key:"remote.git".into(),
        pull_request_target:Some(pr::Target{revision:"target-one".into(),api_base:api.into(),owner:"fixture".into(),repository:"publication".into(),repository_id:71,head_remote:head_remote.unwrap_or_else(||remote.to_string_lossy().into()),base_branch:"main".into(),credential_name:"GITHUB_TOKEN".into()})};
    assert!(matches!(lab.client.call(Operation::RegisterRepository{operation_id:"register-publication-source".into(),registration:reg.clone()}).await.unwrap().outcome,Outcome::Ok{..}));lab.repository.registration=Some(reg.binding());
    let source=lab.submit("publication-source-submit","publication-source-agent").await;let origin=lab.origin.clone();lab.start_worker("source-worker","one",&origin);lab.gate().await.release().await;
    let source=lab.successful(&source.id).await;assert_eq!(fs::read_to_string(lab.root.join("seed/answer.txt")).unwrap(),"before\n");
    lab.workers.last_mut().unwrap().stop();source
}
fn remote(lab:&Lab)->PathBuf {
    let remote=lab.root.join("publication-head.git");git(&lab.root,&["clone","--bare","--quiet",&lab.repository.url,remote.to_str().unwrap()]);git(&lab.root,&["--git-dir",remote.to_str().unwrap(),"update-ref","refs/heads/main",&lab.repository.start_commit]);remote
}
fn actor(task:&Task)->Caller {let a=task.attempt.as_ref().unwrap();Caller{task_id:task.id.clone(),attempt_id:a.id.clone(),generation:a.generation,worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone()}}
async fn status(api:&ApiClient,caller:&Caller,id:&str)->p::View {
    let serial=SEQUENCE.fetch_add(1,Ordering::Relaxed);let call=Invocation{operation_id:format!("fixture-publication-get-{serial}"),logical_call_id:format!("fixture-get-{serial}"),caller:caller.clone(),tool:"collab_publication_get".into(),arguments:json!({"publication_id":id})};
    let reply=api.call(call.operation()).await.unwrap();let Outcome::Ok{data:Data::Publication(view)}=reply.outcome else {panic!("publication status {reply:?}")};*view
}
async fn wait_status(api:&ApiClient,caller:&Caller,id:&str,create:bool,expected:p::State)->p::View {
    let end=Instant::now()+TIMEOUT;loop{let view=status(api,caller,id).await;let state=if create{view.pull_request.as_ref().map(|v|v.state)}else{Some(view.branch.state)};
        if state==Some(expected){return view;}assert!(Instant::now()<end,"publication did not settle: {view:?}");tokio::time::sleep(Duration::from_millis(25)).await;}
}
fn public(value:&Value)->p::View {
    let result:mcp::CallResult=serde_json::from_str(value["content"][0]["text"].as_str().unwrap()).unwrap();let Outcome::Ok{data:Data::Publication(view)}=result.reply.outcome else {panic!("unexpected tool outcome {result:?}")};*view
}
fn inspect_no_secret(root:&Path) {
    for entry in fs::read_dir(root).unwrap(){let path=entry.unwrap().path();let meta=fs::symlink_metadata(&path).unwrap();if meta.file_type().is_symlink(){continue;}if meta.is_dir(){inspect_no_secret(&path);}else if meta.is_file(){if let Ok(text)=fs::read_to_string(&path){assert!(!text.contains(TOKEN),"configured token leaked in {}",path.display());}}}
}

struct Model { endpoint:String,posts:Arc<Mutex<Vec<Value>>>,join:tokio::task::JoinHandle<()> }
impl Drop for Model {fn drop(&mut self){self.join.abort();}}
impl Model {
    async fn start(api:ApiClient,task_id:String,publish:Value,expected:p::State)->Self {
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();let endpoint=format!("http://{}/responses",listener.local_addr().unwrap());let posts=Arc::new(Mutex::new(Vec::new()));let shared=posts.clone();
        let join=tokio::spawn(async move{let mut connections=tokio::task::JoinSet::new();loop{tokio::select!{
            accepted=listener.accept()=>{let(stream,_)=accepted.unwrap();let(api,task_id,publish,posts)=(api.clone(),task_id.clone(),publish.clone(),shared.clone());connections.spawn(async move{
                let service=service_fn(move|request:HttpRequest<Incoming>|{let(api,task_id,publish,posts)=(api.clone(),task_id.clone(),publish.clone(),posts.clone());async move{
                    let bytes=request.into_body().collect().await.unwrap().to_bytes();let body:Value=serde_json::from_slice(&bytes).unwrap();assert!(!body.to_string().contains(TOKEN));let index={let mut saved=posts.lock().unwrap();let n=saved.len();saved.push(body.clone());n};
                    let function=|name:&str,args:Value|json!([{"type":"function_call","status":"completed","call_id":format!("publication-model-call-{index}"),"name":name,"arguments":args.to_string()}]);
                    let output=if index==0 {assert!(body["tools"].as_array().unwrap().iter().any(|t|t["name"]=="collab_repository_publish"));function("collab_repository_publish",publish)} else {
                        let value=body["input"].as_array().unwrap().iter().rev().find(|i|i["type"]=="function_call_output").unwrap();let result:Value=serde_json::from_str(value["output"].as_str().unwrap()).unwrap();
                        // Native continuations carry the retained outcome envelope, not a transport Reply.
                        let outcome:Outcome=serde_json::from_value(result["outcome"].clone()).unwrap();
                        let Outcome::Ok{data:Data::Publication(view)}=outcome else{panic!("native original tool result {result:?}")};
                        if index==1 {assert_eq!(view.branch.state,p::State::Pending);let current=task(api.call(Operation::GetTask{task_id:task_id.clone()}).await.unwrap());wait_status(&api,&actor(&current),&view.publication_id,false,expected).await;function("collab_publication_get",json!({"publication_id":view.publication_id}))}
                        else {assert_eq!(index,2,"no model rerun discovers an unknown effect");assert_eq!(view.branch.state,expected);json!([{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"{\"handled\":true}"}]}])}
                    };
                    let response=json!({"id":format!("publication-response-{index}"),"status":"completed","error":null,"usage":{"input_tokens":5,"output_tokens":3,"total_tokens":8},"output":output});
                    Ok::<_,io::Error>(HttpResponse::new(Full::new(Bytes::from(response.to_string()))))
                }});let _=hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream),service).await;
            });},Some(done)=connections.join_next()=>done.unwrap(),
        }}});Self{endpoint,posts,join}
    }
}

fn native_worker(lab:&mut Lab,endpoint:&str,origin:&str,environment:&[(&str,&str)])->usize {
    let workspace=lab.root.join("publication-native");let fixture=lab.fixture.clone();let process=lab.spawn_env("publication-native-one".into(),&["worker","--controller",origin,"--id","publication-native","--incarnation","one","--workspace-root",workspace.to_str().unwrap(),"--codex",fixture.to_str().unwrap(),"--coding-capacity","0","--native-capacity","1","--native-endpoint",endpoint],environment,"native-worker-limits.json");lab.workers.push(process);lab.workers.len()-1
}
async fn native_task(lab:&Lab,source:&Task)->Task {
    let mut input=lab.input("publication-native-agent");input.definition.executor=ExecutorKind::Native;input.definition.execution_profile=Some(selected(source));input.dependencies=vec![source.id.clone()];input.repository=None;input.acceptance_commands.clear();input.output_schema=Some(json!({"type":"object","required":["handled"],"properties":{"handled":{"type":"boolean"}},"additionalProperties":false}));input.limits.model_requests=10;
    let mut native=lab.limits.execution.native.clone();native.function_count=mcp::catalogue_for_profile(&lab.limits,input.definition.execution_profile.as_ref()).len();
        let profile=borg_runtime::native::NativeLoopProfile{functions:mcp::catalogue_for_profile(&lab.limits,input.definition.execution_profile.as_ref()).into_iter().map(|t|borg_runtime::native::NativeFunction{name:t.name,description:t.description,parameters:t.input_schema}).collect(),web_search:None};
        let carrier=json!({"schema":input.output_schema,"profile":profile,"output_bytes":input.limits.output_bytes});
        native.schema_bytes=native.schema_bytes.max(serde_json::to_vec(&carrier).unwrap().len());
        // Existing complete Native test-definition profile from identity_support;
        // the larger provider request includes its catalogue and context framing.
        native.json_nodes=65_536;
    input.definition.limit_overrides=Some(Box::new(borg_runtime::limits::ExecutionLimitOverrides{native:Some(native),..Default::default()}));
    task(lab.client.call(Operation::SubmitTask{operation_id:"native-publication-submit".into(),input}).await.unwrap())
}

#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn publication_mcp_native_dependency_uses_real_git_and_reconciles_lost_git_acknowledgment() {
    let mut lab=Lab::new("publication-mcp-native-git-ack").await;let remote=remote(&lab);let provider=Provider::start(lab.root.clone(),remote.clone(),false).await;
    // The existing trusted Git selector drives a local transport fixture. It
    // forwards real receive-pack advertisements, then drops the real final ACK
    // after receive-pack has committed. Upload-pack readback remains available.
    let ssh=lab.root.join("git-transport.py");fs::write(&ssh,r#"#!/usr/bin/python3
import json,os,pathlib,shlex,subprocess,sys
cfg=json.loads(pathlib.Path(__file__).with_suffix('.json').read_text())
if '-G' in sys.argv: sys.exit(0)
parts=shlex.split(sys.argv[-1]); assert parts[0] in ('git-upload-pack','git-receive-pack') and parts[1]==cfg['remote']
if parts[0]=='git-upload-pack': os.execv('/usr/bin/git-upload-pack',['git-upload-pack',parts[1]])
assert not pathlib.Path(cfg['received']).exists(), 'second receive-pack is forbidden'
p=subprocess.Popen(['/usr/bin/git-receive-pack',parts[1]],stdin=sys.stdin.buffer,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
while True:
    size=p.stdout.read(4); assert len(size)==4
    n=int(size,16); data=p.stdout.read(n-4) if n>4 else b''
    sys.stdout.buffer.write(size+data);sys.stdout.buffer.flush()
    if n==0: break
reply=p.stdout.read(); assert p.wait()==0 and b'unpack ok' in reply
pathlib.Path(cfg['received']).write_text('one committed receive; final acknowledgment deliberately lost\n')
sys.exit(1)
"#).unwrap();fs::set_permissions(&ssh,fs::Permissions::from_mode(0o700)).unwrap();save(ssh.with_extension("json"),&json!({"remote":remote,"received":lab.root.join("git-ack-lost")}));
    let head_remote=format!("ssh://fixture@127.0.0.1{}",remote.to_str().unwrap());let source=source(&mut lab,remote.clone(),&provider.url,Some(head_remote)).await;
    let root=native_task(&lab,&source).await;let model=Model::start(lab.client.clone(),root.id.clone(),args(&source),p::State::Completed).await;let origin=lab.origin.clone();let ssh=ssh.to_str().unwrap();
    native_worker(&mut lab,&model.endpoint,&origin,&[("OPENAI_API_KEY","fixture-native-key"),("GITHUB_TOKEN",TOKEN),("BORG_WORKER_GIT_SSH_COMMAND",ssh)]);
    let done=lab.successful(&root.id).await;assert_eq!(done.input.repository,None);assert_eq!(done.result.as_ref().unwrap().checkpoint,None);assert_eq!(lab.get(&source.id).await,source);
    assert_eq!(model.posts.lock().unwrap().len(),3);assert!(lab.root.join("git-ack-lost").exists());assert_eq!(provider.state.lock().unwrap().creates,0);
    inspect_no_secret(&lab.root.join("publication-native"));lab.restart_controller().await;assert_eq!(lab.get(&source.id).await,source);lab.close();
}

const CODING_PEER:&str=r#"#!/usr/bin/python3
import json,os,pathlib,socket,subprocess,sys
cfg=json.loads(pathlib.Path(__file__).with_suffix('.json').read_text())
assert 'GH_TOKEN' not in os.environ and 'GITHUB_TOKEN' not in os.environ
thread='publication-peer-thread';turn='publication-peer-turn';server=None
def emit(x): print(json.dumps(x),flush=True)
def gate(phase,value):
    with socket.create_connection(('127.0.0.1',cfg['port']),timeout=25) as s:
        s.sendall((json.dumps({'phase':phase,'reply':value,'pid':os.getpid(),'cwd':os.getcwd(),'child_env_clean':True})+'\n').encode());assert s.recv(1)==b'G'
for line in sys.stdin:
    q=json.loads(line);m=q.get('method')
    if 'id' not in q: continue
    if m=='initialize':r={'userAgent':'controlled-publication-peer'}
    elif m=='command/exec':
        assert q['params']['command']==['/usr/bin/true'];subprocess.run(['/usr/bin/true'],check=True);r={'exitCode':0,'stdout':'','stderr':''}
    elif m=='thread/start':
        server=q['params']['config']['mcp_servers']['borg_collaboration'];assert 'collab_repository_publish' in server['enabled_tools'];assert 'collab_pull_request_open' in server['enabled_tools']
        r={'thread':{'id':thread},'model':'gpt-6-astra','reasoningEffort':'max'}
    elif m=='turn/start':
        emit({'id':q['id'],'result':{'turn':{'id':turn,'status':'inProgress'}}})
        bridge=subprocess.Popen([server['command'],*server['args']],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
        serial=0
        def rpc(method,params):
            global serial
            serial+=1;bridge.stdin.write(json.dumps({'jsonrpc':'2.0','id':serial,'method':method,'params':params})+'\n');bridge.stdin.flush();v=json.loads(bridge.stdout.readline());assert v['id']==serial and 'error' not in v,v;return v['result']
        rpc('initialize',{'protocolVersion':'2025-06-18','capabilities':{},'clientInfo':{'name':'publication-peer','version':'1'}})
        tools=rpc('tools/list',{});assert sum(t['name'] in ['collab_repository_publish','collab_pull_request_open','collab_publication_get'] for t in tools['tools'])==3
        def call(name,args,logical):
            emit({'method':'item/started','params':{'threadId':thread,'turnId':turn,'item':{'type':'mcpToolCall','id':logical,'server':'borg_collaboration','tool':name,'arguments':args,'status':'inProgress'}}})
            value=rpc('tools/call',{'name':name,'arguments':args,'_meta':{'callId':logical,'threadId':thread,'sessionId':'publication-peer-session'}})
            emit({'method':'item/completed','params':{'threadId':thread,'turnId':turn,'item':{'type':'mcpToolCall','id':logical,'server':'borg_collaboration','tool':name,'arguments':args,'status':'completed','result':value,'error':None}}})
            return value
        published=call('collab_repository_publish',cfg['publish'],'publish-original');gate('branch',published)
        identity=json.loads(published['content'][0]['text'])['reply']['data']['value']['publication_id']
        opened=call('collab_pull_request_open',{'publication_id':identity,'title':'Exact controlled change','body':'Exact controlled body'},'open-original');gate('create',opened)
        status=call('collab_publication_get',{'publication_id':identity},'get-original')
        state=json.loads(status['content'][0]['text'])['reply']['data']['value'];assert state['pull_request']['state']==cfg['expected'],state
        bridge.stdin.close();bridge.terminate();bridge.wait(timeout=5)
        pathlib.Path('answer.txt').write_text('after\n')
        emit({'method':'item/completed','params':{'item':{'type':'agentMessage','text':'Publication outcome retained through the shared tools.'}}})
        emit({'method':'turn/completed','params':{'threadId':thread,'turn':{'id':turn,'status':'completed'}}});continue
    elif m=='turn/interrupt':r={}
    else:emit({'id':q['id'],'error':{'code':-32601,'message':'unsupported fixture request'}});continue
    emit({'id':q['id'],'result':r})
"#;
fn coding_worker(lab:&mut Lab,incarnation:&str,token:bool)->usize {
    let origin=lab.origin.clone();let workspace=lab.root.join("publication-coding");let fixture=lab.root.join("publication-coding.py");
    let env=if token{vec![("GITHUB_TOKEN",TOKEN)]}else{vec![]};let process=lab.spawn_env(format!("publication-coding-{incarnation}"),&["worker","--controller",&origin,"--id","publication-coding","--incarnation",incarnation,"--workspace-root",workspace.to_str().unwrap(),"--codex",fixture.to_str().unwrap(),"--coding-capacity","1","--native-capacity","0"],&env,"limits.json");lab.workers.push(process);lab.workers.len()-1
}
async fn coding_case(lose_create:bool,token:bool) {
    let mut lab=Lab::new(if lose_create{"publication-mcp-pr-lost-ack"}else if token{"publication-mcp-coding-success"}else{"publication-mcp-missing-token"}).await;
    let remote=remote(&lab);let mut provider=Provider::start(lab.root.clone(),remote.clone(),lose_create).await;let source=source(&mut lab,remote.clone(),&provider.url,None).await;
    let mut input=lab.input("publication-coding-agent");input.definition.execution_profile=Some(selected(&source));input.dependencies=vec![source.id.clone()];input.limits.model_requests=10;
    let root=task(lab.client.call(Operation::SubmitTask{operation_id:"coding-publication-submit".into(),input}).await.unwrap());
    let fixture=lab.root.join("publication-coding.py");fs::write(&fixture,CODING_PEER).unwrap();fs::set_permissions(&fixture,fs::Permissions::from_mode(0o700)).unwrap();save(fixture.with_extension("json"),&json!({"port":lab.gate.local_addr().unwrap().port(),"publish":args(&source),"expected":if token{"completed"}else{"refused"}}));
    let first_worker=coding_worker(&mut lab,"one",token);let branch=lab.gate().await;assert_eq!(branch.observed["phase"],"branch");assert_eq!(branch.observed["child_env_clean"],true);let admitted=public(&branch.observed["reply"]);assert_eq!(admitted.branch.state,p::State::Pending);
    let current=lab.get(&root.id).await;let caller=actor(&current);let branch_view=wait_status(&lab.client,&caller,&admitted.publication_id,false,p::State::Completed).await;
    assert_eq!(branch_view.branch.head.as_ref().unwrap().commit,source.selected_checkpoint.as_ref().unwrap().commit);branch.release().await;
    let create=lab.gate().await;assert_eq!(create.observed["phase"],"create");assert_eq!(public(&create.observed["reply"]).pull_request.unwrap().state,p::State::Pending);
    if lose_create {
        let proof=tokio::time::timeout(TIMEOUT,provider.posts.recv()).await.unwrap().unwrap();assert_eq!(proof["head"]["sha"],source.selected_checkpoint.as_ref().unwrap().commit);
        lab.workers[first_worker].kill();lab.restart_controller().await;provider.release.send_replace(true);coding_worker(&mut lab,"replacement",true);
        let done=wait_status(&lab.client,&caller,&admitted.publication_id,true,p::State::Completed).await;assert_eq!(done.pull_request.unwrap().pull_request.unwrap().number,17);
        assert_eq!(provider.state.lock().unwrap().creates,1);assert!(provider.state.lock().unwrap().lookups>0);
        assert!(tokio::time::timeout(Duration::from_millis(200),lab.gate.accept()).await.is_err(),"publication recovery cannot rerun the model or executor");drop(create);
    } else {
        let expected=if token{p::State::Completed}else{p::State::Refused};let done=wait_status(&lab.client,&caller,&admitted.publication_id,true,expected).await;
        if !token{assert_eq!(done.pull_request.unwrap().refusal,Some(p::Refusal::Configuration));assert_eq!(provider.state.lock().unwrap().creates,0);}else{let r=done.pull_request.unwrap().pull_request.unwrap();assert_eq!(r.head.commit,source.selected_checkpoint.as_ref().unwrap().commit);assert_eq!(r.base.commit,lab.repository.start_commit);assert_eq!(r.provider_id,901);}
        create.release().await;lab.successful(&root.id).await;
    }
    assert_eq!(lab.get(&source.id).await,source);inspect_no_secret(&lab.root.join("publication-coding"));lab.close();
}
#[tokio::test(flavor="multi_thread",worker_threads=4)] async fn publication_mcp_coding_bridge_publishes_exact_source_and_provider_evidence(){coding_case(false,true).await;}
#[tokio::test(flavor="multi_thread",worker_threads=4)] async fn publication_mcp_lost_pr_ack_and_worker_incarnation_change_reconcile_original_marker_once(){coding_case(true,true).await;}
#[tokio::test(flavor="multi_thread",worker_threads=4)] async fn publication_mcp_missing_api_credential_is_bounded_configuration_refusal(){coding_case(false,false).await;}
