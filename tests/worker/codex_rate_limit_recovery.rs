//! Controlled public app-server peer with the real worker, controller, Git,
//! checkpoint acknowledgment and declared acceptance. No provider is contacted.
use super::*;
use borg_runtime::{
    client::ApiClient,
    collaboration::{Query, QueryResult},
};
use serde_json::{Value, json};
use std::{
    os::unix::fs::PermissionsExt,
    process::{Child, Command, Stdio},
};

struct Process {
    child: Child,
    closed: PathBuf,
}
impl Process {
    fn start(image: &Path, args: &[String], root: &Path, label: &str) -> Self {
        let log = std::fs::File::create(root.join(format!("{label}.log"))).unwrap();
        let child = Command::new(image)
            .args(args)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", root)
            .env("OPENAI_API_KEY", "synthetic-rate-fixture-key")
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        Self {
            child,
            closed: root.join(format!("{label}-closed.json")),
        }
    }
    fn live(&mut self) {
        assert!(
            self.child.try_wait().unwrap().is_none(),
            "controlled process exited"
        );
    }
    async fn stop(&mut self) {
        if self.child.try_wait().unwrap().is_none() {
            assert!(
                Command::new("/usr/bin/kill")
                    .args(["-INT", &self.child.id().to_string()])
                    .status()
                    .unwrap()
                    .success()
            );
        }
        let until = tokio::time::Instant::now() + Duration::from_secs(20);
        while self.child.try_wait().unwrap().is_none() {
            assert!(
                tokio::time::Instant::now() < until,
                "owned process stop timed out"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let result = self.child.wait();
        let _=std::fs::write(&self.closed,serde_json::to_vec(&json!({"pid":self.child.id(),"reaped":result.is_ok(),"exit_code":result.ok().and_then(|s|s.code())})).unwrap());
    }
}
async fn call(api: &ApiClient, op: Operation) -> Reply {
    Box::pin(api.call(op)).await.unwrap()
}
async fn task(api: &ApiClient, id: &str) -> Task {
    match call(api, Operation::GetTask { task_id: id.into() })
        .await
        .outcome
    {
        Outcome::Ok {
            data: Data::Task(t),
        } => *t,
        v => panic!("task read: {v:?}"),
    }
}

const PEER: &str = r#"#!/usr/bin/python3
import json,os,pathlib,sys,time
root=pathlib.Path(__file__).parent
scenario=(root/'scenario').read_text()
home=pathlib.Path(os.environ['CODEX_HOME']); cwd=pathlib.Path.cwd()
thread='retained-rate-thread'
def send(v): print(json.dumps(v),flush=True)
def progress(turn,n):
 for i in range(n):send({'method':'item/agentMessage/delta','params':{'threadId':thread,'turnId':turn,'itemId':'progress','delta':'progress-'+str(i)}})
def usage(turn,n):
 send({'method':'thread/tokenUsage/updated','params':{'threadId':thread,'turnId':turn,'tokenUsage':{'total':{'inputTokens':n*10,'outputTokens':n*2}}}})
def terminal(turn,status,error=None):
 t={'id':turn,'status':status,'error':error}
 (home/'public-terminal.json').write_text(json.dumps(t))
 send({'method':'turn/completed','params':{'threadId':thread,'turn':t}})
for line in sys.stdin:
 m=json.loads(line);method=m.get('method');p=m.get('params',{})
 with (root/'requests.jsonl').open('a') as f:f.write(json.dumps({'method':method,'params':p,'cwd':str(cwd),'home':str(home),'pid':os.getpid()})+'\n')
 if method=='initialize':send({'id':m['id'],'result':{'userAgent':'controlled-rate-peer'}})
 elif method=='command/exec':send({'id':m['id'],'result':{'exitCode':0,'stdout':'','stderr':''}})
 elif method=='thread/start':
  assert not (home/'created-thread').exists(),'no replacement thread'
  (home/'created-thread').write_text(thread);(home/'sessions').mkdir(exist_ok=True)
  (home/'sessions/public.jsonl').write_text('controlled delivered transcript\n')
  send({'id':m['id'],'result':{'thread':{'id':thread,'cwd':str(cwd)},'model':'gpt-6-astra','reasoningEffort':'max'}})
 elif method=='thread/inject_items':send({'id':m['id'],'result':{}})
 elif method in ('thread/read','thread/resume'):
  assert p['threadId']==thread and (home/'created-thread').read_text()==thread
  t=json.loads((home/'public-terminal.json').read_text())
  body={'id':thread,'cwd':str(cwd),'turns':[t]}
  if scenario=='cold-swapped':body['cwd']='/different-workspace'
  send({'id':m['id'],'result':{'thread':body,'model':'gpt-6-astra','reasoningEffort':'max'}})
 elif method=='turn/start':
  assert p['threadId']==thread and p['cwd']==str(cwd)
  assert p['model']=='gpt-6-astra' and p['effort']=='max'
  assert p['sandboxPolicy']=={'type':'workspaceWrite','writableRoots':[str(cwd)],'networkAccess':True}
  counter=home/'outer-count'; n=int(counter.read_text())+1 if counter.exists() else 1;counter.write_text(str(n))
  turn='turn-'+str(n)
  if n==1:
   (cwd/'answer.txt').write_text('dirty source survives rate failure\n')
   (root/'first-started').write_text(str(os.getpid()))
  else:
   assert (cwd/'answer.txt').read_text()=='dirty source survives rate failure\n'
   assert 'retained conversation' in p['input'][0]['text']
  if scenario=='unknown-start' and n==2:sys.exit(0)
  if scenario in ('preack-progress','trailing-progress','terminal-before-ack','progress-lost-ack'):progress(turn,128)
  if scenario=='progress-lost-ack' and n==2:sys.exit(0)
  if scenario=='required-overflow' and n==2:
   for i in range(65):send({'method':'item/completed','params':{'threadId':thread,'turnId':turn,'item':{'id':'required-'+str(i),'type':'agentMessage','phase':'commentary','text':'required item'}}})
   continue
  if scenario=='terminal-before-ack' and n==2:
   send({'method':'item/completed','params':{'threadId':thread,'turnId':turn,'item':{'id':'before-ack-final','type':'agentMessage','phase':'final_answer','text':'source ready before ACK'}}})
   usage(turn,n);terminal(turn,'completed')
   send({'id':m['id'],'result':{'turn':{'id':turn,'status':'inProgress'}}});continue
  send({'id':m['id'],'result':{'turn':{'id':'turn-1' if scenario=='duplicate-ack' else turn,'status':'inProgress'}}})
  if scenario=='will-retry':
   send({'method':'error','params':{'threadId':thread,'turnId':turn,'willRetry':True,'error':{'codexErrorInfo':'rateLimitExceeded','message':'internal retry'}}})
   terminal(turn,'completed');continue
  if n==1 or scenario=='repeat-rate' or scenario=='trailing-progress' and n<4:
   if scenario in ('held-terminal','cancel','effects'):
    until=time.monotonic()+30
    while not (root/'release-terminal').exists():
     assert time.monotonic()<until;time.sleep(.01)
   cls='rateLimitExceeded'
   if scenario=='generic':cls='other'
   message='typed rate rejection; text says wait 999999 hours'
   if scenario=='credential':message='synthetic-rate-fixture-key'
   if scenario=='trailing-progress':usage(turn,n)
   terminal(turn,'failed',{'codexErrorInfo':cls,'message':message})
   if scenario=='trailing-progress':
    # Arrive during the canonical backoff. These cannot settle the next turn or
    # inflate its usage, and the progress burst must not occupy pending slots.
    progress(turn,256);usage(turn,999)
    send({'method':'turn/completed','params':{'threadId':thread,'turn':{'id':turn,'status':'failed','error':{'codexErrorInfo':'rateLimitExceeded','message':'trailing duplicate'}}}})
   if scenario.startswith('cold'):sys.exit(0)
  else:
   if scenario=='trailing-progress':usage(turn,n)
   send({'method':'item/completed','params':{'threadId':thread,'turnId':turn,'item':{'type':'agentMessage','phase':'final_answer','text':'source ready'}}})
   terminal(turn,'completed')
 elif method=='turn/interrupt':send({'id':m['id'],'result':{}})
 elif 'id' in m:raise AssertionError('unexpected method '+str(method))
"#;
struct Lab {
    root: PathBuf,
    worker_root: PathBuf,
    api: ApiClient,
    controller: Process,
    worker: Process,
    submitted: Task,
    policy: Arc<borg_runtime::limits::RuntimeLimits>,
}
impl Lab {
    async fn start(
        scenario: &str,
        requests: u32,
        backoff: u64,
        enabled: bool,
        retention: bool,
    ) -> Self {
        let root = area(&format!("supplied-rate-{scenario}"));
        let mut metadata = (*limits()).clone();
        if scenario == "success-no-governance" {
            metadata.governance = None;
        }
        metadata.worker.coding_capacity = 1;
        metadata.worker.native_capacity = 0;
        metadata.controller.command_wait_ms = 20;
        metadata.execution.coding.rate_limit_retry =
            enabled.then_some(borg_runtime::supplied_outer::RetryPolicy {
                initial_backoff_ms: backoff,
                backoff_multiplier: 2,
                max_backoff_ms: backoff,
                jitter_ms: 1,
            });
        if retention {
            metadata.retention = Some(borg_runtime::limits::RetentionLimits {
                worker: borg_runtime::limits::WorkerRetentionLimits {
                    success_grace_ms: 0,
                    failed_grace_ms: 0,
                    cancelled_grace_ms: 0,
                    idle_cache_grace_ms: 604800000,
                    batch_entries: 32,
                    filesystem_entries_per_yield: 32,
                    operation_timeout_ms: 30000,
                    retry_delay_ms: 10,
                    record_bytes: 1048576,
                },
            });
        }
        metadata.validate().unwrap();
        let policy = Arc::new(metadata);
        let selected = write_limits(&root, &policy);
        let image = root.join("runtime");
        retained_images::materialize(Path::new(env!("CARGO_BIN_EXE_borg-runtime")), &image, false);
        let repository = repository(&root);
        let remote = root.join("remote.git");
        let registration = borg_runtime::repository_catalogue::Registration {
            pull_request_target: None,
            repository_id: "rate-fixture".into(),
            registration_revision: "one".into(),
            display_name: "Controlled rate retry source".into(),
            source_endpoint: format!("file://{}", remote.display()),
            checkpoint_endpoint: remote.to_string_lossy().into(),
            source_store_key: "rate-source".into(),
            checkpoint_store_key: "rate-checkpoint".into(),
        };
        std::fs::write(root.join("scenario"), scenario).unwrap();
        let fake = root.join("controlled-codex");
        std::fs::write(&fake, PEER).unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o700)).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        drop(listener);
        let origin = format!("http://{address}");
        let api = ApiClient::new(&origin, policy.clone()).unwrap();
        let mut controller = Process::start(
            &image,
            &[
                "controller".into(),
                "--limits".into(),
                selected.display().to_string(),
                "--db".into(),
                root.join("tasks.sqlite").display().to_string(),
                "--listen".into(),
                address,
                "--origin".into(),
                origin.clone(),
            ],
            &root,
            "controller",
        );
        let until = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            controller.live();
            if api
                .call(Operation::ListTasks {
                    after: None,
                    limit: 1,
                })
                .await
                .is_ok()
            {
                break;
            }
            assert!(tokio::time::Instant::now() < until);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(matches!(
            call(
                &api,
                Operation::RegisterRepository {
                    operation_id: "register-rate".into(),
                    registration: registration.clone()
                }
            )
            .await
            .outcome,
            Outcome::Ok { .. }
        ));
        let input=TaskInput{instruction_source:None,pull_request:None,resolved_limits:None,repository_read:None,instruction:"Keep the dirty source and finish the same task.".into(),definition:Definition{charter:None,id:"rate-engineer".into(),revision:"one".into(),executor:ExecutorKind::Coding,inference:Some(InferenceSettings::selected()),execution_profile:Some(ExecutionProfile::CollaborationV1{web_search:None}),limit_overrides:None},repository:Some(Repository{pull_request_target:None,registration:Some(registration.binding()),url:String::new(),start_commit:repository.start_commit,checkpoint_remote:String::new()}),acceptance_commands:vec![vec!["/usr/bin/python3".into(),"-c".into(),"import pathlib; assert pathlib.Path('answer.txt').read_text() == 'dirty source survives rate failure\\n'".into()]],output_schema:None,limits:Limits{deadline_unix_ms:process::now_ms()+90000,model_requests:requests,output_bytes:16384,result_bytes:policy.task_defaults.result_bytes,max_delegation_depth:4},dependencies:vec![],native_workflow:None};
        let submitted = match call(
            &api,
            Operation::SubmitTask {
                operation_id: "submit-rate".into(),
                input,
            },
        )
        .await
        .outcome
        {
            Outcome::Ok {
                data: Data::Task(t),
            } => *t,
            v => panic!("submit: {v:?}"),
        };
        let worker_root = root.join("worker");
        let worker = Process::start(
            &image,
            &[
                "worker".into(),
                "--limits".into(),
                selected.display().to_string(),
                "--controller".into(),
                origin,
                "--id".into(),
                "rate-worker".into(),
                "--incarnation".into(),
                "rate-incarnation".into(),
                "--workspace-root".into(),
                worker_root.display().to_string(),
                "--codex".into(),
                fake.display().to_string(),
                "--coding-capacity".into(),
                "1".into(),
                "--native-capacity".into(),
                "0".into(),
            ],
            &root,
            "worker",
        );
        Self {
            root,
            worker_root,
            api,
            controller,
            worker,
            submitted,
            policy,
        }
    }
    async fn wait_started(&mut self) -> Task {
        let until = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            self.worker.live();
            let t = task(&self.api, &self.submitted.id).await;
            if self.root.join("first-started").exists() {
                return t;
            }
            assert!(tokio::time::Instant::now() < until, "first start: {t:?}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    async fn finished(&mut self) -> Task {
        let until = tokio::time::Instant::now() + Duration::from_secs(45);
        loop {
            self.worker.live();
            self.controller.live();
            let t = task(&self.api, &self.submitted.id).await;
            if matches!(
                t.status,
                TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::Cancelled
            ) && t.attempt.as_ref().is_some_and(|a| a.stopped)
            {
                return t;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "terminal task absent: {t:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    fn requests(&self) -> Vec<Value> {
        std::fs::read_to_string(self.root.join("requests.jsonl"))
            .unwrap_or_default()
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect()
    }
    fn starts(&self) -> usize {
        self.requests()
            .iter()
            .filter(|r| r["method"] == "turn/start")
            .count()
    }
    fn state(&self) -> PathBuf {
        self.worker_root
            .join(".borg-worker")
            .join(process::component("rate-worker"))
    }
    fn home(&self, t: &Task) -> PathBuf {
        self.state()
            .join("codex")
            .join(process::component(&t.attempt.as_ref().unwrap().id))
    }
    async fn close(&mut self) {
        self.worker.stop().await;
        self.controller.stop().await;
    }
    async fn original(&self, t: &Task, index: u32, phase: &str) -> OperationRecord {
        let id = borg_runtime::supplied_outer::id(&t.attempt.as_ref().unwrap().id, index, phase);
        match call(&self.api, Operation::GetOperation { operation_id: id })
            .await
            .outcome
        {
            Outcome::Ok {
                data: Data::Operation(v),
            } => v,
            v => panic!("original supplied receipt: {v:?}"),
        }
    }
}

#[tokio::test]
async fn supplier_retry_notification_does_not_start_another_outer_turn() {
    let mut lab = Lab::start("will-retry", 4, 20, true, false).await;
    let t = lab.finished().await;
    assert_eq!(t.status, TaskStatus::Succeeded);
    assert_eq!(lab.starts(), 1);
    assert_eq!(
        lab.requests()
            .iter()
            .filter(|r| r["method"] == "thread/start")
            .count(),
        1
    );
    lab.close().await;
}
#[tokio::test]
async fn known_rate_limit_keeps_dirty_workspace_thread_and_accepts_after_continuation() {
    let mut lab = Lab::start("success", 4, 20, true, false).await;
    let t = lab.finished().await;
    assert_eq!(t.status, TaskStatus::Succeeded, "{t:?}");
    assert_eq!(lab.starts(), 2);
    let rows = lab.requests();
    let starts: Vec<_> = rows
        .iter()
        .filter(|r| r["method"] == "turn/start")
        .collect();
    assert_eq!(starts[0]["cwd"], starts[1]["cwd"]);
    assert_eq!(starts[0]["home"], starts[1]["home"]);
    assert_eq!(
        starts[0]["params"]["threadId"],
        starts[1]["params"]["threadId"]
    );
    assert_eq!(t.attempt.as_ref().unwrap().generation, 1);
    assert!(!t.attempt.as_ref().unwrap().uncertain);
    let cp = t.selected_checkpoint.as_ref().unwrap();
    assert_eq!(
        run_git(&lab.root.join("remote.git"), &["rev-parse", &cp.remote_ref]),
        cp.commit
    );
    assert!(
        t.result
            .as_ref()
            .unwrap()
            .validation
            .iter()
            .all(|v| v.contains("pass"))
    );
    let terminal = lab.original(&t, 1, "terminal").await;
    let wait = lab.original(&t, 2, "wait").await;
    let dispatch = lab.original(&t, 2, "dispatch").await;
    let ack = lab.original(&t, 2, "ack").await;
    std::fs::write(lab.root.join("retry-accepted.json"),serde_json::to_vec_pretty(&json!({"task":t,"terminal":terminal,"wait":wait,"dispatch":dispatch,"ack":ack,"requests":rows})).unwrap()).unwrap();
    let thread: Value =
        serde_json::from_slice(&std::fs::read(lab.home(&t).join("borg-thread.json")).unwrap())
            .unwrap();
    assert_eq!(thread["turn_id"], "turn-1");
    let attempts = call(
        &lab.api,
        Operation::QueryCollaboration {
            query: Query::TaskAttempts {
                task_id: t.id.clone(),
                after: None,
                limit: 10,
            },
        },
    )
    .await;
    assert!(
        matches!(attempts.outcome,Outcome::Ok{data:Data::Collaboration(QueryResult::TaskAttempts(p))} if p.items.len()==1)
    );
    lab.close().await;
}
#[tokio::test]
async fn known_thread_resume_requires_original_identity_and_no_active_process() {
    for scenario in ["cold-success", "cold-swapped"] {
        let mut lab = Lab::start(scenario, 4, 100, true, false).await;
        let t = lab.finished().await;
        let rows = lab.requests();
        assert_eq!(
            rows.iter()
                .filter(|r| r["method"] == "thread/start")
                .count(),
            1
        );
        assert_eq!(
            rows.iter().filter(|r| r["method"] == "thread/read").count(),
            1
        );
        if scenario == "cold-success" {
            assert_eq!(t.status, TaskStatus::Succeeded, "{t:?}");
            assert_eq!(lab.starts(), 2);
            assert_eq!(
                rows.iter()
                    .filter(|r| r["method"] == "thread/resume")
                    .count(),
                1
            );
            let starts: Vec<_> = rows
                .iter()
                .filter(|r| r["method"] == "turn/start")
                .collect();
            assert_ne!(starts[0]["pid"], starts[1]["pid"]);
        } else {
            assert_eq!(t.status, TaskStatus::Failed);
            assert_eq!(lab.starts(), 1);
        }
        lab.close().await;
    }
}
#[tokio::test]
async fn policy_bounds_and_non_chartered_attempt_keep_replay_guards() {
    use borg_runtime::supplied_outer::RetryPolicy;
    let p = RetryPolicy {
        initial_backoff_ms: 10,
        backoff_multiplier: 2,
        max_backoff_ms: 20,
        jitter_ms: 1,
    };
    assert!(p.validate().is_ok());
    assert!(p.delay_ms(1, 0, 90, 100).is_err());
    assert!(p.delay_ms(0, 0, 0, 100).is_err());
    let mut invalid = p.clone();
    invalid.initial_backoff_ms = 0;
    assert!(invalid.validate().is_err());
    invalid = p.clone();
    invalid.jitter_ms = u64::MAX;
    assert!(invalid.validate().is_err());
    let mut lab = Lab::start("success", 4, 20, false, false).await;
    let t = lab.finished().await;
    assert_eq!(t.status, TaskStatus::Failed);
    assert_eq!(lab.starts(), 1);
    assert!(!lab.home(&t).join("borg-supplied-outer.json").exists());
    lab.close().await;
    let mut lab = Lab::start("duplicate-ack", 4, 20, true, false).await;
    let t = lab.finished().await;
    assert_eq!(t.status, TaskStatus::Failed);
    assert!(t.attempt.as_ref().unwrap().uncertain);
    assert_eq!(lab.starts(), 2);
    lab.close().await;
}
#[tokio::test]
async fn dispatch_and_ack_crash_cuts_never_duplicate_start() {
    let mut lab = Lab::start("unknown-start", 4, 20, true, false).await;
    let t = lab.finished().await;
    assert_eq!(t.status, TaskStatus::Failed);
    assert!(t.attempt.as_ref().unwrap().uncertain);
    assert_eq!(lab.starts(), 2);
    let journal = std::fs::read(lab.home(&t).join("borg-supplied-outer.json")).unwrap();
    let value: Value = serde_json::from_slice(&journal).unwrap();
    assert_eq!(value["dispatched"], true);
    assert!(value["turn_id"].is_null());
    // Reconciliation/readback cannot turn a retained unknown start into a send.
    assert_eq!(task(&lab.api, &t.id).await, t);
    assert_eq!(
        std::fs::read(lab.home(&t).join("borg-supplied-outer.json")).unwrap(),
        journal
    );
    assert_eq!(lab.starts(), 2);
    lab.close().await;
}
#[tokio::test]
async fn cancel_deadline_stale_owner_and_budget_stop_without_another_start() {
    let mut lab = Lab::start("cancel", 4, 1000, true, false).await;
    let t = lab.wait_started().await;
    assert!(matches!(
        call(
            &lab.api,
            Operation::CancelTask {
                operation_id: "cancel-rate".into(),
                task_id: t.id,
                expected_revision: t.revision
            }
        )
        .await
        .outcome,
        Outcome::Ok { .. }
    ));
    std::fs::write(lab.root.join("release-terminal"), "release").unwrap();
    let t = lab.finished().await;
    assert_eq!(t.status, TaskStatus::Cancelled);
    assert_eq!(lab.starts(), 1);
    lab.close().await;
    let mut lab = Lab::start("success", 1, 100000, true, false).await;
    let t = lab.finished().await;
    assert_eq!(t.status, TaskStatus::Failed);
    assert_eq!(lab.starts(), 1);
    lab.close().await;
    let mut lab = Lab::start("repeat-rate", 1, 20, true, true).await;
    let t = lab.finished().await;
    assert_eq!(t.status, TaskStatus::Failed);
    assert_eq!(lab.starts(), 2);
    let work = lab
        .worker_root
        .join("workspaces")
        .join(process::component(&t.attempt.as_ref().unwrap().id));
    let until = tokio::time::Instant::now() + Duration::from_secs(20);
    while work.exists() {
        lab.worker.live();
        assert!(
            tokio::time::Instant::now() < until,
            "settled refusal must not retain a definitely-unsent retry indefinitely"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    lab.close().await;
}
#[tokio::test]
async fn bounded_diagnostics_preserve_actual_cause_without_provider_text_authority() {
    let mut visible = Lab::start("success", 4, 20, true, false).await;
    let done = visible.finished().await;
    assert_eq!(done.status, TaskStatus::Succeeded);
    let history = call(
        &visible.api,
        Operation::QueryCollaboration {
            query: Query::TaskHistory {
                task_id: done.id,
                after: None,
                limit: 100,
            },
        },
    )
    .await;
    let text = serde_json::to_string(&history).unwrap();
    assert!(
        text.contains("due_ms=")
            && text.contains("policy_initial_ms=20")
            && text.contains("retained-rate-thread"),
        "bounded existing UI/MCP history must expose canonical timing and source"
    );
    visible.close().await;
    let mut lab = Lab::start("generic", 4, 20, true, false).await;
    let t = lab.finished().await;
    assert_eq!(lab.starts(), 1);
    assert_eq!(t.status, TaskStatus::Failed);
    let terminal = lab.original(&t, 1, "terminal").await;
    assert!(
        serde_json::to_string(&terminal)
            .unwrap()
            .contains("999999 hours")
    );
    lab.close().await;
    let mut lab = Lab::start("credential", 4, 20, true, false).await;
    let t = lab.finished().await;
    assert_eq!(lab.starts(), 1);
    assert_eq!(t.status, TaskStatus::Failed);
    let history = call(
        &lab.api,
        Operation::QueryCollaboration {
            query: Query::TaskHistory {
                task_id: t.id,
                after: None,
                limit: 100,
            },
        },
    )
    .await;
    assert!(
        !serde_json::to_string(&history)
            .unwrap()
            .contains("synthetic-rate-fixture-key")
    );
    lab.close().await;
}

#[tokio::test]
async fn unresolved_tool_write_or_completion_obligation_blocks_continuation() {
    let mut lab = Lab::start("effects", 4, 20, true, false).await;
    let t = lab.wait_started().await;
    let attempt = t.attempt.as_ref().unwrap();
    let repository = t.input.repository.as_ref().unwrap();
    let cp = git::checkpoint(
        repository,
        &t.id,
        &attempt.id,
        repository.start_commit.clone(),
    )
    .unwrap();
    let prepare = Operation::PrepareOperation {
        operation_id: cp.operation_id.clone(),
        task_id: t.id.clone(),
        attempt_id: attempt.id.clone(),
        generation: attempt.generation,
        kind: "checkpoint".into(),
        intended: json!({"registration":cp.registration,"repository":cp.repository,"commit":cp.commit,"remote_ref":cp.remote_ref}),
    };
    assert!(matches!(
        call(&lab.api, prepare.clone()).await.outcome,
        Outcome::Ok {
            data: Data::Operation(_)
        }
    ));
    std::fs::write(lab.root.join("release-terminal"), "release").unwrap();
    // An unresolved external intent prevents terminal acceptance even after
    // physical shutdown. Wait for the original refusal notice, not a terminal
    // task status that would incorrectly discard this reconciliation barrier.
    let until = tokio::time::Instant::now() + Duration::from_secs(45);
    let t = loop {
        lab.worker.live();
        lab.controller.live();
        let current = task(&lab.api, &lab.submitted.id).await;
        assert_eq!(lab.starts(), 1, "pending effect must prevent another start");
        if current.attempt.as_ref().is_some_and(|a| a.stopped && a.uncertain)
            && current.progress
                == "Completed result acceptance is refused; original decision retained for reconciliation"
        {
            break current;
        }
        assert!(
            tokio::time::Instant::now() < until,
            "stopped uncertain task and original refusal notice absent: {current:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(t.status, TaskStatus::Active);
    assert!(t.result.is_none());
    assert_eq!(t.attempt.as_ref().unwrap().id, attempt.id);
    let wait = lab.original(&t, 2, "wait").await;
    let wait_outcome: Outcome = serde_json::from_value(wait.outcome.clone().unwrap()).unwrap();
    assert!(matches!(wait_outcome, Outcome::Refused { code, .. }
        if code == "reconciliation_required"));
    assert_eq!(lab.starts(), 1);
    assert!(t.attempt.as_ref().unwrap().uncertain);
    let retained = call(
        &lab.api,
        Operation::GetOperation {
            operation_id: cp.operation_id.clone(),
        },
    )
    .await;
    assert!(
        matches!(&retained.outcome,Outcome::Ok{data:Data::Operation(v)} if v.state=="pending"&&v.outcome.is_none())
    );
    assert_eq!(call(&lab.api, prepare).await.outcome, retained.outcome);
    std::fs::write(
        lab.root.join("unresolved-effects-held.json"),
        serde_json::to_vec_pretty(&json!({
            "task": &t, "retry_wait_original": wait,
            "pending_effect_original": &retained, "requests": lab.requests(),
        })).unwrap(),
    ).unwrap();
    // A failed public request did not settle the earlier external intent, nor
    // publish the changed dirty file as an acknowledged checkpoint.
    assert!(t.selected_checkpoint.is_none());
    assert!(
        std::fs::read_to_string(
            lab.worker_root
                .join("workspaces")
                .join(process::component(&attempt.id))
                .join("answer.txt")
        )
        .unwrap()
        .contains("dirty source")
    );
    lab.close().await;
}
#[tokio::test]
async fn cleanup_holds_pending_retry_and_releases_only_under_selected_policy() {
    let mut lab = Lab::start("success-no-governance", 4, 2000, true, true).await;
    let first = lab.wait_started().await;
    let attempt = first.attempt.as_ref().unwrap();
    let work = lab
        .worker_root
        .join("workspaces")
        .join(process::component(&attempt.id));
    let home = lab.home(&first);
    let until = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let journal = std::fs::read(home.join("borg-supplied-outer.json"))
            .ok()
            .and_then(|b| serde_json::from_slice::<Value>(&b).ok());
        if journal
            .as_ref()
            .is_some_and(|j| j["phase"]["phase"] == "waiting" && j["committed"] == true)
        {
            break;
        }
        assert!(tokio::time::Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(work.join("answer.txt").exists());
    assert!(home.join("sessions/public.jsonl").exists());
    assert_eq!(lab.starts(), 1);
    let t = lab.finished().await;
    assert_eq!(t.status, TaskStatus::Succeeded);
    let exported = lab
        .state()
        .join("codex-transcripts")
        .join(process::component(&attempt.id))
        .join("sessions/public.jsonl");
    let until = tokio::time::Instant::now() + Duration::from_secs(20);
    while work.exists() || !exported.exists() {
        lab.worker.live();
        assert!(
            tokio::time::Instant::now() < until,
            "settled configured cleanup did not run"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(home.join("borg-supplied-outer.json").exists());
    assert_eq!(
        std::fs::read_to_string(exported).unwrap(),
        "controlled delivered transcript\n"
    );
    lab.close().await;
    let mut lab = Lab::start("unknown-start", 4, 20, true, true).await;
    let t = lab.finished().await;
    assert!(t.attempt.as_ref().unwrap().uncertain);
    assert!(lab.home(&t).join("borg-supplied-outer.json").exists());
    assert!(
        lab.worker_root
            .join("workspaces")
            .join(process::component(&t.attempt.as_ref().unwrap().id))
            .exists()
    );
    lab.close().await;
}

#[tokio::test]
async fn progress_bursts_before_ack_and_across_retries_preserve_one_thread_and_usage() {
    for scenario in ["preack-progress", "trailing-progress", "terminal-before-ack"] {
        let mut lab = Lab::start(scenario, 8, 100, true, false).await;
        assert_eq!(lab.policy.execution.coding.pending_notifications, 64);
        let t = lab.finished().await;
        let expected = if scenario == "trailing-progress" { 4 } else { 2 };
        assert_eq!(t.status, TaskStatus::Succeeded, "{scenario}: {t:?}");
        assert_eq!(lab.starts(), expected);
        let requests = lab.requests();
        assert_eq!(requests.iter().filter(|v|v["method"]=="thread/start").count(), 1);
        let starts = requests.iter().filter(|v|v["method"]=="turn/start").collect::<Vec<_>>();
        assert!(starts.iter().all(|v|v["pid"]==starts[0]["pid"] && v["home"]==starts[0]["home"] && v["cwd"]==starts[0]["cwd"]));
        assert!(!t.attempt.as_ref().unwrap().uncertain);
        assert!(t.selected_checkpoint.is_some());
        assert!(t.result.as_ref().unwrap().validation.iter().all(|v|v.contains("pass")));
        if scenario != "preack-progress" {
            let usage:Value=serde_json::from_slice(&std::fs::read(lab.home(&t).join("borg-usage.json")).unwrap()).unwrap();
            assert_eq!(usage["turn_id"], format!("turn-{expected}"));
            assert_eq!(usage["observed_usage"]["input_tokens"], expected * 10);
            assert_eq!(usage["observed_usage"]["output_tokens"], expected * 2);
        }
        for index in 2..=expected {
            let wait = lab.original(&t, index as u32, "wait").await;
            let dispatch = lab.original(&t, index as u32, "dispatch").await;
            let ack = lab.original(&t, index as u32, "ack").await;
            assert!(matches!(serde_json::from_value::<Outcome>(wait.outcome.unwrap()).unwrap(), Outcome::Ok {..}));
            assert!(matches!(serde_json::from_value::<Outcome>(dispatch.outcome.unwrap()).unwrap(), Outcome::Ok {..}));
            assert!(matches!(serde_json::from_value::<Outcome>(ack.outcome.unwrap()).unwrap(), Outcome::Ok {..}));
        }
        lab.close().await;
    }
}

#[tokio::test]
async fn pre_ack_required_overflow_and_lost_ack_never_resend_uncertain_start() {
    for scenario in ["required-overflow", "progress-lost-ack"] {
        let mut lab = Lab::start(scenario, 8, 20, true, false).await;
        let mut t = lab.finished().await;
        let until = tokio::time::Instant::now() + Duration::from_secs(20);
        while t.progress != "Completed result acceptance is refused; original decision retained for reconciliation" {
            lab.worker.live();
            assert!(tokio::time::Instant::now() < until, "final refusal observation absent: {t:?}");
            tokio::time::sleep(Duration::from_millis(10)).await;
            t = task(&lab.api, &t.id).await;
        }
        assert_eq!(t.status, TaskStatus::Failed, "{scenario}: {t:?}");
        assert!(t.attempt.as_ref().unwrap().uncertain);
        assert_eq!(lab.starts(), 2);
        let journal = std::fs::read(lab.home(&t).join("borg-supplied-outer.json")).unwrap();
        let value:Value=serde_json::from_slice(&journal).unwrap();
        assert_eq!(value["dispatched"], true);
        assert!(value["turn_id"].is_null());
        assert_eq!(task(&lab.api, &t.id).await, t);
        assert_eq!(std::fs::read(lab.home(&t).join("borg-supplied-outer.json")).unwrap(), journal);
        assert_eq!(lab.starts(), 2);
        lab.close().await;
    }
}
