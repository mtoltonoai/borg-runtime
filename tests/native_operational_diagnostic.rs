//! Actual native Responses/MCP consumer using a controlled local Responses peer.
//! Origin setup uses a controlled canonical failure report; diagnosis uses the actual worker.
//! No real provider, remote, configured Codex or deployment claim.
use borg_runtime::{client::ApiClient, limits::RuntimeLimits, protocol::*, worker::process};
use serde_json::{Value, json};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request as HttpRequest, Response, body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;
use std::{io, sync::atomic::AtomicBool};
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

static NEXT: AtomicU64 = AtomicU64::new(1);
fn area(label: &str) -> PathBuf {
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("artifacts/native-operational-diagnostic-tests");
    std::fs::create_dir_all(&base).unwrap();
    let p = base.join(format!(
        "{label}-{}-{}-{}",
        process::now_ms(),
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir(&p).unwrap();
    p
}
struct Process {
    child: Child,
    closed: PathBuf,
}
impl Process {
    fn start(args: &[String], root: &Path, label: &str) -> Self {
        Self::start_with_limits(args, root, label, &root.join("limits.json"))
    }
    fn start_with_limits(args: &[String], root: &Path, label: &str, limits: &Path) -> Self {
        let log = std::fs::File::create(root.join(format!("{label}.log"))).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_borg-runtime"))
            .args(args)
            .arg("--limits")
            .arg(limits)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", root)
            .env(
                "OPENAI_API_KEY",
                "synthetic-native-loop-key",
            )
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
        loop {
            if let Some(s) = self.child.try_wait().unwrap() {
                assert!(s.success(), "exit {s}");
                break;
            }
            assert!(tokio::time::Instant::now() < until, "stop timeout");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let exit = self.child.wait();
        let _=std::fs::write(&self.closed,serde_json::to_vec(&json!({"pid":self.child.id(),"reaped":exit.is_ok(),"code":exit.ok().and_then(|s|s.code())})).unwrap());
    }
}
struct Background(tokio::task::JoinHandle<()>);
impl Drop for Background {
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct Peer {
    endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
    _owner: Background,
}
impl Peer {
    async fn start(reply: impl Fn(usize, &Value) -> Option<Value> + Send + Sync + 'static) -> Self {
        Self::start_async(move |index, request| std::future::ready(reply(index, &request))).await
    }
    async fn start_async<F, Fut>(reply: F) -> Self
    where
        F: Fn(usize, Value) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Option<Value>> + Send + 'static,
    {
        let body_bytes = RuntimeLimits::seed().unwrap().transport.body_bytes;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let reply = Arc::new(reply);
        let owner = tokio::spawn(async move {
            let mut joins = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted=listener.accept()=>{let (mut stream,_)=accepted.unwrap();let recorded=recorded.clone();let reply=reply.clone();joins.spawn(async move{
                        let mut head=Vec::new();while !head.ends_with(b"\r\n\r\n"){head.push(stream.read_u8().await.unwrap());assert!(head.len()<16384);}
                        let headers=String::from_utf8(head).unwrap();assert!(headers.starts_with("POST /v1/responses "));
                        let length=headers.lines().find_map(|l|{let(k,v)=l.split_once(':')?;k.eq_ignore_ascii_case("content-length").then(||v.trim().parse::<usize>().unwrap())}).unwrap();assert!(length<=body_bytes);
                        let mut bytes=vec![0;length];stream.read_exact(&mut bytes).await.unwrap();let value:Value=serde_json::from_slice(&bytes).unwrap();
                        assert_eq!(value["model"],"gpt-6-astra");assert_eq!(value["reasoning"],json!({"effort":"max"}));
                        let index={let mut all=recorded.lock().unwrap();let n=all.len();all.push(value.clone());n};
                        if let Some(body)=reply(index,value).await{let body=serde_json::to_vec(&body).unwrap();stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();stream.write_all(&body).await.unwrap();}
                        let _=stream.shutdown().await;
                    });},_ = joins.join_next(),if !joins.is_empty()=>{}
                }
            }
        });
        Self {
            endpoint,
            requests,
            _owner: Background(owner),
        }
    }
    fn count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}
fn envelope(output: Value) -> Value {
    json!({"id":"synthetic-loop-response","status":"completed","error":null,"usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7},"output":output})
}
fn function(index: usize, name: &str, args: Value) -> Value {
    envelope(
        json!([{"type":"function_call","status":"completed","call_id":format!("call-{index}"),"name":name,"arguments":args.to_string()}]),
    )
}
fn final_result(value: Value) -> Value {
    envelope(
        json!([{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":value.to_string()}]}]),
    )
}
fn previous(request: &Value) -> Value {
    let item = request["input"].as_array().unwrap().last().unwrap();
    assert_eq!(item["type"], "function_call_output");
    serde_json::from_str(item["output"].as_str().unwrap()).unwrap()
}
struct Lab {
    root: PathBuf,
    address: String,
    url: String,
    limits: Arc<RuntimeLimits>,
    worker_limits_file: PathBuf,
}
impl Lab {
    fn new(label: &str) -> Self {
        let root = area(label);
        let mut limits = RuntimeLimits::seed().unwrap();
        limits.worker.coding_capacity = 0;
        limits.worker.native_capacity = 1;
        let limits = Arc::new(limits);
        std::fs::write(
            root.join("limits.json"),
            serde_json::to_vec_pretty(&*limits).unwrap(),
        )
        .unwrap();
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = l.local_addr().unwrap().to_string();
        Self {
            worker_limits_file: root.join("limits.json"),
            root,
            url: format!("http://{address}"),
            address,
            limits,
        }
    }
    async fn controller(&self, label: &str) -> Process {
        let p = Process::start(
            &[
                "controller".into(),
                "--db".into(),
                self.root.join("tasks.sqlite").display().to_string(),
                "--listen".into(),
                self.address.clone(),
                "--origin".into(),
                self.url.clone(),
            ],
            &self.root,
            label,
        );
        let api = ApiClient::new(&self.url, self.limits.clone()).unwrap();
        let until = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
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
            assert!(
                tokio::time::Instant::now() < until,
                "controller start timeout"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        p
    }
    fn worker(&self, url: &str, endpoint: &str, inc: &str) -> Process {
        self.worker_with_codex(url, endpoint, inc, Path::new("/usr/bin/false"))
    }
    fn worker_with_codex(&self, url: &str, endpoint: &str, inc: &str, codex: &Path) -> Process {
        Process::start_with_limits(
            &[
                "worker".into(),
                "--controller".into(),
                url.into(),
                "--id".into(),
                "loop-worker".into(),
                "--incarnation".into(),
                inc.into(),
                "--workspace-root".into(),
                self.root.join("worker").display().to_string(),
                "--codex".into(),
                codex.display().to_string(),
                "--native-endpoint".into(),
                endpoint.into(),
            ],
            &self.root,
            inc,
            &self.worker_limits_file,
        )
    }
    fn input(&self, requests: u32) -> TaskInput {
        TaskInput { instruction_source: None,
            pull_request: None,
            resolved_limits: None,
            repository_read: None,
            instruction: "Use the configured collaboration interface; return integer seven.".into(),
            definition: Definition {
                charter: None,
                limit_overrides: None,
                inference: Some(InferenceSettings::selected()),
                execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
                id: "loop-definition".into(),
                revision: "collaboration-v1".into(),
                executor: ExecutorKind::Native,
            },
            repository: None,
            acceptance_commands: vec![],
            output_schema: Some(json!({"type":"integer"})),
            limits: Limits {
                deadline_unix_ms: process::now_ms() + 120_000,
                model_requests: requests,
                output_bytes: 32768,
                result_bytes: 65536,
                max_delegation_depth: 1,
            },
            dependencies: vec![],
            native_workflow: None,
        }
    }
    fn state(&self) -> PathBuf {
        self.root
            .join("worker/.borg-worker")
            .join(process::component("loop-worker"))
    }
    fn usage(&self, attempt: &str) -> (u32, Option<i64>, Option<i64>) {
        let c = rusqlite::Connection::open_with_flags(
            self.root.join("tasks.sqlite"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        c.query_row(
            "SELECT model_requests,input_tokens,output_tokens FROM attempts WHERE id=?1",
            [attempt],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap()
    }
}
async fn submit_identified(api: &ApiClient, input: impl Into<Box<TaskInput>>, operation_id: &str) -> Task {
    match api
        .call(Operation::SubmitTask {
            operation_id: operation_id.into(),
            input: (input).into(),
        })
        .await
        .unwrap()
        .outcome
    {
        Outcome::Ok {
            data: Data::Task(t),
        } => *t,
        other => panic!("submit: {other:?}"),
    }
}
async fn get(api: &ApiClient, id: &str) -> Task {
    match api
        .call(Operation::GetTask { task_id: id.into() })
        .await
        .unwrap()
        .outcome
    {
        Outcome::Ok {
            data: Data::Task(t),
        } => *t,
        other => panic!("get: {other:?}"),
    }
}
async fn until(api: &ApiClient, id: &str, ready: impl Fn(&Task) -> bool) -> Task {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let t = get(api, id).await;
        if ready(&t) {
            return t;
        }
        assert!(
            !matches!(
                t.status,
                TaskStatus::Failed | TaskStatus::Succeeded | TaskStatus::Cancelled
            ) || !t.attempt.as_ref().is_some_and(|a| a.stopped),
            "unexpected terminal task: {t:?}"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "task stalled: {t:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn diagnostic_profile()->Value {serde_json::from_str(include_str!("../metadata/operational-diagnostic.json")).unwrap()}

fn diagnostic_native_profile(limits: &RuntimeLimits) -> borg_runtime::native::NativeLoopProfile {
    borg_runtime::native::NativeLoopProfile {
        functions: borg_runtime::mcp::catalogue(limits).into_iter().map(|tool| borg_runtime::native::NativeFunction {
            name: tool.name, description: tool.description, parameters: tool.input_schema,
        }).collect(),
        web_search: None,
    }
}
fn diagnostic_schema_preflight(limits: &RuntimeLimits, input: &TaskInput) -> Value {
    // Actual production catalogue and public profile serializers, never a
    // handwritten list/estimate. The paired test below checks this projection
    // against the production planner's aggregate-bound boundary.
    let profile = diagnostic_native_profile(limits);
    let validation = json!({"schema": input.output_schema.as_ref().unwrap(), "profile": profile, "output_bytes": input.limits.output_bytes});
    let bytes = serde_json::to_vec(&validation).unwrap();
    let selected = limits.resolve_definition(&input.definition).unwrap();
    assert_eq!(selected.native.schema_bytes, bytes.len(), "diagnostic schema/catalogue changed: review a new explicit definition bound before launch");
    assert!(bytes.len() <= selected.native.input_bytes);
    assert!(bytes.len() <= selected.native.helper_value_bytes);
    json!({"global_revision": limits.revision, "definition_revision": input.definition.revision,
        "schema_bytes": selected.native.schema_bytes, "aggregate_encoded_bytes": bytes.len(),
        "output_schema_encoded_bytes": serde_json::to_vec(input.output_schema.as_ref().unwrap()).unwrap().len(),
        "native_profile_encoded_bytes": serde_json::to_vec(&profile).unwrap().len(),
        "tools": profile.functions.len(), "task_output_bytes": input.limits.output_bytes,
        "scope": "prelaunch actual catalogue serialization; no model/provider/worker execution"})
}
#[test]
fn diagnostic_profile_bound_is_explicit_and_preserves_other_effective_settings() {
    let limits = RuntimeLimits::seed().unwrap();
    let profile = diagnostic_profile();
    let definition: Definition = serde_json::from_value(profile["definition"].clone()).unwrap();
    let selected = limits.resolve_definition(&definition).unwrap();
    let mut expected = limits.execution.clone();
    assert_eq!(expected.native.schema_bytes, 16_384, "the global seed remains unchanged");
    expected.native.schema_bytes = profile["schema_binding"]["schema_bytes"].as_u64().unwrap() as usize;
    assert_eq!(expected.native.validation_work, 8_388_608, "general defaults remain unchanged");
    let work = &profile["validation_work_binding"];
    let exact = work["task_output_bytes"].as_u64().unwrap()
        * work["json_nodes"].as_u64().unwrap()
        * work["expanded_output_schema_nodes"].as_u64().unwrap();
    assert_eq!(work["task_output_bytes"], limits.task_defaults.output_bytes);
    assert_eq!(work["json_nodes"], expected.native.json_nodes);
    assert_eq!(exact, 26_038_239_232);
    expected.native.validation_work = exact as usize;
    assert_eq!(selected, expected, "only the explicit schema byte and diagnostic work fields change");
    assert_eq!(definition.revision, "limits-feedback-v4-output-work");
    assert_eq!(definition.executor, ExecutorKind::Native);
    assert_eq!(definition.required_inference().unwrap(), &InferenceSettings::selected());
    assert_eq!(definition.execution_profile, Some(ExecutionProfile::CollaborationV1 { web_search: None }));
}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn diagnostic_schema_one_byte_under_refuses_before_any_helper_or_provider() {
    let lab = Lab::new("diagnostic-schema-preflight");
    let profile = diagnostic_profile();
    let mut input = lab.input(1);
    input.definition = serde_json::from_value(profile["definition"].clone()).unwrap();
    input.output_schema = Some(profile["output_schema"].clone());
    for output in [input.limits.output_bytes, lab.limits.task_defaults.output_bytes] {
        input.limits.output_bytes = output;
        let receipt = diagnostic_schema_preflight(&lab.limits, &input);
        let required = receipt["aggregate_encoded_bytes"].as_u64().unwrap() as usize;
        for (bound, expected) in [(required - 1, "native_json_byte_bound_native_schema_bytes"), (required, "native_validation_unavailable")] {
            let mut candidate = input.clone();
            candidate.definition.limit_overrides.as_mut().unwrap().native.as_mut().unwrap().schema_bytes = bound;
            candidate.resolved_limits = Some(lab.limits.resolve_task(&candidate).unwrap());
            let accepted = candidate.execution_limits().unwrap();
            let absent = lab.root.join("absent-validation-program");
            assert!(!absent.exists());
            let host = borg_runtime::native::ValidationHost::new(absent, lab.root.join("no-helper-registry"),
                Arc::new(accepted.native.clone()), Arc::new(borg_runtime::native::ValidationRuntime::new(Arc::from(lab.limits.worker.clone())).unwrap()));
            let config = borg_runtime::native::NativeConfig { endpoint: "http://127.0.0.1:1/v1/responses".into(), api_key_env: "BORG_UNUSED_PREFLIGHT_KEY".into(), model: candidate.definition.required_inference().unwrap().model.clone() };
            let observed = borg_runtime::native::execute_loop_step(&candidate, &config, &diagnostic_native_profile(&lab.limits), None, &host,
                async { panic!("preflight must not reach provider-send admission") }, std::future::pending::<()>()).await;
            assert_eq!(observed.result.err().as_deref(), Some(expected));
            assert_eq!(observed.request_exposure, borg_runtime::native::RequestExposure::NotStarted);
            assert_eq!(observed.provider_outcome, borg_runtime::native::ProviderOutcome::NotStarted);
            assert!(!lab.root.join("no-helper-registry").exists());
        }
    }
}
fn final_task(reply:Reply)->Task {match reply.outcome{Outcome::Ok{data:Data::Task(t)}=>*t,x=>panic!("task: {x:?}")}}
async fn controlled_report(api:&ApiClient,a:&Attempt,label:&str,observation:Observation)->Task {
    final_task(api.call(Operation::ObserveWorker{report:Report{report_id:format!("{}-{label}",a.id),worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone(),task_id:a.task_id.clone(),attempt_id:a.id.clone(),generation:a.generation,observation}}).await.unwrap())
}
async fn origin_refusal(lab:&Lab,api:&ApiClient)->(Task,borg_runtime::mcp::Invocation,Reply) {
    let mut input=lab.input(4);input.definition.id="native-diagnostic-origin".into();
    let mut collaboration=lab.limits.execution.collaboration.clone();collaboration.complete_result_bytes=2048;
    input.definition.limit_overrides=Some(Box::new(borg_runtime::limits::ExecutionLimitOverrides{collaboration:Some(collaboration),..Default::default()}));
    let origin=submit_identified(api,input,"native-diagnostic-origin-submit").await;
    api.call(Operation::RegisterWorker{operation_id:"origin-fixture-register".into(),worker:Worker{id:"origin-fixture-worker".into(),incarnation:"one".into(),native_capacity:1,coding_capacity:0}}).await.unwrap();
    let commands=api.call(Operation::AwaitCommands{operation_id:"origin-fixture-command".into(),worker_id:"origin-fixture-worker".into(),incarnation:"one".into(),acknowledged:vec![]}).await.unwrap();
    let a=match commands.outcome{Outcome::Ok{data:Data::Commands(cs)}=>cs.into_iter().find_map(|c|match c{WorkerCommand::RunAttempt{attempt,..}if attempt.task_id==origin.id=>Some(attempt),_=>None}).unwrap(),x=>panic!("commands: {x:?}")};
    controlled_report(api,&a,"started",Observation::Started{thread_id:None,turn_id:None}).await;
    let call=borg_runtime::mcp::Invocation{operation_id:"origin-real-result-refusal".into(),logical_call_id:"origin-function".into(),caller:borg_runtime::mcp::Caller{task_id:origin.id.clone(),attempt_id:a.id.clone(),generation:a.generation,worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone()},tool:"collab_memory_set".into(),arguments:json!({"key":"diagnostic-payload","expected_revision":null,"value":"x".repeat(8192)})};
    let refused=api.call(call.operation()).await.unwrap();
    let message=match &refused.outcome{Outcome::Refused{code,message}=>{assert_eq!(code,"response_bound");assert!(message.contains("execution.collaboration.complete_result_bytes"));message.clone()},x=>panic!("actual result-bound refusal: {x:?}")};assert!(refused.commit.is_some());
    controlled_report(api,&a,"failed",Observation::ExecutorOutcome{success:false,summary:message.clone()}).await;
    controlled_report(api,&a,"stopped",Observation::ProcessTreeStopped).await;
    let finished=controlled_report(api,&a,"result",Observation::ResultCandidate{checks_passed:false,summary:message,checkpoint:None,validation:vec![],output:None}).await;
    assert_eq!(finished.status,TaskStatus::Failed);
    assert!(matches!(api.call(Operation::GetMemory{agent_id:origin.assigned_agent_id.clone(),key:"diagnostic-payload".into()}).await.unwrap().outcome,Outcome::Ok{data:Data::Memory(None)}));
    (finished,call,refused)
}
fn diagnostic_input(lab:&Lab,origin:&Task)->TaskInput {
    let p=diagnostic_profile();let mut input=lab.input(11);
    input.definition=serde_json::from_value(p["definition"].clone()).unwrap();
    input.instruction=format!("{}\nOPERATIONAL_CONTEXT\n{}",p["instruction"].as_str().unwrap(),json!({"origin_task_id":origin.id,"target_agent_id":origin.assigned_agent_id,"later_task_ids":[]}));
    input.output_schema=Some(p["output_schema"].clone());
    let preflight=diagnostic_schema_preflight(&lab.limits,&input);
    std::fs::write(lab.root.join("diagnostic-schema-preflight.json"),serde_json::to_vec_pretty(&preflight).unwrap()).unwrap();
    input
}
fn tool_data(request:&Value)->Value {
    let result=previous(request);assert_eq!(result["outcome"]["status"],"ok");result["outcome"]["data"].clone()
}
// Actual native executor consumes every controlled model-selected read/update
// and returns its accepted structured report. Model choices are fixed; no claim
// of real model diagnosis quality, scheduler operation or automatic improvement.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# The shared agent interface MUST support diagnosis of configuration refusals using existing bounded operational records.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# Evaluation of a configuration change MUST identify its committed change outcome and the definition revisions of the observed work.
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn actual_native_diagnostic_reads_applies_and_truthfully_finishes_unmeasured() {
    positive_diagnostic(false,false).await;
}
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# Evaluation of a configuration change MUST identify its committed change outcome and the definition revisions of the observed work.
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn actual_native_diagnostic_later_independent_work_uses_applied_definition() {
    positive_diagnostic(true,false).await;
}
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn actual_native_diagnostic_lost_apply_ack_reconciles_and_survives_restart() {
    positive_diagnostic(false,true).await;
}
async fn positive_diagnostic(with_later:bool,recover:bool) {
    let lab=Lab::new("diagnostic-positive");let mut controller=lab.controller("controller").await;
    let api=ApiClient::new(&lab.url,lab.limits.clone()).unwrap();let (origin,original_call,original_refusal)=origin_refusal(&lab,&api).await;
    let old=origin.clone();let task=submit_identified(&api,diagnostic_input(&lab,&origin),"ordinary-native-diagnostic").await;
    let selected_limit=lab.limits.execution.collaboration.complete_result_bytes;
    let limits=lab.limits.clone();let source=origin.clone();let saved=Arc::new(Mutex::new(None));let retained=saved.clone();
    let peer=Peer::start(move|index,request|{
        assert!(request["tools"].as_array().unwrap().iter().any(|v|v["name"]=="collab_definition_update"));
        match index {
            0=>{assert!(request.to_string().contains("OPERATIONAL_CONTEXT"));Some(function(index,"collab_task_get",json!({"task_id":source.id})))},
            1=>{let data=tool_data(request);assert_eq!(data["kind"],"task");assert_eq!(data["value"]["status"],"failed");assert_eq!(data["value"]["input"]["definition"]["revision"],source.input.definition.revision);Some(function(index,"collab_task_attempts_list",json!({"task_id":source.id,"cursor":null,"limit":limits.controller.collaboration_page_items})))},
            2=>{let data=tool_data(request);assert!(data.to_string().contains("execution.collaboration.complete_result_bytes"));Some(function(index,"collab_task_history_list",json!({"task_id":source.id,"cursor":null,"limit":limits.controller.collaboration_page_items})))},
            3=>{assert!(tool_data(request).to_string().contains("worker_observation"));Some(function(index,"collab_limits_get",json!({})))},
            4=>{let data=tool_data(request);assert!(data.to_string().contains(&limits.revision));Some(function(index,"collab_agent_get",json!({"agent_id":source.assigned_agent_id})))},
            5=>{let data=tool_data(request);assert_eq!(data["value"]["definition"]["revision"],source.input.definition.revision);Some(function(index,"collab_definition_get",json!({"definition_id":source.assigned_agent_id,"revision":source.input.definition.revision})))},
            6=>{assert!(tool_data(request).to_string().contains(&source.input.definition.revision));Some(function(index,"collab_definition_update",json!({"agent_id":source.assigned_agent_id,"expected_revision":source.input.definition.revision,"expected_global_revision":limits.revision,"expected_policy_revision":limits.definition_updates.as_ref().unwrap().revision,"changes":{"collaboration.complete_result_bytes":selected_limit}})))},
            7=>{let original=previous(request);let outcome:Outcome=serde_json::from_value(original["outcome"].clone()).unwrap();let Outcome::Ok{data:Data::DefinitionUpdate(update)}=outcome else{panic!("actual update absent")};let commit:MutationCommit=serde_json::from_value(original["commit"].clone()).unwrap();assert_eq!(update.selected_definition.id,source.assigned_agent_id);assert_eq!(commit.operation_id,original["operation_id"]);*retained.lock().unwrap()=Some((original.clone(),*update));Some(function(index,"collab_call_get",json!({"call_id":commit.operation_id})))},
            8=>{let lookup=previous(request);let saved=retained.lock().unwrap();let (original,update)=saved.as_ref().unwrap();assert_eq!(lookup["outcome"]["data"]["value"]["id"],original["operation_id"]);assert_eq!(lookup["outcome"]["data"]["value"]["outcome"],original["outcome"]);assert_ne!(lookup["commit"],original["commit"]);Some(function(index,"collab_agent_get",json!({"agent_id":update.selected_definition.id})))},
            9=>{let data=tool_data(request);let saved=retained.lock().unwrap();let (original,update)=saved.as_ref().unwrap();assert_eq!(data["value"]["definition"],serde_json::to_value(&update.selected_definition).unwrap());Some(function(index,"collab_tasks_list",json!({"cursor":null,"limit":limits.controller.collaboration_page_items,"agent_id":source.assigned_agent_id,"parent_task_id":null})))},
            10=>{let _=tool_data(request);let saved=retained.lock().unwrap();let (original,update)=saved.as_ref().unwrap();Some(final_result(json!({"disposition":"applied_unmeasured","origin_task_id":source.id,"target_agent_id":source.assigned_agent_id,"reason":"Actual bounded-result refusal; selected existing global default for later work.","change":{"operation_id":original["commit"]["operation_id"],"controller_revision":original["commit"]["controller_revision"],"previous_revision":update.previous_revision,"selected_revision":update.selected_definition.revision,"global_revision":update.global_revision,"policy_revision":update.policy.revision,"changed_fields":update.changed_fields,"classification":update.classification,"effective_for":update.effective_for},"measurement":{"status":"no_later_task","baseline":null,"later":null,"comparison":"not_comparable","causal_improvement_claim":false,"limitations":["No later comparable work was available; controlled Responses peer."]}})))},
            11 if with_later=>Some(function(index,"collab_memory_set",json!({"key":"diagnostic-payload","expected_revision":null,"value":"x".repeat(8192)}))),
            12 if with_later=>{let data=tool_data(request);assert_eq!(data["kind"],"memory");assert_eq!(data["value"]["value"],json!("x".repeat(8192)));Some(final_result(json!(7)))},
            _=>panic!("unexpected additional model request")
        }
    }).await;
    let proxy=if recover{Some(LostApplyReply::start(lab.url.clone()).await)}else{None};
    let worker_origin=proxy.as_ref().map(|p|p.url.as_str()).unwrap_or(&lab.url);
    let mut worker=lab.worker(worker_origin,&peer.endpoint,"diagnostic-worker");
    let finished=until(&api,&task.id,|t|t.status==TaskStatus::Succeeded&&t.attempt.as_ref().is_some_and(|a|a.stopped)).await;
    assert_eq!(peer.count(),11);assert!(!finished.attempt.as_ref().unwrap().uncertain);assert_eq!(finished.input,task.input);
    let (original,update)=saved.lock().unwrap().clone().unwrap();let output=finished.result.as_ref().unwrap().output.as_ref().unwrap();assert_eq!(output["disposition"],"applied_unmeasured");assert_eq!(output["change"]["controller_revision"],original["commit"]["controller_revision"]);
    let selected=api.call(Operation::GetAgent{agent_id:origin.assigned_agent_id.clone()}).await.unwrap();assert!(matches!(selected.outcome,Outcome::Ok{data:Data::Agent(a)}if a.definition==update.selected_definition));
    let operation=original["operation_id"].as_str().unwrap();let receipt=api.call(Operation::GetOperation{operation_id:operation.into()}).await.unwrap();assert_eq!(receipt.commit.as_ref().unwrap().controller_revision,original["commit"]["controller_revision"].as_u64().unwrap());
    assert_eq!(get(&api,&origin.id).await,old);let prior=api.call(Operation::GetOperation{operation_id:original_call.operation_id.clone()}).await.unwrap();assert_eq!(prior.commit,original_refusal.commit);
    let original_receipt=receipt.clone();
    let journal=lab.state().join("collaboration").join(process::component(&finished.attempt.as_ref().unwrap().id)).join("0000000007.json");
    let stored:Value=serde_json::from_slice(&std::fs::read(&journal).unwrap()).unwrap();assert_eq!(stored["consumed"],true);assert_eq!(stored["outcome_context"]["commit"],original["commit"]);assert_eq!(stored["outcome"],original["outcome"]);
    if with_later {
        let mut later_input=origin.input.clone();later_input.definition=update.selected_definition.clone();later_input.resolved_limits=None;
        let later=submit_identified(&api,later_input,"independent-later-workload").await;assert_ne!(later.id,origin.id);
        let later=until(&api,&later.id,|t|t.status==TaskStatus::Succeeded&&t.attempt.as_ref().is_some_and(|a|a.stopped)).await;
        assert_eq!(peer.count(),13);assert_eq!(later.input.instruction,origin.input.instruction);assert_eq!(later.input.output_schema,origin.input.output_schema);assert_eq!(later.input.repository,origin.input.repository);assert_eq!(later.input.definition,update.selected_definition);
        assert_eq!(later.result.as_ref().unwrap().output,Some(json!(7)));
        let later_call=api.call(Operation::GetOperation{operation_id:borg_runtime::native_loop_contract::call_operation_id(&later.attempt.as_ref().unwrap().id,1,1)}).await.unwrap();
        assert!(later_call.commit.is_some());assert!(matches!(later_call.outcome,Outcome::Ok{data:Data::Operation(_)}));
        let value=api.call(Operation::GetMemory{agent_id:origin.assigned_agent_id.clone(),key:"diagnostic-payload".into()}).await.unwrap();assert!(matches!(value.outcome,Outcome::Ok{data:Data::Memory(Some(ref m))}if m.value==json!("x".repeat(8192))));
        let comparison=json!({"kind":"controlled_canonical_external_comparison","original_change_commit":original["commit"],"baseline":{"task_id":origin.id,"definition_revision":origin.input.definition.revision,"status":origin.status,"task_revision":origin.revision},"later":{"task_id":later.id,"definition_revision":later.input.definition.revision,"status":later.status,"task_revision":later.revision,"observed_controller_revision":later_call.observed_controller_revision},"comparison":"success_after_failure","causal_improvement_claim":false,"first_diagnostic_report_remains":"applied_unmeasured"});
        std::fs::write(lab.root.join("comparison.json"),serde_json::to_vec_pretty(&comparison).unwrap()).unwrap();
        assert_eq!(get(&api,&task.id).await.result.as_ref().unwrap().output.as_ref().unwrap()["disposition"],"applied_unmeasured");assert_eq!(get(&api,&origin.id).await,old);
    }
    worker.stop().await;controller.stop().await;
    if let Some(proxy)=proxy {
        assert!(proxy.dropped.load(Ordering::SeqCst));assert_eq!(proxy.updates.load(Ordering::SeqCst),1);assert!(proxy.lookups.load(Ordering::SeqCst)>=1);
        let mut controller=lab.controller("controller-restarted").await;
        let registrations=proxy.registrations.load(Ordering::SeqCst);
        let mut worker=lab.worker(&proxy.url,&peer.endpoint,"diagnostic-worker-restarted");
        let until=tokio::time::Instant::now()+Duration::from_secs(10);
        while proxy.registrations.load(Ordering::SeqCst)<=registrations {assert!(tokio::time::Instant::now()<until,"restarted worker did not register");tokio::time::sleep(Duration::from_millis(20)).await;}
        assert_eq!(get(&api,&task.id).await,finished);assert_eq!(get(&api,&origin.id).await,old);assert_eq!(peer.count(),11);assert_eq!(proxy.updates.load(Ordering::SeqCst),1);
        let receipt=api.call(Operation::GetOperation{operation_id:operation.into()}).await.unwrap();assert_eq!(receipt.commit.as_ref().unwrap().controller_revision,original["commit"]["controller_revision"].as_u64().unwrap());
        assert_eq!(receipt.outcome,original_receipt.outcome,"restart retains full original operation body and outcome");assert_eq!(receipt.commit,original_receipt.commit);
        let selected=api.call(Operation::GetAgent{agent_id:origin.assigned_agent_id.clone()}).await.unwrap();assert!(matches!(selected.outcome,Outcome::Ok{data:Data::Agent(a)}if a.definition==update.selected_definition));
        worker.stop().await;controller.stop().await;
    }
}

async fn uncertain_origin(lab:&Lab,api:&ApiClient)->Task {
    let mut input=lab.input(4);input.definition.id="uncertain-native-origin".into();
    let origin=submit_identified(api,input,"uncertain-origin-submit").await;
    api.call(Operation::RegisterWorker{operation_id:"uncertain-fixture-register".into(),worker:Worker{id:"uncertain-fixture-worker".into(),incarnation:"one".into(),native_capacity:1,coding_capacity:0}}).await.unwrap();
    let commands=api.call(Operation::AwaitCommands{operation_id:"uncertain-fixture-command".into(),worker_id:"uncertain-fixture-worker".into(),incarnation:"one".into(),acknowledged:vec![]}).await.unwrap();
    let a=match commands.outcome{Outcome::Ok{data:Data::Commands(cs)}=>cs.into_iter().find_map(|c|match c{WorkerCommand::RunAttempt{attempt,..}if attempt.task_id==origin.id=>Some(attempt),_=>None}).unwrap(),x=>panic!("commands: {x:?}")};
    controlled_report(api,&a,"started",Observation::Started{thread_id:None,turn_id:None}).await;
    controlled_report(api,&a,"uncertain",Observation::ExecutionUncertain{message:"original external result is unknown; no retry".into()}).await
}
fn no_application(origin:&Task,disposition:&str,reason:&str)->Value {
    json!({"disposition":disposition,"origin_task_id":origin.id,"target_agent_id":origin.assigned_agent_id,"reason":reason,"change":null,"measurement":{"status":"not_applied","baseline":null,"later":null,"comparison":"not_comparable","causal_improvement_claim":false,"limitations":["Controlled Responses choices; no repeated original work or inferred application."]}})
}
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn actual_native_diagnostic_uncertain_origin_ends_no_action() {
    let lab=Lab::new("diagnostic-uncertain");let mut controller=lab.controller("controller").await;let api=ApiClient::new(&lab.url,lab.limits.clone()).unwrap();
    let origin=uncertain_origin(&lab,&api).await;assert!(origin.attempt.as_ref().unwrap().uncertain);
    let task=submit_identified(&api,diagnostic_input(&lab,&origin),"uncertain-origin-diagnosis").await;let source=origin.clone();let page=lab.limits.controller.collaboration_page_items;
    let peer=Peer::start(move|index,request|match index {
        0=>Some(function(index,"collab_task_get",json!({"task_id":source.id}))),
        1=>{assert_eq!(tool_data(request)["value"]["attempt"]["uncertain"],true);Some(function(index,"collab_task_attempts_list",json!({"task_id":source.id,"cursor":null,"limit":page})))},
        2=>{let data=tool_data(request);assert!(data.to_string().contains("uncertain"));Some(final_result(no_application(&source,"no_action","Original outcome is uncertain; no configuration change or retry.")))},
        _=>panic!("uncertain origin triggered an extra model request")
    }).await;
    let mut worker=lab.worker(&lab.url,&peer.endpoint,"diagnostic-worker");let done=until(&api,&task.id,|t|t.status==TaskStatus::Succeeded&&t.attempt.as_ref().is_some_and(|a|a.stopped)).await;
    assert_eq!(peer.count(),3);assert_eq!(done.result.as_ref().unwrap().output.as_ref().unwrap()["disposition"],"no_action");assert_eq!(get(&api,&origin.id).await,origin);
    let selected=api.call(Operation::GetAgent{agent_id:origin.assigned_agent_id.clone()}).await.unwrap();assert!(matches!(selected.outcome,Outcome::Ok{data:Data::Agent(a)}if a.definition==origin.input.definition));
    let path=lab.state().join("collaboration").join(process::component(&done.attempt.as_ref().unwrap().id));
    let records:Vec<_>=std::fs::read_dir(path).unwrap().map(|f|serde_json::from_slice::<Value>(&std::fs::read(f.unwrap().path()).unwrap()).unwrap()).collect();assert_eq!(records.len(),2);assert!(records.iter().all(|v|v["request"]["input"]["call"]["tool"]!="collab_definition_update"));
    worker.stop().await;controller.stop().await;
}
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn actual_native_diagnostic_stale_policy_refuses_once_without_fallback() {
    let lab=Lab::new("diagnostic-stale-policy");let mut controller=lab.controller("controller").await;let api=ApiClient::new(&lab.url,lab.limits.clone()).unwrap();let (origin,_,_)=origin_refusal(&lab,&api).await;
    let task=submit_identified(&api,diagnostic_input(&lab,&origin),"stale-policy-diagnosis").await;let source=origin.clone();let limits=lab.limits.clone();
    let peer=Peer::start(move|index,request|match index {
        0=>Some(function(index,"collab_task_get",json!({"task_id":source.id}))),
        1=>{assert_eq!(tool_data(request)["value"]["status"],"failed");Some(function(index,"collab_limits_get",json!({})))},
        2=>{let _=tool_data(request);Some(function(index,"collab_definition_update",json!({"agent_id":source.assigned_agent_id,"expected_revision":source.input.definition.revision,"expected_global_revision":limits.revision,"expected_policy_revision":"stale-policy-discriminator","changes":{"collaboration.complete_result_bytes":limits.execution.collaboration.complete_result_bytes}})))},
        3=>{let result=previous(request);assert_eq!(result["outcome"]["status"],"refused");assert_eq!(result["outcome"]["code"],"configuration_conflict");assert!(result["commit"].is_object());Some(final_result(no_application(&source,"refused","Exact policy binding changed; stop after the recorded refusal.")))},
        _=>panic!("refusal triggered fallback or another update")
    }).await;
    let mut worker=lab.worker(&lab.url,&peer.endpoint,"diagnostic-worker");let done=until(&api,&task.id,|t|t.status==TaskStatus::Succeeded&&t.attempt.as_ref().is_some_and(|a|a.stopped)).await;
    assert_eq!(peer.count(),4);assert_eq!(done.result.as_ref().unwrap().output.as_ref().unwrap()["disposition"],"refused");assert_eq!(get(&api,&origin.id).await,origin);
    let selected=api.call(Operation::GetAgent{agent_id:origin.assigned_agent_id.clone()}).await.unwrap();assert!(matches!(selected.outcome,Outcome::Ok{data:Data::Agent(a)}if a.definition==origin.input.definition));
    assert!(matches!(api.call(Operation::GetMemory{agent_id:origin.assigned_agent_id.clone(),key:"diagnostic-payload".into()}).await.unwrap().outcome,Outcome::Ok{data:Data::Memory(None)}));
    let journal=lab.state().join("collaboration").join(process::component(&done.attempt.as_ref().unwrap().id)).join("0000000003.json");let data:Value=serde_json::from_slice(&std::fs::read(journal).unwrap()).unwrap();assert_eq!(data["outcome"]["code"],"configuration_conflict");assert!(data["outcome_context"]["commit"].is_object());
    worker.stop().await;controller.stop().await;
}

// The real controller commits the apply before this proxy loses its response.
// The actual host must reconcile the original occurrence through __host_lookup.
struct LostApplyReply {
    url:String,
    dropped:Arc<AtomicBool>,
    updates:Arc<AtomicU64>,
    lookups:Arc<AtomicU64>,
    registrations:Arc<AtomicU64>,
    _owner:Background,
}
impl LostApplyReply {
    async fn start(upstream:String)->Self {
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();let url=format!("http://{}",listener.local_addr().unwrap());
        let dropped=Arc::new(AtomicBool::new(false));let updates=Arc::new(AtomicU64::new(0));let lookups=Arc::new(AtomicU64::new(0));let registrations=Arc::new(AtomicU64::new(0));
        let d=dropped.clone();let u=updates.clone();let l=lookups.clone();let r=registrations.clone();
        let lost_operation=Arc::new(Mutex::new(None::<String>));
        let client=reqwest::Client::builder().no_proxy().build().unwrap();
        let owner=tokio::spawn(async move{
            let mut joins=tokio::task::JoinSet::new();
            loop {tokio::select!{
                accepted=listener.accept()=>{let(stream,_)=accepted.unwrap();let upstream=upstream.clone();let client=client.clone();let d=d.clone();let u=u.clone();let l=l.clone();let r=r.clone();let lost_operation=lost_operation.clone();joins.spawn(async move{
                    let service=service_fn(move|request:HttpRequest<Incoming>|{let upstream=upstream.clone();let client=client.clone();let d=d.clone();let u=u.clone();let l=l.clone();let r=r.clone();let lost_operation=lost_operation.clone();async move{
                        let path=request.uri().path().to_owned();let method=request.method().clone();let headers=request.headers().clone();let body=request.into_body().collect().await.unwrap().to_bytes();let value:Value=serde_json::from_slice(&body).unwrap_or(Value::Null);
                        let is_apply=path=="/mcp"&&value["method"]=="tools/call"&&value["params"]["name"]=="collab_definition_update";
                        if is_apply{u.fetch_add(1,Ordering::SeqCst);*lost_operation.lock().unwrap()=Some(value["params"]["_meta"]["borg"]["operation_id"].as_str().unwrap().to_owned());}
                        // get_call uses this private wire method, not the internal
                        // __host_lookup name. Count only recovery of the dropped
                        // original apply, excluding ordinary pre-dispatch lookups.
                        if path=="/mcp"&&value["method"]=="borg/calls/get"&&d.load(Ordering::SeqCst)&&value["params"]["call_id"].as_str()==lost_operation.lock().unwrap().as_deref(){l.fetch_add(1,Ordering::SeqCst);}
                        if value["operation"]=="register_worker"{r.fetch_add(1,Ordering::SeqCst);}
                        let mut forward=client.request(reqwest::Method::from_bytes(method.as_str().as_bytes()).unwrap(),format!("{upstream}{path}"));
                        for name in ["content-type","mcp-protocol-version",borg_runtime::mcp::CALLER_HEADER] {if let Some(v)=headers.get(name){forward=forward.header(name,v.to_str().unwrap());}}
                        let response=forward.body(body).send().await.unwrap();let status=response.status();let bytes=response.bytes().await.unwrap();
                        if is_apply&&!d.swap(true,Ordering::SeqCst){let result:Value=serde_json::from_slice(&bytes).unwrap();assert!(result.get("result").is_some(),"canonical response completed before drop");return Ok::<_,io::Error>(Response::builder().status(503).body(Full::new(Bytes::new())).unwrap());}
                        Ok::<_,io::Error>(Response::builder().status(status.as_u16()).header("content-type","application/json").body(Full::new(bytes)).unwrap())
                    }});let _=hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream),service).await;
                });},_=joins.join_next(),if !joins.is_empty()=>{}
            }}
        });Self{url,dropped,updates,lookups,registrations,_owner:Background(owner)}
    }
}

// Additional downstream consumer qualification: request/header propagation of
// max_output_tokens is already covered by native_collaboration. This case
// discriminates actual context admission, not only saved override/receipt data.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# A configuration change outcome MUST distinguish future-task selection from settings retained by already accepted tasks.
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn native_context_change_applies_to_independent_later_execution_only() {
    let lab = Lab::new("diagnostic-native-context");
    let mut controller = lab.controller("controller").await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let mut input = lab.input(1);
    input.definition.id = "context-limited-consumer".into();
    input.definition.revision = "context-small-v1".into();
    let old_context = lab.limits.execution.memory_notice.notice_bytes;
    let mut native = lab.limits.execution.native.clone();
    assert!(old_context < native.context_bytes);
    input.instruction = "x".repeat(old_context.checked_add(1).unwrap());
    native.context_bytes = old_context;
    input.definition.limit_overrides = Some(Box::new(borg_runtime::limits::ExecutionLimitOverrides { native: Some(native), ..Default::default() }));
    let resolved = lab.limits.resolve_definition(&input.definition).unwrap();
    assert_eq!(resolved.native.context_bytes, old_context);
    assert!(input.instruction.len() > resolved.native.context_bytes);
    assert!(input.instruction.len() < lab.limits.execution.native.context_bytes);
    let old = submit_identified(&api, input.clone(), "context-before").await;
    let peer = Peer::start(|index, _| { assert_eq!(index, 0); Some(final_result(json!(7))) }).await;
    let mut worker = lab.worker(&lab.url, &peer.endpoint, "context-worker");
    let refused = until(&api, &old.id, |t| t.status == TaskStatus::Failed && t.attempt.as_ref().is_some_and(|a| a.stopped)).await;
    assert_eq!(refused.input, old.input);
    assert_eq!(peer.count(), 0, "old bound refuses before any actual model request");
    assert_eq!(refused.blocked_reason.as_deref(), Some("acceptance_failed"));
    let attempts = api.call(Operation::QueryCollaboration {
        query: borg_runtime::collaboration::Query::TaskAttempts {
            task_id: refused.id.clone(), after: None, limit: 2,
        },
    }).await.unwrap();
    match attempts.outcome {
        Outcome::Ok { data: Data::Collaboration(borg_runtime::collaboration::QueryResult::TaskAttempts(page)) } => {
            assert_eq!(page.items.len(), 1);
            assert!(page.next_cursor.is_none());
            let view = &page.items[0];
            assert_eq!(Some(&view.attempt), refused.attempt.as_ref());
            assert_eq!(view.executor_outcome, Some(borg_runtime::collaboration::ExecutorOutcomeView {
                success: false, summary: "native_instruction_bound_native_context_bytes".into(),
            }));
            assert_eq!(view.reported_model_requests, 0);
            assert_eq!((view.input_tokens, view.output_tokens), (None, None));
        }
        other => panic!("canonical original attempt missing: {other:?}"),
    }
    let apply = api.call(Operation::UpdateDefinition { operation_id: "context-apply".into(), update: DefinitionUpdate {
        agent_id: old.assigned_agent_id.clone(), expected_revision: old.input.definition.revision.clone(),
        expected_global_revision: lab.limits.revision.clone(), expected_policy_revision: lab.limits.definition_updates.as_ref().unwrap().revision.clone(),
        changes: [(borg_runtime::limits::DefinitionLimitField::NativeContextBytes, lab.limits.execution.native.context_bytes)].into_iter().collect(),
    }}).await.unwrap();
    let update = match &apply.outcome { Outcome::Ok { data: Data::DefinitionUpdate(u) } => u, x => panic!("actual configuration application: {x:?}") };
    assert!(apply.commit.is_some());
    assert_eq!(update.effective_for, ConfigurationEffect::FutureSubmissions);
    let selected = api.call(Operation::GetAgent { agent_id: old.assigned_agent_id.clone() }).await.unwrap();
    assert!(matches!(selected.outcome, Outcome::Ok { data: Data::Agent(a) } if a.definition == update.selected_definition));
    input.definition = update.selected_definition.clone();
    let later = submit_identified(&api, input, "context-independent-after").await;
    assert_ne!(later.id, old.id);
    assert_eq!(later.input.instruction, old.input.instruction);
    assert_eq!(later.input.output_schema, old.input.output_schema);
    assert_eq!(later.input.limits, old.input.limits);
    let done = until(&api, &later.id, |t| t.status == TaskStatus::Succeeded && t.attempt.as_ref().is_some_and(|a| a.stopped)).await;
    assert_eq!(done.result.as_ref().unwrap().output, Some(json!(7)));
    assert_eq!(peer.count(), 1);
    assert_eq!(done.input, later.input);
    assert_eq!(done.input.execution_limits().unwrap().native.context_bytes, lab.limits.execution.native.context_bytes);
    assert_eq!(get(&api, &old.id).await, refused, "the earlier failed task is not retried or reinterpreted");
    let original = api.call(Operation::GetOperation { operation_id: "context-apply".into() }).await.unwrap();
    assert_eq!(original.commit, apply.commit);
    assert!(matches!(original.outcome, Outcome::Ok { data: Data::Operation(record) } if record.outcome == Some(serde_json::to_value(&apply.outcome).unwrap())));
    worker.stop().await;
    controller.stop().await;
    // The controller owns an exclusive SQLite lock until shutdown. Audit the
    // original stopped attempt after that owner closes, without changing it.
    assert_eq!(lab.usage(&refused.attempt.as_ref().unwrap().id).0, 0);
}
