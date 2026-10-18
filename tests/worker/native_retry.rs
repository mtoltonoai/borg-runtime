//! Actual controller and worker processes with a controlled loopback provider.
//! Joins tests/worker.rs. No real provider, inherited credential, or coding peer.
//! Explicit coordination metadata keeps these cases focused on known rejection,
//! accounting, cancellation, recovery and canonical exhausted-allowance blocking.
#[path = "native_request_cuts.rs"]
mod native_request_cuts;

use super::*;
use borg_runtime::{client::ApiClient, limits::{NativeCircuitLimits, NativeRetryLimits, RuntimeLimits},
    native::{NativeRetryBlock, ProviderRequestBinding, RejectionKind, RetryAfter, RetryBlockReason}, native_loop_contract as contract};
use serde_json::{Value, json};
use std::{process::{Child, Command, Stdio}, sync::Mutex};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener};

const WAIT: Duration = Duration::from_secs(30);
const WORKER_ID: &str = "native-retry-worker";

// Same retained binary and owned signal/reap pattern as worker::task_recovery.
// Only these small fixture helpers are repeated; the application is unmodified.
struct Process { child: Child, closed: PathBuf }
impl Process {
    fn start(image: &Path, args: &[String], root: &Path, label: &str) -> Self {
        let log = std::fs::File::create(root.join(format!("{label}.log"))).unwrap();
        let child = Command::new(image).args(args).env_clear()
            .env("PATH", "/usr/bin:/bin").env("HOME", root)
            .env("OPENAI_API_KEY", "synthetic-native-retry-fixture-key")
            .stdin(Stdio::null()).stdout(log.try_clone().unwrap()).stderr(log).spawn().unwrap();
        Self { child, closed: root.join(format!("{label}-closed.json")) }
    }
    fn live(&mut self) { assert!(self.child.try_wait().unwrap().is_none(), "application exited before fixture completed"); }
    async fn stop(&mut self) {
        if self.child.try_wait().unwrap().is_none() {
            assert!(Command::new("/usr/bin/kill").args(["-INT", &self.child.id().to_string()]).status().unwrap().success());
        }
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success(), "application shutdown failed: {status}");
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "application shutdown timed out");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    fn hard_loss(&mut self) {
        self.live();
        self.child.kill().unwrap();
        assert!(!self.child.wait().unwrap().success(), "hard loss did not terminate the worker");
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
        let _ = self.child.kill();
        let waited = self.child.wait();
        let _ = std::fs::write(&self.closed, serde_json::to_vec(&json!({"pid":self.child.id(),
            "reaped":waited.is_ok(),"exit_code":waited.as_ref().ok().and_then(|s|s.code())})).unwrap());
    }
}

struct Lab {
    root: PathBuf, image: PathBuf, address: String, origin: String,
    limits: Arc<RuntimeLimits>, api: ApiClient,
}
impl Lab {
    fn new(label: &str) -> Self {
        Self::with_auth_cooldown(label, 150)
    }
    fn with_auth_cooldown(label: &str, auth_cooldown_ms: u64) -> Self {
        let root = area(label);
        let mut policy = (*limits()).clone();
        policy.worker.coding_capacity = 0;
        policy.worker.native_capacity = 1;
        // Fixture timing only. There is deliberately no separate retry count.
        policy.native_retry = Some(NativeRetryLimits {
            initial_backoff_ms: 5, backoff_multiplier: 2, max_backoff_ms: 20, jitter_ms: 1,
            // Three rejected POSTs must still reach the required fourth POST.
            // These explicit coordination values belong only to this fixture.
            circuit: NativeCircuitLimits { failure_threshold: 4, open_ms: 50, max_entries: 16 },
            auth_cooldown_ms, auth_max_entries: 16, coordination_record_bytes: 32_768,
        });
        policy.validate().unwrap();
        write_limits(&root, &policy);
        let image = root.join("runtime");
        retained_images::materialize(Path::new(env!("CARGO_BIN_EXE_borg-runtime")), &image, false);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        drop(listener);
        let origin = format!("http://{address}");
        let limits = Arc::new(policy);
        let api = ApiClient::new(&origin, limits.clone()).unwrap();
        Self { root, image, address, origin, limits, api }
    }
    async fn controller(&self, label: &str) -> Process {
        let mut child = Process::start(&self.image, &[
            "controller".into(), "--limits".into(), self.root.join("limits.json").display().to_string(),
            "--db".into(), self.root.join("tasks.sqlite").display().to_string(),
            "--listen".into(), self.address.clone(), "--origin".into(), self.origin.clone(),
        ], &self.root, label);
        let until = tokio::time::Instant::now() + WAIT;
        loop {
            child.live();
            if matches!(self.api.call(Operation::ListTasks { after: None, limit: 1 }).await,
                Ok(Reply { outcome: Outcome::Ok { .. }, .. })) { break; }
            assert!(tokio::time::Instant::now() < until, "controller readiness timed out");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        child
    }
    fn worker(&self, endpoint: &str, incarnation: &str) -> Process {
        self.worker_as(endpoint, WORKER_ID, incarnation)
    }
    fn worker_as(&self, endpoint: &str, worker_id: &str, incarnation: &str) -> Process {
        Process::start(&self.image, &[
            "worker".into(), "--limits".into(), self.root.join("limits.json").display().to_string(),
            "--controller".into(), self.origin.clone(), "--id".into(), worker_id.into(),
            "--incarnation".into(), incarnation.into(), "--workspace-root".into(), self.root.join("worker").display().to_string(),
            "--codex".into(), "/usr/bin/false".into(), "--coding-capacity".into(), "0".into(),
            "--native-capacity".into(), "1".into(), "--native-endpoint".into(), endpoint.into(),
        ], &self.root, incarnation)
    }
    fn state(&self) -> PathBuf { self.state_for(WORKER_ID) }
    fn state_for(&self, worker_id: &str) -> PathBuf {
        self.root.join("worker/.borg-worker").join(process::component(worker_id))
    }
    fn completed_command(&self, attempt_id: &str) {
        let mut found = false;
        for entry in std::fs::read_dir(self.state().join("commands")).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") { continue; }
            let saved: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            let command: WorkerCommand = serde_json::from_value(saved["command"].clone()).unwrap();
            if matches!(command, WorkerCommand::RunAttempt { ref attempt, .. } if attempt.id == attempt_id) {
                found = true;
                assert_eq!(saved["completed"], true, "restart left the original run command resumable");
                assert_eq!(saved["effects_started"], true);
            }
        }
        assert!(found, "original durable run command is absent");
    }
    async fn submit(&self, requests: u32) -> Task {
        self.submit_named(requests, "native-retry", "Return JSON with result equal to ok.").await
    }
    async fn submit_named(&self, requests: u32, label: &str, instruction: &str) -> Task {
        let input = TaskInput { instruction_source: None,
            pull_request: None,
            resolved_limits: None, instruction: instruction.into(),
            definition: Definition { limit_overrides: None, charter: None, id: format!("{label}-fixture"), revision: "one".into(),
                executor: ExecutorKind::Native, inference: Some(InferenceSettings::selected()),
                execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }) },
            repository: None, repository_read: None, acceptance_commands: vec![], dependencies: vec![], native_workflow: None,
            output_schema: Some(json!({"type":"object","properties":{"result":{"type":"string"}},"required":["result"],"additionalProperties":false})),
            limits: Limits { deadline_unix_ms: process::now_ms() + 120_000, model_requests: requests,
                output_bytes: self.limits.task_defaults.output_bytes, result_bytes: self.limits.task_defaults.result_bytes,
                max_delegation_depth: 0 },
        };
        let reply = self.api.call(Operation::SubmitTask { operation_id: format!("submit-{label}"), input }).await.unwrap();
        let task = match reply.outcome {
            Outcome::Ok { data: Data::Task(task) } => task,
            other => panic!("submission refused: {other:?}"),
        };
        assert_eq!(task.input.resolved_limits.as_ref().unwrap().global_metadata.native_retry, self.limits.native_retry);
        *task
    }
    async fn registered(&self, incarnation: &str, worker: &mut Process) {
        let until = tokio::time::Instant::now() + WAIT;
        loop {
            worker.live();
            if let Ok(bytes) = std::fs::read(self.state().join("mutations/registration.json")) {
                let saved: Value = serde_json::from_slice(&bytes).unwrap();
                let request: Operation = serde_json::from_value(saved["request"].clone()).unwrap();
                if let Operation::RegisterWorker { operation_id, worker: selected } = request {
                    if selected.incarnation == incarnation {
                        if let Some((_, record)) = lookup(&self.api, &operation_id).await {
                            let outcome: Outcome = serde_json::from_value(record.outcome.unwrap()).unwrap();
                            if matches!(outcome, Outcome::Ok { data: Data::Worker(actual) } if actual == selected) { break; }
                        }
                    }
                }
            }
            assert!(tokio::time::Instant::now() < until, "replacement worker registration timed out");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

async fn get(api: &ApiClient, task_id: &str) -> Task {
    let reply = api.call(Operation::GetTask { task_id: task_id.into() }).await.unwrap();
    match reply.outcome { Outcome::Ok { data: Data::Task(task) } => *task, other => panic!("task unavailable: {other:?}") }
}
async fn until_task(api: &ApiClient, task_id: &str, ready: impl Fn(&Task) -> bool) -> Task {
    let until = tokio::time::Instant::now() + WAIT;
    loop {
        let task = get(api, task_id).await;
        if ready(&task) { return task; }
        assert!(tokio::time::Instant::now() < until, "task did not reach fixture boundary: {:?}", task.status);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
async fn lookup(api: &ApiClient, id: &str) -> Option<(Reply, OperationRecord)> {
    let reply = api.call(Operation::GetOperation { operation_id: id.into() }).await.unwrap();
    match &reply.outcome {
        Outcome::Ok { data: Data::Operation(record) } => Some((reply.clone(), record.clone())),
        Outcome::Refused { code, .. } if code == "not_found" => None,
        other => panic!("receipt lookup failed: {other:?}"),
    }
}
async fn receipt(api: &ApiClient, id: &str) -> (Reply, OperationRecord) {
    lookup(api, id).await.unwrap_or_else(|| panic!("missing canonical receipt: {id}"))
}
async fn until_receipt(api: &ApiClient, id: &str) -> (Reply, OperationRecord) {
    let until = tokio::time::Instant::now() + WAIT;
    loop {
        if let Some(value) = lookup(api, id).await { return value; }
        assert!(tokio::time::Instant::now() < until, "receipt did not commit: {id}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
fn report(record: &OperationRecord) -> Report {
    assert!(matches!(serde_json::from_value::<Outcome>(record.outcome.clone().unwrap()).unwrap(), Outcome::Ok { .. }));
    match serde_json::from_value::<Operation>(record.intended.clone()).unwrap() {
        Operation::ObserveWorker { report } => report,
        other => panic!("receipt is not an actual worker report: {other:?}"),
    }
}
fn committed(reply: &Reply) -> u64 { reply.commit.as_ref().expect("canonical mutation commit").controller_revision }
fn rejection_id(attempt: &str, index: u32) -> String { format!("{attempt}-native-rejection-{index}") }

#[derive(Clone, Copy)]
enum Replies { RejectThenSuccess { rejected: u32, delay_ms: u64 }, LostResponse, Format { invalid: u32, lost_at: Option<u32> }, Cap {partial_call:bool,lost_at:Option<u32>}, Authentication, CircuitProbeLoss }
struct Seen { request: Value, wire_body: Vec<u8>, charge_revision: u64, received: tokio::time::Instant, replied: Option<tokio::time::Instant>, task_id: String, received_unix_ms: u64 }
struct Peer { endpoint: String, seen: Arc<Mutex<Vec<Seen>>>, owner: Option<tokio::task::JoinHandle<()>> }
impl Peer {
    async fn start(lab: &Lab, task_id: &str, replies: Replies) -> Self {
        Self::start_inner(lab, Some(task_id.to_owned()), replies, None).await
    }
    async fn start_auth(lab: &Lab) -> (Self, tokio::sync::oneshot::Receiver<tokio::net::TcpStream>) {
        let (send, receive) = tokio::sync::oneshot::channel();
        (Self::start_inner(lab, None, Replies::Authentication, Some(send)).await, receive)
    }
    async fn start_inner(lab: &Lab, task_id: Option<String>, replies: Replies,
        mut probe: Option<tokio::sync::oneshot::Sender<tokio::net::TcpStream>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = seen.clone();
        let api = lab.api.clone();
        let policy = lab.limits.clone();
        let owner = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut header = Vec::new();
                while !header.ends_with(b"\r\n\r\n") {
                    header.push(stream.read_u8().await.unwrap());
                    assert!(header.len() <= policy.transport.http_header_bytes, "fixture header bound");
                }
                let header = String::from_utf8(header).unwrap();
                assert!(header.starts_with("POST /v1/responses HTTP/1.1\r\n"));
                let length: usize = header.lines().find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length").then(|| value.trim().parse().unwrap())
                }).unwrap();
                assert!(length <= policy.execution.native.request_bytes);
                let mut body = vec![0; length];
                stream.read_exact(&mut body).await.unwrap();
                let received_unix_ms = process::now_ms();
                let request: Value = serde_json::from_slice(&body).unwrap();
                assert_eq!(request["model"], "gpt-6-astra");
                assert_eq!(request["reasoning"], json!({"effort":"max"}));
                assert_eq!(request["store"], false);
                assert!(request.get("previous_response_id").is_none());
                let task = if let Some(task_id) = &task_id { get(&api, task_id).await } else {
                    // Match the actual accepted instruction carried by this POST;
                    // no task assignment or provider outcome is fabricated here.
                    let listed = api.call(Operation::ListTasks { after: None, limit: 16 }).await.unwrap();
                    let Outcome::Ok { data: Data::Tasks(tasks) } = listed.outcome else { panic!("auth fixture tasks unavailable") };
                    let serialized = serde_json::to_string(&request).unwrap();
                    let mut matches = tasks.into_iter().filter(|task| serialized.contains(&task.input.instruction));
                    let task = matches.next().expect("POST has no exact fixture instruction");
                    assert!(matches.next().is_none(), "POST matches more than one fixture task");
                    task
                };
                let offset = captured.lock().unwrap().len();
                let index = u32::try_from(captured.lock().unwrap().iter()
                    .filter(|seen: &&Seen| seen.task_id == task.id).count()).unwrap() + 1;
                // Check the canonical receipt while this physical POST is here,
                // before returning any response. Reservation cannot occur later.
                let attempt = task.attempt.as_ref().unwrap();
                let (charged, record) = receipt(&api, &contract::reservation_id(&attempt.id, index)).await;
                let charged_report = report(&record);
                assert_eq!(charged_report.attempt_id, attempt.id);
                assert_eq!(charged_report.generation, attempt.generation);
                assert!(matches!(charged_report.observation, Observation::Usage { model_requests, .. } if model_requests == index));
                captured.lock().unwrap().push(Seen { request, wire_body: body, charge_revision: committed(&charged),
                    received: tokio::time::Instant::now(), replied: None, task_id: task.id, received_unix_ms });
                if (matches!(replies, Replies::Authentication) && offset == 1)
                    || (matches!(replies, Replies::CircuitProbeLoss) && offset == 4) {
                    // Transfer just this response to the test. The listener keeps
                    // accepting, so a second probe cannot hide behind the barrier.
                    probe.take().unwrap().send(stream).expect("auth probe receiver closed");
                    continue;
                }
                if matches!(replies, Replies::LostResponse)
                    || (matches!(replies, Replies::CircuitProbeLoss) && offset < 4)
                    || matches!(replies, Replies::Format {lost_at:Some(at),..}|Replies::Cap {lost_at:Some(at),..} if at == index) {
                    stream.shutdown().await.unwrap();
                    continue;
                }
                let (rejected, delay_ms) = match replies {
                    Replies::RejectThenSuccess {rejected,delay_ms} => (rejected,delay_ms),
                    Replies::Format {..}|Replies::Cap {..}|Replies::Authentication|Replies::CircuitProbeLoss => (0,0),
                    _ => unreachable!(),
                };
                let final_text = match replies {
                    Replies::Format {invalid,..} if index <= invalid => if index == 1 {"not JSON"} else {"{\"result\":7}"},
                    Replies::Cap {partial_call:false,..} if index>1 => "ok\"}",
                    _ => "{\"result\":\"ok\"}",
                };
                let (status, extra, body) = if matches!(replies, Replies::Authentication) && offset == 0 {
                    (401, String::new(), serde_json::to_vec(&json!({"error":{
                        "code":"invalid_api_key", "type":"authentication_error"}})).unwrap())
                } else if matches!(replies,Replies::Cap {..}) && index==1 {
                    let output=if matches!(replies,Replies::Cap {partial_call:true,..}) {json!([{"type":"function_call","id":"cap-item","call_id":"cap-call","name":"collab_task_get","arguments":"{\"task_id\":","status":"incomplete"}])}
                        else {json!([{"type":"message","role":"assistant","status":"incomplete","content":[{"type":"output_text","text":"{\"result\":\""}]}])};
                    (200,String::new(),serde_json::to_vec(&json!({"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"usage":{"input_tokens":5,"output_tokens":1,"total_tokens":6},"output":output})).unwrap())
                } else if index <= rejected {
                    (429, format!("retry-after-ms: {delay_ms}\r\nx-request-id: req_0123456789abcdef0123456789abcdef\r\n"),
                        serde_json::to_vec(&json!({"error":{"code":"rate_limit_exceeded","type":"rate_limit_error"}})).unwrap())
                } else {
                    (200, String::new(), serde_json::to_vec(&json!({"status":"completed",
                        "usage":{"input_tokens":5,"output_tokens":1,"total_tokens":6},
                        "output":[{"type":"message","role":"assistant","status":"completed",
                            "content":[{"type":"output_text","text":final_text}]}]})).unwrap())
                };
                let response = format!("HTTP/1.1 {status} fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{extra}Connection: close\r\n\r\n", body.len());
                // The timestamp precedes sending the body, hence a retry must
                // never arrive earlier than this plus the provider floor.
                captured.lock().unwrap()[offset].replied = Some(tokio::time::Instant::now());
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.write_all(&body).await.unwrap();
                stream.shutdown().await.unwrap();
            }
        });
        Self { endpoint, seen, owner: Some(owner) }
    }
    fn count(&self) -> usize { self.seen.lock().unwrap().len() }
    async fn quiet(&self, expected: usize) {
        assert_eq!(self.count(), expected);
        // The listener remains alive and will accept/count an erroneous POST.
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(self.count(), expected, "provider was called again after the terminal/recovery boundary");
    }
    async fn close(&mut self) {
        let owner = self.owner.take().unwrap();
        owner.abort();
        if let Err(error) = owner.await { assert!(error.is_cancelled(), "provider fixture panicked: {error}"); }
    }
}
impl Drop for Peer { fn drop(&mut self) { if let Some(owner) = &self.owner { owner.abort(); } } }

async fn charged_once(api: &ApiClient, attempt: &Attempt, index: u32) -> (Reply, OperationRecord) {
    let value = receipt(api, &contract::reservation_id(&attempt.id, index)).await;
    let report = report(&value.1);
    assert_eq!(report.report_id, contract::reservation_id(&attempt.id, index));
    assert_eq!(report.attempt_id, attempt.id);
    assert_eq!(report.generation, attempt.generation);
    assert!(matches!(report.observation, Observation::Usage { model_requests, input_tokens:None, output_tokens:None, .. } if model_requests == index));
    value
}
async fn rejection(api: &ApiClient, attempt: &Attempt, index: u32, peer: &Peer, delay_ms: u64) -> (Reply, OperationRecord) {
    let value = until_receipt(api, &rejection_id(&attempt.id, index)).await;
    let report = report(&value.1);
    assert_eq!(report.report_id, rejection_id(&attempt.id, index));
    assert_eq!(report.attempt_id, attempt.id);
    assert_eq!(report.generation, attempt.generation);
    let Observation::NativeRequestRejected { request_index, endpoint: actual_endpoint, model, rejection, diagnostic, request_binding } = report.observation else {
        panic!("known rejection receipt has wrong observation");
    };
    assert_eq!(request_index, index);
    assert_eq!(actual_endpoint, peer.endpoint);
    let expected_binding = {
        let seen = peer.seen.lock().unwrap();
        let body = &seen[usize::try_from(index - 1).unwrap()].wire_body;
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, body);
        ProviderRequestBinding {
            bytes: u64::try_from(body.len()).unwrap(),
            sha256: digest.as_ref().iter().map(|byte| format!("{byte:02x}")).collect(),
        }
    };
    assert_eq!(request_binding, expected_binding, "canonical rejection must bind the exact received HTTP body");
    assert_eq!(model, "gpt-6-astra");
    assert_eq!(rejection.kind, RejectionKind::RateLimited);
    assert_eq!(rejection.retry_after, RetryAfter::DelayMs(delay_ms));
    assert!(diagnostic.contains("code=rate_limit_exceeded"));
    assert!(!diagnostic.contains("synthetic-native-retry-fixture-key"));
    value
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A platform-owned model request rejected as transient MUST be retried with backoff and jitter within the task's remaining model-request allowance and execution deadline.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A retry of a throttled platform-owned model request MUST honor a valid provider retry-after value.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# Every platform-owned model request dispatched by a retry MUST have an acknowledged durable request reservation before dispatch.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# Each newly dispatched platform-owned model attempt MUST carry a distinct retained request-attempt identity.
async fn known_429_retries_charge_each_post_and_accept_without_fake_tool_groups() {
    for rejected in [1, 3] {
        let total = rejected + 1;
        let lab = Lab::new(&format!("native-retry-success-on-post-{total}"));
        let mut controller = lab.controller("controller-one").await;
        let submitted = lab.submit(total).await;
        let mut peer = Peer::start(&lab, &submitted.id, Replies::RejectThenSuccess { rejected, delay_ms: 20 }).await;
        let mut worker = lab.worker(&peer.endpoint, "worker-one");
        let accepted = until_task(&lab.api, &submitted.id, |task| task.status == TaskStatus::Succeeded).await;
        let attempt = accepted.attempt.as_ref().unwrap();
        assert!(attempt.stopped && !attempt.uncertain);
        assert_eq!(accepted.session_id, submitted.session_id);
        assert_eq!(attempt.deadline_unix_ms, submitted.input.limits.deadline_unix_ms);
        assert_eq!(accepted.input, submitted.input, "retry must retain the accepted snapshot and budget");
        assert_eq!(accepted.result.as_ref().unwrap().output, Some(json!({"result":"ok"})));
        let mut prior = 0;
        for index in 1..=total {
            let (charged, _) = charged_once(&lab.api, attempt, index).await;
            let (health_reply, health_record) = receipt(&lab.api, &format!("{}-native-health-{index}",attempt.id)).await;
            let health_outcome:Outcome=serde_json::from_value(health_record.outcome.clone().unwrap()).unwrap();
            let Outcome::Ok {data:Data::NativeRequestSettlement(health)}=&health_outcome else {panic!("missing canonical health settlement")};
            assert_eq!(health.permit.request_index,index);
            assert_eq!(health.permit.circuit.caller.attempt_id,attempt.id);
            assert_eq!(health.source,borg_runtime::native_retry::NativeSettlementSource::Reported);
            let (grant_reply, grant_record)=receipt(&lab.api,&health.permit.circuit.operation_id).await;
            let grant_outcome:Outcome=serde_json::from_value(grant_record.outcome.clone().unwrap()).unwrap();
            assert!(matches!(&grant_outcome,Outcome::Ok {data:Data::NativeRequestAdmission(borg_runtime::native_retry::NativeRequestAdmission::Granted{permit})} if permit==&health.permit));
            assert!(committed(&grant_reply)<committed(&charged));
            assert!(committed(&health_reply)>committed(&charged));
            if index==total {assert_eq!(health.outcome,borg_runtime::native_retry::NativeRequestOutcome::Healthy)}
            else {assert!(matches!(health.outcome,borg_runtime::native_retry::NativeRequestOutcome::Rejected{..}))}
            assert!(committed(&charged) > prior);
            assert_eq!(peer.seen.lock().unwrap()[index as usize - 1].charge_revision, committed(&charged));
            prior = committed(&charged);
            if index < total {
                let (rejected, _) = rejection(&lab.api, attempt, index, &peer, 20).await;
                assert!(committed(&rejected) > prior);
                assert!(committed(&health_reply)>committed(&rejected));
                prior = committed(&health_reply);
            }
        }
        let (_, usage) = receipt(&lab.api, &contract::usage_id(&attempt.id, total)).await;
        assert!(matches!(report(&usage).observation, Observation::Usage {
            model_requests, input_tokens:Some(5), output_tokens:Some(1), ..
        } if model_requests == total));
        let (completion_reply, completion) = receipt(&lab.api, &contract::completion_id(&attempt.id)).await;
        assert!(committed(&completion_reply) > prior);
        let Observation::NativeLoopCompleted { model_requests, calls, requests, final_response_index } = report(&completion).observation else { panic!("missing loop completion proof") };
        assert_eq!(model_requests, total);
        assert_eq!(final_response_index.unwrap_or(model_requests), total);
        assert!(calls.is_empty(), "rejections must not manufacture tool groups");
        assert_eq!(requests.len(), rejected as usize);
        for (offset, request) in requests.iter().enumerate() {
            let index = u32::try_from(offset + 1).unwrap();
            assert!(matches!(request, NativeRequestEvidence::ConfirmedRejection { request_index, operation_id }
                if *request_index == index && operation_id == &rejection_id(&attempt.id, index)));
        }
        assert!(accepted.result.as_ref().unwrap().validation.contains(&contract::evidence(&attempt.id)));
        assert!(lookup(&lab.api, &contract::reservation_id(&attempt.id, total + 1)).await.is_none());
        {
            let seen = peer.seen.lock().unwrap();
            assert_eq!(seen.len(), total as usize);
            for pair in seen.windows(2) {
                assert_eq!(pair[0].request, pair[1].request, "same logical request changed during rejection retry");
                assert_eq!(pair[0].wire_body, pair[1].wire_body,
                    "every retry, including the successful final POST, must use identical serialized input bytes");
                assert!(pair[1].received.duration_since(pair[0].replied.unwrap()) >= Duration::from_millis(20));
            }
        }
        peer.quiet(total as usize).await;
        process::atomic_json(&lab.root.join("accepted.json"), &accepted).unwrap();
        worker.stop().await;
        controller.stop().await;
        let mut controller = lab.controller("controller-two").await;
        assert_eq!(get(&lab.api, &accepted.id).await, accepted);
        let replay = receipt(&lab.api, &contract::completion_id(&attempt.id)).await;
        assert_eq!(replay.0.commit, completion_reply.commit);
        assert_eq!(replay.1, completion);
        peer.quiet(total as usize).await;
        controller.stop().await;
        peer.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A platform-owned retry MUST revalidate its current task, attempt, ownership and cancellation state before dispatch.
async fn cancellation_interrupts_retry_after_without_a_second_charge_or_post() {
    let lab = Lab::new("native-retry-cancel-wait");
    let mut controller = lab.controller("controller").await;
    let submitted = lab.submit(4).await;
    let mut peer = Peer::start(&lab, &submitted.id, Replies::RejectThenSuccess { rejected: 1, delay_ms: 60_000 }).await;
    let mut worker = lab.worker(&peer.endpoint, "worker-one");
    let assigned = until_task(&lab.api, &submitted.id, |task| task.attempt.is_some()).await;
    let attempt = assigned.attempt.as_ref().unwrap();
    let original_rejection = rejection(&lab.api, attempt, 1, &peer, 60_000).await;
    let cancel_started = tokio::time::Instant::now();
    let mut index = 0u32;
    loop {
        let current = get(&lab.api, &submitted.id).await;
        index += 1;
        let reply = lab.api.call(Operation::CancelTask { operation_id: format!("cancel-retry-{index}"),
            task_id: current.id, expected_revision: current.revision }).await.unwrap();
        match reply.outcome {
            Outcome::Ok { .. } => { assert!(reply.commit.is_some()); break; }
            Outcome::Refused { ref code, .. } if code == "stale_revision" => {}
            other => panic!("cancel refused: {other:?}"),
        }
        assert!(cancel_started.elapsed() < WAIT, "cancel could not bind a current revision");
    }
    let cancelled = until_task(&lab.api, &submitted.id, |task| task.status == TaskStatus::Cancelled
        && task.attempt.as_ref().is_some_and(|attempt| attempt.stopped)).await;
    assert!(cancel_started.elapsed() < WAIT, "worker waited out Retry-After instead of cancelling");
    assert!(!cancelled.attempt.as_ref().unwrap().uncertain);
    assert!(cancelled.result.is_none());
    charged_once(&lab.api, attempt, 1).await;
    assert!(lookup(&lab.api, &contract::reservation_id(&attempt.id, 2)).await.is_none());
    assert!(lookup(&lab.api, &contract::completion_id(&attempt.id)).await.is_none());
    assert_eq!(receipt(&lab.api, &rejection_id(&attempt.id, 1)).await.1, original_rejection.1);
    peer.quiet(1).await;
    worker.stop().await; controller.stop().await; peer.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn known_rejection_then_worker_loss_preserves_receipt_without_automatic_post() {
    let lab = Lab::new("native-retry-known-worker-loss");
    let mut controller = lab.controller("controller").await;
    let submitted = lab.submit(4).await;
    let mut peer = Peer::start(&lab, &submitted.id, Replies::RejectThenSuccess { rejected: 1, delay_ms: 60_000 }).await;
    let mut first = lab.worker(&peer.endpoint, "worker-one");
    let assigned = until_task(&lab.api, &submitted.id, |task| task.attempt.is_some()).await;
    let attempt = assigned.attempt.as_ref().unwrap();
    let original_rejection = rejection(&lab.api, attempt, 1, &peer, 60_000).await;
    let original_charge = charged_once(&lab.api, attempt, 1).await;
    first.hard_loss();
    let mut replacement = lab.worker(&peer.endpoint, "worker-two");
    lab.registered("worker-two", &mut replacement).await;
    let recovered = until_task(&lab.api, &submitted.id, |task| task.attempt.as_ref().is_some_and(|attempt| attempt.stopped)).await;
    assert_eq!(recovered.attempt.as_ref().unwrap().id, attempt.id);
    assert_eq!(recovered.attempt.as_ref().unwrap().deadline_unix_ms, attempt.deadline_unix_ms);
    assert!(recovered.result.is_none());
    lab.completed_command(&attempt.id);
    // The eventual shared blocked-state reason is intentionally not invented.
    // This already proves restart does not dispatch the pending retry.
    let replay = receipt(&lab.api, &rejection_id(&attempt.id, 1)).await;
    assert_eq!(replay.0.commit, original_rejection.0.commit);
    assert_eq!(replay.1, original_rejection.1);
    assert_eq!(receipt(&lab.api, &contract::reservation_id(&attempt.id, 1)).await.1, original_charge.1);
    assert!(lookup(&lab.api, &contract::reservation_id(&attempt.id, 2)).await.is_none());
    peer.quiet(1).await;
    replacement.stop().await; controller.stop().await; peer.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A local transport close or process cleanup MUST NOT establish that an ambiguous provider request had no effect.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A new circuit-breaker probe after a prior unknown outcome MUST NOT replay the original uncertain request.
// Traceability scope: With no supported lookup for the lost response, the actual worker restart retains the original unknown request, refuses RetryTask and sends no second provider POST.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A platform-owned retry of an uncertain provider effect MUST use the receiving interface's supported outcome reconciliation or deduplication before resending that effect.
async fn lost_response_stays_unknown_and_restart_does_not_consume_the_remaining_budget() {
    let lab = Lab::new("native-retry-lost-response");
    let mut controller = lab.controller("controller").await;
    let submitted = lab.submit(4).await;
    let mut peer = Peer::start(&lab, &submitted.id, Replies::LostResponse).await;
    let mut first = lab.worker(&peer.endpoint, "worker-one");
    let unknown = until_task(&lab.api, &submitted.id, |task| task.attempt.as_ref().is_some_and(|a| a.stopped && a.uncertain)).await;
    let attempt = unknown.attempt.as_ref().unwrap();
    assert!(unknown.result.is_none());
    let original_charge = charged_once(&lab.api, attempt, 1).await;
    assert!(lookup(&lab.api, &rejection_id(&attempt.id, 1)).await.is_none());
    assert!(lookup(&lab.api, &contract::completion_id(&attempt.id)).await.is_none());
    peer.quiet(1).await;
    first.stop().await;
    let mut replacement = lab.worker(&peer.endpoint, "worker-two");
    lab.registered("worker-two", &mut replacement).await;
    let retained = get(&lab.api, &submitted.id).await;
    assert_eq!(retained.attempt.as_ref().unwrap().id, attempt.id);
    assert!(retained.attempt.as_ref().unwrap().uncertain);
    lab.completed_command(&attempt.id);
    let refused = lab.api.call(Operation::RetryTask { operation_id: "retry-unreconciled-loss".into(),
        task_id: retained.id, expected_revision: retained.revision }).await.unwrap();
    assert!(matches!(refused.outcome, Outcome::Refused { code, .. } if code == "reconciliation_required"));
    assert_eq!(receipt(&lab.api, &contract::reservation_id(&attempt.id, 1)).await.1, original_charge.1);
    assert!(lookup(&lab.api, &contract::reservation_id(&attempt.id, 2)).await.is_none());
    peer.quiet(1).await;
    replacement.stop().await; controller.stop().await; peer.close().await;
}


#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# Exhaustion of the retry allowance or deadline MUST leave an explicit reason-bearing blocked state without another provider dispatch.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# Every transition into a retry-related blocked state MUST publish its reason as a durable event.
async fn confirmed_rejection_at_request_allowance_commits_blocked_without_result_or_extra_post() {
    let lab = Lab::new("native-retry-allowance-blocked");
    let mut controller = lab.controller("controller-one").await;
    let submitted = lab.submit(1).await;
    // A second POST would succeed, so acceptance would expose an illegal retry.
    let mut peer = Peer::start(&lab, &submitted.id,
        Replies::RejectThenSuccess { rejected: 1, delay_ms: 20 }).await;
    let mut worker = lab.worker(&peer.endpoint, "worker-one");
    let blocked = until_task(&lab.api, &submitted.id, |task|
        task.status == TaskStatus::Blocked
            && task.attempt.as_ref().is_some_and(|attempt| attempt.stopped)).await;
    let attempt = blocked.attempt.as_ref().unwrap();
    assert_eq!(attempt.phase, AttemptPhase::Stopped);
    assert!(!attempt.uncertain, "a complete known rejection must not become unknown execution");
    assert_eq!(blocked.input, submitted.input);
    assert_eq!(blocked.session_id, submitted.session_id);
    assert_eq!(attempt.deadline_unix_ms, submitted.input.limits.deadline_unix_ms);
    assert!(blocked.result.is_none(), "allowance exhaustion is not a failed or successful result");
    let expected = NativeRetryBlock {
        request_index: 1,
        source_operation_id: rejection_id(&attempt.id, 1),
        reason: RetryBlockReason::RequestAllowance,
    };
    assert_eq!(blocked.native_retry_block.as_ref(), Some(&expected));
    assert_eq!(blocked.blocked_reason.as_deref(), Some("native_request_allowance_exhausted"));
    let charge = charged_once(&lab.api, attempt, 1).await;
    let rejection = rejection(&lab.api, attempt, 1, &peer, 20).await;
    let block_id = format!("{}-native-retry-block-1", attempt.id);
    let original_block = receipt(&lab.api, &block_id).await;
    let block_report = report(&original_block.1);
    assert_eq!(block_report.report_id, block_id);
    assert_eq!(block_report.task_id, blocked.id);
    assert_eq!(block_report.attempt_id, attempt.id);
    assert_eq!(block_report.generation, attempt.generation);
    assert!(matches!(block_report.observation,
        Observation::NativeRetryBlocked { block } if block == expected));
    assert!(committed(&charge.0) < committed(&rejection.0));
    assert!(committed(&rejection.0) < committed(&original_block.0));
    assert!(lookup(&lab.api, &contract::reservation_id(&attempt.id, 2)).await.is_none());
    assert!(lookup(&lab.api, &contract::completion_id(&attempt.id)).await.is_none());
    peer.quiet(1).await;
    worker.stop().await;
    lab.completed_command(&attempt.id);
    controller.stop().await;
    let mut controller = lab.controller("controller-two").await;
    let retained = get(&lab.api, &submitted.id).await;
    assert_eq!(retained.status, TaskStatus::Blocked);
    assert_eq!(retained.native_retry_block.as_ref(), Some(&expected));
    assert!(retained.result.is_none());
    assert!(retained.attempt.as_ref().is_some_and(|original|
        original.id == attempt.id && original.generation == attempt.generation
            && original.stopped && !original.uncertain));
    let replay = receipt(&lab.api, &block_id).await;
    assert_eq!(replay.0.commit, original_block.0.commit);
    assert_eq!(replay.1, original_block.1);
    assert_eq!(receipt(&lab.api, &rejection_id(&attempt.id, 1)).await.1, rejection.1);
    assert_eq!(receipt(&lab.api, &contract::reservation_id(&attempt.id, 1)).await.1, charge.1);
    assert!(lookup(&lab.api, &contract::reservation_id(&attempt.id, 2)).await.is_none());
    peer.quiet(1).await;
    controller.stop().await;
    peer.close().await;
}

// Read the worker's real journal only to locate its original controller receipt.
// The journal is never edited, and an uncommitted journal is not admission proof.
async fn acquisition(lab: &Lab, task: &Task) -> Option<(Operation, Reply, OperationRecord,
    borg_runtime::native_retry::NativeRequestAdmission)> {
    let attempt = task.attempt.as_ref()?;
    let path = lab.state_for(&attempt.worker_id).join("native-retry")
        .join(process::component(&attempt.id)).join("0000000001-acquire.json");
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(error) => panic!("acquisition journal unavailable: {error}"),
    };
    assert!(bytes.len() <= lab.limits.worker.mutation_record_bytes);
    let saved: Value = serde_json::from_slice(&bytes).unwrap();
    let request: Operation = serde_json::from_value(saved["request"].clone()).unwrap();
    let Operation::AcquireNativeRequest { operation_id, caller, request_index, key,
        credential_configuration } = &request else { panic!("unexpected acquisition journal") };
    assert_eq!(caller.task_id, task.id);
    assert_eq!(caller.attempt_id, attempt.id);
    assert_eq!(caller.generation, attempt.generation);
    assert_eq!(caller.worker_id, attempt.worker_id);
    assert_eq!(caller.incarnation, attempt.worker_incarnation);
    assert_eq!(*request_index, 1);
    assert_eq!(key.model, "gpt-6-astra");
    assert_eq!(credential_configuration, "OPENAI_API_KEY");
    let (reply, record) = lookup(&lab.api, operation_id).await?;
    assert_eq!(record.intended, serde_json::to_value(&request).unwrap());
    let Outcome::Ok { data: Data::NativeRequestAdmission(admission) } =
        serde_json::from_value(record.outcome.clone().unwrap()).unwrap()
        else { panic!("actual acquisition refused") };
    Some((request, reply, record, admission))
}

async fn authentication_wait(lab: &Lab, task: &Task, reason: borg_runtime::native_retry::NativeWaitReason)
    -> (Operation, Reply, OperationRecord, u64, Option<u64>) {
    let until = tokio::time::Instant::now() + WAIT;
    loop {
        if let Some((request, reply, record, borg_runtime::native_retry::NativeRequestAdmission::Wait {
            reason: actual, retry_at_ms, generation })) = acquisition(lab, task).await {
            if actual == reason { return (request, reply, record, generation, retry_at_ms); }
        }
        assert!(tokio::time::Instant::now() < until, "worker did not commit {reason:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn cancelled_with_acceptance_notice(lab: &Lab, cancelled: &Task) -> Task {
    let attempt = cancelled.attempt.as_ref().unwrap();
    let directory = lab.state_for(&attempt.worker_id).join("outbox/acceptance");
    let until = tokio::time::Instant::now() + WAIT;
    let (original, notice, saved) = loop {
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => Some(entries),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => panic!("acceptance directory unavailable: {error}"),
        };
        let mut selected = None;
        if let Some(entries) = entries {
            for (index, entry) in entries.enumerate() {
                assert!(index < lab.limits.worker.outbox_records);
                let path = entry.unwrap().path();
                if !matches!(path.extension().and_then(|value| value.to_str()), Some("json" | "settled")) { continue; }
                let bytes = match std::fs::read(path) {
                    Ok(bytes) => bytes,
                    // The real cleanup owner may rename json to settled here.
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => panic!("acceptance journal unavailable: {error}"),
                };
                assert!(bytes.len() <= lab.limits.worker.mutation_record_bytes);
                let saved: Value = serde_json::from_slice(&bytes).unwrap();
                let original: Report = serde_json::from_value(saved["original"].clone()).unwrap();
                if original.attempt_id != attempt.id { continue; }
                let notice: Report = serde_json::from_value(saved["notice"].clone()).unwrap();
                assert!(selected.replace((original, notice, saved)).is_none(), "duplicate acceptance source");
            }
        }
        if let Some(selected) = selected { break selected; }
        assert!(tokio::time::Instant::now() < until, "cancelled attempt acceptance source not retained");
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(original.task_id, cancelled.id);
    assert_eq!(original.generation, attempt.generation);
    assert_eq!(original.worker_id, attempt.worker_id);
    assert_eq!(original.incarnation, attempt.worker_incarnation);
    assert!(matches!(&original.observation, Observation::ResultCandidate {
        checks_passed: false, checkpoint: None, output: None, validation, ..
    } if validation.is_empty()));
    let refused = receipt(&lab.api, &original.report_id).await;
    assert_eq!(refused.1.intended, serde_json::to_value(Operation::ObserveWorker { report: original.clone() }).unwrap());
    assert_eq!(refused.1.outcome.as_ref().unwrap(), &saved["refusal"]);
    assert_eq!(serde_json::to_value(refused.0.commit.as_ref().unwrap()).unwrap(), saved["refusal_context"]["commit"]);
    assert!(matches!(serde_json::from_value::<Outcome>(saved["refusal"].clone()).unwrap(),
        Outcome::Refused { code, .. } if code == "invalid_transition"));
    let text = "Completed result acceptance is refused; original decision retained for reconciliation";
    let mut expected_notice = original;
    expected_notice.report_id = notice.report_id.clone();
    expected_notice.observation = Observation::Progress { text: text.into(), output: String::new(), truncated: false };
    assert_eq!(notice, expected_notice);
    let committed_notice = until_receipt(&lab.api, &notice.report_id).await;
    assert_eq!(report(&committed_notice.1), notice);
    assert!(committed(&refused.0) < committed(&committed_notice.0));
    let Outcome::Ok { data: Data::Task(retained) } =
        serde_json::from_value::<Outcome>(committed_notice.1.outcome.unwrap()).unwrap()
        else { panic!("known acceptance notice did not retain a task") };
    // Compare every Task field. Only the exact receipt-proven Progress transition
    // may follow the stopped cancellation snapshot; an arbitrary later change fails.
    let mut expected = cancelled.clone();
    if expected.progress != text {
        assert!(expected.progress.is_empty());
        expected.revision = expected.revision.checked_add(1).unwrap();
        expected.progress = text.into();
    }
    assert_eq!(*retained, expected);
    *retained
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// Traceability scope: Two actual workers share one controlled dependency and credential-configuration cooldown, retain the original alert across controller restart, and admit one probe only after the recorded due time.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A provider authentication failure MUST be coordinated across workers for the same dependency and credential-configuration identity with metadata-controlled backoff and a retained alert.
async fn authentication_cooldown_coordinates_two_workers_across_controller_restart() {
    use borg_runtime::native_retry::{NativeRequestAdmission, NativeRequestOutcome, NativeWaitReason};
    // Fixture policy only. The real retained retry_at controls every due-time
    // assertion; the HTTP barrier, not a latency guess, holds the admitted probe.
    let lab = Lab::with_auth_cooldown("native-auth-two-workers", 30_000);
    let mut controller = lab.controller("controller-one").await;
    let failed_input = lab.submit_named(3, "auth-first", "Auth first fixture. Return JSON with result equal to ok.").await;
    let (mut peer, probe) = Peer::start_auth(&lab).await;
    let mut first = lab.worker_as(&peer.endpoint, "auth-worker-a", "auth-incarnation-a");
    let failed = until_task(&lab.api, &failed_input.id, |task| task.status == TaskStatus::Blocked
        && task.attempt.as_ref().is_some_and(|attempt| attempt.stopped)).await;
    let original_attempt = failed.attempt.as_ref().unwrap();
    assert!(!original_attempt.uncertain);
    assert_eq!(failed.input, failed_input.input);
    assert_eq!(failed.blocked_reason.as_deref(), Some("native_authentication_failed"));
    assert!(failed.result.is_none());
    let source_id = rejection_id(&original_attempt.id, 1);
    let original_charge = charged_once(&lab.api, original_attempt, 1).await;
    let original_rejection = receipt(&lab.api, &source_id).await;
    let Observation::NativeRequestRejected { rejection, endpoint, model, request_binding, diagnostic, .. } =
        report(&original_rejection.1).observation else { panic!("401 did not produce a canonical rejection") };
    assert_eq!(rejection.kind, RejectionKind::AuthenticationFailed);
    assert_eq!(rejection.retry_after, RetryAfter::Absent);
    assert_eq!(endpoint, peer.endpoint);
    assert_eq!(model, "gpt-6-astra");
    assert!(diagnostic.contains("code=invalid_api_key"));
    assert!(!diagnostic.contains("synthetic-native-retry-fixture-key"));
    {
        let seen = peer.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &seen[0].wire_body);
        assert_eq!(request_binding, ProviderRequestBinding { bytes: seen[0].wire_body.len() as u64,
            sha256: digest.as_ref().iter().map(|byte| format!("{byte:02x}")).collect() });
    }
    let expected_alert = NativeRetryBlock { request_index: 1, source_operation_id: source_id.clone(),
        reason: RetryBlockReason::AuthenticationFailed };
    assert_eq!(failed.native_retry_block.as_ref(), Some(&expected_alert));
    let alert_id = format!("{}-native-retry-block-1", original_attempt.id);
    let original_alert = receipt(&lab.api, &alert_id).await;
    assert!(matches!(report(&original_alert.1).observation,
        Observation::NativeRetryBlocked { block } if block == expected_alert));
    let health_id = format!("{}-native-health-1", original_attempt.id);
    let original_health = receipt(&lab.api, &health_id).await;
    let Outcome::Ok { data: Data::NativeRequestSettlement(health) } =
        serde_json::from_value(original_health.1.outcome.clone().unwrap()).unwrap()
        else { panic!("authentication health settlement absent") };
    assert_eq!(health.outcome, NativeRequestOutcome::Rejected { rejection });
    assert_eq!(health.permit.credential_configuration, "OPENAI_API_KEY");
    assert_eq!(health.permit.auth_cooldown_ms, 30_000);
    assert!(!health.permit.auth_probe);
    let retry_at = health.auth_retry_at_ms.unwrap();
    assert!(committed(&original_charge.0) < committed(&original_rejection.0));
    assert!(committed(&original_rejection.0) < committed(&original_health.0));
    assert!(committed(&original_health.0) < committed(&original_alert.0));

    let left_input = lab.submit_named(3, "auth-left", "Auth left fixture. Return JSON with result equal to ok.").await;
    let right_input = lab.submit_named(3, "auth-right", "Auth right fixture. Return JSON with result equal to ok.").await;
    let mut second = lab.worker_as(&peer.endpoint, "auth-worker-b", "auth-incarnation-b");
    let left = until_task(&lab.api, &left_input.id, |task| task.attempt.as_ref()
        .is_some_and(|attempt| attempt.phase == AttemptPhase::Running)).await;
    let right = until_task(&lab.api, &right_input.id, |task| task.attempt.as_ref()
        .is_some_and(|attempt| attempt.phase == AttemptPhase::Running)).await;
    assert_ne!(left.attempt.as_ref().unwrap().worker_id, right.attempt.as_ref().unwrap().worker_id);
    assert_eq!(lab.limits.worker.native_capacity, 1);
    let left_wait = authentication_wait(&lab, &left, NativeWaitReason::AuthenticationCooldown).await;
    let right_wait = authentication_wait(&lab, &right, NativeWaitReason::AuthenticationCooldown).await;
    assert_eq!(left_wait.3, right_wait.3);
    assert_eq!(left_wait.4, Some(retry_at));
    assert_eq!(right_wait.4, Some(retry_at));
    for task in [&left, &right] {
        assert!(retry_at < task.input.limits.deadline_unix_ms);
        assert!(lookup(&lab.api, &contract::reservation_id(&task.attempt.as_ref().unwrap().id, 1)).await.is_none());
    }
    peer.quiet(1).await;
    assert!(process::now_ms() < retry_at, "fixture must restart inside the recorded cooldown");
    controller.stop().await;
    let mut controller = lab.controller("controller-two").await;
    assert!(process::now_ms() < retry_at, "controller restart missed the recorded cooldown");
    first.live(); second.live();
    assert_eq!(get(&lab.api, &failed.id).await, failed);
    for (id, original) in [(&source_id, &original_rejection), (&alert_id, &original_alert), (&health_id, &original_health)] {
        let retained = receipt(&lab.api, id).await;
        assert_eq!(retained.0.commit, original.0.commit);
        assert_eq!(retained.1, original.1);
    }
    for (task, wait) in [(&left, &left_wait), (&right, &right_wait)] {
        let current = get(&lab.api, &task.id).await;
        assert_eq!(current.attempt, task.attempt);
        assert_eq!(current.input, task.input);
        let replay = lab.api.call(wait.0.clone()).await.unwrap();
        assert_eq!(replay.commit, wait.1.commit);
        assert_eq!(replay.outcome, serde_json::from_value::<Outcome>(wait.2.outcome.clone().unwrap()).unwrap());
        assert!(lookup(&lab.api, &contract::reservation_id(&task.attempt.as_ref().unwrap().id, 1)).await.is_none());
    }
    let refused = lab.api.call(Operation::RetryTask { operation_id: "auth-unchanged-retry".into(),
        task_id: failed.id.clone(), expected_revision: failed.revision }).await.unwrap();
    assert!(matches!(refused.outcome, Outcome::Refused { code, .. } if code == "retry_correction_required"));
    peer.quiet(1).await;

    let mut stream = tokio::time::timeout(WAIT, probe).await.expect("no auth probe after recorded cooldown")
        .expect("provider fixture closed before probe");
    let (probe_task, waiting_task) = {
        let seen = peer.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert!(seen[1].received_unix_ms >= retry_at, "POST preceded acknowledged cooldown expiry");
        if seen[1].task_id == left.id { (&left, &right) } else {
            assert_eq!(seen[1].task_id, right.id); (&right, &left)
        }
    };
    let (_, grant_reply, _, NativeRequestAdmission::Granted { permit }) = acquisition(&lab, probe_task).await.unwrap()
        else { panic!("physical probe has no canonical grant") };
    assert!(permit.auth_probe);
    assert_eq!(permit.auth_generation, left_wait.3);
    assert_eq!(permit.circuit.key.endpoint, peer.endpoint);
    assert_eq!(permit.credential_configuration, "OPENAI_API_KEY");
    let probe_charge = charged_once(&lab.api, probe_task.attempt.as_ref().unwrap(), 1).await;
    assert!(committed(&grant_reply) < committed(&probe_charge.0));
    let pending = authentication_wait(&lab, waiting_task, NativeWaitReason::AuthenticationProbePending).await;
    assert_eq!(pending.3, permit.auth_generation);
    assert_eq!(pending.4, None);
    let waiting_attempt = waiting_task.attempt.as_ref().unwrap();
    assert!(lookup(&lab.api, &contract::reservation_id(&waiting_attempt.id, 1)).await.is_none());
    peer.quiet(2).await;

    // Cancel the contender while the admitted probe remains physically open.
    // A new acquire under that exact old caller must refuse after stop; replay
    // of its historical Wait cannot grant or charge another provider request.
    let cancel_until = tokio::time::Instant::now() + WAIT;
    for index in 0u32.. {
        let current = get(&lab.api, &waiting_task.id).await;
        let reply = lab.api.call(Operation::CancelTask { operation_id: format!("auth-cancel-{index}"),
            task_id: current.id, expected_revision: current.revision }).await.unwrap();
        match reply.outcome {
            Outcome::Ok { .. } => break,
            Outcome::Refused { code, .. } if code == "stale_revision" => {},
            other => panic!("auth contender cancellation refused: {other:?}"),
        }
        assert!(tokio::time::Instant::now() < cancel_until);
    }
    let cancelled = until_task(&lab.api, &waiting_task.id, |task| task.status == TaskStatus::Cancelled
        && task.attempt.as_ref().is_some_and(|attempt| attempt.stopped)).await;
    assert!(!cancelled.attempt.as_ref().unwrap().uncertain);
    let mut stale = pending.0.clone();
    let Operation::AcquireNativeRequest { operation_id, .. } = &mut stale else { unreachable!() };
    *operation_id = "auth-stopped-caller-acquire".into();
    let refused = lab.api.call(stale).await.unwrap();
    assert!(matches!(refused.outcome, Outcome::Refused { code, .. } if code == "invalid_transition"));
    let replay = lab.api.call(pending.0).await.unwrap();
    assert_eq!(replay.commit, pending.1.commit);
    assert_eq!(replay.outcome, serde_json::from_value::<Outcome>(pending.2.outcome.unwrap()).unwrap());
    assert!(lookup(&lab.api, &contract::reservation_id(&waiting_attempt.id, 1)).await.is_none());
    peer.quiet(2).await;

    let body = serde_json::to_vec(&json!({"status":"completed", "usage":{"input_tokens":5,"output_tokens":1,"total_tokens":6},
        "output":[{"type":"message","role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":"{\"result\":\"ok\"}"}]}]})).unwrap();
    let header = format!("HTTP/1.1 200 fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
    stream.write_all(header.as_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();
    stream.shutdown().await.unwrap();
    let succeeded = until_task(&lab.api, &probe_task.id, |task| task.status == TaskStatus::Succeeded).await;
    assert_eq!(succeeded.input, probe_task.input);
    assert_eq!(succeeded.attempt.as_ref().unwrap().id, probe_task.attempt.as_ref().unwrap().id);
    assert!(succeeded.attempt.as_ref().is_some_and(|attempt| attempt.stopped && !attempt.uncertain));
    assert_eq!(succeeded.result.as_ref().unwrap().output, Some(json!({"result":"ok"})));
    for task in [&failed, probe_task, waiting_task] {
        assert!(lookup(&lab.api, &contract::reservation_id(&task.attempt.as_ref().unwrap().id, 2)).await.is_none());
    }
    assert_eq!(receipt(&lab.api, &source_id).await.1, original_rejection.1);
    assert_eq!(receipt(&lab.api, &alert_id).await.1, original_alert.1);
    assert_eq!(get(&lab.api, &failed.id).await, failed);
    let cancelled = cancelled_with_acceptance_notice(&lab, &cancelled).await;
    assert_eq!(get(&lab.api, &waiting_task.id).await, cancelled);
    assert_eq!(peer.seen.lock().unwrap()[0].charge_revision, committed(&original_charge.0));
    assert_eq!(peer.seen.lock().unwrap()[1].charge_revision, committed(&probe_charge.0));
    peer.quiet(2).await;
    first.stop().await; second.stop().await; controller.stop().await; peer.close().await;
}

#[path = "native_format.rs"]
mod format_repair;

#[path = "native_output_cap.rs"]
mod output_cap;

#[path = "native_unknown_probe.rs"]
mod unknown_probe;
