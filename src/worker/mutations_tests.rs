// Test-only compatibility helpers keep the existing journal/replay oracles while selecting explicit seed metadata.
fn read(path: &Path) -> Result<Option<Journal>, String> {
    super::read(path, &crate::worker::test_limits())
}
fn create(path: &Path, request: Operation, recovery_epoch: u64) -> Result<Journal, String> {
    super::create(path, request, recovery_epoch, &crate::worker::test_limits())
}
fn record_launch(path: &Path, command: &WorkerCommand) -> Result<bool, String> {
    crate::worker::record_launch(path, command, &crate::worker::test_limits())
}
use super::*;
fn command_receipts(state: &Path) -> Result<Vec<(PathBuf, Receipt)>, String> {
    crate::worker::command_receipts(state, &crate::worker::test_limits())
}
fn read_receipt(path: &Path) -> Result<Receipt, String> {
    crate::worker::read_receipt(path, &crate::worker::test_limits())
}
fn pending_acknowledgments(state: &Path, incarnation: &str) -> Result<Vec<String>, String> {
    crate::worker::pending_acknowledgments(state, incarnation, &crate::worker::test_limits())
}
fn apply_acknowledgments(state: &Path, request: &Operation) -> Result<(), String> {
    crate::worker::apply_acknowledgments(state, request, &crate::worker::test_limits())
}

use crate::controller::{Controller, ControllerConfig};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request as HttpRequest, Response, body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;
use std::sync::Mutex;

type RequiredLookupHold = (
    tokio::sync::oneshot::Sender<Reply>,
    tokio::sync::oneshot::Receiver<()>,
);
// Opt-in report instrumentation leaves the existing mutation fixture unchanged.
// Every returned/withheld outcome still comes from the real Controller.
struct RequiredReportProbe {
    id: String,
    seen: Vec<Operation>,
    original: Option<Reply>,
    lookup_hold: Option<RequiredLookupHold>,
    hold_lookup_after_commit: bool,
    deny_lookup: bool,
    current_failure_phase: Option<&'static str>,
    current_failures: usize,
    lookups: usize,
    contract_fault: Option<(&'static str, &'static str)>,
    contract_faults: usize,
}

struct Fixture {
    state: PathBuf,
    controller_url: String,
    client: ApiClient,
    _controller: Controller,
    _lease: process::WorkerLease,
    drop_id: Arc<Mutex<Option<String>>>,
    deny_report: Arc<Mutex<Option<String>>>,
    sends: Arc<Mutex<Vec<Operation>>>,
    after_commit: Arc<Mutex<Option<Box<dyn FnOnce() + Send>>>>,
    required_report: Arc<Mutex<Option<RequiredReportProbe>>>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Fixture {
    async fn new(label: &str) -> Self {
        Self::with_receipts(label, 1_000_000).await
    }
    async fn with_receipts(label: &str, max_receipts: usize) -> Self {
        Self::with_validation(label, max_receipts, None).await
    }
    async fn with_native_validation(label: &str) -> Self {
        // Native task admission invokes the real helper. The library test
        // harness cannot implement its protocol; bind the same-build runtime
        // explicitly, as in the existing native Controller fixtures.
        let executable = std::env::var_os("BORG_TEST_RUNTIME").map(PathBuf::from).unwrap_or_else(|| {
            // The declared --no-run command builds the ordinary binary beside
            // Cargo's deps directory before this library filter executes.
            std::env::current_exe().unwrap().parent().unwrap().parent().unwrap().join("borg-runtime")
        });
        assert!(executable.is_absolute() && executable.is_file());
        Self::with_validation(label, 1_000_000, Some(executable)).await
    }
    async fn with_validation(label: &str, max_receipts: usize, executable: Option<PathBuf>) -> Self {
        Self::with_policy_validation(label, max_receipts, (*crate::worker::test_limits()).clone(), crate::worker::test_limits(), executable).await
    }
    async fn with_policies(label: &str, max_receipts: usize, controller_limits: RuntimeLimits, startup_limits: Arc<RuntimeLimits>) -> Self {
        Self::with_policy_validation(label, max_receipts, controller_limits, startup_limits, None).await
    }
    async fn with_policy_validation(label: &str, max_receipts: usize, mut controller_limits: RuntimeLimits, startup_limits: Arc<RuntimeLimits>, executable: Option<PathBuf>) -> Self {
        let parent = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("artifacts/v2/build-60156282/mutation-replay-j8fewqvc/worker-tests");
        std::fs::create_dir_all(&parent).unwrap();
        let state = parent.join(fresh_id(label).unwrap());
        std::fs::create_dir(&state).unwrap();
        for name in [
            "commands",
            "acks",
            "outbox",
            "processes",
            "stops",
            "mutations",
            "mutations/settlements",
            "delegations",
            "native-tracking",
        ] {
            std::fs::create_dir_all(state.join(name)).unwrap();
        }
        controller_limits.controller.command_wait_ms = 20;
        // One setup receipt registers the repository. Keep exactly the original
        // available normal-pool slots for the launch/cleanup saturation oracle.
        controller_limits.controller.max_receipts = max_receipts.checked_add(1).unwrap();
        let controller = Controller::open_with_config(
            state.join("tasks.sqlite"),
            ControllerConfig {
                limits: Arc::new(controller_limits),
                validation_executable: executable.unwrap_or_else(|| std::env::current_exe().unwrap()),
                ..ControllerConfig::default()
            },
        )
        .unwrap();
        let registered = controller
            .handle(Request {
                protocol_revision: WIRE_REVISION,
                request_id: "register-fixture-repository".into(),
                command: Operation::RegisterRepository {
                    operation_id: "register-fixture-repository".into(),
                    registration: crate::repository_catalogue::Registration {
                        pull_request_target: None,
                        repository_id: "fixture-repository".into(),
                        registration_revision: "fixture-revision".into(),
                        display_name: "Mutation fixture repository".into(),
                        source_endpoint: "/unused/local.git".into(),
                        checkpoint_endpoint: "/unused/local.git".into(),
                        source_store_key: "local.git".into(),
                        checkpoint_store_key: "local.git".into(),
                    },
                },
            })
            .await;
        assert!(matches!(
            registered.outcome,
            Outcome::Ok {
                data: Data::RepositoryRegistration(_)
            }
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let drop_id = Arc::new(Mutex::new(None::<String>));
        let sends = Arc::new(Mutex::new(Vec::new()));
        let after_commit = Arc::new(Mutex::new(None::<Box<dyn FnOnce() + Send>>));
        let commit_cut = after_commit.clone();
        let deny_report = Arc::new(Mutex::new(None::<String>));
        let denied = deny_report.clone();
        let required_report = Arc::new(Mutex::new(None::<RequiredReportProbe>));
        let report_probe = required_report.clone();
        let (owner, drop_response, recorded) = (controller.clone(), drop_id.clone(), sends.clone());
        let server = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let (owner, drop_response, recorded) =
                    (owner.clone(), drop_response.clone(), recorded.clone());
                let denied = denied.clone();
                let commit_cut = commit_cut.clone();
                let report_probe = report_probe.clone();
                connections.spawn(async move {
                    let service = service_fn(move |request: HttpRequest<Incoming>| {
                        let (owner, drop_response, recorded) =
                            (owner.clone(), drop_response.clone(), recorded.clone());
                        let denied = denied.clone();
                        let commit_cut = commit_cut.clone();
                        let report_probe = report_probe.clone();
                        async move {
                            let request: Request = serde_json::from_slice(
                                &request
                                    .into_body()
                                    .collect()
                                    .await
                                    .map_err(std::io::Error::other)?
                                    .to_bytes(),
                            )
                            .unwrap();
                            if matches!(&request.command, Operation::ObserveWorker { report } if denied.lock().unwrap().as_deref() == Some(&report.report_id)) {
                                return Err(std::io::Error::other("controlled uncertainty transport unavailable"));
                            }
                            let (selected_report, selected_lookup, deny_lookup, deny_current) = {
                                let mut probe = report_probe.lock().unwrap();
                                match probe.as_mut() {
                                    Some(probe) => {
                                        probe.seen.push(request.command.clone());
                                        let selected_lookup = matches!(&request.command, Operation::GetOperation { operation_id } if operation_id == &probe.id);
                                        let deny_current = probe.original.is_some()
                                            && matches!(&request.command, Operation::GetTask { .. })
                                            && match probe.current_failure_phase {
                                                Some("before-lookup") => probe.lookups == 0,
                                                Some("after-lookup") => probe.lookups > 0,
                                                _ => false,
                                            };
                                        if deny_current {
                                            probe.current_failure_phase = None;
                                            probe.current_failures += 1;
                                        }
                                        if selected_lookup { probe.lookups += 1; }
                                        (
                                            matches!(&request.command, Operation::ObserveWorker { report } if report.report_id == probe.id),
                                            selected_lookup,
                                            probe.deny_lookup && selected_lookup,
                                            deny_current,
                                        )
                                    }
                                    None => (false, false, false, false),
                                }
                            };
                            if deny_lookup {
                                return Err(std::io::Error::other("controlled original report lookup unavailable"));
                            }
                            if deny_current {
                                return Err(std::io::Error::other("controlled current-task read unavailable"));
                            }
                            let id = identity(&request.command).ok().map(str::to_owned)
                                .or_else(|| selected_report.then(|| request.command.mutation_id().unwrap().to_owned()));
                            if id.is_some() {
                                recorded.lock().unwrap().push(request.command.clone());
                            }
                            let selected_current = matches!(&request.command, Operation::GetTask { .. });
                            let mut reply = owner.handle(request).await;
                            let lookup_hold = {
                                let mut probe = report_probe.lock().unwrap();
                                if let Some(probe) = probe.as_mut() {
                                    if selected_report && reply.commit.is_some() && probe.original.is_none() {
                                        probe.original = Some(reply.clone());
                                    }
                                    if selected_lookup && (!probe.hold_lookup_after_commit || probe.original.is_some()) {
                                        probe.lookup_hold.take()
                                    } else { None }
                                } else { None }
                            };
                            if let Some((ready, resume)) = lookup_hold {
                                let _ = ready.send(reply.clone());
                                let _ = resume.await;
                            }
                            if id.is_some() && reply.commit.is_some() {
                                let cut = commit_cut.lock().unwrap().take();
                                if let Some(cut) = cut { cut(); }
                            }
                            let drop = {
                                let mut selected = drop_response.lock().unwrap();
                                if selected.is_some()
                                    && *selected == id
                                    && matches!(reply.outcome, Outcome::Ok { .. })
                                {
                                    selected.take();
                                    true
                                } else {
                                    false
                                }
                            };
                            if drop {
                                return Err(std::io::Error::other(
                                    "controlled loss after actual commit",
                                ));
                            }
                            // Corrupt only the transported read, never the real
                            // committed report or the Controller's stored task.
                            let contract_fault = {
                                let mut guard = report_probe.lock().unwrap();
                                guard.as_mut().and_then(|probe| {
                                    let (target, fault) = probe.contract_fault?;
                                    if probe.original.is_some()
                                        && ((target == "task" && selected_current)
                                            || (target == "lookup" && selected_lookup))
                                    {
                                        probe.contract_fault = None;
                                        probe.contract_faults += 1;
                                        Some(fault)
                                    } else { None }
                                })
                            };
                            match contract_fault {
                                Some("request-id") => reply.request_id.push_str("-wrong"),
                                Some("body") => match &mut reply.outcome {
                                    Outcome::Ok { data: Data::Task(task) } => task.id.push_str("-wrong"),
                                    Outcome::Ok { data: Data::Operation(record) } => {
                                        let Operation::ObserveWorker { mut report } =
                                            serde_json::from_value(record.intended.clone()).unwrap()
                                        else { panic!("fixture original report intent") };
                                        report.observation = Observation::Progress {
                                            text: "different body under the original identity".into(),
                                            output: String::new(), truncated: false,
                                        };
                                        record.intended = serde_json::to_value(Operation::ObserveWorker { report }).unwrap();
                                    }
                                    _ => panic!("fixture expected successful typed read"),
                                },
                                Some(fault @ ("outcome-kind" | "outcome-task" | "outcome-attempt-task")) => {
                                    let Outcome::Ok { data: Data::Operation(record) } = &mut reply.outcome
                                        else { panic!("fixture expected original operation") };
                                    let mut outcome: Outcome = serde_json::from_value(record.outcome.clone().unwrap()).unwrap();
                                    if fault == "outcome-kind" {
                                        outcome = Outcome::Ok { data: Data::Acknowledged };
                                    } else {
                                        let Outcome::Ok { data: Data::Task(task) } = &mut outcome
                                            else { panic!("fixture expected original reservation task") };
                                        if fault == "outcome-attempt-task" {
                                            task.attempt.as_mut().unwrap().task_id.push_str("-foreign");
                                        } else {
                                            task.id.push_str("-foreign");
                                        }
                                    }
                                    record.outcome = Some(serde_json::to_value(outcome).unwrap());
                                },
                                None => {},
                                _ => unreachable!(),
                            }
                            Ok::<_, std::io::Error>(Response::new(Full::new(Bytes::from(
                                serde_json::to_vec(&reply).unwrap(),
                            ))))
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        let lease = process::WorkerLease::claim(
            &state.join("worker-owner.json"),
            &startup_limits.worker,
        )
        .unwrap();
        Self {
            state,
            client: ApiClient::new(&url, startup_limits).unwrap(),
            controller_url: url,
            _controller: controller,
            _lease: lease,
            drop_id,
            deny_report,
            sends,
            after_commit,
            required_report,
            server,
        }
    }
    fn fail_after_commit(
        &self,
        path: &Path,
        point: process::storage_tests::Point,
        errno: i32,
        prefix: usize,
    ) -> Arc<Mutex<Option<process::storage_tests::Fault>>> {
        let fault = Arc::new(Mutex::new(None));
        let retained = fault.clone();
        let path = path.to_owned();
        *self.after_commit.lock().unwrap() = Some(Box::new(move || {
            *retained.lock().unwrap() =
                Some(process::storage_tests::inject(&path, point, errno, prefix));
        }));
        fault
    }
    async fn call(&self, command: Operation) -> Outcome {
        self.client.call(command).await.unwrap().outcome
    }
    async fn task(&self, id: &str) -> Task {
        match self.call(Operation::GetTask { task_id: id.into() }).await {
            Outcome::Ok {
                data: Data::Task(task),
            } => *task,
            other => panic!("{other:?}"),
        }
    }
    async fn submit(&self, id: &str) -> Task {
        match self
            .call(Operation::SubmitTask {
                operation_id: id.into(),
                input: input(id),
            })
            .await
        {
            Outcome::Ok {
                data: Data::Task(task),
            } => *task,
            other => panic!("{other:?}"),
        }
    }
    async fn launch(&self) -> WorkerCommand {
        assert!(
            register(&self.client, &self.state, worker("one"))
                .await
                .unwrap()
        );
        let path = self.state.join("mutations/await.json");
        let mut journal = create(&path, wait("launch-wait", "one", vec![]), 0).unwrap();
        match resolve(&self.client, &path, &mut journal).await.unwrap() {
            Some(Outcome::Ok {
                data: Data::Commands(commands),
            }) => commands
                .into_iter()
                .find(|c| matches!(c, WorkerCommand::RunAttempt { .. }))
                .unwrap(),
            other => panic!("{other:?}"),
        }
    }
    fn reporter(&self, attempt: &Attempt) -> Reporter {
        Reporter {
            client: self.client.clone(),
            resource: None,
            config: WorkerConfig {
                limits: crate::worker::test_limits(),
                resource_profile: None,
                controller_url: String::new(),
                id: "worker".into(),
                incarnation: "one".into(),
                workspace_root: self.state.clone(),
                codex_path: "/does-not-exist".into(),
                coding_capacity: 1,
                native_capacity: 0,
                native_endpoint: "http://127.0.0.1:1".into(),
                validation_executable: std::env::current_exe().unwrap(),
            },
            attempt: attempt.clone(),
            outbox: self.state.join("outbox"),
            sequence: Arc::new(AtomicU64::new(1)),
            executor_seen: Arc::new(AtomicBool::new(false)),
            provider_uncertain: Arc::new(AtomicBool::new(false)),
            uncertainty_retained: Arc::new(AtomicBool::new(false)),
            output_limit: 4096,
            result_limit: crate::worker::test_limits().task_defaults.result_bytes,
            coding_limits: crate::worker::test_limits().execution.coding.clone(),
            outbox_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
}
fn worker(incarnation: &str) -> Worker {
    Worker {
        id: "worker".into(),
        incarnation: incarnation.into(),
        coding_capacity: 1,
        native_capacity: 0,
    }
}
fn wait(id: &str, incarnation: &str, acknowledged: Vec<String>) -> Operation {
    Operation::AwaitCommands {
        operation_id: id.into(),
        worker_id: "worker".into(),
        incarnation: incarnation.into(),
        acknowledged,
    }
}
fn input(agent: &str) -> TaskInput {
    TaskInput { instruction_source: None,
        pull_request: None,
        resolved_limits: None,
        repository_read: None,
        instruction: "controlled local launch boundary".into(),
        definition: Definition {
            charter: None,
            limit_overrides: None,
            execution_profile: Some(crate::protocol::ExecutionProfile::CollaborationV1 {
                web_search: None,
            }),
            inference: Some(crate::protocol::InferenceSettings::selected()),
            id: agent.into(),
            revision: "one".into(),
            executor: ExecutorKind::Coding,
        },
        repository: Some(Repository {
            pull_request_target: None,
            registration: Some(crate::repository_catalogue::Revision {
                repository_id: "fixture-repository".into(),
                registration_revision: "fixture-revision".into(),
            }),
            url: String::new(),
            checkpoint_remote: String::new(),
            start_commit: "a".repeat(40),
        }),
        acceptance_commands: vec![vec!["/usr/bin/true".into()]],
        output_schema: None,
        limits: Limits {
            deadline_unix_ms: process::now_ms() + 60_000,
            model_requests: 2,
            output_bytes: 4096,
            result_bytes: crate::worker::test_limits().task_defaults.result_bytes,
            max_delegation_depth: 2,
        },
        dependencies: vec![],
        native_workflow: None,
    }
}
fn control() -> (Control, watch::Sender<bool>) {
    let (tx, rx) = watch::channel(false);
    (
        Control {
            deadline_ms: process::now_ms() + 60_000,
            cancel: rx,
            runtime_limits: crate::worker::test_limits(),
            worker_limits: Arc::new(crate::worker::test_limits().worker.clone()),
            validation_runtime: Arc::new(
                crate::native::ValidationRuntime::new(Arc::new(
                    crate::worker::test_limits().worker.clone(),
                ))
                .unwrap(),
            ),
        },
        tx,
    )
}

async fn required_report_native(
    f: &Fixture,
    lifetime_ms: u64,
) -> (Reporter, TaskInput, Control, watch::Sender<bool>) {
    native_report_fixture(f, lifetime_ms, true).await
}

async fn native_report_fixture(
    f: &Fixture,
    lifetime_ms: u64,
    completed_request: bool,
) -> (Reporter, TaskInput, Control, watch::Sender<bool>) {
    let mut task = input("required-report-agent");
    task.definition.executor = ExecutorKind::Native;
    task.repository = None;
    task.acceptance_commands.clear();
    task.output_schema = Some(json!({"type":"integer"}));
    task.limits.deadline_unix_ms = process::now_ms() + lifetime_ms;
    let submitted = f.call(Operation::SubmitTask {
        operation_id: "required-report-submit".into(), input: task,
    }).await;
    assert!(matches!(&submitted, Outcome::Ok { data: Data::Task(_) }),
        "native fixture admission: {submitted:?}");
    assert!(matches!(f.call(Operation::RegisterWorker {
        operation_id: "required-report-register".into(),
        worker: Worker { id: "worker".into(), incarnation: "one".into(), coding_capacity: 0, native_capacity: 1 },
    }).await, Outcome::Ok { .. }));
    let Outcome::Ok { data: Data::Commands(commands) } = f.call(wait("required-report-await", "one", vec![])).await else {
        panic!("native fixture must receive its real admitted command")
    };
    let (attempt, task) = commands.into_iter().find_map(|command| match command {
        WorkerCommand::RunAttempt { attempt, task, .. } => Some((attempt, task)),
        _ => None,
    }).expect("native fixture RunAttempt");
    let mut reporter = f.reporter(&attempt);
    reporter.config.coding_capacity = 0;
    reporter.config.native_capacity = 1;
    reporter.output_limit = task.limits.output_bytes;
    assert!(reporter.send_identified(format!("{}-initial-start", attempt.id),
        Observation::Started { thread_id: None, turn_id: None }).await.unwrap());
    if completed_request {
        assert!(reporter.send_identified(crate::native_loop_contract::reservation_id(&attempt.id, 1),
            Observation::Usage { native_diagnostic_reserved: true, model_requests: 1, input_tokens: None, output_tokens: None }).await.unwrap());
        // Controlled trusted-worker observation through the real Controller,
        // not provider execution or a fabricated database receipt. The existing
        // required-report cases start after this terminal request observation.
        assert!(reporter.send_identified(crate::native_loop_contract::diagnostic_id(&attempt.id, 1),
            Observation::NativeRequestDiagnostic {
                request_index: 1,
                endpoint: "http://127.0.0.1:1/v1/responses".into(),
                model: task.definition.required_inference().unwrap().model.clone(),
                request_binding: Some(crate::native::ProviderRequestBinding {
                    bytes: 2,
                    // SHA-256 of the exact controlled two-byte body b"{}".
                    sha256: "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a".into(),
                }),
                observed_outcome: crate::native_loop_contract::NativeRequestObservedOutcome::Completed,
                failure: None,
            }).await.unwrap());
    }
    let (mut control, cancel) = control();
    control.deadline_ms = task.limits.deadline_unix_ms;
    assert_eq!(control.deadline_ms, attempt.deadline_unix_ms);
    (reporter, task, control, cancel)
}

fn required_progress() -> Observation {
    Observation::Progress {
        text: "original native observations".into(),
        output: "p".repeat(8192), // Reporter must normalize this before lookup.
        truncated: false,
    }
}

fn required_completion(model_requests: u32) -> Observation {
    Observation::NativeLoopCompleted {
        model_requests, calls: vec![], requests: vec![], final_response_index: None,
    }
}

fn probe_required(f: &Fixture, id: &str, lookup_hold: Option<RequiredLookupHold>) {
    *f.required_report.lock().unwrap() = Some(RequiredReportProbe {
        id: id.into(), seen: vec![], original: None, lookup_hold, hold_lookup_after_commit: false, deny_lookup: false,
        current_failure_phase: None, current_failures: 0, lookups: 0,
        contract_fault: None, contract_faults: 0,
    });
}

fn required_original(f: &Fixture) -> (Report, Reply, Vec<Operation>) {
    let guard = f.required_report.lock().unwrap();
    let probe = guard.as_ref().unwrap();
    let reports = probe.seen.iter().filter_map(|operation| match operation {
        Operation::ObserveWorker { report } => Some(report.clone()),
        _ => None,
    }).collect::<Vec<_>>();
    assert_eq!(reports.len(), 1, "original receipt recovery must not redispatch the report or issue Usage/result/tool work");
    assert_eq!(reports[0].report_id, probe.id);
    (reports[0].clone(), probe.original.clone().expect("actual original committed reply"), probe.seen.clone())
}

async fn assert_required_lookup(f: &Fixture, report: &Report, original: &Reply) {
    let intended = Operation::ObserveWorker { report: report.clone() };
    let lookup = f.client.call(Operation::GetOperation { operation_id: report.report_id.clone() }).await.unwrap();
    assert_eq!(lookup.commit, original.commit, "later observation retains the original commit");
    let Outcome::Ok { data: Data::Operation(record) } = lookup.outcome else { panic!("original report lookup") };
    assert_eq!(record.intended, serde_json::to_value(&intended).unwrap());
    assert_eq!(record.outcome, Some(serde_json::to_value(&original.outcome).unwrap()));
}

#[tokio::test]
async fn required_native_reports_recover_lost_ack_by_exact_original_lookup() {
    for completion in [false, true] {
      for loss_phase in ["before-lookup", "after-lookup"] {
        let f = Fixture::with_native_validation(&format!("required-report-lost-{completion}-{loss_phase}")).await;
        let (reporter, input, control, _cancel) = required_report_native(&f, 60_000).await;
        let id = if completion { crate::native_loop_contract::completion_id(&reporter.attempt.id) }
            else { format!("{}-native-provider-output", reporter.attempt.id) };
        let observation = if completion { required_completion(1) } else { required_progress() };
        probe_required(&f, &id, None);
        f.required_report.lock().unwrap().as_mut().unwrap().current_failure_phase = Some(loss_phase);
        *f.drop_id.lock().unwrap() = Some(id.clone());
        assert!(tokio::time::timeout(Duration::from_secs(5), native_loop::required_report(
            &reporter, &input, &control, id.clone(), observation,
        )).await.expect("lost ACK must reconcile within the original deadline").unwrap());
        let (report, original, seen) = required_original(&f);
        assert_eq!(f.required_report.lock().unwrap().as_ref().unwrap().current_failures, 1,
            "the selected currentness read must actually fail at {loss_phase}");
        assert!(matches!(&original.outcome, Outcome::Ok { data: Data::Task(_) }));
        assert!(seen.iter().any(|op| matches!(op, Operation::GetOperation { operation_id } if operation_id == &id)));
        assert!(seen.iter().all(|op| matches!(op,
            Operation::GetTask { .. } | Operation::GetOperation { .. } | Operation::ObserveWorker { .. })),
            "no provider/tool/charge/new task operation belongs to report reconciliation");
        let path = reporter.outbox.join(format!("{}.json", component(&id)));
        assert_eq!(read_report(&path, &reporter.config.limits).unwrap(), report,
            "lookup leaves the exact pending outbox body available for ordinary drain");
        assert!(!path.with_extension("refused").exists());
        if !completion {
            assert!(matches!(&report.observation, Observation::Progress { output, truncated, .. }
                if output.len() == input.limits.output_bytes && *truncated),
                "lookup must use the normalized durable report, not the caller's oversized text");
        }
        assert_required_lookup(&f, &report, &original).await;
        let current = f.task(&reporter.attempt.task_id).await;
        assert_eq!(current.status, TaskStatus::Active);
        assert!(current.result.is_none());
        assert_eq!(current.input.limits.deadline_unix_ms, input.limits.deadline_unix_ms);
        assert!(!reporter.executor_seen.load(Ordering::Relaxed));
        assert!(!reporter.provider_uncertain.load(Ordering::Relaxed));
      }
    }
}

#[tokio::test]
async fn required_native_report_wait_preserves_cancel_fence_and_original_deadline() {
    for mode in ["cancel", "fence", "local-cancel", "deadline"] {
        let f = Fixture::with_native_validation(&format!("required-report-{mode}")).await;
        let (reporter, input, control, cancel) = required_report_native(&f,
            if mode == "deadline" { 5000 } else { 60_000 }).await;
        let id = format!("{}-native-provider-output", reporter.attempt.id);
        let (ready, held) = tokio::sync::oneshot::channel();
        let (release, resume) = tokio::sync::oneshot::channel();
        probe_required(&f, &id, Some((ready, resume)));
        *f.drop_id.lock().unwrap() = Some(id.clone());
        let (r, i, c, report_id) = (reporter.clone(), input.clone(), control.clone(), id.clone());
        let waiting = tokio::spawn(async move {
            native_loop::required_report(&r, &i, &c, report_id, required_progress()).await
        });
        let held_reply = tokio::time::timeout(Duration::from_secs(5), held).await
            .expect("helper must reach the held original receipt").unwrap();
        let (report, original, _) = required_original(&f);
        assert!(matches!(&original.outcome, Outcome::Ok { .. }));
        assert_eq!(held_reply.commit, original.commit);
        let path = reporter.outbox.join(format!("{}.json", component(&id)));
        let pending = std::fs::read(&path).unwrap();
        assert_eq!(read_report(&path, &reporter.config.limits).unwrap(), report);
        // A shorter per-call timeout must not let the next lookup recover a
        // reply while this case is proving the outer cancellation/deadline.
        if matches!(mode, "local-cancel" | "deadline") {
            f.required_report.lock().unwrap().as_mut().unwrap().deny_lookup = true;
        }
        match mode {
            "cancel" => {
                let current = f.task(&reporter.attempt.task_id).await;
                assert!(matches!(f.call(Operation::CancelTask {
                    operation_id: "cancel-required-report".into(), task_id: current.id,
                    expected_revision: current.revision,
                }).await, Outcome::Ok { .. }));
            }
            "fence" => {
                // This fixture launched no executor. Record that fact through
                // the real Controller before replacing the incarnation; live
                // attempts must never be bypassed by a new registration.
                let mut stopped = report.clone();
                stopped.report_id = "required-report-fixture-stopped".into();
                stopped.observation = Observation::ProcessTreeStopped;
                let reply = f._controller.handle(Request {
                    protocol_revision: WIRE_REVISION,
                    request_id: "required-report-fixture-stopped".into(),
                    command: Operation::ObserveWorker { report: stopped },
                }).await;
                assert!(matches!(reply.outcome, Outcome::Ok { .. }));
                assert!(f.task(&reporter.attempt.task_id).await.attempt.unwrap().stopped);
                assert!(matches!(f.call(Operation::RegisterWorker {
                    operation_id: "fence-required-report".into(),
                    worker: Worker { id: "worker".into(), incarnation: "two".into(), coding_capacity: 0, native_capacity: 1 },
                }).await, Outcome::Ok { .. }));
            },
            "local-cancel" => cancel.send(true).unwrap(),
            "deadline" => {},
            _ => unreachable!(),
        }
        let mut release = Some(release);
        if matches!(mode, "cancel" | "fence") { release.take().unwrap().send(()).unwrap(); }
        let error = tokio::time::timeout(Duration::from_secs(7), waiting).await
            .expect("held lookup must obey the original control bound").unwrap().unwrap_err();
        if matches!(mode, "cancel" | "fence") {
            assert!(error.contains("owner/input changed"), "{mode}: {error}");
        } else {
            assert!(error.contains("cancelled or expired"), "{mode}: {error}");
        }
        if mode == "deadline" { assert!(process::now_ms() >= input.limits.deadline_unix_ms); }
        if let Some(release) = release { let _ = release.send(()); }
        f.required_report.lock().unwrap().as_mut().unwrap().deny_lookup = false;
        assert_eq!(std::fs::read(&path).unwrap(), pending);
        assert!(!path.with_extension("refused").exists(), "unavailable/cancelled is not a committed refusal");
        required_original(&f); // Exactly one report mutation, even after fencing.
        assert_required_lookup(&f, &report, &original).await;
        assert_eq!(control.deadline_ms, input.limits.deadline_unix_ms);
        assert!(!reporter.executor_seen.load(Ordering::Relaxed));
    }
}

#[tokio::test]
async fn required_native_report_committed_refusal_remains_false_and_exact() {
    let f = Fixture::with_native_validation("required-report-refused").await;
    let (reporter, input, control, _cancel) = required_report_native(&f, 60_000).await;
    let id = crate::native_loop_contract::completion_id(&reporter.attempt.id);
    // Only request1 was charged. Request2 cannot become a successful completion.
    let observation = required_completion(2);
    let before = f.task(&reporter.attempt.task_id).await;
    probe_required(&f, &id, None);
    assert!(!tokio::time::timeout(Duration::from_secs(5), native_loop::required_report(
        &reporter, &input, &control, id.clone(), observation.clone(),
    )).await.unwrap().unwrap());
    let (report, original, _) = required_original(&f);
    assert!(matches!(&original.outcome, Outcome::Refused { code, .. } if code == "usage_unavailable"));
    assert!(original.commit.is_some());
    let path = reporter.outbox.join(format!("{}.json", component(&id)));
    assert!(!path.exists());
    let refused = std::fs::read(path.with_extension("refused")).unwrap();
    let outcome_bytes = std::fs::read(path.with_extension("outcome")).unwrap();
    assert_eq!(serde_json::from_slice::<Report>(&refused).unwrap(), report);
    let retained: serde_json::Value = serde_json::from_slice(&outcome_bytes).unwrap();
    assert_eq!(retained["outcome"], serde_json::to_value(&original.outcome).unwrap());
    assert_eq!(retained["context"]["commit"], serde_json::to_value(&original.commit).unwrap());
    assert_required_lookup(&f, &report, &original).await;
    assert!(!tokio::time::timeout(Duration::from_secs(5), native_loop::required_report(
        &reporter, &input, &control, id, observation,
    )).await.unwrap().unwrap());
    required_original(&f);
    assert_eq!(std::fs::read(path.with_extension("refused")).unwrap(), refused);
    assert_eq!(std::fs::read(path.with_extension("outcome")).unwrap(), outcome_bytes);
    assert_eq!(f.task(&reporter.attempt.task_id).await, before);
    assert!(!reporter.executor_seen.load(Ordering::Relaxed));
}

#[tokio::test]
async fn required_native_reports_reject_complete_mismatched_reads_without_resend() {
    for target in ["task", "lookup"] {
        for fault in ["request-id", "body"] {
            let f = Fixture::with_native_validation(&format!("required-report-contract-{target}-{fault}")).await;
            let (reporter, input, control, _cancel) = required_report_native(&f, 60_000).await;
            let id = format!("{}-native-provider-output", reporter.attempt.id);
            probe_required(&f, &id, None);
            f.required_report.lock().unwrap().as_mut().unwrap().contract_fault = Some((target, fault));
            *f.drop_id.lock().unwrap() = Some(id.clone());
            let error = tokio::time::timeout(Duration::from_secs(5), native_loop::required_report(
                &reporter, &input, &control, id.clone(), required_progress(),
            )).await.expect("a complete contract violation must refuse promptly, not wait until the deadline")
                .unwrap_err();
            let expected = match (target, fault) {
                (_, "request-id") => "response request identity mismatch",
                ("task", "body") => "native collaboration owner/input changed",
                ("lookup", "body") => "recorded report operation differs",
                _ => unreachable!(),
            };
            assert!(error.contains(expected), "{target}/{fault}: {error}");
            assert_eq!(f.required_report.lock().unwrap().as_ref().unwrap().contract_faults, 1);
            let (report, original, _) = required_original(&f);
            assert!(matches!(&original.outcome, Outcome::Ok { .. }));
            let path = reporter.outbox.join(format!("{}.json", component(&id)));
            let retained = std::fs::read(&path).unwrap();
            assert_eq!(read_report(&path, &reporter.config.limits).unwrap(), report);
            assert!(!path.with_extension("refused").exists());
            assert!(!path.with_extension("outcome").exists());
            assert_required_lookup(&f, &report, &original).await;
            assert_eq!(std::fs::read(&path).unwrap(), retained);
            let current = f.task(&reporter.attempt.task_id).await;
            assert_eq!(current.status, TaskStatus::Active);
            assert!(current.result.is_none());
            assert!(!reporter.executor_seen.load(Ordering::Relaxed));
        }
    }
}

// These join tests use actual Controller receipts. The unused local listener
// proves the reservation/control paths never connect to a provider; no provider
// response, helper result or controller receipt is fabricated by this fixture.
fn diagnostic_route(input: &TaskInput) -> (std::net::TcpListener, crate::native::NativeConfig) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let config = crate::native::NativeConfig {
        endpoint: format!("http://{}/v1/responses", listener.local_addr().unwrap()),
        api_key_env: "OPENAI_API_KEY".into(),
        model: input.definition.required_inference().unwrap().model.clone(),
    };
    (listener, config)
}

fn assert_no_diagnostic_provider_connection(listener: &std::net::TcpListener) {
    assert!(matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
        "reservation reconciliation and retained facts must not dispatch a provider request");
}

fn diagnostic_path(reporter: &Reporter, index: u32) -> PathBuf {
    reporter.outbox.join(format!("{}.json", component(
        &crate::native_loop_contract::diagnostic_id(&reporter.attempt.id, index))))
}

fn assert_unstarted_diagnostic(reporter: &Reporter, index: u32) -> Report {
    let report = read_report(&diagnostic_path(reporter, index), &reporter.config.limits).unwrap();
    assert_eq!(report.task_id, reporter.attempt.task_id);
    assert_eq!(report.attempt_id, reporter.attempt.id);
    assert_eq!(report.generation, reporter.attempt.generation);
    assert!(matches!(&report.observation, Observation::NativeRequestDiagnostic {
        request_index, request_binding: None,
        observed_outcome: crate::native_loop_contract::NativeRequestObservedOutcome::NotStarted,
        failure: Some(failure), ..
    } if *request_index == index && failure.class == crate::native::ProviderFailureClass::LocalAdmission
        && failure.http.is_none()));
    report
}

async fn original_report_reply(f: &Fixture, expected: &Report) -> Reply {
    let reply = f.client.call(Operation::GetOperation { operation_id: expected.report_id.clone() }).await.unwrap();
    let Outcome::Ok { data: Data::Operation(record) } = &reply.outcome else {
        panic!("original report receipt missing: {:?}", reply.outcome)
    };
    assert_eq!(record.intended, serde_json::to_value(Operation::ObserveWorker { report: expected.clone() }).unwrap());
    assert_eq!(reply.commit.as_ref().unwrap().operation_id, expected.report_id);
    reply
}

#[tokio::test]
async fn native_diagnostic_reservation_loss_retains_unknown_commit_without_dispatch() {
    for recover in [false, true] {
        let f = Fixture::with_native_validation(&format!("diagnostic-reservation-loss-{recover}")).await;
        let (reporter, input, control, cancel) = native_report_fixture(&f, 60_000, false).await;
        let (provider, config) = diagnostic_route(&input);
        let mut request = NativeDiagnosticRequest::preflight(&reporter, &input, &config, 1).unwrap();
        assert!(!request.committed);
        assert!(request.unstarted(&reporter).unwrap().is_none(), "preflight alone creates no reservation/fact");
        let id = crate::native_loop_contract::reservation_id(&reporter.attempt.id, 1);
        let (ready, held) = tokio::sync::oneshot::channel();
        let (release, resume) = tokio::sync::oneshot::channel();
        probe_required(&f, &id, Some((ready, resume)));
        f.required_report.lock().unwrap().as_mut().unwrap().hold_lookup_after_commit = true;
        *f.drop_id.lock().unwrap() = Some(id.clone());
        let path = reporter.outbox.join(format!("{}.json", component(&id)));
        let (result, retained) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(request.reserve(&reporter, &control, None, None), async {
                let reply = held.await.expect("original lookup after the lost reservation ACK");
                assert!(matches!(reply.outcome, Outcome::Ok { data: Data::Operation(_) }));
                let retained = std::fs::read(&path).unwrap();
                if !recover {
                    f.required_report.lock().unwrap().as_mut().unwrap().deny_lookup = true;
                    cancel.send(true).unwrap();
                }
                let _ = release.send(());
                retained
            })
        }).await.expect("original reservation must reconcile or stop within its control bound");
        assert_eq!(result.is_ok(), recover);
        assert_eq!(request.committed, recover,
            "a lost positive ACK is not locally committed until exact original receipt resolution");
        assert!(request.retained_reservation);
        assert!(request.unstarted(&reporter).unwrap().is_some(),
            "a retained but unacknowledged reservation still requires its local no-POST fact");
        assert_unstarted_diagnostic(&reporter, 1);
        assert_eq!(std::fs::read(&path).unwrap(), retained);
        let (report, original, _) = required_original(&f);
        assert!(matches!(&report.observation, Observation::Usage {
            native_diagnostic_reserved: true, model_requests: 1, input_tokens: None, output_tokens: None,
        }));
        assert!(matches!(original.outcome, Outcome::Ok { data: Data::Task(_) }));
        f.required_report.lock().unwrap().as_mut().unwrap().deny_lookup = false;
        assert_required_lookup(&f, &report, &original).await;
        let diagnostic = assert_unstarted_diagnostic(&reporter, 1);
        // Cold recovery uses only retained outbox bodies and original receipts,
        // even when the old request's Control is already cancelled. It cannot
        // infer permission to POST from that successful historical append.
        probe_required(&f, &diagnostic.report_id, None);
        tokio::time::timeout(Duration::from_secs(5), flush_outbox_with_credentials(&f.client, &reporter.outbox, &[]))
            .await.unwrap().unwrap();
        let (diagnostic_reply, sends) = {
            let guard = f.required_report.lock().unwrap();
            let probe = guard.as_ref().unwrap();
            (probe.original.clone().expect("actual cold-recovery diagnostic receipt"),
             probe.seen.iter().filter_map(|op| match op {
                 Operation::ObserveWorker { report } => Some(report.clone()), _ => None,
             }).collect::<Vec<_>>())
        };
        assert_eq!(sends, vec![report.clone(), diagnostic.clone()],
            "cold recovery replays only the original Usage and appends exactly one original diagnostic");
        assert!(matches!(diagnostic_reply.outcome, Outcome::Ok { data: Data::Acknowledged }));
        assert_required_lookup(&f, &diagnostic, &diagnostic_reply).await;
        assert_required_lookup(&f, &report, &original).await;
        assert!(!diagnostic_path(&reporter, 1).exists());
        assert!(!path.exists());
        assert_eq!(request.committed, recover, "cold append does not rewrite the old local dispatch decision");
        assert_no_diagnostic_provider_connection(&provider);
        assert_eq!(control.deadline_ms, input.limits.deadline_unix_ms);
    }
}

#[tokio::test]
async fn native_diagnostic_positive_reservation_retains_fact_after_control_loss() {
    for mode in ["cancel", "currentness"] {
        let f = Fixture::with_native_validation(&format!("diagnostic-pre-dispatch-{mode}")).await;
        let (reporter, input, control, cancel) = native_report_fixture(&f, 60_000, false).await;
        let (provider, config) = diagnostic_route(&input);
        let mut request = NativeDiagnosticRequest::preflight(&reporter, &input, &config, 1).unwrap();
        let id = crate::native_loop_contract::reservation_id(&reporter.attempt.id, 1);
        probe_required(&f, &id, None);
        tokio::time::timeout(Duration::from_secs(5), request.reserve(&reporter, &control, None, None))
            .await.unwrap().unwrap();
        assert!(request.committed);
        let (original, reply, _) = required_original(&f);
        assert!(matches!(reply.outcome, Outcome::Ok { data: Data::Task(_) }));
        if mode == "cancel" {
            cancel.send(true).unwrap();
        } else {
            let task = f.task(&reporter.attempt.task_id).await;
            assert!(matches!(f.call(Operation::CancelTask {
                operation_id: "cancel-before-diagnostic-post".into(), task_id: task.id,
                expected_revision: task.revision,
            }).await, Outcome::Ok { .. }));
        }
        assert!(native_retry::current(&reporter, &input, &control).await.is_err());
        let retained = request.unstarted(&reporter).unwrap();
        assert!(retained.is_some());
        let diagnostic = assert_unstarted_diagnostic(&reporter, 1);
        let path = diagnostic_path(&reporter, 1);
        let bytes = std::fs::read(&path).unwrap();
        cancel.send(true).unwrap();
        assert!(acknowledge_native_diagnostic(&reporter, &control, Ok(retained)).await.is_err());
        assert_eq!(std::fs::read(path).unwrap(), bytes,
            "delivery cancellation must not undo synchronous terminal retention");
        assert_eq!(diagnostic.report_id, crate::native_loop_contract::diagnostic_id(&reporter.attempt.id, 1));
        assert_required_lookup(&f, &original, &reply).await;
        assert_no_diagnostic_provider_connection(&provider);
    }
}

#[tokio::test]
async fn native_diagnostic_reservation_rejects_wrong_success_subject_without_dispatch() {
    for fault in ["outcome-kind", "outcome-task", "outcome-attempt-task"] {
        let f = Fixture::with_native_validation(&format!("diagnostic-reservation-{fault}")).await;
        let (reporter, input, control, _cancel) = native_report_fixture(&f, 60_000, false).await;
        let (provider, config) = diagnostic_route(&input);
        let mut request = NativeDiagnosticRequest::preflight(&reporter, &input, &config, 1).unwrap();
        let id = crate::native_loop_contract::reservation_id(&reporter.attempt.id, 1);
        probe_required(&f, &id, None);
        f.required_report.lock().unwrap().as_mut().unwrap().contract_fault = Some(("lookup", fault));
        let error = tokio::time::timeout(Duration::from_secs(5), request.reserve(&reporter, &control, None, None))
            .await.unwrap().unwrap_err();
        assert!(error.contains("reservation successful outcome differs"), "{fault}: {error}");
        assert!(!request.committed);
        assert!(request.retained_reservation);
        assert_eq!(f.required_report.lock().unwrap().as_ref().unwrap().contract_faults, 1);
        assert!(request.unstarted(&reporter).unwrap().is_some());
        assert_unstarted_diagnostic(&reporter, 1);
        let (original, reply, _) = required_original(&f);
        assert!(matches!(reply.outcome, Outcome::Ok { data: Data::Task(_) }));
        assert_required_lookup(&f, &original, &reply).await;
        assert_no_diagnostic_provider_connection(&provider);
    }
}

fn stage_unstarted_diagnostic(reporter: &Reporter, config: &crate::native::NativeConfig, index: u32) -> Report {
    // A typed pending outbox fixture exercises restart ordering; it is not an
    // assertion that the missing/refused reservation actually committed.
    reporter.retain_identified_with_credentials(crate::native_loop_contract::diagnostic_id(&reporter.attempt.id, index),
        Observation::NativeRequestDiagnostic {
            request_index: index, endpoint: config.endpoint.clone(), model: config.model.clone(),
            request_binding: None,
            observed_outcome: crate::native_loop_contract::NativeRequestObservedOutcome::NotStarted,
            failure: Some(crate::native::ProviderFailureDiagnostic::new(crate::native::ProviderFailureClass::LocalAdmission)),
        }, &[]).unwrap()
}

#[tokio::test]
async fn native_diagnostic_outbox_requires_original_usage_and_prioritizes_only_its_pair() {
    for mode in ["pending-pair", "missing", "refused"] {
        let f = Fixture::with_native_validation(&format!("diagnostic-outbox-{mode}")).await;
        let (reporter, input, _control, _cancel) = native_report_fixture(&f, 60_000, false).await;
        let (provider, config) = diagnostic_route(&input);
        let index = if mode == "refused" { 2 } else { 1 };
        let reservation_id = crate::native_loop_contract::reservation_id(&reporter.attempt.id, index);
        let usage = Observation::Usage { native_diagnostic_reserved: true, model_requests: index,
            input_tokens: None, output_tokens: None };
        let reservation = reporter.retain_identified_with_credentials(reservation_id.clone(), usage, &[]).unwrap();
        if mode == "missing" {
            // Model a retained diagnostic whose reservation file is absent;
            // absence never proves that the original operation committed.
            std::fs::remove_file(reporter.outbox.join(format!("{}.json", component(&reservation_id)))).unwrap();
        } else if mode == "refused" {
            // Index2 without index1 is an actual committed Controller refusal.
            assert!(!reporter.send_identified(reservation_id.clone(), reservation.observation.clone()).await.unwrap());
            let reply = original_report_reply(&f, &reservation).await;
            let Outcome::Ok { data: Data::Operation(record) } = reply.outcome else { unreachable!() };
            let outcome: Outcome = serde_json::from_value(record.outcome.unwrap()).unwrap();
            assert!(matches!(outcome, Outcome::Refused { .. }));
        }
        let diagnostic = stage_unstarted_diagnostic(&reporter, &config, index);
        let before = std::fs::read(diagnostic_path(&reporter, index)).unwrap();
        let ordinary = if mode == "pending-pair" {
            Some(reporter.retain_identified_with_credentials("000-unrelated-usage".into(),
                Observation::Usage { native_diagnostic_reserved: false, model_requests: 1,
                    input_tokens: None, output_tokens: None }, &[]).unwrap())
        } else { None };
        probe_required(&f, &diagnostic.report_id, None);
        tokio::time::timeout(Duration::from_secs(5), flush_outbox_with_credentials(&f.client, &reporter.outbox, &[]))
            .await.unwrap().unwrap();
        let seen = f.required_report.lock().unwrap().as_ref().unwrap().seen.clone();
        let sends = seen.iter().filter_map(|op| match op {
            Operation::ObserveWorker { report } => Some(report.clone()), _ => None,
        }).collect::<Vec<_>>();
        if let Some(ordinary) = ordinary {
            assert_eq!(sends, vec![reservation.clone(), diagnostic.clone(), ordinary.clone()],
                "only the causal Usage moves ahead of the diagnostic; ordinary Usage keeps its priority");
            let reserve_reply = original_report_reply(&f, &reservation).await;
            let diagnostic_reply = original_report_reply(&f, &diagnostic).await;
            let ordinary_reply = original_report_reply(&f, &ordinary).await;
            assert!(reserve_reply.commit.unwrap().controller_revision < diagnostic_reply.commit.unwrap().controller_revision);
            assert!(matches!(ordinary_reply.outcome, Outcome::Ok { .. }));
            assert!(!diagnostic_path(&reporter, index).exists());
            assert!(!reporter.outbox.join(format!("{}.json", component(&ordinary.report_id))).exists());
        } else {
            assert!(sends.is_empty(), "missing/refused original Usage must withhold diagnostic dispatch");
            assert!(seen.iter().any(|op| matches!(op, Operation::GetOperation { operation_id } if operation_id == &reservation_id)));
            assert_eq!(std::fs::read(diagnostic_path(&reporter, index)).unwrap(), before);
            assert!(matches!(f.call(Operation::GetOperation { operation_id: diagnostic.report_id.clone() }).await,
                Outcome::Refused { code, .. } if code == "not_found"));
        }
        assert_no_diagnostic_provider_connection(&provider);
    }
}

#[tokio::test]
async fn native_diagnostic_ack_loss_does_not_suppress_independent_usage_publication() {
    let f = Fixture::with_native_validation("diagnostic-independent-usage").await;
    let (reporter, input, control, cancel) = native_report_fixture(&f, 60_000, false).await;
    let (provider, config) = diagnostic_route(&input);
    let mut request = NativeDiagnosticRequest::preflight(&reporter, &input, &config, 1).unwrap();
    request.reserve(&reporter, &control, None, None).await.unwrap();
    assert!(request.committed);
    let retained = request.unstarted(&reporter).unwrap();
    let diagnostic = assert_unstarted_diagnostic(&reporter, 1);
    let bytes = std::fs::read(diagnostic_path(&reporter, 1)).unwrap();
    probe_required(&f, &diagnostic.report_id, None);
    *f.drop_id.lock().unwrap() = Some(diagnostic.report_id.clone());
    // Cancel delivery after the real Controller has committed the diagnostic,
    // then lose its ACK. This cannot cancel or rewrite independent usage facts.
    let cancel_after_commit = cancel.clone();
    *f.after_commit.lock().unwrap() = Some(Box::new(move || { cancel_after_commit.send(true).unwrap(); }));
    let usage_id = format!("{}-independent-usage", reporter.attempt.id);
    let usage = Observation::Usage { native_diagnostic_reserved: false, model_requests: 1,
        input_tokens: None, output_tokens: None };
    assert!(tokio::time::timeout(Duration::from_secs(5), reporter.send_identified(usage_id.clone(), usage.clone()))
        .await.unwrap().unwrap(), "diagnostic ACK uncertainty must not withhold independently reportable Usage");
    assert!(acknowledge_native_diagnostic(&reporter, &control, Ok(retained)).await.is_err());
    assert_eq!(std::fs::read(diagnostic_path(&reporter, 1)).unwrap(), bytes);
    let expected_usage = Report { report_id: usage_id.clone(), observation: usage,
        worker_id: reporter.config.id.clone(), incarnation: reporter.config.incarnation.clone(),
        task_id: reporter.attempt.task_id.clone(), attempt_id: reporter.attempt.id.clone(), generation: reporter.attempt.generation };
    let original = original_report_reply(&f, &expected_usage).await;
    let Outcome::Ok { data: Data::Operation(record) } = original.outcome else { unreachable!() };
    assert!(matches!(serde_json::from_value::<Outcome>(record.outcome.unwrap()).unwrap(), Outcome::Ok { data: Data::Task(_) }));
    assert!(!reporter.outbox.join(format!("{}.json", component(&usage_id))).exists());
    let diagnostic_reply = original_report_reply(&f, &diagnostic).await;
    let Outcome::Ok { data: Data::Operation(record) } = diagnostic_reply.outcome else { unreachable!() };
    assert!(matches!(serde_json::from_value::<Outcome>(record.outcome.unwrap()).unwrap(), Outcome::Ok { data: Data::Acknowledged }));
    assert!(f.drop_id.lock().unwrap().is_none(), "the selected postcommit ACK loss must execute");
    let sends = f.required_report.lock().unwrap().as_ref().unwrap().seen.iter().filter_map(|op| match op {
        Operation::ObserveWorker { report } => Some(report.clone()), _ => None,
    }).collect::<Vec<_>>();
    assert_eq!(sends, vec![diagnostic, expected_usage], "no charge or diagnostic resend after ACK loss");
    assert_no_diagnostic_provider_connection(&provider);
}

#[tokio::test]
async fn mutation_registration_and_admission_ack_loss_recover_original_bytes() {
    let f = Fixture::new("lost-ack").await;
    let p = f.state.join("mutations/registration.json");
    let mut registration = create(
        &p,
        Operation::RegisterWorker {
            operation_id: "register-lost".into(),
            worker: worker("one"),
        },
        0,
    )
    .unwrap();
    *f.drop_id.lock().unwrap() = Some("register-lost".into());
    assert!(
        resolve(&f.client, &p, &mut registration)
            .await
            .unwrap()
            .is_none()
    );
    let original = std::fs::read(&p).unwrap();
    let mut conflicting_worker = worker("one");
    conflicting_worker.coding_capacity = 2;
    let advanced = f
        .client
        .call(Operation::RegisterWorker {
            operation_id: "advance-observed-revision".into(),
            worker: conflicting_worker,
        })
        .await
        .unwrap();
    assert!(matches!(advanced.outcome, Outcome::Refused { .. }));
    assert!(
        advanced.commit.is_some(),
        "the independent refusal must actually commit"
    );
    let mut restored = read(&p).unwrap().unwrap();
    assert!(matches!(
        resolve(&f.client, &p, &mut restored).await.unwrap(),
        Some(Outcome::Ok {
            data: Data::Worker(_)
        })
    ));
    assert_eq!(
        f.sends
            .lock()
            .unwrap()
            .iter()
            .filter(|operation| operation.mutation_id() == Some("register-lost"))
            .count(),
        1,
        "lookup must reconcile commit without another registration"
    );
    assert_ne!(
        std::fs::read(&p).unwrap(),
        original,
        "record exact response before consuming"
    );
    let context = restored.outcome_context.as_ref().unwrap();
    assert_eq!(context.commit.operation_id, "register-lost");
    assert!(context.observed_controller_revision > context.commit.controller_revision);
    let retained_context = context.clone();
    let submitted = f.submit("work").await;
    resolve(&f.client, &p, &mut restored).await.unwrap();
    assert_eq!(
        read(&p).unwrap().unwrap().outcome_context,
        Some(retained_context),
        "cached outcome must not restamp its original receipt or read context"
    );
    let p = f.state.join("mutations/await.json");
    let mut pending = create(&p, wait("wait-lost", "one", vec![]), 0).unwrap();
    *f.drop_id.lock().unwrap() = Some("wait-lost".into());
    assert!(
        resolve(&f.client, &p, &mut pending)
            .await
            .unwrap()
            .is_none()
    );
    assert!(command_receipts(&f.state).unwrap().is_empty());
    let mut restored = read(&p).unwrap().unwrap();
    let outcome = resolve(&f.client, &p, &mut restored)
        .await
        .unwrap()
        .unwrap();
    let Some(Outcome::Ok {
        data: Data::Commands(saved),
    }) = read(&p).unwrap().unwrap().outcome
    else {
        panic!("response not durable");
    };
    assert_eq!(
        outcome,
        Outcome::Ok {
            data: Data::Commands(saved.clone())
        }
    );
    assert_eq!(saved.len(), 1);
    assert_eq!(
        f.sends
            .lock()
            .unwrap()
            .iter()
            .filter(|r| matches!(r, Operation::AwaitCommands { .. }))
            .count(),
        1
    );
    let actual = f.task(&submitted.id).await;
    let WorkerCommand::RunAttempt { attempt, .. } = &saved[0] else {
        panic!()
    };
    assert_eq!(actual.attempt.as_ref().unwrap().id, attempt.id);
    let changed = f
        .call(wait("wait-lost", "one", vec!["different".into()]))
        .await;
    assert!(matches!(changed, Outcome::Refused { code, .. } if code == "operation_conflict"));
}

#[tokio::test]
async fn mutation_idle_pending_identity_survives_retirement_without_erasing_unknown() {
    let f = Fixture::new("idle-retire").await;
    assert!(register(&f.client, &f.state, worker("one")).await.unwrap());
    let path = f.state.join("mutations/await.json");
    let mut pending = create(&path, wait("idle", "one", vec![]), 0).unwrap();
    let before = std::fs::read(&path).unwrap();
    assert!(
        resolve(&f.client, &path, &mut pending)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert!(matches!(
        acceptance::lookup(&f.client, "idle", &pending.request)
            .await
            .unwrap(),
        acceptance::Lookup::Absent
    ));
    assert!(register(&f.client, &f.state, worker("two")).await.unwrap());
    assert_eq!(
        std::fs::read(&path).unwrap(),
        before,
        "retirement itself cannot consume the original request"
    );
    assert!(
        matches!(resolve(&f.client, &path, &mut pending).await.unwrap(), Some(Outcome::Refused { code, .. }) if code == "stale_incarnation")
    );
    assert!(!pending.consumed);
    consume(&path, &mut pending).unwrap();
}

#[tokio::test]
async fn mutation_cached_launch_and_started_ack_require_current_authority() {
    let f = Fixture::new("stale-launch").await;
    let submitted = f.submit("work").await;
    let command = f.launch().await;
    let WorkerCommand::RunAttempt {
        command_id,
        attempt,
        ..
    } = &command
    else {
        panic!()
    };
    let path = f
        .state
        .join("commands")
        .join(format!("{}.json", component(command_id)));
    record_launch(&path, &command).unwrap();
    let reporter = f.reporter(attempt);
    assert!(
        reporter
            .send_identified(
                format!("{}-initial-start", attempt.id),
                Observation::Started {
                    thread_id: None,
                    turn_id: None
                }
            )
            .await
            .unwrap()
    );
    let current = f.task(&submitted.id).await;
    assert!(matches!(
        f.call(Operation::CancelTask {
            operation_id: "cancel".into(),
            task_id: current.id,
            expected_revision: current.revision
        })
        .await,
        Outcome::Ok { .. }
    ));
    let (control, _cancel) = control();
    assert!(
        !admit_launch(&reporter, &command, &control, &path)
            .await
            .unwrap(),
        "historical Started success cannot authorize a cancelled launch"
    );
    let receipt: Receipt = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert!(!receipt.effects_started);
    assert!(!f.state.join("workspaces").exists());
    let sent = reporter
        .send_identified(
            format!("{}-initial-start", attempt.id),
            Observation::Started {
                thread_id: None,
                turn_id: None,
            },
        )
        .await
        .unwrap();
    assert!(
        sent,
        "negative witness must be an actually replayable old successful receipt"
    );
}

#[tokio::test]
async fn mutation_received_only_restart_stops_without_inventing_unknown_execution() {
    let f = Fixture::new("unconsumed").await;
    let submitted = f.submit("work").await;
    let _command = f.launch().await;
    // Only the exact response is present. No command consumer or executor ran.
    assert!(command_receipts(&f.state).unwrap().is_empty());
    recover_wait(&f.client, &f.state, "worker").await.unwrap();
    process::reconcile_processes(
        &f.state.join("processes"),
        &crate::worker::test_limits().worker,
    )
    .await
    .unwrap();
    account_for_interrupted_commands(&f.client, &f.state, "worker")
        .await
        .unwrap();
    let current = f.task(&submitted.id).await;
    let attempt = current.attempt.unwrap();
    assert!(attempt.stopped);
    assert!(
        !attempt.uncertain,
        "intact received-only phase must not manufacture provider/executor uncertainty"
    );
    assert!(register(&f.client, &f.state, worker("two")).await.unwrap());
}

#[tokio::test]
async fn mutation_new_acknowledgments_are_not_erased_by_older_snapshot() {
    let f = Fixture::new("acks").await;
    f.submit("work").await;
    let command = f.launch().await;
    let (id, _) = command_identity(&command);
    record_launch(
        &f.state
            .join("commands")
            .join(format!("{}.json", component(id))),
        &command,
    )
    .unwrap();
    let snapshot = wait("ack-snapshot", "one", vec![id.into()]);
    let WorkerCommand::RunAttempt { attempt, .. } = &command else {
        panic!()
    };
    let later = WorkerCommand::StopAttempt {
        command_id: "later-stop".into(),
        task_id: attempt.task_id.clone(),
        attempt_id: attempt.id.clone(),
        generation: attempt.generation,
        incarnation: "one".into(),
    };
    record_launch(
        &f.state
            .join("commands")
            .join(format!("{}.json", component("later-stop"))),
        &later,
    )
    .unwrap();
    apply_acknowledgments(&f.state, &snapshot).unwrap();
    assert_eq!(
        pending_acknowledgments(&f.state, "one").unwrap(),
        vec!["later-stop"]
    );
    assert!(
        command_receipts(&f.state)
            .unwrap()
            .iter()
            .all(|(_, r)| !r.effects_started)
    );
}

#[test]
fn mutation_write_failure_and_corrupt_journal_never_create_a_request() {
    let root = std::env::temp_dir().join(fresh_id("bad-journal").unwrap());
    std::fs::create_dir(&root).unwrap();
    let path = root.join("await.json");
    std::fs::create_dir(path.with_extension("pending")).unwrap();
    assert!(create(&path, wait("never-sent", "one", vec![]), 0).is_err());
    assert!(!path.exists());
    std::fs::write(&path, b"{broken").unwrap();
    assert!(read(&path).is_err());
    assert_eq!(std::fs::read(path).unwrap(), b"{broken");
}

// These exact-path cuts inject Linux ENOSPC/EDQUOT; they do not fill a disk or
// establish physical journal headroom. The actual controller/HTTP fixture still
// determines whether a mutation was sent, committed and recovered.
#[tokio::test]
async fn mutation_storage_failure_before_durable_intent_never_dispatches() {
    use process::storage_tests::{Point, inject};
    for errno in [28, 122] {
        for point in [
            Point::Create,
            Point::Write,
            Point::FileSync,
            Point::Rename,
            Point::DirectorySync,
        ] {
            let f = Fixture::new("intent-storage-cut").await;
            let path = f.state.join("mutations/registration.json");
            let fault = inject(
                &path,
                point,
                errno,
                if point == Point::Write { 7 } else { 0 },
            );
            assert!(register(&f.client, &f.state, worker("one")).await.is_err());
            assert_eq!(fault.hits(), 1);
            assert!(f.sends.lock().unwrap().is_empty());
            drop(fault);
            if point == Point::DirectorySync {
                // Rename completed but directory durability was not acknowledged.
                // Reopen only to observe the complete original intent. A read is
                // not a successful sync barrier, so do not authorize a later POST.
                let saved = read(&path).unwrap().unwrap();
                assert!(saved.outcome.is_none());
                assert!(saved.outcome_context.is_none());
                assert!(!saved.consumed);
                assert!(!identity(&saved.request).unwrap().is_empty());
                assert!(
                    matches!(&saved.request, Operation::RegisterWorker { worker: planned, .. } if planned == &worker("one"))
                );
                assert!(f.sends.lock().unwrap().is_empty());
            } else {
                assert!(read(&path).unwrap().is_none());
                if point == Point::Write {
                    assert_eq!(
                        std::fs::metadata(path.with_extension("pending"))
                            .unwrap()
                            .len(),
                        7
                    );
                }
            }
        }
    }
}

// Traceability scope: Actual local controller commit precedes the injected reply-write failure; recovery retains the original operation and performs no second mutation dispatch.
//= spec/impl/v2/implementation.md#interrupted-work-is-reconciled
//= type=test
//# An external effect with an uncertain outcome MUST be reconciled before the platform retries that effect.
//# A retry MUST preserve the original external-operation identity where the receiving interface supports deduplication.
#[tokio::test]
async fn mutation_committed_reply_storage_failure_reopens_original_without_resend() {
    use process::storage_tests::Point;
    for errno in [28, 122] {
        for point in [
            Point::Create,
            Point::Write,
            Point::FileSync,
            Point::Rename,
            Point::DirectorySync,
        ] {
            let f = Fixture::new("reply-storage-cut").await;
            let path = f.state.join("mutations/registration.json");
            let request = Operation::RegisterWorker {
                operation_id: "one-original-registration".into(),
                worker: worker("one"),
            };
            let mut pending = create(&path, request.clone(), 0).unwrap();
            let original = std::fs::read(&path).unwrap();
            let fault = f.fail_after_commit(
                &path,
                point,
                errno,
                if point == Point::Write { 11 } else { 0 },
            );
            // This POST really commits in the fixture controller. Failure to save
            // its reply must not return an outcome that authorizes further work.
            assert!(resolve(&f.client, &path, &mut pending).await.is_err());
            assert_eq!(fault.lock().unwrap().as_ref().unwrap().hits(), 1);
            assert_eq!(f.sends.lock().unwrap().len(), 1);
            assert_eq!(
                serde_json::to_value(&f.sends.lock().unwrap()[0]).unwrap(),
                serde_json::to_value(&request).unwrap()
            );
            drop(pending); // Discard the failed writer's non-durable in-memory result.
            drop(fault);
            let mut reopened = read(&path).unwrap().unwrap();
            assert_eq!(
                serde_json::to_value(&reopened.request).unwrap(),
                serde_json::to_value(&request).unwrap()
            );
            assert!(!reopened.consumed);
            if point != Point::DirectorySync {
                assert_eq!(std::fs::read(&path).unwrap(), original);
                assert!(reopened.outcome.is_none());
                assert!(reopened.outcome_context.is_none());
            }
            let acceptance::Lookup::Recorded(expected, expected_context) =
                acceptance::lookup(&f.client, identity(&request).unwrap(), &request)
                    .await
                    .unwrap()
            else {
                panic!("fixture mutation did not commit")
            };
            if point == Point::DirectorySync {
                // Exact visible bytes and the controller's original receipt are
                // observable, but this read does not repair the failed sync. Do
                // not call resolve's saved-outcome fast path a durability proof.
                assert_eq!(
                    serde_json::to_value(reopened.outcome.as_ref().unwrap()).unwrap(),
                    serde_json::to_value(&expected).unwrap()
                );
                assert_eq!(
                    reopened.outcome_context.as_ref().unwrap().commit,
                    expected_context.commit
                );
                assert_eq!(f.sends.lock().unwrap().len(), 1);
                continue;
            }
            let recovered = resolve(&f.client, &path, &mut reopened)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::to_value(&recovered).unwrap(),
                serde_json::to_value(&expected).unwrap()
            );
            let durable = read(&path).unwrap().unwrap();
            assert_eq!(
                serde_json::to_value(durable.outcome.as_ref().unwrap()).unwrap(),
                serde_json::to_value(&expected).unwrap()
            );
            assert_eq!(
                durable.outcome_context.as_ref().unwrap().commit,
                expected_context.commit
            );
            assert_eq!(
                serde_json::to_value(&durable.request).unwrap(),
                serde_json::to_value(&request).unwrap()
            );
            assert_eq!(
                f.sends.lock().unwrap().len(),
                1,
                "receipt reconciliation must not resend the committed mutation"
            );
        }
    }
}

// Traceability scope: Actual local controller commit precedes the injected reply-write failure; recovery retains the original operation and performs no second mutation dispatch.
//= spec/impl/v2/implementation.md#interrupted-work-is-reconciled
//= type=test
//# An external effect with an uncertain outcome MUST be reconciled before the platform retries that effect.
#[tokio::test]
async fn mutation_committed_reply_repeated_storage_failure_retains_unknown_original() {
    use process::storage_tests::{Point, inject};
    let f = Fixture::new("reply-remains-full").await;
    let path = f.state.join("mutations/registration.json");
    let request = Operation::RegisterWorker {
        operation_id: "retained-original-registration".into(),
        worker: worker("one"),
    };
    let mut pending = create(&path, request.clone(), 0).unwrap();
    let original = std::fs::read(&path).unwrap();
    for iteration in 0..2 {
        let fault = if iteration == 0 {
            f.fail_after_commit(&path, Point::Write, 28, 13)
        } else {
            Arc::new(Mutex::new(Some(inject(&path, Point::Write, 28, 13))))
        };
        assert!(resolve(&f.client, &path, &mut pending).await.is_err());
        assert_eq!(fault.lock().unwrap().as_ref().unwrap().hits(), 1);
        drop(fault);
        assert_eq!(std::fs::read(&path).unwrap(), original);
        // Reopen, rather than reuse a failed writer's updated in-memory state.
        pending = read(&path).unwrap().unwrap();
        assert!(pending.outcome.is_none());
        assert!(pending.outcome_context.is_none());
        assert!(!pending.consumed);
        assert_eq!(
            serde_json::to_value(&pending.request).unwrap(),
            serde_json::to_value(&request).unwrap()
        );
        assert_eq!(f.sends.lock().unwrap().len(), 1);
    }
    assert!(register(&f.client, &f.state, worker("one")).await.unwrap());
    let durable = read(&path).unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(&durable.request).unwrap(),
        serde_json::to_value(&request).unwrap()
    );
    assert!(matches!(durable.outcome, Some(Outcome::Ok { .. })));
    assert_eq!(f.sends.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn mutation_old_wait_admission_wins_before_replacement_registration() {
    let f = Fixture::new("admission-before-replace").await;
    assert!(register(&f.client, &f.state, worker("one")).await.unwrap());
    let submitted = f.submit("work").await;
    let path = f.state.join("mutations/await.json");
    let pending = create(&path, wait("old-pending", "one", vec![]), 0).unwrap();
    // The actual controller commits, while the worker has not recorded its reply.
    assert!(matches!(
        f.call(pending.request.clone()).await,
        Outcome::Ok {
            data: Data::Commands(_)
        }
    ));
    assert!(!register(&f.client, &f.state, worker("two")).await.unwrap());
    let registration_path = f.state.join("mutations/registration.json");
    let refused = read(&registration_path).unwrap().unwrap();
    assert!(
        matches!(&refused.outcome, Some(Outcome::Refused { code, .. }) if code == "reconciliation_required")
    );
    let old_id = identity(&refused.request).unwrap().to_owned();
    recover_wait(&f.client, &f.state, "worker").await.unwrap();
    process::reconcile_processes(
        &f.state.join("processes"),
        &crate::worker::test_limits().worker,
    )
    .await
    .unwrap();
    account_for_interrupted_commands(&f.client, &f.state, "worker")
        .await
        .unwrap();
    let stopped = f.task(&submitted.id).await;
    assert!(stopped.attempt.as_ref().unwrap().stopped);
    assert!(!stopped.attempt.as_ref().unwrap().uncertain);
    assert!(register(&f.client, &f.state, worker("two")).await.unwrap());
    let replacement = read(&registration_path).unwrap().unwrap();
    assert_ne!(identity(&replacement.request).unwrap(), old_id);
    assert!(
        matches!(acceptance::lookup(&f.client, &old_id, &refused.request).await.unwrap(), acceptance::Lookup::Recorded(Outcome::Refused { code, .. }, _) if code == "reconciliation_required")
    );
    assert!(!f.state.join("workspaces").exists());
}

#[tokio::test]
async fn mutation_each_settlement_keeps_its_own_immutable_decision() {
    let f = Fixture::new("settlements").await;
    f.submit("work").await;
    let command = f.launch().await;
    let WorkerCommand::RunAttempt {
        command_id,
        attempt,
        ..
    } = &command
    else {
        panic!()
    };
    let launch_path = f
        .state
        .join("commands")
        .join(format!("{}.json", component(command_id)));
    record_launch(&launch_path, &command).unwrap();
    let reporter = f.reporter(attempt);
    let (control, _cancel) = control();
    assert!(
        admit_launch(&reporter, &command, &control, &launch_path)
            .await
            .unwrap()
    );
    // This case tests the common immutable-settlement journal, not typed PR
    // publication. The supported generic effect performs no external action.
    let intended = json!({"fixture":"no external action performed"});
    let prepared = f.call(Operation::PrepareOperation {
        operation_id: "external".into(),
        task_id: attempt.task_id.clone(),
        attempt_id: attempt.id.clone(),
        generation: attempt.generation,
        kind: "merge".into(),
        intended: intended.clone(),
    }).await;
    assert!(
        matches!(&prepared, Outcome::Ok { data: Data::Operation(record) }
            if record.id == "external" && record.task_id == attempt.task_id
                && record.attempt_id == attempt.id && record.generation == attempt.generation
                && record.kind == "merge" && record.intended == intended
                && record.state == "pending" && record.outcome.is_none()),
        "generic settlement fixture must prepare the exact pending effect: {prepared:?}"
    );
    let uncertain = Operation::SettleOperation {
        settlement_id: "settlement-uncertain".into(),
        operation_id: "external".into(),
        task_id: attempt.task_id.clone(),
        attempt_id: attempt.id.clone(),
        generation: attempt.generation,
        outcome: json!({"acknowledged":false}),
        checkpoint: None,
    };
    *f.drop_id.lock().unwrap() = Some("settlement-uncertain".into());
    let first = settle(&f.client, &f.state, uncertain.clone(), &control)
        .await
        .unwrap();
    assert!(matches!(first, Data::Operation(ref record) if record.state == "uncertain"));
    let final_observation = Operation::SettleOperation {
        settlement_id: "settlement-not-applied".into(),
        operation_id: "external".into(),
        task_id: attempt.task_id.clone(),
        attempt_id: attempt.id.clone(),
        generation: attempt.generation,
        outcome: json!({"acknowledged":true,"not_applied":true}),
        checkpoint: None,
    };
    let final_result = settle(&f.client, &f.state, final_observation.clone(), &control)
        .await
        .unwrap();
    assert!(matches!(final_result, Data::Operation(ref record) if record.state == "settled"));
    let external_snapshot = f
        .client
        .call(Operation::GetOperation {
            operation_id: "external".into(),
        })
        .await
        .unwrap();
    assert!(
        external_snapshot.commit.is_none(),
        "mutable external state is not an immutable application receipt"
    );
    assert!(external_snapshot.observed_controller_revision.is_some());
    assert!(
        matches!(acceptance::lookup(&f.client, "settlement-uncertain", &uncertain).await.unwrap(), acceptance::Lookup::Recorded(Outcome::Ok { data: Data::Operation(record) }, _) if record.state == "uncertain")
    );
    assert!(
        matches!(acceptance::lookup(&f.client, "settlement-not-applied", &final_observation).await.unwrap(), acceptance::Lookup::Recorded(Outcome::Ok { data: Data::Operation(record) }, _) if record.state == "settled")
    );
    assert_eq!(f.sends.lock().unwrap().iter().filter(|op| matches!(op, Operation::SettleOperation { settlement_id, .. } if settlement_id == "settlement-uncertain")).count(), 1, "lost ACK is recovered by exact lookup");
    let saved_path = settlement_path(&f.state, "external");
    let saved = std::fs::read(&saved_path).unwrap();
    let mut conflicting = final_observation;
    if let Operation::SettleOperation { outcome, .. } = &mut conflicting {
        *outcome = json!({"acknowledged":true,"different":true});
    }
    assert!(
        settle(&f.client, &f.state, conflicting, &control)
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(saved_path).unwrap(), saved);
}

#[tokio::test]
async fn mutation_changed_empty_ack_reply_is_durable_and_final() {
    let f = Fixture::new("changed-empty").await;
    f.submit("work").await;
    let command = f.launch().await;
    let (id, _) = command_identity(&command);
    record_launch(
        &f.state
            .join("commands")
            .join(format!("{}.json", component(id))),
        &command,
    )
    .unwrap();
    let path = f.state.join("mutations/ack-test.json");
    let mut pending = create(&path, wait("ack-only", "one", vec![id.into()]), 0).unwrap();
    let result = resolve(&f.client, &path, &mut pending).await.unwrap();
    assert!(
        matches!(&result, Some(Outcome::Ok { data: Data::Commands(commands) }) if commands.is_empty())
    );
    assert_eq!(read(&path).unwrap().unwrap().outcome, result);
    apply_acknowledgments(&f.state, &pending.request).unwrap();
    assert!(pending_acknowledgments(&f.state, "one").unwrap().is_empty());
    assert!(
        matches!(acceptance::lookup(&f.client, "ack-only", &pending.request).await.unwrap(), acceptance::Lookup::Recorded(Outcome::Ok { data: Data::Commands(commands) }, _) if commands.is_empty())
    );
}

#[tokio::test]
async fn mutation_initial_start_refusal_and_journal_failure_prevent_effect_marker() {
    let f = Fixture::new("start-refusal").await;
    let submitted = f.submit("work").await;
    let command = f.launch().await;
    let WorkerCommand::RunAttempt {
        command_id,
        attempt,
        ..
    } = &command
    else {
        panic!()
    };
    let path = f
        .state
        .join("commands")
        .join(format!("{}.json", component(command_id)));
    record_launch(&path, &command).unwrap();
    let reporter = f.reporter(attempt);
    let (control, _cancel) = control();
    let report_id = format!("{}-initial-start", attempt.id);
    let pending = reporter
        .outbox
        .join(format!("{}.pending", component(&report_id)));
    std::fs::create_dir(&pending).unwrap();
    assert!(
        admit_launch(&reporter, &command, &control, &path)
            .await
            .is_err()
    );
    assert!(!read_receipt(&path).unwrap().effects_started);
    assert!(
        matches!(f.call(Operation::GetOperation { operation_id: report_id.clone() }).await, Outcome::Refused { code, .. } if code == "not_found")
    );
    std::fs::remove_dir(pending).unwrap();
    let current = f.task(&submitted.id).await;
    assert!(matches!(
        f.call(Operation::CancelTask {
            operation_id: "cancel-before-start".into(),
            task_id: current.id,
            expected_revision: current.revision
        })
        .await,
        Outcome::Ok { .. }
    ));
    assert!(
        !admit_launch(&reporter, &command, &control, &path)
            .await
            .unwrap()
    );
    assert!(!read_receipt(&path).unwrap().effects_started);
    assert!(
        reporter
            .outbox
            .join(format!("{}.refused", component(&report_id)))
            .exists()
    );
}

#[tokio::test]
async fn mutation_full_normal_pool_defers_started_but_closes_cancelled_attempt() {
    let f = Fixture::with_receipts("full-pool-cleanup", 3).await;
    let submitted = f.submit("work").await;
    let command = f.launch().await;
    let WorkerCommand::RunAttempt {
        command_id,
        attempt,
        ..
    } = &command
    else {
        panic!()
    };
    let receipt_path = f
        .state
        .join("commands")
        .join(format!("{}.json", component(command_id)));
    record_launch(&receipt_path, &command).unwrap();
    let reporter = f.reporter(attempt);
    let start_id = format!("{}-initial-start", attempt.id);
    assert!(
        !reporter
            .send_identified(
                start_id.clone(),
                Observation::Started {
                    thread_id: None,
                    turn_id: None
                }
            )
            .await
            .unwrap()
    );
    let start_path = reporter
        .outbox
        .join(format!("{}.json", component(&start_id)));
    let exact_pending = std::fs::read(&start_path).unwrap();
    assert!(
        matches!(f.call(Operation::GetOperation { operation_id: start_id.clone() }).await, Outcome::Refused { code, .. } if code == "not_found")
    );
    let current = f.task(&submitted.id).await;
    assert!(matches!(
        f.call(Operation::CancelTask {
            operation_id: "cancel-full".into(),
            task_id: current.id,
            expected_revision: current.revision
        })
        .await,
        Outcome::Ok { .. }
    ));
    let stop_path = f.state.join("mutations/stop-wait.json");
    let mut waiting = create(
        &stop_path,
        wait("deliver-stop-full", "one", vec![command_id.clone()]),
        0,
    )
    .unwrap();
    let delivered = resolve(&f.client, &stop_path, &mut waiting).await.unwrap();
    assert!(
        matches!(delivered, Some(Outcome::Ok { data: Data::Commands(ref commands) }) if commands.iter().any(|c|matches!(c,WorkerCommand::StopAttempt {..})))
    );
    let (control, cancel) = control();
    cancel.send(true).unwrap();
    assert!(
        admit_launch(&reporter, &command, &control, &receipt_path)
            .await
            .is_err()
    );
    assert!(!read_receipt(&receipt_path).unwrap().effects_started);
    assert!(
        reporter
            .send_acknowledged(Observation::ProcessTreeStopped)
            .await
            .unwrap(),
        "uncommitted Started must not block reserved verified stop"
    );
    let actual = f.task(&submitted.id).await;
    assert_eq!(actual.status, TaskStatus::Cancelled);
    assert!(actual.attempt.unwrap().stopped);
    assert_eq!(
        std::fs::read(&start_path).unwrap(),
        exact_pending,
        "original pending informational record is not discarded or rewritten"
    );
    assert!(
        matches!(f.call(Operation::GetOperation { operation_id: start_id }).await, Outcome::Refused { code, .. } if code == "not_found")
    );
    reports_ready_before_registration(&f.client, &reporter.outbox, &reporter.outbox_lock)
        .await
        .unwrap();
    assert!(start_path.exists());
}

#[tokio::test]
async fn mutation_deferred_information_never_lets_stop_overtake_unknown() {
    let f = Fixture::new("unknown-before-stop").await;
    let submitted = f.submit("work").await;
    let command = f.launch().await;
    let WorkerCommand::RunAttempt { attempt, .. } = &command else {
        panic!()
    };
    let reporter = f.reporter(attempt);
    *f.deny_report.lock().unwrap() = Some("z-unknown".into());
    assert!(
        !reporter
            .send_identified(
                "z-unknown".into(),
                Observation::ExecutionUncertain {
                    message: "controlled unknown outcome".into()
                }
            )
            .await
            .unwrap()
    );
    let unknown_path = reporter
        .outbox
        .join(format!("{}.json", component("z-unknown")));
    let original = std::fs::read(&unknown_path).unwrap();
    // Lexical ordering deliberately puts stop first: semantic uncertainty is
    // still required before this independently verified local termination.
    assert!(
        !reporter
            .send_identified("a-stopped".into(), Observation::ProcessTreeStopped)
            .await
            .unwrap()
    );
    assert!(!f.task(&submitted.id).await.attempt.unwrap().stopped);
    assert_eq!(std::fs::read(&unknown_path).unwrap(), original);
    assert!(
        reports_ready_before_registration(&f.client, &reporter.outbox, &reporter.outbox_lock)
            .await
            .is_err()
    );
    *f.deny_report.lock().unwrap() = None;
    flush_outbox(&f.client, &reporter.outbox).await.unwrap();
    let actual = f.task(&submitted.id).await;
    assert_eq!(actual.status, TaskStatus::Failed);
    assert!(actual.attempt.as_ref().unwrap().uncertain);
    assert!(actual.attempt.unwrap().stopped);
}

#[tokio::test]
async fn mutation_retained_outcome_requires_original_committed_context() {
    let f = Fixture::new("retained-commit").await;
    assert!(register(&f.client, &f.state, worker("one")).await.unwrap());
    let path = f.state.join("mutations/registration.json");
    let original = std::fs::read(&path).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&original).unwrap();
    for mode in 0..3 {
        let mut changed = value.clone();
        match mode {
            0 => changed["outcome_context"] = serde_json::Value::Null,
            1 => changed["outcome_context"]["commit"]["operation_id"] = json!("another-mutation"),
            _ => changed["outcome_context"]["commit"]["controller_revision"] = json!(0),
        }
        let bytes = serde_json::to_vec(&changed).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        assert!(read(&path).is_err());
        assert_eq!(
            std::fs::read(&path).unwrap(),
            bytes,
            "invalid historical context must not be invented or rewritten"
        );
    }
    std::fs::write(&path, &original).unwrap();
    assert!(read(&path).unwrap().unwrap().outcome_context.is_some());
}

#[tokio::test]
async fn mutation_reserves_full_outcome_context_before_registration_post() {
    let root = std::env::temp_dir().join(fresh_id("journal-reserve").unwrap());
    std::fs::create_dir_all(root.join("commands")).unwrap();
    std::fs::create_dir_all(root.join("mutations")).unwrap();
    let original = b"original unrelated pending evidence";
    std::fs::write(root.join("preserved.json"), original).unwrap();
    let mut limits = (*crate::worker::test_limits()).clone();
    limits.worker.mutation_record_bytes = limits.transport.body_bytes;
    limits.validate().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = ApiClient::new(
        format!("http://{}", listener.local_addr().unwrap()),
        Arc::new(limits),
    )
    .unwrap();
    let error = register(&client, &root, worker("one")).await.unwrap_err();
    assert_eq!(
        error,
        "mutation journal cannot reserve complete outcome/context (worker.mutation_record_bytes; reserves transport.body_bytes and committed context)"
    );
    assert!(!root.join("mutations/registration.json").exists());
    assert!(!root.join("mutations/registration.pending").exists());
    assert_eq!(
        std::fs::read(root.join("preserved.json")).unwrap(),
        original
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err(),
        "reservation failure must precede any HTTP connection"
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn mutation_reopened_unsynced_intent_requires_barrier_before_post() {
    use process::storage_tests::{Point, inject};
    for point in [
        Point::Create,
        Point::Write,
        Point::FileSync,
        Point::Rename,
        Point::DirectorySync,
    ] {
        let f = Fixture::new("intent-recovery-barrier").await;
        let path = f.state.join("mutations/registration.json");
        let request = Operation::RegisterWorker {
            operation_id: "unsynced-original-registration".into(),
            worker: worker("one"),
        };
        let first = inject(&path, Point::DirectorySync, 5, 0);
        assert!(create(&path, request.clone(), 0).is_err());
        assert_eq!(first.hits(), 1);
        drop(first);
        let original = std::fs::read(&path).unwrap();
        let visible = read(&path).unwrap().unwrap();
        assert!(visible.outcome.is_none());
        assert_eq!(
            serde_json::to_value(&visible.request).unwrap(),
            serde_json::to_value(&request).unwrap()
        );
        let blocked = inject(&path, point, 28, if point == Point::Write { 7 } else { 0 });
        assert!(register(&f.client, &f.state, worker("one")).await.is_err());
        assert_eq!(blocked.hits(), 1);
        assert!(
            f.sends.lock().unwrap().is_empty(),
            "failed barrier must precede the first POST"
        );
        assert_eq!(std::fs::read(&path).unwrap(), original);
        drop(blocked);
        assert!(register(&f.client, &f.state, worker("one")).await.unwrap());
        let durable = read(&path).unwrap().unwrap();
        assert_eq!(
            serde_json::to_value(&durable.request).unwrap(),
            serde_json::to_value(&request).unwrap()
        );
        assert!(matches!(durable.outcome, Some(Outcome::Ok { .. })));
        assert_eq!(f.sends.lock().unwrap().len(), 1);
        assert_eq!(
            serde_json::to_value(&f.sends.lock().unwrap()[0]).unwrap(),
            serde_json::to_value(&request).unwrap()
        );
    }
}

#[tokio::test]
async fn mutation_saved_results_require_barrier_for_resolve_and_lookup_only_recovery() {
    use process::storage_tests::{Point, inject};
    for reopen in [false, true] {
        let f = Fixture::new("result-recovery-barrier").await;
        let path = f.state.join("mutations/registration.json");
        let request = Operation::RegisterWorker {
            operation_id: "committed-original-registration".into(),
            worker: worker("one"),
        };
        let mut journal = create(&path, request.clone(), 0).unwrap();
        let first = f.fail_after_commit(&path, Point::DirectorySync, 5, 0);
        assert!(resolve(&f.client, &path, &mut journal).await.is_err());
        assert_eq!(first.lock().unwrap().as_ref().unwrap().hits(), 1);
        assert_eq!(f.sends.lock().unwrap().len(), 1);
        drop(first);
        if reopen {
            journal = read(&path).unwrap().unwrap();
        }
        let acceptance::Lookup::Recorded(expected, context) =
            acceptance::lookup(&f.client, identity(&request).unwrap(), &request)
                .await
                .unwrap()
        else {
            panic!("original mutation must be committed")
        };
        let original = std::fs::read(&path).unwrap();
        for lookup_only in [false, true] {
            for point in [Point::FileSync, Point::DirectorySync] {
                let blocked = inject(&path, point, 5, 0);
                let result = if lookup_only {
                    recorded(&f.client, &path, &mut journal).await
                } else {
                    resolve(&f.client, &path, &mut journal).await
                };
                assert!(
                    result.is_err(),
                    "saved outcome must not bypass failed durability"
                );
                assert_eq!(blocked.hits(), 1);
                assert_eq!(f.sends.lock().unwrap().len(), 1);
                assert_eq!(std::fs::read(&path).unwrap(), original);
                drop(blocked);
                let recovered = if lookup_only {
                    recorded(&f.client, &path, &mut journal).await
                } else {
                    resolve(&f.client, &path, &mut journal).await
                }
                .unwrap()
                .unwrap();
                assert_eq!(
                    serde_json::to_value(recovered).unwrap(),
                    serde_json::to_value(&expected).unwrap()
                );
                let durable = read(&path).unwrap().unwrap();
                assert_eq!(durable.outcome_context.unwrap().commit, context.commit);
                assert_eq!(
                    serde_json::to_value(&durable.request).unwrap(),
                    serde_json::to_value(&request).unwrap()
                );
                assert!(!durable.consumed);
                assert_eq!(f.sends.lock().unwrap().len(), 1);
            }
        }
    }
}

#[tokio::test]
async fn mutation_absent_lookup_remains_read_only_under_storage_failure() {
    use process::storage_tests::{Point, inject};
    let f = Fixture::new("lookup-without-storage").await;
    let path = f.state.join("mutations/registration.json");
    let mut journal = create(
        &path,
        Operation::RegisterWorker {
            operation_id: "never-dispatched-registration".into(),
            worker: worker("one"),
        },
        0,
    )
    .unwrap();
    let original = std::fs::read(&path).unwrap();
    let fault = inject(&path, Point::Create, 28, 0);
    assert!(
        recorded(&f.client, &path, &mut journal)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(fault.hits(), 0, "lookup absence must not require a write");
    assert!(f.sends.lock().unwrap().is_empty());
    assert_eq!(std::fs::read(&path).unwrap(), original);
}


#[tokio::test]
async fn mutation_await_pending_unavailable_and_recorded_keep_original_occurrence() {
    let f = Fixture::new("await-status").await;
    assert!(register(&f.client, &f.state, worker("one")).await.unwrap());
    let path = f.state.join("mutations/await-status.json");
    let operation_id = "original-pending-wait";
    let mut journal = Box::new(create(&path, wait(operation_id, "one", vec![]), 0).unwrap());
    let original_request = journal.request.clone();
    let original_bytes = std::fs::read(&path).unwrap();
    for _ in 0..3 {
        let (next, result) = resolve_await(f.client.clone(), path.clone(), journal).await;
        journal = next;
        assert!(matches!(result.unwrap(), Resolution::Pending));
        assert_eq!(serde_json::to_value(&journal.request).unwrap(), serde_json::to_value(&original_request).unwrap());
        assert!(journal.outcome.is_none() && journal.outcome_context.is_none() && !journal.consumed);
        assert_eq!(std::fs::read(&path).unwrap(), original_bytes);
    }
    let uncommitted = f.client.call(Operation::GetOperation { operation_id: operation_id.into() }).await.unwrap();
    assert!(matches!(uncommitted.outcome, Outcome::Refused { ref code, .. } if code == "not_found"));
    assert!(uncommitted.commit.is_none());
    // A refused local TCP connection is unavailable, not the controller's exact
    // Pending response, and cannot consume or alter this original occurrence.
    let unused = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let unavailable = ApiClient::new(format!("http://{}", unused.local_addr().unwrap()), crate::worker::test_limits()).unwrap();
    drop(unused);
    let (next, result) = resolve_await(unavailable, path.clone(), journal).await;
    journal = next;
    assert!(matches!(result.unwrap(), Resolution::Unavailable));
    assert_eq!(std::fs::read(&path).unwrap(), original_bytes);
    let submitted = f.submit("work-after-pending").await;
    *f.drop_id.lock().unwrap() = Some(operation_id.into());
    let (next, result) = resolve_await(f.client.clone(), path.clone(), journal).await;
    journal = next;
    assert!(matches!(result.unwrap(), Resolution::Unavailable));
    assert_eq!(std::fs::read(&path).unwrap(), original_bytes, "lost response is not a fabricated outcome");
    let sends_after_commit = f.sends.lock().unwrap().iter().filter(|operation| operation.mutation_id() == Some(operation_id)).count();
    assert_eq!(sends_after_commit, 4, "three Pending calls and one committed original delivery");
    let (mut journal, result) = resolve_await(f.client.clone(), path.clone(), journal).await;
    let Resolution::Recorded(outcome) = result.unwrap() else { panic!("original receipt not recovered") };
    assert!(matches!(&outcome, Outcome::Ok { data: Data::Commands(commands) }
        if commands.iter().any(|command| matches!(command, WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == submitted.id))));
    assert_eq!(serde_json::to_value(&journal.request).unwrap(), serde_json::to_value(&original_request).unwrap());
    assert_eq!(journal.outcome, Some(outcome.clone()));
    assert_eq!(journal.outcome_context.as_ref().unwrap().commit.operation_id, operation_id);
    assert_eq!(read(&path).unwrap().unwrap().outcome, Some(outcome.clone()));
    let retained = std::fs::read(&path).unwrap();
    assert_eq!(resolve(&f.client, &path, &mut journal).await.unwrap(), Some(outcome));
    assert_eq!(std::fs::read(&path).unwrap(), retained);
    assert_eq!(f.sends.lock().unwrap().iter().filter(|operation| operation.mutation_id() == Some(operation_id)).count(), sends_after_commit,
        "receipt lookup and cached replay must not resend the committed wait");
}


// Exercise the startup consumer with a real accepted command and controller
// receipt. The fixture controller's metadata differs from worker startup data.
async fn accepted_recovery_settlement(
    f: &Fixture,
    checkpoint_publication: bool,
) -> (WorkerCommand, Operation, PathBuf, Arc<crate::limits::RuntimeLimits>) {
    f.submit("recovery-work").await;
    let command = f.launch().await;
    let WorkerCommand::RunAttempt { command_id, attempt, task, .. } = &command else {
        panic!("expected original run command")
    };
    task.execution_limits().unwrap();
    let limits = Arc::new(task.resolved_limits.as_ref().unwrap().global_metadata.as_ref().clone());
    assert_ne!(*limits, *f.client.limits(), "must distinguish accepted and startup policy");
    let path = f.state.join("commands").join(format!("{}.json", component(command_id)));
    record_launch(&path, &command).unwrap();
    let mut reporter = f.reporter(attempt);
    reporter.client = ApiClient::new(&f.controller_url, limits.clone()).unwrap();
    let (mut control, _cancel) = control();
    control.runtime_limits = limits.clone();
    assert!(admit_launch(&reporter, &command, &control, &path).await.unwrap());
    let request = if checkpoint_publication {
        std::fs::create_dir(f.state.join("publications")).unwrap();
        assert!(reporter.send_identified(
            format!("{}-completed-association", attempt.id),
            Observation::Started { thread_id: Some("original-thread".into()), turn_id: Some("original-turn".into()) },
        ).await.unwrap());
        assert!(reporter.send_identified(
            format!("{}-completed-executor", attempt.id),
            Observation::ExecutorOutcome { success: true, summary: "fixture executor completed".into() },
        ).await.unwrap());
        let checkpoint = git::checkpoint(
            task.repository.as_ref().unwrap(), &attempt.task_id, &attempt.id, "b".repeat(40),
        ).unwrap();
        let mut phase = publication::prepare(&f.state, &command, &reporter, &checkpoint, &control)
            .await.unwrap();
        phase.mark_started(&f.state, attempt).unwrap();
        Operation::SettleOperation {
            settlement_id: phase.settlement_id.clone(),
            operation_id: checkpoint.operation_id.clone(),
            task_id: attempt.task_id.clone(), attempt_id: attempt.id.clone(), generation: attempt.generation,
            outcome: json!({"acknowledged": true}), checkpoint: Some(checkpoint),
        }
    } else {
        // This branch exercises generic archived decisions; typed PR and
        // checkpoint publication use their own bound intent/phase fixtures.
        let intended = json!({"fixture": "no external action"});
        let prepared = f.call(Operation::PrepareOperation {
            operation_id: "original-external".into(), task_id: attempt.task_id.clone(),
            attempt_id: attempt.id.clone(), generation: attempt.generation,
            kind: "merge".into(), intended: intended.clone(),
        }).await;
        assert!(
            matches!(&prepared, Outcome::Ok { data: Data::Operation(record) }
                if record.id == "original-external" && record.task_id == attempt.task_id
                    && record.attempt_id == attempt.id && record.generation == attempt.generation
                    && record.kind == "merge" && record.intended == intended
                    && record.state == "pending" && record.outcome.is_none()),
            "generic recovery fixture must prepare the exact pending effect: {prepared:?}"
        );
        Operation::SettleOperation {
            settlement_id: "original-external-settlement".into(), operation_id: "original-external".into(),
            task_id: attempt.task_id.clone(), attempt_id: attempt.id.clone(), generation: attempt.generation,
            outcome: json!({"acknowledged": true, "not_applied": true}), checkpoint: None,
        }
    };
    let Operation::SettleOperation { operation_id, .. } = &request else { unreachable!() };
    let settlement = settlement_path(&f.state, operation_id);
    (command, request, settlement, limits)
}

#[tokio::test]
async fn mutation_recovery_consumed_checkpoint_uses_original_command_policy() {
    let f = Fixture::new("recovery-consumed-policy").await;
    let (command, request, path, limits) = accepted_recovery_settlement(&f, true).await;
    let original_client = ApiClient::new(&f.controller_url, limits.clone()).unwrap();
    let mut journal = super::create(&path, request, 0, &limits).unwrap();
    assert!(matches!(resolve(&original_client, &path, &mut journal).await.unwrap(), Some(Outcome::Ok { .. })));
    consume(&path, &mut journal).unwrap();
    // A terminal command is still the retained authority for a consumed record.
    let WorkerCommand::RunAttempt { command_id, attempt, .. } = &command else { unreachable!() };
    let command_path = f.state.join("commands").join(format!("{}.json", component(command_id)));
    let mut receipt = read_receipt(&command_path).unwrap();
    receipt.completed = true;
    atomic_json(&command_path, &receipt).unwrap();
    let phase_path = publication::path(&f.state, attempt);
    let before = [path.clone(), command_path, phase_path].map(|p| (p.clone(), std::fs::read(p).unwrap()));
    assert!(super::read(&path, f.client.limits()).err().unwrap().contains("unsupported worker mutation journal"));
    let sent = f.sends.lock().unwrap().len();
    super::recover_settlements(&f.client, &f.state, &f.controller_url, "worker").await.unwrap();
    assert_eq!(f.sends.lock().unwrap().len(), sent, "consumed history causes no mutation send");
    for (path, bytes) in before { assert_eq!(std::fs::read(path).unwrap(), bytes); }
}

#[tokio::test]
async fn mutation_recovery_lost_ack_uses_original_command_policy() {
    let f = Fixture::new("recovery-lost-ack-policy").await;
    let (_, request, path, limits) = accepted_recovery_settlement(&f, true).await;
    let original_client = ApiClient::new(&f.controller_url, limits.clone()).unwrap();
    let id = identity(&request).unwrap().to_owned();
    let mut journal = super::create(&path, request.clone(), 0, &limits).unwrap();
    *f.drop_id.lock().unwrap() = Some(id.clone());
    assert!(resolve(&original_client, &path, &mut journal).await.unwrap().is_none());
    assert!(!journal.consumed && journal.outcome.is_none());
    super::recover_settlements(&f.client, &f.state, &f.controller_url, "worker").await.unwrap();
    let recovered = super::read(&path, &limits).unwrap().unwrap();
    assert!(recovered.consumed);
    assert_eq!(serde_json::to_value(&recovered.request).unwrap(), serde_json::to_value(&request).unwrap());
    assert_eq!(recovered.limits, *limits);
    assert!(matches!(acceptance::lookup(&original_client, &id, &request).await.unwrap(),
        acceptance::Lookup::Recorded(ref outcome, ref context)
        if recovered.outcome.as_ref() == Some(outcome) && recovered.outcome_context.as_ref() == Some(context)));
    assert_eq!(f.sends.lock().unwrap().iter().filter(|op| identity(op).ok() == Some(id.as_str())).count(), 1,
        "startup must look up the original committed settlement, never resend it");
}

#[tokio::test]
async fn mutation_recovery_retains_generic_archived_decisions() {
    let f = Fixture::new("recovery-generic-archive").await;
    let (_, request, path, limits) = accepted_recovery_settlement(&f, false).await;
    let original_client = ApiClient::new(&f.controller_url, limits.clone()).unwrap();
    let mut journal = super::create(&path, request, 0, &limits).unwrap();
    assert!(matches!(resolve(&original_client, &path, &mut journal).await.unwrap(), Some(Outcome::Ok { .. })));
    consume(&path, &mut journal).unwrap();
    let archive = f.state.join("mutations/settlements").join(format!("settled-{}.json", component(identity(&journal.request).unwrap())));
    let bytes = std::fs::read(&path).unwrap();
    std::fs::write(&archive, &bytes).unwrap();
    let sent = f.sends.lock().unwrap().len();
    super::recover_settlements(&f.client, &f.state, &f.controller_url, "worker").await.unwrap();
    assert_eq!(std::fs::read(path).unwrap(), bytes);
    assert_eq!(std::fs::read(archive).unwrap(), bytes);
    assert_eq!(f.sends.lock().unwrap().len(), sent);
}

#[tokio::test]
async fn mutation_recovery_refuses_unbound_changed_or_corrupt_originals() {
    let f = Fixture::new("recovery-original-refusals").await;
    let (command, request, path, limits) = accepted_recovery_settlement(&f, true).await;
    let original_client = ApiClient::new(&f.controller_url, limits.clone()).unwrap();
    let mut journal = super::create(&path, request, 0, &limits).unwrap();
    assert!(matches!(resolve(&original_client, &path, &mut journal).await.unwrap(), Some(Outcome::Ok { .. })));
    consume(&path, &mut journal).unwrap();
    let WorkerCommand::RunAttempt { command_id, attempt, .. } = &command else { unreachable!() };
    let command_path = f.state.join("commands").join(format!("{}.json", component(command_id)));
    let phase_path = publication::path(&f.state, attempt);
    let original = std::fs::read(&path).unwrap();
    let command_bytes = std::fs::read(&command_path).unwrap();
    let phase_bytes = std::fs::read(&phase_path).unwrap();
    let sent = f.sends.lock().unwrap().len();
    for case in ["task", "attempt", "generation", "operation", "settlement", "checkpoint", "no-checkpoint-abort",
        "policy", "format", "context", "outcome", "corrupt", "missing-command", "unbound-command",
        "wrong-worker", "wrong-incarnation", "duplicate-command", "missing-publication", "wrong-path"] {
        let mut value: serde_json::Value = serde_json::from_slice(&original).unwrap();
        let mut receipt: Receipt = serde_json::from_slice(&command_bytes).unwrap();
        let wrong_path = f.state.join("mutations/settlements/wrong.json");
        match case {
            "task" => value["request"]["input"]["task_id"] = json!("other-task"),
            "attempt" => value["request"]["input"]["attempt_id"] = json!("other-attempt"),
            "generation" => value["request"]["input"]["generation"] = json!(attempt.generation + 1),
            "operation" => value["request"]["input"]["operation_id"] = json!("other-operation"),
            "settlement" => value["request"]["input"]["settlement_id"] = json!("other-settlement"),
            "checkpoint" => value["request"]["input"]["checkpoint"]["commit"] = json!("c".repeat(40)),
            "no-checkpoint-abort" => {
                value["request"]["input"]["checkpoint"] = serde_json::Value::Null;
                value["request"]["input"]["outcome"] = json!({"acknowledged":true,"not_applied":true});
                std::fs::remove_file(&phase_path).unwrap();
            }
            "policy" => value["limits"] = serde_json::to_value(f.client.limits()).unwrap(),
            "format" => value["revision"] = json!(2),
            "context" => value["outcome_context"]["commit"]["operation_id"] = json!("wrong-receipt"),
            "outcome" => value["outcome"] = serde_json::Value::Null,
            "missing-command" => { std::fs::remove_file(&command_path).unwrap(); }
            "unbound-command" | "wrong-worker" | "wrong-incarnation" | "duplicate-command" => {
                let WorkerCommand::RunAttempt { command_id, attempt, task, .. } = &mut receipt.command else { unreachable!() };
                match case {
                    "unbound-command" => task.resolved_limits = None,
                    "wrong-worker" => attempt.worker_id = "other-worker".into(),
                    "wrong-incarnation" => attempt.worker_incarnation = "other-incarnation".into(),
                    "duplicate-command" => *command_id = "duplicate".into(),
                    _ => unreachable!(),
                }
                if case == "duplicate-command" {
                    let duplicate = f.state.join("commands").join(format!("{}.json", component(command_id)));
                    atomic_json(&duplicate, &receipt).unwrap();
                } else { atomic_json(&command_path, &receipt).unwrap(); }
            }
            "missing-publication" => { std::fs::remove_file(&phase_path).unwrap(); }
            "corrupt" | "wrong-path" => {}
            _ => unreachable!(),
        }
        let bytes = if case == "corrupt" { b"{not-json".to_vec() } else { serde_json::to_vec(&value).unwrap() };
        std::fs::write(&path, &bytes).unwrap();
        if case == "wrong-path" { std::fs::rename(&path, &wrong_path).unwrap(); }
        assert!(super::recover_settlements(&f.client, &f.state, &f.controller_url, "worker").await.is_err(), "{case}");
        assert_eq!(f.sends.lock().unwrap().len(), sent, "{case}: invalid history must not send");
        let observed = if case == "wrong-path" { &wrong_path } else { &path };
        assert_eq!(std::fs::read(observed).unwrap(), bytes, "{case}: invalid history must remain unchanged");
        if wrong_path.exists() { std::fs::remove_file(&wrong_path).unwrap(); }
        // Restore only this test's disposable inputs between independent cases.
        std::fs::write(&path, &original).unwrap();
        std::fs::write(&command_path, &command_bytes).unwrap();
        std::fs::write(&phase_path, &phase_bytes).unwrap();
        let encoded_duplicate = f.state.join("commands").join(format!("{}.json", component("duplicate")));
        if encoded_duplicate.exists() { std::fs::remove_file(encoded_duplicate).unwrap(); }
    }
}

#[path = "mutations_retained_envelope_tests.rs"]
mod retained_envelopes;
