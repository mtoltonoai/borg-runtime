//! Real worker admission, Controller receipts and the existing controlled stdio
//! peer joined at the initial supplied-context boundary. No provider, installed
//! Codex, repository acquisition, task-result or inner-reply claim.
use super::*;
use crate::client::ApiClient;
use crate::controller::{Controller, ControllerConfig};
use crate::protocol::{
    AttemptPhase, Definition, ExecutorKind, Limits, Observation, Outcome, Repository, Task,
    TaskInput, WIRE_REVISION, Worker, WorkerCommand,
};
use crate::worker::{Reporter, WorkerConfig};

struct AdmissionFixture {
    root: PathBuf,
    state: PathBuf,
    binary: PathBuf,
    runtime: Arc<RuntimeLimits>,
    client: ApiClient,
    scope: InitialCharterScope,
    command: WorkerCommand,
    reporter: Reporter,
    trace: Arc<Mutex<Vec<(Operation, Reply)>>>,
    server: tokio::task::JoinHandle<()>,
    _controller: Controller,
    _lease: process::WorkerLease,
}

fn as_task(reply: Reply) -> Task {
    match reply.outcome {
        Outcome::Ok {
            data: Data::Task(task),
        } => *task,
        other => panic!("{other:?}"),
    }
}
fn as_delivery(reply: &Reply) -> CharterSuppliedContext {
    match &reply.outcome {
        Outcome::Ok {
            data: Data::Charter(value),
        } => match value.as_ref() {
            CharterOutcome::SuppliedContext(value) => value.clone(),
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    }
}
fn refused(reply: &Reply, expected: &str) {
    assert!(
        matches!(&reply.outcome, Outcome::Refused {code,..} if code == expected),
        "{reply:?}"
    );
}
fn peer_requests(root: &Path) -> Vec<Value> {
    match std::fs::read_to_string(root.join("requests.jsonl")) {
        Ok(raw) => raw
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => panic!("{error}"),
    }
}

impl AdmissionFixture {
    async fn new(label: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let parent = std::env::var_os("BORG_TEST_ARTIFACT")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let root = parent.join(format!(
            "charter-admission-{label}-{}-{}-{}",
            std::process::id(),
            process::now_ms(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        process::private_directory(&root).unwrap();
        let state = root.join("state");
        let mut runtime = (*crate::worker::test_limits()).clone();
        runtime.charter = Some(CharterLimits {
            record_bytes: 262144,
            source_bytes: 65536,
            files: 8,
            clauses: 32,
            metadata_bytes: 16384,
            references: 32,
            reply_items: 32,
            tool_calls: 8,
            reference_depth: 8,
            records: 1024,
            page_items: 8,
            page_bytes: 65536,
        });
        runtime.controller.command_wait_ms = 20;
        runtime.validate().unwrap();
        let runtime = Arc::new(runtime);
        crate::worker::handoff::initialize_state(&state,&runtime.worker).unwrap();
        for name in ["commands", "acks", "outbox", "processes", "mutations"] {
            process::private_directory(&state.join(name)).unwrap();
        }

        let controller = Controller::open_with_config(
            root.join("tasks.sqlite"),
            ControllerConfig {
                limits: runtime.clone(),
                ..ControllerConfig::default()
            },
        )
        .unwrap();
        let trace = Arc::new(Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let controller_url = format!("http://{}", listener.local_addr().unwrap());
        let owner = controller.clone();
        let saved = trace.clone();
        let files = root.clone();
        let server = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let owner = owner.clone();
                let saved = saved.clone();
                let files = files.clone();
                connections.spawn(async move {
                    let service = service_fn(move |request: HttpRequest<Incoming>| {
                        let owner = owner.clone();
                        let saved = saved.clone();
                        let files = files.clone();
                        async move {
                            assert_eq!(request.uri().path(), "/api/operations");
                            let request: Request = serde_json::from_slice(
                                &request
                                    .into_body()
                                    .collect()
                                    .await
                                    .map_err(std::io::Error::other)?
                                    .to_bytes(),
                            )
                            .unwrap();
                            let command = request.command.clone();
                            let reply = Box::pin(owner.handle(request)).await;
                            reply.validate_for(&command).unwrap();
                            if matches!(
                                &command,
                                Operation::ObserveCharter {
                                    observation: CharterObservation::BeginSuppliedContext { .. },
                                    ..
                                }
                            ) && !files.join("intent-ack").exists()
                            {
                                // Initialization/preflight may already have run; neither
                                // thread/start nor turn/start precedes the canonical intent.
                                assert!(
                                    peer_requests(&files)
                                        .iter()
                                        .all(|r| r["method"] != "thread/start"
                                            && r["method"] != "turn/start")
                                );
                                if matches!(&reply.outcome, Outcome::Ok { .. }) {
                                    assert!(reply.commit.is_some());
                                    std::fs::write(
                                        files.join("intent-ack"),
                                        b"actual Controller intent",
                                    )
                                    .unwrap();
                                }
                            }
                            if matches!(
                                &command,
                                Operation::ObserveCharter {
                                    observation: CharterObservation::AckSuppliedContext { .. },
                                    ..
                                }
                            ) && matches!(&reply.outcome, Outcome::Ok { .. })
                            {
                                assert!(reply.commit.is_some());
                                std::fs::write(
                                    files.join("context-ack"),
                                    b"actual Controller acknowledgment",
                                )
                                .unwrap();
                            }
                            let drop_ack=files.join("drop-outer-ack").exists()&&matches!(&command,
                                Operation::ObserveCharter{observation:CharterObservation::AckSuppliedTurn{..},..})
                                &&matches!(&reply.outcome,Outcome::Ok{..});
                            saved.lock().unwrap().push((command, reply.clone()));
                            if drop_ack {
                                std::fs::remove_file(files.join("drop-outer-ack")).unwrap();
                                return Err(std::io::Error::new(std::io::ErrorKind::ConnectionReset,"controlled committed ACK response loss"));
                            }
                            Ok::<_, std::io::Error>(HttpResponse::new(Full::new(Bytes::from(
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
        let client = ApiClient::new(&controller_url, runtime.clone()).unwrap();
        let registration = crate::repository_catalogue::Revision {
            repository_id: "admission-repository".into(),
            registration_revision: "one".into(),
        };
        let registered = client
            .call(Operation::RegisterRepository {
                operation_id: "admission-repository".into(),
                registration: crate::repository_catalogue::Registration {
                    repository_id: registration.repository_id.clone(),
                    registration_revision: registration.registration_revision.clone(),
                    display_name: "Controlled admission source".into(),
                    source_endpoint: "/unused/local.git".into(),
                    checkpoint_endpoint: "/unused/local.git".into(),
                    source_store_key: "local.git".into(),
                    checkpoint_store_key: "local.git".into(),
                    pull_request_target: None,
                },
            })
            .await
            .unwrap();
        assert!(matches!(
            registered.outcome,
            Outcome::Ok {
                data: Data::RepositoryRegistration(_)
            }
        ));
        let input = TaskInput {
            definition: Definition {
                id: format!("admission-{label}"),
                revision: "one".into(),
                executor: ExecutorKind::Coding,
                inference: Some(InferenceSettings::selected()),
                execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
                limit_overrides: None,
                charter: Some(DefinitionCharter::Inline {
                    source: source("original").source,
                }),
            },
            instruction: "Original task only.".into(),
            repository: Some(Repository {
                registration: Some(registration),
                url: String::new(),
                checkpoint_remote: String::new(),
                start_commit: "a".repeat(40),
                pull_request_target: None,
            }),
            repository_read: None,
            instruction_source: None,
            pull_request: None,
            resolved_limits: None,
            acceptance_commands: vec![vec!["/usr/bin/true".into()]],
            output_schema: None,
            dependencies: vec![],
            native_workflow: None,
            limits: Limits {
                deadline_unix_ms: process::now_ms() + 60_000,
                model_requests: 2,
                output_bytes: 4096,
                result_bytes: runtime.task_defaults.result_bytes,
                max_delegation_depth: 0,
            },
        };
        let task = as_task(
            client
                .call(Operation::SubmitTask {
                    operation_id: "admission-submit".into(),
                    input: (input).into(),
                })
                .await
                .unwrap(),
        );
        let worker = Worker {
            id: "admission-worker".into(),
            incarnation: "one".into(),
            coding_capacity: 1,
            native_capacity: 0,
        };
        let registered = client
            .call(Operation::RegisterWorker {
                operation_id: "admission-worker-register".into(),
                worker: worker.clone(),
            })
            .await
            .unwrap();
        assert!(matches!(registered.outcome, Outcome::Ok { .. }));
        let command = match client
            .call(Operation::AwaitCommands {
                operation_id: "admission-await".into(),
                worker_id: worker.id.clone(),
                incarnation: worker.incarnation.clone(),
                acknowledged: vec![],
            })
            .await
            .unwrap()
            .outcome
        {
            Outcome::Ok {
                data: Data::Commands(commands),
            } => commands
                .into_iter()
                .find(|c| matches!(c, WorkerCommand::RunAttempt { .. }))
                .unwrap(),
            other => panic!("{other:?}"),
        };
        let WorkerCommand::RunAttempt { attempt, .. } = &command else {
            panic!()
        };
        assert_eq!(attempt.phase, AttemptPhase::Assigned);
        let scope = InitialCharterScope {
            worker_state: state.clone(),
            controller_url: controller_url.clone(),
            session_id: task.session_id,
            caller: crate::mcp::Caller {
                task_id: task.id,
                attempt_id: attempt.id.clone(),
                generation: attempt.generation,
                worker_id: attempt.worker_id.clone(),
                incarnation: attempt.worker_incarnation.clone(),
            },
        };
        let binary = root.join("executor.py");
        std::fs::write(&binary, EXECUTOR).unwrap();
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::write(root.join("mode"), "success").unwrap();
        let reporter = Reporter {
            client: client.clone(),
            resource: None,
            config: WorkerConfig {
                limits: runtime.clone(),
                resource_profile: None,
                controller_url,
                id: worker.id,
                incarnation: worker.incarnation,
                workspace_root: root.clone(),
                codex_path: binary.clone(),
                coding_capacity: 1,
                native_capacity: 0,
                native_endpoint: "http://127.0.0.1:1".into(),
                validation_executable: std::env::current_exe().unwrap(),
            },
            attempt: attempt.clone(),
            outbox: state.join("outbox"),
            sequence: Arc::new(AtomicU64::new(1)),
            executor_seen: Arc::new(AtomicBool::new(false)),
            provider_uncertain: Arc::new(AtomicBool::new(false)),
            uncertainty_retained: Arc::new(AtomicBool::new(false)),
            output_limit: 4096,
            result_limit: runtime.task_defaults.result_bytes,
            coding_limits: runtime.execution.coding.clone(),
            outbox_lock: Arc::new(tokio::sync::Mutex::new(())),
        };
        let lease =
            process::WorkerLease::claim(&state.join("worker-owner.json"), &runtime.worker).unwrap();
        Self {
            root,
            state,
            binary,
            runtime,
            client,
            scope,
            command,
            reporter,
            trace,
            server,
            _controller: controller,
            _lease: lease,
        }
    }
    async fn call(&self, command: Operation) -> Reply {
        self.client.call(command).await.unwrap()
    }
    async fn task(&self) -> Task {
        as_task(
            self.call(Operation::GetTask {
                task_id: self.scope.caller.task_id.clone(),
            })
            .await,
        )
    }
    async fn charter(&self) -> EffectiveCharter {
        match self
            .call(Operation::QueryCharter {
                query: CharterQuery::Session {
                    session_id: self.scope.session_id.clone(),
                },
            })
            .await
            .outcome
        {
            Outcome::Ok {
                data: Data::CharterQuery(value),
            } => match *value {
                CharterQueryResult::Session(Some(value)) => value,
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        }
    }
    async fn intent(&self, id: &str) -> Operation {
        let binding = self.charter().await;
        Operation::ObserveCharter {
            operation_id: id.into(),
            caller: self.scope.caller.clone(),
            observation: CharterObservation::BeginSuppliedContext {
                session_id: self.scope.session_id.clone(),
                expected_revision: binding.session.revision,
                source: binding.charter.reference.clone(),
                request_id: 2,
                developer_instructions: supplied_context_instructions(&binding.charter).unwrap(),
            },
        }
    }
    fn control(&self) -> (watch::Sender<bool>, Control) {
        let (tx, cancel) = watch::channel(false);
        let worker_limits: std::sync::Arc<crate::limits::WorkerLimits> = Arc::from(self.runtime.worker.clone());
        (
            tx,
            Control {
                deadline_ms: self.reporter.attempt.deadline_unix_ms,
                cancel,
                worker_limits: worker_limits.clone(),
                runtime_limits: self.runtime.clone(),
                validation_runtime: Arc::new(
                    crate::native::ValidationRuntime::new(worker_limits).unwrap(),
                ),
            },
        )
    }
    async fn admit(&self, control: &Control) {
        assert!(
            peer_requests(&self.root).is_empty(),
            "no executor process before launch admission"
        );
        assert!(
            std::fs::read_dir(self.state.join("processes"))
                .unwrap()
                .next()
                .is_none()
        );
        let WorkerCommand::RunAttempt { command_id, .. } = &self.command else {
            panic!()
        };
        let path = self
            .state
            .join("commands")
            .join(format!("{}.json", process::component(command_id)));
        assert!(crate::worker::record_launch(&path, &self.command, &self.runtime).unwrap());
        assert!(
            Box::pin(crate::worker::admit_launch(
                &self.reporter,
                &self.command,
                control,
                &path
            ))
            .await
            .unwrap()
        );
        let current = self.task().await;
        assert_eq!(
            current.attempt.as_ref().unwrap().phase,
            AttemptPhase::Running
        );
        assert!(!current.attempt.as_ref().unwrap().stopped);
        assert!(!current.attempt.as_ref().unwrap().uncertain);
        let receipt = crate::worker::read_receipt(&path, &self.runtime).unwrap();
        assert_eq!(receipt.command, self.command);
        assert!(receipt.effects_started);
        assert!(!receipt.completed);
        assert!(peer_requests(&self.root).is_empty());
        let (operation, reply) =
            self.original(&format!("{}-initial-start", self.scope.caller.attempt_id));
        assert!(
            matches!(operation,Operation::ObserveWorker {report} if matches!(report.observation,
            Observation::Started {thread_id:None,turn_id:None}))
        );
        assert!(matches!(
            reply.outcome,
            Outcome::Ok {
                data: Data::Task(_)
            }
        ));
        assert!(reply.commit.is_some());
    }
    fn original(&self, id: &str) -> (Operation, Reply) {
        self.trace
            .lock()
            .unwrap()
            .iter()
            .find(|(op, _)| op.mutation_id() == Some(id))
            .unwrap()
            .clone()
    }
    async fn replay(&self, operation: Operation, original: &Reply) {
        let replay = self.call(operation.clone()).await;
        assert_eq!(replay.outcome, original.outcome);
        assert_eq!(replay.commit, original.commit);
        let lookup = self
            .call(Operation::GetOperation {
                operation_id: operation.mutation_id().unwrap().into(),
            })
            .await;
        assert_eq!(lookup.commit, original.commit);
        let Outcome::Ok {
            data: Data::Operation(record),
        } = lookup.outcome
        else {
            panic!("{lookup:?}")
        };
        assert_eq!(record.intended, serde_json::to_value(operation).unwrap());
        assert_eq!(
            record.outcome,
            Some(serde_json::to_value(&original.outcome).unwrap())
        );
    }
    fn evidence(&self) {
        process::atomic_json(&self.root.join("actual-admission-trace.json"), &json!({
            "command": self.command, "operations_and_replies": self.trace.lock().unwrap().clone(),
            "stdio_requests": peer_requests(&self.root),
        })).unwrap();
    }
    fn closed(&self) {
        assert!(
            std::fs::read_dir(self.state.join("processes"))
                .unwrap()
                .all(|entry| entry
                    .unwrap()
                    .path()
                    .extension()
                    .is_none_or(|extension| extension != "json"))
        );
    }
}
impl Drop for AdmissionFixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

//= spec/impl/v2/implementation.md#definitions-carry-reusable-charters
//= type=test
//# The supplied executor MUST deliver the effective charter to its model.
// Scope: initial outer context only, using a controlled stdio peer.
#[tokio::test]
async fn real_launch_admission_precedes_supplied_begin_and_exact_executor_association() {
    let f = AdmissionFixture::new("initial").await;
    let (_cancel, control) = f.control();
    let accepted = f.task().await.input;
    f.admit(&control).await;
    let (events, mut received) = mpsc::channel(16);
    // Construct the large nested fixture future in the existing isolated
    // constructor frame; retain the ordinary test thread stack and execution.
    let processes = f.state.join("processes");
    let inference = InferenceSettings::selected();
    let result = crate::worker::boxed_execution(|| {
        execute_with_credentials_collaboration_and_charter(
            &f.binary,
            &f.root,
            &f.state,
            &processes,
            &inference,
            &f.runtime.execution,
            "Original task only.",
            &control,
            events,
            &[],
            None,
            None,
            None,
            Some(&f.scope),
        )
    })
    .await
    .unwrap();
    assert!(result.success);
    assert_eq!(result.thread_id, THREAD);
    assert_eq!(result.turn_id, "supplied-turn");
    f.closed();
    let requests = peer_requests(&f.root);
    assert_eq!(
        requests
            .iter()
            .filter(|r| r["method"] == "thread/start")
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|r| r["method"] == "turn/start")
            .count(),
        1
    );
    let begin_id = format!("{}-charter-supplied-initial", f.scope.caller.attempt_id);
    let ack_id = format!("{}-charter-supplied-ack", f.scope.caller.attempt_id);
    let (begin, original) = f.original(&begin_id);
    let (ack, acknowledged) = f.original(&ack_id);
    let begun = as_delivery(&original);
    let complete = as_delivery(&acknowledged);
    assert_eq!(begun.caller, f.scope.caller);
    assert_eq!(begun.session_id, f.scope.session_id);
    assert_eq!(begun.effective, f.charter().await.charter);
    assert_eq!(
        requests
            .iter()
            .find(|r| r["method"] == "thread/start")
            .unwrap()["params"]["developerInstructions"],
        begun.developer_instructions
    );
    assert_eq!(
        complete.acknowledgment.as_ref().unwrap().response,
        serde_json::from_slice::<Value>(&std::fs::read(f.root.join("response.json")).unwrap())
            .unwrap()
    );
    assert_eq!(complete.acknowledgment.as_ref().unwrap().thread_id, THREAD);
    let admission = f
        .original(&format!("{}-initial-start", f.scope.caller.attempt_id))
        .1;
    assert!(
        admission.commit.unwrap().controller_revision
            < original.commit.as_ref().unwrap().controller_revision
    );
    assert!(
        original.commit.as_ref().unwrap().controller_revision
            < acknowledged.commit.as_ref().unwrap().controller_revision
    );
    // Before any actual association report, a different Begin is still refused.
    let repeat = f.intent("another-initial-context").await;
    refused(&f.call(repeat).await, "reconciliation_required");
    f.replay(begin.clone(), &original).await;
    f.replay(ack.clone(), &acknowledged).await;
    let mut started = 0;
    while let Ok(event) = received.try_recv() {
        match event {
            Event::Started { thread_id, turn_id } => {
                assert_eq!(thread_id, THREAD);
                assert_eq!(turn_id, "supplied-turn");
                started += 1;
                assert!(
                    f.reporter
                        .send_identified(
                            "actual-stdio-association".into(),
                            Observation::Started {
                                thread_id: Some(thread_id),
                                turn_id: Some(turn_id)
                            }
                        )
                        .await
                        .unwrap()
                );
            }
            Event::Progress(_) => {}
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(started, 1);
    let associated = f.original("actual-stdio-association").1;
    assert!(
        associated.commit.unwrap().controller_revision
            > acknowledged.commit.as_ref().unwrap().controller_revision
    );
    let after_association = f.intent("after-actual-association").await;
    refused(&f.call(after_association).await, "invalid_transition");
    f.replay(begin, &original).await;
    f.replay(ack, &acknowledged).await;
    assert_eq!(
        peer_requests(&f.root),
        requests,
        "reconciliation cannot launch another executor request"
    );
    f.evidence();
    assert_eq!(f.task().await.input, accepted);
    assert!(f.task().await.result.is_none());
    assert!(
        matches!(f.call(Operation::QueryCharter {query:CharterQuery::Turns {
        session_id:f.scope.session_id.clone(),after:None,limit:4}}).await.outcome,
        Outcome::Ok {data:Data::CharterQuery(value)} if matches!(value.as_ref(), CharterQueryResult::Turns(page) if page.items.is_empty()))
    );
}

// Stale, cancelled, completed or already-associated supplied contexts cannot
// reach the peer or change charter state. This checks initial-boundary
// refusal, not later running-executor charter adoption.
//= spec/impl/v2/implementation.md#charter-changes-apply-at-safe-boundaries
//= type=test
//# A charter change MUST reach the supplied executor only at a supported step boundary.
#[tokio::test]
async fn admitted_supplied_begin_refuses_stale_cancelled_and_each_actual_association_shape() {
    for mode in [
        "stale-owner",
        "cancelled",
        "executor-failed",
        "executor-succeeded",
        "thread-only",
        "turn-only",
        "thread-and-turn",
    ] {
        let f = AdmissionFixture::new(mode).await;
        let (_cancel, control) = f.control();
        f.admit(&control).await;
        let before = f.charter().await;
        if mode == "cancelled" {
            let current = f.task().await;
            as_task(
                f.call(Operation::CancelTask {
                    operation_id: "cancel-before-supplied".into(),
                    task_id: current.id,
                    expected_revision: current.revision,
                })
                .await,
            );
        } else if mode == "executor-failed" || mode == "executor-succeeded" {
            assert!(
                f.reporter
                    .send_identified(
                        "preflight-outcome".into(),
                        Observation::ExecutorOutcome {
                            success: mode == "executor-succeeded",
                            summary: "Controlled executor outcome before thread/start".into(),
                        }
                    )
                    .await
                    .unwrap()
            );
        } else if mode != "stale-owner" {
            assert!(
                f.reporter
                    .send_identified(
                        "prior-actual-association".into(),
                        Observation::Started {
                            thread_id: (mode != "turn-only").then(|| "prior-actual-thread".into()),
                            turn_id: (mode != "thread-only").then(|| "prior-actual-turn".into()),
                        }
                    )
                    .await
                    .unwrap()
            );
        }
        let retained = f.task().await;
        let mut operation = f.intent("refused-initial-context").await;
        if mode == "stale-owner" {
            let Operation::ObserveCharter { caller, .. } = &mut operation else {
                panic!()
            };
            caller.incarnation = "foreign".into();
        }
        let original = f.call(operation.clone()).await;
        refused(
            &original,
            if mode == "stale-owner" {
                "stale_owner"
            } else {
                "invalid_transition"
            },
        );
        f.replay(operation, &original).await;
        assert_eq!(f.charter().await, before);
        assert_eq!(f.task().await, retained);
        refused(
            &f.call(Operation::QueryCharter {
                query: CharterQuery::SuppliedContext {
                    delivery_id: "refused-initial-context".into(),
                },
            })
            .await,
            "not_found",
        );
        f.evidence();
        assert!(peer_requests(&f.root).is_empty());
        assert!(!f.state.join("supplied-charter").exists());
        f.closed();
    }
}

#[path="codex_charter_outer_admission_tests.rs"]
mod outer;
