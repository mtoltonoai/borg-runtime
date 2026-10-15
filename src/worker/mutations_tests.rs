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

struct Fixture {
    state: PathBuf,
    client: ApiClient,
    _controller: Controller,
    _lease: process::WorkerLease,
    drop_id: Arc<Mutex<Option<String>>>,
    deny_report: Arc<Mutex<Option<String>>>,
    sends: Arc<Mutex<Vec<Operation>>>,
    after_commit: Arc<Mutex<Option<Box<dyn FnOnce() + Send>>>>,
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
        let mut controller_limits = (*crate::worker::test_limits()).clone();
        controller_limits.controller.command_wait_ms = 20;
        // One setup receipt registers the repository. Keep exactly the original
        // available normal-pool slots for the launch/cleanup saturation oracle.
        controller_limits.controller.max_receipts = max_receipts.checked_add(1).unwrap();
        let controller = Controller::open_with_config(
            state.join("tasks.sqlite"),
            ControllerConfig {
                limits: Arc::new(controller_limits),
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
        let (owner, drop_response, recorded) = (controller.clone(), drop_id.clone(), sends.clone());
        let server = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let (owner, drop_response, recorded) =
                    (owner.clone(), drop_response.clone(), recorded.clone());
                let denied = denied.clone();
                let commit_cut = commit_cut.clone();
                connections.spawn(async move {
                    let service = service_fn(move |request: HttpRequest<Incoming>| {
                        let (owner, drop_response, recorded) =
                            (owner.clone(), drop_response.clone(), recorded.clone());
                        let denied = denied.clone();
                        let commit_cut = commit_cut.clone();
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
                            let id = identity(&request.command).ok().map(str::to_owned);
                            if id.is_some() {
                                recorded.lock().unwrap().push(request.command.clone());
                            }
                            let reply = owner.handle(request).await;
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
            &crate::worker::test_limits().worker,
        )
        .unwrap();
        Self {
            state,
            client: ApiClient::new(&url, crate::worker::test_limits()).unwrap(),
            _controller: controller,
            _lease: lease,
            drop_id,
            deny_report,
            sends,
            after_commit,
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
    TaskInput {
        resolved_limits: None,
        repository_read: None,
        instruction: "controlled local launch boundary".into(),
        definition: Definition {
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
    assert!(matches!(
        f.call(Operation::PrepareOperation {
            operation_id: "external".into(),
            task_id: attempt.task_id.clone(),
            attempt_id: attempt.id.clone(),
            generation: attempt.generation,
            kind: "pull_request".into(),
            intended: json!({"fixture":"no external action performed"})
        })
        .await,
        Outcome::Ok { .. }
    ));
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
