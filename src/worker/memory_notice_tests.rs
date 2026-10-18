use super::*;
use crate::collaboration::AgentMemoryNotice;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(std::env::temp_dir().join(format!(
            "borg-memory-notice-{}-{}-{}",
            std::process::id(),
            process::now_ms(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        )))
    }
    fn host(&self, caller: &Caller, credentials: Vec<String>) -> Host {
        Host::open_with_credentials(
            ApiClient::new("http://127.0.0.1:1", crate::worker::test_limits()).unwrap(),
            &self.0,
            caller,
            "reader-session",
            "shared-agent",
            &crate::worker::test_limits().execution.memory_notice,
            credentials,
        )
        .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn caller() -> Caller {
    Caller {
        task_id: "reader-task".into(),
        attempt_id: "attempt-1".into(),
        generation: 1,
        worker_id: "worker-1".into(),
        incarnation: "incarnation-1".into(),
    }
}
fn cursor(sequence: u64) -> AgentMemoryCursor {
    AgentMemoryCursor {
        database_id: "database-1".into(),
        agent_id: "shared-agent".into(),
        session_id: "reader-session".into(),
        sequence,
    }
}
fn page() -> AgentMemoryPage {
    AgentMemoryPage {
        from_cursor: cursor(10),
        to_cursor: cursor(15),
        through_sequence: 20,
        notices: vec![AgentMemoryNotice {
            key: "decision".into(),
            revision: 3,
        }],
        has_more: true,
    }
}
fn observation() -> crate::collaboration::AgentMemoryObservation {
    crate::collaboration::AgentMemoryObservation {
        start_sequence: 10,
        acknowledged_cursor: cursor(10),
        pending: Some(AgentMemoryBatch {
            batch_id: "batch-1".into(),
            page: page(),
        }),
        available: None,
    }
}

#[test]
fn historical_pending_mirror_decodes_without_rewriting_or_resuming_delivery() {
    let f = Fixture::new();
    let host = f.host(&caller(), vec![]);
    let batch = observation().pending.unwrap();
    let mirror = Mirror {
        limits: host.limits.clone(), revision: 1, start_sequence: 10,
        acknowledged_cursor: cursor(10),
        pending: Some(Pending { text:host.encode(&batch).unwrap(), batch,
            delivery:Some(Delivery { caller:caller(), context:"old-context".into(), appended:false }) }),
    };
    let path=host.directory.join("delivery.json");
    process::private_directory(&host.directory).unwrap();
    atomic_json(&path,&mirror).unwrap();
    let before=std::fs::read(&path).unwrap();
    for _ in 0..2 {
        let reopened=f.host(&caller(),vec![]);
        assert!(reopened.mirror.as_ref()==Some(&mirror));
        assert_eq!(std::fs::read(&path).unwrap(),before);
        assert_eq!(std::fs::read_dir(&reopened.directory).unwrap().count(),1);
    }
}

#[test]
fn corrupt_historical_mirror_refuses_without_repair() {
    let f=Fixture::new();
    let host=f.host(&caller(),vec![]);
    let batch=observation().pending.unwrap();
    let original=Mirror { limits:host.limits.clone(),revision:1,start_sequence:10,
        acknowledged_cursor:cursor(10),pending:Some(Pending {text:host.encode(&batch).unwrap(),batch,delivery:None}) };
    let path=host.directory.join("delivery.json");
    process::private_directory(&host.directory).unwrap();
    for mode in ["text","cursor","scope","format"] {
        let mut mirror=original.clone();
        match mode {
            "text"=>mirror.pending.as_mut().unwrap().text="changed".into(),
            "cursor"=>mirror.acknowledged_cursor.sequence=11,
            "scope"=>mirror.pending.as_mut().unwrap().batch.page.to_cursor.session_id="other".into(),
            _=>mirror.revision=2,
        }
        atomic_json(&path,&mirror).unwrap();let before=std::fs::read(&path).unwrap();
        assert!(Host::open_with_credentials(host.client.clone(),&f.0,&caller(),"reader-session","shared-agent",&host.limits,vec![]).is_err());
        assert_eq!(std::fs::read(&path).unwrap(),before);
    }
}

#[test]
fn private_record_reads_refuse_symlinks_fifos_nonprivate_and_oversized_files() {
    let f = Fixture::new();
    process::private_directory(&f.0).unwrap();
    let file = f.0.join("record.json");
    atomic_json(&file, &json!({"value":"safe"})).unwrap();
    assert!(
        read_private(&file, 1024, "execution.memory_notice.mirror_bytes")
            .unwrap()
            .is_some()
    );
    assert_eq!(
        read_private(&file, 1, "execution.memory_notice.mirror_bytes").unwrap_err(),
        "memory notification record exceeds bound (execution.memory_notice.mirror_bytes)"
    );
    let link = f.0.join("link.json");
    std::os::unix::fs::symlink(&file, &link).unwrap();
    assert!(read_private(&link, 1024, "execution.memory_notice.mirror_bytes").is_err());
    let fifo = f.0.join("fifo.json");
    let mut child = std::process::Command::new("/usr/bin/mkfifo")
        .args(["-m", "600", "--"])
        .arg(&fifo)
        .env_clear()
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("local FIFO fixture creation exceeded deadline");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(read_private(&fifo, 1024, "execution.memory_notice.mirror_bytes").is_err());
    assert!(read_private(&f.0, 1024, "execution.memory_notice.mirror_bytes").is_err());
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(read_private(&file, 1024, "execution.memory_notice.mirror_bytes").is_err());
}

#[test]
fn long_session_scope_uses_bounded_noncolliding_path_components() {
    let f = Fixture::new();
    let short = "s".repeat(48);
    let long = "s".repeat(256);
    let a = scope_directory(&f.0, &short);
    let b = scope_directory(&f.0, &long);
    assert_ne!(a, b);
    assert!(
        b.strip_prefix(f.0.join("memory-notices"))
            .unwrap()
            .components()
            .all(|p| p.as_os_str().len() <= 96)
    );
}

// Shared real Controller/HTTP fixture for unchanged workflow completion tests.
// It never observes, prepares or acknowledges retired memory notifications.
struct ScanFixture {
    area: Fixture,
    client: ApiClient,
    limits: Arc<crate::limits::RuntimeLimits>,
    reader: Attempt,
    writer: Attempt,
    control: Control,
    _cancel: watch::Sender<bool>,
    drop_ack: Arc<AtomicBool>,
    cancel_ack: Arc<AtomicBool>,
    sends: Arc<std::sync::Mutex<Vec<Operation>>>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for ScanFixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl ScanFixture {
    async fn new(pages: usize) -> Self {
        Self::configured(pages, |_| {}).await
    }
    async fn configured(
        pages: usize,
        configure: impl FnOnce(&mut crate::limits::RuntimeLimits),
    ) -> Self {
        use crate::controller::{Controller, ControllerConfig};
        use bytes::Bytes;
        use http_body_util::{BodyExt, Full};
        use hyper::{Request as HttpRequest, Response, body::Incoming, service::service_fn};
        use hyper_util::rt::TokioIo;
        let area = Fixture::new();
        std::fs::create_dir_all(&area.0).unwrap();
        let mut limits = (*crate::worker::test_limits()).clone();
        limits.revision = "memory-scan-boundary-fixture-v1".into();
        limits.execution.memory_notice.keys = 1;
        limits.execution.memory_notice.empty_prefix_pages = pages;
        limits.controller.memory_notice_scan_min = 1;
        limits.controller.memory_notice_scan_factor = 1;
        limits.controller.memory_notice_scan_max = 1;
        configure(&mut limits);
        limits.validate().unwrap();
        let limits = Arc::new(limits);
        let mut config = ControllerConfig {
            limits: limits.clone(),
            ..ControllerConfig::default()
        };
        // Library tests cannot use their harness binary as the native validator.
        // Existing nonnative callers retain the default executable when unbound.
        if let Some(executable) = std::env::var_os("BORG_TEST_RUNTIME") {
            config.validation_executable = PathBuf::from(executable);
        }
        let controller = Controller::open_with_config(area.0.join("controller.db"), config).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (cancel, rx) = watch::channel(false);
        let drop_ack = Arc::new(AtomicBool::new(false));
        let cancel_ack = Arc::new(AtomicBool::new(false));
        let sends = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (lost, interrupted, recorded, stop) = (
            drop_ack.clone(),
            cancel_ack.clone(),
            sends.clone(),
            cancel.clone(),
        );
        let server = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let (owner, lost, interrupted, recorded, stop) = (
                    controller.clone(),
                    lost.clone(),
                    interrupted.clone(),
                    recorded.clone(),
                    stop.clone(),
                );
                connections.spawn(async move {
                    let service = service_fn(move |request: HttpRequest<Incoming>| {
                        let (owner, lost, interrupted, recorded, stop) = (
                            owner.clone(),
                            lost.clone(),
                            interrupted.clone(),
                            recorded.clone(),
                            stop.clone(),
                        );
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
                            let ack = matches!(
                                &request.command,
                                Operation::AcknowledgeAgentMemory { .. }
                            );
                            recorded.lock().unwrap().push(request.command.clone());
                            let reply = owner.handle(request).await;
                            if ack && matches!(&reply.outcome, Outcome::Ok { .. }) {
                                if interrupted.swap(false, Ordering::SeqCst) {
                                    stop.send(true).unwrap();
                                }
                                if lost.swap(false, Ordering::SeqCst) {
                                    return Err(std::io::Error::other(
                                        "controlled loss after committed memory ACK",
                                    ));
                                }
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
        let client = ApiClient::new(url, limits.clone()).unwrap();
        let registration = crate::repository_catalogue::Registration {
            pull_request_target: None,
            repository_id: "memory-scan-repository".into(),
            registration_revision: "one".into(),
            display_name: "Memory scan fixture".into(),
            source_endpoint: "/unused/local.git".into(),
            checkpoint_endpoint: "/unused/local.git".into(),
            source_store_key: "local.git".into(),
            checkpoint_store_key: "local.git".into(),
        };
        assert!(matches!(
            client
                .call(Operation::RegisterRepository {
                    operation_id: "scan-register-repository".into(),
                    registration,
                })
                .await
                .unwrap()
                .outcome,
            Outcome::Ok { .. }
        ));
        for label in ["writer", "reader"] {
            let input = TaskInput { instruction_source: None,
                pull_request: None,
                resolved_limits: None,
                repository_read: None,
                instruction: "Controlled memory boundary; no executor is launched".into(),
                definition: Definition {
                    charter: None,
                    id: "memory-scan-agent".into(),
                    revision: "one".into(),
                    executor: ExecutorKind::Coding,
                    inference: Some(InferenceSettings::selected()),
                    execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
                    limit_overrides: None,
                },
                repository: Some(Repository {
                    pull_request_target: None,
                    registration: Some(crate::repository_catalogue::Revision {
                        repository_id: "memory-scan-repository".into(),
                        registration_revision: "one".into(),
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
                    result_bytes: limits.task_defaults.result_bytes,
                    max_delegation_depth: 2,
                },
                dependencies: vec![],
                native_workflow: None,
            };
            assert!(matches!(
                client
                    .call(Operation::SubmitTask {
                        operation_id: format!("scan-submit-{label}"),
                        input,
                    })
                    .await
                    .unwrap()
                    .outcome,
                Outcome::Ok {
                    data: Data::Task(_)
                }
            ));
        }
        assert!(matches!(
            client
                .call(Operation::RegisterWorker {
                    operation_id: "scan-register-worker".into(),
                    worker: Worker {
                        id: "scan-worker".into(),
                        incarnation: "one".into(),
                        coding_capacity: 2,
                        native_capacity: 0,
                    },
                })
                .await
                .unwrap()
                .outcome,
            Outcome::Ok { .. }
        ));
        let commands = match client
            .call(Operation::AwaitCommands {
                operation_id: "scan-claim".into(),
                worker_id: "scan-worker".into(),
                incarnation: "one".into(),
                acknowledged: vec![],
            })
            .await
            .unwrap()
            .outcome
        {
            Outcome::Ok {
                data: Data::Commands(commands),
            } => commands,
            other => panic!("scan launch: {other:?}"),
        };
        let mut attempts = Vec::new();
        for command in commands {
            if let WorkerCommand::RunAttempt { attempt, task, .. } = command {
                assert_eq!(
                    task.execution_limits().unwrap().memory_notice,
                    limits.execution.memory_notice
                );
                attempts.push(attempt);
            }
        }
        attempts.sort_by(|a, b| a.task_id.cmp(&b.task_id));
        assert_eq!(attempts.len(), 2);
        let writer = attempts.remove(0);
        let reader = attempts.remove(0);
        let worker_limits = Arc::new(limits.worker.clone());
        let control = Control {
            deadline_ms: reader.deadline_unix_ms,
            cancel: rx,
            runtime_limits: limits.clone(),
            validation_runtime: Arc::new(
                crate::native::ValidationRuntime::new(worker_limits.clone()).unwrap(),
            ),
            worker_limits,
        };
        let f = Self {
            area,
            client,
            limits,
            reader,
            writer,
            control,
            _cancel: cancel,
            drop_ack,
            cancel_ack,
            sends,
            server,
        };
        for attempt in [&f.writer, &f.reader] {
            f.report(
                attempt,
                Observation::Started {
                    thread_id: None,
                    turn_id: None,
                },
            )
            .await;
        }
        f
    }
    async fn report(&self, attempt: &Attempt, observation: Observation) {
        assert!(matches!(
            self.client
                .call(Operation::ObserveWorker {
                    report: Report {
                        report_id: mutations::fresh_id("scan-report").unwrap(),
                        task_id: attempt.task_id.clone(),
                        attempt_id: attempt.id.clone(),
                        generation: attempt.generation,
                        worker_id: attempt.worker_id.clone(),
                        incarnation: attempt.worker_incarnation.clone(),
                        observation,
                    }
                })
                .await
                .unwrap()
                .outcome,
            Outcome::Ok { .. }
        ));
    }
}

#[path = "completion_required_input_tests.rs"]
mod completion_required_input_tests;
