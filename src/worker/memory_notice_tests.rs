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
fn observation() -> AgentMemoryObservation {
    AgentMemoryObservation {
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
fn empty_observation_does_not_create_delivery_or_mutation_files() {
    let f = Fixture::new();
    let mut host = f.host(&caller(), vec![]);
    let observed = AgentMemoryObservation {
        start_sequence: 10,
        acknowledged_cursor: cursor(10),
        pending: None,
        available: None,
    };
    host.accept(&observed).unwrap();
    host.accept(&observed).unwrap();
    assert!(!f.0.exists());
    assert_eq!(host.mirror.unwrap().start_sequence, 10);
}

#[test]
fn observation_cannot_reseed_start_change_scope_or_skip_pending() {
    let f = Fixture::new();
    let mut host = f.host(&caller(), vec![]);
    let observed = observation();
    host.accept(&observed).unwrap();
    let path = host.directory.join("delivery.json");
    let original = std::fs::read(&path).unwrap();
    for mode in [
        "start", "database", "session", "agent", "pending", "backward",
    ] {
        let mut changed = observed.clone();
        match mode {
            "start" => changed.start_sequence = 9,
            "database" => changed.acknowledged_cursor.database_id = "other-database".into(),
            "session" => changed.acknowledged_cursor.session_id = "other-session".into(),
            "agent" => changed.acknowledged_cursor.agent_id = "other-agent".into(),
            "pending" => changed.pending = None,
            _ => changed.acknowledged_cursor.sequence = 9,
        }
        assert!(host.accept(&changed).is_err(), "accepted changed {mode}");
        assert_eq!(std::fs::read(&path).unwrap(), original);
    }
}

//= spec/impl/v2/implementation.md#shared-memory-changes-reach-sessions
//# Pending memory-change notices MUST remain recoverable after a process restart.
#[test]
fn ambiguous_delivery_survives_reopen_and_only_new_caller_context_can_redeliver() {
    let f = Fixture::new();
    let mut host = f.host(&caller(), vec![]);
    host.accept(&observation()).unwrap();
    host.current_confirmed = true;
    assert_eq!(
        host.begin_append("batch-1", "executor-context-1").unwrap(),
        AppendDisposition::Ready
    );
    let original = std::fs::read(host.directory.join("delivery.json")).unwrap();
    drop(host);
    let mut same = f.host(&caller(), vec![]);
    same.current_confirmed = true;
    for context in ["executor-context-1", "another-context-within-same-attempt"] {
        assert_eq!(
            same.begin_append("batch-1", context).unwrap(),
            AppendDisposition::Deferred
        );
        assert_eq!(
            std::fs::read(same.directory.join("delivery.json")).unwrap(),
            original
        );
    }
    let mut next = caller();
    next.attempt_id = "attempt-2".into();
    next.generation = 2;
    next.worker_id = "worker-2".into();
    next.incarnation = "incarnation-2".into();
    let mut replacement = f.host(&next, vec![]);
    assert!(
        replacement
            .begin_append("batch-1", "executor-context-2")
            .is_err()
    );
    replacement.accept(&observation()).unwrap();
    replacement.current_confirmed = true;
    assert_eq!(
        replacement
            .begin_append("batch-1", "executor-context-1")
            .unwrap(),
        AppendDisposition::Deferred
    );
    assert_eq!(
        replacement
            .begin_append("batch-1", "executor-context-2")
            .unwrap(),
        AppendDisposition::Ready
    );
    let mirror = replacement.mirror.unwrap();
    assert_eq!(mirror.start_sequence, 10);
    assert_eq!(mirror.acknowledged_cursor, cursor(10));
    assert_eq!(
        mirror.pending.unwrap().batch,
        observation().pending.unwrap()
    );
}

#[test]
fn hints_are_exact_bounded_key_revisions_without_memory_values() {
    let f = Fixture::new();
    let host = f.host(&caller(), vec![]);
    let batch = observation().pending.unwrap();
    let encoded = host.encode(&batch).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&encoded).unwrap();
    assert_eq!(parsed["batch_id"], "batch-1");
    assert_eq!(parsed["notices"], json!([{"key":"decision","revision":3}]));
    assert!(
        parsed["instruction"]
            .as_str()
            .unwrap()
            .contains("collab_memory_get")
    );
    assert!(parsed.get("value").is_none());
    assert!(
        serde_json::to_vec(&encoded).unwrap().len()
            <= crate::worker::test_limits()
                .execution
                .memory_notice
                .notice_bytes
    );
    let mut crowded = batch.page;
    crowded.notices = (0..16)
        .map(|i| AgentMemoryNotice {
            key: format!("{}{:02}", "\\".repeat(254), i),
            revision: 1,
        })
        .collect();
    let limit = host.fitting_limit(&crowded).unwrap();
    assert!((1..16).contains(&limit));
    crowded.notices.truncate(limit);
    // Existing controller identity representation, not a metadata-sized fake ID.
    let batch_id = format!(
        "memory-batch-{}-{:020}",
        crowded.from_cursor.database_id,
        i64::MAX
    );
    assert!(
        host.encode(&AgentMemoryBatch {
            batch_id,
            page: crowded
        })
        .is_ok()
    );
}

#[test]
fn credential_hint_and_changed_retained_text_refuse_without_rewriting() {
    let f = Fixture::new();
    let mut host = f.host(&caller(), vec!["fixture-secret-49381".into()]);
    let mut secret = observation();
    secret.pending.as_mut().unwrap().page.notices[0].key = "fixture-secret-49381".into();
    let error = host.accept(&secret).unwrap_err();
    assert!(!error.contains("fixture-secret-49381"));
    assert!(!f.0.exists());
    host.accept(&observation()).unwrap();
    let path = host.directory.join("delivery.json");
    let mut mirror = host.mirror.unwrap();
    mirror.pending.as_mut().unwrap().text = "unrelated host text".into();
    atomic_json(&path, &mirror).unwrap();
    let before = std::fs::read(&path).unwrap();
    assert!(
        Host::open_with_credentials(
            ApiClient::new("http://127.0.0.1:1", crate::worker::test_limits()).unwrap(),
            &f.0,
            &caller(),
            "reader-session",
            "shared-agent",
            &crate::worker::test_limits().execution.memory_notice,
            vec![]
        )
        .is_err()
    );
    assert_eq!(std::fs::read(path).unwrap(), before);
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

// These fixtures use the real controller, HTTP client, Host and durable journals.
// They never launch an executor or provider. The caller's existing `?` on prepare
// is the contract that prevents its next request/turn/held-result release.
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
        let controller = Controller::open_with_config(
            area.0.join("controller.db"),
            ControllerConfig {
                limits: limits.clone(),
                ..ControllerConfig::default()
            },
        )
        .unwrap();
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
            let input = TaskInput {
                resolved_limits: None,
                repository_read: None,
                instruction: "Controlled memory boundary; no executor is launched".into(),
                definition: Definition {
                    id: "memory-scan-agent".into(),
                    revision: "one".into(),
                    executor: ExecutorKind::Coding,
                    inference: Some(InferenceSettings::selected()),
                    execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
                    limit_overrides: None,
                },
                repository: Some(Repository {
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
        // Remove finite setup history through the canonical prepare/ACK API.
        // This is fixture setup, not a production continuation beyond its budget.
        for _ in 0..32 {
            let observed = f.observe().await;
            let Some(page) = observed.available else {
                return f;
            };
            assert!(page.notices.is_empty());
            let prepared = f
                .memory(Operation::PrepareAgentMemory {
                    operation_id: mutations::fresh_id("scan-baseline-prepare").unwrap(),
                    caller: f.caller(),
                    expected_cursor: page.from_cursor,
                    through_sequence: page.through_sequence,
                    limit: 1,
                })
                .await;
            f.memory(Operation::AcknowledgeAgentMemory {
                operation_id: mutations::fresh_id("scan-baseline-ack").unwrap(),
                caller: f.caller(),
                batch_id: prepared.pending.unwrap().batch_id,
            })
            .await;
        }
        panic!("memory scan fixture setup exceeded finite history bound")
    }
    fn caller(&self) -> Caller {
        Caller {
            task_id: self.reader.task_id.clone(),
            attempt_id: self.reader.id.clone(),
            generation: self.reader.generation,
            worker_id: self.reader.worker_id.clone(),
            incarnation: self.reader.worker_incarnation.clone(),
        }
    }
    fn host(&self) -> Host {
        Host::open_with_credentials(
            self.client.clone(),
            &self.area.0.join("worker"),
            &self.caller(),
            &self.reader.session_id,
            "memory-scan-agent",
            &self.limits.execution.memory_notice,
            vec![],
        )
        .unwrap()
    }
    async fn memory(&self, command: Operation) -> AgentMemoryObservation {
        match self.client.call(command).await.unwrap().outcome {
            Outcome::Ok {
                data: Data::AgentMemory(observation),
            } => observation,
            other => panic!("scan memory: {other:?}"),
        }
    }
    async fn observe(&self) -> AgentMemoryObservation {
        self.memory(Operation::ObserveAgentMemory {
            caller: self.caller(),
            limit: 1,
        })
        .await
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
    async fn noise(&self) {
        self.report(
            &self.writer,
            Observation::Progress {
                text: "unrelated history".into(),
                output: String::new(),
                truncated: false,
            },
        )
        .await;
    }
    async fn notice(&self) {
        assert!(matches!(
            self.client
                .call(Operation::SetMemory {
                    operation_id: "scan-memory-write".into(),
                    task_id: self.writer.task_id.clone(),
                    attempt_id: self.writer.id.clone(),
                    generation: self.writer.generation,
                    key: "suffix-key".into(),
                    expected_revision: None,
                    value: json!("canonical-value-never-in-notice"),
                })
                .await
                .unwrap()
                .outcome,
            Outcome::Ok { .. }
        ));
    }
    fn ack_sends(&self) -> usize {
        self.sends
            .lock()
            .unwrap()
            .iter()
            .filter(|op| matches!(op, Operation::AcknowledgeAgentMemory { .. }))
            .count()
    }
}

#[tokio::test]
async fn exhausted_empty_prefix_refuses_boundary_and_reopen_delivers_exact_suffix_once() {
    let f = ScanFixture::new(1).await;
    let before = f.observe().await.acknowledged_cursor;
    f.noise().await;
    f.notice().await;
    let page = f.observe().await.available.unwrap();
    assert!(page.notices.is_empty() && page.has_more);
    let mut host = f.host();
    assert_eq!(
        host.prepare(&f.control).await.unwrap_err(),
        "memory notification scan budget exhausted (execution.memory_notice.empty_prefix_pages)"
    );
    let prefix = f.observe().await;
    assert!(prefix.acknowledged_cursor.sequence > before.sequence);
    assert_eq!(prefix.acknowledged_cursor, page.to_cursor);
    assert!(prefix.pending.is_none());
    assert_eq!(
        host.mirror.as_ref().unwrap().acknowledged_cursor,
        page.to_cursor
    );
    drop(host);
    let mut reopened = f.host();
    let notice = reopened.prepare(&f.control).await.unwrap().unwrap();
    let encoded: serde_json::Value = serde_json::from_str(&notice.text).unwrap();
    assert_eq!(
        encoded["notices"],
        json!([{"key":"suffix-key","revision":1}])
    );
    assert!(!notice.text.contains("canonical-value-never-in-notice"));
    assert_eq!(
        reopened
            .begin_append(&notice.batch_id, "required-context")
            .unwrap(),
        AppendDisposition::Ready
    );
    assert_eq!(
        reopened
            .begin_append(&notice.batch_id, "required-context")
            .unwrap(),
        AppendDisposition::Deferred
    );
    reopened
        .appended(&notice.batch_id, "required-context", &f.control)
        .await
        .unwrap();
    assert!(reopened.prepare(&f.control).await.unwrap().is_none());
    assert!(f.observe().await.available.is_none());
}

#[tokio::test]
async fn exact_budget_empty_range_is_acknowledged_and_may_release_boundary() {
    let f = ScanFixture::new(2).await;
    f.noise().await;
    f.noise().await;
    let before = f.observe().await;
    let first = before.available.unwrap();
    assert!(first.has_more);
    let acks = f.ack_sends();
    assert!(f.host().prepare(&f.control).await.unwrap().is_none());
    assert_eq!(f.ack_sends() - acks, 2);
    let after = f.observe().await;
    assert_eq!(after.acknowledged_cursor.sequence, first.through_sequence);
    assert!(after.pending.is_none() && after.available.is_none());
}

#[tokio::test]
async fn notice_on_final_permitted_page_is_delivered_without_preemptive_ack() {
    let f = ScanFixture::new(2).await;
    f.noise().await;
    f.notice().await;
    let acks = f.ack_sends();
    let mut host = f.host();
    let notice = host.prepare(&f.control).await.unwrap().unwrap();
    assert_eq!(f.ack_sends() - acks, 1);
    let retained = f.observe().await.pending.unwrap();
    assert_eq!(retained.batch_id, notice.batch_id);
    assert_eq!(
        retained.page.notices,
        vec![AgentMemoryNotice {
            key: "suffix-key".into(),
            revision: 1
        }]
    );
    assert_eq!(
        host.mirror.as_ref().unwrap().acknowledged_cursor,
        retained.page.from_cursor
    );
}

#[tokio::test]
async fn lost_prefix_ack_reply_recovers_original_receipt_without_repost_or_cursor_skip() {
    let f = ScanFixture::new(1).await;
    f.noise().await;
    f.notice().await;
    let page = f.observe().await.available.unwrap();
    let acks = f.ack_sends();
    f.drop_ack.store(true, Ordering::SeqCst);
    let mut host = f.host();
    assert!(host.prepare(&f.control).await.is_err());
    let ack_path = host.directory.join("acknowledge.json");
    let original = host.read_mutation(&ack_path).unwrap().unwrap();
    assert!(original.outcome.is_none() && !original.consumed);
    assert_eq!(f.observe().await.acknowledged_cursor, page.to_cursor);
    drop(host);
    let mut reopened = f.host();
    let notice = reopened.prepare(&f.control).await.unwrap().unwrap();
    assert_eq!(
        f.ack_sends() - acks,
        1,
        "lost ACK is looked up, never reposted"
    );
    let settled = reopened.read_mutation(&ack_path).unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(settled.request).unwrap(),
        serde_json::to_value(original.request).unwrap()
    );
    assert!(settled.consumed && settled.outcome.is_some());
    assert_eq!(f.observe().await.pending.unwrap().batch_id, notice.batch_id);
}

#[tokio::test]
async fn cancelled_prefix_ack_never_releases_boundary_or_advances_into_notice_suffix() {
    let f = ScanFixture::new(1).await;
    f.noise().await;
    f.notice().await;
    let page = f.observe().await.available.unwrap();
    f.cancel_ack.store(true, Ordering::SeqCst);
    let mut host = f.host();
    assert!(host.prepare(&f.control).await.is_err());
    assert!(*f.control.cancel.borrow());
    let after = f.observe().await;
    assert_eq!(after.acknowledged_cursor, page.to_cursor);
    assert!(
        after
            .available
            .unwrap()
            .notices
            .iter()
            .any(|n| n.key == "suffix-key")
    );
    let retained = std::fs::read(host.directory.join("delivery.json")).unwrap();
    drop(host);
    let mut reopened = f.host();
    assert!(reopened.prepare(&f.control).await.is_err());
    assert_eq!(
        std::fs::read(reopened.directory.join("delivery.json")).unwrap(),
        retained
    );
    assert_eq!(f.observe().await.acknowledged_cursor, page.to_cursor);
}

#[test]
fn inconsistent_memory_page_remainder_flags_refuse_without_rewriting_mirror() {
    let f = Fixture::new();
    let mut host = f.host(&caller(), vec![]);
    let original = observation();
    host.accept(&original).unwrap();
    let path = host.directory.join("delivery.json");
    let before = std::fs::read(&path).unwrap();
    for complete in [false, true] {
        let mut changed = original.clone();
        let page = &mut changed.pending.as_mut().unwrap().page;
        page.to_cursor.sequence = if complete {
            page.through_sequence
        } else {
            page.through_sequence - 1
        };
        page.has_more = complete;
        assert!(host.accept(&changed).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }
}

fn exact_production_notice_bound() -> usize {
    let area = Fixture::new();
    let host = area.host(&caller(), vec![]);
    let mut batch = observation().pending.unwrap();
    let database_id = "a".repeat(32);
    batch.page.from_cursor.database_id = database_id.clone();
    batch.page.to_cursor.database_id = database_id.clone();
    // Existing wire shape used by original and corrected controller versions.
    // This test deliberately does not depend on the newly shared formatter, so
    // the same discriminator can be compiled against the original production.
    batch.batch_id = format!("memory-batch-{database_id}-{:020}", 1);
    batch.page.notices = vec![AgentMemoryNotice {
        key: "suffix-key".into(),
        revision: 1,
    }];
    serde_json::to_vec(&host.encode(&batch).unwrap())
        .unwrap()
        .len()
}

#[tokio::test]
async fn fitting_accepts_original_controller_id_under_smaller_identity_metadata() {
    let f = ScanFixture::configured(1, |limits| {
        limits.revision = "memory-fit-identity-fixture-v1".into();
        limits.transport.identity_bytes = 128;
        limits.execution.native.call_id_bytes = 128;
    })
    .await;
    f.notice().await;
    let mut host = f.host();
    let notice = host.prepare(&f.control).await.unwrap().unwrap();
    let retained = f.observe().await.pending.unwrap();
    assert_eq!(notice.batch_id, retained.batch_id);
    let (prefix, sequence) = retained.batch_id.rsplit_once('-').unwrap();
    assert_eq!(
        prefix,
        format!("memory-batch-{}", retained.page.from_cursor.database_id)
    );
    assert_eq!(sequence.len(), 20);
    assert!(sequence.bytes().all(|byte| byte.is_ascii_digit()));
    assert!((1..=i64::MAX as u64).contains(&sequence.parse::<u64>().unwrap()));
    assert_eq!(
        retained.batch_id,
        format!(
            "memory-batch-{}-{:020}",
            retained.page.from_cursor.database_id,
            sequence.parse::<u64>().unwrap()
        )
    );
    assert!(retained.batch_id.len() < f.limits.transport.identity_bytes);
    assert_eq!(
        host.mirror.as_ref().unwrap().acknowledged_cursor,
        retained.page.from_cursor
    );
}

#[tokio::test]
async fn fitting_exact_original_notice_bound_preserves_lost_ack_and_reopen() {
    let bound = exact_production_notice_bound();
    for identity_bytes in [128, crate::worker::test_limits().transport.identity_bytes] {
        let f = ScanFixture::configured(1, |limits| {
            limits.revision = "memory-fit-notice-fixture-v1".into();
            limits.transport.identity_bytes = identity_bytes;
            limits.execution.native.call_id_bytes = identity_bytes;
            limits.execution.memory_notice.notice_bytes = bound;
        })
        .await;
        f.notice().await;
        let mut host = f.host();
        let notice = host.prepare(&f.control).await.unwrap().unwrap();
        assert_eq!(serde_json::to_vec(&notice.text).unwrap().len(), bound);
        let retained = f.observe().await.pending.unwrap();
        assert_eq!(retained.batch_id, notice.batch_id);
        assert_eq!(
            host.begin_append(&notice.batch_id, "exact-bound-context")
                .unwrap(),
            AppendDisposition::Ready
        );
        let acks = f.ack_sends();
        f.drop_ack.store(true, Ordering::SeqCst);
        assert!(
            host.appended(&notice.batch_id, "exact-bound-context", &f.control)
                .await
                .is_err()
        );
        let ack_path = host.directory.join("acknowledge.json");
        let original = host.read_mutation(&ack_path).unwrap().unwrap();
        assert!(original.outcome.is_none() && !original.consumed);
        assert_eq!(
            f.observe().await.acknowledged_cursor,
            retained.page.to_cursor
        );
        drop(host);
        let mut reopened = f.host();
        assert!(reopened.prepare(&f.control).await.unwrap().is_none());
        assert_eq!(
            f.ack_sends() - acks,
            1,
            "original committed ACK is looked up, never reposted"
        );
        let settled = reopened.read_mutation(&ack_path).unwrap().unwrap();
        assert_eq!(
            serde_json::to_value(settled.request).unwrap(),
            serde_json::to_value(original.request).unwrap()
        );
        assert!(settled.consumed && settled.outcome.is_some());
        assert!(reopened.mirror.as_ref().unwrap().pending.is_none());
        assert!(
            reopened.prepare(&f.control).await.unwrap().is_none(),
            "acknowledged notice is never reinjected"
        );
    }
}

#[tokio::test]
async fn fitting_refuses_one_byte_below_actual_notice_without_retaining_or_skipping() {
    let bound = exact_production_notice_bound();
    let f = ScanFixture::configured(1, |limits| {
        limits.revision = "memory-fit-refusal-fixture-v1".into();
        limits.transport.identity_bytes = 128;
        limits.execution.native.call_id_bytes = 128;
        limits.execution.memory_notice.notice_bytes = bound - 1;
    })
    .await;
    f.notice().await;
    let before = f.observe().await;
    let mut host = f.host();
    assert_eq!(
        host.prepare(&f.control).await.unwrap_err(),
        "memory notification cannot fit the delivery bound (execution.memory_notice.notice_bytes)"
    );
    let after = f.observe().await;
    assert_eq!(after.acknowledged_cursor, before.acknowledged_cursor);
    assert_eq!(after.available, before.available);
    assert!(after.pending.is_none());
    assert!(!host.directory.join("prepare.json").exists());
    assert!(!host.directory.join("acknowledge.json").exists());
}

#[tokio::test]
async fn memory_reopened_intent_barrier_precedes_actual_prepare_mutation() {
    use process::storage_tests::{Point, inject};
    let f = ScanFixture::new(1).await;
    f.notice().await;
    let page = f.observe().await.available.unwrap();
    let host = f.host();
    let path = host.directory.join("prepare.json");
    let request = Operation::PrepareAgentMemory {
        operation_id: "memory-original-barrier".into(),
        caller: f.caller(),
        expected_cursor: page.from_cursor,
        through_sequence: page.through_sequence,
        limit: 1,
    };
    let first = inject(&path, Point::DirectorySync, 5, 0);
    assert!(host.mutation("prepare.json", request.clone()).is_err());
    assert_eq!(first.hits(), 1);
    drop(first);
    let mut journal = host.read_mutation(&path).unwrap().unwrap();
    let original = std::fs::read(&path).unwrap();
    let sends = || {
        f.sends.lock().unwrap().iter().filter(|op|
        matches!(op, Operation::PrepareAgentMemory { operation_id, .. } if operation_id == "memory-original-barrier")
    ).count()
    };
    let blocked = inject(&path, Point::FileSync, 28, 0);
    assert!(host.resolve(&path, &mut journal, &f.control).await.is_err());
    assert_eq!(blocked.hits(), 1);
    assert_eq!(sends(), 0);
    assert_eq!(std::fs::read(&path).unwrap(), original);
    assert!(f.observe().await.pending.is_none());
    drop(blocked);
    assert!(matches!(
        host.resolve(&path, &mut journal, &f.control).await.unwrap(),
        Outcome::Ok {
            data: Data::AgentMemory(_)
        }
    ));
    assert_eq!(sends(), 1);
    let durable = host.read_mutation(&path).unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(&durable.request).unwrap(),
        serde_json::to_value(&request).unwrap()
    );
    assert!(durable.outcome.is_some());
}

#[tokio::test]
async fn memory_saved_prepare_outcome_requires_barrier_before_delivery() {
    use process::storage_tests::{Point, inject};
    for reopen in [false, true] {
        let f = ScanFixture::new(1).await;
        f.notice().await;
        let page = f.observe().await.available.unwrap();
        let host = f.host();
        let request = Operation::PrepareAgentMemory {
            operation_id: "memory-committed-barrier".into(),
            caller: f.caller(),
            expected_cursor: page.from_cursor,
            through_sequence: page.through_sequence,
            limit: 1,
        };
        let (path, mut journal) = host.mutation("prepare.json", request.clone()).unwrap();
        // Commit the exact original once, then use Host's real lookup-and-save path.
        let expected = f.client.call(request.clone()).await.unwrap();
        assert!(matches!(
            &expected.outcome,
            Outcome::Ok {
                data: Data::AgentMemory(_)
            }
        ));
        let first = inject(&path, Point::DirectorySync, 5, 0);
        assert!(host.resolve(&path, &mut journal, &f.control).await.is_err());
        assert_eq!(first.hits(), 1);
        drop(first);
        if reopen {
            journal = host.read_mutation(&path).unwrap().unwrap();
        }
        let original = std::fs::read(&path).unwrap();
        let ack_before = f.ack_sends();
        let blocked = inject(&path, Point::FileSync, 5, 0);
        assert!(host.resolve(&path, &mut journal, &f.control).await.is_err());
        assert_eq!(blocked.hits(), 1);
        assert_eq!(std::fs::read(&path).unwrap(), original);
        assert_eq!(f.ack_sends(), ack_before);
        drop(blocked);
        let recovered = host.resolve(&path, &mut journal, &f.control).await.unwrap();
        assert_eq!(
            serde_json::to_value(recovered).unwrap(),
            serde_json::to_value(&expected.outcome).unwrap()
        );
        let durable = host.read_mutation(&path).unwrap().unwrap();
        assert_eq!(
            durable.outcome_context.unwrap().commit,
            expected.commit.unwrap()
        );
        assert_eq!(
            serde_json::to_value(&durable.request).unwrap(),
            serde_json::to_value(&request).unwrap()
        );
        assert_eq!(f.sends.lock().unwrap().iter().filter(|op|
            matches!(op, Operation::PrepareAgentMemory { operation_id, .. } if operation_id == "memory-committed-barrier")
        ).count(), 1);
        assert_eq!(f.ack_sends(), ack_before);
    }
}
