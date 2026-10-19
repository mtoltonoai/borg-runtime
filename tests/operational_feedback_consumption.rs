//! Definition selection followed by actual worker consumers. These fixtures use
//! the real controller, worker subprocess, validation helper and private Git;
//! native HTTP and Codex stdio peers are controlled. No live model is launched.
use borg_runtime::{
    client::ApiClient,
    controller::{Controller, ControllerConfig},
    limits::{DefinitionLimitField as Field, ExecutionLimitOverrides, RuntimeLimits},
    protocol::*,
    repository_catalogue::Registration,
    worker::process::now_ms,
};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request as HttpRequest, Response, body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

static NEXT: AtomicU64 = AtomicU64::new(1);
const WAIT: Duration = Duration::from_secs(40);

fn save(path: impl AsRef<Path>, value: &impl serde::Serialize) {
    fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}
fn task(reply: Reply) -> Task {
    match reply.outcome {
        Outcome::Ok {
            data: Data::Task(value),
        } => *value,
        other => panic!("expected task: {other:?}"),
    }
}
fn git(cwd: &Path, args: &[&str]) -> String {
    let output = Command::new("/usr/bin/git")
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "user.name=Consumer fixture",
            "-c",
            "user.email=fixture@example.invalid",
        ])
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "Git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().into()
}

struct Background(Option<tokio::task::JoinHandle<()>>);
impl Background {
    async fn stop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
            let _ = task.await;
        }
    }
}
impl Drop for Background {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}
struct WorkerProcess {
    child: Child,
    root: PathBuf,
}
impl WorkerProcess {
    fn stop(&mut self) -> bool {
        let mut forced = false;
        let status = if let Some(status) = self.child.try_wait().unwrap() {
            status
        } else {
            assert!(
                Command::new("/bin/kill")
                    .args(["-INT", "--", &self.child.id().to_string()])
                    .status()
                    .unwrap()
                    .success()
            );
            let until = Instant::now() + Duration::from_secs(18);
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    break status;
                }
                if Instant::now() >= until {
                    forced = true;
                    self.child.kill().unwrap();
                    break self.child.wait().unwrap();
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        };
        save(
            self.root.join("worker-closed.json"),
            &json!({"pid":self.child.id(),"status":status.to_string(),"success":status.success(),"forced":forced,"reaped":true}),
        );
        !forced && status.success()
    }
}
impl Drop for WorkerProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.stop();
        }
    }
}

struct Lab {
    root: PathBuf,
    api: ApiClient,
    url: String,
    _controller: Controller,
    server: Background,
    limits: Arc<RuntimeLimits>,
    repository: Repository,
    gate: TcpListener,
    worker: Option<WorkerProcess>,
}
impl Lab {
    async fn new(field: Field, executor: ExecutorKind) -> Self {
        let base = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("artifacts/operational-feedback-consumption");
        fs::create_dir_all(&base).unwrap();
        let root = loop {
            let path = base.join(format!(
                "{field:?}-{}-{}-{}",
                now_ms(),
                std::process::id(),
                NEXT.fetch_add(1, Ordering::SeqCst)
            ));
            match fs::create_dir(&path) {
                Ok(()) => break path,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("fresh fixture directory: {error}"),
            }
        };
        let mut limits = RuntimeLimits::seed().unwrap();
        limits.revision = format!("consumer-{field:?}");
        limits.controller.command_wait_ms = 10;
        limits.worker.coding_capacity = usize::from(executor == ExecutorKind::Coding);
        limits.worker.native_capacity = usize::from(executor == ExecutorKind::Native);
        limits.validate().unwrap();
        save(root.join("limits.json"), &limits);
        let limits = Arc::new(limits);
        let controller = Controller::open_with_config(
            root.join("tasks.sqlite"),
            ControllerConfig {
                limits: limits.clone(),
                validation_executable: env!("CARGO_BIN_EXE_borg-runtime").into(),
                ..Default::default()
            },
        )
        .unwrap();
        let listener = borg_runtime::server::listen("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (owner, origin) = (controller.clone(), url.clone());
        let server = Background(Some(tokio::spawn(async move {
            borg_runtime::server::serve(owner, listener, origin)
                .await
                .unwrap();
        })));
        let api = ApiClient::new(&url, limits.clone()).unwrap();
        let seed = root.join("seed");
        fs::create_dir(&seed).unwrap();
        fs::write(seed.join("answer.txt"), "before\n").unwrap();
        git(&seed, &["init", "-q"]);
        git(&seed, &["add", "answer.txt"]);
        git(&seed, &["commit", "-qm", "consumer starting source"]);
        let start_commit = git(&seed, &["rev-parse", "HEAD"]);
        let remote = root.join("remote.git");
        git(
            &root,
            &[
                "clone",
                "--bare",
                "--quiet",
                seed.to_str().unwrap(),
                remote.to_str().unwrap(),
            ],
        );
        let registration = Registration {
            pull_request_target: None,
            repository_id: "consumer-repository".into(),
            registration_revision: "v1".into(),
            display_name: "Private consumer fixture".into(),
            source_endpoint: remote.to_string_lossy().into(),
            checkpoint_endpoint: remote.to_string_lossy().into(),
            source_store_key: "remote.git".into(),
            checkpoint_store_key: "remote.git".into(),
        };
        assert!(matches!(
            api.call(Operation::RegisterRepository {
                operation_id: "register-repository".into(),
                registration: registration.clone()
            })
            .await
            .unwrap()
            .outcome,
            Outcome::Ok {
                data: Data::RepositoryRegistration(_)
            }
        ));
        let repository = Repository {
            pull_request_target: None,
            registration: Some(registration.binding()),
            url: String::new(),
            start_commit,
            checkpoint_remote: String::new(),
        };
        let gate = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = root.join("consumer_peer.py");
        fs::write(&peer, include_str!("operational_feedback/consumer_peer.py")).unwrap();
        fs::set_permissions(&peer, fs::Permissions::from_mode(0o700)).unwrap();
        save(
            peer.with_extension("json"),
            &json!({"port":gate.local_addr().unwrap().port()}),
        );
        Self {
            root,
            api,
            url,
            _controller: controller,
            server,
            limits,
            repository,
            gate,
            worker: None,
        }
    }
    fn input(&self, field: Field, value: usize, executor: ExecutorKind) -> TaskInput {
        let mut effective = self.limits.execution.clone();
        field.set(&mut effective, value);
        let mut overrides = ExecutionLimitOverrides::default();
        field.materialize(&mut overrides, &effective);
        TaskInput { instruction_source: None,
            pull_request: None,
            resolved_limits: None, repository_read: None,
            instruction: "Set answer.txt to after; keep all other source unchanged.".into(),
            definition: Definition { charter: None, id: "consumer-agent".into(), revision: "old-v1".into(), executor,
                inference: Some(InferenceSettings::selected()), execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }), limit_overrides: Some(Box::new(overrides)) },
            repository: (executor == ExecutorKind::Coding).then(|| self.repository.clone()),
            acceptance_commands: if executor == ExecutorKind::Coding { vec![vec!["/usr/bin/python3".into(), "-I".into(), "-S".into(), "-B".into(), "-c".into(), "from pathlib import Path; assert Path('answer.txt').read_text() == 'after\\n'".into()]] } else { vec![] },
            output_schema: (executor == ExecutorKind::Native).then(|| json!({"type":"object","properties":{"result":{"type":"string"}},"required":["result"],"additionalProperties":false})),
            limits: Limits { deadline_unix_ms: now_ms()+120_000, model_requests: 1, output_bytes: 8192, result_bytes: self.limits.task_defaults.result_bytes, max_delegation_depth: 1 },
            dependencies: vec![], native_workflow: None,
        }
    }
    async fn submit(&self, name: &str, input: TaskInput) -> Task {
        task(
            self.api
                .call(Operation::SubmitTask {
                    operation_id: name.into(),
                    input: (input).into(),
                })
                .await
                .unwrap(),
        )
    }
    async fn get(&self, id: &str) -> Task {
        task(
            self.api
                .call(Operation::GetTask { task_id: id.into() })
                .await
                .unwrap(),
        )
    }
    async fn attempt(&self, value: &Task) -> borg_runtime::collaboration::AttemptView {
        let reply = self
            .api
            .call(Operation::QueryCollaboration {
                query: borg_runtime::collaboration::Query::TaskAttempts {
                    task_id: value.id.clone(),
                    after: None,
                    limit: 2,
                },
            })
            .await
            .unwrap();
        match reply.outcome {
            Outcome::Ok {
                data:
                    Data::Collaboration(borg_runtime::collaboration::QueryResult::TaskAttempts(
                        mut page,
                    )),
            } => {
                assert_eq!(page.items.len(), 1);
                assert!(page.next_cursor.is_none());
                let view = page.items.remove(0);
                assert_eq!(Some(&view.attempt), value.attempt.as_ref());
                view
            }
            other => panic!("canonical attempt missing: {other:?}"),
        }
    }
    async fn until(&self, id: &str, predicate: impl Fn(&Task) -> bool) -> Task {
        let until = Instant::now() + WAIT;
        loop {
            let value = self.get(id).await;
            if predicate(&value) {
                return value;
            }
            assert!(
                Instant::now() < until,
                "consumer predicate timed out: {value:?}; {}",
                self.root.display()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    async fn pair(&self, input: TaskInput, field: Field, next: usize) -> (Task, Task) {
        let old = self.submit("old-submit", input.clone()).await;
        let old_input = serde_json::to_vec(&old.input).unwrap();
        let update = DefinitionUpdate {
            agent_id: old.assigned_agent_id.clone(),
            expected_revision: old.input.definition.revision.clone(),
            expected_global_revision: self.limits.revision.clone(),
            expected_policy_revision: self
                .limits
                .definition_updates
                .as_ref()
                .unwrap()
                .revision
                .clone(),
            changes: [(field, next)].into_iter().collect(),
        };
        let reply = self
            .api
            .call(Operation::UpdateDefinition {
                operation_id: "apply-definition".into(),
                update,
            })
            .await
            .unwrap();
        assert!(reply.commit.is_some());
        save(self.root.join("definition-commit.json"), &reply);
        let selected = match reply.outcome {
            Outcome::Ok {
                data: Data::DefinitionUpdate(value),
            } => {
                assert_eq!(value.changed_fields, vec![field]);
                assert_eq!(value.effective_for, ConfigurationEffect::FutureSubmissions);
                assert_eq!(value.classification, ConfigurationChangeClass::Widening);
                value.selected_definition
            }
            other => panic!("definition change failed: {other:?}"),
        };
        match self
            .api
            .call(Operation::GetAgent {
                agent_id: selected.id.clone(),
            })
            .await
            .unwrap()
            .outcome
        {
            Outcome::Ok {
                data: Data::Agent(agent),
            } => assert_eq!(agent.definition, selected),
            other => panic!("selected definition missing: {other:?}"),
        }
        let mut next_input = input;
        next_input.definition = selected;
        if field == Field::NativeOutputBytes {
            next_input.limits.output_bytes = 12_288;
        }
        let new = self.submit("new-submit", next_input).await;
        assert_eq!(
            serde_json::to_vec(&self.get(&old.id).await.input).unwrap(),
            old_input
        );
        let mut expected = old.input.execution_limits().unwrap().clone();
        field.set(&mut expected, next);
        assert_eq!(new.input.execution_limits().unwrap(), &expected);
        assert_ne!(old.input.definition.revision, new.input.definition.revision);
        assert_ne!(old.session_id, new.session_id);
        let mut expected_task_limits = old.input.limits.clone();
        if field == Field::NativeOutputBytes {
            expected_task_limits.output_bytes = 12_288;
        }
        assert_eq!(expected_task_limits, new.input.limits);
        save(self.root.join("accepted-old.json"), &old);
        save(self.root.join("accepted-new.json"), &new);
        (old, new)
    }
    fn start_worker(&mut self, executor: ExecutorKind, endpoint: Option<&str>) {
        let log = File::create(self.root.join("worker.log")).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_borg-runtime"));
        command
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", &self.root)
            .args([
                "worker",
                "--controller",
                &self.url,
                "--id",
                "consumer-worker",
                "--incarnation",
                "consumer-incarnation",
                "--workspace-root",
            ])
            .arg(self.root.join("worker"))
            .arg("--codex")
            .arg(self.root.join("consumer_peer.py"))
            .args([
                "--coding-capacity",
                if executor == ExecutorKind::Coding {
                    "1"
                } else {
                    "0"
                },
                "--native-capacity",
                if executor == ExecutorKind::Native {
                    "1"
                } else {
                    "0"
                },
            ])
            .arg("--limits")
            .arg(self.root.join("limits.json"))
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log);
        if let Some(endpoint) = endpoint {
            command
                .args(["--native-endpoint", endpoint])
                .env("OPENAI_API_KEY", "controlled-consumer-key");
        }
        self.worker = Some(WorkerProcess {
            child: command.spawn().unwrap(),
            root: self.root.clone(),
        });
    }
    async fn gate(&self) -> (TcpStream, Value) {
        let (stream, _) = tokio::time::timeout(WAIT, self.gate.accept())
            .await
            .expect("controlled peer did not reach gate")
            .unwrap();
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        tokio::time::timeout(WAIT, reader.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        assert!(line.len() < 4096);
        let value = serde_json::from_str(&line).unwrap();
        (reader.into_inner(), value)
    }
    fn checkpoint(&self, value: &Task) {
        assert_eq!(value.status, TaskStatus::Succeeded);
        let cp = value.selected_checkpoint.as_ref().unwrap();
        assert_eq!(value.result.as_ref().unwrap().checkpoint.as_ref(), Some(cp));
        assert_eq!(
            git(
                &self.root,
                &["--git-dir", &cp.repository, "rev-parse", &cp.remote_ref]
            ),
            cp.commit
        );
        assert_eq!(
            git(
                &self.root,
                &[
                    "--git-dir",
                    &cp.repository,
                    "show",
                    &format!("{}:answer.txt", cp.commit)
                ]
            ),
            "after"
        );
        assert_eq!(
            git(
                &self.root,
                &[
                    "--git-dir",
                    &cp.repository,
                    "diff",
                    "--name-only",
                    &self.repository.start_commit,
                    &cp.commit
                ]
            ),
            "answer.txt"
        );
        let validation = &value.result.as_ref().unwrap().validation;
        assert_eq!(validation.len(), 1);
        assert_eq!(validation[0], format!("check 1 at {}: pass", cp.commit));
    }
    async fn close(&mut self, old: &Task, new: &Task) {
        let mut worker = self.worker.take().unwrap();
        // A blocking wait here would starve a current-thread HTTP server during
        // worker cleanup. Reap on Tokio's blocking pool, preserving live owner.
        assert!(
            tokio::task::spawn_blocking(move || worker.stop())
                .await
                .unwrap(),
            "worker required forced cleanup"
        );
        assert_eq!(self.get(&old.id).await, *old);
        assert_eq!(self.get(&new.id).await, *new);
        self.server.stop().await;
        let address = self.url.strip_prefix("http://").unwrap();
        assert!(TcpStream::connect(address).await.is_err());
        save(
            self.root.join("case-complete.json"),
            &json!({"status":"PASS","old":old,"new":new,"worker_reaped":true,"controller_listener_closed":true,"scope":"Actual worker/controller; controlled native HTTP or Codex stdio; no live model"}),
        );
    }
}

struct Provider {
    endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
    server: Background,
}
impl Provider {
    async fn new(result: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(vec![]));
        let captured = requests.clone();
        let server = Background(Some(tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        let captured = captured.clone();
                        let result = result.clone();
                        connections.spawn(async move {
                            let service = service_fn(move |request: HttpRequest<Incoming>| {
                                let captured = captured.clone();
                                let result = result.clone();
                                async move {
                                    assert_eq!(request.method(), hyper::Method::POST);
                                    assert_eq!(request.uri().path(), "/v1/responses");
                                    let body = request.into_body().collect().await.unwrap().to_bytes();
                                    assert!(body.len() <= 256*1024);
                                    let value: Value = serde_json::from_slice(&body).unwrap();
                                    assert_eq!(value["model"], "gpt-6-astra");
                                    assert_eq!(value["reasoning"]["effort"], "max");
                                    captured.lock().unwrap().push(value);
                                    let response = json!({"id":"controlled-response", "status":"completed", "error":null,
                                        "usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7},
                                        "output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":json!({"result":result}).to_string()}]}]});
                                    Ok::<_, std::convert::Infallible>(Response::builder().header("content-type","application/json").body(Full::new(Bytes::from(response.to_string()))).unwrap())
                                }
                            });
                            hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), service).await.unwrap();
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        })));
        Self {
            endpoint,
            requests,
            server,
        }
    }
}

async fn native_case(field: Field, old_bound: usize, new_bound: usize, error: &str) {
    let mut lab = Lab::new(field, ExecutorKind::Native).await;
    let result = if field == Field::NativeOutputBytes {
        "x".repeat(9000)
    } else {
        "ok".into()
    };
    let mut provider = Provider::new(result.clone()).await;
    let mut input = lab.input(field, old_bound, ExecutorKind::Native);
    match field {
        Field::NativeContextBytes => input.instruction = "x".repeat(80_000),
        Field::NativeOutputBytes => {
            // The native policy is an admission ceiling. Keep this refusal
            // explicit, then admit a valid old task and widen only later work.
            let mut oversized = input.clone();
            oversized.limits.output_bytes = 12_288;
            let refused = lab
                .api
                .call(Operation::SubmitTask {
                    operation_id: "old-output-ceiling-refusal".into(),
                    input: (oversized).into(),
                })
                .await
                .unwrap();
            assert!(
                matches!(&refused.outcome, Outcome::Refused { code, message }
                if code == "invalid_input" && message.ends_with("native_output_limit_invalid_native_output_bytes")),
                "{refused:?}"
            );
            save(lab.root.join("old-output-admission-refusal.json"), &refused);
        }
        Field::NativeResultBytes => {
            input.limits.output_bytes = 4096;
            input.limits.result_bytes = 12_288;
        }
        _ => unreachable!(),
    }
    let (old, new) = lab.pair(input, field, new_bound).await;
    lab.start_worker(ExecutorKind::Native, Some(&provider.endpoint));
    let failed = lab
        .until(&old.id, |task| {
            task.status == TaskStatus::Failed && task.attempt.as_ref().is_some_and(|a| a.stopped)
        })
        .await;
    assert!(!failed.attempt.as_ref().unwrap().uncertain);
    assert_eq!(failed.input, old.input);
    let failed_attempt = lab.attempt(&failed).await;
    let outcome = failed_attempt.executor_outcome.as_ref().unwrap();
    assert!(!outcome.success);
    assert!(
        outcome.summary.contains(error),
        "exact limiting field missing: {outcome:?}"
    );
    let accepted = lab
        .until(&new.id, |task| {
            task.status == TaskStatus::Succeeded && task.attempt.as_ref().is_some_and(|a| a.stopped)
        })
        .await;
    assert_eq!(accepted.input, new.input);
    assert!(!accepted.attempt.as_ref().unwrap().uncertain);
    assert_eq!(
        accepted.result.as_ref().unwrap().output.as_ref().unwrap()["result"],
        result
    );
    let completed_attempt = lab.attempt(&accepted).await;
    assert_eq!(completed_attempt.reported_model_requests, 1);
    assert_eq!(completed_attempt.input_tokens, Some(5));
    assert_eq!(completed_attempt.output_tokens, Some(2));
    save(
        lab.root.join("canonical-attempts.json"),
        &json!({"old":failed_attempt,"new":completed_attempt}),
    );
    assert_eq!(
        provider.requests.lock().unwrap().len(),
        if field == Field::NativeOutputBytes {
            2
        } else {
            1
        },
        "output case exercises both accepted task bounds; other old bounds refuse before HTTP"
    );
    if field == Field::NativeContextBytes {
        assert!(
            provider.requests.lock().unwrap()[0]
                .to_string()
                .contains(&"x".repeat(80_000))
        );
    }
    save(
        lab.root.join("provider-requests.json"),
        &*provider.requests.lock().unwrap(),
    );
    lab.close(&failed, &accepted).await;
    provider.server.stop().await;
}

async fn coding_case(field: Field, old_bound: usize, new_bound: usize) {
    let mut lab = Lab::new(field, ExecutorKind::Coding).await;
    let input = lab.input(field, old_bound, ExecutorKind::Coding);
    let (old, new) = lab.pair(input, field, new_bound).await;
    lab.start_worker(ExecutorKind::Coding, None);
    let mut done = vec![];
    // One coding slot preserves queue order. Each peer remains at its progress
    // boundary until the canonical task shows that exact event.
    for (index, (accepted, bound)) in [(&old, old_bound), (&new, new_bound)]
        .into_iter()
        .enumerate()
    {
        let (mut gate, observed) = lab.gate().await;
        assert_eq!(observed["before"], "before\n");
        let expected_output = if field == Field::CodingCommandOutputBytes {
            48.min(bound)
        } else {
            48
        };
        let progress = lab
            .until(&accepted.id, |task| {
                task.recent_output == "X".repeat(expected_output)
            })
            .await;
        assert_eq!(progress.input, accepted.input);
        assert_eq!(
            progress.output_truncated,
            field == Field::CodingCommandOutputBytes && bound < 48
        );
        assert_eq!(progress.status, TaskStatus::Active);
        let attempt = progress.attempt.as_ref().unwrap();
        assert_eq!(attempt.worker_id, "consumer-worker");
        assert_eq!(attempt.worker_incarnation, "consumer-incarnation");
        save(
            lab.root.join(format!("progress-{index}.json")),
            &json!({"task":progress,"peer":observed}),
        );
        gate.write_all(b"G").await.unwrap();
        drop(gate);
        let value = lab
            .until(&accepted.id, |task| {
                task.status == TaskStatus::Succeeded
                    && task.attempt.as_ref().is_some_and(|a| a.stopped)
            })
            .await;
        assert_eq!(value.input, accepted.input);
        assert!(!value.attempt.as_ref().unwrap().uncertain);
        lab.checkpoint(&value);
        let expected_chars = match field {
            Field::CodingSummaryChars => 24.min(bound),
            Field::CodingSummaryBytes => 24.min(bound / 2),
            Field::CodingCommandOutputBytes => 24,
            _ => unreachable!(),
        };
        let summary = &value.result.as_ref().unwrap().summary;
        assert_eq!(summary, &"\u{e9}".repeat(expected_chars));
        assert_eq!(
            summary.len(),
            expected_chars * 2,
            "character and UTF-8 byte limits remain distinct"
        );
        let observed = lab.attempt(&value).await;
        let reported = observed.executor_outcome.as_ref().unwrap();
        assert!(reported.success);
        assert_eq!(&reported.summary, summary);
        // Codex reports token totals, not its inner provider-request count.
        // The canonical Usage counters therefore remain unreported while the
        // exact matched thread/turn token observation is retained in the result.
        assert_eq!(observed.reported_model_requests, 0);
        assert_eq!(observed.input_tokens, None);
        assert_eq!(observed.output_tokens, None);
        assert_eq!(
            value.result.as_ref().unwrap().output.as_ref().unwrap()["observed_usage"],
            json!({"input_tokens":5,"output_tokens":2,"model_requests":null})
        );
        save(
            lab.root.join(format!("canonical-attempt-{index}.json")),
            &observed,
        );
        done.push(value);
    }
    lab.close(&done[0], &done[1]).await;
}

#[tokio::test]
async fn updated_native_context_is_consumed_without_changing_old_task() {
    native_case(
        Field::NativeContextBytes,
        65_536,
        131_072,
        "native_context_bytes",
    )
    .await;
}
#[tokio::test]
async fn updated_native_output_is_consumed_without_changing_old_task() {
    native_case(Field::NativeOutputBytes, 8192, 16_384, "task_output_bytes").await;
}
#[tokio::test]
async fn updated_native_result_is_consumed_without_changing_old_task() {
    native_case(
        Field::NativeResultBytes,
        8192,
        16_384,
        "native_result_bytes",
    )
    .await;
}
#[tokio::test]
async fn updated_coding_output_is_consumed_at_actual_progress_boundary() {
    coding_case(Field::CodingCommandOutputBytes, 32, 64).await;
}
#[tokio::test]
async fn updated_coding_summary_chars_is_consumed_at_actual_peer_completion() {
    coding_case(Field::CodingSummaryChars, 8, 16).await;
}
#[tokio::test]
async fn updated_coding_summary_bytes_is_consumed_at_actual_result_reporting() {
    coding_case(Field::CodingSummaryBytes, 17, 33).await;
}
