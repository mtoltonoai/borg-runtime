//! Automatic notices are retired; explicit agent memory remains canonical.
//! Real Controller SQL and a pinned prior Controller qualify retained history.
//! No worker/model/provider behavior is inferred from these fixtures.
use borg_runtime::{
    collaboration::{
        AgentMemoryBatch, AgentMemoryCursor, AgentMemoryObservation, AgentMemoryPage, QueryResult,
    },
    controller::{Controller, ControllerConfig},
    mcp::{Caller, Invocation},
    protocol::*,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
static NEXT: AtomicU64 = AtomicU64::new(1);
const PRIOR_SHA: &str = "7b518b38b151d04be14bf8a735408592772b2ec1caed5ec6aace99dd365207ad";
const DISABLED: &str = "automatic shared-memory change notifications are retired";
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
fn dir() -> PathBuf {
    let base = std::env::var_os("BORG_TEST_ARTIFACT")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let p = base.join(format!(
        "borg-memory-retirement-{}-{}-{}",
        std::process::id(),
        now(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir(&p).unwrap();
    p.join("task.db")
}
fn cfg() -> ControllerConfig {
    let mut p = borg_runtime::limits::RuntimeLimits::seed().unwrap();
    p.controller.command_wait_ms = 10;
    ControllerConfig {
        limits: Arc::new(p),
        validation_executable: env!("CARGO_BIN_EXE_borg-runtime").into(),
        ..Default::default()
    }
}
fn input(agent: &str) -> TaskInput {
    TaskInput { instruction_source: None,
        pull_request: None,
        resolved_limits: None,
        repository_read: None,
        instruction: "controlled explicit memory work".into(),
        definition: Definition {
            charter: None,
            limit_overrides: None,
            id: agent.into(),
            revision: "v1".into(),
            executor: ExecutorKind::Native,
            inference: Some(InferenceSettings::selected()),
            execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
        },
        repository: None,
        acceptance_commands: vec![],
        output_schema: Some(
            json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false}),
        ),
        limits: Limits {
            deadline_unix_ms: now() + 600_000,
            model_requests: 20,
            output_bytes: 4096,
            result_bytes: cfg().limits.task_defaults.result_bytes,
            max_delegation_depth: 4,
        },
        dependencies: vec![],
        native_workflow: None,
    }
}
enum Api {
    Current(Controller),
    Prior {
        origin: String,
        client: reqwest::Client,
        maximum: usize,
    },
}
fn open(p: &Path, config: ControllerConfig) -> Api {
    Api::Current(Controller::open_with_config(p, config).unwrap())
}
async fn x(c: &Api, command: Operation) -> Reply {
    let request = Request {
        protocol_revision: if matches!(c, Api::Prior { .. }) {
            17
        } else {
            WIRE_REVISION
        },
        request_id: format!("memory-request-{}", NEXT.fetch_add(1, Ordering::SeqCst)),
        command: command.clone(),
    };
    let r = match c {
        Api::Current(c) => Box::pin(c.handle(request)).await,
        Api::Prior {
            origin,
            client,
            maximum,
        } => {
            let bytes = serde_json::to_vec(&request).unwrap();
            assert!(bytes.len() <= *maximum);
            let mut response = client
                .post(format!("{origin}/api/operations"))
                .header("content-type", "application/json")
                .body(bytes)
                .send()
                .await
                .unwrap();
            assert!(response.status().is_success());
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await.unwrap() {
                assert!(bytes.len().saturating_add(chunk.len()) <= *maximum);
                bytes.extend_from_slice(&chunk);
            }
            let r: Reply = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(r.request_id, request.request_id);
            r
        }
    };
    r.validate_for(&command).unwrap();
    r
}
fn task(r: Reply) -> Task {
    match r.outcome {
        Outcome::Ok {
            data: Data::Task(t),
        } => *t,
        o => panic!("expected task {o:?}"),
    }
}
fn value(r: Reply) -> Option<MemoryValue> {
    match r.outcome {
        Outcome::Ok {
            data: Data::Memory(v),
        } => v,
        o => panic!("expected memory {o:?}"),
    }
}
fn memory(r: Reply) -> AgentMemoryObservation {
    match r.outcome {
        Outcome::Ok {
            data: Data::AgentMemory(v),
        } => v,
        o => panic!("expected prior notice {o:?}"),
    }
}
fn refused(r: &Reply, code: &str) {
    assert!(
        matches!(&r.outcome, Outcome::Refused { code: c, .. } if c == code),
        "{r:?}"
    );
}
fn disabled(r: &Reply) {
    assert!(
        matches!(&r.outcome, Outcome::Refused { code, message } if code == "disabled" && message == DISABLED),
        "{r:?}"
    );
}
fn actor(a: &Attempt) -> Caller {
    Caller {
        task_id: a.task_id.clone(),
        attempt_id: a.id.clone(),
        generation: a.generation,
        worker_id: a.worker_id.clone(),
        incarnation: a.worker_incarnation.clone(),
    }
}
fn invoke(a: &Attempt, id: &str, tool: &str, arguments: Value) -> Operation {
    Operation::CallCollaboration {
        call: Invocation {
            operation_id: id.into(),
            logical_call_id: format!("logical-{id}"),
            caller: actor(a),
            tool: tool.into(),
            arguments,
        },
    }
}
fn write(a: &Attempt, id: &str, key: &str, revision: Option<u64>, v: Value) -> Operation {
    invoke(
        a,
        id,
        "collab_memory_set",
        json!({"key":key,"expected_revision":revision,"value":v}),
    )
}
async fn get(c: &Api, id: &str) -> Task {
    task(x(c, Operation::GetTask { task_id: id.into() }).await)
}
async fn read(c: &Api, a: &Attempt, key: &str) -> Option<MemoryValue> {
    value(
        x(
            c,
            invoke(
                a,
                &format!("read-{}", NEXT.fetch_add(1, Ordering::SeqCst)),
                "collab_memory_get",
                json!({"key":key}),
            ),
        )
        .await,
    )
}
async fn keys(c: &Api, a: &Attempt) -> Vec<borg_runtime::collaboration::MemoryKey> {
    match x(
        c,
        invoke(
            a,
            &format!("keys-{}", NEXT.fetch_add(1, Ordering::SeqCst)),
            "collab_memory_list",
            json!({"cursor":null,"limit":20}),
        ),
    )
    .await
    .outcome
    {
        Outcome::Ok {
            data: Data::Collaboration(QueryResult::MemoryKeys(p)),
        } => {
            assert!(p.next_cursor.is_none());
            p.items
        }
        o => panic!("{o:?}"),
    }
}
async fn report(c: &Api, a: &Attempt, id: &str, observation: Observation) -> Reply {
    x(
        c,
        Operation::ObserveWorker {
            report: Report {
                report_id: id.into(),
                task_id: a.task_id.clone(),
                attempt_id: a.id.clone(),
                generation: a.generation,
                worker_id: a.worker_id.clone(),
                incarnation: a.worker_incarnation.clone(),
                observation,
            },
        },
    )
    .await
}
async fn actors(c: &Api) -> (Task, Attempt, Task, Attempt, Attempt) {
    let ta = task(
        x(
            c,
            Operation::SubmitTask {
                operation_id: "actor-a".into(),
                input: input("shared"),
            },
        )
        .await,
    );
    let tb = task(
        x(
            c,
            Operation::SubmitTask {
                operation_id: "actor-b".into(),
                input: input("shared"),
            },
        )
        .await,
    );
    let tc = task(
        x(
            c,
            Operation::SubmitTask {
                operation_id: "actor-c".into(),
                input: input("unrelated"),
            },
        )
        .await,
    );
    let w = Worker {
        id: "worker".into(),
        incarnation: "incarnation-1".into(),
        native_capacity: 3,
        coding_capacity: 0,
    };
    assert!(matches!(
        x(
            c,
            Operation::RegisterWorker {
                operation_id: "register".into(),
                worker: w.clone()
            }
        )
        .await
        .outcome,
        Outcome::Ok {
            data: Data::Worker(_)
        }
    ));
    let commands = match x(
        c,
        Operation::AwaitCommands {
            operation_id: "await-original".into(),
            worker_id: w.id,
            incarnation: w.incarnation,
            acknowledged: vec![],
        },
    )
    .await
    .outcome
    {
        Outcome::Ok {
            data: Data::Commands(v),
        } => v,
        o => panic!("{o:?}"),
    };
    let attempt = |id: &str| {
        commands
            .iter()
            .find_map(|v| match v {
                WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == id => {
                    Some(attempt.clone())
                }
                _ => None,
            })
            .unwrap()
    };
    let a = attempt(&ta.id);
    let b = attempt(&tb.id);
    let other = attempt(&tc.id);
    for (label, a) in [("a", &a), ("b", &b), ("other", &other)] {
        task(
            report(
                c,
                a,
                &format!("{label}-started"),
                Observation::Started {
                    thread_id: None,
                    turn_id: None,
                },
            )
            .await,
        );
    }
    (ta, a, tb, b, other)
}
async fn closed(p: &Path) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(p.with_extension("db.controller-lock"))
                .unwrap();
            if f.try_lock().is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
// Read only after the owner has released its real SQLite/lock ownership.
// Retain complete rows and explicit overflow sentinels, not just counts.
fn projection(p: &Path) -> BTreeMap<String, Vec<String>> {
    let db = rusqlite::Connection::open_with_flags(p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .unwrap();
    let mut out = BTreeMap::new();
    for (name, sql) in [
        (
            "sessions",
            "SELECT json_array(id,seq,agent,task_id,body,memory_start_seq,memory_cursor_seq,memory_pending) FROM sessions ORDER BY id",
        ),
        (
            "memories",
            "SELECT json_array(agent,key,revision,body,operation_id) FROM memories ORDER BY agent,key",
        ),
        (
            "history",
            "SELECT json_array(seq,task_id,revision,kind,body) FROM history ORDER BY seq",
        ),
        (
            "batch_counter",
            "SELECT json_array(kind,value) FROM counters WHERE kind='memory_batch' ORDER BY kind",
        ),
    ] {
        let mut q = db.prepare(&format!("{sql} LIMIT 513")).unwrap();
        let rows = q
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(rows.len() <= 512);
        out.insert(name.into(), rows);
    }
    out
}
fn cursor(p: &Path, session: &str) -> AgentMemoryCursor {
    let db = rusqlite::Connection::open_with_flags(p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .unwrap();
    let database_id = db
        .query_row(
            "SELECT value FROM settings WHERE name='database_id'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let (agent_id, sequence): (String, i64) = db
        .query_row(
            "SELECT agent,memory_cursor_seq FROM sessions WHERE id=?1",
            [session],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    AgentMemoryCursor {
        database_id,
        agent_id,
        session_id: session.into(),
        sequence: u64::try_from(sequence).expect("nonnegative retained memory cursor"),
    }
}
fn notice_ops(a: &Attempt, c: &AgentMemoryCursor, suffix: &str) -> Vec<Operation> {
    vec![
        Operation::ObserveAgentMemory {
            caller: actor(a),
            limit: 20,
        },
        Operation::PrepareAgentMemory {
            operation_id: format!("disabled-prepare-{suffix}"),
            caller: actor(a),
            expected_cursor: c.clone(),
            through_sequence: c.sequence,
            limit: 20,
        },
        Operation::AcknowledgeAgentMemory {
            operation_id: format!("disabled-ack-{suffix}"),
            caller: actor(a),
            batch_id: "absent-batch".into(),
        },
    ]
}
async fn original(c: &Api, op: &Operation, expected: &Reply) {
    let replay = x(c, op.clone()).await;
    assert_eq!(replay.outcome, expected.outcome);
    assert_eq!(replay.commit, expected.commit);
    let id = op.mutation_id().unwrap();
    match x(
        c,
        Operation::GetOperation {
            operation_id: id.into(),
        },
    )
    .await
    .outcome
    {
        Outcome::Ok {
            data: Data::Operation(record),
        } => {
            assert_eq!(record.id, id);
            assert_eq!(record.intended, serde_json::to_value(op).unwrap());
            assert_eq!(
                serde_json::from_value::<Outcome>(record.outcome.unwrap()).unwrap(),
                expected.outcome
            );
        }
        o => panic!("{o:?}"),
    }
}

// Explicit reads/writes retain the existing agent scope and never create a batch.
//= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
//= type=test
//# The platform MUST persist explicitly recorded agent memory in its database.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# A memory MUST be keyed by agent identity rather than by session.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# An agent's memories MUST persist across every session of that agent identity.
#[tokio::test]
async fn explicit_memory_cas_scope_and_reopen_survive_notification_retirement() {
    let p = dir();
    let c = open(&p, cfg());
    let (ta, a, tb, b, other) = actors(&c).await;
    drop(c);
    closed(&p).await;
    let initial = projection(&p);
    let c = open(&p, cfg());
    let first = write(
        &a,
        "first",
        "shared-key",
        None,
        json!({"text":"original value"}),
    );
    let one = x(&c, first.clone()).await;
    assert_eq!(value(one.clone()).unwrap().revision, 1);
    let two = value(
        x(
            &c,
            write(
                &b,
                "second",
                "shared-key",
                Some(1),
                json!({"text":"replacement value"}),
            ),
        )
        .await,
    )
    .unwrap();
    assert_eq!(two.revision, 2);
    assert_eq!(two.agent_id, "shared");
    assert_eq!(read(&c, &a, "shared-key").await, Some(two.clone()));
    assert_eq!(read(&c, &b, "shared-key").await, Some(two.clone()));
    assert!(read(&c, &other, "shared-key").await.is_none());
    let foreign = value(
        x(
            &c,
            write(
                &other,
                "foreign",
                "shared-key",
                None,
                json!({"text":"foreign value"}),
            ),
        )
        .await,
    )
    .unwrap();
    assert_eq!(foreign.agent_id, "unrelated");
    assert_eq!(foreign.revision, 1);
    assert_eq!(read(&c, &a, "shared-key").await, Some(two.clone()));
    for a in [&a, &b, &other] {
        let list = keys(&c, a).await;
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].key, "shared-key");
        assert_eq!(
            list[0].revision,
            if a.task_id == other.task_id { 1 } else { 2 }
        );
    }
    refused(
        &x(
            &c,
            write(&a, "stale", "shared-key", Some(1), json!("stale")),
        )
        .await,
        "stale_revision",
    );
    original(&c, &first, &one).await;
    assert_eq!(get(&c, &ta.id).await.input, ta.input);
    assert_eq!(get(&c, &tb.id).await.input, tb.input);
    drop(c);
    closed(&p).await;
    let retained = projection(&p);
    assert_eq!(retained["sessions"], initial["sessions"]);
    assert_eq!(retained["batch_counter"], initial["batch_counter"]);
    let c = open(&p, cfg());
    assert_eq!(read(&c, &b, "shared-key").await, Some(two));
    assert_eq!(read(&c, &other, "shared-key").await, Some(foreign));
    original(&c, &first, &one).await;
    drop(c);
    closed(&p).await;
    assert_eq!(projection(&p), retained);
}
#[tokio::test]
async fn fresh_notification_operations_are_disabled_without_cursor_batch_history_or_session_changes()
 {
    let p = dir();
    let c = open(&p, cfg());
    let (_, a, tb, b, _) = actors(&c).await;
    value(
        x(
            &c,
            write(&a, "notice-candidate", "key", None, json!("explicit only")),
        )
        .await,
    )
    .unwrap();
    drop(c);
    closed(&p).await;
    let before = projection(&p);
    let current_cursor = cursor(&p, &tb.session_id);
    let c = open(&p, cfg());
    for op in notice_ops(&b, &current_cursor, "first") {
        let first = x(&c, op.clone()).await;
        disabled(&first);
        if op.mutation_id().is_some() {
            original(&c, &op, &first).await;
        } else {
            assert!(first.commit.is_none());
            for _ in 0..3 {
                let next = x(&c, op.clone()).await;
                disabled(&next);
                assert_eq!(
                    next.observed_controller_revision,
                    first.observed_controller_revision
                );
            }
        }
    }
    drop(c);
    closed(&p).await;
    assert_eq!(projection(&p), before);
    let c = open(&p, cfg());
    for op in notice_ops(&b, &current_cursor, "after-reopen") {
        disabled(&x(&c, op).await);
    }
    drop(c);
    closed(&p).await;
    assert_eq!(projection(&p), before);
}
#[tokio::test]
async fn memory_commit_failure_and_stale_cas_preserve_explicit_value_and_no_notice_state() {
    let p = dir();
    let probe = Arc::new(AtomicBool::new(false));
    let mut config = cfg();
    config.qualification_fail_next_commit = Some(probe.clone());
    let c = open(&p, config.clone());
    let (_, a, _, b, _) = actors(&c).await;
    let op = write(&a, "rolled-back-memory", "key", None, json!("exact value"));
    probe.store(true, Ordering::SeqCst);
    refused(&x(&c, op.clone()).await, "store_unavailable");
    assert!(!probe.load(Ordering::SeqCst));
    assert!(read(&c, &b, "key").await.is_none());
    let committed = x(&c, op.clone()).await;
    let v = value(committed.clone()).unwrap();
    assert_eq!(v.revision, 1);
    refused(
        &x(
            &c,
            write(&b, "stale-after-commit", "key", None, json!("wrong")),
        )
        .await,
        "stale_revision",
    );
    assert_eq!(read(&c, &b, "key").await, Some(v.clone()));
    original(&c, &op, &committed).await;
    drop(c);
    closed(&p).await;
    let before = projection(&p);
    let c = open(&p, config);
    assert_eq!(read(&c, &b, "key").await, Some(v));
    original(&c, &op, &committed).await;
    drop(c);
    closed(&p).await;
    assert_eq!(projection(&p), before);
}
#[tokio::test]
async fn retired_notification_routes_still_check_current_caller_before_disabled() {
    let p = dir();
    let c = open(&p, cfg());
    let (_, _, tb, b, _) = actors(&c).await;
    drop(c);
    closed(&p).await;
    let cur = cursor(&p, &tb.session_id);
    let c = open(&p, cfg());
    for mut op in notice_ops(&b, &cur, "stale") {
        match &mut op {
            Operation::ObserveAgentMemory { caller, .. }
            | Operation::PrepareAgentMemory { caller, .. }
            | Operation::AcknowledgeAgentMemory { caller, .. } => caller.generation += 1,
            _ => unreachable!(),
        }
        refused(&x(&c, op).await, "stale_generation");
    }
    let current = get(&c, &tb.id).await;
    task(
        x(
            &c,
            Operation::CancelTask {
                operation_id: "cancel-receiver".into(),
                task_id: tb.id.clone(),
                expected_revision: current.revision,
            },
        )
        .await,
    );
    for op in notice_ops(&b, &cur, "cancelled") {
        refused(&x(&c, op).await, "invalid_transition");
    }
}

struct PriorProcess(std::process::Child);
impl PriorProcess {
    async fn stop(mut self) {
        assert!(
            std::process::Command::new("/bin/kill")
                .args(["-INT", "--", &self.0.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let status = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(s) = self.0.try_wait().unwrap() {
                    break s;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(status.success(), "prior Controller: {status}");
    }
}
impl Drop for PriorProcess {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}
async fn prior(p: &Path) -> (PriorProcess, Api) {
    let image = PathBuf::from(
        std::env::var_os("BORG_MEMORY_NOTICE_PRIOR_BINARY")
            .expect("explicit genuine prior17 Controller required"),
    );
    assert!(image.is_absolute());
    let hash = std::process::Command::new("/usr/bin/sha256sum")
        .arg("--")
        .arg(&image)
        .output()
        .unwrap();
    assert!(hash.status.success());
    assert_eq!(
        String::from_utf8(hash.stdout)
            .unwrap()
            .split_whitespace()
            .next(),
        Some(PRIOR_SHA)
    );
    let config = cfg();
    let metadata = p.parent().unwrap().join("prior-limits.json");
    std::fs::write(&metadata, serde_json::to_vec(&*config.limits).unwrap()).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let origin = format!("http://{address}");
    let mut process = PriorProcess(
        std::process::Command::new(image)
            .arg("controller")
            .arg("--limits")
            .arg(metadata)
            .arg("--db")
            .arg(p)
            .arg("--listen")
            .arg(address.to_string())
            .arg("--origin")
            .arg(&origin)
            .stdin(std::process::Stdio::null())
            .stdout(std::fs::File::create(p.parent().unwrap().join("prior.stdout")).unwrap())
            .stderr(std::fs::File::create(p.parent().unwrap().join("prior.stderr")).unwrap())
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert!(process.0.try_wait().unwrap().is_none());
            if tokio::net::TcpStream::connect(address).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    (
        process,
        Api::Prior {
            origin,
            client,
            maximum: config.limits.transport.body_bytes,
        },
    )
}
fn prepare(a: &Attempt, id: &str, p: &AgentMemoryPage) -> Operation {
    Operation::PrepareAgentMemory {
        operation_id: id.into(),
        caller: actor(a),
        expected_cursor: p.from_cursor.clone(),
        through_sequence: p.through_sequence,
        limit: 100,
    }
}
async fn observe(c: &Api, a: &Attempt) -> AgentMemoryObservation {
    memory(
        x(
            c,
            Operation::ObserveAgentMemory {
                caller: actor(a),
                limit: 100,
            },
        )
        .await,
    )
}
async fn clear_prior(c: &Api, a: &Attempt) {
    for n in 0..32 {
        let o = observe(c, a).await;
        let b = if let Some(b) = o.pending {
            b
        } else if let Some(p) = o.available {
            memory(x(c, prepare(a, &format!("prior-clear-{n}"), &p)).await)
                .pending
                .unwrap()
        } else {
            return;
        };
        assert!(b.page.notices.is_empty());
        memory(
            x(
                c,
                Operation::AcknowledgeAgentMemory {
                    operation_id: format!("prior-clear-ack-{n}"),
                    caller: actor(a),
                    batch_id: b.batch_id,
                },
            )
            .await,
        );
    }
    panic!("prior fixture baseline exceeds finite pages")
}
struct Historical {
    p: PathBuf,
    receiver: Attempt,
    task: Task,
    pending: AgentMemoryBatch,
    originals: Vec<(Operation, Reply)>,
}
async fn historical() -> Historical {
    let p = dir();
    let (process, c) = prior(&p).await;
    let (_, a, tb, b, _) = actors(&c).await;
    clear_prior(&c, &b).await;
    value(x(&c, write(&a, "prior-memory-1", "key", None, json!("one"))).await).unwrap();
    let page = observe(&c, &b).await.available.unwrap();
    let prepared_op = prepare(&b, "prior-prepared-1", &page);
    let prepared = x(&c, prepared_op.clone()).await;
    let batch = memory(prepared.clone()).pending.unwrap();
    assert_eq!(batch.page.notices.len(), 1);
    assert_eq!(batch.page.notices[0].revision, 1);
    let ack_op = Operation::AcknowledgeAgentMemory {
        operation_id: "prior-ack-1".into(),
        caller: actor(&b),
        batch_id: batch.batch_id,
    };
    let ack = x(&c, ack_op.clone()).await;
    assert!(memory(ack.clone()).pending.is_none());
    value(
        x(
            &c,
            write(&a, "prior-memory-2", "key", Some(1), json!("two")),
        )
        .await,
    )
    .unwrap();
    let page = observe(&c, &b).await.available.unwrap();
    let pending_op = prepare(&b, "prior-prepared-2", &page);
    let pending_reply = x(&c, pending_op.clone()).await;
    let pending = memory(pending_reply.clone()).pending.unwrap();
    assert_eq!(pending.page.notices[0].revision, 2);
    value(
        x(
            &c,
            write(&a, "prior-memory-3", "key", Some(2), json!("three")),
        )
        .await,
    )
    .unwrap();
    assert_eq!(observe(&c, &b).await.pending, Some(pending.clone()));
    drop(c);
    process.stop().await;
    closed(&p).await;
    Historical {
        p,
        receiver: b,
        task: tb,
        pending,
        originals: vec![
            (prepared_op, prepared),
            (ack_op, ack),
            (pending_op, pending_reply),
        ],
    }
}
#[tokio::test]
#[ignore = "requires exact genuine prior17 BORG_MEMORY_NOTICE_PRIOR_BINARY"]
async fn historical_pending_and_original_prepare_ack_receipts_remain_exact_after_retirement() {
    let h = historical().await;
    let before = projection(&h.p);
    let cur = cursor(&h.p, &h.task.session_id);
    for cycle in 0..2 {
        let c = open(&h.p, cfg());
        for (op, reply) in &h.originals {
            original(&c, op, reply).await;
        }
        assert_eq!(
            read(&c, &h.receiver, "key").await.unwrap().value,
            json!("three")
        );
        for mut op in notice_ops(&h.receiver, &cur, &format!("historical-{cycle}")) {
            if let Operation::AcknowledgeAgentMemory { batch_id, .. } = &mut op {
                *batch_id = h.pending.batch_id.clone();
            }
            disabled(&x(&c, op).await);
        }
        assert_eq!(get(&c, &h.task.id).await.input, h.task.input);
        drop(c);
        closed(&h.p).await;
        assert_eq!(projection(&h.p), before);
    }
    // Successful original occurrences still replay after the original caller
    // loses authority; a fresh ACK cannot consume the retained pending batch.
    let c = open(&h.p, cfg());
    let current = get(&c, &h.task.id).await;
    task(
        x(
            &c,
            Operation::CancelTask {
                operation_id: "historical-cancel".into(),
                task_id: h.task.id.clone(),
                expected_revision: current.revision,
            },
        )
        .await,
    );
    for (op, reply) in &h.originals {
        original(&c, op, reply).await;
    }
    refused(
        &x(
            &c,
            Operation::AcknowledgeAgentMemory {
                operation_id: "fresh-after-cancel".into(),
                caller: actor(&h.receiver),
                batch_id: h.pending.batch_id.clone(),
            },
        )
        .await,
        "invalid_transition",
    );
    drop(c);
    closed(&h.p).await;
    let cancelled = projection(&h.p);
    assert_eq!(cancelled["sessions"], before["sessions"]);
    assert_eq!(cancelled["memories"], before["memories"]);
    assert_eq!(cancelled["batch_counter"], before["batch_counter"]);
    let c = open(&h.p, cfg());
    for (op, reply) in &h.originals {
        original(&c, op, reply).await;
    }
    drop(c);
    closed(&h.p).await;
    assert_eq!(projection(&h.p), cancelled);
}
fn database_bytes(p: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut out = BTreeMap::new();
    for f in [
        p.to_owned(),
        p.with_extension("db-wal"),
        p.with_extension("db-shm"),
    ] {
        if f.exists() {
            assert!(std::fs::metadata(&f).unwrap().len() <= 32 * 1024 * 1024);
            let bytes = std::fs::read(&f).unwrap();
            out.insert(f, bytes);
        }
    }
    out
}
#[tokio::test]
#[ignore = "requires exact genuine prior17 BORG_MEMORY_NOTICE_PRIOR_BINARY"]
async fn corrupt_historical_pending_cursor_and_start_refuse_without_repair_after_retirement() {
    let h = historical().await;
    // A valid successor reopen must succeed before any deliberate corruption.
    let valid = projection(&h.p);
    let c = open(&h.p, cfg());
    for (op, reply) in &h.originals {
        original(&c, op, reply).await;
    }
    drop(c);
    closed(&h.p).await;
    assert_eq!(projection(&h.p), valid);
    let original = database_bytes(&h.p);
    for field in ["notice", "cursor", "start"] {
        let p = dir();
        for (f, bytes) in &original {
            std::fs::write(p.parent().unwrap().join(f.file_name().unwrap()), bytes).unwrap();
        }
        let db = rusqlite::Connection::open(&p).unwrap();
        match field {
            "notice" => {
                let mut pending = h.pending.clone();
                pending.page.notices[0].revision += 1;
                assert_eq!(
                    db.execute(
                        "UPDATE sessions SET memory_pending=?2 WHERE id=?1",
                        rusqlite::params![
                            h.task.session_id,
                            serde_json::to_string(&pending).unwrap()
                        ]
                    )
                    .unwrap(),
                    1
                );
            }
            "cursor" => {
                assert_eq!(
                    db.execute(
                        "UPDATE sessions SET memory_cursor_seq=-1 WHERE id=?1",
                        [&h.task.session_id]
                    )
                    .unwrap(),
                    1
                );
            }
            _ => {
                assert_eq!(
                    db.execute(
                        "UPDATE sessions SET memory_start_seq=9223372036854775807 WHERE id=?1",
                        [&h.task.session_id]
                    )
                    .unwrap(),
                    1
                );
            }
        }
        drop(db);
        let before = database_bytes(&p);
        assert!(
            Controller::open_with_config(&p, cfg()).is_err(),
            "corrupt {field} admitted"
        );
        assert_eq!(
            database_bytes(&p),
            before,
            "corrupt {field} bytes rewritten"
        );
    }
}
