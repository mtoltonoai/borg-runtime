//! Genuine wire18/schema19/worker14 producers; no marker downgrade or synthetic journal.
use super::*;
use borg_runtime::{controller::{Controller, ControllerConfig}, protocol::*};
use serde_json::{Value, json};

const PRIOR_IMAGE: &str = "467b75231c2e8b184ddf8b6fbfb997e1bf26a0b9d26d239bf9cbf24c3fdda830";
const PRIOR_LIMITS: &str = "c98a8e12070a1f8b7c89fc26d701ebe5bb30ca2e1a05a295683496ee0d904a77";
const PRIOR_WIRE: u32 = 18;

fn inputs() -> (PathBuf, PathBuf, PathBuf) {
    let old = PathBuf::from(std::env::var_os("BORG_PR_PRIOR_BINARY").expect("select the preserved actual wire18 executable"));
    let limits = PathBuf::from(std::env::var_os("BORG_PR_PRIOR_LIMITS").expect("select exact prior metadata"));
    let current = std::env::var_os("BORG_TEST_RUNTIME").map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_borg-runtime")));
    assert_hash(&old, PRIOR_IMAGE); assert_hash(&limits, PRIOR_LIMITS);
    assert_eq!(WIRE_REVISION, 21);
    (old, limits, current)
}
fn root(label: &str) -> PathBuf {
    let root = retained_images::artifact_root().join(format!("{label}-{}-{}", std::process::id(), process::now_ms()));
    std::fs::create_dir(&root).unwrap(); root
}
fn address() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}
async fn ready(child: &mut HistoricalController, root: &Path, label: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert!(child.0.try_wait().unwrap().is_none(), "controller exited before readiness");
            if std::fs::read_to_string(root.join(format!("{label}.stdout"))).unwrap().contains("Controller listening on") { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("controller readiness bound");
}
async fn exchange(client: &reqwest::Client, address: &str, request: &Request, wire: u32) -> (Reply, Vec<u8>) {
    assert_eq!(request.protocol_revision, wire);
    let bytes = serde_json::to_vec(request).unwrap(); assert!(bytes.len() <= FORMAT9_HTTP_BOUND);
    let mut response = client.post(format!("http://{address}/api/operations"))
        .header("content-type", "application/json").body(bytes).send().await.unwrap();
    assert!(response.status().is_success());
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.unwrap() {
        assert!(bytes.len() + chunk.len() <= FORMAT9_HTTP_BOUND); bytes.extend_from_slice(&chunk);
    }
    let reply: Reply = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(reply.request_id, request.request_id); reply.validate_for(&request.command).unwrap();
    (reply, bytes)
}
async fn original(client: &reqwest::Client, address: &str, command: Operation, saved: &mut Vec<(Operation, Reply)>) -> Reply {
    let request = Request { protocol_revision: PRIOR_WIRE, request_id: format!("prior-{}", saved.len()), command: command.clone() };
    let (reply, _) = exchange(client, address, &request, PRIOR_WIRE).await;
    assert!(matches!(reply.outcome, Outcome::Ok { .. }), "{reply:?}");
    assert!(reply.commit.is_some()); saved.push((command, reply.clone())); reply
}
async fn old_controller_refuses(image: &Path, limits: &Path, db: &Path, address: &str, root: &Path) {
    let before = inventory(db.parent().unwrap());
    let mut child = spawn_controller(image, limits, db, address, root, "old-refuses20");
    let status = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = child.0.try_wait().unwrap() { break status; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("historical19 must refuse current20 promptly");
    assert!(!status.success());
    assert!(std::fs::read_to_string(root.join("old-refuses20.stderr")).unwrap().contains("unsupported controller database schema"));
    assert!(!std::fs::read_to_string(root.join("old-refuses20.stdout")).unwrap().contains("Controller listening on"));
    assert_eq!(inventory(db.parent().unwrap()), before, "old reader must not rewrite any canonical file or inode");
}

#[tokio::test]
#[ignore = "explicit preserved wire18/schema19 executable and metadata required; root executes"]
async fn genuine_schema19_preserves_original_receipts_and_pending_generic_pr_then_old_reader_refuses20() {
    let (old, metadata, current) = inputs(); let root = root("pr-schema19");
    let selected = root.join("prior-runtime"); retained_images::materialize(&old, &selected, true); assert_hash(&selected, PRIOR_IMAGE);
    let state = root.join("canonical"); std::fs::create_dir(&state).unwrap(); let db = state.join("tasks.sqlite");
    let address = address(); let mut producer = spawn_controller(&selected, &metadata, &db, &address, &root, "producer19");
    ready(&mut producer, &root, "producer19").await;
    let limits = std::sync::Arc::new(borg_runtime::limits::RuntimeLimits::parse(&std::fs::read(&metadata).unwrap()).unwrap());
    let client = reqwest::Client::builder().no_proxy().timeout(Duration::from_secs(5)).build().unwrap();
    let mut saved = Vec::new();
    let input = TaskInput { instruction_source: None, pull_request: None, resolved_limits: None, repository_read: None,
        instruction: "Preserve the original generic effect without inventing a typed PR request".into(),
        definition: Definition { charter: None, limit_overrides: None, execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
            inference: Some(InferenceSettings::selected()), id: "prior-owner".into(), revision: "one".into(), executor: ExecutorKind::Native },
        repository: None, acceptance_commands: vec![], output_schema: Some(json!({"type":"integer"})),
        limits: Limits { deadline_unix_ms: process::now_ms() + 600_000, model_requests: 3, output_bytes: 16384,
            result_bytes: limits.task_defaults.result_bytes, max_delegation_depth: 4 }, dependencies: vec![], native_workflow: None };
    let submitted = original(&client, &address, Operation::SubmitTask { operation_id: "prior-submit".into(), input: (input).into() }, &mut saved).await;
    let Outcome::Ok { data: Data::Task(task) } = submitted.outcome else { unreachable!() };
    original(&client, &address, Operation::RegisterWorker { operation_id: "prior-register".into(),
        worker: Worker { id: "prior-worker".into(), incarnation: "one".into(), coding_capacity: 0, native_capacity: 1 } }, &mut saved).await;
    let assigned = original(&client, &address, Operation::AwaitCommands { operation_id: "prior-await".into(),
        worker_id: "prior-worker".into(), incarnation: "one".into(), acknowledged: vec![] }, &mut saved).await;
    let Outcome::Ok { data: Data::Commands(commands) } = assigned.outcome else { unreachable!() };
    let attempt = commands.iter().find_map(|command| match command { WorkerCommand::RunAttempt { attempt, .. } => Some(attempt.clone()), _ => None }).unwrap();
    original(&client, &address, Operation::ObserveWorker { report: Report { report_id: "prior-started".into(),
        worker_id: attempt.worker_id.clone(), incarnation: attempt.worker_incarnation.clone(), task_id: task.id.clone(),
        attempt_id: attempt.id.clone(), generation: attempt.generation, observation: Observation::Started { thread_id: None, turn_id: None } } }, &mut saved).await;
    let pending = original(&client, &address, Operation::PrepareOperation { operation_id: "prior-generic-pr".into(),
        task_id: task.id.clone(), attempt_id: attempt.id.clone(), generation: attempt.generation, kind: "pull_request".into(),
        intended: json!({"legacy_original":"opaque publication intent","caller_supplied_number":7}) }, &mut saved).await;
    let read = Request { protocol_revision: PRIOR_WIRE, request_id: "prior-current".into(), command: Operation::GetTask { task_id: task.id.clone() } };
    let (read, _) = exchange(&client, &address, &read, PRIOR_WIRE).await;
    let Outcome::Ok { data: Data::Task(task) } = read.outcome else { unreachable!() };
    original(&client, &address, Operation::CancelTask { operation_id: "prior-cancel".into(), task_id: task.id.clone(), expected_revision: task.revision }, &mut saved).await;
    original(&client, &address, Operation::ObserveWorker { report: Report { report_id: "prior-stopped".into(),
        worker_id: attempt.worker_id.clone(), incarnation: attempt.worker_incarnation.clone(), task_id: task.id.clone(),
        attempt_id: attempt.id.clone(), generation: attempt.generation, observation: Observation::ProcessTreeStopped } }, &mut saved).await;
    producer.stop().await; owner_released(&db).await; assert_eq!(controller_version(&db), 19);
    let rows = canonical_rows(&db); assert_eq!(rows["external_operations"].len(), 1);
    let identity = inventory(&state);
    let config = ControllerConfig { limits, validation_executable: current, ..ControllerConfig::default() };
    for _ in 0..2 {
        let controller = Controller::open_with_config(&db, config.clone()).unwrap();
        let unsupported = Box::pin(controller.handle(Request { protocol_revision: PRIOR_WIRE,
            request_id: "prior-wire-refused-before-replay".into(), command: saved[0].0.clone() })).await;
        assert!(matches!(unsupported.outcome, Outcome::Refused { code, .. } if code == "unsupported_version"));
        assert!(unsupported.commit.is_none(), "old wire cannot rewrite the original receipt");
        for (command, reply) in &saved {
            let replay = Box::pin(controller.handle(Request { protocol_revision: WIRE_REVISION, request_id: "current-original-replay".into(), command: command.clone() })).await;
            replay.validate_for(command).unwrap(); assert_eq!(replay.outcome, reply.outcome); assert_eq!(replay.commit, reply.commit);
        }
        let lookup = Box::pin(controller.handle(Request { protocol_revision: WIRE_REVISION, request_id: "current-pending-pr".into(),
            command: Operation::GetOperation { operation_id: "prior-generic-pr".into() } })).await;
        assert_eq!(lookup.outcome, pending.outcome, "legacy pending intent is not converted into typed PR publication");
        drop(controller); owner_released(&db).await; assert_eq!(controller_version(&db), 21); assert_eq!(canonical_rows(&db), rows);
    }
    let after = inventory(&state);
    for (path, before) in &identity {
        if path.to_string_lossy().ends_with("-wal") || path.to_string_lossy().ends_with("-shm") { continue; }
        let actual = &after[path]; assert_eq!((actual.0,actual.1,actual.2,actual.3),(before.0,before.1,before.2,before.3));
        if !before.0 && path != Path::new("tasks.sqlite") { assert_eq!(actual.4, before.4); }
    }
    old_controller_refuses(&selected, &metadata, &db, &address, &root).await;
    assert_hash(&old, PRIOR_IMAGE); assert_hash(&metadata, PRIOR_LIMITS);
}

async fn producer_proxy(listener: tokio::net::TcpListener, address: String, ready: tokio::sync::oneshot::Sender<Format9Proof>) -> tokio::net::TcpListener {
    let client = reqwest::Client::builder().no_proxy().timeout(Duration::from_secs(5)).build().unwrap();
    let mut registration = None;
    for _ in 0..8 {
        let (mut stream, _) = tokio::time::timeout(Duration::from_secs(10), listener.accept()).await.unwrap().unwrap();
        let request = format9_read_request(&mut stream).await; assert_eq!(request.protocol_revision, PRIOR_WIRE);
        match &request.command {
            Operation::RegisterWorker { worker, .. } => {
                assert!(registration.is_none()); assert_eq!(worker.id, "compatibility"); assert_eq!(worker.incarnation, "producer-fourteen");
                let (reply, bytes) = exchange(&client, &address, &request, PRIOR_WIRE).await;
                assert!(matches!(&reply.outcome, Outcome::Ok { data: Data::Worker(value) } if value == worker));
                assert!(reply.commit.is_some()); registration = Some((request.clone(), reply)); format9_write_reply(&mut stream, &bytes).await;
            }
            Operation::GetOperation { operation_id } => {
                assert!(operation_id.starts_with("worker-register-") || operation_id.starts_with("worker-await-"));
                let (_, bytes) = exchange(&client, &address, &request, PRIOR_WIRE).await; format9_write_reply(&mut stream, &bytes).await;
            }
            Operation::AwaitCommands { worker_id, incarnation, acknowledged, .. } => {
                assert_eq!(worker_id, "compatibility"); assert_eq!(incarnation, "producer-fourteen"); assert!(acknowledged.is_empty());
                let (registration, registered) = registration.unwrap();
                let Operation::RegisterWorker { worker, .. } = &registration.command else { unreachable!() };
                let mut replacement = worker.clone(); replacement.incarnation = "fixture-fence".into();
                let fence = Request { protocol_revision: PRIOR_WIRE, request_id: "prior-fence".into(), command: Operation::RegisterWorker { operation_id: "prior-fence".into(), worker: replacement.clone() } };
                let (fenced, _) = exchange(&client, &address, &fence, PRIOR_WIRE).await;
                assert!(matches!(&fenced.outcome, Outcome::Ok { data: Data::Worker(value) } if value == &replacement)); assert!(fenced.commit.is_some());
                let (committed_await, _) = exchange(&client, &address, &request, PRIOR_WIRE).await;
                assert!(matches!(&committed_await.outcome, Outcome::Refused { code, .. } if code == "stale_incarnation")); assert!(committed_await.commit.is_some());
                assert!(ready.send(Format9Proof { registration, registered, fence, fenced, awaiting: request, committed_await }).is_ok());
                format9_hold_until_worker_closes(&mut stream).await; return listener;
            }
            other => panic!("empty historical worker attempted unrelated operation: {other:?}"),
        }
    }
    panic!("historical worker exceeded finite producer request count");
}

#[tokio::test]
#[ignore = "explicit preserved wire18/worker14 executable and metadata required; root executes"]
async fn genuine_worker14_original_unknown_await_is_read_after15_admission_and_old_reader_refuses15() {
    let (old, metadata, current) = inputs(); let root = root("pr-worker14");
    let selected = root.join("prior-runtime"); retained_images::materialize(&old, &selected, true); assert_hash(&selected, PRIOR_IMAGE);
    let canonical = root.join("canonical"); std::fs::create_dir(&canonical).unwrap(); let db = canonical.join("tasks.sqlite");
    let address = address(); let mut controller = spawn_controller(&selected, &metadata, &db, &address, &root, "producer19"); ready(&mut controller, &root, "producer19").await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(); let origin = format!("http://{}", listener.local_addr().unwrap());
    let (tx, rx) = tokio::sync::oneshot::channel(); let proxy = tokio::spawn(producer_proxy(listener, address.clone(), tx));
    let workspace = root.join("workspace"); let mut worker = spawn_format9_worker(&selected, &metadata, &workspace, &origin, &root, "producer-fourteen");
    let proof = tokio::time::timeout(Duration::from_secs(10), rx).await.unwrap().unwrap(); worker.stop().await;
    let listener = tokio::time::timeout(Duration::from_secs(10), proxy).await.unwrap().unwrap();
    let state = workspace.join(".borg-worker").join(process::component("compatibility")); assert!(!state.join("worker-owner.json").exists());
    let before = inventory(&workspace); let marker = PathBuf::from(".borg-worker").join(process::component("compatibility")).join("format.json");
    assert_eq!(serde_json::from_slice::<Value>(&before[&marker].4).unwrap(), json!({"format_revision":14}));
    let registration_bytes = std::fs::read(state.join("mutations/registration.json")).unwrap();
    let awaiting_bytes = std::fs::read(state.join("mutations/await.json")).unwrap();
    let registration: Value = serde_json::from_slice(&registration_bytes).unwrap(); let awaiting: Value = serde_json::from_slice(&awaiting_bytes).unwrap();
    assert_eq!(registration["request"], serde_json::to_value(&proof.registration.command).unwrap());
    assert_eq!(registration["outcome"], serde_json::to_value(&proof.registered.outcome).unwrap());
    assert_eq!(registration["outcome_context"]["commit"], serde_json::to_value(&proof.registered.commit).unwrap());
    assert_eq!(awaiting["request"], serde_json::to_value(&proof.awaiting.command).unwrap());
    assert!(awaiting["outcome"].is_null() && awaiting["outcome_context"].is_null()); assert_eq!(awaiting["consumed"], false);
    controller.stop().await; owner_released(&db).await; assert_eq!(controller_version(&db), 19); let rows = canonical_rows(&db);
    assert_eq!(rows["receipts"].len(), 3); assert!(rows["tasks"].is_empty());
    let mut controller = spawn_controller(&current, &metadata, &db, &address, &root, "current20"); ready(&mut controller, &root, "current20").await;
    let await_id = proof.awaiting.command.mutation_id().unwrap().to_owned(); let recovery_address = address.clone();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let proxy = tokio::spawn(async move {
        let (mut stream, _) = tokio::time::timeout(Duration::from_secs(10), listener.accept()).await.unwrap().unwrap();
        let request = format9_read_request(&mut stream).await;
        assert!(matches!(&request.command, Operation::GetOperation { operation_id } if operation_id == &await_id), "current worker must recover the original pending Await before registering a successor");
        let client = reqwest::Client::builder().no_proxy().timeout(Duration::from_secs(5)).build().unwrap();
        let (reply, _) = exchange(&client, &recovery_address, &request, WIRE_REVISION).await;
        assert!(tx.send((request, reply)).is_ok()); format9_hold_until_worker_closes(&mut stream).await;
        assert!(tokio::time::timeout(Duration::from_millis(25), listener.accept()).await.is_err()); listener
    });
    let mut candidate = spawn_format9_worker(&current, &metadata, &workspace, &origin, &root, "candidate-fifteen");
    let (request, reply) = tokio::time::timeout(Duration::from_secs(10), rx).await.unwrap().unwrap();
    format9_assert_lookup(&reply, &proof.awaiting, &proof.committed_await); candidate.stop().await;
    let listener = tokio::time::timeout(Duration::from_secs(10), proxy).await.unwrap().unwrap();
    assert!(!state.join("worker-owner.json").exists()); let mut after = inventory(&workspace);
    let added_directory = PathBuf::from(".borg-worker").join(process::component("compatibility")).join("pull-requests");
    let additions = after.keys().filter(|path| !before.contains_key(*path)).cloned().collect::<Vec<_>>();
    assert_eq!(additions, vec![added_directory.clone()], "only the new empty PR journal directory may be created");
    assert!(after.remove(&added_directory).unwrap().0);
    assert_eq!(std::fs::read_dir(state.join("pull-requests")).unwrap().count(), 0);
    assert_eq!(before.keys().collect::<Vec<_>>(), after.keys().collect::<Vec<_>>());
    for (path, original) in &before {
        let actual = &after[path];
        if path == &marker {
            assert_eq!((actual.0,actual.1,actual.3),(original.0,original.1,original.3)); assert_ne!(actual.2, original.2);
            assert_eq!(serde_json::from_slice::<Value>(&actual.4).unwrap(), json!({"format_revision":17}));
        } else { assert_eq!(actual, original, "historical journal or owner identity changed at {}", path.display()); }
    }
    assert_eq!(std::fs::read(state.join("mutations/registration.json")).unwrap(), registration_bytes);
    assert_eq!(std::fs::read(state.join("mutations/await.json")).unwrap(), awaiting_bytes);
    let client = reqwest::Client::builder().no_proxy().timeout(Duration::from_secs(5)).build().unwrap();
    for (original, committed) in [(&proof.registration,&proof.registered),(&proof.fence,&proof.fenced),(&proof.awaiting,&proof.committed_await)] {
        let lookup = Request { protocol_revision: WIRE_REVISION, request_id: "current-original-lookup".into(), command: Operation::GetOperation { operation_id: original.command.mutation_id().unwrap().into() } };
        let (reply, _) = exchange(&client, &address, &lookup, WIRE_REVISION).await; format9_assert_lookup(&reply, original, committed);
    }
    let before_refusal = inventory(&workspace);
    let diagnostic = refused_worker(&selected, &metadata, &workspace, &origin, &root, "old14-refuses15").await;
    assert!(diagnostic.contains("unsupported worker state format; no migration performed"), "{diagnostic}"); assert_eq!(inventory(&workspace), before_refusal);
    assert!(tokio::time::timeout(Duration::from_millis(25), listener.accept()).await.is_err());
    controller.stop().await; owner_released(&db).await; assert_eq!(controller_version(&db), 21); assert_eq!(canonical_rows(&db), rows);
    std::fs::write(root.join("original-lookup-proof.json"), serde_json::to_vec_pretty(&json!({"request":request,"reply":reply,"reply_delivered":false,"original_await_posts":1,"candidate_mutations_forwarded":0})).unwrap()).unwrap();
    assert_hash(&old, PRIOR_IMAGE); assert_hash(&metadata, PRIOR_LIMITS);
}
