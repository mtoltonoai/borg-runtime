//! Actual Controller-open cases for current-schema startup and legacy migration.
//! Historical schema12 cases explicitly launch the pinned preserved executable.
//! No version stamping, schema downgrade, fabricated binding or valid-row setup.
//! The negative historical case corrupts one field only after the old owner exits.
use super::*;
use borg_runtime::collaboration::{documents::*, questions::*, work::*, workflow::*};
use std::collections::BTreeMap;

fn workflow_config() -> ControllerConfig {
    let mut cfg = config();
    let limits = Arc::make_mut(&mut cfg.limits);
    limits.workflow = Some(borg_runtime::limits::WorkflowLimits {
        record_bytes: 32_768, text_bytes: 4_096, options: 8,
        records: 512, revisions: 4_096, page_items: 8, page_bytes: 32_768,
        waits_per_task: 8, graph_scan_nodes: 512, events_per_mutation: 64,
        event_bytes: 8_192, notice_bytes: 8_192,
        pending_delivery_bytes: 32_768, delivery_scan_rows: 64,
    });
    limits.validate().unwrap();
    cfg
}
async fn reply(c: &Controller, command: Operation) -> Reply {
    let r = Box::pin(c.handle(Request { protocol_revision: WIRE_REVISION,
        request_id: format!("startup-request-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
        command: command.clone() })).await;
    r.validate_for(&command).unwrap();
    r
}
fn create_document(id: &str) -> Operation {
    Operation::MutateDocument { operation_id: id.into(), mutation: DocumentMutation::Create {
        content: DocumentContent { title: "Retained document".into(), body: "Exact retained workflow bytes".into() },
        supplied_content_id: None, responsible_owner: PrincipalRef::TrustedHostOperator,
    }}
}
fn assert_document_created(r: &Reply) {
    assert!(r.commit.is_some());
    assert!(matches!(r.outcome, Outcome::Ok { data: Data::DocumentMutation(DocumentMutationOutcome::Document { .. }) }));
}
fn schema_version(path: &std::path::Path) -> i64 {
    let db = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    db.query_row("PRAGMA user_version", [], |r| r.get(0)).unwrap()
}
// Finite, ordered projection of every canonical record class touched by these
// fixtures. Reads retain serialized bodies rather than re-encoding them.
fn records(path: &std::path::Path) -> BTreeMap<String, Vec<String>> {
    let db = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut values = BTreeMap::new();
    for (name, sql) in [
        ("schema", "SELECT json_array(type,name,tbl_name,sql) FROM sqlite_master ORDER BY type,name"),
        ("settings", "SELECT json_array(name,value) FROM settings ORDER BY name"),
        ("counters", "SELECT json_array(kind,value) FROM counters ORDER BY kind"),
        ("tasks", "SELECT json_array(id,seq,session,agent,executor,status,worker,incarnation,active,deadline,cleanup,body) FROM tasks ORDER BY id"),
        ("sessions", "SELECT json_array(id,seq,agent,task_id,body,memory_start_seq,memory_cursor_seq,memory_pending,workflow_delivery) FROM sessions ORDER BY id"),
        ("agents", "SELECT json_array(id,definition) FROM agents ORDER BY id"),
        ("definitions", "SELECT json_array(id,revision,body) FROM definitions ORDER BY id,revision"),
        ("history", "SELECT json_array(seq,task_id,revision,kind,body) FROM history ORDER BY seq"),
        ("receipts", "SELECT json_array(id,request,outcome,cleanup,controller_revision) FROM receipts ORDER BY id"),
        ("documents", "SELECT json_array(id,revision,created_product_revision,body) FROM workflow_documents ORDER BY id"),
        ("versions", "SELECT json_array(document_id,version,body) FROM workflow_document_versions ORDER BY document_id,version"),
        ("questions", "SELECT json_array(id,revision,created_product_revision,recipient,status,record_kind,record_id,body) FROM workflow_questions ORDER BY id"),
        ("question_actions", "SELECT json_array(question_id,question_revision,product_revision,body) FROM workflow_question_actions ORDER BY question_id,question_revision"),
        ("work_heads", "SELECT json_array(task_id,revision,body) FROM workflow_task_state ORDER BY task_id"),
        ("waits", "SELECT json_array(id,task_id,resolver_kind,resolver_id,closed,body) FROM workflow_waits ORDER BY id"),
        ("closures", "SELECT json_array(wait_id,body) FROM workflow_wait_closures ORDER BY wait_id"),
        ("events", "SELECT json_array(product_revision,ordinal,body,targets) FROM workflow_events ORDER BY product_revision,ordinal"),
    ] {
        // Schema17 has 125 objects; schema18 adds the diagnostic-credit table
        // and its three automatic indexes. Schema23 adds agent_retirements and
        // its PRIMARY KEY and UNIQUE automatic indexes; schemas19-22 add no SQL
        // objects. Schema24 adds nine net lineage table/index objects.
        // Keep every row and the overflow sentinel for these exact deltas.
        let maximum = if name == "schema" { 125 + 1 + 3 + 1 + 2 + 9 } else { 32 };
        let mut statement = db.prepare(&format!("{sql} LIMIT {}", maximum + 1)).unwrap();
        let rows = statement.query_map([], |r| r.get::<_, String>(0)).unwrap()
            .collect::<std::result::Result<Vec<_>, _>>().unwrap();
        assert!(rows.len() <= maximum, "fixture exceeds explicit per-table read bound: {name}");
        values.insert(name.into(), rows);
    }
    values
}
fn retained_bytes(path: &std::path::Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &std::path::Path, here: &std::path::Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(here).unwrap() {
            let path = entry.unwrap().path();
            let metadata = std::fs::symlink_metadata(&path).unwrap();
            if metadata.is_dir() { visit(root, &path, out); }
            else {
                assert!(metadata.is_file());
                assert!(out.len() < 64, "finite private fixture file-count bound");
                assert!(metadata.len() <= 64 * 1024 * 1024, "finite private fixture file-byte bound");
                out.insert(path.strip_prefix(root).unwrap().to_owned(), std::fs::read(path).unwrap());
            }
        }
    }
    let root = path.parent().unwrap();
    let mut out = BTreeMap::new();
    visit(root, root, &mut out);
    out
}

// Both cases below create their input with the current Controller (schema24).
// They qualify current-store reopen/refusal, not a historical schema16 migration.
#[tokio::test]
async fn valid_current_schema_reopens_preserving_canonical_records_and_original_receipt() {
    let p = path();
    let cfg = workflow_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let source = submit(&c, "startup-source", native("startup-source-owner")).await;
    let waiter = submit(&c, "startup-waiter", native("startup-waiter-owner")).await;
    let document_operation = create_document("startup-document");
    let document = reply(&c, document_operation.clone()).await;
    assert_document_created(&document);
    let question = match call(&c, Operation::MutateQuestion { operation_id: "startup-question".into(),
        mutation: QuestionMutation::Pose { question: PoseQuestion {
            record: RecordRef::Task { task_id: source.id.clone() }, recipient: PrincipalRef::TrustedHostOperator,
            prompt: "Choose one option. Recommended: keep.".into(),
            options: vec![
                QuestionOption { id: "keep".into(), label: "Keep".into(), explanation: "Keep this choice".into() },
                QuestionOption { id: "change".into(), label: "Change".into(), explanation: "Use the alternative".into() },
            ], recommended_option_id: "keep".into(),
        } } }).await {
        Outcome::Ok { data: Data::QuestionMutation(value) } => value.question.question,
        other => panic!("question expected: {other:?}"),
    };
    let waited = call(&c, Operation::MutateWork { operation_id: "startup-wait".into(), mutation: WorkMutation::AddWait {
        task_id: waiter.id.clone(), expected_revision: 0, resolver: ResolverRef::Question { question_id: question.id.clone() },
    }}).await;
    assert!(matches!(waited, Outcome::Ok { data: Data::WorkMutation(_) }));
    let closed = call(&c, Operation::MutateQuestion { operation_id: "startup-close-question".into(),
        mutation: QuestionMutation::Cancel { question_id: question.id.clone(), expected_revision: question.revision,
            reason: "Original requirement withdrawn".into() } }).await;
    assert!(matches!(closed, Outcome::Ok { data: Data::QuestionMutation(_) }));
    let source_before = get(&c, &source.id).await;
    let waiter_before = get(&c, &waiter.id).await;
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(schema_version(&p), 24, "fresh Controller uses the joined schema");
    let before = records(&p);
    for name in ["tasks", "sessions", "agents", "definitions", "history", "receipts", "documents", "versions",
        "questions", "question_actions", "work_heads", "waits", "closures", "events"] {
        assert!(!before[name].is_empty(), "real producer must populate {name}");
    }
    for _ in 0..2 {
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        assert_eq!(get(&c, &source.id).await, source_before);
        assert_eq!(get(&c, &waiter.id).await, waiter_before);
        let replay = reply(&c, document_operation.clone()).await;
        assert_eq!(replay.outcome, document.outcome);
        assert_eq!(replay.commit, document.commit);
        let lookup = reply(&c, Operation::GetOperation { operation_id: "startup-document".into() }).await;
        let original = match lookup.outcome {
            Outcome::Ok { data: Data::Operation(value) } => value,
            other => panic!("original receipt expected: {other:?}"),
        };
        assert_eq!(serde_json::from_value::<Outcome>(original.outcome.unwrap()).unwrap(), document.outcome);
        assert_eq!(lookup.commit, document.commit);
        drop(c);
        controller_owner_released(&p).await;
        assert_eq!(schema_version(&p), 24);
        assert_eq!(records(&p), before, "valid current-schema startup and original replay do not rewrite canonical records");
    }
}

#[tokio::test]
async fn nonempty_current_schema_refuses_absent_workflow_policy_without_rewriting_any_file() {
    let p = path();
    let cfg = workflow_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    // No Task or accepted execution-policy snapshot exists in this fixture.
    // Refusal must protect the stored workflow domain, not just a V2 task profile.
    let operation = create_document("policy-retained-document");
    let created = reply(&c, operation.clone()).await;
    assert_document_created(&created);
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(schema_version(&p), 24);
    let before_records = records(&p);
    assert!(before_records["tasks"].is_empty());
    assert!(!before_records["documents"].is_empty());
    assert!(!before_records["versions"].is_empty());
    assert!(!before_records["events"].is_empty());
    let before_bytes = retained_bytes(&p);
    let mut absent = cfg.clone();
    Arc::make_mut(&mut absent.limits).workflow = None;
    absent.limits.validate().unwrap();
    for _ in 0..2 {
        assert!(Controller::open_with_config(&p, absent.clone()).is_err(),
            "nonempty workflow records require explicit selected policy");
        controller_owner_released(&p).await;
        assert_eq!(retained_bytes(&p), before_bytes, "policy refusal preserves every original file byte");
    }
    let c = Controller::open_with_config(&p, cfg).unwrap();
    let replay = reply(&c, operation).await;
    assert_eq!(replay.outcome, created.outcome);
    assert_eq!(replay.commit, created.commit);
    drop(c);
    controller_owner_released(&p).await;
    assert_eq!(schema_version(&p), 24);
    assert_eq!(records(&p), before_records);
}


// Reuse the existing test image retention helper. Production has no old-schema
// switch: only the preserved historical executable creates the schema12 store.
#[path = "../support/retained_images.rs"]
mod historical_images;
const HISTORICAL_COMMIT: &str = "8b4e826ec6616327678062945b3ef4d132c60954";
const HISTORICAL_IMAGE_SHA256: &str = "6dfb55597ee8153df99138e457e5483cb683f4f8ba68a4ca96138c28ce5008aa";
fn assert_file_hash(path: &std::path::Path, expected: &str) {
    let result = std::process::Command::new("/usr/bin/sha256sum")
        .arg("--").arg(path).output().unwrap();
    assert!(result.status.success(), "hash read failed: {}", path.display());
    assert_eq!(String::from_utf8(result.stdout).unwrap().split_whitespace().next(), Some(expected),
        "historical fixture input changed: {}", path.display());
}
fn historical_package() -> (PathBuf, Vec<u8>, Arc<borg_runtime::limits::RuntimeLimits>) {
    let image = PathBuf::from(std::env::var_os("BORG_RECOVERY_BINARY")
        .expect("explicit pinned historical-image selection required"));
    assert!(image.is_absolute());
    // This action selects the preserved package layout, not an arbitrary binary
    // with no bound historical source/metadata evidence.
    assert_eq!(image.file_name().unwrap(), "borg-runtime");
    let release = image.parent().unwrap();
    assert_eq!(release.file_name().unwrap(), "release");
    let target = release.parent().unwrap();
    assert_eq!(target.file_name().unwrap(), "target");
    let package = target.parent().unwrap();
    assert_file_hash(&image, HISTORICAL_IMAGE_SHA256);
    assert_file_hash(&package.join("borg-runtime.tar"), "692385c71f71f03c5914dfe763fc6175e5533517b94076ddd823aaf7730dbcf8");
    assert_file_hash(&package.join("manifest.json"), "3d6b89e86982ecf1f4d0bfa5a08ba6c90bfb1c48c89ecb46c31c72f085ac6a71");
    let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(package.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["source"]["commit"], HISTORICAL_COMMIT);
    for (relative, expected) in [
        ("src/controller/database.rs", "8ee350c51c50385ffed0aeebe766ec363aff58aaaf9750bbe71e8a07fb7d8a79"),
        ("src/controller/schema12.sql", "1da12b5be88b886b65a756d7463520eb99ece3413e73dfdcec63c2c203270665"),
        ("src/protocol.rs", "9552a6ea8c5ff29b974e3433a9fa07b42ef96c8be60128e329c95ce02cdb959f"),
        ("metadata/limits.json", "36ae8e816c602c86bde7a90b3faa552e77e95af87432c4a34855ddd9fe683e34"),
    ] {
        assert_eq!(manifest["source"]["files"][relative]["sha256"], expected);
        assert_file_hash(&package.join("source").join(relative), expected);
    }
    let bytes = std::fs::read(package.join("source/metadata/limits.json")).unwrap();
    let limits = Arc::new(borg_runtime::limits::RuntimeLimits::parse(&bytes).unwrap());
    assert!(limits.workflow.is_none());
    (image, bytes, limits)
}

// Fixture-only old wire11 request envelope. The production ApiClient always
// uses the current wire revision and must never silently negotiate backwards.
struct HistoricalClient {
    origin: String,
    client: reqwest::Client,
    limits: Arc<borg_runtime::limits::RuntimeLimits>,
}
impl HistoricalClient {
    fn new(origin: &str, limits: Arc<borg_runtime::limits::RuntimeLimits>) -> Self {
        let url=reqwest::Url::parse(origin).unwrap();
        assert_eq!(url.scheme(), "http");assert_eq!(url.host_str(),Some("127.0.0.1"));
        assert!(url.username().is_empty() && url.password().is_none());
        assert!(url.query().is_none() && url.fragment().is_none());
        Self { origin: origin.into(), limits, client: reqwest::Client::builder()
            .no_proxy().redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(1)).timeout(Duration::from_secs(10))
            .build().unwrap() }
    }
    async fn call(&self, command: Operation) -> Result<Reply,String> {self.call_at(command,11).await}
    async fn call_at(&self, command: Operation, protocol_revision:u32) -> Result<Reply,String> {
        let request=Request { protocol_revision,
            request_id:format!("historical-wire11-{}",NEXT.fetch_add(1,Ordering::Relaxed)), command };
        let bytes=serde_json::to_vec(&request).map_err(|e|e.to_string())?;
        if bytes.len()>self.limits.transport.body_bytes {return Err("historical request bound".into());}
        let mut response=self.client.post(format!("{}/api/operations",self.origin))
            .header("content-type","application/json").body(bytes).send().await.map_err(|e|e.to_string())?;
        if !response.status().is_success() {return Err(format!("historical HTTP {}",response.status()));}
        let mut bytes=Vec::new();
        while let Some(chunk)=response.chunk().await.map_err(|e|e.to_string())? {
            if bytes.len().saturating_add(chunk.len())>self.limits.transport.body_bytes {return Err("historical reply bound".into());}
            bytes.extend_from_slice(&chunk);
        }
        let reply:Reply=serde_json::from_slice(&bytes).map_err(|e|e.to_string())?;
        if reply.request_id!=request.request_id {return Err("historical request identity".into());}
        reply.validate_for(&request.command)?;Ok(reply)
    }
}

struct HistoricalProducer(std::process::Child);
impl HistoricalProducer {
    async fn stop(mut self) {
        let signalled = std::process::Command::new("/bin/kill")
            .args(["-INT", "--", &self.0.id().to_string()]).status().unwrap();
        assert!(signalled.success());
        let status = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(status) = self.0.try_wait().unwrap() { return status; }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await.expect("historical Controller must exit within its fixture bound");
        assert!(status.success(), "historical Controller exited unsuccessfully: {status}");
    }
}
impl Drop for HistoricalProducer {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(Some(_))) { return; }
        // Panic/timeout cleanup owns exactly the child this fixture spawned.
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
struct HistoricalFixture {
    config: ControllerConfig,
    tasks: Vec<Task>,
    originals: Vec<(Operation, Reply)>,
}
async fn historical_fixture(p: &std::path::Path) -> HistoricalFixture {
    let (selected, metadata, limits) = historical_package();
    assert!(!p.exists(), "old Controller creates the final database path itself");
    let root = p.parent().unwrap();
    let image = root.join("historical-borg-runtime");
    historical_images::materialize(&selected, &image, true);
    assert_file_hash(&image, HISTORICAL_IMAGE_SHA256);
    let metadata_path = root.join("historical-limits.json");
    std::fs::write(&metadata_path, metadata).unwrap();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);
    let origin = format!("http://{address}");
    let mut old = HistoricalProducer(std::process::Command::new(&image)
        .arg("controller").arg("--limits").arg(&metadata_path)
        .arg("--db").arg(p).arg("--listen").arg(address.to_string())
        .arg("--origin").arg(&origin)
        .stdin(std::process::Stdio::null())
        .stdout(std::fs::File::create(root.join("historical.stdout")).unwrap())
        .stderr(std::fs::File::create(root.join("historical.stderr")).unwrap())
        .spawn().unwrap());
    let client = HistoricalClient::new(&origin, limits.clone());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert!(old.0.try_wait().unwrap().is_none(), "historical producer exited during startup");
            if let Ok(r) = client.call(Operation::ListTasks { after: None, limit: 1 }).await {
                assert!(matches!(r.outcome, Outcome::Ok { .. }));
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }).await.expect("historical Controller readiness exceeded fixture bound");
    let mut tasks = Vec::new();
    let mut originals = Vec::new();
    for (index, agent) in ["historical-owner-one", "historical-owner-two"].into_iter().enumerate() {
        let mut input = native(agent);
        input.limits.result_bytes = limits.task_defaults.result_bytes;
        let operation = Operation::SubmitTask { operation_id: format!("historical-submit-{index}"), input };
        let r = tokio::time::timeout(Duration::from_secs(10), client.call(operation.clone())).await.unwrap().unwrap();
        assert!(r.commit.is_some());
        tasks.push(task(r.outcome.clone()));
        originals.push((operation, r));
    }
    let cancellation = Operation::CancelTask { operation_id: "historical-cancel-zero".into(),
        task_id: tasks[0].id.clone(), expected_revision: tasks[0].revision };
    let cancelled = tokio::time::timeout(Duration::from_secs(10), client.call(cancellation.clone())).await.unwrap().unwrap();
    tasks[0] = task(cancelled.outcome.clone());
    assert_eq!(tasks[0].status, TaskStatus::Cancelled);
    assert!(tasks[0].attempt.is_none());
    assert_eq!(tasks[1].status, TaskStatus::Queued);
    originals.push((cancellation, cancelled));
    old.stop().await;
    controller_owner_released(p).await;
    assert_eq!(schema_version(p), 12, "the pinned historical producer created genuine schema12");
    assert_historical_schema(p);
    let mut current = config();
    current.limits = limits;
    HistoricalFixture { config: current, tasks, originals }
}
fn assert_historical_schema(p: &std::path::Path) {
    let db = rusqlite::Connection::open_with_flags(p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let workflow_objects: i64 = db.query_row("SELECT count(*) FROM sqlite_master WHERE name LIKE 'workflow_%'", [], |r| r.get(0)).unwrap();
    let delivery_columns: i64 = db.query_row("SELECT count(*) FROM pragma_table_info('sessions') WHERE name='workflow_delivery'", [], |r| r.get(0)).unwrap();
    let revision_settings: i64 = db.query_row("SELECT count(*) FROM settings WHERE name='workflow_revision'", [], |r| r.get(0)).unwrap();
    assert_eq!((workflow_objects, delivery_columns, revision_settings), (0, 0, 0));
}
// Read every original schema12 table, including empty classes. The only
// exclusions are the additive workflow setting/delivery column and schema14 audit column.
// SQLite Values retain original canonical TEXT without decoding/re-encoding it.
fn historical_records(p: &std::path::Path) -> BTreeMap<String, Vec<Vec<rusqlite::types::Value>>> {
    let db = rusqlite::Connection::open_with_flags(p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut result = BTreeMap::new();
    for table in ["counters", "settings", "tasks", "sessions", "agents", "definitions", "workers",
        "retired_incarnations", "attempts", "commands", "receipts", "external_operations", "history", "waits",
        "memories", "releases", "protected_receipts", "protected_actions", "repositories", "repository_revisions", "sqlite_sequence"] {
        let select = match table {
            "settings" => "SELECT name,value FROM settings WHERE name!='workflow_revision'".to_owned(),
            "sessions" => "SELECT id,seq,agent,task_id,body,memory_start_seq,memory_cursor_seq,memory_pending FROM sessions".to_owned(),
            "receipts" => "SELECT id,request,outcome,cleanup,controller_revision FROM receipts".to_owned(),
            _ => format!("SELECT * FROM {table}"),
        };
        let mut statement = db.prepare(&format!("{select} ORDER BY 1,2 LIMIT 33")).unwrap();
        let columns = statement.column_count();
        let values = statement.query_map([], |r| (0..columns).map(|n| r.get(n)).collect::<rusqlite::Result<Vec<rusqlite::types::Value>>>()).unwrap()
            .collect::<rusqlite::Result<Vec<_>>>().unwrap();
        assert!(values.len() <= 32, "historical fixture exceeded read bound: {table}");
        result.insert(table.into(), values);
    }
    result
}
fn owner_identity(p: &std::path::Path) -> (Vec<(u64, u64)>, Vec<u8>) {
    use std::os::unix::fs::MetadataExt;
    let registry = controller_sidecar(p, ".native-helpers");
    let files = [p.to_owned(), controller_sidecar(p, ".controller-lock"), registry.clone()];
    let identities = files.iter().map(|path| {
        let m = std::fs::symlink_metadata(path).unwrap();
        assert!(!m.file_type().is_symlink());
        (m.dev(), m.ino())
    }).collect();
    (identities, std::fs::read(registry.join("owner.binding")).unwrap())
}
fn assert_empty_workflow_upgrade(p: &std::path::Path) {
    let db = rusqlite::Connection::open_with_flags(p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    assert_eq!(db.query_row("SELECT value FROM settings WHERE name='workflow_revision'", [], |r| r.get::<_, String>(0)).unwrap(), "0");
    assert_eq!(db.query_row("SELECT count(*) FROM sessions WHERE workflow_delivery IS NOT NULL", [], |r| r.get::<_, i64>(0)).unwrap(), 0);
    for table in ["workflow_questions", "workflow_answers", "workflow_question_actions", "workflow_documents",
        "workflow_document_versions", "workflow_comments", "workflow_reviews", "workflow_review_decisions",
        "workflow_review_withdrawals", "workflow_task_state", "workflow_waits", "workflow_wait_closures",
        "workflow_wait_consumptions", "workflow_events"] {
        let count: i64 = db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0)).unwrap();
        assert_eq!(count, 0, "migration must not fabricate historical workflow records: {table}");
    }
}

fn assert_empty_application_upgrade(p: &std::path::Path) {
    let db=rusqlite::Connection::open_with_flags(p,rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    assert_eq!(db.query_row("SELECT count(*) FROM receipts WHERE governance IS NOT NULL",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    for table in ["charter_sessions","charter_session_versions","charter_requests","charter_replies",
        "charter_closures","charter_compaction_results","governance_rules","governance_configurations",
        "governance_state","governance_holds","native_request_coordination",
        "charter_output_chunks","native_request_diagnostic_credits"] {
        let n:i64=db.query_row(&format!("SELECT count(*) FROM {table}"),[],|r|r.get(0)).unwrap();
        assert_eq!(n,0,"migration must not fabricate historical application records: {table}");
    }
}

#[tokio::test]
#[ignore = "historical-image explicit required: pinned BORG_RECOVERY_BINARY package"]
async fn faithful_schema12_upgrades_in_place_preserving_original_records_and_receipts() {
    let p = path();
    let fixture = historical_fixture(&p).await;
    let before = historical_records(&p);
    let identity = owner_identity(&p);
    for table in ["tasks", "sessions", "agents", "definitions", "history", "receipts"] {
        assert!(!before[table].is_empty(), "actual historical API must populate {table}");
    }
    for _ in 0..2 {
        let current = Controller::open_with_config(&p, fixture.config.clone()).unwrap();
        assert_eq!(owner_identity(&p), identity, "original database/registry/lock and binding retained");
        for t in &fixture.tasks { assert_eq!(get(&current, &t.id).await, *t); }
        for (operation, original) in &fixture.originals {
            let replay = reply(&current, operation.clone()).await;
            assert_eq!(replay.outcome, original.outcome);
            assert_eq!(replay.commit, original.commit);
            let lookup = reply(&current, Operation::GetOperation {
                operation_id: operation.mutation_id().unwrap().into(),
            }).await;
            let recorded = match lookup.outcome {
                Outcome::Ok { data: Data::Operation(value) } => value,
                other => panic!("historical original receipt expected: {other:?}"),
            };
            assert_eq!(serde_json::from_value::<Outcome>(recorded.outcome.unwrap()).unwrap(), original.outcome);
            assert_eq!(lookup.commit, original.commit);
        }
        drop(current);
        controller_owner_released(&p).await;
        assert_eq!(schema_version(&p), 24);
        assert_eq!(historical_records(&p), before);
        assert_empty_workflow_upgrade(&p);
        assert_empty_application_upgrade(&p);
    }
}

#[tokio::test]
#[ignore = "historical-image explicit required: pinned BORG_RECOVERY_BINARY package"]
async fn corrupt_schema12_session_refuses_before_migration_without_rewriting_any_file() {
    let p = path();
    let fixture = historical_fixture(&p).await;
    let identity = owner_identity(&p);
    {
        let db = rusqlite::Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE).unwrap();
        assert_eq!(db.execute("UPDATE sessions SET memory_pending=?2 WHERE id=?1",
            rusqlite::params![fixture.tasks[0].session_id, "{\"unsupported_historical_memory_batch\":true}"]).unwrap(), 1);
    }
    assert_eq!(schema_version(&p), 12);
    assert_historical_schema(&p);
    let before_records = historical_records(&p);
    let before_bytes = retained_bytes(&p);
    for _ in 0..2 {
        let error = match Controller::open_with_config(&p, fixture.config.clone()) {
            Err(error) => error,
            Ok(current) => { drop(current); panic!("corrupt historical session must not be migrated"); }
        };
        assert!(error.contains("session/task association"), "expected historical session validation: {error}");
        controller_owner_released(&p).await;
        assert_eq!(retained_bytes(&p), before_bytes, "refused startup preserves full original file bytes");
        assert_eq!(owner_identity(&p), identity);
        assert_eq!(schema_version(&p), 12);
        assert_historical_schema(&p);
        assert_eq!(historical_records(&p), before_records);
        assert_eq!(retained_bytes(&p), before_bytes);
    }
}

#[path = "collaboration_schema13_migration.rs"]
mod schema13;

#[path = "charter_schema19_migration.rs"]
mod charter_schema19_migration;
