//! Explicit historical executable qualification; no historical reader is
//! reconstructed in the new binary. The root runner selects preserved inputs.
#[path = "support/retained_images.rs"]
mod retained_images;

use borg_runtime::worker::process;
use std::{collections::BTreeMap, path::{Path, PathBuf}, time::Duration};

const OLD_IMAGE_SHA256: &str = "6769d613628e4597f05e6352854318b5c123b835d1ee07311bf78f6b5a5e945a";
const OLD_METADATA_SHA256: &str = "c98a8e12070a1f8b7c89fc26d701ebe5bb30ca2e1a05a295683496ee0d904a77";

fn assert_hash(path: &Path, expected: &str) {
    assert!(path.is_absolute());
    assert!(std::fs::symlink_metadata(path).unwrap().file_type().is_file());
    let output = std::process::Command::new("/usr/bin/sha256sum")
        .arg("--").arg(path).output().unwrap();
    assert!(output.status.success());
    assert_eq!(String::from_utf8(output.stdout).unwrap().split_whitespace().next(), Some(expected));
}

type Inventory = BTreeMap<PathBuf, (bool, u64, u64, u32, Vec<u8>)>;
fn inventory(root: &Path) -> Inventory {
    use std::os::unix::fs::MetadataExt;
    fn visit(root: &Path, path: &Path, result: &mut Inventory) {
        let metadata = std::fs::symlink_metadata(path).unwrap();
        assert!(metadata.is_file() || metadata.is_dir());
        result.insert(path.strip_prefix(root).unwrap().into(), (
            metadata.is_dir(), metadata.dev(), metadata.ino(), metadata.mode(),
            if metadata.is_file() { std::fs::read(path).unwrap() } else { Vec::new() },
        ));
        if metadata.is_dir() {
            for entry in std::fs::read_dir(path).unwrap() { visit(root, &entry.unwrap().path(), result); }
        }
    }
    let mut result = BTreeMap::new();
    visit(root, root, &mut result);
    result
}

async fn refused_worker(
    image: &Path, metadata: &Path, workspace: &Path, origin: &str, log_root: &Path, label: &str,
) -> String {
    let stdout = log_root.join(format!("{label}.stdout"));
    let stderr = log_root.join(format!("{label}.stderr"));
    let mut child = tokio::process::Command::new(image)
        .arg("worker").arg("--limits").arg(metadata)
        .arg("--controller").arg(origin).arg("--id").arg("compatibility")
        .arg("--incarnation").arg(label).arg("--workspace-root").arg(workspace)
        .arg("--codex").arg("/must-not-launch")
        .arg("--native-endpoint").arg("http://127.0.0.1:1/no-provider")
        .arg("--resource-profile").arg(log_root.join("missing-profile.json"))
        .env_clear().env("PATH", "/usr/bin:/bin")
        .stdin(std::process::Stdio::null())
        .stdout(std::fs::File::create(stdout).unwrap())
        .stderr(std::fs::File::create(&stderr).unwrap())
        .kill_on_drop(true).spawn().unwrap();
    let status = match tokio::time::timeout(Duration::from_secs(10), child.wait()).await {
        Ok(result) => result.unwrap(),
        Err(_) => {
            child.kill().await.unwrap();
            let _ = child.wait().await.unwrap();
            panic!("historical compatibility refusal did not terminate within the fixture bound");
        }
    };
    assert!(!status.success());
    std::fs::read_to_string(stderr).unwrap()
}

#[tokio::test]
#[ignore = "explicit preserved format11 executable and metadata required; root must execute this qualification"]
async fn historical_format11_runtime_refuses_new_format12_before_owner_or_controller_effects() {
    let old = PathBuf::from(std::env::var_os("BORG_WORKER_FORMAT11_BINARY")
        .expect("select the preserved actual pre-feature format11 executable"));
    let metadata = PathBuf::from(std::env::var_os("BORG_WORKER_FORMAT11_LIMITS")
        .expect("select exact pre-feature metadata"));
    assert_hash(&old, OLD_IMAGE_SHA256);
    assert_hash(&metadata, OLD_METADATA_SHA256);
    let root = retained_images::artifact_root().join(format!("worker-state-compatibility-{}-{}",
        std::process::id(), process::now_ms()));
    std::fs::create_dir(&root).unwrap();
    let selected_old = root.join("historical-runtime");
    retained_images::materialize(&old, &selected_old, true);
    assert_hash(&selected_old, OLD_IMAGE_SHA256);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());

    // Positive old-reader control: it accepts exactly11 and gets as far as the
    // deliberately missing resource profile, without launching any executor.
    let old_workspace = root.join("old-workspace");
    let old_state = old_workspace.join(".borg-worker").join(process::component("compatibility"));
    process::private_directory(&old_state).unwrap();
    let marker = b"{\n  \"format_revision\": 11\n}\n";
    std::fs::write(old_state.join("format.json"), marker).unwrap();
    let diagnostic = refused_worker(&selected_old, &metadata, &old_workspace, &origin, &root, "old-eleven").await;
    assert!(diagnostic.contains("resource profile unavailable"), "unexpected old control refusal: {diagnostic}");
    assert_eq!(std::fs::read(old_state.join("format.json")).unwrap(), marker);

    // The new process, not a test-only old-reader switch, produces the new
    // marker. Resource refusal keeps this test outside model/tool execution.
    let new_workspace = root.join("new-workspace");
    let new_image = Path::new(env!("CARGO_BIN_EXE_borg-runtime"));
    let diagnostic = refused_worker(new_image, &metadata, &new_workspace, &origin, &root, "new-fifteen").await;
    assert!(diagnostic.contains("resource profile unavailable"), "unexpected new control refusal: {diagnostic}");
    let new_state = new_workspace.join(".borg-worker").join(process::component("compatibility"));
    assert_eq!(serde_json::from_slice::<serde_json::Value>(&std::fs::read(new_state.join("format.json")).unwrap()).unwrap(),
        serde_json::json!({"format_revision":17}));
    std::fs::write(new_state.join("original-call-sentinel"), b"original evidence remains unchanged").unwrap();
    let before = inventory(&new_state);
    let diagnostic = refused_worker(&selected_old, &metadata, &new_workspace, &origin, &root, "old-refuses-fifteen").await;
    assert!(diagnostic.contains("unsupported worker state format; no migration performed"),
        "old executable must refuse the version marker itself: {diagnostic}");
    assert_eq!(inventory(&new_state), before, "old process must not touch owner, journal, policy or marker files");
    assert!(tokio::time::timeout(Duration::from_millis(25), listener.accept()).await.is_err(),
        "format/profile refusal must happen before any controller connection");
    assert_hash(&old, OLD_IMAGE_SHA256);
    assert_hash(&metadata, OLD_METADATA_SHA256);
}

struct HistoricalController(std::process::Child);
impl HistoricalController {
    async fn stop(&mut self) {
        assert!(std::process::Command::new("/bin/kill").args(["-INT", "--", &self.0.id().to_string()]).status().unwrap().success());
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(status)=self.0.try_wait().unwrap() { assert!(status.success());break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("historical controller must close within the original fixture bound");
    }
}
impl Drop for HistoricalController {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(),Ok(Some(_))) { return; }
        let _=self.0.kill();let _=self.0.wait();
    }
}
fn spawn_controller(image:&Path, metadata:&Path, db:&Path, address:&str, logs:&Path, label:&str)->HistoricalController {
    HistoricalController(std::process::Command::new(image)
        .arg("controller").arg("--limits").arg(metadata).arg("--db").arg(db)
        .arg("--listen").arg(address).arg("--origin").arg(format!("http://{address}"))
        .env_clear().env("PATH","/usr/bin:/bin").stdin(std::process::Stdio::null())
        .stdout(std::fs::File::create(logs.join(format!("{label}.stdout"))).unwrap())
        .stderr(std::fs::File::create(logs.join(format!("{label}.stderr"))).unwrap()).spawn().unwrap())
}
async fn owner_released(db:&Path) {
    let lock=PathBuf::from(format!("{}.controller-lock",db.display()));
    tokio::time::timeout(Duration::from_secs(3),async {
        loop {
            let file=std::fs::OpenOptions::new().read(true).write(true).open(&lock).unwrap();
            if file.try_lock().is_ok() { break; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.expect("canonical database owner must close before raw fixture reads");
}
fn canonical_rows(db:&Path)->BTreeMap<String,Vec<Vec<rusqlite::types::Value>>> {
    let c=rusqlite::Connection::open_with_flags(db,rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut names=c.prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name LIMIT 129").unwrap();
    let tables=names.query_map([],|r|r.get::<_,String>(0)).unwrap().collect::<Result<Vec<_>,_>>().unwrap();
    assert!(tables.len()<=128);
    let mut result=BTreeMap::new();
    for table in tables {
        let mut q=c.prepare(&format!("SELECT * FROM \"{}\" LIMIT 129",table.replace('"',"\"\""))).unwrap();
        let width=q.column_count();
        let mut rows=q.query_map([],|r|(0..width).map(|i|r.get(i)).collect::<Result<Vec<rusqlite::types::Value>,_>>()).unwrap().collect::<Result<Vec<_>,_>>().unwrap();
        assert!(rows.len()<=128);rows.sort_by_key(|r|format!("{r:?}"));result.insert(table,rows);
    }
    result
}
fn controller_version(db:&Path)->i64 {
    let c=rusqlite::Connection::open_with_flags(db,rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    c.query_row("PRAGMA user_version",[],|r|r.get(0)).unwrap()
}
async fn old_call(client:&reqwest::Client,address:&str,command:borg_runtime::protocol::Operation)->borg_runtime::protocol::Reply {
    use borg_runtime::protocol::*;
    let request=Request {protocol_revision:13,request_id:"historical-schema15-request".into(),command:command.clone()};
    let mut response=client.post(format!("http://{address}/api/operations")).header("content-type","application/json").body(serde_json::to_vec(&request).unwrap()).send().await.unwrap();
    assert!(response.status().is_success());
    let mut body=Vec::new();while let Some(chunk)=response.chunk().await.unwrap() {assert!(body.len()+chunk.len()<=262144);body.extend_from_slice(&chunk);}
    let reply:Reply=serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.request_id,request.request_id);reply.validate_for(&command).unwrap();
    // The Request uses the fixed historical wire; the original outcome and
    // commit are compared after replay through the new current wire.
    assert!(matches!(reply.outcome,Outcome::Ok{..}),"{reply:?}");reply
}

#[tokio::test]
#[ignore = "explicit preserved schema15 executable and metadata required; root executes this compatibility qualification"]
async fn genuine_schema15_migrates_in_place_and_old_reader_refuses_schema16_without_writes() {
    use borg_runtime::{controller::{Controller,ControllerConfig},protocol::*};
    let old=PathBuf::from(std::env::var_os("BORG_WORKER_FORMAT11_BINARY").unwrap());
    let metadata=PathBuf::from(std::env::var_os("BORG_WORKER_FORMAT11_LIMITS").unwrap());
    assert_hash(&old,OLD_IMAGE_SHA256);assert_hash(&metadata,OLD_METADATA_SHA256);
    let limits=std::sync::Arc::new(borg_runtime::limits::RuntimeLimits::parse(&std::fs::read(&metadata).unwrap()).unwrap());
    for corrupt in [false,true] {
        let root=retained_images::artifact_root().join(format!("schema16-compatibility-{}-{}-{corrupt}",std::process::id(),process::now_ms()));
        std::fs::create_dir(&root).unwrap();let state=root.join("state");std::fs::create_dir(&state).unwrap();
        let db=state.join("tasks.sqlite");let selected_old=root.join("historical-runtime");
        retained_images::materialize(&old,&selected_old,true);assert_hash(&selected_old,OLD_IMAGE_SHA256);
        let listener=std::net::TcpListener::bind("127.0.0.1:0").unwrap();let address=listener.local_addr().unwrap().to_string();drop(listener);
        let mut child=spawn_controller(&selected_old,&metadata,&db,&address,&root,"producer15");
        let client=reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();
        tokio::time::timeout(Duration::from_secs(10),async {
            loop {
                assert!(child.0.try_wait().unwrap().is_none());
                if std::fs::read_to_string(root.join("producer15.stdout")).unwrap().contains("Controller listening on") { break; }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await.expect("historical controller readiness");
        let input=TaskInput { instruction_source: None, pull_request: None, resolved_limits:None,repository_read:None,instruction:"retain original receipt".into(),
            definition:Definition {charter:None,limit_overrides:None,execution_profile:Some(ExecutionProfile::CollaborationV1{web_search:None}),inference:Some(InferenceSettings::selected()),id:"schema15-owner".into(),revision:"original-1".into(),executor:ExecutorKind::Native},
            repository:None,acceptance_commands:vec![],output_schema:Some(serde_json::json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false})),
            limits:Limits {deadline_unix_ms:process::now_ms()+600000,model_requests:3,output_bytes:16384,result_bytes:limits.task_defaults.result_bytes,max_delegation_depth:4},dependencies:vec![],native_workflow:None};
        let submit=Operation::SubmitTask {operation_id:"schema15-submit".into(),input: (input).into()};
        let submitted=old_call(&client,&address,submit.clone()).await;
        let task=match &submitted.outcome {Outcome::Ok{data:Data::Task(t)}=>t,_=>panic!("{submitted:?}")};
        let cancel=Operation::CancelTask {operation_id:"schema15-cancel".into(),task_id:task.id.clone(),expected_revision:task.revision};
        let cancelled=old_call(&client,&address,cancel.clone()).await;
        child.stop().await;owner_released(&db).await;assert_eq!(controller_version(&db),15);
        let cfg=ControllerConfig {limits:limits.clone(),validation_executable:PathBuf::from(env!("CARGO_BIN_EXE_borg-runtime")),..ControllerConfig::default()};
        if corrupt {
            // The version barrier must not skip existing15 domain validation.
            let c=rusqlite::Connection::open(&db).unwrap();
            c.execute("INSERT INTO charter_sessions(id,revision,body) VALUES('invalid',1,'{}')",[]).unwrap();drop(c);
            let before=inventory(&state);
            let error=match Controller::open_with_config(&db,cfg) {Ok(_)=>panic!("corrupt15 was admitted"),Err(e)=>e};
            assert!(error.contains("invalid stored charter state"),"{error}");owner_released(&db).await;
            assert_eq!(inventory(&state),before);assert_eq!(controller_version(&db),15);continue;
        }
        let before=canonical_rows(&db);let identities=inventory(&state);
        let c=Controller::open_with_config(&db,cfg).unwrap();
        for (command,original) in [(submit,submitted),(cancel,cancelled)] {
            let replay=Box::pin(c.handle(Request {protocol_revision:WIRE_REVISION,request_id:"new-reader-replay".into(),command:command.clone()})).await;
            replay.validate_for(&command).unwrap();assert_eq!(replay.outcome,original.outcome);assert_eq!(replay.commit,original.commit);
        }
        drop(c);owner_released(&db).await;assert_eq!(controller_version(&db),21);
        let mut after_rows=canonical_rows(&db);
        assert_eq!(after_rows.remove("charter_output_chunks"),Some(Vec::new()));
        assert_eq!(after_rows.remove("native_request_diagnostic_credits"),Some(Vec::new()));
        assert_eq!(after_rows,before,"every historical15 row stays exact; only the two known empty additive tables appear");
        let after=inventory(&state);
        for (path,(dir,dev,ino,mode,bytes)) in &identities {
            if path.to_string_lossy().ends_with("-wal")||path.to_string_lossy().ends_with("-shm") {continue;}
            let actual=after.get(path).expect("owner identity file preserved");assert_eq!((actual.0,actual.1,actual.2,actual.3),(*dir,*dev,*ino,*mode));
            if !dir && path!=Path::new("tasks.sqlite") {assert_eq!(&actual.4,bytes);}
        }
        let before_refusal=inventory(&state);let mut refused=spawn_controller(&selected_old,&metadata,&db,&address,&root,"old-refuses20");
        let status=tokio::time::timeout(Duration::from_secs(10),async {
            loop {if let Some(status)=refused.0.try_wait().unwrap(){break status;}tokio::time::sleep(Duration::from_millis(10)).await;}
        }).await.expect("old reader must promptly refuse newer schema");assert!(!status.success());
        let diagnostic=std::fs::read_to_string(root.join("old-refuses20.stderr")).unwrap();assert!(diagnostic.contains("unsupported controller database schema"),"{diagnostic}");
        assert!(!std::fs::read_to_string(root.join("old-refuses20.stdout")).unwrap().contains("Controller listening on"));
        assert_eq!(inventory(&state),before_refusal,"old15 refusal must preserve every existing byte and inode");
    }
    assert_hash(&old,OLD_IMAGE_SHA256);assert_hash(&metadata,OLD_METADATA_SHA256);
}

// This fixture uses the actual installed-generation state9 producer. It does
// not manufacture a version9 marker or reconstruct an old reader in new code.
const FORMAT9_IMAGE_SHA256: &str = "6dfb55597ee8153df99138e457e5483cb683f4f8ba68a4ca96138c28ce5008aa";
const FORMAT9_METADATA_SHA256: &str = "36ae8e816c602c86bde7a90b3faa552e77e95af87432c4a34855ddd9fe683e34";
const FORMAT9_WIRE: u32 = 11;
const FORMAT9_HTTP_BOUND: usize = 1_048_576;

fn spawn_format9_worker(
    image: &Path, metadata: &Path, workspace: &Path, origin: &str, logs: &Path, label: &str,
) -> HistoricalController {
    HistoricalController(std::process::Command::new(image)
        .arg("worker").arg("--limits").arg(metadata)
        .arg("--controller").arg(origin).arg("--id").arg("compatibility")
        .arg("--incarnation").arg(label).arg("--workspace-root").arg(workspace)
        .arg("--codex").arg("/must-not-launch")
        .arg("--native-endpoint").arg("http://127.0.0.1:1/no-provider")
        .env_clear().env("PATH", "/usr/bin:/bin")
        .stdin(std::process::Stdio::null())
        .stdout(std::fs::File::create(logs.join(format!("{label}.stdout"))).unwrap())
        .stderr(std::fs::File::create(logs.join(format!("{label}.stderr"))).unwrap())
        .spawn().unwrap())
}

async fn format9_read_request(stream: &mut tokio::net::TcpStream) -> borg_runtime::protocol::Request {
    use tokio::io::AsyncReadExt;
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut bytes = Vec::new();
        let (header_end, body_len) = loop {
            if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                assert!(end <= 16_384);
                let header = std::str::from_utf8(&bytes[..end]).unwrap();
                assert_eq!(header.lines().next(), Some("POST /api/operations HTTP/1.1"));
                let mut lengths = Vec::new();
                for line in header.lines().skip(1) {
                    let (key, value) = line.split_once(':').unwrap();
                    assert!(!key.eq_ignore_ascii_case("transfer-encoding"));
                    if key.eq_ignore_ascii_case("content-length") {
                        lengths.push(value.trim().parse::<usize>().unwrap());
                    }
                }
                assert_eq!(lengths.len(), 1);
                assert!(lengths[0] <= FORMAT9_HTTP_BOUND);
                break (end + 4, lengths[0]);
            }
            assert!(bytes.len() <= 16_384);
            let mut chunk = [0; 1024];
            let count = stream.read(&mut chunk).await.unwrap();
            assert_ne!(count, 0, "request ended before its headers");
            bytes.extend_from_slice(&chunk[..count]);
        };
        let end = header_end.checked_add(body_len).unwrap();
        while bytes.len() < end {
            let mut chunk = [0; 1024];
            let count = stream.read(&mut chunk).await.unwrap();
            assert_ne!(count, 0, "request ended before its declared body");
            bytes.extend_from_slice(&chunk[..count]);
        }
        assert_eq!(bytes.len(), end, "no pipelined or trailing request bytes");
        serde_json::from_slice(&bytes[header_end..]).unwrap()
    }).await.expect("fixture request read is bounded")
}

async fn format9_exchange(
    client: &reqwest::Client, controller: &str, request: &borg_runtime::protocol::Request,
) -> (borg_runtime::protocol::Reply, Vec<u8>) {
    assert_eq!(request.protocol_revision, FORMAT9_WIRE);
    let body = serde_json::to_vec(request).unwrap();
    assert!(body.len() <= FORMAT9_HTTP_BOUND);
    let mut response = client.post(format!("http://{controller}/api/operations"))
        .header("content-type", "application/json").body(body).send().await.unwrap();
    assert!(response.status().is_success());
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.unwrap() {
        assert!(body.len() + chunk.len() <= FORMAT9_HTTP_BOUND);
        body.extend_from_slice(&chunk);
    }
    let reply: borg_runtime::protocol::Reply = serde_json::from_slice(&body).unwrap();
    assert_eq!(reply.request_id, request.request_id);
    reply.validate_for(&request.command).unwrap();
    (reply, body)
}

async fn format9_write_reply(stream: &mut tokio::net::TcpStream, body: &[u8]) {
    use tokio::io::AsyncWriteExt;
    let header = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
    tokio::time::timeout(Duration::from_secs(5), async {
        stream.write_all(header.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
        stream.shutdown().await.unwrap();
    }).await.expect("fixture reply write is bounded");
}

async fn format9_hold_until_worker_closes(stream: &mut tokio::net::TcpStream) {
    use tokio::io::AsyncReadExt;
    let mut byte = [0; 1];
    let result = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut byte))
        .await.expect("stopped worker must close its withheld request");
    match result {
        Ok(0) => {}
        Err(error) if matches!(error.kind(), std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted) => {}
        other => panic!("unexpected bytes after complete request: {other:?}"),
    }
}

#[derive(Clone)]
struct Format9Proof {
    registration: borg_runtime::protocol::Request,
    registered: borg_runtime::protocol::Reply,
    fence: borg_runtime::protocol::Request,
    fenced: borg_runtime::protocol::Reply,
    awaiting: borg_runtime::protocol::Request,
    committed_await: borg_runtime::protocol::Reply,
}

async fn format9_producer_proxy(
    listener: tokio::net::TcpListener, controller: String,
    ready: tokio::sync::oneshot::Sender<Format9Proof>,
) -> (tokio::net::TcpListener, Vec<borg_runtime::protocol::Request>) {
    use borg_runtime::protocol::*;
    let client = reqwest::Client::builder().no_proxy().timeout(Duration::from_secs(5)).build().unwrap();
    let mut registration = None;
    let mut seen = Vec::new();
    for _ in 0..8 {
        let (mut stream, _) = tokio::time::timeout(Duration::from_secs(10), listener.accept()).await.unwrap().unwrap();
        let request = format9_read_request(&mut stream).await;
        assert_eq!(request.protocol_revision, FORMAT9_WIRE);
        seen.push(request.clone());
        match &request.command {
            Operation::RegisterWorker { worker, .. } => {
                assert!(registration.is_none());
                assert_eq!(worker.id, "compatibility");
                assert_eq!(worker.incarnation, "producer-nine");
                let (reply, body) = format9_exchange(&client, &controller, &request).await;
                assert!(matches!(&reply.outcome, Outcome::Ok { data: Data::Worker(value) } if value == worker));
                assert!(reply.commit.is_some());
                registration = Some((request.clone(), reply));
                format9_write_reply(&mut stream, &body).await;
            }
            Operation::GetOperation { operation_id } => {
                assert!(operation_id.starts_with("worker-register-") || operation_id.starts_with("worker-await-"));
                let (_, body) = format9_exchange(&client, &controller, &request).await;
                format9_write_reply(&mut stream, &body).await;
            }
            Operation::AwaitCommands { worker_id, incarnation, acknowledged, .. } => {
                assert_eq!(worker_id, "compatibility");
                assert_eq!(incarnation, "producer-nine");
                assert!(acknowledged.is_empty());
                let (registration, registered) = registration.expect("old registration is committed first");
                let Operation::RegisterWorker { worker, .. } = &registration.command else { unreachable!() };
                // No tasks exist. A real replacement registration makes the
                // original Await a committed refusal instead of an uncommitted
                // empty long poll. The proxy never fabricates its response.
                let mut replacement = worker.clone();
                replacement.incarnation = "fixture-fence".into();
                let fence = Request { protocol_revision: FORMAT9_WIRE, request_id: "state9-fence".into(),
                    command: Operation::RegisterWorker { operation_id: "state9-fence".into(), worker: replacement.clone() } };
                let (fenced, _) = format9_exchange(&client, &controller, &fence).await;
                assert!(matches!(&fenced.outcome, Outcome::Ok { data: Data::Worker(value) } if value == &replacement));
                assert!(fenced.commit.is_some());
                let (committed_await, _) = format9_exchange(&client, &controller, &request).await;
                assert!(matches!(&committed_await.outcome, Outcome::Refused { code, .. } if code == "stale_incarnation"));
                assert!(committed_await.commit.is_some());
                let proof = Format9Proof { registration, registered, fence, fenced,
                    awaiting: request, committed_await };
                assert!(ready.send(proof).is_ok());
                // Intentionally do not deliver one byte of this real receipt.
                format9_hold_until_worker_closes(&mut stream).await;
                return (listener, seen);
            }
            other => panic!("old empty-worker fixture attempted an unrelated operation: {other:?}"),
        }
    }
    panic!("old worker did not reach its first Await within the finite request count");
}

fn format9_assert_lookup(
    reply: &borg_runtime::protocol::Reply, original: &borg_runtime::protocol::Request,
    committed: &borg_runtime::protocol::Reply,
) {
    use borg_runtime::protocol::*;
    let id = original.command.mutation_id().unwrap();
    reply.validate_for(&Operation::GetOperation { operation_id: id.into() }).unwrap();
    assert_eq!(reply.commit, committed.commit);
    let Outcome::Ok { data: Data::Operation(record) } = &reply.outcome else { panic!("{reply:?}") };
    assert_eq!(record.id, id);
    assert_eq!(record.kind, "application_mutation");
    assert_eq!(record.state, "settled");
    assert_eq!(record.intended, serde_json::to_value(&original.command).unwrap());
    assert_eq!(record.outcome, Some(serde_json::to_value(&committed.outcome).unwrap()));
}

#[tokio::test]
#[ignore = "requires the exact preserved actual state9 executable and its package metadata; root executes"]
async fn genuine_format9_journals_are_read_before_format12_recovery_and_old9_refuses12() {
    use borg_runtime::protocol::*;
    let old = PathBuf::from(std::env::var_os("BORG_WORKER_FORMAT9_BINARY").expect("select actual format9 executable"));
    let metadata = PathBuf::from(std::env::var_os("BORG_WORKER_FORMAT9_LIMITS").expect("select exact format9 package metadata"));
    assert_hash(&old, FORMAT9_IMAGE_SHA256);
    assert_hash(&metadata, FORMAT9_METADATA_SHA256);
    let root = retained_images::artifact_root().join(format!("genuine-state9-{}-{}", std::process::id(), process::now_ms()));
    std::fs::create_dir(&root).unwrap();
    let selected_old = root.join("historical-runtime");
    retained_images::materialize(&old, &selected_old, true);
    assert_hash(&selected_old, FORMAT9_IMAGE_SHA256);
    let db_root = root.join("canonical"); std::fs::create_dir(&db_root).unwrap();
    let db = db_root.join("tasks.sqlite");
    let address = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let controller_address = address.local_addr().unwrap().to_string(); drop(address);
    let mut controller = spawn_controller(&selected_old, &metadata, &db, &controller_address, &root, "controller-nine");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert!(controller.0.try_wait().unwrap().is_none());
            if std::fs::read_to_string(root.join("controller-nine.stdout")).unwrap().contains("Controller listening on") { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("actual old controller readiness");
    let client = reqwest::Client::builder().no_proxy().timeout(Duration::from_secs(5)).build().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let producer_proxy = tokio::spawn(format9_producer_proxy(listener, controller_address.clone(), ready_tx));
    let workspace = root.join("workspace");
    let mut producer = spawn_format9_worker(&selected_old, &metadata, &workspace, &origin, &root, "producer-nine");
    let proof = tokio::time::timeout(Duration::from_secs(10), ready_rx).await.expect("actual old worker must produce its first committed Await").unwrap();
    producer.stop().await;
    let (listener, old_requests) = tokio::time::timeout(Duration::from_secs(10), producer_proxy).await.unwrap().unwrap();
    assert_eq!(old_requests.iter().filter(|r| matches!(r.command, Operation::RegisterWorker { .. })).count(), 1);
    assert_eq!(old_requests.iter().filter(|r| matches!(r.command, Operation::AwaitCommands { .. })).count(), 1);
    let state = workspace.join(".borg-worker").join(process::component("compatibility"));
    assert!(!state.join("worker-owner.json").exists(), "old worker exited and released its owner");
    let before = inventory(&workspace);
    let marker_path = PathBuf::from(".borg-worker").join(process::component("compatibility")).join("format.json");
    let marker: serde_json::Value = serde_json::from_slice(&before[&marker_path].4).unwrap();
    assert_eq!(marker, serde_json::json!({"format_revision":9}));
    let registration_bytes = std::fs::read(state.join("mutations/registration.json")).unwrap();
    let await_bytes = std::fs::read(state.join("mutations/await.json")).unwrap();
    let registration: serde_json::Value = serde_json::from_slice(&registration_bytes).unwrap();
    let awaiting: serde_json::Value = serde_json::from_slice(&await_bytes).unwrap();
    assert_eq!(registration["revision"], 3);
    assert_eq!(registration["request"], serde_json::to_value(&proof.registration.command).unwrap());
    assert_eq!(registration["outcome"], serde_json::to_value(&proof.registered.outcome).unwrap());
    assert_eq!(registration["outcome_context"]["commit"], serde_json::to_value(&proof.registered.commit).unwrap());
    assert_eq!(registration["consumed"], false);
    assert_eq!(awaiting["revision"], 3);
    assert_eq!(awaiting["request"], serde_json::to_value(&proof.awaiting.command).unwrap());
    assert!(awaiting["outcome"].is_null() && awaiting["outcome_context"].is_null());
    assert_eq!(awaiting["consumed"], false);
    assert_eq!(std::fs::read_dir(state.join("commands")).unwrap().count(), 0);
    assert_eq!(std::fs::read_dir(state.join("outbox")).unwrap().count(), 0);
    std::fs::write(root.join("old-produced-proof.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "registration_request":proof.registration, "registration_reply":proof.registered,
        "fence_request":proof.fence, "fence_reply":proof.fenced,
        "await_request":proof.awaiting, "await_reply_withheld":proof.committed_await,
        "scope":"genuine state9 registration and unknown committed Await; no task or model execution"
    })).unwrap()).unwrap();
    std::fs::write(root.join("worker-inventory-before.json"), serde_json::to_vec_pretty(&before).unwrap()).unwrap();
    let mut original_lookups = Vec::new();
    for (original, committed) in [(&proof.registration, &proof.registered), (&proof.fence, &proof.fenced), (&proof.awaiting, &proof.committed_await)] {
        let request = Request { protocol_revision: FORMAT9_WIRE, request_id: "before-migration-lookup".into(),
            command: Operation::GetOperation { operation_id: original.command.mutation_id().unwrap().into() } };
        let (reply, _) = format9_exchange(&client, &controller_address, &request).await;
        format9_assert_lookup(&reply, original, committed);
        original_lookups.push((request, reply));
    }

    let await_id = proof.awaiting.command.mutation_id().unwrap().to_owned();
    let (lookup_tx, lookup_rx) = tokio::sync::oneshot::channel();
    let lookup_controller = controller_address.clone();
    let recovery_proxy = tokio::spawn(async move {
        let (mut stream, _) = tokio::time::timeout(Duration::from_secs(10), listener.accept()).await.unwrap().unwrap();
        let request = format9_read_request(&mut stream).await;
        assert_eq!(request.protocol_revision, WIRE_REVISION);
        assert!(matches!(&request.command, Operation::GetOperation { operation_id } if operation_id == &await_id),
            "candidate must read only the exact original Await; no new registration or mutation is forwarded");
        // Test-only read adapter: the actual old controller remains canonical.
        // It changes only the envelope revision, never request ID or command.
        // This proves worker state admission, not mixed-wire deployment support.
        let mut forwarded = request.clone(); forwarded.protocol_revision = FORMAT9_WIRE;
        assert_eq!(forwarded.request_id, request.request_id);
        assert_eq!(serde_json::to_value(&forwarded.command).unwrap(), serde_json::to_value(&request.command).unwrap());
        let client = reqwest::Client::builder().no_proxy().timeout(Duration::from_secs(5)).build().unwrap();
        let (reply, _) = format9_exchange(&client, &lookup_controller, &forwarded).await;
        assert!(lookup_tx.send((request, reply)).is_ok());
        format9_hold_until_worker_closes(&mut stream).await;
        assert!(tokio::time::timeout(Duration::from_millis(25), listener.accept()).await.is_err(),
            "candidate must issue no second request while its recovery lookup is unresolved");
        listener
    });
    let current = Path::new(env!("CARGO_BIN_EXE_borg-runtime"));
    let mut candidate = spawn_format9_worker(current, &metadata, &workspace, &origin, &root, "candidate-fifteen");
    let (lookup_request, lookup_reply) = tokio::time::timeout(Duration::from_secs(10), lookup_rx).await
        .expect("new worker must read its old pending journal after admission").unwrap();
    format9_assert_lookup(&lookup_reply, &proof.awaiting, &proof.committed_await);
    candidate.stop().await;
    let listener = tokio::time::timeout(Duration::from_secs(10), recovery_proxy).await.unwrap().unwrap();
    std::fs::write(root.join("candidate-lookup-withheld.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "request":lookup_request, "reply":lookup_reply, "reply_delivered":false,
        "candidate_mutations_forwarded":0, "original_await_posts_total":1,
        "envelope_adapter":"exact GetOperation only; current wire to historical11"
    })).unwrap()).unwrap();
    assert!(!state.join("worker-owner.json").exists(), "candidate stopped and released its owner");
    let mut after = inventory(&workspace);
    let added_directory = PathBuf::from(".borg-worker").join(process::component("compatibility")).join("pull-requests");
    let additions = after.keys().filter(|path| !before.contains_key(*path)).cloned().collect::<Vec<_>>();
    assert_eq!(additions, vec![added_directory.clone()], "only the new empty PR journal directory may be created");
    assert!(after.remove(&added_directory).unwrap().0);
    assert_eq!(std::fs::read_dir(state.join("pull-requests")).unwrap().count(), 0);
    assert_eq!(before.keys().collect::<Vec<_>>(), after.keys().collect::<Vec<_>>());
    for (path, original) in &before {
        let actual = &after[path];
        if path == &marker_path {
            assert_eq!((actual.0, actual.1, actual.3), (original.0, original.1, original.3));
            assert_ne!(actual.2, original.2, "version marker is atomically replaced");
            assert_eq!(serde_json::from_slice::<serde_json::Value>(&actual.4).unwrap(), serde_json::json!({"format_revision":17}));
        } else {
            assert_eq!(actual, original, "old-state byte, inode or mode drift at {}", path.display());
        }
    }
    assert_eq!(std::fs::read(state.join("mutations/registration.json")).unwrap(), registration_bytes);
    assert_eq!(std::fs::read(state.join("mutations/await.json")).unwrap(), await_bytes);
    std::fs::write(root.join("worker-inventory-after.json"), serde_json::to_vec_pretty(&inventory(&workspace)).unwrap()).unwrap();
    for (request, original) in original_lookups {
        let (reply, _) = format9_exchange(&client, &controller_address, &request).await;
        assert_eq!(reply, original, "no canonical receipt or controller revision changed during candidate startup");
    }
    // A real old executable must refuse15 before owner acquisition or network;
    // the deliberately missing profile cannot mask the required version error.
    let before_refusal = inventory(&workspace);
    let diagnostic = refused_worker(&selected_old, &metadata, &workspace, &origin, &root, "old-nine-refuses-fifteen").await;
    assert!(diagnostic.contains("unsupported worker state format; no migration performed"), "{diagnostic}");
    assert_eq!(inventory(&workspace), before_refusal, "old9 refusal changes no path, byte, inode or mode");
    assert!(tokio::time::timeout(Duration::from_millis(25), listener.accept()).await.is_err(), "old9 refusal precedes any controller connection");
    drop(listener);
    controller.stop().await;
    owner_released(&db).await;
    let rows = canonical_rows(&db);
    assert_eq!(rows["receipts"].len(), 3, "only real registration, explicit fencing and original Await committed");
    assert!(rows["tasks"].is_empty(), "no task or executor was admitted");
    assert_hash(&old, FORMAT9_IMAGE_SHA256);
    assert_hash(&metadata, FORMAT9_METADATA_SHA256);
}

#[path = "worker_state_compatibility/pull_request.rs"]
mod pull_request;

#[path = "worker_state_compatibility/instruction_source.rs"]
mod instruction_source;
