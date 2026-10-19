//! Explicit original-output compatibility qualification. Genuine prior wire14,
//! schema16 and worker12 processes are used; root selects preserved inputs.
//! No provider, model, or live installed state is used by these finite fixtures.
#[path = "support/retained_images.rs"]
mod retained_images;

use borg_runtime::worker::process;
use std::{collections::BTreeMap, path::{Path, PathBuf}, time::Duration};

const OLD_IMAGE_SHA256: &str = "c26ec0844dcee999c481b01200f6594d84076da6cbd0aff4ec760510e16d94fa";
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
#[ignore = "explicit preserved format12 executable and metadata required; root must execute this qualification"]
async fn historical_format12_runtime_refuses_new_format13_before_owner_or_controller_effects() {
    let old = PathBuf::from(std::env::var_os("BORG_OUTPUT_PRIOR_BINARY")
        .expect("select the preserved actual pre-feature format12 executable"));
    let metadata = PathBuf::from(std::env::var_os("BORG_OUTPUT_PRIOR_LIMITS")
        .expect("select exact pre-feature metadata"));
    assert_hash(&old, OLD_IMAGE_SHA256);
    assert_hash(&metadata, OLD_METADATA_SHA256);
    let root = retained_images::artifact_root().join(format!("output-worker-compatibility-{}-{}",
        std::process::id(), process::now_ms()));
    std::fs::create_dir(&root).unwrap();
    let selected_old = root.join("historical-runtime");
    retained_images::materialize(&old, &selected_old, true);
    assert_hash(&selected_old, OLD_IMAGE_SHA256);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());

    // Genuine old-worker marker producer. The actual old image creates12 and
    // reaches the deliberate local resource refusal; no worker API is simulated.
    let old_workspace = root.join("old-workspace");
    let old_state = old_workspace.join(".borg-worker").join(process::component("compatibility"));
    let diagnostic = refused_worker(&selected_old, &metadata, &old_workspace, &origin, &root, "old-twelve").await;
    assert!(diagnostic.contains("resource profile unavailable"), "unexpected old control refusal: {diagnostic}");
    let marker = std::fs::read(old_state.join("format.json")).unwrap();
    assert_eq!(serde_json::from_slice::<serde_json::Value>(&marker).unwrap(), serde_json::json!({"format_revision":12}));
    // New reader accepts the original12 marker, but failed local admission must
    // leave its bytes unchanged rather than committing a migration prematurely.
    let new_image = Path::new(env!("CARGO_BIN_EXE_borg-runtime"));
    let diagnostic = refused_worker(new_image, &metadata, &old_workspace, &origin, &root, "new-reads-twelve").await;
    assert!(diagnostic.contains("resource profile unavailable"), "unexpected historical read refusal: {diagnostic}");
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
    let request=Request {protocol_revision:14,request_id:"historical-schema16-request".into(),command:command.clone()};
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
#[ignore = "explicit preserved schema16 executable and metadata required; root executes this compatibility qualification"]
async fn genuine_schema16_migrates_in_place_and_old_reader_refuses_schema18_without_writes() {
    use borg_runtime::{controller::{Controller,ControllerConfig},protocol::*};
    let old=PathBuf::from(std::env::var_os("BORG_OUTPUT_PRIOR_BINARY").unwrap());
    let metadata=PathBuf::from(std::env::var_os("BORG_OUTPUT_PRIOR_LIMITS").unwrap());
    assert_hash(&old,OLD_IMAGE_SHA256);assert_hash(&metadata,OLD_METADATA_SHA256);
    let limits=std::sync::Arc::new(borg_runtime::limits::RuntimeLimits::parse(&std::fs::read(&metadata).unwrap()).unwrap());
    for corrupt in [false,true] {
        let root=retained_images::artifact_root().join(format!("schema18-compatibility-{}-{}-{corrupt}",std::process::id(),process::now_ms()));
        std::fs::create_dir(&root).unwrap();let state=root.join("state");std::fs::create_dir(&state).unwrap();
        let db=state.join("tasks.sqlite");let selected_old=root.join("historical-runtime");
        retained_images::materialize(&old,&selected_old,true);assert_hash(&selected_old,OLD_IMAGE_SHA256);
        let listener=std::net::TcpListener::bind("127.0.0.1:0").unwrap();let address=listener.local_addr().unwrap().to_string();drop(listener);
        let mut child=spawn_controller(&selected_old,&metadata,&db,&address,&root,"producer16");
        let client=reqwest::Client::builder().no_proxy().timeout(Duration::from_secs(5)).build().unwrap();
        tokio::time::timeout(Duration::from_secs(10),async {
            loop {
                assert!(child.0.try_wait().unwrap().is_none());
                if std::fs::read_to_string(root.join("producer16.stdout")).unwrap().contains("Controller listening on") { break; }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await.expect("historical controller readiness");
        let input=TaskInput { instruction_source: None, pull_request: None, resolved_limits:None,repository_read:None,instruction:"retain original receipt".into(),
            definition:Definition {charter:None,limit_overrides:None,execution_profile:Some(ExecutionProfile::CollaborationV1{web_search:None}),inference:Some(InferenceSettings::selected()),id:"schema16-owner".into(),revision:"original-1".into(),executor:ExecutorKind::Native},
            repository:None,acceptance_commands:vec![],output_schema:Some(serde_json::json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false})),
            limits:Limits {deadline_unix_ms:process::now_ms()+600000,model_requests:3,output_bytes:16384,result_bytes:limits.task_defaults.result_bytes,max_delegation_depth:4},dependencies:vec![],native_workflow:None};
        let submit=Operation::SubmitTask {operation_id:"schema16-submit".into(),input: (input).into()};
        let submitted=old_call(&client,&address,submit.clone()).await;
        let task=match &submitted.outcome {Outcome::Ok{data:Data::Task(t)}=>t,_=>panic!("{submitted:?}")};
        let cancel=Operation::CancelTask {operation_id:"schema16-cancel".into(),task_id:task.id.clone(),expected_revision:task.revision};
        let cancelled=old_call(&client,&address,cancel.clone()).await;
        child.stop().await;owner_released(&db).await;assert_eq!(controller_version(&db),16);
        let cfg=ControllerConfig {limits:limits.clone(),validation_executable:PathBuf::from(env!("CARGO_BIN_EXE_borg-runtime")),..ControllerConfig::default()};
        if corrupt {
            // The version barrier must not skip existing16 domain validation.
            let c=rusqlite::Connection::open(&db).unwrap();
            c.execute("INSERT INTO charter_sessions(id,revision,body) VALUES('invalid',1,'{}')",[]).unwrap();drop(c);
            let before=inventory(&state);
            let error=match Controller::open_with_config(&db,cfg) {Ok(_)=>panic!("corrupt16 was admitted"),Err(e)=>e};
            assert!(error.contains("invalid stored charter state"),"{error}");owner_released(&db).await;
            assert_eq!(inventory(&state),before);assert_eq!(controller_version(&db),16);continue;
        }
        let before=canonical_rows(&db);let identities=inventory(&state);
        let c=Controller::open_with_config(&db,cfg).unwrap();
        for (command,original) in [(submit,submitted),(cancel,cancelled)] {
            let replay=Box::pin(c.handle(Request {protocol_revision:WIRE_REVISION,request_id:"new-reader-replay".into(),command:command.clone()})).await;
            replay.validate_for(&command).unwrap();assert_eq!(replay.outcome,original.outcome);assert_eq!(replay.commit,original.commit);
        }
        drop(c);owner_released(&db).await;assert_eq!(controller_version(&db),21);
        let mut after_rows=canonical_rows(&db);
        assert_eq!(after_rows.remove("charter_output_chunks"),Some(Vec::new()),"migration preserves all original rows and adds empty tables");
        assert_eq!(after_rows.remove("native_request_diagnostic_credits"),Some(Vec::new()));
        assert_eq!(after_rows,before,"every original table and typed value is preserved");
        let after=inventory(&state);
        for (path,(dir,dev,ino,mode,bytes)) in &identities {
            if path.to_string_lossy().ends_with("-wal")||path.to_string_lossy().ends_with("-shm") {continue;}
            let actual=after.get(path).expect("owner identity file preserved");assert_eq!((actual.0,actual.1,actual.2,actual.3),(*dir,*dev,*ino,*mode));
            if !dir && path!=Path::new("tasks.sqlite") {assert_eq!(&actual.4,bytes);}
        }
        let before_refusal=inventory(&state);let mut refused=spawn_controller(&selected_old,&metadata,&db,&address,&root,"old-refuses18");
        let status=tokio::time::timeout(Duration::from_secs(10),async {
            loop {if let Some(status)=refused.0.try_wait().unwrap(){break status;}tokio::time::sleep(Duration::from_millis(10)).await;}
        }).await.expect("old reader must promptly refuse newer schema");assert!(!status.success());
        let diagnostic=std::fs::read_to_string(root.join("old-refuses18.stderr")).unwrap();assert!(diagnostic.contains("unsupported controller database schema"),"{diagnostic}");
        assert!(!std::fs::read_to_string(root.join("old-refuses18.stdout")).unwrap().contains("Controller listening on"));
        assert_eq!(inventory(&state),before_refusal,"old16 refusal must preserve every existing byte and inode");
    }
    assert_hash(&old,OLD_IMAGE_SHA256);assert_hash(&metadata,OLD_METADATA_SHA256);
}


// Exact version boundary: historical14/15 are refused before any operation
// receipt; current21 preserves the original output payload vocabulary.
#[tokio::test]
async fn historical_wire14_and15_refuse_before_rows_current16_accepts() {
    use borg_runtime::{charter::*, controller::{Controller,ControllerConfig}, mcp::Caller, protocol::*};
    assert_eq!(WIRE_REVISION,21,"this discriminator binds the exact output-retention wire");
    let limits=std::sync::Arc::new(borg_runtime::limits::RuntimeLimits::seed().unwrap());
    assert!(limits.charter.is_none(),"fixture uses ordinary existing admission, not a new charter grant");
    let cfg=ControllerConfig {limits:limits.clone(),validation_executable:PathBuf::from(env!("CARGO_BIN_EXE_borg-runtime")),..ControllerConfig::default()};
    let root=retained_images::artifact_root().join(format!("output-wire14-to19-{}-{}",std::process::id(),process::now_ms()));
    std::fs::create_dir(&root).unwrap();let db=root.join("tasks.sqlite");
    let c=Controller::open_with_config(&db,cfg.clone()).unwrap();
    drop(c);owner_released(&db).await;
    let before=canonical_rows(&db);
    assert_eq!(before.get("charter_output_chunks"),Some(&Vec::new()));
    let input=TaskInput { instruction_source: None, pull_request: None, resolved_limits:None,repository_read:None,instruction:"retain the exact wire boundary".into(),
        definition:Definition {charter:None,limit_overrides:None,execution_profile:Some(ExecutionProfile::CollaborationV1{web_search:None}),inference:Some(InferenceSettings::selected()),id:"output-wire-owner".into(),revision:"original-1".into(),executor:ExecutorKind::Native},
        repository:None,acceptance_commands:vec![],output_schema:Some(serde_json::json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false})),
        limits:Limits {deadline_unix_ms:process::now_ms()+600000,model_requests:3,output_bytes:16384,result_bytes:limits.task_defaults.result_bytes,max_delegation_depth:4},dependencies:vec![],native_workflow:None};
    let submit=Operation::SubmitTask {operation_id:"output-wire-original-submit".into(),input: (input).into()};
    let who=Caller {task_id:"unadmitted-task".into(),attempt_id:"unadmitted-attempt".into(),generation:1,worker_id:"unadmitted-worker".into(),incarnation:"one".into()};
    let reference=CharterOutputReference {request_id:"unadmitted-request".into(),bytes:4,sha256:aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256,b"null").as_ref().iter().map(|b|format!("{b:02x}")).collect(),chunk_bytes:4,chunks:1};
    let commands=[submit.clone(),
        Operation::ObserveCharter {operation_id:"output-wire-new-chunk".into(),caller:who.clone(),observation:CharterObservation::OutputChunk {chunk:CharterOutputChunk {reference:reference.clone(),index:0,data:"6e756c6c".into()}}},
        Operation::ObserveCharter {operation_id:"output-wire-new-reference".into(),caller:who,observation:CharterObservation::Reply {turn_id:reference.request_id.clone(),reply:CharterReplyObservation {output:None,output_reference:Some(reference),response_id:None,output_item_ids:vec![],tool_call_ids:vec![],declaration:None}}},
    ];
    let c=Controller::open_with_config(&db,cfg.clone()).unwrap();
    for prior_wire in [14,15,19] {
    for (ordinal,command) in commands.iter().enumerate() {
        let request=Request {protocol_revision:prior_wire,request_id:format!("historical{prior_wire}-{ordinal}"),command:command.clone()};
        let wire=serde_json::to_vec(&request).unwrap();
        let parsed:Request=serde_json::from_slice(&wire).unwrap();
        assert_eq!(serde_json::to_value(&parsed.command).unwrap(),serde_json::to_value(command).unwrap(),"new fields are not silently discarded by the current decoder");
        let reply=Box::pin(c.handle(parsed)).await;
        assert_eq!(reply.request_id,request.request_id);
        assert!(matches!(&reply.outcome,Outcome::Refused {code,..} if code=="unsupported_version"),"{reply:?}");
        assert!(reply.commit.is_none(),"unsupported wire must not retain a refusal receipt");
    }
    }
    drop(c);owner_released(&db).await;
    assert_eq!(canonical_rows(&db),before,"historical wire changed receipt, task, counter, or chunk rows");
    let c=Controller::open_with_config(&db,cfg).unwrap();
    for command in &commands {
        let original_id=command.mutation_id().unwrap();
        let lookup=Box::pin(c.handle(Request {protocol_revision:WIRE_REVISION,request_id:"current19-absence".into(),command:Operation::GetOperation {operation_id:original_id.into()}})).await;
        assert!(matches!(&lookup.outcome,Outcome::Refused {code,..} if code=="not_found"),"{lookup:?}");
        assert!(lookup.commit.is_none());
    }
    // Same original operation ID now succeeds: old wires did not commit either an
    // effect or a refusal that would incorrectly replay under the new wire.
    let current=Box::pin(c.handle(Request {protocol_revision:WIRE_REVISION,request_id:"current19-accepted".into(),command:submit.clone()})).await;
    current.validate_for(&submit).unwrap();
    assert!(matches!(&current.outcome,Outcome::Ok {data:Data::Task(_)}),"{current:?}");
    assert_eq!(current.commit.as_ref().map(|v|v.operation_id.as_str()),submit.mutation_id());
    drop(c);owner_released(&db).await;
    let after=canonical_rows(&db);
    assert_eq!(after.get("charter_output_chunks"),Some(&Vec::new()));
    assert_eq!(after.get("receipts").unwrap().len(),before.get("receipts").unwrap().len()+1);
}
