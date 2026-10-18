//! R3: an actual receive-pack commit whose Git status never reaches its client.
//! This does not qualify the separate, still-unfulfilled PR creation/ACK workflow.
use super::*;
use borg_runtime::collaboration::{Query, QueryResult};
use borg_runtime::worker::{git as worker_git, process::component};
use std::{os::{fd::OwnedFd, unix::process::CommandExt}, sync::atomic::AtomicBool};
use tokio::sync::oneshot;

// Fixture bounds, not new runtime policy: a single tiny repository and one push.
const CONNECTIONS: u64 = 16;
const TRANSFER_BYTES: usize = 1_048_576;

#[derive(serde::Serialize)]
struct Cut {
    original: OperationRecord,
    checkpoint: Checkpoint,
    exact_ref_commit: String,
    advertisement_bytes: usize,
    uploaded_bytes: u64,
    withheld_status_bytes: usize,
    forwarded_status_bytes: usize,
}
struct State {
    client: ApiClient,
    root: PathBuf,
    expected: Mutex<Option<Checkpoint>>,
    refuse_readback: AtomicBool,
    receive_packs: AtomicU64,
    readbacks: AtomicU64,
    refused_readbacks: AtomicU64,
    response_bytes: usize,
    cut: mpsc::Sender<Cut>,
}
impl State {
    fn counters(&self) -> Value {
        json!({"receive_pack":self.receive_packs.load(Ordering::SeqCst),
            "upload_pack":self.readbacks.load(Ordering::SeqCst),
            "refused_upload_pack":self.refused_readbacks.load(Ordering::SeqCst)})
    }
}
struct Transport {
    endpoint: String,
    state: Arc<State>,
    cut: mpsc::Receiver<Cut>,
    shutdown: Option<oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<Result<(), String>>>,
}
impl Transport {
    async fn new(lab: &Lab) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("git://{}/remote.git", listener.local_addr().unwrap());
        let (cut_tx, cut) = mpsc::channel(1);
        let state = Arc::new(State {
            client: lab.client.clone(), root: lab.root.clone(), expected: Mutex::new(None),
            refuse_readback: AtomicBool::new(false), receive_packs: AtomicU64::new(0),
            readbacks: AtomicU64::new(0), refused_readbacks: AtomicU64::new(0),
            response_bytes: lab.limits.worker.git_output_bytes, cut: cut_tx,
        });
        let (shutdown, mut stopped) = oneshot::channel();
        let active = state.clone();
        let join = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            let mut serial = 0;
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept() => {
                        let (stream, peer) = accepted.map_err(|e| e.to_string())?;
                        if !peer.ip().is_loopback() { return Err("non-loopback Git client".into()); }
                        serial += 1;
                        if serial > CONNECTIONS { return Err("fixture Git connection bound exceeded".into()); }
                        let state = active.clone();
                        connections.spawn(async move {
                            tokio::time::timeout(TIMEOUT, connection(stream, state, serial)).await
                                .map_err(|_| "fixture Git connection timed out".to_string())?
                        });
                    }
                    joined = connections.join_next(), if !connections.is_empty() => {
                        joined.unwrap().map_err(|e| e.to_string())??;
                    }
                }
            }
            drop(listener);
            while let Some(joined) = connections.join_next().await {
                joined.map_err(|e| e.to_string())??;
            }
            Ok(())
        });
        Self { endpoint, state, cut, shutdown: Some(shutdown), join: Some(join) }
    }
    async fn stop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() { let _ = shutdown.send(()); }
        if let Some(mut join) = self.join.take() {
            match tokio::time::timeout(TIMEOUT, &mut join).await {
                Ok(result) => result.unwrap().unwrap(),
                Err(_) => { join.abort(); let _ = join.await; panic!("Git relay did not close"); }
            }
        }
    }
}
impl Drop for Transport {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() { let _ = shutdown.send(()); }
        if let Some(join) = self.join.take() { join.abort(); }
    }
}

// Every connection owns its real inetd Git daemon and process group. A failed
// fixture cancels all connection tasks and kills their groups, never detaches a
// receive-pack child. Successful paths wait for actual daemon exit.
struct Daemon { child: tokio::process::Child, pid: u32, complete: bool }
impl Daemon {
    async fn spawn(root: &Path, serial: u64) -> Result<(Self, TcpStream), String> {
        let listener = TcpListener::bind("127.0.0.1:0").await.map_err(|e| e.to_string())?;
        let upstream = TcpStream::connect(listener.local_addr().unwrap()).await.map_err(|e| e.to_string())?;
        let (socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
        let socket = socket.into_std().map_err(|e| e.to_string())?;
        socket.set_nonblocking(false).map_err(|e| e.to_string())?;
        let input = socket.try_clone().map_err(|e| e.to_string())?;
        let mut command = tokio::process::Command::new("/usr/lib/git-core/git-daemon");
        command.args(["--inetd", "--export-all", "--enable=receive-pack", "--strict-paths"])
            .arg(format!("--base-path={}", root.display())).arg(root.join("remote.git"))
            .env_clear().env("PATH", "/usr/bin:/bin").env("HOME", root)
            .env("GIT_CONFIG_NOSYSTEM", "1").env("GIT_CONFIG_GLOBAL", "/dev/null")
            .current_dir(root).kill_on_drop(true)
            .stdin(Stdio::from(OwnedFd::from(input))).stdout(Stdio::from(OwnedFd::from(socket)))
            .stderr(Stdio::from(File::create(root.join(format!("git-daemon-{serial}.stderr"))).map_err(|e| e.to_string())?));
        command.as_std_mut().process_group(0);
        let child = command.spawn().map_err(|e| e.to_string())?;
        let pid = child.id().ok_or("Git daemon has no PID")?;
        Ok((Self { child, pid, complete: false }, upstream))
    }
    async fn finish(&mut self, root: &Path, serial: u64) -> Result<(), String> {
        let status = self.child.wait().await.map_err(|e| e.to_string())?;
        self.complete = true;
        save(root.join(format!("git-daemon-{serial}-closed.json")), &json!({"pid":self.pid,"status":status.to_string()}));
        if !status.success() { return Err(format!("actual Git daemon failed: {status}")); }
        Ok(())
    }
}
impl Drop for Daemon {
    fn drop(&mut self) {
        if !self.complete {
            let _ = Command::new("/bin/kill").args(["-KILL", "--", &format!("-{}", self.pid)]).status();
        }
    }
}
async fn packet(stream: &mut TcpStream, cap: usize) -> Result<Vec<u8>, String> {
    let mut header = [0; 4];
    stream.read_exact(&mut header).await.map_err(|e| e.to_string())?;
    let size = usize::from_str_radix(std::str::from_utf8(&header).map_err(|e| e.to_string())?, 16)
        .map_err(|e| e.to_string())?;
    if size == 0 { return Ok(header.to_vec()); }
    if !(4..=cap).contains(&size) { return Err("invalid or oversized Git packet".into()); }
    let mut bytes = vec![0; size]; bytes[..4].copy_from_slice(&header);
    stream.read_exact(&mut bytes[4..]).await.map_err(|e| e.to_string())?;
    Ok(bytes)
}
async fn transfer<R: tokio::io::AsyncRead + Unpin, W: tokio::io::AsyncWrite + Unpin>(
    mut input: R, mut output: W, count: Arc<AtomicU64>,
) -> Result<(), String> {
    let mut bytes = [0; 8192];
    loop {
        let read = input.read(&mut bytes).await.map_err(|e| e.to_string())?;
        if read == 0 { return output.shutdown().await.map_err(|e| e.to_string()); }
        if count.fetch_add(read as u64, Ordering::SeqCst) + read as u64 > TRANSFER_BYTES as u64 {
            return Err("fixture Git transfer byte bound exceeded".into());
        }
        output.write_all(&bytes[..read]).await.map_err(|e| e.to_string())?;
    }
}
async fn connection(mut client: TcpStream, state: Arc<State>, serial: u64) -> Result<(), String> {
    let hello = packet(&mut client, 4096).await?;
    let service = hello[4..].split(|byte| *byte == 0).next().unwrap_or_default();
    let receive = match service {
        b"git-receive-pack /remote.git" => true,
        b"git-upload-pack /remote.git" => false,
        _ => return Err("unexpected Git service or repository".into()),
    };
    if !receive && state.refuse_readback.load(Ordering::SeqCst) {
        state.refused_readbacks.fetch_add(1, Ordering::SeqCst);
        // No fabricated empty-ref response: the transport is unavailable.
        client.shutdown().await.map_err(|e| e.to_string())?;
        return Ok(());
    }
    let original = if receive {
        if state.receive_packs.fetch_add(1, Ordering::SeqCst) != 0 {
            return Err("worker attempted a second receive-pack".into());
        }
        state.refuse_readback.store(true, Ordering::SeqCst);
        let expected = state.expected.lock().unwrap().clone().ok_or("push before fixture armed")?;
        let reply = state.client.call(Operation::GetOperation { operation_id: expected.operation_id.clone() }).await?;
        let Outcome::Ok { data: Data::Operation(record) } = reply.outcome else {
            return Err("receive-pack preceded original canonical preparation".into());
        };
        if record.state != "pending" || record.kind != "checkpoint"
            || record.intended["repository"] != expected.repository
            || record.intended["remote_ref"] != expected.remote_ref {
            return Err("receive-pack original preparation differs".into());
        }
        Some(record)
    } else {
        state.readbacks.fetch_add(1, Ordering::SeqCst); None
    };
    let (mut daemon, mut upstream) = Daemon::spawn(&state.root, serial).await?;
    upstream.write_all(&hello).await.map_err(|e| e.to_string())?;
    let Some(original) = original else {
        let (client_read, client_write) = client.into_split();
        let (server_read, server_write) = upstream.into_split();
        tokio::try_join!(
            transfer(client_read, server_write, Arc::new(AtomicU64::new(0))),
            transfer(server_read, client_write, Arc::new(AtomicU64::new(0))),
        )?;
        return daemon.finish(&state.root, serial).await;
    };
    let checkpoint = Checkpoint {
        registration: serde_json::from_value(original.intended["registration"].clone()).map_err(|e| e.to_string())?,
        operation_id: original.id.clone(), repository: original.intended["repository"].as_str().ok_or("repository absent")?.into(),
        commit: original.intended["commit"].as_str().ok_or("commit absent")?.into(),
        remote_ref: original.intended["remote_ref"].as_str().ok_or("ref absent")?.into(),
    };
    let mut advertisement_bytes = 0;
    loop {
        let advertisement = packet(&mut upstream, 65_520).await?;
        advertisement_bytes += advertisement.len();
        if advertisement_bytes > state.response_bytes { return Err("Git advertisement bound exceeded".into()); }
        client.write_all(&advertisement).await.map_err(|e| e.to_string())?;
        if advertisement.as_slice() == b"0000" { break; }
    }
    let (client_read, mut client_write) = client.into_split();
    let (server_read, server_write) = upstream.into_split();
    let uploaded = Arc::new(AtomicU64::new(0));
    let mut sending = tokio::task::JoinSet::new();
    sending.spawn(transfer(client_read, server_write, uploaded.clone()));
    let mut status = Vec::new();
    server_read.take(state.response_bytes as u64 + 1).read_to_end(&mut status).await.map_err(|e| e.to_string())?;
    if status.is_empty() || status.len() > state.response_bytes { return Err("Git status absent or oversized".into()); }
    daemon.finish(&state.root, serial).await?;
    let accepted_ref = format!("ok {}", checkpoint.remote_ref);
    if !status.windows(b"unpack ok".len()).any(|bytes| bytes == b"unpack ok")
        || !status.windows(accepted_ref.len()).any(|bytes| bytes == accepted_ref.as_bytes()) {
        return Err("actual receive-pack did not generate a positive original-ref status".into());
    }
    // The daemon exited after receiving the real pack. Verify the original ref
    // directly in the backing bare store while the client's status is withheld.
    let exact_ref_commit = git(&state.root, &["--git-dir", state.root.join("remote.git").to_str().unwrap(),
        "rev-parse", "--verify", &checkpoint.remote_ref]);
    if exact_ref_commit != checkpoint.commit { return Err("original bare ref was not committed before cut".into()); }
    let cut = Cut { original, checkpoint, exact_ref_commit, advertisement_bytes,
        uploaded_bytes: uploaded.load(Ordering::SeqCst), withheld_status_bytes: status.len(), forwarded_status_bytes: 0 };
    fs::write(state.root.join("withheld-receive-pack-status.bin"), status).map_err(|e| e.to_string())?;
    save(state.root.join("git-ref-witness-before-transport-cut.json"), &cut);
    sending.shutdown().await;
    client_write.shutdown().await.map_err(|e| e.to_string())?;
    drop(client_write);
    state.cut.send(cut).await.map_err(|e| e.to_string())?;
    Ok(())
}

async fn operation(lab: &Lab, id: &str) -> OperationRecord {
    match lab.client.call(Operation::GetOperation { operation_id: id.into() }).await.unwrap().outcome {
        Outcome::Ok { data: Data::Operation(record) } => record,
        other => panic!("original operation missing: {other:?}"),
    }
}
async fn original_requests(lab: &Lab, attempt: &Attempt) -> u32 {
    let reply = lab.client.call(Operation::QueryCollaboration {
        query: Query::TaskAttempts { task_id: attempt.task_id.clone(), after: None, limit: 4 },
    }).await.unwrap();
    let Outcome::Ok { data: Data::Collaboration(QueryResult::TaskAttempts(page)) } = reply.outcome else {
        panic!("actual attempt history missing");
    };
    assert!(page.next_cursor.is_none());
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].attempt.id, attempt.id);
    page.items[0].reported_model_requests
}
fn worker_state(lab: &Lab) -> PathBuf {
    lab.root.join("worker-a/.borg-worker").join(component("worker-a"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn receive_pack_commit_lost_status_reconciles_original_ref_without_second_push_or_executor() {
    let mut lab = Lab::new("git-transport-ack-loss").await;
    let mut transport = Transport::new(&lab).await;
    let binding = borg_runtime::repository_catalogue::Revision {
        repository_id: "git-transport-recovery".into(), registration_revision: "one".into(),
    };
    let registered = lab.client.call(Operation::RegisterRepository {
        operation_id: "register-git-transport-recovery".into(),
        registration: borg_runtime::repository_catalogue::Registration {
            repository_id: binding.repository_id.clone(), registration_revision: binding.registration_revision.clone(),
            display_name: "Fixture loopback Git transport".into(), source_endpoint: lab.repository.url.clone(),
            checkpoint_endpoint: transport.endpoint.clone(), source_store_key: "remote.git".into(), checkpoint_store_key: "remote.git".into(),
            pull_request_target: None,
        },
    }).await.unwrap();
    assert!(matches!(registered.outcome, Outcome::Ok { data: Data::RepositoryRegistration(_) }));
    lab.repository.registration = Some(binding);
    lab.repository.checkpoint_remote = transport.endpoint.clone();
    let submitted = lab.submit("submit-git-transport", "git-transport-agent").await;
    let origin = lab.origin.clone();
    let worker = lab.start_worker("worker-a", "transport-one", &origin);
    let gate = lab.gate().await;
    let assigned = lab.get(&submitted.id).await;
    let attempt = assigned.attempt.clone().unwrap();
    let request_count = original_requests(&lab, &attempt).await;
    let expected = worker_git::checkpoint(&lab.repository, &submitted.id, &attempt.id, String::new()).unwrap();
    *transport.state.expected.lock().unwrap() = Some(expected.clone());
    gate.release().await;
    let cut = tokio::time::timeout(TIMEOUT, transport.cut.recv()).await.unwrap().expect("Git transport cut missing");
    assert_eq!(cut.original.id, expected.operation_id);
    assert_eq!(cut.original.task_id, submitted.id);
    assert_eq!(cut.original.attempt_id, attempt.id);
    assert_eq!(cut.original.generation, attempt.generation);
    assert_eq!(cut.checkpoint.registration, expected.registration);
    assert_eq!(cut.checkpoint.repository, expected.repository);
    assert_eq!(cut.checkpoint.remote_ref, expected.remote_ref);
    assert_eq!(cut.exact_ref_commit, cut.checkpoint.commit);
    assert_ne!(cut.checkpoint.commit, lab.repository.start_commit);
    assert!(cut.uploaded_bytes > 0 && cut.withheld_status_bytes > 0);
    assert_eq!(cut.forwarded_status_bytes, 0);
    assert!(transport.state.readbacks.load(Ordering::SeqCst) >= 1,
        "the ordinary pre-push readback must use the real transport");
    // ProcessTreeStopped precedes the completed-result refusal and its retained
    // progress notice. Wait for that ordinary report before taking a full Task
    // restart baseline; do not normalize away revision/progress changes.
    let uncertain = lab.until(&submitted.id, "lost Git ACK and original acceptance refusal are retained", |task| {
        task.attempt.as_ref().is_some_and(|a| a.stopped && a.uncertain)
            && task.progress == "Completed result acceptance is refused; original decision retained for reconciliation"
    }).await;
    assert_eq!(uncertain.status, TaskStatus::Active);
    assert!(uncertain.result.is_none() && uncertain.selected_checkpoint.is_none());
    assert_eq!(uncertain.input, submitted.input);
    assert!(transport.state.refused_readbacks.load(Ordering::SeqCst) >= 1);
    assert_eq!(operation(&lab, &expected.operation_id).await, cut.original);
    let phase_path = worker_state(&lab).join("publications").join(format!("{}.json", component(&attempt.id)));
    let settlement_path = worker_state(&lab).join("mutations/settlements").join(format!("{}.json", component(&expected.operation_id)));
    let phase_bytes = fs::read(&phase_path).unwrap();
    let phase: Value = serde_json::from_slice(&phase_bytes).unwrap();
    assert_eq!(phase["publish_may_have_started"], true);
    assert_eq!(phase["checkpoint"], serde_json::to_value(&cut.checkpoint).unwrap());
    assert!(!settlement_path.exists(), "unavailable readback cannot fabricate settlement");
    lab.workers[worker].stop();
    assert_eq!(lab.get(&submitted.id).await, uncertain, "physical worker stop follows the completed report boundary");
    let original_refusal_id = format!("{}-report-{:020}", attempt.id, 3);
    let original_refusal = operation(&lab, &original_refusal_id).await;
    let Operation::ObserveWorker { report } = serde_json::from_value(original_refusal.intended.clone()).unwrap() else {
        panic!("original failed result report missing");
    };
    assert_eq!(report.attempt_id, attempt.id);
    assert_eq!(report.generation, attempt.generation);
    assert_eq!(report.worker_id, attempt.worker_id);
    assert_eq!(report.incarnation, attempt.worker_incarnation);
    assert!(matches!(report.observation, Observation::ResultCandidate {
        checks_passed: false, checkpoint: None, output: None, ..
    }));
    let refusal: Outcome = serde_json::from_value(original_refusal.outcome.clone().unwrap()).unwrap();
    assert!(matches!(refusal, Outcome::Refused { code, .. } if code == "reconciliation_required"));
    assert_eq!(original_requests(&lab, &attempt).await, request_count);
    lab.restart_controller().await;
    assert_eq!(operation(&lab, &original_refusal_id).await, original_refusal);
    assert_eq!(operation(&lab, &expected.operation_id).await, cut.original);
    assert_eq!(lab.get(&submitted.id).await, uncertain);
    assert_eq!(fs::read(&phase_path).unwrap(), phase_bytes);
    let readbacks_before_recovery = transport.state.readbacks.load(Ordering::SeqCst);
    transport.state.refuse_readback.store(false, Ordering::SeqCst);
    let recovered_worker = lab.start_worker("worker-a", "transport-two", &origin);
    let recovered = lab.until(&submitted.id, "fresh incarnation reconciles original Git commit", |task| {
        task.status == TaskStatus::Failed && task.selected_checkpoint.as_ref() == Some(&cut.checkpoint)
            && task.attempt.as_ref().is_some_and(|a| a.stopped && !a.uncertain)
    }).await;
    assert_eq!(recovered.input, submitted.input);
    assert_eq!(recovered.session_id, assigned.session_id);
    let final_attempt = recovered.attempt.as_ref().unwrap();
    assert_eq!(final_attempt.id, attempt.id);
    assert_eq!(final_attempt.generation, attempt.generation);
    assert_eq!(final_attempt.worker_incarnation, attempt.worker_incarnation);
    assert_eq!(final_attempt.deadline_unix_ms, attempt.deadline_unix_ms);
    assert_eq!(original_requests(&lab, &attempt).await, request_count);
    assert!(recovered.result.is_none(), "publication recovery does not fabricate checks or acceptance");
    assert!(transport.state.readbacks.load(Ordering::SeqCst) > readbacks_before_recovery);
    let settled = operation(&lab, &expected.operation_id).await;
    assert_eq!(settled.state, "settled");
    assert_eq!(settled.intended, cut.original.intended);
    assert_eq!(settled.outcome, Some(json!({"acknowledged":true})));
    // Read the worker's actual original settlement only after its owner stops.
    lab.workers[recovered_worker].stop();
    let journal: Value = serde_json::from_slice(&fs::read(&settlement_path).unwrap()).unwrap();
    let request: Operation = serde_json::from_value(journal["request"].clone()).unwrap();
    let Operation::SettleOperation { settlement_id, operation_id, task_id, attempt_id, generation, checkpoint, .. } = &request else {
        panic!("actual settlement journal required");
    };
    assert_eq!(settlement_id, phase["settlement_id"].as_str().unwrap());
    assert_eq!(operation_id, &expected.operation_id);
    assert_eq!(task_id, &submitted.id);
    assert_eq!(attempt_id, &attempt.id);
    assert_eq!(*generation, attempt.generation);
    assert_eq!(checkpoint.as_ref(), Some(&cut.checkpoint));
    assert_eq!(journal["consumed"], true);
    let replay = lab.client.call(request.clone()).await.unwrap();
    assert_eq!(serde_json::to_value(&replay.outcome).unwrap(), journal["outcome"]);
    assert_eq!(serde_json::to_value(&replay.commit).unwrap(), journal["outcome_context"]["commit"]);
    lab.restart_controller().await;
    let reopened = lab.client.call(request).await.unwrap();
    assert_eq!(reopened.commit, replay.commit);
    assert_eq!(reopened.outcome, replay.outcome);
    assert_eq!(operation(&lab, &original_refusal_id).await, original_refusal);
    assert_eq!(lab.get(&submitted.id).await, recovered);
    assert_eq!(fs::read(&phase_path).unwrap(), phase_bytes);
    assert_eq!(transport.state.receive_packs.load(Ordering::SeqCst), 1);
    assert_eq!(lab.starts.len(), 1);
    assert!(tokio::time::timeout(Duration::from_millis(100), lab.gate.accept()).await.is_err());
    assert_eq!(git(&lab.root, &["--git-dir", lab.root.join("remote.git").to_str().unwrap(),
        "rev-parse", "--verify", &cut.checkpoint.remote_ref]), cut.checkpoint.commit);
    transport.stop().await;
    save(lab.root.join("git-transport-recovery-proof.json"), &json!({
        "cut":cut, "uncertain":uncertain, "recovered":recovered, "settled":settled,
        "settlement_replay":reopened, "transport":transport.state.counters(), "executor_starts":lab.starts,
        "scope":"real loopback Git receive-pack/status loss and same-ref recovery; controlled executor; PR creation/ACK remains unfulfilled separately",
    }));
    lab.close();
}
