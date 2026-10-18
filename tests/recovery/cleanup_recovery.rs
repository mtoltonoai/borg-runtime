//! Physical cleanup recovery cuts through the actual worker loop.
//! Controlled local Git/stdin peers, not a provider or production fault hook.
use super::*;
use borg_runtime::{collaboration::Query, worker::process::component};
use std::{collections::BTreeMap, os::unix::fs::MetadataExt};

const DISPOSABLE_FILES: usize = 8192;
const OBSERVER: &str = r#"#!/usr/bin/python3
import ctypes,json,os,select,signal,struct,sys,time
from pathlib import Path
pid=int(sys.argv[1]); expected=sys.argv[2]; directory=Path(sys.argv[3]); root=Path(sys.argv[4])
def proc():
 tail=Path('/proc/'+str(pid)+'/stat').read_text().rsplit(') ',1)[1].split()
 return tail[0],tail[19]
def save(name,value):
 p=root/(name+'.tmp'); p.write_text(json.dumps(value));
 with p.open('rb') as f: os.fsync(f.fileno())
 os.replace(p,root/name)
assert proc()[1]==expected
identity=directory.stat(); count=len(list(directory.iterdir())); assert count==8192
libc=ctypes.CDLL(None,use_errno=True)
libc.inotify_init1.argtypes=[ctypes.c_int];libc.inotify_init1.restype=ctypes.c_int
libc.inotify_add_watch.argtypes=[ctypes.c_int,ctypes.c_char_p,ctypes.c_uint32];libc.inotify_add_watch.restype=ctypes.c_int
fd=libc.inotify_init1(os.O_CLOEXEC);assert fd>=0,ctypes.get_errno()
watch=libc.inotify_add_watch(fd,os.fsencode(directory),0x200);assert watch>=0,ctypes.get_errno()
save('unlink-observer-ready.json',{'worker_pid':pid,'start_ticks':expected,'directory':str(directory),'device':identity.st_dev,'inode':identity.st_ino,'count':count})
stopped=False
try:
 assert select.select([fd],[],[],30)[0],'no actual delete event'
 data=os.read(fd,65536); offset=0; deleted=None
 while offset<len(data):
  wd,mask,cookie,size=struct.unpack_from('iIII',data,offset);offset+=16
  name=data[offset:offset+size].split(b'\0',1)[0].decode();offset+=size
  assert not mask&0x4000,'inotify overflow'
  if wd==watch and mask&0x200 and not mask&0x40000000:
   deleted=name;break
 assert deleted and deleted.startswith('disposable-')
 assert proc()[1]==expected,'owned worker identity changed'
 os.kill(pid,signal.SIGSTOP);stopped=True
 deadline=time.monotonic()+2
 while proc()[0]!='T':
  assert time.monotonic()<deadline,'owned worker did not stop'
  time.sleep(.001)
 assert proc()[1]==expected
 current=directory.stat();assert (current.st_dev,current.st_ino)==(identity.st_dev,identity.st_ino)
 remaining=len(list(directory.iterdir()))
 assert 0<remaining<count,'cut must leave real partial removal, never completed or untouched'
 assert not (directory/deleted).exists()
 save('unlink-observer-cut.json',{'worker_pid':pid,'start_ticks':expected,'directory':str(directory),'device':identity.st_dev,'inode':identity.st_ino,'original_count':count,'remaining_count':remaining,'deleted_name':deleted,'observed_state':'T'})
except BaseException:
 if stopped and proc()[1]==expected:os.kill(pid,signal.SIGCONT)
 raise
finally:os.close(fd)
"#;
const LOCAL_GIT: &str = r#"#!/usr/bin/python3
import json,os,shlex,sys
from pathlib import Path
root=Path(__file__).parent;args=sys.argv[1:]
if '-G' in args:sys.exit(1)
assert len(args)>=2 and args[-2]=='cleanup-fixture'
command=shlex.split(args[-1]);assert len(command)==2 and command[0] in ('git-upload-pack','git-receive-pack')
path=Path(command[1]).resolve(strict=True);assert path==(root/'remote.git').resolve(strict=True)
entry=(json.dumps({'service':command[0],'repository':str(path),'pid':os.getpid()})+'\n').encode()
fd=os.open(root/'cleanup-pack-entries.jsonl',os.O_WRONLY|os.O_APPEND|os.O_CREAT,0o600)
try:
 assert os.write(fd,entry)==len(entry);os.fsync(fd)
finally:os.close(fd)
os.execv('/usr/bin/git',['git',command[0].removeprefix('git-'),str(path)])
"#;

fn read(path: &Path, bound: usize) -> Vec<u8> {
    let meta = fs::symlink_metadata(path).unwrap();
    assert!(meta.file_type().is_file() && meta.len() <= bound as u64);
    let bytes = fs::read(path).unwrap();
    assert!(bytes.len() <= bound);
    bytes
}
fn value(path: &Path, bound: usize) -> Value {
    serde_json::from_slice(&read(path, bound)).unwrap()
}
fn state(lab: &Lab) -> PathBuf {
    lab.root
        .join("worker-a/.borg-worker")
        .join(component("worker-a"))
}
fn cleanup_path(lab: &Lab, attempt: &Attempt) -> PathBuf {
    state(lab)
        .join("cleanup")
        .join(format!("{}.json", component(&attempt.id)))
}
fn proc_identity(pid: u32) -> (String, String) {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    let fields = stat
        .rsplit_once(") ")
        .unwrap()
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    (fields[0].into(), fields[19].into())
}
fn selected_cleanup(lab: &Lab) {
    let mut limits = (*lab.limits).clone();
    limits.revision = format!("{}-cleanup-process-fixture", limits.revision);
    // Test-only due-now policy; neither production defaults nor accepted task
    // policy changes. A positive bounded retry still drives normal scheduling.
    limits.retention = Some(borg_runtime::limits::RetentionLimits {
        worker: borg_runtime::limits::WorkerRetentionLimits {
            success_grace_ms: 0,
            failed_grace_ms: 0,
            cancelled_grace_ms: 0,
            idle_cache_grace_ms: 0,
            batch_entries: 32,
            filesystem_entries_per_yield: 1,
            operation_timeout_ms: 30_000,
            retry_delay_ms: 10,
            record_bytes: 1_048_576,
        },
    });
    limits.validate().unwrap();
    save(lab.root.join("cleanup-limits.json"), &limits);
}
fn instrument_peer(lab: &Lab) {
    let peer = fs::read_to_string(&lab.fixture).unwrap();
    let anchor = "config = json.loads(Path(__file__).with_suffix('.json').read_text())";
    assert_eq!(peer.matches(anchor).count(), 1);
    let logging = r#"entry=(json.dumps({'pid':os.getpid(),'cwd':os.getcwd()})+'\n').encode()
fd=os.open(Path(__file__).parent/'cleanup-executor-entries.jsonl',os.O_WRONLY|os.O_APPEND|os.O_CREAT,0o600)
try:
    assert os.write(fd,entry)==len(entry);os.fsync(fd)
finally:os.close(fd)"#;
    fs::write(
        &lab.fixture,
        peer.replacen(anchor, &format!("{anchor}\n{logging}"), 1),
    )
    .unwrap();
}
fn start(lab: &mut Lab, incarnation: &str, transport: Option<&Path>) -> usize {
    let origin = lab.origin.clone();
    let root = lab.root.join("worker-a");
    let fixture = lab.fixture.clone();
    let environment = transport
        .map(|p| vec![("BORG_WORKER_GIT_SSH_COMMAND", p.to_str().unwrap())])
        .unwrap_or_default();
    let process = lab.spawn_env(
        format!("worker-a-cleanup-{incarnation}"),
        &[
            "worker",
            "--controller",
            &origin,
            "--id",
            "worker-a",
            "--incarnation",
            incarnation,
            "--workspace-root",
            root.to_str().unwrap(),
            "--codex",
            fixture.to_str().unwrap(),
            "--coding-capacity",
            "1",
            "--native-capacity",
            "0",
        ],
        &environment,
        "cleanup-limits.json",
    );
    lab.workers.push(process);
    lab.workers.len() - 1
}
fn cache(lab: &Lab) -> PathBuf {
    let entries = fs::read_dir(lab.root.join("worker-a/repositories"))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(entries.len() <= 8);
    let paths = entries
        .into_iter()
        .filter(|e| e.file_type().unwrap().is_dir())
        .map(|e| e.path())
        .collect::<Vec<_>>();
    assert_eq!(paths.len(), 1);
    paths[0].clone()
}
async fn record(lab: &Lab, attempt: &Attempt, predicate: impl Fn(&Value) -> bool) -> Value {
    let path = cleanup_path(lab, attempt);
    let end = Instant::now() + TIMEOUT;
    loop {
        if path.is_file() {
            let v = value(&path, 1_048_576);
            if predicate(&v) {
                return v;
            }
        }
        assert!(
            Instant::now() < end,
            "cleanup state not reached: {}",
            lab.root.display()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
async fn lookup(lab: &Lab, id: &str) -> Reply {
    lab.client
        .call(Operation::GetOperation {
            operation_id: id.into(),
        })
        .await
        .unwrap()
}
fn operation(reply: &Reply) -> &OperationRecord {
    match &reply.outcome {
        Outcome::Ok {
            data: Data::Operation(r),
        } => r,
        other => panic!("operation: {other:?}"),
    }
}
fn retained(lab: &Lab, attempt: &Attempt) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    let bound = lab
        .limits
        .worker
        .command_record_bytes
        .max(lab.limits.worker.mutation_record_bytes);
    let mut paths = vec![
        state(lab)
            .join("publications")
            .join(format!("{}.json", component(&attempt.id))),
    ];
    let commands = fs::read_dir(state(lab).join("commands"))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(commands.len() <= lab.limits.worker.command_records);
    for entry in commands {
        let p = entry.path();
        if p.extension().and_then(|v| v.to_str()) != Some("json") {
            continue;
        }
        let v = value(&p, bound);
        if v["command"]["attempt"]["id"] == attempt.id {
            paths.push(p);
        }
    }
    assert_eq!(
        paths.len(),
        2,
        "one original RunAttempt journal plus publication"
    );
    let codex = borg_runtime::worker::codex::state_path(&state(lab).join("codex"), &attempt.id);
    for n in [
        "borg-thread.json",
        "borg-collaboration-config.json",
        "borg-usage.json",
    ] {
        paths.push(codex.join(n));
    }
    paths
        .into_iter()
        .map(|p| {
            let bytes = if p.file_name().unwrap() == "borg-usage.json" && !p.exists() {
                None // This controlled peer emits no token-usage notification.
            } else {
                Some(read(&p, bound))
            };
            (p, bytes)
        })
        .collect()
}
fn same_files(files: &BTreeMap<PathBuf, Option<Vec<u8>>>) {
    for (p, b) in files {
        match b {
            Some(bytes) => assert_eq!(
                &fs::read(p).unwrap(),
                bytes,
                "original journal {}",
                p.display()
            ),
            None => assert!(
                !p.exists(),
                "cleanup must not fabricate absent journal {}",
                p.display()
            ),
        }
    }
}
fn one_executor(lab: &Lab) {
    let b = read(
        &lab.root.join("cleanup-executor-entries.jsonl"),
        lab.limits.transport.body_bytes,
    );
    let entries = std::str::from_utf8(&b)
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str::<Value>(s).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        entries.len(),
        1,
        "restart/cleanup cannot start another executor"
    );
    assert_eq!(entries[0]["pid"], lab.starts[0]["pid"]);
    assert_eq!(entries[0]["cwd"], lab.starts[0]["cwd"]);
}
async fn registered(lab: &Lab, incarnation: &str) {
    let end = Instant::now() + TIMEOUT;
    loop {
        let r = lab
            .client
            .call(Operation::QueryCollaboration {
                query: Query::Worker {
                    worker_id: "worker-a".into(),
                },
            })
            .await
            .unwrap();
        if matches!(r.outcome,Outcome::Ok{data:Data::Collaboration(borg_runtime::collaboration::QueryResult::Worker(v))} if v.worker.incarnation==incarnation)
        {
            break;
        }
        assert!(Instant::now() < end, "new worker registration absent");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//= type=test
//# Restart MUST reconcile incomplete cleanup before treating its resources as reclaimed.
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//= type=test
//# Cleanup MUST preserve durable task results, histories, checkpoints, audit metadata, operation receipts and recovery journals in the initial profile.
async fn restart_after_actual_cache_unlink_preserves_accepted_work_and_original_receipts() {
    let mut lab = Lab::new("cleanup-physical-unlink").await;
    selected_cleanup(&lab);
    instrument_peer(&lab);
    let submitted = lab
        .submit("cleanup-physical-submit", "cleanup-physical-agent")
        .await;
    let worker = start(&mut lab, "one", None);
    let gate = lab.gate().await;
    let assigned = lab.get(&submitted.id).await;
    let attempt = assigned.attempt.clone().unwrap();
    let cache = cache(&lab);
    let identity = (
        cache.metadata().unwrap().dev(),
        cache.metadata().unwrap().ino(),
    );
    let scratch = cache.join("disposable-fixture");
    fs::create_dir(&scratch).unwrap();
    for i in 0..DISPOSABLE_FILES {
        fs::write(scratch.join(format!("disposable-{i:05}")), b"x").unwrap();
    }
    let script = lab.root.join("unlink-observer.py");
    fs::write(&script, OBSERVER).unwrap();
    let pid = lab.workers[worker].child.id();
    let (_, start_ticks) = proc_identity(pid);
    let log = File::create(lab.root.join("unlink-observer.log")).unwrap();
    let child = Command::new("/usr/bin/python3")
        .arg(&script)
        .arg(pid.to_string())
        .arg(&start_ticks)
        .arg(&scratch)
        .arg(&lab.root)
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    let mut observer = Process {
        child,
        name: "unlink-observer".into(),
        evidence: lab.root.clone(),
    };
    let end = Instant::now() + TIMEOUT;
    while !lab.root.join("unlink-observer-ready.json").is_file() {
        assert!(
            observer.child.try_wait().unwrap().is_none(),
            "observer ended before ready"
        );
        assert!(Instant::now() < end);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let ready = value(&lab.root.join("unlink-observer-ready.json"), 8192);
    assert_eq!(ready["worker_pid"], pid);
    assert_eq!(ready["start_ticks"], start_ticks);
    gate.release().await;
    let end = Instant::now() + TIMEOUT;
    while !lab.root.join("unlink-observer-cut.json").is_file() {
        if let Some(status) = observer.child.try_wait().unwrap() {
            assert!(
                status.success(),
                "observer did not establish partial unlink cut"
            );
            // It may publish the atomic cut and exit between our path check
            // and wait. A successful exit still requires that exact evidence.
            assert!(lab.root.join("unlink-observer-cut.json").is_file());
            break;
        }
        assert!(Instant::now() < end, "actual unlink cut absent");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let observer_status = observer.child.wait().unwrap();
    assert!(observer_status.success());
    save(
        lab.root.join("unlink-observer-closed.json"),
        &json!({
            "pid":observer.child.id(),"reaped":true,"exit_code":observer_status.code()
        }),
    );
    let cut = value(&lab.root.join("unlink-observer-cut.json"), 8192);
    assert_eq!(cut["worker_pid"], pid);
    assert_eq!(cut["start_ticks"], start_ticks);
    assert_eq!(proc_identity(pid), ("T".into(), start_ticks));
    let remaining = cut["remaining_count"].as_u64().unwrap();
    assert!(remaining > 0 && remaining < DISPOSABLE_FILES as u64);
    assert_eq!(fs::read_dir(&scratch).unwrap().count() as u64, remaining);
    assert_eq!(
        (
            cache.metadata().unwrap().dev(),
            cache.metadata().unwrap().ino()
        ),
        identity
    );
    let removing = value(&cleanup_path(&lab, &attempt), 1_048_576);
    assert_eq!(removing["disposition"], "removing");
    assert_eq!(removing["cache_started"], true);
    assert_eq!(removing["cache_done"], false);
    assert!(
        removing["worktrees"]
            .as_array()
            .unwrap()
            .iter()
            .all(|s| s["done"] == true)
    );
    let barrier = cache.with_file_name(format!(
        "{}.cleanup.json",
        cache.file_name().unwrap().to_str().unwrap()
    ));
    assert_eq!(value(&barrier, 1_048_576), removing["cache"]);
    let accepted = lab.get(&submitted.id).await;
    assert_eq!(accepted.status, TaskStatus::Succeeded);
    assert!(
        accepted.attempt.as_ref().unwrap().stopped && !accepted.attempt.as_ref().unwrap().uncertain
    );
    assert_eq!(accepted.input, submitted.input);
    let cp = accepted.selected_checkpoint.clone().unwrap();
    let receipt = lookup(&lab, &cp.operation_id).await;
    assert_eq!(operation(&receipt).state, "settled");
    let history_op = Operation::QueryCollaboration {
        query: Query::TaskHistory {
            task_id: accepted.id.clone(),
            after: None,
            limit: 100,
        },
    };
    let history = lab.client.call(history_op.clone()).await.unwrap();
    let files = retained(&lab, &attempt);
    let remote_refs = git(
        &lab.root,
        &[
            "--git-dir",
            &cp.repository,
            "for-each-ref",
            "--format=%(refname) %(objectname)",
        ],
    );
    lab.workers[worker].kill();
    assert_eq!(fs::read_dir(&scratch).unwrap().count() as u64, remaining);
    lab.restart_controller().await;
    assert_eq!(lab.get(&accepted.id).await, accepted);
    let next = start(&mut lab, "two", None);
    registered(&lab, "two").await;
    let reclaimed = record(&lab, &attempt, |v| {
        v["disposition"] == "reclaimed" && v["cache_done"] == true
    })
    .await;
    assert_eq!(
        reclaimed["cache"], removing["cache"],
        "original removal proof survives restart"
    );
    assert!(!cache.exists() && !barrier.exists());
    same_files(&files);
    assert_eq!(lab.get(&accepted.id).await, accepted);
    assert_eq!(
        lab.client.call(history_op).await.unwrap().outcome,
        history.outcome
    );
    assert_eq!(
        lookup(&lab, &cp.operation_id).await.outcome,
        receipt.outcome
    );
    // Replay the exact archived settlement request; cleanup never grants a new
    // publication or substitutes an outcome from current repository state.
    let journals = fs::read_dir(state(&lab).join("mutations/settlements"))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(journals.len() <= lab.limits.worker.command_records);
    let requests=journals.into_iter().filter_map(|e|{
        let v=value(&e.path(),lab.limits.worker.mutation_record_bytes);
        let request:Operation=serde_json::from_value(v["request"].clone()).unwrap();
        matches!(&request,Operation::SettleOperation{operation_id,..} if operation_id==&cp.operation_id).then_some(request)
    }).collect::<Vec<_>>();
    assert_eq!(requests.len(), 1);
    let original_id = match &requests[0] {
        Operation::SettleOperation { settlement_id, .. } => settlement_id,
        _ => unreachable!(),
    };
    let original_receipt = lookup(&lab, original_id).await;
    let replay = lab.client.call(requests[0].clone()).await.unwrap();
    assert_eq!(
        Some(serde_json::to_value(&replay.outcome).unwrap()),
        operation(&original_receipt).outcome
    );
    assert_eq!(replay.commit, original_receipt.commit);
    assert_eq!(
        git(
            &lab.root,
            &[
                "--git-dir",
                &cp.repository,
                "for-each-ref",
                "--format=%(refname) %(objectname)"
            ]
        ),
        remote_refs
    );
    lab.workers[next].stop();
    one_executor(&lab);
    same_files(&files);
    save(
        lab.root.join("physical-cleanup-recovery.json"),
        &json!({"cut":cut,"removing":removing,"reclaimed":reclaimed,"accepted":accepted,"original_operation":receipt,"scope":"actual cache unlink/SIGKILL/fresh worker; controlled local processes only"}),
    );
    lab.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//= type=test
//# Cleanup MUST preserve data required to reconcile an uncertain or unacknowledged operation.
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//= type=test
//# Workspace cleanup MUST preserve source edits required to complete or reconcile an outstanding checkpoint publication.
async fn restart_retains_uncertain_publication_material_despite_due_cleanup_policy() {
    let mut lab = Lab::new("cleanup-uncertain-publication").await;
    selected_cleanup(&lab);
    instrument_peer(&lab);
    let transport = lab.root.join("cleanup-git.py");
    fs::write(&transport, LOCAL_GIT).unwrap();
    fs::set_permissions(&transport, fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = format!("cleanup-fixture:{}", lab.root.join("remote.git").display());
    let registration = borg_runtime::repository_catalogue::Registration {
        repository_id: "cleanup-publication".into(),
        registration_revision: "one".into(),
        display_name: "Cleanup uncertainty fixture".into(),
        source_endpoint: endpoint.clone(),
        checkpoint_endpoint: endpoint.clone(),
        source_store_key: "remote.git".into(),
        checkpoint_store_key: "remote.git".into(),
        pull_request_target: None,
    };
    assert!(matches!(
        lab.client
            .call(Operation::RegisterRepository {
                operation_id: "cleanup-register".into(),
                registration: registration.clone()
            })
            .await
            .unwrap()
            .outcome,
        Outcome::Ok { .. }
    ));
    lab.repository.registration = Some(registration.binding());
    lab.repository.url = endpoint.clone();
    lab.repository.checkpoint_remote = endpoint;
    let submitted = lab
        .submit("cleanup-uncertain-submit", "cleanup-uncertain-agent")
        .await;
    let worker = start(&mut lab, "one", Some(&transport));
    let gate = lab.gate().await;
    let assigned = lab.get(&submitted.id).await;
    let attempt = assigned.attempt.clone().unwrap();
    let workspace = PathBuf::from(gate.observed["cwd"].as_str().unwrap());
    let expected = borg_runtime::worker::git::checkpoint(
        &lab.repository,
        &submitted.id,
        &attempt.id,
        String::new(),
    )
    .unwrap();
    let lock = lab
        .root
        .join("remote.git")
        .join(format!("{}.lock", expected.remote_ref));
    fs::create_dir_all(lock.parent().unwrap()).unwrap();
    let mut owned = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&lock)
        .unwrap();
    use std::io::Write;
    owned
        .write_all(b"owned cleanup uncertainty fixture\n")
        .unwrap();
    owned.sync_all().unwrap();
    drop(owned);
    let lock_bytes = fs::read(&lock).unwrap();
    let before_refs = git(
        &lab.root.join("remote.git"),
        &["for-each-ref", "--format=%(refname) %(objectname)"],
    );
    gate.release().await;
    let stopped = lab
        .until(&submitted.id, "real publication refusal", |t| {
            t.attempt.as_ref().is_some_and(|a| a.stopped && a.uncertain)
        })
        .await;
    assert!(stopped.result.is_none() && stopped.selected_checkpoint.is_none());
    // A possibly sent, unretired publication deliberately leaves its command
    // incomplete. Wait for the actual result/refusal/notice archive instead of
    // treating command completion or cleanup admission as the report barrier.
    let outbox = state(&lab).join("outbox");
    let end = Instant::now() + TIMEOUT;
    let (acceptance_path, acceptance) = loop {
        let directory = outbox.join("acceptance");
        let mut settled = Vec::new();
        if directory.exists() {
            let entries = fs::read_dir(&directory)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert!(entries.len() <= lab.limits.worker.outbox_records);
            for entry in entries {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("settled") {
                    let journal = value(&path, lab.limits.worker.mutation_record_bytes);
                    if journal["original"]["attempt_id"] == attempt.id {
                        settled.push((path, journal));
                    }
                }
            }
        }
        assert!(
            settled.len() <= 1,
            "one original result acceptance decision"
        );
        let entries = fs::read_dir(&outbox)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(entries.len() <= lab.limits.worker.outbox_records);
        let pending = entries
            .iter()
            .any(|entry| entry.path().extension().and_then(|s| s.to_str()) == Some("json"));
        if !pending && settled.len() == 1 {
            break settled.pop().unwrap();
        }
        assert!(
            Instant::now() < end,
            "original result/refusal/notice not settled"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(acceptance["reconciliation"], Value::Null);
    assert_eq!(acceptance["outcome"], acceptance["refusal"]);
    assert_eq!(acceptance["outcome_context"], acceptance["refusal_context"]);
    let original_report: Report = serde_json::from_value(acceptance["original"].clone()).unwrap();
    let notice_report: Report = serde_json::from_value(acceptance["notice"].clone()).unwrap();
    for report in [&original_report, &notice_report] {
        assert_eq!(report.task_id, submitted.id);
        assert_eq!(report.attempt_id, attempt.id);
        assert_eq!(report.generation, attempt.generation);
        assert_eq!(report.worker_id, attempt.worker_id);
        assert_eq!(report.incarnation, attempt.worker_incarnation);
    }
    assert!(matches!(
        &original_report.observation,
        Observation::ResultCandidate {
            checks_passed: false,
            checkpoint: None,
            ..
        }
    ));
    const NOTICE: &str =
        "Completed result acceptance is refused; original decision retained for reconciliation";
    assert!(matches!(&notice_report.observation,
        Observation::Progress { text, output, truncated: false } if text == NOTICE && output.is_empty()));
    let result_receipt = lookup(&lab, &original_report.report_id).await;
    let notice_receipt = lookup(&lab, &notice_report.report_id).await;
    for (report, receipt) in [
        (&original_report, &result_receipt),
        (&notice_report, &notice_receipt),
    ] {
        assert_eq!(operation(receipt).state, "settled");
        assert_eq!(
            operation(receipt).intended,
            serde_json::to_value(Operation::ObserveWorker {
                report: report.clone()
            })
            .unwrap()
        );
        assert_eq!(
            receipt.commit.as_ref().unwrap().operation_id,
            report.report_id
        );
    }
    assert_eq!(
        operation(&result_receipt).outcome.as_ref().unwrap(),
        &acceptance["refusal"]
    );
    assert!(
        matches!(serde_json::from_value::<Outcome>(acceptance["refusal"].clone()).unwrap(),
        Outcome::Refused { code, message } if code == "reconciliation_required"
            && message == "execution/effect outcome remains uncertain")
    );
    assert_eq!(
        acceptance["refusal_context"]["commit"],
        json!(result_receipt.commit.as_ref().unwrap())
    );
    assert!(matches!(
        serde_json::from_value::<Outcome>(operation(&notice_receipt).outcome.clone().unwrap())
            .unwrap(),
        Outcome::Ok {
            data: Data::Task(_)
        }
    ));
    let refused_path = outbox.join(format!("{}.refused", component(&original_report.report_id)));
    assert_eq!(
        value(&refused_path, lab.limits.worker.mutation_record_bytes),
        json!(original_report)
    );
    let refusal_path = refused_path.with_extension("outcome");
    assert_eq!(
        value(&refusal_path, lab.limits.worker.mutation_record_bytes),
        json!({"outcome":acceptance["refusal"],"context":acceptance["refusal_context"]})
    );
    let stopped = lab.get(&submitted.id).await;
    assert_eq!(stopped.status, TaskStatus::Active);
    assert_eq!(stopped.progress, NOTICE);
    assert!(
        stopped.attempt.as_ref().unwrap().stopped && stopped.attempt.as_ref().unwrap().uncertain
    );
    let cancelled = task(
        lab.client
            .call(Operation::CancelTask {
                operation_id: "cleanup-cancel-pending".into(),
                task_id: stopped.id.clone(),
                expected_revision: stopped.revision,
            })
            .await
            .unwrap(),
    );
    assert_eq!(cancelled.status, TaskStatus::Cancelled);
    let held = record(&lab, &attempt, |v| {
        v["reason"] == "local_recovery" && v["disposition"] == "retained"
    })
    .await;
    assert!(
        held["worktrees"]
            .as_array()
            .unwrap()
            .iter()
            .all(|v| v["started"] == false && v["done"] == false)
    );
    assert_eq!(held["cache_started"], false);
    assert_eq!(held["cache_done"], false);
    let original = lookup(&lab, &expected.operation_id).await;
    assert_ne!(operation(&original).state, "settled");
    assert_eq!(operation(&original).attempt_id, attempt.id);
    assert_eq!(
        operation(&original).intended["registration"],
        serde_json::to_value(registration.binding()).unwrap()
    );
    let cache = cache(&lab);
    let marker = read(
        &cache.join("identity.json"),
        lab.limits.worker.git_cache_identity_bytes,
    );
    let source = fs::read(workspace.join("answer.txt")).unwrap();
    assert_eq!(source, b"after\n");
    let mut files = retained(&lab, &attempt);
    let command_path = files
        .keys()
        .find(|p| p.parent().unwrap().ends_with("commands"))
        .unwrap();
    let command = value(command_path, lab.limits.worker.command_record_bytes);
    assert_eq!(command["completed"], false);
    assert_eq!(command["effects_started"], true);
    let phase = value(
        &state(&lab)
            .join("publications")
            .join(format!("{}.json", component(&attempt.id))),
        lab.limits.worker.mutation_record_bytes,
    );
    assert_eq!(phase["command"], command["command"]);
    assert_eq!(phase["command"]["command_id"], held["command_id"]);
    assert_eq!(phase["retired_for_checks"], false);
    assert_eq!(phase["publish_may_have_started"], true);
    assert_eq!(phase["prepared"], json!(operation(&original)));
    assert_eq!(phase["checkpoint"]["operation_id"], expected.operation_id);
    assert_eq!(
        phase["checkpoint"]["registration"],
        json!(registration.binding())
    );
    assert_eq!(phase["checkpoint"]["remote_ref"], expected.remote_ref);
    let local_commit = git(&workspace, &["rev-parse", "HEAD"]);
    assert_eq!(phase["checkpoint"]["commit"], local_commit);
    let settlement_path = state(&lab)
        .join("mutations/settlements")
        .join(format!("{}.json", component(&expected.operation_id)));
    assert!(
        !settlement_path.exists(),
        "failed Git publication never reached settlement"
    );
    let registration_path = state(&lab).join("mutations/registration.json");
    for path in [
        acceptance_path.clone(),
        refused_path,
        refusal_path,
        registration_path,
    ] {
        let bytes = read(&path, lab.limits.worker.mutation_record_bytes);
        files.insert(path, Some(bytes));
    }
    files.insert(settlement_path, None);
    let held_bytes = read(&cleanup_path(&lab, &attempt), 1_048_576);
    let pack_bytes = read(
        &lab.root.join("cleanup-pack-entries.jsonl"),
        lab.limits.transport.body_bytes,
    );
    let pushes = std::str::from_utf8(&pack_bytes)
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str::<Value>(s).unwrap())
        .filter(|v| v["service"] == "git-receive-pack")
        .collect::<Vec<_>>();
    assert_eq!(
        pushes.len(),
        1,
        "one actual failed receive-pack, not network ACK loss"
    );
    assert_eq!(
        git(
            &lab.root.join("remote.git"),
            &["for-each-ref", "--format=%(refname) %(objectname)"]
        ),
        before_refs
    );
    lab.workers[worker].stop();
    let stopped_hold = value(&cleanup_path(&lab, &attempt), 1_048_576);
    assert_eq!(stopped_hold["reason"], "local_recovery");
    let stopped_hold_bytes = read(&cleanup_path(&lab, &attempt), 1_048_576);
    same_files(&files);
    lab.restart_controller().await;
    assert!(
        lab.get(&submitted.id)
            .await
            .attempt
            .as_ref()
            .unwrap()
            .uncertain
    );
    let next = start(&mut lab, "two", Some(&transport));
    // Cold recovery must actually read back the original possibly sent phase
    // and refuse admission before registration or another cleanup pass. A
    // positive exact diagnostic, rather than a timeout, establishes this cut.
    let recovery_log = lab.root.join("worker-a-cleanup-two.log");
    const RECOVERY_REFUSAL: &str = "Worker recovery admission refused: empty readback cannot settle a possibly sent publication";
    let end = Instant::now() + TIMEOUT;
    let recovery_bytes = loop {
        let bytes = read(&recovery_log, lab.limits.transport.body_bytes);
        if std::str::from_utf8(&bytes)
            .unwrap()
            .lines()
            .any(|line| line == RECOVERY_REFUSAL)
        {
            break bytes;
        }
        assert!(
            lab.workers[next].child.try_wait().unwrap().is_none(),
            "fresh worker exited before original publication readback: {}",
            String::from_utf8_lossy(&bytes)
        );
        assert!(
            Instant::now() < end,
            "exact original publication recovery refusal absent: {}",
            String::from_utf8_lossy(&bytes)
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    lab.workers[next].stop();
    let registration = lab
        .client
        .call(Operation::QueryCollaboration {
            query: Query::Worker {
                worker_id: "worker-a".into(),
            },
        })
        .await
        .unwrap();
    assert!(
        matches!(&registration.outcome,
        Outcome::Ok { data: Data::Collaboration(borg_runtime::collaboration::QueryResult::Worker(v)) }
            if v.worker.incarnation == "one"),
        "cold refusal must precede fresh registration"
    );
    assert_eq!(
        read(&cleanup_path(&lab, &attempt), 1_048_576),
        stopped_hold_bytes
    );
    let again = value(&cleanup_path(&lab, &attempt), 1_048_576);
    assert_eq!(again["reason"], "local_recovery");
    assert_eq!(again["disposition"], "retained");
    assert_eq!(again["command_id"], held["command_id"]);
    assert_eq!(again["attempt_id"], attempt.id);
    assert_eq!(again["cache_started"], false);
    assert_eq!(again["worktrees"], held["worktrees"]);
    assert_eq!(
        lookup(&lab, &expected.operation_id).await.outcome,
        original.outcome
    );
    let current = lab.get(&submitted.id).await;
    assert!(current.attempt.as_ref().unwrap().uncertain);
    assert_eq!(current.input, submitted.input);
    assert!(current.result.is_none() && current.selected_checkpoint.is_none());
    assert_eq!(
        refused(
            lab.client
                .call(Operation::RetryTask {
                    operation_id: "cleanup-no-unknown-retry".into(),
                    task_id: current.id.clone(),
                    expected_revision: current.revision
                })
                .await
                .unwrap()
        ),
        "reconciliation_required"
    );
    assert_eq!(fs::read(workspace.join("answer.txt")).unwrap(), source);
    assert_eq!(fs::read(cache.join("identity.json")).unwrap(), marker);
    assert_eq!(fs::read(&lock).unwrap(), lock_bytes);
    same_files(&files);
    let after_pack_bytes = read(
        &lab.root.join("cleanup-pack-entries.jsonl"),
        lab.limits.transport.body_bytes,
    );
    let upload_count = |bytes: &[u8]| {
        std::str::from_utf8(bytes)
            .unwrap()
            .lines()
            .map(|s| serde_json::from_str::<Value>(s).unwrap())
            .filter(|v| v["service"] == "git-upload-pack")
            .count()
    };
    assert!(
        upload_count(&after_pack_bytes) > upload_count(&pack_bytes),
        "fresh process must perform the actual original remote readback"
    );
    let after_pushes = std::str::from_utf8(&after_pack_bytes)
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str::<Value>(s).unwrap())
        .filter(|v| v["service"] == "git-receive-pack")
        .collect::<Vec<_>>();
    assert_eq!(
        after_pushes, pushes,
        "original readbacks may run; the failed push must not repeat"
    );
    one_executor(&lab);
    assert_eq!(
        git(
            &lab.root.join("remote.git"),
            &["for-each-ref", "--format=%(refname) %(objectname)"]
        ),
        before_refs
    );
    for (report, original_receipt) in [
        (&original_report, &result_receipt),
        (&notice_report, &notice_receipt),
    ] {
        let reopened_receipt = lookup(&lab, &report.report_id).await;
        assert_eq!(reopened_receipt.outcome, original_receipt.outcome);
        assert_eq!(reopened_receipt.commit, original_receipt.commit);
    }
    save(
        lab.root.join("uncertain-cleanup-held.json"),
        &json!({"original_operation":original,"command":command,"phase":phase,"acceptance":acceptance,"result_receipt":result_receipt,"notice_receipt":notice_receipt,"registration":registration,"recovery_log":String::from_utf8(recovery_bytes).unwrap(),"held":held,"reopened":again,"before_cleanup_record_bytes":held_bytes.len(),"task":current,"scope":"actual ref-lock refusal and cold original readback refusal; no lost-network-ACK claim or reconciliation authorization"}),
    );
    lab.close();
}
