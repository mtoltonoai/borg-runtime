use borg_runtime::{
    protocol::*,
    worker::{
        self, codex, git,
        process::{self, Control},
    },
};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, atomic::{AtomicU64, Ordering}},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::watch;

fn limits() -> Arc<borg_runtime::limits::RuntimeLimits> {
    Arc::new(borg_runtime::limits::RuntimeLimits::seed().unwrap())
}
fn write_limits(root: &Path, policy: &borg_runtime::limits::RuntimeLimits) -> PathBuf {
    let path = root.join("limits.json");
    process::atomic_json(&path, policy).unwrap();
    path
}
static SEQUENCE: AtomicU64 = AtomicU64::new(1);
fn area(label: &str) -> PathBuf {
    let base =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("artifacts/v2/build-60156282/worker/tests");
    std::fs::create_dir_all(&base).unwrap();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let p = base.join(format!(
        "{label}-{timestamp}-{}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    // PID namespaces recycle IDs between tool calls. Atomic leaf allocation
    // refuses a collision instead of silently reusing earlier test evidence.
    std::fs::create_dir(&p).expect("worker fixture directory must be new");
    p
}
fn control() -> (Control, watch::Sender<bool>) {
    let (tx, rx) = watch::channel(false);
    let runtime_limits = limits();
    let worker_limits = Arc::new(runtime_limits.worker.clone());
    let validation_runtime = Arc::new(borg_runtime::native::ValidationRuntime::new(worker_limits.clone()).unwrap());
    (
        Control {
            runtime_limits,
            worker_limits,
            validation_runtime,
            deadline_ms: process::now_ms() + 60_000,
            cancel: rx,
        },
        tx,
    )
}
fn process_records(path: &Path) -> usize {
    std::fs::read_dir(path)
        .unwrap()
        .flatten()
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("json"))
        .count()
}
fn run_git(cwd: &Path, args: &[&str]) -> String {
    let result = std::process::Command::new("/usr/bin/git")
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "user.name=Worker Test",
            "-c",
            "user.email=worker@example.invalid",
        ])
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "fixture Git failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    String::from_utf8(result.stdout).unwrap().trim().to_owned()
}
fn repository(root: &Path) -> Repository {
    let seed = root.join("seed");
    let remote = root.join("remote.git");
    std::fs::create_dir(&seed).unwrap();
    run_git(&seed, &["init", "-q"]);
    std::fs::write(seed.join("answer.txt"), "before\n").unwrap();
    run_git(&seed, &["add", "answer.txt"]);
    run_git(&seed, &["commit", "-qm", "initial"]);
    let commit = run_git(&seed, &["rev-parse", "HEAD"]);
    run_git(
        root,
        &[
            "clone",
            "--bare",
            "--quiet",
            seed.to_str().unwrap(),
            remote.to_str().unwrap(),
        ],
    );
    Repository {
        registration: Some(borg_runtime::repository_catalogue::Revision {
            repository_id: "fixture-repository".into(),
            registration_revision: "fixture-revision".into(),
        }),
        url: remote.to_string_lossy().into_owned(),
        start_commit: commit,
        checkpoint_remote: remote.to_string_lossy().into_owned(),
    }
}

#[tokio::test]
async fn command_output_is_bounded_and_deadline_stops_group() {
    // Traceability scope: This test covers output/deadline, not all CPU/memory/disk/resource limits.
    //= spec/impl/v2/implementation.md#workers-are-trusted-disposable-environments
    //# A worker MUST enforce the attempt's execution deadline.
    let root = area("process");
    let registry = root.join("processes");
    let (c, _cancel) = control();
    let split = process::command(
        Path::new("/bin/sh"),
        &[
            "-c".into(),
            "printf commit-identity; printf diagnostic-warning >&2".into(),
        ],
        &root,
        &registry,
        &c,
        1024,
    )
    .await
    .unwrap();
    assert_eq!(split.stdout, "commit-identity");
    assert!(split.text.contains("diagnostic-warning"));
    let output = process::command(
        Path::new("/bin/sh"),
        &["-c".into(), "head -c 200000 /dev/zero".into()],
        &root,
        &registry,
        &c,
        1024,
    )
    .await
    .unwrap();
    assert!(output.success && output.truncated);
    assert!(output.text.len() <= 1024);
    let deadline = Control {
        deadline_ms: process::now_ms() + 100,
        ..c.clone()
    };
    let stopped = process::command(
        Path::new("/bin/sh"),
        &["-c".into(), "sleep 30 & wait".into()],
        &root,
        &registry,
        &deadline,
        1024,
    )
    .await;
    assert!(stopped.is_err());
    assert_eq!(process_records(&registry), 0);
}

#[tokio::test]
async fn cancellation_accounts_for_child_with_separate_session() {
    //= spec/impl/v2/implementation.md#interrupted-work-is-reconciled
    //# The platform MUST NOT report an interrupted command as stopped until its worker confirms termination or the worker is known unable to continue it.
    let root = area("cancel-tree");
    let registry = root.join("processes");
    let (c, cancel) = control();
    let argv = ["-c".into(), "setsid sleep 30 & wait".into()];
    let child = process::command(Path::new("/bin/sh"), &argv, &root, &registry, &c, 1024);
    let trigger = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel.send(true).unwrap();
    };
    let (result, ()) = tokio::join!(child, trigger);
    assert!(result.is_err());
    assert_eq!(process_records(&registry), 0);
}

#[test]
fn worker_owner_prevents_two_local_incarnations() {
    let root = area("lease");
    let path = root.join("owner.json");
    let lease = process::WorkerLease::claim(&path, &limits().worker).unwrap();
    let held = match process::WorkerLease::claim(&path, &limits().worker) {
        Err(error) => error,
        Ok(_) => panic!("a held worker lease admitted a second owner"),
    };
    assert!(held.contains("WouldBlock"), "held owner error: {held}");
    drop(lease);
    let _replacement = process::WorkerLease::claim(&path, &limits().worker).unwrap_or_else(|error| {
        panic!(
            "lease reacquire after owner drop failed: {error}; owner_record_present={}",
            path.exists()
        )
    });
}

fn launch() -> WorkerCommand {
    let definition = Definition {
                limit_overrides: None,
                inference: Some(borg_runtime::protocol::InferenceSettings::selected()),
                execution_profile: Some(borg_runtime::protocol::ExecutionProfile::CollaborationV1 { web_search: None }),
                id: "coding".into(),
                revision: "revision-1".into(),
                executor: ExecutorKind::Coding,
            };
    let mut command = WorkerCommand::RunAttempt {
        command_id: "command-1".into(),
        session: Session { id: "session-1".into(), agent_id: definition.id.clone(), definition: definition.clone(), task_id: "task-1".into() },
        attempt: Attempt {
            session_id: "session-1".into(),
            id: "attempt-1".into(),
            task_id: "task-1".into(),
            generation: 1,
            worker_id: "worker-1".into(),
            worker_incarnation: "incarnation-1".into(),
            phase: AttemptPhase::Assigned,
            deadline_unix_ms: process::now_ms() + 60_000,
            stopped: false,
            uncertain: false,
        },
        task: TaskInput {
            resolved_limits: None,
            instruction: "edit the source".into(),
            definition: definition.clone(),
            repository: None,
            repository_read: None,
            acceptance_commands: vec![],
            output_schema: None,
            limits: Limits {
                deadline_unix_ms: process::now_ms() + 60_000,
                model_requests: 1,
                output_bytes: 4096,
                result_bytes: limits().task_defaults.result_bytes,
                max_delegation_depth: 4,
            },
            dependencies: vec![],
            native_workflow: None,
        },
        checkpoint: None,
        continuation: None,
    };
    if let WorkerCommand::RunAttempt { task, .. } = &mut command {
        task.resolved_limits = Some(limits().resolve_task(task).unwrap());
    }
    command
}

#[test]
// Traceability scope: Persists full command and rejects differing input under the same command identity.
//= spec/impl/v2/implementation.md#worker-commands-do-not-create-duplicate-execution
//= type=test
//# A launch command MUST bind its attempt, generation, worker incarnation and payload identity.
fn durable_duplicate_launch_and_payload_conflict() {
    //= spec/impl/v2/implementation.md#worker-commands-do-not-create-duplicate-execution
    //# A duplicate launch in the same worker incarnation MUST return the existing execution or outcome instead of starting another execution.
    let root = area("dedupe");
    let path = root.join("command.json");
    let command = launch();
    assert!(worker::record_launch(&path, &command, &limits()).unwrap());
    assert!(!worker::record_launch(&path, &command, &limits()).unwrap());
    let mut changed = command.clone();
    if let WorkerCommand::RunAttempt { task, .. } = &mut changed {
        task.instruction = "different source changes".into();
    }
    assert!(worker::record_launch(&path, &changed, &limits()).is_err());
    assert!(!worker::record_launch(&path, &command, &limits()).unwrap());
}

#[tokio::test]
// This same-worker bare-Git fixture checks exact refetch and replacement source.
// It does not establish availability outside the execution worker.
// Traceability scope: Local bare-Git exact-reference readback, repeated publication and conflict refusal; no lost-ack injection in this test.
//= spec/impl/v2/implementation.md#checkpoint-publication-is-reconciled
//= type=test
//# A publication with an uncertain outcome MUST be reconciled against its original remote reference and commit.
// Traceability scope: Local bare repository only; exact Git identity/acknowledgment, idempotent publication, replacement checkout and conflicting-ref refusal.
//= spec/impl/v2/implementation.md#work-starts-from-a-durable-source
//= type=test
//# A checkpoint MUST identify the exact work it preserves.
// Traceability scope: Local bare repository only; exact Git identity/acknowledgment, idempotent publication, replacement checkout and conflicting-ref refusal.
//= spec/impl/v2/implementation.md#work-starts-from-a-durable-source
//= type=test
//# A workspace MUST identify its repository and starting revision.
async fn checkpoint_is_remote_exact_idempotent_and_not_a_dirty_tree() {
    //= spec/impl/v2/implementation.md#work-starts-from-a-durable-source
    //# A checkpoint MUST NOT be reported durable before its external storage acknowledges it.
    //= spec/impl/v2/implementation.md#checkpoint-publication-is-reconciled
    //# Checkpoint publication MUST NOT overwrite an unrelated remote reference.
    let root = area("checkpoint");
    let repo = repository(&root);
    let registry = root.join("processes");
    let (c, _cancel) = control();
    let workspace = git::checkout(
        &root.join("workspaces"),
        "attempt/one",
        &repo,
        None,
        &registry,
        &c,
    )
    .await
    .unwrap();
    std::fs::write(workspace.join("answer.txt"), "after\n").unwrap();
    let commit = git::commit_source(&workspace, "attempt/one", &registry, &c)
        .await
        .unwrap();
    assert_ne!(commit, repo.start_commit);
    let checkpoint = git::checkpoint(&repo, "task/one", "attempt/one", commit.clone()).unwrap();
    assert_eq!(
        git::remote_commit(&workspace, &checkpoint, &registry, &c)
            .await
            .unwrap(),
        None
    );
    git::publish(&workspace, &checkpoint, &registry, &c)
        .await
        .unwrap();
    git::publish(&workspace, &checkpoint, &registry, &c)
        .await
        .unwrap();
    assert_eq!(
        git::remote_commit(&workspace, &checkpoint, &registry, &c)
            .await
            .unwrap(),
        Some(commit)
    );
    //= spec/impl/v2/implementation.md#interrupted-work-is-reconciled
    //# Recovery MUST NOT assume that uncheckpointed workspace content survived worker loss.
    std::fs::write(workspace.join("answer.txt"), "uncheckpointed\n").unwrap();
    //= spec/impl/v2/implementation.md#loss-returns-to-the-last-checkpoint
    //# A replacement workspace MUST be reconstructed from an identified durable checkpoint.
    let replacement = git::checkout(
        &root.join("workspaces"),
        "attempt/two",
        &repo,
        Some(&checkpoint),
        &registry,
        &c,
    )
    .await
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(replacement.join("answer.txt")).unwrap(),
        "after\n"
    );
    let mut conflicting = checkpoint.clone();
    conflicting.commit = repo.start_commit.clone();
    assert!(
        git::publish(&workspace, &conflicting, &registry, &c)
            .await
            .is_err()
    );
    assert!(
        git::checkout(
            &root.join("workspaces"),
            "attempt/three",
            &repo,
            Some(&conflicting),
            &registry,
            &c
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn checkpoint_checks_cannot_depend_on_excluded_working_files() {
    // Traceability scope: This fixture checks source and excluded files, not exact executable provenance.
    let root = area("clean-acceptance");
    let repo = repository(&root);
    let registry = root.join("processes");
    let (c, _cancel) = control();
    let source = git::checkout(
        &root.join("workspaces"),
        "editing-attempt",
        &repo,
        None,
        &registry,
        &c,
    )
    .await
    .unwrap();
    std::fs::write(source.join("answer.txt"), "after\n").unwrap();
    std::fs::write(source.join(".env"), "fixture-only\n").unwrap();
    run_git(&source, &["add", ".env"]);
    std::fs::write(source.join(".gitignore"), "/cache.txt\n").unwrap();
    std::fs::write(source.join("cache.txt"), "working-tree-only\n").unwrap();
    let commit = git::commit_source(&source, "editing-attempt", &registry, &c)
        .await
        .unwrap();
    assert!(
        run_git(&source, &["ls-tree", "--name-only", "HEAD"])
            .lines()
            .all(|p| p != ".env" && p != "cache.txt")
    );
    let checkpoint = git::checkpoint(&repo, "task-clean", "editing-attempt", commit).unwrap();
    git::publish(&source, &checkpoint, &registry, &c)
        .await
        .unwrap();
    let clean = git::checkout(
        &root.join("validation"),
        "editing-attempt",
        &repo,
        Some(&checkpoint),
        &registry,
        &c,
    )
    .await
    .unwrap();
    assert!(!clean.join(".env").exists());
    assert!(!clean.join("cache.txt").exists());
    assert!(
        git::source_unchanged(&clean, &checkpoint.commit, &registry, &c)
            .await
            .unwrap()
    );
    std::fs::write(clean.join("answer.txt"), "checker mutated input\n").unwrap();
    assert!(
        !git::source_unchanged(&clean, &checkpoint.commit, &registry, &c)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn actual_pinned_codex_handshake_without_a_model_turn() {
    //= spec/impl/v2/implementation.md#the-supplied-executor-owns-its-inner-loop
    //# The supplied executor adapter MUST use the documented interface of its pinned executor release.
    let binary = std::env::var_os("BORG_QUALIFICATION_CODEX")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(
            "/home/dev-user/.cache/dotslash/5b/35b49a2cd6bf588d5594902cb316e9cb1a777d/bin/codex",
        ));
    assert!(binary.is_absolute() && binary.is_file());
    let hash = std::process::Command::new("/usr/bin/sha256sum")
        .arg("--")
        .arg(&binary)
        .output()
        .unwrap();
    assert!(hash.status.success());
    assert_eq!(
        String::from_utf8(hash.stdout)
            .unwrap()
            .split_whitespace()
            .next(),
        Some("70000c54b0eef44deabaddd249533c757533c8ab80d3906d73681029d3f23072")
    );
    // This one pinned-binary qualification can run as the deployed worker UID
    // with an operator-selected TMPDIR. It never writes the compiled checkout.
    let root = std::env::temp_dir().join(format!(
        "borg-codex-handshake-{}-{}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let (c, _cancel) = control();
    let mut session = codex::Session::start(
        &binary,
        &root,
        &root.join("codex-state"),
        &root.join("processes"),
        &borg_runtime::protocol::InferenceSettings::selected(),
        &limits().execution,
        Arc::new(limits().worker.clone()),
        &limits(),
    )
    .await
    .unwrap();
    let initialized = session.initialize(&c).await.unwrap();
    assert!(initialized.is_object());
    use std::os::unix::fs::PermissionsExt;
    assert!(root.join("processes/home/code").is_dir());
    assert_eq!(
        std::fs::metadata(root.join("processes/home/code"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(root.join("codex-state"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    let command = session.preflight_command(&root, &c).await;
    // Qualification of the actual pinned config-to-thread mapping, without a
    // model turn. This is an observed response assertion, not a runtime guard.
    let thread = if command.is_ok() {
        session
            .request(
                "thread/start",
                serde_json::json!({
                    "model":"gpt-6-astra", "cwd":root,
                    "approvalPolicy":"never", "sandbox":"workspace-write", "ephemeral":true
                }),
                &c,
            )
            .await
    } else {
        Err("preflight failed; no thread created".into())
    };
    let cwd_write = if let Ok(thread) = &thread {
        session
            .request(
                "command/exec",
                serde_json::json!({
                    "command":["/bin/sh", "-c", "printf '%s\\n' workspace-cwd-write-read > qualification-cwd-proof && cat qualification-cwd-proof"],
                    "cwd":root, "timeoutMs":5000, "outputBytesCap":1024,
                    "sandboxPolicy":thread["sandbox"]
                }),
                &c,
            )
            .await
    } else {
        Err("thread unavailable; no cwd write attempted".into())
    };
    session.shutdown().await.unwrap();
    assert_eq!(process_records(&root.join("processes")), 0);
    command.unwrap();
    let thread = thread.unwrap();
    let cwd_write = cwd_write.unwrap();
    std::fs::write(
        root.join("resolved-sandbox.json"),
        serde_json::to_vec_pretty(
            &serde_json::json!({"cwd":thread["cwd"],"sandbox":thread["sandbox"],"cwd_write":cwd_write}),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(cwd_write["exitCode"], 0);
    assert_eq!(cwd_write["stdout"], "workspace-cwd-write-read\n");
    assert_eq!(
        std::fs::read_to_string(root.join("qualification-cwd-proof")).unwrap(),
        "workspace-cwd-write-read\n"
    );
    assert_eq!(thread["cwd"], serde_json::json!(root));
    assert_eq!(thread["model"], "gpt-6-astra");
    assert_eq!(thread["reasoningEffort"], "max");
    // Pinned alpha10 SandboxPolicy roots are additional to the implicit cwd;
    // its resolved thread policy removes a redundant explicit cwd entry.
    assert_eq!(
        thread["sandbox"],
        serde_json::json!({
            "type":"workspaceWrite", "writableRoots":[], "networkAccess":true,
            "excludeSlashTmp":false, "excludeTmpdirEnvVar":false
        })
    );
}

#[tokio::test]
async fn codex_oversized_or_interrupted_protocol_never_succeeds() {
    use std::os::unix::fs::PermissionsExt;
    let root = area("codex-bounds");
    let fake = root.join("fake-codex");
    std::fs::write(&fake,"#!/usr/bin/python3\nimport sys\nsys.stdout.write('x' * 1048577 + '\\n')\nsys.stdout.flush()\n").unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o700)).unwrap();
    let (c, _cancel) = control();
    let mut session = codex::Session::start(
        &fake,
        &root,
        &root.join("codex-state"),
        &root.join("processes"),
        &borg_runtime::protocol::InferenceSettings::selected(),
        &limits().execution,
        Arc::new(limits().worker.clone()),
        &limits(),
    )
    .await
    .unwrap();
    assert!(session.initialize(&c).await.is_err());
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn codex_early_completion_and_unknown_request_are_handled() {
    //= spec/impl/v2/implementation.md#the-supplied-executor-is-qualified-at-its-actual-boundary
    //# An unknown executor request MUST fail without silently granting authority.
    use std::os::unix::fs::PermissionsExt;
    let root = area("codex-events");
    let fake = root.join("fake-codex");
    std::fs::write(&fake,r#"#!/usr/bin/python3
import sys,json,pathlib
overrides=[sys.argv[i+1] for i,x in enumerate(sys.argv[:-1]) if x=='-c']
roots=[x.split('=',1)[1] for x in overrides if x.startswith('sandbox_workspace_write.writable_roots=')]
assert len(roots)==1 and json.loads(roots[0])==[str(pathlib.Path.cwd())]
assert 'model_providers.borg_openai.env_key="OPENAI_API_KEY"' in overrides
assert 'model_providers.borg_openai.requires_openai_auth=false' in overrides
assert not any('danger-full-access' in x or 'dangerFullAccess' in x for x in sys.argv)
def send(value):
 print(json.dumps(value),flush=True)
for line in sys.stdin:
 m=json.loads(line)
 if m.get('method')=='initialize': send({'id':m['id'],'result':{'userAgent':'fixture'}})
 elif m.get('method')=='command/exec':
  assert m['params']['sandboxPolicy']['type']=='workspaceWrite'
  send({'id':m['id'],'result':{'exitCode':0,'stdout':'','stderr':''}})
 elif m.get('method')=='thread/start':
  assert m['params']['sandbox']=='workspace-write'
  assert m['params']['model']=='gpt-6-astra' and m['params']['config']['model_reasoning_effort']=='max'
  send({'id':m['id'],'result':{'thread':{'id':'thread-fixture'},'model':'gpt-6-astra','reasoningEffort':'max'}})
 elif m.get('method')=='turn/start':
  assert m['params']['model']=='gpt-6-astra' and m['params']['effort']=='max'
  assert m['params']['sandboxPolicy']=={'type':'workspaceWrite','writableRoots':[str(pathlib.Path.cwd())],'networkAccess':True}
  send({'id':900,'method':'unrecognized/grant','params':{}})
  denied=json.loads(sys.stdin.readline())
  assert denied['id']==900 and denied['error']['code']==-32601
  open('unknown-request-declined','w').write('yes')
  for thread,turn,inputs,outputs in [('other','turn-fixture',900,900),('thread-fixture','other',900,900),('thread-fixture','turn-fixture',100,12),('thread-fixture','turn-fixture',100,12),('thread-fixture','turn-fixture',3,1),('thread-fixture','turn-fixture',-1,15)]:
   send({'method':'thread/tokenUsage/updated','params':{'threadId':thread,'turnId':turn,'tokenUsage':{'total':{'inputTokens':inputs,'outputTokens':outputs}}}})
  send({'method':'item/completed','params':{'item':{'type':'agentMessage','text':'fixture completed'}}})
  send({'method':'turn/completed','params':{'threadId':'thread-fixture','turn':{'id':'turn-fixture','status':'completed'}}})
  send({'id':m['id'],'result':{'turn':{'id':'turn-fixture','status':'inProgress'}}})
"#).unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o700)).unwrap();
    let (c, _cancel) = control();
    let (tx, _rx) = tokio::sync::mpsc::channel(16);
    let outcome = codex::execute(
        &fake,
        &root,
        &root.join("state"),
        &root.join("processes"),
        &borg_runtime::protocol::InferenceSettings::selected(),
        &limits().execution,
        "fixture instruction",
        &c,
        tx,
    )
    .await
    .unwrap();
    assert!(outcome.success);
    assert_eq!(outcome.summary, "fixture completed");
    let expected = serde_json::json!({"input_tokens":100,"output_tokens":12,"model_requests":null});
    assert_eq!(
        serde_json::to_value(&outcome.observed_usage).unwrap(),
        expected
    );
    let receipt: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("state/borg-usage.json")).unwrap())
            .unwrap();
    assert_eq!(receipt["thread_id"], "thread-fixture");
    assert_eq!(receipt["turn_id"], "turn-fixture");
    assert_eq!(receipt["observed_usage"], expected);
    let without_usage = root.join("fake-without-usage");
    std::fs::write(
        &without_usage,
        std::fs::read_to_string(&fake)
            .unwrap()
            .replace("thread/tokenUsage/updated", "unrecognized/progress"),
    )
    .unwrap();
    std::fs::set_permissions(&without_usage, std::fs::Permissions::from_mode(0o700)).unwrap();
    let (tx, _rx) = tokio::sync::mpsc::channel(16);
    let unobserved = codex::execute(
        &without_usage,
        &root,
        &root.join("unobserved"),
        &root.join("processes"),
        &borg_runtime::protocol::InferenceSettings::selected(),
        &limits().execution,
        "fixture",
        &c,
        tx,
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::to_value(unobserved.observed_usage).unwrap(),
        serde_json::json!({"input_tokens":null,"output_tokens":null,"model_requests":null})
    );
    assert!(!root.join("unobserved/borg-usage.json").exists());
    assert_eq!(
        std::fs::read_to_string(root.join("unknown-request-declined")).unwrap(),
        "yes"
    );
}

#[tokio::test]
async fn refused_command_preflight_never_starts_model_work() {
    use std::os::unix::fs::PermissionsExt;
    let root = area("codex-preflight");
    let fake = root.join("fake-codex");
    std::fs::write(&fake,r#"#!/usr/bin/python3
import sys,json
for line in sys.stdin:
 m=json.loads(line)
 method=m.get('method')
 if method=='initialize': print(json.dumps({'id':m['id'],'result':{}}),flush=True)
 elif method=='command/exec': print(json.dumps({'id':m['id'],'result':{'exitCode':1,'stdout':'','stderr':'fixture environment refused'}}),flush=True)
 elif method in ['thread/start','turn/start']: open('model-started','w').write(method)
"#).unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o700)).unwrap();
    let (c, _cancel) = control();
    let (tx, _rx) = tokio::sync::mpsc::channel(16);
    let error = codex::execute(
        &fake,
        &root,
        &root.join("state"),
        &root.join("processes"),
        &borg_runtime::protocol::InferenceSettings::selected(),
        &limits().execution,
        "fixture",
        &c,
        tx,
    )
    .await
    .unwrap_err();
    assert!(error.contains("preflight failed before model start"));
    assert!(!root.join("model-started").exists());
    assert_eq!(process_records(&root.join("processes")), 0);
}

async fn interrupt_during_controller_wait(reconnect: bool, clock_probes: bool) {
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use hyper::{
        Request as HttpRequest, Response as HttpResponse, body::Incoming, service::service_fn,
    };
    use hyper_util::rt::TokioIo;
    use std::{convert::Infallible, process::Stdio, sync::Arc};
    let root = area(match (reconnect, clock_probes) {
        (false, false) => "shutdown-register",
        (true, false) => "shutdown-reconnect",
        (false, true) => "clock-probe-register",
        (true, true) => "clock-probe-reconnect",
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let registrations = Arc::new(AtomicU64::new(0));
    let observed_calls = registrations.clone();
    let held_call_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let held_call_check = held_call_dropped.clone();
    struct HeldCall(Arc<std::sync::atomic::AtomicBool>);
    impl Drop for HeldCall {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let (entered, mut observed) = tokio::sync::mpsc::unbounded_channel();
    let server = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let count = registrations.clone();
            let entered = entered.clone();
            let held_call_dropped = held_call_dropped.clone();
            connections.spawn(async move {
                let service = service_fn(move |request: HttpRequest<Incoming>| {
                    let count = count.clone();
                    let entered = entered.clone();
                    let held_call_dropped = held_call_dropped.clone();
                    async move {
                        let request: Request = serde_json::from_slice(
                            &request.into_body().collect().await.unwrap().to_bytes(),
                        )
                        .unwrap();
                        match request.command {
                            Operation::GetOperation { .. } => {
                                Ok::<_, Infallible>(HttpResponse::new(Full::new(Bytes::from(
                                    serde_json::to_vec(&Reply::refused(
                                        request.request_id,
                                        "not_found",
                                        "no fixture receipt",
                                    ))
                                    .unwrap(),
                                ))))
                            }
                            Operation::RegisterWorker {
                                operation_id,
                                worker,
                            } => {
                                let number = count.fetch_add(1, Ordering::SeqCst);
                                if !reconnect || number > 0 {
                                    let _held = HeldCall(held_call_dropped);
                                    entered.send(()).unwrap();
                                    std::future::pending::<()>().await;
                                }
                                Ok::<_, Infallible>(HttpResponse::new(Full::new(Bytes::from(
                                    serde_json::to_vec(&Reply {
                                        request_id: request.request_id,
                                        outcome: Outcome::Ok {
                                            data: Data::Worker(worker),
                                        },
                                        commit: Some(MutationCommit {
                                            operation_id,
                                            controller_revision: 1,
                                        }),
                                        observed_controller_revision: Some(1),
                                    })
                                    .unwrap(),
                                ))))
                            }
                            Operation::AwaitCommands { .. } => {
                                // First wait loses transport; retry retains its saved
                                // mutation and is interrupted while awaiting the controller.
                                if count.fetch_add(1, Ordering::SeqCst) > 1 {
                                    let _held = HeldCall(held_call_dropped);
                                    entered.send(()).unwrap();
                                    std::future::pending::<()>().await;
                                }
                                Ok(HttpResponse::builder()
                                    .status(503)
                                    .body(Full::new(Bytes::new()))
                                    .unwrap())
                            }
                            _ => panic!("unexpected fixture operation"),
                        }
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    let selected_metadata = write_limits(&root, &limits());
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_borg-runtime"))
        .args([
            "worker",
            "--limits",
            selected_metadata.to_str().unwrap(),
            "--controller",
            &url,
            "--id",
            "shutdown-test",
            "--incarnation",
            "one",
            "--workspace-root",
            root.to_str().unwrap(),
            "--codex",
            "/usr/bin/false",
        ])
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", &root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), observed.recv())
        .await
        .unwrap()
        .unwrap();
    let pid = child.id().unwrap();
    if clock_probes {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
        let calls_before = observed_calls.load(Ordering::SeqCst);
        let stderr = child.stderr.take().unwrap();
        // Explicit finite fixture read, not a new production diagnostic bound.
        let mut lines = BufReader::new(stderr.take(65536));
        let mut last_elapsed = None;
        for sequence in 1..=2 {
            assert!(
                tokio::process::Command::new("/bin/kill")
                    .args(["-USR1", "--", &pid.to_string()])
                    .status()
                    .await
                    .unwrap()
                    .success()
            );
            let record = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let mut line = String::new();
                    assert!(
                        lines.read_line(&mut line).await.unwrap() > 0,
                        "clock diagnostic absent"
                    );
                    if let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) {
                        if value["event"] == "borg_native_clock_v1" {
                            break value;
                        }
                    }
                }
            })
            .await
            .expect("bounded probe acknowledgement");
            let keys: std::collections::BTreeSet<_> = record
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect();
            assert_eq!(
                keys,
                [
                    "event",
                    "pid",
                    "process_start_ticks",
                    "probe_sequence",
                    "elapsed_ns",
                    "enqueue_dropped_total",
                    "write_failed_total"
                ]
                .into_iter()
                .collect()
            );
            assert_eq!(record["pid"], pid);
            assert!(
                record["process_start_ticks"]
                    .as_u64()
                    .is_some_and(|n| n > 0)
            );
            assert_eq!(record["probe_sequence"], sequence);
            let elapsed = record["elapsed_ns"].as_u64().unwrap();
            assert!(last_elapsed.is_none_or(|previous| previous < elapsed));
            last_elapsed = Some(elapsed);
            assert_eq!(record["enqueue_dropped_total"], 0);
            assert_eq!(record["write_failed_total"], 0);
            assert!(
                child.try_wait().unwrap().is_none(),
                "probe terminated worker"
            );
            assert_eq!(
                observed_calls.load(Ordering::SeqCst),
                calls_before,
                "probe recreated controller await"
            );
            assert!(
                !held_call_check.load(Ordering::SeqCst),
                "original controller call was dropped"
            );
            assert!(
                observed.try_recv().is_err(),
                "probe duplicated original call"
            );
        }
    }
    assert!(
        tokio::process::Command::new("/bin/kill")
            .args(["-INT", "--", &pid.to_string()])
            .status()
            .await
            .unwrap()
            .success()
    );
    let status = tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .expect("CtrlC must interrupt registration/reconnection promptly")
        .unwrap();
    server.abort();
    let _ = server.await;
    assert!(
        status.success(),
        "worker did not gracefully shut down: {status}"
    );
    assert!(
        !root
            .join(".borg-worker")
            .join(process::component("shutdown-test"))
            .join("worker-owner.json")
            .exists()
    );
    std::fs::write(root.join("result.json"),serde_json::to_vec(&serde_json::json!({"pid":pid,"reconnecting":reconnect,"signal":"SIGINT","success":true,"forced_kill":false})).unwrap()).unwrap();
}

#[tokio::test]
async fn interrupt_while_registration_response_is_stalled() {
    interrupt_during_controller_wait(false, false).await;
}
#[tokio::test]
async fn interrupt_while_reconnecting_to_controller() {
    interrupt_during_controller_wait(true, false).await;
}

#[tokio::test]
async fn clock_probes_preserve_stalled_registration_then_interrupt_still_shuts_down() {
    interrupt_during_controller_wait(false, true).await;
}
#[tokio::test]
async fn clock_probes_preserve_reconnecting_controller_await_then_interrupt_still_shuts_down() {
    interrupt_during_controller_wait(true, true).await;
}

#[tokio::test]
async fn registered_validator_journals_before_input_and_cleans_normal_exit() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let root = area("registered-validator");
    let registry = root.join("registry");
    let mut owner = process::ManagedProcess::spawn_registered(
        Path::new(env!("CARGO_BIN_EXE_borg-runtime")),
        &registry,
        Arc::new(limits().worker.clone()),
    )
    .unwrap();
    assert_eq!(process_records(&registry), 1);
    let path = std::fs::read_dir(&registry)
        .unwrap()
        .flatten()
        .find(|e| e.path().extension().and_then(|s| s.to_str()) == Some("json"))
        .unwrap()
        .path();
    let record: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(record["pid"], record["process_group"]);
    assert!(record["context"]["boot_id"].as_str().unwrap().len() == 36);
    let schema = b"{\"type\":\"object\"}";
    let mut header = b"NV1\0".to_vec();
    header.extend([6, 0]);
    header.extend(1024u32.to_be_bytes());
    header.extend((process::now_ms() + 30_000).to_be_bytes());
    header.extend((schema.len() as u32).to_be_bytes());
    header.extend(0u32.to_be_bytes());
    header.push(0); // Exact task output bound source in revision 6.
    let mut input = owner.stdin.take().unwrap();
    input.write_all(&header).await.unwrap();
    // Same-binary helper revision 6 carries the exact explicit native policy
    // after its fixed framing header, before schema bytes.
    let native_policy = limits().execution.native.clone();
    let mut policy_frame = Vec::new();
    policy_frame.extend_from_slice(&(native_policy.context_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.input_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.output_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.result_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.schema_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.json_depth as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.json_nodes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.schema_expansion as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.validation_work as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.regex_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.regex_dfa_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.request_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.response_wire_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.helper_value_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.max_output_tokens as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.function_count as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.function_name_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.function_description_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.call_id_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.function_calls_per_response as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.memory_notice_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.endpoint_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.api_key_env_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.api_key_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.http_connect_timeout_ms as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.error_body_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.error_read_timeout_ms as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.decode_check_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.helper_error_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.task_tool_observation_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.summary_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.decision_envelope_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.continuation_envelope_bytes as u64).to_be_bytes());
    policy_frame.extend_from_slice(&(native_policy.task_tool_identity_bytes as u64).to_be_bytes());
    input.write_all(&policy_frame).await.unwrap();
    input.write_all(schema).await.unwrap();
    drop(input);
    let mut output = Vec::new();
    owner
        .child
        .stdout
        .take()
        .unwrap()
        .read_to_end(&mut output)
        .await
        .unwrap();
    assert!(output.starts_with(b"NVR\0"));
    let status = owner
        .finish_registered(false, tokio::time::Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(status.success());
    assert_eq!(process_records(&registry), 0);
    assert!(owner.try_finish_registered().unwrap().unwrap().success());
}

#[tokio::test]
async fn failed_registration_keeps_actual_child_owned_and_closes_input() {
    let root = area("registration-failure");
    let registry = root.join("registry");
    let pid = process::qualification_fail_next_registration(&registry);
    let failure = match process::ManagedProcess::spawn_registered(
        Path::new(env!("CARGO_BIN_EXE_borg-runtime")),
        &registry,
        Arc::new(limits().worker.clone()),
    ) {
        Ok(_) => panic!("registration failure not injected"),
        Err(failure) => failure,
    };
    let mut owner = failure.owner.expect("actual spawned child must stay owned");
    let pid = pid.await.unwrap();
    assert!(owner.stdin.is_none());
    assert!(failure.error.contains("registration failure"));
    owner
        .finish_registered(true, tokio::time::Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(!Path::new(&format!("/proc/{pid}")).exists());
    assert_eq!(process_records(&registry), 0);
}

#[tokio::test]
async fn recovery_refuses_wrong_boot_legacy_corrupt_and_oversized_records() {
    let root = area("registry-refusal");
    let registry = root.join("registry");
    let mut owner = process::ManagedProcess::spawn_registered(
        Path::new(env!("CARGO_BIN_EXE_borg-runtime")),
        &registry,
        Arc::new(limits().worker.clone()),
    )
    .unwrap();
    let path = std::fs::read_dir(&registry)
        .unwrap()
        .flatten()
        .find(|e| e.path().extension().and_then(|s| s.to_str()) == Some("json"))
        .unwrap()
        .path();
    let original = std::fs::read(&path).unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&original).unwrap();
    value["context"]["boot_id"] = serde_json::json!("00000000-0000-0000-0000-000000000000");
    let wrong = serde_json::to_vec(&value).unwrap();
    std::fs::write(&path, &wrong).unwrap();
    assert!(process::reconcile_processes(&registry, &limits().worker).await.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), wrong);
    assert!(owner.child.try_wait().unwrap().is_none());
    for bad in [
        b"{\"pid\":9,\"start_ticks\":1}".to_vec(),
        b"corrupt".to_vec(),
        vec![b' '; 256 * 1024 + 1],
    ] {
        std::fs::write(&path, &bad).unwrap();
        assert!(process::reconcile_processes(&registry, &limits().worker).await.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bad);
        assert!(owner.child.try_wait().unwrap().is_none());
    }
    std::fs::write(&path, original).unwrap();
    owner
        .finish_registered(true, tokio::time::Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn registry_count_bound_refuses_before_inspection_or_signaling() {
    let root = area("registry-count");
    std::fs::create_dir(root.join("home")).unwrap();
    std::fs::write(root.join("home/ignored.json"), b"not a record").unwrap();
    process::reconcile_processes(&root, &limits().worker).await.unwrap();
    for n in 0..=limits().worker.process_registry_records {
        std::fs::write(root.join(format!("{n}.json")), b"invalid").unwrap();
    }
    assert!(
        process::reconcile_processes(&root, &limits().worker)
            .await
            .unwrap_err()
            .contains("record count")
    );
    assert_eq!(process_records(&root), limits().worker.process_registry_records + 1);
}

#[test]
fn simultaneous_stale_worker_claimants_have_one_owner_and_retained_lock() {
    use std::sync::{Arc, Barrier};
    let root = area("lease-concurrent");
    let path = root.join("owner.json");
    // A cleanly dropped owner leaves the stable lock inode in place.
    let first = process::WorkerLease::claim(&path, &limits().worker).unwrap();
    let mut stale: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let retained = first.owner_lock();
    drop(first);
    assert!(process::WorkerLease::claim(&path, &limits().worker).is_err());
    drop(retained);
    // Synthetic dead PID fixture exercises the stale-record reclamation path;
    // no process is signaled by a lease claim.
    stale["pid"] = serde_json::json!(u32::MAX - 1);
    std::fs::write(&path, serde_json::to_vec(&stale).unwrap()).unwrap();
    let barrier = Arc::new(Barrier::new(3));
    let release = Arc::new(Barrier::new(3));
    let wins = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    std::thread::scope(|scope| {
        for _ in 0..2 {
            let barrier = barrier.clone();
            let release = release.clone();
            let wins = wins.clone();
            let path = path.clone();
            scope.spawn(move || {
                barrier.wait();
                let owner = process::WorkerLease::claim(&path, &limits().worker);
                if owner.is_ok() {
                    wins.fetch_add(1, Ordering::SeqCst);
                }
                release.wait();
                drop(owner);
            });
        }
        barrier.wait();
        release.wait();
    });
    assert_eq!(wins.load(Ordering::SeqCst), 1);
    assert!(path.with_extension("lock").exists());
    let _replacement = process::WorkerLease::claim(&path, &limits().worker).unwrap_or_else(|error| {
        panic!("lease reacquire after concurrent claimants failed: {error}")
    });
}

// Real application processes and a synthetic local HTTP peer. No provider key
// or request leaves the fixture; arbitrary native execution is not simulated.
mod task_recovery {
    use super::*;
    use borg_runtime::client::ApiClient;
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use hyper::{Request as HttpRequest, Response, body::Incoming, service::service_fn};
    use hyper_util::rt::TokioIo;
    use serde_json::{Value, json};
    use std::{
        io,
        process::{Child, Command, Stdio},
        sync::{Arc, Mutex, atomic::AtomicBool},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    struct Process {
        child: Child,
        closed_evidence: PathBuf,
    }
    impl Process {
        fn start(image: &Path, args: &[String], root: &Path, label: &str) -> Self {
            let log = std::fs::File::create(root.join(format!("{label}.log"))).unwrap();
            let child = Command::new(image)
                .args(args)
                .env_clear()
                .env("PATH", "/usr/bin:/bin")
                .env("HOME", root)
                .env("OPENAI_API_KEY", "synthetic-worker-recovery-key")
                .stdin(Stdio::null())
                .stdout(log.try_clone().unwrap())
                .stderr(log)
                .spawn()
                .unwrap();
            Self {
                child,
                closed_evidence: root.join(format!("{label}-closed.json")),
            }
        }
        async fn stop(&mut self) {
            if self.child.try_wait().unwrap().is_none() {
                assert!(
                    Command::new("/usr/bin/kill")
                        .args(["-INT", &self.child.id().to_string()])
                        .status()
                        .unwrap()
                        .success()
                );
            }
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    assert!(
                        status.success(),
                        "application did not shut down successfully: {status}"
                    );
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "application shutdown timed out"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    impl Drop for Process {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let waited = self.child.wait();
            let evidence = serde_json::to_vec(&json!({"pid":self.child.id(),"reaped":waited.is_ok(),"exit_code":waited.as_ref().ok().and_then(|status|status.code())})).unwrap();
            let _ = std::fs::write(&self.closed_evidence, evidence);
        }
    }
    struct Background(tokio::task::JoinHandle<()>);
    impl Drop for Background {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    struct Peer {
        endpoint: String,
        posts: Arc<AtomicU64>,
        _owner: Background,
    }
    impl Peer {
        async fn start(mode: usize) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
            let posts = Arc::new(AtomicU64::new(0));
            let count = posts.clone();
            let owner = tokio::spawn(async move {
                let mut connections = tokio::task::JoinSet::new();
                loop {
                    tokio::select! {
                        accepted=listener.accept()=>{
                            let (mut stream,_)=accepted.unwrap();let count=count.clone();
                            connections.spawn(async move {
                                let mut header=Vec::new();
                                while !header.ends_with(b"\r\n\r\n") {
                                    header.push(stream.read_u8().await.unwrap());
                                    assert!(header.len()<16*1024,"fixture HTTP header bound");
                                }
                                let text=String::from_utf8(header).unwrap();
                                let length=text.lines().find_map(|line| {
                                    let (name,value)=line.split_once(':')?;
                                    name.eq_ignore_ascii_case("content-length").then(||value.trim().parse::<usize>().unwrap())
                                }).unwrap();
                                assert!(length<=limits().transport.body_bytes);
                                let mut body=vec![0;length];stream.read_exact(&mut body).await.unwrap();
                                let request:Value=serde_json::from_slice(&body).unwrap();
                                assert!(request.get("input").is_some());
                                count.fetch_add(1,Ordering::SeqCst);
                                if mode==1 {
                                    stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1000\r\nConnection: close\r\n\r\n{").await.unwrap();
                                } else if mode==2 {
                                    let body=serde_json::to_vec(&json!({"status":"completed","usage":{"input_tokens":5,"output_tokens":1,"total_tokens":6},"output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"7"}]}]})).unwrap();
                                    stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
                                    stream.write_all(&body).await.unwrap();
                                }
                                stream.shutdown().await.unwrap();
                            });
                        }
                        _=connections.join_next(),if !connections.is_empty()=>{}
                    }
                }
            });
            Self {
                endpoint,
                posts,
                _owner: Background(owner),
            }
        }
    }

    struct Proxy {
        url: String,
        block_uncertainty: Arc<AtomicBool>,
        uncertainty: Arc<Mutex<Vec<Report>>>,
        stops: Arc<AtomicU64>,
        refusals: Arc<Mutex<Vec<(Report, Outcome)>>>,
        reconciliations: Arc<Mutex<Vec<ReconcileResultInput>>>,
        registrations: Arc<Mutex<Vec<Worker>>>,
        dropped_reconcile_ack: Arc<AtomicBool>,
        _owner: Background,
    }
    impl Proxy {
        fn registered(&self, incarnation: &str) -> bool {
            self.registrations
                .lock()
                .unwrap()
                .last()
                .is_some_and(|worker| worker.incarnation == incarnation)
        }
        async fn start(upstream: String, hide_validator: Option<PathBuf>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let block_uncertainty = Arc::new(AtomicBool::new(false));
            let uncertainty = Arc::new(Mutex::new(vec![]));
            let stops = Arc::new(AtomicU64::new(0));
            let refusals = Arc::new(Mutex::new(vec![]));
            let reconciliations = Arc::new(Mutex::new(vec![]));
            let registrations = Arc::new(Mutex::new(vec![]));
            let registered = registrations.clone();
            let dropped_reconcile_ack = Arc::new(AtomicBool::new(false));
            let dropped = dropped_reconcile_ack.clone();
            let (block, unknown, stopped, refused, reconciled) = (
                block_uncertainty.clone(),
                uncertainty.clone(),
                stops.clone(),
                refusals.clone(),
                reconciliations.clone(),
            );
            let hidden = Arc::new(AtomicBool::new(false));
            let owner = tokio::spawn(async move {
                let client = reqwest::Client::builder().no_proxy().build().unwrap();
                let mut connections = tokio::task::JoinSet::new();
                loop {
                    tokio::select! {
                        accepted=listener.accept()=>{
                            let (stream,_)=accepted.unwrap();
                            let (upstream,client,block,unknown,stopped,refused,reconciled,registered,dropped,hidden,hide_validator)=(upstream.clone(),client.clone(),block.clone(),unknown.clone(),stopped.clone(),refused.clone(),reconciled.clone(),registered.clone(),dropped.clone(),hidden.clone(),hide_validator.clone());
                            connections.spawn(async move {
                                let service=service_fn(move |request:HttpRequest<Incoming>| {
                                    let (upstream,client,block,unknown,stopped,refused,reconciled,registered,dropped,hidden,hide_validator)=(upstream.clone(),client.clone(),block.clone(),unknown.clone(),stopped.clone(),refused.clone(),reconciled.clone(),registered.clone(),dropped.clone(),hidden.clone(),hide_validator.clone());
                                    async move {
                                        let route=request.uri().path().to_owned();
                                        assert!(matches!(route.as_str(),"/api/operations"|"/mcp"));
                                        // The selected collaboration profile initializes and reads
                                        // its real MCP catalogue before any provider request. Keep
                                        // that JSON-RPC body and host caller/version headers exact;
                                        // only private operations enter the interception below.
                                        let mut headers=reqwest::header::HeaderMap::new();
                                        for name in ["content-type","accept","mcp-protocol-version","x-borg-caller"] {
                                            if let Some(value)=request.headers().get(name) { headers.insert(name,value.clone()); }
                                        }
                                        let bytes=request.into_body().collect().await.unwrap().to_bytes();
                                        assert!(bytes.len()<=limits().transport.body_bytes);
                                        if route=="/mcp" {
                                            let response=client.post(format!("{upstream}/mcp")).headers(headers).body(bytes.to_vec()).send().await.map_err(io::Error::other)?;
                                            let status=response.status();
                                            let content_type=response.headers().get("content-type").cloned();
                                            let body=response.bytes().await.map_err(io::Error::other)?;
                                            assert!(body.len()<=limits().transport.body_bytes);
                                            let mut reply=Response::builder().status(status);
                                            if let Some(value)=content_type { reply=reply.header("content-type",value); }
                                            return Ok::<_,io::Error>(reply.body(Full::new(body)).unwrap());
                                        }
                                        let request:Request=serde_json::from_slice(&bytes).unwrap();
                                        if let Operation::ObserveWorker { report }=&request.command {
                                            if matches!(report.observation,Observation::ExecutionUncertain { .. }) {
                                                unknown.lock().unwrap().push(report.clone());
                                                if block.load(Ordering::SeqCst) { return Ok::<_,io::Error>(Response::builder().status(503).body(Full::new(Bytes::new())).unwrap()); }
                                            }
                                            if matches!(report.observation,Observation::ProcessTreeStopped) { stopped.fetch_add(1,Ordering::SeqCst); }
                                            if matches!(report.observation,Observation::ResultCandidate { checks_passed:true,.. }) {
                                                if let Some(path)=hide_validator.as_ref() {
                                                    if !hidden.swap(true,Ordering::SeqCst) { std::fs::rename(path,path.with_extension("held")).unwrap(); }
                                                }
                                            }
                                        }
                                        if let Operation::ReconcileResult(input)=&request.command { reconciled.lock().unwrap().push(input.clone()); }
                                        let response=client.post(format!("{upstream}/api/operations")).header("content-type","application/json").body(bytes.to_vec()).send().await.map_err(io::Error::other)?;
                                        let response=response.bytes().await.map_err(io::Error::other)?;
                                        let reply:Reply=serde_json::from_slice(&response).unwrap();
                                        if matches!(request.command,Operation::RegisterWorker { .. }) {
                                            if let Outcome::Ok { data:Data::Worker(worker) }=&reply.outcome { registered.lock().unwrap().push(worker.clone()); }
                                        }
                                        if let Operation::ObserveWorker { report }=&request.command {
                                            if matches!(report.observation,Observation::ResultCandidate { .. }) && matches!(reply.outcome,Outcome::Refused { .. }) {
                                                refused.lock().unwrap().push((report.clone(),reply.outcome.clone()));
                                            }
                                        }
                                        if hide_validator.is_some() && matches!(request.command,Operation::ReconcileResult(_))
                                            && matches!(reply.outcome,Outcome::Ok { .. }) && !dropped.swap(true,Ordering::SeqCst) {
                                            return Ok(Response::builder().status(503).body(Full::new(Bytes::new())).unwrap());
                                        }
                                        Ok(Response::new(Full::new(response)))
                                    }
                                });
                                let _=hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream),service).await;
                            });
                        }
                        _=connections.join_next(),if !connections.is_empty()=>{}
                    }
                }
            });
            Self {
                url,
                block_uncertainty,
                uncertainty,
                stops,
                refusals,
                reconciliations,
                registrations,
                dropped_reconcile_ack,
                _owner: Background(owner),
            }
        }
    }

    fn recovery_limits() -> Arc<borg_runtime::limits::RuntimeLimits> {
        let mut policy = (*limits()).clone();
        policy.worker.coding_capacity = 0;
        policy.worker.native_capacity = 1;
        policy.validate().unwrap();
        Arc::new(policy)
    }
    struct Lab {
        root: PathBuf,
        controller_image: PathBuf,
        worker_image: PathBuf,
        address: String,
        origin: String,
    }
    impl Lab {
        fn new(label: &str) -> Self {
            let root = area(label);
            write_limits(&root, &recovery_limits());
            let controller_image = root.join("controller-runtime");
            let worker_image = root.join("worker-runtime");
            for image in [&controller_image, &worker_image] {
                std::fs::copy(env!("CARGO_BIN_EXE_borg-runtime"), image).unwrap();
            }
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap().to_string();
            drop(listener);
            Self {
                root,
                controller_image,
                worker_image,
                origin: format!("http://{address}"),
                address,
            }
        }
        async fn controller(&self, label: &str) -> Process {
            let process = Process::start(
                &self.controller_image,
                &[
                    "controller".into(),
                    "--limits".into(),
                    self.root.join("limits.json").display().to_string(),
                    "--db".into(),
                    self.root.join("tasks.sqlite").display().to_string(),
                    "--listen".into(),
                    self.address.clone(),
                    "--origin".into(),
                    self.origin.clone(),
                ],
                &self.root,
                label,
            );
            let api = ApiClient::new(&self.origin, recovery_limits()).unwrap();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            loop {
                if api
                    .call(Operation::ListTasks {
                        after: None,
                        limit: 1,
                    })
                    .await
                    .is_ok()
                {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "controller did not start"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            process
        }
        fn worker(&self, origin: &str, endpoint: &str, incarnation: &str) -> Process {
            Process::start(
                &self.worker_image,
                &[
                    "worker".into(),
                    "--limits".into(),
                    self.root.join("limits.json").display().to_string(),
                    "--controller".into(),
                    origin.into(),
                    "--id".into(),
                    "recovery-worker".into(),
                    "--incarnation".into(),
                    incarnation.into(),
                    "--workspace-root".into(),
                    self.root.join("worker").display().to_string(),
                    "--codex".into(),
                    "/usr/bin/false".into(),
                    "--coding-capacity".into(),
                    "0".into(),
                    "--native-capacity".into(),
                    "1".into(),
                    "--native-endpoint".into(),
                    endpoint.into(),
                ],
                &self.root,
                incarnation,
            )
        }
        fn state(&self) -> PathBuf {
            self.root
                .join("worker/.borg-worker")
                .join(process::component("recovery-worker"))
        }
        fn usage(&self, attempt: &str) -> (u32, Option<i64>, Option<i64>) {
            let db = rusqlite::Connection::open_with_flags(
                self.root.join("tasks.sqlite"),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .unwrap();
            db.query_row(
                "SELECT model_requests,input_tokens,output_tokens FROM attempts WHERE id=?1",
                [attempt],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap()
        }
    }
    async fn until(mut ready: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while !ready() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "fixture state did not converge"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    async fn get(api: &ApiClient, id: &str) -> Task {
        match api
            .call(Operation::GetTask { task_id: id.into() })
            .await
            .unwrap()
            .outcome
        {
            Outcome::Ok {
                data: Data::Task(task),
            } => *task,
            other => panic!("task lookup failed: {other:?}"),
        }
    }
    async fn wait_task(api: &ApiClient, id: &str, ready: impl Fn(&Task) -> bool) -> Task {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let task = get(api, id).await;
            if ready(&task) {
                return task;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "task did not converge: {:?}",
                task.status
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    async fn submit(api: &ApiClient) -> Task {
        let input = TaskInput {
            resolved_limits: None,
            instruction: "Return integer seven".into(),
            definition: Definition {
                limit_overrides: None,
                inference: Some(borg_runtime::protocol::InferenceSettings::selected()),
                execution_profile: Some(borg_runtime::protocol::ExecutionProfile::CollaborationV1 { web_search: None }),
                id: "native-recovery".into(),
                revision: "one".into(),
                executor: ExecutorKind::Native,
            },
            repository: None,
            repository_read: None,
            acceptance_commands: vec![],
            output_schema: Some(json!({"type":"integer"})),
            limits: Limits {
                deadline_unix_ms: process::now_ms() + 120_000,
                model_requests: 2,
                output_bytes: 4096,
                result_bytes: 8192,
                max_delegation_depth: 0,
            },
            dependencies: vec![],
            native_workflow: None,
        };
        match api
            .call(Operation::SubmitTask {
                operation_id: "native-recovery-submit".into(),
                input,
            })
            .await
            .unwrap()
            .outcome
        {
            Outcome::Ok {
                data: Data::Task(task),
            } => *task,
            other => panic!("submit failed: {other:?}"),
        }
    }

    async fn disconnected(mode: usize, block_uncertainty: bool) {
        let lab = Lab::new(if block_uncertainty {
            "native-unknown-pending"
        } else {
            "native-unknown"
        });
        let mut controller = lab.controller("controller-one").await;
        let peer = Peer::start(mode).await;
        let proxy = Proxy::start(lab.origin.clone(), None).await;
        proxy
            .block_uncertainty
            .store(block_uncertainty, Ordering::SeqCst);
        let mut worker = lab.worker(&proxy.url, &peer.endpoint, "inc-one");
        let api = ApiClient::new(&lab.origin, recovery_limits()).unwrap();
        let task = submit(&api).await;
        until(|| !proxy.uncertainty.lock().unwrap().is_empty()).await;
        if block_uncertainty {
            until(|| {
                std::fs::read_dir(lab.state().join("commands"))
                    .unwrap()
                    .filter_map(Result::ok)
                    .any(|entry| {
                        serde_json::from_slice::<Value>(&std::fs::read(entry.path()).unwrap())
                            .unwrap()["completed"]
                            == true
                    })
            })
            .await;
            assert_eq!(
                proxy.stops.load(Ordering::SeqCst),
                0,
                "stop overtook an unrecorded uncertainty report"
            );
            assert!(!get(&api, &task.id).await.attempt.unwrap().stopped);
        } else {
            wait_task(&api, &task.id, |task| {
                task.status == TaskStatus::Failed
                    && task
                        .attempt
                        .as_ref()
                        .is_some_and(|a| a.stopped && a.uncertain)
            })
            .await;
        }
        let first = get(&api, &task.id).await;
        assert!(matches!(
            api.call(Operation::RetryTask {
                operation_id: "retry-before".into(),
                task_id: task.id.clone(),
                expected_revision: first.revision
            })
            .await
            .unwrap()
            .outcome,
            Outcome::Refused { .. }
        ));
        assert_eq!(peer.posts.load(Ordering::SeqCst), 1);
        worker.stop().await;
        controller.stop().await;
        let mut controller = lab.controller("controller-two").await;
        let seen = proxy.uncertainty.lock().unwrap().len();
        let mut worker = lab.worker(&proxy.url, &peer.endpoint, "inc-two");
        if block_uncertainty {
            until(|| proxy.uncertainty.lock().unwrap().len() > seen).await;
            assert!(
                proxy.registered("inc-one"),
                "replacement registered with unresolved uncertainty delivery"
            );
            assert_eq!(proxy.stops.load(Ordering::SeqCst), 0);
            proxy.block_uncertainty.store(false, Ordering::SeqCst);
        }
        until(|| proxy.registered("inc-two")).await;
        let settled = wait_task(&api, &task.id, |task| {
            task.status == TaskStatus::Failed
                && task
                    .attempt
                    .as_ref()
                    .is_some_and(|a| a.stopped && a.uncertain)
        })
        .await;
        assert_eq!(
            settled.attempt.as_ref().unwrap().id,
            first.attempt.as_ref().unwrap().id
        );
        assert!(
            matches!(api.call(Operation::RetryTask { operation_id:"retry-after".into(),task_id:task.id.clone(),expected_revision:settled.revision }).await.unwrap().outcome,Outcome::Refused { code,.. } if code=="reconciliation_required")
        );
        assert_eq!(
            peer.posts.load(Ordering::SeqCst),
            1,
            "unknown POST was repeated"
        );
        if block_uncertainty {
            let reports = proxy.uncertainty.lock().unwrap();
            assert!(
                reports.iter().all(|report| report == &reports[0]),
                "uncertainty replay changed identity/body"
            );
        }
        worker.stop().await;
        controller.stop().await;
        assert_eq!(
            lab.usage(&settled.attempt.as_ref().unwrap().id),
            (1, None, None)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    // Traceability scope: Actual local POST disconnect/truncated response, budget2, controller/worker restart; one POST and retry refused.
    //= spec/impl/v2/implementation.md#interrupted-work-is-reconciled
    //= type=test
    //# An external effect with an uncertain outcome MUST be reconciled before the platform retries that effect.
    async fn task_recovery_native_disconnect_budget_two_never_repeats_post_after_restart() {
        disconnected(0, false).await;
        disconnected(1, false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn task_recovery_pending_uncertainty_blocks_stop_and_replacement_registration() {
        disconnected(0, true).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn task_recovery_completed_result_revalidates_original_after_worker_controller_restart() {
        let lab = Lab::new("result-acceptance-recovery");
        let mut controller = lab.controller("controller-one").await;
        let peer = Peer::start(2).await;
        let proxy = Proxy::start(lab.origin.clone(), Some(lab.controller_image.clone())).await;
        let mut worker = lab.worker(&proxy.url, &peer.endpoint, "inc-one");
        let api = ApiClient::new(&lab.origin, recovery_limits()).unwrap();
        let task = submit(&api).await;
        until(|| {
            !proxy.refusals.lock().unwrap().is_empty()
                && !proxy.reconciliations.lock().unwrap().is_empty()
        })
        .await;
        let (original, refusal) = proxy.refusals.lock().unwrap()[0].clone();
        assert!(
            matches!(refusal,Outcome::Refused { ref code,.. } if code=="validation_unavailable")
        );
        let pending = get(&api, &task.id).await;
        assert_eq!(pending.status, TaskStatus::Active);
        assert!(pending.attempt.as_ref().unwrap().stopped);
        assert!(!pending.attempt.as_ref().unwrap().uncertain);
        let reconciliation = proxy.reconciliations.lock().unwrap()[0].clone();
        assert_eq!(reconciliation.original_report_id, original.report_id);
        let original_path = lab.state().join("outbox").join(format!(
            "{}.refused",
            process::component(&original.report_id)
        ));
        let original_bytes = std::fs::read(&original_path).unwrap();
        assert_eq!(peer.posts.load(Ordering::SeqCst), 1);
        worker.stop().await;
        controller.stop().await;
        std::fs::rename(
            lab.controller_image.with_extension("held"),
            &lab.controller_image,
        )
        .unwrap();
        let mut controller = lab.controller("controller-two").await;
        let mut worker = lab.worker(&proxy.url, &peer.endpoint, "inc-two");
        until(|| proxy.registered("inc-two")).await;
        let accepted = wait_task(&api, &task.id, |task| task.status == TaskStatus::Succeeded).await;
        assert_eq!(accepted.result.as_ref().unwrap().output, Some(json!(7)));
        assert!(
            proxy.dropped_reconcile_ack.load(Ordering::SeqCst),
            "actual accepted reconciliation ACK was not dropped"
        );
        assert_eq!(accepted.attempt.as_ref().unwrap().id, original.attempt_id);
        assert_eq!(
            peer.posts.load(Ordering::SeqCst),
            1,
            "acceptance reconciliation repeated the model"
        );
        assert!(
            proxy
                .reconciliations
                .lock()
                .unwrap()
                .iter()
                .all(
                    |request| request.operation_id == reconciliation.operation_id
                        && request.original_report_id == original.report_id
                )
        );
        assert_eq!(std::fs::read(original_path).unwrap(), original_bytes);
        let lookup = api
            .call(Operation::GetOperation {
                operation_id: original.report_id,
            })
            .await
            .unwrap();
        assert!(
            matches!(lookup.outcome,Outcome::Ok { data:Data::Operation(ref operation) } if operation.outcome==Some(serde_json::to_value(&refusal).unwrap()))
        );
        worker.stop().await;
        controller.stop().await;
        assert_eq!(lab.usage(&original.attempt_id), (1, Some(5), Some(1)));
    }
}

#[tokio::test]
async fn credential_repository_identities_refuse_before_git_or_workspace_effects() {
    let root = area("credential-git-boundary");
    let (control, _cancel) = control();
    let clean = Repository {
        registration: Some(borg_runtime::repository_catalogue::Revision {
            repository_id: "fixture-repository".into(),
            registration_revision: "fixture-revision".into(),
        }),
        url: "/unused/credential-free.git".into(),
        start_commit: "a".repeat(40),
        checkpoint_remote: "/unused/credential-free.git".into(),
    };
    for (index, credential_url) in [
        "https://user:fixture-password@example.invalid/repo",
        "https::https://fixture-token@example.invalid/repo",
        "ssh://git:fixture-password@example.invalid/repo",
    ]
    .into_iter()
    .enumerate()
    {
        for field in 0..3 {
            let mut repository = clean.clone();
            let mut checkpoint = git::checkpoint(&clean, "task", "attempt", "a".repeat(40)).unwrap();
            match field {
                0 => repository.url = credential_url.into(),
                1 => repository.checkpoint_remote = credential_url.into(),
                _ => checkpoint.repository = credential_url.into(),
            }
            let workspace = root.join(format!("workspaces-{index}-{field}"));
            let registry = root.join(format!("processes-{index}-{field}"));
            let error = git::checkout(
                &workspace,
                "attempt",
                &repository,
                (field == 2).then_some(&checkpoint),
                &registry,
                &control,
            )
            .await
            .unwrap_err();
            assert_eq!(
                error,
                "repository identity contains credentials; use configured Git authentication"
            );
            assert!(!workspace.exists() && !registry.exists());
        }
        let mut checkpoint = git::checkpoint(&clean, "task", "attempt", "a".repeat(40)).unwrap();
        checkpoint.repository = credential_url.into();
        let workspace = root.join(format!("uncreated-workspace-{index}"));
        let registry = root.join(format!("uncreated-processes-{index}"));
        for error in [
            git::remote_commit(&workspace, &checkpoint, &registry, &control)
                .await
                .unwrap_err(),
            git::publish(&workspace, &checkpoint, &registry, &control)
                .await
                .unwrap_err(),
        ] {
            assert_eq!(
                error,
                "repository identity contains credentials; use configured Git authentication"
            );
        }
        assert!(!workspace.exists() && !registry.exists());
    }
}

#[path = "worker/resource_profile.rs"]
mod resource_profile;
