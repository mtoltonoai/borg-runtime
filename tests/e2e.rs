//! Explicitly invoked real-executor acceptance. Ordinary `cargo test` does not
//! spend provider capacity or publish to a real repository.
use borg_runtime::{client::ApiClient, protocol::*};
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(Some(_))) {
            return;
        }
        // Give the worker its normal cancellation/reaping path before the
        // controller closes. An interrupt acknowledgment is not stop proof.
        let _ = Command::new("/bin/kill")
            .args(["-INT", "--", &self.0.id().to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            if matches!(self.0.try_wait(), Ok(Some(_))) {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn git(cwd: &Path, args: &[&str]) -> String {
    let output = Command::new("/usr/bin/git")
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("git fixture command");
    assert!(
        output.status.success(),
        "git fixture failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().into()
}
fn task(reply: Reply) -> Task {
    match reply.outcome {
        Outcome::Ok {
            data: Data::Task(t),
        } => *t,
        other => panic!("unexpected task reply {other:?}"),
    }
}
async fn wait_controller(client: &ApiClient) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if client
            .call(Operation::ListTasks {
                after: None,
                limit: 1,
            })
            .await
            .is_ok()
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "controller startup failed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
#[ignore = "requires resolved profile, actual pinned Codex and explicit BORG_CODEX_E2E=1"]
async fn real_coding_checkpoint_and_controller_restart() {
    assert_eq!(std::env::var("BORG_CODEX_E2E").as_deref(), Ok("1"));
    let root =
        PathBuf::from(std::env::var("BORG_E2E_DIR").expect("explicit evidence/work directory"));
    assert!(!root.exists(), "use a fresh evidence directory");
    std::fs::create_dir_all(&root).unwrap();
    let repo = root.join("source");
    let remote = root.join("checkpoints.git");
    std::fs::create_dir_all(repo.join("src")).unwrap();
    std::fs::write(repo.join("src/main.rs"),"fn answer()-> &'static str { \"before\" }\nfn main(){ println!(\"{}\", answer()); }\n#[test] fn required_answer(){assert_eq!(answer(), \"after\");}\n").unwrap();
    std::fs::write(repo.join(".gitignore"), "/acceptance\n/demo\n").unwrap();
    git(&repo, &["init", "-b", "main"]);
    git(&repo, &["add", "."]);
    git(
        &repo,
        &[
            "-c",
            "user.name=Borg qualification",
            "-c",
            "user.email=borg-test@example.invalid",
            "commit",
            "-m",
            "synthetic starting point",
        ],
    );
    git(&root, &["init", "--bare", remote.to_str().unwrap()]);
    let start = git(&repo, &["rev-parse", "HEAD"]);
    git(
        &repo,
        &["push", remote.to_str().unwrap(), "HEAD:refs/heads/main"],
    );
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);
    let origin = format!("http://{address}");
    let db = root.join("tasks.sqlite");
    let mut limits = borg_runtime::limits::RuntimeLimits::seed().unwrap();
    limits.worker.coding_capacity = 1;
    limits.worker.native_capacity = 0;
    limits.validate().unwrap();
    let limits = std::sync::Arc::new(limits);
    let limits_path = root.join("limits.json");
    std::fs::write(
        &limits_path,
        serde_json::to_vec_pretty(limits.as_ref()).unwrap(),
    )
    .unwrap();
    // Pin subprocesses to the same copied image across restart even while
    // independent component tests rebuild Cargo's ordinary target directory.
    let runtime = std::env::var_os("BORG_E2E_BINARY")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_borg-runtime")));
    let spawn_controller = || {
        Process(
            Command::new(&runtime)
                .args([
                    "controller",
                    "--limits",
                    limits_path.to_str().unwrap(),
                    "--db",
                    db.to_str().unwrap(),
                    "--listen",
                    &address.to_string(),
                    "--origin",
                    &origin,
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        )
    };
    let controller = spawn_controller();
    let client = ApiClient::new(&origin, limits.clone()).unwrap();
    wait_controller(&client).await;
    let rustc = std::env::var("BORG_RUSTC").unwrap_or_else(|_| {
        "/home/dev-user/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin/rustc".into()
    });
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let registration = borg_runtime::repository_catalogue::Registration {
        pull_request_target: None,
        repository_id: "e2e-repository".into(),
        registration_revision: "1".into(),
        display_name: "Explicit E2E fixture".into(),
        source_endpoint: remote.to_string_lossy().into_owned(),
        checkpoint_endpoint: remote.to_string_lossy().into_owned(),
        source_store_key: "remote.git".into(),
        checkpoint_store_key: "remote.git".into(),
    };
    assert!(matches!(
        client
            .call(Operation::RegisterRepository {
                operation_id: "e2e-register-repository".into(),
                registration: registration.clone()
            })
            .await
            .unwrap()
            .outcome,
        Outcome::Ok {
            data: Data::RepositoryRegistration(_)
        }
    ));
    let input=TaskInput{ instruction_source: None, pull_request: None, resolved_limits:None,repository_read:None,instruction:"Change answer() in src/main.rs to return the literal after. Preserve the test. Make only that source correction.".into(),definition:Definition{charter: None, limit_overrides:None, inference: Some(InferenceSettings::selected()), execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),id:"agent-e2e".into(),revision:"fixture-1".into(),executor:ExecutorKind::Coding},repository:Some(Repository{ pull_request_target: None, registration:Some(registration.binding()),url:String::new(),start_commit:start.clone(),checkpoint_remote:String::new()}),acceptance_commands:vec![vec![rustc.clone(),"--test".into(),"src/main.rs".into(),"-o".into(),"acceptance".into()],vec!["./acceptance".into()],vec![rustc,"src/main.rs".into(),"-o".into(),"demo".into()]],output_schema:None,limits:Limits{deadline_unix_ms:now+300_000,model_requests:4,output_bytes:65536,result_bytes:limits.task_defaults.result_bytes,max_delegation_depth:4},dependencies:vec![],native_workflow:None};
    let submitted = task(
        client
            .call(Operation::SubmitTask {
                operation_id: "e2e-submit-1".into(),
                input,
            })
            .await
            .unwrap(),
    );
    let workspace = root.join("worker");
    let codex = std::env::var("BORG_CODEX").expect("pinned Codex executable path");
    let worker = Process(
        Command::new(&runtime)
            .args([
                "worker",
                "--limits",
                limits_path.to_str().unwrap(),
                "--controller",
                &origin,
                "--id",
                "worker-e2e",
                "--incarnation",
                "incarnation-1",
                "--workspace-root",
                workspace.to_str().unwrap(),
                "--codex",
                &codex,
                "--coding-capacity",
                "1",
                "--native-capacity",
                "0",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
    let accepted = loop {
        let state = task(
            client
                .call(Operation::GetTask {
                    task_id: submitted.id.clone(),
                })
                .await
                .unwrap(),
        );
        if matches!(state.status, TaskStatus::Failed | TaskStatus::Cancelled) {
            panic!("task did not pass: {state:?}");
        }
        if state.status == TaskStatus::Succeeded
            && state.attempt.as_ref().is_some_and(|a| a.stopped)
        {
            break state;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "coding task timed out: {state:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(accepted.assigned_agent_id, accepted.input.definition.id);
    assert_eq!(accepted.session_id, submitted.session_id);
    assert_eq!(
        accepted.attempt.as_ref().unwrap().session_id,
        submitted.session_id
    );
    let checkpoint = accepted.selected_checkpoint.as_ref().unwrap();
    assert_ne!(checkpoint.commit, start);
    assert_eq!(
        git(
            &root,
            &[
                "--git-dir",
                remote.to_str().unwrap(),
                "rev-parse",
                &checkpoint.remote_ref
            ]
        ),
        checkpoint.commit
    );
    assert!(
        git(
            &root,
            &[
                "--git-dir",
                remote.to_str().unwrap(),
                "show",
                &format!("{}:src/main.rs", checkpoint.commit)
            ]
        )
        .contains("\"after\"")
    );
    std::fs::write(
        root.join("accepted-result.json"),
        serde_json::to_vec_pretty(&accepted).unwrap(),
    )
    .unwrap();
    drop(worker);
    drop(client);
    drop(controller);
    let _restarted = spawn_controller();
    let client = ApiClient::new(&origin, limits.clone()).unwrap();
    wait_controller(&client).await;
    let recovered = task(
        client
            .call(Operation::GetTask {
                task_id: submitted.id,
            })
            .await
            .unwrap(),
    );
    assert_eq!(recovered.result, accepted.result);
    assert_eq!(recovered.selected_checkpoint, accepted.selected_checkpoint);
    assert_eq!(recovered.status, TaskStatus::Succeeded);
    std::fs::write(
        root.join("recovered-result.json"),
        serde_json::to_vec_pretty(&recovered).unwrap(),
    )
    .unwrap();
}
