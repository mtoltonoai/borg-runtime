use super::*;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request as HttpRequest, Response, body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn invalid_resource_profile_reaps_owned_process_and_retains_reports_without_new_work() {
    let root = area("resource-refusal-cleanup");
    let workspace = root.join("workspace");
    let state = workspace
        .join(".borg-worker")
        .join(process::component("worker"));
    std::fs::create_dir_all(&state).unwrap();
    let mut selected_policy = (*limits()).clone();
    selected_policy.worker.native_capacity = 1;
    selected_policy.validate().unwrap();
    let selected_policy = Arc::new(selected_policy);
    process::atomic_json(&state.join("limits.json"), &*selected_policy).unwrap();
    process::atomic_json(
        &state.join("format.json"),
        &serde_json::json!({"format_revision":9}),
    )
    .unwrap();
    for name in ["outbox", "mutations", "delegations"] {
        std::fs::create_dir_all(state.join(name)).unwrap();
    }
    // Pending work journals are deliberately not executable test inputs. If
    // resource refusal falls through to recovery it will fail/retry instead of
    // returning the profile error; their exact bytes must stay untouched.
    let await_bytes = b"pending original await must not be read or sent";
    let handoff_bytes = b"pending child creation must not be replayed";
    std::fs::write(state.join("mutations/await.json"), await_bytes).unwrap();
    std::fs::write(state.join("delegations/original.json"), handoff_bytes).unwrap();
    let report = Report {
        report_id: "retained-stop".into(),
        worker_id: "worker".into(),
        incarnation: "prior".into(),
        task_id: "task".into(),
        attempt_id: "attempt".into(),
        generation: 1,
        observation: Observation::ProcessTreeStopped,
    };
    let report_path = state
        .join("outbox")
        .join(format!("{}.json", process::component(&report.report_id)));
    process::atomic_json(&report_path, &report).unwrap();
    let original_report = std::fs::read(&report_path).unwrap();
    let registry = state.join("processes/prior");
    let mut child = process::ManagedProcess::spawn(
        Path::new("/bin/sleep"),
        &["30".into()],
        &root,
        &registry,
        &[],
        Arc::new(selected_policy.worker.clone()),
    )
    .await
    .unwrap();
    let pid = child.child.id().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let seen = observed.clone();
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let service = service_fn(move |request: HttpRequest<Incoming>| {
            let seen = seen.clone();
            async move {
                let request: Request = serde_json::from_slice(
                    &request.into_body().collect().await.unwrap().to_bytes(),
                )
                .unwrap();
                seen.lock().unwrap().push(request.command);
                let reply = Reply {
                    request_id: request.request_id,
                    commit: None,
                    observed_controller_revision: None,
                    outcome: Outcome::Refused {
                        code: "store_unavailable".into(),
                        message: "controlled retained-report outage".into(),
                    },
                };
                Ok::<_, std::convert::Infallible>(Response::new(Full::new(Bytes::from(
                    serde_json::to_vec(&reply).unwrap(),
                ))))
            }
        });
        let _ = hyper::server::conn::http1::Builder::new()
            .serve_connection(TokioIo::new(stream), service)
            .await;
    });
    let config = worker::WorkerConfig {
        limits: selected_policy.clone(),
        resource_profile: Some(root.join("missing-profile.json")),
        controller_url: format!("http://{address}"),
        id: "worker".into(),
        incarnation: "next".into(),
        workspace_root: workspace,
        codex_path: PathBuf::from("/must-not-launch"),
        coding_capacity: 1,
        native_capacity: 1,
        native_endpoint: "http://127.0.0.1:1/no-provider".into(),
        validation_executable: PathBuf::from(env!("CARGO_BIN_EXE_borg-runtime")),
    };
    let result = tokio::time::timeout(Duration::from_secs(10), worker::run(config))
        .await
        .expect("profile refusal and cleanup are bounded");
    // Capture worker-induced termination before any emergency fixture cleanup;
    // cleanup below must not make a skipped worker reap appear successful.
    let stopped_by_worker = child.child.try_wait().unwrap().is_some();
    let records_after_worker = process_records(&registry);
    // Always reap the fixture's original Child handle before assertions.
    let status = child
        .finish_registered(true, tokio::time::Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    peer.abort();
    let _ = peer.await;
    assert!(result.unwrap_err().contains("resource profile unavailable"));
    assert!(
        stopped_by_worker,
        "worker itself must terminate the original child"
    );
    assert_eq!(
        records_after_worker, 0,
        "worker itself must reconcile the ownership record"
    );
    assert!(!status.success());
    assert!(!Path::new(&format!("/proc/{pid}")).exists());
    assert_eq!(process_records(&registry), 0);
    let operations = observed.lock().unwrap();
    assert!(
        !operations.is_empty(),
        "bounded retained-report cleanup still attempted"
    );
    assert!(
        operations.iter().all(|op| match op {
            Operation::ObserveWorker { report: r } => r == &report,
            Operation::GetOperation { operation_id } => operation_id == &report.report_id,
            _ => false,
        }),
        "no capacity registration, wait replay or delegation on refusal: {operations:?}"
    );
    assert_eq!(std::fs::read(&report_path).unwrap(), original_report);
    assert_eq!(
        std::fs::read(state.join("mutations/await.json")).unwrap(),
        await_bytes
    );
    assert_eq!(
        std::fs::read(state.join("delegations/original.json")).unwrap(),
        handoff_bytes
    );
    let _lease = process::WorkerLease::claim(&state.join("worker-owner.json"), &selected_policy.worker).unwrap();
}

#[tokio::test]
// Traceability scope: An invalid configured resource profile refuses before worker registration or executor spawn.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# A worker MUST verify the configured host controls before claiming that those controls are enforced.
async fn resource_profile_cli_refuses_before_registering_capacity_or_spawning_executor() {
    let root = area("resource-cli");
    let profile = root.join("invalid.json");
    std::fs::write(&profile, b"{}").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let selected_metadata = write_limits(&root, &limits());
    let output = tokio::time::timeout(
        Duration::from_secs(10),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_borg-runtime"))
            .args([
                "worker",
                "--limits",
                selected_metadata.to_str().unwrap(),
                "--controller",
                &format!("http://{address}"),
                "--id",
                "worker",
                "--incarnation",
                "one",
                "--workspace-root",
                root.join("workspace").to_str().unwrap(),
                "--codex",
                "/must-not-launch",
                "--resource-profile",
                profile.to_str().unwrap(),
            ])
            .env_remove("OPENAI_API_KEY")
            .env_remove("CODEX_API_KEY")
            .env_remove("GH_TOKEN")
            .env_remove("GITHUB_TOKEN")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid worker resource profile"));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err(),
        "invalid configured envelope must not advertise capacity"
    );
}
