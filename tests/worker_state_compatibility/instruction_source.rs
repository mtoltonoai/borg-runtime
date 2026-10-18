//! Genuine immediate-prior worker15 producer and reader; no provider/executor.
use super::*;
use borg_runtime::{limits::RuntimeLimits, protocol::*};
const PRIOR_SHA: &str = "ebeda84269b6b6acf646b9da41f7f5244bccef80e0aae9ffd66281febc3cbd05";

#[tokio::test]
#[ignore = "explicit preserved wire19/worker15 binary required"]
async fn genuine_worker15_migrates_to16_and_old_reader_refuses_before_ownership() {
    let old = PathBuf::from(
        std::env::var_os("BORG_SCHEMA20_BINARY").expect("select preserved genuine worker15"),
    );
    assert_hash(&old, PRIOR_SHA);
    let root = retained_images::artifact_root().join(format!(
        "instruction-worker-format-{}-{}",
        std::process::id(),
        process::now_ms()
    ));
    std::fs::create_dir(&root).unwrap();
    let metadata = root.join("limits.json");
    std::fs::write(
        &metadata,
        serde_json::to_vec(&RuntimeLimits::seed().unwrap()).unwrap(),
    )
    .unwrap();
    let selected = root.join("prior-runtime");
    retained_images::materialize(&old, &selected, true);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let workspace = root.join("workspace");
    let diagnostic = refused_worker(
        &selected,
        &metadata,
        &workspace,
        &origin,
        &root,
        "prior15-producer",
    )
    .await;
    assert!(
        diagnostic.contains("resource profile unavailable"),
        "{diagnostic}"
    );
    let state = workspace
        .join(".borg-worker")
        .join(process::component("compatibility"));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &std::fs::read(state.join("format.json")).unwrap()
        )
        .unwrap(),
        serde_json::json!({"format_revision":15})
    );
    assert!(!state.join("worker-owner.json").exists());
    assert!(
        tokio::time::timeout(Duration::from_millis(25), listener.accept())
            .await
            .is_err()
    );
    let old_limits = std::fs::read(state.join("limits.json")).unwrap();
    let current = Path::new(env!("CARGO_BIN_EXE_borg-runtime"));
    let mut worker =
        spawn_format9_worker(current, &metadata, &workspace, &origin, &root, "current16");
    let (mut stream, _) = tokio::time::timeout(Duration::from_secs(10), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let request = format9_read_request(&mut stream).await;
    assert_eq!(request.protocol_revision, 20);
    // A durable registration is reconciled before its first mutation POST.
    let registration_bytes = std::fs::read(state.join("mutations/registration.json")).unwrap();
    let journal: serde_json::Value = serde_json::from_slice(&registration_bytes).unwrap();
    let intended: Operation = serde_json::from_value(journal["request"].clone()).unwrap();
    let operation_id = match &intended {
        Operation::RegisterWorker {
            operation_id,
            worker,
        } => {
            assert_eq!(worker.id, "compatibility");
            assert_eq!(worker.incarnation, "current16");
            operation_id
        }
        other => panic!("unexpected retained registration: {other:?}"),
    };
    assert!(
        matches!(&request.command, Operation::GetOperation { operation_id: actual } if actual == operation_id)
    );
    assert!(journal["outcome"].is_null() && journal["outcome_context"].is_null());
    std::fs::write(
        root.join("current16-lookup-withheld.json"),
        serde_json::to_vec_pretty(
            &serde_json::json!({"lookup":request,"original_journal":journal}),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &std::fs::read(state.join("format.json")).unwrap()
        )
        .unwrap(),
        serde_json::json!({"format_revision":17})
    );
    assert_eq!(
        std::fs::read(state.join("limits.json")).unwrap(),
        old_limits
    );
    worker.stop().await;
    format9_hold_until_worker_closes(&mut stream).await;
    assert!(!state.join("worker-owner.json").exists());
    assert_eq!(
        std::fs::read(state.join("mutations/registration.json")).unwrap(),
        registration_bytes
    );
    assert!(std::fs::read_dir(state.join("commands"))
        .unwrap()
        .next()
        .is_none());
    let before = inventory(&state);
    let refused = refused_worker(
        &selected,
        &metadata,
        &workspace,
        &origin,
        &root,
        "old-refuses16",
    )
    .await;
    assert!(
        refused.contains("unsupported worker state format; no migration performed"),
        "{refused}"
    );
    assert_eq!(inventory(&state), before);
    assert!(
        tokio::time::timeout(Duration::from_millis(25), listener.accept())
            .await
            .is_err()
    );
    assert_hash(&old, PRIOR_SHA);
}
