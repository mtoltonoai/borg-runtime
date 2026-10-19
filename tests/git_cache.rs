//! Actual local Git only. These tests do not establish remote availability,
//! hostile-workspace isolation, deployed worker integration or provider behavior.
use borg_runtime::protocol::Repository;
use borg_runtime::worker::{
    git,
    process::{self, Control},
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
static NEXT: AtomicU64 = AtomicU64::new(1);
fn area(label: &str) -> PathBuf {
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("artifacts/git-cache-tests");
    std::fs::create_dir_all(&base).unwrap();
    let root = base.join(format!(
        "{label}-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    root
}
fn run(cwd: &Path, args: &[&str]) -> String {
    let out = std::process::Command::new("/usr/bin/git")
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "user.name=Cache Fixture",
            "-c",
            "user.email=fixture@example.invalid",
        ])
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "Git fixture: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().into()
}
fn repository(root: &Path) -> Repository {
    let seed = root.join("seed");
    std::fs::create_dir(&seed).unwrap();
    run(&seed, &["init", "--quiet"]);
    std::fs::write(seed.join("answer.txt"), "original\n").unwrap();
    run(&seed, &["add", "answer.txt"]);
    run(&seed, &["commit", "--quiet", "-m", "initial"]);
    let commit = run(&seed, &["rev-parse", "HEAD"]);
    let remote = root.join("central.git");
    run(
        root,
        &[
            "clone",
            "--quiet",
            "--bare",
            "--no-hardlinks",
            seed.to_str().unwrap(),
            remote.to_str().unwrap(),
        ],
    );
    Repository {
        pull_request_target: None,
        registration: Some(borg_runtime::repository_catalogue::Revision {
            repository_id: "fixture-repository".into(),
            registration_revision: "fixture-revision".into(),
        }),
        url: format!("file://{}", remote.display()),
        start_commit: commit,
        checkpoint_remote: remote.to_string_lossy().into_owned(),
    }
}
fn control() -> (Control, watch::Sender<bool>) {
    let (tx, rx) = watch::channel(false);
    let runtime_limits = limits();
    let worker_limits: Arc<borg_runtime::limits::WorkerLimits> = Arc::from(runtime_limits.worker.clone());
    let validation_runtime = Arc::new(borg_runtime::native::ValidationRuntime::new(worker_limits.clone()).unwrap());
    (
        Control {
            runtime_limits,
            worker_limits,
            validation_runtime,
            deadline_ms: process::now_ms() + 90_000,
            cancel: rx,
        },
        tx,
    )
}
async fn checkout(
    root: &Path,
    attempt: &str,
    repo: &Repository,
    control: &Control,
) -> Result<PathBuf, String> {
    git::checkout_cached(
        &root.join("cache"),
        &root.join("workspaces"),
        attempt,
        repo,
        None,
        &root.join(format!("process-{attempt}")),
        control,
    )
    .await
}
fn common(path: &Path) -> PathBuf {
    PathBuf::from(run(
        path,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
    ))
}
fn files(root: &Path) -> Vec<PathBuf> {
    let mut result = vec![];
    if root.exists() {
        for entry in std::fs::read_dir(root).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                result.extend(files(&path))
            } else {
                result.push(path)
            }
        }
    }
    result
}
fn identities(root: &Path) -> std::collections::BTreeSet<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    files(root)
        .into_iter()
        .map(|p| {
            let m = std::fs::metadata(p).unwrap();
            (m.dev(), m.ino())
        })
        .collect()
}

#[tokio::test]
async fn concurrent_worktrees_share_only_objects_and_preserve_clean_validation_and_publication() {
    let root = area("parallel");
    let repo = repository(&root);
    let (c, _tx) = control();
    let (a, b) = tokio::join!(
        checkout(&root, "a", &repo, &c),
        checkout(&root, "b", &repo, &c)
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    let bare = common(&a);
    assert_eq!(bare, common(&b));
    assert_ne!(
        run(
            &a,
            &["rev-parse", "--path-format=absolute", "--git-path", "index"]
        ),
        run(
            &b,
            &["rev-parse", "--path-format=absolute", "--git-path", "index"]
        )
    );
    assert!(
        identities(&bare.join("objects"))
            .is_disjoint(&identities(&root.join("central.git/objects")))
    );
    std::fs::write(a.join("answer.txt"), "a answer\n").unwrap();
    std::fs::write(a.join(".env"), "fixture-only-private\n").unwrap();
    std::fs::write(b.join("answer.txt"), "b answer\n").unwrap();
    run(&a, &["add", "answer.txt"]);
    assert_eq!(run(&b, &["diff", "--cached", "--name-only"]), "");
    let commit_a = git::commit_source(&a, "a", &root.join("process-a"), &c)
        .await
        .unwrap();
    let checkpoint = git::checkpoint(&repo, "task-a", "a", commit_a.clone()).unwrap();
    git::publish(&a, &checkpoint, &root.join("process-a"), &c)
        .await
        .unwrap();
    git::publish(&a, &checkpoint, &root.join("process-a"), &c)
        .await
        .unwrap();
    assert_eq!(
        git::remote_commit(&a, &checkpoint, &root.join("process-a"), &c)
            .await
            .unwrap(),
        Some(commit_a.clone())
    );
    let validation = git::checkout_cached(
        &root.join("cache"),
        &root.join("validation"),
        "a",
        &repo,
        Some(&checkpoint),
        &root.join("process-validation"),
        &c,
    )
    .await
    .unwrap();
    assert_eq!(common(&validation), bare);
    assert_eq!(
        std::fs::read_to_string(validation.join("answer.txt")).unwrap(),
        "a answer\n"
    );
    assert!(!validation.join(".env").exists());
    assert!(
        git::source_unchanged(&validation, &commit_a, &root.join("process-validation"), &c)
            .await
            .unwrap()
    );
    let commit_b = git::commit_source(&b, "b", &root.join("process-b"), &c)
        .await
        .unwrap();
    assert_ne!(commit_a, commit_b);
    assert_eq!(run(&validation, &["rev-parse", "HEAD"]), commit_a);
    let mut conflict = checkpoint.clone();
    conflict.commit = commit_b;
    assert!(
        git::publish(&b, &conflict, &root.join("process-b"), &c)
            .await
            .is_err()
    );
    assert!(
        git::checkout_cached(
            &root.join("cache"),
            &root.join("validation"),
            "wrong-ref",
            &repo,
            Some(&conflict),
            &root.join("process-wrong"),
            &c
        )
        .await
        .is_err()
    );
    assert_eq!(run(&a, &["rev-parse", "HEAD"]), commit_a);
    // Explicit cleanup refuses dirty/untracked data, and only removes its exact
    // identified clean validation tree. It never prunes another active tree.
    assert!(
        git::remove_cached_worktree(
            &root.join("cache"),
            &root.join("workspaces"),
            "a",
            &repo,
            &root.join("process-remove"),
            &c
        )
        .await
        .is_err()
    );
    git::remove_cached_worktree(
        &root.join("cache"),
        &root.join("validation"),
        "a",
        &repo,
        &root.join("process-remove"),
        &c,
    )
    .await
    .unwrap();
    assert!(!validation.exists());
    assert!(a.join(".env").exists());
    assert!(b.join("answer.txt").exists());
    assert!(
        checkout(&root, "a", &repo, &c)
            .await
            .unwrap_err()
            .contains("refusing to overwrite")
    );
}

fn add_large_commit(root: &Path) -> String {
    let seed = root.join("seed");
    let mut bytes = vec![0u8; 24 * 1024 * 1024];
    let mut state = 0x12345678u64;
    for byte in &mut bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state as u8;
    }
    std::fs::write(seed.join("large.bin"), bytes).unwrap();
    run(&seed, &["add", "large.bin"]);
    run(&seed, &["commit", "--quiet", "-m", "large source"]);
    let commit = run(&seed, &["rev-parse", "HEAD"]);
    run(
        &seed,
        &[
            "push",
            "--quiet",
            root.join("central.git").to_str().unwrap(),
            "HEAD:refs/heads/new-source",
        ],
    );
    commit
}
async fn wait_for_git(registry: &Path, verb: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if registry.exists() {
            for p in files(registry) {
                if p.extension().and_then(|s| s.to_str()) != Some("json") {
                    continue;
                }
                if let Ok(bytes) = std::fs::read(p) {
                    if let Ok(record) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                        if let Some(pid) = record["pid"].as_u64() {
                            if let Ok(command) = std::fs::read(format!("/proc/{pid}/cmdline")) {
                                if command.split(|b| *b == 0).any(|arg| arg == verb.as_bytes()) {
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "did not observe owned Git {verb}"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_cache_creation_is_never_adopted_and_next_attempt_recovers() {
    let root = area("interrupted-create");
    let mut repo = repository(&root);
    repo.start_commit = add_large_commit(&root);
    let (c, tx) = control();
    let (r, repository, ctl) = (root.clone(), repo.clone(), c.clone());
    let task = tokio::spawn(async move { checkout(&r, "cancelled", &repository, &ctl).await });
    wait_for_git(&root.join("process-cancelled"), "clone").await;
    tx.send(true).unwrap();
    assert!(task.await.unwrap().is_err());
    assert!(
        !root
            .join("workspaces")
            .join(process::component("cancelled"))
            .exists()
    );
    let stages: Vec<_> = std::fs::read_dir(root.join("cache"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(".creating-")
        })
        .collect();
    assert!(!stages.is_empty());
    let (renewed, _tx) = control();
    let result = checkout(&root, "recovered", &repo, &renewed).await.unwrap();
    assert_eq!(run(&result, &["rev-parse", "HEAD"]), repo.start_commit);
    assert!(stages.iter().all(|p| p.exists()));
    assert!(
        files(&root.join("process-cancelled"))
            .iter()
            .all(|p| p.extension().and_then(|s| s.to_str()) != Some("json"))
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_fetch_never_repoints_existing_worktree_and_exact_retry_recovers() {
    let root = area("interrupted-fetch");
    let mut repo = repository(&root);
    let (c, _tx) = control();
    let original = checkout(&root, "original", &repo, &c).await.unwrap();
    let original_commit = repo.start_commit.clone();
    repo.start_commit = add_large_commit(&root);
    let (cancel, tx) = control();
    let (r, repository, ctl) = (root.clone(), repo.clone(), cancel.clone());
    let task = tokio::spawn(async move { checkout(&r, "fetching", &repository, &ctl).await });
    wait_for_git(&root.join("process-fetching"), "fetch").await;
    tx.send(true).unwrap();
    assert!(task.await.unwrap().is_err());
    assert_eq!(run(&original, &["rev-parse", "HEAD"]), original_commit);
    assert_eq!(
        std::fs::read_to_string(original.join("answer.txt")).unwrap(),
        "original\n"
    );
    assert!(
        !root
            .join("workspaces")
            .join(process::component("fetching"))
            .exists()
    );
    let (renewed, _tx) = control();
    let recovered = checkout(&root, "fetching", &repo, &renewed).await.unwrap();
    assert_eq!(run(&recovered, &["rev-parse", "HEAD"]), repo.start_commit);
    assert_eq!(common(&original), common(&recovered));
    assert_eq!(run(&original, &["rev-parse", "HEAD"]), original_commit);
    assert!(
        files(&root.join("process-fetching"))
            .iter()
            .all(|p| p.extension().and_then(|s| s.to_str()) != Some("json"))
    );
}
#[tokio::test]
async fn cache_identity_and_external_ownership_fail_closed_without_overwrite() {
    let root = area("identity");
    let repo = repository(&root);
    let (c, _tx) = control();
    let original = checkout(&root, "original", &repo, &c).await.unwrap();
    let bare = common(&original);
    let cache = bare.parent().unwrap();
    let marker = cache.join("identity.json");
    let bytes = std::fs::read(&marker).unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value["source_endpoint"] = serde_json::json!("file:///different-repository");
    std::fs::write(&marker, serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(
        checkout(&root, "mismatch", &repo, &c)
            .await
            .unwrap_err()
            .contains("identity mismatch")
    );
    std::fs::write(&marker, &bytes).unwrap();
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.join("cache").join(format!(
            "{}.lock",
            cache.file_name().unwrap().to_string_lossy()
        )))
        .unwrap();
    lock.try_lock().unwrap();
    assert!(
        checkout(&root, "busy", &repo, &c)
            .await
            .unwrap_err()
            .contains("another process")
    );
    drop(lock);
    let result = checkout(&root, "busy", &repo, &c).await.unwrap();
    assert_eq!(common(&result), bare);
    assert_eq!(run(&original, &["rev-parse", "HEAD"]), repo.start_commit);
}

// Traceability scope: Real Git reconstructs the exact acknowledged commit from a separate checkpoint repository after the original source becomes unavailable.
//= spec/impl/v2/implementation.md#work-starts-from-a-durable-source
//= type=test
//# A workspace's starting revision MUST resolve to an immutable source revision available outside the worker.
#[tokio::test]
async fn replacement_cache_can_recover_only_from_acknowledged_checkpoint_destination() {
    let root = area("checkpoint-only-recovery");
    let mut repo = repository(&root);
    let destination = root.join("checkpoints.git");
    run(
        &root,
        &[
            "clone",
            "--quiet",
            "--bare",
            "--no-hardlinks",
            root.join("central.git").to_str().unwrap(),
            destination.to_str().unwrap(),
        ],
    );
    repo.checkpoint_remote = destination.to_string_lossy().into_owned();
    let (c, _tx) = control();
    let source = checkout(&root, "source", &repo, &c).await.unwrap();
    std::fs::write(source.join("answer.txt"), "acknowledged recovery\n").unwrap();
    let commit = git::commit_source(&source, "source", &root.join("process-source"), &c)
        .await
        .unwrap();
    let saved = git::checkpoint(&repo, "task", "source", commit.clone()).unwrap();
    git::publish(&source, &saved, &root.join("process-source"), &c)
        .await
        .unwrap();
    assert_eq!(saved.registration, repo.registration.clone().unwrap());
    assert_eq!(saved.repository, repo.checkpoint_remote);
    assert_eq!(run(&destination, &["rev-parse", &saved.remote_ref]), commit);
    assert!(run(&root.join("central.git"), &["for-each-ref", "--format=%(refname)", &saved.remote_ref]).is_empty());
    let mut changed_registration = saved.clone();
    changed_registration.registration.registration_revision.push_str("-other");
    assert!(git::checkout_cached(
        &root.join("refused-cache"), &root.join("refused-workspaces"), "wrong-revision",
        &repo, Some(&changed_registration), &root.join("process-refused"), &c,
    ).await.unwrap_err().contains("checkpoint registration"));
    assert!(!root.join("refused-cache").exists());
    assert!(!root.join("refused-workspaces").exists());
    let mut changed_destination = saved.clone();
    changed_destination.repository = repo.url.clone();
    assert!(git::checkout_cached(
        &root.join("refused-cache"), &root.join("refused-workspaces"), "wrong-destination",
        &repo, Some(&changed_destination), &root.join("process-refused"), &c,
    ).await.unwrap_err().contains("checkpoint destination"));
    assert!(!root.join("refused-cache").exists());
    assert!(!root.join("refused-workspaces").exists());
    std::fs::rename(
        root.join("central.git"),
        root.join("source-unavailable.git"),
    )
    .unwrap();
    let recovered = git::checkout_cached(
        &root.join("replacement-cache"),
        &root.join("replacement"),
        "recovered",
        &repo,
        Some(&saved),
        &root.join("process-recovered"),
        &c,
    )
    .await
    .unwrap();
    assert_eq!(run(&recovered, &["rev-parse", "HEAD"]), commit);
    assert_eq!(
        std::fs::read_to_string(recovered.join("answer.txt")).unwrap(),
        "acknowledged recovery\n"
    );
}

#[tokio::test]
async fn repository_metadata_refusals_name_the_selected_bound_without_changing_cache() {
    let root = area("metadata-refusals");
    let repo = repository(&root);
    let (c, _tx) = control();
    let original = checkout(&root, "original", &repo, &c).await.unwrap();
    let marker = common(&original).parent().unwrap().join("identity.json");
    let retained = std::fs::read(&marker).unwrap();
    let mut policy = (*c.worker_limits).clone();
    policy.repository_identity_bytes = repo.url.len() - 1;
    let short_identity = Control { worker_limits: Arc::new(policy), ..c.clone() };
    let error = checkout(&root, "identity-refused", &repo, &short_identity).await.unwrap_err();
    assert!(error.starts_with("repository identity exceeds cache bound"));
    assert!(error.contains("worker.repository_identity_bytes"));
    assert_eq!(std::fs::read(&marker).unwrap(), retained);
    let mut policy = (*c.worker_limits).clone();
    policy.git_cache_identity_bytes = retained.len() - 1;
    let short_marker = Control { worker_limits: Arc::new(policy), ..c.clone() };
    let error = checkout(&root, "marker-refused", &repo, &short_marker).await.unwrap_err();
    assert!(error.starts_with("invalid repository cache identity record"));
    assert!(error.contains("worker.git_cache_identity_bytes"));
    assert_eq!(std::fs::read(&marker).unwrap(), retained);
    assert_eq!(run(&original, &["rev-parse", "HEAD"]), repo.start_commit);
}

#[tokio::test]
async fn cache_binding_separates_registration_revisions_and_both_endpoints() {
    let root = area("registration-cache");
    let repo = repository(&root);
    let (c, _tx) = control();
    let original = checkout(&root, "original", &repo, &c).await.unwrap();
    let original_bare = common(&original);
    let marker = original_bare.parent().unwrap().join("identity.json");
    let retained = std::fs::read(&marker).unwrap();
    let identity: serde_json::Value = serde_json::from_slice(&retained).unwrap();
    assert_eq!(identity["version"], 2);
    assert_eq!(identity["registration"], serde_json::to_value(repo.registration.as_ref().unwrap()).unwrap());
    assert_eq!(identity["source_endpoint"], repo.url);
    assert_eq!(identity["checkpoint_endpoint"], repo.checkpoint_remote);
    let same = checkout(&root, "same", &repo, &c).await.unwrap();
    assert_eq!(common(&same), original_bare);

    let destination = root.join("separate-checkpoints.git");
    run(&root, &["clone", "--quiet", "--bare", "--no-hardlinks",
        root.join("central.git").to_str().unwrap(), destination.to_str().unwrap()]);
    let mut changed_id = repo.clone();
    changed_id.registration.as_mut().unwrap().repository_id.push_str("-other");
    let mut changed_revision = repo.clone();
    changed_revision.registration.as_mut().unwrap().registration_revision.push_str("-new");
    let mut changed_source = repo.clone();
    changed_source.url = root.join("central.git").to_string_lossy().into_owned();
    let mut changed_destination = repo.clone();
    changed_destination.checkpoint_remote = destination.to_string_lossy().into_owned();
    let mut caches = std::collections::BTreeSet::from([original_bare.clone()]);
    for (name, changed) in [
        ("changed-id", changed_id), ("changed-revision", changed_revision),
        ("changed-source", changed_source), ("changed-destination", changed_destination),
    ] {
        let workspace = checkout(&root, name, &changed, &c).await.unwrap();
        assert!(caches.insert(common(&workspace)), "a distinct accepted binding must not alias a cache");
        assert_eq!(run(&workspace, &["rev-parse", "HEAD"]), repo.start_commit);
        assert_eq!(std::fs::read(&marker).unwrap(), retained, "the original binding is never relabeled");
    }
    assert_eq!(caches.len(), 5);
    assert_eq!(run(&original_bare, &["config", "--get", "remote.origin.url"]), repo.url);
    assert_eq!(run(&original, &["rev-parse", "HEAD"]), repo.start_commit);
}

#[tokio::test]
async fn missing_registration_and_legacy_cache_marker_refuse_without_rewrite() {
    let root = area("registration-refusal");
    let mut repo = repository(&root);
    let (c, _tx) = control();
    let registration = repo.registration.take().unwrap();
    assert!(checkout(&root, "unbound", &repo, &c).await.unwrap_err().contains("registration is missing"));
    assert!(git::checkpoint(&repo, "task", "unbound", repo.start_commit.clone()).unwrap_err().contains("registration is missing"));
    assert!(!root.join("cache").exists());
    assert!(!root.join("workspaces").exists());
    repo.registration = Some(registration);
    let original = checkout(&root, "original", &repo, &c).await.unwrap();
    let marker = common(&original).parent().unwrap().join("identity.json");
    let legacy = serde_json::to_vec(&serde_json::json!({"version":1,"repository":repo.url})).unwrap();
    std::fs::write(&marker, &legacy).unwrap();
    assert!(checkout(&root, "legacy", &repo, &c).await.unwrap_err().contains("invalid repository cache identity"));
    assert_eq!(std::fs::read(&marker).unwrap(), legacy);
    assert!(!root.join("workspaces").join(process::component("legacy")).exists());
    assert_eq!(run(&original, &["rev-parse", "HEAD"]), repo.start_commit);
}
