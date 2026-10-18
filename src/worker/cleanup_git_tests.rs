//! Real private Git/filesystem effects; no executor/provider or deployed paths.
use super::*;
use crate::worker::process;
use std::{
    os::unix::fs::symlink,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::watch;

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum Point {
    OpenedRoot,
    BeforeChildOpen,
    AfterUnlink,
    AfterCopyChunk,
    BeforeWorktreeRemove,
}
type Hook = (Point, Box<dyn FnOnce(&Path)>);
thread_local! { static HOOK: std::cell::RefCell<Option<Hook>> = const { std::cell::RefCell::new(None) }; }
pub(crate) fn cut(path: &Path, point: Point) {
    let hook = HOOK.with(|h| {
        let mut h = h.borrow_mut();
        if h.as_ref().is_some_and(|(p, _)| *p == point) {
            h.take()
        } else {
            None
        }
    });
    if let Some((_, hook)) = hook {
        hook(path);
    }
}
pub(crate) fn hook(point: Point, call: impl FnOnce(&Path) + 'static) {
    HOOK.with(|h| {
        assert!(h.borrow_mut().replace((point, Box::new(call))).is_none());
    });
}
fn area(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let parent = std::env::var_os("BORG_TEST_ARTIFACT")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let root = parent.join(format!(
        "cleanup-{label}-{}-{}-{}",
        std::process::id(),
        process::now_ms(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&parent).unwrap();
    std::fs::create_dir(&root).unwrap();
    root
}
fn run(root: &Path, args: &[&str]) -> String {
    let out = std::process::Command::new("/usr/bin/git")
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "user.name=Cleanup Fixture",
            "-c",
            "user.email=cleanup@example.invalid",
        ])
        .args(args)
        .current_dir(root)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().into()
}
fn repo(root: &Path) -> Repository {
    let seed = root.join("seed");
    std::fs::create_dir(&seed).unwrap();
    run(&seed, &["init", "--quiet"]);
    std::fs::write(seed.join("source"), "original").unwrap();
    std::fs::write(seed.join(".gitignore"), "target/\n").unwrap();
    run(&seed, &["add", "."]);
    run(&seed, &["commit", "--quiet", "-m", "initial"]);
    let commit = run(&seed, &["rev-parse", "HEAD"]);
    let remote = root.join("remote.git");
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
        registration: Some(crate::repository_catalogue::Revision {
            repository_id: "cleanup".into(),
            registration_revision: "1".into(),
        }),
        url: format!("file://{}", remote.display()),
        checkpoint_remote: remote.to_string_lossy().into_owned(),
        start_commit: commit,
    }
}
fn control() -> (Control, watch::Sender<bool>) {
    let limits = Arc::new(crate::limits::RuntimeLimits::seed().unwrap());
    let (tx, rx) = watch::channel(false);
    let worker = Arc::new(limits.worker.clone());
    (
        Control {
            deadline_ms: process::now_ms() + 90_000,
            runtime_limits: limits,
            worker_limits: worker.clone(),
            validation_runtime: Arc::new(crate::native::ValidationRuntime::new(worker).unwrap()),
            cancel: rx,
        },
        tx,
    )
}
async fn checkout(root: &Path, id: &str, repo: &Repository, c: &Control) -> PathBuf {
    super::super::checkout_cached(
        &root.join("cache"),
        &root.join("workspaces"),
        id,
        repo,
        None,
        &root.join("process"),
        c,
    )
    .await
    .unwrap()
}
async fn prepare(
    root: &Path,
    id: &str,
    repo: &Repository,
    dirty: bool,
    c: &Control,
) -> Result<Option<WorktreeProof>, String> {
    prepare_worktree(
        &root.join("cache"),
        &root.join("workspaces"),
        id,
        repo,
        &repo.start_commit,
        dirty,
        &root.join("process"),
        c,
    )
    .await
}
async fn remove(
    root: &Path,
    id: &str,
    repo: &Repository,
    p: &WorktreeProof,
    resume: bool,
    c: &Control,
) -> Result<(), String> {
    remove_worktree(
        p,
        &root.join("cache"),
        &root.join("workspaces"),
        id,
        repo,
        &root.join("process"),
        c,
        resume,
    )
    .await
}

#[tokio::test]
async fn cleanup_real_git_ignored_outputs_reclaimed_dirty_success_retained() {
    let root = area("outputs");
    let repo = repo(&root);
    let (c, _tx) = control();
    let tree = checkout(&root, "a", &repo, &c).await;
    std::fs::create_dir(tree.join("target")).unwrap();
    std::fs::write(tree.join("target/build"), vec![7; 8192]).unwrap();
    std::fs::write(tree.join("unpublished"), "keep").unwrap();
    assert!(prepare(&root, "a", &repo, false, &c).await.is_err());
    assert!(tree.join("target/build").exists());
    std::fs::remove_file(tree.join("unpublished")).unwrap();
    std::fs::write(tree.join("source"), "dirty").unwrap();
    assert!(prepare(&root, "a", &repo, false, &c).await.is_err());
    std::fs::write(tree.join("source"), "original").unwrap();
    let p = prepare(&root, "a", &repo, false, &c)
        .await
        .unwrap()
        .unwrap();
    remove(&root, "a", &repo, &p, false, &c).await.unwrap();
    assert!(!tree.exists());
    assert_eq!(
        run(&root.join("remote.git"), &["rev-parse", "HEAD"]),
        repo.start_commit
    );
    assert!(root.join("cache").exists());
    let remaining = std::fs::read_dir(root.join("process"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert!(
        remaining
            .iter()
            .all(|path| path.file_name().unwrap() == "home"
                && std::fs::symlink_metadata(path).unwrap().is_dir())
    );
    assert!(root.join("process/home/code").is_dir());
}

#[tokio::test]
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Cache cleanup MUST preserve a cache while a worktree, accepted task or unresolved recovery record depends on that local cache.
// Scope: This case discriminates a live worktree dependency; accepted-task and unresolved-recovery cache joins are separate gates.
async fn cleanup_expired_failure_disposition_removes_dirty_tree_but_live_cache_is_held() {
    let root = area("failed");
    let repo = repo(&root);
    let (c, _tx) = control();
    let a = checkout(&root, "a", &repo, &c).await;
    let b = checkout(&root, "b", &repo, &c).await;
    std::fs::write(a.join("source"), "expired failed edit").unwrap();
    std::fs::write(a.join("private-draft"), "expired failed draft").unwrap();
    // The scheduler supplies this disposition only after canonical/grace gates.
    let p = prepare(&root, "a", &repo, true, &c).await.unwrap().unwrap();
    remove(&root, "a", &repo, &p, false, &c).await.unwrap();
    assert!(!a.exists());
    assert!(b.join("source").exists());
    assert!(
        prepare_cache_removal(&root.join("cache"), &repo, &root.join("process"), &c)
            .await
            .is_err()
    );
}

#[tokio::test]
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Restart MUST reconcile incomplete cleanup before treating its resources as reclaimed.
// Scope: Persists and resumes the exact Git proof; it does not kill and restart a worker during removal.
async fn cleanup_intent_restart_rechecks_new_source_and_replays_completed_removal() {
    let root = area("restart");
    let repo = repo(&root);
    let (c, _tx) = control();
    let tree = checkout(&root, "a", &repo, &c).await;
    let p = prepare(&root, "a", &repo, false, &c)
        .await
        .unwrap()
        .unwrap();
    let bytes = serde_json::to_vec(&p).unwrap();
    let retained: WorktreeProof = serde_json::from_slice(&bytes).unwrap();
    std::fs::write(tree.join("new-source"), "after intent").unwrap();
    assert!(
        remove(&root, "a", &repo, &retained, true, &c)
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read(tree.join("new-source")).unwrap(),
        b"after intent"
    );
    std::fs::remove_file(tree.join("new-source")).unwrap();
    remove(&root, "a", &repo, &retained, true, &c)
        .await
        .unwrap();
    remove(&root, "a", &repo, &retained, true, &c)
        .await
        .unwrap();
    assert!(!tree.exists());
    assert_eq!(serde_json::to_vec(&retained).unwrap(), bytes);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Cleanup MUST verify the exact owned path and repository identity before removing content.
async fn cleanup_wrong_cache_symlink_and_dangling_lock_do_not_touch_outside() {
    let root = area("paths");
    let repo = repo(&root);
    let (c, _tx) = control();
    let tree = checkout(&root, "a", &repo, &c).await;
    let p = prepare(&root, "a", &repo, false, &c)
        .await
        .unwrap()
        .unwrap();
    let outside = root.join("outside");
    std::fs::create_dir(&outside).unwrap();
    std::fs::write(outside.join("keep"), "outside").unwrap();
    let retained = root.join("retained-tree");
    std::fs::rename(&tree, &retained).unwrap();
    symlink(&outside, &tree).unwrap();
    assert!(remove(&root, "a", &repo, &p, true, &c).await.is_err());
    assert_eq!(std::fs::read(outside.join("keep")).unwrap(), b"outside");
    std::fs::remove_file(&tree).unwrap();
    std::fs::rename(&retained, &tree).unwrap();
    let mut other = repo.clone();
    other.registration.as_mut().unwrap().registration_revision = "wrong".into();
    assert!(remove(&root, "a", &other, &p, false, &c).await.is_err());
    let target = root.join("must-not-create");
    let lock = root.join("dangling.lock");
    symlink(&target, &lock).unwrap();
    assert!(super::super::cache_guard(&lock, &c).await.is_err());
    assert!(!target.exists());
    assert!(tree.join("source").exists());
}

#[tokio::test]
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Restart MUST reconcile incomplete cleanup before treating its resources as reclaimed.
// Scope: Direct cache partial-removal and exact-inode resume; worker-process mid-removal restart remains separate.
async fn cleanup_cache_partial_removal_keeps_reuse_barrier_and_resumes_exact_inode() {
    let root = area("cache-restart");
    let repo = repo(&root);
    let (c, tx) = control();
    checkout(&root, "a", &repo, &c).await;
    let p = prepare(&root, "a", &repo, false, &c)
        .await
        .unwrap()
        .unwrap();
    remove(&root, "a", &repo, &p, false, &c).await.unwrap();
    let proof = prepare_cache_removal(&root.join("cache"), &repo, &root.join("process"), &c)
        .await
        .unwrap()
        .unwrap();
    let tx2 = tx.clone();
    hook(Point::AfterUnlink, move |_| {
        tx2.send(true).unwrap();
    });
    assert!(
        remove_cache(
            &proof,
            &root.join("cache"),
            &repo,
            &root.join("process"),
            &c,
            1,
            false
        )
        .await
        .is_err()
    );
    assert!(has_removal_barrier(&root.join("cache"), &repo, &c).unwrap());
    let (next, _nexttx) = control();
    assert!(
        super::super::checkout_cached(
            &root.join("cache"),
            &root.join("workspaces"),
            "b",
            &repo,
            None,
            &root.join("process"),
            &next
        )
        .await
        .is_err()
    );
    remove_cache(
        &proof,
        &root.join("cache"),
        &repo,
        &root.join("process"),
        &next,
        1,
        true,
    )
    .await
    .unwrap();
    assert!(!has_removal_barrier(&root.join("cache"), &repo, &next).unwrap());
    assert!(!root.join("cache").join(key(&repo, &next).unwrap()).exists());
    assert!(root.join("remote.git").exists());
}

#[tokio::test]
async fn cleanup_descriptor_traversal_refuses_replaced_root_without_following_symlink() {
    let root = area("descriptor");
    let selected = root.join("selected");
    let moved = root.join("moved");
    let outside = root.join("outside");
    std::fs::create_dir(&selected).unwrap();
    std::fs::write(selected.join("own"), "own").unwrap();
    std::fs::create_dir(&outside).unwrap();
    std::fs::write(outside.join("keep"), "outside").unwrap();
    let id = DirectoryIdentity::read(&selected).unwrap();
    let selected2 = selected.clone();
    let outside2 = outside.clone();
    let moved2 = moved.clone();
    hook(Point::OpenedRoot, move |_| {
        std::fs::rename(&selected2, &moved2).unwrap();
        symlink(&outside2, &selected2).unwrap();
    });
    let (c, _tx) = control();
    assert!(remove_tree(&selected, &id, &c, 1).await.is_err());
    assert_eq!(std::fs::read(outside.join("keep")).unwrap(), b"outside");
    assert_eq!(std::fs::read(moved.join("own")).unwrap(), b"own");
}

#[tokio::test]
async fn cleanup_worktree_replacement_after_clean_is_refused_before_git_remove() {
    let root = area("handoff");
    let repo = repo(&root);
    let (c, _tx) = control();
    let tree = checkout(&root, "a", &repo, &c).await;
    let proof = prepare(&root, "a", &repo, false, &c)
        .await
        .unwrap()
        .unwrap();
    let retained = root.join("original-tree");
    let outside = root.join("outside");
    std::fs::create_dir(&outside).unwrap();
    std::fs::write(outside.join("keep"), "outside").unwrap();
    let retained2 = retained.clone();
    let outside2 = outside.clone();
    hook(Point::BeforeWorktreeRemove, move |path| {
        std::fs::rename(path, &retained2).unwrap();
        symlink(&outside2, path).unwrap();
    });
    assert!(remove(&root, "a", &repo, &proof, false, &c).await.is_err());
    assert_eq!(std::fs::read(outside.join("keep")).unwrap(), b"outside");
    assert_eq!(std::fs::read(retained.join("source")).unwrap(), b"original");
}
