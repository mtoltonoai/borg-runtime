//! Source checkpoints identify acknowledged Git objects, never a dirty directory.
use super::process::{Control, command, component};
use crate::protocol::{Checkpoint, Repository};
use crate::repository_catalogue::Revision;
use std::path::{Path, PathBuf};

pub fn valid_commit(s: &str) -> bool {
    matches!(s.len(), 40 | 64) && s.bytes().all(|b| b.is_ascii_hexdigit())
}
fn check_repository(repository: &str) -> Result<(), String> {
    if crate::credential::repository_contains_credentials(repository) {
        return Err(
            "repository identity contains credentials; use configured Git authentication".into(),
        );
    }
    Ok(())
}
async fn git(
    args: Vec<String>,
    cwd: &Path,
    registry: &Path,
    control: &Control,
) -> Result<String, String> {
    let mut full = vec![
        "-c".into(),
        "core.hooksPath=/dev/null".into(),
        "-c".into(),
        "credential.interactive=false".into(),
        "-c".into(),
        "gc.auto=0".into(),
        "-c".into(),
        "maintenance.auto=false".into(),
    ];
    full.extend(args);
    let result = command(
        Path::new("/usr/bin/git"),
        &full,
        cwd,
        registry,
        control,
        control.worker_limits.git_output_bytes,
    )
    .await?;
    if !result.success {
        return Err(
            "Git operation failed; inspect the task's repository and configured access".into(),
        );
    }
    if result.truncated {
        return Err("Git metadata exceeded its bound (worker.git_output_bytes)".into());
    }
    Ok(result.stdout.trim().to_owned())
}

// The cache is disposable object storage, not a checkpoint or publication receipt.
// Exact configured identities are retained and checked even when their local
// noncryptographic directory keys collide. No cache path grants repository access.
fn cache_name(identity: &CacheIdentity) -> String {
    use std::hash::{Hash, Hasher};
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    identity.version.hash(&mut hash);
    identity.registration.repository_id.hash(&mut hash);
    identity.registration.registration_revision.hash(&mut hash);
    identity.source_endpoint.hash(&mut hash);
    identity.checkpoint_endpoint.hash(&mut hash);
    format!("repository-{:016x}", hash.finish())
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct CacheIdentity {
    version: u32,
    registration: Revision,
    source_endpoint: String,
    checkpoint_endpoint: String,
}

fn accepted_registration(repository: &Repository) -> Result<&Revision, String> {
    let registration = repository.registration.as_ref()
        .ok_or("accepted repository registration is missing")?;
    if [&registration.repository_id, &registration.registration_revision].iter()
        .any(|value| value.is_empty() || value.chars().any(char::is_control))
    {
        return Err("accepted repository registration is invalid".into());
    }
    Ok(registration)
}

fn cache_identity(repository: &Repository, control: &Control) -> Result<CacheIdentity, String> {
    let registration = accepted_registration(repository)?;
    for value in [&registration.repository_id, &registration.registration_revision] {
        if value.len() > control.runtime_limits.transport.identity_bytes {
            return Err("accepted repository registration exceeds bound (transport.identity_bytes)".into());
        }
    }
    for endpoint in [&repository.url, &repository.checkpoint_remote] {
        check_repository(endpoint)?;
        if endpoint.is_empty() || endpoint.chars().any(char::is_control) {
            return Err("accepted repository endpoint is invalid".into());
        }
        if endpoint.len() > control.worker_limits.repository_identity_bytes {
            return Err("repository identity exceeds cache bound (worker.repository_identity_bytes)".into());
        }
    }
    let identity = CacheIdentity {
        version: 2,
        registration: registration.clone(),
        source_endpoint: repository.url.clone(),
        checkpoint_endpoint: repository.checkpoint_remote.clone(),
    };
    crate::protocol::bounded_json_size(&identity, control.worker_limits.git_cache_identity_bytes)
        .map_err(|_| "invalid repository cache identity record (worker.git_cache_identity_bytes)")?;
    Ok(identity)
}

struct CacheGuard {
    _local: tokio::sync::OwnedMutexGuard<()>,
    _file: std::fs::File,
}

async fn cache_guard(path: &Path, control: &Control) -> Result<CacheGuard, String> {
    use std::collections::BTreeMap;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    use std::sync::{Arc, Mutex, OnceLock, Weak};
    static LOCKS: OnceLock<Mutex<BTreeMap<PathBuf, Weak<tokio::sync::Mutex<()>>>>> =
        OnceLock::new();
    control.check()?;
    let mutex = {
        let mut locks = LOCKS
            .get_or_init(Mutex::default)
            .lock()
            .map_err(|_| "cache lock registry poisoned")?;
        locks.retain(|_, lock| lock.strong_count() != 0);
        match locks.get(path).and_then(Weak::upgrade) {
            Some(lock) => lock,
            None => {
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                locks.insert(path.into(), Arc::downgrade(&lock));
                lock
            }
        }
    };
    let mut stopping = control.clone();
    let local = tokio::select! {
        guard = mutex.lock_owned() => guard,
        _ = stopping.stopped() => return Err("cache wait cancelled or deadline reached".into()),
    };
    control.check()?;
    // Stable inode: never unlink a lock to defeat another process's ownership.
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(path)
        .map_err(|e| e.to_string())?;
    let metadata = std::fs::symlink_metadata(path).map_err(|e| e.to_string())?;
    if !metadata.file_type().is_file()
        || metadata.nlink() != 1
        || metadata.uid()
            != std::fs::metadata("/proc/self")
                .map_err(|e| e.to_string())?
                .uid()
        || file.metadata().map_err(|e| e.to_string())?.ino() != metadata.ino()
    {
        return Err("cache lock has unexpected identity or ownership".into());
    }
    file.try_lock()
        .map_err(|_| "repository cache is owned by another process")?;
    Ok(CacheGuard {
        _local: local,
        _file: file,
    })
}

fn plain_directory(path: &Path) -> Result<(), String> {
    let metadata = std::fs::symlink_metadata(path).map_err(|e| e.to_string())?;
    if !metadata.file_type().is_dir() {
        return Err("repository cache directory is not a plain directory".into());
    }
    Ok(())
}

async fn verify_cache(
    cache: &Path,
    identity: &CacheIdentity,
    registry: &Path,
    control: &Control,
) -> Result<PathBuf, String> {
    plain_directory(cache)?;
    let marker = cache.join("identity.json");
    let metadata = std::fs::symlink_metadata(&marker).map_err(|e| e.to_string())?;
    if !metadata.file_type().is_file() {
        return Err("invalid repository cache identity record".into());
    }
    if metadata.len() > control.worker_limits.git_cache_identity_bytes as u64 {
        return Err("invalid repository cache identity record (worker.git_cache_identity_bytes)".into());
    }
    let saved: CacheIdentity =
        serde_json::from_slice(&std::fs::read(marker).map_err(|e| e.to_string())?)
            .map_err(|_| "invalid repository cache identity record")?;
    if saved != *identity {
        return Err("repository cache identity mismatch; refusing reuse".into());
    }
    let bare = cache.join("repository.git");
    plain_directory(&bare)?;
    if git(
        vec!["rev-parse".into(), "--is-bare-repository".into()],
        &bare,
        registry,
        control,
    )
    .await?
        != "true"
        || git(
            vec!["config".into(), "--get".into(), "remote.origin.url".into()],
            &bare,
            registry,
            control,
        )
        .await?
            != identity.source_endpoint
    {
        return Err("repository cache configuration differs from its bound identity".into());
    }
    Ok(bare)
}

async fn prepare_cache(
    cache_root: &Path,
    cache: &Path,
    identity: &CacheIdentity,
    clone_source: &str,
    registry: &Path,
    control: &Control,
) -> Result<PathBuf, String> {
    if std::fs::symlink_metadata(cache).is_ok() {
        return verify_cache(cache, identity, registry, control).await;
    }
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let stage = cache_root.join(format!(
        ".creating-{}-{}-{}",
        std::process::id(),
        super::process::now_ms(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&stage).map_err(|e| e.to_string())?;
    // Failed/crashed construction is never a cache. Retain its isolated directory
    // for diagnosis; a later attempt creates a new one rather than adopting or
    // deleting bytes that an interrupted process might still own.
    let bare = stage.join("repository.git");
    git(
        vec![
            "clone".into(),
            "--quiet".into(),
            "--bare".into(),
            "--no-local".into(),
            "--no-hardlinks".into(),
            "--".into(),
            clone_source.into(),
            bare.to_string_lossy().into_owned(),
        ],
        cache_root,
        registry,
        control,
    )
    .await?;
    // Recovery may begin from the acknowledged checkpoint destination even if
    // the original source remote is unavailable. The cache remains bound to the
    // accepted logical repository, never to a model-selected replacement URL.
    git(
        vec![
            "remote".into(),
            "set-url".into(),
            "origin".into(),
            identity.source_endpoint.clone(),
        ],
        &bare,
        registry,
        control,
    )
    .await?;
    super::process::atomic_json(&stage.join("identity.json"), identity)?;
    verify_cache(&stage, identity, registry, control).await?;
    control.check()?;
    if std::fs::symlink_metadata(cache).is_ok() {
        return Err("repository cache appeared during construction".into());
    }
    std::fs::rename(&stage, cache).map_err(|e| e.to_string())?;
    std::fs::File::open(cache_root)
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())?;
    Ok(cache.join("repository.git"))
}

// Compatibility callers retain a cache within their own checkout root. The
// worker passes one explicit cache_root to checkout_cached for both edit and
// validation roots, so they share objects but never an index or working files.
//= spec/impl/v2/implementation.md#work-starts-from-a-durable-source
//# A workspace MUST identify its repository and starting revision.
pub async fn checkout(
    root: &Path,
    attempt: &str,
    repository: &Repository,
    checkpoint: Option<&Checkpoint>,
    registry: &Path,
    control: &Control,
) -> Result<PathBuf, String> {
    checkout_cached(
        &root.join("repositories"),
        root,
        attempt,
        repository,
        checkpoint,
        registry,
        control,
    )
    .await
}

pub async fn checkout_cached(
    cache_root: &Path,
    root: &Path,
    attempt: &str,
    repository: &Repository,
    checkpoint: Option<&Checkpoint>,
    registry: &Path,
    control: &Control,
) -> Result<PathBuf, String> {
    control.check()?;
    let identity = cache_identity(repository, control)?;
    if let Some(saved) = checkpoint {
        check_repository(&saved.repository)?;
        if saved.registration != identity.registration {
            return Err("checkpoint registration does not match the accepted repository revision".into());
        }
        if saved.repository != repository.checkpoint_remote {
            return Err(
                "checkpoint repository does not match the configured checkpoint destination".into(),
            );
        }
    }
    let start = checkpoint
        .map(|c| c.commit.as_str())
        .unwrap_or(&repository.start_commit);
    if !valid_commit(start) {
        return Err("workspace start must be an exact hexadecimal commit identity".into());
    }
    std::fs::create_dir_all(root).map_err(|e| e.to_string())?;
    let root = std::fs::canonicalize(root).map_err(|e| e.to_string())?;
    let directory = root.join(component(attempt));
    if std::fs::symlink_metadata(&directory).is_ok() {
        return Err(
            "existing attempt workspace requires reconciliation; refusing to overwrite it".into(),
        );
    }
    super::process::private_directory(cache_root)?;
    let cache_root = std::fs::canonicalize(cache_root).map_err(|e| e.to_string())?;
    let name = cache_name(&identity);
    let _guard = cache_guard(&cache_root.join(format!("{name}.lock")), control).await?;
    let bare = prepare_cache(
        &cache_root,
        &cache_root.join(name),
        &identity,
        checkpoint
            .map(|c| c.repository.as_str())
            .unwrap_or(&repository.url),
        registry,
        control,
    )
    .await?;
    // Never infer a replacement from dirty files or a local detached commit.
    //= spec/impl/v2/implementation.md#interrupted-work-is-reconciled
    //# Recovery MUST NOT assume that uncheckpointed workspace content survived worker loss.
    //= spec/impl/v2/implementation.md#loss-returns-to-the-last-checkpoint
    //# A replacement workspace MUST be reconstructed from an identified durable checkpoint.
    let url = checkpoint
        .map(|c| c.repository.as_str())
        .unwrap_or(&repository.url);
    if let Some(saved) = checkpoint {
        if !remote_commit(&bare, saved, registry, control)
            .await?
            .is_some_and(|c| c.eq_ignore_ascii_case(start))
        {
            return Err("remote checkpoint no longer identifies the expected commit".into());
        }
    }
    // No destination ref or FETCH_HEAD is written: interrupted fetch may leave
    // disposable pack temporaries, but cannot replace another attempt's ref.
    git(
        vec![
            "fetch".into(),
            "--quiet".into(),
            "--no-tags".into(),
            "--no-write-fetch-head".into(),
            "--no-auto-maintenance".into(),
            "--no-recurse-submodules".into(),
            "--".into(),
            url.into(),
            start.into(),
        ],
        &bare,
        registry,
        control,
    )
    .await?;
    if let Some(saved) = checkpoint {
        if !remote_commit(&bare, saved, registry, control)
            .await?
            .is_some_and(|c| c.eq_ignore_ascii_case(start))
        {
            return Err("remote checkpoint changed during source retrieval".into());
        }
    }
    let actual = git(
        vec!["rev-parse".into(), format!("{start}^{{commit}}")],
        &bare,
        registry,
        control,
    )
    .await?;
    if !actual.eq_ignore_ascii_case(start) {
        return Err("retrieved source differs from expected commit".into());
    }
    // No --force, branch checkout, prune or GC. A failed add is left for explicit
    // reconciliation; a later call never overwrites an existing attempt path.
    git(
        vec![
            "worktree".into(),
            "add".into(),
            "--quiet".into(),
            "--detach".into(),
            "--".into(),
            directory.to_string_lossy().into_owned(),
            start.into(),
        ],
        &bare,
        registry,
        control,
    )
    .await?;
    let actual = git(
        vec!["rev-parse".into(), "HEAD".into()],
        &directory,
        registry,
        control,
    )
    .await?;
    if !actual.eq_ignore_ascii_case(start) {
        return Err("checked out source differs from expected commit".into());
    }
    Ok(directory)
}

/// Remove only this explicitly identified, clean, quiesced worktree. The caller
/// owns quiescence; this never scans/prunes other worktrees or forces dirty loss.
/// Normal attempt retention is unchanged until a caller deliberately uses this.
pub async fn remove_cached_worktree(
    cache_root: &Path,
    root: &Path,
    attempt: &str,
    repository: &Repository,
    registry: &Path,
    control: &Control,
) -> Result<(), String> {
    control.check()?;
    let identity = cache_identity(repository, control)?;
    let cache_root = std::fs::canonicalize(cache_root).map_err(|e| e.to_string())?;
    let root = std::fs::canonicalize(root).map_err(|e| e.to_string())?;
    let directory = root.join(component(attempt));
    plain_directory(&directory)?;
    let name = cache_name(&identity);
    let _guard = cache_guard(&cache_root.join(format!("{name}.lock")), control).await?;
    let bare = verify_cache(&cache_root.join(name), &identity, registry, control).await?;
    let common = git(
        vec![
            "rev-parse".into(),
            "--path-format=absolute".into(),
            "--git-common-dir".into(),
        ],
        &directory,
        registry,
        control,
    )
    .await?;
    if Path::new(&common) != bare {
        return Err("worktree does not belong to the selected repository cache".into());
    }
    git(
        vec![
            "worktree".into(),
            "remove".into(),
            "--".into(),
            directory.to_string_lossy().into_owned(),
        ],
        &bare,
        registry,
        control,
    )
    .await?;
    Ok(())
}

pub async fn commit_source(
    workspace: &Path,
    attempt: &str,
    registry: &Path,
    control: &Control,
) -> Result<String, String> {
    // Rebuild the index so a pre-staged local credential file cannot bypass the
    // exclusions below. This is deliberately not a general content-secret scanner.
    git(
        vec![
            "reset".into(),
            "--quiet".into(),
            "--mixed".into(),
            "HEAD".into(),
        ],
        workspace,
        registry,
        control,
    )
    .await?;
    git(
        vec![
            "add".into(),
            "-A".into(),
            "--".into(),
            ".".into(),
            ":!.env".into(),
            ":!*.pem".into(),
            ":!*.key".into(),
            ":!id_rsa".into(),
            ":!credentials.json".into(),
        ],
        workspace,
        registry,
        control,
    )
    .await?;
    let changed = git(
        vec!["diff".into(), "--cached".into(), "--name-only".into()],
        workspace,
        registry,
        control,
    )
    .await?;
    if !changed.is_empty() {
        git(
            vec![
                "-c".into(),
                "user.name=Borg Worker".into(),
                "-c".into(),
                "user.email=borg@localhost".into(),
                "commit".into(),
                "--quiet".into(),
                "-m".into(),
                format!("Borg source checkpoint for {attempt}"),
            ],
            workspace,
            registry,
            control,
        )
        .await?;
    }
    let commit = git(
        vec!["rev-parse".into(), "HEAD".into()],
        workspace,
        registry,
        control,
    )
    .await?;
    if !valid_commit(&commit) {
        return Err("Git returned invalid commit identity".into());
    }
    Ok(commit)
}

pub async fn source_unchanged(
    workspace: &Path,
    commit: &str,
    registry: &Path,
    control: &Control,
) -> Result<bool, String> {
    let head = git(
        vec!["rev-parse".into(), "HEAD".into()],
        workspace,
        registry,
        control,
    )
    .await?;
    let changes = git(
        vec![
            "status".into(),
            "--porcelain".into(),
            "--untracked-files=no".into(),
        ],
        workspace,
        registry,
        control,
    )
    .await?;
    Ok(head == commit && changes.is_empty())
}

//= spec/impl/v2/implementation.md#work-starts-from-a-durable-source
//# A checkpoint MUST identify the exact work it preserves.
pub fn checkpoint(
    repository: &Repository,
    task: &str,
    attempt: &str,
    commit: String,
) -> Result<Checkpoint, String> {
    let registration = accepted_registration(repository)?.clone();
    let operation_id = format!("checkpoint-{attempt}");
    Ok(Checkpoint {
        registration,
        repository: repository.checkpoint_remote.clone(),
        commit,
        remote_ref: format!(
            "refs/heads/borg/tasks/{}/{}/{}",
            component(task),
            component(attempt),
            component(&operation_id)
        ),
        operation_id,
    })
}

pub async fn remote_commit(
    workspace: &Path,
    checkpoint: &Checkpoint,
    registry: &Path,
    control: &Control,
) -> Result<Option<String>, String> {
    check_repository(&checkpoint.repository)?;
    let text = git(
        vec![
            "ls-remote".into(),
            "--refs".into(),
            "--".into(),
            checkpoint.repository.clone(),
            checkpoint.remote_ref.clone(),
        ],
        workspace,
        registry,
        control,
    )
    .await?;
    parse_remote(&text, &checkpoint.remote_ref)
}

fn parse_remote(text: &str, expected: &str) -> Result<Option<String>, String> {
    if text.is_empty() {
        return Ok(None);
    }
    let mut lines = text.lines();
    let (commit, reference) = lines
        .next()
        .and_then(|line| line.split_once('\t'))
        .ok_or("malformed remote checkpoint response")?;
    if lines.next().is_some() || reference != expected || !valid_commit(commit) {
        return Err("ambiguous or invalid remote checkpoint response".into());
    }
    Ok(Some(commit.into()))
}
#[cfg(test)]
mod readback_tests {
    use super::*;
    #[test]
    fn only_successful_empty_or_one_exact_valid_ref_is_decisive() {
        let expected = "refs/heads/original";
        let commit = "a".repeat(40);
        assert_eq!(parse_remote("", expected).unwrap(), None);
        assert_eq!(
            parse_remote(&format!("{commit}\t{expected}"), expected).unwrap(),
            Some(commit.clone())
        );
        for text in [
            "malformed".to_owned(),
            format!("bad\t{expected}"),
            format!("{commit}\trefs/heads/other"),
            format!("{commit}\t{expected}\n{commit}\t{expected}"),
            format!("{commit}\t{expected}\nnoise"),
        ] {
            assert!(parse_remote(&text, expected).is_err());
        }
    }
}

//= spec/impl/v2/implementation.md#checkpoint-publication-is-reconciled
//# Checkpoint publication MUST NOT overwrite an unrelated remote reference.
//= spec/impl/v2/implementation.md#checkpoint-publication-is-reconciled
//# A publication with an uncertain outcome MUST be reconciled against its original remote reference and commit.
// Traceability scope: Uses the same repository/ref/exact commit on readback and retry; never changes an unresolved publication identity.
//= spec/impl/v2/implementation.md#interrupted-work-is-reconciled
//# A retry MUST preserve the original external-operation identity where the receiving interface supports deduplication.
pub async fn publish(
    workspace: &Path,
    checkpoint: &Checkpoint,
    registry: &Path,
    control: &Control,
) -> Result<(), String> {
    check_repository(&checkpoint.repository)?;
    if let Some(commit) = remote_commit(workspace, checkpoint, registry, control).await? {
        return if commit == checkpoint.commit {
            Ok(())
        } else {
            Err("checkpoint ref already has different content; refusing overwrite".into())
        };
    }
    let pushed = git(
        vec![
            "push".into(),
            "--porcelain".into(),
            format!("--force-with-lease={}:", checkpoint.remote_ref),
            "--".into(),
            checkpoint.repository.clone(),
            format!("{}:{}", checkpoint.commit, checkpoint.remote_ref),
        ],
        workspace,
        registry,
        control,
    )
    .await;
    // Explicit empty lease is compare-and-create: a racing unrelated update is never overwritten.
    match remote_commit(workspace,checkpoint,registry,control).await {
        Ok(Some(commit)) if commit==checkpoint.commit=>Ok(()),
        _=>Err(if pushed.is_ok(){"checkpoint push acknowledged but exact remote identity could not be verified; outcome uncertain"}else{"checkpoint publication outcome uncertain; reconcile the original ref before retry"}.into()),
    }
}
