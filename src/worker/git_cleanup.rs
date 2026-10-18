//! Exact-path disposal helpers. The worker owns canonical eligibility and intent.
use super::*;
use serde::{Deserialize, Serialize};
use std::os::unix::fs::MetadataExt;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectoryIdentity {
    pub device: u64,
    pub inode: u64,
}
impl DirectoryIdentity {
    pub(crate) fn read(path: &Path) -> Result<Self, String> {
        let m = std::fs::symlink_metadata(path).map_err(|_| "cleanup directory unavailable")?;
        if !m.is_dir()
            || m.uid()
                != std::fs::metadata("/proc/self")
                    .map_err(|_| "cleanup owner unavailable")?
                    .uid()
        {
            return Err("cleanup directory type or owner mismatch".into());
        }
        Ok(Self {
            device: m.dev(),
            inode: m.ino(),
        })
    }
    fn matches(&self, path: &Path) -> Result<(), String> {
        if Self::read(path)? != *self {
            return Err("cleanup directory identity changed".into());
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorktreeProof {
    path: PathBuf,
    root: DirectoryIdentity,
    directory: DirectoryIdentity,
    cache: DirectoryIdentity,
    head: String,
    discard_source: bool,
}

fn plain_root(root: &Path) -> Result<PathBuf, String> {
    DirectoryIdentity::read(root)?;
    let canonical = std::fs::canonicalize(root).map_err(|_| "cleanup root unavailable")?;
    // The worker's canonical workspace root is the only ancestor authority.
    if canonical != root {
        return Err("cleanup root contains a symlink".into());
    }
    Ok(canonical)
}

async fn association(
    directory: &Path,
    bare: &Path,
    registry: &Path,
    control: &Control,
) -> Result<(), String> {
    let common = git(
        vec![
            "rev-parse".into(),
            "--path-format=absolute".into(),
            "--git-common-dir".into(),
        ],
        directory,
        registry,
        control,
    )
    .await?;
    if Path::new(&common) != bare {
        return Err("cleanup worktree cache mismatch".into());
    }
    Ok(())
}

async fn clean(
    directory: &Path,
    head: &str,
    registry: &Path,
    control: &Control,
) -> Result<(), String> {
    if git(
        vec!["rev-parse".into(), "HEAD".into()],
        directory,
        registry,
        control,
    )
    .await?
        != head
    {
        return Err("cleanup worktree checkpoint mismatch".into());
    }
    // Ignored build products are disposable. Every tracked modification and
    // nonignored untracked source file prevents successful-task disposal.
    if !git(
        vec![
            "status".into(),
            "--porcelain=v1".into(),
            "--untracked-files=all".into(),
        ],
        directory,
        registry,
        control,
    )
    .await?
    .is_empty()
    {
        return Err("cleanup successful worktree has unpublished source".into());
    }
    Ok(())
}

pub(crate) async fn prepare_worktree(
    cache_root: &Path,
    root: &Path,
    attempt: &str,
    repository: &Repository,
    expected: &str,
    discard_source: bool,
    registry: &Path,
    control: &Control,
) -> Result<Option<WorktreeProof>, String> {
    control.check()?;
    if std::fs::symlink_metadata(root).is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound) {
        return Ok(None);
    }
    let root = plain_root(root)?;
    let path = root.join(component(attempt));
    if std::fs::symlink_metadata(&path).is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound) {
        return Ok(None);
    }
    let directory = DirectoryIdentity::read(&path)?;
    let identity = cache_identity(repository, control)?;
    let cache_root = plain_root(cache_root)?;
    let name = cache_name(&identity);
    let _guard = cache_guard(&cache_root.join(format!("{name}.lock")), control).await?;
    let cache_path = cache_root.join(name);
    let bare = verify_cache(&cache_path, &identity, registry, control).await?;
    association(&path, &bare, registry, control).await?;
    let head = git(
        vec!["rev-parse".into(), "HEAD".into()],
        &path,
        registry,
        control,
    )
    .await?;
    if !valid_commit(&head) {
        return Err("cleanup worktree commit invalid".into());
    }
    if !discard_source {
        clean(&path, expected, registry, control).await?;
    }
    Ok(Some(WorktreeProof {
        path,
        root: DirectoryIdentity::read(&root)?,
        directory,
        cache: DirectoryIdentity::read(&cache_path)?,
        head,
        discard_source,
    }))
}

//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Cleanup MUST verify the exact owned path and repository identity before removing content.
pub(crate) async fn remove_worktree(
    proof: &WorktreeProof,
    cache_root: &Path,
    root: &Path,
    attempt: &str,
    repository: &Repository,
    registry: &Path,
    control: &Control,
    _resuming: bool,
) -> Result<(), String> {
    control.check()?;
    let root = plain_root(root)?;
    proof.root.matches(&root)?;
    let path = root.join(component(attempt));
    if path != proof.path {
        return Err("cleanup worktree path mismatch".into());
    }
    let identity = cache_identity(repository, control)?;
    let cache_root = plain_root(cache_root)?;
    let name = cache_name(&identity);
    let _guard = cache_guard(&cache_root.join(format!("{name}.lock")), control).await?;
    let cache_path = cache_root.join(name);
    proof.cache.matches(&cache_path)?;
    let bare = verify_cache(&cache_path, &identity, registry, control).await?;
    let inventory = git(
        vec!["worktree".into(), "list".into(), "--porcelain".into()],
        &bare,
        registry,
        control,
    )
    .await?;
    let listed = inventory.lines().any(|line| {
        line.strip_prefix("worktree ")
            .is_some_and(|value| Path::new(value) == path)
    });
    match std::fs::symlink_metadata(&path) {
        Ok(_) => {
            proof.directory.matches(&path)?;
            // An intent is not proof removal started. Recheck source even on
            // restart; a partial tree that cannot be proved stays uncertain.
            association(&path, &bare, registry, control).await?;
            if !proof.discard_source {
                clean(&path, &proof.head, registry, control).await?;
            }
            if !listed {
                return Err("cleanup worktree registration missing".into());
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && !listed => return Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err("cleanup worktree unavailable".into()),
    }
    #[cfg(test)]
    tests::cut(&path, tests::Point::BeforeWorktreeRemove);
    proof.root.matches(&root)?;
    proof.cache.matches(&cache_path)?;
    match std::fs::symlink_metadata(&path) {
        Ok(_) => proof.directory.matches(&path)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err("cleanup worktree final identity unavailable".into()),
    }
    // One exact, durably recorded, quiesced worktree only. Never --force twice,
    // lock override, prune, cache-parent deletion, or removal of another tree.
    git(
        vec![
            "worktree".into(),
            "remove".into(),
            "--force".into(),
            "--".into(),
            path.to_string_lossy().into_owned(),
        ],
        &bare,
        registry,
        control,
    )
    .await?;
    match std::fs::symlink_metadata(&path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        _ => return Err("cleanup worktree removal unverified".into()),
    }
    std::fs::File::open(root)
        .and_then(|f| f.sync_all())
        .map_err(|_| "cleanup directory sync failed")?;
    Ok(())
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct CacheProof {
    identity: CacheIdentity,
    root: DirectoryIdentity,
    directory: DirectoryIdentity,
    pub(crate) last_use_ms: u64,
    last_use_revision: u64,
}

pub(crate) fn key(repository: &Repository, control: &Control) -> Result<String, String> {
    Ok(cache_name(&cache_identity(repository, control)?))
}

pub(super) fn read_use(path: &Path, control: &Control) -> Result<Option<CacheUse>, String> {
    // This marker also exists when cleanup is disabled, so its finite read bound
    // comes from the existing cache identity metadata, not a hidden fallback.
    let value: Option<CacheUse> =
        super::super::cleanup::read_record(path, control.worker_limits.git_cache_identity_bytes)?;
    if value
        .as_ref()
        .is_some_and(|v| v.format_revision != 1 || v.use_revision == 0)
    {
        return Err("cache use marker invalid".into());
    }
    Ok(value)
}
pub(crate) fn has_removal_barrier(
    root: &Path,
    repository: &Repository,
    control: &Control,
) -> Result<bool, String> {
    let name = key(repository, control)?;
    match std::fs::symlink_metadata(root.join(format!("{name}.cleanup.json"))) {
        Ok(m) if m.is_file() => Ok(true),
        Ok(_) => Err("cleanup cache barrier type invalid".into()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err("cleanup cache barrier unreadable".into()),
    }
}

pub(crate) async fn prepare_cache_removal(
    root: &Path,
    repository: &Repository,
    registry: &Path,
    control: &Control,
) -> Result<Option<CacheProof>, String> {
    if std::fs::symlink_metadata(root).is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound) {
        return Ok(None);
    }
    let root = plain_root(root)?;
    let identity = cache_identity(repository, control)?;
    let name = cache_name(&identity);
    let _guard = cache_guard(&root.join(format!("{name}.lock")), control).await?;
    let path = root.join(name);
    if std::fs::symlink_metadata(&path).is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound) {
        return Ok(None);
    }
    let bare = verify_cache(&path, &identity, registry, control).await?;
    let inventory = git(
        vec!["worktree".into(), "list".into(), "--porcelain".into()],
        &bare,
        registry,
        control,
    )
    .await?;
    if inventory
        .lines()
        .filter_map(|l| l.strip_prefix("worktree "))
        .any(|p| Path::new(p) != bare)
    {
        return Err("cleanup cache has registered worktrees".into());
    }
    let use_path = path.join("last-use.json");
    let last_use = match read_use(&use_path, control)? {
        Some(record) => record,
        None => {
            let record = CacheUse {
                format_revision: 1,
                used_unix_ms: super::super::process::now_ms(),
                use_revision: 1,
            };
            super::super::process::atomic_json(&use_path, &record)?;
            record
        }
    };
    Ok(Some(CacheProof {
        identity,
        root: DirectoryIdentity::read(&root)?,
        directory: DirectoryIdentity::read(&path)?,
        last_use_ms: last_use.used_unix_ms,
        last_use_revision: last_use.use_revision,
    }))
}

//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Cleanup MUST verify the exact owned path and repository identity before removing content.
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Restart MUST reconcile incomplete cleanup before treating its resources as reclaimed.
pub(crate) async fn remove_cache(
    proof: &CacheProof,
    root: &Path,
    repository: &Repository,
    registry: &Path,
    control: &Control,
    entries_per_yield: usize,
    resuming: bool,
) -> Result<(), String> {
    let root = plain_root(root)?;
    proof.root.matches(&root)?;
    if proof.identity != cache_identity(repository, control)? {
        return Err("cleanup cache identity changed".into());
    }
    let name = cache_name(&proof.identity);
    let _guard = cache_guard(&root.join(format!("{name}.lock")), control).await?;
    let tombstone = root.join(format!("{name}.cleanup.json"));
    let bound = control
        .runtime_limits
        .retention
        .as_ref()
        .ok_or("cleanup policy absent")?
        .worker
        .record_bytes;
    let saved: Option<CacheProof> = super::super::cleanup::read_record(&tombstone, bound)?;
    if saved.as_ref().is_some_and(|saved| saved != proof) {
        return Err("cleanup cache tombstone changed".into());
    }
    let path = root.join(name);
    match std::fs::symlink_metadata(&path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            if saved.is_some() {
                std::fs::remove_file(&tombstone)
                    .map_err(|_| "cleanup tombstone retirement failed")?;
            }
            std::fs::File::open(&root)
                .and_then(|f| f.sync_all())
                .map_err(|_| "cleanup cache parent sync failed")?;
            return Ok(());
        }
        _ => proof.directory.matches(&path)?,
    }
    if !resuming || saved.is_none() {
        let bare = verify_cache(&path, &proof.identity, registry, control).await?;
        if git(
            vec!["worktree".into(), "list".into(), "--porcelain".into()],
            &bare,
            registry,
            control,
        )
        .await?
        .lines()
        .filter_map(|l| l.strip_prefix("worktree "))
        .any(|p| Path::new(p) != bare)
        {
            return Err("cleanup cache acquired a worktree".into());
        }
        let use_record: CacheUse = super::super::cleanup::read_record(
            &path.join("last-use.json"),
            control
                .runtime_limits
                .retention
                .as_ref()
                .ok_or("cleanup policy absent")?
                .worker
                .record_bytes,
        )?
        .ok_or("cleanup cache use missing")?;
        if use_record.format_revision != 1
            || use_record.used_unix_ms != proof.last_use_ms
            || use_record.use_revision != proof.last_use_revision
        {
            return Err("cleanup cache was reused".into());
        }
    }
    if saved.is_none() {
        crate::protocol::bounded_json_size(proof, bound)
            .map_err(|_| "retention.worker.record_bytes")?;
        super::super::process::atomic_json(&tombstone, proof)?;
    }
    remove_tree(&path, &proof.directory, control, entries_per_yield).await?;
    std::fs::remove_file(&tombstone).map_err(|_| "cleanup tombstone retirement failed")?;
    std::fs::File::open(root)
        .and_then(|f| f.sync_all())
        .map_err(|_| "cleanup cache parent sync failed")?;
    Ok(())
}

fn directory_handle(path: &Path) -> Result<std::fs::File, String> {
    use std::os::unix::fs::OpenOptionsExt;
    // Selected Linux UAPI flags; std owns the syscall, no unsafe/new dependency.
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(0x10000 | 0x20000)
        .open(path)
        .map_err(|_| "cleanup no-follow directory open failed".into())
}
fn descriptor_path(file: &std::fs::File) -> PathBuf {
    use std::os::fd::AsRawFd;
    PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()))
}
fn handle_matches(file: &std::fs::File, identity: &DirectoryIdentity) -> Result<(), String> {
    let m = file
        .metadata()
        .map_err(|_| "cleanup directory handle unreadable")?;
    if !m.is_dir() || m.dev() != identity.device || m.ino() != identity.inode {
        return Err("cleanup directory handle changed".into());
    }
    Ok(())
}
async fn remove_tree(
    root: &Path,
    identity: &DirectoryIdentity,
    control: &Control,
    entries_per_yield: usize,
) -> Result<(), String> {
    remove_tree_checked(root, identity, control, entries_per_yield, false, None).await
}

// Attempt-private disposable trees additionally refuse foreign-owned entries.
// Eligibility, durable intent and the exact parent chain are the caller's duty.
pub(crate) async fn remove_owned_tree(
    root: &Path, identity: &DirectoryIdentity, control: &Control, entries_per_yield: usize,
) -> Result<(), String> {
    remove_tree_checked(root, identity, control, entries_per_yield, true, None).await
}

// The caller already proved and holds the parent. Never reopen its proc-fd
// alias with O_NOFOLLOW: that alias itself is a symlink. Child opens remain
// no-follow, and the existing traversal still verifies every held identity.
pub(crate) async fn remove_owned_child(
    parent: &std::fs::File, name: &std::ffi::OsStr, identity: &DirectoryIdentity,
    control: &Control, entries_per_yield: usize,
) -> Result<(), String> {
    let mut parts = Path::new(name).components();
    if !matches!((parts.next(), parts.next()), (Some(std::path::Component::Normal(_)), None)) {
        return Err("cleanup child name invalid".into());
    }
    let root = descriptor_path(parent).join(name);
    remove_tree_checked(&root, identity, control, entries_per_yield, true, Some(parent)).await
}

async fn remove_tree_checked(
    root: &Path, identity: &DirectoryIdentity, control: &Control,
    entries_per_yield: usize, require_owner: bool, held_parent: Option<&std::fs::File>,
) -> Result<(), String> {
    let owner = std::fs::metadata("/proc/self").map_err(|_| "cleanup owner unavailable")?.uid();
    if entries_per_yield == 0 { return Err("cleanup yield bound absent".into()); }
    // Directory descriptors anchor every child lookup/unlink. A renamed parent
    // or replaced symlink cannot redirect traversal into a different directory.
    struct Frame {
        file: std::fs::File,
        parent: std::fs::File,
        name: std::ffi::OsString,
        identity: DirectoryIdentity,
        entries: std::fs::ReadDir,
    }
    let parent = match held_parent {
        Some(parent) => parent.try_clone().map_err(|_| "cleanup held parent clone failed")?,
        None => directory_handle(root.parent().ok_or("cleanup root parent absent")?)?,
    };
    let name = root
        .file_name()
        .ok_or("cleanup root name absent")?
        .to_owned();
    let anchored = descriptor_path(&parent).join(&name);
    let file = directory_handle(&anchored)?;
    handle_matches(&file, identity)?;
    #[cfg(test)]
    tests::cut(root, tests::Point::OpenedRoot);
    let entries =
        std::fs::read_dir(descriptor_path(&file)).map_err(|_| "cleanup root enumeration failed")?;
    let mut stack = vec![Frame {
        file,
        parent,
        name,
        identity: identity.clone(),
        entries,
    }];
    let mut used = 0;
    while let Some(frame) = stack.last_mut() {
        control.check()?;
        handle_matches(&frame.file, &frame.identity)?;
        // Refuse replacements, but use the held original directory for every
        // child operation rather than following a checked pathname afterward.
        frame
            .identity
            .matches(&descriptor_path(&frame.parent).join(&frame.name))?;
        match frame.entries.next() {
            Some(entry) => {
                let name = entry
                    .map_err(|_| "cleanup cache entry unavailable")?
                    .file_name();
                let p = descriptor_path(&frame.file).join(&name);
                let m =
                    std::fs::symlink_metadata(&p).map_err(|_| "cleanup cache entry unreadable")?;
                if require_owner && m.uid() != owner {
                    return Err("cleanup disposable tree has foreign owner".into());
                }
                if m.dev() != identity.device {
                    return Err("cleanup cache contains another filesystem".into());
                }
                if m.is_dir() {
                    #[cfg(test)]
                    tests::cut(&p, tests::Point::BeforeChildOpen);
                    let file = directory_handle(&p)?;
                    let id = DirectoryIdentity {
                        device: m.dev(),
                        inode: m.ino(),
                    };
                    handle_matches(&file, &id)?;
                    let entries = std::fs::read_dir(descriptor_path(&file))
                        .map_err(|_| "cleanup cache child unavailable")?;
                    let parent = frame
                        .file
                        .try_clone()
                        .map_err(|_| "cleanup parent handle unavailable")?;
                    stack.push(Frame {
                        file,
                        parent,
                        name,
                        identity: id,
                        entries,
                    });
                } else if m.file_type().is_file() || m.file_type().is_symlink() {
                    // unlink affects only this directory entry, never its target.
                    std::fs::remove_file(&p).map_err(|_| "cleanup cache unlink failed")?;
                    #[cfg(test)]
                    tests::cut(root, tests::Point::AfterUnlink);
                } else {
                    return Err("cleanup cache contains unsupported file type".into());
                }
            }
            None => {
                let p = descriptor_path(&frame.parent).join(&frame.name);
                frame.identity.matches(&p)?;
                std::fs::remove_dir(p).map_err(|_| "cleanup cache directory removal failed")?;
                frame
                    .parent
                    .sync_all()
                    .map_err(|_| "cleanup parent sync failed")?;
                stack.pop();
            }
        }
        used += 1;
        if used == entries_per_yield {
            used = 0;
            tokio::task::yield_now().await;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "cleanup_git_tests.rs"]
pub(crate) mod tests;
