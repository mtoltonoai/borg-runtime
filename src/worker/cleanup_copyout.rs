//! Bounded, restartable transcript export and terminal vendor-content disposal.
//! Original small Borg journal paths remain readable by both current and old workers.
use super::*;
use aws_lc_rs::digest;
use std::{
    ffi::OsStr,
    io::{Read, Write},
};

const TRANSCRIPTS: [&str; 3] = ["sessions", "archived_sessions", "history.jsonl"];
fn journal(name: &str) -> bool {
    matches!(
        name,
        "borg-thread.json"
            | "borg-usage.json"
            | "borg-collaboration-config.json"
            | "collaboration-codex"
            | "collaboration-notices"
            | "memory-notices"
            | "supplied-charter"
            | "completion"
    )
}

fn has_thread(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(fields) => fields.iter().any(|(key, value)| {
            (matches!(key.as_str(), "thread_id" | "threadId")
                && value.as_str().is_some_and(|s| !s.is_empty()))
                || has_thread(value)
        }),
        serde_json::Value::Array(values) => values.iter().any(has_thread),
        _ => false,
    }
}
// Corroborating Borg association evidence must not become a fictitious
// pre-thread failure when borg-thread.json is absent. Only original Borg
// journal names are read; vendor databases are neither parsed nor preserved.
async fn recorded_association(
    parent: &File,
    name: &str,
    policy: &WorkerRetentionLimits,
    control: &Control,
) -> Result<bool, String> {
    control.check()?;
    let path = fd_path(parent).join(name);
    let Some(m) = meta(&path)? else {
        return Ok(false);
    };
    owned(&m, parent)?;
    if m.is_dir() {
        let dir = child(parent, name, false)?;
        for nested in names(&dir, policy, control).await? {
            if Box::pin(recorded_association(&dir, &nested, policy, control)).await? {
                return Ok(true);
            }
        }
        Ok(false)
    } else if m.is_file() && name.ends_with(".json") {
        let value = read_record::<serde_json::Value>(&path, policy.record_bytes)?
            .ok_or("Borg journal disappeared")?;
        Ok(has_thread(&value))
    } else if m.file_type().is_symlink() {
        Err("Borg journal path is a symlink".into())
    } else {
        Ok(false)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Archive {
    parent: DirectoryIdentity,
    directory: DirectoryIdentity,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Export {
    name: String,
    present: bool,
    sha256: String,
    files: u64,
    bytes: u64,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Source {
    name: String,
    entry: Option<Entry>,
    signature: Export,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Entry {
    name: String,
    device: u64,
    inode: u64,
    kind: String,
}
fn id(file: &File) -> Result<DirectoryIdentity, String> {
    let m = file
        .metadata()
        .map_err(|_| "copyout directory stat failed")?;
    Ok(DirectoryIdentity {
        device: m.dev(),
        inode: m.ino(),
    })
}
fn meta(path: &Path) -> Result<Option<std::fs::Metadata>, String> {
    match std::fs::symlink_metadata(path) {
        Ok(m) => Ok(Some(m)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err("copyout entry unavailable".into()),
    }
}
fn owned(m: &std::fs::Metadata, parent: &File) -> Result<(), String> {
    let p = parent
        .metadata()
        .map_err(|_| "copyout parent stat failed")?;
    if m.uid() != p.uid() || m.dev() != p.dev() {
        return Err("copyout foreign owner or filesystem".into());
    }
    Ok(())
}
async fn names(
    file: &File,
    policy: &WorkerRetentionLimits,
    control: &Control,
) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut bytes = 0usize;
    let mut used = 0;
    if policy.filesystem_entries_per_yield == 0 {
        return Err("cleanup yield bound absent".into());
    }
    for e in std::fs::read_dir(fd_path(file)).map_err(|_| "copyout enumeration failed")? {
        control.check()?;
        let name = e
            .map_err(|_| "copyout entry unavailable")?
            .file_name()
            .into_string()
            .map_err(|_| "copyout entry name is not UTF-8")?;
        bytes = bytes
            .checked_add(name.len() + std::mem::size_of::<String>())
            .ok_or("copyout names overflow")?;
        if bytes > policy.record_bytes {
            return Err("retention.worker.record_bytes".into());
        }
        out.push(name);
        used += 1;
        if used == policy.filesystem_entries_per_yield {
            used = 0;
            tokio::task::yield_now().await;
        }
    }
    out.sort();
    Ok(out)
}
fn child(parent: &File, name: &str, create: bool) -> Result<File, String> {
    let path = fd_path(parent).join(name);
    if create {
        use std::os::unix::fs::DirBuilderExt;
        match std::fs::DirBuilder::new().mode(0o700).create(&path) {
            Ok(()) => parent
                .sync_all()
                .map_err(|_| "copyout parent sync failed")?,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => return Err("copyout directory create failed".into()),
        }
    }
    let m = meta(&path)?.ok_or("copyout directory missing")?;
    owned(&m, parent)?;
    open_directory(&path)
}
fn same(file: &File, path: &Path, before: &std::fs::Metadata) -> Result<(), String> {
    let now = file
        .metadata()
        .map_err(|_| "copyout open file stat failed")?;
    let at = meta(path)?.ok_or("copyout source disappeared")?;
    if [now.dev(), now.ino(), now.len()] != [before.dev(), before.ino(), before.len()]
        || now.mtime() != before.mtime()
        || now.mtime_nsec() != before.mtime_nsec()
        || at.dev() != now.dev()
        || at.ino() != now.ino()
        || !at.is_file()
    {
        return Err("copyout file changed".into());
    }
    Ok(())
}
async fn copy_entry(
    src: &File,
    dst: &File,
    name: &str,
    control: &Control,
    policy: &WorkerRetentionLimits,
) -> Result<(), String> {
    control.check()?;
    let sp = fd_path(src).join(name);
    let dp = fd_path(dst).join(name);
    let m = meta(&sp)?.ok_or("copyout source absent")?;
    owned(&m, src)?;
    if m.is_dir() {
        let source = child(src, name, false)?;
        let target = child(dst, name, true)?;
        if id(&source)?
            != (DirectoryIdentity {
                device: m.dev(),
                inode: m.ino(),
            })
        {
            return Err("copyout source replaced".into());
        }
        for n in names(&source, policy, control).await? {
            Box::pin(copy_entry(&source, &target, &n, control, policy)).await?;
            control.check()?;
            tokio::task::yield_now().await;
        }
        if DirectoryIdentity::read(&sp)? != id(&source)? {
            return Err("copyout source parent changed".into());
        }
        target
            .sync_all()
            .map_err(|_| "copyout directory sync failed")?;
    } else if m.is_file() && m.nlink() == 1 {
        let mut input = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(0x20000 | 0x800)
            .open(&sp)
            .map_err(|_| "copyout source open failed")?;
        same(&input, &sp, &m)?;
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(0x20000 | 0x800)
            .open(&dp)
            .map_err(|_| "copyout destination open failed")?;
        let target = output
            .metadata()
            .map_err(|_| "copyout destination stat failed")?;
        owned(&target, dst)?;
        if !target.is_file()
            || target.nlink() != 1
            || (target.dev(), target.ino()) == (m.dev(), m.ino())
        {
            return Err("copyout destination identity invalid".into());
        }
        output
            .set_len(0)
            .map_err(|_| "copyout destination truncate failed")?;
        let mut buf = vec![0; policy.record_bytes];
        if buf.is_empty() {
            return Err("retention.worker.record_bytes".into());
        }
        loop {
            control.check()?;
            let used = input.read(&mut buf).map_err(|_| "copyout read failed")?;
            if used == 0 {
                break;
            }
            output
                .write_all(&buf[..used])
                .map_err(|_| "copyout write failed")?;
            #[cfg(test)]
            git::cleanup::tests::cut(&sp, git::cleanup::tests::Point::AfterCopyChunk);
            tokio::task::yield_now().await;
        }
        same(&input, &sp, &m)?;
        output.sync_all().map_err(|_| "copyout file sync failed")?;
        let at = meta(&dp)?.ok_or("copyout destination disappeared")?;
        if at.dev() != target.dev() || at.ino() != target.ino() || !at.is_file() {
            return Err("copyout destination replaced".into());
        }
        dst.sync_all()
            .map_err(|_| "copyout destination parent sync failed")?;
    } else {
        return Err("copyout transcript is a link or unsupported entry".into());
    }
    Ok(())
}
async fn hash_entry(
    parent: &File,
    name: &str,
    relative: &str,
    hash: &mut digest::Context,
    files: &mut u64,
    bytes: &mut u64,
    control: &Control,
    policy: &WorkerRetentionLimits,
) -> Result<(), String> {
    control.check()?;
    if relative.len() > policy.record_bytes {
        return Err("retention.worker.record_bytes".into());
    }
    let path = fd_path(parent).join(name);
    let m = meta(&path)?.ok_or("copyout verification entry absent")?;
    owned(&m, parent)?;
    hash.update(relative.as_bytes());
    hash.update(&[0]);
    if m.is_dir() {
        hash.update(b"directory\0");
        let dir = child(parent, name, false)?;
        if id(&dir)?
            != (DirectoryIdentity {
                device: m.dev(),
                inode: m.ino(),
            })
        {
            return Err("copyout verified directory changed".into());
        }
        for n in names(&dir, policy, control).await? {
            Box::pin(hash_entry(
                &dir,
                &n,
                &format!("{relative}/{n}"),
                hash,
                files,
                bytes,
                control,
                policy,
            ))
            .await?;
            control.check()?;
            tokio::task::yield_now().await;
        }
        if DirectoryIdentity::read(&path)? != id(&dir)? {
            return Err("copyout verified parent changed".into());
        }
    } else if m.is_file() && m.nlink() == 1 {
        hash.update(b"file\0");
        hash.update(&m.len().to_be_bytes());
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(0x20000 | 0x800)
            .open(&path)
            .map_err(|_| "copyout verification open failed")?;
        same(&file, &path, &m)?;
        let mut buf = vec![0; policy.record_bytes];
        if buf.is_empty() {
            return Err("retention.worker.record_bytes".into());
        }
        loop {
            control.check()?;
            let used = file
                .read(&mut buf)
                .map_err(|_| "copyout verification read failed")?;
            if used == 0 {
                break;
            }
            hash.update(&buf[..used]);
            tokio::task::yield_now().await;
        }
        same(&file, &path, &m)?;
        *files = files.checked_add(1).ok_or("copyout file count overflow")?;
        *bytes = bytes
            .checked_add(m.len())
            .ok_or("copyout byte count overflow")?;
    } else {
        return Err("copyout verified transcript is a link or unsupported entry".into());
    }
    Ok(())
}
async fn signature(
    parent: &File,
    name: &str,
    control: &Control,
    policy: &WorkerRetentionLimits,
) -> Result<Export, String> {
    let present = meta(&fd_path(parent).join(name))?.is_some();
    let mut hash = digest::Context::new(&digest::SHA256);
    let mut files = 0;
    let mut bytes = 0;
    if present {
        hash_entry(
            parent, name, name, &mut hash, &mut files, &mut bytes, control, policy,
        )
        .await?;
    }
    Ok(Export {
        name: name.into(),
        present,
        sha256: hash
            .finish()
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect(),
        files,
        bytes,
    })
}
fn entry(home: &File, name: &str) -> Result<Option<Entry>, String> {
    let Some(m) = meta(&fd_path(home).join(name))? else {
        return Ok(None);
    };
    owned(&m, home)?;
    let kind = if m.is_dir() {
        "directory"
    } else if m.is_file() {
        "file"
    } else if m.file_type().is_symlink() {
        "link"
    } else {
        return Err("cleanup vendor entry unsupported".into());
    };
    Ok(Some(Entry {
        name: name.into(),
        device: m.dev(),
        inode: m.ino(),
        kind: kind.into(),
    }))
}
fn matches(a: &Entry, b: &Entry) -> bool {
    a.name == b.name && a.device == b.device && a.inode == b.inode && a.kind == b.kind
}
pub(super) async fn remaining_is_journals(
    home: &File,
    policy: &WorkerRetentionLimits,
    control: &Control,
) -> Result<(), String> {
    if names(home, policy, control)
        .await?
        .iter()
        .any(|n| !journal(n))
    {
        return Err("compacted home acquired vendor content".into());
    }
    Ok(())
}
fn verify_archive(state: &Path, record: &Progress) -> Result<(File, File), String> {
    let root = state_handle(state)?;
    let parent = child(&root, "codex-transcripts", false)?;
    let dir = child(&parent, &component(&record.attempt_id), false)?;
    let proof = record
        .archive
        .as_ref()
        .ok_or("copyout archive proof absent")?;
    if id(&parent)? != proof.parent || id(&dir)? != proof.directory {
        return Err("copyout archive replaced".into());
    }
    Ok((parent, dir))
}

//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Cleanup MUST copy required transcripts from an eligible terminal supplied-executor attempt's private home to retained storage outside the contents selected for removal.
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Cleanup MUST verify the completeness and durability of each required transcript copy before removing its source.
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Cleanup MUST reclaim the remaining unneeded contents of an eligible terminal supplied-executor attempt's private home under its configured retention policy.
pub(super) async fn compact(
    state: &Path,
    found: &Located,
    record: &mut Progress,
    control: &Control,
    config: &WorkerConfig,
    client: &ApiClient,
    active: &BTreeMap<String, Active>,
    receipt: &Receipt,
    expected_task: &Task,
) -> Result<(), String> {
    let policy = record.policy.clone();
    control.check()?;
    found.verify(state, &record.attempt_id)?;
    let home = open_directory(&found.anchored())?;
    if Some(&id(&home)?) != record.proof.as_ref().map(|p| &p.directory) {
        return Err("cleanup home replaced".into());
    }
    let thread = read_record::<serde_json::Value>(
        &fd_path(&home).join("borg-thread.json"),
        policy.record_bytes,
    )?;
    if thread.is_none() {
        for name in [
            "borg-usage.json",
            "borg-collaboration-config.json",
            "collaboration-codex",
            "collaboration-notices",
            "memory-notices",
            "supplied-charter",
            "completion",
        ] {
            if recorded_association(&home, name, &policy, control).await? {
                return Err(
                    "recorded thread association missing its original Borg thread journal".into(),
                );
            }
        }
    }
    let expected = if let Some(thread) = thread {
        if !thread
            .get("thread_id")
            .and_then(|v| v.as_str())
            .is_some_and(|s| !s.is_empty())
        {
            return Err("copyout thread association invalid".into());
        }
        true
    } else {
        false
    };
    if record
        .transcript_expected
        .is_some_and(|saved| saved != expected)
    {
        return Err("copyout transcript obligation changed".into());
    }
    if record.transcript_expected.is_none() {
        record.transcript_expected = Some(expected);
        save(state, record)?;
    }
    if record.transcript_sources.is_none() {
        let mut sources = Vec::new();
        for name in TRANSCRIPTS {
            sources.push(Source {
                name: name.into(),
                entry: entry(&home, name)?,
                signature: signature(&home, name, control, &policy).await?,
            });
        }
        if expected
            && sources
                .iter()
                .filter(|v| v.name != "history.jsonl")
                .all(|v| v.signature.files == 0)
        {
            return Err("copyout expected session transcript missing".into());
        }
        record.transcript_sources = Some(sources);
        save(state, record)?;
    }
    let sources = record
        .transcript_sources
        .as_ref()
        .ok_or("copyout expected membership missing")?;
    if sources.len() != 3 || sources.iter().map(|v| v.name.as_str()).ne(TRANSCRIPTS) {
        return Err("copyout expected membership invalid".into());
    }
    if record.exports.is_none() {
        for source in sources {
            let now = entry(&home, &source.name)?;
            if signature(&home, &source.name, control, &policy).await? != source.signature {
                return Err("copyout expected transcript membership or content changed".into());
            }
            match (&source.entry, now) {
                (None, None) => {}
                (Some(original), Some(current)) if matches(original, &current) => {}
                _ => return Err("copyout expected transcript source disappeared or changed".into()),
            }
        }
    }
    if record.archive.is_none() {
        let root = state_handle(state)?;
        let parent = child(&root, "codex-transcripts", true)?;
        let archive = child(&parent, &component(&record.attempt_id), true)?;
        // An interrupted mkdir can be adopted only while empty. Never overwrite
        // previously unbound archive contents, even when owned by this worker.
        if !names(&archive, &policy, control).await?.is_empty() {
            return Err("copyout unbound archive is not empty".into());
        }
        record.archive = Some(Archive {
            parent: id(&parent)?,
            directory: id(&archive)?,
        });
        save(state, record)?;
    }
    let (_archive_parent, archive) = verify_archive(state, record)?;
    if record.exports.is_none() {
        let mut exports = Vec::new();
        for name in TRANSCRIPTS {
            control.check()?;
            if meta(&fd_path(&home).join(name))?.is_some() {
                copy_entry(&home, &archive, name, control, &policy).await?;
            }
            let source = signature(&home, name, control, &policy).await?;
            if record
                .transcript_sources
                .as_ref()
                .unwrap()
                .iter()
                .find(|s| s.name == name)
                .map(|s| &s.signature)
                != Some(&source)
            {
                return Err("copyout source changed during copy".into());
            }
            if signature(&archive, name, control, &policy).await? != source {
                return Err("copyout incomplete or changed transcript".into());
            }
            exports.push(source);
        }
        // A durable thread association establishes a session transcript
        // obligation. The current adapter records no vendor rollout pathname,
        // so retain every relative path under both session roots, refusing an
        // empty set. CLI history is optional and never substitutes for it.
        if expected
            && exports
                .iter()
                .filter(|e| e.name != "history.jsonl")
                .all(|e| e.files == 0)
        {
            return Err("copyout expected session transcript missing".into());
        }
        archive
            .sync_all()
            .map_err(|_| "copyout final archive sync failed")?;
        verify_archive(state, record)?;
        found.verify(state, &record.attempt_id)?;
        record.exports = Some(exports);
        save(state, record)?;
    }
    let exports = record.exports.clone().ok_or("copyout manifest absent")?;
    if exports.len() != TRANSCRIPTS.len() || exports.iter().map(|e| e.name.as_str()).ne(TRANSCRIPTS)
    {
        return Err("copyout manifest binding invalid".into());
    }
    for saved in &exports {
        if signature(&archive, &saved.name, control, &policy).await? != *saved {
            return Err("retained transcript verification failed".into());
        }
    }
    let expected = exports
        .iter()
        .filter(|e| e.present)
        .map(|e| e.name.clone())
        .collect::<std::collections::BTreeSet<_>>();
    if names(&archive, &policy, control)
        .await?
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
        != expected
    {
        return Err("copyout archive contains unbound entry".into());
    }
    loop {
        control.check()?;
        found.verify(state, &record.attempt_id)?;
        verify_archive(state, record)?;
        if DirectoryIdentity::read(&found.anchored())? != id(&home)? {
            return Err("cleanup home changed during removal".into());
        }
        if record.removing.is_none() {
            let Some(name) = names(&home, &policy, control)
                .await?
                .into_iter()
                .find(|n| !journal(n))
            else {
                break;
            };
            if let Some(saved) = exports.iter().find(|e| e.name == name) {
                if signature(&archive, &name, control, &policy).await? != *saved {
                    return Err("retained transcript changed before source removal".into());
                }
                if signature(&home, &name, control, &policy).await? != *saved {
                    return Err("transcript changed before source removal".into());
                }
            }
            record.removing = entry(&home, &name)?;
            save(state, record)?;
        }
        let removal = record
            .removing
            .as_ref()
            .ok_or("cleanup removal intent absent")?;
        if journal(&removal.name)
            || removal.name.is_empty()
            || removal.name.contains('/')
            || removal.name == "."
            || removal.name == ".."
        {
            return Err("cleanup removal intent selects protected path".into());
        }
        if let Some(saved) = exports.iter().find(|e| e.name == removal.name) {
            if signature(&archive, &removal.name, control, &policy).await? != *saved {
                return Err("retained transcript changed during source removal".into());
            }
        }
        // Copying/hashing may yield for substantial work. Re-establish actual
        // canonical and local eligibility after it, immediately before each
        // destructive child operation; command arrival can cancel the future.
        let current = canonical(client, &receipt.command, state, policy.operation_timeout_ms).await;
        if !matches!(current,Ok(ref task) if task==expected_task)
            || !local_ready(config, state, receipt, active).unwrap_or(false)
        {
            return Err("copyout cleanup authority changed before removal".into());
        }
        control.check()?;
        found.verify(state, &record.attempt_id)?;
        verify_archive(state, record)?;
        if let Some(current) = entry(&home, &removal.name)? {
            if !matches(removal, &current) {
                return Err("cleanup removal entry replaced".into());
            }
            if removal.kind == "directory" {
                git::cleanup::remove_owned_child(
                    &home,
                    OsStr::new(&removal.name),
                    &DirectoryIdentity {
                        device: removal.device,
                        inode: removal.inode,
                    },
                    control,
                    policy.filesystem_entries_per_yield,
                )
                .await?;
            } else {
                std::fs::remove_file(fd_path(&home).join(&removal.name))
                    .map_err(|_| "cleanup vendor unlink failed")?;
            }
        }
        if meta(&fd_path(&home).join(&removal.name))?.is_some() {
            return Err("cleanup removed entry replaced".into());
        }
        home.sync_all().map_err(|_| "cleanup home sync failed")?;
        record.removing = None;
        save(state, record)?;
        tokio::task::yield_now().await;
    }
    remaining_is_journals(&home, &policy, control).await?;
    found.verify(state, &record.attempt_id)?;
    verify_archive(state, record)?;
    Ok(())
}

// A previous completed claim is revalidated on restart; archive loss must not
// silently become evidence that a required transcript remains retained.
pub(super) async fn verify_completed(
    state: &Path,
    record: &Progress,
    control: &Control,
) -> Result<(), String> {
    let (_parent, archive) = verify_archive(state, record)?;
    let exports = record
        .exports
        .as_ref()
        .ok_or("completed copyout manifest missing")?;
    if exports.len() != 3 || exports.iter().map(|e| e.name.as_str()).ne(TRANSCRIPTS) {
        return Err("completed copyout manifest invalid".into());
    }
    for saved in exports {
        if signature(&archive, &saved.name, control, &record.policy).await? != *saved {
            return Err("completed retained transcript missing or corrupt".into());
        }
    }
    Ok(())
}
