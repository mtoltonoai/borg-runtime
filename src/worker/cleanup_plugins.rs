//! Terminal Codex home compaction: retain Borg journals and export transcripts.
//! A new sibling sidecar leaves both strict legacy cleanup records untouched.
#[path = "cleanup_copyout.rs"]
mod copyout;
use super::*;
use git::cleanup::DirectoryIdentity;
use std::{fs::File, os::fd::AsRawFd};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Proof {
    path: PathBuf,
    parents: Vec<DirectoryIdentity>,
    directory: DirectoryIdentity,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Progress {
    format_revision: u32,
    command_id: String,
    attempt_id: String,
    policy: WorkerRetentionLimits,
    policy_revision: String,
    status: Option<TaskStatus>,
    checkpoint: Option<Checkpoint>,
    first_safe_ms: Option<u64>,
    last_observed_ms: u64,
    next_due_ms: Option<u64>,
    proof: Option<Proof>,
    archive: Option<copyout::Archive>,
    exports: Option<Vec<copyout::Export>>,
    transcript_expected: Option<bool>,
    transcript_sources: Option<Vec<copyout::Source>>,
    removing: Option<copyout::Entry>,
    started: bool,
    done: bool,
    reason: Reason,
}

fn path(state: &Path, attempt: &str) -> PathBuf {
    codex::state_path(&state.join("codex"), attempt)
}
fn fd_path(file: &File) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()))
}
fn open_directory(path: &Path) -> Result<File, String> {
    let id = DirectoryIdentity::read(path)?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(0x10000 | 0x20000)
        .open(path)
        .map_err(|_| "Codex cleanup directory open failed")?;
    let m = file
        .metadata()
        .map_err(|_| "Codex cleanup directory stat failed")?;
    if m.dev() != id.device || m.ino() != id.inode {
        return Err("Codex cleanup directory replaced".into());
    }
    Ok(file)
}
fn state_handle(state: &Path) -> Result<File, String> {
    if std::fs::canonicalize(state).map_err(|_| "Codex cleanup state unavailable")? != state {
        return Err("Codex cleanup state path is not canonical".into());
    }
    open_directory(state)
}

// Keep every ancestor open. Every subsequent lookup is relative to its held
// parent descriptor, never through a symlink or a newly substituted ancestor.
struct Located {
    handles: Vec<File>,
    name: String,
    parents: Vec<DirectoryIdentity>,
    directory: Option<DirectoryIdentity>,
}
impl Located {
    fn anchored(&self) -> PathBuf {
        fd_path(self.handles.last().unwrap()).join(&self.name)
    }
    fn verify(&self, state: &Path, attempt: &str) -> Result<(), String> {
        let names = ["codex".to_owned()];
        if self.name != component(attempt) {
            return Err("cleanup home binding changed".into());
        }
        if DirectoryIdentity::read(state)? != self.parents[0] {
            return Err("Codex cleanup state replaced".into());
        }
        for (i, name) in names.iter().enumerate() {
            if DirectoryIdentity::read(&fd_path(&self.handles[i]).join(name))?
                != self.parents[i + 1]
            {
                return Err("Codex cleanup ancestor replaced".into());
            }
        }
        Ok(())
    }
}
fn locate(state: &Path, attempt: &str) -> Result<Option<Located>, String> {
    let mut handles = vec![state_handle(state)?];
    let m = handles[0]
        .metadata()
        .map_err(|_| "Codex cleanup state handle unavailable")?;
    let mut parents = vec![DirectoryIdentity {
        device: m.dev(),
        inode: m.ino(),
    }];
    for name in ["codex".to_owned()] {
        let child = fd_path(handles.last().unwrap()).join(name);
        match std::fs::symlink_metadata(&child) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err("Codex cleanup ancestor unavailable".into()),
            Ok(_) => {}
        }
        let file = open_directory(&child)?;
        let id = DirectoryIdentity::read(&child)?;
        let m = file
            .metadata()
            .map_err(|_| "Codex cleanup ancestor stat failed")?;
        if m.dev() != parents[0].device || m.dev() != id.device || m.ino() != id.inode {
            return Err("Codex cleanup ancestor identity or device changed".into());
        }
        parents.push(id);
        handles.push(file);
    }
    let leaf = fd_path(handles.last().unwrap()).join(component(attempt));
    let directory = match std::fs::symlink_metadata(&leaf) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => return Err("Codex cleanup tree unavailable".into()),
        Ok(_) => Some(DirectoryIdentity::read(&leaf)?),
    };
    if directory
        .as_ref()
        .is_some_and(|id| id.device != parents[0].device)
    {
        return Err("Codex cleanup tree is on another filesystem".into());
    }
    let found = Located {
        handles,
        name: component(attempt),
        parents,
        directory,
    };
    found.verify(state, attempt)?;
    Ok(Some(found))
}

// Store through an owned, held sidecar-directory descriptor. Interrupted writes
// reuse only an owned regular single-link pending file, without following links.
fn sidecars(state: &Path) -> Result<File, String> {
    let root = state_handle(state)?;
    let directory = fd_path(&root).join("cleanup-codex");
    use std::os::unix::fs::DirBuilderExt;
    match std::fs::DirBuilder::new().mode(0o700).create(&directory) {
        Ok(()) => root
            .sync_all()
            .map_err(|_| "Codex cleanup sidecar parent sync failed")?,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => return Err("Codex cleanup sidecar directory create failed".into()),
    }
    open_directory(&directory)
}
fn save(state: &Path, record: &Progress) -> Result<(), String> {
    use std::io::Write;
    bounded_json_size(record, record.policy.record_bytes)
        .map_err(|_| "retention.worker.record_bytes")?;
    let directory = sidecars(state)?;
    let file = fd_path(&directory).join(format!("{}.json", component(&record.attempt_id)));
    let pending = file.with_extension("pending");
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(0x20000 | 0x800)
        .open(&pending)
        .map_err(|_| "Codex cleanup progress open failed")?;
    let m = output
        .metadata()
        .map_err(|_| "Codex cleanup progress stat failed")?;
    if !m.is_file()
        || m.nlink() != 1
        || m.uid()
            != directory
                .metadata()
                .map_err(|_| "Codex cleanup owner unavailable")?
                .uid()
    {
        return Err("Codex cleanup progress identity invalid".into());
    }
    output
        .set_len(0)
        .map_err(|_| "Codex cleanup progress truncate failed")?;
    output
        .write_all(&serde_json::to_vec(record).map_err(|e| e.to_string())?)
        .and_then(|_| output.sync_all())
        .map_err(|_| "Codex cleanup progress write failed")?;
    std::fs::rename(pending, file).map_err(|_| "Codex cleanup progress rename failed")?;
    directory
        .sync_all()
        .map_err(|_| "Codex cleanup progress directory sync failed")?;
    eprintln!(
        "BORG_CLEANUP codex-home={} reason={:?} attempt={} scope=terminal-codex-home-vendor-remainder",
        if record.done {
            "reclaimed"
        } else if record.started {
            "uncertain"
        } else {
            "retained"
        },
        record.reason,
        record.attempt_id
    );
    Ok(())
}
fn hold(
    state: &Path,
    record: &mut Progress,
    reason: Reason,
    now: u64,
) -> Result<Option<u64>, String> {
    record.reason = reason;
    if !matches!(reason, Reason::Clock | Reason::Filesystem) {
        record.first_safe_ms = None;
    }
    record.next_due_ms = Some(add_ms(now, record.policy.retry_delay_ms)?);
    save(state, record)?;
    Ok(record.next_due_ms)
}

//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Cleanup MUST use bounded work under its configured metadata policy.
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Restart MUST reconcile incomplete cleanup before treating its resources as reclaimed.
pub(super) async fn attempt_at(
    config: &WorkerConfig,
    state: &Path,
    client: &ApiClient,
    active: &BTreeMap<String, Active>,
    validation_runtime: &Arc<crate::native::ValidationRuntime>,
    policy: &WorkerRetentionLimits,
    policy_revision: &str,
    command_path: &Path,
    now: u64,
) -> Result<Option<u64>, String> {
    let receipt: Receipt = read_record(command_path, config.limits.worker.command_record_bytes)?
        .ok_or("Codex cleanup command disappeared")?;
    let WorkerCommand::RunAttempt {
        command_id,
        task,
        attempt,
        ..
    } = &receipt.command
    else {
        return Ok(None);
    };
    if task.definition.executor != ExecutorKind::Coding {
        return Ok(None);
    }
    if command_path
        != state
            .join("commands")
            .join(format!("{}.json", component(command_id)))
    {
        return Err("Codex cleanup command path mismatch".into());
    }
    // Absence is not a persistent reclamation claim. A later observed checkout
    // must undergo its own eligibility and grace even after old Git cleanup.
    let located = locate(state, &attempt.id)?;
    let directory = sidecars(state)?;
    let file = fd_path(&directory).join(format!("{}.json", component(&attempt.id)));
    let stored = read_record::<Progress>(&file, policy.record_bytes)?;
    if stored.is_none() && located.as_ref().is_none_or(|p| p.directory.is_none()) {
        return Ok(None);
    }
    let mut record = stored.unwrap_or_else(|| Progress {
        format_revision: 1,
        command_id: command_id.clone(),
        attempt_id: attempt.id.clone(),
        policy: policy.clone(),
        policy_revision: policy_revision.into(),
        status: None,
        checkpoint: None,
        first_safe_ms: None,
        last_observed_ms: now,
        next_due_ms: None,
        proof: None,
        archive: None,
        exports: None,
        transcript_expected: None,
        transcript_sources: None,
        removing: None,
        started: false,
        done: false,
        reason: Reason::Startup,
    });
    if record.format_revision != 1
        || record.command_id != *command_id
        || record.attempt_id != attempt.id
        || record.policy != *policy
        || record.policy_revision != policy_revision
        || record.started && record.proof.is_none()
        || record.done && !record.started
        || record
            .proof
            .as_ref()
            .is_some_and(|p| p.path != path(state, &attempt.id) || p.parents.len() != 2)
    {
        return Err("Codex cleanup retained binding invalid".into());
    }
    let (_sender, cancel) = watch::channel(false);
    let control = Control {
        deadline_ms: add_ms(process::now_ms(), policy.operation_timeout_ms)?,
        worker_limits: Arc::new(config.limits.worker.clone()),
        runtime_limits: config.limits.clone(),
        validation_runtime: validation_runtime.clone(),
        cancel,
    };
    if record.done {
        let found = located.ok_or("compacted home missing")?;
        found.verify(state, &attempt.id)?;
        if found.directory.as_ref() != record.proof.as_ref().map(|p| &p.directory) {
            return Err("compacted home replaced".into());
        }
        copyout::verify_completed(state, &record, &control).await?;
        copyout::remaining_is_journals(&open_directory(&found.anchored())?, policy, &control)
            .await?;
        return Ok(None);
    }
    if now < record.last_observed_ms {
        return hold(state, &mut record, Reason::Clock, now);
    }
    record.last_observed_ms = now;
    if !local_ready(config, state, &receipt, active).unwrap_or(false) {
        return hold(state, &mut record, Reason::LocalRecovery, now);
    }
    // Never inherit the old Reclaimed / worktrees-done canonical shortcuts.
    let current =
        match canonical(client, &receipt.command, state, policy.operation_timeout_ms).await {
            Ok(current) => current,
            Err(reason) => return hold(state, &mut record, reason, now),
        };
    let workspace: Record =
        read_record(&super::record_path(state, &attempt.id), policy.record_bytes)?
            .ok_or("Codex cleanup source proof absent")?;
    if workspace.format_revision != 1
        || workspace.command_id != *command_id
        || workspace.attempt_id != attempt.id
        || workspace.policy != *policy
        || workspace.policy_revision != policy_revision
    {
        return Err("Codex cleanup source binding invalid".into());
    }
    if workspace.status.as_ref() != Some(&current.status)
        || workspace.checkpoint != current.selected_checkpoint
        || !(workspace.worktrees.iter().all(|s| s.done) || workspace.reason == Reason::Grace)
    {
        return hold(state, &mut record, Reason::SourceOrPath, now);
    }
    if record.status.as_ref() != Some(&current.status)
        || record.checkpoint != current.selected_checkpoint
    {
        if record.started {
            return Err("Codex cleanup authority changed after intent".into());
        }
        record.first_safe_ms = None;
        record.status = Some(current.status.clone());
        record.checkpoint = current.selected_checkpoint.clone();
    }
    let found = located.ok_or("Codex cleanup intent ancestor absent")?;
    found.verify(state, &attempt.id)?;
    if let Some(prior) = &record.proof {
        if prior.parents != found.parents
            || found
                .directory
                .as_ref()
                .is_some_and(|id| id != &prior.directory)
        {
            return Err("Codex cleanup tree or ancestor replaced".into());
        }
    }
    if record.proof.is_none() {
        record.proof = Some(Proof {
            path: path(state, &attempt.id),
            parents: found.parents.clone(),
            directory: found
                .directory
                .clone()
                .ok_or("Codex cleanup unobserved tree absent")?,
        });
    }
    let due = add_ms(
        *record.first_safe_ms.get_or_insert(now),
        grace(policy, &current.status)?,
    )?;
    if now < due {
        record.reason = Reason::Grace;
        record.next_due_ms = Some(due);
        save(state, &record)?;
        return Ok(Some(due));
    }
    if !workspace.worktrees.iter().all(|s| s.done) {
        return hold(state, &mut record, Reason::SourceOrPath, now);
    }
    // A fresh canonical/local check immediately precedes durable intent and
    // destructive work. No await intervenes between this check and saved intent.
    let check = canonical(client, &receipt.command, state, policy.operation_timeout_ms).await;
    if !matches!(check, Ok(ref t) if t == &current)
        || !local_ready(config, state, &receipt, active).unwrap_or(false)
    {
        return hold(state, &mut record, Reason::CanonicalBinding, now);
    }
    found.verify(state, &attempt.id)?;
    if found.directory.is_none() && !record.started {
        return Err("Codex cleanup tree absent without intent".into());
    }
    record.started = true;
    record.reason = Reason::Filesystem;
    save(state, &record)?;
    if let Err(error) = copyout::compact(
        state,
        &found,
        &mut record,
        &control,
        config,
        client,
        active,
        &receipt,
        &current,
    )
    .await
    {
        eprintln!("BORG_CLEANUP codex-home retained: {error}");
        return hold(state, &mut record, Reason::Filesystem, now);
    }
    found.verify(state, &attempt.id)?;
    record.done = true;
    record.reason = Reason::Complete;
    record.next_due_ms = None;
    save(state, &record)?;
    Ok(None)
}

/// The existing copyout verifier, not a caller-supplied positive hash, produces
/// custody. Original transcripts and the copyout record remain unchanged.
pub(super) async fn read_custody(state:&Path,attempt:&Attempt,policy:&WorkerRetentionLimits,revision:&str,control:&Control)
    ->Result<crate::read_retention::TranscriptCustody,String> {
    let directory=sidecars(state)?;
    let record:Progress=read_record(&fd_path(&directory).join(format!("{}.json",component(&attempt.id))),policy.record_bytes)?.ok_or("verified transcript cleanup record missing")?;
    if !record.done || record.attempt_id!=attempt.id || record.policy!=*policy || record.policy_revision!=revision {
        return Err("transcript cleanup provenance differs".into());
    }
    copyout::verify_completed(state,&record,control).await?;
    let exports=record.exports.as_ref().ok_or("copyout exports absent")?;
    let (bytes,files)=copyout::custody_counts(exports)?;
    Ok(crate::read_retention::TranscriptCustody::CodingCopy {
        manifest_sha256:crate::read_retention::digest(&serde_json::to_vec(exports).map_err(|_|"copyout manifest encoding")?),bytes,files,
    })
}
