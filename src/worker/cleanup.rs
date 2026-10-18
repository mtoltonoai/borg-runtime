//! Automatic reclamation of disposable worker material; never receipt expiry.
use super::*;
use crate::limits::WorkerRetentionLimits;
use git::cleanup::{CacheProof, WorktreeProof};
use std::{
    collections::VecDeque,
    io::Read,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Disposition {
    Retained,
    Removing,
    Reclaimed,
    Uncertain,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Reason {
    Startup,
    Active,
    LocalRecovery,
    CanonicalUnavailable,
    CanonicalBinding,
    NotTerminal,
    NotStopped,
    UncertainEffect,
    Checkpoint,
    Grace,
    Clock,
    SourceOrPath,
    Filesystem,
    Complete,
    CacheDependency,
    CacheGrace,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Slot {
    proof: Option<WorktreeProof>,
    started: bool,
    done: bool,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
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
    disposition: Disposition,
    reason: Reason,
    worktrees: [Slot; 2],
    cache: Option<CacheProof>,
    cache_started: bool,
    cache_safe_ms: Option<u64>,
    cache_done: bool,
}

pub(super) fn read_record<T: serde::de::DeserializeOwned>(
    path: &Path,
    bound: usize,
) -> Result<Option<T>, String> {
    read_record_bytes(path, bound)?.map(|bytes| {
        serde_json::from_slice(&bytes).map_err(|_| "cleanup record invalid".into())
    }).transpose()
}

pub(super) fn read_record_bytes(path: &Path, bound: usize) -> Result<Option<Vec<u8>>, String> {
    let meta = match std::fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("cleanup record unavailable".into()),
    };
    if !meta.is_file()
        || meta.nlink() != 1
        || meta.uid()
            != std::fs::metadata("/proc/self")
                .map_err(|_| "cleanup owner unavailable")?
                .uid()
        || meta.len() > bound as u64
    {
        return Err("cleanup record identity or retention.worker.record_bytes invalid".into());
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(0x20000 | 0x800)
        .open(path)
        .map_err(|_| "cleanup record open failed")?;
    let opened = file.metadata().map_err(|_| "cleanup record stat failed")?;
    if opened.dev() != meta.dev() || opened.ino() != meta.ino() {
        return Err("cleanup record replaced".into());
    }
    let mut bytes = Vec::new();
    file.take(bound as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "cleanup record unreadable")?;
    if bytes.len() > bound {
        return Err("retention.worker.record_bytes".into());
    }
    Ok(Some(bytes))
}

fn save(path: &Path, record: &Record) -> Result<(), String> {
    bounded_json_size(record, record.policy.record_bytes)
        .map_err(|_| "retention.worker.record_bytes")?;
    atomic_json(path, record)
}
fn record_path(state: &Path, id: &str) -> PathBuf {
    state
        .join("cleanup")
        .join(format!("{}.json", component(id)))
}
fn add_ms(now: u64, duration: u64) -> Result<u64, String> {
    now.checked_add(duration)
        .ok_or_else(|| "cleanup time overflow".into())
}
// ManagedProcess retains a private home/code directory after verified stop.
// Only that exact owned directory is non-obligation material; every record or
// unknown entry remains a hold, including malformed process JSON.
fn process_obligation(path: &Path) -> Result<bool, String> {
    let meta = match std::fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(_) => return Err("cleanup process registry unavailable".into()),
    };
    let owner = std::fs::metadata("/proc/self")
        .map_err(|_| "cleanup process owner unavailable")?
        .uid();
    if !meta.is_dir() || meta.uid() != owner {
        return Err("cleanup process registry identity invalid".into());
    }
    for entry in std::fs::read_dir(path).map_err(|_| "cleanup process registry unreadable")? {
        let entry = entry.map_err(|_| "cleanup process registry unreadable")?;
        if entry.file_name() != "home" {
            return Ok(true);
        }
        let home = std::fs::symlink_metadata(entry.path())
            .map_err(|_| "cleanup process home unavailable")?;
        if !home.is_dir() || home.uid() != owner {
            return Err("cleanup process home identity invalid".into());
        }
    }
    Ok(false)
}
fn attempt_outbox(state: &Path, attempt: &Attempt, limits: &RuntimeLimits) -> Result<bool, String> {
    if acceptance::pending_for(&state.join("outbox"), attempt, limits)? {
        return Ok(true);
    }
    let mut count = 0;
    for e in std::fs::read_dir(state.join("outbox")).map_err(|_| "cleanup outbox unavailable")? {
        let path = e.map_err(|_| "cleanup outbox unreadable")?.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        count += 1;
        if count > limits.worker.outbox_records {
            return Err("worker.outbox_records".into());
        }
        let report = read_report(&path, limits)?;
        if report.task_id.is_empty()
            || report.attempt_id.is_empty()
            || report.worker_id.is_empty()
            || report.incarnation.is_empty()
        {
            return Err("cleanup outbox association invalid".into());
        }
        if report.task_id == attempt.task_id || report.attempt_id == attempt.id {
            return Ok(true);
        }
    }
    Ok(false)
}
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Cleanup MUST preserve data required by an active attempt.
fn local_ready(
    config: &WorkerConfig,
    state: &Path,
    receipt: &Receipt,
    active: &BTreeMap<String, Active>,
) -> Result<bool, String> {
    let WorkerCommand::RunAttempt { attempt, task, .. } = &receipt.command else {
        return Ok(false);
    };
    if attempt.worker_id != config.id
        || task.definition.executor != ExecutorKind::Coding
        || !receipt.completed
        || active.contains_key(&attempt.id)
    {
        return Ok(false);
    }
    if process_obligation(&state.join("processes").join(component(&attempt.id)))?
        || attempt_outbox(state, attempt, &config.limits)?
    {
        return Ok(false);
    }
    // Delegation journals belong to native decisions. A coding command with one
    // is an unsupported local recovery association, not disposable work.
    if std::fs::symlink_metadata(handoff::path(state, attempt)).is_ok() {
        return Ok(false);
    }
    if !codex::retry::cleanup_ready(&codex::state_path(&state.join("codex"),&attempt.id),
        &config.workspace_root.join("workspaces").join(component(&attempt.id)),
        attempt.deadline_unix_ms.min(task.limits.deadline_unix_ms),&config.limits.worker)? {
        return Ok(false);
    }
    coding_collaboration_closed(config, task, attempt, state)
}

//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# A workspace MUST become eligible for cleanup only after its canonical task is terminal and its bound attempt's execution descendants have stopped.
fn terminal(current: &Task, input: &TaskInput, attempt: &Attempt) -> Result<(), Reason> {
    if !coding_recovery_binding(current, input, attempt) {
        return Err(Reason::CanonicalBinding);
    }
    if !matches!(
        current.status,
        TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::Cancelled
    ) {
        return Err(Reason::NotTerminal);
    }
    let actual = current.attempt.as_ref().ok_or(Reason::CanonicalBinding)?;
    if !actual.stopped || actual.phase != AttemptPhase::Stopped {
        return Err(Reason::NotStopped);
    }
    if actual.uncertain {
        return Err(Reason::UncertainEffect);
    }
    Ok(())
}
fn acknowledged(record: &OperationRecord, checkpoint: &Checkpoint, attempt: &Attempt) -> bool {
    record.id == checkpoint.operation_id
        && record.task_id == attempt.task_id
        && record.attempt_id == attempt.id
        && record.generation == attempt.generation
        && record.kind == "checkpoint"
        && record.state == "settled"
        && record.intended
            == json!({"registration":checkpoint.registration,"repository":checkpoint.repository,"commit":checkpoint.commit,"remote_ref":checkpoint.remote_ref})
        && record
            .outcome
            .as_ref()
            .is_some_and(|v| v.get("acknowledged").and_then(|x| x.as_bool()) == Some(true))
}
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Cleanup MUST preserve data required to reconcile an uncertain or unacknowledged operation.
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Workspace cleanup MUST preserve source edits required to complete or reconcile an outstanding checkpoint publication.
async fn canonical(
    client: &ApiClient,
    command: &WorkerCommand,
    state: &Path,
    timeout_ms: u64,
) -> Result<Task, Reason> {
    let WorkerCommand::RunAttempt { task, attempt, .. } = command else {
        return Err(Reason::CanonicalBinding);
    };
    let reply = tokio::time::timeout(
        Duration::from_millis(timeout_ms),
        client.call(Operation::GetTask {
            task_id: attempt.task_id.clone(),
        }),
    )
    .await
    .map_err(|_| Reason::CanonicalUnavailable)?
    .map_err(|_| Reason::CanonicalUnavailable)?;
    let Outcome::Ok {
        data: Data::Task(current),
    } = reply.outcome
    else {
        return Err(Reason::CanonicalUnavailable);
    };
    terminal(&current, task, attempt)?;
    if !codex::retry::cleanup_verified(client,&codex::state_path(&state.join("codex"),&attempt.id),task,attempt,timeout_ms)
        .await.map_err(|_|Reason::CanonicalUnavailable)? {return Err(Reason::LocalRecovery);}
    if !pull_request::cleanup_ready(state, command).map_err(|_| Reason::LocalRecovery)?
        || !pull_request::cleanup_verified(client, state, command).await.map_err(|_| Reason::CanonicalUnavailable)? {
        return Err(Reason::LocalRecovery);
    }
    let phase = publication::read(&publication::path(state, attempt), command)
        .map_err(|_| Reason::LocalRecovery)?;
    let publication_checkpoint = phase
        .as_ref()
        .map(|p| p.cleanup_checkpoint())
        .transpose()
        .map_err(|_| Reason::Checkpoint)?;
    let selected = if current.status == TaskStatus::Succeeded {
        let checkpoint = current
            .selected_checkpoint
            .as_ref()
            .ok_or(Reason::Checkpoint)?;
        if !current.result.as_ref().is_some_and(|r| {
            r.attempt_id == attempt.id && r.checkpoint.as_ref() == Some(checkpoint)
        }) || publication_checkpoint != Some(checkpoint)
        {
            return Err(Reason::Checkpoint);
        }
        Some(checkpoint)
    } else {
        publication_checkpoint
    };
    if let Some(checkpoint) = selected {
        let reply = tokio::time::timeout(
            Duration::from_millis(timeout_ms),
            client.call(Operation::GetOperation {
                operation_id: checkpoint.operation_id.clone(),
            }),
        )
        .await
        .map_err(|_| Reason::CanonicalUnavailable)?
        .map_err(|_| Reason::CanonicalUnavailable)?;
        if !matches!(reply.outcome, Outcome::Ok { data: Data::Operation(ref record) } if acknowledged(record, checkpoint, attempt))
        {
            return Err(Reason::Checkpoint);
        }
    }
    Ok(*current)
}

pub(super) struct Scheduler {
    policy: Option<WorkerRetentionLimits>,
    policy_revision: String,
    scan: Option<std::fs::ReadDir>,
    scanned: usize,
    rescan: bool,
    retry_turn: bool,
    inventory_retry_ms: Option<u64>,
    ready: VecDeque<PathBuf>,
    due: BTreeMap<PathBuf, u64>,
    pending_path: Option<PathBuf>,
}
impl Scheduler {
    #[cfg(test)]
    pub(super) fn open(config: &WorkerConfig, state: &Path) -> Result<Self, String> {
        Self::open_selected(config, state, &config.limits)
    }
    //= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
    //# Automatic cleanup MUST use per-kind retention settings from worker metadata.
    pub(super) fn open_selected(
        _config: &WorkerConfig,
        state: &Path,
        selected: &RuntimeLimits,
    ) -> Result<Self, String> {
        let policy = selected.retention.as_ref().map(|v| v.worker.clone());
        if policy.is_some() {
            process::private_directory(&state.join("cleanup"))?;
        }
        Ok(Self {
            policy,
            policy_revision: selected.revision.clone(),
            scan: None,
            scanned: 0,
            rescan: true,
            retry_turn: false,
            inventory_retry_ms: None,
            ready: VecDeque::new(),
            due: BTreeMap::new(),
            pending_path: None,
        })
    }
    // A command/completion may cancel a cleanup future. Its process registry
    // and removal intent remain durable. Reap that exact ownership before a new
    // RunAttempt can use the repository cache, and before cleanup resumes.
    //= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
    //# Restart MUST reconcile incomplete cleanup before treating its resources as reclaimed.
    pub(super) async fn reconcile_interrupted(
        &mut self,
        config: &WorkerConfig,
        state: &Path,
    ) -> Result<(), String> {
        let Some(path) = self.pending_path.as_ref() else {
            return Ok(());
        };
        let receipt = read_receipt(path, &config.limits)?;
        let WorkerCommand::RunAttempt {
            command_id,
            attempt,
            task,
            ..
        } = &receipt.command
        else {
            // Valid Stop receipts never enter an owned cleanup operation.
            self.pending_path = None;
            return Ok(());
        };
        if task.definition.executor != ExecutorKind::Coding {
            self.pending_path = None;
            return Ok(());
        }
        if attempt.worker_id != config.id
            || *path
                != state
                    .join("commands")
                    .join(format!("{}.json", component(command_id)))
        {
            return Err("cleanup interrupted command binding differs".into());
        }
        let registry = state
            .join("processes")
            .join(format!("cleanup-{}", component(&attempt.id)));
        process::reconcile_processes(&registry, &config.limits.worker).await?;
        if process_obligation(&registry)? {
            return Err("cleanup interrupted process obligation remains".into());
        }
        self.pending_path = None;
        Ok(())
    }
    pub(super) fn notify(&mut self) {
        self.rescan = true;
    }
    pub(super) async fn wait(deadline: Option<u64>) {
        match deadline {
            Some(deadline) => {
                tokio::time::sleep(Duration::from_millis(
                    deadline.saturating_sub(process::now_ms()),
                ))
                .await
            }
            None => std::future::pending::<()>().await,
        }
    }
    pub(super) fn deadline(&self) -> Option<u64> {
        self.policy.as_ref()?;
        if self.rescan || self.scan.is_some() || !self.ready.is_empty() {
            return Some(process::now_ms());
        }
        self.due
            .values()
            .copied()
            .chain(self.inventory_retry_ms)
            .min()
    }
    fn next_path(&mut self, now: u64, inventory_bound: usize, retry_ms: u64) -> Option<PathBuf> {
        let due = self
            .due
            .iter()
            .filter(|(_, at)| **at <= now)
            .min_by_key(|(path, at)| (**at, *path))
            .map(|(p, _)| p.clone());
        let retry = self.retry_turn || self.scan.is_none();
        self.retry_turn = !self.retry_turn;
        if retry {
            if let Some(path) = due.clone() {
                self.due.remove(&path);
                return Some(path);
            }
        }
        if let Some(scan) = self.scan.as_mut() {
            match scan.next() {
                Some(Ok(entry)) => {
                    self.scanned += 1;
                    if self.scanned > inventory_bound {
                        self.scan = None;
                        self.inventory_retry_ms = now.checked_add(retry_ms);
                        eprintln!("BORG_CLEANUP retained worker.command_records");
                    } else {
                        let path = entry.path();
                        if path.extension().and_then(|s| s.to_str()) == Some("json") {
                            return Some(path);
                        }
                    }
                }
                Some(Err(_)) => {
                    self.scan = None;
                    self.inventory_retry_ms = now.checked_add(retry_ms);
                }
                None => self.scan = None,
            }
        }
        if let Some(path) = due {
            self.due.remove(&path);
            return Some(path);
        }
        self.ready.pop_front()
    }
    //= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
    //# A worker MUST reclaim eligible disposable workspaces and local repository caches without an operator command.
    //= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
    //# Cleanup MUST use bounded work under its configured metadata policy.
    pub(super) async fn step(
        &mut self,
        config: &WorkerConfig,
        state: &Path,
        client: &ApiClient,
        active: &BTreeMap<String, Active>,
        validation_runtime: &Arc<crate::native::ValidationRuntime>,
    ) {
        let Some(policy) = self.policy.clone() else {
            return;
        };
        if self.reconcile_interrupted(config, state).await.is_err() {
            eprintln!("BORG_CLEANUP retained interrupted_process");
            return;
        }
        let now = process::now_ms();
        if self.inventory_retry_ms.is_some_and(|at| at <= now) {
            self.inventory_retry_ms = None;
            self.rescan = true;
        }
        if self.scan.is_none() && self.rescan {
            self.rescan = false;
            self.scanned = 0;
            match std::fs::read_dir(state.join("commands")) {
                Ok(scan) => self.scan = Some(scan),
                Err(_) => {
                    self.inventory_retry_ms = now.checked_add(policy.retry_delay_ms);
                    eprintln!("BORG_CLEANUP retained command_inventory");
                }
            }
        }
        for _ in 0..policy.batch_entries {
            let Some(path) = self.next_path(
                now,
                config.limits.worker.command_records,
                policy.retry_delay_ms,
            ) else {
                continue;
            };
            // Installed before the first await: dropping this step cannot lose
            // the selected inventory entry or its owned process recovery.
            self.due
                .insert(path.clone(), now.saturating_add(policy.retry_delay_ms));
            self.pending_path = Some(path.clone());
            let outcome = attempt(
                config,
                state,
                client,
                active,
                validation_runtime,
                &policy,
                &self.policy_revision,
                &path,
            )
            .await;
            if self.reconcile_interrupted(config, state).await.is_err() {
                eprintln!("BORG_CLEANUP retained interrupted_process");
                return;
            }
            match outcome {
                Ok(Some(at)) => {
                    self.due.insert(path, at);
                }
                Ok(None) => {
                    self.due.remove(&path);
                }
                Err(_) => {
                    eprintln!("BORG_CLEANUP retained invalid_local_record"); /* no invalid record overwrite */
                }
            }
        }
        tokio::task::yield_now().await;
    }
}

//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# A cleanup outcome MUST distinguish reclaimed, retained and uncertain resources.
fn retained_disposition(record: &Record) -> Disposition {
    if record.worktrees.iter().any(|s| s.started && !s.done)
        || record.cache_started && !record.cache_done
    {
        Disposition::Uncertain
    } else {
        Disposition::Retained
    }
}

fn retain(record: &mut Record, reason: Reason, now: u64) -> Result<(), String> {
    record.reason = reason;
    record.disposition = retained_disposition(record);
    if matches!(
        reason,
        Reason::CanonicalBinding
            | Reason::NotTerminal
            | Reason::NotStopped
            | Reason::UncertainEffect
            | Reason::LocalRecovery
            | Reason::Checkpoint
            | Reason::Active
            | Reason::SourceOrPath
    ) {
        record.first_safe_ms = None;
    }
    record.next_due_ms = Some(add_ms(now, record.policy.retry_delay_ms)?);
    Ok(())
}
// These exclusions describe this local deletion guarantee, not an inventory
// asserting that replicas, exports or backups exist. No retained record changes.
fn result_line(record: &Record, worktree: Option<&str>) -> String {
    let (result, cache) = match worktree {
        Some(kind) => (
            format!("BORG_CLEANUP reclaimed worktree kind={kind} attempt={}", record.attempt_id),
            "outside-this-worktree-result",
        ),
        None => (
            format!("BORG_CLEANUP {:?} {:?} attempt={}", record.disposition, record.reason, record.attempt_id),
            if record.cache_done { "cleanup-complete" } else { "cleanup-incomplete" },
        ),
    };
    format!(
        "{result} scope=owned-disposable-worker-copies repository-cache={cache} \
         cache-policy=dependency-holds-and-grace \
         excluded=durable-task-results,history,checkpoints,audit-metadata,operation-receipts,recovery-journals,worker-state-and-logs-outside-disposable-paths,other-worktrees,other-repository-caches \
         excluded-copies-if-present=database-pages,replicas,exports,backups \
         cryptographic-erasure=not-guaranteed"
    )
}

fn emit(record: &Record) {
    eprintln!("{}", result_line(record, None));
}

async fn attempt(
    config: &WorkerConfig,
    state: &Path,
    client: &ApiClient,
    active: &BTreeMap<String, Active>,
    validation_runtime: &Arc<crate::native::ValidationRuntime>,
    policy: &WorkerRetentionLimits,
    policy_revision: &str,
    path: &Path,
) -> Result<Option<u64>, String> {
    attempt_at(
        config,
        state,
        client,
        active,
        validation_runtime,
        policy,
        policy_revision,
        path,
        process::now_ms(),
    )
    .await
}
#[path = "cleanup_plugins.rs"]
mod plugins;
#[path = "cleanup_reads.rs"]
mod reads;

async fn attempt_at(
    config: &WorkerConfig, state: &Path, client: &ApiClient,
    active: &BTreeMap<String, Active>,
    validation_runtime: &Arc<crate::native::ValidationRuntime>,
    policy: &WorkerRetentionLimits, policy_revision: &str, path: &Path, now: u64,
) -> Result<Option<u64>, String> {
    let workspace_due = workspace_attempt_at(config, state, client, active, validation_runtime,
        policy, policy_revision, path, now).await?;
    // Plugin progress is independent of repository-cache grace and of the old
    // worktree record's Reclaimed shortcut. The sidecar repeats local/canonical
    // eligibility before any removal; old format-1 records remain unchanged.
    let plugin_due = plugins::attempt_at(config, state, client, active, validation_runtime,
        policy, policy_revision, path, now).await?;
    let read_due=reads::attempt_at(config,state,client,active,validation_runtime,policy,policy_revision,path,now).await?;
    Ok([workspace_due,plugin_due,read_due].into_iter().flatten().min())
}

async fn workspace_attempt_at(
    config: &WorkerConfig,
    state: &Path,
    client: &ApiClient,
    active: &BTreeMap<String, Active>,
    validation_runtime: &Arc<crate::native::ValidationRuntime>,
    policy: &WorkerRetentionLimits,
    policy_revision: &str,
    path: &Path,
    now: u64,
) -> Result<Option<u64>, String> {
    let receipt = read_receipt(path, &config.limits)?;
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
    if path
        != state
            .join("commands")
            .join(format!("{}.json", component(command_id)))
    {
        return Err("cleanup command path mismatch".into());
    }
    let _: Receipt = read_record(path, config.limits.worker.command_record_bytes)?
        .ok_or("cleanup command disappeared")?;
    let file = record_path(state, &attempt.id);
    let mut record = read_record::<Record>(&file, policy.record_bytes)?.unwrap_or_else(|| Record {
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
        disposition: Disposition::Retained,
        reason: Reason::Startup,
        worktrees: std::array::from_fn(|_| Slot {
            proof: None,
            started: false,
            done: false,
        }),
        cache: None,
        cache_started: false,
        cache_safe_ms: None,
        cache_done: false,
    });
    if record.format_revision != 1
        || record.command_id != *command_id
        || record.attempt_id != attempt.id
        || (record.policy != *policy || record.policy_revision != policy_revision)
        || record
            .worktrees
            .iter()
            .any(|s| s.started && s.proof.is_none())
        || record.cache_started && record.cache.is_none()
    {
        return Err("cleanup retained binding invalid".into());
    }
    if record.disposition == Disposition::Reclaimed {
        return Ok(None);
    }
    if now < record.last_observed_ms {
        retain(&mut record, Reason::Clock, now)?;
        save(&file, &record)?;
        emit(&record);
        return Ok(record.next_due_ms);
    }
    record.last_observed_ms = now;
    // A newly observed local obligation, including an unreadable recovery
    // proof, interrupts cache inactivity just as a shared dependency does.
    // The command and retained cleanup binding have already been validated.
    //= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
    //# Cache cleanup MUST preserve a cache while a worktree, accepted task or unresolved recovery record depends on that local cache.
    if !local_ready(config, state, &receipt, active).unwrap_or(false) {
        record.cache_safe_ms = None;
        retain(&mut record, Reason::LocalRecovery, now)?;
        save(&file, &record)?;
        emit(&record);
        return Ok(record.next_due_ms);
    }
    let current = if record.worktrees.iter().all(|slot| slot.done) {
        None
    } else {
        match canonical(client, &receipt.command, state, policy.operation_timeout_ms).await {
            Ok(task) => Some(task),
            Err(reason) => {
                retain(&mut record, reason, now)?;
                save(&file, &record)?;
                emit(&record);
                return Ok(record.next_due_ms);
            }
        }
    };
    let (_sender, cancel) = watch::channel(false);
    let mut cleanup_limits = (*config.limits).clone();
    cleanup_limits.retention = Some(crate::limits::RetentionLimits {
        worker: policy.clone(),
    });
    let control = Control {
        deadline_ms: add_ms(process::now_ms(), policy.operation_timeout_ms)?,
        worker_limits: Arc::new(config.limits.worker.clone()),
        runtime_limits: Arc::new(cleanup_limits),
        validation_runtime: validation_runtime.clone(),
        cancel,
    };
    let registry = state
        .join("processes")
        .join(format!("cleanup-{}", component(&attempt.id)));
    process::private_directory(&registry)?;
    process::reconcile_processes(&registry, &config.limits.worker).await?;
    let repository = task
        .repository
        .as_ref()
        .ok_or("cleanup repository absent")?;
    let cache_root = config.workspace_root.join("repositories");
    if let Some(current) = &current {
        if record.status.as_ref() != Some(&current.status)
            || record.checkpoint != current.selected_checkpoint
        {
            if record.worktrees.iter().any(|s| s.started) || record.cache_started {
                return Err("cleanup authority changed after removal intent".into());
            }
            record.first_safe_ms = None;
            record.status = Some(current.status.clone());
            record.checkpoint = current.selected_checkpoint.clone();
        }
        // First-safe time begins only once both exact owned worktree/cache
        // associations and the canonical/local recovery conditions are valid.
        for (index, kind) in ["workspaces", "validation"].iter().enumerate() {
            if record.worktrees[index].done || record.worktrees[index].started {
                continue;
            }
            let expected = current
                .selected_checkpoint
                .as_ref()
                .map_or(repository.start_commit.as_str(), |cp| cp.commit.as_str());
            let fresh = match git::cleanup::prepare_worktree(
                &cache_root,
                &config.workspace_root.join(kind),
                &attempt.id,
                repository,
                expected,
                current.status != TaskStatus::Succeeded,
                &registry,
                &control,
            )
            .await
            {
                Ok(proof) => proof,
                Err(_) => {
                    retain(&mut record, Reason::SourceOrPath, now)?;
                    save(&file, &record)?;
                    emit(&record);
                    return Ok(record.next_due_ms);
                }
            };
            if record.worktrees[index].proof != fresh {
                record.first_safe_ms = None;
            }
            record.worktrees[index].proof = fresh;
        }
        let safe = *record.first_safe_ms.get_or_insert(now);
        let grace = grace(policy, &current.status)?;
        let due = add_ms(safe, grace)?;
        if now < due {
            record.reason = Reason::Grace;
            record.next_due_ms = Some(due);
            save(&file, &record)?;
            emit(&record);
            return Ok(Some(due));
        }
    }
    save(&file, &record)?; // durable first-safe observation before any disposal
    let expected = record
        .checkpoint
        .as_ref()
        .map_or(repository.start_commit.as_str(), |c| c.commit.as_str())
        .to_owned();
    let discard_source = record.status.as_ref() != Some(&TaskStatus::Succeeded);
    //= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
    //# Cleanup MUST preserve durable task results, histories, checkpoints, audit metadata, operation receipts and recovery journals in the initial profile.
    for (index, kind) in ["workspaces", "validation"].iter().enumerate() {
        if record.worktrees[index].done {
            continue;
        }
        let root = config.workspace_root.join(kind);
        if record.worktrees[index].proof.is_none() {
            match git::cleanup::prepare_worktree(
                &cache_root,
                &root,
                &attempt.id,
                repository,
                &expected,
                discard_source,
                &registry,
                &control,
            )
            .await
            {
                Ok(proof) => record.worktrees[index].proof = proof,
                Err(_) => {
                    retain(&mut record, Reason::SourceOrPath, now)?;
                    save(&file, &record)?;
                    emit(&record);
                    return Ok(record.next_due_ms);
                }
            }
        }
        if let Some(proof) = record.worktrees[index].proof.clone() {
            // Re-read canonical authority immediately before each destructive
            // step; an intervening retry/current-attempt change invalidates it.
            if !canonical(client, &receipt.command, state, policy.operation_timeout_ms)
                .await
                .as_ref()
                .ok()
                .is_some_and(|fresh| {
                    current.as_ref().is_some_and(|original| {
                        fresh.status == original.status
                            && fresh.selected_checkpoint == original.selected_checkpoint
                            && fresh.result == original.result
                    })
                })
            {
                retain(&mut record, Reason::CanonicalBinding, now)?;
                save(&file, &record)?;
                return Ok(record.next_due_ms);
            }
            let resuming = record.worktrees[index].started;
            record.worktrees[index].started = true;
            record.disposition = Disposition::Removing;
            save(&file, &record)?;
            if git::cleanup::remove_worktree(
                &proof,
                &cache_root,
                &root,
                &attempt.id,
                repository,
                &registry,
                &control,
                resuming,
            )
            .await
            .is_err()
            {
                retain(&mut record, Reason::Filesystem, now)?;
                save(&file, &record)?;
                emit(&record);
                return Ok(record.next_due_ms);
            }
        }
        record.worktrees[index].done = true;
        save(&file, &record)?;
        eprintln!("{}", result_line(&record, Some(kind)));
    }
    // Unknown canonical/journal dependencies cannot preserve an earlier safe
    // interval. The current cleanup record is valid; leave the dependency's
    // bytes untouched and durably record this cache hold before retrying.
    //= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
    //# Cache cleanup MUST preserve a cache while a worktree, accepted task or unresolved recovery record depends on that local cache.
    let unreferenced = cache_unreferenced(config, state, client, repository, active, &control, policy)
        .await
        .unwrap_or(false);
    if !unreferenced {
        record.cache_safe_ms = None;
        retain(&mut record, Reason::CacheDependency, now)?;
        save(&file, &record)?;
        emit(&record);
        return Ok(record.next_due_ms);
    }
    if record.cache_started
        && !git::cleanup::has_removal_barrier(&cache_root, repository, &control)?
    {
        // Intent persisted before the reuse barrier: no deletion was authorized
        // yet. Revalidate current cache and restart idle observation. If it was
        // fully removed before the result saved, absence remains idempotent.
        record.cache_started = false;
        record.cache_safe_ms = None;
    }
    if !record.cache_started {
        // A different attempt may have used this cache during its idle grace.
        // Re-read exact last use and identity before recording removal intent.
        match git::cleanup::prepare_cache_removal(&cache_root, repository, &registry, &control)
            .await
        {
            Ok(proof) => {
                if record.cache != proof {
                    record.cache_safe_ms = None;
                }
                record.cache = proof;
            }
            Err(_) => {
                // Registered or unverified Git dependencies also break the
                // idle interval, including those absent from command inventory.
                record.cache_safe_ms = None;
                retain(&mut record, Reason::CacheDependency, now)?;
                save(&file, &record)?;
                emit(&record);
                return Ok(record.next_due_ms);
            }
        }
    }
    if let Some(proof) = record.cache.clone() {
        let safe = *record.cache_safe_ms.get_or_insert(now);
        let due = add_ms(proof.last_use_ms.max(safe), policy.idle_cache_grace_ms)?;
        if now < due {
            record.disposition = retained_disposition(&record);
            record.reason = Reason::CacheGrace;
            record.next_due_ms = Some(due);
            save(&file, &record)?;
            emit(&record);
            return Ok(Some(due));
        }
        let resuming = record.cache_started;
        record.cache_started = true;
        record.disposition = Disposition::Removing;
        save(&file, &record)?;
        if git::cleanup::remove_cache(
            &proof,
            &cache_root,
            repository,
            &registry,
            &control,
            policy.filesystem_entries_per_yield,
            resuming,
        )
        .await
        .is_err()
        {
            retain(&mut record, Reason::Filesystem, now)?;
            save(&file, &record)?;
            emit(&record);
            return Ok(record.next_due_ms);
        }
    }
    //= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
    //# A cleanup outcome MUST distinguish reclaimed, retained and uncertain resources.
    record.cache_done = true;
    record.disposition = Disposition::Reclaimed;
    record.reason = Reason::Complete;
    record.next_due_ms = None;
    save(&file, &record)?;
    emit(&record);
    Ok(None)
}

//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Cache cleanup MUST preserve a cache while a worktree, accepted task or unresolved recovery record depends on that local cache.
async fn cache_unreferenced(
    config: &WorkerConfig,
    state: &Path,
    client: &ApiClient,
    repository: &Repository,
    active: &BTreeMap<String, Active>,
    control: &Control,
    policy: &WorkerRetentionLimits,
) -> Result<bool, String> {
    let selected = git::cleanup::key(repository, control)?;
    let mut n = 0;
    for entry in std::fs::read_dir(state.join("commands"))
        .map_err(|_| "cleanup command inventory unavailable")?
    {
        control.check()?;
        n += 1;
        if n > config.limits.worker.command_records {
            return Err("worker.command_records".into());
        }
        let path = entry
            .map_err(|_| "cleanup command inventory invalid")?
            .path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let receipt = read_receipt(&path, &config.limits)?;
        if let WorkerCommand::RunAttempt { task, attempt, .. } = &receipt.command {
            if let Some(repo) = task.repository.as_ref() {
                if task.definition.executor == ExecutorKind::Coding
                    && git::cleanup::key(repo, control)? == selected
                {
                    if !local_ready(config, state, &receipt, active)? {
                        return Ok(false);
                    }
                    if !historical_stopped(client, task, attempt, &receipt.command, state, control)
                        .await?
                    {
                        return Ok(false);
                    }
                    for kind in ["workspaces", "validation"] {
                        if std::fs::symlink_metadata(
                            config
                                .workspace_root
                                .join(kind)
                                .join(component(&attempt.id)),
                        )
                        .is_ok()
                        {
                            return Ok(false);
                        }
                    }
                }
            }
        }
        if n % policy.batch_entries == 0 {
            tokio::task::yield_now().await;
        }
    }
    Ok(true)
}

fn grace(policy: &WorkerRetentionLimits, status: &TaskStatus) -> Result<u64, String> {
    match status {
        TaskStatus::Succeeded => Ok(policy.success_grace_ms),
        TaskStatus::Failed => Ok(policy.failed_grace_ms),
        TaskStatus::Cancelled => Ok(policy.cancelled_grace_ms),
        _ => Err("cleanup nonterminal grace refused".into()),
    }
}
async fn controlled_call(
    client: &ApiClient,
    operation: Operation,
    control: &Control,
) -> Result<Reply, String> {
    control.check()?;
    tokio::time::timeout(
        Duration::from_millis(control.deadline_ms.saturating_sub(process::now_ms())),
        client.call(operation),
    )
    .await
    .map_err(|_| "cleanup controller lookup deadline")?
}
async fn historical_stopped(
    client: &ApiClient,
    input: &TaskInput,
    original: &Attempt,
    command: &WorkerCommand,
    state: &Path,
    control: &Control,
) -> Result<bool, String> {
    use crate::collaboration::{Query, QueryResult};
    if !pull_request::cleanup_ready(state, command)? || !pull_request::cleanup_verified(client, state, command).await? {
        return Ok(false);
    }
    let phase = publication::read(&publication::path(state, original), command)?;
    if let Some(phase) = phase {
        let checkpoint = phase.cleanup_checkpoint()?;
        let reply = controlled_call(
            client,
            Operation::GetOperation {
                operation_id: checkpoint.operation_id.clone(),
            },
            control,
        )
        .await?;
        if !matches!(reply.outcome, Outcome::Ok { data: Data::Operation(ref record) } if acknowledged(record, checkpoint, original))
        {
            return Ok(false);
        }
    }
    let reply = controlled_call(
        client,
        Operation::GetTask {
            task_id: original.task_id.clone(),
        },
        control,
    )
    .await?;
    let Outcome::Ok {
        data: Data::Task(current),
    } = reply.outcome
    else {
        return Ok(false);
    };
    if current.input != *input
        || current.session_id != original.session_id
        || current.assigned_agent_id != input.definition.id
    {
        return Ok(false);
    }
    let mut after = None;
    // This is only the absence of a LIVE LOCAL cache dependency. It grants no
    // authority to delete a historical worktree whose Task selected a retry.
    for _ in 0..control.worker_limits.command_records {
        control.check()?;
        let reply = controlled_call(
            client,
            Operation::QueryCollaboration {
                query: Query::TaskAttempts {
                    task_id: original.task_id.clone(),
                    after: after.clone(),
                    limit: control.runtime_limits.controller.collaboration_page_items,
                },
            },
            control,
        )
        .await?;
        let Outcome::Ok {
            data: Data::Collaboration(QueryResult::TaskAttempts(page)),
        } = reply.outcome
        else {
            return Ok(false);
        };
        for item in page.items {
            let a = item.attempt;
            if a.id == original.id {
                return Ok(a.task_id == original.task_id
                    && a.session_id == original.session_id
                    && a.generation == original.generation
                    && a.worker_id == original.worker_id
                    && a.worker_incarnation == original.worker_incarnation
                    && a.deadline_unix_ms == original.deadline_unix_ms
                    && a.phase == AttemptPhase::Stopped
                    && a.stopped
                    && !a.uncertain);
            }
        }
        if page.next_cursor.is_none() {
            return Ok(false);
        }
        if page.next_cursor == after {
            return Err("cleanup attempt cursor did not advance".into());
        }
        after = page.next_cursor;
        tokio::task::yield_now().await;
    }
    Err("cleanup historical attempt scan exceeds worker.command_records".into())
}

#[cfg(test)]
#[path = "cleanup_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "cleanup_controller_tests.rs"]
mod controller_tests;

// Test-only copied-record decoder; it never invokes the cleanup scheduler,
// canonical API, workspace inspection, mutation, persistence or removal.
#[cfg(test)]
pub(super) fn validate_retained_cleanup_copy(
    path: &Path,
    policy: &WorkerRetentionLimits,
    revision: &str,
    command_id: &str,
    attempt_id: &str,
) -> Result<serde_json::Value, String> {
    let record=read_record::<Record>(path,policy.record_bytes)?.ok_or("copied cleanup record missing")?;
    if record.format_revision!=1 || record.command_id!=command_id || record.attempt_id!=attempt_id
        || record.policy!=*policy || record.policy_revision!=revision
        || record.worktrees.iter().any(|s|s.started && s.proof.is_none())
        || record.cache_started && record.cache.is_none() {
        return Err("copied cleanup record binding differs".into());
    }
    serde_json::to_value(record).map_err(|_|"copied cleanup serialization failed".into())
}
