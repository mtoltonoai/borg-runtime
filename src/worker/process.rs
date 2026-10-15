//! Owned Linux process groups; no claim of hostile-process containment.
use crate::limits::WorkerLimits;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{Child, ChildStdin, Command},
    sync::watch,
};

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
pub fn component(s: &str) -> String {
    s.as_bytes().iter().map(|b| format!("{b:02x}")).collect()
}

/// Establish a private directory owned by this process's OS account. Call only
/// for directories allocated to this worker, never an operator's personal home.
pub fn private_directory(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)
        .map_err(|e| e.to_string())?;
    let metadata = std::fs::symlink_metadata(path).map_err(|e| e.to_string())?;
    let uid = std::fs::metadata("/proc/self")
        .map_err(|e| e.to_string())?
        .uid();
    if !metadata.file_type().is_dir() || metadata.uid() != uid {
        return Err("worker private directory has unexpected ownership or type".into());
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
        .map_err(|e| e.to_string())
}

pub fn atomic_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let bytes = serde_json::to_vec(value).map_err(|e| e.to_string())?;
    let temp = path.with_extension("pending");
    #[cfg(test)]
    storage_tests::cut(path, storage_tests::Point::Create, None, &bytes)?;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&temp)
        .map_err(|e| e.to_string())?;
    #[cfg(test)]
    storage_tests::cut(path, storage_tests::Point::Write, Some(&mut file), &bytes)?;
    file.write_all(&bytes).map_err(|e| e.to_string())?;
    #[cfg(test)]
    storage_tests::cut(path, storage_tests::Point::FileSync, None, &bytes)?;
    file.sync_all().map_err(|e| e.to_string())?;
    #[cfg(test)]
    storage_tests::cut(path, storage_tests::Point::Rename, None, &bytes)?;
    std::fs::rename(temp, path).map_err(|e| e.to_string())?;
    #[cfg(test)]
    storage_tests::cut(path, storage_tests::Point::DirectorySync, None, &bytes)?;
    std::fs::File::open(path.parent().ok_or("missing parent")?)
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())
}

#[derive(Clone)]
pub struct Control {
    pub deadline_ms: u64,
    pub worker_limits: Arc<WorkerLimits>,
    pub runtime_limits: Arc<crate::limits::RuntimeLimits>,
    pub validation_runtime: Arc<crate::native::ValidationRuntime>,
    pub cancel: watch::Receiver<bool>,
}
impl Control {
    pub fn check(&self) -> Result<(), String> {
        if *self.cancel.borrow() {
            Err("attempt cancelled".into())
        } else if now_ms() >= self.deadline_ms {
            Err("attempt deadline reached".into())
        } else {
            Ok(())
        }
    }
    //= spec/impl/v2/implementation.md#workers-are-trusted-disposable-environments
    //# A worker MUST enforce the attempt's execution deadline.
    pub async fn stopped(&mut self) {
        if self.check().is_err() {
            return;
        }
        tokio::select! { _=tokio::time::sleep(Duration::from_millis(self.deadline_ms.saturating_sub(now_ms())))=>{}, _=self.cancel.changed()=>{} }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProcessContext {
    boot_id: String,
    pid_namespace: String,
}
impl ProcessContext {
    pub(crate) fn current() -> Result<Self, String> {
        let boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .map_err(|_| "process boot identity unavailable")?
            .trim()
            .to_owned();
        if boot_id.len() != 36
            || !boot_id.bytes().enumerate().all(|(i, c)| {
                if [8, 13, 18, 23].contains(&i) {
                    c == b'-'
                } else {
                    c.is_ascii_hexdigit()
                }
            })
        {
            return Err("invalid process boot identity".into());
        }
        let pid_namespace = std::fs::read_link("/proc/self/ns/pid")
            .map_err(|_| "process namespace identity unavailable")?
            .to_string_lossy()
            .into_owned();
        if pid_namespace
            .strip_prefix("pid:[")
            .and_then(|s| s.strip_suffix(']'))
            .and_then(|s| s.parse::<u64>().ok())
            .is_none()
        {
            return Err("invalid process namespace identity".into());
        }
        Ok(Self {
            boot_id,
            pid_namespace,
        })
    }
    pub(crate) fn verify(&self) -> Result<(), String> {
        if *self != Self::current()? {
            return Err("process boot or namespace differs; cleanup unverified".into());
        }
        Ok(())
    }
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessRecord {
    pid: u32,
    start_ticks: u64,
    process_group: u32,
    context: ProcessContext,
    descendants: Vec<OwnedPid>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnedPid {
    pid: u32,
    start_ticks: u64,
}
#[derive(Clone, Copy)]
struct Identity {
    start: u64,
    group: u32,
    parent: u32,
    state: char,
}
fn parse_identity(text: &str) -> Result<Identity, &'static str> {
    let tail = text.rsplit_once(") ").ok_or("stat_frame")?.1;
    let fields: Vec<_> = tail.split_whitespace().collect();
    let state = fields
        .first()
        .filter(|s| s.len() == 1)
        .and_then(|s| s.chars().next())
        .ok_or("state")?;
    let parent = fields.get(1).and_then(|s| s.parse().ok()).ok_or("parent")?;
    let group = fields
        .get(2)
        .and_then(|s| s.parse().ok())
        .ok_or("process_group")?;
    let start = fields
        .get(19)
        .and_then(|s| s.parse().ok())
        .ok_or("start_ticks")?;
    Ok(Identity {
        start,
        group,
        parent,
        state,
    })
}
fn identity(pid: u32, limits: &WorkerLimits) -> Result<Option<Identity>, String> {
    inspect_identity(pid, limits, || {
        std::fs::read_to_string(format!("/proc/{pid}/stat"))
    })
}
fn inspect_identity(
    pid: u32,
    limits: &WorkerLimits,
    mut read: impl FnMut() -> std::io::Result<String>,
) -> Result<Option<Identity>, String> {
    let mut field = "stat_frame";
    for attempt in 0..limits.identity_read_attempts {
        match read() {
            Ok(text) => match parse_identity(&text) {
                Ok(identity) => return Ok(Some(identity)),
                Err(invalid) => field = invalid,
            },
            // Linux procfs can return ESRCH after open when the task exits.
            // Both outcomes prove absence; permission/I/O failures do not.
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    || error.raw_os_error() == Some(3) =>
            {
                return Ok(None);
            }
            Err(error) => {
                return Err(format!(
                    "process pid={pid} inspection failed: errno={:?} kind={:?}; cleanup unverified",
                    error.raw_os_error(),
                    error.kind()
                ));
            }
        }
        // A task can finish while procfs constructs its snapshot. Do not accept
        // a malformed snapshot as absence: require a valid re-read or absence.
        if attempt + 1 < limits.identity_read_attempts {
            std::thread::sleep(Duration::from_micros(limits.identity_retry_us));
        }
    }
    Err(format!(
        "process pid={pid} stat field={field} invalid after configured reads; cleanup unverified (worker.identity_read_attempts)"
    ))
}

fn census(limits: &WorkerLimits) -> Result<std::collections::BTreeMap<u32, Identity>, String> {
    let mut info = std::collections::BTreeMap::new();
    for entry in std::fs::read_dir("/proc").map_err(|_| "process census unavailable")? {
        let entry = entry.map_err(|_| "process census entry unavailable")?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<u32>().ok())
        else {
            continue;
        };
        if let Some(value) = identity(pid, limits)? {
            info.insert(pid, value);
        }
        if info.len() > limits.process_census_entries {
            return Err("process census exceeds bound (worker.process_census_entries)".into());
        }
    }
    Ok(info)
}
fn descendants(
    root: u32,
    known: &[OwnedPid],
    limits: &WorkerLimits,
) -> Result<Vec<OwnedPid>, String> {
    let info = census(limits)?;
    let mut owned = std::collections::BTreeMap::new();
    for k in known {
        if info.get(&k.pid).is_some_and(|i| i.start == k.start_ticks) {
            owned.insert(k.pid, k.start_ticks);
        }
    }
    loop {
        let count = owned.len();
        for (&pid, i) in &info {
            if i.parent == root || owned.contains_key(&i.parent) {
                owned.insert(pid, i.start);
            }
        }
        if owned.len() > limits.tracked_descendants {
            return Err(
                "owned descendant census exceeds bound (worker.tracked_descendants)".into(),
            );
        }
        if count == owned.len() {
            break;
        }
    }
    Ok(owned
        .into_iter()
        .map(|(pid, start_ticks)| OwnedPid { pid, start_ticks })
        .collect())
}
fn stop_owned(owned: &[OwnedPid], limits: &WorkerLimits) -> Result<(), String> {
    for p in owned.iter().rev() {
        if p.pid > 1
            && p.pid != std::process::id()
            && identity(p.pid, limits)?.is_some_and(|i| i.start == p.start_ticks)
        {
            let _ = std::process::Command::new("/bin/kill")
                .args(["-KILL", "--", &p.pid.to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
    Ok(())
}
fn any_owned_live(owned: &[OwnedPid], limits: &WorkerLimits) -> Result<bool, String> {
    for p in owned {
        if identity(p.pid, limits)?
            .is_some_and(|i| i.start == p.start_ticks && !matches!(i.state, 'Z' | 'X'))
        {
            return Ok(true);
        }
    }
    Ok(false)
}
fn live_group(group: u32, limits: &WorkerLimits) -> Result<Vec<OwnedPid>, String> {
    Ok(census(limits)?
        .into_iter()
        .filter(|(_, i)| i.group == group && !matches!(i.state, 'Z' | 'X'))
        .map(|(pid, i)| OwnedPid {
            pid,
            start_ticks: i.start,
        })
        .collect())
}
fn read_record(path: &Path, limits: &WorkerLimits) -> Result<ProcessRecord, String> {
    use std::io::Read;
    let file =
        std::fs::File::open(path).map_err(|_| "process record unreadable; cleanup unverified")?;
    if file
        .metadata()
        .map_err(|_| "process record metadata unavailable")?
        .len()
        > limits.process_record_bytes as u64
    {
        return Err("process record exceeds bound (worker.process_record_bytes)".into());
    }
    let mut bytes = Vec::new();
    file.take(limits.process_record_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "process record unreadable")?;
    if bytes.len() as u64 > limits.process_record_bytes as u64 {
        return Err("process record exceeds bound (worker.process_record_bytes)".into());
    }
    let record: ProcessRecord = serde_json::from_slice(&bytes)
        .map_err(|_| "corrupt or legacy process record; refusing cleanup")?;
    if record.descendants.len() > limits.tracked_descendants {
        return Err("invalid process ownership record (worker.tracked_descendants)".into());
    }
    if record.pid <= 1 || record.process_group == 0 {
        return Err("invalid process ownership record".into());
    }
    record.context.verify()?;
    Ok(record)
}
fn verify_root(record: &ProcessRecord, limits: &WorkerLimits) -> Result<bool, String> {
    record.context.verify()?;
    match identity(record.pid, limits)? {
        Some(i) if i.start == record.start_ticks && i.group == record.process_group => Ok(true),
        Some(_) => Err("process identity reused; cleanup unverified".into()),
        None => Ok(false),
    }
}
fn signal_record(record: &ProcessRecord, limits: &WorkerLimits) -> Result<(), String> {
    let present = verify_root(record, limits)?;
    // A missing leader cannot authorize a reused numeric group. Every live
    // member must still have an exact independently retained identity.
    if !present {
        for member in live_group(record.process_group, limits)? {
            if !record
                .descendants
                .iter()
                .any(|p| p.pid == member.pid && p.start_ticks == member.start_ticks)
            {
                return Err(
                    "leaderless process group ownership ambiguous; cleanup unverified".into(),
                );
            }
        }
    }
    stop_owned(&record.descendants, limits)?;
    if present {
        signal_group(record.process_group, "-KILL");
    }
    Ok(())
}

fn signal_group(pid: u32, signal: &str) {
    if pid <= 1 {
        return;
    }
    // No unsafe FFI or additional dependency. IDs originate only in our spawn record.
    let _ = std::process::Command::new("/bin/kill")
        .args([signal, "--", &format!("-{pid}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

pub struct WorkerLease {
    path: PathBuf,
    lock: Arc<std::fs::File>,
}
impl WorkerLease {
    pub fn claim(path: &Path, limits: &WorkerLimits) -> Result<Self, String> {
        use std::os::unix::fs::OpenOptionsExt;
        // The stable lock inode is never removed; it covers stale-record reclaim.
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(path.with_extension("lock"))
            .map_err(|e| e.to_string())?;
        // Retry only genuine lock contention for a bounded startup window.
        // Concurrent fork/exec may briefly retain a close-on-exec descriptor;
        // that is a possible transient, never permission to bypass the lock.
        let deadline =
            std::time::Instant::now() + Duration::from_millis(limits.owner_lock_timeout_ms);
        loop {
            match lock.try_lock() {
                Ok(()) => break,
                Err(std::fs::TryLockError::WouldBlock) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(limits.owner_lock_retry_ms));
                }
                Err(std::fs::TryLockError::WouldBlock) => {
                    return Err(
                        "worker ownership lock remained busy for configured timeout (WouldBlock) (worker.owner_lock_timeout_ms)"
                            .into(),
                    );
                }
                Err(std::fs::TryLockError::Error(error)) => {
                    return Err(format!("worker ownership lock failed: {error}"));
                }
            }
        }
        if path.exists() {
            let record = read_record(path, limits)?;
            if identity(record.pid, limits)?.is_some_and(|i| i.start == record.start_ticks) {
                return Err("another worker process owns this local worker identity".into());
            }
        }
        let context = ProcessContext::current()?;
        let pid = std::process::id();
        let i = identity(pid, limits)?.ok_or("cannot identify worker process")?;
        let record = ProcessRecord {
            pid,
            start_ticks: i.start,
            process_group: i.group,
            context,
            descendants: vec![],
        };
        crate::protocol::bounded_json_size(&record, limits.process_record_bytes)
            .map_err(|error| format!("{error} (worker.process_record_bytes)"))?;
        // Keep the validated stale owner until its complete successor has been
        // written and file-synced. The same stable lock covers the rename and
        // directory sync. Any failure returns without a lease; after rename,
        // uncertain directory durability never authorizes new execution.
        atomic_json(path, &record).map_err(|e| format!("worker owner claim failed: {e}"))?;
        Ok(Self {
            path: path.to_owned(),
            lock: Arc::new(lock),
        })
    }
    pub fn owner_lock(&self) -> Arc<std::fs::File> {
        self.lock.clone()
    }
}
impl Drop for WorkerLease {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub struct SpawnFailure {
    pub owner: Option<ManagedProcess>,
    pub error: String,
}
impl std::fmt::Debug for SpawnFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpawnFailure")
            .field("error", &self.error)
            .field("owns_child", &self.owner.is_some())
            .finish()
    }
}
#[cfg(debug_assertions)]
static REGISTRATION_FAILURES: std::sync::Mutex<Vec<(PathBuf, tokio::sync::oneshot::Sender<u32>)>> =
    std::sync::Mutex::new(Vec::new());
#[cfg(debug_assertions)]
pub fn qualification_fail_next_registration(
    registry: &Path,
) -> tokio::sync::oneshot::Receiver<u32> {
    let (send, receive) = tokio::sync::oneshot::channel();
    REGISTRATION_FAILURES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push((registry.to_owned(), send));
    receive
}

pub struct ManagedProcess {
    limits: Arc<WorkerLimits>,
    pub child: Child,
    pub stdin: Option<ChildStdin>,
    pid: u32,
    record: PathBuf,
    clean: bool,
    start_ticks: Option<u64>,
    context: ProcessContext,
    exit_status: Option<ExitStatus>,
    registered: bool,
    descendants: Vec<OwnedPid>,
}
impl ManagedProcess {
    pub async fn spawn(
        program: &Path,
        args: &[String],
        cwd: &Path,
        registry: &Path,
        env: &[(String, String)],
        limits: Arc<WorkerLimits>,
    ) -> Result<Self, String> {
        std::fs::create_dir_all(registry).map_err(|e| e.to_string())?;
        // The gate prevents repository code from running before its ownership record is durable.
        let mut command = Command::new("/bin/sh");
        // Traceability scope: The child environment allowlist cannot prove deployment-wide worker credential provisioning.
        command.env_clear();
        command.env("GIT_TERMINAL_PROMPT", "0");
        for key in [
            "PATH",
            "LANG",
            "LC_ALL",
            "LC_CTYPE",
            "TZ",
            "TMPDIR",
            "TMP",
            "TEMP",
            "RUSTUP_HOME",
            "RUSTUP_TOOLCHAIN",
            "CARGO_HOME",
            "CARGO_TARGET_DIR",
            "RUSTFLAGS",
            "CC",
            "CXX",
            "AR",
            "CFLAGS",
            "CXXFLAGS",
            "SQLITE3_LIB_DIR",
            "SQLITE3_INCLUDE_DIR",
            "SQLITE3_STATIC",
            "PKG_CONFIG_LIBDIR",
            "PKG_CONFIG_PATH",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "NO_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
            "no_proxy",
            "SSL_CERT_FILE",
            "SSL_CERT_DIR",
            "REQUESTS_CA_BUNDLE",
            "CURL_CA_BUNDLE",
            "NODE_EXTRA_CA_CERTS",
        ] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        let home = if let Some(configured) = std::env::var_os("BORG_WORKER_HOME") {
            let home = PathBuf::from(configured);
            // An externally configured home is not ours to chmod or provision.
            if !home.is_dir() {
                return Err("configured worker home is unavailable".into());
            }
            home
        } else {
            let home = registry.join("home");
            private_directory(&home)?;
            // The host's managed filesystem profile resolves ~/code against
            // this private HOME. Its mount target must be a real directory.
            // Provision only our empty target; never copy the user's checkout
            // or change the managed policy's access rules.
            private_directory(&home.join("code"))?;
            home
        };
        command.env("HOME", &home);
        if let Some(original_home) = std::env::var_os("HOME") {
            if std::env::var_os("RUSTUP_HOME").is_none() {
                command.env("RUSTUP_HOME", Path::new(&original_home).join(".rustup"));
            }
            if std::env::var_os("CARGO_HOME").is_none() {
                command.env("CARGO_HOME", Path::new(&original_home).join(".cargo"));
            }
        }
        // An operator may explicitly select a worker Git identity; never inherit an unrelated SSH agent.
        for (source, destination) in [
            ("BORG_WORKER_SSH_AUTH_SOCK", "SSH_AUTH_SOCK"),
            ("BORG_WORKER_GIT_SSH_COMMAND", "GIT_SSH_COMMAND"),
        ] {
            if let Some(value) = std::env::var_os(source) {
                command.env(destination, value);
            }
        }
        command
            .args([
                "-c",
                "umask 077; IFS= read -r token && [ \"$token\" = go ] || exit 125; printf 'borg-ready\\n' >&2; exec \"$@\"",
                "borg-owned-process",
            ])
            .arg(program)
            .args(args)
            .current_dir(cwd)
            .envs(env.iter().cloned())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        command.process_group(0);
        let context = ProcessContext::current()?;
        let child = command
            .spawn()
            .map_err(|e| format!("process spawn failed: {e}"))?;
        let mut process = match Self::register_child(child, registry, context, limits.clone()) {
            Ok(process) => process,
            Err(mut failure) => {
                if let Some(owner) = failure.owner.as_mut() {
                    let _ = owner
                        .finish_registered(
                            true,
                            tokio::time::Instant::now()
                                + Duration::from_millis(limits.process_stop_timeout_ms),
                        )
                        .await;
                }
                return Err(failure.error);
            }
        };
        process
            .stdin
            .as_mut()
            .ok_or("missing process stdin")?
            .write_all(b"go\n")
            .await
            .map_err(|e| e.to_string())?;
        // Do not send executor JSON until the shell has finished reading its gate line.
        // Read bytewise so no actual command stderr is lost to a temporary buffer.
        let ready = tokio::time::timeout(
            Duration::from_millis(limits.spawn_ready_timeout_ms),
            async {
                let stderr = process
                    .child
                    .stderr
                    .as_mut()
                    .ok_or("missing process stderr")?;
                let mut marker = Vec::new();
                loop {
                    let byte = stderr
                        .read_u8()
                        .await
                        .map_err(|_| "process startup gate closed")?;
                    marker.push(byte);
                    if byte == b'\n' {
                        break;
                    }
                    if marker.len() > limits.spawn_marker_bytes {
                        return Err("invalid process startup gate (worker.spawn_marker_bytes)");
                    }
                }
                if marker != b"borg-ready\n" {
                    return Err("invalid process startup gate");
                }
                Ok(())
            },
        )
        .await
        .map_err(|_| "process startup gate timed out (worker.spawn_ready_timeout_ms)")?;
        ready?;
        Ok(process)
    }
    fn record_value(&self) -> Result<ProcessRecord, String> {
        Ok(ProcessRecord {
            pid: self.pid,
            start_ticks: self
                .start_ticks
                .ok_or("process identity unavailable; cleanup unverified")?,
            process_group: self.pid,
            context: self.context.clone(),
            descendants: self.descendants.clone(),
        })
    }
    fn register_child(
        mut child: Child,
        registry: &Path,
        context: ProcessContext,
        limits: Arc<WorkerLimits>,
    ) -> Result<Self, SpawnFailure> {
        let pid = child.id().unwrap_or(0);
        let mut owner = Self {
            limits: limits.clone(),
            stdin: child.stdin.take(),
            child,
            pid,
            record: registry.join(format!("{pid}.json")),
            clean: false,
            start_ticks: None,
            context,
            exit_status: None,
            registered: false,
            descendants: vec![],
        };
        let registered = (|| -> Result<(), String> {
            if pid <= 1 {
                return Err("spawned process identity unavailable".into());
            }
            let i = identity(pid, &limits)?.ok_or("spawned process identity unavailable")?;
            if i.group != pid {
                return Err("spawned process group mismatch".into());
            }
            owner.start_ticks = Some(i.start);
            #[cfg(debug_assertions)]
            {
                let mut failures = REGISTRATION_FAILURES
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                if let Some(index) = failures.iter().position(|(path, _)| path == registry) {
                    let (_, send) = failures.swap_remove(index);
                    let _ = send.send(pid);
                    return Err("injected process registration failure".into());
                }
            }
            if owner.record.exists() {
                return Err("process record identity already exists; refusing overwrite".into());
            }
            let record = owner.record_value()?;
            crate::protocol::bounded_json_size(&record, limits.process_record_bytes)
                .map_err(|error| format!("{error} (worker.process_record_bytes)"))?;
            atomic_json(&owner.record, &record)?;
            owner.registered = true;
            Ok(())
        })();
        match registered {
            Ok(()) => Ok(owner),
            Err(error) => {
                owner.stdin.take();
                Err(SpawnFailure {
                    owner: Some(owner),
                    error,
                })
            }
        }
    }
    /// The trusted validator cannot start validation until its first stdin frame.
    /// This function contains no await and returns writable input only after fsync.
    pub fn spawn_registered(
        program: &Path,
        registry: &Path,
        limits: Arc<WorkerLimits>,
    ) -> Result<Self, SpawnFailure> {
        let before = (|| -> Result<ProcessContext, String> {
            std::fs::create_dir_all(registry).map_err(|e| e.to_string())?;
            ProcessContext::current()
        })();
        let context = before.map_err(|error| SpawnFailure { owner: None, error })?;
        let mut command = Command::new(program);
        command
            .arg("__native_validate")
            .env_clear()
            .current_dir(registry)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .process_group(0);
        let child = command.spawn().map_err(|_| SpawnFailure {
            owner: None,
            error: "validator spawn failed".into(),
        })?;
        Self::register_child(child, registry, context, limits.clone())
    }
    pub fn refresh_ownership(&mut self) -> Result<(), String> {
        let record = self.record_value()?;
        if verify_root(&record, &self.limits)? {
            self.descendants = descendants(self.pid, &self.descendants, &self.limits)?;
            // While the exact leader still exists (including an unreaped
            // zombie), its group cannot have been recycled. Retain orphaned
            // group members before reaping the leader.
            for member in live_group(self.pid, &self.limits)? {
                if member.pid != self.pid
                    && !self
                        .descendants
                        .iter()
                        .any(|p| p.pid == member.pid && p.start_ticks == member.start_ticks)
                {
                    self.descendants.push(member);
                }
            }
            if self.descendants.len() > self.limits.tracked_descendants {
                return Err(
                    "owned descendant census exceeds bound (worker.tracked_descendants)".into(),
                );
            }
        } else {
            for member in live_group(self.pid, &self.limits)? {
                if !self
                    .descendants
                    .iter()
                    .any(|p| p.pid == member.pid && p.start_ticks == member.start_ticks)
                {
                    return Err(
                        "leaderless process group ownership ambiguous; cleanup unverified".into(),
                    );
                }
            }
        }
        if self.registered {
            let record = self.record_value()?;
            crate::protocol::bounded_json_size(&record, self.limits.process_record_bytes)
                .map_err(|error| format!("{error} (worker.process_record_bytes)"))?;
            atomic_json(&self.record, &record)?;
        }
        Ok(())
    }
    pub fn request_kill_registered(&mut self) -> Result<(), String> {
        self.stdin.take();
        let result = (|| {
            self.context.verify()?;
            self.refresh_ownership()?;
            signal_record(&self.record_value()?, &self.limits)
        })();
        // This live Child handle remains ours even if journal inspection fails.
        // It is never reconstructed from a numeric PID during recovery.
        let _ = self.child.start_kill();
        result
    }
    pub fn try_finish_registered(&mut self) -> Result<Option<ExitStatus>, String> {
        if self.clean {
            return Ok(self.exit_status);
        }
        self.context.verify()?;
        if self.exit_status.is_none() {
            if self.start_ticks.is_some() {
                self.refresh_ownership()?;
            }
            self.exit_status = self
                .child
                .try_wait()
                .map_err(|_| "process wait unavailable; cleanup unverified")?;
        }
        let Some(status) = self.exit_status else {
            return Ok(None);
        };
        if self.start_ticks.is_some() {
            self.refresh_ownership()?;
            if !live_group(self.pid, &self.limits)?.is_empty()
                || any_owned_live(&self.descendants, &self.limits)?
            {
                return Ok(None);
            }
        }
        if self.registered {
            match std::fs::remove_file(&self.record) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return Err("process registry cleanup unverified".into()),
            }
        }
        self.clean = true;
        Ok(Some(status))
    }
    pub async fn finish_registered(
        &mut self,
        kill: bool,
        deadline: tokio::time::Instant,
    ) -> Result<ExitStatus, String> {
        if kill {
            self.request_kill_registered()?;
        }
        self.stdin.take();
        loop {
            if let Some(status) = self.try_finish_registered()? {
                return Ok(status);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("process cleanup deadline reached; termination unverified".into());
            }
            if kill {
                self.request_kill_registered()?;
            }
            tokio::time::sleep_until(
                (tokio::time::Instant::now() + Duration::from_millis(self.limits.process_poll_ms))
                    .min(deadline),
            )
            .await;
        }
    }
    //= spec/impl/v2/implementation.md#interrupted-work-is-reconciled
    //# The platform MUST NOT report an interrupted command as stopped until its worker confirms termination or the worker is known unable to continue it.
    pub async fn stop(&mut self) -> Result<(), String> {
        self.finish_registered(
            true,
            tokio::time::Instant::now()
                + Duration::from_millis(self.limits.process_stop_timeout_ms),
        )
        .await
        .map(|_| ())
        .map_err(|error| {
            if error == "process cleanup deadline reached; termination unverified" {
                format!("{error} (worker.process_stop_timeout_ms)")
            } else {
                error
            }
        })
    }
}
impl Drop for ManagedProcess {
    fn drop(&mut self) {
        if !self.clean {
            let _ = self.request_kill_registered();
        }
    }
}

//= spec/impl/v2/implementation.md#worker-commands-do-not-create-duplicate-execution
//# A restarted worker MUST account for surviving prior-incarnation processes before accepting new work.
pub async fn reconcile_processes(registry: &Path, limits: &WorkerLimits) -> Result<(), String> {
    std::fs::create_dir_all(registry).map_err(|e| e.to_string())?;
    // Bound the whole record set before signaling anything. Ignore the private
    // home directory and other non-record entries without recursive traversal.
    let mut records = Vec::new();
    for entry in std::fs::read_dir(registry).map_err(|e| e.to_string())? {
        let path = entry.map_err(|e| e.to_string())?.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        records.push(path);
        if records.len() > limits.process_registry_records {
            return Err("process registry record count exceeds recovery bound (worker.process_registry_records)".into());
        }
    }
    records.sort();
    for path in records {
        let mut record = read_record(&path, limits)?;
        if record.process_group != record.pid {
            return Err("process group identity invalid; refusing cleanup".into());
        }
        if verify_root(&record, limits)? {
            record.descendants = descendants(record.pid, &record.descendants, limits)?;
            crate::protocol::bounded_json_size(&record, limits.process_record_bytes)
                .map_err(|error| format!("{error} (worker.process_record_bytes)"))?;
            atomic_json(&path, &record)?;
        }
        signal_record(&record, limits)?;
        for _ in 0..limits.recovery_checks {
            if live_group(record.process_group, limits)?.is_empty()
                && !any_owned_live(&record.descendants, limits)?
            {
                break;
            }
            signal_record(&record, limits)?;
            tokio::time::sleep(Duration::from_millis(limits.recovery_interval_ms)).await;
        }
        if !live_group(record.process_group, limits)?.is_empty()
            || any_owned_live(&record.descendants, limits)?
        {
            return Err("prior process group remains live; cleanup unverified (worker.recovery_checks, worker.recovery_interval_ms)".into());
        }
        std::fs::remove_file(path).map_err(|e| e.to_string())?;
    }
    Ok(())
}

#[derive(Debug)]
pub struct CommandOutput {
    pub success: bool,
    pub stdout: String,
    pub text: String,
    pub truncated: bool,
}
pub async fn read_bounded(
    mut stream: impl AsyncRead + Unpin,
    limit: usize,
) -> Result<(Vec<u8>, bool), String> {
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 4096];
    let mut truncated = false;
    loop {
        let n = stream.read(&mut chunk).await.map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        let keep = n.min(limit.saturating_sub(bytes.len()));
        bytes.extend_from_slice(&chunk[..keep]);
        truncated |= keep < n;
    }
    Ok((bytes, truncated))
}
pub async fn command(
    program: &Path,
    args: &[String],
    cwd: &Path,
    registry: &Path,
    control: &Control,
    limit: usize,
) -> Result<CommandOutput, String> {
    control.check()?;
    let mut p = ManagedProcess::spawn(
        program,
        args,
        cwd,
        registry,
        &[],
        control.worker_limits.clone(),
    )
    .await?;
    p.stdin.take();
    let stdout = p.child.stdout.take().ok_or("missing stdout")?;
    let stderr = p.child.stderr.take().ok_or("missing stderr")?;
    let out = tokio::spawn(read_bounded(stdout, limit / 2));
    let err = tokio::spawn(read_bounded(stderr, limit / 2));
    let mut cancellation = control.clone();
    let waited: Result<ExitStatus, String> = tokio::select! {
        status=async {
            loop {
                p.refresh_ownership()?;
                if let Some(status)=p.child.try_wait().map_err(|e|e.to_string())? {break Ok(status);}
                tokio::time::sleep(Duration::from_millis(control.worker_limits.process_poll_ms)).await;
            }
        }=>status,
        _=cancellation.stopped()=>Err("command cancelled or deadline reached".into())
    };
    let cleanup = p.stop().await;
    if let Err(e) = cleanup {
        out.abort();
        err.abort();
        return Err(e);
    }
    let (a, at) = tokio::time::timeout(
        Duration::from_millis(control.worker_limits.stream_drain_timeout_ms),
        out,
    )
    .await
    .map_err(|_| "process descendants unverified: stdout drain timed out (worker.stream_drain_timeout_ms)")?
    .map_err(|e| e.to_string())??;
    let (b, bt) = tokio::time::timeout(
        Duration::from_millis(control.worker_limits.stream_drain_timeout_ms),
        err,
    )
    .await
    .map_err(|_| "process descendants unverified: stderr drain timed out (worker.stream_drain_timeout_ms)")?
    .map_err(|e| e.to_string())??;
    let status = waited?;
    Ok(CommandOutput {
        success: status.success(),
        stdout: String::from_utf8_lossy(&a).into_owned(),
        text: format!(
            "{}{}",
            String::from_utf8_lossy(&a),
            String::from_utf8_lossy(&b)
        ),
        truncated: at || bt,
    })
}

#[cfg(test)]
#[path = "process_storage_tests.rs"]
pub(super) mod storage_tests;

#[cfg(test)]
mod identity_tests {
    use super::*;
    fn stat(group: &str) -> String {
        let mut fields = vec!["0"; 20];
        fields[0] = "S";
        fields[1] = "1";
        fields[2] = group;
        fields[19] = "1234";
        format!("71 (fixture comm) {}", fields.join(" "))
    }
    #[test]
    fn transient_snapshot_requires_valid_reread_or_confirmed_exit() {
        let mut reads = 0;
        let identity = inspect_identity(71, &crate::worker::test_limits().worker, || {
            reads += 1;
            if reads == 1 {
                Ok(stat("-1"))
            } else {
                Ok(stat("71"))
            }
        })
        .unwrap()
        .unwrap();
        assert_eq!(reads, 2);
        assert_eq!(identity.group, 71);
        assert_eq!(identity.start, 1234);
        let mut reads = 0;
        assert!(
            inspect_identity(71, &crate::worker::test_limits().worker, || {
                reads += 1;
                if reads == 1 {
                    Ok(stat("-1"))
                } else {
                    Err(std::io::Error::from_raw_os_error(2))
                }
            })
            .unwrap()
            .is_none()
        );
        assert_eq!(reads, 2);
    }
    #[test]
    fn esrch_is_absence_but_permission_and_io_errors_are_not() {
        assert!(
            inspect_identity(71, &crate::worker::test_limits().worker, || Err(
                std::io::Error::from_raw_os_error(3)
            ))
            .unwrap()
            .is_none()
        );
        for errno in [13, 5] {
            let error = match inspect_identity(71, &crate::worker::test_limits().worker, || {
                Err(std::io::Error::from_raw_os_error(errno))
            }) {
                Err(error) => error,
                Ok(_) => panic!("unknown inspection cannot prove absence"),
            };
            assert!(error.contains("pid=71"));
            assert!(error.contains(&format!("errno=Some({errno})")));
            assert!(error.contains("unverified"));
        }
    }
    #[test]
    fn persistent_invalid_field_never_becomes_absence() {
        let mut reads = 0;
        let error = match inspect_identity(71, &crate::worker::test_limits().worker, || {
            reads += 1;
            Ok(stat("-1"))
        }) {
            Err(error) => error,
            Ok(_) => panic!("invalid group cannot prove absence"),
        };
        assert_eq!(
            reads,
            crate::worker::test_limits().worker.identity_read_attempts
        );
        assert!(error.contains("pid=71"));
        assert!(error.contains("field=process_group"));
        assert!(error.contains("worker.identity_read_attempts"));
        assert!(!error.contains("fixture comm"));
    }
}
