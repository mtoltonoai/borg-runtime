//! Ephemeral host observations. These are host-wide measurements, not task
//! admission, process RSS, cgroup limits, disk health, or a capacity promise.
use serde::{Deserialize, Serialize};
use std::{collections::{BTreeMap, BTreeSet}, io::Read, path::{Component, Path, PathBuf}, sync::Arc, time::Duration};
use tokio::{io::AsyncReadExt, sync::watch};

pub const PROFILE_VERSION: u32 = 1;
pub const STAT_PATH: &str = "/usr/bin/stat";
const STAT_FORMAT: &str = "%b %f %a %S %i\n";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Role { Controller, Worker }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Machine {
    pub machine_id: String,
    pub label: String,
    pub role: Role,
    pub worker_id: Option<String>,
    pub storage_labels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StorageScope { pub label: String, pub path: PathBuf }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HelperIdentity { pub path: PathBuf, pub sha256: String }

/// Separate operator metadata. It is never folded into accepted RuntimeLimits.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Bounds {
    pub sample_interval_ms: u64,
    pub collection_timeout_ms: u64,
    pub cleanup_timeout_ms: u64,
    pub report_timeout_ms: u64,
    pub stale_after_ms: u64,
    pub max_window_ms: u64,
    pub kernel_read_bytes: usize,
    pub helper_output_bytes: usize,
    pub helper_binary_bytes: usize,
    pub max_filesystems: usize,
    pub max_machines: usize,
    pub max_cpu_cores: u32,
    pub identity_bytes: usize,
    pub label_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub version: u32,
    pub local_machine_id: String,
    pub machines: Vec<Machine>,
    pub storage_scopes: Vec<StorageScope>,
    pub stat_helper: HelperIdentity,
    pub bounds: Bounds,
}

fn public_id(s: &str, bound: usize) -> bool {
    !s.is_empty() && s.len() <= bound
        && s.bytes().all(|c| c.is_ascii_alphanumeric() || b"-_.:".contains(&c))
}
fn label(s: &str, bound: usize) -> bool {
    !s.is_empty() && s.len() <= bound && !s.chars().any(char::is_control)
        && !s.contains('/') && !s.contains('\\')
}
fn literal_directory(path: &Path) -> bool {
    path.is_absolute() && path.to_str().is_some_and(|s| !s.chars().any(char::is_control))
        && path.components().all(|c| matches!(c, Component::RootDir | Component::Normal(_)))
        && path.to_str().is_some_and(|s| s == "/" || (!s.ends_with('/') && !s.contains("//")
            && !s.split('/').any(|part| matches!(part,"."|".."))))
}
impl Profile {
    pub fn load(path: &Path, byte_bound: usize) -> Result<Self, String> {
        let bytes = read_file(path, byte_bound).map_err(|_| "host observation profile unavailable or exceeds transport.body_bytes")?;
        let value: Self = serde_json::from_slice(&bytes).map_err(|_| "invalid host observation profile")?;
        value.validate()?;
        Ok(value)
    }
    pub fn local(&self) -> &Machine {
        self.machines.iter().find(|m| m.machine_id == self.local_machine_id)
            .expect("validated local machine binding")
    }
    pub fn validate(&self) -> Result<(), String> {
        let b = &self.bounds;
        if self.version != PROFILE_VERSION { return Err("unsupported host observation profile version".into()); }
        if [b.sample_interval_ms,b.collection_timeout_ms,b.cleanup_timeout_ms,b.report_timeout_ms,b.stale_after_ms,b.max_window_ms]
            .iter().any(|v| *v == 0 || *v > u32::MAX as u64)
            || b.collection_timeout_ms >= b.sample_interval_ms
            || b.stale_after_ms <= b.sample_interval_ms
            || b.max_window_ms < b.sample_interval_ms
            || [b.kernel_read_bytes,b.helper_output_bytes,b.helper_binary_bytes,b.max_filesystems,b.max_machines,b.identity_bytes,b.label_bytes]
                .contains(&0) || b.max_cpu_cores == 0 {
            return Err("invalid host observation bounds".into());
        }
        if self.stat_helper.path != Path::new(STAT_PATH) || self.stat_helper.sha256.len() != 64
            || !self.stat_helper.sha256.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
            return Err("host observation requires the fixed stat helper and explicit sha256 identity".into());
        }
        if self.machines.is_empty() || self.machines.len() > b.max_machines
            || self.storage_scopes.is_empty() || self.storage_scopes.len() > b.max_filesystems {
            return Err("host observation machine/filesystem count exceeds profile bounds".into());
        }
        let mut ids = BTreeSet::new();
        let mut workers = BTreeSet::new();
        let mut controllers = 0;
        for m in &self.machines {
            if !public_id(&m.machine_id,b.identity_bytes) || !ids.insert(&m.machine_id)
                || !label(&m.label,b.label_bytes) || m.storage_labels.is_empty()
                || m.storage_labels.len() > b.max_filesystems
                || m.storage_labels.iter().any(|s| !label(s,b.label_bytes))
                || m.storage_labels.iter().collect::<BTreeSet<_>>().len() != m.storage_labels.len() {
                return Err("invalid host observation public mapping".into());
            }
            match (&m.role,&m.worker_id) {
                (Role::Controller,None) => controllers += 1,
                (Role::Worker,Some(id)) if public_id(id,b.identity_bytes) && workers.insert(id) => (),
                _ => return Err("invalid host observation worker/role mapping".into()),
            }
        }
        if controllers != 1 || !ids.contains(&self.local_machine_id) {
            return Err("host observation requires one configured controller and an exact local binding".into());
        }
        let mut scopes = BTreeSet::new();
        for s in &self.storage_scopes {
            if !literal_directory(&s.path) || !scopes.insert(&s.label) {
                return Err("host observation scopes require unique labels and literal absolute local directories".into());
            }
        }
        if scopes != self.local().storage_labels.iter().collect() {
            return Err("host observation local scopes differ from the public mapping".into());
        }
        Ok(())
    }
}

/// Closed public diagnostic vocabulary. Never helper stderr, paths or commands.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    FirstSample, CounterReset, BootChanged, CpuCountChanged, InvalidInterval,
    ReadUnavailable, ReadBound, InvalidData, NumericOverflow, ScopeChanged,
    ScopeUnavailable, UnsupportedFilesystem, HelperUnavailable, HelperIdentity,
    HelperFailed, HelperOutputBound, TimedOut, Cancelled, CleanupUnverified,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum Metric<T> {
    Available { value: T },
    WarmingUp { reason: Reason },
    Unavailable { reason: Reason },
}
impl<T> Metric<T> {
    fn unavailable(reason: Reason) -> Self { Self::Unavailable { reason } }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CpuUsage {
    pub used_core_equivalents: f64,
    pub total_online_logical_cores: u32,
    pub percent: f64,
    pub busy_ticks: u64,
    pub total_ticks: u64,
    pub window_ms: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MemoryUsage { pub used_bytes: u64, pub total_bytes: u64, pub available_bytes: u64 }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DiskUsage { pub used_bytes: u64, pub total_bytes: u64, pub free_bytes: u64, pub available_bytes: u64 }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Filesystem { pub scope_labels: Vec<String>, pub space: Metric<DiskUsage> }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkerBinding { pub worker_id: String, pub incarnation: String }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Sample {
    pub machine_id: String,
    pub role: Role,
    pub worker: Option<WorkerBinding>,
    pub producer_epoch: String,
    pub sequence: u64,
    /// Sender wall time is informational; it never controls receiver freshness.
    pub observed_at_unix_ms: u64,
    /// CPU delta interval; None when there is no valid pair of observations.
    pub sample_window_ms: Option<u64>,
    pub cpu: Metric<CpuUsage>,
    pub memory: Metric<MemoryUsage>,
    pub filesystems: Vec<Filesystem>,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Freshness { Unobserved, Fresh, Stale }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct View {
    pub machine: Machine,
    pub freshness: Freshness,
    pub received_at_unix_ms: Option<u64>,
    pub receipt_age_ms: Option<u64>,
    pub stale_after_ms: u64,
    /// Freshness/age are an observation at this read, including in read receipts.
    pub as_of_unix_ms: u64,
    pub sample: Option<Sample>,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReportDisposition { Replaced, Duplicate }

impl Sample {
    pub fn validate(&self, machine: &Machine, b: &Bounds) -> Result<(), &'static str> {
        if self.machine_id != machine.machine_id || self.role != machine.role || self.sequence == 0
            || self.producer_epoch.len()>b.identity_bytes || parse_producer_epoch(&self.producer_epoch).is_none()
            || self.worker.as_ref().map(|w| &w.worker_id) != machine.worker_id.as_ref()
            || self.worker.as_ref().is_some_and(|w| !public_id(&w.incarnation,b.identity_bytes)) {
            return Err("machine_identity");
        }
        if self.sample_window_ms.is_some_and(|n| n == 0 || n > b.max_window_ms) { return Err("sample_window"); }
        if let Metric::Available { value: c } = &self.cpu {
            let fraction = c.busy_ticks as f64 / c.total_ticks as f64;
            if c.total_ticks == 0 || c.busy_ticks > c.total_ticks || c.total_online_logical_cores == 0
                || c.total_online_logical_cores > b.max_cpu_cores || self.sample_window_ms != Some(c.window_ms)
                || !c.percent.is_finite() || !c.used_core_equivalents.is_finite()
                || c.percent < 0.0 || c.percent > 100.0
                || c.used_core_equivalents < 0.0 || c.used_core_equivalents > c.total_online_logical_cores as f64
                || (c.percent - fraction * 100.0).abs() > f64::EPSILON * 128.0
                || (c.used_core_equivalents - fraction * c.total_online_logical_cores as f64).abs() > f64::EPSILON * b.max_cpu_cores as f64 {
                return Err("cpu_consistency");
            }
        }
        if let Metric::Available { value: m } = &self.memory {
            if m.total_bytes == 0 || m.total_bytes.checked_sub(m.available_bytes) != Some(m.used_bytes) { return Err("memory_consistency"); }
        }
        if self.filesystems.len() > b.max_filesystems { return Err("filesystem_count"); }
        let mut labels = BTreeSet::new();
        for f in &self.filesystems {
            if f.scope_labels.is_empty() || f.scope_labels.len() > b.max_filesystems
                || f.scope_labels.iter().any(|s| !labels.insert(s)) { return Err("filesystem_scope"); }
            if let Metric::Available { value: d } = &f.space {
                if d.total_bytes == 0 || d.total_bytes.checked_sub(d.free_bytes) != Some(d.used_bytes) || d.available_bytes > d.free_bytes {
                    return Err("filesystem_consistency");
                }
            }
        }
        if labels != machine.storage_labels.iter().collect() { return Err("filesystem_scope"); }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CpuCounters { counters: [u64; 8], cores: u32, boot: String, at_ms: u64 }
pub fn parse_cpu(text: &str, boot: &str, at_ms: u64, max_cores: u32) -> Result<CpuCounters, Reason> {
    if boot.len() != 36 || !boot.bytes().enumerate().all(|(i,c)| if [8,13,18,23].contains(&i) {c==b'-'} else {c.is_ascii_hexdigit()}) {
        return Err(Reason::InvalidData);
    }
    let mut aggregate = None;
    let mut cpus = BTreeSet::new();
    for line in text.lines() {
        let mut fields = line.split_ascii_whitespace();
        let Some(name) = fields.next() else { continue; };
        if name == "cpu" {
            if aggregate.is_some() { return Err(Reason::InvalidData); }
            let values = fields.map(|s| s.parse::<u64>().map_err(|_| Reason::InvalidData)).collect::<Result<Vec<_>,_>>()?;
            if !(8..=10).contains(&values.len()) { return Err(Reason::InvalidData); }
            // guest and guest_nice are already in user/nice. Only the first eight
            // counters participate in total, and idle+iowait are not busy.
            aggregate = Some(values[..8].try_into().map_err(|_| Reason::InvalidData)?);
        } else if let Some(id) = name.strip_prefix("cpu").filter(|s| !s.is_empty() && s.bytes().all(|b|b.is_ascii_digit())) {
            let id: u32 = id.parse().map_err(|_|Reason::InvalidData)?;
            if !cpus.insert(id) || cpus.len() > max_cores as usize { return Err(Reason::InvalidData); }
        }
    }
    if cpus.is_empty() { return Err(Reason::InvalidData); }
    Ok(CpuCounters { counters: aggregate.ok_or(Reason::InvalidData)?, cores: cpus.len() as u32, boot:boot.into(), at_ms })
}
pub fn cpu_window(previous: Option<&CpuCounters>, current: &CpuCounters, max_window_ms: u64) -> Metric<CpuUsage> {
    let warming = |reason| Metric::WarmingUp { reason };
    let Some(previous) = previous else { return warming(Reason::FirstSample); };
    if previous.boot != current.boot { return warming(Reason::BootChanged); }
    if previous.cores != current.cores { return warming(Reason::CpuCountChanged); }
    let Some(window_ms) = current.at_ms.checked_sub(previous.at_ms).filter(|v| *v > 0 && *v <= max_window_ms) else {
        return Metric::unavailable(Reason::InvalidInterval);
    };
    let mut delta = [0u64;8];
    for (i,d) in delta.iter_mut().enumerate() {
        let Some(n) = current.counters[i].checked_sub(previous.counters[i]) else { return warming(Reason::CounterReset); };
        *d = n;
    }
    let Some(total) = delta.iter().try_fold(0u64,|a,n| a.checked_add(*n)) else { return Metric::unavailable(Reason::NumericOverflow); };
    let Some(idle) = delta[3].checked_add(delta[4]) else { return Metric::unavailable(Reason::NumericOverflow); };
    if total == 0 { return Metric::unavailable(Reason::InvalidInterval); }
    let Some(busy) = total.checked_sub(idle) else { return Metric::unavailable(Reason::InvalidData); };
    let fraction = busy as f64 / total as f64;
    Metric::Available { value: CpuUsage { used_core_equivalents:fraction * current.cores as f64,
        total_online_logical_cores:current.cores, percent:fraction * 100.0, busy_ticks:busy,total_ticks:total,window_ms } }
}
pub fn parse_memory(text: &str) -> Result<MemoryUsage, Reason> {
    let mut total = None;
    let mut available = None;
    for line in text.lines() {
        let mut words = line.split_ascii_whitespace();
        let slot = match words.next() { Some("MemTotal:") => &mut total, Some("MemAvailable:") => &mut available, _ => continue };
        if slot.is_some() { return Err(Reason::InvalidData); }
        let n = words.next().and_then(|s|s.parse::<u64>().ok()).ok_or(Reason::InvalidData)?;
        if words.next() != Some("kB") || words.next().is_some() { return Err(Reason::InvalidData); }
        *slot = Some(n.checked_mul(1024).ok_or(Reason::NumericOverflow)?);
    }
    let total_bytes = total.filter(|v| *v > 0).ok_or(Reason::InvalidData)?;
    let available_bytes = available.ok_or(Reason::InvalidData)?;
    let used_bytes = total_bytes.checked_sub(available_bytes).ok_or(Reason::InvalidData)?;
    Ok(MemoryUsage { total_bytes,used_bytes,available_bytes })
}
fn parse_disk(bytes: &[u8]) -> Result<(String,DiskUsage),Reason> {
    let text = std::str::from_utf8(bytes).map_err(|_|Reason::InvalidData)?;
    let fields = text.split_ascii_whitespace().collect::<Vec<_>>();
    if fields.len() != 5 { return Err(Reason::InvalidData); }
    let n = |i:usize| fields[i].parse::<u64>().map_err(|_|Reason::InvalidData);
    let (blocks,free,available,size) = (n(0)?,n(1)?,n(2)?,n(3)?);
    if size == 0 || blocks == 0 || available > free || free > blocks
        || fields[4].is_empty() || fields[4].len()>32 || !fields[4].bytes().all(|c|c.is_ascii_hexdigit()) {
        return Err(Reason::InvalidData);
    }
    let bytes = |v:u64| v.checked_mul(size).ok_or(Reason::NumericOverflow);
    Ok((fields[4].to_ascii_lowercase(), DiskUsage { total_bytes:bytes(blocks)?, used_bytes:bytes(blocks-free)?,free_bytes:bytes(free)?,available_bytes:bytes(available)? }))
}

/// Grow only with actual bytes. A large configured ceiling is not an allocation.
fn read_file(path: &Path, bound: usize) -> Result<Vec<u8>,Reason> {
    let mut f = std::fs::File::open(path).map_err(|_|Reason::ReadUnavailable)?;
    let mut out = Vec::new();
    let mut chunk = [0u8;4096];
    loop {
        let n = f.read(&mut chunk).map_err(|_|Reason::ReadUnavailable)?;
        if n==0 { return Ok(out); }
        if n > bound.saturating_sub(out.len()) { return Err(Reason::ReadBound); }
        out.try_reserve(n).map_err(|_|Reason::ReadBound)?;
        out.extend_from_slice(&chunk[..n]);
    }
}
fn read_text(path: &str, bound: usize) -> Result<String,Reason> {
    String::from_utf8(read_file(Path::new(path),bound)?).map_err(|_|Reason::InvalidData)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DirectoryIdentity { device:u64, inode:u64, mount:u64, mount_record:String }
fn directory_identity(path: &Path, bound: usize) -> Result<DirectoryIdentity, Reason> {
    use std::os::{fd::AsRawFd, unix::fs::{MetadataExt,OpenOptionsExt}};
    if !literal_directory(path) { return Err(Reason::ScopeUnavailable); }
    let mut prefix = PathBuf::new();
    for c in path.components() {
        prefix.push(c.as_os_str());
        let m=std::fs::symlink_metadata(&prefix).map_err(|_|Reason::ScopeUnavailable)?;
        if !m.is_dir() || m.file_type().is_symlink() { return Err(Reason::ScopeUnavailable); }
    }
    // Linux O_PATH | O_DIRECTORY | O_NOFOLLOW. Metadata-only descriptors; no
    // unsafe FFI, traversal, filesystem mutations, or opening unselected scopes.
    let f=std::fs::OpenOptions::new().read(true).custom_flags(0x200000|0x10000|0x20000)
        .open(path).map_err(|_|Reason::ScopeUnavailable)?;
    let meta=f.metadata().map_err(|_|Reason::ScopeUnavailable)?;
    let info=read_text(&format!("/proc/self/fdinfo/{}",f.as_raw_fd()),bound)?;
    let ids=info.lines().filter_map(|l|l.strip_prefix("mnt_id:")).collect::<Vec<_>>();
    if ids.len()!=1 { return Err(Reason::ScopeUnavailable); }
    let mount=ids[0].trim().parse::<u64>().map_err(|_|Reason::ScopeUnavailable)?;
    let mounts=read_text("/proc/self/mountinfo",bound)?;
    let mut matches=mounts.lines().filter(|l|l.split_ascii_whitespace().next().and_then(|s|s.parse::<u64>().ok())==Some(mount));
    let record=matches.next().ok_or(Reason::ScopeUnavailable)?;
    if matches.next().is_some() { return Err(Reason::ScopeUnavailable); }
    let kind=record.split_once(" - ").and_then(|(_,t)|t.split_ascii_whitespace().next()).ok_or(Reason::ScopeUnavailable)?;
    // Only selected existing local filesystem families. Memory, proc, network,
    // FUSE and unknown mounts never trigger a stat helper.
    if !matches!(kind,"ext4"|"xfs"|"btrfs"|"overlay") { return Err(Reason::UnsupportedFilesystem); }
    Ok(DirectoryIdentity {device:meta.dev(),inode:meta.ino(),mount,mount_record:record.into()})
}
fn same_directory(before:&DirectoryIdentity, after:&DirectoryIdentity)->Result<(),Reason> {
    if before==after {Ok(())} else {Err(Reason::ScopeChanged)}
}
fn verified_helper(profile: &Profile) -> Result<(),Reason> {
    use std::os::unix::fs::MetadataExt;
    let path=&profile.stat_helper.path;
    let before=std::fs::symlink_metadata(path).map_err(|_|Reason::HelperUnavailable)?;
    if !before.is_file() || before.file_type().is_symlink() { return Err(Reason::HelperIdentity); }
    let data=read_file(path,profile.bounds.helper_binary_bytes).map_err(|_|Reason::HelperIdentity)?;
    let digest=aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256,&data);
    let hash=digest.as_ref().iter().map(|b|format!("{b:02x}")).collect::<String>();
    let after=std::fs::symlink_metadata(path).map_err(|_|Reason::HelperIdentity)?;
    if before.dev()!=after.dev() || before.ino()!=after.ino() || before.len()!=after.len()
        || before.mtime()!=after.mtime() || before.mtime_nsec()!=after.mtime_nsec() || hash!=profile.stat_helper.sha256 {
        return Err(Reason::HelperIdentity);
    }
    Ok(())
}
async fn cancelled(stop: &mut watch::Receiver<bool>) {
    if *stop.borrow() { return; }
    let _=stop.changed().await;
}

/// One non-forking, hash-selected system helper. Its direct Child is owned until
/// wait confirms reap. No shell/gate process, model arguments, or sample files.
async fn helper_output(program: &Path, args: &[std::ffi::OsString], b: &Bounds,
    deadline: tokio::time::Instant, stop: &mut watch::Receiver<bool>) -> Result<Vec<u8>,Reason> {
    if *stop.borrow() { return Err(Reason::Cancelled); }
    let mut child=tokio::process::Command::new(program).args(args).env_clear().env("LC_ALL","C")
        .stdin(std::process::Stdio::null()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::null())
        .kill_on_drop(true).spawn().map_err(|_|Reason::HelperUnavailable)?;
    let mut stdout=child.stdout.take().ok_or(Reason::HelperUnavailable)?;
    let read=async {
        let mut bytes=Vec::new();let mut chunk=[0u8;1024];
        loop {
            let n=stdout.read(&mut chunk).await.map_err(|_|Reason::HelperFailed)?;
            if n==0 { return Ok(bytes); }
            if n>b.helper_output_bytes.saturating_sub(bytes.len()) {return Err(Reason::HelperOutputBound);}
            bytes.try_reserve(n).map_err(|_|Reason::HelperOutputBound)?;bytes.extend_from_slice(&chunk[..n]);
        }
    };
    let result=tokio::select! { biased;
        _=cancelled(stop)=>Err(Reason::Cancelled),
        _=tokio::time::sleep_until(deadline)=>Err(Reason::TimedOut),
        result=async {let bytes=read.await?;let status=child.wait().await.map_err(|_|Reason::HelperFailed)?;
            if status.success(){Ok(bytes)}else{Err(Reason::HelperFailed)}}=>result,
    };
    if result.is_err() {
        let _=child.start_kill();
        // Never drop a timed-out wait and then start another helper. If cleanup
        // misses its deadline, this owner continues to hold/reap the child; it
        // publishes only CleanupUnverified after actual reap, not success.
        if !matches!(tokio::time::timeout(Duration::from_millis(b.cleanup_timeout_ms),child.wait()).await, Ok(Ok(_))) {
            // A wait I/O error is not a reap acknowledgement either. Keep this
            // child owned and do not permit another sample/helper until wait
            // actually confirms exit. A shutdown may therefore remain held by
            // unverified cleanup instead of falsely reporting success.
            loop {
                let _=child.start_kill();
                if child.wait().await.is_ok() { break; }
                tokio::time::sleep(Duration::from_millis(b.cleanup_timeout_ms)).await;
            }
            return Err(Reason::CleanupUnverified);
        }
    }
    result
}

pub struct Collector { profile:Arc<Profile>, previous:Option<CpuCounters>, origin:std::time::Instant }
impl Collector {
    pub fn new(profile:Arc<Profile>) -> Result<Self,String> { profile.validate()?;Ok(Self {profile,previous:None,origin:std::time::Instant::now()}) }
    pub async fn collect(&mut self, epoch:&str, sequence:u64, worker:Option<WorkerBinding>, stop:&mut watch::Receiver<bool>) -> Sample {
        let p=&self.profile;let b=&p.bounds;
        let deadline=tokio::time::Instant::now()+Duration::from_millis(b.collection_timeout_ms);
        let at=u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX);
        let current=(|| {
            let text=read_text("/proc/stat",b.kernel_read_bytes)?;
            let boot=read_text("/proc/sys/kernel/random/boot_id",b.kernel_read_bytes)?;
            parse_cpu(&text,boot.trim(),at,b.max_cpu_cores)
        })();
        let cpu=match current {Ok(current)=>{let metric=cpu_window(self.previous.as_ref(),&current,b.max_window_ms);self.previous=Some(current);metric},
            Err(reason)=>{self.previous=None;Metric::unavailable(reason)}};
        let memory=match read_text("/proc/meminfo",b.kernel_read_bytes).and_then(|s|parse_memory(&s)) {
            Ok(value)=>Metric::Available{value},Err(reason)=>Metric::unavailable(reason),
        };
        let helper=verified_helper(p);
        let mut filesystems:Vec<Filesystem>=Vec::new();
        let mut seen:BTreeMap<(u64,String),usize>=BTreeMap::new();
        for scope in &p.storage_scopes {
            let collected=async {
                helper?;
                if *stop.borrow() {return Err(Reason::Cancelled);}
                if tokio::time::Instant::now()>=deadline {return Err(Reason::TimedOut);}
                let before=directory_identity(&scope.path,b.kernel_read_bytes)?;
                let args=vec!["--file-system".into(),format!("--printf={STAT_FORMAT}").into(),"--".into(),scope.path.as_os_str().into()];
                let bytes=helper_output(Path::new(STAT_PATH),&args,b,deadline,stop).await?;
                let after=directory_identity(&scope.path,b.kernel_read_bytes)?;
                same_directory(&before,&after)?;
                let (filesystem,value)=parse_disk(&bytes)?;
                Ok(((before.device,filesystem),value))
            }.await;
            match collected {
                Ok((identity,value))=>{
                    if let Some(index)=seen.get(&identity) {filesystems[*index].scope_labels.push(scope.label.clone());}
                    else {seen.insert(identity,filesystems.len());filesystems.push(Filesystem{scope_labels:vec![scope.label.clone()],space:Metric::Available{value}});}
                }
                Err(reason)=>filesystems.push(Filesystem{scope_labels:vec![scope.label.clone()],space:Metric::unavailable(reason)}),
            }
        }
        let sample_window_ms=if let Metric::Available{value}=&cpu {Some(value.window_ms)}else{None};
        Sample {machine_id:p.local_machine_id.clone(),role:p.local().role,worker,producer_epoch:epoch.into(),sequence,
            observed_at_unix_ms:crate::worker::process::now_ms(),sample_window_ms,cpu,memory,filesystems}
    }
}

/// The producer has one collection, one report, and one replaceable latest slot.
/// Tasks/model permits and durable worker command journals are not involved.
pub struct Producer { stop:watch::Sender<bool>, collector:tokio::task::JoinHandle<()>, reporter:tokio::task::JoinHandle<()> }
impl Producer {
    pub fn start<F,Fut>(profile:Arc<Profile>,worker:Option<WorkerBinding>,epoch:String,mut report:F)->Result<Self,String>
    where F:FnMut(Sample)->Fut+Send+'static,Fut:std::future::Future<Output=()>+Send+'static {
        let mut collector=Collector::new(profile.clone())?;
        let machine=profile.local();
        if worker.as_ref().map(|w|&w.worker_id)!=machine.worker_id.as_ref()
            || epoch.len()>profile.bounds.identity_bytes || parse_producer_epoch(&epoch).is_none() {return Err("producer identity differs from host profile".into());}
        let (stop,mut collecting_stop)=watch::channel(false);
        let mut reporting_stop=collecting_stop.clone();
        let (latest,mut pending)=watch::channel::<Option<Sample>>(None);
        let interval=Duration::from_millis(profile.bounds.sample_interval_ms);
        let timeout=Duration::from_millis(profile.bounds.report_timeout_ms);
        let collector=tokio::spawn(async move {
            let mut sequence=0u64;
            loop {
                if *collecting_stop.borrow() {break;}
                let Some(next)=sequence.checked_add(1) else{break;}; sequence=next;
                let sample=collector.collect(&epoch,sequence,worker.clone(),&mut collecting_stop).await;
                if *collecting_stop.borrow() {break;}
                latest.send_replace(Some(sample));
                tokio::select! {biased;_=cancelled(&mut collecting_stop)=>break,_=tokio::time::sleep(interval)=>()}
            }
        });
        let reporter=tokio::spawn(async move {
            loop {
                tokio::select! {biased;_=cancelled(&mut reporting_stop)=>break,result=pending.changed()=>if result.is_err(){break;}}
                let sample=pending.borrow_and_update().clone();
                if let Some(sample)=sample {
                    tokio::select! {biased;_=cancelled(&mut reporting_stop)=>break,_=tokio::time::timeout(timeout,report(sample))=>()}
                }
            }
        });
        Ok(Self {stop,collector,reporter})
    }
    pub async fn shutdown(self) -> Result<(),String> {
        self.stop.send_replace(true);
        let (a,b)=tokio::join!(self.collector,self.reporter);
        a.and(b).map_err(|_|"host observation service stopped unexpectedly".into())
    }
}

/// Host lifecycle order is separate from sample/receiver time. A same-boot
/// process restart has a later kernel start tick; boot replacement requires a
/// fresh registered worker incarnation. No wall timestamp grants freshness.
pub(crate) fn parse_producer_epoch(value:&str)->Option<(&str,u64)> {
    let (boot,start)=value.split_once(':')?;
    if boot.len()!=36 || !boot.bytes().enumerate().all(|(i,c)|if [8,13,18,23].contains(&i){c==b'-'}else{c.is_ascii_hexdigit()&&!c.is_ascii_uppercase()}) {return None;}
    let ticks=start.parse::<u64>().ok().filter(|n|*n>0)?;
    (ticks.to_string()==start).then_some((boot,ticks))
}
/// Report-only epoch; the public read projection exposes only its digest.
pub fn producer_epoch(bound:usize)->Result<String,String> {
    let boot=read_text("/proc/sys/kernel/random/boot_id",bound).map_err(|_|"host producer boot unavailable")?;
    let stat=read_text("/proc/self/stat",bound).map_err(|_|"host producer start unavailable")?;
    let start=stat.rsplit_once(") ").and_then(|(_,t)|t.split_ascii_whitespace().nth(19))
        .and_then(|v|v.parse::<u64>().ok()).ok_or("host producer start unavailable")?;
    let epoch=format!("{}:{start}",boot.trim());
    if parse_producer_epoch(&epoch).is_none(){return Err("host producer epoch invalid".into());}
    Ok(epoch)
}

/// Walk actual typed carriers only. Arbitrary result JSON is never vocabulary.
pub(crate) fn outcome_requires_telemetry(outcome:&crate::protocol::Outcome)->bool {
    use crate::{protocol::{Outcome,Data},collaboration::QueryResult};
    match outcome {
        Outcome::Ok{data:Data::Collaboration(QueryResult::Machines(_)|QueryResult::Machine(_))}=>true,
        Outcome::Ok{data:Data::Collaboration(QueryResult::Call(record))}=>outcome_requires_telemetry(&record.result.reply.outcome),
        Outcome::Ok{data:Data::Operation(record)} if record.kind=="application_mutation"=>record.outcome.as_ref()
            .and_then(|value|serde_json::from_value::<Outcome>(value.clone()).ok()).is_some_and(|o|outcome_requires_telemetry(&o)),
        _=>false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const BOOT:&str="11111111-1111-1111-1111-111111111111";
    fn cpu(values:&str,cores:usize,at:u64)->CpuCounters {
        let mut text=format!("cpu {values}\n");
        for n in 0..cores {text.push_str(&format!("cpu{n} 1 1 1 1 1 1 1 1 0 0\n"));}
        parse_cpu(&text,BOOT,at,64).unwrap()
    }
    #[test]
    fn host_resources_cpu_window_and_counter_reset() {
        let old=cpu("100 20 30 400 10 5 5 0 40 10",4,100);
        let next=cpu("130 30 40 440 20 5 5 0 60 20",4,1100);
        assert!(matches!(cpu_window(None,&old,5000),Metric::WarmingUp{reason:Reason::FirstSample}));
        let Metric::Available{value}=cpu_window(Some(&old),&next,5000) else{panic!("expected measured delta")};
        assert_eq!((value.busy_ticks,value.total_ticks,value.window_ms),(50,100,1000));
        assert_eq!((value.used_core_equivalents,value.percent,value.total_online_logical_cores),(2.0,50.0,4));
        let mut changed=next.clone();changed.boot="22222222-2222-2222-2222-222222222222".into();
        assert!(matches!(cpu_window(Some(&old),&changed,5000),Metric::WarmingUp{reason:Reason::BootChanged}));
        changed=next.clone();changed.cores=3;
        assert!(matches!(cpu_window(Some(&old),&changed,5000),Metric::WarmingUp{reason:Reason::CpuCountChanged}));
        changed=next.clone();changed.counters[0]=1;
        assert!(matches!(cpu_window(Some(&old),&changed,5000),Metric::WarmingUp{reason:Reason::CounterReset}));
        for at in [0,100,5101] {changed=next.clone();changed.at_ms=at;assert!(matches!(cpu_window(Some(&old),&changed,5000),Metric::Unavailable{reason:Reason::InvalidInterval}));}
        changed=old.clone();changed.at_ms=101;
        assert!(matches!(cpu_window(Some(&old),&changed,5000),Metric::Unavailable{reason:Reason::InvalidInterval}));
        let huge=cpu(&format!("{} {} 0 0 0 0 0 0 0 0",u64::MAX,u64::MAX),4,1000);
        let zero=cpu("0 0 0 0 0 0 0 0 0 0",4,0);
        assert!(matches!(cpu_window(Some(&zero),&huge,5000),Metric::Unavailable{reason:Reason::NumericOverflow}));
        assert!(parse_cpu("cpu 1 2 3\ncpu0 1\n",BOOT,1,4).is_err());
        assert!(parse_cpu("cpu 1 2 3 4 5 6 7 8\ncpu0 1\ncpu0 1\n",BOOT,1,4).is_err());
        assert!(parse_cpu("cpu 1 2 3 4 5 6 7 8\ncpu0 1\ncpu1 1\n",BOOT,1,1).is_err());
    }
    #[test]
    fn host_resources_memory_checked_units_and_unavailable() {
        assert_eq!(parse_memory("MemTotal: 1000 kB\nMemAvailable: 250 kB\nCached: 11 kB\n").unwrap(),MemoryUsage{total_bytes:1_024_000,used_bytes:768_000,available_bytes:256_000});
        for text in ["MemTotal: 10 kB\n", "MemAvailable: 1 kB\n", "MemTotal: 10 kB\nMemTotal: 10 kB\nMemAvailable: 0 kB\n", "MemTotal: 10 kB\nMemAvailable: 1 kB\nMemAvailable: 1 kB\n", "MemTotal: 10 kB\nMemAvailable: 11 kB\n", "MemTotal: 10 MB\nMemAvailable: 1 kB\n", "MemTotal: 0 kB\nMemAvailable: 0 kB\n", "MemTotal: 10 kB extra\nMemAvailable: 1 kB\n"] {assert!(parse_memory(text).is_err(),"{text}");}
        assert_eq!(parse_memory(&format!("MemTotal: {} kB\nMemAvailable: 1 kB\n",u64::MAX)),Err(Reason::NumericOverflow));
        assert_eq!(parse_memory("MemTotal: 10 kB\nMemAvailable: 0 kB\n").unwrap().available_bytes,0,"a measured zero is valid only with complete consistent input");
    }
    struct Area(PathBuf);
    impl Area {
        fn new()->Self {
            static NEXT:std::sync::atomic::AtomicU64=std::sync::atomic::AtomicU64::new(1);
            let stamp=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
            let path=std::env::temp_dir().join(format!("borg-machine-test-{}-{stamp}-{}",std::process::id(),NEXT.fetch_add(1,std::sync::atomic::Ordering::Relaxed)));
            std::fs::create_dir(&path).unwrap();Self(path)
        }
    }
    impl Drop for Area {fn drop(&mut self){let _=std::fs::remove_dir_all(&self.0);}}
    fn profile(path:&Path)->Profile {
        let mut p:Profile=serde_json::from_str(include_str!("../metadata/machine-resources.example.json")).unwrap();
        p.storage_scopes.iter_mut().for_each(|s|s.path=path.to_owned());
        let bytes=std::fs::read(STAT_PATH).unwrap();
        p.stat_helper.sha256=aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256,&bytes).as_ref().iter().map(|b|format!("{b:02x}")).collect();
        p
    }
    #[tokio::test]
    async fn host_resources_disk_bounds_mount_identity_and_available() {
        let (_,disk)=parse_disk(b"100 30 20 4096 abc123\n").unwrap();
        assert_eq!(disk,DiskUsage{total_bytes:409600,used_bytes:286720,free_bytes:122880,available_bytes:81920});
        for bytes in [b"100 101 20 4096 abc\n".as_slice(),b"100 30 31 4096 abc\n",b"100 30 20 0 abc\n",b"100 30 20 4096 bad/path\n",b"100 30 20 4096 abc extra\n"] {assert!(parse_disk(bytes).is_err());}
        assert!(parse_disk(format!("{} 0 0 4096 abc\n",u64::MAX).as_bytes()).is_err());
        let area=Area::new();let dir=area.0.join("selected");std::fs::create_dir(&dir).unwrap();
        let p=profile(&dir);p.validate().unwrap();
        let before=directory_identity(&dir,p.bounds.kernel_read_bytes).unwrap();
        assert_eq!(same_directory(&before,&before),Ok(()));
        let mut changed=before.clone();changed.mount+=1;
        assert_eq!(same_directory(&before,&changed),Err(Reason::ScopeChanged));
        std::fs::rename(&dir,area.0.join("original")).unwrap();std::fs::create_dir(&dir).unwrap();
        let after=directory_identity(&dir,p.bounds.kernel_read_bytes).unwrap();
        assert_eq!(same_directory(&before,&after),Err(Reason::ScopeChanged));
        let link=area.0.join("linked");std::os::unix::fs::symlink(&dir,&link).unwrap();
        assert_eq!(directory_identity(&link,p.bounds.kernel_read_bytes),Err(Reason::ScopeUnavailable));
        assert!(!literal_directory(&area.0.join("selected/../original")));
        assert!(!literal_directory(Path::new("/tmp/./selected")));
        let (_stop,mut rx)=watch::channel(false);
        let mut collector=Collector::new(Arc::new(p.clone())).unwrap();
        let sample=collector.collect("11111111-1111-1111-1111-111111111111:1",1,None,&mut rx).await;
        sample.validate(p.local(),&p.bounds).unwrap();
        assert_eq!(sample.filesystems.len(),1,"shared filesystem is projected once, not summed");
        assert_eq!(sample.filesystems[0].scope_labels.len(),3);
        assert!(matches!(sample.filesystems[0].space,Metric::Available{..}));
        let mut invalid=p.clone();invalid.stat_helper.path="/tmp/model-selected-helper".into();assert!(invalid.validate().is_err());
        invalid=p.clone();invalid.stat_helper.sha256="0".repeat(64);
        let failed=Collector::new(Arc::new(invalid)).unwrap().collect("11111111-1111-1111-1111-111111111111:1",2,None,&mut rx).await;
        assert!(failed.filesystems.iter().all(|f|matches!(f.space,Metric::Unavailable{reason:Reason::HelperIdentity})));
        let raw=serde_json::to_string(&sample).unwrap();assert!(!raw.contains(dir.to_str().unwrap()));assert!(!raw.contains(STAT_PATH));
    }
    #[tokio::test]
    async fn host_resources_helper_timeout_oversize_and_cleanup() {
        let area=Area::new();let mut p=profile(&area.0);p.bounds.helper_output_bytes=128;
        for kind in ["timeout","nonzero","malformed","oversize","cancel"] {
            let pidfile=area.0.join(kind);
            let code=format!("import os,sys,time\nopen(sys.argv[1],'w').write(str(os.getpid()))\n{}",match kind {
                "timeout"|"cancel"=>"time.sleep(30)","nonzero"=>"sys.exit(9)","malformed"=>"print('not numeric')",_=>"sys.stdout.write('x'*4096)",
            });
            let args=vec!["-c".into(),code.into(),pidfile.as_os_str().into()];
            let (stop,mut rx)=watch::channel(false);
            let b=p.bounds.clone();
            let deadline=tokio::time::Instant::now()+Duration::from_millis(if kind=="timeout"{500}else{3000});
            let task=tokio::spawn(async move {helper_output(Path::new("/usr/bin/python3"),&args,&b,deadline,&mut rx).await});
            if kind=="cancel" {
                tokio::time::timeout(Duration::from_secs(2),async {while !pidfile.exists(){tokio::time::sleep(Duration::from_millis(2)).await;}}).await.unwrap();
                stop.send_replace(true);
            }
            let result=task.await.unwrap();
            match kind {"timeout"=>assert_eq!(result,Err(Reason::TimedOut)),"nonzero"=>assert_eq!(result,Err(Reason::HelperFailed)),"oversize"=>assert_eq!(result,Err(Reason::HelperOutputBound)),"cancel"=>assert_eq!(result,Err(Reason::Cancelled)),_=>assert_eq!(parse_disk(&result.unwrap()),Err(Reason::InvalidData))}
            let pid=std::fs::read_to_string(&pidfile).unwrap();
            assert!(!Path::new(&format!("/proc/{pid}")).exists(),"helper {kind} must actually be reaped");
        }
        let (stop,mut rx)=watch::channel(true);let _keep=stop;
        assert_eq!(helper_output(Path::new("/usr/bin/python3"),&[],&p.bounds,tokio::time::Instant::now()+Duration::from_secs(1),&mut rx).await,Err(Reason::Cancelled));
        assert_eq!(read_file(&area.0.join("malformed"),0),Err(Reason::ReadBound));
    }
}
