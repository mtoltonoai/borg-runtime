//! Read-only admission of an operator-installed worker cgroup. This never
//! provisions controls or migrates processes. Leaf equality is a ceiling check,
//! not a claim about ancestor capacity, cleanup headroom or workload throughput.
use crate::limits::WorkerLimits;
use serde::Deserialize;
use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions},
    io::Read,
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Component, Path, PathBuf},
    sync::Arc,
};

const O_PATH: i32 = 1 << 21;
const O_NOFOLLOW: i32 = 1 << 17;
const O_DIRECTORY: i32 = 1 << 16;
const O_NONBLOCK: i32 = 1 << 11;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Profile {
    cgroup_path: PathBuf,
    cpu_quota_us: u64,
    cpu_period_us: u64,
    memory_max_bytes: u64,
    swap_max_bytes: u64,
    pids_max: u64,
}
impl Profile {
    fn parse(text: &str, limits: &WorkerLimits) -> Result<Self, String> {
        if text.len() > limits.resource_profile_bytes {
            return Err("worker resource profile exceeds bound (worker.resource_profile_bytes)".into());
        }
        let p: Self = serde_json::from_str(text).map_err(|_| "invalid worker resource profile")?;
        clean_path(&p.cgroup_path, limits)?;
        if p.cpu_quota_us == 0 || p.cpu_period_us == 0 || p.memory_max_bytes == 0 || p.pids_max == 0
        {
            return Err(
                "worker resource ceilings must be finite and positive (swap may be zero)".into(),
            );
        }
        Ok(p)
    }
}

fn clean_path(path: &Path, limits: &WorkerLimits) -> Result<(), String> {
    let text = path.to_str().ok_or("resource path is not UTF-8")?;
    if text.len() > limits.resource_path_bytes {
        return Err("resource path must be a normalized absolute path (worker.resource_path_bytes)".into());
    }
    if !path.is_absolute()
        || text.contains('\0')
        || text
            .split('/')
            .skip(1)
            .any(|p| p == "." || p == ".." || p.is_empty())
            && text != "/"
    {
        return Err("resource path must be a normalized absolute path".into());
    }
    Ok(())
}
fn bounded(mut file: File, cap: usize, field: &'static str) -> Result<String, String> {
    let mut bytes = Vec::new();
    (&mut file)
        .take(cap as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "resource input unreadable")?;
    if bytes.len() > cap {
        return Err(format!("resource input exceeds bound ({field})"));
    }
    String::from_utf8(bytes).map_err(|_| "resource input is not UTF-8".into())
}
fn proc_read(path: impl AsRef<Path>, cap: usize, field: &'static str) -> Result<String, String> {
    bounded(
        File::open(path).map_err(|_| "resource kernel metadata unavailable")?,
        cap,
        field,
    )
}
fn fd_path(file: &File) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()))
}
fn same(a: &File, b: &File) -> Result<bool, String> {
    let a = a.metadata().map_err(|_| "resource identity unavailable")?;
    let b = b.metadata().map_err(|_| "resource identity unavailable")?;
    Ok(a.dev() == b.dev() && a.ino() == b.ino())
}
// Walk each real directory component using the retained parent FD. O_NOFOLLOW
// on just the final absolute path would still allow a symlink ancestor.
fn directory(path: &Path, limits: &WorkerLimits) -> Result<File, String> {
    clean_path(path, limits)?;
    let open = |p: &Path| {
        OpenOptions::new()
            .read(true)
            .custom_flags(O_PATH | O_NOFOLLOW | O_DIRECTORY)
            .open(p)
            .map_err(|_| "resource directory is unavailable or aliased".to_owned())
    };
    let mut file = open(Path::new("/"))?;
    for part in path.components() {
        if let Component::Normal(name) = part {
            file = open(&fd_path(&file).join(name))?;
        }
    }
    Ok(file)
}
fn number(text: &str) -> Result<u64, String> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return Err("resource control is not a finite unsigned integer".into());
    }
    text.parse().map_err(|_| "resource control overflow".into())
}
fn fd_mount(text: &str) -> Result<u64, String> {
    if !text.ends_with('\n') {
        return Err("incomplete resource fd metadata".into());
    }
    let mut found = None;
    for line in text.lines() {
        if let Some(v) = line.strip_prefix("mnt_id:") {
            let v = number(v.trim())?;
            if v == 0 || found.replace(v).is_some() {
                return Err("ambiguous resource mount id".into());
            }
        }
    }
    found.ok_or_else(|| "resource mount id missing".into())
}
fn mount_id(file: &File, limits: &WorkerLimits) -> Result<u64, String> {
    fd_mount(&proc_read(
        format!("/proc/self/fdinfo/{}", file.as_raw_fd()),
        limits.resource_line_bytes,
        "worker.resource_line_bytes",
    )?)
}
fn unescape(text: &str, limits: &WorkerLimits) -> Result<PathBuf, String> {
    let bytes = text.as_bytes();
    let mut result = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'\\' {
            result.push(bytes[i]);
            i += 1;
            continue;
        }
        let escape = bytes
            .get(i + 1..i + 4)
            .ok_or("malformed mount path escape")?;
        result.push(match escape {
            b"040" => b' ',
            b"011" => b'\t',
            b"012" => b'\n',
            b"134" => b'\\',
            _ => return Err("unknown mount path escape".into()),
        });
        i += 4;
    }
    let path = PathBuf::from(String::from_utf8(result).map_err(|_| "mount path is not UTF-8")?);
    clean_path(&path, limits)?;
    Ok(path)
}
#[derive(Debug, PartialEq)]
struct Mount {
    id: u64,
    root: PathBuf,
    location: PathBuf,
}
fn mount_entry(text: &str, selected: u64, limits: &WorkerLimits) -> Result<Mount, String> {
    if text.len() > limits.resource_proc_bytes {
        return Err("incomplete or oversized resource mount table (worker.resource_proc_bytes)".into());
    }
    if !text.ends_with('\n') {
        return Err("incomplete or oversized resource mount table".into());
    }
    let mut ids = BTreeSet::new();
    let mut found = None;
    for (i, line) in text.lines().enumerate() {
        if i >= limits.resource_lines {
            return Err("resource mount table exceeds bound (worker.resource_lines)".into());
        }
        if line.len() > limits.resource_line_bytes {
            return Err("resource mount table exceeds bound (worker.resource_line_bytes)".into());
        }
        let fields: Vec<_> = line.split_ascii_whitespace().collect();
        if fields.len() < 10 {
            return Err("malformed resource mount entry".into());
        }
        let id = number(fields[0])?;
        if id == 0 || !ids.insert(id) {
            return Err("duplicate resource mount id".into());
        }
        let separator = fields
            .iter()
            .position(|v| *v == "-")
            .ok_or("missing mount separator")?;
        if separator < 6 || fields.len() != separator + 4 {
            return Err("malformed mount separator".into());
        }
        if id == selected {
            if fields[separator + 1] != "cgroup2" {
                return Err("resource directory is not a genuine cgroup2 mount".into());
            }
            found = Some(Mount {
                id,
                root: unescape(fields[3], limits)?,
                location: unescape(fields[4], limits)?,
            });
        }
    }
    found.ok_or_else(|| "opened resource mount is absent from mount table".into())
}
fn mount(file: &File, limits: &WorkerLimits) -> Result<Mount, String> {
    let before = mount_id(file, limits)?;
    let selected = mount_entry(
        &proc_read("/proc/self/mountinfo", limits.resource_proc_bytes, "worker.resource_proc_bytes")?,
        before,
        limits,
    )?;
    if mount_id(file, limits)? != before {
        return Err("resource mount changed".into());
    }
    Ok(selected)
}
fn membership(text: &str, limits: &WorkerLimits) -> Result<PathBuf, String> {
    if text.len() > limits.resource_line_bytes {
        return Err("invalid cgroup membership (worker.resource_line_bytes)".into());
    }
    if !text.ends_with('\n') {
        return Err("invalid cgroup membership".into());
    }
    let mut current = None;
    for line in text.lines() {
        let mut parts = line.splitn(3, ':');
        let id = parts.next().ok_or("invalid cgroup membership")?;
        let controllers = parts.next().ok_or("invalid cgroup membership")?;
        let path = parts.next().ok_or("invalid cgroup membership")?;
        number(id)?;
        if id == "0" {
            if !controllers.is_empty() || current.is_some() {
                return Err("ambiguous unified cgroup membership".into());
            }
            let path = PathBuf::from(path);
            clean_path(&path, limits)?;
            current = Some(path);
        }
    }
    current.ok_or_else(|| "unified cgroup membership unavailable".into())
}
fn binds(mount: &Mount, path: &Path, member: &Path) -> Result<(), String> {
    let relative = path
        .strip_prefix(&mount.location)
        .map_err(|_| "cgroup directory is outside its mount")?;
    if mount.root.join(relative) != member {
        return Err("worker is not in the configured cgroup".into());
    }
    Ok(())
}

pub(super) struct Envelope {
    limits: Arc<WorkerLimits>,
    profile: Profile,
    directory: File,
    mount: Mount,
}
impl Envelope {
    fn control(&self, name: &str) -> Result<String, String> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(O_NOFOLLOW | O_NONBLOCK)
            .open(fd_path(&self.directory).join(name))
            .map_err(|_| "resource control unavailable or aliased")?;
        if !file
            .metadata()
            .map_err(|_| "resource control metadata unavailable")?
            .is_file()
            || mount_id(&file, &self.limits)? != self.mount.id
        {
            return Err("resource control is not on the admitted cgroup mount".into());
        }
        let text = bounded(file, self.limits.resource_control_bytes, "worker.resource_control_bytes")?;
        if !text.ends_with('\n') {
            return Err("incomplete resource control".into());
        }
        Ok(text)
    }
    pub(super) fn verify(&self) -> Result<(), String> {
        let member = membership(
            &proc_read("/proc/self/cgroup", self.limits.resource_line_bytes, "worker.resource_line_bytes")?,
            &self.limits,
        )?;
        let opened = directory(&self.profile.cgroup_path, &self.limits)?;
        if !same(&opened, &self.directory)? || mount(&opened, &self.limits)? != self.mount {
            return Err("configured cgroup identity changed".into());
        }
        binds(&self.mount, &self.profile.cgroup_path, &member)?;
        if self.control("cgroup.type")? != "domain\n" {
            return Err("worker requires a domain cgroup".into());
        }
        let cpu = self.control("cpu.max")?;
        let cpu: Vec<_> = cpu.split_ascii_whitespace().collect();
        if cpu.len() != 2
            || number(cpu[0])? != self.profile.cpu_quota_us
            || number(cpu[1])? != self.profile.cpu_period_us
            || number(self.control("memory.max")?.trim())? != self.profile.memory_max_bytes
            || number(self.control("memory.swap.max")?.trim())? != self.profile.swap_max_bytes
            || number(self.control("pids.max")?.trim())? != self.profile.pids_max
        {
            return Err("worker resource controls differ from configured finite ceilings".into());
        }
        if membership(
            &proc_read("/proc/self/cgroup", self.limits.resource_line_bytes, "worker.resource_line_bytes")?,
            &self.limits,
        )? != member
            || !same(
                &directory(&self.profile.cgroup_path, &self.limits)?,
                &self.directory,
            )?
        {
            return Err("worker resource binding changed during admission".into());
        }
        Ok(())
    }
}
pub(super) fn verify(envelope: &Option<Arc<Envelope>>) -> Result<(), String> {
    if let Some(envelope) = envelope {
        envelope.verify()?;
    }
    Ok(())
}
// Traceability scope: A configured profile becomes an envelope only after genuine cgroup identity, worker membership and finite controls are verified; registration also rechecks the envelope.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//# A worker MUST verify the configured host controls before claiming that those controls are enforced.
pub(super) fn load(
    path: Option<&Path>,
    limits: Arc<WorkerLimits>,
) -> Result<Option<Arc<Envelope>>, String> {
    let Some(path) = path else {
        return Ok(None);
    };
    // Relative operator filenames are fine; pin the opened regular file once.
    // Neither task data nor later profile rewrites can change this incarnation.
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(O_NOFOLLOW | O_NONBLOCK)
        .open(path)
        .map_err(|_| "worker resource profile unavailable or aliased")?;
    if !file
        .metadata()
        .map_err(|_| "resource profile metadata unavailable")?
        .is_file()
    {
        return Err("worker resource profile must be a regular file".into());
    }
    let profile = Profile::parse(&bounded(file, limits.resource_profile_bytes, "worker.resource_profile_bytes")?, &limits)?;
    let directory = directory(&profile.cgroup_path, &limits)?;
    let mount = mount(&directory, &limits)?;
    let envelope = Arc::new(Envelope {
        limits,
        profile,
        directory,
        mount,
    });
    envelope.verify()?;
    Ok(Some(envelope))
}

#[cfg(test)]
mod tests;
