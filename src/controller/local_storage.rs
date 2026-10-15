//! Linux local-filesystem admission, not a physical-media or power-loss proof.
//! Sources: docs.kernel.org/filesystems/proc.html sections 3.5 and 3.8;
//! sqlite.org/wal.html (WAL is unsupported on network filesystems).
use crate::limits::ControllerLimits;
use std::{
    collections::HashSet,
    fs::{self, File, Metadata, OpenOptions},
    io::Read,
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
};

// Selected Linux x86_64 UAPI values from include/uapi/asm-generic/fcntl.h.
// O_PATH descriptors do not trigger locks_remove_posix on close (fs/open.c).
// std performs the syscall; no unsafe FFI or optional dependency is introduced.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn metadata_file(path: &Path) -> Result<File, String> {
    const O_PATH: i32 = 1 << 21;
    const O_NOFOLLOW: i32 = 1 << 17;
    OpenOptions::new()
        .read(true)
        .custom_flags(O_PATH | O_NOFOLLOW)
        .open(path)
        .map_err(|e| e.to_string())
}
#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
fn metadata_file(_: &Path) -> Result<File, String> {
    Err("local filesystem admission is unqualified on this target".into())
}


fn bounded_read(mut source: impl Read, limit: usize, field: &str) -> Result<String, String> {
    let mut bytes = Vec::new();
    source
        .by_ref()
        .take((limit + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| "local storage mount metadata read failed")?;
    if bytes.len() > limit {
        return Err(format!("local storage mount metadata exceeds bound: {field}"));
    }
    String::from_utf8(bytes).map_err(|_| "invalid local storage mount metadata encoding".into())
}
fn number(s: &str) -> Result<u64, String> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err("malformed local storage mount identity".into());
    }
    s.parse()
        .map_err(|_| "local storage mount identity overflow".into())
}
fn fd_mount_id(limits: &ControllerLimits, text: &str) -> Result<u64, String> {
    if text.len() > limits.fdinfo_bytes { return Err("incomplete or oversized fd mount metadata: controller.fdinfo_bytes".into()); }
    if !text.ends_with('\n') { return Err("incomplete or oversized fd mount metadata".into()); }
    let mut id = None;
    for (index, line) in text.lines().enumerate() {
        if index >= limits.fdinfo_lines { return Err("fd metadata bound: controller.fdinfo_lines".into()); }
        if line.len() > limits.mountinfo_line_bytes { return Err("fd metadata bound: controller.mountinfo_line_bytes".into()); }
        let (key, value) = line.split_once(':').ok_or("malformed fd metadata")?;
        if key == "mnt_id" {
            let parsed = number(value.trim())?;
            if parsed == 0 || id.replace(parsed).is_some() {
                return Err("duplicate or invalid fd mount identity".into());
            }
        }
    }
    id.ok_or_else(|| "missing fd mount identity".into())
}

#[derive(Debug, PartialEq, Eq)]
struct Mount {
    id: u64,
    filesystem: String,
}
fn mount_entry(limits: &ControllerLimits, text: &str, wanted: u64) -> Result<Mount, String> {
    if text.len() > limits.mountinfo_bytes { return Err("incomplete or oversized mount table: controller.mountinfo_bytes".into()); }
    if !text.ends_with('\n') { return Err("incomplete or oversized mount table".into()); }
    let mut seen = HashSet::new();
    let mut found = None;
    for (index, line) in text.lines().enumerate() {
        if index >= limits.mountinfo_mounts { return Err("mount table bound: controller.mountinfo_mounts".into()); }
        if line.len() > limits.mountinfo_line_bytes { return Err("mount table bound: controller.mountinfo_line_bytes".into()); }
        let fields: Vec<_> = line.split_ascii_whitespace().take(limits.mountinfo_fields + 1).collect();
        if fields.len() > limits.mountinfo_fields { return Err("malformed mount table record: controller.mountinfo_fields".into()); }
        if fields.len() < 10 { return Err("malformed mount table record".into()); }
        let id = number(fields[0])?;
        if id == 0 || !seen.insert(id) {
            return Err("duplicate or invalid mount table identity".into());
        }
        number(fields[1])?;
        let (major, minor) = fields[2].split_once(':').ok_or("malformed mount device")?;
        number(major)?;
        number(minor)?;
        if !fields[3].starts_with('/') || !fields[4].starts_with('/') {
            return Err("malformed mount root/location".into());
        }
        let separator = fields
            .iter()
            .position(|v| *v == "-")
            .ok_or("missing mount separator")?;
        if separator < 6 || fields.len() != separator + 4 {
            return Err("malformed mount fields".into());
        }
        // Unknown optional fields before '-' are intentionally ignored, as the
        // kernel interface requires. Paths/source labels are never selectors.
        if id == wanted {
            found = Some(Mount {
                id,
                filesystem: fields[separator + 1].to_owned(),
            });
        }
    }
    let found = found.ok_or("opened file's mount is absent from mount table")?;
    // Native block-filesystem families with local VFS/locking semantics. Only
    // the current ext4 deployment is exercised here; recognizing a family is
    // not qualification of every mount option, device or deployment.
    if !matches!(found.filesystem.as_str(), "ext4" | "xfs" | "btrfs") {
        return Err("controller storage requires a recognized local filesystem (ext4, xfs or btrfs); network, FUSE, overlay, memory and unverified filesystems are refused".into());
    }
    Ok(found)
}
fn mount(limits: &ControllerLimits, file: &File) -> Result<Mount, String> {
    let fdinfo = format!("/proc/self/fdinfo/{}", file.as_raw_fd());
    let before = fd_mount_id(limits, &bounded_read(
        File::open(&fdinfo).map_err(|_| "fd mount metadata unavailable")?,
        limits.fdinfo_bytes, "controller.fdinfo_bytes",
    )?)?;
    let table = bounded_read(
        File::open("/proc/self/mountinfo").map_err(|_| "mount table unavailable")?,
        limits.mountinfo_bytes, "controller.mountinfo_bytes",
    )?;
    let result = mount_entry(limits, &table, before)?;
    let after = fd_mount_id(limits, &bounded_read(
        File::open(fdinfo).map_err(|_| "fd mount metadata unavailable")?,
        limits.fdinfo_bytes, "controller.fdinfo_bytes",
    )?)?;
    if before != after {
        return Err("opened mount changed during local storage admission".into());
    }
    Ok(result)
}
fn same(a: &Metadata, b: &Metadata) -> bool {
    a.dev() == b.dev() && a.ino() == b.ino()
}

pub(super) struct LocalStorage {
    limits: ControllerLimits,
    directory: File,
    path: PathBuf,
    mount: Mount,
}
impl LocalStorage {
    //= spec/impl/v2/implementation.md#accepted-state-has-one-durable-owner
    //# The controller's database MUST reside on local storage.
    pub fn admit(database: &Path, limits: &ControllerLimits) -> Result<Self, String> {
        let path = database
            .parent()
            .ok_or("missing database parent")?
            .to_owned();
        let metadata = fs::symlink_metadata(&path).map_err(|e| e.to_string())?;
        if !metadata.is_dir() {
            return Err("database parent is not a real directory".into());
        }
        let directory = metadata_file(&path)?;
        if !same(&metadata, &directory.metadata().map_err(|e| e.to_string())?) {
            return Err("database parent changed while opening".into());
        }
        let storage = Self {
            limits: limits.clone(),
            mount: mount(limits, &directory)?,
            directory,
            path,
        };
        // Do this before creating even the controller lock. WAL, SHM and the
        // rollback journal all use SQLite's same-directory basename suffixes.
        for suffix in ["", "-wal", "-shm", "-journal", ".controller-lock"] {
            let mut name = database.as_os_str().to_os_string();
            name.push(suffix);
            storage.existing_file(Path::new(&name))?;
        }
        storage.recheck_parent()?;
        Ok(storage)
    }
    fn existing_file(&self, path: &Path) -> Result<(), String> {
        let metadata = match fs::symlink_metadata(path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.to_string()),
        };
        if !metadata.is_file() || metadata.nlink() != 1 {
            return Err(
                "database/sidecar must be regular without symlink or hardlink aliases".into(),
            );
        }
        // O_PATH is necessary even before this controller opens SQLite: an
        // existing owner in this process may hold POSIX locks on the same DB.
        let file = metadata_file(path)?;
        if !same(&metadata, &file.metadata().map_err(|e| e.to_string())?)
            || !same(
                &metadata,
                &fs::symlink_metadata(path).map_err(|e| e.to_string())?,
            )
        {
            return Err("database/sidecar path changed during admission".into());
        }
        self.check_file(&file)
    }
    pub fn check_file(&self, file: &File) -> Result<(), String> {
        let limits = &self.limits;
        if mount(limits, file)? != self.mount {
            return Err("database/sidecar/lock differs from admitted directory mount".into());
        }
        Ok(())
    }
    pub fn before_sqlite(&self, database: &Path) -> Result<(), String> {
        self.recheck_parent()?;
        for suffix in ["", "-wal", "-shm", "-journal"] {
            let mut name = database.as_os_str().to_os_string();
            name.push(suffix);
            self.existing_file(Path::new(&name))?;
        }
        Ok(())
    }
    pub fn recheck_parent(&self) -> Result<(), String> {
        let limits = &self.limits;
        let current = fs::symlink_metadata(&self.path).map_err(|e| e.to_string())?;
        if !current.is_dir()
            || !same(
                &current,
                &self.directory.metadata().map_err(|e| e.to_string())?,
            )
            || mount(limits, &self.directory)? != self.mount
        {
            return Err("database directory changed during admission".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: u64, filesystem: &str) -> String {
        format!(
            "{id} 1 8:1 / /data rw,relatime shared:7 future-tag:value - {filesystem} /dev/example rw\n"
        )
    }
    #[test]
    fn mount_join_uses_opened_id_not_path_or_source_labels() {
        let limits = &crate::limits::RuntimeLimits::seed().expect("test metadata").controller;
        // A deeper-looking path and an ext4-looking source string do not change
        // the kernel mount ID's actual filesystem. Optional fields stay opaque.
        let table = "19 1 0:4 / /data rw - nfs ext4 rw\n20 1 8:1 / /data/deep rw future:9 - ext4 remote-label rw\n";
        assert!(mount_entry(limits, table, 19).is_err());
        assert_eq!(mount_entry(limits, table, 20).unwrap().filesystem, "ext4");
        assert!(mount_entry(limits, table, 21).is_err());
        let escaped =
            "22 1 8:1 /escaped\\040root /data\\040path rw unknown:value - ext4 /dev/disk rw\n";
        assert_eq!(mount_entry(limits, escaped, 22).unwrap().id, 22);
    }
    #[test]
    fn mount_admission_refuses_network_memory_overlay_and_unknown_families() {
        let limits = &crate::limits::RuntimeLimits::seed().expect("test metadata").controller;
        for family in [
            "nfs",
            "nfs4",
            "cifs",
            "smb3",
            "9p",
            "ceph",
            "afs",
            "fuse",
            "fuse.sshfs",
            "fuseblk",
            "overlay",
            "tmpfs",
            "ramfs",
            "proc",
            "new-filesystem",
        ] {
            assert!(mount_entry(limits, &entry(19, family), 19).is_err(), "{family}");
        }
        // Parser/semantic admission only: no xfs/btrfs deployment was mounted or
        // qualified by this data test.
        for family in ["ext4", "xfs", "btrfs"] {
            assert_eq!(
                mount_entry(limits, &entry(19, family), 19).unwrap().filesystem,
                family
            );
        }
    }
    #[test]
    fn mount_metadata_refuses_malformed_duplicate_and_missing_identities() {
        let limits = &crate::limits::RuntimeLimits::seed().expect("test metadata").controller;
        assert_eq!(
            fd_mount_id(limits, "pos:\t0\nflags:\t012000000\nmnt_id:\t19\nino:\t4\n").unwrap(),
            19
        );
        for data in [
            "",
            "mnt_id: 19",
            "pos: 0\n",
            "mnt_id: 19\nmnt_id: 19\n",
            "mnt_id: -1\n",
            "mnt_id: 0\n",
            "mnt_id: 19 20\n",
            "mnt_id: 18446744073709551616\n",
            "mnt_id 19\n",
        ] {
            assert!(fd_mount_id(limits, data).is_err(), "{data:?}");
        }
        for data in [
            "",
            "19 1 8:1 / /data rw - ext4 dev rw",
            "19 1 8:1 / /data rw ext4 dev rw\n",
            "19 1 8:1 / /data rw - ext4 dev\n",
            "19 1 wrong / /data rw - ext4 dev rw\n",
            "19 1 8:1 relative /data rw - ext4 dev rw\n",
            "19 1 8:1 / /data rw - ext4 dev rw extra\n",
        ] {
            assert!(mount_entry(limits, data, 19).is_err(), "{data:?}");
        }
        assert!(mount_entry(limits, &(entry(19, "ext4") + &entry(19, "ext4")), 19).is_err());
        assert!(
            mount_entry(limits, 
                &(entry(19, "ext4") + &entry(20, "xfs") + &entry(20, "xfs")),
                19
            )
            .is_err()
        );
    }
    #[test]
    fn mount_metadata_reads_and_parsing_have_finite_bounds() {
        let limits = &crate::limits::RuntimeLimits::seed().expect("test metadata").controller;
        assert!(bounded_read(&b"12345"[..], 4, "controller.fdinfo_bytes").is_err());
        assert_eq!(bounded_read(&b"1234"[..], 4, "controller.fdinfo_bytes").unwrap(), "1234");
        assert!(bounded_read(&b"\xff"[..], 1, "controller.fdinfo_bytes").is_err());
        assert!(fd_mount_id(limits, &format!("mnt_id: 19\nother:{}\n", "x".repeat(limits.fdinfo_bytes))).is_err());
        assert!(
            mount_entry(limits, 
                &format!("19 1 8:1 / /{} rw - ext4 dev rw\n", "x".repeat(limits.mountinfo_line_bytes)),
                19
            )
            .is_err()
        );
        assert!(
            mount_entry(limits, 
                &format!(
                    "19 1 8:1 / /data rw {} - ext4 dev rw\n",
                    "tag:x ".repeat(limits.mountinfo_fields)
                ),
                19
            )
            .is_err()
        );
        let table: String = (1..=limits.mountinfo_mounts + 1)
            .map(|id| entry(id as u64, "ext4"))
            .collect();
        assert!(mount_entry(limits, &table, 1).is_err());
    }
    #[test]
    fn actual_opened_directory_is_joined_to_its_current_ext4_mount() {
        let limits = &crate::limits::RuntimeLimits::seed().expect("test metadata").controller;
        let directory = metadata_file(Path::new(env!("CARGO_MANIFEST_DIR"))).unwrap();
        let current = mount(limits, &directory).unwrap();
        assert_eq!(
            current.filesystem, "ext4",
            "this acceptance selects the actual current ext4 host"
        );
        // No fixed numeric mount ID is configured or persisted across boots.
        assert!(current.id > 0);
        let target = Path::new(env!("CARGO_MANIFEST_DIR")).join("local-storage-never-created.db");
        let admitted = LocalStorage::admit(&target, &crate::limits::RuntimeLimits::seed().unwrap().controller).unwrap();
        assert_eq!(admitted.mount, current);
        assert!(!target.exists());
        // This host's existing /tmp and runtime mounts are separately mounted
        // ext4. A matching family alone must not authorize a sidecar elsewhere.
        let other = metadata_file(Path::new("/tmp")).unwrap();
        let other_mount = mount(limits, &other).unwrap();
        assert_eq!(other_mount.filesystem, "ext4");
        assert_ne!(other_mount.id, current.id);
        assert!(admitted.check_file(&other).is_err());
    }
}
