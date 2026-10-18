//! The controller's local process registry belongs to one durable database.
//! The persistent lock inode is never unlinked. Its last real holder releases
//! ownership, including a native helper awaiting observed cleanup.
use crate::limits::ControllerLimits;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, Metadata, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

pub(super) struct Tracking {
    pub path: PathBuf,
    pub registry: PathBuf,
    pub lock: Arc<File>,
    lock_path: PathBuf,
    storage: super::local_storage::LocalStorage,
    original: Root,
    database: Option<Identity>,
    limits: ControllerLimits,
}

#[derive(PartialEq, Eq)]
struct Root {
    database: Option<Identity>,
    binding: Option<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Binding {
    revision: u32,
    database_id: String,
    database_path: PathBuf,
    registry_path: PathBuf,
    database: Identity,
    registry: Identity,
    lock: Identity,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Identity {
    device: u64,
    inode: u64,
}

fn suffixed(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    name.into()
}

fn present(path: &Path) -> Result<bool, String> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.to_string()),
    }
}

fn marker_bytes(limits: &ControllerLimits, registry: &Path) -> Result<String, String> {
    let marker = registry.join("owner.binding");
    regular(&marker)?;
    let mut bytes = Vec::new();
    File::open(marker)
        .map_err(|e| e.to_string())?
        .take((limits.owner_binding_bytes as u64).saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() > limits.owner_binding_bytes {
        return Err("oversized helper registry binding: controller.owner_binding_bytes".into());
    }
    String::from_utf8(bytes).map_err(|_| "invalid helper registry binding encoding".into())
}

// This pre-lock inspection must not open a normal descriptor for the DB:
// closing one could release another same-process SQLite owner's POSIX locks.
// Scope: refuse a lost/partial known local root; no restore or reconstruction.
//= spec/impl/v2/implementation.md#backups-are-outside-the-initial-milestone
//# A recovery procedure MUST NOT fabricate previously committed tasks when their storage has been lost.
fn inspect_root(
    limits: &ControllerLimits,
    path: &Path,
    registry: &Path,
    lock_path: &Path,
    fresh_lock: Option<&File>,
) -> Result<Root, String> {
    if !present(path)? {
        let owned_fresh_lock = if let Some(lock) = fresh_lock {
            Identity::of(&regular(lock_path)?)
                == Identity::of(&lock.metadata().map_err(|e| e.to_string())?)
        } else {
            false
        };
        if present(registry)?
            || (present(lock_path)? && !owned_fresh_lock)
            || ["-wal", "-shm", "-journal"]
                .iter()
                .any(|suffix| present(&suffixed(path, suffix)).unwrap_or(true))
        {
            return Err("controller database is missing from a bound or partial root; original sidecars retained".into());
        }
        return Ok(Root {
            database: None,
            binding: None,
        });
    }
    let database = regular(path)?;
    // Only a file created by this initialization can start empty. Existing
    // empty/short files, bound or not, are not fresh initialization requests.
    if database.len() < 100 {
        return Err("controller database is empty or truncated; original sidecars retained".into());
    }
    let binding = if present(registry)? {
        let directory = fs::symlink_metadata(registry).map_err(|e| e.to_string())?;
        if !directory.is_dir() || directory.file_type().is_symlink() {
            return Err("controller helper registry is not its original directory".into());
        }
        let encoded = marker_bytes(limits, registry)?;
        let expected: Binding = serde_json::from_str(&encoded).map_err(|e| e.to_string())?;
        if serde_json::to_string(&expected).map_err(|e| e.to_string())? != encoded
            || expected.revision != 1
            || expected.database_path != path
            || expected.registry_path != registry
            || expected.database != Identity::of(&database)
            || expected.registry != Identity::of(&directory)
            || expected.lock != Identity::of(&regular(lock_path)?)
        {
            return Err(
                "controller database/helper registry identity mismatch before SQLite".into(),
            );
        }
        Some(encoded)
    } else {
        // Current stores always carry their durable helper association. Do
        // not open an unbound/partial predecessor and let SQLite recover or
        // discard its WAL before the inevitable unsupported-store refusal.
        return Err(
            "missing durable helper registry; migration required; original data retained".into(),
        );
    };
    Ok(Root {
        database: Some(Identity::of(&database)),
        binding,
    })
}
impl Identity {
    fn of(m: &Metadata) -> Self {
        Self {
            device: m.dev(),
            inode: m.ino(),
        }
    }
}

fn regular(path: &Path) -> Result<Metadata, String> {
    let m = fs::symlink_metadata(path).map_err(|e| e.to_string())?;
    if !m.is_file() || m.nlink() != 1 {
        return Err("controller file must be regular, without symlink/hardlink aliases".into());
    }
    Ok(m)
}

/// Resolve ordinary relative paths, while refusing actual symlink aliases in
/// every existing component. No process-wide current-directory change occurs.
fn canonical_location(path: &Path) -> Result<PathBuf, String> {
    if path.as_os_str().is_empty()
        || path == Path::new(":memory:")
        || path.to_string_lossy().starts_with("file:")
    {
        return Err("controller requires a durable local database path".into());
    }
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()
            .map_err(|e| e.to_string())?
            .join(path)
    };
    let mut resolved = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::RootDir => resolved.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                resolved.pop();
            }
            Component::Normal(name) => {
                resolved.push(name);
                match fs::symlink_metadata(&resolved) {
                    Ok(m) if m.file_type().is_symlink() => {
                        return Err("controller path contains a symlink alias".into());
                    }
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.to_string()),
                }
            }
            Component::Prefix(_) => return Err("unsupported controller path".into()),
        }
    }
    let parent = resolved.parent().ok_or("missing database parent")?;
    let canonical_parent = fs::canonicalize(parent).map_err(|e| e.to_string())?;
    if canonical_parent.as_os_str() != parent.as_os_str() || resolved.file_name().is_none() {
        return Err("controller parent path changed during resolution".into());
    }
    match fs::symlink_metadata(&resolved) {
        Ok(_) => {
            regular(&resolved)?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.to_string()),
    }
    Ok(resolved)
}

impl Tracking {
    pub fn acquire(path: &Path, limits: &ControllerLimits) -> Result<Self, String> {
        Self::acquire_cancellable(path, limits, &super::StartupCancellation::default())
    }
    pub(super) fn acquire_cancellable(path: &Path, limits: &ControllerLimits,
        cancellation: &super::StartupCancellation) -> Result<Self, String> {
        cancellation.check()?;
        let path = canonical_location(path)?;
        let storage = super::local_storage::LocalStorage::admit(&path, limits)?;
        let lock_path = suffixed(&path, ".controller-lock");
        let registry = suffixed(&path, ".native-helpers");
        // Refuse known store loss before creating a lock, DB, or SQLite sidecar.
        let original = inspect_root(limits, &path, &registry, &lock_path, None)?;
        let existing_lock = present(&lock_path)?;
        match fs::symlink_metadata(&lock_path) {
            Ok(_) => {
                regular(&lock_path)?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.to_string()),
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(!existing_lock)
            .truncate(false)
            .mode(0o600)
            .open(&lock_path)
            .map_err(|e| e.to_string())?;
        storage.check_file(&lock)?;
        let until = Instant::now() + Duration::from_millis(limits.owner_lock_wait_ms);
        loop {
            cancellation.check()?;
            // Only our already-owned ManagedProcess handles are observed here.
            // Reading another controller's on-disk registry is forbidden until
            // the exclusive owner and its durable association are established.
            crate::native::validation_cleanup_pending();
            match lock.try_lock() {
                Ok(()) => break,
                Err(std::fs::TryLockError::WouldBlock) if Instant::now() < until => {
                    cancellation.wait(Duration::from_millis(limits.owner_lock_retry_ms)
                        .min(until.saturating_duration_since(Instant::now())))?
                }
                Err(std::fs::TryLockError::WouldBlock) => return Err("controller owner lock unavailable: controller.owner_lock_wait_ms".into()),
                Err(e) => return Err(format!("controller owner lock unavailable: {e}")),
            }
        }
        if Identity::of(&regular(&lock_path)?)
            != Identity::of(&lock.metadata().map_err(|e| e.to_string())?)
        {
            return Err("controller lock path changed".into());
        }
        let fresh_lock = original.database.is_none().then_some(&lock);
        if inspect_root(limits, &path, &registry, &lock_path, fresh_lock)? != original {
            return Err("controller root changed while acquiring ownership".into());
        }
        let database = original.database.clone();
        Ok(Self {
            path,
            registry,
            lock: Arc::new(lock),
            lock_path,
            storage,
            original,
            database,
            limits: limits.clone(),
        })
    }

    pub fn before_sqlite(&mut self) -> Result<(), String> {
        self.storage.before_sqlite(&self.path)?;
        if present(&suffixed(&self.path, "-journal"))? {
            // The selected runtime uses WAL. A rollback journal may require
            // SQLite recovery writes before the first schema query; do not
            // run that recovery while deciding whether this store is usable.
            return Err("rollback-journal recovery is unsupported at controller admission; original data retained".into());
        }
        let fresh_lock = self
            .original
            .database
            .is_none()
            .then_some(self.lock.as_ref());
        if inspect_root(&self.limits, &self.path, &self.registry, &self.lock_path, fresh_lock)? != self.original {
            return Err("controller root changed before SQLite open".into());
        }
        if self.original.database.is_none() {
            // Only a root observed wholly fresh, still fresh under ownership,
            // may create this exact file. SQLite itself never has CREATE.
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&self.path)
                .map_err(|e| e.to_string())?;
            self.storage.check_file(&file)?;
            self.database = Some(Identity::of(&file.metadata().map_err(|e| e.to_string())?));
        }
        self.check_database_identity()
    }

    pub fn fresh(&self) -> bool {
        self.original.database.is_none()
    }

    pub fn check_database_identity(&self) -> Result<(), String> {
        if self.database.as_ref() != Some(&Identity::of(&regular(&self.path)?)) {
            return Err("controller database path changed; replacement refused".into());
        }
        Ok(())
    }

    fn binding(&self, database_id: &str) -> Result<Binding, String> {
        let registry = fs::symlink_metadata(&self.registry).map_err(|e| e.to_string())?;
        if !registry.is_dir() || registry.file_type().is_symlink() {
            return Err("controller helper registry is not its original directory".into());
        }
        let lock = regular(&self.lock_path)?;
        if Identity::of(&lock) != Identity::of(&self.lock.metadata().map_err(|e| e.to_string())?) {
            return Err("controller lock identity changed".into());
        }
        Ok(Binding {
            revision: 1,
            database_id: database_id.into(),
            database_path: self.path.clone(),
            registry_path: self.registry.clone(),
            database: Identity::of(&regular(&self.path)?),
            registry: Identity::of(&registry),
            lock: Identity::of(&lock),
        })
    }

    /// Called inside the fresh schema transaction, after SQLite exclusion. A
    /// pre-existing or partly initialized registry is never adopted as empty.
    pub fn create(&self, database_id: &str) -> Result<String, String> {
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&self.registry)
            .map_err(|e| format!("fresh helper registry unavailable: {e}"))?;
        let encoded =
            serde_json::to_string(&self.binding(database_id)?).map_err(|e| e.to_string())?;
        if encoded.len() > self.limits.owner_binding_bytes { return Err("helper registry binding exceeds selected limit: controller.owner_binding_bytes".into()); }
        let mut marker = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(self.registry.join("owner.binding"))
            .map_err(|e| e.to_string())?;
        marker
            .write_all(encoded.as_bytes())
            .map_err(|e| e.to_string())?;
        marker.sync_all().map_err(|e| e.to_string())?;
        File::open(&self.registry)
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())?;
        File::open(self.registry.parent().ok_or("missing registry parent")?)
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())?;
        Ok(encoded)
    }

    pub fn verify(&self, database_id: &str, encoded: &str) -> Result<(), String> {
        if encoded.len() > self.limits.owner_binding_bytes {
            return Err("oversized helper registry binding: controller.owner_binding_bytes".into());
        }
        let expected: Binding = serde_json::from_str(encoded).map_err(|e| e.to_string())?;
        if serde_json::to_string(&expected).map_err(|e| e.to_string())? != encoded
            || expected != self.binding(database_id)?
        {
            return Err("controller database/helper registry identity mismatch".into());
        }
        let marker = self.registry.join("owner.binding");
        regular(&marker)?;
        let mut bytes = Vec::new();
        File::open(marker)
            .map_err(|e| e.to_string())?
            .take((self.limits.owner_binding_bytes as u64).saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(|e| e.to_string())?;
        if bytes != encoded.as_bytes() {
            return Err("missing or mismatched helper registry marker".into());
        }
        Ok(())
    }
}

/// Error-path cleanup for only a zero-byte WAL created by this startup read.
/// Prior sidecars are never reclaimed here, including prior empty WAL files.
pub(super) struct StartupWal {
    path: PathBuf,
    absent: bool,
}
impl StartupWal {
    pub fn before(path: &Path) -> Result<Self, String> {
        let wal_path = suffixed(path, "-wal");
        let absent = !present(&wal_path)?;
        if !absent {
            let wal = regular(&wal_path)?;
            // Pinned SQLite robust_open harmonizes an existing empty WAL's
            // mode with its main DB. Refuse that incompatible preimage before
            // opening SQLite; never undo a permission mutation afterward.
            if wal.len() == 0 && wal.mode() & 0o777 != regular(path)?.mode() & 0o777 {
                return Err(
                    "existing empty WAL permissions differ from database; original data retained"
                        .into(),
                );
            }
        }
        Ok(Self {
            absent,
            path: wal_path,
        })
    }
    pub fn observe_created(&self) -> Result<Option<(u64, u64)>, String> {
        if !self.absent || !present(&self.path)? {
            return Ok(None);
        }
        let m = regular(&self.path)?;
        if m.len() != 0 {
            return Err(
                "startup created a nonempty WAL; cleanup refused and evidence retained".into(),
            );
        }
        Ok(Some((m.dev(), m.ino())))
    }
    pub fn remove_after_close(&self, created: Option<(u64, u64)>) -> Result<(), String> {
        let Some(identity) = created else {
            return Ok(());
        };
        let m = regular(&self.path)?;
        if !self.absent || (m.dev(), m.ino()) != identity || m.len() != 0 {
            return Err(
                "startup WAL identity changed; cleanup refused and evidence retained".into(),
            );
        }
        fs::remove_file(&self.path).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root() -> PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let seq = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "borg-store-preopen-{}-{stamp}-{seq}",
            std::process::id()
        ));
        fs::create_dir(&dir).unwrap();
        dir.join("task.db")
    }

    #[test]
    fn owned_existing_root_rechecks_store_loss_before_sqlite() {
        let path = root();
        let database = super::super::database::Database::open(
            &path,
            super::super::ControllerConfig::default(),
        )
        .unwrap();
        drop(database);
        let mut owned = Tracking::acquire(&path, &crate::limits::RuntimeLimits::seed().unwrap().controller).unwrap();
        let retained = path.with_file_name("retained.db");
        fs::rename(&path, &retained).unwrap();
        let bytes = fs::read(&retained).unwrap();
        let wal = suffixed(&path, "-wal");
        fs::write(&wal, b"retain WAL evidence after controlled loss").unwrap();
        assert!(owned.before_sqlite().is_err());
        assert!(!path.exists());
        assert_eq!(fs::read(&retained).unwrap(), bytes);
        assert_eq!(
            fs::read(wal).unwrap(),
            b"retain WAL evidence after controlled loss"
        );
    }

    #[test]
    fn owned_fresh_root_rechecks_partial_initialization_before_database_creation() {
        let path = root();
        let mut owned = Tracking::acquire(&path, &crate::limits::RuntimeLimits::seed().unwrap().controller).unwrap();
        fs::create_dir(&owned.registry).unwrap();
        fs::write(owned.registry.join("retained-evidence"), b"unclaimed").unwrap();
        assert!(owned.before_sqlite().is_err());
        assert!(!path.exists());
        assert_eq!(
            fs::read(owned.registry.join("retained-evidence")).unwrap(),
            b"unclaimed"
        );
    }
}
