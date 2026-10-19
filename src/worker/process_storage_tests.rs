//! Test-only exact-path filesystem failure cuts, not a quota or disk-fill test.
use super::*;
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Point {
    Create,
    Write,
    FileSync,
    Rename,
    DirectorySync,
}

struct Failure {
    id: u64,
    path: PathBuf,
    point: Point,
    errno: i32,
    prefix: usize,
    hits: Arc<AtomicUsize>,
}
static FAILURES: Mutex<Vec<Failure>> = Mutex::new(Vec::new());
static NEXT: AtomicU64 = AtomicU64::new(1);

pub(crate) struct Fault {
    id: u64,
    hits: Arc<AtomicUsize>,
}
impl Fault {
    pub(crate) fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }
}
impl Drop for Fault {
    fn drop(&mut self) {
        FAILURES.lock().unwrap().retain(|f| f.id != self.id);
    }
}

pub(crate) fn inject(path: &Path, point: Point, errno: i32, prefix: usize) -> Fault {
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let hits = Arc::new(AtomicUsize::new(0));
    let mut failures = FAILURES.lock().unwrap();
    assert!(
        failures.iter().all(|f| f.path != path),
        "overlapping storage fault"
    );
    failures.push(Failure {
        id,
        path: path.into(),
        point,
        errno,
        prefix,
        hits: hits.clone(),
    });
    Fault { id, hits }
}

pub(super) fn cut(
    path: &Path,
    point: Point,
    file: Option<&mut std::fs::File>,
    bytes: &[u8],
) -> Result<(), String> {
    let fault = {
        let mut failures = FAILURES.lock().unwrap();
        failures
            .iter()
            .position(|f| f.path == path && f.point == point)
            .map(|index| failures.remove(index))
    };
    if let Some(fault) = fault {
        fault.hits.fetch_add(1, Ordering::SeqCst);
        if fault.prefix != 0 {
            assert_eq!(point, Point::Write);
            file.expect("partial write requires opened pending file")
                .write_all(&bytes[..fault.prefix.min(bytes.len())])
                .map_err(|e| e.to_string())?;
        }
        return Err(std::io::Error::from_raw_os_error(fault.errno).to_string());
    }
    Ok(())
}

struct Area(PathBuf);
impl Area {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "borg-storage-cut-{}-{}-{}",
            std::process::id(),
            now_ms(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Area {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn limits() -> Box<WorkerLimits> {
    crate::worker::test_limits().worker.clone()
}
fn stale_owner(path: &Path) -> Vec<u8> {
    // A closed synthetic old record in this exact process namespace. No process
    // is signalled; explicitly verify that this fixture PID is absent.
    let pid = u32::MAX;
    assert!(identity(pid, &limits()).unwrap().is_none());
    let record = ProcessRecord {
        pid,
        start_ticks: 1,
        process_group: pid,
        context: ProcessContext::current().unwrap(),
        descendants: vec![],
    };
    // Cold recovery starts with the original stable lock already present; this
    // fixture does not assume space for allocating a replacement lock inode.
    std::fs::File::create(path.with_extension("lock"))
        .unwrap()
        .sync_all()
        .unwrap();
    atomic_json(path, &record).unwrap();
    std::fs::read(path).unwrap()
}
fn file_identity(path: &Path) -> (u64, u64) {
    let m = std::fs::metadata(path).unwrap();
    (m.dev(), m.ino())
}

#[test]
fn atomic_journal_cuts_preserve_original_until_rename_and_never_claim_sync_success() {
    let original = serde_json::json!({"operation_id":"same-original", "outcome":null});
    let replacement =
        serde_json::json!({"operation_id":"same-original", "outcome":{"status":"ok"}});
    let expected = serde_json::to_vec(&replacement).unwrap();
    for errno in [28, 122] {
        // Linux ENOSPC and EDQUOT, injected without filling a filesystem.
        for point in [
            Point::Create,
            Point::Write,
            Point::FileSync,
            Point::Rename,
            Point::DirectorySync,
        ] {
            let area = Area::new();
            let path = area.0.join("journal.json");
            atomic_json(&path, &original).unwrap();
            let before = std::fs::read(&path).unwrap();
            let old_identity = file_identity(&path);
            let fault = inject(
                &path,
                point,
                errno,
                if point == Point::Write { 7 } else { 0 },
            );
            assert!(
                atomic_json(&path, &replacement).is_err(),
                "{point:?} must not acknowledge durability"
            );
            assert_eq!(fault.hits(), 1);
            if point == Point::DirectorySync {
                assert_eq!(std::fs::read(&path).unwrap(), expected);
                assert!(!path.with_extension("pending").exists());
            } else {
                assert_eq!(std::fs::read(&path).unwrap(), before);
                assert_eq!(file_identity(&path), old_identity);
                if point == Point::Write {
                    assert_eq!(
                        std::fs::read(path.with_extension("pending")).unwrap(),
                        expected[..7]
                    );
                }
            }
            drop(fault);
            // This is a local persistence retry of the same body, not a second
            // external effect or a fabricated settled outcome.
            atomic_json(&path, &replacement).unwrap();
            assert_eq!(std::fs::read(&path).unwrap(), expected);
        }
    }
}

#[test]
fn owner_replacement_write_refusal_preserves_original_record() {
    let area = Area::new();
    let path = area.0.join("worker-owner.json");
    let original = stale_owner(&path);
    let original_identity = file_identity(&path);
    // Real namespace failure; no test fault hook is needed for this regression.
    std::fs::create_dir(path.with_extension("pending")).unwrap();
    let claimed = WorkerLease::claim(&path, &limits());
    assert!(
        claimed.is_err(),
        "failed replacement must not return a lease"
    );
    assert_eq!(std::fs::read(&path).unwrap(), original);
    assert_eq!(file_identity(&path), original_identity);
    let lock_identity = file_identity(&path.with_extension("lock"));
    std::fs::remove_dir(path.with_extension("pending")).unwrap();
    let lease = WorkerLease::claim(&path, &limits()).unwrap();
    assert_eq!(file_identity(&path.with_extension("lock")), lock_identity);
    assert_eq!(
        read_record(&path, &limits()).unwrap().pid,
        std::process::id()
    );
    drop(lease);
}

#[test]
fn full_storage_owner_replacement_preserves_identity_and_other_recovery_evidence() {
    for errno in [28, 122] {
        for point in [Point::Create, Point::Write, Point::FileSync, Point::Rename] {
            let area = Area::new();
            let path = area.0.join("worker-owner.json");
            let original = stale_owner(&path);
            let old_identity = file_identity(&path);
            let lock_identity = file_identity(&path.with_extension("lock"));
            let evidence = area.0.join("original-pending-operation.json");
            let retained = b"{\"operation_id\":\"original\",\"outcome\":null}";
            std::fs::write(&evidence, retained).unwrap();
            let fault = inject(
                &path,
                point,
                errno,
                if point == Point::Write { 9 } else { 0 },
            );
            assert!(WorkerLease::claim(&path, &limits()).is_err());
            assert_eq!(fault.hits(), 1);
            assert_eq!(std::fs::read(&path).unwrap(), original);
            assert_eq!(file_identity(&path), old_identity);
            assert_eq!(file_identity(&path.with_extension("lock")), lock_identity);
            assert_eq!(std::fs::read(&evidence).unwrap(), retained);
            drop(fault);
            let lease = WorkerLease::claim(&path, &limits()).unwrap();
            assert_eq!(file_identity(&path.with_extension("lock")), lock_identity);
            assert_eq!(std::fs::read(&evidence).unwrap(), retained);
            drop(lease);
        }
    }
}

#[test]
fn owner_directory_sync_failure_returns_no_lease_and_retains_complete_uncertain_successor() {
    let area = Area::new();
    let path = area.0.join("worker-owner.json");
    stale_owner(&path);
    let fault = inject(&path, Point::DirectorySync, 5, 0); // EIO after rename.
    assert!(WorkerLease::claim(&path, &limits()).is_err());
    assert_eq!(fault.hits(), 1);
    let retained = std::fs::read(&path).unwrap();
    assert_eq!(
        read_record(&path, &limits()).unwrap().pid,
        std::process::id()
    );
    drop(fault);
    // We cannot claim another incarnation while this exact PID remains alive.
    assert!(WorkerLease::claim(&path, &limits()).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), retained);
}

#[test]
fn owner_record_limits_and_live_owner_refuse_without_erasing_original() {
    let area = Area::new();
    let path = area.0.join("worker-owner.json");
    let original = stale_owner(&path);
    let old_identity = file_identity(&path);
    let mut too_small = limits();
    too_small.process_record_bytes = original.len() - 1;
    assert!(WorkerLease::claim(&path, &too_small).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), original);
    assert_eq!(file_identity(&path), old_identity);
    let lease = WorkerLease::claim(&path, &limits()).unwrap();
    let current = std::fs::read(&path).unwrap();
    let mut immediate = limits();
    immediate.owner_lock_timeout_ms = 1;
    immediate.owner_lock_retry_ms = 1;
    assert!(WorkerLease::claim(&path, &immediate).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), current);
    drop(lease);
}
