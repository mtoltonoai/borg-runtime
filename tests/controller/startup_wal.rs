//! Real SQLite committed-WAL fixtures; no copied bound database or file-format
//! parser. Deliberate SQL defects are private test inputs. The separate release
//! compatibility probe uses an actual historical binary/schema producer.
use super::*;
use rusqlite::{Connection, config::DbConfig};
use std::{
    collections::BTreeMap,
    os::unix::fs::{MetadataExt, PermissionsExt},
};

fn inventory(root: &std::path::Path) -> BTreeMap<PathBuf, (u64, u64, u32, Vec<u8>)> {
    fn walk(
        root: &std::path::Path,
        at: &std::path::Path,
        out: &mut BTreeMap<PathBuf, (u64, u64, u32, Vec<u8>)>,
    ) {
        for entry in std::fs::read_dir(at).unwrap() {
            let path = entry.unwrap().path();
            let m = std::fs::symlink_metadata(&path).unwrap();
            if m.is_dir() {
                walk(root, &path, out);
            } else {
                assert!(m.is_file());
                out.insert(
                    path.strip_prefix(root).unwrap().into(),
                    (m.dev(), m.ino(), m.mode(), std::fs::read(path).unwrap()),
                );
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}
async fn populated() -> (PathBuf, Task) {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let t = submit(
        &c,
        "original-current-submission",
        coding("wal-current-agent"),
    )
    .await;
    drop(c);
    controller_owner_released(&p).await;
    (p, t)
}
fn writer(p: &std::path::Path, exclusive: bool) -> Connection {
    let c = Connection::open_with_flags(p, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE).unwrap();
    c.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true)
        .unwrap();
    c.pragma_update(
        None,
        "locking_mode",
        if exclusive { "EXCLUSIVE" } else { "NORMAL" },
    )
    .unwrap();
    c.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    let mode: String = c
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "wal");
    c
}

#[tokio::test]
// The committed incompatible/corrupt value is in genuine WAL frames; ignoring
// the WAL would admit the unchanged supported main file and fail this oracle.
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# An unsupported stored schema version MUST be refused without rewriting its data.
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# Corrupt stored task state MUST be refused without rewriting its data.
async fn committed_wal_rejection_preserves_database_wal_shm_and_binding_exactly() {
    for (defect, exclusive) in [
        ("schema", false),
        ("application", false),
        ("task", false),
        ("unreadable_schema", false),
        ("schema", true),
    ] {
        let (p, t) = populated().await;
        let main_before = std::fs::read(&p).unwrap();
        let c = writer(&p, exclusive);
        c.execute_batch("BEGIN IMMEDIATE; CREATE TABLE wal_sentinel(value TEXT); INSERT INTO wal_sentinel VALUES('committed WAL state');").unwrap();
        match defect {
            "schema"=>c.pragma_update(None,"user_version",999).unwrap(),
            "application"=>c.pragma_update(None,"application_id",123).unwrap(),
            "task"=>{c.execute("UPDATE tasks SET body='not supported canonical task data' WHERE id=?1",[&t.id]).unwrap();},
            "unreadable_schema"=>c.execute_batch("PRAGMA writable_schema=ON; UPDATE sqlite_schema SET sql='not valid SQL' WHERE name='tasks'; PRAGMA writable_schema=OFF;").unwrap(),
            _=>unreachable!(),
        }
        c.execute_batch("COMMIT").unwrap();
        c.close().unwrap();
        // The real committed transaction was not checkpointed into main.
        assert_eq!(std::fs::read(&p).unwrap(), main_before);
        let wal = controller_sidecar(&p, "-wal");
        let shm = controller_sidecar(&p, "-shm");
        assert!(std::fs::metadata(&wal).unwrap().len() > 32);
        if exclusive {
            assert!(
                !shm.exists(),
                "actual exclusive SQLite writer uses heap WAL index"
            );
        } else {
            assert!(std::fs::metadata(&shm).unwrap().len() > 0);
        }
        let before = inventory(p.parent().unwrap());
        for _ in 0..2 {
            let error = Controller::open_with_config(&p, config())
                .err()
                .expect("must read committed WAL defect");
            if matches!(defect, "schema" | "application") {
                assert!(
                    error.contains("unsupported controller database schema"),
                    "{error}"
                );
            } else if defect == "task" {
                assert!(error.contains("corrupt stored task"), "{error}");
            }
            assert_eq!(
                inventory(p.parent().unwrap()),
                before,
                "{defect}, exclusive={exclusive}: refusal must preserve all original sidecars"
            );
        }
    }
}

#[tokio::test]
async fn refused_startup_removes_only_its_new_empty_wal_not_an_existing_empty_wal() {
    for prior_empty in [false, true] {
        let (p, _) = populated().await;
        // A normal SQLite close checkpoints this deliberately unsupported test
        // version. This tests only error-path newly-created-empty-WAL cleanup.
        let c =
            Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE).unwrap();
        c.pragma_update(None, "locking_mode", "EXCLUSIVE").unwrap();
        c.pragma_update(None, "user_version", 999).unwrap();
        c.close().unwrap();
        let wal = controller_sidecar(&p, "-wal");
        assert!(!wal.exists());
        if prior_empty {
            std::fs::write(&wal, b"").unwrap();
            let mode = std::fs::metadata(&p).unwrap().mode() & 0o777;
            std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(mode)).unwrap();
        }
        let before = inventory(p.parent().unwrap());
        let error = Controller::open_with_config(&p, config())
            .err()
            .expect("unsupported schema");
        assert!(
            error.contains("unsupported controller database schema"),
            "{error}"
        );
        assert_eq!(inventory(p.parent().unwrap()), before);
        assert_eq!(wal.exists(), prior_empty);
    }
}

#[tokio::test]
async fn empty_wal_permission_mismatch_refuses_without_chmod_or_schema_read() {
    for unsupported in [false, true] {
        let (p, _) = populated().await;
        if unsupported {
            let c = Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)
                .unwrap();
            c.pragma_update(None, "locking_mode", "EXCLUSIVE").unwrap();
            c.pragma_update(None, "user_version", 999).unwrap();
            c.close().unwrap();
        }
        let wal = controller_sidecar(&p, "-wal");
        assert!(!wal.exists());
        assert_eq!(std::fs::metadata(&p).unwrap().mode() & 0o777, 0o600);
        std::fs::write(&wal, b"").unwrap();
        std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(0o644)).unwrap();
        let before = inventory(p.parent().unwrap());
        for _ in 0..2 {
            let error = Controller::open_with_config(&p, config())
                .err()
                .expect("mode mismatch must refuse before SQLite harmonizes it");
            assert!(
                error.contains("existing empty WAL permissions differ"),
                "{error}"
            );
            assert_eq!(inventory(p.parent().unwrap()), before);
        }
    }
}

#[tokio::test]
async fn valid_current_empty_wal_with_matching_mode_remains_restartable() {
    let (p, t) = populated().await;
    let wal = controller_sidecar(&p, "-wal");
    assert!(!wal.exists());
    assert_eq!(std::fs::metadata(&p).unwrap().mode() & 0o777, 0o600);
    std::fs::write(&wal, b"").unwrap();
    std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(0o600)).unwrap();
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(get(&c, &t.id).await.id, t.id);
    let next = submit(
        &c,
        "after-legitimate-empty-wal",
        coding("legitimate-empty-wal-agent"),
    )
    .await;
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(get(&c, &t.id).await.id, t.id);
    assert_eq!(get(&c, &next.id).await.id, next.id);
}

#[tokio::test]
async fn rollback_journal_presence_refuses_before_sqlite_recovery_reads() {
    let (p, t) = populated().await;
    let c = Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE).unwrap();
    let mode: String = c
        .query_row("PRAGMA journal_mode=DELETE", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "delete");
    c.execute_batch("BEGIN IMMEDIATE").unwrap();
    c.execute("UPDATE tasks SET body=body || ' ' WHERE id=?1", [&t.id])
        .unwrap();
    let journal = controller_sidecar(&p, "-journal");
    assert!(std::fs::metadata(&journal).unwrap().len() > 0);
    let before = inventory(p.parent().unwrap());
    let error = Controller::open_with_config(&p, config())
        .err()
        .expect("rollback journal requires explicit disposition");
    assert!(
        error.contains("rollback-journal recovery is unsupported"),
        "{error}"
    );
    assert_eq!(inventory(p.parent().unwrap()), before);
    // Fixture owner ends its own transaction only after the preservation check.
    c.execute_batch("ROLLBACK").unwrap();
    c.close().unwrap();
}

#[tokio::test]
async fn valid_current_committed_wal_remains_readable_and_writable_after_startup() {
    let (p, t) = populated().await;
    let main_before = std::fs::read(&p).unwrap();
    let c = writer(&p, true);
    c.execute("UPDATE tasks SET body=json_set(body,'$.task.progress','committed-only-in-wal') WHERE id=?1",[&t.id]).unwrap();
    assert_eq!(
        c.query_row(
            "SELECT json_extract(body,'$.task.progress') FROM tasks WHERE id=?1",
            [&t.id],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        "committed-only-in-wal"
    );
    c.close().unwrap();
    assert_eq!(std::fs::read(&p).unwrap(), main_before);
    assert!(
        std::fs::metadata(controller_sidecar(&p, "-wal"))
            .unwrap()
            .len()
            > 32
    );
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(get(&c, &t.id).await.progress, "committed-only-in-wal");
    let another = submit(&c, "after-valid-wal", coding("after-wal-agent")).await;
    assert_eq!(another.status, TaskStatus::Queued);
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(get(&c, &t.id).await.progress, "committed-only-in-wal");
    assert_eq!(get(&c, &another.id).await.id, another.id);
}
