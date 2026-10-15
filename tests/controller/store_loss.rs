//! Store-loss admission uses private local fixture roots. Retained original
//! files are test evidence, not a production backup/restore mechanism.
use super::*;
use std::collections::BTreeMap;
use std::os::unix::fs::MetadataExt;

fn inventory(root: &std::path::Path) -> BTreeMap<PathBuf, (u64, u64, Vec<u8>)> {
    fn walk(
        base: &std::path::Path,
        at: &std::path::Path,
        out: &mut BTreeMap<PathBuf, (u64, u64, Vec<u8>)>,
    ) {
        for entry in std::fs::read_dir(at).unwrap() {
            let path = entry.unwrap().path();
            let m = std::fs::symlink_metadata(&path).unwrap();
            if m.is_dir() {
                walk(base, &path, out);
            } else {
                assert!(m.is_file() && !m.file_type().is_symlink());
                out.insert(
                    path.strip_prefix(base).unwrap().to_owned(),
                    (m.dev(), m.ino(), std::fs::read(&path).unwrap()),
                );
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}
fn sidecar_evidence(p: &std::path::Path) {
    for suffix in ["-wal", "-shm", "-journal"] {
        std::fs::write(
            controller_sidecar(p, suffix),
            format!("retained controlled sidecar {suffix}"),
        )
        .unwrap();
    }
}

#[tokio::test]
// Scope: a known lost root refuses before opening SQLite or changing its sidecars.
//= spec/impl/v2/implementation.md#backups-are-outside-the-initial-milestone
//= type=test
//# A recovery procedure MUST NOT fabricate previously committed tasks when their storage has been lost.
async fn missing_bound_database_refuses_without_creating_db_lock_or_touching_sidecars() {
    for missing_companion in [None, Some(".controller-lock"), Some(".native-helpers")] {
        let p = path();
        let c = Controller::open_with_config(&p, config()).unwrap();
        let submitted = submit(&c, "retained-task", coding("retained-agent")).await;
        assert_eq!(get(&c, &submitted.id).await.id, submitted.id);
        drop(c);
        controller_owner_released(&p).await;
        std::fs::rename(&p, p.with_file_name("retained-original.db")).unwrap();
        if let Some(suffix) = missing_companion {
            std::fs::rename(
                controller_sidecar(&p, suffix),
                p.with_file_name(format!("retained{suffix}")),
            )
            .unwrap();
        }
        sidecar_evidence(&p);
        let before = inventory(p.parent().unwrap());
        for _ in 0..2 {
            let refusal = Controller::open_with_config(&p, config())
                .err()
                .expect("known store loss refuses");
            assert!(
                refusal.contains("missing") || refusal.contains("partial"),
                "{refusal}"
            );
            assert!(!p.exists());
            assert_eq!(
                inventory(p.parent().unwrap()),
                before,
                "all evidence, including lock presence, stays exact"
            );
        }
    }
}

#[test]
fn partial_root_artifacts_and_unbound_empty_files_do_not_trigger_initialization() {
    for partial in [
        ".controller-lock",
        "-wal",
        "-shm",
        "-journal",
        ".native-helpers",
        "empty-db",
    ] {
        let p = path();
        if partial == ".native-helpers" {
            let directory = controller_sidecar(&p, partial);
            std::fs::create_dir(&directory).unwrap();
            std::fs::write(directory.join("unclaimed"), b"retain").unwrap();
        } else if partial == "empty-db" {
            std::fs::write(&p, b"").unwrap();
            sidecar_evidence(&p);
        } else {
            std::fs::write(controller_sidecar(&p, partial), b"retain").unwrap();
        }
        let before = inventory(p.parent().unwrap());
        for _ in 0..2 {
            assert!(
                Controller::open_with_config(&p, config()).is_err(),
                "{partial}"
            );
            assert_eq!(inventory(p.parent().unwrap()), before, "{partial}");
        }
    }
}

#[tokio::test]
// Scope: original bound inode empty/short truncation, and replaced inode, are
// rejected before SQLite; this is not an exhaustive physical-corruption proof.
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# Corrupt stored task state MUST be refused without rewriting its data.
async fn bound_empty_short_or_replaced_database_preserves_all_existing_evidence() {
    for defect in [
        "empty-original-inode",
        "short-original-inode",
        "replaced-inode",
    ] {
        let p = path();
        let c = Controller::open_with_config(&p, config()).unwrap();
        submit(&c, "bound-submission", coding("bound-agent")).await;
        drop(c);
        controller_owner_released(&p).await;
        let original = std::fs::read(&p).unwrap();
        let identity = std::fs::metadata(&p).unwrap().ino();
        let retained = p.with_file_name("retained-original.db");
        if defect == "replaced-inode" {
            std::fs::rename(&p, &retained).unwrap();
            std::fs::write(&p, &original).unwrap();
            assert_ne!(std::fs::metadata(&p).unwrap().ino(), identity);
        } else {
            std::fs::write(&retained, &original).unwrap();
            let length = if defect == "empty-original-inode" {
                0
            } else {
                31
            };
            std::fs::OpenOptions::new()
                .write(true)
                .open(&p)
                .unwrap()
                .set_len(length)
                .unwrap();
            assert_eq!(std::fs::metadata(&p).unwrap().ino(), identity);
        }
        sidecar_evidence(&p);
        let before = inventory(p.parent().unwrap());
        assert!(
            Controller::open_with_config(&p, config()).is_err(),
            "{defect}"
        );
        assert_eq!(inventory(p.parent().unwrap()), before, "{defect}");
        assert_eq!(std::fs::read(retained).unwrap(), original);
    }
}

#[tokio::test]
async fn missing_registry_refuses_existing_database_before_sidecar_recovery_or_lock_creation() {
    let original = path();
    let c = Controller::open_with_config(&original, config()).unwrap();
    submit(&c, "source-submission", coding("source-agent")).await;
    drop(c);
    controller_owner_released(&original).await;
    // A copied database without its binding cannot become a new root. Keep
    // both original and target bytes; no SQLite open is needed to refuse it.
    let p = path();
    std::fs::copy(&original, &p).unwrap();
    sidecar_evidence(&p);
    let before = inventory(p.parent().unwrap());
    let refusal = Controller::open_with_config(&p, config())
        .err()
        .expect("unbound database refusal");
    assert!(refusal.contains("registry"), "{refusal}");
    assert_eq!(inventory(p.parent().unwrap()), before);
    assert!(!controller_sidecar(&p, ".controller-lock").exists());
}

#[tokio::test]
async fn genuinely_fresh_active_and_restarted_roots_keep_original_task_and_receipt() {
    let p = path();
    assert!(inventory(p.parent().unwrap()).is_empty());
    let c = Controller::open_with_config(&p, config()).unwrap();
    register_repository_fixture(&c, &fixture_repository_registration()).await;
    let command = Operation::SubmitTask {
        operation_id: "fresh-source".into(),
        input: coding("fresh-agent"),
    };
    let request = |id: &str| Request {
        protocol_revision: WIRE_REVISION,
        request_id: id.into(),
        command: command.clone(),
    };
    let original = c.handle(request("fresh")).await;
    let submitted = task(original.outcome.clone());
    assert!(original.commit.is_some());
    assert!(
        Controller::open_with_config(&p, config()).is_err(),
        "active owner remains exclusive"
    );
    assert_eq!(get(&c, &submitted.id).await.id, submitted.id);
    let marker = controller_sidecar(&p, ".native-helpers").join("owner.binding");
    let binding = std::fs::read(&marker).unwrap();
    let identity = std::fs::metadata(&p).unwrap().ino();
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, config()).unwrap();
    let replay = c.handle(request("restart")).await;
    assert_eq!(replay.commit, original.commit);
    assert_eq!(replay.outcome, original.outcome);
    assert_eq!(std::fs::metadata(&p).unwrap().ino(), identity);
    assert_eq!(std::fs::read(marker).unwrap(), binding);
    assert_eq!(get(&c, &submitted.id).await.id, submitted.id);
}
