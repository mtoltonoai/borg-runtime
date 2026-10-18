//! Genuine schema17 / worker13 bytes from the closed tests38 producer.
//!
//! This exercises the real retained-record validators and additive SQL migration
//! on a copy. It deliberately does not claim physical Controller::open or worker
//! startup: the historical helper registry binds its original path and inodes.
//! Root supplies the hash-pinned artifact; no old-format fixture is reconstructed.
use super::{ControllerConfig, application_schema, database::StoreContext};
use crate::{limits::RuntimeLimits, protocol::*};
use rusqlite::{Connection, types::Value as SqlValue};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

const MANIFEST_SHA256: &str = "e938f8754d1a5d66b7b8bb76243fbbe97a92de3b5f68bf6a58caeb6e597db589";
const DATABASE_ID: &str = "07d0077c51a7b581add5425b8140d920";
const ATTEMPT: &str = "attempt-task-07d0077c51a7b581add5425b8140d920-00000000000000000001-1";

struct Fixture {
    root: PathBuf,
    manifest: serde_json::Value,
}
fn assert_hash(path: &Path, expected: &str) {
    assert!(path.is_absolute());
    assert!(
        std::fs::symlink_metadata(path)
            .unwrap()
            .file_type()
            .is_file()
    );
    let output = std::process::Command::new("/usr/bin/sha256sum")
        .arg("--")
        .arg(path)
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout)
            .unwrap()
            .split_whitespace()
            .next(),
        Some(expected)
    );
}
impl Fixture {
    fn selected() -> Self {
        let root = PathBuf::from(
            std::env::var_os("BORG_NATIVE_DIAGNOSTIC_PRIOR_FIXTURE")
                .expect("select the preserved genuine schema17/worker13 artifact root"),
        );
        assert!(root.is_absolute());
        let path = root.join("fixture-manifest.json");
        assert_hash(&path, MANIFEST_SHA256);
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(
            manifest["producer"]["source_commit"],
            "1c512dcc4ac88f1e4c98830b64ecb57b805be5e4"
        );
        assert_eq!(manifest["producer"]["receipt_status"], "FAIL");
        let selected = Self { root, manifest };
        selected.verify();
        selected
    }
    fn verify(&self) {
        for (name, entry) in self.manifest["files"].as_object().unwrap() {
            assert!(!name.contains('/') && !name.contains(".."));
            let path = self.root.join("fixture").join(name);
            assert_eq!(
                std::fs::metadata(&path).unwrap().len(),
                entry["bytes"].as_u64().unwrap()
            );
            assert_hash(&path, entry["sha256"].as_str().unwrap());
        }
    }
    fn bytes(&self, name: &str) -> Vec<u8> {
        assert!(self.manifest["files"].get(name).is_some());
        std::fs::read(self.root.join("fixture").join(name)).unwrap()
    }
    fn config(&self) -> ControllerConfig {
        ControllerConfig {
            limits: Arc::new(RuntimeLimits::parse(&self.bytes("limits.json")).unwrap()),
            ..ControllerConfig::default()
        }
    }
    fn copy_database(&self, label: &str) -> PathBuf {
        let root = PathBuf::from(
            std::env::var_os("BORG_TEST_ARTIFACT").expect("select a separate test output root"),
        );
        assert!(root.is_absolute());
        std::fs::create_dir_all(&root).unwrap();
        let root = std::fs::canonicalize(root).unwrap();
        assert!(!root.starts_with(std::fs::canonicalize(&self.root).unwrap()));
        assert!(!root.starts_with(std::fs::canonicalize(env!("CARGO_MANIFEST_DIR")).unwrap()));
        let area = root.join(format!(
            "native-diagnostic-compatibility-{label}-{}-{}",
            std::process::id(),
            crate::worker::process::now_ms()
        ));
        std::fs::create_dir(&area).unwrap();
        let path = area.join("tasks.sqlite");
        std::fs::copy(self.root.join("fixture/controller.sqlite"), &path).unwrap();
        assert_hash(
            &path,
            self.manifest["files"]["controller.sqlite"]["sha256"]
                .as_str()
                .unwrap(),
        );
        path
    }
}

type LogicalRows = BTreeMap<String, Vec<Vec<SqlValue>>>;
fn logical_rows(conn: &Connection) -> LogicalRows {
    let mut names = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name LIMIT 129")
        .unwrap();
    let names = names
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(names.len() <= 128);
    names
        .into_iter()
        .map(|name| {
            let quoted = name.replace('"', "\"\"");
            let width = conn
                .prepare(&format!("SELECT * FROM \"{quoted}\" LIMIT 0"))
                .unwrap()
                .column_count();
            let order = (1..=width)
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(",");
            let mut q = conn
                .prepare(&format!(
                    "SELECT * FROM \"{quoted}\" ORDER BY {order} LIMIT 1025"
                ))
                .unwrap();
            let rows = q
                .query_map([], |r| {
                    (0..width)
                        .map(|n| r.get(n))
                        .collect::<Result<Vec<SqlValue>, _>>()
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert!(
                rows.len() <= 1024,
                "fixed historical fixture row bound: {name}"
            );
            (name, rows)
        })
        .collect()
}

// The same production logical validators used by Database::initialize, without
// changing or pretending to verify the original physical root association.
fn validate_logical(conn: &Connection, config: &ControllerConfig) -> Result<(), String> {
    let version = conn
        .query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
        .map_err(|e| e.to_string())?;
    if !matches!(version, 17 | 18 | 19 | 20 | 21 | 22 | 23 | 24 | 25 | 26) {
        return Err("fixture requires schema17,18,19,20,21,22,23,24,25 or26".into());
    }
    super::compatibility::validate_shape(conn, version)?;
    super::compatibility::validate_records(conn)?;
    let tx = StoreContext::new(conn, &config.limits);
    let mut q = conn
        .prepare("SELECT id FROM tasks ORDER BY seq")
        .map_err(|e| e.to_string())?;
    let ids = q
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    for id in ids {
        let stored = super::database::load(&tx, &id).map_err(|e| format!("{e:?}"))?;
        super::native_retry_validation::validate_task(&tx, &stored.task)
            .map_err(|e| format!("{e:?}"))?;
        if let Some(hold) = &stored.completion_hold {
            super::governance_completion::validate_hold(&tx, &stored.task, hold)
                .map_err(|e| format!("{e:?}"))?;
        }
    }
    super::metadata::validate_existing(&tx, config).map_err(|e| format!("{e:?}"))?;
    super::agents::validate_existing(&tx, config, version).map_err(|e| format!("{e:?}"))?;
    super::credits::validate_existing(&tx, config).map_err(|e| format!("{e:?}"))?;
    super::native_request_diagnostics::validate_existing(&tx, version >= 18)
        .map_err(|e| format!("{e:?}"))?;
    super::operations::releases::validate_existing(&tx, config).map_err(|e| format!("{e:?}"))?;
    super::protected::validate_existing(&tx, config).map_err(|e| format!("{e:?}"))?;
    super::repositories::validate_existing(&tx).map_err(|e| format!("{e:?}"))?;
    super::collaboration_workflow::validate_existing(&tx, (config.clock)())
        .map_err(|e| format!("{e:?}"))?;
    super::sessions::validate_existing(&tx, config, true)?;
    super::charters::validate_existing(&tx).map_err(|e| format!("{e:?}"))?;
    super::native_request_coordination::validate_stored(&tx, config)
        .map_err(|e| format!("{e:?}"))?;
    super::governance_validation::validate_stored(&tx, config).map_err(|e| format!("{e:?}"))?;
    if !super::database::cleanup_available(&tx, config, 0).map_err(|e| format!("{e:?}"))? {
        return Err("historical cleanup reservation accounting differs".into());
    }
    Ok(())
}
fn validate_then_migrate(conn: &Connection, config: &ControllerConfig) -> Result<(), String> {
    conn.pragma_update(None, "query_only", true)
        .map_err(|e| e.to_string())?;
    validate_logical(conn, config)?;
    let version = conn
        .query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
        .map_err(|e| e.to_string())?;
    conn.pragma_update(None, "query_only", false)
        .map_err(|e| e.to_string())?;
    if version == 17 {
        application_schema::upgrade_from_validated(conn, 17)?;
    }
    Ok(())
}

#[test]
#[ignore = "root selects the hash-pinned actual old producer fixture; no new producer run"]
fn genuine_schema17_native_receipts_migrate_without_new_credits_or_record_rewrites() {
    let f = Fixture::selected();
    let path = f.copy_database("valid");
    let config = f.config();
    let conn = Connection::open(&path).unwrap();
    assert_eq!(
        conn.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        17
    );
    let before = logical_rows(&conn);
    assert_eq!(before["receipts"].len(), 53);
    assert!(!before.contains_key("native_request_diagnostic_credits"));
    assert_eq!(
        conn.query_row(
            "SELECT value FROM settings WHERE name='database_id'",
            [],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        DATABASE_ID
    );
    for label in ["usage", "completion"] {
        let expected = String::from_utf8(f.bytes(&format!("{label}-request.json"))).unwrap();
        let operation: Operation = serde_json::from_str(&expected).unwrap();
        let Operation::ObserveWorker { report } = operation else {
            panic!("original worker request")
        };
        let actual: (String, String) = conn
            .query_row(
                "SELECT request,outcome FROM receipts WHERE id=?1",
                [&report.report_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(actual.0, expected);
        assert_eq!(
            actual.1.as_bytes(),
            f.bytes(&format!("{label}-outcome.json"))
        );
    }
    //= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
    //= type=test
    //# A change to stored task state representation MUST have an explicit migration disposition.
    validate_then_migrate(&conn, &config).unwrap();
    drop(conn);
    // Reopen through the same logical reader, not the physical owner API.
    let conn = Connection::open(&path).unwrap();
    validate_then_migrate(&conn, &config).unwrap();
    assert_eq!(
        conn.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        application_schema::VERSION
    );
    let mut after = logical_rows(&conn);
    let expected_inputs=conn.prepare("SELECT session,id,json_extract(body,'$.task.input'),NULL,NULL FROM tasks ORDER BY session,id").unwrap()
        .query_map([],|r|(0..5).map(|i|r.get::<_,SqlValue>(i)).collect::<rusqlite::Result<Vec<_>>>()).unwrap()
        .collect::<rusqlite::Result<Vec<_>>>().unwrap();
    assert_eq!(after.remove("session_inputs"),Some(expected_inputs),"each immutable input is derived exactly from its original accepted task");
    assert_eq!(after.remove("task_assignments"),Some(Vec::new()));
    assert_eq!(after.remove("agent_retirements"), Some(Vec::new()));
    assert_eq!(
        after.remove("native_request_diagnostic_credits"),
        Some(Vec::new())
    );
    assert_eq!(
        after, before,
        "all prior rows, including original receipt bytes and root binding, remain exact"
    );
    f.verify();
}

#[test]
#[ignore = "root selects the hash-pinned actual old producer fixture; no new producer run"]
fn marked_usage_in_historical_schema_refuses_before_migration_without_rewriting_corruption() {
    let f = Fixture::selected();
    let path = f.copy_database("invalid-vocabulary");
    let conn = Connection::open(&path).unwrap();
    let config = f.config();
    conn.pragma_update(None, "query_only", true).unwrap();
    validate_logical(&conn, &config).unwrap();
    conn.pragma_update(None, "query_only", false).unwrap();
    let mut request: Operation = serde_json::from_slice(&f.bytes("usage-request.json")).unwrap();
    let Operation::ObserveWorker { report } = &mut request else {
        panic!("original worker request")
    };
    let id = report.report_id.clone();
    let Observation::Usage {
        native_diagnostic_reserved,
        ..
    } = &mut report.observation
    else {
        panic!("original usage")
    };
    *native_diagnostic_reserved = true;
    assert_eq!(
        conn.execute(
            "UPDATE receipts SET request=?1 WHERE id=?2",
            rusqlite::params![serde_json::to_string(&request).unwrap(), id]
        )
        .unwrap(),
        1
    );
    let before = logical_rows(&conn);
    //= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
    //= type=test
    //# Corrupt stored task state MUST be refused without rewriting its data.
    let context = StoreContext::new(&conn, &config.limits);
    let error = super::native_request_diagnostics::validate_existing(&context, false)
        .expect_err("marked Usage never occurred in schema17");
    assert!(
        format!("{error:?}").contains("historical schema contains native diagnostic vocabulary")
    );
    assert!(validate_then_migrate(&conn, &config).is_err());
    assert_eq!(
        conn.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        17
    );
    assert_eq!(logical_rows(&conn), before);
    assert!(!before.contains_key("native_request_diagnostic_credits"));
    f.verify();
}

#[test]
#[ignore = "root selects the hash-pinned actual old producer fixture; parsing is not worker startup"]
fn genuine_unmarked_native_requests_and_worker13_pending_report_keep_exact_identity() {
    let f = Fixture::selected();
    for (name, usage) in [
        ("usage-request.json", true),
        ("completion-request.json", false),
    ] {
        let raw = f.bytes(name);
        let operation: Operation = serde_json::from_slice(&raw).unwrap();
        assert_eq!(
            serde_json::to_vec(&operation).unwrap(),
            raw,
            "missing historical fields stay absent"
        );
        let Operation::ObserveWorker { report } = operation else {
            panic!("original worker request")
        };
        assert_eq!(report.attempt_id, ATTEMPT);
        assert_eq!(report.worker_id, "loop-worker");
        assert_eq!(report.incarnation, "original-output-worker-one");
        assert_eq!(report.generation, 1);
        if usage {
            assert_eq!(report.report_id, format!("{ATTEMPT}-native-request-1"));
            assert!(matches!(
                report.observation,
                Observation::Usage {
                    model_requests: 1,
                    input_tokens: None,
                    output_tokens: None,
                    native_diagnostic_reserved: false
                }
            ));
        } else {
            assert_eq!(report.report_id, format!("{ATTEMPT}-native-loop-completed"));
            assert!(
                matches!(report.observation, Observation::NativeLoopCompleted { model_requests: 1, calls, requests, final_response_index: None } if calls.is_empty() && requests.is_empty())
            );
        }
    }
    assert_eq!(f.bytes("format13.json"), b"{\"format_revision\":13}");
    let raw = f.bytes("pending-report.json");
    let report: Report = serde_json::from_slice(&raw).unwrap();
    assert_eq!(serde_json::to_vec(&report).unwrap(), raw);
    assert_eq!(
        report.report_id,
        format!("{ATTEMPT}-report-00000000000000000003")
    );
    assert_eq!(report.attempt_id, ATTEMPT);
    assert_eq!(
        report.task_id,
        format!("task-{DATABASE_ID}-00000000000000000001")
    );
    assert!(
        matches!(report.observation, Observation::ResultCandidate { checks_passed: true, checkpoint: None, output: Some(ref value), ref validation, .. }
        if value == &serde_json::json!(7) && validation.iter().any(|v| v.ends_with(&format!("{ATTEMPT}-native-loop-completed"))))
    );
    f.verify();
}
