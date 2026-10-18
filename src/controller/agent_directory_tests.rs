//! Controlled schema fixtures and real browser boundary; no historical binary claim.
use super::*;
use crate::collaboration::workflow::TrustedHostPath;
use crate::controller::{Controller, application_schema, compatibility};
use rusqlite::Connection as Sql;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

fn definition() -> Definition {
    Definition {
        id: "directory-owner".into(),
        revision: "revision-1".into(),
        executor: ExecutorKind::Native,
        inference: Some(InferenceSettings::selected()),
        execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
        limit_overrides: None,
        charter: None,
    }
}
fn sql(version: i64) -> Sql {
    let conn = Sql::open_in_memory().unwrap();
    application_schema::expected_schema(&conn, version).unwrap();
    conn
}
fn retained(conn: &Sql) -> Vec<(String, String, String)> {
    conn.prepare("SELECT id,request,outcome FROM receipts ORDER BY id")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap()
}
fn record(conn: &Sql, operation: &Operation, outcome: &Outcome, revision: i64) {
    conn.execute(
        "UPDATE counters SET value=?1 WHERE kind='controller_revision'",
        [revision],
    )
    .unwrap();
    conn.execute("INSERT INTO receipts(id,request,outcome,cleanup,controller_revision) VALUES(?1,?2,?3,0,?4)",
        params![operation.mutation_id().unwrap(), encode(operation).unwrap(), encode(outcome).unwrap(), revision]).unwrap();
}
fn registration() -> Operation {
    Operation::RegisterAgent {
        operation_id: "register".into(),
        definition: definition(),
    }
}
fn retirement() -> Operation {
    Operation::RetireAgent {
        operation_id: "retire".into(),
        agent_id: definition().id,
        expected_revision: definition().revision,
    }
}
fn successful(conn: &Sql, cfg: &ControllerConfig, command: &Operation, revision: i64) {
    let tx = Connection::new(conn, &cfg.limits);
    let (data, changed) = apply(&tx, command, cfg, 1).unwrap();
    assert!(changed);
    record(conn, command, &Outcome::Ok { data }, revision);
}

#[test]
fn lifecycle_requires_actual_operator_path_and_legacy_active_bytes_stay_exact() {
    let cfg = ControllerConfig::default();
    let conn = sql(23);
    for path in [
        TrustedHostPath::Browser,
        TrustedHostPath::ControllerMaintenance,
    ] {
        let mut tx = Connection::new(&conn, &cfg.limits);
        tx.invocation_path = path;
        for command in [registration(), retirement()] {
            assert!(matches!(
                apply(&tx, &command, &cfg, 1),
                Err(Fault::Refused("governance_operator_required", _))
            ));
        }
    }
    assert_eq!(
        conn.query_row("SELECT count(*) FROM agents", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    #[serde(deny_unknown_fields)]
    struct OldAgent {
        id: String,
        definition: Definition,
    }
    let old = OldAgent {
        id: definition().id,
        definition: definition(),
    };
    let bytes = encode(&old).unwrap();
    let active: AgentSummary = decode(&bytes).unwrap();
    assert!(!active.retired);
    assert_eq!(encode(&active).unwrap(), bytes);
    assert_eq!(
        serde_json::from_str::<OldAgent>(&encode(&active).unwrap()).unwrap(),
        old
    );
    let explicit = format!("{},\"retired\":false}}", bytes.strip_suffix('}').unwrap());
    assert!(
        decode::<AgentSummary>(&explicit).is_err(),
        "explicit false was never an old canonical field"
    );
    let mut retired = active;
    retired.retired = true;
    assert!(
        serde_json::from_str::<OldAgent>(&encode(&retired).unwrap()).is_err(),
        "old strict reader refuses retirement vocabulary"
    );
}

#[test]
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# A change to stored task state representation MUST have an explicit migration disposition.
fn schema22_upgrade_is_additive_atomic_and_old_shape_refuses_schema23() {
    let cfg = ControllerConfig::default();
    let conn = sql(22);
    let old_op = Operation::CancelTask {
        operation_id: "old-refusal".into(),
        task_id: "missing".into(),
        expected_revision: 0,
    };
    record(
        &conn,
        &old_op,
        &Outcome::Refused {
            code: "not_found".into(),
            message: "original refusal".into(),
        },
        1,
    );
    let before = retained(&conn);
    let tx = Connection::new(&conn, &cfg.limits);
    validate_existing(&tx, &cfg, 22).unwrap();
    compatibility::validate_shape(&conn, 22).unwrap();
    conn.commit_hook(Some(|| true)).unwrap();
    assert!(application_schema::upgrade_from_validated(&conn, 22).is_err());
    assert_eq!(
        conn.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        22
    );
    compatibility::validate_shape(&conn, 22).unwrap();
    assert_eq!(retained(&conn), before);
    conn.commit_hook(None::<fn() -> bool>).unwrap();
    application_schema::upgrade_from_validated(&conn, 22).unwrap();
    assert_eq!(
        conn.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        application_schema::VERSION
    );
    assert_eq!(
        conn.query_row("SELECT count(*) FROM agent_retirements", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(retained(&conn), before);
    compatibility::validate_shape(&conn, application_schema::VERSION).unwrap();
    assert!(
        compatibility::validate_shape(&conn, 22).is_err(),
        "the old exact shape never silently accepts the additive table"
    );
    validate_existing(&Connection::new(&conn, &cfg.limits), &cfg, 23).unwrap();
    assert_eq!(application_schema::SUPPLIED_TURN_VERSION, 22);
}

#[test]
fn historical_marker_rejects_lifecycle_receipts_even_when_the_operation_was_refused() {
    let cfg = ControllerConfig::default();
    for command in [registration(), retirement()] {
        let conn = sql(22);
        record(
            &conn,
            &command,
            &Outcome::Refused {
                code: "controlled".into(),
                message: "no effect".into(),
            },
            1,
        );
        let before = retained(&conn);
        compatibility::validate_shape(&conn, 22).unwrap();
        assert!(validate_existing(&Connection::new(&conn, &cfg.limits), &cfg, 22).is_err());
        assert_eq!(retained(&conn), before);
        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            22
        );
    }
}

#[test]
fn tombstone_and_successful_receipt_require_bidirectional_exact_correspondence() {
    for defect in [
        "none",
        "missing-receipt",
        "missing-tombstone",
        "wrong-revision",
        "wrong-operation",
        "wrong-agent",
    ] {
        let cfg = ControllerConfig::default();
        let conn = sql(23);
        successful(&conn, &cfg, &registration(), 1);
        successful(&conn, &cfg, &retirement(), 2);
        validate_existing(&Connection::new(&conn, &cfg.limits), &cfg, 23).unwrap();
        match defect {
            "none" => (),
            "missing-receipt" => {
                conn.execute("DELETE FROM receipts WHERE id='retire'", [])
                    .unwrap();
            }
            "missing-tombstone" => {
                conn.execute("DELETE FROM agent_retirements", []).unwrap();
            }
            "wrong-revision" => {
                conn.execute(
                    "UPDATE agent_retirements SET definition_revision='another'",
                    [],
                )
                .unwrap();
            }
            "wrong-operation" => {
                conn.execute("UPDATE agent_retirements SET operation_id='register'", [])
                    .unwrap();
            }
            "wrong-agent" => {
                conn.execute("DELETE FROM agents", []).unwrap();
            }
            _ => unreachable!(),
        }
        let original = retained(&conn);
        let result = validate_existing(&Connection::new(&conn, &cfg.limits), &cfg, 23);
        assert_eq!(result.is_ok(), defect == "none", "{defect}: {result:?}");
        assert_eq!(retained(&conn), original);
    }
}

static NEXT: AtomicU64 = AtomicU64::new(1);
async fn response(c: &Controller, command: Operation, browser: bool, wire: u32) -> Reply {
    let request = Request {
        protocol_revision: wire,
        request_id: format!("directory-ui-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
        command,
    };
    if browser {
        c.handle_browser(request).await
    } else {
        c.handle(request).await
    }
}
async fn ui(c: &Controller, path: &str) -> crate::product::UiResponse {
    crate::product::handle(
        c,
        crate::product::UiRequest {
            method: "GET".into(),
            path: path.into(),
            body: vec![],
            origin: None,
            expected_origin: "http://localhost".into(),
        },
    )
    .await
}
#[tokio::test]
//= spec/impl/v2/implementation.md#agent-identity-lifecycle
//= type=test
//# A retired agent identity MUST remain resolvable for historical reads.
async fn actual_browser_cannot_mutate_directory_and_retired_detail_has_no_new_work_links() {
    let root = std::env::temp_dir().join(format!(
        "borg-agent-directory-ui-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let c = Controller::open(root.join("tasks.sqlite")).unwrap();
    let old = response(&c, registration(), false, 21).await;
    assert!(matches!(old.outcome,Outcome::Refused{ref code,..} if code=="unsupported_version"));
    assert!(old.commit.is_none());
    let browser = response(&c, registration(), true, WIRE_REVISION).await;
    assert!(
        matches!(browser.outcome,Outcome::Refused{ref code,..} if code=="governance_operator_required")
    );
    let mut register = registration();
    if let Operation::RegisterAgent { operation_id, .. } = &mut register {
        *operation_id = "actual-operator".into();
    }
    assert!(matches!(
        response(&c, register, false, WIRE_REVISION).await.outcome,
        Outcome::Ok {
            data: Data::Agent(_)
        }
    ));
    let browser = response(&c, retirement(), true, WIRE_REVISION).await;
    assert!(
        matches!(browser.outcome,Outcome::Refused{ref code,..} if code=="governance_operator_required")
    );
    let mut retire = retirement();
    if let Operation::RetireAgent { operation_id, .. } = &mut retire {
        *operation_id = "actual-retirement".into();
    }
    assert!(
        matches!(response(&c,retire,false,WIRE_REVISION).await.outcome,Outcome::Ok{data:Data::Agent(a)} if a.retired)
    );
    let detail = ui(&c, "/agents/directory-owner").await;
    assert_eq!(detail.status, 200);
    assert!(detail.body.contains("Retired"));
    assert!(detail.body.contains("All sessions"));
    assert!(detail.body.contains("Shared agent memory"));
    assert!(detail.body.contains("Exact agent revision"));
    assert!(!detail.body.contains("Start a new session"));
    assert!(!detail.body.contains("/settings"));
    assert_eq!(
        ui(&c, "/tasks/new?agent_id=directory-owner").await.status,
        409
    );
    assert_eq!(ui(&c, "/agents/directory-owner/settings").await.status, 409);
    let directory = ui(&c, "/agents").await;
    assert_eq!(directory.status, 200);
    assert!(!directory.body.contains("directory-owner"));
}
