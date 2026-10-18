//! Coordinated additive charter, governance and native-request records.
//! The database owner validates the original schema and records before writing.
use rusqlite::Connection;

// Schema18 adds credits for original-request diagnostics. Historical request
// receipts and schema17 output chunks remain byte-exact during migration.
// Schema19 fences new charter publication/adoption receipt and audit variants.
// Its SQL layout is identical to18; original records are never rewritten.
// Schema20 fences requested PR publication targets, typed effects and results.
// Existing tables and original historical rows are preserved byte-for-byte.
// Schema21 fences accepted instruction sources and the Instructions selector.
// This is a vocabulary-only transition with no table or row rewrite.
// Schema22 fences supplied public outer-turn intent/ACK/result vocabulary.
// SQL layout and existing records remain byte-exact.
pub(super) const SUPPLIED_TURN_VERSION: i64 = 22;
// Schema23 adds permanent agent tombstones and lifecycle receipt vocabulary.
// The distinct schema22 supplied-turn fence remains unchanged.
pub(super) const AGENT_DIRECTORY_VERSION: i64 = 23;
// Schema24 adds immutable per-session inputs and pending same-task handoffs,
// and fences root submission drafts/default-routing metadata. Existing row bodies stay exact.
pub(super) const DEFAULT_ASSIGNMENT_VERSION: i64 = 24;
// Schema25 jointly fences identity/task, machine read and supplied retry vocabulary.
// Existing schema24 rows and SQL shape remain byte-exact.
pub(super) const IDENTITY_ADMINISTRATION_VERSION: i64 = 25;
pub(super) const MACHINE_RESOURCES_VERSION: i64 = 25;
pub(super) const SUPPLIED_RETRY_VERSION: i64 = 25;
// One vocabulary generation contains identity fields, paginated reads and read retention.
pub(super) const IDENTITY_LIMIT_FIELDS_VERSION: i64 = 26;
pub(super) const READ_PAGINATION_VERSION: i64 = 26;
pub(super) const VERSION: i64 = 26;
const SESSION_HANDOFF_DELTA: &str = include_str!("schema24-from23.sql");
const AGENT_DIRECTORY_DELTA: &str = include_str!("schema23-from22.sql");
const DELTA: &str = include_str!("schema14-from13.sql");
const OUTPUT_DELTA:&str=include_str!("schema17-from16.sql");
const DIAGNOSTIC_DELTA: &str = include_str!("schema18-from17.sql");

fn install14_in_transaction(conn: &Connection) -> Result<(), String> {
    install14_checked(conn).map_err(|e| format!("{e:?}"))
}
fn install14_checked(conn: &Connection) -> super::database::Result<()> {
    if conn.is_autocommit() {
        return Err(super::database::Fault::Store("application schema installation requires the owner transaction".into()));
    }
    let old: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if old != super::workflow_schema::VERSION {
        return Err(super::database::Fault::Store("application migration requires validated schema13".into()));
    }
    Ok(conn.execute_batch(DELTA)?)
}

pub(super) fn install_in_transaction(conn: &Connection) -> Result<(), String> {
    install_checked(conn).map_err(|e| format!("{e:?}"))
}
pub(super) fn install_checked(conn: &Connection) -> super::database::Result<()> {
    install14_checked(conn)?;
    conn.execute_batch(OUTPUT_DELTA)?;
    if VERSION >= 18 { conn.execute_batch(DIAGNOSTIC_DELTA)?; }
    conn.execute_batch(AGENT_DIRECTORY_DELTA)?;
    conn.execute_batch(SESSION_HANDOFF_DELTA)?;
    Ok(conn.pragma_update(None, "user_version", VERSION)?)
}

/// The production startup owner supplies the checked outer transaction and its
/// retained original version. This body performs no process or filesystem work.

// Traceability scope: Only the explicitly supported historical versions migrate under the validated owner transaction.
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//# A change to stored task state representation MUST have an explicit migration disposition.

pub(super) fn upgrade_checked(conn: &Connection, expected_old: i64) -> super::database::Result<()> {
    use super::database::Fault;
    if conn.is_autocommit() || !matches!(expected_old, 12 | 13 | 14 | 15 | 16 | 17 | 18 | 19 | 20 | 21 | 22 | 23 | 24 | 25) || expected_old >= VERSION {
        return Err(Fault::Store("application migration requires an owned validated historical transaction".into()));
    }
    let actual: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if actual != expected_old { return Err(Fault::Store("historical schema changed after validation".into())); }
    if actual == 12 { super::workflow_schema::install_checked(conn)?; }
    if actual < 14 { install14_checked(conn)?; }
    if actual < 17 { conn.execute_batch(OUTPUT_DELTA)?; }
    if actual < 18 { conn.execute_batch(DIAGNOSTIC_DELTA)?; }
    if actual < AGENT_DIRECTORY_VERSION { conn.execute_batch(AGENT_DIRECTORY_DELTA)?; }
    if actual < DEFAULT_ASSIGNMENT_VERSION { conn.execute_batch(SESSION_HANDOFF_DELTA)?; }
    Ok(conn.pragma_update(None, "user_version", VERSION)?)
}

pub(super) fn upgrade_validated(conn: &Connection) -> Result<(), String> {
    if !conn.is_autocommit() {
        return Err("application migration cannot nest in an existing transaction".into());
    }
    let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
    install_in_transaction(&tx)?;
    tx.commit().map_err(|e| e.to_string())
}

/// The normal startup path uses one transaction for every necessary additive
/// step. In particular, schema12 cannot be left at schema13 if COMMIT fails.
pub(super) fn upgrade_from_validated(conn: &Connection, expected_old: i64) -> Result<(), String> {
    if !conn.is_autocommit() || !matches!(expected_old, 12 | 13 | 14 | 15 | 16 | 17 | 18 | 19 | 20 | 21 | 22 | 23 | 24 | 25)
        || expected_old >= VERSION {
        return Err("application migration requires an unnested validated historical store".into());
    }
    let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
    let actual: i64 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))
        .map_err(|e| e.to_string())?;
    if actual != expected_old {
        return Err("historical schema changed after validation".into());
    }
    if actual == 12 {
        super::workflow_schema::install_in_transaction(&tx)?;
    }
    if actual < 14 { install14_in_transaction(&tx)?; }
    if actual < 17 { tx.execute_batch(OUTPUT_DELTA).map_err(|e|e.to_string())?; }
    if actual < 18 { tx.execute_batch(DIAGNOSTIC_DELTA).map_err(|e| e.to_string())?; }
    if actual < AGENT_DIRECTORY_VERSION { tx.execute_batch(AGENT_DIRECTORY_DELTA).map_err(|e| e.to_string())?; }
    if actual < DEFAULT_ASSIGNMENT_VERSION { tx.execute_batch(SESSION_HANDOFF_DELTA).map_err(|e| e.to_string())?; }
    tx.pragma_update(None, "user_version", VERSION).map_err(|e| e.to_string())?;
    tx.commit().map_err(|e| e.to_string())
}

pub(super) fn expected_schema(conn: &Connection, version: i64) -> Result<(), String> {
    match version {
        12 | 13 => super::workflow_schema::expected_schema(conn, version),
        14 | 15 | 16 => {
            super::workflow_schema::expected_schema(conn, super::workflow_schema::VERSION)?;
            let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
            install14_in_transaction(&tx)?;
            tx.pragma_update(None, "user_version", version).map_err(|e| e.to_string())?;
            tx.commit().map_err(|e| e.to_string())
        }
        17 if VERSION > 17 => {
            super::workflow_schema::expected_schema(conn, super::workflow_schema::VERSION)?;
            let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
            install14_in_transaction(&tx)?;
            tx.execute_batch(OUTPUT_DELTA).map_err(|e| e.to_string())?;
            tx.pragma_update(None, "user_version", 17).map_err(|e| e.to_string())?;
            tx.commit().map_err(|e| e.to_string())
        }
        18 => {
            expected_schema(conn,17)?;
            let tx=conn.unchecked_transaction().map_err(|e|e.to_string())?;
            tx.execute_batch(DIAGNOSTIC_DELTA).map_err(|e|e.to_string())?;
            tx.pragma_update(None,"user_version",18).map_err(|e|e.to_string())?;
            tx.commit().map_err(|e|e.to_string())
        }
        19 => {
            expected_schema(conn,18)?;
            conn.pragma_update(None,"user_version",19).map_err(|e|e.to_string())
        }
        20 => {
            expected_schema(conn,19)?;
            conn.pragma_update(None,"user_version",20).map_err(|e|e.to_string())
        }
        21 => {
            expected_schema(conn,20)?;
            conn.pragma_update(None,"user_version",21).map_err(|e|e.to_string())
        }
        SUPPLIED_TURN_VERSION => {
            expected_schema(conn,21)?;
            conn.pragma_update(None,"user_version",SUPPLIED_TURN_VERSION).map_err(|e|e.to_string())
        }
        AGENT_DIRECTORY_VERSION => {
            expected_schema(conn,SUPPLIED_TURN_VERSION)?;
            conn.execute_batch(AGENT_DIRECTORY_DELTA).map_err(|e|e.to_string())?;
            conn.pragma_update(None,"user_version",AGENT_DIRECTORY_VERSION).map_err(|e|e.to_string())
        }
        DEFAULT_ASSIGNMENT_VERSION => {
            expected_schema(conn, AGENT_DIRECTORY_VERSION)?;
            conn.execute_batch(SESSION_HANDOFF_DELTA).map_err(|e|e.to_string())?;
            conn.pragma_update(None,"user_version",DEFAULT_ASSIGNMENT_VERSION).map_err(|e|e.to_string())
        }
        25 => {
            expected_schema(conn, DEFAULT_ASSIGNMENT_VERSION)?;
            conn.pragma_update(None, "user_version", 25).map_err(|e| e.to_string())
        }
        VERSION => {
            super::workflow_schema::expected_schema(conn, super::workflow_schema::VERSION)?;
            upgrade_validated(conn)
        }
        _ => Err("unsupported expected application schema".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn original() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        expected_schema(&conn, 13).unwrap();
        conn.execute("INSERT INTO receipts(id,request,outcome,cleanup,controller_revision) VALUES('original',?1,?2,0,1)",
            ["original request bytes", "original outcome bytes"]).unwrap();
        conn
    }
    fn retained(conn: &Connection) -> (String, String, String) {
        let identity = conn.query_row("SELECT value FROM settings WHERE name='database_id'", [], |r|r.get(0)).unwrap();
        let (request,outcome) = conn.query_row("SELECT request,outcome FROM receipts WHERE id='original'", [], |r|Ok((r.get(0)?,r.get(1)?))).unwrap();
        (identity,request,outcome)
    }
    fn shape(conn: &Connection) -> Vec<(String,String,Option<String>)> {
        let mut q = conn.prepare("SELECT type,name,sql FROM sqlite_master ORDER BY type,name").unwrap();
        q.query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap()
            .collect::<Result<Vec<_>,_>>().unwrap()
    }

    #[test]
    fn additive_application_upgrade_preserves_original_bytes_without_policy_activation() {
        let conn=original();let before=retained(&conn);
        upgrade_validated(&conn).unwrap();
        assert_eq!(retained(&conn),before);
        assert_eq!(conn.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),VERSION);
        assert!(conn.query_row("SELECT governance FROM receipts WHERE id='original'",[],|r|r.get::<_,Option<String>>(0)).unwrap().is_none());
        for table in ["governance_rules","governance_configurations","governance_state","governance_holds","native_request_coordination"] {
            assert_eq!(conn.query_row(&format!("SELECT count(*) FROM {table}"),[],|r|r.get::<_,i64>(0)).unwrap(),0);
        }
        if VERSION >= 18 {
            assert_eq!(conn.query_row("SELECT count(*) FROM native_request_diagnostic_credits", [], |r| r.get::<_,i64>(0)).unwrap(), 0);
        }
        let expected=Connection::open_in_memory().unwrap();expected_schema(&expected,VERSION).unwrap();
        assert_eq!(shape(&conn),shape(&expected));
    }

    #[test]
    fn failed_application_upgrade_commit_preserves_old_shape_and_records() {
        let conn=original();let before=retained(&conn);let old_shape=shape(&conn);
        conn.commit_hook(Some(||true)).unwrap();
        assert!(upgrade_validated(&conn).is_err());
        assert!(conn.is_autocommit());assert_eq!(retained(&conn),before);assert_eq!(shape(&conn),old_shape);
        assert_eq!(conn.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),13);
        conn.commit_hook(None::<fn()->bool>).unwrap();
        upgrade_validated(&conn).unwrap();assert_eq!(retained(&conn),before);
    }

    #[test]
    fn complete_historical_upgrade_is_atomic_for_all_supported_inputs() {
        for version in [12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25].into_iter().filter(|version| *version < VERSION) {
            let conn=Connection::open_in_memory().unwrap();
            expected_schema(&conn,version).unwrap();
            assert_eq!(conn.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),version);
            conn.execute("INSERT INTO receipts(id,request,outcome,cleanup,controller_revision) VALUES('original',?1,?2,0,1)",
                ["retained request", "retained outcome"]).unwrap();
            let before=retained(&conn);let old_shape=shape(&conn);
            // A stale expected version cannot start either step.
            assert!(upgrade_from_validated(&conn,if version==12 {13}else{12}).is_err());
            assert_eq!(shape(&conn),old_shape);
            let outer=conn.unchecked_transaction().unwrap();
            assert!(upgrade_from_validated(&outer,version).is_err());
            outer.rollback().unwrap();
            conn.commit_hook(Some(||true)).unwrap();
            assert!(upgrade_from_validated(&conn,version).is_err());
            assert!(conn.is_autocommit());assert_eq!(shape(&conn),old_shape);
            assert_eq!(retained(&conn),before);
            assert_eq!(conn.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),version);
            conn.commit_hook(None::<fn()->bool>).unwrap();
            upgrade_from_validated(&conn,version).unwrap();
            assert_eq!(conn.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),VERSION);
            assert_eq!(retained(&conn),before);
            let expected=Connection::open_in_memory().unwrap();expected_schema(&expected,VERSION).unwrap();
            assert_eq!(shape(&conn),shape(&expected));
            let after=shape(&conn);assert!(upgrade_from_validated(&conn,VERSION).is_err());
            assert_eq!(shape(&conn),after);
        }
    }

    #[test]
    fn application_upgrade_refuses_nested_repeated_and_wrong_version() {
        let conn=original();let before=shape(&conn);
        let tx=conn.unchecked_transaction().unwrap();
        assert!(upgrade_validated(&tx).is_err());assert_eq!(shape(&tx),before);tx.rollback().unwrap();
        upgrade_validated(&conn).unwrap();let after=shape(&conn);
        assert!(upgrade_validated(&conn).is_err());assert_eq!(shape(&conn),after);
        let old=Connection::open_in_memory().unwrap();expected_schema(&old,12).unwrap();let old_shape=shape(&old);
        assert!(upgrade_validated(&old).is_err());assert_eq!(shape(&old),old_shape);
        let unknown=Connection::open_in_memory().unwrap();
        assert!(expected_schema(&unknown,VERSION+1).is_err());assert!(shape(&unknown).is_empty());
    }
}
