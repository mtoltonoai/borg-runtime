//! Additive workflow storage. The caller validates all old records read-only
//! before allowing this transaction; migration never rewrites their bodies.
use rusqlite::Connection;

pub(super) const VERSION: i64 = 13;
const DELTA: &str = include_str!("schema13-from12.sql");

pub(super) fn install_in_transaction(conn: &Connection) -> Result<(), String> {
    install_checked(conn).map_err(|e| format!("{e:?}"))
}

pub(super) fn install_checked(conn: &Connection) -> super::database::Result<()> {
    if conn.is_autocommit() {
        return Err(super::database::Fault::Store("workflow schema installation requires the owner transaction".into()));
    }
    let old: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if old != 12 {
        return Err(super::database::Fault::Store("workflow migration requires validated schema12".into()));
    }
    Ok(conn.execute_batch(DELTA)?)
}

/// Only invoked after the database owner's complete historical validation and
/// process reconciliation. Any error leaves the transaction uncommitted.
pub(super) fn upgrade_validated(conn: &Connection) -> Result<(), String> {
    if !conn.is_autocommit() {
        return Err("workflow migration cannot nest in an existing transaction".into());
    }
    let transaction = conn.unchecked_transaction().map_err(|e| e.to_string())?;
    install_in_transaction(&transaction)?;
    transaction.commit().map_err(|e| e.to_string())
}

pub(super) fn expected_schema(conn: &Connection, version: i64) -> Result<(), String> {
    conn.execute_batch(include_str!("schema12.sql")).map_err(|e| e.to_string())?;
    match version {
        12 => Ok(()),
        VERSION => upgrade_validated(conn),
        _ => Err("unsupported expected controller schema".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn old() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        expected_schema(&conn, 12).unwrap();
        conn.execute("INSERT INTO sessions VALUES('session',1,'agent','task',?1,7,9,?2)",
            ["retained session bytes", "retained memory pending bytes"]).unwrap();
        conn.execute("INSERT INTO receipts VALUES('operation',?1,?2,0,1)",
            ["retained request bytes", "retained response bytes"]).unwrap();
        conn
    }
    fn originals(conn: &Connection) -> Vec<String> {
        let mut output = Vec::new();
        for sql in [
            "SELECT value FROM settings WHERE name='database_id'",
            "SELECT body FROM sessions", "SELECT memory_pending FROM sessions",
            "SELECT request FROM receipts", "SELECT outcome FROM receipts",
        ] { output.push(conn.query_row(sql, [], |r| r.get(0)).unwrap()); }
        output
    }
    fn shape(conn: &Connection) -> Vec<(String,String,Option<String>)> {
        let mut q=conn.prepare("SELECT type,name,sql FROM sqlite_master ORDER BY type,name").unwrap();
        q.query_map([], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap()
            .collect::<Result<Vec<_>,_>>().unwrap()
    }

    #[test]
    fn schema_upgrade_preserves_original_records_and_keeps_delivery_absent() {
        let conn=old();let before=originals(&conn);
        upgrade_validated(&conn).unwrap();
        assert_eq!(originals(&conn),before);
        assert_eq!(conn.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),VERSION);
        assert_eq!(conn.query_row("SELECT value FROM settings WHERE name='workflow_revision'",[],|r|r.get::<_,String>(0)).unwrap(),"0");
        assert!(conn.query_row("SELECT workflow_delivery FROM sessions",[],|r|r.get::<_,Option<String>>(0)).unwrap().is_none());
        let expected=Connection::open_in_memory().unwrap();expected_schema(&expected,VERSION).unwrap();
        assert_eq!(shape(&conn),shape(&expected));
    }

    #[test]
    fn failed_migration_commit_rolls_back_schema_and_retained_bytes() {
        let conn=old();let before=originals(&conn);let schema=shape(&conn);
        conn.commit_hook(Some(|| true)).unwrap();
        assert!(upgrade_validated(&conn).is_err());
        assert!(conn.is_autocommit());assert_eq!(shape(&conn),schema);assert_eq!(originals(&conn),before);
        assert_eq!(conn.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),12);
        conn.commit_hook(None::<fn()->bool>).unwrap();
        upgrade_validated(&conn).unwrap();assert_eq!(originals(&conn),before);
    }

    #[test]
    fn migration_refuses_nested_or_repeated_execution_without_changing_state() {
        let conn=old();let before=shape(&conn);
        let outer=conn.unchecked_transaction().unwrap();
        assert!(upgrade_validated(&outer).is_err());assert_eq!(shape(&outer),before);
        outer.rollback().unwrap();
        upgrade_validated(&conn).unwrap();let after=shape(&conn);
        assert!(upgrade_validated(&conn).is_err());assert_eq!(shape(&conn),after);
    }
}
