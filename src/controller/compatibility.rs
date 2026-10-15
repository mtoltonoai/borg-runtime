//! Explicit storage barrier. No backfill, implicit inference, or data rewrite.
use super::database::decode;
use crate::protocol::*;
use rusqlite::Connection;

fn objects(conn: &Connection) -> Result<Vec<(String, String, String, Option<String>)>, String> {
    let mut q = conn
        .prepare("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name")
        .map_err(|e| e.to_string())?;
    q.query_map([], |r| {
        Ok((
            r.get(0)?,
            r.get(1)?,
            r.get(2)?,
            r.get::<_, Option<String>>(3)?
                .map(|s| s.split_whitespace().collect::<Vec<_>>().join(" ")),
        ))
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}
pub(super) fn validate_shape(conn: &Connection) -> Result<(), String> {
    let expected = Connection::open_in_memory().map_err(|e| e.to_string())?;
    expected
        .execute_batch(include_str!("schema12.sql"))
        .map_err(|e| e.to_string())?;
    if objects(conn)? != objects(&expected)? {
        return Err("unsupported controller schema shape; original data retained".into());
    }
    Ok(())
}
fn records<T: serde::de::DeserializeOwned + serde::Serialize>(
    conn: &Connection,
    sql: &str,
    mut check: impl FnMut(T) -> Result<(), String>,
) -> Result<(), String> {
    let mut q = conn.prepare(sql).map_err(|e| e.to_string())?;
    let mut rows = q.query([]).map_err(|e| e.to_string())?;
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let text: String = row.get(0).map_err(|e| e.to_string())?;
        let value = decode(&text).map_err(|_| "unsupported stored canonical record".to_string())?;
        check(value)?;
    }
    Ok(())
}
pub(super) fn validate_records(conn: &Connection) -> Result<(), String> {
    for sql in [
        "SELECT definition FROM agents",
        "SELECT body FROM definitions",
    ] {
        records::<Definition>(conn, sql, |_| Ok(()))?;
    }
    records::<Worker>(conn, "SELECT body FROM workers", |_| Ok(()))?;
    Ok(())
}
