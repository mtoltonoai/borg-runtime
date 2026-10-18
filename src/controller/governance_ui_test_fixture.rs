//! Test-only, one-shot fault injection on the actual isolated database owner's
//! connection after ordinary startup validation. No public command or normal
//! runtime symbol exposes this fixture.
use std::{collections::BTreeMap, path::{Path, PathBuf}, sync::{Arc, Mutex, OnceLock, atomic::{AtomicBool, Ordering}}};

fn pending() -> &'static Mutex<BTreeMap<PathBuf, Arc<AtomicBool>>> {
    static PENDING: OnceLock<Mutex<BTreeMap<PathBuf, Arc<AtomicBool>>>> = OnceLock::new();
    PENDING.get_or_init(|| Mutex::new(BTreeMap::new()))
}
pub(crate) struct MissingSelection {
    path: PathBuf,
    applied: Arc<AtomicBool>,
}
impl MissingSelection {
    pub(crate) fn assert_applied(&self) {
        assert!(self.applied.load(Ordering::SeqCst), "the actual owner must consume this exact path's fixture");
    }
}
impl Drop for MissingSelection {
    fn drop(&mut self) {
        // Also removes an unused registration if real startup fails first.
        pending().lock().unwrap().remove(&self.path);
    }
}
pub(crate) fn missing_selection(path: &Path) -> MissingSelection {
    let applied = Arc::new(AtomicBool::new(false));
    assert!(pending().lock().unwrap().insert(path.to_owned(), applied.clone()).is_none());
    MissingSelection { path: path.to_owned(), applied }
}
pub(super) fn after_validated_open(path: &Path, conn: &rusqlite::Connection) -> Result<(), String> {
    let Some(applied) = pending().lock().unwrap().remove(path) else { return Ok(()); };
    let occupied: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM tasks) OR EXISTS(SELECT 1 FROM governance_state)", [], |r| r.get(0),
    ).map_err(|e| e.to_string())?;
    if occupied { return Err("governance UI fixture requires its fresh isolated owner".into()); }
    let raw = serde_json::to_string(&crate::governance::GovernanceState {
        selected_revision: "missing-ordinary-policy".into(), temporary_override: None,
    }).map_err(|e| e.to_string())?;
    conn.execute("INSERT INTO governance_state(singleton,body) VALUES(1,?1)", [&raw])
        .map_err(|e| e.to_string())?;
    applied.store(true, Ordering::SeqCst);
    Ok(())
}
