//! Controller startup admission through the real bounded validator. Historical
//! marker/body injections are explicit fixtures, not claims of an old producer.
#[path = "../identity_limit_fields_support.rs"]
mod support;
use support::*;
use borg_runtime::{controller::Controller, limits::DefinitionLimitField, protocol::*};
use serde_json::json;
use std::{collections::BTreeMap, path::Path};

fn version(path: &Path) -> i64 {
    rusqlite::Connection::open(path).unwrap().query_row("PRAGMA user_version", [], |r| r.get(0)).unwrap()
}
fn set_version(path: &Path, value: i64) {
    rusqlite::Connection::open(path).unwrap().pragma_update(None, "user_version", value).unwrap();
}

// Preserve every typed SQL value in every original table, including exact JSON
// strings. The format-only transition is allowed to change only user_version.
fn rows(path: &Path) -> BTreeMap<String, Vec<String>> {
    let c = rusqlite::Connection::open(path).unwrap();
    let mut q = c.prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").unwrap();
    let names = q.query_map([], |r| r.get::<_, String>(0)).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    let mut result = BTreeMap::new();
    for name in names {
        let mut q = c.prepare(&format!("SELECT * FROM \"{}\"", name.replace('"', "\"\""))).unwrap();
        let columns = q.column_count();
        let mut values = q.query_map([], |r| {
            let row = (0..columns).map(|i| r.get::<_, rusqlite::types::Value>(i)).collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(format!("{row:?}"))
        }).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        values.sort();
        result.insert(name, values);
    }
    result
}

async fn refuses_without_rewrite(path: &Path, cfg: &borg_runtime::controller::ControllerConfig) {
    let original_rows = rows(path);
    let original_bytes = std::fs::read(path).unwrap();
    let marker = version(path);
    let error = match Controller::open_with_config(path, cfg.clone()) {
        Ok(c) => { drop(c); owner_closed(path).await; panic!("historical field vocabulary admitted") },
        Err(error) => error,
    };
    assert!(error.contains("identity limit field admission") && error.contains("schema26"), "{error}");
    owner_closed(path).await;
    assert_eq!(version(path), marker);
    assert_eq!(rows(path), original_rows);
    assert_eq!(std::fs::read(path).unwrap(), original_bytes);
}

#[tokio::test]
async fn identity_limit_fields_schema25_ordinary_numeric_values_upgrade_without_row_rewrite() {
    let h = Harness::new("identity-limit-fields-old-numbers", "charter", ExecutorKind::Native, true).await;
    let original = h.current().await;
    let values = original.input.execution_limits().unwrap();
    assert!(values.coding.frame_bytes > 0 && values.collaboration.journal_bytes > 0);
    assert!(!h.cfg.limits.definition_updates.as_ref().unwrap().allowed_fields.iter().any(|f| FIELDS.contains(f)));
    // Ordinary identity text is not a typed DefinitionLimitField position.
    let d = inherited(&h.c, &h.cfg.limits, "coding.frame_bytes-collaboration.journal_bytes").await;
    let cfg = h.cfg.clone(); let path = h.db.clone();
    h.close().await;
    let current = version(&path); assert!(current > 25);
    set_version(&path, 25);
    let before = rows(&path);
    let c = Controller::open_with_config(&path, cfg).unwrap();
    assert_eq!(task(api(&c, Operation::GetTask { task_id: original.id.clone() }).await).input, original.input);
    assert_eq!(selected(&c, &d.id).await.definition, d);
    drop(c); owner_closed(&path).await;
    assert_eq!(version(&path), current);
    assert_eq!(rows(&path), before);
}

#[tokio::test]
async fn identity_limit_fields_refused_original_requests_fence_schema25_then_replay_current_exactly() {
    let h = Harness::new("identity-limit-fields-refused-history", "charter", ExecutorKind::Native, true).await;
    let d = selected(&h.c, "charter").await.definition;
    let values = h.cfg.limits.resolve_definition(&d).unwrap();
    let mut originals = Vec::new();
    for field in FIELDS {
        let call = invocation(&h.caller, "collab_definition_update", numeric_args(&h.cfg.limits, &d,
            changes(&[(field, field.value(&values).unwrap() + 1)])));
        let reply = h.invoke(&call).await;
        assert_eq!(refused(reply.clone()), "configuration_policy");
        assert!(reply.commit.is_some());
        originals.push((call, reply));
    }
    let input = h.current().await.input;
    let cfg = h.cfg.clone(); let path = h.db.clone();
    h.close().await;
    let current = version(&path); assert!(current > 25);
    set_version(&path, 25);
    refuses_without_rewrite(&path, &cfg).await;
    set_version(&path, current);
    let before = rows(&path);
    let c = Controller::open_with_config(&path, cfg).unwrap();
    for (call, original) in originals {
        let replay = api(&c, call.operation()).await;
        assert_eq!(replay.outcome, original.outcome);
        assert_eq!(replay.commit, original.commit);
    }
    assert_eq!(selected(&c, &d.id).await.definition, d);
    let Data::Tasks(tasks) = ok(api(&c, Operation::ListTasks { after: None, limit: 100 }).await) else { panic!("tasks") };
    assert!(tasks.iter().any(|task| task.input == input));
    drop(c); owner_closed(&path).await;
    assert_eq!(rows(&path), before);
}

#[tokio::test]
async fn identity_limit_fields_outcome_only_and_nested_typed_receipts_fence_before_upgrade() {
    let h = Harness::new("identity-limit-fields-outcome-history", "charter", ExecutorKind::Native, true).await;
    let d = selected(&h.c, "charter").await.definition;
    let values = h.cfg.limits.resolve_definition(&d).unwrap();
    // The real original has an old field and no new tags in its request, task,
    // policy or governance. Each injection below changes only its outcome.
    let call = invocation(&h.caller, "collab_definition_update", numeric_args(&h.cfg.limits, &d,
        changes(&[(DefinitionLimitField::NativeContextBytes, values.native.context_bytes + 1)])));
    let reply = h.invoke(&call).await;
    let original_update = updated(&reply);
    assert!(!original_update.policy.allowed_fields.iter().any(|f| FIELDS.contains(f)));
    let Data::Operation(original_operation) = ok(api(&h.c, Operation::GetOperation {
        operation_id: call.operation_id.clone(),
    }).await) else { panic!("original operation") };
    let cfg = h.cfg.clone(); let path = h.db.clone();
    h.close().await;
    let current = version(&path); assert!(current > 25);
    let c = rusqlite::Connection::open(&path).unwrap();
    let original: String = c.query_row("SELECT outcome FROM receipts WHERE id=?1", [&call.operation_id], |r| r.get(0)).unwrap();
    drop(c);
    let original_rows = rows(&path);
    for field in FIELDS {
        let mut changed = original_update.clone();
        changed.changed_fields = vec![field];
        let tagged = Outcome::Ok { data: Data::DefinitionUpdate(Box::new(changed)) };
        let mut nested_record = original_operation.clone();
        nested_record.outcome = Some(serde_json::to_value(&tagged).unwrap());
        let nested = Outcome::Ok { data: Data::Operation(nested_record) };
        for outcome in [tagged, nested] {
            let c = rusqlite::Connection::open(&path).unwrap();
            assert_eq!(c.execute("UPDATE receipts SET outcome=?1 WHERE id=?2",
                rusqlite::params![serde_json::to_string(&outcome).unwrap(), call.operation_id]).unwrap(), 1);
            c.pragma_update(None, "user_version", 25).unwrap(); drop(c);
            refuses_without_rewrite(&path, &cfg).await;
            let c = rusqlite::Connection::open(&path).unwrap();
            assert_eq!(c.execute("UPDATE receipts SET outcome=?1 WHERE id=?2", rusqlite::params![original, call.operation_id]).unwrap(), 1);
            c.pragma_update(None, "user_version", current).unwrap(); drop(c);
            assert_eq!(rows(&path), original_rows);
        }
    }
    let c = Controller::open_with_config(&path, cfg).unwrap();
    let replay = api(&c, call.operation()).await;
    assert_eq!(replay.outcome, reply.outcome); assert_eq!(replay.commit, reply.commit);
    drop(c); owner_closed(&path).await;
    assert_eq!(rows(&path), original_rows);
}

#[tokio::test]
async fn identity_limit_fields_current_selected_policy_and_original_success_refusal_reopen_exactly() {
    let h = Harness::with_policy("identity-limit-fields-current-history", "charter", ExecutorKind::Native, true, policy()).await;
    let input = h.current().await.input;
    let mut d = selected(&h.c, "charter").await.definition;
    let mut originals = Vec::new();
    for field in FIELDS {
        let value = field.value(&h.cfg.limits.resolve_definition(&d).unwrap()).unwrap() + 1;
        let call = invocation(&h.caller, "collab_definition_update", numeric_args(&h.cfg.limits, &d, changes(&[(field, value)])));
        let reply = h.invoke(&call).await;
        d = updated(&reply).selected_definition;
        let stale = invocation(&h.caller, "collab_definition_update", call.arguments.clone());
        let refusal = h.invoke(&stale).await;
        assert_eq!(refused(refusal.clone()), "configuration_conflict");
        originals.extend([(call, reply), (stale, refusal)]);
    }
    let cfg = h.cfg.clone(); let path = h.db.clone(); let task_id = h.original.id.clone();
    h.close().await;
    let current = version(&path); assert!(current > 25);
    let before = rows(&path);
    let c = Controller::open_with_config(&path, cfg).unwrap();
    for (call, original) in originals {
        let replay = api(&c, call.operation()).await;
        assert_eq!(replay.outcome, original.outcome);
        assert_eq!(replay.commit, original.commit);
        let lookup = api(&c, Operation::GetCollaborationCall { caller: call.caller.clone(), operation_id: call.operation_id.clone() }).await;
        let recovered = borg_runtime::mcp::recovered(&call, lookup).unwrap();
        assert_eq!(recovered.reply.outcome, original.outcome);
        assert_eq!(recovered.reply.commit, original.commit);
    }
    assert_eq!(selected(&c, &d.id).await.definition, d);
    assert_eq!(task(api(&c, Operation::GetTask { task_id }).await).input, input);
    drop(c); owner_closed(&path).await;
    assert_eq!(version(&path), current);
    assert_eq!(rows(&path), before);
}
