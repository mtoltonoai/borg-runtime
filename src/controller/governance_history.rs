//! Original governance metadata evidence. No current selection or rule effect is
//! applied here; the same original receipt owns each future snapshot.
use super::*;
use super::super::database::{decode, sql_bound};

fn checked(record:&DecisionRecord,metadata:&crate::limits::RuntimeLimits)->Result<()> {
    metadata.validate().map_err(Fault::Store)?;
    if metadata.revision!=record.configuration.global_revision
        ||metadata.definition_updates.as_ref().map(|p|&p.revision)
            !=record.configuration.definition_update_policy_revision.as_ref() {
        return Err(Fault::Store("governance metadata differs from original configuration binding".into()))
    }
    Ok(())
}
fn add(values:&mut BTreeMap<String,crate::limits::RuntimeLimits>,value:&crate::limits::RuntimeLimits)->Result<()> {
    match values.get(&value.revision) {
        Some(original) if original!=value=>Err(Fault::Store("governance metadata revision has contradictory retained evidence".into())),
        Some(_)=>Ok(()),
        None=>{values.insert(value.revision.clone(),value.clone());Ok(())}
    }
}
/// Bounded inventories reuse the enclosing receipt/task capacities. New audit
/// snapshots remain available when no task exists. Legacy rows may additionally
/// use an exact accepted snapshot, never a guessed current revision or default.
fn inventory(tx:&StoreContext<'_>)->Result<BTreeMap<String,crate::limits::RuntimeLimits>> {
    let mut values=BTreeMap::new();add(&mut values,tx.governance_limits)?;
    let bounds=limits(tx)?;
    let maximum=tx.governance_limits.controller.max_receipts.checked_add(tx.governance_limits.controller.max_cleanup_receipts)
        .ok_or_else(||Fault::Store("governance metadata receipt capacity overflow".into()))?;
    let mut query=tx.prepare("SELECT CAST(substr(CAST(id AS BLOB),1,?1) AS TEXT),CAST(substr(CAST(governance AS BLOB),1,?2) AS TEXT),controller_revision FROM receipts WHERE governance IS NOT NULL ORDER BY controller_revision LIMIT ?3")?;
    let mut rows=query.query(params![sql_bound(tx.governance_limits.transport.identity_bytes.saturating_add(1))?,sql_bound(bounds.record_bytes.saturating_add(1))?,sql_bound(maximum.saturating_add(1))?])?;
    let mut count=0;
    while let Some(row)=rows.next()? {
        count+=1;if count>maximum{return Err(Fault::Store("governance metadata inventory exceeds receipt bound".into()))}
        let id:String=row.get(0)?;identity(tx,&id)?;
        let raw:String=row.get(1)?;let revision:i64=row.get(2)?;
        if raw.len()>bounds.record_bytes{return Err(Fault::Store("governance metadata audit exceeds bound".into()))}
        let records:Vec<DecisionRecord>=decode(&raw)?;
        if records.is_empty()||records.len()>5{return Err(Fault::Store("governance metadata audit has invalid decision set".into()))}
        let original=original_receipt(tx,&id)?.ok_or_else(||Fault::Store("governance metadata lacks original receipt".into()))?;
        for record in records {
            if record.id.operation_id!=id||super::super::database::integer(record.controller_revision)?!=revision
                ||record.controller_revision!=original.commit.controller_revision {
                return Err(Fault::Store("governance metadata differs from original receipt identity".into()))
            }
            if let Some(metadata)=record.configuration_metadata.as_ref() {checked(&record,metadata)?;add(&mut values,metadata)?;}
        }
    }
    let task_max=tx.governance_limits.controller.max_tasks;
    let mut query=tx.prepare("SELECT CAST(substr(CAST(id AS BLOB),1,?1) AS TEXT) FROM tasks ORDER BY seq LIMIT ?2")?;
    let ids=query.query_map(params![sql_bound(tx.governance_limits.transport.identity_bytes.saturating_add(1))?,sql_bound(task_max.saturating_add(1))?],|r|r.get::<_,String>(0))?
        .collect::<std::result::Result<Vec<_>,_>>()?;
    if ids.len()>task_max{return Err(Fault::Store("governance metadata inventory exceeds task bound".into()))}
    for id in ids {
        let task=super::super::database::load(tx,&id)?.task;
        if let Some(accepted)=task.input.resolved_limits.as_ref() {
            // load already validates the accepted snapshot against its original
            // TaskInput. Same immutable revision must retain identical metadata.
            add(&mut values,&accepted.global_metadata)?;
        }
    }
    Ok(values)
}
pub(super) fn validate_metadata_inventory(tx:&StoreContext<'_>)->Result<()> {inventory(tx).map(|_|())}
pub(super) fn metadata_at_revision(tx:&StoreContext<'_>,revision:&str)->Result<Option<crate::limits::RuntimeLimits>> {
    identity(tx,revision)?;
    Ok(inventory(tx)?.remove(revision))
}
pub(super) fn decision_metadata(tx:&StoreContext<'_>,record:&DecisionRecord)->Result<crate::limits::RuntimeLimits> {
    let metadata=match record.configuration_metadata.as_deref() {
        Some(value)=>value.clone(),None=>metadata_at_revision(tx,&record.configuration.global_revision)?
            .ok_or_else(||Fault::Store("original governance metadata unavailable; retained legacy bytes preserved".into()))?,
    };
    checked(record,&metadata)?;
    if metadata.revision==tx.governance_limits.revision&&metadata!=*tx.governance_limits {
        return Err(Fault::Store("governance metadata conflicts with same selected revision".into()))
    }
    Ok(metadata)
}
pub(super) fn validate_retained_configuration(tx:&StoreContext<'_>,value:&ConfigurationVersion)->Result<()> {
    let id:String=tx.query_row("SELECT CAST(substr(CAST(operation_id AS BLOB),1,?2) AS TEXT) FROM governance_configurations WHERE revision=?1",
        params![value.revision,sql_bound(tx.governance_limits.transport.identity_bytes.saturating_add(1))?],|r|r.get(0))?;
    let record=read_decision(tx,&DecisionRef{operation_id:id,point:DecisionPoint::RuleAdministration})?;
    if !matches!(&record.detail,DecisionDetail::RuleAdministration(RuleAdministrationDecision::RegisteredConfiguration{revision}) if revision==&value.revision)
        ||record.actual_outcome!=ActualDecisionOutcome::Applied {
        return Err(Fault::Store("governance configuration lacks exact successful registration audit".into()))
    }
    let metadata=decision_metadata(tx,&record)?;
    validate_configuration(&StoreContext::new(tx,&metadata),value)
}
