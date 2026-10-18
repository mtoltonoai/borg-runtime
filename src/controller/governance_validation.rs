//! Read-only retained governance validation before startup writes. Replays only
//! the closed audit data from original receipts; never invokes a rule or effect.
use super::{database::{decode, integer, Fault, Result, StoreContext}, governance, ControllerConfig};
use crate::governance::*;
use crate::protocol::{Operation, Outcome};
use rusqlite::{params, OptionalExtension};
use std::collections::{BTreeMap, BTreeSet};

fn corruption(message:&str)->Fault { Fault::Store(message.into()) }
fn scope_key(scope:&ControlScope)->String {match scope {ControlScope::AllTasks=>"all".into(),ControlScope::Task{task_id}=>format!("task:{task_id}")}}

pub(super) fn validate_stored(tx:&StoreContext<'_>,cfg:&ControllerConfig)->Result<()> {
    let any:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM governance_rules) OR EXISTS(SELECT 1 FROM governance_configurations) OR EXISTS(SELECT 1 FROM governance_state) OR EXISTS(SELECT 1 FROM governance_holds) OR EXISTS(SELECT 1 FROM receipts WHERE governance IS NOT NULL)",[],|r|r.get(0))?;
    let Some(bounds)=tx.governance_limits.governance.as_ref() else {
        return if any {Err(corruption("retained governance requires its configured metadata"))} else {Ok(())}
    };
    governance::validate_metadata_inventory(tx)?;
    let id_cap=tx.governance_limits.transport.identity_bytes.saturating_add(1);
    let mut query=tx.prepare("SELECT CAST(substr(CAST(id AS BLOB),1,?1) AS TEXT),CAST(substr(CAST(revision AS BLOB),1,?1) AS TEXT) FROM governance_rules ORDER BY id,revision LIMIT ?2")?;
    let rules=query.query_map(params![super::database::sql_bound(id_cap)?,super::database::sql_bound(bounds.rule_versions.saturating_add(1))?],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?.collect::<std::result::Result<Vec<_>,_>>()?;
    if rules.len()>bounds.rule_versions{return Err(corruption("governance rule inventory exceeds its bound"))}
    let actual_rules:BTreeSet<_>=rules.iter().cloned().collect();
    for (id,revision) in rules {
        let reference=RuleRef{id,revision};
        if governance::bootstrap_rule(&reference).is_some(){return Err(corruption("implicit builtin rule has an unexpected stored replacement"))}
        governance::read_rule(tx,&reference)?;
    }
    let mut query=tx.prepare("SELECT CAST(substr(CAST(revision AS BLOB),1,?1) AS TEXT),CAST(substr(CAST(qualification AS BLOB),1,?1) AS TEXT) FROM governance_configurations ORDER BY revision LIMIT ?2")?;
    let configurations=query.query_map(params![super::database::sql_bound(id_cap)?,super::database::sql_bound(bounds.configuration_versions.saturating_add(1))?],|r|Ok((r.get::<_,String>(0)?,r.get::<_,Option<String>>(1)?)))?.collect::<std::result::Result<Vec<_>,_>>()?;
    if configurations.len()>bounds.configuration_versions{return Err(corruption("governance configuration inventory exceeds its bound"))}
    for (revision,qualification) in &configurations {
        if revision==&governance::bootstrap(tx).revision{return Err(corruption("implicit bootstrap has an unexpected stored replacement"))}
        let configuration=governance::read_configuration(tx,revision)?;
        governance::validate_retained_configuration(tx,&configuration)?;
        if qualification.is_some() {governance::require_qualified(tx,revision)?;}
    }
    let mut active=GovernanceState{selected_revision:governance::bootstrap(tx).revision,temporary_override:None};
    let mut wrote_state=false;
    let mut holds=BTreeMap::<String,EmergencyHold>::new();
    let mut qualified=BTreeMap::<String,String>::new();
    let mut registered_rules=BTreeSet::<(String,String)>::new();
    let mut registered_configurations=BTreeSet::<String>::new();
    let maximum=cfg.max_receipts.checked_add(cfg.max_cleanup_receipts).ok_or_else(||corruption("governance receipt inventory overflow"))?;
    let mut query=tx.prepare("SELECT CAST(substr(CAST(id AS BLOB),1,?1) AS TEXT),CAST(substr(CAST(governance AS BLOB),1,?2) AS TEXT),controller_revision FROM receipts WHERE governance IS NOT NULL ORDER BY controller_revision LIMIT ?3")?;
    let mut rows=query.query(params![super::database::sql_bound(id_cap)?,super::database::sql_bound(bounds.record_bytes.saturating_add(1))?,super::database::sql_bound(maximum.saturating_add(1))?])?;
    let mut count=0usize;let mut previous=0u64;
    while let Some(row)=rows.next()? {
        count+=1;if count>maximum{return Err(corruption("governance receipt inventory exceeds enclosing bound"))}
        let id:String=row.get(0)?;let raw:String=row.get(1)?;let revision:i64=row.get(2)?;
        if raw.len()>bounds.record_bytes{return Err(corruption("governance receipt audit exceeds bound"))}
        let records:Vec<DecisionRecord>=decode(&raw)?;
        if records.is_empty()||records.len()>5{return Err(corruption("governance receipt has an invalid closed decision set"))}
        let mut points=BTreeSet::new();
        for record in &records {
            // Until an actual promotion writes a selection, the implicit
            // bootstrap follows the metadata recorded by this original act.
            // Once selected, later metadata reloads cannot rewrite that state.
            if !wrote_state {active.selected_revision=record.configuration.global_revision.clone();}
            if record.id.operation_id!=id||integer(record.controller_revision)?!=revision||record.controller_revision<=previous||!points.insert(record.id.point) {
                return Err(corruption("governance audit row identity/order differs"))
            }
            if governance::read_decision(tx,&record.id)?!=*record{return Err(corruption("governance audit differs from original receipt"))}
            if matches!(record.detail,DecisionDetail::CharterConfiguration(_)) && record.configuration.governance_revision!=active.selected_revision {
                return Err(corruption("charter decision differs from its original active governance selection"));
            }
            if matches!(record.detail,DecisionDetail::Completion(_)|DecisionDetail::DefinitionLimits(_)) {
                let expected_mode=if !wrote_state {DecisionMode::Observation}else{DecisionMode::Enforcing};
                if record.configuration.governance_revision!=active.selected_revision||record.mode!=expected_mode {
                    return Err(corruption("ordinary decision differs from its original active governance selection"))
                }
            }
        }
        previous=u64::try_from(revision).map_err(|_|corruption("negative governance revision"))?;
        let original=governance::original_receipt(tx,&id)?.ok_or_else(||corruption("governance receipt vanished"))?;
        // Mandatory replacement refusals retain evaluation evidence but none of
        // the rolled-back rule/configuration/control effects.
        if !matches!(original.outcome,Outcome::Ok{..}) {continue}
        for record in &records {
            match &record.detail {
                DecisionDetail::RuleAdministration(RuleAdministrationDecision::RegisteredRule{rule})=>{
                    if !registered_rules.insert((rule.id.clone(),rule.revision.clone())) {return Err(corruption("rule version registered more than once"))}
                }
                DecisionDetail::RuleAdministration(RuleAdministrationDecision::RegisteredConfiguration{revision})=>{
                    let configuration=governance::read_configuration(tx,revision)?;
                    for rule in [&configuration.completion,&configuration.definition_limits] {
                        if governance::bootstrap_rule(rule).is_none()&&!registered_rules.contains(&(rule.id.clone(),rule.revision.clone())) {
                            return Err(corruption("configuration precedes original rule registration"))
                        }
                    }
                    if !registered_configurations.insert(revision.clone()){return Err(corruption("configuration registered more than once"))}
                }
                DecisionDetail::RuleAdministration(RuleAdministrationDecision::Promoted{prior_revision,selected_revision,evidence})=>{
                    governance::validate_promotion_evidence(tx,&governance::read_configuration(tx,selected_revision)?,evidence,record.controller_revision)?;
                    if !registered_configurations.contains(selected_revision){return Err(corruption("promotion precedes its exact immutable registration"))}
                    if &active.selected_revision!=prior_revision{return Err(corruption("promotion prior selection differs from original chronology"))}
                    if let Some(override_value)=&active.temporary_override {
                        if !records.iter().any(|row|matches!(&row.detail,DecisionDetail::EmergencyControl(ControlDecision::OverrideSuperseded{original_operation_id,selected_revision:replacement}) if original_operation_id==&override_value.operation_id&&replacement==selected_revision)) {
                            return Err(corruption("promotion omitted the exact superseded override audit"))
                        }
                    } else if records.iter().any(|row|matches!(row.detail,DecisionDetail::EmergencyControl(ControlDecision::OverrideSuperseded{..}))) {
                        return Err(corruption("promotion supersedes an absent override"))
                    }
                    qualified.entry(selected_revision.clone()).or_insert(id.clone());
                    active=GovernanceState{selected_revision:selected_revision.clone(),temporary_override:None};wrote_state=true;
                }
                DecisionDetail::EmergencyControl(ControlDecision::Paused{scope})=>{
                    let Outcome::Ok{data:crate::protocol::Data::Governance(value)}=&original.outcome else{return Err(corruption("pause lacks original typed outcome"))};
                    let GovernanceResult::Hold(hold)=value.as_ref() else{return Err(corruption("pause lacks original hold"))};
                    if holds.insert(scope_key(scope),hold.clone()).is_some(){return Err(corruption("pause replaced an unreleased original hold"))}
                    if holds.len()>bounds.scan_rows{return Err(corruption("emergency holds exceed bounded inventory"))}
                }
                DecisionDetail::EmergencyControl(ControlDecision::HoldReleased{scope})=>{
                    let Operation::MutateGovernance{mutation:GovernanceMutation::ReleaseHold{expected_operation_id,..},..}=&original.request else{return Err(corruption("release lacks original request"))};
                    if holds.remove(&scope_key(scope)).is_none_or(|hold|hold.operation_id!=*expected_operation_id){return Err(corruption("hold release differs from original hold identity"))}
                }
                DecisionDetail::EmergencyControl(ControlDecision::OverrideSelected{prior_revision,replacement_revision,expires_at_ms,reason,..})=>{
                    if active.temporary_override.is_some()||&active.selected_revision!=prior_revision||!qualified.contains_key(replacement_revision) {
                        return Err(corruption("override lacks exact prior selection or earlier qualification"))
                    }
                    active=GovernanceState{selected_revision:replacement_revision.clone(),temporary_override:Some(TemporaryOverride{operation_id:id.clone(),prior_revision:prior_revision.clone(),replacement_revision:replacement_revision.clone(),expires_at_ms:*expires_at_ms,actor:record.actor.clone(),reason:reason.clone()})};wrote_state=true;
                }
                DecisionDetail::EmergencyControl(ControlDecision::OverrideExpired{original_operation_id,restored_revision})=>{
                    let prior=active.temporary_override.as_ref().ok_or_else(||corruption("expiry has no retained original override"))?;
                    if &prior.operation_id!=original_operation_id||&prior.prior_revision!=restored_revision{return Err(corruption("expiry differs from exact original override"))}
                    active=GovernanceState{selected_revision:restored_revision.clone(),temporary_override:None};wrote_state=true;
                }
                _=>(),
            }
        }
    }
    if actual_rules!=registered_rules||configurations.iter().map(|(revision,_)|revision.clone()).collect::<BTreeSet<_>>()!=registered_configurations {
        return Err(corruption("retained governance version membership differs from original registrations"))
    }
    if wrote_state {governance::read_configuration(tx,&active.selected_revision)?;}
    if let Some(value)=&active.temporary_override {
        governance::read_configuration(tx,&value.prior_revision)?;
        governance::read_configuration(tx,&value.replacement_revision)?;
    }
    for (revision,qualification) in configurations {
        if qualification!=qualified.get(&revision).cloned(){return Err(corruption("configuration qualification differs from first actual promotion"))}
    }
    let raw:Option<String>=tx.query_row("SELECT CAST(substr(CAST(body AS BLOB),1,?1) AS TEXT) FROM governance_state WHERE singleton=1",[super::database::sql_bound(bounds.record_bytes.saturating_add(1))?],|r|r.get(0)).optional()?;
    match raw {
        Some(raw) if wrote_state&&raw.len()<=bounds.record_bytes&&decode::<GovernanceState>(&raw)?==active=>(),
        None if !wrote_state=>(),
        _=>return Err(corruption("active governance state differs from original committed chronology")),
    }
    let mut query=tx.prepare("SELECT CAST(substr(CAST(scope AS BLOB),1,?1) AS TEXT),CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM governance_holds ORDER BY scope LIMIT ?3")?;
    let actual=query.query_map(params![super::database::sql_bound(id_cap.saturating_add(5))?,super::database::sql_bound(bounds.record_bytes.saturating_add(1))?,super::database::sql_bound(bounds.scan_rows.saturating_add(1))?],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?.collect::<std::result::Result<Vec<_>,_>>()?;
    if actual.len()!=holds.len(){return Err(corruption("retained emergency hold membership differs"))}
    for (key,raw) in actual {
        if raw.len()>bounds.record_bytes||holds.get(&key)!=Some(&decode::<EmergencyHold>(&raw)?){return Err(corruption("retained emergency hold differs from original pause/release chronology"))}
    }
    Ok(())
}
