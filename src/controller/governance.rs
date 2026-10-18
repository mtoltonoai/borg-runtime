//! Closed governance decisions inside the existing owner transaction/receipt.
//! No provider, executor, model budget, background work or caller-supplied actor.
use super::database::{decode, encode, integer, refuse, valid_id, Fault, Result, StoreContext};
use crate::collaboration::workflow::{ActorProvenance, PrincipalRef, RecordedActor, TrustedHostPath};
use crate::governance::*;
use crate::limits::{ExecutionLimits, GovernanceLimits};
use crate::protocol::{Operation, Outcome};
use rusqlite::{params, OptionalExtension};
use std::{cell::RefCell, collections::{BTreeMap, BTreeSet}, rc::Rc};

#[derive(Clone, Default)]
pub(super) struct DecisionBuffer(Rc<RefCell<BTreeMap<DecisionPoint, Draft>>>);
#[derive(Clone)]
struct Draft {
    mode: DecisionMode,
    operation_id: String, rule: RuleRef, configuration: ConfigurationBinding,
    actor: RecordedActor, detail: DecisionDetail, observed_product_revision: Option<u64>,
}
impl DecisionBuffer { pub(super) fn new()->Self { Self::default() } }

pub(super) fn limits<'a>(tx:&'a StoreContext<'_>)->Result<&'a GovernanceLimits> {
    tx.governance_limits.governance.as_ref().ok_or_else(||Fault::Refused("governance_disabled","governance metadata is absent".into()))
}
pub(super) fn bounded<T:serde::Serialize>(tx:&StoreContext<'_>,value:&T)->Result<String> {
    let raw=encode(value)?;
    if raw.len()>limits(tx)?.record_bytes{return refuse("capacity","Configured limit exceeded: governance.record_bytes")}
    Ok(raw)
}
fn parse<T:serde::de::DeserializeOwned+serde::Serialize>(tx:&StoreContext<'_>,raw:&str)->Result<T> {
    if raw.len()>limits(tx)?.record_bytes{return Err(Fault::Store("retained governance record exceeds governance.record_bytes".into()))}
    decode(raw)
}
fn identity(tx:&StoreContext<'_>,id:&str)->Result<()> {
    if !valid_id(tx.governance_limits,id){return refuse("invalid_input","invalid governance identity: transport.identity_bytes")}
    Ok(())
}
//= spec/impl/v2/implementation.md#configuration-changes-preserve-governance
//# A governance-rule or change-policy revision MUST be applied through an operator-controlled path outside agent-writable work.
//= spec/impl/v2/implementation.md#emergency-control-is-independent-of-ordinary-policy
//# An emergency-control record MUST identify the actual authenticated invocation boundary rather than a caller-supplied identity.
pub(super) fn operator(tx:&StoreContext<'_>)->Result<RecordedActor> {
    // This in-process path is established by the transport dispatcher. It is
    // never parsed from a mutation, model argument, human name or product vote.
    if tx.bound_actor.is_some() || tx.invocation_path!=TrustedHostPath::OperatorApi {
        return refuse("governance_operator_required","governance control requires the configured operator API path")
    }
    Ok(RecordedActor{principal:PrincipalRef::TrustedHostOperator,
        provenance:ActorProvenance::TrustedHost{path:tx.invocation_path}})
}
fn rule_ref(id:&str)->RuleRef {RuleRef{id:id.into(),revision:"1".into()}}
pub(super) fn bootstrap_rule(reference:&RuleRef)->Option<RuleVersion> {
    let body=if *reference==rule_ref("completion-required-work") {RuleBody::Completion{allow_continuation:true}}
        else if *reference==rule_ref("definition-resource-allowance") {RuleBody::DefinitionLimits{allow_configured_widening:true}}
        else {return None};
    Some(RuleVersion{reference:reference.clone(),body})
}
pub(super) fn bootstrap(tx:&StoreContext<'_>)->ConfigurationVersion {
    let root=tx.governance_limits;
    ConfigurationVersion {revision:root.revision.clone(),global_revision:root.revision.clone(),
        definition_update_policy_revision:root.definition_updates.as_ref().map(|p|p.revision.clone()),
        completion:rule_ref("completion-required-work"),definition_limits:rule_ref("definition-resource-allowance"),
        widening_fields:root.definition_updates.as_ref().map(|p|p.allowed_fields.clone()).unwrap_or_default()}
}
/// Refuse oversized retained bytes before the existing receipt decoder reads
/// them; this does not replace that decoder's identity/outcome/commit guards.
pub(super) fn original_receipt(tx:&StoreContext<'_>,id:&str)->Result<Option<super::metadata::Recorded>> {
    identity(tx,id)?;
    let sizes:Option<(String,i64,String,i64)>=tx.query_row(
        "SELECT typeof(request),length(CAST(request AS BLOB)),typeof(outcome),length(CAST(outcome AS BLOB)) FROM receipts WHERE id=?1",
        [id],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).optional()?;
    let Some((request_type,request_bytes,outcome_type,outcome_bytes))=sizes else{return Ok(None)};
    let cap=tx.limits.transport.body_bytes;
    if request_type!="text"||outcome_type!="text"||usize::try_from(request_bytes).ok().is_none_or(|n|n>cap)
        ||usize::try_from(outcome_bytes).ok().is_none_or(|n|n>cap) {
        return Err(Fault::Store("governance original receipt exceeds its closed representation".into()))
    }
    super::metadata::read(tx,id)
}
//= spec/impl/v2/implementation.md#decision-rules-have-versions-and-evidence
//# Every enforced governance rule MUST have an immutable version identity.
pub(super) fn read_rule(tx:&StoreContext<'_>,reference:&RuleRef)->Result<RuleVersion> {
    identity(tx,&reference.id)?;identity(tx,&reference.revision)?;
    if let Some(rule)=bootstrap_rule(reference){return Ok(rule)}
    let row:Option<(String,String)>=tx.query_row("SELECT CAST(substr(CAST(body AS BLOB),1,?3) AS TEXT),CAST(substr(CAST(operation_id AS BLOB),1,?4) AS TEXT) FROM governance_rules WHERE id=?1 AND revision=?2",
        params![reference.id,reference.revision,super::database::sql_bound(limits(tx)?.record_bytes.saturating_add(1))?,super::database::sql_bound(tx.governance_limits.transport.identity_bytes.saturating_add(1))?],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let (raw,original)=row.ok_or_else(||Fault::Refused("not_found","governance rule absent".into()))?;
    identity(tx,&original)?;let rule:RuleVersion=parse(tx,&raw)?;
    if rule.reference!=*reference{return Err(Fault::Store("governance rule index mismatch".into()))}
    let receipt=original_receipt(tx,&original)?.ok_or_else(||Fault::Store("governance rule lacks original registration".into()))?;
    if !matches!(receipt.request,Operation::MutateGovernance{mutation:GovernanceMutation::RegisterRule{rule:ref saved},..} if saved==&rule)
        ||!matches!(receipt.outcome,Outcome::Ok{data:crate::protocol::Data::Governance(ref value)} if **value==GovernanceResult::Rule(rule.clone())) {
        return Err(Fault::Store("governance rule differs from original immutable registration".into()))
    } Ok(rule)
}
pub(super) fn read_configuration(tx:&StoreContext<'_>,revision:&str)->Result<ConfigurationVersion> {
    identity(tx,revision)?;
    let initial=bootstrap(tx);if revision==initial.revision{return Ok(initial)}
    let row:Option<(String,String)>=tx.query_row("SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT),CAST(substr(CAST(operation_id AS BLOB),1,?3) AS TEXT) FROM governance_configurations WHERE revision=?1",
        params![revision,super::database::sql_bound(limits(tx)?.record_bytes.saturating_add(1))?,super::database::sql_bound(tx.governance_limits.transport.identity_bytes.saturating_add(1))?],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let Some((raw,original))=row else {
        // A historical implicit bootstrap is derived only from exact retained
        // metadata evidence; a revision string alone never invents defaults.
        let metadata=history::metadata_at_revision(tx,revision)?
            .ok_or_else(||Fault::Refused("not_found","governance configuration absent".into()))?;
        return Ok(bootstrap(&StoreContext::new(tx,&metadata)))
    };
    identity(tx,&original)?;let value:ConfigurationVersion=parse(tx,&raw)?;
    if value.revision!=revision{return Err(Fault::Store("governance configuration index mismatch".into()))}
    let receipt=original_receipt(tx,&original)?.ok_or_else(||Fault::Store("governance configuration lacks original registration".into()))?;
    if !matches!(receipt.request,Operation::MutateGovernance{mutation:GovernanceMutation::RegisterConfiguration{configuration:ref saved},..} if saved==&value)
        ||!matches!(receipt.outcome,Outcome::Ok{data:crate::protocol::Data::Governance(ref saved)} if **saved==GovernanceResult::Configuration(value.clone())) {
        return Err(Fault::Store("governance configuration differs from original immutable registration".into()))
    } Ok(value)
}
pub(super) fn state(tx:&StoreContext<'_>)->Result<GovernanceState> {
    let raw:Option<String>=tx.query_row("SELECT CAST(substr(CAST(body AS BLOB),1,?1) AS TEXT) FROM governance_state WHERE singleton=1",
        [super::database::sql_bound(limits(tx)?.record_bytes.saturating_add(1))?],|r|r.get(0)).optional()?;
    raw.map(|v|parse(tx,&v)).transpose().map(|v|v.unwrap_or(GovernanceState{selected_revision:bootstrap(tx).revision,temporary_override:None}))
}
pub(super) fn save_state(tx:&StoreContext<'_>,value:&GovernanceState)->Result<()> {
    tx.execute("INSERT INTO governance_state(singleton,body) VALUES(1,?1) ON CONFLICT(singleton) DO UPDATE SET body=excluded.body",[bounded(tx,value)?])?;Ok(())
}
//= spec/impl/v2/implementation.md#configuration-changes-preserve-governance
//# A widening configuration change MUST be refused unless the authenticated invocation is trusted for that configured class of change.
pub(super) fn validate_configuration(tx:&StoreContext<'_>,value:&ConfigurationVersion)->Result<()> {
    identity(tx,&value.revision)?;
    let current=bootstrap(tx);
    if value.global_revision!=current.global_revision||value.definition_update_policy_revision!=current.definition_update_policy_revision {
        return refuse("configuration_conflict","governance selection requires exact current global/change-policy revisions")
    }
    if !matches!(read_rule(tx,&value.completion)?.body,RuleBody::Completion{..})
        ||!matches!(read_rule(tx,&value.definition_limits)?.body,RuleBody::DefinitionLimits{..}) {
        return refuse("invalid_input","governance rule is attached to the wrong closed decision point")
    }
    let fields:BTreeSet<_>=value.widening_fields.iter().copied().collect();
    if fields.len()!=value.widening_fields.len()||fields.iter().any(|f|!current.widening_fields.contains(f)) {
        return refuse("configuration_policy","governance cannot create an unconfigured widening class")
    } Ok(())
}
/// Initial metadata selects bounded audit capability, not new ordinary
/// enforcement. Only a recorded exact promotion can enable that configuration.
pub(super) fn completion_enforced(tx:&StoreContext<'_>)->Result<bool> {
    if tx.governance_limits.governance.is_none(){return Ok(false)}
    let selected=state(tx)?;
    if selected.selected_revision==bootstrap(tx).revision{return Ok(false)}
    require_qualified(tx,&selected.selected_revision)?;
    Ok(true)
}
pub(super) fn require_qualified(tx:&StoreContext<'_>,revision:&str)->Result<()> {
    let original:Option<Option<String>>=tx.query_row(
        "SELECT CAST(substr(CAST(qualification AS BLOB),1,?2) AS TEXT) FROM governance_configurations WHERE revision=?1",
        params![revision,super::database::sql_bound(tx.governance_limits.transport.identity_bytes.saturating_add(1))?],|row|row.get(0)).optional()?;
    let id=original.flatten().ok_or_else(||Fault::Refused("governance_evidence","configuration has no original passing promotion".into()))?;
    identity(tx,&id)?;
    let record=read_decision(tx,&DecisionRef{operation_id:id,point:DecisionPoint::RuleAdministration})?;
    if !matches!(record.actual_outcome,ActualDecisionOutcome::Applied)
        ||!matches!(record.detail,DecisionDetail::RuleAdministration(RuleAdministrationDecision::Promoted{selected_revision,..}) if selected_revision==revision) {
        return refuse("governance_evidence","configuration qualification differs from original promotion")
    }
    Ok(())
}
pub(super) fn selection(tx:&StoreContext<'_>)->Result<ConfigurationVersion> {
    limits(tx)?;let active=state(tx)?;let selected=read_configuration(tx,&active.selected_revision)?;
    validate_configuration(tx,&selected)?;Ok(selected)
}
fn binding(tx:&StoreContext<'_>,selected:&ConfigurationVersion,accepted:Option<String>)->ConfigurationBinding {
    ConfigurationBinding{global_revision:tx.governance_limits.revision.clone(),governance_revision:selected.revision.clone(),
        accepted_global_revision:accepted,definition_update_policy_revision:selected.definition_update_policy_revision.clone()}
}
fn point(detail:&DecisionDetail)->DecisionPoint {match detail {DecisionDetail::CharterConfiguration(_)=>DecisionPoint::CharterConfiguration,DecisionDetail::Completion(_)=>DecisionPoint::Completion,
    DecisionDetail::DefinitionLimits(_)=>DecisionPoint::DefinitionLimits,DecisionDetail::EmergencyControl(_)=>DecisionPoint::EmergencyControl,
    DecisionDetail::RuleAdministration(_)=>DecisionPoint::RuleAdministration}}
//= spec/impl/v2/implementation.md#decision-rules-have-versions-and-evidence
//# A governance decision record MUST identify its rule version, exact subject, caller provenance, outcome and bounded reason.
fn stage(tx:&StoreContext<'_>,draft:Draft)->Result<()> {
    identity(tx,&draft.operation_id)?;
    super::collaboration_workflow::validate_recorded_actor(tx,&draft.actor)?;
    // At most one decision per closed point in the original mutation. Exact
    // repeated staging is an authoring defect, never last-writer-wins evidence.
    bounded(tx,&DecisionRecord{mode:draft.mode,id:DecisionRef{operation_id:draft.operation_id.clone(),point:point(&draft.detail)},
        rule:draft.rule.clone(),configuration:draft.configuration.clone(),configuration_metadata:Some(Box::new(tx.governance_limits.clone())),actor:draft.actor.clone(),detail:draft.detail.clone(),
        actual_outcome:ActualDecisionOutcome::Applied,controller_revision:i64::MAX as u64,observed_product_revision:draft.observed_product_revision})?;
    let mut pending=tx.governance_decisions.0.borrow_mut();
    if pending.contains_key(&point(&draft.detail)){return Err(Fault::Store("duplicate governance decision point in original mutation".into()))}
    pending.insert(point(&draft.detail),draft);Ok(())
}
/// The evaluator owns canonical required-work/input proofs and accepted limits.
/// It may stage only after binding the existing supported completion boundary.
pub(super) fn stage_completion(tx:&StoreContext<'_>,operation_id:&str,actor:&RecordedActor,decision:CompletionDecision,observed_product_revision:u64)->Result<()> {
    let selected=selection(tx)?;
    if decision.reasons.len()>limits(tx)?.reason_items||encode(&decision.reasons)?.len()>limits(tx)?.reason_bytes {
        return refuse("capacity","Configured limit exceeded: governance.reason_items/reason_bytes")
    }
    if observed_product_revision>super::collaboration_workflow::product_revision(tx)? {
        return Err(Fault::Store("completion watermark is from a future canonical view".into()))
    }
    stage(tx,Draft{mode:if completion_enforced(tx)? {DecisionMode::Enforcing}else{DecisionMode::Observation},operation_id:operation_id.into(),rule:selected.completion.clone(),configuration:binding(tx,&selected,Some(decision.subject.accepted_global_revision.clone())),
        actor:actor.clone(),detail:DecisionDetail::Completion(decision),observed_product_revision:Some(observed_product_revision)})
}
/// Zero is a refusal of additional continuation, never permission to complete.
pub(super) fn continuation_limit(tx:&StoreContext<'_>,accepted:&crate::limits::RuntimeLimits)->Result<usize> {
    let selected=selection(tx)?;
    let RuleBody::Completion{allow_continuation}=read_rule(tx,&selected.completion)?.body else {return Err(Fault::Store("wrong completion rule".into()))};
    let historical=accepted.governance.as_ref().ok_or_else(||Fault::Refused("governance_unavailable","accepted completion bounds absent".into()))?;
    Ok(if allow_continuation {limits(tx)?.completion_continuations.min(historical.completion_continuations)} else {0})
}
//= spec/impl/v2/implementation.md#configuration-changes-preserve-governance
//# A governed configuration change MUST identify the immutable classification-rule revision used to evaluate it.
pub(super) fn compare_definition(tx:&StoreContext<'_>,operation_id:&str,actor:&RecordedActor,agent_id:&str,previous_revision:&str,
    selected_revision:Option<String>,before:&ExecutionLimits,after:&ExecutionLimits,changes:&BTreeMap<crate::limits::DefinitionLimitField,usize>)->Result<DefinitionLimitDecision> {
    let selected=selection(tx)?;
    let RuleBody::DefinitionLimits{allow_configured_widening}=read_rule(tx,&selected.definition_limits)?.body else{return Err(Fault::Store("wrong definition-limit rule".into()))};
    let (comparisons,allowed,reason)=crate::governance_comparison::compare(before,after,changes,&selected.widening_fields,allow_configured_widening)
        .map_err(|message|Fault::Refused("configuration_policy",message))?;
    let decision=DefinitionLimitDecision{agent_id:agent_id.into(),previous_revision:previous_revision.into(),selected_revision,comparisons,allowed,reason};
    stage(tx,Draft{mode:if completion_enforced(tx)? {DecisionMode::Enforcing}else{DecisionMode::Observation},operation_id:operation_id.into(),rule:selected.definition_limits.clone(),configuration:binding(tx,&selected,None),actor:actor.clone(),
        detail:DecisionDetail::DefinitionLimits(decision.clone()),observed_product_revision:None})?;
    Ok(decision)
}
fn assert_original_actor(tx:&StoreContext<'_>,op:&Operation,actor:&RecordedActor)->Result<()> {
    match (op,&actor.provenance) {
        (Operation::CallCollaboration{call:crate::mcp::Invocation{caller,..}}|Operation::EvaluateCompletion(crate::governance_completion::EvaluateCompletion{caller,..}),ActorProvenance::BoundAgentSession{task_id,session_id,attempt_id,generation,worker_id,incarnation,agent_id})=>{
            let task=super::sessions::historical_task(tx,task_id,session_id)?;
            if caller.task_id!=*task_id||caller.attempt_id!=*attempt_id||caller.generation!=*generation||caller.worker_id!=*worker_id||caller.incarnation!=*incarnation
                ||task.session_id!=*session_id||task.assigned_agent_id!=*agent_id||actor.principal!=(PrincipalRef::Agent{agent_id:agent_id.clone()}) {
                return Err(Fault::Store("governance actor differs from original bound caller".into()))
            }
        }
        (Operation::AwaitCommands{..},ActorProvenance::TrustedHost{path:TrustedHostPath::ControllerMaintenance})
            if actor.principal==PrincipalRef::TrustedHostOperator
                &&tx.governance_decisions.0.borrow().get(&DecisionPoint::EmergencyControl)
                    .is_some_and(|draft|matches!(draft.detail,DecisionDetail::EmergencyControl(ControlDecision::OverrideExpired{..})))=>(),
        (_,ActorProvenance::TrustedHost{path}) if !matches!(op,Operation::CallCollaboration{..}|Operation::EvaluateCompletion(_))&&*path==tx.invocation_path&&actor.principal==PrincipalRef::TrustedHostOperator=>(),
        (Operation::ObserveWorker{report},ActorProvenance::BoundAgentSession{task_id,attempt_id,generation,worker_id,incarnation,..})
            if matches!(report.observation,crate::protocol::Observation::ResultCandidate{..})
                &&report.task_id==*task_id&&report.attempt_id==*attempt_id&&report.generation==*generation
                &&report.worker_id==*worker_id&&report.incarnation==*incarnation
                &&tx.governance_decisions.0.borrow().values().all(|d|matches!(d.detail,DecisionDetail::Completion(_)))=>{
            super::collaboration_workflow::validate_recorded_actor(tx,actor)?;
        }
        _=>return Err(Fault::Store("governance actor is not the original invocation".into())),
    }Ok(())
}
/// After complete-call/outcome/receipt bounds, before original receipt INSERT.
/// Returning bytes lets the owner attach audit to that same receipt row.
//= spec/impl/v2/implementation.md#configuration-changes-preserve-governance
//# A governed configuration change MUST commit its comparison evidence with its immutable revision and original operation outcome.
//= spec/impl/v2/implementation.md#decision-rules-have-versions-and-evidence
//# An enforcing governance decision MUST commit its audit event atomically with the transition it gates.
pub(super) fn commit_decisions(tx:&StoreContext<'_>,op:&Operation,outcome:&Outcome,revision:u64,now:u64)->Result<Option<String>> {
    let pending=tx.governance_decisions.0.borrow().clone();if pending.is_empty(){return Ok(None)}
    let id=op.mutation_id().ok_or_else(||Fault::Store("governance draft lacks original mutation".into()))?;
    let mut records=Vec::new();
    for (decision_point,draft) in pending {
        if draft.operation_id!=id{return Err(Fault::Store("governance draft changed original operation identity".into()))}
        assert_original_actor(tx,op,&draft.actor)?;
        let expected_refusal=match &draft.detail {DecisionDetail::Completion(_)=>"governance_completion_blocked",DecisionDetail::DefinitionLimits(_)=>"governance_configuration_blocked",_=>""};
        let actual_outcome=match outcome {Outcome::Ok{..}=>ActualDecisionOutcome::Applied,
            Outcome::Refused{code,..} if code==expected_refusal=>ActualDecisionOutcome::GovernanceBlocked,
            Outcome::Refused{code,..}=>ActualDecisionOutcome::MandatoryRefusal{code:code.clone()},
            Outcome::Pending{..}|Outcome::Uncertain{..}=>return Err(Fault::Store("governance transition cannot commit a nonterminal original outcome".into()))};
        let mut record=DecisionRecord{mode:draft.mode,id:DecisionRef{operation_id:id.into(),point:decision_point},rule:draft.rule,configuration:draft.configuration,configuration_metadata:Some(Box::new(tx.governance_limits.clone())),actor:draft.actor,
            detail:draft.detail,actual_outcome,controller_revision:revision,observed_product_revision:draft.observed_product_revision};
        if !matches!(record.actual_outcome,ActualDecisionOutcome::Applied) {
            if let DecisionDetail::DefinitionLimits(decision)=&mut record.detail {decision.selected_revision=None;}
        }
        if let DecisionDetail::Completion(decision)=&record.detail {
            let apply=matches!((&decision.disposition,&record.actual_outcome),
                (CompletionDisposition::Admit,ActualDecisionOutcome::Applied)|(CompletionDisposition::Blocked{..},ActualDecisionOutcome::GovernanceBlocked));
            if apply&&record.mode==DecisionMode::Enforcing {super::governance_completion::persist_hold(tx,&record.id,decision,record.observed_product_revision.ok_or_else(||Fault::Store("completion draft lacks evaluated watermark".into()))?,now)?;}
        }
        records.push(record);
    }
    Ok(Some(bounded(tx,&records)?))
}
//= spec/impl/v2/implementation.md#decision-rules-have-versions-and-evidence
//# Replay of a recorded governance decision MUST retain its original decision identity and outcome.
pub(super) fn read_decision(tx:&StoreContext<'_>,id:&DecisionRef)->Result<DecisionRecord> {
    identity(tx,&id.operation_id)?;
    let row:Option<(Option<String>,i64)>=tx.query_row("SELECT CAST(substr(CAST(governance AS BLOB),1,?2) AS TEXT),controller_revision FROM receipts WHERE id=?1",
        params![id.operation_id,super::database::sql_bound(limits(tx)?.record_bytes.saturating_add(1))?],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let (raw,revision)=row.ok_or_else(||Fault::Refused("not_found","original governance receipt absent".into()))?;
    let records:Vec<DecisionRecord>=parse(tx,&raw.ok_or_else(||Fault::Refused("not_found","receipt contains no governance decision".into()))?)?;
    let mut matches=records.into_iter().filter(|record|record.id==*id);
    let record=matches.next().ok_or_else(||Fault::Refused("not_found","decision point absent".into()))?;
    if matches.next().is_some()||integer(record.controller_revision)?!=revision||record.id.point!=point(&record.detail) {
        return Err(Fault::Store("governance receipt identity mismatch".into()))
    }
    let original=original_receipt(tx,&id.operation_id)?.ok_or_else(||Fault::Store("governance original receipt missing".into()))?;
    let agrees=match (&record.actual_outcome,&original.outcome) {
        (ActualDecisionOutcome::Applied,Outcome::Ok{..})=>true,
        (ActualDecisionOutcome::GovernanceBlocked,Outcome::Refused{code,..})=>match record.id.point {
            DecisionPoint::Completion=>code=="governance_completion_blocked",
            DecisionPoint::DefinitionLimits=>code=="governance_configuration_blocked",
            _=>false,
        },
        (ActualDecisionOutcome::MandatoryRefusal{code},Outcome::Refused{code:actual,..})=>code==actual,
        _=>false,
    };
    if !agrees||original.commit.controller_revision!=record.controller_revision {
        return Err(Fault::Store("governance audit differs from final original outcome".into()))
    }
    super::collaboration_workflow::validate_recorded_actor(tx,&record.actor)?;
    // Historical audit is checked under its original immutable metadata, never
    // against the newly selected defaults used to admit a fresh mutation.
    let metadata=history::decision_metadata(tx,&record)?;
    let historical=StoreContext::new(tx,&metadata);
    validate_original_detail(&historical,&record,&original.request,&original.outcome)?;
    Ok(record)
}
/// The audit column is not independent authority. Its exact subject must be the
/// immutable original request, and successful administration/control records
/// must equal the original typed response before they can qualify another act.
fn validate_original_detail(tx:&StoreContext<'_>,record:&DecisionRecord,request:&Operation,outcome:&Outcome)->Result<()> {
    let applied=matches!(record.actual_outcome,ActualDecisionOutcome::Applied);
    let result=match outcome {Outcome::Ok{data:crate::protocol::Data::Governance(value)}=>Some(value.as_ref()),_=>None};
    let admin_actor=RecordedActor{principal:PrincipalRef::TrustedHostOperator,
        provenance:ActorProvenance::TrustedHost{path:TrustedHostPath::OperatorApi}};
    let agrees=match &record.detail {
        DecisionDetail::CharterConfiguration(detail)=>{
            (record.actor==admin_actor || matches!(&record.actor.provenance,ActorProvenance::BoundAgentSession{..})
                && super::identity_administration::request(request)?.is_some()) && record.mode==DecisionMode::Enforcing
                && record.rule==rule_ref("charter-source-binding")
                && super::charter_definitions::validate_decision(tx,record,detail,request,outcome)?
        }
        DecisionDetail::RuleAdministration(detail)=>{
            if record.actor!=admin_actor||record.mode!=DecisionMode::Enforcing||record.rule!=rule_ref("governance-administration") {
                return Err(Fault::Store("governance administration audit has a different original authority".into()))
            }
            match (detail,request) {
                (RuleAdministrationDecision::RegisteredRule{rule},Operation::MutateGovernance{mutation:GovernanceMutation::RegisterRule{rule:saved},..})=>
                    rule==&saved.reference&&(!applied||result==Some(&GovernanceResult::Rule(saved.clone()))),
                (RuleAdministrationDecision::RegisteredConfiguration{revision},Operation::MutateGovernance{mutation:GovernanceMutation::RegisterConfiguration{configuration},..})=>
                    revision==&configuration.revision&&(!applied||result==Some(&GovernanceResult::Configuration(configuration.clone()))),
                (RuleAdministrationDecision::Shadow{result:shadow},Operation::MutateGovernance{mutation:GovernanceMutation::EvaluateShadow{original,candidate,configuration_revision},..})=>{
                    let mut agrees=&shadow.original==original&&&shadow.candidate==candidate&&&shadow.configuration_revision==configuration_revision
                        &&shadow.original.operation_id!=record.id.operation_id
                        &&shadow.original_controller_revision<=shadow.evaluated_at_controller_revision
                        &&shadow.evaluated_at_controller_revision<record.controller_revision
                        &&(!applied||result==Some(&GovernanceResult::Shadow(shadow.clone())));
                    if agrees {
                        let prior=original_receipt(tx,&original.operation_id)?.ok_or_else(||Fault::Store("shadow source receipt absent".into()))?;
                        agrees=prior.commit.controller_revision==shadow.original_controller_revision;
                        if let ShadowInput::CurrentNotEnforcedCompletion{assessment,..}=&shadow.input {
                            agrees&=original.point==DecisionPoint::Completion
                                &&shadow.original_controller_revision==shadow.evaluated_at_controller_revision
                                &&shadow.candidate_completion.as_ref()==Some(assessment)
                                &&matches!(&prior.request,Operation::EvaluateCompletion(value)
                                    if value.caller.task_id==assessment.subject.task_id&&value.caller.attempt_id==assessment.subject.attempt_id
                                        &&value.caller.generation==assessment.subject.generation)
                                &&matches!(prior.outcome,Outcome::Ok{data:crate::protocol::Data::Completion(crate::governance_completion::CompletionOutcome::NotEnforced)});
                        }
                    }
                    agrees
                }
                (RuleAdministrationDecision::Promoted{prior_revision,selected_revision,evidence},Operation::MutateGovernance{mutation:GovernanceMutation::Promote{expected_revision,configuration_revision,evidence:original,..},..})=>{
                    let matches=prior_revision==expected_revision&&selected_revision==configuration_revision&&evidence==original
                        &&(!applied||result==Some(&GovernanceResult::State(GovernanceState{selected_revision:selected_revision.clone(),temporary_override:None})));
                    matches
                },
                _=>false,
            }
        }
        DecisionDetail::EmergencyControl(detail)=>{
            if record.mode!=DecisionMode::Enforcing||record.rule!=rule_ref("emergency-control") {return Err(Fault::Store("emergency audit rule or mode differs".into()))}
            let actor_ok=record.actor==admin_actor;
            match (detail,request) {
                (ControlDecision::Paused{scope},Operation::MutateGovernance{mutation:GovernanceMutation::Pause{scope:original,reason},..})=>
                    actor_ok&&scope==original&&(!applied||result==Some(&GovernanceResult::Hold(EmergencyHold{scope:scope.clone(),operation_id:record.id.operation_id.clone(),actor:record.actor.clone(),reason:reason.clone()}))),
                (ControlDecision::HoldReleased{scope},Operation::MutateGovernance{mutation:GovernanceMutation::ReleaseHold{scope:original,..},..})=>
                    actor_ok&&scope==original&&(!applied||result==Some(&GovernanceResult::Released(scope.clone()))),
                (ControlDecision::StopRequested{task_id},Operation::MutateGovernance{mutation:GovernanceMutation::Stop{task_id:original,..},..})=>
                    actor_ok&&task_id==original&&(!applied||result==Some(&GovernanceResult::StopRequested{task_id:task_id.clone()})),
                (ControlDecision::OverrideSelected{scope:ControlScope::AllTasks,prior_revision,replacement_revision,expires_at_ms,reason},Operation::MutateGovernance{mutation:GovernanceMutation::Override{expected_revision,configuration_revision,expires_at_ms:expires,reason:why},..})=>
                    actor_ok&&prior_revision==expected_revision&&replacement_revision==configuration_revision&&expires_at_ms==expires&&reason==why
                        &&(!applied||result==Some(&GovernanceResult::State(GovernanceState{selected_revision:replacement_revision.clone(),temporary_override:Some(TemporaryOverride{
                            operation_id:record.id.operation_id.clone(),prior_revision:prior_revision.clone(),replacement_revision:replacement_revision.clone(),expires_at_ms:*expires_at_ms,actor:record.actor.clone(),reason:reason.clone()})}))),
                (ControlDecision::OverrideExpired{original_operation_id,restored_revision},Operation::MutateGovernance{mutation:GovernanceMutation::ReconcileExpiry{expected_operation_id},..})=>
                    actor_ok&&original_operation_id==expected_operation_id&&(!applied||result==Some(&GovernanceResult::State(GovernanceState{selected_revision:restored_revision.clone(),temporary_override:None}))),
                (ControlDecision::OverrideExpired{..},Operation::AwaitCommands{..})=>record.actor==RecordedActor{principal:PrincipalRef::TrustedHostOperator,provenance:ActorProvenance::TrustedHost{path:TrustedHostPath::ControllerMaintenance}},
                (ControlDecision::OverrideSuperseded{original_operation_id,selected_revision},Operation::MutateGovernance{mutation:GovernanceMutation::Promote{configuration_revision,supersedes_override:Some(original),..},..})=>
                    actor_ok&&original_operation_id==original&&selected_revision==configuration_revision,
                _=>false,
            }
        }
        DecisionDetail::DefinitionLimits(decision)=>validate_definition_original(tx,record,decision,request,outcome)?,
        DecisionDetail::Completion(decision)=>validate_completion_original(tx,record,decision,request,outcome)?,
    };
    if !agrees{return Err(Fault::Store("governance audit differs from original typed request/result".into()))}
    Ok(())
}
fn actor_matches_caller(actor:&RecordedActor,caller:&crate::mcp::Caller)->bool {
    matches!(&actor.provenance,ActorProvenance::BoundAgentSession{task_id,attempt_id,generation,worker_id,incarnation,agent_id,..}
        if task_id==&caller.task_id&&attempt_id==&caller.attempt_id&&*generation==caller.generation&&worker_id==&caller.worker_id
            &&incarnation==&caller.incarnation&&actor.principal==(PrincipalRef::Agent{agent_id:agent_id.clone()}))
}
fn validate_completion_original(tx:&StoreContext<'_>,record:&DecisionRecord,decision:&CompletionDecision,request:&Operation,outcome:&Outcome)->Result<bool> {
    let subject=&decision.subject;
    let task=super::sessions::historical_task(tx,&subject.task_id,&subject.session_id)?;
    let selected=read_configuration(tx,&record.configuration.governance_revision)?;
    let accepted=task.input.resolved_limits.as_ref().and_then(|input|input.global_metadata.governance.as_ref());
    let Some(accepted)=accepted else{return Ok(false)};
    if record.rule!=selected.completion||!matches!(read_rule(tx,&record.rule)?.body,RuleBody::Completion{..})
        ||selected.global_revision!=record.configuration.global_revision||selected.definition_update_policy_revision!=record.configuration.definition_update_policy_revision
        ||record.mode!=DecisionMode::Enforcing||selected.revision==bootstrap(tx).revision||subject.task_revision>task.revision
        ||decision.reasons.len()>accepted.reason_items||encode(&decision.reasons)?.len()>accepted.reason_bytes
        ||decision.consecutive_count>accepted.completion_continuations.saturating_add(1) {return Ok(false)}
    if task.session_id!=subject.session_id||task.input.resolved_limits.as_ref().is_none_or(|input|input.global_revision!=subject.accepted_global_revision)
        ||record.configuration.accepted_global_revision.as_ref()!=Some(&subject.accepted_global_revision)
        ||record.observed_product_revision.is_none() {return Ok(false)}
    let expected=match request {
        Operation::EvaluateCompletion(value)=>{
            if !actor_matches_caller(&record.actor,&value.caller)||value.caller.task_id!=subject.task_id
                ||value.caller.attempt_id!=subject.attempt_id||value.caller.generation!=subject.generation {return Ok(false)}
            match outcome {
                Outcome::Ok{data:crate::protocol::Data::Completion(crate::governance_completion::CompletionOutcome::Evaluated(evaluation))}=>
                    evaluation.decision==record.id&&evaluation.state==*decision&&evaluation.boundary==value.boundary,
                Outcome::Refused{..}=>true,
                _=>false,
            }
        }
        Operation::ObserveWorker{report}=>{
            matches!(report.observation,crate::protocol::Observation::ResultCandidate{..})
                &&report.task_id==subject.task_id&&report.attempt_id==subject.attempt_id&&report.generation==subject.generation
                &&matches!(&record.actor.provenance,ActorProvenance::BoundAgentSession{task_id,attempt_id,generation,worker_id,incarnation,..}
                    if task_id==&report.task_id&&attempt_id==&report.attempt_id&&*generation==report.generation
                        &&worker_id==&report.worker_id&&incarnation==&report.incarnation)
        }
        _=>false,
    };
    Ok(expected)
}
fn validate_definition_original(tx:&StoreContext<'_>,record:&DecisionRecord,decision:&DefinitionLimitDecision,request:&Operation,outcome:&Outcome)->Result<bool> {
    let input=match request {
        Operation::UpdateDefinition{update,..} if matches!(record.actor.provenance,ActorProvenance::TrustedHost{..})=>update.clone(),
        Operation::CallCollaboration{call} if call.tool=="collab_definition_update"&&actor_matches_caller(&record.actor,&call.caller)=>
            serde_json::from_value::<crate::protocol::DefinitionUpdate>(call.arguments.clone())?,
        _=>return Ok(false),
    };
    if input.agent_id!=decision.agent_id||input.expected_revision!=decision.previous_revision||input.expected_global_revision!=record.configuration.global_revision
        ||record.configuration.definition_update_policy_revision.as_ref()!=Some(&input.expected_policy_revision)||record.configuration.accepted_global_revision.is_some() {
        return Ok(false)
    }
    let selected=read_configuration(tx,&record.configuration.governance_revision)?;
    if selected.global_revision!=record.configuration.global_revision||selected.definition_update_policy_revision!=record.configuration.definition_update_policy_revision
        ||record.rule!=selected.definition_limits||record.mode!=(if selected.revision==bootstrap(tx).revision {DecisionMode::Observation}else{DecisionMode::Enforcing}) {
        return Ok(false)
    }
    let raw:String=tx.query_row("SELECT CAST(substr(CAST(body AS BLOB),1,?3) AS TEXT) FROM definitions WHERE id=?1 AND revision=?2",
        params![decision.agent_id,decision.previous_revision,super::database::sql_bound(tx.governance_limits.transport.body_bytes.saturating_add(1))?],|row|row.get(0))?;
    if raw.len()>tx.governance_limits.transport.body_bytes{return Err(Fault::Store("governance prior definition exceeds bound".into()))}
    let prior:crate::protocol::Definition=decode(&raw)?;
    if prior.id!=decision.agent_id||prior.revision!=decision.previous_revision{return Ok(false)}
    let before=tx.governance_limits.resolve_definition(&prior).map_err(Fault::Store)?;
    let mut after=before.clone();for (field,value) in &input.changes {field.set(&mut after,*value)}
    let RuleBody::DefinitionLimits{allow_configured_widening}=read_rule(tx,&record.rule)?.body else{return Ok(false)};
    let (comparisons,allowed,reason)=crate::governance_comparison::compare(&before,&after,&input.changes,&selected.widening_fields,allow_configured_widening).map_err(Fault::Store)?;
    if comparisons!=decision.comparisons||allowed!=decision.allowed||reason!=decision.reason{return Ok(false)}
    match outcome {
        Outcome::Ok{data:crate::protocol::Data::DefinitionUpdate(value)}=>Ok(value.previous_revision==decision.previous_revision
            &&value.selected_definition.id==decision.agent_id&&decision.selected_revision.as_ref()==Some(&value.selected_definition.revision)
            &&value.global_revision==input.expected_global_revision&&value.policy.revision==input.expected_policy_revision
            &&tx.governance_limits.resolve_definition(&value.selected_definition).map_err(Fault::Store)?==after),
        Outcome::Refused{..}=>Ok(decision.selected_revision.is_none()),
        _=>Ok(false),
    }
}
fn stage_admin(tx:&StoreContext<'_>,operation_id:&str,actor:RecordedActor,detail:RuleAdministrationDecision)->Result<()> {
    let current=bootstrap(tx);
    stage(tx,Draft{mode:DecisionMode::Enforcing,operation_id:operation_id.into(),rule:rule_ref("governance-administration"),configuration:binding(tx,&current,None),actor,
        detail:DecisionDetail::RuleAdministration(detail),observed_product_revision:None})
}
fn count_room(tx:&StoreContext<'_>,table:&str,max:usize)->Result<()> {
    let sql=match table {"governance_rules"=>"SELECT count(*) FROM (SELECT 1 FROM governance_rules LIMIT ?1)","governance_configurations"=>"SELECT count(*) FROM (SELECT 1 FROM governance_configurations LIMIT ?1)",_=>return Err(Fault::Store("invalid closed governance inventory".into()))};
    let count:i64=tx.query_row(sql,[super::database::sql_bound(max.saturating_add(1))?],|r|r.get(0))?;
    if count<0||count as usize>=max{return refuse("capacity","governance immutable version capacity exhausted")}
    Ok(())
}
//= spec/impl/v2/implementation.md#decision-rules-have-versions-and-evidence
//# A shadow result MUST record whether substituting its verdict at the selected decision point would have changed the actual outcome.
fn evaluate_shadow(tx:&StoreContext<'_>,original:&DecisionRecord,candidate:&RuleVersion,configuration:&ConfigurationVersion)->Result<ShadowResult> {
    let fail=|code:&str|ShadowEvaluation::Failed{code:code.into()};
    let mut candidate_completion=None;
    let evaluation=if original.configuration.global_revision!=configuration.global_revision
        ||original.configuration.definition_update_policy_revision!=configuration.definition_update_policy_revision {fail("configuration_subject_mismatch")}
    else {
        let verdict=match (&original.detail,&candidate.body) {
            (DecisionDetail::DefinitionLimits(decision),RuleBody::DefinitionLimits{allow_configured_widening}) if candidate.reference==configuration.definition_limits=>{
                let widening=decision.comparisons.iter().any(|c|c.change==EffectiveChange::Widening);
                let allowed=!widening||(*allow_configured_widening&&decision.comparisons.iter().filter(|c|c.change==EffectiveChange::Widening).all(|c|configuration.widening_fields.contains(&c.field)));
                Some(if allowed {ShadowVerdict::Allow}else{ShadowVerdict::Block})
            }
            (DecisionDetail::Completion(decision),RuleBody::Completion{allow_continuation}) if candidate.reference==configuration.completion=>{
                let prior=read_rule(tx,&original.rule)?;
                if decision.reasons.iter().any(|reason|matches!(reason,CompletionReason::EvaluationUnavailable{..}))
                    ||matches!(prior.body,RuleBody::Completion{allow_continuation:false})&&*allow_continuation {
                    // Old blocked eligibility cannot prove fresh budget/turn
                    // availability. Failed evidence cannot qualify promotion.
                    None
                } else {
                    let mut predicted=decision.clone();
                    if !allow_continuation {if let CompletionDisposition::Blocked{continuation_eligible,..}=&mut predicted.disposition {*continuation_eligible=false}}
                    candidate_completion=Some(predicted);
                    Some(if decision.reasons.is_empty()&&matches!(decision.disposition,CompletionDisposition::Admit){ShadowVerdict::Allow}else{ShadowVerdict::Block})
                }
            }
            _=>None,
        };
        match verdict {
            None=>fail("decision_input_unavailable_or_mismatched"),
            Some(verdict)=>{
                let changed=match original.actual_outcome {
                    ActualDecisionOutcome::MandatoryRefusal{..}=>false,
                    ActualDecisionOutcome::Applied=>verdict==ShadowVerdict::Block,
                    ActualDecisionOutcome::GovernanceBlocked=>verdict==ShadowVerdict::Allow,
                };
                let passing=!(matches!(original.actual_outcome,ActualDecisionOutcome::GovernanceBlocked)&&verdict==ShadowVerdict::Allow);
                ShadowEvaluation::Evaluated{verdict,actual_outcome_would_change:changed,passing}
            }
        }
    };
    Ok(ShadowResult{original:original.id.clone(),original_controller_revision:original.controller_revision,input:ShadowInput::RecordedDecision,candidate_completion,
        candidate:candidate.reference.clone(),configuration_revision:configuration.revision.clone(),evaluated_at_controller_revision:super::metadata::current(tx)?,evaluation})
}
//= spec/impl/v2/implementation.md#decision-rules-have-versions-and-evidence
//# A shadow evaluation MUST NOT delay or change the enforcing decision or its effects.
fn shadow_input(tx:&StoreContext<'_>,original:&DecisionRef,candidate:&RuleVersion,configuration:&ConfigurationVersion,now:u64)->Result<ShadowResult> {
    let receipt=original_receipt(tx,&original.operation_id)?.ok_or_else(||Fault::Refused("not_found","original shadow receipt absent".into()))?;
    if let (DecisionPoint::Completion,Operation::EvaluateCompletion(value),Outcome::Ok{data:crate::protocol::Data::Completion(crate::governance_completion::CompletionOutcome::NotEnforced)})=
        (&original.point,&receipt.request,&receipt.outcome) {
        let RuleBody::Completion{allow_continuation}=&candidate.body else{return refuse("invalid_input","completion shadow requires a completion rule")};
        if candidate.reference!=configuration.completion{return refuse("configuration_conflict","completion candidate is not the selected configuration rule")}
        let cap=if *allow_continuation {limits(tx)?.completion_continuations}else{0};
        let assessed=super::governance_completion::shadow_assess_original(tx,value,cap,now)?
            .ok_or_else(||Fault::Refused("governance_unavailable","original task has no accepted completion policy".into()))?;
        let failed=assessed.decision.reasons.iter().any(|reason|matches!(reason,CompletionReason::EvaluationUnavailable{..}));
        let verdict=if matches!(assessed.decision.disposition,CompletionDisposition::Admit){ShadowVerdict::Allow}else{ShadowVerdict::Block};
        let evaluation=if failed {ShadowEvaluation::Failed{code:"completion_assessment_unavailable".into()}}
            else {ShadowEvaluation::Evaluated{verdict,actual_outcome_would_change:verdict==ShadowVerdict::Block,passing:true}};
        return Ok(ShadowResult{original:original.clone(),original_controller_revision:receipt.commit.controller_revision,
            input:ShadowInput::CurrentNotEnforcedCompletion{assessment:assessed.decision.clone(),observed_product_revision:assessed.observed_product_revision},
            candidate_completion:Some(assessed.decision),candidate:candidate.reference.clone(),configuration_revision:configuration.revision.clone(),
            evaluated_at_controller_revision:super::metadata::current(tx)?,evaluation})
    }
    evaluate_shadow(tx,&read_decision(tx,original)?,candidate,configuration)
}
//= spec/impl/v2/implementation.md#decision-rules-have-versions-and-evidence
//# A new or changed ordinary governance rule MUST run in shadow before enforcement.
//= spec/impl/v2/implementation.md#decision-rules-have-versions-and-evidence
//# Promotion of a governance rule MUST bind passing evaluation evidence to the exact rule and configuration revisions being promoted.
pub(super) fn validate_promotion_evidence(tx:&StoreContext<'_>,configuration:&ConfigurationVersion,evidence:&[ShadowEvidenceRef],before_revision:u64)->Result<()> {
    if configuration.revision==bootstrap(tx).revision||evidence.is_empty()||evidence.len()>limits(tx)?.shadow_evidence_items {
        return refuse("governance_evidence","bounded evidence for a registered non-bootstrap configuration is required")
    }
    let mut seen=BTreeSet::new();let mut completion=false;let mut definition=false;
    for item in evidence {
        if !seen.insert(&item.operation_id){return refuse("invalid_input","duplicate shadow evidence")}
        // Check decreasing original revision and closed request kind before
        // reading its audit; corrupt cycles cannot recurse through promotions.
        let prior=original_receipt(tx,&item.operation_id)?.ok_or_else(||Fault::Refused("governance_evidence","shadow original receipt absent".into()))?;
        if prior.commit.controller_revision>=before_revision||!matches!(prior.request,Operation::MutateGovernance{mutation:GovernanceMutation::EvaluateShadow{..},..}) {
            return refuse("governance_evidence","promotion evidence is not an earlier original shadow request")
        }
        let record=read_decision(tx,&DecisionRef{operation_id:item.operation_id.clone(),point:DecisionPoint::RuleAdministration})?;
        let DecisionDetail::RuleAdministration(RuleAdministrationDecision::Shadow{result})=record.detail else{return refuse("invalid_input","promotion evidence is not an actual shadow evaluation")};
        if !matches!(record.actual_outcome,ActualDecisionOutcome::Applied)||result.configuration_revision!=configuration.revision
            ||!matches!(result.evaluation,ShadowEvaluation::Evaluated{passing:true,..})||result.evaluated_at_controller_revision>=record.controller_revision {
            return refuse("governance_evidence","shadow evidence failed or does not bind this exact configuration")
        }
        completion|=result.candidate==configuration.completion;definition|=result.candidate==configuration.definition_limits;
    }
    if !completion||!definition{return refuse("governance_evidence","promotion requires passing evidence for both exact ordinary rules")}
    Ok(())
}
/// Direct operator branch only. Root dispatch must not advertise this as a tool.
pub(super) fn mutate(tx:&StoreContext<'_>,operation_id:&str,mutation:&GovernanceMutation,now:u64,cfg:&super::ControllerConfig)->Result<GovernanceResult> {
    let actor=operator(tx)?;identity(tx,operation_id)?;limits(tx)?;
    match mutation {
        GovernanceMutation::RegisterRule{rule}=>{
            identity(tx,&rule.reference.id)?;identity(tx,&rule.reference.revision)?;
            if bootstrap_rule(&rule.reference).is_some(){return refuse("configuration_conflict","built-in rule identities cannot be replaced")}
            count_room(tx,"governance_rules",limits(tx)?.rule_versions)?;
            let exists:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM governance_rules WHERE id=?1 AND revision=?2)",params![rule.reference.id,rule.reference.revision],|r|r.get(0))?;
            if exists{return refuse("configuration_conflict","rule identity already exists")}
            tx.execute("INSERT INTO governance_rules(id,revision,body,operation_id) VALUES(?1,?2,?3,?4)",params![rule.reference.id,rule.reference.revision,bounded(tx,rule)?,operation_id])?;
            stage_admin(tx,operation_id,actor,RuleAdministrationDecision::RegisteredRule{rule:rule.reference.clone()})?;
            Ok(GovernanceResult::Rule(rule.clone()))
        }
        GovernanceMutation::RegisterConfiguration{configuration}=>{
            validate_configuration(tx,configuration)?;
            if configuration.revision==bootstrap(tx).revision{return refuse("configuration_conflict","bootstrap configuration cannot be replaced")}
            count_room(tx,"governance_configurations",limits(tx)?.configuration_versions)?;
            let exists:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM governance_configurations WHERE revision=?1)",[&configuration.revision],|r|r.get(0))?;
            if exists{return refuse("configuration_conflict","configuration identity already exists")}
            tx.execute("INSERT INTO governance_configurations(revision,body,operation_id) VALUES(?1,?2,?3)",params![configuration.revision,bounded(tx,configuration)?,operation_id])?;
            stage_admin(tx,operation_id,actor,RuleAdministrationDecision::RegisteredConfiguration{revision:configuration.revision.clone()})?;
            Ok(GovernanceResult::Configuration(configuration.clone()))
        }
        GovernanceMutation::EvaluateShadow{original,candidate,configuration_revision}=>{
            let rule=read_rule(tx,candidate)?;let configuration=read_configuration(tx,configuration_revision)?;validate_configuration(tx,&configuration)?;
            let result=shadow_input(tx,original,&rule,&configuration,now)?;
            stage_admin(tx,operation_id,actor,RuleAdministrationDecision::Shadow{result:result.clone()})?;
            Ok(GovernanceResult::Shadow(result))
        }
        GovernanceMutation::Promote{expected_revision,configuration_revision,evidence,supersedes_override}=>{
            let active=state(tx)?;
            if active.selected_revision!=*expected_revision{return refuse("configuration_conflict","active governance revision changed")}
            match (&active.temporary_override,supersedes_override) {
                (None,None)=>(),
                (Some(original),Some(expected)) if &original.operation_id==expected=>(),
                _=>return refuse("configuration_conflict","normal promotion must explicitly name the exact active override it supersedes"),
            }
            if configuration_revision==&bootstrap(tx).revision{return refuse("governance_evidence","observation-only bootstrap cannot be promoted")}
            let configuration=read_configuration(tx,configuration_revision)?;validate_configuration(tx,&configuration)?;
            validate_promotion_evidence(tx,&configuration,evidence,super::metadata::current(tx)?.saturating_add(1))?;
            if tx.execute("UPDATE governance_configurations SET qualification=COALESCE(qualification,?2) WHERE revision=?1",
                params![configuration.revision,operation_id])?!=1 {
                return Err(Fault::Store("promotion did not qualify exactly one registered immutable configuration".into()))
            }
            if let Some(original)=&active.temporary_override {
                stage_control(tx,operation_id,actor.clone(),ControlDecision::OverrideSuperseded {
                    original_operation_id:original.operation_id.clone(),selected_revision:configuration.revision.clone(),
                })?;
            }
            save_state(tx,&GovernanceState{selected_revision:configuration.revision.clone(),temporary_override:None})?;
            stage_admin(tx,operation_id,actor,RuleAdministrationDecision::Promoted{prior_revision:active.selected_revision,selected_revision:configuration.revision.clone(),evidence:evidence.clone()})?;
            Ok(GovernanceResult::State(state(tx)?))
        }
        _=>super::governance_control::mutate(tx,operation_id,mutation,now,cfg,actor),
    }
}
pub(super) fn query(tx:&StoreContext<'_>,query:&GovernanceQuery)->Result<GovernanceResult> {
    operator(tx)?;match query {
        GovernanceQuery::Decision{decision}=>Ok(GovernanceResult::Decision(read_decision(tx,decision)?)),
        GovernanceQuery::TaskCompletion{task_id}=>{
            identity(tx,task_id)?;
            let stored=super::database::load(tx,task_id)?;
            // The task's retained hold is the locator, not a public blocker
            // string or a guess at the latest operation. A broken reference
            // remains an error, distinct from a task with no assessment.
            let decision=match &stored.completion_hold {
                Some(hold)=>{
                    super::governance_completion::validate_hold(tx,&stored.task,hold)?;
                    Some(Box::new(read_decision(tx,&hold.decision)?))
                },
                None=>None,
            };
            Ok(GovernanceResult::TaskCompletion{task_id:task_id.clone(),decision})
        },
        GovernanceQuery::Rule{rule}=>Ok(GovernanceResult::Rule(read_rule(tx,rule)?)),
        GovernanceQuery::Configuration{revision}=>Ok(GovernanceResult::Configuration(read_configuration(tx,revision)?)),
        GovernanceQuery::State=>Ok(GovernanceResult::State(state(tx)?)),
    }
}
pub(super) fn stage_control(tx:&StoreContext<'_>,operation_id:&str,actor:RecordedActor,detail:ControlDecision)->Result<()> {
    // Independent control never reads/executes the selected ordinary rule.
    let current=bootstrap(tx);
    stage(tx,Draft{mode:DecisionMode::Enforcing,operation_id:operation_id.into(),rule:rule_ref("emergency-control"),configuration:binding(tx,&current,None),actor,
        detail:DecisionDetail::EmergencyControl(detail),observed_product_revision:None})
}

/// Admission reserves the representable minimum failure/hold/audit before any
/// task can reach an enabled completion boundary. This is data sizing only.
pub(super) fn validate_failure_envelope(tx:&StoreContext<'_>,stored:&super::database::StoredTask)->Result<()> {
    let task=&stored.task;
    let accepted=task.input.resolved_limits.as_ref().ok_or_else(||Fault::Store("governance envelope lacks accepted metadata".into()))?;
    let Some(bound)=accepted.global_metadata.governance.as_ref() else{return Ok(())};
    let wide="x".repeat(tx.governance_limits.transport.identity_bytes.max(accepted.global_metadata.transport.identity_bytes));
    let reason=CompletionReason::EvaluationUnavailable{code:CompletionFailure::BoundExceeded};
    let decision=CompletionDecision{
        subject:CompletionSubject{task_id:task.id.clone(),session_id:task.session_id.clone(),attempt_id:wide.clone(),generation:u64::MAX,
            task_revision:u64::MAX,accepted_global_revision:accepted.global_revision.clone()},
        disposition:CompletionDisposition::Blocked{continuation_eligible:false,exhausted:true},reasons:vec![reason],
        consecutive_count:bound.completion_continuations.saturating_add(1),independent_reset_event:None,
    };
    let id=DecisionRef{operation_id:wide.clone(),point:DecisionPoint::Completion};
    let hold=CompletionHold{decision:id.clone(),state:decision.clone(),observed_product_revision:i64::MAX as u64,continuation:None};
    let record=DecisionRecord{mode:DecisionMode::Enforcing,id,rule:RuleRef{id:wide.clone(),revision:wide.clone()},
        configuration:ConfigurationBinding{global_revision:wide.clone(),governance_revision:wide.clone(),
            accepted_global_revision:Some(accepted.global_revision.clone()),definition_update_policy_revision:Some(wide.clone())},
        configuration_metadata:Some(Box::new(tx.governance_limits.clone())),
        actor:RecordedActor{principal:PrincipalRef::Agent{agent_id:task.assigned_agent_id.clone()},provenance:ActorProvenance::BoundAgentSession{
            agent_id:task.assigned_agent_id.clone(),session_id:task.session_id.clone(),task_id:task.id.clone(),attempt_id:wide.clone(),
            generation:u64::MAX,worker_id:wide.clone(),incarnation:wide}},
        detail:DecisionDetail::Completion(decision),actual_outcome:ActualDecisionOutcome::GovernanceBlocked,
        controller_revision:i64::MAX as u64,observed_product_revision:Some(i64::MAX as u64)};
    if encode(&hold)?.len()>bound.record_bytes.min(limits(tx)?.record_bytes)||encode(&vec![record])?.len()>limits(tx)?.record_bytes {
        return refuse("invalid_limits","governance.record_bytes cannot retain this accepted task's minimum failure envelope")
    }
    let mut task_body=serde_json::to_value(stored)?;
    task_body.as_object_mut().ok_or_else(||Fault::Store("stored task is not an object".into()))?
        .insert("completion_hold".into(),serde_json::to_value(hold)?);
    if super::bounded_json_size(&task_body,tx.governance_limits.transport.body_bytes.min(accepted.global_metadata.transport.body_bytes)).is_err() {
        return refuse("invalid_limits","accepted task cannot retain its minimal completion failure: transport.body_bytes")
    }
    super::governance_completion::validate_stop_failure_envelope(tx,stored)?;
    Ok(())
}

#[path = "governance_history.rs"]
mod history;
pub(super) fn validate_retained_configuration(tx:&StoreContext<'_>,value:&ConfigurationVersion)->Result<()> {
    history::validate_retained_configuration(tx,value)
}
pub(super) fn validate_metadata_inventory(tx:&StoreContext<'_>)->Result<()> {history::validate_metadata_inventory(tx)}

/// The fixed operator charter class does not widen the agent limit tool.
pub(super) fn stage_charter(tx:&StoreContext<'_>,operation_id:&str,actor:RecordedActor,detail:CharterConfigurationDecision)->Result<()> {
    let selected=selection(tx)?;
    stage(tx,Draft{mode:DecisionMode::Enforcing,operation_id:operation_id.into(),rule:rule_ref("charter-source-binding"),
        configuration:binding(tx,&selected,None),actor,detail:DecisionDetail::CharterConfiguration(detail),observed_product_revision:None})
}
