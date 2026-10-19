//! Real Controller requests and original receipts, with controlled worker reports.
//! No provider/process qualification or fabricated successful promotion records.
use super::*;
use borg_runtime::governance::*;
use borg_runtime::governance_completion::{self, CompletionBoundary, CompletionOutcome, EvaluateCompletion};
use borg_runtime::limits::{DefinitionLimitField, GovernanceLimits, WorkflowLimits};
use borg_runtime::mcp::Caller;
use std::collections::BTreeMap;

pub(super) fn governance_config() -> ControllerConfig {
    let mut cfg = config();
    let p = Arc::make_mut(&mut cfg.limits);
    p.workflow = Some(WorkflowLimits {
        record_bytes: 32_768, text_bytes: 4_096, options: 8,
        records: 512, revisions: 4_096, page_items: 8, page_bytes: 32_768,
        waits_per_task: 8, graph_scan_nodes: 512, events_per_mutation: 64,
        event_bytes: 8_192, notice_bytes: 8_192,
        pending_delivery_bytes: 32_768, delivery_scan_rows: 64,
    });
    p.governance = Some(GovernanceLimits {
        record_bytes: 65_536, reason_items: 16, reason_bytes: 4_096,
        scan_rows: 512, rule_versions: 16, configuration_versions: 16,
        shadow_evidence_items: 16, completion_continuations: 2,
        override_max_ms: 10_000,
    });
    p.validate().unwrap();
    cfg
}
fn governance_native(agent: &str) -> TaskInput {
    let mut input = native(agent);
    input.definition.execution_profile = Some(ExecutionProfile::CollaborationV2 { publication: None, web_search: None });
    input
}
pub(super) async fn governance_reply(c: &Controller, command: Operation) -> Reply {
    let r = Box::pin(c.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: format!("governance-request-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
        command: command.clone(),
    })).await;
    r.validate_for(&command).unwrap();
    r
}
async fn mutate(c: &Controller, id: &str, mutation: GovernanceMutation) -> Reply {
    governance_reply(c, Operation::MutateGovernance { operation_id: id.into(), mutation }).await
}
fn result(reply: &Reply) -> GovernanceResult {
    match &reply.outcome {
        Outcome::Ok { data: Data::Governance(value) } => (**value).clone(),
        other => panic!("governance result expected: {other:?}"),
    }
}
async fn query(c: &Controller, query: GovernanceQuery) -> GovernanceResult {
    let reply = governance_reply(c, Operation::QueryGovernance { query }).await;
    assert!(reply.commit.is_none());
    result(&reply)
}
async fn state(c: &Controller) -> GovernanceState {
    match query(c, GovernanceQuery::State).await {
        GovernanceResult::State(value) => value,
        other => panic!("state expected: {other:?}"),
    }
}
async fn configuration(c: &Controller, revision: &str) -> ConfigurationVersion {
    match query(c, GovernanceQuery::Configuration { revision: revision.into() }).await {
        GovernanceResult::Configuration(value) => value,
        other => panic!("configuration expected: {other:?}"),
    }
}
async fn decision(c: &Controller, operation_id: &str, point: DecisionPoint) -> DecisionRecord {
    match query(c, GovernanceQuery::Decision { decision: DecisionRef { operation_id: operation_id.into(), point } }).await {
        GovernanceResult::Decision(value) => value,
        other => panic!("decision expected: {other:?}"),
    }
}
async fn selected(c: &Controller, id: &str) -> Definition {
    match call(c, Operation::GetAgent { agent_id: id.into() }).await {
        Outcome::Ok { data: Data::Agent(value) } => value.definition,
        other => panic!("selected definition expected: {other:?}"),
    }
}
fn update(c: &Controller, id: &str, before: &Definition, value: usize) -> Operation {
    Operation::UpdateDefinition { operation_id: id.into(), update: DefinitionUpdate {
        agent_id: before.id.clone(), expected_revision: before.revision.clone(),
        expected_global_revision: c.limits().revision.clone(),
        expected_policy_revision: c.limits().definition_updates.as_ref().unwrap().revision.clone(),
        changes: BTreeMap::from([(DefinitionLimitField::NativeContextBytes, value)]),
    } }
}
fn changed(reply: &Reply) -> DefinitionUpdateOutcome {
    match &reply.outcome {
        Outcome::Ok { data: Data::DefinitionUpdate(value) } => (**value).clone(),
        other => panic!("definition result expected: {other:?}"),
    }
}
async fn register_candidate(c: &Controller, label: &str, allow_widening: bool) -> ConfigurationVersion {
    let completion = RuleVersion { reference: RuleRef { id: format!("{label}-completion"), revision: "1".into() },
        body: RuleBody::Completion { allow_continuation: true } };
    let definition = RuleVersion { reference: RuleRef { id: format!("{label}-definition"), revision: "1".into() },
        body: RuleBody::DefinitionLimits { allow_configured_widening: allow_widening } };
    for (suffix, rule) in [("completion", &completion), ("definition", &definition)] {
        let r = mutate(c, &format!("{label}-register-{suffix}"), GovernanceMutation::RegisterRule { rule: rule.clone() }).await;
        assert!(r.commit.is_some());
        assert_eq!(result(&r), GovernanceResult::Rule(rule.clone()));
    }
    let mut candidate = configuration(c, &c.limits().revision).await;
    candidate.revision = format!("{label}-configuration");
    candidate.completion = completion.reference;
    candidate.definition_limits = definition.reference;
    let r = mutate(c, &format!("{label}-register-configuration"), GovernanceMutation::RegisterConfiguration { configuration: candidate.clone() }).await;
    assert!(r.commit.is_some());
    assert_eq!(result(&r), GovernanceResult::Configuration(candidate.clone()));
    candidate
}
async fn active_native(c: &Controller, label: &str) -> (Task, Attempt, Caller) {
    active_native_with_subject(c, label, governance_native(label)).await
}
async fn active_native_with_subject(c: &Controller, label: &str, subject: TaskInput) -> (Task, Attempt, Caller) {
    let t = submit(c, &format!("{label}-submit"), subject).await;
    let w = worker(&format!("{label}-worker"), "incarnation-1", 1, 0);
    register(c, &w).await;
    let issued = commands(c, &w, vec![]).await;
    let a = run(&issued);
    assert_eq!(a.task_id, t.id, "fixture must own its requested task");
    task(report(c, &a, &format!("{label}-started"), Observation::Started { thread_id: None, turn_id: None }).await);
    let acknowledged: Vec<String> = issued.iter().filter_map(|command| match command {
        WorkerCommand::RunAttempt { command_id, .. } => Some(command_id.clone()),
        _ => None,
    }).collect();
    let remaining = commands(c, &w, acknowledged).await;
    assert!(!remaining.iter().any(|command| matches!(command, WorkerCommand::RunAttempt { .. })));
    completed_native_request(c, &a, Some(4), Some(2)).await;
    let caller = Caller { task_id: t.id.clone(), attempt_id: a.id.clone(), generation: a.generation,
        worker_id: a.worker_id.clone(), incarnation: a.worker_incarnation.clone() };
    (t, a, caller)
}
/// Controlled current-protocol request evidence through the real receipt path.
/// This fixture does not call a provider or rewrite historical unmarked Usage.
pub(super) async fn completed_native_request(c: &Controller, a: &Attempt, input_tokens: Option<u64>, output_tokens: Option<u64>) {
    task(report(c, a, &borg_runtime::native_loop_contract::reservation_id(&a.id, 1), Observation::Usage {
        native_diagnostic_reserved: true,
        model_requests: 1, input_tokens, output_tokens,
    }).await);
    let accepted = get(c, &a.task_id).await;
    let outcome = report(c, a, &borg_runtime::native_loop_contract::diagnostic_id(&a.id, 1),
        Observation::NativeRequestDiagnostic {
            request_index: 1,
            endpoint: "https://api.openai.com/v1/responses".into(),
            model: accepted.input.definition.required_inference().unwrap().model.clone(),
            // Exact binding of the synthetic request bytes b"{}".
            request_binding: Some(borg_runtime::native::ProviderRequestBinding {
                bytes: 2,
                sha256: "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a".into(),
            }),
            observed_outcome: borg_runtime::native_loop_contract::NativeRequestObservedOutcome::Completed,
            failure: None,
        }).await;
    assert!(matches!(outcome, Outcome::Ok { data: Data::Acknowledged }), "{outcome:?}");
}
async fn retire_subject(c: &Controller, a: &Attempt, label: &str) {
    let current = get(c, &a.task_id).await;
    task(call(c, Operation::CancelTask { operation_id: format!("{label}-cancel"),
        task_id: current.id, expected_revision: current.revision }).await);
    let stopped = task(report(c, a, &format!("{label}-stopped"), Observation::ProcessTreeStopped).await);
    assert!(stopped.attempt.as_ref().unwrap().stopped);
}
async fn shadow(c: &Controller, id: &str, original: DecisionRef, candidate: RuleRef, configuration_revision: &str,
    task_id: &str, expected: ShadowEvaluation) -> Reply {
    // Read-only snapshots do not advance the bootstrap input's controller view.
    let task_before = get(c, task_id).await;
    let definition_before = selected(c, &task_before.assigned_agent_id).await;
    let state_before = state(c).await;
    let lookup = Operation::GetOperation { operation_id: original.operation_id.clone() };
    let receipt_before = governance_reply(c, lookup.clone()).await;
    let original_commit = receipt_before.commit.as_ref().expect("original receipt must be committed");
    let r = mutate(c, id, GovernanceMutation::EvaluateShadow { original: original.clone(), candidate: candidate.clone(),
        configuration_revision: configuration_revision.into() }).await;
    let shadow_commit = r.commit.as_ref().expect("shadow has its own committed receipt");
    assert_eq!(shadow_commit.operation_id, id);
    assert_ne!(shadow_commit.operation_id, original_commit.operation_id);
    assert!(shadow_commit.controller_revision > original_commit.controller_revision);
    assert_eq!(serde_json::to_value(get(c, task_id).await).unwrap(), serde_json::to_value(&task_before).unwrap());
    assert_eq!(selected(c, &task_before.assigned_agent_id).await, definition_before);
    assert_eq!(state(c).await, state_before);
    let receipt_after = governance_reply(c, lookup).await;
    assert_eq!(receipt_after.outcome, receipt_before.outcome);
    assert_eq!(receipt_after.commit, receipt_before.commit);
    let GovernanceResult::Shadow(view) = result(&r) else { panic!("shadow result expected") };
    assert_eq!(view.original, original);
    assert_eq!(view.candidate, candidate);
    assert_eq!(view.configuration_revision, configuration_revision);
    assert_eq!(view.evaluation, expected, "exact counterfactual differs: {view:?}");
    let audit = decision(c, id, DecisionPoint::RuleAdministration).await;
    assert_eq!(audit.mode, DecisionMode::Enforcing);
    assert_eq!(audit.actual_outcome, ActualDecisionOutcome::Applied);
    assert_eq!(audit.controller_revision, r.commit.as_ref().unwrap().controller_revision);
    assert_eq!(audit.detail, DecisionDetail::RuleAdministration(RuleAdministrationDecision::Shadow { result: view }));
    r
}
struct Evidence {
    original: Operation, reply: Reply, operations: Vec<String>, attempt: Attempt,
}
async fn evidence(c: &Controller, label: &str, candidate: &ConfigurationVersion) -> Evidence {
    evidence_with_subject(c, label, candidate, governance_native(&format!("{label}-subject"))).await
}
async fn evidence_with_subject(c: &Controller, label: &str, candidate: &ConfigurationVersion, subject: TaskInput) -> Evidence {
    let (t, attempt, caller) = active_native_with_subject(c, &format!("{label}-subject"), subject).await;
    let boundary = CompletionBoundary::NativeResponse { request_index: 1 };
    let original = Operation::EvaluateCompletion(EvaluateCompletion {
        operation_id: governance_completion::evaluation_id(&caller, &boundary).unwrap(), caller, boundary, intervening_requests: vec![],
    });
    let reply = governance_reply(c, original.clone()).await;
    assert!(reply.commit.is_some(), "completion observation must retain its original receipt: {reply:?}");
    let supported = match &reply.outcome {
        Outcome::Ok { data: Data::Completion(value) } => matches!(value.as_ref(), CompletionOutcome::NotEnforced | CompletionOutcome::Evaluated(_)),
        Outcome::Refused { code, .. } => code == "governance_completion_blocked",
        _ => false,
    };
    assert!(supported, "{reply:?}");
    let completion_id = format!("{label}-shadow-completion");
    // No intervening mutation: bootstrap reconstruction requires this exact
    // NotEnforced receipt to remain the current canonical input view.
    shadow(c, &completion_id, DecisionRef { operation_id: original.mutation_id().unwrap().into(), point: DecisionPoint::Completion },
        candidate.completion.clone(), &candidate.revision, &t.id, ShadowEvaluation::Evaluated {
            verdict: ShadowVerdict::Allow, actual_outcome_would_change: false, passing: true,
        }).await;
    let before = selected(c, &t.assigned_agent_id).await;
    let value = c.limits().resolve_definition(&before).unwrap().native.context_bytes;
    let definition_id = format!("{label}-neutral-definition");
    let definition = governance_reply(c, update(c, &definition_id, &before, value)).await;
    assert_eq!(changed(&definition).classification, ConfigurationChangeClass::Neutral);
    let definition_id_shadow = format!("{label}-shadow-definition");
    shadow(c, &definition_id_shadow, DecisionRef { operation_id: definition_id, point: DecisionPoint::DefinitionLimits },
        candidate.definition_limits.clone(), &candidate.revision, &t.id, ShadowEvaluation::Evaluated {
            verdict: ShadowVerdict::Allow, actual_outcome_would_change: false, passing: true,
        }).await;
    Evidence { original, reply, operations: vec![completion_id, definition_id_shadow], attempt }
}

pub(super) struct Promotion {
    pub configuration: ConfigurationVersion,
    pub request: Operation,
    pub reply: Reply,
    pub completion_original: Operation,
    pub completion_reply: Reply,
    pub shadow_operations: Vec<String>,
}
/// Shared real qualification helper. Invoke before queuing another test task.
/// It frees its actual attempt using cancellation and a stopped observation.
pub(super) async fn promote(c: &Controller, label: &str, allow_widening: bool) -> Promotion {
    promote_with_subject(c, label, allow_widening, governance_native(&format!("{label}-subject"))).await
}
/// Explicit caller-supplied fixture input retains admission under its selected
/// policy; it does not repair an arbitrary task or bypass charter validation.
pub(super) async fn promote_with_subject(c: &Controller, label: &str, allow_widening: bool, subject: TaskInput) -> Promotion {
    let candidate = register_candidate(c, label, allow_widening).await;
    let prepared = evidence_with_subject(c, label, &candidate, subject).await;
    let prior = state(c).await;
    let request = Operation::MutateGovernance { operation_id: format!("{label}-promote"), mutation: GovernanceMutation::Promote {
        expected_revision: prior.selected_revision, configuration_revision: candidate.revision.clone(),
        evidence: prepared.operations.iter().map(|id| ShadowEvidenceRef { operation_id: id.clone() }).collect(),
        supersedes_override: None,
    } };
    let reply = governance_reply(c, request.clone()).await;
    assert!(reply.commit.is_some());
    assert_eq!(result(&reply), GovernanceResult::State(GovernanceState {
        selected_revision: candidate.revision.clone(), temporary_override: None,
    }));
    retire_subject(c, &prepared.attempt, label).await;
    Promotion { configuration: candidate, request, reply, completion_original: prepared.original,
        completion_reply: prepared.reply, shadow_operations: prepared.operations }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#decision-rules-have-versions-and-evidence
//= type=test
//# Every enforced governance rule MUST have an immutable version identity.
async fn immutable_registration_replays_original_and_refuses_changed_rule_identity() {
    let c = Controller::open_with_config(path(), governance_config()).unwrap();
    let rule = RuleVersion { reference: RuleRef { id: "registered-rule".into(), revision: "1".into() },
        body: RuleBody::Completion { allow_continuation: true } };
    let op = Operation::MutateGovernance { operation_id: "register-original".into(), mutation: GovernanceMutation::RegisterRule { rule: rule.clone() } };
    let original = governance_reply(&c, op.clone()).await;
    assert_eq!(result(&original), GovernanceResult::Rule(rule.clone()));
    let replay = governance_reply(&c, op).await;
    assert_eq!(replay.outcome, original.outcome); assert_eq!(replay.commit, original.commit);
    let changed = RuleVersion { body: RuleBody::Completion { allow_continuation: false }, ..rule.clone() };
    assert_eq!(code(mutate(&c, "register-original", GovernanceMutation::RegisterRule { rule: changed.clone() }).await.outcome), "operation_conflict");
    assert_eq!(code(mutate(&c, "replace-same-rule", GovernanceMutation::RegisterRule { rule: changed }).await.outcome), "configuration_conflict");
    assert_eq!(query(&c, GovernanceQuery::Rule { rule: rule.reference.clone() }).await, GovernanceResult::Rule(rule));
    let audit = decision(&c, "register-original", DecisionPoint::RuleAdministration).await;
    assert_eq!(audit.actual_outcome, ActualDecisionOutcome::Applied);
    assert_eq!(audit.controller_revision, original.commit.unwrap().controller_revision);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#decision-rules-have-versions-and-evidence
//= type=test
//# Replay of a recorded governance decision MUST retain its original decision identity and outcome.
//= spec/impl/v2/implementation.md#decision-rules-have-versions-and-evidence
//= type=test
//# A new or changed ordinary governance rule MUST run in shadow before enforcement.
//= spec/impl/v2/implementation.md#decision-rules-have-versions-and-evidence
//= type=test
//# A shadow evaluation MUST NOT delay or change the enforcing decision or its effects.
//= spec/impl/v2/implementation.md#decision-rules-have-versions-and-evidence
//= type=test
//# A shadow result MUST record whether substituting its verdict at the selected decision point would have changed the actual outcome.
async fn real_shadow_promotion_and_original_receipts_survive_restart_and_replay() {
    let p = path(); let cfg = governance_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let promoted = promote(&c, "real-proof", false).await;
    assert!(matches!(&promoted.completion_reply.outcome, Outcome::Ok { data: Data::Completion(value) }
        if matches!(value.as_ref(), CompletionOutcome::NotEnforced)));
    let before = decision(&c, promoted.request.mutation_id().unwrap(), DecisionPoint::RuleAdministration).await;
    let mut shadows = Vec::new();
    for id in &promoted.shadow_operations { shadows.push(decision(&c, id, DecisionPoint::RuleAdministration).await); }
    drop(c); controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg).unwrap();
    let replay = governance_reply(&c, promoted.request.clone()).await;
    assert_eq!(replay.outcome, promoted.reply.outcome); assert_eq!(replay.commit, promoted.reply.commit);
    let historical = governance_reply(&c, promoted.completion_original).await;
    assert_eq!(historical.outcome, promoted.completion_reply.outcome); assert_eq!(historical.commit, promoted.completion_reply.commit);
    assert_eq!(state(&c).await.selected_revision, promoted.configuration.revision);
    assert_eq!(decision(&c, promoted.request.mutation_id().unwrap(), DecisionPoint::RuleAdministration).await, before);
    for (id, original) in promoted.shadow_operations.iter().zip(shadows) {
        assert_eq!(decision(&c, id, DecisionPoint::RuleAdministration).await, original);
    }

    // A real enforcing refusal supplies a different counterfactual: allowing
    // this widening would change the outcome and is not passing promotion proof.
    let candidate = register_candidate(&c, "widening-shadow", true).await;
    let prepared = evidence(&c, "widening-shadow", &candidate).await;
    let subject = get(&c, &prepared.attempt.task_id).await;
    let head = selected(&c, &subject.assigned_agent_id).await;
    let allowance = c.limits().resolve_definition(&head).unwrap().native.context_bytes;
    let denied_op = update(&c, "widening-shadow-original", &head, allowance.checked_add(1).unwrap());
    let denied = governance_reply(&c, denied_op.clone()).await;
    assert_eq!(code(denied.outcome.clone()), "governance_configuration_blocked");
    assert!(denied.commit.is_some());
    let denied_audit = decision(&c, "widening-shadow-original", DecisionPoint::DefinitionLimits).await;
    assert_eq!(denied_audit.actual_outcome, ActualDecisionOutcome::GovernanceBlocked);
    let altered_id = "widening-shadow-counterfactual";
    shadow(&c, altered_id, DecisionRef { operation_id: "widening-shadow-original".into(), point: DecisionPoint::DefinitionLimits },
        candidate.definition_limits.clone(), &candidate.revision, &subject.id, ShadowEvaluation::Evaluated {
            verdict: ShadowVerdict::Allow, actual_outcome_would_change: true, passing: false,
        }).await;
    assert_eq!(decision(&c, "widening-shadow-original", DecisionPoint::DefinitionLimits).await, denied_audit);
    let original_after = governance_reply(&c, denied_op).await;
    assert_eq!(original_after.outcome, denied.outcome);
    assert_eq!(original_after.commit, denied.commit);

    let wrong_candidate = register_candidate(&c, "wrong-shadow-subject", true).await;
    let active_before = state(&c).await;
    let task_before = get(&c, &subject.id).await;
    let head_before = selected(&c, &subject.assigned_agent_id).await;
    for (id, revision, proof) in [
        ("promotion-missing-proof", candidate.revision.clone(), vec![prepared.operations[0].clone()]),
        ("promotion-wrong-subject", wrong_candidate.revision.clone(), prepared.operations.clone()),
        ("promotion-failed-shadow", candidate.revision.clone(), vec![prepared.operations[0].clone(), altered_id.into()]),
    ] {
        let refused = mutate(&c, id, GovernanceMutation::Promote {
            expected_revision: active_before.selected_revision.clone(), configuration_revision: revision,
            evidence: proof.into_iter().map(|operation_id| ShadowEvidenceRef { operation_id }).collect(),
            supersedes_override: None,
        }).await;
        assert_eq!(code(refused.outcome), "governance_evidence", "unexpected promotion result: {id}");
        assert_eq!(state(&c).await, active_before);
        assert_eq!(selected(&c, &subject.assigned_agent_id).await, head_before);
        assert_eq!(serde_json::to_value(get(&c, &subject.id).await).unwrap(), serde_json::to_value(&task_before).unwrap());
    }
    retire_subject(&c, &prepared.attempt, "widening-shadow").await;
}

#[tokio::test]
async fn retained_audit_and_version_membership_corruption_refuse_without_repair() {
    for corruption in ["shadow-audit", "stored-builtin", "deleted-selection"] {
        let p = path(); let cfg = governance_config();
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        let promoted = promote(&c, corruption, false).await;
        let id = promoted.shadow_operations[0].clone();
        drop(c); controller_owner_released(&p).await;
        let db = rusqlite::Connection::open(&p).unwrap();
        let original: (String, String, i64, String) = db.query_row(
            "SELECT request,outcome,controller_revision,governance FROM receipts WHERE id=?1", [&id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))).unwrap();
        let retained_state: String = db.query_row("SELECT body FROM governance_state WHERE singleton=1", [], |r| r.get(0)).unwrap();
        let mut expected_audit = original.3.clone();
        let builtin = RuleVersion { reference: RuleRef { id: "completion-required-work".into(), revision: "1".into() },
            body: RuleBody::Completion { allow_continuation: false } };
        let builtin_body = serde_json::to_string(&builtin).unwrap();
        match corruption {
            "shadow-audit" => {
                let mut audit: Vec<DecisionRecord> = serde_json::from_str(&original.3).unwrap();
                let record = audit.iter_mut().find(|r| r.id.point == DecisionPoint::RuleAdministration).unwrap();
                let DecisionDetail::RuleAdministration(RuleAdministrationDecision::Shadow { result }) = &mut record.detail else { panic!("real shadow expected") };
                result.configuration_revision = "another-configuration".into();
                expected_audit = serde_json::to_string(&audit).unwrap();
                assert_eq!(db.execute("UPDATE receipts SET governance=?2 WHERE id=?1", rusqlite::params![id, expected_audit]).unwrap(), 1);
            }
            "stored-builtin" => {
                // This is a corruption injection, not a successful registration:
                // public RegisterRule forbids this exact builtin identity.
                assert_eq!(db.execute("INSERT INTO governance_rules(id,revision,body,operation_id) VALUES(?1,?2,?3,?4)",
                    rusqlite::params![builtin.reference.id, builtin.reference.revision, builtin_body, id]).unwrap(), 1);
            }
            "deleted-selection" => {
                assert_eq!(db.execute("DELETE FROM governance_configurations WHERE revision=?1",
                    [&promoted.configuration.revision]).unwrap(), 1);
            }
            _ => unreachable!(),
        }
        drop(db);
        for _ in 0..2 {
            assert!(Controller::open_with_config(&p, cfg.clone()).is_err(), "retained corruption accepted: {corruption}");
            controller_owner_released(&p).await;
            let db = rusqlite::Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
            let actual: (String, String, i64, String) = db.query_row(
                "SELECT request,outcome,controller_revision,governance FROM receipts WHERE id=?1", [&id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))).unwrap();
            assert_eq!(actual, (original.0.clone(), original.1.clone(), original.2, expected_audit.clone()));
            assert_eq!(db.query_row("SELECT body FROM governance_state WHERE singleton=1", [], |r| r.get::<_, String>(0)).unwrap(), retained_state);
            match corruption {
                "stored-builtin" => {
                    let retained: (String, String) = db.query_row("SELECT body,operation_id FROM governance_rules WHERE id=?1 AND revision=?2",
                        rusqlite::params![builtin.reference.id, builtin.reference.revision], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
                    assert_eq!(retained, (builtin_body.clone(), id.clone()));
                }
                "deleted-selection" => assert_eq!(db.query_row("SELECT count(*) FROM governance_configurations WHERE revision=?1",
                    [&promoted.configuration.revision], |r| r.get::<_, i64>(0)).unwrap(), 0),
                _ => (),
            }
        }
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#decision-rules-have-versions-and-evidence
//= type=test
//# Promotion of a governance rule MUST bind passing evaluation evidence to the exact rule and configuration revisions being promoted.
async fn implicit_observation_bootstrap_cannot_report_a_successful_promotion() {
    let c = Controller::open_with_config(path(), governance_config()).unwrap();
    let initial = state(&c).await;
    let bootstrap = configuration(&c, &initial.selected_revision).await;
    let prepared = evidence(&c, "bootstrap-noop", &bootstrap).await;
    let r = mutate(&c, "bootstrap-noop-promote", GovernanceMutation::Promote {
        expected_revision: initial.selected_revision.clone(), configuration_revision: bootstrap.revision,
        evidence: prepared.operations.iter().map(|id| ShadowEvidenceRef { operation_id: id.clone() }).collect(),
        supersedes_override: None,
    }).await;
    assert_eq!(code(r.outcome), "governance_evidence");
    assert_eq!(state(&c).await, initial);
    retire_subject(&c, &prepared.attempt, "bootstrap-noop").await;
}

#[tokio::test]
//= spec/impl/v2/implementation.md#configuration-changes-preserve-governance
//= type=test
//# A governed configuration change MUST identify the immutable classification-rule revision used to evaluate it.
//= spec/impl/v2/implementation.md#decision-rules-have-versions-and-evidence
//= type=test
//# A governance decision record MUST identify its rule version, exact subject, caller provenance, outcome and bounded reason.
async fn enforced_comparison_distinguishes_neutral_narrowing_and_disallowed_widening() {
    let c = Controller::open_with_config(path(), governance_config()).unwrap();
    promote(&c, "comparison", false).await;
    let t = submit(&c, "comparison-task", governance_native("comparison-agent")).await;
    let accepted = serde_json::to_vec(&t.input).unwrap();
    let original = selected(&c, &t.assigned_agent_id).await;
    let effective = c.limits().resolve_definition(&original).unwrap();
    let neutral = governance_reply(&c, update(&c, "comparison-neutral", &original, effective.native.context_bytes)).await;
    assert_eq!(changed(&neutral).classification, ConfigurationChangeClass::Neutral);
    let head = changed(&neutral).selected_definition;
    let narrowed = governance_reply(&c, update(&c, "comparison-narrow", &head, effective.native.context_bytes - 1)).await;
    assert_eq!(changed(&narrowed).classification, ConfigurationChangeClass::Narrowing);
    let head = changed(&narrowed).selected_definition;
    let refused_op = update(&c, "comparison-wide", &head, effective.native.context_bytes);
    let denied = governance_reply(&c, refused_op.clone()).await;
    assert_eq!(code(denied.outcome.clone()), "governance_configuration_blocked");
    let replay = governance_reply(&c, refused_op).await;
    assert_eq!(replay.outcome, denied.outcome); assert_eq!(replay.commit, denied.commit);
    assert_eq!(selected(&c, &t.assigned_agent_id).await, head);
    let audit = decision(&c, "comparison-wide", DecisionPoint::DefinitionLimits).await;
    assert_eq!(audit.mode, DecisionMode::Enforcing);
    assert_eq!(audit.actual_outcome, ActualDecisionOutcome::GovernanceBlocked);
    let DecisionDetail::DefinitionLimits(value) = audit.detail else { panic!("comparison expected") };
    assert!(!value.allowed); assert_eq!(value.reason, ConfigurationReason::WideningNotConfigured);
    assert_eq!(value.comparisons.len(), 1); assert_eq!(value.comparisons[0].change, EffectiveChange::Widening);
    let mut expected = effective; expected.native.context_bytes -= 1;
    assert_eq!(c.limits().resolve_definition(&head).unwrap(), expected);
    assert_eq!(serde_json::to_vec(&get(&c, &t.id).await.input).unwrap(), accepted);
}

#[cfg(debug_assertions)]
#[tokio::test]
//= spec/impl/v2/implementation.md#configuration-changes-preserve-governance
//= type=test
//# A governed configuration change MUST commit its comparison evidence with its immutable revision and original operation outcome.
//= spec/impl/v2/implementation.md#decision-rules-have-versions-and-evidence
//= type=test
//# An enforcing governance decision MUST commit its audit event atomically with the transition it gates.
async fn commit_failure_rolls_back_definition_head_audit_and_original_receipt_together() {
    let mut cfg = governance_config();
    let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
    cfg.qualification_fail_next_commit = Some(fail.clone());
    let c = Controller::open_with_config(path(), cfg).unwrap();
    promote(&c, "rollback", false).await;
    let t = submit(&c, "rollback-task", governance_native("rollback-agent")).await;
    let head = selected(&c, &t.assigned_agent_id).await;
    let value = c.limits().resolve_definition(&head).unwrap().native.context_bytes;
    let op = update(&c, "rollback-original", &head, value - 1);
    fail.store(true, Ordering::SeqCst);
    let failed = governance_reply(&c, op.clone()).await;
    assert!(!fail.load(Ordering::SeqCst)); assert_eq!(code(failed.outcome), "store_unavailable");
    assert!(failed.commit.is_none()); assert_eq!(selected(&c, &t.assigned_agent_id).await, head);
    assert_eq!(code(call(&c, Operation::GetOperation { operation_id: "rollback-original".into() }).await), "not_found");
    let audit = governance_reply(&c, Operation::QueryGovernance { query: GovernanceQuery::Decision {
        decision: DecisionRef { operation_id: "rollback-original".into(), point: DecisionPoint::DefinitionLimits },
    } }).await;
    assert_eq!(code(audit.outcome), "not_found");
    let succeeded = governance_reply(&c, op).await;
    assert_eq!(changed(&succeeded).classification, ConfigurationChangeClass::Narrowing);
    let audit = decision(&c, "rollback-original", DecisionPoint::DefinitionLimits).await;
    assert_eq!(audit.actual_outcome, ActualDecisionOutcome::Applied);
    assert_eq!(audit.controller_revision, succeeded.commit.unwrap().controller_revision);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#emergency-control-is-independent-of-ordinary-policy
//= type=test
//# Expiry or removal of a temporary governance override MUST NOT resume held work without the normal recovery checks.
async fn emergency_pause_preserves_original_hold_and_release_only_reopens_admission() {
    let c = Controller::open_with_config(path(), governance_config()).unwrap();
    let t = submit(&c, "paused-task", governance_native("paused-agent")).await;
    let w = worker("paused-worker", "incarnation-1", 1, 0); register(&c, &w).await;
    let pause = Operation::MutateGovernance { operation_id: "pause-original".into(), mutation: GovernanceMutation::Pause {
        scope: ControlScope::AllTasks, reason: "operator maintenance".into(),
    } };
    let original = governance_reply(&c, pause.clone()).await;
    let GovernanceResult::Hold(hold) = result(&original) else { panic!("hold expected") };
    assert_eq!(hold.operation_id, "pause-original"); assert_eq!(hold.reason, "operator maintenance");
    assert!(commands(&c, &w, vec![]).await.iter().all(|v| !matches!(v, WorkerCommand::RunAttempt { .. })));
    assert_eq!(get(&c, &t.id).await.attempt, None);
    assert_eq!(code(mutate(&c, "duplicate-pause", GovernanceMutation::Pause { scope: ControlScope::AllTasks, reason: "replacement".into() }).await.outcome), "governance_hold_exists");
    assert_eq!(code(mutate(&c, "stale-release", GovernanceMutation::ReleaseHold {
        scope: ControlScope::AllTasks, expected_operation_id: "wrong".into(), reason: "stale".into(),
    }).await.outcome), "configuration_conflict");
    let replay = governance_reply(&c, pause).await;
    assert_eq!(replay.outcome, original.outcome); assert_eq!(replay.commit, original.commit);
    let released = mutate(&c, "exact-release", GovernanceMutation::ReleaseHold {
        scope: ControlScope::AllTasks, expected_operation_id: hold.operation_id, reason: "maintenance finished".into(),
    }).await;
    assert_eq!(result(&released), GovernanceResult::Released(ControlScope::AllTasks));
    assert_eq!(get(&c, &t.id).await.attempt, None, "release itself must not dispatch");
    let a = run(&commands(&c, &w, vec![]).await); assert_eq!(a.task_id, t.id);
    retire_subject(&c, &a, "paused").await;
}

#[tokio::test]
async fn emergency_stop_requests_real_stop_without_fabricating_physical_completion() {
    let c = Controller::open_with_config(path(), governance_config()).unwrap();
    promote(&c, "stop-rule", false).await;
    let (t, a, _) = active_native(&c, "stop-subject").await;
    let current = get(&c, &t.id).await;
    let request = Operation::MutateGovernance { operation_id: "stop-original".into(), mutation: GovernanceMutation::Stop {
        task_id: t.id.clone(), expected_revision: current.revision, reason: "independent operator stop".into(),
    } };
    let stopped = governance_reply(&c, request.clone()).await;
    assert_eq!(result(&stopped), GovernanceResult::StopRequested { task_id: t.id.clone() });
    assert!(!get(&c, &t.id).await.attempt.as_ref().unwrap().stopped);
    let w = worker(&a.worker_id, &a.worker_incarnation, 1, 0);
    let cmds = commands(&c, &w, vec![]).await;
    assert!(cmds.iter().any(|v| matches!(v, WorkerCommand::StopAttempt { task_id, attempt_id, .. } if task_id == &t.id && attempt_id == &a.id)));
    assert!(cmds.iter().all(|v| !matches!(v, WorkerCommand::RunAttempt { .. })));
    let current = task(report(&c, &a, "actual-stop-observation", Observation::ProcessTreeStopped).await);
    assert!(current.attempt.as_ref().unwrap().stopped);
    let replay = governance_reply(&c, request).await;
    assert_eq!(replay.outcome, stopped.outcome); assert_eq!(replay.commit, stopped.commit);
    assert_eq!(decision(&c, "stop-original", DecisionPoint::EmergencyControl).await.actual_outcome, ActualDecisionOutcome::Applied);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#emergency-control-is-independent-of-ordinary-policy
//= type=test
//# A temporary governance override MUST record its exact scope, prior policy, replacement policy, reason and expiry in a durable audit event.
async fn override_expiry_restores_exact_prior_selection_and_keeps_emergency_hold() {
    let clock = Arc::new(AtomicU64::new(future() - 300_000));
    let mut cfg = governance_config(); let source = clock.clone(); cfg.clock = Arc::new(move || source.load(Ordering::SeqCst));
    let c = Controller::open_with_config(path(), cfg).unwrap();
    let first = promote(&c, "expiry-first", false).await;
    let second = promote(&c, "expiry-second", false).await;
    let now = clock.load(Ordering::SeqCst);
    let hold = mutate(&c, "expiry-hold", GovernanceMutation::Pause { scope: ControlScope::AllTasks, reason: "retain this hold".into() }).await;
    let override_request = Operation::MutateGovernance { operation_id: "override-original".into(), mutation: GovernanceMutation::Override {
        expected_revision: second.configuration.revision.clone(), configuration_revision: first.configuration.revision.clone(),
        expires_at_ms: now + 100, reason: "temporary known policy".into(),
    } };
    let selected = governance_reply(&c, override_request.clone()).await;
    let active = state(&c).await;
    assert_eq!(active.selected_revision, first.configuration.revision);
    assert_eq!(active.temporary_override.as_ref().unwrap().prior_revision, second.configuration.revision);
    let early = Operation::MutateGovernance { operation_id: "expiry-early".into(), mutation: GovernanceMutation::ReconcileExpiry {
        expected_operation_id: "override-original".into(),
    } };
    let refused = governance_reply(&c, early.clone()).await;
    assert_eq!(code(refused.outcome.clone()), "override_not_expired");
    assert_eq!(state(&c).await, active);
    clock.store(now + 100, Ordering::SeqCst);
    let expired = mutate(&c, "expiry-original", GovernanceMutation::ReconcileExpiry { expected_operation_id: "override-original".into() }).await;
    assert_eq!(result(&expired), GovernanceResult::State(GovernanceState { selected_revision: second.configuration.revision, temporary_override: None }));
    let historical = governance_reply(&c, early).await;
    assert_eq!(historical.outcome, refused.outcome); assert_eq!(historical.commit, refused.commit);
    let original_override = governance_reply(&c, override_request).await;
    assert_eq!(original_override.outcome, selected.outcome); assert_eq!(original_override.commit, selected.commit);
    assert_eq!(code(mutate(&c, "hold-still-exists", GovernanceMutation::Pause { scope: ControlScope::AllTasks, reason: "cannot replace".into() }).await.outcome), "governance_hold_exists");
    let old_hold = governance_reply(&c, Operation::MutateGovernance { operation_id: "expiry-hold".into(), mutation: GovernanceMutation::Pause {
        scope: ControlScope::AllTasks, reason: "retain this hold".into(),
    } }).await;
    assert_eq!(old_hold.outcome, hold.outcome); assert_eq!(old_hold.commit, hold.commit);
}

#[tokio::test]
async fn absent_historical_governance_metadata_is_not_backfilled_on_reopen() {
    let p = path(); let old = config();
    assert!(old.limits.governance.is_none());
    let raw = serde_json::to_vec(old.limits.as_ref()).unwrap();
    assert!(serde_json::from_slice::<serde_json::Value>(&raw).unwrap().get("governance").is_none());
    let decoded: borg_runtime::limits::RuntimeLimits = serde_json::from_slice(&raw).unwrap();
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), raw);
    let c = Controller::open_with_config(&p, old).unwrap();
    let t = submit(&c, "historical-no-governance", native("historical-agent")).await;
    let accepted = serde_json::to_vec(&t.input).unwrap();
    assert!(t.input.resolved_limits.as_ref().unwrap().global_metadata.governance.is_none());
    drop(c); controller_owner_released(&p).await;
    let mut enabled = governance_config();
    Arc::make_mut(&mut enabled.limits).revision = format!("{}-governance-enabled", decoded.revision);
    enabled.limits.validate().unwrap();
    assert_ne!(enabled.limits.revision, decoded.revision);
    let c = Controller::open_with_config(&p, enabled).unwrap();
    let retained = get(&c, &t.id).await;
    assert_eq!(serde_json::to_vec(&retained.input).unwrap(), accepted);
    assert!(retained.input.resolved_limits.as_ref().unwrap().global_metadata.governance.is_none());
    assert_eq!(state(&c).await.selected_revision, c.limits().revision);
}

#[tokio::test]
async fn normal_promotion_supersedes_active_override_and_late_expiry_cannot_restore_prior_policy() {
    // A controlled response-loss boundary, not a real network outage. The
    // adapter receives the actual canonical reply, destroys it, and delivers
    // only ConnectionReset. The consumer obtains its first usable outcome
    // from GetOperation; it cannot retain or compare against the lost reply.
    async fn commit_then_lose_response(c: &Controller, command: Operation) -> std::io::Result<Reply> {
        assert!(matches!(&command, Operation::MutateGovernance {
            mutation: GovernanceMutation::Promote { .. }, ..
        }));
        let operation_id = command.mutation_id().unwrap().to_owned();
        let reply = governance_reply(c, command).await;
        assert_eq!(reply.commit.as_ref().unwrap().operation_id, operation_id);
        assert!(matches!(&reply.outcome, Outcome::Ok { data: Data::Governance(value) }
            if matches!(value.as_ref(), GovernanceResult::State(_))));
        drop(reply);
        Err(std::io::Error::new(std::io::ErrorKind::ConnectionReset, "controlled loss of committed Promote reply"))
    }
    async fn original(c: &Controller, request: &Operation) -> (Reply, Outcome) {
        let operation_id = request.mutation_id().unwrap();
        let found = governance_reply(c, Operation::GetOperation { operation_id: operation_id.into() }).await;
        let Outcome::Ok { data: Data::Operation(record) } = &found.outcome else {
            panic!("canonical original receipt expected: {found:?}");
        };
        assert_eq!(record.id, operation_id);
        assert_eq!(record.kind, "application_mutation");
        assert_eq!(record.state, "settled");
        assert_eq!(record.intended, serde_json::to_value(request).unwrap());
        let commit = found.commit.as_ref().unwrap();
        assert_eq!(commit.operation_id, operation_id);
        assert!(commit.controller_revision > 0);
        assert!(commit.controller_revision <= found.observed_controller_revision.unwrap());
        let outcome: Outcome = serde_json::from_value(record.outcome.clone().unwrap()).unwrap();
        (found, outcome)
    }
    async fn still_held(c: &Controller, request: &Operation, receipt: &Reply, w: &Worker, task_id: &str, label: &str) {
        let replay = governance_reply(c, request.clone()).await;
        assert_eq!(replay.outcome, receipt.outcome);
        assert_eq!(replay.commit, receipt.commit);
        assert_eq!(code(mutate(c, &format!("{label}-competing-pause"), GovernanceMutation::Pause {
            scope: ControlScope::AllTasks, reason: "must not replace original hold".into(),
        }).await.outcome), "governance_hold_exists");
        assert_eq!(code(mutate(c, &format!("{label}-wrong-release"), GovernanceMutation::ReleaseHold {
            scope: ControlScope::AllTasks, expected_operation_id: "not-the-original-hold".into(),
            reason: "must not release another hold".into(),
        }).await.outcome), "configuration_conflict");
        assert!(commands(c, w, vec![]).await.is_empty(), "normal admission must remain held");
        let queued = get(c, task_id).await;
        assert_eq!(queued.status, TaskStatus::Queued);
        assert!(queued.attempt.is_none());
    }

    let p = path();
    let clock = Arc::new(AtomicU64::new(future() - 300_000));
    let mut cfg = governance_config();
    let source = clock.clone();
    cfg.clock = Arc::new(move || source.load(Ordering::SeqCst));
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    // Each helper performs actual completion/definition shadow evaluations,
    // qualifies the immutable configuration, and retires its real subject.
    // B is selected last, so neither A nor B can be mistaken for final C.
    let a = promote(&c, "supersede-a", false).await;
    let final_c = promote(&c, "supersede-c", false).await;
    let b = promote(&c, "supersede-b", false).await;
    assert_ne!(a.configuration.revision, b.configuration.revision);
    assert_ne!(a.configuration.revision, final_c.configuration.revision);
    assert_ne!(b.configuration.revision, final_c.configuration.revision);
    assert_eq!(state(&c).await.selected_revision, b.configuration.revision);
    let hold_request = Operation::MutateGovernance { operation_id: "supersede-hold".into(), mutation: GovernanceMutation::Pause {
        scope: ControlScope::AllTasks, reason: "hold survives normal promotion and old expiry".into(),
    } };
    let hold_reply = governance_reply(&c, hold_request.clone()).await;
    let GovernanceResult::Hold(hold) = result(&hold_reply) else { panic!("actual hold expected") };
    assert_eq!(hold.operation_id, "supersede-hold");
    assert_eq!(hold.scope, ControlScope::AllTasks);
    let hold_audit = decision(&c, "supersede-hold", DecisionPoint::EmergencyControl).await;
    let queued = submit(&c, "supersede-queued", governance_native("supersede-queued-agent")).await;
    let w = worker("supersede-held-worker", "incarnation-1", 1, 0);
    register(&c, &w).await;
    let expires_at_ms = clock.load(Ordering::SeqCst) + 100;
    let override_request = Operation::MutateGovernance { operation_id: "supersede-original-override".into(), mutation: GovernanceMutation::Override {
        expected_revision: b.configuration.revision.clone(), configuration_revision: a.configuration.revision.clone(),
        expires_at_ms, reason: "temporary qualified A instead of B".into(),
    } };
    let override_reply = governance_reply(&c, override_request.clone()).await;
    let active = state(&c).await;
    assert_eq!(result(&override_reply), GovernanceResult::State(active.clone()));
    assert_eq!(active.selected_revision, a.configuration.revision);
    let temporary = active.temporary_override.as_ref().unwrap();
    assert_eq!(temporary.operation_id, "supersede-original-override");
    assert_eq!(temporary.prior_revision, b.configuration.revision);
    assert_eq!(temporary.replacement_revision, a.configuration.revision);
    assert_eq!(temporary.expires_at_ms, expires_at_ms);
    let override_audit = decision(&c, "supersede-original-override", DecisionPoint::EmergencyControl).await;
    still_held(&c, &hold_request, &hold_reply, &w, &queued.id, "override-active").await;

    let evidence: Vec<ShadowEvidenceRef> = final_c.shadow_operations.iter()
        .map(|id| ShadowEvidenceRef { operation_id: id.clone() }).collect();
    let promotion = Operation::MutateGovernance { operation_id: "supersede-normal-promote".into(), mutation: GovernanceMutation::Promote {
        expected_revision: a.configuration.revision.clone(), configuration_revision: final_c.configuration.revision.clone(),
        evidence: evidence.clone(), supersedes_override: Some("supersede-original-override".into()),
    } };
    let lost = commit_then_lose_response(&c, promotion.clone()).await.unwrap_err();
    assert_eq!(lost.kind(), std::io::ErrorKind::ConnectionReset);
    // This original lookup is the consumer's first usable promotion result.
    // Do not retry Promote or read State before resolving its original ID.
    let (resolved, resolved_outcome) = original(&c, &promotion).await;
    let expected = GovernanceState { selected_revision: final_c.configuration.revision.clone(), temporary_override: None };
    assert_eq!(resolved_outcome, Outcome::Ok { data: Data::Governance(Box::new(GovernanceResult::State(expected.clone()))) });
    assert_eq!(state(&c).await, expected);
    let promoted = decision(&c, "supersede-normal-promote", DecisionPoint::RuleAdministration).await;
    let superseded = decision(&c, "supersede-normal-promote", DecisionPoint::EmergencyControl).await;
    assert_eq!(promoted.detail, DecisionDetail::RuleAdministration(RuleAdministrationDecision::Promoted {
        prior_revision: a.configuration.revision.clone(), selected_revision: final_c.configuration.revision.clone(), evidence,
    }));
    assert_eq!(superseded.detail, DecisionDetail::EmergencyControl(ControlDecision::OverrideSuperseded {
        original_operation_id: "supersede-original-override".into(), selected_revision: final_c.configuration.revision.clone(),
    }));
    for audit in [&promoted, &superseded] {
        assert_eq!(audit.id.operation_id, "supersede-normal-promote");
        assert_eq!(audit.mode, DecisionMode::Enforcing);
        assert_eq!(audit.actual_outcome, ActualDecisionOutcome::Applied);
        assert_eq!(audit.actor, hold.actor);
        assert_eq!(audit.controller_revision, resolved.commit.as_ref().unwrap().controller_revision);
    }
    still_held(&c, &hold_request, &hold_reply, &w, &queued.id, "normal-promoted").await;
    assert_eq!(decision(&c, "supersede-hold", DecisionPoint::EmergencyControl).await, hold_audit);
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg).unwrap();
    assert_eq!(state(&c).await, expected);
    let (after_reopen, reopened_outcome) = original(&c, &promotion).await;
    assert_eq!(after_reopen.outcome, resolved.outcome);
    assert_eq!(after_reopen.commit, resolved.commit);
    assert_eq!(reopened_outcome, resolved_outcome);
    still_held(&c, &hold_request, &hold_reply, &w, &queued.id, "after-reopen").await;

    clock.store(expires_at_ms + 1, Ordering::SeqCst);
    let late_request = Operation::MutateGovernance { operation_id: "supersede-late-expiry".into(), mutation: GovernanceMutation::ReconcileExpiry {
        expected_operation_id: "supersede-original-override".into(),
    } };
    let late = governance_reply(&c, late_request.clone()).await;
    assert_eq!(code(late.outcome.clone()), "not_found");
    assert!(late.commit.is_some());
    assert_eq!(state(&c).await, expected);
    still_held(&c, &hold_request, &hold_reply, &w, &queued.id, "after-late-expiry").await;
    let replay_override = governance_reply(&c, override_request.clone()).await;
    assert_eq!(replay_override.outcome, override_reply.outcome);
    assert_eq!(replay_override.commit, override_reply.commit);
    assert_eq!(state(&c).await, expected, "old override receipt must not reactivate A or B");
    let (original_override, override_outcome) = original(&c, &override_request).await;
    assert_eq!(original_override.commit, override_reply.commit);
    assert_eq!(override_outcome, override_reply.outcome);
    let replay_promote = governance_reply(&c, promotion.clone()).await;
    assert_eq!(replay_promote.outcome, resolved_outcome);
    assert_eq!(replay_promote.commit, resolved.commit);
    let (retained_promotion, _) = original(&c, &promotion).await;
    assert_eq!(retained_promotion.outcome, resolved.outcome);
    assert_eq!(retained_promotion.commit, resolved.commit);
    let late_replay = governance_reply(&c, late_request).await;
    assert_eq!(late_replay.outcome, late.outcome);
    assert_eq!(late_replay.commit, late.commit);
    assert_eq!(decision(&c, "supersede-original-override", DecisionPoint::EmergencyControl).await, override_audit);
    assert_eq!(decision(&c, "supersede-normal-promote", DecisionPoint::RuleAdministration).await, promoted);
    assert_eq!(decision(&c, "supersede-normal-promote", DecisionPoint::EmergencyControl).await, superseded);
    assert_eq!(decision(&c, "supersede-hold", DecisionPoint::EmergencyControl).await, hold_audit);
    assert_eq!(state(&c).await, expected);
    still_held(&c, &hold_request, &hold_reply, &w, &queued.id, "after-original-replays").await;

    let released = mutate(&c, "supersede-release-original-hold", GovernanceMutation::ReleaseHold {
        scope: ControlScope::AllTasks, expected_operation_id: hold.operation_id.clone(), reason: "explicit recovery after reconciliation".into(),
    }).await;
    assert_eq!(result(&released), GovernanceResult::Released(ControlScope::AllTasks));
    let before_admission = get(&c, &queued.id).await;
    assert!(before_admission.attempt.is_none(), "ReleaseHold itself must not dispatch work");
    assert_eq!(before_admission.status, TaskStatus::Queued);
    let admitted = run(&commands(&c, &w, vec![]).await);
    assert_eq!(admitted.task_id, queued.id);
    assert_eq!(admitted.worker_id, w.id);
    assert_eq!(state(&c).await, expected);
    retire_subject(&c, &admitted, "supersede-final").await;
    drop(c);
    controller_owner_released(&p).await;
}

#[tokio::test]
async fn promotion_refuses_stale_selected_binding_with_genuine_evidence_and_preserves_original_refusal() {
    async fn original(c: &Controller, request: &Operation) -> (Reply, Outcome) {
        let operation_id = request.mutation_id().unwrap();
        let found = governance_reply(c, Operation::GetOperation { operation_id: operation_id.into() }).await;
        let Outcome::Ok { data: Data::Operation(record) } = &found.outcome else {
            panic!("canonical original mutation expected: {found:?}");
        };
        assert_eq!(record.id, operation_id);
        assert_eq!(record.kind, "application_mutation");
        assert_eq!(record.state, "settled");
        assert_eq!(record.intended, serde_json::to_value(request).unwrap());
        let commit = found.commit.as_ref().unwrap();
        assert_eq!(commit.operation_id, operation_id);
        assert!(commit.controller_revision > 0);
        assert!(commit.controller_revision <= found.observed_controller_revision.unwrap());
        let outcome = serde_json::from_value(record.outcome.clone().unwrap()).unwrap();
        (found, outcome)
    }
    async fn no_success_audit(c: &Controller, operation_id: &str) {
        for point in [DecisionPoint::RuleAdministration, DecisionPoint::EmergencyControl] {
            let absent = governance_reply(c, Operation::QueryGovernance { query: GovernanceQuery::Decision {
                decision: DecisionRef { operation_id: operation_id.into(), point },
            } }).await;
            assert_eq!(code(absent.outcome), "not_found");
            assert!(absent.commit.is_none());
        }
    }

    let p = path();
    let cfg = governance_config();
    let mut c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let target = promote(&c, "stale-selected-c", false).await;
    let earlier_selection = state(&c).await;
    assert_eq!(earlier_selection.selected_revision, target.configuration.revision);
    assert!(earlier_selection.temporary_override.is_none());
    let qualification_audit = decision(&c, target.request.mutation_id().unwrap(), DecisionPoint::RuleAdministration).await;
    assert_eq!(qualification_audit.actual_outcome, ActualDecisionOutcome::Applied);
    assert_eq!(qualification_audit.controller_revision, target.reply.commit.as_ref().unwrap().controller_revision);
    let mut shadows = Vec::new();
    for id in &target.shadow_operations {
        let lookup = governance_reply(&c, Operation::GetOperation { operation_id: id.clone() }).await;
        let audit = decision(&c, id, DecisionPoint::RuleAdministration).await;
        let DecisionDetail::RuleAdministration(RuleAdministrationDecision::Shadow { result: shadow }) = &audit.detail else {
            panic!("actual passing shadow required");
        };
        assert_eq!(shadow.configuration_revision, target.configuration.revision);
        assert!(matches!(shadow.evaluation, ShadowEvaluation::Evaluated { passing: true, .. }));
        assert_eq!(audit.actual_outcome, ActualDecisionOutcome::Applied);
        assert_eq!(audit.controller_revision, lookup.commit.as_ref().unwrap().controller_revision);
        shadows.push((id.clone(), lookup, audit));
    }
    assert_eq!(shadows.len(), 2);
    // This is a real intervening governed selection, not an aged, altered or
    // wrong-subject evidence record. Both qualified targets share the same
    // unchanged global metadata and change-policy binding.
    let other = promote(&c, "stale-selected-b", false).await;
    let current = state(&c).await;
    assert_eq!(current.selected_revision, other.configuration.revision);
    assert_ne!(current.selected_revision, earlier_selection.selected_revision);
    assert!(current.temporary_override.is_none());
    assert_eq!(target.configuration.global_revision, other.configuration.global_revision);
    assert_eq!(target.configuration.definition_update_policy_revision, other.configuration.definition_update_policy_revision);
    let evidence: Vec<ShadowEvidenceRef> = target.shadow_operations.iter()
        .map(|id| ShadowEvidenceRef { operation_id: id.clone() }).collect();
    let stale = Operation::MutateGovernance { operation_id: "stale-selected-original".into(), mutation: GovernanceMutation::Promote {
        expected_revision: earlier_selection.selected_revision.clone(), configuration_revision: target.configuration.revision.clone(),
        evidence: evidence.clone(), supersedes_override: None,
    } };
    let refused = governance_reply(&c, stale.clone()).await;
    assert_eq!(refused.outcome, Outcome::Refused {
        code: "configuration_conflict".into(), message: "active governance revision changed".into(),
    });
    assert!(refused.commit.is_some());
    let (first_lookup, first_outcome) = original(&c, &stale).await;
    assert_eq!(first_outcome, refused.outcome);
    assert_eq!(first_lookup.commit, refused.commit);

    for reopen in [false, true] {
        if reopen {
            drop(c);
            controller_owner_released(&p).await;
            c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        }
        assert_eq!(state(&c).await, current);
        assert_eq!(configuration(&c, &target.configuration.revision).await, target.configuration);
        let (qualification, qualification_outcome) = original(&c, &target.request).await;
        assert_eq!(qualification_outcome, target.reply.outcome);
        assert_eq!(qualification.commit, target.reply.commit);
        assert_eq!(decision(&c, target.request.mutation_id().unwrap(), DecisionPoint::RuleAdministration).await, qualification_audit);
        for (id, prior, audit) in &shadows {
            let found = governance_reply(&c, Operation::GetOperation { operation_id: id.clone() }).await;
            assert_eq!(found.outcome, prior.outcome);
            assert_eq!(found.commit, prior.commit);
            assert_eq!(decision(&c, id, DecisionPoint::RuleAdministration).await, *audit);
        }
        let (found, found_outcome) = original(&c, &stale).await;
        assert_eq!(found.outcome, first_lookup.outcome);
        assert_eq!(found.commit, first_lookup.commit);
        assert_eq!(found_outcome, refused.outcome);
        let replay = governance_reply(&c, stale.clone()).await;
        assert_eq!(replay.outcome, refused.outcome);
        assert_eq!(replay.commit, refused.commit);
        no_success_audit(&c, "stale-selected-original").await;
        assert_eq!(state(&c).await, current, "refusal and original replay must not select C");
    }

    // A distinct operation with the current B binding accepts the same exact
    // C evidence. This rules out an evidence-age or wrong-candidate oracle.
    let fresh = mutate(&c, "stale-selected-fresh-binding", GovernanceMutation::Promote {
        expected_revision: current.selected_revision.clone(), configuration_revision: target.configuration.revision.clone(),
        evidence: evidence.clone(), supersedes_override: None,
    }).await;
    assert_eq!(result(&fresh), GovernanceResult::State(earlier_selection.clone()));
    let fresh_audit = decision(&c, "stale-selected-fresh-binding", DecisionPoint::RuleAdministration).await;
    assert_eq!(fresh_audit.detail, DecisionDetail::RuleAdministration(RuleAdministrationDecision::Promoted {
        prior_revision: current.selected_revision, selected_revision: target.configuration.revision, evidence,
    }));
    assert_eq!(fresh_audit.actual_outcome, ActualDecisionOutcome::Applied);
    assert_eq!(fresh_audit.controller_revision, fresh.commit.as_ref().unwrap().controller_revision);
    let replay = governance_reply(&c, stale.clone()).await;
    assert_eq!(replay.outcome, refused.outcome);
    assert_eq!(replay.commit, refused.commit);
    let (found, outcome) = original(&c, &stale).await;
    assert_eq!(found.outcome, first_lookup.outcome);
    assert_eq!(found.commit, first_lookup.commit);
    assert_eq!(outcome, refused.outcome);
    no_success_audit(&c, "stale-selected-original").await;
    assert_eq!(state(&c).await, earlier_selection);
    drop(c);
    controller_owner_released(&p).await;
}
