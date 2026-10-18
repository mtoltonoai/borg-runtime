//! Actual HTTP transport and original receipt boundaries; no provider or worker.
use super::*;
use crate::controller::ControllerConfig;
use crate::governance::*;
use crate::collaboration::workflow::{ActorProvenance, PrincipalRef, TrustedHostPath};
use std::sync::Arc;

fn config() -> ControllerConfig {
    let mut cfg = ControllerConfig::default();
    Arc::make_mut(&mut cfg.limits).governance = Some(crate::limits::GovernanceLimits {
        record_bytes: 65_536, reason_items: 16, reason_bytes: 4096, scan_rows: 512,
        rule_versions: 16, configuration_versions: 16, shadow_evidence_items: 16,
        completion_continuations: 2, override_max_ms: 10_000,
    });
    cfg.limits.validate().unwrap();
    cfg
}
fn request(command: Operation) -> Request {
    Request { protocol_revision: WIRE_REVISION, request_id: identity(), command }
}
fn pause(id: &str) -> Operation {
    Operation::MutateGovernance { operation_id: id.into(), mutation: GovernanceMutation::Pause {
        scope: ControlScope::AllTasks, reason: "bounded operator maintenance".into(),
    } }
}
fn refused(reply: &Reply, expected: &str) {
    assert!(matches!(&reply.outcome, Outcome::Refused { code, .. } if code == expected), "{reply:?}");
}
fn value(reply: &Reply) -> &GovernanceResult {
    let Outcome::Ok { data: Data::Governance(value) } = &reply.outcome else { panic!("{reply:?}") };
    value
}
struct Fixture {
    controller: Controller, origin: String, client: reqwest::Client,
    server: tokio::task::JoinHandle<Result<(), String>>,
}
impl Fixture {
    async fn new() -> Self { Self::open(false).await }
    async fn broken_ordinary_policy() -> Self { Self::open(true).await }
    async fn open(broken: bool) -> Self {
        let dir = std::env::temp_dir().join(format!("borg-governance-product-{}", identity()));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("tasks.sqlite");
        let injection = broken.then(|| crate::controller::governance_ui_test_fixture::missing_selection(&path));
        let controller = Controller::open_with_config(&path, config()).unwrap();
        if let Some(injection) = injection { injection.assert_applied(); }
        let listener = crate::server::listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(crate::server::serve(controller.clone(), listener, origin.clone()));
        Self { controller, origin, client: reqwest::Client::builder().no_proxy().build().unwrap(), server }
    }
    async fn api(&self, command: Operation) -> Reply {
        let req = request(command);
        let response = self.client.post(format!("{}/api/operations", self.origin))
            .header("Origin", &self.origin).header("Content-Type", "application/json")
            .body(serde_json::to_vec(&req).unwrap()).send().await.unwrap();
        assert_eq!(response.status(), 200);
        let reply: Reply = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
        assert_eq!(reply.request_id, req.request_id);
        reply.validate_for(&req.command).unwrap();
        reply
    }
    async fn decision(&self, id: &str) -> DecisionRecord {
        let r = self.api(Operation::QueryGovernance { query: GovernanceQuery::Decision { decision: DecisionRef {
            operation_id: id.into(), point: DecisionPoint::EmergencyControl,
        } } }).await;
        let GovernanceResult::Decision(d) = value(&r) else { panic!("decision missing") };
        d.clone()
    }
    async fn close(self) { self.server.abort(); let _ = self.server.await; }
}

#[tokio::test]
async fn governance_page_has_no_browser_authority_and_http_boundary_records_actual_operator() {
    let f = Fixture::new().await;
    let html = f.client.get(format!("{}/governance", f.origin)).send().await.unwrap();
    assert_eq!(html.status(), 200);
    let html = html.text().await.unwrap();
    assert!(html.contains("/ui/governance.js"));
    assert!(html.contains("data-live-refresh=\"false\""));
    assert_eq!(html.matches("class=\"governance-control\"").count(), 4);
    assert!(!html.contains("protected_release") && !html.contains("grant_role"));
    let browser = Box::pin(f.controller.handle_browser(request(pause("browser-denied")))).await;
    refused(&browser, "governance_operator_required");
    let cross = f.client.post(format!("{}/api/operations", f.origin))
        .header("Origin", "http://untrusted.invalid").body(serde_json::to_vec(&request(pause("cross-denied"))).unwrap())
        .send().await.unwrap();
    assert_eq!(cross.status(), 403);
    refused(&f.api(Operation::GetOperation { operation_id: "cross-denied".into() }).await, "not_found");
    let original = f.api(pause("http-operator")).await;
    let GovernanceResult::Hold(hold) = value(&original) else { panic!("hold missing") };
    assert_eq!(hold.scope, ControlScope::AllTasks);
    assert_eq!(hold.actor.principal, PrincipalRef::TrustedHostOperator);
    assert_eq!(hold.actor.provenance, ActorProvenance::TrustedHost { path: TrustedHostPath::OperatorApi });
    let decision = f.decision("http-operator").await;
    assert_eq!(decision.actor, hold.actor);
    assert_eq!(decision.actual_outcome, ActualDecisionOutcome::Applied);
    assert_eq!(decision.rule, RuleRef { id: "emergency-control".into(), revision: "1".into() });
    let replay = f.api(pause("http-operator")).await;
    assert_eq!((replay.outcome, replay.commit), (original.outcome, original.commit));
    f.close().await;
}

#[tokio::test]
async fn governance_same_origin_keeps_closed_schema_and_original_refusal_receipt() {
    let f = Fixture::new().await;
    let mut raw = serde_json::to_value(request(pause("forged-person"))).unwrap();
    raw["input"]["mutation"]["actor"] = serde_json::json!({"kind":"trusted_host_operator"});
    let response = f.client.post(format!("{}/api/operations", f.origin)).header("Origin", &f.origin)
        .body(serde_json::to_vec(&raw).unwrap()).send().await.unwrap();
    assert_eq!(response.status(), 400);
    refused(&f.api(Operation::GetOperation { operation_id: "forged-person".into() }).await, "not_found");
    let held = f.api(pause("first-hold")).await;
    assert!(matches!(value(&held), GovernanceResult::Hold(_)));
    let stale = Operation::MutateGovernance { operation_id: "stale-release".into(), mutation: GovernanceMutation::ReleaseHold {
        scope: ControlScope::AllTasks, expected_operation_id: "not-first-hold".into(), reason: "wrong original".into(),
    } };
    let original = f.api(stale.clone()).await;
    refused(&original, "configuration_conflict");
    let replay = f.api(stale).await;
    assert_eq!((&replay.outcome, &replay.commit), (&original.outcome, &original.commit));
    let lookup = f.api(Operation::GetOperation { operation_id: "stale-release".into() }).await;
    let Outcome::Ok { data: Data::Operation(record) } = lookup.outcome else { panic!("lookup missing") };
    assert_eq!(record.id, "stale-release"); assert_eq!(record.state, "settled");
    assert_eq!(record.outcome, Some(serde_json::to_value(&original.outcome).unwrap()));
    assert_eq!(lookup.commit, original.commit);
    let release = f.api(Operation::MutateGovernance { operation_id: "exact-release".into(), mutation: GovernanceMutation::ReleaseHold {
        scope: ControlScope::AllTasks, expected_operation_id: "first-hold".into(), reason: "exact original".into(),
    } }).await;
    assert_eq!(value(&release), &GovernanceResult::Released(ControlScope::AllTasks));
    f.close().await;
}

#[tokio::test]
async fn governance_emergency_http_controls_work_when_selected_ordinary_policy_is_broken() {
    // The cfg(test)-only one-shot fixture uses this isolated owner's actual
    // connection after real startup validation. EXCLUSIVE ownership is retained;
    // no second SQLite writer, admission API, or valid restart is claimed.
    let f = Fixture::broken_ordinary_policy().await;
    let failed = f.api(Operation::QueryGovernance { query: GovernanceQuery::Configuration { revision: "missing-ordinary-policy".into() } }).await;
    refused(&failed, "not_found");
    let original = f.api(pause("emergency-broken-policy")).await;
    assert!(matches!(value(&original), GovernanceResult::Hold(_)));
    let audit = f.decision("emergency-broken-policy").await;
    assert_eq!(audit.actor.provenance, ActorProvenance::TrustedHost { path: TrustedHostPath::OperatorApi });
    assert_eq!(audit.actual_outcome, ActualDecisionOutcome::Applied);
    let released = f.api(Operation::MutateGovernance { operation_id: "release-broken-policy".into(), mutation: GovernanceMutation::ReleaseHold {
        scope: ControlScope::AllTasks, expected_operation_id: "emergency-broken-policy".into(), reason: "remove exact hold only".into(),
    } }).await;
    assert_eq!(value(&released), &GovernanceResult::Released(ControlScope::AllTasks));
    let state = f.api(Operation::QueryGovernance { query: GovernanceQuery::State }).await;
    let GovernanceResult::State(state) = value(&state) else { panic!("state missing") };
    assert_eq!(state.selected_revision, "missing-ordinary-policy", "emergency access must not repair or select ordinary policy");
    f.close().await;
}
