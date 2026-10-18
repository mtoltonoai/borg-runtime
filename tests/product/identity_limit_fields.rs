//! Real in-process product routes and canonical SQLite receipts. No DOM,
//! provider or actual executor claim; the accepted configured child is retained.
use super::*;
use borg_runtime::limits::{DefinitionLimitField, DefinitionUpdatePolicy};
use std::sync::Arc;
#[path = "../identity_support.rs"]
mod closure;

const FIELDS: [(&str, DefinitionLimitField); 2] = [
    ("coding.frame_bytes", DefinitionLimitField::CodingFrameBytes),
    ("collaboration.journal_bytes", DefinitionLimitField::CollaborationJournalBytes),
];

fn selected_config(fields: Vec<DefinitionLimitField>) -> ControllerConfig {
    let mut cfg = config();
    let p = Arc::make_mut(&mut cfg.limits);
    p.revision = "identity-limit-fields-product-global".into();
    p.definition_updates = Some(DefinitionUpdatePolicy {
        revision: "identity-limit-fields-product-policy".into(), allowed_fields: fields,
    });
    p.validate().unwrap();
    cfg
}

async fn fixture(cfg: ControllerConfig, label: &str) -> (Fixture, Task) {
    let root = std::env::temp_dir().join(format!("borg-identity-limit-fields-ui-{}-{}",
        std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("tasks.sqlite");
    let controller = Controller::open_with_config(&path, cfg).unwrap();
    register_fixture_repository(&controller).await;
    let deadline = now() + 3_600_000;
    let child = serde_json::to_string(&workflow(deadline)).unwrap();
    let response = ui(&controller, "POST", "/tasks",
        native_submission(label, deadline, Some(&child))).await;
    let accepted = task(&controller, task_id(&response)).await;
    assert!(matches!(accepted.input.native_workflow.as_deref(), Some(NativeWorkflow::CodingThenContinue { .. })));
    (Fixture { controller, path }, accepted)
}

fn route(id: &str) -> String { format!("/agents/{}/settings", encode(id)) }

fn body(c: &Controller, id: &str, d: &Definition, edits: &[(&str, String)]) -> Vec<u8> {
    let mut fields = vec![
        ("operation_id", id.into()), ("expected_revision", d.revision.clone()),
        ("expected_global_revision", c.limits().revision.clone()),
        ("expected_policy_revision", c.limits().definition_updates.as_ref().unwrap().revision.clone()),
    ];
    fields.extend(edits.iter().cloned());
    form(&fields)
}

async fn selected(c: &Controller, id: &str) -> Definition {
    let Data::Agent(agent) = call(c, Operation::GetAgent { agent_id: id.into() }).await else { panic!("selected definition") };
    agent.definition
}

async fn receipt(c: &Controller, id: &str) -> (OperationRecord, DefinitionUpdateOutcome) {
    let Data::Operation(record) = call(c, Operation::GetOperation { operation_id: id.into() }).await else { panic!("original operation") };
    let outcome: Outcome = serde_json::from_value(record.outcome.clone().unwrap()).unwrap();
    let Outcome::Ok { data: Data::DefinitionUpdate(value) } = outcome else { panic!("original update outcome") };
    (record, *value)
}

#[tokio::test]
async fn identity_limit_fields_product_bytes_apply_replay_and_preserve_parent_child_snapshots() {
    let cfg = selected_config(FIELDS.iter().map(|(_, f)| *f).collect());
    let (f, accepted) = fixture(cfg.clone(), "identity-limit-fields-ui-seed").await;
    let input_bytes = serde_json::to_vec(&accepted.input).unwrap();
    let child_bytes = serde_json::to_vec(&accepted.input.native_workflow).unwrap();
    let global_bytes = serde_json::to_vec(f.controller.limits()).unwrap();
    let mut before = selected(&f.controller, &accepted.input.definition.id).await;
    let original_effective = f.controller.limits().resolve_definition(&before).unwrap();
    let mut originals = Vec::new();
    for (step, delta) in [1isize, -2].into_iter().enumerate() {
        let prior = f.controller.limits().resolve_definition(&before).unwrap();
        let page = ui(&f.controller, "GET", &route(&before.id), vec![]).await;
        assert_eq!(page.status, 200, "{}", page.body);
        for (name, field) in FIELDS {
            assert!(page.body.contains(&format!("{name} <small>Current effective value: {} · bytes</small>", field.value(&prior).unwrap())), "{}", page.body);
            assert!(page.body.contains(&format!("name=\"{name}\"")));
        }
        let mut expected = prior.clone();
        let edits: Vec<_> = FIELDS.iter().map(|(name, field)| {
            let value = field.value(&prior).unwrap().checked_add_signed(delta).unwrap();
            field.set(&mut expected, value);
            (*name, value.to_string())
        }).collect();
        let id = format!("identity-limit-fields-product-{step}");
        let submitted = body(&f.controller, &id, &before, &edits);
        let response = ui(&f.controller, "POST", &route(&before.id), submitted.clone()).await;
        assert!(matches!(response.status, 200 | 303), "{}", response.body);
        let (record, outcome) = receipt(&f.controller, &id).await;
        let intended: Operation = serde_json::from_value(record.intended.clone()).unwrap();
        let Operation::UpdateDefinition { operation_id, update } = intended else { panic!("canonical operator adapter") };
        assert_eq!(operation_id, id);
        assert_eq!(update.expected_revision, before.revision);
        assert_eq!(update.changes.len(), 2);
        for (_, field) in FIELDS { assert_eq!(update.changes[&field], field.value(&expected).unwrap()); }
        assert_eq!(outcome.classification, if delta > 0 { ConfigurationChangeClass::Widening } else { ConfigurationChangeClass::Narrowing });
        assert_eq!(outcome.effective_for, ConfigurationEffect::FutureSubmissions);
        assert_eq!(outcome.selected_definition.executor, before.executor);
        assert_eq!(outcome.selected_definition.inference, before.inference);
        assert_eq!(outcome.selected_definition.execution_profile, before.execution_profile);
        assert_eq!(f.controller.limits().resolve_definition(&outcome.selected_definition).unwrap(), expected);
        let overrides = outcome.selected_definition.limit_overrides.as_ref().unwrap();
        assert_eq!(overrides.coding.as_ref(), Some(&expected.coding));
        assert_eq!(overrides.collaboration.as_ref(), Some(&expected.collaboration));
        assert_eq!(expected.coding.rate_limit_retry, original_effective.coding.rate_limit_retry);
        assert_eq!(expected.coding.model_context_window, original_effective.coding.model_context_window);
        assert_eq!(expected.collaboration.capabilities, original_effective.collaboration.capabilities);
        let current = task(&f.controller, &accepted.id).await;
        assert_eq!(serde_json::to_vec(&current.input).unwrap(), input_bytes);
        assert_eq!(serde_json::to_vec(&current.input.native_workflow).unwrap(), child_bytes);
        assert_eq!(serde_json::to_vec(f.controller.limits()).unwrap(), global_bytes);
        originals.push((id, before.clone(), submitted, record));
        before = outcome.selected_definition;
    }
    let Fixture { controller, path } = f;
    drop(controller);
    closure::owner_closed(&path).await;
    let c = Controller::open_with_config(&path, cfg).unwrap();
    for (id, old, submitted, original) in originals {
        let response = ui(&c, "POST", &route(&old.id), submitted).await;
        assert!(matches!(response.status, 200 | 303), "{}", response.body);
        assert_eq!(receipt(&c, &id).await.0, original);
        assert_eq!(selected(&c, &old.id).await, before);
        let page = ui(&c, "GET", &format!("/operations/{}", encode(&id)), vec![]).await;
        assert_eq!(page.status, 200);
        assert!(page.body.contains(&id));
    }
    assert_eq!(serde_json::to_vec(&task(&c, &accepted.id).await.input).unwrap(), input_bytes);
    drop(c); closure::owner_closed(&path).await;
}

#[tokio::test]
async fn identity_limit_fields_product_policy_and_stale_or_invalid_input_never_widen_authority() {
    let (f, accepted) = fixture(selected_config(vec![FIELDS[0].1]), "identity-limit-fields-ui-refusal").await;
    let before = accepted.input.definition.clone();
    let values = f.controller.limits().resolve_definition(&before).unwrap();
    let page = ui(&f.controller, "GET", &route(&before.id), vec![]).await;
    assert_eq!(page.status, 200);
    assert!(page.body.contains("name=\"coding.frame_bytes\""));
    assert!(!page.body.contains("name=\"collaboration.journal_bytes\""));
    let cases = [
        ("unselected", "collaboration.journal_bytes", (values.collaboration.journal_bytes + 1).to_string(), 409, "configuration_policy"),
        ("zero", "coding.frame_bytes", "0".into(), 409, "invalid_input"),
        ("fraction", "coding.frame_bytes", "1.5".into(), 400, "must be an integer"),
        ("unknown", "coding.unknown", "1".into(), 400, "Unknown definition-limit field"),
        ("grant", "host_admin", "true".into(), 400, "Unknown definition-limit field"),
    ];
    for (label, name, value, status, reason) in cases {
        let id = format!("identity-limit-fields-ui-{label}");
        let response = ui(&f.controller, "POST", &route(&before.id), body(&f.controller, &id, &before, &[(name, value)])).await;
        assert_eq!(response.status, status, "{label}: {}", response.body);
        assert!(response.body.contains(reason), "{label}: {}", response.body);
        assert_eq!(selected(&f.controller, &before.id).await, before);
    }
    for stale in ["expected_revision", "expected_global_revision", "expected_policy_revision"] {
        let id = format!("identity-limit-fields-ui-{stale}");
        let fields = vec![
            ("operation_id", id),
            ("expected_revision", if stale == "expected_revision" { "obsolete".into() } else { before.revision.clone() }),
            ("expected_global_revision", if stale == "expected_global_revision" { "obsolete".into() } else { f.controller.limits().revision.clone() }),
            ("expected_policy_revision", if stale == "expected_policy_revision" { "obsolete".into() } else { f.controller.limits().definition_updates.as_ref().unwrap().revision.clone() }),
            ("coding.frame_bytes", (values.coding.frame_bytes + 1).to_string()),
        ];
        let response = ui(&f.controller, "POST", &route(&before.id), form(&fields)).await;
        assert_eq!(response.status, 409, "{}", response.body);
        assert!(response.body.contains("configuration_conflict"));
        assert_eq!(selected(&f.controller, &before.id).await, before);
    }
    assert_eq!(task(&f.controller, &accepted.id).await.input, accepted.input);
    let Fixture { controller, path } = f;
    drop(controller); closure::owner_closed(&path).await;
}
