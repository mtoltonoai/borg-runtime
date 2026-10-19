//! Product tests for the operator's definition-limit form. Include as a child
//! module of tests/product.rs. All effects and receipts come from Controller;
//! mutations use the real local operator HTTP route; there is no model, worker
//! caller, external service or fabricated commit proof. Client JavaScript is
//! qualified separately by definition_settings_operator.cjs.
use super::*;
use borg_runtime::limits::{DefinitionLimitField, DefinitionUpdatePolicy};
use std::sync::Arc;

const SETTINGS_FIELDS: [(&str, DefinitionLimitField); 8] = [
    ("native.context_bytes", DefinitionLimitField::NativeContextBytes),
    ("native.output_bytes", DefinitionLimitField::NativeOutputBytes),
    ("native.result_bytes", DefinitionLimitField::NativeResultBytes),
    ("native.max_output_tokens", DefinitionLimitField::NativeMaxOutputTokens),
    ("coding.command_output_bytes", DefinitionLimitField::CodingCommandOutputBytes),
    ("coding.summary_chars", DefinitionLimitField::CodingSummaryChars),
    ("coding.summary_bytes", DefinitionLimitField::CodingSummaryBytes),
    ("collaboration.complete_result_bytes", DefinitionLimitField::CollaborationCompleteResultBytes),
];

fn settings_config() -> ControllerConfig {
    let mut cfg = config();
    let limits = Arc::make_mut(&mut cfg.limits);
    limits.revision = "product-settings-global-1".into();
    limits.definition_updates = Some(DefinitionUpdatePolicy {
        revision: "product-settings-policy-1".into(),
        allowed_fields: SETTINGS_FIELDS.iter().map(|(_, field)| *field).collect(),
    });
    limits.validate().unwrap();
    cfg
}

async fn settings_fixture(cfg: ControllerConfig) -> Fixture {
    let root = std::env::temp_dir().join(format!(
        "borg-product-settings-{}-{}-{}",
        std::process::id(), now(), NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("tasks.sqlite");
    let controller = Controller::open_with_config(&path, cfg).unwrap();
    register_fixture_repository(&controller).await;
    Fixture { controller, path }
}

fn settings_replace(body: Vec<u8>, key: &str, value: Option<&str>) -> Vec<u8> {
    let key = encode(key);
    let text = String::from_utf8(body).unwrap();
    let mut fields = text.split('&')
        .filter(|field| field.split_once('=').is_none_or(|(name, _)| name != key))
        .map(str::to_owned).collect::<Vec<_>>();
    if let Some(value) = value {
        fields.push(format!("{key}={}", encode(value)));
    }
    fields.join("&").into_bytes()
}

async fn settings_seed(controller: &Controller, operation: &str) -> Task {
    let deadline = now() + 3_600_000;
    let configured_child = serde_json::to_string(&workflow(deadline)).unwrap();
    let body = settings_replace(
        native_submission(operation, deadline, Some(&configured_child)),
        "web_search_max_tool_calls", Some("1"),
    );
    let response = ui(controller, "POST", "/tasks", body).await;
    task(controller, task_id(&response)).await
}

async fn settings_selected(controller: &Controller, agent_id: &str) -> Definition {
    match call(controller, Operation::GetAgent { agent_id: agent_id.into() }).await {
        Data::Agent(agent) => agent.definition,
        other => panic!("expected selected agent definition, got {other:?}"),
    }
}

fn settings_update(controller: &Controller, definition: &Definition, value: usize) -> DefinitionUpdate {
    DefinitionUpdate {
        agent_id: definition.id.clone(),
        expected_revision: definition.revision.clone(),
        expected_global_revision: controller.limits().revision.clone(),
        expected_policy_revision: controller.limits().definition_updates.as_ref()
            .map(|policy| policy.revision.clone()).unwrap_or_else(|| "product-settings-policy-1".into()),
        changes: [(DefinitionLimitField::NativeContextBytes, value)].into_iter().collect(),
    }
}

fn settings_body(operation_id: &str, update: &DefinitionUpdate) -> Vec<u8> {
    let mut fields = vec![
        ("operation_id", operation_id.into()),
        ("expected_revision", update.expected_revision.clone()),
        ("expected_global_revision", update.expected_global_revision.clone()),
        ("expected_policy_revision", update.expected_policy_revision.clone()),
    ];
    // A browser submits the direct numeric fields; omitted changes are empty.
    for (name, field) in SETTINGS_FIELDS {
        fields.push((name, update.changes.get(&field).map(usize::to_string).unwrap_or_default()));
    }
    form(&fields)
}

fn settings_route(agent_id: &str) -> String {
    format!("/agents/{}/settings", encode(agent_id))
}

fn settings_submission_input(task: &Task, definition: Definition) -> Box<TaskInput> {
    let mut input = task.input.clone();
    input.resolved_limits = None;
    input.definition = definition;
    if let Some(NativeWorkflow::CodingThenContinue { child }) = input.native_workflow.as_deref_mut() {
        // External task input carries exact registration references; the owner
        // resolves its endpoints again instead of accepting server-owned data.
        child.repository.url.clear();
        child.repository.checkpoint_remote.clear();
    }
    input
}

// Same actual HTTP boundary as governance controls. This helper does not
// manufacture Browser authority or mock the controller/transport.
pub(super) async fn operator_reply(controller: &Controller, operation_id: &str, update: &DefinitionUpdate) -> Reply {
    let listener = borg_runtime::server::listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(borg_runtime::server::serve(controller.clone(), listener, origin.clone()));
    let request = Request { protocol_revision: WIRE_REVISION,
        request_id: format!("settings-operator-request-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
        command: Operation::UpdateDefinition { operation_id: operation_id.into(), update: update.clone() } };
    let response = reqwest::Client::builder().no_proxy().build().unwrap()
        .post(format!("{origin}/api/operations")).header("Origin", &origin)
        .header("Content-Type", "application/json").body(serde_json::to_vec(&request).unwrap())
        .send().await.unwrap();
    assert_eq!(response.status(), 200);
    let reply: Reply = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(reply.request_id, request.request_id);
    reply.validate_for(&request.command).unwrap();
    server.abort(); let _ = server.await;
    reply
}
pub(super) async fn operator_post(controller: &Controller, operation_id: &str, update: &DefinitionUpdate) -> UiResponse {
    let reply = operator_reply(controller, operation_id, update).await;
    assert!(reply.commit.is_some(), "no committed receipt: inspect original API reply without presenting a different stored outcome: {reply:?}");
    ui(controller, "GET", &format!("/operations/{}", encode(operation_id)), vec![]).await
}
async fn settings_post(controller: &Controller, operation_id: &str, update: &DefinitionUpdate) -> UiResponse {
    operator_post(controller, operation_id, update).await
}
async fn settings_browser_post(controller: &Controller, operation_id: &str, update: &DefinitionUpdate) -> UiResponse {
    ui(controller, "POST", &settings_route(&update.agent_id), settings_body(operation_id, update)).await
}

async fn settings_receipt_page(controller: &Controller, response: UiResponse) -> UiResponse {
    if response.status == 303 {
        let location = response.location.as_deref().expect("successful redirect has a location");
        assert!(location.starts_with("/operations/"), "{location}");
        let page = ui(controller, "GET", location, vec![]).await;
        assert_eq!(page.status, 200, "{}", page.body);
        page
    } else {
        assert_eq!(response.status, 200, "{}", response.body);
        response
    }
}

async fn settings_replay(controller: &Controller, operation_id: &str, update: &DefinitionUpdate) -> Reply {
    controller.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: format!("settings-read-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
        command: Operation::UpdateDefinition { operation_id: operation_id.into(), update: update.clone() },
    }).await
}

fn settings_outcome(reply: &Reply) -> &DefinitionUpdateOutcome {
    assert!(reply.commit.is_some(), "expected actual committed receipt: {reply:?}");
    match &reply.outcome {
        Outcome::Ok { data: Data::DefinitionUpdate(value) } => value,
        other => panic!("expected committed definition update, got {other:?}"),
    }
}

fn settings_assert_receipt(page: &UiResponse, operation_id: &str, outcome: &DefinitionUpdateOutcome) {
    assert_eq!(page.status, 200, "{}", page.body);
    for value in [operation_id, &outcome.previous_revision, &outcome.selected_definition.revision,
        &outcome.global_revision, &outcome.policy.revision] {
        assert!(page.body.contains(value), "missing {value}: {}", page.body);
    }
    let body = page.body.to_ascii_lowercase();
    let class = match outcome.classification {
        ConfigurationChangeClass::Neutral => "neutral",
        ConfigurationChangeClass::Narrowing => "narrowing",
        ConfigurationChangeClass::Widening => "widening",
        ConfigurationChangeClass::PreviousUnspecified => "previous executor window unspecified; other changed fields may widen",
    };
    assert!(body.contains(class), "{}", page.body);
    assert!(body.contains("future submissions") || body.contains("future_submissions"), "{}", page.body);
}

// Traceability scope: In-process browser requests exercise the real controller
// and durable state. This is not a live browser/provider execution claim.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# The shared agent interface MUST expose supported definition-limit changes through the canonical controller operation layer.
#[tokio::test]
async fn settings_form_displays_active_policy_selected_revision_and_only_allowed_inputs() {
    let f = settings_fixture(settings_config()).await;
    let accepted = settings_seed(&f.controller, "settings-form-seed").await;
    let definition = &accepted.input.definition;
    let page = ui(&f.controller, "GET", &settings_route(&definition.id), vec![]).await;
    assert_eq!(page.status, 200, "{}", page.body);
    assert!(page.body.contains("data-live-refresh=\"false\""));
    for (name, value) in [
        ("expected_revision", definition.revision.as_str()),
        ("expected_global_revision", f.controller.limits().revision.as_str()),
        ("expected_policy_revision", "product-settings-policy-1"),
    ] {
        assert!(page.body.contains(&format!("name=\"{name}\"")), "{}", page.body);
        assert!(page.body.contains(&format!("value=\"{value}\"")), "{}", page.body);
    }
    assert!(page.body.contains("name=\"operation_id\""));
    for (name, _) in SETTINGS_FIELDS {
        assert!(page.body.contains(&format!("name=\"{name}\"")), "missing {name}: {}", page.body);
    }
    for forbidden in ["changes", "model", "reasoning_effort", "executor", "execution_profile", "worker_model", "host_admin"] {
        assert!(!page.body.contains(&format!("name=\"{forbidden}\"")), "editable {forbidden}: {}", page.body);
    }
    let agent = ui(&f.controller, "GET", &format!("/agents/{}", encode(&definition.id)), vec![]).await;
    assert!(agent.body.contains(&settings_route(&definition.id)), "{}", agent.body);
    assert_eq!(settings_selected(&f.controller, &definition.id).await, *definition);
    assert_eq!(task(&f.controller, &accepted.id).await.input, accepted.input);

    let mut cfg = settings_config();
    Arc::make_mut(&mut cfg.limits).definition_updates.as_mut().unwrap().allowed_fields =
        vec![DefinitionLimitField::NativeContextBytes];
    let restricted = settings_fixture(cfg).await;
    let accepted = settings_seed(&restricted.controller, "settings-restricted-form").await;
    let page = ui(&restricted.controller, "GET", &settings_route(&accepted.input.definition.id), vec![]).await;
    assert_eq!(page.status, 200, "{}", page.body);
    assert!(page.body.contains("name=\"native.context_bytes\""));
    for (name, field) in SETTINGS_FIELDS {
        if field != DefinitionLimitField::NativeContextBytes {
            assert!(!page.body.contains(&format!("name=\"{name}\"")), "editable {name}: {}", page.body);
        }
    }
}

//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# A permitted definition-limit change MUST atomically commit its immutable definition, selected head and durable operation outcome.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# A definition-limit change MUST preserve the executor, inference settings and tool capabilities of its predecessor.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# A configuration change outcome MUST distinguish future-task selection from settings retained by already accepted tasks.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# Submitting an explicitly pinned definition MUST NOT change an existing agent's selected definition revision.
#[tokio::test]
async fn settings_apply_uses_canonical_receipt_and_preserves_accepted_task_and_child_pins() {
    let f = settings_fixture(settings_config()).await;
    let old = settings_seed(&f.controller, "settings-apply-seed").await;
    let before = old.input.definition.clone();
    let accepted_bytes = serde_json::to_vec(&old.input).unwrap();
    let prior = f.controller.limits().resolve_definition(&before).unwrap();
    let update = settings_update(&f.controller, &before, prior.native.context_bytes + 1);
    let response = settings_post(&f.controller, "settings-apply", &update).await;
    let page = settings_receipt_page(&f.controller, response).await;
    let recorded = settings_replay(&f.controller, "settings-apply", &update).await;
    let outcome = settings_outcome(&recorded);
    assert_eq!(recorded.commit.as_ref().unwrap().operation_id, "settings-apply");
    assert_eq!(outcome.previous_revision, before.revision);
    assert_eq!(outcome.classification, ConfigurationChangeClass::Widening);
    assert_eq!(outcome.effective_for, ConfigurationEffect::FutureSubmissions);
    settings_assert_receipt(&page, "settings-apply", outcome);

    let selected = settings_selected(&f.controller, &before.id).await;
    assert_eq!(selected, outcome.selected_definition);
    assert_ne!(selected.revision, before.revision);
    assert_eq!(selected.executor, before.executor);
    assert_eq!(selected.inference, before.inference);
    assert_eq!(selected.execution_profile, before.execution_profile);
    let mut expected = prior;
    expected.native.context_bytes += 1;
    assert_eq!(f.controller.limits().resolve_definition(&selected).unwrap(), expected);
    assert_eq!(serde_json::to_vec(&task(&f.controller, &old.id).await.input).unwrap(), accepted_bytes);

    let next_form = ui(&f.controller, "GET", &format!("/tasks/new?agent_id={}", encode(&before.id)), vec![]).await;
    assert_eq!(next_form.status, 200, "{}", next_form.body);
    assert!(next_form.body.contains(&format!("value=\"{}\"", selected.revision)), "{}", next_form.body);
    let next_input = settings_submission_input(&old, selected.clone());
    let Data::Task(next) = call(&f.controller, Operation::SubmitTask {
        operation_id: "settings-next-selected".into(), input: (next_input).into(),
    }).await else { panic!("selected revision task") };
    assert_eq!(next.input.definition, selected);
    assert_eq!(next.input.resolved_limits.as_ref().unwrap().values, expected);
    assert_eq!(next.input.limits, old.input.limits);
    assert_eq!(next.input.native_workflow, old.input.native_workflow);

    let comparison = ui(&f.controller, "GET", &format!("{}?task_id={}", settings_route(&before.id), encode(&old.id)), vec![]).await;
    assert_eq!(comparison.status, 200, "{}", comparison.body);
    assert!(comparison.body.contains("Accepted settings remain pinned"), "{}", comparison.body);
    assert!(comparison.body.contains(&format!("Accepted definition revision</dt><dd>{}", before.revision)), "{}", comparison.body);
    assert!(comparison.body.contains(&format!("Selected definition revision</dt><dd>{}", selected.revision)), "{}", comparison.body);
    assert!(comparison.body.contains(&old.id));

    let explicit_old = settings_submission_input(&old, before.clone());
    let Data::Task(pinned) = call(&f.controller, Operation::SubmitTask {
        operation_id: "settings-explicit-old".into(), input: (explicit_old).into(),
    }).await else { panic!("old pinned revision task") };
    assert_eq!(pinned.input.definition, before);
    assert_eq!(settings_selected(&f.controller, &before.id).await, selected);
    let retained = ui(&f.controller, "GET", &format!("/definitions/{}?revision={}", encode(&before.id), encode(&before.revision)), vec![]).await;
    assert_eq!(retained.status, 200, "{}", retained.body);
    assert!(retained.body.contains(&before.revision));
}

//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# A definition-limit change MUST be restricted to the fields allowed by the configured change policy.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# A definition-limit change MUST validate the complete resulting effective configuration before commit.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# A definition-limit change MUST NOT grant host administration, credentials or protected release authority.
#[tokio::test]
async fn settings_refuse_disallowed_zero_invalid_unknown_and_authority_fields_without_mutation() {
    let mut cfg = settings_config();
    Arc::make_mut(&mut cfg.limits).definition_updates.as_mut().unwrap().allowed_fields =
        vec![DefinitionLimitField::NativeContextBytes];
    let f = settings_fixture(cfg).await;
    let old = settings_seed(&f.controller, "settings-invalid-seed").await;
    let before = old.input.definition.clone();
    let value = f.controller.limits().resolve_definition(&before).unwrap().native.context_bytes + 1;
    let update = settings_update(&f.controller, &before, value);
    let route = settings_route(&before.id);
    let cases = [
        ("disallowed", "coding.summary_bytes", "10", 409, "configuration_policy"),
        ("zero", "native.context_bytes", "0", 409, "invalid_limits"),
        ("negative", "native.context_bytes", "-1", 400, ""),
        ("fraction", "native.context_bytes", "1.5", 400, ""),
        ("overflow", "native.context_bytes", "18446744073709551616", 400, ""),
        ("cross-field", "native.context_bytes", "1", 409, "invalid_limits"),
        ("unknown", "native.model_requests", "1", 400, ""),
        ("json-shape", "changes", "{\"native.context_bytes\":1}", 400, ""),
        ("model", "model", "gpt-6.1-sol", 400, ""),
        ("effort", "reasoning_effort", "low", 400, ""),
        ("executor", "executor", "coding", 400, ""),
        ("profile", "execution_profile", "other", 400, ""),
        ("inference", "inference", "{}", 400, ""),
        ("worker", "worker_model", "other", 400, ""),
        ("budget", "model_requests", "500", 400, ""),
        ("host", "host_admin", "true", 400, ""),
        ("credential", "credentials", "requested", 400, ""),
        ("release", "protected_release", "true", 400, ""),
        ("policy", "definition_updates.allowed_fields", "native.context_bytes", 400, ""),
    ];
    for (name, field, raw, status, code) in cases {
        let body = settings_replace(settings_body(&format!("settings-invalid-{name}"), &update), field, Some(raw));
        let response = if status == 400 {
            ui(&f.controller, "POST", &route, body).await
        } else {
            let mut selected = update.clone();
            selected.changes.insert(serde_json::from_value(serde_json::json!(field)).unwrap(), raw.parse().unwrap());
            settings_post(&f.controller, &format!("settings-invalid-{name}"), &selected).await
        };
        assert_eq!(response.status, status, "{name}: {}", response.body);
        if !code.is_empty() { assert!(response.body.contains(code), "{name}: {}", response.body); }
        assert_eq!(settings_selected(&f.controller, &before.id).await, before, "{name}");
        assert_eq!(task(&f.controller, &old.id).await.input, old.input, "{name}");
    }
    let mut empty = update.clone();
    empty.changes.clear();
    let response = settings_post(&f.controller, "settings-empty", &empty).await;
    assert_eq!(response.status, 409, "{}", response.body);
    assert!(response.body.contains("invalid_input"), "{}", response.body);
    for field in ["operation_id", "expected_revision", "expected_global_revision", "expected_policy_revision"] {
        let body = settings_replace(settings_body(&format!("settings-missing-{field}"), &update), field, None);
        let response = ui(&f.controller, "POST", &route, body).await;
        assert_eq!(response.status, 400, "missing {field}: {}", response.body);
    }
    let mut duplicate = settings_body("settings-duplicate", &update);
    duplicate.extend_from_slice(b"&native.context_bytes=2");
    assert_eq!(ui(&f.controller, "POST", &route, duplicate).await.status, 400);
    assert_eq!(settings_selected(&f.controller, &before.id).await, before);
    assert_eq!(task(&f.controller, &old.id).await.input, old.input);
}

//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# A definition-limit change MUST compare the expected selected definition revision before applying a new immutable revision.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# A definition-limit change MUST compare the expected active global metadata and change-policy revisions before application.
#[tokio::test]
async fn settings_refuse_stale_global_policy_and_head_revisions() {
    let f = settings_fixture(settings_config()).await;
    let old = settings_seed(&f.controller, "settings-stale-seed").await;
    let before = old.input.definition.clone();
    let value = f.controller.limits().resolve_definition(&before).unwrap().native.context_bytes + 1;
    let update = settings_update(&f.controller, &before, value);
    for field in ["expected_global_revision", "expected_policy_revision"] {
        let mut stale = update.clone();
        match field { "expected_global_revision" => stale.expected_global_revision = "obsolete-revision".into(),
            "expected_policy_revision" => stale.expected_policy_revision = "obsolete-revision".into(), _ => unreachable!() }
        let response = settings_post(&f.controller, &format!("settings-stale-{field}"), &stale).await;
        assert_eq!(response.status, 409, "{}", response.body);
        assert!(response.body.contains("configuration_conflict"), "{}", response.body);
        assert_eq!(settings_selected(&f.controller, &before.id).await, before);
    }
    let response = settings_post(&f.controller, "settings-head-winner", &update).await;
    settings_receipt_page(&f.controller, response).await;
    let selected = settings_selected(&f.controller, &before.id).await;
    assert_ne!(selected.revision, before.revision);
    let response = settings_post(&f.controller, "settings-stale-head", &update).await;
    assert_eq!(response.status, 409, "{}", response.body);
    assert!(response.body.contains("configuration_conflict"), "{}", response.body);
    let repeat = settings_post(&f.controller, "settings-stale-head", &update).await;
    assert_eq!(repeat.status, response.status);
    assert_eq!(repeat.body, response.body);
    assert_eq!(settings_selected(&f.controller, &before.id).await, selected);
    assert_eq!(task(&f.controller, &old.id).await.input, old.input);
}

#[tokio::test]
async fn settings_exact_replay_and_lookup_keep_original_outcome_after_head_advances() {
    let f = settings_fixture(settings_config()).await;
    let old = settings_seed(&f.controller, "settings-replay-seed").await;
    let value = f.controller.limits().resolve_definition(&old.input.definition).unwrap().native.context_bytes;
    let update = settings_update(&f.controller, &old.input.definition, value + 1);
    let response = settings_post(&f.controller, "settings-original", &update).await;
    let first_page = settings_receipt_page(&f.controller, response).await;
    let first = settings_replay(&f.controller, "settings-original", &update).await;
    let original = settings_outcome(&first).clone();
    settings_assert_receipt(&first_page, "settings-original", &original);
    let second_update = settings_update(&f.controller, &original.selected_definition, value + 2);
    let response = settings_post(&f.controller, "settings-later", &second_update).await;
    settings_receipt_page(&f.controller, response).await;
    let selected = settings_selected(&f.controller, &old.input.definition.id).await;
    assert_ne!(selected.revision, original.selected_definition.revision);

    let response = settings_post(&f.controller, "settings-original", &update).await;
    let replay_page = settings_receipt_page(&f.controller, response).await;
    settings_assert_receipt(&replay_page, "settings-original", &original);
    let replay = settings_replay(&f.controller, "settings-original", &update).await;
    assert_eq!(replay.outcome, first.outcome);
    assert_eq!(replay.commit, first.commit);
    let lookup = ui(&f.controller, "GET", "/operations/settings-original", vec![]).await;
    settings_assert_receipt(&lookup, "settings-original", &original);

    let mut different = update.clone();
    different.changes.insert(DefinitionLimitField::NativeContextBytes, value + 3);
    let conflict = operator_reply(&f.controller, "settings-original", &different).await;
    assert!(conflict.commit.is_none());
    assert!(matches!(conflict.outcome, Outcome::Refused { ref code, .. } if code == "operation_conflict"));
    let conflict_page = settings_browser_post(&f.controller, "settings-original", &different).await;
    assert_eq!(conflict_page.status, 409, "{}", conflict_page.body);
    assert!(conflict_page.body.contains("operation_conflict"));
    assert_eq!(settings_selected(&f.controller, &old.input.definition.id).await, selected);
    assert_eq!(task(&f.controller, &old.id).await.input, old.input);
    let lookup_after_conflict = ui(&f.controller, "GET", "/operations/settings-original", vec![]).await;
    assert_eq!(lookup_after_conflict.body, lookup.body);
}

#[tokio::test]
async fn settings_blank_fields_are_omitted_and_all_supported_fields_classify_effective_values() {
    let f = settings_fixture(settings_config()).await;
    let old = settings_seed(&f.controller, "settings-classification-seed").await;
    let effective = f.controller.limits().resolve_definition(&old.input.definition).unwrap();
    let mut update = settings_update(&f.controller, &old.input.definition, effective.native.context_bytes);
    update.changes = SETTINGS_FIELDS.iter().map(|(_, field)| (*field, field.value(&effective).expect("explicit numeric settings fixture field"))).collect();
    let response = settings_post(&f.controller, "settings-all-neutral", &update).await;
    let page = settings_receipt_page(&f.controller, response).await;
    let receipt = settings_replay(&f.controller, "settings-all-neutral", &update).await;
    let outcome = settings_outcome(&receipt);
    assert_eq!(outcome.classification, ConfigurationChangeClass::Neutral);
    assert_eq!(outcome.changed_fields, update.changes.keys().copied().collect::<Vec<_>>());
    assert_eq!(f.controller.limits().resolve_definition(&outcome.selected_definition).unwrap(), effective);
    settings_assert_receipt(&page, "settings-all-neutral", outcome);

    let narrow = settings_update(&f.controller, &outcome.selected_definition, effective.native.context_bytes - 1);
    let response = settings_post(&f.controller, "settings-narrow", &narrow).await;
    let page = settings_receipt_page(&f.controller, response).await;
    let receipt = settings_replay(&f.controller, "settings-narrow", &narrow).await;
    let outcome = settings_outcome(&receipt);
    assert_eq!(outcome.classification, ConfigurationChangeClass::Narrowing);
    assert_eq!(outcome.changed_fields, vec![DefinitionLimitField::NativeContextBytes]);
    settings_assert_receipt(&page, "settings-narrow", outcome);
    let mut expected = effective;
    expected.native.context_bytes -= 1;
    assert_eq!(f.controller.limits().resolve_definition(&outcome.selected_definition).unwrap(), expected);
    assert_eq!(task(&f.controller, &old.id).await.input, old.input);
}

#[tokio::test]
async fn settings_disabled_policy_and_untrusted_origins_cannot_change_definition_or_claim_operation() {
    let mut cfg = settings_config();
    Arc::make_mut(&mut cfg.limits).definition_updates = None;
    let disabled = settings_fixture(cfg).await;
    let old = settings_seed(&disabled.controller, "settings-disabled-seed").await;
    let page = ui(&disabled.controller, "GET", &settings_route(&old.input.definition.id), vec![]).await;
    assert_eq!(page.status, 200, "{}", page.body);
    assert!(!page.body.contains("method=\"post\""), "{}", page.body);
    assert!(page.body.contains("Settings changes are disabled: no definition update policy is configured."));
    let value = disabled.controller.limits().resolve_definition(&old.input.definition).unwrap().native.context_bytes + 1;
    let update = settings_update(&disabled.controller, &old.input.definition, value);
    let response = settings_post(&disabled.controller, "settings-disabled", &update).await;
    assert_eq!(response.status, 409, "{}", response.body);
    assert!(response.body.contains("configuration_disabled"), "{}", response.body);
    assert_eq!(settings_selected(&disabled.controller, &old.input.definition.id).await, old.input.definition);
    assert_eq!(task(&disabled.controller, &old.id).await.input, old.input);

    let f = settings_fixture(settings_config()).await;
    let old = settings_seed(&f.controller, "settings-origin-seed").await;
    let value = f.controller.limits().resolve_definition(&old.input.definition).unwrap().native.context_bytes + 1;
    let update = settings_update(&f.controller, &old.input.definition, value);
    for origin in [None, Some("http://untrusted.invalid".into())] {
        let response = handle(&f.controller, UiRequest {
            method: "POST".into(), path: settings_route(&update.agent_id),
            body: settings_body("settings-origin", &update), origin, expected_origin: ORIGIN.into(),
        }).await;
        assert_eq!(response.status, 403, "{}", response.body);
        assert_eq!(settings_selected(&f.controller, &old.input.definition.id).await, old.input.definition);
        assert_eq!(task(&f.controller, &old.id).await.input, old.input);
    }
    let lookup = ui(&f.controller, "GET", "/operations/settings-origin", vec![]).await;
    assert_eq!(lookup.status, 404, "{}", lookup.body);
    let response = settings_post(&f.controller, "settings-origin", &update).await;
    settings_receipt_page(&f.controller, response).await;
    assert_ne!(settings_selected(&f.controller, &old.input.definition.id).await.revision, old.input.definition.revision);
}

#[tokio::test]
async fn settings_commit_wakes_live_changes_without_fabricating_task_activity() {
    let f = settings_fixture(settings_config()).await;
    let old = settings_seed(&f.controller, "settings-live-seed").await;
    let activity = ui(&f.controller, "GET", "/ui/activity", vec![]).await;
    assert_eq!(activity.status, 200, "{}", activity.body);
    let activity: serde_json::Value = serde_json::from_str(&activity.body).unwrap();
    let cursor = activity["cursor"].as_str().unwrap().to_string();
    assert!(!activity["items"].as_array().unwrap().is_empty());
    let baseline = ui(&f.controller, "GET", "/ui/changes?after=0", vec![]).await;
    assert_eq!(baseline.status, 200, "{}", baseline.body);
    let baseline: serde_json::Value = serde_json::from_str(&baseline.body).unwrap();
    let revision = baseline["revision"].as_str().unwrap().parse::<u64>().unwrap();
    let controller = f.controller.clone();
    let route = format!("/ui/changes?after={revision}");
    let waiting = tokio::spawn(async move { ui(&controller, "GET", &route, vec![]).await });

    let value = f.controller.limits().resolve_definition(&old.input.definition).unwrap().native.context_bytes + 1;
    let update = settings_update(&f.controller, &old.input.definition, value);
    let response = settings_post(&f.controller, "settings-live-update", &update).await;
    let receipt = settings_receipt_page(&f.controller, response).await;
    assert!(receipt.body.contains("data-live-refresh=\"false\""), "{}", receipt.body);
    for element in ["aria-label=\"Borg activity\"", "id=\"activity-items\"", "src=\"/ui/live.js\""] {
        assert!(receipt.body.contains(element), "missing {element}: {}", receipt.body);
    }
    let changed = tokio::time::timeout(std::time::Duration::from_secs(2), waiting).await.unwrap().unwrap();
    assert_eq!(changed.status, 200, "{}", changed.body);
    let changed: serde_json::Value = serde_json::from_str(&changed.body).unwrap();
    assert_eq!(changed["changed"], true);
    assert_eq!(changed["reset"], false);
    assert!(changed["revision"].as_str().unwrap().parse::<u64>().unwrap() > revision);

    // The existing activity feed describes task events. A settings commit moves
    // the durable controller revision without inventing an event on this task.
    let after = ui(&f.controller, "GET", &format!("/ui/activity?after={cursor}"), vec![]).await;
    assert_eq!(after.status, 200, "{}", after.body);
    let after: serde_json::Value = serde_json::from_str(&after.body).unwrap();
    assert!(after["items"].as_array().unwrap().is_empty(), "{after}");
    assert_eq!(after["cursor"].as_str(), Some(cursor.as_str()));
    assert_eq!(task(&f.controller, &old.id).await, old);
    let page = ui(&f.controller, "GET", &settings_route(&update.agent_id), vec![]).await;
    assert_eq!(page.status, 200, "{}", page.body);
    assert!(page.body.contains("data-live-refresh=\"false\""));
    assert!(page.body.contains("aria-label=\"Borg activity\""));
}

#[tokio::test]
async fn settings_empty_policy_stays_readable_and_never_renders_an_apply_form() {
    let mut cfg = settings_config();
    Arc::make_mut(&mut cfg.limits).definition_updates.as_mut().unwrap().allowed_fields.clear();
    let f = settings_fixture(cfg).await;
    let old = settings_seed(&f.controller, "settings-empty-policy-seed").await;
    let page = ui(&f.controller, "GET", &format!("{}?task_id={}", settings_route(&old.input.definition.id), old.id), vec![]).await;
    assert_eq!(page.status, 200, "{}", page.body);
    assert!(page.body.contains("current policy allows no settings changes"));
    assert!(page.body.contains("Accepted settings remain pinned"));
    assert!(!page.body.contains("method=\"post\""));
    let value = old.input.resolved_limits.as_ref().unwrap().values.native.context_bytes + 1;
    let update = settings_update(&f.controller, &old.input.definition, value);
    let response = settings_post(&f.controller, "settings-empty-policy-write", &update).await;
    assert_eq!(response.status, 409, "{}", response.body);
    assert!(response.body.contains("configuration_policy"));
    assert_eq!(settings_selected(&f.controller, &old.input.definition.id).await, old.input.definition);
}

#[tokio::test]
async fn settings_form_explains_fields_and_errors_do_not_echo_unknown_operands() {
    let f = settings_fixture(settings_config()).await;
    let old = settings_seed(&f.controller, "settings-accessible-seed").await;
    let route = settings_route(&old.input.definition.id);
    let page = ui(&f.controller, "GET", &route, vec![]).await;
    assert!(page.body.contains("<legend>Allowed execution limits</legend>"));
    assert!(page.body.contains("aria-describedby=\"settings-change-help\""));
    assert!(page.body.contains("id=\"settings-change-help\""));
    let value = old.input.resolved_limits.as_ref().unwrap().values.native.context_bytes + 1;
    let update = settings_update(&f.controller, &old.input.definition, value);
    let mut body = settings_body("settings-private-error", &update);
    body.extend_from_slice(b"&must-not-echo-field=must-not-echo-value");
    let error = ui(&f.controller, "POST", &route, body).await;
    assert_eq!(error.status, 400);
    for private in ["must-not-echo-field", "must-not-echo-value"] { assert!(!error.body.contains(private)); }
    assert!(error.body.contains("role=\"alert\" tabindex=\"-1\" autofocus"));
    let mut stale = update.clone();
    stale.expected_revision = "stale-revision".into();
    let refused = settings_post(&f.controller, "settings-focus-refusal", &stale).await;
    assert_eq!(refused.status, 409);
    assert!(refused.body.contains("role=\"alert\" tabindex=\"-1\" autofocus"));
    assert_eq!(settings_selected(&f.controller, &old.input.definition.id).await, old.input.definition);
}

#[tokio::test]
async fn original_non_task_operation_lookup_shows_commit_without_an_empty_task_link() {
    let f = settings_fixture(settings_config()).await;
    let original = "settings-observer-worker-registration";
    call(&f.controller, Operation::RegisterWorker {
        operation_id: original.into(),
        worker: Worker { id: "settings-observer-worker".into(), incarnation: "one".into(), coding_capacity: 0, native_capacity: 0 },
    }).await;
    let response = ui(&f.controller, "GET", &format!("/operations/{original}"), vec![]).await;
    assert_eq!(response.status, 200, "{}", response.body);
    assert!(response.body.contains("Original committed controller revision"));
    assert!(response.body.contains("does not retry it"));
    assert!(!response.body.contains("href=\"/tasks/\""));
}

#[tokio::test]
async fn unresolved_settings_receipt_keeps_original_identity_and_does_not_refresh_into_a_new_form() {
    let mut cfg = settings_config();
    Arc::make_mut(&mut cfg.limits).controller.max_receipts = 2;
    let f = settings_fixture(cfg).await;
    let old = settings_seed(&f.controller, "settings-pending-seed").await;
    let update = settings_update(&f.controller, &old.input.definition,
        old.input.resolved_limits.as_ref().unwrap().values.native.context_bytes + 1);
    // The fixture registration and original submission filled ordinary receipt
    // capacity. The real controller retains no commit for this new operation.
    let response = settings_browser_post(&f.controller, "settings-pending-original", &update).await;
    // Receipt-capacity refusal has no committed stamp. The browser treats this
    // uncommitted refusal as unresolved (503), distinct from Pending (202).
    assert_eq!(response.status, 503, "{}", response.body);
    assert!(response.body.contains("Operation outcome unresolved"));
    assert!(response.body.contains("settings-pending-original"));
    assert!(response.body.contains("data-live-refresh=\"false\""));
    assert!(response.body.contains("aria-label=\"Borg activity\""));
    assert!(!response.body.contains("method=\"post\""));
    let lookup = ui(&f.controller, "GET", "/operations/settings-pending-original", vec![]).await;
    assert_eq!(lookup.status, 404);
    assert!(lookup.body.contains("Operation outcome unresolved"));
    assert!(lookup.body.contains("does not establish whether an earlier request applied"));
    assert!(lookup.body.contains("settings-pending-original"));
    assert!(lookup.body.contains("data-live-refresh=\"false\""));
    assert_eq!(settings_selected(&f.controller, &old.input.definition.id).await, old.input.definition);
    assert_eq!(task(&f.controller, &old.id).await.input, old.input);
}

#[tokio::test]
async fn oversized_browser_receipt_preserves_operation_identity_after_the_actual_commit() {
    let cfg = super::response_bounds::small_config(settings_config());
    let bound = cfg.limits.product.response_bytes;
    let f = settings_fixture(cfg).await;
    let submitted = ui(&f.controller, "POST", "/tasks", native_submission("settings-small-render-seed", now() + 3_600_000, None)).await;
    super::response_bounds::assert_compact(&submitted, bound, 502, Some("settings-small-render-seed"), None);
    let Data::Tasks(tasks) = call(&f.controller, Operation::ListTasks { after: None, limit: 1 }).await else { panic!("committed task list") };
    assert_eq!(tasks.len(), 1);
    let old = &tasks[0];
    let update = settings_update(&f.controller, &old.input.definition,
        old.input.resolved_limits.as_ref().unwrap().values.native.context_bytes + 1);
    let response = settings_post(&f.controller, "settings-small-render", &update).await;
    super::response_bounds::assert_compact(&response, bound, 502, Some("settings-small-render"), None);
    let actual = settings_replay(&f.controller, "settings-small-render", &update).await;
    let outcome = settings_outcome(&actual);
    assert_eq!(settings_selected(&f.controller, &old.input.definition.id).await, outcome.selected_definition);
    assert_ne!(outcome.selected_definition.revision, old.input.definition.revision);
    let lookup = ui(&f.controller, "GET", "/operations/settings-small-render", vec![]).await;
    super::response_bounds::assert_compact(&lookup, bound, 502, Some("settings-small-render"), None);
    let retry = settings_post(&f.controller, "settings-small-render", &update).await;
    super::response_bounds::assert_compact(&retry, bound, 502, Some("settings-small-render"), None);
    let replay = settings_replay(&f.controller, "settings-small-render", &update).await;
    assert_eq!(replay.commit, actual.commit);
    assert_eq!(replay.outcome, actual.outcome);
    assert_eq!(settings_selected(&f.controller, &old.input.definition.id).await, outcome.selected_definition);
    assert_eq!(task(&f.controller, &old.id).await.input, old.input);

    // A changed body under the same identity preserves the original successful
    // receipt. A different stale identity records a final refusal of its own.
    let mut conflict = update.clone();
    *conflict.changes.get_mut(&DefinitionLimitField::NativeContextBytes).unwrap() += 1;
    let response = operator_reply(&f.controller, "settings-small-render", &conflict).await;
    assert!(response.commit.is_none());
    assert!(matches!(response.outcome, Outcome::Refused { ref code, .. } if code == "operation_conflict"));
    // Explicitly exercise the old raw-form correlation rendering too. This is
    // a changed-body refusal against an existing original, never a new grant.
    let conflict_page = settings_browser_post(&f.controller, "settings-small-render", &conflict).await;
    super::response_bounds::assert_compact(&conflict_page, bound, 409, Some("settings-small-render"), None);
    // The original successful receipt still renders with the same bound.
    let original_page = ui(&f.controller, "GET", "/operations/settings-small-render", vec![]).await;
    super::response_bounds::assert_compact(&original_page, bound, 502, Some("settings-small-render"), None);
    let rejected = settings_replay(&f.controller, "settings-small-render", &conflict).await;
    assert!(matches!(rejected.outcome, Outcome::Refused { ref code, .. } if code == "operation_conflict"));
    assert!(rejected.commit.is_none());
    let stale = settings_post(&f.controller, "settings-small-stale", &update).await;
    super::response_bounds::assert_compact(&stale, bound, 409, Some("settings-small-stale"), None);
    let refusal = settings_replay(&f.controller, "settings-small-stale", &update).await;
    assert!(matches!(refusal.outcome, Outcome::Refused { ref code, .. } if code == "configuration_conflict"));
    assert!(refusal.commit.is_some());
    let lookup = ui(&f.controller, "GET", "/operations/settings-small-stale", vec![]).await;
    super::response_bounds::assert_compact(&lookup, bound, 409, Some("settings-small-stale"), None);
    let replay = settings_replay(&f.controller, "settings-small-stale", &update).await;
    assert_eq!(replay.commit, refusal.commit);
    assert_eq!(replay.outcome, refusal.outcome);
    let original = settings_replay(&f.controller, "settings-small-render", &update).await;
    assert_eq!(original.commit, actual.commit);
    assert_eq!(original.outcome, actual.outcome);
    assert_eq!(settings_selected(&f.controller, &old.input.definition.id).await, outcome.selected_definition);
    assert_eq!(task(&f.controller, &old.id).await.input, old.input);
}

#[tokio::test]
async fn compact_uncommitted_capacity_keeps_503_and_original_identity_without_a_receipt() {
    let mut cfg = super::response_bounds::small_config(settings_config());
    Arc::make_mut(&mut cfg.limits).controller.max_receipts = 2;
    let bound = cfg.limits.product.response_bytes;
    let f = settings_fixture(cfg).await;
    let submitted = ui(&f.controller, "POST", "/tasks", native_submission("small-pending-seed", now() + 3_600_000, None)).await;
    super::response_bounds::assert_compact(&submitted, bound, 502, Some("small-pending-seed"), None);
    let Data::Tasks(tasks) = call(&f.controller, Operation::ListTasks { after: None, limit: 1 }).await else { panic!("committed task list") };
    assert_eq!(tasks.len(), 1);
    let old = &tasks[0];
    let update = settings_update(&f.controller, &old.input.definition,
        old.input.resolved_limits.as_ref().unwrap().values.native.context_bytes + 1);
    for _ in 0..2 {
        let response = settings_browser_post(&f.controller, "small-pending-original", &update).await;
        super::response_bounds::assert_compact(&response, bound, 503, Some("small-pending-original"), None);
        let actual = settings_replay(&f.controller, "small-pending-original", &update).await;
        assert!(actual.commit.is_none());
        assert!(matches!(actual.outcome, Outcome::Refused { ref code, .. } if code == "capacity"));
        let lookup = ui(&f.controller, "GET", "/operations/small-pending-original", vec![]).await;
        super::response_bounds::assert_compact(&lookup, bound, 404, Some("small-pending-original"), None);
        assert_eq!(settings_selected(&f.controller, &old.input.definition.id).await, old.input.definition);
        assert_eq!(task(&f.controller, &old.id).await.input, old.input);
    }
}

#[tokio::test]
async fn sessions_share_current_memory_revision_without_reinterpreting_accepted_definitions() {
    let f = settings_fixture(settings_config()).await;
    let first = settings_seed(&f.controller, "settings-memory-first").await;
    let second = settings_seed(&f.controller, "settings-memory-second").await;
    assert_ne!(first.session_id, second.session_id);
    assert_eq!(first.assigned_agent_id, second.assigned_agent_id);
    let worker = Worker { id: "settings-memory-worker".into(), incarnation: "one".into(), coding_capacity: 0, native_capacity: 2 };
    call(&f.controller, Operation::RegisterWorker { operation_id: "settings-memory-register".into(), worker: worker.clone() }).await;
    let Data::Commands(commands) = call(&f.controller, Operation::AwaitCommands {
        operation_id: "settings-memory-launch".into(), worker_id: worker.id, incarnation: worker.incarnation, acknowledged: vec![],
    }).await else { panic!("actual controller launch bindings") };
    let attempt = commands.iter().find_map(|command| match command {
        WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == first.id => Some(attempt.clone()),
        _ => None,
    }).expect("first session has an actual controller-issued attempt");
    for (revision, expected) in [(1, None), (2, Some(1))] {
        call(&f.controller, Operation::SetMemory {
            operation_id: format!("settings-memory-write-{revision}"), task_id: first.id.clone(), attempt_id: attempt.id.clone(), generation: attempt.generation,
            key: "shared-fact".into(), expected_revision: expected, value: serde_json::json!({"revision":revision}),
        }).await;
        let memory = ui(&f.controller, "GET", &format!("/agents/{}/memory/shared-fact", encode(&first.assigned_agent_id)), vec![]).await;
        assert_eq!(memory.status, 200, "{}", memory.body);
        assert!(memory.body.contains(&format!("revision&quot;: {revision}")));
        assert!(memory.body.contains("does not establish that any particular session consumed it"));
        assert!(memory.body.contains("data-live-refresh=\"true\""));
    }
    for old in [&first, &second] {
        let session = ui(&f.controller, "GET", &format!("/sessions/{}", encode(&old.session_id)), vec![]).await;
        assert_eq!(session.status, 200, "{}", session.body);
        assert!(session.body.contains(&format!("href=\"/agents/{}/memory\"", encode(&old.assigned_agent_id))));
        assert!(session.body.contains(&format!("/settings?task_id={}", encode(&old.id))));
        assert!(session.body.contains(&format!("Accepted agent revision</dt><dd>{}", old.input.definition.revision)));
        assert_eq!(task(&f.controller, &old.id).await.input, old.input);
    }
}

#[tokio::test]
async fn accepted_task_configuration_exposes_escaped_schema_and_retains_its_definition() {
    let fixture = settings_fixture(settings_config()).await;
    let c = &fixture.controller;
    let schema = r#"{"type":"object","description":"<img src=x onerror=alert(1)>","properties":{"diagnostic_signal":{"type":"string"}}}"#;
    let response = ui(c, "POST", "/tasks", settings_replace(
        native_submission("visible-accepted-schema", now() + 3_600_000, None),
        "output_schema", Some(schema),
    )).await;
    let accepted = task(c, task_id(&response)).await;
    let update = settings_update(c, &accepted.input.definition,
        accepted.input.resolved_limits.as_ref().unwrap().values.native.context_bytes + 1);
    settings_receipt_page(c, settings_post(c, "schema-later-selected-head", &update).await).await;
    let selected = settings_selected(c, &accepted.input.definition.id).await;
    assert_ne!(selected.revision, accepted.input.definition.revision);

    let page = ui(c, "GET", &format!("/tasks/{}", encode(&accepted.id)), vec![]).await;
    assert_eq!(page.status, 200, "{}", page.body);
    let (_, detail) = page.body.split_once("<summary>Accepted task configuration</summary>")
        .expect("accepted configuration should be reachable on the task page");
    let detail = detail.split_once("</details>").unwrap().0;
    assert!(detail.contains("&quot;output_schema&quot;"));
    assert!(detail.contains("&quot;diagnostic_signal&quot;"));
    assert!(detail.contains("&lt;img src=x onerror=alert(1)&gt;"));
    assert!(!detail.contains("<img"));
    assert!(detail.contains(&accepted.input.definition.revision));
    assert!(!detail.contains(&selected.revision));
    assert_eq!(task(c, &accepted.id).await.input, accepted.input);
}

// Exercises the actual Rust-rendered operator form and canonical update receipt.
// Token windows remain distinct from native byte limits and accepted task pins.
#[tokio::test]
async fn settings_optional_coding_window_shows_units_and_preserves_unspecified_accepted_value() {
    let mut cfg = settings_config();
    let limits = Arc::make_mut(&mut cfg.limits);
    limits.execution.coding.model_context_window = None;
    limits.definition_updates.as_mut().unwrap().allowed_fields.push(DefinitionLimitField::CodingModelContextWindow);
    limits.validate().unwrap();
    let f = settings_fixture(cfg).await;
    let submitted = ui(&f.controller, "POST", "/tasks", submission("settings-window-seed", "Keep the original accepted configuration", "")).await;
    let old = task(&f.controller, task_id(&submitted)).await;
    let prior = f.controller.limits().resolve_definition(&old.input.definition).unwrap();
    assert_eq!(prior.coding.model_context_window, None);
    let route = settings_route(&old.input.definition.id);
    let before = ui(&f.controller, "GET", &route, vec![]).await;
    assert_eq!(before.status, 200, "{}", before.body);
    fn label<'a>(body: &'a str, field: &str) -> &'a str {
        let marker = format!("<label>{field} ");
        let start = body.find(&marker).expect("allowed setting label exists");
        body[start..].split("</label>").next().unwrap()
    }
    let window = label(&before.body, "coding.model_context_window");
    assert!(window.contains("Current effective value: executor default (unspecified) · tokens"), "{window}");
    assert!(window.contains("placeholder=\"Keep executor default (unspecified)\""), "{window}");
    assert!(!window.contains("value=\"0\""), "{window}");
    assert!(label(&before.body, "native.context_bytes").contains("· bytes"));
    assert!(label(&before.body, "native.max_output_tokens").contains("· tokens"));
    assert!(label(&before.body, "coding.summary_chars").contains("· characters"));

    let mut update = settings_update(&f.controller, &old.input.definition, prior.native.context_bytes);
    update.changes = [(DefinitionLimitField::CodingModelContextWindow, 131_072)].into_iter().collect();
    let response = settings_post(&f.controller, "settings-window-apply", &update).await;
    let page = settings_receipt_page(&f.controller, response).await;
    let recorded = settings_replay(&f.controller, "settings-window-apply", &update).await;
    let outcome = settings_outcome(&recorded);
    assert_eq!(outcome.classification, ConfigurationChangeClass::PreviousUnspecified);
    assert_eq!(outcome.changed_fields, vec![DefinitionLimitField::CodingModelContextWindow]);
    assert_eq!(outcome.effective_for, ConfigurationEffect::FutureSubmissions);
    settings_assert_receipt(&page, "settings-window-apply", outcome);
    let selected = settings_selected(&f.controller, &old.input.definition.id).await;
    assert_eq!(selected, outcome.selected_definition);
    assert_eq!(selected.executor, old.input.definition.executor);
    assert_eq!(selected.inference, old.input.definition.inference);
    assert_eq!(selected.execution_profile, old.input.definition.execution_profile);
    let mut expected = prior;
    expected.coding.model_context_window = Some(131_072);
    assert_eq!(f.controller.limits().resolve_definition(&selected).unwrap(), expected);
    assert_eq!(task(&f.controller, &old.id).await.input, old.input);
    assert_eq!(old.input.execution_limits().unwrap().coding.model_context_window, None);
    let after = ui(&f.controller, "GET", &route, vec![]).await;
    assert_eq!(after.status, 200, "{}", after.body);
    assert!(label(&after.body, "coding.model_context_window").contains("Current effective value: 131072 · tokens"));
}

#[tokio::test]
async fn settings_operator_http_preserves_browser_refusal_origin_and_original_governance_subject() {
    use borg_runtime::collaboration::workflow::{ActorProvenance, PrincipalRef, TrustedHostPath};
    use borg_runtime::governance::{DecisionPoint, DecisionRef, GovernanceQuery, GovernanceResult};
    let mut cfg = settings_config();
    Arc::make_mut(&mut cfg.limits).governance = Some(borg_runtime::limits::GovernanceLimits {
        record_bytes: 65_536, reason_items: 16, reason_bytes: 4_096, scan_rows: 512,
        rule_versions: 16, configuration_versions: 16, shadow_evidence_items: 16,
        completion_continuations: 2, override_max_ms: 10_000,
    });
    cfg.limits.validate().unwrap();
    let f = settings_fixture(cfg).await;
    let old = settings_seed(&f.controller, "settings-operator-boundary-seed").await;
    let update = settings_update(&f.controller, &old.input.definition,
        f.controller.limits().resolve_definition(&old.input.definition).unwrap().native.context_bytes + 1);
    let browser = settings_browser_post(&f.controller, "settings-browser-not-operator", &update).await;
    assert_eq!(browser.status, 409);
    assert!(browser.body.contains("governance_operator_required"));
    assert_eq!(settings_selected(&f.controller, &old.assigned_agent_id).await, old.input.definition);

    let listener = borg_runtime::server::listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(borg_runtime::server::serve(f.controller.clone(), listener, origin.clone()));
    let forbidden = Request { protocol_revision: WIRE_REVISION, request_id: "settings-cross-origin-request".into(),
        command: Operation::UpdateDefinition { operation_id: "settings-cross-origin".into(), update: update.clone() } };
    let response = reqwest::Client::builder().no_proxy().build().unwrap().post(format!("{origin}/api/operations"))
        .header("Origin", "http://untrusted.invalid").header("Content-Type", "application/json")
        .body(serde_json::to_vec(&forbidden).unwrap()).send().await.unwrap();
    assert_eq!(response.status(), 403);
    server.abort(); let _ = server.await;
    let absent = f.controller.handle(Request { protocol_revision: WIRE_REVISION,
        request_id: "settings-cross-origin-lookup".into(),
        command: Operation::GetOperation { operation_id: "settings-cross-origin".into() } }).await;
    assert!(matches!(absent.outcome, Outcome::Refused { ref code, .. } if code == "not_found"));
    assert_eq!(settings_selected(&f.controller, &old.assigned_agent_id).await, old.input.definition);

    let applied = operator_reply(&f.controller, "settings-real-operator", &update).await;
    settings_outcome(&applied);
    let Data::Governance(result) = call(&f.controller, Operation::QueryGovernance { query: GovernanceQuery::Decision {
        decision: DecisionRef { operation_id: "settings-real-operator".into(), point: DecisionPoint::DefinitionLimits }
    }}).await else { panic!("original governance decision") };
    let GovernanceResult::Decision(decision) = *result else { panic!("original subject") };
    assert_eq!(decision.actor.principal, PrincipalRef::TrustedHostOperator);
    assert_eq!(decision.actor.provenance, ActorProvenance::TrustedHost { path: TrustedHostPath::OperatorApi });
    assert_eq!(decision.controller_revision, applied.commit.as_ref().unwrap().controller_revision);
    let original = ui(&f.controller, "GET", "/operations/settings-real-operator", vec![]).await;
    settings_assert_receipt(&original, "settings-real-operator", settings_outcome(&applied));
    let retained = ui(&f.controller, "GET", &format!("{}?original_operation=settings-real-operator", settings_route(&old.assigned_agent_id)), vec![]).await;
    assert_eq!(retained.status, 200);
    assert!(retained.body.contains("/operations/settings-real-operator"));
    assert!(retained.body.contains("data-live-refresh=\"false\""));
    assert!(!retained.body.contains("method=\"post\""));
    let script = ui(&f.controller, "GET", "/ui/definition-settings.js", vec![]).await;
    assert_eq!(script.status, 200);
    assert!(script.content_type.starts_with("text/javascript"));
    assert_eq!(script.body, include_str!("../../src/product/definition_settings.js"));
    assert_eq!(task(&f.controller, &old.id).await.input, old.input);
}
