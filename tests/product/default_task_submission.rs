//! Real product/controller admission and receipt replay. No executor or provider is run.
use super::*;
use std::sync::Arc;
use borg_runtime::collaboration::{Query, QueryResult};

fn configured(agent: Option<&str>, revision: &str) -> ControllerConfig {
    let mut cfg = config();
    let metadata = Arc::make_mut(&mut cfg.limits);
    metadata.task_defaults.agent_id = agent.map(String::from);
    metadata.revision = revision.into();
    metadata.validate().unwrap();
    cfg
}
async fn fixture(cfg: ControllerConfig) -> Fixture {
    let root = std::env::temp_dir().join(format!("borg-product-default-agent-{}-{}-{}",
        std::process::id(), now(), NEXT.fetch_add(1, Ordering::Relaxed)));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("tasks.sqlite");
    let controller = Controller::open_with_config(&path, cfg).unwrap();
    register_fixture_repository(&controller).await;
    Fixture { controller, path }
}
fn definition(id: &str) -> Definition {
    Definition {
        charter: None, limit_overrides: None, id: id.into(), revision: "revision-1".into(),
        executor: ExecutorKind::Coding, inference: Some(InferenceSettings::selected()),
        execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
    }
}
async fn register(controller: &Controller, value: &Definition) {
    assert!(matches!(call(controller, Operation::RegisterAgent {
        operation_id: format!("register-{}", value.id), definition: value.clone(),
    }).await, Data::Agent(agent) if agent.definition == *value && !agent.retired));
}
fn omitted(operation: &str, instruction: &str) -> Vec<u8> {
    // Retain task operands from the existing explicit browser fixture, removing
    // definition settings rather than substituting a fake definition.
    let body = String::from_utf8(submission(operation, instruction, "")).unwrap();
    let mut body = body.split('&').filter(|field| {
        !["definition_id", "definition_revision", "model", "reasoning_effort", "execution_profile", "executor"]
            .iter().any(|key| field.starts_with(&format!("{key}=")))
    }).collect::<Vec<_>>().join("&").into_bytes();
    body.extend_from_slice(b"&result_bytes=8192");
    body
}
async fn operation(controller: &Controller, id: &str) -> OperationRecord {
    match call(controller, Operation::GetOperation { operation_id: id.into() }).await {
        Data::Operation(record) => record,
        other => panic!("expected operation record, got {other:?}"),
    }
}
async fn owner_released(path: &std::path::Path) {
    let mut lock_path = path.as_os_str().to_os_string(); lock_path.push(".controller-lock");
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let lock = std::fs::OpenOptions::new().read(true).write(true).open(&lock_path).unwrap();
            if lock.try_lock().is_ok() { break; }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }).await.expect("actual controller owner releases before restart");
}
async fn assert_no_work(controller: &Controller) {
    assert!(matches!(call(controller, Operation::ListTasks { after: None, limit: 10 }).await,
        Data::Tasks(tasks) if tasks.is_empty()));
    assert!(matches!(call(controller, Operation::QueryCollaboration {
        query: Query::Sessions { agent_id: None, after: None, limit: 10 },
    }).await, Data::Collaboration(QueryResult::Sessions(page)) if page.items.is_empty() && page.next_cursor.is_none()));
}

#[tokio::test]
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST support task submission.
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# An exact retry of a committed mutating request MUST return its recorded outcome.
//= spec/impl/v2/implementation.md#default-task-assignment
//= type=test
//# The browser interface MUST support task submission without an explicit agent selection.
async fn omitted_assignment_replays_after_default_change_retirement_and_restart() {
    let first_agent = definition("product-default-first");
    let second_agent = definition("product-default-second");
    let f = fixture(configured(Some(&first_agent.id), "product-default-r1")).await;
    register(&f.controller, &first_agent).await;
    register(&f.controller, &second_agent).await;
    assert_no_work(&f.controller).await;
    let page = ui(&f.controller, "GET", "/tasks/new", vec![]).await;
    assert_eq!(page.status, 200, "{}", page.body);
    assert!(page.body.contains(&format!("Default agent: <strong>{}</strong>", first_agent.id)));
    for forbidden in ["definition_id", "definition_revision", "executor", "model", "execution_profile"] {
        assert!(!page.body.contains(&format!("name=\"{forbidden}\"")), "{}", page.body);
    }
    for expected in ["operation_id", "model_requests", "deadline_unix_ms", "output_bytes", "result_bytes", "max_delegation_depth"] {
        assert!(page.body.contains(&format!("name=\"{expected}\"")));
    }
    let body = omitted("default-browser-original", "Preserve <work> and the original assignment");
    let first = ui(&f.controller, "POST", "/tasks", body.clone()).await;
    let id = task_id(&first).to_owned();
    let accepted = task(&f.controller, &id).await;
    assert_eq!(accepted.input.definition, first_agent);
    assert_eq!(accepted.assigned_agent_id, first_agent.id);
    let snapshot = accepted.input.resolved_limits.as_ref().unwrap();
    assert_eq!(snapshot.global_revision, "product-default-r1");
    assert_eq!(snapshot.global_metadata.task_defaults.agent_id.as_deref(), Some(first_agent.id.as_str()));
    let original = operation(&f.controller, "default-browser-original").await;
    assert_eq!(original.intended["operation"], "submit_task_draft");
    assert!(original.intended["input"]["input"].as_object().unwrap().get("definition").is_none());
    assert_eq!(original.task_id, id);
    assert!(matches!(call(&f.controller, Operation::RetireAgent {
        operation_id: "retire-first-default".into(), agent_id: first_agent.id.clone(),
        expected_revision: first_agent.revision.clone(),
    }).await, Data::Agent(agent) if agent.retired));
    let path = f.path.clone(); drop(f); owner_released(&path).await;
    let mut cfg = configured(Some(&second_agent.id), "product-default-r2");
    let metadata = Arc::make_mut(&mut cfg.limits);
    metadata.task_defaults.model_requests = 99;
    metadata.task_defaults.result_bytes = metadata.task_defaults.result_bytes.checked_sub(1).unwrap();
    assert_ne!(metadata.task_defaults.result_bytes, snapshot.global_metadata.task_defaults.result_bytes);
    metadata.validate().unwrap();
    let controller = Controller::open_with_config(&path, cfg).unwrap();
    let replay = ui(&controller, "POST", "/tasks", body).await;
    assert_eq!(replay.status, first.status, "{}", replay.body);
    assert_eq!(replay.location, first.location);
    assert_eq!(operation(&controller, "default-browser-original").await, original);
    assert_eq!(task(&controller, &id).await.input, accepted.input);
    assert!(matches!(call(&controller, Operation::ListTasks { after: None, limit: 10 }).await,
        Data::Tasks(tasks) if tasks.len() == 1 && tasks[0].id == id));
    let conflict = ui(&controller, "POST", "/tasks", omitted("default-browser-original", "Different work")).await;
    assert_eq!(conflict.status, 409, "{}", conflict.body);
    assert!(conflict.body.contains("operation_conflict"));
    assert_eq!(operation(&controller, "default-browser-original").await, original);
    let fresh = ui(&controller, "POST", "/tasks", omitted("default-browser-new", "Use the new default")).await;
    let fresh = task(&controller, task_id(&fresh)).await;
    assert_ne!(fresh.id, id);
    assert_eq!(fresh.input.definition, second_agent);
    assert_eq!(fresh.input.resolved_limits.as_ref().unwrap().global_revision, "product-default-r2");
}

#[tokio::test]
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST support task submission.
//= spec/impl/v2/implementation.md#agent-identity-lifecycle
//= type=test
//# A retired agent identity MUST be refused for new work.
async fn unavailable_default_is_visible_and_leaves_no_task_or_session() {
    for (name, selected, retire) in [
        ("missing", None, false),
        ("unknown", Some("product-default-unknown"), false),
        ("retired", Some("product-default-retired"), true),
    ] {
        let f = fixture(configured(selected, &format!("product-default-{name}"))).await;
        if retire {
            let value = definition(selected.unwrap());
            register(&f.controller, &value).await;
            call(&f.controller, Operation::RetireAgent {
                operation_id: "retire-default".into(), agent_id: value.id, expected_revision: value.revision,
            }).await;
        }
        let body = omitted(&format!("default-refused-{name}"), "Never create work for an unavailable default");
        let refused = ui(&f.controller, "POST", "/tasks", body.clone()).await;
        assert_eq!(refused.status, 409, "{name}: {}", refused.body);
        assert!(refused.body.contains("task_defaults.agent_id"), "{name}: {}", refused.body);
        let code = match name {
            "missing" => "default_agent_not_configured",
            "unknown" => "default_agent_not_found",
            _ => "agent_retired",
        };
        assert!(refused.body.contains(code), "{}", refused.body);
        assert!(refused.location.is_none());
        assert_no_work(&f.controller).await;
        let record = operation(&f.controller, &format!("default-refused-{name}")).await;
        assert_eq!(record.intended["operation"], "submit_task_draft");
        assert!(record.intended["input"]["input"].as_object().unwrap().get("definition").is_none());
        let replay = ui(&f.controller, "POST", "/tasks", body).await;
        assert_eq!(replay.status, refused.status);
        assert_eq!(operation(&f.controller, &format!("default-refused-{name}")).await, record);
        assert_no_work(&f.controller).await;
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# An exact retry of a committed mutating request MUST return its recorded outcome.
async fn explicit_forms_stay_explicit_and_partial_or_mixed_definition_fields_refuse() {
    let f = fixture(configured(None, "product-no-default")).await;
    let body = submission("explicit-browser-original", "Explicit legacy selection", "");
    let first = ui(&f.controller, "POST", "/tasks", body.clone()).await;
    let id = task_id(&first).to_owned();
    let accepted = task(&f.controller, &id).await;
    assert_eq!(accepted.input.definition, definition("coding-default"));
    let original = operation(&f.controller, "explicit-browser-original").await;
    assert_eq!(original.intended["operation"], "submit_task");
    assert_eq!(original.intended["input"]["input"]["definition"], serde_json::to_value(&accepted.input.definition).unwrap());
    let selected_form = ui(&f.controller, "GET", "/tasks/new?agent_id=coding-default", vec![]).await;
    assert_eq!(selected_form.status, 200, "{}", selected_form.body);
    assert!(selected_form.body.contains("name=\"definition_id\" type=\"text\" value=\"coding-default\""));
    assert!(selected_form.body.contains("name=\"definition_revision\" type=\"text\" value=\"revision-1\""));
    let replay = ui(&f.controller, "POST", "/tasks", body).await;
    assert_eq!(replay.location, first.location);
    assert_eq!(operation(&f.controller, "explicit-browser-original").await, original);
    for (suffix, extra, message) in [
        ("id", "&definition_id=coding-default", "Specify both agent ID and immutable revision"),
        ("revision", "&definition_revision=revision-1", "Specify both agent ID and immutable revision"),
        ("mixed", "&executor=coding", "Unknown form field"),
    ] {
        let mut body = omitted(&format!("bad-assignment-{suffix}"), "Do not silently replace partial assignment");
        body.extend_from_slice(extra.as_bytes());
        let refused = ui(&f.controller, "POST", "/tasks", body).await;
        assert_eq!(refused.status, 400, "{}", refused.body);
        assert!(refused.body.contains(message), "{}", refused.body);
        let reply = Box::pin(f.controller.handle(Request {
            protocol_revision: WIRE_REVISION, request_id: format!("lookup-{suffix}"),
            command: Operation::GetOperation { operation_id: format!("bad-assignment-{suffix}") },
        })).await;
        assert!(matches!(reply.outcome, Outcome::Refused { code, .. } if code == "not_found"));
    }
    assert_eq!(task(&f.controller, &id).await, accepted);
    assert!(matches!(call(&f.controller, Operation::ListTasks { after: None, limit: 10 }).await,
        Data::Tasks(tasks) if tasks.len() == 1 && tasks[0].id == id));
}

fn rendered_value(page: &UiResponse, name: &str) -> String {
    page.body.split(&format!("name=\"{name}\""))
        .nth(1).expect("rendered field")
        .split("value=\"").nth(1).expect("rendered value")
        .split('"').next().unwrap().into()
}

#[tokio::test]
//= spec/impl/v2/implementation.md#default-task-assignment
//= type=test
//# A new root task submitted without an explicit agent MUST resolve the configured default agent identity.
//= spec/impl/v2/implementation.md#default-task-assignment
//= type=test
//# The default task agent MUST be selected through metadata.
async fn ordinary_form_submits_a_native_default_and_preserves_a_refused_draft_identity() {
    let mut native = definition("product-native-default");
    native.executor = ExecutorKind::Native;
    let f = fixture(configured(Some(&native.id), "product-native-default-r1")).await;
    register(&f.controller, &native).await;
    let page = ui(&f.controller, "GET", "/tasks/new", vec![]).await;
    assert_eq!(page.status, 200, "{}", page.body);
    let mut fields: Vec<(&str, String)> = ["operation_id", "model_requests", "deadline_unix_ms", "output_bytes", "result_bytes", "max_delegation_depth"]
        .into_iter().map(|name| (name, rendered_value(&page, name))).collect();
    let operation_id = fields[0].1.clone();
    fields.push(("instruction", "Return a structured analysis".into()));
    fields.push(("output_schema", "invalid JSON".into()));
    let refused = ui(&f.controller, "POST", "/tasks", form(&fields)).await;
    assert_eq!(refused.status, 400, "{}", refused.body);
    assert!(refused.body.contains("Invalid output schema JSON"));
    assert_eq!(rendered_value(&refused, "operation_id"), operation_id);
    for name in ["model_requests", "deadline_unix_ms", "output_bytes", "result_bytes", "max_delegation_depth"] {
        assert_eq!(rendered_value(&refused, name), rendered_value(&page, name));
    }
    assert!(!refused.body.contains("name=\"definition_id\""));
    assert_no_work(&f.controller).await;
    fields.last_mut().unwrap().1 = "{\"type\":\"object\"}".into();
    let body = form(&fields);
    let submitted = ui(&f.controller, "POST", "/tasks", body.clone()).await;
    let accepted = task(&f.controller, task_id(&submitted)).await;
    assert_eq!(accepted.input.definition, native);
    assert!(accepted.input.repository.is_none());
    assert_eq!(accepted.input.output_schema, Some(serde_json::json!({"type":"object"})));
    assert_eq!(accepted.input.limits.model_requests.to_string(), rendered_value(&page, "model_requests"));
    let record = operation(&f.controller, &operation_id).await;
    assert_eq!(record.intended["operation"], "submit_task_draft");
    assert!(record.intended["input"]["input"].as_object().unwrap().get("definition").is_none());
    assert_eq!(ui(&f.controller, "POST", "/tasks", body).await.location, submitted.location);
    assert_eq!(operation(&f.controller, &operation_id).await, record);
}

#[tokio::test]
// Traceability scope: Real browser/controller submission preserves explicit target configuration, exact root limits and original operation replay without creating or executing a child.
//= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
//= type=test
//# A reassigned session MUST bind the selected target agent's accepted definition revision.
//= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
//= type=test
//# A task reassignment MUST preserve the task's accepted deadline.
async fn ordinary_browser_submits_explicit_same_task_handoff_scope_without_child_defaults() {
    let mut native = definition("browser-triage"); native.executor = ExecutorKind::Native;
    let coding = definition("browser-coding");
    let f = fixture(configured(Some(&native.id), "browser-handoff-r1")).await;
    register(&f.controller, &native).await; register(&f.controller, &coding).await;
    let page = ui(&f.controller, "GET", "/tasks/new", vec![]).await;
    assert_eq!(page.status, 200);
    assert!(page.body.contains("Reassignment example") && page.body.contains("no child or return continuation"));
    let mut fields: Vec<(&str, String)> = ["operation_id", "model_requests", "deadline_unix_ms", "output_bytes", "result_bytes", "max_delegation_depth"]
        .into_iter().map(|name| (name, rendered_value(&page, name))).collect();
    let original_id = fields[0].1.clone();
    let limits = Limits {
        deadline_unix_ms: rendered_value(&page, "deadline_unix_ms").parse().unwrap(),
        model_requests: rendered_value(&page, "model_requests").parse().unwrap(),
        output_bytes: rendered_value(&page, "output_bytes").parse().unwrap(),
        result_bytes: rendered_value(&page, "result_bytes").parse().unwrap(),
        max_delegation_depth: rendered_value(&page, "max_delegation_depth").parse().unwrap(),
    };
    let target = CodingTaskTemplate { instruction_source: None, pull_request: None, instruction_prefix: String::new(),
        definition: coding.clone(), repository: Repository { registration: Some(borg_runtime::repository_catalogue::Revision {
            repository_id: "product-fixture-repository".into(), registration_revision: "r1".into() }),
            url: String::new(), start_commit: "1".repeat(40), checkpoint_remote: String::new(), pull_request_target: None },
        acceptance_commands: vec![vec!["true".into()]], dependencies: vec![], limits: limits.clone() };
    let workflow = NativeWorkflow::Reassign { target };
    fields.push(("instruction", "Keep the same work and result obligations".into()));
    fields.push(("output_schema", "{\"type\":\"object\"}".into()));
    fields.push(("native_workflow", serde_json::to_string(&workflow).unwrap()));
    let mut incomplete = serde_json::to_value(&workflow).unwrap();
    incomplete["target"]["limits"].as_object_mut().unwrap().remove("model_requests");
    let mut refused_fields = fields.clone(); refused_fields.last_mut().unwrap().1 = incomplete.to_string();
    let refused = ui(&f.controller, "POST", "/tasks", form(&refused_fields)).await;
    assert_eq!(refused.status, 400, "{}", refused.body);
    assert_eq!(rendered_value(&refused, "operation_id"), original_id);
    assert_no_work(&f.controller).await;
    let body = form(&fields);
    let submitted = ui(&f.controller, "POST", "/tasks", body.clone()).await;
    let accepted = task(&f.controller, task_id(&submitted)).await;
    assert_eq!(accepted.input.definition, native);
    assert_eq!(accepted.input.limits, limits);
    assert_eq!(accepted.input.output_schema, Some(serde_json::json!({"type":"object"})));
    assert_eq!(accepted.input.instruction, "Keep the same work and result obligations");
    let Some(NativeWorkflow::Reassign { target: accepted_target }) = accepted.input.native_workflow.as_deref() else { panic!("missing explicit handoff scope") };
    assert_eq!(accepted_target.definition, coding); assert_eq!(accepted_target.limits, accepted.input.limits);
    assert!(accepted_target.instruction_prefix.is_empty());
    assert_eq!(accepted_target.dependencies, accepted.input.dependencies);
    assert!(accepted.parent_task_id.is_none() && accepted.waiting_child_id.is_none() && accepted.completed_child.is_none());
    let original = operation(&f.controller, &original_id).await;
    assert!(original.intended["input"]["input"].as_object().unwrap().get("definition").is_none());
    assert_eq!(original.intended["input"]["input"]["native_workflow"], serde_json::to_value(workflow).unwrap());
    assert_eq!(ui(&f.controller, "POST", "/tasks", body).await.location, submitted.location);
    assert_eq!(operation(&f.controller, &original_id).await, original);
    assert!(matches!(call(&f.controller, Operation::ListTasks { after: None, limit: 10 }).await,
        Data::Tasks(tasks) if tasks.len() == 1 && tasks[0].id == accepted.id));
}
