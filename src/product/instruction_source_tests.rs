use super::*;
use crate::repository_view::SourceSelector;
use serde_json::{json, Value};

const PRIMARY: &str = "1111111111111111111111111111111111111111";
const SPEC: &str = "2222222222222222222222222222222222222222";

fn fields(metadata: &RuntimeLimits) -> Form {
    let mut values = create_defaults(ExecutorKind::Coding, metadata).unwrap();
    for (key, value) in [
        ("operation_id", "instruction-submit"), ("instruction", "Read the accepted instructions"),
        ("definition_id", "instruction-agent"), ("definition_revision", "one"),
        ("repository_id", "primary"), ("registration_revision", "primary-one"),
        ("start_commit", PRIMARY),
    ] {
        values.insert(key.into(), value.into());
    }
    values
}
fn bind(values: &mut Form) {
    for (key, value) in [
        ("instruction_source_id", "specification"),
        ("instruction_source_revision", "spec-one"),
        ("instruction_source_commit", SPEC),
    ] {
        values.insert(key.into(), value.into());
    }
}
fn workflow(input: &TaskInput) -> Value {
    json!({"kind":"coding_then_continue", "child": {
        "instruction_prefix":"Read the configured source", "definition":input.definition,
        "repository":input.repository, "acceptance_commands":input.acceptance_commands,
        "dependencies":input.dependencies, "limits":input.limits
    }})
}
fn task(input: TaskInput) -> Task {
    Task {
        id: "instruction-task".into(), session_id: "instruction-session".into(),
        assigned_agent_id: input.definition.id.clone(), input, status: TaskStatus::Queued,
        revision: 1, attempt: None, selected_checkpoint: None, result: None,
        blocked_reason: None, pending_assignment: None, native_retry_block: None, progress: String::new(),
        recent_output: String::new(), output_truncated: false, parent_task_id: None,
        waiting_child_id: None, completed_child: None,
    }
}

#[test]
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//= type=test
//# A task MUST have at most one primary repository association.
fn instruction_forms_preserve_absent_bytes_and_explicit_profile_and_primary() {
    let metadata = RuntimeLimits::seed().unwrap();
    for profile in ["collaboration_v1", "collaboration_v2"] {
        let mut values = fields(&metadata);
        values.insert("execution_profile".into(), profile.into());
        let absent = task_input(&values, &metadata).unwrap();
        let encoded = serde_json::to_vec(&absent).unwrap();
        assert!(serde_json::to_value(&absent).unwrap().get("instruction_source").is_none());
        for key in ["instruction_source_id", "instruction_source_revision", "instruction_source_commit"] {
            values.insert(key.into(), String::new());
        }
        assert_eq!(serde_json::to_vec(&task_input(&values, &metadata).unwrap()).unwrap(), encoded);
        bind(&mut values);
        let mut bound = task_input(&values, &metadata).unwrap();
        let source = bound.instruction_source.take().unwrap();
        assert_eq!(source.registration.unwrap(), crate::repository_catalogue::Revision {
            repository_id: "specification".into(), registration_revision: "spec-one".into(),
        });
        assert!(source.repository.is_empty());
        assert_eq!(source.start_commit, SPEC);
        assert_eq!(bound, absent, "binding changed an existing task field");
        assert_eq!(serde_json::to_vec(&bound).unwrap(), encoded);
    }
}

#[test]
fn instruction_forms_refuse_partial_native_unconfigured_and_endpoint_overrides() {
    let metadata = RuntimeLimits::seed().unwrap();
    let mut complete = fields(&metadata);
    bind(&mut complete);
    for key in ["instruction_source_id", "instruction_source_revision", "instruction_source_commit"] {
        let mut incomplete = complete.clone();
        incomplete.remove(key);
        assert!(task_input(&incomplete, &metadata).is_err(), "missing {key}");
    }
    for commit in ["main".to_owned(), "A".repeat(40), "a".repeat(39), "a".repeat(65)] {
        let mut invalid = complete.clone();
        invalid.insert("instruction_source_commit".into(), commit);
        assert_eq!(task_input(&invalid, &metadata).unwrap_err(), "An instruction source requires an exact lowercase commit");
    }
    for key in ["instruction_source_id", "instruction_source_revision"] {
        let mut bounded = complete.clone();
        bounded.insert(key.into(), "a".repeat(metadata.transport.identity_bytes));
        assert!(task_input(&bounded, &metadata).is_ok());
        bounded.insert(key.into(), "a".repeat(metadata.transport.identity_bytes + 1));
        assert!(task_input(&bounded, &metadata).unwrap_err().contains("transport.identity_bytes"));
    }
    for profile in ["", "implicit", "none"] {
        let mut invalid = complete.clone();
        invalid.insert("execution_profile".into(), profile.into());
        assert!(task_input(&invalid, &metadata).is_err());
    }
    let mut invalid = complete.clone();
    invalid.insert("instruction_source_repository".into(), "https://user:secret@private.invalid/spec".into());
    assert!(task_input(&invalid, &metadata).is_err());
    let mut native = complete;
    native.insert("executor".into(), "native".into());
    native.insert("output_schema".into(), "{\"type\":\"integer\"}".into());
    native.insert("acceptance_commands".into(), "[]".into());
    for key in ["repository_id", "registration_revision", "start_commit"] { native.remove(key); }
    assert_eq!(task_input(&native, &metadata).unwrap_err(), "An instruction source requires a coding task with an explicit collaboration profile");
    for key in ["instruction_source_id", "instruction_source_revision", "instruction_source_commit"] { native.remove(key); }
    for (key, value) in [("repository_read_id", "native-source"), ("repository_read_revision", "native-one"), ("repository_read_commit", PRIMARY)] {
        native.insert(key.into(), value.into());
    }
    let input = task_input(&native, &metadata).unwrap();
    assert!(input.instruction_source.is_none());
    assert_eq!(input.repository_read.unwrap().start_commit, PRIMARY);
}

#[test]
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST present the information exposed through the shared agent interface.
fn instruction_child_json_preserves_binding_and_refuses_server_endpoint_redisplay() {
    let metadata = RuntimeLimits::seed().unwrap();
    let input = task_input(&fields(&metadata), &metadata).unwrap();
    let mut value = workflow(&input);
    let decode = |value: &Value| task_defaults::native_workflow(&value.to_string(), input.limits.model_requests, false, &metadata);
    let absent = decode(&value).unwrap().unwrap();
    assert_eq!(serde_json::to_value(&absent).unwrap(), value);
    value["child"]["instruction_source"] = json!({
        "registration":{"repository_id":"specification","registration_revision":"spec-one"},
        "repository":"", "start_commit":SPEC
    });
    let decoded = decode(&value).unwrap().unwrap();
    assert_eq!(serde_json::to_value(&decoded).unwrap(), value);
    assert!(task_defaults::may_redisplay_native_workflow(&value.to_string()));
    let mut parent = input.clone();
    parent.definition.executor = ExecutorKind::Native;
    parent.repository = None;
    parent.native_workflow = Some(decoded);
    assert!(parent.instruction_source.is_none());
    assert!(!repository::instruction_source_enabled(&parent));
    assert!(repository::selected("instructions", &parent).is_err());
    for endpoint in ["/private/spec.git", "https://user:secret@private.invalid/spec"] {
        let mut invalid = value.clone();
        invalid["child"]["instruction_source"]["repository"] = json!(endpoint);
        assert!(decode(&invalid).is_err());
        assert!(!task_defaults::may_redisplay_native_workflow(&invalid.to_string()));
    }
    for field in ["registration", "start_commit"] {
        let mut invalid = value.clone();
        invalid["child"]["instruction_source"].as_object_mut().unwrap().remove(field);
        assert!(decode(&invalid).is_err(), "missing {field}");
    }
    let mut invalid = value.clone();
    invalid["child"]["definition"]["execution_profile"] = Value::Null;
    assert!(decode(&invalid).is_err());
    invalid = value;
    invalid["child"]["instruction_source"]["source_store_key"] = json!("private.git");
    assert!(decode(&invalid).is_err());
    assert!(!task_defaults::may_redisplay_native_workflow(&invalid.to_string()));
}

#[test]
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST present the information exposed through the shared agent interface.
fn instruction_source_views_are_bound_escaped_and_leave_absent_selectors_exact() {
    let metadata = RuntimeLimits::seed().unwrap();
    let mut values = fields(&metadata);
    let absent = task_input(&values, &metadata).unwrap();
    assert_eq!(repository::source_select(&absent, "source", "Accepted source", "start"),
        "<label>Accepted source<select name=\"source\"><option value=\"start\" selected>Start</option><option value=\"checkpoint\" >Acknowledged checkpoint</option></select></label>");
    assert_eq!(repository::selected("start", &absent).unwrap(), SourceSelector::Start);
    assert_eq!(repository::selected("checkpoint", &absent).unwrap(), SourceSelector::Checkpoint);
    assert!(repository::selected("instructions", &absent).is_err());
    assert!(!render_task(&task(absent), false).body.contains("Read accepted instructions"));
    bind(&mut values);
    let mut bound = task_input(&values, &metadata).unwrap();
    bound.instruction_source.as_mut().unwrap().registration.as_mut().unwrap().repository_id = "spec<unsafe>&".into();
    bound.instruction_source.as_mut().unwrap().repository = "https://private.invalid/spec.git".into();
    let original = task(bound.clone());
    let before = original.clone();
    let body = render_task(&original, false).body;
    let summary = body.split_once("<dl>").unwrap().1.split_once("</dl>").unwrap().0;
    assert!(summary.contains("spec&lt;unsafe&gt;&amp;") && summary.contains("spec-one") && summary.contains(SPEC));
    assert!(summary.contains("/repository?source=instructions"));
    assert!(!summary.contains("private.invalid"));
    assert!(!body.contains("spec<unsafe>"));
    // The existing configuration panel retains the complete accepted input as escaped JSON.
    let configuration = body.split_once("<details><summary>Accepted task configuration</summary>").unwrap().1
        .split_once("<pre>").unwrap().1.split_once("</pre>").unwrap().0;
    assert_eq!(configuration, escape(&serde_json::to_string_pretty(&original.input).unwrap()));
    assert_eq!(original, before);
    assert_eq!(repository::selected("instructions", &bound).unwrap(), SourceSelector::Instructions);
    assert!(repository::source_select(&bound, "source", "Accepted source", "instructions")
        .contains("<option value=\"instructions\" selected>Instructions</option>"));
    for value in ["/private/spec.git", "https://private.invalid/spec", "latest"] {
        assert!(repository::selected(value, &bound).is_err());
    }
    bound.definition.execution_profile = None;
    assert!(repository::selected("instructions", &bound).is_err());
    assert!(!repository::source_select(&bound, "source", "Accepted source", "start").contains("instructions"));
}

#[tokio::test]
async fn instruction_invalid_post_preserves_safe_tuple_and_original_identity_without_echoing_endpoint() {
    let root = std::env::temp_dir().join(format!("borg-instruction-form-{}", identity()));
    std::fs::create_dir(&root).unwrap();
    let controller = Controller::open_with_config(root.join("tasks.sqlite"), crate::controller::ControllerConfig::default()).unwrap();
    let mut values = fields(controller.limits());
    bind(&mut values);
    values.insert("model".into(), "unsupported".into());
    let response = Box::pin(post(&controller, "/tasks", values.clone())).await;
    assert_eq!(response.status, 400);
    for (key, value) in [("operation_id", "instruction-submit"), ("instruction_source_id", "specification"),
        ("instruction_source_revision", "spec-one"), ("instruction_source_commit", SPEC)] {
        let expected = if key == "operation_id" { hidden(key, value) }
            else { format!("name=\"{key}\" type=\"text\" value=\"{value}\"") };
        assert!(response.body.contains(&expected), "missing {key}");
    }
    let input = task_input(&fields(controller.limits()), controller.limits()).unwrap();
    let mut child = workflow(&input);
    child["child"]["instruction_source"] = json!({
        "registration":{"repository_id":"specification","registration_revision":"spec-one"},
        "repository":"https://user:secret@private.invalid/spec", "start_commit":SPEC
    });
    values.insert("native_workflow".into(), child.to_string());
    values.insert("instruction_source_repository".into(), "https://user:secret@private.invalid/spec".into());
    let refused = Box::pin(post(&controller, "/tasks", values)).await;
    assert_eq!(refused.status, 400);
    for secret in ["user:secret", "private.invalid", "source_store_key"] { assert!(!refused.body.contains(secret)); }
    assert!(refused.body.contains("specification") && refused.body.contains(SPEC));
    let absent = Box::pin(call(&controller, Operation::GetOperation { operation_id: "instruction-submit".into() })).await;
    assert!(matches!(absent.outcome, Outcome::Refused { ref code, .. } if code == "not_found"));
    drop(controller);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST support task submission.
async fn instruction_post_resolves_only_registered_sources_and_replays_original_task() {
    let root = std::env::temp_dir().join(format!("borg-instruction-post-{}", identity()));
    std::fs::create_dir(&root).unwrap();
    let controller = Controller::open_with_config(root.join("tasks.sqlite"), crate::controller::ControllerConfig::default()).unwrap();
    for (id, revision) in [("primary", "primary-one"), ("specification", "spec-one")] {
        let registered = Box::pin(call(&controller, Operation::RegisterRepository {
            operation_id: format!("register-{id}"),
            registration: crate::repository_catalogue::Registration {
                pull_request_target: None, repository_id: id.into(), registration_revision: revision.into(),
                display_name: id.into(), source_endpoint: format!("/fixture/{id}.git"),
                checkpoint_endpoint: format!("/fixture/{id}-checkpoints.git"),
                source_store_key: format!("{id}.git"), checkpoint_store_key: format!("{id}-checkpoints.git"),
            },
        })).await;
        assert!(matches!(registered.outcome, Outcome::Ok { data: Data::RepositoryRegistration(_) }), "{registered:?}");
    }
    let mut values = fields(controller.limits());
    bind(&mut values);
    let first = Box::pin(post(&controller, "/tasks", values.clone())).await;
    assert_eq!(first.status, 303, "{}", first.body);
    let original = Box::pin(call(&controller, Operation::GetOperation { operation_id: "instruction-submit".into() })).await;
    let Outcome::Ok { data: Data::Operation(ref record) } = original.outcome else { panic!("original task receipt absent: {original:?}"); };
    assert_eq!(record.intended, serde_json::to_value(Operation::SubmitTask {
        operation_id: "instruction-submit".into(), input: task_input(&values, controller.limits()).unwrap(),
    }).unwrap());
    let retained: Outcome = serde_json::from_value(record.outcome.clone().unwrap()).unwrap();
    let Outcome::Ok { data: Data::Task(accepted) } = retained else { panic!("submission did not retain its task"); };
    let source = accepted.input.instruction_source.as_ref().unwrap();
    assert_eq!(source.start_commit, SPEC);
    assert_eq!(source.repository, "/fixture/specification.git");
    assert_eq!(source.registration.as_ref().unwrap().registration_revision, "spec-one");
    let primary = accepted.input.repository.as_ref().unwrap();
    assert_eq!(primary.start_commit, PRIMARY);
    assert_eq!(primary.url, "/fixture/primary.git");
    assert_eq!(primary.checkpoint_remote, "/fixture/primary-checkpoints.git");
    let page = render_task(&accepted, false);
    let summary = page.body.split_once("<dl>").unwrap().1.split_once("</dl>").unwrap().0;
    assert!(summary.contains("spec-one") && summary.contains(SPEC));
    assert!(!summary.contains("/fixture/specification.git"));
    let configuration = page.body.split_once("<details><summary>Accepted task configuration</summary>").unwrap().1
        .split_once("<pre>").unwrap().1.split_once("</pre>").unwrap().0;
    assert_eq!(configuration, escape(&serde_json::to_string_pretty(&accepted.input).unwrap()));
    let replay = Box::pin(post(&controller, "/tasks", values)).await;
    assert_eq!(replay.status, first.status);
    assert_eq!(replay.location, first.location);
    let recovered = Box::pin(call(&controller, Operation::GetOperation { operation_id: "instruction-submit".into() })).await;
    assert_eq!(recovered.outcome, original.outcome);
    assert_eq!(recovered.commit, original.commit);
    drop(controller);
    std::fs::remove_dir_all(root).unwrap();
}
