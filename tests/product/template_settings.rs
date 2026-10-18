use super::*;
use serde_json::json;

fn fields(body: Vec<u8>) -> Vec<(String, String)> {
    String::from_utf8(body)
        .unwrap()
        .split('&')
        .map(|v| {
            let (k, v) = v.split_once('=').unwrap();
            (k.into(), v.into())
        })
        .collect()
}
fn replace(body: Vec<u8>, key: &str, value: Option<&str>) -> Vec<u8> {
    let mut f = fields(body);
    f.retain(|(k, _)| k != key);
    if let Some(v) = value {
        f.push((key.into(), encode(v)));
    }
    f.into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
        .into_bytes()
}
fn legacy(mut body: Vec<u8>) -> Vec<u8> {
    for key in ["model", "reasoning_effort", "execution_profile"] {
        body = replace(body, key, None);
    }
    body
}
fn read_scope(body: Vec<u8>, repository: &str, commit: &str) -> Vec<u8> {
    replace(
        replace(
            replace(body, "repository_read_id", Some(repository)),
            "repository_read_revision",
            Some("r1"),
        ),
        "repository_read_commit",
        Some(commit),
    )
}

#[tokio::test]
async fn forms_bind_selected_settings_to_immutable_template_and_preserve_exact_replay() {
    let f = Fixture::new().await;
    for kind in ["native", "coding"] {
        let page = ui(
            &f.controller,
            "GET",
            &format!("/tasks/new?executor={kind}"),
            vec![],
        )
        .await;
        assert!(page.body.contains("value=\"gpt-6-astra\" selected"));
        assert!(page.body.contains("value=\"max\" selected"));
        assert!(
            page.body
                .contains("name=\"execution_profile\" value=\"collaboration_v1\"")
        );
        assert!(page.body.contains("immutable agent revision"));
    }
    let body = native_submission("immutable-settings", now() + 3_600_000, None);
    let reply = ui(&f.controller, "POST", "/tasks", body.clone()).await;
    let original = task(&f.controller, task_id(&reply)).await;
    assert_eq!(
        original.input.definition.inference,
        Some(InferenceSettings::selected())
    );
    assert_eq!(
        original.input.definition.execution_profile,
        Some(ExecutionProfile::CollaborationV1 { web_search: None })
    );
    let mut different = original.input.clone();
    // New submissions cannot assert the server-owned accepted snapshot.
    // Keep this case focused on immutable agent-definition reuse.
    different.resolved_limits = None;
    different.definition.execution_profile = Some(ExecutionProfile::CollaborationV1 {
        web_search: Some(NativeSearchProfile { max_tool_calls: 1 }),
    });
    let conflict = f
        .controller
        .handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: "settings-conflict".into(),
            command: Operation::SubmitTask {
                operation_id: "settings-conflict".into(),
                input: different,
            },
        })
        .await;
    assert!(
        matches!(conflict.outcome,Outcome::Refused{ref code,..} if code=="definition_conflict"),
        "{conflict:?}"
    );
    assert_eq!(task(&f.controller, &original.id).await, original);
    let path = f.path.clone();
    drop(f.controller);
    let reopened = Controller::open_with_config(path, config()).unwrap();
    let replay = ui(&reopened, "POST", "/tasks", body).await;
    assert_eq!(task_id(&replay), original.id);
    assert_eq!(task(&reopened, &original.id).await, original);
}

#[tokio::test]
async fn invalid_or_missing_settings_refuse_without_redisplay_defaults_or_worker_override() {
    let f = Fixture::new().await;
    for (index, (key, value)) in [
        ("model", Some("gpt-6.1-sol")),
        ("reasoning_effort", Some("ultra")),
        ("execution_profile", Some("unknown")),
        ("model", None),
        ("reasoning_effort", None),
        ("execution_profile", None),
    ]
    .into_iter()
    .enumerate()
    {
        let body = replace(
            native_submission(&format!("bad-setting-{index}"), now() + 3_600_000, None),
            key,
            value,
        );
        let response = ui(&f.controller, "POST", "/tasks", body).await;
        assert_eq!(response.status, 400, "{}", response.body);
        if let Some(value) = value {
            assert!(
                response.body.contains(&format!("value=\"{value}\"")),
                "{}",
                response.body
            );
        }
        if key == "execution_profile" && value.is_none() {
            assert!(!response.body.contains("name=\"execution_profile\""));
        }
    }
    let old = legacy(native_submission(
        "fresh-old-unbound",
        now() + 3_600_000,
        None,
    ));
    let rejected = ui(&f.controller, "POST", "/tasks", old.clone()).await;
    assert_eq!(rejected.status, 400, "{}", rejected.body);
    assert!(rejected.body.contains("model"));
    let invalid = replace(old, "executor", Some("invalid"));
    let redisplay = ui(&f.controller, "POST", "/tasks", invalid).await;
    assert_eq!(redisplay.status, 400);
    assert!(!redisplay.body.contains("name=\"execution_profile\""));
    assert!(!redisplay.body.contains("value=\"gpt-6-astra\" selected"));
    assert!(!redisplay.body.contains("value=\"max\" selected"));
    let unsupported = ui(
        &f.controller,
        "POST",
        "/tasks",
        replace(
            native_submission("worker-override", now() + 3_600_000, None),
            "worker_model",
            Some(SELECTED_MODEL),
        ),
    )
    .await;
    assert_eq!(unsupported.status, 400);
}

#[tokio::test]
async fn native_repository_reads_bind_paired_exact_revision_without_coding_or_child_inheritance() {
    let f = Fixture::new().await;
    for id in ["selected-repository", "analysis-repository"] {
        call(
            &f.controller,
            Operation::RegisterRepository {
                operation_id: format!("register-{id}"),
                registration: borg_runtime::repository_catalogue::Registration {
                    pull_request_target: None,
                    repository_id: id.into(),
                    registration_revision: "r1".into(),
                    display_name: id.into(),
                    source_endpoint: format!("file:///tmp/{id}"),
                    checkpoint_endpoint: "origin".into(),
                    source_store_key: format!("{id}.git"),
                    checkpoint_store_key: format!("{id}.git"),
                },
            },
        )
        .await;
    }
    let deadline = now() + 3_600_000;
    for width in [40, 64] {
        let body = read_scope(
            native_submission(&format!("read-{width}"), deadline, None),
            "selected-repository",
            &"a".repeat(width),
        );
        let reply = ui(&f.controller, "POST", "/tasks", body.clone()).await;
        let original = task(&f.controller, task_id(&reply)).await;
        assert_eq!(
            original.input.repository_read,
            Some(RepositoryReadSource {
                registration: Some(borg_runtime::repository_catalogue::Revision {
                    repository_id: "selected-repository".into(),
                    registration_revision: "r1".into(),
                }),
                repository: "file:///tmp/selected-repository".into(),
                start_commit: "a".repeat(width)
            })
        );
        assert!(original.input.repository.is_none());
        assert_eq!(
            task_id(&ui(&f.controller, "POST", "/tasks", body).await),
            original.id
        );
    }
    for (index, (repository, commit)) in [
        ("selected-repository", "".into()),
        ("", "a".repeat(40)),
        ("selected-repository", "main".into()),
        ("selected-repository", "A".repeat(40)),
    ]
    .into_iter()
    .enumerate()
    {
        let response = ui(
            &f.controller,
            "POST",
            "/tasks",
            read_scope(
                native_submission(&format!("bad-read-{index}"), deadline, None),
                repository,
                &commit,
            ),
        )
        .await;
        assert_eq!(response.status, 400, "{}", response.body);
        assert!(
            response
                .body
                .contains(&format!("name=\"operation_id\" value=\"bad-read-{index}\""))
        );
        assert!(
            response.body.contains("Missing repository_read")
                || response.body.contains("exact lowercase commit")
        );
    }
    let coding = ui(
        &f.controller,
        "POST",
        "/tasks",
        read_scope(
            submission("coding-extra-read", "edit selected repository", ""),
            "other-repository",
            &"b".repeat(40),
        ),
    )
    .await;
    assert_eq!(coding.status, 400);
    assert!(
        coding
            .body
            .contains("Coding tasks use their own repository")
    );
    let flow = serde_json::to_string(&workflow(deadline)).unwrap();
    let response = ui(
        &f.controller,
        "POST",
        "/tasks",
        read_scope(
            native_submission("read-parent", deadline, Some(&flow)),
            "analysis-repository",
            &"c".repeat(40),
        ),
    )
    .await;
    let parent = task(&f.controller, task_id(&response)).await;
    let child = borg_runtime::delegation::coding_child(&parent.input, "configured edit").unwrap();
    assert!(child.repository_read.is_none());
    assert_eq!(
        child.repository.as_ref().unwrap().url,
        "file:///tmp/local-test-repository"
    );
    assert_eq!(child.definition.id, "coding-child");
    assert_eq!(
        child.definition.inference,
        Some(InferenceSettings::selected())
    );
    assert_eq!(
        child.definition.execution_profile,
        Some(ExecutionProfile::CollaborationV1 { web_search: None })
    );
}

#[tokio::test]
async fn new_inspect_workflow_refuses_and_child_settings_are_explicit_not_inherited() {
    let f = Fixture::new().await;
    let deadline = now() + 3_600_000;
    let inspect = json!({"kind":"inspect_current_task_then_continue"}).to_string();
    // Historical wire decoding stays available; the new browser producer does
    // not create this incompatible fixed two-request workflow.
    assert!(matches!(
        serde_json::from_str::<NativeWorkflow>(&inspect).unwrap(),
        NativeWorkflow::InspectCurrentTaskThenContinue {}
    ));
    let response = ui(
        &f.controller,
        "POST",
        "/tasks",
        native_submission("new-inspect", deadline, Some(&inspect)),
    )
    .await;
    assert_eq!(response.status, 400);
    assert!(response.body.contains("normal collaboration tools"));
    for (index, key) in ["inference", "execution_profile"].into_iter().enumerate() {
        let mut child = serde_json::to_value(workflow(deadline)).unwrap();
        child["child"]["definition"]
            .as_object_mut()
            .unwrap()
            .remove(key);
        let response = ui(
            &f.controller,
            "POST",
            "/tasks",
            native_submission(
                &format!("missing-child-{index}"),
                deadline,
                Some(&child.to_string()),
            ),
        )
        .await;
        assert_eq!(response.status, 409, "{}", response.body);
        assert!(
            response.body.contains(if key == "inference" {
                "template_inference_unbound"
            } else {
                "template_profile_unbound"
            }),
            "{}",
            response.body
        );
    }
}
