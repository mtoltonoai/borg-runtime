use super::*;
use serde_json::json;

fn omit_allowance(body: Vec<u8>) -> Vec<u8> {
    String::from_utf8(body)
        .unwrap()
        .split('&')
        .filter(|entry| !entry.starts_with("model_requests="))
        .collect::<Vec<_>>()
        .join("&")
        .into_bytes()
}
fn set_allowance(body: Vec<u8>, value: &str) -> Vec<u8> {
    let mut body = omit_allowance(body);
    body.extend_from_slice(format!("&model_requests={}", encode(value)).as_bytes());
    body
}

#[tokio::test]
//= spec/impl/v2/implementation.md#initial-model-budget-policy
//= type=test
//# The default task model-request allowance MUST be the maximum finite value supported by the task representation.
async fn new_operator_tasks_use_maximum_allowance_and_legacy_explicit_replay_is_unchanged() {
    let fixture = Fixture::new().await;
    assert_eq!(
        borg_runtime::limits::RuntimeLimits::seed()
            .unwrap()
            .task_defaults
            .model_requests,
        u32::MAX
    );
    for query in ["/tasks/new?executor=coding", "/tasks/new?executor=native"] {
        let form = ui(&fixture.controller, "GET", query, vec![]).await;
        assert_eq!(form.status, 200);
        assert!(form.body.contains(&format!(
            "type=\"hidden\" name=\"model_requests\" value=\"{}\"",
            fixture.controller.limits().task_defaults.model_requests
        )));
        assert!(
            form.body
                .contains("selected metadata model-request allowance")
        );
        for field in [
            "deadline_unix_ms",
            "output_bytes",
            "result_bytes",
            "max_delegation_depth",
        ] {
            assert!(form.body.contains(&format!("name=\"{field}\"")));
        }
    }
    let deadline = now() + 3_600_000;
    let direct = json!({"kind":"inspect_current_task_then_continue"}).to_string();
    let inputs = [
        omit_allowance(submission("default-coding", "bounded source work", "")),
        omit_allowance(native_submission("default-native", deadline, None)),
    ];
    let mut originals = Vec::new();
    for body in inputs {
        let created = ui(&fixture.controller, "POST", "/tasks", body.clone()).await;
        let stored = task(&fixture.controller, task_id(&created)).await;
        assert_eq!(stored.input.limits.model_requests, u32::MAX);
        assert_eq!(stored.input.limits.output_bytes, 4096);
        let replay = ui(&fixture.controller, "POST", "/tasks", body.clone()).await;
        assert_eq!(replay.location, created.location);
        originals.push((body, stored));
    }
    let old_workflow = ui(
        &fixture.controller,
        "POST",
        "/tasks",
        omit_allowance(native_submission("default-tool", deadline, Some(&direct))),
    )
    .await;
    assert_eq!(old_workflow.status, 400);
    assert!(old_workflow.body.contains("normal collaboration tools"));
    let old_body = submission("legacy-explicit", "legacy fixed allowance", "");
    let invalid_legacy = String::from_utf8(old_body.clone())
        .unwrap()
        .replace("executor=coding", "executor=unknown")
        .into_bytes();
    let redisplayed = ui(&fixture.controller, "POST", "/tasks", invalid_legacy).await;
    assert_eq!(redisplayed.status, 400);
    assert!(
        redisplayed
            .body
            .contains("type=\"hidden\" name=\"model_requests\" value=\"5\"")
    );
    let old = ui(&fixture.controller, "POST", "/tasks", old_body.clone()).await;
    let old_task = task(&fixture.controller, task_id(&old)).await;
    assert_eq!(old_task.input.limits.model_requests, 5);
    originals.push((old_body, old_task));
    let path = fixture.path.clone();
    drop(fixture.controller);
    let reopened = Controller::open_with_config(path, config()).unwrap();
    for (body, original) in originals {
        let replay = ui(&reopened, "POST", "/tasks", body).await;
        assert_eq!(task_id(&replay), original.id);
        assert_eq!(task(&reopened, &original.id).await, original);
    }
}

#[tokio::test]
async fn workflow_child_default_uses_largest_applicable_reservation_without_overflow_or_rewriting()
{
    let fixture = Fixture::new().await;
    let deadline = now() + 3_600_000;
    let explicit = workflow(deadline);
    let explicit_json = serde_json::to_string(&explicit).unwrap();
    let explicit_reply = ui(
        &fixture.controller,
        "POST",
        "/tasks",
        native_submission("explicit-template", deadline, Some(&explicit_json)),
    )
    .await;
    let explicit_task = task(&fixture.controller, task_id(&explicit_reply)).await;
    assert_eq!(explicit_task.input.limits.model_requests, 8);
    let mut resolved = explicit.clone();
    let NativeWorkflow::CodingThenContinue { child } = &mut resolved else {
        unreachable!()
    };
    child.repository.url = "file:///tmp/local-test-repository".into();
    child.repository.checkpoint_remote = "origin".into();
    assert_eq!(
        explicit_task.input.native_workflow,
        Some(Box::new(resolved))
    );
    let mut draft = serde_json::to_value(&explicit).unwrap();
    draft["child"]["limits"]
        .as_object_mut()
        .unwrap()
        .remove("model_requests");
    let draft_json = draft.to_string();
    let body = omit_allowance(native_submission(
        "max-workflow",
        deadline,
        Some(&draft_json),
    ));
    let reply = ui(&fixture.controller, "POST", "/tasks", body.clone()).await;
    let parent = task(&fixture.controller, task_id(&reply)).await;
    assert_eq!(parent.input.limits.model_requests, u32::MAX);
    let child =
        borg_runtime::delegation::coding_child(&parent.input, "controlled handoff instruction")
            .unwrap();
    assert_eq!(child.limits.model_requests, u32::MAX - 2);
    assert_eq!(
        u64::from(child.limits.model_requests) + 2,
        u64::from(parent.input.limits.model_requests)
    );
    borg_runtime::delegation::validate_workflow(&parent.input).unwrap();
    // Actual controller admission and reservation, not a saturating arithmetic
    // witness. The controlled Usage is not a provider/Codex measurement.
    call(
        &fixture.controller,
        Operation::RegisterWorker {
            operation_id: "max-workflow-worker".into(),
            worker: Worker {
                id: "max-worker".into(),
                incarnation: "one".into(),
                native_capacity: 2,
                coding_capacity: 1,
            },
        },
    )
    .await;
    let Data::Commands(commands) = call(
        &fixture.controller,
        Operation::AwaitCommands {
            operation_id: "max-workflow-await".into(),
            worker_id: "max-worker".into(),
            incarnation: "one".into(),
            acknowledged: vec![],
        },
    )
    .await
    else {
        panic!()
    };
    let attempt = commands
        .iter()
        .find_map(|command| match command {
            WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == parent.id => {
                Some(attempt)
            }
            _ => None,
        })
        .unwrap();
    report(
        &fixture.controller,
        attempt,
        Observation::Usage {
            native_diagnostic_reserved: false,
            model_requests: 1,
            input_tokens: None,
            output_tokens: None,
        },
    )
    .await;
    let Data::Task(delegated) = call(
        &fixture.controller,
        Operation::DelegateTask {
            operation_id: "max-workflow-delegate".into(),
            parent_task_id: parent.id.clone(),
            parent_attempt_id: attempt.id.clone(),
            generation: attempt.generation,
            child: (child.clone()).into(),
        },
    )
    .await
    else {
        panic!()
    };
    assert_eq!(delegated.input.limits.model_requests, u32::MAX - 2);
    let repeated = ui(&fixture.controller, "POST", "/tasks", body).await;
    assert_eq!(task_id(&repeated), parent.id);
    assert_eq!(
        task(&fixture.controller, &parent.id).await.input,
        parent.input
    );

    let mut overflowing = serde_json::to_value(&explicit).unwrap();
    overflowing["child"]["limits"]["model_requests"] = json!(u32::MAX);
    let mut beyond = serde_json::to_value(&explicit).unwrap();
    beyond["child"]["limits"]["model_requests"] = json!(u64::from(u32::MAX) + 1);
    let mut null = serde_json::to_value(&explicit).unwrap();
    null["child"]["limits"]["model_requests"] = json!(null);
    let duplicate = explicit_json.replace(
        "\"model_requests\":2",
        "\"model_requests\":2,\"model_requests\":3",
    );
    assert_ne!(duplicate, explicit_json);
    for (index, value) in [
        overflowing.to_string(),
        beyond.to_string(),
        null.to_string(),
        duplicate,
    ]
    .iter()
    .enumerate()
    {
        let rejected = ui(
            &fixture.controller,
            "POST",
            "/tasks",
            omit_allowance(native_submission(
                &format!("bad-maximum-{index}"),
                deadline,
                Some(value),
            )),
        )
        .await;
        if index == 0 {
            assert_recorded_policy_refusal(
                &fixture.controller,
                &format!("bad-maximum-{index}"),
                &rejected,
            )
            .await;
        } else {
            assert_eq!(rejected.status, 400, "{}", rejected.body);
        }
    }
    let too_small = ui(
        &fixture.controller,
        "POST",
        "/tasks",
        set_allowance(
            native_submission("no-child-room", deadline, Some(&draft_json)),
            "2",
        ),
    )
    .await;
    assert_eq!(too_small.status, 400);
    let zero = ui(
        &fixture.controller,
        "POST",
        "/tasks",
        set_allowance(native_submission("zero-allowance", deadline, None), "0"),
    )
    .await;
    assert_recorded_policy_refusal(&fixture.controller, "zero-allowance", &zero).await;
    assert_eq!(
        task(&fixture.controller, &explicit_task.id).await.input,
        explicit_task.input
    );
}
