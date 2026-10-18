use borg_runtime::controller::{Controller, ControllerConfig};
use borg_runtime::product::{UiRequest, UiResponse, handle};
use borg_runtime::protocol::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const ORIGIN: &str = "http://127.0.0.1:7080";
static NEXT: AtomicU64 = AtomicU64::new(1);

fn config() -> ControllerConfig {
    let mut limits = borg_runtime::limits::RuntimeLimits::seed().unwrap();
    // Keep widening and narrowing discriminators independent of the default ceiling.
    limits.execution.native.context_bytes = 65_536;
    limits.validate().unwrap();
    ControllerConfig {
        limits: std::sync::Arc::new(limits),
        validation_executable: PathBuf::from(env!("CARGO_BIN_EXE_borg-runtime")),
        ..ControllerConfig::default()
    }
}

struct Fixture {
    controller: Controller,
    path: PathBuf,
}

impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "borg-product-{}-{}-{}",
            std::process::id(),
            now(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let path = root.join("tasks.sqlite");
        let controller = Controller::open_with_config(&path, config()).unwrap();
        register_fixture_repository(&controller).await;
        Self { controller, path }
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn encode(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                char::from(b).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

fn form(fields: &[(&str, String)]) -> Vec<u8> {
    fields
        .iter()
        .map(|(key, value)| format!("{}={}", encode(key), encode(value)))
        .collect::<Vec<_>>()
        .join("&")
        .into_bytes()
}

fn submission(operation: &str, instruction: &str, dependencies: &str) -> Vec<u8> {
    form(&[
        ("operation_id", operation.into()),
        ("instruction", instruction.into()),
        ("definition_id", "coding-default".into()),
        ("definition_revision", "revision-1".into()),
        ("model", SELECTED_MODEL.into()),
        ("reasoning_effort", SELECTED_REASONING_EFFORT.into()),
        ("execution_profile", "collaboration_v1".into()),
        ("executor", "coding".into()),
        ("repository_id", "product-fixture-repository".into()),
        ("registration_revision", "r1".into()),
        (
            "start_commit",
            "1111111111111111111111111111111111111111".into(),
        ),
        (
            "acceptance_commands",
            "[[\"cargo\",\"test\",\"--locked\"]]".into(),
        ),
        ("output_schema", String::new()),
        ("dependencies", dependencies.into()),
        ("deadline_unix_ms", (now() + 3_600_000).to_string()),
        ("model_requests", "5".into()),
        ("output_bytes", "4096".into()),
        ("max_delegation_depth", "0".into()),
    ])
}

fn workflow(deadline: u64) -> NativeWorkflow {
    NativeWorkflow::CodingThenContinue {
        child: CodingTaskTemplate { instruction_source: None,
            pull_request: None,
            instruction_prefix: "Make the requested change and preserve existing tests.".into(),
            definition: Definition {
                charter: None,
                limit_overrides: None,
                id: "coding-child".into(),
                revision: "revision-1".into(),
                executor: ExecutorKind::Coding,
                inference: Some(InferenceSettings::selected()),
                execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
            },
            repository: Repository {
                pull_request_target: None,
                registration: Some(borg_runtime::repository_catalogue::Revision {
                    repository_id: "product-fixture-repository".into(),
                    registration_revision: "r1".into(),
                }),
                url: String::new(),
                start_commit: "1111111111111111111111111111111111111111".into(),
                checkpoint_remote: String::new(),
            },
            acceptance_commands: vec![vec!["cargo".into(), "test".into()]],
            dependencies: vec![],
            limits: Limits {
                deadline_unix_ms: deadline - 1000,
                model_requests: 2,
                output_bytes: 1024,
                result_bytes: 4096,
                max_delegation_depth: 1,
            },
        },
    }
}

fn native_submission(operation: &str, deadline: u64, workflow: Option<&str>) -> Vec<u8> {
    let mut fields = vec![
        ("operation_id", operation.into()),
        (
            "instruction",
            "Return the requested structured analysis.".into(),
        ),
        ("definition_id", "native-analysis".into()),
        ("definition_revision", "revision-1".into()),
        ("model", SELECTED_MODEL.into()),
        ("reasoning_effort", SELECTED_REASONING_EFFORT.into()),
        ("execution_profile", "collaboration_v1".into()),
        ("executor", "native".into()),
        ("acceptance_commands", "[]".into()),
        ("output_schema", "{\"type\":\"object\"}".into()),
        ("deadline_unix_ms", deadline.to_string()),
        ("model_requests", "8".into()),
        ("output_bytes", "4096".into()),
        ("max_delegation_depth", "2".into()),
    ];
    if let Some(workflow) = workflow {
        fields.push(("native_workflow", workflow.into()));
    }
    form(&fields)
}

async fn ui(controller: &Controller, method: &str, path: &str, body: Vec<u8>) -> UiResponse {
    // Keep the real route future off each long test's inline async state.
    // This changes only fixture allocation, not requests, polling or assertions.
    Box::pin(handle(
        controller,
        UiRequest {
            method: method.into(),
            path: path.into(),
            body,
            origin: Some(ORIGIN.into()),
            expected_origin: ORIGIN.into(),
        },
    ))
    .await
}

async fn task(controller: &Controller, id: &str) -> Task {
    // Keep the real controller future off the inline state of long UI tests.
    // Requests, scheduling, returned projections and assertions are unchanged.
    match Box::pin(controller.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: format!("read-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
        command: Operation::GetTask { task_id: id.into() },
    }))
    .await
    .outcome
    {
        Outcome::Ok {
            data: Data::Task(task),
        } => *task,
        other => panic!("expected task, got {other:?}"),
    }
}

async fn call(controller: &Controller, command: Operation) -> Data {
    // This is fixture allocation only, not a larger runtime stack or a bypass.
    let reply = Box::pin(controller.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: format!("fixture-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
        command,
    }))
    .await;
    match reply.outcome {
        Outcome::Ok { data } => data,
        other => panic!("fixture operation refused: {other:?}"),
    }
}

async fn register_fixture_repository(controller: &Controller) {
    call(
        controller,
        Operation::RegisterRepository {
            operation_id: "product-register-fixture".into(),
            registration: borg_runtime::repository_catalogue::Registration {
                pull_request_target: None,
                repository_id: "product-fixture-repository".into(),
                registration_revision: "r1".into(),
                display_name: "Product test repository".into(),
                source_endpoint: "file:///tmp/local-test-repository".into(),
                checkpoint_endpoint: "origin".into(),
                source_store_key: "product-source.git".into(),
                checkpoint_store_key: "product-checkpoint.git".into(),
            },
        },
    )
    .await;
}

#[tokio::test]
async fn repository_catalogue_forms_preserve_safe_metadata_replay_and_explicit_task_bindings() {
    use borg_runtime::repository_catalogue::Entry;
    async fn entry(controller: &Controller, id: &str, revision: Option<&str>) -> Entry {
        match call(
            controller,
            Operation::GetRepository {
                repository_id: id.into(),
                registration_revision: revision.map(str::to_owned),
            },
        )
        .await
        {
            Data::RepositoryRegistration(entry) => entry,
            other => panic!("expected registration, got {other:?}"),
        }
    }
    fn registration_body(
        operation: &str,
        id: &str,
        revision: &str,
        generation: Option<u64>,
    ) -> Vec<u8> {
        let mut fields = vec![
            ("operation_id", operation.into()),
            ("repository_id", id.into()),
            ("registration_revision", revision.into()),
            ("display_name", "<script>same label</script>".into()),
            (
                "source_endpoint",
                format!("https://example.test/{id}/{revision}"),
            ),
            (
                "checkpoint_endpoint",
                format!("https://example.test/{id}/checkpoint"),
            ),
            ("source_store_key", "private-source-key.git".into()),
            ("checkpoint_store_key", "private-checkpoint-key.git".into()),
        ];
        if let Some(generation) = generation {
            fields.push(("expected_generation", generation.to_string()));
        }
        form(&fields)
    }
    let fixture = Fixture::new().await;
    let c = &fixture.controller;
    let new_task = ui(c, "GET", "/tasks/new", vec![]).await;
    assert!(
        new_task
            .body
            .contains("name=\"repository_id\" type=\"text\" value=\"\"")
    );
    assert!(
        new_task
            .body
            .contains("name=\"registration_revision\" type=\"text\" value=\"\"")
    );
    assert!(!new_task.body.contains("name=\"repository\""));
    assert!(!new_task.body.contains("name=\"checkpoint_remote\""));
    let body = registration_body("register-a", "catalogue-a", "r1", None);
    let first = ui(c, "POST", "/repositories", body.clone()).await;
    assert_eq!(first.status, 303, "{}", first.body);
    assert_eq!(
        first.location.as_deref(),
        Some("/repositories/id/catalogue-a?registration_revision=r1")
    );
    let replay = ui(c, "POST", "/repositories", body).await;
    assert_eq!(replay.location, first.location);
    assert_eq!(entry(c, "catalogue-a", None).await.generation, 1);
    let second = ui(
        c,
        "POST",
        "/repositories",
        registration_body("register-b", "catalogue-b", "r1", None),
    )
    .await;
    assert_eq!(second.status, 303, "{}", second.body);
    for path in [
        "/repositories?limit=1",
        "/repositories/id/catalogue-a?registration_revision=r1",
    ] {
        let view = ui(c, "GET", path, vec![]).await;
        assert_eq!(view.status, 200, "{}", view.body);
        assert!(
            view.body
                .contains("&lt;script&gt;same label&lt;/script&gt;")
        );
        assert!(!view.body.contains("<script>same label"));
        for private in [
            "source_store_key",
            "checkpoint_store_key",
            "private-source-key",
            "private-checkpoint-key",
        ] {
            assert!(
                !view.body.contains(private),
                "private metadata exposed at {path}"
            );
        }
    }
    let first_page = ui(c, "GET", "/repositories?limit=1", vec![]).await;
    assert!(first_page.body.contains("after=catalogue-a&amp;limit=1"));
    assert!(!first_page.body.contains("/repositories/id/catalogue-b?"));
    let next_page = ui(c, "GET", "/repositories?after=catalogue-a&limit=1", vec![]).await;
    assert!(
        next_page
            .body
            .contains("/repositories/id/catalogue-b?registration_revision=r1")
    );
    assert!(!next_page.body.contains("/repositories/id/catalogue-a?"));

    let task_body = String::from_utf8(submission("catalogue-task", "Explicit selection", ""))
        .unwrap()
        .replace(
            "repository_id=product-fixture-repository",
            "repository_id=catalogue-a",
        );
    let submitted = ui(c, "POST", "/tasks", task_body.into_bytes()).await;
    let accepted = task(c, task_id(&submitted)).await;
    assert_eq!(
        accepted
            .input
            .repository
            .as_ref()
            .unwrap()
            .registration
            .as_ref()
            .unwrap()
            .repository_id,
        "catalogue-a"
    );
    assert_eq!(
        accepted.input.repository.as_ref().unwrap().url,
        "https://example.test/catalogue-a/r1"
    );
    let revised = ui(
        c,
        "POST",
        "/repositories/id/catalogue-a/revise",
        registration_body("revise-a", "catalogue-a", "r2", Some(1)),
    )
    .await;
    assert_eq!(revised.status, 303, "{}", revised.body);
    assert_eq!(entry(c, "catalogue-a", None).await.generation, 2);
    let old = entry(c, "catalogue-a", Some("r1")).await;
    assert_eq!(old.source_endpoint, "https://example.test/catalogue-a/r1");
    assert_eq!(old.current_revision, "r2");
    let draft = ui(c, "GET", "/repositories/id/catalogue-a/revise", vec![]).await;
    assert_eq!(draft.status, 200, "{}", draft.body);
    assert!(
        draft
            .body
            .contains("name=\"registration_revision\" type=\"text\" value=\"\"")
    );
    assert!(
        draft
            .body
            .contains("name=\"source_store_key\" type=\"text\" value=\"\"")
    );
    assert!(!draft.body.contains("private-source-key"));
    let stale = ui(
        c,
        "POST",
        "/repositories/id/catalogue-a/enabled",
        form(&[
            ("operation_id", "disable-stale".into()),
            ("expected_generation", "1".into()),
            ("enabled_for_new_tasks", "false".into()),
        ]),
    )
    .await;
    assert_eq!(stale.status, 409, "{}", stale.body);
    assert!(stale.body.contains("disable-stale"));
    assert!(entry(c, "catalogue-a", None).await.enabled_for_new_tasks);
    let disabled = ui(
        c,
        "POST",
        "/repositories/id/catalogue-a/enabled",
        form(&[
            ("operation_id", "disable-a".into()),
            ("expected_generation", "2".into()),
            ("enabled_for_new_tasks", "false".into()),
        ]),
    )
    .await;
    assert_eq!(disabled.status, 303, "{}", disabled.body);
    assert!(!entry(c, "catalogue-a", None).await.enabled_for_new_tasks);
    assert_eq!(task(c, &accepted.id).await, accepted);
    let task_body = String::from_utf8(submission(
        "catalogue-task-disabled",
        "Explicit disabled selection",
        "",
    ))
    .unwrap()
    .replace(
        "repository_id=product-fixture-repository",
        "repository_id=catalogue-a",
    )
    .replace("registration_revision=r1", "registration_revision=r2");
    let refused = ui(c, "POST", "/tasks", task_body.into_bytes()).await;
    assert_eq!(refused.status, 409, "{}", refused.body);
    assert!(refused.body.contains("repository_disabled"));
    let mut bad_form = registration_body("preserve-draft", "catalogue-c", "r1", None);
    bad_form.extend_from_slice(b"&unknown=x");
    let invalid = ui(c, "POST", "/repositories", bad_form).await;
    assert_eq!(invalid.status, 400);
    assert!(
        invalid
            .body
            .contains("name=\"operation_id\" value=\"preserve-draft\"")
    );
    assert!(invalid.body.contains("value=\"catalogue-c\""));
}

async fn report(controller: &Controller, attempt: &Attempt, observation: Observation) {
    let report_id = match &observation {
        Observation::Usage { model_requests, .. } => {
            borg_runtime::native_loop_contract::reservation_id(&attempt.id, *model_requests)
        }
        Observation::NativeLoopCompleted { .. } => {
            borg_runtime::native_loop_contract::completion_id(&attempt.id)
        }
        _ => format!("report-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
    };
    call(
        controller,
        Operation::ObserveWorker {
            report: Report {
                report_id,
                worker_id: attempt.worker_id.clone(),
                incarnation: attempt.worker_incarnation.clone(),
                task_id: attempt.task_id.clone(),
                attempt_id: attempt.id.clone(),
                generation: attempt.generation,
                observation,
            },
        },
    )
    .await;
}

// Controlled API producer fixture for UI/receipt tests only. It does not run a
// worker, compiler, package verifier or protected release checker.
async fn controlled_release_source(fixture: &Fixture) -> Task {
    let created = ui(
        &fixture.controller,
        "POST",
        "/tasks",
        submission("release-source", "controlled source fixture", ""),
    )
    .await;
    let id = task_id(&created).to_string();
    call(
        &fixture.controller,
        Operation::RegisterWorker {
            operation_id: "release-fixture-registration".into(),
            worker: Worker {
                id: "release-fixture-worker".into(),
                incarnation: "incarnation-1".into(),
                coding_capacity: 1,
                native_capacity: 0,
            },
        },
    )
    .await;
    let Data::Commands(commands) = call(
        &fixture.controller,
        Operation::AwaitCommands {
            operation_id: format!("release-fixture-assignment-{id}"),
            worker_id: "release-fixture-worker".into(),
            incarnation: "incarnation-1".into(),
            acknowledged: vec![],
        },
    )
    .await
    else {
        panic!("fixture assignment missing");
    };
    let attempt = commands
        .iter()
        .find_map(|command| match command {
            WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == id => {
                Some(attempt.clone())
            }
            _ => None,
        })
        .expect("controlled assigned attempt");
    report(
        &fixture.controller,
        &attempt,
        Observation::Started {
            thread_id: Some("fixture-thread".into()),
            turn_id: Some("fixture-turn".into()),
        },
    )
    .await;
    let source = task(&fixture.controller, &id).await;
    let checkpoint = borg_runtime::worker::git::checkpoint(
        source.input.repository.as_ref().unwrap(),
        &id,
        &attempt.id,
        "a".repeat(40),
    )
    .unwrap();
    call(&fixture.controller, Operation::PrepareOperation {
        operation_id: checkpoint.operation_id.clone(), task_id: id.clone(), attempt_id: attempt.id.clone(), generation: attempt.generation,
        kind: "checkpoint".into(), intended: serde_json::json!({"registration":checkpoint.registration,"repository":checkpoint.repository,"commit":checkpoint.commit,"remote_ref":checkpoint.remote_ref}),
    }).await;
    call(
        &fixture.controller,
        Operation::SettleOperation {
            settlement_id: format!("release-fixture-settlement-{id}"),
            operation_id: checkpoint.operation_id.clone(),
            task_id: id.clone(),
            attempt_id: attempt.id.clone(),
            generation: attempt.generation,
            outcome: serde_json::json!({"acknowledged":true}),
            checkpoint: Some(checkpoint.clone()),
        },
    )
    .await;
    report(
        &fixture.controller,
        &attempt,
        Observation::ExecutorOutcome {
            success: true,
            summary: "controlled task outcome only".into(),
        },
    )
    .await;
    report(
        &fixture.controller,
        &attempt,
        Observation::ProcessTreeStopped,
    )
    .await;
    report(
        &fixture.controller,
        &attempt,
        Observation::ResultCandidate {
            checks_passed: true,
            summary: "controlled task check passed".into(),
            checkpoint: Some(checkpoint),
            validation: vec!["controlled task validation; not protected release evidence".into()],
            output: Some(
                serde_json::json!({"checks":"passed","human_rollout":"authorized","deployed":true}),
            ),
        },
    )
    .await;
    task(&fixture.controller, &id).await
}

fn release_claim(source: &Task, digest: &str) -> ReleaseArtifact {
    ReleaseArtifact {
        format: "borg-v2-release/1".into(),
        package_sha256: digest.repeat(64),
        package_bytes: 20_480,
        manifest_sha256: "e".repeat(64),
        source_commit: source.selected_checkpoint.as_ref().unwrap().commit.clone(),
        source_tree: "b".repeat(40),
        object_format: "sha1".into(),
        binary_sha256: "f".repeat(64),
        binary_bytes: 1024,
        target: "x86_64-unknown-linux-gnu".into(),
    }
}

async fn prepare_controlled_release(
    fixture: &Fixture,
    source: &Task,
    digest: &str,
) -> ReleaseCandidate {
    let Data::Release(candidate) = call(
        &fixture.controller,
        Operation::PrepareRelease(PrepareReleaseInput {
            operation_id: format!("release-claim-{digest}"),
            task_id: source.id.clone(),
            expected_task_revision: source.revision,
            checkpoint: source.selected_checkpoint.clone().unwrap(),
            artifact: release_claim(source, digest),
        }),
    )
    .await
    else {
        panic!("expected controlled release candidate");
    };
    *candidate
}

fn task_id(response: &UiResponse) -> &str {
    assert_eq!(response.status, 303, "{}", response.body);
    response
        .location
        .as_deref()
        .unwrap()
        .strip_prefix("/tasks/")
        .unwrap()
}

#[tokio::test]
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST support task submission.
// Traceability scope: Module architecture inspection plus shared-state behavior, not deferred topic delivery.
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The collaboration product MUST NOT create an independent agent inbox.
// Traceability scope: V2 events are task commands/state transitions; general topics remain deferred.
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The collaboration product MUST use the platform's task state and event interfaces.
async fn browser_submission_replay_and_restart_show_one_committed_task() {
    let fixture = Fixture::new().await;
    let initial = ui(&fixture.controller, "GET", "/tasks/new", vec![]).await;
    assert_eq!(initial.status, 200);
    assert!(initial.body.contains("method=\"post\""));
    assert!(initial.body.contains("name=\"operation_id\""));
    let input = submission(
        "submit-browser-1",
        "Fix <script>alert('x')</script> & report",
        "",
    );
    let first = ui(&fixture.controller, "POST", "/tasks", input.clone()).await;
    let id = task_id(&first).to_string();
    let repeated = ui(&fixture.controller, "POST", "/tasks", input).await;
    assert_eq!(first.location, repeated.location);
    let lookup = ui(
        &fixture.controller,
        "GET",
        "/operations/submit-browser-1",
        vec![],
    )
    .await;
    assert_eq!(lookup.status, 200);
    assert!(lookup.body.contains("submit-browser-1"));
    assert!(lookup.body.contains(&id));
    let list = ui(&fixture.controller, "GET", "/tasks", vec![]).await;
    assert_eq!(
        list.body.matches(&format!("href=\"/tasks/{id}\"")).count(),
        1
    );
    let view = ui(&fixture.controller, "GET", &format!("/tasks/{id}"), vec![]).await;
    assert!(
        view.body
            .contains("&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt; &amp; report")
    );
    assert!(!view.body.contains("<script>"));
    assert!(view.body.contains("No accepted task result"));
    let path = fixture.path.clone();
    drop(fixture.controller);
    let reopened = Controller::open_with_config(path, config()).unwrap();
    let after = ui(&reopened, "GET", &format!("/tasks/{id}"), vec![]).await;
    assert_eq!(after.status, 200);
    assert!(after.body.contains("coding-default"));
    assert!(after.body.contains("&lt;script&gt;"));
    let conflicts = ui(
        &reopened,
        "POST",
        "/tasks",
        submission("submit-browser-1", "Different work", ""),
    )
    .await;
    assert_eq!(conflicts.status, 409);
    assert!(conflicts.body.contains("Action refused"));
}

#[tokio::test]
async fn same_origin_and_non_get_guards_prevent_browser_mutations() {
    let fixture = Fixture::new().await;
    for origin in [
        None,
        Some("https://attacker.example".into()),
        Some("null".into()),
    ] {
        let response = handle(
            &fixture.controller,
            UiRequest {
                method: "POST".into(),
                path: "/tasks".into(),
                body: submission("blocked-origin", "never submitted", ""),
                origin,
                expected_origin: ORIGIN.into(),
            },
        )
        .await;
        assert_eq!(response.status, 403);
    }
    assert_eq!(
        ui(
            &fixture.controller,
            "GET",
            "/tasks/not-a-task/cancel",
            vec![]
        )
        .await
        .status,
        405
    );
    assert_eq!(
        ui(&fixture.controller, "PUT", "/tasks", vec![])
            .await
            .status,
        405
    );
    assert_eq!(
        ui(
            &fixture.controller,
            "POST",
            "/tasks",
            b"operation_id=x&operation_id=y".to_vec()
        )
        .await
        .status,
        400
    );
    assert_eq!(
        ui(
            &fixture.controller,
            "POST",
            "/tasks?operation_id=x",
            submission("id", "work", "")
        )
        .await
        .status,
        400
    );
    let list = ui(&fixture.controller, "GET", "/tasks", vec![]).await;
    assert!(list.body.contains("No tasks in this page"));
}

#[tokio::test]
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST support task assignment.
async fn stale_action_refuses_and_does_not_retarget_a_form() {
    let fixture = Fixture::new().await;
    // Exact same-agent assignment is idempotently recorded; another agent
    // cannot retarget the accepted definition in the clean session schema.
    let created = ui(
        &fixture.controller,
        "POST",
        "/tasks",
        submission("submit-stale", "work", ""),
    )
    .await;
    let id = task_id(&created);
    let before = task(&fixture.controller, id).await;
    let assign = form(&[
        ("operation_id", "assign-once".into()),
        ("expected_revision", before.revision.to_string()),
        ("agent_id", "coding-default".into()),
    ]);
    let assigned = ui(
        &fixture.controller,
        "POST",
        &format!("/tasks/{id}/assign"),
        assign.clone(),
    )
    .await;
    assert_eq!(assigned.status, 303, "{}", assigned.body);
    let again = ui(
        &fixture.controller,
        "POST",
        &format!("/tasks/{id}/assign"),
        assign,
    )
    .await;
    assert_eq!(again.status, 303);
    let stale = ui(
        &fixture.controller,
        "POST",
        &format!("/tasks/{id}/cancel"),
        form(&[
            ("operation_id", "stale-cancel".into()),
            ("expected_revision", before.revision.to_string()),
        ]),
    )
    .await;
    assert_eq!(stale.status, 409);
    assert!(stale.body.contains("stale-cancel"));
    assert!(stale.body.contains("Refresh"));
    let after = task(&fixture.controller, id).await;
    assert_eq!(after.assigned_agent_id, "coding-default");
    assert_eq!(after.status, TaskStatus::Queued);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST show task dependencies.
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST expose task cancellation.
// Traceability scope: Controlled event producers with real controller; no full live browser/model journey.
//= spec/impl/v2/implementation.md#observable-end-to-end-result
//= type=test
//# The initial system MUST expose the task's actual execution status through its product interface.
// Traceability scope: Owned handler tests are not live browser/model qualification.
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST show actual task status.
async fn dependencies_and_failed_or_cancelled_state_remain_visible() {
    let fixture = Fixture::new().await;
    let created = ui(
        &fixture.controller,
        "POST",
        "/tasks",
        submission("parent-submit", "prerequisite", ""),
    )
    .await;
    let parent = task_id(&created).to_string();
    let before = task(&fixture.controller, &parent).await;
    let cancelled = ui(
        &fixture.controller,
        "POST",
        &format!("/tasks/{parent}/cancel"),
        form(&[
            ("operation_id", "cancel-parent".into()),
            ("expected_revision", before.revision.to_string()),
        ]),
    )
    .await;
    assert_eq!(cancelled.status, 303, "{}", cancelled.body);
    let child = ui(
        &fixture.controller,
        "POST",
        "/tasks",
        submission("dependent-submit", "dependent", &parent),
    )
    .await;
    let child_id = task_id(&child);
    let view = ui(
        &fixture.controller,
        "GET",
        &format!("/tasks/{child_id}"),
        vec![],
    )
    .await;
    assert!(view.body.contains(&format!("href=\"/tasks/{parent}\"")));
    assert!(view.body.contains("Blocked reason"));
    assert_eq!(
        task(&fixture.controller, child_id).await.status,
        TaskStatus::Queued
    );
    let parent_view = ui(
        &fixture.controller,
        "GET",
        &format!("/tasks/{parent}"),
        vec![],
    )
    .await;
    assert!(parent_view.body.contains("cancelled"));
    assert!(parent_view.body.contains("Request retry"));
    assert_eq!(
        ui(
            &fixture.controller,
            "GET",
            &format!("/tasks/{parent}/result"),
            vec![]
        )
        .await
        .status,
        404
    );
    assert_eq!(
        ui(
            &fixture.controller,
            "GET",
            &format!("/tasks/{parent}/checkpoint"),
            vec![]
        )
        .await
        .status,
        404
    );
}

#[tokio::test]
async fn invalid_submission_retains_operation_identity_and_fields() {
    let fixture = Fixture::new().await;
    let fields = form(&[
        ("operation_id", "preserve-identity".into()),
        ("instruction", "Keep <this> input".into()),
        ("executor", "unrecognized".into()),
    ]);
    let response = ui(&fixture.controller, "POST", "/tasks", fields).await;
    assert_eq!(response.status, 400);
    assert!(response.body.contains("value=\"preserve-identity\""));
    assert!(response.body.contains("Keep &lt;this&gt; input"));
}

#[tokio::test]
async fn native_form_stores_only_explicit_typed_coding_workflow_configuration() {
    let fixture = Fixture::new().await;
    let initial = ui(&fixture.controller, "GET", "/tasks/new", vec![]).await;
    assert!(initial.body.contains("name=\"native_workflow\""));
    assert!(initial.body.contains("name=\"result_bytes\""));
    let native_form = ui(
        &fixture.controller,
        "GET",
        "/tasks/new?executor=native",
        vec![],
    )
    .await;
    assert_eq!(native_form.status, 200);
    assert!(native_form.body.contains("value=\"native\" selected"));
    assert!(
        native_form
            .body
            .contains("name=\"acceptance_commands\" rows=\"3\">[]</textarea>")
    );
    assert!(
        native_form
            .body
            .contains("name=\"native_workflow\" rows=\"8\"></textarea>")
    );
    let deadline = now() + 3_600_000;
    let plain = ui(
        &fixture.controller,
        "POST",
        "/tasks",
        native_submission("native-plain", deadline, None),
    )
    .await;
    let plain_task = task(&fixture.controller, task_id(&plain)).await;
    assert_eq!(plain_task.input.native_workflow, None);
    assert_eq!(
        plain_task.input.limits.result_bytes,
        borg_runtime::limits::RuntimeLimits::seed()
            .unwrap()
            .task_defaults
            .result_bytes
    );
    let plain_view = ui(
        &fixture.controller,
        "GET",
        &format!("/tasks/{}", plain_task.id),
        vec![],
    )
    .await;
    assert!(plain_view.body.contains("no coding handoff configured"));

    let configured = workflow(deadline);
    let input = serde_json::to_string(&configured).unwrap();
    let submitted = ui(
        &fixture.controller,
        "POST",
        "/tasks",
        native_submission("native-configured", deadline, Some(&input)),
    )
    .await;
    let saved = task(&fixture.controller, task_id(&submitted)).await;
    let mut resolved = configured;
    let NativeWorkflow::CodingThenContinue { child } = &mut resolved else {
        unreachable!()
    };
    child.repository.url = "file:///tmp/local-test-repository".into();
    child.repository.checkpoint_remote = "origin".into();
    assert_eq!(saved.input.native_workflow, Some(Box::new(resolved)));
    assert_eq!(saved.status, TaskStatus::Queued);
    assert!(saved.attempt.is_none());
    let view = ui(
        &fixture.controller,
        "GET",
        &format!("/tasks/{}", saved.id),
        vec![],
    )
    .await;
    assert!(view.body.contains("Configured native workflow"));
    assert!(view.body.contains("coding-child"));
    assert!(view.body.contains("file:///tmp/local-test-repository"));
    assert!(view.body.contains("No accepted task result"));
}

#[tokio::test]
async fn workflow_form_rejects_unknown_fields_bad_bounds_and_coding_parent() {
    let fixture = Fixture::new().await;
    let deadline = now() + 3_600_000;
    let configured = workflow(deadline);
    let mut unknown = serde_json::to_value(&configured).unwrap();
    unknown["model_repository"] = serde_json::json!("model-controlled-override");
    let mut bad_executor = serde_json::to_value(&configured).unwrap();
    bad_executor["child"]["definition"]["executor"] = serde_json::json!("native");
    let mut bad_result_bound = serde_json::to_value(&configured).unwrap();
    bad_result_bound["child"]["limits"]["result_bytes"] = serde_json::json!(
        borg_runtime::limits::RuntimeLimits::seed()
            .unwrap()
            .task_defaults
            .result_bytes
    );
    for (index, invalid) in [
        "not-json<script>".into(),
        unknown.to_string(),
        bad_executor.to_string(),
        bad_result_bound.to_string(),
    ]
    .iter()
    .enumerate()
    {
        let operation = format!("invalid-workflow-{index}");
        let response = ui(
            &fixture.controller,
            "POST",
            "/tasks",
            native_submission(&operation, deadline, Some(invalid)),
        )
        .await;
        if index < 2 {
            // Malformed/unknown draft fields remain a local structural refusal.
            assert_eq!(response.status, 400, "{}", response.body);
            assert!(response.body.contains(&format!("value=\"{operation}\"")));
        } else {
            // Policy now runs after canonical receipt lookup. The unchanged
            // mutation_reply mapping exposes its committed refusal as HTTP409.
            assert_recorded_policy_refusal(&fixture.controller, &operation, &response).await;
        }
        assert!(!response.body.contains("<script>"));
    }
    let mut coding = submission("coding-cannot-delegate", "coding work", "");
    coding.extend_from_slice(
        format!(
            "&native_workflow={}",
            encode(&serde_json::to_string(&configured).unwrap())
        )
        .as_bytes(),
    );
    let response = ui(&fixture.controller, "POST", "/tasks", coding).await;
    assert_eq!(response.status, 400);
    assert!(response.body.contains("only configure a native task"));
    let tasks = call(
        &fixture.controller,
        Operation::ListTasks {
            after: None,
            limit: 100,
        },
    )
    .await;
    assert!(matches!(tasks, Data::Tasks(tasks) if tasks.is_empty()));
}

#[tokio::test]
async fn task_cursor_and_mutation_query_are_preserved_at_the_product_boundary() {
    let fixture = Fixture::new().await;
    let first = ui(
        &fixture.controller,
        "POST",
        "/tasks",
        submission("cursor-first", "first", ""),
    )
    .await;
    let first_id = task_id(&first).to_string();
    let second = ui(
        &fixture.controller,
        "POST",
        "/tasks",
        submission("cursor-second", "second", ""),
    )
    .await;
    let second_id = task_id(&second).to_string();
    let page = ui(
        &fixture.controller,
        "GET",
        &format!("/tasks?after={first_id}"),
        vec![],
    )
    .await;
    assert_eq!(page.status, 200);
    assert!(!page.body.contains(&format!("href=\"/tasks/{first_id}\"")));
    assert!(page.body.contains(&format!("href=\"/tasks/{second_id}\"")));
    assert!(page.body.contains(&format!("/tasks?after={second_id}")));
    let last = ui(
        &fixture.controller,
        "GET",
        &format!("/tasks?after={second_id}"),
        vec![],
    )
    .await;
    assert!(last.body.contains("No tasks in this page"));
    assert_eq!(
        ui(&fixture.controller, "GET", "/tasks?after=%ZZ", vec![])
            .await
            .status,
        400
    );
    assert_eq!(
        ui(
            &fixture.controller,
            "POST",
            &format!("/tasks/{first_id}/cancel?x=1"),
            vec![]
        )
        .await
        .status,
        400
    );
    assert_eq!(
        task(&fixture.controller, &first_id).await.status,
        TaskStatus::Queued
    );
}

#[tokio::test]
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST support retrying a failed task.
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST show reported task progress.
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST show available recent task output.
// Traceability scope: Controlled event producers with real controller; no full live browser/model journey.
//= spec/impl/v2/implementation.md#observable-end-to-end-result
//= type=test
//# The initial system MUST expose the task's actual execution status through its product interface.
// Traceability scope: Owned handler tests are not live browser/model qualification.
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST show actual task status.
// Traceability scope: V2 events are task commands/state transitions; general topics remain deferred.
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The collaboration product MUST use the platform's task state and event interfaces.
async fn failed_task_retry_and_active_cancellation_use_real_controller_transitions() {
    let fixture = Fixture::new().await;
    let created = ui(
        &fixture.controller,
        "POST",
        "/tasks",
        submission("submit-failure", "fail checks", ""),
    )
    .await;
    let id = task_id(&created).to_string();
    call(
        &fixture.controller,
        Operation::RegisterWorker {
            operation_id: "failure-fixture-registration".into(),
            worker: Worker {
                id: "worker-1".into(),
                incarnation: "incarnation-1".into(),
                coding_capacity: 1,
                native_capacity: 0,
            },
        },
    )
    .await;
    let commands = call(
        &fixture.controller,
        Operation::AwaitCommands {
            operation_id: "failure-fixture-initial-assignment".into(),
            worker_id: "worker-1".into(),
            incarnation: "incarnation-1".into(),
            acknowledged: vec![],
        },
    )
    .await;
    let Data::Commands(commands) = commands else {
        panic!("expected assigned command");
    };
    let attempt = commands
        .iter()
        .find_map(|c| match c {
            WorkerCommand::RunAttempt { attempt, .. } => Some(attempt.clone()),
            _ => None,
        })
        .expect("run assignment");
    report(
        &fixture.controller,
        &attempt,
        Observation::Started {
            thread_id: Some("thread-fixture".into()),
            turn_id: Some("turn-fixture".into()),
        },
    )
    .await;
    report(
        &fixture.controller,
        &attempt,
        Observation::Progress {
            text: "running <checks>".into(),
            output: "<script>untrusted</script>".into(),
            truncated: true,
        },
    )
    .await;
    let progress = ui(&fixture.controller, "GET", &format!("/tasks/{id}"), vec![]).await;
    assert!(progress.body.contains("running &lt;checks&gt;"));
    assert!(
        progress
            .body
            .contains("&lt;script&gt;untrusted&lt;/script&gt;")
    );
    assert!(progress.body.contains("Output is truncated"));
    report(
        &fixture.controller,
        &attempt,
        Observation::ExecutorOutcome {
            success: true,
            summary: "executor exited zero".into(),
        },
    )
    .await;
    report(
        &fixture.controller,
        &attempt,
        Observation::ResultCandidate {
            checks_passed: false,
            summary: "acceptance failed".into(),
            checkpoint: None,
            validation: vec!["check failed".into()],
            output: None,
        },
    )
    .await;
    report(
        &fixture.controller,
        &attempt,
        Observation::ProcessTreeStopped,
    )
    .await;
    let failed = task(&fixture.controller, &id).await;
    assert_eq!(failed.status, TaskStatus::Failed);
    let failed_view = ui(&fixture.controller, "GET", &format!("/tasks/{id}"), vec![]).await;
    assert!(failed_view.body.contains("Request retry"));
    assert!(failed_view.body.contains("No accepted task result"));
    let retry = ui(
        &fixture.controller,
        "POST",
        &format!("/tasks/{id}/retry"),
        form(&[
            ("operation_id", "retry-original".into()),
            ("expected_revision", failed.revision.to_string()),
        ]),
    )
    .await;
    assert_eq!(retry.status, 303, "{}", retry.body);
    let commands = call(
        &fixture.controller,
        Operation::AwaitCommands {
            operation_id: "failure-fixture-retry-assignment".into(),
            worker_id: "worker-1".into(),
            incarnation: "incarnation-1".into(),
            acknowledged: commands
                .iter()
                .map(|c| match c {
                    WorkerCommand::RunAttempt { command_id, .. }
                    | WorkerCommand::StopAttempt { command_id, .. }
                    | WorkerCommand::CollaborationReady { command_id, .. } => command_id.clone(),
                })
                .collect(),
        },
    )
    .await;
    let Data::Commands(commands) = commands else {
        panic!("expected retry command");
    };
    assert!(commands.iter().any(|c| matches!(c, WorkerCommand::RunAttempt { attempt: next, .. } if next.generation > attempt.generation)));
    let active = task(&fixture.controller, &id).await;
    let cancel = ui(
        &fixture.controller,
        "POST",
        &format!("/tasks/{id}/cancel"),
        form(&[
            ("operation_id", "cancel-running".into()),
            ("expected_revision", active.revision.to_string()),
        ]),
    )
    .await;
    assert_eq!(cancel.status, 303, "{}", cancel.body);
    let stopping = ui(&fixture.controller, "GET", &format!("/tasks/{id}"), vec![]).await;
    assert!(stopping.body.contains("stop requested"));
    assert!(stopping.body.contains("Termination confirmed</dt><dd>no"));
}

#[tokio::test]
async fn release_navigation_is_read_only_and_does_not_infer_protection_from_task_output() {
    let fixture = Fixture::new().await;
    let source = controlled_release_source(&fixture).await;
    let task_view = ui(
        &fixture.controller,
        "GET",
        &format!("/tasks/{}", source.id),
        vec![],
    )
    .await;
    assert!(
        task_view
            .body
            .contains(&format!("href=\"/tasks/{}/releases\"", source.id))
    );
    let empty = ui(
        &fixture.controller,
        "GET",
        &format!("/tasks/{}/releases", source.id),
        vec![],
    )
    .await;
    assert!(empty.body.contains("No release candidates on this page"));
    let candidate = prepare_controlled_release(&fixture, &source, "c").await;
    let listing = ui(
        &fixture.controller,
        "GET",
        &format!("/tasks/{}/releases", source.id),
        vec![],
    )
    .await;
    assert_eq!(listing.status, 200);
    assert!(
        listing
            .body
            .contains(&format!("href=\"/releases/{}\"", candidate.id))
    );
    assert!(listing.body.contains(&candidate.artifact.package_sha256));
    let detail = ui(
        &fixture.controller,
        "GET",
        &format!("/releases/{}", candidate.id),
        vec![],
    )
    .await;
    assert_eq!(detail.status, 200);
    for value in [
        &candidate.artifact.package_sha256,
        &candidate.artifact.manifest_sha256,
        &candidate.artifact.binary_sha256,
        &candidate.artifact.source_commit,
        &candidate.artifact.source_tree,
    ] {
        assert!(detail.body.contains(value));
    }
    assert!(
        detail
            .body
            .contains("Identity claimed; independent verification is not established")
    );
    assert!(
        detail
            .body
            .contains("<dt>Protected checks</dt><dd>Not configured</dd>")
    );
    assert!(
        detail
            .body
            .contains("<dt>Human rollout authorization</dt><dd>Not configured</dd>")
    );
    assert!(
        detail
            .body
            .contains("<dt>Build target</dt><dd>x86_64-unknown-linux-gnu</dd>")
    );
    assert!(
        detail
            .body
            .contains("<dt>Deployment target</dt><dd>Not recorded</dd>")
    );
    assert!(
        detail
            .body
            .contains("<dt>Deployment outcome</dt><dd>Not recorded</dd>")
    );
    assert!(detail.body.contains("href=\"#artifact\""));
    assert!(detail.body.contains("href=\"#checks\""));
    assert!(!detail.body.contains("<form"));
    assert!(
        !detail
            .body
            .split("</main>")
            .next()
            .unwrap()
            .contains("<button")
    );
    assert!(detail.body.contains("id=\"live-refresh\" type=\"button\""));
    assert_eq!(
        ui(
            &fixture.controller,
            "POST",
            &format!("/releases/{}/approve", candidate.id),
            form(&[("approved", "true".into())])
        )
        .await
        .status,
        405
    );
    assert_eq!(
        ui(
            &fixture.controller,
            "POST",
            &format!("/tasks/{}/releases", source.id),
            vec![]
        )
        .await
        .status,
        405
    );
    let Data::Release(unchanged) = call(
        &fixture.controller,
        Operation::GetRelease {
            release_id: candidate.id.clone(),
        },
    )
    .await
    else {
        panic!("candidate missing");
    };
    assert_eq!(*unchanged, candidate);
    assert_eq!(
        task(&fixture.controller, &source.id).await.status,
        TaskStatus::Succeeded
    );
}

#[tokio::test]
async fn release_pages_recover_exact_candidates_and_keep_task_scoped_cursors() {
    let fixture = Fixture::new().await;
    let source = controlled_release_source(&fixture).await;
    let first = prepare_controlled_release(&fixture, &source, "c").await;
    let second = prepare_controlled_release(&fixture, &source, "d").await;
    let again = prepare_controlled_release(&fixture, &source, "c").await;
    assert_eq!(again.id, first.id);
    let page = ui(
        &fixture.controller,
        "GET",
        &format!("/tasks/{}/releases?after={}", source.id, first.id),
        vec![],
    )
    .await;
    assert_eq!(page.status, 200);
    assert!(
        !page
            .body
            .contains(&format!("href=\"/releases/{}\"", first.id))
    );
    assert!(
        page.body
            .contains(&format!("href=\"/releases/{}\"", second.id))
    );
    assert_eq!(
        ui(
            &fixture.controller,
            "GET",
            &format!("/tasks/{}/releases?after=unknown", source.id),
            vec![]
        )
        .await
        .status,
        409
    );
    assert_eq!(
        ui(
            &fixture.controller,
            "GET",
            &format!("/tasks/{}/releases?extra=field", source.id),
            vec![]
        )
        .await
        .status,
        400
    );
    assert_eq!(
        ui(&fixture.controller, "GET", "/releases/missing", vec![])
            .await
            .status,
        404
    );
    assert_eq!(
        ui(
            &fixture.controller,
            "GET",
            &format!("/releases/{}?approve=true", first.id),
            vec![]
        )
        .await
        .status,
        400
    );
    let path = fixture.path.clone();
    drop(fixture.controller);
    let reopened = Controller::open_with_config(path, config()).unwrap();
    let recovered = ui(&reopened, "GET", &format!("/releases/{}", first.id), vec![]).await;
    assert_eq!(recovered.status, 200);
    assert!(recovered.body.contains(&first.artifact.package_sha256));
    assert!(
        recovered
            .body
            .contains("Human rollout authorization</dt><dd>Not configured")
    );
    let Data::Release(record) = call(
        &reopened,
        Operation::GetRelease {
            release_id: first.id.clone(),
        },
    )
    .await
    else {
        panic!("candidate missing after restart");
    };
    assert_eq!(*record, first);
}

// Controlled API reports and an acknowledged fixture checkpoint only. This
// exercises the actual product handler and controller, not a worker, browser,
// provider, Git publication or protected release checker.
#[tokio::test]
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST present accepted task results.
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST link the task's published source checkpoints.
//= spec/impl/v2/implementation.md#observable-end-to-end-result
//= type=test
//# The initial system MUST return references to the source changes and validation results produced by a successful task.
async fn accepted_root_result_routes_show_committed_output_checks_and_checkpoint() {
    let fixture = Fixture::new().await;
    let accepted = controlled_release_source(&fixture).await;
    assert_eq!(accepted.status, TaskStatus::Succeeded);
    assert!(accepted.parent_task_id.is_none());
    let result = accepted.result.as_ref().expect("accepted root result");
    let checkpoint = result
        .checkpoint
        .as_ref()
        .expect("accepted source checkpoint");
    assert_eq!(Some(checkpoint), accepted.selected_checkpoint.as_ref());
    assert_eq!(
        result.output,
        Some(serde_json::json!({
            "checks": "passed", "human_rollout": "authorized", "deployed": true
        }))
    );
    let root_path = format!("/tasks/{}", accepted.id);
    let view = ui(&fixture.controller, "GET", &root_path, vec![]).await;
    assert_eq!(view.status, 200);
    assert!(view.body.contains("<dt>Status</dt><dd>succeeded</dd>"));
    assert!(view.body.contains("<h2>Accepted result</h2>"));
    assert!(view.body.contains(&format!("href=\"{root_path}/result\"")));
    assert!(
        view.body
            .contains(&format!("href=\"{root_path}/checkpoint\""))
    );
    assert!(!view.body.contains("No accepted task result"));

    let page = ui(
        &fixture.controller,
        "GET",
        &format!("{root_path}/result"),
        vec![],
    )
    .await;
    assert_eq!(page.status, 200);
    assert!(page.body.contains("Accepted task result"));
    assert!(page.body.contains(&result.attempt_id));
    assert!(page.body.contains("controlled task check passed"));
    assert!(
        page.body
            .contains("controlled task validation; not protected release evidence")
    );
    assert!(page.body.contains("&quot;checks&quot;: &quot;passed&quot;"));
    assert!(
        page.body
            .contains("&quot;human_rollout&quot;: &quot;authorized&quot;")
    );
    assert!(page.body.contains("&quot;deployed&quot;: true"));
    assert!(page.body.contains(&checkpoint.commit));
    assert!(page.body.contains(&checkpoint.remote_ref));
    assert!(page.body.contains(&checkpoint.operation_id));
    // These output fields remain inert result data, not protected decisions.
    assert!(!page.body.contains("<form"));
    assert!(
        !page
            .body
            .split("</main>")
            .next()
            .unwrap()
            .contains("<button")
    );
    assert!(page.body.contains("id=\"live-refresh\" type=\"button\""));

    let source = ui(
        &fixture.controller,
        "GET",
        &format!("{root_path}/checkpoint"),
        vec![],
    )
    .await;
    assert_eq!(source.status, 200);
    assert!(source.body.contains(&checkpoint.commit));
    assert!(source.body.contains(&checkpoint.remote_ref));
    assert!(source.body.contains("not a safe browser"));
    assert!(!source.body.contains("href=\"origin\""));
    assert_eq!(task(&fixture.controller, &accepted.id).await, accepted);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST present accepted task results.
// Actual controller acceptance/schema validation with synthetic reports; no
// executor/provider or real-browser invocation. Unknown remains unknown.
async fn root_result_routes_escape_output_and_distinguish_failure_and_uncertainty() {
    let fixture = Fixture::new().await;
    let worker = Worker {
        id: "root-result-worker".into(),
        incarnation: "root-result-incarnation".into(),
        coding_capacity: 0,
        native_capacity: 1,
    };
    call(
        &fixture.controller,
        Operation::RegisterWorker {
            operation_id: "root-result-registration".into(),
            worker: worker.clone(),
        },
    )
    .await;
    let output = serde_json::json!({
        "<script>root-key</script>": "<img src=x onerror=alert(1)>& literal",
        "nested": [{"answer": 42}]
    });
    for mode in ["accepted", "failed", "unknown"] {
        let created = ui(
            &fixture.controller,
            "POST",
            "/tasks",
            native_submission(&format!("root-result-{mode}"), now() + 3_600_000, None),
        )
        .await;
        let id = task_id(&created).to_string();
        let Data::Commands(commands) = call(
            &fixture.controller,
            Operation::AwaitCommands {
                operation_id: format!("root-result-assignment-{mode}"),
                worker_id: worker.id.clone(),
                incarnation: worker.incarnation.clone(),
                acknowledged: vec![],
            },
        )
        .await
        else {
            panic!("expected controlled native assignment");
        };
        let attempt = commands
            .iter()
            .find_map(|command| match command {
                WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == id => {
                    Some(attempt.clone())
                }
                _ => None,
            })
            .expect("assigned native result fixture");
        report(
            &fixture.controller,
            &attempt,
            Observation::Usage {
                native_diagnostic_reserved: false,
                model_requests: 1,
                input_tokens: None,
                output_tokens: None,
            },
        )
        .await;
        if mode == "unknown" {
            report(
                &fixture.controller,
                &attempt,
                Observation::ExecutionUncertain {
                    message:
                        "Controlled provider outcome remains unknown <script>uncertain</script>"
                            .into(),
                },
            )
            .await;
        } else {
            report(
                &fixture.controller,
                &attempt,
                Observation::NativeLoopCompleted {
                    requests: Vec::new(),
                    final_response_index: None,
                    model_requests: 1,
                    calls: vec![],
                },
            )
            .await;
            report(
                &fixture.controller,
                &attempt,
                Observation::ExecutorOutcome {
                    success: true,
                    summary: "controlled executor completion".into(),
                },
            )
            .await;
            report(
                &fixture.controller,
                &attempt,
                Observation::ProcessTreeStopped,
            )
            .await;
            report(
                &fixture.controller,
                &attempt,
                Observation::ResultCandidate {
                    checks_passed: mode == "accepted",
                    summary: "Result <b>summary & detail</b>".into(),
                    checkpoint: None,
                    validation: vec![
                        "<i>validation & detail</i>".into(),
                        borg_runtime::native_loop_contract::evidence(&attempt.id),
                    ],
                    output: Some(output.clone()),
                },
            )
            .await;
        }
        let committed = task(&fixture.controller, &id).await;
        let page = ui(
            &fixture.controller,
            "GET",
            &format!("/tasks/{id}/result"),
            vec![],
        )
        .await;
        let view = ui(&fixture.controller, "GET", &format!("/tasks/{id}"), vec![]).await;
        if mode == "accepted" {
            assert_eq!(committed.status, TaskStatus::Succeeded);
            let result = committed
                .result
                .as_ref()
                .expect("schema-valid committed result");
            assert_eq!(result.output, Some(output.clone()));
            assert_eq!(page.status, 200);
            assert!(page.body.contains("&lt;script&gt;root-key&lt;/script&gt;"));
            assert!(
                page.body
                    .contains("&lt;img src=x onerror=alert(1)&gt;&amp; literal")
            );
            assert!(
                page.body
                    .contains("&lt;i&gt;validation &amp; detail&lt;/i&gt;")
            );
            assert!(page.body.contains("&quot;answer&quot;: 42"));
            assert!(
                view.body
                    .contains("Result &lt;b&gt;summary &amp; detail&lt;/b&gt;")
            );
            assert!(!view.body.contains("No accepted task result"));
        } else {
            assert_eq!(committed.status, TaskStatus::Failed);
            assert!(committed.result.is_none());
            assert_eq!(page.status, 404);
            assert!(page.body.contains("No accepted task result"));
            assert!(view.body.contains("No accepted task result"));
            assert!(!view.body.contains("<h2>Accepted result</h2>"));
            if mode == "unknown" {
                let attempt = committed.attempt.as_ref().unwrap();
                assert!(attempt.uncertain && !attempt.stopped);
                assert!(view.body.contains("Unresolved outcome"));
                assert!(view.body.contains("Termination confirmed</dt><dd>no"));
            } else {
                assert!(committed.attempt.as_ref().unwrap().stopped);
                assert!(!committed.attempt.as_ref().unwrap().uncertain);
            }
        }
        for rendered in [&page.body, &view.body] {
            assert!(!rendered.contains("<script>"));
            assert!(!rendered.contains("<img"));
            assert!(!rendered.contains("<i>validation"));
            assert!(!rendered.contains("<b>summary"));
        }
        assert_eq!(task(&fixture.controller, &id).await, committed);
    }
}

#[path = "product/model_allowance.rs"]
mod model_allowance;

// Controller-backed directory tests. Synthetic worker reports are not a live
// browser, provider, supplied executor or protected producer qualification.
fn agent_submission(operation: &str, agent: &str, revision: &str) -> Vec<u8> {
    String::from_utf8(submission(operation, "agent directory fixture", ""))
        .unwrap()
        .replace(
            "definition_id=coding-default",
            &format!("definition_id={}", encode(agent)),
        )
        .replace(
            "definition_revision=revision-1",
            &format!("definition_revision={}", encode(revision)),
        )
        .into_bytes()
}

#[tokio::test]
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST provide an agent list.
async fn agent_directory_uses_distinct_bindings_and_all_bounded_pages() {
    let fixture = Fixture::new().await;
    let empty = ui(&fixture.controller, "GET", "/agents", vec![]).await;
    assert_eq!(empty.status, 200);
    assert!(empty.body.contains("No agents found"));
    let controller = fixture.controller.clone();
    let ids: Vec<String> = (0..52).map(|index| format!("agent-{index:03}")).collect();
    for (index, id) in ids.iter().enumerate() {
        let created = ui(
            &controller,
            "POST",
            "/tasks",
            agent_submission(&format!("agent-page-{index}"), id, "revision-1"),
        )
        .await;
        assert_eq!(created.status, 303, "{}", created.body);
    }
    let mut after = String::new();
    let mut observed = Vec::new();
    for _ in 0..4 {
        let route = if after.is_empty() {
            "/agents".into()
        } else {
            format!("/agents?after={}", encode(&after))
        };
        let page = ui(&controller, "GET", &route, vec![]).await;
        assert_eq!(page.status, 200, "{}", page.body);
        let present = ids
            .iter()
            .filter(|id| page.body.contains(&format!("href=\"/agents/{id}\"")))
            .cloned()
            .collect::<Vec<_>>();
        if present.is_empty() {
            assert!(page.body.contains("No agents found"));
            assert!(!page.body.contains("Next agent page"));
            break;
        }
        assert!(present.len() <= 50);
        after = present.last().unwrap().to_string();
        assert!(page.body.contains(&format!("/agents?after={after}")));
        assert!(page.body.contains("@ revision-1 (coding)"));
        observed.extend(present);
    }
    assert_eq!(observed, ids);
    for route in [
        "/agents?after=%ZZ",
        "/agents?limit=5000",
        "/agents/agent-000/extra",
        "/agents/",
    ] {
        assert_eq!(
            ui(&controller, "GET", route, vec![]).await.status,
            400,
            "{route}"
        );
    }
    assert_eq!(
        ui(&controller, "GET", "/agents/not-known", vec![])
            .await
            .status,
        404
    );
    assert_eq!(
        ui(
            &controller,
            "POST",
            "/agents/agent-a",
            form(&[("operation_id", "not-agent-control".into())])
        )
        .await
        .status,
        405
    );
}

#[tokio::test]
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST provide an agent list.
async fn agent_sessions_handoff_keeps_original_inputs_replay_revision_and_failure_facts() {
    use borg_runtime::collaboration::{Query, QueryResult};
    async fn session_input(c: &Controller, id: &str) -> (Session, TaskInput, bool) {
        let Data::Collaboration(QueryResult::SessionInput {
            session,
            input,
            current,
        }) = call(
            c,
            Operation::QueryCollaboration {
                query: Query::SessionInput {
                    session_id: id.into(),
                },
            },
        )
        .await
        else {
            panic!("exact accepted session input")
        };
        (session, *input, current)
    }
    async fn history(
        c: &Controller,
        id: &str,
    ) -> borg_runtime::collaboration::Page<borg_runtime::collaboration::HistoryEntry> {
        let Data::Collaboration(QueryResult::TaskHistory(page)) = call(
            c,
            Operation::QueryCollaboration {
                query: Query::TaskHistory {
                    task_id: id.into(),
                    after: None,
                    limit: 10,
                },
            },
        )
        .await
        else {
            panic!("retained task history")
        };
        assert!(page.next_cursor.is_none());
        page
    }
    async fn original(c: &Controller, id: &str) -> Reply {
        let command = Operation::GetOperation {
            operation_id: id.into(),
        };
        let reply = Box::pin(c.handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: format!(
                "directory-original-{}",
                NEXT.fetch_add(1, Ordering::Relaxed)
            ),
            command: command.clone(),
        }))
        .await;
        reply.validate_for(&command).unwrap();
        assert!(
            matches!(&reply.outcome,Outcome::Ok { data:Data::Operation(record) } if record.id==id && record.state=="settled")
        );
        assert_eq!(reply.commit.as_ref().unwrap().operation_id, id);
        reply
    }
    let fixture = Fixture::new().await;
    let first = ui(
        &fixture.controller,
        "POST",
        "/tasks",
        agent_submission("directory-first", "agent-a", "revision-1"),
    )
    .await;
    assert_eq!(first.status, 303, "{}", first.body);
    let first_id = task_id(&first).to_string();
    let other = ui(
        &fixture.controller,
        "POST",
        "/tasks",
        agent_submission("directory-other", "agent-b", "revision-1"),
    )
    .await;
    assert_eq!(other.status, 303);
    let initial = task(&fixture.controller, &first_id).await;
    let initial_session = session_input(&fixture.controller, &initial.session_id).await;
    assert!(initial_session.2);
    assert_eq!(initial_session.1, initial.input);
    let initial_history = history(&fixture.controller, &first_id).await;
    let cross_body = form(&[
        ("operation_id", "directory-cross-agent".into()),
        ("expected_revision", initial.revision.to_string()),
        ("agent_id", "agent-b".into()),
    ]);
    let assigned = ui(
        &fixture.controller,
        "POST",
        &format!("/tasks/{first_id}/assign"),
        cross_body.clone(),
    )
    .await;
    assert_eq!(assigned.status, 303, "{}", assigned.body);
    let retargeted = task(&fixture.controller, &first_id).await;
    assert_eq!(retargeted.id, initial.id);
    assert_eq!(retargeted.assigned_agent_id, "agent-b");
    assert_ne!(retargeted.session_id, initial.session_id);
    assert!(retargeted.revision > initial.revision);
    assert!(
        retargeted.pending_assignment.is_none()
            && retargeted.attempt.is_none()
            && retargeted.result.is_none()
    );
    assert_eq!(retargeted.status, TaskStatus::Queued);
    assert_eq!(retargeted.input.instruction, initial.input.instruction);
    assert_eq!(retargeted.input.repository, initial.input.repository);
    assert_eq!(
        retargeted.input.acceptance_commands,
        initial.input.acceptance_commands
    );
    assert_eq!(retargeted.input.limits, initial.input.limits);
    assert_eq!(retargeted.input.output_schema, initial.input.output_schema);
    assert_eq!(retargeted.input.dependencies, initial.input.dependencies);
    let retained = session_input(&fixture.controller, &initial.session_id).await;
    assert_eq!(
        (retained.0, retained.1),
        (initial_session.0.clone(), initial.input.clone())
    );
    assert!(!retained.2);
    let successor = session_input(&fixture.controller, &retargeted.session_id).await;
    assert_eq!(successor.0.task_id, first_id);
    assert_eq!(successor.0.agent_id, "agent-b");
    assert_eq!(successor.1, retargeted.input);
    assert!(successor.2);
    let cross_history = history(&fixture.controller, &first_id).await;
    assert!(cross_history.items.starts_with(&initial_history.items));
    assert!(
        cross_history
            .items
            .iter()
            .any(|entry| entry.kind == "assignment_activated")
    );
    let original_cross = original(&fixture.controller, "directory-cross-agent").await;
    let replay = ui(
        &fixture.controller,
        "POST",
        &format!("/tasks/{first_id}/assign"),
        cross_body.clone(),
    )
    .await;
    assert_eq!(replay.status, 303, "{}", replay.body);
    let replay_cross = original(&fixture.controller, "directory-cross-agent").await;
    assert_eq!(replay_cross.commit, original_cross.commit);
    assert_eq!(replay_cross.outcome, original_cross.outcome);
    assert_eq!(task(&fixture.controller, &first_id).await, retargeted);
    assert_eq!(history(&fixture.controller, &first_id).await, cross_history);
    let old = ui(&fixture.controller, "GET", "/agents/agent-a", vec![]).await;
    assert!(!old.body.contains(&format!("href=\"/tasks/{first_id}\"")));
    let other_view = ui(&fixture.controller, "GET", "/agents/agent-b", vec![]).await;
    assert!(
        other_view
            .body
            .contains(&format!("href=\"/tasks/{first_id}\""))
    );
    assert!(old.body.contains("Shared agent memory"));
    let historical = ui(
        &fixture.controller,
        "GET",
        &format!("/sessions/{}", encode(&initial.session_id)),
        vec![],
    )
    .await;
    assert_eq!(historical.status, 200, "{}", historical.body);
    assert!(historical.body.contains("Historical — authority closed"));
    assert!(!historical.body.contains("settings?task_id="));
    let current_view = ui(
        &fixture.controller,
        "GET",
        &format!("/sessions/{}", encode(&retargeted.session_id)),
        vec![],
    )
    .await;
    assert_eq!(current_view.status, 200, "{}", current_view.body);
    assert!(
        current_view
            .body
            .contains("<dt>Session authority</dt><dd>Current</dd>")
    );
    let newer = ui(
        &fixture.controller,
        "POST",
        "/tasks",
        agent_submission("directory-revision", "agent-a", "revision-2"),
    )
    .await;
    assert_eq!(newer.status, 303);
    let newer_id = task_id(&newer).to_string();
    let Data::Agent(selected) = call(
        &fixture.controller,
        Operation::GetAgent {
            agent_id: "agent-a".into(),
        },
    )
    .await
    else {
        panic!("selected agent")
    };
    // An explicit task revision does not advance the selected future-task head.
    assert_eq!(selected.definition, initial.input.definition);
    let newer_task = task(&fixture.controller, &newer_id).await;
    assert_eq!(newer_task.input.definition.revision, "revision-2");
    let newer_assigned = ui(
        &fixture.controller,
        "POST",
        &format!("/tasks/{newer_id}/assign"),
        form(&[
            ("operation_id", "directory-newer-pinned".into()),
            ("expected_revision", newer_task.revision.to_string()),
            ("agent_id", "agent-a".into()),
        ]),
    )
    .await;
    assert_eq!(newer_assigned.status, 303);
    let newer_current = task(&fixture.controller, &newer_id).await;
    assert_eq!(newer_current.input, newer_task.input);
    assert_eq!(
        newer_current, newer_task,
        "same-agent assignment must not allocate a session or increment task revision"
    );
    let no_op = original(&fixture.controller, "directory-newer-pinned").await;
    let unchanged_history = history(&fixture.controller, &newer_id).await;
    let replay = ui(
        &fixture.controller,
        "POST",
        &format!("/tasks/{newer_id}/assign"),
        form(&[
            ("operation_id", "directory-newer-pinned".into()),
            ("expected_revision", newer_task.revision.to_string()),
            ("agent_id", "agent-a".into()),
        ]),
    )
    .await;
    assert_eq!(replay.status, 303);
    let no_op_replay = original(&fixture.controller, "directory-newer-pinned").await;
    assert_eq!(no_op_replay.commit, no_op.commit);
    assert_eq!(no_op_replay.outcome, no_op.outcome);
    assert_eq!(task(&fixture.controller, &newer_id).await, newer_task);
    assert_eq!(
        history(&fixture.controller, &newer_id).await,
        unchanged_history
    );
    let Data::Agent(after_newer_assignment) = call(
        &fixture.controller,
        Operation::GetAgent {
            agent_id: "agent-a".into(),
        },
    )
    .await
    else {
        panic!("selected agent")
    };
    assert_eq!(after_newer_assignment, selected);
    let new = ui(&fixture.controller, "GET", "/agents/agent-a", vec![]).await;
    assert!(new.body.contains("agent-a @ revision-2 (coding)"));
    assert!(new.body.contains("agent-a @ revision-1 (coding)"));
    assert_eq!(
        task(&fixture.controller, &first_id)
            .await
            .input
            .definition
            .revision,
        "revision-1"
    );
    // A genuine cross-agent return to selected A1 creates another session.
    // The independent stale-CAS check below uses the original pre-handoff revision.
    let assigned = ui(
        &fixture.controller,
        "POST",
        &format!("/tasks/{first_id}/assign"),
        form(&[
            ("operation_id", "directory-old-pinned".into()),
            ("expected_revision", retargeted.revision.to_string()),
            ("agent_id", "agent-a".into()),
        ]),
    )
    .await;
    assert_eq!(assigned.status, 303);
    let current = task(&fixture.controller, &first_id).await;
    assert!(current.revision > retargeted.revision);
    assert_ne!(current.session_id, initial.session_id);
    assert_ne!(current.session_id, retargeted.session_id);
    assert_eq!(current.id, initial.id);
    assert_eq!(current.input, initial.input);
    let old_b = session_input(&fixture.controller, &retargeted.session_id).await;
    assert_eq!((old_b.0, old_b.1), (successor.0, successor.1));
    assert!(!old_b.2);
    let current_input = session_input(&fixture.controller, &current.session_id).await;
    assert_eq!(current_input.1, initial.input);
    assert!(current_input.2);
    let returned_history = history(&fixture.controller, &first_id).await;
    assert!(returned_history.items.starts_with(&cross_history.items));
    // Original replay must not rewind current ownership to its old B session.
    let replay = ui(
        &fixture.controller,
        "POST",
        &format!("/tasks/{first_id}/assign"),
        cross_body,
    )
    .await;
    assert_eq!(replay.status, 303);
    assert_eq!(task(&fixture.controller, &first_id).await, current);
    assert_eq!(
        history(&fixture.controller, &first_id).await,
        returned_history
    );
    let after_return_original = original(&fixture.controller, "directory-cross-agent").await;
    assert_eq!(after_return_original.commit, original_cross.commit);
    assert_eq!(after_return_original.outcome, original_cross.outcome);
    assert_eq!(current.assigned_agent_id, "agent-a");
    let Data::Agent(after_old_assignment) = call(
        &fixture.controller,
        Operation::GetAgent {
            agent_id: "agent-a".into(),
        },
    )
    .await
    else {
        panic!("selected agent")
    };
    assert_eq!(after_old_assignment, selected);
    let rejected = ui(
        &fixture.controller,
        "POST",
        &format!("/tasks/{first_id}/assign"),
        form(&[
            ("operation_id", "directory-stale".into()),
            ("expected_revision", initial.revision.to_string()),
            ("agent_id", "agent-a".into()),
        ]),
    )
    .await;
    assert_eq!(rejected.status, 409);
    assert_eq!(task(&fixture.controller, &first_id).await, current);
    call(
        &fixture.controller,
        Operation::RegisterWorker {
            operation_id: "directory-worker".into(),
            worker: Worker {
                id: "worker-not-agent".into(),
                incarnation: "inc".into(),
                coding_capacity: 1,
                native_capacity: 0,
            },
        },
    )
    .await;
    let Data::Commands(commands) = call(
        &fixture.controller,
        Operation::AwaitCommands {
            operation_id: "directory-next".into(),
            worker_id: "worker-not-agent".into(),
            incarnation: "inc".into(),
            acknowledged: vec![],
        },
    )
    .await
    else {
        panic!("commands")
    };
    let attempt = commands
        .iter()
        .find_map(|command| match command {
            WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == first_id => {
                Some(attempt.clone())
            }
            _ => None,
        })
        .unwrap();
    let active = ui(&fixture.controller, "GET", "/agents/agent-a", vec![]).await;
    assert!(active.body.contains("<td>active</td>"));
    assert!(
        active
            .body
            .contains("Attempt phase: assigned; termination confirmed: no")
    );
    report(
        &fixture.controller,
        &attempt,
        Observation::ExecutionUncertain {
            message: "uncertain <effect>".into(),
        },
    )
    .await;
    let failed = ui(&fixture.controller, "GET", "/agents/agent-a", vec![]).await;
    assert!(failed.body.contains("<td>failed</td>"));
    assert!(
        failed
            .body
            .contains("Unresolved outcome: execution or effects remain uncertain")
    );
    assert!(failed.body.contains("termination confirmed: no"));
    let directory = ui(&fixture.controller, "GET", "/agents", vec![]).await;
    assert!(!directory.body.contains("worker-not-agent"));
    assert_eq!(attempt.session_id, current.session_id);
    let historical = ui(
        &fixture.controller,
        "GET",
        &format!("/sessions/{}", encode(&initial.session_id)),
        vec![],
    )
    .await;
    assert_eq!(historical.status, 200);
    assert!(historical.body.contains("Historical — authority closed"));
    assert!(
        !historical.body.contains(&attempt.id),
        "historical session must not borrow successor attempt"
    );
    let old_input = session_input(&fixture.controller, &initial.session_id).await;
    assert_eq!(old_input.1, initial.input);
    assert!(!old_input.2);
}

async fn next_ui_attempt(controller: &Controller, worker: &Worker, task_id: &str) -> Attempt {
    let Data::Commands(commands) = call(
        controller,
        Operation::AwaitCommands {
            operation_id: format!("ui-next-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
            worker_id: worker.id.clone(),
            incarnation: worker.incarnation.clone(),
            acknowledged: vec![],
        },
    )
    .await
    else {
        panic!("commands")
    };
    commands
        .iter()
        .find_map(|command| match command {
            WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == task_id => {
                Some(attempt.clone())
            }
            _ => None,
        })
        .expect("expected task assignment")
}

// The product reads real durable parent/wait/result transitions; these worker
// observations are controlled fixtures, not a model or supplied-executor run.
#[tokio::test]
async fn delegation_views_follow_committed_wait_child_and_parent_continuation() {
    let fixture = Fixture::new().await;
    let deadline = now() + 3_600_000;
    let flow = serde_json::to_string(&workflow(deadline)).unwrap();
    let submitted = ui(
        &fixture.controller,
        "POST",
        "/tasks",
        native_submission("ui-delegating", deadline, Some(&flow)),
    )
    .await;
    assert_eq!(submitted.status, 303, "{}", submitted.body);
    let parent_id = task_id(&submitted).to_string();
    let worker = Worker {
        id: "ui-both-worker".into(),
        incarnation: "inc".into(),
        coding_capacity: 1,
        native_capacity: 1,
    };
    call(
        &fixture.controller,
        Operation::RegisterWorker {
            operation_id: "ui-both-register".into(),
            worker: worker.clone(),
        },
    )
    .await;
    let attempt = next_ui_attempt(&fixture.controller, &worker, &parent_id).await;
    report(
        &fixture.controller,
        &attempt,
        Observation::Usage {
            native_diagnostic_reserved: false,
            model_requests: 1,
            input_tokens: None,
            output_tokens: None,
        },
    )
    .await;
    let parent = task(&fixture.controller, &parent_id).await;
    let child_input =
        borg_runtime::delegation::coding_child(&parent.input, "bounded <edit>").unwrap();
    let Data::Task(child) = call(
        &fixture.controller,
        Operation::DelegateTask {
            operation_id: "ui-delegate".into(),
            parent_task_id: parent_id.clone(),
            parent_attempt_id: attempt.id.clone(),
            generation: attempt.generation,
            child: child_input,
        },
    )
    .await
    else {
        panic!("child")
    };
    let waiting = ui(
        &fixture.controller,
        "GET",
        &format!("/tasks/{parent_id}"),
        vec![],
    )
    .await;
    assert!(waiting.body.contains(&format!(
        "Waiting for child: <a href=\"/tasks/{}\"",
        child.id
    )));
    assert!(!waiting.body.contains("<h2>Accepted result</h2>"));
    let agent = ui(
        &fixture.controller,
        "GET",
        "/agents/native-analysis",
        vec![],
    )
    .await;
    assert!(agent.body.contains("Waiting for child:"));
    let child_view = ui(
        &fixture.controller,
        "GET",
        &format!("/tasks/{}", child.id),
        vec![],
    )
    .await;
    assert!(
        child_view
            .body
            .contains(&format!("Parent task: <a href=\"/tasks/{parent_id}\""))
    );
    assert!(child_view.body.contains("bounded &lt;edit&gt;"));
    report(
        &fixture.controller,
        &attempt,
        Observation::ProcessTreeStopped,
    )
    .await;
    let child_attempt = next_ui_attempt(&fixture.controller, &worker, &child.id).await;
    let checkpoint = borg_runtime::worker::git::checkpoint(
        child.input.repository.as_ref().unwrap(),
        &child.id,
        &child_attempt.id,
        "a".repeat(40),
    )
    .unwrap();
    call(&fixture.controller,Operation::PrepareOperation{operation_id:checkpoint.operation_id.clone(),task_id:child.id.clone(),attempt_id:child_attempt.id.clone(),generation:child_attempt.generation,kind:"checkpoint".into(),intended:serde_json::json!({"registration":checkpoint.registration,"repository":checkpoint.repository,"commit":checkpoint.commit,"remote_ref":checkpoint.remote_ref})}).await;
    call(
        &fixture.controller,
        Operation::SettleOperation {
            settlement_id: "ui-child-published".into(),
            operation_id: checkpoint.operation_id.clone(),
            task_id: child.id.clone(),
            attempt_id: child_attempt.id.clone(),
            generation: child_attempt.generation,
            outcome: serde_json::json!({"acknowledged":true}),
            checkpoint: Some(checkpoint.clone()),
        },
    )
    .await;
    report(
        &fixture.controller,
        &child_attempt,
        Observation::ExecutorOutcome {
            success: true,
            summary: "child executed".into(),
        },
    )
    .await;
    report(
        &fixture.controller,
        &child_attempt,
        Observation::ProcessTreeStopped,
    )
    .await;
    report(
        &fixture.controller,
        &child_attempt,
        Observation::ResultCandidate {
            checks_passed: true,
            summary: "child <accepted>".into(),
            checkpoint: Some(checkpoint),
            validation: vec!["controlled checks".into()],
            output: Some(serde_json::json!({"edit":true})),
        },
    )
    .await;
    let ready = ui(
        &fixture.controller,
        "GET",
        &format!("/tasks/{parent_id}"),
        vec![],
    )
    .await;
    assert!(ready.body.contains("Recorded child outcome"));
    assert!(ready.body.contains("child &lt;accepted&gt;"));
    assert!(ready.body.contains(&format!(
        "href=\"/tasks/{}/result\">Accepted child result",
        child.id
    )));
    assert!(
        ready
            .body
            .contains("A child outcome does not by itself complete this task")
    );
    assert!(!ready.body.contains("<h2>Accepted result</h2>"));
    let resumed = next_ui_attempt(&fixture.controller, &worker, &parent_id).await;
    assert!(resumed.generation > attempt.generation);
    report(
        &fixture.controller,
        &resumed,
        Observation::Usage {
            native_diagnostic_reserved: false,
            model_requests: 1,
            input_tokens: None,
            output_tokens: None,
        },
    )
    .await;
    report(
        &fixture.controller,
        &resumed,
        Observation::NativeLoopCompleted {
            requests: Vec::new(),
            final_response_index: None,
            model_requests: 1,
            calls: vec![],
        },
    )
    .await;
    report(
        &fixture.controller,
        &resumed,
        Observation::ExecutorOutcome {
            success: true,
            summary: "native continuation".into(),
        },
    )
    .await;
    report(
        &fixture.controller,
        &resumed,
        Observation::ProcessTreeStopped,
    )
    .await;
    report(
        &fixture.controller,
        &resumed,
        Observation::ResultCandidate {
            checks_passed: true,
            summary: "parent final".into(),
            checkpoint: None,
            validation: vec![
                "controlled final schema".into(),
                borg_runtime::native_loop_contract::evidence(&resumed.id),
            ],
            output: Some(serde_json::json!({"done":true})),
        },
    )
    .await;
    let finished = ui(
        &fixture.controller,
        "GET",
        &format!("/tasks/{parent_id}"),
        vec![],
    )
    .await;
    assert!(finished.body.contains("<h2>Accepted result</h2>"));
    assert!(finished.body.contains("parent final"));
    assert!(finished.body.contains("Recorded child outcome"));
    assert!(finished.body.contains("Status</dt><dd>succeeded"));
    let agent = ui(&fixture.controller, "GET", "/agents/coding-child", vec![]).await;
    assert!(agent.body.contains("<td>succeeded</td>"));
    assert!(
        agent
            .body
            .contains(&format!("Parent task: <a href=\"/tasks/{parent_id}\""))
    );
}

#[tokio::test]
async fn agent_assigned_task_pages_include_every_retained_assignment_once() {
    let fixture = Fixture::new().await;
    let mut ids = Vec::new();
    for index in 0..51 {
        let created = ui(
            &fixture.controller,
            "POST",
            "/tasks",
            agent_submission(
                &format!("agent-task-page-{index}"),
                "one-agent",
                "revision-1",
            ),
        )
        .await;
        assert_eq!(created.status, 303, "{}", created.body);
        ids.push(task_id(&created).to_string());
    }
    let first = ui(&fixture.controller, "GET", "/agents/one-agent", vec![]).await;
    assert_eq!(first.status, 200);
    for id in &ids[..50] {
        assert!(first.body.contains(&format!("href=\"/tasks/{id}\"")));
    }
    assert!(!first.body.contains(&format!("href=\"/tasks/{}\"", ids[50])));
    assert!(
        first
            .body
            .contains(&format!("/agents/one-agent?after={}", ids[49]))
    );
    let last = ui(
        &fixture.controller,
        "GET",
        &format!("/agents/one-agent?after={}", ids[49]),
        vec![],
    )
    .await;
    assert!(last.body.contains(&format!("href=\"/tasks/{}\"", ids[50])));
    for id in &ids[..50] {
        assert!(!last.body.contains(&format!("href=\"/tasks/{id}\"")));
    }
    assert!(
        last.body
            .contains(&format!("/agents/one-agent?after={}", ids[50]))
    );
    let end = ui(
        &fixture.controller,
        "GET",
        &format!("/agents/one-agent?after={}", ids[50]),
        vec![],
    )
    .await;
    assert!(end.body.contains("No tasks on this page"));
    assert!(!end.body.contains("Next assigned-task page"));
}

#[path = "product/template_instances.rs"]
mod template_instances;

#[path = "product/template_settings.rs"]
mod template_settings;

#[path = "product/collaboration_views.rs"]
mod collaboration_views;

// Admission policy is checked after exact receipt lookup; the existing product
// mutation_reply maps its durable refusal to409, not a draft parsing error.
async fn assert_recorded_policy_refusal(
    controller: &Controller,
    operation: &str,
    response: &UiResponse,
) {
    assert_eq!(response.status, 409, "{}", response.body);
    assert!(response.location.is_none());
    assert!(response.body.contains("Action refused"));
    assert!(response.body.contains(&format!("<code>{operation}</code>")));
    let Data::Operation(record) = call(
        controller,
        Operation::GetOperation {
            operation_id: operation.into(),
        },
    )
    .await
    else {
        panic!("missing refusal receipt")
    };
    assert_eq!(record.id, operation);
    let outcome: Outcome =
        serde_json::from_value(record.outcome.expect("durable outcome")).unwrap();
    assert!(matches!(outcome, Outcome::Refused { code, .. } if code == "invalid_input"));
}

#[tokio::test]
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# The browser interface MUST expose the effective limits applicable to displayed work.
async fn limits_routes_distinguish_current_defaults_from_accepted_task_bounds_after_restart() {
    fn section_json(body: &str, heading: &str) -> serde_json::Value {
        let after_heading = body.split_once(heading).expect("expected section").1;
        let encoded = after_heading
            .split_once("<pre>")
            .unwrap()
            .1
            .split_once("</pre>")
            .unwrap()
            .0;
        let decoded = encoded
            .replace("&quot;", "\"")
            .replace("&#39;", "'")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&amp;", "&");
        serde_json::from_str(&decoded).expect("rendered metadata must remain valid JSON")
    }
    fn assert_task_bounds(body: &str, accepted: &Task) {
        let section = body
            .split_once("<h2>Accepted task limits</h2>")
            .unwrap()
            .1
            .split_once("</dl>")
            .unwrap()
            .0;
        for (label, value) in [
            (
                "Deadline (Unix milliseconds)",
                accepted.input.limits.deadline_unix_ms.to_string(),
            ),
            (
                "Model requests",
                accepted.input.limits.model_requests.to_string(),
            ),
            (
                "Output bytes",
                accepted.input.limits.output_bytes.to_string(),
            ),
            (
                "Complete result bytes",
                accepted.input.limits.result_bytes.to_string(),
            ),
            (
                "Maximum delegation depth",
                accepted.input.limits.max_delegation_depth.to_string(),
            ),
        ] {
            assert!(
                section.contains(&format!("<dt>{label}</dt><dd>{value}</dd>")),
                "missing accepted {label}"
            );
        }
        assert!(section.contains(
            "<dt>Accepted global metadata revision</dt><dd>ui-accepted&lt;&amp;&gt;</dd>"
        ));
        assert!(section.contains("<dt>Accepted agent revision</dt><dd>revision-1</dd>"));
    }

    let root = std::env::temp_dir().join(format!(
        "borg-product-limits-{}-{}-{}",
        std::process::id(),
        now(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("tasks.sqlite");
    let mut cfg = config();
    let original = std::sync::Arc::make_mut(&mut cfg.limits);
    original.revision = "ui-accepted<&>".into();
    let mut coding_override = original.execution.coding.clone();
    coding_override.progress_chars = 123;
    let overrides = borg_runtime::limits::ExecutionLimitOverrides {
        coding: Some(coding_override.clone()),
        ..Default::default()
    };
    let c = Controller::open_with_config(&path, cfg.clone()).unwrap();
    register_fixture_repository(&c).await;
    let mut body = submission("limits-browser-submit", "Inspect retained limits", "");
    body.extend_from_slice(b"&");
    body.extend(form(&[
        (
            "limit_overrides",
            serde_json::to_string(&overrides).unwrap(),
        ),
        ("result_bytes", "12345".into()),
    ]));
    let submitted = ui(&c, "POST", "/tasks", body).await;
    let id = task_id(&submitted).to_owned();
    let accepted = task(&c, &id).await;
    let snapshot = accepted
        .input
        .resolved_limits
        .clone()
        .expect("admission must retain exact metadata");
    assert_eq!(snapshot.global_revision, "ui-accepted<&>");
    assert_eq!(snapshot.values.coding, coding_override);
    assert_ne!(
        snapshot.values.coding,
        snapshot.global_metadata.execution.coding
    );
    assert_eq!(accepted.input.limits.result_bytes, 12345);
    let selected_path = format!("/limits?task_id={}", encode(&id));
    let initial = ui(&c, "GET", &selected_path, vec![]).await;
    assert_eq!(initial.status, 200, "{}", initial.body);
    assert_task_bounds(&initial.body, &accepted);
    drop(c);
    // Wait for the actual exclusive owner to drain before reopening; no DB file
    // rewrite, fake migration, or broad retry of failed startup is involved.
    let mut lock_path = path.as_os_str().to_os_string();
    lock_path.push(".controller-lock");
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let lock = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&lock_path)
                .unwrap();
            if lock.try_lock().is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("old controller owner must release before restart");

    let current = std::sync::Arc::make_mut(&mut cfg.limits);
    current.revision = "ui-current-r2".into();
    current.execution.coding.progress_chars = 987;
    current.task_defaults.model_requests = 7;
    current.task_defaults.output_bytes = 8192;
    current.task_defaults.result_bytes = 54321;
    current.task_defaults.max_delegation_depth = 4;
    let expected_global = cfg.limits.clone();
    let reopened = Controller::open_with_config(&path, cfg).unwrap();

    let global = ui(&reopened, "GET", "/limits", vec![]).await;
    assert_eq!(global.status, 200, "{}", global.body);
    assert_eq!(
        section_json(&global.body, "<h2>Current global metadata</h2>"),
        serde_json::to_value(&*expected_global).unwrap()
    );
    assert!(
        !global
            .body
            .contains("<h2>Recorded task execution metadata</h2>")
    );
    assert!(!global.body.contains("<h2>Accepted task limits</h2>"));

    let selected = ui(&reopened, "GET", &selected_path, vec![]).await;
    assert_eq!(selected.status, 200, "{}", selected.body);
    assert_eq!(
        section_json(&selected.body, "<h2>Current global metadata</h2>"),
        serde_json::to_value(&*expected_global).unwrap()
    );
    assert_eq!(
        section_json(&selected.body, "<h2>Recorded task execution metadata</h2>"),
        serde_json::to_value(&snapshot).unwrap()
    );
    assert_task_bounds(&selected.body, &accepted);
    assert!(
        !selected.body.contains("ui-accepted<&>"),
        "metadata is untrusted text, not markup"
    );

    let detail = ui(&reopened, "GET", &format!("/tasks/{id}"), vec![]).await;
    assert_eq!(detail.status, 200, "{}", detail.body);
    assert_task_bounds(&detail.body, &accepted);
    assert!(detail.body.contains(&format!("href=\"{selected_path}\"")));
    assert!(!detail.body.contains("ui-accepted<&>"));
    // Read views never adopt current defaults into an accepted task.
    assert_eq!(task(&reopened, &id).await, accepted);
    assert_eq!(reopened.limits(), expected_global.as_ref());
}

#[tokio::test]
// Traceability scope: Real browser handler renders configured refusal fields;
// rejected submissions do not create controller task state.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# An operation refused by a configured limit MUST identify the limiting metadata field.
async fn browser_limit_refusals_render_fields_without_admitting_mutations() {
    let root = std::env::temp_dir().join(format!(
        "borg-product-limit-refusal-{}-{}-{}",
        std::process::id(),
        now(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let mut cfg = config();
    let metadata = std::sync::Arc::make_mut(&mut cfg.limits);
    metadata.product.request_path_bytes = 32;
    metadata.product.form_fields = 1;
    metadata.product.form_key_bytes = 1;
    let body_bytes = metadata.transport.body_bytes;
    cfg.limits
        .validate()
        .expect("browser refusal fixture metadata");
    let controller = Controller::open_with_config(&root.join("tasks.sqlite"), cfg.clone()).unwrap();
    for (path, body, status, field) in [
        (
            format!("/{}", "x".repeat(32)),
            vec![],
            413,
            "product.request_path_bytes",
        ),
        (
            "/tasks".into(),
            vec![b'x'; body_bytes + 1],
            413,
            "transport.body_bytes",
        ),
        (
            "/tasks".into(),
            b"a=x&b=y".to_vec(),
            400,
            "product.form_fields",
        ),
        (
            "/tasks".into(),
            b"ab=x".to_vec(),
            400,
            "product.form_key_bytes",
        ),
    ] {
        let response = ui(&controller, "POST", &path, body).await;
        assert_eq!(response.status, status, "{}", response.body);
        assert!(
            response
                .body
                .contains(&format!("Configured limit exceeded: {field}")),
            "{}",
            response.body
        );
    }
    let malformed = ui(&controller, "POST", "/tasks", b"=x".to_vec()).await;
    assert_eq!(malformed.status, 400);
    assert!(malformed.body.contains("Invalid form field name"));
    assert!(!malformed.body.contains("Configured limit exceeded"));
    let tasks = call(
        &controller,
        Operation::ListTasks {
            after: None,
            limit: 1,
        },
    )
    .await;
    assert!(matches!(tasks, Data::Tasks(tasks) if tasks.is_empty()));

    std::sync::Arc::make_mut(&mut cfg.limits)
        .product
        .response_bytes = 1;
    let invalid_path = root.join("bounded-view.sqlite");
    let reason = match Controller::open_with_config(&invalid_path, cfg) {
        Ok(_) => panic!("a one-byte response limit must fail configuration validation"),
        Err(reason) => reason,
    };
    assert!(reason.contains("product.response_bytes"), "{reason}");
    assert!(!invalid_path.exists(), "invalid metadata must refuse before database admission");
}

#[path = "product/definition_settings.rs"]
mod definition_settings;

#[path = "product/response_bounds.rs"]
mod response_bounds;

#[path = "product/call_receipts.rs"]
mod call_receipts;

#[path = "product/documents_reviews.rs"]
mod documents_reviews;

#[path = "product/collaboration_work.rs"]
mod collaboration_work;

#[path = "product/collaboration_joined.rs"]
mod collaboration_joined;

#[path = "product/default_task_submission.rs"]
mod default_task_submission;

#[path = "product/host_resources.rs"]
mod host_resources;
