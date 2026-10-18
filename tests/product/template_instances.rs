use super::*;
use serde_json::json;

fn input_value(page: &UiResponse, name: &str) -> String {
    let tail = page.body.split(&format!("name=\"{name}\"")).nth(1).unwrap();
    tail.split("value=\"")
        .nth(1)
        .unwrap()
        .split('"')
        .next()
        .unwrap()
        .into()
}

async fn memory(controller: &Controller, agent: &str) -> Option<MemoryValue> {
    match call(
        controller,
        Operation::GetMemory {
            agent_id: agent.into(),
            key: "agent-note".into(),
        },
    )
    .await
    {
        Data::Memory(value) => value,
        other => panic!("memory: {other:?}"),
    }
}

async fn remember(controller: &Controller, attempt: &Attempt, revision: Option<u64>, value: &str) {
    let result = call(
        controller,
        Operation::SetMemory {
            operation_id: format!("memory-{}-{}", attempt.id, revision.unwrap_or(0)),
            task_id: attempt.task_id.clone(),
            attempt_id: attempt.id.clone(),
            generation: attempt.generation,
            key: "agent-note".into(),
            expected_revision: revision,
            value: json!({"note":value}),
        },
    )
    .await;
    assert!(
        matches!(result, Data::Memory(Some(MemoryValue { revision: actual, .. })) if actual == revision.unwrap_or(0) + 1)
    );
}

// Real controller/SQLite and actual UI handlers with controlled worker reports.
// This proves identity/memory/result semantics, not provider or browser execution.
async fn finish(controller: &Controller, attempt: &Attempt, answer: &str) -> Task {
    for observation in [
        Observation::Usage {
            native_diagnostic_reserved: false,
            model_requests: 1,
            input_tokens: None,
            output_tokens: None,
        },
        Observation::NativeLoopCompleted {
            requests: Vec::new(),
            final_response_index: None,
            model_requests: 1,
            calls: vec![],
        },
        Observation::ExecutorOutcome {
            success: true,
            summary: "controlled native completion".into(),
        },
        Observation::ProcessTreeStopped,
        Observation::ResultCandidate {
            checks_passed: true,
            summary: answer.into(),
            checkpoint: None,
            validation: vec![borg_runtime::native_loop_contract::evidence(&attempt.id)],
            output: Some(json!({"answer":answer})),
        },
    ] {
        report(controller, attempt, observation).await;
    }
    let result = task(controller, &attempt.task_id).await;
    assert_eq!(result.status, TaskStatus::Succeeded);
    assert_eq!(
        result.result.as_ref().unwrap().output,
        Some(json!({"answer":answer}))
    );
    result
}

#[tokio::test]
async fn fresh_sessions_share_agent_identity_but_exact_submission_replay_keeps_one_session() {
    let fixture = Fixture::new().await;
    let mut originals = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for executor in ["native", "coding"] {
        for _ in 0..2 {
            let page = ui(
                &fixture.controller,
                "GET",
                &format!("/tasks/new?executor={executor}"),
                vec![],
            )
            .await;
            assert_eq!(page.status, 200);
            assert!(page.body.contains("share that agent's durable memory"));
            assert!(!page.body.contains("name=\"agent_id\""));
            let operation = input_value(&page, "operation_id");
            assert!(seen.insert(operation.clone()));
            let body = if executor == "native" {
                native_submission(&operation, now() + 3_600_000, None)
            } else {
                submission(&operation, "separate session work", "")
            };
            let invalid = String::from_utf8(body.clone())
                .unwrap()
                .replace(&format!("executor={executor}"), "executor=invalid")
                .into_bytes();
            let redisplayed = ui(&fixture.controller, "POST", "/tasks", invalid).await;
            assert_eq!(redisplayed.status, 400);
            assert_eq!(input_value(&redisplayed, "operation_id"), operation);
            let created = ui(&fixture.controller, "POST", "/tasks", body.clone()).await;
            assert_eq!(created.status, 303, "{}", created.body);
            let original = task(&fixture.controller, task_id(&created)).await;
            assert_eq!(original.assigned_agent_id, original.input.definition.id);
            assert_ne!(original.session_id, original.id);
            assert_ne!(original.session_id, original.assigned_agent_id);
            let replay = ui(&fixture.controller, "POST", "/tasks", body.clone()).await;
            assert_eq!(replay.location, created.location);
            originals.push((body, original));
        }
    }
    assert_eq!(
        originals[0].1.assigned_agent_id,
        originals[1].1.assigned_agent_id
    );
    assert_ne!(originals[0].1.session_id, originals[1].1.session_id);
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
async fn same_agent_sessions_execute_concurrently_share_revisioned_memory_and_keep_results_separate()
 {
    let fixture = Fixture::new().await;
    let mut tasks = Vec::new();
    for i in 0..3 {
        let created = ui(
            &fixture.controller,
            "POST",
            "/tasks",
            native_submission(&format!("parallel-session-{i}"), now() + 3_600_000, None),
        )
        .await;
        assert_eq!(created.status, 303, "{}", created.body);
        tasks.push(task(&fixture.controller, task_id(&created)).await);
    }
    let ids: std::collections::BTreeSet<_> = tasks.iter().map(|t| t.session_id.clone()).collect();
    assert_eq!(ids.len(), 3);
    assert!(
        tasks
            .iter()
            .all(|t| t.assigned_agent_id == "native-analysis")
    );
    let worker = Worker {
        id: "session-worker".into(),
        incarnation: "one".into(),
        native_capacity: 3,
        coding_capacity: 0,
    };
    call(
        &fixture.controller,
        Operation::RegisterWorker {
            operation_id: "sessions-register".into(),
            worker: worker.clone(),
        },
    )
    .await;
    let Data::Commands(commands) = call(
        &fixture.controller,
        Operation::AwaitCommands {
            operation_id: "sessions-await".into(),
            worker_id: worker.id.clone(),
            incarnation: worker.incarnation.clone(),
            acknowledged: vec![],
        },
    )
    .await
    else {
        panic!("commands")
    };
    let attempts: Vec<_> = commands
        .into_iter()
        .filter_map(|c| {
            if let WorkerCommand::RunAttempt {
                attempt, session, ..
            } = c
            {
                assert_eq!(attempt.session_id, session.id);
                Some(attempt)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        attempts.len(),
        3,
        "same-agent sessions use available concurrent capacity"
    );
    for t in &tasks {
        assert_eq!(
            task(&fixture.controller, &t.id).await.status,
            TaskStatus::Active
        );
    }
    let first = attempts.iter().find(|a| a.task_id == tasks[0].id).unwrap();
    let second = attempts.iter().find(|a| a.task_id == tasks[1].id).unwrap();
    remember(&fixture.controller, first, None, "shared first value").await;
    assert_eq!(
        memory(&fixture.controller, &tasks[1].assigned_agent_id)
            .await
            .unwrap()
            .value,
        json!({"note":"shared first value"})
    );
    // Concurrent stale memory writes refuse instead of silently overwriting.
    let stale = fixture
        .controller
        .handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: "stale-memory-request".into(),
            command: Operation::SetMemory {
                operation_id: "stale-shared-write".into(),
                task_id: second.task_id.clone(),
                attempt_id: second.id.clone(),
                generation: second.generation,
                key: "agent-note".into(),
                expected_revision: None,
                value: json!({"note":"lost update"}),
            },
        })
        .await;
    assert!(matches!(stale.outcome, Outcome::Refused { .. }));
    remember(&fixture.controller, second, Some(1), "shared second value").await;
    let directory = ui(&fixture.controller, "GET", "/agents", vec![]).await;
    assert_eq!(
        directory
            .body
            .matches("<tr><th scope=\"row\"><a href=\"/agents/native-analysis\"")
            .count(),
        1
    );
    let sessions = ui(
        &fixture.controller,
        "GET",
        "/sessions?agent_id=native-analysis",
        vec![],
    )
    .await;
    assert_eq!(sessions.status, 200);
    for t in &tasks {
        assert!(sessions.body.contains(&t.session_id));
        let detail = ui(
            &fixture.controller,
            "GET",
            &format!("/sessions/{}", t.session_id),
            vec![],
        )
        .await;
        assert_eq!(detail.status, 200);
        assert!(detail.body.contains(&t.id));
        assert!(detail.body.contains("Shared agent memory"));
    }
    let mut completed = Vec::new();
    for (i, a) in attempts.iter().enumerate() {
        completed.push(finish(&fixture.controller, a, &format!("answer-{i}")).await);
    }
    let different = String::from_utf8(native_submission("other-agent", now() + 3_600_000, None))
        .unwrap()
        .replace("definition_id=native-analysis", "definition_id=other-agent");
    let created = ui(
        &fixture.controller,
        "POST",
        "/tasks",
        different.into_bytes(),
    )
    .await;
    assert_eq!(created.status, 303);
    assert!(memory(&fixture.controller, "other-agent").await.is_none());
    let path = fixture.path.clone();
    drop(fixture.controller);
    let reopened = Controller::open_with_config(&path, config()).unwrap();
    for t in completed {
        assert_eq!(task(&reopened, &t.id).await, t);
    }
    assert_eq!(
        memory(&reopened, "native-analysis").await.unwrap().value,
        json!({"note":"shared second value"})
    );
    drop(reopened);
    let db = rusqlite::Connection::open(path).unwrap();
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM sessions", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        4
    );
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM agents", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn session_pages_are_bounded_and_existing_agent_draft_preserves_exact_settings() {
    let fixture = Fixture::new().await;
    let mut expected = Vec::new();
    for i in 0..51 {
        let mut body = native_submission(&format!("session-page-{i}"), now() + 3_600_000, None);
        body.extend_from_slice(b"&web_search_max_tool_calls=2");
        let made = ui(&fixture.controller, "POST", "/tasks", body).await;
        assert_eq!(made.status, 303, "{}", made.body);
        expected.push(task(&fixture.controller, task_id(&made)).await.session_id);
    }
    let first = ui(
        &fixture.controller,
        "GET",
        "/sessions?agent_id=native-analysis",
        vec![],
    )
    .await;
    assert_eq!(first.status, 200);
    assert_eq!(
        expected
            .iter()
            .filter(|id| first.body.contains(&format!("href=\"/sessions/{id}\"")))
            .count(),
        50
    );
    let cursor = first
        .body
        .split("&amp;after=")
        .nth(1)
        .unwrap()
        .split('"')
        .next()
        .unwrap();
    let second = ui(
        &fixture.controller,
        "GET",
        &format!("/sessions?agent_id=native-analysis&after={cursor}"),
        vec![],
    )
    .await;
    assert_eq!(second.status, 200);
    assert_eq!(
        expected
            .iter()
            .filter(|id| second.body.contains(&format!("href=\"/sessions/{id}\"")))
            .count(),
        1
    );
    let draft = ui(
        &fixture.controller,
        "GET",
        "/tasks/new?agent_id=native-analysis",
        vec![],
    )
    .await;
    assert_eq!(draft.status, 200);
    assert_eq!(input_value(&draft, "definition_id"), "native-analysis");
    assert_eq!(input_value(&draft, "definition_revision"), "revision-1");
    assert_eq!(input_value(&draft, "web_search_max_tool_calls"), "2");
    assert!(!draft.body.contains("name=\"agent_id\""));
    let mut body = native_submission(
        &input_value(&draft, "operation_id"),
        now() + 3_600_000,
        None,
    );
    body.extend_from_slice(b"&web_search_max_tool_calls=2");
    let made = ui(&fixture.controller, "POST", "/tasks", body).await;
    assert_eq!(made.status, 303);
    assert_eq!(
        task(&fixture.controller, task_id(&made))
            .await
            .input
            .definition
            .execution_profile,
        Some(ExecutionProfile::CollaborationV1 {
            web_search: Some(NativeSearchProfile { max_tool_calls: 2 })
        })
    );
    for route in [
        "/sessions?limit=5000",
        "/sessions?after=%ZZ",
        "/sessions/not-known?agent_id=wrong",
        "/tasks/new?agent_id=native-analysis&executor=coding",
    ] {
        assert_eq!(
            ui(&fixture.controller, "GET", route, vec![]).await.status,
            400,
            "{route}"
        );
    }
    for route in ["/sessions", "/sessions/not-known"] {
        assert_eq!(
            ui(&fixture.controller, "POST", route, vec![]).await.status,
            405
        );
    }
    assert_eq!(
        ui(&fixture.controller, "GET", "/sessions/not-known", vec![])
            .await
            .status,
        404
    );
}
