//! Actual local controller projections. No provider or protected-role claim.
use super::*;

#[tokio::test]
async fn shared_records_are_visible_without_manufacturing_agent_write_authority() {
    let fixture = Fixture::new().await;
    let created = ui(
        &fixture.controller,
        "POST",
        "/tasks",
        native_submission("ui-parity", now() + 3_600_000, None),
    )
    .await;
    let id = task_id(&created).to_string();
    let worker = Worker {
        id: "worker-parity".into(),
        incarnation: "incarnation-1".into(),
        coding_capacity: 0,
        native_capacity: 2,
    };
    call(
        &fixture.controller,
        Operation::RegisterWorker {
            operation_id: "ui-parity-worker".into(),
            worker: worker.clone(),
        },
    )
    .await;
    let Data::Commands(commands) = call(
        &fixture.controller,
        Operation::AwaitCommands {
            operation_id: "ui-parity-launch".into(),
            worker_id: worker.id.clone(),
            incarnation: worker.incarnation.clone(),
            acknowledged: vec![],
        },
    )
    .await
    else {
        panic!("no commands")
    };
    let attempt = commands
        .iter()
        .find_map(|command| match command {
            WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == id => {
                Some(attempt.clone())
            }
            _ => None,
        })
        .unwrap();
    report(
        &fixture.controller,
        &attempt,
        Observation::Progress {
            text: "Review <source> & facts".into(),
            output: String::new(),
            truncated: false,
        },
    )
    .await;
    call(
        &fixture.controller,
        Operation::SetMemory {
            operation_id: "ui-parity-memory".into(),
            task_id: id.clone(),
            attempt_id: attempt.id.clone(),
            generation: attempt.generation,
            key: "facts/<source>".into(),
            expected_revision: None,
            value: serde_json::json!({"note":"<script>not executable</script>"}),
        },
    )
    .await;
    let saved = task(&fixture.controller, &id).await;
    for (route, expected) in [
        ("/collaboration".to_string(), "does not act as that agent"),
        ("/workers".into(), "worker-parity"),
        ("/workers/worker-parity".into(), "incarnation-1"),
        (
            "/definitions/native-analysis?revision=revision-1".into(),
            SELECTED_MODEL,
        ),
        (
            format!("/tasks/{id}/history"),
            "Review &lt;source&gt; &amp; facts",
        ),
        (format!("/tasks/{id}/attempts"), "reported_model_requests"),
        (format!("/tasks/{id}/checkpoints"), "publication records"),
        (
            format!("/agents/{}/memory", saved.assigned_agent_id),
            "facts/&lt;source&gt;",
        ),
        (
            format!(
                "/agents/{}/memory/facts%2F%3Csource%3E",
                saved.assigned_agent_id
            ),
            "&lt;script&gt;not executable&lt;/script&gt;",
        ),
    ] {
        let r = ui(&fixture.controller, "GET", &route, vec![]).await;
        assert_eq!(r.status, 200, "{route}: {}", r.body);
        assert!(r.body.contains(expected), "{route}: {}", r.body);
        assert!(!r.body.contains("<script>not executable"));
    }
    let attempts = ui(
        &fixture.controller,
        "GET",
        &format!("/tasks/{id}/attempts"),
        vec![],
    )
    .await;
    assert!(attempts.body.contains("input_tokens&quot;: null"));
    assert!(attempts.body.contains("output_tokens&quot;: null"));
    for route in [
        format!("/agents/{}/memory", saved.assigned_agent_id),
        format!("/tasks/{id}/progress"),
        format!("/tasks/{id}/delegate"),
    ] {
        let r = ui(
            &fixture.controller,
            "POST",
            &route,
            form(&[
                ("operation_id", "cannot-impersonate".into()),
                ("expected_revision", saved.revision.to_string()),
            ]),
        )
        .await;
        assert!(matches!(r.status, 404 | 405), "{route}:{}", r.body);
    }
    assert_eq!(task(&fixture.controller, &id).await, saved);
}

#[tokio::test]
async fn worker_pages_use_bounded_shared_cursors_and_reject_scope_confusion() {
    let fixture = Fixture::new().await;
    for n in 0..51 {
        call(
            &fixture.controller,
            Operation::RegisterWorker {
                operation_id: format!("register-{n}"),
                worker: Worker {
                    id: format!("worker-{n:03}"),
                    incarnation: "one".into(),
                    coding_capacity: 1,
                    native_capacity: 1,
                },
            },
        )
        .await;
    }
    let first = ui(&fixture.controller, "GET", "/workers", vec![]).await;
    assert_eq!(first.status, 200);
    assert!(first.body.contains("worker-049"));
    assert!(!first.body.contains("worker-050"));
    let marker = "href=\"/workers?after=";
    let start = first.body.find(marker).unwrap() + "href=\"".len();
    let end = first.body[start..].find('"').unwrap() + start;
    let second = ui(&fixture.controller, "GET", &first.body[start..end], vec![]).await;
    assert_eq!(second.status, 200);
    assert!(second.body.contains("worker-050"));
    assert!(!second.body.contains("Next page"));
    let wrong = first.body[start..end].replacen("/workers?", "/agents/absent/memory?", 1);
    assert_eq!(
        ui(&fixture.controller, "GET", &wrong, vec![]).await.status,
        404
    );
    assert_eq!(
        ui(&fixture.controller, "GET", "/workers?sql=anything", vec![])
            .await
            .status,
        400
    );
}

#[tokio::test]
async fn shared_view_links_and_static_live_code_preserve_forms_and_private_scope() {
    let fixture = Fixture::new().await;
    let created = ui(
        &fixture.controller,
        "POST",
        "/tasks",
        native_submission("ui-links", now() + 3_600_000, None),
    )
    .await;
    let id = task_id(&created);
    let detail = ui(&fixture.controller, "GET", &format!("/tasks/{id}"), vec![]).await;
    for suffix in ["history", "attempts", "checkpoints", "repository", "calls"] {
        assert!(
            detail
                .body
                .contains(&format!("href=\"/tasks/{id}/{suffix}\""))
        );
    }
    assert!(detail.body.contains("aria-label=\"Borg activity\""));
    assert!(detail.body.contains("data-live-refresh=\"true\""));
    let draft = ui(
        &fixture.controller,
        "GET",
        "/tasks/new?executor=native",
        vec![],
    )
    .await;
    assert!(draft.body.contains("data-live-refresh=\"false\""));
    assert!(draft.body.contains("fresh session"));
    assert!(
        draft
            .body
            .contains("name=\"execution_profile\" value=\"collaboration_v1\"")
    );
    let script = ui(&fixture.controller, "GET", "/ui/live.js", vec![]).await;
    assert_eq!(script.status, 200);
    assert!(script.content_type.starts_with("text/javascript"));
    assert!(!script.body.contains("/api/operations"));
    assert!(!script.body.contains("method:\"POST\""));
}

#[tokio::test]
async fn live_endpoints_use_durable_string_cursors_and_observe_committed_change() {
    let f = Fixture::new().await;
    let made = ui(
        &f.controller,
        "POST",
        "/tasks",
        native_submission("live-endpoints", now() + 3_600_000, None),
    )
    .await;
    let id = task_id(&made).to_string();
    let before = task(&f.controller, &id).await;
    let activity = ui(&f.controller, "GET", "/ui/activity", vec![]).await;
    assert_eq!(activity.status, 200);
    let a: serde_json::Value = serde_json::from_str(&activity.body).unwrap();
    let cursor = a["cursor"].as_str().unwrap().to_string();
    assert!(!a["items"].as_array().unwrap().is_empty());
    assert!(
        a["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|e| e["sequence"].is_string()
                && e["task_revision"].is_string()
                && e.get("body").is_none())
    );
    let baseline = ui(&f.controller, "GET", "/ui/changes?after=0", vec![]).await;
    assert_eq!(baseline.status, 200);
    let b: serde_json::Value = serde_json::from_str(&baseline.body).unwrap();
    let revision = b["revision"].as_str().unwrap().to_string();
    let c = f.controller.clone();
    let route = format!("/ui/changes?after={revision}");
    let waiter = tokio::spawn(async move { ui(&c, "GET", &route, vec![]).await });
    let cancelled = ui(
        &f.controller,
        "POST",
        &format!("/tasks/{id}/cancel"),
        form(&[
            ("operation_id", "live-cancel".into()),
            ("expected_revision", before.revision.to_string()),
        ]),
    )
    .await;
    assert_eq!(cancelled.status, 303);
    let changed = tokio::time::timeout(std::time::Duration::from_secs(2), waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(changed.status, 200);
    let change: serde_json::Value = serde_json::from_str(&changed.body).unwrap();
    assert_eq!(change["changed"], true);
    assert!(!change["reset"].as_bool().unwrap());
    assert!(
        change["revision"].as_str().unwrap().parse::<u64>().unwrap()
            > revision.parse::<u64>().unwrap()
    );
    let after = ui(
        &f.controller,
        "GET",
        &format!("/ui/activity?after={cursor}"),
        vec![],
    )
    .await;
    let a: serde_json::Value = serde_json::from_str(&after.body).unwrap();
    assert!(
        a["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["task_id"] == id)
    );
    assert_eq!(
        ui(&f.controller, "GET", "/ui/activity?after=01", vec![])
            .await
            .status,
        400
    );
    assert_eq!(
        ui(
            &f.controller,
            "GET",
            "/ui/changes?after=9223372036854775808",
            vec![]
        )
        .await
        .status,
        400
    );
}

#[tokio::test]
async fn all_repository_views_use_exact_selected_source_after_stop_without_worker_impersonation() {
    let root = std::env::temp_dir().join(format!(
        "borg-ui-repository-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let seed = root.join("seed");
    std::fs::create_dir(&seed).unwrap();
    let git = |args: &[&str]| {
        let o = std::process::Command::new("/usr/bin/git")
            .args([
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "user.name=UI fixture",
                "-c",
                "user.email=fixture@invalid",
            ])
            .args(args)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", &root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .current_dir(&seed)
            .output()
            .unwrap();
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
        String::from_utf8(o.stdout).unwrap().trim().to_string()
    };
    git(&["init", "-q"]);
    std::fs::write(
        seed.join("answer.txt"),
        "<script>not executable</script>\nsecond line\n",
    )
    .unwrap();
    git(&["add", "answer.txt"]);
    git(&["commit", "-qm", "untrusted <message>"]);
    let commit = git(&["rev-parse", "HEAD"]);
    let bare = root.join("selected.git");
    git(&[
        "clone",
        "--quiet",
        "--bare",
        "--no-hardlinks",
        seed.to_str().unwrap(),
        bare.to_str().unwrap(),
    ]);
    let mut cfg = config();
    cfg.repository_read.store_root = Some(root.clone());
    let c = Controller::open_with_config(root.join("tasks.db"), cfg).unwrap();
    call(
        &c,
        Operation::RegisterRepository {
            operation_id: "register-selected".into(),
            registration: borg_runtime::repository_catalogue::Registration {
                repository_id: "selected".into(),
                registration_revision: "r1".into(),
                display_name: "Selected UI repository".into(),
                source_endpoint: "selected".into(),
                checkpoint_endpoint: "selected".into(),
                source_store_key: "selected.git".into(),
                checkpoint_store_key: "selected.git".into(),
            },
        },
    )
    .await;
    let mut body = native_submission("repository-ui", now() + 3_600_000, None);
    body.extend_from_slice(
        format!("&repository_read_id=selected&repository_read_revision=r1&repository_read_commit={commit}").as_bytes(),
    );
    let made = ui(&c, "POST", "/tasks", body).await;
    assert_eq!(made.status, 303, "{}", made.body);
    let id = task_id(&made);
    let task_before = task(&c, id).await;
    assert_eq!(
        ui(
            &c,
            "POST",
            &format!("/tasks/{id}/cancel"),
            form(&[
                ("operation_id", "repository-stop".into()),
                ("expected_revision", task_before.revision.to_string())
            ])
        )
        .await
        .status,
        303
    );
    let stopped = task(&c, id).await;
    assert!(stopped.attempt.is_none());
    for query in [
        "view=tree",
        "view=read&path=answer.txt",
        "view=search&needle=executable",
        "view=diff&source=start&base_source=start",
        "view=history",
    ] {
        let r = ui(
            &c,
            "GET",
            &format!("/tasks/{id}/repository?{query}"),
            vec![],
        )
        .await;
        assert_eq!(r.status, 200, "{query}: {}", r.body);
        assert!(r.body.contains(&commit));
        assert!(r.body.contains("untrusted data"));
        assert!(!r.body.contains("<script>not executable"));
        if query.starts_with("view=read") {
            assert!(
                r.body
                    .contains("&lt;script&gt;not executable&lt;/script&gt;")
            );
        }
    }
    for (query, status) in [
        ("view=read&path=..%2Foutside", 409),
        ("view=tree&repository=%2Fetc", 400),
        ("view=tree&source=arbitrary", 400),
    ] {
        assert_eq!(
            ui(
                &c,
                "GET",
                &format!("/tasks/{id}/repository?{query}"),
                vec![]
            )
            .await
            .status,
            status
        );
    }
    assert_eq!(task(&c, id).await, stopped);
}
