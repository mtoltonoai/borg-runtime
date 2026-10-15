//! Synthetic URL userinfo only; no real credential, Git, provider or host access.
//! Historical unknown/transformed credentials are outside this finite guard.
use super::*;

const SECRET: &str = "SYNTHETIC_CRED_URL_7f1a";
fn credential_url() -> String {
    format!("https://synthetic-user:{SECRET}@example.invalid/repository.git")
}
async fn rejected(c: &Controller, operation: Operation) {
    let reply = c
        .handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: "synthetic-credential-refusal".into(),
            command: operation.clone(),
        })
        .await;
    reply.validate_for(&operation).unwrap();
    assert!(
        reply.commit.is_none(),
        "credential-bearing input must not become even a refusal receipt"
    );
    assert!(
        reply.observed_controller_revision.is_none(),
        "guard runs before database dispatch"
    );
    assert_eq!(
        reply.outcome,
        Outcome::Refused {
            code: "invalid_input".into(),
            message: "invalid or credential-bearing repository identities are not accepted".into()
        }
    );
    assert!(!serde_json::to_string(&reply).unwrap().contains(SECRET));
}
async fn absent(c: &Controller, id: &str) {
    assert_eq!(
        code(
            call(
                c,
                Operation::GetOperation {
                    operation_id: id.into()
                }
            )
            .await
        ),
        "not_found"
    );
}
fn no_secret_files(root: &std::path::Path) {
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            no_secret_files(&path);
        } else {
            let bytes = std::fs::read(&path).unwrap();
            assert!(
                !bytes
                    .windows(SECRET.len())
                    .any(|part| part == SECRET.as_bytes()),
                "synthetic credential retained in owned durable fixture"
            );
        }
    }
}
fn repository_mut(input: &mut TaskInput, template: bool) -> &mut Repository {
    if template {
        &mut configured_template_mut(input).repository
    } else {
        input.repository.as_mut().unwrap()
    }
}

#[tokio::test]
async fn credential_repository_and_workflow_inputs_refuse_before_any_durable_receipt() {
    let p = path();
    // A configured native input reaching validation would fail differently.
    // URL admission must refuse before any such CPU helper work is requested.
    let cfg = ControllerConfig {
        validation_executable: PathBuf::from("/usr/bin/false"),
        ..config()
    };
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let urls = [
        format!("https://user:{SECRET}@example.invalid/repo"),
        format!("https://{SECRET}@example.invalid/repo"),
        format!("http://user:{SECRET}%40tail@example.invalid/repo"),
        format!("ssh://git:{SECRET}@example.invalid/repo"),
        format!("git+ssh://git:{SECRET}@example.invalid/repo"),
        format!("https::https://user:{SECRET}@example.invalid/repo"),
        format!("https://user:{SECRET}@[invalid/repo"),
    ];
    let mut index = 0;
    for url in &urls {
        for template in [false, true] {
            for destination in [false, true] {
                let mut input = if template {
                    configured_native("url-template")
                } else {
                    coding("url-task")
                };
                let repository = repository_mut(&mut input, template);
                if destination {
                    repository.checkpoint_remote = url.clone();
                } else {
                    repository.url = url.clone();
                }
                let id = format!("credential-input-{index}");
                index += 1;
                let operation = Operation::SubmitTask {
                    operation_id: id.clone(),
                    input,
                };
                rejected(&c, operation.clone()).await;
                rejected(&c, operation).await;
                absent(&c, &id).await;
            }
        }
    }
    assert!(
        matches!(call(&c, Operation::ListTasks { after: None, limit: 100 }).await, Outcome::Ok { data: Data::Tasks(ref tasks) } if tasks.is_empty())
    );
    drop(c);
    controller_owner_released(&p).await;
    no_secret_files(p.parent().unwrap());
    {
        let db =
            rusqlite::Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        for table in ["receipts", "tasks", "history", "definitions", "agents"] {
            assert_eq!(
                db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                    .get::<_, i64>(0))
                    .unwrap(),
                0,
                "{table}"
            );
        }
    }
    let c = Controller::open_with_config(&p, cfg).unwrap();
    // The invalid preflight did not consume the mutation identity or reserve
    // task/definition state. A clean request with that ID may now commit.
    let accepted = submit(&c, "credential-input-0", coding("clean-after-refusal")).await;
    assert_eq!(accepted.status, TaskStatus::Queued);
    let mut contaminated = accepted.input.clone();
    contaminated.repository.as_mut().unwrap().url = credential_url();
    rejected(
        &c,
        Operation::SubmitTask {
            operation_id: "credential-input-0".into(),
            input: contaminated,
        },
    )
    .await;
    assert_eq!(get(&c, &accepted.id).await, accepted);
    drop(c);
    controller_owner_released(&p).await;
    no_secret_files(p.parent().unwrap());
}

#[tokio::test]
async fn local_and_password_free_ssh_repository_controls_remain_durable_inputs() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    for (n, value) in [
        "/tmp/repository.git",
        "./repository.git",
        "../repository.git",
        "repository.git",
        "git@example.invalid:org/repo.git",
        "ssh://git@example.invalid/org/repo.git",
        "git+ssh://git@example.invalid/org/repo.git",
        "ssh+git://git@example.invalid/org/repo.git",
        "https://example.invalid/repo.git",
        "file:///tmp/repo.git",
        "/tmp/https://user:local-directory@name/repo.git",
    ]
    .into_iter()
    .enumerate()
    {
        let mut input = coding(&format!("safe-url-{n}"));
        let mut registration = fixture_repository_registration();
        registration.repository_id = format!("safe-url-{n}");
        registration.source_endpoint = value.into();
        registration.checkpoint_endpoint = value.into();
        register_repository_fixture(&c, &registration).await;
        input.repository.as_mut().unwrap().registration = Some(registration.binding());
        let t = submit(&c, &format!("safe-url-{n}"), input.clone()).await;
        let mut expected = accepted_input(&c, &input);
        project_repository_fixture(&mut expected, &registration);
        assert_eq!(t.input, expected);
        assert_eq!(get(&c, &t.id).await.input, expected);
        assert_eq!(t.input.repository.as_ref().unwrap().url, value);
        assert_eq!(t.input.repository.as_ref().unwrap().checkpoint_remote, value);
    }
    drop(c);
    controller_owner_released(&p).await;
    no_secret_files(p.parent().unwrap());
}

#[tokio::test]
async fn credential_child_rejection_does_not_create_wait_reservation_or_child() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let parent = submit(&c, "credential-parent", coding("credential-parent-agent")).await;
    let w = worker("credential-worker", "inc", 0, 1);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let before = get(&c, &parent.id).await;
    for template in [false, true] {
        for destination in [false, true] {
            let mut child = if template {
                configured_native("credential-native-child")
            } else {
                coding("credential-child")
            };
            child.limits.deadline_unix_ms = parent.input.limits.deadline_unix_ms;
            if !template {
                child.limits.model_requests = 1;
            }
            let repository = repository_mut(&mut child, template);
            if destination {
                repository.checkpoint_remote = credential_url();
            } else {
                repository.url = credential_url();
            }
            let id = format!("credential-child-{template}-{destination}");
            rejected(
                &c,
                Operation::DelegateTask {
                    operation_id: id.clone(),
                    parent_task_id: parent.id.clone(),
                    parent_attempt_id: a.id.clone(),
                    generation: a.generation,
                    child,
                },
            )
            .await;
            absent(&c, &id).await;
            assert_eq!(get(&c, &parent.id).await, before);
        }
    }
    drop(c);
    controller_owner_released(&p).await;
    no_secret_files(p.parent().unwrap());
    {
        let db =
            rusqlite::Connection::open_with_flags(&p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        assert_eq!(
            db.query_row("SELECT count(*) FROM tasks", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            db.query_row("SELECT count(*) FROM waits", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            db.query_row(
                "SELECT json_extract(body,'$.delegated_model_requests') FROM tasks WHERE id=?1",
                [&parent.id],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
    }
    let c = Controller::open_with_config(&p, config()).unwrap();
    let mut child = coding("accepted-ssh-child");
    child.limits.model_requests = 1;
    child.limits.deadline_unix_ms = parent.input.limits.deadline_unix_ms;
    let mut registration = fixture_repository_registration();
    registration.repository_id = "accepted-ssh-child".into();
    registration.source_endpoint = "git@example.invalid:org/repository.git".into();
    register_repository_fixture(&c, &registration).await;
    child.repository.as_mut().unwrap().registration = Some(registration.binding());
    let accepted = task(
        call(
            &c,
            Operation::DelegateTask {
                operation_id: "credential-child-false-false".into(),
                parent_task_id: parent.id.clone(),
                parent_attempt_id: a.id,
                generation: a.generation,
                child,
            },
        )
        .await,
    );
    assert_eq!(accepted.parent_task_id.as_deref(), Some(parent.id.as_str()));
    assert_eq!(
        get(&c, &parent.id).await.waiting_child_id.as_deref(),
        Some(accepted.id.as_str())
    );
}

#[tokio::test]
async fn credential_checkpoint_intent_settlement_result_and_release_never_enter_receipts() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let t = submit(
        &c,
        "checkpoint-credential-task",
        coding("checkpoint-credential-agent"),
    )
    .await;
    let w = worker("checkpoint-credential-worker", "inc", 0, 1);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let before = get(&c, &t.id).await;
    let clean = Checkpoint {
        registration: fixture_repository_binding(),
        repository: "origin".into(),
        commit: "b".repeat(40),
        remote_ref: checkpoint_ref(&t.id, &a.id, "clean-intent"),
        operation_id: "clean-intent".into(),
    };
    let prepared = call(&c, Operation::PrepareOperation { operation_id: "clean-intent".into(), task_id: t.id.clone(), attempt_id: a.id.clone(), generation: a.generation, kind: "checkpoint".into(), intended: json!({"registration":clean.registration,"repository":clean.repository,"commit":clean.commit,"remote_ref":clean.remote_ref}) }).await;
    assert!(matches!(
        prepared,
        Outcome::Ok {
            data: Data::Operation(_)
        }
    ));
    for (field, n) in [("repository", 0), ("remote_ref", 1)] {
        let mut intended = json!({"registration":fixture_repository_binding(),"repository":"origin", "commit":"b".repeat(40), "remote_ref":checkpoint_ref(&t.id,&a.id,&format!("bad-intent-{n}"))});
        intended[field] = json!(credential_url());
        let id = format!("bad-intent-{n}");
        rejected(
            &c,
            Operation::PrepareOperation {
                operation_id: id.clone(),
                task_id: t.id.clone(),
                attempt_id: a.id.clone(),
                generation: a.generation,
                kind: "checkpoint".into(),
                intended,
            },
        )
        .await;
        absent(&c, &id).await;
        // The named checkpoint operand is malformed, but still contains a
        // recognizable credential URL. Reject before an ordinary type-error
        // receipt could persist the entire nested value.
        let id = format!("nested-checkpoint-operand-{n}");
        let mut intended = json!({"registration":fixture_repository_binding(),"repository":"origin", "commit":"b".repeat(40), "remote_ref":checkpoint_ref(&t.id,&a.id,&id)});
        intended[field] = json!({"nested":[credential_url()]});
        rejected(
            &c,
            Operation::PrepareOperation {
                operation_id: id.clone(),
                task_id: t.id.clone(),
                attempt_id: a.id.clone(),
                generation: a.generation,
                kind: "checkpoint".into(),
                intended,
            },
        )
        .await;
        absent(&c, &id).await;
        let mut contaminated = clean.clone();
        if n == 0 {
            contaminated.repository = credential_url();
        } else {
            contaminated.remote_ref = credential_url();
        }
        let id = format!("bad-settlement-{n}");
        rejected(
            &c,
            Operation::SettleOperation {
                settlement_id: id.clone(),
                operation_id: "clean-intent".into(),
                task_id: t.id.clone(),
                attempt_id: a.id.clone(),
                generation: a.generation,
                outcome: json!({"acknowledged":true}),
                checkpoint: Some(contaminated.clone()),
            },
        )
        .await;
        absent(&c, &id).await;
        let id = format!("bad-checkpoint-result-{n}");
        rejected(
            &c,
            Operation::ObserveWorker {
                report: Report {
                    report_id: id.clone(),
                    worker_id: a.worker_id.clone(),
                    incarnation: a.worker_incarnation.clone(),
                    task_id: a.task_id.clone(),
                    attempt_id: a.id.clone(),
                    generation: a.generation,
                    observation: Observation::ResultCandidate {
                        checks_passed: true,
                        summary: "controlled candidate".into(),
                        checkpoint: Some(contaminated),
                        validation: vec!["controlled check".into()],
                        output: None,
                    },
                },
            },
        )
        .await;
        absent(&c, &id).await;
    }
    assert_eq!(get(&c, &t.id).await, before);
    assert_eq!(
        call(
            &c,
            Operation::GetOperation {
                operation_id: "clean-intent".into()
            }
        )
        .await,
        prepared
    );
    assert!(matches!(
        call(
            &c,
            Operation::SettleOperation {
                settlement_id: "clean-settlement".into(),
                operation_id: "clean-intent".into(),
                task_id: t.id.clone(),
                attempt_id: a.id.clone(),
                generation: a.generation,
                outcome: json!({"acknowledged":true}),
                checkpoint: Some(clean)
            }
        )
        .await,
        Outcome::Ok { .. }
    ));
    let accepted = finish_controlled_coding(&c, &a, "credential-source-completion").await;
    assert_eq!(accepted.status, TaskStatus::Succeeded);
    for destination in [false, true] {
        let id = format!("bad-release-{destination}");
        let mut input = release_input(&accepted, &id);
        if destination {
            input.checkpoint.remote_ref = credential_url();
        } else {
            input.checkpoint.repository = credential_url();
        }
        rejected(&c, Operation::PrepareRelease(input)).await;
        absent(&c, &id).await;
    }
    assert_eq!(get(&c, &t.id).await, accepted);
    let clean_release = release(
        call(
            &c,
            Operation::PrepareRelease(release_input(&accepted, "bad-release-false")),
        )
        .await,
    );
    assert_unqualified(&clean_release);
    drop(c);
    controller_owner_released(&p).await;
    no_secret_files(p.parent().unwrap());
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(get(&c, &t.id).await, accepted);
    assert_eq!(
        release(
            call(
                &c,
                Operation::GetRelease {
                    release_id: clean_release.id.clone()
                }
            )
            .await
        ),
        clean_release
    );
}
