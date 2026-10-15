use borg_runtime::collaboration::{Query, QueryResult};
use borg_runtime::repository_view::{Request as RepoRequest, SourceSelector};
async fn observe(c: &Controller, task_id: &str, request: RepoRequest) -> Reply {
    let op = Operation::QueryCollaboration {
        query: Query::Repository {
            task_id: task_id.into(),
            request,
        },
    };
    let r = c
        .handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: "operator-repository".into(),
            command: op.clone(),
        })
        .await;
    r.validate_for(&op).unwrap();
    assert!(r.commit.is_none());
    r
}
fn read(source: SourceSelector) -> RepoRequest {
    RepoRequest::ReadFile {
        source,
        path: "answer.txt".into(),
        start_line: 1,
        max_lines: 1,
    }
}
fn observed(r: &Reply) -> &Output {
    match &r.outcome {
        Outcome::Ok {
            data: Data::Collaboration(QueryResult::Repository(v)),
        } => v,
        x => panic!("{x:?}"),
    }
}
#[tokio::test]
async fn ui_reads_operator_repository_after_cancel_expiry_has_no_caller_or_receipt() {
    let f = Repo::new();
    let clock = Arc::new(AtomicU64::new(1000));
    let mut cfg = f.config();
    let clock2 = clock.clone();
    cfg.clock = Arc::new(move || clock2.load(Ordering::SeqCst));
    let c = Controller::open_with_config(path(), cfg).unwrap();
    let mut input = f.input("operator-source");
    input.limits.deadline_unix_ms = 2000;
    register_repository_fixture(&c, &read_repository_registration()).await;
    let t = submit(&c, "operator-submit", input).await;
    // Never registered a worker or admitted an attempt. Cancellation is genuine.
    let stopped = task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "operator-cancel".into(),
                task_id: t.id.clone(),
                expected_revision: t.revision,
            },
        )
        .await,
    );
    assert_eq!(stopped.status, TaskStatus::Cancelled);
    assert!(stopped.attempt.is_none());
    clock.store(3000, Ordering::SeqCst);
    let r = observe(&c, &t.id, read(SourceSelector::Start)).await;
    assert_eq!(observed(&r).source.commit, f.old);
    assert!(observed(&r).untrusted);
    assert!(matches!(&observed(&r).data,RepoData::File{text,..} if text=="before\n"));
    let after = get(&c, &t.id).await;
    assert_eq!(after, stopped);
    let second = observe(&c, &t.id, read(SourceSelector::Start)).await;
    assert_eq!(
        r.observed_controller_revision,
        second.observed_controller_revision
    );
    assert_eq!(
        code(
            observe(&c, &t.id, read(SourceSelector::Checkpoint))
                .await
                .outcome
        ),
        "repository_read_unavailable"
    );
    assert_eq!(
        code(
            observe(
                &c,
                &t.id,
                RepoRequest::ReadFile {
                    source: SourceSelector::Start,
                    path: "../escape".into(),
                    start_line: 1,
                    max_lines: 1
                }
            )
            .await
            .outcome
        ),
        "repository_read_invalid"
    );
}
#[tokio::test]
async fn ui_reads_repository_source_recheck_and_detached_cleanup_without_receipt() {
    let f = Repo::new();
    let c = Controller::open_with_config(path(), f.config()).unwrap();
    let mut input = coding("ui-checkpoint");
    input.repository = Some(Repository {
        registration: Some(read_repository_registration().binding()),
        url: String::new(),
        start_commit: f.old.clone(),
        checkpoint_remote: String::new(),
    });
    let (t, a, _) = admitted(&c, "ui-checkpoint", input).await;
    checkpoint(&c, &a, "ui-cp1", &f.old).await;
    let (ready, resume) = c.qualification_pause_next_finalization();
    let cc = c.clone();
    let id = t.id.clone();
    let job =
        tokio::spawn(async move { observe(&cc, &id, read(SourceSelector::Checkpoint)).await });
    ready.await.unwrap();
    checkpoint(&c, &a, "ui-cp2", &f.new).await;
    resume.send(()).unwrap();
    assert_eq!(
        code(job.await.unwrap().outcome),
        "repository_source_changed"
    );
    let r = observe(&c, &t.id, read(SourceSelector::Checkpoint)).await;
    assert_eq!(observed(&r).source.commit, f.new);
    let (ready, resume) =
        borg_runtime::repository_read::qualification_pause_next_blob(f.bare.clone());
    let cc = c.clone();
    let id = t.id.clone();
    let job = tokio::spawn(async move {
        observe(
            &cc,
            &id,
            RepoRequest::ReadFile {
                source: SourceSelector::Start,
                path: "waiting.txt".into(),
                start_line: 1,
                max_lines: 1,
            },
        )
        .await
    });
    let pid = tokio::time::timeout(Duration::from_secs(2), ready)
        .await
        .unwrap()
        .unwrap();
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while std::path::Path::new(&format!("/proc/{pid}")).exists() {
            tokio::time::sleep(Duration::from_millis(5)).await
        }
    })
    .await
    .unwrap();
    assert!(get(&c, &t.id).await.selected_checkpoint.is_some());
}
#[tokio::test]
async fn ui_reads_call_receipt_exact_task_historical_association_and_no_control_receipts() {
    let f = Repo::new();
    let p = path();
    let c = Controller::open_with_config(&p, f.config()).unwrap();
    let (t, _, actor) = admitted(&c, "call-observe", f.input("call-observe")).await;
    let v = read_call(&actor, "ui-original-call", "start");
    let original = invoke(&c, &v).await;
    output(&original);
    let current = get(&c, &t.id).await;
    task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "cancel-ui-call".into(),
                task_id: t.id.clone(),
                expected_revision: current.revision,
            },
        )
        .await,
    );
    let other = submit(&c, "ui-other-task", f.input("ui-other")).await;
    let query = |task_id: String, operation_id: &str| Operation::QueryCollaboration {
        query: Query::Call {
            task_id,
            operation_id: operation_id.into(),
        },
    };
    let record = match call(&c, query(t.id.clone(), &v.operation_id)).await {
        Outcome::Ok {
            data: Data::Collaboration(QueryResult::Call(r)),
        } => r,
        x => panic!("{x:?}"),
    };
    assert_eq!(record.invocation, v);
    assert_eq!(record.result.reply.outcome, original.outcome);
    assert_eq!(record.result.reply.commit, original.commit);
    assert!(!record.result.requires_quiescence);
    assert_eq!(
        code(call(&c, query(other.id.clone(), &v.operation_id)).await),
        "not_found"
    );
    assert_eq!(
        code(call(&c, query(t.id.clone(), "cancel-ui-call")).await),
        "not_found"
    );
    let before = get(&c, &t.id).await;
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, f.config()).unwrap();
    let replay = match call(&c, query(t.id.clone(), &v.operation_id)).await {
        Outcome::Ok {
            data: Data::Collaboration(QueryResult::Call(r)),
        } => r,
        x => panic!("{x:?}"),
    };
    assert_eq!(replay, record);
    assert_eq!(get(&c, &t.id).await, before);
}
#[tokio::test]
async fn ui_reads_activity_uses_existing_history_sanitizer() {
    let f = Repo::new();
    let c = Controller::open_with_config(path(), f.config()).unwrap();
    let (_, a, _) = admitted(&c, "history-view", f.input("history-view")).await;
    // Opaque execution identifiers and external intent body must not be exposed.
    task(
        report(
            &c,
            &a,
            "history-start-sensitive",
            Observation::Started {
                thread_id: Some("private-thread-marker".into()),
                turn_id: Some("private-turn-marker".into()),
            },
        )
        .await,
    );
    assert!(matches!(
        call(
            &c,
            Operation::PrepareOperation {
                operation_id: "opaque-effect".into(),
                task_id: a.task_id.clone(),
                attempt_id: a.id.clone(),
                generation: a.generation,
                kind: "pull_request".into(),
                intended: json!({"opaque":"opaque-intent-marker"})
            }
        )
        .await,
        Outcome::Ok { .. }
    ));
    let page = call(
        &c,
        Operation::QueryCollaboration {
            query: Query::Activity {
                after: Some(0),
                limit: 100,
            },
        },
    )
    .await;
    let encoded = serde_json::to_string(&page).unwrap();
    assert!(!encoded.contains("private-thread-marker"));
    assert!(!encoded.contains("private-turn-marker"));
    assert!(!encoded.contains("opaque-intent-marker"));
    assert!(encoded.contains("external_intent"));
    assert!(encoded.contains("opaque-effect"));
}

#[tokio::test]
async fn ui_reads_activity_byte_bound_resumes_without_skipping_large_entries() {
    let f = Repo::new();
    let c = Controller::open_with_config(path(), f.config()).unwrap();
    let (_, a, _) = admitted(&c, "large-history", f.input("large-history")).await;
    for i in 0..70 {
        task(
            report(
                &c,
                &a,
                &format!("large-history-{i}"),
                Observation::Progress {
                    text: format!("entry-{i}"),
                    output: "x".repeat(16384),
                    truncated: false,
                },
            )
            .await,
        );
    }
    let query = |after| Operation::QueryCollaboration {
        query: Query::Activity {
            after: Some(after),
            limit: 100,
        },
    };
    let page = |outcome| match outcome {
        Outcome::Ok {
            data: Data::Collaboration(QueryResult::Activity(v)),
        } => v,
        x => panic!("{x:?}"),
    };
    let first = page(call(&c, query(0)).await);
    assert!(first.has_more);
    assert!(first.items.len() < 70);
    assert!(serde_json::to_vec(&first).unwrap().len() < metadata().transport.body_bytes);
    let second = page(call(&c, query(first.cursor)).await);
    assert!(!second.has_more);
    assert!(second.items[0].sequence > first.cursor);
    let entries = first
        .items
        .into_iter()
        .chain(second.items)
        .filter(|e| {
            e.kind == "worker_observation"
                && e.body.get("kind").and_then(serde_json::Value::as_str) == Some("progress")
        })
        .collect::<Vec<_>>();
    assert_eq!(entries.len(), 70);
    assert!(entries.windows(2).all(|p| p[0].sequence < p[1].sequence));
}
