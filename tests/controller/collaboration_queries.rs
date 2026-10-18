//! Controlled controller records exercise readers, not MCP authentication or real providers.
use super::*;
use borg_runtime::collaboration::{Query, QueryResult};
async fn query(c: &Controller, query: Query) -> QueryResult {
    let op = Operation::QueryCollaboration { query };
    let reply = c
        .handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: "query".into(),
            command: op.clone(),
        })
        .await;
    reply.validate_for(&op).unwrap();
    assert!(reply.commit.is_none());
    assert!(
        reply.observed_controller_revision.is_some(),
        "query {op:?}: {reply:?}"
    );
    match reply.outcome {
        Outcome::Ok {
            data: Data::Collaboration(v),
        } => v,
        x => panic!("query result expected: {x:?}"),
    }
}
#[tokio::test]
// Traceability scope: Actual canonical worker capacity and attempt/history projections preserve the task and survive reopen; checkpoint/release projections are exercised by the companion case below.
//= spec/impl/v2/implementation.md#shared-collaboration-tool-interface
//= type=test
//# The collaboration tool interface MUST provide read-only views of worker capacity, task attempts, checkpoints and release-check records.
async fn collaboration_queries_read_existing_records_without_mutating_revision() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let t = submit(&c, "query-submit", native("query-agent")).await;
    let w = worker("query-worker", "query-inc", 2, 3);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    task(
        report(
            &c,
            &a,
            "query-start",
            Observation::Started {
                thread_id: Some("opaque-thread-do-not-expose".into()),
                turn_id: Some("opaque-turn".into()),
            },
        )
        .await,
    );
    controlled_native_request(&c, &a, 1, None, None).await;
    for key in ["alpha", "omega"] {
        assert!(matches!(
            call(
                &c,
                Operation::SetMemory {
                    operation_id: format!("query-memory-{key}"),
                    task_id: t.id.clone(),
                    attempt_id: a.id.clone(),
                    generation: a.generation,
                    key: key.into(),
                    expected_revision: None,
                    value: json!({"not_in_key_listing":"synthetic private value"})
                }
            )
            .await,
            Outcome::Ok { .. }
        ));
    }
    let before = get(&c, &t.id).await;
    let QueryResult::Worker(v) = query(
        &c,
        Query::Worker {
            worker_id: w.id.clone(),
        },
    )
    .await
    else {
        panic!()
    };
    assert_eq!(v.worker, w);
    assert_eq!((v.native_occupied, v.coding_occupied), (1, 0));
    let QueryResult::Workers(v) = query(
        &c,
        Query::Workers {
            after: None,
            limit: 1,
        },
    )
    .await
    else {
        panic!()
    };
    assert_eq!(v.items.len(), 1);
    assert!(v.next_cursor.is_none());
    assert_eq!(
        query(
            &c,
            Query::Definition {
                definition_id: t.input.definition.id.clone(),
                revision: t.input.definition.revision.clone()
            }
        )
        .await,
        QueryResult::Definition(t.input.definition.clone())
    );
    let QueryResult::MemoryKeys(first) = query(
        &c,
        Query::MemoryKeys {
            agent_id: "query-agent".into(),
            after: None,
            limit: 1,
        },
    )
    .await
    else {
        panic!()
    };
    assert_eq!(first.items[0].key, "alpha");
    assert!(first.next_cursor.is_some());
    assert!(
        !serde_json::to_string(&first)
            .unwrap()
            .contains("synthetic private value")
    );
    let QueryResult::MemoryKeys(second) = query(
        &c,
        Query::MemoryKeys {
            agent_id: "query-agent".into(),
            after: first.next_cursor,
            limit: 1,
        },
    )
    .await
    else {
        panic!()
    };
    assert_eq!(second.items[0].key, "omega");
    assert!(second.next_cursor.is_none());
    let QueryResult::TaskAttempts(attempts) = query(
        &c,
        Query::TaskAttempts {
            task_id: t.id.clone(),
            after: None,
            limit: 10,
        },
    )
    .await
    else {
        panic!()
    };
    assert_eq!(attempts.items.len(), 1);
    let projected = &attempts.items[0];
    assert_eq!(projected.attempt.id, a.id);
    assert_eq!(projected.reported_model_requests, 1);
    assert_eq!(projected.input_tokens, None);
    assert_eq!(projected.output_tokens, None);
    assert!(projected.executor_outcome.is_none());
    assert!(
        !serde_json::to_string(&attempts)
            .unwrap()
            .contains("opaque-thread")
    );
    let mut cursor = None;
    let mut history = vec![];
    for _ in 0..32 {
        let QueryResult::TaskHistory(page) = query(
            &c,
            Query::TaskHistory {
                task_id: t.id.clone(),
                after: cursor,
                limit: 1,
            },
        )
        .await
        else {
            panic!()
        };
        history.extend(page.items);
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert!(cursor.is_none());
    assert!(history.len() >= 5);
    assert!(history.windows(2).all(|v| v[0].sequence < v[1].sequence));
    assert!(
        history
            .iter()
            .any(|v| v.body.get("executor_association_omitted") == Some(&json!(true)))
    );
    assert!(
        !serde_json::to_string(&history)
            .unwrap()
            .contains("opaque-thread")
    );
    assert_eq!(get(&c, &t.id).await, before);
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(get(&c, &t.id).await, before);
    assert_eq!(
        query(
            &c,
            Query::TaskAttempts {
                task_id: t.id.clone(),
                after: None,
                limit: 10
            }
        )
        .await,
        QueryResult::TaskAttempts(attempts)
    );
}
#[tokio::test]
// Traceability scope: Actual checkpoint and empty release-check projections preserve the unqualified candidate and original task; reads do not manufacture release authority.
//= spec/impl/v2/implementation.md#shared-collaboration-tool-interface
//= type=test
//# The collaboration tool interface MUST provide read-only views of worker capacity, task attempts, checkpoints and release-check records.
//= spec/impl/v2/implementation.md#shared-collaboration-tool-interface
//= type=test
//# Operational queries MUST NOT grant authority to mutate worker controls or protected release state.
async fn collaboration_queries_checkpoint_and_release_reads_do_not_create_authority() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let t = accepted_release_task(&c, "query-coding").await;
    let candidate = release(
        call(
            &c,
            Operation::PrepareRelease(release_input(&t, "query-release")),
        )
        .await,
    );
    let QueryResult::TaskCheckpoints(p) = query(
        &c,
        Query::TaskCheckpoints {
            task_id: t.id.clone(),
            after: None,
            limit: 10,
        },
    )
    .await
    else {
        panic!()
    };
    assert_eq!(p.items.len(), 1);
    let cp = &p.items[0];
    assert!(cp.selected);
    assert_eq!(cp.state, "settled");
    assert_eq!(Some(&cp.checkpoint), t.selected_checkpoint.as_ref());
    assert_eq!(cp.outcome.as_ref().unwrap().acknowledged, Some(true));
    let QueryResult::ReleaseChecks(checks) = query(
        &c,
        Query::ReleaseChecks {
            release_id: candidate.id.clone(),
            after: None,
            limit: 10,
        },
    )
    .await
    else {
        panic!()
    };
    assert!(checks.items.is_empty());
    assert!(checks.next_cursor.is_none());
    assert_unqualified(&candidate);
    assert_eq!(
        release(
            call(
                &c,
                Operation::GetRelease {
                    release_id: candidate.id.clone()
                }
            )
            .await
        ),
        candidate
    );
    let QueryResult::TaskAttempts(a) = query(
        &c,
        Query::TaskAttempts {
            task_id: t.id.clone(),
            after: None,
            limit: 10,
        },
    )
    .await
    else {
        panic!()
    };
    assert!(a.items[0].attempt.stopped);
    assert_eq!(a.items[0].executor_outcome.as_ref().unwrap().success, true);
    assert_eq!(get(&c, &t.id).await, t);
}
#[tokio::test]
async fn collaboration_queries_refuse_bad_bounds_and_cross_query_cursors() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let t = submit(&c, "query-bound-submit", native("q-agent")).await;
    let QueryResult::TaskHistory(p) = query(
        &c,
        Query::TaskHistory {
            task_id: t.id.clone(),
            after: None,
            limit: 1,
        },
    )
    .await
    else {
        panic!()
    };
    assert!(p.next_cursor.is_none());
    for q in [
        Query::TaskHistory {
            task_id: t.id.clone(),
            after: Some(r#"{"kind":"task_attempts","scope":"wrong","position":"1"}"#.into()),
            limit: 1,
        },
        Query::Workers {
            after: None,
            limit: 0,
        },
        Query::Workers {
            after: None,
            limit: 101,
        },
    ] {
        assert_eq!(
            code(call(&c, Operation::QueryCollaboration { query: q }).await),
            "invalid_input"
        );
    }
    assert_eq!(
        code(
            call(
                &c,
                Operation::QueryCollaboration {
                    query: Query::Definition {
                        definition_id: "missing".into(),
                        revision: "r".into()
                    }
                }
            )
            .await
        ),
        "not_found"
    );
}
