//! Real controller transactions with controlled worker observations; no provider.
use super::*;

async fn history(c: &Controller, task_id: &str) -> Vec<borg_runtime::collaboration::HistoryEntry> {
    let result = Box::pin(super::super::call(
        c,
        Operation::QueryCollaboration {
            query: borg_runtime::collaboration::Query::TaskHistory {
                task_id: task_id.into(),
                after: None,
                limit: c.limits().controller.collaboration_page_items,
            },
        },
    ))
    .await;
    match result {
        Outcome::Ok {
            data: Data::Collaboration(borg_runtime::collaboration::QueryResult::TaskHistory(page)),
        } => {
            assert!(page.next_cursor.is_none(), "fixture history must fit one page");
            page.items
        }
        other => panic!("expected canonical history: {other:?}"),
    }
}

async fn progress_call(c: &Controller, call: &Invocation) -> Reply {
    let reply = Box::pin(c.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: "compact-progress".into(),
        command: call.operation(),
    }))
    .await;
    reply.validate_for(&call.operation()).unwrap();
    reply
}

#[tokio::test]
// Traceability scope: A large accepted task commits one progress observation through
// the canonical operation and replays its compact exact-revision receipt on reopen.
//= spec/impl/v2/implementation.md#shared-collaboration-tool-interface
//= type=test
//# The collaboration tool interface MUST use the controller's canonical operation layer.
async fn large_task_progress_commits_compact_revision_and_replays_after_restart() {
    Box::pin(async {
        let p = path();
        let mut cfg = config();
        Arc::make_mut(&mut cfg.limits).interface.mcp_data_bytes = 4096;
        let bound = cfg.limits.interface.mcp_data_bytes;
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        let mut input = profiled("compact-progress");
        input.instruction = "x".repeat(bound * 2);
        let submitted = Box::pin(submit(&c, "compact-progress-submit", input)).await;
        let w = worker("compact-progress-worker", "inc", 1, 0);
        Box::pin(register(&c, &w)).await;
        let attempt = run(&Box::pin(commands(&c, &w, vec![])).await);
        task(Box::pin(report(&c, &attempt, "compact-progress-start", Observation::Started {
            thread_id: None, turn_id: None,
        })).await);
        // The ordinary worker API still returns its full Task and retains output.
        let before = task(Box::pin(report(&c, &attempt, "compact-progress-output", Observation::Progress {
            text: "before MCP".into(), output: "retained worker output".into(), truncated: true,
        })).await);
        assert!(before.input.instruction.len() > bound);
        assert!(serde_json::to_vec(&Data::Task(Box::new(before.clone()))).unwrap().len() > bound);
        let prior_history = Box::pin(history(&c, &submitted.id)).await;
        let actor = Caller {
            task_id: submitted.id.clone(), attempt_id: attempt.id.clone(),
            generation: attempt.generation, worker_id: attempt.worker_id.clone(),
            incarnation: attempt.worker_incarnation.clone(),
        };
        let call = invocation(&actor, "compact-progress-once", "collab_task_progress",
            json!({"text":"reported progress, not an accepted result"}));
        let first = progress_call(&c, &call).await;
        let mut expected = before.clone();
        expected.progress = "reported progress, not an accepted result".into();
        expected.revision += 1;
        assert_eq!(first.outcome, Outcome::Ok {
            data: Data::CollaborationPage(mcp::ListPage {
                items: vec![json!({"task_id":expected.id,"session_id":expected.session_id,"revision":expected.revision})],
                next_cursor: None,
            }),
        });
        assert_eq!(first.commit.as_ref().unwrap().operation_id, call.operation_id);
        let Outcome::Ok { data } = &first.outcome else { unreachable!() };
        assert!(serde_json::to_vec(data).unwrap().len() <= bound);
        let model_result = mcp::result(&call, first.clone()).unwrap();
        assert!(!model_result.requires_quiescence);
        assert_eq!(Box::pin(get(&c, &submitted.id)).await, expected);
        let committed_history = Box::pin(history(&c, &submitted.id)).await;
        assert_eq!(committed_history.len(), prior_history.len() + 1);
        assert_eq!(&committed_history[..prior_history.len()], prior_history.as_slice());
        let entry = committed_history.last().unwrap();
        assert_eq!(entry.task_revision, expected.revision);
        assert_eq!(entry.kind, "worker_observation");
        assert_eq!(entry.body, serde_json::to_value(Observation::Progress {
            text: expected.progress.clone(), output: before.recent_output.clone(),
            truncated: before.output_truncated,
        }).unwrap());

        drop(c);
        controller_owner_released(&p).await;
        let c = Controller::open_with_config(&p, cfg).unwrap();
        let replay = progress_call(&c, &call).await;
        assert_eq!(replay.outcome, first.outcome);
        assert_eq!(replay.commit, first.commit);
        let recovered = mcp::recovered(&call, Box::pin(lookup(&c, &call)).await).unwrap();
        assert_eq!(recovered.reply.outcome, first.outcome);
        assert_eq!(recovered.reply.commit, first.commit);
        assert!(!recovered.requires_quiescence);
        assert_eq!(Box::pin(get(&c, &submitted.id)).await, expected);
        assert_eq!(Box::pin(history(&c, &submitted.id)).await, committed_history);

        let mut conflicting = call.clone();
        conflicting.arguments["text"] = json!("different original input");
        assert_eq!(code(progress_call(&c, &conflicting).await.outcome), "operation_conflict");
        let mut stale = invocation(&actor, "compact-progress-stale", "collab_task_progress",
            json!({"text":"must not commit"}));
        stale.caller.generation += 1;
        assert_eq!(code(progress_call(&c, &stale).await.outcome), "stale_generation");
        assert_eq!(Box::pin(get(&c, &submitted.id)).await, expected);
        assert_eq!(Box::pin(history(&c, &submitted.id)).await, committed_history);
    }).await;
}

#[tokio::test]
// Traceability scope: Compact progress still obeys the accepted response bound;
// a refusal leaves task progress/revision/history unchanged and names the limit.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# An operation refused by a configured limit MUST identify the limiting metadata field.
async fn compact_progress_still_refuses_response_bound_without_mutation() {
    Box::pin(async {
        let mut cfg = config();
        Arc::make_mut(&mut cfg.limits).interface.mcp_data_bytes = 1;
        let c = Controller::open_with_config(path(), cfg).unwrap();
        let (submitted, _, actor) = Box::pin(fixture(&c, "compact-progress-bound")).await;
        let before = Box::pin(get(&c, &submitted.id)).await;
        let prior_history = Box::pin(history(&c, &submitted.id)).await;
        let call = invocation(&actor, "compact-progress-too-small", "collab_task_progress",
            json!({"text":"must roll back"}));
        let first = progress_call(&c, &call).await;
        assert_eq!(code(first.outcome.clone()), "response_bound");
        assert!(matches!(&first.outcome, Outcome::Refused { message, .. }
            if message.contains("interface.mcp_data_bytes")));
        assert_eq!(first.commit.as_ref().unwrap().operation_id, call.operation_id);
        let replay = progress_call(&c, &call).await;
        assert_eq!(replay.outcome, first.outcome);
        assert_eq!(replay.commit, first.commit);
        assert_eq!(Box::pin(get(&c, &submitted.id)).await, before);
        assert_eq!(Box::pin(history(&c, &submitted.id)).await, prior_history);
    }).await;
}
