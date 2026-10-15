//! Actual controller read/watch boundaries. No browser or remote qualification.
use super::*;
use borg_runtime::collaboration::{ActivityPage, Query, QueryResult};
async fn query(c: &Controller, query: Query) -> Reply {
    let op = Operation::QueryCollaboration { query };
    let r = c
        .handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: "ui-observation".into(),
            command: op.clone(),
        })
        .await;
    r.validate_for(&op).unwrap();
    assert!(r.commit.is_none(), "operator reads do not create receipts");
    r
}
fn activity(r: Reply) -> ActivityPage {
    match r.outcome {
        Outcome::Ok {
            data: Data::Collaboration(QueryResult::Activity(v)),
        } => v,
        x => panic!("{x:?}"),
    }
}
async fn revision(c: &Controller, id: &str) -> u64 {
    c.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: "revision".into(),
        command: Operation::GetTask { task_id: id.into() },
    })
    .await
    .observed_controller_revision
    .unwrap()
}
#[tokio::test]
async fn ui_reads_activity_tail_catchup_empty_and_restart() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let t = submit(&c, "activity-a", native("activity-a")).await;
    submit(&c, "activity-b", native("activity-b")).await;
    submit(&c, "activity-c", native("activity-c")).await;
    let rev = revision(&c, &t.id).await;
    let all = activity(
        query(
            &c,
            Query::Activity {
                after: Some(0),
                limit: 100,
            },
        )
        .await,
    );
    assert!(!all.has_more);
    assert!(all.items.len() >= 3);
    assert!(all.items.windows(2).all(|x| x[0].sequence < x[1].sequence));
    let tail = activity(
        query(
            &c,
            Query::Activity {
                after: None,
                limit: 2,
            },
        )
        .await,
    );
    assert_eq!(tail.items, all.items[all.items.len() - 2..]);
    assert_eq!(tail.cursor, all.cursor);
    assert!(!tail.has_more);
    let mut cursor = 0;
    let mut accumulated = vec![];
    loop {
        let page = activity(
            query(
                &c,
                Query::Activity {
                    after: Some(cursor),
                    limit: 1,
                },
            )
            .await,
        );
        assert!(page.items.len() <= 1);
        cursor = page.cursor;
        accumulated.extend(page.items);
        if !page.has_more {
            break;
        }
    }
    assert_eq!(accumulated, all.items);
    let empty = activity(
        query(
            &c,
            Query::Activity {
                after: Some(cursor),
                limit: 1,
            },
        )
        .await,
    );
    assert!(empty.items.is_empty());
    assert_eq!(empty.cursor, cursor);
    assert!(!empty.has_more);
    assert_eq!(revision(&c, &t.id).await, rev);
    assert_eq!(
        code(
            query(
                &c,
                Query::Activity {
                    after: Some(u64::MAX),
                    limit: 1
                }
            )
            .await
            .outcome
        ),
        "invalid_input"
    );
    assert_eq!(
        code(
            query(
                &c,
                Query::Activity {
                    after: None,
                    limit: 101
                }
            )
            .await
            .outcome
        ),
        "invalid_input"
    );
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(
        activity(
            query(
                &c,
                Query::Activity {
                    after: Some(0),
                    limit: 100
                }
            )
            .await
        ),
        all
    );
}
#[tokio::test]
async fn ui_reads_task_page_uses_scoped_bounded_summary_cursor() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let a = submit(&c, "tasks-a", native("tasks-shared")).await;
    let b = submit(&c, "tasks-b", native("tasks-shared")).await;
    submit(&c, "tasks-c", native("tasks-other")).await;
    let q = |after| Query::Tasks {
        agent_id: Some("tasks-shared".into()),
        parent_task_id: None,
        after,
        limit: 1,
    };
    let result = |r: Reply| match r.outcome {
        Outcome::Ok {
            data: Data::Collaboration(QueryResult::Tasks(v)),
        } => v,
        x => panic!("{x:?}"),
    };
    let one = result(query(&c, q(None)).await);
    assert_eq!(one.items.len(), 1);
    assert_eq!(one.items[0]["id"], a.id);
    assert!(one.items[0].get("instruction").is_none());
    let after = one.next_cursor.unwrap();
    let two = result(query(&c, q(Some(after.clone()))).await);
    assert_eq!(two.items[0]["id"], b.id);
    assert_eq!(
        code(
            query(
                &c,
                Query::Tasks {
                    agent_id: Some("tasks-other".into()),
                    parent_task_id: None,
                    after: Some(after),
                    limit: 1
                }
            )
            .await
            .outcome
        ),
        "invalid_input"
    );
}
#[tokio::test]
async fn ui_reads_watch_durable_revision_wakes_and_times_out_without_mutation() {
    let p = path();
    let mut cfg = config();
    Arc::make_mut(&mut cfg.limits).controller.command_wait_ms = (Duration::from_millis(150)).as_millis().try_into().unwrap();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let t = submit(&c, "watch-a", native("watch-a")).await;
    let rev = revision(&c, &t.id).await;
    // Either order of actual task commit versus waiter admission must observe it.
    for i in 0..4 {
        let before = revision(&c, &t.id).await;
        let cc = c.clone();
        let waiting = tokio::spawn(async move { cc.wait_for_change(before).await });
        submit(
            &c,
            &format!("watch-next-{i}"),
            native(&format!("watch-next-{i}")),
        )
        .await;
        let observed = tokio::time::timeout(Duration::from_secs(2), waiting)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(observed.changed);
        assert!(!observed.reset);
        assert!(observed.revision > before);
    }
    let now = revision(&c, &t.id).await;
    assert!(now > rev);
    let quiet = c.wait_for_change(now).await.unwrap();
    assert_eq!(quiet.revision, now);
    assert!(!quiet.changed);
    assert!(!quiet.reset);
    assert_eq!(revision(&c, &t.id).await, now);
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    assert_eq!(c.wait_for_change(now).await.unwrap(), quiet);
    let new = Controller::open_with_config(path(), cfg).unwrap();
    let reset = new.wait_for_change(now).await.unwrap();
    assert_eq!(reset.revision, 0);
    assert!(reset.changed && reset.reset);
}
#[tokio::test]
async fn ui_reads_watch_capacity_and_cancel_release_are_separate_and_bounded() {
    let mut cfg = config();
    Arc::make_mut(&mut cfg.limits).controller.command_wait_ms = (Duration::from_millis(250)).as_millis().try_into().unwrap();
    Arc::make_mut(&mut cfg.limits).controller.max_workers = 1;
    Arc::make_mut(&mut cfg.limits).controller.ui_waiters = 1;
    let c = Controller::open_with_config(path(), cfg).unwrap();
    let t = submit(&c, "capacity-watch", native("capacity-watch")).await;
    let rev = revision(&c, &t.id).await;
    let cc = c.clone();
    let waiting = tokio::spawn(async move { cc.wait_for_change(rev).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        c.wait_for_change(rev).await.unwrap_err(),
        "UI wait capacity exhausted: controller.ui_waiters"
    );
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    let quiet = c.wait_for_change(rev).await.unwrap();
    assert!(!quiet.changed);
    let w = worker("ordinary-wait", "inc", 0, 1);
    register(&c, &w).await;
    // No coding tasks exist; the worker long-poll remains separately available.
    let cc = c.clone();
    let rr = revision(&c, &t.id).await;
    let ui = tokio::spawn(async move { cc.wait_for_change(rr).await });
    assert!(commands(&c, &w, vec![]).await.is_empty());
    ui.await.unwrap().unwrap();
}
