//! Active original asker priority uses the same real controller transactions as
//! the existing current-owner fallback suite. Controlled Begin/ACK proves the
//! controller obligation; actual consumer append is covered separately.
use super::*;

#[tokio::test]
async fn active_asker_keeps_one_resolution_across_owner_changes_restart_and_lost_ack() {
    let lab = Lab::new();
    let (_, asker) = actor(&lab.c, "joined-active-asker").await;
    let (_, first_owner) = actor(&lab.c, "joined-first-owner").await;
    let (_, next_owner) = actor(&lab.c, "joined-next-owner").await;
    let doc = document(&lab.c, "joined-first-owner").await;
    let question = question(&lab.c, &doc, Some(&asker)).await;
    let doc = owner(&lab.c, &doc, "joined-next-owner").await;
    let (answer_op, answer_receipt, event) = answer(&lab.c, &question).await;
    assert!(matches!(&event.routing.as_ref().unwrap().requested_by.provenance,
        ActorProvenance::BoundAgentSession { task_id, .. } if task_id == &asker.task_id));
    let unchanged = inventory(&lab.c).await;
    scan_without(&lab.c, &first_owner, &event.id).await;
    scan_without(&lab.c, &next_owner, &event.id).await;
    let (prepare_op, prepare_receipt, batch) = seek(&lab.c, &asker, &event).await;
    let intent = begin(&asker, &batch, "joined-original-asker-input");
    let begin_receipt = call(&lab.c, intent.clone()).await;
    let begun = delivery(&begin_receipt).pending.expect("original requester claim");
    assert!(begun.delivery.is_some());
    owner(&lab.c, &doc, "joined-first-owner").await;
    let lab = lab.restart().await;
    assert_eq!(observe(&lab.c, &asker).await.pending, Some(begun));
    exact_receipt(&lab.c, &prepare_op, &prepare_receipt).await;
    exact_receipt(&lab.c, &intent, &begin_receipt).await;
    scan_without(&lab.c, &first_owner, &event.id).await;
    scan_without(&lab.c, &next_owner, &event.id).await;
    assert!(matches!(call(&lab.c, ack(&first_owner, &batch, "joined-original-asker-input")).await.outcome,
        Outcome::Refused { .. }));
    let acknowledge = ack(&asker, &batch, "joined-original-asker-input");
    let ack_receipt = call(&lab.c, acknowledge.clone()).await;
    assert!(delivery(&ack_receipt).pending.is_none());
    // Lose only the reply after the original ACK commit, then reopen and replay
    // the original operation. No second consumer append or fresh ACK is created.
    let lab = lab.restart().await;
    for (op, receipt) in [(&answer_op, &answer_receipt), (&prepare_op, &prepare_receipt),
        (&intent, &begin_receipt), (&acknowledge, &ack_receipt)] {
        exact_receipt(&lab.c, op, receipt).await;
    }
    scan_without(&lab.c, &asker, &event.id).await;
    scan_without(&lab.c, &first_owner, &event.id).await;
    scan_without(&lab.c, &next_owner, &event.id).await;
    assert_eq!(activity(&lab.c).await.iter().filter(|e| e.id == event.id).collect::<Vec<_>>(), vec![&event]);
    assert_eq!(inventory(&lab.c).await, unchanged,
        "routing and receipt replay must not create executions or mutate accepted task inputs");
}
