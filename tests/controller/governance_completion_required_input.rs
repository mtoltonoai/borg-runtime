//! Child of governance_completion: real promoted rules, original receipts and
//! Controller delivery operations. No SQL fixture mutation or model claim.
use super::*;

async fn advisory(c: &Controller, owner: &Task) -> WorkflowActivity {
    let mut question = pose_input(&owner.id);
    question.recipient = PrincipalRef::Agent { agent_id:owner.assigned_agent_id.clone() };
    let posed = match call(c, Operation::MutateQuestion { operation_id:id("begin-advisory"),
        mutation:QuestionMutation::Pose { question },
    }).await {
        Outcome::Ok { data:Data::QuestionMutation(value) } => value.question.question,
        other => panic!("actual addressed advisory expected: {other:?}"),
    };
    events(c).await.into_iter().find(|event|
        event.subject == (RecordRef::Question { question_id:posed.id.clone() })
            && matches!(&event.detail, ActivityDetail::Question(QuestionChange::Posed { .. })))
        .expect("canonical addressed question event")
}

#[tokio::test]
async fn completion_begin_requires_an_actual_batch_intersecting_original_unconsumed_events() {
    for mode in ["missing", "wrong_nonempty", "required"] {
        let c = Controller::open_with_config(path(), governance_config()).unwrap();
        promote(&c, &id("required-batch-policy"), true).await;
        let (accepted, _attempt, actor, _worker) = active(&c, mode).await;
        let unrelated = if mode == "wrong_nonempty" { Some(advisory(&c, &accepted).await) } else { None };
        let question = pose(&c, &actor).await;
        let terminal = close_question(&c, &question, None).await;
        let (evaluation, refusal, state) = evaluated(&c, &actor, 1, true).await;
        assert!(has_input(&state, &terminal.id));
        assert_eq!(state.disposition, CompletionDisposition::Blocked { continuation_eligible:true, exhausted:false });
        let original = audit(&c, &evaluation).await;
        let batch = match mode {
            "missing" => None,
            "wrong_nonempty" => {
                let view = observe(&c, &actor).await;
                let batch = prepare(&c, &actor, &view).await;
                assert!(batch.page.has_input()); assert!(!batch.page.contains_event(&terminal.id));
                assert!(batch.page.contains_event(&unrelated.as_ref().unwrap().id));
                Some(batch)
            },
            _ => {
                let view = observe_required_input(&c, &actor).await;
                let batch = prepare(&c, &actor, &view).await;
                assert!(batch.page.contains_event(&terminal.id)); Some(batch)
            },
        };
        let delivery_before = observe(&c, &actor).await;
        let begin = begin_completion(&actor, &decision_ref(&evaluation), 2,
            batch.as_ref().map(|batch|batch.batch_id.clone()));
        let begun = reply(&c, begin.clone()).await;
        if mode == "required" {
            let Outcome::Ok { data:Data::Completion(completion) } = &begun.outcome
                else { panic!("exact required batch must permit the original next input: {begun:?}") };
            let CompletionOutcome::Begun(feedback) = completion.as_ref()
                else { panic!("exact required batch must begin the original next input: {begun:?}") };
            assert_eq!(feedback.evaluation.state, state);
            assert_eq!(feedback.evaluation.decision, original.id);
            assert_eq!(feedback.continuation.delivery_batch_id.as_deref(), batch.as_ref().map(|b|b.batch_id.as_str()));
            assert!(feedback.continuation.acknowledged.is_none());
        } else {
            assert_eq!(code(begun.outcome.clone()), "delivery_pending");
        }
        same_original(&reply(&c, begin).await, &begun);
        same_original(&reply(&c, evaluation.clone()).await, &refusal);
        assert_eq!(audit(&c, &evaluation).await, original);
        let after = observe(&c, &actor).await;
        assert_eq!(after.acknowledged_cursor, delivery_before.acknowledged_cursor);
        assert_eq!(after.routing_cursor, delivery_before.routing_cursor);
        assert_eq!(after.pending, delivery_before.pending, "Begin neither ACKs nor replaces workflow input");
        assert!(after.pending.as_ref().is_none_or(|pending|pending.delivery.is_none()));
        assert_eq!(get(&c, &accepted.id).await.input, accepted.input);
    }
}

#[tokio::test]
async fn completion_begin_accepts_one_required_page_without_dropping_other_original_reasons() {
    let c = Controller::open_with_config(path(), governance_config()).unwrap();
    promote(&c, "required-intersection-policy", true).await;
    let (_accepted, _attempt, actor, _worker) = active(&c, "required-intersection").await;
    let first = pose(&c, &actor).await;
    let second = pose(&c, &actor).await;
    let first_event = close_question(&c, &first, None).await;
    let second_event = close_question(&c, &second, None).await;
    let (evaluation, _, state) = evaluated(&c, &actor, 1, true).await;
    assert!(has_input(&state, &first_event.id)); assert!(has_input(&state, &second_event.id));
    let view = observe_required_input(&c, &actor).await;
    let prepared = delivery(reply(&c, Operation::PrepareCollaborationDelivery {
        operation_id:id("one-required-page"), caller:actor.clone(),
        expected_cursor:view.acknowledged_cursor.clone(), expected_routing_cursor:view.routing_cursor.clone(),
        through:view.available.as_ref().unwrap().through.clone(), limit:1,
    }).await).pending.unwrap();
    assert!(prepared.page.contains_event(&first_event.id));
    assert!(!prepared.page.contains_event(&second_event.id));
    assert!(prepared.page.routing.as_ref().unwrap().has_more);
    let begin = begin_completion(&actor, &decision_ref(&evaluation), 2, Some(prepared.batch_id.clone()));
    let begun = reply(&c, begin.clone()).await;
    let Outcome::Ok { data:Data::Completion(completion) } = &begun.outcome
        else { panic!("a bounded page intersecting required work must permit progress: {begun:?}") };
    let CompletionOutcome::Begun(feedback) = completion.as_ref()
        else { panic!("a bounded page intersecting required work must begin progress: {begun:?}") };
    assert_eq!(feedback.evaluation.state, state);
    assert!(has_input(&feedback.evaluation.state, &first_event.id));
    assert!(has_input(&feedback.evaluation.state, &second_event.id));
    assert_eq!(observe(&c, &actor).await.pending, Some(prepared));
    same_original(&reply(&c, begin).await, &begun);
}

#[tokio::test]
async fn initial_reset_begin_without_its_exact_reset_event_batch_is_a_durable_refusal() {
    let c = Controller::open_with_config(path(), governance_config()).unwrap();
    promote(&c, "required-initial-reset-policy", true).await;
    let (accepted, attempt, actor, worker) = active(&c, "required-initial-reset").await;
    let old_question = pose(&c, &actor).await;
    close_question(&c, &old_question, Some(&actor)).await;
    evaluated(&c, &actor, 1, true).await;
    reserve(&c, &attempt, 2).await; evaluated(&c, &actor, 2, true).await;
    reserve(&c, &attempt, 3).await;
    let (evaluation, original_reply, exhausted) = evaluated(&c, &actor, 3, true).await;
    assert!(matches!(exhausted.disposition, CompletionDisposition::Blocked { exhausted:true, .. }));
    let original_audit = audit(&c, &evaluation).await;
    let stopped = task(report(&c, &attempt, &id("required-reset-stop"), Observation::ProcessTreeStopped).await);
    assert_eq!(stopped.status, TaskStatus::Queued); assert!(stopped.attempt.unwrap().stopped);
    let (question, _wait) = required_question(&c, &accepted.id).await;
    let reset = close_question(&c, &question, None).await;
    let commands = commands(&c, &worker, vec![]).await;
    let (next, resume) = commands.into_iter().find_map(|command| match command {
        WorkerCommand::RunAttempt { attempt, completion_continuation:Some(resume), .. }
            if attempt.task_id == accepted.id => Some((attempt, resume)),
        _ => None,
    }).expect("ordinary same-task reset admission");
    assert_eq!(resume.reset_event, Some(reset.id.clone()));
    assert_eq!(resume.evaluation.decision, original_audit.id);
    assert_eq!(resume.evaluation.state, exhausted);
    task(report(&c, &next, &id("required-reset-start"), Observation::Started { thread_id:None, turn_id:None }).await);
    let successor = caller(&next);
    let before = observe(&c, &successor).await;
    assert!(before.pending.is_none());
    assert!(before.available.as_ref().unwrap().contains_event(&reset.id),
        "the admitted continuation exposes its exact required event before Begin");
    let begin = Operation::BeginCompletionContinuation(BeginCompletionContinuation {
        operation_id:begin_id(&successor, &resume.evaluation.decision).unwrap(), caller:successor.clone(),
        decision:resume.evaluation.decision.clone(), context_binding:id("missing-reset-context"),
        target:CompletionTarget::InitialNativeRequest { request_index:1 }, delivery_batch_id:None,
    });
    let refusal = reply(&c, begin.clone()).await;
    assert_eq!(code(refusal.outcome.clone()), "delivery_pending");
    same_original(&reply(&c, begin).await, &refusal);
    same_original(&reply(&c, evaluation.clone()).await, &original_reply);
    assert_eq!(audit(&c, &evaluation).await, original_audit);
    assert_eq!(observe(&c, &successor).await, before);
    assert_eq!(get(&c, &accepted.id).await.input, accepted.input);
}
