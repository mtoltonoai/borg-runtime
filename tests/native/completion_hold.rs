//! Actual native worker completion holds against the canonical Controller.
//! The local peer supplies completed responses; bootstrap reports select policy
//! only. No live provider or supplied coding executor is involved.
use super::*;

fn stored_hold(lab: &Lab, task_id: &str) -> CompletionHold {
    let db = rusqlite::Connection::open_with_flags(
        lab.root.join("tasks.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let body: String = db
        .query_row("SELECT body FROM tasks WHERE id=?1", [task_id], |row| {
            row.get(0)
        })
        .unwrap();
    let value: Value = serde_json::from_str(&body).unwrap();
    serde_json::from_value(value["completion_hold"].clone()).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exhausted_native_completion_stops_without_request_and_preserves_original_after_restart() {
    Box::pin(scenario(false)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_completion_evaluator_bound_failure_stops_without_request_and_preserves_original_after_restart(
) {
    Box::pin(scenario(true)).await;
}

async fn scenario(evaluator_failure: bool) {
    let mut lab = Lab::new(if evaluator_failure {
        "native-completion-bound-failure"
    } else {
        "native-completion-exhausted"
    });
    governed(&mut lab);
    if evaluator_failure {
        // This case needs two independently committed tool results in POST1.
        Arc::make_mut(&mut lab.limits)
            .execution
            .native
            .function_calls_per_response = 2;
    }
    let bounds = Arc::make_mut(&mut lab.limits).governance.as_mut().unwrap();
    if evaluator_failure {
        // Two real required terminal inputs exceed this valid fixture bound.
        // The evaluator must retain its typed failure, not truncate to success.
        bounds.reason_items = 1;
    } else {
        // Zero is the declared no-additional-continuation setting, not a
        // reduced model-request allowance or a new production default.
        bounds.completion_continuations = 0;
    }
    save_limits(&lab);
    let mut controller = lab.controller("hold-controller").await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    promote(&api, &lab).await;
    let mut accepted = input(&lab, 3);
    accepted.definition.execution_profile =
        Some(ExecutionProfile::CollaborationV2 { publication: None, web_search: None });
    let task = submit_identified(&api, accepted, "submit-native-completion-hold").await;
    let required_count = if evaluator_failure { 2 } else { 1 };
    let subject = task.id.clone();
    let posed = Arc::new(Mutex::new(Vec::<Question>::new()));
    let questions = posed.clone();
    let entered = Arc::new(tokio::sync::Notify::new());
    let released = Arc::new(tokio::sync::Notify::new());
    let final_entered = entered.clone();
    let final_released = released.clone();
    let mut peer = BoundedPeer::start(
        api.clone(),
        task.id.clone(),
        lab.limits.clone(),
        2,
        move |index, request| {
            let (subject, questions, entered, released) = (
                subject.clone(),
                questions.clone(),
                final_entered.clone(),
                final_released.clone(),
            );
            async move {
                match index {
                    0 => {
                        let mut reply = function(
                            0,
                            "collab_question_pose",
                            json!({"question":question(&subject)}),
                        );
                        if required_count == 2 {
                            let second = function(
                                1,
                                "collab_question_pose",
                                json!({"question":question(&subject)}),
                            );
                            reply["output"]
                                .as_array_mut()
                                .unwrap()
                                .push(second["output"][0].clone());
                        }
                        HttpReply::ok(with_metadata(reply, "r1"))
                    }
                    1 => {
                        let outputs: Vec<_> = request["input"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .filter(|item| item["type"] == "function_call_output")
                            .collect();
                        assert_eq!(outputs.len(), required_count);
                        let mut actual = Vec::new();
                        for (index, output) in outputs.into_iter().enumerate() {
                            assert_eq!(output["call_id"], format!("call-{index}"));
                            let result: Value =
                                serde_json::from_str(output["output"].as_str().unwrap()).unwrap();
                            let Outcome::Ok {
                                data: Data::QuestionMutation(value),
                            } = serde_json::from_value(result["outcome"].clone()).unwrap()
                            else {
                                panic!("actual native question receipt")
                            };
                            actual.push(value.question.question);
                        }
                        *questions.lock().unwrap() = actual;
                        entered.notify_one();
                        tokio::time::timeout(Duration::from_secs(30), released.notified())
                            .await
                            .unwrap();
                        HttpReply::ok(final_reply("r1", json!(7)))
                    }
                    _ => panic!("completion hold authorized another physical POST"),
                }
            }
        },
    )
    .await;
    let proxy = Proxy::start(lab.url.clone(), None, false, lab.limits.clone()).await;
    let mut worker = lab.worker(&proxy.url, &peer.endpoint, "native-hold-worker");
    tokio::time::timeout(Duration::from_secs(30), entered.notified())
        .await
        .unwrap();
    let active = get(&api, &task.id).await;
    assert_eq!(active.status, TaskStatus::Active);
    assert_eq!(active.input, task.input);
    let attempt = active.attempt.clone().unwrap();
    let actor = caller(&attempt);
    assert_eq!(attempt.phase, AttemptPhase::Running);
    assert!(!attempt.stopped && !attempt.uncertain);
    let questions = posed.lock().unwrap().clone();
    assert_eq!(questions.len(), required_count);
    let mut terminal_events = Vec::new();
    for (index, question) in questions.iter().enumerate() {
        assert_eq!(
            question.poser.principal,
            PrincipalRef::Agent {
                agent_id: task.assigned_agent_id.clone()
            }
        );
        assert_eq!(
            question.poser.provenance,
            ActorProvenance::BoundAgentSession {
                agent_id: task.assigned_agent_id.clone(),
                session_id: task.session_id.clone(),
                task_id: task.id.clone(),
                attempt_id: attempt.id.clone(),
                generation: attempt.generation,
                worker_id: attempt.worker_id.clone(),
                incarnation: attempt.worker_incarnation.clone(),
            }
        );
        let closed = api.call(Operation::MutateQuestion {
            operation_id: format!("resolve-before-held-final-{index}"),
            mutation: QuestionMutation::Cancel { question_id: question.id.clone(), expected_revision: question.revision,
                reason: "Operator resolved the question while the original native response was in flight".into() },
        }).await.unwrap();
        assert!(closed.commit.is_some());
        assert!(matches!(
            closed.outcome,
            Outcome::Ok {
                data: Data::QuestionMutation(_)
            }
        ));
        let event = closed_event(&api, &question.id).await;
        assert!(!terminal_events.contains(&event));
        terminal_events.push(event);
    }
    released.notify_one();
    let held = until(&api, &task.id, |task| {
        task.status == TaskStatus::Queued
            && task
                .attempt
                .as_ref()
                .is_some_and(|attempt| attempt.stopped && !attempt.uncertain)
    })
    .await;
    assert_eq!(
        held.blocked_reason.as_deref(),
        Some("governance_completion_blocked")
    );
    assert!(held.result.is_none());
    assert_eq!(held.input, task.input);
    assert_eq!(held.session_id, task.session_id);
    assert_eq!(held.assigned_agent_id, task.assigned_agent_id);
    let stopped = held.attempt.as_ref().unwrap();
    assert_eq!(stopped.id, attempt.id);
    assert_eq!(stopped.generation, attempt.generation);
    assert_eq!(stopped.deadline_unix_ms, attempt.deadline_unix_ms);
    assert!(
        process::now_ms() < attempt.deadline_unix_ms,
        "deadline did not cause this hold"
    );
    assert_eq!(peer.count(), 2);
    assert_eq!(live_usage(&api, stopped).await, (2, Some(10), Some(4)));
    assert_eq!(
        task.input.limits.model_requests, 3,
        "one request remained available"
    );
    assert!(receipt(&api, &contract::reservation_id(&attempt.id, 3))
        .await
        .is_none());

    let boundary = CompletionBoundary::NativeResponse { request_index: 2 };
    let decision_ref = DecisionRef {
        operation_id: evaluation_id(&actor, &boundary).unwrap(),
        point: DecisionPoint::Completion,
    };
    let original = query(
        &api,
        GovernanceQuery::Decision {
            decision: decision_ref.clone(),
        },
    )
    .await;
    let GovernanceResult::Decision(ref record) = original else {
        panic!("original completion decision")
    };
    assert_eq!(record.id, decision_ref);
    assert_eq!(record.mode, DecisionMode::Enforcing);
    assert_eq!(
        record.actual_outcome,
        ActualDecisionOutcome::GovernanceBlocked
    );
    let DecisionDetail::Completion(ref state) = record.detail else {
        panic!("typed completion decision")
    };
    assert_eq!(state.consecutive_count, 1);
    assert_eq!(
        state.disposition,
        CompletionDisposition::Blocked {
            continuation_eligible: false,
            exhausted: !evaluator_failure
        }
    );
    if evaluator_failure {
        assert_eq!(
            state.reasons,
            vec![CompletionReason::EvaluationUnavailable {
                code: CompletionFailure::BoundExceeded
            }]
        );
    } else {
        assert_eq!(
            state.reasons,
            vec![CompletionReason::UnconsumedInput {
                event_id: terminal_events[0].clone()
            }]
        );
    }
    let bounds = task
        .input
        .resolved_limits
        .as_ref()
        .unwrap()
        .global_metadata
        .governance
        .as_ref()
        .unwrap();
    assert!(state.reasons.len() <= bounds.reason_items);
    assert!(serde_json::to_vec(&state.reasons).unwrap().len() <= bounds.reason_bytes);
    let evaluation = receipt(&api, &decision_ref.operation_id).await.unwrap();
    assert_eq!(
        evaluation.intended,
        json!(Operation::EvaluateCompletion(EvaluateCompletion {
            operation_id: decision_ref.operation_id.clone(),
            caller: actor.clone(),
            boundary: boundary.clone(),
            intervening_requests: vec![],
        }))
    );
    assert!(
        matches!(serde_json::from_value::<Outcome>(evaluation.outcome.clone().unwrap()).unwrap(),
        Outcome::Refused { code, .. } if code == "governance_completion_blocked")
    );
    let begin = receipt(&api, &begin_id(&actor, &decision_ref).unwrap())
        .await
        .unwrap();
    let Operation::BeginCompletionContinuation(ref begin_input) =
        serde_json::from_value::<Operation>(begin.intended.clone()).unwrap()
    else {
        panic!("original continuation refusal")
    };
    assert_eq!(begin_input.caller, actor);
    assert_eq!(begin_input.decision, decision_ref);
    assert_eq!(
        begin_input.target,
        CompletionTarget::NativeRequest { request_index: 3 }
    );
    assert!(
        matches!(serde_json::from_value::<Outcome>(begin.outcome.clone().unwrap()).unwrap(),
        Outcome::Refused { code, .. } if code == "governance_completion_blocked")
    );
    assert!(
        receipt(&api, &acknowledgement_id(&actor, &begin.id).unwrap())
            .await
            .is_none()
    );
    let stop = receipt(&api, &format!("{}-completion-held-stopped", attempt.id))
        .await
        .unwrap();
    let Operation::ObserveWorker { report } =
        serde_json::from_value::<Operation>(stop.intended.clone()).unwrap()
    else {
        panic!("physical stop report")
    };
    assert_eq!(report.attempt_id, attempt.id);
    assert_eq!(report.generation, attempt.generation);
    assert!(matches!(
        report.observation,
        Observation::ProcessTreeStopped
    ));
    assert!(matches!(
        serde_json::from_value::<Outcome>(stop.outcome.clone().unwrap()).unwrap(),
        Outcome::Ok { .. }
    ));

    worker.stop().await;
    controller.stop().await;
    let before_hold = stored_hold(&lab, &task.id);
    assert_eq!(before_hold.decision, decision_ref);
    assert_eq!(
        &before_hold.state, state,
        "physical stop retained the original completion hold"
    );
    assert!(before_hold.continuation.is_none());
    assert_eq!(lab.usage(&attempt.id), (2, Some(10), Some(4)));
    let mut reopened = lab.controller("hold-controller-reopened").await;
    assert_eq!(get(&api, &task.id).await, held);
    assert_eq!(
        query(
            &api,
            GovernanceQuery::Decision {
                decision: decision_ref.clone()
            }
        )
        .await,
        original
    );
    for row in [&evaluation, &begin, &stop] {
        assert_eq!(receipt(&api, &row.id).await.as_ref(), Some(row));
    }
    let mut replacement = lab.worker(&proxy.url, &peer.endpoint, "native-hold-replacement");
    registered(&lab, &api, "native-hold-replacement", &mut replacement).await;
    // Registration alone is before command admission. Require an actual
    // replacement AwaitCommands reply through the existing proxy as well.
    // An idle accepted AwaitCommands may consume the whole controller long
    // poll before Pending. Include the accepted client transport bound.
    let await_bound = Duration::from_millis(lab.limits.controller.command_wait_ms)
        .checked_add(Duration::from_millis(
            lab.limits.transport.client_timeout_ms,
        ))
        .unwrap();
    tokio::time::timeout(await_bound, async {
        loop {
            assert!(replacement.child.try_wait().unwrap().is_none());
            if proxy
                .await_replies
                .lock()
                .unwrap()
                .iter()
                .any(|(worker, incarnation)| {
                    worker == "loop-worker" && incarnation == "native-hold-replacement"
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("replacement reached actual command admission");
    assert_eq!(get(&api, &task.id).await, held);
    assert_eq!(live_usage(&api, stopped).await, (2, Some(10), Some(4)));
    assert!(receipt(&api, &contract::reservation_id(&attempt.id, 3))
        .await
        .is_none());
    assert_eq!(
        query(
            &api,
            GovernanceQuery::Decision {
                decision: decision_ref
            }
        )
        .await,
        original
    );
    replacement.stop().await;
    reopened.stop().await;
    assert_eq!(stored_hold(&lab, &task.id), before_hold);
    assert_eq!(lab.usage(&attempt.id), (2, Some(10), Some(4)));
    assert_eq!(
        peer.count(),
        2,
        "restart did not turn the held final reply into another request"
    );
    peer.close().await;
}
