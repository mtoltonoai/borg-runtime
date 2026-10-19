//! The bootstrap reports below qualify only governance policy setup. The subject
//! task uses the actual native worker, bounded local HTTP and canonical MCP.
#[path = "completion_ack_recovery.rs"]
mod completion_ack_recovery;
#[path = "completion_hold.rs"]
mod completion_hold;
use super::*;
use borg_runtime::{
    collaboration::{questions::*, workflow::*},
    governance::*,
    governance_completion::*,
    limits::{DefinitionLimitField, GovernanceLimits, WorkflowLimits},
    mcp::Caller,
};
use std::collections::BTreeMap;

fn governed(lab: &mut Lab) {
    configure(lab);
    let policy = Arc::make_mut(&mut lab.limits);
    // This fixture selects the full V2 workflow catalogue, whose aggregate
    // schema exceeds the seed's 16,384-byte envelope. Keep a fixed local bound;
    // the production catalogue and all native admission checks still apply.
    policy.execution.native.schema_bytes = 65_536;
    // The isolated helper counts both object keys and values in the full
    // validation envelope; the V2 catalogue exceeds the seed's 4,096 nodes.
    policy.execution.native.json_nodes = 8_192;
    policy.workflow = Some(WorkflowLimits {
        record_bytes: 32_768,
        text_bytes: 4_096,
        options: 8,
        records: 512,
        revisions: 4_096,
        page_items: 8,
        page_bytes: 32_768,
        waits_per_task: 8,
        graph_scan_nodes: 512,
        events_per_mutation: 64,
        event_bytes: 8_192,
        notice_bytes: 8_192,
        pending_delivery_bytes: 32_768,
        delivery_scan_rows: 64,
    });
    policy.governance = Some(GovernanceLimits {
        record_bytes: 65_536,
        reason_items: 16,
        reason_bytes: 4_096,
        scan_rows: 512,
        rule_versions: 16,
        configuration_versions: 16,
        shadow_evidence_items: 16,
        completion_continuations: 2,
        override_max_ms: 10_000,
    });
    save_limits(lab);
}
fn caller(attempt: &Attempt) -> Caller {
    Caller {
        task_id: attempt.task_id.clone(),
        attempt_id: attempt.id.clone(),
        generation: attempt.generation,
        worker_id: attempt.worker_id.clone(),
        incarnation: attempt.worker_incarnation.clone(),
    }
}
async fn query(api: &ApiClient, value: GovernanceQuery) -> GovernanceResult {
    match api
        .call(Operation::QueryGovernance { query: value })
        .await
        .unwrap()
        .outcome
    {
        Outcome::Ok {
            data: Data::Governance(value),
        } => *value,
        other => panic!("governance query {other:?}"),
    }
}
async fn mutate(api: &ApiClient, id: &str, mutation: GovernanceMutation) -> GovernanceResult {
    let value = api
        .call(Operation::MutateGovernance {
            operation_id: id.into(),
            mutation,
        })
        .await
        .unwrap();
    assert!(value.commit.is_some());
    match value.outcome {
        Outcome::Ok {
            data: Data::Governance(value),
        } => *value,
        other => panic!("governance mutation {id}: {other:?}"),
    }
}
async fn report(api: &ApiClient, a: &Attempt, id: &str, observation: Observation) {
    assert!(matches!(
        api.call(Operation::ObserveWorker {
            report: Report {
                report_id: id.into(),
                worker_id: a.worker_id.clone(),
                incarnation: a.worker_incarnation.clone(),
                task_id: a.task_id.clone(),
                attempt_id: a.id.clone(),
                generation: a.generation,
                observation
            }
        })
        .await
        .unwrap()
        .outcome,
        Outcome::Ok { .. }
    ));
}
async fn shadow(
    api: &ApiClient,
    id: &str,
    original: DecisionRef,
    candidate: RuleRef,
    configuration: &str,
) {
    let value = mutate(
        api,
        id,
        GovernanceMutation::EvaluateShadow {
            original,
            candidate,
            configuration_revision: configuration.into(),
        },
    )
    .await;
    let GovernanceResult::Shadow(value) = value else {
        panic!("shadow record")
    };
    assert_eq!(
        value.evaluation,
        ShadowEvaluation::Evaluated {
            verdict: ShadowVerdict::Allow,
            actual_outcome_would_change: false,
            passing: true
        }
    );
}
async fn promote(api: &ApiClient, lab: &Lab) {
    let completion = RuleVersion {
        reference: RuleRef {
            id: "charter-completion".into(),
            revision: "1".into(),
        },
        body: RuleBody::Completion {
            allow_continuation: true,
        },
    };
    let definition = RuleVersion {
        reference: RuleRef {
            id: "charter-definition".into(),
            revision: "1".into(),
        },
        body: RuleBody::DefinitionLimits {
            allow_configured_widening: false,
        },
    };
    for (label, rule) in [
        ("completion", completion.clone()),
        ("definition", definition.clone()),
    ] {
        assert_eq!(
            mutate(
                api,
                &format!("register-{label}"),
                GovernanceMutation::RegisterRule { rule: rule.clone() }
            )
            .await,
            GovernanceResult::Rule(rule)
        );
    }
    let GovernanceResult::Configuration(mut configuration) = query(
        api,
        GovernanceQuery::Configuration {
            revision: lab.limits.revision.clone(),
        },
    )
    .await
    else {
        panic!("bootstrap configuration")
    };
    configuration.revision = "charter-enforced".into();
    configuration.completion = completion.reference.clone();
    configuration.definition_limits = definition.reference.clone();
    assert_eq!(
        mutate(
            api,
            "register-configuration",
            GovernanceMutation::RegisterConfiguration {
                configuration: configuration.clone()
            }
        )
        .await,
        GovernanceResult::Configuration(configuration.clone())
    );
    let mut setup = input(lab, 2);
    setup.definition.id = "policy-bootstrap-subject".into();
    setup.definition.execution_profile =
        Some(ExecutionProfile::CollaborationV2 { publication: None, web_search: None });
    let task = submit_identified(api, setup, "bootstrap-submit").await;
    let worker = Worker {
        id: "policy-bootstrap-worker".into(),
        incarnation: "fixture".into(),
        native_capacity: 1,
        coding_capacity: 0,
    };
    assert!(matches!(
        api.call(Operation::RegisterWorker {
            operation_id: "bootstrap-register".into(),
            worker: worker.clone()
        })
        .await
        .unwrap()
        .outcome,
        Outcome::Ok { .. }
    ));
    let commands = match api
        .call(Operation::AwaitCommands {
            operation_id: "bootstrap-admission".into(),
            worker_id: worker.id.clone(),
            incarnation: worker.incarnation.clone(),
            acknowledged: vec![],
        })
        .await
        .unwrap()
        .outcome
    {
        Outcome::Ok {
            data: Data::Commands(value),
        } => value,
        other => panic!("bootstrap admission {other:?}"),
    };
    let attempt = commands
        .iter()
        .find_map(|command| match command {
            WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == task.id => {
                Some(attempt.clone())
            }
            _ => None,
        })
        .expect("genuine bootstrap admission");
    let acknowledged = commands
        .iter()
        .filter_map(|command| match command {
            WorkerCommand::RunAttempt { command_id, .. } => Some(command_id.clone()),
            _ => None,
        })
        .collect();
    assert!(matches!(
        api.call(Operation::AwaitCommands {
            operation_id: "bootstrap-command-ack".into(),
            worker_id: worker.id,
            incarnation: worker.incarnation,
            acknowledged
        })
        .await
        .unwrap()
        .outcome,
        Outcome::Ok { .. }
    ));
    report(
        api,
        &attempt,
        "bootstrap-start",
        Observation::Started {
            thread_id: None,
            turn_id: None,
        },
    )
    .await;
    report(
        api,
        &attempt,
        &contract::reservation_id(&attempt.id, 1),
        Observation::Usage {
            native_diagnostic_reserved: true,
            model_requests: 1,
            input_tokens: Some(4),
            output_tokens: Some(2),
        },
    )
    .await;
    let actor = caller(&attempt);
    let boundary = CompletionBoundary::NativeResponse { request_index: 1 };
    let evaluate_id = evaluation_id(&actor, &boundary).unwrap();
    let original = api
        .call(Operation::EvaluateCompletion(EvaluateCompletion {
            operation_id: evaluate_id.clone(),
            caller: actor,
            boundary,
            intervening_requests: vec![],
        }))
        .await
        .unwrap();
    assert!(matches!(
        original.outcome,
        Outcome::Ok {
            data: Data::Completion(result)
        } if matches!(*result, CompletionOutcome::NotEnforced)
    ));
    shadow(
        api,
        "shadow-completion",
        DecisionRef {
            operation_id: evaluate_id,
            point: DecisionPoint::Completion,
        },
        completion.reference,
        &configuration.revision,
    )
    .await;
    let before = match api
        .call(Operation::GetAgent {
            agent_id: task.assigned_agent_id.clone(),
        })
        .await
        .unwrap()
        .outcome
    {
        Outcome::Ok {
            data: Data::Agent(value),
        } => value.definition,
        other => panic!("bootstrap definition {other:?}"),
    };
    let update = DefinitionUpdate {
        agent_id: before.id.clone(),
        expected_revision: before.revision.clone(),
        expected_global_revision: lab.limits.revision.clone(),
        expected_policy_revision: lab
            .limits
            .definition_updates
            .as_ref()
            .unwrap()
            .revision
            .clone(),
        changes: BTreeMap::from([(
            DefinitionLimitField::NativeContextBytes,
            lab.limits
                .resolve_definition(&before)
                .unwrap()
                .native
                .context_bytes,
        )]),
    };
    let changed = api
        .call(Operation::UpdateDefinition {
            operation_id: "bootstrap-neutral".into(),
            update,
        })
        .await
        .unwrap();
    assert!(
        matches!(changed.outcome, Outcome::Ok { data: Data::DefinitionUpdate(value) } if value.classification == ConfigurationChangeClass::Neutral)
    );
    shadow(
        api,
        "shadow-definition",
        DecisionRef {
            operation_id: "bootstrap-neutral".into(),
            point: DecisionPoint::DefinitionLimits,
        },
        definition.reference,
        &configuration.revision,
    )
    .await;
    let GovernanceResult::State(before) = query(api, GovernanceQuery::State).await else {
        panic!("governance state")
    };
    assert_eq!(
        mutate(
            api,
            "promote-charter-governance",
            GovernanceMutation::Promote {
                expected_revision: before.selected_revision,
                configuration_revision: configuration.revision.clone(),
                evidence: ["shadow-completion", "shadow-definition"]
                    .into_iter()
                    .map(|id| ShadowEvidenceRef {
                        operation_id: id.into()
                    })
                    .collect(),
                supersedes_override: None
            }
        )
        .await,
        GovernanceResult::State(GovernanceState {
            selected_revision: configuration.revision,
            temporary_override: None
        })
    );
    let current = get(api, &task.id).await;
    assert!(matches!(
        api.call(Operation::CancelTask {
            operation_id: "retire-bootstrap".into(),
            task_id: task.id,
            expected_revision: current.revision
        })
        .await
        .unwrap()
        .outcome,
        Outcome::Ok { .. }
    ));
    report(
        api,
        &attempt,
        "bootstrap-stopped",
        Observation::ProcessTreeStopped,
    )
    .await;
}
async fn closed_event(api: &ApiClient, question_id: &str) -> EventId {
    let mut cursor = None;
    for _ in 0..32 {
        let page = match api
            .call(Operation::QueryWorkflowActivity { cursor, limit: 8 })
            .await
            .unwrap()
            .outcome
        {
            Outcome::Ok {
                data: Data::WorkflowActivity(page),
            } => page,
            other => panic!("workflow event page {other:?}"),
        };
        if let Some(event) = page.items.iter().find(|event| {
            event.subject
                == (RecordRef::Question {
                    question_id: question_id.into(),
                })
                && matches!(
                    event.detail,
                    ActivityDetail::Question(QuestionChange::Closed { .. })
                )
        }) {
            return event.id.clone();
        }
        cursor = page.next_cursor;
        assert!(cursor.is_some(), "actual question terminal event absent");
    }
    panic!("question terminal event exceeded fixture scan bound")
}
fn question(task_id: &str) -> PoseQuestion {
    PoseQuestion {
        record: RecordRef::Task {
            task_id: task_id.into(),
        },
        recipient: PrincipalRef::TrustedHostOperator,
        prompt: "Choose how this fixture should proceed".into(),
        options: vec![
            QuestionOption {
                id: "keep".into(),
                label: "Keep".into(),
                explanation: "Keep the existing choice".into(),
            },
            QuestionOption {
                id: "change".into(),
                label: "Change".into(),
                explanation: "Choose the alternative".into(),
            },
        ],
        recommended_option_id: "keep".into(),
    }
}
fn with_metadata(mut value: Value, revision: &str) -> Value {
    value["output"].as_array_mut().unwrap().insert(0, json!({"id":format!("commentary-{revision}"),"type":"message","role":"assistant","status":"completed","phase":"commentary",
        "content":[{"type":"output_text","text":json!({"charter_references":metadata(revision)}).to_string()}]}));
    value
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn governed_final_reply_keeps_its_index_after_real_compaction_rejection_and_summary() {
    scenario(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn governed_final_compaction_fixes_continuation_target_after_all_real_debits() {
    scenario(true).await;
}
async fn scenario(continue_work: bool) {
    let mut lab = Lab::new("charter-governed-final");
    governed(&mut lab);
    let mut controller = lab.controller("governed-controller").await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    promote(&api, &lab).await;
    let mut retained = input(&lab, 1);
    retained.definition.revision = "r2".into();
    retained.definition.charter = Some(source("r2"));
    let retained = submit_identified(&api, retained, "retain-final-charter").await;
    assert!(matches!(
        api.call(Operation::CancelTask {
            operation_id: "cancel-retained-source".into(),
            task_id: retained.id,
            expected_revision: retained.revision
        })
        .await
        .unwrap()
        .outcome,
        Outcome::Ok { .. }
    ));
    let mut accepted = input(&lab, if continue_work { 7 } else { 5 });
    accepted.definition.execution_profile =
        Some(ExecutionProfile::CollaborationV2 { publication: None, web_search: None });
    let task = submit(&api, accepted).await;
    let subject = task.id.clone();
    let posed = Arc::new(Mutex::new(None::<Question>));
    let peer_question = posed.clone();
    let terminal = Arc::new(Mutex::new(None::<EventId>));
    let peer_terminal = terminal.clone();
    let summary_entered = Arc::new(tokio::sync::Notify::new());
    let summary_release = Arc::new(tokio::sync::Notify::new());
    let entered = summary_entered.clone();
    let release = summary_release.clone();
    let mut peer = BoundedPeer::start(api.clone(), task.id.clone(), lab.limits.clone(), if continue_work { 6 } else { 4 },
        move |index, request| { let subject = subject.clone(); let posed = peer_question.clone();
            let entered = entered.clone(); let release = release.clone(); let terminal = peer_terminal.clone(); async move {
            match index {
                0 => HttpReply::ok(if continue_work { with_metadata(function(0, "collab_question_pose", json!({"question":question(&subject)})), "r1") } else { tool_reply(0, "r1") }),
                1 => {
                    assert!(request["input"].to_string().contains("Charter change"));
                    assert!(request["input"].to_string().contains("r2"));
                    if continue_work {
                        let value = request["input"].as_array().unwrap().iter().rev().find(|value| value["type"] == "function_call_output").unwrap();
                        let result: Value = serde_json::from_str(value["output"].as_str().unwrap()).unwrap();
                        let outcome: Outcome = serde_json::from_value(result["outcome"].clone()).unwrap();
                        let Outcome::Ok { data: Data::QuestionMutation(value) } = outcome else { panic!("actual question receipt") };
                        *posed.lock().unwrap() = Some(value.question.question);
                    }
                    HttpReply::ok(final_reply("r2", json!(7)))
                },
                2 => { assert!(request.get("tools").is_none()); HttpReply::rejected("retry-after-ms: 5\r\n") },
                3 => {
                    assert!(request.get("tools").is_none());
                    if continue_work { entered.notify_one(); release.notified().await; }
                    HttpReply::ok(final_reply("r2", json!({"summary":"The original tool completed. Resolve any remaining question before completion."})))
                },
                4 if continue_work => {
                    assert!(request.get("tools").is_some());
                    let text = request["input"].to_string();
                    assert!(text.contains("Derived conversation summary"));
                    let event = terminal.lock().unwrap().clone().unwrap();
                    let feedback = request["input"].as_array().unwrap().iter().find_map(|message| {
                        if message["role"] != "developer" { return None; }
                        let text = message["content"].as_str()?;
                        let encoded = text.strip_prefix("The controller refused voluntary completion. Address this recorded remaining work within the existing task authority and limits:\n")?;
                        Some(serde_json::from_str::<Value>(encoded).unwrap())
                    }).expect("actual completion feedback missing");
                    let reasons: Vec<CompletionReason> = serde_json::from_value(feedback["remaining_work"].clone()).unwrap();
                    assert_eq!(reasons, vec![CompletionReason::UnconsumedInput { event_id: event.clone() }],
                        "concrete original remaining-work event differs");
                    let expected = serde_json::to_value(&event).unwrap();
                    assert!(request["input"].as_array().unwrap().iter().any(|message| {
                        if message["role"] != "developer" { return false; }
                        let Some(content) = message["content"].as_str() else { return false; };
                        let Ok(notice) = serde_json::from_str::<Value>(content) else { return false; };
                        notice["type"] == "collaboration_activity"
                            && notice["events"].as_array().is_some_and(|events| events.iter().any(|event| event["id"] == expected))
                    }), "exact terminal event missing from actual routed input");
                    // Consuming the exact terminal event is the required work;
                    // an unanswered optional question alone is not a hold.
                    HttpReply::ok(tool_reply(4, "r2"))
                },
                5 if continue_work => HttpReply::ok(final_reply("r2", json!(7))),
                _ => panic!("unexpected final-compaction POST index"),
            }
        }}).await;
    let proxy = Proxy::start(lab.url.clone(), None, false, lab.limits.clone()).await;
    let held = Arc::new(HeldToolReply::new(if continue_work {
        "collab_question_pose"
    } else {
        "collab_memory_get"
    }));
    *proxy.held_reply.lock().unwrap() = Some(held.clone());
    let mut worker = lab.worker(&proxy.url, &peer.endpoint, "governed-native-worker");
    tokio::time::timeout(Duration::from_secs(20), held.entered.notified())
        .await
        .unwrap();
    assert!(matches!(
        pending(&api, &task.session_id, "r2").await.outcome,
        Outcome::Ok { .. }
    ));
    *proxy.held_reply.lock().unwrap() = None;
    held.released.store(true, Ordering::SeqCst);
    held.release.notify_one();
    if continue_work {
        tokio::time::timeout(Duration::from_secs(20), summary_entered.notified())
            .await
            .unwrap();
        let question = posed.lock().unwrap().clone().unwrap();
        let closed = api
            .call(Operation::MutateQuestion {
                operation_id: "question-terminal-during-summary".into(),
                mutation: QuestionMutation::Cancel {
                    question_id: question.id.clone(),
                    expected_revision: question.revision,
                    reason:
                        "The operator resolved the optional question during the original summary"
                            .into(),
                },
            })
            .await
            .unwrap();
        assert!(matches!(
            closed.outcome,
            Outcome::Ok {
                data: Data::QuestionMutation(_)
            }
        ));
        let event = closed_event(&api, &question.id).await;
        *terminal.lock().unwrap() = Some(event);
        summary_release.notify_one();
    }
    let done = until(&api, &task.id, |task| task.status == TaskStatus::Succeeded).await;
    let attempt = done.attempt.as_ref().unwrap();
    let actor = caller(attempt);
    let total = if continue_work { 6 } else { 4 };
    assert_eq!(peer.count(), total);
    assert_eq!(live_usage(&api,attempt).await.0, total as u32);
    assert_eq!(done.result.as_ref().unwrap().output, Some(json!(7)));
    {
        let bodies = peer.bodies.lock().unwrap();
        assert_eq!(
            bodies[2], bodies[3],
            "compaction retry changed actual request bytes"
        );
    }
    for index in [3, 4] {
        let run = match api
            .call(Operation::QueryCharter {
                query: CharterQuery::Compaction {
                    run_id: format!("{}-charter-compaction-{index}", attempt.id),
                },
            })
            .await
            .unwrap()
            .outcome
        {
            Outcome::Ok {
                data: Data::CharterQuery(result),
            } => match *result {
                CharterQueryResult::Compaction(run) => run,
                other => panic!("original compaction {other:?}"),
            },
            other => panic!("original compaction {other:?}"),
        };
        assert_eq!(run.request_index, index);
        assert_eq!(run.caller, actor);
        assert_eq!(run.effective.reference, reference("r2"));
        if index == 3 {
            assert!(run.reply.is_none() && run.summary.is_none() && run.disposition.is_none());
        } else {
            let captured = run
                .reply
                .as_ref()
                .expect("successful summary reply is durable");
            assert_eq!(
                captured.observation.output.as_ref().unwrap(),
                &final_reply(
                    "r2",
                    json!({"summary":"The original tool completed. Resolve any remaining question before completion."})
                )["output"]
            );
            assert_eq!(
                run.disposition,
                Some(CompactionDisposition::CurrentSourceSummary)
            );
            assert!(run.summary.is_some());
        }
    }
    let ordinary = turns(&api, &task.session_id).await;
    let original = ordinary
        .iter()
        .find(|turn| turn.boundary.request_index == 2)
        .unwrap();
    assert!(original.reply.is_some() && original.completed_by.is_some());
    assert!(
        original
            .reply
            .as_ref()
            .unwrap()
            .observation
            .tool_call_ids
            .is_empty()
    );
    assert!(
        !ordinary
            .iter()
            .any(|turn| matches!(turn.boundary.request_index, 3 | 4)),
        "compactions invented ordinary turns"
    );
    let boundary = CompletionBoundary::NativeResponse { request_index: 2 };
    let evaluate_id = evaluation_id(&actor, &boundary).unwrap();
    let evaluated = receipt(&api, &evaluate_id).await.unwrap();
    let Operation::EvaluateCompletion(evaluation) =
        serde_json::from_value(evaluated.intended.clone()).unwrap()
    else {
        panic!("original evaluation")
    };
    assert_eq!(evaluation.boundary, boundary);
    assert_eq!(
        evaluation.intervening_requests,
        vec![
            contract::NativeRequestEvidence::ConfirmedRejection {
                request_index: 3,
                operation_id: contract::rejection_id(&attempt.id, 3)
            },
            contract::NativeRequestEvidence::Compaction {
                request_index: 4,
                operation_id: format!("{}-charter-compaction-4", attempt.id)
            },
        ]
    );
    let GovernanceResult::Decision(decision) = query(
        &api,
        GovernanceQuery::Decision {
            decision: DecisionRef {
                operation_id: evaluate_id.clone(),
                point: DecisionPoint::Completion,
            },
        },
    )
    .await
    else {
        panic!("decision")
    };
    assert_eq!(decision.mode, DecisionMode::Enforcing);
    let DecisionDetail::Completion(completion) = decision.detail else {
        panic!("completion decision")
    };
    if continue_work {
        assert!(matches!(
            completion.disposition,
            CompletionDisposition::Blocked {
                continuation_eligible: true,
                ..
            }
        ));
        let event = terminal.lock().unwrap().clone().unwrap();
        assert!(completion.reasons.iter().any(|reason| matches!(reason,
            CompletionReason::UnconsumedInput { event_id } if event_id == &event)));
        let begin_id = begin_id(
            &actor,
            &DecisionRef {
                operation_id: evaluate_id,
                point: DecisionPoint::Completion,
            },
        )
        .unwrap();
        let begin = receipt(&api, &begin_id).await.unwrap();
        let Operation::BeginCompletionContinuation(intent) =
            serde_json::from_value(begin.intended).unwrap()
        else {
            panic!("begin continuation")
        };
        assert_eq!(
            intent.target,
            CompletionTarget::NativeRequest { request_index: 5 }
        );
        let batch_id = intent
            .delivery_batch_id
            .expect("exact workflow batch paired with completion");
        let batch_record = receipt(&api, &batch_id).await.unwrap();
        let Outcome::Ok {
            data: Data::CollaborationDelivery(delivery),
        } = serde_json::from_value::<Outcome>(batch_record.outcome.unwrap()).unwrap()
        else {
            panic!("original delivery prepare")
        };
        let batch = delivery.pending.expect("original retained prepared batch");
        assert_eq!(batch.batch_id, batch_id);
        assert!(
            batch.page.items.iter().any(|item| item.id == event)
                || batch
                    .page
                    .routing
                    .as_ref()
                    .is_some_and(|routing| routing.items.iter().any(|item| item.id == event))
        );
        let ack = receipt(&api, &acknowledgement_id(&actor, &begin_id).unwrap())
            .await
            .unwrap();
        let Operation::AcknowledgeCompletionInput(ack) =
            serde_json::from_value(ack.intended).unwrap()
        else {
            panic!("ack")
        };
        assert_eq!(
            ack.consumed_at,
            CompletionBoundary::NativeResponse { request_index: 5 }
        );
        let delivery_ack = receipt(
            &api,
            &ack.delivery_ack_operation_id
                .expect("paired original delivery ACK"),
        )
        .await
        .unwrap();
        assert!(matches!(
            serde_json::from_value::<Outcome>(delivery_ack.outcome.unwrap()).unwrap(),
            Outcome::Ok { .. }
        ));
        let Operation::AcknowledgeCollaborationDelivery {
            batch_id: actual_batch,
            caller: actual_caller,
            ..
        } = serde_json::from_value(delivery_ack.intended).unwrap()
        else {
            panic!("original delivery ACK body")
        };
        assert_eq!(actual_batch, batch_id);
        assert_eq!(actual_caller, actor);
    } else {
        assert_eq!(completion.disposition, CompletionDisposition::Admit);
    }
    let row = receipt(&api, &contract::completion_id(&attempt.id))
        .await
        .unwrap();
    let Operation::ObserveWorker { report } = serde_json::from_value(row.intended).unwrap() else {
        panic!("completion report")
    };
    let Observation::NativeLoopCompleted {
        model_requests,
        calls,
        requests,
        final_response_index,
    } = report.observation
    else {
        panic!("completion evidence")
    };
    assert_eq!(model_requests, total as u32);
    assert_eq!(
        final_response_index,
        if continue_work { None } else { Some(2) }
    );
    assert_eq!(calls.len(), if continue_work { 2 } else { 1 });
    assert!(requests.iter().any(|entry| matches!(
        entry,
        contract::NativeRequestEvidence::ConfirmedRejection {
            request_index: 3,
            ..
        }
    )));
    assert!(requests.iter().any(|entry| matches!(
        entry,
        contract::NativeRequestEvidence::Compaction {
            request_index: 4,
            ..
        }
    )));
    assert_eq!(
        requests
            .iter()
            .filter(|entry| matches!(
                entry,
                contract::NativeRequestEvidence::CompletionContinuation { .. }
            ))
            .count(),
        usize::from(continue_work)
    );
    assert!(
        binding(&api, &task.session_id)
            .await
            .session
            .compaction_due
            .is_none()
    );
    worker.stop().await;
    controller.stop().await;
    peer.close().await;
}
