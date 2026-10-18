//! Canonical charter/output-cap receipt joins using controlled host observations.
//! No provider or helper execution is claimed by this controller fixture.
use super::*;
use borg_runtime::native::ProviderRequestBinding;

fn cap_config() -> ControllerConfig {
    let mut cfg = charter_config();
    Arc::make_mut(&mut cfg.limits).native_retry = Some(borg_runtime::limits::NativeRetryLimits {
        initial_backoff_ms: 5,
        backoff_multiplier: 2,
        max_backoff_ms: 20,
        jitter_ms: 1,
        circuit: borg_runtime::limits::NativeCircuitLimits {
            failure_threshold: 2, open_ms: 50, max_entries: 8,
        },
        auth_cooldown_ms: 80,
        auth_max_entries: 8,
        coordination_record_bytes: 65_536,
    });
    cfg.limits.validate().unwrap();
    cfg
}

fn binding(bytes: &[u8]) -> ProviderRequestBinding {
    ProviderRequestBinding {
        bytes: bytes.len() as u64,
        sha256: aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes)
            .as_ref().iter().map(|b| format!("{b:02x}")).collect(),
    }
}

#[tokio::test]
async fn capped_partial_call_closes_only_after_original_cap_without_fabricating_results() {
    // Wrong output SHA uses a separate original operation/DB. Its committed
    // refusal cannot be reused or overwritten by the successful positive case.
    for wrong_sha in [true, false] {
        let p = path();
        let cfg = cap_config();
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        let (task_input, attempt, caller) = setup(&c, "cap-charter").await;
        let boundary = begin(&c, &attempt, &caller, 1, "cap-charter-turn").await;
        let original_items = json!([{
            "id": "partial-function-item", "type": "function_call", "status": "incomplete",
            "call_id": "partial-provider-call", "name": "collab_task_get",
            "arguments": "{\"task_id\":",
        }]);
        let observed = CharterReplyObservation {
            output_reference: None,
            output: Some(original_items.clone()),
            response_id: Some("controlled-capped-response".into()),
            output_item_ids: vec!["partial-function-item".into()],
            tool_call_ids: vec!["partial-provider-call".into()],
            declaration: None,
        };
        let reply_operation = Operation::ObserveCharter {
            operation_id: "cap-charter-reply".into(), caller: caller.clone(),
            observation: CharterObservation::Reply {
                turn_id: boundary.turn_id.clone(), reply: observed.clone(),
            },
        };
        let original_reply = exchange(&c, reply_operation.clone()).await;
        let CharterOutcome::Reply(original_record) = outcome(&original_reply) else { panic!() };
        assert_eq!(original_record.observation, observed);
        assert!(original_record.audit.findings.contains(&CharterAuditFinding::MissingMetadata));
        let unclosed = turn(&c, &boundary.turn_id).await;
        assert!(unclosed.completed_by.is_none());

        let premature = observe(&c, &caller, "cap-close-before-evidence", CharterObservation::CompleteTurn {
            turn_id: boundary.turn_id.clone(), results: vec![],
        }).await;
        assert_eq!(code(premature.outcome), "reconciliation_required");
        assert_eq!(turn(&c, &boundary.turn_id).await, unclosed);
        let bytes = serde_json::to_vec(&original_items).unwrap();
        let mut output_binding = binding(&bytes);
        if wrong_sha {
            output_binding.sha256.replace_range(..1,
                if output_binding.sha256.starts_with('0') { "1" } else { "0" });
        }
        let cap_id = ids::output_cap_id(&attempt.id, 1);
        // b"{}" is controlled fixture request evidence, not a measured POST.
        let cap_operation = Operation::ObserveWorker { report: Report {
            report_id: cap_id.clone(), task_id: attempt.task_id.clone(),
            attempt_id: attempt.id.clone(), generation: attempt.generation,
            worker_id: attempt.worker_id.clone(), incarnation: attempt.worker_incarnation.clone(),
            observation: Observation::NativeOutputCapped {
                request_index: 1, undispatched_calls: 1,
                request_binding: binding(b"{}"), output_binding,
            },
        }};
        let before_cap = get(&c, &task_input.id).await;
        let cap = exchange(&c, cap_operation.clone()).await;
        if wrong_sha {
            assert_eq!(code(cap.outcome.clone()), "source_mismatch");
            assert!(cap.commit.is_some());
            assert_eq!(get(&c, &task_input.id).await, before_cap);
            let replay = exchange(&c, reply_operation).await;
            assert_eq!(replay.outcome, original_reply.outcome);
            assert_eq!(replay.commit, original_reply.commit);
            continue;
        }
        assert!(matches!(&cap.outcome, Outcome::Ok { data: Data::Task(_) }), "{cap:?}");
        assert!(cap.commit.as_ref().unwrap().controller_revision
            > original_reply.commit.as_ref().unwrap().controller_revision);
        let bogus = observe(&c, &caller, "cap-close-bogus-result", CharterObservation::CompleteTurn {
            turn_id: boundary.turn_id.clone(),
            results: vec![CharterResultReceipt::Collaboration {
                call_id: "partial-provider-call".into(),
                operation_id: ids::call_operation_id(&attempt.id, 1, 1),
            }],
        }).await;
        assert_eq!(code(bogus.outcome), "reconciliation_required");
        assert_eq!(turn(&c, &boundary.turn_id).await, unclosed);
        let close_operation = Operation::ObserveCharter {
            operation_id: "cap-close-original".into(), caller: caller.clone(),
            observation: CharterObservation::CompleteTurn {
                turn_id: boundary.turn_id.clone(), results: vec![],
            },
        };
        let closed = exchange(&c, close_operation.clone()).await;
        let CharterOutcome::Turn(saved) = outcome(&closed) else { panic!() };
        assert_eq!(saved.reply, Some(original_record));
        assert_eq!(saved.completed_by.as_deref(), Some("cap-close-original"));
        assert!(saved.delivered_results.is_empty());
        assert_eq!(saved.reply.as_ref().unwrap().observation.output, Some(original_items));
        assert_eq!(saved.reply.as_ref().unwrap().observation.tool_call_ids, vec!["partial-provider-call"]);
        assert!(closed.commit.as_ref().unwrap().controller_revision > cap.commit.as_ref().unwrap().controller_revision);
        assert_eq!(code(call(&c, Operation::GetOperation {
            operation_id: ids::call_operation_id(&attempt.id, 1, 1),
        }).await), "not_found", "no tool result occurrence may be fabricated");
        assert_eq!(get(&c, &task_input.id).await.input, task_input.input);
        drop(c);
        controller_owner_released(&p).await;
        let c = Controller::open_with_config(&p, cfg).unwrap();
        assert_eq!(turn(&c, &boundary.turn_id).await, saved);
        for (operation, original) in [(reply_operation, original_reply), (cap_operation, cap), (close_operation, closed)] {
            let replay = exchange(&c, operation).await;
            assert_eq!(replay.outcome, original.outcome);
            assert_eq!(replay.commit, original.commit);
        }
        assert_eq!(get(&c, &task_input.id).await.input, task_input.input);
    }
}
