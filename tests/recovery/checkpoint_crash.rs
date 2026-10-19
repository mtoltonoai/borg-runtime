use super::*;
use borg_runtime::worker::process::component;
fn worker_state(lab: &Lab) -> PathBuf {
    lab.root
        .join("worker-a/.borg-worker")
        .join(component("worker-a"))
}
fn start_cut(lab: &mut Lab, position: &str) -> usize {
    start_cut_inc(lab, position, "one")
}
fn start_cut_inc(lab: &mut Lab, position: &str, incarnation: &str) -> usize {
    let origin = lab.origin.clone();
    let workspace = lab.root.join("worker-a");
    let fixture = lab.fixture.clone();
    let worker = lab.spawn_env(
        format!("worker-a-cut-{incarnation}"),
        &[
            "worker",
            "--controller",
            &origin,
            "--id",
            "worker-a",
            "--incarnation",
            incarnation,
            "--workspace-root",
            workspace.to_str().unwrap(),
            "--codex",
            fixture.to_str().unwrap(),
            "--coding-capacity",
            "1",
            "--native-capacity",
            "0",
        ],
        &[("BORG_QUALIFICATION_CHECKPOINT_CUT", position)],
        "limits.json",
    );
    lab.workers.push(worker);
    lab.workers.len() - 1
}
async fn cut(lab: &Lab) -> Value {
    let path = worker_state(lab).join("checkpoint-qualification.json");
    let end = Instant::now() + TIMEOUT;
    loop {
        if path.exists() {
            return serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        }
        assert!(
            Instant::now() < end,
            "crash cut not reached: {}",
            lab.root.display()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
#[tokio::test]
async fn checkpoint_crash_after_publication_recovers_exact_original_without_executor_rerun() {
    let mut lab = Lab::new("checkpoint-before-settlement-crash").await;
    let submitted = lab.submit("submit-crash", "agent-crash").await;
    let worker = start_cut(&mut lab, "after_publish");
    lab.gate().await.release().await;
    let reached = cut(&lab).await;
    let checkpoint: Checkpoint = serde_json::from_value(reached["checkpoint"].clone()).unwrap();
    let attempt: Attempt = serde_json::from_value(reached["attempt"].clone()).unwrap();
    assert_eq!(
        git(
            &lab.root,
            &[
                "--git-dir",
                &checkpoint.repository,
                "rev-parse",
                &checkpoint.remote_ref
            ]
        ),
        checkpoint.commit
    );
    let settlement = worker_state(&lab)
        .join("mutations/settlements")
        .join(format!("{}.json", component(&checkpoint.operation_id)));
    assert!(
        !settlement.exists(),
        "cut must precede the first settlement journal"
    );
    lab.workers[worker].kill();
    let origin = lab.origin.clone();
    lab.start_worker("worker-a", "two", &origin);
    let stopped = lab
        .until(&submitted.id, "recovered original attempt stopped", |t| {
            t.status == TaskStatus::Failed && t.attempt.as_ref().is_some_and(|a| a.stopped)
        })
        .await;
    let original = lab
        .client
        .call(Operation::GetOperation {
            operation_id: checkpoint.operation_id.clone(),
        })
        .await
        .unwrap();
    save(
        lab.root.join("crash-recovery-proof.json"),
        &json!({"cut":reached,"original_operation":original,"recovered_task":stopped,"scope":"debug-only bounded cut after actual local Git publication; real SIGKILL and fresh worker; not release proof"}),
    );
    let starts = lab.starts.len();
    assert_eq!(stopped.attempt.as_ref().unwrap().id, attempt.id);
    assert_eq!(
        stopped.selected_checkpoint,
        Some(checkpoint),
        "known-completed publication must be reconciled on restart"
    );
    assert!(!stopped.attempt.as_ref().unwrap().uncertain);
    assert_eq!(starts, 1, "no repeated supplied-executor turn");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), lab.gate.accept())
            .await
            .is_err(),
        "no unobserved second executor connection"
    );
    assert_eq!(stopped.input.limits, submitted.input.limits);
    lab.restart_controller().await;
    assert_eq!(lab.get(&submitted.id).await, stopped);
    lab.close();
}

async fn prepared_loss(label: &str, position: &str) -> (Lab, Task, Checkpoint, Attempt) {
    let mut lab = Lab::new(label).await;
    let submitted = lab.submit("submit-crash", "agent-crash").await;
    let worker = start_cut(&mut lab, position);
    lab.gate().await.release().await;
    let reached = cut(&lab).await;
    let checkpoint: Checkpoint = serde_json::from_value(reached["checkpoint"].clone()).unwrap();
    let attempt: Attempt = serde_json::from_value(reached["attempt"].clone()).unwrap();
    lab.workers[worker].kill();
    (lab, submitted, checkpoint, attempt)
}
fn phase_path(lab: &Lab, attempt: &Attempt) -> PathBuf {
    worker_state(lab)
        .join("publications")
        .join(format!("{}.json", component(&attempt.id)))
}
#[tokio::test]
async fn checkpoint_crash_before_publish_settles_verified_absence_without_pushing() {
    let (mut lab, submitted, checkpoint, attempt) =
        prepared_loss("checkpoint-absent-crash", "before_publish").await;
    let original_phase = fs::read(phase_path(&lab, &attempt)).unwrap();
    // Missing local workspace cannot be treated as preserved source. Readback
    // uses only the original remote identity from the acknowledged phase.
    fs::remove_dir_all(lab.root.join("worker-a/workspaces")).unwrap();
    let origin = lab.origin.clone();
    lab.start_worker("worker-a", "two", &origin);
    let stopped = lab
        .until(
            &submitted.id,
            "absent original publication reconciled",
            |t| t.status == TaskStatus::Failed && t.attempt.as_ref().is_some_and(|a| a.stopped),
        )
        .await;
    assert!(stopped.selected_checkpoint.is_none());
    assert!(!stopped.attempt.as_ref().unwrap().uncertain);
    assert_eq!(stopped.input.limits, submitted.input.limits);
    let original = lab
        .client
        .call(Operation::GetOperation {
            operation_id: checkpoint.operation_id.clone(),
        })
        .await
        .unwrap();
    assert!(
        matches!(&original.outcome,Outcome::Ok{data:Data::Operation(record)} if record.state=="settled"&&record.outcome==Some(json!({"acknowledged":true,"not_applied":true})))
    );
    assert_eq!(
        git(
            &lab.root,
            &[
                "ls-remote",
                "--refs",
                &checkpoint.repository,
                &checkpoint.remote_ref
            ]
        ),
        ""
    );
    assert_eq!(
        fs::read(phase_path(&lab, &attempt)).unwrap(),
        original_phase
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), lab.gate.accept())
            .await
            .is_err()
    );
    assert!(!lab.root.join("worker-a/workspaces").exists());
    save(
        lab.root.join("absence-recovery-proof.json"),
        &json!({"original":original,"task":stopped,"executor_starts":lab.starts,"scope":"actual absent remote after verified owner process cleanup; no new push or executor source path"}),
    );
    lab.close();
}
#[tokio::test]
async fn checkpoint_crash_preserves_independent_unknown_even_after_exact_settlement() {
    let (mut lab, submitted, checkpoint, attempt) =
        prepared_loss("checkpoint-independent-unknown", "after_publish").await;
    let reply = lab
        .client
        .call(Operation::ObserveWorker {
            report: Report {
                report_id: "independent-executor-unknown".into(),
                worker_id: attempt.worker_id.clone(),
                incarnation: attempt.worker_incarnation.clone(),
                task_id: attempt.task_id.clone(),
                attempt_id: attempt.id.clone(),
                generation: attempt.generation,
                observation: Observation::ExecutionUncertain {
                    message: "independently observed unknown executor effect".into(),
                },
            },
        })
        .await
        .unwrap();
    assert!(reply.commit.is_some());
    let origin = lab.origin.clone();
    lab.start_worker("worker-a", "two", &origin);
    let stopped = lab
        .until(
            &submitted.id,
            "known checkpoint settled while unrelated unknown retained",
            |t| {
                t.selected_checkpoint.as_ref() == Some(&checkpoint)
                    && t.attempt.as_ref().is_some_and(|a| a.stopped)
            },
        )
        .await;
    assert!(stopped.attempt.as_ref().unwrap().uncertain);
    assert_eq!(
        refused(
            lab.client
                .call(Operation::RetryTask {
                    operation_id: "retry-still-unknown".into(),
                    task_id: submitted.id.clone(),
                    expected_revision: stopped.revision
                })
                .await
                .unwrap()
        ),
        "reconciliation_required"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), lab.gate.accept())
            .await
            .is_err()
    );
    lab.close();
}
#[tokio::test]
async fn checkpoint_crash_conflicting_unavailable_or_untrusted_phase_never_settles() {
    for fault in [
        "different_ref",
        "unreadable_remote",
        "corrupt_phase",
        "foreign_phase",
        "legacy_phase",
        "missing_phase",
        "resource_refusal",
        "prepare_stamp",
    ] {
        let (mut lab, submitted, checkpoint, attempt) =
            prepared_loss(&format!("checkpoint-blocked-{fault}"), "after_publish").await;
        let file = phase_path(&lab, &attempt);
        match fault {
            "different_ref" => {
                git(
                    &lab.root,
                    &[
                        "--git-dir",
                        &checkpoint.repository,
                        "update-ref",
                        &checkpoint.remote_ref,
                        &lab.repository.start_commit,
                    ],
                );
            }
            "unreadable_remote" => {
                fs::rename(
                    &checkpoint.repository,
                    lab.root.join("remote-unavailable.git"),
                )
                .unwrap();
            }
            "corrupt_phase" => fs::write(&file, b"{broken").unwrap(),
            "foreign_phase" => {
                let mut value: Value = serde_json::from_slice(&fs::read(&file).unwrap()).unwrap();
                value["command"]["attempt"]["generation"] = json!(999);
                save(&file, &value);
            }
            "legacy_phase" => {
                let mut value: Value = serde_json::from_slice(&fs::read(&file).unwrap()).unwrap();
                value["revision"] = json!(0);
                save(&file, &value);
            }
            "missing_phase" => fs::remove_file(&file).unwrap(),
            "prepare_stamp" => {
                let mut value: Value = serde_json::from_slice(&fs::read(&file).unwrap()).unwrap();
                value["prepare_commit"]["controller_revision"] = json!(
                    value["prepare_commit"]["controller_revision"]
                        .as_u64()
                        .unwrap()
                        + 1
                );
                save(&file, &value);
            }
            "resource_refusal" => {}
            _ => unreachable!(),
        }
        let before = fs::read(&file).ok();
        let origin = lab.origin.clone();
        if fault == "resource_refusal" {
            let workspace = lab.root.join("worker-a");
            let fixture = lab.fixture.clone();
            let profile = lab.root.join("unavailable-profile");
            let process = lab.spawn(
                "worker-resource-refusal".into(),
                &[
                    "worker",
                    "--controller",
                    &origin,
                    "--id",
                    "worker-a",
                    "--incarnation",
                    "two",
                    "--workspace-root",
                    workspace.to_str().unwrap(),
                    "--codex",
                    fixture.to_str().unwrap(),
                    "--resource-profile",
                    profile.to_str().unwrap(),
                ],
            );
            lab.workers.push(process);
        } else {
            lab.start_worker("worker-a", "two", &origin);
        }
        if fault == "missing_phase" {
            lab.until(
                &submitted.id,
                "missing phase preserves generic unknown",
                |t| t.attempt.as_ref().is_some_and(|a| a.stopped && a.uncertain),
            )
            .await;
        } else {
            let (log, expected) = match fault {
                "resource_refusal" => (
                    "worker-resource-refusal.log",
                    "worker resource profile unavailable",
                ),
                "different_ref" => (
                    "worker-a-two.log",
                    "original checkpoint ref has different content",
                ),
                "unreadable_remote" => ("worker-a-two.log", "Git operation failed"),
                "corrupt_phase" => ("worker-a-two.log", "corrupt or legacy publication phase"),
                "prepare_stamp" => (
                    "worker-a-two.log",
                    "publication original prepare acknowledgment changed",
                ),
                _ => (
                    "worker-a-two.log",
                    "invalid original checkpoint publication phase",
                ),
            };
            wait_log(&lab, log, expected).await;
        }
        let current = lab.get(&submitted.id).await;
        assert!(current.selected_checkpoint.is_none(), "{fault}");
        assert_eq!(
            fs::read(&file).ok(),
            before,
            "phase not repaired on {fault}"
        );
        let reply = lab
            .client
            .call(Operation::GetOperation {
                operation_id: checkpoint.operation_id.clone(),
            })
            .await
            .unwrap();
        assert!(
            matches!(&reply.outcome,Outcome::Ok{data:Data::Operation(record)} if record.state=="pending"),
            "{fault}: {reply:?}"
        );
        assert!(
            !worker_state(&lab)
                .join("mutations/settlements")
                .join(format!("{}.json", component(&checkpoint.operation_id)))
                .exists()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), lab.gate.accept())
                .await
                .is_err(),
            "{fault}: no executor rerun"
        );
        save(
            lab.root.join("blocked-recovery-proof.json"),
            &json!({"fault":fault,"task":current,"original":reply,"phase_unchanged":true}),
        );
        lab.close();
    }
}
#[tokio::test]
async fn checkpoint_crash_settlement_ack_loss_replays_stable_original_decision() {
    let (mut lab, submitted, checkpoint, attempt) =
        prepared_loss("checkpoint-recovery-settlement-ack", "after_publish").await;
    let mut proxy = Proxy::new(
        lab.origin.clone(),
        Fault::DropSettlement,
        lab.root.clone(),
        lab.limits.transport.body_bytes,
    )
    .await;
    lab.start_worker("worker-a", "two", &proxy.url);
    let event = proxy.event().await;
    let operation = event.original.command;
    let expected = event.reply;
    let stopped = lab
        .until(&submitted.id, "recovered stable settlement accepted", |t| {
            t.status == TaskStatus::Failed && t.attempt.as_ref().is_some_and(|a| a.stopped)
        })
        .await;
    assert_eq!(stopped.selected_checkpoint, Some(checkpoint));
    assert!(!stopped.attempt.as_ref().unwrap().uncertain);
    assert!(
        matches!(&operation,Operation::SettleOperation{attempt_id,generation,..} if attempt_id==&attempt.id&&generation==&attempt.generation)
    );
    let replay = lab.client.call(operation).await.unwrap();
    assert_eq!(replay.commit, expected.commit);
    assert_eq!(replay.outcome, expected.outcome);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), lab.gate.accept())
            .await
            .is_err()
    );
    lab.close();
}

async fn wait_log(lab: &Lab, name: &str, expected: &str) {
    let end = Instant::now() + TIMEOUT;
    loop {
        if fs::read_to_string(lab.root.join(name))
            .unwrap_or_default()
            .contains(expected)
        {
            return;
        }
        assert!(
            Instant::now() < end,
            "expected reached recovery refusal {expected}: {}",
            lab.root.display()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
async fn wait_completed_receipt(lab: &Lab, attempt: &Attempt) -> Value {
    let end = Instant::now() + TIMEOUT;
    loop {
        for entry in fs::read_dir(worker_state(lab).join("commands")).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let value: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
            if value["command"]["attempt"]["id"] == attempt.id && value["completed"] == true {
                return value;
            }
        }
        assert!(
            Instant::now() < end,
            "local publication receipt must finish: {}",
            lab.root.display()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
#[tokio::test]
async fn checkpoint_crash_cancelled_or_expired_task_closes_recorded_terminal_refusal() {
    for expired in [false, true] {
        let mut lab = Lab::new(if expired {
            "checkpoint-expired"
        } else {
            "checkpoint-cancelled"
        })
        .await;
        let mut input = lab.input("agent");
        if expired {
            input.limits.deadline_unix_ms = now() + 5000;
        }
        let submitted = task(
            lab.client
                .call(Operation::SubmitTask {
                    operation_id: "submit-terminal".into(),
                    input: (input).into(),
                })
                .await
                .unwrap(),
        );
        let worker = start_cut(&mut lab, "after_publish");
        lab.gate().await.release().await;
        let reached = cut(&lab).await;
        let attempt: Attempt = serde_json::from_value(reached["attempt"].clone()).unwrap();
        let checkpoint: Checkpoint = serde_json::from_value(reached["checkpoint"].clone()).unwrap();
        lab.workers[worker].kill();
        if expired {
            tokio::time::sleep(Duration::from_millis(
                submitted
                    .input
                    .limits
                    .deadline_unix_ms
                    .saturating_sub(now())
                    + 20,
            ))
            .await;
        } else {
            let current = lab.get(&submitted.id).await;
            task(
                lab.client
                    .call(Operation::CancelTask {
                        operation_id: "cancel-completed-publication".into(),
                        task_id: current.id,
                        expected_revision: current.revision,
                    })
                    .await
                    .unwrap(),
            );
        }
        let origin = lab.origin.clone();
        let replacement = lab.start_worker("worker-a", "two", &origin);
        let receipt = wait_completed_receipt(&lab, &attempt).await;
        let current = lab.get(&submitted.id).await;
        assert!(matches!(
            current.status,
            TaskStatus::Failed | TaskStatus::Cancelled
        ));
        assert_eq!(current.selected_checkpoint, Some(checkpoint));
        assert!(current.attempt.as_ref().unwrap().stopped);
        assert!(!current.attempt.as_ref().unwrap().uncertain);
        assert_eq!(current.input.limits, submitted.input.limits);
        let result_id = format!("{}-publication-recovery-result", attempt.id);
        let refusal = lab
            .client
            .call(Operation::GetOperation {
                operation_id: result_id,
            })
            .await
            .unwrap();
        assert!(refusal.commit.is_some());
        assert!(
            matches!(&refusal.outcome,Outcome::Ok{data:Data::Operation(record)} if serde_json::from_value::<Outcome>(record.outcome.clone().unwrap()).is_ok_and(|o|matches!(o,Outcome::Refused{..})))
        );
        lab.workers[replacement].kill();
        lab.restart_controller().await;
        lab.start_worker("worker-a", "three", &origin);
        assert_eq!(wait_completed_receipt(&lab, &attempt).await, receipt);
        assert_eq!(lab.get(&submitted.id).await, current);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), lab.gate.accept())
                .await
                .is_err()
        );
        save(
            lab.root.join("terminal-refusal-proof.json"),
            &json!({"expired":expired,"original_refusal":refusal,"task":current,"receipt":receipt}),
        );
        lab.close();
    }
}
#[tokio::test]
async fn checkpoint_crash_historical_terminal_receipts_do_not_block_or_modify_successor() {
    let (mut lab, submitted, _checkpoint, attempt) =
        prepared_loss("checkpoint-superseded-completion", "after_publish").await;
    fs::remove_file(worker_state(&lab).join("checkpoint-qualification.json")).unwrap();
    let replacement = start_cut_inc(&mut lab, "recovered_terminal", "two");
    let reached = cut(&lab).await;
    assert_eq!(reached["position"], "recovered_terminal");
    lab.workers[replacement].kill();
    let current = lab.get(&submitted.id).await;
    assert_eq!(current.status, TaskStatus::Failed);
    assert!(current.attempt.as_ref().unwrap().stopped);
    task(
        lab.client
            .call(Operation::RetryTask {
                operation_id: "explicit-successor-after-terminal".into(),
                task_id: current.id,
                expected_revision: current.revision,
            })
            .await
            .unwrap(),
    );
    let origin = lab.origin.clone();
    lab.start_worker("worker-b", "one", &origin);
    let gate = lab.gate().await;
    assert_eq!(gate.observed["before"], "after\n");
    gate.release().await;
    let accepted = lab.successful(&submitted.id).await;
    assert_ne!(accepted.attempt.as_ref().unwrap().id, attempt.id);
    lab.start_worker("worker-a", "three", &origin);
    wait_completed_receipt(&lab, &attempt).await;
    assert_eq!(
        lab.get(&submitted.id).await,
        accepted,
        "historical replay cannot modify successor task or selected checkpoint"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), lab.gate.accept())
            .await
            .is_err()
    );
    lab.close();
}

#[tokio::test]
async fn checkpoint_crash_empty_ref_after_possible_send_stays_unresolved_without_blind_push() {
    let (mut lab, submitted, checkpoint, attempt) =
        prepared_loss("checkpoint-may-send-empty", "after_publish").await;
    let original_phase = fs::read(phase_path(&lab, &attempt)).unwrap();
    let phase: Value = serde_json::from_slice(&original_phase).unwrap();
    assert_eq!(phase["publish_may_have_started"], true);
    git(
        &lab.root,
        &[
            "--git-dir",
            &checkpoint.repository,
            "update-ref",
            "-d",
            &checkpoint.remote_ref,
        ],
    );
    let origin = lab.origin.clone();
    lab.start_worker("worker-a", "two", &origin);
    wait_log(
        &lab,
        "worker-a-two.log",
        "empty readback cannot settle a possibly sent publication",
    )
    .await;
    let original = lab
        .client
        .call(Operation::GetOperation {
            operation_id: checkpoint.operation_id.clone(),
        })
        .await
        .unwrap();
    assert!(
        matches!(&original.outcome,Outcome::Ok{data:Data::Operation(record)} if record.state=="pending")
    );
    assert!(
        !worker_state(&lab)
            .join("mutations/settlements")
            .join(format!("{}.json", component(&checkpoint.operation_id)))
            .exists()
    );
    assert_eq!(
        git(
            &lab.root,
            &[
                "ls-remote",
                "--refs",
                &checkpoint.repository,
                &checkpoint.remote_ref
            ]
        ),
        ""
    );
    assert_eq!(
        fs::read(phase_path(&lab, &attempt)).unwrap(),
        original_phase
    );
    let current = lab.get(&submitted.id).await;
    assert!(current.selected_checkpoint.is_none());
    let cancelled = task(
        lab.client
            .call(Operation::CancelTask {
                operation_id: "cancel-maybe-sent".into(),
                task_id: current.id,
                expected_revision: current.revision,
            })
            .await
            .unwrap(),
    );
    assert_eq!(
        refused(
            lab.client
                .call(Operation::RetryTask {
                    operation_id: "retry-unsettled-maybe-sent".into(),
                    task_id: cancelled.id,
                    expected_revision: cancelled.revision
                })
                .await
                .unwrap()
        ),
        "reconciliation_required"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), lab.gate.accept())
            .await
            .is_err()
    );
    save(
        lab.root.join("possibly-sent-absence-proof.json"),
        &json!({"original":original,"phase":phase,"selected_checkpoint":null,"retry_refused":true,"scope":"actual local empty readback demonstrates conservative branch; does not simulate remote receiver termination"}),
    );
    lab.close();
}
