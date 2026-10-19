//! Real local Git publication failure. This is distinct from controller ACK
//! loss and does not simulate or claim lost Git-side acknowledgments.
use super::*;
use borg_runtime::worker::{
    git as worker_git,
    process::{self, Control},
};

async fn operation(lab: &Lab, id: &str) -> OperationRecord {
    match lab
        .client
        .call(Operation::GetOperation {
            operation_id: id.into(),
        })
        .await
        .unwrap()
        .outcome
    {
        Outcome::Ok {
            data: Data::Operation(record),
        } => record,
        other => panic!("expected original operation: {other:?}"),
    }
}
async fn original_failure_report(lab: &Lab) -> Report {
    let outbox = lab
        .root
        .join("worker-a/.borg-worker")
        .join(process::component("worker-a"))
        .join("outbox");
    let end = Instant::now() + TIMEOUT;
    loop {
        for entry in fs::read_dir(&outbox).unwrap().flatten() {
            // Pending .json exists before send and can be renamed while read.
            // .refused is retained only after the worker verified the exact
            // committed refusal through its normal receipt lookup.
            if entry.path().extension().and_then(|s| s.to_str()) != Some("refused")
                || !entry.file_type().unwrap().is_file()
            {
                continue;
            }
            let Ok(report) = serde_json::from_slice::<Report>(&fs::read(entry.path()).unwrap())
            else {
                continue;
            };
            if matches!(&report.observation,Observation::ResultCandidate{summary,checks_passed:false,..} if summary=="checkpoint publication outcome uncertain; reconcile the original ref before retry")
            {
                return report;
            }
        }
        assert!(
            Instant::now() < end,
            "Git failure report was not retained: {}",
            lab.root.display()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_git_ref_lock_failure_requires_original_readback_before_retry() {
    let mut lab = Lab::new("git-ref-lock-publication").await;
    let submitted = lab
        .submit("git-locked-publication", "git-locked-agent")
        .await;
    let origin = lab.origin.clone();
    let worker = lab.start_worker("worker-a", "inc-one", &origin);
    let gate = lab.gate().await;
    let workspace = PathBuf::from(gate.observed["cwd"].as_str().unwrap());
    let assigned = lab.get(&submitted.id).await;
    let attempt = assigned.attempt.clone().unwrap();
    let expected = worker_git::checkpoint(&lab.repository, &submitted.id, &attempt.id, "".into()).unwrap();
    // Only the exact intended ref is blocked. The actual remote remains a
    // readable real repository, so the pre-push read finds no existing ref.
    let lock = PathBuf::from(&expected.repository).join(format!("{}.lock", expected.remote_ref));
    fs::create_dir_all(lock.parent().unwrap()).unwrap();
    let mut owned = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lock)
        .unwrap();
    use std::io::Write;
    owned
        .write_all(b"owned by the publication failure fixture\n")
        .unwrap();
    owned.sync_all().unwrap();
    drop(owned);
    let lock_bytes = fs::read(&lock).unwrap();
    gate.release().await;
    let stopped = lab
        .until(&submitted.id, "actual Git refusal and local cleanup", |t| {
            t.attempt.as_ref().is_some_and(|a| a.stopped && a.uncertain)
        })
        .await;
    assert!(stopped.result.is_none());
    assert!(stopped.selected_checkpoint.is_none());
    assert_eq!(
        stopped.status,
        TaskStatus::Active,
        "a known completed executor must not acquire generic execution uncertainty from its tracked Git effect"
    );
    let failure = original_failure_report(&lab).await;
    let report_receipt = operation(&lab, &failure.report_id).await;
    assert!(
        matches!(serde_json::from_value::<Outcome>(report_receipt.outcome.clone().unwrap()).unwrap(),Outcome::Refused{code,..} if code=="reconciliation_required")
    );
    let original = operation(&lab, &expected.operation_id).await;
    assert_eq!(original.kind, "checkpoint");
    assert_ne!(original.state, "settled");
    assert_eq!(original.task_id, submitted.id);
    assert_eq!(original.attempt_id, attempt.id);
    assert_eq!(original.generation, attempt.generation);
    let checkpoint = Checkpoint {
        registration: serde_json::from_value(original.intended["registration"].clone()).unwrap(),
        operation_id: original.id.clone(),
        repository: original.intended["repository"].as_str().unwrap().into(),
        commit: original.intended["commit"].as_str().unwrap().into(),
        remote_ref: original.intended["remote_ref"].as_str().unwrap().into(),
    };
    assert_eq!(checkpoint.registration, expected.registration);
    assert_eq!(checkpoint.repository, expected.repository);
    assert_eq!(checkpoint.remote_ref, expected.remote_ref);
    assert_ne!(checkpoint.commit, lab.repository.start_commit);
    assert_eq!(fs::read(&lock).unwrap(), lock_bytes);
    // Stop the actual owner before inspecting or settling its failed external
    // effect. No helper or test success flag substitutes for its termination.
    lab.workers[worker].stop();
    lab.restart_controller().await;
    assert_eq!(operation(&lab, &checkpoint.operation_id).await, original);
    let current = lab.get(&submitted.id).await;
    assert!(current.attempt.as_ref().unwrap().uncertain);
    assert!(current.selected_checkpoint.is_none());
    let cancelled = task(
        lab.client
            .call(Operation::CancelTask {
                operation_id: "cancel-unsettled-git".into(),
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
                    operation_id: "retry-before-git-readback".into(),
                    task_id: cancelled.id.clone(),
                    expected_revision: cancelled.revision
                })
                .await
                .unwrap()
        ),
        "reconciliation_required"
    );
    assert_eq!(lab.starts.len(), 1);
    assert_eq!(fs::read(&lock).unwrap(), lock_bytes);
    fs::remove_file(&lock).unwrap();
    let (_cancel, receiver) = tokio::sync::watch::channel(false);
    let runtime_limits = accepted_limits(&assigned);
    let worker_limits: Arc<borg_runtime::limits::WorkerLimits> = Arc::from(lab.limits.worker.clone());
    let control = Control {
        runtime_limits,
        worker_limits: worker_limits.clone(),
        validation_runtime: Arc::new(
            borg_runtime::native::ValidationRuntime::new(worker_limits).unwrap(),
        ),
        deadline_ms: now() + 30_000,
        cancel: receiver,
    };
    let registry = lab.root.join("original-git-readback");
    let actual = worker_git::remote_commit(&workspace, &checkpoint, &registry, &control)
        .await
        .unwrap();
    assert_eq!(
        actual, None,
        "must observe the original ref before not-applied settlement"
    );
    assert_eq!(
        lab.starts.len(),
        1,
        "readback must not repeat coding execution"
    );
    let settle = Operation::SettleOperation {
        settlement_id: "git-original-not-applied".into(),
        operation_id: checkpoint.operation_id.clone(),
        task_id: submitted.id.clone(),
        attempt_id: attempt.id.clone(),
        generation: attempt.generation,
        outcome: json!({"acknowledged":true,"not_applied":true}),
        checkpoint: None,
    };
    let settled = lab.client.call(settle.clone()).await.unwrap();
    assert!(settled.commit.is_some());
    assert!(
        matches!(&settled.outcome,Outcome::Ok{data:Data::Operation(record)} if record.state=="settled"&&record.intended==original.intended)
    );
    let replay = lab.client.call(settle.clone()).await.unwrap();
    assert_eq!(replay.commit, settled.commit);
    assert_eq!(replay.outcome, settled.outcome);
    let current = lab.get(&submitted.id).await;
    assert!(!current.attempt.as_ref().unwrap().uncertain);
    assert!(current.selected_checkpoint.is_none());
    lab.restart_controller().await;
    let current = lab.get(&submitted.id).await;
    task(
        lab.client
            .call(Operation::RetryTask {
                operation_id: "retry-after-exact-git-readback".into(),
                task_id: current.id,
                expected_revision: current.revision,
            })
            .await
            .unwrap(),
    );
    lab.start_worker("worker-a", "inc-two", &origin);
    let next = lab.gate().await;
    assert_eq!(
        next.observed["before"], "before\n",
        "failed unreferenced source must not become the selected checkpoint"
    );
    next.release().await;
    let accepted = lab.successful(&submitted.id).await;
    assert_ne!(accepted.attempt.as_ref().unwrap().id, attempt.id);
    assert_eq!(lab.starts.len(), 2);
    assert_ne!(
        accepted.selected_checkpoint.as_ref().unwrap().operation_id,
        checkpoint.operation_id
    );
    assert_eq!(
        worker_git::remote_commit(&workspace, &checkpoint, &registry, &control)
            .await
            .unwrap(),
        None
    );
    let replay = lab.client.call(settle).await.unwrap();
    assert_eq!(replay.commit, settled.commit);
    assert_eq!(replay.outcome, settled.outcome);
    assert_eq!(lab.get(&submitted.id).await, accepted);
    save(
        lab.root.join("git-publication-failure-proof.json"),
        &json!({"original":original,"actual_readback":actual,"settlement":settled,"accepted_successor":accepted,"scope":"actual local Git ref-lock refusal and original-ref readback; no lost Git-side ACK claim"}),
    );
    lab.close();
}
