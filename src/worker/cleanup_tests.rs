use super::*;

fn area(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "cleanup-policy-{label}-{}-{}-{}",
        std::process::id(),
        process::now_ms(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    root
}
fn config(limits: RuntimeLimits) -> WorkerConfig {
    WorkerConfig {
        limits: Arc::new(limits),
        resource_profile: None,
        controller_url: "http://127.0.0.1:1".into(),
        id: "worker".into(),
        incarnation: "new".into(),
        workspace_root: area("material"),
        codex_path: "/usr/bin/false".into(),
        coding_capacity: 1,
        native_capacity: 0,
        native_endpoint: "http://127.0.0.1:1/v1/responses".into(),
        validation_executable: std::env::current_exe().unwrap(),
    }
}
#[test]
fn cleanup_metadata_is_explicit_historical_absence_stays_absent() {
    let seed = RuntimeLimits::seed().unwrap();
    let policy = seed.retention.as_ref().unwrap();
    assert_eq!(policy.worker.success_grace_ms, 0);
    assert_eq!(policy.worker.failed_grace_ms, 86_400_000);
    assert_eq!(policy.worker.cancelled_grace_ms, 86_400_000);
    assert_eq!(policy.worker.idle_cache_grace_ms, 604_800_000);
    let mut old = serde_json::to_value(&seed).unwrap();
    old.as_object_mut().unwrap().remove("retention");
    let old_bytes = serde_json::to_vec(&old).unwrap();
    let decoded = RuntimeLimits::parse(&old_bytes).unwrap();
    assert!(decoded.retention.is_none());
    assert_eq!(serde_json::to_value(&decoded).unwrap(), old);
    for field in [
        "batch_entries",
        "filesystem_entries_per_yield",
        "operation_timeout_ms",
        "retry_delay_ms",
        "record_bytes",
    ] {
        let mut invalid = serde_json::to_value(&seed).unwrap();
        invalid["retention"]["worker"][field] = json!(0);
        assert!(
            RuntimeLimits::parse(&serde_json::to_vec(&invalid).unwrap())
                .unwrap_err()
                .contains(field)
        );
    }
    let mut invalid = serde_json::to_value(&seed).unwrap();
    invalid["retention"]["worker"]["unknown"] = json!(1);
    assert!(RuntimeLimits::parse(&serde_json::to_vec(&invalid).unwrap()).is_err());
}
#[test]
fn cleanup_current_policy_is_separate_from_unchanged_retained_execution() {
    let mut old = RuntimeLimits::seed().unwrap();
    old.retention = None;
    old.revision = "historical".into();
    let old = Arc::new(old);
    let state = area("retained");
    atomic_json(&state.join("limits.json"), &*old).unwrap();
    let before = std::fs::read(state.join("limits.json")).unwrap();
    let mut selected = (*old).clone();
    selected.revision = "retention-enabled".into();
    selected.retention = RuntimeLimits::seed().unwrap().retention;
    let retained = retained_worker_limits(&state, &Arc::new(selected.clone())).unwrap();
    assert_eq!(retained, old);
    assert!(same_execution_policy(&retained, &selected));
    let cfg = config((*old).clone());
    let enabled = Scheduler::open_selected(&cfg, &state, &selected).unwrap();
    assert!(enabled.deadline().is_some());
    let disabled = Scheduler::open_selected(&cfg, &state, &old).unwrap();
    assert_eq!(disabled.deadline(), None);
    assert_eq!(std::fs::read(state.join("limits.json")).unwrap(), before);
    selected.worker.git_output_bytes += 1;
    assert!(!same_execution_policy(&retained, &selected));
    let mut revision_only = (*old).clone();
    revision_only.revision = "unrelated".into();
    assert!(!same_execution_policy(&old, &revision_only));
}
#[test]
fn cleanup_grace_boundary_backward_clock_and_persisted_due_are_conservative() {
    let limits = RuntimeLimits::seed().unwrap();
    let policy = limits.retention.unwrap().worker;
    let safe = 1_000u64;
    for status in [TaskStatus::Failed, TaskStatus::Cancelled] {
        let at = add_ms(safe, grace(&policy, &status).unwrap()).unwrap();
        assert_eq!(at, 86_401_000);
        assert!(at - 1 < at);
        assert!(at >= safe);
    }
    assert_eq!(grace(&policy, &TaskStatus::Succeeded).unwrap(), 0);
    assert!(grace(&policy, &TaskStatus::Active).is_err());
    assert!(add_ms(u64::MAX, 1).is_err());
    let record = Record {
        format_revision: 1,
        command_id: "c".into(),
        attempt_id: "a".into(),
        policy: policy.clone(),
        policy_revision: "r".into(),
        status: Some(TaskStatus::Failed),
        checkpoint: None,
        first_safe_ms: Some(safe),
        last_observed_ms: safe,
        next_due_ms: Some(safe + policy.failed_grace_ms),
        disposition: Disposition::Retained,
        reason: Reason::Grace,
        worktrees: std::array::from_fn(|_| Slot {
            proof: None,
            started: false,
            done: false,
        }),
        cache: None,
        cache_started: false,
        cache_safe_ms: None,
        cache_done: false,
    };
    let path = area("record").join("record.json");
    save(&path, &record).unwrap();
    let mut reopened: Record = read_record(&path, policy.record_bytes).unwrap().unwrap();
    assert_eq!(reopened.next_due_ms, Some(86_401_000));
    retain(&mut reopened, Reason::Clock, safe - 1).unwrap();
    assert_eq!(reopened.first_safe_ms, Some(safe));
    assert_eq!(reopened.last_observed_ms, safe);
    retain(&mut reopened, Reason::UncertainEffect, safe).unwrap();
    assert_eq!(reopened.first_safe_ms, None);
}
#[test]
fn cleanup_unrelated_pending_report_does_not_pin_safe_attempt_but_unknown_does() {
    let state = area("outbox");
    process::private_directory(&state.join("outbox")).unwrap();
    let limits = RuntimeLimits::seed().unwrap();
    let a = Attempt {
        id: "attempt-a".into(),
        task_id: "task-a".into(),
        session_id: "session-a".into(),
        generation: 1,
        worker_id: "worker".into(),
        worker_incarnation: "old".into(),
        phase: AttemptPhase::Stopped,
        deadline_unix_ms: 1,
        stopped: true,
        uncertain: false,
    };
    let report = Report {
        report_id: "pending-b".into(),
        task_id: "task-b".into(),
        attempt_id: "attempt-b".into(),
        worker_id: "worker".into(),
        incarnation: "old".into(),
        generation: 1,
        observation: Observation::Progress {
            text: "pending unrelated".into(),
            output: String::new(),
            truncated: false,
        },
    };
    atomic_json(&state.join("outbox/b.json"), &report).unwrap();
    assert!(!attempt_outbox(&state, &a, &limits).unwrap());
    let mut report = report;
    report.task_id = a.task_id.clone();
    atomic_json(&state.join("outbox/b.json"), &report).unwrap();
    assert!(attempt_outbox(&state, &a, &limits).unwrap());
    std::fs::write(state.join("outbox/b.json"), b"{").unwrap();
    assert!(attempt_outbox(&state, &a, &limits).is_err());
}

#[test]
fn cleanup_batch_one_due_retry_cannot_starve_later_inventory() {
    let limits = RuntimeLimits::seed().unwrap();
    let cfg = config(limits);
    let state = area("fair");
    process::private_directory(&state.join("commands")).unwrap();
    for id in ["a", "b", "c"] {
        std::fs::write(state.join("commands").join(format!("{id}.json")), b"{}").unwrap();
    }
    let mut scheduler = Scheduler::open(&cfg, &state).unwrap();
    scheduler.scan = Some(std::fs::read_dir(state.join("commands")).unwrap());
    scheduler.rescan = false;
    let retry = state.join("always-due.json");
    let mut found = BTreeSet::new();
    for _ in 0..8 {
        scheduler.due.insert(retry.clone(), 0);
        if let Some(path) = scheduler.next_path(1, 10, 100) {
            if path != retry {
                found.insert(path.file_name().unwrap().to_owned());
            }
        }
    }
    assert_eq!(found.len(), 3);
    assert!(found.contains(std::ffi::OsStr::new("c.json")));
    scheduler.scan = None;
    scheduler.due.clear();
    let first = state.join("a-retry.json");
    let second = state.join("z-retry.json");
    scheduler.due.insert(first.clone(), 1);
    scheduler.due.insert(second.clone(), 2);
    assert_eq!(scheduler.next_path(10, 10, 100), Some(first.clone()));
    scheduler.due.insert(first, 9);
    assert_eq!(scheduler.next_path(10, 10, 100), Some(second));
}

#[test]
fn cleanup_process_home_is_not_an_obligation_but_records_and_unknown_entries_are() {
    let registry = area("process-obligations");
    process::private_directory(&registry.join("home/code")).unwrap();
    assert!(!process_obligation(&registry).unwrap());
    std::fs::write(registry.join("123.json"), b"malformed record stays held").unwrap();
    assert!(process_obligation(&registry).unwrap());
    std::fs::remove_file(registry.join("123.json")).unwrap();
    std::fs::write(registry.join("unknown"), b"unknown association stays held").unwrap();
    assert!(process_obligation(&registry).unwrap());
    std::fs::remove_file(registry.join("unknown")).unwrap();
    std::fs::rename(registry.join("home"), registry.join("retained-home")).unwrap();
    std::fs::rename(
        registry.join("retained-home"),
        registry.with_extension("home"),
    )
    .unwrap();
    std::os::unix::fs::symlink(registry.with_extension("home"), registry.join("home")).unwrap();
    assert!(process_obligation(&registry).is_err());
    assert!(registry.with_extension("home").join("code").is_dir());
}
