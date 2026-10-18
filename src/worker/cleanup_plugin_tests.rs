//! Real canonical Controller and private filesystem effects; no supplied executor.
use super::*;
use std::os::unix::fs::symlink;

fn plugin_path(lab: &Lab) -> PathBuf {
    codex::state_path(&lab.state.join("codex"), &lab.attempt.id).join(".tmp/plugins")
}
fn progress_path(lab: &Lab) -> PathBuf {
    lab.state
        .join("cleanup-codex")
        .join(format!("{}.json", component(&lab.attempt.id)))
}
fn progress(lab: &Lab) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(progress_path(lab)).unwrap()).unwrap()
}
fn checkout_plugins(lab: &Lab) -> PathBuf {
    let path = plugin_path(lab);
    process::private_directory(&path.join(".git/objects/pack")).unwrap();
    std::fs::write(
        path.join(".git/objects/pack/reconstructible.pack"),
        b"fixture plugin pack",
    )
    .unwrap();
    std::fs::write(path.join("plugin.json"), b"fixture plugin content").unwrap();
    path
}
async fn only_plugins(lab: &Lab, now: u64) -> Result<Option<u64>, String> {
    let policy = &lab.config.limits.retention.as_ref().unwrap().worker;
    Box::pin(super::super::plugins::attempt_at(
        &lab.config,
        &lab.state,
        &lab.client,
        &BTreeMap::new(),
        &lab.control.validation_runtime,
        policy,
        &lab.config.limits.revision,
        &lab.command_path,
        now,
    ))
    .await
}
async fn workspace_only(lab: &Lab, now: u64) {
    let policy = &lab.config.limits.retention.as_ref().unwrap().worker;
    Box::pin(workspace_attempt_at(
        &lab.config,
        &lab.state,
        &lab.client,
        &BTreeMap::new(),
        &lab.control.validation_runtime,
        policy,
        &lab.config.limits.revision,
        &lab.command_path,
        now,
    ))
    .await
    .unwrap();
}

#[tokio::test]
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Cleanup MUST preserve durable task results, histories, checkpoints, audit metadata, operation receipts and recovery journals in the initial profile.
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Cleanup MUST reclaim the remaining unneeded contents of an eligible terminal supplied-executor attempt's private home under its configured retention policy.
async fn codex_home_terminal_success_grace_preserves_session_peer_and_canonical_records() {
    const GRACE: u64 = 1_000;
    let lab = Lab::with_retention("plugin-success", GRACE, 0, 604_800_000).await;
    let plugins = checkout_plugins(&lab);
    let home = plugins.parent().unwrap().parent().unwrap();
    for file in [
        "state_5.sqlite",
        "state_5.sqlite-wal",
        "state_5.sqlite-shm",
        "logs_2.sqlite",
    ] {
        std::fs::write(home.join(file), format!("fixture {file}")).unwrap();
    }
    for file in [
        "sessions/2026/session.jsonl",
        "archived_sessions/old.jsonl",
        "history.jsonl",
    ] {
        let p = home.join(file);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, format!("required transcript {file}")).unwrap();
    }
    let transcripts = [
        "sessions/2026/session.jsonl",
        "archived_sessions/old.jsonl",
        "history.jsonl",
    ]
    .map(|n| (n, std::fs::read(home.join(n)).unwrap()));
    std::fs::write(
        home.join("borg-usage.json"),
        br#"{"thread_id":"fixture-thread","observed_usage":{"input_tokens":1}}"#,
    )
    .unwrap();
    std::fs::write(
        home.join("borg-thread.json"),
        br#"{"thread_id":"fixture-thread","cwd":"fixture"}"#,
    )
    .unwrap();
    let borg_usage = std::fs::read(home.join("borg-usage.json")).unwrap();
    let peer = codex::state_path(&lab.state.join("codex"), "independent-live-peer");
    process::private_directory(&peer.join(".tmp/plugins")).unwrap();
    std::fs::write(peer.join(".tmp/plugins/keep"), b"other attempt").unwrap();
    let _scheduler = Scheduler::open(&lab.config, &lab.state).unwrap();
    let checkpoint = cache_success(&lab).await;
    let task = lab.task().await;
    let original = lab
        .call(Operation::GetOperation {
            operation_id: checkpoint.operation_id.clone(),
        })
        .await;
    let command = std::fs::read(&lab.command_path).unwrap();
    let journal = std::fs::read(publication::path(&lab.state, &lab.attempt)).unwrap();
    let peer_before = cache_fixture_files(&peer);
    let safe = process::now_ms();
    assert_eq!(cache_observe(&lab, safe).await, Some(safe + GRACE));
    assert_eq!(progress(&lab)["first_safe_ms"], safe);
    cache_observe(&lab, safe + GRACE - 1).await;
    assert!(plugins.exists());
    let cache_due = cache_observe(&lab, safe + GRACE).await.unwrap();
    assert!(
        cache_due > safe + GRACE,
        "shared Git-cache grace is independent"
    );
    assert!(!plugins.exists());
    assert_eq!(progress(&lab)["done"], true);
    assert!(cache_directory(&lab).exists());
    let archive = lab
        .state
        .join("codex-transcripts")
        .join(component(&lab.attempt.id));
    for (name, bytes) in transcripts {
        assert_eq!(std::fs::read(archive.join(name)).unwrap(), bytes);
        assert!(!home.join(name).exists());
    }
    assert_eq!(
        std::fs::read(home.join("borg-usage.json")).unwrap(),
        borg_usage
    );
    for name in [
        "state_5.sqlite",
        "state_5.sqlite-wal",
        "state_5.sqlite-shm",
        "logs_2.sqlite",
    ] {
        assert!(!home.join(name).exists());
    }
    assert_eq!(
        cache_fixture_files(home).len(),
        2,
        "only the original small Borg journal remains"
    );
    assert_eq!(cache_fixture_files(&peer), peer_before);
    assert_eq!(lab.task().await, task);
    let after = lab
        .call(Operation::GetOperation {
            operation_id: checkpoint.operation_id,
        })
        .await;
    assert_eq!(after.outcome, original.outcome);
    assert_eq!(after.commit, original.commit);
    assert_eq!(std::fs::read(&lab.command_path).unwrap(), command);
    assert_eq!(
        std::fs::read(publication::path(&lab.state, &lab.attempt)).unwrap(),
        journal
    );
    lab.close().await;
}

#[tokio::test]
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Automatic cleanup MUST use per-kind retention settings from worker metadata.
async fn codex_home_failed_and_cancelled_attempts_keep_exact_existing_grace() {
    const GRACE: u64 = 86_400_000;
    for cancelled in [false, true] {
        let lab = Lab::with_grace(
            if cancelled {
                "plugin-cancel"
            } else {
                "plugin-fail"
            },
            GRACE,
        )
        .await;
        let plugins = checkout_plugins(&lab);
        if cancelled {
            let task = lab.task().await;
            assert!(matches!(
                lab.call(Operation::CancelTask {
                    operation_id: "plugin-cancel".into(),
                    task_id: task.id,
                    expected_revision: task.revision
                })
                .await
                .outcome,
                Outcome::Ok { .. }
            ));
            lab.report("plugin-stop", Observation::ProcessTreeStopped)
                .await;
        } else {
            lab.report(
                "plugin-executor",
                Observation::ExecutorOutcome {
                    success: false,
                    summary: "controlled failure".into(),
                },
            )
            .await;
            lab.report("plugin-stop", Observation::ProcessTreeStopped)
                .await;
            lab.report(
                "plugin-result",
                Observation::ResultCandidate {
                    checks_passed: false,
                    summary: "controlled failure".into(),
                    checkpoint: None,
                    validation: vec![],
                    output: None,
                },
            )
            .await;
        }
        let _scheduler = Scheduler::open(&lab.config, &lab.state).unwrap();
        let safe = process::now_ms();
        assert_eq!(cache_observe(&lab, safe).await, Some(safe + GRACE));
        cache_observe(&lab, safe + GRACE - 1).await;
        assert!(plugins.exists());
        cache_observe(&lab, safe + GRACE).await;
        assert!(!plugins.exists());
        assert_eq!(progress(&lab)["done"], true);
        lab.close().await;
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Cleanup MUST preserve data required to reconcile an uncertain or unacknowledged operation.
async fn codex_home_holds_nonterminal_unacknowledged_uncertain_and_local_recovery() {
    for kind in [
        "nonterminal",
        "unacknowledged",
        "uncertain",
        "active",
        "unfinished",
        "process",
        "handoff",
        "outbox",
    ] {
        let lab = Lab::open(kind).await;
        let plugins = checkout_plugins(&lab);
        let _scheduler = Scheduler::open(&lab.config, &lab.state).unwrap();
        let mut active = BTreeMap::new();
        match kind {
            "nonterminal" => {
                lab.report("plugin-stop", Observation::ProcessTreeStopped)
                    .await;
            }
            "unacknowledged" => {
                lab.prepare_checkpoint().await;
                let task = lab.task().await;
                lab.call(Operation::CancelTask {
                    operation_id: "plugin-cancel".into(),
                    task_id: task.id,
                    expected_revision: task.revision,
                })
                .await;
                lab.report("plugin-stop", Observation::ProcessTreeStopped)
                    .await;
            }
            "uncertain" => {
                lab.report(
                    "plugin-uncertain",
                    Observation::ExecutionUncertain {
                        message: "controlled unresolved effect".into(),
                    },
                )
                .await;
                lab.report("plugin-stop", Observation::ProcessTreeStopped)
                    .await;
            }
            _ => {
                cache_success(&lab).await;
                // First establish an independently legitimate old reclaimed
                // record, then inject a new local hold. Its shortcut must not
                // authorize the new terminal home compaction.
                workspace_only(&lab, process::now_ms()).await;
                match kind {
                    "active" => {
                        let (cancel, _) = watch::channel(false);
                        active.insert(
                            lab.attempt.id.clone(),
                            Active {
                                task_id: lab.attempt.task_id.clone(),
                                generation: lab.attempt.generation,
                                executor: ExecutorKind::Coding,
                                cancel,
                                join: tokio::spawn(std::future::pending::<()>()),
                            },
                        );
                    }
                    "unfinished" => {
                        let mut receipt =
                            read_receipt(&lab.command_path, &lab.config.limits).unwrap();
                        receipt.completed = false;
                        atomic_json(&lab.command_path, &receipt).unwrap();
                    }
                    "process" => {
                        let directory =
                            lab.state.join("processes").join(component(&lab.attempt.id));
                        process::private_directory(&directory).unwrap();
                        std::fs::write(directory.join("unknown.json"), b"unresolved").unwrap();
                    }
                    "handoff" => {
                        let p = handoff::path(&lab.state, &lab.attempt);
                        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
                        std::fs::write(p, b"unresolved").unwrap();
                    }
                    "outbox" => {
                        std::fs::write(lab.state.join("outbox/unknown.json"), b"unresolved")
                            .unwrap();
                    }
                    _ => unreachable!(),
                }
            }
        }
        let before = cache_fixture_files(&plugins);
        let policy = &lab.config.limits.retention.as_ref().unwrap().worker;
        assert!(
            Box::pin(super::super::plugins::attempt_at(
                &lab.config,
                &lab.state,
                &lab.client,
                &active,
                &lab.control.validation_runtime,
                policy,
                &lab.config.limits.revision,
                &lab.command_path,
                process::now_ms()
            ))
            .await
            .unwrap()
            .is_some()
        );
        assert_eq!(cache_fixture_files(&plugins), before);
        assert_eq!(progress(&lab)["started"], false);
        assert_eq!(progress(&lab)["done"], false);
        for entry in active.into_values() {
            entry.join.abort();
            let _ = entry.join.await;
        }
        lab.close().await;
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Restart MUST reconcile incomplete cleanup before treating its resources as reclaimed.
async fn codex_home_old_reclaimed_record_is_unchanged_and_canonical_binding_is_rechecked() {
    for retry in [false, true] {
        let lab = Lab::with_retention(
            if retry {
                "plugin-old-retry"
            } else {
                "plugin-old-complete"
            },
            0,
            0,
            0,
        )
        .await;
        lab.report(
            "plugin-executor",
            Observation::ExecutorOutcome {
                success: false,
                summary: "controlled failure".into(),
            },
        )
        .await;
        lab.report("plugin-stop", Observation::ProcessTreeStopped)
            .await;
        lab.report(
            "plugin-result",
            Observation::ResultCandidate {
                checks_passed: false,
                summary: "controlled failure".into(),
                checkpoint: None,
                validation: vec![],
                output: None,
            },
        )
        .await;
        let _scheduler = Scheduler::open(&lab.config, &lab.state).unwrap();
        workspace_only(&lab, process::now_ms()).await;
        assert_eq!(lab.record().disposition, Disposition::Reclaimed);
        let old_bytes = std::fs::read(record_path(&lab.state, &lab.attempt.id)).unwrap();
        let plugins = checkout_plugins(&lab);
        if retry {
            let task = lab.task().await;
            assert!(matches!(
                lab.call(Operation::RetryTask {
                    operation_id: "plugin-retry".into(),
                    task_id: task.id,
                    expected_revision: task.revision
                })
                .await
                .outcome,
                Outcome::Ok { .. }
            ));
        }
        cache_observe(&lab, process::now_ms()).await;
        assert_eq!(plugins.exists(), retry);
        assert_eq!(
            std::fs::read(record_path(&lab.state, &lab.attempt.id)).unwrap(),
            old_bytes
        );
        // This is the unchanged strict legacy Record decoder, not a permissive
        // new wrapper. An old reader never opens cleanup-codex/ at all.
        let old: Record = serde_json::from_slice(&old_bytes).unwrap();
        assert_eq!(old.format_revision, 1);
        assert_eq!(old.disposition, Disposition::Reclaimed);
        let sidecar = std::fs::read(progress_path(&lab)).unwrap();
        workspace_only(&lab, process::now_ms()).await;
        assert_eq!(
            std::fs::read(progress_path(&lab)).unwrap(),
            sidecar,
            "the unchanged old writer leaves compaction progress intact on rollback"
        );
        if !retry {
            let mut unsupported: serde_json::Value = serde_json::from_slice(&sidecar).unwrap();
            unsupported["format_revision"] = json!(2);
            atomic_json(&progress_path(&lab), &unsupported).unwrap();
            assert!(only_plugins(&lab, process::now_ms()).await.is_err());
            assert_eq!(
                std::fs::read(record_path(&lab.state, &lab.attempt.id)).unwrap(),
                old_bytes
            );
        }
        lab.close().await;
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Restart MUST reconcile incomplete cleanup before treating its resources as reclaimed.
async fn codex_home_partial_removal_and_absent_after_intent_resume_without_replacing_identity() {
    for already_absent in [false, true] {
        let lab = Lab::open(if already_absent {
            "plugin-absent-restart"
        } else {
            "plugin-partial-restart"
        })
        .await;
        cache_success(&lab).await;
        let _scheduler = Scheduler::open(&lab.config, &lab.state).unwrap();
        workspace_only(&lab, process::now_ms()).await;
        let plugins = checkout_plugins(&lab);
        for n in 0..100 {
            std::fs::write(plugins.join(format!("payload-{n}")), b"reconstructible").unwrap();
        }
        let (send, receive) = tokio::sync::oneshot::channel();
        git::cleanup::tests::hook(git::cleanup::tests::Point::AfterUnlink, move |_| {
            let _ = send.send(());
        });
        // Like command arrival in the worker select loop: drop the future only
        // after the production remover performed a real unlink and yielded.
        tokio::select! {
            biased;
            result = receive => { result.unwrap(); }
            result = only_plugins(&lab, process::now_ms()) => { panic!("expected bounded interruption: {result:?}"); }
        }
        let pending = progress(&lab);
        assert_eq!(pending["started"], true);
        assert_eq!(pending["done"], false);
        assert!(plugins.exists());
        assert!(cache_fixture_files(&plugins).len() < 102);
        if already_absent {
            std::fs::remove_dir_all(&plugins).unwrap();
        }
        drop(_scheduler);
        let _reopened = Scheduler::open(&lab.config, &lab.state).unwrap();
        only_plugins(&lab, process::now_ms()).await.unwrap();
        assert!(!plugins.exists());
        assert_eq!(progress(&lab)["done"], true);
        assert_eq!(progress(&lab)["proof"], pending["proof"]);
        lab.close().await;
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Cleanup MUST preserve data required to reconcile an uncertain or unacknowledged operation.
async fn codex_home_symlink_replacement_foreign_tree_and_parent_redirection_are_refused() {
    for replace_parent in [false, true] {
        let lab = Lab::with_retention("plugin-path-proof", 1_000, 0, 604_800_000).await;
        cache_success(&lab).await;
        let plugins = checkout_plugins(&lab);
        let outside = lab.root.join("foreign-attempt");
        process::private_directory(&outside).unwrap();
        std::fs::write(outside.join("sentinel"), b"must survive").unwrap();
        let _scheduler = Scheduler::open(&lab.config, &lab.state).unwrap();
        let safe = process::now_ms();
        cache_observe(&lab, safe).await;
        let target = if replace_parent {
            plugins
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .to_path_buf()
        } else {
            plugins.parent().unwrap().parent().unwrap().to_path_buf()
        };
        let original = target.with_extension("original");
        std::fs::rename(&target, &original).unwrap();
        symlink(&outside, &target).unwrap();
        assert!(only_plugins(&lab, safe + 1_000).await.is_err());
        assert_eq!(
            std::fs::read(outside.join("sentinel")).unwrap(),
            b"must survive"
        );
        std::fs::remove_file(&target).unwrap();
        process::private_directory(&target).unwrap();
        assert!(
            only_plugins(&lab, safe + 1_000).await.is_err(),
            "different owned inode is still foreign to proof"
        );
        std::fs::remove_dir(&target).unwrap();
        std::fs::rename(&original, &target).unwrap();
        // A nested link is unlinked, never traversed into the foreign tree.
        symlink(&outside, plugins.join("external-link")).unwrap();
        cache_observe(&lab, safe + 1_000).await;
        assert!(!plugins.exists());
        assert_eq!(
            std::fs::read(outside.join("sentinel")).unwrap(),
            b"must survive"
        );
        lab.close().await;
    }
}

#[tokio::test]
async fn codex_home_expired_deadline_and_cancelled_control_leave_owned_tree_intact() {
    let lab = Lab::open("plugin-budget").await;
    let plugins = checkout_plugins(&lab);
    let identity = git::cleanup::DirectoryIdentity::read(&plugins).unwrap();
    let before = cache_fixture_files(&plugins);
    let mut control = lab.control.clone();
    control.deadline_ms = 0;
    assert!(
        git::cleanup::remove_owned_tree(&plugins, &identity, &control, 1)
            .await
            .is_err()
    );
    assert_eq!(cache_fixture_files(&plugins), before);
    let (cancel, receive) = watch::channel(true);
    control.cancel = receive;
    control.deadline_ms = process::now_ms() + 30_000;
    assert!(
        git::cleanup::remove_owned_tree(&plugins, &identity, &control, 1)
            .await
            .is_err()
    );
    assert_eq!(cache_fixture_files(&plugins), before);
    drop(cancel);
    lab.close().await;
}

#[tokio::test]
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Cleanup MUST copy required transcripts from an eligible terminal supplied-executor attempt's private home to retained storage outside the contents selected for removal.
async fn codex_home_interrupted_copy_restarts_before_any_vendor_disposal() {
    let lab = Lab::open("home-copy-interrupt").await;
    cache_success(&lab).await;
    let _scheduler = Scheduler::open(&lab.config, &lab.state).unwrap();
    workspace_only(&lab, process::now_ms()).await;
    let plugins = checkout_plugins(&lab);
    let home = plugins.parent().unwrap().parent().unwrap();
    process::private_directory(&home.join("sessions/2026")).unwrap();
    let bytes = vec![
        b'x';
        lab.config
            .limits
            .retention
            .as_ref()
            .unwrap()
            .worker
            .record_bytes
            + 17
    ];
    std::fs::write(home.join("sessions/2026/run.jsonl"), &bytes).unwrap();
    std::fs::write(
        home.join("sessions/2026/z-not-copied.jsonl"),
        b"another required transcript",
    )
    .unwrap();
    std::fs::write(home.join("vendor.sqlite"), b"disposable vendor bytes").unwrap();
    let (send, receive) = tokio::sync::oneshot::channel();
    git::cleanup::tests::hook(git::cleanup::tests::Point::AfterCopyChunk, move |_| {
        let _ = send.send(());
    });
    tokio::select! {biased;
        result=receive=>{result.unwrap();}
        result=only_plugins(&lab,process::now_ms())=>panic!("copy did not yield: {result:?}"),
    }
    assert!(progress(&lab)["exports"].is_null());
    assert!(plugins.exists() && home.join("vendor.sqlite").exists());
    assert_eq!(
        std::fs::read(home.join("sessions/2026/run.jsonl")).unwrap(),
        bytes
    );
    drop(_scheduler);
    let _reopened = Scheduler::open(&lab.config, &lab.state).unwrap();
    let later = home.join("sessions/2026/z-not-copied.jsonl");
    let temporarily_absent = lab.root.join("original-transcript");
    std::fs::rename(&later, &temporarily_absent).unwrap();
    assert!(
        only_plugins(&lab, process::now_ms())
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        plugins.exists() && home.join("vendor.sqlite").exists(),
        "nested disappearance cannot certify reduced copy"
    );
    std::fs::rename(temporarily_absent, later).unwrap();
    only_plugins(&lab, process::now_ms()).await.unwrap();
    let archive = lab
        .state
        .join("codex-transcripts")
        .join(component(&lab.attempt.id));
    assert_eq!(
        std::fs::read(archive.join("sessions/2026/run.jsonl")).unwrap(),
        bytes
    );
    assert!(
        !plugins.exists()
            && !home.join("vendor.sqlite").exists()
            && !home.join("sessions").exists()
    );
    assert_eq!(progress(&lab)["done"], true);
    lab.close().await;
}

#[tokio::test]
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Cleanup MUST verify the completeness and durability of each required transcript copy before removing its source.
async fn codex_home_corrupt_copy_and_replaced_archive_hold_interrupted_removal() {
    let lab = Lab::open("home-copy-verify").await;
    cache_success(&lab).await;
    let _scheduler = Scheduler::open(&lab.config, &lab.state).unwrap();
    workspace_only(&lab, process::now_ms()).await;
    let plugins = checkout_plugins(&lab);
    let home = plugins.parent().unwrap().parent().unwrap();
    process::private_directory(&home.join("sessions")).unwrap();
    std::fs::write(home.join("sessions/run.jsonl"), b"required transcript").unwrap();
    for n in 0..100 {
        std::fs::write(plugins.join(format!("payload-{n}")), b"disposable").unwrap();
    }
    let (send, receive) = tokio::sync::oneshot::channel();
    git::cleanup::tests::hook(git::cleanup::tests::Point::AfterUnlink, move |_| {
        let _ = send.send(());
    });
    tokio::select! {biased;
        result=receive=>{result.unwrap();}
        result=only_plugins(&lab,process::now_ms())=>panic!("removal did not yield: {result:?}"),
    }
    assert!(!progress(&lab)["exports"].is_null());
    let archive = lab
        .state
        .join("codex-transcripts")
        .join(component(&lab.attempt.id));
    let remaining = cache_fixture_files(home);
    std::fs::write(archive.join("sessions/run.jsonl"), b"corrupt copy").unwrap();
    assert!(
        only_plugins(&lab, process::now_ms())
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(cache_fixture_files(home), remaining);
    std::fs::write(archive.join("sessions/run.jsonl"), b"required transcript").unwrap();
    let original = archive.with_extension("original");
    std::fs::rename(&archive, &original).unwrap();
    process::private_directory(&archive).unwrap();
    assert!(
        only_plugins(&lab, process::now_ms())
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(cache_fixture_files(home), remaining);
    std::fs::remove_dir(&archive).unwrap();
    std::fs::rename(original, &archive).unwrap();
    only_plugins(&lab, process::now_ms()).await.unwrap();
    assert_eq!(progress(&lab)["done"], true);
    assert_eq!(
        std::fs::read(archive.join("sessions/run.jsonl")).unwrap(),
        b"required transcript"
    );
    std::fs::write(
        archive.join("sessions/run.jsonl"),
        b"corrupt after completion",
    )
    .unwrap();
    assert!(
        only_plugins(&lab, process::now_ms()).await.is_err(),
        "completed state must still detect missing/corrupt retained copy"
    );
    lab.close().await;
}

#[tokio::test]
async fn codex_home_transcript_link_or_unbound_archive_never_authorizes_vendor_removal() {
    for unbound in [false, true] {
        let lab = Lab::open("home-copy-foreign").await;
        cache_success(&lab).await;
        let _scheduler = Scheduler::open(&lab.config, &lab.state).unwrap();
        workspace_only(&lab, process::now_ms()).await;
        let plugins = checkout_plugins(&lab);
        let home = plugins.parent().unwrap().parent().unwrap();
        let foreign = lab.root.join("foreign-transcripts");
        process::private_directory(&foreign).unwrap();
        std::fs::write(foreign.join("keep"), b"unrelated bytes").unwrap();
        if unbound {
            let archive = lab
                .state
                .join("codex-transcripts")
                .join(component(&lab.attempt.id));
            process::private_directory(&archive).unwrap();
            std::fs::write(archive.join("unbound"), b"pre-existing bytes").unwrap();
        } else {
            symlink(&foreign, home.join("sessions")).unwrap();
        }
        let before = cache_fixture_files(&plugins);
        assert!(
            only_plugins(&lab, process::now_ms())
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(cache_fixture_files(&plugins), before);
        assert_eq!(
            std::fs::read(foreign.join("keep")).unwrap(),
            b"unrelated bytes"
        );
        assert_eq!(progress(&lab)["done"], false);
        lab.close().await;
    }
}

#[tokio::test]
async fn codex_home_recorded_thread_requires_session_copy_history_alone_is_not_enough() {
    let lab = Lab::open("home-copy-required").await;
    cache_success(&lab).await;
    let _scheduler = Scheduler::open(&lab.config, &lab.state).unwrap();
    workspace_only(&lab, process::now_ms()).await;
    let plugins = checkout_plugins(&lab);
    let home = plugins.parent().unwrap().parent().unwrap();
    std::fs::write(
        home.join("borg-usage.json"),
        br#"{"thread_id":"recorded-thread"}"#,
    )
    .unwrap();
    assert!(
        only_plugins(&lab, process::now_ms())
            .await
            .unwrap()
            .is_some(),
        "known association cannot be treated as pre-thread absence"
    );
    std::fs::write(home.join("borg-thread.json"), b"malformed association").unwrap();
    assert!(
        only_plugins(&lab, process::now_ms())
            .await
            .unwrap()
            .is_some()
    );
    assert!(plugins.exists());
    std::fs::write(
        home.join("borg-thread.json"),
        br#"{"thread_id":"recorded-thread","cwd":"fixture"}"#,
    )
    .unwrap();
    std::fs::write(home.join("history.jsonl"), b"optional CLI history").unwrap();
    assert!(
        only_plugins(&lab, process::now_ms())
            .await
            .unwrap()
            .is_some()
    );
    assert!(plugins.exists());
    assert_eq!(progress(&lab)["done"], false);
    process::private_directory(&home.join("archived_sessions")).unwrap();
    std::fs::write(
        home.join("archived_sessions/recorded-thread.jsonl"),
        b"required session",
    )
    .unwrap();
    only_plugins(&lab, process::now_ms()).await.unwrap();
    assert!(!plugins.exists());
    assert!(home.join("borg-thread.json").exists());
    let archive = lab
        .state
        .join("codex-transcripts")
        .join(component(&lab.attempt.id));
    assert_eq!(
        std::fs::read(archive.join("archived_sessions/recorded-thread.jsonl")).unwrap(),
        b"required session"
    );
    assert_eq!(
        std::fs::read(archive.join("history.jsonl")).unwrap(),
        b"optional CLI history"
    );
    lab.close().await;
}
