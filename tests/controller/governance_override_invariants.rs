//! Actual controller guards while a qualified temporary override is active.
//! Worker outcomes and protected checks are controlled fixture inputs. This does
//! not execute a provider, Git publication, protected rollout, or host change.
use super::*;
use borg_runtime::governance::{
    GovernanceMutation, GovernanceQuery, GovernanceResult, GovernanceState,
};
use borg_runtime::governance_completion::{
    self, CompletionBoundary, CompletionOutcome, EvaluateCompletion,
};
use borg_runtime::mcp::Caller;
use borg_runtime::protected_release as protected;
use std::collections::BTreeMap;
use std::os::unix::fs::{MetadataExt, PermissionsExt};

struct Fixture {
    controller: Controller,
    clock: Arc<AtomicU64>,
    active: GovernanceState,
    override_request: Operation,
    override_reply: Reply,
    protected_config: Option<protected::Config>,
    subject: Option<protected::Subject>,
}

async fn original(c: &Controller, operation: Operation) -> Reply {
    governance::governance_reply(c, operation).await
}

async fn governance_state(c: &Controller) -> GovernanceState {
    match call(
        c,
        Operation::QueryGovernance {
            query: GovernanceQuery::State,
        },
    )
    .await
    {
        Outcome::Ok {
            data: Data::Governance(value),
        } => match *value {
            GovernanceResult::State(state) => state,
            other => panic!("governance state expected: {other:?}"),
        },
        other => panic!("governance query expected: {other:?}"),
    }
}

impl Fixture {
    async fn new(with_protected: bool) -> Self {
        let database = path();
        let root = database.parent().unwrap();
        // This is only the newly allocated local fixture directory.
        std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let clock = Arc::new(AtomicU64::new(future() - 300_000));
        let mut cfg = governance::governance_config();
        let observed = clock.clone();
        cfg.clock = Arc::new(move || observed.load(Ordering::SeqCst));
        if with_protected {
            // Keep only the socket under the existing runner's short private root.
            // The database and all release-authority operands stay unchanged.
            let socket_root = std::env::var_os("BORG_TEST_SOCKETS")
                .map(PathBuf::from)
                .unwrap_or_else(std::env::temp_dir)
                .join(format!("go-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
            std::fs::create_dir(&socket_root).unwrap();
            std::fs::set_permissions(&socket_root, std::fs::Permissions::from_mode(0o700)).unwrap();
            let uid = std::fs::metadata(root).unwrap().uid();
            cfg.protected_release = Some(protected::Config {
                socket: socket_root.join("protected.sock"),
                worker_uids: vec![uid.wrapping_add(1)],
                producers: [
                    ("checker", protected::Role::Checker),
                    ("human", protected::Role::Human),
                    ("rollout", protected::Role::Rollout),
                ]
                .into_iter()
                .map(|(id, role)| protected::Producer {
                    id: id.into(),
                    uid,
                    role,
                })
                .collect(),
                policy: "override-test-policy".into(),
                recipe_sha256: "e".repeat(64),
                required_checks: vec!["package".into(), "behavior".into()],
                max_check_age_ms: 60_000,
                repository: fixture_repository_registration().source_endpoint,
                repository_path: root.join("unused-repository.git"),
                target_ref: "refs/heads/test".into(),
                deployment_target: "local-fixture".into(),
                target_revision: "fixture-revision-1".into(),
                installed_package_sha256: "1".repeat(64),
                max_records: 1000,
            });
        }
        let protected_config = cfg.protected_release.clone();
        let controller = Controller::open_with_config(database, cfg).unwrap();
        // Seed the accepted release through the normal controlled controller
        // fixture before ordinary completion governance becomes enforced.
        let subject = if let Some(config) = &protected_config {
            let task = accepted_release_task(&controller, "override-release").await;
            assert_eq!(
                task.input.repository.as_ref().unwrap().url,
                config.repository
            );
            let claim = release(
                call(
                    &controller,
                    Operation::PrepareRelease(release_input(&task, "override-release-claim")),
                )
                .await,
            );
            Some(protected::Subject {
                release_id: claim.id,
                artifact: claim.artifact,
                repository: config.repository.clone(),
                target_ref: config.target_ref.clone(),
                expected_base: "0".repeat(40),
                policy: config.policy.clone(),
                deployment_target: config.deployment_target.clone(),
                target_revision: config.target_revision.clone(),
                installed_package_sha256: config.installed_package_sha256.clone(),
            })
        } else {
            None
        };
        // Both revisions have real immutable rules, counterfactual evidence and
        // normal promotion receipts. No qualification rows are inserted by SQL.
        let first = governance::promote(&controller, "override-permissive", true).await;
        let second = governance::promote(&controller, "override-prior", false).await;
        let now = clock.load(Ordering::SeqCst);
        let override_request = Operation::MutateGovernance {
            operation_id: "override-active".into(),
            mutation: GovernanceMutation::Override {
                expected_revision: second.configuration.revision.clone(),
                configuration_revision: first.configuration.revision.clone(),
                expires_at_ms: now + 1000,
                reason: "Exercise independent mandatory guards".into(),
            },
        };
        let override_reply = original(&controller, override_request.clone()).await;
        assert!(override_reply.commit.is_some());
        let active = governance_state(&controller).await;
        assert_eq!(active.selected_revision, first.configuration.revision);
        let temporary = active.temporary_override.as_ref().unwrap();
        assert_eq!(temporary.prior_revision, second.configuration.revision);
        assert_eq!(temporary.replacement_revision, active.selected_revision);
        assert_eq!(temporary.expires_at_ms, now + 1000);
        Self {
            controller,
            clock,
            active,
            override_request,
            override_reply,
            protected_config,
            subject,
        }
    }

    async fn assert_active(&self) {
        assert_eq!(governance_state(&self.controller).await, self.active);
        assert!(
            self.clock.load(Ordering::SeqCst)
                < self
                    .active
                    .temporary_override
                    .as_ref()
                    .unwrap()
                    .expires_at_ms
        );
        let replay = original(&self.controller, self.override_request.clone()).await;
        assert_eq!(replay.outcome, self.override_reply.outcome);
        assert_eq!(replay.commit, self.override_reply.commit);
    }

    async fn active_task(&self, label: &str, coding_task: bool) -> (Task, Attempt, Worker) {
        let input = if coding_task {
            coding(label)
        } else {
            native(label)
        };
        let queued = submit(&self.controller, &format!("{label}-submit"), input).await;
        let w = worker(
            &format!("{label}-worker"),
            "current-incarnation",
            usize::from(!coding_task),
            usize::from(coding_task),
        );
        register(&self.controller, &w).await;
        let issued = commands(&self.controller, &w, vec![]).await;
        let attempt = run(&issued);
        assert_eq!(attempt.task_id, queued.id);
        let acknowledgments = issued
            .iter()
            .filter_map(|command| match command {
                WorkerCommand::RunAttempt { command_id, .. } => Some(command_id.clone()),
                _ => None,
            })
            .collect();
        assert!(
            commands(&self.controller, &w, acknowledgments)
                .await
                .iter()
                .all(|command| !matches!(command, WorkerCommand::RunAttempt { .. }))
        );
        let current = task(
            report(
                &self.controller,
                &attempt,
                &format!("{label}-started"),
                Observation::Started {
                    thread_id: None,
                    turn_id: None,
                },
            )
            .await,
        );
        (current, attempt, w)
    }
}

struct ProtectedServer(tokio::task::JoinHandle<Result<(), String>>);
impl Drop for ProtectedServer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn protected_call(f: &Fixture, id: &str, command: protected::Command) -> protected::Reply {
    protected::call(
        &f.protected_config.as_ref().unwrap().socket,
        &protected::Request {
            revision: protected::REVISION,
            operation_id: id.into(),
            command,
        },
        &f.controller.limits().interface,
    )
    .await
    .unwrap()
}

#[tokio::test]
//= spec/impl/v2/implementation.md#emergency-control-is-independent-of-ordinary-policy
//= type=test
//# A temporary governance override MUST NOT bypass protected-release authorization, mandatory validation, ownership fencing or uncertain-effect reconciliation.
// Scoped matrix member: exact protected human-grant guard under an active override.
async fn active_override_cannot_replace_exact_human_release_authority() {
    let f = Fixture::new(true).await;
    let config = f.protected_config.as_ref().unwrap().clone();
    let listener = protected::listen(&config, &f.controller.limits().interface).unwrap();
    let mut server = ProtectedServer(tokio::spawn(protected::serve(
        f.controller.clone(),
        config.clone(),
        listener,
    )));
    let subject = f.subject.as_ref().unwrap().clone();
    let now = f.clock.load(Ordering::SeqCst);
    let check = protected::Check {
        subject: subject.clone(),
        recipe_sha256: config.recipe_sha256,
        checked_at_ms: now,
        valid_until_ms: now + 30_000,
        results: BTreeMap::from([("package".into(), true), ("behavior".into(), true)]),
        cleanup_verified: true,
    };
    assert_eq!(
        protected_call(&f, "override-check", protected::Command::Check(check))
            .await
            .status,
        "ok"
    );
    let prepare = |grant_id| protected::Command::Prepare {
        kind: protected::Kind::Rollout,
        subject: subject.clone(),
        check_id: "override-check".into(),
        grant_id,
    };
    f.assert_active().await;
    let refused = protected_call(&f, "override-no-human", prepare(None)).await;
    assert_eq!(refused.status, "human_authorization_required");
    assert_eq!(
        protected_call(
            &f,
            "override-no-action",
            protected::Command::GetAction {
                action_id: "override-no-human".into(),
            }
        )
        .await
        .status,
        "not_found"
    );
    assert_eq!(
        protected_call(
            &f,
            "override-grant",
            protected::Command::Grant {
                subject: subject.clone(),
                valid_until_ms: now + 20_000,
            }
        )
        .await
        .status,
        "ok"
    );
    let prepared = protected_call(
        &f,
        "override-authorized",
        prepare(Some("override-grant".into())),
    )
    .await;
    assert_eq!(prepared.status, "ok");
    let action: protected::Action = serde_json::from_value(prepared.data).unwrap();
    assert_eq!(action.kind, protected::Kind::Rollout);
    assert_eq!(action.state, protected::State::Prepared);
    assert_eq!(action.subject, subject);
    let replay = protected_call(&f, "override-no-human", prepare(None)).await;
    assert!(replay.replayed);
    assert_eq!(replay.status, refused.status);
    assert_eq!(replay.data, refused.data);
    f.assert_active().await;
    server.0.abort();
    assert!((&mut server.0).await.unwrap_err().is_cancelled());
    // No StartRollout, ObserveRollout or external adapter is invoked.
}

#[tokio::test]
//= spec/impl/v2/implementation.md#emergency-control-is-independent-of-ordinary-policy
//= type=test
//# A temporary governance override MUST NOT bypass protected-release authorization, mandatory validation, ownership fencing or uncertain-effect reconciliation.
// Scoped matrix member: actual schema helper refusal, followed by valid acceptance.
async fn active_override_cannot_bypass_mandatory_native_result_validation() {
    let f = Fixture::new(false).await;
    let (initial, a, _) = f.active_task("override-validation", false).await;
    governance::completed_native_request(&f.controller, &a, None, None).await;
    complete_native_loop(&f.controller, &a).await;
    let caller = Caller {
        task_id: a.task_id.clone(),
        attempt_id: a.id.clone(),
        generation: a.generation,
        worker_id: a.worker_id.clone(),
        incarnation: a.worker_incarnation.clone(),
    };
    let boundary = CompletionBoundary::NativeResponse { request_index: 1 };
    let admitted = call(
        &f.controller,
        Operation::EvaluateCompletion(EvaluateCompletion {
            operation_id: governance_completion::evaluation_id(&caller, &boundary).unwrap(),
            caller,
            boundary,
            intervening_requests: vec![],
        }),
    )
    .await;
    assert!(
        matches!(admitted, Outcome::Ok { data: Data::Completion(CompletionOutcome::Evaluated(ref value)) }
        if value.state.disposition == borg_runtime::governance::CompletionDisposition::Admit),
        "{admitted:?}"
    );
    task(
        report(
            &f.controller,
            &a,
            "override-validation-executor",
            Observation::ExecutorOutcome {
                success: true,
                summary: "controlled successful executor".into(),
            },
        )
        .await,
    );
    task(
        report(
            &f.controller,
            &a,
            "override-validation-stop",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    let before = get(&f.controller, &initial.id).await;
    let candidate = |id: &str, output| Operation::ObserveWorker {
        report: Report {
            report_id: id.into(),
            worker_id: a.worker_id.clone(),
            incarnation: a.worker_incarnation.clone(),
            task_id: a.task_id.clone(),
            attempt_id: a.id.clone(),
            generation: a.generation,
            observation: Observation::ResultCandidate {
                checks_passed: true,
                summary: "same candidate".into(),
                checkpoint: None,
                validation: vec![borg_runtime::native_loop_contract::evidence(&a.id)],
                output: Some(output),
            },
        },
    };
    f.assert_active().await;
    let bad = candidate(
        "override-invalid-schema",
        json!({"answer":"not an integer"}),
    );
    let refused = original(&f.controller, bad.clone()).await;
    assert_eq!(code(refused.outcome.clone()), "schema_validation");
    assert_eq!(get(&f.controller, &initial.id).await, before);
    assert!(before.result.is_none());
    let valid = task(
        call(
            &f.controller,
            candidate("override-valid-schema", json!({"answer":42})),
        )
        .await,
    );
    assert_eq!(valid.status, TaskStatus::Succeeded);
    assert_eq!(
        valid.result.as_ref().unwrap().output,
        Some(json!({"answer":42}))
    );
    let replay = original(&f.controller, bad).await;
    assert_eq!(replay.outcome, refused.outcome);
    assert_eq!(replay.commit, refused.commit);
    assert_eq!(get(&f.controller, &initial.id).await, valid);
    f.assert_active().await;
}

#[tokio::test]
//= spec/impl/v2/implementation.md#emergency-control-is-independent-of-ordinary-policy
//= type=test
//# A temporary governance override MUST NOT bypass protected-release authorization, mandatory validation, ownership fencing or uncertain-effect reconciliation.
// Scoped matrix member: a real retired generation cannot write current memory.
async fn active_override_cannot_restore_a_retired_attempt_owner() {
    let f = Fixture::new(false).await;
    let (initial, old, worker) = f.active_task("override-owner", false).await;
    task(
        call(
            &f.controller,
            Operation::CancelTask {
                operation_id: "override-owner-cancel".into(),
                task_id: initial.id.clone(),
                expected_revision: initial.revision,
            },
        )
        .await,
    );
    let stopped = task(
        report(
            &f.controller,
            &old,
            "override-owner-stop",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    let queued = task(
        call(
            &f.controller,
            Operation::RetryTask {
                operation_id: "override-owner-retry".into(),
                task_id: initial.id.clone(),
                expected_revision: stopped.revision,
            },
        )
        .await,
    );
    assert_eq!(queued.status, TaskStatus::Queued);
    let current = run(&commands(&f.controller, &worker, vec![]).await);
    assert_ne!(current.id, old.id);
    assert!(current.generation > old.generation);
    let write = |attempt: &Attempt, id: &str, value| Operation::SetMemory {
        operation_id: id.into(),
        task_id: initial.id.clone(),
        attempt_id: attempt.id.clone(),
        generation: attempt.generation,
        key: "override-owner-value".into(),
        expected_revision: None,
        value,
    };
    let before = get(&f.controller, &initial.id).await;
    f.assert_active().await;
    let stale = write(&old, "override-old-write", json!(1));
    let refused = original(&f.controller, stale.clone()).await;
    assert_eq!(code(refused.outcome.clone()), "stale_generation");
    assert_eq!(get(&f.controller, &initial.id).await, before);
    let accepted = call(
        &f.controller,
        write(&current, "override-current-write", json!(2)),
    )
    .await;
    assert!(matches!(accepted, Outcome::Ok { .. }), "{accepted:?}");
    let replay = original(&f.controller, stale).await;
    assert_eq!(replay.outcome, refused.outcome);
    assert_eq!(replay.commit, refused.commit);
    let memory = call(
        &f.controller,
        Operation::GetMemory {
            agent_id: initial.assigned_agent_id.clone(),
            key: "override-owner-value".into(),
        },
    )
    .await;
    assert!(
        matches!(memory, Outcome::Ok { data: Data::Memory(Some(ref value)) } if value.value == json!(2)),
        "{memory:?}"
    );
    assert_eq!(
        get(&f.controller, &initial.id)
            .await
            .attempt
            .as_ref()
            .unwrap()
            .id,
        current.id
    );
    f.assert_active().await;
}

#[tokio::test]
//= spec/impl/v2/implementation.md#emergency-control-is-independent-of-ordinary-policy
//= type=test
//# A temporary governance override MUST NOT bypass protected-release authorization, mandatory validation, ownership fencing or uncertain-effect reconciliation.
// Scoped matrix member: original unknown effect must settle before a new attempt.
async fn active_override_cannot_replace_original_uncertain_effect_reconciliation() {
    let f = Fixture::new(false).await;
    let (initial, a, _) = f.active_task("override-effect", true).await;
    let checkpoint = Checkpoint {
        registration: fixture_repository_binding(),
        repository: initial
            .input
            .repository
            .as_ref()
            .unwrap()
            .checkpoint_remote
            .clone(),
        commit: "b".repeat(40),
        remote_ref: checkpoint_ref(&initial.id, &a.id, "override-effect"),
        operation_id: "override-effect".into(),
    };
    let prepared = Operation::PrepareOperation {
        operation_id: checkpoint.operation_id.clone(),
        task_id: a.task_id.clone(),
        attempt_id: a.id.clone(),
        generation: a.generation,
        kind: "checkpoint".into(),
        intended: json!({"registration":checkpoint.registration,"repository":checkpoint.repository,
            "commit":checkpoint.commit,"remote_ref":checkpoint.remote_ref}),
    };
    let prepared_reply = original(&f.controller, prepared.clone()).await;
    assert!(matches!(prepared_reply.outcome, Outcome::Ok { .. }));
    let before_cancel = get(&f.controller, &initial.id).await;
    task(
        call(
            &f.controller,
            Operation::CancelTask {
                operation_id: "override-effect-cancel".into(),
                task_id: initial.id.clone(),
                expected_revision: before_cancel.revision,
            },
        )
        .await,
    );
    let stopped = task(
        report(
            &f.controller,
            &a,
            "override-effect-stop",
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    assert!(stopped.attempt.as_ref().unwrap().uncertain);
    let retry = Operation::RetryTask {
        operation_id: "override-unknown-retry".into(),
        task_id: initial.id.clone(),
        expected_revision: stopped.revision,
    };
    f.assert_active().await;
    let refused = original(&f.controller, retry.clone()).await;
    assert_eq!(code(refused.outcome.clone()), "reconciliation_required");
    assert_eq!(get(&f.controller, &initial.id).await, stopped);
    let original_prepare = original(&f.controller, prepared).await;
    assert_eq!(original_prepare.outcome, prepared_reply.outcome);
    assert_eq!(original_prepare.commit, prepared_reply.commit);
    assert!(
        get(&f.controller, &initial.id)
            .await
            .attempt
            .as_ref()
            .unwrap()
            .uncertain
    );
    let settlement = Operation::SettleOperation {
        settlement_id: "override-original-settlement".into(),
        operation_id: checkpoint.operation_id.clone(),
        task_id: initial.id.clone(),
        attempt_id: a.id.clone(),
        generation: a.generation,
        outcome: json!({"acknowledged":true}),
        checkpoint: Some(checkpoint.clone()),
    };
    assert!(matches!(
        call(&f.controller, settlement).await,
        Outcome::Ok { .. }
    ));
    let known = get(&f.controller, &initial.id).await;
    assert!(!known.attempt.as_ref().unwrap().uncertain);
    assert_eq!(known.selected_checkpoint, Some(checkpoint));
    let queued = task(
        call(
            &f.controller,
            Operation::RetryTask {
                operation_id: "override-known-retry".into(),
                task_id: initial.id.clone(),
                expected_revision: known.revision,
            },
        )
        .await,
    );
    assert_eq!(queued.status, TaskStatus::Queued);
    let replay = original(&f.controller, retry).await;
    assert_eq!(replay.outcome, refused.outcome);
    assert_eq!(replay.commit, refused.commit);
    assert_eq!(get(&f.controller, &initial.id).await, queued);
    f.assert_active().await;
}
