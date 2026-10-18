use super::*;
use crate::controller::{Controller, ControllerConfig};
use crate::product::{UiRequest, UiResponse, handle};
use crate::protected_release::{self as pr, Command as Pc, Request as Pq};
use crate::protocol::*;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
const ORIGIN: &str = "http://127.0.0.1:7080";
static NEXT: AtomicU64 = AtomicU64::new(1);
struct Fixture {
    controller: Controller,
    path: PathBuf,
    config: ControllerConfig,
    clock: Arc<AtomicU64>,
}
impl Fixture {
    fn new() -> Self {
        let root = PathBuf::from(std::env::var("BORG_TEST_ARTIFACT").unwrap())
            .join(format!("case-{}", NEXT.fetch_add(1, Ordering::Relaxed)));
        std::fs::create_dir_all(&root).unwrap();
        let clock = Arc::new(AtomicU64::new(now()));
        let held = clock.clone();
        let config = ControllerConfig {
            validation_executable: PathBuf::from(std::env::var("BORG_TEST_RUNTIME").unwrap()),
            clock: Arc::new(move || held.load(Ordering::SeqCst)),
            protected_release: Some(Config {
                socket: PathBuf::from(std::env::var("BORG_TEST_SOCKETS").unwrap())
                    .join(format!("case-{}", NEXT.fetch_add(1, Ordering::Relaxed))),
                worker_uids: vec![9004],
                producers: vec![
                    Producer {
                        id: "checker".into(),
                        uid: 9000,
                        role: Role::Checker,
                    },
                    Producer {
                        id: "repository".into(),
                        uid: 9001,
                        role: Role::Repository,
                    },
                    Producer {
                        id: "human".into(),
                        uid: 9002,
                        role: Role::Human,
                    },
                    Producer {
                        id: "rollout".into(),
                        uid: 9003,
                        role: Role::Rollout,
                    },
                ],
                policy: "policy-1".into(),
                recipe_sha256: "e".repeat(64),
                required_checks: vec!["package".into(), "behavior".into()],
                max_check_age_ms: 60_000,
                repository: "file:///tmp/local-test-repository".into(),
                repository_path: root.join("repo.git"),
                target_ref: "refs/heads/main".into(),
                deployment_target: "test-target".into(),
                target_revision: "revision-1".into(),
                installed_package_sha256: "1".repeat(64),
                max_records: 1000,
            }),
            ..ControllerConfig::default()
        };
        let path = root.join("tasks.sqlite");
        let controller = Controller::open_with_config(&path, config.clone()).unwrap();
        Self {
            controller,
            path,
            config,
            clock,
        }
    }
    async fn seed(&self) -> Subject {
        self.seed_commit(&"a".repeat(40)).await
    }
    async fn seed_commit(&self, commit: &str) -> Subject {
        self.seed_tree(commit, &"b".repeat(40)).await
    }
    async fn seed_tree(&self, commit: &str, tree: &str) -> Subject {
        let t = controlled_release_source(self, commit).await;
        let mut artifact = release_claim(&t, "d");
        artifact.source_tree = tree.into();
        let Data::Release(r) = call(
            &self.controller,
            Operation::PrepareRelease(PrepareReleaseInput {
                operation_id: "release-claim".into(),
                task_id: t.id.clone(),
                expected_task_revision: t.revision,
                checkpoint: t.selected_checkpoint.clone().unwrap(),
                artifact,
            }),
        )
        .await
        else {
            panic!()
        };
        let c = self.config.protected_release.as_ref().unwrap();
        Subject {
            release_id: r.id,
            artifact: r.artifact,
            repository: c.repository.clone(),
            target_ref: c.target_ref.clone(),
            expected_base: "0".repeat(40),
            policy: c.policy.clone(),
            deployment_target: c.deployment_target.clone(),
            target_revision: c.target_revision.clone(),
            installed_package_sha256: c.installed_package_sha256.clone(),
        }
    }

    async fn seed_artifact(&self, artifact: ReleaseArtifact) -> Subject {
        let t = controlled_release_source(self, &artifact.source_commit).await;
        let Data::Release(r) = call(
            &self.controller,
            Operation::PrepareRelease(PrepareReleaseInput {
                operation_id: "package-release".into(),
                task_id: t.id.clone(),
                expected_task_revision: t.revision,
                checkpoint: t.selected_checkpoint.clone().unwrap(),
                artifact,
            }),
        )
        .await
        else {
            panic!()
        };
        let c = self.config.protected_release.as_ref().unwrap();
        Subject {
            release_id: r.id,
            artifact: r.artifact,
            repository: c.repository.clone(),
            target_ref: c.target_ref.clone(),
            expected_base: "0".repeat(40),
            policy: c.policy.clone(),
            deployment_target: c.deployment_target.clone(),
            target_revision: c.target_revision.clone(),
            installed_package_sha256: c.installed_package_sha256.clone(),
        }
    }
    async fn p(&self, uid: u32, id: &str, command: Pc) -> pr::Reply {
        self.controller
            .protected(
                uid,
                Pq {
                    revision: 1,
                    operation_id: id.into(),
                    command,
                },
            )
            .await
    }
    fn check(&self, s: &Subject) -> Check {
        let n = self.clock.load(Ordering::SeqCst);
        Check {
            subject: s.clone(),
            recipe_sha256: "e".repeat(64),
            checked_at_ms: n,
            valid_until_ms: n + 30_000,
            results: std::collections::BTreeMap::from([
                ("package".into(), true),
                ("behavior".into(), true),
            ]),
            cleanup_verified: true,
        }
    }
    async fn checks(&self, s: &Subject) {
        assert_eq!(
            self.p(9000, "check", Pc::Check(self.check(s))).await.status,
            "ok"
        )
    }
    async fn merge(&self, s: &Subject, id: &str) -> pr::Reply {
        self.p(
            9001,
            id,
            Pc::Prepare {
                kind: Kind::Merge,
                subject: s.clone(),
                check_id: "check".into(),
                grant_id: None,
            },
        )
        .await
    }
    async fn close(self) -> (PathBuf, ControllerConfig, Arc<AtomicU64>) {
        let Self {
            controller,
            path,
            config,
            clock,
        } = self;
        drop(controller);
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        (path, config, clock)
    }
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn encode(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                char::from(b).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

fn form(fields: &[(&str, String)]) -> Vec<u8> {
    fields
        .iter()
        .map(|(key, value)| format!("{}={}", encode(key), encode(value)))
        .collect::<Vec<_>>()
        .join("&")
        .into_bytes()
}

fn submission(operation: &str, instruction: &str, dependencies: &str) -> Vec<u8> {
    form(&[
        ("operation_id", operation.into()),
        ("instruction", instruction.into()),
        ("definition_id", "coding-default".into()),
        ("definition_revision", "revision-1".into()),
        ("executor", "coding".into()),
        ("model", "gpt-6-astra".into()),
        ("reasoning_effort", "max".into()),
        ("execution_profile", "collaboration_v1".into()),
        ("repository_id", "protected-ui-fixture".into()),
        ("registration_revision", "r1".into()),
        (
            "start_commit",
            "1111111111111111111111111111111111111111".into(),
        ),
        (
            "acceptance_commands",
            "[[\"cargo\",\"test\",\"--locked\"]]".into(),
        ),
        ("output_schema", String::new()),
        ("dependencies", dependencies.into()),
        ("deadline_unix_ms", (now() + 3_600_000).to_string()),
        ("model_requests", "5".into()),
        ("output_bytes", "4096".into()),
        ("max_delegation_depth", "0".into()),
    ])
}

async fn ui(controller: &Controller, method: &str, path: &str, body: Vec<u8>) -> UiResponse {
    handle(
        controller,
        UiRequest {
            method: method.into(),
            path: path.into(),
            body,
            origin: Some(ORIGIN.into()),
            expected_origin: ORIGIN.into(),
        },
    )
    .await
}

async fn task(controller: &Controller, id: &str) -> Task {
    match controller
        .handle(crate::protocol::Request {
            protocol_revision: WIRE_REVISION,
            request_id: format!("read-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
            command: Operation::GetTask { task_id: id.into() },
        })
        .await
        .outcome
    {
        Outcome::Ok {
            data: Data::Task(task),
        } => *task,
        other => panic!("expected task, got {other:?}"),
    }
}

async fn call(controller: &Controller, command: Operation) -> Data {
    let reply = controller
        .handle(crate::protocol::Request {
            protocol_revision: WIRE_REVISION,
            request_id: format!("fixture-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
            command,
        })
        .await;
    match reply.outcome {
        Outcome::Ok { data } => data,
        other => panic!("fixture operation refused: {other:?}"),
    }
}

async fn report(controller: &Controller, attempt: &Attempt, observation: Observation) {
    call(
        controller,
        Operation::ObserveWorker {
            report: Report {
                report_id: format!("report-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
                worker_id: attempt.worker_id.clone(),
                incarnation: attempt.worker_incarnation.clone(),
                task_id: attempt.task_id.clone(),
                attempt_id: attempt.id.clone(),
                generation: attempt.generation,
                observation,
            },
        },
    )
    .await;
}

async fn controlled_release_source(fixture: &Fixture, commit: &str) -> Task {
    call(
        &fixture.controller,
        Operation::RegisterRepository {
            operation_id: "register-protected-ui-fixture".into(),
            registration: crate::repository_catalogue::Registration {
                pull_request_target: None,
                repository_id: "protected-ui-fixture".into(),
                registration_revision: "r1".into(),
                display_name: "Protected UI source fixture".into(),
                source_endpoint: "file:///tmp/local-test-repository".into(),
                checkpoint_endpoint: "origin".into(),
                source_store_key: "source.git".into(),
                checkpoint_store_key: "checkpoint.git".into(),
            },
        },
    )
    .await;
    let created = ui(
        &fixture.controller,
        "POST",
        "/tasks",
        submission("release-source", "controlled source fixture", ""),
    )
    .await;
    let id = task_id(&created).to_string();
    call(
        &fixture.controller,
        Operation::RegisterWorker {
            operation_id: "release-fixture-registration".into(),
            worker: Worker {
                id: "release-fixture-worker".into(),
                incarnation: "incarnation-1".into(),
                coding_capacity: 1,
                native_capacity: 0,
            },
        },
    )
    .await;
    let Data::Commands(commands) = call(
        &fixture.controller,
        Operation::AwaitCommands {
            operation_id: format!("release-fixture-assignment-{id}"),
            worker_id: "release-fixture-worker".into(),
            incarnation: "incarnation-1".into(),
            acknowledged: vec![],
        },
    )
    .await
    else {
        panic!("fixture assignment missing");
    };
    let attempt = commands
        .iter()
        .find_map(|command| match command {
            WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == id => {
                Some(attempt.clone())
            }
            _ => None,
        })
        .expect("controlled assigned attempt");
    report(
        &fixture.controller,
        &attempt,
        Observation::Started {
            thread_id: Some("fixture-thread".into()),
            turn_id: Some("fixture-turn".into()),
        },
    )
    .await;
    let source = task(&fixture.controller, &id).await;
    let checkpoint = crate::worker::git::checkpoint(
        source.input.repository.as_ref().unwrap(),
        &id,
        &attempt.id,
        commit.into(),
    )
    .unwrap();
    call(&fixture.controller, Operation::PrepareOperation {
        operation_id: checkpoint.operation_id.clone(), task_id: id.clone(), attempt_id: attempt.id.clone(), generation: attempt.generation,
        kind: "checkpoint".into(), intended: serde_json::json!({"registration":checkpoint.registration,"repository":checkpoint.repository,"commit":checkpoint.commit,"remote_ref":checkpoint.remote_ref}),
    }).await;
    call(
        &fixture.controller,
        Operation::SettleOperation {
            settlement_id: format!("release-fixture-settlement-{id}"),
            operation_id: checkpoint.operation_id.clone(),
            task_id: id.clone(),
            attempt_id: attempt.id.clone(),
            generation: attempt.generation,
            outcome: serde_json::json!({"acknowledged":true}),
            checkpoint: Some(checkpoint.clone()),
        },
    )
    .await;
    report(
        &fixture.controller,
        &attempt,
        Observation::ExecutorOutcome {
            success: true,
            summary: "controlled task outcome only".into(),
        },
    )
    .await;
    report(
        &fixture.controller,
        &attempt,
        Observation::ProcessTreeStopped,
    )
    .await;
    report(
        &fixture.controller,
        &attempt,
        Observation::ResultCandidate {
            checks_passed: true,
            summary: "controlled task check passed".into(),
            checkpoint: Some(checkpoint),
            validation: vec!["controlled task validation; not protected release evidence".into()],
            output: Some(
                serde_json::json!({"checks":"passed","human_rollout":"authorized","deployed":true}),
            ),
        },
    )
    .await;
    task(&fixture.controller, &id).await
}

fn release_claim(source: &Task, digest: &str) -> ReleaseArtifact {
    ReleaseArtifact {
        format: "borg-v2-release/1".into(),
        package_sha256: digest.repeat(64),
        package_bytes: 20_480,
        manifest_sha256: "e".repeat(64),
        source_commit: source.selected_checkpoint.as_ref().unwrap().commit.clone(),
        source_tree: "b".repeat(40),
        object_format: "sha1".into(),
        binary_sha256: "f".repeat(64),
        binary_bytes: 1024,
        target: "x86_64-unknown-linux-gnu".into(),
    }
}

fn task_id(response: &UiResponse) -> &str {
    assert_eq!(response.status, 303, "{}", response.body);
    response
        .location
        .as_deref()
        .unwrap()
        .strip_prefix("/tasks/")
        .unwrap()
}
async fn operator_fixture() -> Fixture {
    let f = Fixture::new();
    let (path, mut config, clock) = f.close().await;
    let protected = config.protected_release.as_mut().unwrap();
    for producer in &mut protected.producers {
        producer.uid = 9000;
    }
    // Deliberately put neither the checker nor the human first.
    protected.producers.rotate_left(1);
    protected.validate(&config.limits.interface).unwrap();
    let controller = Controller::open_with_config(&path, config.clone()).unwrap();
    Fixture { controller, path, config, clock }
}

#[tokio::test]
async fn one_trusted_operator_retains_typed_receipts_and_unknown_rollout_reconciliation() {
    let f = operator_fixture().await;
    let s = f.seed().await;
    let check = Pc::Check(f.check(&s));
    assert_eq!(f.p(9004, "worker-check", check.clone()).await.status, "unauthenticated");
    assert_eq!(f.p(9005, "unknown-check", check.clone()).await.status, "unauthenticated");
    assert_eq!(f.p(9000, "operator-check", check.clone()).await.status, "ok");
    assert!(f.p(9000, "operator-check", check).await.replayed);
    let grant = Pc::Grant {
        subject: s.clone(), valid_until_ms: f.clock.load(Ordering::SeqCst) + 20_000,
    };
    assert_eq!(f.p(9000, "operator-grant", grant).await.status, "ok");
    for (id, kind, grant_id) in [
        ("operator-merge", Kind::Merge, None),
        ("operator-rollout", Kind::Rollout, Some("operator-grant".into())),
    ] {
        assert_eq!(f.p(9000, id, Pc::Prepare {
            kind, subject: s.clone(), check_id: "operator-check".into(), grant_id,
        }).await.status, "ok");
    }
    for (id, role) in [
        ("operator-check", "checker"), ("operator-grant", "human"),
        ("operator-merge", "repository"), ("operator-rollout", "rollout"),
    ] {
        let receipt = f.p(9000, "read", Pc::GetReceipt { operation_id: id.into() }).await;
        assert_eq!(receipt.status, "ok");
        assert_eq!(receipt.data["producer"]["uid"], 9000);
        assert_eq!(receipt.data["producer"]["id"], role);
        assert_eq!(receipt.data["producer"]["role"], role);
    }
    let start = Pc::StartRollout { action_id: "operator-rollout".into(), subject: s.clone() };
    let started = f.p(9000, "operator-start", start.clone()).await;
    assert_eq!(started.status, "ok");
    assert_eq!(started.data["state"], "started");
    // A lost acknowledgement is resolved by the original stored request/reply.
    let readback = f.p(9000, "read-start", Pc::GetReceipt { operation_id: "operator-start".into() }).await;
    assert_eq!(readback.data["producer"]["role"], "rollout");
    assert_eq!(readback.data["request"]["command"], serde_json::to_value(&start).unwrap());
    assert_eq!(readback.data["reply"], serde_json::to_value(&started).unwrap());
    assert!(f.p(9000, "operator-start", start).await.replayed);
    let mut changed = s.clone(); changed.target_revision = "other-target".into();
    assert_eq!(f.p(9000, "operator-start", Pc::StartRollout {
        action_id: "operator-rollout".into(), subject: changed,
    }).await.status, "operation_conflict");
    assert_eq!(f.p(9000, "observe-unknown", Pc::ObserveRollout {
        action_id: "operator-rollout".into(), subject: s.clone(),
        installed_package_sha256: s.installed_package_sha256.clone(), observed: false,
    }).await.data["state"], "unknown");
    assert_eq!(f.p(9000, "observe-wrong", Pc::ObserveRollout {
        action_id: "operator-rollout".into(), subject: s.clone(),
        installed_package_sha256: s.installed_package_sha256.clone(), observed: true,
    }).await.status, "wrong_installed_artifact");
    assert_eq!(f.p(9000, "observe-applied", Pc::ObserveRollout {
        action_id: "operator-rollout".into(), subject: s.clone(),
        installed_package_sha256: s.artifact.package_sha256.clone(), observed: true,
    }).await.data["state"], "applied");
    let (path, mut config, clock) = f.close().await;
    config.protected_release.as_mut().unwrap().producers.reverse();
    let controller = Controller::open_with_config(&path, config.clone()).unwrap();
    let f = Fixture { controller, path, config, clock };
    let record = f.p(9000, "after-restart", Pc::GetReceipt { operation_id: "operator-start".into() }).await;
    assert_eq!(record.data, readback.data);
    let replay = f.p(9000, "operator-start", Pc::StartRollout {
        action_id: "operator-rollout".into(), subject: s,
    }).await;
    assert!(replay.replayed); assert_eq!(replay.data, started.data);
}

//= spec/impl/v2/implementation.md#changes-can-be-prepared-without-per-change-human-approval
//# The final rollout MUST require human authorization bound to that artifact and target.
//= spec/impl/v2/implementation.md#changes-can-be-prepared-without-per-change-human-approval
//# Human authorization MUST NOT bypass applicable mandatory validation.
#[tokio::test]
async fn trusted_operator_still_needs_current_exact_checks_and_human_grant() {
    let f = operator_fixture().await;
    let s = f.seed().await;
    let prepare = |check_id: &str, grant_id: Option<&str>| Pc::Prepare {
        kind: Kind::Rollout, subject: s.clone(), check_id: check_id.into(),
        grant_id: grant_id.map(str::to_string),
    };
    let mut failed = f.check(&s); failed.results.insert("behavior".into(), false);
    assert_eq!(f.p(9000, "failed-check", Pc::Check(failed)).await.status, "ok");
    assert_eq!(f.p(9000, "grant", Pc::Grant {
        subject: s.clone(), valid_until_ms: f.clock.load(Ordering::SeqCst) + 1000,
    }).await.status, "ok");
    assert_eq!(f.p(9000, "failed-prepare", prepare("failed-check", Some("grant"))).await.status, "failed_or_missing_check");
    assert_eq!(f.p(9000, "good-check", Pc::Check(f.check(&s))).await.status, "ok");
    assert_eq!(f.p(9000, "no-grant", prepare("good-check", None)).await.status, "human_authorization_required");
    let mut changed = s.clone(); changed.target_revision = "other".into();
    assert_eq!(f.p(9000, "wrong-grant", Pc::Grant {
        subject: changed, valid_until_ms: f.clock.load(Ordering::SeqCst) + 1000,
    }).await.status, "subject_mismatch");
    assert_eq!(f.p(9000, "prepared", prepare("good-check", Some("grant"))).await.status, "ok");
    f.clock.fetch_add(1001, Ordering::SeqCst);
    assert_eq!(f.p(9000, "expired-start", Pc::StartRollout {
        action_id: "prepared".into(), subject: s.clone(),
    }).await.status, "human_authorization_mismatch");
    // Removing a logical role does not transfer its historical authority to
    // another role of the same kernel UID.
    let (path, mut config, clock) = f.close().await;
    config.protected_release.as_mut().unwrap().producers.retain(|p| p.role != Role::Checker);
    let controller = Controller::open_with_config(&path, config.clone()).unwrap();
    let f = Fixture { controller, path, config, clock };
    assert_eq!(f.p(9000, "removed-checker", Pc::Check(f.check(&s))).await.status, "wrong_producer_role");
    assert_eq!(f.p(9000, "untrusted-check-prepare", prepare("good-check", Some("grant"))).await.status, "untrusted_check");
}

#[tokio::test]
async fn protected_roles_and_ordinary_claims_never_mint_evidence() {
    let f = Fixture::new();
    let s = f.seed().await;
    let c = f.check(&s);
    assert_eq!(
        f.p(9004, "forged", Pc::Check(c.clone())).await.status,
        "unauthenticated"
    );
    assert_eq!(
        f.p(9002, "wrong-role", Pc::Check(c.clone())).await.status,
        "wrong_producer_role"
    );
    assert_eq!(
        f.p(9001, "repo-check", Pc::Check(c.clone())).await.status,
        "wrong_producer_role"
    );
    let forged = serde_json::json!({"protocol_revision":WIRE_REVISION,"request_id":"forged","operation":"check","input":c});
    assert!(serde_json::from_value::<crate::protocol::Request>(forged).is_err());
    assert_eq!(f.merge(&s, "without-check").await.status, "not_found");
    let Data::ProtectedRelease(v) = call(
        &f.controller,
        Operation::GetProtectedRelease {
            release_id: s.release_id.clone(),
        },
    )
    .await
    else {
        panic!()
    };
    assert!(v.actions.is_empty());
    assert_eq!(
        v.candidate.qualification.checks,
        ProtectedStatus::Unconfigured
    );
    let prepare = serde_json::json!({"operation_id":"x","task_id":"x","expected_task_revision":1,"checkpoint":v.candidate.checkpoint,"artifact":s.artifact,"checks":"passed"});
    assert!(serde_json::from_value::<PrepareReleaseInput>(prepare).is_err());
}
#[tokio::test]
async fn failed_missing_stale_and_wrong_subject_checks_refuse() {
    let f = Fixture::new();
    let s = f.seed().await;
    let mut c = f.check(&s);
    c.results.insert("behavior".into(), false);
    assert_eq!(f.p(9000, "check", Pc::Check(c)).await.status, "ok");
    assert_eq!(f.merge(&s, "bad").await.status, "failed_or_missing_check");
    let mut c = f.check(&s);
    c.results.remove("behavior");
    assert_eq!(f.p(9000, "missing", Pc::Check(c)).await.status, "ok");
    assert_eq!(
        f.p(
            9001,
            "missing-action",
            Pc::Prepare {
                kind: Kind::Merge,
                subject: s.clone(),
                check_id: "missing".into(),
                grant_id: None
            }
        )
        .await
        .status,
        "failed_or_missing_check"
    );
    let mut wrong = s.clone();
    wrong.artifact.source_commit = "b".repeat(40);
    assert_eq!(
        f.p(9000, "wrong", Pc::Check(f.check(&wrong))).await.status,
        "subject_mismatch"
    );
    let mut wrong = s.clone();
    wrong.target_revision = "revision-2".into();
    assert_eq!(
        f.p(9000, "wrong-target", Pc::Check(f.check(&wrong)))
            .await
            .status,
        "subject_mismatch"
    );
    assert_eq!(f.p(9000, "good", Pc::Check(f.check(&s))).await.status, "ok");
    f.clock.fetch_add(30_000, Ordering::SeqCst);
    assert_eq!(
        f.p(
            9001,
            "expired",
            Pc::Prepare {
                kind: Kind::Merge,
                subject: s.clone(),
                check_id: "good".into(),
                grant_id: None
            }
        )
        .await
        .status,
        "stale_check"
    );
}
#[tokio::test]
async fn exact_receipts_replay_after_restart_and_changed_input_refuses() {
    let f = Fixture::new();
    let s = f.seed().await;
    f.checks(&s).await;
    let m = f.merge(&s, "merge").await;
    assert_eq!(m.status, "ok");
    let copy = m.data.clone();
    let mut wrong = s.clone();
    wrong.expected_base = "2".repeat(40);
    assert_eq!(f.merge(&wrong, "merge").await.status, "operation_conflict");
    let (path, config, clock) = f.close().await;
    let controller = Controller::open_with_config(&path, config.clone()).unwrap();
    let f = Fixture {
        controller,
        path,
        config,
        clock,
    };
    let replay = f.merge(&s, "merge").await;
    assert!(replay.replayed);
    assert_eq!(replay.data, copy);
    assert_eq!(
        f.p(
            9001,
            "start:merge",
            Pc::Start {
                action_id: "merge".into()
            }
        )
        .await
        .status,
        "ok"
    );
    assert!(
        f.p(
            9001,
            "start:merge",
            Pc::Start {
                action_id: "merge".into()
            }
        )
        .await
        .replayed
    );
    assert_eq!(
        f.p(
            9001,
            "uncertain",
            Pc::Finish {
                action_id: "merge".into(),
                state: State::Unknown,
                observation: "ACK lost".into()
            }
        )
        .await
        .status,
        "ok"
    );
    let settled = f
        .p(
            9001,
            "reconcile",
            Pc::Finish {
                action_id: "merge".into(),
                state: State::Applied,
                observation: "actual original CAS witness".into(),
            },
        )
        .await;
    assert_eq!(settled.status, "ok");
    let old = f
        .p(
            9001,
            "uncertain",
            Pc::Finish {
                action_id: "merge".into(),
                state: State::Unknown,
                observation: "ACK lost".into(),
            },
        )
        .await;
    assert_eq!(old.data["state"], "unknown");
    assert!(old.replayed);
    let current = f
        .p(
            9000,
            "view",
            Pc::GetAction {
                action_id: "merge".into(),
            },
        )
        .await;
    assert_eq!(current.data["state"], "applied");
}
#[tokio::test]
// Traceability scope: The separate rollout preparation refuses a missing human grant, wrong producer, changed subject and stale grant/check.
//= spec/impl/v2/implementation.md#configured-actions-and-automatic-merge
//= type=test
//# A merge MUST NOT trigger a rollout without the rollout's required human authorization.
async fn human_grant_is_separate_exact_and_never_bypasses_checks() {
    let f = Fixture::new();
    let s = f.seed().await;
    f.checks(&s).await;
    let prep = |id: &str, grant: Option<&str>| Pq {
        revision: 1,
        operation_id: id.into(),
        command: Pc::Prepare {
            kind: Kind::Rollout,
            subject: s.clone(),
            check_id: "check".into(),
            grant_id: grant.map(String::from),
        },
    };
    assert_eq!(
        f.controller
            .protected(9003, prep("no-human", None))
            .await
            .status,
        "human_authorization_required"
    );
    let grant = Pc::Grant {
        subject: s.clone(),
        valid_until_ms: f.clock.load(Ordering::SeqCst) + 20_000,
    };
    assert_eq!(
        f.p(9000, "checker-grant", grant.clone()).await.status,
        "wrong_producer_role"
    );
    assert_eq!(f.p(9002, "grant", grant).await.status, "ok");
    assert_eq!(
        f.controller
            .protected(9003, prep("rollout", Some("grant")))
            .await
            .status,
        "ok"
    );
    let mut wrong = s.clone();
    wrong.artifact.package_sha256 = "f".repeat(64);
    assert_eq!(
        f.p(
            9003,
            "wrong-start",
            Pc::StartRollout {
                action_id: "rollout".into(),
                subject: wrong
            }
        )
        .await
        .status,
        "action_not_startable"
    );
    assert_eq!(
        f.p(
            9003,
            "start-rollout",
            Pc::StartRollout {
                action_id: "rollout".into(),
                subject: s.clone()
            }
        )
        .await
        .status,
        "ok"
    );
    assert_eq!(
        f.p(
            9003,
            "wrong-observe",
            Pc::ObserveRollout {
                action_id: "rollout".into(),
                subject: s.clone(),
                installed_package_sha256: "c".repeat(64),
                observed: true
            }
        )
        .await
        .status,
        "wrong_installed_artifact"
    );
    f.clock.fetch_add(25_000, Ordering::SeqCst);
    assert_eq!(
        f.p(
            9003,
            "observe",
            Pc::ObserveRollout {
                action_id: "rollout".into(),
                subject: s.clone(),
                installed_package_sha256: s.artifact.package_sha256.clone(),
                observed: true
            }
        )
        .await
        .status,
        "ok",
        "historical actual effect must remain recordable after grant expiry"
    );
    assert_eq!(
        f.controller
            .protected(9003, prep("expired-grant", Some("grant")))
            .await
            .status,
        "human_authorization_mismatch"
    );
    f.clock.fetch_add(6_000, Ordering::SeqCst);
    assert_eq!(
        f.controller
            .protected(9003, prep("expired-check", Some("grant")))
            .await
            .status,
        "stale_check"
    );
}
// Traceability scope: A real private Unix socket uses the actual peer UID, accepts its configured checker role and refuses an unauthorized producer role and internal operations.
//= spec/impl/v2/implementation.md#transport-and-access-controls-remain
//= type=test
//# Same-host communication MAY rely on ordinary host access controls.
#[tokio::test]
async fn unix_peer_identity_and_wire_internal_operation_refusal() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let mut f = Fixture::new();
    let s = f.seed().await;
    let (path, mut config, clock) = f.close().await;
    let root = path.parent().unwrap();
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let uid = std::fs::metadata(root).unwrap().uid();
    let c = config.protected_release.as_mut().unwrap();
    c.producers[0].uid = uid;
    let cfg = c.clone();
    let controller = Controller::open_with_config(&path, config.clone()).unwrap();
    f = Fixture {
        controller,
        path,
        config,
        clock,
    };
    let listener = pr::listen(
        &cfg,
        &crate::limits::RuntimeLimits::seed().unwrap().interface,
    )
    .unwrap();
    let ctl = f.controller.clone();
    let join = tokio::spawn(pr::serve(ctl, cfg.clone(), listener));
    let q = Pq {
        revision: 1,
        operation_id: "real-peer-check".into(),
        command: Pc::Check(f.check(&s)),
    };
    assert_eq!(
        pr::call(
            &cfg.socket,
            &q,
            &crate::limits::RuntimeLimits::seed().unwrap().interface
        )
        .await
        .unwrap()
        .status,
        "ok"
    );
    let q = Pq {
        revision: 2,
        operation_id: "bad-revision".into(),
        command: Pc::ReconcileMerge {
            action_id: "x".into(),
        },
    };
    assert!(
        pr::call(
            &cfg.socket,
            &q,
            &crate::limits::RuntimeLimits::seed().unwrap().interface
        )
        .await
        .is_err()
    );
    let q = Pq {
        revision: 1,
        operation_id: "finish:forged".into(),
        command: Pc::Check(f.check(&s)),
    };
    assert!(
        pr::call(
            &cfg.socket,
            &q,
            &crate::limits::RuntimeLimits::seed().unwrap().interface
        )
        .await
        .is_err()
    );
    let q = Pq {
        revision: 1,
        operation_id: "real-peer-grant".into(),
        command: Pc::Grant {
            subject: s,
            valid_until_ms: now() + 10_000,
        },
    };
    assert_eq!(
        pr::call(
            &cfg.socket,
            &q,
            &crate::limits::RuntimeLimits::seed().unwrap().interface
        )
        .await
        .unwrap()
        .status,
        "wrong_producer_role"
    );
    let q = Pq {
        revision: 1,
        operation_id: "forged-internal".into(),
        command: Pc::Start {
            action_id: "x".into(),
        },
    };
    assert!(
        pr::call(
            &cfg.socket,
            &q,
            &crate::limits::RuntimeLimits::seed().unwrap().interface
        )
        .await
        .is_err()
    );
    join.abort();
    let _ = join.await;
}
#[tokio::test]
async fn absent_configuration_fails_closed_and_schema7_refuses_unchanged() {
    let f = Fixture::new();
    let s = f.seed().await;
    let (path, mut config, clock) = f.close().await;
    config.protected_release = None;
    let controller = Controller::open_with_config(&path, config.clone()).unwrap();
    let f = Fixture {
        controller,
        path,
        config,
        clock,
    };
    assert_eq!(
        f.p(9000, "no-config", Pc::Check(f.check(&s))).await.status,
        "unconfigured"
    );
    let (path, config, _) = f.close().await;
    let db = rusqlite::Connection::open(&path).unwrap();
    db.pragma_update(None, "user_version", 7).unwrap();
    drop(db);
    let before = std::fs::read(&path).unwrap();
    assert!(Controller::open_with_config(&path, config).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before);
}

fn git_fixture(path: &std::path::Path, args: &[&str], input: &[u8]) -> String {
    use std::{
        io::Write,
        process::{Command, Stdio},
    };
    let mut cmd = Command::new("/usr/bin/git");
    cmd.arg("--no-replace-objects")
        .arg(format!("--git-dir={}", path.display()))
        .args(["-c", "core.hooksPath=/dev/null"])
        .args(args)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", "/nonexistent")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_AUTHOR_NAME", "fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@invalid")
        .env("GIT_COMMITTER_NAME", "fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@invalid")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().into()
}
fn repository_fixture(f: &Fixture) -> (String, String, String) {
    let c = f.config.protected_release.as_ref().unwrap();
    let p = &c.repository_path;
    git_fixture(p, &["init", "--bare", p.to_str().unwrap()], b"");
    let tree = git_fixture(p, &["mktree"], b"");
    let base = git_fixture(p, &["commit-tree", &tree], b"base\n");
    let head = git_fixture(p, &["commit-tree", &tree, "-p", &base], b"candidate\n");
    git_fixture(p, &["update-ref", "refs/heads/main", &base], b"");
    (base, head, tree)
}
#[tokio::test]
// Traceability scope: Actual local protected Git CAS uses the checked exact subject; reopening reads back that merge. This permission does not require automatic merge or qualify hosted branch policies.
//= spec/impl/v2/implementation.md#configured-actions-and-automatic-merge
//= type=test
//# An agent MAY merge a change after protected checks have passed for its exact revision.
// Traceability scope: The completed merge remains the sole action, automatic_rollout is false and human rollout qualification remains unconfigured.
//= spec/impl/v2/implementation.md#configured-actions-and-automatic-merge
//= type=test
//# A merge MUST NOT trigger a rollout without the rollout's required human authorization.
async fn actual_git_cas_restart_readback_and_no_rollout() {
    let f = Fixture::new();
    let (base, head, tree) = repository_fixture(&f);
    let mut s = f.seed_tree(&head, &tree).await;
    s.expected_base = base;
    f.checks(&s).await;
    assert_eq!(f.merge(&s, "merge").await.status, "ok");
    let start = f
        .p(
            9001,
            "start:merge",
            Pc::Start {
                action_id: "merge".into(),
            },
        )
        .await;
    let a: Action = serde_json::from_value(start.data).unwrap();
    let c = f.config.protected_release.as_ref().unwrap();
    let (state, _) = pr::repository::execute(
        c,
        &a,
        &crate::limits::RuntimeLimits::seed().unwrap().interface,
    )
    .await;
    assert_eq!(state, State::Applied);
    // Crash/lost result after actual CAS, before recording its outcome.
    let (path, config, clock) = f.close().await;
    let controller = Controller::open_with_config(&path, config.clone()).unwrap();
    let f = Fixture {
        controller,
        path,
        config,
        clock,
    };
    let current = f
        .p(
            9001,
            "current",
            Pc::GetAction {
                action_id: "merge".into(),
            },
        )
        .await;
    let a: Action = serde_json::from_value(current.data).unwrap();
    assert_eq!(a.state, State::Started);
    let c = f.config.protected_release.as_ref().unwrap();
    let (state, observation) = pr::repository::observe(
        c,
        &a,
        &crate::limits::RuntimeLimits::seed().unwrap().interface,
    )
    .await;
    assert_eq!(state, State::Applied);
    let end = f
        .p(
            9001,
            "settle",
            Pc::Finish {
                action_id: "merge".into(),
                state,
                observation,
            },
        )
        .await;
    assert_eq!(end.status, "ok");
    let Data::ProtectedRelease(v) = call(
        &f.controller,
        Operation::GetProtectedRelease {
            release_id: s.release_id,
        },
    )
    .await
    else {
        panic!()
    };
    assert_eq!(v.actions.len(), 1);
    assert_eq!(v.actions[0].kind, Kind::Merge);
    assert!(!v.automatic_rollout);
    assert_eq!(
        v.candidate.qualification.human_rollout,
        ProtectedStatus::Unconfigured
    );
}
#[tokio::test]
async fn changed_base_and_wrong_tree_never_move_the_branch() {
    let f = Fixture::new();
    let (base, head, tree) = repository_fixture(&f);
    let mut s = f.seed_tree(&head, &tree).await;
    s.expected_base = base.clone();
    f.checks(&s).await;
    f.merge(&s, "merge").await;
    let start = f
        .p(
            9001,
            "start:merge",
            Pc::Start {
                action_id: "merge".into(),
            },
        )
        .await;
    let a: Action = serde_json::from_value(start.data).unwrap();
    let c = f.config.protected_release.as_ref().unwrap();
    let other = git_fixture(
        &c.repository_path,
        &["commit-tree", &tree, "-p", &base],
        b"different\n",
    );
    git_fixture(
        &c.repository_path,
        &["update-ref", "refs/heads/main", &other, &base],
        b"",
    );
    assert_eq!(
        pr::repository::execute(
            c,
            &a,
            &crate::limits::RuntimeLimits::seed().unwrap().interface
        )
        .await
        .0,
        State::Refused
    );
    assert_eq!(
        git_fixture(&c.repository_path, &["rev-parse", "refs/heads/main"], b""),
        other
    );
    assert_eq!(
        pr::repository::observe(
            c,
            &a,
            &crate::limits::RuntimeLimits::seed().unwrap().interface
        )
        .await
        .0,
        State::Unknown,
        "absence is not proof of nonexecution"
    );
    git_fixture(
        &c.repository_path,
        &["update-ref", "refs/heads/main", &base, &other],
        b"",
    );
    let mut wrong = a.clone();
    wrong.subject.artifact.source_tree = "c".repeat(40);
    assert_eq!(
        pr::repository::execute(
            c,
            &wrong,
            &crate::limits::RuntimeLimits::seed().unwrap().interface
        )
        .await
        .0,
        State::Refused
    );
    assert_eq!(
        git_fixture(&c.repository_path, &["rev-parse", "refs/heads/main"], b""),
        base
    );
}
#[tokio::test]
async fn agent_directory_pagination_preserves_canonical_identity() {
    let limits = &crate::limits::RuntimeLimits::seed().expect("test metadata");
    let f = Fixture::new();
    let make = |id: &str| TaskInput { instruction_source: None,
        pull_request: None,
        resolved_limits: None,
        instruction: "canonical directory fixture".into(),
        definition: Definition {
            charter: None,
            limit_overrides: None,
            id: id.into(),
            revision: "v1".into(),
            executor: ExecutorKind::Native,
            inference: Some(InferenceSettings::selected()),
            execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
        },
        repository_read: None,
        repository: None,
        acceptance_commands: vec![],
        native_workflow: None,
        output_schema: Some(serde_json::json!({"type":"object"})),
        dependencies: vec![],
        limits: Limits {
            deadline_unix_ms: now() + 60_000,
            model_requests: 2,
            output_bytes: 4096,
            result_bytes: crate::limits::RuntimeLimits::seed()
                .unwrap()
                .task_defaults
                .result_bytes,
            max_delegation_depth: 1,
        },
    };
    let Data::Task(a) = call(
        &f.controller,
        Operation::SubmitTask {
            operation_id: "agent-first".into(),
            input: make("agent-1"),
        },
    )
    .await
    else {
        panic!()
    };
    let Data::Task(b) = call(
        &f.controller,
        Operation::SubmitTask {
            operation_id: "agent-second".into(),
            input: make("agent-2"),
        },
    )
    .await
    else {
        panic!()
    };
    let Data::Agents(first) = call(
        &f.controller,
        Operation::ListAgents {
            after: None,
            limit: 1,
        },
    )
    .await
    else {
        panic!()
    };
    let Data::Agents(next) = call(
        &f.controller,
        Operation::ListAgents {
            after: Some(first[0].id.clone()),
            limit: 1,
        },
    )
    .await
    else {
        panic!()
    };
    assert_eq!(first[0].id, "agent-1");
    assert_eq!(next[0].id, "agent-2");
    assert_eq!(first[0].id, first[0].definition.id);
    assert_eq!(next[0].id, next[0].definition.id);

    for t in [&a, &b] {
        let Data::Tasks(rows) = call(
            &f.controller,
            Operation::ListAgentTasks {
                agent_id: t.assigned_agent_id.clone(),
                after: None,
                limit: 1,
            },
        )
        .await
        else {
            panic!()
        };
        assert_eq!(rows[0].id, t.id);
        assert_eq!(rows[0].input.definition.id, t.assigned_agent_id);
    }
    // Pagination above observes both original owners. The current accepted
    // handoff creates a new session for the same idle task, not a refusal.
    let r = f
        .controller
        .handle(crate::protocol::Request {
            protocol_revision: WIRE_REVISION,
            request_id: "reassign".into(),
            command: Operation::AssignTask {
                operation_id: "reassign".into(),
                task_id: a.id.clone(),
                expected_revision: a.revision,
                agent_id: "agent-2".into(),
            },
        })
        .await;
    let Outcome::Ok { data: Data::Task(reassigned) } = r.outcome else {
        panic!("idle same-executor reassignment must be accepted: {:?}", r.outcome);
    };
    assert_eq!(reassigned.id, a.id);
    assert_ne!(reassigned.session_id, a.session_id);
    assert_eq!(reassigned.assigned_agent_id, b.assigned_agent_id);
    assert_eq!(reassigned.input.definition, b.input.definition);
    assert_eq!(reassigned.input.instruction, a.input.instruction);
    assert_eq!(reassigned.input.output_schema, a.input.output_schema);
    assert_eq!(reassigned.input.dependencies, a.input.dependencies);
    assert_eq!(reassigned.input.limits, a.input.limits);
    assert_eq!(reassigned.status, TaskStatus::Queued);
    assert!(reassigned.pending_assignment.is_none());
}

#[tokio::test]
async fn terminal_action_capacity_is_reserved_against_ordinary_producer_traffic() {
    let f = Fixture::new();
    let s = f.seed().await;
    let (path, mut config, clock) = f.close().await;
    config.protected_release.as_mut().unwrap().max_records = 4;
    let controller = Controller::open_with_config(&path, config.clone()).unwrap();
    let f = Fixture {
        controller,
        path,
        config,
        clock,
    };
    f.checks(&s).await;
    assert_eq!(f.merge(&s, "merge").await.status, "ok");
    assert_eq!(
        f.p(9000, "filler", Pc::Check(f.check(&s))).await.status,
        "capacity"
    );
    assert_eq!(
        f.p(
            9001,
            "start",
            Pc::Start {
                action_id: "merge".into()
            }
        )
        .await
        .status,
        "ok"
    );
    assert_eq!(
        f.p(
            9001,
            "final",
            Pc::Finish {
                action_id: "merge".into(),
                state: State::Applied,
                observation: "exact protected readback fixture".into()
            }
        )
        .await
        .status,
        "ok"
    );
    assert_eq!(
        f.p(
            9000,
            "view",
            Pc::GetAction {
                action_id: "merge".into()
            }
        )
        .await
        .data["state"],
        "applied"
    );
}

#[tokio::test]
async fn actual_fixed_package_checker_cli_reaches_kernel_authenticated_producer() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let f = Fixture::new();
    let descriptor = std::fs::read(std::env::var("BORG_TEST_PACKAGE_DESCRIPTOR").unwrap()).unwrap();
    let artifact: ReleaseArtifact = serde_json::from_slice(&descriptor).unwrap();
    let s = f.seed_artifact(artifact).await;
    let (path, mut config, clock) = f.close().await;
    let root = path.parent().unwrap();
    let uid = std::fs::metadata(root).unwrap().uid();
    let c = config.protected_release.as_mut().unwrap();
    c.producers[0].uid = uid;
    c.required_checks = vec!["package_integrity".into()];
    let cfg = c.clone();
    config.clock = Arc::new(now);
    let controller = Controller::open_with_config(&path, config.clone()).unwrap();
    let f = Fixture {
        controller,
        path: path.clone(),
        config,
        clock,
    };
    let listener = pr::listen(
        &cfg,
        &crate::limits::RuntimeLimits::seed().unwrap().interface,
    )
    .unwrap();
    let join = tokio::spawn(pr::serve(f.controller.clone(), cfg.clone(), listener));
    let config_file = root.join("producer.json");
    std::fs::write(&config_file,serde_json::to_vec(&serde_json::json!({"socket":cfg.socket,"server_uid":uid,"producer_uid":uid,"policy":cfg.policy,"recipe_sha256":cfg.recipe_sha256,"valid_for_ms":30000})).unwrap()).unwrap();
    std::fs::set_permissions(&config_file, std::fs::Permissions::from_mode(0o600)).unwrap();
    let subject_file = root.join("subject.json");
    std::fs::write(&subject_file, serde_json::to_vec(&s).unwrap()).unwrap();
    let installed = root.join("installed-checker");
    std::fs::create_dir(&installed).unwrap();
    std::fs::set_permissions(&installed, std::fs::Permissions::from_mode(0o700)).unwrap();
    for name in ["protected_release_v2.py", "package_v2.py"] {
        let to = installed.join(name);
        std::fs::copy(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tooling")
                .join(name),
            &to,
        )
        .unwrap();
        std::fs::set_permissions(to, std::fs::Permissions::from_mode(0o500)).unwrap();
    }
    let cli = installed.join("protected_release_v2.py");
    let output = tokio::process::Command::new("/usr/bin/python3")
        .args(["-I", "-S", "-B"])
        .arg(&cli)
        .arg("check-package")
        .arg("--config")
        .arg(&config_file)
        .arg("--input")
        .arg(&subject_file)
        .args(["--operation-id", "real-package-check"])
        .arg("--package")
        .arg(std::env::var("BORG_TEST_PACKAGE").unwrap())
        .arg("--receipt")
        .arg(root.join("checker-receipt"))
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let record = f
        .p(
            9001,
            "lookup",
            Pc::GetReceipt {
                operation_id: "real-package-check".into(),
            },
        )
        .await;
    assert_eq!(record.status, "ok");
    assert_eq!(
        record.data["request"]["command"]["input"]["results"],
        serde_json::json!({"package_integrity":true})
    );
    for mode in ["lookup", "retry"] {
        let output = tokio::process::Command::new("/usr/bin/python3")
            .args(["-I", "-S", "-B"])
            .arg(&cli)
            .arg(mode)
            .arg("--config")
            .arg(&config_file)
            .arg("--original-receipt")
            .arg(root.join("checker-receipt"))
            .args(["--operation-id", "real-package-check"])
            .arg("--receipt")
            .arg(root.join(format!("checker-{mode}")))
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let replay: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("checker-retry/receipt.json")).unwrap())
            .unwrap();
    assert_eq!(replay["reply"]["replayed"], true);
    assert_eq!(
        std::fs::read(root.join("checker-retry/request.json")).unwrap(),
        std::fs::read(root.join("checker-receipt/request.json")).unwrap()
    );
    // Full behavior policy remains deliberately absent; the fixed producer cannot
    // claim repository test/compile results or protected production qualification.
    assert_eq!(
        record.data["request"]["command"]["input"]["subject"]["artifact"]["package_sha256"],
        s.artifact.package_sha256
    );
    join.abort();
    let _ = join.await;
}

#[tokio::test]
async fn public_merge_and_reconciliation_replay_original_outcomes_without_repeat_cas() {
    use std::os::unix::fs::MetadataExt;
    let f = Fixture::new();
    let (base, head, tree) = repository_fixture(&f);
    let mut s = f.seed_tree(&head, &tree).await;
    s.expected_base = base.clone();
    f.checks(&s).await;
    assert_eq!(f.merge(&s, "merge").await.status, "ok");
    let (path, mut config, clock) = f.close().await;
    let uid = std::fs::metadata(path.parent().unwrap()).unwrap().uid();
    config.protected_release.as_mut().unwrap().producers[1].uid = uid;
    let cfg = config.protected_release.as_ref().unwrap().clone();
    let controller = Controller::open_with_config(&path, config.clone()).unwrap();
    let f = Fixture {
        controller,
        path,
        config,
        clock,
    };
    let join = tokio::spawn(pr::serve(
        f.controller.clone(),
        cfg.clone(),
        pr::listen(
            &cfg,
            &crate::limits::RuntimeLimits::seed().unwrap().interface,
        )
        .unwrap(),
    ));
    let q = Pq {
        revision: 1,
        operation_id: "public-execute".into(),
        command: Pc::ExecuteMerge {
            action_id: "merge".into(),
        },
    };
    let first = pr::call(
        &cfg.socket,
        &q,
        &crate::limits::RuntimeLimits::seed().unwrap().interface,
    )
    .await
    .unwrap();
    assert_eq!(first.status, "ok");
    assert_eq!(first.data["state"], "started");
    let replay = pr::call(
        &cfg.socket,
        &q,
        &crate::limits::RuntimeLimits::seed().unwrap().interface,
    )
    .await
    .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.data, first.data);
    let r = Pq {
        revision: 1,
        operation_id: "public-reconcile".into(),
        command: Pc::ReconcileMerge {
            action_id: "merge".into(),
        },
    };
    let observed = pr::call(
        &cfg.socket,
        &r,
        &crate::limits::RuntimeLimits::seed().unwrap().interface,
    )
    .await
    .unwrap();
    assert_eq!(observed.data["state"], "applied");
    let replay = pr::call(
        &cfg.socket,
        &r,
        &crate::limits::RuntimeLimits::seed().unwrap().interface,
    )
    .await
    .unwrap();
    assert!(replay.replayed);
    assert_eq!(observed.data, replay.data);
    let history = git_fixture(
        &cfg.repository_path,
        &["reflog", "show", "--format=%gs", "refs/heads/main"],
        b"",
    );
    assert_eq!(
        history
            .lines()
            .filter(|l| l.starts_with("borg-release:merge:"))
            .count(),
        1
    );
    let mut conflict = r.clone();
    conflict.command = Pc::ReconcileMerge {
        action_id: "different".into(),
    };
    assert_eq!(
        pr::call(
            &cfg.socket,
            &conflict,
            &crate::limits::RuntimeLimits::seed().unwrap().interface
        )
        .await
        .unwrap()
        .status,
        "operation_conflict"
    );
    join.abort();
    let _ = join.await;
    std::fs::remove_file(&cfg.socket).unwrap();
    let (path, config, clock) = f.close().await;
    let controller = Controller::open_with_config(&path, config.clone()).unwrap();
    let f = Fixture {
        controller,
        path,
        config,
        clock,
    };
    let join = tokio::spawn(pr::serve(
        f.controller.clone(),
        cfg.clone(),
        pr::listen(
            &cfg,
            &crate::limits::RuntimeLimits::seed().unwrap().interface,
        )
        .unwrap(),
    ));
    let replay = pr::call(
        &cfg.socket,
        &q,
        &crate::limits::RuntimeLimits::seed().unwrap().interface,
    )
    .await
    .unwrap();
    assert!(replay.replayed);
    assert_eq!(first.data, replay.data);
    let replay = pr::call(
        &cfg.socket,
        &r,
        &crate::limits::RuntimeLimits::seed().unwrap().interface,
    )
    .await
    .unwrap();
    assert!(replay.replayed);
    assert_eq!(observed.data, replay.data);
    join.abort();
    let _ = join.await;
}

#[tokio::test]
async fn stage_view_refuses_incrementally_when_retained_records_exceed_response_bound() {
    let f = Fixture::new();
    let s = f.seed().await;
    let mut c = f.check(&s);
    c.results.insert("x".repeat(30_000), false);
    for i in 0..5 {
        assert_eq!(
            f.p(9000, &format!("large-{i}"), Pc::Check(c.clone()))
                .await
                .status,
            "ok"
        );
    }
    let r = f
        .controller
        .handle(crate::protocol::Request {
            protocol_revision: WIRE_REVISION,
            request_id: "bounded-stage".into(),
            command: Operation::GetProtectedRelease {
                release_id: s.release_id,
            },
        })
        .await;
    assert!(matches!(r.outcome,Outcome::Refused{ref code,..} if code=="response_bound"));
}

#[tokio::test]
async fn oversized_original_lookup_envelope_is_refused_before_commit() {
    let f = Fixture::new();
    let s = f.seed().await;
    let mut c = f.check(&s);
    c.results.insert("x".repeat(70_000), false);
    assert_eq!(
        f.p(9000, "too-wide", Pc::Check(c)).await.status,
        "response_bound"
    );
    assert_eq!(
        f.p(
            9000,
            "lookup-wide",
            Pc::GetReceipt {
                operation_id: "too-wide".into()
            }
        )
        .await
        .status,
        "not_found"
    );
}

#[tokio::test]
async fn protected_limit_refusal_preserves_status_field_and_original_replay() {
    let f = Fixture::new();
    let subject = f.seed().await;
    let mut check = f.check(&subject);
    check.valid_until_ms = check.checked_at_ms
        + f.config
            .protected_release
            .as_ref()
            .unwrap()
            .max_check_age_ms
        + 1;
    let first = f.p(9000, "limited-check", Pc::Check(check.clone())).await;
    assert_eq!(first.status, "invalid_check");
    assert_eq!(
        first.data["limiting_field"],
        "protected_release.max_check_age_ms"
    );
    assert!(!first.replayed);
    let (path, config, clock) = f.close().await;
    let controller = Controller::open_with_config(&path, config.clone()).unwrap();
    let f = Fixture {
        controller,
        path,
        config,
        clock,
    };
    let replay = f.p(9000, "limited-check", Pc::Check(check)).await;
    assert_eq!(replay.status, first.status);
    assert_eq!(replay.data, first.data);
    assert!(replay.replayed);
    let retained = f
        .p(
            9000,
            "read-limited-check",
            Pc::GetReceipt {
                operation_id: "limited-check".into(),
            },
        )
        .await;
    assert_eq!(retained.status, "ok");
    assert_eq!(retained.data["reply"]["data"], first.data);
}

#[test]
fn protected_limit_diagnostic_frames_are_closed_and_fit_serializer_minimum() {
    let minimum = pr::minimum_limit_refusal_bytes().unwrap();
    let known = pr::refusal_with_limit(
        "response_bound",
        "original receipt lookup envelope exceeds bound: interface.protected_reply_bytes",
    );
    assert_eq!(known.status, "response_bound");
    assert_eq!(
        known.data["limiting_field"],
        "interface.protected_reply_bytes"
    );
    assert!(serde_json::to_vec(&known).unwrap().len() <= minimum);
    let unrelated = pr::refusal_with_limit(
        "invalid_input",
        "candidate-controlled secret: interface.protected_reply_bytes",
    );
    assert_eq!(unrelated.data, serde_json::Value::Null);
}

// Actual canonical controller/SQLite and kernel-authenticated producer transport.
// Python uses real package/fsync/journals with an explicitly controlled host
// service seam. This is not installed UID separation or systemd/SSH proof.
#[tokio::test]
async fn actual_rollout_adapter_original_receipts_and_no_effect_retry() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let f = Fixture::new();
    let artifact: ReleaseArtifact = serde_json::from_slice(
        &std::fs::read(std::env::var("BORG_TEST_PACKAGE_DESCRIPTOR").unwrap()).unwrap(),
    )
    .unwrap();
    let s = f.seed_artifact(artifact).await;
    let (path, mut config, clock) = f.close().await;
    let root = path.parent().unwrap();
    let uid = std::fs::metadata(root).unwrap().uid();
    let c = config.protected_release.as_mut().unwrap();
    c.producers[3].uid = uid;
    let cfg = c.clone();
    config.clock = Arc::new(now);
    let f = Fixture {
        controller: Controller::open_with_config(&path, config.clone()).unwrap(),
        path: path.clone(),
        config,
        clock,
    };
    let listener = pr::listen(
        &cfg,
        &crate::limits::RuntimeLimits::seed().unwrap().interface,
    )
    .unwrap();
    let join = tokio::spawn(pr::serve(f.controller.clone(), cfg.clone(), listener));
    for loss in ["none", "before", "after"] {
        let stamp = now();
        let check_id = format!("adapter-check-{loss}");
        let grant_id = format!("adapter-grant-{loss}");
        let action_id = format!("adapter-rollout-{loss}");
        let mut check = f.check(&s);
        check.checked_at_ms = stamp;
        check.valid_until_ms = stamp + 30_000;
        assert_eq!(f.p(9000, &check_id, Pc::Check(check)).await.status, "ok");
        assert_eq!(
            f.p(
                9002,
                &grant_id,
                Pc::Grant {
                    subject: s.clone(),
                    valid_until_ms: stamp + 30_000
                }
            )
            .await
            .status,
            "ok"
        );
        let prepared = f
            .p(
                uid,
                &action_id,
                Pc::Prepare {
                    kind: Kind::Rollout,
                    subject: s.clone(),
                    check_id: check_id.clone(),
                    grant_id: Some(grant_id.clone()),
                },
            )
            .await;
        assert_eq!(prepared.status, "ok");
        let input = root.join(format!("adapter-input-{loss}.json"));
        let output = root.join(format!("adapter-output-{loss}"));
        std::fs::write(&input, serde_json::to_vec(&serde_json::json!({
            "subject":s, "controller":cfg,
            "producer":{"socket":cfg.socket,"server_uid":uid,"producer_uid":uid,"policy":cfg.policy,"recipe_sha256":cfg.recipe_sha256,"valid_for_ms":30000},
            "package":std::env::var("BORG_TEST_PACKAGE").unwrap(),
            "output":output,"action_id":action_id,"loss":loss
        })).unwrap()).unwrap();
        std::fs::set_permissions(&input, std::fs::Permissions::from_mode(0o600)).unwrap();
        let mut command = tokio::process::Command::new("/usr/bin/python3");
        command
            .args(["-I", "-S", "-B"])
            .arg(
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/tooling/run_rollout_integration.py"),
            )
            .arg(&input)
            .kill_on_drop(true);
        let result = tokio::time::timeout(std::time::Duration::from_secs(90), command.output())
            .await
            .unwrap()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let receipt: serde_json::Value =
            serde_json::from_slice(&std::fs::read(output.join("integration-result.json")).unwrap())
                .unwrap();
        assert_eq!(receipt["no_effect_retry"], true);
        let start = f
            .p(
                uid,
                "adapter-lookup",
                Pc::GetReceipt {
                    operation_id: format!("{action_id}:start"),
                },
            )
            .await;
        if loss == "before" {
            assert_eq!(start.status, "not_found");
        } else {
            assert_eq!(start.status, "ok");
            assert_eq!(
                start.data["request"]["command"]["operation"],
                "start_rollout"
            );
            assert_eq!(start.data["reply"]["status"], "ok");
            assert_eq!(start.data["reply"]["data"]["state"], "started");
            assert_eq!(
                start.data["reply"]["data"]["subject"],
                serde_json::to_value(&s).unwrap()
            );
        }
        let current = f
            .p(
                uid,
                "adapter-current",
                Pc::GetAction {
                    action_id: action_id.clone(),
                },
            )
            .await;
        assert_eq!(current.status, "ok");
        assert_eq!(
            current.data["state"],
            if loss == "none" {
                "applied"
            } else if loss == "before" {
                "prepared"
            } else {
                "unknown"
            }
        );
    }
    let before = f
        .p(
            uid,
            "before-reopen",
            Pc::GetAction {
                action_id: "adapter-rollout-none".into(),
            },
        )
        .await;
    join.abort();
    let _ = join.await;
    let (path, config, _) = f.close().await;
    let controller = Controller::open_with_config(&path, config).unwrap();
    let after = controller
        .protected(
            uid,
            Pq {
                revision: 1,
                operation_id: "after-reopen".into(),
                command: Pc::GetAction {
                    action_id: "adapter-rollout-none".into(),
                },
            },
        )
        .await;
    assert_eq!(before.data, after.data);
}
