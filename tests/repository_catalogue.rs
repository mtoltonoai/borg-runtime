//! Canonical metadata mutations and actual local Git reads; no provider or fetch.
use borg_runtime::{
    collaboration::{Query, QueryResult},
    controller::{Controller, ControllerConfig},
    protocol::*,
    repository_catalogue::{Entry, Registration, Revision},
    repository_view::{self, SourceSelector},
};
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
static NEXT: AtomicU64 = AtomicU64::new(1);
fn id(prefix: &str) -> String {
    format!("{prefix}-{}", NEXT.fetch_add(1, Ordering::Relaxed))
}
fn root() -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "borg-catalogue-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        id("root")
    ));
    std::fs::create_dir(&path).unwrap();
    path
}
fn config() -> ControllerConfig {
    ControllerConfig {
        validation_executable: env!("CARGO_BIN_EXE_borg-runtime").into(),
        ..ControllerConfig::default()
    }
}
async fn raw(c: &Controller, operation: Operation) -> Reply {
    c.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: id("read"),
        command: operation,
    })
    .await
}
fn entry(reply: Reply) -> Entry {
    match reply.outcome {
        Outcome::Ok {
            data: Data::RepositoryRegistration(v),
        } => v,
        other => panic!("expected registration: {other:?}"),
    }
}
fn task(reply: Reply) -> Task {
    match reply.outcome {
        Outcome::Ok {
            data: Data::Task(v),
        } => *v,
        other => panic!("expected task: {other:?}"),
    }
}
fn refused(reply: Reply, code: &str) {
    assert!(
        matches!(reply.outcome,Outcome::Refused{code:ref got,..} if got==code),
        "{reply:?}"
    );
}
fn registration(
    repository_id: &str,
    revision: &str,
    source: &str,
    checkpoint: &str,
) -> Registration {
    Registration {
        pull_request_target: None,
        repository_id: repository_id.into(),
        registration_revision: revision.into(),
        display_name: format!("Repository {repository_id}"),
        source_endpoint: format!("https://example.invalid/{source}"),
        checkpoint_endpoint: format!("https://example.invalid/{checkpoint}"),
        source_store_key: source.into(),
        checkpoint_store_key: checkpoint.into(),
    }
}
async fn register(c: &Controller, value: &Registration) -> Entry {
    entry(
        raw(
            c,
            Operation::RegisterRepository {
                operation_id: id("register"),
                registration: value.clone(),
            },
        )
        .await,
    )
}
fn input(binding: Revision, commit: &str, coding: bool) -> TaskInput {
    let limits = borg_runtime::limits::RuntimeLimits::seed().unwrap();
    let deadline = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 600_000;
    TaskInput {
        instruction_source: None,
        pull_request: None,
        resolved_limits: None,
        instruction: "Inspect the configured repository".into(),
        definition: Definition {
            charter: None,
            limit_overrides: None,
            execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
            inference: Some(InferenceSettings::selected()),
            id: id("agent"),
            revision: "1".into(),
            executor: if coding {
                ExecutorKind::Coding
            } else {
                ExecutorKind::Native
            },
        },
        repository: if coding {
            Some(Repository {
                pull_request_target: None,
                registration: Some(binding.clone()),
                url: String::new(),
                start_commit: commit.into(),
                checkpoint_remote: String::new(),
            })
        } else {
            None
        },
        repository_read: if coding {
            None
        } else {
            Some(RepositoryReadSource {
                registration: Some(binding),
                repository: String::new(),
                start_commit: commit.into(),
            })
        },
        acceptance_commands: if coding {
            vec![vec!["true".into()]]
        } else {
            vec![]
        },
        output_schema: if coding {
            None
        } else {
            Some(json!({"type":"string"}))
        },
        limits: limits.task_defaults.at(deadline).unwrap(),
        dependencies: vec![],
        native_workflow: None,
    }
}
async fn submit(c: &Controller, value: TaskInput) -> Task {
    task(
        raw(
            c,
            Operation::SubmitTask {
                operation_id: id("submit"),
                input: value,
            },
        )
        .await,
    )
}
async fn reopen(path: &Path, cfg: ControllerConfig) -> Controller {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match Controller::open_with_config(path, cfg.clone()) {
                Ok(c) => return c,
                Err(e) if e.contains("owned") || e.contains("lock") => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                Err(e) => panic!("reopen: {e}"),
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
// Traceability scope: Real controller receipts, stale head CAS and historical revision lookup across restart; no concurrent-writer or browser qualification.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//= type=test
//# A repository registration MUST have a stable repository identity.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//= type=test
//# A repository registration revision MUST be immutable.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//= type=test
//# A repository-catalogue update MUST compare the current repository head generation supplied by the operator.
async fn exact_registration_receipts_head_cas_and_immutable_revisions_survive_restart() {
    let root = root();
    let db = root.join("task.db");
    let cfg = config();
    let c = Controller::open_with_config(&db, cfg.clone()).unwrap();
    let first = registration("alpha", "r1", "source1.git", "checkpoint1.git");
    let op = Operation::RegisterRepository {
        operation_id: "register-alpha".into(),
        registration: first.clone(),
    };
    let receipt = raw(&c, op.clone()).await;
    let replay = raw(&c, op).await;
    assert_eq!(receipt.outcome, replay.outcome);
    assert_eq!(receipt.commit, replay.commit);
    assert_eq!(entry(receipt).generation, 1);
    let disabled = entry(
        raw(
            &c,
            Operation::SetRepositoryEnabled {
                operation_id: id("disable"),
                repository_id: "alpha".into(),
                expected_generation: 1,
                enabled_for_new_tasks: false,
            },
        )
        .await,
    );
    assert_eq!(disabled.generation, 2);
    assert!(!disabled.enabled_for_new_tasks);
    let second = registration("alpha", "r2", "source2.git", "checkpoint2.git");
    refused(
        raw(
            &c,
            Operation::ReviseRepository {
                operation_id: id("stale-revise"),
                expected_generation: 1,
                registration: second.clone(),
            },
        )
        .await,
        "revision_conflict",
    );
    refused(
        raw(
            &c,
            Operation::SetRepositoryEnabled {
                operation_id: id("stale-toggle"),
                repository_id: "alpha".into(),
                expected_generation: 1,
                enabled_for_new_tasks: true,
            },
        )
        .await,
        "revision_conflict",
    );
    let changed = entry(
        raw(
            &c,
            Operation::ReviseRepository {
                operation_id: id("revise"),
                expected_generation: 2,
                registration: second.clone(),
            },
        )
        .await,
    );
    assert_eq!(changed.generation, 3);
    assert!(!changed.enabled_for_new_tasks);
    let historical = entry(
        raw(
            &c,
            Operation::GetRepository {
                repository_id: "alpha".into(),
                registration_revision: Some("r1".into()),
            },
        )
        .await,
    );
    assert_eq!(historical.source_endpoint, first.source_endpoint);
    assert_eq!(historical.current_revision, "r2");
    let public = serde_json::to_string(&historical).unwrap();
    assert!(!public.contains("store_key"));
    assert!(!public.contains(root.to_str().unwrap()));
    drop(c);
    let c = reopen(&db, cfg).await;
    assert_eq!(
        entry(
            raw(
                &c,
                Operation::GetRepository {
                    repository_id: "alpha".into(),
                    registration_revision: Some("r1".into())
                }
            )
            .await
        ),
        historical
    );
    refused(
        raw(
            &c,
            Operation::ReviseRepository {
                operation_id: id("rewrite"),
                expected_generation: 3,
                registration: first,
            },
        )
        .await,
        "repository_conflict",
    );
    drop(c);
}

#[tokio::test]
// Traceability scope: Typed admission and retained task input across revision and disable; distinct source/checkpoint endpoints; no actual provider execution.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//= type=test
//# An accepted task MUST retain its selected repository registration revision.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//= type=test
//# An accepted repository association MUST distinguish the source endpoint from the checkpoint destination.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//= type=test
//# A newly resolved repository association MUST reject a repository registration disabled for new tasks.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//= type=test
//# A catalogue revision or enable-state change MUST NOT change an accepted task's repository association.
async fn admission_rejects_missing_or_forged_projection_and_retains_r1_after_r2_disable() {
    let root = root();
    let c = Controller::open_with_config(root.join("task.db"), config()).unwrap();
    let first = registration("alpha", "r1", "source1.git", "checkpoint1.git");
    register(&c, &first).await;
    let supplied = input(first.binding(), &"a".repeat(40), true);
    let mut missing = supplied.clone();
    missing.repository.as_mut().unwrap().registration = None;
    refused(
        raw(
            &c,
            Operation::SubmitTask {
                operation_id: id("missing"),
                input: missing,
            },
        )
        .await,
        "repository_binding_required",
    );
    let mut forged = supplied.clone();
    forged.repository.as_mut().unwrap().url = first.source_endpoint.clone();
    refused(
        raw(
            &c,
            Operation::SubmitTask {
                operation_id: id("forged"),
                input: forged,
            },
        )
        .await,
        "invalid_input",
    );
    let accepted = submit(&c, supplied.clone()).await;
    assert_eq!(
        accepted.input.repository.as_ref().unwrap().url,
        first.source_endpoint
    );
    assert_eq!(
        accepted
            .input
            .repository
            .as_ref()
            .unwrap()
            .checkpoint_remote,
        first.checkpoint_endpoint
    );
    assert_eq!(
        accepted.input.repository.as_ref().unwrap().registration,
        Some(first.binding())
    );
    let second = registration("alpha", "r2", "source2.git", "checkpoint2.git");
    entry(
        raw(
            &c,
            Operation::ReviseRepository {
                operation_id: id("revise"),
                expected_generation: 1,
                registration: second.clone(),
            },
        )
        .await,
    );
    refused(
        raw(
            &c,
            Operation::SubmitTask {
                operation_id: id("stale-input"),
                input: supplied,
            },
        )
        .await,
        "repository_revision_changed",
    );
    entry(
        raw(
            &c,
            Operation::SetRepositoryEnabled {
                operation_id: id("disable"),
                repository_id: "alpha".into(),
                expected_generation: 2,
                enabled_for_new_tasks: false,
            },
        )
        .await,
    );
    refused(
        raw(
            &c,
            Operation::SubmitTask {
                operation_id: id("disabled-input"),
                input: input(second.binding(), &"b".repeat(40), true),
            },
        )
        .await,
        "repository_disabled",
    );
    assert_eq!(
        task(
            raw(
                &c,
                Operation::GetTask {
                    task_id: accepted.id.clone()
                }
            )
            .await
        )
        .input,
        accepted.input
    );
}

#[tokio::test]
async fn catalogue_bounds_refuse_without_partial_revision_or_head_change() {
    let root = root();
    let mut cfg = config();
    let limits = Arc::make_mut(&mut cfg.limits);
    limits.repository.catalogue_entries = 1;
    limits.repository.catalogue_revisions = 1;
    let c = Controller::open_with_config(root.join("task.db"), cfg).unwrap();
    let first = registration("alpha", "r1", "source.git", "checkpoint.git");
    register(&c, &first).await;
    let result = raw(
        &c,
        Operation::RegisterRepository {
            operation_id: id("capacity"),
            registration: registration("beta", "r1", "other.git", "other.git"),
        },
    )
    .await;
    assert!(
        matches!(result.outcome,Outcome::Refused{ref code,ref message} if code=="capacity" && message.contains("repository.catalogue_entries"))
    );
    let result = raw(
        &c,
        Operation::ReviseRepository {
            operation_id: id("revisions"),
            expected_generation: 1,
            registration: registration("alpha", "r2", "other.git", "other.git"),
        },
    )
    .await;
    assert!(
        matches!(result.outcome,Outcome::Refused{ref code,ref message} if code=="capacity" && message.contains("repository.catalogue_revisions"))
    );
    let current = entry(
        raw(
            &c,
            Operation::GetRepository {
                repository_id: "alpha".into(),
                registration_revision: None,
            },
        )
        .await,
    );
    assert_eq!(current.generation, 1);
    assert_eq!(current.current_revision, "r1");
    refused(
        raw(
            &c,
            Operation::GetRepository {
                repository_id: "alpha".into(),
                registration_revision: Some("r2".into()),
            },
        )
        .await,
        "repository_not_found",
    );
}

#[tokio::test]
// Traceability scope: Absent local material root remains absent after registration and refused read; does not establish permission or grant invariance.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//= type=test
//# A repository registration operation MUST NOT acquire repository material.
async fn hostile_store_keys_are_refused_and_registration_does_not_acquire_material() {
    let root = root();
    let mut cfg = config();
    cfg.repository_read.store_root = Some(root.join("absent-material"));
    let c = Controller::open_with_config(root.join("task.db"), cfg).unwrap();
    for key in ["../outside", "/outside", "a/b", "a\\b", ".", "..", "a%2fb"] {
        let mut value = registration(&id("bad"), "r1", "source.git", "checkpoint.git");
        value.source_store_key = key.into();
        refused(
            raw(
                &c,
                Operation::RegisterRepository {
                    operation_id: id("reject"),
                    registration: value,
                },
            )
            .await,
            "invalid_input",
        );
    }
    let valid = registration("unavailable", "r1", "source.git", "checkpoint.git");
    register(&c, &valid).await;
    assert!(!root.join("absent-material").exists());
    let t = submit(&c, input(valid.binding(), &"a".repeat(40), false)).await;
    refused(raw(&c, read(&t.id)).await, "repository_read_unavailable");
    assert!(!root.join("absent-material").exists());
}

fn git(at: &Path, args: &[&str]) -> String {
    let out = Command::new("/usr/bin/git")
        .current_dir(at)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", "/nonexistent")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .args([
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@invalid",
            "-c",
            "core.hooksPath=/dev/null",
        ])
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "fixture Git failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().into()
}
fn material(root: &Path, key: &str, text: &str) -> String {
    let work = root.join(format!("work-{key}"));
    std::fs::create_dir(&work).unwrap();
    git(&work, &["init", "-q"]);
    std::fs::write(work.join("same.txt"), text).unwrap();
    git(&work, &["add", "same.txt"]);
    git(&work, &["commit", "-qm", "fixture"]);
    let commit = git(&work, &["rev-parse", "HEAD"]);
    git(
        root,
        &["clone", "--bare", "--quiet", work.to_str().unwrap(), key],
    );
    commit
}
fn read(task_id: &str) -> Operation {
    Operation::QueryCollaboration {
        query: Query::Repository {
            task_id: task_id.into(),
            request: repository_view::Request::ReadFile {
                source: SourceSelector::Start,
                path: "same.txt".into(),
                start_line: 1,
                max_lines: 1,
            },
        },
    }
}
fn content(reply: Reply) -> repository_view::Output {
    match reply.outcome {
        Outcome::Ok {
            data: Data::Collaboration(QueryResult::Repository(v)),
        } => *v,
        other => panic!("read: {other:?}"),
    }
}
#[tokio::test]
// Traceability scope: Two actual local Git stores with the same filename; exact accepted objects and unavailable-source refusal; no live browser/model journey.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//= type=test
//# The runtime MUST support multiple repository registrations described by metadata.
async fn two_actual_git_repositories_and_old_revision_read_exact_pinned_objects() {
    let root = root();
    let a = material(&root, "alpha.git", "alpha\n");
    let b = material(&root, "beta.git", "beta\n");
    let mut cfg = config();
    cfg.repository_read.store_root = Some(root.clone());
    let c = Controller::open_with_config(root.join("task.db"), cfg).unwrap();
    let alpha = registration("alpha", "r1", "alpha.git", "checkpoint-alpha.git");
    let beta = registration("beta", "r1", "beta.git", "checkpoint-beta.git");
    register(&c, &alpha).await;
    register(&c, &beta).await;
    let ta = submit(&c, input(alpha.binding(), &a, false)).await;
    let tb = submit(&c, input(beta.binding(), &b, false)).await;
    for (task, binding, text) in [
        (&ta, alpha.binding(), "alpha\n"),
        (&tb, beta.binding(), "beta\n"),
    ] {
        let output = content(raw(&c, read(&task.id)).await);
        assert_eq!(output.source.registration, binding);
        assert!(
            matches!(output.data,repository_view::Data::File{text: ref value,..} if value==text)
        );
        let serialized = serde_json::to_string(&output).unwrap();
        assert!(!serialized.contains("store_key"));
        assert!(!serialized.contains(root.to_str().unwrap()));
    }
    entry(
        raw(
            &c,
            Operation::ReviseRepository {
                operation_id: id("revise"),
                expected_generation: 1,
                registration: registration("alpha", "r2", "beta.git", "checkpoint-beta.git"),
            },
        )
        .await,
    );
    entry(
        raw(
            &c,
            Operation::SetRepositoryEnabled {
                operation_id: id("disable"),
                repository_id: "alpha".into(),
                expected_generation: 2,
                enabled_for_new_tasks: false,
            },
        )
        .await,
    );
    let old = content(raw(&c, read(&ta.id)).await);
    assert_eq!(old.source.registration, alpha.binding());
    assert!(matches!(old.data,repository_view::Data::File{text,..} if text=="alpha\n"));
    std::fs::remove_dir_all(root.join("alpha.git")).unwrap();
    refused(raw(&c, read(&ta.id)).await, "repository_read_unavailable");
    // No fallback to beta's same filename or current registration is permitted.
    assert!(
        matches!(content(raw(&c,read(&tb.id)).await).data,repository_view::Data::File{text,..} if text=="beta\n")
    );
}

#[tokio::test]
// Traceability scope: Controlled parent admission and child delegation after revise/disable; no actual coding executor launched.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//= type=test
//# A configured child repository association MUST be resolved when its parent workflow is accepted.
async fn configured_child_keeps_parent_admission_revision_after_revise_and_disable() {
    let root = root();
    let c = Controller::open_with_config(root.join("task.db"), config()).unwrap();
    let first = registration("alpha", "r1", "source.git", "checkpoint.git");
    register(&c, &first).await;
    let mut child = input(first.binding(), &"a".repeat(40), true);
    child.limits.model_requests = 1;
    child.limits.result_bytes = 4096;
    child.limits.output_bytes = 1024;
    let mut parent = input(first.binding(), &"a".repeat(40), false);
    parent.repository_read = None;
    parent.limits.model_requests = 4;
    parent.native_workflow = Some(Box::new(NativeWorkflow::CodingThenContinue {
        child: CodingTaskTemplate {
            instruction_source: None,
            pull_request: None,
            instruction_prefix: "Implement".into(),
            definition: child.definition.clone(),
            repository: child.repository.unwrap(),
            acceptance_commands: child.acceptance_commands,
            dependencies: vec![],
            limits: child.limits,
        },
    }));
    let parent = submit(&c, parent).await;
    entry(
        raw(
            &c,
            Operation::ReviseRepository {
                operation_id: id("revise"),
                expected_generation: 1,
                registration: registration("alpha", "r2", "new.git", "new-checkpoint.git"),
            },
        )
        .await,
    );
    entry(
        raw(
            &c,
            Operation::SetRepositoryEnabled {
                operation_id: id("disable"),
                repository_id: "alpha".into(),
                expected_generation: 2,
                enabled_for_new_tasks: false,
            },
        )
        .await,
    );
    let worker = Worker {
        id: "native-worker".into(),
        incarnation: "inc".into(),
        coding_capacity: 0,
        native_capacity: 1,
    };
    assert!(matches!(
        raw(
            &c,
            Operation::RegisterWorker {
                operation_id: id("worker"),
                worker: worker.clone()
            }
        )
        .await
        .outcome,
        Outcome::Ok { .. }
    ));
    let commands = raw(
        &c,
        Operation::AwaitCommands {
            operation_id: id("commands"),
            worker_id: worker.id.clone(),
            incarnation: worker.incarnation.clone(),
            acknowledged: vec![],
        },
    )
    .await;
    let attempt = match commands.outcome {
        Outcome::Ok {
            data: Data::Commands(values),
        } => values
            .into_iter()
            .find_map(|v| match v {
                WorkerCommand::RunAttempt { attempt, .. } => Some(attempt),
                _ => None,
            })
            .unwrap(),
        v => panic!("{v:?}"),
    };
    task(
        raw(
            &c,
            Operation::ObserveWorker {
                report: Report {
                    report_id: id("started"),
                    worker_id: worker.id,
                    incarnation: worker.incarnation,
                    task_id: parent.id.clone(),
                    attempt_id: attempt.id.clone(),
                    generation: attempt.generation,
                    observation: Observation::Started {
                        thread_id: None,
                        turn_id: None,
                    },
                },
            },
        )
        .await,
    );
    let child = borg_runtime::delegation::coding_child(&parent.input, "bounded change").unwrap();
    let accepted = task(
        raw(
            &c,
            Operation::DelegateTask {
                operation_id: id("delegate"),
                parent_task_id: parent.id,
                parent_attempt_id: attempt.id,
                generation: attempt.generation,
                child,
            },
        )
        .await,
    );
    assert_eq!(
        accepted.input.repository.as_ref().unwrap().registration,
        Some(first.binding())
    );
    assert_eq!(
        accepted.input.repository.as_ref().unwrap().url,
        first.source_endpoint
    );
}

#[tokio::test]
async fn aggregate_catalogue_bytes_refuse_without_registering_a_head() {
    let root = root();
    let first = registration("alpha", "r1", "source.git", "checkpoint.git");
    let mut cfg = config();
    Arc::make_mut(&mut cfg.limits).repository.catalogue_bytes =
        (serde_json::to_vec(&first).unwrap().len() - 1) as u64;
    let c = Controller::open_with_config(root.join("task.db"), cfg).unwrap();
    let reply = raw(
        &c,
        Operation::RegisterRepository {
            operation_id: id("byte-capacity"),
            registration: first,
        },
    )
    .await;
    assert!(
        matches!(reply.outcome,Outcome::Refused{ref code,ref message} if code=="capacity" && message.contains("repository.catalogue_bytes"))
    );
    let reply = raw(
        &c,
        Operation::ListRepositories {
            after: None,
            limit: 1,
        },
    )
    .await;
    assert!(
        matches!(reply.outcome,Outcome::Ok{data:Data::RepositoryRegistrations(ref page)} if page.items.is_empty() && page.next_cursor.is_none())
    );
}

#[cfg(debug_assertions)]
#[tokio::test]
// Traceability scope: Injected canonical receipt commit failure leaves no registration/head; later exact registration commits without partial state.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//= type=test
//# An operator repository-catalogue mutation MUST use the canonical durable controller operation path.
async fn failed_receipt_commit_leaves_no_catalogue_revision_or_head() {
    use std::sync::atomic::AtomicBool;
    let root = root();
    let fail = Arc::new(AtomicBool::new(false));
    let mut cfg = config();
    cfg.qualification_fail_next_commit = Some(fail.clone());
    let c = Controller::open_with_config(root.join("task.db"), cfg).unwrap();
    let value = registration("alpha", "r1", "source.git", "checkpoint.git");
    let op = Operation::RegisterRepository {
        operation_id: "original-register-after-rollback".into(),
        registration: value,
    };
    fail.store(true, Ordering::SeqCst);
    let failed = raw(&c, op.clone()).await;
    assert!(matches!(failed.outcome, Outcome::Refused { .. }));
    assert!(failed.commit.is_none());
    refused(
        raw(
            &c,
            Operation::GetRepository {
                repository_id: "alpha".into(),
                registration_revision: None,
            },
        )
        .await,
        "repository_not_found",
    );
    let committed = entry(raw(&c, op).await);
    assert_eq!(committed.generation, 1);
    assert_eq!(committed.current_revision, "r1");
}

#[cfg(target_os = "linux")]
#[tokio::test]
// Traceability scope: Actual local Unix directory permissions and access stay
// denied across canonical registration. This does not qualify remote ACLs,
// credential grants, or privilege changes on another host.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//= type=test
//# A repository registration operation MUST NOT grant repository access.
async fn registration_preserves_denied_local_repository_access_and_permissions() {
    use std::{
        fs::{self, Permissions},
        io::ErrorKind,
        os::unix::fs::{MetadataExt, PermissionsExt},
    };

    fn process_ids() -> (Vec<u32>, Vec<u32>) {
        let status = fs::read_to_string("/proc/self/status").unwrap();
        let numbers = |key: &str| {
            let rows = status
                .lines()
                .filter_map(|line| line.strip_prefix(key))
                .collect::<Vec<_>>();
            assert_eq!(rows.len(), 1);
            let ids = rows[0]
                .split_whitespace()
                .map(|value| value.parse::<u32>().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(ids.len(), 4);
            ids
        };
        (numbers("Uid:"), numbers("Gid:"))
    }
    fn access_state(path: &Path) -> (u64, u64, u32, u32, u32) {
        let metadata = fs::symlink_metadata(path).unwrap();
        assert!(metadata.is_dir());
        assert!(!metadata.file_type().is_symlink());
        (
            metadata.dev(),
            metadata.ino(),
            metadata.uid(),
            metadata.gid(),
            metadata.mode(),
        )
    }
    fn assert_denied(path: &Path) {
        let directory = fs::read_dir(path).err().expect(
            "fixture precondition: directory access must be denied; no DAC-bypassing profile",
        );
        assert_eq!(directory.kind(), ErrorKind::PermissionDenied);
        let file = fs::read(path.join("HEAD")).err().expect(
            "fixture precondition: file access must be denied; no DAC-bypassing profile",
        );
        assert_eq!(file.kind(), ErrorKind::PermissionDenied);
    }
    struct RestoreFixture {
        root: PathBuf,
        saved: Vec<(PathBuf, Permissions)>,
    }
    impl RestoreFixture {
        fn restore(&self) -> std::io::Result<()> {
            for (path, permissions) in &self.saved {
                fs::set_permissions(path, permissions.clone())?;
            }
            Ok(())
        }
    }
    impl Drop for RestoreFixture {
        fn drop(&mut self) {
            // The guard owns only this test's newly created Git material. Arm
            // it before chmod so assertion/panic or partial setup restores
            // every permission that the fixture may have changed.
            for (path, permissions) in &self.saved {
                let _ = fs::set_permissions(path, permissions.clone());
            }
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    let starting_ids = process_ids();
    assert!(
        starting_ids.0.iter().all(|uid| *uid != 0),
        "local permission qualification requires non-root real/effective/saved/filesystem UIDs: {:?}",
        starting_ids.0
    );
    let root = root();
    let material_root = root.join("permission-material");
    fs::create_dir(&material_root).unwrap();
    let mut fixture = RestoreFixture {
        root: material_root.clone(),
        saved: vec![],
    };
    let expected_text = "source remains private until fixture restores access\n";
    let source_commit = material(&material_root, "source.git", expected_text);
    material(&material_root, "checkpoint.git", "separate checkpoint store\n");
    let paths = [
        material_root.join("source.git"),
        material_root.join("checkpoint.git"),
    ];
    let original = paths.iter().map(|p| access_state(p)).collect::<Vec<_>>();
    for path in &paths {
        fixture
            .saved
            .push((path.clone(), fs::metadata(path).unwrap().permissions()));
    }
    for path in &paths {
        fs::set_permissions(path, Permissions::from_mode(0o000)).unwrap();
        assert_denied(path);
    }
    let denied = paths.iter().map(|p| access_state(p)).collect::<Vec<_>>();
    assert!(denied.iter().all(|state| state.4 & 0o7777 == 0));

    // Controller storage and the operational root remain accessible; only the
    // two already provisioned fixture repositories have denied permissions.
    let mut cfg = config();
    cfg.repository_read.store_root = Some(material_root.clone());
    let c = Controller::open_with_config(root.join("task.db"), cfg).unwrap();
    let mut value = registration("denied-source", "r1", "source.git", "checkpoint.git");
    value.source_endpoint = format!("file://{}", paths[0].display());
    value.checkpoint_endpoint = format!("file://{}", paths[1].display());
    let reply = raw(
        &c,
        Operation::RegisterRepository {
            operation_id: id("register-without-access"),
            registration: value.clone(),
        },
    )
    .await;
    assert!(reply.commit.is_some());
    assert_eq!(entry(reply).registration_revision, "r1");
    assert_eq!(process_ids(), starting_ids);
    for (path, expected) in paths.iter().zip(&denied) {
        assert_eq!(&access_state(path), expected);
        assert_denied(path);
    }
    let accepted = submit(&c, input(value.binding(), &source_commit, false)).await;
    refused(raw(&c, read(&accepted.id)).await, "repository_read_unavailable");
    for (path, expected) in paths.iter().zip(&denied) {
        assert_eq!(&access_state(path), expected);
        assert_denied(path);
    }
    assert_eq!(process_ids(), starting_ids);

    // Restore only this fixture's permissions as a positive control. The same
    // accepted task and receipt-free operator read now return the exact data;
    // neither registration nor accepted association is replaced or revised.
    fixture.restore().unwrap();
    for (path, expected) in paths.iter().zip(&original) {
        assert_eq!(&access_state(path), expected);
    }
    let output = content(raw(&c, read(&accepted.id)).await);
    assert_eq!(output.source.registration, value.binding());
    assert_eq!(output.source.commit, source_commit);
    assert!(output.untrusted);
    assert!(
        matches!(output.data, repository_view::Data::File { text, .. } if text == expected_text)
    );
    assert_eq!(process_ids(), starting_ids);
    drop(c);
    fs::remove_dir_all(&material_root).unwrap();
    assert!(!material_root.exists());
}

#[path = "repository_catalogue/instruction_sources.rs"]
mod instruction_sources;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_repository_revisions_commit_one_head_and_replay_both_original_outcomes() {
    async fn original(c: &Controller, request: &Operation, receipt: &Reply) -> Reply {
        let operation_id = request.mutation_id().unwrap();
        let lookup = Operation::GetOperation { operation_id: operation_id.into() };
        let found = raw(c, lookup.clone()).await;
        found.validate_for(&lookup).unwrap();
        let Outcome::Ok { data: Data::Operation(record) } = &found.outcome else {
            panic!("original mutation receipt expected: {found:?}");
        };
        assert_eq!(record.id, operation_id);
        assert_eq!(record.kind, "application_mutation");
        assert_eq!(record.state, "settled");
        assert_eq!(record.intended, serde_json::to_value(request).unwrap());
        assert_eq!(record.outcome, Some(serde_json::to_value(&receipt.outcome).unwrap()));
        assert_eq!(found.commit, receipt.commit);
        let commit = found.commit.as_ref().unwrap();
        assert_eq!(commit.operation_id, operation_id);
        assert!(commit.controller_revision > 0);
        assert!(commit.controller_revision <= found.observed_controller_revision.unwrap());
        found
    }
    async fn retained(c: &Controller, first: &Registration, winner: &Registration, loser: &Registration) {
        let head = entry(raw(c, Operation::GetRepository {
            repository_id: first.repository_id.clone(), registration_revision: None,
        }).await);
        assert_eq!(head.current_revision, winner.registration_revision);
        assert_eq!(head.registration_revision, winner.registration_revision);
        assert_eq!(head.generation, 2);
        assert!(head.enabled_for_new_tasks);
        for expected in [first, winner] {
            let stored = entry(raw(c, Operation::GetRepository {
                repository_id: expected.repository_id.clone(),
                registration_revision: Some(expected.registration_revision.clone()),
            }).await);
            assert_eq!(stored.repository_id, expected.repository_id);
            assert_eq!(stored.registration_revision, expected.registration_revision);
            assert_eq!(stored.display_name, expected.display_name);
            assert_eq!(stored.source_endpoint, expected.source_endpoint);
            assert_eq!(stored.checkpoint_endpoint, expected.checkpoint_endpoint);
            assert_eq!(stored.current_revision, winner.registration_revision);
            assert_eq!(stored.generation, 2);
        }
        refused(raw(c, Operation::GetRepository {
            repository_id: loser.repository_id.clone(),
            registration_revision: Some(loser.registration_revision.clone()),
        }).await, "repository_not_found");
    }

    let directory = root();
    let db = directory.join("task.db");
    let cfg = config();
    let c = Controller::open_with_config(&db, cfg.clone()).unwrap();
    let first = registration("concurrent-cas", "r1", "initial.git", "checkpoint.git");
    assert_eq!(register(&c, &first).await.generation, 1);
    let candidates = [
        registration("concurrent-cas", "left-r2", "left.git", "left-checkpoint.git"),
        registration("concurrent-cas", "right-r2", "right.git", "right-checkpoint.git"),
    ];
    let operations = [
        Operation::ReviseRepository { operation_id: "concurrent-left".into(), expected_generation: 1, registration: candidates[0].clone() },
        Operation::ReviseRepository { operation_id: "concurrent-right".into(), expected_generation: 1, registration: candidates[1].clone() },
    ];
    // Both original requests are live behind the same barrier before either
    // caller awaits a result. The real owner transaction chooses the winner.
    let start = Arc::new(tokio::sync::Barrier::new(2));
    let mut calls = Vec::new();
    for operation in &operations {
        let owner = c.clone();
        let request = operation.clone();
        let start = start.clone();
        calls.push(tokio::spawn(async move {
            start.wait().await;
            let reply = raw(&owner, request.clone()).await;
            reply.validate_for(&request).unwrap();
            reply
        }));
    }
    let right_call = calls.pop().unwrap();
    let left_call = calls.pop().unwrap();
    let (left, right) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(left_call, right_call)
    }).await.unwrap();
    let receipts = [left.unwrap(), right.unwrap()];
    let successful: Vec<usize> = receipts.iter().enumerate().filter_map(|(index, reply)| {
        matches!(reply.outcome, Outcome::Ok { data: Data::RepositoryRegistration(_) }).then_some(index)
    }).collect();
    assert_eq!(successful.len(), 1, "one concurrent CAS must win: {receipts:?}");
    let winner = successful[0];
    let loser = 1 - winner;
    refused(receipts[loser].clone(), "revision_conflict");
    assert_eq!(entry(receipts[winner].clone()).generation, 2);
    assert_ne!(receipts[0].commit, receipts[1].commit);
    retained(&c, &first, &candidates[winner], &candidates[loser]).await;

    let mut originals = Vec::new();
    for (request, receipt) in operations.iter().zip(&receipts) {
        originals.push(original(&c, request, receipt).await);
        let replay = raw(&c, request.clone()).await;
        assert_eq!(replay.outcome, receipt.outcome);
        assert_eq!(replay.commit, receipt.commit);
        let mut changed = request.clone();
        let Operation::ReviseRepository { registration, .. } = &mut changed else { unreachable!() };
        registration.display_name.push_str(" changed body");
        refused(raw(&c, changed).await, "operation_conflict");
        let unchanged = original(&c, request, receipt).await;
        assert_eq!(unchanged.outcome, originals.last().unwrap().outcome);
        assert_eq!(unchanged.commit, originals.last().unwrap().commit);
    }
    retained(&c, &first, &candidates[winner], &candidates[loser]).await;
    drop(c);
    let c = reopen(&db, cfg).await;
    retained(&c, &first, &candidates[winner], &candidates[loser]).await;
    for ((request, receipt), before) in operations.iter().zip(&receipts).zip(&originals) {
        let after = original(&c, request, receipt).await;
        assert_eq!(after.outcome, before.outcome);
        assert_eq!(after.commit, before.commit);
        let replay = raw(&c, request.clone()).await;
        assert_eq!(replay.outcome, receipt.outcome);
        assert_eq!(replay.commit, receipt.commit);
    }
    retained(&c, &first, &candidates[winner], &candidates[loser]).await;
}
