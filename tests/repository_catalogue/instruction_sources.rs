//! Actual Controller and local Git source joins. No provider, fetch, worker, or
//! production metadata activation. Publication acknowledgements are fixture input.
use super::*;
use borg_runtime::mcp::{Caller, Invocation};
use std::collections::BTreeMap;

async fn call(c: &Controller, command: Operation) -> Reply {
    Box::pin(c.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: id("instruction-read"),
        command,
    }))
    .await
}
async fn registered(c: &Controller, registration: &Registration) {
    entry(
        call(
            c,
            Operation::RegisterRepository {
                operation_id: id("instruction-register"),
                registration: registration.clone(),
            },
        )
        .await,
    );
}
async fn submitted(c: &Controller, input: TaskInput) -> (Task, Operation, Reply) {
    let op = Operation::SubmitTask {
        operation_id: id("instruction-submit"),
        input,
    };
    let receipt = call(c, op.clone()).await;
    (task(receipt.clone()), op, receipt)
}
fn source(registration: &Registration, commit: &str) -> RepositoryReadSource {
    RepositoryReadSource {
        registration: Some(registration.binding()),
        repository: String::new(),
        start_commit: commit.into(),
    }
}
fn selected_read(task_id: &str, source: SourceSelector) -> Operation {
    Operation::QueryCollaboration {
        query: Query::Repository {
            task_id: task_id.into(),
            request: repository_view::Request::ReadFile {
                source,
                path: "same.txt".into(),
                start_line: 1,
                max_lines: 1,
            },
        },
    }
}
fn exact_file(
    output: repository_view::Output,
    registration: &Registration,
    commit: &str,
    text: &str,
    root: &Path,
) {
    assert!(output.untrusted);
    assert_eq!(output.source.registration, registration.binding());
    assert_eq!(output.source.commit, commit);
    assert_eq!(output.source.base_commit, None);
    assert!(matches!(&output.data, repository_view::Data::File {text: got, ..} if got == text));
    let public = serde_json::to_string(&output).unwrap();
    assert!(!public.contains("store_key"));
    assert!(!public.contains(root.to_str().unwrap()));
}
async fn started(c: &Controller, task: &Task) -> (Attempt, Caller) {
    let coding = task.input.definition.executor == ExecutorKind::Coding;
    let worker = Worker {
        id: id("instruction-worker"),
        incarnation: id("inc"),
        native_capacity: usize::from(!coding),
        coding_capacity: usize::from(coding),
    };
    assert!(matches!(
        call(
            c,
            Operation::RegisterWorker {
                operation_id: id("worker"),
                worker: worker.clone()
            }
        )
        .await
        .outcome,
        Outcome::Ok { .. }
    ));
    let commands = call(
        c,
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
            data: Data::Commands(commands),
        } => commands
            .into_iter()
            .find_map(|v| match v {
                WorkerCommand::RunAttempt {
                    attempt,
                    task: accepted,
                    ..
                } if attempt.task_id == task.id => {
                    assert_eq!(accepted, task.input);
                    Some(attempt)
                }
                _ => None,
            })
            .unwrap(),
        other => panic!("commands: {other:?}"),
    };
    task_from_started(c, &attempt).await;
    let caller = Caller {
        task_id: task.id.clone(),
        attempt_id: attempt.id.clone(),
        generation: attempt.generation,
        worker_id: worker.id,
        incarnation: worker.incarnation,
    };
    (attempt, caller)
}
async fn task_from_started(c: &Controller, attempt: &Attempt) {
    task(
        call(
            c,
            Operation::ObserveWorker {
                report: Report {
                    report_id: id("started"),
                    worker_id: attempt.worker_id.clone(),
                    incarnation: attempt.worker_incarnation.clone(),
                    task_id: attempt.task_id.clone(),
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
}
fn read_call(caller: &Caller) -> Invocation {
    Invocation {
        operation_id: id("instruction-call"),
        logical_call_id: id("logical"),
        caller: caller.clone(),
        tool: "collab_repository_read".into(),
        arguments: json!({"source":"instructions","path":"same.txt","start_line":1,"max_lines":1}),
    }
}
fn repository_reply(reply: &Reply) -> repository_view::Output {
    assert!(reply.commit.is_some());
    match &reply.outcome {
        Outcome::Ok {
            data: Data::Repository(output),
        } => (**output).clone(),
        other => panic!("repository call: {other:?}"),
    }
}
async fn owner_released(path: &Path) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(format!("{}.controller-lock", path.display()))
                .unwrap();
            if file.try_lock().is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
fn database_bytes(path: &Path) -> BTreeMap<String, Vec<u8>> {
    ["", "-wal", "-shm"]
        .into_iter()
        .filter_map(|suffix| {
            let name = format!("{}{suffix}", path.display());
            std::fs::read(&name).ok().map(|bytes| (name, bytes))
        })
        .collect()
}

#[tokio::test]
async fn coding_dual_source_profiles_read_exact_objects_and_replay_after_cancel_reopen() {
    for profile in [
        ExecutionProfile::CollaborationV1 { web_search: None },
        ExecutionProfile::CollaborationV2 { web_search: None },
    ] {
        let root = root();
        let db = root.join("task.db");
        let work_commit = material(&root, "work.git", "workspace\n");
        let spec_commit = material(&root, "instructions.git", "instructions\n");
        let checkpoint_commit = material(&root, "checkpoint.git", "checkpoint\n");
        let work = registration("work", "r1", "work.git", "checkpoint.git");
        let spec = registration("spec", "r1", "instructions.git", "unused.git");
        let mut cfg = config();
        cfg.repository_read.store_root = Some(root.clone());
        let c = Controller::open_with_config(&db, cfg.clone()).unwrap();
        registered(&c, &work).await;
        registered(&c, &spec).await;
        let mut input = input(work.binding(), &work_commit, true);
        input.definition.execution_profile = Some(profile.clone());
        input.instruction_source = Some(source(&spec, &spec_commit));
        let (accepted, submit_op, submit_receipt) = submitted(&c, input).await;
        assert_eq!(accepted.input.definition.execution_profile, Some(profile));
        assert_eq!(
            accepted.input.repository.as_ref().unwrap().url,
            work.source_endpoint
        );
        assert_eq!(
            accepted
                .input
                .repository
                .as_ref()
                .unwrap()
                .checkpoint_remote,
            work.checkpoint_endpoint
        );
        assert_eq!(
            accepted
                .input
                .instruction_source
                .as_ref()
                .unwrap()
                .repository,
            spec.source_endpoint
        );
        let (attempt, caller) = started(&c, &accepted).await;
        exact_file(
            content(call(&c, selected_read(&accepted.id, SourceSelector::Start)).await),
            &work,
            &work_commit,
            "workspace\n",
            &root,
        );
        let invocation = read_call(&caller);
        let read_receipt = call(&c, invocation.operation()).await;
        read_receipt.validate_for(&invocation.operation()).unwrap();
        exact_file(
            repository_reply(&read_receipt),
            &spec,
            &spec_commit,
            "instructions\n",
            &root,
        );
        let publication = id("checkpoint");
        let component = borg_runtime::worker::process::component;
        let checkpoint = Checkpoint {
            registration: work.binding(),
            repository: work.checkpoint_endpoint.clone(),
            commit: checkpoint_commit.clone(),
            remote_ref: format!(
                "refs/heads/borg/tasks/{}/{}/{}",
                component(&accepted.id),
                component(&attempt.id),
                component(&publication)
            ),
            operation_id: publication.clone(),
        };
        assert!(matches!(call(&c, Operation::PrepareOperation { operation_id: publication.clone(), task_id: accepted.id.clone(), attempt_id: attempt.id.clone(), generation: attempt.generation,
            kind: "checkpoint".into(), intended: json!({"registration":checkpoint.registration,"repository":checkpoint.repository,"commit":checkpoint.commit,"remote_ref":checkpoint.remote_ref}) }).await.outcome, Outcome::Ok{..}));
        assert!(matches!(
            call(
                &c,
                Operation::SettleOperation {
                    settlement_id: id("settled"),
                    operation_id: publication,
                    task_id: accepted.id.clone(),
                    attempt_id: attempt.id.clone(),
                    generation: attempt.generation,
                    outcome: json!({"acknowledged":true}),
                    checkpoint: Some(checkpoint.clone())
                }
            )
            .await
            .outcome,
            Outcome::Ok { .. }
        ));
        let cp = content(call(&c, selected_read(&accepted.id, SourceSelector::Checkpoint)).await);
        assert_eq!(cp.source.repository, work.checkpoint_endpoint);
        exact_file(cp, &work, &checkpoint_commit, "checkpoint\n", &root);
        refused(
            call(
                &c,
                Operation::QueryCollaboration {
                    query: Query::Repository {
                        task_id: accepted.id.clone(),
                        request: repository_view::Request::Diff {
                            source: SourceSelector::Instructions,
                            base_source: SourceSelector::Start,
                            path: "same.txt".into(),
                        },
                    },
                },
            )
            .await,
            "repository_read_invalid",
        );
        // No current-head fallback or implicit acquisition when only the exact
        // instruction material disappears. The independent workspace still reads.
        std::fs::rename(
            root.join("instructions.git"),
            root.join("held-instructions.git"),
        )
        .unwrap();
        refused(
            call(
                &c,
                selected_read(&accepted.id, SourceSelector::Instructions),
            )
            .await,
            "repository_read_unavailable",
        );
        assert!(!root.join("instructions.git").exists());
        exact_file(
            content(call(&c, selected_read(&accepted.id, SourceSelector::Start)).await),
            &work,
            &work_commit,
            "workspace\n",
            &root,
        );
        let current = task(
            call(
                &c,
                Operation::GetTask {
                    task_id: accepted.id.clone(),
                },
            )
            .await,
        );
        let cancelled = task(
            call(
                &c,
                Operation::CancelTask {
                    operation_id: id("cancel"),
                    task_id: current.id,
                    expected_revision: current.revision,
                },
            )
            .await,
        );
        let replay = call(&c, invocation.operation()).await;
        assert_eq!(replay.outcome, read_receipt.outcome);
        assert_eq!(replay.commit, read_receipt.commit);
        let lookup = Operation::GetCollaborationCall {
            caller: caller.clone(),
            operation_id: invocation.operation_id.clone(),
        };
        let old_lookup = call(&c, lookup.clone()).await;
        old_lookup.validate_for(&lookup).unwrap();
        let fresh = read_call(&caller);
        assert!(matches!(
            call(&c, fresh.operation()).await.outcome,
            Outcome::Refused { .. }
        ));
        drop(c);
        owner_released(&db).await;
        let c = reopen(&db, cfg).await;
        assert_eq!(
            task(
                call(
                    &c,
                    Operation::GetTask {
                        task_id: accepted.id.clone()
                    }
                )
                .await
            ),
            cancelled
        );
        let replay = call(&c, invocation.operation()).await;
        assert_eq!(replay.outcome, read_receipt.outcome);
        assert_eq!(replay.commit, read_receipt.commit);
        assert_eq!(call(&c, lookup).await.outcome, old_lookup.outcome);
        let replay = call(&c, submit_op).await;
        assert_eq!(replay.outcome, submit_receipt.outcome);
        assert_eq!(replay.commit, submit_receipt.commit);
    }
}

#[tokio::test]
async fn instruction_admission_refuses_unbound_forged_and_unselected_scope_without_effect() {
    let root = root();
    let c = Controller::open_with_config(root.join("task.db"), config()).unwrap();
    let work = registration("work", "r1", "work.git", "checkpoint.git");
    let spec = registration("spec", "r1", "instructions.git", "unused.git");
    registered(&c, &work).await;
    registered(&c, &spec).await;
    let base = input(work.binding(), &"a".repeat(40), true);
    // Absent optional vocabulary has exactly the original serialization.
    let old = serde_json::to_value(&base).unwrap();
    assert!(!old.as_object().unwrap().contains_key("instruction_source"));
    let round: TaskInput = serde_json::from_value(old.clone()).unwrap();
    assert_eq!(serde_json::to_value(round).unwrap(), old);
    let mut bound = base.clone();
    bound.instruction_source = Some(source(&spec, &"b".repeat(40)));
    let mut cases = Vec::new();
    let mut v = bound.clone();
    v.instruction_source.as_mut().unwrap().registration = None;
    cases.push((v, "repository_binding_required"));
    let mut v = bound.clone();
    v.instruction_source.as_mut().unwrap().repository = "https://forged.invalid/repo".into();
    cases.push((v, "invalid_input"));
    for commit in [
        "HEAD".into(),
        "b".repeat(39),
        "B".repeat(40),
        "b".repeat(41),
    ] {
        let mut v = bound.clone();
        v.instruction_source.as_mut().unwrap().start_commit = commit;
        cases.push((v, "invalid_input"));
    }
    let mut v = bound.clone();
    v.definition.execution_profile = None;
    cases.push((v, "template_profile_unbound"));
    let mut v = input(work.binding(), &"a".repeat(40), false);
    v.instruction_source = bound.instruction_source.clone();
    cases.push((v, "invalid_input"));
    for (input, code) in cases {
        refused(
            call(
                &c,
                Operation::SubmitTask {
                    operation_id: id("bad-instruction"),
                    input,
                },
            )
            .await,
            code,
        );
    }
    // No partial task or definition was admitted by any refused source.
    assert!(
        matches!(call(&c,Operation::ListTasks{after:None,limit:1}).await.outcome,Outcome::Ok{data:Data::Tasks(ref tasks)} if tasks.is_empty())
    );
    let (absent, _, _) = submitted(&c, base).await;
    refused(
        call(&c, selected_read(&absent.id, SourceSelector::Instructions)).await,
        "repository_read_unavailable",
    );
    let mut revised = spec.clone();
    revised.registration_revision = "r2".into();
    entry(
        call(
            &c,
            Operation::ReviseRepository {
                operation_id: id("revise"),
                expected_generation: 1,
                registration: revised.clone(),
            },
        )
        .await,
    );
    refused(
        call(
            &c,
            Operation::SubmitTask {
                operation_id: id("stale"),
                input: bound.clone(),
            },
        )
        .await,
        "repository_revision_changed",
    );
    entry(
        call(
            &c,
            Operation::SetRepositoryEnabled {
                operation_id: id("disable"),
                repository_id: spec.repository_id,
                expected_generation: 2,
                enabled_for_new_tasks: false,
            },
        )
        .await,
    );
    bound.instruction_source.as_mut().unwrap().registration = Some(revised.binding());
    refused(
        call(
            &c,
            Operation::SubmitTask {
                operation_id: id("disabled"),
                input: bound,
            },
        )
        .await,
        "repository_disabled",
    );
}

#[tokio::test]
async fn configured_instruction_source_is_pinned_at_parent_admission_and_child_reopen() {
    for through_mcp in [false, true] {
        let root = root();
        let db = root.join("task.db");
        let work_commit = material(&root, "work.git", "work\n");
        let spec_commit = material(&root, "spec.git", "old instructions\n");
        let other_commit = material(&root, "other.git", "new instructions\n");
        let work = registration("work", "r1", "work.git", "checkpoint.git");
        let spec = registration("spec", "r1", "spec.git", "unused.git");
        let mut cfg = config();
        cfg.repository_read.store_root = Some(root.clone());
        let c = Controller::open_with_config(&db, cfg.clone()).unwrap();
        registered(&c, &work).await;
        registered(&c, &spec).await;
        let mut child = input(work.binding(), &work_commit, true);
        child.limits.model_requests = 1;
        child.limits.result_bytes = 4096;
        child.limits.output_bytes = 1024;
        let mut parent = input(work.binding(), &work_commit, false);
        parent.repository_read = None;
        parent.limits.model_requests = 4;
        // A template is resolved now; it grants no Instructions selector to Native.
        parent.native_workflow = Some(Box::new(NativeWorkflow::CodingThenContinue {
            child: CodingTaskTemplate {
                instruction_source: Some(source(&spec, &spec_commit)),
                pull_request: None,
                instruction_prefix: "Implement".into(),
                definition: child.definition,
                repository: child.repository.unwrap(),
                acceptance_commands: child.acceptance_commands,
                dependencies: vec![],
                limits: child.limits,
            },
        }));
        let (parent, _, _) = submitted(&c, parent).await;
        let (attempt, caller) = started(&c, &parent).await;
        refused(
            call(&c, selected_read(&parent.id, SourceSelector::Instructions)).await,
            "repository_read_unavailable",
        );
        let mut newer = spec.clone();
        newer.registration_revision = "r2".into();
        newer.source_endpoint = "https://example.invalid/other.git".into();
        newer.source_store_key = "other.git".into();
        entry(
            call(
                &c,
                Operation::ReviseRepository {
                    operation_id: id("revise"),
                    expected_generation: 1,
                    registration: newer,
                },
            )
            .await,
        );
        entry(
            call(
                &c,
                Operation::SetRepositoryEnabled {
                    operation_id: id("disable"),
                    repository_id: spec.repository_id.clone(),
                    expected_generation: 2,
                    enabled_for_new_tasks: false,
                },
            )
            .await,
        );
        let child =
            borg_runtime::delegation::coding_child(&parent.input, "bounded change").unwrap();
        assert_eq!(
            child.instruction_source.as_ref().unwrap().start_commit,
            spec_commit
        );
        let mut forged = child.clone();
        forged.instruction_source.as_mut().unwrap().start_commit = other_commit;
        refused(
            call(
                &c,
                Operation::DelegateTask {
                    operation_id: id("forged-child"),
                    parent_task_id: parent.id.clone(),
                    parent_attempt_id: attempt.id.clone(),
                    generation: attempt.generation,
                    child: forged,
                },
            )
            .await,
            "workflow_mismatch",
        );
        let op = if through_mcp {
            Invocation {
                operation_id: id("configured-child"),
                logical_call_id: id("logical"),
                caller,
                tool: "collab_task_delegate".into(),
                arguments: json!({"instruction":"bounded change"}),
            }
            .operation()
        } else {
            Operation::DelegateTask {
                operation_id: id("configured-child"),
                parent_task_id: parent.id.clone(),
                parent_attempt_id: attempt.id,
                generation: attempt.generation,
                child,
            }
        };
        let receipt = call(&c, op.clone()).await;
        let child = task(receipt.clone());
        assert_eq!(child.parent_task_id.as_deref(), Some(parent.id.as_str()));
        assert_eq!(
            child
                .input
                .instruction_source
                .as_ref()
                .unwrap()
                .registration,
            Some(spec.binding())
        );
        assert_eq!(
            child.input.instruction_source.as_ref().unwrap().repository,
            spec.source_endpoint
        );
        assert_eq!(
            child.input.repository.as_ref().unwrap().registration,
            Some(work.binding())
        );
        exact_file(
            content(call(&c, selected_read(&child.id, SourceSelector::Instructions)).await),
            &spec,
            &spec_commit,
            "old instructions\n",
            &root,
        );
        drop(c);
        owner_released(&db).await;
        let c = reopen(&db, cfg).await;
        assert_eq!(
            task(
                call(
                    &c,
                    Operation::GetTask {
                        task_id: child.id.clone()
                    }
                )
                .await
            )
            .input,
            child.input
        );
        assert_eq!(
            task(call(&c, Operation::GetTask { task_id: parent.id }).await).input,
            parent.input
        );
        let replay = call(&c, op).await;
        assert_eq!(replay.outcome, receipt.outcome);
        assert_eq!(replay.commit, receipt.commit);
        exact_file(
            content(call(&c, selected_read(&child.id, SourceSelector::Instructions)).await),
            &spec,
            &spec_commit,
            "old instructions\n",
            &root,
        );
    }
}

#[tokio::test]
async fn instruction_endpoint_and_complete_input_bounds_are_enforced_without_omission() {
    let root = root();
    let mut cfg = config();
    Arc::make_mut(&mut cfg.limits)
        .controller
        .max_repository_identity_bytes = 64;
    let c = Controller::open_with_config(root.join("task.db"), cfg.clone()).unwrap();
    let work = registration("work", "r1", "work.git", "cp.git");
    let mut spec = registration("spec", "r1", "spec.git", "unused.git");
    spec.source_endpoint = "s".repeat(64);
    registered(&c, &work).await;
    registered(&c, &spec).await;
    let mut value = input(work.binding(), &"a".repeat(40), true);
    value.instruction_source = Some(source(&spec, &"b".repeat(64)));
    let (accepted, _, _) = submitted(&c, value.clone()).await;
    assert_eq!(
        accepted
            .input
            .instruction_source
            .as_ref()
            .unwrap()
            .repository
            .len(),
        64
    );
    let mut excessive = spec.clone();
    excessive.repository_id = "too-long".into();
    excessive.source_endpoint.push('s');
    registered(&c, &excessive).await;
    value.definition.id = id("overbound-agent");
    value.instruction_source = Some(source(&excessive, &"b".repeat(64)));
    let reply = call(
        &c,
        Operation::SubmitTask {
            operation_id: id("overbound"),
            input: value,
        },
    )
    .await;
    assert!(
        matches!(reply.outcome,Outcome::Refused{ref code,ref message} if code=="invalid_input"&&message.contains("controller.max_repository_identity_bytes"))
    );
    let mut complete = accepted.input.clone();
    complete.resolved_limits = None;
    complete.repository.as_mut().unwrap().url.clear();
    complete
        .repository
        .as_mut()
        .unwrap()
        .checkpoint_remote
        .clear();
    complete
        .instruction_source
        .as_mut()
        .unwrap()
        .repository
        .clear();
    complete.definition.id = id("input-bound-agent");
    // The selected source remains inside the complete accepted-input budget.
    // A large instruction is not dropped or truncated to make the source fit.
    complete.instruction = "x".repeat(cfg.limits.controller.max_task_input_bytes);
    let reply = call(
        &c,
        Operation::SubmitTask {
            operation_id: id("complete-bound"),
            input: complete,
        },
    )
    .await;
    assert!(
        matches!(reply.outcome,Outcome::Refused{ref code,ref message} if code=="capacity"&&message.contains("controller.max_task_input_bytes"))
    );
}

#[tokio::test]
async fn corrupted_instruction_selection_or_original_receipt_refuses_reopen_without_rewrite() {
    for corruption in [
        "commit",
        "endpoint",
        "receipt",
        "duplicate-admission",
        "oversized-command",
        "old-marker",
    ] {
        let root = root();
        let db = root.join("task.db");
        let cfg = config();
        let c = Controller::open_with_config(&db, cfg.clone()).unwrap();
        let work = registration("work", "r1", "work.git", "cp.git");
        let spec = registration("spec", "r1", "spec.git", "unused.git");
        registered(&c, &work).await;
        registered(&c, &spec).await;
        let mut value = input(work.binding(), &"a".repeat(40), true);
        value.instruction_source = Some(source(&spec, &"b".repeat(40)));
        let (accepted, op, original) = submitted(&c, value).await;
        if corruption == "oversized-command" {
            started(&c, &accepted).await;
        }
        drop(c);
        owner_released(&db).await;
        let sql = rusqlite::Connection::open(&db).unwrap();
        match corruption {
            "commit" | "endpoint" => {
                let raw: String = sql
                    .query_row("SELECT body FROM tasks WHERE id=?1", [&accepted.id], |r| {
                        r.get(0)
                    })
                    .unwrap();
                let mut stored: serde_json::Value = serde_json::from_str(&raw).unwrap();
                let source = &mut stored["task"]["input"]["instruction_source"];
                if corruption == "commit" {
                    source["start_commit"] = json!("c".repeat(40));
                } else {
                    source["repository"] = json!("https://forged.invalid/repo");
                }
                // Preserve canonical struct field order; this is a semantic
                // substitution, not merely a noncanonical-JSON failure.
                let changed = if corruption == "commit" {
                    raw.replace(
                        &format!("\"start_commit\":\"{}\"", "b".repeat(40)),
                        &format!("\"start_commit\":\"{}\"", "c".repeat(40)),
                    )
                } else {
                    raw.replace(
                        &format!("\"repository\":\"{}\"", spec.source_endpoint),
                        "\"repository\":\"https://forged.invalid/repo\"",
                    )
                };
                assert_ne!(changed, raw);
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&changed).unwrap(),
                    stored
                );
                sql.execute(
                    "UPDATE tasks SET body=?2 WHERE id=?1",
                    rusqlite::params![accepted.id, changed],
                )
                .unwrap();
            }
            "receipt" => {
                assert_eq!(
                    sql.execute(
                        "DELETE FROM receipts WHERE id=?1",
                        [op.mutation_id().unwrap()]
                    )
                    .unwrap(),
                    1
                );
            }
            "duplicate-admission" => {
                let mut duplicate = op.clone();
                let Operation::SubmitTask { operation_id, .. } = &mut duplicate else {
                    unreachable!()
                };
                *operation_id = "forged-second-admission".into();
                sql.execute(
                    "UPDATE counters SET value=value+1 WHERE kind='controller_revision'",
                    [],
                )
                .unwrap();
                let revision: i64 = sql
                    .query_row(
                        "SELECT value FROM counters WHERE kind='controller_revision'",
                        [],
                        |r| r.get(0),
                    )
                    .unwrap();
                sql.execute("INSERT INTO receipts(id,request,outcome,cleanup,controller_revision) VALUES(?1,?2,?3,0,?4)",rusqlite::params![duplicate.mutation_id().unwrap(),serde_json::to_string(&duplicate).unwrap(),serde_json::to_string(&original.outcome).unwrap(),revision]).unwrap();
            }
            "oversized-command" => {
                let body = "x".repeat(cfg.limits.transport.body_bytes + 1);
                assert_eq!(
                    sql.execute(
                        "UPDATE commands SET body=?1 WHERE task_id=?2",
                        rusqlite::params![body, accepted.id]
                    )
                    .unwrap(),
                    1
                );
            }
            // Negative corruption only: genuine historical production is tested
            // separately below and never obtained by stamping this fixture down.
            "old-marker" => sql.pragma_update(None, "user_version", 20).unwrap(),
            _ => unreachable!(),
        }
        drop(sql);
        let before = database_bytes(&db);
        let error = match Controller::open_with_config(&db, cfg) {
            Ok(_) => panic!("corrupt source accepted: {corruption}"),
            Err(e) => e,
        };
        assert!(
            error.contains("retained") || error.contains("metadata"),
            "{corruption}: {error}"
        );
        assert_eq!(
            database_bytes(&db),
            before,
            "refused reopen changed {corruption}"
        );
    }
}

// Real prior-schema producer; never create an alleged old fixture by lowering
// the version of a new-schema database. The explicit image is runner-owned.
const OLD20: &str = "ebeda84269b6b6acf646b9da41f7f5244bccef80e0aae9ffd66281febc3cbd05";
fn image_hash(path: &Path) {
    let out = Command::new("/usr/bin/sha256sum")
        .arg("--")
        .arg(path)
        .output()
        .unwrap();
    assert!(out.status.success());
    assert_eq!(
        String::from_utf8(out.stdout)
            .unwrap()
            .split_whitespace()
            .next(),
        Some(OLD20)
    );
}
struct OldController(std::process::Child);
impl OldController {
    async fn stop(mut self) {
        assert!(
            Command::new("/bin/kill")
                .args(["-INT", "--", &self.0.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let status = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(s) = self.0.try_wait().unwrap() {
                    break s;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(status.success(), "{status}");
    }
}
impl Drop for OldController {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}
struct OldClient {
    client: reqwest::Client,
    origin: String,
    body_bytes: usize,
}
impl OldClient {
    async fn call(&self, command: Operation) -> Result<Reply, String> {
        let request = Request {
            protocol_revision: 19,
            request_id: id("historical20"),
            command,
        };
        let bytes = serde_json::to_vec(&request).unwrap();
        assert!(bytes.len() <= self.body_bytes);
        let mut response = self
            .client
            .post(format!("{}/api/operations", self.origin))
            .header("content-type", "application/json")
            .body(bytes)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !response.status().is_success() {
            return Err(format!("HTTP {}", response.status()));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|e| e.to_string())? {
            if bytes.len().saturating_add(chunk.len()) > self.body_bytes {
                return Err("old reply bound".into());
            }
            bytes.extend_from_slice(&chunk);
        }
        let reply: Reply = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
        assert_eq!(reply.request_id, request.request_id);
        reply.validate_for(&request.command)?;
        Ok(reply)
    }
}
fn rows(path: &Path) -> BTreeMap<String, Vec<Vec<rusqlite::types::Value>>> {
    let sql =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    let mut q = sql
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name LIMIT 65")
        .unwrap();
    let tables = q
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(tables.len() <= 64);
    let mut all = BTreeMap::new();
    for table in tables {
        assert!(table.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
        let q = sql
            .prepare(&format!("SELECT * FROM \"{table}\" LIMIT 65"))
            .unwrap();
        let columns = q.column_count();
        assert!(columns <= 32);
        drop(q);
        let order = (1..=columns)
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let mut q = sql
            .prepare(&format!(
                "SELECT * FROM \"{table}\" ORDER BY {order} LIMIT 65"
            ))
            .unwrap();
        let values = q
            .query_map([], |r| {
                (0..columns)
                    .map(|i| r.get(i))
                    .collect::<Result<Vec<rusqlite::types::Value>, _>>()
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(values.len() <= 64);
        all.insert(table, values);
    }
    all
}
fn version(path: &Path) -> i64 {
    rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .unwrap()
        .pragma_query_value(None, "user_version", |r| r.get(0))
        .unwrap()
}
#[tokio::test]
#[ignore = "requires exact preserved schema20 producer BORG_SCHEMA20_BINARY"]
async fn genuine_schema20_refused_instructions_call_migrates_unchanged_and_old_reader_refuses21() {
    let image = PathBuf::from(
        std::env::var_os("BORG_SCHEMA20_BINARY").expect("explicit genuine schema20 producer"),
    );
    assert!(image.is_absolute());
    image_hash(&image);
    let root = root();
    let db = root.join("task.db");
    let cfg = config();
    let metadata = root.join("old20-limits.json");
    std::fs::write(&metadata, serde_json::to_vec(&*cfg.limits).unwrap()).unwrap();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);
    let origin = format!("http://{address}");
    let spawn = |label: &str| {
        OldController(
            Command::new(&image)
                .arg("controller")
                .arg("--limits")
                .arg(&metadata)
                .arg("--db")
                .arg(&db)
                .arg("--listen")
                .arg(address.to_string())
                .arg("--origin")
                .arg(&origin)
                .stdin(std::process::Stdio::null())
                .stdout(std::fs::File::create(root.join(format!("{label}.stdout"))).unwrap())
                .stderr(std::fs::File::create(root.join(format!("{label}.stderr"))).unwrap())
                .spawn()
                .unwrap(),
        )
    };
    let mut old = spawn("producer");
    let client = OldClient {
        client: reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(1))
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap(),
        origin: origin.clone(),
        body_bytes: cfg.limits.transport.body_bytes,
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert!(old.0.try_wait().unwrap().is_none());
            if let Ok(reply) = client
                .call(Operation::ListTasks {
                    after: None,
                    limit: 1,
                })
                .await
            {
                assert!(matches!(reply.outcome, Outcome::Ok { .. }));
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    let registration = registration("work", "r1", "work.git", "checkpoint.git");
    entry(
        client
            .call(Operation::RegisterRepository {
                operation_id: id("old-register"),
                registration: registration.clone(),
            })
            .await
            .unwrap(),
    );
    let op = Operation::SubmitTask {
        operation_id: id("old-submit"),
        input: input(registration.binding(), &"a".repeat(40), true),
    };
    let accepted_receipt = client.call(op.clone()).await.unwrap();
    let accepted = task(accepted_receipt.clone());
    assert!(accepted.input.instruction_source.is_none());
    let worker = Worker {
        id: id("old-worker"),
        incarnation: id("old-inc"),
        native_capacity: 0,
        coding_capacity: 1,
    };
    assert!(matches!(
        client
            .call(Operation::RegisterWorker {
                operation_id: id("old-register-worker"),
                worker: worker.clone()
            })
            .await
            .unwrap()
            .outcome,
        Outcome::Ok { .. }
    ));
    let commands = client
        .call(Operation::AwaitCommands {
            operation_id: id("old-await"),
            worker_id: worker.id.clone(),
            incarnation: worker.incarnation.clone(),
            acknowledged: vec![],
        })
        .await
        .unwrap();
    let attempt = match commands.outcome {
        Outcome::Ok {
            data: Data::Commands(commands),
        } => commands
            .into_iter()
            .find_map(|v| {
                if let WorkerCommand::RunAttempt { attempt, .. } = v {
                    Some(attempt)
                } else {
                    None
                }
            })
            .unwrap(),
        other => panic!("{other:?}"),
    };
    let current = task(
        client
            .call(Operation::ObserveWorker {
                report: Report {
                    report_id: id("old-started"),
                    worker_id: worker.id.clone(),
                    incarnation: worker.incarnation.clone(),
                    task_id: accepted.id.clone(),
                    attempt_id: attempt.id.clone(),
                    generation: attempt.generation,
                    observation: Observation::Started {
                        thread_id: None,
                        turn_id: None,
                    },
                },
            })
            .await
            .unwrap(),
    );
    let caller = Caller {
        task_id: accepted.id.clone(),
        attempt_id: attempt.id,
        generation: attempt.generation,
        worker_id: worker.id,
        incarnation: worker.incarnation,
    };
    // Generic old argument JSON can contain an unsupported selector and has a
    // real refused receipt. It must not be mistaken for accepted new vocabulary.
    let rejected = read_call(&caller).operation();
    let refusal = client.call(rejected.clone()).await.unwrap();
    assert!(matches!(refusal.outcome, Outcome::Refused { .. }));
    assert!(refusal.commit.is_some());
    let cancelled = task(
        client
            .call(Operation::CancelTask {
                operation_id: id("old-cancel"),
                task_id: current.id,
                expected_revision: current.revision,
            })
            .await
            .unwrap(),
    );
    old.stop().await;
    owner_released(&db).await;
    assert_eq!(version(&db), 20);
    let before = rows(&db);
    for _ in 0..2 {
        let c = Controller::open_with_config(&db, cfg.clone()).unwrap();
        let replay = call(&c, op.clone()).await;
        assert_eq!(replay.outcome, accepted_receipt.outcome);
        assert_eq!(replay.commit, accepted_receipt.commit);
        let replay = call(&c, rejected.clone()).await;
        assert_eq!(replay.outcome, refusal.outcome);
        assert_eq!(replay.commit, refusal.commit);
        assert_eq!(
            task(
                call(
                    &c,
                    Operation::GetTask {
                        task_id: accepted.id.clone()
                    }
                )
                .await
            ),
            cancelled
        );
        drop(c);
        owner_released(&db).await;
        assert_eq!(version(&db), 21);
        assert_eq!(rows(&db), before);
    }
    let bytes = database_bytes(&db);
    let mut reader = spawn("old-reader");
    let status = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = reader.0.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(!status.success());
    let error = std::fs::read_to_string(root.join("old-reader.stderr")).unwrap();
    assert!(
        error.contains("unsupported") && error.contains("schema"),
        "{error}"
    );
    assert_eq!(database_bytes(&db), bytes);
    assert_eq!(version(&db), 21);
    assert_eq!(rows(&db), before);
    image_hash(&image);
}
