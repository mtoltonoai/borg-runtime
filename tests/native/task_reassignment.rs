//! Actual local controller and worker, one controlled native response, then the
//! existing controlled Coding app-server fixture. No external provider claim.
use super::*;
use borg_runtime::collaboration::{Query, QueryResult};
use borg_runtime::charter::{CharterObservation, CharterOutcome, CharterQuery, CharterQueryResult, CharterResultReceipt, DefinitionCharter, CharterSource, CharterFile, CharterClauseRef};
use std::os::unix::fs::PermissionsExt;

fn git_at(root: &Path, args: &[&str]) -> String {
    let output = Command::new("/usr/bin/git")
        .args(["-c", "core.hooksPath=/dev/null", "-c", "user.name=Handoff Fixture",
            "-c", "user.email=handoff@example.invalid"])
        .args(args).current_dir(root).output().unwrap();
    assert!(output.status.success(), "fixture git: {}", String::from_utf8_lossy(&output.stderr));
    String::from_utf8(output.stdout).unwrap().trim().into()
}

async fn lineage(api: &ApiClient, session_id: &str) -> (Session, TaskInput, bool) {
    let operation = Operation::QueryCollaboration {
        query: Query::SessionInput { session_id: session_id.into() },
    };
    let reply = api.call(operation.clone()).await.unwrap(); reply.validate_for(&operation).unwrap();
    let Outcome::Ok { data: Data::Collaboration(QueryResult::SessionInput { session, input, current }) }
        = reply.outcome else { panic!("exact session input required: {reply:?}") };
    (session, *input, current)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// Traceability scope: Real worker quiescence, controller receipts, Coding checkpoint and checks; native and Coding providers are explicitly controlled local peers.
//= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
//= type=test
//# An accepted reassignment to a different agent MUST create a distinct session for the same task identity.
//= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
//= type=test
//# A task reassignment MUST retain the task's prior session and execution-input history.
//= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
//= type=test
//# A reassigned session MUST NOT execute until the prior execution is known unable to continue.
//= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
//= type=test
//# A task reassignment MUST preserve the task's accepted deadline.
//= spec/impl/v2/implementation.md#task-reassignment-creates-a-new-session
//= type=test
//# A task reassignment MUST preserve its recorded usage accounting.
async fn native_self_handoff_stops_before_same_task_coding_checkpoint_and_original_result() {
    Box::pin(journey()).await;
}

async fn journey() {
    let mut lab = Lab::new("same-task-native-coding");
    let mut metadata = (*lab.limits).clone(); metadata.worker.coding_capacity = 1;
    // Exact small policy from native/charter_worker.rs, also used by its
    // charter_callers child; no new retry/timeout or execution allowance.
    metadata.charter = Some(borg_runtime::limits::CharterLimits {
        record_bytes: 262_144, source_bytes: 65_536, files: 8, clauses: 32,
        metadata_bytes: 16_384, references: 32, reply_items: 32, tool_calls: 8,
        reference_depth: 8, records: 1024, page_items: 32, page_bytes: 262_144,
    });
    metadata.execution.native.context_bytes = 262_144;
    super::charter_worker::configure_native_schema(&mut metadata);
    metadata.validate().unwrap(); lab.limits = Arc::new(metadata);
    std::fs::write(&lab.worker_limits_file, serde_json::to_vec_pretty(&*lab.limits).unwrap()).unwrap();
    let seed = lab.root.join("seed"); std::fs::create_dir(&seed).unwrap();
    std::fs::write(seed.join("answer.txt"), "before\n").unwrap();
    git_at(&seed, &["init", "-q"]); git_at(&seed, &["add", "answer.txt"]);
    git_at(&seed, &["commit", "-qm", "original source"]);
    let start_commit = git_at(&seed, &["rev-parse", "HEAD"]);
    let remote = lab.root.join("remote.git");
    git_at(&lab.root, &["clone", "--bare", "--quiet", seed.to_str().unwrap(), remote.to_str().unwrap()]);
    let gate = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coding_peer = lab.root.join("codex_protocol.py");
    std::fs::write(&coding_peer, include_str!("../recovery/fixtures/codex_protocol.py")).unwrap();
    std::fs::set_permissions(&coding_peer, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(coding_peer.with_extension("json"), json!({"port":gate.local_addr().unwrap().port()}).to_string()).unwrap();
    let mut controller = lab.controller("controller").await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let registration = borg_runtime::repository_catalogue::Revision {
        repository_id: "handoff-source".into(), registration_revision: "1".into(),
    };
    let reply = api.call(Operation::RegisterRepository {
        operation_id: "handoff-register-source".into(),
        registration: borg_runtime::repository_catalogue::Registration {
            pull_request_target: None, repository_id: registration.repository_id.clone(),
            registration_revision: registration.registration_revision.clone(), display_name: "Handoff source".into(),
            source_endpoint: remote.display().to_string(), checkpoint_endpoint: remote.display().to_string(),
            source_store_key: "remote.git".into(), checkpoint_store_key: "remote.git".into(),
        },
    }).await.unwrap();
    assert!(matches!(reply.outcome, Outcome::Ok { data: Data::RepositoryRegistration(_) }));
    let mut input = lab.input(2);
    input.definition.revision = "r1".into();
    // Same inline source binding as charter_callers::input; the Coding target
    // receives its own source identity when its explicit definition is registered.
    let sentence = "The agent MUST retain the r1 task purpose.";
    input.definition.charter = Some(DefinitionCharter::Inline { source: CharterSource {
        files: vec![CharterFile { file: "charter.md".into(), text: format!("# Purpose\n\n{sentence}\n") }],
        clauses: vec![CharterClauseRef { file: "charter.md".into(), section: "Purpose".into(), sentence: sentence.into() }],
    }});
    input.instruction = "Hand this same task to the configured Coding agent; set answer.txt to after and satisfy the original result contract.".into();
    input.output_schema = Some(json!({"type":"object","properties":{"observed_usage":{
        "type":"object","properties":{"input_tokens":{"type":"null"},"output_tokens":{"type":"null"},
            "model_requests":{"type":"null"}},"required":["input_tokens","output_tokens","model_requests"],
        "additionalProperties":false}},"required":["observed_usage"],"additionalProperties":false}));
    let mut coding_definition = input.definition.clone();
    coding_definition.id = "handoff-coding".into(); coding_definition.revision = "1".into();
    coding_definition.executor = ExecutorKind::Coding;
    let registered = api.call(Operation::RegisterAgent {
        operation_id: "handoff-register-agent".into(), definition: coding_definition.clone(),
    }).await.unwrap();
    assert!(matches!(registered.outcome, Outcome::Ok { data: Data::Agent(_) }));
    let checks = vec![vec!["/bin/sh".into(), "-c".into(), "test \"$(cat answer.txt)\" = after".into()]];
    input.native_workflow = Some(Box::new(NativeWorkflow::Reassign { target: CodingTaskTemplate {
        instruction_source: None, pull_request: None, instruction_prefix: String::new(), definition: coding_definition.clone(),
        repository: Repository { registration: Some(registration.clone()), url: String::new(),
            checkpoint_remote: String::new(), start_commit, pull_request_target: None },
        acceptance_commands: checks.clone(), dependencies: input.dependencies.clone(), limits: input.limits.clone(),
    }}));
    let original = submit(&api, input).await;
    let charged = Arc::new(Mutex::new(None::<Task>));
    let peer = Peer::start_async({
        let api = api.clone(); let task_id = original.id.clone(); let agent_id = coding_definition.id.clone();
        let charged = charged.clone();
        move |index, _request| {
            let api = api.clone(); let task_id = task_id.clone(); let agent_id = agent_id.clone(); let charged = charged.clone();
            async move {
                assert_eq!(index, 0, "handoff cannot request a native continuation or create a child");
                // The request reservation is already charged. Read the real CAS
                // revision via the API; never guess it or bypass revision zero.
                let current = get(&api, &task_id).await;
                let revision = current.revision;
                assert!(current.attempt.as_ref().is_some_and(|a| !a.stopped));
                *charged.lock().unwrap() = Some(current);
                let mut response = function(index, "collab_task_assign", json!({
                    "task_id":task_id,"expected_revision":revision,"agent_id":agent_id,
                }));
                // This controlled peer supplies no token counts. Unknown usage
                // stays null and does not introduce a later token-report CAS.
                response["usage"] = Value::Null;
                Some(response)
            }
        }
    }).await;
    let mut worker = lab.worker_with_codex(&lab.url, &peer.endpoint, "handoff-worker", &coding_peer);
    let (mut coding, _) = tokio::time::timeout(Duration::from_secs(30), gate.accept()).await.unwrap().unwrap();
    let observation = tokio::time::timeout(Duration::from_secs(30), async {
        let mut bytes = Vec::new();
        loop { let byte = coding.read_u8().await.unwrap(); if byte == b'\n' { break; }
            bytes.push(byte); assert!(bytes.len() <= 4096); }
        serde_json::from_slice::<Value>(&bytes).unwrap()
    }).await.unwrap();
    assert_eq!(observation["before"], "before\n");
    let charged = charged.lock().unwrap().clone().expect("actual native request CAS read");
    let first = charged.attempt.as_ref().unwrap();
    let current = get(&api, &original.id).await;
    let second = current.attempt.as_ref().unwrap().clone();
    assert_eq!(current.id, original.id); assert_ne!(current.session_id, original.session_id);
    assert_eq!(current.assigned_agent_id, coding_definition.id); assert_eq!(current.input.definition, coding_definition);
    assert_eq!(current.input.instruction, original.input.instruction);
    assert_eq!(current.input.output_schema, original.input.output_schema);
    assert_eq!(current.input.limits, original.input.limits); assert_eq!(current.input.dependencies, original.input.dependencies);
    assert_eq!(current.input.acceptance_commands, checks); assert!(current.input.native_workflow.is_none());
    assert!(current.pending_assignment.is_none() && current.parent_task_id.is_none() && current.waiting_child_id.is_none());
    assert_eq!(second.session_id, current.session_id); assert_eq!(second.generation, first.generation + 1);
    assert_eq!(second.deadline_unix_ms, first.deadline_unix_ms); assert!(!second.stopped && !second.uncertain);
    let (old_session, old_input, old_current) = lineage(&api, &original.session_id).await;
    assert_eq!(old_session.definition, original.input.definition); assert_eq!(old_input, original.input); assert!(!old_current);
    let (_, new_input, new_current) = lineage(&api, &current.session_id).await;
    assert_eq!(new_input, current.input); assert!(new_current);
    for (session_id, definition) in [(&original.session_id, &original.input.definition), (&current.session_id, &current.input.definition)] {
        let reply = api.call(Operation::QueryCharter { query: CharterQuery::Session { session_id: session_id.clone() } }).await.unwrap();
        let Outcome::Ok { data: Data::CharterQuery(CharterQueryResult::Session(Some(binding))) } = reply.outcome
            else { panic!("each actual session has its own accepted charter") };
        assert_eq!(binding.session.session_id, *session_id);
        assert_eq!(binding.session.effective.definition_id, definition.id);
        assert_eq!(binding.session.effective.definition_revision, definition.revision);
    }
    let attempts = api.call(Operation::QueryCollaboration {
        query: Query::TaskAttempts { task_id: original.id.clone(), after: None, limit: 3 },
    }).await.unwrap();
    let Outcome::Ok { data: Data::Collaboration(QueryResult::TaskAttempts(page)) } = attempts.outcome
        else { panic!("actual attempt history required") };
    assert!(page.next_cursor.is_none()); assert_eq!(page.items.len(), 2);
    let stopped = page.items.iter().find(|view| view.attempt.id == first.id).unwrap();
    assert!(stopped.attempt.stopped && !stopped.attempt.uncertain);
    assert_eq!(stopped.attempt.phase, AttemptPhase::Stopped);
    assert_eq!(stopped.reported_model_requests, 1); assert_eq!((stopped.input_tokens, stopped.output_tokens), (None, None));
    assert_eq!(peer.count(), 1);
    coding.write_all(b"G").await.unwrap(); drop(coding);
    let finished = until(&api, &original.id, |task| task.status == TaskStatus::Succeeded
        && task.attempt.as_ref().is_some_and(|a| a.stopped)).await;
    assert_eq!(finished.session_id, current.session_id); assert_eq!(finished.input, current.input);
    assert!(finished.parent_task_id.is_none() && finished.waiting_child_id.is_none() && finished.completed_child.is_none());
    let result = finished.result.as_ref().unwrap();
    assert_eq!(result.output, Some(json!({"observed_usage":{"input_tokens":null,"output_tokens":null,"model_requests":null}})));
    let checkpoint = result.checkpoint.as_ref().expect("actual accepted Coding checkpoint");
    assert_eq!(checkpoint.registration, registration);
    assert_eq!(git_at(&remote, &["rev-parse", &checkpoint.remote_ref]), checkpoint.commit);
    assert_eq!(git_at(&remote, &["show", &format!("{}:answer.txt", checkpoint.commit)]), "after");
    assert_eq!(result.validation, vec![format!("check 1 at {}: pass", checkpoint.commit)],
        "the original configured check ran against the exact accepted checkpoint");
    assert!(matches!(api.call(Operation::ListTasks { after: None, limit: 2 }).await.unwrap().outcome,
        Outcome::Ok { data: Data::Tasks(tasks) } if tasks.len() == 1 && tasks[0].id == original.id));
    assert_eq!(peer.count(), 1);
    worker.stop().await; controller.stop().await; drop(worker); drop(controller);
    for label in ["handoff-worker", "controller"] {
        let closed: Value = serde_json::from_slice(&std::fs::read(lab.root.join(format!("{label}-closed.json"))).unwrap()).unwrap();
        assert_eq!(closed["reaped"], true); assert_eq!(closed["code"], 0);
    }
    // No live SQLite reader: both actual process owners have closed above.
    let db = rusqlite::Connection::open_with_flags(lab.root.join("tasks.sqlite"), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    assert_eq!(db.query_row("SELECT count(*) FROM tasks", [], |r| r.get::<_, i64>(0)).unwrap(), 1);
    assert_eq!(db.query_row("SELECT count(*) FROM sessions", [], |r| r.get::<_, i64>(0)).unwrap(), 2);
    assert_eq!(db.query_row("SELECT count(*) FROM attempts", [], |r| r.get::<_, i64>(0)).unwrap(), 2);
    let bound = i64::try_from(lab.limits.worker.command_records).unwrap();
    let commands = db.prepare("SELECT body FROM commands WHERE task_id=?1 ORDER BY seq LIMIT ?2").unwrap()
        .query_map(rusqlite::params![original.id, bound + 1], |r| r.get::<_, String>(0)).unwrap()
        .map(|row| serde_json::from_str::<WorkerCommand>(&row.unwrap()).unwrap()).collect::<Vec<_>>();
    assert!(commands.len() <= bound as usize);
    let runs: Vec<_> = commands.iter().filter_map(|command| match command {
        WorkerCommand::RunAttempt { session, attempt, task, continuation, .. } => Some((session, attempt, task, continuation)),
        _ => None,
    }).collect();
    assert_eq!(runs.len(), 2); assert_eq!(runs[0].1.id, first.id); assert_eq!(runs[1].1.id, second.id);
    assert_eq!(runs[0].0.id, original.session_id); assert_eq!(runs[1].0.id, current.session_id);
    assert!(runs.iter().all(|run| run.3.is_none()));
    let mut remaining = current.input.clone(); remaining.limits.model_requests = original.input.limits.model_requests - 1;
    assert_eq!(runs[1].2, &remaining, "the successor Run retains the one consumed native request");
    assert!(commands.iter().any(|command| matches!(command, WorkerCommand::StopAttempt { attempt_id, .. } if attempt_id == &first.id)));
    let bound = i64::try_from(lab.limits.controller.max_receipts).unwrap();
    let receipts = db.prepare("SELECT request,outcome,controller_revision FROM receipts ORDER BY controller_revision LIMIT ?1").unwrap()
        .query_map([bound + 1], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?))).unwrap()
        .map(|row| { let (request, outcome, revision) = row.unwrap(); let revision = u64::try_from(revision).unwrap();
            (serde_json::from_str::<Operation>(&request).unwrap(), serde_json::from_str::<Outcome>(&outcome).unwrap(), revision) })
        .collect::<Vec<_>>();
    assert!(receipts.len() <= bound as usize);
    let assigns: Vec<_> = receipts.iter().filter(|(operation, _, _)| matches!(operation,
        Operation::CallCollaboration { call } if call.tool == "collab_task_assign" && call.caller.attempt_id == first.id)).collect();
    assert_eq!(assigns.len(), 1);
    let (Operation::CallCollaboration { call }, Outcome::Ok { data: Data::Task(pending) }, assign_revision) = assigns[0]
        else { panic!("actual committed native assignment required") };
    assert_eq!(call.arguments["expected_revision"], json!(charged.revision));
    assert_eq!(pending.session_id, original.session_id); assert_eq!(pending.input, original.input);
    let pending_assignment = pending.pending_assignment.as_ref().unwrap();
    assert_eq!(pending_assignment.session.id, current.session_id);
    assert_eq!(pending_assignment.from_session_id, original.session_id);
    assert!(pending.attempt.as_ref().is_some_and(|a| a.id == first.id && !a.stopped && a.phase == AttemptPhase::StopRequested));
    let stops: Vec<_> = receipts.iter().filter(|(operation, outcome, _)| matches!(operation,
        Operation::ObserveWorker { report } if report.attempt_id == first.id && report.observation == Observation::ProcessTreeStopped)
        && matches!(outcome, Outcome::Ok { .. })).collect();
    assert_eq!(stops.len(), 1);
    let deliveries: Vec<_> = receipts.iter().filter(|(operation, outcome, _)| matches!(operation, Operation::AwaitCommands { .. })
        && matches!(outcome, Outcome::Ok { data: Data::Commands(commands) }
            if commands.iter().any(|command| matches!(command, WorkerCommand::RunAttempt { attempt, .. } if attempt.id == second.id)))).collect();
    assert!(!deliveries.is_empty());
    let completions: Vec<_> = receipts.iter().filter(|(operation, outcome, _)| matches!(operation,
        Operation::ObserveCharter { caller, observation: CharterObservation::CompleteTurn { .. }, .. }
            if caller.task_id == original.id && caller.attempt_id == first.id)
        && matches!(outcome, Outcome::Ok { data: Data::Charter(CharterOutcome::Turn(_)) })).collect();
    assert_eq!(completions.len(), 1, "the predecessor's real charter turn closes despite its self-stop");
    let (Operation::ObserveCharter { operation_id, observation: CharterObservation::CompleteTurn { turn_id, results }, .. },
        Outcome::Ok { data: Data::Charter(CharterOutcome::Turn(turn)) }, completion_revision) = completions[0]
        else { unreachable!() };
    assert_eq!(results, &vec![CharterResultReceipt::Collaboration { call_id: call.logical_call_id.clone(), operation_id: call.operation_id.clone() }]);
    assert_eq!(&turn.boundary.turn_id, turn_id); assert_eq!(turn.completed_by.as_ref(), Some(operation_id));
    assert_eq!(&turn.delivered_results, results);
    assert!(*assign_revision < *completion_revision && deliveries.iter().all(|delivery| *completion_revision < delivery.2),
        "the successor cannot run while the predecessor's real charter turn remains open");
    assert!(*assign_revision < stops[0].2 && deliveries.iter().all(|delivery| stops[0].2 < delivery.2),
        "the successor Run is delivered only after the prior real stopped report commits");
    assert_eq!(lab.usage(&first.id), (1, None, None));
    std::fs::write(lab.root.join("same-task-reassignment.json"), serde_json::to_vec_pretty(&json!({
        "scope":"actual local worker/controller and Git, controlled native and Coding providers",
        "original":original,"charged_native":charged,"coding_started":current,"finished":finished,
        "coding_observation":observation,"commands":commands,"assignment_receipt":assigns[0],
        "stopped_receipt":stops[0],"charter_completion_receipt":completions[0],"successor_delivery_receipts":deliveries,"native_posts":peer.count(),
    })).unwrap()).unwrap();
}
