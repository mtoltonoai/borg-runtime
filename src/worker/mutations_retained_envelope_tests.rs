//! Mixed-policy command admission and the actual startup mutation consumer.
//! The copied proposed profile is a local envelope fixture, not adopted policy.
use super::*;
use crate::charter::{CharterClauseRef, CharterFile, CharterSource, DefinitionCharter};

fn policies() -> (Arc<RuntimeLimits>, RuntimeLimits) {
    let old: RuntimeLimits = serde_json::from_str(include_str!("testdata/retained-startup-envelope.json")).unwrap();
    let new: RuntimeLimits = serde_json::from_str(include_str!("testdata/proposed-controller-envelope.json")).unwrap();
    old.validate().unwrap();
    new.validate().unwrap();
    assert_eq!(old.transport.body_bytes, 1_048_576);
    assert_eq!(old.worker.mutation_record_bytes, 3_145_728);
    assert_eq!(new.transport.body_bytes, 67_108_864);
    assert_eq!(new.controller.command_page_bytes, old.controller.command_page_bytes);
    assert_eq!(new.controller.max_commands_per_reply, 32);
    (Arc::new(old), new)
}
fn task_input(label: &str, limits: &RuntimeLimits) -> TaskInput {
    let mut t = input(label);
    t.limits.deadline_unix_ms = process::now_ms() + limits.task_defaults.deadline_ms;
    t.limits.model_requests = limits.task_defaults.model_requests;
    t.limits.output_bytes = limits.controller.max_task_output_bytes;
    t.definition.charter = Some(DefinitionCharter::Inline { source: CharterSource {
        files: vec![CharterFile { file: "charter.md".into(), text: "# Purpose\n\nThe agent MUST preserve the original input.\n".into() }],
        clauses: vec![CharterClauseRef { file: "charter.md".into(), section: "Purpose".into(), sentence: "The agent MUST preserve the original input.".into() }],
    }});
    t
}
async fn api_task(client: &ApiClient, op: Operation) -> Task {
    match client.call(op).await.unwrap().outcome {
        Outcome::Ok { data: Data::Task(t) } => *t,
        other => panic!("expected task: {other:?}"),
    }
}
async fn post_report(client: &ApiClient, a: &Attempt, id: &str, observation: Observation) -> Task {
    api_task(client, Operation::ObserveWorker { report: Report {
        report_id: id.into(), worker_id: a.worker_id.clone(), incarnation: a.worker_incarnation.clone(),
        task_id: a.task_id.clone(), attempt_id: a.id.clone(), generation: a.generation, observation,
    }}).await
}
fn wait_for(w: &Worker, ack: Vec<String>) -> Operation {
    Operation::AwaitCommands { operation_id: fresh_id("await").unwrap(), worker_id: w.id.clone(), incarnation: w.incarnation.clone(), acknowledged: ack }
}
fn command_list(reply: Reply) -> Vec<WorkerCommand> {
    match reply.outcome { Outcome::Ok { data: Data::Commands(v) } => v, other => panic!("commands: {other:?}") }
}

#[tokio::test]
async fn retained_startup_await_loss_recovers_near_full_page_with_original_policy() {
    let (old, selected) = policies();
    let f = Fixture::with_policies("retained-envelope", 1_000_000, selected, old.clone()).await;
    let accepted = Arc::new(f._controller.limits().clone());
    let admin = ApiClient::new(&f.controller_url, accepted.clone()).unwrap();
    // These are the real startup maxima, including worst permitted JSON escaping.
    let w = Worker { id: "\\".repeat(old.worker.identity_bytes), incarnation: "\"".repeat(old.worker.identity_bytes),
        coding_capacity: old.worker.coding_capacity, native_capacity: old.worker.native_capacity };
    assert!(register(&f.client, &f.state, w.clone()).await.unwrap());
    let mut originals = Vec::new();
    let mut ack = Vec::new();
    for batch in 0..4 {
        for n in 0..8 {
            let label = format!("small-{batch}-{n}");
            api_task(&admin, Operation::SubmitTask { operation_id: format!("submit-{label}"), input: task_input(&label, &accepted) }).await;
        }
        let commands = command_list(f.client.call(wait_for(&w, ack.clone())).await.unwrap());
        assert_eq!(commands.len(), 8);
        for (n, c) in commands.into_iter().enumerate() {
            let WorkerCommand::RunAttempt { command_id, attempt, .. } = &c else { panic!() };
            post_report(&admin, attempt, &format!("stop-{batch}-{n}"), Observation::ProcessTreeStopped).await;
            let t = post_report(&admin, attempt, &format!("fail-{batch}-{n}"), Observation::ResultCandidate {
                checks_passed: false, summary: "controlled no-executor fixture".into(), checkpoint: None, validation: vec![], output: None,
            }).await;
            assert_eq!(t.status, TaskStatus::Failed);
            ack.push(command_id.clone());
            originals.push(c);
        }
    }
    for command in &originals {
        let (id, _) = crate::worker::command_identity(command);
        crate::worker::record_launch(&f.state.join("commands").join(format!("{}.json", component(id))), command, &old).unwrap();
    }
    assert_eq!(crate::worker::pending_acknowledgments(&f.state, &w.incarnation, &old).unwrap().len(), 1,
        "the actual producer is more conservative than this maximum-controller-ACK stress case");
    assert_eq!(ack.len(), 32);
    assert_eq!(ack.iter().collect::<std::collections::BTreeSet<_>>().len(), 32);
    let mut sample = originals[0].clone();
    let WorkerCommand::RunAttempt { task, .. } = &mut sample else { panic!() };
    task.instruction.clear();
    let overhead = serde_json::to_vec(&sample).unwrap().len();
    let desired = (accepted.controller.command_page_bytes - 5 - 128) / 4;
    let instruction_len = desired.checked_sub(overhead).unwrap();
    for n in 0..4 {
        let label = format!("large-0-{n}"); // same byte length as the sample's agent ID
        let mut t = task_input(&label, &accepted);
        t.instruction = "x".repeat(instruction_len);
        api_task(&admin, Operation::SubmitTask { operation_id: format!("submit-{label}"), input: t }).await;
    }
    let op = wait_for(&w, ack);
    let id = identity(&op).unwrap().to_owned();
    let path = f.state.join("mutations/await.json");
    let mut pending = super::super::create(&path, op.clone(), 7, &old).unwrap();
    let before = std::fs::read(&path).unwrap();
    *f.drop_id.lock().unwrap() = Some(id.clone());
    assert!(resolve(&f.client, &path, &mut pending).await.unwrap().is_none());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    // Read the actual original receipt through the larger administrative client
    // solely to measure it; startup resolution below still uses the old client.
    let lookup_op = Operation::GetOperation { operation_id: id.clone() };
    let lookup = admin.call(lookup_op.clone()).await.unwrap();
    let Outcome::Ok { data: Data::Operation(record) } = &lookup.outcome else { panic!("{lookup:?}") };
    assert_eq!(record.intended, serde_json::to_value(&op).unwrap());
    let original_outcome: Outcome = serde_json::from_value(record.outcome.clone().unwrap()).unwrap();
    let Outcome::Ok { data: Data::Commands(delivered) } = &original_outcome else { panic!("{original_outcome:?}") };
    assert_eq!(delivered.len(), 4);
    let page_bytes = serde_json::to_vec(delivered).unwrap().len();
    assert!(page_bytes <= accepted.controller.command_page_bytes);
    assert!(page_bytes > accepted.controller.command_page_bytes - 512, "must exercise a genuinely full page: {page_bytes}");
    let lookup_bytes = serde_json::to_vec(&lookup).unwrap().len();
    println!("ENVELOPE page={page_bytes} original_request={} lookup={lookup_bytes} old_body={} journal_bound={} fixture={}", serde_json::to_vec(&op).unwrap().len(), old.transport.body_bytes, old.worker.mutation_record_bytes, f.state.display());
    assert!(lookup_bytes <= old.transport.body_bytes, "original GetOperation exceeds retained startup body");
    let mut reopened = super::super::read(&path, &old).unwrap().unwrap();
    let result = resolve(&f.client, &path, &mut reopened).await.unwrap().unwrap();
    assert_eq!(result, original_outcome);
    assert_eq!(reopened.outcome_context.as_ref().unwrap().commit, lookup.commit.unwrap());
    assert_eq!(serde_json::to_value(&reopened.request).unwrap(), serde_json::to_value(&op).unwrap());
    assert_eq!(reopened.limits, *old);
    let disk = super::super::read(&path, &old).unwrap().unwrap();
    assert_eq!(disk.outcome.as_ref(), Some(&original_outcome));
    assert!(super::super::read(&path, &accepted).is_err(), "startup journal must not silently adopt task metadata");
    assert_eq!(f.sends.lock().unwrap().iter().filter(|v| identity(v).ok() == Some(&id)).count(), 1, "lost response recovery must not resend Await");
    crate::worker::recover_wait(&f.client, &f.state, &w.id).await.unwrap();
    assert!(super::super::read(&path, &old).unwrap().unwrap().consumed);
    for command in &originals {
        let (id, _) = crate::worker::command_identity(command);
        let marker = std::fs::read(f.state.join("acks").join(component(id))).unwrap();
        assert_eq!(serde_json::from_slice::<String>(&marker).unwrap(), id);
    }
    for command in delivered {
        let WorkerCommand::RunAttempt { task, .. } = command else { panic!() };
        assert_eq!(*task.resolved_limits.as_ref().unwrap().global_metadata, *accepted);
        assert_ne!(*task.resolved_limits.as_ref().unwrap().global_metadata, *old);
        let (id, _) = crate::worker::command_identity(command);
        let command_path = f.state.join("commands").join(format!("{}.json", component(id)));
        assert_eq!(crate::worker::read_receipt(&command_path, &old).unwrap().command, *command);
        assert!(std::fs::metadata(command_path).unwrap().len() <= old.worker.command_record_bytes as u64);
    }
}

#[test]
fn retained_startup_single_ack_maximum_serialized_envelopes_fit() {
    let (old, selected) = policies();
    // All identity bytes may escape twice; no ASCII-only assumption about
    // retained IDs or wall-clock/sequence width is needed for this bound.
    let id = "\\".repeat(old.transport.identity_bytes);
    let op = Operation::AwaitCommands {
        operation_id: id.clone(), worker_id: "\\".repeat(old.worker.identity_bytes),
        incarnation: "\"".repeat(old.worker.identity_bytes), acknowledged: vec![id.clone()],
    };
    let empty = Outcome::Ok { data: Data::Commands(vec![]) };
    let context = CommitContext { commit: MutationCommit { operation_id: id.clone(), controller_revision: i64::MAX as u64 }, observed_controller_revision: i64::MAX as u64 };
    let initial = Reply { request_id: id.clone(), commit: Some(context.commit.clone()), observed_controller_revision: Some(context.observed_controller_revision), outcome: empty.clone() };
    let lookup = Reply { outcome: Outcome::Ok { data: Data::Operation(OperationRecord {
        id: id.clone(), task_id: String::new(), attempt_id: String::new(), generation: 0,
        kind: "application_mutation".into(), intended: serde_json::to_value(&op).unwrap(),
        state: "settled".into(), outcome: Some(serde_json::to_value(&empty).unwrap()),
    }) }, ..initial.clone() };
    // Replace the one empty command array (2 bytes) by the entire allowed page.
    // Values are embedded as JSON, never quoted JSON strings: no second escape.
    let initial_max = serde_json::to_vec(&initial).unwrap().len() - 2 + selected.controller.command_page_bytes;
    let lookup_max = serde_json::to_vec(&lookup).unwrap().len() - 2 + selected.controller.command_page_bytes;
    let journal = Journal { limits: (*old).clone(), revision: 3, request: op,
        outcome: Some(empty), outcome_context: Some(context), consumed: false, recovery_epoch: u64::MAX };
    let journal_max = serde_json::to_vec(&journal).unwrap().len() - 2 + selected.controller.command_page_bytes;
    // Likewise the existing local receipt adds exactly this wrapper to one Run.
    let receipt_wrapper = b"{\"command\":null,\"completed\":false,\"effects_started\":false}".len() - 4;
    let command_max = selected.controller.run_command_bytes + receipt_wrapper;
    println!("MAXIMUM_ENVELOPES initial={initial_max} lookup={lookup_max} body={} journal={journal_max} journal_limit={} command={command_max} command_limit={}", old.transport.body_bytes, old.worker.mutation_record_bytes, old.worker.command_record_bytes);
    assert!(initial_max <= old.transport.body_bytes);
    assert!(lookup_max <= old.transport.body_bytes);
    assert!(journal_max <= old.worker.mutation_record_bytes);
    assert!(command_max <= old.worker.command_record_bytes);
    assert_eq!(selected.execution.native.context_bytes, u32::MAX as usize);
    assert_eq!(selected.execution.native.response_wire_bytes, u32::MAX as usize);
}
