//! Durable pre-check checkpoint publication, not executor-history resumption.
use super::*;
use std::io::Read;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReportProof {
    report: Report,
    commit: MutationCommit,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Phase {
    revision: u32,
    command: WorkerCommand,
    association: ReportProof,
    completion: ReportProof,
    prepared: OperationRecord,
    prepare_commit: MutationCommit,
    checkpoint: Checkpoint,
    pub(super) settlement_id: String,
    retired_for_checks: bool,
    publish_may_have_started: bool,
}
pub(super) fn path(state: &Path, attempt: &Attempt) -> PathBuf {
    state
        .join("publications")
        .join(format!("{}.json", component(&attempt.id)))
}
async fn call(client: &ApiClient, operation: Operation) -> Result<Reply, String> {
    tokio::time::timeout(
        Duration::from_millis(client.limits().worker.publication_call_timeout_ms),
        client.call(operation),
    )
        .await
        .map_err(|_| "checkpoint recovery controller lookup timed out (worker.publication_call_timeout_ms)")?
}
fn report_identity(report: &Report, attempt: &Attempt) -> bool {
    report.worker_id == attempt.worker_id
        && report.incarnation == attempt.worker_incarnation
        && report.task_id == attempt.task_id
        && report.attempt_id == attempt.id
        && report.generation == attempt.generation
}
async fn report_record(
    client: &ApiClient,
    attempt: &Attempt,
    id: &str,
) -> Result<(ReportProof, Outcome), String> {
    let reply = call(
        client,
        Operation::GetOperation {
            operation_id: id.into(),
        },
    )
    .await?;
    let commit = reply
        .commit
        .ok_or("completed report has no original committed receipt")?;
    let Outcome::Ok {
        data: Data::Operation(record),
    } = reply.outcome
    else {
        return Err("completed report unavailable".into());
    };
    let operation: Operation = serde_json::from_value(record.intended.clone())
        .map_err(|_| "invalid completed report request")?;
    if serde_json::to_value(&operation).map_err(|_| "completed report encoding failed")?
        != record.intended
        || record.kind != "application_mutation"
        || commit.operation_id != id
    {
        return Err("completed report identity mismatch".into());
    }
    let Operation::ObserveWorker { report } = operation else {
        return Err("completed report has wrong operation kind".into());
    };
    if report.report_id != id || !report_identity(&report, attempt) {
        return Err("completed report owner mismatch".into());
    }
    let outcome: Outcome =
        serde_json::from_value(record.outcome.ok_or("completed report outcome missing")?)
            .map_err(|_| "invalid completed report outcome")?;
    if !matches!(outcome, Outcome::Ok { .. } | Outcome::Refused { .. }) {
        return Err("report has no committed final disposition".into());
    }
    Ok((ReportProof { report, commit }, outcome))
}
async fn report_proof(
    client: &ApiClient,
    attempt: &Attempt,
    id: &str,
) -> Result<ReportProof, String> {
    let (proof, outcome) = report_record(client, attempt, id).await?;
    if !matches!(
        outcome,
        Outcome::Ok {
            data: Data::Task(_)
        }
    ) {
        return Err("completed report was not accepted".into());
    }
    Ok(proof)
}

async fn verify_report(
    client: &ApiClient,
    attempt: &Attempt,
    original: &ReportProof,
) -> Result<(), String> {
    let fresh = report_proof(client, attempt, &original.report.report_id).await?;
    if fresh.report != original.report || fresh.commit != original.commit {
        return Err("original completed report changed".into());
    }
    Ok(())
}
fn intent(attempt: &Attempt, checkpoint: &Checkpoint) -> OperationRecord {
    OperationRecord {
        id: checkpoint.operation_id.clone(),
        task_id: attempt.task_id.clone(),
        attempt_id: attempt.id.clone(),
        generation: attempt.generation,
        kind: "checkpoint".into(),
        intended: json!({"registration":checkpoint.registration,"repository":checkpoint.repository,"commit":checkpoint.commit,"remote_ref":checkpoint.remote_ref}),
        state: "pending".into(),
        outcome: None,
    }
}
fn same_intent(a: &OperationRecord, b: &OperationRecord) -> bool {
    a.id == b.id
        && a.task_id == b.task_id
        && a.attempt_id == b.attempt_id
        && a.generation == b.generation
        && a.kind == b.kind
        && a.intended == b.intended
}
fn command_limits(command: &WorkerCommand) -> Result<&crate::limits::RuntimeLimits, String> {
    let WorkerCommand::RunAttempt { task, .. } = command else {
        return Err("publication has no original run command".into());
    };
    // The full retained command binds both the values and their source metadata.
    // A recovery read must not reinterpret this phase using current defaults.
    task.execution_limits()?;
    Ok(&task.resolved_limits.as_ref().ok_or("publication limits missing")?.global_metadata)
}
impl Phase {
    fn save(&self, file: &Path) -> Result<(), String> {
        self.validate(&self.command)?;
        bounded_json_size(self, command_limits(&self.command)?.worker.publication_record_bytes)
            .map_err(|_| "publication phase exceeds bound (worker.publication_record_bytes)")?;
        atomic_json(file, self)
    }
    fn validate(&self, command: &WorkerCommand) -> Result<(), String> {
        super::validate_command_session(command)?;
        let WorkerCommand::RunAttempt { attempt, task, .. } = command else {
            return Err("publication has no original run command".into());
        };
        let limits = command_limits(command)?;
        let repository = task
            .repository
            .as_ref()
            .ok_or("publication repository missing")?;
        if self.settlement_id.len() > limits.transport.identity_bytes {
            return Err("invalid original checkpoint publication phase (transport.identity_bytes)".into());
        }
        if self.revision != 2
            || &self.command != command
            || task.definition.executor != ExecutorKind::Coding
            || self.checkpoint
                != git::checkpoint(
                    repository,
                    &attempt.task_id,
                    &attempt.id,
                    self.checkpoint.commit.clone(),
                )?
            || !git::valid_commit(&self.checkpoint.commit)
            || crate::credential::repository_contains_credentials(&self.checkpoint.repository)
            || self.prepared != intent(attempt, &self.checkpoint)
            || self.prepare_commit.operation_id != self.checkpoint.operation_id
            || self.prepare_commit.controller_revision == 0
            || (self.retired_for_checks && !self.publish_may_have_started)
            || self.settlement_id.is_empty()
            || self.settlement_id == self.checkpoint.operation_id
            || !report_identity(&self.association.report, attempt)
            || !report_identity(&self.completion.report, attempt)
            || self.association.report.report_id != format!("{}-completed-association", attempt.id)
            || self.completion.report.report_id != format!("{}-completed-executor", attempt.id)
            || !matches!(&self.association.report.observation,Observation::Started{thread_id:Some(t),turn_id:Some(u)} if !t.is_empty() && !u.is_empty())
            || !matches!(
                &self.completion.report.observation,
                Observation::ExecutorOutcome { success: true, .. }
            )
        {
            return Err("invalid original checkpoint publication phase".into());
        }
        Ok(())
    }
    pub(super) fn mark_started(&mut self, state: &Path, attempt: &Attempt) -> Result<(), String> {
        if self.publish_may_have_started || self.retired_for_checks {
            return Err("publication already entered; exact reconciliation required".into());
        }
        self.publish_may_have_started = true;
        self.save(&path(state, attempt))
    }
    pub(super) fn retire(&mut self, state: &Path, attempt: &Attempt) -> Result<(), String> {
        self.retired_for_checks = true;
        self.save(&path(state, attempt))
    }
}
pub(super) fn read(file: &Path, command: &WorkerCommand) -> Result<Option<Phase>, String> {
    let record_bytes = command_limits(command)?.worker.publication_record_bytes;
    let file = match std::fs::File::open(file) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("publication phase unavailable".into()),
    };
    let mut bytes = Vec::new();
    let read_bound = u64::try_from(record_bytes)
        .ok()
        .and_then(|bytes| bytes.checked_add(1))
        .ok_or("publication phase bound exceeds reader representation (worker.publication_record_bytes)")?;
    file.take(read_bound)
        .read_to_end(&mut bytes)
        .map_err(|_| "publication phase unreadable")?;
    if bytes.len() > record_bytes {
        return Err("publication phase exceeds bound (worker.publication_record_bytes)".into());
    }
    let phase: Phase =
        serde_json::from_slice(&bytes).map_err(|_| "corrupt or legacy publication phase")?;
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| "corrupt publication phase")?;
    if serde_json::to_value(&phase).map_err(|_| "publication encoding failed")? != value {
        return Err("publication contains unsupported nested fields".into());
    }
    phase.validate(command)?;
    Ok(Some(phase))
}

pub(super) async fn prepare(
    state: &Path,
    command: &WorkerCommand,
    reporter: &Reporter,
    checkpoint: &Checkpoint,
    control: &Control,
) -> Result<Phase, String> {
    super::validate_command_session(command)?;
    let limits = command_limits(command)?;
    if reporter.client.limits() != limits || control.runtime_limits.as_ref() != limits {
        return Err("publication client limits differ from original command".into());
    }
    let attempt = &reporter.attempt;
    let association = report_proof(
        &reporter.client,
        attempt,
        &format!("{}-completed-association", attempt.id),
    )
    .await?;
    let completion = report_proof(
        &reporter.client,
        attempt,
        &format!("{}-completed-executor", attempt.id),
    )
    .await?;
    control.check()?;
    let expected = intent(attempt, checkpoint);
    let request = Operation::PrepareOperation {
        operation_id: checkpoint.operation_id.clone(),
        task_id: attempt.task_id.clone(),
        attempt_id: attempt.id.clone(),
        generation: attempt.generation,
        kind: expected.kind.clone(),
        intended: expected.intended.clone(),
    };
    let reply = call(&reporter.client, request).await?;
    let prepare_commit = reply
        .commit
        .ok_or("checkpoint prepare was not durably acknowledged")?;
    let Outcome::Ok {
        data: Data::Operation(prepared),
    } = reply.outcome
    else {
        return Err("checkpoint prepare refused".into());
    };
    if prepared != expected {
        return Err("checkpoint prepare does not match original publication".into());
    }
    let phase = Phase {
        revision: 2,
        command: command.clone(),
        association,
        completion,
        prepared,
        prepare_commit,
        checkpoint: checkpoint.clone(),
        settlement_id: mutations::fresh_id("settlement")?,
        retired_for_checks: false,
        publish_may_have_started: false,
    };
    phase.validate(command)?;
    let file = path(state, attempt);
    if file.exists() {
        return Err("existing publication phase requires reconciliation".into());
    }
    phase.save(&file)?;
    control.check()?;
    resource::verify(&reporter.resource)?;
    Ok(phase)
}

fn original_task(task: &Task, command: &WorkerCommand) -> bool {
    if super::validate_command_session(command).is_err() {
        return false;
    }
    let WorkerCommand::RunAttempt {
        session,
        attempt,
        task: input,
        continuation,
        ..
    } = command
    else {
        return false;
    };
    let mut accepted = task.input.clone();
    accepted.limits.model_requests = input.limits.model_requests;
    task.id == attempt.task_id
        && task.session_id == session.id
        && task.assigned_agent_id == session.agent_id
        && accepted == *input
        && task.completed_child == *continuation
        && task.attempt.as_ref().is_some_and(|a| {
            a.id == attempt.id
                && a.session_id == session.id
                && a.generation == attempt.generation
                && a.worker_id == attempt.worker_id
                && a.worker_incarnation == attempt.worker_incarnation
                && a.deadline_unix_ms == attempt.deadline_unix_ms
        })
}
fn failed_candidate(observation: &Observation) -> bool {
    matches!(observation,Observation::ResultCandidate{checks_passed:false,checkpoint:None,validation,output:None,..} if validation.is_empty())
}
async fn historical_completion(
    client: &ApiClient,
    state: &Path,
    phase: &Phase,
    attempt: &Attempt,
) -> Result<bool, String> {
    let Some(journal) = mutations::read(
        &mutations::settlement_path(state, &phase.checkpoint.operation_id),
        client.limits(),
    )?
    else {
        return Ok(false);
    };
    validate_settlement(&journal.request, phase, attempt)?;
    let acceptance::Lookup::Recorded(
        Outcome::Ok {
            data: Data::Operation(settled),
        },
        _,
    ) = acceptance::lookup(client, &phase.settlement_id, &journal.request).await?
    else {
        return Ok(false);
    };
    if settled.state != "settled" || !same_intent(&settled, &phase.prepared) {
        return Ok(false);
    }
    let (stop, outcome) = report_record(
        client,
        attempt,
        &format!("{}-publication-recovery-stop", attempt.id),
    )
    .await?;
    if stop.report.observation != Observation::ProcessTreeStopped
        || !matches!(outcome,Outcome::Ok{data:Data::Task(ref task)} if original_task(task,&phase.command)&&task.attempt.as_ref().is_some_and(|a|a.stopped))
    {
        return Ok(false);
    }
    let (candidate, _) = report_record(
        client,
        attempt,
        &format!("{}-publication-recovery-result", attempt.id),
    )
    .await?;
    // These historical receipts close only the old local launch. They never
    // authorize a mutation against the controller's successor generation.
    Ok(failed_candidate(&candidate.report.observation))
}
async fn terminal_refusal(
    client: &ApiClient,
    phase: &Phase,
    attempt: &Attempt,
) -> Result<bool, String> {
    let (candidate, outcome) = report_record(
        client,
        attempt,
        &format!("{}-publication-recovery-result", attempt.id),
    )
    .await?;
    if !failed_candidate(&candidate.report.observation)
        || !matches!(outcome, Outcome::Refused { .. })
    {
        return Ok(false);
    }
    let reply = call(
        client,
        Operation::GetTask {
            task_id: attempt.task_id.clone(),
        },
    )
    .await?;
    Ok(
        matches!(reply.outcome,Outcome::Ok{data:Data::Task(ref task)} if original_task(task,&phase.command)
        && matches!(task.status,TaskStatus::Cancelled|TaskStatus::Failed|TaskStatus::Succeeded)
        &&task.attempt.as_ref().is_some_and(|a|a.stopped)),
    )
}

async fn recover_one(
    config: &WorkerConfig,
    client: &ApiClient,
    state: &Path,
    resource: &Option<Arc<resource::Envelope>>,
    outbox_lock: &Arc<tokio::sync::Mutex<()>>,
    phase: &Phase,
    attempt: &Attempt,
    input: &TaskInput,
    validation_runtime: &Arc<crate::native::ValidationRuntime>,
) -> Result<(), String> {
    verify_report(client, attempt, &phase.association).await?;
    verify_report(client, attempt, &phase.completion).await?;
    let reply = call(
        client,
        Operation::GetTask {
            task_id: attempt.task_id.clone(),
        },
    )
    .await?;
    let Outcome::Ok {
        data: Data::Task(task),
    } = reply.outcome
    else {
        return Err("publication original task unavailable".into());
    };
    let same_task = original_task(&task, &phase.command);
    let reply = call(
        client,
        Operation::GetOperation {
            operation_id: phase.checkpoint.operation_id.clone(),
        },
    )
    .await?;
    let Outcome::Ok {
        data: Data::Operation(original),
    } = reply.outcome
    else {
        return Err("publication original prepare unavailable".into());
    };
    if !same_intent(&original, &phase.prepared)
        || !matches!(original.state.as_str(), "pending" | "uncertain" | "settled")
    {
        return Err("publication original prepare changed".into());
    }
    // External GetOperation is current state, not the original mutation stamp.
    // After establishing that exact durable intent already exists, replay only
    // its identical preparation ID to verify the original immutable ACK.
    let prepared = call(
        client,
        Operation::PrepareOperation {
            operation_id: phase.prepared.id.clone(),
            task_id: attempt.task_id.clone(),
            attempt_id: attempt.id.clone(),
            generation: attempt.generation,
            kind: phase.prepared.kind.clone(),
            intended: phase.prepared.intended.clone(),
        },
    )
    .await?;
    if prepared.commit.as_ref() != Some(&phase.prepare_commit)
        || !matches!(prepared.outcome,Outcome::Ok{data:Data::Operation(ref record)} if record==&phase.prepared)
    {
        return Err("publication original prepare acknowledgment changed".into());
    }
    if !same_task {
        if historical_completion(client, state, phase, attempt).await? {
            return Ok(());
        }
        return Err("publication belongs to a superseded or changed task".into());
    }
    // Startup owns WorkerLease and has already reaped the original process tree.
    // Local cleanup cannot prove that a remote receiver has stopped. Once
    // publication may have been entered, empty readback remains unresolved.
    let registry = state.join("processes").join(component(&attempt.id));
    process::reconcile_processes(&registry, &config.limits.worker).await?;
    let (_cancel, receiver) = watch::channel(false);
    // Reconciliation has its own bounded cleanup deadline, not a new execution
    // allowance. No task deadline, request allowance or attempt is changed.
    let control = Control {
        deadline_ms: process::now_ms()
            .checked_add(config.limits.worker.publication_recovery_timeout_ms)
            .ok_or("recovery clock exhausted (worker.publication_recovery_timeout_ms)")?,
        worker_limits: Arc::new(config.limits.worker.clone()),
        runtime_limits: Arc::new(command_limits(&phase.command)?.clone()),
        validation_runtime: validation_runtime.clone(),
        cancel: receiver,
    };
    let settlement_path = mutations::settlement_path(state, &phase.checkpoint.operation_id);
    let request = if let Some(saved) = mutations::read(&settlement_path, client.limits())? {
        validate_settlement(&saved.request, phase, attempt)?;
        if phase.publish_may_have_started
            && matches!(
                &saved.request,
                Operation::SettleOperation {
                    checkpoint: None,
                    ..
                }
            )
        {
            let accepted = acceptance::lookup(client, &phase.settlement_id, &saved.request).await?;
            if !matches!(accepted,acceptance::Lookup::Recorded(Outcome::Ok{data:Data::Operation(ref record)},_) if record.state=="settled"&&same_intent(record,&phase.prepared)&&record.outcome==Some(json!({"acknowledged":true,"not_applied":true})))
            {
                return Err(
                    "possibly sent publication has no committed not-applied decision".into(),
                );
            }
        }
        saved.request
    } else {
        let directory = state.join("publication-readback");
        process::private_directory(&directory)?;
        resource::verify(resource)?;
        let observed =
            git::remote_commit(&directory, &phase.checkpoint, &registry, &control).await?;
        let checkpoint = match observed {
            Some(commit) if commit == phase.checkpoint.commit => Some(phase.checkpoint.clone()),
            None if !phase.publish_may_have_started => None,
            None => return Err("empty readback cannot settle a possibly sent publication".into()),
            Some(_) => {
                return Err(
                    "original checkpoint ref has different content; reconciliation blocked".into(),
                );
            }
        };
        Operation::SettleOperation {
            settlement_id: phase.settlement_id.clone(),
            operation_id: phase.checkpoint.operation_id.clone(),
            task_id: attempt.task_id.clone(),
            attempt_id: attempt.id.clone(),
            generation: attempt.generation,
            outcome: if checkpoint.is_some() {
                json!({"acknowledged":true})
            } else {
                json!({"acknowledged":true,"not_applied":true})
            },
            checkpoint,
        }
    };
    validate_settlement(&request, phase, attempt)?;
    let data = mutations::settle(client, state, request, &control).await?;
    if !matches!(data,Data::Operation(ref record) if record.state=="settled"&&same_intent(record,&phase.prepared))
    {
        return Err("publication settlement not established".into());
    }
    let mut original_config = config.clone();
    original_config.incarnation = attempt.worker_incarnation.clone();
    let reporter = Reporter {
        resource: resource.clone(),
        client: client.clone(),
        config: original_config,
        attempt: attempt.clone(),
        outbox: state.join("outbox"),
        sequence: Arc::new(AtomicU64::new(1)),
        executor_seen: Arc::new(AtomicBool::new(true)),
        provider_uncertain: Arc::new(AtomicBool::new(false)),
        uncertainty_retained: Arc::new(AtomicBool::new(false)),
        output_limit: input.limits.output_bytes,
        result_limit: input.limits.result_bytes,
        coding_limits: input.execution_limits()?.coding.clone(),
        outbox_lock: outbox_lock.clone(),
    };
    if !reporter
        .send_identified(
            format!("{}-publication-recovery-stop", attempt.id),
            Observation::ProcessTreeStopped,
        )
        .await?
    {
        return Err("publication recovered stop awaits acknowledgment".into());
    }
    if !reporter.send_identified(format!("{}-publication-recovery-result",attempt.id),Observation::ResultCandidate{checks_passed:false,summary:"Worker restarted after completed execution; original checkpoint publication was reconciled. Acceptance checks were not resumed.".into(),checkpoint:None,validation:vec![],output:None}).await? {
        if !terminal_refusal(client,phase,attempt).await? {
            return Err("publication recovery result awaits acknowledgment".into());
        }
    }
    #[cfg(debug_assertions)]
    checkpoint_qualification_cut(
        state,
        "recovered_terminal",
        attempt,
        &phase.checkpoint,
        &control,
    )
    .await?;
    Ok(())
}
fn validate_settlement(
    request: &Operation,
    phase: &Phase,
    attempt: &Attempt,
) -> Result<(), String> {
    let Operation::SettleOperation {
        settlement_id,
        operation_id,
        task_id,
        attempt_id,
        generation,
        outcome,
        checkpoint,
    } = request
    else {
        return Err("publication settlement kind differs".into());
    };
    if settlement_id != &phase.settlement_id
        || operation_id != &phase.checkpoint.operation_id
        || task_id != &attempt.task_id
        || attempt_id != &attempt.id
        || generation != &attempt.generation
        || !((checkpoint.as_ref() == Some(&phase.checkpoint)
            && *outcome == json!({"acknowledged":true}))
            || (checkpoint.is_none()
                && *outcome == json!({"acknowledged":true,"not_applied":true})))
    {
        return Err("publication settlement identity differs".into());
    }
    Ok(())
}
pub(super) async fn recover(
    config: &WorkerConfig,
    client: &ApiClient,
    state: &Path,
    resource: &Option<Arc<resource::Envelope>>,
    outbox_lock: &Arc<tokio::sync::Mutex<()>>,
    validation_runtime: &Arc<crate::native::ValidationRuntime>,
) -> Result<(), String> {
    for (file, mut receipt) in command_receipts(state, client.limits())? {
        if receipt.completed || !receipt.effects_started {
            continue;
        }
        let WorkerCommand::RunAttempt { attempt, task, .. } = &receipt.command else {
            continue;
        };
        if task.definition.executor != ExecutorKind::Coding {
            continue;
        }
        let Some(phase) = read(&path(state, attempt), &receipt.command)? else {
            continue;
        };
        if phase.retired_for_checks {
            continue;
        }
        if attempt.worker_id != config.id {
            return Err("publication belongs to another worker".into());
        }
        let original_client = ApiClient::new(
            config.controller_url.clone(),
            Arc::new(command_limits(&receipt.command)?.clone()),
        )?;
        recover_one(
            config,
            &original_client,
            state,
            resource,
            outbox_lock,
            &phase,
            attempt,
            task,
            validation_runtime,
        )
        .await?;
        // All original settlement and terminal report acknowledgments precede
        // completion. A dropped response replays the same IDs on next recovery.
        receipt.completed = true;
        bounded_json_size(&receipt, client.limits().worker.command_record_bytes)
            .map_err(|_| "publication command receipt exceeds bound (worker.command_record_bytes)")?;
        atomic_json(&file, &receipt)?;
    }
    Ok(())
}

#[cfg(test)]
mod catalogue_tests {
    use super::*;

    #[test]
    // Traceability scope: Worker intent from a fixed accepted-shaped repository fixture; tuple mismatch and serialization checks, not controller admission or actual Git.
    //= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
    //= type=test
    //# A checkpoint publication MUST identify its accepted repository registration revision.
    fn publication_intent_binds_registration_and_destination_independently() {
        let attempt = Attempt {
            id: "attempt".into(), session_id: "session".into(), task_id: "task".into(),
            generation: 1, worker_id: "worker".into(), worker_incarnation: "incarnation".into(),
            phase: AttemptPhase::Running, deadline_unix_ms: 1000, stopped: false, uncertain: false,
        };
        let repository = Repository {
            registration: Some(crate::repository_catalogue::Revision {
                repository_id: "repository".into(), registration_revision: "revision-one".into(),
            }),
            url: "/fixture/source.git".into(), start_commit: "a".repeat(40),
            checkpoint_remote: "/fixture/checkpoints.git".into(),
        };
        let checkpoint = git::checkpoint(&repository, &attempt.task_id, &attempt.id, "b".repeat(40)).unwrap();
        let original = intent(&attempt, &checkpoint);
        assert_eq!(original.intended["registration"], serde_json::to_value(repository.registration.as_ref().unwrap()).unwrap());
        assert_eq!(original.intended["repository"], repository.checkpoint_remote);
        assert_ne!(original.intended["repository"], repository.url);
        let round_trip: OperationRecord = serde_json::from_slice(&serde_json::to_vec(&original).unwrap()).unwrap();
        assert!(same_intent(&original, &round_trip));
        let mut changed = checkpoint.clone();
        changed.registration.registration_revision = "revision-two".into();
        assert!(!same_intent(&original, &intent(&attempt, &changed)));
        changed = checkpoint.clone();
        changed.repository = repository.url.clone();
        assert!(!same_intent(&original, &intent(&attempt, &changed)));
        let mut legacy = original.clone();
        legacy.intended.as_object_mut().unwrap().remove("registration");
        assert!(!same_intent(&original, &legacy));
    }
}
