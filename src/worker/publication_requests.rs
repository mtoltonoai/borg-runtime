//! Owned publication commands in the ordinary worker. The command and original
//! effect are durable before work starts. Recovery can only close a never-sent
//! request or read the original ref/marker; it never dispatches a second write.
use super::*;
use crate::{mcp::publication as p, pull_request as pr};
use super::pull_request::{Effect, validate_effect};
use std::io::Read;
use serde_json::Value;
use std::os::unix::fs::OpenOptionsExt;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Phase {
    revision: u32,
    command_sha256: String,
    effect: Effect,
    unknown_recorded: bool,
    retired: bool,
}
fn context(command: &WorkerCommand) -> Result<(&str, &p::Command, &RuntimeLimits), String> {
    let WorkerCommand::PublishRepository { command_id, request } = command else { return Err("publication command expected".into()); };
    request.validate(command_id)?;
    Ok((command_id, request, &request.input.resolved_limits.as_ref().ok_or("publication limits absent")?.global_metadata))
}
fn path(state: &Path, id: &str) -> PathBuf { state.join("publication-requests").join(format!("{}.json", component(id))) }
fn registry(state: &Path, id: &str) -> PathBuf { state.join("processes").join(format!("publication-{}", component(id))) }
impl Phase {
    fn new(command: &WorkerCommand) -> Result<Self, String> {
        let (_, request, limits) = context(command)?;
        Ok(Self { revision: 1, command_sha256: p::digest(command)?, effect: Effect::new(request.effect(limits)?)?, unknown_recorded: false, retired: false })
    }
    fn validate(&self, command: &WorkerCommand) -> Result<(), String> {
        let (_, request, limits) = context(command)?;
        if self.revision != 1 || self.command_sha256 != p::digest(command)?
            || self.unknown_recorded && !self.effect.may_have_started
            || self.retired && !self.effect.closed() { return Err("publication phase binding differs".into()); }
        validate_effect(&self.effect, &request.effect(limits)?, &pr::Bounds::from_limits(limits))?;
        bounded_json_size(self, limits.worker.publication_record_bytes).map_err(|_| "publication phase exceeds worker.publication_record_bytes")?;
        Ok(())
    }
    fn save(&self, state: &Path, command: &WorkerCommand) -> Result<(), String> {
        self.validate(command)?;
        let (id, _, limits) = context(command)?;
        let value = serde_json::to_value(self).map_err(|_| "publication phase encoding failed")?;
        if value_contains_credential(&value, &configured_credentials(limits)?, &limits.worker)? {
            return Err("publication phase contains a configured credential".into());
        }
        process::private_directory(&state.join("publication-requests"))?;
        atomic_json(&path(state, id), self)
    }
}
fn read(state: &Path, command: &WorkerCommand) -> Result<Option<Phase>, String> {
    let (id, _, limits) = context(command)?;
    match std::fs::symlink_metadata(state.join("publication-requests")) {
        Ok(m) if !m.is_dir() => return Err("publication phase root is not an owned directory".into()),
        Ok(_) => {}, Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("publication phase root unavailable".into()),
    }
    let file = match std::fs::OpenOptions::new().read(true).custom_flags(0x20000 | 0x800).open(path(state, id)) {
        Ok(file) => file, Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("publication phase unavailable".into()),
    };
    if !file.metadata().map_err(|_| "publication phase metadata unavailable")?.is_file() { return Err("publication phase is not a regular file".into()); }
    let mut bytes = Vec::new();
    file.take(limits.worker.publication_record_bytes as u64 + 1).read_to_end(&mut bytes).map_err(|_| "publication phase unreadable")?;
    if bytes.len() > limits.worker.publication_record_bytes { return Err("publication phase exceeds worker.publication_record_bytes".into()); }
    let phase: Phase = serde_json::from_slice(&bytes).map_err(|_| "publication phase corrupt or unsupported")?;
    if serde_json::to_value(&phase).map_err(|_| "publication phase encoding failed")? != serde_json::from_slice::<Value>(&bytes).map_err(|_| "publication phase JSON invalid")? {
        return Err("publication phase has unsupported nested fields".into());
    }
    phase.validate(command)?;
    Ok(Some(phase))
}

/// Pure future-envelope sizing. These maximum-shaped values are never persisted
/// or treated as observations. Controller admission calls this before commit.
pub(crate) fn preflight(command: &WorkerCommand) -> Result<(), String> {
    let (_, request, limits) = context(command)?;
    bounded_json_size(&Receipt { command: command.clone(), completed: true, effects_started: true }, limits.worker.command_record_bytes)
        .map_err(|_| "publication command exceeds worker.command_record_bytes")?;
    let mut phase = Phase::new(command)?;
    let stamp = |id: &str| MutationCommit { operation_id: id.into(), controller_revision: i64::MAX as u64 };
    phase.effect.prepare_commit = Some(stamp(&phase.effect.intended.id));
    phase.effect.begin()?;
    phase.unknown_recorded = true;
    phase.effect.outcome = Some(if let Some(intent) = &request.create {
        let t = &request.head.target;
        let mut url = reqwest::Url::parse(&t.api_base).map_err(|_| "publication API origin invalid")?;
        if url.host_str() == Some("api.github.com") { url.set_host(Some("github.com")).map_err(|_| "publication URL invalid")?; }
        url.set_path(&format!("/{}/{}/pull/{}", t.owner, t.repository, u64::MAX));
        let branch = |name: String, commit: String| pr::BranchProof { repository_id: t.repository_id, owner: t.owner.clone(), repository: t.repository.clone(), branch: name, commit };
        let proof = pr::Proof { operation_id: request.head.binding.operation_id.clone(), provider_id: u64::MAX, number: u64::MAX,
            url: url.to_string(), head: branch(request.head.head_ref.strip_prefix("refs/heads/").ok_or("publication head invalid")?.into(), request.head.binding.checkpoint.commit.clone()),
            base: branch(t.base_branch.clone(), "f".repeat(64)), title: intent.title.clone(), body: intent.body.clone(), marker: intent.marker.clone() };
        pr::validate_proof(intent, &proof, &pr::Bounds::from_limits(limits))?;
        json!(pr::Settlement { acknowledged: true, proof })
    } else { pull_request::head_outcome(&request.head)? });
    let mut settled = phase.effect.intended.clone(); settled.state = "settled".into(); settled.outcome = phase.effect.outcome.clone();
    phase.effect.settled = Some(settled.clone()); phase.retired = true;
    phase.validate(command)?;
    for (op, outcome) in [(phase.effect.request(), Outcome::Ok { data: Data::Operation(phase.effect.intended.clone()) }),
        (phase.effect.settlement()?, Outcome::Ok { data: Data::Operation(settled) })] {
        pull_request::receipt_preflight(limits, &op, outcome.clone())?;
        // Prepare lives in the shared Effect, not in the separate settlement
        // journal whose closed operation vocabulary intentionally excludes it.
        if !matches!(op, Operation::SettleOperation { .. }) { continue; }
        let mut journal = mutations::prepare(op.clone(), 0, limits)?;
        journal.outcome = Some(outcome); journal.outcome_context = Some(CommitContext { commit: stamp(op.mutation_id().ok_or("publication mutation missing")?), observed_controller_revision: i64::MAX as u64 });
        bounded_json_size(&journal, limits.worker.mutation_record_bytes).map_err(|_| "publication journal exceeds worker.mutation_record_bytes")?;
    }
    // Bound future public evidence under both accepted tool-result and transport
    // carriers before allowing the branch or create to become externally visible.
    let binding = request.head.publication.as_ref().unwrap();
    let head = pr::HeadProof { operation_id: request.head.head_operation_id.clone(), target_revision: request.head.target.revision.clone(),
        remote: request.head.target.head_remote.clone(), remote_ref: request.head.head_ref.clone(), commit: request.head.binding.checkpoint.commit.clone() };
    let public = p::View { publication_id: binding.publication_id.clone(), source_task_id: binding.source.task_id.clone(), source_revision: binding.source.task_revision,
        source_attempt_id: binding.source.checkpoint_attempt_id.clone(), checkpoint_operation_id: request.head.binding.checkpoint.operation_id.clone(),
        registration: request.head.binding.registration.clone(), target_revision: request.head.target.revision.clone(), commit: request.head.binding.checkpoint.commit.clone(),
        branch: p::ActionView { state: p::State::Completed, refusal: None, head: Some(head), pull_request: None },
        pull_request: if request.create.is_some() { Some(p::ActionView { state: p::State::Completed, refusal: None, head: None,
            pull_request: Some(pr::reference_from_record(phase.effect.settled.as_ref().unwrap(), &pr::Bounds::from_limits(limits))?) }) } else { None } };
    let data = Data::Publication(Box::new(public));
    bounded_json_size(&data, limits.interface.mcp_data_bytes).map_err(|_| "publication evidence exceeds interface.mcp_data_bytes")?;
    let outcome = Outcome::Ok { data };
    pull_request::receipt_preflight(limits, &Operation::CallCollaboration { call: request.invocation.clone() }, outcome.clone())?;
    let frame = crate::mcp::CallResult { reply: Reply { request_id: "x".repeat(limits.transport.identity_bytes),
        commit: Some(stamp(&request.invocation.operation_id)), observed_controller_revision: Some(i64::MAX as u64), outcome }, requires_quiescence: false };
    bounded_json_size(&frame, request.input.execution_limits()?.collaboration.complete_result_bytes)
        .map_err(|_| "publication result exceeds execution.collaboration.complete_result_bytes")?;
    Ok(())
}

fn unknown_request(phase: &Phase) -> Result<Operation, String> {
    let r = &phase.effect.intended;
    Ok(Operation::SettleOperation { settlement_id: format!("publication-unknown-{}", p::digest(&r.id)?), operation_id: r.id.clone(),
        task_id: r.task_id.clone(), attempt_id: r.attempt_id.clone(), generation: r.generation,
        outcome: json!({"acknowledged":false,"publication_outcome":"unknown"}), checkpoint: None })
}
fn refuse_effect(phase: &mut Phase, reason: p::Refusal) -> Result<(), String> {
    if phase.effect.may_have_started { return Err("possible publication cannot be refused as not dispatched".into()); }
    phase.effect.outcome = Some(json!(pr::NotApplied { acknowledged: true, not_applied: true, refusal: Some(reason) }));
    Ok(())
}
async fn current(client: &ApiClient, request: &p::Command, control: &Control) -> Result<(), p::Refusal> {
    if control.check().is_err() { return Err(if process::now_ms() >= control.deadline_ms { p::Refusal::Deadline } else { p::Refusal::Cancelled }); }
    let caller = request.caller();
    let reply = pull_request::call(client, Operation::GetTask { task_id: caller.task_id.clone() }, control).await.map_err(|_| p::Refusal::StaleCaller)?;
    let Outcome::Ok { data: Data::Task(task) } = reply.outcome else { return Err(p::Refusal::StaleCaller); };
    if task.status == TaskStatus::Cancelled { return Err(p::Refusal::Cancelled); }
    if task.status != TaskStatus::Active || task.input != request.input || task.session_id != request.session.id
        || task.attempt.as_ref().is_none_or(|a| a.id != caller.attempt_id || a.generation != caller.generation || a.worker_id != caller.worker_id
            || a.worker_incarnation != caller.incarnation || a.stopped || a.uncertain || a.phase == AttemptPhase::StopRequested)
        || process::now_ms() >= task.input.limits.deadline_unix_ms { return Err(p::Refusal::StaleCaller); }
    let reply = pull_request::call(client, Operation::QueryCollaboration { query: crate::collaboration::Query::Worker { worker_id: caller.worker_id.clone() } }, control)
        .await.map_err(|_| p::Refusal::StaleCaller)?;
    if !matches!(reply.outcome, Outcome::Ok { data: Data::Collaboration(crate::collaboration::QueryResult::Worker(ref w)) } if w.worker.incarnation==caller.incarnation) { return Err(p::Refusal::StaleCaller); }
    control.check().map_err(|_| p::Refusal::Cancelled)
}
fn reconciliation_control(control: &Control) -> Result<(watch::Sender<bool>, Control), String> {
    let (sender, cancel) = watch::channel(false);
    Ok((sender, Control { deadline_ms: process::now_ms().checked_add(control.worker_limits.publication_recovery_timeout_ms).ok_or("publication readback clock overflow")?,
        cancel, ..control.clone() }))
}

pub(super) async fn execute(config: &WorkerConfig, state: &Path, command: &WorkerCommand,
    resource: &Option<Arc<resource::Envelope>>, control: &Control, recovery: bool) -> Result<(), String> {
    let (id, request, limits) = context(command)?;
    if request.caller().worker_id != config.id || !recovery && request.caller().incarnation != config.incarnation { return Err("publication command worker differs".into()); }
    preflight(command)?;
    let client = ApiClient::new(&config.controller_url, Arc::new(limits.clone()))?;
    let receipt_path = state.join("commands").join(format!("{}.json", component(id)));
    let mut receipt = read_receipt(&receipt_path, &config.limits)?;
    let prior = read(state, command)?;
    if prior.is_none() && receipt.effects_started { return Err("publication effect phase missing after possible entry".into()); }
    let reconcile = recovery || prior.is_some();
    let mut phase = prior.unwrap_or(Phase::new(command)?);
    let (_cleanup_owner, cleanup) = reconciliation_control(control)?;
    phase.save(state, command)?;
    // Original Prepare was committed atomically with command admission. Its
    // replay is deliberately before fresh task or incarnation checks.
    pull_request::prepare_effect(&client, &mut phase.effect, &cleanup).await?;
    phase.save(state, command)?;
    if phase.effect.prepare_refusal.is_some() { return Err("admitted publication lost its original prepare".into()); }
    let original = pull_request::lookup_effect(&client, &phase.effect, &cleanup).await?;
    if phase.retired {
        if phase.effect.settled.as_ref() != Some(&original) { return Err("retired publication lost its original settled proof".into()); }
        receipt.completed = true; atomic_json(&receipt_path, &receipt)?;
        return Ok(());
    }
    if original.state == "settled" {
        if phase.effect.outcome.as_ref().is_some_and(|old| Some(old) != original.outcome.as_ref()) { return Err("publication original settlement differs".into()); }
        phase.effect.outcome = original.outcome;
    } else if phase.effect.outcome.is_none() {
        if reconcile {
            if !phase.effect.may_have_started { refuse_effect(&mut phase, p::Refusal::NotDispatched)?; }
            else {
                let directory = state.join("publication-readback"); process::private_directory(&directory)?;
                phase.effect.outcome = pull_request::readback(&request.head, request.create.as_ref(), &directory, &registry(state,id), &cleanup).await.ok();
            }
        } else if let Err(reason) = current(&client, request, control).await {
            refuse_effect(&mut phase, reason)?;
        } else {
            resource::verify(resource)?;
            receipt.effects_started = true; atomic_json(&receipt_path, &receipt)?;
            let directory = state.join("publication-readback"); process::private_directory(&directory)?;
            let registry = registry(state,id);
            let provider = if request.create.is_some() {
                match pull_request_provider::Provider::configured(&request.head.target, Arc::new(limits.clone())) {
                    Ok(provider) => Some(provider),
                    Err(_) => { refuse_effect(&mut phase, p::Refusal::Configuration)?; None },
                }
            } else { None };
            let mut workspace = None;
            if phase.effect.outcome.is_none() {
                let preflight = if let Some(intent) = &request.create {
                    provider.as_ref().unwrap().preflight(intent, control).await
                } else {
                    match git::remote_commit(&directory, &pull_request::head_checkpoint(&request.head), &registry, control).await {
                        Ok(Some(commit)) if commit != request.head.binding.checkpoint.commit => { refuse_effect(&mut phase, p::Refusal::Conflict)?; Ok(()) },
                        Ok(_) => {
                            let binding = request.head.publication.as_ref().unwrap();
                            match git::checkout_cached(&config.workspace_root.join("repositories"), &config.workspace_root.join("validation"), id,
                                &binding.source.repository, Some(&request.head.binding.checkpoint), &registry, control).await {
                                Ok(path) => { workspace = Some(path); Ok(()) }, Err(error) => Err(error),
                            }
                        }, Err(error) => Err(error),
                    }
                };
                if let Err(error) = preflight {
                    eprintln!("Publication configuration refusal: {}", scrub(error, limits.execution.coding.summary_bytes, limits));
                    let reason = if control.check().is_err() {
                        if process::now_ms() >= control.deadline_ms { p::Refusal::Deadline } else { p::Refusal::Cancelled }
                    } else { p::Refusal::Configuration };
                    refuse_effect(&mut phase, reason)?;
                }
            }
            if phase.effect.outcome.is_none() {
                if let Err(reason) = current(&client, request, control).await { refuse_effect(&mut phase, reason)?; }
                else {
                    resource::verify(resource)?;
                    phase.effect.begin()?;
                    phase.save(state, command)?; // possible dispatch durable before network
                    phase.effect.outcome = if let Some(intent) = &request.create {
                        pull_request::dispatch_create(provider.as_ref().unwrap(), intent, control).await.ok()
                    } else { pull_request::dispatch_head(&request.head, workspace.as_ref().ok_or("publication source workspace absent")?, &registry, control).await.ok() };
                    if phase.effect.outcome.is_none() {
                        phase.effect.outcome = pull_request::readback(&request.head, request.create.as_ref(), &directory, &registry, &cleanup).await.ok();
                    }
                }
            }
            process::reconcile_processes(&registry, &limits.worker).await?;
            // The validation tree is disposable only after a closed original
            // outcome. Unknown effects retain it and its cache association.
            if phase.effect.outcome.is_some() {
                if let Some(_workspace) = workspace {
                    let binding = request.head.publication.as_ref().unwrap();
                    git::remove_cached_worktree(&config.workspace_root.join("repositories"), &config.workspace_root.join("validation"), id, &binding.source.repository, &registry, &cleanup).await?;
                }
            }
        }
    }
    phase.save(state, command)?;
    if phase.effect.outcome.is_none() {
        // This is an original nonfinal observation, not a successful publication
        // or permission to dispatch. It has its own stable settlement identity.
        let report = unknown_request(&phase)?;
        mutations::settle(&client, state, report, &cleanup).await?;
        phase.unknown_recorded = true; phase.save(state, command)?;
        return Err("publication outcome unknown; original ref or marker requires readback".into());
    }
    pull_request::settle_effect(&client, state, &mut phase.effect, &cleanup).await?;
    phase.retired = true; phase.save(state, command)?;
    receipt.completed = true; atomic_json(&receipt_path, &receipt)?;
    Ok(())
}

pub(super) async fn recover(config: &WorkerConfig, state: &Path, resource: &Option<Arc<resource::Envelope>>, runtime: &Arc<crate::native::ValidationRuntime>) -> Result<(), String> {
    for (_, receipt) in command_receipts(state, &config.limits)? {
        let WorkerCommand::PublishRepository { request, .. } = &receipt.command else { continue; };
        let limits = &request.input.resolved_limits.as_ref().ok_or("publication limits absent")?.global_metadata;
        let (_sender, cancel) = watch::channel(false);
        let control = Control { deadline_ms: process::now_ms().checked_add(limits.worker.publication_recovery_timeout_ms).ok_or("publication recovery deadline overflow")?,
            worker_limits: Arc::new(limits.worker.clone()), runtime_limits: Arc::new((**limits).clone()), validation_runtime: runtime.clone(), cancel };
        boxed_execution(|| execute(config, state, &receipt.command, resource, &control, true)).await?;
    }
    Ok(())
}

pub(super) fn validate_saved_settlement(state: &Path, run: &WorkerCommand, operation: &Operation) -> Result<bool, String> {
    let WorkerCommand::RunAttempt { attempt, task, .. } = run else { return Ok(false); };
    let Operation::SettleOperation { operation_id, .. } = operation else { return Ok(false); };
    let limits = &task.resolved_limits.as_ref().ok_or("publication original run policy absent")?.global_metadata;
    for (_, receipt) in command_receipts(state, limits)? {
        let WorkerCommand::PublishRepository { request, .. } = &receipt.command else { continue; };
        let expected = request.effect(limits)?;
        if &expected.id != operation_id { continue; }
        if request.attempt.id != attempt.id || request.input != *task { return Err("publication settlement original run differs".into()); }
        let phase = read(state, &receipt.command)?.ok_or("publication settlement phase absent")?;
        if serde_json::to_value(operation).map_err(|_| "publication settlement invalid")? == serde_json::to_value(unknown_request(&phase)?).map_err(|_| "publication settlement invalid")? {
            if !phase.effect.may_have_started { return Err("publication unknown precedes possible dispatch".into()); }
            return Ok(true);
        }
        if serde_json::to_value(phase.effect.settlement()?).map_err(|_| "publication settlement encoding failed")? != serde_json::to_value(operation).map_err(|_| "publication settlement encoding failed")? { return Err("publication original settlement changed".into()); }
        return Ok(true);
    }
    Ok(false)
}

pub(super) fn admit(state: &Path, limits: &RuntimeLimits, worker: &str) -> Result<(), String> {
    let current = handoff::admits_publication_requests(state, &limits.worker)?;
    let mut expected = std::collections::BTreeSet::new();
    for (command_path, receipt) in command_receipts(state, limits)? {
        match &receipt.command {
            WorkerCommand::PublishRepository { command_id, request } => {
                if !current || request.caller().worker_id != worker { return Err("publication command requires original worker21 binding".into()); }
                if command_path != state.join("commands").join(format!("{}.json", component(command_id))) { return Err("publication command path differs from original identity".into()); }
                request.validate(command_id)?;
                if read(state, &receipt.command)?.is_some() { expected.insert(path(state, command_id)); }
                else if receipt.effects_started { return Err("publication phase missing after effects started".into()); }
            },
            WorkerCommand::RunAttempt { task, .. } if !current && crate::protocol::input_requires_publication(task) => return Err("publication accepted profile requires worker21".into()),
            _ => {},
        }
    }
    let entries = match std::fs::read_dir(state.join("publication-requests")) {
        Ok(entries) => entries, Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err("publication phase inventory unavailable".into()),
    };
    for (n, entry) in entries.enumerate() {
        if n >= limits.worker.command_records || !expected.remove(&entry.map_err(|_| "publication phase inventory unreadable")?.path()) {
            return Err("publication phase has no original command or exceeds worker.command_records".into());
        }
    }
    if !expected.is_empty() { return Err("publication phase disappeared during admission".into()); }
    Ok(())
}
