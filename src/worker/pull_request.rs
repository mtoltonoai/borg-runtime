//! Original PR publication phases. Recovery observes original external effects;
//! it never reruns an executor, pushes a new head, or repeats PR creation.
use super::*;
use crate::pull_request::{self as contract, Bounds, HeadIntent, Intent};
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Effect {
    pub(super) intended: OperationRecord,
    pub(super) prepare_commit: Option<MutationCommit>,
    pub(super) prepare_refusal: Option<String>, // SHA256 of the exact original refusal, not arbitrary provider/controller text
    pub(super) may_have_started: bool,
    pub(super) outcome: Option<serde_json::Value>,
    pub(super) settlement_id: String,
    pub(super) settled: Option<OperationRecord>,
}
impl Effect {
    pub(super) fn begin(&mut self) -> Result<(), String> {
        if self.closed() || self.prepare_commit.is_none() || self.may_have_started || self.outcome.is_some() {
            return Err("original publication cannot be dispatched again".into());
        }
        self.may_have_started = true;
        Ok(())
    }
    pub(super) fn new(intended: OperationRecord) -> Result<Self, String> {
        let settlement_id = format!("pr-settle-{}", digest(&intended.id)?);
        Ok(Self { intended, prepare_commit: None, prepare_refusal:None, may_have_started: false,
            outcome: None, settlement_id, settled: None })
    }
    pub(super) fn request(&self) -> Operation {
        Operation::PrepareOperation { operation_id: self.intended.id.clone(), task_id: self.intended.task_id.clone(),
            attempt_id: self.intended.attempt_id.clone(), generation: self.intended.generation,
            kind: self.intended.kind.clone(), intended: self.intended.intended.clone() }
    }
    pub(super) fn settlement(&self) -> Result<Operation, String> {
        Ok(Operation::SettleOperation { settlement_id: self.settlement_id.clone(), operation_id: self.intended.id.clone(),
            task_id: self.intended.task_id.clone(), attempt_id: self.intended.attempt_id.clone(), generation: self.intended.generation,
            outcome: self.outcome.clone().ok_or("PR original outcome is unresolved")?, checkpoint: None })
    }
    pub(super) fn applied(&self) -> bool { self.settled.as_ref().is_some_and(|r| r.outcome.as_ref().is_some_and(|v| v.get("proof").is_some())) }
    pub(super) fn closed(&self) -> bool { self.settled.is_some() || self.prepare_refusal.is_some() }
}

/// Shared effect implementation for requested Coding completion and the owned
/// MCP command. Both callers persist Effect::begin before entering these paths.
pub(super) async fn dispatch_head(head: &HeadIntent, workspace: &Path, registry: &Path, control: &Control) -> Result<serde_json::Value, String> {
    git::publish(workspace, &head_checkpoint(head), registry, control).await?;
    head_outcome(head)
}
pub(super) async fn dispatch_create(provider: &pull_request_provider::Provider, intent: &Intent, control: &Control) -> Result<serde_json::Value, String> {
    let proof = match provider.create(intent, control).await {
        Ok(proof) => proof,
        Err(_) => match provider.reconcile(intent, control).await? {
            contract::Reconciliation::Observed(proof) => proof,
            contract::Reconciliation::Unknown => return Err("PR original create outcome remains unresolved".into()),
        },
    };
    serde_json::to_value(contract::Settlement { acknowledged: true, proof }).map_err(|_| "PR proof encoding failed".into())
}
pub(super) async fn readback(head: &HeadIntent, intent: Option<&Intent>, directory: &Path, registry: &Path, control: &Control) -> Result<serde_json::Value, String> {
    if let Some(intent) = intent {
        let provider = pull_request_provider::Provider::configured(&head.target, control.runtime_limits.clone())?;
        let contract::Reconciliation::Observed(proof) = provider.reconcile(intent, control).await? else {
            return Err("PR original create remains unresolved; no repeat is permitted".into());
        };
        serde_json::to_value(contract::Settlement { acknowledged: true, proof }).map_err(|_| "PR recovered proof encoding failed".into())
    } else {
        match git::remote_commit(directory, &head_checkpoint(head), registry, control).await? {
            Some(commit) if commit == head.binding.checkpoint.commit => head_outcome(head),
            Some(_) => Err("PR original head has different content; no overwrite is permitted".into()),
            None => Err("PR original head may have been sent; empty lookup remains unresolved".into()),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Phase {
    revision: u32,
    command_sha256: String,
    head: HeadIntent,
    head_effect: Effect,
    create: Option<Intent>,
    create_effect: Option<Effect>,
    /// Actual checks already completed on the exact acknowledged checkpoint.
    candidate: Observation,
    final_observation: Option<Observation>,
    final_report: Option<Report>,
    final_report_id: String,
    final_commit: Option<MutationCommit>,
    retired: bool,
}

fn digest(value: &impl Serialize) -> Result<String, String> {
    let bytes = serde_json::to_vec(value).map_err(|_| "PR binding encoding failed")?;
    Ok(aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &bytes).as_ref().iter().map(|b|format!("{b:02x}")).collect())
}
fn context(command: &WorkerCommand) -> Result<(&Attempt, &TaskInput, &RuntimeLimits), String> {
    validate_command_session(command)?;
    let WorkerCommand::RunAttempt { attempt, task, .. } = command else { return Err("PR phase has no original RunAttempt".into()); };
    task.execution_limits()?;
    let limits = task.resolved_limits.as_ref().ok_or("PR command limits missing")?.global_metadata.as_ref();
    Ok((attempt, task, limits))
}
pub(super) fn path(state: &Path, attempt: &Attempt) -> PathBuf {
    state.join("pull-requests").join(format!("{}.json", component(&attempt.id)))
}
pub(super) fn same_intent(a: &OperationRecord, b: &OperationRecord) -> bool {
    a.id == b.id && a.task_id == b.task_id && a.attempt_id == b.attempt_id && a.generation == b.generation
        && a.kind == b.kind && a.intended == b.intended
}
pub(super) fn validate_effect(effect: &Effect, expected: &OperationRecord, bounds: &Bounds) -> Result<(), String> {
    if &effect.intended != expected || effect.settlement_id != format!("pr-settle-{}", digest(&expected.id)?)
        || effect.prepare_commit.as_ref().is_some_and(|c| c.operation_id != expected.id || c.controller_revision == 0)
        || effect.may_have_started && (effect.prepare_commit.is_none() || effect.prepare_refusal.is_some())
        || effect.prepare_refusal.as_ref().is_some_and(|v| v.len()!=64 || !v.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()) || effect.prepare_commit.is_none() || effect.outcome.is_some())
        || effect.settled.is_some() && effect.outcome.is_none()
        || effect.outcome.is_some() && effect.prepare_commit.is_none() {
        return Err("PR durable effect identity differs".into());
    }
    if let Some(outcome) = &effect.outcome {
        let not_applied = if expected.kind == contract::HEAD_KIND {
            let intent: HeadIntent = serde_json::from_value(expected.intended.clone()).map_err(|_| "PR head intent invalid")?;
            let value: contract::HeadOutcome = serde_json::from_value(outcome.clone()).map_err(|_| "PR head outcome invalid")?;
            contract::validate_head_outcome(&intent, &value, bounds)?;
            matches!(value, contract::HeadOutcome::NotApplied(_))
        } else {
            let intent: Intent = serde_json::from_value(expected.intended.clone()).map_err(|_| "PR create intent invalid")?;
            let value: contract::CreateOutcome = serde_json::from_value(outcome.clone()).map_err(|_| "PR create outcome invalid")?;
            contract::validate_outcome(&intent, &value, bounds)?;
            matches!(value, contract::CreateOutcome::NotApplied(_))
        };
        if not_applied == effect.may_have_started {
            return Err("PR outcome contradicts original send barrier".into());
        }
    }
    if let Some(settled) = &effect.settled {
        if !same_intent(settled, expected) || settled.state != "settled" || settled.outcome != effect.outcome {
            return Err("PR original settled record differs".into());
        }
    }
    Ok(())
}
impl Phase {
    fn validate(&self, command: &WorkerCommand) -> Result<(), String> {
        let (attempt, input, limits) = context(command)?;
        let bounds = Bounds::from_limits(limits);
        let repository = input.repository.as_ref().ok_or("PR accepted repository absent")?;
        let target = repository.pull_request_target.as_ref().ok_or("PR accepted target absent")?;
        let request = input.pull_request.as_ref().ok_or("PR was not requested")?;
        let expected = contract::prepare_head(target, repository, attempt, &self.head.binding.checkpoint,
            &format!("pr-create-{}", digest(&(attempt.id.as_str(), attempt.generation))?), &bounds)?;
        if self.revision != 1 || self.command_sha256 != digest(command)? || self.head != expected
            || input.definition.executor != ExecutorKind::Coding
            || self.final_report_id != format!("pr-result-{}", digest(&self.head.binding.operation_id)?)
            || self.retired && (self.final_report.is_none() || self.final_commit.is_none())
            || self.final_commit.is_some() && self.final_report.is_none()
            || self.final_commit.as_ref().is_some_and(|c| c.operation_id != self.final_report_id || c.controller_revision == 0) {
            return Err("PR phase differs from original command".into());
        }
        match &self.candidate {
            Observation::ResultCandidate { checks_passed:true, checkpoint:Some(cp), validation, .. }
                if cp == &self.head.binding.checkpoint && validation.len() >= input.acceptance_commands.len() => {},
            _ => return Err("PR phase lacks completed exact-checkpoint validation".into()),
        }
        validate_effect(&self.head_effect, &contract::head_record(&self.head, &bounds)?, &bounds)?;
        match (&self.create, &self.create_effect) {
            (Some(intent), Some(effect)) => {
                let settled = self.head_effect.settled.as_ref().ok_or("PR create has no settled head")?;
                if &contract::prepare_pull_request(&self.head, settled, request, &bounds)? != intent {
                    return Err("PR create differs from original request or head proof".into());
                }
                validate_effect(effect, &contract::intent_record(intent, &bounds)?, &bounds)?;
            },
            (None, None) => {},
            _ => return Err("PR create phase is incomplete".into()),
        }
        if let Some(final_observation) = &self.final_observation {
            if !self.head_effect.closed() || self.create_effect.as_ref().is_some_and(|effect| !effect.closed()) {
                return Err("PR final decision precedes original effect closure".into());
            }
            if self.create_effect.as_ref().is_some_and(Effect::applied) {
                if final_observation != &self.candidate { return Err("PR successful candidate changed".into()); }
            } else if final_observation != &aborted_candidate() {
                return Err("PR unresolved phase has a successful candidate".into());
            }
        }
        if let Some(report)=&self.final_report {
            if self.final_observation.is_none() || report.report_id!=self.final_report_id
                || report.worker_id!=attempt.worker_id || report.incarnation!=attempt.worker_incarnation
                || report.task_id!=attempt.task_id || report.attempt_id!=attempt.id || report.generation!=attempt.generation {
                return Err("PR final report differs from original attempt".into());
            }
            // Normalization can bound/scrub presentation strings but cannot
            // change the proved checkpoint, result shape, or success decision.
            match (&report.observation,self.final_observation.as_ref().unwrap()) {
                (Observation::ResultCandidate{checks_passed:a,checkpoint:ac,validation:av,output:ao,..},
                 Observation::ResultCandidate{checks_passed:b,checkpoint:bc,validation:bv,output:bo,..})
                    if a==b && ac==bc && ao==bo && av.len()==bv.len() => {},
                _=>return Err("PR normalized report changed original result semantics".into()),
            }
            bounded_json_size(report,limits.transport.body_bytes).map_err(|_| "PR original final report exceeds transport.body_bytes")?;
        }
        let value = serde_json::to_value(self).map_err(|_| "PR phase encoding failed")?;
        if value_contains_credential(&value, &configured_credentials(limits)?, &limits.worker)
            .map_err(|_| "PR phase credential inspection bound")? {
            return Err("PR phase contains configured credential".into());
        }
        bounded_json_size(self, limits.worker.publication_record_bytes)
            .map_err(|_| "PR phase exceeds worker.publication_record_bytes")?;
        Ok(())
    }
    fn save(&self, state: &Path, command: &WorkerCommand) -> Result<(), String> {
        self.validate(command)?;
        let (attempt, _, _) = context(command)?;
        atomic_json(&path(state, attempt), self)
    }
}
fn aborted_candidate() -> Observation {
    Observation::ResultCandidate { checks_passed:false,
        summary:"Requested pull request publication did not complete; recovery does not send a new publication.".into(),
        checkpoint:None, validation:vec![], output:None }
}
pub(super) fn read(state: &Path, command: &WorkerCommand) -> Result<Option<Phase>, String> {
    let (attempt, _, limits) = context(command)?;
    match std::fs::symlink_metadata(state.join("pull-requests")) {
        Ok(m) if !m.is_dir() => return Err("PR phase root is not a directory".into()),
        Ok(_) => {},
        Err(e) if e.kind()==std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("PR phase root unavailable".into()),
    }
    let file = match std::fs::OpenOptions::new().read(true).custom_flags(0x20000 | 0x800).open(path(state, attempt)) {
        Ok(file) => file, Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("PR phase unavailable".into()),
    };
    if !file.metadata().map_err(|_| "PR phase metadata unreadable")?.is_file() {
        return Err("PR phase is not a regular file".into());
    }
    let bound = limits.worker.publication_record_bytes;
    let mut bytes = Vec::new();
    file.take(u64::try_from(bound).ok().and_then(|n|n.checked_add(1)).ok_or("PR phase reader bound overflow")?)
        .read_to_end(&mut bytes).map_err(|_| "PR phase unreadable")?;
    if bytes.len() > bound { return Err("PR phase exceeds worker.publication_record_bytes".into()); }
    let phase: Phase = serde_json::from_slice(&bytes).map_err(|_| "PR phase is corrupt or unsupported")?;
    if serde_json::to_value(&phase).map_err(|_| "PR phase encoding failed")?
        != serde_json::from_slice::<serde_json::Value>(&bytes).map_err(|_| "PR phase JSON invalid")? {
        return Err("PR phase contains unsupported nested fields".into());
    }
    phase.validate(command)?;
    Ok(Some(phase))
}

pub(super) async fn call(client: &ApiClient, operation: Operation, control: &Control) -> Result<Reply, String> {
    control.check()?;
    let mut stopped = control.clone();
    tokio::select! {
        value=tokio::time::timeout(Duration::from_millis(client.limits().worker.publication_call_timeout_ms), client.call(operation)) =>
            value.map_err(|_| "PR controller call timed out")?,
        _=stopped.stopped()=>Err("PR controller call interrupted".into()),
    }
}
pub(super) async fn prepare_effect(client: &ApiClient, effect: &mut Effect, control: &Control) -> Result<(), String> {
    // The Phase containing this exact request already exists durably. Replaying
    // the same controller mutation ID is not an external publication retry.
    let request = effect.request();
    bounded_json_size(&request, client.limits().transport.body_bytes)
        .map_err(|_| "PR prepare exceeds transport.body_bytes")?;
    let reply = call(client, request, control).await?;
    let commit = reply.commit.ok_or("PR prepare has no original commit")?;
    if commit.operation_id != effect.intended.id || commit.controller_revision == 0
        || effect.prepare_commit.as_ref().is_some_and(|old| old != &commit) {
        return Err("PR original prepare receipt differs or was refused".into());
    }
    match reply.outcome {
        Outcome::Ok{data:Data::Operation(ref record)} if record == &effect.intended && effect.prepare_refusal.is_none() => {},
        outcome @ Outcome::Refused{..} => {
            let fingerprint=digest(&outcome)?;
            if effect.prepare_refusal.as_ref().is_some_and(|old|old!=&fingerprint) {
                return Err("PR original refusal body differs".into());
            }
            effect.prepare_refusal=Some(fingerprint);
        },
        _ => return Err("PR original prepare outcome differs".into()),
    }
    effect.prepare_commit = Some(commit);
    Ok(())
}
pub(super) async fn settle_effect(client: &ApiClient, state: &Path, effect: &mut Effect, control: &Control) -> Result<(), String> {
    let request = effect.settlement()?;
    let data = mutations::settle(client, state, request, control).await?;
    let Data::Operation(record) = data else { return Err("PR settlement returned wrong kind".into()); };
    if !same_intent(&record, &effect.intended) || record.state != "settled" || record.outcome != effect.outcome
        || effect.settled.as_ref().is_some_and(|old| old != &record) {
        return Err("PR original settlement differs".into());
    }
    effect.settled = Some(record);
    Ok(())
}
pub(super) fn head_checkpoint(head: &HeadIntent) -> Checkpoint {
    Checkpoint { registration:head.binding.registration.clone(), repository:head.target.head_remote.clone(),
        commit:head.binding.checkpoint.commit.clone(), remote_ref:head.head_ref.clone(), operation_id:head.head_operation_id.clone() }
}
pub(super) fn head_outcome(head: &HeadIntent) -> Result<serde_json::Value, String> {
    serde_json::to_value(contract::HeadSettlement { acknowledged:true,
        proof:contract::HeadProof { operation_id:head.head_operation_id.clone(), target_revision:head.target.revision.clone(),
            remote:head.target.head_remote.clone(), remote_ref:head.head_ref.clone(), commit:head.binding.checkpoint.commit.clone() } })
        .map_err(|_| "PR head proof encoding failed".into())
}
pub(super) fn receipt_preflight(limits:&RuntimeLimits, request:&Operation, outcome:Outcome) -> Result<(),String> {
    let id=request.mutation_id().ok_or("PR mutation identity missing")?;
    let lookup=Outcome::Ok{data:Data::Operation(OperationRecord{id:id.into(),task_id:String::new(),attempt_id:String::new(),generation:0,
        kind:"application_mutation".into(),intended:serde_json::to_value(request).map_err(|_| "PR request encoding failed")?,state:"settled".into(),
        outcome:Some(serde_json::to_value(&outcome).map_err(|_| "PR outcome encoding failed")?)})};
    // Same original receipt/GetOperation envelopes and escaped request-ID pad
    // enforced independently by controller::metadata::receipt_bound.
    for outcome in [outcome,lookup] {
        bounded_json_size(&outcome,limits.controller.outcome_bytes).map_err(|_| "PR original receipt/lookup exceeds controller.outcome_bytes")?;
        let frame=Reply{request_id:String::new(),commit:Some(MutationCommit{operation_id:id.into(),controller_revision:i64::MAX as u64}),observed_controller_revision:Some(i64::MAX as u64),outcome};
        let size=bounded_json_size(&frame,limits.transport.body_bytes)?;
        if limits.transport.identity_bytes.checked_mul(2).and_then(|padding|size.checked_add(padding)).is_none_or(|total|total>limits.transport.body_bytes) {
            return Err("PR complete original receipt exceeds transport.body_bytes".into());
        }
    }
    Ok(())
}

fn preflight_phase(phase: &Phase, command: &WorkerCommand) -> Result<(), String> {
    let (attempt, input, limits) = context(command)?;
    let bounds=Bounds::from_limits(limits);
    // Size a typed future envelope only. These values are never written or
    // treated as observations. All strings except the provider's base SHA and
    // decimal IDs are already fixed by the accepted request and target. A
    // 64-byte base SHA and u64::MAX IDs dominate their allowed encodings.
    // The HTTP read ceiling is independent and is not a retained phase field.
    let mut complete=phase.clone();
    let stamp=|id:&str| MutationCommit{operation_id:id.into(),controller_revision:i64::MAX as u64};
    complete.head_effect.prepare_commit=Some(stamp(&complete.head_effect.intended.id));
    complete.head_effect.may_have_started=true;
    complete.head_effect.outcome=Some(head_outcome(&complete.head)?);
    let mut settled=complete.head_effect.intended.clone();
    settled.state="settled".into();settled.outcome=complete.head_effect.outcome.clone();
    complete.head_effect.settled=Some(settled.clone());
    let intent=contract::prepare_pull_request(&complete.head,&settled,input.pull_request.as_ref().ok_or("PR request absent")?,&bounds)?;
    let mut effect=Effect::new(contract::intent_record(&intent,&bounds)?)?;
    effect.prepare_commit=Some(stamp(&effect.intended.id));effect.may_have_started=true;
    let target=&complete.head.target;
    let mut url=reqwest::Url::parse(&target.api_base).map_err(|_| "PR API origin invalid")?;
    if url.host_str()==Some("api.github.com") { url.set_host(Some("github.com")).map_err(|_| "PR URL bound invalid")?; }
    url.set_path(&format!("/{}/{}/pull/{}",target.owner,target.repository,u64::MAX));
    let branch=|name:String,commit:String| contract::BranchProof{repository_id:target.repository_id,
        owner:target.owner.clone(),repository:target.repository.clone(),branch:name,commit};
    let proof=contract::Proof{operation_id:complete.head.binding.operation_id.clone(),provider_id:u64::MAX,number:u64::MAX,
        url:url.to_string(),head:branch(complete.head.head_ref.strip_prefix("refs/heads/").ok_or("PR head branch invalid")?.into(),complete.head.binding.checkpoint.commit.clone()),
        base:branch(target.base_branch.clone(),"f".repeat(64)),title:intent.title.clone(),body:intent.body.clone(),marker:intent.marker.clone()};
    let reference=contract::validate_proof(&intent,&proof,&bounds)?;
    effect.outcome=Some(serde_json::to_value(contract::Settlement{acknowledged:true,proof}).map_err(|_| "PR bound encoding failed")?);
    let mut record=effect.intended.clone();record.state="settled".into();record.outcome=effect.outcome.clone();
    // The complete typed OperationRecord, not just its proof, is checked.
    contract::reference_from_record(&record,&bounds)?;
    effect.settled=Some(record);
    complete.create=Some(intent);complete.create_effect=Some(effect);
    complete.final_observation=Some(complete.candidate.clone());
    complete.final_commit=Some(stamp(&complete.final_report_id));complete.retired=true;
    complete.final_report=Some(Report{report_id:complete.final_report_id.clone(),worker_id:attempt.worker_id.clone(),incarnation:attempt.worker_incarnation.clone(),
        task_id:attempt.task_id.clone(),attempt_id:attempt.id.clone(),generation:attempt.generation,observation:complete.candidate.clone()});
    bounded_json_size(&complete,limits.worker.publication_record_bytes)
        .map_err(|_| "PR complete future phase exceeds worker.publication_record_bytes")?;
    let Observation::ResultCandidate{summary,checkpoint,validation,output,..}=&complete.candidate else { return Err("PR final candidate missing".into()); };
    let result=AcceptedResult{pull_request:Some(reference),attempt_id:attempt.id.clone(),summary:summary.clone(),checkpoint:checkpoint.clone(),validation:validation.clone(),output:output.clone()};
    bounded_json_size(&result,input.limits.result_bytes).map_err(|_| "PR complete future result exceeds accepted result_bytes")?;
    // Original SettleOperation journals have their own accepted-policy envelope.
    // Preflight both before any Git/HTTP effect, rather than discovering their
    // existing independent bound after publication.
    for effect in std::iter::once(&complete.head_effect).chain(complete.create_effect.as_ref()) {
        receipt_preflight(limits,&effect.request(),Outcome::Ok{data:Data::Operation(effect.intended.clone())})?;
        let request=effect.settlement()?;
        receipt_preflight(limits,&request,Outcome::Ok{data:Data::Operation(effect.settled.clone().unwrap())})?;
        let mut journal=mutations::prepare(request,0,limits)?;
        journal.outcome=Some(Outcome::Ok{data:Data::Operation(effect.settled.clone().unwrap())});
        journal.outcome_context=Some(CommitContext{commit:stamp(&effect.settlement_id),observed_controller_revision:i64::MAX as u64});
        bounded_json_size(&journal,limits.worker.mutation_record_bytes).map_err(|_| "PR complete settlement exceeds worker.mutation_record_bytes")?;
    }
    // Refusal text stays in the canonical original receipt. The phase retains
    // only its exact digest. Verify both definite negative alternatives too.
    for create in [false,true] {
        let mut negative=complete.clone();
        let effect=if create {negative.create_effect.as_mut().unwrap()} else {&mut negative.head_effect};
        effect.prepare_refusal=Some("f".repeat(64));effect.may_have_started=false;effect.outcome=None;effect.settled=None;
        if !create {negative.create=None;negative.create_effect=None;}
        negative.final_observation=Some(aborted_candidate());
        bounded_json_size(&negative,limits.worker.publication_record_bytes).map_err(|_| "PR refused future phase exceeds worker.publication_record_bytes")?;
    }
    Ok(())
}

/// Called only after executor completion, acknowledged checkpoint and checks.
/// Returns after the fixed final candidate is acknowledged, never a PR promise.
pub(super) async fn publish(state: &Path, command: &WorkerCommand, reporter: &Reporter,
    workspace: &Path, registry: &Path, candidate: Observation, control: &Control) -> Result<(), String> {
    let (attempt, input, limits) = context(command)?;
    let repository = input.repository.as_ref().ok_or("PR accepted repository absent")?;
    let target = repository.pull_request_target.as_ref().ok_or("PR accepted target absent")?;
    let request = input.pull_request.as_ref().ok_or("PR request absent")?;
    let Observation::ResultCandidate { checks_passed:true, checkpoint:Some(checkpoint), .. } = &candidate else {
        return Err("PR publication requires successful exact-checkpoint validation".into());
    };
    if read(state, command)?.is_some() { return Err("existing PR phase requires reconciliation".into()); }
    let bounds = Bounds::from_limits(limits);
    let head = contract::prepare_head(target, repository, attempt, checkpoint,
        &format!("pr-create-{}", digest(&(attempt.id.as_str(), attempt.generation))?), &bounds)?;
    let mut phase = Phase { revision:1, command_sha256:digest(command)?, head_effect:Effect::new(contract::head_record(&head,&bounds)?)?,
        final_report_id:format!("pr-result-{}", digest(&head.binding.operation_id)?), head,
        create:None, create_effect:None, candidate, final_observation:None, final_report:None, final_commit:None, retired:false };
    preflight_phase(&phase, command)?;
    // Validate credential availability before any external write, without
    // retaining it in this phase or interpreting task-supplied environment.
    let provider = pull_request_provider::Provider::configured(target, Arc::new(limits.clone()))?;
    phase.save(state, command)?; // includes original identity BEFORE Prepare ACK
    prepare_effect(&reporter.client, &mut phase.head_effect, control).await?;
    phase.save(state, command)?;
    if phase.head_effect.prepare_refusal.is_some() {
        phase.final_observation=Some(aborted_candidate()); phase.save(state,command)?;
        return boxed_execution(|| finish(state,command,reporter,&mut phase)).await;
    }
    control.check()?; resource::verify(&reporter.resource)?;
    phase.head_effect.begin()?;
    phase.save(state, command)?;
    phase.head_effect.outcome = Some(dispatch_head(&phase.head, workspace, registry, control).await?);
    phase.save(state, command)?;
    settle_effect(&reporter.client, state, &mut phase.head_effect, control).await?;
    phase.save(state, command)?;
    let intent = contract::prepare_pull_request(&phase.head, phase.head_effect.settled.as_ref().unwrap(), request, &bounds)?;
    phase.create_effect = Some(Effect::new(contract::intent_record(&intent,&bounds)?)?);
    phase.create = Some(intent.clone());
    phase.save(state, command)?;
    prepare_effect(&reporter.client, phase.create_effect.as_mut().unwrap(), control).await?;
    phase.save(state, command)?;
    if phase.create_effect.as_ref().unwrap().prepare_refusal.is_some() {
        phase.final_observation=Some(aborted_candidate()); phase.save(state,command)?;
        return boxed_execution(|| finish(state,command,reporter,&mut phase)).await;
    }
    provider.preflight(&intent, control).await?;
    control.check()?; resource::verify(&reporter.resource)?;
    phase.create_effect.as_mut().unwrap().begin()?;
    phase.save(state, command)?;
    phase.create_effect.as_mut().unwrap().outcome = Some(dispatch_create(&provider, &intent, control).await?);
    phase.save(state, command)?;
    settle_effect(&reporter.client, state, phase.create_effect.as_mut().unwrap(), control).await?;
    phase.final_observation = Some(phase.candidate.clone());
    phase.save(state, command)?;
    boxed_execution(|| finish(state, command, reporter, &mut phase)).await
}

fn final_success_matches(phase: &Phase, command: &WorkerCommand, outcome: &Outcome) -> Result<bool, String> {
    let Outcome::Ok{data:Data::Task(task)}=outcome else {return Ok(false);};
    if !publication::original_task(task,command) || !task.attempt.as_ref().is_some_and(|a|a.stopped) { return Ok(false); }
    if let Some(effect)=phase.create_effect.as_ref().filter(|e|e.applied()) {
        let (_,_,limits)=context(command)?;
        let reference=contract::reference_from_record(effect.settled.as_ref().ok_or("PR settled result absent")?,&Bounds::from_limits(limits))?;
        Ok(task.status==TaskStatus::Succeeded && task.result.as_ref().and_then(|r|r.pull_request.as_ref())==Some(&reference)
            && task.selected_checkpoint.as_ref()==Some(&phase.head.binding.checkpoint))
    } else { Ok(task.status==TaskStatus::Failed && task.result.is_none()) }
}

async fn finish(state: &Path, command: &WorkerCommand, reporter: &Reporter, phase: &mut Phase) -> Result<(), String> {
    let credentials=configured_credentials(&reporter.config.limits)?;
    if phase.final_report.is_none() {
        let observation=phase.final_observation.clone().ok_or("PR final decision missing")?;
        let report=reporter.normalize_identified_with_credentials(phase.final_report_id.clone(),observation,&credentials)?;
        phase.final_report=Some(report);
        // This exact normalized original is durable BEFORE it can reach the
        // outbox or controller. A crash cannot force receipt reconstruction.
        phase.save(state,command)?;
    }
    let stop_id=format!("pr-stop-{}", digest(&phase.head.binding.operation_id)?);
    if !boxed_execution(|| reporter.send_identified(stop_id, Observation::ProcessTreeStopped)).await? {
        return Err("PR original stop awaits acknowledgment".into());
    }
    let report=phase.final_report.clone().ok_or("PR original final report missing")?;
    let accepted={
        let _lock=reporter.outbox_lock.lock().await;
        reporter.retain_identified_report(report.clone())?;
        boxed_execution(|| flush_outbox_with_credentials(&reporter.client,&reporter.outbox,&credentials)).await?;
        let path=reporter.outbox.join(format!("{}.json",component(&report.report_id)));
        !path.exists() && !path.with_extension("refused").exists()
    };
    let request = final_request(phase, &reporter.attempt)?;
    let acceptance::Lookup::Recorded(outcome, context) = boxed_execution(|| acceptance::lookup(&reporter.client, &phase.final_report_id, &request)).await? else {
        return Err("PR original final result receipt is unavailable".into());
    };
    if !accepted {
        if !matches!(outcome,Outcome::Refused{..}) { return Err("PR final refusal receipt differs".into()); }
        let reply=boxed_execution(|| reporter.client.call(Operation::GetTask{task_id:reporter.attempt.task_id.clone()})).await?;
        if !matches!(reply.outcome,Outcome::Ok{data:Data::Task(ref task)} if publication::original_task(task,command)
            && matches!(task.status,TaskStatus::Succeeded|TaskStatus::Failed|TaskStatus::Cancelled)
            && task.attempt.as_ref().is_some_and(|a|a.stopped)) {
            return Err("PR final result refused without exact terminal original task".into());
        }
    } else if !final_success_matches(phase,command,&outcome)? {
        return Err("PR final acknowledgment receipt differs".into());
    }
    phase.final_commit=Some(context.commit);
    phase.retired = true;
    phase.save(state, command)
}

fn final_request(phase: &Phase, attempt: &Attempt) -> Result<Operation, String> {
    let report=phase.final_report.clone().ok_or("PR original normalized final report missing")?;
    if report.report_id!=phase.final_report_id || report.worker_id!=attempt.worker_id || report.incarnation!=attempt.worker_incarnation
        || report.task_id!=attempt.task_id || report.attempt_id!=attempt.id || report.generation!=attempt.generation {
        return Err("PR original normalized report identity differs".into());
    }
    Ok(Operation::ObserveWorker{report})
}

/// No network, mutation, or marker transition; every new phase is checked under
/// its original retained command, including already completed command records.
pub(super) fn admit(state: &Path, limits: &RuntimeLimits, worker_id: &str) -> Result<(), String> {
    let mut expected = std::collections::BTreeSet::new();
    let mut new_records = false;
    for (_, receipt) in command_receipts(state, limits)? {
        if let WorkerCommand::RunAttempt { attempt, task, .. } = &receipt.command {
            new_records |= task.pull_request.is_some()
                || task.repository.as_ref().is_some_and(|r| r.pull_request_target.is_some())
                || task.native_workflow.as_ref().is_some_and(|w| match w.as_ref() {
                    NativeWorkflow::CodingThenContinue { child } | NativeWorkflow::Reassign { target: child } => child.pull_request.is_some() || child.repository.pull_request_target.is_some(),
                    NativeWorkflow::InspectCurrentTaskThenContinue { .. } => false,
                });
            if attempt.worker_id != worker_id { return Err("PR command belongs to another worker".into()); }
            if read(state, &receipt.command)?.is_some() { expected.insert(path(state, attempt)); }
        }
    }
    if (new_records || !expected.is_empty()) && !handoff::admits_pull_request_records(state, &limits.worker)? {
        return Err("PR records require the original worker15 marker".into());
    }
    let directory = state.join("pull-requests");
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries, Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err("PR phase inventory unavailable".into()),
    };
    let mut count = 0usize;
    for entry in entries {
        count += 1;
        let path = entry.map_err(|_| "PR phase inventory unreadable")?.path();
        if count > limits.worker.command_records || !expected.remove(&path) {
            return Err("PR phase has no exact retained command or exceeds inventory bound".into());
        }
    }
    if !expected.is_empty() { return Err("PR phase disappeared during inventory".into()); }
    Ok(())
}

pub(super) fn validate_saved_settlement(state: &Path, command: &WorkerCommand, request: &Operation) -> Result<(), String> {
    let Operation::SettleOperation { operation_id, .. } = request else { return Err("PR settlement operation differs".into()); };
    let Some(phase) = read(state, command)? else {
        if super::publication_requests::validate_saved_settlement(state, command, request)? { return Ok(()); }
        if operation_id.starts_with("pr-") { return Err("PR settlement has no original phase".into()); }
        return Ok(());
    };
    for effect in std::iter::once(&phase.head_effect).chain(phase.create_effect.as_ref()) {
        if &effect.intended.id == operation_id {
            if serde_json::to_value(effect.settlement()?).map_err(|_| "PR settlement encoding failed")?
                != serde_json::to_value(request).map_err(|_| "PR settlement encoding failed")? {
                return Err("PR saved settlement changed".into());
            }
            return Ok(());
        }
    }
    // A Coding attempt may retain its legacy result phase and independently
    // own an MCP publication. Only the exact retained MCP command/phase can
    // account for a settlement which did not match either legacy effect.
    if super::publication_requests::validate_saved_settlement(state, command, request)? { return Ok(()); }
    if operation_id.starts_with("pr-") { return Err("PR settlement identity differs from original phase".into()); }
    Ok(())
}

pub(super) fn cleanup_ready(state: &Path, command: &WorkerCommand) -> Result<bool, String> {
    Ok(read(state, command)?.is_none_or(|phase| phase.retired))
}

pub(super) async fn cleanup_verified(client: &ApiClient, state: &Path, command: &WorkerCommand) -> Result<bool, String> {
    let Some(phase) = read(state, command)? else { return Ok(true); };
    if !phase.retired { return Ok(false); }
    let (attempt, _, _) = context(command)?;
    let request=final_request(&phase,attempt)?;
    let acceptance::Lookup::Recorded(_, context)=acceptance::lookup(client,&phase.final_report_id,&request).await? else {
        return Ok(false);
    };
    if Some(&context.commit)!=phase.final_commit.as_ref() { return Err("PR cleanup original final receipt differs".into()); }
    for effect in std::iter::once(&phase.head_effect).chain(phase.create_effect.as_ref()) {
        if let Some(refused)=&effect.prepare_refusal {
            let acceptance::Lookup::Recorded(outcome, context)=acceptance::lookup(client,&effect.intended.id,&effect.request()).await? else {
                return Ok(false);
            };
            if Some(&context.commit)!=effect.prepare_commit.as_ref() || !matches!(outcome,Outcome::Refused{..}) || digest(&outcome)?!=*refused { return Err("PR cleanup original refusal differs".into()); }
        } else {
            let reply=client.call(Operation::GetOperation{operation_id:effect.intended.id.clone()}).await?;
            if !matches!(reply.outcome,Outcome::Ok{data:Data::Operation(ref record)} if effect.settled.as_ref()==Some(record)) {
                return Err("PR cleanup original settled proof differs".into());
            }
        }
    }
    Ok(true)
}

pub(super) async fn lookup_effect(client: &ApiClient, effect: &Effect, control: &Control) -> Result<OperationRecord, String> {
    let reply = call(client, Operation::GetOperation { operation_id:effect.intended.id.clone() }, control).await?;
    let Outcome::Ok { data:Data::Operation(record) } = reply.outcome else { return Err("PR original effect is unavailable".into()); };
    if !same_intent(&record, &effect.intended) || !matches!(record.state.as_str(), "pending"|"uncertain"|"settled") {
        return Err("PR original effect identity changed".into());
    }
    Ok(record)
}

async fn recover_effect(client: &ApiClient, state: &Path, command: &WorkerCommand,
    phase: &mut Phase, create: bool, control: &Control) -> Result<(), String> {
    let mut effect = if create { phase.create_effect.clone().ok_or("PR create effect missing")? } else { phase.head_effect.clone() };
    // Replaying only the exact controller Prepare establishes the original
    // receipt even when its ACK was lost. This does not perform Git/HTTP writes.
    prepare_effect(client, &mut effect, control).await?;
    if create { phase.create_effect = Some(effect.clone()); } else { phase.head_effect = effect.clone(); }
    phase.save(state, command)?;
    if effect.prepare_refusal.is_some() { return Ok(()); }
    let original = lookup_effect(client, &effect, control).await?;
    if original.state == "settled" {
        // A complete canonical outcome may have outlived its local ACK. Its
        // exact body is validated against the same immutable intent below.
        if effect.outcome.as_ref().is_some_and(|old| Some(old) != original.outcome.as_ref()) {
            return Err("PR canonical settled outcome differs from retained proof".into());
        }
        effect.outcome = original.outcome.clone();
    } else if effect.outcome.is_none() {
        if !effect.may_have_started {
            effect.outcome = Some(json!({"acknowledged":true,"not_applied":true}));
        } else {
            let directory = state.join("publication-readback");
            process::private_directory(&directory)?;
            let (attempt, _, _) = context(command)?;
            let registry = state.join("processes").join(component(&attempt.id));
            let intent = if create { Some(phase.create.as_ref().ok_or("PR create intent missing")?) } else { None };
            effect.outcome = Some(readback(&phase.head, intent, &directory, &registry, control).await?);
        }
    }
    if create { phase.create_effect = Some(effect.clone()); } else { phase.head_effect = effect.clone(); }
    phase.save(state, command)?; // exact original proof BEFORE settlement
    settle_effect(client, state, &mut effect, control).await?;
    if create { phase.create_effect = Some(effect); } else { phase.head_effect = effect; }
    phase.save(state, command)
}

pub(super) async fn recover(config: &WorkerConfig, state: &Path,
    resource: &Option<Arc<resource::Envelope>>, outbox_lock: &Arc<tokio::sync::Mutex<()>>,
    validation_runtime: &Arc<crate::native::ValidationRuntime>) -> Result<(), String> {
    for (command_path, mut receipt) in command_receipts(state, &config.limits)? {
        let WorkerCommand::RunAttempt { attempt, task:input, .. } = &receipt.command else { continue; };
        let Some(mut phase) = read(state, &receipt.command)? else { continue; };
        if phase.retired { continue; }
        if receipt.completed {
            receipt.completed=false;
            atomic_json(&command_path,&receipt)?;
        }
        if attempt.worker_id != config.id { return Err("PR phase belongs to another worker".into()); }
        let (_, _, limits) = context(&receipt.command)?;
        let client = ApiClient::new(&config.controller_url, Arc::new(limits.clone()))?;
        let (_sender, cancel) = watch::channel(false);
        let control = Control {
            // Readback cleanup budget only, never new task/model allowance.
            deadline_ms:process::now_ms().checked_add(limits.worker.publication_recovery_timeout_ms)
                .ok_or("PR recovery clock overflow")?,
            worker_limits:Arc::from(limits.worker.clone()), runtime_limits:Arc::new(limits.clone()),
            validation_runtime:validation_runtime.clone(), cancel,
        };
        let final_request = phase.final_report.as_ref().map(|_|final_request(&phase,attempt)).transpose()?;
        if let Some(request) = &final_request {
            if let acceptance::Lookup::Recorded(outcome @ Outcome::Ok {data:Data::Task(_)}, context) =
                acceptance::lookup(&client, &phase.final_report_id, request).await? {
                if !final_success_matches(&phase,&receipt.command,&outcome)? {
                    return Err("PR original final receipt differs from exact settled result".into());
                }
                // Original terminal receipt closes local bookkeeping, even if
                // the canonical task now belongs to a successor generation.
                phase.retired = true;
                phase.final_commit=Some(context.commit);
                phase.save(state, &receipt.command)?;
                receipt.completed = true;
                atomic_json(&command_path, &receipt)?;
                continue;
            }
        }
        let reply = call(&client, Operation::GetTask {task_id:attempt.task_id.clone()}, &control).await?;
        let Outcome::Ok { data:Data::Task(current) } = reply.outcome else { return Err("PR original task unavailable".into()); };
        if !publication::original_task(&current, &receipt.command) {
            return Err("PR phase belongs to a superseded or changed task; original evidence retained".into());
        }
        resource::verify(resource)?;
        recover_effect(&client, state, &receipt.command, &mut phase, false, &control).await?;
        if phase.create_effect.is_some() {
            recover_effect(&client, state, &receipt.command, &mut phase, true, &control).await?;
        }
        if phase.final_observation.is_none() {
            phase.final_observation = Some(if phase.create_effect.as_ref().is_some_and(Effect::applied) {
                phase.candidate.clone()
            } else { aborted_candidate() });
            phase.save(state, &receipt.command)?;
        }
        let mut original_config = config.clone();
        original_config.incarnation = attempt.worker_incarnation.clone();
        original_config.limits = Arc::new(limits.clone());
        let reporter = Reporter { resource:resource.clone(), client, config:original_config, attempt:attempt.clone(),
            outbox:state.join("outbox"), sequence:Arc::new(AtomicU64::new(1)), executor_seen:Arc::new(AtomicBool::new(true)),
            provider_uncertain:Arc::new(AtomicBool::new(false)), uncertainty_retained:Arc::new(AtomicBool::new(false)),
            output_limit:input.limits.output_bytes, result_limit:input.limits.result_bytes,
            coding_limits:input.execution_limits()?.coding.clone(), outbox_lock:outbox_lock.clone() };
        boxed_execution(|| finish(state, &receipt.command, &reporter, &mut phase)).await?;
        receipt.completed = true;
        bounded_json_size(&receipt, config.limits.worker.command_record_bytes)
            .map_err(|_| "PR recovered command exceeds worker.command_record_bytes")?;
        atomic_json(&command_path, &receipt)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "pull_request_tests.rs"]
mod tests;
