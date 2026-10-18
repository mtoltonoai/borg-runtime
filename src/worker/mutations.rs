//! Small durable slots for worker-originated mutations, not execution history.
use super::*;
use std::io::Read;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Journal {
    pub limits: crate::limits::RuntimeLimits,
    revision: u32,
    pub request: Operation,
    pub outcome: Option<Outcome>,
    pub outcome_context: Option<CommitContext>,
    pub consumed: bool,
    pub recovery_epoch: u64,
}

pub(super) fn fresh_id(kind: &str) -> Result<String, String> {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| "mutation identity clock unavailable")?
        .as_nanos();
    Ok(format!(
        "worker-{kind}-{stamp}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

fn identity(operation: &Operation) -> Result<&str, String> {
    match operation {
        Operation::CallCollaboration { call } => Ok(&call.operation_id),
        Operation::EvaluateCompletion(input) => Ok(&input.operation_id),
        Operation::BeginCompletionContinuation(input) => Ok(&input.operation_id),
        Operation::AcknowledgeCompletionInput(input) => Ok(&input.operation_id),
        Operation::AcquireNativeRequest { operation_id, .. }
        | Operation::SettleNativeRequest { operation_id, .. }
        | Operation::ObserveCharter { operation_id, .. }
        | Operation::PrepareAgentMemory { operation_id, .. }
        | Operation::AcknowledgeAgentMemory { operation_id, .. }
        | Operation::PrepareCollaborationDelivery { operation_id, .. }
        | Operation::BeginCollaborationDelivery { operation_id, .. }
        | Operation::AcknowledgeCollaborationDelivery { operation_id, .. }
        | Operation::RegisterWorker { operation_id, .. }
        | Operation::AwaitCommands { operation_id, .. } => Ok(operation_id),
        Operation::SettleOperation { settlement_id, .. } => Ok(settlement_id),
        _ => Err("unsupported worker mutation journal operation".into()),
    }
}

pub(super) fn read(
    path: &Path,
    limits: &crate::limits::RuntimeLimits,
) -> Result<Option<Journal>, String> {
    let Some(bytes) = read_bytes(path, limits.worker.mutation_record_bytes)? else {
        return Ok(None);
    };
    decode(&bytes, limits).map(Some)
}

fn read_bytes(path: &Path, bound: usize) -> Result<Option<Vec<u8>>, String> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("worker mutation journal unavailable".into()),
    };
    let mut bytes = Vec::new();
    let read_bound = u64::try_from(bound)
        .ok()
        .and_then(|bound| bound.checked_add(1))
        .ok_or("worker mutation journal bound exceeds reader representation")?;
    file.take(read_bound)
        .read_to_end(&mut bytes)
        .map_err(|_| "worker mutation journal unreadable")?;
    if bytes.len() > bound {
        return Err("worker mutation journal exceeds bound (worker.mutation_record_bytes)".into());
    }
    Ok(Some(bytes))
}

fn decode(bytes: &[u8], limits: &crate::limits::RuntimeLimits) -> Result<Journal, String> {
    if bytes.len() > limits.worker.mutation_record_bytes {
        return Err("worker mutation journal exceeds bound (worker.mutation_record_bytes)".into());
    }
    let journal: Journal =
        serde_json::from_slice(&bytes).map_err(|_| "corrupt worker mutation journal")?;
    let original: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| "corrupt worker mutation journal")?;
    if journal.limits != *limits
        || journal.revision != 3
        || identity(&journal.request)?.is_empty()
        || original.get("request")
            != Some(
                &serde_json::to_value(&journal.request).map_err(|_| "invalid mutation encoding")?,
            )
        || bounded_json_size(&journal.request, limits.transport.body_bytes).is_err()
        || journal.outcome.as_ref().is_some_and(|v| {
            !matches!(v, Outcome::Ok { .. } | Outcome::Refused { .. })
                || bounded_json_size(v, limits.transport.body_bytes).is_err()
        })
        || journal.consumed && journal.outcome.is_none()
    {
        return Err("unsupported worker mutation journal".into());
    }
    if journal.outcome.is_some() != journal.outcome_context.is_some() {
        return Err("mutation outcome lacks committed context".into());
    }
    if let (Some(outcome), Some(context)) = (&journal.outcome, &journal.outcome_context) {
        context.validate(&journal.request, outcome)?;
    }
    Ok(journal)
}

pub(super) fn create(path:&Path, request:Operation, recovery_epoch:u64, limits:&crate::limits::RuntimeLimits)->Result<Journal,String>{
    let journal=prepare(request,recovery_epoch,limits)?;
    atomic_json(path,&journal)?;
    Ok(journal)
}

pub(super) fn prepare(
    request: Operation,
    recovery_epoch: u64,
    limits: &crate::limits::RuntimeLimits,
) -> Result<Journal, String> {
    identity(&request)?;
    bounded_json_size(&request, limits.transport.body_bytes)
        .map_err(|_| "worker mutation request exceeds bound (transport.body_bytes)")?;
    let journal = Journal {
        limits: limits.clone(),
        revision: 3,
        request,
        outcome: None,
        outcome_context: None,
        consumed: false,
        recovery_epoch,
    };
    // Reserve the largest transport-bounded outcome plus its original receipt
    // context before any POST. The null placeholders remain in this conservative
    // size calculation; there is no hidden allowance or post-effect limit raise.
    let context = CommitContext {
        commit: MutationCommit {
            operation_id: identity(&journal.request)?.to_owned(),
            controller_revision: u64::MAX,
        },
        observed_controller_revision: u64::MAX,
    };
    let context_bytes = bounded_json_size(&context, limits.worker.mutation_record_bytes)
        .map_err(|reason| format!("{reason} (worker.mutation_record_bytes)"))?;
    let reserve = limits
        .transport
        .body_bytes
        .checked_add(context_bytes)
        .ok_or("mutation outcome reservation overflow (transport.body_bytes; worker.mutation_record_bytes)")?;
    let pending_bound = limits
        .worker
        .mutation_record_bytes
        .checked_sub(reserve)
        .ok_or("mutation journal cannot reserve complete outcome/context (worker.mutation_record_bytes; reserves transport.body_bytes and committed context)")?;
    bounded_json_size(&journal, pending_bound)
        .map_err(|_| "mutation journal cannot reserve complete outcome/context (worker.mutation_record_bytes; reserves transport.body_bytes and committed context)")?;
    Ok(journal)
}

// A reopened record, or an in-memory outcome retained after failed persistence,
// is not a durability acknowledgement. Re-establish the exact existing Journal
// before it authorizes a send or a saved-result continuation. Keep lookup-only
// reconciliation available when storage cannot accept this barrier.
pub(super) fn persist(path: &Path, journal: &Journal) -> Result<(), String> {
    bounded_json_size(journal, journal.limits.worker.mutation_record_bytes)
        .map_err(|reason| format!("{reason} (worker.mutation_record_bytes)"))?;
    atomic_json(path, journal)
}

pub(super) fn consume(path: &Path, journal: &mut Journal) -> Result<(), String> {
    if journal.outcome.is_none() {
        return Err("cannot consume unresolved mutation".into());
    }
    journal.consumed = true;
    bounded_json_size(journal, journal.limits.worker.mutation_record_bytes)
        .map_err(|reason| format!("{reason} (worker.mutation_record_bytes)"))?;
    atomic_json(path, journal)
}

// AwaitCommands Pending is a validated, uncommitted long-poll result. It is
// not a transport failure and must not consume or replace the original journal.
pub(super) enum Resolution {
    Recorded(Outcome),
    Pending,
    Unavailable,
}

pub(super) async fn resolve(
    client: &ApiClient,
    path: &Path,
    journal: &mut Journal,
) -> Result<Option<Outcome>, String> {
    Ok(match resolve_status(client, path, journal).await? {
        Resolution::Recorded(outcome) => Some(outcome),
        Resolution::Pending | Resolution::Unavailable => None,
    })
}

// Owning the journal keeps the actual request future alive across short cleanup
// batches without borrowing the serving loop's replacement journal slot.
pub(super) async fn resolve_await(
    client: ApiClient,
    path: PathBuf,
    mut journal: Box<Journal>,
) -> (Box<Journal>, Result<Resolution, String>) {
    let result = if matches!(&journal.request, Operation::AwaitCommands { .. }) {
        resolve_status(&client, &path, &mut journal).await
    } else {
        Err("unexpected wait journal operation".into())
    };
    (journal, result)
}

async fn resolve_status(
    client: &ApiClient,
    path: &Path,
    journal: &mut Journal,
) -> Result<Resolution, String> {
    if let Some(outcome) = &journal.outcome {
        persist(path, journal)?;
        return Ok(Resolution::Recorded(outcome.clone()));
    }
    let id = identity(&journal.request)?;
    let acknowledgement = matches!(&journal.request, Operation::AcknowledgeCompletionInput(_));
    let original = if acknowledgement {
        acceptance::lookup_required(client, id, &journal.request).await?
    } else {
        acceptance::lookup(client, id, &journal.request).await?
    };
    let observed = match original {
        acceptance::Lookup::Recorded(outcome, context) => Some((outcome, context)),
        acceptance::Lookup::Unavailable => return Ok(Resolution::Unavailable),
        acceptance::Lookup::Absent => {
            persist(path, journal)?;
            match tokio::time::timeout(
                Duration::from_millis(journal.limits.worker.mutation_call_timeout_ms),
                client.call_observed(journal.request.clone()),
            )
            .await
            {
                Ok(Ok(reply)) => {
                    if let Some(context) = CommitContext::from_reply(&reply, &journal.request)? {
                        Some((reply.outcome, context))
                    } else if matches!(
                        (&journal.request, &reply.outcome),
                        (Operation::AwaitCommands { operation_id: expected, .. },
                         Outcome::Pending { operation_id }) if operation_id == expected
                    ) {
                        return Ok(Resolution::Pending);
                    } else if matches!(reply.outcome, Outcome::Refused { .. }) {
                        let original = if acknowledgement {
                            acceptance::lookup_required(client, id, &journal.request).await?
                        } else {
                            acceptance::lookup(client, id, &journal.request).await?
                        };
                        match original {
                            acceptance::Lookup::Recorded(outcome, context) => {
                                Some((outcome, context))
                            }
                            _ => None,
                        }
                    } else {
                        None
                    }
                }
                Ok(Err(crate::client::CallFailure::Contract(error))) if acknowledgement => return Err(error),
                Ok(Err(crate::client::CallFailure::Unavailable(_))) | Err(_) if acknowledgement => {
                    // The actual input proof is already durable. Recover only its
                    // original ACK receipt, never another mutation or model turn.
                    match acceptance::lookup_required(client, id, &journal.request).await? {
                        acceptance::Lookup::Recorded(outcome, context) => Some((outcome, context)),
                        acceptance::Lookup::Absent | acceptance::Lookup::Unavailable => None,
                    }
                }
                _ => None,
            }
        }
    };
    if let Some((outcome, context)) = &observed {
        bounded_json_size(outcome, journal.limits.transport.body_bytes)
            .map_err(|_| "worker mutation response exceeds bound (transport.body_bytes)")?;
        journal.outcome = Some(outcome.clone());
        journal.outcome_context = Some(context.clone());
        // Exact outcome and original receipt context precede command handling.
        bounded_json_size(journal, journal.limits.worker.mutation_record_bytes)
            .map_err(|reason| format!("{reason} (worker.mutation_record_bytes)"))?;
        atomic_json(path, journal)?;
    }
    Ok(match observed {
        Some((outcome, _)) => Resolution::Recorded(outcome),
        None => Resolution::Unavailable,
    })
}

pub(super) async fn recorded(
    client: &ApiClient,
    path: &Path,
    journal: &mut Journal,
) -> Result<Option<Outcome>, String> {
    if let Some(outcome) = &journal.outcome {
        persist(path, journal)?;
        return Ok(Some(outcome.clone()));
    }
    if let acceptance::Lookup::Recorded(outcome, context) =
        acceptance::lookup(client, identity(&journal.request)?, &journal.request).await?
    {
        journal.outcome = Some(outcome.clone());
        journal.outcome_context = Some(context);
        bounded_json_size(journal, journal.limits.worker.mutation_record_bytes)
            .map_err(|reason| format!("{reason} (worker.mutation_record_bytes)"))?;
        atomic_json(path, journal)?;
        return Ok(Some(outcome));
    }
    Ok(None)
}

pub(super) async fn register(
    client: &ApiClient,
    state: &Path,
    worker: Worker,
) -> Result<bool, String> {
    let path = state.join("mutations/registration.json");
    let epoch = recovery_epoch(state, client.limits())?;
    let mut saved = read(&path, client.limits())?;
    if let Some(journal) = &mut saved {
        let Operation::RegisterWorker { worker: old, .. } = &journal.request else {
            return Err("unexpected registration journal operation".into());
        };
        if old != &worker {
            if resolve(client, &path, journal).await?.is_none() {
                return Ok(false);
            }
            consume(&path, journal)?;
            saved = None;
        } else if matches!(&journal.outcome, Some(Outcome::Refused { code, .. }) if code == "reconciliation_required")
            && epoch > journal.recovery_epoch
        {
            // This is a new logical registration after actual durable recovery,
            // not a retry that evades the prior immutable refusal.
            consume(&path, journal)?;
            saved = None;
        }
    }
    let mut journal = match saved {
        Some(journal) => journal,
        None => create(
            &path,
            Operation::RegisterWorker {
                operation_id: fresh_id("register")?,
                worker: worker.clone(),
            },
            epoch,
            client.limits(),
        )?,
    };
    match resolve(client, &path, &mut journal).await? {
        Some(Outcome::Ok {
            data: Data::Worker(accepted),
        }) if accepted == worker => Ok(true),
        Some(Outcome::Refused { code, .. }) if code == "reconciliation_required" => Ok(false),
        Some(Outcome::Refused { code, .. }) => Err(format!("worker registration refused: {code}")),
        Some(_) => Err("unexpected recorded registration reply".into()),
        None => Ok(false),
    }
}

pub(super) fn settlement_path(state: &Path, operation_id: &str) -> PathBuf {
    state
        .join("mutations/settlements")
        .join(format!("{}.json", component(operation_id)))
}

pub(super) async fn settle(
    client: &ApiClient,
    state: &Path,
    request: Operation,
    control: &Control,
) -> Result<Data, String> {
    let Operation::SettleOperation { operation_id, .. } = &request else {
        return Err("expected settlement".into());
    };
    let path = settlement_path(state, operation_id);
    let mut journal = match read(&path, client.limits())? {
        Some(journal) => {
            if serde_json::to_value(&journal.request).map_err(|_| "settlement encoding failed")?
                != serde_json::to_value(&request).map_err(|_| "settlement encoding failed")?
            {
                if !journal.consumed || identity(&journal.request)? == identity(&request)? {
                    return Err("saved settlement differs; exact reconciliation required".into());
                }
                // A later observation has a different mutation ID, after the
                // predecessor's exact decision was durably consumed. Keep the
                // predecessor bytes as bounded history, not a mutable receipt.
                let retained = state.join("mutations/settlements").join(format!(
                    "settled-{}.json",
                    component(identity(&journal.request)?)
                ));
                if retained.exists() {
                    let bytes =
                        std::fs::read(&retained).map_err(|_| "prior settlement unavailable")?;
                    if bytes != std::fs::read(&path).map_err(|_| "settlement unavailable")? {
                        return Err("prior settlement differs".into());
                    }
                } else {
                    atomic_json(&retained, &journal)?;
                }
                create(&path, request, 0, &control.runtime_limits)?
            } else {
                journal
            }
        }
        None => create(&path, request, 0, &control.runtime_limits)?,
    };
    loop {
        control.check()?;
        let mut stopped = control.clone();
        let result = tokio::select! {
            result = resolve(client, &path, &mut journal) => result?,
            _ = stopped.stopped() => return Err("external settlement interrupted; outcome uncertain".into()),
        };
        match result {
            Some(Outcome::Ok { data }) => {
                consume(&path, &mut journal)?;
                return Ok(data);
            }
            Some(Outcome::Refused { code, .. }) => {
                return Err(format!("controller refused settlement: {code}"));
            }
            _ => {
                tokio::time::sleep(Duration::from_millis(
                    control.worker_limits.reconnect_initial_ms,
                ))
                .await
            }
        }
    }
}

pub(super) async fn recover_settlements(
    client: &ApiClient,
    state: &Path,
    controller_url: &str,
    worker_id: &str,
) -> Result<(), String> {
    let mut paths = Vec::new();
    let mut count = 0usize;
    for entry in std::fs::read_dir(state.join("mutations/settlements"))
        .map_err(|_| "settlement inventory unavailable")?
    {
        let path = entry.map_err(|_| "settlement inventory unreadable")?.path();
        count += 1;
        if path.extension().and_then(|v| v.to_str()) == Some("json") {
            paths.push(path);
        }
        if count > client.limits().worker.command_records {
            return Err("settlement inventory exceeds bound (worker.command_records)".into());
        }
    }
    paths.sort();
    if paths.is_empty() {
        return Ok(());
    }
    // The retained RunAttempt is the policy authority. A journal's policy is
    // untrusted until the exact task/attempt/generation selects that command.
    let commands = super::command_receipts(state, client.limits())?;
    let mut original = Vec::new();
    let mut read_bound = 0;
    for (path, receipt) in &commands {
        let WorkerCommand::RunAttempt { command_id, attempt, task, .. } = &receipt.command else {
            continue;
        };
        task.execution_limits()?;
        let limits = task.resolved_limits.as_ref().ok_or("settlement command limits missing")?.global_metadata.as_ref();
        if path != &state.join("commands").join(format!("{}.json", component(command_id)))
            || attempt.worker_id != worker_id
            || attempt.worker_incarnation.is_empty()
        {
            return Err("settlement original command identity differs".into());
        }
        read_bound = read_bound.max(limits.worker.mutation_record_bytes);
        original.push((&receipt.command, attempt, limits));
    }
    if original.is_empty() {
        return Err("settlement has no original run command".into());
    }
    for path in paths {
        let bytes = read_bytes(&path, read_bound)?.ok_or("settlement journal disappeared")?;
        // Decode only the selector first, with a finite bound derived above.
        // Neither this value nor its saved limits can authorize recovery.
        let selector: Journal = serde_json::from_slice(&bytes)
            .map_err(|_| "corrupt worker mutation journal")?;
        let Operation::SettleOperation {
            settlement_id, operation_id, task_id, attempt_id, generation, ..
        } = &selector.request else {
            return Err("unexpected settlement journal operation".into());
        };
        let mut matching = original.iter().filter(|(_, attempt, _)| {
            &attempt.task_id == task_id && &attempt.id == attempt_id && &attempt.generation == generation
        });
        let (command, _, limits) = matching.next().ok_or("settlement original command missing")?;
        if matching.next().is_some() {
            return Err("settlement original command is ambiguous".into());
        }
        let archived = state.join("mutations/settlements")
            .join(format!("settled-{}.json", component(settlement_id)));
        if operation_id.is_empty() || settlement_id.is_empty() || operation_id == settlement_id
            || (path != settlement_path(state, operation_id) && path != archived)
        {
            return Err("settlement original operation identity differs".into());
        }
        // Validate the same bytes, including completed decisions, under the
        // accepted policy. Do not migrate, rewrite or skip a consumed journal.
        let mut journal = decode(&bytes, limits)?;
        publication::validate_saved_settlement(state, command, &journal.request)?;
        pull_request::validate_saved_settlement(state, command, &journal.request)?;
        if journal.consumed {
            continue;
        }
        let original_client = ApiClient::new(controller_url, Arc::new((**limits).clone()))?;
        match resolve(&original_client, &path, &mut journal).await? {
            Some(_) => consume(&path, &mut journal)?,
            None => return Err("external settlement awaits exact reconciliation".into()),
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "mutations_tests.rs"]
mod tests;
