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
        Operation::PrepareAgentMemory { operation_id, .. }
        | Operation::AcknowledgeAgentMemory { operation_id, .. }
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
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("worker mutation journal unavailable".into()),
    };
    let mut bytes = Vec::new();
    file.take(limits.worker.mutation_record_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "worker mutation journal unreadable")?;
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
    Ok(Some(journal))
}

pub(super) fn create(
    path: &Path,
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
    atomic_json(path, &journal)?;
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

pub(super) async fn resolve(
    client: &ApiClient,
    path: &Path,
    journal: &mut Journal,
) -> Result<Option<Outcome>, String> {
    if let Some(outcome) = &journal.outcome {
        persist(path, journal)?;
        return Ok(Some(outcome.clone()));
    }
    let id = identity(&journal.request)?;
    let observed = match acceptance::lookup(client, id, &journal.request).await? {
        acceptance::Lookup::Recorded(outcome, context) => Some((outcome, context)),
        acceptance::Lookup::Unavailable => return Ok(None),
        acceptance::Lookup::Absent => {
            persist(path, journal)?;
            match tokio::time::timeout(
                Duration::from_millis(journal.limits.worker.mutation_call_timeout_ms),
                client.call(journal.request.clone()),
            )
            .await
            {
                Ok(Ok(reply)) => {
                    if let Some(context) = CommitContext::from_reply(&reply, &journal.request)? {
                        Some((reply.outcome, context))
                    } else if matches!(reply.outcome, Outcome::Refused { .. }) {
                        match acceptance::lookup(client, id, &journal.request).await? {
                            acceptance::Lookup::Recorded(outcome, context) => {
                                Some((outcome, context))
                            }
                            _ => None,
                        }
                    } else {
                        None
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
    Ok(observed.map(|(outcome, _)| outcome))
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

pub(super) async fn recover_settlements(client: &ApiClient, state: &Path) -> Result<(), String> {
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
    for path in paths {
        let mut journal = read(&path, client.limits())?.ok_or("settlement journal disappeared")?;
        if !matches!(journal.request, Operation::SettleOperation { .. }) {
            return Err("unexpected settlement journal operation".into());
        }
        if journal.consumed {
            continue;
        }
        match resolve(client, &path, &mut journal).await? {
            Some(_) => consume(&path, &mut journal)?,
            None => return Err("external settlement awaits exact reconciliation".into()),
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "mutations_tests.rs"]
mod tests;
