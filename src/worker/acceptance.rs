//! Reconcile acceptance of a completed result, never repeat its execution.
use super::*;
use std::io::Read;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    limits: crate::limits::RuntimeLimits,
    format_revision: u32,
    original: Report,
    refusal: Outcome,
    refusal_context: CommitContext,
    reconciliation: Option<ReconcileResultInput>,
    outcome: Option<Outcome>,
    outcome_context: Option<CommitContext>,
    notice: Report,
}

pub(super) enum Lookup {
    Absent,
    Recorded(Outcome, CommitContext),
    Unavailable,
}

async fn call(client: &ApiClient, operation: Operation) -> Option<Reply> {
    match tokio::time::timeout(
        Duration::from_millis(client.limits().worker.acceptance_call_timeout_ms),
        client.call(operation),
    )
    .await
    {
        Ok(Ok(reply)) => Some(reply),
        _ => None,
    }
}

pub(super) async fn lookup(
    client: &ApiClient,
    id: &str,
    intended: &Operation,
) -> Result<Lookup, String> {
    let query = Operation::GetOperation {
        operation_id: id.into(),
    };
    let Some(reply) = call(client, query.clone()).await else {
        return Ok(Lookup::Unavailable);
    };
    lookup_reply(reply, &query, id, intended)
}

// Required native report recovery may wait for unavailable transport, but a
// complete invalid response is never an availability retry signal.
pub(super) async fn lookup_required(client: &ApiClient, id: &str, intended: &Operation) -> Result<Lookup, String> {
    let query = Operation::GetOperation { operation_id: id.into() };
    let reply = match tokio::time::timeout(
        Duration::from_millis(client.limits().worker.acceptance_call_timeout_ms),
        client.call_observed(query.clone()),
    ).await {
        Ok(Ok(reply)) => reply,
        Ok(Err(crate::client::CallFailure::Contract(error))) => return Err(error),
        Ok(Err(crate::client::CallFailure::Unavailable(_))) | Err(_) => return Ok(Lookup::Unavailable),
    };
    if matches!(&reply.outcome, Outcome::Ok { data } if !matches!(data, Data::Operation(_))) {
        return Err("required report lookup response kind differs".into());
    }
    lookup_reply(reply, &query, id, intended)
}

fn lookup_reply(reply: Reply, query: &Operation, id: &str, intended: &Operation) -> Result<Lookup, String> {
    let context = CommitContext::from_reply(&reply, query)?;
    match reply.outcome {
        Outcome::Ok {
            data: Data::Operation(record),
        } => {
            if record.id != id
                || record.kind != "application_mutation"
                || record.state != "settled"
                || record.intended
                    != serde_json::to_value(intended)
                        .map_err(|_| "report lookup encoding failed")?
            {
                return Err(
                    "recorded report operation differs; acceptance reconciliation required".into(),
                );
            }
            let context = context.ok_or("recorded mutation lacks committed context")?;
            match record
                .outcome
                .and_then(|value| serde_json::from_value(value).ok())
            {
                Some(outcome @ (Outcome::Ok { .. } | Outcome::Refused { .. })) => {
                    context.validate(intended, &outcome)?;
                    Ok(Lookup::Recorded(outcome, context))
                }
                _ => Err(
                    "recorded report outcome is invalid; acceptance reconciliation required".into(),
                ),
            }
        }
        Outcome::Refused { code, .. } if code == "not_found" => Ok(Lookup::Absent),
        _ => Ok(Lookup::Unavailable),
    }
}

async fn committed_call(
    client: &ApiClient,
    operation: Operation,
) -> Result<Option<(Outcome, CommitContext)>, String> {
    let Some(reply) = call(client, operation.clone()).await else {
        return Ok(None);
    };
    let Some(context) = CommitContext::from_reply(&reply, &operation)? else {
        return Ok(None);
    };
    Ok(Some((reply.outcome, context)))
}

fn eligible(outcome: &Outcome) -> bool {
    matches!(outcome, Outcome::Refused { code, message } if matches!((code.as_str(),message.as_str()),
        ("capacity", "controller native validation capacity exhausted")
        | ("capacity", "controller native validation capacity exhausted: controller.validation_parallelism")
        | ("validation_unavailable", "native validation helper failed")
        | ("validation_cleanup_unverified", "native validation helper cleanup remains unverified")))
}

fn fresh_id() -> Result<String, String> {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| "acceptance identity clock unavailable")?
        .as_nanos();
    Ok(format!(
        "acceptance-{stamp}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

fn path(outbox: &Path, id: &str) -> PathBuf {
    outbox
        .join("acceptance")
        .join(format!("{}.json", component(id)))
}

fn read(path: &Path, limits: &crate::limits::RuntimeLimits) -> Result<Journal, String> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|_| "acceptance journal unavailable")?
        .take(limits.worker.mutation_record_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "acceptance journal unreadable")?;
    if bytes.len() > limits.worker.mutation_record_bytes {
        return Err("acceptance journal exceeds bound (worker.mutation_record_bytes)".into());
    }
    let journal: Journal =
        serde_json::from_slice(&bytes).map_err(|_| "invalid acceptance journal")?;
    if journal.limits != *limits
        || journal.format_revision != 3
        || !matches!(
            journal.original.observation,
            Observation::ResultCandidate { .. }
        )
        || !matches!(journal.refusal, Outcome::Refused { .. })
        || journal
            .reconciliation
            .as_ref()
            .is_some_and(|request| request.original_report_id != journal.original.report_id)
    {
        return Err("unsupported acceptance journal".into());
    }
    journal.refusal_context.validate(
        &Operation::ObserveWorker {
            report: journal.original.clone(),
        },
        &journal.refusal,
    )?;
    if journal.outcome.is_some() != journal.outcome_context.is_some() {
        return Err("acceptance outcome lacks committed context".into());
    }
    if let (Some(outcome), Some(context)) = (&journal.outcome, &journal.outcome_context) {
        let original = Operation::ObserveWorker {
            report: journal.original.clone(),
        };
        let operation = journal
            .reconciliation
            .as_ref()
            .map(|input| Operation::ReconcileResult(input.clone()))
            .unwrap_or(original);
        context.validate(&operation, outcome)?;
    }
    Ok(journal)
}

fn entries(
    outbox: &Path,
    include_settled: bool,
    limits: &crate::limits::RuntimeLimits,
) -> Result<Vec<PathBuf>, String> {
    let directory = outbox.join("acceptance");
    if !directory.exists() {
        return Ok(vec![]);
    }
    let mut paths = Vec::new();
    for entry in
        std::fs::read_dir(directory).map_err(|_| "acceptance journal directory unavailable")?
    {
        let path = entry
            .map_err(|_| "acceptance journal entry unavailable")?
            .path();
        let extension = path.extension().and_then(|x| x.to_str());
        if extension == Some("json") || include_settled && extension == Some("settled") {
            paths.push(path);
        }
        if paths.len() > limits.worker.outbox_records {
            return Err("acceptance journal inventory exceeds bound (worker.outbox_records)".into());
        }
    }
    paths.sort();
    Ok(paths)
}

pub(super) fn retain(
    outbox: &Path,
    original: &Report,
    refusal: &Outcome,
    context: &CommitContext,
    limits: &crate::limits::RuntimeLimits,
) -> Result<(), String> {
    context.validate(
        &Operation::ObserveWorker {
            report: original.clone(),
        },
        refusal,
    )?;
    let target = path(outbox, &original.report_id);
    for retained in [&target, &target.with_extension("settled")] {
        if retained.exists() {
            let previous = read(retained, limits)?;
            if previous.original != *original
                || previous.refusal != *refusal
                || previous.refusal_context.commit != context.commit
            {
                return Err("acceptance journal identity has changed body".into());
            }
            return Ok(());
        }
    }
    let id = fresh_id()?;
    let reconciliation = eligible(refusal).then(|| ReconcileResultInput {
        operation_id: id.clone(),
        original_report_id: original.report_id.clone(),
    });
    let mut notice = original.clone();
    notice.report_id = format!("{id}-notice");
    notice.observation = Observation::Progress {
        text:
            "Completed result acceptance is refused; original decision retained for reconciliation"
                .into(),
        output: String::new(),
        truncated: false,
    };
    let journal = Journal {
        limits: limits.clone(),
        format_revision: 3,
        original: original.clone(),
        refusal: refusal.clone(),
        refusal_context: context.clone(),
        outcome_context: reconciliation.is_none().then(|| context.clone()),
        outcome: reconciliation.is_none().then(|| refusal.clone()),
        reconciliation,
        notice,
    };
    if bounded_json_size(&journal, limits.worker.mutation_record_bytes).is_err() {
        return Err("acceptance journal exceeds bound (worker.mutation_record_bytes)".into());
    }
    std::fs::create_dir_all(target.parent().unwrap())
        .map_err(|_| "acceptance journal directory unavailable")?;
    // Exact request, original refusal and fresh identity are durable before the
    // first revalidation call. Restart never regenerates an acceptance identity.
    atomic_json(&target, &journal)
}

pub(super) fn pending(
    outbox: &Path,
    limits: &crate::limits::RuntimeLimits,
) -> Result<bool, String> {
    Ok(!entries(outbox, false, limits)?.is_empty())
}

// Exact local attempt association, with all journal/context parsing retained.
pub(super) fn pending_for(outbox: &Path, attempt: &Attempt, limits: &crate::limits::RuntimeLimits) -> Result<bool, String> {
    for path in entries(outbox, false, limits)? {
        let journal = read(&path, limits)?;
        let report = &journal.original;
        if report.task_id == attempt.task_id || report.attempt_id == attempt.id { return Ok(true); }
    }
    Ok(false)
}

pub(super) fn completed_decision(
    outbox: &Path,
    attempt: &Attempt,
    limits: &crate::limits::RuntimeLimits,
) -> Result<bool, String> {
    for path in entries(outbox, true, limits)? {
        let journal = read(&path, limits)?;
        let r = &journal.original;
        if r.attempt_id == attempt.id
            && r.task_id == attempt.task_id
            && r.generation == attempt.generation
            && r.worker_id == attempt.worker_id
            && r.incarnation == attempt.worker_incarnation
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) async fn flush(
    client: &ApiClient,
    outbox: &Path,
    credentials: &[String],
) -> Result<(), String> {
    let limits = client.limits();
    for path in entries(outbox, false, limits)?
        .into_iter()
        .take(limits.worker.outbox_batch)
    {
        let mut journal = read(&path, limits)?;
        if report_contains_credential(&journal.original, credentials, &limits.worker)
            .map_err(|reason| format!("acceptance journal exceeds bounded credential inspection: {reason}"))?
            || report_contains_credential(&journal.notice, credentials, &limits.worker)
                .map_err(|reason| format!("acceptance notice exceeds bounded credential inspection: {reason}"))?
        {
            return Err(
                "acceptance journal contains a configured credential; reconciliation withheld"
                    .into(),
            );
        }
        if journal.outcome.is_none() {
            let input = journal
                .reconciliation
                .as_ref()
                .ok_or("acceptance reconciliation identity missing")?;
            let operation = Operation::ReconcileResult(input.clone());
            let observed = match lookup(client, &input.operation_id, &operation).await? {
                Lookup::Recorded(outcome, context) => Some((outcome, context)),
                Lookup::Absent => committed_call(client, operation).await?,
                Lookup::Unavailable => None,
            };
            let Some((outcome, context)) = observed else {
                continue;
            };
            journal.outcome = Some(outcome);
            journal.outcome_context = Some(context);
            bounded_json_size(&journal, limits.worker.mutation_record_bytes).map_err(|reason| format!("{reason} (worker.mutation_record_bytes)"))?;
            atomic_json(&path, &journal)?;
        }
        if matches!(journal.outcome, Some(Outcome::Refused { .. })) {
            // A permanent acceptance decision is not a new executor failure.
            // Publish a bounded status observation, never a replacement result.
            let operation = Operation::ObserveWorker {
                report: journal.notice.clone(),
            };
            let settled = match lookup(client, &journal.notice.report_id, &operation).await? {
                Lookup::Recorded(..) => true,
                Lookup::Absent => committed_call(client, operation).await?.is_some(),
                Lookup::Unavailable => false,
            };
            if !settled {
                continue;
            }
        }
        std::fs::rename(&path, path.with_extension("settled"))
            .map_err(|_| "acceptance decision retention failed")?;
    }
    Ok(())
}

#[cfg(test)]
mod diagnostic_tests {
    use super::*;

    #[test]
    fn revalidation_capacity_eligibility_accepts_only_the_two_exact_messages() {
        for message in [
            "controller native validation capacity exhausted",
            "controller native validation capacity exhausted: controller.validation_parallelism",
        ] {
            assert!(eligible(&Outcome::Refused {
                code: "capacity".into(),
                message: message.into(),
            }));
        }
        for (code, message) in [
            ("capacity", "controller native validation capacity exhausted: worker.native_validation_capacity"),
            ("capacity", "controller native validation capacity exhausted: controller.validation_parallelism extra"),
            ("capacity", "controller native validation capacity exhausted extra"),
            ("invalid_input", "controller native validation capacity exhausted: controller.validation_parallelism"),
        ] {
            assert!(!eligible(&Outcome::Refused {
                code: code.into(),
                message: message.into(),
            }), "unexpected eligibility for {code}: {message}");
        }
    }
}
