//! Original request diagnostics are durable observations, never retry authority.
//! Physical provider/helper teardown must finish before calling `retain`; it
//! never waits for `acknowledge`. Call `retain` synchronously even if Control has
//! already stopped, then let delivery stop with the exact original outbox intact.
use super::{
    CommitContext, Control, Data, Observation, Operation, Outcome, Reply, Report, Reporter,
    Request, TaskInput, acceptance, bounded_json_size, component, configured_credentials,
    contains_credential, read_report, value_contains_credential,
};
use crate::{
    limits::{NativeLimits, RuntimeLimits},
    native::{
        NativeConfig, ProviderFailureDiagnostic, ProviderOutcome, ProviderRequestBinding,
        RejectionKind,
    },
    native_loop_contract as contract,
    protocol::{MutationCommit, OperationRecord, WIRE_REVISION},
};
use std::{path::Path, time::Duration};

pub(super) struct Admission {
    original: Report,
    native: NativeLimits,
    accepted: RuntimeLimits,
    startup: RuntimeLimits,
    client: RuntimeLimits,
    lookup_backoff_ms: u64,
}

pub(super) struct RetainedDiagnostic {
    original: Report,
    lookup_backoff_ms: u64,
}

fn add(a: usize, b: usize) -> Result<usize, String> {
    a.checked_add(b)
        .ok_or_else(|| "native diagnostic admission size overflow".into())
}

// Concrete metadata::lookup_record shape, including its empty non-task result
// identities. The worker does not interpret an absent diagnostic as success.
fn lookup_record(report: &Report) -> Result<OperationRecord, String> {
    Ok(OperationRecord {
        id: report.report_id.clone(),
        task_id: String::new(),
        attempt_id: String::new(),
        generation: 0,
        kind: "application_mutation".into(),
        state: "settled".into(),
        intended: serde_json::to_value(Operation::ObserveWorker {
            report: report.clone(),
        })
        .map_err(|e| e.to_string())?,
        outcome: Some(
            serde_json::to_value(Outcome::Ok {
                data: Data::Acknowledged,
            })
            .map_err(|e| e.to_string())?,
        ),
    })
}

fn size(
    value: &impl serde::Serialize,
    extra: usize,
    cap: usize,
    name: &str,
) -> Result<usize, String> {
    let bytes = bounded_json_size(value, cap)
        .map_err(|_| format!("native diagnostic admission exceeds {name}"))?;
    let bytes = add(bytes, extra)?;
    if bytes > cap {
        return Err(format!("native diagnostic admission exceeds {name}"));
    }
    Ok(bytes)
}

/// Same Request, acknowledged Reply and original GetOperation/OperationRecord
/// serializers as the controller. `extra` substitutes bounded typed facts for
/// the None sentinel; structured JSON is embedded once, not quoted as a string.
fn envelope(report: &Report, extra: usize, policy: &RuntimeLimits) -> Result<usize, String> {
    let padding = policy
        .transport
        .identity_bytes
        .checked_mul(2)
        .ok_or("native diagnostic request identity bound overflow")?;
    for id in [
        &report.report_id,
        &report.worker_id,
        &report.incarnation,
        &report.task_id,
        &report.attempt_id,
    ] {
        if id.is_empty()
            || id.len() > policy.transport.identity_bytes
            || id.chars().any(char::is_control)
        {
            return Err("native diagnostic identity exceeds transport.identity_bytes".into());
        }
    }
    let report_bytes = size(
        report,
        extra,
        policy.transport.body_bytes,
        "transport.body_bytes (outbox)",
    )?;
    let operation = Operation::ObserveWorker {
        report: report.clone(),
    };
    let request = Request {
        protocol_revision: WIRE_REVISION,
        request_id: String::new(),
        command: operation,
    };
    size(
        &request,
        add(extra, padding)?,
        policy.transport.body_bytes,
        "transport.body_bytes (complete report request)",
    )?;
    let query = Request {
        protocol_revision: WIRE_REVISION,
        request_id: String::new(),
        command: Operation::GetOperation {
            operation_id: report.report_id.clone(),
        },
    };
    size(
        &query,
        padding,
        policy.transport.body_bytes,
        "transport.body_bytes (lookup request)",
    )?;
    let mut reply = Reply {
        request_id: String::new(),
        outcome: Outcome::Ok {
            data: Data::Acknowledged,
        },
        commit: Some(MutationCommit {
            operation_id: report.report_id.clone(),
            controller_revision: i64::MAX as u64,
        }),
        observed_controller_revision: Some(i64::MAX as u64),
    };
    size(
        &reply.outcome,
        0,
        policy.controller.outcome_bytes,
        "controller.outcome_bytes (ACK)",
    )?;
    size(
        &reply,
        padding,
        policy.transport.body_bytes,
        "transport.body_bytes (ACK)",
    )?;
    reply.outcome = Outcome::Ok {
        data: Data::Operation(lookup_record(report)?),
    };
    size(
        &reply.outcome,
        extra,
        policy.controller.outcome_bytes,
        "controller.outcome_bytes (original lookup)",
    )?;
    size(
        &reply,
        add(extra, padding)?,
        policy.transport.body_bytes,
        "transport.body_bytes (original lookup)",
    )?;
    Ok(report_bytes)
}

fn inventory(outbox: &Path, capacity: usize) -> Result<(), String> {
    // Each admitted outbox report can have json/refused, outcome and atomic
    // pending siblings; acceptance is one separate directory. Bound physical
    // traversal too, so unrelated files cannot create an unbounded preflight.
    let scan_cap = capacity
        .checked_mul(4)
        .and_then(|n| n.checked_add(1))
        .ok_or("native diagnostic outbox inventory bound overflow")?;
    let mut records = 0usize;
    for (index, entry) in std::fs::read_dir(outbox)
        .map_err(|_| "native diagnostic outbox unavailable")?
        .enumerate()
    {
        if index >= scan_cap {
            return Err("native diagnostic outbox inventory exceeds worker.outbox_records".into());
        }
        let entry = entry.map_err(|_| "native diagnostic outbox entry unavailable")?;
        if entry.path().extension().and_then(|s| s.to_str()) == Some("json") {
            records += 1;
            if records >= capacity {
                return Err("native diagnostic outbox has no room (worker.outbox_records)".into());
            }
        }
    }
    Ok(())
}

/// Before the marked Usage reservation. Its controller transaction still owns
/// actual credit reservation and current-controller capacity admission.
pub(super) fn preflight(
    reporter: &Reporter,
    input: &TaskInput,
    config: &NativeConfig,
    index: u32,
) -> Result<Admission, String> {
    let snapshot = input
        .resolved_limits
        .as_ref()
        .ok_or("native diagnostic accepted limits absent")?;
    snapshot.validate_for(input)?;
    let native = input.execution_limits()?.native.clone();
    if index == 0
        || index > input.limits.model_requests
        || config.model != input.definition.required_inference()?.model
        || reporter.attempt.worker_id != reporter.config.id
        || reporter.attempt.worker_incarnation != reporter.config.incarnation
    {
        return Err("native diagnostic original caller/index/model differs".into());
    }
    let url =
        reqwest::Url::parse(&config.endpoint).map_err(|_| "native diagnostic endpoint invalid")?;
    let loopback = url
        .host_str()
        .and_then(|host| {
            host.trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .ok()
        })
        .is_some_and(|ip| ip.is_loopback());
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
        || url.host_str().is_none()
        || url.as_str().len() > native.endpoint_bytes
    {
        return Err(
            "native diagnostic trusted route invalid or exceeds native.endpoint_bytes".into(),
        );
    }
    let original = Report {
        report_id: contract::diagnostic_id(&reporter.attempt.id, index),
        worker_id: reporter.config.id.clone(),
        incarnation: reporter.config.incarnation.clone(),
        task_id: reporter.attempt.task_id.clone(),
        attempt_id: reporter.attempt.id.clone(),
        generation: reporter.attempt.generation,
        observation: Observation::NativeRequestDiagnostic {
            request_index: index,
            endpoint: url.to_string(),
            model: config.model.clone(),
            request_binding: Some(ProviderRequestBinding {
                bytes: u64::MAX,
                sha256: "0".repeat(64),
            }),
            observed_outcome: contract::NativeRequestObservedOutcome::KnownRejected {
                rejection_kind: RejectionKind::AuthenticationFailed,
            },
            failure: None,
        },
    };
    let extra = ProviderFailureDiagnostic::maximum_encoded_bytes(&native)?
        .checked_sub(4)
        .ok_or("native diagnostic maximum shape is smaller than null")?;
    for policy in [
        &*snapshot.global_metadata,
        &*reporter.config.limits,
        reporter.client.limits(),
    ] {
        let bytes = envelope(&original, extra, policy)?;
        // Scanner charges <= encoded scalar/key bytes + one per JSON value.
        // Each value consumes at least one encoded byte, so twice the complete
        // encoded Report bounds its work without a source-sized allocation.
        if bytes
            .checked_mul(2)
            .is_none_or(|n| n > policy.worker.credential_scan_units)
        {
            return Err("native diagnostic admission exceeds worker.credential_scan_units".into());
        }
        // Deepest scalar: Report.observation.failure.http.quota.value.limit.
        if policy.worker.credential_scan_depth < 6 {
            return Err("native diagnostic admission exceeds worker.credential_scan_depth".into());
        }
        inventory(&reporter.outbox, policy.worker.outbox_records)?;
    }
    let credentials = configured_credentials(&reporter.config.limits)?;
    for text in [
        original.report_id.as_str(),
        original.worker_id.as_str(),
        original.incarnation.as_str(),
        original.task_id.as_str(),
        original.attempt_id.as_str(),
        config.model.as_str(),
        url.as_str(),
    ] {
        if contains_credential(text, &credentials) {
            return Err("native diagnostic identity/route contains configured credential".into());
        }
    }
    Ok(Admission {
        original,
        native,
        accepted: (*snapshot.global_metadata).clone(),
        startup: (*reporter.config.limits).clone(),
        client: reporter.client.limits().clone(),
        lookup_backoff_ms: input.execution_limits()?.collaboration.lookup_backoff_ms,
    })
}

//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A failed platform-owned provider request MUST retain bounded diagnostics sufficient to identify the observed failure class and available provider retry or quota information.
fn retain_with_credentials(
    reporter: &Reporter,
    admission: &Admission,
    observed_outcome: ProviderOutcome,
    request_binding: Option<&ProviderRequestBinding>,
    failure: Option<&ProviderFailureDiagnostic>,
    credentials: &[String],
) -> Result<RetainedDiagnostic, String> {
    let expected = &admission.original;
    if reporter.config.id != expected.worker_id
        || reporter.config.incarnation != expected.incarnation
        || reporter.attempt.id != expected.attempt_id
        || reporter.attempt.task_id != expected.task_id
        || reporter.attempt.generation != expected.generation
        || *reporter.config.limits != admission.startup
        || reporter.client.limits() != &admission.client
    {
        return Err("native diagnostic original admission differs".into());
    }
    if let Some(binding) = request_binding {
        binding.validate(admission.native.request_bytes)?;
    } else if !matches!(observed_outcome, ProviderOutcome::NotStarted) {
        return Err("native diagnostic exposed request lacks original binding".into());
    }
    if matches!(
        observed_outcome,
        ProviderOutcome::NotStarted | ProviderOutcome::Unknown | ProviderOutcome::Rejected(_)
    ) && failure.is_none()
    {
        return Err("native diagnostic failed request lacks typed failure".into());
    }
    let mut facts = failure.cloned();
    if matches!(observed_outcome, ProviderOutcome::NotStarted)
        && facts.as_ref().is_some_and(|facts| facts.http.is_some())
    {
        return Err("native diagnostic unstarted request has HTTP facts".into());
    }
    if let Some(facts) = &mut facts {
        facts.validate(&admission.native)?;
        for credential in credentials {
            facts.redact_credential(credential);
        }
    }
    let mut original = expected.clone();
    let Observation::NativeRequestDiagnostic {
        request_binding: binding,
        observed_outcome: disposition,
        failure,
        ..
    } = &mut original.observation
    else {
        return Err("native diagnostic admission kind differs".into());
    };
    *binding = request_binding.cloned();
    *disposition = observed_outcome.into();
    *failure = facts;
    for policy in [&admission.accepted, &admission.startup, &admission.client] {
        envelope(&original, 0, policy)?;
        let value = serde_json::to_value(&original)
            .map_err(|_| "native diagnostic report encoding failed")?;
        if value_contains_credential(&value, credentials, &policy.worker)? {
            return Err("native diagnostic contains configured credential; report withheld".into());
        }
    }
    // No cancellation, deadline, current-owner lookup or await precedes this
    // original outbox barrier. Never repair/scrub an already retained body.
    let path = reporter
        .outbox
        .join(format!("{}.json", component(&original.report_id)));
    for prior in [&path, &path.with_extension("refused")] {
        if prior.exists() && read_report(prior, &reporter.config.limits)? != original {
            return Err("native diagnostic stable identity has changed body".into());
        }
    }
    let saved = reporter.retain_identified_with_credentials(
        original.report_id.clone(),
        original.observation.clone(),
        credentials,
    )?;
    if saved != original {
        return Err("native diagnostic retained normalization changed original facts".into());
    }
    Ok(RetainedDiagnostic {
        original: saved,
        lookup_backoff_ms: admission.lookup_backoff_ms,
    })
}

pub(super) fn retain(
    reporter: &Reporter,
    admission: &Admission,
    observed_outcome: ProviderOutcome,
    request_binding: Option<&ProviderRequestBinding>,
    failure: Option<&ProviderFailureDiagnostic>,
) -> Result<RetainedDiagnostic, String> {
    retain_with_credentials(
        reporter,
        admission,
        observed_outcome,
        request_binding,
        failure,
        &configured_credentials(&reporter.config.limits)?,
    )
}

fn remove_acknowledged(reporter: &Reporter, report: &Report) -> Result<(), String> {
    let path = reporter
        .outbox
        .join(format!("{}.json", component(&report.report_id)));
    if path.exists() {
        if read_report(&path, &reporter.config.limits)? != *report {
            return Err("native diagnostic retained body changed".into());
        }
        std::fs::remove_file(path)
            .map_err(|_| "native diagnostic acknowledged outbox removal failed")?;
        std::fs::File::open(&reporter.outbox)
            .and_then(|f| f.sync_all())
            .map_err(|_| "native diagnostic outbox sync failed")?;
    }
    Ok(())
}

/// Delivery cancellation preserves the original report. A returned ACK proves
/// only this historical append, never current ownership or permission to POST.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A failed platform-owned provider request MUST retain bounded diagnostics sufficient to identify the observed failure class and available provider retry or quota information.
pub(super) async fn acknowledge(
    reporter: &Reporter,
    control: &Control,
    retained: &RetainedDiagnostic,
) -> Result<CommitContext, String> {
    let report = &retained.original;
    let intended = Operation::ObserveWorker {
        report: report.clone(),
    };
    let credentials = configured_credentials(&reporter.config.limits)?;
    let value =
        serde_json::to_value(report).map_err(|_| "native diagnostic retained encoding failed")?;
    if value_contains_credential(&value, &credentials, &reporter.config.limits.worker)? {
        return Err("native diagnostic retained credential match; original bytes preserved".into());
    }
    let mut stopped = control.clone();
    let delivery = async {
        loop {
            control.check()?;
            let lock = reporter.outbox_lock.lock().await;
            // A locally retained NotStarted may precede resolution of a lost
            // Usage ACK. It proves no POST, not that the reservation committed.
            // Never send it until the exact original successful charge exists.
            if !super::native_diagnostic_reservation_ready(&reporter.client, report).await? {
                drop(lock);
                tokio::time::sleep(Duration::from_millis(retained.lookup_backoff_ms)).await;
                continue;
            }
            match acceptance::lookup_required(&reporter.client, &report.report_id, &intended)
                .await?
            {
                acceptance::Lookup::Recorded(
                    Outcome::Ok {
                        data: Data::Acknowledged,
                    },
                    context,
                ) => {
                    remove_acknowledged(reporter, report)?;
                    return Ok(context);
                }
                acceptance::Lookup::Recorded(_, _) => {
                    return Err("native diagnostic original append refused; outbox retained".into());
                }
                acceptance::Lookup::Absent => {
                    let path = reporter
                        .outbox
                        .join(format!("{}.json", component(&report.report_id)));
                    if !path.exists() || read_report(&path, &reporter.config.limits)? != *report {
                        return Err(
                            "native diagnostic original outbox absent or changed before replay"
                                .into(),
                        );
                    }
                    let call = tokio::time::timeout(
                        Duration::from_millis(
                            reporter.client.limits().worker.outbox_call_timeout_ms,
                        ),
                        reporter.client.call_observed(intended.clone()),
                    )
                    .await;
                    match call {
                        Ok(Ok(reply)) => {
                            let context = CommitContext::from_reply(&reply, &intended)?;
                            if matches!(reply.outcome, Outcome::Refused { .. }) {
                                return Err(
                                    "native diagnostic original append refused; outbox retained"
                                        .into(),
                                );
                            }
                            if let Some(context) = context {
                                if matches!(
                                    reply.outcome,
                                    Outcome::Ok {
                                        data: Data::Acknowledged
                                    }
                                ) {
                                    remove_acknowledged(reporter, report)?;
                                    return Ok(context);
                                }
                                return Err(
                                    "native diagnostic original append refused; outbox retained"
                                        .into(),
                                );
                            }
                        }
                        Ok(Err(crate::client::CallFailure::Contract(error))) => return Err(error),
                        Ok(Err(crate::client::CallFailure::Unavailable(_))) | Err(_) => {}
                    }
                }
                acceptance::Lookup::Unavailable => {}
            }
            drop(lock);
            tokio::time::sleep(Duration::from_millis(retained.lookup_backoff_ms)).await;
        }
    };
    tokio::select! { biased;
        _ = stopped.stopped() => Err("native diagnostic delivery cancelled or expired; original outbox retained".into()),
        result = delivery => result,
    }
}

#[cfg(test)]
#[path = "native_request_diagnostics_tests.rs"]
mod tests;
