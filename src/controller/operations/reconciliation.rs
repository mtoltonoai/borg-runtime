//! Retry only a genuine immutable result report whose original receipt records
//! one known infrastructure refusal. No replacement candidate or approval data.
use super::*;

pub(crate) fn original_report(tx: &Connection, id: &str) -> Result<Report> {
    let limits = tx.limits;
    if !valid_id(limits, id) {
        return refuse("invalid_input", "invalid original report identity");
    }
    let Some(receipt) = super::super::metadata::read(tx, id)? else {
        return refuse("not_found", "original report has no committed receipt");
    };
    let Operation::ObserveWorker { report } = receipt.request else {
        return refuse(
            "invalid_transition",
            "original operation is not a result report",
        );
    };
    if report.report_id != id || !matches!(report.observation, Observation::ResultCandidate { .. })
    {
        return refuse(
            "invalid_transition",
            "original operation is not the named result candidate",
        );
    }
    let outcome = receipt.outcome;
    let eligible = match outcome {
        Outcome::Refused { code, message } => matches!(
            (code.as_str(), message.as_str()),
            (
                "capacity",
                "controller native validation capacity exhausted"
            ) | (
                "capacity",
                "controller native validation capacity exhausted: controller.validation_parallelism"
            ) | ("validation_unavailable", "native validation helper failed")
                | (
                    "validation_cleanup_unverified",
                    "native validation helper cleanup remains unverified"
                )
        ),
        _ => false,
    };
    if !eligible {
        return refuse(
            "not_reconcilable",
            "original result did not receive a retryable validation infrastructure refusal",
        );
    }
    Ok(report)
}
