//! Serializable circuit coordination; the controller owns persistence and receipts.
use crate::mcp::Caller;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct CircuitKey {
    /// The normalized URL of the actual selected endpoint, including its path.
    pub endpoint: String,
    /// The exact selected model identifier; never an inferred provider alias.
    pub model: String,
}

impl CircuitKey {
    /// The caller must first apply the existing accepted endpoint and model policy.
    pub fn new(endpoint: &reqwest::Url, model: &str) -> Result<Self, String> {
        let value = Self { endpoint: endpoint.as_str().to_owned(), model: model.to_owned() };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), String> {
        let endpoint = reqwest::Url::parse(&self.endpoint)
            .map_err(|_| "native circuit endpoint is invalid")?;
        if endpoint.as_str() != self.endpoint
            || !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || self.model.is_empty()
        {
            return Err("native circuit key is invalid or not normalized".into());
        }
        Ok(())
    }
}

/// Accepted failure behavior. Retained-key capacity belongs to controller metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CircuitPolicy {
    pub failure_threshold: u32,
    pub open_ms: u64,
}

impl CircuitPolicy {
    pub fn validate(&self) -> Result<(), String> {
        if self.failure_threshold == 0 || self.open_ms == 0 {
            return Err("native circuit failure_threshold and open_ms must be positive".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CircuitPermit {
    /// Original acquire operation, resolved through its canonical receipt on retry.
    pub operation_id: String,
    pub caller: Caller,
    pub key: CircuitKey,
    pub generation: u64,
    pub probe: bool,
    /// The exact accepted policy at acquisition, not a later worker default.
    pub policy: CircuitPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CircuitPhase {
    Closed,
    Open { until_ms: u64 },
    HalfOpen { probe: CircuitPermit },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CircuitState {
    pub key: CircuitKey,
    /// Fences replies from requests admitted before a later open/close transition.
    pub generation: u64,
    pub consecutive_failures: u32,
    pub phase: CircuitPhase,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CircuitAdmission {
    Admitted { permit: CircuitPermit },
    Open { retry_at_ms: u64 },
    ProbePending { operation_id: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CircuitAcquisition {
    pub state: CircuitState,
    pub admission: CircuitAdmission,
}

/// Health evidence only. Retry safety is a separate native provider disposition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CircuitOutcome {
    Healthy,
    TransientFailure,
    /// Neutral circuit-health observation: an unstarted request or a complete
    /// known nontransient rejection. The immutable request-slot outcome, not
    /// this health transition, establishes whether a POST started or was rejected.
    NotStarted,
    /// The request effect is unresolved. Health may enter cooldown without
    /// asserting that effect absent or granting a resend of the old request.
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CircuitCompletionDisposition {
    Applied,
    StaleGeneration,
    OutcomeUnknown { retry_at_ms: Option<u64> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CircuitCompletion {
    pub state: CircuitState,
    pub disposition: CircuitCompletionDisposition,
}

/// Auth coordination deliberately omits model; this is a nonsecret host handle.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeAuthenticationKey {
    pub endpoint: String,
    pub credential_configuration: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeRequestPermit {
    pub circuit: CircuitPermit,
    pub request_index: u32,
    pub credential_configuration: String,
    pub auth_generation: u64,
    pub auth_probe: bool,
    pub auth_cooldown_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NativeWaitReason { CircuitOpen, CircuitProbePending, AuthenticationCooldown, AuthenticationProbePending }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeRequestAdmission {
    Granted { permit: NativeRequestPermit },
    Wait { reason: NativeWaitReason, retry_at_ms: Option<u64>, generation: u64 },
}

/// Coordination evidence is not a provider-effect reconciliation or retry grant.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeRequestOutcome {
    Healthy,
    Rejected { rejection: crate::native::ProviderRejection },
    /// Compatibility spelling requiring the exact canonical authentication
    /// rejection and Usage receipts. New workers use Rejected for this class.
    AuthenticationFailed,
    Unknown,
    NotStarted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NativeSettlementSource { Reported, FencedOwner }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeRequestSettlement {
    pub operation_id: String,
    pub permit: NativeRequestPermit,
    pub outcome: NativeRequestOutcome,
    pub source: NativeSettlementSource,
    pub circuit: CircuitCompletionDisposition,
    pub circuit_retry_at_ms: Option<u64>,
    pub auth_retry_at_ms: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalized_endpoint_and_exact_model_define_the_key() {
        let first = CircuitKey::new(&reqwest::Url::parse("https://EXAMPLE.invalid:443/v1/responses").unwrap(), "model-a").unwrap();
        let same = CircuitKey::new(&reqwest::Url::parse("https://example.invalid/v1/responses").unwrap(), "model-a").unwrap();
        assert_eq!(first, same);
        assert_ne!(first, CircuitKey::new(&reqwest::Url::parse(&first.endpoint).unwrap(), "model-b").unwrap());
        assert_ne!(first, CircuitKey::new(&reqwest::Url::parse("https://example.invalid/other/responses").unwrap(), "model-a").unwrap());
        assert_ne!(first, CircuitKey::new(&reqwest::Url::parse("https://other.invalid/v1/responses").unwrap(), "model-a").unwrap());
    }

    #[test]
    fn stored_noncanonical_or_credential_bearing_key_refuses() {
        for endpoint in ["https://EXAMPLE.invalid/v1/responses", "https://user@example.invalid/v1/responses", "https://example.invalid/v1/responses?key=value", "https://example.invalid/v1/responses#fragment"] {
            assert!(CircuitKey { endpoint: endpoint.into(), model: "model-a".into() }.validate().is_err());
        }
    }
}
