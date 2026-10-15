//! Shared pure validation, not operation identity or currentness authority.
use super::{ValidationHost, cpu, live, schema};
use serde_json::Value;
use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::time::Instant;

#[derive(Debug, PartialEq, Eq)]
pub enum JsonValidationError {
    /// A static resource/profile/schema mismatch code; never input text.
    Refused(String),
    Deadline,
    /// The helper did not establish a result. This is not invalid-schema proof.
    Helper(String),
    /// Termination is unverified; no completion/termination acknowledgment.
    CleanupUnverified,
}

/// Only a successful invocation of the real CPU validator constructs this.
/// It binds data equality, not caller authority, task state or operation identity.
pub struct ValidatedJson {
    schema: Value,
    output: Option<Value>,
    output_bound: usize,
}
impl ValidatedJson {
    pub fn matches(&self, schema: &Value, output: Option<&Value>, output_bound: usize) -> bool {
        self.output_bound == output_bound
            && &self.schema == schema
            && self.output.as_ref() == output
    }
}

/// Debug compatibility fixture only, not restart-qualified. Release builds
/// require validate_json_with_host and refuse a program-only invocation.
pub async fn validate_json_with_program(
    schema_value: &Value,
    output: Option<&Value>,
    output_bound: usize,
    deadline: Instant,
    program: &Path,
) -> Result<ValidatedJson, JsonValidationError> {
    #[cfg(debug_assertions)]
    {
        let host = super::host::FixtureHost::new(program).map_err(JsonValidationError::Helper)?;
        validate_json_with_host(schema_value, output, output_bound, deadline, &host.0).await
    }
    #[cfg(not(debug_assertions))]
    {
        let _ = (schema_value, output, output_bound, deadline, program);
        Err(JsonValidationError::Helper(
            "native_validation_host_required".into(),
        ))
    }
}

/// Run outside every SQL transaction. The owner recovers the durable helper
/// registry under exclusive ownership before admission, and rechecks mutable
/// state before commit. Pure validation does not confer any of that authority.
//= spec/impl/v2/implementation.md#the-native-executor-is-bounded
//# The native executor MUST validate a result against that schema before accepting it.
// Native component scope: shared data validation only; callers retain exact-operation, freshness and commit checks.
pub async fn validate_json_with_host(
    schema_value: &Value,
    output: Option<&Value>,
    output_bound: usize,
    deadline: Instant,
    host: &ValidationHost,
) -> Result<ValidatedJson, JsonValidationError> {
    let policy = &host.native_limits;
    policy.validate().map_err(JsonValidationError::Refused)?;
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or(JsonValidationError::Deadline)?;
    let deadline_ms = SystemTime::now()
        .checked_add(remaining)
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .and_then(|d| u64::try_from(d.as_millis()).ok())
        .ok_or_else(|| JsonValidationError::Helper("native_clock_invalid".into()))?;
    let mut cpu = cpu::State::new(host);
    let result = {
        let work = async {
            if output_bound == 0 || output_bound > policy.output_bytes {
                return Err("native_output_limit_invalid_native_output_bytes".to_string());
            }
            schema::measure(
                schema_value,
                policy.schema_bytes,
                policy,
                "native_schema_bytes",
            )?;
            let encoded = match output {
                Some(output) => {
                    schema::measure(output, output_bound, policy, "task_output_bytes")?;
                    Some(
                        serde_json::to_vec(output)
                            .map_err(|_| "native_request_encoding".to_string())?,
                    )
                }
                None => None,
            };
            let actual = cpu
                .run(
                    host,
                    schema_value,
                    encoded.as_deref(),
                    output_bound,
                    cpu::ReplyBound::TaskOutput,
                    deadline_ms,
                    true,
                )
                .await?;
            // A result for some other input, or schema-only success for a Value,
            // cannot mint a binding even if an internal protocol bug is added.
            if actual != encoded {
                return Err("native_validation_reply_invalid".to_string());
            }
            live(deadline)?;
            let bound = ValidatedJson {
                schema: schema_value.clone(),
                output: output.cloned(),
                output_bound,
            };
            tokio::task::yield_now().await;
            live(deadline)?;
            Ok(bound)
        };
        tokio::pin!(work);
        tokio::select! {
            biased;
            _=tokio::time::sleep_until(deadline)=>Err("native_deadline_exceeded".to_string()),
            result=&mut work=>result,
        }
    };
    cpu.finish(true).await.map_err(classify)?;
    result.map_err(classify)
}

fn classify(code: String) -> JsonValidationError {
    match code.as_str() {
        "native_deadline_exceeded" => JsonValidationError::Deadline,
        "native_validation_cleanup_unverified" => JsonValidationError::CleanupUnverified,
        "native_validation_work_bound"
        | "native_output_limit_invalid_native_output_bytes"
        | "native_output_not_json"
        | "native_output_schema_mismatch" => JsonValidationError::Refused(code),
        value
            if value.starts_with("native_schema_")
                || value.starts_with("native_json_")
                || value.starts_with("native_validation_work_bound_") =>
        {
            JsonValidationError::Refused(code)
        }
        _ => JsonValidationError::Helper(code),
    }
}
