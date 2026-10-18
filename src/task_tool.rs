//! The one configured native tool's finite data contract. Values here are data;
//! the worker performs the real controller read and the controller verifies the
//! retained report/current attempt before accepting the final result.
use crate::{limits::NativeLimits, protocol::*};
use serde::{Deserialize, Serialize};

pub const NAME: &str = "get_current_task";
pub const PROGRESS_TEXT: &str = "Native get_current_task observation";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CurrentTaskProjection {
    pub task_id: String,
    pub agent_id: String,
    pub definition: Definition,
    pub status: TaskStatus,
    pub observed_controller_revision: u64,
    pub deadline_unix_ms: u64,
    pub attempt_id: String,
    pub generation: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ToolObservation {
    pub call_id: String,
    pub task: CurrentTaskProjection,
}

pub fn enabled(input: &TaskInput) -> bool {
    matches!(
        input.native_workflow.as_deref(),
        Some(NativeWorkflow::InspectCurrentTaskThenContinue {})
    )
}
pub fn report_id(attempt: &str) -> String {
    format!("{attempt}-native-current-task")
}
pub fn evidence(attempt: &str) -> String {
    format!("native get_current_task receipt: {}", report_id(attempt))
}
fn identity(value: &str, limits: &NativeLimits) -> bool {
    !value.is_empty()
        && value.len() <= limits.task_tool_identity_bytes
        && !value.chars().any(char::is_control)
}

pub fn project(reply: &Reply, expected: &Attempt) -> Result<CurrentTaskProjection, String> {
    reply.validate_for(&Operation::GetTask {
        task_id: expected.task_id.clone(),
    })?;
    let Outcome::Ok {
        data: Data::Task(task),
    } = &reply.outcome
    else {
        return Err("native_task_tool_read_unavailable".into());
    };
    let a = task
        .attempt
        .as_ref()
        .ok_or("native_task_tool_owner_unavailable")?;
    if task.id != expected.task_id
        || task.status != TaskStatus::Active
        || a.id != expected.id
        || a.generation != expected.generation
        || a.worker_id != expected.worker_id
        || a.worker_incarnation != expected.worker_incarnation
        || a.deadline_unix_ms != expected.deadline_unix_ms
        || a.stopped
        || a.uncertain
        || a.phase == AttemptPhase::StopRequested
        || !enabled(&task.input)
    {
        return Err("native_task_tool_owner_changed".into());
    }
    let observed = reply
        .observed_controller_revision
        .ok_or("native_task_tool_read_unbound")?;
    if observed == 0 {
        return Err("native_task_tool_read_unbound".into());
    }
    Ok(CurrentTaskProjection {
        task_id: task.id.clone(),
        agent_id: task.assigned_agent_id.clone(),
        definition: task.input.definition.clone(),
        status: task.status,
        observed_controller_revision: observed,
        deadline_unix_ms: a.deadline_unix_ms,
        attempt_id: a.id.clone(),
        generation: a.generation,
    })
}

pub fn matches_task(observation: &ToolObservation, task: &Task) -> bool {
    let Some(a) = &task.attempt else {
        return false;
    };
    let Ok(limits) = task.input.execution_limits() else {
        return false;
    };
    let p = &observation.task;
    identity(&observation.call_id, &limits.native)
        && p.task_id == task.id
        && p.agent_id == task.assigned_agent_id
        && p.definition == task.input.definition
        && p.status == TaskStatus::Active
        && p.observed_controller_revision > 0
        && p.observed_controller_revision <= i64::MAX as u64
        && p.deadline_unix_ms == task.input.limits.deadline_unix_ms
        && p.attempt_id == a.id
        && p.generation == a.generation
}
pub fn encode(
    observation: &ToolObservation,
    output_limit: usize,
    limits: &NativeLimits,
) -> Result<String, String> {
    bounded_json_size(
        observation,
        output_limit.min(limits.task_tool_observation_bytes),
    )?;
    if !identity(&observation.call_id, limits) {
        return Err("native_tool_call_identity_invalid".into());
    }
    serde_json::to_string(observation).map_err(|_| "native_tool_observation_encoding".into())
}
pub fn decode(
    encoded: &str,
    output_limit: usize,
    limits: &NativeLimits,
) -> Result<ToolObservation, String> {
    if encoded.len() > output_limit.min(limits.task_tool_observation_bytes) {
        return Err("native_tool_observation_bound".into());
    }
    let value: ToolObservation =
        serde_json::from_str(encoded).map_err(|_| "native_tool_observation_shape")?;
    if encode(&value, output_limit, limits)? != encoded {
        return Err("native_tool_observation_not_canonical".into());
    }
    Ok(value)
}

#[cfg(test)]
mod metadata_tests {
    use super::*;

    #[test]
    fn observation_codec_uses_explicit_identity_and_encoded_byte_bounds() {
        let mut limits = crate::limits::RuntimeLimits::seed()
            .unwrap()
            .execution
            .native;
        let observation = ToolObservation {
            call_id: "c".repeat(300),
            task: CurrentTaskProjection {
                task_id: "task".into(),
                agent_id: "agent".into(),
                definition: Definition {
                    charter: None,
                    id: "definition".into(),
                    revision: "r1".into(),
                    executor: ExecutorKind::Native,
                    inference: None,
                    execution_profile: None,
                    limit_overrides: None,
                },
                status: TaskStatus::Active,
                observed_controller_revision: 1,
                deadline_unix_ms: 1,
                attempt_id: "attempt".into(),
                generation: 1,
            },
        };
        assert!(encode(&observation, limits.task_tool_observation_bytes, &limits).is_err());
        limits.task_tool_identity_bytes = observation.call_id.len();
        let encoded = encode(&observation, limits.task_tool_observation_bytes, &limits).unwrap();
        limits.task_tool_observation_bytes = encoded.len();
        assert_eq!(
            decode(&encoded, encoded.len(), &limits).unwrap(),
            observation
        );
        limits.task_tool_observation_bytes -= 1;
        assert!(decode(&encoded, encoded.len(), &limits).is_err());
        assert!(encode(&observation, encoded.len(), &limits).is_err());
    }
}
