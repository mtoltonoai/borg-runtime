//! Public supplied-executor occurrences, never a count of hidden provider POSTs.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    pub initial_backoff_ms: u64,
    pub backoff_multiplier: u32,
    pub max_backoff_ms: u64,
    pub jitter_ms: u64,
}
impl RetryPolicy {
    pub fn validate(&self) -> Result<(), String> {
        if self.initial_backoff_ms == 0
            || self.backoff_multiplier == 0
            || self.max_backoff_ms < self.initial_backoff_ms
            || self.jitter_ms == 0
            || self.max_backoff_ms.checked_add(self.jitter_ms).is_none()
            || std::time::Instant::now()
                .checked_add(std::time::Duration::from_millis(
                    self.max_backoff_ms.saturating_add(self.jitter_ms),
                ))
                .is_none()
        {
            return Err("coding.rate_limit_retry is outside checked clock bounds".into());
        }
        Ok(())
    }
    pub fn delay_ms(
        &self,
        index: u32,
        sample: u64,
        now: u64,
        deadline: u64,
    ) -> Result<u64, String> {
        self.validate()?;
        let exponent = index
            .checked_sub(1)
            .ok_or("supplied retry index must be positive")?;
        let delay = self
            .initial_backoff_ms
            .saturating_mul(u64::from(self.backoff_multiplier).saturating_pow(exponent))
            .min(self.max_backoff_ms)
            .checked_add(sample % (self.jitter_ms + 1))
            .ok_or("supplied retry delay overflow")?;
        if now.checked_add(delay).is_none_or(|at| at >= deadline) {
            return Err("supplied retry reaches original deadline".into());
        }
        Ok(delay)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Cause {
    RateLimit { retry_index: u32 },
    Completion { begin_operation_id: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum Observation {
    Acknowledged {
        index: u32,
        thread_id: String,
        turn_id: String,
    },
    Terminal {
        index: u32,
        thread_id: String,
        turn_id: String,
        terminal: serde_json::Value,
    },
    Waiting {
        index: u32,
        thread_id: String,
        previous_turn_id: String,
        cause: Cause,
    },
    Dispatch {
        index: u32,
        thread_id: String,
        previous_turn_id: String,
        cause: Cause,
    },
}
impl Observation {
    pub fn index(&self) -> u32 {
        match self {
            Self::Acknowledged { index, .. }
            | Self::Terminal { index, .. }
            | Self::Waiting { index, .. }
            | Self::Dispatch { index, .. } => *index,
        }
    }
    pub fn phase(&self) -> &'static str {
        match self {
            Self::Acknowledged { .. } => "ack",
            Self::Terminal { .. } => "terminal",
            Self::Waiting { .. } => "wait",
            Self::Dispatch { .. } => "dispatch",
        }
    }
    pub fn thread(&self) -> &str {
        match self {
            Self::Acknowledged { thread_id, .. }
            | Self::Terminal { thread_id, .. }
            | Self::Waiting { thread_id, .. }
            | Self::Dispatch { thread_id, .. } => thread_id,
        }
    }
    pub fn turn(&self) -> &str {
        match self {
            Self::Acknowledged { turn_id, .. } | Self::Terminal { turn_id, .. } => turn_id,
            Self::Waiting {
                previous_turn_id, ..
            }
            | Self::Dispatch {
                previous_turn_id, ..
            } => previous_turn_id,
        }
    }
}
pub fn id(attempt: &str, index: u32, phase: &str) -> String {
    format!("{attempt}-supplied-outer-{index}-{phase}")
}
/// Only the adapter's typed public terminal class is control input. A message's
/// prose, retry duration, or a nonterminal willRetry notification is not.
pub fn rate_limited(terminal: &serde_json::Value) -> bool {
    terminal["status"] == "failed" && terminal["error"]["codexErrorInfo"] == "rateLimitExceeded"
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Wait {
    pub index: u32,
    pub rate_failures: u32,
    pub admitted_at_ms: u64,
    pub not_before_ms: u64,
}
pub fn jitter_sample(thread: &str, index: u32) -> u64 {
    let mut bytes = thread.as_bytes().to_vec();
    bytes.extend_from_slice(&index.to_le_bytes());
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &bytes);
    let mut sample = [0u8; 8];
    sample.copy_from_slice(&digest.as_ref()[..8]);
    u64::from_le_bytes(sample)
}

pub fn definition_uses(d: &crate::protocol::Definition) -> bool {
    d.limit_overrides
        .as_ref()
        .and_then(|v| v.coding.as_ref())
        .is_some_and(|v| v.rate_limit_retry.is_some())
}
pub fn input_uses(input: &crate::protocol::TaskInput) -> bool {
    use crate::protocol::NativeWorkflow;
    definition_uses(&input.definition)
        || input.resolved_limits.as_ref().is_some_and(|s| {
            s.values.coding.rate_limit_retry.is_some()
                || s.global_metadata
                    .execution
                    .coding
                    .rate_limit_retry
                    .is_some()
        })
        || match input.native_workflow.as_deref() {
            Some(NativeWorkflow::CodingThenContinue { child }) => {
                definition_uses(&child.definition)
            }
            Some(NativeWorkflow::Reassign { target }) => definition_uses(&target.definition),
            _ => false,
        }
}
pub fn command_uses(c: &crate::protocol::WorkerCommand) -> bool {
    matches!(c,crate::protocol::WorkerCommand::RunAttempt{task,..} if input_uses(task))
}
pub fn operation_uses(op: &crate::protocol::Operation) -> bool {
    use crate::protocol::Operation;
    match op {
        Operation::ObserveWorker { report } => matches!(
            report.observation,
            crate::protocol::Observation::SuppliedOuter { .. }
        ),
        Operation::RegisterAgent { definition, .. } => definition_uses(definition),
        Operation::SubmitTask { input, .. } | Operation::DelegateTask { child: input, .. } => {
            input_uses(input)
        }
        Operation::SubmitTaskDraft { input, .. } => {
            input.definition.as_ref().is_some_and(definition_uses)
                || match input.native_workflow.as_deref() {
                    Some(crate::protocol::NativeWorkflow::CodingThenContinue { child }) => {
                        definition_uses(&child.definition)
                    }
                    Some(crate::protocol::NativeWorkflow::Reassign { target }) => {
                        definition_uses(&target.definition)
                    }
                    _ => false,
                }
        }
        _ => false,
    }
}
pub fn task_uses(task: &crate::protocol::Task) -> bool {
    input_uses(&task.input)
        || task
            .pending_assignment
            .as_ref()
            .is_some_and(|p| definition_uses(&p.session.definition))
}
pub fn outcome_uses(outcome: &crate::protocol::Outcome) -> bool {
    use crate::protocol::{Data, Outcome};
    let Outcome::Ok { data } = outcome else {
        return false;
    };
    match data {
        Data::SuppliedOuterWait(_) => true,
        Data::Task(t) => task_uses(t),
        Data::Tasks(ts) => ts.iter().any(task_uses),
        Data::Commands(cs) => cs.iter().any(command_uses),
        Data::Agent(a) => definition_uses(&a.definition),
        Data::Agents(xs) => xs.iter().any(|a| definition_uses(&a.definition)),
        Data::DefinitionUpdate(v) => definition_uses(&v.selected_definition),
        Data::WorkMutation(v) => task_uses(&v.view.task),
        Data::WorkQuery(crate::collaboration::work::WorkQueryResult::Task(v)) => task_uses(&v.task),
        Data::Collaboration(v) => match v {
            crate::collaboration::QueryResult::Call(c) => outcome_uses(&c.result.reply.outcome),
            crate::collaboration::QueryResult::Definition(d) => definition_uses(d),
            crate::collaboration::QueryResult::Session(s) => definition_uses(&s.definition),
            crate::collaboration::QueryResult::Sessions(xs) => {
                xs.items.iter().any(|s| definition_uses(&s.definition))
            }
            crate::collaboration::QueryResult::SessionInput { input, .. } => input_uses(input),
            crate::collaboration::QueryResult::Limits { global, execution } => {
                global.execution.coding.rate_limit_retry.is_some()
                    || execution.as_ref().is_some_and(|s| {
                        s.values.coding.rate_limit_retry.is_some()
                            || s.global_metadata
                                .execution
                                .coding
                                .rate_limit_retry
                                .is_some()
                    })
            }
            _ => false,
        },
        Data::Charter(value) => match value.as_ref() {
            crate::charter::CharterOutcome::Definition(v) => definition_uses(&v.definition),
            _ => false,
        },
        _ => false,
    }
}
