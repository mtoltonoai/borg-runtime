//! A private, one-use bridge between an exact DB preflight and the genuine
//! bounded native validator. The ticket is data; all mutable permission/state
//! conditions are checked again in the final transaction.
use super::database::{self, Fault, StoredTask};
use crate::{
    native::{JsonValidationError, ValidatedJson},
    protocol::*,
};
use serde_json::Value;
use std::{sync::Arc, time::Duration};

#[derive(Debug, Clone, PartialEq)]
struct Owner {
    task_id: String,
    input: TaskInput,
    assigned_agent: String,
    attempt_id: String,
    generation: u64,
    worker: String,
    incarnation: String,
}
impl Owner {
    fn read(original: &StoredTask) -> database::Result<Self> {
        let attempt = original
            .task
            .attempt
            .as_ref()
            .ok_or_else(|| Fault::Store("validation owner has no attempt".into()))?;
        Ok(Self {
            task_id: original.task.id.clone(),
            input: original.task.input.clone(),
            assigned_agent: original.task.assigned_agent_id.clone(),
            attempt_id: attempt.id.clone(),
            generation: attempt.generation,
            worker: attempt.worker_id.clone(),
            incarnation: attempt.worker_incarnation.clone(),
        })
    }
}
#[derive(Debug)]
pub(super) struct Ticket {
    issuer: Arc<()>,
    request: String,
    input: TaskInput,
    output: Option<Value>,
    owner: Option<Owner>,
    prepared_at: u64,
    validation_timeout_ms: u64,
}
enum Completion {
    Finished(Result<ValidatedJson, JsonValidationError>),
    Capacity,
}
pub(super) struct Proof {
    ticket: Ticket,
    completion: Completion,
}
pub(super) struct Context<'a> {
    pub limits: &'a crate::limits::RuntimeLimits,
    pub issuer: &'a Arc<()>,
    pub request: &'a str,
    pub proof: Option<&'a Proof>,
    pub repository: Option<&'a super::repository_queries::Proof>,
}
impl Context<'_> {
    pub fn require(
        &self,
        input: &TaskInput,
        output: Option<&Value>,
        owner: Option<&StoredTask>,
        now: u64,
    ) -> database::Result<()> {
        let owner = owner.map(Owner::read).transpose()?;
        let Some(proof) = self.proof else {
            return Err(Fault::Validation(Box::new(Ticket {
                issuer: self.issuer.clone(),
                request: self.request.to_owned(),
                input: input.clone(),
                output: output.cloned(),
                owner,
                prepared_at: now,
                validation_timeout_ms: self.limits.controller.validation_timeout_ms,
            })));
        };
        let ticket = &proof.ticket;
        if !Arc::ptr_eq(self.issuer, &ticket.issuer)
            || self.request != ticket.request
            || input != &ticket.input
            || output != ticket.output.as_ref()
            || owner != ticket.owner
        {
            return database::refuse(
                "invalid_validation",
                "validation does not bind this exact operation and owner",
            );
        }
        if now < ticket.prepared_at {
            return database::refuse(
                "clock_unavailable",
                "clock moved backwards during native validation",
            );
        }
        match &proof.completion {
            Completion::Capacity => Err(Fault::TemporaryValidation(
                "capacity",
                "controller native validation capacity exhausted: controller.validation_parallelism",
            )),
            Completion::Finished(Ok(validated)) => {
                let schema = input
                    .output_schema
                    .as_ref()
                    .ok_or_else(|| Fault::Store("native schema missing".into()))?;
                if validated.matches(schema, output, input.limits.output_bytes) {
                    Ok(())
                } else {
                    database::refuse(
                        "invalid_validation",
                        "native validation token does not match the final inputs",
                    )
                }
            }
            Completion::Finished(Err(JsonValidationError::Refused(reason))) => database::refuse(
                if output.is_some() {
                    "schema_validation"
                } else {
                    "invalid_input"
                },
                format!("native schema or result refused by bounded validation: {reason}"),
            ),
            Completion::Finished(Err(JsonValidationError::Deadline)) => {
                let field = if ticket.input.limits.deadline_unix_ms.saturating_sub(ticket.prepared_at) <= ticket.validation_timeout_ms {
                    "task.input.limits.deadline_unix_ms"
                } else { "controller.validation_timeout_ms" };
                database::refuse("deadline", format!("native validation deadline exceeded: {field}"))
            }
            Completion::Finished(Err(JsonValidationError::Helper(_))) => {
                Err(Fault::TemporaryValidation(
                    "validation_unavailable",
                    "native validation helper failed",
                ))
            }
            Completion::Finished(Err(JsonValidationError::CleanupUnverified)) => {
                Err(Fault::TemporaryValidation(
                    "validation_cleanup_unverified",
                    "native validation helper cleanup remains unverified",
                ))
            }
        }
    }
}
impl Ticket {
    pub fn no_capacity(self) -> Proof {
        Proof {
            ticket: self,
            completion: Completion::Capacity,
        }
    }
    pub async fn execute(self, host: &crate::native::ValidationHost) -> Proof {
        let duration = Duration::from_millis(
            self.input
                .limits
                .deadline_unix_ms
                .saturating_sub(self.prepared_at),
        )
        .min(Duration::from_millis(self.validation_timeout_ms));
        let deadline = tokio::time::Instant::now() + duration;
        let native = match self.input.execution_limits() {
            Ok(v) => v.native.clone(),
            Err(reason) => return Proof { ticket: self, completion: Completion::Finished(Err(JsonValidationError::Helper(reason))) },
        };
        let host = host.clone().with_limits(Arc::new(native));
        let result = match self.input.output_schema.as_ref() {
            Some(schema) => {
                crate::native::validate_json_with_host(
                    schema,
                    self.output.as_ref(),
                    self.input.limits.output_bytes,
                    deadline,
                    &host,
                )
                .await
            }
            None => Err(JsonValidationError::Helper("missing native schema".into())),
        };
        Proof {
            ticket: self,
            completion: Completion::Finished(result),
        }
    }
}
