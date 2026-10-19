//! Private exact-source ticket. Neither MCP arguments nor stored view DTOs can
//! construct a completion proof. The caller check precedes this both times.
use super::{
    database::{self, Fault, StoredTask},
    validation::Context,
};
use crate::{
    protocol::*,
    repository_read::{self, Query, Source},
    repository_view,
};
use std::{sync::Arc, time::Duration};

pub(super) use crate::repository_view::SourceSelector as Selector;
#[derive(Debug, Clone, PartialEq)]
struct Binding {
    input: Box<TaskInput>,
    assigned_agent: String,
    // Including the exact full checkpoint prevents a newer publication with
    // equal commit bytes from being mistaken for the original selected source.
    checkpoint: Option<Checkpoint>,
    source: Source,
}
#[derive(Debug)]
pub(super) struct Ticket {
    issuer: Arc<()>,
    request: String,
    binding: Binding,
    query: Query,
    prepared_at: u64,
    deadline: tokio::time::Instant,
    operator_observation: bool,
    limits: crate::limits::RepositoryLimits,
}
enum Completion {
    Capacity,
    Finished(Result<repository_view::Output, repository_read::Error>),
}
pub(super) struct Proof {
    // Preserve the allocation supplied by the controller's preflight boundary.
    ticket: Box<Ticket>,
    completion: Completion,
}

// Traceability scope: Shared repository calls resolve only accepted registered sources or the acknowledged checkpoint; caller fields cannot select arbitrary material.
//= spec/impl/v2/implementation.md#repository-reads-through-the-shared-tool-interface
//# The shared agent interface MUST provide read-only repository access within the task's accepted repository scope.
fn resolve(
    tx: &database::StoreContext<'_>,
    task: &Task,
    selector: Selector,
) -> database::Result<(Source, Option<Checkpoint>)> {
    let (binding, identity, commit, checkpoint, is_checkpoint) =
        match (task.input.definition.executor, selector) {
            (ExecutorKind::Coding, Selector::Instructions) => {
                if !crate::execution_profile::collaboration(&task.input) {
                    return database::refuse("repository_read_unavailable", "instruction source requires the accepted collaboration profile");
                }
                let source = task.input.instruction_source.as_ref().ok_or_else(|| {
                    Fault::Refused("repository_read_unavailable", "task has no accepted instruction source".into())
                })?;
                (source.registration.as_ref(), source.repository.clone(), source.start_commit.clone(), None, false)
            }
            (ExecutorKind::Native, Selector::Start) => {
                let source = task.input.repository_read.as_ref().ok_or_else(|| {
                    Fault::Refused(
                        "repository_read_unavailable",
                        "task has no accepted read association".into(),
                    )
                })?;
                (
                    source.registration.as_ref(),
                    source.repository.clone(),
                    source.start_commit.clone(),
                    None,
                    false,
                )
            }
            (ExecutorKind::Coding, Selector::Start) => {
                let source = task.input.repository.as_ref().ok_or_else(|| {
                    Fault::Refused(
                        "repository_read_unavailable",
                        "task has no accepted repository".into(),
                    )
                })?;
                (
                    source.registration.as_ref(),
                    source.url.clone(),
                    source.start_commit.clone(),
                    None,
                    false,
                )
            }
            (ExecutorKind::Coding, Selector::Checkpoint) => {
                let cp = task.selected_checkpoint.as_ref().ok_or_else(|| {
                    Fault::Refused(
                        "repository_read_unavailable",
                        "task has no acknowledged checkpoint".into(),
                    )
                })?;
                let repo = task
                    .input
                    .repository
                    .as_ref()
                    .ok_or_else(|| Fault::Store("coding source absent".into()))?;
                if cp.repository != repo.checkpoint_remote
                    || Some(&cp.registration) != repo.registration.as_ref()
                {
                    return database::refuse(
                        "repository_read_unavailable",
                        "checkpoint differs from accepted publication source",
                    );
                }
                (
                    repo.registration.as_ref(),
                    cp.repository.clone(),
                    cp.commit.clone(),
                    Some(cp.clone()),
                    true,
                )
            }
            _ => {
                return database::refuse(
                    "repository_read_unavailable",
                    "task has no acknowledged checkpoint source",
                );
            }
        };
    let binding =
        binding.ok_or_else(|| Fault::Store("accepted repository registration absent".into()))?;
    let accepted_policy = task
        .input
        .resolved_limits
        .as_ref()
        .ok_or_else(|| Fault::Store("accepted repository policy absent".into()))?
        .global_metadata
        .as_ref();
    let accepted_context = tx.with_limits(accepted_policy);
    let record = super::repositories::revision(&accepted_context, binding)?;
    let (endpoint, store_key) = if is_checkpoint {
        (record.checkpoint_endpoint, record.checkpoint_store_key)
    } else {
        (record.source_endpoint, record.source_store_key)
    };
    if endpoint != identity {
        return Err(Fault::Store(
            "accepted repository endpoint differs from immutable registration".into(),
        ));
    }
    Ok((
        Source {
            registration: binding.clone(),
            store_key,
            repository: identity,
            commit,
            base_commit: None,
        },
        checkpoint,
    ))
}
fn binding(
    tx: &database::StoreContext<'_>,
    task: &Task,
    selected: Selector,
    base: Option<Selector>,
) -> database::Result<Binding> {
    let (mut source, mut checkpoint) = resolve(tx, task, selected)?;
    if let Some(base) = base {
        let (other, cp) = resolve(tx, task, base)?;
        if other.repository != source.repository
            || other.store_key != source.store_key
            || other.registration != source.registration
        {
            return database::refuse("repository_read_invalid", "diff source identities differ");
        }
        if cp.is_some() {
            checkpoint = cp;
        }
        source.base_commit = Some(other.commit);
    }
    Ok(Binding {
        input: task.input.clone(),
        assigned_agent: task.assigned_agent_id.clone(),
        checkpoint,
        source,
    })
}
pub(super) fn observe(
    tx: &database::StoreContext<'_>,
    context: &Context<'_>,
    original: &StoredTask,
    request: &crate::repository_view::Request,
    now: u64,
) -> database::Result<repository_view::Output> {
    if matches!(request,repository_view::Request::ReadPage {..}) {
        return super::read_pages::repository_observe(tx,context,original,request,now);
    }
    let (selected, base, query) = request.parts();
    require_inner(tx, context, original, selected, base, query, now, true)
}
pub(super) fn observe_slice(tx:&database::StoreContext<'_>,context:&Context<'_>,original:&StoredTask,selected:Selector,query:Query,now:u64)->database::Result<repository_view::Output> {
    require_inner(tx,context,original,selected,None,query,now,true)
}
pub(super) fn require(
    tx: &database::StoreContext<'_>,
    context: &Context<'_>,
    original: &StoredTask,
    selected: Selector,
    base: Option<Selector>,
    query: Query,
    now: u64,
) -> database::Result<repository_view::Output> {
    require_inner(tx, context, original, selected, base, query, now, false)
}
fn require_inner(
    tx: &database::StoreContext<'_>,
    context: &Context<'_>,
    original: &StoredTask,
    selected: Selector,
    base: Option<Selector>,
    query: Query,
    now: u64,
    operator_observation: bool,
) -> database::Result<repository_view::Output> {
    query
        .validate(&context.limits.repository)
        .map_err(|e| Fault::Refused(e.code(), e.message("repository query refused")))?;
    let bound = binding(tx, &original.task, selected, base)?;
    require_bound(context, original, bound, query, now, operator_observation)
}

/// Original recovery selects the retained resolved source, never today's head
/// or checkpoint. The validated release provenance supplies the original input.
pub(super) fn recover_original(
    tx: &database::StoreContext<'_>, context: &Context<'_>, original: &StoredTask,
    source: &repository_view::Source, query: Query, now: u64,
) -> database::Result<repository_view::Output> {
    query.validate(&context.limits.repository)
        .map_err(|e| Fault::Refused(e.code(), e.message("original repository query refused")))?;
    let accepted = original.task.input.resolved_limits.as_ref()
        .ok_or_else(|| Fault::Store("original repository policy absent".into()))?;
    let accepted_tx = tx.with_limits(&accepted.global_metadata);
    let record = super::repositories::revision(&accepted_tx, &source.registration)?;
    let store_key = if record.source_endpoint == source.repository { record.source_store_key }
        else if record.checkpoint_endpoint == source.repository { record.checkpoint_store_key }
        else { return database::refuse("original_read_body_unavailable", "recorded immutable repository is unavailable"); };
    let bound = Binding { input:original.task.input.clone(), assigned_agent:original.task.assigned_agent_id.clone(),
        checkpoint:None, source:Source { registration:source.registration.clone(), store_key,
            repository:source.repository.clone(), commit:source.commit.clone(), base_commit:source.base_commit.clone() } };
    require_bound(context, original, bound, query, now, true)
}

fn require_bound(
    context: &Context<'_>, original: &StoredTask, bound: Binding,
    query: Query, now: u64, operator_observation: bool,
) -> database::Result<repository_view::Output> {
    let Some(proof) = context.repository else {
        let remaining = if operator_observation {
            Duration::from_millis(context.limits.repository.read_timeout_ms)
        } else {
            let attempt = original
                .task
                .attempt
                .as_ref()
                .ok_or_else(|| Fault::Store("repository caller lacks attempt".into()))?;
            let deadline_ms = attempt
                .deadline_unix_ms
                .min(original.task.input.limits.deadline_unix_ms);
            Duration::from_millis(deadline_ms.saturating_sub(now)).min(Duration::from_millis(
                context.limits.repository.read_timeout_ms,
            ))
        };
        return Err(Fault::Repository(Box::new(Ticket {
            issuer: context.issuer.clone(),
            request: context.request.into(),
            binding: bound,
            query,
            prepared_at: now,
            deadline: tokio::time::Instant::now() + remaining,
            operator_observation,
            limits: context.limits.repository.clone(),
        })));
    };
    let ticket = &proof.ticket;
    if !Arc::ptr_eq(context.issuer, &ticket.issuer)
        || context.request != ticket.request
        || ticket.binding != bound
        || ticket.query != query
        || ticket.operator_observation != operator_observation
        || ticket.limits != context.limits.repository
    {
        return database::refuse(
            "repository_source_changed",
            "repository read proof differs from current operation or source",
        );
    }
    if now < ticket.prepared_at {
        return database::refuse(
            "clock_unavailable",
            "clock moved backwards during repository read",
        );
    }
    if tokio::time::Instant::now() >= ticket.deadline {
        return database::refuse(
            "repository_read_deadline",
            "repository read deadline exceeded",
        );
    }
    match &proof.completion {
        Completion::Capacity => database::refuse(
            "capacity",
            "controller read capacity exhausted: controller.validation_parallelism",
        ),
        Completion::Finished(Err(error)) => database::refuse(
            error.code(),
            error.message(
                if matches!(
                    error,
                    repository_read::Error::Unavailable
                        | repository_read::Error::UnavailableLimit(_)
                ) {
                    "no readable local material for the exact accepted repository and commit"
                } else {
                    "repository read refused"
                },
            ),
        ),
        Completion::Finished(Ok(output)) => Ok(output.clone()),
    }
}
impl Ticket {
    pub fn no_capacity(self: Box<Self>) -> Proof {
        Proof {
            ticket: self,
            completion: Completion::Capacity,
        }
    }
    pub async fn execute(
        self: Box<Self>,
        config: &repository_read::Config,
        clock: &Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Proof {
        // Neither queue delay nor validation time can extend accepted duration.
        let now = clock();
        let result = if now < self.prepared_at
            || (!self.operator_observation && now >= self.binding.input.limits.deadline_unix_ms)
        {
            Err(repository_read::Error::Deadline)
        } else {
            repository_read::read(
                config,
                &self.limits,
                &self.binding.source,
                &self.query,
                self.deadline,
            )
            .await
            .map(Into::into)
        };
        Proof {
            ticket: self,
            completion: Completion::Finished(result),
        }
    }
}
