//! Independent task creation uses ordinary admission and the original receipt.
//! No delegated wait, copied endpoint exception, second task store or inner API call.
use super::{ControllerConfig, database::*, validation::Context};
use crate::{mcp::Invocation, protocol::*, repository_read::Source};
use rusqlite::{OptionalExtension, params};

#[derive(Debug, Clone, PartialEq)]
pub(super) struct Admission {
    pub input: TaskInput,
    pub sources: Vec<Source>,
    pub provenance: TaskCreationProvenance,
}
fn route(tx: &StoreContext<'_>, args: &TaskCreate) -> Result<(Definition, Option<String>)> {
    for value in [&args.agent_id, &args.expected_agent_revision]
        .into_iter()
        .flatten()
    {
        if !valid_id(tx.limits, value) {
            return refuse(
                "invalid_input",
                "invalid explicit agent identity or revision",
            );
        }
    }
    let default = if args.agent_id.is_none() {
        Some(tx.limits.task_defaults.agent_id.clone().ok_or_else(|| {
            Fault::Refused(
                "default_agent_not_configured",
                "task_defaults.agent_id is absent".into(),
            )
        })?)
    } else {
        None
    };
    let id = args
        .agent_id
        .as_ref()
        .or(default.as_ref())
        .ok_or_else(|| Fault::Store("task routing absent".into()))?;
    let definition = super::agents::selected_active_definition(tx, id)?;
    if args
        .expected_agent_revision
        .as_ref()
        .is_some_and(|r| *r != definition.revision)
    {
        return refuse(
            "configuration_conflict",
            "expected agent selected revision differs",
        );
    }
    Ok((definition, default))
}
fn build(
    args: &TaskCreate,
    definition: Definition,
    now: u64,
    limits: &crate::limits::RuntimeLimits,
) -> Result<TaskInput> {
    let mut workload = limits
        .task_defaults
        .at(now)
        .map_err(|e| Fault::Refused("invalid_input", e))?;
    if let Some(w) = &args.workload {
        if let Some(v) = w.deadline_ms {
            if v == 0 {
                return refuse("invalid_input", "workload.deadline_ms must be positive");
            }
            workload.deadline_unix_ms = now.checked_add(v).ok_or_else(|| {
                Fault::Refused(
                    "invalid_input",
                    "workload.deadline_ms overflows timestamp".into(),
                )
            })?;
        }
        if let Some(v) = w.model_requests {
            workload.model_requests = v
        }
        if let Some(v) = w.output_bytes {
            workload.output_bytes = v
        }
        if let Some(v) = w.result_bytes {
            workload.result_bytes = v
        }
        if let Some(v) = w.max_delegation_depth {
            workload.max_delegation_depth = v
        }
    }
    let read = |s: &TaskRepositorySelection| RepositoryReadSource {
        registration: Some(s.registration.clone()),
        repository: String::new(),
        start_commit: s.start_commit.clone(),
    };
    Ok(TaskInput {
        instruction: args.instruction.clone(),
        definition,
        repository: args.repository.as_ref().map(|s| Repository {
            registration: Some(s.registration.clone()),
            url: String::new(),
            checkpoint_remote: String::new(),
            start_commit: s.start_commit.clone(),
            pull_request_target: None,
        }),
        repository_read: args.repository_read.as_ref().map(read),
        instruction_source: args.instruction_source.as_ref().map(read),
        output_schema: args.output_schema.clone(),
        acceptance_commands: args.acceptance_commands.clone(),
        dependencies: args.dependencies.clone(),
        limits: workload,
        resolved_limits: None,
        pull_request: None,
        native_workflow: None,
    })
}
fn sources(tx: &StoreContext<'_>, input: &TaskInput) -> Result<Vec<Source>> {
    let mut result = vec![];
    let refs = input
        .repository
        .iter()
        .map(|r| (r.registration.as_ref(), &r.start_commit))
        .chain(
            input
                .repository_read
                .iter()
                .map(|r| (r.registration.as_ref(), &r.start_commit)),
        )
        .chain(
            input
                .instruction_source
                .iter()
                .map(|r| (r.registration.as_ref(), &r.start_commit)),
        );
    for (reference, commit) in refs {
        let reference = reference.ok_or_else(|| {
            Fault::Refused(
                "repository_binding_required",
                "explicit registration required".into(),
            )
        })?;
        let r = super::repositories::revision(tx, reference)?;
        if !super::operations::commit_id(commit) {
            return refuse("invalid_input", "exact source commit required");
        }
        result.push(Source {
            registration: reference.clone(),
            store_key: r.source_store_key,
            repository: r.source_endpoint,
            commit: commit.clone(),
            base_commit: None,
        });
    }
    Ok(result)
}
pub(super) fn apply(
    tx: &StoreContext<'_>,
    call: &Invocation,
    args: &TaskCreate,
    cfg: &ControllerConfig,
    now: u64,
    validation: &Context<'_>,
) -> Result<(Data, bool)> {
    let caller = super::collaboration_calls::caller(tx, &call.caller, now)?;
    let future = tx.with_limits(&cfg.limits);
    let policy = cfg
        .limits
        .task_creation
        .as_ref()
        .filter(|p| p.enabled)
        .ok_or_else(|| {
            Fault::Refused(
                "configuration_disabled",
                "task_creation is disabled or missing".into(),
            )
        })?;
    let (definition, default) = route(&future, args)?;
    let governance = super::governance::selection(&future)?.revision;
    let frozen = validation.creation_admission();
    let accepted_at = frozen
        .map(|a| a.provenance.accepted_at_unix_ms)
        .unwrap_or(now);
    let input = build(args, definition, accepted_at, &cfg.limits)?;
    let mut resolved = input.clone();
    // Fresh source resolution; no accepted-child endpoint bypass for new roots.
    super::repositories::resolve_input(&future, &mut resolved, false)?;
    let provenance = TaskCreationProvenance {
        operation_id: call.operation_id.clone(),
        logical_call_id: call.logical_call_id.clone(),
        creator: super::identity_administration::bound_actor(&caller.task, &call.caller),
        policy: policy.clone(),
        global_revision: cfg.limits.revision.clone(),
        governance_revision: governance,
        default_agent_id: default,
        accepted_at_unix_ms: accepted_at,
    };
    let admission = Admission {
        sources: sources(&future, &resolved)?,
        input,
        provenance,
    };
    if frozen.is_some_and(|old| old != &admission) {
        return refuse(
            "configuration_conflict",
            "frozen task creation target/default/policy/source changed during validation",
        );
    }
    let context = Context {
        limits: &cfg.limits,
        issuer: validation.issuer,
        request: validation.request,
        proof: validation.proof,
        repository: validation.repository,
        creation: Some(&admission),
    };
    let t = super::operations::submit(&future, &admission.input, cfg, now, None, 0, &context)?;
    super::collaboration_calls::caller(tx, &call.caller, (cfg.clock)())?;
    history(&future, &t.task, "created_by", &admission.provenance)?;
    Ok((Data::Task(Box::new(t.task)), true))
}
pub(super) fn validate_original(
    tx: &StoreContext<'_>,
    r: &super::metadata::Recorded,
) -> Result<()> {
    let Operation::CallCollaboration { call } = &r.request else {
        return Ok(());
    };
    let Outcome::Ok {
        data: Data::Task(task),
    } = &r.outcome
    else {
        return Ok(());
    };
    let args: TaskCreate = serde_json::from_value(call.arguments.clone())?;
    let raw: Option<String> = tx
        .query_row(
            "SELECT body FROM history WHERE task_id=?1 AND kind='created_by'",
            [&task.id],
            |r| r.get(0),
        )
        .optional()?;
    let provenance: TaskCreationProvenance =
        decode(&raw.ok_or_else(|| Fault::Store("created task provenance missing".into()))?)?;
    let crate::collaboration::workflow::ActorProvenance::BoundAgentSession { session_id, .. } =
        &provenance.creator.provenance
    else {
        return Err(Fault::Store("created task has no bound creator".into()));
    };
    let caller = super::sessions::historical_task(tx, &call.caller.task_id, session_id)?;
    super::collaboration_workflow::validate_recorded_actor(tx, &provenance.creator)?;
    if provenance.creator != super::identity_administration::bound_actor(&caller, &call.caller)
        || provenance.operation_id != call.operation_id
        || provenance.logical_call_id != call.logical_call_id
        || task.parent_task_id.is_some()
        || task.id == caller.id
        || task.session_id == caller.session_id
    {
        return Err(Fault::Store(
            "created task original creator association differs".into(),
        ));
    }
    let input = super::sessions::initial_input(tx, &task.id)?;
    let global = &input
        .resolved_limits
        .as_ref()
        .ok_or_else(|| Fault::Store("created task snapshot absent".into()))?
        .global_metadata;
    if global.revision != provenance.global_revision
        || global.task_creation.as_ref() != Some(&provenance.policy)
    {
        return Err(Fault::Store("created task metadata differs".into()));
    }
    let mut expected = build(
        &args,
        input.definition.clone(),
        provenance.accepted_at_unix_ms,
        global,
    )?;
    // Historical source validation compares retained exact registrations, never
    // today's enabled/head selection and never acquiring material on reopen.
    if let Some(r) = &mut expected.repository {
        let reg = super::repositories::revision(tx, r.registration.as_ref().unwrap())?;
        r.url = reg.source_endpoint;
        r.checkpoint_remote = reg.checkpoint_endpoint;
        r.pull_request_target = reg.pull_request_target;
    }
    for r in [
        &mut expected.repository_read,
        &mut expected.instruction_source,
    ]
    .into_iter()
    .flatten()
    {
        r.repository =
            super::repositories::revision(tx, r.registration.as_ref().unwrap())?.source_endpoint;
    }
    expected.resolved_limits = Some(global.resolve_task(&expected).map_err(Fault::Store)?);
    if expected != input || crate::pull_request_view::input(&input) != task.input {
        return Err(Fault::Store(
            "created task differs from original frozen input".into(),
        ));
    }
    if args
        .agent_id
        .as_ref()
        .or(provenance.default_agent_id.as_ref())
        != Some(&input.definition.id)
        || args
            .expected_agent_revision
            .as_ref()
            .is_some_and(|r| r != &input.definition.revision)
    {
        return Err(Fault::Store("created task routing differs".into()));
    }
    let depth: i64 = tx.query_row(
        "SELECT json_extract(body,'$.depth') FROM tasks WHERE id=?1",
        params![task.id],
        |r| r.get(0),
    )?;
    if depth != 0 {
        return Err(Fault::Store("independent task has delegated depth".into()));
    }
    Ok(())
}
