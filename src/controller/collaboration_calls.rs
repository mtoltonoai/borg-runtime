//! Closed collaboration dispatch inside the existing receipt transaction.
//! Exact receipt replay happens before currentness in Database::process. A new
//! occurrence always rechecks the actor and reuses canonical task operations.
use super::{ControllerConfig, database::*, operations, validation::Context};
use crate::{
    collaboration::Query,
    mcp::{Caller, Invocation, ListPage},
    protocol::*,
};
use super::database::StoreContext as Connection;
use rusqlite::{ OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
#[derive(Deserialize)]
#[serde(tag = "tool", content = "arguments", deny_unknown_fields)]
enum ToolArgs {
    #[serde(rename = "collab_repositories_list")]
    Repositories { after: Option<String>, limit: usize },
    #[serde(rename = "collab_repository_get")]
    RepositoryRegistration { repository_id: String, registration_revision: Option<String> },
    #[serde(rename = "collab_limits_get")]
    Limits {},
    #[serde(rename = "collab_session_get")]
    Session { session_id: String },
    #[serde(rename = "collab_sessions_list")]
    Sessions {
        agent_id: Option<String>,
        cursor: Option<String>,
        limit: usize,
    },
    #[serde(rename = "collab_repository_tree")]
    RepositoryTree {
        source: super::repository_queries::Selector,
        path: String,
        after: Option<String>,
        limit: usize,
    },
    #[serde(rename = "collab_repository_read")]
    RepositoryRead {
        source: super::repository_queries::Selector,
        path: String,
        start_line: usize,
        max_lines: usize,
    },
    #[serde(rename = "collab_repository_search")]
    RepositorySearch {
        source: super::repository_queries::Selector,
        path: String,
        needle: String,
    },
    #[serde(rename = "collab_repository_diff")]
    RepositoryDiff {
        source: super::repository_queries::Selector,
        base_source: super::repository_queries::Selector,
        path: String,
    },
    #[serde(rename = "collab_repository_history")]
    RepositoryHistory {
        source: super::repository_queries::Selector,
        limit: usize,
    },
    #[serde(rename = "collab_call_get")]
    CallGet { call_id: String },
    #[serde(rename = "collab_tasks_list")]
    Tasks {
        cursor: Option<String>,
        limit: usize,
        agent_id: Option<String>,
        parent_task_id: Option<String>,
    },
    #[serde(rename = "collab_task_get")]
    Task { task_id: String },
    #[serde(rename = "collab_task_history_list")]
    History {
        task_id: String,
        cursor: Option<String>,
        limit: usize,
    },
    #[serde(rename = "collab_agents_list")]
    Agents {
        cursor: Option<String>,
        limit: usize,
    },
    #[serde(rename = "collab_agent_get")]
    Agent { agent_id: String },
    #[serde(rename = "collab_definition_update")]
    DefinitionUpdate(crate::protocol::DefinitionUpdate),
    #[serde(rename = "collab_definition_get")]
    Definition {
        definition_id: String,
        revision: String,
    },
    #[serde(rename = "collab_memory_list")]
    MemoryKeys {
        cursor: Option<String>,
        limit: usize,
    },
    #[serde(rename = "collab_memory_get")]
    Memory { key: String },
    #[serde(rename = "collab_memory_set")]
    SetMemory {
        key: String,
        expected_revision: Option<u64>,
        value: Value,
    },
    #[serde(rename = "collab_task_progress")]
    Progress { text: String },
    #[serde(rename = "collab_task_delegate")]
    Delegate { instruction: String },
    #[serde(rename = "collab_task_assign")]
    Assign {
        task_id: String,
        expected_revision: u64,
        agent_id: String,
    },
    #[serde(rename = "collab_task_cancel")]
    Cancel {
        task_id: String,
        expected_revision: u64,
    },
    #[serde(rename = "collab_task_retry")]
    Retry {
        task_id: String,
        expected_revision: u64,
    },
    #[serde(rename = "collab_workers_list")]
    Workers {
        cursor: Option<String>,
        limit: usize,
    },
    #[serde(rename = "collab_worker_get")]
    Worker { worker_id: String },
    #[serde(rename = "collab_machines_list")]
    Machines { cursor: Option<String>, limit: usize },
    #[serde(rename = "collab_machine_get")]
    Machine { machine_id: String },
    #[serde(rename = "collab_task_attempts_list")]
    Attempts {
        task_id: String,
        cursor: Option<String>,
        limit: usize,
    },
    #[serde(rename = "collab_task_checkpoints_list")]
    Checkpoints {
        task_id: String,
        cursor: Option<String>,
        limit: usize,
    },
    #[serde(rename = "collab_task_releases_list")]
    Releases {
        task_id: String,
        cursor: Option<String>,
        limit: usize,
    },
    #[serde(rename = "collab_release_get")]
    Release { release_id: String },
    #[serde(rename = "collab_release_checks_list")]
    Checks {
        release_id: String,
        cursor: Option<String>,
        limit: usize,
    },
}
fn parse(limits: &crate::limits::RuntimeLimits, call: &Invocation, input: Option<&TaskInput>) -> Result<ToolArgs> {
    call.validate(limits)
        .map_err(|_| Fault::Refused("invalid_input", "invalid collaboration invocation".into()))?;
    match input {
        Some(input) => crate::mcp::validate_arguments_for_task(&call.tool, &call.arguments, limits,
            input.definition.execution_profile.as_ref(), input.instruction_source.as_ref()),
        None => crate::mcp::validate_arguments(&call.tool, &call.arguments, limits),
    }.map_err(|reason| Fault::Refused("invalid_input", reason))?;
    serde_json::from_value(json!({"tool":call.tool,"arguments":call.arguments})).map_err(|_| {
        Fault::Refused(
            "invalid_input",
            "tool arguments differ from closed schema".into(),
        )
    })
}
pub(super) fn cancel_operation(limits: &crate::limits::RuntimeLimits, call: &Invocation) -> Option<Operation> {
    match parse(limits, call, None).ok()? {
        ToolArgs::Cancel {
            task_id,
            expected_revision,
        } => Some(Operation::CancelTask {
            operation_id: call.operation_id.clone(),
            task_id,
            expected_revision,
        }),
        _ => None,
    }
}
// Traceability scope: New occurrences validate current task/attempt/generation, worker incarnation, uncertainty and deadline before dispatch; retained exact replay is handled separately.
//= spec/impl/v2/implementation.md#shared-collaboration-tool-interface
//# The collaboration tool interface MUST validate its caller's current attempt before a new mutation.
pub(super) fn caller(tx: &Connection, actor: &Caller, now: u64) -> Result<StoredTask> {
    actor
        .validate(&tx.limits.transport)
        .map_err(|_| Fault::Refused("invalid_input", "invalid collaboration caller".into()))?;
    let task = operations::owned(
        tx,
        &actor.task_id,
        &actor.attempt_id,
        actor.generation,
        true,
    )?;
    let a = task
        .task
        .attempt
        .as_ref()
        .ok_or_else(|| Fault::Store("current caller lacks attempt".into()))?;
    if a.worker_id != actor.worker_id || a.worker_incarnation != actor.incarnation {
        return refuse("stale_owner", "collaboration worker binding differs");
    }
    let incarnation: Option<String> = tx
        .query_row(
            "SELECT body FROM workers WHERE id=?1",
            [&actor.worker_id],
            |r| r.get(0),
        )
        .optional()?;
    if incarnation
        .as_deref()
        .map(decode::<Worker>)
        .transpose()?
        .as_ref()
        .is_none_or(|w| w.incarnation != actor.incarnation)
    {
        return refuse(
            "stale_owner",
            "collaboration incarnation is no longer current",
        );
    }
    if a.uncertain || task.unresolved_execution || task.task.waiting_child_id.is_some() {
        return refuse(
            "reconciliation_required",
            "caller cannot start another collaboration effect",
        );
    }
    if now >= a.deadline_unix_ms || now >= task.task.input.limits.deadline_unix_ms {
        return refuse("deadline", "collaboration caller deadline expired");
    }
    if task.task.input.definition.execution_profile.is_none() {
        return refuse("not_configured", "collaboration profile is not configured");
    }
    Ok(task)
}
pub(super) fn lookup(tx: &Connection, actor: &Caller, id: &str, context: &Context<'_>, now: u64) -> Result<(Data, bool)> {
    let limits = tx.limits;
    actor
        .validate(&limits.transport)
        .map_err(|_| Fault::Refused("invalid_input", "invalid lookup caller".into()))?;
    if !valid_id(limits, id) {
        return refuse("invalid_input", "invalid call identity");
    }
    let Some(receipt) = super::metadata::read_retained(tx, id)? else {
        return refuse("not_found", "collaboration call has no committed outcome");
    };
    let Operation::CallCollaboration { call } = &receipt.request else {
        return refuse("not_found", "receipt is not a collaboration call");
    };
    if &call.caller != actor {
        return refuse("caller_mismatch", "receipt belongs to another caller");
    }
    // Unknown tool refusals are retained but never provide recursive GetOperation.
    call.validate(limits)
        .map_err(|_| Fault::Store("stored collaboration invocation is invalid".into()))?;
    if let super::metadata::RetainedOutcome::ReadReference(reference) = &receipt.outcome {
        if let Some(outcome) = recover_original(tx, context, call, reference, now)? {
            return Ok((Data::Operation(super::metadata::lookup_record(id, &receipt.request, &outcome)?), false));
        }
    }
    Ok((Data::Operation(receipt.lookup()?), false))
}

/// Bind a reconstructed lookup to the same retained original before attaching
/// its commit. The body must satisfy the compact receipt's exact typed digest;
/// a fresh query, another caller, or a different OperationRecord cannot pass.
pub(super) fn validate_recovered_lookup(
    operation: &Operation, receipt: &super::metadata::Retained, record: &OperationRecord,
) -> Result<()> {
    let invalid = || Fault::Store("mutation lookup changed original outcome".into());
    let (
        Operation::GetCollaborationCall { caller, operation_id },
        Operation::CallCollaboration { call },
        super::metadata::RetainedOutcome::ReadReference(reference),
    ) = (operation, &receipt.request, &receipt.outcome) else { return Err(invalid()); };
    if caller != &call.caller || operation_id != &call.operation_id
        || operation_id != &receipt.commit.operation_id {
        return Err(invalid());
    }
    let outcome: Outcome = serde_json::from_value(record.outcome.clone().ok_or_else(invalid)?)
        .map_err(|_| invalid())?;
    reference.committed_read_reference.evidence.verify(call, &outcome).map_err(|_| invalid())?;
    if *record != super::metadata::lookup_record(operation_id, &receipt.request, &outcome)? {
        return Err(invalid());
    }
    Ok(())
}

fn recover_original(tx: &Connection, context: &Context<'_>, call: &Invocation,
    reference: &crate::read_retention::CompactOutcome, now: u64) -> Result<Option<Outcome>> {
    use crate::read_retention::SourceProjection;
    let evidence = &reference.committed_read_reference.evidence;
    // read_retained has already verified release/custody/first-safe provenance.
    // Decode the original RunAttempt, rather than selecting a later session.
    let release = super::metadata::read(tx, &reference.committed_read_reference.release_operation_id)?
        .ok_or_else(|| Fault::Store("original read release missing".into()))?;
    let Operation::ReleaseTerminalReads(release) = release.request else { return Err(Fault::Store("original release kind".into())); };
    let command:String = tx.query_row("SELECT body FROM commands WHERE id=?1",[&release.command_id],|row|row.get(0))?;
    let WorkerCommand::RunAttempt {task:input, attempt, ..} = decode(&command)? else {return Err(Fault::Store("original read command kind".into()));};
    let mut original = load(tx, &call.caller.task_id)?;
    original.task.assigned_agent_id = input.definition.id.clone();
    original.task.input = input;
    original.task.attempt = Some(attempt);
    let recovered = match &evidence.source {
        SourceProjection::Repository {source, ..} => {
            let args:ToolArgs = serde_json::from_value(json!({"tool":call.tool,"arguments":call.arguments}))
                .map_err(|_|Fault::Store("retained repository arguments invalid".into()))?;
            use crate::repository_read::Query as R;
            let query = match args {
                ToolArgs::RepositoryTree {path,after,limit,..} => R::Tree {path,after,limit},
                ToolArgs::RepositoryRead {path,start_line,max_lines,..} => R::ReadFile {path,start_line,max_lines},
                ToolArgs::RepositorySearch {path,needle,..} => R::Search {path,needle},
                ToolArgs::RepositoryDiff {path,..} => R::Diff {path},
                ToolArgs::RepositoryHistory {limit,..} => R::History {limit},
                _ => return Err(Fault::Store("retained repository tool differs".into())),
            };
            match super::repository_queries::recover_original(tx, context, &original, source, query, now) {
                Ok(value) => Some(Outcome::Ok {data:Data::Repository(Box::new(value))}),
                Err(Fault::Refused(..)) => None,
                Err(other) => return Err(other),
            }
        }
        SourceProjection::Document {version} => {
            let query = Operation::QueryDocument {query:crate::collaboration::documents::DocumentQuery::Get {
                document_id:version.document_id.clone(),version:Some(version.version)}};
            match super::collaboration_routing::direct(tx, &query, now) {
                Ok((data,false)) => Some(Outcome::Ok {data}),
                Ok(_) => return Err(Fault::Store("original document read mutated state".into())),
                Err(Fault::Refused(..)) => None,
                Err(other) => return Err(other),
            }
        }
        SourceProjection::Page{page} => {
            use crate::read_page::{Source as P,DocumentPart,Encoding};
            match &page.identity.source {
                P::Document{version,part,record_revision:None} => {
                    let query=Operation::QueryDocument{query:crate::collaboration::documents::DocumentQuery::Get{
                        document_id:version.document_id.clone(),version:Some(version.version)}};
                    match super::collaboration_routing::direct(tx,&query,now) {
                        Ok((Data::DocumentQuery(crate::collaboration::documents::DocumentQueryOutcome::Document(view)),false))=>{
                            let (text,encoding)=match part {
                                DocumentPart::Body=>(view.version.content.body.clone(),Encoding::Utf8),
                                DocumentPart::Record=>(crate::read_page::canonical(&view.version).map_err(Fault::Store)?,Encoding::CanonicalJsonUtf8),
                            };
                            let end=page.offset.checked_add(page.returned_bytes);
                            if view.version.reference!=*version || encoding!=page.encoding || text.len()!=page.total_bytes
                                || crate::read_page::digest(text.as_bytes())!=page.identity.digest {None}
                            else {end.and_then(|end|text.get(page.offset..end)).map(|content|Outcome::Ok{
                                data:Data::ReadPage(Box::new(page.with_content(content.to_owned())))})}
                        }
                        Ok(_)=>None,Err(Fault::Refused(..))=>None,Err(other)=>return Err(other),
                    }
                }
                P::Repository{registration,commit,blob,checkpoint_operation} => {
                    let crate::read_page::Selection::Repository{source,path,start_line,max_lines,..}=&page.subject
                        else {return Err(Fault::Store("retained repository page selection differs".into()));};
                    let checkpoint=*source==crate::repository_view::SourceSelector::Checkpoint;
                    if checkpoint!=checkpoint_operation.is_some() {return Err(Fault::Store("retained checkpoint page association differs".into()));}
                    let record=super::repositories::revision(tx,registration)?;
                    let exact=crate::repository_view::Source {registration:registration.clone(),
                        repository:if checkpoint {record.checkpoint_endpoint}else{record.source_endpoint},
                        commit:commit.clone(),base_commit:None};
                    let query=crate::repository_read::Query::ReadFileSlice {path:path.clone(),start_line:*start_line,max_lines:*max_lines,offset:page.offset};
                    match super::repository_queries::recover_original(tx,context,&original,&exact,query,now) {
                        Ok(value)=>match value.data {
                            crate::repository_view::Data::FileSlice{path:actual,object,start_line:start,max_lines:lines,total_lines,next_line,offset,total_bytes,digest,text}
                                if actual==*path && object==*blob && start==*start_line && lines==*max_lines
                                && offset==page.offset && total_bytes==page.total_bytes && digest==page.identity.digest
                                && page.summary==(crate::read_page::Summary::Repository{start_line:start,max_lines:lines,total_lines,next_line}) =>
                            {
                                text.get(..page.returned_bytes).map(|content|Outcome::Ok{data:Data::ReadPage(Box::new(page.with_content(content.into())))})
                            }
                            _=>None,
                        },
                        Err(Fault::Refused(..))=>None,Err(other)=>return Err(other),
                    }
                }
                // Lists, task records and public item projections are volatile.
                // No current query is substituted for this original occurrence.
                _=>None,
            }
        }
        SourceProjection::ReadList{..} | SourceProjection::Observation => None,
    };
    // A changed immutable projection is unavailable, never a fresh old result.
    Ok(recovered.filter(|outcome| evidence.verify(call, outcome).is_ok()))
}
// Traceability scope: New accepted-caller operations bind retained metadata. The interface exposes its retained snapshot through collab_limits_get and explicit TaskInput.limits through collab_task_get.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//# Changing default metadata MUST NOT silently change the limits of an already accepted task.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//# The shared agent interface MUST expose the effective limits applicable to its caller.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//# The shared agent interface MUST expose supported definition-limit changes through the canonical controller operation layer.
// Traceability scope: The closed catalogue dispatches supported mutations through existing canonical handlers and their receipt transaction; this is not arbitrary worker or protected control access.
//= spec/impl/v2/implementation.md#shared-collaboration-tool-interface
//# The collaboration tool interface MUST expose configured task-space mutations under their current operation rules.
//= spec/impl/v2/implementation.md#shared-collaboration-tool-interface
//# The collaboration tool interface MUST use the controller's canonical operation layer.
pub(super) fn apply(
    tx: &Connection,
    call: &Invocation,
    cfg: &ControllerConfig,
    now: u64,
    validation: &Context<'_>,
) -> Result<(Data, bool)> {
    if call.tool == "collab_publication_get" {
        return bounded(tx.limits, super::publication_requests::get(tx, call)?);
    }
    let original = caller(tx, &call.caller, now)?;
    original.task.input.execution_limits().map_err(|reason| Fault::Store(reason))?;
    let selected = original.task.input.resolved_limits.as_ref().ok_or_else(|| Fault::Store("accepted caller limits absent".into()))?;
    let mut selected_limits=selected.global_metadata.clone();
    selected_limits.execution.collaboration=original.task.input.execution_limits().map_err(Fault::Store)?.collaboration.clone();
    let limits=&selected_limits;
    let mut bound_tx = tx.with_limits(limits);
    bound_tx.bound_actor=Some(super::identity_administration::bound_actor(&original.task,&call.caller));
    let bound_validation = Context { creation: None, issuer:validation.issuer, request:validation.request, proof:validation.proof, repository:validation.repository, limits };
    let result = if super::read_pages::is_read(&call.tool) {
        super::read_pages::agent(&bound_tx, call, cfg, now, &bound_validation)?
    } else {
        apply_inner(&bound_tx, call, cfg, now, &bound_validation)?
    };
    let observed = (cfg.clock)();
    if observed < now {
        return refuse(
            "clock_unavailable",
            "clock moved backwards during collaboration call",
        );
    }
    if observed >= original.task.input.limits.deadline_unix_ms {
        return refuse("deadline", "collaboration caller expired before commit");
    }
    bounded(limits, result)
}
pub(super) fn apply_inner(
    tx: &Connection,
    call: &Invocation,
    cfg: &ControllerConfig,
    now: u64,
    validation: &Context<'_>,
) -> Result<(Data, bool)> {
    let limits = tx.limits;
    let original = caller(tx, &call.caller, now)?;
    let actor = &call.caller;
    let id = call.operation_id.clone();
    if crate::mcp::publication::handles(&call.tool) {
        return super::publication_requests::apply(tx, call, cfg, now);
    }
    if crate::mcp::identity_administration::handles(&call.tool) {
        call.validate(limits).map_err(|e|Fault::Refused("invalid_input",e))?;
        crate::mcp::validate_arguments_for_profile(&call.tool,&call.arguments,limits,original.task.input.definition.execution_profile.as_ref()).map_err(|e|Fault::Refused("invalid_input",e))?;
        if call.tool=="collab_task_create" {
            let args:TaskCreate=serde_json::from_value(call.arguments.clone()).map_err(|e|Fault::Refused("invalid_input",e.to_string()))?;
            return super::task_creation::apply(tx,call,&args,cfg,now,validation);
        }
        let args=crate::mcp::identity_administration::parse(&call.tool,&call.arguments).map_err(|e|Fault::Refused("invalid_input",e))?;
        return super::identity_administration::apply(tx,&call.operation_id,&args,cfg,now);
    }
    if crate::mcp::workflow_tool(&call.tool) {
        return super::collaboration_routing::agent(tx, call, now);
    }
    let args = parse(limits, call, Some(&original.task.input))?;
    let query = |q| -> Result<(Data, bool)> {
        Ok((
            Data::Collaboration(super::collaboration_queries::apply(tx, &q, cfg)?),
            false,
        ))
    };
    let repository = |source, base, query| -> Result<(Data, bool)> {
        safe(limits, (
            Data::Repository(Box::new(super::repository_queries::require(
                tx, validation, &original, source, base, query, now,
            )?)),
            false,
        ))
    };
    let inner = match args {
        ToolArgs::Repositories { after, limit } => return safe(limits, query(Query::Repositories { after, limit })?),
        ToolArgs::RepositoryRegistration { repository_id, registration_revision } => return safe(limits, query(Query::RepositoryRegistration { repository_id, registration_revision })?),
        ToolArgs::Limits {} => return query(Query::Limits { task_id: Some(call.caller.task_id.clone()) }),
        ToolArgs::Session { session_id } => return safe(limits, query(Query::Session { session_id })?),
        ToolArgs::Sessions {
            agent_id,
            cursor,
            limit,
        } => {
            return safe(limits, query(Query::Sessions {
                agent_id,
                after: cursor,
                limit,
            })?);
        }
        ToolArgs::RepositoryTree {
            source,
            path,
            after,
            limit,
        } => {
            return repository(
                source,
                None,
                crate::repository_read::Query::Tree { path, after, limit },
            );
        }
        ToolArgs::RepositoryRead {
            source,
            path,
            start_line,
            max_lines,
        } => {
            return repository(
                source,
                None,
                crate::repository_read::Query::ReadFile {
                    path,
                    start_line,
                    max_lines,
                },
            );
        }
        ToolArgs::RepositorySearch {
            source,
            path,
            needle,
        } => {
            return repository(
                source,
                None,
                crate::repository_read::Query::Search { path, needle },
            );
        }
        ToolArgs::RepositoryDiff {
            source,
            base_source,
            path,
        } => {
            return repository(
                source,
                Some(base_source),
                crate::repository_read::Query::Diff { path },
            );
        }
        ToolArgs::RepositoryHistory { source, limit } => {
            return repository(
                source,
                None,
                crate::repository_read::Query::History { limit },
            );
        }
        ToolArgs::CallGet { call_id } => return safe(limits, lookup(tx, actor, &call_id, validation, now)?),
        ToolArgs::Tasks {
            cursor,
            limit,
            agent_id,
            parent_task_id,
        } => {
            return list(
                tx,
                "tasks",
                &json!([agent_id, parent_task_id]),
                cursor,
                limit,
                cfg,
                now,
            );
        }
        ToolArgs::Agents { cursor, limit } => {
            return list(tx, "agents", &Value::Null, cursor, limit, cfg, now);
        }
        ToolArgs::Releases {
            task_id,
            cursor,
            limit,
        } => return list(tx, "releases", &json!(task_id), cursor, limit, cfg, now),
        ToolArgs::Task { task_id } => Operation::GetTask { task_id },
        ToolArgs::Agent { agent_id } => Operation::GetAgent { agent_id },
        ToolArgs::DefinitionUpdate(input) => {
            // The already validated current caller supplies provenance, never
            // an argument of the closed twelve-field configuration payload.
            let recorded_actor = crate::collaboration::workflow::RecordedActor {
                principal: crate::collaboration::workflow::PrincipalRef::Agent { agent_id: original.task.assigned_agent_id.clone() },
                provenance: crate::collaboration::workflow::ActorProvenance::BoundAgentSession {
                    agent_id: original.task.assigned_agent_id.clone(), session_id: original.task.session_id.clone(),
                    task_id: call.caller.task_id.clone(), attempt_id: call.caller.attempt_id.clone(),
                    generation: call.caller.generation, worker_id: call.caller.worker_id.clone(), incarnation: call.caller.incarnation.clone(),
                },
            };
            return super::definition_updates::apply(tx, &input, cfg, &call.operation_id, &recorded_actor);
        }
        ToolArgs::Definition {
            definition_id,
            revision,
        } => {
            return safe(limits, query(Query::Definition {
                definition_id,
                revision,
            })?);
        }
        ToolArgs::MemoryKeys { cursor, limit } => {
            return safe(limits, query(Query::MemoryKeys {
                agent_id: original.task.assigned_agent_id,
                after: cursor,
                limit,
            })?);
        }
        ToolArgs::Memory { key } => Operation::GetMemory {
            agent_id: original.task.assigned_agent_id.clone(),
            key,
        },
        ToolArgs::SetMemory {
            key,
            expected_revision,
            value,
        } => Operation::SetMemory {
            operation_id: id,
            task_id: actor.task_id.clone(),
            attempt_id: actor.attempt_id.clone(),
            generation: actor.generation,
            key,
            expected_revision,
            value,
        },
        ToolArgs::Progress { text } => Operation::ObserveWorker {
            report: Report {
                report_id: id,
                worker_id: actor.worker_id.clone(),
                incarnation: actor.incarnation.clone(),
                task_id: actor.task_id.clone(),
                attempt_id: actor.attempt_id.clone(),
                generation: actor.generation,
                observation: Observation::Progress {
                    text,
                    output: original.task.recent_output.clone(),
                    truncated: original.task.output_truncated,
                },
            },
        },
        ToolArgs::Delegate { instruction } => {
            if crate::delegation::template(&original.task.input).is_none() {
                return refuse("not_configured", "no configured child template");
            }
            let child = crate::delegation::coding_child(&original.task.input, &instruction)
                .map_err(|_| {
                    Fault::Refused(
                        "invalid_input",
                        "configured child instruction refused".into(),
                    )
                })?;
            Operation::DelegateTask {
                operation_id: id,
                parent_task_id: actor.task_id.clone(),
                parent_attempt_id: actor.attempt_id.clone(),
                generation: actor.generation,
                child: (child).into(),
            }
        }
        ToolArgs::Assign {
            task_id,
            expected_revision,
            agent_id,
        } => Operation::AssignTask {
            operation_id: id,
            task_id,
            expected_revision,
            agent_id,
        },
        ToolArgs::Cancel {
            task_id,
            expected_revision,
        } => Operation::CancelTask {
            operation_id: id,
            task_id,
            expected_revision,
        },
        ToolArgs::Retry {
            task_id,
            expected_revision,
        } => Operation::RetryTask {
            operation_id: id,
            task_id,
            expected_revision,
        },
        ToolArgs::Workers { cursor, limit } => {
            return safe(limits, query(Query::Workers {
                after: cursor,
                limit,
            })?);
        }
        ToolArgs::Worker { worker_id } => return safe(limits, query(Query::Worker { worker_id })?),
        ToolArgs::Machines { cursor, limit } => return safe(limits, query(Query::Machines { after: cursor, limit })?),
        ToolArgs::Machine { machine_id } => return safe(limits, query(Query::Machine { machine_id })?),
        ToolArgs::History {
            task_id,
            cursor,
            limit,
        } => {
            return safe(limits, query(Query::TaskHistory {
                task_id,
                after: cursor,
                limit,
            })?);
        }
        ToolArgs::Attempts {
            task_id,
            cursor,
            limit,
        } => {
            return safe(limits, query(Query::TaskAttempts {
                task_id,
                after: cursor,
                limit,
            })?);
        }
        ToolArgs::Checkpoints {
            task_id,
            cursor,
            limit,
        } => {
            return safe(limits, query(Query::TaskCheckpoints {
                task_id,
                after: cursor,
                limit,
            })?);
        }
        ToolArgs::Release { release_id } => Operation::GetRelease { release_id },
        ToolArgs::Checks {
            release_id,
            cursor,
            limit,
        } => {
            return safe(limits, query(Query::ReleaseChecks {
                release_id,
                after: cursor,
                limit,
            })?);
        }
    };
    let result = operations::apply(tx, &inner, cfg, now, validation)?;
    // Recheck the original immutable deadline at finalization. No database
    // transaction survives helper waits; validation dispatch re-enters here.
    let final_now = (cfg.clock)();
    if final_now < now {
        return refuse(
            "clock_unavailable",
            "clock moved backwards during collaboration call",
        );
    }
    if final_now >= original.task.input.limits.deadline_unix_ms {
        return refuse("deadline", "collaboration caller expired before commit");
    }
    // Progress acknowledges the canonical mutation without echoing its entire
    // accepted input/output. Keep its exact task revision and changed bit; the
    // outer response bound and original receipt transaction still apply.
    //= spec/impl/v2/implementation.md#shared-collaboration-tool-interface
    //# The collaboration tool interface MUST use the controller's canonical operation layer.
    let result = match (call.tool.as_str(), result) {
        ("collab_task_progress", (Data::Task(task), changed)) => (
            Data::CollaborationPage(ListPage {
                items: vec![json!({
                    "task_id": task.id,
                    "session_id": task.session_id,
                    "revision": task.revision,
                })],
                next_cursor: None,
            }),
            changed,
        ),
        (_, result) => result,
    };
    safe(limits, result)
}
fn bounded(limits: &crate::limits::RuntimeLimits, value: (Data, bool)) -> Result<(Data, bool)> {
    let value = safe(limits, value)?;
    crate::protocol::bounded_json_size(&value.0, limits.interface.mcp_data_bytes).map_err(|_| {
        Fault::Refused("response_bound", "collaboration result exceeds tool bound: interface.mcp_data_bytes".into())
    })?;
    Ok(value)
}
pub(super) fn safe(limits: &crate::limits::RuntimeLimits, mut value: (Data, bool)) -> Result<(Data, bool)> {
    // The original public receipt is projected before commit. Canonical worker
    // GetTask/RunAttempt inputs keep their immutable provider target unchanged.
    value.0 = crate::pull_request_view::data(value.0).map_err(Fault::Store)?;
    // Known repository operands cannot expose credential-bearing identities.
    //= spec/impl/v2/implementation.md#shared-collaboration-tool-interface
    //# Collaboration tool responses MUST exclude credential values.
    let safe = match &value.0 {
        Data::Task(t) => !super::credential_admission::rejected(&Operation::SubmitTask {
            operation_id: "read-projection".into(),
            input: (t.input.clone()).into(),
        }),
        Data::WorkMutation(result) => !super::credential_admission::rejected(&Operation::SubmitTask {
            operation_id: "read-projection".into(), input: (result.view.task.input.clone()).into(),
        }),
        Data::WorkQuery(crate::collaboration::work::WorkQueryResult::Task(view)) => !super::credential_admission::rejected(&Operation::SubmitTask {
            operation_id: "read-projection".into(), input: (view.task.input.clone()).into(),
        }),
        Data::Release(r) => {
            !crate::credential::repository_contains_credentials(&r.checkpoint.repository)
                && !crate::credential::repository_contains_credentials(&r.checkpoint.remote_ref)
        }
        _ => true,
    };
    if !safe {
        return refuse(
            "credential_metadata",
            "credential-bearing metadata is not exposed",
        );
    }
    Ok(value)
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    kind: String,
    scope: Value,
    position: String,
}
pub(super) fn task_page(
    tx: &Connection,
    cfg: &ControllerConfig,
    agent_id: &Option<String>,
    parent_task_id: &Option<String>,
    after: &Option<String>,
    limit: usize,
) -> Result<ListPage> {
    let limits = tx.limits;
    if agent_id.as_deref().is_some_and(|s| !valid_id(limits, s))
        || parent_task_id.as_deref().is_some_and(|s| !valid_id(limits, s))
    {
        return refuse("invalid_input", "invalid task filter");
    }
    let (data, _) = list(
        tx,
        "tasks",
        &json!([agent_id, parent_task_id]),
        after.clone(),
        limit,
        cfg,
        (cfg.clock)(),
    )?;
    match data {
        Data::CollaborationPage(page) => Ok(page),
        _ => Err(Fault::Store("task page shape".into())),
    }
}
/// Browser fallback keeps the original legacy after identity as a bounded key
/// lookup, then uses the same scoped cursor and projection as the shared read.
/// Native/supplied callers never enter this legacy-route conversion.
pub(super) fn operator_list_cursor(tx:&Connection,kind:&str,scope:&Value,after:&Option<String>,cfg:&ControllerConfig,now:u64)->Result<Option<String>> {
    let Some(raw)=after else{return Ok(None)};
    if serde_json::from_str::<Cursor>(raw).is_ok() {return Ok(Some(raw.clone()));}
    if !valid_id(tx.limits,raw) {return refuse("invalid_input","invalid legacy page cursor");}
    let position=match kind {
        "tasks" if *scope==json!([null,null])=>tx.query_row("SELECT seq FROM tasks WHERE id=?1",[raw],|r|r.get::<_,i64>(0)).optional()?
            .filter(|seq|*seq>0).ok_or_else(||Fault::Refused("invalid_input","unknown page cursor".into()))?.to_string(),
        "agents"=>{
            super::agents::apply(tx,&Operation::GetAgent{agent_id:raw.clone()},cfg,now)?;
            raw.clone()
        },
        _=>return refuse("invalid_input","cursor differs from query scope"),
    };
    Ok(Some(encode(&Cursor{kind:kind.into(),scope:scope.clone(),position})?))
}
pub(super) fn agent_page(tx:&Connection,after:&Option<String>,limit:usize,cfg:&ControllerConfig,now:u64)->Result<(Data,bool)> {
    list(tx,"agents",&Value::Null,after.clone(),limit,cfg,now)
}
fn list(
    tx: &Connection,
    kind: &str,
    scope: &Value,
    after: Option<String>,
    limit: usize,
    cfg: &ControllerConfig,
    now: u64,
) -> Result<(Data, bool)> {
    let limits = tx.limits;
    if limit == 0 { return refuse("invalid_input", "invalid collaboration page limit"); }
    if limit > limits.controller.collaboration_page_items {
        return refuse("invalid_input", "invalid collaboration page limit: controller.collaboration_page_items");
    }
    if limit > limits.controller.max_list_tasks {
        return refuse("invalid_input", "invalid collaboration page limit: controller.max_list_tasks");
    }
    let start = match after {
        None => String::new(),
        Some(raw) => {
            if raw.len() > limits.controller.collaboration_cursor_bytes {
                return refuse("invalid_input", "cursor bound: controller.collaboration_cursor_bytes");
            }
            let c: Cursor = serde_json::from_str(&raw).map_err(|_| {
                Fault::Refused("invalid_input", "invalid collaboration cursor".into())
            })?;
            if c.kind != kind || c.scope != *scope || !valid_id(limits, &c.position) || encode(&c)? != raw {
                return refuse("invalid_input", "cursor differs from query scope");
            };
            c.position
        }
    };
    let cursor = |position: String| {
        encode(&Cursor {
            kind: kind.into(),
            scope: scope.clone(),
            position,
        })
    };
    let mut values = Vec::new();
    if kind == "tasks" {
        let seq = if start.is_empty() {
            0
        } else {
            start
                .parse::<i64>()
                .ok()
                .filter(|v| *v > 0 && v.to_string() == start)
                .ok_or_else(|| Fault::Refused("invalid_input", "invalid task cursor".into()))?
        };
        let agent = scope[0].as_str();
        let parent = scope[1].as_str();
        // Parent filtering is bounded by scanned rows, not an unbounded walk.
        let mut q=tx.prepare("SELECT id,seq FROM tasks WHERE seq>?1 AND (?2 IS NULL OR agent=?2) ORDER BY seq LIMIT ?3")?;
        let rows = q
            .query_map(params![seq, agent, (limit + 1) as i64], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for (id, seq) in rows {
            let t = operations::view(tx, load(tx, &id)?, cfg, now)?;
            let keep = parent.is_none_or(|p| t.parent_task_id.as_deref() == Some(p));
            let value=keep.then(||json!({"id":t.id,"session_id":t.session_id,"revision":t.revision,"status":t.status,"assigned_agent_id":t.assigned_agent_id,"definition":t.input.definition,"dependencies":t.input.dependencies,"parent_task_id":t.parent_task_id,"attempt":t.attempt,"has_result":t.result.is_some(),"has_checkpoint":t.selected_checkpoint.is_some()}));
            values.push((seq.to_string(), value));
        }
    } else if kind == "agents" {
        let mut q =
            tx.prepare("SELECT id FROM agents WHERE id>?1 AND NOT EXISTS(SELECT 1 FROM agent_retirements r WHERE r.id=agents.id) ORDER BY id COLLATE BINARY LIMIT ?2")?;
        let rows = q
            .query_map(params![start, (limit + 1) as i64], |r| {
                r.get::<_, String>(0)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for id in rows {
            let (data, _) = super::agents::apply(
                tx,
                &Operation::GetAgent {
                    agent_id: id.clone(),
                },
                cfg,
                now,
            )?;
            let Data::Agent(agent) = data else {
                unreachable!()
            };
            values.push((id, Some(serde_json::to_value(agent)?)));
        }
    } else {
        let task_id = scope
            .as_str()
            .ok_or_else(|| Fault::Store("release scope".into()))?;
        load(tx, task_id)?;
        let mut q=tx.prepare("SELECT id FROM releases WHERE task_id=?1 AND id>?2 ORDER BY id COLLATE BINARY LIMIT ?3")?;
        let rows = q
            .query_map(params![task_id, start, (limit + 1) as i64], |r| {
                r.get::<_, String>(0)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for id in rows {
            let data = operations::apply(
                tx,
                &Operation::GetRelease {
                    release_id: id.clone(),
                },
                cfg,
                now,
                &Context {
                    creation: None,
                    limits,
                    issuer: &std::sync::Arc::new(()),
                    request: "read",
                    proof: None,
                    repository: None,
                },
            )?;
            let (data, _) = safe(limits, data)?;
            let Data::Release(release) = data else {
                unreachable!()
            };
            values.push((id, Some(serde_json::to_value(release)?)));
        }
    }
    let more = values.len() > limit;
    let mut page = ListPage {
        items: vec![],
        next_cursor: None,
    };
    let mut last = None;
    for (position, item) in values.into_iter().take(limit) {
        if let Some(item) = item {
            page.items.push(item);
            page.next_cursor = Some(cursor(position.clone())?);
            if !tx.fitting_read && crate::protocol::bounded_json_size(&page, limits.controller.collaboration_item_bytes).is_err() {
                page.items.pop();
                if page.items.is_empty() {
                    return refuse(
                        "response_bound",
                        "first collaboration page item exceeds bound",
                    );
                };
                page.next_cursor = last;
                return safe(limits, (Data::CollaborationPage(page), false));
            }
        }
        last = Some(cursor(position)?);
    }
    page.next_cursor = if more { last } else { None };
    safe(limits, (Data::CollaborationPage(page), false))
}

#[cfg(test)]
#[path = "collaboration_calls/workflow_projection_tests.rs"]
mod workflow_projection_tests;


#[cfg(test)]
mod recovered_lookup_tests {
    use super::*;
    use super::super::metadata::{self, Retained, RetainedOutcome};
    use crate::read_retention::{CompactOutcome, CompactRead, ReadEvidence};

    fn fixture() -> (Operation, Retained, OperationRecord) {
        let call = Invocation {
            operation_id: "original-read".into(), logical_call_id: "original-logical".into(),
            caller: Caller { task_id: "task".into(), attempt_id: "attempt".into(), generation: 1,
                worker_id: "worker".into(), incarnation: "incarnation".into() },
            tool: "collab_repository_read".into(),
            arguments: json!({"source":"start","path":"body.txt","start_line":1,"max_lines":1}),
        };
        let outcome = Outcome::Ok { data: Data::Repository(Box::new(crate::repository_view::Output {
            source: crate::repository_view::Source {
                registration: crate::repository_catalogue::Revision {
                    repository_id: "repository".into(), registration_revision: "registration".into(),
                },
                repository: "file:///immutable.git".into(), commit: "1".repeat(40), base_commit: None,
            },
            untrusted: true,
            data: crate::repository_view::Data::File { path: "body.txt".into(), object: "2".repeat(40),
                start_line: 1, total_lines: 1, text: "original body\n".into(), next_line: None },
        })) };
        let evidence = ReadEvidence::from_outcome(&call, &outcome).unwrap();
        let request = call.operation();
        let record = metadata::lookup_record(&call.operation_id, &request, &outcome).unwrap();
        let operation = Operation::GetCollaborationCall {
            caller: call.caller.clone(), operation_id: call.operation_id.clone(),
        };
        // This focused seam starts after read_retained has validated durable
        // release/custody provenance. Real receipt/reopen tests cover that join.
        let retained = Retained { request, outcome: RetainedOutcome::ReadReference(CompactOutcome {
            committed_read_reference: CompactRead { evidence, release_operation_id: "release".into(),
                attempt_id: call.caller.attempt_id, session_id: "session".into() },
        }), commit: MutationCommit { operation_id: call.operation_id, controller_revision: 7 } };
        (operation, retained, record)
    }

    #[test]
    fn reconstructed_lookup_requires_exact_original_request_caller_commit_and_evidence() {
        let (operation, retained, record) = fixture();
        assert!(validate_recovered_lookup(&operation, &retained, &record).is_ok());
        assert_ne!(record, retained.lookup().unwrap(), "the selected seam actually reconstructs a body");
        assert!(validate_recovered_lookup(&Operation::GetOperation {
            operation_id: retained.commit.operation_id.clone(),
        }, &retained, &record).is_err(), "ordinary GetOperation cannot use this exception");

        let (mut operation, retained, record) = fixture();
        let Operation::GetCollaborationCall { caller, .. } = &mut operation else { unreachable!() };
        caller.incarnation = "other-incarnation".into();
        assert!(validate_recovered_lookup(&operation, &retained, &record).is_err());
        let (mut operation, retained, record) = fixture();
        let Operation::GetCollaborationCall { operation_id, .. } = &mut operation else { unreachable!() };
        *operation_id = "other-operation".into();
        assert!(validate_recovered_lookup(&operation, &retained, &record).is_err());

        let (operation, mut retained, record) = fixture();
        retained.commit.operation_id = "other-commit".into();
        assert!(validate_recovered_lookup(&operation, &retained, &record).is_err());
        let (operation, mut retained, record) = fixture();
        let Operation::CallCollaboration { call } = &mut retained.request else { unreachable!() };
        call.arguments["path"] = json!("different.txt");
        assert!(validate_recovered_lookup(&operation, &retained, &record).is_err());
        let (operation, mut retained, record) = fixture();
        let RetainedOutcome::ReadReference(reference) = &mut retained.outcome else { unreachable!() };
        reference.committed_read_reference.evidence.outcome_sha256 = "f".repeat(64);
        assert!(validate_recovered_lookup(&operation, &retained, &record).is_err());

        let (operation, retained, record) = fixture();
        let mut changed: Outcome = serde_json::from_value(record.outcome.clone().unwrap()).unwrap();
        let Outcome::Ok { data: Data::Repository(body) } = &mut changed else { unreachable!() };
        let crate::repository_view::Data::File { text, .. } = &mut body.data else { unreachable!() };
        *text = "different body\n".into();
        let changed = metadata::lookup_record(&retained.commit.operation_id, &retained.request, &changed).unwrap();
        assert!(validate_recovered_lookup(&operation, &retained, &changed).is_err(), "well-typed changed body must fail its original digest");

        let (operation, retained, record) = fixture();
        for field in ["id", "task_id", "attempt_id", "generation", "kind", "intended", "state", "outcome"] {
            let mut changed = serde_json::to_value(&record).unwrap();
            changed[field] = match field {
                "generation" => json!(9),
                "intended" => json!({"different":"request"}),
                "outcome" => json!({"status":"refused","code":"not_found","message":"different"}),
                _ => json!("different"),
            };
            let changed: OperationRecord = serde_json::from_value(changed).unwrap();
            assert!(validate_recovered_lookup(&operation, &retained, &changed).is_err(), "changed {field}");
        }
    }
}
