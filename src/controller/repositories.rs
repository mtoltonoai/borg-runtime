//! Immutable repository registration revisions in the existing controller store.
//! Operator mutation receipts provide replay/CAS atomicity. No filesystem or Git I/O.
use super::{bounded_json_size, database::*};
use crate::{collaboration::Page, protocol::*, repository_catalogue::*};
use rusqlite::{OptionalExtension, params};

fn identity(tx: &StoreContext<'_>, value: &str) -> Result<()> {
    if !valid_id(tx.limits, value) {
        return refuse(
            "invalid_input",
            "invalid repository metadata identity: transport.identity_bytes",
        );
    }
    Ok(())
}
pub(super) fn validate_registration(tx: &StoreContext<'_>, value: &Registration) -> Result<()> {
    identity(tx, &value.repository_id)?;
    identity(tx, &value.registration_revision)?;
    identity(tx, &value.display_name)?;
    for key in [&value.source_store_key, &value.checkpoint_store_key] {
        identity(tx, key)?;
        if !crate::repository_config::valid_store_key(key) {
            return refuse(
                "invalid_input",
                "repository store key must be a single safe component",
            );
        }
    }
    for endpoint in [&value.source_endpoint, &value.checkpoint_endpoint] {
        if endpoint.is_empty() || endpoint.chars().any(char::is_control) {
            return refuse("invalid_input", "invalid repository endpoint");
        }
        if endpoint.len() > tx.limits.repository.endpoint_bytes {
            return refuse(
                "invalid_input",
                "repository endpoint exceeds metadata limit: repository.endpoint_bytes",
            );
        }
        if crate::credential::repository_contains_credentials(endpoint) {
            return refuse(
                "credential_metadata",
                "repository endpoint contains credentials",
            );
        }
    }
    if let Some(target) = &value.pull_request_target {
        crate::pull_request::validate_target(target, &crate::pull_request::Bounds::from_limits(tx.limits))
            .map_err(|e| Fault::Refused("pull_request_invalid", e.to_string()))?;
    }
    Ok(())
}
struct Head {
    revision: String,
    generation: u64,
    enabled: bool,
}
fn size(value: usize) -> Result<u64> {
    u64::try_from(value)
        .map_err(|_| Fault::Store("repository size exceeds unsigned representation".into()))
}
fn head(tx: &StoreContext<'_>, id: &str) -> Result<Head> {
    identity(tx, id)?;
    let (revision, generation, enabled): (String, i64, i64) = tx
        .query_row(
            "SELECT current_revision,generation,enabled FROM repositories WHERE id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?
        .ok_or_else(|| {
            Fault::Refused(
                "repository_not_found",
                "repository registration does not exist".into(),
            )
        })?;
    let generation = unsigned(generation)?;
    if generation == 0 {
        return Err(Fault::Store(
            "repository head generation must be positive".into(),
        ));
    }
    let enabled = match enabled {
        0 => false,
        1 => true,
        _ => {
            return Err(Fault::Store(
                "invalid stored repository enabled state".into(),
            ));
        }
    };
    Ok(Head {
        revision,
        generation,
        enabled,
    })
}
pub(super) fn revision(tx: &StoreContext<'_>, binding: &Revision) -> Result<Registration> {
    identity(tx, &binding.repository_id)?;
    identity(tx, &binding.registration_revision)?;
    let raw: String = tx
        .query_row(
            "SELECT body FROM repository_revisions WHERE repository_id=?1 AND revision=?2",
            params![binding.repository_id, binding.registration_revision],
            |r| r.get(0),
        )
        .optional()?
        .ok_or_else(|| {
            Fault::Refused(
                "repository_not_found",
                "repository registration revision does not exist".into(),
            )
        })?;
    if size(raw.len())? > tx.limits.repository.catalogue_bytes {
        return Err(Fault::Store(
            "repository record exceeds repository.catalogue_bytes".into(),
        ));
    }
    let value: Registration = decode(&raw)?;
    validate_registration(tx, &value)?;
    if value.binding() != *binding {
        return Err(Fault::Store(
            "repository revision key differs from body".into(),
        ));
    }
    Ok(value)
}
pub(super) fn get(tx: &StoreContext<'_>, id: &str, selected: Option<&str>) -> Result<Entry> {
    let h = head(tx, id)?;
    let value = revision(
        tx,
        &Revision {
            repository_id: id.into(),
            registration_revision: selected.unwrap_or(&h.revision).into(),
        },
    )?;
    Ok(Entry {
        pull_request_available: value.pull_request_target.is_some(),
        repository_id: value.repository_id,
        registration_revision: value.registration_revision,
        display_name: value.display_name,
        source_endpoint: value.source_endpoint,
        checkpoint_endpoint: value.checkpoint_endpoint,
        current_revision: h.revision,
        generation: h.generation,
        enabled_for_new_tasks: h.enabled,
    })
}
pub(super) fn list(
    tx: &StoreContext<'_>,
    after: &Option<String>,
    limit: usize,
) -> Result<Page<Entry>> {
    if limit == 0 {
        return refuse("invalid_input", "repository page limit must be positive");
    }
    if limit > tx.limits.controller.collaboration_page_items {
        return refuse(
            "invalid_input",
            "repository page exceeds metadata limit: controller.collaboration_page_items",
        );
    }
    if let Some(after) = after {
        identity(tx, after)?;
    }
    let count = limit
        .checked_add(1)
        .and_then(|n| i64::try_from(n).ok())
        .ok_or_else(|| {
            Fault::Refused(
                "invalid_input",
                "repository page exceeds SQLite representation".into(),
            )
        })?;
    let mut q = tx.prepare("SELECT id FROM repositories WHERE id>?1 ORDER BY id LIMIT ?2")?;
    let ids = q
        .query_map(params![after.as_deref().unwrap_or(""), count], |r| {
            r.get::<_, String>(0)
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let more = ids.len() > limit;
    let mut result = Page {
        items: Vec::new(),
        next_cursor: None,
    };
    for id in ids.into_iter().take(limit) {
        result.items.push(get(tx, &id, None)?);
        result.next_cursor = Some(id);
        if bounded_json_size(&result, tx.limits.controller.collaboration_page_bytes).is_err() {
            result.items.pop();
            if result.items.is_empty() {
                return refuse(
                    "response_bound",
                    "repository entry exceeds metadata limit: controller.collaboration_page_bytes",
                );
            }
            result.next_cursor = result.items.last().map(|v| v.repository_id.clone());
            return Ok(result);
        }
    }
    if !more {
        result.next_cursor = None;
    }
    Ok(result)
}
fn capacity(tx: &StoreContext<'_>, body: &str, new_head: bool) -> Result<()> {
    let limits = &tx.limits.repository;
    let heads: i64 = tx.query_row("SELECT count(*) FROM repositories", [], |r| r.get(0))?;
    let (count, bytes): (i64, i64) = tx.query_row(
        "SELECT count(*),coalesce(sum(length(CAST(body AS BLOB))),0) FROM repository_revisions",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if new_head && unsigned(heads)? >= size(limits.catalogue_entries)? {
        return refuse(
            "capacity",
            "repository catalogue exhausted: repository.catalogue_entries",
        );
    }
    if unsigned(count)? >= size(limits.catalogue_revisions)? {
        return refuse(
            "capacity",
            "repository revision catalogue exhausted: repository.catalogue_revisions",
        );
    }
    if unsigned(bytes)?
        .checked_add(size(body.len())?)
        .is_none_or(|total| total > limits.catalogue_bytes)
    {
        return refuse(
            "capacity",
            "repository catalogue bytes exhausted: repository.catalogue_bytes",
        );
    }
    Ok(())
}
fn next_generation(current: &Head, expected: u64) -> Result<i64> {
    if current.generation != expected {
        return refuse(
            "revision_conflict",
            "repository catalogue head generation changed",
        );
    }
    let next = current.generation.checked_add(1).ok_or_else(|| {
        Fault::Refused(
            "capacity",
            "repository generation exceeds SQLite representation".into(),
        )
    })?;
    integer(next).map_err(|_| {
        Fault::Refused(
            "capacity",
            "repository generation exceeds SQLite representation".into(),
        )
    })
}
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//# The runtime MUST support multiple repository registrations described by metadata.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//# A repository registration MUST have a stable repository identity.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//# A repository registration revision MUST be immutable.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//# An operator repository-catalogue mutation MUST use the canonical durable controller operation path.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//# A repository-catalogue update MUST compare the current repository head generation supplied by the operator.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//# A repository registration operation MUST NOT acquire repository material.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//# A repository registration operation MUST NOT grant repository access.
pub(super) fn apply(tx: &StoreContext<'_>, op: &Operation) -> Result<(Data, bool)> {
    match op {
        Operation::ListRepositories { after, limit } => Ok((
            Data::RepositoryRegistrations(list(tx, after, *limit)?),
            false,
        )),
        Operation::GetRepository {
            repository_id,
            registration_revision,
        } => Ok((
            Data::RepositoryRegistration(get(tx, repository_id, registration_revision.as_deref())?),
            false,
        )),
        Operation::RegisterRepository { registration, .. } => {
            validate_registration(tx, registration)?;
            if tx
                .query_row(
                    "SELECT 1 FROM repositories WHERE id=?1",
                    [&registration.repository_id],
                    |_| Ok(()),
                )
                .optional()?
                .is_some()
            {
                return refuse("repository_conflict", "repository ID already exists");
            }
            let body = encode(registration)?;
            capacity(tx, &body, true)?;
            tx.execute(
                "INSERT INTO repository_revisions(repository_id,revision,body) VALUES(?1,?2,?3)",
                params![
                    registration.repository_id,
                    registration.registration_revision,
                    body
                ],
            )?;
            tx.execute("INSERT INTO repositories(id,current_revision,generation,enabled) VALUES(?1,?2,1,1)", params![registration.repository_id,registration.registration_revision])?;
            Ok((
                Data::RepositoryRegistration(get(tx, &registration.repository_id, None)?),
                true,
            ))
        }
        Operation::ReviseRepository {
            expected_generation,
            registration,
            ..
        } => {
            validate_registration(tx, registration)?;
            let current = head(tx, &registration.repository_id)?;
            let generation = next_generation(&current, *expected_generation)?;
            if tx
                .query_row(
                    "SELECT 1 FROM repository_revisions WHERE repository_id=?1 AND revision=?2",
                    params![
                        registration.repository_id,
                        registration.registration_revision
                    ],
                    |_| Ok(()),
                )
                .optional()?
                .is_some()
            {
                return refuse(
                    "repository_conflict",
                    "repository revision is immutable and already exists",
                );
            }
            let body = encode(registration)?;
            capacity(tx, &body, false)?;
            tx.execute(
                "INSERT INTO repository_revisions(repository_id,revision,body) VALUES(?1,?2,?3)",
                params![
                    registration.repository_id,
                    registration.registration_revision,
                    body
                ],
            )?;
            tx.execute(
                "UPDATE repositories SET current_revision=?2,generation=?3 WHERE id=?1",
                params![
                    registration.repository_id,
                    registration.registration_revision,
                    generation
                ],
            )?;
            Ok((
                Data::RepositoryRegistration(get(tx, &registration.repository_id, None)?),
                true,
            ))
        }
        Operation::SetRepositoryEnabled {
            repository_id,
            expected_generation,
            enabled_for_new_tasks,
            ..
        } => {
            let current = head(tx, repository_id)?;
            let generation = next_generation(&current, *expected_generation)?;
            tx.execute(
                "UPDATE repositories SET enabled=?2,generation=?3 WHERE id=?1",
                params![repository_id, enabled_for_new_tasks, generation],
            )?;
            Ok((
                Data::RepositoryRegistration(get(tx, repository_id, None)?),
                true,
            ))
        }
        _ => Err(Fault::Store("not a repository catalogue operation".into())),
    }
}
fn selected(
    tx: &StoreContext<'_>,
    binding: Option<&Revision>,
    new_task: bool,
) -> Result<Registration> {
    let binding = binding.ok_or_else(|| {
        Fault::Refused(
            "repository_binding_required",
            "repository ID and registration revision are required".into(),
        )
    })?;
    if new_task {
        let h = head(tx, &binding.repository_id)?;
        if !h.enabled {
            return refuse(
                "repository_disabled",
                "repository is disabled for new tasks",
            );
        }
        if h.revision != binding.registration_revision {
            return refuse(
                "repository_revision_changed",
                "new task must select the current repository registration revision",
            );
        }
    }
    revision(tx, binding)
}
/// Resolve once at admission. A configured child is resolved with its parent and
/// later checked against that exact accepted template, not current enable state.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//# An accepted task MUST retain its selected repository registration revision.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//# A newly resolved repository association MUST reject a repository registration disabled for new tasks.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//# A catalogue revision or enable-state change MUST NOT change an accepted task's repository association.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//# A configured child repository association MUST be resolved when its parent workflow is accepted.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//# An accepted repository association MUST distinguish the source endpoint from the checkpoint destination.
pub(super) fn resolve_input(
    tx: &StoreContext<'_>,
    input: &mut TaskInput,
    accepted_child: bool,
) -> Result<()> {
    if let Some(repo) = &mut input.repository {
        let registration = selected(tx, repo.registration.as_ref(), !accepted_child)?;
        if accepted_child {
            if repo.url != registration.source_endpoint
                || repo.checkpoint_remote != registration.checkpoint_endpoint
                || repo.pull_request_target != registration.pull_request_target
            {
                return refuse(
                    "repository_binding_mismatch",
                    "accepted repository endpoints differ from registration revision",
                );
            }
        } else {
            if !repo.url.is_empty() || !repo.checkpoint_remote.is_empty() || repo.pull_request_target.is_some() {
                return refuse(
                    "invalid_input",
                    "external submission cannot supply resolved repository endpoints",
                );
            }
            repo.url = registration.source_endpoint;
            repo.checkpoint_remote = registration.checkpoint_endpoint;
            repo.pull_request_target = registration.pull_request_target;
        }
    }
    if let Some(repo) = &mut input.repository_read {
        let registration = selected(tx, repo.registration.as_ref(), !accepted_child)?;
        if accepted_child {
            if repo.repository != registration.source_endpoint {
                return refuse(
                    "repository_binding_mismatch",
                    "accepted read endpoint differs from registration revision",
                );
            }
        } else {
            if !repo.repository.is_empty() {
                return refuse(
                    "invalid_input",
                    "external submission cannot supply resolved read endpoint",
                );
            }
            repo.repository = registration.source_endpoint;
        }
    }
    if let Some(source) = &mut input.instruction_source {
        resolve_instruction_source(tx, source, accepted_child)?;
    }
    if let Some(NativeWorkflow::CodingThenContinue { child } | NativeWorkflow::Reassign { target: child }) = input.native_workflow.as_deref_mut()
    {
        let registration = selected(tx, child.repository.registration.as_ref(), !accepted_child)?;
        if accepted_child {
            if child.repository.url != registration.source_endpoint
                || child.repository.checkpoint_remote != registration.checkpoint_endpoint
                || child.repository.pull_request_target != registration.pull_request_target
            {
                return refuse(
                    "repository_binding_mismatch",
                    "configured child endpoints differ from accepted registration",
                );
            }
        } else {
            if !child.repository.url.is_empty() || !child.repository.checkpoint_remote.is_empty()
                || child.repository.pull_request_target.is_some() {
                return refuse(
                    "invalid_input",
                    "external workflow cannot supply resolved repository endpoints",
                );
            }
            child.repository.url = registration.source_endpoint;
            child.repository.checkpoint_remote = registration.checkpoint_endpoint;
            child.repository.pull_request_target = registration.pull_request_target;
        }
        if let Some(source) = &mut child.instruction_source {
            resolve_instruction_source(tx, source, accepted_child)?;
        }
    }
    Ok(())
}
fn resolve_instruction_source(
    tx: &StoreContext<'_>,
    source: &mut RepositoryReadSource,
    accepted: bool,
) -> Result<()> {
    let registration = selected(tx, source.registration.as_ref(), !accepted)?;
    if accepted {
        if source.repository != registration.source_endpoint {
            return refuse(
                "repository_binding_mismatch",
                "accepted instruction endpoint differs from registration revision",
            );
        }
    } else {
        if !source.repository.is_empty() {
            return refuse(
                "invalid_input",
                "external submission cannot supply resolved instruction endpoint",
            );
        }
        source.repository = registration.source_endpoint;
    }
    source
        .validate_instruction_binding(tx.limits)
        .map_err(|reason| Fault::Refused("invalid_input", reason))
}

fn instruction_selection(
    input: &TaskInput,
) -> (Option<&RepositoryReadSource>, Option<&RepositoryReadSource>) {
    (
        input.instruction_source.as_ref(),
        crate::delegation::configured_template(input).and_then(|child| child.instruction_source.as_ref()),
    )
}
fn has_instruction_source(input: &TaskInput) -> bool {
    instruction_selection(input) != (None, None)
}
fn instruction_invalid() -> Fault {
    Fault::Store("retained instruction source differs from original accepted evidence".into())
}
fn retained_instruction_input(
    tx: &StoreContext<'_>,
    input: &TaskInput,
    version: i64,
) -> Result<()> {
    if !has_instruction_source(input) {
        return Ok(());
    }
    if version < 21 {
        return Err(instruction_invalid());
    }
    crate::execution_profile::validate(input).map_err(|_| instruction_invalid())?;
    let limits = &input
        .resolved_limits
        .as_ref()
        .ok_or_else(instruction_invalid)?
        .global_metadata;
    let accepted = tx.with_limits(limits);
    for source in [
        instruction_selection(input).0,
        instruction_selection(input).1,
    ]
    .into_iter()
    .flatten()
    {
        let mut checked = source.clone();
        resolve_instruction_source(&accepted, &mut checked, true)?;
        if &checked != source {
            return Err(instruction_invalid());
        }
    }
    Ok(())
}
fn original_instruction_selection(requested: &TaskInput, accepted: &TaskInput) -> bool {
    let (rt, rc) = instruction_selection(requested);
    let (at, ac) = instruction_selection(accepted);
    [(rt, at), (rc, ac)]
        .into_iter()
        .all(|(requested, accepted)| match (requested, accepted) {
            (None, None) => true,
            (Some(r), Some(a)) => {
                r.registration == a.registration
                    && r.start_commit == a.start_commit
                    && (r.repository.is_empty() || r.repository == a.repository)
            }
            _ => false,
        })
}
fn instruction_command(tx: &StoreContext<'_>, command: &WorkerCommand, version: i64) -> Result<()> {
    if let WorkerCommand::RunAttempt { attempt, task, .. } = command {
        retained_instruction_input(tx, task, version)?;
        let original = super::sessions::historical_task(tx,&attempt.task_id,&attempt.session_id)?;
        if instruction_selection(task) != instruction_selection(&original.input)
            || has_instruction_source(task)
                && task.resolved_limits != original.input.resolved_limits
        {
            return Err(instruction_invalid());
        }
    }
    Ok(())
}
/// Validate the new vocabulary before the owner advances the schema marker.
/// Both directions matter: a valid registration alone cannot replace an accepted
/// commit, and removing the only original admission receipt cannot hide a source.
pub(super) fn validate_instruction_sources(
    tx: &StoreContext<'_>,
    cfg: &super::ControllerConfig,
    version: i64,
) -> Result<()> {
    use std::collections::BTreeSet;
    let receipt_bound = cfg
        .max_receipts
        .checked_add(cfg.max_cleanup_receipts)
        .ok_or_else(instruction_invalid)?;
    let command_bound = receipt_bound
        .checked_add(cfg.max_tasks)
        .and_then(|n| n.checked_mul(2))
        .ok_or_else(instruction_invalid)?;
    let mut selected_tasks = BTreeSet::new();
    let mut admitted = BTreeSet::new();
    let mut q = tx.prepare("SELECT id FROM tasks ORDER BY seq")?;
    let mut rows = q.query([])?;
    let mut count = 0usize;
    while let Some(row) = rows.next()? {
        count += 1;
        if count > cfg.max_tasks {
            return Err(instruction_invalid());
        }
        let mut task = load(tx, &row.get::<_, String>(0)?)?.task;
        task.input=super::sessions::initial_input(tx,&task.id)?;
        retained_instruction_input(tx, &task.input, version)?;
        if has_instruction_source(&task.input) {
            selected_tasks.insert(task.id);
        }
    }
    let mut q = tx.prepare("SELECT length(CAST(body AS BLOB)), CASE WHEN length(CAST(body AS BLOB))<=?1 THEN body ELSE NULL END FROM commands ORDER BY seq")?;
    let mut rows = q.query([sql_bound(tx.limits.transport.body_bytes)?])?;
    count = 0;
    while let Some(row) = rows.next()? {
        count += 1;
        if count > command_bound {
            return Err(instruction_invalid());
        }
        if unsigned(row.get(0)?)? > size(tx.limits.transport.body_bytes)? {
            return Err(instruction_invalid());
        }
        instruction_command(
            tx,
            &decode::<WorkerCommand>(&row.get::<_, String>(1)?)?,
            version,
        )?;
    }
    let mut q = tx.prepare("SELECT id FROM receipts ORDER BY controller_revision")?;
    let mut rows = q.query([])?;
    count = 0;
    while let Some(row) = rows.next()? {
        count += 1;
        if count > receipt_bound {
            return Err(instruction_invalid());
        }
        let record = super::metadata::read(tx, &row.get::<_, String>(0)?)?
            .ok_or_else(instruction_invalid)?;
        if version < 21 {
            let new_vocabulary = match &record.request {
                Operation::SubmitTask { input, .. } => has_instruction_source(input),
                Operation::DelegateTask { child, .. } => has_instruction_source(child),
                Operation::CallCollaboration { call } => {
                    matches!(record.outcome, Outcome::Ok { .. })
                        && call.tool.starts_with("collab_repository_")
                        && ["source", "base_source"].iter().any(|key| {
                            call.arguments.get(*key).and_then(serde_json::Value::as_str)
                                == Some("instructions")
                        })
                }
                _ => false,
            };
            if new_vocabulary {
                return Err(instruction_invalid());
            }
        }
        let Outcome::Ok { data } = &record.outcome else {
            continue;
        };
        match data {
            Data::Commands(commands) => {
                for command in commands {
                    instruction_command(tx, command, version)?;
                }
            }
            Data::Task(task) => {
                retained_instruction_input(tx, &task.input, version)?;
                let current = super::sessions::historical_task(tx,&task.id,&task.session_id)?;
                if instruction_selection(&task.input) != instruction_selection(&current.input)
                    || has_instruction_source(&task.input)
                        && task.input.resolved_limits != current.input.resolved_limits
                {
                    return Err(instruction_invalid());
                }
                if !has_instruction_source(&task.input) {
                    continue;
                }
                match &record.request {
                    Operation::SubmitTaskDraft { input, .. } => {
                        let original=input.resolve(task.input.definition.clone());
                        if !original_instruction_selection(&original, &task.input) || !admitted.insert(task.id.clone()) { return Err(instruction_invalid()); }
                    }
                    Operation::SubmitTask { input, .. }
                    | Operation::DelegateTask { child: input, .. } => {
                        if !original_instruction_selection(input, &task.input) {
                            return Err(instruction_invalid());
                        }
                        if !admitted.insert(task.id.clone()) {
                            return Err(instruction_invalid());
                        }
                    }
                    Operation::CallCollaboration { call }
                        if call.tool == "collab_task_delegate" =>
                    {
                        #[derive(serde::Deserialize)]
                        #[serde(deny_unknown_fields)]
                        struct Args {
                            instruction: String,
                        }
                        let args: Args = serde_json::from_value(call.arguments.clone())?;
                        let mut parent = load(tx, &call.caller.task_id)?.task;
                        let raw: String = tx.query_row(
                            "SELECT body FROM attempts WHERE id=?1 AND task_id=?2",
                            params![call.caller.attempt_id, parent.id],
                            |r| r.get(0),
                        )?;
                        let attempt: Attempt = decode(&raw)?;
                        parent=super::sessions::historical_task(tx,&parent.id,&attempt.session_id)?;
                        if attempt.id != call.caller.attempt_id
                            || attempt.task_id != parent.id
                            || attempt.session_id != parent.session_id
                            || attempt.generation != call.caller.generation
                            || attempt.worker_id != call.caller.worker_id
                            || attempt.worker_incarnation != call.caller.incarnation
                            || task.parent_task_id.as_ref() != Some(&parent.id)
                        {
                            return Err(instruction_invalid());
                        }
                        let expected =
                            crate::delegation::coding_child(&parent.input, &args.instruction)
                                .map_err(|_| instruction_invalid())?;
                        let wait: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM waits WHERE child=?1 AND parent=?2 AND parent_attempt=?3 AND parent_generation=?4 AND operation_id=?5)",
                            params![task.id, parent.id, attempt.id, integer(attempt.generation)?, call.operation_id], |r| r.get(0))?;
                        if instruction_selection(&expected) != instruction_selection(&task.input)
                            || !wait
                        {
                            return Err(instruction_invalid());
                        }
                        if !admitted.insert(task.id.clone()) {
                            return Err(instruction_invalid());
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
    if selected_tasks != admitted {
        return Err(instruction_invalid());
    }
    Ok(())
}
pub(super) fn validate_existing(tx: &StoreContext<'_>) -> Result<()> {
    let limits = &tx.limits.repository;
    let (heads, revisions, bytes): (i64,i64,i64) = tx.query_row("SELECT (SELECT count(*) FROM repositories),(SELECT count(*) FROM repository_revisions),(SELECT coalesce(sum(length(CAST(body AS BLOB))),0) FROM repository_revisions)", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    if unsigned(heads)? > size(limits.catalogue_entries)?
        || unsigned(revisions)? > size(limits.catalogue_revisions)?
        || unsigned(bytes)? > limits.catalogue_bytes
    {
        return Err(Fault::Store(
            "repository catalogue exceeds metadata capacity".into(),
        ));
    }
    let mut q = tx.prepare(
        "SELECT repository_id,revision FROM repository_revisions ORDER BY repository_id,revision",
    )?;
    for item in q.query_map([], |r| {
        Ok(Revision {
            repository_id: r.get(0)?,
            registration_revision: r.get(1)?,
        })
    })? {
        let binding = item?;
        revision(tx, &binding)?;
        head(tx, &binding.repository_id)?;
    }
    let mut q = tx.prepare("SELECT id FROM repositories ORDER BY id")?;
    for id in q.query_map([], |r| r.get::<_, String>(0))? {
        get(tx, &id?, None)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn database() -> Connection {
        let sql = Connection::open_in_memory().unwrap();
        // Deliberately no CHECK constraints: these are corrupt stored-value probes,
        // not a production schema or a new admissible state.
        sql.execute_batch("CREATE TABLE repositories(id TEXT PRIMARY KEY,current_revision TEXT NOT NULL,generation INTEGER NOT NULL,enabled INTEGER NOT NULL);").unwrap();
        sql
    }
    #[test]
    fn catalogue_sqlite_generation_boundary_roundtrips_and_refuses_overflow() {
        let sql = database();
        let limits = crate::limits::RuntimeLimits::seed().unwrap();
        let tx = StoreContext::new(&sql, &limits);
        sql.execute(
            "INSERT INTO repositories VALUES('repo','r1',?1,1)",
            [i64::MAX - 1],
        )
        .unwrap();
        let original = head(&tx, "repo").unwrap();
        let next = next_generation(&original, original.generation).unwrap();
        assert_eq!(next, i64::MAX);
        sql.execute(
            "UPDATE repositories SET generation=?1 WHERE id='repo'",
            [next],
        )
        .unwrap();
        let last = head(&tx, "repo").unwrap();
        assert_eq!(last.generation, u64::try_from(i64::MAX).unwrap());
        assert!(matches!(
            next_generation(&last, last.generation),
            Err(Fault::Refused("capacity", _))
        ));
        assert!(matches!(
            next_generation(&last, u64::MAX),
            Err(Fault::Refused("revision_conflict", _))
        ));
        assert_eq!(
            sql.query_row("SELECT generation FROM repositories", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            i64::MAX
        );
    }
    #[test]
    fn catalogue_head_refuses_negative_zero_noninteger_and_out_of_range_storage() {
        use rusqlite::types::Type;
        for (value, expected_type) in [
            ("-1", Type::Integer),
            ("0", Type::Integer),
            ("9223372036854775808", Type::Real),
            ("'corrupt'", Type::Text),
            ("1.5", Type::Real),
        ] {
            let sql = database();
            sql.execute_batch(&format!(
                "INSERT INTO repositories VALUES('repo','r1',{value},1)"
            ))
            .unwrap();
            let limits = crate::limits::RuntimeLimits::seed().unwrap();
            let tx = StoreContext::new(&sql, &limits);
            let before: rusqlite::types::Value = sql
                .query_row("SELECT generation FROM repositories", [], |r| r.get(0))
                .unwrap();
            assert_eq!(before.data_type(), expected_type, "stored generation {value}");
            let fault = match head(&tx, "repo") {
                Err(fault) => fault,
                Ok(_) => panic!("accepted corrupt stored generation {value}"),
            };
            // Invalid integer values and invalid SQLite storage types retain
            // their distinct errors; neither is an admissible catalogue head.
            let expected_refusal = match (&fault, expected_type) {
                (Fault::Store(message), Type::Integer) => match value {
                    "-1" => message == "negative stored unsigned integer",
                    "0" => message == "repository head generation must be positive",
                    _ => false,
                },
                (
                    Fault::Sqlite(rusqlite::Error::InvalidColumnType(index, column, actual_type)),
                    Type::Real | Type::Text,
                ) => *index == 1 && column == "generation" && *actual_type == expected_type,
                _ => false,
            };
            assert!(expected_refusal, "stored generation {value}: {fault:?}");
            assert!(head(&tx, "repo").is_err());
            let after: rusqlite::types::Value = sql
                .query_row("SELECT generation FROM repositories", [], |r| r.get(0))
                .unwrap();
            assert_eq!(after, before);
        }
        for enabled in [-1i64, 2] {
            let sql = database();
            sql.execute(
                "INSERT INTO repositories VALUES('repo','r1',1,?1)",
                [enabled],
            )
            .unwrap();
            let limits = crate::limits::RuntimeLimits::seed().unwrap();
            assert!(matches!(
                head(&StoreContext::new(&sql, &limits), "repo"),
                Err(Fault::Store(_))
            ));
        }
    }
}
