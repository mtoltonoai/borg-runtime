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
            {
                return refuse(
                    "repository_binding_mismatch",
                    "accepted repository endpoints differ from registration revision",
                );
            }
        } else {
            if !repo.url.is_empty() || !repo.checkpoint_remote.is_empty() {
                return refuse(
                    "invalid_input",
                    "external submission cannot supply resolved repository endpoints",
                );
            }
            repo.url = registration.source_endpoint;
            repo.checkpoint_remote = registration.checkpoint_endpoint;
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
    if let Some(NativeWorkflow::CodingThenContinue { child }) = input.native_workflow.as_deref_mut()
    {
        let registration = selected(tx, child.repository.registration.as_ref(), !accepted_child)?;
        if accepted_child {
            if child.repository.url != registration.source_endpoint
                || child.repository.checkpoint_remote != registration.checkpoint_endpoint
            {
                return refuse(
                    "repository_binding_mismatch",
                    "configured child endpoints differ from accepted registration",
                );
            }
        } else {
            if !child.repository.url.is_empty() || !child.repository.checkpoint_remote.is_empty() {
                return refuse(
                    "invalid_input",
                    "external workflow cannot supply resolved repository endpoints",
                );
            }
            child.repository.url = registration.source_endpoint;
            child.repository.checkpoint_remote = registration.checkpoint_endpoint;
        }
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
        for value in ["-1", "0", "9223372036854775808", "'corrupt'", "1.5"] {
            let sql = database();
            sql.execute_batch(&format!(
                "INSERT INTO repositories VALUES('repo','r1',{value},1)"
            ))
            .unwrap();
            let limits = crate::limits::RuntimeLimits::seed().unwrap();
            let tx = StoreContext::new(&sql, &limits);
            assert!(
                matches!(head(&tx, "repo"), Err(Fault::Store(_))),
                "stored generation {value}"
            );
            let before: rusqlite::types::Value = sql
                .query_row("SELECT generation FROM repositories", [], |r| r.get(0))
                .unwrap();
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
