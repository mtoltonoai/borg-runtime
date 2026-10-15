//! Operator forms use canonical controller operations; reads expose only Entry.
use super::*;
use crate::repository_catalogue::{Entry, Registration, Revision};

const REGISTRATION_FIELDS: &[&str] = &[
    "operation_id",
    "repository_id",
    "registration_revision",
    "display_name",
    "source_endpoint",
    "checkpoint_endpoint",
    "source_store_key",
    "checkpoint_store_key",
];

fn identity_field(fields: &Form, key: &str, metadata: &RuntimeLimits) -> Result<String, String> {
    let value = required(fields, key)?;
    if !crate::mcp::valid_identity(value, metadata.transport.identity_bytes) {
        return Err(format!(
            "{key} exceeds transport.identity_bytes or is not a valid identity"
        ));
    }
    Ok(value.into())
}

pub(super) fn binding(
    fields: &Form,
    id: &str,
    revision: &str,
    metadata: &RuntimeLimits,
) -> Result<Revision, String> {
    Ok(Revision {
        repository_id: identity_field(fields, id, metadata)?,
        registration_revision: identity_field(fields, revision, metadata)?,
    })
}

// Field policy belongs to the controller after exact receipt lookup. Keeping
// this parser structural allows an original request to replay after policy
// changes, provided the HTTP/form transport still admits its bytes.
fn registration(fields: &Form) -> Result<Registration, String> {
    Ok(Registration {
        repository_id: required(fields, "repository_id")?.into(),
        registration_revision: required(fields, "registration_revision")?.into(),
        display_name: required(fields, "display_name")?.into(),
        source_endpoint: required(fields, "source_endpoint")?.into(),
        checkpoint_endpoint: required(fields, "checkpoint_endpoint")?.into(),
        source_store_key: required(fields, "source_store_key")?.into(),
        checkpoint_store_key: required(fields, "checkpoint_store_key")?.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registration_fields() -> Form {
        Form::from([
            ("operation_id".into(), "op".into()),
            ("repository_id".into(), "repo".into()),
            ("registration_revision".into(), "r1".into()),
            ("display_name".into(), "ééé".into()),
            ("source_endpoint".into(), "source".into()),
            ("checkpoint_endpoint".into(), "check".into()),
            ("source_store_key".into(), "s.git".into()),
            ("checkpoint_store_key".into(), "c.git".into()),
        ])
    }

    #[test]
    fn registration_parsing_preserves_exact_payload_while_form_hints_follow_metadata() {
        let mut metadata = RuntimeLimits::seed().unwrap();
        let fields = registration_fields();
        let original = registration(&fields).unwrap();
        metadata.transport.identity_bytes = 5;
        metadata.repository.endpoint_bytes = 4;
        metadata.repository.catalogue_bytes = 1;
        // The controller owns the semantic refusal after receipt lookup.
        assert_eq!(registration(&fields).unwrap(), original);
        let page = registration_form(&fields, false, None, &metadata);
        assert!(
            page.body
                .contains("name=\"display_name\" type=\"text\" value=\"ééé\" maxlength=\"5\"")
        );
        assert!(
            page.body.contains(
                "name=\"source_endpoint\" type=\"text\" value=\"source\" maxlength=\"4\""
            )
        );
        assert!(registration(&Form::new()).is_err());
    }

    #[test]
    fn registration_views_escape_public_metadata_and_preserve_unresolved_operation_identity() {
        let entry = Entry {
            repository_id: "repo/<x>".into(),
            registration_revision: "r&1".into(),
            display_name: "<script>label</script>".into(),
            source_endpoint: "https://example.test/source".into(),
            checkpoint_endpoint: "https://example.test/checkpoint".into(),
            current_revision: "r2".into(),
            generation: 2,
            enabled_for_new_tasks: true,
        };
        let view = detail(&entry);
        assert!(view.body.contains("&lt;script&gt;label&lt;/script&gt;"));
        assert!(!view.body.contains("<script>label"));
        assert!(!view.body.contains("source_store_key"));
        assert!(!view.body.contains("checkpoint_store_key"));
        assert!(view.body.contains("/repositories/id/repo%2F%3Cx%3E/revise"));
        assert!(
            view.body
                .contains("name=\"expected_generation\" value=\"2\"")
        );
        // Mutating forms do not refresh into a new operation identity.
        assert!(view.body.contains("data-live-refresh=\"false\""));
        let response = mutation_reply_for(
            Reply::refused("request", "store_unavailable", "reconcile"),
            "original-registration",
            None,
            "/repositories",
        );
        assert_eq!(response.status, 503);
        assert!(response.body.contains("/operations/original-registration"));
        assert!(response.location.is_none());
    }

    #[test]
    fn task_forms_require_explicit_registration_and_never_supply_endpoints() {
        let metadata = RuntimeLimits::seed().unwrap();
        let mut fields = create_defaults(ExecutorKind::Coding, &metadata).unwrap();
        for (key, value) in [
            ("instruction", "Work"),
            ("definition_id", "agent"),
            ("definition_revision", "a1"),
            ("start_commit", "1111111111111111111111111111111111111111"),
        ] {
            fields.insert(key.into(), value.into());
        }
        assert!(task_input(&fields, &metadata).is_err());
        fields.insert("repository_id".into(), "repo".into());
        assert!(task_input(&fields, &metadata).is_err());
        fields.insert("registration_revision".into(), "r1".into());
        let input = task_input(&fields, &metadata).unwrap();
        let repo = input.repository.unwrap();
        assert_eq!(
            repo.registration.unwrap(),
            Revision {
                repository_id: "repo".into(),
                registration_revision: "r1".into()
            }
        );
        assert!(repo.url.is_empty());
        assert!(repo.checkpoint_remote.is_empty());
        fields.insert("repository".into(), "model-supplied-url".into());
        assert!(task_input(&fields, &metadata).is_err());
        fields.remove("repository");
        fields.insert("repository_read_id".into(), "other".into());
        assert!(task_input(&fields, &metadata).is_err());
        for key in ["repository_id", "registration_revision", "start_commit"] {
            fields.remove(key);
        }
        fields.insert("executor".into(), "native".into());
        fields.insert("acceptance_commands".into(), "[]".into());
        fields.insert("output_schema".into(), "{\"type\":\"object\"}".into());
        assert!(task_input(&fields, &metadata).is_err());
        fields.insert("repository_read_revision".into(), "r2".into());
        fields.insert("repository_read_commit".into(), "a".repeat(40));
        let read = task_input(&fields, &metadata)
            .unwrap()
            .repository_read
            .unwrap();
        assert_eq!(read.registration.unwrap().repository_id, "other");
        assert!(read.repository.is_empty());
    }
}

pub(super) fn entry_path(id: &str, revision: Option<&str>) -> String {
    let mut path = format!("/repositories/id/{}", encode(id));
    if let Some(revision) = revision {
        let _ = write!(path, "?registration_revision={}", encode(revision));
    }
    path
}

fn registration_form(
    fields: &Form,
    revise: bool,
    problem: Option<&str>,
    metadata: &RuntimeLimits,
) -> UiResponse {
    let value = |key: &str| fields.get(key).map(String::as_str).unwrap_or("");
    let action = if revise {
        format!("{}/revise", entry_path(value("repository_id"), None))
    } else {
        "/repositories".into()
    };
    let mut body = String::from(
        "<p><a href=\"/repositories\">Repository catalogue</a></p><p>Register already provisioned repository material. Registration does not acquire repositories, change access, or prove availability. Source and checkpoint destinations remain separate.</p>",
    );
    if let Some(problem) = problem {
        let _ = write!(body, "<p class=\"notice\">{}</p>", escape(problem));
    }
    let _ = write!(
        body,
        "<form method=\"post\" action=\"{}\">{}",
        escape(&action),
        hidden("operation_id", value("operation_id"))
    );
    if revise {
        body.push_str(&hidden("expected_generation", value("expected_generation")));
        body.push_str(&hidden("repository_id", value("repository_id")));
        let _ = write!(
            body,
            "<p>Repository ID: <code>{}</code>. Expected generation: {}. Supply a new immutable revision and both storage keys explicitly. Existing tasks retain their accepted revision.</p>",
            escape(value("repository_id")),
            escape(value("expected_generation"))
        );
    }
    for (name, label) in [
        ("repository_id", "Repository ID"),
        (
            "registration_revision",
            "New immutable registration revision",
        ),
        ("display_name", "Display name"),
        ("source_endpoint", "Source endpoint"),
        ("checkpoint_endpoint", "Checkpoint endpoint"),
        ("source_store_key", "Source storage key"),
        ("checkpoint_store_key", "Checkpoint storage key"),
    ] {
        if revise && name == "repository_id" {
            continue;
        }
        let bound = if name.ends_with("_endpoint") {
            metadata.repository.endpoint_bytes
        } else {
            metadata.transport.identity_bytes
        };
        let _ = write!(
            body,
            "<label>{}<input name=\"{}\" type=\"text\" value=\"{}\" maxlength=\"{bound}\" required><small>At most {bound} UTF-8 bytes.</small></label>",
            escape(label),
            name,
            escape(value(name))
        );
    }
    let _ = write!(
        body,
        "<button type=\"submit\">{}</button></form>",
        if revise {
            "Record revision"
        } else {
            "Register repository"
        }
    );
    page(
        if problem.is_some() { 400 } else { 200 },
        if revise {
            "Revise repository"
        } else {
            "Register repository"
        },
        body,
    )
}

fn detail(entry: &Entry) -> UiResponse {
    let path = entry_path(&entry.repository_id, None);
    let mut body = String::from(
        "<p><a href=\"/repositories\">Repository catalogue</a> · <a href=\"/tasks/new\">Create task</a></p><p>Metadata is not an availability or access guarantee. Tasks must select an explicit repository ID, immutable registration revision and exact commit. Disabling new selection does not revoke accepted tasks.</p><dl>",
    );
    for (label, value) in [
        ("Repository ID", entry.repository_id.as_str()),
        (
            "Registration revision",
            entry.registration_revision.as_str(),
        ),
        ("Display name", entry.display_name.as_str()),
        ("Source endpoint", entry.source_endpoint.as_str()),
        ("Checkpoint endpoint", entry.checkpoint_endpoint.as_str()),
        ("Current revision", entry.current_revision.as_str()),
    ] {
        let _ = write!(body, "<dt>{label}</dt><dd>{}</dd>", escape(value));
    }
    let _ = write!(
        body,
        "<dt>Generation</dt><dd>{}</dd><dt>Enabled for new tasks</dt><dd>{}</dd></dl><p><a href=\"{}/revise\">Record a new immutable revision</a> · <a href=\"{}\">Refresh current registration</a></p><form method=\"get\" action=\"{}\">{}<button>Read exact revision</button></form><form method=\"post\" action=\"{}/enabled\">{}{}{}<button>{}</button></form>",
        entry.generation,
        entry.enabled_for_new_tasks,
        escape(&path),
        escape(&path),
        escape(&path),
        field(
            "registration_revision",
            "Immutable revision to inspect",
            &entry.registration_revision,
            "text",
            true
        ),
        escape(&path),
        hidden("operation_id", &identity()),
        hidden("expected_generation", &entry.generation.to_string()),
        hidden(
            "enabled_for_new_tasks",
            if entry.enabled_for_new_tasks {
                "false"
            } else {
                "true"
            }
        ),
        if entry.enabled_for_new_tasks {
            "Disable new task selection"
        } else {
            "Enable new task selection"
        }
    );
    page(200, "Repository registration", body)
}

pub(super) async fn get(controller: &Controller, path: &str, query: &str) -> UiResponse {
    let metadata = controller.limits();
    let fields = match parse_form(query.as_bytes(), metadata) {
        Ok(v) => v,
        Err(e) => return error(400, &e),
    };
    if path == "/repositories" {
        if let Err(e) = fields_only(&fields, &["after", "limit"]) {
            return error(400, &e);
        }
        let after = match fields.get("after") {
            Some(value) if crate::mcp::valid_identity(value, metadata.transport.identity_bytes) => {
                Some(value.clone())
            }
            Some(_) => return error(400, "Invalid repository cursor"),
            None => None,
        };
        let limit = match fields.get("limit") {
            Some(value) => match value.parse::<usize>() {
                Ok(n) if n > 0 && n <= metadata.controller.collaboration_page_items => n,
                _ => {
                    return error(
                        400,
                        "Limit exceeds controller.collaboration_page_items or is invalid",
                    );
                }
            },
            None => metadata.controller.collaboration_page_items,
        };
        let reply = call(controller, Operation::ListRepositories { after, limit }).await;
        let Outcome::Ok {
            data: Data::RepositoryRegistrations(entries),
        } = reply.outcome
        else {
            return not_task(reply);
        };
        let mut body = String::from(
            "<p><a href=\"/repositories/new\">Register repository</a></p><p>Choose explicit IDs and immutable revisions when creating tasks. Duplicate display names do not identify the same repository. Registration does not prove material is available.</p><table><thead><tr><th>Repository ID</th><th>Display name</th><th>Current revision</th><th>Enabled for new tasks</th></tr></thead><tbody>",
        );
        for entry in entries.items {
            let _ = write!(
                body,
                "<tr><td><a href=\"{}\">{}</a></td><td>{}</td><td>{}</td><td>{}</td></tr>",
                escape(&entry_path(
                    &entry.repository_id,
                    Some(&entry.registration_revision)
                )),
                escape(&entry.repository_id),
                escape(&entry.display_name),
                escape(&entry.current_revision),
                entry.enabled_for_new_tasks
            );
        }
        body.push_str("</tbody></table>");
        if let Some(after) = entries.next_cursor {
            let _ = write!(
                body,
                "<p><a href=\"/repositories?after={}&amp;limit={limit}\">Next page</a></p>",
                encode(&after)
            );
        }
        return page(200, "Repository catalogue", body);
    }
    if path == "/repositories/new" {
        if !fields.is_empty() {
            return error(400, "Registration drafts do not accept query fields");
        }
        return registration_form(
            &Form::from([("operation_id".into(), identity())]),
            false,
            None,
            metadata,
        );
    }
    let Some(raw) = path.strip_prefix("/repositories/id/") else {
        return error(404, "Unknown repository view");
    };
    let (raw, revise) = match raw.strip_suffix("/revise") {
        Some(id) => (id, true),
        None => (raw, false),
    };
    if raw.contains('/') {
        return error(404, "Unknown repository view");
    }
    let id = match decode(raw, false) {
        Ok(id) if crate::mcp::valid_identity(&id, metadata.transport.identity_bytes) => id,
        _ => return error(400, "Invalid repository ID"),
    };
    if let Err(e) = fields_only(
        &fields,
        if revise {
            &[]
        } else {
            &["registration_revision"]
        },
    ) {
        return error(400, &e);
    }
    let revision = match fields.get("registration_revision") {
        Some(value) if crate::mcp::valid_identity(value, metadata.transport.identity_bytes) => {
            Some(value.clone())
        }
        Some(_) => return error(400, "Invalid registration revision"),
        None => None,
    };
    let reply = call(
        controller,
        Operation::GetRepository {
            repository_id: id,
            registration_revision: revision,
        },
    )
    .await;
    let Outcome::Ok {
        data: Data::RepositoryRegistration(entry),
    } = reply.outcome
    else {
        return not_task(reply);
    };
    if !revise {
        return detail(&entry);
    }
    let fields = Form::from([
        ("operation_id".into(), identity()),
        ("repository_id".into(), entry.repository_id),
        ("expected_generation".into(), entry.generation.to_string()),
        ("display_name".into(), entry.display_name),
        ("source_endpoint".into(), entry.source_endpoint),
        ("checkpoint_endpoint".into(), entry.checkpoint_endpoint),
    ]);
    registration_form(&fields, true, None, metadata)
}

pub(super) async fn post(
    controller: &Controller,
    path: &str,
    fields: Form,
    operation_id: &str,
) -> UiResponse {
    let metadata = controller.limits();
    let command = (|| -> Result<Operation, String> {
        if path == "/repositories" {
            fields_only(&fields, REGISTRATION_FIELDS)?;
            return Ok(Operation::RegisterRepository {
                operation_id: operation_id.into(),
                registration: registration(&fields)?,
            });
        }
        let (raw, action) = path
            .strip_prefix("/repositories/id/")
            .and_then(|p| p.split_once('/'))
            .ok_or("Unknown repository action")?;
        let id = decode(raw, false)?;
        let expected_generation: u64 = number(&fields, "expected_generation")?;
        if expected_generation == 0 || expected_generation > i64::MAX as u64 {
            return Err("Invalid expected_generation".into());
        }
        match action {
            "revise" => {
                let mut allowed = REGISTRATION_FIELDS.to_vec();
                allowed.push("expected_generation");
                fields_only(&fields, &allowed)?;
                let registration = registration(&fields)?;
                if registration.repository_id != id {
                    return Err("Repository ID differs from the form destination".into());
                }
                Ok(Operation::ReviseRepository {
                    operation_id: operation_id.into(),
                    expected_generation,
                    registration,
                })
            }
            "enabled" => {
                fields_only(
                    &fields,
                    &[
                        "operation_id",
                        "expected_generation",
                        "enabled_for_new_tasks",
                    ],
                )?;
                let enabled_for_new_tasks = match required(&fields, "enabled_for_new_tasks")? {
                    "true" => true,
                    "false" => false,
                    _ => return Err("Invalid enabled_for_new_tasks".into()),
                };
                Ok(Operation::SetRepositoryEnabled {
                    operation_id: operation_id.into(),
                    repository_id: id,
                    expected_generation,
                    enabled_for_new_tasks,
                })
            }
            _ => Err("Unknown repository action".into()),
        }
    })();
    match command {
        Ok(command) => mutation_reply_for(
            call(controller, command).await,
            operation_id,
            None,
            "/repositories",
        ),
        Err(e) if path == "/repositories" || path.ends_with("/revise") => {
            registration_form(&fields, path.ends_with("/revise"), Some(&e), metadata)
        }
        Err(e) => error(400, &e),
    }
}
