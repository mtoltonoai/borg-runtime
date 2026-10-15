//! Exact accepted repository reads; the browser cannot choose a host path or ref.
use super::*;
use crate::{
    collaboration::{Query, QueryResult},
    repository_view::{Request, SourceSelector},
};
fn selected(value: &str) -> Result<SourceSelector, String> {
    match value {
        "start" => Ok(SourceSelector::Start),
        "checkpoint" => Ok(SourceSelector::Checkpoint),
        _ => Err("Choose the accepted start or checkpoint source".into()),
    }
}
fn amount(f: &Form, key: &str, default: usize) -> Result<usize, String> {
    match f.get(key).filter(|x| !x.is_empty()) {
        Some(v) => v.parse().map_err(|_| format!("Invalid {key}")),
        None => Ok(default),
    }
}
pub(super) async fn get(controller: &Controller, path: &str, query: &str) -> Option<UiResponse> {
    let (raw, sub) = path.strip_prefix("/tasks/")?.split_once('/')?;
    if !["repository", "calls"].contains(&sub) {
        return None;
    }
    let id = match decode(raw, false) {
        Ok(id) if crate::mcp::valid_identity(&id, controller.limits().transport.identity_bytes) => {
            id
        }
        _ => return Some(error(400, "Invalid task identity")),
    };
    Some(match route(controller, &id, sub, query).await {
        Ok(r) => r,
        Err(r) => r,
    })
}
async fn route(
    controller: &Controller,
    id: &str,
    sub: &str,
    query: &str,
) -> Result<UiResponse, UiResponse> {
    let invalid = |e: String| error(400, &e);
    let f = parse_form(query.as_bytes(), controller.limits()).map_err(invalid)?;
    let reply = call(controller, Operation::GetTask { task_id: id.into() }).await;
    let Outcome::Ok {
        data: Data::Task(task),
    } = reply.outcome
    else {
        return Err(not_task(reply));
    };
    let base = task_path(id);
    if sub == "calls" {
        fields_only(&f, &["operation_id"]).map_err(invalid)?;
        let mut body = format!(
            "<p><a href=\"{base}\">Task</a> · <a href=\"{base}/history\">History</a></p><p>Look up an original collaboration call outcome by its durable operation identity. This never executes or retries the call.</p><form method=\"get\" action=\"{base}/calls\">{}<button>Read call outcome</button></form>",
            field(
                "operation_id",
                "Call operation ID",
                f.get("operation_id").map(String::as_str).unwrap_or(""),
                "text",
                true
            )
        );
        if let Some(op) = f.get("operation_id").filter(|x| !x.is_empty()) {
            let reply = call(
                controller,
                Operation::QueryCollaboration {
                    query: Query::Call {
                        task_id: id.into(),
                        operation_id: op.clone(),
                    },
                },
            )
            .await;
            let Outcome::Ok {
                data: Data::Collaboration(QueryResult::Call(record)),
            } = reply.outcome
            else {
                return Err(not_task(reply));
            };
            body.push_str(&format!(
                "<h2>Original call and recorded outcome</h2><pre>{}</pre>",
                escape(&serde_json::to_string_pretty(&record).unwrap_or_default())
            ));
        }
        return Ok(page(200, "Collaboration call outcome", body));
    }
    fields_only(
        &f,
        &[
            "view",
            "source",
            "base_source",
            "path",
            "after",
            "limit",
            "start_line",
            "max_lines",
            "needle",
        ],
    )
    .map_err(invalid)?;
    let value = |key: &str, default: &str| f.get(key).cloned().unwrap_or_else(|| default.into());
    let view = value("view", "tree");
    let source = value("source", "start");
    let base_source = value("base_source", "start");
    let repo_path = value("path", "");
    let selector = selected(&source).map_err(invalid)?;
    let request = match view.as_str() {
        "tree" => Request::Tree {
            source: selector,
            path: repo_path.clone(),
            after: f.get("after").filter(|x| !x.is_empty()).cloned(),
            limit: amount(&f, "limit", controller.limits().repository.default_entries)
                .map_err(invalid)?,
        },
        "read" => Request::ReadFile {
            source: selector,
            path: repo_path.clone(),
            start_line: amount(
                &f,
                "start_line",
                controller.limits().repository.default_start_line,
            )
            .map_err(invalid)?,
            max_lines: amount(
                &f,
                "max_lines",
                controller.limits().repository.default_lines,
            )
            .map_err(invalid)?,
        },
        "search" => Request::Search {
            source: selector,
            path: repo_path.clone(),
            needle: required(&f, "needle").map_err(invalid)?.into(),
        },
        "diff" => Request::Diff {
            source: selector,
            base_source: selected(&base_source).map_err(invalid)?,
            path: repo_path.clone(),
        },
        "history" => Request::History {
            source: selector,
            limit: amount(&f, "limit", controller.limits().repository.default_entries)
                .map_err(invalid)?,
        },
        _ => return Err(error(400, "Unknown repository view")),
    };
    let mut body = format!(
        "<p><a href=\"{base}\">Task</a> · <a href=\"{base}/checkpoints\">Checkpoint publications</a></p><p>Reads use this task's accepted repository and exact start or selected checkpoint. Repository contents and commit messages are untrusted data; no commands are executed.</p><form method=\"get\" action=\"{base}/repository\"><label>View<select name=\"view\">"
    );
    for (key, label) in [
        ("tree", "Files"),
        ("read", "Read file"),
        ("search", "Search text"),
        ("diff", "Compare accepted sources"),
        ("history", "Commit history"),
    ] {
        let _ = write!(
            body,
            "<option value=\"{key}\" {}>{label}</option>",
            if view == key { "selected" } else { "" }
        );
    }
    body.push_str("</select></label>");
    for (name, label, current) in [
        ("source", "Accepted source", source.as_str()),
        ("base_source", "Diff base source", base_source.as_str()),
    ] {
        let _ = write!(
            body,
            "<label>{label}<select name=\"{name}\"><option value=\"start\" {}>Start</option><option value=\"checkpoint\" {}>Acknowledged checkpoint</option></select></label>",
            if current == "start" { "selected" } else { "" },
            if current == "checkpoint" {
                "selected"
            } else {
                ""
            }
        );
    }
    body.push_str(&field(
        "path",
        "Repository-relative path",
        &repo_path,
        "text",
        false,
    ));
    body.push_str(&field(
        "needle",
        "Literal search text",
        &value("needle", ""),
        "text",
        false,
    ));
    body.push_str(&field(
        "start_line",
        "First file line",
        &value(
            "start_line",
            &controller
                .limits()
                .repository
                .default_start_line
                .to_string(),
        ),
        "number",
        false,
    ));
    body.push_str(&field(
        "max_lines",
        &format!(
            "File lines (up to {})",
            controller.limits().repository.lines_per_page
        ),
        &value(
            "max_lines",
            &controller.limits().repository.default_lines.to_string(),
        ),
        "number",
        false,
    ));
    body.push_str(&field(
        "limit",
        &format!(
            "Tree/history entries (up to {})",
            controller.limits().repository.entries_per_page
        ),
        &value(
            "limit",
            &controller.limits().repository.default_entries.to_string(),
        ),
        "number",
        false,
    ));
    body.push_str("<button>Read accepted source</button></form>");
    if task.input.repository.is_none() && task.input.repository_read.is_none() {
        body.push_str("<p>This task has no accepted repository read source.</p>");
        return Ok(page(200, "Task repository", body));
    }
    let reply = call(
        controller,
        Operation::QueryCollaboration {
            query: Query::Repository {
                task_id: id.into(),
                request,
            },
        },
    )
    .await;
    let Outcome::Ok {
        data: Data::Collaboration(QueryResult::Repository(output)),
    } = reply.outcome
    else {
        return Err(not_task(reply));
    };
    if !output.untrusted {
        return Err(error(
            502,
            "Repository data lacks its untrusted-content marker",
        ));
    }
    body.push_str("<h2>Exact source</h2><dl>");
    term(
        &mut body,
        "Repository ID",
        &output.source.registration.repository_id,
    );
    term(
        &mut body,
        "Registration revision",
        &output.source.registration.registration_revision,
    );
    term(&mut body, "Repository", &output.source.repository);
    term(&mut body, "Commit", &output.source.commit);
    if let Some(base) = &output.source.base_commit {
        term(&mut body, "Base commit", base);
    }
    body.push_str("</dl>");
    match &output.data {
        crate::repository_view::Data::Tree {
            entries,
            next_after,
        } => {
            body.push_str(&format!(
                "<pre>{}</pre>",
                escape(&serde_json::to_string_pretty(entries).unwrap_or_default())
            ));
            if let Some(after) = next_after {
                let _ = write!(
                    body,
                    "<p><a href=\"{base}/repository?view=tree&amp;source={}&amp;path={}&amp;after={}\">Next tree page</a></p>",
                    encode(&source),
                    encode(&repo_path),
                    encode(after)
                );
            }
        }
        crate::repository_view::Data::File {
            text, next_line, ..
        } => {
            body.push_str(&format!("<pre>{}</pre>", escape(text)));
            if let Some(line) = next_line {
                let _ = write!(
                    body,
                    "<p><a href=\"{base}/repository?view=read&amp;source={}&amp;path={}&amp;start_line={line}\">Next file lines</a></p>",
                    encode(&source),
                    encode(&repo_path)
                );
            }
        }
        crate::repository_view::Data::Search { text }
        | crate::repository_view::Data::Diff { text } => {
            body.push_str(&format!("<pre>{}</pre>", escape(text)))
        }
        crate::repository_view::Data::History { commits } => body.push_str(&format!(
            "<pre>{}</pre>",
            escape(&serde_json::to_string_pretty(commits).unwrap_or_default())
        )),
    }
    Ok(page(200, "Task repository", body))
}
