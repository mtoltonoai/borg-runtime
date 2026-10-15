//! Bounded notification and durable existing-history projection for the browser.
use super::*;
use crate::collaboration::{Query, QueryResult};

fn fields(query: &str, metadata: &RuntimeLimits) -> Result<Form, UiResponse> {
    let f = parse_form(query.as_bytes(), metadata).map_err(|e| error(400, &e))?;
    fields_only(&f, &["after"]).map_err(|e| error(400, &e))?;
    Ok(f)
}
fn after(f: &Form) -> Result<Option<u64>, UiResponse> {
    f.get("after")
        .map(|value| {
            let n = value
                .parse::<u64>()
                .map_err(|_| error(400, "Invalid durable cursor"))?;
            if n > i64::MAX as u64 || n.to_string() != *value {
                return Err(error(400, "Invalid durable cursor"));
            }
            Ok(n)
        })
        .transpose()
}
fn json(value: serde_json::Value) -> UiResponse {
    UiResponse {
        status: 200,
        content_type: "application/json".into(),
        body: value.to_string(),
        location: None,
    }
}
pub(super) async fn get(controller: &Controller, path: &str, query: &str) -> Option<UiResponse> {
    if !["/ui/changes", "/ui/activity", "/activity"].contains(&path) {
        return None;
    }
    Some(match route(controller, path, query).await {
        Ok(r) => r,
        Err(r) => r,
    })
}
async fn route(controller: &Controller, path: &str, query: &str) -> Result<UiResponse, UiResponse> {
    let f = fields(query, controller.limits())?;
    let cursor = after(&f)?;
    if path == "/ui/changes" {
        let after =
            cursor.ok_or_else(|| error(400, "Change observation requires a durable revision"))?;
        let change = controller
            .wait_for_change(after)
            .await
            .map_err(|_| error(503, "Live changes are temporarily unavailable"))?;
        // JavaScript must never round a durable u64 identity through f64.
        return Ok(json(
            serde_json::json!({"revision":change.revision.to_string(),"changed":change.changed,"reset":change.reset}),
        ));
    }
    let reply = call(
        controller,
        Operation::QueryCollaboration {
            query: Query::Activity {
                after: cursor,
                limit: controller.limits().product.page_items,
            },
        },
    )
    .await;
    let Outcome::Ok {
        data: Data::Collaboration(QueryResult::Activity(activity)),
    } = reply.outcome
    else {
        return Err(not_task(reply));
    };
    if path == "/ui/activity" {
        let items:Vec<_>=activity.items.iter().map(|entry|serde_json::json!({"sequence":entry.sequence.to_string(),"task_id":entry.task_id,"task_revision":entry.task_revision.to_string(),"kind":entry.kind})).collect();
        return Ok(json(
            serde_json::json!({"items":items,"cursor":activity.cursor.to_string(),"has_more":activity.has_more}),
        ));
    }
    let mut body = String::from(
        "<p>Recorded task activity from the controller's existing durable history. This is not a complete provider transcript or a log of unrecorded host activity.</p><ol>",
    );
    for entry in activity.items.iter().rev() {
        let _ = write!(
            body,
            "<li><a href=\"{}/history\">{}</a> · {} · task revision {} · event {}</li>",
            task_path(&entry.task_id),
            escape(&entry.kind.replace('_', " ")),
            escape(&entry.task_id),
            entry.task_revision,
            entry.sequence
        );
    }
    body.push_str("</ol>");
    if activity.items.is_empty() {
        body.push_str("<p>No activity on this page.</p>");
    }
    if activity.has_more {
        let _ = write!(
            body,
            "<p><a href=\"/activity?after={}\">Continue recorded activity</a></p>",
            activity.cursor
        );
    }
    Ok(page(200, "Borg activity", body))
}
