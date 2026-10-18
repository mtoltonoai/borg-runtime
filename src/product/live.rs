//! Bounded notifications and independent task/workflow history projections.
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
        limiting_field: None,
    }
}
pub(super) async fn get(controller: &Controller, path: &str, query: &str) -> Option<UiResponse> {
    if !["/ui/changes", "/ui/activity", "/activity", "/ui/workflow-activity"].contains(&path) {
        return None;
    }
    Some(match route(controller, path, query).await {
        Ok(r) => r,
        Err(r) => r,
    })
}
async fn route(controller: &Controller, path: &str, query: &str) -> Result<UiResponse, UiResponse> {
    if path == "/ui/workflow-activity" {
        return workflow_activity(controller, query).await;
    }
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

// Preserve the canonical typed event structure, including nested version and
// acceptance references. Decimal strings avoid rounding any durable integer in
// JavaScript; the opaque canonical cursor is deliberately not re-encoded here.
fn browser_numbers(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Number(number) => {
            *value = serde_json::Value::String(number.to_string());
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(browser_numbers),
        serde_json::Value::Object(fields) => fields.values_mut().for_each(browser_numbers),
        _ => {}
    }
}

async fn workflow_activity(controller: &Controller, query: &str) -> Result<UiResponse, UiResponse> {
    let limits = controller.limits();
    let policy = limits.workflow.as_ref().ok_or_else(|| error(404, "Workflow activity is not enabled"))?;
    let form = parse_form(query.as_bytes(), limits).map_err(|e| error(400, &e))?;
    fields_only(&form, &["cursor"]).map_err(|e| error(400, &e))?;
    let cursor = form.get("cursor").cloned();
    if let Some(cursor) = &cursor {
        if cursor.is_empty() {
            return Err(error(400, "Invalid workflow cursor"));
        }
        if cursor.len() > limits.controller.collaboration_cursor_bytes {
            return Err(error(400, &FormError::limit(failure::LimitingField::CollaborationCursor)));
        }
    }
    // The controller validates the cursor's database and exact existing event.
    let reply = call(controller, Operation::QueryWorkflowActivity {
        cursor,
        limit: limits.product.page_items.min(policy.page_items),
    }).await;
    let Outcome::Ok { data: Data::WorkflowActivity(activity) } = reply.outcome else {
        return Err(not_task(reply));
    };
    let has_more = activity.next_cursor.is_some();
    let cursor = match activity.next_cursor {
        Some(cursor) => Some(cursor),
        None => activity.through.as_ref().map(serde_json::to_string).transpose()
            .map_err(|_| error(502, "Workflow cursor could not be encoded"))?,
    };
    if cursor.as_ref().is_some_and(|cursor| cursor.len() > limits.controller.collaboration_cursor_bytes) {
        return Err(error(502, &FormError::limit(failure::LimitingField::CollaborationCursor)));
    }
    let mut projection = serde_json::json!({
        "items": activity.items, "through": activity.through,
        "cursor": cursor, "has_more": has_more,
    });
    browser_numbers(&mut projection);
    let response = json(projection);
    // This checks the final browser representation, after integer conversion and
    // its cursor envelope. Product finalization independently bounds every reply.
    if response.body.len() > policy.page_bytes {
        return Err(error(502, &FormError::limit(failure::LimitingField::WorkflowPage)));
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::browser_numbers;

    #[test]
    fn workflow_projection_preserves_large_nested_references_and_opaque_cursor() {
        let cursor = r#"{"database_id":"db","product_revision":9007199254740993,"ordinal":0}"#;
        let mut value = serde_json::json!({
            "cursor": cursor,
            "through": {"database_id":"db","product_revision":9007199254740993_u64,"ordinal":0},
            "items": [{"subject_revision":u64::MAX,"detail":{"kind":"review","change":{
                "kind":"requested","target":{"kind":"accepted_task_result","result":{
                    "terminal_task_revision":9007199254740995_u64,
                    "acceptance_history_sequence":9007199254740997_u64,
                    "checkpoint":null
                }}
            }},"truncated":false}]
        });
        browser_numbers(&mut value);
        assert_eq!(value["cursor"], cursor);
        assert_eq!(value["through"]["product_revision"], "9007199254740993");
        assert_eq!(value["through"]["ordinal"], "0");
        assert_eq!(value["items"][0]["subject_revision"], "18446744073709551615");
        let result = &value["items"][0]["detail"]["change"]["target"]["result"];
        assert_eq!(result["terminal_task_revision"], "9007199254740995");
        assert_eq!(result["acceptance_history_sequence"], "9007199254740997");
        assert!(result["checkpoint"].is_null());
        assert_eq!(value["items"][0]["truncated"], false);
        assert_eq!(value["items"][0]["detail"]["kind"], "review");
    }
}
