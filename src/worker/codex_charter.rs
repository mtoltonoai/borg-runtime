//! Initial supplied-context delivery, not a native model-request occurrence.
//! The original send marker survives loss of the executor or controller ACK.
use super::*;
use crate::charter::{CharterObservation, CharterOutcome, CharterQuery, CharterQueryResult,
    supplied_context_instructions};
use crate::protocol::{Data, Operation, Outcome, bounded_json_size};
use serde::Deserialize;
use std::io::Read;

/// Derived by the worker from the accepted Definition; never model input.
#[derive(Clone)]
pub struct Scope {
    /// Owned worker root for format admission and recoverable result storage.
    /// Executor dispatch journals stay in their separate per-attempt directory.
    pub worker_state: PathBuf,
    pub controller_url: String,
    pub caller: crate::mcp::Caller,
    pub session_id: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Dispatch {
    delivery_id: String,
    request_id: u64,
    /// An acknowledged thread is still not permission to start it again.
    thread_id: Option<String>,
}

fn directory(state: &Path) -> PathBuf { state.join("supplied-charter") }

fn inspect_dispatch(path: &Path, control: &Control) -> std::result::Result<bool, String> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(_) => return Err("supplied charter dispatch record unavailable; outcome uncertain".into()),
    };
    let bound = control.runtime_limits.worker.mutation_record_bytes;
    let mut bytes = Vec::new();
    file.take(bound as u64 + 1).read_to_end(&mut bytes)
        .map_err(|_| "supplied charter dispatch unreadable; outcome uncertain")?;
    if bytes.len() > bound { return Err("supplied charter dispatch exceeds retained bound; outcome uncertain".into()); }
    let _: Dispatch = serde_json::from_slice(&bytes)
        .map_err(|_| "supplied charter dispatch corrupt; outcome uncertain")?;
    Ok(true)
}

fn clean(value: &Value, credentials: &[String], control: &Control) -> std::result::Result<(), String> {
    if crate::worker::value_contains_credential(value, credentials, &control.runtime_limits.worker)? {
        return Err("supplied charter data contains a configured credential; withheld".into());
    }
    Ok(())
}

pub(super) async fn mutation_outcome(client: &crate::client::ApiClient, path: &Path, operation: Operation,
    control: &Control, credentials: &[String]) -> std::result::Result<Box<CharterOutcome>, String> {
    let value = serde_json::to_value(&operation).map_err(|_| "supplied charter operation encoding")?;
    clean(&value, credentials, control)?;
    let mut journal = match crate::worker::mutations::read(path, &control.runtime_limits)? {
        Some(journal) => {
            if serde_json::to_value(&journal.request).map_err(|_| "supplied charter journal encoding")? != value {
                return Err("supplied charter original operation differs; reconciliation required".into());
            }
            journal
        }
        None => crate::worker::mutations::create(path, operation, 0, &control.runtime_limits)?,
    };
    // Late acknowledgment records an observed result; cancelling it here would
    // discard evidence. The canonical owner validates the original occurrence.
    let outcome=crate::worker::mutations::resolve(client,path,&mut journal).await?;
    // Reconcile the same canonical outer intent/ACK after a lost response. This
    // cannot repeat an executor request; the durable dispatch marker fences it.
    let outcome=if outcome.is_none()&&matches!(&journal.request,Operation::ObserveCharter{
        observation:CharterObservation::BeginSuppliedTurn{..}|CharterObservation::AckSuppliedTurn{..},..}) {
        crate::worker::mutations::resolve(client,path,&mut journal).await?
    }else{outcome};
    match outcome {
        Some(Outcome::Ok { data: Data::Charter(value) }) => {
            crate::worker::mutations::consume(path, &mut journal)?;
            Ok(value)
        }
        Some(Outcome::Refused { code, .. }) => Err(format!("supplied charter operation refused: {code}")),
        Some(_) => Err("supplied charter operation response differs".into()),
        None => Err("supplied charter operation outcome uncertain; no executor resend".into()),
    }
}

// Traceability scope: initial new-thread delivery only. No mid-turn boundary or
// every-inner-response observability is inferred from an outer thread receipt.
//= spec/impl/v2/implementation.md#definitions-carry-reusable-charters
//# The supplied executor MUST deliver the effective charter to its model.
pub(super) async fn start_thread(session: &mut Session, scope: &Scope, state: &Path,
    mut config: Value, inference: &InferenceSettings, control: &Control, credentials: &[String])
    -> std::result::Result<Value, String> {
    control.check()?;
    let directory = directory(state);
    process::private_directory(&directory)?;
    let dispatch_path = directory.join("dispatch.json");
    if inspect_dispatch(&dispatch_path, control)? {
        return Err("original supplied charter context requires reconciliation; outcome uncertain; no second thread/start".into());
    }
    let client = crate::client::ApiClient::new(&scope.controller_url, control.runtime_limits.clone())?;
    let before = match crate::worker::controller_call(&client, Operation::QueryCharter {
        query: CharterQuery::Session { session_id: scope.session_id.clone() },
    }, control).await? {
        Data::CharterQuery(value) => match *value {
            CharterQueryResult::Session(Some(value)) => value,
            _ => return Err("accepted supplied charter session is absent".into()),
        },
        _ => return Err("accepted supplied charter session is absent".into()),
    };
    if before.session.session_id != scope.session_id { return Err("supplied charter session differs".into()); }
    let selected = before.session.pending.as_ref().unwrap_or(&before.session.effective).clone();
    let source = if selected == before.charter.reference { before.charter } else {
        match crate::worker::controller_call(&client, Operation::QueryCharter {
            query: CharterQuery::Source { source: selected.clone() },
        }, control).await? {
            Data::CharterQuery(value) => match *value {
                CharterQueryResult::Source(value) if value.reference == selected => value,
                _ => return Err("supplied charter source differs".into()),
            },
            _ => return Err("supplied charter source differs".into()),
        }
    };
    let instructions = supplied_context_instructions(&source).map_err(|_| "supplied charter rendering failed")?;
    config["developerInstructions"] = Value::String(instructions.clone());
    // Bound the actual complete RPC, not just its instruction substring.
    let request_id = session.next_id;
    if request_id == 0 || request_id == u64::MAX { return Err("supplied charter request identity exhausted".into()); }
    bounded_json_size(&json!({"id":request_id,"method":"thread/start","params":config}), session.limits.frame_bytes)
        .map_err(|_| "supplied charter request exceeds execution.coding.frame_bytes")?;
    clean(&config, credentials, control)?;
    let delivery_id = format!("{}-charter-supplied-initial", scope.caller.attempt_id);
    let expected_revision = before.session.revision.checked_add(1).ok_or("supplied charter binding overflow")?;
    let mut intent = mutation_outcome(&client, &directory.join("intent.json"), Operation::ObserveCharter {
        operation_id: delivery_id.clone(), caller: scope.caller.clone(),
        observation: CharterObservation::BeginSuppliedContext { session_id: scope.session_id.clone(),
            expected_revision: before.session.revision, source: selected, request_id,
            developer_instructions: instructions.clone() },
    }, control, credentials).await?;
    let CharterOutcome::SuppliedContext(context) = intent.as_ref() else {
        return Err("supplied charter original context response kind differs".into());
    };
    if context.delivery_id != delivery_id || context.caller != scope.caller || context.session_id != scope.session_id
        || context.binding_revision != expected_revision || context.effective != source
        || context.request_id != request_id || context.developer_instructions != instructions || context.acknowledgment.is_some() {
        return Err("supplied charter original intent acknowledgment differs".into());
    }
    control.check()?;
    // No await lies between this durable marker and the one original send.
    let mut dispatch = Dispatch { delivery_id: delivery_id.clone(), request_id, thread_id: None };
    process::atomic_json(&dispatch_path, &dispatch)
        .map_err(|e| format!("supplied charter dispatch persistence failed; outcome uncertain: {e}"))?;
    let result = async {
        let response = session.request("thread/start", config, control).await?;
        if response["model"].as_str() != Some(inference.model.as_str())
            || response["reasoningEffort"].as_str() != Some(inference.reasoning_effort.as_str()) {
            return Err("executor effective model or reasoning effort differs from selected profile".into());
        }
        let thread = response["thread"]["id"].as_str().ok_or("executor did not return a thread id")?;
        check_association_ids(thread, None, credentials)?;
        clean(&response, credentials, control)?;
        let ack_id = format!("{}-charter-supplied-ack", scope.caller.attempt_id);
        let acknowledged = mutation_outcome(&client, &directory.join("ack.json"), Operation::ObserveCharter {
            operation_id: ack_id.clone(), caller: scope.caller.clone(),
            observation: CharterObservation::AckSuppliedContext { delivery_id: delivery_id.clone(), request_id, response: response.clone() },
        }, control, credentials).await?;
        let CharterOutcome::SuppliedContext(expected) = intent.as_mut() else {
            return Err("supplied charter original context response kind differs".into());
        };
        expected.acknowledgment = Some(crate::charter::CharterSuppliedAcknowledgment {
            operation_id: ack_id, request_id, thread_id: thread.to_owned(), response: response.clone(),
        });
        if !matches!(acknowledged.as_ref(), CharterOutcome::SuppliedContext(_)) {
            return Err("supplied charter original context response kind differs".into());
        }
        if acknowledged != intent { return Err("supplied charter original result acknowledgment differs".into()); }
        dispatch.thread_id = Some(thread.to_owned());
        process::atomic_json(&dispatch_path, &dispatch)?;
        control.check()?;
        Ok(response)
    }.await;
    result.map_err(|e: String| format!("supplied charter context outcome uncertain; no second thread/start: {e}"))
}
