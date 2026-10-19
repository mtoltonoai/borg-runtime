//! Canonical charter turn operations. Every mutation is retained in the
//! existing worker journal and resolved by its original operation identity.
//! No provider request is retried or reconstructed by this module.
use super::*;
use crate::charter::{
    CharterBoundary, CharterObservation, CharterOutcome, CharterQuery, CharterQueryResult,
    CharterReplyObservation, CharterResultReceipt, EffectiveCharter,
};

pub(super) struct Turn {
    pub boundary: CharterBoundary,
    rendered_changed: bool,
    pub exchange: Arc<crate::native::CharterExchange>,
}

pub(super) fn caller(attempt: &Attempt) -> crate::mcp::Caller {
    crate::mcp::Caller {
        task_id: attempt.task_id.clone(),
        attempt_id: attempt.id.clone(),
        generation: attempt.generation,
        worker_id: attempt.worker_id.clone(),
        incarnation: attempt.worker_incarnation.clone(),
    }
}

pub(super) async fn session(
    reporter: &Reporter,
    input: &TaskInput,
    control: &Control,
) -> Result<Option<EffectiveCharter>, String> {
    if input.definition.charter.is_none() {
        return Ok(None);
    }
    match controller_call(
        &reporter.client,
        Operation::QueryCharter {
            query: CharterQuery::Session {
                session_id: reporter.attempt.session_id.clone(),
            },
        },
        control,
    )
    .await?
    {
        Data::CharterQuery(value) => match *value {
            CharterQueryResult::Session(value) => Ok(value),
            _ => Err("charter session response differs".into()),
        },
        _ => Err("charter session response differs".into()),
    }
}

pub(super) async fn mutate(
    reporter: &Reporter,
    state: &Path,
    control: &Control,
    operation_id: String,
    observation: CharterObservation,
) -> Result<Box<CharterOutcome>, String> {
    mutate_original(
        &reporter.client,
        state,
        &control.runtime_limits,
        caller(&reporter.attempt),
        operation_id,
        observation,
    )
    .await
}

/// Retain only an original observation; also used by startup after selecting its
/// immutable RunAttempt policy. It never begins execution or a provider request.
async fn mutate_original(
    client: &ApiClient,
    state: &Path,
    limits: &RuntimeLimits,
    original: crate::mcp::Caller,
    operation_id: String,
    observation: CharterObservation,
) -> Result<Box<CharterOutcome>, String> {
    let request = Operation::ObserveCharter {
        operation_id: operation_id.clone(),
        caller: original,
        observation,
    };
    let value = serde_json::to_value(&request).map_err(|_| "charter observation encoding")?;
    if value_contains_credential(&value, &configured_credentials(limits)?, &limits.worker)? {
        return Err("charter observation contains a configured credential".into());
    }
    let directory = state.join("charter");
    process::private_directory(&directory)?;
    let path = directory.join(format!("{}.json", component(&operation_id)));
    let mut journal = match mutations::read(&path, limits)? {
        Some(journal) => {
            if serde_json::to_value(&journal.request).map_err(|_| "charter observation encoding")?
                != value
            {
                return Err("charter occurrence body changed".into());
            }
            journal
        }
        None => mutations::create(&path, request, 0, limits)?,
    };
    // Recording an already-observed reply does not execute anything. Controller
    // validates its original occurrence and caller; no local cancellation check
    // drops the original reply/audit before its durable journal is written.
    match mutations::resolve(client, &path, &mut journal).await? {
        Some(Outcome::Ok {
            data: Data::Charter(value),
        }) => {
            mutations::consume(&path, &mut journal)?;
            Ok(value)
        }
        Some(Outcome::Refused { code, .. }) => Err(format!("charter observation refused: {code}")),
        Some(_) => Err("charter observation response differs".into()),
        None => Err("charter occurrence outcome uncertain; later execution withheld".into()),
    }
}

pub(super) async fn begin(
    reporter: &Reporter,
    input: &TaskInput,
    state: &Path,
    control: &Control,
    index: u32,
) -> Result<Option<Turn>, String> {
    begin_same_input(reporter, input, state, control, index, None).await
}

/// A retry gets a new canonical physical occurrence and fresh capture. Its
/// source and rendering mode remain those of the first actual logical input.
/// This does not authorize retry: C's original rejection and body binding do.
pub(super) async fn begin_same_input(
    reporter: &Reporter,
    input: &TaskInput,
    state: &Path,
    control: &Control,
    index: u32,
    original: Option<&Turn>,
) -> Result<Option<Turn>, String> {
    if input.definition.charter.is_none() {
        return Ok(None);
    }
    control.check()?;
    let before = session(reporter, input, control).await?;
    // A delegated parent resumes under a newly current attempt. Only the
    // controller can join the original call receipts, settled wait and actual
    // RunAttempt ChildOutcome. Public locator tuples alone cannot close it.
    if let Some(id) = before
        .as_ref()
        .and_then(|v| v.session.last_turn_id.as_ref())
    {
        let previous = match controller_call(
            &reporter.client,
            Operation::QueryCharter {
                query: CharterQuery::Turn {
                    turn_id: id.clone(),
                },
            },
            control,
        )
        .await?
        {
            Data::CharterQuery(value) => match *value {
                CharterQueryResult::Turn(value) => value,
                _ => return Err("prior charter turn response differs".into()),
            },
            _ => return Err("prior charter turn response differs".into()),
        };
        // A new canonical attempt may follow a confirmed rejected request,
        // which has no reply or delivered tool results to close. RetryTask,
        // admission and BeginTurn retain currentness/uncertainty authority;
        // absence of a reply does not itself authorize another request.
        if previous.completed_by.is_none()
            && previous.boundary.caller.attempt_id != reporter.attempt.id
            && let Some(reply) = previous.reply.as_ref()
        {
            let mut results = Vec::new();
            for (offset, id) in reply.observation.tool_call_ids.iter().enumerate() {
                let ordinal = u32::try_from(offset)
                    .ok()
                    .and_then(|v| v.checked_add(1))
                    .ok_or("charter tool ordinal overflow")?;
                results.push(CharterResultReceipt::Collaboration {
                    call_id: id.clone(),
                    operation_id: crate::native_loop_contract::call_operation_id(
                        &previous.boundary.caller.attempt_id,
                        previous.boundary.request_index,
                        ordinal,
                    ),
                });
            }
            match mutate(
                reporter,
                state,
                control,
                format!("{}-charter-prior-closed", reporter.attempt.id),
                CharterObservation::CompleteTurn {
                    turn_id: previous.boundary.turn_id.clone(),
                    results,
                },
            )
            .await?
            .as_ref()
            {
                CharterOutcome::Turn(value)
                    if value.completed_by.is_some() && value.boundary == previous.boundary => {}
                _ => return Err("prior charter turn not durably closed".into()),
            }
        }
    }
    let expected_revision = before
        .as_ref()
        .ok_or("charter accepted session binding absent")?
        .session
        .revision;
    let boundary = match mutate(
        reporter,
        state,
        control,
        format!("{}-charter-turn-{index}", reporter.attempt.id),
        CharterObservation::BeginTurn {
            session_id: reporter.attempt.session_id.clone(),
            expected_revision,
            request_index: index,
        },
    )
    .await?
    .as_ref()
    {
        CharterOutcome::Boundary(value) => value.clone(),
        _ => return Err("charter boundary response differs".into()),
    };
    if boundary.caller != caller(&reporter.attempt)
        || boundary.session_id != reporter.attempt.session_id
        || boundary.request_index != index
    {
        return Err("charter boundary owner or request differs".into());
    }
    let changed = before
        .as_ref()
        .is_some_and(|b| b.session.effective != boundary.effective.reference);
    let changed = if let Some(original) = original {
        if boundary.effective != original.boundary.effective {
            return Err(
                "charter retry source changed; original request body cannot be reused".into(),
            );
        }
        original.rendered_changed
    } else {
        changed
    };
    let exchange = exchange(&boundary.effective, changed, input)?;
    control.check()?;
    Ok(Some(Turn {
        boundary,
        rendered_changed: changed,
        exchange,
    }))
}

pub(super) fn exchange(
    source: &crate::charter::ResolvedCharter,
    changed: bool,
    input: &TaskInput,
) -> Result<Arc<crate::native::CharterExchange>, String> {
    let limits = &input.execution_limits()?.native;
    bounded_json_size(&source.source, limits.context_bytes)
        .map_err(|_| "charter source exceeds native.context_bytes")?;
    let source_text =
        serde_json::to_string(&source.source).map_err(|_| "charter source encoding")?;
    Ok(Arc::new(crate::native::CharterExchange::new(
        crate::native::CharterInput {
            revision: serde_json::to_value(&source.reference)
                .map_err(|_| "charter revision encoding")?,
            source: source_text,
            changed,
        },
        limits,
    )?))
}

pub(super) fn capture(
    exchange: &crate::native::CharterExchange,
) -> Result<Option<CharterReplyObservation>, String> {
    Ok(exchange.take_reply()?.map(|reply| CharterReplyObservation {
        response_id: reply.response_id,
        output_item_ids: reply.item_ids,
        tool_call_ids: reply.call_ids,
        declaration: reply.declared,
        output: reply.original_output,
        output_reference: None,
    }))
}

pub(super) async fn record(
    reporter: &Reporter,
    input: &TaskInput,
    state: &Path,
    control: &Control,
    turn: Option<&Turn>,
) -> Result<(), String> {
    let Some(turn) = turn else {
        return Ok(());
    };
    let failure = turn.exchange.take_failure()?;
    let Some(reply) = capture(&turn.exchange)? else {
        return failure.map_or(Ok(()), Err);
    };
    let result: Result<(), String> = match record_original(
        reporter,
        input,
        state,
        control,
        format!("{}-reply", turn.boundary.turn_id),
        CharterObservation::Reply {
            turn_id: turn.boundary.turn_id.clone(),
            reply,
        },
    )
    .await?
    .as_ref()
    {
        CharterOutcome::Reply(value)
            if value.turn_id == turn.boundary.turn_id
                && value.effective == turn.boundary.effective.reference =>
        {
            Ok(())
        }
        _ => Err("charter reply association differs".into()),
    };
    result?;
    failure.map_or(Ok(()), Err)
}

pub(super) async fn close(
    reporter: &Reporter,
    state: &Path,
    control: &Control,
    turn: Option<&Turn>,
    results: Vec<CharterResultReceipt>,
) -> Result<(), String> {
    let Some(turn) = turn else {
        return Ok(());
    };
    // This records already-delivered results, not new execution. The canonical
    // controller checks the retained original occurrence even after cancellation.
    match mutate(
        reporter,
        state,
        control,
        format!("{}-closed", turn.boundary.turn_id),
        CharterObservation::CompleteTurn {
            turn_id: turn.boundary.turn_id.clone(),
            results,
        },
    )
    .await?
    .as_ref()
    {
        CharterOutcome::Turn(value)
            if value.boundary == turn.boundary && value.completed_by.is_some() =>
        {
            Ok(())
        }
        _ => Err("charter completion association differs".into()),
    }
}

/// Preserve an original execution/cleanup error while retaining independently
/// observed usage, health and passive audit errors from the same occurrence.
pub(super) fn with_secondary<T>(
    primary: Result<T, String>,
    secondary: Result<(), String>,
    label: &str,
) -> Result<T, String> {
    match (primary, secondary) {
        (value, Ok(())) => value,
        (Ok(_), Err(error)) => Err(format!("{label}: {error}")),
        (Err(original), Err(error)) => Err(format!("{original}; {label}: {error}")),
    }
}

#[path = "charter_output.rs"]
mod output;
pub(super) use output::{admit, recover};

pub(super) async fn record_original(
    reporter: &Reporter,
    input: &TaskInput,
    state: &Path,
    control: &Control,
    operation_id: String,
    observation: CharterObservation,
) -> Result<Box<CharterOutcome>, String> {
    // Preserve the original inline representation whenever the complete audit
    // and journal have ample room. Large originals use the same occurrence's
    // bounded reference; this does not lower the accepted response maximum.
    let limits = &control.runtime_limits;
    let l = limits
        .charter
        .as_ref()
        .ok_or("original output charter policy absent")?;
    if bounded_json_size(&observation, l.record_bytes / 2).is_ok()
        && mutations::prepare(
            Operation::ObserveCharter {
                operation_id: operation_id.clone(),
                caller: caller(&reporter.attempt),
                observation: observation.clone(),
            },
            0,
            limits,
        )
        .is_ok()
    {
        return mutate(reporter, state, control, operation_id, observation).await;
    }
    let maximum = input.execution_limits()?.native.canonical_output_bytes()?;
    let manifest = output::retain(
        state,
        limits,
        maximum,
        caller(&reporter.attempt),
        operation_id,
        observation,
    )?;
    output::upload(&reporter.client, state, limits, maximum, &manifest).await
}

/// A public supplied result uses the same durable audit journal/chunk carrier.
/// Caller and coding frame limit come from the owned accepted command, never
/// model metadata. Product result validation is outside this audit operation.
pub(super) async fn record_supplied_original(
    client: &ApiClient,
    state: &Path,
    limits: &RuntimeLimits,
    frame_bytes: usize,
    original: crate::mcp::Caller,
    operation_id: String,
    observation: CharterObservation,
) -> Result<Box<CharterOutcome>, String> {
    if !matches!(&observation, CharterObservation::SuppliedReply { .. }) {
        return Err("not a supplied original result".into());
    }
    let policy = limits
        .charter
        .as_ref()
        .ok_or("supplied result policy absent")?;
    let maximum = crate::charter::supplied_output_bytes(frame_bytes, policy.reply_items)?;
    let manifest = output::retain(state, limits, maximum, original, operation_id, observation)?;
    output::upload(client, state, limits, maximum, &manifest).await
}

/// Finish only the original received tool turn before a self-assignment stop.
/// Uses canonical original-only queries/observations after cancellation; never
/// resumes a provider, dispatches another tool, or invents missing call results.
pub(super) async fn close_assignment(
    reporter: &Reporter,
    state: &Path,
    limits: &RuntimeLimits,
    call: &crate::mcp::Invocation,
) -> Result<(), String> {
    if limits.charter.is_none() {
        return Ok(());
    }
    let op = Operation::QueryCharter {
        query: CharterQuery::Session {
            session_id: reporter.attempt.session_id.clone(),
        },
    };
    let reply = reporter.client.call(op.clone()).await?;
    reply.validate_for(&op)?;
    let Outcome::Ok {
        data: Data::CharterQuery(view),
    } = reply.outcome
    else {
        return Err("assignment charter session unavailable".into());
    };
    let CharterQueryResult::Session(view) = *view else {
        return Err("assignment charter session unavailable".into());
    };
    let Some(view) = view else { return Ok(()) };
    let id = view
        .session
        .last_turn_id
        .ok_or("assignment original native turn absent")?;
    let op = Operation::QueryCharter {
        query: CharterQuery::Turn {
            turn_id: id.clone(),
        },
    };
    let reply = reporter.client.call(op.clone()).await?;
    reply.validate_for(&op)?;
    let Outcome::Ok {
        data: Data::CharterQuery(turn),
    } = reply.outcome
    else {
        return Err("assignment original turn unavailable".into());
    };
    let CharterQueryResult::Turn(turn) = *turn else {
        return Err("assignment original turn unavailable".into());
    };
    if turn.boundary.caller != call.caller
        || turn.boundary.session_id != reporter.attempt.session_id
    {
        return Err("assignment turn owner differs".into());
    }
    let observed = turn
        .reply
        .as_ref()
        .ok_or("assignment original reply absent")?;
    if observed.observation.tool_call_ids.last() != Some(&call.logical_call_id) {
        return Err("assignment is not the original final call".into());
    }
    let mut results = Vec::new();
    for (offset, id) in observed.observation.tool_call_ids.iter().enumerate() {
        let ordinal = u32::try_from(offset)
            .ok()
            .and_then(|n| n.checked_add(1))
            .ok_or("assignment tool ordinal overflow")?;
        let operation_id = crate::native_loop_contract::call_operation_id(
            &call.caller.attempt_id,
            turn.boundary.request_index,
            ordinal,
        );
        if id == &call.logical_call_id && operation_id != call.operation_id {
            return Err("assignment original occurrence differs".into());
        }
        results.push(CharterResultReceipt::Collaboration {
            call_id: id.clone(),
            operation_id,
        });
    }
    // If closure already committed, its existing query verifier checked the
    // complete original receipt set; no new observation identity is needed.
    if turn.completed_by.is_some() {
        return Ok(());
    }
    match mutate_original(
        &reporter.client,
        state,
        limits,
        call.caller.clone(),
        format!("{}-closed", turn.boundary.turn_id),
        CharterObservation::CompleteTurn {
            turn_id: turn.boundary.turn_id.clone(),
            results,
        },
    )
    .await?
    .as_ref()
    {
        CharterOutcome::Turn(value)
            if value.boundary == turn.boundary && value.completed_by.is_some() =>
        {
            Ok(())
        }
        _ => Err("assignment original turn closure differs".into()),
    }
}
