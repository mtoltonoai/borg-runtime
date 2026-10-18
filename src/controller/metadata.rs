//! Receipt revisions come from the same durable transaction as their outcomes.
//! These positions describe history; they are not execution authority.
use super::{ControllerConfig, database::*};
use crate::protocol::*;
use super::database::StoreContext as Connection;
use rusqlite::{ OptionalExtension};

pub(super) struct Recorded {
    pub request: Operation,
    pub outcome: Outcome,
    pub commit: MutationCommit,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub(super) enum RetainedOutcome {
    Inline(Outcome),
    ReadReference(crate::read_retention::CompactOutcome),
}
pub(super) struct Retained {
    pub request:Operation,
    pub outcome:RetainedOutcome,
    pub commit:MutationCommit,
}
impl Retained {
    pub fn lookup(&self) -> Result<OperationRecord> {
        match &self.outcome {
            RetainedOutcome::Inline(outcome) => lookup_record(&self.commit.operation_id,&self.request,outcome),
            RetainedOutcome::ReadReference(reference) => {
                let Operation::CallCollaboration { call }=&self.request else {return Err(Fault::Store("compact read request differs".into()))};
                Ok(OperationRecord { id:self.commit.operation_id.clone(), task_id:call.caller.task_id.clone(),
                    attempt_id:call.caller.attempt_id.clone(), generation:call.caller.generation,
                    kind:"committed_read_reference".into(), intended:serde_json::to_value(&self.request)?,
                    state:crate::read_retention::UNAVAILABLE.into(), outcome:Some(serde_json::to_value(reference)?) })
            }
        }
    }
    pub fn replay(self) -> Result<Outcome> {
        match self.outcome {
            RetainedOutcome::Inline(outcome)=>Ok(outcome),
            RetainedOutcome::ReadReference(record)=> {
                let Operation::CallCollaboration { call:invocation }=self.request else {return Err(Fault::Store("compact read request differs".into()))};
                Ok(Outcome::Ok {data:Data::OriginalReadBodyUnavailable(Box::new(crate::read_retention::ReleasedRead {invocation,original_commit:self.commit,record}))})
            }
        }
    }
}

pub(super) fn current(tx: &Connection) -> Result<u64> {
    let value: Option<i64> = tx
        .query_row(
            "SELECT value FROM counters WHERE kind='controller_revision'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    match value {
        Some(n) if n >= 0 => Ok(n as u64),
        _ => Err(Fault::Store(
            "missing or invalid controller revision".into(),
        )),
    }
}

pub(super) fn advance(tx: &Connection) -> Result<u64> {
    // Never recreate a missing counter or infer old revisions from row order.
    let value: Option<i64> = tx.query_row(
        "UPDATE counters SET value=value+1 WHERE kind='controller_revision' AND value>=0 AND value<9223372036854775807 RETURNING value",
        [], |r| r.get(0),
    ).optional()?;
    match value {
        Some(n) if n > 0 => Ok(n as u64),
        _ => Err(Fault::Store(
            "controller revision exhausted or unavailable".into(),
        )),
    }
}

pub(super) fn read(tx: &Connection, id: &str) -> Result<Option<Recorded>> {
    let Some(value)=read_retained(tx,id)? else {return Ok(None)};
    match value.outcome {
        RetainedOutcome::Inline(outcome)=>Ok(Some(Recorded {request:value.request,outcome,commit:value.commit})),
        RetainedOutcome::ReadReference(_)=>Err(Fault::Store(crate::read_retention::UNAVAILABLE.into())),
    }
}

/// Inventory scanners that inspect write/control families explicitly skip an
/// already validated compact read. This is not an original-operation lookup.
pub(super) fn scan_inline(tx:&Connection,id:&str)->Result<Option<Recorded>> {
    let value=read_retained(tx,id)?.ok_or_else(||Fault::Store("receipt inventory changed".into()))?;
    Ok(match value.outcome {
        RetainedOutcome::Inline(outcome)=>Some(Recorded {request:value.request,outcome,commit:value.commit}),
        RetainedOutcome::ReadReference(_)=>None,
    })
}

pub(super) fn read_retained(tx: &Connection, id: &str) -> Result<Option<Retained>> {
    let limits = tx.limits;
    let row: Option<(String, String, i64)> = tx
        .query_row(
            "SELECT request,outcome,controller_revision FROM receipts WHERE id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let Some((request, outcome, revision)) = row else {
        return Ok(None);
    };
    if request.len() > limits.transport.body_bytes || outcome.len() > limits.transport.body_bytes || !valid_id(limits, id) {
        return Err(Fault::Store(
            "stored receipt exceeds its identity or body bound".into(),
        ));
    }
    let request: Operation = decode(&request)?;
    let outcome: RetainedOutcome = decode(&outcome)?;
    if request.mutation_id() != Some(id)
        || revision <= 0
        || revision as u64 > current(tx)?
    {
        return Err(Fault::Store("invalid committed mutation receipt".into()));
    }
    match &outcome {
        RetainedOutcome::Inline(outcome) => {
            if !matches!(outcome,Outcome::Ok {..}|Outcome::Refused {..}) {return Err(Fault::Store("invalid committed outcome".into()))}
            receipt_bound(limits,&request,outcome)?;
        }
        RetainedOutcome::ReadReference(reference) => {
            let version:i64=tx.query_row("PRAGMA user_version",[],|r|r.get(0))?;
            if version<26 {return Err(Fault::Store("compact read vocabulary requires schema26".into()))}
            reference.validate(&request).map_err(Fault::Store)?;
            super::read_retention::validate_origin(tx,id,&request,reference,revision as u64)?;
        }
    }
    Ok(Some(Retained {
        request,
        outcome,
        commit: MutationCommit {
            operation_id: id.into(),
            controller_revision: revision as u64,
        },
    }))
}

pub(super) fn validate_existing(tx: &Connection, cfg: &ControllerConfig) -> Result<()> {
    let observed = current(tx)?;
    let bound = cfg
        .max_receipts
        .checked_add(cfg.max_cleanup_receipts)
        .and_then(|n| n.checked_add(1))
        .and_then(|n| i64::try_from(n).ok())
        .ok_or_else(|| Fault::Store("receipt inventory bound overflow".into()))?;
    let mut query = tx.prepare(
        "SELECT id,controller_revision,cleanup FROM receipts ORDER BY controller_revision LIMIT ?1",
    )?;
    let mut rows = query.query([bound])?;
    let mut previous = 0;
    let mut normal = 0usize;
    let mut cleanup = 0usize;
    while let Some(row) = rows.next()? {
        let id: String = row.get(0)?;
        let revision: i64 = row.get(1)?;
        match row.get::<_, i64>(2)? {
            0 => normal += 1,
            1 => cleanup += 1,
            _ => return Err(Fault::Store("invalid receipt capacity class".into())),
        }
        if normal > cfg.max_receipts
            || cleanup > cfg.max_cleanup_receipts
            || revision <= previous
            || revision <= 0
            || revision as u64 > observed
        {
            return Err(Fault::Store(
                "invalid receipt revision order or capacity".into(),
            ));
        }
        read_retained(tx, &id)?
            .ok_or_else(|| Fault::Store("receipt disappeared during validation".into()))?;
        previous = revision;
    }
    Ok(())
}

pub(super) fn reply(limits: &crate::limits::RuntimeLimits, 
    request_id: &str,
    operation: &Operation,
    outcome: Outcome,
    commit: Option<MutationCommit>,
    observed: u64,
) -> Result<Reply> {
    let reply = Reply {
        request_id: request_id.into(),
        outcome,
        commit,
        observed_controller_revision: Some(observed),
    };
    reply.validate_for(operation).map_err(Fault::Store)?;
    full_bound(limits, &reply)?;
    Ok(reply)
}

fn full_bound(limits: &crate::limits::RuntimeLimits, reply: &Reply) -> Result<()> {
    bounded_json_size(reply, limits.transport.body_bytes)
        .map(|_| ())
        .map_err(|_| {
            Fault::Refused(
                "capacity",
                "complete response exceeds response bound: transport.body_bytes".into(),
            )
        })
}

// Use the same concrete OperationRecord and Reply serializers as the real
// lookup path. All temporary bodies remain bounded by the existing wire cap.
pub(super) fn lookup_record(
    id: &str,
    request: &Operation,
    outcome: &Outcome,
) -> Result<OperationRecord> {
    let task_id = match outcome {
        Outcome::Ok {
            data: Data::Task(task),
        } => task.id.clone(),
        _ => String::new(),
    };
    Ok(OperationRecord {
        id: id.into(),
        task_id,
        attempt_id: String::new(),
        generation: 0,
        kind: "application_mutation".into(),
        intended: serde_json::to_value(request)?,
        state: "settled".into(),
        outcome: Some(serde_json::to_value(outcome)?),
    })
}

const COMPLETE_CALL_BOUND_MESSAGE: &str = "complete collaboration result exceeds accepted limit: execution.collaboration.complete_result_bytes";
pub(super) fn complete_call_refusal() -> Outcome {
    Outcome::Refused { code: "response_bound".into(), message: COMPLETE_CALL_BOUND_MESSAGE.into() }
}
fn complete_call_frame(operation_id:&str, outcome:Outcome, requires_quiescence:bool)->crate::mcp::CallResult {
    crate::mcp::CallResult {
        requires_quiescence,
        reply: Reply { request_id:String::new(),
            commit:Some(MutationCommit { operation_id:operation_id.into(), controller_revision:i64::MAX as u64 }),
            observed_controller_revision:Some(i64::MAX as u64), outcome },
    }
}
pub(super) fn minimum_collaboration_result_bytes(transport:&crate::limits::TransportLimits)->std::result::Result<usize,String> {
    let frame=complete_call_frame("",complete_call_refusal(),false);
    let fixed=bounded_json_size(&frame,usize::MAX)?;
    transport.identity_bytes.checked_mul(4).and_then(|padding|fixed.checked_add(padding)).ok_or_else(||"collaboration refusal envelope exceeds representation".into())
}
pub(super) fn complete_call_bound(transport:&crate::limits::TransportLimits,call:&crate::mcp::Invocation,outcome:&Outcome,limit:usize)->Result<()> {
    let frame=complete_call_frame(&call.operation_id,outcome.clone(),call.tool=="collab_task_delegate" && matches!(outcome,Outcome::Ok{..}));
    padded_bound(&frame,transport.identity_bytes,limit).map_err(|_|Fault::Refused("response_bound",COMPLETE_CALL_BOUND_MESSAGE.into()))
}

fn padded_bound<T:serde::Serialize>(value:&T,identity_bytes:usize,cap:usize)->std::result::Result<(),String> {
    let size=bounded_json_size(value,cap)?;
    if identity_bytes.checked_mul(2).and_then(|padding|size.checked_add(padding)).is_none_or(|full|full>cap) {
        return Err("escaped transport identity exceeds reply bound".into());
    }
    Ok(())
}

fn bound_owned(limits: &crate::limits::RuntimeLimits, operation: &Operation, outcome: Outcome) -> Result<()> {
    let id = operation
        .mutation_id()
        .ok_or_else(|| Fault::Store("read has no mutation receipt".into()))?;
    // IDs obey the selected metadata byte bound and exclude controls. Backslashes attain
    // maximum JSON escaping; revisions use the existing checked SQL range.
    let mut frame = Reply {
        request_id: String::new(),
        commit: Some(MutationCommit {
            operation_id: id.into(),
            controller_revision: i64::MAX as u64,
        }),
        observed_controller_revision: Some(i64::MAX as u64),
        outcome,
    };
    bounded_json_size(&frame.outcome, limits.controller.outcome_bytes).map_err(|_| {
        Fault::Refused(
            "capacity",
            "operation response exceeds response bound: controller.outcome_bytes".into(),
        )
    })?;
    padded_bound(&frame,limits.transport.identity_bytes,limits.transport.body_bytes)
        .map_err(|message|Fault::Refused("capacity",format!("{message}: transport.body_bytes")))?;
    let record = lookup_record(id, operation, &frame.outcome)?;
    frame.outcome = Outcome::Ok {
        data: Data::Operation(record),
    };
    // GetOperation still applies the common Outcome ceiling before framing.
    // A receipt must remain recoverable through that actual reader too.
    bounded_json_size(&frame.outcome, limits.controller.outcome_bytes).map_err(|_| {
        Fault::Refused("capacity", "operation lookup exceeds response bound: controller.outcome_bytes".into())
    })?;
    padded_bound(&frame,limits.transport.identity_bytes,limits.transport.body_bytes)
        .map_err(|message|Fault::Refused("capacity",format!("{message}: transport.body_bytes")))
}

pub(super) fn receipt_bound(limits: &crate::limits::RuntimeLimits, operation: &Operation, outcome: &Outcome) -> Result<()> {
    bound_owned(limits, operation, outcome.clone())
}

pub(super) fn commands_fit(limits: &crate::limits::RuntimeLimits, operation: &Operation, commands: &[&WorkerCommand]) -> bool {
    let outcome = Outcome::Ok {
        data: Data::Commands(commands.iter().map(|c| (*c).clone()).collect()),
    };
    bound_owned(limits, operation, outcome).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_reply_bound_counts_escaped_transport_and_commit_metadata() {
    let limits = &crate::limits::RuntimeLimits::seed().expect("test metadata");
        let id = "\"".repeat(256);
        let mut reply = Reply {
            request_id: String::new(),
            commit: Some(MutationCommit {
                operation_id: id,
                controller_revision: i64::MAX as u64,
            }),
            observed_controller_revision: Some(i64::MAX as u64),
            outcome: Outcome::Refused {
                code: "fixture".into(),
                message: String::new(),
            },
        };
        let overhead = serde_json::to_vec(&reply).unwrap().len();
        let mut message = "\\".repeat((limits.transport.body_bytes - overhead) / 2);
        if overhead + message.len() * 2 < limits.transport.body_bytes {
            message.push('x');
        }
        let Outcome::Refused { message: value, .. } = &mut reply.outcome else {
            unreachable!()
        };
        *value = message;
        assert_eq!(serde_json::to_vec(&reply).unwrap().len(), limits.transport.body_bytes);
        assert!(full_bound(limits, &reply).is_ok());
        let Outcome::Refused { message, .. } = &mut reply.outcome else {
            unreachable!()
        };
        message.push('x');
        assert_eq!(
            serde_json::to_vec(&reply).unwrap().len(),
            limits.transport.body_bytes + 1
        );
        assert!(matches!(
            full_bound(limits, &reply),
            Err(Fault::Refused("capacity", _))
        ));
    }
}
