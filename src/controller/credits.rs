//! Finite receipt reservations for existing command and effect transitions.
use super::{ControllerConfig, database::*};
use crate::protocol::*;
use super::database::StoreContext as Connection;
use rusqlite::{ OptionalExtension};

pub(super) fn final_candidate(tx: &Connection, op: &Operation) -> Result<Option<String>> {
    let Operation::SettleOperation {
        operation_id,
        outcome,
        ..
    } = op
    else {
        return Ok(None);
    };
    if outcome
        .get("acknowledged")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        return Ok(None);
    }
    let available: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM external_operations WHERE id=?1 AND finalization_reserved=1 AND settlement IS NULL)",
        [operation_id], |r| r.get(0),
    )?;
    Ok(available.then(|| operation_id.clone()))
}

pub(super) fn control_available(tx: &Connection, attempt: &str) -> Result<bool> {
    let credit: Option<i64> = tx
        .query_row(
            "SELECT control_remaining FROM attempts WHERE id=?1",
            [attempt],
            |r| r.get(0),
        )
        .optional()?;
    match credit {
        Some(0..=3) => Ok(credit.unwrap() > 0),
        _ => Err(Fault::Store("invalid command control reservation".into())),
    }
}

pub(super) fn debit_control(tx: &Connection, attempt: &str) -> Result<()> {
    if tx.execute("UPDATE attempts SET control_remaining=control_remaining-1 WHERE id=?1 AND control_remaining>0", [attempt])? != 1 {
        return Err(Fault::Store("missing earned control credit".into()));
    }
    Ok(())
}

pub(super) fn debit_effect(tx: &Connection, operation: &str) -> Result<()> {
    if tx.execute("UPDATE external_operations SET finalization_reserved=0 WHERE id=?1 AND finalization_reserved=1 AND settlement IS NOT NULL", [operation])? != 1 {
        return Err(Fault::Store("missing final settlement credit".into()));
    }
    Ok(())
}

pub(super) fn receipt(tx: &Connection, id: &str) -> Result<(Operation, Outcome)> {
    let receipt = super::metadata::read(tx, id)?
        .ok_or_else(|| Fault::Store("missing referenced mutation receipt".into()))?;
    Ok((receipt.request, receipt.outcome))
}

pub(super) fn validate_existing(tx: &Connection, cfg: &ControllerConfig) -> Result<()> {
    let bound = cfg
        .max_receipts
        .checked_add(cfg.max_cleanup_receipts)
        .and_then(|n| n.checked_add(cfg.max_tasks))
        .ok_or_else(|| Fault::Store("credit inventory bound overflow".into()))?;
    let mut q = tx.prepare("SELECT id,task_id,body,control_remaining FROM attempts ORDER BY id")?;
    let mut rows = q.query([])?;
    let mut count = 0usize;
    while let Some(row) = rows.next()? {
        count += 1;
        if count > bound {
            return Err(Fault::Store("attempt inventory exceeds bound".into()));
        }
        let id: String = row.get(0)?;
        let task: String = row.get(1)?;
        let a: Attempt = decode(&row.get::<_, String>(2)?)?;
        let remaining: i64 = row.get(3)?;
        if a.id != id
            || a.task_id != task
            || a.generation == 0
            || !(0..=3).contains(&remaining)
            || (a.stopped && remaining != 0)
        {
            return Err(Fault::Store("attempt control binding differs".into()));
        }
        load(tx, &task)?;
    }
    let mut q = tx.prepare("SELECT id,worker,incarnation,task_id,attempt_id,generation,acknowledged,body,delivery_operation_id FROM commands ORDER BY seq")?;
    let mut rows = q.query([])?;
    count = 0;
    while let Some(row) = rows.next()? {
        count += 1;
        if count > bound.saturating_mul(2) {
            return Err(Fault::Store("command inventory exceeds bound".into()));
        }
        let id: String = row.get(0)?;
        let worker: String = row.get(1)?;
        let inc: String = row.get(2)?;
        let task: String = row.get(3)?;
        let attempt: String = row.get(4)?;
        let generation = unsigned(row.get(5)?)?;
        let ack: i64 = row.get(6)?;
        let command: WorkerCommand = decode(&row.get::<_, String>(7)?)?;
        let delivered: Option<String> = row.get(8)?;
        let body: String =
            tx.query_row("SELECT body FROM attempts WHERE id=?1", [&attempt], |r| {
                r.get(0)
            })?;
        let owner: Attempt = decode(&body)?;
        if !matches!(ack, 0 | 1)
            || owner.task_id != task
            || owner.worker_id != worker
            || owner.worker_incarnation != inc
            || owner.generation != generation
            || (owner.stopped && ack != 1 && !matches!(&command, WorkerCommand::PublishRepository { .. }))
        {
            return Err(Fault::Store("command owner/index differs".into()));
        }
        let valid = match &command {
            WorkerCommand::RunAttempt {
                command_id,
                attempt: a,
                session,
                task: input,
                ..
            } => {
                session.validate_for(a, input).is_ok()
                    && session == &super::sessions::get(tx, &a.session_id)?
                    && command_id == &id
                    && a.id == attempt
                    && a.task_id == task
                    && a.generation == generation
                    && a.worker_id == worker
                    && a.worker_incarnation == inc
            }
            WorkerCommand::StopAttempt {
                command_id,
                task_id,
                attempt_id,
                generation: g,
                incarnation,
            } => {
                command_id == &id
                    && task_id == &task
                    && attempt_id == &attempt
                    && *g == generation
                    && incarnation == &inc
            }
            WorkerCommand::PublishRepository { command_id, request } => {
                command_id == &id && request.validate(command_id).is_ok()
                    && request.attempt.id == attempt && request.attempt.task_id == task
                    && request.attempt.generation == generation
                    && request.attempt.worker_id == worker && request.attempt.worker_incarnation == inc
                    && request.session == super::sessions::get(tx, &request.attempt.session_id)?
            }
            // Reserved wake hints are not produced by the current controller.
            // Refuse an injected durable command rather than treating it as
            // an execution or cancellation credit.
            WorkerCommand::CollaborationReady { .. } => false,
        };
        if !valid {
            return Err(Fault::Store("command body/index differs".into()));
        }
        if matches!(&command, WorkerCommand::RunAttempt { .. }) && delivered.is_none() {
            return Err(Fault::Store(
                "Run lacks its original delivery receipt".into(),
            ));
        }
        if let Some(delivered) = delivered {
            let (request, outcome) = receipt(tx, &delivered)?;
            if !matches!(request, Operation::AwaitCommands { worker_id, incarnation, .. } if worker_id == worker && incarnation == inc)
                || !matches!(outcome, Outcome::Ok { data: Data::Commands(ref commands) } if commands.contains(&command))
            {
                return Err(Fault::Store("command delivery receipt differs".into()));
            }
        }
    }
    let mut q = tx.prepare("SELECT id,body,prepare,settlement,finalization_reserved FROM external_operations ORDER BY id")?;
    let mut rows = q.query([])?;
    count = 0;
    while let Some(row) = rows.next()? {
        count += 1;
        if count > bound {
            return Err(Fault::Store("effect inventory exceeds bound".into()));
        }
        let id: String = row.get(0)?;
        let record: OperationRecord = decode(&row.get::<_, String>(1)?)?;
        let prepare: Operation = decode(&row.get::<_, String>(2)?)?;
        let settlement: Option<String> = row.get(3)?;
        let reserved: i64 = row.get(4)?;
        if record.id != id
            || !matches!(prepare, Operation::PrepareOperation {
            operation_id, task_id, attempt_id, generation, kind, intended
        } if operation_id == id && task_id == record.task_id && attempt_id == record.attempt_id
            && generation == record.generation && kind == record.kind && intended == record.intended)
        {
            return Err(Fault::Store("effect original binding differs".into()));
        }
        match (record.state.as_str(), settlement, reserved) {
            ("pending" | "uncertain", None, 1) => {}
            ("settled", Some(settlement), 0) => {
                let final_request: Operation = decode(&settlement)?;
                let Operation::SettleOperation {
                    ref settlement_id,
                    ref operation_id,
                    ref task_id,
                    ref attempt_id,
                    generation,
                    ref outcome,
                    ..
                } = final_request
                else {
                    return Err(Fault::Store("invalid stored settlement".into()));
                };
                let (request, reply) = receipt(tx, settlement_id)?;
                if operation_id != &id
                    || task_id != &record.task_id
                    || attempt_id != &record.attempt_id
                    || generation != record.generation
                    || record.outcome.as_ref() != Some(outcome)
                    || encode(&request)? != settlement
                    || !matches!(reply, Outcome::Ok { data: Data::Operation(ref r) } if r == &record)
                {
                    return Err(Fault::Store("effect final receipt differs".into()));
                }
            }
            _ => return Err(Fault::Store("effect final reservation differs".into())),
        }
    }
    Ok(())
}
