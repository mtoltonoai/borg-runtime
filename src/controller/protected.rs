//! Same-owner durable protected receipts. No process or repository I/O in transactions.
use super::{ControllerConfig, database::*};
use crate::protected_release::*;
use super::database::StoreContext as Connection;
use rusqlite::{ OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize)]
struct Recorded {
    producer: Producer,
    request: Request,
    reply: Reply,
}
fn recorded(tx: &Connection, id: &str) -> Result<Recorded> {
    let row: Option<(String, String, String)> = tx
        .query_row(
            "SELECT producer,request,reply FROM protected_receipts WHERE id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let Some((p, q, r)) = row else {
        return refuse("not_found", "protected receipt absent");
    };
    Ok(Recorded {
        producer: decode(&p)?,
        request: decode(&q)?,
        reply: decode(&r)?,
    })
}
fn action(tx: &Connection, id: &str) -> Result<Action> {
    let row: Option<String> = tx
        .query_row(
            "SELECT body FROM protected_actions WHERE id=?1",
            [id],
            |r| r.get(0),
        )
        .optional()?;
    let Some(s) = row else {
        return refuse("not_found", "protected action absent");
    };
    let a: Action = decode(&s)?;
    if a.id != id {
        return Err(Fault::Store("action association mismatch".into()));
    }
    Ok(a)
}
fn put_action(tx: &Connection, a: &Action) -> Result<()> {
    tx.execute("INSERT INTO protected_actions(id,body) VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET body=excluded.body",params![a.id,encode(a)?])?;
    Ok(())
}
fn permit(p: &Producer, role: Role) -> Result<()> {
    if p.role == role {
        Ok(())
    } else {
        refuse(
            "wrong_producer_role",
            "producer cannot perform this operation",
        )
    }
}
fn subject(tx: &Connection, c: &Config, s: &Subject) -> Result<()> {
    let r = super::operations::releases::read(tx, &s.release_id)?;
    let task = load(tx, &r.task_id)?.task;
    if r.artifact != s.artifact
        || task.input.repository.as_ref().map(|v| v.url.as_str()) != Some(s.repository.as_str())
        || s.repository != c.repository
        || s.target_ref != c.target_ref
        || s.policy != c.policy
        || s.deployment_target != c.deployment_target
        || s.target_revision != c.target_revision
        || s.installed_package_sha256 != c.installed_package_sha256
        || s.expected_base.len() != s.artifact.source_commit.len()
        || !s
            .expected_base
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || s.expected_base == s.artifact.source_commit
    {
        return refuse(
            "subject_mismatch",
            "artifact, source, target, policy or expected base differs",
        );
    }
    Ok(())
}
fn check(tx: &Connection, c: &Config, s: &Subject, id: &str, now: u64) -> Result<()> {
    let r = recorded(tx, id)?;
    if !c.producers.contains(&r.producer)
        || r.producer.role != Role::Checker
        || r.reply.status != "ok"
    {
        return refuse("untrusted_check", "check producer unavailable");
    }
    let Command::Check(v) = r.request.command else {
        return refuse("missing_check", "not a checker receipt");
    };
    if v.subject != *s || v.recipe_sha256 != c.recipe_sha256 {
        return refuse("check_subject_mismatch", "check subject or recipe differs");
    }
    if v.checked_at_ms > now
        || v.valid_until_ms <= now
        || now - v.checked_at_ms > c.max_check_age_ms
    {
        return refuse("stale_check", "check expired");
    }
    if !v.cleanup_verified
        || c.required_checks
            .iter()
            .any(|n| v.results.get(n) != Some(&true))
    {
        return refuse(
            "failed_or_missing_check",
            "mandatory check or cleanup absent/failed",
        );
    }
    Ok(())
}
//= spec/impl/v2/implementation.md#changes-can-be-prepared-without-per-change-human-approval
//# Human authorization MUST NOT bypass applicable mandatory validation.
// Grant lookup is separate from the mandatory check lookup at Prepare/Start.
fn grant(tx: &Connection, c: &Config, s: &Subject, id: Option<&String>, now: u64) -> Result<()> {
    let Some(id) = id else {
        return refuse("human_authorization_required", "no rollout grant");
    };
    let r = recorded(tx, id)?;
    if !c.producers.contains(&r.producer)
        || r.producer.role != Role::Human
        || r.reply.status != "ok"
    {
        return refuse("human_authorization_required", "grant producer invalid");
    }
    match r.request.command {
        Command::Grant {
            subject,
            valid_until_ms,
        } if subject == *s && valid_until_ms > now => Ok(()),
        _ => refuse(
            "human_authorization_mismatch",
            "grant is stale or for another artifact/target",
        ),
    }
}
//= spec/impl/v2/implementation.md#configured-actions-and-automatic-merge
//# A merge MUST NOT trigger a rollout without the rollout's required human authorization.
// Merge actions and rollout actions have separate dispatch and authority.
pub(crate) fn view(tx: &Connection, cfg: &ControllerConfig, release_id: &str) -> Result<StageView> {
    let limits = tx.limits;
    let candidate = super::operations::releases::read(tx, release_id)?;
    let mut q = tx.prepare("SELECT id FROM protected_receipts ORDER BY revision")?;
    let ids = q.query_map([], |r| r.get::<_, String>(0))?;
    let mut records = vec![];
    let mut bytes = encode(&candidate)?.len().saturating_add(limits.controller.protected_envelope_reserve_bytes);
    for id in ids {
        let id = id?;
        let r = recorded(tx, &id)?;
        let subject = match &r.request.command {
            Command::Check(v) => Some(&v.subject),
            Command::Grant { subject, .. } => Some(subject),
            _ => None,
        };
        if r.reply.status == "ok" && subject.is_some_and(|s| s.release_id == release_id) {
            let item = StageRecord {
                operation_id: id,
                producer_id: r.producer.id,
                command: r.request.command,
            };
            bytes = bytes.saturating_add(encode(&item)?.len() + 1);
            if bytes > limits.interface.protected_reply_bytes {
                return refuse("response_bound", "stage records exceed bound: interface.protected_reply_bytes");
            };
            records.push(item);
        }
    }
    let mut q = tx.prepare("SELECT id FROM protected_actions ORDER BY id")?;
    let ids = q.query_map([], |r| r.get::<_, String>(0))?;
    let mut actions = vec![];
    for id in ids {
        let a = action(tx, &id?)?;
        if a.subject.release_id == release_id {
            bytes = bytes.saturating_add(encode(&a)?.len() + 1);
            if bytes > limits.interface.protected_reply_bytes {
                return refuse("response_bound", "stage actions exceed bound: interface.protected_reply_bytes");
            };
            actions.push(a)
        }
    }
    let result = StageView {
        candidate,
        configured: cfg.protected_release.is_some(),
        policy: cfg.protected_release.as_ref().map(|c| c.policy.clone()),
        records,
        actions,
        automatic_rollout: false,
    };
    if encode(&result)?.len() > limits.interface.protected_reply_bytes {
        return refuse("response_bound", "stage view exceeds bound: interface.protected_reply_bytes");
    };
    Ok(result)
}
fn apply(tx: &Connection, c: &Config, p: &Producer, req: &Request, now: u64) -> Result<Reply> {
    let limits = tx.limits;
    match &req.command {
        Command::Check(v) => {
            permit(p, Role::Checker)?;
            subject(tx, c, &v.subject)?;
            if v.recipe_sha256 != c.recipe_sha256
                || !hash(&v.recipe_sha256)
                || v.checked_at_ms > now
                || v.valid_until_ms <= now

            {
                return refuse(
                    "invalid_check",
                    "bounded configured check interval/recipe required",
                );
            }
            if v.valid_until_ms.saturating_sub(v.checked_at_ms) > c.max_check_age_ms {
                return refuse("invalid_check", "bounded configured check interval/recipe required: protected_release.max_check_age_ms");
            }
            if v.results.len() > limits.controller.protected_check_results {
                return refuse("invalid_check", "bounded configured check interval/recipe required: controller.protected_check_results");
            }
            Ok(Reply::ok(v))
        }
        Command::Grant {
            subject: s,
            valid_until_ms,
        } => {
            permit(p, Role::Human)?;
            subject(tx, c, s)?;
            if *valid_until_ms <= now {
                return refuse("invalid_grant", "bounded human grant interval required");
            }
            if valid_until_ms.saturating_sub(now) > c.max_check_age_ms {
                return refuse("invalid_grant", "bounded human grant interval required: protected_release.max_check_age_ms");
            }
            Ok(Reply::ok(&req.command))
        }
        Command::Prepare {
            kind,
            subject: s,
            check_id,
            grant_id,
        } => {
            permit(
                p,
                if *kind == Kind::Merge {
                    Role::Repository
                } else {
                    Role::Rollout
                },
            )?;
            subject(tx, c, s)?;
            check(tx, c, s, check_id, now)?;
            if *kind == Kind::Rollout {
                grant(tx, c, s, grant_id.as_ref(), now)?
            } else if grant_id.is_some() {
                return refuse("invalid_input", "merge does not consume rollout authority");
            }
            let a = Action {
                id: req.operation_id.clone(),
                kind: kind.clone(),
                subject: s.clone(),
                check_id: check_id.clone(),
                grant_id: grant_id.clone(),
                state: State::Prepared,
                observation: None,
                dispatch_until_ms: None,
            };
            put_action(tx, &a)?;
            Ok(Reply::ok(a))
        }
        Command::Start { action_id } | Command::ExecuteMerge { action_id } => {
            permit(p, Role::Repository)?;
            let mut a = action(tx, action_id)?;
            if a.kind != Kind::Merge || a.state != State::Prepared {
                return refuse(
                    "action_not_startable",
                    "already attempted or wrong action kind",
                );
            }
            subject(tx, c, &a.subject)?;
            check(tx, c, &a.subject, &a.check_id, now)?;
            let r = recorded(tx, &a.check_id)?;
            let Command::Check(v) = r.request.command else {
                unreachable!()
            };
            a.dispatch_until_ms = Some(
                v.valid_until_ms
                    .min(v.checked_at_ms.saturating_add(c.max_check_age_ms)),
            );
            a.state = State::Started;
            put_action(tx, &a)?;
            Ok(Reply::ok(a))
        }
        Command::Finish {
            action_id,
            state,
            observation,
        } => {
            permit(p, Role::Repository)?;
            let mut a = action(tx, action_id)?;
            if a.kind != Kind::Merge
                || !matches!(a.state, State::Started | State::Unknown)
                || !matches!(state, State::Applied | State::Refused | State::Unknown)
            {
                return refuse("invalid_observation", "invalid original action observation");
            }
            if observation.len() > limits.controller.protected_observation_bytes {
                return refuse("invalid_observation", "invalid original action observation: controller.protected_observation_bytes");
            }
            // Historical effect observation is retained even if policy changed after dispatch.
            // It grants no new action or rollout authority.
            a.state = state.clone();
            a.observation = Some(observation.clone());
            put_action(tx, &a)?;
            Ok(Reply::ok(a))
        }
        Command::StartRollout {
            action_id,
            subject: s,
        } => {
            permit(p, Role::Rollout)?;
            let mut a = action(tx, action_id)?;
            if a.kind != Kind::Rollout || a.subject != *s || a.state != State::Prepared {
                return refuse(
                    "action_not_startable",
                    "rollout already attempted or subject differs",
                );
            }
            subject(tx, c, s)?;
            check(tx, c, s, &a.check_id, now)?;
            grant(tx, c, s, a.grant_id.as_ref(), now)?;
            let r = recorded(tx, &a.check_id)?;
            let Command::Check(v) = r.request.command else {
                unreachable!()
            };
            let g = recorded(tx, a.grant_id.as_ref().unwrap())?;
            let Command::Grant { valid_until_ms, .. } = g.request.command else {
                unreachable!()
            };
            a.dispatch_until_ms = Some(
                valid_until_ms
                    .min(v.valid_until_ms)
                    .min(v.checked_at_ms.saturating_add(c.max_check_age_ms)),
            );
            a.state = State::Started;
            put_action(tx, &a)?;
            Ok(Reply::ok(a))
        }
        Command::ObserveRollout {
            action_id,
            subject: s,
            installed_package_sha256,
            observed,
        } => {
            permit(p, Role::Rollout)?;
            let mut a = action(tx, action_id)?;
            if a.kind != Kind::Rollout
                || a.subject != *s
                || !matches!(a.state, State::Started | State::Unknown)
            {
                return refuse("invalid_observation", "rollout action binding differs");
            }
            // This is an observation, never an apply command. Authorization must be
            // consumed by an external operator-controlled adapter immediately before action.
            // Historical observation is not fresh authorization; expired policy or
            // grants do not erase an already dispatched external effect.
            if *observed && installed_package_sha256 != &s.artifact.package_sha256 {
                return refuse("wrong_installed_artifact", "target readback differs");
            }
            a.state = if *observed {
                State::Applied
            } else {
                State::Unknown
            };
            a.observation = Some(installed_package_sha256.clone());
            put_action(tx, &a)?;
            Ok(Reply::ok(a))
        }
        Command::ReconcileObservation {
            action_id,
            state,
            observation,
        } => {
            permit(p, Role::Repository)?;
            let mut a = action(tx, action_id)?;
            if a.kind != Kind::Merge {
                return refuse("invalid_observation", "invalid original action readback");
            }
            if observation.len() > limits.controller.protected_observation_bytes {
                return refuse("invalid_observation", "invalid original action readback: controller.protected_observation_bytes");
            }
            if matches!(a.state, State::Started | State::Unknown)
                && matches!(state, State::Applied | State::Refused | State::Unknown)
            {
                a.state = state.clone();
                a.observation = Some(observation.clone());
                put_action(tx, &a)?;
            } else if a.state != *state {
                return refuse(
                    "action_state_changed",
                    "original action state changed before readback commit",
                );
            }
            Ok(Reply::ok(a))
        }
        Command::GetAction { action_id } => Ok(Reply::ok(action(tx, action_id)?)),
        Command::GetReceipt { operation_id } => {
            let r = recorded(tx, operation_id)?;
            Ok(Reply::ok(r))
        }
        Command::View { release_id } => {
            let candidate = super::operations::releases::read(tx, release_id)?;
            let mut query = tx.prepare("SELECT body FROM protected_actions ORDER BY id")?;
            let rows = query.query_map([], |r| r.get::<_, String>(0))?;
            let mut actions = vec![];
            let mut bytes = encode(&candidate)?.len().saturating_add(limits.controller.protected_envelope_reserve_bytes);
            for row in rows {
                let a: Action = decode(&row?)?;
                if a.subject.release_id == *release_id {
                    bytes = bytes.saturating_add(encode(&a)?.len() + 1);
                    if bytes > limits.interface.protected_reply_bytes {
                        return refuse("response_bound", "action view exceeds bound: interface.protected_reply_bytes");
                    };
                    actions.push(a)
                }
            }
            Ok(Reply::ok(
                serde_json::json!({"candidate":candidate,"actions":actions,"automatic_rollout":false}),
            ))
        }
        Command::ReconcileMerge { .. } => refuse(
            "adapter_required",
            "repository operation requires installed adapter",
        ),
    }
}
impl Database {
    pub(super) fn protected(&mut self, uid: u32, req: Request) -> (Reply, bool) {
    let limits = &self.config.limits;
        let result = (|| -> Result<(Reply, bool)> {
            let Some(c) = &self.config.protected_release else {
                return refuse("unconfigured", "no protected producers configured");
            };
            let Some(p) = c.producers.iter().find(|p| p.uid == uid) else {
                return refuse("unauthenticated", "peer UID is not a configured producer");
            };
            if req.revision != REVISION || req.operation_id.is_empty() || req.operation_id.chars().any(char::is_control) {
                return refuse("invalid_input", "revision, identity or body bound");
            }
            if req.operation_id.len() > limits.transport.identity_bytes {
                return refuse("invalid_input", "revision, identity or body bound: transport.identity_bytes");
            }
            if encode(&req)?.len() > limits.interface.protected_request_bytes {
                return refuse("invalid_input", "revision, identity or body bound: interface.protected_request_bytes");
            }
            let stored_req = if let Command::ReconcileObservation { action_id, .. } = &req.command {
                Request {
                    revision: req.revision,
                    operation_id: req.operation_id.clone(),
                    command: Command::ReconcileMerge {
                        action_id: action_id.clone(),
                    },
                }
            } else {
                req.clone()
            };
            let transaction = self
                .conn
                .transaction_with_behavior(TransactionBehavior::Immediate)?;
            let tx = Connection::new(&transaction, &self.config.limits);
            let read_only = matches!(
                req.command,
                Command::GetAction { .. } | Command::GetReceipt { .. } | Command::View { .. }
            );
            if !read_only {
                let exists: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM protected_receipts WHERE id=?1)",
                    [&req.operation_id],
                    |r| r.get(0),
                )?;
                if exists {
                    let old = recorded(&tx, &req.operation_id)?;
                    if old.producer != *p || old.request != stored_req {
                        return refuse(
                            "operation_conflict",
                            "identity has different producer or input",
                        );
                    }
                    let mut reply = old.reply;
                    reply.replayed = true;
                    return Ok((reply, false));
                }
                let n: i64 =
                    tx.query_row("SELECT count(*) FROM protected_receipts", [], |r| r.get(0))?;
                if n as usize >= c.max_records {
                    return refuse("capacity", "protected receipt capacity exhausted: protected_release.max_records");
                }
            }
            let reply = match apply(&tx, c, p, &req, (self.config.clock)()) {
                Ok(v) => v,
                Err(Fault::Refused(code, message)) => crate::protected_release::refusal_with_limit(code, &message),
                Err(e) => return Err(e),
            };
            if encode(&reply)?.len() > limits.interface.protected_reply_bytes {
                return refuse("response_bound", "protected view exceeds bound: interface.protected_reply_bytes");
            }
            if read_only {
                return Ok((reply, false));
            }
            // An admitted original must itself remain retrievable through the
            // same bounded GetReceipt transport, not only fit its first reply.
            let retained = Recorded {
                producer: p.clone(),
                request: stored_req.clone(),
                reply: reply.clone(),
            };
            if encode(&Reply::ok(&retained))?.len() > limits.interface.protected_reply_bytes {
                return refuse(
                    "response_bound",
                    "original receipt lookup envelope exceeds bound: interface.protected_reply_bytes",
                );
            }
            // Prepared actions retain two settlement slots (start + final
            // observation), started/unknown actions retain one. Ordinary traffic
            // cannot consume the last slot needed for truthful terminal readback.
            let n: i64 =
                tx.query_row("SELECT count(*) FROM protected_receipts", [], |r| r.get(0))?;
            let mut q = tx.prepare("SELECT body FROM protected_actions")?;
            let rows = q.query_map([], |r| r.get::<_, String>(0))?;
            let mut reserved = 0usize;
            for row in rows {
                let a: Action = decode(&row?)?;
                reserved += match a.state {
                    State::Prepared => 2,
                    State::Started | State::Unknown => 1,
                    _ => 0,
                };
            }
            drop(q);
            if (n as usize).saturating_add(1).saturating_add(reserved) > c.max_records {
                return refuse(
                    "capacity",
                    "protected action settlement capacity is reserved: protected_release.max_records",
                );
            }
            let revision = super::metadata::advance(&tx)?;
            tx.execute("INSERT INTO protected_receipts(id,producer,request,reply,revision) VALUES(?1,?2,?3,?4,?5)",params![req.operation_id,encode(p)?,encode(&stored_req)?,encode(&reply)?,integer(revision)?])?;
            transaction.commit()?;
            Ok((reply, true))
        })();
        match result {
            Ok(v) => v,
            Err(Fault::Refused(code, message)) => (crate::protected_release::refusal_with_limit(code, &message), false),
            Err(_) => (
                Reply::refused("store_unavailable_reconcile_original"),
                false,
            ),
        }
    }
}
pub(super) fn validate_existing(tx: &Connection, cfg: &ControllerConfig) -> Result<()> {
    let limits = tx.limits;
    let mut q = tx.prepare("SELECT id,revision FROM protected_receipts ORDER BY revision")?;
    let mut rows = q.query([])?;
    let mut count = 0;
    let mut last = 0;
    let mut witnesses = std::collections::BTreeSet::new();
    while let Some(row) = rows.next()? {
        count += 1;
        let id: String = row.get(0)?;
        let rev: i64 = row.get(1)?;
        let rec = recorded(tx, &id)?;
        if rec.reply.status == "ok" {
            if let Ok(a) = serde_json::from_value::<Action>(rec.reply.data.clone()) {
                witnesses.insert(encode(&a)?);
            }
        }
        if count
            > cfg
                .protected_release
                .as_ref()
                .map_or(cfg.protected_default_records, |c| c.max_records)
            || rev <= last
            || rev as u64 > super::metadata::current(tx)?
            || rec.request.operation_id != id
            || rec.reply.replayed
            || !valid_id(limits, &id)
        {
            return Err(Fault::Store("invalid protected receipt association".into()));
        }
        last = rev;
    }
    let mut q = tx.prepare("SELECT id FROM protected_actions")?;
    let ids = q.query_map([], |r| r.get::<_, String>(0))?;
    for id in ids {
        let id = id?;
        let a = action(tx, &id)?;
        let rec = recorded(tx, &id)?;
        if rec.reply.status != "ok"
            || !matches!(rec.request.command,Command::Prepare{ref subject,..} if subject==&a.subject)
        {
            return Err(Fault::Store("unbound protected action".into()));
        }
    }
    let mut q = tx.prepare("SELECT id,body FROM protected_actions")?;
    let rows = q.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    for row in rows {
        let (id, body) = row?;
        let a: Action = decode(&body)?;
        if !witnesses.contains(&encode(&a)?) || a.id != id {
            return Err(Fault::Store(
                "action lacks exact committed transition witness".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "protected_tests.rs"]
mod tests;

