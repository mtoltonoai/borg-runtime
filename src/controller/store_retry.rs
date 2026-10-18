//! Optional, in-process scheduling of one original SQL phase. Parked work owns
//! neither a transaction nor a Controller sender. External proof work stays in
//! the existing caller path and is never executed by this scheduler.
use super::{database::{self, Database, Response}, protected::{ProtectedResponse, ProtectedFailure},
    Envelope, Message, OrdinaryMessage, OriginalRequest};
use crate::{limits::StoreRetryLimits, protocol::{Operation, Outcome, Reply}};
use std::{collections::VecDeque, sync::{Arc, Mutex, Weak, mpsc}, time::{Duration, Instant}};
use tokio::sync::{oneshot, watch, OwnedSemaphorePermit};

struct Waiter<T> {
    request_id: String,
    reply: oneshot::Sender<T>,
    // Drop phase ownership before returning capacity to concurrent admission.
    _episode: Option<Arc<Episode>>,
    _permit: Option<OwnedSemaphorePermit>,
}
enum Work {
    Ordinary { original: OriginalRequest, waiters: Vec<Waiter<Response>>, receipt_only: bool },
    Protected { uid: u32, request: crate::protected_release::Request,
        waiters: Vec<Waiter<crate::protected_release::Reply>>, receipt_only: bool },
}
struct Key {
    protected: bool,
    phase: u8,
    mutation: bool,
    identity: String,
    body: String,
    provenance: String,
}
impl Key {
    fn same_slot(&self, other: &Self) -> bool {
        self.protected == other.protected && self.phase == other.phase
            && self.mutation == other.mutation && self.identity == other.identity
            && (self.mutation || (self.body == other.body && self.provenance == other.provenance))
    }
    fn exact(&self, other: &Self) -> bool {
        self.same_slot(other) && self.body == other.body && self.provenance == other.provenance
    }
}
struct Item { work: Work, key: Key, expires: Instant, due: Instant }

// Admission and queued/parked waiters share only the original phase deadline.
// The weak registry owns no work, sender, proof or durable identity. Dead entries
// are pruned on admission; live entries are bounded by the total waiter permit.
pub(super) struct Episode { key: Key, expires: Instant }
#[derive(Default)]
pub(super) struct Episodes(Mutex<Vec<Weak<Episode>>>);
impl Episodes {
    pub(super) fn admit(&self, message: &Message, admitted: Instant,
        policy: &StoreRetryLimits) -> Option<Arc<Episode>> {
        let key = match message {
            Message::Ordinary(message) => ordinary_key(&message.original),
            Message::Protected { uid, request, .. } => protected_key(*uid, request),
            Message::Revision { .. } => return None,
        };
        let mut entries = self.0.lock().expect("owner phase registry poisoned");
        entries.retain(|entry| entry.strong_count() != 0);
        for entry in entries.iter().filter_map(Weak::upgrade) {
            if entry.key.same_slot(&key) { return Some(entry); }
        }
        let expires = admitted.checked_add(Duration::from_millis(policy.operation_timeout_ms))
            .unwrap_or(admitted);
        let episode = Arc::new(Episode { key, expires });
        entries.push(Arc::downgrade(&episode));
        Some(episode)
    }
}

fn eligible(disposition: super::checked_transaction::RetryDisposition) -> bool {
    use super::checked_transaction::RetryDisposition::*;
    // The selected service policy deliberately admits the separate known-BEGIN
    // refusal. It is NOT an acknowledged rollback or external-absence proof.
    matches!(disposition, RolledBackTransient { .. } | BeginNotStartedTransient { .. })
}

pub(super) fn maintenance_safe(operation: &Operation) -> bool {
    use crate::{governance::GovernanceMutation, protocol::Observation};
    // Exact original receipts precede this fence in Database::process. Reads
    // grant no new work. AwaitCommands uses its bounded Stop-only exchange.
    operation.mutation_id().is_none() || matches!(operation,
        Operation::CancelTask { .. } | Operation::AwaitCommands { .. }
        | Operation::SettleOperation { .. } | Operation::SettleNativeRequest { .. }
        | Operation::AcknowledgeAgentMemory { .. }
        | Operation::AcknowledgeCollaborationDelivery { .. }
        | Operation::AcknowledgeCompletionInput(_)
        | Operation::MutateGovernance { mutation: GovernanceMutation::Stop { .. }
            | GovernanceMutation::Pause { .. }, .. }
        | Operation::ObserveWorker { report: crate::protocol::Report { observation:
            Observation::Progress { .. } | Observation::ExecutorOutcome { .. }
            | Observation::ExecutionUncertain { .. } | Observation::ResultCandidate { .. }
            | Observation::ProcessTreeStopped | Observation::NativeRequestDiagnostic { .. }
            | Observation::NativeRequestRejected { .. } | Observation::NativeRetryBlocked { .. }
            | Observation::NativeLoopCompleted { .. }, .. } })
}

#[derive(Clone, Copy)]
enum MaintenancePhase { Deadline, Expire { through_ms: u64 } }
struct Maintenance { phase: MaintenancePhase, expires: Instant, due: Instant }
impl Maintenance {
    fn new(phase: MaintenancePhase, policy: &StoreRetryLimits) -> Self {
        let now = Instant::now();
        Self { phase, expires: now.checked_add(Duration::from_millis(policy.operation_timeout_ms)).unwrap_or(now), due: now }
    }
    fn attempt(&mut self, db: &mut Database, changed: &watch::Sender<u64>, policy: &StoreRetryLimits)
        -> Result<Option<Option<Duration>>, ()> {
        if Instant::now() >= self.expires { return Err(()); }
        let outcome = match self.phase {
            MaintenancePhase::Deadline => db.next_deadline_delay().map(Some),
            MaintenancePhase::Expire { through_ms } => db.expire_at(through_ms).map(|mutated| {
                if mutated { changed.send_modify(|n| *n = n.wrapping_add(1)); }
                None
            }),
        };
        match outcome {
            Ok(delay) => Ok(Some(delay.flatten())),
            Err(error) => {
                let eligible = match (&self.phase, &error) {
                    (MaintenancePhase::Deadline, database::Fault::Sqlite(error)) =>
                        db.conn.is_autocommit() && super::checked_transaction::sqlite_code(error)
                            .is_some_and(|code| super::checked_transaction::known_transient(&code)),
                    (MaintenancePhase::Expire { .. }, _) => super::store_phase::retryable(&error),
                    _ => false,
                };
                if !eligible { return Err(()); }
                self.due = Instant::now().checked_add(Duration::from_millis(policy.backoff_ms))
                    .unwrap_or(self.expires).min(self.expires);
                Ok(None)
            }
        }
    }
}
fn phase(original: &OriginalRequest) -> u8 {
    match original.validation { None => 0, Some(super::ExternalProof::Native(_)) => 1,
        Some(super::ExternalProof::Repository(_)) => 2 }
}
fn ordinary_key(original: &OriginalRequest) -> Key {
    let identity = database::operation_identity(&original.request.command);
    Key { protected: false, phase: phase(original), mutation: identity.is_some(),
        identity: identity.unwrap_or(&original.request.request_id).to_owned(),
        body: serde_json::to_string(&original.request.command).expect("validated serializable operation"),
        provenance: format!("{:?}", original.invocation_path) }
}
fn protected_key(uid: u32, request: &crate::protected_release::Request) -> Key {
    use crate::protected_release::Command;
    let read = matches!(request.command, Command::GetAction { .. } | Command::GetReceipt { .. } | Command::View { .. });
    Key { protected: true, phase: 0, mutation: !read, identity: request.operation_id.clone(),
        body: serde_json::to_string(request).expect("validated serializable protected request"), provenance: uid.to_string() }
}
fn unavailable(original: &OriginalRequest) -> Reply {
    let outcome = match &original.request.command {
        Operation::ReconcileResult(input) => Outcome::Pending { operation_id: input.operation_id.clone() },
        _ => Outcome::Refused { code: "store_unavailable".into(),
            message: "store operation failed; reconcile original operation before retry".into() },
    };
    Reply { request_id: original.request.request_id.clone(), commit: None,
        observed_controller_revision: None, outcome }
}
fn ordinary_reply(waiters: Vec<Waiter<Response>>, reply: Reply) {
    for waiter in waiters {
        let mut correlated = reply.clone();
        correlated.request_id = waiter.request_id;
        let _ = waiter.reply.send(Response::Reply(correlated));
    }
}
fn protected_reply(waiters: Vec<Waiter<crate::protected_release::Reply>>, reply: crate::protected_release::Reply) {
    for waiter in waiters { let _ = waiter.reply.send(reply.clone()); }
}
impl Work {
    fn unavailable(self) {
        match self {
            Self::Ordinary { original, waiters, .. } => ordinary_reply(waiters, unavailable(&original)),
            Self::Protected { waiters, .. } => protected_reply(waiters,
                crate::protected_release::Reply::refused("store_unavailable_reconcile_original")),
        }
    }
    fn conflict(self) {
        match self {
            Self::Ordinary { original, waiters, .. } => ordinary_reply(waiters,
                Reply::refused(original.request.request_id, "operation_conflict", "pending original operation has different input or provenance")),
            Self::Protected { waiters, .. } => protected_reply(waiters,
                crate::protected_release::Reply::refused("operation_conflict")),
        }
    }
    fn append(&mut self, other: Work) {
        // Keep the first exact original request, UID, and external proof. Only
        // transport waiters move; a duplicate cannot replace proof with None.
        match (self, other) {
            (Self::Ordinary { waiters, .. }, Self::Ordinary { waiters: extra, .. }) => waiters.extend(extra),
            (Self::Protected { waiters, .. }, Self::Protected { waiters: extra, .. }) => waiters.extend(extra),
            _ => unreachable!("distinct owner domains cannot coalesce"),
        }
    }
}

fn enqueue(queue: &mut VecDeque<Item>, item: Item) {
    if let Some(pending) = queue.iter_mut().find(|pending| pending.key.same_slot(&item.key)) {
        if pending.key.exact(&item.key) {
            pending.expires = pending.expires.min(item.expires);
            pending.work.append(item.work);
        } else { item.work.conflict(); }
    } else { queue.push_back(item); }
}
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A transient failure of a platform-owned durable-store transaction MUST be retried with metadata-controlled backoff while retaining the original operation identity.
fn park(queue: &mut VecDeque<Item>, work: Work, key: Key, expires: Instant, policy: &StoreRetryLimits) {
    let now = Instant::now();
    if now >= expires { work.unavailable(); return; }
    let due = now.checked_add(Duration::from_millis(policy.backoff_ms)).unwrap_or(expires).min(expires);
    enqueue(queue, Item { work, key, expires, due });
}

fn run_item(db: &mut Database, changed: &watch::Sender<u64>, queue: &mut VecDeque<Item>,
    item: Item, policy: &StoreRetryLimits) {
    let Item { work, key, expires, .. } = item;
    if Instant::now() >= expires { work.unavailable(); return; }
    match work {
        Work::Ordinary { original, mut waiters, receipt_only } => {
            let request = original.request.clone();
            let invocation_path = original.invocation_path;
            let (response, mutated) = if receipt_only { db.handle_replay(original) } else { db.handle(original) };
            if mutated { changed.send_modify(|n| *n = n.wrapping_add(1)); }
            match response {
                Response::StoreFailure(failure) if eligible(failure.disposition()) => {
                    park(queue, Work::Ordinary { original: failure.original, waiters, receipt_only }, key, expires, policy);
                }
                Response::StoreFailure(failure) => ordinary_reply(waiters, failure.into_reply()),
                Response::Reply(reply) => {
                    let first = waiters.remove(0);
                    let _ = first.reply.send(Response::Reply(reply.clone()));
                    if reply.commit.is_some() && !waiters.is_empty() {
                        // Canonical receipt lookup, never fresh reply fan-out.
                        // Missing/corrupt evidence cannot repeat the operation.
                        for waiter in waiters {
                            let mut exact = request.clone(); exact.request_id = waiter.request_id.clone();
                            let original = OriginalRequest { request: exact, invocation_path, validation: None };
                            let mut replay_key = ordinary_key(&original); replay_key.phase = 3;
                            enqueue(queue, Item { work: Work::Ordinary { original, waiters: vec![waiter], receipt_only: true },
                                key: replay_key, expires, due: Instant::now() });
                        }
                    } else { ordinary_reply(waiters, reply); }
                }
                ticket @ (Response::Validation(_) | Response::Repository(_)) => {
                    // Exactly one one-use ticket goes back to its original
                    // caller. No receipt exists yet; duplicates get unresolved
                    // identity, never another external helper or fabricated ACK.
                    let first = waiters.remove(0);
                    let _ = first.reply.send(ticket);
                    let unresolved = if let Some(id) = request.command.mutation_id() {
                        Reply { request_id: request.request_id, commit: None, observed_controller_revision: None,
                            outcome: Outcome::Pending { operation_id: id.to_owned() } }
                    } else { Reply::refused(request.request_id, "store_unavailable", "original external phase is unresolved") };
                    ordinary_reply(waiters, unresolved);
                }
            }
        }
        Work::Protected { uid, request, mut waiters, receipt_only } => {
            let exact = request.clone();
            let (response, mutated) = if receipt_only { db.protected_replay_observed(uid, request) }
                else { db.protected_observed(uid, request) };
            if mutated { changed.send_modify(|n| *n = n.wrapping_add(1)); }
            match response {
                ProtectedResponse::StoreFailure(failure) if eligible(failure.disposition()) => {
                    let ProtectedFailure { uid, request, .. } = *failure;
                    park(queue, Work::Protected { uid, request, waiters, receipt_only }, key, expires, policy);
                }
                ProtectedResponse::StoreFailure(failure) => protected_reply(waiters, failure.into_reply()),
                ProtectedResponse::Reply(reply) => {
                    let first = waiters.remove(0);
                    let _ = first.reply.send(reply.clone());
                    if (mutated || reply.replayed) && !waiters.is_empty() {
                        for waiter in waiters {
                            let mut replay_key = protected_key(uid, &exact); replay_key.phase = 3;
                            enqueue(queue, Item { work: Work::Protected { uid, request: exact.clone(), waiters: vec![waiter], receipt_only: true },
                                key: replay_key, expires, due: Instant::now() });
                        }
                    } else { protected_reply(waiters, reply); }
                }
            }
        }
    }
}

pub(super) fn run(mut db: Database, receiver: mpsc::Receiver<Envelope>, changed: watch::Sender<u64>, policy: StoreRetryLimits) {
    let mut queue = VecDeque::<Item>::new();
    let mut incoming = None;
    let mut prefer_retry = false;
    let mut maintenance: Option<Maintenance> = None;
    loop {
        if incoming.is_none() {
            match receiver.try_recv() {
                Ok(message) => incoming = Some(message),
                Err(mpsc::TryRecvError::Disconnected) => break,
                Err(mpsc::TryRecvError::Empty) => {}
            }
        }
        // One parked maintenance episode owns its first deadline. Do not read
        // the still-overdue task deadline while parked: zero would bypass the
        // configured backoff on every command or receive timeout.
        let mut deadline_delay = None;
        if maintenance.is_none() {
            maintenance = Some(Maintenance::new(MaintenancePhase::Deadline, &policy));
        }
        if maintenance.as_ref().is_some_and(|phase| phase.due <= Instant::now()) {
            let phase = maintenance.as_mut().expect("maintenance phase");
            match phase.attempt(&mut db, &changed, &policy) {
                Ok(Some(delay)) => {
                    if matches!(phase.phase, MaintenancePhase::Expire { .. }) {
                        maintenance = None;
                        continue; // Recover the next future deadline before sleeping.
                    }
                    if matches!(phase.phase, MaintenancePhase::Deadline) && delay.is_some_and(|d| d.is_zero()) {
                        // This transition keeps the original lifetime; a due
                        // task cannot create a second budget after read retry.
                        phase.phase = MaintenancePhase::Expire { through_ms: (db.config.clock)() };
                        match phase.attempt(&mut db, &changed, &policy) {
                            Ok(Some(_)) => { maintenance = None; continue; },
                            Ok(None) => {}, Err(()) => break,
                        }
                    } else { deadline_delay = delay; maintenance = None; }
                },
                Ok(None) => {}, Err(()) => break,
            }
        }
        db.maintenance_pending = maintenance.is_some();
        let now = Instant::now();
        let due = queue.iter().position(|item| item.due <= now || item.expires <= now);
        if let Some(index) = due.filter(|_| prefer_retry || incoming.is_none()) {
            let item = queue.remove(index).expect("selected pending item");
            run_item(&mut db, &changed, &mut queue, item, &policy);
            prefer_retry = false;
        } else if let Some(envelope) = incoming.take() {
            let Envelope { message, admitted, permit, episode } = envelope;
            let expires = episode.as_ref().map(|e| e.expires).unwrap_or_else(||
                admitted.checked_add(Duration::from_millis(policy.operation_timeout_ms)).unwrap_or(admitted));
            match message {
                Message::Revision { reply } => {
                    let result = super::metadata::current(&database::StoreContext::new(&db.conn, &db.config.limits))
                        .map_err(|_| "durable revision unavailable".to_owned());
                    let _ = reply.send(result);
                }
                Message::Ordinary(OrdinaryMessage { original, reply }) => {
                    let key = ordinary_key(&original);
                    let waiter = Waiter { request_id: original.request.request_id.clone(), reply, _permit: permit, _episode: episode };
                    let work = Work::Ordinary { original, waiters: vec![waiter], receipt_only: false };
                    let item = Item { work, key, expires, due: now };
                    if queue.iter().any(|pending| pending.key.same_slot(&item.key)) { enqueue(&mut queue, item); }
                    else { run_item(&mut db, &changed, &mut queue, item, &policy); }
                }
                Message::Protected { uid, request, reply } => {
                    let key = protected_key(uid, &request);
                    let waiter = Waiter { request_id: request.operation_id.clone(), reply, _permit: permit, _episode: episode };
                    let work = Work::Protected { uid, request, waiters: vec![waiter], receipt_only: false };
                    let item = Item { work, key, expires, due: now };
                    if queue.iter().any(|pending| pending.key.same_slot(&item.key)) { enqueue(&mut queue, item); }
                    else { run_item(&mut db, &changed, &mut queue, item, &policy); }
                }
            }
            prefer_retry = true;
        } else {
            let next_retry = queue.iter().map(|item| item.due.min(item.expires).saturating_duration_since(Instant::now())).min();
            let deadline_delay = if let Some(phase) = &maintenance {
                Some(phase.due.min(phase.expires).saturating_duration_since(Instant::now()))
            } else { deadline_delay };
            let wait = match (next_retry, deadline_delay) { (Some(a), Some(b)) => Some(a.min(b)), (a, b) => a.or(b) };
            match wait {
                Some(delay) => match receiver.recv_timeout(delay) {
                    Ok(message) => incoming = Some(message),
                    Err(mpsc::RecvTimeoutError::Timeout) => {},
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                },
                None => match receiver.recv() { Ok(message) => incoming = Some(message), Err(_) => break },
            }
        }
        if !db.conn.is_autocommit() { break; }
    }
    // Channel closure is the owned shutdown signal. Parked items hold no sender,
    // so they cannot delay it or dispatch a retry after shutdown is observed.
    for item in queue { item.work.unavailable(); }
}

#[cfg(test)]
#[path = "store_retry_owner_tests.rs"]
mod tests;
