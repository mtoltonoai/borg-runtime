//! The single durable task owner. `handle` is an application boundary: its caller
//! must already be authenticated by the configured private host/HTTP adapter.
//! Worker identity strings are correlation data, not authentication credentials.
mod agents;
mod identity_administration;
mod task_creation;
mod task_submission;
mod task_handoffs;
mod definition_updates;
mod identity_limit_fields;
mod governance;
mod governance_control;
mod governance_validation;
mod collaboration_calls;
mod collaboration_workflow;
mod collaboration_notifications;
mod collaboration_document_tools;
mod collaboration_documents;
mod collaboration_reviews;
mod collaboration_questions;
mod collaboration_work;
mod collaboration_routing;
mod collaboration_finalizer;
mod collaboration_queries;
mod read_pages;
mod read_pagination;
pub(crate) mod compatibility;
mod credential_admission;
mod credits;
mod charters;
mod charter_definitions;
mod checked_transaction;
mod store_retry;
mod store_phase;
pub use store_phase::StartupCancellation;
mod database;
pub mod machine_resources;
mod pull_request;
#[cfg(test)]
mod pull_request_tests;
mod governance_completion;
mod local_storage;
mod memory_notifications;
mod native_retry_state;
mod native_request_coordination;
mod native_request_diagnostics;
#[cfg(test)]
mod native_diagnostic_compatibility_tests;
mod native_retry_validation;
mod metadata;
mod read_retention;
mod operations;
mod protected;
mod repositories;
mod repository_queries;
mod sessions;
mod tracking;
mod validation;
mod workflow_schema;
mod application_schema;

use crate::protocol::{Operation, Outcome, Reply, Request, WIRE_REVISION};
use crate::collaboration::workflow::TrustedHostPath;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, mpsc},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{oneshot, watch};

#[derive(Clone)]
pub struct Controller {
    inner: Arc<Inner>,
}

struct Inner {
    sender: mpsc::SyncSender<Envelope>,
    pending: Option<Arc<tokio::sync::Semaphore>>,
    episodes: store_retry::Episodes,
    changes: watch::Receiver<u64>,
    waiters: tokio::sync::Semaphore,
    ui_waiters: tokio::sync::Semaphore,
    validations: tokio::sync::Semaphore,
    #[cfg(debug_assertions)]
    finalization_probe: std::sync::Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
    config: ControllerConfig,
    validation_host: Arc<crate::native::ValidationHost>,
}

/// Deployment limits, not extra per-build admission permits. The clock is an
/// owned dependency so deadline races can be tested without sleeping or polling.
#[derive(Clone)]
pub struct ControllerConfig {
    pub machine_resources: Option<machine_resources::Config>,
    pub limits: Arc<crate::limits::RuntimeLimits>,
    pub protected_release: Option<crate::protected_release::Config>,
    /// Trusted existing bare repositories. Empty disables source material reads.
    pub repository_read: crate::repository_read::Config,
    /// Trusted installed application binary. Never selected by a task or environment.
    pub validation_executable: PathBuf,
    pub clock: Arc<dyn Fn() -> u64 + Send + Sync>,
    /// Qualification only: true is consumed by SQLite's next receipt-write COMMIT,
    /// which asks SQLite itself to roll back. Installed only after valid startup.
    #[cfg(debug_assertions)]
    pub qualification_fail_next_commit: Option<Arc<std::sync::atomic::AtomicBool>>,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            machine_resources: None,
            limits: Arc::new(
                crate::limits::RuntimeLimits::seed().expect("invalid installed limits metadata"),
            ),
            protected_release: None,
            repository_read: crate::repository_read::Config::default(),
            validation_executable: std::env::current_exe().unwrap_or_default(),
            #[cfg(debug_assertions)]
            qualification_fail_next_commit: None,
            clock: Arc::new(|| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
                    .unwrap_or(0)
            }),
        }
    }
}

impl std::ops::Deref for ControllerConfig {
    type Target = crate::limits::ControllerLimits;
    fn deref(&self) -> &Self::Target {
        &self.limits.controller
    }
}
impl ControllerConfig {
    pub fn command_wait(&self) -> Duration {
        Duration::from_millis(self.command_wait_ms)
    }
}
enum Message {
    Revision {
        reply: oneshot::Sender<Result<u64, String>>,
    },
    Ordinary(OrdinaryMessage),
    Protected {
        uid: u32,
        request: crate::protected_release::Request,
        reply: oneshot::Sender<crate::protected_release::Reply>,
    },
}
struct Envelope {
    message: Message,
    admitted: std::time::Instant,
    episode: Option<Arc<store_retry::Episode>>,
    // Counts queued, active and parked transport waiters together. It never
    // retains Controller/sender and therefore cannot keep its owner alive.
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}
impl Inner {
    // Traceability scope: Configured owner capacity counts queued, active and parked waiters; a full semaphore or bounded channel refuses admission.
    //= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
    //# Admission MUST enforce configured queue and resource bounds.
    fn send(&self, message: Message) -> Result<(), mpsc::TrySendError<Message>> {
        let permit = if let Some(capacity) = &self.pending {
            match capacity.clone().try_acquire_owned() {
                Ok(p) => Some(p),
                Err(_) => return Err(mpsc::TrySendError::Full(message)),
            }
        } else { None };
        let admitted = std::time::Instant::now();
        let episode = self.config.store_retry.as_ref()
            .and_then(|policy| self.episodes.admit(&message, admitted, policy));
        let envelope = Envelope { message, admitted, permit, episode };
        self.sender.try_send(envelope).map_err(|error| match error {
            mpsc::TrySendError::Full(e) => mpsc::TrySendError::Full(e.message),
            mpsc::TrySendError::Disconnected(e) => mpsc::TrySendError::Disconnected(e.message),
        })
    }
}
enum ExternalProof {
    Native(validation::Proof),
    Repository(repository_queries::Proof),
}
struct OrdinaryMessage {
    original: OriginalRequest,
    reply: oneshot::Sender<database::Response>,
}
/// Retained only across the existing in-process owner boundary. A future retry
/// consumer must select an applicable policy and reenter with these exact inputs;
/// it must not rerun external validation or replace the original invocation path.
struct OriginalRequest {
    request: Request,
    invocation_path: TrustedHostPath,
    validation: Option<ExternalProof>,
}

pub(crate) fn minimum_collaboration_result_bytes(
    transport: &crate::limits::TransportLimits,
) -> Result<usize, String> {
    metadata::minimum_collaboration_result_bytes(transport)
}

impl Controller {
    pub fn limits(&self) -> &crate::limits::RuntimeLimits {
        &self.inner.config.limits
    }
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        Self::open_with_config(path, ControllerConfig::default())
    }

    pub fn open_with_config(
        path: impl AsRef<Path>,
        config: ControllerConfig,
    ) -> Result<Self, String> {
        Self::open_with_config_cancellable(path, config, StartupCancellation::default())
    }

    pub fn open_with_config_cancellable(
        path: impl AsRef<Path>, config: ControllerConfig, cancellation: StartupCancellation,
    ) -> Result<Self, String> {
        cancellation.check()?;
        if let Some(c) = &config.protected_release {
            c.validate(&config.limits.interface)?;
        }
        config.limits.validate()?;
        if let Some(observations) = &config.machine_resources {
            observations.profile.validate()?;
            if observations.profile.local().role != crate::machine_resources::Role::Controller {
                return Err("controller host profile must select its controller row".into());
            }
        }
        if !config.validation_executable.is_absolute() || !config.validation_executable.is_file() {
            return Err("controller requires an absolute trusted validation executable".into());
        }
        let path = path.as_ref().to_path_buf();
        let (sender, receiver) = mpsc::sync_channel::<Envelope>(config.queue_capacity);
        let (changed, changes) = watch::channel(0u64);
        let (started, start) = mpsc::sync_channel(1);
        let settings = config.clone();
        let startup_cancellation = cancellation.clone();
        let owner = thread::Builder::new()
            .name("borg-task-database".into())
            .spawn(move || {
                let _startup_owner = startup_cancellation.owner();
                let mut db = match database::Database::open_cancellable(&path, settings, &startup_cancellation) {
                    Ok(db) => {
                        if let Err(error) = startup_cancellation.check() {
                            drop(db);
                            let _ = started.send(Err(error));
                            return;
                        }
                        #[cfg(test)]
                        if let Err(error) = governance_ui_test_fixture::after_validated_open(&path, &db.conn) {
                            drop(db);
                            let _ = started.send(Err(error));
                            return;
                        }
                        let _ = started.send(Ok(db.validation_host.clone()));
                        db
                    }
                    Err(e) => {
                        let _ = started.send(Err(e));
                        return;
                    }
                };
                if let Some(policy) = db.config.limits.controller.store_retry.clone() {
                    store_retry::run(db, receiver, changed, policy);
                    return;
                }
                loop {
                    // This timeout is the next actual task deadline, not a periodic
                    // database/liveness scan. All other wakeups are queued commands.
                    let message = match db.next_deadline_delay() {
                        Ok(Some(delay)) => match receiver.recv_timeout(delay) {
                            Ok(m) => Some(m),
                            Err(mpsc::RecvTimeoutError::Timeout) => None,
                            Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        },
                        Ok(None) | Err(_) => match receiver.recv() {
                            Ok(m) => Some(m),
                            Err(_) => break,
                        },
                    };
                    let message = message.map(|envelope| envelope.message);
                    let maintenance_changed = match db.expire() {
                        Ok(changed) => changed,
                        Err(_) => break,
                    };
                    if let Some(Message::Protected {
                        uid,
                        request,
                        reply,
                    }) = message
                    {
                        let (result, mutated) = db.protected(uid, request);
                        if mutated || maintenance_changed {
                            changed.send_modify(|n| *n = n.wrapping_add(1));
                        }
                        let _ = reply.send(result);
                    } else if let Some(Message::Ordinary(message)) = message {
                        let (reply, mutated) = db.handle(message.original);
                        if mutated || maintenance_changed {
                            changed.send_modify(|n| *n = n.wrapping_add(1));
                        }
                        // A cancelled HTTP future does not cancel a submitted SQL
                        // mutation: the caller reconciles using its durable ID.
                        let _ = message.reply.send(reply);
                    } else if let Some(Message::Revision { reply }) = message {
                        let result = metadata::current(&database::StoreContext::new(
                            &db.conn,
                            &db.config.limits,
                        ))
                        .map_err(|_| "durable revision unavailable".to_string());
                        if maintenance_changed {
                            changed.send_modify(|n| *n = n.wrapping_add(1));
                        }
                        let _ = reply.send(result);
                    } else if maintenance_changed {
                        changed.send_modify(|n| *n = n.wrapping_add(1));
                    }
                }
                // Dropping the last Controller closes the queue. The owner finishes
                // already submitted work, then closes its sole SQLite connection.
            })
            .map_err(|e| e.to_string())?;
        let validation_host = match start.recv() {
            Ok(Ok(host)) => host,
            result => {
                // Startup cancellation is an owned signal, not cancellation of
                // this wait. Observe actual owner exit before returning.
                let _ = owner.join();
                return Err(match result { Ok(Err(error)) => error,
                    _ => "database owner stopped during startup".into() });
            }
        };
        Ok(Self {
            inner: Arc::new(Inner {
                sender,
                pending: config.store_retry.as_ref().map(|_| Arc::new(tokio::sync::Semaphore::new(config.queue_capacity))),
                episodes: store_retry::Episodes::default(),
                validation_host,
                changes,
                waiters: tokio::sync::Semaphore::new(config.worker_waiters),
                ui_waiters: tokio::sync::Semaphore::new(config.ui_waiters),
                validations: tokio::sync::Semaphore::new(config.validation_parallelism),
                #[cfg(debug_assertions)]
                finalization_probe: std::sync::Mutex::new(None),
                config,
            }),
        })
    }

    /// Durable revision observations for the UI. The process-local watch is only
    /// a wakeup hint, never the revision/cursor and never a new durable record.
    pub async fn wait_for_change(
        &self,
        after_revision: u64,
    ) -> Result<crate::collaboration::Change, String> {
        let mut changes = self.inner.changes.clone();
        let deadline = tokio::time::Instant::now() + self.inner.config.command_wait();
        let mut permit = None;
        loop {
            // Arm before the owner read so a racing commit cannot be missed.
            changes.borrow_and_update();
            let (reply, receive) = oneshot::channel();
            self.inner
                .send(Message::Revision { reply })
                .map_err(|e| match e {
                    mpsc::TrySendError::Full(_) => {
                        "controller request queue is full: controller.queue_capacity".to_string()
                    }
                    mpsc::TrySendError::Disconnected(_) => "database owner is closed".to_string(),
                })?;
            let revision = tokio::time::timeout_at(deadline, receive)
                .await
                .map_err(|_| {
                    "durable revision observation timed out: controller.command_wait_ms".to_string()
                })?
                .map_err(|_| "database owner is closed".to_string())??;
            let observed = crate::collaboration::Change {
                revision,
                changed: revision != after_revision,
                reset: revision < after_revision,
            };
            if observed.changed {
                return Ok(observed);
            }
            if permit.is_none() {
                permit = Some(self.inner.ui_waiters.try_acquire().map_err(|_| {
                    "UI wait capacity exhausted: controller.ui_waiters".to_string()
                })?);
            }
            match tokio::time::timeout_at(deadline, changes.changed()).await {
                Ok(Ok(())) => continue,
                Ok(Err(_)) => return Err("database owner is closed".into()),
                // This is an actual durable observation, not a promise that no
                // later commit exists. The next request catches any later one.
                Err(_) => return Ok(observed),
            }
        }
    }

    pub(crate) async fn protected(
        &self,
        uid: u32,
        request: crate::protected_release::Request,
    ) -> crate::protected_release::Reply {
        let (reply, receive) = oneshot::channel();
        match self.inner.send(Message::Protected {
            uid,
            request,
            reply,
        }) {
            Ok(()) => receive
                .await
                .unwrap_or_else(|_| crate::protected_release::Reply::refused("store_unavailable")),
            Err(mpsc::TrySendError::Full(_)) => crate::protected_release::refusal_with_limit(
                "capacity_or_unavailable",
                "controller request queue is full: controller.queue_capacity",
            ),
            Err(mpsc::TrySendError::Disconnected(_)) => {
                crate::protected_release::Reply::refused("capacity_or_unavailable")
            }
        }
    }

    async fn dispatch(
        &self,
        request: Request,
        validation: Option<ExternalProof>,
        invocation_path: TrustedHostPath,
    ) -> database::Response {
        let id = request.request_id.clone();
        let (reply, receive) = oneshot::channel();
        match self
            .inner
            .send(Message::Ordinary(OrdinaryMessage {
                original: OriginalRequest { request, validation, invocation_path },
                reply,
            })) {
            Ok(()) => receive.await.unwrap_or_else(|_| {
                database::Response::Reply(Reply::refused(
                    id,
                    "store_unavailable",
                    "database owner unavailable; reconcile original operation",
                ))
            }),
            Err(mpsc::TrySendError::Full(_)) => database::Response::Reply(Reply::refused(
                id,
                "capacity",
                "controller request queue is full: controller.queue_capacity",
            )),
            Err(mpsc::TrySendError::Disconnected(_)) => database::Response::Reply(Reply::refused(
                id,
                "store_unavailable",
                "database owner is closed",
            )),
        }
    }

    //= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
    //# A controller transaction MUST NOT remain open across worker, provider or repository I/O.
    async fn once(&self, request: Request, invocation_path: TrustedHostPath) -> Reply {
        let response = self.dispatch(request.clone(), None, invocation_path).await;
        let ticket = match response {
            database::Response::Reply(reply) => return reply,
            // The optional owner scheduler handles eligible failures internally.
            // Preserve the existing outcome for failures returned by that boundary.
            database::Response::StoreFailure(failure) => return failure.into_reply(),
            database::Response::Validation(ticket) => ticket,
            database::Response::Repository(ticket) => {
                // The owned completion survives cancellation of this transport future.
                // It awaits the reader and cleanup before final DB dispatch.
                let controller = self.clone();
                let id = request.request_id.clone();
                return tokio::spawn(
                    async move { controller.finish_repository(request, ticket, invocation_path).await },
                )
                .await
                .unwrap_or_else(|_| {
                    Reply::refused(
                        id,
                        "repository_read_unavailable",
                        "repository read completion unavailable; reconcile original occurrence",
                    )
                });
            }
        };
        // The owner has returned from process(): its transaction was rolled back
        // before this await. A small finite set of tickets can own CPU work.
        let permit = self.inner.validations.try_acquire();
        let proof = if permit.is_ok() {
            let proof = ticket.execute(&self.inner.validation_host, &self.inner.config.repository_read).await;
            #[cfg(debug_assertions)]
            {
                let probe = self
                    .inner
                    .finalization_probe
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take();
                if let Some((ready, resume)) = probe {
                    let _ = ready.send(());
                    let _ = resume.await;
                }
            }
            proof
        } else {
            ticket.no_capacity()
        };
        match self
            .dispatch(request.clone(), Some(ExternalProof::Native(proof)), invocation_path)
            .await
        {
            database::Response::Reply(reply) => reply,
            database::Response::StoreFailure(failure) => failure.into_reply(),
            database::Response::Validation(_) | database::Response::Repository(_) => {
                Reply::refused(
                    request.request_id,
                    "invalid_validation",
                    "finalization requested a second proof",
                )
            }
        }
    }

    async fn finish_repository(
        &self,
        request: Request,
        ticket: Box<repository_queries::Ticket>,
        invocation_path: TrustedHostPath,
    ) -> Reply {
        let permit = self.inner.validations.try_acquire();
        let proof = if permit.is_ok() {
            let proof = ticket
                .execute(&self.inner.config.repository_read, &self.inner.config.clock)
                .await;
            #[cfg(debug_assertions)]
            {
                let probe = self
                    .inner
                    .finalization_probe
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take();
                if let Some((ready, resume)) = probe {
                    let _ = ready.send(());
                    let _ = resume.await;
                }
            }
            proof
        } else {
            ticket.no_capacity()
        };
        match self
            .dispatch(request.clone(), Some(ExternalProof::Repository(proof)), invocation_path)
            .await
        {
            database::Response::Reply(reply) => reply,
            database::Response::StoreFailure(failure) => failure.into_reply(),
            _ => Reply::refused(
                request.request_id,
                "invalid_repository_read",
                "finalization requested a second proof",
            ),
        }
    }

    /// Debug-only qualification gate after a real validator result, before final
    /// DB admission. The gate cannot issue or alter a validation result.
    #[cfg(debug_assertions)]
    pub fn qualification_pause_next_finalization(
        &self,
    ) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (ready, started) = oneshot::channel();
        let (resume, released) = oneshot::channel();
        let mut slot = self
            .inner
            .finalization_probe
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        assert!(
            slot.is_none(),
            "one finalization qualification gate at a time"
        );
        *slot = Some((ready, released));
        (started, resume)
    }

    pub async fn handle(&self, request: Request) -> Reply {
        self.handle_from(request, TrustedHostPath::OperatorApi).await
    }

    /// Only the in-process product adapter selects browser provenance. The
    /// serialized request cannot assert this path or authenticated-person identity.
    pub(crate) async fn handle_browser(&self, request: Request) -> Reply {
        self.handle_from(request, TrustedHostPath::Browser).await
    }

    async fn handle_from(&self, request: Request, invocation_path: TrustedHostPath) -> Reply {
        let limits = &self.inner.config.limits;
        if request.protocol_revision != WIRE_REVISION {
            return Reply::refused(
                request.request_id,
                "unsupported_version",
                "unsupported task wire revision",
            );
        }
        if !database::valid_id(limits, &request.request_id)
            || bounded_json_size(&request, self.limits().transport.body_bytes).is_err()
        {
            return Reply::refused(
                request.request_id,
                "invalid_input",
                "invalid request identity or oversized request",
            );
        }
        if let Operation::CallCollaboration { call } = &request.command {
            if call.caller.validate(&limits.transport).is_err()
                || !database::valid_id(limits, &call.operation_id)
                || !database::valid_id(limits, &call.logical_call_id)
                || !database::valid_id(limits, &call.tool)
                || !call.arguments.is_object()
            {
                return Reply::refused(
                    &request.request_id,
                    "invalid_input",
                    "invalid bounded collaboration envelope",
                );
            }
        }
        // Invocation policy belongs to the accepted task and is checked only
        // after original receipt lookup. Current service transport bounds above
        // still apply, but changing MCP defaults cannot reinterpret a replay.
        if credential_admission::rejected(&request.command) {
            // This must precede dispatch: ordinary transactional refusals
            // record their original input for replay, including its strings.
            // Keep the error static and leave both receipt and task unchanged.
            return Reply::refused(
                request.request_id,
                "invalid_input",
                "invalid or credential-bearing repository identities are not accepted",
            );
        }
        if !matches!(request.command,
            Operation::AwaitCommands { .. } | Operation::ObserveNativeRequest { .. }) {
            return self.once(request, invocation_path).await;
        }
        // Arm before reading. A commit racing the read/sleep gap changes this
        // retained watch version, so changed() completes without losing the wake.
        let mut changed = self.inner.changes.clone();
        let mut waiting_slot = None;
        let deadline = tokio::time::Instant::now() + self.inner.config.command_wait();
        loop {
            changed.borrow_and_update();
            let result = self.once(request.clone(), invocation_path).await;
            let awaiting_command = matches!(&result.outcome,
                Outcome::Pending { operation_id }
                if matches!(&request.command, Operation::AwaitCommands { operation_id: expected, .. }
                    if operation_id == expected));
            let awaiting_readiness = matches!((&request.command, &result.outcome),
                (Operation::ObserveNativeRequest { acquire_operation_id: expected, .. },
                 Outcome::Ok { data: crate::protocol::Data::NativeRequestReadiness {
                    acquire_operation_id, changed: false } }) if acquire_operation_id == expected);
            if !awaiting_command && !awaiting_readiness {
                return result;
            }
            // Committed outcomes and changed readiness return before waiter
            // admission. A readiness read never creates an operation receipt;
            // without a free wait slot, its unchanged snapshot returns normally.
            if waiting_slot.is_none() {
                match self.inner.waiters.try_acquire() {
                    Ok(slot) => waiting_slot = Some(slot),
                    Err(_) if awaiting_readiness => return result,
                    Err(_) => {
                        return Reply::refused(
                            &request.request_id,
                            "capacity",
                            "controller command wait capacity is full: controller.worker_waiters",
                        );
                    }
                }
            }
            tokio::select! {
                answer = changed.changed() => if answer.is_err() { return Reply::refused(&request.request_id, "store_unavailable", "database owner is closed"); },
                _ = tokio::time::sleep_until(deadline) => return result,
            }
        }
    }
}

pub(crate) use crate::protocol::bounded_json_size;

#[cfg(test)]
mod waiter_replay_tests {
    use super::*;
    use crate::protocol::Worker;

    #[tokio::test]
    async fn committed_replay_precedes_saturated_waiter_admission() {
        let limits = &crate::limits::RuntimeLimits::seed().expect("test metadata");
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory =
            std::env::temp_dir().join(format!("borg-waiter-replay-{}-{stamp}", std::process::id()));
        std::fs::create_dir(&directory).unwrap();
        let mut metadata = crate::limits::RuntimeLimits::seed().unwrap();
        metadata.controller.max_workers = 1;
        metadata.controller.worker_waiters = 1;
        metadata.controller.command_wait_ms = 30_000;
        let c = Controller::open_with_config(
            directory.join("tasks.db"),
            ControllerConfig {
                limits: Arc::new(metadata),
                ..ControllerConfig::default()
            },
        )
        .unwrap();
        let request = |id: &str, command| Request {
            protocol_revision: WIRE_REVISION,
            request_id: id.into(),
            command,
        };
        let original = Operation::AwaitCommands {
            operation_id: "original-refusal".into(),
            worker_id: "later-worker".into(),
            incarnation: "inc".into(),
            acknowledged: vec![],
        };
        let refused = c
            .handle(request("before-registration", original.clone()))
            .await
            .outcome;
        assert!(matches!(&refused, Outcome::Refused { code, .. } if code == "unknown_worker"));
        assert!(matches!(
            c.handle(request(
                "register",
                Operation::RegisterWorker {
                    operation_id: "registration".into(),
                    worker: Worker {
                        id: "later-worker".into(),
                        incarnation: "inc".into(),
                        coding_capacity: 1,
                        native_capacity: 0,
                    },
                }
            ))
            .await
            .outcome,
            Outcome::Ok { .. }
        ));
        let idle_owner = c.clone();
        let idle = tokio::spawn(async move {
            idle_owner
                .handle(Request {
                    protocol_revision: WIRE_REVISION,
                    request_id: "idle".into(),
                    command: Operation::AwaitCommands {
                        operation_id: "pending-exchange".into(),
                        worker_id: "later-worker".into(),
                        incarnation: "inc".into(),
                        acknowledged: vec![],
                    },
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while c.inner.waiters.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the actual pending request must hold the sole waiter slot");
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(2),
                c.handle(request("replay", original))
            )
            .await
            .unwrap()
            .outcome,
            refused
        );
        assert_eq!(c.inner.waiters.available_permits(), 0);
        idle.abort();
        assert!(idle.await.unwrap_err().is_cancelled());
    }
}

#[cfg(test)]
mod limits_tests;

#[cfg(test)]
pub(crate) mod governance_ui_test_fixture;

#[cfg(test)]
mod store_retry_tests;
