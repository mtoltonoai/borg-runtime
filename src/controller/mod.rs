//! The single durable task owner. `handle` is an application boundary: its caller
//! must already be authenticated by the configured private host/HTTP adapter.
//! Worker identity strings are correlation data, not authentication credentials.
mod agents;
mod collaboration_calls;
mod collaboration_queries;
pub(crate) mod compatibility;
mod credential_admission;
mod credits;
mod database;
mod local_storage;
mod memory_notifications;
mod metadata;
mod operations;
mod protected;
mod repositories;
mod repository_queries;
mod sessions;
mod tracking;
mod validation;

use crate::protocol::{Operation, Outcome, Reply, Request, WIRE_REVISION};
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
    sender: mpsc::SyncSender<Message>,
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
enum ExternalProof {
    Native(validation::Proof),
    Repository(repository_queries::Proof),
}
struct OrdinaryMessage {
    request: Request,
    validation: Option<ExternalProof>,
    reply: oneshot::Sender<database::Response>,
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
        if let Some(c) = &config.protected_release {
            c.validate(&config.limits.interface)?;
        }
        config.limits.validate()?;
        if !config.validation_executable.is_absolute() || !config.validation_executable.is_file() {
            return Err("controller requires an absolute trusted validation executable".into());
        }
        let path = path.as_ref().to_path_buf();
        let (sender, receiver) = mpsc::sync_channel::<Message>(config.queue_capacity);
        let (changed, changes) = watch::channel(0u64);
        let (started, start) = mpsc::sync_channel(1);
        let settings = config.clone();
        thread::Builder::new()
            .name("borg-task-database".into())
            .spawn(move || {
                let mut db = match database::Database::open(&path, settings) {
                    Ok(db) => {
                        let _ = started.send(Ok(db.validation_host.clone()));
                        db
                    }
                    Err(e) => {
                        let _ = started.send(Err(e));
                        return;
                    }
                };
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
                        let (reply, mutated) = db.handle(message.request, message.validation);
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
        let validation_host = start
            .recv()
            .map_err(|_| "database owner stopped during startup".to_string())??;
        Ok(Self {
            inner: Arc::new(Inner {
                sender,
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
                .sender
                .try_send(Message::Revision { reply })
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
        match self.inner.sender.try_send(Message::Protected {
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
    ) -> database::Response {
        let id = request.request_id.clone();
        let (reply, receive) = oneshot::channel();
        match self
            .inner
            .sender
            .try_send(Message::Ordinary(OrdinaryMessage {
                request,
                validation,
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
    async fn once(&self, request: Request) -> Reply {
        let response = self.dispatch(request.clone(), None).await;
        let ticket = match response {
            database::Response::Reply(reply) => return reply,
            database::Response::Validation(ticket) => ticket,
            database::Response::Repository(ticket) => {
                // The owned completion survives cancellation of this transport future.
                // It awaits the reader and cleanup before final DB dispatch.
                let controller = self.clone();
                let id = request.request_id.clone();
                return tokio::spawn(
                    async move { controller.finish_repository(request, ticket).await },
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
            let proof = ticket.execute(&self.inner.validation_host).await;
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
            .dispatch(request.clone(), Some(ExternalProof::Native(proof)))
            .await
        {
            database::Response::Reply(reply) => reply,
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
            .dispatch(request.clone(), Some(ExternalProof::Repository(proof)))
            .await
        {
            database::Response::Reply(reply) => reply,
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
        if !matches!(request.command, Operation::AwaitCommands { .. }) {
            return self.once(request).await;
        }
        // Arm before reading. A commit racing the read/sleep gap changes this
        // retained watch version, so changed() completes without losing the wake.
        let mut changed = self.inner.changes.clone();
        let mut waiting_slot = None;
        let deadline = tokio::time::Instant::now() + self.inner.config.command_wait();
        loop {
            changed.borrow_and_update();
            let result = self.once(request.clone()).await;
            if !matches!(&result.outcome, Outcome::Pending { operation_id } if matches!(&request.command, Operation::AwaitCommands { operation_id: expected, .. } if operation_id == expected))
            {
                return result;
            }
            // Already committed outcomes must replay even when every idle
            // waiting slot is occupied. Only uncommitted Pending needs a slot.
            if waiting_slot.is_none() {
                match self.inner.waiters.try_acquire() {
                    Ok(slot) => waiting_slot = Some(slot),
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
