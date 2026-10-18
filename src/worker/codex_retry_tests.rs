//! Direct durable Owner cuts. The observer responses below are controlled local
//! acknowledgments, not claims that a controller or supplied executor ran.
use super::*;
use crate::supplied_outer::Wait;
use std::future::Future;
use tokio::sync::{oneshot, watch};

const THREAD: &str = "retry-cut-thread";
const TURN: &str = "retry-cut-original-turn";

struct Fixture {
    state: PathBuf,
    cwd: PathBuf,
    control: Control,
    policy: RetryPolicy,
    binding: Value,
    _cancel: watch::Sender<bool>,
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "borg-supplied-retry-cut-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        std::fs::create_dir(&root).unwrap();
        process::private_directory(&root).unwrap();
        let cwd = root.join("worktree");
        let state = root.join("codex-home");
        process::private_directory(&cwd).unwrap();
        process::private_directory(&state).unwrap();
        std::fs::write(
            cwd.join("dirty-sentinel"),
            b"uncheckpointed original bytes\n",
        )
        .unwrap();
        let policy = RetryPolicy {
            initial_backoff_ms: 1,
            backoff_multiplier: 2,
            max_backoff_ms: 4,
            jitter_ms: 1,
        };
        let (cancel, receiver) = watch::channel(false);
        let mut control = crate::worker::test_control(process::now_ms() + 10_000, receiver);
        Arc::make_mut(&mut control.runtime_limits)
            .execution
            .coding
            .rate_limit_retry = Some(policy.clone());
        let binding = json!({"caller":{
            "task_id":"retry-cut-task","attempt_id":"retry-cut-attempt","generation":1,
            "worker_id":"retry-cut-worker","incarnation":"retry-cut-incarnation"
        },"session_id":"retry-cut-session"});
        Self {
            state,
            cwd,
            control,
            policy,
            binding,
            _cancel: cancel,
        }
    }
    fn owner(&self) -> Owner {
        Owner::new(
            &self.state,
            &self.cwd,
            THREAD,
            &self.policy,
            self.binding.clone(),
            &self.control,
        )
        .unwrap()
    }
    fn reopen(&self) -> Owner {
        // Reconstruct only from the actual bounded production reader. This
        // tests retained method guards; it does not grant worker restart authority.
        Owner {
            state: self.state.clone(),
            record: read(&self.state, &self.control).unwrap().unwrap(),
        }
    }
    fn bytes(&self) -> Vec<u8> {
        std::fs::read(self.state.join(JOURNAL)).unwrap()
    }
    fn cleanable(&self) -> bool {
        cleanup_ready(
            &self.state,
            &self.cwd,
            self.control.deadline_ms,
            &self.control.worker_limits,
        )
        .unwrap()
    }
    fn sentinel(&self) {
        assert_eq!(
            std::fs::read(self.cwd.join("dirty-sentinel")).unwrap(),
            b"uncheckpointed original bytes\n"
        );
    }
    fn wait(&self) -> Wait {
        let admitted_at_ms = process::now_ms();
        let delay = self
            .policy
            .delay_ms(
                1,
                crate::supplied_outer::jitter_sample(THREAD, 2),
                admitted_at_ms,
                self.control.deadline_ms,
            )
            .unwrap();
        Wait {
            index: 2,
            rate_failures: 1,
            admitted_at_ms,
            not_before_ms: admitted_at_ms + delay,
        }
    }
}

async fn bounded<T>(f: &Fixture, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(
        Duration::from_millis(f.control.deadline_ms.saturating_sub(process::now_ms())),
        future,
    )
    .await
    .expect("owned event exchange must finish within the original fixture deadline")
}
type Response = oneshot::Sender<std::result::Result<Option<Wait>, String>>;
async fn event(receiver: &mut mpsc::Receiver<Event>) -> (Outer, Response) {
    loop {
        match receiver.recv().await.expect("original report event") {
            Event::SuppliedOuter {
                observation,
                acknowledged,
            } => return (observation, acknowledged),
            Event::Progress(text) => assert!(text.starts_with("Supplied outer ")),
            _ => panic!("unexpected event at original supplied phase"),
        }
    }
}

fn rate_terminal() -> Value {
    json!({"method":"turn/completed","params":{"threadId":THREAD,"turn":{
        "id":TURN,"status":"failed","error":{"codexErrorInfo":"rateLimitExceeded","message":"controlled rate terminal"}
    }}})
}
fn wait_observation() -> Outer {
    Outer::Waiting {
        index: 2,
        thread_id: THREAD.into(),
        previous_turn_id: TURN.into(),
        cause: Cause::RateLimit { retry_index: 1 },
    }
}
fn dispatch_observation() -> Outer {
    Outer::Dispatch {
        index: 2,
        thread_id: THREAD.into(),
        previous_turn_id: TURN.into(),
        cause: Cause::RateLimit { retry_index: 1 },
    }
}
async fn terminal_owner(f: &Fixture) -> Owner {
    let mut owner = f.owner();
    owner.before_send(&f.control).unwrap();
    let (events, mut receiver) = mpsc::channel(2);
    let observed = async {
        let (value, response) = event(&mut receiver).await;
        assert_eq!(
            value,
            Outer::Acknowledged {
                index: 1,
                thread_id: THREAD.into(),
                turn_id: TURN.into()
            }
        );
        let retained = read(&f.state, &f.control).unwrap().unwrap();
        assert_eq!(retained.phase, Some(value));
        assert!(!retained.committed);
        assert!(retained.dispatched);
        response.send(Ok(None)).unwrap();
    };
    let (result, ()) = tokio::join!(owner.acknowledged(TURN, &f.control, &events), observed);
    result.unwrap();
    let message = rate_terminal();
    let observed = async {
        let (value, response) = event(&mut receiver).await;
        let retained = read(&f.state, &f.control).unwrap().unwrap();
        assert!(
            matches!(&value, Outer::Terminal { index: 1, thread_id, turn_id, terminal }
            if thread_id == THREAD && turn_id == TURN && crate::supplied_outer::rate_limited(terminal))
        );
        assert_eq!(retained.phase, Some(value));
        assert!(!retained.committed);
        assert!(retained.last_terminal.is_some());
        response.send(Ok(None)).unwrap();
    };
    let (result, ()) = tokio::join!(owner.terminal(&message, &f.control, &events, &[]), observed);
    result.unwrap();
    assert!(owner.record.committed && owner.record.terminal);
    assert_eq!(
        read(&f.state, &f.control).unwrap().as_ref(),
        Some(&owner.record)
    );
    owner
}
async fn admitted_owner(f: &Fixture) -> Owner {
    let mut owner = terminal_owner(f).await;
    let (events, mut receiver) = mpsc::channel(2);
    let observed = async {
        let (value, response) = event(&mut receiver).await;
        assert_eq!(value, wait_observation());
        response.send(Ok(Some(f.wait()))).unwrap();
        let (value, response) = event(&mut receiver).await;
        assert_eq!(value, dispatch_observation());
        let retained = read(&f.state, &f.control).unwrap().unwrap();
        assert_eq!(retained.phase, Some(value));
        assert!(
            !retained.committed,
            "intent must persist before its acknowledgment"
        );
        response.send(Ok(None)).unwrap();
    };
    let cause = owner.rate_cause(&f.control).unwrap();
    let (result, ()) = tokio::join!(owner.reserve(cause, &f.control, &events), observed);
    assert_eq!(
        result.unwrap(),
        1,
        "one additional visible outer reservation"
    );
    assert_eq!(owner.index(), 2);
    owner
}

#[tokio::test]
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//# An exact retry of a committed mutating request MUST return its recorded outcome.
async fn waiting_report_response_loss_retains_exact_original_before_any_next_send() {
    let f = Fixture::new();
    bounded(&f, async {
        let mut owner = terminal_owner(&f).await;
        let (events, mut receiver) = mpsc::channel(2);
        let observed = async {
            let (value, response) = event(&mut receiver).await;
            assert_eq!(value, wait_observation());
            let saved = read(&f.state, &f.control).unwrap().unwrap();
            assert_eq!(saved.phase, Some(value));
            assert!(!saved.committed);
            assert_eq!(saved.index, 1);
            assert_eq!(saved.turn_id.as_deref(), Some(TURN));
            // Drop this exact response. No claim is made about a remote commit.
            drop(response);
        };
        let (result, ()) = tokio::join!(owner.reserve(Cause::RateLimit { retry_index: 1 }, &f.control, &events), observed);
        assert!(result.unwrap_err().contains("original receipt unavailable"));
        let original = f.bytes();
        drop(owner);
        let mut restored = f.reopen();
        assert_eq!(restored.record.phase, Some(wait_observation()));
        assert!(!restored.record.committed);
        assert!(restored.before_send(&f.control).is_err());
        assert!(restored.resume_predecessor(&f.control).is_err());
        assert!(Owner::new(&f.state, &f.cwd, THREAD, &f.policy, f.binding.clone(), &f.control).is_err());
        assert_eq!(f.bytes(), original, "refusal cannot replace original intent");

        // Exercise the actual report method with the retained exact phase.
        // Only a controlled successful original response resolves this local cut.
        let exact = restored.record.phase.clone().unwrap();
        let canonical_wait = f.wait();
        let observed = async {
            let (value, response) = event(&mut receiver).await;
            assert_eq!(value, exact);
            response.send(Ok(Some(canonical_wait.clone()))).unwrap();
        };
        let (result, ()) = tokio::join!(restored.report(wait_observation(), &f.control, &events), observed);
        result.unwrap();
        let saved = f.reopen();
        assert!(saved.record.committed);
        assert_eq!(saved.record.wait.as_ref(), Some(&canonical_wait));
        assert_eq!(saved.record.index, 1);
        assert!(saved.resume_predecessor(&f.control).is_err(), "Waiting is not Dispatch");
        assert!(f.cleanable(), "settled old terminal has no new supplied send; outer cleanup admission remains required");
        f.sentinel();
    }).await;
}

#[tokio::test]
//= spec/impl/v2/implementation.md#automatic-cleanup-preserves-durable-work
//# Automatic cleanup MUST use per-kind retention settings from worker metadata.
async fn committed_dispatch_before_send_reopens_as_known_unsent_cleanup_candidate() {
    let f = Fixture::new();
    bounded(&f, async {
        let owner = admitted_owner(&f).await;
        assert!(owner.record.committed);
        assert!(!owner.record.dispatched);
        assert!(!owner.record.terminal);
        assert!(owner.record.turn_id.is_none());
        assert!(owner.record.last_terminal.is_some());
        let original = f.bytes();
        drop(owner);
        let restored = f.reopen();
        assert_eq!(restored.record.phase, Some(dispatch_observation()));
        assert_eq!(
            restored.resume_predecessor(&f.control).unwrap(),
            (THREAD, TURN)
        );
        let history =
            json!({"id":THREAD,"cwd":f.cwd,"turns":[rate_terminal()["params"]["turn"].clone()]});
        restored
            .verify_history(&history, &f.cwd, &f.control)
            .unwrap();
        let mut wrong = history.clone();
        wrong["turns"][0]["error"]["codexErrorInfo"] = json!("usageLimitExceeded");
        assert!(restored.verify_history(&wrong, &f.cwd, &f.control).is_err());
        assert!(
            f.cleanable(),
            "committed Dispatch with no send retains exact terminal; it is not unknown execution"
        );
        assert!(
            Owner::new(
                &f.state,
                &f.cwd,
                THREAD,
                &f.policy,
                f.binding.clone(),
                &f.control
            )
            .is_err()
        );
        assert_eq!(
            f.bytes(),
            original,
            "readback and cleanup eligibility never dispatch or rewrite"
        );
        f.sentinel();
    })
    .await;

    // Cut directly after report's acknowledgment write. Calling reserve here
    // would hide a stale-index intermediate journal behind its later updates.
    let f = Fixture::new();
    bounded(&f, async {
        let mut owner = terminal_owner(&f).await;
        let (events, mut receiver) = mpsc::channel(2);
        let canonical_wait = f.wait();
        let observed = async {
            let (value, response) = event(&mut receiver).await;
            assert_eq!(value, wait_observation());
            response.send(Ok(Some(canonical_wait.clone()))).unwrap();
        };
        let (result, ()) = tokio::join!(
            owner.report(wait_observation(), &f.control, &events),
            observed
        );
        result.unwrap();
        assert_eq!(owner.record.wait.as_ref(), Some(&canonical_wait));
        assert_eq!(owner.index(), 1);

        let observed = async {
            let (value, response) = event(&mut receiver).await;
            assert_eq!(value, dispatch_observation());
            let intent = read(&f.state, &f.control).unwrap().unwrap();
            assert_eq!(intent.phase, Some(value));
            assert_eq!(intent.index, 1);
            assert!(!intent.committed);
            response.send(Ok(None)).unwrap();
        };
        let (result, ()) = tokio::join!(
            owner.report(dispatch_observation(), &f.control, &events),
            observed
        );
        result.unwrap();
        drop(owner);

        let original = f.bytes();
        let restored = f.reopen();
        assert_eq!(restored.index(), 2);
        assert_eq!(restored.record.phase, Some(dispatch_observation()));
        assert!(restored.record.committed);
        assert!(!restored.record.dispatched);
        assert!(!restored.record.terminal);
        assert!(restored.record.turn_id.is_none());
        assert!(restored.record.last_terminal.is_some());
        assert_eq!(
            restored.resume_predecessor(&f.control).unwrap(),
            (THREAD, TURN)
        );
        assert!(f.cleanable());
        assert_eq!(f.bytes(), original);
        f.sentinel();
    })
    .await;
    // Lose the Dispatch response before the atomic successor write. The
    // predecessor remains terminal, so this is a cleanup lookup candidate,
    // never proof that the Dispatch committed or permission to send again.
    let f = Fixture::new();
    bounded(&f, async {
        let mut owner = terminal_owner(&f).await;
        let (events, mut receiver) = mpsc::channel(2);
        let observed = async {
            let (value, response) = event(&mut receiver).await;
            assert_eq!(value, wait_observation());
            response.send(Ok(Some(f.wait()))).unwrap();
        };
        let (result, ()) = tokio::join!(
            owner.report(wait_observation(), &f.control, &events),
            observed
        );
        result.unwrap();
        let observed = async {
            let (value, response) = event(&mut receiver).await;
            assert_eq!(value, dispatch_observation());
            drop(response);
        };
        let (result, ()) = tokio::join!(
            owner.report(dispatch_observation(), &f.control, &events),
            observed
        );
        assert!(result.unwrap_err().contains("original receipt unavailable"));
        drop(owner);

        let original = f.bytes();
        let mut restored = f.reopen();
        assert_eq!(restored.index(), 1);
        assert_eq!(restored.record.phase, Some(dispatch_observation()));
        assert!(!restored.record.committed);
        assert!(restored.record.terminal);
        assert!(restored.record.dispatched, "only the predecessor was sent");
        assert_eq!(restored.record.turn_id.as_deref(), Some(TURN));
        assert!(restored.record.last_terminal.is_some());
        assert!(
            f.cleanable(),
            "canonical Dispatch and predecessor Terminal lookup still required"
        );
        assert!(
            restored
                .before_send(&f.control)
                .unwrap_err()
                .contains("no resend")
        );
        assert!(restored.resume_predecessor(&f.control).is_err());
        assert_eq!(
            read(&f.state, &f.control).unwrap().as_ref(),
            Some(&restored.record)
        );
        assert_eq!(f.bytes(), original);
        f.sentinel();
    })
    .await;
}

#[tokio::test]
//= spec/impl/v2/implementation.md#the-supplied-executor-is-qualified-at-its-actual-boundary
//# A lost start response MUST be reconciled before another executor turn is started for that command.
async fn before_send_without_supplier_ack_remains_unknown_and_cannot_be_replayed_or_cleaned() {
    let f = Fixture::new();
    bounded(&f, async {
        let mut owner = admitted_owner(&f).await;
        assert!(f.cleanable());
        owner.before_send(&f.control).unwrap();
        let saved = read(&f.state, &f.control).unwrap().unwrap();
        assert_eq!(saved.index, 2);
        assert!(saved.dispatched);
        assert!(saved.turn_id.is_none());
        assert!(!saved.terminal);
        assert!(
            saved.last_terminal.is_some(),
            "old terminal cannot close the new send"
        );
        let unknown = f.bytes();
        drop(owner);
        let mut restored = f.reopen();
        assert!(
            restored
                .before_send(&f.control)
                .unwrap_err()
                .contains("no resend")
        );
        assert!(restored.resume_predecessor(&f.control).is_err());
        assert!(
            !f.cleanable(),
            "a prior terminal cannot authorize deleting an unknown subsequent start"
        );
        assert!(
            Owner::new(
                &f.state,
                &f.cwd,
                THREAD,
                &f.policy,
                f.binding.clone(),
                &f.control
            )
            .is_err()
        );
        assert_eq!(f.bytes(), unknown);
        f.sentinel();
    })
    .await;
}
