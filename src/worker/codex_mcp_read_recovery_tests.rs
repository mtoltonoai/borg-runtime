//! Actual Bridge::invoke with controlled transport and its real private journal.
//! No Codex/model process is launched or manufactured by recovery.
use super::*;

#[derive(Clone)]
struct ReadPeer {
    original: Fake,
    seen: Arc<Mutex<ReadSeen>>,
}
#[derive(Default)]
struct ReadSeen {
    calls: Vec<Invocation>,
    events: Vec<&'static str>,
    failures: usize,
    lost_committed_reply: bool,
    unavailable_lookup: bool,
    receipt_after_absence: bool,
    cancel_on_absence: Option<Arc<AtomicBool>>,
}
impl ReadPeer {
    async fn call_typed(&self, call: &Invocation) -> Result<CallResult, mcp::CallFailure> {
        let fail = {
            let mut seen = self.seen.lock().unwrap();
            seen.calls.push(call.clone());
            seen.events.push("call");
            seen.calls.len() <= seen.failures
        };
        let previous = self.original.state.lock().unwrap().mode;
        self.original.state.lock().unwrap().mode = if fail { Mode::LossBefore }
            else if self.seen.lock().unwrap().lost_committed_reply { Mode::LossAfter }
            else { previous };
        // Fake asserts exact intent already exists on disk, before every send.
        let result = self.original.call_observed(call).await;
        self.original.state.lock().unwrap().mode = previous;
        result
    }
    async fn lookup_typed(&self, call: &Invocation) -> Result<Option<CallResult>, mcp::CallFailure> {
        {
            let mut seen = self.seen.lock().unwrap();
            seen.events.push("lookup");
            if seen.unavailable_lookup { return Err(mcp::CallFailure::Transport("injected lookup unavailable".into())); }
        }
        let result = self.original.lookup_observed(call).await?;
        if result.is_none() {
            let late_receipt = {
                let mut seen = self.seen.lock().unwrap();
                let late = seen.receipt_after_absence;
                seen.receipt_after_absence = false;
                late
            };
            if late_receipt {
                // The read observed absence; a concurrent original commit is
                // visible only to the next lookup, after the required wait.
                self.original.state.lock().unwrap().receipts.insert(
                    call.operation_id.clone(), super::result(call, Mode::Good));
            }
            if let Some(cancel) = &self.seen.lock().unwrap().cancel_on_absence {
                cancel.store(true, Ordering::SeqCst);
            }
        }
        Ok(result)
    }
}
impl Transport for ReadPeer {
    async fn call(&self, call: &Invocation) -> Result<CallResult, String> {
        self.call_typed(call).await.map_err(mcp::CallFailure::into_message)
    }
    async fn lookup(&self, call: &Invocation) -> Result<Option<CallResult>, String> {
        self.lookup_typed(call).await.map_err(mcp::CallFailure::into_message)
    }
    async fn call_observed(&self, call: &Invocation) -> Result<CallResult, mcp::CallFailure> {
        self.call_typed(call).await
    }
    async fn lookup_observed(&self, call: &Invocation) -> Result<Option<CallResult>, mcp::CallFailure> {
        self.lookup_typed(call).await
    }
}

fn fixture(mode: Mode) -> (Area, Config, ReadPeer, Arc<AtomicBool>) {
    let (area, mut cfg, original, cancel) = setup(mode);
    cfg.limits.lookup_backoff_ms = 2;
    cfg.limits.liveness_interval_ms = 1;
    cfg.runtime_limits.execution.collaboration = cfg.limits.clone();
    (area, cfg, ReadPeer { original, seen: Arc::new(Mutex::new(ReadSeen::default())) }, cancel)
}
fn read_args() -> Value { json!({"task_id":"fixture-target"}) }

#[tokio::test]
async fn fourth_read_dispatch_reuses_one_occurrence_and_looks_up_after_each_backoff() {
    let (area, cfg, peer, cancel) = fixture(Mode::Good);
    peer.seen.lock().unwrap().failures = 3;
    let association = std::fs::read(area.0.join("borg-thread.json")).unwrap();
    let mut bridge = Bridge::open(cfg, peer.clone(), vec![], cancel).unwrap();
    let (wire, index) = bridge.invoke("collab_task_get", read_args(), &meta("same-read"))
        .await.unwrap();
    assert_eq!(wire["isError"], false);
    let seen = peer.seen.lock().unwrap();
    assert_eq!(seen.calls.len(), 4, "no hidden three-attempt cap");
    assert!(seen.calls.iter().all(|call| call == &seen.calls[0]));
    for position in seen.events.iter().enumerate().filter_map(|(i,e)| (*e == "call").then_some(i)).skip(1) {
        assert_eq!(&seen.events[position-2..position], &["lookup", "lookup"],
            "failed send is reconciled, then a fresh lookup follows backoff");
    }
    assert_eq!(bridge.journal.entries.len(), 1);
    assert_eq!(bridge.journal.entries[index].invocation, seen.calls[0]);
    assert!(bridge.journal.entries[index].outcome.is_some());
    assert!(!bridge.journal.entries[index].delivered);
    assert_eq!(std::fs::read(area.0.join("borg-thread.json")).unwrap(), association);
    drop(seen);
    bridge.delivered(index).unwrap();
    let replay = bridge.invoke("collab_task_get", read_args(), &meta("same-read")).await.unwrap().0;
    assert_eq!(replay, wire);
    assert_eq!(peer.seen.lock().unwrap().calls.len(), 4);
}

#[tokio::test]
async fn read_lost_commit_reply_and_committed_refusal_retain_original_without_redispatch() {
    for mode in [Mode::Good, Mode::Refused] {
        let (_area, cfg, peer, cancel) = fixture(mode);
        if matches!(mode, Mode::Good) { peer.seen.lock().unwrap().lost_committed_reply = true; }
        let mut bridge = Bridge::open(cfg, peer.clone(), vec![], cancel).unwrap();
        let (wire, index) = bridge.invoke("collab_task_get", read_args(), &meta("original-read"))
            .await.unwrap();
        assert_eq!(wire["isError"], matches!(mode, Mode::Refused));
        assert_eq!(peer.seen.lock().unwrap().calls.len(), 1);
        let call = &bridge.journal.entries[index].invocation;
        assert_eq!(bridge.journal.entries[index].outcome,
            peer.original.state.lock().unwrap().receipts.get(&call.operation_id).cloned());
    }
}

#[tokio::test]
async fn original_commit_after_absent_snapshot_wins_before_read_redispatch() {
    let (_area, mut cfg, peer, cancel) = fixture(Mode::Good);
    cfg.limits.lookup_backoff_ms = 20;
    cfg.runtime_limits.execution.collaboration = cfg.limits.clone();
    {
        let mut seen = peer.seen.lock().unwrap();
        seen.failures = 1;
        seen.receipt_after_absence = true;
    }
    let mut bridge = Bridge::open(cfg, peer.clone(), vec![], cancel).unwrap();
    let (_, index) = bridge.invoke("collab_task_get", read_args(), &meta("racing-original"))
        .await.unwrap();
    assert_eq!(peer.seen.lock().unwrap().calls.len(), 1,
        "the first absent snapshot must not be reused as redispatch authority");
    assert_eq!(peer.original.state.lock().unwrap().lookups, 2);
    let call = &bridge.journal.entries[index].invocation;
    assert_eq!(bridge.journal.entries[index].outcome,
        peer.original.state.lock().unwrap().receipts.get(&call.operation_id).cloned());
}

#[tokio::test]
async fn unavailable_lookup_or_invalid_read_receipt_never_authorizes_a_second_send() {
    for mode in [Mode::LossBefore, Mode::Uncommitted] {
        let (_area, mut cfg, peer, cancel) = fixture(mode);
        cfg.deadline_unix_ms = process::now_ms() + 500;
        if matches!(mode, Mode::LossBefore) { peer.seen.lock().unwrap().unavailable_lookup = true; }
        let mut bridge = Bridge::open(cfg, peer.clone(), vec![], cancel).unwrap();
        assert!(bridge.invoke("collab_task_get", read_args(), &meta("pending-read")).await.is_err());
        assert_eq!(peer.seen.lock().unwrap().calls.len(), 1);
        assert!(bridge.journal.entries[0].outcome.is_none());
        assert!(!bridge.journal.entries[0].delivered);
        assert_eq!(outcome_disposition(&bridge.config).unwrap(), OutcomeDisposition::Unresolved);
    }
}

#[tokio::test]
async fn read_cancel_during_recovery_and_mutation_deadline_preserve_pending_intent() {
    for mutation in [false, true] {
        let (_area, mut cfg, peer, cancel) = fixture(Mode::Good);
        cfg.deadline_unix_ms = process::now_ms() + 500;
        peer.seen.lock().unwrap().failures = 1;
        if !mutation { peer.seen.lock().unwrap().cancel_on_absence = Some(cancel.clone()); }
        let mut bridge = Bridge::open(cfg, peer.clone(), vec![], cancel).unwrap();
        let (name, input) = if mutation { ("collab_memory_set", args()) }
            else { ("collab_task_get", read_args()) };
        assert!(bridge.invoke(name, input, &meta("pending-original")).await.is_err());
        assert_eq!(peer.seen.lock().unwrap().calls.len(), 1,
            "a second dispatch would succeed, but cancellation/mutation blocks it");
        assert!(bridge.journal.entries[0].outcome.is_none());
        assert!(!bridge.journal.entries[0].delivered);
        let retained: Journal = decode(&std::fs::read(&bridge.path).unwrap(),
            bridge.config.limits.journal_bytes).unwrap();
        assert_eq!(retained.entries[0].invocation, peer.seen.lock().unwrap().calls[0]);
        assert!(retained.entries[0].outcome.is_none());
    }
}
