//! Child of codex_mcp_tests: exercise the real admission/stdio branches with
//! the existing durable-intent-checking Fake. No provider or host execution.
use super::*;

fn large_admission_arguments() -> Value {
    // Same escaped aggregate used by the existing journal-reservation test.
    json!({"key":"large","expected_revision":null,"value":"\"".repeat(90_000)})
}

async fn fill_to_admission_refusal(bridge: &mut Bridge<Fake>) -> (Value, usize) {
    let arguments = large_admission_arguments();
    let argument_bytes = serde_json::to_vec(&arguments).unwrap().len();
    let maximum_calls = bridge.config.limits.journal_bytes / argument_bytes + 1;
    let mut first = None;
    for n in 0..maximum_calls {
        let before = std::fs::read(&bridge.path).ok();
        let entries = bridge.journal.entries.len();
        match bridge
            .invoke("collab_memory_set", arguments.clone(), &meta(&format!("admission-fill-{n:08}")))
            .await
        {
            Ok((wire, index)) => {
                if first.is_none() { first = Some((wire, index)); }
                bridge.delivered(index).unwrap();
            }
            Err(InvokeError::JournalAdmission(refusal)) => {
                assert!(entries > 0, "fixture must retain real completed calls before saturation");
                assert_eq!(bridge.journal.entries.len(), entries);
                assert_eq!(std::fs::read(&bridge.path).ok(), before);
                assert_eq!(refusal.journal_bytes, bridge.config.limits.journal_bytes);
                assert_eq!(refusal.complete_result_bytes, bridge.config.limits.complete_result_bytes);
                return first.unwrap();
            }
            Err(other) => panic!("saturation must reach typed admission refusal: {other:?}"),
        }
    }
    panic!("escaped argument growth did not reach the configured journal bound");
}

fn assert_unsent_body(body: &Value, cfg: &Config) {
    assert_eq!(body["isError"], true);
    let content = body["content"].as_array().unwrap();
    assert_eq!(content.len(), 1);
    assert_eq!(content[0]["type"], "text");
    let detail: Value = serde_json::from_str(content[0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(detail, json!({
        "code":"collaboration_journal_admission", "dispatch":"not_sent",
        "message":JOURNAL_ADMISSION_MESSAGE, "journal_bytes":cfg.limits.journal_bytes,
        "complete_result_bytes":cfg.limits.complete_result_bytes,
    }));
    let message = detail["message"].as_str().unwrap();
    assert!(message.contains("execution.collaboration.journal_bytes"));
    assert!(message.contains("execution.collaboration.complete_result_bytes"));
    assert!(serde_json::to_vec(body).unwrap().len() <= cfg.limits.frame_bytes);
}

fn seed_non_authoritative_admission(cfg: &Config) -> Vec<u8> {
    // A valid diagnostic for a different, never-admitted occurrence. Its
    // presence must not settle, reclassify or authorize any retained call.
    let diagnostic = AdmissionRefusal {
        revision: 1, caller: cfg.caller.clone(), dispatch: AdmissionDisposition::NotSent,
        operation_id: "diagnostic-only-not-sent".into(),
        logical_call_id: "prior-admission-refusal".into(),
        thread_id: "thread-1".into(), session_id: "session-1".into(),
        journal_bytes: cfg.limits.journal_bytes,
        complete_result_bytes: cfg.limits.complete_result_bytes,
    };
    let path = cfg.directory().join("admission.json");
    bounded_save(&path, &diagnostic, cfg.limits.gate_bytes, "execution.collaboration.gate_bytes").unwrap();
    std::fs::read(path).unwrap()
}

// Traceability scope: the real stdio refusal names both the journal limit and
// its complete-result reservation, before any new intent or transport send.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# An operation refused by a configured limit MUST identify the limiting metadata field.
#[tokio::test]
async fn fresh_journal_refusal_is_unsent_bounded_and_diagnostic_io_is_best_effort() {
    let (_area, cfg, fake, cancel) = setup(Mode::Good);
    let cfg = workflow_config(cfg);
    let mut bridge = Bridge::open(cfg.clone(), fake.clone(), vec![], cancel).unwrap();
    enable_memory_gate(&cfg).unwrap();
    fill_to_admission_refusal(&mut bridge).await;
    let original = std::fs::read(&bridge.path).unwrap();
    let original_entries = serde_json::to_value(&bridge.journal).unwrap();
    let calls = fake.state.lock().unwrap().calls;
    let gate = std::fs::read(cfg.directory().join("memory-gate.json")).unwrap();
    let (body, index) = bridge.invoke_for_stdio(
        "collab_memory_set", large_admission_arguments(), &meta("refused-fresh-after-saturation"),
    ).await.unwrap();
    assert_unsent_body(&body, &cfg);
    assert_eq!(index, None, "no journal occurrence exists to acknowledge");
    let path = cfg.directory().join("admission.json");
    let bytes = std::fs::read(&path).unwrap();
    assert!(bytes.len() <= cfg.limits.gate_bytes);
    let diagnostic: AdmissionRefusal = decode(&bytes, cfg.limits.gate_bytes).unwrap();
    assert_eq!(diagnostic.revision, 1);
    assert_eq!(serde_json::to_value(&diagnostic.dispatch).unwrap(), json!("not_sent"));
    assert_eq!(diagnostic.caller, cfg.caller);
    assert_eq!(diagnostic.logical_call_id, "refused-fresh-after-saturation");
    assert_eq!(diagnostic.thread_id, "thread-1");
    assert_eq!(diagnostic.session_id, "session-1");
    assert_eq!(diagnostic.journal_bytes, cfg.limits.journal_bytes);
    assert_eq!(diagnostic.complete_result_bytes, cfg.limits.complete_result_bytes);
    assert!(!bridge.journal.entries.iter().any(|e| e.invocation.operation_id == diagnostic.operation_id));
    assert!(diagnostic_reason(&cfg).unwrap().is_none());
    // Deterministic failure of diagnostic replacement, without changing any
    // policy or relying on the test process's permission privileges.
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    let (again, index) = bridge.invoke_for_stdio(
        "collab_memory_set", large_admission_arguments(), &meta("refused-fresh-diagnostic-unwritable"),
    ).await.unwrap();
    assert_unsent_body(&again, &cfg);
    assert_eq!(index, None);
    assert!(path.is_dir());
    assert_eq!(fake.state.lock().unwrap().calls, calls);
    assert_eq!(fake.state.lock().unwrap().lookups, 0);
    assert_eq!(std::fs::read(&bridge.path).unwrap(), original);
    assert_eq!(serde_json::to_value(&bridge.journal).unwrap(), original_entries);
    assert_eq!(std::fs::read(cfg.directory().join("memory-gate.json")).unwrap(), gate);
    assert!(!cfg.directory().join("memory-boundary.json").exists());
    assert!(!cfg.directory().join("memory-release.json").exists());
    assert!(!cfg.stop_path().exists());
    assert_eq!(outcome_disposition(&cfg).unwrap(), OutcomeDisposition::Committed);
}

#[tokio::test]
async fn saturated_journal_replays_through_workflow_boundary_and_reopens_exactly() {
    let (_area, cfg, fake, cancel) = setup(Mode::Good);
    let cfg = workflow_config(cfg);
    let mut bridge = Bridge::open(cfg.clone(), fake.clone(), vec![], cancel.clone()).unwrap();
    let (original_wire, original_index) = fill_to_admission_refusal(&mut bridge).await;
    let original = std::fs::read(&bridge.path).unwrap();
    let calls = fake.state.lock().unwrap().calls;
    enable_memory_gate(&cfg).unwrap();
    let replay_meta = meta("admission-fill-00000000");
    let mut waiting = Box::pin(bridge.invoke_for_stdio("collab_memory_set", large_admission_arguments(), &replay_meta));
    let boundary = tokio::select! {
        result = &mut waiting => panic!("retained replay skipped its workflow boundary: {result:?}"),
        boundary = wait_memory_boundary(&cfg) => boundary.unwrap(),
    };
    assert_eq!(boundary.index, original_index);
    assert_eq!(boundary.logical_call_id, "admission-fill-00000000");
    assert_eq!(std::fs::read(&fake.journal).unwrap(), original);
    release_memory_boundary(&cfg, &boundary).unwrap();
    let (wire, index) = waiting.await.unwrap();
    assert_eq!(wire, original_wire);
    assert_eq!(index, Some(original_index));
    assert_eq!(std::fs::read(&bridge.path).unwrap(), original);
    drop(bridge);
    let mut restarted = Bridge::open(cfg.clone(), fake.clone(), vec![], cancel).unwrap();
    assert_eq!(std::fs::read(&restarted.path).unwrap(), original);
    let (wire, index) = restarted.invoke_for_stdio("collab_memory_set", large_admission_arguments(), &replay_meta).await.unwrap();
    assert_eq!(wire, original_wire);
    assert_eq!(index, Some(original_index));
    let (refused, index) = restarted.invoke_for_stdio(
        "collab_memory_set", large_admission_arguments(), &meta("refused-fresh-after-exact-restart"),
    ).await.unwrap();
    assert_unsent_body(&refused, &cfg);
    assert_eq!(index, None);
    assert_eq!(std::fs::read(&restarted.path).unwrap(), original);
    assert_eq!(fake.state.lock().unwrap().calls, calls);
    assert_eq!(fake.state.lock().unwrap().lookups, 0);
    assert!(!cfg.stop_path().exists());
}

#[tokio::test]
async fn undelivered_result_is_fatal_for_new_call_despite_admission_diagnostic() {
    let (_area, cfg, fake, cancel) = setup(Mode::Good);
    let mut bridge = Bridge::open(cfg.clone(), fake.clone(), vec![], cancel.clone()).unwrap();
    let diagnostic = seed_non_authoritative_admission(&cfg);
    let (wire, index) = bridge.invoke_for_stdio("collab_memory_set", args(), &meta("reply-not-acknowledged")).await.unwrap();
    assert_eq!(index, Some(0));
    let original = std::fs::read(&bridge.path).unwrap();
    assert!(!bridge.journal.entries[0].delivered);
    drop(bridge);
    let mut restarted = Bridge::open(cfg.clone(), fake.clone(), vec![], cancel).unwrap();
    let error = restarted.invoke_for_stdio("collab_memory_set", args(), &meta("different-new-call")).await.unwrap_err();
    assert_eq!(error, "prior collaboration delivery is unresolved");
    assert_eq!(std::fs::read(&restarted.path).unwrap(), original);
    assert_eq!(std::fs::read(cfg.directory().join("admission.json")).unwrap(), diagnostic);
    assert_eq!(fake.state.lock().unwrap().calls, 1);
    assert_eq!(fake.state.lock().unwrap().lookups, 0);
    let (replayed, index) = restarted.invoke_for_stdio("collab_memory_set", args(), &meta("reply-not-acknowledged")).await.unwrap();
    assert_eq!(replayed, wire);
    assert_eq!(index, Some(0));
    restarted.delivered(index.unwrap()).unwrap();
    assert!(restarted.journal.entries[0].delivered);
    assert_eq!(fake.state.lock().unwrap().calls, 1);
}

#[tokio::test]
async fn lost_transport_ack_keeps_original_lookup_semantics_despite_admission_diagnostic() {
    for mode in [Mode::LossBefore, Mode::LossAfter] {
        let (_area, cfg, fake, cancel) = setup(mode);
        let mut bridge = Bridge::open(cfg.clone(), fake.clone(), vec![], cancel.clone()).unwrap();
        let diagnostic = seed_non_authoritative_admission(&cfg);
        if matches!(mode, Mode::LossBefore) {
            let logical = meta("unknown-original");
            let mut waiting = Box::pin(bridge.invoke_for_stdio("collab_memory_set", args(), &logical));
            let remaining = Duration::from_millis(cfg.deadline_unix_ms.saturating_sub(process::now_ms()));
            tokio::select! {
                result = &mut waiting => panic!("unknown call returned before original lookup observation: {result:?}"),
                observed = tokio::time::timeout(remaining, async {
                    loop {
                        if fake.state.lock().unwrap().lookups > 0 { break; }
                        tokio::task::yield_now().await;
                    }
                }) => observed.unwrap(),
            }
            cancel.store(true, Ordering::SeqCst);
            assert_eq!(waiting.await.unwrap_err(), "collaboration bridge cancelled");
            let original = std::fs::read(&bridge.path).unwrap();
            assert!(bridge.journal.entries[0].outcome.is_none());
            assert!(!bridge.journal.entries[0].delivered);
            assert_eq!(outcome_disposition(&cfg).unwrap(), OutcomeDisposition::Unresolved);
            assert_eq!(diagnostic_reason(&cfg).unwrap(), Some("mcp_transport"));
            let invocation = bridge.journal.entries[0].invocation.clone();
            drop(bridge);
            let mut restarted = Bridge::open(cfg.clone(), fake.clone(), vec![], Arc::new(AtomicBool::new(false))).unwrap();
            assert_eq!(std::fs::read(&restarted.path).unwrap(), original);
            assert_eq!(restarted.invoke_for_stdio("collab_memory_set", args(), &meta("fresh-cannot-replace-unknown")).await.unwrap_err(), "prior collaboration delivery is unresolved");
            assert_eq!(std::fs::read(&restarted.path).unwrap(), original);
            // Only a subsequently available real occurrence receipt resolves it.
            fake.state.lock().unwrap().receipts.insert(invocation.operation_id.clone(), result(&invocation, Mode::Good));
            let (_, index) = restarted.invoke_for_stdio("collab_memory_set", args(), &logical).await.unwrap();
            assert_eq!(index, Some(0));
            assert_eq!(restarted.journal.entries[0].invocation, invocation);
            assert!(!restarted.journal.entries[0].delivered);
        } else {
            // A lost transport ACK with an available canonical receipt remains
            // recoverable by exact lookup; it is not a known-unsent refusal.
            let (wire, index) = bridge.invoke_for_stdio("collab_memory_set", args(), &meta("lost-ack-original")).await.unwrap();
            assert_eq!(wire["isError"], false);
            assert_eq!(index, Some(0));
            assert_eq!(fake.state.lock().unwrap().lookups, 1);
            assert!(bridge.journal.entries[0].outcome.is_some());
            assert!(!bridge.journal.entries[0].delivered);
        }
        assert_eq!(fake.state.lock().unwrap().calls, 1, "unknown mutations must never redispatch");
        assert_eq!(std::fs::read(cfg.directory().join("admission.json")).unwrap(), diagnostic);
    }
}

struct AdmissionTextContract(Fake);
impl Transport for AdmissionTextContract {
    async fn call(&self, call: &Invocation) -> Result<CallResult, String> {
        self.call_observed(call).await.map_err(mcp::CallFailure::into_message)
    }
    async fn call_observed(&self, call: &Invocation) -> Result<CallResult, mcp::CallFailure> {
        // Preserve the existing Fake's exact durable-intent-before-send oracle.
        assert!(self.0.call_typed(call).await.is_err());
        Err(mcp::CallFailure::Contract(JOURNAL_ADMISSION_MESSAGE.into()))
    }
    async fn lookup(&self, call: &Invocation) -> Result<Option<CallResult>, String> {
        self.0.lookup(call).await
    }
}

#[tokio::test]
async fn admission_looking_quiescence_text_remains_fatal_through_real_stdio_match() {
    for stdio in [false, true] {
        let (_area, cfg, fake, cancel) = setup(Mode::LossBefore);
        let mut bridge = Bridge::open(cfg.clone(), AdmissionTextContract(fake.clone()), vec![], cancel).unwrap();
        if stdio {
            assert_eq!(bridge.invoke_for_stdio("collab_memory_set", args(), &meta("lookalike-contract")).await.unwrap_err(), JOURNAL_ADMISSION_MESSAGE);
        } else {
            match bridge.invoke("collab_memory_set", args(), &meta("lookalike-contract")).await.unwrap_err() {
                InvokeError::Quiesce(reason) => assert_eq!(reason, JOURNAL_ADMISSION_MESSAGE),
                other => panic!("text similarity reclassified a dispatched call: {other:?}"),
            }
        }
        assert_eq!(fake.state.lock().unwrap().calls, 1);
        assert_eq!(fake.state.lock().unwrap().lookups, 0);
        assert_eq!(bridge.journal.entries.len(), 1);
        assert!(bridge.journal.entries[0].outcome.is_none());
        assert!(!bridge.journal.entries[0].delivered);
        assert_eq!(outcome_disposition(&cfg).unwrap(), OutcomeDisposition::Unresolved);
        assert!(!cfg.directory().join("admission.json").exists());
    }
}
