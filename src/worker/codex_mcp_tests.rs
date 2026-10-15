use super::*;
use std::{
    collections::BTreeMap,
    sync::{Mutex, atomic::AtomicU64},
};
static NEXT: AtomicU64 = AtomicU64::new(1);
struct Area(PathBuf);
impl Area {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "borg-codex-mcp-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&p).unwrap();
        process::private_directory(&p).unwrap();
        Self(p)
    }
}
impl Drop for Area {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[derive(Clone, Copy)]
enum Mode {
    Good,
    Refused,
    LossAfter,
    LossBefore,
    Secret,
}
struct State {
    calls: usize,
    lookups: usize,
    mode: Mode,
    receipts: BTreeMap<String, CallResult>,
}
#[derive(Clone)]
struct Fake {
    state: Arc<Mutex<State>>,
    journal: PathBuf,
    journal_bytes: usize,
}
fn result(call: &Invocation, mode: Mode) -> CallResult {
    let outcome = match mode {
        Mode::Refused => Outcome::Refused {
            code: "capacity".into(),
            message: "known committed refusal".into(),
        },
        Mode::Secret => Outcome::Ok {
            data: Data::Memory(Some(MemoryValue {
                agent_id: "agent-1".into(),
                key: "fixture".into(),
                revision: 1,
                value: json!("synthetic-private-key"),
            })),
        },
        _ => Outcome::Ok {
            data: Data::Acknowledged,
        },
    };
    CallResult {
        reply: Reply {
            request_id: "fixture".into(),
            outcome,
            commit: Some(MutationCommit {
                operation_id: call.operation_id.clone(),
                controller_revision: 1,
            }),
            observed_controller_revision: Some(1),
        },
        requires_quiescence: false,
    }
}
impl Transport for Fake {
    async fn call(&self, call: &Invocation) -> Result<CallResult, String> {
        let bytes = read_private(
            &self.journal,
            self.journal_bytes,
            "execution.collaboration.journal_bytes",
        )?
        .ok_or("missing intent")?;
        let journal: Journal = decode(&bytes, self.journal_bytes)?;
        assert!(
            journal
                .entries
                .iter()
                .any(|e| e.invocation == *call && e.outcome.is_none() && !e.delivered),
            "exact durable intent must precede dispatch"
        );
        let mut state = self.state.lock().unwrap();
        state.calls += 1;
        if matches!(state.mode, Mode::LossBefore) {
            return Err("injected loss before commit".into());
        }
        let receipt = result(call, state.mode);
        state
            .receipts
            .insert(call.operation_id.clone(), receipt.clone());
        if matches!(state.mode, Mode::LossAfter) {
            Err("injected lost receipt".into())
        } else {
            Ok(receipt)
        }
    }
    async fn lookup(&self, call: &Invocation) -> Result<Option<CallResult>, String> {
        let mut state = self.state.lock().unwrap();
        state.lookups += 1;
        Ok(state.receipts.get(&call.operation_id).cloned())
    }
}
fn setup(mode: Mode) -> (Area, Config, Fake, Arc<AtomicBool>) {
    let area = Area::new();
    let runtime_limits = crate::worker::test_limits();
    let config = Config {
        runtime_limits: (*runtime_limits).clone(),
        limits: runtime_limits.execution.collaboration.clone(),
        worker_limits: runtime_limits.worker.clone(),
        controller_url: "http://127.0.0.1:9".into(),
        caller: Caller {
            task_id: "task-1".into(),
            attempt_id: "attempt-1".into(),
            generation: 1,
            worker_id: "worker-1".into(),
            incarnation: "incarnation-1".into(),
        },
        deadline_unix_ms: process::now_ms() + 5000,
        state: area.0.clone(),
    };
    process::atomic_json(
        &area.0.join("borg-thread.json"),
        &json!({"thread_id":"thread-1","turn_id":"turn-1"}),
    )
    .unwrap();
    let fake = Fake {
        state: Arc::new(Mutex::new(State {
            calls: 0,
            lookups: 0,
            mode,
            receipts: BTreeMap::new(),
        })),
        journal: config.directory().join("calls.json"),
        journal_bytes: config.limits.journal_bytes,
    };
    (area, config, fake, Arc::new(AtomicBool::new(false)))
}
fn meta(id: &str) -> Value {
    json!({"callId":id,"threadId":"thread-1","sessionId":"session-1","progressToken":17})
}
fn args() -> Value {
    json!({"key":"fixture","expected_revision":null,"value":"same"})
}
// Traceability scope: Fake::call reads the real private journal and asserts the exact pending Invocation already exists at dispatch; lost reply recovery performs lookup without another send.
//= spec/impl/v2/implementation.md#native-tool-profiles-and-continuations
//= type=test
//# An execution attempt MUST retain a tool call's exact argument binding before sending a mutation.
#[tokio::test]
async fn exact_intent_precedes_dispatch_lost_commit_reply_recovers_without_repost() {
    let (_area, cfg, fake, cancel) = setup(Mode::LossAfter);
    let mut bridge = Bridge::open(cfg, fake.clone(), vec![], cancel).unwrap();
    let (wire, index) = bridge
        .invoke("collab_memory_set", args(), &meta("logical-1"))
        .await
        .unwrap();
    assert_eq!(wire["isError"], false);
    assert_eq!(fake.state.lock().unwrap().calls, 1);
    assert_eq!(fake.state.lock().unwrap().lookups, 1);
    let retained: Journal = decode(
        &read_private(
            &bridge.path,
            bridge.config.limits.journal_bytes,
            "execution.collaboration.journal_bytes",
        )
        .unwrap()
        .unwrap(),
        bridge.config.limits.journal_bytes,
    )
    .unwrap();
    assert!(retained.entries[0].outcome.is_some());
    assert!(!retained.entries[0].delivered);
    bridge.delivered(index).unwrap();
    let (replay, _) = bridge
        .invoke("collab_memory_set", args(), &meta("logical-1"))
        .await
        .unwrap();
    assert_eq!(wire, replay);
    assert_eq!(fake.state.lock().unwrap().calls, 1);
}
#[tokio::test]
async fn known_refusal_replays_but_new_intent_with_identical_body_is_distinct() {
    let (_area, cfg, fake, cancel) = setup(Mode::Refused);
    let mut bridge = Bridge::open(cfg, fake.clone(), vec![], cancel).unwrap();
    let (first, i) = bridge
        .invoke("collab_memory_set", args(), &meta("first"))
        .await
        .unwrap();
    assert_eq!(first["isError"], true);
    bridge.delivered(i).unwrap();
    let (repeat, _) = bridge
        .invoke("collab_memory_set", args(), &meta("first"))
        .await
        .unwrap();
    assert_eq!(repeat, first);
    let (_, j) = bridge
        .invoke("collab_memory_set", args(), &meta("intentional-second"))
        .await
        .unwrap();
    bridge.delivered(j).unwrap();
    assert_eq!(fake.state.lock().unwrap().calls, 2);
    assert_ne!(
        bridge.journal.entries[0].invocation.operation_id,
        bridge.journal.entries[1].invocation.operation_id
    );
}
// Traceability scope: Changing arguments or thread for a retained occurrence refuses without another transport call or changes to journal bytes.
//= spec/impl/v2/implementation.md#native-tool-profiles-and-continuations
//= type=test
//# An execution attempt MUST retain a tool call's exact argument binding before sending a mutation.
#[tokio::test]
async fn changed_body_or_thread_cannot_retarget_an_existing_occurrence() {
    let (_area, cfg, fake, cancel) = setup(Mode::Good);
    let mut bridge = Bridge::open(cfg, fake.clone(), vec![], cancel).unwrap();
    let (_, i) = bridge
        .invoke("collab_memory_set", args(), &meta("same-id"))
        .await
        .unwrap();
    bridge.delivered(i).unwrap();
    let bytes = std::fs::read(&bridge.path).unwrap();
    assert!(
        bridge
            .invoke(
                "collab_memory_set",
                json!({"key":"different"}),
                &meta("same-id")
            )
            .await
            .is_err()
    );
    let mut bad = meta("same-id");
    bad["threadId"] = json!("unbound");
    assert!(
        bridge
            .invoke("collab_memory_set", args(), &bad)
            .await
            .is_err()
    );
    assert_eq!(fake.state.lock().unwrap().calls, 1);
    assert_eq!(std::fs::read(&bridge.path).unwrap(), bytes);
}
#[tokio::test]
async fn reply_loss_restart_retains_result_and_blocks_fresh_call_until_exact_delivery() {
    let (_area, cfg, fake, cancel) = setup(Mode::Good);
    let mut bridge = Bridge::open(cfg.clone(), fake.clone(), vec![], cancel.clone()).unwrap();
    let (wire, _) = bridge
        .invoke("collab_memory_set", args(), &meta("pending-delivery"))
        .await
        .unwrap();
    drop(bridge);
    let mut restarted = Bridge::open(cfg, fake.clone(), vec![], cancel).unwrap();
    assert!(
        restarted
            .invoke("collab_memory_set", args(), &meta("fresh"))
            .await
            .is_err()
    );
    let (recovered, i) = restarted
        .invoke("collab_memory_set", args(), &meta("pending-delivery"))
        .await
        .unwrap();
    assert_eq!(wire, recovered);
    restarted.delivered(i).unwrap();
    assert_eq!(fake.state.lock().unwrap().calls, 1);
}
#[tokio::test]
async fn uncertain_precommit_loss_never_reposts_and_stops_at_deadline() {
    let (_area, mut cfg, fake, cancel) = setup(Mode::LossBefore);
    cfg.deadline_unix_ms = process::now_ms() + 1000;
    let mut bridge = Bridge::open(cfg, fake.clone(), vec![], cancel).unwrap();
    assert!(
        bridge
            .invoke("collab_memory_set", args(), &meta("unknown"))
            .await
            .is_err()
    );
    assert_eq!(fake.state.lock().unwrap().calls, 1);
    assert!(bridge.journal.entries[0].outcome.is_none());
    assert!(!bridge.journal.entries[0].delivered);
    bridge.stop("test_unknown").unwrap();
    assert!(check_stop(&bridge.config).is_err());
}
#[tokio::test]
async fn recovered_unresolved_intent_is_lookup_only_on_restart() {
    let (_area, cfg, fake, cancel) = setup(Mode::Good);
    let mut first = Bridge::open(cfg.clone(), fake.clone(), vec![], cancel.clone()).unwrap();
    let call = Invocation {
        operation_id: "durable-before-send".into(),
        logical_call_id: "old".into(),
        caller: cfg.caller.clone(),
        tool: "collab_memory_set".into(),
        arguments: args(),
    };
    first.journal.entries.push(Entry {
        thread_id: "thread-1".into(),
        session_id: "session-1".into(),
        invocation: call.clone(),
        outcome: None,
        delivered: false,
    });
    first.save().unwrap();
    drop(first);
    fake.state
        .lock()
        .unwrap()
        .receipts
        .insert(call.operation_id.clone(), result(&call, Mode::Good));
    let mut next = Bridge::open(cfg, fake.clone(), vec![], cancel).unwrap();
    next.invoke("collab_memory_set", args(), &meta("old"))
        .await
        .unwrap();
    assert_eq!(fake.state.lock().unwrap().calls, 0);
    assert_eq!(fake.state.lock().unwrap().lookups, 1);
}
#[tokio::test]
async fn cancellation_and_single_owner_prevent_fresh_dispatch() {
    let (_area, cfg, fake, cancel) = setup(Mode::Good);
    let mut bridge = Bridge::open(cfg.clone(), fake.clone(), vec![], cancel.clone()).unwrap();
    assert!(Bridge::open(cfg, fake.clone(), vec![], cancel.clone()).is_err());
    cancel.store(true, Ordering::SeqCst);
    assert!(
        bridge
            .invoke("collab_memory_set", args(), &meta("cancelled"))
            .await
            .is_err()
    );
    assert_eq!(fake.state.lock().unwrap().calls, 0);
}
#[tokio::test]
async fn configured_secret_is_not_journaled_or_rewritten_into_an_ordinary_result() {
    let (_area, cfg, fake, cancel) = setup(Mode::Secret);
    let mut bridge = Bridge::open(
        cfg,
        fake.clone(),
        vec!["synthetic-private-key".into()],
        cancel,
    )
    .unwrap();
    assert!(
        bridge
            .invoke(
                "collab_memory_set",
                json!({"value":"synthetic-private-key"}),
                &meta("bad-input")
            )
            .await
            .is_err()
    );
    assert_eq!(fake.state.lock().unwrap().calls, 0);
    assert!(
        bridge
            .invoke("collab_memory_set", args(), &meta("bad-result"))
            .await
            .is_err()
    );
    assert_eq!(fake.state.lock().unwrap().calls, 1);
    assert!(bridge.journal.entries[0].outcome.is_none());
    assert!(
        !String::from_utf8(std::fs::read(&bridge.path).unwrap())
            .unwrap()
            .contains("synthetic-private-key")
    );
}
#[tokio::test]
async fn escaped_aggregate_and_worst_result_are_reserved_before_dispatch() {
    let (_area, cfg, fake, cancel) = setup(Mode::Good);
    let mut bridge = Bridge::open(cfg, fake.clone(), vec![], cancel).unwrap();
    let big = json!({"key":"large","expected_revision":null,"value":"\"".repeat(90_000)});
    let mut accepted = 0;
    for n in 0..10 {
        match bridge
            .invoke(
                "collab_memory_set",
                big.clone(),
                &meta(&format!("call-{n}")),
            )
            .await
        {
            Ok((_, i)) => {
                bridge.delivered(i).unwrap();
                accepted += 1
            }
            Err(e) => {
                assert_eq!(
                    e,
                    "collaboration journal admission bound (execution.collaboration.journal_bytes; reserves execution.collaboration.complete_result_bytes)"
                );
                break;
            }
        }
    }
    assert!(accepted > 0 && accepted < 10);
    assert_eq!(fake.state.lock().unwrap().calls, accepted);
    assert!(
        std::fs::metadata(&bridge.path).unwrap().len() <= bridge.config.limits.journal_bytes as u64
    );
}
#[tokio::test]
async fn corrupt_or_changed_binding_is_not_repaired_or_treated_as_empty() {
    let (_area, cfg, fake, cancel) = setup(Mode::Good);
    let mut bridge = Bridge::open(cfg.clone(), fake.clone(), vec![], cancel.clone()).unwrap();
    let (_, i) = bridge
        .invoke("collab_memory_set", args(), &meta("first"))
        .await
        .unwrap();
    bridge.delivered(i).unwrap();
    drop(bridge);
    let mut changed = cfg.clone();
    changed.deadline_unix_ms += 1;
    assert!(Bridge::open(changed, fake.clone(), vec![], cancel.clone()).is_err());
    std::fs::write(&fake.journal, b"{\"revision\":1,\"revision\":1}").unwrap();
    let bytes = std::fs::read(&fake.journal).unwrap();
    assert!(Bridge::open(cfg, fake.clone(), vec![], cancel).is_err());
    assert_eq!(std::fs::read(&fake.journal).unwrap(), bytes);
}

#[tokio::test]
async fn cancellation_during_unknown_outcome_retains_intent_and_never_reports_delivery() {
    let (_area, cfg, fake, cancel) = setup(Mode::LossBefore);
    let state = fake.state.clone();
    let stop = cancel.clone();
    let canceller = tokio::spawn(async move {
        loop {
            if state.lock().unwrap().lookups > 0 {
                stop.store(true, Ordering::SeqCst);
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    let mut bridge = Bridge::open(cfg, fake.clone(), vec![], cancel).unwrap();
    assert!(
        bridge
            .invoke("collab_memory_set", args(), &meta("cancel-after-send"))
            .await
            .is_err()
    );
    canceller.await.unwrap();
    assert_eq!(fake.state.lock().unwrap().calls, 1);
    assert!(bridge.journal.entries[0].outcome.is_none());
    assert!(!bridge.journal.entries[0].delivered);
    assert!(
        bridge
            .invoke("collab_memory_set", args(), &meta("later"))
            .await
            .is_err()
    );
    assert_eq!(fake.state.lock().unwrap().calls, 1);
}
#[test]
fn private_reader_refuses_symlink_and_nonregular_without_blocking() {
    use std::os::unix::fs::symlink;
    let area = Area::new();
    let path = area.0.join("alias");
    symlink("/dev/null", &path).unwrap();
    assert!(read_private(&path, 1024, "execution.collaboration.journal_bytes").is_err());
    assert!(
        read_private(
            Path::new("/dev/null"),
            1024,
            "execution.collaboration.journal_bytes"
        )
        .is_err()
    );
}

#[test]
fn exact_task_config_does_not_approve_other_servers_tools_or_change_annotations() {
    let runtime_limits = crate::worker::test_limits();
    let config = server_config(
        Path::new("/host/borg"),
        Path::new("/private/config"),
        &runtime_limits,
        &runtime_limits.execution.collaboration,
        60,
    );
    let writes = [
        "collab_memory_set",
        "collab_task_progress",
        "collab_task_assign",
        "collab_task_cancel",
        "collab_task_retry",
    ];
    let tools = config["tools"].as_object().unwrap();
    assert_eq!(tools.len(), writes.len());
    for name in writes {
        assert_eq!(tools[name], json!({"approval_mode":"approve"}));
    }
    assert!(config.get("default_tools_approval_mode").is_none());
    assert!(config.get("approval_policy").is_none());
    assert!(config.get("sandbox").is_none());
    assert!(!tools.contains_key("unrelated_write"));
    assert!(!tools.contains_key("collab_task_delegate"));
    assert!(!tools.contains_key("collab_call_get"));
    let enabled = config["enabled_tools"].as_array().unwrap();
    let expected = [
        "collab_tasks_list",
        "collab_task_get",
        "collab_task_history_list",
        "collab_agents_list",
        "collab_agent_get",
        "collab_definition_get",
        "collab_memory_list",
        "collab_memory_get",
        "collab_memory_set",
        "collab_task_progress",
        "collab_task_assign",
        "collab_task_cancel",
        "collab_task_retry",
        "collab_workers_list",
        "collab_worker_get",
        "collab_task_attempts_list",
        "collab_task_checkpoints_list",
        "collab_task_releases_list",
        "collab_release_get",
        "collab_release_checks_list",
        "collab_call_get",
        "collab_repository_tree",
        "collab_repository_read",
        "collab_repository_search",
        "collab_repository_diff",
        "collab_repository_history",
        "collab_session_get",
        "collab_sessions_list",
        "collab_limits_get",
        "collab_repositories_list",
        "collab_repository_get",
    ]
    .into_iter()
    .collect::<std::collections::BTreeSet<_>>();
    let actual = enabled
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(actual, expected, "exact permitted server tool set changed");
    assert!(!enabled.contains(&json!("collab_task_delegate")));
    for name in [
        "collab_session_get",
        "collab_sessions_list",
        "collab_limits_get",
    ] {
        assert!(enabled.contains(&json!(name)));
        assert!(!tools.contains_key(name));
    }
    assert_eq!(
        enabled
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        enabled.len(),
    );
    for name in [
        "collab_repository_tree",
        "collab_repository_read",
        "collab_repository_search",
        "collab_repository_diff",
        "collab_repository_history",
        "collab_repositories_list",
        "collab_repository_get",
    ] {
        assert!(
            enabled.contains(&json!(name)),
            "repository read absent: {name}"
        );
        assert!(
            !tools.contains_key(name),
            "read gained a write approval: {name}"
        );
        let tool = mcp::catalogue(&runtime_limits)
            .into_iter()
            .find(|t| t.name == name)
            .unwrap();
        assert_eq!(tool.annotations["readOnlyHint"], true);
        assert_eq!(tool.annotations["destructiveHint"], false);
        assert_eq!(tool.annotations["openWorldHint"], false);
    }
    for tool in mcp::catalogue(&runtime_limits) {
        if writes.contains(&tool.name.as_str()) {
            assert_eq!(tool.annotations["readOnlyHint"], false);
        }
    }
}
#[tokio::test]
async fn post_stop_disposition_keeps_unknown_corrupt_and_known_refusal_distinct() {
    let (_area, cfg, fake, cancel) = setup(Mode::Refused);
    assert_eq!(
        outcome_disposition(&cfg).unwrap(),
        OutcomeDisposition::NoCalls
    );
    let mut bridge = Bridge::open(cfg.clone(), fake.clone(), vec![], cancel).unwrap();
    bridge
        .invoke("collab_memory_set", args(), &meta("known-refusal"))
        .await
        .unwrap();
    drop(bridge);
    assert_eq!(
        outcome_disposition(&cfg).unwrap(),
        OutcomeDisposition::Committed
    );
    let mut journal: Journal = decode(
        &std::fs::read(&fake.journal).unwrap(),
        cfg.limits.journal_bytes,
    )
    .unwrap();
    journal.entries[0].outcome = None;
    bounded_save(
        &fake.journal,
        &journal,
        cfg.limits.journal_bytes,
        "execution.collaboration.journal_bytes",
    )
    .unwrap();
    assert_eq!(
        outcome_disposition(&cfg).unwrap(),
        OutcomeDisposition::Unresolved
    );
    std::fs::write(&fake.journal, b"invalid").unwrap();
    assert!(outcome_disposition(&cfg).is_err());
}

#[tokio::test]
async fn repository_reads_preserve_exact_host_caller_arguments_and_refusal() {
    let (_area, cfg, fake, cancel) = setup(Mode::Refused);
    let caller = cfg.caller.clone();
    let mut bridge = Bridge::open(cfg, fake.clone(), vec![], cancel).unwrap();
    let cases = [
        (
            "collab_repository_tree",
            json!({"source":"start","path":"","after":null,"limit":10}),
        ),
        (
            "collab_repository_read",
            json!({"source":"start","path":"README.md","start_line":1,"max_lines":10}),
        ),
        (
            "collab_repository_search",
            json!({"source":"checkpoint","path":"src","needle":"fn main"}),
        ),
        (
            "collab_repository_diff",
            json!({"source":"checkpoint","base_source":"start","path":"src"}),
        ),
        (
            "collab_repository_history",
            json!({"source":"start","limit":10}),
        ),
    ];
    for (n, (name, arguments)) in cases.into_iter().enumerate() {
        let logical = format!("repository-read-{n}");
        let (wire, index) = bridge
            .invoke(name, arguments.clone(), &meta(&logical))
            .await
            .unwrap();
        assert_eq!(
            wire["isError"], true,
            "committed refusal must not become success"
        );
        let entry = &bridge.journal.entries[index];
        assert_eq!(entry.invocation.caller, caller);
        assert_eq!(entry.invocation.tool, name);
        assert_eq!(entry.invocation.arguments, arguments);
        assert_eq!(entry.invocation.logical_call_id, logical);
        assert!(matches!(
            entry.outcome.as_ref().unwrap().reply.outcome,
            Outcome::Refused { .. }
        ));
        bridge.delivered(index).unwrap();
        let (replay, _) = bridge
            .invoke(name, arguments, &meta(&logical))
            .await
            .unwrap();
        assert_eq!(replay, wire);
        assert_eq!(
            fake.state.lock().unwrap().calls,
            n + 1,
            "same occurrence must not dispatch again"
        );
    }
    assert_eq!(fake.state.lock().unwrap().calls, 5);
    assert_eq!(fake.state.lock().unwrap().lookups, 0);
}

#[tokio::test]
async fn repository_write_and_coding_delegation_are_rejected_before_dispatch() {
    let (_area, cfg, fake, cancel) = setup(Mode::Good);
    let mut bridge = Bridge::open(cfg, fake.clone(), vec![], cancel).unwrap();
    for (n, name) in [
        "collab_repository_write",
        "collab_repository_delete",
        "collab_task_delegate",
        "unknown_tool",
    ]
    .into_iter()
    .enumerate()
    {
        let error = bridge
            .invoke(name, json!({}), &meta(&format!("refused-{n}")))
            .await
            .unwrap_err();
        assert_eq!(error, "collaboration tool not applicable to coding task");
    }
    assert_eq!(fake.state.lock().unwrap().calls, 0);
    assert_eq!(fake.state.lock().unwrap().lookups, 0);
    assert!(bridge.journal.entries.is_empty());
}

#[tokio::test]
async fn memory_boundary_holds_exact_retained_tool_result_until_matching_release() {
    let (_area, cfg, fake, cancel) = setup(Mode::Good);
    let mut bridge = Bridge::open(cfg.clone(), fake.clone(), vec![], cancel).unwrap();
    enable_memory_gate(&cfg).unwrap();
    let (wire, index) = bridge
        .invoke("collab_memory_set", args(), &meta("notice-gate"))
        .await
        .unwrap();
    let retained = std::fs::read(&bridge.path).unwrap();
    let mut waiting = Box::pin(bridge.hold_memory_boundary(index));
    let gate = tokio::select! {
        result=&mut waiting=>panic!("tool response released before gate: {result:?}"),
        gate=wait_memory_boundary(&cfg)=>gate.unwrap(),
    };
    let mut wrong = gate.clone();
    wrong.logical_call_id = "other".into();
    assert!(release_memory_boundary(&cfg, &wrong).is_err());
    assert!(!boundary_released(&cfg, &gate).unwrap());
    assert_eq!(std::fs::read(&bridge.path).unwrap(), retained);
    release_memory_boundary(&cfg, &gate).unwrap();
    tokio::time::timeout(Duration::from_secs(1), &mut waiting)
        .await
        .unwrap()
        .unwrap();
    drop(waiting);
    assert_eq!(std::fs::read(&bridge.path).unwrap(), retained);
    bridge.delivered(index).unwrap();
    let (again, _) = bridge
        .invoke("collab_memory_set", args(), &meta("notice-gate"))
        .await
        .unwrap();
    assert_eq!(again, wire);
    assert_eq!(fake.state.lock().unwrap().calls, 1);
}

#[tokio::test]
async fn missing_memory_release_retains_unconsumed_tool_result_at_cancel() {
    let (_area, cfg, fake, cancel) = setup(Mode::Good);
    let mut bridge = Bridge::open(cfg.clone(), fake.clone(), vec![], cancel.clone()).unwrap();
    enable_memory_gate(&cfg).unwrap();
    let (_, index) = bridge
        .invoke("collab_memory_set", args(), &meta("pending-notice"))
        .await
        .unwrap();
    let mut waiting = Box::pin(bridge.hold_memory_boundary(index));
    tokio::select! {
        result=&mut waiting=>panic!("unexpected gate closure {result:?}"),
        gate=wait_memory_boundary(&cfg)=>{gate.unwrap();},
    }
    cancel.store(true, Ordering::SeqCst);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), &mut waiting)
            .await
            .unwrap()
            .is_err()
    );
    drop(waiting);
    assert!(bridge.journal.entries[index].outcome.is_some());
    assert!(!bridge.journal.entries[index].delivered);
    assert_eq!(fake.state.lock().unwrap().calls, 1);
}

#[tokio::test]
async fn metadata_journal_reserve_refuses_before_dispatch_and_larger_policy_admits() {
    let (_small_area, mut cfg, mut fake, cancel) = setup(Mode::Good);
    // A complete result is reserved before the first POST. This explicit policy
    // leaves less room for escaped arguments than the fresh larger-policy case.
    cfg.limits.journal_bytes = cfg.limits.complete_result_bytes + 32 * 1024;
    fake.journal_bytes = cfg.limits.journal_bytes;
    let mut bridge = Bridge::open(cfg, fake.clone(), vec![], cancel).unwrap();
    bridge.save().unwrap();
    let before = std::fs::read(&bridge.path).unwrap();
    let arguments =
        json!({"key":"fixture","expected_revision":null,"value":"\"".repeat(24 * 1024)});
    let intent_room =
        bridge.config.limits.journal_bytes - bridge.config.limits.complete_result_bytes;
    assert!(before.len() < intent_room, "empty retained journal fits");
    assert!(serde_json::to_vec(&arguments).unwrap().len() > intent_room);
    let error = bridge
        .invoke(
            "collab_memory_set",
            arguments.clone(),
            &meta("sized-occurrence"),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error,
        "collaboration journal admission bound (execution.collaboration.journal_bytes; reserves execution.collaboration.complete_result_bytes)"
    );
    assert!(bridge.journal.entries.is_empty());
    assert_eq!(std::fs::read(&bridge.path).unwrap(), before);
    assert_eq!(fake.state.lock().unwrap().calls, 0);
    assert_eq!(fake.state.lock().unwrap().lookups, 0);

    let (_large_area, mut larger, mut larger_fake, larger_cancel) = setup(Mode::Good);
    larger.limits = bridge.config.limits.clone();
    larger.limits.journal_bytes += 128 * 1024;
    larger_fake.journal_bytes = larger.limits.journal_bytes;
    let mut admitted = Bridge::open(larger, larger_fake.clone(), vec![], larger_cancel).unwrap();
    let (wire, index) = admitted
        .invoke(
            "collab_memory_set",
            arguments.clone(),
            &meta("sized-occurrence"),
        )
        .await
        .unwrap();
    assert_eq!(wire["isError"], false);
    assert_eq!(
        admitted.journal.entries[index].invocation.arguments,
        arguments
    );
    assert!(admitted.journal.entries[index].outcome.is_some());
    assert_eq!(larger_fake.state.lock().unwrap().calls, 1);
    assert_eq!(larger_fake.state.lock().unwrap().lookups, 0);
    assert!(
        std::fs::metadata(&admitted.path).unwrap().len()
            <= admitted.config.limits.journal_bytes as u64
    );
}

#[tokio::test]
async fn retained_policy_drift_refuses_without_changing_known_refusal_or_reposting() {
    let (_area, cfg, fake, cancel) = setup(Mode::Refused);
    let mut bridge = Bridge::open(cfg.clone(), fake.clone(), vec![], cancel.clone()).unwrap();
    let (refused, index) = bridge
        .invoke("collab_memory_set", args(), &meta("policy-bound-refusal"))
        .await
        .unwrap();
    assert_eq!(refused["isError"], true);
    bridge.delivered(index).unwrap();
    let retained = std::fs::read(&bridge.path).unwrap();
    drop(bridge);

    let mut collaboration_drift = cfg.clone();
    collaboration_drift.limits.liveness_interval_ms += 1;
    let mut global_drift = cfg.clone();
    global_drift.runtime_limits.transport.body_bytes += 1024;
    let mut worker_drift = cfg.clone();
    worker_drift.worker_limits.process_poll_ms += 1;
    for changed in [collaboration_drift, global_drift, worker_drift] {
        changed.validate().unwrap();
        assert!(outcome_disposition(&changed).is_err());
        assert!(Bridge::open(changed, fake.clone(), vec![], cancel.clone()).is_err());
        assert_eq!(std::fs::read(&fake.journal).unwrap(), retained);
        assert_eq!(fake.state.lock().unwrap().calls, 1);
        assert_eq!(fake.state.lock().unwrap().lookups, 0);
    }
    let mut recovered = Bridge::open(cfg, fake.clone(), vec![], cancel).unwrap();
    let (replay, _) = recovered
        .invoke("collab_memory_set", args(), &meta("policy-bound-refusal"))
        .await
        .unwrap();
    assert_eq!(replay, refused);
    assert_eq!(std::fs::read(&fake.journal).unwrap(), retained);
    assert_eq!(fake.state.lock().unwrap().calls, 1);
    assert_eq!(fake.state.lock().unwrap().lookups, 0);
}

#[tokio::test]
async fn memory_boundary_above_1024_uses_exact_retained_occurrence_without_count_quota() {
    let (_area, mut cfg, mut fake, cancel) = setup(Mode::Good);
    cfg.limits.journal_bytes *= 4;
    fake.journal_bytes = cfg.limits.journal_bytes;
    let mut bridge = Bridge::open(cfg.clone(), fake.clone(), vec![], cancel.clone()).unwrap();
    let selected = 1025;
    for index in 0..=selected {
        let invocation = Invocation {
            operation_id: format!("boundary-operation-{index}"),
            logical_call_id: format!("boundary-occurrence-{index}"),
            caller: cfg.caller.clone(),
            tool: "collab_memory_set".into(),
            arguments: args(),
        };
        let outcome = result(&invocation, Mode::Good);
        bridge.journal.entries.push(Entry {
            thread_id: "thread-1".into(),
            session_id: "session-1".into(),
            invocation,
            outcome: Some(outcome),
            delivered: index < selected,
        });
    }
    bridge.save().unwrap();
    drop(bridge);
    // Reopening validates every unique invocation and original committed receipt.
    let mut bridge = Bridge::open(cfg.clone(), fake.clone(), vec![], cancel).unwrap();
    assert_eq!(bridge.journal.entries.len(), selected + 1);
    let retained = std::fs::read(&bridge.path).unwrap();
    assert!(retained.len() < cfg.limits.journal_bytes);
    let gate = MemoryBoundary {
        revision: 1,
        caller: cfg.caller.clone(),
        deadline_unix_ms: cfg.deadline_unix_ms,
        index: selected,
        thread_id: "thread-1".into(),
        logical_call_id: format!("boundary-occurrence-{selected}"),
    };
    let boundary_path = cfg.directory().join("memory-boundary.json");
    bounded_save(
        &boundary_path,
        &gate,
        cfg.limits.boundary_bytes,
        "execution.collaboration.boundary_bytes",
    )
    .unwrap();
    assert_eq!(read_memory_boundary(&cfg).unwrap(), Some(gate.clone()));

    let mut wrong_index = gate.clone();
    wrong_index.index -= 1;
    let mut missing_index = gate.clone();
    missing_index.index = bridge.journal.entries.len();
    let mut wrong_logical = gate.clone();
    wrong_logical.logical_call_id = "unretained-occurrence".into();
    let mut wrong_thread = gate.clone();
    wrong_thread.thread_id = "other-thread".into();
    let mut wrong_caller = gate.clone();
    wrong_caller.caller.attempt_id = "other-attempt".into();
    for changed in [
        wrong_index,
        missing_index,
        wrong_logical,
        wrong_thread,
        wrong_caller,
    ] {
        bounded_save(
            &boundary_path,
            &changed,
            cfg.limits.boundary_bytes,
            "execution.collaboration.boundary_bytes",
        )
        .unwrap();
        assert!(read_memory_boundary(&cfg).is_err());
        assert_eq!(std::fs::read(&bridge.path).unwrap(), retained);
    }
    bounded_save(
        &boundary_path,
        &gate,
        cfg.limits.boundary_bytes,
        "execution.collaboration.boundary_bytes",
    )
    .unwrap();
    assert!(
        bridge
            .invoke(
                "collab_memory_set",
                json!({"key":"changed-body","expected_revision":null,"value":"same"}),
                &meta(&gate.logical_call_id),
            )
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&bridge.path).unwrap(), retained);
    let (replay, index) = bridge
        .invoke("collab_memory_set", args(), &meta(&gate.logical_call_id))
        .await
        .unwrap();
    assert_eq!(index, selected);
    assert_eq!(replay["isError"], false);
    assert_eq!(fake.state.lock().unwrap().calls, 0);
    assert_eq!(fake.state.lock().unwrap().lookups, 0);
    bridge.journal.entries[selected].outcome = None;
    bridge.save().unwrap();
    assert!(
        read_memory_boundary(&cfg).is_err(),
        "uncommitted occurrence cannot release a boundary"
    );
}

#[derive(Clone)]
struct CommittedSaveFailure {
    inner: Fake,
    point: process::storage_tests::Point,
    fault: Arc<Mutex<Option<process::storage_tests::Fault>>>,
}
impl Transport for CommittedSaveFailure {
    async fn call(&self, call: &Invocation) -> Result<CallResult, String> {
        let result = self.inner.call(call).await?;
        // Fake::call has already retained the original receipt and incremented
        // its dispatch count. Only the following bridge save can hit this cut.
        *self.fault.lock().unwrap() = Some(process::storage_tests::inject(
            &self.inner.journal,
            self.point,
            5,
            0,
        ));
        Ok(result)
    }
    async fn lookup(&self, call: &Invocation) -> Result<Option<CallResult>, String> {
        self.inner.lookup(call).await
    }
}

#[tokio::test]
async fn failed_result_save_requires_barrier_for_in_memory_and_reopened_codex_occurrence() {
    use process::storage_tests::{Point, inject};
    for point in [Point::FileSync, Point::DirectorySync] {
        for reopen in [false, true] {
            let (_area, config, fake, cancel) = setup(Mode::Good);
            let first = Arc::new(Mutex::new(None));
            let transport = CommittedSaveFailure {
                inner: fake.clone(),
                point,
                fault: first.clone(),
            };
            let mut bridge =
                Bridge::open(config.clone(), transport.clone(), vec![], cancel.clone()).unwrap();
            assert!(
                bridge
                    .invoke("collab_memory_set", args(), &meta("storage-original"))
                    .await
                    .is_err()
            );
            assert_eq!(first.lock().unwrap().as_ref().unwrap().hits(), 1);
            assert_eq!(fake.state.lock().unwrap().calls, 1);
            let original = bridge.journal.entries[0].invocation.clone();
            let expected = fake.state.lock().unwrap().receipts[&original.operation_id].clone();
            first.lock().unwrap().take();
            if reopen {
                drop(bridge);
                bridge = Bridge::open(config.clone(), transport, vec![], cancel).unwrap();
            }
            let before = std::fs::read(&bridge.path).unwrap();
            let blocked = inject(&bridge.path, Point::FileSync, 28, 0);
            assert!(
                bridge
                    .invoke("collab_memory_set", args(), &meta("storage-original"))
                    .await
                    .is_err()
            );
            assert_eq!(blocked.hits(), 1);
            assert_eq!(std::fs::read(&bridge.path).unwrap(), before);
            assert_eq!(fake.state.lock().unwrap().calls, 1);
            drop(blocked);
            let (wire, index) = bridge
                .invoke("collab_memory_set", args(), &meta("storage-original"))
                .await
                .unwrap();
            assert_eq!(wire["isError"], false);
            let result: CallResult =
                serde_json::from_str(wire["content"][0]["text"].as_str().unwrap()).unwrap();
            assert_eq!(
                serde_json::to_value(&result).unwrap(),
                serde_json::to_value(&expected).unwrap()
            );
            assert_eq!(bridge.journal.entries[index].invocation, original);
            assert!(!bridge.journal.entries[index].delivered);
            let durable: Journal = decode(
                &std::fs::read(&bridge.path).unwrap(),
                config.limits.journal_bytes,
            )
            .unwrap();
            assert_eq!(durable.entries[index].invocation, original);
            assert_eq!(
                serde_json::to_value(durable.entries[index].outcome.as_ref().unwrap()).unwrap(),
                serde_json::to_value(&expected).unwrap()
            );
            assert_eq!(
                fake.state.lock().unwrap().calls,
                1,
                "storage retries must not dispatch the tool again"
            );
        }
    }
}
