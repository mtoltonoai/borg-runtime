//! Production Codex consumer with a controlled canonical batch and stdio peer.
//! This checks retention ordering for an admitted wait resolution beyond the
//! ordinary scanned prefix. It does not claim Controller admission authority.
use super::*;
use crate::collaboration::workflow::{ActivityDetail, QuestionChange, ResolvedWait,
    ResolverOutcome, ResolverRef, WaitRef, WorkflowContinuation};

#[tokio::test]
async fn codex_full_consumer_keeps_exact_continuation_beyond_prefix_before_one_turn() {
    let f = Fixture::new(true, true, "success").await;
    let original = batch();
    let ActivityDetail::Question(QuestionChange::Closed { resolution }) =
        original.page.items[0].detail.clone() else { panic!("question fixture") };
    let continuation = WorkflowContinuation { task_id: caller().task_id,
        session_id: SESSION.into(), resolved_waits: vec![ResolvedWait {
            wait: WaitRef { task_id: caller().task_id, wait_id: "joined-required-wait".into() },
            resolver: ResolverRef::Question { question_id: "question-join".into() },
            event_id: event(42), outcome: ResolverOutcome::Question(resolution),
        }] };
    {
        let mut peer = f.peer.lock().unwrap();
        let page = &mut peer.observation.pending.as_mut().unwrap().page;
        page.items.clear(); page.through = event(42); page.has_more = true;
        page.continuation = Some(continuation.clone());
        assert!(page.to_cursor.position.as_ref().unwrap() < &continuation.resolved_waits[0].event_id);
    }
    let (_cancel, control) = f.control();
    let (events, _received) = mpsc::channel(16);
    let collaboration = Collaboration { instruction_source: None, controller_url: f.url.clone(), caller: caller(),
        bridge_binary: std::env::current_exe().unwrap(),
        memory: Some(MemoryScope { agent_id: AGENT.into(), session_id: SESSION.into() }),
        workflow_enabled: true,
    };
    let state = f.state(); let registry = f.root.join("processes");
    let mut execution = Box::pin(execute_with_credentials_and_collaboration(
        &f.binary, &f.root, &state, &registry,
        f.input.definition.inference.as_ref().unwrap(), f.input.execution_limits().unwrap(),
        &f.input.instruction, &control, events, &[], None, Some(&collaboration), None,
    ));
    tokio::select! {
        result = &mut execution => panic!("required input completed before append reply: {result:?}"),
        _ = wait_for_inject(&f.root) => {},
    }
    assert_eq!(f.begin_count(), 1); assert_eq!(f.ack_count(), 0);
    let requests = f.executor_requests();
    assert!(requests.iter().all(|value| value["method"] != "turn/start"));
    let inject = requests.iter().find(|value| value["method"] == "thread/inject_items").unwrap();
    let text = inject["params"]["items"][0]["content"][0]["text"].as_str().unwrap();
    let rendered: Value = serde_json::from_str(text).unwrap();
    assert_eq!(rendered["events"], json!([]));
    assert_eq!(rendered["continuation"], serde_json::to_value(&continuation).unwrap());
    assert_eq!(inject["params"]["items"].as_array().unwrap().len(), 1);
    let before: Value = serde_json::from_slice(&f.mirror().1).unwrap();
    assert_eq!(before["pending"]["delivery"]["appended"], false);
    assert_eq!(before["pending"]["batch"]["page"]["continuation"], serde_json::to_value(&continuation).unwrap());
    std::fs::write(f.root.join("release-response"), b"actual controlled append response").unwrap();
    assert!(execution.await.unwrap().success);
    f.assert_closed();
    assert_eq!(f.inject_count(), 1); assert_eq!(f.begin_count(), 1); assert_eq!(f.ack_count(), 1);
    let requests = f.executor_requests();
    let turns: Vec<_> = requests.iter().enumerate().filter(|(_, value)| value["method"] == "turn/start").collect();
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].1["params"]["input"], json!([{"type":"text","text":f.input.instruction}]));
    assert!(requests.iter().position(|value| value["method"] == "thread/inject_items").unwrap() < turns[0].0);
    assert_eq!(f.peer.lock().unwrap().observation.acknowledged_cursor, cursor(11));
    assert!(f.peer.lock().unwrap().observation.pending.is_none());
    let after: Value = serde_json::from_slice(&f.mirror().1).unwrap();
    assert!(after["pending"].is_null());
}
