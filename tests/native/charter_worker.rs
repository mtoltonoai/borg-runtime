//! Actual controller + worker + local Responses + MCP paths. The fixture
//! publishes retained source definitions through ordinary task admission, then
//! cancels those source-only fixture tasks before starting any worker.
use super::*;
use borg_runtime::charter::*;

fn limits() -> borg_runtime::limits::CharterLimits {
    borg_runtime::limits::CharterLimits {
        record_bytes: 262_144, source_bytes: 65_536, files: 8, clauses: 32,
        metadata_bytes: 16_384, references: 32, reply_items: 32, tool_calls: 8,
        reference_depth: 8, records: 1024, page_items: 32, page_bytes: 262_144,
    }
}
fn source(revision: &str) -> DefinitionCharter {
    let sentence = format!("The agent MUST retain the {revision} task purpose.");
    DefinitionCharter::Inline { source: CharterSource {
        files: vec![CharterFile { file: "charter.md".into(), text: format!("# Purpose\n\n{sentence}\n") }],
        clauses: vec![CharterClauseRef { file: "charter.md".into(), section: "Purpose".into(), sentence }],
    } }
}
fn reference(revision: &str) -> CharterSourceRef {
    CharterSourceRef { definition_id: "loop-definition".into(), definition_revision: revision.into() }
}
fn metadata(revision: &str) -> Value {
    json!({"revision":reference(revision),"clauses":[{"file":"charter.md","section":"Purpose",
        "sentence":format!("The agent MUST retain the {revision} task purpose.")}]})
}
fn final_reply(revision: &str, result: Value) -> Value {
    envelope(json!([{"id":format!("message-{revision}"),"type":"message","role":"assistant","status":"completed",
        "content":[{"type":"output_text","text":json!({"charter_references":metadata(revision),"result":result}).to_string()}]}]))
}
fn tool_reply(index: usize, revision: &str) -> Value {
    let mut reply = function(index, "collab_memory_get", json!({"key":"note"}));
    reply["output"].as_array_mut().unwrap().insert(0,json!({"id":format!("commentary-{index}"),
        "type":"message","role":"assistant","status":"completed","phase":"commentary",
        "content":[{"type":"output_text","text":json!({"charter_references":metadata(revision)}).to_string()}]}));
    reply
}
async fn binding(api: &ApiClient, session: &str) -> EffectiveCharter {
    match api.call(Operation::QueryCharter { query: CharterQuery::Session { session_id:session.into() } }).await.unwrap().outcome {
        Outcome::Ok { data: Data::CharterQuery(CharterQueryResult::Session(Some(value))) } => value,
        other => panic!("binding {other:?}"),
    }
}
async fn pending(api: &ApiClient, session: &str, revision: &str) -> Reply {
    let before = binding(api,session).await;
    api.call(Operation::MutateCharter { operation_id:format!("change-{revision}"),
        mutation:CharterMutation::SetPending { session_id:session.into(),expected_revision:before.session.revision,target:reference(revision) } }).await.unwrap()
}

//= spec/impl/v2/implementation.md#native-charter-changes-preserve-prompt-continuity
//# A native charter change MUST schedule a low-priority compaction that merges the change into the system prompt.
//= spec/impl/v2/implementation.md#native-charter-changes-preserve-prompt-continuity
//# Native charter changes MUST be debounced, so that a burst of them costs one compaction.
//= spec/impl/v2/implementation.md#native-charter-changes-preserve-prompt-continuity
//# A native charter change arriving while a compaction is already due MUST be included in that compaction.
#[tokio::test(flavor="multi_thread", worker_threads=4)]
async fn actual_turn_cas_tool_wait_burst_compaction_and_passive_audit_survive_restart() {
    worker_scenario(false).await;
}
// The original summary keeps its source revision when a newer change arrives;
// it remains obsolete and the newer authoritative source stays selected.
//= spec/impl/v2/implementation.md#a-revision-identifies-authoritative-source
//= type=test
//# A rendered or compacted representation of a charter MUST NOT replace that charter's authoritative source.
#[tokio::test(flavor="multi_thread", worker_threads=4)]
async fn change_during_actual_summary_never_applies_the_obsolete_summary() {
    worker_scenario(true).await;
}
async fn worker_scenario(change_during_compaction:bool) {
    let mut lab=Lab::new(if change_during_compaction {"charter-native-obsolete"} else {"charter-native-join"});
    let mut policy=(*lab.limits).clone(); policy.charter=Some(limits());
    policy.execution.native.context_bytes=262_144;
    policy.charter.as_ref().unwrap().validate(&policy).unwrap();
    lab.limits=Arc::new(policy);
    std::fs::write(lab.root.join("limits.json"),serde_json::to_vec_pretty(&*lab.limits).unwrap()).unwrap();
    let mut controller=lab.controller("charter-controller").await;
    let api=ApiClient::new(&lab.url,lab.limits.clone()).unwrap();
    for revision in ["r2","r3","r4","r5"] {
        let mut input=lab.input(1); input.definition.revision=revision.into();input.definition.charter=Some(source(revision));
        let task=submit_identified(&api,input,&format!("retain-source-{revision}")).await;
        assert!(matches!(api.call(Operation::CancelTask {operation_id:format!("cancel-source-{revision}"),task_id:task.id,
            expected_revision:task.revision}).await.unwrap().outcome,Outcome::Ok{..}));
    }
    let second_entered=Arc::new(tokio::sync::Notify::new());let second_release=Arc::new(tokio::sync::Notify::new());
    let entered=second_entered.clone();let release=second_release.clone();
    let summary_entered=Arc::new(tokio::sync::Notify::new());let summary_release=Arc::new(tokio::sync::Notify::new());
    let summary_seen=summary_entered.clone();let summary_continue=summary_release.clone();
    let peer=Peer::start_async(move |index,request| {
        let entered=entered.clone();let release=release.clone();
        let summary_seen=summary_seen.clone();let summary_continue=summary_continue.clone();async move {
            match index {
                0=>{assert!(request["input"].to_string().contains("r1"));Some(tool_reply(index,"r1"))},
                1=>{assert!(request["input"].to_string().contains("Charter change"));
                    assert!(request["input"].to_string().contains("r3"));
                    entered.notify_one();release.notified().await;
                    // Intentionally stale metadata. Only the audit differs;
                    // the valid tool call and normal continuation still occur.
                    Some(tool_reply(index,"r1"))},
                2=>{assert!(request.get("tools").is_none());assert!(request["input"].to_string().contains("r4"));
                    if change_during_compaction {summary_seen.notify_one();summary_continue.notified().await;}
                    Some(final_reply("r4",json!({"summary":"Two real scoped reads completed; return the task result."})))},
                3=>{assert!(request.get("tools").is_some());
                    let context=request["input"].to_string();
                    if change_during_compaction {
                        assert!(!context.contains("Derived conversation summary"));
                        assert!(context.contains("r5"));assert!(context.contains("function_call_output"));
                        assert!(context.contains("native-collab-"));
                    } else {assert!(context.contains("Derived conversation summary"));}
                    Some(final_reply(if change_during_compaction {"r5"} else {"r4"},json!(7)))},
                _=>panic!("unexpected model request or duplicate compaction"),
            }
        }
    }).await;
    let proxy=Proxy::start(lab.url.clone(),None,false,lab.limits.clone()).await;
    let held=Arc::new(HeldToolReply::new("collab_memory_get"));
    *proxy.held_reply.lock().unwrap()=Some(held.clone());
    let mut input=lab.input(4);input.definition.revision="r1".into();input.definition.charter=Some(source("r1"));
    let original=input.clone();let task=submit(&api,input).await;
    let mut worker=lab.worker(&proxy.url,&peer.endpoint,"charter-worker-one");
    tokio::time::timeout(Duration::from_secs(20),held.entered.notified()).await.unwrap();
    assert!(matches!(pending(&api,&task.session_id,"r2").await.outcome,Outcome::Ok{..}));
    assert!(matches!(pending(&api,&task.session_id,"r3").await.outcome,Outcome::Ok{..}));
    let during=binding(&api,&task.session_id).await;
    assert_eq!(during.session.effective,reference("r1"));assert_eq!(during.session.pending,Some(reference("r3")));
    assert_eq!(peer.count(),1);
    *proxy.held_reply.lock().unwrap()=None;
    held.released.store(true,Ordering::SeqCst);held.release.notify_one();
    tokio::time::timeout(Duration::from_secs(20),second_entered.notified()).await.unwrap();
    let due=binding(&api,&task.session_id).await;
    assert_eq!(due.session.effective,reference("r3"));assert_eq!(due.session.compaction_due,Some(reference("r3")));
    let update=pending(&api,&task.session_id,"r4").await;assert!(matches!(update.outcome,Outcome::Ok{..}));
    second_release.notify_one();
    if change_during_compaction {
        tokio::time::timeout(Duration::from_secs(20),summary_entered.notified()).await.unwrap();
        let inflight=binding(&api,&task.session_id).await;
        assert!(inflight.session.compaction_in_flight.is_some());assert_eq!(inflight.session.effective,reference("r4"));
        assert!(matches!(pending(&api,&task.session_id,"r5").await.outcome,Outcome::Ok{..}));
        summary_release.notify_one();
    }
    let accepted=until(&api,&task.id,|t|t.status==TaskStatus::Succeeded).await;
    assert_eq!(accepted.result.as_ref().unwrap().output,Some(json!(7)));assert_eq!(peer.count(),4);
    assert_eq!(accepted.input.definition,original.definition);
    let head=binding(&api,&task.session_id).await;
    assert_eq!(head.session.effective,reference(if change_during_compaction {"r5"} else {"r4"}));
    assert_eq!(head.session.compaction_due,if change_during_compaction {Some(reference("r5"))} else {None});
    assert!(head.session.compaction_in_flight.is_none());
    let run_id=format!("{}-charter-compaction-3",accepted.attempt.as_ref().unwrap().id);
    let run=match api.call(Operation::QueryCharter {query:CharterQuery::Compaction {run_id}}).await.unwrap().outcome {
        Outcome::Ok{data:Data::CharterQuery(CharterQueryResult::Compaction(value))}=>value,
        other=>panic!("compaction {other:?}"),
    };
    assert_eq!(run.effective.reference,reference("r4"));assert!(run.reply.is_some());assert!(run.summary.is_some());
    assert_eq!(run.disposition,Some(if change_during_compaction {CompactionDisposition::ObsoleteSourceSummary} else {CompactionDisposition::CurrentSourceSummary}));
    let turns=match api.call(Operation::QueryCharter {query:CharterQuery::Turns {session_id:task.session_id.clone(),after:None,limit:10}}).await.unwrap().outcome {
        Outcome::Ok{data:Data::CharterQuery(CharterQueryResult::Turns(page))}=>page.items,
        other=>panic!("turns {other:?}"),
    };
    assert_eq!(turns.len(),3);assert!(turns.iter().all(|t|t.reply.is_some()&&t.completed_by.is_some()));
    assert!(turns.iter().all(|t|t.reply.as_ref().unwrap().observation.output.as_ref().is_some_and(Value::is_array)));
    assert!(turns.iter().any(|t|t.reply.as_ref().unwrap().observation.output.as_ref().unwrap().to_string().contains("commentary-1")));
    assert!(turns.iter().any(|t|t.reply.as_ref().unwrap().audit.findings.contains(&CharterAuditFinding::RevisionMismatch)));
    assert_eq!(live_usage(&api,accepted.attempt.as_ref().unwrap()).await,(4,Some(20),Some(8)));
    worker.stop().await;controller.stop().await;
    let mut controller=lab.controller("charter-controller-restart").await;
    assert_eq!(binding(&api,&task.session_id).await,head);
    assert_eq!(peer.count(),4);assert_eq!(get(&api,&task.id).await,accepted);
    controller.stop().await;
}

#[path = "charter_retry.rs"]
mod retry;

#[path = "charter_callers.rs"]
mod callers;

#[path="charter_output_retention.rs"]
mod output_retention;
