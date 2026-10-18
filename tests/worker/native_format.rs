//! Actual helper/worker/controller/HTTP path. Invalid completed text is new
//! input with exact feedback; dropped or malformed wire is never a retry grant.
use super::*;
use borg_runtime::native::{FormatIssue, SchemaRule};

fn binding(bytes: &[u8]) -> ProviderRequestBinding {
    ProviderRequestBinding {bytes: bytes.len() as u64,
        sha256:aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256,bytes).as_ref().iter().map(|b|format!("{b:02x}")).collect()}
}

#[tokio::test(flavor="multi_thread",worker_threads=4)]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A complete model reply that violates its declared output format MUST be retried with the specific validation error within the remaining request allowance and deadline.
async fn completed_format_repairs_keep_originals_feedback_charges_and_restart_receipts() {
    let lab=Lab::new("format-repair-complete");let mut controller=lab.controller("controller-one").await;
    let submitted=lab.submit(3).await;
    let mut peer=Peer::start(&lab,&submitted.id,Replies::Format {invalid:2,lost_at:None}).await;
    process::private_directory(&lab.state()).unwrap();
    process::atomic_json(&lab.state().join("format.json"),&json!({"format_revision":10})).unwrap();
    let sentinel=lab.state().join("pre-upgrade-sentinel");std::fs::write(&sentinel,b"original-format10-evidence").unwrap();
    let mut worker=lab.worker(&peer.endpoint,"worker-one");
    let task=until_task(&lab.api,&submitted.id,|t|t.status==TaskStatus::Succeeded).await;
    assert_eq!(serde_json::from_slice::<Value>(&std::fs::read(lab.state().join("format.json")).unwrap()).unwrap(),json!({"format_revision":17}));
    assert_eq!(std::fs::read(sentinel).unwrap(),b"original-format10-evidence");
    assert_eq!(task.input,submitted.input);assert_eq!(task.result.as_ref().unwrap().output,Some(json!({"result":"ok"})));
    let a=task.attempt.as_ref().unwrap();assert!(a.stopped&&!a.uncertain);
    let mut originals=Vec::new();
    for index in 1..=2 {
        let (charge,charge_record)=receipt(&lab.api,&contract::reservation_id(&a.id,index)).await;
        assert!(matches!(report(&charge_record).observation,Observation::Usage {model_requests,input_tokens,output_tokens, ..}
            if model_requests==index && input_tokens==if index==1 {None} else {Some(5)} && output_tokens==if index==1 {None} else {Some(1)}));
        let original=receipt(&lab.api,&contract::format_failure_id(&a.id,index)).await;
        let Observation::NativeFormatFailure {request_index,issue,request_binding,output_binding}=report(&original.1).observation else {panic!("typed original format occurrence missing")};
        assert_eq!(request_index,index);
        if index==1 {assert!(matches!(issue,FormatIssue::InvalidJson {..}));}
        else {assert_eq!(issue,FormatIssue::Schema {rule:SchemaRule::Type});}
        let seen=peer.seen.lock().unwrap();
        assert_eq!(request_binding,binding(&seen[index as usize-1].wire_body));
        let old_text=if index==1 {"not JSON"} else {"{\"result\":7}"};
        let items=json!([{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":old_text}]}]);
        assert_eq!(output_binding,binding(&serde_json::to_vec(&items).unwrap()));
        let next=&seen[index as usize].request;
        let inputs=next["input"].as_array().unwrap();
        assert!(inputs.iter().any(|v|v==&items[0]),"invalid original output must remain actual input");
        assert!(inputs.iter().any(|v|v["role"]=="developer"&&v["content"]==issue.feedback()));
        assert_ne!(seen[index as usize-1].wire_body,seen[index as usize].wire_body,"format repair is a new logical input");
        assert_eq!(next["text"],seen[0].request["text"],"schema unchanged");
        assert!(seen[index as usize].charge_revision>committed(&original.0));
        drop(seen);
        assert!(committed(&original.0)>committed(&charge));
        assert!(lookup(&lab.api,&rejection_id(&a.id,index)).await.is_none());
        originals.push(original);
    }
    let complete=receipt(&lab.api,&contract::completion_id(&a.id)).await;
    let Observation::NativeLoopCompleted {model_requests,calls,requests,final_response_index}=report(&complete.1).observation else {panic!("completion absent")};
    assert_eq!(model_requests,3);assert_eq!(final_response_index.unwrap_or(model_requests),3);assert!(calls.is_empty());
    assert_eq!(requests,(1..=2).map(|i|NativeRequestEvidence::FormatRepair {request_index:i,operation_id:contract::format_failure_id(&a.id,i)}).collect::<Vec<_>>());
    peer.quiet(3).await;worker.stop().await;controller.stop().await;
    let mut controller=lab.controller("controller-two").await;
    assert_eq!(get(&lab.api,&task.id).await,task);
    for (i,old) in originals.iter().enumerate() {let current=receipt(&lab.api,&contract::format_failure_id(&a.id,i as u32+1)).await;assert_eq!(current.1,old.1);assert_eq!(current.0.commit,old.0.commit);}
    peer.quiet(3).await;controller.stop().await;peer.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=4)]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A complete model reply that violates its declared output format MUST be retried with the specific validation error within the remaining request allowance and deadline.
async fn format_failure_exhaustion_blocks_without_refund_or_extra_post_after_replacement() {
    let lab=Lab::new("format-repair-exhaustion");let mut controller=lab.controller("controller-one").await;
    let submitted=lab.submit(1).await;let mut peer=Peer::start(&lab,&submitted.id,Replies::Format {invalid:1,lost_at:None}).await;
    let mut worker=lab.worker(&peer.endpoint,"worker-one");
    let task=until_task(&lab.api,&submitted.id,|t|t.status==TaskStatus::Blocked&&t.attempt.as_ref().is_some_and(|a|a.stopped)).await;
    let a=task.attempt.as_ref().unwrap();assert!(!a.uncertain);assert!(task.result.is_none());assert_eq!(task.input,submitted.input);
    assert_eq!(task.native_retry_block.as_ref().unwrap().reason,RetryBlockReason::RequestAllowance);
    receipt(&lab.api,&contract::format_failure_id(&a.id,1)).await;
    assert!(lookup(&lab.api,&contract::reservation_id(&a.id,2)).await.is_none());
    worker.stop().await;let mut worker=lab.worker(&peer.endpoint,"worker-two");lab.registered("worker-two",&mut worker).await;
    assert_eq!(get(&lab.api,&task.id).await,task);peer.quiet(1).await;
    worker.stop().await;controller.stop().await;peer.close().await;
}

#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn unknown_reply_after_format_repair_keeps_spare_allowance_and_is_not_retried() {
    let lab=Lab::new("format-repair-unknown");let mut controller=lab.controller("controller-one").await;
    let submitted=lab.submit(4).await;let mut peer=Peer::start(&lab,&submitted.id,Replies::Format {invalid:1,lost_at:Some(2)}).await;
    let mut worker=lab.worker(&peer.endpoint,"worker-one");
    let task=until_task(&lab.api,&submitted.id,|t|t.attempt.as_ref().is_some_and(|a|a.stopped&&a.uncertain)).await;
    let a=task.attempt.as_ref().unwrap();assert!(task.result.is_none());assert!(task.native_retry_block.is_none());
    receipt(&lab.api,&contract::format_failure_id(&a.id,1)).await;
    let (_,charged)=receipt(&lab.api,&contract::reservation_id(&a.id,2)).await;
    assert!(matches!(report(&charged).observation,Observation::Usage {model_requests:2,input_tokens:Some(5),output_tokens:Some(1), ..}));
    assert!(lookup(&lab.api,&contract::format_failure_id(&a.id,2)).await.is_none());
    assert!(lookup(&lab.api,&contract::reservation_id(&a.id,3)).await.is_none());
    worker.stop().await;let mut worker=lab.worker(&peer.endpoint,"worker-two");lab.registered("worker-two",&mut worker).await;
    let after=get(&lab.api,&task.id).await;assert_eq!(after.input,submitted.input);assert!(after.result.is_none());assert!(after.attempt.as_ref().unwrap().uncertain);
    peer.quiet(2).await;worker.stop().await;controller.stop().await;peer.close().await;
}
