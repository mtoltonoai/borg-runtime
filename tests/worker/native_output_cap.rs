//! Counted real worker/controller/helper continuation, with original accounting.
use super::*;
fn binding(bytes:&[u8])->ProviderRequestBinding {ProviderRequestBinding {bytes:bytes.len() as u64,sha256:aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256,bytes).as_ref().iter().map(|b|format!("{b:02x}")).collect()}}
#[tokio::test(flavor="multi_thread",worker_threads=4)]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A complete validated provider terminal response identifying an output-cap truncation MUST be continued from retained output within the remaining request allowance and deadline.
async fn capped_prefix_and_undispatched_call_continue_with_original_receipts() {
 for partial_call in [false,true] {
    let lab=Lab::new(if partial_call {"cap-partial-call"} else {"cap-text"});let mut controller=lab.controller("controller-one").await;
    let submitted=lab.submit(2).await;let mut peer=Peer::start(&lab,&submitted.id,Replies::Cap {partial_call,lost_at:None}).await;
    let mut worker=lab.worker(&peer.endpoint,"worker-one");
    let t=until_task(&lab.api,&submitted.id,|t|t.status==TaskStatus::Succeeded).await;
    assert_eq!(t.input,submitted.input);assert_eq!(t.result.as_ref().unwrap().output,Some(json!({"result":"ok"})));
    let a=t.attempt.as_ref().unwrap();assert!(a.stopped&&!a.uncertain);
    let original=receipt(&lab.api,&contract::output_cap_id(&a.id,1)).await;
    let Observation::NativeOutputCapped {request_index,undispatched_calls,request_binding,output_binding}=report(&original.1).observation else {panic!("original cap missing")};
    assert_eq!(request_index,1);assert_eq!(undispatched_calls,u32::from(partial_call));
    let items=if partial_call {json!([{"type":"function_call","id":"cap-item","call_id":"cap-call","name":"collab_task_get","arguments":"{\"task_id\":","status":"incomplete"}])} else {json!([{"type":"message","role":"assistant","status":"incomplete","content":[{"type":"output_text","text":"{\"result\":\""}]}])};
    assert_eq!(output_binding,binding(&serde_json::to_vec(&items).unwrap()));
    {
      let seen=peer.seen.lock().unwrap();assert_eq!(seen.len(),2);assert_eq!(request_binding,binding(&seen[0].wire_body));
      assert!(seen[0].charge_revision<committed(&original.0)&&committed(&original.0)<seen[1].charge_revision);
      let inputs=seen[1].request["input"].as_array().unwrap();
      if partial_call {let encoded=serde_json::to_string(&items[0]).unwrap();assert!(inputs.iter().any(|i|i["role"]=="assistant"&&i["content"].as_str().is_some_and(|s|s.ends_with(&encoded))));}
      else {assert!(inputs.contains(&items[0]));}
      assert!(!inputs.iter().any(|i|i["type"]=="function_call_output"));
      assert!(inputs.iter().any(|i|i["role"]=="developer"&&i["content"].as_str().is_some_and(|s|s.contains("output cap"))));
      assert_eq!(seen[0].request["text"],seen[1].request["text"]);assert_eq!(seen[0].request["max_output_tokens"],seen[1].request["max_output_tokens"]);
    }
    assert!(lookup(&lab.api,&contract::call_operation_id(&a.id,1,1)).await.is_none());
    assert!(lookup(&lab.api,&contract::format_failure_id(&a.id,1)).await.is_none());
    let (_,charge)=receipt(&lab.api,&contract::reservation_id(&a.id,2)).await;
    assert!(matches!(report(&charge).observation,Observation::Usage {model_requests:2,input_tokens:Some(5),output_tokens:Some(1), ..}));
    let (_,complete)=receipt(&lab.api,&contract::completion_id(&a.id)).await;
    let Observation::NativeLoopCompleted {model_requests,calls,requests,final_response_index}=report(&complete).observation else {panic!("completion missing")};
    assert_eq!(model_requests,2);assert_eq!(final_response_index.unwrap_or(model_requests),2);assert!(calls.is_empty());assert_eq!(requests,vec![NativeRequestEvidence::OutputCap {request_index:1,operation_id:contract::output_cap_id(&a.id,1)}]);
    peer.quiet(2).await;worker.stop().await;controller.stop().await;
    let mut controller=lab.controller("controller-two").await;assert_eq!(get(&lab.api,&t.id).await,t);
    assert_eq!(receipt(&lab.api,&contract::output_cap_id(&a.id,1)).await.1,original.1);
    peer.quiet(2).await;controller.stop().await;peer.close().await;
 }
}
#[tokio::test(flavor="multi_thread",worker_threads=4)]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A complete validated provider terminal response identifying an output-cap truncation MUST be continued from retained output within the remaining request allowance and deadline.
async fn capped_allowance_blocks_without_refund_or_replacement_post() {
 let lab=Lab::new("cap-allowance");let mut controller=lab.controller("controller-one").await;let submitted=lab.submit(1).await;
 let mut peer=Peer::start(&lab,&submitted.id,Replies::Cap {partial_call:false,lost_at:None}).await;let mut worker=lab.worker(&peer.endpoint,"worker-one");
 let t=until_task(&lab.api,&submitted.id,|t|t.status==TaskStatus::Blocked&&t.attempt.as_ref().is_some_and(|a|a.stopped)).await;
 let a=t.attempt.as_ref().unwrap();assert!(!a.uncertain);assert!(t.result.is_none());assert_eq!(t.input,submitted.input);
 assert_eq!(t.native_retry_block.as_ref().unwrap().reason,RetryBlockReason::RequestAllowance);receipt(&lab.api,&contract::output_cap_id(&a.id,1)).await;
 assert!(lookup(&lab.api,&contract::reservation_id(&a.id,2)).await.is_none());worker.stop().await;
 let mut worker=lab.worker(&peer.endpoint,"worker-two");lab.registered("worker-two",&mut worker).await;assert_eq!(get(&lab.api,&t.id).await,t);
 peer.quiet(1).await;worker.stop().await;controller.stop().await;peer.close().await;
}
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn unknown_after_cap_keeps_spare_allowance_and_never_resends_on_replacement() {
 let lab=Lab::new("cap-unknown");let mut controller=lab.controller("controller-one").await;let submitted=lab.submit(4).await;
 let mut peer=Peer::start(&lab,&submitted.id,Replies::Cap {partial_call:false,lost_at:Some(2)}).await;let mut worker=lab.worker(&peer.endpoint,"worker-one");
 let t=until_task(&lab.api,&submitted.id,|t|t.attempt.as_ref().is_some_and(|a|a.stopped&&a.uncertain)).await;let a=t.attempt.as_ref().unwrap();
 assert!(t.result.is_none()&&t.native_retry_block.is_none());receipt(&lab.api,&contract::output_cap_id(&a.id,1)).await;
 let (_,r)=receipt(&lab.api,&contract::reservation_id(&a.id,2)).await;assert!(matches!(report(&r).observation,Observation::Usage {model_requests:2,input_tokens:Some(5),output_tokens:Some(1), ..}));
 assert!(lookup(&lab.api,&contract::output_cap_id(&a.id,2)).await.is_none());assert!(lookup(&lab.api,&contract::reservation_id(&a.id,3)).await.is_none());
 worker.stop().await;let mut worker=lab.worker(&peer.endpoint,"worker-two");lab.registered("worker-two",&mut worker).await;
 let after=get(&lab.api,&t.id).await;assert_eq!(after.input,submitted.input);assert!(after.result.is_none()&&after.attempt.as_ref().unwrap().uncertain);
 peer.quiet(2).await;worker.stop().await;controller.stop().await;peer.close().await;
}
