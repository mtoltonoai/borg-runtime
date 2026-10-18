//! One actual native task, two explicit reads and three controlled POSTs.
use super::*;
use borg_runtime::read_page::ReadPage;
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn shared_read_pagination_native_explicit_continuation_no_autofetch() {
    Box::pin(journey()).await;
}
async fn journey() {
    let mut lab=Lab::new("shared-read-pagination");
    let limits=Arc::make_mut(&mut lab.limits);limits.execution.native.schema_bytes=65_536;limits.execution.native.json_nodes=16_384;limits.execution.native.context_bytes=262_144;
    limits.validate().unwrap();std::fs::write(lab.root.join("limits.json"),serde_json::to_vec_pretty(&*lab.limits).unwrap()).unwrap();
    let mut controller=lab.controller("controller").await;
    let api=ApiClient::new(&lab.url,lab.limits.clone()).unwrap();
    let mut input=lab.input(3);input.instruction="Explicit page fixture \"\\\n🙂 <script>not executable</script> ".repeat(300);
    let task=submit(&api,input.clone()).await;let id=task.id.clone();
    let pages=Arc::new(Mutex::new(Vec::<ReadPage>::new()));let observed=pages.clone();
    let peer=Peer::start(move|index,request|match index {
        0=>Some(function(index,"collab_task_get",json!({"task_id":id,"part":"instruction","max_bytes":8192}))),
        1|2=>{
            let call_id=format!("call-{}",index-1);
            let items=request["input"].as_array().unwrap().iter().filter(|v|v["type"]=="function_call_output"&&v["call_id"]==call_id).collect::<Vec<_>>();
            assert_eq!(items.len(),1);
            let result:Value=serde_json::from_str(items[0]["output"].as_str().unwrap()).unwrap();
            assert_eq!(result["outcome"]["status"],"ok");assert_eq!(result["outcome"]["data"]["kind"],"read_page");
            let page:ReadPage=serde_json::from_value(result["outcome"]["data"]["value"].clone()).unwrap();
            assert!(page.returned_bytes>0);assert!(!page.complete,"the fixture intentionally leaves unread content");
            let mut seen=observed.lock().unwrap();
            if let Some(first)=seen.first(){assert_eq!(page.identity,first.identity);assert_eq!(page.offset,first.returned_bytes);}
            let cursor=page.next_cursor.clone().unwrap();seen.push(page);
            if index==1{Some(function(index,"collab_read_continue",json!({"cursor":cursor})))}else{Some(final_value())}
        }
        _=>panic!("pagination must not automatically fetch pages or dispatch another model request"),
    }).await;
    let mut worker=lab.worker(&lab.url,&peer.endpoint,"page-reader");
    let finished=until(&api,&task.id,|t|t.status==TaskStatus::Succeeded).await;
    assert_eq!(finished.session_id,task.session_id);assert_eq!(finished.input,task.input);
    assert_eq!(finished.result.as_ref().unwrap().output,Some(json!(7)));assert_eq!(peer.count(),3);
    assert_eq!(pages.lock().unwrap().len(),2);
    let attempt=finished.attempt.as_ref().unwrap();
    let journal=lab.state().join("collaboration").join(process::component(&attempt.id));
    assert_eq!(std::fs::read_dir(&journal).unwrap().count(),2);
    for (i,page) in pages.lock().unwrap().iter().enumerate() {
        let value:Value=serde_json::from_slice(&std::fs::read(journal.join(format!("{:010}.json",i+1))).unwrap()).unwrap();
        assert_eq!(value["consumed"],true);assert_eq!(value["outcome"]["data"]["value"],serde_json::to_value(page).unwrap());
        assert_eq!(value["request"]["input"]["call"]["caller"]["task_id"],task.id);
    }
    worker.stop().await;controller.stop().await;
    assert_eq!(lab.usage(&attempt.id).0,3);
}
