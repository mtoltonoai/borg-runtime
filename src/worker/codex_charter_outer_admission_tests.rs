//! Real Controller and process-owned scripted public peer. No provider or
//! installed Codex qualification is implied by these controlled JSON events.
use super::*;
const OUTER_PEER:&str=r#"#!/usr/bin/python3
import json,pathlib,sys
root=pathlib.Path.cwd()
def emit(value):print(json.dumps(value,separators=(',',':')),flush=True)
for line in sys.stdin:
    r=json.loads(line)
    with (root/'requests.jsonl').open('a') as f:f.write(json.dumps(r,separators=(',',':'))+'\n')
    if 'id' not in r:continue
    method=r.get('method')
    if method=='thread/start':
        assert (root/'intent-ack').exists()
        response={'thread':{'id':'supplied-original-thread','items':[]},'model':r['params']['model'],'reasoningEffort':r['params']['config']['model_reasoning_effort']}
        (root/'response.json').write_text(json.dumps(response))
    elif method=='turn/start':
        assert (root/'context-ack').exists()
        assert r['params']['outputSchema']['properties']['charter_references']
        marker=root/'executor-state/supplied-charter/outer-1/dispatch.json'
        assert json.loads(marker.read_text())['request_id']==r['id']
        if (root/'lose-start').exists():sys.exit(0)
        emit({'id':r['id'],'result':{'turn':{'id':'supplied-turn','items':[],'status':'inProgress'}}})
        if (root/'terminal-failure').exists():
            item={'type':'agentMessage','id':'actual-commentary','phase':'commentary','text':'work in progress, not a final answer'}
            emit({'method':'item/completed','params':{'threadId':'supplied-original-thread','turnId':'supplied-turn','completedAtMs':1,'item':item}})
            terminal={'method':'turn/completed','params':{'threadId':'supplied-original-thread','turn':{'id':'supplied-turn','status':'failed','error':{'message':'controlled rate limit','codexErrorInfo':'usageLimitExceeded'},'items':[item]}}}
            (root/'terminal.json').write_text(json.dumps(terminal));emit(terminal);continue
        declaration=json.loads((root/'declaration.json').read_text())
        text=json.dumps({'result':'original result','charter_references':declaration},separators=(',',':'))
        item={'type':'agentMessage','id':'actual-final-item','phase':'final_answer','text':text}
        emit({'method':'item/completed','params':{'threadId':'supplied-original-thread','turnId':'supplied-turn','completedAtMs':1,'item':item}})
        terminal={'method':'turn/completed','params':{'threadId':'supplied-original-thread','turn':{'id':'supplied-turn','status':'completed','items':[item]}}}
        (root/'terminal.json').write_text(json.dumps(terminal))
        emit(terminal);continue
    elif method=='command/exec':response={'exitCode':0,'stdout':'','stderr':''}
    else:response={}
    emit({'id':r['id'],'result':response})
"#;
async fn run_outer(f:&AdmissionFixture,control:&Control)->std::result::Result<super::super::Result,ExecutionFailure>{
    let (events,_received)=mpsc::channel(16);
    let executor_state=f.root.join("executor-state");process::private_directory(&executor_state).unwrap();
    let processes=f.state.join("processes");let inference=InferenceSettings::selected();
    crate::worker::boxed_execution(|| execute_with_credentials_collaboration_and_charter(&f.binary,&f.root,&executor_state,&processes,
        &inference,&f.runtime.execution,"Original product task.",control,events,&[],None,None,None,Some(&f.scope))).await
}
#[tokio::test]
async fn supplied_final_result_original_ack_loss_reconciles_without_second_start_and_keeps_full_metadata(){
    let f=AdmissionFixture::new("outer-result").await;let (_cancel,control)=f.control();f.admit(&control).await;
    std::fs::write(&f.binary,OUTER_PEER).unwrap();
    let source=f.charter().await.charter;let declaration=serde_json::to_value(DeclaredCharterReferences{revision:source.reference,clauses:source.source.clauses}).unwrap();
    std::fs::write(f.root.join("declaration.json"),serde_json::to_vec(&declaration).unwrap()).unwrap();
    std::fs::write(f.root.join("drop-outer-ack"),b"one actual committed response loss").unwrap();
    let accepted=f.task().await.input;let result=run_outer(&f,&control).await.unwrap();assert!(result.success);assert_eq!(result.summary,"original result");f.closed();
    assert_eq!(peer_requests(&f.root).iter().filter(|v|v["method"]=="turn/start").count(),1);
    let id=format!("{}-charter-outer-1",f.scope.caller.attempt_id);
    let result=f.call(Operation::QueryCharter{query:CharterQuery::SuppliedTurn{turn_id:id.clone()}}).await;
    let Outcome::Ok{data:Data::CharterQuery(CharterQueryResult::SuppliedTurn(value))}=result.outcome else{panic!("{result:?}")};
    let reply=value.reply.unwrap();assert!(reply.audit.findings.is_empty());assert_eq!(reply.observation.declaration,Some(declaration));
    assert!(reply.observation.output.is_none());assert!(reply.observation.response_id.is_none());assert!(reply.observation.tool_call_ids.is_empty());
    let reference=reply.observation.output_reference.unwrap();let mut bytes=Vec::new();
    for index in 0..reference.chunks{
        let result=f.call(Operation::QueryCharter{query:CharterQuery::OutputChunk{request_id:id.clone(),index}}).await;
        let Outcome::Ok{data:Data::CharterQuery(CharterQueryResult::OutputChunk(chunk))}=result.outcome else{panic!("{result:?}")};
        for pair in chunk.data.as_bytes().chunks_exact(2){bytes.push(u8::from_str_radix(std::str::from_utf8(pair).unwrap(),16).unwrap());}
    }
    let raw:Value=serde_json::from_slice(&bytes).unwrap();assert_eq!(raw["terminal"],serde_json::from_slice::<Value>(&std::fs::read(f.root.join("terminal.json")).unwrap()).unwrap());
    assert_eq!(raw["items"][0]["params"]["item"]["id"],"actual-final-item");
    let (op,committed)=f.original(&format!("{id}-reply"));f.replay(op,&committed).await;
    assert!(f.trace.lock().unwrap().iter().any(|(op,_)|matches!(op,Operation::GetOperation{operation_id} if operation_id==&format!("{id}-ack"))));
    assert_eq!(f.task().await.input,accepted);assert!(f.task().await.result.is_none());f.evidence();
}
#[tokio::test]
async fn supplied_lost_public_start_stays_one_original_occurrence_without_final_or_restart(){
    let f=AdmissionFixture::new("outer-unknown").await;let (_cancel,control)=f.control();f.admit(&control).await;
    std::fs::write(&f.binary,OUTER_PEER).unwrap();std::fs::write(f.root.join("lose-start"),b"EOF after actual start").unwrap();
    let error=run_outer(&f,&control).await.unwrap_err();assert!(error.message.contains("uncertain"));f.closed();
    let id=format!("{}-charter-outer-1",f.scope.caller.attempt_id);
    let result=f.call(Operation::QueryCharter{query:CharterQuery::SuppliedTurn{turn_id:id}}).await;
    assert!(matches!(result.outcome,Outcome::Ok{data:Data::CharterQuery(CharterQueryResult::SuppliedTurn(v))} if v.acknowledgment.is_none()&&v.reply.is_none()));
    assert_eq!(peer_requests(&f.root).iter().filter(|v|v["method"]=="turn/start").count(),1);
    assert!(error.execution_uncertain);f.evidence();
}

#[tokio::test]
async fn supplied_failed_public_turn_retains_error_and_missing_final_without_commentary_inference(){
    let f=AdmissionFixture::new("outer-failed").await;let (_cancel,control)=f.control();f.admit(&control).await;
    std::fs::write(&f.binary,OUTER_PEER).unwrap();std::fs::write(f.root.join("terminal-failure"),b"public terminal without final item").unwrap();
    let accepted=f.task().await.input;let result=run_outer(&f,&control).await.unwrap();
    assert!(!result.success);assert_eq!(result.summary,"Executor turn failed");f.closed();
    let id=format!("{}-charter-outer-1",f.scope.caller.attempt_id);
    let queried=f.call(Operation::QueryCharter{query:CharterQuery::SuppliedTurn{turn_id:id.clone()}}).await;
    let Outcome::Ok{data:Data::CharterQuery(CharterQueryResult::SuppliedTurn(v))}=queried.outcome else{panic!("{queried:?}")};
    let terminal=v.terminal.unwrap();assert_eq!(terminal.status,"failed");
    assert_eq!(terminal.error,Some(json!({"message":"controlled rate limit","codexErrorInfo":"usageLimitExceeded"})));
    let reply=v.reply.unwrap();assert_eq!(reply.audit.findings,vec![crate::charter::CharterAuditFinding::MissingMetadata]);
    assert!(reply.observation.declaration.is_none());assert_eq!(reply.observation.output_item_ids,vec!["actual-commentary"]);
    assert!(reply.observation.response_id.is_none());assert!(reply.observation.tool_call_ids.is_empty());
    assert_eq!(peer_requests(&f.root).iter().filter(|v|v["method"]=="turn/start").count(),1);
    // Real accepted RunAttempt and main worker root admit and replay the same
    // output carrier; no executor event is replayed and no task result granted.
    assert!(f.state.join("charter-output").is_dir());assert!(!f.root.join("executor-state/charter-output").exists());
    let marker=std::fs::read(f.state.join("format.json")).unwrap();
    process::atomic_json(&f.state.join("format.json"),&json!({"format_revision":16})).unwrap();
    assert!(crate::worker::charter::admit(&f.state,&f.runtime,&f.scope.caller.worker_id).unwrap_err().contains("worker17"));
    // Supplied output retains its worker17 floor through both predecessor21
    // and current union22; admission never upgrades or rewrites the marker.
    for revision in [17,20,21,22] {
        process::atomic_json(&f.state.join("format.json"),&json!({"format_revision":revision})).unwrap();
        let selected=std::fs::read(f.state.join("format.json")).unwrap();
        crate::worker::charter::admit(&f.state,&f.runtime,&f.scope.caller.worker_id).unwrap();
        assert_eq!(std::fs::read(f.state.join("format.json")).unwrap(),selected);
    }
    std::fs::write(f.state.join("format.json"),marker).unwrap();
    crate::worker::charter::admit(&f.state,&f.runtime,&f.scope.caller.worker_id).unwrap();
    let original=f.original(&format!("{id}-reply"));
    crate::worker::charter::recover(&f.client,&f.state,&f.scope.controller_url,&f.scope.caller.worker_id).await.unwrap();
    assert_eq!(serde_json::to_value(f.original(&format!("{id}-reply"))).unwrap(),serde_json::to_value(original).unwrap());
    assert_eq!(f.task().await.input,accepted);assert!(f.task().await.result.is_none());f.evidence();
}
