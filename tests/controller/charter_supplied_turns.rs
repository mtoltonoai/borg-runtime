//! Actual controller transaction/reopen boundaries; public event bodies are
//! explicit controlled inputs, not a provider or installed Codex qualification.
use super::*;
use serde_json::Value;
fn begun(caller:&Caller,id:&str,delivery:&CharterSuppliedContext,index:u32)->Operation{
    Operation::ObserveCharter{operation_id:id.into(),caller:caller.clone(),observation:CharterObservation::BeginSuppliedTurn{
        delivery_id:delivery.delivery_id.clone(),outer_index:index,request_id:u64::from(index)+10,
        thread_id:"actual-scripted-thread".into(),output_schema:supplied_result_schema()}}
}
fn turn(reply:&Reply)->CharterSuppliedTurn{match outcome(reply){CharterOutcome::SuppliedTurn(v)=>v,v=>panic!("{v:?}")}}
fn turn_ack(caller:&Caller,id:&str,index:u32)->Operation{
    Operation::ObserveCharter{operation_id:format!("{id}-ack"),caller:caller.clone(),observation:CharterObservation::AckSuppliedTurn{
        turn_id:id.into(),request_id:u64::from(index)+10,response:json!({"turn":{"id":format!("public-{index}"),"status":"inProgress","items":[]}})}}
}
fn result(caller:&Caller,id:&str,index:u32,declaration:Option<Value>)->Operation{
    Operation::ObserveCharter{operation_id:format!("{id}-reply"),caller:caller.clone(),observation:CharterObservation::SuppliedReply{
        turn_id:id.into(),terminal:CharterSuppliedTerminal{thread_id:"actual-scripted-thread".into(),turn_id:format!("public-{index}"),status:"completed".into(),error:None},
        reply:CharterReplyObservation{output:Some(json!({"controlled_public_result":"original"})),output_reference:None,response_id:None,
            output_item_ids:vec![format!("item-{index}")],tool_call_ids:vec![],declaration}}}
}
async fn delivered(c:&Controller,caller:&Caller,id:&str)->CharterSuppliedContext{
    let request=intent(c,caller,id).await;delivery(&send(c,request).await);
    delivery(&send(c,ack(caller,&format!("{id}-ack"),id)).await)
}
async fn outer(c:&Controller,id:&str)->CharterSuppliedTurn{
    match query(c,CharterQuery::SuppliedTurn{turn_id:id.into()}).await{CharterQueryResult::SuppliedTurn(v)=>v,v=>panic!("{v:?}")}
}
#[tokio::test]
async fn supplied_outer_original_result_replay_reopen_keeps_delivered_source_and_passive_audit(){
    let p=path();let cfg=charter_config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let (t,_,caller)=coding_setup(&c,"outer-original").await;let accepted=get(&c,&t.id).await;
    let delivery=delivered(&c,&caller,"outer-context").await;
    let op=begun(&caller,"outer-one",&delivery,1);
    let old=c.handle(Request{protocol_revision:20,request_id:"old-supplied-wire".into(),command:op.clone()}).await;
    assert_eq!(code(old.outcome),"unsupported_version");assert!(old.commit.is_none());
    assert_eq!(code(call(&c,Operation::GetOperation{operation_id:"outer-one".into()}).await),"not_found");
    let first=send(&c,op.clone()).await;let boundary=turn(&first);
    let newer=other_source(&c,"outer-newer-pending").await;let before=session(&c,&t.session_id).await;
    outcome(&set(&c,"outer-set-pending",&before.session,&newer).await);let pending=session(&c,&t.session_id).await;
    let ack=turn_ack(&caller,"outer-one",1);let acked=send(&c,ack.clone()).await;
    // The first usable outcome comes from the original operation lookup too.
    let original=call(&c,Operation::GetOperation{operation_id:"outer-one-ack".into()}).await;
    assert!(matches!(original,Outcome::Ok{data:Data::Operation(_)}));
    let declaration=serde_json::to_value(DeclaredCharterReferences{revision:delivery.effective.reference.clone(),clauses:delivery.effective.source.clauses.clone()}).unwrap();
    let observed=result(&caller,"outer-one",1,Some(declaration.clone()));let committed=send(&c,observed.clone()).await;
    let CharterOutcome::Reply(reply)=outcome(&committed) else{panic!()};assert!(reply.audit.findings.is_empty());
    assert_eq!(reply.observation.declaration,Some(declaration));assert_eq!(reply.effective,boundary.boundary.effective.reference);
    assert_eq!(session(&c,&t.session_id).await,pending);assert_eq!(get(&c,&t.id).await,accepted);
    let second=begun(&caller,"outer-two",&delivery,2);let next=turn(&send(&c,second).await);
    assert_eq!(next.boundary.effective,boundary.boundary.effective);assert_eq!(pending.session.pending,Some(newer));
    send(&c,turn_ack(&caller,"outer-two",2)).await;
    let missing=send(&c,result(&caller,"outer-two",2,None)).await;
    assert!(matches!(outcome(&missing),CharterOutcome::Reply(v) if v.audit.findings==vec![CharterAuditFinding::MissingMetadata]));
    assert_eq!(code(call(&c,Operation::GetOperation{operation_id:ids::reservation_id(&caller.attempt_id,1)}).await),"not_found");
    assert!(matches!(query(&c,CharterQuery::Turns{session_id:t.session_id.clone(),after:None,limit:4}).await,CharterQueryResult::Turns(page) if page.items.is_empty()));
    let exact=outer(&c,"outer-one").await;drop(c);controller_owner_released(&p).await;
    let c=Controller::open_with_config(&p,cfg).unwrap();assert_eq!(outer(&c,"outer-one").await,exact);
    assert_eq!(send(&c,op).await.commit,first.commit);assert_eq!(send(&c,ack).await.commit,acked.commit);
    assert_eq!(send(&c,observed).await.commit,committed.commit);assert_eq!(get(&c,&t.id).await,accepted);
}
#[tokio::test]
async fn supplied_outer_unknown_start_wrong_identity_and_late_result_never_regrant_work(){
    let p=path();let cfg=charter_config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let (t,_,caller)=coding_setup(&c,"outer-fences").await;let delivery=delivered(&c,&caller,"fenced-context").await;
    send(&c,begun(&caller,"fenced-one",&delivery,1)).await;
    assert_eq!(code(send(&c,begun(&caller,"not-another",&delivery,2)).await.outcome),"reconciliation_required");
    assert!(matches!(send(&c,result(&caller,"fenced-one",1,None)).await.outcome,Outcome::Refused{..}));
    send(&c,turn_ack(&caller,"fenced-one",1)).await;
    let mut wrong=result(&caller,"fenced-one",1,None);
    if let Operation::ObserveCharter{operation_id,observation:CharterObservation::SuppliedReply{terminal,..},..}=&mut wrong{*operation_id="wrong-thread".into();terminal.thread_id="foreign".into();}
    assert_eq!(code(send(&c,wrong).await.outcome),"source_mismatch");
    let revision=get(&c,&t.id).await.revision;task(call(&c,Operation::CancelTask{operation_id:"outer-cancel".into(),task_id:t.id.clone(),expected_revision:revision}).await);
    let cancelled=get(&c,&t.id).await;let binding=session(&c,&t.session_id).await;
    // Late immutable audit is allowed; it neither grants work nor clears stop.
    let mut late=result(&caller,"fenced-one",1,Some(json!({"wrong":"shape"})));
    if let Operation::ObserveCharter{operation_id,..}=&mut late{*operation_id="late-result".into();}
    assert!(matches!(outcome(&send(&c,late).await),CharterOutcome::Reply(v) if v.audit.findings==vec![CharterAuditFinding::MalformedMetadata]));
    assert_eq!(get(&c,&t.id).await,cancelled);assert_eq!(session(&c,&t.session_id).await,binding);
    assert!(matches!(send(&c,begun(&caller,"after-stop",&delivery,2)).await.outcome,Outcome::Refused{..}));
    drop(c);controller_owner_released(&p).await;let c=Controller::open_with_config(&p,cfg).unwrap();
    assert_eq!(outer(&c,"fenced-one").await.reply.unwrap().audit.findings,vec![CharterAuditFinding::MalformedMetadata]);
    assert_eq!(get(&c,&t.id).await,cancelled);
}
#[tokio::test]
async fn supplied_schema21_upgrade_preserves_original_bytes_and_current_schema_reopens_new_records(){
    let p=path();let mut cfg=charter_config();Arc::make_mut(&mut cfg.limits).task_defaults.agent_id=None;
    let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let (t,_,caller)=coding_setup(&c,"outer-schema").await;let context=delivered(&c,&caller,"schema-context").await;
    drop(c);controller_owner_released(&p).await;
    let rows=|conn:&rusqlite::Connection|->Vec<(String,String,String)>{
        conn.prepare("SELECT id,request,outcome FROM receipts ORDER BY id").unwrap().query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap().map(|r|r.unwrap()).collect()
    };
    // A controlled prior-vocabulary fixture: no claim that current bytes were
    // emitted by a historical executable. This fixture explicitly restores the old unique-task session shape and drops only proven empty/derived new tables.
    let sql=rusqlite::Connection::open(&p).unwrap();let before=rows(&sql);
    let original_context:String=sql.query_row("SELECT body FROM charter_requests WHERE id='schema-context'",[],|r|r.get(0)).unwrap();
    assert_eq!(sql.query_row("SELECT count(*) FROM agent_retirements",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    controlled_prior_session_shape(&sql);
    sql.execute_batch("DROP TABLE agent_retirements").unwrap();
    sql.pragma_update(None,"user_version",21).unwrap();drop(sql);
    let c=Controller::open_with_config(&p,cfg.clone()).unwrap();assert_eq!(read(&c,"schema-context").await,context);
    drop(c);controller_owner_released(&p).await;let sql=rusqlite::Connection::open(&p).unwrap();
    assert_eq!(sql.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),24);
    assert_eq!(rows(&sql),before);assert_eq!(sql.query_row("SELECT body FROM charter_requests WHERE id='schema-context'",[],|r|r.get::<_,String>(0)).unwrap(),original_context);drop(sql);
    let c=Controller::open_with_config(&p,cfg.clone()).unwrap();send(&c,begun(&caller,"schema-outer",&context,1)).await;
    send(&c,turn_ack(&caller,"schema-outer",1)).await;send(&c,result(&caller,"schema-outer",1,None)).await;
    let original=outer(&c,"schema-outer").await;drop(c);controller_owner_released(&p).await;
    let c=Controller::open_with_config(&p,cfg.clone()).unwrap();assert_eq!(outer(&c,"schema-outer").await,original);assert_eq!(get(&c,&t.id).await.input.definition.executor,ExecutorKind::Coding);
    drop(c);controller_owner_released(&p).await;
    // New vocabulary falsely relabeled21 is refused before migration/rewrite.
    let sql=rusqlite::Connection::open(&p).unwrap();assert_eq!(sql.query_row("SELECT count(*) FROM agent_retirements",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    controlled_prior_session_shape(&sql);
    sql.execute_batch("DROP TABLE agent_retirements").unwrap();
    sql.pragma_update(None,"user_version",21).unwrap();let corrupt=rows(&sql);drop(sql);
    assert!(Controller::open_with_config(&p,cfg).is_err());controller_owner_released(&p).await;
    let sql=rusqlite::Connection::open(&p).unwrap();assert_eq!(rows(&sql),corrupt);assert_eq!(sql.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),21);
}

#[tokio::test]
async fn supplied_canonical_retry_requires_original_terminal_and_ack_and_only_then_delivers_pending(){
    for missing in ["none","context_ack","public_terminal","legacy_terminal"] {
        let p=path();let cfg=charter_config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
        let (t,a,caller)=coding_setup(&c,&format!("retry-{missing}")).await;
        let request=intent(&c,&caller,"retry-context").await;let original_reply=send(&c,request.clone()).await;
        let mut old=delivery(&original_reply);
        if missing!="context_ack" {old=delivery(&send(&c,ack(&caller,"retry-context-ack","retry-context")).await);}
        if missing=="none"||missing=="public_terminal" {
            send(&c,begun(&caller,"retry-public",&old,1)).await;
            send(&c,turn_ack(&caller,"retry-public",1)).await;
            if missing=="none" {
                let mut failure=result(&caller,"retry-public",1,None);
                if let Operation::ObserveCharter{observation:CharterObservation::SuppliedReply{terminal,..},..}=&mut failure {
                    terminal.status="failed".into();terminal.error=Some(json!({"message":"controlled public failure"}));
                }
                outcome(&send(&c,failure).await);
            }
        }
        // No fabricated ExecutorOutcome: this exactly exercises the existing
        // stopped failed-candidate contract that a historical retry may carry.
        task(report(&c,&a,"retry-stopped",Observation::ProcessTreeStopped).await);
        let failed=task(report(&c,&a,"retry-candidate",Observation::ResultCandidate{checks_passed:false,summary:"acceptance failed".into(),checkpoint:None,validation:vec![],output:None}).await);
        assert_eq!(failed.status,TaskStatus::Failed);assert!(failed.attempt.as_ref().unwrap().stopped);assert!(!failed.attempt.as_ref().unwrap().uncertain);
        let pending=other_source(&c,&format!("retry-new-source-{missing}")).await;let binding=session(&c,&t.session_id).await;
        outcome(&set(&c,"retry-pending",&binding.session,&pending).await);let before=session(&c,&t.session_id).await;
        let queued=task(call(&c,Operation::RetryTask{operation_id:"canonical-retry".into(),task_id:t.id.clone(),expected_revision:failed.revision}).await);
        assert_eq!(queued.id,t.id);assert_eq!(queued.session_id,t.session_id);
        let w=worker(&a.worker_id,&a.worker_incarnation,0,1);let successor=run(&commands(&c,&w,vec![]).await);
        assert_ne!(successor.id,a.id);assert!(successor.generation>a.generation);
        let next=Caller{task_id:t.id.clone(),attempt_id:successor.id.clone(),generation:successor.generation,worker_id:successor.worker_id.clone(),incarnation:successor.worker_incarnation.clone()};
        let request_next=intent(&c,&next,"retry-context-next").await;let response=send(&c,request_next.clone()).await;
        if missing!="none" {
            assert_eq!(code(response.outcome),"reconciliation_required");assert_eq!(session(&c,&t.session_id).await,before);
            assert_eq!(read(&c,"retry-context").await,old);continue;
        }
        let selected=delivery(&response);assert_eq!(selected.effective.reference,pending);assert!(selected.acknowledgment.is_none());
        assert_eq!(read(&c,"retry-context").await,old); // old delivered source is immutable
        let mut acknowledged=ack(&next,"retry-context-next-ack","retry-context-next");
        if let Operation::ObserveCharter{observation:CharterObservation::AckSuppliedContext{response,..},..}=&mut acknowledged {response["thread"]["id"]=json!("actual-retry-thread");}
        let delivered_next=delivery(&send(&c,acknowledged).await);assert_eq!(delivered_next.acknowledgment.unwrap().thread_id,"actual-retry-thread");
        assert_eq!(send(&c,request_next).await.commit,response.commit);assert_eq!(send(&c,request).await.commit,original_reply.commit);
        drop(c);controller_owner_released(&p).await;let c=Controller::open_with_config(&p,cfg).unwrap();
        assert_eq!(read(&c,"retry-context").await,old);assert_eq!(read(&c,"retry-context-next").await.effective.reference,pending);
        assert!(get(&c,&t.id).await.result.is_none());
    }
}

#[tokio::test]
async fn supplied_refused_new_tags_require_schema22_even_when_charter_policy_is_absent(){
    for tag in 0..3 {
        let p=path();let cfg=config();assert!(cfg.limits.charter.is_none());
        let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
        let caller=Caller{task_id:"absent-task".into(),attempt_id:"absent-attempt".into(),generation:1,worker_id:"absent-worker".into(),incarnation:"one".into()};
        let observation=match tag {
            0=>CharterObservation::BeginSuppliedTurn{delivery_id:"absent-context".into(),outer_index:1,request_id:1,thread_id:"thread".into(),output_schema:supplied_result_schema()},
            1=>CharterObservation::AckSuppliedTurn{turn_id:"absent-outer".into(),request_id:1,response:json!({"turn":{"id":"public"}})},
            _=>CharterObservation::SuppliedReply{turn_id:"absent-outer".into(),terminal:CharterSuppliedTerminal{thread_id:"thread".into(),turn_id:"public".into(),status:"failed".into(),error:None},reply:CharterReplyObservation{output:None,output_reference:None,response_id:None,output_item_ids:vec![],tool_call_ids:vec![],declaration:None}},
        };
        let op=Operation::ObserveCharter{operation_id:"disabled-original".into(),caller,observation};
        let refused=send(&c,op.clone()).await;assert_eq!(code(refused.outcome.clone()),"not_configured");assert!(refused.commit.is_some());
        assert_eq!(send(&c,op.clone()).await.commit,refused.commit);
        drop(c);controller_owner_released(&p).await;
        let c=Controller::open_with_config(&p,cfg.clone()).unwrap();assert_eq!(send(&c,op).await.outcome,refused.outcome);
        drop(c);controller_owner_released(&p).await;
        let sql=rusqlite::Connection::open(&p).unwrap();
        assert_eq!(sql.query_row("SELECT count(*) FROM charter_requests",[],|r|r.get::<_,i64>(0)).unwrap(),0);
        assert_eq!(sql.query_row("SELECT count(*) FROM agent_retirements",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    controlled_prior_session_shape(&sql);
    sql.execute_batch("DROP TABLE agent_retirements").unwrap();
    sql.pragma_update(None,"user_version",21).unwrap();drop(sql);let before=stored_bytes(&p);
        assert!(Controller::open_with_config(&p,cfg).is_err(),"new tag {tag} escaped schema22 with policy absent");
        assert_eq!(stored_bytes(&p),before,"failed admission rewrote new tag {tag}");
    }
}

#[tokio::test]
async fn supplied_successful_ack_requires_its_own_original_receipt_after_terminal(){
    let p=path();let cfg=charter_config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let (_,_,caller)=coding_setup(&c,"outer-ack-membership").await;let context=delivered(&c,&caller,"membership-context").await;
    send(&c,begun(&caller,"membership-outer",&context,1)).await;
    let original=send(&c,turn_ack(&caller,"membership-outer",1)).await;outcome(&original);
    outcome(&send(&c,result(&caller,"membership-outer",1,None)).await);
    let mut duplicate=turn_ack(&caller,"membership-outer",1);
    if let Operation::ObserveCharter{operation_id,..}=&mut duplicate{*operation_id="different-ack-original".into();}
    assert_eq!(code(send(&c,duplicate).await.outcome),"operation_conflict");
    let complete=outer(&c,"membership-outer").await;drop(c);controller_owner_released(&p).await;
    // A later real terminal does not change the original ACK outcome shape.
    let c=Controller::open_with_config(&p,cfg.clone()).unwrap();assert_eq!(outer(&c,"membership-outer").await,complete);
    drop(c);controller_owner_released(&p).await;
    // Deliberate storage corruption: change only the genuine duplicate refusal
    // to another operation's success. Its own ID/revision/request stay intact.
    let sql=rusqlite::Connection::open(&p).unwrap();
    assert_eq!(sql.execute("UPDATE receipts SET outcome=?1 WHERE id='different-ack-original'",[serde_json::to_string(&original.outcome).unwrap()]).unwrap(),1);
    drop(sql);let before=stored_bytes(&p);
    assert!(Controller::open_with_config(&p,cfg).is_err());assert_eq!(stored_bytes(&p),before);
}

// Source-constructed prior-format fixture only, never a historical producer.
// Refuse to discard real lineage or new metadata; retain all original row bytes.
fn controlled_prior_session_shape(sql:&rusqlite::Connection) {
    assert_eq!(sql.query_row("SELECT count(*) FROM task_assignments",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    assert_eq!(sql.query_row("SELECT count(*) FROM session_inputs WHERE previous_session_id IS NOT NULL OR operation_id IS NOT NULL",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    let inputs=sql.prepare("SELECT i.body,json_extract(t.body,'$.task.input') FROM session_inputs i JOIN tasks t ON t.id=i.task_id AND t.session=i.session_id ORDER BY i.session_id").unwrap()
        .query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?))).unwrap().map(|r|r.unwrap()).collect::<Vec<_>>();
    assert_eq!(inputs.len() as i64,sql.query_row("SELECT count(*) FROM sessions",[],|r|r.get::<_,i64>(0)).unwrap());
    for (actual,original) in inputs {
        assert_eq!(actual,original);let input:TaskInput=serde_json::from_str(&actual).unwrap();
        assert!(input.resolved_limits.as_ref().unwrap().global_metadata.task_defaults.agent_id.is_none());
        assert!(!matches!(input.native_workflow.as_deref(),Some(NativeWorkflow::Reassign{..})));
    }
    sql.execute_batch("CREATE TEMP TABLE original_sessions AS SELECT * FROM sessions; DROP TABLE sessions;").unwrap();
    let original=include_str!("../../src/controller/schema12.sql").split(';').find(|statement|statement.trim_start().starts_with("CREATE TABLE sessions(")).unwrap();
    sql.execute_batch(original).unwrap();
    sql.execute_batch("ALTER TABLE sessions ADD COLUMN workflow_delivery TEXT; INSERT INTO sessions SELECT * FROM original_sessions; DROP TABLE original_sessions; CREATE INDEX sessions_agent ON sessions(agent,seq); DROP TABLE session_inputs; DROP TABLE task_assignments;").unwrap();
}
