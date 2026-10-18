//! Real controller, canonical operation receipts and reopen tests with controlled
//! observations. No provider/Codex/full-streaming qualification is claimed.
use super::*;
use borg_runtime::{charter::*,limits::{CharterLimits,DefinitionLimitField},mcp::Caller,native_loop_contract as ids};
fn charter_config()->ControllerConfig {
    let mut c=config();Arc::make_mut(&mut c.limits).charter=Some(CharterLimits{record_bytes:65_536,source_bytes:16_384,files:4,clauses:32,metadata_bytes:8192,references:32,reply_items:32,tool_calls:8,reference_depth:4,records:1024,page_items:4,page_bytes:65_536});c
}
fn source(label:&str)->CharterSource {
    let sentence=format!("The agent MUST preserve {label}.");
    CharterSource{files:vec![CharterFile{file:"charter.md".into(),text:format!("# Purpose\n\n{sentence}\n")}],clauses:vec![CharterClauseRef{file:"charter.md".into(),section:"Purpose".into(),sentence}]}
}
fn input(label:&str)->TaskInput {let mut t=native(label);t.definition.charter=Some(DefinitionCharter::Inline{source:source(label)});t}
async fn exchange(c:&Controller,command:Operation)->Reply {let r=c.handle(Request{protocol_revision:WIRE_REVISION,request_id:"charter-test".into(),command:command.clone()}).await;r.validate_for(&command).unwrap();r}
fn outcome(reply:&Reply)->CharterOutcome {match &reply.outcome {Outcome::Ok{data:Data::Charter(v)}=>v.clone(),x=>panic!("expected charter result: {x:?}")}}
async fn query(c:&Controller,query:CharterQuery)->CharterQueryResult {match call(c,Operation::QueryCharter{query}).await {Outcome::Ok{data:Data::CharterQuery(v)}=>v,x=>panic!("charter query: {x:?}")}}
async fn session(c:&Controller,id:&str)->EffectiveCharter {match query(c,CharterQuery::Session{session_id:id.into()}).await {CharterQueryResult::Session(Some(v))=>v,x=>panic!("session: {x:?}")}}
async fn turn(c:&Controller,id:&str)->CharterTurn {match query(c,CharterQuery::Turn{turn_id:id.into()}).await {CharterQueryResult::Turn(v)=>v,x=>panic!("turn: {x:?}")}}
async fn observe(c:&Controller,who:&Caller,id:&str,observation:CharterObservation)->Reply {exchange(c,Operation::ObserveCharter{operation_id:id.into(),caller:who.clone(),observation}).await}
async fn set(c:&Controller,id:&str,prior:&CharterSession,target:&CharterSourceRef)->Reply {exchange(c,Operation::MutateCharter{operation_id:id.into(),mutation:CharterMutation::SetPending{session_id:prior.session_id.clone(),expected_revision:prior.revision,target:target.clone()}}).await}
async fn setup(c:&Controller,label:&str)->(Task,Attempt,Caller) {
    let t=submit(c,&format!("{label}-submit"),input(label)).await;let w=worker(&format!("{label}-worker"),"one",1,0);register(c,&w).await;let a=run(&commands(c,&w,vec![]).await);
    task(report(c,&a,&format!("{label}-started"),Observation::Started{thread_id:None,turn_id:None}).await);
    let caller=Caller{task_id:t.id.clone(),attempt_id:a.id.clone(),generation:a.generation,worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone()};(t,a,caller)
}
async fn reserve(c:&Controller,a:&Attempt,index:u32) {task(report(c,a,&ids::reservation_id(&a.id,index),Observation::Usage{native_diagnostic_reserved: true,model_requests:index,input_tokens:None,output_tokens:None}).await);}
async fn begin(c:&Controller,a:&Attempt,who:&Caller,index:u32,id:&str)->CharterBoundary {
    reserve(c,a,index).await;let s=session(c,&a.session_id).await;
    match outcome(&observe(c,who,id,CharterObservation::BeginTurn{session_id:a.session_id.clone(),expected_revision:s.session.revision,request_index:index}).await) {CharterOutcome::Boundary(v)=>v,x=>panic!("boundary {x:?}")}
}
fn reply(declaration:Option<serde_json::Value>,calls:&[&str])->CharterReplyObservation {CharterReplyObservation{output_reference:None,output:Some(serde_json::json!([{"id":"actual-item","type":"message","content":[{"type":"output_text","text":"original controlled output"}]}])),response_id:Some("actual-controlled-response".into()),output_item_ids:vec!["actual-item".into()],tool_call_ids:calls.iter().map(|s|(*s).into()).collect(),declaration}}
async fn no_tools(c:&Controller,who:&Caller,b:&CharterBoundary,label:&str)->CharterTurn {
    outcome(&observe(c,who,&format!("{label}-reply"),CharterObservation::Reply{turn_id:b.turn_id.clone(),reply:reply(None,&[])}).await);
    match outcome(&observe(c,who,&format!("{label}-close"),CharterObservation::CompleteTurn{turn_id:b.turn_id.clone(),results:vec![]}).await) {CharterOutcome::Turn(v)=>v,x=>panic!("closed {x:?}")}
}
async fn other_source(c:&Controller,label:&str)->CharterSourceRef {let t=submit(c,&format!("{label}-submit"),input(label)).await;CharterSourceRef{definition_id:t.input.definition.id,definition_revision:t.input.definition.revision}}
#[tokio::test]
//= spec/impl/v2/implementation.md#a-revision-identifies-authoritative-source
//= type=test
//# A charter revision reference MUST reuse an existing source identity when that identity resolves the complete authoritative charter source.
async fn charter_initial_source_reuse_pending_cas_and_accepted_input_are_distinct() {
    let c=Controller::open_with_config(path(),charter_config()).unwrap();let (t,_,_)=setup(&c,"initial").await;let old=get(&c,&t.id).await.input;
    let first=session(&c,&t.session_id).await;assert_eq!(first.session.revision,1);assert_eq!(first.charter.source,source("initial"));assert_eq!(first.session.initial,first.session.effective);
    let target=other_source(&c,"next").await;let committed=set(&c,"set-next",&first.session,&target).await;assert!(committed.commit.is_some());
    assert_eq!(set(&c,"set-next",&first.session,&target).await.outcome,committed.outcome);
    let pending=session(&c,&t.session_id).await;assert_eq!(pending.session.pending,Some(target.clone()));assert_eq!(pending.session.effective,first.session.effective);
    assert_eq!(code(set(&c,"stale-set",&first.session,&target).await.outcome),"stale_revision");assert_eq!(session(&c,&t.session_id).await,pending);assert_eq!(get(&c,&t.id).await.input,old);
    let mut alias=input("alias");alias.definition.charter=Some(DefinitionCharter::Reference{source:target.clone()});let alias=submit(&c,"alias-submit",alias).await;
    assert_eq!(session(&c,&alias.session_id).await.charter.reference,target);assert_eq!(session(&c,&alias.session_id).await.session.initial,target);
}
// Raw metadata, structural findings and original reply/receipt association
// survive exact replay and controller reopen.
// Native controller observation, replay and reopen only; this case does not
// qualify supplied outer-turn result ingestion or a Codex provider exchange.
//= spec/impl/v2/implementation.md#every-turn-reply-carries-charter-references
//= type=test
//# A record that cites a native turn reply or supplied outer-turn result for audit inspection MUST preserve the cited reply's or result's charter-reference metadata association.
// A mismatched declaration is retained with audit findings and can close
// normally; reference validity does not act as a behavioral-compliance gate.
//= spec/impl/v2/implementation.md#reference-checks-produce-audit-information
//= type=test
//# A charter reference check MUST NOT be treated as proof of behavioral compliance.
#[tokio::test]
async fn charter_every_reply_keeps_raw_passive_audit_and_exact_replay_across_reopen() {
    let p=path();let cfg=charter_config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();let (t,a,who)=setup(&c,"audit").await;
    let b=begin(&c,&a,&who,1,"audit-turn").await;let bad=reply(Some(json!({"revision":{"definition_id":"wrong","definition_revision":"other"},"clauses":[]})),&[]);
    let operation=Operation::ObserveCharter{operation_id:"audit-reply".into(),caller:who.clone(),observation:CharterObservation::Reply{turn_id:b.turn_id.clone(),reply:bad.clone()}};
    let original=exchange(&c,operation.clone()).await;let CharterOutcome::Reply(saved)=outcome(&original) else{panic!()};
    assert_eq!(saved.observation,bad);assert!(saved.audit.findings.contains(&CharterAuditFinding::RevisionMismatch));assert!(saved.audit.findings.contains(&CharterAuditFinding::EmptyApplicabilityUnspecified));
    let close=observe(&c,&who,"audit-close",CharterObservation::CompleteTurn{turn_id:b.turn_id.clone(),results:vec![]}).await;outcome(&close);
    let retained=turn(&c,&b.turn_id).await;let input=get(&c,&t.id).await.input;let binding=session(&c,&t.session_id).await;
    drop(c);controller_owner_released(&p).await;let c=Controller::open_with_config(&p,cfg).unwrap();
    let replay=exchange(&c,operation).await;assert_eq!(replay.outcome,original.outcome);assert_eq!(replay.commit,original.commit);assert_eq!(turn(&c,&b.turn_id).await,retained);assert_eq!(get(&c,&t.id).await.input,input);assert_eq!(session(&c,&t.session_id).await,binding);
}
// Missing or reordered original tool-result receipts prevent adoption;
// the complete ordered closure permits the next native boundary.
//= spec/impl/v2/implementation.md#charter-changes-apply-at-safe-boundaries
//= type=test
//# A charter change MUST NOT apply between a tool call and the delivery of its result.
#[tokio::test]
async fn charter_pending_waits_for_each_original_tool_result_and_adopts_latest_once() {
    let c=Controller::open_with_config(path(),charter_config()).unwrap();let (t,a,who)=setup(&c,"tool-boundary").await;let b=begin(&c,&a,&who,1,"tool-turn").await;
    outcome(&observe(&c,&who,"tool-reply",CharterObservation::Reply{turn_id:b.turn_id.clone(),reply:reply(None,&["provider-a","provider-b"])}).await);
    let target=other_source(&c,"pending-a").await;let old=session(&c,&t.session_id).await;outcome(&set(&c,"pending-a-set",&old.session,&target).await);
    let latest=other_source(&c,"pending-b").await;let old=session(&c,&t.session_id).await;outcome(&set(&c,"pending-b-set",&old.session,&latest).await);
    reserve(&c,&a,2).await;let pending=session(&c,&t.session_id).await;
    assert_eq!(code(observe(&c,&who,"too-early",CharterObservation::BeginTurn{session_id:t.session_id.clone(),expected_revision:pending.session.revision,request_index:2}).await.outcome),"reconciliation_required");
    assert_eq!(session(&c,&t.session_id).await,pending);
    let mut results=vec![];
    for (ordinal,logical) in [(1,"provider-a"),(2,"provider-b")] {
        let id=ids::call_operation_id(&a.id,1,ordinal);
        let invocation=borg_runtime::mcp::Invocation{operation_id:id.clone(),logical_call_id:logical.into(),caller:who.clone(),tool:"collab_memory_get".into(),arguments:json!({"key":"absent"})};
        let original=exchange(&c,invocation.operation()).await;assert!(original.commit.is_some());assert!(matches!(original.outcome,Outcome::Ok{..}|Outcome::Refused{..}));
        results.push(CharterResultReceipt::Collaboration{call_id:logical.into(),operation_id:id});
    }
    assert_eq!(code(observe(&c,&who,"missing-result",CharterObservation::CompleteTurn{turn_id:b.turn_id.clone(),results:results[..1].to_vec()}).await.outcome),"reconciliation_required");
    let mut reversed=results.clone();reversed.reverse();assert_eq!(code(observe(&c,&who,"reversed-results",CharterObservation::CompleteTurn{turn_id:b.turn_id.clone(),results:reversed}).await.outcome),"reconciliation_required");
    outcome(&observe(&c,&who,"tools-complete",CharterObservation::CompleteTurn{turn_id:b.turn_id,results}).await);
    let next=begin(&c,&a,&who,2,"adopted-turn").await;assert_eq!(next.effective.reference,latest);let adopted=session(&c,&t.session_id).await;assert!(adopted.session.pending.is_none());assert_eq!(adopted.session.compaction_due,Some(latest));
}
#[tokio::test]
async fn charter_compaction_debounces_adopts_latest_and_preserves_newer_inflight_update() {
    let c=Controller::open_with_config(path(),charter_config()).unwrap();let (t,a,who)=setup(&c,"compact").await;
    let b=begin(&c,&a,&who,1,"compact-first").await;no_tools(&c,&who,&b,"compact-first").await;
    let one=other_source(&c,"compact-one").await;let s=session(&c,&t.session_id).await;outcome(&set(&c,"compact-set-one",&s.session,&one).await);
    let b=begin(&c,&a,&who,2,"compact-changed").await;assert_eq!(b.effective.reference,one);no_tools(&c,&who,&b,"compact-changed").await;
    let two=other_source(&c,"compact-two").await;let s=session(&c,&t.session_id).await;outcome(&set(&c,"compact-set-two",&s.session,&two).await);
    reserve(&c,&a,3).await;let s=session(&c,&t.session_id).await;
    let begin_op=Operation::ObserveCharter{operation_id:"compact-original".into(),caller:who.clone(),observation:CharterObservation::BeginCompaction{session_id:t.session_id.clone(),expected_revision:s.session.revision,request_index:3}};
    let committed=exchange(&c,begin_op.clone()).await;let CharterOutcome::Compaction(run)=outcome(&committed) else{panic!()};assert_eq!(run.effective.reference,two);
    assert_eq!(exchange(&c,begin_op).await.commit,committed.commit);assert!(run.reply.is_none());
    let three=other_source(&c,"compact-three").await;let s=session(&c,&t.session_id).await;outcome(&set(&c,"compact-set-three",&s.session,&three).await);
    let s=session(&c,&t.session_id).await;assert_eq!(code(observe(&c,&who,"compact-overlap",CharterObservation::BeginCompaction{session_id:t.session_id.clone(),expected_revision:s.session.revision,request_index:4}).await.outcome),"reconciliation_required");
    let result=observe(&c,&who,"compact-result",CharterObservation::CompactionReply{run_id:run.run_id.clone(),reply:reply(None,&[]),summary:Some("Actual controlled summary under revision two".into())}).await;
    let CharterOutcome::Compaction(done)=outcome(&result) else{panic!()};assert_eq!(done.disposition,Some(CompactionDisposition::ObsoleteSourceSummary));assert_eq!(done.effective.reference,two);
    let s=session(&c,&t.session_id).await;assert_eq!(s.session.pending,Some(three.clone()));assert_eq!(s.session.compaction_due,Some(three.clone()));assert!(s.session.compaction_in_flight.is_none());
    let next=begin(&c,&a,&who,4,"compact-newer-turn").await;assert_eq!(next.effective.reference,three);
}
#[tokio::test]
async fn charter_late_reply_audit_after_cancel_does_not_regrant_a_boundary() {
    let c=Controller::open_with_config(path(),charter_config()).unwrap();let (t,a,who)=setup(&c,"cancelled").await;let b=begin(&c,&a,&who,1,"cancelled-turn").await;
    // Reserve while current so the later refusal isolates execution currentness.
    reserve(&c,&a,2).await;
    task(call(&c,Operation::CancelTask{operation_id:"cancel-charter".into(),task_id:t.id.clone(),expected_revision:get(&c,&t.id).await.revision}).await);
    let after=get(&c,&t.id).await;let binding=session(&c,&t.session_id).await;
    let v=observe(&c,&who,"late-reply",CharterObservation::Reply{turn_id:b.turn_id.clone(),reply:reply(None,&[])}).await;outcome(&v);
    outcome(&observe(&c,&who,"late-close",CharterObservation::CompleteTurn{turn_id:b.turn_id,results:vec![]}).await);
    assert_eq!(get(&c,&t.id).await,after);assert_eq!(session(&c,&t.session_id).await,binding);
    assert_eq!(code(observe(&c,&who,"late-new-boundary",CharterObservation::BeginTurn{session_id:t.session_id.clone(),expected_revision:binding.session.revision,request_index:2}).await.outcome),"invalid_transition");
    assert_eq!(session(&c,&t.session_id).await,binding);
    assert_eq!(code(call(&c,Operation::QueryCharter{query:CharterQuery::Turn{turn_id:"late-new-boundary".into()}}).await),"not_found");
}
#[tokio::test]
async fn charter_unreserved_boundary_and_foreign_reply_have_no_effect() {
    let c=Controller::open_with_config(path(),charter_config()).unwrap();let (t,a,who)=setup(&c,"unreserved").await;let before=session(&c,&t.session_id).await;
    assert_eq!(code(observe(&c,&who,"unreserved-turn",CharterObservation::BeginTurn{session_id:t.session_id.clone(),expected_revision:before.session.revision,request_index:1}).await.outcome),"reconciliation_required");assert_eq!(session(&c,&t.session_id).await,before);
    let b=begin(&c,&a,&who,1,"reserved-turn").await;let mut foreign=who.clone();foreign.incarnation="other".into();
    assert!(matches!(observe(&c,&foreign,"foreign-reply",CharterObservation::Reply{turn_id:b.turn_id.clone(),reply:reply(None,&[])}).await.outcome,Outcome::Refused{..}));assert!(turn(&c,&b.turn_id).await.reply.is_none());
    let mut large=reply(None,&[]);large.declaration=Some(json!("x".repeat(8192)));
    assert_eq!(code(observe(&c,&who,"large-reply",CharterObservation::Reply{turn_id:b.turn_id.clone(),reply:large}).await.outcome),"input_bound");assert!(turn(&c,&b.turn_id).await.reply.is_none());
}
#[cfg(debug_assertions)]
#[tokio::test]
async fn charter_commit_failure_preserves_no_pending_transition_and_same_original_retry() {
    let p=path();let mut cfg=charter_config();let hook=Arc::new(std::sync::atomic::AtomicBool::new(false));cfg.qualification_fail_next_commit=Some(hook.clone());
    let c=Controller::open_with_config(&p,cfg.clone()).unwrap();let (t,_,_)=setup(&c,"rollback").await;let target=other_source(&c,"rollback-next").await;let before=session(&c,&t.session_id).await;
    hook.store(true,Ordering::SeqCst);let failed=set(&c,"rollback-set",&before.session,&target).await;assert!(!hook.load(Ordering::SeqCst));assert_eq!(code(failed.outcome),"store_unavailable");assert!(failed.commit.is_none());assert_eq!(session(&c,&t.session_id).await,before);
    assert_eq!(code(call(&c,Operation::GetOperation{operation_id:"rollback-set".into()}).await),"not_found");drop(c);controller_owner_released(&p).await;
    let c=Controller::open_with_config(&p,cfg).unwrap();assert_eq!(session(&c,&t.session_id).await,before);let committed=set(&c,"rollback-set",&before.session,&target).await;outcome(&committed);assert_eq!(set(&c,"rollback-set",&before.session,&target).await.commit,committed.commit);
}
fn stored_bytes(p:&std::path::Path)->Vec<(PathBuf,Option<Vec<u8>>)> {
    [p.to_path_buf(),controller_sidecar(p,"-wal"),controller_sidecar(p,"-shm")].into_iter().map(|p|{let bytes=std::fs::read(&p).ok();(p,bytes)}).collect()
}
#[tokio::test]
async fn charter_canonical_audit_or_version_rollback_and_oversize_indexes_refuse_before_writes() {
    for defect in 0..4 {
        let p=path();let cfg=charter_config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();let (t,a,who)=setup(&c,"stored").await;
        let boundary=begin(&c,&a,&who,1,"stored-turn").await;let declaration=DeclaredCharterReferences{revision:boundary.effective.reference.clone(),clauses:boundary.effective.source.clauses.clone()};
        outcome(&observe(&c,&who,"stored-reply",CharterObservation::Reply{turn_id:boundary.turn_id.clone(),reply:reply(Some(serde_json::to_value(declaration).unwrap()),&[])}).await);
        outcome(&observe(&c,&who,"stored-close",CharterObservation::CompleteTurn{turn_id:boundary.turn_id.clone(),results:vec![]}).await);
        let target=other_source(&c,"stored-next").await;let s=session(&c,&t.session_id).await;outcome(&set(&c,"stored-pending",&s.session,&target).await);
        let exact=turn(&c,&boundary.turn_id).await;let binding=session(&c,&t.session_id).await;drop(c);controller_owner_released(&p).await;
        let c=Controller::open_with_config(&p,cfg.clone()).unwrap();assert_eq!(turn(&c,&boundary.turn_id).await,exact);assert_eq!(session(&c,&t.session_id).await,binding);drop(c);controller_owner_released(&p).await;
        let sql=rusqlite::Connection::open(&p).unwrap();match defect {
            0=>{ // Still canonical, still recomputed audit, but no longer original observation.
                let raw:String=sql.query_row("SELECT body FROM charter_replies WHERE id='stored-reply'",[],|r|r.get(0)).unwrap();let mut v:CharterReplyRecord=serde_json::from_str(&raw).unwrap();v.observation.declaration=None;v.audit.findings=vec![CharterAuditFinding::MissingMetadata];
                sql.execute("UPDATE charter_replies SET body=?1 WHERE id='stored-reply'",[serde_json::to_string(&v).unwrap()]).unwrap();
            }
            1=>{ // Head and latest immutable row agree; exact original CAS receipt must reject rollback.
                let mut v=binding.session.clone();v.pending=None;let raw=serde_json::to_string(&v).unwrap();
                sql.execute("UPDATE charter_sessions SET body=?1 WHERE id=?2",rusqlite::params![raw,t.session_id]).unwrap();sql.execute("UPDATE charter_session_versions SET body=?1 WHERE id=?2 AND revision=?3",rusqlite::params![raw,t.session_id,i64::try_from(v.revision).unwrap()]).unwrap();
            }
            2=>{sql.execute("UPDATE charter_requests SET kind=?1 WHERE id='stored-turn'",["x".repeat(cfg.limits.transport.identity_bytes+1)]).unwrap();}
            _=>{sql.execute("DELETE FROM charter_sessions WHERE id=?1",[&t.session_id]).unwrap();sql.execute("DELETE FROM charter_session_versions WHERE id=?1",[&t.session_id]).unwrap();}
        }drop(sql);let bytes=stored_bytes(&p);assert!(Controller::open_with_config(&p,cfg).is_err(),"defect {defect}");assert_eq!(stored_bytes(&p),bytes,"startup repaired defect {defect}");
    }
}
#[tokio::test]
async fn charter_enabled_admission_requires_source_and_preserves_historical_no_charter_task() {
    let p=path();let c=Controller::open_with_config(&p,config()).unwrap();let accepted=submit(&c,"historical-no-charter",native("historical")).await;let old=get(&c,&accepted.id).await;drop(c);controller_owner_released(&p).await;
    let c=Controller::open_with_config(&p,charter_config()).unwrap();assert_eq!(get(&c,&accepted.id).await,old);
    assert!(matches!(query(&c,CharterQuery::Session{session_id:accepted.session_id}).await,CharterQueryResult::Session(None)));
    assert_eq!(code(call(&c,Operation::SubmitTask{operation_id:"new-unchartered".into(),input:native("new")}).await),"charter_required");
    // Reusing an old retained definition is not a bypass for a new acceptance.
    let mut reused=old.input.clone();reused.resolved_limits=None;reused.limits.deadline_unix_ms=future();assert_eq!(code(call(&c,Operation::SubmitTask{operation_id:"old-definition-new-task".into(),input:reused}).await),"charter_required");
    let forbidden=Operation::UpdateDefinition{operation_id:"old-definition-limit-clone".into(),update:DefinitionUpdate{agent_id:old.assigned_agent_id.clone(),expected_revision:old.input.definition.revision.clone(),expected_global_revision:c.limits().revision.clone(),expected_policy_revision:c.limits().definition_updates.as_ref().unwrap().revision.clone(),changes:std::collections::BTreeMap::from([(DefinitionLimitField::NativeMaxOutputTokens,1024)])}};
    assert_eq!(code(call(&c,forbidden).await),"charter_required");
    assert_eq!(get(&c,&old.id).await,old);
}
#[tokio::test]
async fn charter_coding_boundary_is_explicitly_unsupported_without_state_or_request() {
    let c=Controller::open_with_config(path(),charter_config()).unwrap();let mut input=coding("coding-charter");input.definition.charter=Some(DefinitionCharter::Inline{source:source("coding identity")});
    let t=submit(&c,"coding-charter-task",input).await;let w=worker("coding-charter-worker","one",0,1);register(&c,&w).await;let a=run(&commands(&c,&w,vec![]).await);
    task(report(&c,&a,"coding-charter-start",Observation::Started{thread_id:Some("controlled-thread".into()),turn_id:Some("controlled-outer-turn".into())}).await);
    let who=Caller{task_id:t.id.clone(),attempt_id:a.id.clone(),generation:a.generation,worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone()};let s=session(&c,&t.session_id).await;
    assert_eq!(code(observe(&c,&who,"unsupported-coding-boundary",CharterObservation::BeginTurn{session_id:t.session_id.clone(),expected_revision:s.session.revision,request_index:1}).await.outcome),"unsupported_charter_boundary");
    assert_eq!(session(&c,&t.session_id).await,s);assert!(matches!(query(&c,CharterQuery::Turns{session_id:t.session_id,after:None,limit:4}).await,CharterQueryResult::Turns(page) if page.items.is_empty()&&page.next_cursor.is_none()));
}
#[tokio::test]
async fn charter_delegation_creation_cannot_close_until_exact_child_result_is_delivered() {
    let c=Controller::open_with_config(path(),charter_config()).unwrap();let mut parent=input("delegating-charter");let mut child=coding("chartered-child");child.definition.charter=Some(DefinitionCharter::Inline{source:source("child")});child.limits.model_requests=2;child.limits.result_bytes=2048;child.limits.deadline_unix_ms=parent.limits.deadline_unix_ms;
    parent.native_workflow=Some(Box::new(NativeWorkflow::CodingThenContinue{child:CodingTaskTemplate{ instruction_source: None, pull_request: None, instruction_prefix:"fixed".into(),definition:child.definition,repository:child.repository.unwrap(),acceptance_commands:child.acceptance_commands,dependencies:vec![],limits:child.limits}}));
    let t=submit(&c,"delegating-charter-task",parent).await;let w=worker("delegating-charter-worker","one",1,0);register(&c,&w).await;let a=run(&commands(&c,&w,vec![]).await);
    task(report(&c,&a,"delegating-charter-start",Observation::Started{thread_id:None,turn_id:None}).await);
    let who=Caller{task_id:t.id.clone(),attempt_id:a.id.clone(),generation:a.generation,worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone()};let b=begin(&c,&a,&who,1,"delegating-turn").await;
    outcome(&observe(&c,&who,"delegating-reply",CharterObservation::Reply{turn_id:b.turn_id.clone(),reply:reply(None,&["actual-delegate-call"])}).await);
    let operation_id=ids::call_operation_id(&a.id,1,1);let invocation=borg_runtime::mcp::Invocation{operation_id:operation_id.clone(),logical_call_id:"actual-delegate-call".into(),caller:who.clone(),tool:"collab_task_delegate".into(),arguments:json!({"instruction":"make the fixed change"})};
    let delegated=exchange(&c,invocation.operation()).await;let child=task(delegated.outcome.clone());assert!(delegated.commit.is_some());
    let proof=vec![CharterResultReceipt::Collaboration{call_id:"actual-delegate-call".into(),operation_id}];
    assert_eq!(code(observe(&c,&who,"creation-is-not-delivery",CharterObservation::CompleteTurn{turn_id:b.turn_id.clone(),results:proof.clone()}).await.outcome),"reconciliation_required");assert!(turn(&c,&b.turn_id).await.completed_by.is_none());
    let pending=other_source(&c,"delegating-next").await;let original=session(&c,&t.session_id).await;outcome(&set(&c,"delegating-pending",&original.session,&pending).await);
    task(call(&c,Operation::CancelTask{operation_id:"cancel-controlled-child".into(),task_id:child.id.clone(),expected_revision:child.revision}).await);
    task(report(&c,&a,"delegating-stop-ack",Observation::ProcessTreeStopped).await);
    let delivery=commands(&c,&w,vec![format!("run-{}",a.id),format!("stop-{}",a.id)]).await;let next=run(&delivery);
    assert!(delivery.iter().any(|command|matches!(command,WorkerCommand::RunAttempt{attempt,continuation:Some(result),..}if attempt.id==next.id&&result.task_id==child.id&&result.status==TaskStatus::Cancelled)));
    task(report(&c,&next,"delegating-continuation-start",Observation::Started{thread_id:None,turn_id:None}).await);
    let resumed=Caller{task_id:t.id.clone(),attempt_id:next.id.clone(),generation:next.generation,worker_id:next.worker_id.clone(),incarnation:next.worker_incarnation.clone()};
    let close=observe(&c,&resumed,"delivered-child-close",CharterObservation::CompleteTurn{turn_id:b.turn_id.clone(),results:proof}).await;outcome(&close);assert_eq!(turn(&c,&b.turn_id).await.completed_by.as_deref(),Some("delivered-child-close"));
    let next_boundary=begin(&c,&next,&resumed,1,"after-child-boundary").await;assert_eq!(next_boundary.effective.reference,pending);assert_eq!(get(&c,&t.id).await.input,t.input);
}
#[tokio::test]
async fn charter_compaction_unaccepted_tool_output_remains_original_audit_without_dispatch() {
    let p=path();let cfg=charter_config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();let (t,a,who)=setup(&c,"invalid-summary").await;
    let first=begin(&c,&a,&who,1,"invalid-summary-first").await;no_tools(&c,&who,&first,"invalid-summary-first").await;
    let target=other_source(&c,"invalid-summary-target").await;let s=session(&c,&t.session_id).await;outcome(&set(&c,"invalid-summary-set",&s.session,&target).await);
    let changed=begin(&c,&a,&who,2,"invalid-summary-changed").await;no_tools(&c,&who,&changed,"invalid-summary-changed").await;
    reserve(&c,&a,3).await;let s=session(&c,&t.session_id).await;outcome(&observe(&c,&who,"invalid-summary-run",CharterObservation::BeginCompaction{session_id:t.session_id.clone(),expected_revision:s.session.revision,request_index:3}).await);
    let mut actual=reply(Some(json!("malformed declaration")),&["unexpected-provider-call"]);actual.output=Some(json!([{"id":"actual-item","type":"function_call","call_id":"unexpected-provider-call","name":"collab_memory_set","arguments":"{\"key\":\"forbidden\",\"value\":1}"}]));
    let operation=Operation::ObserveCharter{operation_id:"invalid-summary-observed".into(),caller:who,observation:CharterObservation::CompactionReply{run_id:"invalid-summary-run".into(),reply:actual.clone(),summary:None}};
    let result=exchange(&c,operation.clone()).await;let CharterOutcome::Compaction(run)=outcome(&result) else{panic!()};assert_eq!(run.disposition,Some(CompactionDisposition::NoAcceptedSummary));assert_eq!(run.reply.as_ref().unwrap().observation,actual);assert_eq!(run.summary,None);
    assert!(matches!(call(&c,Operation::GetMemory{agent_id:t.assigned_agent_id.clone(),key:"forbidden".into()}).await,Outcome::Ok{data:Data::Memory(None)}));let s=session(&c,&t.session_id).await;assert_eq!(s.session.compaction_due,Some(target));assert!(s.session.compaction_in_flight.is_none());
    drop(c);controller_owner_released(&p).await;let c=Controller::open_with_config(&p,cfg).unwrap();assert_eq!(exchange(&c,operation).await.commit,result.commit);assert_eq!(query(&c,CharterQuery::Compaction{run_id:"invalid-summary-run".into()}).await,CharterQueryResult::Compaction(run));
}
#[tokio::test]
//= spec/impl/v2/implementation.md#a-revision-identifies-authoritative-source
//= type=test
//# A charter revision reference MUST reuse an existing source identity when that identity resolves the complete authoritative charter source.
async fn charter_limit_only_successor_reuses_authoritative_source_and_keeps_old_session() {
    let c=Controller::open_with_config(path(),charter_config()).unwrap();let (t,_,_)=setup(&c,"limit-charter").await;let accepted=get(&c,&t.id).await.input;let original=session(&c,&t.session_id).await;
    let native=c.limits().resolve_definition(&t.input.definition).unwrap().native;
    let operation=Operation::UpdateDefinition{operation_id:"charter-limit-only".into(),update:DefinitionUpdate{agent_id:t.assigned_agent_id.clone(),expected_revision:t.input.definition.revision.clone(),expected_global_revision:c.limits().revision.clone(),expected_policy_revision:c.limits().definition_updates.as_ref().unwrap().revision.clone(),changes:std::collections::BTreeMap::from([(DefinitionLimitField::NativeMaxOutputTokens,native.max_output_tokens-1)])}};
    let result=exchange(&c,operation.clone()).await;let Outcome::Ok{data:Data::DefinitionUpdate(update)}=&result.outcome else{panic!("{result:?}")};
    assert_eq!(update.selected_definition.charter,Some(DefinitionCharter::Reference{source:original.charter.reference.clone()}));assert_eq!(session(&c,&t.session_id).await,original);assert_eq!(get(&c,&t.id).await.input,accepted);
    let mut new=input("limit-charter");new.definition=update.selected_definition.clone();let new=submit(&c,"charter-limit-new-task",new).await;assert_eq!(session(&c,&new.session_id).await.charter,original.charter);
    assert_eq!(exchange(&c,operation).await.commit,result.commit);assert_eq!(get(&c,&t.id).await.input,accepted);
}

// Controlled trusted host observations exercise canonical rejection receipts;
// these tests do not claim to execute a provider POST or retry/backoff loop.
// The body binding declares the controlled fixture bytes b"{}"; it is not a
// measured provider body. C owns the actual adapter body-equality discriminator.
async fn rejected_post(c:&Controller,a:&Attempt,index:u32)->(Operation,Reply) {
    rejected_post_kind(c,a,index,borg_runtime::native::RejectionKind::RateLimited).await
}
async fn rejected_post_kind(c:&Controller,a:&Attempt,index:u32,kind:borg_runtime::native::RejectionKind)->(Operation,Reply) {
    let accepted=get(c,&a.task_id).await;
    let operation=Operation::ObserveWorker{report:Report{report_id:ids::rejection_id(&a.id,index),worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone(),task_id:a.task_id.clone(),attempt_id:a.id.clone(),generation:a.generation,observation:Observation::NativeRequestRejected{request_index:index,request_binding:borg_runtime::native::ProviderRequestBinding{bytes:2,sha256:"44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a".into()},endpoint:"https://api.openai.com/v1/responses".into(),model:accepted.input.definition.required_inference().unwrap().model.clone(),rejection:borg_runtime::native::ProviderRejection{kind,retry_after:borg_runtime::native::RetryAfter::Absent},diagnostic:if kind==borg_runtime::native::RejectionKind::RateLimited{"controlled complete HTTP 429 rejection".into()}else{"controlled complete provider rejection".into()}}}};
    let reply=exchange(c,operation.clone()).await;assert!(reply.commit.is_some(),"{reply:?}");assert!(matches!(reply.outcome,Outcome::Ok{data:Data::Task(_)}),"{reply:?}");(operation,reply)
}
async fn begun_compaction(c:&Controller,label:&str)->(Task,Attempt,Caller,CharterCompaction) {
    let (t,a,who)=setup(c,label).await;
    let first=begin(c,&a,&who,1,&format!("{label}-first")).await;no_tools(c,&who,&first,&format!("{label}-first")).await;
    let target=other_source(c,&format!("{label}-target")).await;let s=session(c,&t.session_id).await;outcome(&set(c,&format!("{label}-set"),&s.session,&target).await);
    let changed=begin(c,&a,&who,2,&format!("{label}-changed")).await;no_tools(c,&who,&changed,&format!("{label}-changed")).await;
    reserve(c,&a,3).await;let s=session(c,&t.session_id).await;
    let result=observe(c,&who,&format!("{label}-run"),CharterObservation::BeginCompaction{session_id:t.session_id.clone(),expected_revision:s.session.revision,request_index:3}).await;
    let CharterOutcome::Compaction(run)=outcome(&result) else{panic!()};(t,a,who,run)
}
#[tokio::test]
async fn charter_confirmed_rejection_advances_without_reply_and_never_reuses_index() {
    let p=path();let cfg=charter_config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();let (t,a,who)=setup(&c,"reject-turn").await;
    let b=begin(&c,&a,&who,1,"reject-turn-original").await;let unchanged=session(&c,&t.session_id).await;
    assert_eq!(code(observe(&c,&who,"reject-turn-unknown",CharterObservation::BeginTurn{session_id:t.session_id.clone(),expected_revision:unchanged.session.revision,request_index:2}).await.outcome),"reconciliation_required");
    assert_eq!(session(&c,&t.session_id).await,unchanged);assert!(turn(&c,&b.turn_id).await.reply.is_none());
    let (original,proof)=rejected_post(&c,&a,1).await;
    assert_eq!(exchange(&c,original.clone()).await.commit,proof.commit);
    assert_eq!(code(observe(&c,&who,"reject-turn-fake-reply",CharterObservation::Reply{turn_id:b.turn_id.clone(),reply:reply(None,&[])}).await.outcome),"operation_conflict");
    assert_eq!(code(observe(&c,&who,"reject-turn-fake-complete",CharterObservation::CompleteTurn{turn_id:b.turn_id.clone(),results:vec![]}).await.outcome),"reconciliation_required");
    let target=other_source(&c,"reject-turn-next").await;let s=session(&c,&t.session_id).await;outcome(&set(&c,"reject-turn-pending",&s.session,&target).await);
    let before=session(&c,&t.session_id).await;
    assert_eq!(code(observe(&c,&who,"reject-turn-reuse",CharterObservation::BeginTurn{session_id:t.session_id.clone(),expected_revision:before.session.revision,request_index:1}).await.outcome),"operation_conflict");assert_eq!(session(&c,&t.session_id).await,before);
    let next=begin(&c,&a,&who,2,"reject-turn-next-boundary").await;assert_eq!(next.request_index,2);assert_eq!(next.effective.reference,target);
    let rejected=turn(&c,&b.turn_id).await;assert!(rejected.reply.is_none()&&rejected.completed_by.is_none()&&rejected.delivered_results.is_empty());assert_eq!(get(&c,&t.id).await.input,t.input);
    drop(c);controller_owner_released(&p).await;let c=Controller::open_with_config(&p,cfg).unwrap();assert_eq!(turn(&c,&b.turn_id).await,rejected);assert_eq!(exchange(&c,original).await.commit,proof.commit);
    // A report already committed before a charter Begin cannot be relabeled as
    // a newly begun POST. The accepted request index stays consumed.
    let c=Controller::open_with_config(path(),charter_config()).unwrap();let (t,a,who)=setup(&c,"reject-before-begin").await;reserve(&c,&a,1).await;rejected_post(&c,&a,1).await;let s=session(&c,&t.session_id).await;
    assert_eq!(code(observe(&c,&who,"late-begin",CharterObservation::BeginTurn{session_id:t.session_id.clone(),expected_revision:s.session.revision,request_index:1}).await.outcome),"operation_conflict");assert_eq!(session(&c,&t.session_id).await,s);
}
#[tokio::test]
async fn charter_rejected_compaction_clears_exact_flight_restores_latest_due_and_replays() {
    let p=path();let cfg=charter_config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();let (t,a,who,run)=begun_compaction(&c,"reject-summary").await;
    let pending=other_source(&c,"reject-summary-newer").await;let s=session(&c,&t.session_id).await;outcome(&set(&c,"reject-summary-pending",&s.session,&pending).await);
    let before=session(&c,&t.session_id).await;let report_id=ids::rejection_id(&a.id,3);
    assert_eq!(code(observe(&c,&who,"reject-summary-unknown",CharterObservation::CompactionRejected{run_id:run.run_id.clone(),report_id:report_id.clone()}).await.outcome),"reconciliation_required");assert_eq!(session(&c,&t.session_id).await,before);
    let (original,rejection)=rejected_post(&c,&a,3).await;
    assert_eq!(code(observe(&c,&who,"reject-summary-wrong-proof",CharterObservation::CompactionRejected{run_id:run.run_id.clone(),report_id:ids::rejection_id(&a.id,2)}).await.outcome),"source_mismatch");
    let mut foreign=who.clone();foreign.incarnation="foreign".into();assert!(matches!(observe(&c,&foreign,"reject-summary-foreign",CharterObservation::CompactionRejected{run_id:run.run_id.clone(),report_id:report_id.clone()}).await.outcome,Outcome::Refused{..}));assert_eq!(session(&c,&t.session_id).await,before);
    let operation=Operation::ObserveCharter{operation_id:"reject-summary-reconciled".into(),caller:who.clone(),observation:CharterObservation::CompactionRejected{run_id:run.run_id.clone(),report_id}};
    let reconciled=exchange(&c,operation.clone()).await;assert!(reconciled.commit.as_ref().unwrap().controller_revision>rejection.commit.as_ref().unwrap().controller_revision);assert_eq!(outcome(&reconciled),CharterOutcome::Compaction(run.clone()));
    let ready=session(&c,&t.session_id).await;assert!(ready.session.compaction_in_flight.is_none());assert_eq!(ready.session.compaction_due,Some(pending.clone()));assert_eq!(ready.session.pending,Some(pending.clone()));assert_eq!(ready.session.effective,before.session.effective);
    assert_eq!(exchange(&c,operation.clone()).await.commit,reconciled.commit);assert_eq!(session(&c,&t.session_id).await,ready);
    assert_eq!(code(observe(&c,&who,"reject-summary-late-reply",CharterObservation::CompactionReply{run_id:run.run_id.clone(),reply:reply(None,&[]),summary:Some("invented late summary".into())}).await.outcome),"operation_conflict");
    assert_eq!(code(observe(&c,&who,"reject-summary-reuse",CharterObservation::BeginCompaction{session_id:t.session_id.clone(),expected_revision:ready.session.revision,request_index:3}).await.outcome),"operation_conflict");assert_eq!(session(&c,&t.session_id).await,ready);
    reserve(&c,&a,4).await;let result=observe(&c,&who,"reject-summary-new-post",CharterObservation::BeginCompaction{session_id:t.session_id.clone(),expected_revision:ready.session.revision,request_index:4}).await;let CharterOutcome::Compaction(next)=outcome(&result) else{panic!()};assert_eq!(next.request_index,4);assert_ne!(next.run_id,run.run_id);assert_eq!(next.effective.reference,pending);assert_eq!(get(&c,&t.id).await.input,t.input);
    assert_eq!(query(&c,CharterQuery::Compaction{run_id:run.run_id.clone()}).await,CharterQueryResult::Compaction(run.clone()));let final_binding=session(&c,&t.session_id).await;
    drop(c);controller_owner_released(&p).await;let c=Controller::open_with_config(&p,cfg).unwrap();assert_eq!(session(&c,&t.session_id).await,final_binding);assert_eq!(exchange(&c,original).await.commit,rejection.commit);assert_eq!(exchange(&c,operation).await.commit,reconciled.commit);assert_eq!(query(&c,CharterQuery::Compaction{run_id:run.run_id.clone()}).await,CharterQueryResult::Compaction(run));
}
#[tokio::test]
async fn charter_late_compaction_rejection_retains_original_proof_without_execution_authority() {
    let c=Controller::open_with_config(path(),charter_config()).unwrap();let (t,a,who,run)=begun_compaction(&c,"reject-cancelled").await;
    // A confirmed rejection settles the latest charged POST before reserving
    // its successor. Both happen while current; only reconciliation is late.
    let (_,rejection)=rejected_post(&c,&a,3).await;reserve(&c,&a,4).await;
    task(call(&c,Operation::CancelTask{operation_id:"reject-cancelled-stop".into(),task_id:t.id.clone(),expected_revision:get(&c,&t.id).await.revision}).await);
    let stopped=get(&c,&t.id).await;
    let operation=Operation::ObserveCharter{operation_id:"reject-cancelled-reconciled".into(),caller:who.clone(),observation:CharterObservation::CompactionRejected{run_id:run.run_id.clone(),report_id:ids::rejection_id(&a.id,3)}};
    let retained=exchange(&c,operation.clone()).await;assert_eq!(outcome(&retained),CharterOutcome::Compaction(run));assert!(retained.commit.as_ref().unwrap().controller_revision>rejection.commit.as_ref().unwrap().controller_revision);
    let ready=session(&c,&t.session_id).await;assert!(ready.session.compaction_in_flight.is_none());assert!(ready.session.compaction_due.is_some());
    assert_eq!(code(observe(&c,&who,"reject-cancelled-new-request",CharterObservation::BeginCompaction{session_id:t.session_id.clone(),expected_revision:ready.session.revision,request_index:4}).await.outcome),"invalid_transition");
    assert_eq!(code(call(&c,Operation::QueryCharter{query:CharterQuery::Compaction{run_id:"reject-cancelled-new-request".into()}}).await),"not_found");
    let after=get(&c,&t.id).await;assert_eq!(after.status,stopped.status);assert_eq!(after.input,stopped.input);assert_eq!(after.attempt.as_ref().unwrap().phase,stopped.attempt.as_ref().unwrap().phase);assert_eq!(session(&c,&t.session_id).await,ready);assert_eq!(exchange(&c,operation).await.commit,retained.commit);
}
#[tokio::test]
async fn charter_rejected_compaction_canonical_result_tamper_refuses_reopen_before_writes() {
    let p=path();let cfg=charter_config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();let (t,a,who,run)=begun_compaction(&c,"reject-corrupt").await;
    rejected_post(&c,&a,3).await;outcome(&observe(&c,&who,"reject-corrupt-reconciled",CharterObservation::CompactionRejected{run_id:run.run_id.clone(),report_id:ids::rejection_id(&a.id,3)}).await);let binding=session(&c,&t.session_id).await;
    drop(c);controller_owner_released(&p).await;let c=Controller::open_with_config(&p,cfg.clone()).unwrap();assert_eq!(session(&c,&t.session_id).await,binding);assert_eq!(query(&c,CharterQuery::Compaction{run_id:run.run_id.clone()}).await,CharterQueryResult::Compaction(run.clone()));drop(c);controller_owner_released(&p).await;
    let sql=rusqlite::Connection::open(&p).unwrap();let body:String=sql.query_row("SELECT body FROM charter_compaction_results WHERE request_id=?1",[&run.run_id],|r|r.get(0)).unwrap();let mut altered:serde_json::Value=serde_json::from_str(&body).unwrap();altered["report_id"]=json!(ids::rejection_id(&a.id,2));
    sql.execute("UPDATE charter_compaction_results SET body=?1 WHERE request_id=?2",rusqlite::params![serde_json::to_string(&altered).unwrap(),run.run_id]).unwrap();drop(sql);let before=stored_bytes(&p);assert!(Controller::open_with_config(&p,cfg).is_err());assert_eq!(stored_bytes(&p),before);
}

#[tokio::test]
async fn charter_successful_receipts_require_deleted_reply_closure_result_and_version_membership() {
    for defect in 0..4 {
        let p=path();let cfg=charter_config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
        let (t,_,who,run)=begun_compaction(&c,"membership").await;
        let summary_operation=Operation::ObserveCharter{operation_id:"membership-summary".into(),caller:who.clone(),observation:CharterObservation::CompactionReply{run_id:run.run_id.clone(),reply:reply(None,&[]),summary:Some("Controlled retained summary".into())}};
        let result=exchange(&c,summary_operation.clone()).await;
        let CharterOutcome::Compaction(completed)=outcome(&result) else{panic!()};
        let target=other_source(&c,"membership-pending-source").await;let before_pending=session(&c,&t.session_id).await;
        let pending=set(&c,"membership-pending",&before_pending.session,&target).await;outcome(&pending);
        let binding=session(&c,&t.session_id).await;let original_turn=turn(&c,"membership-first").await;
        assert!(original_turn.reply.is_some()&&original_turn.completed_by.is_some());
        drop(c);controller_owner_released(&p).await;
        // First prove that all original records and receipts reopen unchanged.
        let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
        assert_eq!(turn(&c,"membership-first").await,original_turn);
        assert_eq!(query(&c,CharterQuery::Compaction{run_id:run.run_id.clone()}).await,CharterQueryResult::Compaction(completed));
        assert_eq!(session(&c,&t.session_id).await,binding);
        let replay=exchange(&c,summary_operation).await;assert_eq!(replay.commit,result.commit);assert_eq!(replay.outcome,result.outcome);
        drop(c);controller_owner_released(&p).await;
        let sql=rusqlite::Connection::open(&p).unwrap();
        let receipts=|sql:&rusqlite::Connection|->Vec<(String,String,String,i64)>{
            let mut q=sql.prepare("SELECT id,request,outcome,controller_revision FROM receipts ORDER BY id").unwrap();
            let rows=q.query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
            rows.collect::<Result<Vec<_>,_>>().unwrap()
        };
        let original_receipts=receipts(&sql);
        match defect {
            0=>{sql.execute("DELETE FROM charter_closures WHERE request_id='membership-first'",[]).unwrap();sql.execute("DELETE FROM charter_replies WHERE request_id='membership-first'",[]).unwrap();}
            1=>{sql.execute("DELETE FROM charter_closures WHERE request_id='membership-first'",[]).unwrap();}
            2=>{sql.execute("DELETE FROM charter_compaction_results WHERE request_id=?1",[&run.run_id]).unwrap();sql.execute("DELETE FROM charter_replies WHERE request_id=?1",[&run.run_id]).unwrap();}
            _=>{
                // The head agrees with the surviving latest version, but the
                // original successful SetPending receipt still requires its row.
                sql.execute("DELETE FROM charter_session_versions WHERE id=?1 AND revision=?2",rusqlite::params![t.session_id,i64::try_from(binding.session.revision).unwrap()]).unwrap();
                let raw:String=sql.query_row("SELECT body FROM charter_session_versions WHERE id=?1 AND revision=?2",rusqlite::params![t.session_id,i64::try_from(before_pending.session.revision).unwrap()],|r|r.get(0)).unwrap();
                sql.execute("UPDATE charter_sessions SET revision=?1,body=?2 WHERE id=?3",rusqlite::params![i64::try_from(before_pending.session.revision).unwrap(),raw,t.session_id]).unwrap();
            }
        }
        assert_eq!(receipts(&sql),original_receipts,"fixture changed original receipts");
        drop(sql);let bytes=stored_bytes(&p);
        assert!(Controller::open_with_config(&p,cfg).is_err(),"missing membership defect {defect}");
        assert_eq!(stored_bytes(&p),bytes,"startup repaired missing membership defect {defect}");
    }
}

#[tokio::test]
async fn charter_fatal_rejection_retains_history_but_never_admits_same_attempt_successor() {
    use borg_runtime::native::RejectionKind;
    for kind in [RejectionKind::InvalidRequest,RejectionKind::ContextLimit,RejectionKind::PolicyRefused,RejectionKind::QuotaExhausted,RejectionKind::AuthenticationFailed] {
        for compact in [false,true] {
            let c=Controller::open_with_config(path(),charter_config()).unwrap();
            let (t,a,who,request_id,index)=if compact {
                let (t,a,who,run)=begun_compaction(&c,"fatal-compact").await;
                (t,a,who,run.run_id,run.request_index)
            }else{
                let (t,a,who)=setup(&c,"fatal-turn").await;
                let b=begin(&c,&a,&who,1,"fatal-turn-original").await;
                (t,a,who,b.turn_id,b.request_index)
            };
            let (original,proof)=rejected_post_kind(&c,&a,index,kind).await;
            if compact {
                let reconciled=observe(&c,&who,"fatal-reconciled",CharterObservation::CompactionRejected{run_id:request_id.clone(),report_id:ids::rejection_id(&a.id,index)}).await;
                let CharterOutcome::Compaction(run)=outcome(&reconciled)else{panic!()};
                assert!(run.reply.is_none()&&run.summary.is_none()&&run.disposition.is_none());
                assert!(session(&c,&t.session_id).await.session.compaction_in_flight.is_none());
            }else{
                let retained=turn(&c,&request_id).await;
                assert!(retained.reply.is_none()&&retained.completed_by.is_none());
            }
            // Charge the successor while still current. Every normal admission
            // prerequisite is present; only the original fatal class denies it.
            reserve(&c,&a,index+1).await;
            let current=get(&c,&t.id).await;
            assert_eq!(current.status,TaskStatus::Active);assert_eq!(current.input,t.input);
            assert!(!current.attempt.as_ref().unwrap().stopped);
            let binding=session(&c,&t.session_id).await;
            let observation=if compact {CharterObservation::BeginCompaction{session_id:t.session_id.clone(),expected_revision:binding.session.revision,request_index:index+1}}
                else{CharterObservation::BeginTurn{session_id:t.session_id.clone(),expected_revision:binding.session.revision,request_index:index+1}};
            let denied=observe(&c,&who,"fatal-next",observation).await;
            assert!(matches!(denied.outcome,Outcome::Refused{ref code,ref message} if code=="reconciliation_required"&&message=="confirmed rejection does not permit another request in this attempt"),"{denied:?}");
            assert_eq!(session(&c,&t.session_id).await,binding);
            let query=if compact {CharterQuery::Compaction{run_id:"fatal-next".into()}}else{CharterQuery::Turn{turn_id:"fatal-next".into()}};
            assert_eq!(code(call(&c,Operation::QueryCharter{query}).await),"not_found");
            let replay=exchange(&c,original).await;assert_eq!(replay.commit,proof.commit);assert_eq!(replay.outcome,proof.outcome);
        }
    }
}

#[path = "charter_request_evidence.rs"]
mod request_evidence;

#[path = "charter_supplied.rs"]
mod supplied_context;

#[path = "charter_output_cap.rs"]
mod output_cap;

#[path="charter_output_retention.rs"]
mod output_retention;
