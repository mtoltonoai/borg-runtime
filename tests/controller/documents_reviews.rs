//! Join under tests/controller.rs to reuse actual controller/native acceptance fixtures.
//! These are real SQLite/operation tests; no provider, host or rollout action occurs.
use super::*;
use borg_runtime::collaboration::{documents::*,workflow::*};
use borg_runtime::collaboration::work::{WorkQuery,WorkQueryResult};

fn workflow_config()->ControllerConfig {
    let mut c=config();
    let limits=Arc::make_mut(&mut c.limits);
    limits.workflow=Some(borg_runtime::limits::WorkflowLimits {
        record_bytes:8192,text_bytes:2048,options:8,records:128,revisions:1024,
        page_items:4,page_bytes:16384,waits_per_task:8,graph_scan_nodes:128,
        events_per_mutation:16,event_bytes:4096,notice_bytes:4096,
        pending_delivery_bytes:16384,delivery_scan_rows:64,
    });
    c
}
fn doc(o:Outcome)->DocumentView {match o{Outcome::Ok{data:Data::DocumentMutation(DocumentMutationOutcome::Document{view})}=>view,x=>panic!("expected document mutation: {x:?}")}}
fn rev(o:Outcome)->ReviewView {match o{Outcome::Ok{data:Data::ReviewMutation(ReviewMutationOutcome::Review(view))}=>view,x=>panic!("expected review mutation: {x:?}")}}
async fn create(c:&Controller,id:&str,body:&str)->DocumentView {
    doc(call(c,Operation::MutateDocument{operation_id:id.into(),mutation:DocumentMutation::Create{content:DocumentContent{title:"Working note".into(),body:body.into()},supplied_content_id:None,responsible_owner:PrincipalRef::TrustedHostOperator}}).await)
}
async fn read(c:&Controller,id:&str,version:Option<u64>)->DocumentView {
    match call(c,Operation::QueryDocument{query:DocumentQuery::Get{document_id:id.into(),version}}).await{Outcome::Ok{data:Data::DocumentQuery(DocumentQueryResult::Document(v))}=>v,x=>panic!("expected document read: {x:?}")}
}
async fn get_review(c:&Controller,id:&str)->ReviewView {
    match call(c,Operation::QueryReview{query:ReviewQuery::Get{review_id:id.into()}}).await{Outcome::Ok{data:Data::ReviewQuery(ReviewQueryResult::Review(v))}=>v,x=>panic!("expected review read: {x:?}")}
}
fn append(id:&str,view:&DocumentView,body:&str)->Operation {
    Operation::MutateDocument{operation_id:id.into(),mutation:DocumentMutation::AppendVersion{document_id:view.document.id.clone(),expected_revision:view.document.revision,content:DocumentContent{title:format!("Version {}",view.document.current.version+1),body:body.into()},supplied_content_id:None}}
}
async fn request(c:&Controller,id:&str,target:ReviewTarget,reviewer:PrincipalRef)->ReviewView {
    rev(call(c,Operation::MutateReview{operation_id:id.into(),mutation:ReviewMutation::Request{target,reviewer,summary:"Assess this exact result".into()}}).await)
}
async fn decide(c:&Controller,id:&str,review:&ReviewView,verdict:ReviewVerdict)->Outcome {
    call(c,Operation::MutateReview{operation_id:id.into(),mutation:ReviewMutation::Decide{review_id:review.review.id.clone(),expected_revision:review.review.revision,verdict,explanation:"Recorded assessment".into()}}).await
}

#[tokio::test]
//= spec/impl/v2/implementation.md#document-versions-and-comments-are-immutable
//= type=test
//# A committed document version MUST be immutable.
//= spec/impl/v2/implementation.md#document-versions-and-comments-are-immutable
//= type=test
//# A document version number MUST NOT be reused.
//= spec/impl/v2/implementation.md#document-versions-and-comments-are-immutable
//= type=test
//# A document edit MUST create a new version without changing an earlier version.
//= spec/impl/v2/implementation.md#document-versions-and-comments-are-immutable
//= type=test
//# The initial collaboration profile MUST retain committed document versions.

// Traceability scope: Document/version identities and original mutation receipts survive replay and reopen.
//= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
//= type=test
//# Every collaboration record MUST have a durable identity.

async fn retained_versions_survive_restart_and_exact_mutation_replay() {
    let p=path();let cfg=workflow_config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let first=create(&c,"doc-initial","original bytes <&>").await;
    let operation=append("doc-edit",&first,"second version");
    let committed=call(&c,operation.clone()).await;let second=doc(committed.clone());
    assert_eq!(second.version.reference.version,2);
    assert_ne!(first.version.reference.content_id,second.version.reference.content_id);
    assert_eq!(call(&c,operation.clone()).await,committed);
    let mut changed=operation.clone();if let Operation::MutateDocument{mutation:DocumentMutation::AppendVersion{content,..},..}=&mut changed{content.body="different operation input".into();}
    assert_eq!(code(call(&c,changed).await),"operation_conflict");
    assert_eq!(read(&c,&first.document.id,Some(1)).await.version,first.version);
    drop(c);controller_owner_released(&p).await;
    let c=Controller::open_with_config(&p,cfg).unwrap();
    assert_eq!(read(&c,&first.document.id,Some(1)).await.version,first.version);
    assert_eq!(read(&c,&first.document.id,None).await,second);
    assert_eq!(call(&c,operation).await,committed);
    let third=doc(call(&c,append("doc-edit-third",&second,"third")).await);
    assert_eq!(third.version.reference.version,3);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#document-versions-and-comments-are-immutable
//= type=test
//# The controller MUST refuse a supplied document content identity that differs from its computed content identity.
async fn mismatched_content_identity_refuses_before_current_head_changes() {
    let c=Controller::open_with_config(path(),workflow_config()).unwrap();
    let first=create(&c,"content-id-initial","first").await;
    let mut operation=append("content-id-mismatch",&first,"different");
    if let Operation::MutateDocument{mutation:DocumentMutation::AppendVersion{supplied_content_id,..},..}=&mut operation{*supplied_content_id=Some(first.version.reference.content_id.clone());}
    assert_eq!(code(call(&c,operation).await),"content_identity_mismatch");
    assert_eq!(read(&c,&first.document.id,None).await,first);
    assert_eq!(code(call(&c,Operation::QueryDocument{query:DocumentQuery::Get{document_id:first.document.id,version:Some(2)}}).await),"not_found");
}

#[tokio::test]
//= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
//= type=test
//# A mutation of an existing collaboration record MUST compare the expected record revision.
async fn concurrent_expected_head_edits_commit_only_one_new_version() {
    let c=Controller::open_with_config(path(),workflow_config()).unwrap();
    let first=create(&c,"head-race-start","start").await;
    let (a,b)=tokio::join!(call(&c,append("head-a",&first,"A")),call(&c,append("head-b",&first,"B")));
    let winner=match (a,b){(ok@Outcome::Ok{..},refused@Outcome::Refused{..})|(refused@Outcome::Refused{..},ok@Outcome::Ok{..})=>{assert_eq!(code(refused),"revision_conflict");doc(ok)},v=>panic!("expected one CAS winner: {v:?}")};
    assert_eq!(winner.document.current.version,2);
    assert_eq!(read(&c,&first.document.id,Some(1)).await.version,first.version);
    assert_eq!(read(&c,&first.document.id,None).await,winner);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#document-versions-and-comments-are-immutable
//= type=test
//# A posted collaboration comment MUST be immutable.
async fn comments_replay_original_bytes_and_advance_only_subject_revision() {
    let c=Controller::open_with_config(path(),workflow_config()).unwrap();
    let first=create(&c,"comment-doc","body").await;
    let op=Operation::MutateDocument{operation_id:"comment-once".into(),mutation:DocumentMutation::PostComment{record:RecordRef::Document{document_id:first.document.id.clone()},expected_revision:first.document.revision,body:"Comment <script> & original".into()}};
    let saved=call(&c,op.clone()).await;
    assert_eq!(call(&c,op.clone()).await,saved);
    let comment=match saved{Outcome::Ok{data:Data::DocumentMutation(DocumentMutationOutcome::Comment{comment,record_revision})}=>{assert_eq!(record_revision,first.document.revision+1);comment},x=>panic!("comment expected {x:?}")};
    assert_eq!(comment.author.principal,PrincipalRef::TrustedHostOperator);
    let mut changed=op;if let Operation::MutateDocument{mutation:DocumentMutation::PostComment{body,..},..}=&mut changed{*body="edit original".into();}
    assert_eq!(code(call(&c,changed).await),"operation_conflict");
    let current=read(&c,&first.document.id,None).await;
    assert_eq!(current.version,first.version);assert_eq!(current.document.revision,first.document.revision+1);
    match call(&c,Operation::QueryDocument{query:DocumentQuery::Comments{record:RecordRef::Document{document_id:first.document.id},after:None,limit:1}}).await {
        Outcome::Ok{data:Data::DocumentQuery(DocumentQueryResult::Comments(p))}=>assert_eq!(p.items,vec![comment]),x=>panic!("comments expected {x:?}")
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#reviews-bind-the-exact-subject-version
//= type=test
//# A review MUST bind the exact immutable subject version or result reference it evaluates.
//= spec/impl/v2/implementation.md#reviews-bind-the-exact-subject-version
//= type=test
//# A review decision MUST retain the reviewed subject identity.
//= spec/impl/v2/implementation.md#reviews-bind-the-exact-subject-version
//= type=test
//# A new document version MUST NOT inherit the decision of an earlier version's review.
async fn review_keeps_historical_subject_and_new_version_has_no_inherited_decision() {
    let c=Controller::open_with_config(path(),workflow_config()).unwrap();
    let first=create(&c,"review-doc","review version one").await;
    let first_target=ReviewTarget::DocumentVersion{version:first.version.reference.clone()};
    let review=request(&c,"review-first",first_target.clone(),PrincipalRef::TrustedHostOperator).await;
    let decided=rev(decide(&c,"review-approve",&review,ReviewVerdict::Approved).await);
    let second=doc(call(&c,append("review-doc-edit",&first,"version two")).await);
    assert_eq!(get_review(&c,&review.review.id).await,decided);
    assert_eq!(decided.decision.as_ref().unwrap().target,first_target);
    let next=request(&c,"review-second",ReviewTarget::DocumentVersion{version:second.version.reference.clone()},PrincipalRef::TrustedHostOperator).await;
    assert_eq!(next.review.status,ReviewStatus::Open);assert!(next.decision.is_none());
    assert_ne!(next.review.target,decided.review.target);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#reviews-bind-the-exact-subject-version
//= type=test
//# A review decision MUST match its recorded direct reviewer to the caller context established by the controller.
async fn operator_view_cannot_answer_a_review_owed_by_an_agent() {
    let c=Controller::open_with_config(path(),workflow_config()).unwrap();
    let task=submit(&c,"reviewer-agent-register",native("specific-reviewer")).await;
    let document=create(&c,"reviewer-document","subject").await;
    let review=request(&c,"reviewer-directed",ReviewTarget::DocumentVersion{version:document.version.reference},PrincipalRef::Agent{agent_id:task.assigned_agent_id}).await;
    assert_eq!(code(decide(&c,"reviewer-impersonation",&review,ReviewVerdict::Approved).await),"wrong_reviewer");
    assert_eq!(get_review(&c,&review.review.id).await,review);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#reviews-bind-the-exact-subject-version
//= type=test
//# A recorded review decision MUST be immutable.
async fn concurrent_decision_and_withdrawal_keep_one_terminal_outcome_and_comments_still_work() {
    let c=Controller::open_with_config(path(),workflow_config()).unwrap();
    let d=create(&c,"terminal-doc","body").await;
    let review=request(&c,"terminal-review",ReviewTarget::DocumentVersion{version:d.version.reference},PrincipalRef::TrustedHostOperator).await;
    let withdraw=Operation::MutateReview{operation_id:"terminal-withdraw".into(),mutation:ReviewMutation::Withdraw{review_id:review.review.id.clone(),expected_revision:review.review.revision,reason:"No longer needed".into()}};
    let (a,b)=tokio::join!(decide(&c,"terminal-decide",&review,ReviewVerdict::ChangesRequested),call(&c,withdraw));
    let settled=match(a,b){(ok@Outcome::Ok{..},refused@Outcome::Refused{..})|(refused@Outcome::Refused{..},ok@Outcome::Ok{..})=>{assert_eq!(code(refused),"revision_conflict");rev(ok)},x=>panic!("single terminal transition expected {x:?}")};
    assert_ne!(settled.decision.is_some(),settled.withdrawal.is_some());
    let comment=call(&c,Operation::MutateDocument{operation_id:"terminal-comment".into(),mutation:DocumentMutation::PostComment{record:RecordRef::Review{review_id:settled.review.id.clone()},expected_revision:settled.review.revision,body:"Historical note after resolution".into()}}).await;
    assert!(matches!(comment,Outcome::Ok{data:Data::DocumentMutation(DocumentMutationOutcome::Comment{..})}));
    let after=get_review(&c,&settled.review.id).await;
    assert_eq!(after.decision,settled.decision);assert_eq!(after.withdrawal,settled.withdrawal);
    assert_eq!(after.review.revision,settled.review.revision+1);
}

async fn withdrawal_activity(c: &Controller) -> WorkflowActivityPage {
    match call(c, Operation::QueryWorkflowActivity { cursor: None, limit: 4 }).await {
        Outcome::Ok { data: Data::WorkflowActivity(page) } => {
            assert!(page.next_cursor.is_none(), "the finite withdrawal fixture fits one page");
            page
        }
        other => panic!("committed withdrawal activity expected: {other:?}"),
    }
}

fn assert_withdrawal_activity(before: &WorkflowActivityPage, after: &WorkflowActivityPage, view: &ReviewView) {
    assert_eq!(after.items.len(), before.items.len() + 1);
    assert_eq!(&after.items[..before.items.len()], before.items.as_slice());
    let event = after.items.last().unwrap();
    let withdrawal = view.withdrawal.as_ref().unwrap();
    assert_eq!(event.subject, RecordRef::Review { review_id: view.review.id.clone() });
    assert_eq!(event.subject_revision, view.review.revision);
    assert_eq!(event.id.product_revision, withdrawal.product_revision);
    assert_eq!(event.actor, withdrawal.actor);
    assert_eq!(event.detail, ActivityDetail::Review(ReviewChange::Closed {
        resolution: ReviewResolution::Withdrawn {
            review_id: view.review.id.clone(), target: withdrawal.target.clone(),
            action_revision: view.review.revision, reason: withdrawal.reason.clone(),
        },
    }));
    assert_eq!(after.through.as_ref(), Some(&event.id));
}

#[cfg(debug_assertions)]
async fn withdrawal_reply(c: &Controller, operation: &Operation, request_id: &str) -> Reply {
    let reply=Box::pin(c.handle(Request {protocol_revision:WIRE_REVISION,request_id:request_id.into(),command:operation.clone()})).await;
    reply.validate_for(operation).unwrap();
    reply
}

#[cfg(debug_assertions)]
fn same_withdrawal_receipt(original: &Reply, replay: &Reply) {
    assert!(original.commit.is_some());
    assert_eq!(replay.outcome,original.outcome);
    assert_eq!(replay.commit,original.commit);
    assert_eq!(replay.observed_controller_revision,original.observed_controller_revision);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#reviews-bind-the-exact-subject-version
//= type=test
//# Withdrawing a review MUST record its acting caller context and reason.
//= spec/impl/v2/implementation.md#reviews-bind-the-exact-subject-version
//= type=test
//# A review withdrawal MUST publish committed activity identifying the affected review.
//= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
//= type=test
//# A collaboration change activity MUST identify its typed record subject.
async fn withdrawal_preserves_reason_context_and_exact_original_target() {
    let c=Controller::open_with_config(path(),workflow_config()).unwrap();let d=create(&c,"withdraw-doc","body").await;
    let open=request(&c,"withdraw-review",ReviewTarget::DocumentVersion{version:d.version.reference},PrincipalRef::TrustedHostOperator).await;
    let before=withdrawal_activity(&c).await;
    let request=Operation::MutateReview{operation_id:"withdraw-action".into(),mutation:ReviewMutation::Withdraw{review_id:open.review.id.clone(),expected_revision:open.review.revision,reason:"Superseded by a separate investigation".into()}};
    let committed=call(&c,request.clone()).await;let view=rev(committed.clone());let action=view.withdrawal.as_ref().unwrap();
    assert_eq!(action.reason,"Superseded by a separate investigation");assert_eq!(action.actor.principal,PrincipalRef::TrustedHostOperator);assert_eq!(action.target,open.review.target);
    let after=withdrawal_activity(&c).await;
    assert_withdrawal_activity(&before,&after,&view);
    assert_eq!(call(&c,request).await,committed);
    assert_eq!(code(decide(&c,"withdraw-late-decision",&view,ReviewVerdict::Approved).await),"review_closed");
    assert_eq!(withdrawal_activity(&c).await,after,"replay and a refused late decision cannot publish a second withdrawal");
}

#[cfg(debug_assertions)]
#[tokio::test]
//= spec/impl/v2/implementation.md#reviews-bind-the-exact-subject-version
//= type=test
//# A review withdrawal MUST publish committed activity identifying the affected review.
//= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
//= type=test
//# A collaboration change activity MUST identify its typed record subject.
//= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
//= type=test
//# A collaboration mutation MUST commit its record change and corresponding activity atomically.
async fn withdrawal_commit_failure_publishes_nothing_and_original_retry_commits_one_event() {
    let p=path();let probe=Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut cfg=workflow_config();cfg.qualification_fail_next_commit=Some(probe.clone());
    let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let d=create(&c,"withdraw-cut-document","preserved source").await;
    let open=request(&c,"withdraw-cut-review",ReviewTarget::DocumentVersion{version:d.version.reference},PrincipalRef::TrustedHostOperator).await;
    let before=withdrawal_activity(&c).await;
    let operation=Operation::MutateReview{operation_id:"withdraw-cut-action".into(),mutation:ReviewMutation::Withdraw {
        review_id:open.review.id.clone(),expected_revision:open.review.revision,reason:"Review no longer requested".into(),
    }};
    probe.store(true,Ordering::SeqCst);
    let failed=Box::pin(c.handle(Request {protocol_revision:WIRE_REVISION,request_id:"withdraw-cut-action".into(),command:operation.clone()})).await;
    failed.validate_for(&operation).unwrap();
    assert!(!probe.load(Ordering::SeqCst),"the real SQLite COMMIT failure hook was consumed");
    assert_eq!(code(failed.outcome),"store_unavailable");assert!(failed.commit.is_none());
    assert!(failed.observed_controller_revision.is_none());
    assert_eq!(get_review(&c,&open.review.id).await,open);
    assert_eq!(withdrawal_activity(&c).await,before,"rolled-back activity is not published");
    assert_eq!(code(call(&c,Operation::GetOperation{operation_id:"withdraw-cut-action".into()}).await),"not_found");
    drop(c);controller_owner_released(&p).await;
    let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    assert_eq!(withdrawal_activity(&c).await,before);
    let committed=withdrawal_reply(&c,&operation,"withdraw-retry-request").await;
    let withdrawn=rev(committed.outcome.clone());
    let after=withdrawal_activity(&c).await;assert_withdrawal_activity(&before,&after,&withdrawn);
    same_withdrawal_receipt(&committed,&withdrawal_reply(&c,&operation,"withdraw-replay-request").await);
    assert_eq!(withdrawal_activity(&c).await,after);
    drop(c);controller_owner_released(&p).await;
    let c=Controller::open_with_config(&p,cfg).unwrap();
    assert_eq!(get_review(&c,&open.review.id).await,withdrawn);
    same_withdrawal_receipt(&committed,&withdrawal_reply(&c,&operation,"withdraw-restart-request").await);
    assert_eq!(withdrawal_activity(&c).await,after,"restart and replay retain the one original occurrence");
}

#[tokio::test]
async fn accepted_result_selector_is_controller_owned_and_not_a_current_revision_guess() {
    let c=Controller::open_with_config(path(),workflow_config()).unwrap();
    let t=submit(&c,"accepted-target-task",native("accepted-target-agent")).await;let w=worker("accepted-target-worker","one",1,0);register(&c,&w).await;
    let attempt=run(&commands(&c,&w,vec![]).await);let accepted=finish_native(&c,&attempt,"accepted-target").await;
    let target=match call(&c,Operation::QueryReview{query:ReviewQuery::AcceptedTarget{task_id:t.id.clone(),scope:AcceptedReviewScope::Result}}).await {
        Outcome::Ok{data:Data::ReviewQuery(ReviewQueryResult::AcceptedTarget(target))}=>target,x=>panic!("canonical accepted target expected {x:?}")};
    let ReviewTarget::AcceptedTaskResult{result}=target.clone() else{panic!("result target expected")};
    assert_eq!(result.task_id,t.id);assert_eq!(result.attempt_id,attempt.id);assert!(result.checkpoint.is_none());
    let mut forged=target.clone();if let ReviewTarget::AcceptedTaskResult{result}=&mut forged{result.acceptance_history_sequence+=1;}
    assert!(matches!(call(&c,Operation::MutateReview{operation_id:"forged-acceptance".into(),mutation:ReviewMutation::Request{target:forged,reviewer:PrincipalRef::TrustedHostOperator,summary:"Forge occurrence".into()}}).await,Outcome::Refused{..}));
    // Mutable collaboration activity cannot retarget the original acceptance.
    // Task comments CAS the separate WORK header, not execution Task.revision.
    let before=match call(&c,Operation::QueryWork{query:WorkQuery::Get{task_id:t.id.clone()}}).await {
        Outcome::Ok{data:Data::WorkQuery(WorkQueryResult::Task(view))}=>view,x=>panic!("work view expected {x:?}")};
    assert_eq!(before.coordination_revision,0);assert_eq!(before.task,accepted);
    let comment=call(&c,Operation::MutateDocument{operation_id:"accepted-task-comment".into(),mutation:DocumentMutation::PostComment{record:RecordRef::Task{task_id:t.id.clone()},expected_revision:before.coordination_revision,body:"After acceptance".into()}}).await;
    match comment {
        Outcome::Ok{data:Data::DocumentMutation(DocumentMutationOutcome::Comment{record_revision,..})}=>assert_eq!(record_revision,before.coordination_revision+1),x=>panic!("committed comment expected {x:?}")};
    let after=match call(&c,Operation::QueryWork{query:WorkQuery::Get{task_id:t.id.clone()}}).await {
        Outcome::Ok{data:Data::WorkQuery(WorkQueryResult::Task(view))}=>view,x=>panic!("work view expected {x:?}")};
    assert_eq!(after.coordination_revision,before.coordination_revision+1);
    assert_eq!(after.task,accepted);assert_eq!(get(&c,&t.id).await,accepted);
    let current_target=match call(&c,Operation::QueryReview{query:ReviewQuery::AcceptedTarget{task_id:t.id.clone(),scope:AcceptedReviewScope::Result}}).await {
        Outcome::Ok{data:Data::ReviewQuery(ReviewQueryResult::AcceptedTarget(value))}=>value,x=>panic!("canonical accepted target expected {x:?}")};
    assert_eq!(current_target,target);
    let review=request(&c,"actual-result-review",target.clone(),PrincipalRef::TrustedHostOperator).await;
    assert_eq!(review.review.target,target);
    assert_eq!(get(&c,&t.id).await.result,accepted.result);
    assert!(matches!(call(&c,Operation::QueryReview{query:ReviewQuery::AcceptedTarget{task_id:t.id,scope:AcceptedReviewScope::Checkpoint}}).await,Outcome::Refused{..}));
}

#[tokio::test]
async fn version_cursors_are_scoped_and_text_limits_refuse_without_partial_edit() {
    let c=Controller::open_with_config(path(),workflow_config()).unwrap();let first=create(&c,"page-first","one").await;let other=create(&c,"page-other","other").await;
    let second=doc(call(&c,append("page-second",&first,"two")).await);
    let cursor=match call(&c,Operation::QueryDocument{query:DocumentQuery::Versions{document_id:first.document.id.clone(),after:None,limit:1}}).await{
        Outcome::Ok{data:Data::DocumentQuery(DocumentQueryResult::Versions(page))}=>{assert_eq!(page.items[0],first.version);page.next_cursor.expect("more versions")},x=>panic!("versions expected {x:?}")};
    assert_eq!(code(call(&c,Operation::QueryDocument{query:DocumentQuery::Versions{document_id:other.document.id,after:Some(cursor.clone()),limit:1}}).await),"invalid_input");
    match call(&c,Operation::QueryDocument{query:DocumentQuery::Versions{document_id:first.document.id.clone(),after:Some(cursor),limit:1}}).await{
        Outcome::Ok{data:Data::DocumentQuery(DocumentQueryResult::Versions(page))}=>{assert_eq!(page.items,vec![second.version.clone()]);assert!(page.next_cursor.is_none())},x=>panic!("second page expected {x:?}")}
    assert_eq!(code(call(&c,append("oversized-edit",&second,&"x".repeat(2049))).await),"invalid_input");
    assert_eq!(read(&c,&first.document.id,None).await,second);
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn real_commit_failure_rolls_back_document_and_review_outcome_before_same_id_retry() {
    let p=path();let probe=Arc::new(std::sync::atomic::AtomicBool::new(false));let mut cfg=workflow_config();cfg.qualification_fail_next_commit=Some(probe.clone());
    let c=Controller::open_with_config(&p,cfg.clone()).unwrap();let d=create(&c,"rollback-document","kept").await;
    let review=request(&c,"rollback-review",ReviewTarget::DocumentVersion{version:d.version.reference},PrincipalRef::TrustedHostOperator).await;
    probe.store(true,Ordering::SeqCst);
    let command=Operation::MutateReview{operation_id:"rollback-decision".into(),mutation:ReviewMutation::Decide{review_id:review.review.id.clone(),expected_revision:review.review.revision,verdict:ReviewVerdict::Rejected,explanation:"Recorded assessment".into()}};
    let failed=c.handle(Request { protocol_revision: WIRE_REVISION, request_id: "rollback-decision".into(), command: command.clone() }).await;
    failed.validate_for(&command).unwrap();
    assert!(!probe.load(Ordering::SeqCst));assert_eq!(code(failed.outcome),"store_unavailable");
    assert!(failed.commit.is_none());assert!(failed.observed_controller_revision.is_none());
    assert_eq!(get_review(&c,&review.review.id).await,review);
    assert_eq!(code(call(&c,Operation::GetOperation{operation_id:"rollback-decision".into()}).await),"not_found");
    drop(c);controller_owner_released(&p).await;
    let sql=rusqlite::Connection::open(&p).unwrap();assert_eq!(sql.query_row("SELECT count(*) FROM workflow_review_decisions",[],|r|r.get::<_,i64>(0)).unwrap(),0);drop(sql);
    let c=Controller::open_with_config(&p,cfg).unwrap();let committed=decide(&c,"rollback-decision",&review,ReviewVerdict::Rejected).await;
    assert_eq!(rev(committed.clone()).review.status,ReviewStatus::Decided);
    assert_eq!(decide(&c,"rollback-decision",&review,ReviewVerdict::Rejected).await,committed);
}


async fn mcp_actor(c:&Controller,label:&str,v2:bool)->(Task,Attempt,borg_runtime::mcp::Caller) {
    let mut input=native(label);
    input.definition.execution_profile=Some(if v2{ExecutionProfile::CollaborationV2{web_search:None}}else{ExecutionProfile::CollaborationV1{web_search:None}});
    let t=submit(c,&format!("{label}-task"),input).await;
    let w=worker(&format!("{label}-worker"),"one",1,0);register(c,&w).await;
    let a=run(&commands(c,&w,vec![]).await);
    task(report(c,&a,&format!("{label}-started"),Observation::Started{thread_id:None,turn_id:None}).await);
    let caller=borg_runtime::mcp::Caller{task_id:t.id.clone(),attempt_id:a.id.clone(),generation:a.generation,worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone()};
    (t,a,caller)
}
fn tool(caller:&borg_runtime::mcp::Caller,id:&str,name:&str,arguments:serde_json::Value)->Operation {
    borg_runtime::mcp::Invocation{operation_id:id.into(),logical_call_id:format!("logical-{id}"),caller:caller.clone(),tool:name.into(),arguments}.operation()
}

fn named_limit(outcome:Outcome,field:&str) {
    match outcome {
        Outcome::Refused{message,..}=>assert_eq!(message,format!("Configured limit exceeded: {field}")),
        other=>panic!("expected configured-limit refusal for {field}: {other:?}"),
    }
}
fn malformed(outcome:Outcome) {
    match outcome {
        Outcome::Refused{code,message}=>{assert_eq!(code,"invalid_input");assert!(!message.contains("Configured limit"),"{message}");},
        other=>panic!("expected malformed input: {other:?}"),
    }
}
#[tokio::test]
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# An operation refused by a configured limit MUST identify the limiting metadata field.
async fn workflow_configured_refusals_name_text_record_page_cursor_and_identity_fields() {
    let cfg=workflow_config();let limits=cfg.limits.clone();let c=Controller::open_with_config(path(),cfg).unwrap();
    let first=create(&c,"limit-doc","kept").await;
    for (label,body) in [("ascii","x".repeat(2049)),("utf8","é".repeat(1025))] {
        named_limit(call(&c,append(&format!("limit-text-{label}"),&first,&body)).await,"workflow.text_bytes");
    }
    // Accepted UTF-8 length can still exceed canonical record bytes after JSON
    // escaping; this must report the record field rather than the text field.
    named_limit(call(&c,append("limit-record",&first,&"\u{0001}".repeat(2048))).await,"workflow.record_bytes");
    named_limit(call(&c,Operation::QueryDocument{query:DocumentQuery::Versions{document_id:first.document.id.clone(),after:None,limit:5}}).await,"workflow.page_items");
    named_limit(call(&c,Operation::QueryDocument{query:DocumentQuery::Versions{document_id:first.document.id.clone(),after:Some("x".repeat(limits.controller.collaboration_cursor_bytes+1)),limit:1}}).await,"controller.collaboration_cursor_bytes");
    named_limit(call(&c,Operation::QueryDocument{query:DocumentQuery::Get{document_id:"x".repeat(limits.transport.identity_bytes+1),version:None}}).await,"transport.identity_bytes");
    malformed(call(&c,Operation::QueryDocument{query:DocumentQuery::Versions{document_id:first.document.id.clone(),after:None,limit:0}}).await);
    malformed(call(&c,Operation::MutateDocument{operation_id:"malformed-comment".into(),mutation:DocumentMutation::PostComment{record:RecordRef::Document{document_id:first.document.id.clone()},expected_revision:first.document.revision,body:"bad\0comment".into()}}).await);
    assert_eq!(read(&c,&first.document.id,None).await,first);
    let mut cfg=workflow_config();Arc::make_mut(&mut cfg.limits).workflow.as_mut().unwrap().page_bytes=64;
    let small=Controller::open_with_config(path(),cfg).unwrap();let first=create(&small,"small-page","one version larger than the page envelope").await;
    named_limit(call(&small,Operation::QueryDocument{query:DocumentQuery::Versions{document_id:first.document.id,after:None,limit:1}}).await,"workflow.page_bytes");
}

#[tokio::test]
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# An operation refused by a configured limit MUST identify the limiting metadata field.
async fn mcp_nested_workflow_limits_reach_named_domain_refusals_and_malformed_stays_structural() {
    let cfg=workflow_config();let limits=cfg.limits.clone();let c=Controller::open_with_config(path(),cfg).unwrap();
    let (task,_,caller)=mcp_actor(&c,"limit-mcp-v2",true).await;
    let owner=PrincipalRef::Agent{agent_id:task.assigned_agent_id.clone()};
    let first=doc(call(&c,tool(&caller,"limit-mcp-first","collab_document_create",json!({"content":{"title":"First","body":"kept"},"supplied_content_id":null,"responsible_owner":owner}))).await);
    let target=ReviewTarget::DocumentVersion{version:first.version.reference.clone()};
    let review=request(&c,"limit-mcp-review",target.clone(),owner.clone()).await;
    let cases=[
        ("collab_document_create",json!({"content":{"title":"é".repeat(1025),"body":"body"},"supplied_content_id":null,"responsible_owner":owner}),"workflow.text_bytes"),
        ("collab_document_version_create",json!({"document_id":first.document.id,"expected_revision":first.document.revision,"content":{"title":"Version","body":"x".repeat(2049)},"supplied_content_id":null}),"workflow.text_bytes"),
        ("collab_comment_post",json!({"record":{"kind":"document","document_id":first.document.id},"expected_revision":first.document.revision,"body":"é".repeat(1025)}),"workflow.text_bytes"),
        ("collab_review_create",json!({"target":target,"reviewer":owner,"summary":"x".repeat(2049)}),"workflow.text_bytes"),
        ("collab_review_decide",json!({"review_id":review.review.id,"expected_revision":review.review.revision,"verdict":"approved","explanation":"é".repeat(1025)}),"workflow.text_bytes"),
        ("collab_review_withdraw",json!({"review_id":review.review.id,"expected_revision":review.review.revision,"reason":"x".repeat(2049)}),"workflow.text_bytes"),
        ("collab_document_versions_list",json!({"document_id":first.document.id,"cursor":null,"limit":5}),"workflow.page_items"),
    ];
    for (index,(name,args,field)) in cases.into_iter().enumerate() {
        assert!(borg_runtime::mcp::validate_arguments_for_profile(name,&args,&limits,task.input.definition.execution_profile.as_ref()).is_ok(),"configured bound must reach domain: {name}");
        named_limit(call(&c,tool(&caller,&format!("mcp-limit-{index}"),name,args)).await,field);
    }
    let malformed_args=json!({"content":{"title":42,"body":"body"},"supplied_content_id":null,"responsible_owner":owner});
    let error=borg_runtime::mcp::validate_arguments_for_profile("collab_document_create",&malformed_args,&limits,task.input.definition.execution_profile.as_ref()).unwrap_err();
    assert!(!error.contains("Configured limit"));
    malformed(call(&c,tool(&caller,"mcp-malformed-document","collab_document_create",malformed_args)).await);
    assert_eq!(read(&c,&first.document.id,None).await,first);
    assert_eq!(get_review(&c,&review.review.id).await,review);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#collaboration-surfaces-preserve-existing-authority
//= type=test
//# The shared agent interface MUST expose every supported collaboration operation permitted to its current caller.
async fn mcp_first_task_comment_accepts_zero_coordination_cas_without_widening_other_revisions() {
    let cfg=workflow_config();let limits=cfg.limits.clone();let c=Controller::open_with_config(path(),cfg).unwrap();
    let (task,_,caller)=mcp_actor(&c,"first-comment-mcp",true).await;
    let before=get(&c,&task.id).await;
    let args=json!({"record":{"kind":"task","task_id":task.id},"expected_revision":0,"body":"First task comment"});
    assert!(borg_runtime::mcp::validate_arguments_for_profile("collab_comment_post",&args,&limits,task.input.definition.execution_profile.as_ref()).is_ok());
    // Legacy catalogue remains closed; only the task's accepted V2 profile admits this operation.
    assert!(borg_runtime::mcp::validate_arguments("collab_comment_post",&args,&limits).is_err());
    let operation=tool(&caller,"first-task-comment","collab_comment_post",args.clone());
    let saved=call(&c,operation.clone()).await;
    assert!(matches!(&saved,Outcome::Ok{data:Data::DocumentMutation(DocumentMutationOutcome::Comment{record_revision:1,..})}));
    assert_eq!(call(&c,operation).await,saved);
    assert!(matches!(call(&c,tool(&caller,"stale-task-comment","collab_comment_post",args)).await,Outcome::Refused{..}));
    assert_eq!(get(&c,&task.id).await,before);
    let d=create(&c,"zero-comment-document","body").await;
    assert!(matches!(call(&c,tool(&caller,"zero-document-comment","collab_comment_post",json!({"record":{"kind":"document","document_id":d.document.id},"expected_revision":0,"body":"Stale document revision"}))).await,Outcome::Refused{..}));
    for (name,args) in [
        ("collab_document_version_create",json!({"document_id":d.document.id,"expected_revision":0,"content":{"title":"No","body":"No"},"supplied_content_id":null})),
        ("collab_review_decide",json!({"review_id":"review-example","expected_revision":0,"verdict":"approved","explanation":"No"})),
    ] {assert!(borg_runtime::mcp::validate_arguments_for_profile(name,&args,&limits,task.input.definition.execution_profile.as_ref()).is_err(),"{name}");}
    assert_eq!(read(&c,&d.document.id,None).await,d);
}

async fn retained_workflow_store()->(PathBuf,ControllerConfig,DocumentVersion) {
    let p=path();let cfg=workflow_config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let first=create(&c,"retained-first","historical bytes").await;
    let second=doc(call(&c,append("retained-second",&first,"current bytes")).await);
    let mut revision=second.document.revision;
    for i in 0..6 {
        match call(&c,Operation::MutateDocument{operation_id:format!("retained-comment-{i}"),mutation:DocumentMutation::PostComment{record:RecordRef::Document{document_id:first.document.id.clone()},expected_revision:revision,body:format!("Comment {i}")}}).await {
            Outcome::Ok{data:Data::DocumentMutation(DocumentMutationOutcome::Comment{record_revision,..})}=>revision=record_revision,x=>panic!("retained comment expected {x:?}")};
    }
    let target=ReviewTarget::DocumentVersion{version:first.version.reference.clone()};
    let rejected=request(&c,"retained-review-decided",target.clone(),PrincipalRef::TrustedHostOperator).await;
    assert_eq!(rev(decide(&c,"retained-decision",&rejected,ReviewVerdict::Rejected).await).review.status,ReviewStatus::Decided);
    let withdrawn=request(&c,"retained-review-withdrawn",target.clone(),PrincipalRef::TrustedHostOperator).await;
    assert_eq!(rev(call(&c,Operation::MutateReview{operation_id:"retained-withdrawal".into(),mutation:ReviewMutation::Withdraw{review_id:withdrawn.review.id,expected_revision:withdrawn.review.revision,reason:"Retained withdrawal".into()}}).await).review.status,ReviewStatus::Withdrawn);
    for i in 0..5 {request(&c,&format!("retained-open-review-{i}"),target.clone(),PrincipalRef::TrustedHostOperator).await;}
    drop(c);controller_owner_released(&p).await;(p,cfg,first.version)
}
fn retained_store_bytes(p:&std::path::Path)->Vec<(PathBuf,Option<Vec<u8>>)> {
    [p.to_path_buf(),controller_sidecar(p,"-wal"),controller_sidecar(p,"-shm")].into_iter()
        .map(|path|{let bytes=std::fs::read(&path).ok();(path,bytes)}).collect()
}
fn assert_corrupt_reopen_unchanged(p:&std::path::Path,cfg:ControllerConfig,label:&str) {
    let before=retained_store_bytes(p);
    assert!(Controller::open_with_config(p,cfg).is_err(),"corrupt workflow admitted: {label}");
    assert_eq!(retained_store_bytes(p),before,"corrupt workflow rewritten: {label}");
}

#[tokio::test]
async fn valid_retained_versions_comments_and_review_outcomes_reopen_beyond_a_page() {
    let (p,cfg,first)=retained_workflow_store().await;let c=Controller::open_with_config(&p,cfg).unwrap();
    assert_eq!(read(&c,&first.reference.document_id,Some(1)).await.version,first);
    let mut after=None;let mut comments=0;
    loop {
        let page=match call(&c,Operation::QueryDocument{query:DocumentQuery::Comments{record:RecordRef::Document{document_id:first.reference.document_id.clone()},after,limit:4}}).await {
            Outcome::Ok{data:Data::DocumentQuery(DocumentQueryResult::Comments(page))}=>page,x=>panic!("comments expected {x:?}")};
        comments+=page.items.len();after=page.next_cursor;if after.is_none(){break}
    }
    assert_eq!(comments,6);
    let mut after=None;let mut reviews=0;
    loop {
        let page=match call(&c,Operation::QueryReview{query:ReviewQuery::List{record:None,status:None,after,limit:4}}).await {
            Outcome::Ok{data:Data::ReviewQuery(ReviewQueryResult::Reviews(page))}=>page,x=>panic!("reviews expected {x:?}")};
        reviews+=page.items.len();after=page.next_cursor;if after.is_none(){break}
    }
    assert_eq!(reviews,7);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# Corrupt stored task state MUST be refused without rewriting its data.
async fn every_retained_document_review_table_refuses_noncanonical_bytes_without_repair() {
    for table in ["workflow_documents","workflow_document_versions","workflow_comments","workflow_reviews","workflow_review_decisions","workflow_review_withdrawals"] {
        let (p,cfg,_)=retained_workflow_store().await;
        let db=rusqlite::Connection::open(&p).unwrap();
        let predicate=if table=="workflow_document_versions"{" WHERE version=1"}else{""};
        assert!(db.execute(&format!("UPDATE {table} SET body=body || ' '{predicate}"),[]).unwrap()>0);
        db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();drop(db);
        assert_corrupt_reopen_unchanged(&p,cfg,table);
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# Corrupt stored task state MUST be refused without rewriting its data.
async fn retained_index_orphan_gap_and_oversized_rows_refuse_reopen_without_repair() {
    for sql in [
        "UPDATE workflow_documents SET revision=revision+1",
        "UPDATE workflow_document_versions SET document_id='missing-document' WHERE version=1",
        "DELETE FROM workflow_document_versions WHERE version=1",
        "UPDATE workflow_comments SET record_id='missing-document'",
        "UPDATE workflow_reviews SET reviewer='not a canonical principal'",
        "UPDATE workflow_review_decisions SET review_id='missing-review'",
        "UPDATE workflow_review_withdrawals SET review_id='missing-review'",
        "UPDATE workflow_comments SET body=zeroblob(8193)",
        "UPDATE workflow_reviews SET reviewer=zeroblob(8193)",
    ] {
        let (p,cfg,_)=retained_workflow_store().await;let db=rusqlite::Connection::open(&p).unwrap();
        db.execute_batch("PRAGMA foreign_keys=OFF").unwrap();db.execute_batch(sql).unwrap();
        db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();drop(db);
        assert_corrupt_reopen_unchanged(&p,cfg,sql);
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# Corrupt stored task state MUST be refused without rewriting its data.
async fn canonical_historical_version_and_comment_associations_are_checked_at_startup() {
    for defect in ["future_version","changed_historical_content","missing_comment_subject","review_target","decision_target","decision_before_request","withdrawal_before_request"] {
        let (p,cfg,_)=retained_workflow_store().await;let db=rusqlite::Connection::open(&p).unwrap();
        if defect=="missing_comment_subject" {
            let raw:String=db.query_row("SELECT body FROM workflow_comments ORDER BY id LIMIT 1",[],|r|r.get(0)).unwrap();
            let mut comment:RecordedComment=serde_json::from_str(&raw).unwrap();comment.record=RecordRef::Document{document_id:"missing-document".into()};
            db.execute("UPDATE workflow_comments SET record_id='missing-document',body=?2 WHERE id=?1",rusqlite::params![comment.id,serde_json::to_string(&comment).unwrap()]).unwrap();
        } else if defect=="review_target" {
            let raw:String=db.query_row("SELECT body FROM workflow_reviews WHERE status='open' ORDER BY id LIMIT 1",[],|r|r.get(0)).unwrap();
            let mut review:Review=serde_json::from_str(&raw).unwrap();
            let ReviewTarget::DocumentVersion{version}=&mut review.target else{panic!("document target expected")};version.content_id="wrong-content-identity".into();
            db.execute("UPDATE workflow_reviews SET body=?2 WHERE id=?1",rusqlite::params![review.id,serde_json::to_string(&review).unwrap()]).unwrap();
        } else if defect=="decision_target" || defect=="decision_before_request" {
            let raw:String=db.query_row("SELECT body FROM workflow_review_decisions LIMIT 1",[],|r|r.get(0)).unwrap();
            let mut decision:RecordedReviewDecision=serde_json::from_str(&raw).unwrap();
            if defect=="decision_target" {
                let ReviewTarget::DocumentVersion{version}=&mut decision.target else{panic!("document target expected")};version.content_id="wrong-content-identity".into();
            } else {
                let created:i64=db.query_row("SELECT created_product_revision FROM workflow_reviews WHERE id=?1",[&decision.review_id],|r|r.get(0)).unwrap();decision.product_revision=created as u64;
            }
            db.execute("UPDATE workflow_review_decisions SET body=?2 WHERE id=?1",rusqlite::params![decision.id,serde_json::to_string(&decision).unwrap()]).unwrap();
        } else if defect=="withdrawal_before_request" {
            let raw:String=db.query_row("SELECT body FROM workflow_review_withdrawals LIMIT 1",[],|r|r.get(0)).unwrap();
            let mut withdrawal:RecordedReviewWithdrawal=serde_json::from_str(&raw).unwrap();
            let created:i64=db.query_row("SELECT created_product_revision FROM workflow_reviews WHERE id=?1",[&withdrawal.review_id],|r|r.get(0)).unwrap();withdrawal.product_revision=created as u64;
            db.execute("UPDATE workflow_review_withdrawals SET body=?2 WHERE review_id=?1",rusqlite::params![withdrawal.review_id,serde_json::to_string(&withdrawal).unwrap()]).unwrap();
        } else {
            let raw:String=db.query_row("SELECT body FROM workflow_document_versions WHERE version=1",[],|r|r.get(0)).unwrap();
            let mut version:DocumentVersion=serde_json::from_str(&raw).unwrap();
            if defect=="future_version" {version.product_revision=i64::MAX as u64;}else{version.content.body="changed historical bytes".into();}
            db.execute("UPDATE workflow_document_versions SET body=?1 WHERE version=1",[serde_json::to_string(&version).unwrap()]).unwrap();
        }
        db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();drop(db);
        assert_corrupt_reopen_unchanged(&p,cfg,defect);
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# Corrupt stored task state MUST be refused without rewriting its data.
async fn canonical_subject_marker_rollback_below_retained_comments_refuses_reopen_without_repair() {
    use borg_runtime::collaboration::questions::{PoseQuestion,QuestionMutation,QuestionOption};
    for kind in ["task","question","document","review"] {
        let p=path();let cfg=workflow_config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
        let document=create(&c,"comment-chronology-document","Exact retained bytes").await;
        let (subject,mut revision,table,key,subject_id)=match kind {
            "task"=>{
                let task=submit(&c,"comment-chronology-task",native("comment-chronology-agent")).await;
                // Task execution revision is separate; the first coordination comment CAS is zero.
                (RecordRef::Task{task_id:task.id.clone()},0,"workflow_task_state","task_id",task.id)
            }
            "question"=>{
                let posed=PoseQuestion{record:RecordRef::Document{document_id:document.document.id.clone()},
                    recipient:PrincipalRef::TrustedHostOperator,prompt:"Choose the retained wording.".into(),
                    options:vec![QuestionOption{id:"keep".into(),label:"Keep".into(),explanation:"Preserve the wording".into()},
                        QuestionOption{id:"change".into(),label:"Change".into(),explanation:"Revise the wording".into()}],
                    recommended_option_id:"keep".into()};
                let question=match call(&c,Operation::MutateQuestion{operation_id:"comment-chronology-question".into(),mutation:QuestionMutation::Pose{question:posed}}).await {
                    Outcome::Ok{data:Data::QuestionMutation(changed)}=>changed.question.question,x=>panic!("question expected {x:?}")};
                (RecordRef::Question{question_id:question.id.clone()},question.revision,"workflow_questions","id",question.id)
            }
            "document"=>(RecordRef::Document{document_id:document.document.id.clone()},document.document.revision,"workflow_documents","id",document.document.id.clone()),
            "review"=>{
                let review=request(&c,"comment-chronology-review",ReviewTarget::DocumentVersion{version:document.version.reference.clone()},PrincipalRef::TrustedHostOperator).await.review;
                (RecordRef::Review{review_id:review.id.clone()},review.revision,"workflow_reviews","id",review.id)
            }
            _=>unreachable!(),
        };
        let mut comments=Vec::new();
        for i in 0..2 {
            match call(&c,Operation::MutateDocument{operation_id:format!("comment-chronology-{i}"),mutation:DocumentMutation::PostComment{
                record:subject.clone(),expected_revision:revision,body:format!("Retained comment {i}")}}).await {
                Outcome::Ok{data:Data::DocumentMutation(DocumentMutationOutcome::Comment{comment,record_revision})}=>{
                    assert_eq!(record_revision,revision+1);revision=record_revision;comments.push(comment);
                }
                x=>panic!("comment expected for {kind}: {x:?}"),
            }
        }
        assert!(comments[0].product_revision>0);
        assert!(comments[0].product_revision<comments[1].product_revision);
        drop(c);controller_owner_released(&p).await;
        // First prove the exact retained fixture reopens and all subject kinds are readable.
        // For Question/Review this also rejects mistakenly checking the older linked Document marker.
        let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
        match call(&c,Operation::QueryDocument{query:DocumentQuery::Comments{record:subject,after:None,limit:4}}).await {
            Outcome::Ok{data:Data::DocumentQuery(DocumentQueryResult::Comments(page))}=>{
                assert_eq!(page.items,comments);assert!(page.next_cursor.is_none());
            }
            x=>panic!("valid retained comments expected for {kind}: {x:?}"),
        }
        drop(c);controller_owner_released(&p).await;
        let db=rusqlite::Connection::open(&p).unwrap();
        let (indexed_revision,raw):(i64,String)=db.query_row(&format!("SELECT revision,body FROM {table} WHERE {key}=?1"),[&subject_id],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
        assert_eq!(indexed_revision,revision as i64);
        let before:serde_json::Value=serde_json::from_str(&raw).unwrap();
        assert_eq!(before["changed_product_revision"].as_u64(),Some(comments[1].product_revision));
        // Preserve canonical field order, every index and every immutable child byte.
        // A positive earlier comment marker keeps the task header otherwise valid too.
        let token=format!("\"changed_product_revision\":{}",comments[1].product_revision);
        assert_eq!(raw.matches(&token).count(),1);
        let rolled_back=raw.replacen(&token,&format!("\"changed_product_revision\":{}",comments[0].product_revision),1);
        let mut expected=before;expected["changed_product_revision"]=json!(comments[0].product_revision);
        assert_eq!(serde_json::from_str::<serde_json::Value>(&rolled_back).unwrap(),expected);
        assert_eq!(db.execute(&format!("UPDATE {table} SET body=?2 WHERE {key}=?1"),rusqlite::params![subject_id,rolled_back]).unwrap(),1);
        db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();drop(db);
        assert_corrupt_reopen_unchanged(&p,cfg,kind);
    }
}

#[tokio::test]
async fn browser_and_explicit_v2_mcp_share_documents_comments_and_review_decisions() {
    let c=Controller::open_with_config(path(),workflow_config()).unwrap();
    let (t,_,caller)=mcp_actor(&c,"document-mcp-v2",true).await;
    let owner=PrincipalRef::Agent{agent_id:t.assigned_agent_id.clone()};
    let create=tool(&caller,"mcp-doc-create","collab_document_create",json!({"content":{"title":"Agent <draft>","body":"Shared & exact"},"supplied_content_id":null,"responsible_owner":owner}));
    let first_outcome=call(&c,create.clone()).await;let first=doc(first_outcome.clone());
    assert_eq!(first.version.author.principal,owner);
    assert!(matches!(&first.version.author.provenance,ActorProvenance::BoundAgentSession{task_id,attempt_id,..} if task_id==&caller.task_id&&attempt_id==&caller.attempt_id));
    let browser=Box::pin(borg_runtime::product::handle(&c,borg_runtime::product::UiRequest{method:"GET".into(),path:format!("/documents/{}",first.document.id),body:vec![],origin:None,expected_origin:"http://127.0.0.1:7080".into()})).await;
    assert_eq!(browser.status,200,"{}",browser.body);assert!(browser.body.contains("Shared &amp; exact"));
    let target=ReviewTarget::DocumentVersion{version:first.version.reference.clone()};
    let requested=rev(call(&c,tool(&caller,"mcp-review-create","collab_review_create",json!({"target":target,"reviewer":owner,"summary":"Assess exact target"}))).await);
    let approved=rev(call(&c,tool(&caller,"mcp-review-decide","collab_review_decide",json!({"review_id":requested.review.id,"expected_revision":requested.review.revision,"verdict":"approved","explanation":"Agent product assessment"}))).await);
    assert_eq!(approved.decision.as_ref().unwrap().reviewer.principal,owner);
    assert_eq!(approved.decision.as_ref().unwrap().target,target);
    assert_eq!(get_review(&c,&requested.review.id).await,approved);
    assert_eq!(code(call(&c,tool(&caller,"mcp-doc-forged-author","collab_document_create",json!({"content":{"title":"forged","body":"body"},"supplied_content_id":null,"responsible_owner":owner,"author":{"principal":{"kind":"trusted_host_operator"}}}))).await),"invalid_input");
    let current=get(&c,&t.id).await;
    call(&c,Operation::CancelTask{operation_id:"cancel-doc-agent".into(),task_id:t.id,expected_revision:current.revision}).await;
    assert_eq!(call(&c,create).await,first_outcome);
    assert!(matches!(call(&c,tool(&caller,"mcp-doc-after-cancel","collab_comment_post",json!({"record":{"kind":"document","document_id":first.document.id},"expected_revision":first.document.revision,"body":"new stale occurrence"}))).await,Outcome::Refused{..}));
    assert_eq!(read(&c,&first.document.id,None).await,first);
    assert_eq!(get_review(&c,&requested.review.id).await,approved);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#collaboration-surfaces-preserve-existing-authority
//= type=test
//# Adding collaboration operations MUST NOT expand an already accepted task's configured tool profile.
async fn existing_v1_profile_refuses_new_document_tools_without_changing_accepted_settings() {
    let c=Controller::open_with_config(path(),workflow_config()).unwrap();
    let (t,_,caller)=mcp_actor(&c,"document-mcp-v1",false).await;
    let before=get(&c,&t.id).await.input;
    let result=call(&c,tool(&caller,"v1-forbidden-document","collab_document_create",json!({"content":{"title":"not admitted","body":"not admitted"},"supplied_content_id":null,"responsible_owner":{"kind":"trusted_host_operator"}}))).await;
    assert!(matches!(result,Outcome::Refused{..}));
    assert_eq!(get(&c,&t.id).await.input,before);
    match call(&c,Operation::QueryDocument{query:DocumentQuery::List{after:None,limit:1}}).await {Outcome::Ok{data:Data::DocumentQuery(DocumentQueryResult::Documents(page))}=>assert!(page.items.is_empty()),x=>panic!("empty document list expected {x:?}")}
}


// Traceability scope: A successful exact-checkpoint review preserves the entire metadata-only release candidate and its unconfigured rollout authority.
//= spec/impl/v2/implementation.md#collaboration-surfaces-preserve-existing-authority
//= type=test
//# A product question or review decision MUST NOT confer protected rollout authority.

#[tokio::test]
async fn accepted_checkpoint_review_checks_every_original_tuple_and_does_not_qualify_release() {
    let c=Controller::open_with_config(path(),workflow_config()).unwrap();
    let t=accepted_release_task(&c,"checkpoint-review").await;
    let selected=match call(&c,Operation::QueryReview{query:ReviewQuery::AcceptedTarget{task_id:t.id.clone(),scope:AcceptedReviewScope::Checkpoint}}).await {
        Outcome::Ok{data:Data::ReviewQuery(ReviewQueryResult::AcceptedTarget(target))}=>target,x=>panic!("accepted checkpoint target expected {x:?}")};
    let ReviewTarget::AcceptedTaskCheckpoint{result}=&selected else{panic!("checkpoint scope expected")};
    assert_eq!(result.checkpoint,t.result.as_ref().unwrap().checkpoint);
    for field in ["registration","repository","commit","remote_ref","publication","attempt","terminal_revision"] {
        let mut altered=selected.clone();
        let ReviewTarget::AcceptedTaskCheckpoint{result}=&mut altered else{unreachable!()};
        match field {
            "registration"=>result.checkpoint.as_mut().unwrap().registration.registration_revision.push_str("-other"),
            "repository"=>result.checkpoint.as_mut().unwrap().repository.push_str("-other"),
            "commit"=>result.checkpoint.as_mut().unwrap().commit="f".repeat(40),
            "remote_ref"=>result.checkpoint.as_mut().unwrap().remote_ref.push_str("-other"),
            "publication"=>result.checkpoint.as_mut().unwrap().operation_id.push_str("-other"),
            "attempt"=>result.attempt_id.push_str("-other"),
            "terminal_revision"=>result.terminal_task_revision+=1,
            _=>unreachable!(),
        }
        assert!(matches!(call(&c,Operation::MutateReview{operation_id:format!("forged-checkpoint-{field}"),mutation:ReviewMutation::Request{target:altered,reviewer:PrincipalRef::TrustedHostOperator,summary:"Invalid immutable subject".into()}}).await,Outcome::Refused{..}),"forged {field}");
    }
    let release=release(call(&c,Operation::PrepareRelease(release_input(&t,"checkpoint-release"))).await);assert_unqualified(&release);
    let review=request(&c,"checkpoint-product-review",selected,PrincipalRef::TrustedHostOperator).await;
    let settled=rev(decide(&c,"checkpoint-product-approve",&review,ReviewVerdict::Approved).await);
    assert_eq!(settled.review.status,ReviewStatus::Decided);
    let after=match call(&c,Operation::GetRelease{release_id:release.id.clone()}).await {Outcome::Ok{data:Data::Release(value)}=>*value,x=>panic!("release expected {x:?}")};
    assert_eq!(after,release);assert_unqualified(&after);
}


#[tokio::test]
async fn owner_change_updates_routing_without_rewriting_accepted_execution_or_review_subject() {
    let c=Controller::open_with_config(path(),workflow_config()).unwrap();
    let task=submit(&c,"owner-existing-agent",native("owner-agent")).await;
    let initial=create(&c,"owner-document","retained content").await;
    let review=request(&c,"owner-review",ReviewTarget::DocumentVersion{version:initial.version.reference.clone()},PrincipalRef::TrustedHostOperator).await;
    let owner=PrincipalRef::Agent{agent_id:task.assigned_agent_id.clone()};
    let changed=doc(call(&c,Operation::MutateDocument{operation_id:"owner-change".into(),mutation:DocumentMutation::SetResponsibleOwner{document_id:initial.document.id.clone(),expected_revision:initial.document.revision,responsible_owner:owner.clone()}}).await);
    assert_eq!(changed.version,initial.version);assert_eq!(changed.document.responsible_owner,owner);
    let current_review=get_review(&c,&review.review.id).await;
    assert_eq!(current_review.review,review.review);assert_eq!(current_review.current_responsible_owner,owner);
    assert_eq!(get(&c,&task.id).await.input,task.input);
    assert_eq!(get(&c,&task.id).await.assigned_agent_id,task.assigned_agent_id);
    let stale=Operation::MutateDocument{operation_id:"owner-change-stale".into(),mutation:DocumentMutation::SetResponsibleOwner{document_id:initial.document.id,expected_revision:initial.document.revision,responsible_owner:PrincipalRef::TrustedHostOperator}};
    assert_eq!(code(call(&c,stale).await),"revision_conflict");
}

fn docs_encode(value: &str) -> String {
    value.bytes().map(|b| if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
        char::from(b).to_string()
    } else { format!("%{b:02X}") }).collect()
}
fn docs_form(fields: &[(&str, String)]) -> Vec<u8> {
    fields.iter().map(|(key, value)| format!("{}={}", docs_encode(key), docs_encode(value)))
        .collect::<Vec<_>>().join("&").into_bytes()
}
async fn docs_browser(c: &Controller, method: &str, path: &str, body: Vec<u8>) -> borg_runtime::product::UiResponse {
    Box::pin(borg_runtime::product::handle(c, borg_runtime::product::UiRequest {
        method: method.into(), path: path.into(), body,
        origin: Some("http://127.0.0.1:7080".into()), expected_origin: "http://127.0.0.1:7080".into(),
    })).await
}
fn docs_queue(outcome: Outcome) -> borg_runtime::collaboration::questions::DecisionQueuePage {
    match outcome {
        Outcome::Ok { data: Data::QuestionQuery(borg_runtime::collaboration::questions::QuestionQueryResult::DecisionQueue(page)) } => page,
        other => panic!("expected decision queue: {other:?}"),
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST present the information exposed through the shared agent interface.
//= spec/impl/v2/implementation.md#the-supported-decision-queue-is-complete
//= type=test
//# The decision queue MUST include every open question directly routed to its viewer.
//= spec/impl/v2/implementation.md#the-supported-decision-queue-is-complete
//= type=test
//# The decision queue MUST include every open review directly routed to its viewer.
//= spec/impl/v2/implementation.md#reviews-bind-the-exact-subject-version
//= type=test
//# A review decision MUST match its recorded direct reviewer to the caller context established by the controller.
async fn browser_and_mcp_mixed_queue_project_the_same_records_and_resolution() {
    use borg_runtime::collaboration::questions::*;
    let c = Controller::open_with_config(path(), workflow_config()).unwrap();
    let (t, _, caller) = mcp_actor(&c, "mixed-parity-agent", true).await;
    let viewer = PrincipalRef::Agent { agent_id: t.assigned_agent_id.clone() };
    let d = create(&c, "mixed-parity-doc", "Shared <subject> & exact bytes").await;
    let q = match call(&c, Operation::MutateQuestion {
        operation_id: "mixed-parity-question".into(),
        mutation: QuestionMutation::Pose { question: PoseQuestion {
            record: RecordRef::Document { document_id: d.document.id.clone() },
            recipient: viewer.clone(), prompt: "Which version should be retained?".into(),
            options: vec![
                QuestionOption { id: "keep".into(), label: "Keep".into(), explanation: "Keep this exact version".into() },
                QuestionOption { id: "revise".into(), label: "Revise".into(), explanation: "Prepare a separate version".into() },
            ], recommended_option_id: "keep".into(),
        } },
    }).await {
        Outcome::Ok { data: Data::QuestionMutation(value) } => value.question,
        other => panic!("expected question: {other:?}"),
    };
    let target = ReviewTarget::DocumentVersion { version: d.version.reference.clone() };
    let review = request(&c, "mixed-parity-review", target.clone(), viewer.clone()).await;
    let canonical = docs_queue(call(&c, Operation::QueryQuestion {
        query: QuestionQuery::DecisionQueue { viewer: viewer.clone(), after: None, limit: 4 },
    }).await);
    let mcp = docs_queue(call(&c, tool(&caller, "mixed-queue-read", "collab_decisions_list", json!({"after":null,"limit":4}))).await);
    assert_eq!(mcp, canonical);
    assert_eq!(canonical.items.len(), 2);
    assert!(canonical.items.iter().any(|item| matches!(item, DecisionQueueItem::Question { question_id, .. } if question_id == &q.question.id)));
    assert!(canonical.items.iter().any(|item| matches!(item, DecisionQueueItem::Review { review_id, target: actual, .. } if review_id == &review.review.id && actual == &target)));
    let queue_path = format!("/decisions?recipient_agent_id={}", docs_encode(&t.assigned_agent_id));
    let browser = docs_browser(&c, "GET", &queue_path, vec![]).await;
    assert_eq!(browser.status, 200, "{}", browser.body);
    assert!(browser.body.contains(&format!("/questions/{}", q.question.id)));
    assert!(browser.body.contains(&format!("/reviews/{}", review.review.id)));
    let mcp_document = call(&c, tool(&caller, "mixed-document-read", "collab_document_get", json!({"document_id":d.document.id,"version":1}))).await;
    assert_eq!(mcp_document, Outcome::Ok { data: Data::DocumentQuery(DocumentQueryResult::Document(d.clone())) });
    let browser_document = docs_browser(&c, "GET", &format!("/documents/{}?version=1", d.document.id), vec![]).await;
    assert_eq!(browser_document.status, 200);
    assert!(browser_document.body.contains("Shared &lt;subject&gt; &amp; exact bytes"));
    assert_eq!(call(&c, tool(&caller, "mixed-review-read", "collab_review_get", json!({"review_id":review.review.id}))).await,
        Outcome::Ok { data: Data::ReviewQuery(ReviewQueryResult::Review(review.clone())) });
    // Selecting an agent queue is a read; it does not impersonate that reviewer.
    let denied = docs_browser(&c, "POST", &format!("/reviews/{}/decide", review.review.id), docs_form(&[
        ("operation_id", "mixed-browser-wrong-reviewer".into()), ("expected_revision", review.review.revision.to_string()),
        ("verdict", "approved".into()), ("explanation", "This view grants no reviewer identity".into()),
    ])).await;
    assert_eq!(denied.status, 409, "{}", denied.body);
    assert!(denied.body.contains("wrong_reviewer"));
    assert_eq!(get_review(&c, &review.review.id).await, review);
    let decided = rev(call(&c, tool(&caller, "mixed-agent-review-decide", "collab_review_decide", json!({
        "review_id":review.review.id,"expected_revision":review.review.revision,
        "verdict":"approved","explanation":"Exact subject accepted as a product review"
    }))).await);
    let decision = decided.decision.as_ref().unwrap();
    assert_eq!(decision.target, target);
    assert_eq!(decision.reviewer.principal, viewer);
    assert!(matches!(&decision.reviewer.provenance, ActorProvenance::BoundAgentSession { task_id, attempt_id, .. }
        if task_id == &caller.task_id && attempt_id == &caller.attempt_id));
    let remaining = docs_queue(call(&c, tool(&caller, "mixed-queue-after-review", "collab_decisions_list", json!({"after":null,"limit":4}))).await);
    assert!(matches!(remaining.items.as_slice(), [DecisionQueueItem::Question { question_id, .. }] if question_id == &q.question.id));
    let browser = docs_browser(&c, "GET", &queue_path, vec![]).await;
    assert!(browser.body.contains(&format!("/questions/{}", q.question.id)));
    assert!(!browser.body.contains(&format!("/reviews/{}", review.review.id)));
    let answered = call(&c, tool(&caller, "mixed-agent-answer", "collab_question_answer", json!({
        "question_id":q.question.id,"expected_revision":q.question.revision,
        "answer":{"option_id":"keep","explanation":"Keep the exact recorded version"}
    }))).await;
    assert!(matches!(answered, Outcome::Ok { data: Data::QuestionMutation(_) }), "{answered:?}");
    assert!(docs_queue(call(&c, tool(&caller, "mixed-queue-empty", "collab_decisions_list", json!({"after":null,"limit":4}))).await).items.is_empty());
    let browser = docs_browser(&c, "GET", &queue_path, vec![]).await;
    assert!(!browser.body.contains(&format!("/questions/{}", q.question.id)));
    assert!(!browser.body.contains(&format!("/reviews/{}", review.review.id)));
}

#[tokio::test]
//= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
//= type=test
//# A mutation of an existing collaboration record MUST compare the expected record revision.
//= spec/impl/v2/implementation.md#document-versions-and-comments-are-immutable
//= type=test
//# The controller MUST refuse a supplied document content identity that differs from its computed content identity.
//= spec/impl/v2/implementation.md#reviews-bind-the-exact-subject-version
//= type=test
//# A review MUST bind the exact immutable subject version or result reference it evaluates.
async fn browser_head_change_cannot_be_overwritten_or_relabelled_by_stale_mcp_content() {
    let c = Controller::open_with_config(path(), workflow_config()).unwrap();
    let (_, _, caller) = mcp_actor(&c, "head-parity-agent", true).await;
    let first = create(&c, "head-parity-doc", "original subject").await;
    let old_review = request(&c, "head-parity-review", ReviewTarget::DocumentVersion { version: first.version.reference.clone() }, PrincipalRef::TrustedHostOperator).await;
    let route = format!("/documents/{}/versions", first.document.id);
    let edited = docs_browser(&c, "POST", &route, docs_form(&[
        ("operation_id", "head-parity-browser-edit".into()), ("expected_revision", first.document.revision.to_string()),
        ("title", "Browser successor".into()), ("body", "new authoritative version".into()),
    ])).await;
    assert_eq!(edited.status, 303, "{}", edited.body);
    let current = read(&c, &first.document.id, None).await;
    assert_eq!(current.version.reference.version, 2);
    let stale = json!({"document_id":first.document.id,"expected_revision":first.document.revision,
        "content":{"title":"Stale overwrite","body":"lost update"},"supplied_content_id":null});
    assert_eq!(code(call(&c, tool(&caller, "head-parity-mcp-stale", "collab_document_version_create", stale)).await), "revision_conflict");
    let mislabeled = json!({"document_id":first.document.id,"expected_revision":current.document.revision,
        "content":{"title":"Mislabeled","body":"different bytes"},"supplied_content_id":first.version.reference.content_id});
    assert_eq!(code(call(&c, tool(&caller, "head-parity-mcp-mismatch", "collab_document_version_create", mislabeled)).await), "content_identity_mismatch");
    let mut forged = current.version.reference.clone();
    forged.content_id = first.version.reference.content_id.clone();
    assert!(matches!(call(&c, tool(&caller, "head-parity-forged-review", "collab_review_create", json!({
        "target":ReviewTarget::DocumentVersion { version: forged },"reviewer":PrincipalRef::TrustedHostOperator,
        "summary":"Reject invented version/content association"
    }))).await, Outcome::Refused { .. }));
    assert_eq!(read(&c, &first.document.id, None).await, current);
    assert_eq!(read(&c, &first.document.id, Some(1)).await.version, first.version);
    assert_eq!(get_review(&c, &old_review.review.id).await, old_review);
    assert_eq!(code(call(&c, Operation::QueryDocument { query: DocumentQuery::Get { document_id:first.document.id,version:Some(3) } }).await), "not_found");
}

fn docs_json(response: borg_runtime::product::UiResponse) -> serde_json::Value {
    assert_eq!(response.status, 200, "{}", response.body);
    assert_eq!(response.content_type, "application/json");
    serde_json::from_str(&response.body).unwrap()
}
fn docs_browser_numbers(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Number(n) => *value = serde_json::Value::String(n.to_string()),
        serde_json::Value::Array(items) => items.iter_mut().for_each(docs_browser_numbers),
        serde_json::Value::Object(fields) => fields.values_mut().for_each(docs_browser_numbers),
        _ => {},
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The collaboration product MUST use the platform's task state and event interfaces.
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST update displayed state without requiring a manual page reload.
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST display a live activity feed on the right side of the page.
async fn workflow_live_route_joins_canonical_events_and_preserves_legacy_cursor_across_restart() {
    use borg_runtime::collaboration::questions::*;
    let p = path(); let cfg = workflow_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let legacy_task = submit(&c, "live-legacy-task", native("live-legacy-agent")).await;
    let legacy_before = docs_json(docs_browser(&c, "GET", "/ui/activity", vec![]).await);
    assert!(legacy_before["items"].as_array().unwrap().iter().any(|row| row["task_id"] == legacy_task.id));
    assert!(legacy_before["cursor"].is_string());
    let initial_revision = c.wait_for_change(0).await.unwrap().revision;
    let d = create(&c, "live-canonical-doc", "First live version").await;
    let q = match call(&c, Operation::MutateQuestion {
        operation_id: "live-canonical-question".into(),
        mutation: QuestionMutation::Pose { question: PoseQuestion {
            record: RecordRef::Document { document_id: d.document.id.clone() },
            recipient: PrincipalRef::TrustedHostOperator, prompt: "Keep this subject?".into(),
            options: vec![QuestionOption { id: "keep".into(), label: "Keep".into(), explanation: "Preserve it".into() },
                QuestionOption { id: "change".into(), label: "Change".into(), explanation: "Prepare a new version".into() }],
            recommended_option_id: "keep".into(),
        } },
    }).await { Outcome::Ok { data: Data::QuestionMutation(value) } => value.question, other => panic!("{other:?}") };
    let review = request(&c, "live-canonical-review", ReviewTarget::DocumentVersion { version: d.version.reference.clone() }, PrincipalRef::TrustedHostOperator).await;
    let second = doc(call(&c, append("live-canonical-edit", &d, "Second live version")).await);
    let decided = rev(decide(&c, "live-canonical-decide", &review, ReviewVerdict::Approved).await);
    assert_eq!(decided.decision.as_ref().unwrap().target, review.review.target);
    let changes = docs_json(docs_browser(&c, "GET", &format!("/ui/changes?after={initial_revision}"), vec![]).await);
    assert_eq!(changes["changed"], true);
    assert!(changes["revision"].as_str().unwrap().parse::<u64>().unwrap() > initial_revision);
    // Compare each real browser page with the canonical controller page at the
    // same cursor. The independent legacy sequence never becomes a workflow ID.
    let mut cursor: Option<String> = None;
    let mut rows = Vec::new(); let mut pages = 0;
    loop {
        let limit = c.limits().product.page_items.min(c.limits().workflow.as_ref().unwrap().page_items);
        let canonical = match call(&c, Operation::QueryWorkflowActivity { cursor: cursor.clone(), limit }).await {
            Outcome::Ok { data: Data::WorkflowActivity(value) } => value, other => panic!("{other:?}"),
        };
        let route = cursor.as_ref().map_or_else(|| "/ui/workflow-activity".into(), |c| format!("/ui/workflow-activity?cursor={}", docs_encode(c)));
        let browser = docs_json(docs_browser(&c, "GET", &route, vec![]).await);
        let mut expected_items = serde_json::to_value(&canonical.items).unwrap(); docs_browser_numbers(&mut expected_items);
        let mut expected_head = serde_json::to_value(&canonical.through).unwrap(); docs_browser_numbers(&mut expected_head);
        assert_eq!(browser["items"], expected_items); assert_eq!(browser["through"], expected_head);
        assert_eq!(browser["has_more"], canonical.next_cursor.is_some());
        let expected_cursor = canonical.next_cursor.clone().or_else(|| canonical.through.as_ref().map(|id| serde_json::to_string(id).unwrap()));
        assert_eq!(browser["cursor"], serde_json::to_value(&expected_cursor).unwrap());
        rows.extend(canonical.items); cursor = expected_cursor; pages += 1;
        assert!(pages <= 8, "bounded fixture must finish durable catch-up");
        if canonical.next_cursor.is_none() { break; }
    }
    assert!(pages >= 2, "fixture must exercise a real workflow continuation");
    assert!(rows.iter().any(|event| event.subject == RecordRef::Document { document_id: d.document.id.clone() }));
    assert!(rows.iter().any(|event| event.subject == RecordRef::Question { question_id: q.question.id.clone() }));
    assert!(rows.iter().any(|event| event.subject == RecordRef::Review { review_id: review.review.id.clone() }));
    let legacy_after = docs_json(docs_browser(&c, "GET", "/ui/activity", vec![]).await);
    assert_eq!(legacy_after, legacy_before, "workflow-only commits must not fabricate legacy task history");
    let view = docs_browser(&c, "GET", &format!("/documents/{}", d.document.id), vec![]).await;
    assert_eq!(view.status, 200);
    let aside = view.body.split("<aside").nth(1).unwrap().split("</aside>").next().unwrap();
    for id in ["activity-items", "workflow-activity", "workflow-items"] { assert!(aside.contains(&format!("id=\"{id}\""))); }
    assert!(view.body.contains("data-live-refresh=\"true\""));
    assert!(view.body.contains("workflow_cursor_bytes"));
    let revision_before_restart = c.wait_for_change(0).await.unwrap().revision;
    drop(c); controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg).unwrap();
    let route = format!("/ui/workflow-activity?cursor={}", docs_encode(cursor.as_ref().unwrap()));
    let caught_up = docs_json(docs_browser(&c, "GET", &route, vec![]).await);
    assert!(caught_up["items"].as_array().unwrap().is_empty());
    assert_eq!(caught_up["cursor"], serde_json::to_value(&cursor).unwrap());
    let legacy_tail = docs_json(docs_browser(&c, "GET", &format!("/ui/activity?after={}", legacy_before["cursor"].as_str().unwrap()), vec![]).await);
    assert!(legacy_tail["items"].as_array().unwrap().is_empty()); assert_eq!(legacy_tail["cursor"], legacy_before["cursor"]);
    let third = doc(call(&c, append("live-after-restart", &second, "Committed after restart")).await);
    let changes = docs_json(docs_browser(&c, "GET", &format!("/ui/changes?after={revision_before_restart}"), vec![]).await);
    assert_eq!(changes["changed"], true);
    let new_tail = docs_json(docs_browser(&c, "GET", &route, vec![]).await);
    assert_eq!(new_tail["items"].as_array().unwrap().len(), 1);
    assert_eq!(new_tail["items"][0]["subject"]["document_id"], third.document.id);
    assert_eq!(new_tail["items"][0]["subject_revision"], third.document.revision.to_string());
    assert_ne!(new_tail["cursor"], caught_up["cursor"]);
    let updated = docs_browser(&c, "GET", &format!("/documents/{}", third.document.id), vec![]).await;
    assert!(updated.body.contains("Committed after restart"));
}

#[tokio::test]
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# An operation refused by a configured limit MUST identify the limiting metadata field.
async fn workflow_live_route_cursor_refusals_are_bounded_and_do_not_change_committed_records() {
    let mut cfg = workflow_config();
    let metadata = Arc::make_mut(&mut cfg.limits);
    metadata.product.request_path_bytes = metadata.product.request_path_bytes.max(metadata.controller.collaboration_cursor_bytes + 128);
    let c = Controller::open_with_config(path(), cfg).unwrap();
    let d = create(&c, "live-bounds-doc", "Bounded subject").await;
    let before = docs_json(docs_browser(&c, "GET", "/ui/workflow-activity", vec![]).await);
    let long = "x".repeat(c.limits().controller.collaboration_cursor_bytes + 1);
    let response = docs_browser(&c, "GET", &format!("/ui/workflow-activity?cursor={long}"), vec![]).await;
    assert_eq!(response.status, 400, "{}", response.body);
    assert!(response.body.contains("controller.collaboration_cursor_bytes"));
    for query in ["cursor=", "after=1", "cursor=not-json"] {
        let response = docs_browser(&c, "GET", &format!("/ui/workflow-activity?{query}"), vec![]).await;
        assert!(matches!(response.status, 400 | 409), "{}", response.body);
        assert!(!response.body.contains("Configured limit exceeded"), "malformed input is not a configured-bound failure");
    }
    assert_eq!(read(&c, &d.document.id, None).await, d);
    assert_eq!(docs_json(docs_browser(&c, "GET", "/ui/workflow-activity", vec![]).await), before);
}

fn rewrite_retained_activity(p:&std::path::Path,revision:u64,change:impl FnOnce(&mut WorkflowActivity)) {
    let db=rusqlite::Connection::open(p).unwrap();
    let raw:String=db.query_row("SELECT body FROM workflow_events WHERE product_revision=?1 AND ordinal=0",[revision as i64],|row|row.get(0)).unwrap();
    let mut event:WorkflowActivity=serde_json::from_str(&raw).unwrap();
    assert_eq!(event.id.product_revision,revision);change(&mut event);
    assert_eq!(db.execute("UPDATE workflow_events SET body=?2 WHERE product_revision=?1 AND ordinal=0",
        rusqlite::params![revision as i64,serde_json::to_string(&event).unwrap()]).unwrap(),1);
    db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();drop(db);
}
async fn prove_workflow_fixture_reopens(p:&std::path::Path,cfg:&ControllerConfig) {
    let c=Controller::open_with_config(p,cfg.clone()).expect("valid original workflow must reopen before corruption");
    drop(c);controller_owner_released(p).await;
}

#[tokio::test]
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# Corrupt stored task state MUST be refused without rewriting its data.
async fn retained_task_owner_events_bind_coordination_receipts_actor_and_previous_owner_chain() {
    use borg_runtime::collaboration::work::WorkMutation;
    for defect in ["previous_owner","current_owner","coordination_revision","agent_actor","host_actor","truncated"] {
        let p=path();let cfg=workflow_config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
        let (task,_,caller)=mcp_actor(&c,"retained-owner-agent",true).await;
        let accepted=get(&c,&task.id).await;
        let other=submit(&c,"retained-other-task",native("retained-other-agent")).await;
        let first=match call(&c,tool(&caller,"retained-owner-first","collab_task_owner",json!({
            "task_id":task.id,"expected_revision":0,"owner":PrincipalRef::TrustedHostOperator}))).await {
            Outcome::Ok{data:Data::WorkMutation(value)}=>value,other=>panic!("bound agent owner change expected: {other:?}"),
        };
        let second=match call(&c,Operation::MutateWork{operation_id:"retained-owner-second".into(),mutation:WorkMutation::ChangeOwner{
            task_id:task.id.clone(),expected_revision:first.view.coordination_revision,owner:PrincipalRef::Agent{agent_id:task.assigned_agent_id.clone()}}}).await {
            Outcome::Ok{data:Data::WorkMutation(value)}=>value,other=>panic!("operator owner change expected: {other:?}"),
        };
        assert_eq!(first.view.coordination_revision,1);assert_eq!(second.view.coordination_revision,2);
        assert_eq!(get(&c,&task.id).await,accepted,"coordination cannot alter accepted execution identity or result");
        drop(c);controller_owner_released(&p).await;prove_workflow_fixture_reopens(&p,&cfg).await;
        let revision=if defect=="host_actor"{second.product_revision}else{first.product_revision};
        rewrite_retained_activity(&p,revision,|event| {
            match defect {
                "previous_owner"=>if let ActivityDetail::Task(TaskChange::OwnerChanged{previous,..})=&mut event.detail {
                    *previous=PrincipalRef::Agent{agent_id:other.assigned_agent_id.clone()};
                }else{panic!("owner event expected")},
                "current_owner"=>if let ActivityDetail::Task(TaskChange::OwnerChanged{current,..})=&mut event.detail {
                    *current=PrincipalRef::Agent{agent_id:other.assigned_agent_id.clone()};
                }else{panic!("owner event expected")},
                "coordination_revision"=>event.subject_revision+=1,
                "agent_actor"=>event.actor=RecordedActor{principal:PrincipalRef::TrustedHostOperator,
                    provenance:ActorProvenance::TrustedHost{path:TrustedHostPath::ControllerMaintenance}},
                "host_actor"=>event.actor=RecordedActor{principal:PrincipalRef::Agent{agent_id:task.assigned_agent_id.clone()},
                    provenance:ActorProvenance::BoundAgentSession{agent_id:task.assigned_agent_id.clone(),session_id:task.session_id.clone(),
                        task_id:caller.task_id.clone(),attempt_id:caller.attempt_id.clone(),generation:caller.generation,
                        worker_id:caller.worker_id.clone(),incarnation:caller.incarnation.clone()}},
                "truncated"=>event.truncated=true,
                _=>unreachable!(),
            }
        });
        assert_corrupt_reopen_unchanged(&p,cfg,defect);
    }
}

async fn retained_agent_comment_store()->(PathBuf,ControllerConfig,RecordedComment,DocumentView) {
    let p=path();let cfg=workflow_config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let (task,_,caller)=mcp_actor(&c,"retained-comment-agent",true).await;
    let document=doc(call(&c,tool(&caller,"retained-agent-document","collab_document_create",json!({
        "content":{"title":"Original comment subject","body":"immutable"},"supplied_content_id":null,
        "responsible_owner":PrincipalRef::Agent{agent_id:task.assigned_agent_id}}))).await);
    let other=create(&c,"retained-comment-other-document","independent valid subject").await;
    let comment=match call(&c,tool(&caller,"retained-agent-comment","collab_comment_post",json!({
        "record":RecordRef::Document{document_id:document.document.id},"expected_revision":document.document.revision,
        "body":"é".repeat(750)}))).await {
        Outcome::Ok{data:Data::DocumentMutation(DocumentMutationOutcome::Comment{comment,..})}=>comment,
        other=>panic!("retained bound comment expected: {other:?}"),
    };
    drop(c);controller_owner_released(&p).await;(p,cfg,comment,other)
}

#[tokio::test]
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# Corrupt stored task state MUST be refused without rewriting its data.
async fn retained_comment_events_match_immutable_occurrence_and_original_projection() {
    for defect in ["subject","comment_id","revision","actor","text","truncated_flag","body_flag"] {
        let (p,cfg,comment,other)=retained_agent_comment_store().await;
        prove_workflow_fixture_reopens(&p,&cfg).await;
        rewrite_retained_activity(&p,comment.product_revision,|event| {
            assert_eq!(event.actor,comment.author);
            let ActivityDetail::Document(DocumentChange::CommentAdded{comment_id,body})=&mut event.detail else{panic!("comment event expected")};
            assert!(body.truncated&&event.truncated);assert!(comment.body.starts_with(&body.text));
            assert!(body.text.len()<comment.body.len());
            match defect {
                "subject"=>event.subject=RecordRef::Document{document_id:other.document.id.clone()},
                "comment_id"=>*comment_id="absent-comment".into(),
                "revision"=>event.subject_revision+=1,
                "actor"=>event.actor=RecordedActor{principal:PrincipalRef::TrustedHostOperator,
                    provenance:ActorProvenance::TrustedHost{path:TrustedHostPath::ControllerMaintenance}},
                "text"=>body.text="not the immutable prefix".into(),
                "truncated_flag"=>event.truncated=false,
                "body_flag"=>{body.truncated=false;event.truncated=false;},
                _=>unreachable!(),
            }
        });
        assert_corrupt_reopen_unchanged(&p,cfg,defect);
    }
}

#[tokio::test]
async fn retained_comment_projection_is_not_recomputed_from_a_later_event_limit() {
    let (p,mut cfg,comment,_)=retained_agent_comment_store().await;
    let before={let db=rusqlite::Connection::open(&p).unwrap();db.query_row(
        "SELECT body FROM workflow_events WHERE product_revision=?1 AND ordinal=0",[comment.product_revision as i64],|r|r.get::<_,String>(0)).unwrap()};
    let original:WorkflowActivity=serde_json::from_str(&before).unwrap();
    let ActivityDetail::Document(DocumentChange::CommentAdded{body,..})=&original.detail else{panic!("comment event expected")};
    assert!(body.truncated&&body.text.len()<comment.body.len());
    // The later limit could fit the whole original body, but cannot revise a committed projection.
    Arc::make_mut(&mut cfg.limits).workflow.as_mut().unwrap().event_bytes=8192;
    prove_workflow_fixture_reopens(&p,&cfg).await;
    let db=rusqlite::Connection::open(&p).unwrap();let after:String=db.query_row(
        "SELECT body FROM workflow_events WHERE product_revision=?1 AND ordinal=0",[comment.product_revision as i64],|r|r.get(0)).unwrap();
    assert_eq!(after,before);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//= type=test
//# Corrupt stored task state MUST be refused without rewriting its data.
async fn retained_review_resolution_routes_to_exact_review_even_when_document_has_same_owner() {
    let p=path();let cfg=workflow_config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let document=create(&c,"retained-routing-document","Reviewed bytes").await;
    let review=request(&c,"retained-routing-review",ReviewTarget::DocumentVersion{version:document.version.reference.clone()},PrincipalRef::TrustedHostOperator).await;
    let decided=rev(decide(&c,"retained-routing-decide",&review,ReviewVerdict::Approved).await);
    let revision=decided.decision.as_ref().unwrap().product_revision;
    drop(c);controller_owner_released(&p).await;prove_workflow_fixture_reopens(&p,&cfg).await;
    rewrite_retained_activity(&p,revision,|event|{
        assert_eq!(event.subject,RecordRef::Review{review_id:review.review.id.clone()});
        let routing=event.routing.as_mut().expect("canonical review resolution route");
        assert_eq!(routing.linked_record,event.subject);
        // Same current owner does not make another entity the original routing record.
        routing.linked_record=RecordRef::Document{document_id:document.document.id.clone()};
    });
    assert_corrupt_reopen_unchanged(&p,cfg,"review route substituted with target document");
}

#[tokio::test]
async fn concurrent_document_edit_and_review_decision_keep_exact_subject_after_restart() {
    async fn full(c: &Controller, command: Operation) -> Reply {
        let reply = Box::pin(c.handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: format!("review-edit-race-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
            command: command.clone(),
        })).await;
        reply.validate_for(&command).unwrap();
        reply
    }
    async fn discard_response(c: &Controller, command: Operation) {
        // Deliberately return no canonical response to the test consumer.
        let _ = Box::pin(c.handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: format!("review-edit-discard-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
            command,
        })).await;
    }
    async fn recovered(c: &Controller, command: &Operation) -> (Reply, Outcome) {
        let id = match command {
            Operation::MutateDocument { operation_id, .. } | Operation::MutateReview { operation_id, .. } => operation_id,
            _ => panic!("expected original mutation"),
        };
        let lookup = full(c, Operation::GetOperation { operation_id: id.clone() }).await;
        let Outcome::Ok { data: Data::Operation(record) } = &lookup.outcome else { panic!("{lookup:?}") };
        assert_eq!(&record.id, id); assert_eq!(record.state, "settled");
        assert_eq!(record.intended, serde_json::to_value(command).unwrap());
        assert_eq!(&lookup.commit.as_ref().unwrap().operation_id, id);
        let outcome = serde_json::from_value(record.outcome.clone().unwrap()).unwrap();
        (lookup, outcome)
    }
    let p = path(); let cfg = workflow_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let first = create(&c, "edit-review-race-create", "Immutable reviewed version").await;
    let target = ReviewTarget::DocumentVersion { version: first.version.reference.clone() };
    let open = request(&c, "edit-review-race-request", target.clone(), PrincipalRef::TrustedHostOperator).await;

    // Archive is outside the selected API. Exercise its unsupported browser
    // route while an actual review is open; this proves refusal/no effect only,
    // not an implemented archive operation or pruning policy.
    let archive_id = "open-review-unsupported-archive";
    let refused_archive = docs_browser(&c, "POST", &format!("/documents/{}/archive", first.document.id), docs_form(&[
        ("operation_id", archive_id.into()), ("expected_revision", first.document.revision.to_string()),
    ])).await;
    assert_eq!(refused_archive.status, 404, "{}", refused_archive.body);
    assert_eq!(read(&c, &first.document.id, None).await, first);
    assert_eq!(get_review(&c, &open.review.id).await, open);
    assert_eq!(code(call(&c, Operation::GetOperation { operation_id: archive_id.into() }).await), "not_found");

    let edit = append("edit-review-race-edit", &first, "Independent new version");
    let decision = Operation::MutateReview { operation_id: "edit-review-race-decide".into(), mutation: ReviewMutation::Decide {
        review_id: open.review.id.clone(), expected_revision: open.review.revision,
        verdict: ReviewVerdict::Approved, explanation: "Only the original immutable target is approved".into(),
    }};
    // Both responses are lost at the controlled canonical-handler-to-caller
    // adapter. Only original GetOperation exposes their first usable outcomes;
    // this is not an HTTP/TCP interruption claim.
    tokio::join!(discard_response(&c, edit.clone()), discard_response(&c, decision.clone()));
    let edited_reply = recovered(&c, &edit).await;
    let decided_reply = recovered(&c, &decision).await;
    let second = doc(edited_reply.1.clone());
    let settled = rev(decided_reply.1.clone());
    assert!(edited_reply.0.commit.is_some()); assert!(decided_reply.0.commit.is_some());
    assert_eq!(second.document.current.version, 2);
    assert_ne!(second.version.reference, first.version.reference);
    assert_eq!(settled.review.status, ReviewStatus::Decided);
    assert_eq!(settled.review.target, target);
    assert_eq!(settled.decision.as_ref().unwrap().target, target);
    assert_eq!(settled.decision.as_ref().unwrap().reviewer.principal, PrincipalRef::TrustedHostOperator);
    assert!(settled.withdrawal.is_none());
    assert_eq!(read(&c, &first.document.id, Some(1)).await.version, first.version);
    let new_review = request(&c, "edit-review-race-request-new", ReviewTarget::DocumentVersion { version: second.version.reference.clone() }, PrincipalRef::TrustedHostOperator).await;
    assert_eq!(new_review.review.status, ReviewStatus::Open);
    assert!(new_review.decision.is_none()); assert!(new_review.withdrawal.is_none());
    let mut changed = decision.clone();
    if let Operation::MutateReview { mutation: ReviewMutation::Decide { verdict, .. }, .. } = &mut changed { *verdict = ReviewVerdict::Rejected; }
    assert_eq!(code(call(&c, changed.clone()).await), "operation_conflict");
    assert_eq!(get_review(&c, &open.review.id).await, settled);
    let mut originals = Vec::new();
    for (id, operation, committed) in [
        ("edit-review-race-edit", &edit, &edited_reply),
        ("edit-review-race-decide", &decision, &decided_reply),
    ] {
        let lookup = full(&c, Operation::GetOperation { operation_id: id.into() }).await;
        let Outcome::Ok { data: Data::Operation(record) } = &lookup.outcome else { panic!("{lookup:?}") };
        assert_eq!(record.id, id); assert_eq!(record.state, "settled");
        assert_eq!(record.intended, serde_json::to_value(operation).unwrap());
        assert_eq!(serde_json::from_value::<Outcome>(record.outcome.clone().unwrap()).unwrap(), committed.1);
        assert_eq!(lookup.commit, committed.0.commit);
        originals.push(lookup);
    }
    drop(c); controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg).unwrap();
    assert_eq!(read(&c, &first.document.id, Some(1)).await.version, first.version);
    assert_eq!(read(&c, &first.document.id, None).await, second);
    assert_eq!(get_review(&c, &open.review.id).await, settled);
    assert_eq!(get_review(&c, &new_review.review.id).await, new_review);
    for ((operation, committed), original) in [(edit, edited_reply), (decision, decided_reply)].into_iter().zip(originals) {
        let replay = full(&c, operation).await;
        assert_eq!(replay.outcome, committed.1); assert_eq!(replay.commit, committed.0.commit);
        let lookup = full(&c, Operation::GetOperation { operation_id: committed.0.commit.unwrap().operation_id }).await;
        assert_eq!(lookup.outcome, original.outcome); assert_eq!(lookup.commit, original.commit);
    }
    assert_eq!(code(call(&c, changed).await), "operation_conflict");
    assert_eq!(get_review(&c, &open.review.id).await, settled);
    assert_eq!(get_review(&c, &new_review.review.id).await, new_review);
    assert_eq!(code(call(&c, Operation::QueryDocument { query: DocumentQuery::Get { document_id: first.document.id, version: Some(3) } }).await), "not_found");
}


// Traceability scope: A committed Browser answer, original-receipt replay and reopen preserve the candidate and protected stage; no deployment is exercised.
//= spec/impl/v2/implementation.md#collaboration-surfaces-preserve-existing-authority
//= type=test
//# A product question or review decision MUST NOT confer protected rollout authority.

#[tokio::test]
async fn answered_product_question_preserves_original_release_and_unconfigured_rollout() {
    use borg_runtime::collaboration::questions::*;
    async fn stage(c: &Controller, id: &str) -> borg_runtime::protected_release::StageView {
        match call(
            c,
            Operation::GetProtectedRelease {
                release_id: id.into(),
            },
        )
        .await
        {
            Outcome::Ok {
                data: Data::ProtectedRelease(value),
            } => *value,
            other => panic!("protected release view expected: {other:?}"),
        }
    }
    async fn full(c: &Controller, command: Operation) -> Reply {
        let reply = Box::pin(c.handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: format!("question-release-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
            command: command.clone(),
        }))
        .await;
        reply.validate_for(&command).unwrap();
        reply
    }
    async fn access_binding(
        c: &Controller,
        id: &str,
    ) -> (
        TaskInput,
        String,
        String,
        borg_runtime::repository_catalogue::Entry,
    ) {
        let task = get(c, id).await;
        let binding = task
            .input
            .repository
            .as_ref()
            .unwrap()
            .registration
            .as_ref()
            .unwrap();
        let entry = match call(
            c,
            Operation::GetRepository {
                repository_id: binding.repository_id.clone(),
                registration_revision: Some(binding.registration_revision.clone()),
            },
        )
        .await
        {
            Outcome::Ok {
                data: Data::RepositoryRegistration(value),
            } => value,
            other => panic!("accepted repository binding expected: {other:?}"),
        };
        (task.input, task.session_id, task.assigned_agent_id, entry)
    }
    let p = path();
    let cfg = workflow_config();
    assert!(cfg.protected_release.is_none());
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    // Existing controlled checkpoint reports and metadata-only artifact claim;
    // no package validation, Git publication, host/provider access or rollout.
    let accepted = accepted_release_task(&c, "question-release-source").await;
    let original = release(
        call(
            &c,
            Operation::PrepareRelease(release_input(&accepted, "question-release-candidate")),
        )
        .await,
    );
    assert_unqualified(&original);
    let before = stage(&c, &original.id).await;
    // Reread owned bindings, not external ACLs, private storage keys or secrets.
    // TaskInput includes the accepted definition/profile/inference/repository.
    let access_before = access_binding(&c, &accepted.id).await;
    assert_eq!(access_before.0, accepted.input);
    assert_eq!(before.candidate, original);
    assert!(!before.configured && before.policy.is_none());
    assert!(before.records.is_empty() && before.actions.is_empty());
    assert!(!before.automatic_rollout);
    let posed = match call(&c, Operation::MutateQuestion {
        operation_id: "question-release-pose".into(),
        mutation: QuestionMutation::Pose { question: PoseQuestion {
            record: RecordRef::Task { task_id: accepted.id.clone() },
            recipient: PrincipalRef::TrustedHostOperator,
            prompt: "Should this candidate proceed through the separate release process?".into(),
            options: vec![
                QuestionOption { id: "proceed".into(), label: "Proceed with candidate".into(),
                    explanation: "Continue the ordinary workflow; protected checks and authorization remain separate".into() },
                QuestionOption { id: "hold".into(), label: "Hold candidate".into(),
                    explanation: "Keep the candidate for later consideration".into() },
            ],
            recommended_option_id: "hold".into(),
        } },
    }).await {
        Outcome::Ok { data: Data::QuestionMutation(value) } => value.question,
        other => panic!("question expected: {other:?}"),
    };
    assert_eq!(posed.question.status, QuestionStatus::Open);
    assert!(posed.active_answer.is_none());
    let activity_before = withdrawal_activity(&c).await;
    let answer_path = format!("/questions/{}/answer", posed.question.id);
    let answer_form = docs_form(&[
        ("operation_id", "question-release-answer".into()),
        ("expected_revision", posed.question.revision.to_string()),
        ("option_id", "proceed".into()),
        (
            "explanation",
            "Continue with this candidate as a product decision".into(),
        ),
    ]);
    let response = docs_browser(&c, "POST", &answer_path, answer_form.clone()).await;
    assert_eq!(response.status, 303, "{}", response.body);
    let answered = match call(
        &c,
        Operation::QueryQuestion {
            query: QuestionQuery::Get {
                question_id: posed.question.id.clone(),
            },
        },
    )
    .await
    {
        Outcome::Ok {
            data: Data::QuestionQuery(QuestionQueryResult::Question(value)),
        } => value,
        other => panic!("answered question expected: {other:?}"),
    };
    assert_eq!(answered.question.status, QuestionStatus::Answered);
    assert_eq!(answered.question.posed, posed.question.posed);
    assert_eq!(answered.question.revision, posed.question.revision + 1);
    let answer = answered.active_answer.as_ref().unwrap();
    assert_eq!(answer.answer.option_id, "proceed");
    assert_eq!(
        answer.author,
        RecordedActor {
            principal: PrincipalRef::TrustedHostOperator,
            provenance: ActorProvenance::TrustedHost {
                path: TrustedHostPath::Browser
            },
        }
    );
    let activity_after = withdrawal_activity(&c).await;
    assert_eq!(activity_after.items.len(), activity_before.items.len() + 1);
    assert_eq!(
        &activity_after.items[..activity_before.items.len()],
        activity_before.items.as_slice()
    );
    let event = activity_after.items.last().unwrap();
    assert_eq!(
        event.subject,
        RecordRef::Question {
            question_id: posed.question.id.clone()
        }
    );
    assert_eq!(event.subject_revision, answered.question.revision);
    assert_eq!(event.actor, answer.author);
    assert_eq!(event.id.product_revision, answer.product_revision);
    assert_eq!(
        event.detail,
        ActivityDetail::Question(QuestionChange::Closed {
            resolution: QuestionResolution::Answered {
                question_id: posed.question.id.clone(),
                answer: AnswerRef {
                    question_id: posed.question.id.clone(),
                    answer_id: answer.id.clone()
                },
                selected_option_id: "proceed".into(),
            },
        })
    );
    let after = release(
        call(
            &c,
            Operation::GetRelease {
                release_id: original.id.clone(),
            },
        )
        .await,
    );
    assert_eq!(after, original);
    assert_unqualified(&after);
    assert_eq!(stage(&c, &original.id).await, before);
    assert_eq!(access_binding(&c, &accepted.id).await, access_before);
    let lookup = Operation::GetOperation {
        operation_id: "question-release-answer".into(),
    };
    let receipt = full(&c, lookup.clone()).await;
    let Outcome::Ok {
        data: Data::Operation(record),
    } = &receipt.outcome
    else {
        panic!("{receipt:?}")
    };
    assert_eq!(record.id, "question-release-answer");
    assert_eq!(record.state, "settled");
    assert!(receipt.commit.is_some());
    let Outcome::Ok {
        data: Data::QuestionMutation(saved),
    } = serde_json::from_value(record.outcome.clone().unwrap()).unwrap()
    else {
        panic!("original answer outcome expected")
    };
    assert_eq!(saved.question, answered);
    let replay = docs_browser(&c, "POST", &answer_path, answer_form).await;
    assert_eq!(replay.status, 303, "{}", replay.body);
    let replay_receipt = full(&c, lookup.clone()).await;
    assert_eq!(replay_receipt.outcome, receipt.outcome);
    assert_eq!(replay_receipt.commit, receipt.commit);
    assert_eq!(withdrawal_activity(&c).await, activity_after);
    assert_eq!(stage(&c, &original.id).await, before);
    assert_eq!(access_binding(&c, &accepted.id).await, access_before);
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg).unwrap();
    assert_eq!(
        call(
            &c,
            Operation::QueryQuestion {
                query: QuestionQuery::Get {
                    question_id: posed.question.id
                },
            }
        )
        .await,
        Outcome::Ok {
            data: Data::QuestionQuery(QuestionQueryResult::Question(answered))
        }
    );
    assert_eq!(stage(&c, &original.id).await, before);
    assert_eq!(access_binding(&c, &accepted.id).await, access_before);
    assert_eq!(withdrawal_activity(&c).await, activity_after);
    let reopened = full(&c, lookup).await;
    assert_eq!(reopened.outcome, receipt.outcome);
    assert_eq!(reopened.commit, receipt.commit);
}


// Traceability scope: Fresh owned configuration and tool/server projections remain equal around successful answer and review; private/external access is not measured.
//= spec/impl/v2/implementation.md#collaboration-surfaces-preserve-existing-authority
//= type=test
//# A product question or review decision MUST NOT grant host, repository, provider or credential access.

#[tokio::test]
async fn successful_question_and_review_preserve_owned_access_configuration() {
    use borg_runtime::collaboration::{Query, QueryResult, questions::*};
    use std::path::Path;

    async fn query(c: &Controller, query: Query) -> QueryResult {
        match call(c, Operation::QueryCollaboration { query }).await {
            Outcome::Ok {
                data: Data::Collaboration(value),
            } => value,
            other => panic!("canonical configuration query expected: {other:?}"),
        }
    }
    async fn question(c: &Controller, id: &str) -> QuestionView {
        match call(
            c,
            Operation::QueryQuestion {
                query: QuestionQuery::Get {
                    question_id: id.into(),
                },
            },
        )
        .await
        {
            Outcome::Ok {
                data: Data::QuestionQuery(QuestionQueryResult::Question(value)),
            } => value,
            other => panic!("question view expected: {other:?}"),
        }
    }
    async fn original(c: &Controller, id: &str) -> Reply {
        let command = Operation::GetOperation {
            operation_id: id.into(),
        };
        let reply = Box::pin(c.handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: format!("access-original-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
            command: command.clone(),
        }))
        .await;
        reply.validate_for(&command).unwrap();
        let Outcome::Ok {
            data: Data::Operation(record),
        } = &reply.outcome
        else {
            panic!("{reply:?}")
        };
        assert_eq!(record.id, id);
        assert_eq!(record.state, "settled");
        assert!(record.outcome.is_some() && reply.commit.is_some());
        reply
    }
    async fn snapshot(c: &Controller, id: &str) -> serde_json::Value {
        // Every projection is derived from a fresh canonical read. These are
        // owned settings and selectors, not remote permissions or secret values.
        let task = get(c, id).await;
        let session = Box::pin(query(
            c,
            Query::Session {
                session_id: task.session_id.clone(),
            },
        ))
        .await;
        let QueryResult::Session(bound_session) = &session else {
            panic!("{session:?}")
        };
        assert_eq!(bound_session.task_id, task.id);
        assert_eq!(bound_session.agent_id, task.assigned_agent_id);
        assert_eq!(bound_session.definition, task.input.definition);
        let definition = Box::pin(query(
            c,
            Query::Definition {
                definition_id: task.input.definition.id.clone(),
                revision: task.input.definition.revision.clone(),
            },
        ))
        .await;
        assert_eq!(
            definition,
            QueryResult::Definition(task.input.definition.clone())
        );
        let agent = match call(
            c,
            Operation::GetAgent {
                agent_id: task.assigned_agent_id.clone(),
            },
        )
        .await
        {
            Outcome::Ok {
                data: Data::Agent(value),
            } => value,
            other => panic!("current agent definition expected: {other:?}"),
        };
        assert_eq!(agent.definition, task.input.definition);
        let limits = Box::pin(query(
            c,
            Query::Limits {
                task_id: Some(task.id.clone()),
            },
        ))
        .await;
        let QueryResult::Limits { execution, .. } = &limits else {
            panic!("{limits:?}")
        };
        assert_eq!(execution, &task.input.resolved_limits);
        let registration = task
            .input
            .repository
            .as_ref()
            .unwrap()
            .registration
            .as_ref()
            .unwrap();
        let selected = Box::pin(query(
            c,
            Query::RepositoryRegistration {
                repository_id: registration.repository_id.clone(),
                registration_revision: Some(registration.registration_revision.clone()),
            },
        ))
        .await;
        let current = Box::pin(query(
            c,
            Query::RepositoryRegistration {
                repository_id: registration.repository_id.clone(),
                registration_revision: None,
            },
        ))
        .await;
        assert_eq!(selected, current);
        let repositories = Box::pin(query(
            c,
            Query::Repositories {
                after: None,
                limit: 4,
            },
        ))
        .await;
        let QueryResult::Repositories(page) = &repositories else {
            panic!("{repositories:?}")
        };
        assert_eq!(page.items.len(), 1);
        assert!(page.next_cursor.is_none());
        assert_eq!(
            selected,
            QueryResult::RepositoryRegistration(page.items[0].clone())
        );
        let bound = task.input.resolved_limits.as_ref().unwrap();
        let execution = task.input.execution_limits().unwrap();
        let profile = task.input.definition.execution_profile.as_ref();
        assert!(matches!(
            profile,
            Some(ExecutionProfile::CollaborationV2 { .. })
        ));
        let tools = borg_runtime::mcp::catalogue_for_task(
            &bound.global_metadata,
            profile,
            task.input.instruction_source.as_ref(),
        );
        for required in ["collab_question_answer", "collab_review_decide"] {
            assert!(tools.iter().any(|tool| tool.name == required));
        }
        for forbidden in [
            "observe_collaboration_delivery",
            "prepare_collaboration_delivery",
            "begin_collaboration_delivery",
            "acknowledge_collaboration_delivery",
            "collab_rollout",
        ] {
            assert!(
                !tools.iter().any(|tool| tool.name == forbidden),
                "{forbidden}"
            );
        }
        // Fixed synthetic launch selectors only. No config file, process, key,
        // host access or provider request is created or inspected by this call.
        let server = borg_runtime::worker::codex_mcp::server_config_for_profile(
            Path::new("/fixture/borg-runtime"),
            Path::new("/fixture/collaboration.json"),
            &bound.global_metadata,
            &execution.collaboration,
            profile,
            60,
        );
        assert_eq!(server["env_vars"], json!(["OPENAI_API_KEY"]));
        for forbidden in ["default_tools_approval_mode", "approval_policy", "sandbox"] {
            assert!(server.get(forbidden).is_none(), "{forbidden}");
        }
        let approvals = server["tools"].as_object().unwrap();
        assert!(!approvals.contains_key("collab_task_delegate"));
        assert!(!approvals.contains_key("unrelated_write"));
        assert!(!approvals.contains_key("collab_call_get"));
        assert!(
            !server["enabled_tools"]
                .as_array()
                .unwrap()
                .contains(&json!("collab_task_delegate"))
        );
        json!({"input": task.input, "session": session, "definition": definition,
            "current_agent": agent, "global_and_accepted_limits": limits,
            "selected_repository": selected, "current_repository": current,
            "complete_repository_catalogue": repositories,
            "complete_tool_catalogue": tools, "complete_coding_server": server})
    }

    let p = path();
    let cfg = workflow_config();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let mut input = coding("owned-access-source");
    input.definition.execution_profile =
        Some(ExecutionProfile::CollaborationV2 { web_search: None });
    let submitted = submit(&c, "owned-access-submit", input).await;
    let worker = worker("owned-access-worker", "owned-access-incarnation", 0, 1);
    register(&c, &worker).await;
    let attempt = run(&commands(&c, &worker, vec![]).await);
    assert_eq!(attempt.task_id, submitted.id);
    // Existing controlled acknowledged checkpoint fixture, not Codex execution.
    let accepted = Box::pin(finish_controlled_coding(&c, &attempt, "owned-access")).await;
    let before = Box::pin(snapshot(&c, &accepted.id)).await;
    let posed = match call(
        &c,
        Operation::MutateQuestion {
            operation_id: "owned-access-pose".into(),
            mutation: QuestionMutation::Pose {
                question: PoseQuestion {
                    record: RecordRef::Task {
                        task_id: accepted.id.clone(),
                    },
                    recipient: PrincipalRef::TrustedHostOperator,
                    prompt: "Which recorded result should remain?".into(),
                    options: vec![QuestionOption {
                        id: "keep".into(),
                        label: "Keep".into(),
                        explanation: "Retain the accepted result".into(),
                    }],
                    recommended_option_id: "keep".into(),
                },
            },
        },
    )
    .await
    {
        Outcome::Ok {
            data: Data::QuestionMutation(value),
        } => value.question,
        other => panic!("question expected: {other:?}"),
    };
    let target = match call(
        &c,
        Operation::QueryReview {
            query: ReviewQuery::AcceptedTarget {
                task_id: accepted.id.clone(),
                scope: AcceptedReviewScope::Checkpoint,
            },
        },
    )
    .await
    {
        Outcome::Ok {
            data: Data::ReviewQuery(ReviewQueryResult::AcceptedTarget(value)),
        } => value,
        other => panic!("accepted review target expected: {other:?}"),
    };
    let requested = Box::pin(request(
        &c,
        "owned-access-review",
        target.clone(),
        PrincipalRef::TrustedHostOperator,
    ))
    .await;
    assert_eq!(posed.question.status, QuestionStatus::Open);
    assert_eq!(requested.review.status, ReviewStatus::Open);
    // Preparing the two records must not change the accepted configuration either.
    assert_eq!(Box::pin(snapshot(&c, &accepted.id)).await, before);
    let answer_path = format!("/questions/{}/answer", posed.question.id);
    let answer_form = docs_form(&[
        ("operation_id", "owned-access-answer".into()),
        ("expected_revision", posed.question.revision.to_string()),
        ("option_id", "keep".into()),
        ("explanation", "Keep the original accepted result".into()),
    ]);
    let response = Box::pin(docs_browser(&c, "POST", &answer_path, answer_form.clone())).await;
    assert_eq!(response.status, 303, "{}", response.body);
    let answered = Box::pin(question(&c, &posed.question.id)).await;
    assert_eq!(answered.question.status, QuestionStatus::Answered);
    assert_eq!(answered.question.revision, posed.question.revision + 1);
    assert_eq!(answered.question.posed, posed.question.posed);
    let author = RecordedActor {
        principal: PrincipalRef::TrustedHostOperator,
        provenance: ActorProvenance::TrustedHost {
            path: TrustedHostPath::Browser,
        },
    };
    assert_eq!(answered.active_answer.as_ref().unwrap().author, author);
    assert_eq!(
        answered.active_answer.as_ref().unwrap().answer.option_id,
        "keep"
    );
    let answer_receipt = Box::pin(original(&c, "owned-access-answer")).await;
    let after_answer = Box::pin(snapshot(&c, &accepted.id)).await;
    assert_eq!(after_answer, before);
    assert_eq!(
        Box::pin(get_review(&c, &requested.review.id)).await,
        requested
    );

    let review_path = format!("/reviews/{}/decide", requested.review.id);
    let review_form = docs_form(&[
        ("operation_id", "owned-access-decide".into()),
        ("expected_revision", requested.review.revision.to_string()),
        ("verdict", "approved".into()),
        (
            "explanation",
            "Approve this product result without granting access".into(),
        ),
    ]);
    let response = Box::pin(docs_browser(&c, "POST", &review_path, review_form.clone())).await;
    assert_eq!(response.status, 303, "{}", response.body);
    let decided = Box::pin(get_review(&c, &requested.review.id)).await;
    assert_eq!(decided.review.status, ReviewStatus::Decided);
    assert_eq!(decided.review.revision, requested.review.revision + 1);
    let decision = decided.decision.as_ref().unwrap();
    assert_eq!(decision.target, target);
    assert_eq!(decision.verdict, ReviewVerdict::Approved);
    assert_eq!(decision.reviewer, author);
    assert_eq!(decision.operation_id, "owned-access-decide");
    let review_receipt = Box::pin(original(&c, "owned-access-decide")).await;
    assert_ne!(review_receipt.commit, answer_receipt.commit);
    assert_eq!(Box::pin(snapshot(&c, &accepted.id)).await, after_answer);
    assert_eq!(Box::pin(question(&c, &posed.question.id)).await, answered);
    for (route, form, id, receipt) in [
        (
            &answer_path,
            answer_form,
            "owned-access-answer",
            &answer_receipt,
        ),
        (
            &review_path,
            review_form,
            "owned-access-decide",
            &review_receipt,
        ),
    ] {
        let response = Box::pin(docs_browser(&c, "POST", route, form)).await;
        assert_eq!(response.status, 303, "{}", response.body);
        let replay = Box::pin(original(&c, id)).await;
        assert_eq!(replay.outcome, receipt.outcome);
        assert_eq!(replay.commit, receipt.commit);
        assert_eq!(Box::pin(snapshot(&c, &accepted.id)).await, before);
    }
    assert_eq!(Box::pin(question(&c, &posed.question.id)).await, answered);
    assert_eq!(
        Box::pin(get_review(&c, &requested.review.id)).await,
        decided
    );
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg).unwrap();
    assert_eq!(Box::pin(snapshot(&c, &accepted.id)).await, before);
    assert_eq!(Box::pin(question(&c, &posed.question.id)).await, answered);
    assert_eq!(
        Box::pin(get_review(&c, &requested.review.id)).await,
        decided
    );
    for (id, receipt) in [
        ("owned-access-answer", answer_receipt),
        ("owned-access-decide", review_receipt),
    ] {
        let reopened = Box::pin(original(&c, id)).await;
        assert_eq!(reopened.outcome, receipt.outcome);
        assert_eq!(reopened.commit, receipt.commit);
    }
}
