//! Child of the existing charter controller fixture, using its real producers.
//! No SQL initializer, fabricated source record, or provider execution is used.
use super::*;
use borg_runtime::native_loop_contract::NativeRequestEvidence as Kind;
use borg_runtime::{native as n, native_loop_contract::NativeRequestObservedOutcome as Observed};

// Explicit controlled response facts, independent from the evidence vector under
// test. Reserve/begin remain nonterminal so other charter fixtures retain their
// unfinished/unknown semantics. The controlled request body is exactly b"{}".
#[derive(Clone, Copy)]
enum Response { Completed, Rejected }
async fn terminal(c:&Controller,who:&Caller,index:u32,response:Response) {
    let accepted=get(c,&who.task_id).await;
    let observed_outcome=match response {
        Response::Completed=>Observed::Completed,
        Response::Rejected=>Observed::KnownRejected {rejection_kind:n::RejectionKind::RateLimited},
    };
    let failure=if matches!(response,Response::Rejected) {
        Some(n::ProviderFailureDiagnostic::http(n::ProviderFailureClass::HttpRefused,n::HttpDiagnostic {
            status:n::HttpStatusDiagnostic::Value(429),body:n::DiagnosticBodyState::Complete,
            category:n::CategoryDiagnostic::Value(n::ProviderErrorCategory::RateLimited),
            scheduler_retry_after:n::SchedulerRetryDiagnostic::Value(n::RetryAfter::Absent),
            retry_after:n::RetryHeaderDiagnostic::Absent,retry_after_ms:n::NumericDiagnostic::Absent,
            requests_limit:n::NumericDiagnostic::Absent,requests_remaining:n::NumericDiagnostic::Absent,
            tokens_limit:n::NumericDiagnostic::Absent,tokens_remaining:n::NumericDiagnostic::Absent,
            requests_reset_ms:n::NumericDiagnostic::Absent,tokens_reset_ms:n::NumericDiagnostic::Absent,
            quota:n::QuotaDiagnostic::Absent,
        }))
    } else {None};
    let operation=Operation::ObserveWorker {report:Report {
        report_id:ids::diagnostic_id(&who.attempt_id,index),task_id:who.task_id.clone(),attempt_id:who.attempt_id.clone(),
        generation:who.generation,worker_id:who.worker_id.clone(),incarnation:who.incarnation.clone(),
        observation:Observation::NativeRequestDiagnostic {request_index:index,
            endpoint:"https://api.openai.com/v1/responses".into(),model:accepted.input.definition.required_inference().unwrap().model.clone(),
            request_binding:Some(n::ProviderRequestBinding {bytes:2,sha256:"44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a".into()}),
            observed_outcome,failure},
    }};
    let retained=exchange(c,operation.clone()).await;
    assert!(matches!(retained.outcome,Outcome::Ok {data:Data::Acknowledged}),"{retained:?}");
    assert!(retained.commit.is_some());
    let replay=exchange(c,operation).await;
    assert_eq!(replay.outcome,retained.outcome);assert_eq!(replay.commit,retained.commit);
}
async fn tool_turn(c:&Controller,a:&Attempt,who:&Caller)->NativeCallEvidence {
    let b=begin(c,a,who,1,"mixed-original-turn").await;
    terminal(c,who,1,Response::Completed).await;
    outcome(&observe(c,who,"mixed-original-reply",CharterObservation::Reply{turn_id:b.turn_id.clone(),reply:reply(None,&["mixed-provider-call"])}).await);
    let id=ids::call_operation_id(&a.id,1,1);
    let inv=borg_runtime::mcp::Invocation{caller:who.clone(),operation_id:id.clone(),logical_call_id:"mixed-provider-call".into(),tool:"collab_task_get".into(),arguments:json!({"task_id":a.task_id})};
    let result=exchange(c,inv.operation()).await;assert!(matches!(result.outcome,Outcome::Ok{..}),"{result:?}");
    outcome(&observe(c,who,"mixed-original-close",CharterObservation::CompleteTurn{turn_id:b.turn_id,results:vec![CharterResultReceipt::Collaboration{call_id:"mixed-provider-call".into(),operation_id:id.clone()}]}).await);
    NativeCallEvidence{request_index:1,call_index:1,operation_id:id,logical_call_id:"mixed-provider-call".into()}
}
async fn compact(c:&Controller,t:&Task,a:&Attempt,who:&Caller,index:u32,id:&str)->CharterCompaction {
    reserve(c,a,index).await;let s=session(c,&t.session_id).await;
    match outcome(&observe(c,who,id,CharterObservation::BeginCompaction{session_id:t.session_id.clone(),expected_revision:s.session.revision,request_index:index}).await){CharterOutcome::Compaction(v)=>v,x=>panic!("{x:?}")}
}
async fn summarized(c:&Controller,who:&Caller,run:&CharterCompaction,id:&str) {
    terminal(c,who,run.request_index,Response::Completed).await;
    outcome(&observe(c,who,id,CharterObservation::CompactionReply{run_id:run.run_id.clone(),reply:reply(None,&[]),summary:Some("actual controlled summary".into())}).await);
}
fn completion(a:&Attempt,total:u32,calls:Vec<NativeCallEvidence>,requests:Vec<Kind>)->Operation {
    Operation::ObserveWorker{report:Report{report_id:ids::completion_id(&a.id),task_id:a.task_id.clone(),attempt_id:a.id.clone(),generation:a.generation,worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone(),observation:Observation::NativeLoopCompleted{model_requests:total,calls,requests,final_response_index:None}}}
}
async fn due(c:&Controller,t:&Task) {
    let target=other_source(c,"mixed-source-target").await;let s=session(c,&t.session_id).await;
    outcome(&set(c,"mixed-source-change",&s.session,&target).await);
}
#[tokio::test]
async fn charter_native_kinds_real_tool_rejected_compaction_summary_and_final_accept() {
    for with_rejection in [false,true] {
        let c=Controller::open_with_config(path(),charter_config()).unwrap();let (t,a,who)=setup(&c,"mixed-charter").await;
        due(&c,&t).await;let tool=tool_turn(&c,&a,&who).await;
        let mut run=compact(&c,&t,&a,&who,2,"mixed-summary-begin").await;let mut kinds=vec![];
        if with_rejection {
            terminal(&c,&who,2,Response::Rejected).await;
            let (_,proof)=rejected_post(&c,&a,2).await;let report_id=proof.commit.as_ref().unwrap().operation_id.clone();
            outcome(&observe(&c,&who,"mixed-rejected-summary",CharterObservation::CompactionRejected{run_id:run.run_id.clone(),report_id:report_id.clone()}).await);
            kinds.push(Kind::ConfirmedRejection{request_index:2,operation_id:report_id});
            run=compact(&c,&t,&a,&who,3,"mixed-summary-retry").await;
        }
        summarized(&c,&who,&run,"mixed-summary-reply").await;
        kinds.push(Kind::Compaction{request_index:run.request_index,operation_id:run.run_id.clone()});
        let total=run.request_index+1;let final_turn=begin(&c,&a,&who,total,"mixed-final-turn").await;terminal(&c,&who,total,Response::Completed).await;no_tools(&c,&who,&final_turn,"mixed-final").await;
        let operation=completion(&a,total,vec![tool],kinds);let result=exchange(&c,operation.clone()).await;
        assert!(matches!(result.outcome,Outcome::Ok{..}),"{result:?}");assert_eq!(exchange(&c,operation).await.commit,result.commit);
        task(report(&c,&a,"mixed-executor",Observation::ExecutorOutcome{success:true,summary:"finished".into()}).await);
        task(report(&c,&a,"mixed-stopped",Observation::ProcessTreeStopped).await);
        let accepted=task(report(&c,&a,"mixed-result",Observation::ResultCandidate{summary:"final answer".into(),checks_passed:true,checkpoint:None,validation:vec![ids::evidence(&a.id)],output:Some(json!({"answer":42}))}).await);
        assert_eq!(accepted.status,TaskStatus::Succeeded);
    }
}
#[tokio::test]
async fn charter_native_final_kind_is_actual_ordinary_reply_not_omitted_compaction() {
    let c=Controller::open_with_config(path(),charter_config()).unwrap();let (t,a,who)=setup(&c,"false-final").await;
    due(&c,&t).await;let tool=tool_turn(&c,&a,&who).await;let run=compact(&c,&t,&a,&who,2,"false-final-summary").await;summarized(&c,&who,&run,"false-final-summary-reply").await;
    let before=get(&c,&t.id).await;let result=exchange(&c,completion(&a,2,vec![tool],vec![])).await;
    assert!(matches!(result.outcome,Outcome::Refused{ref code,..} if code=="reconciliation_required"),"{result:?}");assert_eq!(get(&c,&t.id).await,before);
}
#[tokio::test]
async fn charter_native_compaction_missing_wrong_kind_or_misordered_original_refuses() {
    for defect in 0..3 {
        let c=Controller::open_with_config(path(),charter_config()).unwrap();let (t,a,who)=setup(&c,"summary-negative").await;
        due(&c,&t).await;let tool=tool_turn(&c,&a,&who).await;let run=compact(&c,&t,&a,&who,2,"summary-negative-begin").await;
        if defect==2 {reserve(&c,&a,3).await;}
        summarized(&c,&who,&run,"summary-negative-reply").await;
        let final_turn=begin(&c,&a,&who,3,"summary-negative-final").await;terminal(&c,&who,3,Response::Completed).await;no_tools(&c,&who,&final_turn,"summary-negative-final").await;
        let operation_id=if defect==1 {"mixed-original-turn".into()}else{run.run_id};
        let kinds=if defect==0 {vec![]}else{vec![Kind::Compaction{request_index:2,operation_id}]};let before=get(&c,&t.id).await;
        let result=exchange(&c,completion(&a,3,vec![tool],kinds)).await;
        assert!(matches!(result.outcome,Outcome::Refused{..}),"defect {defect}: {result:?}");assert_eq!(get(&c,&t.id).await,before);
    }
}

#[tokio::test]
async fn charter_native_final_before_compaction_requires_original_governed_boundary() {
    use borg_runtime::governance_completion::{CompletionBoundary,CompletionOutcome,EvaluateCompletion,evaluation_id};
    for with_evaluation in [false,true] {
        let c=Controller::open_with_config(path(),charter_config()).unwrap();let (t,a,who)=setup(&c,"trailing-summary").await;
        due(&c,&t).await;let final_turn=begin(&c,&a,&who,1,"trailing-original-final").await;terminal(&c,&who,1,Response::Completed).await;no_tools(&c,&who,&final_turn,"trailing-original-final").await;
        let run=compact(&c,&t,&a,&who,2,"trailing-summary-begin").await;summarized(&c,&who,&run,"trailing-summary-reply").await;
        let gap=vec![Kind::Compaction{request_index:2,operation_id:run.run_id}];
        if with_evaluation {
            let boundary=CompletionBoundary::NativeResponse{request_index:1};
            let evaluated=exchange(&c,Operation::EvaluateCompletion(EvaluateCompletion{operation_id:evaluation_id(&who,&boundary).unwrap(),caller:who.clone(),boundary,intervening_requests:gap.clone()})).await;
            assert!(matches!(&evaluated.outcome,Outcome::Ok{data:Data::Completion(value)} if matches!(value.as_ref(),CompletionOutcome::NotEnforced)),"{evaluated:?}");
        }
        let mut operation=completion(&a,2,vec![],gap);
        let Operation::ObserveWorker{report}=&mut operation else{unreachable!()};
        let Observation::NativeLoopCompleted{final_response_index,..}=&mut report.observation else{unreachable!()};*final_response_index=Some(1);
        let before=get(&c,&t.id).await;let result=exchange(&c,operation).await;
        if with_evaluation {assert!(matches!(result.outcome,Outcome::Ok{..}),"{result:?}")} else {
            assert!(matches!(result.outcome,Outcome::Refused{ref code,..} if code=="source_mismatch"),"{result:?}");assert_eq!(get(&c,&t.id).await,before);
        }
    }
}
