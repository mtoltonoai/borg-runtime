//! Real canonical controller operations with controlled host observations.
//! These cases do not execute a provider, certify its output, or manufacture rows.
use super::*;
use borg_runtime::{mcp::{Caller, Invocation}, native as n, native_loop_contract::{self as ids, NativeRequestEvidence as Kind, NativeRequestObservedOutcome as Observed}};

async fn setup(c: &Controller, label: &str) -> (Task, Attempt, Caller) {
    let t=submit(c,&format!("{label}-submit"),native(label)).await;
    let w=worker(&format!("{label}-worker"),"one",1,0);register(c,&w).await;
    let a=run(&commands(c,&w,vec![]).await);
    task(report(c,&a,&format!("{label}-start"),Observation::Started{thread_id:None,turn_id:None}).await);
    let caller=Caller{task_id:t.id.clone(),attempt_id:a.id.clone(),generation:a.generation,worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone()};
    (t,a,caller)
}
async fn exchange(c:&Controller,command:Operation)->Reply {
    let result=c.handle(Request{protocol_revision:WIRE_REVISION,request_id:"request-kind-test".into(),command:command.clone()}).await;
    result.validate_for(&command).unwrap();result
}
// These are controlled response observations, selected independently from the
// completion proof vector under test. Missing or malformed proof entries below
// must still reach their original proof oracle after the current charge contract.
#[derive(Clone, Copy)]
enum Response { Completed, Rejected, InvalidFormat, Capped }
async fn reserve(c:&Controller,a:&Attempt,index:u32,response:Response) {
    task(report(c,a,&ids::reservation_id(&a.id,index),Observation::Usage{native_diagnostic_reserved: true,model_requests:index,input_tokens:None,output_tokens:None}).await);
    let observed_outcome=match response {
        Response::Rejected=>Observed::KnownRejected {rejection_kind:n::RejectionKind::RateLimited},
        Response::Capped=>Observed::OutputCapped,
        Response::Completed|Response::InvalidFormat=>Observed::Completed,
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
    let request_binding=n::ProviderRequestBinding {bytes:2,sha256:if matches!(response,Response::InvalidFormat|Response::Capped) {
        "a".repeat(64)
    } else {"44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a".into()}};
    let t=get(c,&a.task_id).await;
    let reply=exchange(c,Operation::ObserveWorker {report:Report {
        report_id:ids::diagnostic_id(&a.id,index),task_id:a.task_id.clone(),attempt_id:a.id.clone(),
        generation:a.generation,worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone(),
        observation:Observation::NativeRequestDiagnostic {request_index:index,
            endpoint:"https://api.openai.com/v1/responses".into(),model:t.input.definition.required_inference().unwrap().model.clone(),
            request_binding:Some(request_binding),observed_outcome,failure},
    }}).await;
    assert!(matches!(reply.outcome,Outcome::Ok {data:Data::Acknowledged}),"{reply:?}");
    assert!(reply.commit.is_some());
}
async fn rejected(c:&Controller,a:&Attempt,index:u32)->String {
    let t=get(c,&a.task_id).await;let id=ids::rejection_id(&a.id,index);
    task(report(c,a,&id,Observation::NativeRequestRejected{request_index:index,endpoint:"https://api.openai.com/v1/responses".into(),model:t.input.definition.required_inference().unwrap().model.clone(),
        request_binding:borg_runtime::native::ProviderRequestBinding{bytes:2,sha256:"44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a".into()},
        rejection:borg_runtime::native::ProviderRejection{kind:borg_runtime::native::RejectionKind::RateLimited,retry_after:borg_runtime::native::RetryAfter::Absent},diagnostic:"controlled complete 429".into()}).await);id
}
async fn tool(c:&Controller,who:&Caller,index:u32)->NativeCallEvidence {
    let id=ids::call_operation_id(&who.attempt_id,index,1);let logical=format!("original-provider-call-{index}");
    let r=exchange(c,Invocation{caller:who.clone(),operation_id:id.clone(),logical_call_id:logical.clone(),tool:"collab_task_get".into(),arguments:json!({"task_id":who.task_id})}.operation()).await;
    assert!(matches!(r.outcome,Outcome::Ok{..}),"{r:?}");
    NativeCallEvidence{request_index:index,call_index:1,operation_id:id,logical_call_id:logical}
}
fn completion(a:&Attempt,total:u32,calls:Vec<NativeCallEvidence>,requests:Vec<Kind>,final_index:Option<u32>)->Operation {
    Operation::ObserveWorker{report:Report{report_id:ids::completion_id(&a.id),task_id:a.task_id.clone(),attempt_id:a.id.clone(),generation:a.generation,worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone(),
        observation:Observation::NativeLoopCompleted{model_requests:total,calls,requests,final_response_index:final_index}}}
}
async fn finish(c:&Controller,a:&Attempt)->Task {
    task(report(c,a,"joined-executor",Observation::ExecutorOutcome{success:true,summary:"controlled final answer".into()}).await);
    task(report(c,a,"joined-stop",Observation::ProcessTreeStopped).await);
    task(report(c,a,"joined-result",Observation::ResultCandidate{summary:"accepted".into(),checks_passed:true,checkpoint:None,validation:vec![ids::evidence(&a.id)],output:Some(json!({"answer":42}))}).await)
}
#[test]
fn native_request_evidence_legacy_serialization_is_exact_and_union_is_closed() {
    let old=json!({"kind":"native_loop_completed","model_requests":1,"calls":[]});
    let observed:Observation=serde_json::from_value(old.clone()).unwrap();
    assert_eq!(serde_json::to_value(&observed).unwrap(),old);
    assert!(matches!(observed,Observation::NativeLoopCompleted{requests,final_response_index:None,..} if requests.is_empty()));
    let value=Kind::ConfirmedRejection{request_index:1,operation_id:"original".into()};
    assert_eq!(serde_json::from_value::<Kind>(serde_json::to_value(&value).unwrap()).unwrap(),value);
    assert!(serde_json::from_value::<Kind>(json!({"kind":"confirmed_rejection","request_index":1,"operation_id":"original","permission":true})).is_err());
    assert!(serde_json::from_value::<Kind>(json!({"kind":"arbitrary_failure","request_index":1,"operation_id":"original"})).is_err());
}
#[tokio::test]
async fn native_request_evidence_legacy_empty_and_joined_rejection_tool_paths_accept_and_replay() {
    for joined in [false,true] {
        let p=path();let cfg=config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
        let (_,a,who)=setup(&c,"joined-positive").await;
        reserve(&c,&a,1,if joined {Response::Rejected} else {Response::Completed}).await;
        let (calls,requests,total)=if joined {
            let proof=rejected(&c,&a,1).await;reserve(&c,&a,2,Response::Completed).await;
            let call=tool(&c,&who,2).await;reserve(&c,&a,3,Response::Completed).await;
            (vec![call],vec![Kind::ConfirmedRejection{request_index:1,operation_id:proof}],3)
        } else {let call=tool(&c,&who,1).await;reserve(&c,&a,2,Response::Completed).await;(vec![call],vec![],2)};
        let operation=completion(&a,total,calls,requests,None);let original=exchange(&c,operation.clone()).await;
        assert!(matches!(original.outcome,Outcome::Ok{..}),"{original:?}");assert!(original.commit.is_some());
        assert_eq!(finish(&c,&a).await.status,TaskStatus::Succeeded);
        drop(c);controller_owner_released(&p).await;let c=Controller::open_with_config(&p,cfg).unwrap();
        let replay=exchange(&c,operation).await;assert_eq!(replay.outcome,original.outcome);assert_eq!(replay.commit,original.commit);
    }
}
#[tokio::test]
async fn native_request_evidence_missing_duplicate_foreign_kind_and_overlap_refuse_exact_completion() {
    // Every mutation gets a fresh task and the exact canonical completion ID,
    // so an outer ID mismatch or earlier failed receipt cannot mask the oracle.
    for variant in 0..9 {
        let c=Controller::open_with_config(path(),config()).unwrap();let (t,a,who)=setup(&c,"kind-negative").await;
        reserve(&c,&a,1,Response::Rejected).await;let proof=rejected(&c,&a,1).await;
        reserve(&c,&a,2,Response::Completed).await;let call=tool(&c,&who,2).await;reserve(&c,&a,3,Response::Completed).await;
        let mut calls=vec![call];let mut requests=vec![Kind::ConfirmedRejection{request_index:1,operation_id:proof.clone()}];let mut total=3;
        match variant {
            0=>requests.clear(),
            1=>requests.push(requests[0].clone()),
            2=>requests.push(Kind::ConfirmedRejection{request_index:2,operation_id:proof}),
            3=>requests[0]=Kind::Compaction{request_index:1,operation_id:proof},
            4=>requests[0]=Kind::ConfirmedRejection{request_index:0,operation_id:proof},
            5=>requests[0]=Kind::ConfirmedRejection{request_index:3,operation_id:proof},
            6=>requests[0]=Kind::ConfirmedRejection{request_index:1,operation_id:"absent-original".into()},
            7=>calls[0].logical_call_id="substituted-provider-call".into(),
            _=>total=4,
        }
        let before=get(&c,&t.id).await;let result=exchange(&c,completion(&a,total,calls,requests,None)).await;
        assert!(matches!(result.outcome,Outcome::Refused{ref code,..} if code=="source_mismatch"||code=="usage_unavailable"||code=="reconciliation_required"),"variant {variant}: {result:?}");
        assert_eq!(get(&c,&t.id).await,before);
    }
}
#[tokio::test]
async fn native_request_evidence_foreign_real_rejection_and_rejected_final_refuse() {
    for foreign in [false,true] {
        let c=Controller::open_with_config(path(),config()).unwrap();let (t,a,who)=setup(&c,"original-owner").await;
        reserve(&c,&a,1,if foreign {Response::Rejected} else {Response::Completed}).await;let original=tool(&c,&who,1).await;reserve(&c,&a,2,if foreign {Response::Completed} else {Response::Rejected}).await;
        let requests=if foreign {
            let (_,other,_)=setup(&c,"foreign-owner").await;reserve(&c,&other,1,Response::Rejected).await;
            let otherproof=rejected(&c,&other,1).await;
            vec![Kind::ConfirmedRejection{request_index:1,operation_id:otherproof}]
        } else {rejected(&c,&a,2).await;vec![]};
        let calls=if foreign {vec![]}else{vec![original]};let before=get(&c,&t.id).await;
        let result=exchange(&c,completion(&a,2,calls,requests,None)).await;
        assert!(matches!(result.outcome,Outcome::Refused{ref code,..} if code=="source_mismatch"),"{result:?}");
        assert_eq!(get(&c,&t.id).await,before);
    }
}
#[tokio::test]
async fn native_request_evidence_reversed_rejections_and_tool_after_next_charge_refuse() {
    for wrong_tool_order in [false,true] {
        let c=Controller::open_with_config(path(),config()).unwrap();let (t,a,who)=setup(&c,"ordering").await;
        reserve(&c,&a,1,if wrong_tool_order {Response::Completed} else {Response::Rejected}).await;
        let (calls,requests,total)=if wrong_tool_order {
            reserve(&c,&a,2,Response::Completed).await;let late=tool(&c,&who,1).await;(vec![late],vec![],2)
        } else {
            let first=rejected(&c,&a,1).await;reserve(&c,&a,2,Response::Rejected).await;let second=rejected(&c,&a,2).await;reserve(&c,&a,3,Response::Completed).await;
            (vec![],vec![Kind::ConfirmedRejection{request_index:2,operation_id:second},Kind::ConfirmedRejection{request_index:1,operation_id:first}],3)
        };
        let before=get(&c,&t.id).await;let result=exchange(&c,completion(&a,total,calls,requests,None)).await;
        assert!(matches!(result.outcome,Outcome::Refused{ref code,..} if code=="source_mismatch"),"{result:?}");assert_eq!(get(&c,&t.id).await,before);
    }
}

fn format_config() -> borg_runtime::controller::ControllerConfig {
    let mut cfg=config();let mut p=(*cfg.limits).clone();
    p.native_retry=Some(borg_runtime::limits::NativeRetryLimits {
        initial_backoff_ms:5,backoff_multiplier:2,max_backoff_ms:20,jitter_ms:1,
        circuit:borg_runtime::limits::NativeCircuitLimits {failure_threshold:4,open_ms:50,max_entries:16},
        auth_cooldown_ms:150,auth_max_entries:16,coordination_record_bytes:32768,
    });p.validate().unwrap();cfg.limits=std::sync::Arc::new(p);cfg
}
fn format_report(a:&Attempt,index:u32)->Observation {
    let _=a;
    Observation::NativeFormatFailure {request_index:index,
        issue:borg_runtime::native::FormatIssue::Schema {rule:borg_runtime::native::SchemaRule::Type},
        request_binding:borg_runtime::native::ProviderRequestBinding {bytes:2,sha256:"a".repeat(64)},
        output_binding:borg_runtime::native::ProviderRequestBinding {bytes:2,sha256:"b".repeat(64)},
    }
}
#[tokio::test]
async fn native_format_original_receipt_accepts_exact_join_and_replays_after_restart() {
    let p=path();let cfg=format_config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let (_,a,_)=setup(&c,"format-original").await;reserve(&c,&a,1,Response::InvalidFormat).await;
    let id=ids::format_failure_id(&a.id,1);
    let original=report(&c,&a,&id,format_report(&a,1)).await;assert!(matches!(original,Outcome::Ok{..}),"{original:?}");
    reserve(&c,&a,2,Response::Completed).await;
    let operation=completion(&a,2,vec![],vec![Kind::FormatRepair {request_index:1,operation_id:id}],None);
    let accepted=exchange(&c,operation.clone()).await;assert!(matches!(accepted.outcome,Outcome::Ok{..}),"{accepted:?}");
    assert_eq!(finish(&c,&a).await.status,TaskStatus::Succeeded);
    drop(c);controller_owner_released(&p).await;let c=Controller::open_with_config(&p,cfg).unwrap();
    let replay=exchange(&c,operation).await;assert_eq!(replay.outcome,accepted.outcome);assert_eq!(replay.commit,accepted.commit);
}
#[tokio::test]
async fn native_format_missing_foreign_rejected_final_and_conflicting_proof_refuse() {
    for variant in 0..6 {
        let c=Controller::open_with_config(path(),format_config()).unwrap();let(t,a,_)=setup(&c,"format-negative").await;
        reserve(&c,&a,1,Response::InvalidFormat).await;let id=ids::format_failure_id(&a.id,1);
        if variant!=0 {task(report(&c,&a,&id,format_report(&a,1)).await);}
        reserve(&c,&a,2,Response::Completed).await;
        let mut entries=vec![Kind::FormatRepair {request_index:1,operation_id:id.clone()}];
        match variant {
            0=>{},
            1=>entries.push(entries[0].clone()),
            2=>entries[0]=Kind::ConfirmedRejection {request_index:1,operation_id:id},
            3=>entries[0]=Kind::FormatRepair {request_index:1,operation_id:"foreign-format-proof".into()},
            4=>entries[0]=Kind::FormatRepair {request_index:2,operation_id:id},
            _=>entries.clear(),
        }
        let before=get(&c,&t.id).await;let r=exchange(&c,completion(&a,2,vec![],entries,None)).await;
        assert!(matches!(r.outcome,Outcome::Refused{ref code,..} if code=="source_mismatch"),"variant {variant}: {r:?}");assert_eq!(get(&c,&t.id).await,before);
    }
    let c=Controller::open_with_config(path(),format_config()).unwrap();let(_,a,_)=setup(&c,"format-final").await;
    reserve(&c,&a,1,Response::InvalidFormat).await;task(report(&c,&a,&ids::format_failure_id(&a.id,1),format_report(&a,1)).await);
    assert!(matches!(exchange(&c,completion(&a,1,vec![],vec![],None)).await.outcome,Outcome::Refused{ref code,..} if code=="source_mismatch"));
}
#[tokio::test]
async fn native_format_report_requires_selected_policy_and_original_charge() {
    for selected in [false,true] {
        let c=Controller::open_with_config(path(),if selected {format_config()} else {config()}).unwrap();
        let(t,a,_)=setup(&c,"format-no-grant").await;
        if !selected {reserve(&c,&a,1,Response::InvalidFormat).await;}
        let before=get(&c,&t.id).await;let r=report(&c,&a,&ids::format_failure_id(&a.id,1),format_report(&a,1)).await;
        assert!(matches!(r,Outcome::Refused{ref code,..} if code=="source_mismatch"),"{r:?}");assert_eq!(get(&c,&t.id).await,before);
    }
}
#[tokio::test]
async fn wire12_refuses_format_occurrence_without_committing_or_reinterpreting_it() {
    let c=Controller::open_with_config(path(),format_config()).unwrap();let(t,a,_)=setup(&c,"format-old-wire").await;reserve(&c,&a,1,Response::InvalidFormat).await;
    let op=Operation::ObserveWorker {report:Report {report_id:ids::format_failure_id(&a.id,1),task_id:t.id.clone(),attempt_id:a.id.clone(),generation:a.generation,worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone(),observation:format_report(&a,1)}};
    let before=get(&c,&t.id).await;
    let r=c.handle(Request {protocol_revision:12,request_id:"old-format-wire".into(),command:op}).await;
    assert!(matches!(r.outcome,Outcome::Refused{..}));assert!(r.commit.is_none());assert_eq!(get(&c,&t.id).await,before);
}

#[tokio::test]
async fn native_format_deadline_block_reopens_with_original_occurrence_and_unchanged_allowance() {
    let p=path();let now=std::sync::Arc::new(std::sync::atomic::AtomicU64::new(borg_runtime::worker::process::now_ms()));
    let mut cfg=format_config();let clock=now.clone();cfg.clock=std::sync::Arc::new(move||clock.load(std::sync::atomic::Ordering::SeqCst));
    let c=Controller::open_with_config(&p,cfg.clone()).unwrap();let(t,a,_)=setup(&c,"format-deadline").await;
    reserve(&c,&a,1,Response::InvalidFormat).await;let id=ids::format_failure_id(&a.id,1);task(report(&c,&a,&id,format_report(&a,1)).await);
    let original=exchange(&c,Operation::GetOperation {operation_id:id.clone()}).await;
    now.store(t.input.limits.deadline_unix_ms+1,std::sync::atomic::Ordering::SeqCst);
    drop(c);controller_owner_released(&p).await;let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let blocked=get(&c,&t.id).await;assert_eq!(blocked.status,TaskStatus::Blocked);assert_eq!(blocked.input,t.input);
    assert_eq!(blocked.native_retry_block.as_ref().unwrap(),&borg_runtime::native::NativeRetryBlock {request_index:1,source_operation_id:id.clone(),reason:borg_runtime::native::RetryBlockReason::Deadline});
    drop(c);controller_owner_released(&p).await;let c=Controller::open_with_config(&p,cfg).unwrap();
    assert_eq!(get(&c,&t.id).await,blocked);let replay=exchange(&c,Operation::GetOperation {operation_id:id}).await;
    assert_eq!(replay.outcome,original.outcome);assert_eq!(replay.commit,original.commit);
    assert!(matches!(exchange(&c,Operation::GetOperation {operation_id:ids::reservation_id(&a.id,2)}).await.outcome,Outcome::Refused{ref code,..} if code=="not_found"));
}

fn cap_report(a:&Attempt,index:u32)->Observation {
    let _=a;
    Observation::NativeOutputCapped {request_index:index,undispatched_calls:0,
        request_binding:borg_runtime::native::ProviderRequestBinding {bytes:2,sha256:"a".repeat(64)},
        output_binding:borg_runtime::native::ProviderRequestBinding {bytes:2,sha256:"b".repeat(64)},
    }
}
#[tokio::test]
async fn native_output_cap_original_receipt_accepts_exact_join_and_replays_after_restart() {
    let p=path();let cfg=format_config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let (_,a,_)=setup(&c,"cap-original").await;reserve(&c,&a,1,Response::Capped).await;
    let id=ids::output_cap_id(&a.id,1);
    let original=report(&c,&a,&id,cap_report(&a,1)).await;assert!(matches!(original,Outcome::Ok{..}),"{original:?}");
    reserve(&c,&a,2,Response::Completed).await;
    let operation=completion(&a,2,vec![],vec![Kind::OutputCap {request_index:1,operation_id:id}],None);
    let accepted=exchange(&c,operation.clone()).await;assert!(matches!(accepted.outcome,Outcome::Ok{..}),"{accepted:?}");
    assert_eq!(finish(&c,&a).await.status,TaskStatus::Succeeded);
    drop(c);controller_owner_released(&p).await;let c=Controller::open_with_config(&p,cfg).unwrap();
    let replay=exchange(&c,operation).await;assert_eq!(replay.outcome,accepted.outcome);assert_eq!(replay.commit,accepted.commit);
}
#[tokio::test]
async fn native_output_cap_missing_foreign_rejected_final_and_conflicting_proof_refuse() {
    for variant in 0..6 {
        let c=Controller::open_with_config(path(),format_config()).unwrap();let(t,a,_)=setup(&c,"cap-negative").await;
        reserve(&c,&a,1,Response::Capped).await;let id=ids::output_cap_id(&a.id,1);
        if variant!=0 {task(report(&c,&a,&id,cap_report(&a,1)).await);}
        reserve(&c,&a,2,Response::Completed).await;
        let mut entries=vec![Kind::OutputCap {request_index:1,operation_id:id.clone()}];
        match variant {
            0=>{},
            1=>entries.push(entries[0].clone()),
            2=>entries[0]=Kind::ConfirmedRejection {request_index:1,operation_id:id},
            3=>entries[0]=Kind::OutputCap {request_index:1,operation_id:"foreign-format-proof".into()},
            4=>entries[0]=Kind::OutputCap {request_index:2,operation_id:id},
            _=>entries.clear(),
        }
        let before=get(&c,&t.id).await;let r=exchange(&c,completion(&a,2,vec![],entries,None)).await;
        assert!(matches!(r.outcome,Outcome::Refused{ref code,..} if code=="source_mismatch"),"variant {variant}: {r:?}");assert_eq!(get(&c,&t.id).await,before);
    }
    let c=Controller::open_with_config(path(),format_config()).unwrap();let(_,a,_)=setup(&c,"cap-final").await;
    reserve(&c,&a,1,Response::Capped).await;task(report(&c,&a,&ids::output_cap_id(&a.id,1),cap_report(&a,1)).await);
    assert!(matches!(exchange(&c,completion(&a,1,vec![],vec![],None)).await.outcome,Outcome::Refused{ref code,..} if code=="source_mismatch"));
}
#[tokio::test]
async fn native_cap_report_requires_selected_policy_and_original_charge() {
    for selected in [false,true] {
        let c=Controller::open_with_config(path(),if selected {format_config()} else {config()}).unwrap();
        let(t,a,_)=setup(&c,"cap-no-grant").await;
        if !selected {reserve(&c,&a,1,Response::Capped).await;}
        let before=get(&c,&t.id).await;let r=report(&c,&a,&ids::output_cap_id(&a.id,1),cap_report(&a,1)).await;
        assert!(matches!(r,Outcome::Refused{ref code,..} if code=="source_mismatch"),"{r:?}");assert_eq!(get(&c,&t.id).await,before);
    }
}
#[tokio::test]
async fn wire13_refuses_output_cap_occurrence_without_committing_or_reinterpreting_it() {
    let c=Controller::open_with_config(path(),format_config()).unwrap();let(t,a,_)=setup(&c,"cap-old-wire").await;reserve(&c,&a,1,Response::Capped).await;
    let op=Operation::ObserveWorker {report:Report {report_id:ids::output_cap_id(&a.id,1),task_id:t.id.clone(),attempt_id:a.id.clone(),generation:a.generation,worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone(),observation:cap_report(&a,1)}};
    let before=get(&c,&t.id).await;
    let r=c.handle(Request {protocol_revision:13,request_id:"old-format-wire".into(),command:op}).await;
    assert!(matches!(r.outcome,Outcome::Refused{..}));assert!(r.commit.is_none());assert_eq!(get(&c,&t.id).await,before);
}

#[tokio::test]
async fn native_output_cap_deadline_block_reopens_with_original_occurrence_and_unchanged_allowance() {
    let p=path();let now=std::sync::Arc::new(std::sync::atomic::AtomicU64::new(borg_runtime::worker::process::now_ms()));
    let mut cfg=format_config();let clock=now.clone();cfg.clock=std::sync::Arc::new(move||clock.load(std::sync::atomic::Ordering::SeqCst));
    let c=Controller::open_with_config(&p,cfg.clone()).unwrap();let(t,a,_)=setup(&c,"cap-deadline").await;
    reserve(&c,&a,1,Response::Capped).await;let id=ids::output_cap_id(&a.id,1);task(report(&c,&a,&id,cap_report(&a,1)).await);
    let original=exchange(&c,Operation::GetOperation {operation_id:id.clone()}).await;
    now.store(t.input.limits.deadline_unix_ms+1,std::sync::atomic::Ordering::SeqCst);
    drop(c);controller_owner_released(&p).await;let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let blocked=get(&c,&t.id).await;assert_eq!(blocked.status,TaskStatus::Blocked);assert_eq!(blocked.input,t.input);
    assert_eq!(blocked.native_retry_block.as_ref().unwrap(),&borg_runtime::native::NativeRetryBlock {request_index:1,source_operation_id:id.clone(),reason:borg_runtime::native::RetryBlockReason::Deadline});
    drop(c);controller_owner_released(&p).await;let c=Controller::open_with_config(&p,cfg).unwrap();
    assert_eq!(get(&c,&t.id).await,blocked);let replay=exchange(&c,Operation::GetOperation {operation_id:id}).await;
    assert_eq!(replay.outcome,original.outcome);assert_eq!(replay.commit,original.commit);
    assert!(matches!(exchange(&c,Operation::GetOperation {operation_id:ids::reservation_id(&a.id,2)}).await.outcome,Outcome::Refused{ref code,..} if code=="not_found"));
}

#[tokio::test]
async fn native_output_cap_conflicting_format_rejection_or_dispatched_call_refuses_original() {
 for variant in 0..3 {
    let c=Controller::open_with_config(path(),format_config()).unwrap();let(t,a,who)=setup(&c,"cap-conflict").await;
    reserve(&c,&a,1,match variant {0=>Response::InvalidFormat,1=>Response::Rejected,_=>Response::Completed}).await;
    if variant==0 {task(report(&c,&a,&ids::format_failure_id(&a.id,1),format_report(&a,1)).await);}
    else if variant==1 {rejected(&c,&a,1).await;} else {tool(&c,&who,1).await;}
    let before=get(&c,&t.id).await;let mut obs=cap_report(&a,1);
    if variant==2 {if let Observation::NativeOutputCapped {undispatched_calls,..}=&mut obs {*undispatched_calls=1;}}
    let result=report(&c,&a,&ids::output_cap_id(&a.id,1),obs).await;
    assert!(matches!(result,Outcome::Refused{ref code,..} if code=="source_mismatch"),"{variant}: {result:?}");
    assert_eq!(get(&c,&t.id).await,before);
 }
}
