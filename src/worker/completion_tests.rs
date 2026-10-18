//! Retained-receipt fixtures only. These test producer behavior and unknown
//! dispatch retention; controller rule evaluation is covered in its own lane.
use super::*;
use crate::governance::{CompletionDecision,CompletionSubject,CompletionReason};
use crate::limits::GovernanceLimits;

pub(in crate::worker) fn policy() -> RuntimeLimits {
    let mut limits = (*crate::worker::test_limits()).clone();
    limits.governance = Some(GovernanceLimits {
        record_bytes:16_384,reason_items:16,reason_bytes:2_048,scan_rows:32,
        rule_versions:16,configuration_versions:16,shadow_evidence_items:16,
        completion_continuations:1,override_max_ms:100,
    });
    limits
}
pub(in crate::worker) fn caller() -> Caller {
    Caller {task_id:"completion-task".into(),attempt_id:"completion-attempt".into(),generation:1,
        worker_id:"completion-worker".into(),incarnation:"completion-incarnation".into()}
}
pub(in crate::worker) fn area() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let path = std::env::temp_dir().join(format!("borg-completion-input-{}-{}-{}",
        std::process::id(),process::now_ms(),NEXT.fetch_add(1,Ordering::Relaxed)));
    process::private_directory(&path).unwrap();
    path
}
pub(in crate::worker) fn feedback(boundary: CompletionBoundary,target: CompletionTarget) -> CompletionFeedback {
    let caller = caller();
    let decision = DecisionRef {operation_id:match &boundary {CompletionBoundary::StoppedResult {report_id}=>report_id.clone(),
        _=>evaluation_id(&caller,&boundary).unwrap()},point:DecisionPoint::Completion};
    CompletionFeedback {
        evaluation:CompletionEvaluation {decision:decision.clone(),boundary,
            state:CompletionDecision {
                subject:CompletionSubject {task_id:caller.task_id.clone(),session_id:"completion-session".into(),
                    attempt_id:caller.attempt_id.clone(),generation:1,task_revision:7,accepted_global_revision:"accepted-global-1".into()},
                disposition:CompletionDisposition::Blocked {continuation_eligible:true,exhausted:false},
                reasons:vec![CompletionReason::DelegatedTask {task_id:"remaining-child".into()}],
                consecutive_count:1,independent_reset_event:None,
            }},
        continuation:CompletionContinuation {begin_operation_id:begin_id(&caller,&decision).unwrap(),caller,decision,
            context_binding:match &target {CompletionTarget::NativeRequest {..} | CompletionTarget::InitialNativeRequest {..}=>"completion-attempt".into(),
                CompletionTarget::CodexTurn {thread_id,..} | CompletionTarget::InitialCodexTurn {thread_id}=>thread_id.clone()},target,delivery_batch_id:None,acknowledged:None},
    }
}
// Explicit query fixture, not a controller or an alternate runtime decision cache.
// Every other request fails; the returned record binds the seeded original receipt.
pub(in crate::worker) fn decision_record(feedback:&CompletionFeedback,limits:&RuntimeLimits) -> crate::governance::DecisionRecord {
    use crate::collaboration::workflow::{ActorProvenance,PrincipalRef,RecordedActor};
    use crate::governance::{ActualDecisionOutcome,ConfigurationBinding,DecisionDetail,DecisionMode,DecisionRecord,RuleRef};
    let caller=&feedback.continuation.caller;
    DecisionRecord {
        mode:DecisionMode::Enforcing,id:feedback.evaluation.decision.clone(),
        rule:RuleRef {id:"fixture-completion".into(),revision:"1".into()},
        configuration:ConfigurationBinding {
            global_revision:limits.revision.clone(),governance_revision:"fixture-governance-1".into(),
            accepted_global_revision:Some(feedback.evaluation.state.subject.accepted_global_revision.clone()),
            definition_update_policy_revision:None,
        },
        configuration_metadata:None,
        actor:RecordedActor {
            principal:PrincipalRef::Agent {agent_id:"completion-agent".into()},
            provenance:ActorProvenance::BoundAgentSession {
                agent_id:"completion-agent".into(),session_id:"completion-session".into(),
                task_id:caller.task_id.clone(),attempt_id:caller.attempt_id.clone(),generation:caller.generation,
                worker_id:caller.worker_id.clone(),incarnation:caller.incarnation.clone(),
            },
        },
        detail:DecisionDetail::Completion(feedback.evaluation.state.clone()),
        actual_outcome:ActualDecisionOutcome::GovernanceBlocked,
        controller_revision:7,observed_product_revision:Some(1),
    }
}
pub(in crate::worker) struct DecisionQueryPeer {
    pub(in crate::worker) url:String,
    server:Option<tokio::task::JoinHandle<()>>,
    queries:Arc<AtomicU64>,
}
impl DecisionQueryPeer {
    pub(in crate::worker) async fn start(expected:DecisionRef,record:crate::governance::DecisionRecord,limits:&RuntimeLimits) -> Self {
        use bytes::Bytes;
        use http_body_util::{BodyExt,Full,Limited};
        use hyper::{body::Incoming,service::service_fn,Request as HttpRequest,Response};
        use hyper_util::rt::TokioIo;
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url=format!("http://{}",listener.local_addr().unwrap());
        let queries=Arc::new(AtomicU64::new(0));
        let seen=queries.clone();
        let body_bytes=limits.transport.body_bytes;
        let server=tokio::spawn(async move {
            tokio::time::timeout(std::time::Duration::from_secs(10),async move {
                let (stream,_)=listener.accept().await.unwrap();
                let service=service_fn(move |request:HttpRequest<Incoming>| {
                    let (expected,record,seen)=(expected.clone(),record.clone(),seen.clone());
                    async move {
                        assert_eq!(request.method(),hyper::Method::POST);
                        assert_eq!(request.uri().path(),"/api/operations");
                        let body=Limited::new(request.into_body(),body_bytes).collect().await.unwrap().to_bytes();
                        let request:crate::protocol::Request=serde_json::from_slice(&body).unwrap();
                        assert_eq!(request.protocol_revision,crate::protocol::WIRE_REVISION);
                        let expected=Operation::QueryGovernance {query:crate::governance::GovernanceQuery::Decision {decision:expected}};
                        assert_eq!(serde_json::to_value(&request.command).unwrap(),serde_json::to_value(&expected).unwrap());
                        assert_eq!(seen.fetch_add(1,Ordering::SeqCst),0,"only the original Decision query is permitted");
                        let reply=crate::protocol::Reply {
                            request_id:request.request_id,commit:None,observed_controller_revision:Some(9),
                            outcome:Outcome::Ok {data:Data::Governance(Box::new(crate::governance::GovernanceResult::Decision(record)))},
                        };
                        reply.validate_for(&expected).unwrap();
                        let body=serde_json::to_vec(&reply).unwrap();
                        assert!(body.len()<=body_bytes);
                        Ok::<_,std::convert::Infallible>(Response::builder().header("content-type","application/json")
                            .header("connection","close").body(Full::new(Bytes::from(body))).unwrap())
                    }
                });
                hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream),service).await.unwrap();
            }).await.expect("bounded Decision fixture timed out");
        });
        Self {url,server:Some(server),queries}
    }
    pub(in crate::worker) async fn finish(mut self) {
        self.server.take().unwrap().await.unwrap();
        assert_eq!(self.queries.load(Ordering::SeqCst),1);
    }
}
impl Drop for DecisionQueryPeer {
    fn drop(&mut self) {
        if let Some(server)=self.server.take() {server.abort();}
    }
}
pub(in crate::worker) fn receipt(state:&Path,limits:&RuntimeLimits,boundary:&CompletionBoundary,
    name:&str,request:Operation,outcome:Outcome) {
    let request_caller=match &request {
        Operation::EvaluateCompletion(value)=>&value.caller,
        Operation::BeginCompletionContinuation(value)=>&value.caller,
        Operation::AcknowledgeCompletionInput(value)=>&value.caller,
        _=>panic!("completion fixture operation expected"),
    };
    let directory = state.join("completion").join(key(request_caller).unwrap()).join(key(boundary).unwrap());
    process::private_directory(&directory).unwrap();
    let path = directory.join(name);
    let id = operation_id(&request).unwrap().to_owned();
    let mut journal = mutations::create(&path,request,0,limits).unwrap();
    journal.outcome = Some(outcome);
    journal.outcome_context = Some(CommitContext {commit:MutationCommit {operation_id:id,controller_revision:7},observed_controller_revision:9});
    mutations::persist(&path,&journal).unwrap();
}
pub(in crate::worker) fn refused_and_begin(state:&Path,limits:&RuntimeLimits,feedback:&CompletionFeedback,allow:bool) {
    let caller = feedback.continuation.caller.clone();
    let boundary = &feedback.evaluation.boundary;
    receipt(state,limits,boundary,"evaluate.json",Operation::EvaluateCompletion(EvaluateCompletion {
        operation_id:feedback.evaluation.decision.operation_id.clone(),caller:caller.clone(),boundary:boundary.clone(),intervening_requests:vec![]}),
        Outcome::Refused {code:"governance_completion_blocked".into(),message:"recorded refusal".into()});
    receipt(state,limits,boundary,"begin.json",Operation::BeginCompletionContinuation(BeginCompletionContinuation {
        operation_id:feedback.continuation.begin_operation_id.clone(),caller,decision:feedback.evaluation.decision.clone(),
        context_binding:feedback.continuation.context_binding.clone(),target:feedback.continuation.target.clone(),delivery_batch_id:None}),
        if allow {Outcome::Ok {data:Data::Completion(CompletionOutcome::Begun(feedback.clone()))}}
        else {Outcome::Refused {code:"governance_completion_exhausted".into(),message:"recorded bound".into()}});
}
pub(in crate::worker) fn initial_begin_receipt(state:&Path,limits:&RuntimeLimits,feedback:&CompletionFeedback,allow:bool) {
    let value=&feedback.continuation;
    receipt(state,limits,&feedback.evaluation.boundary,"begin.json",Operation::BeginCompletionContinuation(BeginCompletionContinuation {
        operation_id:value.begin_operation_id.clone(),caller:value.caller.clone(),decision:value.decision.clone(),
        context_binding:value.context_binding.clone(),target:value.target.clone(),delivery_batch_id:None,
    }),if allow {Outcome::Ok {data:Data::Completion(CompletionOutcome::Begun(feedback.clone()))}}
    else {Outcome::Refused {code:"governance_completion_exhausted".into(),message:"original recorded bound".into()}});
}
pub(in crate::worker) fn acknowledge_receipt(state:&Path,limits:&RuntimeLimits,feedback:&CompletionFeedback,boundary:CompletionBoundary) {
    let native_input=matches!(&boundary,CompletionBoundary::NativeResponse {..}).then(||NativeCompletionInput {
        request_binding:binding(),rejections:vec![],
    });
    acknowledge_native_receipt(state,limits,feedback,boundary,native_input)
}
fn binding()->crate::native::ProviderRequestBinding {
    crate::native::ProviderRequestBinding {bytes:128,sha256:"ab".repeat(32)}
}
fn acknowledge_native_receipt(state:&Path,limits:&RuntimeLimits,feedback:&CompletionFeedback,
    boundary:CompletionBoundary,native_input:Option<NativeCompletionInput>) {
    let id = acknowledgement_id(&feedback.continuation.caller,&feedback.continuation.begin_operation_id).unwrap();
    let mut continuation = feedback.continuation.clone();
    continuation.acknowledged = Some(CompletionInputAck {operation_id:id.clone(),consumed_at:boundary.clone(),delivery_ack_operation_id:None,native_input:native_input.clone()});
    receipt(state,limits,&feedback.evaluation.boundary,"acknowledge.json",Operation::AcknowledgeCompletionInput(AcknowledgeCompletionInput {
        operation_id:id,caller:continuation.caller.clone(),begin_operation_id:continuation.begin_operation_id.clone(),consumed_at:boundary,
        delivery_ack_operation_id:None,native_input}),Outcome::Ok {data:Data::Completion(CompletionOutcome::InputAcknowledged(continuation))});
}
fn host(state:&Path,limits:&RuntimeLimits) -> Host {
    Host::open(ApiClient::new("http://127.0.0.1:9",Arc::new(limits.clone())).unwrap(),caller(),"completion-session",state).unwrap()
}
fn control(limits:&RuntimeLimits) -> (watch::Sender<bool>,Control) {
    let (sender,receiver) = watch::channel(false);
    let mut control = crate::worker::test_control(process::now_ms()+10_000,receiver);
    control.runtime_limits = Arc::new(limits.clone());
    control.worker_limits = Arc::new(limits.worker.clone());
    (sender,control)
}

#[tokio::test]
async fn original_begin_is_consumed_once_and_ack_requires_actual_retained_turn() {
    let limits = policy(); let state = area();
    let (_cancel,control) = control(&limits);
    let first = CompletionBoundary::CodexTurn {thread_id:"thread".into(),turn_id:"turn-1".into()};
    let second = CompletionBoundary::CodexTurn {thread_id:"thread".into(),turn_id:"turn-2".into()};
    let value = feedback(first.clone(),CompletionTarget::CodexTurn {thread_id:"thread".into(),previous_turn_id:"turn-1".into()});
    let peer=DecisionQueryPeer::start(value.evaluation.decision.clone(),decision_record(&value,&limits),&limits).await;
    let host=Host::open(ApiClient::new(&peer.url,Arc::new(limits.clone())).unwrap(),caller(),"completion-session",&state).unwrap();
    refused_and_begin(&state,&limits,&value,true);
    acknowledge_receipt(&state,&limits,&value,second.clone());
    assert!(matches!(host.evaluate(&first,&[],&control).await.unwrap(),Gate::Blocked(ref evaluation) if evaluation==&value.evaluation));
    let mut input = host.begin(&first,&value.evaluation.decision,"thread",value.continuation.target.clone(),None,&control).await.unwrap().unwrap();
    assert!(input.text().contains("remaining-child"));
    assert!(host.acknowledged(&mut input,second.clone(),None,None,&control).await.is_err());
    input.mark_dispatch(&limits).unwrap();
    assert!(input.mark_dispatch(&limits).is_err());
    assert!(host.begin(&first,&value.evaluation.decision,"thread",value.continuation.target.clone(),None,&control).await.is_err());
    assert!(host.acknowledged(&mut input,second.clone(),None,None,&control).await.is_err());
    input.record_turn_start(second.clone(),&limits).unwrap();
    let wrong = CompletionBoundary::CodexTurn {thread_id:"thread".into(),turn_id:"turn-3".into()};
    assert!(host.acknowledged(&mut input,wrong,None,None,&control).await.is_err());
    let acknowledged = host.acknowledged(&mut input,second.clone(),None,None,&control).await.unwrap();
    assert_eq!(acknowledged.acknowledged.as_ref().unwrap().consumed_at,second);
    let retained = read_input(&input.directory.join("input.json"),&limits).unwrap().unwrap();
    assert_eq!(retained.accepted_at,retained.consumed_at);
    assert!(retained.started);
    peer.finish().await;
}

#[tokio::test]
async fn zero_continuations_is_visible_refusal_and_never_creates_input() {
    let mut limits = policy(); limits.governance.as_mut().unwrap().completion_continuations=0;
    let state = area(); let (_cancel,control)=control(&limits);
    let boundary=CompletionBoundary::NativeResponse {request_index:1};
    let value=feedback(boundary.clone(),CompletionTarget::NativeRequest {request_index:2});
    let peer=DecisionQueryPeer::start(value.evaluation.decision.clone(),decision_record(&value,&limits),&limits).await;
    let host=Host::open(ApiClient::new(&peer.url,Arc::new(limits.clone())).unwrap(),caller(),"completion-session",&state).unwrap();
    refused_and_begin(&state,&limits,&value,false);
    assert!(matches!(host.evaluate(&boundary,&[],&control).await.unwrap(),Gate::Blocked(_)));
    assert!(host.begin(&boundary,&value.evaluation.decision,"completion-attempt",value.continuation.target,None,&control).await.unwrap().is_none());
    assert!(!state.join("completion").join(key(&caller()).unwrap()).join(key(&boundary).unwrap()).join("input.json").exists());
    peer.finish().await;
}

#[tokio::test]
async fn blocked_decision_query_rejects_changed_original_revision_or_subject_without_consuming_receipts() {
    for changed_revision in [true,false] {
        let limits=policy();let state=area();let (_cancel,control)=control(&limits);
        let boundary=CompletionBoundary::NativeResponse {request_index:1};
        let value=feedback(boundary.clone(),CompletionTarget::NativeRequest {request_index:2});
        refused_and_begin(&state,&limits,&value,true);
        let directory=state.join("completion").join(key(&caller()).unwrap()).join(key(&boundary).unwrap());
        let evaluate=std::fs::read(directory.join("evaluate.json")).unwrap();
        let begin=std::fs::read(directory.join("begin.json")).unwrap();
        let mut record=decision_record(&value,&limits);
        if changed_revision {record.controller_revision=8;}
        else if let crate::governance::DecisionDetail::Completion(ref mut state)=record.detail {
            state.subject.session_id="another-session".into();
        } else {unreachable!();}
        let peer=DecisionQueryPeer::start(value.evaluation.decision.clone(),record,&limits).await;
        let host=Host::open(ApiClient::new(&peer.url,Arc::new(limits.clone())).unwrap(),caller(),"completion-session",&state).unwrap();
        let error=host.evaluate(&boundary,&[],&control).await.err().expect("changed original decision must refuse");
        assert_eq!(error,if changed_revision {"completion lookup differs from its original enforcing refusal"}
            else {"completion decision subject or reasons differ"});
        assert_eq!(std::fs::read(directory.join("evaluate.json")).unwrap(),evaluate);
        assert_eq!(std::fs::read(directory.join("begin.json")).unwrap(),begin);
        assert!(!directory.join("input.json").exists());
        peer.finish().await;
    }
}

#[test]
fn reason_bounds_and_dispatch_marker_survive_reopen_without_resend() {
    let limits=policy(); let value=feedback(CompletionBoundary::NativeResponse {request_index:1},CompletionTarget::NativeRequest {request_index:2});
    let state=area(); let record=InputRecord {revision:1,feedback:value.clone(),text:feedback_text(&value,&limits).unwrap(),
        started:false,accepted_at:None,consumed_at:None,native_input:None};
    persist_input(&state.join("input.json"),&record,&limits).unwrap();
    let mut input=Input {directory:state.clone(),record}; input.mark_dispatch(&limits).unwrap();
    let record=read_input(&state.join("input.json"),&limits).unwrap().unwrap();
    let mut reopened=Input {directory:state,record};
    assert!(reopened.mark_dispatch(&limits).is_err());
    let mut bounded=limits.clone(); bounded.governance.as_mut().unwrap().reason_bytes=1;
    assert!(feedback_text(&value,&bounded).unwrap_err().contains("governance.reason_bytes"));
}

#[tokio::test]
async fn successor_initial_reason_requires_exact_original_subject_and_new_caller() {
    let limits=policy();let state=area();let (_cancel,control)=control(&limits);
    let mut current=caller();current.attempt_id="completion-attempt-2".into();current.generation=2;
    let host=Host::open(ApiClient::new("http://127.0.0.1:9",Arc::new(limits.clone())).unwrap(),current.clone(),"completion-session",&state).unwrap();
    let mut value=feedback(CompletionBoundary::StoppedResult {report_id:"old-final-candidate".into()},CompletionTarget::InitialNativeRequest {request_index:1});
    value.continuation.caller=current.clone();value.continuation.context_binding=current.attempt_id.clone();
    value.continuation.begin_operation_id=begin_id(&current,&value.evaluation.decision).unwrap();
    initial_begin_receipt(&state,&limits,&value,true);
    let resume=CompletionResume {evaluation:value.evaluation.clone(),reset_event:None};
    let mut changed=resume.clone();changed.evaluation.state.subject.session_id="other-session".into();
    assert!(host.begin_initial(&changed,&current.attempt_id,value.continuation.target.clone(),None,&control).await.is_err());
    assert!(host.begin_initial(&resume,&current.attempt_id,CompletionTarget::InitialNativeRequest {request_index:2},None,&control).await.is_err());
    let mut input=host.begin_initial(&resume,&current.attempt_id,value.continuation.target.clone(),None,&control).await.unwrap().unwrap();
    assert_eq!(input.feedback().evaluation,resume.evaluation);
    input.mark_dispatch(&limits).unwrap();
    let first=CompletionBoundary::NativeResponse {request_index:1};
    acknowledge_receipt(&state,&limits,&value,first.clone());
    let native_input=input.native_consumption(&binding(),1,3,1024,&limits).unwrap();
    let retained=host.acknowledged(&mut input,first,None,Some(native_input),&control).await.unwrap();
    assert_eq!(retained.caller,current);
    assert_eq!(retained.decision,resume.evaluation.decision);
}

#[tokio::test]
async fn native_ack_keeps_original_target_and_exact_retained_rejection_chain() {
    for initial in [false,true] {
        let limits=policy();let state=area();let host=host(&state,&limits);let (_cancel,control)=control(&limits);
        let first=if initial {1}else{2};
        let target=if initial {CompletionTarget::InitialNativeRequest {request_index:1}}
            else {CompletionTarget::NativeRequest {request_index:first}};
        let value=feedback(CompletionBoundary::NativeResponse {request_index:1},target.clone());
        let record=InputRecord {revision:1,feedback:value.clone(),text:feedback_text(&value,&limits).unwrap(),
            started:false,accepted_at:None,consumed_at:None,native_input:None};
        let directory=host.directory.join(key(&value.evaluation.boundary).unwrap());
        process::private_directory(&directory).unwrap();
        persist_input(&directory.join("input.json"),&record,&limits).unwrap();
        let mut input=Input {directory,record};input.mark_dispatch(&limits).unwrap();
        for index in first..first+2 {
            input.record_native_rejection(&binding(),NativeCompletionRejection {
                request_index:index,operation_id:format!("original-rejection-{index}")},first+2,1024,&limits).unwrap();
            // A confirmed rejection is not consumption and creates no ACK journal.
            assert!(input.record.consumed_at.is_none());
            assert!(!input.directory.join("acknowledge.json").exists());
            assert_eq!(input.feedback().continuation.target,target);
        }
        let proof=input.native_consumption(&binding(),first+2,first+2,1024,&limits).unwrap();
        let boundary=CompletionBoundary::NativeResponse {request_index:first+2};
        assert!(host.acknowledged(&mut input,boundary.clone(),None,None,&control).await.is_err());
        let mut changed=proof.clone();changed.request_binding.sha256="cd".repeat(32);
        assert!(host.acknowledged(&mut input,boundary.clone(),None,Some(changed),&control).await.is_err());
        acknowledge_native_receipt(&state,&limits,&value,boundary.clone(),Some(proof.clone()));
        let ack=host.acknowledged(&mut input,boundary.clone(),None,Some(proof.clone()),&control).await.unwrap();
        assert_eq!(ack.target,target);
        assert_eq!(ack.acknowledged.unwrap().native_input,Some(proof));
        assert_eq!(read_input(&input.directory.join("input.json"),&limits).unwrap().unwrap().consumed_at,Some(boundary));
    }
}

#[test]
fn native_retry_rejects_changed_binding_gaps_duplicates_and_limits_without_advancing_input() {
    let limits=policy();let state=area();
    let value=feedback(CompletionBoundary::NativeResponse {request_index:1},CompletionTarget::NativeRequest {request_index:2});
    let record=InputRecord {revision:1,feedback:value.clone(),text:feedback_text(&value,&limits).unwrap(),
        started:false,accepted_at:None,consumed_at:None,native_input:None};
    persist_input(&state.join("input.json"),&record,&limits).unwrap();
    let mut input=Input {directory:state,record};input.mark_dispatch(&limits).unwrap();
    let rejection=|index,id:&str|NativeCompletionRejection {request_index:index,operation_id:id.into()};
    assert!(input.record_native_rejection(&binding(),rejection(3,"gap"),4,1024,&limits).is_err());
    input.record_native_rejection(&binding(),rejection(2,"original"),4,1024,&limits).unwrap();
    let retained=input.record.clone();
    let mut changed=binding();changed.sha256="cd".repeat(32);
    assert!(input.record_native_rejection(&changed,rejection(3,"changed"),4,1024,&limits).is_err());
    assert!(input.record_native_rejection(&binding(),rejection(3,"original"),4,1024,&limits).is_err());
    assert!(input.record_native_rejection(&binding(),rejection(3,"limit"),3,1024,&limits).is_err());
    let mut bounded=limits.clone();bounded.governance.as_mut().unwrap().scan_rows=1;
    assert!(input.record_native_rejection(&binding(),rejection(3,"rows"),4,1024,&bounded).is_err());
    assert!(input.native_consumption(&changed,3,4,1024,&limits).is_err());
    assert!(input.native_consumption(&binding(),2,4,1024,&limits).is_err());
    assert!(input.record==retained);
    let reopened=read_input(&input.directory.join("input.json"),&limits).unwrap().unwrap();
    assert!(reopened==retained);
    let mut reopened=Input {directory:input.directory,record:reopened};
    assert!(reopened.mark_dispatch(&limits).is_err());
}

#[tokio::test]
async fn exhausted_original_decision_requires_exact_command_reset_and_canonical_begin() {
    for reset in [false,true] {
        let limits=policy();let state=area();let (_cancel,control)=control(&limits);
        let mut current=caller();current.attempt_id="successor".into();current.generation=2;
        let host=Host::open(ApiClient::new("http://127.0.0.1:9",Arc::new(limits.clone())).unwrap(),current.clone(),"completion-session",&state).unwrap();
        let mut value=feedback(CompletionBoundary::StoppedResult {report_id:"old-final".into()},CompletionTarget::InitialNativeRequest {request_index:1});
        value.evaluation.state.disposition=CompletionDisposition::Blocked {continuation_eligible:false,exhausted:true};
        value.continuation.caller=current.clone();value.continuation.context_binding=current.attempt_id.clone();
        value.continuation.begin_operation_id=begin_id(&current,&value.evaluation.decision).unwrap();
        initial_begin_receipt(&state,&limits,&value,true);
        let resume=CompletionResume {evaluation:value.evaluation.clone(),reset_event:reset.then(||crate::collaboration::workflow::EventId {
            database_id:"fixture-database".into(),product_revision:8,ordinal:0})};
        let result=host.begin_initial(&resume,&current.attempt_id,value.continuation.target.clone(),None,&control).await;
        if reset {assert_eq!(result.unwrap().unwrap().feedback().evaluation,resume.evaluation);}
        else {assert!(result.is_err());}
    }
}
