//! Controlled local HTTP statuses through actual controller, worker and MCP.
//! This child reuses the retained charter_worker Lab/source/receipt helpers.
use super::*;
use borg_runtime::{limits::{NativeRetryLimits,NativeCircuitLimits},native_loop_contract as contract};

#[derive(Clone,Copy,PartialEq,Eq)]
enum Case {CompactionRetry,OrdinaryRetry,Fatal,Unknown}
struct StatusPeer {
    endpoint:String,
    bodies:Arc<Mutex<Vec<Vec<u8>>>>,
    owner:Option<tokio::task::JoinHandle<()>>,
}
impl StatusPeer {
    async fn start(api:ApiClient,task_id:String,policy:Arc<RuntimeLimits>,case:Case,
        second_entered:Arc<tokio::sync::Notify>,second_release:Arc<tokio::sync::Notify>)->Self {
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint=format!("http://{}/v1/responses",listener.local_addr().unwrap());
        let bodies=Arc::new(Mutex::new(Vec::new()));let captured=bodies.clone();
        let owner=tokio::spawn(async move {
            loop {
                let (mut stream,_)=listener.accept().await.unwrap();
                let mut head=Vec::new();
                while !head.ends_with(b"\r\n\r\n") {head.push(stream.read_u8().await.unwrap());assert!(head.len()<=policy.transport.http_header_bytes);}
                let head=String::from_utf8(head).unwrap();assert!(head.starts_with("POST /v1/responses "));
                let length=head.lines().find_map(|line|{let(k,v)=line.split_once(':')?;k.eq_ignore_ascii_case("content-length").then(||v.trim().parse::<usize>().unwrap())}).unwrap();
                assert!(length<=policy.execution.native.request_bytes);
                let mut bytes=vec![0;length];stream.read_exact(&mut bytes).await.unwrap();
                let request:Value=serde_json::from_slice(&bytes).unwrap();
                assert_eq!(request["model"],"gpt-6-astra");assert_eq!(request["reasoning"],json!({"effort":"max"}));
                let index={let mut all=captured.lock().unwrap();let n=all.len();assert!(n<5,"unexpected additional POST");all.push(bytes);n};
                // Actual debit must already exist before this physical POST's reply.
                let task=get(&api,&task_id).await;let attempt=task.attempt.as_ref().unwrap();
                let charged=receipt(&api,&contract::reservation_id(&attempt.id,u32::try_from(index+1).unwrap())).await.unwrap();
                let Operation::ObserveWorker{report}=serde_json::from_value(charged.intended).unwrap() else{panic!("not original debit")};
                assert_eq!(report.attempt_id,attempt.id);assert_eq!(report.generation,attempt.generation);
                assert!(matches!(report.observation,Observation::Usage{model_requests,..} if model_requests==u32::try_from(index+1).unwrap()));
                let ordinary_success=if case==Case::OrdinaryRetry {2}else{1};
                let compaction=if case==Case::OrdinaryRetry {3}else{2};
                let (status,extra,body)=if index==0 {
                    assert!(request.get("tools").is_some());(200,"",tool_reply(0,"r1"))
                } else if case==Case::OrdinaryRetry && index==1 {
                    assert!(request["input"].to_string().contains("Charter change"));
                    (429,"retry-after-ms: 5\r\n",json!({"error":{"code":"rate_limit_exceeded","type":"rate_limit_error"}}))
                } else if index==ordinary_success {
                    assert!(request.get("tools").is_some());assert!(request["input"].to_string().contains("r3"));
                    second_entered.notify_one();second_release.notified().await;
                    (200,"",tool_reply(index,"r3"))
                } else if index==compaction {
                    assert!(request.get("tools").is_none());assert!(request["input"].to_string().contains("r4"));
                    match case {
                        Case::CompactionRetry=>(429,"retry-after-ms: 5\r\n",json!({"error":{"code":"rate_limit_exceeded","type":"rate_limit_error"}})),
                        Case::OrdinaryRetry=>(200,"",final_reply("r4",json!({"summary":"The two original scoped reads completed."}))),
                        Case::Fatal=>(429,"",json!({"error":{"code":"insufficient_quota","type":"insufficient_quota"}})),
                        Case::Unknown=>{stream.shutdown().await.unwrap();continue;},
                    }
                } else if case==Case::CompactionRetry && index==3 {
                    assert!(request.get("tools").is_none());
                    (200,"",final_reply("r4",json!({"summary":"The two original scoped reads completed."})))
                } else if index==4 && matches!(case,Case::CompactionRetry|Case::OrdinaryRetry) {
                    assert!(request.get("tools").is_some());assert!(request["input"].to_string().contains("Derived conversation summary"));
                    (200,"",final_reply("r4",json!(7)))
                } else {panic!("unexpected POST kind/index")};
                let body=serde_json::to_vec(&body).unwrap();
                stream.write_all(format!("HTTP/1.1 {status} fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{extra}Connection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
                stream.write_all(&body).await.unwrap();stream.shutdown().await.unwrap();
            }
        });
        Self{endpoint,bodies,owner:Some(owner)}
    }
    fn count(&self)->usize{self.bodies.lock().unwrap().len()}
    async fn close(&mut self){let owner=self.owner.take().unwrap();owner.abort();if let Err(e)=owner.await{assert!(e.is_cancelled(),"peer panicked: {e}");}}
}
impl Drop for StatusPeer {fn drop(&mut self){if let Some(owner)=&self.owner{owner.abort();}}}
async fn receipt(api:&ApiClient,id:&str)->Option<OperationRecord>{
    match api.call(Operation::GetOperation{operation_id:id.into()}).await.unwrap().outcome {
        Outcome::Ok{data:Data::Operation(row)}=>Some(row),
        Outcome::Refused{code,..} if code=="not_found"=>None,
        other=>panic!("receipt {other:?}"),
    }
}
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn compaction_rate_limit_retries_identical_body_with_real_rejection_and_summary(){scenario(Case::CompactionRetry).await;}
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn ordinary_changed_charter_retry_keeps_original_rendering_and_new_occurrence(){scenario(Case::OrdinaryRetry).await;}
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn fatal_compaction_reconciles_without_reply_or_successor_post(){scenario(Case::Fatal).await;}
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn lost_compaction_reply_keeps_original_inflight_and_no_resend(){scenario(Case::Unknown).await;}

async fn scenario(case:Case){
    let mut lab=Lab::new("charter-actual-retry");let mut policy=(*lab.limits).clone();policy.charter=Some(limits());
    policy.execution.native.context_bytes=262_144;
    policy.native_retry=Some(NativeRetryLimits{initial_backoff_ms:5,backoff_multiplier:2,max_backoff_ms:20,jitter_ms:1,
        circuit:NativeCircuitLimits{failure_threshold:4,open_ms:50,max_entries:16},auth_cooldown_ms:150,auth_max_entries:16,coordination_record_bytes:32_768});
    policy.validate().unwrap();lab.limits=Arc::new(policy);
    std::fs::write(lab.root.join("limits.json"),serde_json::to_vec_pretty(&*lab.limits).unwrap()).unwrap();
    let mut controller=lab.controller("charter-retry-controller").await;
    let api=ApiClient::new(&lab.url,lab.limits.clone()).unwrap();
    for revision in ["r2","r3","r4"]{
        let mut input=lab.input(1);input.definition.revision=revision.into();input.definition.charter=Some(source(revision));
        let task=submit_identified(&api,input,&format!("source-{revision}")).await;
        assert!(matches!(api.call(Operation::CancelTask{operation_id:format!("cancel-{revision}"),task_id:task.id,expected_revision:task.revision}).await.unwrap().outcome,Outcome::Ok{..}));
    }
    let mut input=lab.input(5);input.definition.revision="r1".into();input.definition.charter=Some(source("r1"));
    let task=submit(&api,input).await;
    let second_entered=Arc::new(tokio::sync::Notify::new());let second_release=Arc::new(tokio::sync::Notify::new());
    let mut peer=StatusPeer::start(api.clone(),task.id.clone(),lab.limits.clone(),case,second_entered.clone(),second_release.clone()).await;
    let proxy=Proxy::start(lab.url.clone(),None,false,lab.limits.clone()).await;
    let held=Arc::new(HeldToolReply::new("collab_memory_get"));*proxy.held_reply.lock().unwrap()=Some(held.clone());
    let mut worker=lab.worker(&proxy.url,&peer.endpoint,"charter-retry-worker");
    tokio::time::timeout(Duration::from_secs(20),held.entered.notified()).await.unwrap();
    for revision in ["r2","r3"]{assert!(matches!(pending(&api,&task.session_id,revision).await.outcome,Outcome::Ok{..}));}
    *proxy.held_reply.lock().unwrap()=None;held.released.store(true,Ordering::SeqCst);held.release.notify_one();
    tokio::time::timeout(Duration::from_secs(20),second_entered.notified()).await.unwrap();
    assert!(matches!(pending(&api,&task.session_id,"r4").await.outcome,Outcome::Ok{..}));second_release.notify_one();
    let done=until(&api,&task.id,|task|match case {
        Case::CompactionRetry|Case::OrdinaryRetry=>task.status==TaskStatus::Succeeded,
        Case::Fatal=>task.status==TaskStatus::Blocked && task.attempt.as_ref().is_some_and(|a|a.stopped),
        Case::Unknown=>task.attempt.as_ref().is_some_and(|a|a.stopped&&a.uncertain),
    }).await;
    // The physical stop is deliberately earlier than passive reconciliation.
    // Wait only for the exact selected original observation; do not relax the
    // shared terminal helper or the complete Task equality after reopen.
    let done=match case {
        Case::Fatal=>{
            let attempt=done.attempt.as_ref().unwrap();
            let run_id=format!("{}-charter-compaction-3",attempt.id);
            let operation_id=format!("{run_id}-rejected");
            let expected=Operation::ObserveCharter {
                operation_id:operation_id.clone(),
                caller:borg_runtime::mcp::Caller {task_id:done.id.clone(),attempt_id:attempt.id.clone(),
                    generation:attempt.generation,worker_id:attempt.worker_id.clone(),incarnation:attempt.worker_incarnation.clone()},
                observation:CharterObservation::CompactionRejected {run_id:run_id.clone(),report_id:contract::rejection_id(&attempt.id,3)},
            };
            let deadline=tokio::time::Instant::now()+Duration::from_secs(30);
            loop {
                if let Some(row)=receipt(&api,&operation_id).await {
                    assert_eq!(row.id,operation_id);
                    assert_eq!(row.intended,serde_json::to_value(&expected).unwrap());
                    let Outcome::Ok {data:Data::Charter(result)}=
                        serde_json::from_value::<Outcome>(row.outcome.unwrap()).unwrap() else {panic!("original compaction rejection did not commit")};
                    let CharterOutcome::Compaction(original) = *result else { panic!("original compaction rejection did not commit") };
                    assert_eq!(original.run_id,run_id);assert_eq!(original.request_index,3);
                    assert!(original.reply.is_none()&&original.summary.is_none()&&original.disposition.is_none());
                    break;
                }
                assert_eq!(get(&api,&task.id).await,done,"blocked stopped Task changed before original reconciliation");
                assert!(tokio::time::Instant::now()<deadline,"original compaction rejection reconciliation absent");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert_eq!(get(&api,&task.id).await,done,"passive reconciliation changed original blocked Task");
            done
        },
        Case::Unknown=>{
            const NOTICE:&str="Completed result acceptance is refused; original decision retained for reconciliation";
            assert_eq!(done.status,TaskStatus::Failed);
            assert_eq!(done.blocked_reason.as_deref(),Some("execution_uncertain"));
            assert!(done.result.is_none());
            assert!(done.attempt.as_ref().is_some_and(|a|a.stopped&&a.uncertain));
            if done.progress==NOTICE {done} else {
                assert!(done.progress.is_empty(),"unexpected progress before original refusal notice");
                let mut expected=done.clone();
                expected.revision=expected.revision.checked_add(1).unwrap();
                expected.progress=NOTICE.into();
                let deadline=tokio::time::Instant::now()+Duration::from_secs(30);
                loop {
                    let current=get(&api,&task.id).await;
                    if current.progress==NOTICE {assert_eq!(current,expected);break current;}
                    assert_eq!(current,done,"unexpected terminal change before original refusal notice");
                    assert!(tokio::time::Instant::now()<deadline,"original result refusal notice absent");
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        },
        _=>done,
    };
    let attempt=done.attempt.as_ref().unwrap();let successes=matches!(case,Case::CompactionRetry|Case::OrdinaryRetry);
    let posts=if successes{5}else{3};assert_eq!(peer.count(),posts);
    // Observe canonical usage while the Controller owns SQLite for both the
    // ordinary retry and the three Call-carrier cases.
    let charged_requests=match case {
        Case::CompactionRetry|Case::Fatal|Case::Unknown=>live_usage(&api,attempt).await.0,
        Case::OrdinaryRetry=>live_usage(&api,attempt).await.0,
    };
    assert_eq!(charged_requests,posts as u32);
    let rejected_index=if case==Case::OrdinaryRetry{2}else{3};
    let rejection=receipt(&api,&contract::rejection_id(&attempt.id,rejected_index)).await;
    if case==Case::Unknown{assert!(rejection.is_none());}else{
        let row=rejection.unwrap();assert!(matches!(serde_json::from_value::<Outcome>(row.outcome.unwrap()).unwrap(),Outcome::Ok{..}));
        let Operation::ObserveWorker{report}=serde_json::from_value(row.intended).unwrap()else{panic!("not rejection report")};
        let Observation::NativeRequestRejected{request_index,request_binding,rejection,..}=report.observation else{panic!("not typed rejection")};
        assert_eq!(request_index,rejected_index);
        assert_eq!(rejection.kind,if case==Case::Fatal {borg_runtime::native::RejectionKind::QuotaExhausted}else{borg_runtime::native::RejectionKind::RateLimited});
        let all=peer.bodies.lock().unwrap();let body=&all[rejected_index as usize-1];
        let digest=aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256,body);
        assert_eq!(request_binding.bytes,body.len() as u64);
        assert_eq!(request_binding.sha256,digest.as_ref().iter().map(|byte|format!("{byte:02x}")).collect::<String>());
        if successes{assert_eq!(body,&all[rejected_index as usize],"retry changed original serialized input");}
    }
    if case==Case::OrdinaryRetry {
        let turns=match api.call(Operation::QueryCharter{query:CharterQuery::Turns{session_id:task.session_id.clone(),after:None,limit:10}}).await.unwrap().outcome {
            Outcome::Ok{data:Data::CharterQuery(result)}=>match *result { CharterQueryResult::Turns(page) => page.items, other => panic!("turns {other:?}") },other=>panic!("turns {other:?}"),
        };
        let rejected=turns.iter().find(|t|t.boundary.request_index==2).unwrap();
        let successor=turns.iter().find(|t|t.boundary.request_index==3).unwrap();
        assert_ne!(rejected.boundary.turn_id,successor.boundary.turn_id);
        assert_eq!(rejected.boundary.effective,successor.boundary.effective);
        assert_eq!(rejected.boundary.effective.reference,reference("r3"));
        assert!(rejected.reply.is_none()&&rejected.completed_by.is_none());
        assert!(successor.reply.is_some()&&successor.completed_by.is_some());
    }
    if case==Case::Fatal {
        let row=receipt(&api,&format!("{}-native-retry-block-3",attempt.id)).await.unwrap();
        assert!(matches!(serde_json::from_value::<Outcome>(row.outcome.unwrap()).unwrap(),Outcome::Ok{..}));
        let Operation::ObserveWorker{report}=serde_json::from_value(row.intended).unwrap()else{panic!("not block report")};
        let Observation::NativeRetryBlocked{block}=report.observation else{panic!("not typed block")};
        assert_eq!(block.request_index,3);assert_eq!(block.source_operation_id,contract::rejection_id(&attempt.id,3));
        assert_eq!(block.reason,borg_runtime::native::RetryBlockReason::QuotaExhausted);
    }
    let first_compaction=if case==Case::OrdinaryRetry{4}else{3};
    let run=match api.call(Operation::QueryCharter{query:CharterQuery::Compaction{run_id:format!("{}-charter-compaction-{first_compaction}",attempt.id)}}).await.unwrap().outcome{
        Outcome::Ok{data:Data::CharterQuery(result)}=>match *result { CharterQueryResult::Compaction(run) => run, other => panic!("compaction {other:?}") },other=>panic!("run {other:?}"),
    };
    if case!=Case::OrdinaryRetry{assert!(run.reply.is_none()&&run.summary.is_none()&&run.disposition.is_none());}
    let head=binding(&api,&task.session_id).await;
    if case==Case::Unknown{assert!(head.session.compaction_in_flight.is_some());assert!(done.result.is_none());}
    else{assert!(head.session.compaction_in_flight.is_none());}
    if successes{
        assert_eq!(done.result.as_ref().unwrap().output,Some(json!(7)));assert!(head.session.compaction_due.is_none());
        let success=match api.call(Operation::QueryCharter{query:CharterQuery::Compaction{run_id:format!("{}-charter-compaction-4",attempt.id)}}).await.unwrap().outcome{
            Outcome::Ok{data:Data::CharterQuery(result)}=>match *result { CharterQueryResult::Compaction(run) => run, other => panic!("compaction {other:?}") },other=>panic!("successful run {other:?}"),
        };
        assert!(success.reply.is_some());assert_eq!(success.disposition,Some(CompactionDisposition::CurrentSourceSummary));
        assert!(receipt(&api,&contract::completion_id(&attempt.id)).await.is_some());
    }else{assert!(done.result.is_none());assert!(receipt(&api,&contract::completion_id(&attempt.id)).await.is_none());}
    worker.stop().await;controller.stop().await;
    let mut controller=lab.controller("charter-retry-restart").await;
    assert_eq!(binding(&api,&task.session_id).await,head);
    assert_eq!(get(&api,&task.id).await,done);assert_eq!(peer.count(),posts);
    controller.stop().await;peer.close().await;
}
