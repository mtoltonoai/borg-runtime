//! Complete original output is retained independently of the small validated
//! result. Controlled POST counting proves storage recovery never asks again.
use super::*;

fn unhex(value:&str)->Vec<u8>{value.as_bytes().chunks_exact(2).map(|p|u8::from_str_radix(std::str::from_utf8(p).unwrap(),16).unwrap()).collect()}
#[derive(Default)]
struct ResultAck {report:Option<Report>,reply:Option<Reply>,replays:u64,different_report:bool}
struct OutputProxy {url:String,hold:Arc<AtomicBool>,dropped:Arc<AtomicBool>,result_ack:Option<Arc<Mutex<ResultAck>>>,_owner:Background}
impl OutputProxy {
    async fn start(upstream:String,lose_result_ack:bool)->Self{
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();let url=format!("http://{}",listener.local_addr().unwrap());
        let hold=Arc::new(AtomicBool::new(true));let dropped=Arc::new(AtomicBool::new(false));let(h,d)=(hold.clone(),dropped.clone());
        let result_ack=lose_result_ack.then(||Arc::new(Mutex::new(ResultAck::default())));let result_probe=result_ack.clone();
        let owner=tokio::spawn(async move{let client=reqwest::Client::builder().no_proxy().build().unwrap();let mut joins=tokio::task::JoinSet::new();loop{tokio::select!{
            accepted=listener.accept()=>{let(stream,_)=accepted.unwrap();let(upstream,client,h,d,result_probe)=(upstream.clone(),client.clone(),h.clone(),d.clone(),result_probe.clone());joins.spawn(async move{
                let service=service_fn(move|request:HttpRequest<Incoming>|{let(upstream,client,h,d,result_probe)=(upstream.clone(),client.clone(),h.clone(),d.clone(),result_probe.clone());async move{
                    let path=request.uri().path().to_owned();let caller=request.headers().get("x-borg-caller").cloned();let version=request.headers().get("mcp-protocol-version").cloned();let bytes=request.into_body().collect().await.unwrap().to_bytes();let value:Value=serde_json::from_slice(&bytes).unwrap();
                    let result_report=if result_probe.is_some()&&path=="/api/operations"&&value["operation"]=="observe_worker"{
                        let request:Request=serde_json::from_slice(&bytes).unwrap();
                        if let Operation::ObserveWorker{report}=request.command{if matches!(&report.observation,Observation::ResultCandidate{..}){Some(report)}else{None}}else{None}
                    }else{None};
                    if let Some(report)=&result_report{let mut probe=result_probe.as_ref().unwrap().lock().unwrap();if let Some(original)=&probe.report{if original!=report{probe.different_report=true;}}else{probe.report=Some(report.clone());}}
                    let chunk=path=="/api/operations"&&value["operation"]=="observe_charter"&&value["input"]["observation"]["kind"]=="output_chunk";
                    let lookup=path=="/api/operations"&&value["operation"]=="get_operation"&&value["input"]["operation_id"].as_str().is_some_and(|id|id.starts_with("charter-output-"));
                    if h.load(Ordering::SeqCst)&&d.load(Ordering::SeqCst)&&(chunk||lookup){return Ok::<_,io::Error>(Response::builder().status(503).body(Full::new(Bytes::new())).unwrap())}
                    let mut forward=client.post(format!("{upstream}{path}")).header("content-type","application/json").body(bytes.to_vec());if let Some(c)=caller{forward=forward.header("x-borg-caller",c);}if let Some(v)=version{forward=forward.header("mcp-protocol-version",v);}let response=forward.send().await.map_err(io::Error::other)?;
                    let status=response.status();let bytes=response.bytes().await.map_err(io::Error::other)?;
                    if chunk{let result:Reply=serde_json::from_slice(&bytes).expect("canonical chunk reply");assert!(status.is_success(),"chunk HTTP {status}");assert!(matches!(&result.outcome,Outcome::Ok{data:Data::Charter(CharterOutcome::OutputChunk(_))}),"chunk outcome {:?}",result.outcome);assert_eq!(result.commit.as_ref().map(|c|c.operation_id.as_str()),value["input"]["operation_id"].as_str(),"held ACK must be the committed original chunk");}
                    if chunk&&h.load(Ordering::SeqCst)&&!d.swap(true,Ordering::SeqCst){return Ok(Response::builder().status(503).body(Full::new(Bytes::new())).unwrap())}
                    if let Some(report)=result_report{if status.is_success(){let reply:Reply=serde_json::from_slice(&bytes).expect("canonical ResultCandidate reply");if matches!(&reply.outcome,Outcome::Ok{..}){
                        reply.validate_for(&Operation::ObserveWorker{report:report.clone()}).unwrap();assert_eq!(reply.commit.as_ref().map(|c|c.operation_id.as_str()),Some(report.report_id.as_str()));
                        let mut probe=result_probe.as_ref().unwrap().lock().unwrap();
                        if let Some(original)=&probe.reply{assert_eq!(reply.outcome,original.outcome);assert_eq!(reply.commit,original.commit);probe.replays+=1;}
                        else{probe.reply=Some(reply);return Ok(Response::builder().status(503).body(Full::new(Bytes::new())).unwrap())}
                    }}}
                    Ok(Response::builder().status(status.as_u16()).body(Full::new(bytes)).unwrap())
                }});let _=hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream),service).await;
            });},_=joins.join_next(),if !joins.is_empty()=>{}}}});
        Self{url,hold,dropped,result_ack,_owner:Background(owner)}
    }
}
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn actual_nine_mib_original_and_small_final_use_bounded_receipts(){Box::pin(scenario(false)).await;}
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn lost_chunk_ack_restart_retains_original_without_another_provider_post(){Box::pin(scenario(true)).await;}
async fn scenario(restart:bool){
    let mut lab=Lab::new(if restart{"output-chunk-restart"}else{"output-chunk-final"});
    let l=Arc::make_mut(&mut lab.limits);l.charter=Some(limits());let charter=l.charter.as_mut().unwrap();charter.record_bytes=8*1024*1024;charter.records=4096;charter.page_bytes=16*1024*1024;
    l.transport.body_bytes=32*1024*1024;l.worker.mutation_record_bytes=80*1024*1024;
    l.controller.collaboration_page_bytes=16*1024*1024;
    l.execution.native.context_bytes=16*1024*1024;l.execution.native.response_wire_bytes=16*1024*1024;
    l.execution.native.request_bytes=32*1024*1024;l.execution.native.helper_value_bytes=16*1024*1024;
    // Match the explicit accepted original response bound; the global scanner
    // remains mandatory through original output upload and restart.
    l.worker.credential_scan_units=l.execution.native.response_wire_bytes.checked_add(25*l.execution.native.json_nodes).unwrap();
    configure_native_schema(l);
    let accepted_native=l.execution.native.clone();
    if restart {
        // The exact accepted definition override is larger than the global
        // defaults used for API/journal authority, including after replacement.
        l.execution.native=RuntimeLimits::seed().unwrap().execution.native;
        let original_minimum=l.execution.native.output_bytes.checked_add(l.execution.native.decision_envelope_bytes).unwrap().max(1024*1024);
        assert!(original_minimum<9*1024*1024);
        l.execution.native.response_wire_bytes=original_minimum;
        l.execution.native.context_bytes=1024*1024;
        l.execution.native.request_bytes=1024*1024;
        l.execution.native.helper_value_bytes=original_minimum;
    }
    l.validate().unwrap();std::fs::write(lab.root.join("limits.json"),serde_json::to_vec_pretty(&*lab.limits).unwrap()).unwrap();
    let mut response=if restart {function(0,"collab_memory_set",json!({"key":"note","expected_revision":null,"value":7}))}else{final_reply("r1",json!(7))};
    if restart {
        response["output"].as_array_mut().unwrap().insert(0,json!({"id":"retained-large-commentary","type":"message","role":"assistant","phase":"commentary","status":"completed","content":[{"type":"output_text","text":"x".repeat(9*1024*1024)}]}));
    } else {
        // The original wrapper string includes these exact bytes. Existing
        // charter projection yields only result7; projected observations still
        // obey the unchanged small task output bound.
        let text=response["output"][0]["content"][0]["text"].as_str().unwrap();
        response["output"][0]["content"][0]["text"]=json!(format!("{}{text}"," ".repeat(9*1024*1024)));
    }
    let original=response["output"].clone();let expected=serde_json::to_vec(&original).unwrap();assert!(expected.len()>8*1024*1024);
    let peer=Peer::start(move |i,_|{assert_eq!(i,0,"storage recovery must not resend provider POST");Some(response.clone())}).await;
    let mut controller=lab.controller("original-output-controller").await;let api=ApiClient::new(&lab.url,lab.limits.clone()).unwrap();
    let proxy=OutputProxy::start(lab.url.clone(),!restart).await;if !restart{proxy.hold.store(false,Ordering::SeqCst);}
    let mut input=lab.input(3);input.definition.revision="r1".into();input.definition.charter=Some(source("r1"));if restart{input.definition.limit_overrides=Some(Box::new(borg_runtime::limits::ExecutionLimitOverrides{native:Some(accepted_native),..Default::default()}));}
    let task=submit(&api,input).await;
    let mut worker=lab.worker(&proxy.url,&peer.endpoint,"original-output-worker-one");
    // These large-output cases observe the original accepted absolute deadline.
    // Worker replacement never grants a new interval or changes task metadata.
    let original_deadline=task.input.limits.deadline_unix_ms;
    let remaining=original_deadline.checked_sub(process::now_ms()).filter(|v|*v>0).expect("original accepted deadline already elapsed");
    let active_end=tokio::time::Instant::now()+Duration::from_millis(remaining);
    let started=tokio::time::Instant::now();
    let turn=tokio::time::timeout_at(active_end,Box::pin(async {
    if restart{
        while !proxy.dropped.load(Ordering::SeqCst){tokio::time::sleep(Duration::from_millis(20)).await;}
        assert_eq!(peer.count(),1);assert!(matches!(memory(&api).await,Outcome::Ok{data:Data::Memory(None)}));
        let current=get(&api,&task.id).await;let a=current.attempt.as_ref().unwrap();
        let call=borg_runtime::native_loop_contract::call_operation_id(&a.id,1,1);
        assert!(matches!(api.call(Operation::GetOperation{operation_id:call}).await.unwrap().outcome,Outcome::Refused{ref code,..}if code=="not_found"));
        worker.stop().await;proxy.hold.store(false,Ordering::SeqCst);
        worker=lab.worker(&proxy.url,&peer.endpoint,"original-output-worker-two");
    }else{
        let done=loop{
            let t=get(&api,&task.id).await;
            if t.status==TaskStatus::Succeeded{break t;}
            assert!(!matches!(t.status,TaskStatus::Failed|TaskStatus::Succeeded|TaskStatus::Cancelled)||!t.attempt.as_ref().is_some_and(|a|a.stopped),"unexpected terminal task: {t:?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_eq!(done.result.as_ref().unwrap().output,Some(json!(7)));
    }
    let turn=loop{
        let value=api.call(Operation::QueryCharter{query:CharterQuery::Turns{session_id:task.session_id.clone(),after:None,limit:1}}).await.unwrap();
        if let Outcome::Ok{data:Data::CharterQuery(CharterQueryResult::Turns(page))}=value.outcome{assert!(page.next_cursor.is_none());if let Some(v)=page.items.first().filter(|v|v.reply.is_some()){break v.clone()}}
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    turn
    })).await.expect("original output work missed its original accepted deadline");
    assert!(process::now_ms()<=original_deadline,"original output work completed after its accepted deadline");
    eprintln!("original output work: restart={restart}, elapsed_ms={}, remaining_accepted_ms={}",started.elapsed().as_millis(),original_deadline.saturating_sub(process::now_ms()));
    // Reassembly and closed-owner reopening are separate finite readback work;
    // they grant neither execution time nor another provider request.
    let readback_end=tokio::time::Instant::now()+Duration::from_secs(120);
    tokio::time::timeout_at(readback_end,Box::pin(async {
    if let Some(probe)=&proxy.result_ack{
        let(report,committed)=loop{
            let observed={let probe=probe.lock().unwrap();assert!(!probe.different_report,"worker emitted another ResultCandidate identity or body");probe.reply.as_ref().filter(|_|probe.replays>0).map(|reply|(probe.report.as_ref().unwrap().clone(),reply.clone()))};
            if let Some((report,reply))=observed{let pending=lab.state().join("outbox").join(format!("{}.json",process::component(&report.report_id)));if !pending.try_exists().unwrap(){assert!(!pending.with_extension("refused").try_exists().unwrap());assert!(!pending.with_extension("outcome").try_exists().unwrap());break(report,reply);}}
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_eq!(report.task_id,task.id);assert!(matches!(&report.observation,Observation::ResultCandidate{checks_passed:true,output:Some(value),..}if value==&json!(7)));
        let lookup=api.call(Operation::GetOperation{operation_id:report.report_id.clone()}).await.unwrap();assert_eq!(lookup.commit,committed.commit);
        let Outcome::Ok{data:Data::Operation(record)}=&lookup.outcome else{panic!("original ResultCandidate receipt unavailable: {lookup:?}")};
        assert_eq!(record.id,report.report_id);assert_eq!(record.intended,serde_json::to_value(Operation::ObserveWorker{report}).unwrap());assert_eq!(record.outcome,Some(serde_json::to_value(&committed.outcome).unwrap()));assert_eq!(peer.count(),1);
    }
    let reply=turn.reply.as_ref().unwrap();assert!(reply.observation.output.is_none());let reference=reply.observation.output_reference.as_ref().unwrap();assert_eq!(reference.bytes,expected.len()as u64);
    let mut bytes=Vec::new();for index in 0..reference.chunks{let value=api.call(Operation::QueryCharter{query:CharterQuery::OutputChunk{request_id:turn.boundary.turn_id.clone(),index}}).await.unwrap();let Outcome::Ok{data:Data::CharterQuery(CharterQueryResult::OutputChunk(chunk))}=value.outcome else{panic!("chunk query differs")};assert_eq!(&chunk.reference,reference);assert_eq!(chunk.index,index);bytes.extend(unhex(&chunk.data));}
    assert_eq!(bytes,expected);assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(),original);assert_eq!(peer.count(),1);
    let original_receipt=api.call(Operation::GetOperation{operation_id:reply.reply_id.clone()}).await.unwrap();assert!(serde_json::to_vec(&original_receipt).unwrap().len()<64*1024);
    if restart{assert!(matches!(memory(&api).await,Outcome::Ok{data:Data::Memory(None)}));let call=borg_runtime::native_loop_contract::call_operation_id(&turn.boundary.caller.attempt_id,1,1);assert!(matches!(api.call(Operation::GetOperation{operation_id:call}).await.unwrap().outcome,Outcome::Refused{ref code,..}if code=="not_found"));}
    let current=get(&api,&task.id).await;assert_eq!(live_usage(&api,current.attempt.as_ref().unwrap()).await.0,1);
    worker.stop().await;if let Some(probe)=&proxy.result_ack{let probe=probe.lock().unwrap();assert!(!probe.different_report);assert!(probe.replays>0);}controller.stop().await;let mut controller=reopen_for_readback(&lab,"original-output-controller-reopen",readback_end).await;
    let reread=api.call(Operation::GetOperation{operation_id:reply.reply_id.clone()}).await.unwrap();assert_eq!(reread.outcome,original_receipt.outcome);assert_eq!(peer.count(),1);controller.stop().await;
    })).await.expect("original output readback/cleanup exceeded its separate finite bound");
    eprintln!("original output full fixture: restart={restart}, elapsed_ms={}",started.elapsed().as_millis());
}

async fn reopen_for_readback(lab:&Lab,label:&str,deadline:tokio::time::Instant)->Process{
    let process=Process::start(&["controller".into(),"--db".into(),lab.root.join("tasks.sqlite").display().to_string(),"--listen".into(),lab.address.clone(),"--origin".into(),lab.url.clone()],&lab.root,label);
    let api=ApiClient::new(&lab.url,lab.limits.clone()).unwrap();
    loop{
        if api.call(Operation::ListTasks{after:None,limit:1}).await.is_ok(){return process;}
        assert!(tokio::time::Instant::now()<deadline,"large original-output readback reopen exceeded its bound");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
