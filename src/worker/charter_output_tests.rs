use super::*;
fn limits()->RuntimeLimits{
    let mut l=RuntimeLimits::seed().unwrap();l.charter=Some(crate::limits::CharterLimits{
        record_bytes:8*1024*1024,source_bytes:65536,files:8,clauses:32,metadata_bytes:16384,
        references:32,reply_items:32,tool_calls:8,reference_depth:8,records:4096,page_items:1,page_bytes:16*1024*1024});
    l.transport.body_bytes=32*1024*1024;l.worker.mutation_record_bytes=80*1024*1024;l.execution.native.response_wire_bytes=16*1024*1024;l.execution.native.helper_value_bytes=16*1024*1024;l.execution.native.context_bytes=16*1024*1024;l.execution.native.request_bytes=16*1024*1024;l.controller.collaboration_page_bytes=16*1024*1024;
    // Scan units cover original Value/key bytes plus bounded numeric rendering;
    // this fixture selects the matching budget instead of bypassing the guard.
    l.worker.credential_scan_units=l.execution.native.response_wire_bytes.checked_add(25*l.execution.native.json_nodes).unwrap();l
}
fn state(label:&str)->PathBuf{
    static N:AtomicU64=AtomicU64::new(0);
    let parent=std::env::var_os("BORG_TEST_ARTIFACT").map(PathBuf::from).unwrap_or_else(std::env::temp_dir);
    std::fs::create_dir_all(&parent).unwrap();
    let p=parent.join(format!("borg-output-retain-{}-{label}-{}-{}",std::process::id(),process::now_ms(),N.fetch_add(1,Ordering::Relaxed)));
    std::fs::create_dir(&p).unwrap();process::private_directory(&p).unwrap();p
}
fn who()->crate::mcp::Caller{crate::mcp::Caller{task_id:"task".into(),attempt_id:"attempt".into(),generation:1,worker_id:"worker".into(),incarnation:"one".into()}}
fn observed(id:&str,output:Option<serde_json::Value>)->CharterObservation{CharterObservation::Reply{turn_id:id.into(),reply:CharterReplyObservation{output,output_reference:None,response_id:Some("response".into()),output_item_ids:vec!["one".into()],tool_call_ids:vec![],declaration:Some(serde_json::json!("passive malformed references"))}}}
fn bytes(state:&Path,m:&Manifest)->Vec<u8>{let mut bytes=Vec::new();if let Some(r)=&request(&m.observation).unwrap().1.output_reference{for i in 0..r.chunks{bytes.extend(read(&chunk_path(&directory(state,&m.operation_id),i),r.chunk_bytes as usize).unwrap());}}bytes}
#[test]
fn nine_mib_original_is_lossless_and_manifest_journal_is_small(){
    let s=state("large");let l=limits();let original=serde_json::json!([{"id":"one","text":"x".repeat(9*1024*1024)},null,{"ordered":[3,2,1]}]);
    let m=retain(&s,&l,l.execution.native.response_wire_bytes,who(),"turn-reply".into(),observed("turn",Some(original.clone()))).unwrap();
    assert_eq!(serde_json::from_slice::<serde_json::Value>(&bytes(&s,&m)).unwrap(),original);assert!(serde_json::to_vec(&m).unwrap().len()<4096);verify_files(&s,&l,l.execution.native.response_wire_bytes,&m).unwrap();
    let same=retain(&s,&l,l.execution.native.response_wire_bytes,who(),"turn-reply".into(),observed("turn",Some(original))).unwrap();assert!(m==same);
}
#[test]
fn absent_null_cap_and_compaction_keep_distinct_original_values(){
    let s=state("values");let l=limits();
    let absent=retain(&s,&l,l.execution.native.response_wire_bytes,who(),"absent-reply".into(),observed("absent",None)).unwrap();assert!(request(&absent.observation).unwrap().1.output_reference.is_none());
    let null=retain(&s,&l,l.execution.native.response_wire_bytes,who(),"null-reply".into(),observed("null",Some(serde_json::Value::Null))).unwrap();assert_eq!(bytes(&s,&null),b"null");
    let prefix=serde_json::json!([{"type":"function_call","arguments":"{unfinished","call_id":"original"}]);
    let cap=retain(&s,&l,l.execution.native.response_wire_bytes,who(),"cap-reply".into(),observed("cap",Some(prefix.clone()))).unwrap();assert_eq!(serde_json::from_slice::<serde_json::Value>(&bytes(&s,&cap)).unwrap(),prefix);
    let CharterObservation::Reply{reply,..}=observed("ignored",Some(serde_json::json!(["separate summary response"])))else{unreachable!()};
    let summary=retain(&s,&l,l.execution.native.response_wire_bytes,who(),"summary-reply".into(),CharterObservation::CompactionReply{run_id:"summary".into(),reply,summary:Some("derived small summary".into())}).unwrap();
    assert_ne!(bytes(&s,&summary),bytes(&s,&cap));assert!(request(&summary.observation).unwrap().1.declaration.as_ref().unwrap().is_string());
}
#[test]
fn incomplete_conflicting_extra_order_and_identity_files_refuse_without_repair(){
    let l=limits();for defect in 0..6{let s=state(&format!("corrupt-{defect}"));let original=serde_json::json!("z".repeat(bound(&l).unwrap()+10));let mut m=retain(&s,&l,l.execution.native.response_wire_bytes,who(),"turn-reply".into(),observed("turn",Some(original))).unwrap();let dir=directory(&s,&m.operation_id);let p=chunk_path(&dir,0);
        match defect{
            0=>{std::fs::remove_file(&p).unwrap();},
            1=>{let mut b=read(&p,bound(&l).unwrap()).unwrap();b[0]^=1;std::fs::write(&p,b).unwrap();},
            2=>{std::fs::copy(&p,chunk_path(&dir,99)).unwrap();},
            3=>{std::fs::rename(&p,dir.join("0000000000.pending")).unwrap();},
            4=>{reply_mut(&mut m.observation).unwrap().output_reference.as_mut().unwrap().request_id="other".into();},
            _=>{let a=std::fs::read(&p).unwrap();let b=std::fs::read(chunk_path(&dir,1)).unwrap();std::fs::write(&p,b).unwrap();std::fs::write(chunk_path(&dir,1),a).unwrap();},
        }
        let before=std::fs::read(dir.join("manifest.json")).unwrap();assert!(verify_files(&s,&l,l.execution.native.response_wire_bytes,&m).is_err(),"defect {defect}");assert_eq!(std::fs::read(dir.join("manifest.json")).unwrap(),before);
    }
}
#[test]
fn original_output_and_journal_one_over_are_refused_without_a_provider_call(){
    let s=state("bounds");let mut l=limits();l.execution.native.response_wire_bytes=7;
    let exact=retain(&s,&l,l.execution.native.response_wire_bytes,who(),"exact-reply".into(),observed("exact",Some(serde_json::json!("12345")))).unwrap();assert_eq!(bytes(&s,&exact),b"\"12345\"");
    assert!(retain(&s,&l,l.execution.native.response_wire_bytes,who(),"over-reply".into(),observed("over",Some(serde_json::json!("123456")))).is_err());
    let mut l=limits();l.worker.mutation_record_bytes=l.transport.body_bytes;
    assert!(retain(&s,&l,l.execution.native.response_wire_bytes,who(),"envelope-reply".into(),observed("envelope",Some(serde_json::Value::Null))).is_err());
}

thread_local!{static SYNC_CUT:std::cell::Cell<Option<usize>>=const{std::cell::Cell::new(None)};}
pub(super) fn sync_cut(point:usize)->Result<(),String>{SYNC_CUT.with(|v|if v.get()==Some(point){v.set(None);Err(format!("original output directory sync cut {point}"))}else{Ok(())})}
#[tokio::test]
async fn failed_parent_directory_sync_is_reestablished_before_any_outbound_upload(){
    let s=state("parent-sync");let l=limits();let original=observed("turn",Some(serde_json::json!(["exact reply"])));
    // A visible manifest after a failed parent sync is not a durable send grant.
    for point in [1,2]{SYNC_CUT.with(|v|v.set(Some(point)));assert!(retain(&s,&l,l.execution.native.response_wire_bytes,who(),"turn-reply".into(),original.clone()).err().unwrap().contains("directory sync cut"));}
    let m=retain(&s,&l,l.execution.native.response_wire_bytes,who(),"turn-reply".into(),original).unwrap();
    let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api=ApiClient::new(format!("http://{}",listener.local_addr().unwrap()),Arc::new(l.clone())).unwrap();
    for point in [1,2]{SYNC_CUT.with(|v|v.set(Some(point)));let error=upload(&api,&s,&l,l.execution.native.response_wire_bytes,&m).await.unwrap_err();assert!(error.contains("directory sync cut"));assert!(tokio::time::timeout(Duration::from_millis(10),listener.accept()).await.is_err());}
    verify_files(&s,&l,l.execution.native.response_wire_bytes,&m).unwrap();sync_ancestry(&s,&directory(&s,&m.operation_id)).unwrap();
}

#[test]
fn accepted_native_override_owns_output_extent_without_replacing_global_journal_policy(){
    let s=state("accepted-override");let mut l=limits();l.execution.native.response_wire_bytes=128;
    let original=serde_json::json!("x".repeat(1024));
    let m=retain(&s,&l,2048,who(),"override-reply".into(),observed("override",Some(original.clone()))).unwrap();
    assert!(verify_files(&s,&l,128,&m).is_err());verify_files(&s,&l,2048,&m).unwrap();
    assert_eq!(serde_json::from_slice::<serde_json::Value>(&bytes(&s,&m)).unwrap(),original);
}

#[test]
fn output_chunk_accounts_for_exact_original_lookup_ceiling_before_send(){
    let s=state("lookup-envelope");let l=limits();
    let m=retain(&s,&l,l.execution.native.response_wire_bytes,who(),"lookup-reply".into(),observed("lookup",Some(serde_json::json!("x".repeat(600_000))))).unwrap();
    assert_eq!(bound(&l).unwrap(),l.controller.outcome_bytes/4);
    let reference=request(&m.observation).unwrap().1.output_reference.as_ref().unwrap();
    let bytes=read(&chunk_path(&directory(&s,&m.operation_id),0),reference.chunk_bytes as usize).unwrap();
    let chunk=CharterOutputChunk{reference:reference.clone(),index:0,data:hex(&bytes)};
    let ack=crate::charter::CharterOutputChunkReceipt{reference:reference.clone(),index:0,sha256:sha(&bytes),bytes:bytes.len()as u32};
    chunk_preflight(&l,&m,&chunk,&ack).unwrap();
    let required=chunk_frames(&m,&chunk,&ack).unwrap().iter().map(|f|bounded_json_size(&f.outcome,usize::MAX).unwrap()).max().unwrap();
    let mut exact=l.clone();exact.controller.outcome_bytes=required;
    chunk_preflight(&exact,&m,&chunk,&ack).unwrap();
    exact.controller.outcome_bytes-=1;
    assert!(chunk_preflight(&exact,&m,&chunk,&ack).unwrap_err().contains("controller.outcome_bytes"));
    // The formerly selected 512KiB raw piece fits the original journal/body,
    // but its hex-bearing original lookup cannot fit the independent ceiling.
    let raw=vec![b'x';512*1024];let mut old=reference.clone();old.bytes=raw.len()as u64;old.chunk_bytes=raw.len()as u32;old.chunks=1;old.sha256=sha(&raw);
    let chunk=CharterOutputChunk{reference:old.clone(),index:0,data:hex(&raw)};
    let ack=crate::charter::CharterOutputChunkReceipt{reference:old,index:0,sha256:sha(&raw),bytes:raw.len()as u32};
    mutations::prepare(operation(&m,CharterObservation::OutputChunk{chunk:chunk.clone()},chunk_operation(&chunk.reference,0)),0,&l).unwrap();
    assert!(chunk_preflight(&l,&m,&chunk,&ack).unwrap_err().contains("controller.outcome_bytes"));
}

#[test]
fn canonical_expansion_retains_and_reopens_under_original_resolved_policy() {
    let s=state("canonical-expansion");let mut l=limits();
    l.charter.as_mut().unwrap().record_bytes=128*1024;
    let mut selected=l.execution.native.clone();selected.response_wire_bytes=65_536;selected.json_nodes=256;
    selected.context_bytes=65_536;selected.helper_value_bytes=65_536;selected.output_bytes=16_384;
    selected.validate().unwrap();
    // Original API/journal policy deliberately has a different native bound.
    l.execution.native.response_wire_bytes=4096;l.execution.native.helper_value_bytes=4096;
    l.execution.native.context_bytes=4096;l.execution.native.output_bytes=1024;
    l.execution.memory_notice.notice_bytes=1024;l.validate().unwrap();
    let template=include_str!("testdata/canonical-output-expansion.json");
    let wire=template.replace("__PADDING__",&" ".repeat(selected.response_wire_bytes-template.len()+11));
    let value:serde_json::Value=serde_json::from_str(&wire).unwrap();let original=value["output"].clone();
    let expected=serde_json::to_vec(&original).unwrap();assert!(expected.len()>selected.response_wire_bytes);
    assert!(retain(&s,&l,selected.response_wire_bytes,who(),"turn-reply".into(),observed("turn",Some(original.clone()))).is_err());
    assert!(!root(&s).exists());
    let maximum=selected.canonical_output_bytes().unwrap();
    let m=retain(&s,&l,maximum,who(),"turn-reply".into(),observed("turn",Some(original))).unwrap();
    assert_eq!(bytes(&s,&m),expected);
    let over=serde_json::json!("x".repeat(maximum-1)); // Quotes make exactly maximum+1.
    assert!(retain(&s,&l,maximum,who(),"over-reply".into(),observed("over",Some(over))).is_err());
    let mut task=TaskInput { instruction_source: None, pull_request: None, resolved_limits:None,repository_read:None,instruction:"original".into(),
        definition:Definition{charter:Some(crate::charter::DefinitionCharter::Inline{source:crate::charter::CharterSource{files:vec![crate::charter::CharterFile{file:"charter.md".into(),text:"# Rule\n\nAgent MUST preserve the original.\n".into()}],clauses:vec![crate::charter::CharterClauseRef{file:"charter.md".into(),section:"Rule".into(),sentence:"Agent MUST preserve the original.".into()}]}}),id:"agent".into(),revision:"r1".into(),executor:ExecutorKind::Native,
            execution_profile:Some(ExecutionProfile::CollaborationV1{web_search:None}),inference:Some(InferenceSettings::selected()),
            limit_overrides:Some(Box::new(crate::limits::ExecutionLimitOverrides{native:Some(selected.clone()),..Default::default()}))},
        repository:None,acceptance_commands:vec![],output_schema:Some(serde_json::json!({"type":"integer"})),
        limits:Limits{deadline_unix_ms:process::now_ms()+60_000,model_requests:1,output_bytes:128,result_bytes:1024,max_delegation_depth:1},
        dependencies:vec![],native_workflow:None};
    task.resolved_limits=Some(l.resolve_task(&task).unwrap());
    let attempt=Attempt{id:"attempt".into(),session_id:"session".into(),task_id:"task".into(),generation:1,worker_id:"worker".into(),worker_incarnation:"one".into(),phase:AttemptPhase::Running,deadline_unix_ms:task.limits.deadline_unix_ms,stopped:false,uncertain:false};
    let command=WorkerCommand::RunAttempt{command_id:"command".into(),session:(crate::protocol::Session{id:"session".into(),agent_id:"agent".into(),definition:task.definition.clone(),task_id:"task".into()}).into(),attempt,task: (task).into(),checkpoint:None,continuation:None,workflow_continuation:None,completion_continuation:None};
    std::fs::create_dir(s.join("commands")).unwrap();let command_path=s.join("commands").join(format!("{}.json",component("command")));
    record_launch(&command_path,&command,&l).unwrap();let command_before=std::fs::read(&command_path).unwrap();
    atomic_json(&s.join("format.json"),&serde_json::json!({"format_revision":13})).unwrap();
    // The actual startup inventory used by recover selects both original B and N.
    let mut startup=RuntimeLimits::seed().unwrap();startup.execution.native.json_nodes=1;
    let manifest_path=directory(&s,&m.operation_id).join("manifest.json");
    let manifest_before=std::fs::read(&manifest_path).unwrap();
    // The output carrier predates both standalone publication21 and union22.
    // Unknown future23 and pre-carrier12 remain strict, read-only refusals.
    for revision in [12,23] {
        atomic_json(&s.join("format.json"),&serde_json::json!({"format_revision":revision})).unwrap();
        let marker=std::fs::read(s.join("format.json")).unwrap();
        assert!(admit(&s,&startup,"worker").unwrap_err().contains("new output records under historical worker marker"));
        assert_eq!(std::fs::read(s.join("format.json")).unwrap(),marker);
        assert_eq!(std::fs::read(&manifest_path).unwrap(),manifest_before);
        assert_eq!(std::fs::read(&command_path).unwrap(),command_before);
        assert_eq!(bytes(&s,&m),expected);
    }
    for revision in [13,14,20,21,22] {
        atomic_json(&s.join("format.json"),&serde_json::json!({"format_revision":revision})).unwrap();
        let marker=std::fs::read(s.join("format.json")).unwrap();
        admit(&s,&startup,"worker").unwrap();
        assert_eq!(std::fs::read(s.join("format.json")).unwrap(),marker);
        assert_eq!(std::fs::read(&manifest_path).unwrap(),manifest_before);
        assert_eq!(std::fs::read(&command_path).unwrap(),command_before);
        assert_eq!(bytes(&s,&m),expected);
    }
    let restored=inventory(&s,&startup,"worker").unwrap();assert_eq!(restored.len(),1);
    assert_eq!(restored[0].1,l);assert_eq!(restored[0].2,maximum);
    verify_files(&s,&restored[0].1,restored[0].2,&restored[0].0).unwrap();
    assert_eq!(bytes(&s,&restored[0].0),expected);assert_eq!(std::fs::read(command_path).unwrap(),command_before);
    let first=chunk_path(&directory(&s,&m.operation_id),0);let before=std::fs::read(&first).unwrap();
    let mut corrupt=before.clone();corrupt[0]^=1;std::fs::write(&first,&corrupt).unwrap();
    assert!(admit(&s,&startup,"worker").is_err());assert_eq!(std::fs::read(&first).unwrap(),corrupt);
}
