//! Real controller receipts and local repository helper; no provider or deployment.
use super::*;
use borg_runtime::{read_page::{self,ReadPage,Selection,TaskPart,DocumentPart},collaboration::{Query,QueryResult,documents::*,workflow::PrincipalRef}};
const CAP:usize=8192;
fn page(reply:Reply)->ReadPage {
    match reply.outcome {Outcome::Ok{data:Data::ReadPage(p)}=>*p,x=>panic!("expected bounded page: {x:?}")}
}
async fn query(c:&Controller,selection:Selection,cursor:Option<String>)->ReadPage {
    let data=call(c,Operation::QueryCollaboration {query:Query::Read(read_page::Request {selection,cursor,max_bytes:Some(CAP)})}).await;
    match data {Outcome::Ok{data:Data::Collaboration(QueryResult::Read(v))}=>match *v {Data::ReadPage(p)=>*p,x=>panic!("{x:?}")},x=>panic!("{x:?}")}
}
fn task_selection(t:&Task,part:TaskPart)->Selection {Selection::Task{task_id:t.id.clone(),session_id:Some(t.session_id.clone()),part}}
fn text()->String {"escape \"\\\n\t🙂e\u{301} <script>never execute</script> ".repeat(600)}
async fn read_task(c:&Controller,actor:&Caller,id:&str,target:&Task,part:&str)->Reply {
    invoke(c,&invocation(actor,id,"collab_task_get",json!({"task_id":target.id,"part":part,"max_bytes":CAP}))).await
}
async fn continue_page(c:&Controller,actor:&Caller,id:&str,cursor:&str)->Reply {
    invoke(c,&invocation(actor,id,"collab_read_continue",json!({"cursor":cursor}))).await
}
async fn reconstruct(c:&Controller,actor:&Caller,mut p:ReadPage,prefix:&str)->String {
    let identity=p.identity.clone();let mut value=String::new();let mut count=0;
    loop {
        assert_eq!(p.offset,value.len());assert_eq!(p.identity,identity);
        assert_eq!(p.returned_bytes,p.content.len());assert!(p.returned_bytes>0||p.total_bytes==0);
        assert_eq!(p.complete,p.offset+p.returned_bytes==p.total_bytes);
        value.push_str(&p.content);
        if p.complete {assert!(p.next_cursor.is_none());break;}
        count+=1;assert!(count<500,"finite accepted fixture");
        p=page(continue_page(c,actor,&format!("{prefix}-{count}"),p.next_cursor.as_ref().unwrap()).await);
    }
    assert_eq!(read_page::digest(value.as_bytes()),identity.digest);value
}
#[tokio::test]
async fn shared_read_pagination_task_instruction_result_stable_identity() {
    let c=Controller::open_with_config(path(),config()).unwrap();
    let mut input=native("pagination-task");input.instruction=text();
    let (t,a,_)=admitted(&c,"parts",input.clone()).await;
    // A distinct actual caller reads the target. Its MCP calls must not be
    // invented as calls of the target's later accepted native completion.
    let (_,_,actor)=admitted(&c,"parts-reader",native("pagination-reader")).await;
    let original=get(&c,&t.id).await;
    let first=page(read_task(&c,&actor,"instruction-first",&t,"instruction").await);
    assert!(!first.complete);assert_eq!(first.total_bytes,input.instruction.len());
    assert_eq!(reconstruct(&c,&actor,first,"instruction-next").await,input.instruction);
    assert_eq!(get(&c,&t.id).await,original,"read calls do not change task state");
    let observed=task(report(&c,&a,"pagination-output",Observation::Progress{text:"retained output".into(),output:text().chars().take(1000).collect(),truncated:true}).await);
    let output=query(&c,task_selection(&t,TaskPart::RecentOutput),None).await;
    assert_eq!(output.retained_truncation,Some(true));assert_eq!(output.total_bytes,observed.recent_output.len());
    // Complete through the original controller acceptance fixture, never seed an accepted result.
    let completed=finish_native(&c,&a,"pagination-finish").await;
    assert_eq!(completed.status,TaskStatus::Succeeded);
    let result=query(&c,task_selection(&completed,TaskPart::Result),None).await;
    assert!(result.complete);assert_eq!(serde_json::from_str::<AcceptedResult>(&result.content).unwrap(),completed.result.unwrap());
}
#[tokio::test]
async fn shared_read_pagination_original_replay_conflict_and_restart() {
    let p=path();let cfg=config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let mut input=native("pagination-replay");input.instruction=text();let (t,_,actor)=admitted(&c,"replay",input).await;
    let invocation=invocation(&actor,"original-read","collab_task_get",json!({"task_id":t.id,"part":"instruction","max_bytes":CAP}));
    let original=invoke(&c,&invocation).await;assert!(!page(original.clone()).complete);
    let duplicate=invoke(&c,&invocation).await;assert_eq!(original.outcome,duplicate.outcome);assert_eq!(original.commit,duplicate.commit);
    let mut changed=invocation.clone();changed.arguments["part"]=json!("record");
    assert!(matches!(invoke(&c,&changed).await.outcome,Outcome::Refused{code,..} if code=="operation_conflict"));
    drop(c);controller_owner_released(&p).await;
    let sql=rusqlite::Connection::open(&p).unwrap();let before:(String,String)=sql.query_row("SELECT request,outcome FROM receipts WHERE id='original-read'",[],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();drop(sql);
    let c=Controller::open_with_config(&p,cfg).unwrap();let replay=invoke(&c,&invocation).await;
    assert_eq!(replay.outcome,original.outcome);assert_eq!(replay.commit,original.commit);
    let retained=call(&c,Operation::QueryCollaboration{query:Query::Call{task_id:t.id,operation_id:invocation.operation_id}}).await;
    assert!(matches!(retained,Outcome::Ok{data:Data::Collaboration(QueryResult::Call(v))} if v.result.reply.outcome==original.outcome));
    drop(c);controller_owner_released(&p).await;let sql=rusqlite::Connection::open(&p).unwrap();
    let after:(String,String)=sql.query_row("SELECT request,outcome FROM receipts WHERE id='original-read'",[],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();assert_eq!(before,after);
}
#[tokio::test]
async fn shared_read_pagination_stale_mutable_source_without_global_revision_fence() {
    let c=Controller::open_with_config(path(),config()).unwrap();
    let mut input=native("pagination-mutable");input.instruction=text();let (t,a,actor)=admitted(&c,"mutable",input).await;
    let instruction=page(read_task(&c,&actor,"immutable-before",&t,"instruction").await);
    let record=page(read_task(&c,&actor,"mutable-before",&t,"record").await);
    assert!(!record.complete);
    let _other=submit(&c,"unrelated-task",native("unrelated-agent")).await;
    let next=page(continue_page(&c,&actor,"unrelated-revision",record.next_cursor.as_ref().unwrap()).await);
    assert_eq!(next.identity,record.identity,"global controller revision does not fence unchanged content");
    task(report(&c,&a,"change-this-task",Observation::Progress{text:"changed".into(),output:"changed".into(),truncated:false}).await);
    assert!(matches!(continue_page(&c,&actor,"stale-record",record.next_cursor.as_ref().unwrap()).await.outcome,Outcome::Refused{code,..} if code=="stale_read"));
    let next=page(continue_page(&c,&actor,"immutable-after",instruction.next_cursor.as_ref().unwrap()).await);
    assert_eq!(next.identity,instruction.identity);
    let mut bad=read_page::Cursor::decode(instruction.next_cursor.as_ref().unwrap(),c.limits().controller.collaboration_cursor_bytes).unwrap();
    bad.offset=usize::MAX;
    assert!(matches!(continue_page(&c,&actor,"past-end",&serde_json::to_string(&bad).unwrap()).await.outcome,Outcome::Refused{..}));
}
async fn document(c:&Controller,id:&str,body:&str)->DocumentView {
    match call(c,Operation::MutateDocument{operation_id:id.into(),mutation:DocumentMutation::Create{content:DocumentContent{title:"Page fixture".into(),body:body.into()},supplied_content_id:None,responsible_owner:PrincipalRef::TrustedHostOperator}}).await {
        Outcome::Ok{data:Data::DocumentMutation(DocumentMutationOutcome::Document{view})}=>view,x=>panic!("{x:?}")
    }
}
#[tokio::test]
async fn shared_read_pagination_document_version_and_scope() {
    let c=Controller::open_with_config(path(),config()).unwrap();let d=document(&c,"doc-page",&text()).await;
    let selected=Selection::Document{document_id:d.document.id.clone(),version:Some(1),part:DocumentPart::Body};
    let first=query(&c,selected.clone(),None).await;assert!(!first.complete);
    let result=call(&c,Operation::MutateDocument{operation_id:"append-page".into(),mutation:DocumentMutation::AppendVersion{document_id:d.document.id.clone(),expected_revision:d.document.revision,content:DocumentContent{title:"New".into(),body:"new exact version".into()},supplied_content_id:None}}).await;
    assert!(matches!(result,Outcome::Ok{..}));
    let next=query(&c,selected,first.next_cursor.clone()).await;assert_eq!(first.identity,next.identity);
    let other=document(&c,"other-page","other").await;
    let response=call(&c,Operation::QueryCollaboration{query:Query::Read(read_page::Request{selection:Selection::Document{document_id:other.document.id,version:Some(1),part:DocumentPart::Body},cursor:first.next_cursor,max_bytes:Some(CAP)})}).await;
    assert!(matches!(response,Outcome::Refused{code,..} if code=="invalid_read_cursor"));
}
#[tokio::test]
async fn shared_read_pagination_oversized_first_list_item_is_reachable() {
    let mut cfg=config();Arc::make_mut(&mut cfg.limits).controller.collaboration_item_bytes=4096;
    let c=Controller::open_with_config(path(),cfg).unwrap();let mut input=native("pagination-list");
    input.definition.execution_profile=Some(ExecutionProfile::CollaborationV2{web_search:None,publication:None});
    let (t,_,actor)=admitted(&c,"list",input).await;
    let first_doc=document(&c,"list-document",&text()).await;
    let second=call(&c,Operation::MutateDocument{operation_id:"list-version-two".into(),mutation:DocumentMutation::AppendVersion{document_id:first_doc.document.id.clone(),expected_revision:first_doc.document.revision,content:DocumentContent{title:"Small second version".into(),body:"second".into()},supplied_content_id:None}}).await;
    assert!(matches!(second,Outcome::Ok{..}));
    let reply=invoke(&c,&invocation(&actor,"list-first","collab_document_versions_list",json!({"document_id":first_doc.document.id,"cursor":null,"limit":2}))).await;
    let Outcome::Ok{data:Data::ReadList(list)}=reply.outcome else{panic!("{reply:?}")};
    assert_eq!(list.items.len(),1);assert!(!list.complete);let list_cursor=list.next_cursor.clone().unwrap();
    let read_page::PagedItem::Partial{content_complete,page:first}=&list.items[0] else{panic!("first item must be reachable as bounded content")};
    assert_eq!(*content_complete,first.complete);assert!(serde_json::to_vec(&list.items[0]).unwrap().len()<=4096);
    let reconstructed=reconstruct(&c,&actor,first.clone(),"item-next").await;
    assert_eq!(serde_json::from_str::<DocumentVersion>(&reconstructed).unwrap(),first_doc.version);
    let reply=invoke(&c,&invocation(&actor,"list-second-page","collab_document_versions_list",json!({"document_id":first_doc.document.id,"cursor":list_cursor,"limit":1}))).await;
    let Outcome::Ok{data:Data::DocumentQuery(DocumentQueryOutcome::Versions(list))}=reply.outcome else{panic!("{reply:?}")};
    assert_eq!(list.items.len(),1);assert_eq!(list.items[0].reference.version,2);assert!(list.next_cursor.is_none());
    let _second=submit(&c,"list-another-task",native("pagination-another")).await;
    let empty=invoke(&c,&invocation(&actor,"list-filtered-empty","collab_tasks_list",json!({"cursor":null,"limit":1,"agent_id":null,"parent_task_id":t.id}))).await;
    let Outcome::Ok{data:Data::CollaborationPage(list)}=empty.outcome else{panic!("{empty:?}")};assert!(list.items.is_empty());assert!(list.next_cursor.is_some(),"scanned filtered row still advances");
}
#[tokio::test]
async fn shared_read_pagination_repository_long_line_and_checkpoint_change() {
    let repo=Repo::new();let c=Controller::open_with_config(path(),repo.config()).unwrap();
    let (t,a,actor)=admitted(&c,"repository-page",repo.input("repository-page")).await;
    let call=invocation(&actor,"repo-first","collab_repository_read",json!({"source":"start","path":"waiting.txt","start_line":1,"max_lines":1,"max_bytes":CAP}));
    let first=page(invoke(&c,&call).await);assert!(!first.complete);assert_eq!(first.total_bytes,128*1024+1);
    let data=reconstruct(&c,&actor,first.clone(),"repo-next").await;assert_eq!(data,format!("{}\n","x".repeat(128*1024)));
    assert!(matches!(first.identity.source,read_page::Source::Repository{ref commit,ref registration,..} if commit==&repo.old && registration==&read_repository_registration().binding()));
    // Native cannot select a checkpoint. A changed source selector cannot turn a
    // start cursor into checkpoint or instructions authority.
    let mut wrong=call.clone();wrong.operation_id="repo-wrong-source".into();wrong.logical_call_id="repo-wrong-source".into();wrong.arguments["source"]=json!("checkpoint");wrong.arguments["cursor"]=json!(first.next_cursor.clone());
    assert!(matches!(invoke(&c,&wrong).await.outcome,Outcome::Refused{..}));
    let current=get(&c,&t.id).await;
    task(call_cancel(&c,&current).await);task(report(&c,&a,"repository-page-stopped",Observation::ProcessTreeStopped).await);
    assert!(matches!(continue_page(&c,&actor,"repo-cancelled",first.next_cursor.as_ref().unwrap()).await.outcome,Outcome::Refused{..}));
    let mut coding=repo.input("coding-page");coding.definition.executor=ExecutorKind::Coding;coding.repository_read=None;
    coding.repository=Some(Repository{registration:Some(read_repository_registration().binding()),url:String::new(),start_commit:repo.old.clone(),checkpoint_remote:String::new(),pull_request_target:None});coding.acceptance_commands=vec![vec!["/usr/bin/true".into()]];
    let (_,a,actor)=admitted(&c,"coding-page",coding).await;
    checkpoint(&c,&a,"page-checkpoint-one",&repo.old).await;
    let first=page(invoke(&c,&invocation(&actor,"checkpoint-first","collab_repository_read",json!({"source":"checkpoint","path":"waiting.txt","start_line":1,"max_lines":1,"max_bytes":CAP}))).await);
    checkpoint(&c,&a,"page-checkpoint-two",&repo.new).await;
    assert!(matches!(continue_page(&c,&actor,"checkpoint-stale",first.next_cursor.as_ref().unwrap()).await.outcome,Outcome::Refused{code,..} if code=="stale_read"));
}
async fn call_cancel(c:&Controller,t:&Task)->Outcome {call(c,Operation::CancelTask{operation_id:format!("cancel-{}",t.id),task_id:t.id.clone(),expected_revision:t.revision}).await}
#[tokio::test]
async fn shared_read_pagination_redaction_and_mutations_unchanged() {
    let c=Controller::open_with_config(path(),config()).unwrap();let (t,_,actor)=admitted(&c,"read-only",native("read-only")).await;
    let before=get(&c,&t.id).await;
    let raw=invoke(&c,&invocation(&actor,"memory-user-json","collab_memory_set",json!({"key":"ordinary","expected_revision":null,"value":{"items":["read_page"],"next_cursor":"not-a-cursor"}}))).await;
    assert!(matches!(raw.outcome,Outcome::Ok{..}));
    let value=invoke(&c,&invocation(&actor,"memory-get","collab_memory_get",json!({"key":"ordinary","max_bytes":CAP}))).await;
    assert!(matches!(value.outcome,Outcome::Ok{data:Data::Memory(Some(_))}),"ordinary user JSON is not a list carrier");
    let mut bad=invocation(&actor,"no-paged-write","collab_memory_set",json!({"key":"ordinary","expected_revision":1,"value":"new","max_bytes":CAP}));
    assert!(matches!(invoke(&c,&bad).await.outcome,Outcome::Refused{code,..} if code=="invalid_input"));
    bad.tool="collab_read_continue".into();bad.operation_id="no-write-cursor".into();bad.logical_call_id="no-write-cursor".into();bad.arguments=json!({"cursor":"{\"tool\":\"collab_memory_set\"}"});assert!(matches!(invoke(&c,&bad).await.outcome,Outcome::Refused{..}));
    let current=get(&c,&t.id).await;assert_eq!(current.input,before.input);assert_eq!(current.status,before.status);
    // Existing safe projection removes credential-bearing repository operands
    // before page identity is computed; unsupported credential-bearing admission remains refused.
    let mut unsafe_input=native("credential-read");unsafe_input.repository_read=Some(RepositoryReadSource{registration:None,repository:"https://user:password@example.invalid/repo".into(),start_commit:"a".repeat(40)});
    assert!(matches!(call(&c,Operation::SubmitTask{operation_id:"unsafe-read-source".into(),input:unsafe_input}).await,Outcome::Refused{..}));
    let tiny=invoke(&c,&invocation(&actor,"tiny-page","collab_task_get",json!({"task_id":t.id,"part":"instruction","max_bytes":1}))).await;
    assert!(matches!(tiny.outcome,Outcome::Refused{code,message} if code=="response_bound"&&message.contains("requested max_bytes")));
}
#[tokio::test]
async fn shared_read_pagination_previous_schema_receipts_upgrade_or_refuse() {
    // A genuine schema25 SQL shape, with no pagination vocabulary, upgrades
    // marker only and keeps the exact original receipt strings.
    let p=path();let cfg=config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let (t,_,actor)=admitted(&c,"old-read",native("old-read")).await;
    let old=invocation(&actor,"legacy-original","collab_task_get",json!({"task_id":t.id}));invoke(&c,&old).await;
    drop(c);controller_owner_released(&p).await;
    let receipts=|sql:&rusqlite::Connection|->Vec<(String,String,String)>{let mut q=sql.prepare("SELECT id,request,outcome FROM receipts ORDER BY id").unwrap();q.query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap().map(Result::unwrap).collect()};
    let sql=rusqlite::Connection::open(&p).unwrap();sql.pragma_update(None,"user_version",25).unwrap();let original=receipts(&sql);drop(sql);
    let c=Controller::open_with_config(&p,cfg.clone()).unwrap();let replay=invoke(&c,&old).await;assert!(matches!(replay.outcome,Outcome::Ok{..}));
    drop(c);controller_owner_released(&p).await;let sql=rusqlite::Connection::open(&p).unwrap();assert_eq!(receipts(&sql),original);assert_eq!(sql.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),28);drop(sql);
    let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let _=read_task(&c,&actor,"new-typed-page",&t,"instruction").await;
    drop(c);controller_owner_released(&p).await;let sql=rusqlite::Connection::open(&p).unwrap();sql.pragma_update(None,"user_version",25).unwrap();let original=receipts(&sql);drop(sql);let bytes=std::fs::read(&p).unwrap();
    let failed=Controller::open_with_config(&p,cfg);assert!(matches!(failed,Err(ref e) if e.contains("schema26")));
    assert_eq!(std::fs::read(&p).unwrap(),bytes);let sql=rusqlite::Connection::open(&p).unwrap();assert_eq!(receipts(&sql),original);assert_eq!(sql.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),25);
}


// Operator reads use the same closed projection/fitter, with no fabricated task caller.
fn operator_selection(tool:read_page::ReadTool,arguments:serde_json::Value)->Selection {
    Selection::Public{tool,arguments,item:false,caller_task_id:None}
}
async fn operator_read(c:&Controller,selection:Selection,cursor:Option<String>)->Outcome {
    call(c,Operation::QueryCollaboration{query:Query::Read(read_page::Request{selection,cursor,max_bytes:Some(CAP)})}).await
}
fn operator_data(outcome:Outcome)->Data {
    match outcome {Outcome::Ok{data:Data::Collaboration(QueryResult::Read(value))}=>*value,x=>panic!("operator read: {x:?}")}
}
async fn reconstruct_operator(c:&Controller,mut p:ReadPage)->String {
    let original=p.clone();let mut body=String::new();let mut pages=0;
    loop {
        assert_eq!(p.subject,original.subject);assert_eq!(p.identity,original.identity);
        assert_eq!(p.offset,body.len());assert_eq!(p.returned_bytes,p.content.len());
        assert_eq!(p.complete,p.offset+p.returned_bytes==p.total_bytes);
        assert!(p.returned_bytes>0);assert!(serde_json::to_vec(&Data::ReadPage(Box::new(p.clone()))).unwrap().len()<=CAP);
        body.push_str(&p.content);pages+=1;assert!(pages<500,"finite admitted source");
        if p.complete {assert!(p.next_cursor.is_none());break;}
        let next=operator_data(operator_read(c,p.subject.clone(),p.next_cursor.clone()).await);
        let Data::ReadPage(next)=next else{panic!("content continuation is a page")};p=*next;
    }
    assert_eq!(body.len(),original.total_bytes);assert_eq!(read_page::digest(body.as_bytes()),original.identity.digest);body
}
#[tokio::test]
async fn shared_read_pagination_operator_versions_preserves_partial_source_and_scope() {
    Box::pin(operator_versions_journey()).await;
}
async fn operator_versions_journey() {
    let mut cfg=config();let limits=Arc::make_mut(&mut cfg.limits);
    limits.workflow.as_mut().unwrap().page_bytes=CAP;
    limits.controller.collaboration_item_bytes=4096;limits.validate().unwrap();
    let c=Controller::open_with_config(path(),cfg).unwrap();
    let first=document(&c,"operator-version-one",&text()).await;
    assert!(serde_json::to_vec(&first.version).unwrap().len()>CAP,"admitted body genuinely exceeds page budget");
    let appended=call(&c,Operation::MutateDocument{operation_id:"operator-version-two".into(),mutation:DocumentMutation::AppendVersion{
        document_id:first.document.id.clone(),expected_revision:first.document.revision,
        content:DocumentContent{title:"Small successor".into(),body:"second exact body".into()},supplied_content_id:None}}).await;
    let Outcome::Ok{data:Data::DocumentMutation(DocumentMutationOutcome::Document{view:second})}=appended else{panic!("{appended:?}")};
    let legacy=call(&c,Operation::QueryDocument{query:DocumentQuery::Versions{document_id:first.document.id.clone(),after:None,limit:2}}).await;
    assert!(matches!(legacy,Outcome::Refused{ref code,..} if code=="response_bound"),"{legacy:?}");
    let selected=operator_selection(read_page::ReadTool::Versions, json!({"document_id":first.document.id,"cursor":null,"limit":2}));
    let data=operator_data(operator_read(&c,selected.clone(),None).await);
    assert!(serde_json::to_vec(&data).unwrap().len()<=CAP);
    let Data::ReadList(list)=data else{panic!("oversized first item must remain reachable")};
    assert_eq!(list.items.len(),1);assert!(!list.complete);
    let list_cursor=list.next_cursor.clone().unwrap();
    let read_page::PagedItem::Partial{content_complete,page:first_page}=&list.items[0] else{panic!("genuine partial item required")};
    assert!(!*content_complete);assert!(!first_page.complete);
    assert_eq!(first_page.subject,Selection::Public{tool:read_page::ReadTool::Versions,
        arguments:json!({"document_id":first.document.id,"cursor":null,"limit":1}),item:true,caller_task_id:None});
    assert!(serde_json::to_vec(&list.items[0]).unwrap().len()<=4096);
    assert_ne!(first_page.next_cursor.as_ref().unwrap(),&list_cursor,"content and list positions are distinct");
    // An unrelated new head cannot retarget the selected immutable first version.
    let appended=call(&c,Operation::MutateDocument{operation_id:"operator-version-three".into(),mutation:DocumentMutation::AppendVersion{
        document_id:first.document.id.clone(),expected_revision:second.document.revision,
        content:DocumentContent{title:"Newest".into(),body:"third exact body".into()},supplied_content_id:None}}).await;
    assert!(matches!(appended,Outcome::Ok{..}));
    let body=reconstruct_operator(&c,first_page.clone()).await;
    assert_eq!(serde_json::from_str::<DocumentVersion>(&body).unwrap(),first.version);
    let next=operator_data(operator_read(&c,operator_selection(read_page::ReadTool::Versions,
        json!({"document_id":first.document.id,"cursor":list_cursor,"limit":1})),None).await);
    let Data::DocumentQuery(DocumentQueryOutcome::Versions(next))=next else{panic!("small complete successor uses original projection")};
    assert_eq!(next.items,vec![second.version.clone()]);
    let other=document(&c,"operator-other-document","other body").await;
    let mut foreign=first_page.subject.clone();if let Selection::Public{arguments,..}=&mut foreign {arguments["document_id"]=json!(other.document.id);}
    assert!(matches!(operator_read(&c,foreign,first_page.next_cursor.clone()).await,Outcome::Refused{code,..} if code=="invalid_read_cursor"));
    let mut forged=read_page::Cursor::decode(first_page.next_cursor.as_ref().unwrap(),c.limits().controller.collaboration_cursor_bytes).unwrap();
    forged.identity.digest=read_page::digest(b"different original source");
    assert!(matches!(operator_read(&c,first_page.subject.clone(),Some(forged.encode(c.limits().controller.collaboration_cursor_bytes).unwrap())).await,Outcome::Refused{code,..} if code=="stale_read"));
    let mut caller=selected.clone();if let Selection::Public{caller_task_id,..}=&mut caller {*caller_task_id=Some("task-1".into());}
    let mut extra=selected.clone();if let Selection::Public{arguments,..}=&mut extra {arguments["unexpected"]=json!(true);}
    let unknown=operator_selection(read_page::ReadTool::Task,json!({"task_id":"task-1"}));
    for invalid in [caller,extra,unknown] {assert!(matches!(operator_read(&c,invalid,None).await,Outcome::Refused{..}));}
    let tiny=call(&c,Operation::QueryCollaboration{query:Query::Read(read_page::Request{selection:selected,cursor:None,max_bytes:Some(1)})}).await;
    assert!(matches!(tiny,Outcome::Refused{code,..} if code=="response_bound"));
}
#[tokio::test]
async fn shared_read_pagination_operator_tasks_agents_keep_keysets_and_retirement() {
    Box::pin(operator_directory_journey()).await;
}
async fn operator_directory_journey() {
    let mut cfg=config();Arc::make_mut(&mut cfg.limits).controller.list_page_bytes=64;cfg.limits.validate().unwrap();
    let c=Controller::open_with_config(path(),cfg).unwrap();
    let mut input=native("adapter-a");input.instruction=text();let first=submit(&c,"operator-task-a",input.clone()).await;
    let second=submit(&c,"operator-task-b",native("adapter-b")).await;
    assert!(serde_json::to_vec(&first).unwrap().len()>c.limits().controller.list_page_bytes);
    assert!(matches!(call(&c,Operation::ListTasks{after:None,limit:1}).await,Outcome::Ok{data:Data::Tasks(v)} if v.is_empty()));
    assert!(matches!(call(&c,Operation::ListAgents{after:None,limit:1}).await,Outcome::Ok{data:Data::Agents(v)} if v.is_empty()));
    let args=|cursor:Option<String>,agent:Option<String>|json!({"cursor":cursor,"limit":1,"agent_id":agent,"parent_task_id":null});
    let Data::CollaborationPage(tasks)=operator_data(operator_read(&c,operator_selection(read_page::ReadTool::Tasks,args(None,None)),None).await) else{panic!("bounded task summaries")};
    assert_eq!(tasks.items.len(),1);assert_eq!(tasks.items[0]["id"],json!(first.id));
    assert!(tasks.items[0].get("instruction").is_none());assert!(serde_json::to_vec(&tasks).unwrap().len()<=CAP);
    let Data::CollaborationPage(next)=operator_data(operator_read(&c,operator_selection(read_page::ReadTool::Tasks,args(Some(first.id.clone()),None)),None).await) else{panic!("legacy after identity converted")};
    assert_eq!(next.items[0]["id"],json!(second.id));
    let Data::CollaborationPage(filtered)=operator_data(operator_read(&c,operator_selection(read_page::ReadTool::Tasks,args(None,Some("adapter-b".into()))),None).await) else{panic!("agent filter")};
    assert_eq!(filtered.items.len(),1);assert_eq!(filtered.items[0]["id"],json!(second.id));
    let parent_args=|cursor:Option<String>|json!({"cursor":cursor,"limit":1,"agent_id":null,"parent_task_id":first.id});
    let Data::CollaborationPage(empty)=operator_data(operator_read(&c,operator_selection(read_page::ReadTool::Tasks,parent_args(None)),None).await) else{panic!("filtered page")};
    assert!(empty.items.is_empty());let cursor=empty.next_cursor.unwrap();
    let Data::CollaborationPage(end)=operator_data(operator_read(&c,operator_selection(read_page::ReadTool::Tasks,parent_args(Some(cursor.clone()))),None).await) else{panic!("filtered continuation")};
    assert!(end.items.is_empty());assert!(end.next_cursor.is_none());
    assert!(matches!(operator_read(&c,operator_selection(read_page::ReadTool::Tasks,args(Some(cursor),None)),None).await,Outcome::Refused{..}));
    let agents=|cursor:Option<String>|operator_selection(read_page::ReadTool::Agents,json!({"cursor":cursor,"limit":1}));
    let Data::CollaborationPage(active)=operator_data(operator_read(&c,agents(None),None).await) else{panic!("bounded directory")};
    let selected:AgentSummary=serde_json::from_value(active.items[0].clone()).unwrap();assert_eq!(selected.id,"adapter-a");assert!(!selected.retired);
    assert!(serde_json::to_vec(&selected).unwrap().len()>c.limits().controller.list_page_bytes);
    let retirement=call(&c,Operation::RetireAgent{operation_id:"operator-retire-a".into(),agent_id:selected.id.clone(),expected_revision:selected.definition.revision.clone()}).await;
    assert!(matches!(retirement,Outcome::Ok{data:Data::Agent(ref a)} if a.retired));
    for cursor in [None,Some(selected.id.clone()),active.next_cursor.clone()] {
        let Data::CollaborationPage(page)=operator_data(operator_read(&c,agents(cursor),None).await) else{panic!("retained cursor remains usable")};
        let value:AgentSummary=serde_json::from_value(page.items[0].clone()).unwrap();assert_eq!(value.id,"adapter-b");assert!(!value.retired);
    }
    assert!(matches!(call(&c,Operation::GetAgent{agent_id:selected.id.clone()}).await,Outcome::Ok{data:Data::Agent(a)} if a.retired));
    assert!(matches!(call(&c,Operation::SubmitTask{operation_id:"operator-retired-new-task".into(),input}).await,Outcome::Refused{code,..} if code=="agent_retired"));
    assert_eq!(get(&c,&first.id).await.input,first.input,"read and retirement do not alter accepted work");
}


#[tokio::test]
async fn shared_read_pagination_oversized_accepted_result_survives_restart_exactly() {
    Box::pin(oversized_accepted_result_journey()).await;
}
async fn oversized_accepted_result_journey() {
    let p=path();let cfg=config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let mut input=native("oversized-result");
    input.output_schema=Some(json!({"type":"object","required":["answer"],
        "properties":{"answer":{"type":"string"}},"additionalProperties":false}));
    let output=json!({"answer":"🙂é\\\"\n".repeat(900)});
    let output_bytes=serde_json::to_vec(&output).unwrap().len();
    assert!(output_bytes>CAP,"the accepted output itself exceeds one read page");
    assert!(output_bytes<input.limits.output_bytes,"ordinary admitted output limit suffices");
    let (submitted,attempt,_)=admitted(&c,"oversized-result",input).await;
    // Use the canonical completion sequence from finish_native. This is a
    // controlled worker report fixture, not a provider or seeded Task.result.
    task(report(&c,&attempt,"large-result-started",Observation::Started{thread_id:None,turn_id:None}).await);
    controlled_native_request(&c,&attempt,1,Some(4),Some(2)).await;
    task(report(&c,&attempt,&borg_runtime::native_loop_contract::completion_id(&attempt.id),
        Observation::NativeLoopCompleted{requests:Vec::new(),final_response_index:None,model_requests:1,calls:vec![]}).await);
    task(report(&c,&attempt,"large-result-executor",Observation::ExecutorOutcome{
        success:true,summary:"actual controlled completion".into()}).await);
    task(report(&c,&attempt,"large-result-stopped",Observation::ProcessTreeStopped).await);
    let candidate=Observation::ResultCandidate{checks_passed:true,summary:"accepted oversized answer".into(),
        checkpoint:None,validation:vec![borg_runtime::native_loop_contract::evidence(&attempt.id)],output:Some(output.clone())};
    let completed=task(report(&c,&attempt,"large-result-accepted",candidate.clone()).await);
    assert_eq!(completed.status,TaskStatus::Succeeded);
    assert!(completed.attempt.as_ref().unwrap().stopped);
    let accepted=completed.result.clone().expect("canonical accepted result");
    assert_eq!(accepted.attempt_id,attempt.id);assert_eq!(accepted.output,Some(output));
    let exact=read_page::canonical(&accepted).unwrap();
    assert!(exact.len()>CAP);assert!(exact.len()<=completed.input.limits.result_bytes);
    let selection=task_selection(&completed,TaskPart::Result);
    let first=query(&c,selection.clone(),None).await;
    assert!(!first.complete);assert_eq!(first.offset,0);assert!(first.next_cursor.is_some());
    assert_eq!(first.total_bytes,exact.len());assert_eq!(first.subject,selection);
    assert_eq!(first.encoding,read_page::Encoding::CanonicalJsonUtf8);
    assert_eq!(first.retained_truncation,None);
    assert_eq!(first.identity.source,read_page::Source::Task{task_id:submitted.id.clone(),
        session_id:submitted.session_id.clone(),part:TaskPart::Result,revision:attempt.id.clone()});
    assert_eq!(first.identity.digest,read_page::digest(exact.as_bytes()));
    assert!(serde_json::to_vec(&Data::ReadPage(Box::new(first.clone()))).unwrap().len()<=CAP);
    assert_eq!(get(&c,&completed.id).await,completed);
    let operation=Operation::GetOperation{operation_id:"large-result-accepted".into()};
    let receipt=Box::pin(c.handle(Request{protocol_revision:WIRE_REVISION,
        request_id:"large-result-original-receipt".into(),command:operation.clone()})).await;
    receipt.validate_for(&operation).unwrap();assert!(receipt.commit.is_some());
    assert!(matches!(receipt.outcome,Outcome::Ok{data:Data::Operation(_)}));
    drop(c);controller_owner_released(&p).await;
    let raw_receipt=|sql:&rusqlite::Connection|->(String,String,u64){
        sql.query_row("SELECT request,outcome,controller_revision FROM receipts WHERE id='large-result-accepted'",
            [],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?))).unwrap()
    };
    let sql=rusqlite::Connection::open(&p).unwrap();let original=raw_receipt(&sql);drop(sql);
    let c=Controller::open_with_config(&p,cfg).unwrap();
    // The first fragment/cursor was captured before owner release. Continue it
    // after reopen; the helper checks every offset, identity, envelope bound,
    // positive progress, final total and digest rather than only visible links.
    assert_eq!(query(&c,selection,None).await,first);
    let reconstructed=reconstruct_operator(&c,first).await;
    assert_eq!(reconstructed,exact);
    assert_eq!(serde_json::from_str::<AcceptedResult>(&reconstructed).unwrap(),accepted);
    assert_eq!(get(&c,&completed.id).await,completed);
    let replay=Box::pin(c.handle(Request{protocol_revision:WIRE_REVISION,
        request_id:"large-result-reopened-receipt".into(),command:operation.clone()})).await;
    replay.validate_for(&operation).unwrap();assert_eq!(replay.outcome,receipt.outcome);assert_eq!(replay.commit,receipt.commit);
    assert_eq!(task(report(&c,&attempt,"large-result-accepted",candidate).await),completed);
    assert_eq!(get(&c,&completed.id).await,completed);
    drop(c);controller_owner_released(&p).await;
    let sql=rusqlite::Connection::open(&p).unwrap();assert_eq!(raw_receipt(&sql),original);
}
