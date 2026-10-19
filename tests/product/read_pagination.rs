//! Browser routes and canonical controller observations, including the actual Git helper.
use super::*;
use borg_runtime::{read_page::{self,ReadPage,Selection,TaskPart,DocumentPart},collaboration::{Query,QueryResult,documents::*,workflow::PrincipalRef},repository_view::{Request as RepositoryRequest,SourceSelector}};
fn git(root:&std::path::Path,args:&[&str])->String {
    let output=std::process::Command::new("/usr/bin/git").args(["-c","user.name=Fixture","-c","user.email=fixture@invalid","-c","core.hooksPath=/dev/null"]).args(args).current_dir(root).env_clear().env("PATH","/usr/bin:/bin").env("HOME","/nonexistent").env("GIT_CONFIG_NOSYSTEM","1").env("GIT_CONFIG_GLOBAL","/dev/null").output().unwrap();
    assert!(output.status.success());String::from_utf8(output.stdout).unwrap().trim().into()
}
fn escaped(value:&str)->String {value.replace('&',"&amp;").replace('<',"&lt;").replace('>',"&gt;").replace('"',"&quot;").replace('\'',"&#39;")}
fn assert_page(response:&UiResponse,page:&ReadPage,full:&str) {
    assert_eq!(response.status,200,"{}",response.body);assert!(!page.complete);
    assert!(response.body.contains("Partial content"));assert!(response.body.contains(&escaped(&page.content)));
    assert!(response.body.contains(&page.identity.digest));assert!(response.body.contains(&format!("bytes {}–{} of {}",page.offset,page.offset+page.returned_bytes,page.total_bytes)));
    assert!(response.body.contains("rel=\"next\""));assert!(!response.body.contains(&escaped(full)));
    assert!(!response.body.contains("<textarea"));assert!(!response.body.contains("type=\"hidden\""));
    assert!(!response.body.contains("<script>UNTRUSTED"));
}
async fn selected(c:&Controller,selection:Selection)->ReadPage {
    match call(c,Operation::QueryCollaboration{query:Query::Read(read_page::Request{selection,cursor:None,max_bytes:Some(8192)})}).await {
        Data::Collaboration(QueryResult::Read(d))=>match *d{Data::ReadPage(p)=>*p,x=>panic!("{x:?}")},x=>panic!("{x:?}")
    }
}
#[tokio::test]
async fn shared_read_pagination_browser_task_document_repository_parity() {Box::pin(journey()).await;}
async fn journey() {
    let f=Fixture::new().await;let root=f.path.parent().unwrap();let work=root.join("work");std::fs::create_dir(&work).unwrap();git(&work,&["init","-q"]);
    let full="<script>UNTRUSTED</script> & quoted \"\\🙂 ".repeat(800);
    std::fs::write(work.join("long.txt"),&full).unwrap();git(&work,&["add","long.txt"]);git(&work,&["commit","-qm","exact source"]);let commit=git(&work,&["rev-parse","HEAD"]);
    let bare=root.join("read.git");git(root,&["clone","--bare","--quiet",work.to_str().unwrap(),bare.to_str().unwrap()]);
    // Use a second normal controller with the existing explicit repository root.
    let mut cfg=config();cfg.repository_read.store_root=Some(root.to_owned());let c=Controller::open_with_config(root.join("reads.sqlite"),cfg).unwrap();
    let registration=borg_runtime::repository_catalogue::Registration{repository_id:"browser-page-repo".into(),registration_revision:"r1".into(),display_name:"Page fixture".into(),source_endpoint:"source".into(),checkpoint_endpoint:"source".into(),source_store_key:"read.git".into(),checkpoint_store_key:"read.git".into(),pull_request_target:None};
    call(&c,Operation::RegisterRepository{operation_id:"browser-page-registration".into(),registration:registration.clone()}).await;
    let input=TaskInput {instruction_source:None,pull_request:None,resolved_limits:None,instruction:full.clone(),definition:Definition{charter:None,limit_overrides:None,id:"browser-page-agent".into(),revision:"r1".into(),executor:ExecutorKind::Native,inference:Some(InferenceSettings::selected()),execution_profile:Some(ExecutionProfile::CollaborationV1{web_search:None})},repository_read:Some(RepositoryReadSource{registration:Some(registration.binding()),repository:String::new(),start_commit:commit}),repository:None,acceptance_commands:vec![],output_schema:Some(serde_json::json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false})),limits:c.limits().task_defaults.at(now()).unwrap(),dependencies:vec![],native_workflow:None};
    let Data::Task(task)=call(&c,Operation::SubmitTask{operation_id:"browser-page-task".into(),input: (input).into()}).await else{panic!("task expected")};
    let selected_task=selected(&c,Selection::Task{task_id:task.id.clone(),session_id:None,part:TaskPart::Instruction}).await;
    let task_html=ui(&c,"GET",&format!("/tasks/{}?part=instruction&max_bytes=8192",task.id),vec![]).await;assert_page(&task_html,&selected_task,&full);
    let Data::DocumentMutation(DocumentMutationOutcome::Document{view:doc})=call(&c,Operation::MutateDocument{operation_id:"browser-page-document".into(),mutation:DocumentMutation::Create{content:DocumentContent{title:"Exact large body".into(),body:full.clone()},supplied_content_id:None,responsible_owner:PrincipalRef::TrustedHostOperator}}).await else{panic!("document expected")};
    let selected_doc=selected(&c,Selection::Document{document_id:doc.document.id.clone(),version:Some(1),part:DocumentPart::Body}).await;
    let doc_html=ui(&c,"GET",&format!("/documents/{}?version=1&part=body&max_bytes=8192",doc.document.id),vec![]).await;assert_page(&doc_html,&selected_doc,&full);
    let Data::Collaboration(QueryResult::Repository(output))=call(&c,Operation::QueryCollaboration{query:Query::Repository{task_id:task.id.clone(),request:RepositoryRequest::ReadPage{source:SourceSelector::Start,path:"long.txt".into(),start_line:1,max_lines:1,cursor:None,max_bytes:Some(8192)}}}).await else{panic!("repository output expected")};
    let borg_runtime::repository_view::Data::ReadPage(selected_repo)=output.data else{panic!("repository page expected")};
    let repo_html=ui(&c,"GET",&format!("/tasks/{}/repository?view=read&source=start&path=long.txt&start_line=1&max_lines=1&max_bytes=8192",task.id),vec![]).await;assert_page(&repo_html,&selected_repo,&full);
    assert_eq!(selected_task.identity.digest,selected_doc.identity.digest);assert_eq!(selected_repo.identity.digest,selected_task.identity.digest);
    // Supplying a cursor to the original whole-value mutation is refused. No
    // partial fragment is silently used to replace the complete document.
    let route=format!("/documents/{}/versions",doc.document.id);
    let refusal=ui(&c,"POST",&route,form(&[("operation_id","page-is-not-edit".into()),("expected_revision",doc.document.revision.to_string()),("title","Partial".into()),("body",selected_doc.content.clone()),("cursor",selected_doc.next_cursor.unwrap())])).await;
    assert_eq!(refusal.status,400);
    let Data::DocumentQuery(DocumentQueryResult::Document(unchanged))=call(&c,Operation::QueryDocument{query:DocumentQuery::Get{document_id:doc.document.id.clone(),version:None}}).await else{panic!("document expected")};assert_eq!(unchanged,doc);
    let changed=ui(&c,"POST",&route,form(&[("operation_id","whole-edit".into()),("expected_revision",doc.document.revision.to_string()),("title","Whole new value".into()),("body","Complete replacement".into())])).await;assert_eq!(changed.status,303,"{}",changed.body);
    let whole=ui(&c,"GET",&format!("/documents/{}",doc.document.id),vec![]).await;assert_eq!(whole.status,200);assert!(whole.body.contains("<textarea"));assert!(whole.body.contains("Complete replacement"));
    let prior=ui(&c,"GET",&format!("/documents/{}?version=1&part=body&max_bytes=8192",doc.document.id),vec![]).await;assert!(prior.body.contains(&selected_doc.identity.digest));
    assert_eq!(super::task(&c,&task.id).await.input,task.input,"reads did not mutate accepted input");
}


fn browser_list_selection(tool:read_page::ReadTool,arguments:serde_json::Value)->Selection {
    Selection::Public{tool,arguments,item:false,caller_task_id:None}
}
async fn browser_outcome(c:&Controller,command:Operation)->Outcome {
    Box::pin(c.handle(Request{protocol_revision:WIRE_REVISION,request_id:format!("bounded-browser-{}",NEXT.fetch_add(1,Ordering::Relaxed)),command})).await.outcome
}
async fn browser_list(c:&Controller,selection:Selection,cursor:Option<String>)->Data {
    match call(c,Operation::QueryCollaboration{query:Query::Read(read_page::Request{selection,cursor,max_bytes:None})}).await {
        Data::Collaboration(QueryResult::Read(data))=>*data,x=>panic!("canonical bounded list: {x:?}")
    }
}
fn href(body:&str,label:&str)->Option<String> {
    body.split("<a ").find(|anchor|anchor.contains(&format!(">{label}</a>")))
        .map(|anchor|anchor.split_once("href=\"").unwrap().1.split('"').next().unwrap().replace("&amp;","&"))
}
fn bounded_html(c:&Controller,response:&UiResponse) {
    assert_eq!(response.status,200,"{}",response.body);
    assert!(response.body.len()<=c.limits().product.response_bytes);
}
fn exact_fragment(response:&UiResponse,page:&ReadPage) {
    assert!(response.body.contains(&format!("<pre data-read-content>{}</pre>",escaped(&page.content))));
    assert!(response.body.contains(&format!("data-read-complete=\"{}\"",page.complete)));
    assert!(response.body.contains(&page.identity.digest));
    assert!(response.body.contains(&format!("bytes {}–{} of {}",page.offset,page.offset+page.returned_bytes,page.total_bytes)));
    assert!(!response.body.contains("<textarea"));assert!(!response.body.contains("<script>UNTRUSTED"));
}
async fn browser_document(c:&Controller,id:&str,body:&str)->DocumentView {
    match call(c,Operation::MutateDocument{operation_id:id.into(),mutation:DocumentMutation::Create{
        content:DocumentContent{title:"Browser bounded versions".into(),body:body.into()},supplied_content_id:None,responsible_owner:PrincipalRef::TrustedHostOperator}}).await {
        Data::DocumentMutation(DocumentMutationOutcome::Document{view})=>view,x=>panic!("{x:?}")
    }
}
#[tokio::test]
async fn shared_read_pagination_browser_oversized_version_lists_continue_exact_content() {
    Box::pin(browser_versions_journey()).await;
}
async fn browser_versions_journey() {
    let f=Fixture::new().await;let mut cfg=config();let limits=std::sync::Arc::make_mut(&mut cfg.limits);
    limits.workflow.as_mut().unwrap().page_bytes=8192;limits.controller.collaboration_item_bytes=4096;
    limits.product.page_items=2;limits.validate().unwrap();
    let c=Controller::open_with_config(f.path.parent().unwrap().join("version-lists.sqlite"),cfg).unwrap();
    let full="<script>UNTRUSTED</script> & quoted \"\\🙂 ".repeat(800);
    let first=browser_document(&c,"browser-versions-one",&full).await;
    assert!(serde_json::to_vec(&first.version).unwrap().len()>8192);
    let Data::DocumentMutation(DocumentMutationOutcome::Document{view:second})=call(&c,Operation::MutateDocument{
        operation_id:"browser-versions-two".into(),mutation:DocumentMutation::AppendVersion{document_id:first.document.id.clone(),expected_revision:first.document.revision,
            content:DocumentContent{title:"Small exact successor".into(),body:"second body".into()},supplied_content_id:None}}).await else{panic!("second version")};
    let legacy=browser_outcome(&c,Operation::QueryDocument{query:DocumentQuery::Versions{document_id:first.document.id.clone(),after:None,limit:2}}).await;
    assert!(matches!(legacy,Outcome::Refused{ref code,..} if code=="response_bound"),"{legacy:?}");
    let selection=browser_list_selection(read_page::ReadTool::Versions,serde_json::json!({"document_id":first.document.id,"cursor":null,"limit":2}));
    let canonical=browser_list(&c,selection,None).await;assert!(serde_json::to_vec(&canonical).unwrap().len()<=8192);
    let Data::ReadList(list)=canonical else{panic!("first version must be partial, not omitted")};
    assert_eq!(list.items.len(),1);assert!(!list.complete);
    let read_page::PagedItem::Partial{content_complete,page}=&list.items[0] else{panic!("partial canonical version")};
    assert!(!*content_complete);assert!(serde_json::to_vec(&list.items[0]).unwrap().len()<=4096);
    let route=format!("/documents/{}/versions",first.document.id);
    let first_html=ui(&c,"GET",&route,vec![]).await;bounded_html(&c,&first_html);exact_fragment(&first_html,page);
    let list_link=href(&first_html.body,"Next page").unwrap();
    let content_link=href(&first_html.body,"Next content page").unwrap();
    assert_eq!(list_link,format!("{route}?after={}",encode(list.next_cursor.as_ref().unwrap())));
    assert_eq!(content_link,format!("{route}?cursor={}",encode(page.next_cursor.as_ref().unwrap())));
    assert_ne!(list_link,content_link);assert!(!first_html.body.contains(&escaped(&full)));
    let identity=page.identity.clone();let mut current=page.clone();let mut html=first_html;let mut reconstructed=String::new();let mut pages=0;
    loop {
        exact_fragment(&html,&current);assert_eq!(current.identity,identity);assert_eq!(current.offset,reconstructed.len());
        assert_eq!(current.returned_bytes,current.content.len());assert!(current.returned_bytes>0);
        reconstructed.push_str(&current.content);pages+=1;assert!(pages<500,"bounded admitted document");
        if current.complete {assert!(current.next_cursor.is_none());assert!(href(&html.body,"Next content page").is_none());break;}
        let next_link=href(&html.body,"Next content page").unwrap();
        let Data::ReadPage(next)=browser_list(&c,current.subject.clone(),current.next_cursor.clone()).await else{panic!("canonical continuation")};
        html=ui(&c,"GET",&next_link,vec![]).await;bounded_html(&c,&html);
        assert!(href(&html.body,"Next page").is_none(),"content cursor cannot invent the next list position");
        assert_eq!(href(&html.body,"Return to selected list page"),Some(route.clone()));current=*next;
    }
    assert_eq!(reconstructed.len(),page.total_bytes);assert_eq!(read_page::digest(reconstructed.as_bytes()),identity.digest);
    assert_eq!(serde_json::from_str::<DocumentVersion>(&reconstructed).unwrap(),first.version);
    let next_html=ui(&c,"GET",&list_link,vec![]).await;bounded_html(&c,&next_html);
    assert!(next_html.body.contains("Version 2: Small exact successor"));assert!(!next_html.body.contains("Version 1:"));
    assert!(next_html.body.contains("data-list-complete=\"true\""));assert!(href(&next_html.body,"Next page").is_none());
    assert!(!next_html.body.contains("data-read-content"),"small complete version keeps its normal renderer");
    let other=browser_document(&c,"browser-versions-other","other source").await;
    let foreign=format!("/documents/{}/versions?cursor={}",other.document.id,encode(page.next_cursor.as_ref().unwrap()));
    assert_eq!(ui(&c,"GET",&foreign,vec![]).await.status,400);
    for invalid in [format!("{content_link}&after={}",encode(list.next_cursor.as_ref().unwrap())),format!("{route}?unknown=true")] {
        assert_eq!(ui(&c,"GET",&invalid,vec![]).await.status,400);
    }
    let mut changed=read_page::Cursor::decode(page.next_cursor.as_ref().unwrap(),c.limits().controller.collaboration_cursor_bytes).unwrap();
    changed.identity.digest=read_page::digest(b"different version source");
    let stale=ui(&c,"GET",&format!("{route}?cursor={}",encode(&changed.encode(c.limits().controller.collaboration_cursor_bytes).unwrap())),vec![]).await;
    assert_eq!(stale.status,409);assert!(stale.body.contains("stale_read"));
    let refused=ui(&c,"POST",&route,form(&[("operation_id","partial-not-version".into()),("expected_revision",second.document.revision.to_string()),("title","Not complete".into()),("body",page.content.clone()),("cursor",page.next_cursor.clone().unwrap())])).await;
    assert_eq!(refused.status,400);
    let Data::DocumentQuery(DocumentQueryResult::Document(after))=call(&c,Operation::QueryDocument{query:DocumentQuery::Get{document_id:first.document.id.clone(),version:None}}).await else{panic!("current document")};
    assert_eq!(after,second,"reads and refused partial edit retain the accepted current version");
}
fn directory_input(c:&Controller,id:&str,instruction:&str)->TaskInput {
    TaskInput{instruction_source:None,pull_request:None,resolved_limits:None,instruction:instruction.into(),
        definition:Definition{charter:None,limit_overrides:None,id:id.into(),revision:"r1".into(),executor:ExecutorKind::Native,
            inference:Some(InferenceSettings::selected()),execution_profile:Some(ExecutionProfile::CollaborationV1{web_search:None})},
        repository_read:None,repository:None,acceptance_commands:vec![],
        output_schema:Some(serde_json::json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false})),
        limits:c.limits().task_defaults.at(now()).unwrap(),dependencies:vec![],native_workflow:None}
}
#[tokio::test]
async fn shared_read_pagination_browser_task_agent_false_empty_and_retired_cursor() {
    Box::pin(browser_directory_journey()).await;
}
async fn browser_directory_journey() {
    let f=Fixture::new().await;let mut cfg=config();let limits=std::sync::Arc::make_mut(&mut cfg.limits);
    limits.controller.list_page_bytes=64;limits.product.page_items=1;limits.validate().unwrap();
    let c=Controller::open_with_config(f.path.parent().unwrap().join("directory-lists.sqlite"),cfg).unwrap();
    let input=directory_input(&c,"browser-adapter-a",&"long accepted instruction ".repeat(800));
    let Data::Task(first)=call(&c,Operation::SubmitTask{operation_id:"browser-directory-a".into(),input:(input.clone()).into()}).await else{panic!("first task")};
    let Data::Task(second)=call(&c,Operation::SubmitTask{operation_id:"browser-directory-b".into(),input:(directory_input(&c,"browser-adapter-b","second")).into()}).await else{panic!("second task")};
    assert!(serde_json::to_vec(&first).unwrap().len()>c.limits().controller.list_page_bytes);
    assert!(matches!(call(&c,Operation::ListTasks{after:None,limit:1}).await,Data::Tasks(v) if v.is_empty()));
    assert!(matches!(call(&c,Operation::ListAgents{after:None,limit:1}).await,Data::Agents(v) if v.is_empty()));
    let tasks=ui(&c,"GET","/tasks",vec![]).await;bounded_html(&c,&tasks);
    assert!(tasks.body.contains(&format!("href=\"/tasks/{}\"",first.id)));assert!(!tasks.body.contains(&escaped(&first.input.instruction)));
    let next=ui(&c,"GET",&href(&tasks.body,"Next filtered page").unwrap(),vec![]).await;bounded_html(&c,&next);
    assert!(next.body.contains(&format!("href=\"/tasks/{}\"",second.id)));assert!(href(&next.body,"Next filtered page").is_none());
    let legacy_after=ui(&c,"GET",&format!("/tasks?after={}",first.id),vec![]).await;bounded_html(&c,&legacy_after);
    assert!(legacy_after.body.contains(&format!("href=\"/tasks/{}\"",second.id)));
    let filtered=ui(&c,"GET","/tasks?agent_id=browser-adapter-b",vec![]).await;bounded_html(&c,&filtered);
    assert!(filtered.body.contains(&format!("href=\"/tasks/{}\"",second.id)));
    assert!(!filtered.body.contains(&format!("href=\"/tasks/{}\"",first.id)));assert!(href(&filtered.body,"Next filtered page").is_none());
    let empty=ui(&c,"GET",&format!("/tasks?parent_task_id={}",first.id),vec![]).await;bounded_html(&c,&empty);
    assert!(empty.body.contains("No matching"));assert!(empty.body.contains("data-list-complete=\"false\""));
    let empty_next=href(&empty.body,"Next filtered page").unwrap();
    let exhausted=ui(&c,"GET",&empty_next,vec![]).await;bounded_html(&c,&exhausted);
    assert!(exhausted.body.contains("data-list-complete=\"true\""));assert!(href(&exhausted.body,"Next filtered page").is_none());
    let wrong_filter=empty_next.replace(&format!("parent_task_id={}",first.id),&format!("parent_task_id={}",second.id));
    assert_ne!(wrong_filter,empty_next);assert_eq!(ui(&c,"GET",&wrong_filter,vec![]).await.status,409);
    let agents=ui(&c,"GET","/agents",vec![]).await;bounded_html(&c,&agents);
    assert!(agents.body.contains("href=\"/agents/browser-adapter-a\""));assert!(!agents.body.contains("No agents found"));
    let agent_next=href(&agents.body,"Next agent page").unwrap();assert!(agent_next.contains("page_cursor="));
    let Data::Agent(selected)=call(&c,Operation::GetAgent{agent_id:"browser-adapter-a".into()}).await else{panic!("selected agent")};
    assert!(serde_json::to_vec(&selected).unwrap().len()>c.limits().controller.list_page_bytes);
    let Data::Agent(retired)=call(&c,Operation::RetireAgent{operation_id:"browser-retire-a".into(),agent_id:selected.id.clone(),expected_revision:selected.definition.revision.clone()}).await else{panic!("retired agent")};assert!(retired.retired);
    for route in ["/agents".to_owned(),agent_next.clone(),"/agents?after=browser-adapter-a".into()] {
        let active=ui(&c,"GET",&route,vec![]).await;bounded_html(&c,&active);
        assert!(active.body.contains("href=\"/agents/browser-adapter-b\""));assert!(!active.body.contains("href=\"/agents/browser-adapter-a\""));
        assert!(href(&active.body,"Next agent page").is_none());
    }
    assert_eq!(ui(&c,"GET",&format!("{agent_next}&after=browser-adapter-a"),vec![]).await.status,400);
    let refused=browser_outcome(&c,Operation::SubmitTask{operation_id:"browser-retired-new-task".into(),input: (input).into()}).await;
    assert!(matches!(refused,Outcome::Refused{code,..} if code=="agent_retired"));
    assert_eq!(super::task(&c,&first.id).await.input,first.input);
    // Normal fitting pages keep their existing progress/definition presentation.
    // Reports are controlled fixture observations; no executor or provider runs.
    let created=ui(&f.controller,"POST","/tasks",submission("ordinary-list-task","ordinary small task","")).await;
    assert_eq!(created.status,303,"{}",created.body);let ordinary_id=task_id(&created).to_owned();
    let queued=super::task(&f.controller,&ordinary_id).await;
    let queued_html=ui(&f.controller,"GET","/tasks",vec![]).await;bounded_html(&f.controller,&queued_html);
    assert!(queued_html.body.contains("Progress or blocker"));
    if let Some(reason)=&queued.blocked_reason {assert!(!reason.is_empty());assert!(queued_html.body.contains(&escaped(reason)));}
    let worker=Worker{id:"ordinary-list-worker".into(),incarnation:"incarnation-1".into(),coding_capacity:1,native_capacity:0};
    call(&f.controller,Operation::RegisterWorker{operation_id:"ordinary-list-register".into(),worker:worker.clone()}).await;
    let attempt=next_ui_attempt(&f.controller,&worker,&ordinary_id).await;
    report(&f.controller,&attempt,Observation::Started{thread_id:Some("ordinary-list-thread".into()),turn_id:Some("ordinary-list-turn".into())}).await;
    report(&f.controller,&attempt,Observation::Progress{text:"running <checks> & exact progress".into(),output:String::new(),truncated:false}).await;
    let ordinary=super::task(&f.controller,&ordinary_id).await;
    assert_eq!(ordinary.progress,"running <checks> & exact progress");assert!(ordinary.blocked_reason.is_none());
    assert!(matches!(call(&f.controller,Operation::ListTasks{after:None,limit:1}).await,Data::Tasks(rows) if rows.len()==1 && rows[0]==ordinary));
    let ordinary_html=ui(&f.controller,"GET","/tasks",vec![]).await;bounded_html(&f.controller,&ordinary_html);
    assert!(ordinary_html.body.contains("Progress or blocker"));assert!(ordinary_html.body.contains(&format!("href=\"/tasks/{ordinary_id}\"")));
    assert!(ordinary_html.body.contains(&escaped(&ordinary.progress)));assert!(!ordinary_html.body.contains("running <checks>"));
    let ordinary_agents=ui(&f.controller,"GET","/agents",vec![]).await;bounded_html(&f.controller,&ordinary_agents);
    assert!(ordinary_agents.body.contains("Selected definition for new sessions"));
    assert!(ordinary_agents.body.contains(&format!("{} @ {} (coding)",escaped(&ordinary.input.definition.id),escaped(&ordinary.input.definition.revision))));
}
