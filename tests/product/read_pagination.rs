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
    let input=TaskInput {instruction_source:None,pull_request:None,resolved_limits:None,instruction:full.clone(),definition:Definition{charter:None,limit_overrides:None,id:"browser-page-agent".into(),revision:"r1".into(),executor:ExecutorKind::Native,inference:Some(InferenceSettings::selected()),execution_profile:Some(ExecutionProfile::CollaborationV1{web_search:None})},repository_read:Some(RepositoryReadSource{registration:Some(registration.binding()),repository:String::new(),start_commit:commit}),repository:None,acceptance_commands:vec![],output_schema:Some(json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false})),limits:c.limits().task_defaults.at(now()).unwrap(),dependencies:vec![],native_workflow:None};
    let Data::Task(task)=call(&c,Operation::SubmitTask{operation_id:"browser-page-task".into(),input}).await else{panic!("task expected")};
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
