//! Child of the real controller retention fixture. These cases use canonical
//! paginated operations; custody reports are controlled worker fixture evidence,
//! not a claim that an executor transcript was copied out in this test.
use super::*;
use borg_runtime::{
    collaboration::{documents::*, workflow::PrincipalRef},
    read_page::{DocumentPart, PagedItem, ReadList, ReadPage, Selection},
    read_retention::{CompactOutcome, ListItemProjection, PageProjection, ReadReference, SourceProjection},
};

fn page_text() -> String {
    "Original immutable page \"\\\n🙂e\u{301} <script>untrusted</script> ".repeat(500)
}
fn projection(page: &ReadPage) -> PageProjection {
    PageProjection {
        subject: page.subject.clone(), identity: page.identity.clone(), summary: page.summary.clone(),
        encoding: page.encoding, offset: page.offset, returned_bytes: page.returned_bytes,
        total_bytes: page.total_bytes, complete: page.complete, next_cursor: page.next_cursor.clone(),
        retained_truncation: page.retained_truncation,
    }
}
fn original_page(reply: &Reply) -> &ReadPage {
    match &reply.outcome { Outcome::Ok {data: Data::ReadPage(page)} => page, other => panic!("expected actual page: {other:?}") }
}
fn original_list(reply: &Reply) -> &ReadList {
    match &reply.outcome { Outcome::Ok {data: Data::ReadList(list)} => list, other => panic!("expected actual partial list: {other:?}") }
}
fn reference(call: &Invocation, reply: &Reply) -> ReadReference {
    ReadReference::new(call, &mcp::result(call, reply.clone()).unwrap()).unwrap()
}
async fn document(f: &RetainedFixture, id: &str, body: &str) -> DocumentView {
    let reply=exchange(&f.c, Operation::MutateDocument {
        operation_id:id.into(), mutation:DocumentMutation::Create {
            content:DocumentContent {title:"Retained page".into(),body:body.into()},
            supplied_content_id:None,responsible_owner:PrincipalRef::TrustedHostOperator,
        },
    }).await;
    match reply.outcome {Outcome::Ok {data:Data::DocumentMutation(DocumentMutationOutcome::Document {view})}=>view,other=>panic!("{other:?}")}
}
async fn append(f: &RetainedFixture, id: &str, current: &DocumentView, body: &str) -> DocumentView {
    let reply=exchange(&f.c, Operation::MutateDocument {
        operation_id:id.into(), mutation:DocumentMutation::AppendVersion {
            document_id:current.document.id.clone(),expected_revision:current.document.revision,
            content:DocumentContent {title:"Later version".into(),body:body.into()},supplied_content_id:None,
        },
    }).await;
    match reply.outcome {Outcome::Ok {data:Data::DocumentMutation(DocumentMutationOutcome::Document {view})}=>view,other=>panic!("{other:?}")}
}
async fn page_call(f:&RetainedFixture,id:&str,tool:&str,args:serde_json::Value)->(Invocation,Reply) {
    let call=invocation(&f.actor,id,tool,args);let reply=invoke(&f.c,&call).await;(call,reply)
}
async fn compact_lookup(c:&Controller,call:&Invocation)->CompactOutcome {
    // The operator occurrence read exposes retained metadata. It does not try
    // another source read and therefore remains compact even for immutable pages.
    let reply=exchange(c,Operation::GetOperation {operation_id:call.operation_id.clone()}).await;
    let Outcome::Ok {data:Data::Operation(record)}=reply.outcome else {panic!("compact original expected")};
    assert_eq!(record.kind,"committed_read_reference");
    assert_eq!(record.state,borg_runtime::read_retention::UNAVAILABLE);
    assert_eq!(record.intended,serde_json::to_value(call.operation()).unwrap());
    let compact:CompactOutcome=serde_json::from_value(record.outcome.unwrap()).unwrap();
    compact.validate(&call.operation()).unwrap();compact
}
fn receipts(path:&std::path::Path)->Vec<(String,String,String,i64)> {
    // Called only after the controller owner has released its exclusive store.
    let sql=rusqlite::Connection::open(path).unwrap();
    let mut q=sql.prepare("SELECT id,request,outcome,controller_revision FROM receipts ORDER BY id").unwrap();
    q.query_map([],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).unwrap()
        .map(Result::unwrap).collect()
}

#[tokio::test]
async fn terminal_read_pages_and_partial_items_keep_exact_compact_provenance_after_reopen() {
    Box::pin(pages_and_items()).await;
}
async fn pages_and_items() {
    let f=retained_fixture("retained-pages").await;
    let doc=document(&f,"retained-page-document",&page_text()).await;
    let _later=append(&f,"retained-page-small-version",&doc,"second version").await;
    let first=page_call(&f,"retained-page-first","collab_document_get",
        json!({"document_id":doc.document.id,"version":1,"part":"body","max_bytes":8192})).await;
    let first_page=original_page(&first.1);assert!(!first_page.complete);assert_eq!(first_page.offset,0);
    let second=page_call(&f,"retained-page-second","collab_read_continue",
        json!({"cursor":first_page.next_cursor.clone().unwrap()})).await;
    let second_page=original_page(&second.1);
    assert_eq!(second_page.identity,first_page.identity);assert_eq!(second_page.offset,first_page.returned_bytes);
    let list=page_call(&f,"retained-version-list","collab_document_versions_list",
        json!({"document_id":doc.document.id,"cursor":null,"limit":2,"max_bytes":8192})).await;
    let list_body=original_list(&list.1);assert_eq!(list_body.items.len(),1);assert!(!list_body.complete);
    let PagedItem::Partial {content_complete,page:item}=&list_body.items[0] else {panic!("first oversized version must remain reachable")};
    assert!(!*content_complete);assert!(!item.complete);assert!(list_body.next_cursor.is_some());
    assert_ne!(list_body.next_cursor,item.next_cursor,"list traversal is not item content continuation");
    let item_next=page_call(&f,"retained-version-item-next","collab_read_continue",
        json!({"cursor":item.next_cursor.clone().unwrap()})).await;
    assert_eq!(original_page(&item_next.1).identity,item.identity);
    assert_eq!(original_page(&item_next.1).offset,item.returned_bytes);
    let calls=vec![first.clone(),second.clone(),list.clone(),item_next.clone()];
    let references:Vec<_>=calls.iter().map(|(call,reply)| {
        let original=reference(call,reply);
        let saved=serde_json::to_vec(&original).unwrap();
        let restored:ReadReference=serde_json::from_slice(&saved).unwrap();
        assert_eq!(restored,original,"typed compact references survive retained serialization");
        restored
    }).collect();
    for index in [0,1,3] {
        assert_eq!(references[index].evidence.source,SourceProjection::Page {page:projection(original_page(&calls[index].1))});
    }
    assert_eq!(references[2].evidence.source,SourceProjection::ReadList {
        items:vec![ListItemProjection::Partial {content_complete:*content_complete,page:projection(item)}],
        complete:list_body.complete,next_cursor:list_body.next_cursor.clone(),
    });
    for ((call,reply),reference) in calls.iter().zip(&references) {
        assert_eq!(reference.restore(call,mcp::result(call,reply.clone()).unwrap()).unwrap(),mcp::result(call,reply.clone()).unwrap());
        assert_eq!(reference.evidence.outcome_bytes,serde_json::to_vec(&reply.outcome).unwrap().len());
        assert_eq!(reference.evidence.outcome_sha256,borg_runtime::read_retention::digest(&serde_json::to_vec(&reply.outcome).unwrap()));
    }
    close_cancelled(&f).await;
    let plan=release_plan(&f,&calls,"retained-pages-custody").await;
    let plan=custody_and_safe(&f,&plan).await;
    let released=release(&f,&plan).await;
    let Outcome::Ok {data:Data::TerminalReadRelease(result)}=&released.outcome else {panic!("{released:?}")};
    assert_eq!(result.released,calls.iter().map(|(call,_)|call.operation_id.clone()).collect::<Vec<_>>());
    assert!(result.body_bytes_after<result.body_bytes_before);
    let terminal=get(&f.c,&f.task.id).await;
    for ((call,reply),reference) in calls.iter().zip(&references) {
        let compact=compact_lookup(&f.c,call).await;
        assert_eq!(compact.committed_read_reference.evidence,reference.evidence);
        assert_eq!(compact.committed_read_reference.release_operation_id,plan.operation_id);
        assert_eq!(compact.committed_read_reference.attempt_id,f.attempt.id);
        assert_eq!(compact.committed_read_reference.session_id,f.task.session_id);
        let replay=invoke(&f.c,call).await;assert_eq!(replay.commit,reply.commit);
        assert!(matches!(replay.outcome,Outcome::Ok {data:Data::OriginalReadBodyUnavailable(_)}));
    }
    // Exact immutable document pages can be reconstructed from their retained
    // version/range. A volatile list or item observation must not read a new list.
    for index in [0,1] {
        let recovered=mcp::recovered(&calls[index].0,lookup(&f.c,&calls[index].0).await).unwrap();
        assert_eq!(references[index].restore(&calls[index].0,recovered).unwrap().reply.outcome,calls[index].1.outcome);
    }
    for index in [2,3] {
        assert_eq!(mcp::recovered(&calls[index].0,lookup(&f.c,&calls[index].0).await).unwrap_err(),borg_runtime::read_retention::UNAVAILABLE);
    }
    let RetainedFixture {c,cfg,path,..}=f;drop(c);controller_owner_released(&path).await;
    let originals=receipts(&path);
    let c=Controller::open_with_config(&path,cfg.clone()).unwrap();assert_eq!(get(&c,&terminal.id).await,terminal);
    for ((call,_),reference) in calls.iter().zip(&references) {
        assert_eq!(compact_lookup(&c,call).await.committed_read_reference.evidence,reference.evidence);
    }
    drop(c);controller_owner_released(&path).await;assert_eq!(receipts(&path),originals);
    let sql=rusqlite::Connection::open(&path).unwrap();sql.pragma_update(None,"user_version",25).unwrap();drop(sql);
    let old_bytes=std::fs::read(&path).unwrap();
    let refused=Controller::open_with_config(&path,cfg);
    assert!(matches!(refused,Err(ref reason) if reason.contains("schema26")),"old reader must refuse the combined page/reference vocabulary");
    assert_eq!(std::fs::read(&path).unwrap(),old_bytes);assert_eq!(receipts(&path),originals);
}

#[tokio::test]
async fn terminal_read_pages_original_version_and_occurrence_never_retarget() {
    Box::pin(original_version_and_occurrence()).await;
}
async fn original_version_and_occurrence() {
    let f=retained_fixture("retained-page-source").await;
    let doc=document(&f,"retained-source-document",&page_text()).await;
    let old=page_call(&f,"retained-source-old","collab_document_get",
        json!({"document_id":doc.document.id,"part":"body","max_bytes":8192})).await;
    let reference=reference(&old.0,&old.1);let old_page=original_page(&old.1).clone();
    let _new=append(&f,"retained-source-change",&doc,&"Different immutable version ".repeat(1000)).await;
    let current=page_call(&f,"retained-source-new-occurrence","collab_document_get",old.0.arguments.clone()).await;
    assert_ne!(original_page(&current.1).identity,old_page.identity);assert_ne!(current.1.commit,old.1.commit);
    assert!(reference.restore(&old.0,mcp::result(&current.0,current.1.clone()).unwrap()).is_err());
    let fixed=page_call(&f,"retained-source-old-continuation","collab_read_continue",
        json!({"cursor":old_page.next_cursor.clone().unwrap()})).await;
    assert_eq!(original_page(&fixed.1).identity,old_page.identity);
    assert_eq!(original_page(&fixed.1).offset,old_page.returned_bytes);
    assert_eq!(invoke(&f.c,&old.0).await.outcome,old.1.outcome);
    // A result with the original commit but altered subject/range/body is not
    // restoration of that original occurrence.
    for change in ["subject","identity","offset","length","encoding","cursor","body"] {
        let mut forged=old.1.clone();let Outcome::Ok {data:Data::ReadPage(page)}=&mut forged.outcome else {unreachable!()};
        match change {
            "subject"=>page.subject=Selection::Document {document_id:"foreign-document".into(),version:Some(1),part:DocumentPart::Body},
            "identity"=>page.identity.digest="0".repeat(64),
            "offset"=>page.offset+=1,
            "length"=>page.total_bytes+=1,
            "encoding"=>page.encoding=borg_runtime::read_page::Encoding::CanonicalJsonUtf8,
            "cursor"=>page.next_cursor=Some("another-range".into()),
            _=>page.content.push('x'),
        }
        assert!(reference.restore(&old.0,mcp::result(&old.0,forged).unwrap()).is_err(),"{change}");
    }
    close_cancelled(&f).await;
    let plan=release_plan(&f,&[old.clone(),current.clone()],"retained-source-custody").await;
    let plan=custody_and_safe(&f,&plan).await;assert!(matches!(release(&f,&plan).await.outcome,Outcome::Ok {..}));
    let recovered=mcp::recovered(&old.0,lookup(&f.c,&old.0).await).unwrap();
    assert_eq!(reference.restore(&old.0,recovered).unwrap().reply.outcome,old.1.outcome,"current document head must not substitute for the original version");
    assert_ne!(compact_lookup(&f.c,&old.0).await.committed_read_reference.evidence,
        compact_lookup(&f.c,&current.0).await.committed_read_reference.evidence);
}

#[tokio::test]
async fn terminal_read_pages_reject_nonread_unknown_and_forged_projection() {
    Box::pin(nonread_unknown_and_forged_projection()).await;
}
async fn nonread_unknown_and_forged_projection() {
    let f=retained_fixture("retained-page-refusals").await;
    let doc=document(&f,"retained-refusal-document",&page_text()).await;
    let original=page_call(&f,"retained-refusal-original","collab_document_get",
        json!({"document_id":doc.document.id,"version":1,"part":"body","max_bytes":8192})).await;
    for tool in ["collab_memory_set","collab_fake_get","collab_document_version_create"] {
        let mut wrong=original.0.clone();wrong.tool=tool.into();
        assert!(ReadEvidence::from_outcome(&wrong,&original.1.outcome).is_err(),"{tool}");
    }
    let opaque=Outcome::Ok {data:Data::Memory(Some(MemoryValue {agent_id:f.task.assigned_agent_id.clone(),key:"ordinary".into(),revision:1,
        value:json!({"kind":"read_page","value":original_page(&original.1)})}))};
    assert!(ReadEvidence::from_outcome(&original.0,&opaque).is_err(),"JSON spelling cannot manufacture a canonical page outcome");
    let mut unknown=serde_json::to_value(reference(&original.0,&original.1)).unwrap();
    unknown["evidence"]["source"]["undeclared_source_override"]=json!("not authority");
    assert!(serde_json::from_value::<ReadReference>(unknown).is_err());
    let mut changed=reference(&original.0,&original.1);
    let SourceProjection::Page {page}=&mut changed.evidence.source else {panic!("typed page projection expected")};
    page.offset+=1;assert!(changed.evidence.verify(&original.0,&original.1.outcome).is_err());
    let mut wrong=original.0.clone();wrong.operation_id="another-original".into();wrong.logical_call_id="another-logical".into();
    assert!(reference(&original.0,&original.1).validate(&wrong).is_err());
    assert_eq!(invoke(&f.c,&original.0).await.outcome,original.1.outcome,"refusal checks leave the actual original unchanged");
}
