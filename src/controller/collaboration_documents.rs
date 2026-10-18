//! Artifact staging: bounded immutable document versions and attributed comments.
//! Root integrates this module into its existing transaction/receipt wrapper.
use super::database::{decode, encode, integer, refuse, valid_id, Fault, Result};
use super::collaboration_workflow::{self as workflow, Context, DomainChange, QueryContext};
use crate::collaboration::documents::*;
use crate::collaboration::workflow::*;
use rusqlite::{params, OptionalExtension};

fn identity(q: &QueryContext<'_, '_>, value: &str) -> Result<()> {
    if value.is_empty() || value.chars().any(char::is_control) { return refuse("invalid_input", "invalid document identity"); }
    if value.len() > q.tx.limits.transport.identity_bytes { return refuse("invalid_input", "Configured limit exceeded: transport.identity_bytes"); }
    Ok(())
}
fn next(value: u64) -> Result<u64> {
    value.checked_add(1).filter(|v| *v <= i64::MAX as u64)
        .ok_or_else(|| Fault::Refused("capacity", "document revision exhausted".into()))
}
fn record<T: serde::Serialize + serde::de::DeserializeOwned>(q: &QueryContext<'_, '_>, raw: &str) -> Result<T> {
    if raw.len() > q.limits.record_bytes { return Err(Fault::Store("stored document record exceeds its bound".into())); }
    decode(raw)
}
fn read_comment(q:&QueryContext<'_, '_>,id:&str,kind:&str,record_id:&str,seq:i64,raw:&str)->Result<RecordedComment> {
    let comment:RecordedComment=record(q,raw)?;
    if comment.id!=id || !valid_id(q.tx.limits,id) || !valid_id(q.tx.limits,&comment.operation_id)
        || record_parts(&comment.record)!=(kind,record_id) || integer(comment.product_revision)?!=seq || seq<=0 {
        return Err(Fault::Store("comment index and body disagree".into()));
    }
    text(&comment.body,q.limits.text_bytes,false).map_err(|_|Fault::Store("invalid stored comment text".into()))?;
    workflow::validate_recorded_actor(q.tx,&comment.author)?;
    let subject=workflow::record_owner_revision(q.tx,&comment.record)?;
    if subject.revision==0 {return Err(Fault::Store("comment lacks its subject revision change".into()))}
    // Validate the requested subject's marker, not its possibly different owner target.
    // This reader is shared by ordinary comment queries and read-only startup scans.
    let changed=workflow::record_changed_product_revision(q.tx,&comment.record)?;
    //= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
    //# Corrupt stored task state MUST be refused without rewriting its data.
    if comment.product_revision>changed {return Err(Fault::Store("comment follows its subject's committed change".into()))}
    Ok(comment)
}
fn bounded<T: serde::Serialize>(ctx: &Context<'_, '_>, value: &T) -> Result<String> {
    let value = encode(value)?;
    if value.len() > ctx.limits.record_bytes { return refuse("capacity", "Configured limit exceeded: workflow.record_bytes"); }
    Ok(value)
}
fn expected(actual: u64, supplied: u64) -> Result<()> {
    //= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
    //# A mutation of an existing collaboration record MUST compare the expected record revision.
    if actual != supplied { return refuse("revision_conflict", "document revision changed"); }
    Ok(())
}
fn text(value: &str, bound: usize, empty: bool) -> Result<()> {
    if (!empty && value.trim().is_empty()) || value.contains('\0') {
        return refuse("invalid_input", "document text is empty or invalid");
    }
    //= spec/impl/v2/implementation.md#limits-are-versioned-metadata
    //# An operation refused by a configured limit MUST identify the limiting metadata field.
    if value.len() > bound { return refuse("invalid_input", "Configured limit exceeded: workflow.text_bytes"); }
    Ok(())
}

pub(super) fn get_document(q: &QueryContext<'_, '_>, id: &str) -> Result<Document> {
    identity(q,id)?;
    let row: Option<(i64,i64,String)> = q.tx.query_row(
        "SELECT revision,created_product_revision,CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM workflow_documents WHERE id=?1",
        params![id, super::database::sql_bound(q.limits.record_bytes.saturating_add(1))?], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    let Some((revision,created,body))=row else {return refuse("not_found","document not found")};
    let doc: Document=record(q,&body)?;
    if doc.id != id || integer(doc.revision)? != revision || integer(doc.created_product_revision)? != created
        || doc.revision == 0 || doc.current.document_id != id || doc.current.version == 0 || doc.current.version>doc.revision
        || doc.changed_product_revision < doc.created_product_revision || doc.created_product_revision == 0 {
        return Err(Fault::Store("document index and canonical record disagree".into()));
    }
    workflow::validate_direct_principal(q.tx,&doc.responsible_owner)?;
    let (last,count):(Option<i64>,i64)=q.tx.query_row("SELECT MAX(version),COUNT(*) FROM workflow_document_versions WHERE document_id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?)))?;
    if last != Some(integer(doc.current.version)?) || count != integer(doc.current.version)? {return Err(Fault::Store("document head differs from retained version history".into()))}
    let head=get_version(q,id,doc.current.version)?;
    if head.reference!=doc.current {return Err(Fault::Store("document head content identity mismatch".into()))}
    Ok(doc)
}

pub(super) fn get_version(q: &QueryContext<'_, '_>, id: &str, version: u64) -> Result<DocumentVersion> {
    identity(q,id)?;
    if version == 0 {return refuse("invalid_input","document version must be positive")}
    let raw: Option<String> = q.tx.query_row(
        "SELECT CAST(substr(CAST(body AS BLOB),1,?3) AS TEXT) FROM workflow_document_versions WHERE document_id=?1 AND version=?2",
        params![id,integer(version)?,super::database::sql_bound(q.limits.record_bytes.saturating_add(1))?],|r|r.get(0)).optional()?;
    let Some(raw)=raw else {return refuse("not_found","document version not found")};
    let saved: DocumentVersion=record(q,&raw)?;
    if saved.reference.document_id != id || saved.reference.version != version || saved.product_revision == 0
        || !valid_id(q.tx.limits,&saved.operation_id) {
        return Err(Fault::Store("document version identity mismatch".into()));
    }
    text(&saved.content.title,q.limits.text_bytes,false).map_err(|_|Fault::Store("invalid stored document title".into()))?;
    text(&saved.content.body,q.limits.text_bytes,true).map_err(|_|Fault::Store("invalid stored document content".into()))?;
    workflow::validate_recorded_actor(q.tx,&saved.author)?;
    let computed=workflow::content_identity(q,&saved.content)?;
    if computed != saved.reference.content_id {return Err(Fault::Store("stored document content identity mismatch".into()))}
    Ok(saved)
}

fn view(q: &QueryContext<'_, '_>, document: Document, version: Option<u64>) -> Result<DocumentView> {
    let version=get_version(q,&document.id,version.unwrap_or(document.current.version))?;
    if version.reference.version == document.current.version && version.reference != document.current {
        return Err(Fault::Store("document current version reference mismatch".into()));
    }
    Ok(DocumentView {document,version})
}

fn make_version(ctx: &Context<'_, '_>, document_id: &str, version: u64, content: &DocumentContent,
    supplied: &Option<String>) -> Result<DocumentVersion> {
    text(&content.title,ctx.limits.text_bytes,false)?;
    text(&content.body,ctx.limits.text_bytes,true)?;
    let content_id=workflow::content_identity(&ctx.query(),content)?;
    if let Some(supplied)=supplied { identity(&ctx.query(),supplied)?; }
    //= spec/impl/v2/implementation.md#document-versions-and-comments-are-immutable
    //# The controller MUST refuse a supplied document content identity that differs from its computed content identity.
    if supplied.as_ref().is_some_and(|value| value != &content_id) {
        return refuse("content_identity_mismatch","supplied document content identity differs from accepted content");
    }
    Ok(DocumentVersion { reference:DocumentVersionRef{document_id:document_id.into(),version,content_id},
        content:content.clone(),author:ctx.actor.clone(),product_revision:ctx.product_revision,
        operation_id:ctx.operation_id.into() })
}

fn insert_version(ctx:&Context<'_, '_>,version:&DocumentVersion)->Result<()> {
    //= spec/impl/v2/implementation.md#document-versions-and-comments-are-immutable
    //# A committed document version MUST be immutable.
    //= spec/impl/v2/implementation.md#document-versions-and-comments-are-immutable
    //# A document version number MUST NOT be reused.
    //= spec/impl/v2/implementation.md#document-versions-and-comments-are-immutable
    //# The initial collaboration profile MUST retain committed document versions.
    ctx.tx.execute("INSERT INTO workflow_document_versions(document_id,version,body) VALUES(?1,?2,?3)",
        params![version.reference.document_id,integer(version.reference.version)?,bounded(ctx,version)?])?;
    Ok(())
}

fn save_document(ctx:&Context<'_, '_>,old:u64,doc:&Document)->Result<()> {
    let count=ctx.tx.execute("UPDATE workflow_documents SET revision=?3,body=?4 WHERE id=?1 AND revision=?2",
        params![doc.id,integer(old)?,integer(doc.revision)?,bounded(ctx,doc)?])?;
    if count != 1 {return refuse("revision_conflict","document revision changed")}
    Ok(())
}

pub(super) fn apply_document(ctx:&Context<'_, '_>,op:&DocumentMutation)->Result<DomainChange<DocumentMutationOutcome>> {
    let q=ctx.query();
    let (outcome,subject,subject_revision,detail)=match op {
        DocumentMutation::Create {content,supplied_content_id,responsible_owner}=>{
            workflow::validate_new_principal(q.tx,responsible_owner)?;
            workflow::ensure_capacity(ctx,1,1)?;
            let id=new_record_id(ctx,"document")?;
            let version=make_version(ctx,&id,1,content,supplied_content_id)?;
            let document=Document {id:id.clone(),revision:1,current:version.reference.clone(),
                responsible_owner:responsible_owner.clone(),created_product_revision:ctx.product_revision,
                changed_product_revision:ctx.product_revision};
            ctx.tx.execute("INSERT INTO workflow_documents(id,revision,created_product_revision,body) VALUES(?1,?2,?3,?4)",
                params![id,1,integer(ctx.product_revision)?,bounded(ctx,&document)?])?;
            insert_version(ctx,&version)?;
            let detail=DocumentChange::Created{version:version.reference.clone(),responsible_owner:responsible_owner.clone(),title:activity_text(ctx,&content.title),body:activity_text(ctx,&content.body)};
            (DocumentMutationOutcome::Document{view:DocumentView{document,version}},RecordRef::Document{document_id:id},1,detail)
        }
        DocumentMutation::AppendVersion {document_id,expected_revision,content,supplied_content_id}=>{
            let mut document=get_document(&q,document_id)?;
            expected(document.revision,*expected_revision)?;
            // Validate existing head before extending: corruption never becomes a new history.
            let _=view(&q,document.clone(),None)?;
            workflow::ensure_capacity(ctx,0,1)?;
            let previous=document.current.clone();
            let version=make_version(ctx,document_id,next(document.current.version)?,content,supplied_content_id)?;
            //= spec/impl/v2/implementation.md#document-versions-and-comments-are-immutable
            //# A document edit MUST create a new version without changing an earlier version.
            insert_version(ctx,&version)?;
            //= spec/impl/v2/implementation.md#reviews-bind-the-exact-subject-version
            //# A new document version MUST NOT inherit the decision of an earlier version's review.
            document.current=version.reference.clone();
            document.revision=next(document.revision)?;
            document.changed_product_revision=ctx.product_revision;
            save_document(ctx,*expected_revision,&document)?;
            let detail=DocumentChange::VersionAdded{previous,version:version.reference.clone(),title:activity_text(ctx,&content.title),body:activity_text(ctx,&content.body)};
            let revision=document.revision;
            (DocumentMutationOutcome::Document{view:DocumentView{document,version}},RecordRef::Document{document_id:document_id.clone()},revision,detail)
        }
        DocumentMutation::SetResponsibleOwner {document_id,expected_revision,responsible_owner}=>{
            let mut document=get_document(&q,document_id)?;
            expected(document.revision,*expected_revision)?;
            workflow::validate_new_principal(q.tx,responsible_owner)?;
            let previous=document.responsible_owner.clone();
            document.responsible_owner=responsible_owner.clone();
            document.revision=next(document.revision)?;
            document.changed_product_revision=ctx.product_revision;
            //= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
            //# A record-owner change MUST preserve its open questions.
            // No question/review target/poser or accepted execution row is edited.
            save_document(ctx,*expected_revision,&document)?;
            let view=view(&q,document,None)?;
            let revision=view.document.revision;
            (DocumentMutationOutcome::Document{view},RecordRef::Document{document_id:document_id.clone()},revision,DocumentChange::OwnerChanged{previous,current:responsible_owner.clone()})
        }
        DocumentMutation::PostComment {record:subject,expected_revision,body}=>{
            text(body,ctx.limits.text_bytes,false)?;
            workflow::ensure_capacity(ctx,0,1)?;
            let id=new_record_id(ctx,"comment")?;
            let comment=RecordedComment{id:id.clone(),record:subject.clone(),body:body.clone(),author:ctx.actor.clone(),
                product_revision:ctx.product_revision,operation_id:ctx.operation_id.into()};
            // Common helper compares/touches each actual subject kind. In particular,
            // this module does not independently rewrite the questions aggregate.
            let record_revision=workflow::stage_record_change(ctx,subject,*expected_revision)?;
            let (record_kind,record_id)=record_parts(subject);
            //= spec/impl/v2/implementation.md#document-versions-and-comments-are-immutable
            //# A posted collaboration comment MUST be immutable.
            ctx.tx.execute("INSERT INTO workflow_comments(id,record_kind,record_id,created_product_revision,body) VALUES(?1,?2,?3,?4,?5)",
                params![id,record_kind,record_id,integer(ctx.product_revision)?,bounded(ctx,&comment)?])?;
            let detail=DocumentChange::CommentAdded{comment_id:comment.id.clone(),body:activity_text(ctx,body)};
            (DocumentMutationOutcome::Comment{comment,record_revision},subject.clone(),record_revision,detail)
        }
    };
    Ok(DomainChange {outcome,effects:vec![WorkflowEffect::Activity{subject,subject_revision,detail:ActivityDetail::Document(detail)}]})
}

pub(super) fn query_document(q:&QueryContext<'_, '_>,query:&DocumentQuery)->Result<DocumentQueryOutcome> {
    match query {
        DocumentQuery::Get{document_id,version}=>Ok(DocumentQueryOutcome::Document(view(q,get_document(q,document_id)?,*version)?)),
        DocumentQuery::List{after,limit}=>{
            let page=query_page(q,"documents","",after,*limit,|start,bound| {
                let mut stmt=q.tx.prepare("SELECT id FROM workflow_documents WHERE id>?1 ORDER BY id LIMIT ?2")?;
                let ids=stmt.query_map(params![start,bound],|r|r.get::<_,String>(0))?.collect::<std::result::Result<Vec<_>,_>>()?;
                ids.into_iter().map(|id|{let doc=get_document(q,&id)?;Ok((id,Some(doc)))}).collect()
            })?;
            Ok(DocumentQueryOutcome::Documents(page))
        }
        DocumentQuery::Versions{document_id,after,limit}=>{
            let _=get_document(q,document_id)?;
            let page=query_page(q,"document_versions",document_id,after,*limit,|start,bound| {
                let start=sequence_cursor(start)?;
                let mut stmt=q.tx.prepare("SELECT version FROM workflow_document_versions WHERE document_id=?1 AND version>?2 ORDER BY version LIMIT ?3")?;
                let versions=stmt.query_map(params![document_id,start,bound],|r|r.get::<_,i64>(0))?.collect::<std::result::Result<Vec<_>,_>>()?;
                versions.into_iter().map(|n|Ok((n.to_string(),Some(get_version(q,document_id,u64::try_from(n).map_err(|_|Fault::Store("negative document version".into()))?)?)))).collect()
            })?;
            Ok(DocumentQueryOutcome::Versions(page))
        }
        DocumentQuery::Comments{record:subject,after,limit}=>{
            let _=workflow::record_owner_revision(q.tx,subject)?;
            let (kind,id)=record_parts(subject);
            let scope=encode(subject)?;
            let page=query_page(q,"comments",&scope,after,*limit,|start,bound| {
                let start=sequence_cursor(start)?;
                let mut stmt=q.tx.prepare("SELECT id,created_product_revision,CAST(substr(CAST(body AS BLOB),1,?5) AS TEXT) FROM workflow_comments WHERE record_kind=?1 AND record_id=?2 AND created_product_revision>?3 ORDER BY created_product_revision LIMIT ?4")?;
                let rows=stmt.query_map(params![kind,id,start,bound,super::database::sql_bound(q.limits.record_bytes.saturating_add(1))?],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?,r.get::<_,String>(2)?)))?.collect::<std::result::Result<Vec<_>,_>>()?;
                rows.into_iter().map(|(comment_id,seq,body)|{
                    let comment=read_comment(q,&comment_id,kind,id,seq,&body)?;
                    Ok((seq.to_string(),Some(comment)))
                }).collect()
            })?;
            Ok(DocumentQueryOutcome::Comments(page))
        }
    }
}



// Traceability scope: Document, comment and review identities use the pending durable product revision.
//= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
//# Every collaboration record MUST have a durable identity.

pub(super) fn new_record_id(ctx:&Context<'_, '_>,kind:&str)->Result<String> {
    // Product revisions are allocated once by the canonical transaction owner.
    // This representation is local to this database; it is not an auth token.
    let id=format!("{kind}-{}",ctx.product_revision);
    if ctx.product_revision == 0 {return refuse("capacity","invalid generated record revision")}
    if id.len()>ctx.tx.limits.transport.identity_bytes {return refuse("capacity","Configured limit exceeded: transport.identity_bytes")}
    Ok(id)
}
pub(super) fn record_parts(record:&RecordRef)->(&'static str,&str) {
    match record {
        RecordRef::Task{task_id}=>("task",task_id),
        RecordRef::Question{question_id}=>("question",question_id),
        RecordRef::Document{document_id}=>("document",document_id),
        RecordRef::Review{review_id}=>("review",review_id),
    }
}
pub(super) fn activity_text(ctx:&Context<'_, '_>,value:&str)->BoundedText {
    // Events carry exact references; text is a visibly truncated projection.
    // Leave room for the second text field and typed identities. Root checks the
    // complete serialized event against event_bytes before any commit.
    let cap=ctx.limits.event_bytes/4;
    let mut end=value.len().min(cap);
    while !value.is_char_boundary(end) {end-=1;}
    BoundedText{text:value[..end].into(),truncated:end<value.len()}
}

#[derive(serde::Serialize,serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {kind:String,scope:String,position:String}
pub(super) fn sequence_cursor(value:&str)->Result<i64> {
    if value.is_empty(){return Ok(0)}
    let n:i64=value.parse().map_err(|_|Fault::Refused("invalid_input","invalid document sequence cursor".into()))?;
    if n<=0 || n.to_string()!=value {return refuse("invalid_input","invalid document sequence cursor")}
    Ok(n)
}
fn cursor(kind:&str,scope:&str,position:&str)->Result<String>{encode(&Cursor{kind:kind.into(),scope:scope.into(),position:position.into()})}
// Same concrete cursor/page discipline as the existing collaboration projections,
// used here by two domain families: document history/comments and review lists.
pub(super) fn query_page<T:serde::Serialize,F>(q:&QueryContext<'_, '_>,kind:&str,scope:&str,after:&Option<String>,limit:usize,read:F)
    ->Result<crate::collaboration::Page<T>> where F:FnOnce(&str,i64)->Result<Vec<(String,Option<T>)>> {
    if limit==0 {return refuse("invalid_input","workflow page limit must be positive")}
    if limit>q.limits.page_items {return refuse("invalid_input","Configured limit exceeded: workflow.page_items")}
    let start=match after {
        None=>String::new(),
        Some(raw)=>{
            if raw.len()>q.tx.limits.controller.collaboration_cursor_bytes {return refuse("invalid_input","Configured limit exceeded: controller.collaboration_cursor_bytes")}
            let c:Cursor=serde_json::from_str(raw).map_err(|_|Fault::Refused("invalid_input","invalid workflow cursor".into()))?;
            if c.kind!=kind || c.scope!=scope || c.position.is_empty() || encode(&c)?!=*raw {return refuse("invalid_input","workflow cursor differs from query scope")}
            c.position
        }
    };
    let bound=limit.checked_add(1).and_then(|n|i64::try_from(n).ok()).ok_or_else(||Fault::Refused("invalid_input","workflow page limit overflow".into()))?;
    let rows=read(&start,bound)?;
    let more=rows.len()>limit;
    let mut page=crate::collaboration::Page{items:vec![],next_cursor:None};
    let mut last=None;
    for (position,item) in rows.into_iter().take(limit) {
        if let Some(item)=item {
            page.items.push(item);
            page.next_cursor=Some(cursor(kind,scope,&position)?);
            if super::bounded_json_size(&page,q.limits.page_bytes).is_err() {
                page.items.pop();
                if page.items.is_empty(){return refuse("response_bound","Configured limit exceeded: workflow.page_bytes")}
                page.next_cursor=last;
                return Ok(page)
            }
        }
        last=Some(cursor(kind,scope,&position)?);
    }
    page.next_cursor=if more {last}else{None};
    if super::bounded_json_size(&page,q.limits.page_bytes).is_err(){return refuse("response_bound","Configured limit exceeded: workflow.page_bytes")}
    Ok(page)
}


/// Root's cross-domain comment helper touches only the mutable document header.
/// No content/version/review/question fields are rewritten.
pub(super) fn touch(ctx:&Context<'_, '_>,document_id:&str,expected_revision:u64)->Result<u64> {
    let mut document=get_document(&ctx.query(),document_id)?;
    expected(document.revision,expected_revision)?;
    document.revision=next(document.revision)?;
    document.changed_product_revision=ctx.product_revision;
    save_document(ctx,expected_revision,&document)?;
    Ok(document.revision)
}

pub(super) fn startup_scan_limit(bound:usize)->Result<i64> {
    bound.checked_add(1).and_then(|n|i64::try_from(n).ok())
        .ok_or_else(||Fault::Store("workflow startup scan bound is not representable".into()))
}

/// Root calls inside existing-store query-only validation, before any schema or
/// application writes. Every retained row is inspected, not just current heads.
/// Root also checks aggregate capacity across all workflow domains.
// Traceability scope: Read-only validation of the selected collaboration records
// associated with the stored task workflow; other state validators remain root-owned.
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//# Corrupt stored task state MUST be refused without rewriting its data.
pub(super) fn validate_stored(q:&QueryContext<'_, '_>)->Result<()> {
    let committed=workflow::product_revision(q.tx)?;
    let id_bytes=q.tx.limits.transport.identity_bytes.saturating_add(1);
    let record_bytes=q.limits.record_bytes.saturating_add(1);
    // get_document checks each head's complete version range. Bound the whole
    // version table first, before those indexed parent/history checks can scan it.
    let versions_seen:i64=q.tx.query_row("SELECT COUNT(*) FROM (SELECT 1 FROM workflow_document_versions LIMIT ?1)",
        [startup_scan_limit(q.limits.revisions)?],|row|row.get(0))?;
    if usize::try_from(versions_seen).ok().is_none_or(|n|n>q.limits.revisions) {
        return Err(Fault::Store("stored document version count exceeds workflow.revisions".into()));
    }
    let mut heads=q.tx.prepare("SELECT CAST(substr(CAST(id AS BLOB),1,?1) AS TEXT) FROM workflow_documents ORDER BY id LIMIT ?2")?;
    let mut rows=heads.query(params![super::database::sql_bound(id_bytes)?,startup_scan_limit(q.limits.records)?])?;
    let mut count=0usize;
    while let Some(row)=rows.next()? {
        count+=1;if count>q.limits.records{return Err(Fault::Store("stored document count exceeds workflow.records".into()))}
        let id:String=row.get(0)?;let doc=get_document(q,&id)?;
        if doc.changed_product_revision>committed {return Err(Fault::Store("document revision exceeds committed product history".into()))}
    }
    let mut versions=q.tx.prepare("SELECT CAST(substr(CAST(document_id AS BLOB),1,?1) AS TEXT),version FROM workflow_document_versions ORDER BY document_id,version LIMIT ?2")?;
    let mut rows=versions.query(params![super::database::sql_bound(id_bytes)?,startup_scan_limit(q.limits.revisions)?])?;
    let mut count=0usize;let mut previous:Option<(String,u64,u64)>=None;let mut current_document:Option<Document>=None;
    while let Some(row)=rows.next()? {
        count+=1;if count>q.limits.revisions{return Err(Fault::Store("stored document version count exceeds workflow.revisions".into()))}
        let id:String=row.get(0)?;let ordinal:i64=row.get(1)?;
        let ordinal=u64::try_from(ordinal).map_err(|_|Fault::Store("negative retained document version".into()))?;
        // Ordered traversal retains one bounded head, avoiding a complete range
        // count for every historical version of the same document.
        if current_document.as_ref().is_none_or(|doc|doc.id!=id) {current_document=Some(get_document(q,&id)?);}
        let doc=current_document.as_ref().expect("document head initialized above");let version=get_version(q,&id,ordinal)?;
        if !valid_id(q.tx.limits,&version.reference.content_id) || version.product_revision>committed
            || version.product_revision<doc.created_product_revision || version.product_revision>doc.changed_product_revision {
            return Err(Fault::Store("retained document version association mismatch".into()));
        }
        match &previous {
            Some((prior_id,prior_version,prior_product)) if prior_id==&id=>{
                if prior_version.checked_add(1)!=Some(ordinal) || version.product_revision<=*prior_product {
                    return Err(Fault::Store("retained document versions are not append-only".into()));
                }
            }
            _=>if ordinal!=1 || version.product_revision!=doc.created_product_revision {
                return Err(Fault::Store("retained initial document version mismatch".into()));
            },
        }
        previous=Some((id,ordinal,version.product_revision));
    }
    let mut comments=q.tx.prepare("SELECT CAST(substr(CAST(id AS BLOB),1,?1) AS TEXT),CAST(substr(CAST(record_kind AS BLOB),1,?1) AS TEXT),CAST(substr(CAST(record_id AS BLOB),1,?1) AS TEXT),created_product_revision,CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM workflow_comments ORDER BY id LIMIT ?3")?;
    let mut rows=comments.query(params![super::database::sql_bound(id_bytes)?,super::database::sql_bound(record_bytes)?,startup_scan_limit(q.limits.revisions)?])?;
    let mut count=0usize;
    while let Some(row)=rows.next()? {
        count+=1;if count>q.limits.revisions{return Err(Fault::Store("stored comment count exceeds workflow.revisions".into()))}
        let id:String=row.get(0)?;let kind:String=row.get(1)?;let record_id:String=row.get(2)?;
        let seq:i64=row.get(3)?;let raw:String=row.get(4)?;
        let comment=read_comment(q,&id,&kind,&record_id,seq,&raw)?;
        if comment.product_revision>committed {return Err(Fault::Store("comment exceeds committed product history".into()))}
    }
    Ok(())
}
