//! Charter records inside the existing controller transaction and operation journal.
//! No source fetching, internal COMMIT, idle wake or agent tool routing.
use super::{bounded_json_size,database::*,metadata};
use crate::{charter::*,limits::CharterLimits,mcp::Caller,protocol::*};
use rusqlite::{params,OptionalExtension};
use serde::{Deserialize,Serialize,de::DeserializeOwned};
use std::collections::BTreeSet;
const TABLES:&[&str]=&["charter_sessions","charter_session_versions","charter_requests","charter_replies","charter_closures","charter_compaction_results"];
pub(crate) struct Context<'a,'db>{pub tx:&'a StoreContext<'db>,pub operation_id:&'a str,pub now_ms:u64,pub limits:&'a CharterLimits}
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq)]
#[serde(tag="kind",content="value",rename_all="snake_case",deny_unknown_fields)]
enum RequestRecord{Turn(CharterBoundary),Compaction(CharterCompaction),SuppliedContext(CharterSuppliedContext),SuppliedTurn(CharterSuppliedBoundary)}
impl RequestRecord{
    fn session(&self)->&str{match self{Self::Turn(v)=>&v.session_id,Self::Compaction(v)=>&v.session_id,Self::SuppliedContext(v)=>&v.session_id,Self::SuppliedTurn(v)=>&v.session_id}}
    fn caller(&self)->&Caller{match self{Self::Turn(v)=>&v.caller,Self::Compaction(v)=>&v.caller,Self::SuppliedContext(v)=>&v.caller,Self::SuppliedTurn(v)=>&v.caller}}
    fn index(&self)->u32{match self{Self::Turn(v)=>v.request_index,Self::Compaction(v)=>v.request_index,Self::SuppliedContext(_)=>0,Self::SuppliedTurn(v)=>v.outer_index}}
    fn id(&self)->&str{match self{Self::Turn(v)=>&v.turn_id,Self::Compaction(v)=>&v.run_id,Self::SuppliedContext(v)=>&v.delivery_id,Self::SuppliedTurn(v)=>&v.turn_id}}
    fn source(&self)->&ResolvedCharter{match self{Self::Turn(v)=>&v.effective,Self::Compaction(v)=>&v.effective,Self::SuppliedContext(v)=>&v.effective,Self::SuppliedTurn(v)=>&v.effective}}
    fn kind(&self)->&str{match self{Self::Turn(_)=>"turn",Self::Compaction(_)=>"compaction",Self::SuppliedContext(_)=>"supplied_context",Self::SuppliedTurn(_)=>"supplied_turn"}}
}
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq)]
#[serde(deny_unknown_fields)]
struct Closure{operation_id:String,caller:Caller,results:Vec<CharterResultReceipt>}
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq)]
#[serde(deny_unknown_fields)]
struct CompactionResult{summary:Option<String>,disposition:CompactionDisposition}
/// A rejection is a separate canonical event, never an empty model response.
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq)]
#[serde(deny_unknown_fields)]
struct CompactionRejection{operation_id:String,report_id:String}
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq)]
#[serde(untagged)]
enum CompactionResultRecord{Summary(CompactionResult),Rejected(CompactionRejection)}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub(crate) enum RejectedRequestKind{Turn,Compaction}

fn policy<'a>(tx:&'a StoreContext<'_>)->Result<&'a CharterLimits>{tx.limits.charter.as_ref().ok_or_else(||Fault::Refused("not_configured","charter policy is not configured".into()))}
fn identity(tx:&StoreContext<'_>,id:&str)->Result<()>{if valid_id(tx.limits,id){Ok(())}else{refuse("invalid_input","invalid charter identity")}}
fn corrupt(message:&str)->Fault{Fault::Store(message.into())}
fn bounded<T:Serialize>(value:&T,max:usize)->Result<()>{bounded_json_size(value,max).map(|_|()).map_err(|_|Fault::Refused("input_bound","charter value exceeds configured encoded bound".into()))}
fn stored<T:DeserializeOwned+Serialize>(raw:&str,max:usize)->Result<T>{if raw.len()>max{return Err(corrupt("stored charter value exceeds bound"));}decode(raw)}
/// Count at most the configured allowance plus one, including corrupted stores.
/// Table names come only from the closed internal inventory, never input SQL.
fn bounded_count(tx:&StoreContext<'_>,table:&str,cap:usize)->Result<usize>{
    if !TABLES.contains(&table)&&table!="charter_output_chunks"&&table!="sessions"&&table!="receipts"{return Err(corrupt("unknown charter inventory"));}
    let limit=cap.checked_add(1).and_then(|n|i64::try_from(n).ok()).unwrap_or(i64::MAX);
    let n:i64=tx.query_row(&format!("SELECT count(*) FROM (SELECT 1 FROM {table} LIMIT ?1)"),[limit],|r|r.get(0))?;
    let n=usize::try_from(unsigned(n)?).map_err(|_|corrupt("charter count overflow"))?;
    // Preserve the representable maximum without overflowing LIMIT. This extra
    // existence probe is reachable only at that exact actual row count.
    if n==cap&&cap==i64::MAX as usize{
        let more:bool=tx.query_row(&format!("SELECT EXISTS(SELECT 1 FROM {table} LIMIT 1 OFFSET ?1)"),[i64::MAX],|r|r.get(0))?;
        if more{return Err(corrupt("charter inventory exceeds representable count"));}
    }Ok(n)
}
fn inventory_fits(tx:&StoreContext<'_>,cap:usize)->Result<bool>{
    let mut remaining=cap;
    for table in TABLES{let n=bounded_count(tx,table,remaining)?;if n>remaining{return Ok(false)}remaining-=n;}
    let chunks:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='charter_output_chunks')",[],|r|r.get(0))?;
    if chunks&&bounded_count(tx,"charter_output_chunks",remaining)?>remaining{return Ok(false)}
    Ok(true)
}
fn capacity(tx:&StoreContext<'_>,l:&CharterLimits,additional:usize)->Result<()>{
    let Some(remaining)=l.records.checked_sub(additional)else{return refuse("capacity","charter.records exhausted; retained evidence is not erased")};
    if !inventory_fits(tx,remaining)?{return refuse("capacity","charter.records exhausted; retained evidence is not erased");}Ok(())
}
fn raw_body(tx:&StoreContext<'_>,sql:&str,id:&str,max:usize)->Result<Option<String>>{tx.query_row(sql,params![id,super::database::sql_bound(max.saturating_add(1))?],|r|r.get(0)).optional().map_err(Into::into)}
pub(super) fn definition(tx:&StoreContext<'_>,reference:&CharterSourceRef)->Result<Definition>{
    identity(tx,&reference.definition_id)?;identity(tx,&reference.definition_revision)?;
    let raw:Option<String>=tx.query_row("SELECT CAST(substr(CAST(body AS BLOB),1,?3) AS TEXT) FROM definitions WHERE id=?1 AND revision=?2",params![reference.definition_id,reference.definition_revision,super::database::sql_bound(tx.limits.transport.body_bytes.saturating_add(1))?],|r|r.get(0)).optional()?;
    let d:Definition=stored(&raw.ok_or_else(||Fault::Refused("not_found","charter definition revision absent".into()))?,tx.limits.transport.body_bytes)?;
    if d.id!=reference.definition_id||d.revision!=reference.definition_revision{return Err(corrupt("charter definition/index differs"));}Ok(d)
}
/// Exact authored Markdown paragraphs, not an NLP/behavioral compliance judge.
/// Stable heading text and source line wrapping have one deterministic projection.
// Validate the exact file, stable heading and normative paragraph tuple
// against retained authoritative source bytes; this is structural validation.
//= spec/impl/v2/implementation.md#every-turn-reply-carries-charter-references
//# A charter clause reference MUST resolve to the clause's source file, section, and exact sentence within the identified charter revision.
fn validate_source(tx:&StoreContext<'_>,source:&CharterSource,l:&CharterLimits)->Result<()>{
    bounded(source,l.source_bytes)?;
    if source.files.is_empty()||source.files.len()>l.files||source.clauses.is_empty()||source.clauses.len()>l.clauses{return refuse("input_bound","charter file/clause count");}
    let mut paths=BTreeSet::new();let mut expected=BTreeSet::new();
    for file in &source.files{
        identity(tx,&file.file)?;
        if !paths.insert(&file.file)||file.text.contains('\0')||file.text.contains('\r'){return refuse("invalid_input","duplicate file or unsupported source encoding");}
        let mut heading=String::new();let mut headings=BTreeSet::new();let mut paragraph=Vec::new();
        let mut flush=|paragraph:&mut Vec<String>,heading:&str|->Result<()>{
            if paragraph.is_empty(){return Ok(())}let sentence=paragraph.join(" ");paragraph.clear();
            let normative=sentence.split(|c:char|!c.is_ascii_alphabetic()).any(|s|matches!(s,"MUST"|"SHOULD"|"MAY"|"REQUIRED"|"RECOMMENDED"|"OPTIONAL"));
            if normative{
                if heading.is_empty(){return refuse("invalid_input","charter clause lacks stable heading");}
                if !expected.insert(CharterClauseRef{file:file.file.clone(),section:heading.into(),sentence}){return refuse("invalid_input","ambiguous duplicate charter clause");}
            }Ok(())
        };
        for line in file.text.lines().chain(std::iter::once("")){
            if line.starts_with("```")||line.starts_with("~~~"){return refuse("invalid_input","charter authoring profile excludes fenced code");}
            if line.starts_with('#'){
                flush(&mut paragraph,&heading)?;let n=line.bytes().take_while(|b|*b==b'#').count();
                if n>6||line.as_bytes().get(n)!=Some(&b' '){return refuse("invalid_input","unsupported charter heading");}
                heading=line[n+1..].trim().into();identity(tx,&heading)?;
                if !headings.insert(heading.clone()){return refuse("invalid_input","duplicate charter section");}
            }else if line.trim().is_empty(){flush(&mut paragraph,&heading)?;}else{paragraph.push(line.trim().into());}
        }
    }
    let declared:BTreeSet<_>=source.clauses.iter().cloned().collect();
    if declared.len()!=source.clauses.len()||declared!=expected{return refuse("invalid_input","clause index differs from exact normative source paragraphs");}Ok(())
}
//= spec/impl/v2/implementation.md#a-revision-identifies-authoritative-source
//# A charter revision reference MUST reuse an existing source identity when that identity resolves the complete authoritative charter source.
// Resolve complete retained immutable definition bytes through the bounded
// source-reference chain; no separate source service or access grant is added.
//= spec/impl/v2/implementation.md#a-revision-identifies-authoritative-source
//# A charter revision reference MUST resolve to the exact authoritative source of that revision under the applicable access and retention rules.
// Authoritative source is resolved from immutable definitions; rendered text
// and separately stored compaction summaries do not become source records.
//= spec/impl/v2/implementation.md#a-revision-identifies-authoritative-source
//# A rendered or compacted representation of a charter MUST NOT replace that charter's authoritative source.
// Selected charter-enabled definitions identify Inline source or a retained
// Reference; historical accepted no-charter definitions are not rewritten.
//= spec/impl/v2/implementation.md#definitions-carry-reusable-charters
//# An agent definition MUST identify its reusable charter source.
pub(crate) fn resolve(tx:&StoreContext<'_>,requested:&CharterSourceRef,l:&CharterLimits)->Result<ResolvedCharter>{
    let mut reference=requested.clone();let mut seen=BTreeSet::new();
    for _ in 0..l.reference_depth{
        if !seen.insert(reference.clone()){return refuse("invalid_input","charter source reference cycle");}
        match definition(tx,&reference)?.charter{
            Some(DefinitionCharter::Inline{source})=>{validate_source(tx,&source,l)?;return Ok(ResolvedCharter{reference,source})},
            Some(DefinitionCharter::Reference{source})=>reference=source,
            None=>return refuse("not_configured","definition has no charter source"),
        }
    }refuse("input_bound","charter.reference_depth exceeded")
}
fn bounded_session(tx:&StoreContext<'_>,id:&str)->Result<Session>{
    identity(tx,id)?;
    let row:Option<(i64,String)>=tx.query_row("SELECT max(length(CAST(agent AS BLOB)),length(CAST(task_id AS BLOB)),length(CAST(body AS BLOB))),CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM sessions WHERE id=?1",params![id,super::database::sql_bound(tx.limits.transport.body_bytes.saturating_add(1))?],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let (size,raw)=row.ok_or_else(||Fault::Refused("not_found","charter session absent".into()))?;
    if unsigned(size)?>tx.limits.transport.body_bytes as u64{return Err(corrupt("charter session/index exceeds bound"));}
    let value:Session=stored(&raw,tx.limits.transport.body_bytes)?;
    definition(tx,&CharterSourceRef{definition_id:value.definition.id,definition_revision:value.definition.revision})?;
    super::sessions::get(tx,id)
}
fn initial(tx:&StoreContext<'_>,id:&str,l:&CharterLimits)->Result<Option<ResolvedCharter>>{
    let session=bounded_session(tx,id)?;
    if session.definition.charter.is_none(){Ok(None)}else{resolve(tx,&CharterSourceRef{definition_id:session.definition.id,definition_revision:session.definition.revision},l).map(Some)}
}
// Native scope: BeginTurn saves the effective revision in immutable session versions.
// This annotation does not establish delivery for later supplied outer turns.
//= spec/impl/v2/implementation.md#definitions-carry-reusable-charters
//# A session MUST retain the charter revision effective for each native turn or delivered for each supplied outer turn.
fn save_session(tx:&StoreContext<'_>,s:&CharterSession,l:&CharterLimits,old:Option<u64>,operation_id:Option<&str>)->Result<()>{
    bounded(s,l.record_bytes)?;capacity(tx,l,if old.is_some(){1}else{2})?;let body=encode(s)?;
    tx.execute("INSERT INTO charter_session_versions(id,revision,body,operation_id) VALUES(?1,?2,?3,?4)",params![s.session_id,integer(s.revision)?,body,operation_id])?;
    if let Some(old)=old{if tx.execute("UPDATE charter_sessions SET revision=?2,body=?3 WHERE id=?1 AND revision=?4",params![s.session_id,integer(s.revision)?,body,integer(old)?])?!=1{return refuse("stale_revision","charter binding changed");}}
    else{tx.execute("INSERT INTO charter_sessions(id,revision,body) VALUES(?1,?2,?3)",params![s.session_id,integer(s.revision)?,body])?;}Ok(())
}
fn advance(s:&mut CharterSession)->Result<u64>{let old=s.revision;s.revision=old.checked_add(1).filter(|x|*x<=i64::MAX as u64).ok_or_else(||corrupt("charter revision exhausted"))?;Ok(old)}
/// Admission invokes this after the immutable session/source insertion in the
/// same transaction. Existing no-charter sessions remain historical, not backfilled.
pub(crate) fn initialize(tx:&StoreContext<'_>,session_id:&str)->Result<Option<EffectiveCharter>>{
    if bounded_session(tx,session_id)?.definition.charter.is_none(){return Ok(None)}
    let l=policy(tx)?;let charter=initial(tx,session_id,l)?.ok_or_else(||corrupt("configured charter missing"))?;
    let s=CharterSession{session_id:session_id.into(),revision:1,initial:charter.reference.clone(),effective:charter.reference.clone(),pending:None,last_turn_id:None,compaction_due:None,compaction_in_flight:None};
    save_session(tx,&s,l,None,None)?;Ok(Some(EffectiveCharter{session:s,charter}))
}
fn session(tx:&StoreContext<'_>,id:&str,l:&CharterLimits)->Result<Option<CharterSession>>{
    identity(tx,id)?;let raw=raw_body(tx,"SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM charter_sessions WHERE id=?1",id,l.record_bytes)?;
    let Some(raw)=raw else{if initial(tx,id,l)?.is_some(){return Err(corrupt("configured charter session missing"));}return Ok(None)};
    let s:CharterSession=stored(&raw,l.record_bytes)?;
    let (revision,last):(i64,i64)=tx.query_row("SELECT revision,(SELECT max(revision) FROM charter_session_versions WHERE id=?1) FROM charter_sessions WHERE id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?)))?;
    let historical:String=tx.query_row("SELECT CAST(substr(CAST(body AS BLOB),1,?3) AS TEXT) FROM charter_session_versions WHERE id=?1 AND revision=?2",params![id,revision,super::database::sql_bound(l.record_bytes.saturating_add(1))?],|r|r.get(0))?;
    if s.session_id!=id||s.revision==0||s.revision!=unsigned(revision)?||revision!=last||historical!=raw{return Err(corrupt("charter session head/version mismatch"));}
    if initial(tx,id,l)?.map(|r|r.reference)!=Some(s.initial.clone()){return Err(corrupt("charter initial source differs from accepted definition"));}
    for r in [&s.initial,&s.effective].into_iter().chain(s.pending.iter()).chain(s.compaction_due.iter()){if resolve(tx,r,l)?.reference!=*r{return Err(corrupt("charter source is not complete authoritative revision"));}}
    if let Some(id)=&s.last_turn_id{let r=request(tx,id,l)?;if !matches!(&r,RequestRecord::Turn(v)if v.binding_revision<=s.revision)||r.session()!=s.session_id{return Err(corrupt("charter last-turn binding differs"));}}
    if let Some(id)=&s.compaction_in_flight{let r=request(tx,id,l)?;if !matches!(&r,RequestRecord::Compaction(_))||r.session()!=s.session_id{return Err(corrupt("charter in-flight compaction differs"));}}
    Ok(Some(s))
}
fn require_session(tx:&StoreContext<'_>,id:&str,l:&CharterLimits)->Result<CharterSession>{session(tx,id,l)?.ok_or_else(||Fault::Refused("not_configured","historical session has no accepted charter".into()))}
fn bounded_task(tx:&StoreContext<'_>,id:&str)->Result<Task>{
    identity(tx,id)?;
    let size:Option<i64>=tx.query_row("SELECT max(length(CAST(body AS BLOB)),length(CAST(session AS BLOB)),length(CAST(agent AS BLOB)),length(CAST(executor AS BLOB)),length(CAST(status AS BLOB)),coalesce(length(CAST(worker AS BLOB)),0),coalesce(length(CAST(incarnation AS BLOB)),0)) FROM tasks WHERE id=?1",[id],|r|r.get(0)).optional()?;
    if unsigned(size.ok_or_else(||Fault::Refused("not_found","charter task absent".into()))?)?>tx.limits.transport.body_bytes as u64{return Err(corrupt("charter task record exceeds bound"));}
    Ok(super::database::load(tx,id)?.task)
}
fn current(ctx:&Context<'_,'_>,caller:&Caller,session_id:&str)->Result<Task>{
    bounded_task(ctx.tx,&caller.task_id)?;let t=super::collaboration_calls::caller(ctx.tx,caller,ctx.now_ms)?.task;
    if t.session_id!=session_id{return refuse("caller_mismatch","charter caller belongs to another session");}
    if t.input.definition.executor!=ExecutorKind::Native{return refuse("unsupported_charter_boundary","pinned Codex does not prove every inner model boundary");}Ok(t)
}
fn historical(tx:&StoreContext<'_>,caller:&Caller,session_id:&str)->Result<Task>{
    caller.validate(&tx.limits.transport).map_err(|_|corrupt("invalid retained charter caller"))?;
    bounded_task(tx,&caller.task_id)?;let mut t=super::sessions::historical_task(tx,&caller.task_id,session_id)?;let raw=raw_body(tx,"SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM attempts WHERE id=?1",&caller.attempt_id,tx.limits.transport.body_bytes)?.ok_or_else(||corrupt("charter attempt absent"))?;
    let a:Attempt=stored(&raw,tx.limits.transport.body_bytes)?;
    if a.task_id!=caller.task_id||a.id!=caller.attempt_id||a.generation!=caller.generation||a.worker_id!=caller.worker_id||a.worker_incarnation!=caller.incarnation||a.session_id!=session_id||t.session_id!=session_id{return Err(corrupt("retained charter occurrence binding differs"));}
    bounded_session(tx,session_id)?.validate_for(&a,&t.input).map_err(|_|corrupt("charter retained session differs"))?;t.attempt=Some(a);Ok(t)
}
fn request(tx:&StoreContext<'_>,id:&str,l:&CharterLimits)->Result<RequestRecord>{
    identity(tx,id)?;let raw=raw_body(tx,"SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM charter_requests WHERE id=?1",id,l.record_bytes)?.ok_or_else(||Fault::Refused("not_found","charter request absent".into()))?;let r:RequestRecord=stored(&raw,l.record_bytes)?;
    let (session,attempt,index,kind):(String,String,i64,String)=tx.query_row("SELECT CAST(substr(CAST(session_id AS BLOB),1,?2) AS TEXT),CAST(substr(CAST(attempt_id AS BLOB),1,?2) AS TEXT),request_index,CAST(substr(CAST(kind AS BLOB),1,?2) AS TEXT) FROM charter_requests WHERE id=?1",params![id,super::database::sql_bound(tx.limits.transport.identity_bytes.saturating_add(1))?],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
    if r.id()!=id||(r.index()==0)!=matches!(&r,RequestRecord::SuppliedContext(_))||session!=r.session()||attempt!=r.caller().attempt_id||unsigned(index)?!=r.index()as u64||kind!=r.kind(){return Err(corrupt("charter request/index mismatch"));}
    historical(tx,r.caller(),r.session())?;if resolve(tx,&r.source().reference,l)?!=*r.source(){return Err(corrupt("charter original source changed"));}
    if let RequestRecord::SuppliedContext(c)=&r{supplied::validate_intent(tx,c,l)?;}
    if let RequestRecord::SuppliedTurn(c)=&r{supplied_turns::validate_intent(tx,c,l)?;}
    if let RequestRecord::Compaction(c)=&r{if c.reply.is_some()||c.summary.is_some()||c.disposition.is_some(){return Err(corrupt("mutable result in original compaction request"));}}Ok(r)
}
fn insert_request(ctx:&Context<'_,'_>,r:&RequestRecord)->Result<()>{
    identity(ctx.tx,ctx.operation_id)?;bounded(r,ctx.limits.record_bytes)?;capacity(ctx.tx,ctx.limits,1)?;
    let exists:bool=ctx.tx.query_row("SELECT EXISTS(SELECT 1 FROM charter_requests WHERE attempt_id=?1 AND request_index=?2)",params![r.caller().attempt_id,r.index()],|r|r.get(0))?;
    if exists{return refuse("operation_conflict","request index already has its original charter occurrence");}
    ctx.tx.execute("INSERT INTO charter_requests(id,session_id,attempt_id,request_index,kind,body) VALUES(?1,?2,?3,?4,?5,?6)",params![r.id(),r.session(),r.caller().attempt_id,r.index(),r.kind(),encode(r)?])?;Ok(())
}
// The original native request or acknowledged supplied context determines the
// authoritative revision; audit findings never authorize execution.
//= spec/impl/v2/implementation.md#reference-checks-produce-audit-information
//# The platform MUST check each native turn reply's or supplied outer-turn result's charter references against the authoritative charter revision effective for the native turn or delivered for the supplied outer turn.
//= spec/impl/v2/implementation.md#reference-checks-produce-audit-information
//# A charter reference check MUST NOT be treated as proof of behavioral compliance.
fn audit(source:&ResolvedCharter,raw:&Option<serde_json::Value>)->CharterAudit{
    let mut findings=vec![];
    match raw{
        None=>findings.push(CharterAuditFinding::MissingMetadata),
        Some(raw)=>match serde_json::from_value::<DeclaredCharterReferences>(raw.clone()){
            Err(_)=>findings.push(CharterAuditFinding::MalformedMetadata),
            Ok(v)=>{
                if v.revision!=source.reference{findings.push(CharterAuditFinding::RevisionMismatch);}
                if v.clauses.is_empty(){findings.push(CharterAuditFinding::EmptyApplicabilityUnspecified);}
                let mut seen=BTreeSet::new();for (index,c) in v.clauses.iter().enumerate(){if !source.source.clauses.contains(c){findings.push(CharterAuditFinding::UnknownClause{index});}if !seen.insert(c){findings.push(CharterAuditFinding::DuplicateClause{index});}}
            }
        }
    }CharterAudit{findings}
}
fn observation(tx:&StoreContext<'_>,v:&CharterReplyObservation,l:&CharterLimits)->Result<()>{
    if v.output.is_some()&&v.output_reference.is_some(){return refuse("invalid_input","original output cannot be both inline and referenced");}
    bounded(v,l.record_bytes)?;bounded(&(&v.response_id,&v.output_item_ids,&v.tool_call_ids,&v.declaration),l.metadata_bytes)?;if v.output_item_ids.len()>l.reply_items||v.tool_call_ids.len()>l.tool_calls{return refuse("input_bound","charter reply item count");}
    for ids in [&v.output_item_ids,&v.tool_call_ids]{let mut seen=BTreeSet::new();for id in ids{identity(tx,id)?;if !seen.insert(id){return refuse("invalid_input","duplicate actual reply item identity");}}}
    if let Some(id)=&v.response_id{identity(tx,id)?;}
    if v.declaration.as_ref().and_then(|r|r.get("clauses")).and_then(|r|r.as_array()).is_some_and(|a|a.len()>l.references){return refuse("input_bound","charter.references exceeded");}Ok(())
}
// Native replies and public supplied results retain their original metadata.
//= spec/impl/v2/implementation.md#every-turn-reply-carries-charter-references
//# The durable record of a native turn reply or supplied outer-turn result MUST retain its charter-reference metadata.
// The stored reply must match its original request, effective source and raw
// reference declaration; the recomputed audit must retain that association.
// Supplied occurrence identities remain distinct from hidden model reply IDs.
//= spec/impl/v2/implementation.md#every-turn-reply-carries-charter-references
//# A record that cites a native turn reply or supplied outer-turn result for audit inspection MUST preserve the cited reply's or result's charter-reference metadata association.
fn reply(tx:&StoreContext<'_>,r:&RequestRecord,l:&CharterLimits)->Result<Option<CharterReplyRecord>>{
    let Some(raw)=raw_body(tx,"SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM charter_replies WHERE request_id=?1",r.id(),l.record_bytes)? else{return Ok(None)};
    let v:CharterReplyRecord=stored(&raw,l.record_bytes)?;let id:String=tx.query_row("SELECT CAST(substr(CAST(id AS BLOB),1,?2) AS TEXT) FROM charter_replies WHERE request_id=?1",params![r.id(),super::database::sql_bound(tx.limits.transport.identity_bytes.saturating_add(1))?],|r|r.get(0))?;
    observation(tx,&v.observation,l).map_err(|_|corrupt("stored reply violates bound"))?;
    outputs::verify_representation(tx,r,v.observation.output_reference.as_ref(),l)?;
    if v.reply_id!=id||v.turn_id!=r.id()||v.effective!=r.source().reference||v.audit!=audit(r.source(),&v.observation.declaration){return Err(corrupt("charter reply original association/audit mismatch"));}Ok(Some(v))
}
// Both ingestion paths retain the genuine declaration and passive findings.
//= spec/impl/v2/implementation.md#reference-checks-produce-audit-information
//# The platform MUST record invalid or mismatched charter references with the native turn reply or supplied outer-turn result they came from for audit inspection.
fn insert_reply(ctx:&Context<'_,'_>,r:&RequestRecord,value:&CharterReplyObservation)->Result<CharterReplyRecord>{
    if request_rejection(ctx.tx,r,ctx.limits)?.is_some(){return refuse("operation_conflict","confirmed rejected POST cannot acquire a model reply");}
    observation(ctx.tx,value,ctx.limits)?;outputs::verify_representation(ctx.tx,r,value.output_reference.as_ref(),ctx.limits)?;
    if reply(ctx.tx,r,ctx.limits)?.is_some(){return refuse("operation_conflict","original request already has a reply");}
    let v=CharterReplyRecord{reply_id:ctx.operation_id.into(),turn_id:r.id().into(),effective:r.source().reference.clone(),observation:value.clone(),audit:audit(r.source(),&value.declaration)};bounded(&v,ctx.limits.record_bytes)?;capacity(ctx.tx,ctx.limits,1)?;
    ctx.tx.execute("INSERT INTO charter_replies(id,request_id,body) VALUES(?1,?2,?3)",params![v.reply_id,v.turn_id,encode(&v)?])?;Ok(v)
}
pub(super) fn receipt(tx:&StoreContext<'_>,id:&str)->Result<metadata::Recorded>{
    identity(tx,id)?;let sizes:Option<(i64,i64)>=tx.query_row("SELECT length(CAST(request AS BLOB)),length(CAST(outcome AS BLOB)) FROM receipts WHERE id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let Some((a,b))=sizes else{return refuse("reconciliation_required","original operation has not committed")};
    if unsigned(a)?>tx.limits.transport.body_bytes as u64||unsigned(b)?>tx.limits.transport.body_bytes as u64{return Err(corrupt("original receipt exceeds bound"));}metadata::read(tx,id)?.ok_or_else(||corrupt("receipt disappeared within transaction"))
}
/// Bound the existing receipt before C's canonical typed proof reads it.
fn confirmed_rejection(tx:&StoreContext<'_>,caller:&Caller,index:u32)->Result<Option<super::operations::native_retry::NativeRejectionProof>>{
    let id=crate::native_loop_contract::rejection_id(&caller.attempt_id,index);
    let present:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM receipts WHERE id=?1)",[&id],|r|r.get(0))?;
    if !present{return Ok(None)}
    receipt(tx,&id)?;
    super::operations::native_retry::confirmed_rejection(tx,caller,index)
}
/// A trusted rejected POST has no reply. Its report follows this exact begun
/// occurrence, and the route stays the host-attested route in C's original proof.
fn request_rejection(tx:&StoreContext<'_>,r:&RequestRecord,l:&CharterLimits)->Result<Option<super::operations::native_retry::NativeRejectionProof>>{
    if matches!(r,RequestRecord::SuppliedTurn(_)){return Ok(None)}
    if matches!(r,RequestRecord::SuppliedContext(_)){return refuse("invalid_input","supplied context is not a native provider request");}
    let Some(proof)=confirmed_rejection(tx,r.caller(),r.index())? else{return Ok(None)};
    let task=historical(tx,r.caller(),r.session())?;
    let model=&task.input.definition.required_inference().map_err(|e|corrupt(e))?.model;
    validate_request_receipt(tx,r)?;
    if proof.request_index!=r.index()||&proof.model!=model||proof.controller_revision<=receipt(tx,r.id())?.commit.controller_revision{return refuse("source_mismatch","rejection does not follow this original charter request");}
    let observed:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM charter_replies WHERE request_id=?1) OR EXISTS(SELECT 1 FROM charter_closures WHERE request_id=?1)",[r.id()],|r|r.get(0))?;
    if observed{return refuse("source_mismatch","a rejected request cannot also have a model reply or delivered-result closure");}
    // Exact bounded host report remains the evidence, not an invented endpoint
    // field in the accepted Definition or a synthesized provider response.
    bounded(&(&proof.operation_id,&proof.endpoint,&proof.model),l.record_bytes)?;
    Ok(Some(proof))
}
fn request_at(tx:&StoreContext<'_>,caller:&Caller,index:u32,l:&CharterLimits)->Result<RequestRecord>{
    let id:Option<String>=tx.query_row("SELECT CAST(substr(CAST(id AS BLOB),1,?3) AS TEXT) FROM charter_requests WHERE attempt_id=?1 AND request_index=?2",params![caller.attempt_id,index,super::database::sql_bound(tx.limits.transport.identity_bytes.saturating_add(1))?],|r|r.get(0)).optional()?;
    let r=request(tx,&id.ok_or_else(||Fault::Refused("reconciliation_required","original charter request absent".into()))?,l)?;
    if r.caller()!=caller||r.index()!=index{return refuse("caller_mismatch","rejection belongs to another original charter occurrence");}Ok(r)
}
fn delegated_result(tx:&StoreContext<'_>,r:&RequestRecord,operation_id:&str,outcome:&Outcome,delivered_by:&Caller)->Result<()>{
    let Outcome::Ok{data:Data::Task(created)}=outcome else{return Err(corrupt("delegation receipt lacks original child"))};
    if delivered_by.attempt_id==r.caller().attempt_id{return refuse("reconciliation_required","child creation is not result delivery to resumed parent");}
    historical(tx,delivered_by,r.session())?;
    let row:Option<(String,String,i64,String,Option<String>)>=tx.query_row("SELECT CAST(substr(CAST(parent AS BLOB),1,?2) AS TEXT),CAST(substr(CAST(parent_attempt AS BLOB),1,?2) AS TEXT),parent_generation,CAST(substr(CAST(state AS BLOB),1,?2) AS TEXT),CAST(substr(CAST(outcome AS BLOB),1,?2) AS TEXT) FROM waits WHERE child=?1 AND operation_id=?3",params![created.id,super::database::sql_bound(tx.limits.transport.body_bytes.saturating_add(1))?,operation_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
    let Some((parent,attempt,generation,state,Some(raw)))=row else{return refuse("reconciliation_required","child wait lacks committed result")};
    if parent!=r.caller().task_id||attempt!=r.caller().attempt_id||unsigned(generation)?!=r.caller().generation||state!="settled"{return refuse("reconciliation_required","original child wait not settled");}
    let child:Task=stored(&raw,tx.limits.transport.body_bytes)?;
    if child.id!=created.id||child.parent_task_id.as_deref()!=Some(&parent)||!terminal(child.status){return Err(corrupt("settled child outcome differs from original wait"));}
    let expected=ChildOutcome{task_id:child.id,status:child.status,result:child.result.clone(),summary:child.result.as_ref().map(|v|v.summary.clone()).or(child.blocked_reason).unwrap_or_default()};
    let id=format!("run-{}",delivered_by.attempt_id);let raw=raw_body(tx,"SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM commands WHERE id=?1",&id,tx.limits.controller.run_command_bytes)?.ok_or_else(||corrupt("resumed command absent"))?;
    let command:WorkerCommand=stored(&raw,tx.limits.controller.run_command_bytes)?;
    let WorkerCommand::RunAttempt{command_id,attempt,session,continuation:Some(actual),..}=command else{return refuse("reconciliation_required","resumed command lacks child result")};
    if command_id!=id||attempt.id!=delivered_by.attempt_id||attempt.task_id!=delivered_by.task_id||attempt.generation!=delivered_by.generation||attempt.worker_id!=delivered_by.worker_id||attempt.worker_incarnation!=delivered_by.incarnation||session.id!=r.session()||actual!=expected{return refuse("reconciliation_required","delivered continuation differs from exact child result");}Ok(())
}
//= spec/impl/v2/implementation.md#charter-changes-apply-at-safe-boundaries
//# A charter change MUST NOT apply between a tool call and the delivery of its result.
fn results(tx:&StoreContext<'_>,r:&RequestRecord,v:&CharterReplyRecord,values:&[CharterResultReceipt],delivered_by:&Caller)->Result<()>{
    if let Some(cap)=super::operations::native_output_cap::confirmed(tx,r.caller(),r.index())? {
        if !matches!(r,RequestRecord::Turn(_)) || delivered_by!=r.caller() || !values.is_empty()
            || cap.controller_revision<=receipt(tx,&v.reply_id)?.commit.controller_revision {
            return refuse("reconciliation_required","capped turn has no dispatched result to deliver");
        }
        // Original call IDs remain in the immutable reply; this is expressly
        // an undispatched cap closure, never a fabricated function result.
        historical(tx,delivered_by,r.session())?;return Ok(());
    }

    if values.len()!=v.observation.tool_call_ids.len()||!values.iter().zip(&v.observation.tool_call_ids).all(|(a,b)|a.call_id()==b){return refuse("reconciliation_required","every original ordered call requires its exact committed result");}
    historical(tx,delivered_by,r.session())?;let boundary_commit=receipt(tx,r.id())?.commit.controller_revision;let reply_commit=receipt(tx,&v.reply_id)?.commit.controller_revision;let mut child_delivered=false;
    for (position,value) in values.iter().enumerate(){match value{
        CharterResultReceipt::Collaboration{call_id,operation_id}=>{
            let ordinal=u32::try_from(position).ok().and_then(|i|i.checked_add(1)).ok_or_else(||corrupt("tool ordinal overflow"))?;
            if *operation_id!=crate::native_loop_contract::call_operation_id(&r.caller().attempt_id,r.index(),ordinal){return refuse("invalid_input","tool operation differs from original request/ordinal");}
            let original=receipt(tx,operation_id)?;if original.commit.controller_revision<=boundary_commit||original.commit.controller_revision<=reply_commit{return refuse("reconciliation_required","tool receipt preceded durable original reply");}
            let Operation::CallCollaboration{call}=original.request else{return refuse("invalid_input","evidence is not original collaboration operation")};
            if call.caller!=*r.caller()||call.logical_call_id!=*call_id{return refuse("caller_mismatch","tool receipt belongs to another occurrence");}
            if !matches!(&original.outcome,Outcome::Ok{..}|Outcome::Refused{..}){return refuse("reconciliation_required","tool outcome unresolved");}
            if call.tool=="collab_task_delegate"&&matches!(&original.outcome,Outcome::Ok{..}){delegated_result(tx,r,operation_id,&original.outcome,delivered_by)?;child_delivered=true;}
        }
        CharterResultReceipt::CurrentTask{call_id,report_id}=>{
            let original=receipt(tx,report_id)?;if original.commit.controller_revision<=boundary_commit||original.commit.controller_revision<=reply_commit{return refuse("reconciliation_required","current-task result preceded durable original reply");}
            let Operation::ObserveWorker{report}=original.request else{return refuse("invalid_input","current-task proof is not worker report")};
            if report.report_id!=crate::task_tool::report_id(&r.caller().attempt_id)||report.task_id!=r.caller().task_id||report.attempt_id!=r.caller().attempt_id||report.generation!=r.caller().generation||report.worker_id!=r.caller().worker_id||report.incarnation!=r.caller().incarnation{return refuse("caller_mismatch","current-task report owner differs");}
            let Observation::Progress{text,output,truncated:false}=report.observation else{return refuse("invalid_input","current-task report is not untruncated observation")};
            let task=historical(tx,r.caller(),r.session())?;let l=task.input.execution_limits().map_err(|_|corrupt("accepted execution metadata absent"))?;
            let observed=crate::task_tool::decode(&output,task.input.limits.output_bytes,&l.native).map_err(|_|Fault::Refused("invalid_input","invalid original current-task observation".into()))?;
            if text!=crate::task_tool::PROGRESS_TEXT||observed.call_id!=*call_id||observed.task.task_id!=r.caller().task_id||observed.task.attempt_id!=r.caller().attempt_id||observed.task.generation!=r.caller().generation||observed.task.definition!=task.input.definition||!matches!(&original.outcome,Outcome::Ok{..}){return refuse("invalid_input","current-task result association differs");}
        }
    }}
    if delivered_by!=r.caller()&&!child_delivered{return refuse("caller_mismatch","another attempt cannot close without its exact delivered child result");}Ok(())
}
fn turn(tx:&StoreContext<'_>,id:&str,l:&CharterLimits)->Result<CharterTurn>{
    let r=request(tx,id,l)?;let RequestRecord::Turn(boundary)=&r else{return refuse("invalid_input","request is compaction")};let reply=reply(tx,&r,l)?;
    request_rejection(tx,&r,l)?;
    let closure=raw_body(tx,"SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM charter_closures WHERE request_id=?1",id,l.record_bytes)?.map(|raw|stored::<Closure>(&raw,l.record_bytes)).transpose()?;
    if let Some(c)=&closure{identity(tx,&c.operation_id)?;results(tx,&r,reply.as_ref().ok_or_else(||corrupt("closed turn lacks reply"))?,&c.results,&c.caller).map_err(|_|corrupt("stored turn result proof differs"))?;}
    Ok(CharterTurn{boundary:boundary.clone(),reply,completed_by:closure.as_ref().map(|c|c.operation_id.clone()),delivered_results:closure.map(|c|c.results).unwrap_or_default()})
}
fn compaction_result(tx:&StoreContext<'_>,id:&str,l:&CharterLimits)->Result<Option<CompactionResultRecord>>{
    raw_body(tx,"SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM charter_compaction_results WHERE request_id=?1",id,l.record_bytes)?.map(|raw|stored(&raw,l.record_bytes)).transpose()
}
fn compaction(tx:&StoreContext<'_>,id:&str,l:&CharterLimits)->Result<CharterCompaction>{
    let r=request(tx,id,l)?;let RequestRecord::Compaction(mut c)=r.clone() else{return refuse("invalid_input","request is ordinary turn")};c.reply=reply(tx,&r,l)?;
    match compaction_result(tx,id,l)?{
        Some(CompactionResultRecord::Summary(result))=>{
            if c.reply.is_none()||(result.summary.is_some()&&!c.reply.as_ref().unwrap().observation.tool_call_ids.is_empty())||result.summary.is_some()!=!matches!(result.disposition,CompactionDisposition::NoAcceptedSummary){return Err(corrupt("compaction reply/result association differs"));}
            request_rejection(tx,&r,l)?;
            c.summary=result.summary;c.disposition=Some(result.disposition);
        }
        Some(CompactionResultRecord::Rejected(result))=>{
            identity(tx,&result.operation_id)?;identity(tx,&result.report_id)?;
            let proof=request_rejection(tx,&r,l)?.ok_or_else(||corrupt("compaction rejection lacks exact original provider proof"))?;
            if proof.operation_id!=result.report_id||c.reply.is_some(){return Err(corrupt("compaction rejection differs from original report"));}
            // Return the original begun occurrence with no invented response,
            // summary, or summary disposition. The typed rejection helper below
            // joins the separate canonical report and reconciliation receipt.
        }
        None=>{if c.reply.is_some(){return Err(corrupt("compaction reply lacks atomic result"));}}
    }Ok(c)
}
/// Read-only NativeLoopCompleted evidence join, including original receipts.
pub(crate) fn completed_compaction(tx:&StoreContext<'_>,caller:&Caller,operation_id:&str,request_index:u32)->Result<CharterCompaction>{
    let l=policy(tx)?;let value=compaction(tx,operation_id,l)?;
    if value.caller!=*caller||value.request_index!=request_index||value.reply.is_none()||value.disposition.is_none(){return refuse("reconciliation_required","compaction is not completed original occurrence");}
    let r=RequestRecord::Compaction(CharterCompaction{reply:None,summary:None,disposition:None,..value.clone()});validate_request_receipt(tx,&r)?;validate_reply_receipt(tx,&r,value.reply.as_ref().unwrap(),l)?;Ok(value)
}
/// Original reply only: output-cap validation must not recursively consume a
/// closure whose permission is this same cap receipt. Original raw audit stays exact.
pub(crate) fn recorded_native_reply(tx:&StoreContext<'_>,caller:&Caller,index:u32)->Result<(CharterReplyRecord,u64)> {
    let l=policy(tx)?;let r=request_at(tx,caller,index,l)?;
    if !matches!(&r,RequestRecord::Turn(_)) {return refuse("source_mismatch","output cap is not ordinary turn");}
    let v=reply(tx,&r,l)?.ok_or_else(||Fault::Refused("reconciliation_required","original reply absent".into()))?;
    validate_request_receipt(tx,&r)?;validate_reply_receipt(tx,&r,&v,l)?;
    let revision=receipt(tx,&v.reply_id)?.commit.controller_revision;Ok((v,revision))
}
/// Return a completed ordinary occurrence for the typed native accounting join.
/// The consumer checks final no-tool semantics and ordered group equivalence.
pub(crate) fn completed_native_turn(tx:&StoreContext<'_>,caller:&Caller,request_index:u32)->Result<CharterTurn>{
    let l=policy(tx)?;
    let id:Option<String>=tx.query_row("SELECT CAST(substr(CAST(id AS BLOB),1,?3) AS TEXT) FROM charter_requests WHERE attempt_id=?1 AND request_index=?2",params![caller.attempt_id,request_index,super::database::sql_bound(tx.limits.transport.identity_bytes.saturating_add(1))?],|r|r.get(0)).optional()?;
    let id=id.ok_or_else(||Fault::Refused("reconciliation_required","ordinary charter occurrence is absent".into()))?;
    let r=request(tx,&id,l)?;
    if r.caller()!=caller||r.index()!=request_index||!matches!(&r,RequestRecord::Turn(_)){return refuse("reconciliation_required","charged occurrence is not this original ordinary turn");}
    let value=turn(tx,&id,l)?;
    let observed=value.reply.as_ref().ok_or_else(||Fault::Refused("reconciliation_required","ordinary charter reply is absent".into()))?;
    if value.completed_by.is_none(){return refuse("reconciliation_required","ordinary charter turn has not closed");}
    validate_request_receipt(tx,&r)?;validate_reply_receipt(tx,&r,observed,l)?;validate_closure_receipt_for_turn(tx,&id,l,&value)?;Ok(value)
}
/// Exact rejected compaction proof for the shared request-kind evidence merge.
pub(crate) fn rejected_compaction(tx:&StoreContext<'_>,caller:&Caller,request_index:u32,report_id:&str)->Result<CharterCompaction>{
    let l=policy(tx)?;let r=request_at(tx,caller,request_index,l)?;
    let RequestRecord::Compaction(value)=&r else{return refuse("source_mismatch","rejected occurrence was not a compaction")};
    let proof=request_rejection(tx,&r,l)?.ok_or_else(||Fault::Refused("reconciliation_required","compaction rejection report absent".into()))?;
    if proof.operation_id!=report_id{return refuse("source_mismatch","compaction rejection report identity differs");}
    validate_compaction_rejection(tx,r.id(),l)?;
    Ok(value.clone())
}
/// Ordinary rejected POSTs remain no-reply occurrences. Compactions additionally
/// require the exact durable reconciliation that clears their in-flight state.
pub(crate) fn rejected_request_kind(tx:&StoreContext<'_>,caller:&Caller,request_index:u32,report_id:&str)->Result<RejectedRequestKind>{
    let l=policy(tx)?;let r=request_at(tx,caller,request_index,l)?;
    let proof=request_rejection(tx,&r,l)?.ok_or_else(||Fault::Refused("reconciliation_required","original provider rejection absent".into()))?;
    if proof.operation_id!=report_id{return refuse("source_mismatch","rejection report identity differs");}
    match r{RequestRecord::Turn(_)=>Ok(RejectedRequestKind::Turn),RequestRecord::Compaction(_)=>{rejected_compaction(tx,caller,request_index,report_id)?;Ok(RejectedRequestKind::Compaction)},RequestRecord::SuppliedContext(_)|RequestRecord::SuppliedTurn(_)=>refuse("invalid_input","supplied context has no native rejection kind")}
}
fn safe_boundary(ctx:&Context<'_,'_>,s:&CharterSession,caller:&Caller)->Result<()>{
    if let Some(id)=&s.last_turn_id{let old=turn(ctx.tx,id,ctx.limits)?;
        if old.completed_by.is_none()&&(old.boundary.caller.attempt_id==caller.attempt_id||old.reply.as_ref().is_some_and(|r|!r.observation.tool_call_ids.is_empty())){
            let r=RequestRecord::Turn(old.boundary.clone());
            if request_rejection(ctx.tx,&r,ctx.limits)?.is_none(){return refuse("reconciliation_required","previous turn has neither exact delivered results nor confirmed rejection");}
        }
    }Ok(())
}
fn reservation(tx:&StoreContext<'_>,caller:&Caller,index:u32)->Result<metadata::Recorded> {
    let id=crate::native_loop_contract::reservation_id(&caller.attempt_id,index);
    let original=receipt(tx,&id)?;
    let valid=match &original.request {
        Operation::ObserveWorker{report}=>report.report_id==id&&report.task_id==caller.task_id&&report.attempt_id==caller.attempt_id&&report.generation==caller.generation&&report.worker_id==caller.worker_id&&report.incarnation==caller.incarnation&&matches!(report.observation,Observation::Usage{model_requests,..}if model_requests==index),
        _=>false,
    };
    if !valid||!matches!(&original.outcome,Outcome::Ok{data:Data::Task(task)}if task.id==caller.task_id&&task.attempt.as_ref().is_some_and(|a|a.id==caller.attempt_id&&!a.stopped&&!a.uncertain&&a.phase!=AttemptPhase::StopRequested)){return refuse("reconciliation_required","charter request lacks its exact acknowledged model reservation");}Ok(original)
}
fn index(ctx:&Context<'_, '_>,caller:&Caller,task:&Task,index:u32)->Result<()> {
    let id=format!("run-{}",caller.attempt_id);
    let raw=raw_body(ctx.tx,"SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM commands WHERE id=?1",&id,ctx.tx.limits.controller.run_command_bytes)?.ok_or_else(||corrupt("charter original RunAttempt absent"))?;
    let command:WorkerCommand=stored(&raw,ctx.tx.limits.controller.run_command_bytes)?;
    let WorkerCommand::RunAttempt{command_id,attempt,task:accepted,session,..}=command else{return Err(corrupt("charter original command kind differs"))};
    if command_id!=id||attempt.id!=caller.attempt_id||attempt.task_id!=caller.task_id||attempt.generation!=caller.generation||attempt.worker_id!=caller.worker_id||attempt.worker_incarnation!=caller.incarnation||session.id!=task.session_id{return Err(corrupt("charter original RunAttempt identity differs"));}
    let mut expected=task.input.clone();expected.limits.model_requests=accepted.limits.model_requests;
    if accepted!=expected||index==0||index>accepted.limits.model_requests{return refuse("input_bound","charter request index exceeds original remaining model allowance");}
    reservation(ctx.tx,caller,index)?;
    if confirmed_rejection(ctx.tx,caller,index)?.is_some(){return refuse("operation_conflict","a confirmed rejected request index cannot begin again");}
    let charged:i64=ctx.tx.query_row("SELECT model_requests FROM attempts WHERE id=?1",[&caller.attempt_id],|r|r.get(0))?;
    if unsigned(charged)?<index as u64{return Err(corrupt("charter model reservation counter rolled back"));}
    let previous:i64=ctx.tx.query_row("SELECT coalesce(max(request_index),0) FROM charter_requests WHERE attempt_id=?1",[&caller.attempt_id],|r|r.get(0))?;
    if unsigned(previous)?.checked_add(1)!=Some(index as u64){return refuse("operation_conflict","charter request index is not the next original occurrence");}
    if previous>0 {
        let previous=u32::try_from(unsigned(previous)?).map_err(|_|corrupt("charter previous index exceeds native representation"))?;
        if confirmed_rejection(ctx.tx,caller,previous)?.is_some_and(|proof|!proof.rejection.kind.same_input_retryable()) {
            return refuse("reconciliation_required","confirmed rejection does not permit another request in this attempt");
        }
    }Ok(())
}
fn adopt(s:&mut CharterSession) {
    if let Some(next)=s.pending.take(){if next!=s.effective{s.effective=next.clone();s.compaction_due=Some(next);}}
}
fn effective(tx:&StoreContext<'_>,session:CharterSession,l:&CharterLimits)->Result<EffectiveCharter>{let charter=resolve(tx,&session.effective,l)?;Ok(EffectiveCharter{session,charter})}
/// Root permits only its established trusted configuration invocation route.
/// This helper never receives an asserted model actor or creates a wake event.
pub(crate) fn mutate(ctx:&Context<'_, '_>,input:&CharterMutation)->Result<CharterOutcome> {
    identity(ctx.tx,ctx.operation_id)?;
    super::identity_administration::authorize(ctx.tx)?;
    match input {
        CharterMutation::Publish{..}|CharterMutation::SelectForFutureTasks{..} => super::charter_definitions::apply(ctx,input),
        CharterMutation::SetPending{session_id,expected_revision,target}=>{
            let mut s=require_session(ctx.tx,session_id,ctx.limits)?;
            if s.revision!=*expected_revision{return refuse("stale_revision","charter binding differs");}
            let next=resolve(ctx.tx,target,ctx.limits)?.reference;
            // Explicit update is recorded even if it supersedes another pending
            // change; the canonical operation journal retains both decisions.
            s.pending=if next==s.effective{None}else{Some(next)};
            let old=advance(&mut s)?;save_session(ctx.tx,&s,ctx.limits,Some(old),Some(ctx.operation_id))?;
            Ok(CharterOutcome::Session(effective(ctx.tx,s,ctx.limits)?))
        }
    }
}
/// Currentness gates new execution boundaries; original late audit remains
/// bound to its retained occurrence and confers no new execution authority. Exact
/// original operation replay is performed by Database::process before dispatch.
pub(crate) fn observe(ctx:&Context<'_, '_>,caller:&Caller,input:&CharterObservation)->Result<CharterOutcome> {
    identity(ctx.tx,ctx.operation_id)?;
    match input {
        CharterObservation::BeginSuppliedTurn{delivery_id,outer_index,request_id,thread_id,output_schema}=>supplied_turns::begin(ctx,caller,delivery_id,*outer_index,*request_id,thread_id,output_schema),
        CharterObservation::AckSuppliedTurn{turn_id,request_id,response}=>supplied_turns::ack(ctx,caller,turn_id,*request_id,response),
        CharterObservation::SuppliedReply{turn_id,terminal,reply}=>supplied_turns::result(ctx,caller,turn_id,terminal,reply),
        CharterObservation::OutputChunk{chunk}=>outputs::put(ctx,caller,chunk).map(CharterOutcome::OutputChunk),
        CharterObservation::BeginSuppliedContext{session_id,expected_revision,source,request_id,developer_instructions}=>supplied::begin(ctx,caller,session_id,*expected_revision,source,*request_id,developer_instructions),
        CharterObservation::AckSuppliedContext{delivery_id,request_id,response}=>supplied::ack(ctx,caller,delivery_id,*request_id,response),
        CharterObservation::BeginTurn{session_id,expected_revision,request_index}=>{
            let task=current(ctx,caller,session_id)?;let mut s=require_session(ctx.tx,session_id,ctx.limits)?;
            if s.revision!=*expected_revision{return refuse("stale_revision","charter binding differs");}
            safe_boundary(ctx,&s,caller)?;
            if s.compaction_in_flight.is_some(){return refuse("reconciliation_required","original compaction remains unresolved");}
            index(ctx,caller,&task,*request_index)?;adopt(&mut s);
            let old=advance(&mut s)?;
            let boundary=CharterBoundary{turn_id:ctx.operation_id.into(),session_id:session_id.clone(),caller:caller.clone(),request_index:*request_index,binding_revision:s.revision,effective:resolve(ctx.tx,&s.effective,ctx.limits)?};
            insert_request(ctx,&RequestRecord::Turn(boundary.clone()))?;s.last_turn_id=Some(boundary.turn_id.clone());save_session(ctx.tx,&s,ctx.limits,Some(old),Some(ctx.operation_id))?;
            Ok(CharterOutcome::Boundary(boundary))
        }
        CharterObservation::Reply{turn_id,reply:value}=>{
            let r=request(ctx.tx,turn_id,ctx.limits)?;
            if !matches!(&r,RequestRecord::Turn(_)){return refuse("invalid_input","use compaction observation for this request");}
            historical(ctx.tx,caller,r.session())?;
            if r.caller()!=caller{return refuse("caller_mismatch","reply belongs to another original attempt");}
            Ok(CharterOutcome::Reply(insert_reply(ctx,&r,value)?))
        }
        CharterObservation::CompleteTurn{turn_id,results:values}=>{
            let r=request(ctx.tx,turn_id,ctx.limits)?;
            // Original occurrence may append audit after stop/cancel. A later
            // attempt may close only a proved delivered child continuation.
            if r.caller()==caller {historical(ctx.tx,caller,r.session())?;} else {current(ctx,caller,r.session())?;}
            if !matches!(&r,RequestRecord::Turn(_)){return refuse("invalid_input","cannot close compaction as tool turn");}
            let prior=turn(ctx.tx,turn_id,ctx.limits)?;
            if prior.completed_by.is_some(){return refuse("operation_conflict","original turn already closed");}
            let received=prior.reply.as_ref().ok_or_else(||Fault::Refused("reconciliation_required","no original reply was recorded".into()))?;
            results(ctx.tx,&r,received,values,caller)?;
            let closure=Closure{operation_id:ctx.operation_id.into(),caller:caller.clone(),results:values.clone()};bounded(&closure,ctx.limits.record_bytes)?;capacity(ctx.tx,ctx.limits,1)?;
            ctx.tx.execute("INSERT INTO charter_closures(request_id,body) VALUES(?1,?2)",params![turn_id,encode(&closure)?])?;
            Ok(CharterOutcome::Turn(turn(ctx.tx,turn_id,ctx.limits)?))
        }
        //= spec/impl/v2/implementation.md#native-charter-changes-preserve-prompt-continuity
        //# Native charter changes MUST be debounced, so that a burst of them costs one compaction.
        //= spec/impl/v2/implementation.md#native-charter-changes-preserve-prompt-continuity
        //# A native charter change arriving while a compaction is already due MUST be included in that compaction.
        CharterObservation::BeginCompaction{session_id,expected_revision,request_index}=>{
            let task=current(ctx,caller,session_id)?;let mut s=require_session(ctx.tx,session_id,ctx.limits)?;
            if s.revision!=*expected_revision{return refuse("stale_revision","charter binding differs");}
            safe_boundary(ctx,&s,caller)?;
            if s.compaction_in_flight.is_some(){return refuse("reconciliation_required","original compaction outcome remains in flight");}
            if s.compaction_due.is_none(){return refuse("not_due","no charter-change compaction is due");}
            // A newer pending change joins an already due compaction at this
            // actual safe boundary. No wake or extra idle request is produced.
            adopt(&mut s);
            if s.compaction_due.as_ref()!=Some(&s.effective){return refuse("not_due","no charter-change compaction is due");}
            index(ctx,caller,&task,*request_index)?;
            let c=CharterCompaction{run_id:ctx.operation_id.into(),session_id:session_id.clone(),caller:caller.clone(),request_index:*request_index,effective:resolve(ctx.tx,&s.effective,ctx.limits)?,reply:None,summary:None,disposition:None};
            insert_request(ctx,&RequestRecord::Compaction(c.clone()))?;
            s.compaction_in_flight=Some(c.run_id.clone());s.compaction_due=None;let old=advance(&mut s)?;save_session(ctx.tx,&s,ctx.limits,Some(old),Some(ctx.operation_id))?;
            Ok(CharterOutcome::Compaction(c))
        }
        CharterObservation::CompactionRejected{run_id,report_id}=>{
            let r=request(ctx.tx,run_id,ctx.limits)?;
            let RequestRecord::Compaction(original)=&r else{return refuse("invalid_input","request is not a compaction")};
            historical(ctx.tx,caller,r.session())?;
            if r.caller()!=caller{return refuse("caller_mismatch","compaction rejection belongs to another original attempt");}
            let proof=request_rejection(ctx.tx,&r,ctx.limits)?.ok_or_else(||Fault::Refused("reconciliation_required","original rejection has not committed".into()))?;
            if proof.operation_id!=*report_id{return refuse("source_mismatch","compaction rejection report identity differs");}
            let mut s=require_session(ctx.tx,r.session(),ctx.limits)?;
            if s.compaction_in_flight.as_deref()!=Some(run_id)||compaction_result(ctx.tx,run_id,ctx.limits)?.is_some(){return refuse("operation_conflict","compaction is not the original unresolved in-flight occurrence");}
            let result=CompactionResultRecord::Rejected(CompactionRejection{operation_id:ctx.operation_id.into(),report_id:report_id.clone()});
            bounded(&result,ctx.limits.record_bytes)?;capacity(ctx.tx,ctx.limits,1)?;
            ctx.tx.execute("INSERT INTO charter_compaction_results(request_id,body) VALUES(?1,?2)",params![run_id,encode(&result)?])?;
            s.compaction_in_flight=None;s.compaction_due=Some(s.pending.clone().unwrap_or_else(||s.effective.clone()));
            let old=advance(&mut s)?;save_session(ctx.tx,&s,ctx.limits,Some(old),Some(ctx.operation_id))?;
            Ok(CharterOutcome::Compaction(original.clone()))
        }
        CharterObservation::CompactionReply{run_id,reply:value,summary}=>{
            let r=request(ctx.tx,run_id,ctx.limits)?;
            let RequestRecord::Compaction(original)=&r else{return refuse("invalid_input","request is not a compaction")};
            historical(ctx.tx,caller,r.session())?;
            if r.caller()!=caller{return refuse("caller_mismatch","compaction reply belongs to original attempt");}
            let still_current=match current(ctx,caller,r.session()) {Ok(_)=>true,Err(Fault::Refused(..))=>false,Err(e)=>return Err(e)};
            let mut s=require_session(ctx.tx,r.session(),ctx.limits)?;
            if s.compaction_in_flight.as_deref()!=Some(run_id){return refuse("operation_conflict","compaction is not the original in-flight occurrence");}
            if summary.is_some()&&!value.tool_call_ids.is_empty(){return refuse("invalid_input","an accepted compaction summary cannot claim tool calls");}
            if summary.as_ref().is_some_and(|v|v.trim().is_empty()||v.contains('\0')){return refuse("invalid_input","accepted compaction summary is empty or malformed");}
            let disposition=match summary {
                None=>CompactionDisposition::NoAcceptedSummary,
                Some(_) if !still_current || s.effective!=original.effective.reference||s.pending.as_ref().is_some_and(|p|p!=&original.effective.reference)=>CompactionDisposition::ObsoleteSourceSummary,
                Some(_)=>CompactionDisposition::CurrentSourceSummary,
            };
            let result=CompactionResult{summary:summary.clone(),disposition:disposition.clone()};bounded(&result,ctx.limits.record_bytes)?;capacity(ctx.tx,ctx.limits,2)?;
            insert_reply(ctx,&r,value)?;
            ctx.tx.execute("INSERT INTO charter_compaction_results(request_id,body) VALUES(?1,?2)",params![run_id,encode(&result)?])?;
            s.compaction_in_flight=None;
            if !matches!(disposition,CompactionDisposition::CurrentSourceSummary){s.compaction_due=Some(s.pending.clone().unwrap_or_else(||s.effective.clone()));}
            let old=advance(&mut s)?;save_session(ctx.tx,&s,ctx.limits,Some(old),Some(ctx.operation_id))?;
            Ok(CharterOutcome::Compaction(compaction(ctx.tx,run_id,ctx.limits)?))
        }
    }
}
pub(crate) fn query(tx:&StoreContext<'_>,q:&CharterQuery)->Result<CharterQueryResult> {
    let l=policy(tx)?;
    let result=match q {
        CharterQuery::SuppliedTurn{turn_id}=>CharterQueryResult::SuppliedTurn(supplied_turns::query(tx,turn_id,l)?),
        CharterQuery::OutputChunk{request_id,index}=>CharterQueryResult::OutputChunk(outputs::query(tx,request_id,*index,l)?),
        CharterQuery::SuppliedContext{delivery_id}=>CharterQueryResult::SuppliedContext(supplied::query(tx,delivery_id,l)?),
        CharterQuery::Source{source}=>CharterQueryResult::Source(resolve(tx,source,l)?),
        CharterQuery::Session{session_id}=>CharterQueryResult::Session(session(tx,session_id,l)?.map(|s|effective(tx,s,l)).transpose()?),
        CharterQuery::Turn{turn_id}=>CharterQueryResult::Turn(turn(tx,turn_id,l)?),
        CharterQuery::Compaction{run_id}=>CharterQueryResult::Compaction(compaction(tx,run_id,l)?),
        CharterQuery::Turns{session_id,after,limit}=>{
            identity(tx,session_id)?;bounded_session(tx,session_id)?;
            if *limit==0||*limit>l.page_items{return refuse("input_bound","charter.page_items exceeded");}
            if let Some(a)=after{identity(tx,a)?;let r=request(tx,a,l)?;if r.session()!=session_id||!matches!(r,RequestRecord::Turn(_)){return refuse("invalid_input","charter cursor belongs to another session or record kind");}}
            let mut s=tx.prepare("SELECT CAST(substr(CAST(id AS BLOB),1,?4) AS TEXT) FROM charter_requests WHERE session_id=?1 AND kind='turn' AND (?2 IS NULL OR id>?2) ORDER BY id LIMIT ?3")?;
            let mut rows=s.query(params![session_id,after,i64::try_from(limit.saturating_add(1)).map_err(|_|corrupt("page bound overflow"))?,super::database::sql_bound(tx.limits.transport.identity_bytes.saturating_add(1))?])?;
            let mut items=vec![];let mut next=None;
            while let Some(row)=rows.next()?{let id:String=row.get(0)?;identity(tx,&id)?;if items.len()==*limit{next=items.last().map(|t:&CharterTurn|t.boundary.turn_id.clone());break}items.push(turn(tx,&id,l)?);bounded(&items,l.page_bytes)?;}
            CharterQueryResult::Turns(crate::collaboration::Page{items,next_cursor:next})
        }
    };
    bounded(&result,tx.limits.transport.body_bytes)?;Ok(result)
}
/// Validate a new immutable definition before admission. Source reuse is only
/// through an existing complete definition revision; no ambient file fetching.
pub(crate) fn validate_definition(tx:&StoreContext<'_>,definition:&Definition)->Result<()> {
    let Some(charter)=&definition.charter else {
        return if tx.limits.charter.is_some(){refuse("charter_required","new definitions and task admissions require an explicit resolvable charter under enabled policy")}else{Ok(())}
    };
    let l=policy(tx)?;
    match charter {
        DefinitionCharter::Inline{source}=>validate_source(tx,source,l),
        DefinitionCharter::Reference{source}=>resolve(tx,source,l).map(|_|()),
    }
}
fn charter_outcome(record:&metadata::Recorded)->Result<&CharterOutcome> {
    match &record.outcome {Outcome::Ok{data:Data::Charter(value)}=>Ok(value),_=>Err(corrupt("charter record lacks its successful original operation"))}
}
fn validate_request_receipt(tx:&StoreContext<'_>,r:&RequestRecord)->Result<()> {
    let original=receipt(tx,r.id())?;
    if !matches!(r,RequestRecord::SuppliedContext(_)|RequestRecord::SuppliedTurn(_))&&reservation(tx,r.caller(),r.index())?.commit.controller_revision>=original.commit.controller_revision{return Err(corrupt("charter boundary preceded model reservation"));}
    let Operation::ObserveCharter{operation_id,caller,observation}=&original.request else{return Err(corrupt("charter request receipt kind differs"))};
    if operation_id!=r.id()||caller!=r.caller(){return Err(corrupt("charter request original caller differs"));}
    let valid=match (r,observation,charter_outcome(&original)?) {
        (RequestRecord::SuppliedContext(a),CharterObservation::BeginSuppliedContext{session_id,expected_revision,source,request_id,developer_instructions},CharterOutcome::SuppliedContext(b))=>a==b&&session_id==&a.session_id&&*request_id==a.request_id&&source==&a.effective.reference&&developer_instructions==&a.developer_instructions&&expected_revision.checked_add(1)==Some(a.binding_revision),
        (RequestRecord::SuppliedTurn(a),CharterObservation::BeginSuppliedTurn{delivery_id,outer_index,request_id,thread_id,output_schema},CharterOutcome::SuppliedTurn(b))=>a==&b.boundary&&b.acknowledgment.is_none()&&b.reply.is_none()&&b.terminal.is_none()&&delivery_id==&a.delivery_id&&*outer_index==a.outer_index&&*request_id==a.request_id&&thread_id==&a.thread_id&&output_schema==&a.output_schema,
        (RequestRecord::Turn(a),CharterObservation::BeginTurn{session_id,expected_revision,request_index},CharterOutcome::Boundary(b))=>a==b&&session_id==&a.session_id&&*request_index==a.request_index&&expected_revision.checked_add(1)==Some(a.binding_revision),
        (RequestRecord::Compaction(a),CharterObservation::BeginCompaction{session_id,request_index,..},CharterOutcome::Compaction(b))=>a==b&&session_id==&a.session_id&&*request_index==a.request_index,
        _=>false,
    };
    if !valid{return Err(corrupt("charter request differs from original accepted boundary"));}Ok(())
}
fn validate_reply_receipt(tx:&StoreContext<'_>,r:&RequestRecord,v:&CharterReplyRecord,l:&CharterLimits)->Result<()> {
    let original=receipt(tx,&v.reply_id)?;
    if original.commit.controller_revision<=receipt(tx,r.id())?.commit.controller_revision{return Err(corrupt("charter reply preceded its original request"));}
    let Operation::ObserveCharter{caller,observation,..}=&original.request else{return Err(corrupt("charter reply receipt kind differs"))};
    if caller!=r.caller(){return Err(corrupt("charter reply original caller differs"));}
    let valid=match (r,observation,charter_outcome(&original)?) {
        (RequestRecord::SuppliedTurn(_),CharterObservation::SuppliedReply{turn_id,terminal,reply},CharterOutcome::Reply(record))=>{
            supplied_turns::validate_terminal(tx,r.id(),terminal,l)?;
            turn_id==r.id()&&reply==&v.observation&&record==v
        },
        (RequestRecord::Turn(_),CharterObservation::Reply{turn_id,reply},CharterOutcome::Reply(record))=>turn_id==r.id()&&reply==&v.observation&&record==v,
        (RequestRecord::Compaction(_),CharterObservation::CompactionReply{run_id,reply,summary},CharterOutcome::Compaction(record))=>run_id==r.id()&&reply==&v.observation&&summary==&record.summary&&record==&compaction(tx,r.id(),l)?,
        _=>false,
    };
    if !valid{return Err(corrupt("charter reply differs from original committed observation"));}Ok(())
}
fn validate_closure_receipt(tx:&StoreContext<'_>,id:&str,l:&CharterLimits)->Result<()> {
    let value=turn(tx,id,l)?;
    validate_closure_receipt_for_turn(tx,id,l,&value)
}
// The caller has already validated this complete turn in this same transaction.
// Reuse that value; original closure/result/receipt chronology remains checked.
fn validate_closure_receipt_for_turn(tx:&StoreContext<'_>,id:&str,l:&CharterLimits,value:&CharterTurn)->Result<()> {
    let Some(operation_id)=&value.completed_by else{return Ok(())};
    let raw=raw_body(tx,"SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM charter_closures WHERE request_id=?1",id,l.record_bytes)?.ok_or_else(||corrupt("closure disappeared"))?;
    let c:Closure=stored(&raw,l.record_bytes)?;
    let original=receipt(tx,operation_id)?;
    let reply=value.reply.as_ref().ok_or_else(||corrupt("closure has no original reply"))?;
    if receipt(tx,&reply.reply_id)?.commit.controller_revision>=original.commit.controller_revision{return Err(corrupt("closure preceded original reply"));}
    if let Some(cap)=super::operations::native_output_cap::confirmed(tx,&value.boundary.caller,value.boundary.request_index)? {
        if cap.controller_revision>=original.commit.controller_revision {return Err(corrupt("cap closure preceded original cap receipt"));}
    }
    for result in &c.results {
        let id=match result{CharterResultReceipt::Collaboration{operation_id,..}=>operation_id,CharterResultReceipt::CurrentTask{report_id,..}=>report_id};
        if receipt(tx,id)?.commit.controller_revision>=original.commit.controller_revision{return Err(corrupt("closure preceded committed result"));}
    }
    let valid=match (&original.request,charter_outcome(&original)?) {
        (Operation::ObserveCharter{caller,observation:CharterObservation::CompleteTurn{turn_id,results},..},CharterOutcome::Turn(record))=>caller==&c.caller&&turn_id==id&&results==&c.results&&record==value,
        _=>false,
    };
    if !valid{return Err(corrupt("charter completion differs from original committed delivery"));}Ok(())
}
fn validate_compaction_rejection(tx:&StoreContext<'_>,id:&str,l:&CharterLimits)->Result<()>{
    let r=request(tx,id,l)?;let RequestRecord::Compaction(value)=&r else{return Err(corrupt("rejected result belongs to an ordinary request"))};
    let Some(CompactionResultRecord::Rejected(result))=compaction_result(tx,id,l)? else{return refuse("reconciliation_required","compaction rejection has not been reconciled")};
    let proof=request_rejection(tx,&r,l)?.ok_or_else(||corrupt("compaction rejection proof absent"))?;
    let original=receipt(tx,&result.operation_id)?;
    if proof.operation_id!=result.report_id||proof.controller_revision>=original.commit.controller_revision{return Err(corrupt("compaction rejection reconciliation preceded its exact original report"));}
    let valid=matches!((&original.request,charter_outcome(&original)?),(Operation::ObserveCharter{operation_id,caller,observation:CharterObservation::CompactionRejected{run_id,report_id}},CharterOutcome::Compaction(record)) if operation_id==&result.operation_id&&caller==r.caller()&&run_id==id&&report_id==&result.report_id&&record==value);
    if !valid{return Err(corrupt("compaction rejection differs from original reconciliation receipt"));}
    let mut statement=tx.prepare("SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM charter_session_versions WHERE operation_id=?1 LIMIT 2")?;
    let mut rows=statement.query(params![result.operation_id,super::database::sql_bound(l.record_bytes.saturating_add(1))?])?;
    let row=rows.next()?.ok_or_else(||corrupt("compaction rejection lacks retained session transition"))?;
    let next:CharterSession=stored(&row.get::<_,String>(0)?,l.record_bytes)?;
    if rows.next()?.is_some()||next.session_id!=r.session()||next.revision<2{return Err(corrupt("compaction rejection has an ambiguous session transition"));}
    let raw:String=tx.query_row("SELECT CAST(substr(CAST(body AS BLOB),1,?3) AS TEXT) FROM charter_session_versions WHERE id=?1 AND revision=?2",params![next.session_id,integer(next.revision-1)?,super::database::sql_bound(l.record_bytes.saturating_add(1))?],|r|r.get(0))?;
    let previous:CharterSession=stored(&raw,l.record_bytes)?;
    version_transition(tx,Some(&previous),&next,Some(&result.operation_id),l)?;Ok(())
}
fn version_transition(tx:&StoreContext<'_>,previous:Option<&CharterSession>,next:&CharterSession,operation_id:Option<&str>,l:&CharterLimits)->Result<()> {
    let Some(previous)=previous else {
        if operation_id.is_some()||next.revision!=1||next.effective!=next.initial||next.pending.is_some()||next.last_turn_id.is_some()||next.compaction_due.is_some()||next.compaction_in_flight.is_some(){return Err(corrupt("initial charter binding is not accepted-definition identity"));}
        return Ok(())
    };
    let operation_id=operation_id.ok_or_else(||corrupt("charter transition lacks original operation identity"))?;
    let original=receipt(tx,operation_id)?;
    let original=super::identity_administration::project(original)?;
    let outcome=charter_outcome(&original)?;
    let mut expected=previous.clone();
    match (&original.request,outcome) {
        (Operation::ObserveCharter{observation:CharterObservation::BeginSuppliedContext{session_id,expected_revision,..},..},CharterOutcome::SuppliedContext(delivery)) if session_id==&previous.session_id&&*expected_revision==previous.revision=>{
            supplied::initial_binding(&mut expected)?;
            if delivery.delivery_id!=operation_id||delivery.binding_revision!=next.revision||delivery.effective!=resolve(tx,&expected.effective,l)?{return Err(corrupt("supplied context source/version differs"));}
        }
        (Operation::MutateCharter{mutation:CharterMutation::SetPending{session_id,expected_revision,target},..},CharterOutcome::Session(result)) if session_id==&previous.session_id&&*expected_revision==previous.revision=>{
            let target=resolve(tx,target,l)?.reference;expected.pending=if target==expected.effective{None}else{Some(target)};
            if result.session!=*next||result.charter!=resolve(tx,&next.effective,l)?{return Err(corrupt("charter configuration result differs from retained version"));}
        }
        (Operation::ObserveCharter{observation:CharterObservation::BeginTurn{session_id,expected_revision,..},..},CharterOutcome::Boundary(boundary)) if session_id==&previous.session_id&&*expected_revision==previous.revision=>{
            if previous.compaction_in_flight.is_some(){return Err(corrupt("turn began over unresolved compaction"));}
            adopt(&mut expected);expected.last_turn_id=Some(operation_id.into());
            if boundary.turn_id!=operation_id||boundary.binding_revision!=next.revision||boundary.effective!=resolve(tx,&expected.effective,l)?{return Err(corrupt("charter boundary version differs"));}
        }
        (Operation::ObserveCharter{observation:CharterObservation::BeginCompaction{session_id,expected_revision,..},..},CharterOutcome::Compaction(run)) if session_id==&previous.session_id&&*expected_revision==previous.revision=>{
            if previous.compaction_due.is_none()||previous.compaction_in_flight.is_some(){return Err(corrupt("compaction transition was not due or already in flight"));}
            adopt(&mut expected);expected.compaction_due=None;expected.compaction_in_flight=Some(operation_id.into());
            if run.run_id!=operation_id||run.effective!=resolve(tx,&expected.effective,l)?{return Err(corrupt("compaction effective revision differs"));}
        }
        (Operation::ObserveCharter{caller,observation:CharterObservation::CompactionRejected{run_id,report_id},..},CharterOutcome::Compaction(run)) if previous.compaction_in_flight.as_deref()==Some(run_id.as_str())=>{
            let r=request(tx,run_id,l)?;
            let RequestRecord::Compaction(begun)=&r else{return Err(corrupt("rejection transition is not a compaction"))};
            let proof=request_rejection(tx,&r,l)?.ok_or_else(||corrupt("rejection transition lacks its canonical proof"))?;
            let Some(CompactionResultRecord::Rejected(result))=compaction_result(tx,run_id,l)? else{return Err(corrupt("rejection transition lacks its exact result"))};
            if caller!=r.caller()||run!=begun||proof.operation_id!=*report_id||result.report_id!=*report_id||result.operation_id!=operation_id||proof.controller_revision>=original.commit.controller_revision{return Err(corrupt("rejection transition differs from original report and begun occurrence"));}
            expected.compaction_in_flight=None;expected.compaction_due=Some(expected.pending.clone().unwrap_or_else(||expected.effective.clone()));
        }
        (Operation::ObserveCharter{observation:CharterObservation::CompactionReply{run_id,..},..},CharterOutcome::Compaction(run)) if previous.compaction_in_flight.as_deref()==Some(run_id.as_str())=>{
            expected.compaction_in_flight=None;
            if run.run_id!=*run_id||run.disposition.is_none()||compaction(tx,run_id,l)?!=*run{return Err(corrupt("compaction transition lacks original result"));}
            if run.disposition!=Some(CompactionDisposition::CurrentSourceSummary){expected.compaction_due=Some(expected.pending.clone().unwrap_or_else(||expected.effective.clone()));}
        }
        _=>return Err(corrupt("charter version transition is not its original operation")),
    }
    advance(&mut expected)?;
    if expected!=*next{return Err(corrupt("charter version differs from exact recorded transition"));}Ok(())
}
/// The original operation requires exactly one retained session transition.
/// This reverse join also catches a deleted latest version plus rolled-back head.
fn require_version_membership(tx:&StoreContext<'_>,operation_id:&str,session_id:&str,l:&CharterLimits)->Result<()> {
    let mut q=tx.prepare("SELECT CAST(substr(CAST(id AS BLOB),1,?2) AS TEXT),revision,CAST(substr(CAST(body AS BLOB),1,?3) AS TEXT) FROM charter_session_versions WHERE operation_id=?1 LIMIT 2")?;
    let mut rows=q.query(params![operation_id,super::database::sql_bound(tx.limits.transport.identity_bytes.saturating_add(1))?,super::database::sql_bound(l.record_bytes.saturating_add(1))?])?;
    let row=rows.next()?.ok_or_else(||corrupt("successful charter operation lacks its session transition"))?;
    let id:String=row.get(0)?;let revision=unsigned(row.get(1)?)?;
    let next:CharterSession=stored(&row.get::<_,String>(2)?,l.record_bytes)?;
    if rows.next()?.is_some()||id!=session_id||next.session_id!=id||next.revision!=revision||revision<2{return Err(corrupt("original charter transition membership differs"));}
    let raw:String=tx.query_row("SELECT CAST(substr(CAST(body AS BLOB),1,?3) AS TEXT) FROM charter_session_versions WHERE id=?1 AND revision=?2",params![id,integer(revision-1)?,super::database::sql_bound(l.record_bytes.saturating_add(1))?],|r|r.get(0))?;
    let previous:CharterSession=stored(&raw,l.record_bytes)?;
    if previous.session_id!=id||previous.revision!=revision-1{return Err(corrupt("original charter transition predecessor differs"));}
    version_transition(tx,Some(&previous),&next,Some(operation_id),l)
}
/// Surviving rows alone cannot prove retained membership: Reply and CompleteTurn
/// do not advance the session head. Successful original receipts must still have
/// every row they committed, even when both reply and closure were deleted.
fn validate_request_vocabulary(tx:&StoreContext<'_>,request:&Operation)->Result<()> {
    if matches!(request,Operation::ObserveCharter{observation:CharterObservation::BeginSuppliedTurn{..}|CharterObservation::AckSuppliedTurn{..}|CharterObservation::SuppliedReply{..},..}){supplied_turns::require_schema(tx)?;}
    Ok(())
}
fn validate_original_membership(tx:&StoreContext<'_>,original:&metadata::Recorded,l:&CharterLimits)->Result<()> {
    let projected=super::identity_administration::project(metadata::Recorded{request:original.request.clone(),outcome:original.outcome.clone(),commit:original.commit.clone()})?;
    let original=&projected;
    validate_request_vocabulary(tx,&original.request)?;
    if !matches!(&original.request,Operation::MutateCharter{..}|Operation::ObserveCharter{..})||matches!(&original.outcome,Outcome::Refused{..}){return Ok(())}
    if matches!(&original.request,Operation::ObserveCharter{observation:CharterObservation::OutputChunk{..},..}){return outputs::validate_original_membership(tx,original,l)}
    let operation_id=&original.commit.operation_id;
    match (&original.request,charter_outcome(original)?) {
        (Operation::ObserveCharter{observation:CharterObservation::BeginSuppliedTurn{..},..},CharterOutcome::SuppliedTurn(record))=>{
            let r=request(tx,operation_id,l)?;validate_request_receipt(tx,&r)?;
            if !matches!(r,RequestRecord::SuppliedTurn(ref v) if v==&record.boundary){return Err(corrupt("supplied turn original membership differs"));}
        }
        (Operation::ObserveCharter{observation:CharterObservation::AckSuppliedTurn{turn_id,..},..},CharterOutcome::SuppliedTurn(record))=>{
            // Compare the original ACK shape, without a later reply/terminal.
            let actual=supplied_turns::acknowledged(tx,turn_id,l)?;
            if actual.acknowledgment.as_ref().map(|v|&v.operation_id)!=Some(operation_id)||actual!=*record{return Err(corrupt("successful supplied turn acknowledgment lacks its exact retained row"));}
            supplied_turns::validate_ack_receipt(tx,turn_id,l)?;
        },
        (Operation::ObserveCharter{observation:CharterObservation::SuppliedReply{turn_id,..},..},CharterOutcome::Reply(record))=>{
            let r=request(tx,turn_id,l)?;let observed=reply(tx,&r,l)?.ok_or_else(||corrupt("supplied reply missing"))?;
            if &observed!=record||observed.reply_id!=*operation_id{return Err(corrupt("supplied reply original membership differs"));}
            validate_reply_receipt(tx,&r,&observed,l)?;
        }
        (Operation::MutateCharter{mutation:CharterMutation::Publish{..}|CharterMutation::SelectForFutureTasks{..},..},CharterOutcome::Definition(_)) => super::charter_definitions::validate_membership(tx,original)?,
        (Operation::MutateCharter{mutation:CharterMutation::SetPending{session_id,..},..},CharterOutcome::Session(_))=>require_version_membership(tx,operation_id,session_id,l)?,
        (Operation::ObserveCharter{observation:CharterObservation::BeginSuppliedContext{session_id,..},..},CharterOutcome::SuppliedContext(_))|
        (Operation::ObserveCharter{observation:CharterObservation::BeginTurn{session_id,..},..},CharterOutcome::Boundary(_))|
        (Operation::ObserveCharter{observation:CharterObservation::BeginCompaction{session_id,..},..},CharterOutcome::Compaction(_))=>{
            let r=request(tx,operation_id,l)?;validate_request_receipt(tx,&r)?;
            require_version_membership(tx,operation_id,session_id,l)?;
        }
        (Operation::ObserveCharter{observation:CharterObservation::AckSuppliedContext{delivery_id,..},..},CharterOutcome::SuppliedContext(record))=>{
            let actual=supplied::get(tx,delivery_id,l)?;
            if actual.acknowledgment.as_ref().map(|v|&v.operation_id)!=Some(operation_id)||actual!=*record{return Err(corrupt("successful supplied context acknowledgment lacks its exact retained row"));}
            supplied::validate_ack_receipt(tx,delivery_id,l)?;
        }
        (Operation::ObserveCharter{observation:CharterObservation::Reply{turn_id,..},..},CharterOutcome::Reply(record))=>{
            let r=request(tx,turn_id,l)?;
            let observed=reply(tx,&r,l)?.ok_or_else(||corrupt("successful charter reply lacks its retained row"))?;
            if observed.reply_id!=*operation_id||observed!=*record{return Err(corrupt("original charter reply membership differs"));}
            validate_reply_receipt(tx,&r,&observed,l)?;
        }
        (Operation::ObserveCharter{observation:CharterObservation::CompleteTurn{turn_id,..},..},CharterOutcome::Turn(record))=>{
            let actual=turn(tx,turn_id,l)?;
            if actual.completed_by.as_ref()!=Some(operation_id)||actual!=*record{return Err(corrupt("successful charter completion lacks its exact closure"));}
            validate_closure_receipt(tx,turn_id,l)?;
        }
        (Operation::ObserveCharter{observation:CharterObservation::CompactionReply{run_id,..},..},CharterOutcome::Compaction(record))=>{
            let r=request(tx,run_id,l)?;let actual=compaction(tx,run_id,l)?;
            let observed=actual.reply.as_ref().ok_or_else(||corrupt("successful compaction reply lacks its retained row"))?;
            if observed.reply_id!=*operation_id||actual!=*record{return Err(corrupt("original compaction result membership differs"));}
            validate_reply_receipt(tx,&r,observed,l)?;require_version_membership(tx,operation_id,r.session(),l)?;
        }
        (Operation::ObserveCharter{observation:CharterObservation::CompactionRejected{run_id,..},..},CharterOutcome::Compaction(record))=>{
            let Some(CompactionResultRecord::Rejected(result))=compaction_result(tx,run_id,l)? else{return Err(corrupt("successful compaction reconciliation lacks its result"))};
            if result.operation_id!=*operation_id||compaction(tx,run_id,l)?!=*record{return Err(corrupt("original rejected compaction membership differs"));}
            validate_compaction_rejection(tx,run_id,l)?;
        }
        _=>return Err(corrupt("successful charter receipt has a mismatched result kind")),
    }Ok(())
}
fn validate_receipt_membership(tx:&StoreContext<'_>,l:Option<&CharterLimits>)->Result<()> {
    let cap=tx.limits.controller.max_receipts.checked_add(tx.limits.controller.max_cleanup_receipts).ok_or_else(||corrupt("receipt inventory bound overflow"))?;
    if bounded_count(tx,"receipts",cap)?>cap{return Err(corrupt("receipt inventory exceeds existing controller bound"));}
    let limit=cap.checked_add(1).and_then(|n|i64::try_from(n).ok()).ok_or_else(||corrupt("receipt inventory SQL bound overflow"))?;
    let mut q=tx.prepare("SELECT CAST(substr(CAST(id AS BLOB),1,?1) AS TEXT) FROM receipts ORDER BY id LIMIT ?2")?;
    let mut rows=q.query(params![super::database::sql_bound(tx.limits.transport.identity_bytes.saturating_add(1))?,limit])?;
    while let Some(row)=rows.next()?{let id:String=row.get(0)?;
        identity(tx,&id).map_err(|_|corrupt("stored receipt identity exceeds bound"))?;
        // receipt() checks request/outcome lengths before metadata::read decodes.
        let original=receipt(tx,&id)?;
        if let Some(l)=l {validate_original_membership(tx,&original,l)?;}
        else {validate_request_vocabulary(tx,&original.request)?;}
    }
    if l.is_some(){super::charter_definitions::validate_selected_heads(tx)?;}
    Ok(())
}
/// Read-only startup verification before migration/new mutation. Every retained
/// table is bounded, indexed bodies are compared, and corruption never repairs.
pub(crate) fn validate_existing(tx:&StoreContext<'_>)->Result<()> {
    let configured=tx.limits.charter.as_ref();
    let Some(l)=configured else {
        if !inventory_fits(tx,0)?{return Err(corrupt("retained charter records require configured reader policy"));}
        if bounded_count(tx,"sessions",tx.limits.controller.max_tasks.checked_add(tx.limits.controller.max_receipts).ok_or_else(||corrupt("session inventory bound overflow"))?)?>tx.limits.controller.max_tasks.checked_add(tx.limits.controller.max_receipts).ok_or_else(||corrupt("session inventory bound overflow"))?{return Err(corrupt("session inventory exceeds existing bound"));}
        let mut q=tx.prepare("SELECT CAST(substr(CAST(id AS BLOB),1,?1) AS TEXT) FROM sessions ORDER BY id LIMIT ?2")?;
        let mut rows=q.query(params![super::database::sql_bound(tx.limits.transport.identity_bytes.saturating_add(1))?,super::database::sql_bound(tx.limits.controller.max_tasks.checked_add(tx.limits.controller.max_receipts).ok_or_else(||corrupt("session inventory bound overflow"))?.saturating_add(1))?])?;
        while let Some(row)=rows.next()?{let id:String=row.get(0)?;if bounded_session(tx,&id)?.definition.charter.is_some(){return Err(corrupt("accepted charter requires reader policy and retained binding"));}}
        // Refused new-vocabulary operations are durable even without policy.
        // Their version barrier is independent of configured charter tables.
        return validate_receipt_membership(tx,None)
    };
    if !inventory_fits(tx,l.records)?{return Err(corrupt("retained charter inventory exceeds configured record bound"));}
    for (table,key) in [("charter_sessions","id"),("charter_requests","id"),("charter_replies","id"),("charter_closures","request_id"),("charter_compaction_results","request_id")] {
        let mut stmt=tx.prepare(&format!("SELECT CAST(substr(CAST({key} AS BLOB),1,?1) AS TEXT) FROM {table} ORDER BY {key} LIMIT ?2"))?;
        let mut rows=stmt.query(params![super::database::sql_bound(tx.limits.transport.identity_bytes.saturating_add(1))?,super::database::sql_bound(l.records.saturating_add(1))?])?;
        while let Some(row)=rows.next()?{let id:String=row.get(0)?;identity(tx,&id).map_err(|_|corrupt("stored charter key exceeds identity bound"))?;
            match table {
                "charter_sessions"=>{require_session(tx,&id,l)?;},
                "charter_requests"=>{let r=request(tx,&id,l)?;validate_request_receipt(tx,&r)?;match r {RequestRecord::Turn(_)=>{turn(tx,&id,l)?;},RequestRecord::Compaction(_)=>{compaction(tx,&id,l)?;},RequestRecord::SuppliedContext(_)=>{supplied::get(tx,&id,l)?;},RequestRecord::SuppliedTurn(_)=>{supplied_turns::query(tx,&id,l)?;}}},
                "charter_replies"=>{let owner:String=tx.query_row("SELECT CAST(substr(CAST(request_id AS BLOB),1,?2) AS TEXT) FROM charter_replies WHERE id=?1",params![id,super::database::sql_bound(tx.limits.transport.identity_bytes.saturating_add(1))?],|r|r.get(0))?;let r=request(tx,&owner,l)?;let v=reply(tx,&r,l)?.ok_or_else(||corrupt("orphan charter reply"))?;validate_reply_receipt(tx,&r,&v,l)?;},
                "charter_closures"=>{if matches!(request(tx,&id,l)?,RequestRecord::SuppliedTurn(_)){supplied_turns::validate_ack_receipt(tx,&id,l)?;}else if matches!(request(tx,&id,l)?,RequestRecord::SuppliedContext(_)){supplied::validate_ack_receipt(tx,&id,l)?;}else{validate_closure_receipt(tx,&id,l)?;}},
                _=>{compaction(tx,&id,l)?;if matches!(compaction_result(tx,&id,l)?,Some(CompactionResultRecord::Rejected(_))){validate_compaction_rejection(tx,&id,l)?;}},
            }
        }
    }
    let mut stmt=tx.prepare("SELECT CAST(substr(CAST(id AS BLOB),1,?1) AS TEXT),revision,CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT),CAST(substr(CAST(operation_id AS BLOB),1,?1) AS TEXT) FROM charter_session_versions ORDER BY id,revision LIMIT ?3")?;
    let mut rows=stmt.query(params![super::database::sql_bound(tx.limits.transport.identity_bytes.saturating_add(1))?,super::database::sql_bound(l.record_bytes.saturating_add(1))?,super::database::sql_bound(l.records.saturating_add(1))?])?;
    let mut previous:Option<CharterSession>=None;
    while let Some(row)=rows.next()?{
        let id:String=row.get(0)?;let rev=unsigned(row.get(1)?)?;let body:String=row.get(2)?;let s:CharterSession=stored(&body,l.record_bytes)?;
        let prior=previous.as_ref().filter(|p|p.session_id==id);
        let expected=match prior{Some(p)=>p.revision.checked_add(1).ok_or_else(||corrupt("charter version exhausted"))?,None=>1};
        if s.session_id!=id||s.revision!=rev||rev!=expected{return Err(corrupt("charter version history/index mismatch"));}
        let current=require_session(tx,&id,l)?;
        if s.initial!=current.initial||rev>current.revision{return Err(corrupt("charter retained initial identity/head differs"));}
        for reference in [&s.initial,&s.effective].into_iter().chain(s.pending.iter()).chain(s.compaction_due.iter()){if resolve(tx,reference,l)?.reference!=*reference{return Err(corrupt("charter historical source is not authoritative"));}}
        if let Some(turn_id)=&s.last_turn_id{if request(tx,turn_id,l)?.session()!=id{return Err(corrupt("charter historical turn owner differs"));}}
        if let Some(run_id)=&s.compaction_in_flight{if request(tx,run_id,l)?.session()!=id{return Err(corrupt("charter historical compaction owner differs"));}}
        let operation_id:Option<String>=row.get(3)?;version_transition(tx,prior,&s,operation_id.as_deref(),l)?;
        previous=Some(s);
    }
    // Detect a missing whole charter head, not just corrupt surviving rows. The
    // existing bounded session reader owns this inventory; charter adds no repair.
    if bounded_count(tx,"sessions",tx.limits.controller.max_tasks.checked_add(tx.limits.controller.max_receipts).ok_or_else(||corrupt("session inventory bound overflow"))?)?>tx.limits.controller.max_tasks.checked_add(tx.limits.controller.max_receipts).ok_or_else(||corrupt("session inventory bound overflow"))?{return Err(corrupt("session inventory exceeds existing controller bound"));}
    let mut stmt=tx.prepare("SELECT CAST(substr(CAST(id AS BLOB),1,?1) AS TEXT) FROM sessions ORDER BY id LIMIT ?2")?;
    let mut rows=stmt.query(params![super::database::sql_bound(tx.limits.transport.identity_bytes.saturating_add(1))?,super::database::sql_bound(tx.limits.controller.max_tasks.checked_add(tx.limits.controller.max_receipts).ok_or_else(||corrupt("session inventory bound overflow"))?.saturating_add(1))?])?;
    while let Some(row)=rows.next()?{let id:String=row.get(0)?;session(tx,&id,l)?;}
    outputs::validate_existing(tx,l)?;
    validate_receipt_membership(tx,Some(l))
}

#[cfg(test)]
#[path="charters_tests.rs"]
mod tests;

#[path="charters_supplied.rs"]
mod supplied;

#[path="charter_outputs.rs"]
mod outputs;

#[path="charters_supplied_turns.rs"]
mod supplied_turns;

/// A new agent session cannot shed unresolved model work from its predecessor.
/// Pending future charter/compaction settings are not in-flight effects.
pub(super) fn handoff_closed(tx:&StoreContext<'_>,session_id:&str)->Result<bool> {
    let Some(limits)=tx.limits.charter.as_ref() else{return Ok(true)};
    let owner=super::sessions::get(tx,session_id)?;
    let Some(state)=session(tx,session_id,limits)? else{return Ok(true)};
    let check=(||->Result<()> {
        if state.compaction_in_flight.is_some(){return refuse("reconciliation_required","original compaction remains unresolved")}
        if owner.definition.executor==ExecutorKind::Coding {
            supplied::closed_session(tx,session_id,&owner.task_id,limits)?;
        } else if let Some(id)=&state.last_turn_id {
            let original=request(tx,id,limits)?;
            if original.session()!=session_id{return Err(corrupt("predecessor turn session differs"))}
            if request_rejection(tx,&original,limits)?.is_none(){
                completed_native_turn(tx,original.caller(),original.index())?;
            }
        }
        Ok(())
    })();
    match check {Ok(())=>Ok(true),Err(Fault::Refused("reconciliation_required",_))=>Ok(false),Err(e)=>Err(e)}
}
