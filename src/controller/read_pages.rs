//! Canonical public selection and one-page fitting. No writes or model calls.
use super::{database::{self, Fault, Result, StoreContext}, validation::Context, ControllerConfig};
use crate::{protocol::*, read_page::*, mcp::Invocation};
use serde_json::{json, Value};
use crate::read_page::Request as ReadRequest;

fn fault(e: Error) -> Fault { Fault::Refused(e.code, e.message) }
fn invalid(message: &str) -> Fault { fault(Error::invalid(message)) }
fn stored(message: impl Into<String>) -> Fault { Fault::Store(message.into()) }

pub(super) struct Bounds<'a> {
    pub limits: &'a crate::limits::RuntimeLimits,
    pub call: Option<&'a Invocation>,
    pub max_bytes: Option<usize>,
    pub workflow: bool,
    pub repository: bool,
    pub browser: bool,
}
impl Bounds<'_> {
    pub fn check(&self, data: &Data) -> std::result::Result<(), Error> {
        let l = self.limits;
        let check = |value: &Data, cap, field| bounded_json_size(value,cap).map(|_|()).map_err(|_|Error::bound(field));
        check(data,l.interface.mcp_data_bytes,"interface.mcp_data_bytes")?;
        check(data,l.controller.collaboration_page_bytes,"controller.collaboration_page_bytes")?;
        if matches!(data,Data::ReadPage(_)) { check(data,l.controller.collaboration_item_bytes,"controller.collaboration_item_bytes")?; }
        if let Data::ReadList(value) = data {
            for item in &value.items {
                bounded_json_size(item,l.controller.collaboration_item_bytes)
                    .map_err(|_|Error::bound("controller.collaboration_item_bytes"))?;
            }
        } else if let Some((items,_)) = list(data) {
            for item in items {
                bounded_json_size(&item,l.controller.collaboration_item_bytes)
                    .map_err(|_|Error::bound("controller.collaboration_item_bytes"))?;
            }
        }
        if self.workflow {
            let p = l.workflow.as_ref().ok_or_else(||Error::invalid("workflow reads are not configured"))?;
            check(data,p.page_bytes,"workflow.page_bytes")?;
        }
        if self.repository { check(data,l.repository.output_bytes,"repository.output_bytes")?; }
        if let Some(max) = self.max_bytes { check(data,max,"requested max_bytes")?; }
        if self.browser && !crate::product::read_pages::fits(data,l) {
            return Err(Error::bound("product.response_bytes"));
        }
        let outcome = Outcome::Ok { data:data.clone() };
        bounded_json_size(&outcome,l.controller.outcome_bytes).map_err(|_|Error::bound("controller.outcome_bytes"))?;
        let id = self.call.map_or("public-read",|call|call.operation_id.as_str());
        let frame = crate::mcp::CallResult { reply:Reply {
            request_id:"\\".repeat(l.transport.identity_bytes),
            commit:Some(MutationCommit {operation_id:id.into(),controller_revision:i64::MAX as u64}),
            observed_controller_revision:Some(i64::MAX as u64), outcome:outcome.clone(),
        },requires_quiescence:false };
        bounded_json_size(&frame,l.execution.collaboration.complete_result_bytes)
            .map_err(|_|Error::bound("execution.collaboration.complete_result_bytes"))?;
        // The MCP text string escapes the complete CallResult a second time.
        // Use its real serializer, including the maximum permitted RPC identity.
        let wire = crate::mcp::tool_result(json!("\\".repeat(l.transport.identity_bytes)), &frame);
        bounded_json_size(&wire,l.transport.body_bytes).map_err(|_|Error::bound("transport.body_bytes"))?;
        if let Some(max)=self.max_bytes {
            bounded_json_size(&wire,max).map_err(|_|Error::bound("requested max_bytes"))?;
        }
        if let Some(call) = self.call {
            super::metadata::receipt_bound(l,&call.operation(),&outcome).map_err(|e|match e {
                Fault::Refused(_,message)=>Error {code:"response_bound",message},
                _=>Error::bound("controller.outcome_bytes"),
            })?;
            super::metadata::complete_call_bound(&l.transport,call,&outcome,l.execution.collaboration.complete_result_bytes)
                .map_err(|_|Error::bound("execution.collaboration.complete_result_bytes"))?;
        }
        Ok(())
    }
}

fn max_bytes(requested: Option<usize>, retained: Option<usize>) -> Result<Option<usize>> {
    if requested == Some(0) { return Err(invalid("max_bytes must be positive")); }
    Ok(match (requested,retained) { (Some(a),Some(b))=>Some(a.min(b)), (a,b)=>a.or(b) })
}
fn compatible(request: &Selection, cursor: &Selection) -> bool {
    match (request,cursor) {
        (Selection::Task{task_id:a,session_id:s,part:p},Selection::Task{task_id:b,session_id:t,part:q}) => a==b && p==q && (s.is_none() || s==t),
        (Selection::Document{document_id:a,version:v,part:p},Selection::Document{document_id:b,version:w,part:q}) => a==b && p==q && (v.is_none() || v==w),
        _ => request==cursor,
    }
}
fn resolve_request(request: &ReadRequest, limits: &crate::limits::RuntimeLimits) -> Result<(Selection,Option<Cursor>,Option<usize>)> {
    let cursor = request.cursor.as_deref().map(|c|Cursor::decode(c,limits.controller.collaboration_cursor_bytes)).transpose().map_err(fault)?;
    if cursor.as_ref().is_some_and(|c|!compatible(&request.selection,&c.selection)) { return Err(invalid("read cursor differs from requested source, part or range")); }
    Ok((cursor.as_ref().map_or_else(||request.selection.clone(),|c|c.selection.clone()),cursor.clone(),max_bytes(request.max_bytes,cursor.as_ref().and_then(|c|c.max_bytes))?))
}

struct Projection { legacy: Data, page: ReadPage, text: String }
fn projection(selection: Selection, source: Source, summary: Summary, encoding: Encoding, text: String, legacy: Data, truncation: Option<bool>) -> Projection {
    let identity = Identity { source,digest:digest(text.as_bytes()) };
    Projection { page:ReadPage { subject:selection, identity, summary,encoding,offset:0,returned_bytes:0,total_bytes:text.len(),content:String::new(),complete:false,next_cursor:None,retained_truncation:truncation },text,legacy }
}
fn task(tx: &StoreContext<'_>, selection: &Selection, cfg: &ControllerConfig, now: u64) -> Result<Projection> {
    let Selection::Task {task_id,session_id,part} = selection else {unreachable!()};
    let stored = database::load(tx,task_id)?;
    let original = super::operations::view(tx,stored,cfg,now)?;
    let (safe,_) = super::collaboration_calls::safe(tx.limits,(Data::Task(Box::new(original)),false))?;
    let Data::Task(current) = &safe else {unreachable!()};
    let session = super::sessions::get(tx,session_id.as_deref().unwrap_or(&current.session_id))?;
    if session.task_id != *task_id { return Err(invalid("session does not belong to the requested task")); }
    let input = super::sessions::input(tx,&session)?;
    if super::credential_admission::rejected(&Operation::SubmitTask {operation_id:"read-projection".into(),input:input.clone()}) {
        return database::refuse("credential_metadata","credential-bearing input is not exposed");
    }
    let input = crate::pull_request_view::input(&input);
    let fixed = Selection::Task {task_id:task_id.clone(),session_id:Some(session.id.clone()),part:*part};
    let (text,encoding,revision,truncated) = match part {
        TaskPart::Record => {
            if session.id != current.session_id { return Err(invalid("task record is a current observation; select instruction for a historical session")); }
            (canonical(current.as_ref()).map_err(stored_error)?,Encoding::CanonicalJsonUtf8,current.revision.to_string(),None)
        }
        TaskPart::Instruction => (input.instruction,Encoding::Utf8,session.id.clone(),None),
        TaskPart::Result => {
            if session.id!=current.session_id {return Err(invalid("result belongs to the current task session"));}
            let result=current.result.as_ref().ok_or_else(||Fault::Refused("not_available","task has no accepted result".into()))?;
            (canonical(result).map_err(stored_error)?,Encoding::CanonicalJsonUtf8,result.attempt_id.clone(),None)
        }
        TaskPart::RecentOutput => {
            if session.id!=current.session_id {return Err(invalid("recent output belongs to the current task session"));}
            (current.recent_output.clone(),Encoding::Utf8,current.revision.to_string(),Some(current.output_truncated))
        },
    };
    let source=Source::Task {task_id:task_id.clone(),session_id:session.id,part:*part,revision};
    let summary=Summary::Task {status:current.status,parts:vec![TaskPart::Record,TaskPart::Instruction,TaskPart::Result,TaskPart::RecentOutput]};
    Ok(projection(fixed,source,summary,encoding,text,safe,truncated))
}
fn stored_error(message: String) -> Fault { stored(message) }
fn document(tx: &StoreContext<'_>, selection: &Selection, now: u64) -> Result<Projection> {
    use crate::collaboration::documents::*;
    let Selection::Document{document_id,version,part}=selection else {unreachable!()};
    let op=Operation::QueryDocument {query:DocumentQuery::Get {document_id:document_id.clone(),version:*version}};
    let (legacy,_) = super::collaboration_routing::direct(tx,&op,now)?;
    let Data::DocumentQuery(DocumentQueryOutcome::Document(view))=&legacy else {return Err(stored("document projection shape"))};
    // A selected version/body excludes the mutable header from its digest.
    let fixed_version = if *part==DocumentPart::Body || version.is_some() {Some(view.version.reference.version)} else {None};
    let fixed=Selection::Document {document_id:document_id.clone(),version:fixed_version,part:*part};
    let (text,encoding)=match part {
        DocumentPart::Body=>(view.version.content.body.clone(),Encoding::Utf8),
        DocumentPart::Record=>(if version.is_some(){canonical(&view.version)}else{canonical(view)}.map_err(stored_error)?,Encoding::CanonicalJsonUtf8),
    };
    let source=Source::Document {version:view.version.reference.clone(),part:*part,record_revision:if fixed_version.is_none(){Some(view.document.revision)}else{None}};
    Ok(projection(fixed,source,Summary::Document{parts:vec![DocumentPart::Record,DocumentPart::Body]},encoding,text,legacy,None))
}

// = The digest is over the selected safe value, never the controller revision.
//= spec/impl/v2/implementation.md#bounded-read-pagination
//# A content continuation MUST remain bound to the exact source content it continues.
fn finish(mut value: Projection, cursor: Option<&Cursor>, bounds: &Bounds<'_>, force: bool) -> Result<Data> {
    if let Some(cursor)=cursor {
        if cursor.identity!=value.page.identity || cursor.encoding!=value.page.encoding || cursor.selection!=value.page.subject { return Err(fault(Error::stale())); }
        if cursor.offset>value.text.len() || !value.text.is_char_boundary(cursor.offset) { return Err(invalid("read offset is outside the selected value or not a UTF-8 boundary")); }
        value.page.offset=cursor.offset;
    } else if !force && bounds.check(&value.legacy).is_ok() { return Ok(value.legacy); }
    let page=crate::read_page::fit(value.page.clone(),&value.text[value.page.offset..],bounds.max_bytes,bounds.limits.controller.collaboration_cursor_bytes,
        |page|bounds.check(&Data::ReadPage(Box::new(page.clone())))).map_err(fault)?;
    Ok(Data::ReadPage(Box::new(page)))
}

/// Operator reads select public records through the same projection. The
/// existing repository operation retains its I/O ticket and separate adapter.
pub(super) fn query(tx:&StoreContext<'_>,request:&ReadRequest,cfg:&ControllerConfig) -> Result<Data> {
    let now=(cfg.clock)();
    let (selection,cursor,max_bytes)=resolve_request(request,tx.limits)?;
    if let Selection::Public{tool,caller_task_id,..}=&selection {
        if caller_task_id.is_some() || !matches!(tool,ReadTool::Tasks|ReadTool::Agents|ReadTool::Versions) {
            return Err(invalid("operator list selector is outside its closed domain"));
        }
        return operator_list(tx,selection,cursor.as_ref(),max_bytes,cfg,now);
    }
    let (value,workflow)=match &selection {
        Selection::Task{..}=>(task(tx,&selection,cfg,now)?,false),
        Selection::Document{..}=>(document(tx,&selection,now)?,true),
        _=>return Err(invalid("this operator read selector requires its original domain route")),
    };
    let bounds=Bounds {limits:tx.limits,call:None,max_bytes,workflow,repository:false,browser:true};
    finish(value,cursor.as_ref(),&bounds,true)
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskListArgs { cursor:Option<String>, limit:usize, agent_id:Option<String>, parent_task_id:Option<String> }
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentListArgs { cursor:Option<String>, limit:usize }
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct VersionListArgs { document_id:String, cursor:Option<String>, limit:usize }
fn list_args<T:serde::de::DeserializeOwned>(args:&Value,keys:&[&str])->Result<T> {
    if args.as_object().is_none_or(|o|o.len()!=keys.len() || keys.iter().any(|k|!o.contains_key(*k))) {
        return Err(invalid("operator list arguments differ from their closed schema"));
    }
    serde_json::from_value(args.clone()).map_err(|_|invalid("operator list arguments differ from their closed schema"))
}
fn operator_list(tx:&StoreContext<'_>,mut selection:Selection,cursor:Option<&Cursor>,max_bytes:Option<usize>,cfg:&ControllerConfig,now:u64)->Result<Data> {
    let Selection::Public{tool,arguments,item,caller_task_id:None}=&mut selection else{return Err(invalid("operator list scope differs"))};
    let tool=*tool;
    if *item && (cursor.is_none() || arguments.get("limit").and_then(Value::as_u64)!=Some(1)) {
        return Err(invalid("item continuation requires its original single-item cursor"));
    }
    // Only the original unfiltered browser routes accepted raw after identities.
    // Convert once before fitting so retained content uses the canonical cursor.
    match tool {
        ReadTool::Tasks=>{
            let a:TaskListArgs=list_args(arguments,&["cursor","limit","agent_id","parent_task_id"])?;
            arguments["cursor"]=json!(super::collaboration_calls::operator_list_cursor(tx,"tasks",&json!([a.agent_id,a.parent_task_id]),&a.cursor,cfg,now)?);
        },
        ReadTool::Agents=>{
            let a:AgentListArgs=list_args(arguments,&["cursor","limit"])?;
            arguments["cursor"]=json!(super::collaboration_calls::operator_list_cursor(tx,"agents",&Value::Null,&a.cursor,cfg,now)?);
        },
        ReadTool::Versions=>{let _:VersionListArgs=list_args(arguments,&["document_id","cursor","limit"])?;},
        _=>return Err(invalid("operator list selector is outside its closed domain")),
    }
    let args=arguments.clone();
    let mut raw_tx=tx.with_limits(tx.limits);raw_tx.fitting_read=true;
    let bounds=Bounds{limits:tx.limits,call:None,max_bytes,workflow:tool==ReadTool::Versions,repository:false,browser:true};
    public_projection(&raw_tx,tool,args,None,selection,cursor,&bounds,|args|match tool {
        ReadTool::Tasks=>{
            let a:TaskListArgs=list_args(args,&["cursor","limit","agent_id","parent_task_id"])?;
            Ok((Data::CollaborationPage(super::collaboration_calls::task_page(&raw_tx,cfg,&a.agent_id,&a.parent_task_id,&a.cursor,a.limit)?),false))
        },
        ReadTool::Agents=>{
            let a:AgentListArgs=list_args(args,&["cursor","limit"])?;
            super::collaboration_calls::agent_page(&raw_tx,&a.cursor,a.limit,cfg,now)
        },
        ReadTool::Versions=>{
            let a:VersionListArgs=list_args(args,&["document_id","cursor","limit"])?;
            super::collaboration_routing::direct(&raw_tx,&Operation::QueryDocument{query:crate::collaboration::documents::DocumentQuery::Versions{document_id:a.document_id,after:a.cursor,limit:a.limit}},now)
        },
        _=>unreachable!(),
    })
}

fn selection(call:&Invocation) -> Result<ReadRequest> {
    let mut args=call.arguments.clone();
    let options=crate::mcp::take_read_options(&call.tool,&mut args).map_err(|e|Fault::Refused("invalid_input",e))?;
    let get=|name:&str|args.get(name).and_then(Value::as_str).map(String::from).ok_or_else(||invalid("missing read identity"));
    let tool=ReadTool::parse(&call.tool).ok_or_else(||invalid("operation is not a permitted read"))?;
    let selected=match tool {
        ReadTool::Task=>Selection::Task {task_id:get("task_id")?,session_id:options.session_id,part:options.task_part.unwrap_or_default()},
        ReadTool::RepositoryRead=>Selection::Repository {
            task_id:call.caller.task_id.clone(), source:serde_json::from_value(args.get("source").cloned().ok_or_else(||invalid("missing repository source"))?).map_err(|_|invalid("invalid repository source"))?,
            path:get("path")?, start_line:args.get("start_line").and_then(Value::as_u64).and_then(|n|usize::try_from(n).ok()).ok_or_else(||invalid("invalid start line"))?,
            max_lines:args.get("max_lines").and_then(Value::as_u64).and_then(|n|usize::try_from(n).ok()).ok_or_else(||invalid("invalid line count"))?,
        },
        ReadTool::Document=>Selection::Document {document_id:get("document_id")?,version:args.get("version").and_then(Value::as_u64),part:options.document_part.unwrap_or_default()},
        _=>Selection::Public {tool,arguments:args,item:false,caller_task_id:Some(call.caller.task_id.clone())},
    };
    Ok(ReadRequest {selection:selected,cursor:options.cursor,max_bytes:options.max_bytes})
}
fn arguments_for(selection:&Selection) -> Result<(ReadTool,Value)> {
    Ok(match selection {
        Selection::Task{task_id,..}=>(ReadTool::Task,json!({"task_id":task_id})),
        Selection::Document{document_id,version,..}=>(ReadTool::Document,json!({"document_id":document_id,"version":version})),
        Selection::Public{tool,arguments,..}=>(*tool,arguments.clone()),
        Selection::Repository{source,path,start_line,max_lines,..}=>(ReadTool::RepositoryRead,json!({"source":source,"path":path,"start_line":start_line,"max_lines":max_lines})),
    })
}
pub(super) fn is_read(name:&str)->bool { name=="collab_read_continue" || ReadTool::parse(name).is_some() }

// One model call yields one normal occurrence and one bounded result. Neither
// executor has a page-fetch loop; its existing original-call lookup stays first.
//= spec/impl/v2/implementation.md#bounded-read-pagination
//# The collaboration tool interface MUST NOT automatically concatenate all read pages into model context.
//= spec/impl/v2/implementation.md#bounded-read-pagination
//# The collaboration tool interface MUST provide the same pagination behavior to both executors.
pub(super) fn agent(tx:&StoreContext<'_>,call:&Invocation,cfg:&ControllerConfig,now:u64,validation:&Context<'_>) -> Result<(Data,bool)> {
    let original=super::collaboration_calls::caller(tx,&call.caller,now)?;
    crate::mcp::validate_arguments_for_task(&call.tool,&call.arguments,tx.limits,original.task.input.definition.execution_profile.as_ref(),original.task.input.instruction_source.as_ref())
        .map_err(|e|Fault::Refused("invalid_input",e))?;
    let request=if call.tool=="collab_read_continue" {
        let raw=call.arguments.get("cursor").and_then(Value::as_str).ok_or_else(||invalid("continuation requires a cursor"))?;
        let c=Cursor::decode(raw,tx.limits.controller.collaboration_cursor_bytes).map_err(fault)?;
        ReadRequest {selection:c.selection,cursor:Some(raw.into()),max_bytes:call.arguments.get("max_bytes").and_then(Value::as_u64).map(|n|usize::try_from(n).map_err(|_|invalid("max_bytes overflow"))).transpose()?}
    } else {selection(call)?};
    let (selection,cursor,max_bytes)=resolve_request(&request,tx.limits)?;
    if let Selection::Public {caller_task_id,..}=&selection {
        if caller_task_id.as_deref()!=Some(call.caller.task_id.as_str()) {return Err(invalid("read continuation caller scope differs"));}
    }
    if let Selection::Repository {task_id,..}=&selection {
        if task_id != &call.caller.task_id { return Err(invalid("repository continuation caller scope differs")); }
    }
    let (tool,args)=arguments_for(&selection)?;
    crate::mcp::validate_arguments_for_task(tool.name(),&args,tx.limits,original.task.input.definition.execution_profile.as_ref(),original.task.input.instruction_source.as_ref())
        .map_err(|e|Fault::Refused("invalid_input",e))?;
    let mut raw_call=call.clone();raw_call.tool=tool.name().into();raw_call.arguments=args;
    let mut raw_tx=tx.with_limits(tx.limits);raw_tx.fitting_read=true;
    let bounds=Bounds {limits:tx.limits,call:Some(call),max_bytes,workflow:crate::mcp::workflow_tool(tool.name()),repository:tool.name().starts_with("collab_repository_") && tool!=ReadTool::Repository,browser:false};
    let force=cursor.is_some() || call.arguments.get("part").is_some() || call.arguments.get("max_bytes").is_some();
    let data=match &selection {
        Selection::Task{part,..}=>finish(task(&raw_tx,&selection,cfg,now)?,cursor.as_ref(),&bounds,force || *part!=TaskPart::Record)?,
        Selection::Document{part,..}=>finish(document(&raw_tx,&selection,now)?,cursor.as_ref(),&bounds,force || *part!=DocumentPart::Record)?,
        _=>public(&raw_tx,&raw_call,selection,cursor.as_ref(),&bounds,cfg,now,validation)?,
    };
    Ok((data,false))
}

fn list(value:&Data) -> Option<(Vec<Value>,Option<String>)> {
    // These are closed adapter frames, not a recursive search of user content.
    use crate::collaboration::{QueryResult as Q,questions::QuestionQueryResult as Questions,documents::{DocumentQueryOutcome as Documents,ReviewQueryOutcome as Reviews},work::WorkQueryResult as Work};
    if !matches!(value,Data::CollaborationPage(_) | Data::RepositoryRegistrations(_)
        | Data::Collaboration(Q::Repositories(_)|Q::Sessions(_)|Q::Tasks(_)|Q::MemoryKeys(_)|Q::Workers(_)|Q::Machines(_)|Q::TaskHistory(_)|Q::TaskAttempts(_)|Q::TaskCheckpoints(_)|Q::ReleaseChecks(_))
        | Data::QuestionQuery(Questions::Questions(_)|Questions::Answers(_)|Questions::Actions(_)|Questions::DecisionQueue(_))
        | Data::DocumentQuery(Documents::Documents(_)|Documents::Versions(_)|Documents::Comments(_))
        | Data::ReviewQuery(Reviews::Reviews(_)) | Data::WorkQuery(Work::Waits(_))) {return None;}
    let value=serde_json::to_value(value).ok()?;
    let outer=value.get("value")?;
    let page=if outer.get("items").is_some(){outer}else{outer.get("value")?};
    let items=page.get("items")?.as_array()?.clone();
    let next=page.get("next_cursor")?.as_str().map(String::from);
    Some((items,next))
}
fn item_source(tool:ReadTool,value:&Value)->Source {
    let key=["id","task_id","question_id","review_id","operation_id","key","repository_id","sequence"].iter()
        .find_map(|key|value.get(*key).and_then(|v|v.as_str().map(String::from).or_else(||v.as_u64().map(|n|n.to_string()))));
    Source::Public {tool,item_id:key,revision:value.get("revision").or_else(||value.get("task_revision")).or_else(||value.get("product_revision")).and_then(Value::as_u64)}
}
fn public(tx:&StoreContext<'_>,call:&Invocation,selection:Selection,cursor:Option<&Cursor>,bounds:&Bounds<'_>,cfg:&ControllerConfig,now:u64,validation:&Context<'_>) -> Result<Data> {
    let (tool,_)=arguments_for(&selection)?;
    if tool==ReadTool::RepositoryRead {return repository_agent(tx,call,&selection,cursor,bounds,cfg,now,validation);}
    let mut raw=call.clone();
    public_projection(tx,tool,raw.arguments.clone(),Some(call.caller.task_id.clone()),selection,cursor,bounds,|args|{
        raw.arguments=args.clone();
        super::collaboration_calls::apply_inner(tx,&raw,cfg,now,validation)
    })
}

// Both entry points use this one fitter. The callback retains its original
// domain/caller checks; it cannot grant authority or request a mutation.
fn public_projection<F>(tx:&StoreContext<'_>,tool:ReadTool,mut arguments:Value,caller_task_id:Option<String>,selection:Selection,cursor:Option<&Cursor>,bounds:&Bounds<'_>,mut read:F)->Result<Data>
where F:FnMut(&Value)->Result<(Data,bool)> {
    loop {
        let (value,changed)=read(&arguments)?;
        if changed {return Err(stored("closed read unexpectedly changed state"));}
        let (value,_)=super::collaboration_calls::safe(tx.limits,(value,false))?;
        let item=matches!(&selection,Selection::Public{item:true,..});
        if !item && cursor.is_none() && bounds.check(&value).is_ok()
            && (!bounds.browser || crate::product::read_pages::list_fits(&selection,&value,tx.limits)) {return Ok(value);}
        if let Some((items,next))=list(&value) {
            if item {
                if items.len()!=1 {return Err(fault(Error::stale()));}
                let source=item_source(tool,&items[0]);
                let text=canonical(&items[0]).map_err(stored_error)?;
                return finish(projection(selection,source,Summary::Public{tool},Encoding::CanonicalJsonUtf8,text,value,None),cursor,bounds,true);
            }
            // Re-evaluate a smaller bounded keyset prefix in this same database
            // observation. Filtered rows still advance under the old semantics.
            // There is no network/model continuation and no page concatenation.
            let n=arguments.get("limit").and_then(Value::as_u64).unwrap_or(1);
            if n>1 {arguments["limit"]=json!(n/2);continue;}
            if items.is_empty() {return Err(fault(bounds.check(&value).err().unwrap_or_else(||Error::bound("product.response_bytes"))));}
            if items.len()!=1 {return Err(stored("one-row query returned multiple items"));}
            let selected=Selection::Public {tool,arguments:arguments.clone(),item:true,caller_task_id:caller_task_id.clone()};
            let source=item_source(tool,&items[0]);
            let text=canonical(&items[0]).map_err(stored_error)?;
            let p=projection(selected,source,Summary::Public{tool},Encoding::CanonicalJsonUtf8,text,value,None);
            let page=crate::read_page::fit(p.page,&p.text,bounds.max_bytes,tx.limits.controller.collaboration_cursor_bytes,|p| {
                bounds.check(&Data::ReadList(ReadList {items:vec![PagedItem::Partial {content_complete:p.complete,page:p.clone()}],complete:next.is_none(),next_cursor:next.clone()}))
            }).map_err(fault)?;
            return Ok(Data::ReadList(ReadList {items:vec![PagedItem::Partial {content_complete:page.complete,page}],complete:next.is_none(),next_cursor:next}));
        }
        let text=canonical(&value).map_err(stored_error)?;
        let source=Source::Public {tool,item_id:None,revision:None};
        return finish(projection(selection,source,Summary::Public{tool},Encoding::CanonicalJsonUtf8,text,value,None),cursor,bounds,true);
    }
}

// Repository slicing is supplied by its owned Git reader. The source selector
// is resolved afresh before I/O; a cursor's commit is never an access ticket.
fn repository_agent(tx:&StoreContext<'_>,call:&Invocation,selection:&Selection,cursor:Option<&Cursor>,bounds:&Bounds<'_>,_cfg:&ControllerConfig,now:u64,validation:&Context<'_>) -> Result<Data> {
    let Selection::Repository {task_id,source,path,start_line,max_lines}=selection else { return Err(invalid("invalid repository selection")); };
    if task_id != &call.caller.task_id { return Err(invalid("repository continuation caller scope differs")); }
    let original=super::collaboration_calls::caller(tx,&call.caller,now)?;
    let output=super::repository_queries::require(tx,validation,&original,*source,None,
        crate::repository_read::Query::ReadFileSlice {path:path.clone(),start_line:*start_line,max_lines:*max_lines,offset:cursor.map_or(0,|c|c.offset)},now)?;
    repository_output(selection,cursor,bounds,&original.task,output,
        cursor.is_some() || call.arguments.get("max_bytes").is_some())
}

pub(super) fn repository_output(selection:&Selection,cursor:Option<&Cursor>,bounds:&Bounds<'_>,task:&Task,output:crate::repository_view::Output,force:bool)->Result<Data> {
    use crate::repository_view::Data as R;
    let Selection::Repository {source,..}=selection else {return Err(invalid("invalid repository selection"));};
    let R::FileSlice {path,object,start_line,max_lines,total_lines,next_line,offset,total_bytes,digest,text}=&output.data else {return Err(stored("repository helper returned a different shape"));};
    let identity=Identity {source:Source::Repository {registration:output.source.registration.clone(),commit:output.source.commit.clone(),blob:object.clone(),
        checkpoint_operation:if *source==crate::repository_view::SourceSelector::Checkpoint {task.selected_checkpoint.as_ref().map(|c|c.operation_id.clone())} else {None}},digest:digest.clone()};
    if cursor.is_some_and(|c|c.selection!=*selection || c.identity!=identity || c.encoding!=Encoding::Utf8 || c.offset!=*offset) {return Err(fault(Error::stale()));}
    let page=ReadPage {subject:selection.clone(),identity,summary:Summary::Repository {start_line:*start_line,max_lines:*max_lines,total_lines:*total_lines,next_line:*next_line},encoding:Encoding::Utf8,
        offset:*offset,returned_bytes:0,total_bytes:*total_bytes,content:String::new(),complete:false,next_cursor:None,retained_truncation:None};
    if !force && *offset==0 && text.len()==*total_bytes {
        let mut legacy=output.clone();
        legacy.data=R::File {path:path.clone(),object:object.clone(),start_line:*start_line,total_lines:*total_lines,text:text.clone(),next_line:*next_line};
        let data=Data::Repository(Box::new(legacy));
        if bounds.check(&data).is_ok() {return Ok(data);}
    }
    let page=crate::read_page::fit(page,text,bounds.max_bytes,bounds.limits.controller.collaboration_cursor_bytes,|page| {
        bounds.check(&Data::ReadPage(Box::new(page.clone())))?;
        let mut outer=output.clone();outer.data=R::ReadPage(Box::new(page.clone()));
        bounded_json_size(&outer,bounds.limits.repository.output_bytes).map_err(|_|Error::bound("repository.output_bytes"))?;
        Ok(())
    }).map_err(fault)?;
    Ok(Data::ReadPage(Box::new(page)))
}

pub(super) fn repository_observe(tx:&StoreContext<'_>,context:&Context<'_>,original:&super::database::StoredTask,request:&crate::repository_view::Request,now:u64)->Result<crate::repository_view::Output> {
    let crate::repository_view::Request::ReadPage {source,path,start_line,max_lines,cursor,max_bytes}=request else {return Err(invalid("expected repository page request"));};
    let request=ReadRequest {selection:Selection::Repository {task_id:original.task.id.clone(),source:*source,path:path.clone(),start_line:*start_line,max_lines:*max_lines},cursor:cursor.clone(),max_bytes:*max_bytes};
    let (selection,cursor,max_bytes)=resolve_request(&request,tx.limits)?;
    let output=super::repository_queries::observe_slice(tx,context,original,*source,
        crate::repository_read::Query::ReadFileSlice {path:path.clone(),start_line:*start_line,max_lines:*max_lines,offset:cursor.as_ref().map_or(0,|c|c.offset)},now)?;
    let bounds=Bounds {limits:tx.limits,call:None,max_bytes,workflow:false,repository:true,browser:true};
    let mut outer=output.clone();
    let Data::ReadPage(page)=repository_output(&selection,cursor.as_ref(),&bounds,&original.task,output,true)? else {return Err(stored("repository page projection shape"));};
    outer.data=crate::repository_view::Data::ReadPage(page);
    Ok(outer)
}
