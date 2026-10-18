//! Rendering of the canonical page only; never fetch or concatenate a successor.
use super::*;
use crate::{collaboration::{Query,QueryResult},read_page::{ReadPage,ReadTool,PagedItem,Request,Selection,TaskPart}};

pub(crate) fn render(value:&ReadPage)->UiResponse {
    page(200,"Read content",content(value))
}
fn content(value:&ReadPage)->String {
    let mut body=format!("<p data-read-complete=\"{}\">{}: bytes {}–{} of {}.</p><dl>",value.complete,
        if value.complete {"End of selected content"} else {"Partial content"},value.offset,value.offset+value.returned_bytes,value.total_bytes);
    term(&mut body,"Exact content identity",&serde_json::to_string(&value.identity).unwrap_or_default());
    term(&mut body,"Selected source",&serde_json::to_string(&value.subject).unwrap_or_default());
    term(&mut body,"Encoding",&serde_json::to_string(&value.encoding).unwrap_or_default());
    body.push_str("</dl>");
    if matches!(&value.subject,Selection::Document{version:Some(_),..}) {
        body.push_str("<p>This is a retained historical version. Open the current version to edit.</p>");
    }
    if value.retained_truncation==Some(true) {body.push_str("<p>Earlier output was already truncated; this page covers only retained output.</p>");}
    body.push_str(&format!("<pre data-read-content>{}</pre>",escape(&value.content)));
    if !value.complete {body.push_str("<p>This fragment is not a complete replacement value. No edit form is populated from it.</p>");}
    if let Some(cursor)=&value.next_cursor {
        let url=match &value.subject {
            Selection::Task{task_id,session_id,part}=>format!("{}?part={}&amp;session_id={}&amp;cursor={}",task_path(task_id),part_name(*part),encode(session_id.as_deref().unwrap_or("")),encode(cursor)),
            Selection::Document{document_id,version,part}=>format!("/documents/{}?part={}{}&amp;cursor={}",encode(document_id),if *part==crate::read_page::DocumentPart::Body {"body"} else {"record"},version.map_or(String::new(),|v|format!("&amp;version={v}")),encode(cursor)),
            Selection::Repository{task_id,source,path,start_line,max_lines}=>format!("{}/repository?view=read&amp;source={}&amp;path={}&amp;start_line={start_line}&amp;max_lines={max_lines}&amp;cursor={}",task_path(task_id),match source {crate::repository_view::SourceSelector::Start=>"start",crate::repository_view::SourceSelector::Checkpoint=>"checkpoint",crate::repository_view::SourceSelector::Instructions=>"instructions"},encode(path),encode(cursor)),
            Selection::Public{tool,arguments,caller_task_id:None,..}=>list_url(*tool,arguments).map_or(String::new(),|base|format!("{base}{}cursor={}",if base.contains('?'){"&amp;"}else{"?"},encode(cursor))),
            Selection::Public{..}=>String::new(),
        };
        if !url.is_empty() {body.push_str(&format!("<p><a rel=\"next\" href=\"{url}\">Next content page</a></p>"));}
    }
    if let Selection::Public{tool,arguments,caller_task_id:None,..}=&value.subject {
        if let Some(url)=list_url(*tool,arguments) {
            let after=arguments.get("cursor").and_then(serde_json::Value::as_str);
            body.push_str(&format!("<p><a href=\"{}\">Return to selected list page</a></p>",list_after(&url,*tool,after)));
        }
    }
    body
}
// Only the three existing list domains have browser selectors. A cursor is
// carried through its original domain route, never a general object viewer.
fn list_url(tool:ReadTool,args:&serde_json::Value)->Option<String> {
    Some(match tool {
        ReadTool::Tasks=>format!("/tasks?agent_id={}&amp;parent_task_id={}",encode(args.get("agent_id").and_then(serde_json::Value::as_str).unwrap_or("")),encode(args.get("parent_task_id").and_then(serde_json::Value::as_str).unwrap_or(""))),
        ReadTool::Agents=>"/agents".into(),
        ReadTool::Versions=>format!("/documents/{}/versions",encode(args.get("document_id")?.as_str()?)),
        _=>return None,
    })
}
fn list_after(base:&str,tool:ReadTool,cursor:Option<&str>)->String {
    cursor.map_or_else(||base.to_owned(),|cursor|format!("{base}{}{}={}",if base.contains('?'){"&amp;"}else{"?"},if tool==ReadTool::Agents{"page_cursor"}else{"after"},encode(cursor)))
}
fn rendered_fits(response:UiResponse,limits:&RuntimeLimits)->bool {
    // Use the existing final encoder, including escaped runtime metadata, not
    // a guessed allowance for HTML/bootstrap overhead.
    let response=super::finalize(limits,response,None);
    response.status==200 && response.content_type.starts_with("text/html")
}
pub(crate) fn fits(data:&Data,limits:&RuntimeLimits)->bool {
    match data {
        Data::ReadPage(value)=>rendered_fits(render(value),limits),
        Data::ReadList(list)=>list.items.iter().find_map(|item|match item {
            PagedItem::Partial{page,..}=>Some(list_fits(&page.subject,data,limits)),_=>None,
        }).unwrap_or(false),
        _=>true,
    }
}
pub(crate) fn list_fits(selection:&Selection,data:&Data,limits:&RuntimeLimits)->bool {
    render_list(selection,data).is_some_and(|response|rendered_fits(response,limits))
}
pub(crate) fn render_list(selection:&Selection,data:&Data)->Option<UiResponse> {
    use crate::collaboration::documents::DocumentQueryOutcome;
    let Selection::Public{tool,arguments,caller_task_id:None,..}=selection else{return None};
    let base=list_url(*tool,arguments)?;
    let title=match tool {ReadTool::Tasks=>if arguments.get("agent_id").is_some_and(|v|!v.is_null()) || arguments.get("parent_task_id").is_some_and(|v|!v.is_null()) {"Filtered tasks"}else{"Tasks"},ReadTool::Agents=>"Agents",ReadTool::Versions=>"Document versions",_=>return None};
    if let Data::ReadPage(value)=data {return Some(render(value));}
    if let Data::DocumentQuery(DocumentQueryOutcome::Versions(value))=data {
        if *tool!=ReadTool::Versions{return None;}
        let mut response=super::documents_reviews::versions(arguments.get("document_id")?.as_str()?,value);
        response.body=response.body.replace("</main>",&format!("<p data-list-complete=\"{}\">{}</p></main>",value.next_cursor.is_none(),if value.next_cursor.is_none(){"End of version list."}else{"More versions remain."}));
        return Some(response);
    }
    let mut body=match tool {
        ReadTool::Tasks=>"<p><a href=\"/tasks\">All tasks</a> · <a href=\"/tasks/new\">Create a task</a></p><p>Bounded task summaries. Open a task for its recorded progress and full content. A filtered page can contain no matches while later pages remain.</p>".to_owned(),
        ReadTool::Agents=>"<p><a href=\"/agents\">Refresh agents</a></p>".to_owned(),
        ReadTool::Versions=>format!("<p><a href=\"/documents/{}\">Current document</a></p>",encode(arguments.get("document_id")?.as_str()?)),
        _=>return None,
    };
    let (complete,next)=match data {
        Data::ReadList(list)=>{
            for item in &list.items {
                // The shared first-item fitter currently emits one partial
                // record, never a fabricated complete value or edit source.
                let PagedItem::Partial{content_complete,page}=item else{return None};
                if *content_complete!=page.complete{return None;}
                body.push_str(&content(page));
            }
            (list.complete,list.next_cursor.as_deref())
        },
        Data::CollaborationPage(list) if matches!(tool,ReadTool::Tasks|ReadTool::Agents)=>{
            body.push_str(if *tool==ReadTool::Tasks {"<table><thead><tr><th>Task</th><th>Agent</th><th>Status</th></tr></thead><tbody>"}else{"<table><caption>Agents</caption><thead><tr><th>Agent</th><th>Selected definition for new sessions</th><th>Assigned tasks</th></tr></thead><tbody>"});
            for item in &list.items {
                let id=item.get("id")?.as_str()?;
                if *tool==ReadTool::Tasks {
                    let agent=item.get("assigned_agent_id")?.as_str()?;
                    let status=item.get("status")?.as_str()?;
                    body.push_str(&format!("<tr><td><a href=\"{}\">{}</a></td><td><a href=\"{}\">{}</a></td><td>{}</td></tr>",task_path(id),escape(id),agents::path(agent),escape(agent),escape(status)));
                } else {
                    let agent:AgentSummary=serde_json::from_value(item.clone()).ok()?;
                    if agent.retired{return None;}
                    body.push_str(&format!("<tr><td><a href=\"{}\">{}</a></td><td>{} @ {}</td><td><a href=\"{}\">View assigned tasks</a></td></tr>",agents::path(id),escape(id),escape(&agent.definition.id),escape(&agent.definition.revision),agents::path(id)));
                }
            }
            body.push_str("</tbody></table>");
            if list.items.is_empty(){body.push_str(if *tool==ReadTool::Agents {"<p>No agents found.</p>"}else if title=="Filtered tasks" {"<p>No matching tasks on this page.</p>"}else{"<p>No tasks in this page.</p>"});}
            (list.next_cursor.is_none(),list.next_cursor.as_deref())
        },
        _=>return None,
    };
    body.push_str(&format!("<p data-list-complete=\"{complete}\">{}</p>",if complete{"End of selected list."}else{"More list items remain."}));
    if let Some(cursor)=next {
        let label=match tool {ReadTool::Tasks=>"Next filtered page",ReadTool::Agents=>"Next agent page",_=>"Next page"};
        body.push_str(&format!("<p><a data-list-next href=\"{}\">{label}</a></p>",list_after(&base,*tool,Some(cursor))));
    }
    Some(page(200,title,body))
}
pub(super) fn list_cursor(f:&Form)->Result<Option<String>,UiResponse> {
    let after=f.get("after").filter(|v|!v.is_empty());
    let scoped=f.get("page_cursor").filter(|v|!v.is_empty());
    if after.is_some() && scoped.is_some(){return Err(error(400,"Choose one list cursor"));}
    Ok(scoped.or(after).cloned())
}
pub(super) async fn public_list(controller:&Controller,tool:ReadTool,arguments:serde_json::Value,f:&Form)->Result<UiResponse,UiResponse> {
    let (cursor,max_bytes)=options(f)?;
    let mut selection=Selection::Public{tool,arguments:arguments.clone(),item:false,caller_task_id:None};
    if let Some(raw)=&cursor {
        if list_cursor(f)?.is_some(){return Err(error(400,"Content and list cursors are distinct"));}
        let retained=crate::read_page::Cursor::decode(raw,controller.limits().controller.collaboration_cursor_bytes).map_err(|_|error(400,"Invalid content cursor"))?;
        let Selection::Public{tool:other,arguments:old,item:true,caller_task_id:None}=&retained.selection else{return Err(error(400,"Content cursor is outside this list domain"));};
        let scope=|value:&serde_json::Value|{
            let mut value=value.clone();
            if let Some(object)=value.as_object_mut(){object.remove("cursor");object.remove("limit");}
            value
        };
        if *other!=tool || scope(old)!=scope(&arguments){return Err(error(400,"Content cursor differs from this list scope"));}
        selection=retained.selection;
    }
    let data=boxed_route(||query_data(controller,Request{selection:selection.clone(),cursor,max_bytes})).await?;
    render_list(&selection,&data).ok_or_else(||error(502,"Unexpected bounded list projection"))
}
fn part_name(part:TaskPart)->&'static str {match part {TaskPart::Record=>"record",TaskPart::Instruction=>"instruction",TaskPart::Result=>"result",TaskPart::RecentOutput=>"recent_output"}}
pub(super) fn options(f:&Form)->Result<(Option<String>,Option<usize>),UiResponse> {
    let cursor=f.get("cursor").filter(|v|!v.is_empty()).cloned();
    let max=f.get("max_bytes").map(|v|v.parse::<usize>().ok().filter(|n|*n>0).ok_or_else(||error(400,"Invalid max_bytes"))).transpose()?;
    Ok((cursor,max))
}
pub(super) async fn query(controller:&Controller,request:Request)->Result<ReadPage,UiResponse> {
    match boxed_route(||query_data(controller,request)).await? {Data::ReadPage(page)=>Ok(*page),_=>Err(error(502,"Unexpected read page"))}
}
async fn query_data(controller:&Controller,request:Request)->Result<Data,UiResponse> {
    let reply=boxed_route(||call(controller,Operation::QueryCollaboration {query:Query::Read(request)})).await;
    match reply.outcome {
        Outcome::Ok{data:Data::Collaboration(QueryResult::Read(data))}=>Ok(*data),
        _=>Err(not_task(reply)),
    }
}
pub(super) async fn task(controller:&Controller,path:&str,raw_query:&str)->Option<UiResponse> {
    let tail=path.strip_prefix("/tasks/")?;
    let (raw,sub)=tail.split_once('/').unwrap_or((tail,""));
    if !matches!(sub,""|"instruction"|"result"|"recent-output") || raw=="new" || raw=="committed" {return None;}
    Some(match boxed_route(||task_inner(controller,raw,sub,raw_query)).await {Ok(response)=>response,Err(response)=>response})
}
async fn task_inner(controller:&Controller,raw:&str,sub:&str,raw_query:&str)->Result<UiResponse,UiResponse> {
    let task_id=decode(raw,false).map_err(|e|error(400,&e))?;
    let f=parse_form(raw_query.as_bytes(),controller.limits()).map_err(|e|error(400,&e))?;
    fields_only(&f,&["part","session_id","cursor","max_bytes"]).map_err(|e|error(400,&e))?;
    let default=match sub {"instruction"=>"instruction","result"=>"result","recent-output"=>"recent_output",_=>"record"};
    let part=match f.get("part").map(String::as_str).unwrap_or(default) {"record"=>TaskPart::Record,"instruction"=>TaskPart::Instruction,"result"=>TaskPart::Result,"recent_output"=>TaskPart::RecentOutput,_=>return Err(error(400,"Invalid task part"))};
    let (cursor,max_bytes)=options(&f)?;
    let page=boxed_route(||query(controller,Request {selection:Selection::Task{task_id,session_id:f.get("session_id").cloned(),part},cursor,max_bytes})).await?;
    if part==TaskPart::Record && page.offset==0 && page.complete && f.is_empty() && sub.is_empty() {
        let task:Task=serde_json::from_str(&page.content).map_err(|_|error(502,"Unexpected task projection"))?;
        let full=render_task(&task,controller.limits().workflow.is_some());
        if full.body.len()<=controller.limits().product.response_bytes {return Ok(full);}
    }
    Ok(render(&page))
}
