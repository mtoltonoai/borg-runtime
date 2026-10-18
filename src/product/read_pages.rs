//! Rendering of the canonical page only; never fetch or concatenate a successor.
use super::*;
use crate::{collaboration::{Query,QueryResult},read_page::{ReadPage,Request,Selection,TaskPart}};

pub(crate) fn render(value:&ReadPage)->UiResponse {
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
            Selection::Public{..}=>String::new(),
        };
        if !url.is_empty() {body.push_str(&format!("<p><a rel=\"next\" href=\"{url}\">Next content page</a></p>"));}
    }
    page(200,"Read content",body)
}
fn part_name(part:TaskPart)->&'static str {match part {TaskPart::Record=>"record",TaskPart::Instruction=>"instruction",TaskPart::Result=>"result",TaskPart::RecentOutput=>"recent_output"}}
pub(super) fn options(f:&Form)->Result<(Option<String>,Option<usize>),UiResponse> {
    let cursor=f.get("cursor").filter(|v|!v.is_empty()).cloned();
    let max=f.get("max_bytes").map(|v|v.parse::<usize>().ok().filter(|n|*n>0).ok_or_else(||error(400,"Invalid max_bytes"))).transpose()?;
    Ok((cursor,max))
}
pub(super) async fn query(controller:&Controller,request:Request)->Result<ReadPage,UiResponse> {
    let reply=boxed_route(||call(controller,Operation::QueryCollaboration {query:Query::Read(request)})).await;
    match reply.outcome {
        Outcome::Ok{data:Data::Collaboration(QueryResult::Read(data))}=>match *data {Data::ReadPage(page)=>Ok(*page),_=>Err(error(502,"Unexpected read page"))},
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
