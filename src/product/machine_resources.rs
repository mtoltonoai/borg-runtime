//! Escaped host measurements from the same typed controller projection as MCP.
use super::*;
use crate::{collaboration::{Query,QueryResult},machine_resources::{View,Metric,Freshness,Role}};

fn status<T>(metric:&Metric<T>)->Option<String> {
    match metric {
        Metric::Available{..}=>None,
        Metric::WarmingUp{reason}=>Some(format!("Warming up · {}",serde_json::to_value(reason).unwrap().as_str().unwrap())),
        Metric::Unavailable{reason}=>Some(format!("Unavailable · {}",serde_json::to_value(reason).unwrap().as_str().unwrap())),
    }
}
fn row(view:&View)->String {
    let freshness=match view.freshness {Freshness::Fresh=>"Fresh",Freshness::Stale=>"Stale · retained sample",Freshness::Unobserved=>"Unobserved"};
    let role=match view.machine.role {Role::Controller=>"controller",Role::Worker=>"worker"};
    let age=view.receipt_age_ms.map(|n|n.to_string()).unwrap_or_default();
    let mut html=format!("<article data-machine-row data-machine-age-ms=\"{age}\" data-machine-stale-after-ms=\"{}\" data-machine-freshness=\"{}\"><h2><a href=\"/machines/{}\">{}</a></h2><p>{} · {role}</p><p data-machine-age-status>{freshness}</p>",
        view.stale_after_ms,if view.freshness==Freshness::Unobserved{"unobserved"}else if view.freshness==Freshness::Stale{"stale"}else{"fresh"},
        encode(&view.machine.machine_id),escape(&view.machine.label),escape(&view.machine.machine_id));
    if let Some(worker)=&view.machine.worker_id {
        let _=write!(html,"<p><a href=\"/workers/{}\">Registered capacity and occupancy</a> are separate from measured host usage and admitted cgroup limits.</p>",encode(worker));
    }
    let Some(sample)=&view.sample else {
        html.push_str("<p>CPU, memory and filesystem usage: unobserved. No current measurement is available.</p></article>");
        return html;
    };
    let _=write!(html,"<p>Producer observed: {} Unix ms · Controller received: {} Unix ms · Receipt age: <span data-machine-age>{age}</span> ms · Stale after: {} ms · Read as of: {} Unix ms</p><p>Sample {} · CPU window: {} ms</p><table><thead><tr><th>Metric</th><th>Used / total</th><th>Available or free</th></tr></thead><tbody>",
        sample.observed_at_unix_ms,view.received_at_unix_ms.map(|n|n.to_string()).unwrap_or_else(||"unobserved".into()),view.stale_after_ms,view.as_of_unix_ms,sample.sequence,
        sample.sample_window_ms.map(|n|n.to_string()).unwrap_or_else(||"unavailable".into()));
    if let Metric::Available{value}=&sample.cpu {
        let _=write!(html,"<tr><th>Host CPU</th><td>{:.3} core-equivalents / {} online logical cores</td><td>{:.2}%</td></tr>",value.used_core_equivalents,value.total_online_logical_cores,value.percent);
    } else {let _=write!(html,"<tr><th>Host CPU</th><td colspan=\"2\">{}</td></tr>",escape(&status(&sample.cpu).unwrap()));}
    if let Metric::Available{value}=&sample.memory {
        let _=write!(html,"<tr><th>Host memory</th><td>{} / {} bytes</td><td>{} available bytes</td></tr>",value.used_bytes,value.total_bytes,value.available_bytes);
    } else {let _=write!(html,"<tr><th>Host memory</th><td colspan=\"2\">{}</td></tr>",escape(&status(&sample.memory).unwrap()));}
    for filesystem in &sample.filesystems {
        let scope=escape(&filesystem.scope_labels.join(", "));
        if let Metric::Available{value}=&filesystem.space {
            let _=write!(html,"<tr><th>Filesystem · {scope}</th><td>{} / {} bytes</td><td>{} service-available bytes; {} free bytes</td></tr>",value.used_bytes,value.total_bytes,value.available_bytes,value.free_bytes);
        } else {let _=write!(html,"<tr><th>Filesystem · {scope}</th><td colspan=\"2\">{}</td></tr>",escape(&status(&filesystem.space).unwrap()));}
    }
    html.push_str("</tbody></table></article>");html
}
pub(super) async fn get(controller:&Controller,path:&str,query:&str)->Option<UiResponse> {
    if path!="/machines" && !path.starts_with("/machines/") {return None;}
    Some(match route(controller,path,query).await {Ok(response)=>response,Err(response)=>response})
}
async fn route(controller:&Controller,path:&str,raw_query:&str)->Result<UiResponse,UiResponse> {
    let fields=parse_form(raw_query.as_bytes(),controller.limits()).map_err(|e|error(400,&e))?;
    let query=if path=="/machines" {
        fields_only(&fields,&["after"]).map_err(|e|error(400,&e))?;
        Query::Machines{after:fields.get("after").cloned(),limit:controller.limits().product.page_items.min(controller.limits().controller.collaboration_page_items)}
    } else {
        fields_only(&fields,&[]).map_err(|e|error(400,&e))?;
        let raw=path.strip_prefix("/machines/").filter(|s|!s.is_empty()&&!s.contains('/')).ok_or_else(||error(404,"Machine not found"))?;
        Query::Machine{machine_id:decode(raw,false).map_err(|e|error(400,&e))?}
    };
    let reply=call(controller,Operation::QueryCollaboration{query}).await;
    let (views,next)=match reply.outcome {
        Outcome::Ok{data:Data::Collaboration(QueryResult::Machines(page))}=>(page.items,page.next_cursor),
        Outcome::Ok{data:Data::Collaboration(QueryResult::Machine(view))}=>(vec![view],None),
        _=>return Err(not_task(reply)),
    };
    let mut content=String::from("<p><a href=\"/machines\">All machines</a> · <a href=\"/workers\">Worker capacity and occupancy</a></p><p>Measured host usage. CPU is core-equivalents and percent over the stated interval; memory and filesystem space are bytes. Shared filesystems appear once with their scope labels. Filesystem space is not directory size or drive health. Capacity, occupancy and admitted cgroup limits have separate meanings.</p><section id=\"machine-resources\" aria-label=\"Machine observations\"><p data-machine-connection role=\"status\">Machine observations as of the last read. Sender time does not control freshness.</p>");
    for view in &views {content.push_str(&row(view));}
    if views.is_empty() {content.push_str("<p>No configured registered machines on this page. An operator host-observation profile is required.</p>");}
    if let Some(next)=next {let _=write!(content,"<p><a href=\"/machines?after={}\">Next page</a></p>",encode(&next));}
    content.push_str("</section>");
    let mut response=page(200,"Machines",content);
    if let Some(interval)=controller.machine_refresh_interval_ms() {
        response.body=response.body.replacen("<main data-live-refresh=\"true\">",&format!("<main data-live-refresh=\"true\" data-machine-refresh-ms=\"{interval}\">"),1);
    }
    Ok(response)
}
