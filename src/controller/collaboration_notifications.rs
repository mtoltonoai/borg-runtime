//! Committed typed events and session delivery in the existing receipt transaction.
//! No own commit, receipt table, agent inbox, notification poll or network access.
use super::{database::*, ControllerConfig};
use super::database::StoreContext as Connection;
use crate::{collaboration::{notifications::*,workflow::*},limits::WorkflowLimits,protocol::*};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize,Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct State {
    acknowledged_cursor: DeliveryCursor,
    pending: Option<DeliveryBatch>,
    last_ack_context: Option<String>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    routing_cursor: Option<EventId>,
    #[serde(default,skip_serializing_if="is_false")]
    routing_turn: bool,
    #[serde(default,skip_serializing_if="Option::is_none")]
    superseded: Option<DeliverySupersession>,
}
fn is_false(value:&bool)->bool{!*value}
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq)]
#[serde(untagged)]
enum Targets { Legacy(Vec<DeliveryScope>), Routed(RouteTargets) }
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq)]
#[serde(deny_unknown_fields)]
struct RouteTargets {
    concrete:Vec<DeliveryScope>, route:Option<RouteClaim>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    retained:Option<RetainedRouteInput>,
}
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq)]
#[serde(deny_unknown_fields)]
struct RetainedRouteInput {
    operation_id:String, scope:DeliveryScope,
    caller:crate::mcp::Caller, context_binding:String,
}
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq)]
#[serde(deny_unknown_fields)]
struct RouteClaim {
    batch_id:String,
    scope:DeliveryScope,
    intent:DeliveryIntent,
    /// ACK retains the original exact input attribution; no new counter.
    acknowledged:bool,
}
impl Targets {
    fn concrete(&self)->&[DeliveryScope]{match self{Self::Legacy(v)=>v,Self::Routed(v)=>&v.concrete}}
    fn claim(&self)->Option<&RouteClaim>{match self{Self::Legacy(_)=>None,Self::Routed(v)=>v.route.as_ref()}}
    fn owed(&self)->bool{matches!(self,Self::Routed(v)if v.route.is_none()&&v.retained.is_none())}
    fn acknowledged_for(&self,scope:&DeliveryScope)->bool {
        match self {
            Self::Legacy(_)=>false,
            Self::Routed(v)=>v.route.as_ref().is_some_and(|claim|claim.acknowledged && &claim.scope==scope)
                ||v.retained.as_ref().is_some_and(|input|&input.scope==scope),
        }
    }
}
fn policy<'a>(tx:&'a Connection<'_>)->Result<&'a WorkflowLimits> {
    tx.limits.workflow.as_ref().ok_or_else(||Fault::Refused("workflow_disabled","workflow policy is absent".into()))
}
fn database_id(tx:&Connection)->Result<String> {
    let value:String=tx.query_row("SELECT value FROM settings WHERE name='database_id'",[],|r|r.get(0))?;
    if value.len()!=32 || !value.bytes().all(|c|c.is_ascii_hexdigit() && !c.is_ascii_uppercase()) {
        return Err(Fault::Store("invalid workflow database identity".into()));
    }
    Ok(value)
}
fn position(tx:&Connection,id:&EventId)->Result<()> {
    if id.database_id!=database_id(tx)? || id.product_revision==0 || id.product_revision>i64::MAX as u64 {
        return refuse("invalid_input","workflow event position differs from database");
    }
    Ok(())
}
fn exists(tx:&Connection,id:&EventId)->Result<()> {
    position(tx,id)?;
    let found:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM workflow_events WHERE product_revision=?1 AND ordinal=?2)",params![integer(id.product_revision)?,i64::from(id.ordinal)],|r|r.get(0))?;
    if !found {return refuse("invalid_input","workflow event position is not committed");}
    Ok(())
}
fn scope(tx:&Connection,s:&Session)->Result<DeliveryScope> {
    let retained=super::sessions::get(tx,&s.id)?;
    if retained!=*s || super::sessions::input(tx,s)?.definition!=s.definition {
        return Err(Fault::Store("workflow session/task scope differs".into()));
    }
    Ok(DeliveryScope{task_id:s.task_id.clone(),session_id:s.id.clone(),agent_id:s.agent_id.clone()})
}
fn initial(tx:&Connection,s:&Session)->Result<State> {
    Ok(State{acknowledged_cursor:DeliveryCursor{database_id:database_id(tx)?,scope:scope(tx,s)?,position:None},pending:None,last_ack_context:None,routing_cursor:None,routing_turn:false,superseded:None})
}
fn head(tx:&Connection)->Result<Option<EventId>> {
    let row:Option<(i64,i64)>=tx.query_row("SELECT product_revision,ordinal FROM workflow_events ORDER BY product_revision DESC,ordinal DESC LIMIT 1",[],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    row.map(|(r,o)|Ok(EventId{database_id:database_id(tx)?,product_revision:unsigned(r)?,ordinal:u32::try_from(o).map_err(|_|Fault::Store("invalid event ordinal".into()))?})).transpose()
}
fn read_event(tx:&Connection,revision:i64,ordinal:i64,body:&str,targets:&str)->Result<(WorkflowActivity,Targets)> {
    let p=policy(tx)?;
    if body.len()>p.event_bytes || targets.len()>p.event_bytes {return Err(Fault::Store("workflow event exceeds bound".into()));}
    let event:WorkflowActivity=decode(body)?;
    let targets:Targets=decode(targets)?;
    if event.id.product_revision!=unsigned(revision)? || i64::from(event.id.ordinal)!=ordinal || event.subject_revision==0 || event.subject_revision>i64::MAX as u64 {
        return Err(Fault::Store("workflow event/index binding differs".into()));
    }
    position(tx,&event.id)?;
    if bounded_json_size(&(&event,&targets),p.event_bytes).is_err() || targets.concrete().len()>p.page_items {
        return Err(Fault::Store("workflow event/target envelope exceeds bound".into()));
    }
    for (i,t) in targets.concrete().iter().enumerate() {
        if !valid_id(tx.limits,&t.task_id)||!valid_id(tx.limits,&t.session_id)||!valid_id(tx.limits,&t.agent_id)||targets.concrete()[..i].contains(t) {
            return Err(Fault::Store("invalid workflow event destination".into()));
        }
    }
    if event.routing.is_some()!=matches!(&targets,Targets::Routed(_)) {return Err(Fault::Store("workflow routing envelope differs from immutable event".into()));}
    if let Some(claim)=targets.claim() {
        let s=super::sessions::get(tx,&claim.scope.session_id)?;
        if scope(tx,&s)?!=claim.scope || !valid_id(tx.limits,&claim.batch_id)
            || !valid_id(tx.limits,&claim.intent.operation_id)||!valid_id(tx.limits,&claim.intent.context_binding) {
            return Err(Fault::Store("workflow route claim binding differs".into()));
        }
        retained_caller(tx,&s,&claim.intent.caller)?;
    }
    if let Targets::Routed(envelope)=&targets {
        if let Some(retained)=&envelope.retained {
            if envelope.route.is_some() || !valid_id(tx.limits,&retained.operation_id)
                || retained_input(tx,&retained.scope,&event.id)?.as_ref()!=Some(&(retained.caller.clone(),retained.context_binding.clone())) {
                return Err(Fault::Store("workflow retained route input proof differs".into()));
            }
        }
    }
    Ok((event,targets))
}
/// Root's common finalizer calls this after canonical resolver/destination checks.
/// The sole receipt owner rolls back this row if any later effect/bound fails.
// Traceability scope: destination admission rejects terminal tasks. Later
// Begin/ACK operations also require a current nonterminal caller; the finalizer
// and wait resolver preserve terminal state before staging these events.
//= spec/impl/v2/implementation.md#committed-resolutions-reach-their-consumers
//# A resolution notice MUST NOT reopen a terminal task.
pub(super) fn stage_event(tx:&Connection,event:&WorkflowActivity,targets:&[DeliveryScope])->Result<()> {
    let p=policy(tx)?;
    position(tx,&event.id)?;
    super::collaboration_workflow::validate_recorded_actor(tx,&event.actor)?;
    let envelope=if event.routing.is_some(){Targets::Routed(RouteTargets{concrete:targets.to_vec(),route:None,retained:None})}else{Targets::Legacy(targets.to_vec())};
    if event.subject_revision==0 || targets.len()>p.page_items || bounded_json_size(&(event,&envelope),p.event_bytes).is_err() {
        return refuse("response_bound","workflow event exceeds selected bound");
    }
    for (i,t) in targets.iter().enumerate() {
        let session=super::sessions::get(tx,&t.session_id)?;
        if &scope(tx,&session)?!=t || targets[..i].contains(t) {return refuse("invalid_input","workflow destination differs");}
        let current=load(tx,&t.task_id)?.task;
        if terminal(current.status)||current.session_id!=t.session_id {return refuse("invalid_input","historical or terminal session cannot receive new workflow execution input");}
    }
    tx.execute("INSERT INTO workflow_events(product_revision,ordinal,body,targets) VALUES(?1,?2,?3,?4)",params![integer(event.id.product_revision)?,i64::from(event.id.ordinal),encode(event)?,encode(&envelope)?])?;
    Ok(())
}
fn limit(tx:&Connection,n:usize)->Result<usize> {
    if n==0 || n>policy(tx)?.page_items {return refuse("invalid_input","workflow page item bound");}
    Ok(n)
}
fn page(tx:&Connection,from:&DeliveryCursor,through:&EventId,n:usize,continuation:Option<&WorkflowContinuation>)->Result<Option<DeliveryPage>> {
    page_window(tx,from,through,through,n,continuation)
}
fn page_fits(page:&DeliveryPage,p:&WorkflowLimits)->bool {
    bounded_json_size(page,p.page_bytes).is_ok() && bounded_json_size(page,p.pending_delivery_bytes).is_ok()
}
// Reconstruct a retained prefix using its original response window. Changing
// `through` while checking a saved page changes its exact encoded byte size.
fn page_window(tx:&Connection,from:&DeliveryCursor,through:&EventId,scan_through:&EventId,n:usize,continuation:Option<&WorkflowContinuation>)->Result<Option<DeliveryPage>> {
    let p=policy(tx)?;
    limit(tx,n)?;exists(tx,through)?;exists(tx,scan_through)?;
    if from.database_id!=through.database_id {return refuse("invalid_input","workflow cursor database differs");}
    if scan_through>through {return refuse("invalid_input","workflow scan exceeds window");}
    if continuation.is_some_and(|c|c.resolved_waits.iter().any(|r|r.event_id>*through)) {return refuse("invalid_input","workflow continuation exceeds delivery window");}
    if let Some(pos)=&from.position {exists(tx,pos)?;if pos>scan_through{return refuse("invalid_input","workflow cursor exceeds window");}}
    if from.position.as_ref()==Some(scan_through) && continuation.is_none(){return Ok(None)}
    let (r,o)=from.position.as_ref().map(|x|(x.product_revision,i64::from(x.ordinal))).unwrap_or((0,-1));
    let sql=format!("SELECT product_revision,ordinal,CAST(substr(CAST(body AS BLOB),1,{probe}) AS TEXT),CAST(substr(CAST(targets AS BLOB),1,{probe}) AS TEXT) FROM workflow_events WHERE (product_revision>?1 OR (product_revision=?1 AND ordinal>?2)) AND (product_revision<?3 OR (product_revision=?3 AND ordinal<=?4)) ORDER BY product_revision,ordinal LIMIT ?5",probe=p.event_bytes.saturating_add(1));
    let mut q=tx.prepare(&sql)?;
    let mut rows=q.query(params![integer(r)?,o,integer(scan_through.product_revision)?,i64::from(scan_through.ordinal),integer(p.delivery_scan_rows as u64)?])?;
    let mut result=DeliveryPage{from_cursor:from.clone(),to_cursor:from.clone(),through:through.clone(),items:vec![],continuation:continuation.cloned(),has_more:from.position.as_ref()!=Some(through),routing:None};
    if !page_fits(&result,p) {return refuse("response_bound","complete workflow continuation and cursor exceed workflow.page_bytes or workflow.pending_delivery_bytes");}
    for _ in 0..p.delivery_scan_rows {
        let Some(row)=rows.next()? else {break};
        let (event,targets)=read_event(tx,row.get(0)?,row.get(1)?,&row.get::<_,String>(2)?,&row.get::<_,String>(3)?)?;
        let represented=continuation.is_some_and(|c|c.resolved_waits.iter().any(|r|r.event_id==event.id));
        let acknowledged=if targets.concrete().contains(&from.scope) {
            targets.acknowledged_for(&from.scope)||super::collaboration_work::acknowledged_event(tx,&from.scope.task_id,&from.scope.session_id,&event.id,p)?
        } else {false};
        let included=targets.concrete().contains(&from.scope) && !represented && !acknowledged;
        if included && result.items.len()==n {break}
        let mut candidate=result.clone();
        candidate.to_cursor.position=Some(event.id.clone());
        if included {candidate.items.push(event);}
        candidate.has_more=candidate.to_cursor.position.as_ref()!=Some(through);
        // Include cursor growth and the final false boolean in the exact bound,
        // including rows represented by the indivisible required continuation.
        if !page_fits(&candidate,p) {break}
        result=candidate;
    }
    if result.to_cursor.position==from.position && continuation.is_none() {return refuse("response_bound","next workflow event exceeds workflow.page_bytes or workflow.pending_delivery_bytes");}
    Ok(Some(result))
}
fn event_row(tx:&Connection,id:&EventId)->Result<(WorkflowActivity,Targets,String)> {
    exists(tx,id)?;
    let probe=policy(tx)?.event_bytes.saturating_add(1);
    let (body,raw):(String,String)=tx.query_row(&format!("SELECT CAST(substr(CAST(body AS BLOB),1,{probe}) AS TEXT),CAST(substr(CAST(targets AS BLOB),1,{probe}) AS TEXT) FROM workflow_events WHERE product_revision=?1 AND ordinal=?2"),params![integer(id.product_revision)?,i64::from(id.ordinal)],|r|Ok((r.get(0)?,r.get(1)?)))?;
    let (event,targets)=read_event(tx,integer(id.product_revision)?,i64::from(id.ordinal),&body,&raw)?;
    Ok((event,targets,raw))
}
fn save_targets(tx:&Connection,event:&WorkflowActivity,targets:&Targets,old:&str)->Result<()> {
    if bounded_json_size(&(event,targets),policy(tx)?.event_bytes).is_err() {return refuse("response_bound","workflow route attribution exceeds workflow.event_bytes");}
    if tx.execute("UPDATE workflow_events SET targets=?3 WHERE product_revision=?1 AND ordinal=?2 AND targets=?4",params![integer(event.id.product_revision)?,i64::from(event.id.ordinal),encode(targets)?,old])?!=1 {
        return Err(Fault::Store("workflow route changed during transaction".into()));
    }
    Ok(())
}
fn route_eligible(tx:&Connection,event:&WorkflowActivity,target:&DeliveryScope,now:u64)->Result<bool> {
    let Some(routing)=&event.routing else{return Ok(false)};
    let q=super::collaboration_workflow::QueryContext{tx,actor:&routing.requested_by,now_ms:now,limits:policy(tx)?};
    Ok(super::collaboration_finalizer::resolution_targets(&q,routing)?.contains(target))
}
fn retained_input(tx:&Connection,target:&DeliveryScope,id:&EventId)->Result<Option<(crate::mcp::Caller,String)>> {
    let task=super::sessions::historical_task(tx,&target.task_id,&target.session_id)?;
    if task.session_id!=target.session_id || task.assigned_agent_id!=target.agent_id {return Err(Fault::Store("retained workflow input scope differs".into()));}
    let accepted=task.input.resolved_limits.as_ref().ok_or_else(||Fault::Store("retained workflow input has no accepted metadata".into()))?;
    let view=tx.with_limits(&accepted.global_metadata);
    super::collaboration_work::acknowledged_event_input(&view,&target.task_id,&target.session_id,id,policy(&view)?)
}
struct Selection { page:DeliveryPage, retained:Vec<(EventId,crate::mcp::Caller,String)> }
fn routing_page(tx:&Connection,st:&State,through:&EventId,n:usize,now:u64)->Result<Option<Selection>> {
    let p=policy(tx)?;limit(tx,n)?;exists(tx,through)?;
    if st.acknowledged_cursor.position.as_ref()>Some(through){return refuse("invalid_input","ordinary cursor exceeds routing window");}
    if let Some(from)=&st.routing_cursor {exists(tx,from)?;if from>through{return refuse("invalid_input","routing cursor exceeds window");}}
    let mut result=DeliveryPage{from_cursor:st.acknowledged_cursor.clone(),to_cursor:st.acknowledged_cursor.clone(),through:through.clone(),items:vec![],continuation:None,has_more:st.acknowledged_cursor.position.as_ref()<Some(through),routing:Some(DeliveryRoutingPage{from_cursor:st.routing_cursor.clone(),to_cursor:None,items:vec![],has_more:false})};
    if !page_fits(&result,p){return refuse("response_bound","routing cursor exceeds workflow.page_bytes or workflow.pending_delivery_bytes");}
    let (r,o)=st.routing_cursor.as_ref().map(|x|(x.product_revision,i64::from(x.ordinal))).unwrap_or((0,-1));
    let mut q=tx.prepare(&format!("SELECT product_revision,ordinal,CAST(substr(CAST(body AS BLOB),1,{probe}) AS TEXT),CAST(substr(CAST(targets AS BLOB),1,{probe}) AS TEXT) FROM workflow_events WHERE (product_revision>?1 OR (product_revision=?1 AND ordinal>?2)) AND (product_revision<?3 OR (product_revision=?3 AND ordinal<=?4)) ORDER BY product_revision,ordinal LIMIT ?5",probe=p.event_bytes.saturating_add(1)))?;
    let mut rows=q.query(params![integer(r)?,o,integer(through.product_revision)?,i64::from(through.ordinal),integer(p.delivery_scan_rows as u64)?])?;
    let mut retained=vec![];let mut scanned=false;
    for _ in 0..p.delivery_scan_rows {
        let Some(row)=rows.next()? else{break};
        let (event,targets)=read_event(tx,row.get(0)?,row.get(1)?,&row.get::<_,String>(2)?,&row.get::<_,String>(3)?)?;
        let eligible=targets.owed()&&route_eligible(tx,&event,&st.acknowledged_cursor.scope,now)?;
        let proof=if eligible{retained_input(tx,&st.acknowledged_cursor.scope,&event.id)?}else{None};
        if eligible && result.routing.as_ref().unwrap().items.len()+retained.len()==n{break}
        let mut candidate=result.clone();let route=candidate.routing.as_mut().unwrap();
        route.to_cursor=(event.id!=*through).then(||event.id.clone());route.has_more=route.to_cursor.is_some();
        if eligible&&proof.is_none(){route.items.push(event.clone());}
        if !page_fits(&candidate,p){if !scanned{return refuse("response_bound","first routed event exceeds workflow.page_bytes or workflow.pending_delivery_bytes");}break}
        if let Some((caller,context))=proof{retained.push((event.id,caller,context));}
        result=candidate;scanned=true;
    }
    let route=result.routing.as_ref().unwrap();
    if !scanned && st.routing_cursor.as_ref()!=Some(through) {return Err(Fault::Store("routing window lost its committed end".into()));}
    if route.items.is_empty()&&retained.is_empty()&&route.to_cursor==st.routing_cursor{return Ok(None)}
    Ok(Some(Selection{page:result,retained}))
}
fn select_page(tx:&Connection,st:&State,through:&EventId,n:usize,continuation:Option<&WorkflowContinuation>,now:u64)->Result<Option<Selection>> {
    // A complete admitted continuation always bypasses lane preference. Each
    // lane is scanned at most once; no catch-up loop or executor turn is added.
    if continuation.is_some(){return Ok(page(tx,&st.acknowledged_cursor,through,n,continuation)?.map(|page|Selection{page,retained:vec![]}))}
    if st.routing_turn {
        if let Some(selected)=routing_page(tx,st,through,n,now)?{return Ok(Some(selected))}
    }
    match page(tx,&st.acknowledged_cursor,through,n,None) {
        Ok(Some(page))=>return Ok(Some(Selection{page,retained:vec![]})),
        Ok(None)=>{},
        Err(error)=>{
            if !st.routing_turn&&matches!(&error,Fault::Refused("response_bound",_)) {
                if let Some(selected)=routing_page(tx,st,through,n,now)?{return Ok(Some(selected))}
            }
            return Err(error);
        },
    }
    if !st.routing_turn{return routing_page(tx,st,through,n,now)}
    Ok(None)
}
fn retain_routes(tx:&Connection,st:&State,selected:&Selection,operation_id:&str,now:u64)->Result<()> {
    for (id,caller,context_binding) in &selected.retained {
        let (event,mut targets,raw)=event_row(tx,id)?;
        if !targets.owed()||!route_eligible(tx,&event,&st.acknowledged_cursor.scope,now)?
            || retained_input(tx,&st.acknowledged_cursor.scope,id)?.as_ref()!=Some(&(caller.clone(),context_binding.clone())) {
            return Err(Fault::Store("workflow retained input changed during preparation".into()));
        }
        let Targets::Routed(envelope)=&mut targets else{unreachable!()};
        envelope.retained=Some(RetainedRouteInput{operation_id:operation_id.into(),scope:st.acknowledged_cursor.scope.clone(),caller:caller.clone(),context_binding:context_binding.clone()});
        save_targets(tx,&event,&targets,&raw)?;
    }
    Ok(())
}
fn input_events(batch:&DeliveryBatch)->std::collections::BTreeSet<EventId> {
    batch.page.items.iter().map(|e|e.id.clone())
        .chain(batch.page.routing.iter().flat_map(|r|r.items.iter().map(|e|e.id.clone())))
        .chain(batch.page.continuation.iter().flat_map(|c|c.resolved_waits.iter().map(|r|r.event_id.clone())))
        .collect()
}
fn begin_claims(tx:&Connection,batch:&DeliveryBatch,intent:&DeliveryIntent,now:u64)->Result<bool> {
    let target=&batch.page.from_cursor.scope;
    let mut changes=vec![];
    for id in input_events(batch) {
        let (event,mut targets,raw)=event_row(tx,&id)?;
        let required=batch.page.routing.as_ref().is_some_and(|r|r.items.iter().any(|e|e.id==id));
        let eligible=targets.owed()&&route_eligible(tx,&event,target,now)?;
        let already_retained=if eligible{retained_input(tx,target,&id)?.is_some()}else{false};
        if required&&(!eligible||already_retained){return Ok(false)}
        if eligible&&!already_retained {
            let Targets::Routed(envelope)=&mut targets else{unreachable!()};
            envelope.route=Some(RouteClaim{batch_id:batch.batch_id.clone(),scope:target.clone(),intent:intent.clone(),acknowledged:false});
            if bounded_json_size(&(&event,&targets),policy(tx)?.event_bytes).is_err(){return refuse("response_bound","workflow Begin attribution exceeds workflow.event_bytes");}
            changes.push((event,targets,raw));
        }
    }
    // All required alternatives were rechecked before any claim. The caller's
    // original receipt transaction commits or rolls back the complete set.
    for (event,targets,raw) in changes{save_targets(tx,&event,&targets,&raw)?;}
    Ok(true)
}
fn acknowledge_routes(tx:&Connection,batch:&DeliveryBatch)->Result<()> {
    let Some(intent)=&batch.delivery else{return Ok(())};
    for id in input_events(batch) {
        let (event,mut targets,raw)=event_row(tx,&id)?;
        let required=batch.page.routing.as_ref().is_some_and(|r|r.items.iter().any(|e|e.id==id));
        let own=targets.claim().is_some_and(|claim|claim.batch_id==batch.batch_id
            && claim.scope==batch.page.from_cursor.scope && claim.intent==*intent && !claim.acknowledged);
        if required&&!own{return Err(Fault::Store("workflow routed ACK has no exact original event claim".into()));}
        if own {
            let Targets::Routed(envelope)=&mut targets else{unreachable!()};
            envelope.route.as_mut().unwrap().acknowledged=true;
            save_targets(tx,&event,&targets,&raw)?;
        }
    }
    Ok(())
}
fn validate_routing_page(tx:&Connection,st:&State,batch:&DeliveryBatch)->Result<()> {
    let route=batch.page.routing.as_ref().ok_or_else(||Fault::Store("routing page absent".into()))?;
    if !batch.page.items.is_empty()||batch.page.continuation.is_some()
        ||batch.page.from_cursor!=batch.page.to_cursor||route.from_cursor!=st.routing_cursor
        ||batch.page.to_cursor.position.as_ref()>Some(&batch.page.through)
        ||batch.page.has_more!=(batch.page.to_cursor.position.as_ref()<Some(&batch.page.through))
        ||route.items.len()>policy(tx)?.page_items||route.has_more!=route.to_cursor.is_some() {
        return Err(Fault::Store("workflow routing page binding differs".into()));
    }
    if let Some(from)=&route.from_cursor {exists(tx,from)?;if from>&batch.page.through{return Err(Fault::Store("routing start exceeds window".into()));}}
    if let Some(to)=&route.to_cursor {
        exists(tx,to)?;
        if Some(to)<=route.from_cursor.as_ref()||to>=&batch.page.through{return Err(Fault::Store("routing end exceeds bounded scan window".into()));}
    }
    let end=route.to_cursor.as_ref().unwrap_or(&batch.page.through);let mut previous=route.from_cursor.as_ref();
    let (r,o)=route.from_cursor.as_ref().map(|v|(v.product_revision,i64::from(v.ordinal))).unwrap_or((0,-1));
    let scanned:i64=tx.query_row("SELECT count(*) FROM (SELECT 1 FROM workflow_events WHERE (product_revision>?1 OR (product_revision=?1 AND ordinal>?2)) AND (product_revision<?3 OR (product_revision=?3 AND ordinal<=?4)) LIMIT ?5)",params![integer(r)?,o,integer(end.product_revision)?,i64::from(end.ordinal),integer(policy(tx)?.delivery_scan_rows.saturating_add(1) as u64)?],|r|r.get(0))?;
    if unsigned(scanned)? > policy(tx)?.delivery_scan_rows as u64{return Err(Fault::Store("retained routing page exceeds bounded scan".into()));}
    for event in &route.items {
        if Some(&event.id)<=previous||&event.id>end||event.routing.is_none(){return Err(Fault::Store("routing events differ from exact ordered window".into()));}
        let (recorded,targets,_)=event_row(tx,&event.id)?;
        if &recorded!=event{return Err(Fault::Store("routing event differs from committed occurrence".into()));}
        if let Some(intent)=&batch.delivery {
            if targets.claim().is_none_or(|claim|claim.batch_id!=batch.batch_id||claim.scope!=batch.page.from_cursor.scope||claim.intent!=*intent||claim.acknowledged) {
                return Err(Fault::Store("pending routing event differs from original Begin claim".into()));
            }
        }
        previous=Some(&event.id);
    }
    Ok(())
}
fn retained_caller(tx:&Connection,s:&Session,caller:&crate::mcp::Caller)->Result<()> {
    caller.validate(&tx.limits.transport).map_err(|_|Fault::Store("workflow retained caller invalid".into()))?;
    let raw:String=tx.query_row(&format!("SELECT CAST(substr(CAST(body AS BLOB),1,{}) AS TEXT) FROM attempts WHERE id=?1 AND task_id=?2",tx.limits.transport.body_bytes.saturating_add(1)),params![caller.attempt_id,s.task_id],|r|r.get(0))?;
    if raw.len()>tx.limits.transport.body_bytes{return Err(Fault::Store("workflow retained attempt exceeds bound".into()));}
    let a:Attempt=decode(&raw)?;
    if a.id!=caller.attempt_id || a.task_id!=caller.task_id || a.task_id!=s.task_id || a.session_id!=s.id || a.generation!=caller.generation || a.worker_id!=caller.worker_id || a.worker_incarnation!=caller.incarnation {
        return Err(Fault::Store("workflow retained caller/attempt differs".into()));
    }
    Ok(())
}
// Traceability scope: exact pending page/caller reconstruction from durable session state.
//= spec/impl/v2/implementation.md#committed-resolutions-reach-their-consumers
//# Pending resolution delivery MUST remain recoverable after a controller restart.
fn state(tx:&Connection,s:&Session)->Result<(State,Option<String>)> {
    let p=policy(tx)?;
    let raw:Option<String>=tx.query_row(&format!("SELECT CAST(substr(CAST(workflow_delivery AS BLOB),1,{}) AS TEXT) FROM sessions WHERE id=?1",p.pending_delivery_bytes.saturating_add(1)),[&s.id],|r|r.get(0))?;
    let st=if let Some(raw)=&raw {if raw.len()>p.pending_delivery_bytes{return Err(Fault::Store("workflow delivery state exceeds bound".into()));}decode::<State>(raw)?}else{initial(tx,s)?};
    let expected=initial(tx,s)?.acknowledged_cursor;
    if st.acknowledged_cursor.database_id!=expected.database_id || st.acknowledged_cursor.scope!=expected.scope {return Err(Fault::Store("workflow delivery state scope differs".into()));}
    if let Some(pos)=&st.acknowledged_cursor.position {exists(tx,pos)?;}
    if let Some(pos)=&st.routing_cursor {exists(tx,pos)?;}
    if st.last_ack_context.as_ref().is_some_and(|v|!valid_id(tx.limits,v)) {return Err(Fault::Store("workflow acknowledged context invalid".into()));}
    if let Some(skipped)=&st.superseded {
        if st.pending.is_some()||!valid_id(tx.limits,&skipped.batch_id)||!valid_id(tx.limits,&skipped.operation_id)||!valid_id(tx.limits,&skipped.context_binding){return Err(Fault::Store("workflow supersession binding differs".into()));}
        retained_caller(tx,s,&skipped.caller)?;
    }
    if let Some(batch)=&st.pending {
        if !valid_id(tx.limits,&batch.batch_id)||batch.page.from_cursor!=st.acknowledged_cursor||batch.prepared_by.task_id!=s.task_id {return Err(Fault::Store("workflow pending binding differs".into()));}
        retained_caller(tx,s,&batch.prepared_by)?;
        exists(tx,&batch.page.through)?;
        if !page_fits(&batch.page,p) {return Err(Fault::Store("workflow pending page exceeds selected bounds".into()));}
        if batch.page.routing.is_some() {
            validate_routing_page(tx,&st,batch)?;
        } else if let Some(end)=batch.page.to_cursor.position.as_ref() {
            let rebuilt=page_window(tx,&st.acknowledged_cursor,&batch.page.through,end,p.page_items,batch.page.continuation.as_ref())?.ok_or_else(||Fault::Store("workflow pending page empty".into()))?;
            if rebuilt!=batch.page {return Err(Fault::Store("workflow pending differs from committed events".into()));}
        } else if st.acknowledged_cursor.position.is_some() || batch.page.to_cursor!=st.acknowledged_cursor
            || !batch.page.items.is_empty() || batch.page.continuation.is_none() || !batch.page.has_more {
            return Err(Fault::Store("workflow pending initial cursor requires a complete continuation without scanned events".into()));
        }
        // A full admitted continuation may fit while the first targeted event
        // does not. Retaining None -> None consumes no event and skips no cursor;
        // the exact original admitted references remain mandatory input below.
        if let Some(continuation)=&batch.page.continuation {
            let admitted=super::collaboration_work::admitted_continuation(tx,&s.task_id,&batch.prepared_by.attempt_id,p)?;
            if admitted.as_ref()!=Some(continuation) {return Err(Fault::Store("pending continuation differs from original admitted input".into()));}
            for resolved in &continuation.resolved_waits {
                exists(tx,&resolved.event_id)?;
                if resolved.event_id>batch.page.through {return Err(Fault::Store("pending continuation exceeds committed delivery window".into()));}
            }
        }
        if let Some(intent)=&batch.delivery {
            if !valid_id(tx.limits,&intent.operation_id)||!valid_id(tx.limits,&intent.context_binding)||intent.caller.task_id!=s.task_id||!batch.page.has_input(){return Err(Fault::Store("workflow append intent binding invalid".into()));}
            retained_caller(tx,s,&intent.caller)?;
        }
    }
    Ok((st,raw))
}
fn save(tx:&Connection,s:&Session,st:&State,old:&Option<String>)->Result<()> {
    if bounded_json_size(st,policy(tx)?.pending_delivery_bytes).is_err(){return refuse("response_bound","workflow delivery state exceeds bound");}
    if tx.execute("UPDATE sessions SET workflow_delivery=?2 WHERE id=?1 AND workflow_delivery IS ?3",params![s.id,encode(st)?,old])?!=1{return Err(Fault::Store("workflow delivery state changed during transaction".into()));}
    Ok(())
}
fn observed(tx:&Connection,st:State,available:Option<DeliveryPage>)->Result<Data> {
    let v=DeliveryObservation{acknowledged_cursor:st.acknowledged_cursor,pending:st.pending,available,routing_cursor:st.routing_cursor,routing_turn:st.routing_turn,superseded:st.superseded};
    bounded_json_size(&v,policy(tx)?.pending_delivery_bytes).map_err(|_|Fault::Refused("response_bound","workflow observation exceeds bound".into()))?;
    Ok(Data::CollaborationDelivery(Box::new(v)))
}
pub(super) fn validate_existing(tx:&Connection,s:&Session)->Result<()> {
    let present:bool=tx.query_row("SELECT workflow_delivery IS NOT NULL FROM sessions WHERE id=?1",[&s.id],|r|r.get(0))?;
    if !present { return Ok(()); }
    let task=load(tx,&s.task_id)?;
    let accepted=task.task.input.resolved_limits.as_ref().ok_or_else(||Fault::Store("workflow state has no accepted metadata".into()))?;
    let view=tx.with_limits(&accepted.global_metadata);
    state(&view,s).map(|_|())
}
// First-close events retain the original answer/action occurrence even after
// later comments, answer replacement, withdrawal or moot change the head.
fn validate_question_resolution(tx:&Connection,event:&WorkflowActivity,question:&crate::collaboration::questions::Question,resolution:&QuestionResolution)->Result<()> {
    use crate::collaboration::questions::{QuestionAction,QuestionMutation,QuestionStatus,RecordedQuestionAction};
    let fail=||Fault::Store("workflow question closure differs from its original immutable occurrence".into());
    let p=policy(tx)?;
    let (operation_id,answer,action)=match resolution {
        QuestionResolution::Answered{question_id,answer,selected_option_id}=>{
            if question_id!=&question.id||answer.question_id!=question.id{return Err(fail())}
            let original=super::collaboration_questions::get_answer(tx,answer,p.record_bytes)?;
            if original.replaces.is_some()||original.product_revision!=event.id.product_revision
                ||original.author!=event.actor||original.answer.option_id!=*selected_option_id{return Err(fail())}
            (original.operation_id.clone(),Some(original),None)
        },
        QuestionResolution::Cancelled{question_id,..}|QuestionResolution::Superseded{question_id,..}=>{
            if question_id!=&question.id{return Err(fail())}
            if matches!(resolution,QuestionResolution::Cancelled{action_revision,..}if *action_revision!=event.subject_revision){return Err(fail())}
            let row:Option<(i64,String)>=tx.query_row(
                "SELECT product_revision,CAST(substr(CAST(body AS BLOB),1,?3) AS TEXT) FROM workflow_question_actions WHERE question_id=?1 AND question_revision=?2",
                params![question.id,integer(event.subject_revision)?,super::database::sql_bound(p.record_bytes.saturating_add(1))?],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
            let (product,raw)=row.ok_or_else(fail)?;
            if raw.len()>p.record_bytes{return Err(fail())}
            let original:RecordedQuestionAction=decode(&raw)?;
            if encode(&original)?!=raw||original.question_id!=question.id||original.question_revision!=event.subject_revision
                ||original.product_revision!=unsigned(product)?||original.product_revision!=event.id.product_revision
                ||original.actor!=event.actor{return Err(fail())}
            let exact=match (&original.action,resolution) {
                (QuestionAction::Cancelled{reason},QuestionResolution::Cancelled{reason:actual,..})=>reason==actual,
                (QuestionAction::Superseded{replacement_question_id,reason},QuestionResolution::Superseded{replacement_question_id:actual,reason:actual_reason,..})=>replacement_question_id==actual&&reason==actual_reason,
                _=>false,
            };
            if !exact{return Err(fail())}
            (original.operation_id.clone(),None,Some(original))
        },
    };
    if !valid_id(tx.limits,&operation_id){return Err(fail())}
    // RecordedAnswer has no question revision: the original retained receipt
    // binds that revision without deriving it from a later mutable head.
    let row:Option<(String,String)>=tx.query_row(
        "SELECT CAST(substr(CAST(request AS BLOB),1,?2) AS TEXT),CAST(substr(CAST(outcome AS BLOB),1,?3) AS TEXT) FROM receipts WHERE id=?1",
        params![operation_id,super::database::sql_bound(tx.limits.transport.body_bytes.saturating_add(1))?,super::database::sql_bound(tx.limits.controller.outcome_bytes.saturating_add(1))?],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let (request,raw)=row.ok_or_else(fail)?;
    if request.len()>tx.limits.transport.body_bytes||raw.len()>tx.limits.controller.outcome_bytes{return Err(fail())}
    let operation:Operation=decode(&request)?;let outcome:Outcome=decode(&raw)?;
    if operation.mutation_id()!=Some(operation_id.as_str())||encode(&operation)?!=request||encode(&outcome)?!=raw{return Err(fail())}
    let mutation=match operation {
        Operation::MutateQuestion{mutation,..}=>mutation,
        Operation::CallCollaboration{call}=>match crate::mcp::questions::parse(&call.tool,&call.arguments,&event.actor).map_err(|_|fail())? {
            crate::mcp::questions::Command::Mutation(mutation)=>mutation,
            _=>return Err(fail()),
        },
        _=>return Err(fail()),
    };
    let expected=event.subject_revision.checked_sub(1).ok_or_else(fail)?;
    if mutation.expected_record()!=Some((question.id.as_str(),expected)){return Err(fail())}
    let exact=match (&mutation,resolution) {
        (QuestionMutation::Answer{answer:original,..},QuestionResolution::Answered{..})=>answer.as_ref().is_some_and(|a|a.answer==*original),
        (QuestionMutation::Cancel{reason,..},QuestionResolution::Cancelled{reason:actual,..})=>reason==actual,
        (QuestionMutation::Supersede{reason,..},QuestionResolution::Superseded{reason:actual,..})=>reason==actual,
        _=>false,
    };
    let Outcome::Ok{data:Data::QuestionMutation(changed)}=outcome else{return Err(fail())};
    let closed=&changed.question.question;
    if !exact||closed.id!=question.id||closed.revision!=event.subject_revision||closed.changed_product_revision!=event.id.product_revision
        ||closed.posed!=question.posed||closed.poser!=question.poser||changed.new_answer!=answer||changed.action!=action{return Err(fail())}
    let exact=match resolution {
        QuestionResolution::Answered{answer,..}=>closed.status==QuestionStatus::Answered&&closed.latest_answer_id.as_deref()==Some(answer.answer_id.as_str())
            &&closed.active_answer_id==closed.latest_answer_id&&changed.question.active_answer==changed.new_answer&&changed.replacement.is_none(),
        QuestionResolution::Cancelled{..}=>closed.status==QuestionStatus::Cancelled&&changed.replacement.is_none(),
        QuestionResolution::Superseded{replacement_question_id,..}=>closed.status==QuestionStatus::Superseded&&closed.superseded_by.as_ref()==Some(replacement_question_id)
            &&changed.replacement.as_ref().is_some_and(|q|q.question.id==*replacement_question_id),
    };
    if !exact{return Err(fail())}
    Ok(())
}
fn validate_review_resolution(tx:&Connection,event:&WorkflowActivity,review:&crate::collaboration::documents::Review,resolution:&ReviewResolution)->Result<()> {
    use crate::collaboration::documents::{ReviewMutation,ReviewMutationOutcome};
    let fail=||Fault::Store("workflow review closure differs from its original immutable occurrence".into());
    let q=super::collaboration_workflow::QueryContext{tx,actor:&event.actor,now_ms:0,limits:policy(tx)?};
    let view=super::collaboration_reviews::review_view(&q,review.clone())?;
    let operation_id=match resolution {
        ReviewResolution::Decided{review_id,decision_id,target,verdict}=>{
            let original=view.decision.as_ref().ok_or_else(fail)?;
            if review_id!=&review.id||decision_id!=&original.id||target!=&original.target||verdict!=&original.verdict
                ||original.reviewer!=event.actor||original.product_revision!=event.id.product_revision{return Err(fail())}
            &original.operation_id
        },
        ReviewResolution::Withdrawn{review_id,target,action_revision,reason}=>{
            let original=view.withdrawal.as_ref().ok_or_else(fail)?;
            if review_id!=&review.id||target!=&original.target||*action_revision!=event.subject_revision||reason!=&original.reason
                ||original.actor!=event.actor||original.product_revision!=event.id.product_revision{return Err(fail())}
            &original.operation_id
        },
    };
    // Comments can advance the review head after closure. Its original receipt
    // supplies the exact closed revision alongside the immutable outcome row.
    let row:Option<(String,String)>=tx.query_row(
        "SELECT CAST(substr(CAST(request AS BLOB),1,?2) AS TEXT),CAST(substr(CAST(outcome AS BLOB),1,?3) AS TEXT) FROM receipts WHERE id=?1",
        params![operation_id,super::database::sql_bound(tx.limits.transport.body_bytes.saturating_add(1))?,super::database::sql_bound(tx.limits.controller.outcome_bytes.saturating_add(1))?],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let (request,raw)=row.ok_or_else(fail)?;
    if request.len()>tx.limits.transport.body_bytes||raw.len()>tx.limits.controller.outcome_bytes{return Err(fail())}
    let operation:Operation=decode(&request)?;let outcome:Outcome=decode(&raw)?;
    if operation.mutation_id()!=Some(operation_id.as_str()){return Err(fail())}
    let mutation=match operation {
        Operation::MutateReview{mutation,..}=>mutation,
        Operation::CallCollaboration{call}=>match super::collaboration_document_tools::operation(&call).map_err(|_|fail())? {
            Some(Operation::MutateReview{mutation,..})=>mutation,
            _=>return Err(fail()),
        },
        _=>return Err(fail()),
    };
    let exact=match (&mutation,resolution) {
        (ReviewMutation::Decide{review_id,expected_revision,verdict,explanation},ReviewResolution::Decided{..})=>review_id==&review.id
            &&expected_revision.checked_add(1)==Some(event.subject_revision)
            &&view.decision.as_ref().is_some_and(|d|&d.verdict==verdict && &d.explanation==explanation),
        (ReviewMutation::Withdraw{review_id,expected_revision,reason},ReviewResolution::Withdrawn{..})=>review_id==&review.id
            &&expected_revision.checked_add(1)==Some(event.subject_revision)&&view.withdrawal.as_ref().is_some_and(|w|&w.reason==reason),
        _=>false,
    };
    let Outcome::Ok{data:Data::ReviewMutation(changed)}=outcome else{return Err(fail())};
    let ReviewMutationOutcome::Review(changed)=*changed;
    let closed=&changed.review;
    if !exact||closed.id!=review.id||closed.revision!=event.subject_revision||closed.changed_product_revision!=event.id.product_revision
        ||closed.target!=review.target||closed.reviewer!=review.reviewer||closed.requested_by!=review.requested_by
        ||closed.summary!=review.summary||closed.created_product_revision!=review.created_product_revision
        ||closed.status!=review.status||closed.decision_id!=review.decision_id
        ||changed.decision!=view.decision||changed.withdrawal!=view.withdrawal{return Err(fail())}
    Ok(())
}
// Read the original receipt through bounded projections. Startup validates
// historical occurrences; it never re-enters current-caller authorization.
fn event_receipt(tx:&Connection,operation_id:&str)->Result<(Operation,Outcome)> {
    let fail=||Fault::Store("workflow event lacks its exact original receipt".into());
    let row:Option<(String,String)>=tx.query_row(
        "SELECT CAST(substr(CAST(request AS BLOB),1,?2) AS TEXT),CAST(substr(CAST(outcome AS BLOB),1,?3) AS TEXT) FROM receipts WHERE id=?1",
        params![operation_id,super::database::sql_bound(tx.limits.transport.body_bytes.saturating_add(1))?,super::database::sql_bound(tx.limits.controller.outcome_bytes.saturating_add(1))?],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let (request,raw)=row.ok_or_else(fail)?;
    if request.len()>tx.limits.transport.body_bytes||raw.len()>tx.limits.controller.outcome_bytes{return Err(fail())}
    let operation:Operation=decode(&request)?;let outcome:Outcome=decode(&raw)?;
    if operation.mutation_id()!=Some(operation_id)||encode(&operation)?!=request||encode(&outcome)?!=raw{return Err(fail())}
    Ok((operation,outcome))
}
fn event_actor_matches_operation(event:&WorkflowActivity,operation:&Operation)->bool {
    match operation {
        Operation::CallCollaboration{call}=>match &event.actor.provenance {
            ActorProvenance::BoundAgentSession{task_id,attempt_id,generation,worker_id,incarnation,..}=>
                task_id==&call.caller.task_id&&attempt_id==&call.caller.attempt_id&&*generation==call.caller.generation
                    &&worker_id==&call.caller.worker_id&&incarnation==&call.caller.incarnation,
            _=>false,
        },
        Operation::MutateDocument{..}|Operation::MutateWork{..}=>event.actor.principal==PrincipalRef::TrustedHostOperator
            &&matches!(&event.actor.provenance,ActorProvenance::TrustedHost{..}),
        _=>false,
    }
}
fn validate_comment_event(tx:&Connection,event:&WorkflowActivity,comment_id:&str,body:&BoundedText)->Result<()> {
    use crate::collaboration::documents::{DocumentMutation,DocumentMutationOutcome,RecordedComment};
    use super::collaboration_workflow as workflow;
    let fail=||Fault::Store("workflow comment event differs from its original immutable occurrence".into());
    if !valid_id(tx.limits,comment_id){return Err(fail())}
    let p=policy(tx)?;
    let row:Option<(String,String,i64,String)>=tx.query_row(
        "SELECT CAST(substr(CAST(record_kind AS BLOB),1,16) AS TEXT),CAST(substr(CAST(record_id AS BLOB),1,?3) AS TEXT),created_product_revision,CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM workflow_comments WHERE id=?1",
        params![comment_id,super::database::sql_bound(p.record_bytes.saturating_add(1))?,super::database::sql_bound(tx.limits.transport.identity_bytes.saturating_add(1))?],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
    let (kind,id,revision,raw)=row.ok_or_else(fail)?;
    if raw.len()>p.record_bytes{return Err(fail())}
    let comment:RecordedComment=decode(&raw)?;
    let parts=super::collaboration_documents::record_parts(&event.subject);
    if encode(&comment)?!=raw||comment.id!=comment_id||comment.record!=event.subject||comment.author!=event.actor
        ||comment.product_revision!=event.id.product_revision||unsigned(revision)?!=comment.product_revision
        ||kind!=parts.0||id!=parts.1{return Err(fail())}
    let (operation,outcome)=event_receipt(tx,&comment.operation_id)?;
    if !event_actor_matches_operation(event,&operation){return Err(fail())}
    let mutation=match operation {
        Operation::MutateDocument{mutation,..}=>mutation,
        Operation::CallCollaboration{call}=>match super::collaboration_document_tools::operation(&call).map_err(|_|fail())? {
            Some(Operation::MutateDocument{mutation,..})=>mutation,_=>return Err(fail()),
        },
        _=>return Err(fail()),
    };
    let DocumentMutation::PostComment{record,expected_revision,body:original_body}=mutation else{return Err(fail())};
    let Outcome::Ok{data:Data::DocumentMutation(DocumentMutationOutcome::Comment{comment:recorded,record_revision})}=outcome else{return Err(fail())};
    //= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
    //# Corrupt stored task state MUST be refused without rewriting its data.
    if record!=event.subject||original_body!=comment.body||expected_revision.checked_add(1)!=Some(event.subject_revision)
        ||recorded!=comment||record_revision!=event.subject_revision
        ||workflow::record_owner_revision(tx,&event.subject)?.revision<event.subject_revision
        ||workflow::record_changed_product_revision(tx,&event.subject)?<event.id.product_revision{return Err(fail())}
    // The original policy may differ from today's limit. A retained projection
    // must remain exactly the whole body or its marked proper UTF-8 prefix.
    let exact_text=if body.truncated {
        !body.text.is_empty()&&body.text.len()<comment.body.len()&&comment.body.starts_with(&body.text)
    }else{body.text==comment.body};
    if !exact_text||event.truncated!=body.truncated{return Err(fail())}
    Ok(())
}
fn validate_task_owner_event(tx:&Connection,event:&WorkflowActivity,task_id:&str,previous:&PrincipalRef,current:&PrincipalRef,
    owners:&mut std::collections::BTreeMap<String,(PrincipalRef,u64)>)->Result<()> {
    use crate::collaboration::work::WorkMutation;
    use super::collaboration_workflow as workflow;
    let fail=||Fault::Store("workflow task owner event differs from its original coordination occurrence".into());
    // One top-level mutation owns each product revision. Select at most two
    // bounded receipt IDs, rejecting an absent or ambiguous original outcome.
    let mut query=tx.prepare("SELECT CAST(substr(CAST(id AS BLOB),1,?2) AS TEXT) FROM (SELECT id,CAST(substr(CAST(outcome AS BLOB),1,?3) AS TEXT) AS raw FROM receipts) WHERE CASE WHEN json_valid(raw) THEN json_extract(raw,'$.status')='ok' AND json_extract(raw,'$.data.kind')='work_mutation' AND json_extract(raw,'$.data.value.product_revision')=?1 ELSE 0 END ORDER BY id LIMIT 2")?;
    let mut rows=query.query(params![integer(event.id.product_revision)?,super::database::sql_bound(tx.limits.transport.identity_bytes.saturating_add(1))?,super::database::sql_bound(tx.limits.controller.outcome_bytes.saturating_add(1))?])?;
    let operation_id:String=rows.next()?.ok_or_else(fail)?.get(0)?;
    if !valid_id(tx.limits,&operation_id)||rows.next()?.is_some(){return Err(fail())}
    let (operation,outcome)=event_receipt(tx,&operation_id)?;
    if !event_actor_matches_operation(event,&operation){return Err(fail())}
    let mutation=match operation {
        Operation::MutateWork{mutation,..}=>mutation,
        Operation::CallCollaboration{call}=>match crate::mcp::work::parse(&call.tool,&call.arguments).map_err(|_|fail())? {
            crate::mcp::work::Command::Mutation(mutation)=>mutation,_=>return Err(fail()),
        },
        _=>return Err(fail()),
    };
    let WorkMutation::ChangeOwner{task_id:record,expected_revision,owner}=mutation else{return Err(fail())};
    let Outcome::Ok{data:Data::WorkMutation(changed)}=outcome else{return Err(fail())};
    let baseline=PrincipalRef::Agent{agent_id:changed.view.task.assigned_agent_id.clone()};
    let (last_owner,last_revision)=owners.get(task_id).map(|(owner,revision)|(owner,*revision)).unwrap_or((&baseline,0));
    let head=workflow::record_owner_revision(tx,&event.subject)?;
    //= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
    //# Corrupt stored task state MUST be refused without rewriting its data.
    if event.truncated||record!=task_id||owner!=*current||previous==current||previous!=last_owner||event.subject_revision<=last_revision
        ||expected_revision.checked_add(1)!=Some(event.subject_revision)||changed.product_revision!=event.id.product_revision
        ||changed.changed_wait.is_some()||changed.view.task.id!=task_id||changed.view.coordination_revision!=event.subject_revision
        ||changed.view.responsible_owner!=*current||head.revision<event.subject_revision
        ||workflow::record_changed_product_revision(tx,&event.subject)?<event.id.product_revision
        ||(event.actor.principal!=PrincipalRef::TrustedHostOperator&&event.actor.principal!=*previous){return Err(fail())}
    workflow::validate_direct_principal(tx,previous)?;workflow::validate_direct_principal(tx,current)?;
    owners.insert(task_id.into(),(current.clone(),event.subject_revision));
    Ok(())
}

fn validate_event_record(tx:&Connection,event:&WorkflowActivity,owners:&mut std::collections::BTreeMap<String,(PrincipalRef,u64)>)->Result<()> {
    use super::collaboration_workflow as workflow;
    workflow::validate_recorded_actor(tx,&event.actor)?;
    let _=workflow::record_owner(tx,&event.subject)?;
    let mut expected_routing=None;
    let same_subject=match (&event.subject,&event.detail) {
        (_,ActivityDetail::Document(DocumentChange::CommentAdded{comment_id,body}))=>{
            validate_comment_event(tx,event,comment_id,body)?;true
        },
        (RecordRef::Task{task_id},ActivityDetail::Task(TaskChange::OwnerChanged{previous,current}))=>{
            validate_task_owner_event(tx,event,task_id,previous,current,owners)?;true
        },
        (RecordRef::Task{task_id},ActivityDetail::Task(TaskChange::Closed{resolution}))=>{
            let task=load(tx,task_id)?.task;
            super::collaboration_finalizer::validate_task_resolution(tx,resolution)?;
            expected_routing=Some(ResolutionRouting{requested_by:event.actor.clone(),linked_record:event.subject.clone()});
            resolution.task_id==*task_id&&resolution.terminal_task_revision==event.subject_revision&&task.revision>=event.subject_revision
        },
        (RecordRef::Task{task_id},ActivityDetail::Wait(change))=>{
            let wait=match change{WaitChange::Added{wait,..}|WaitChange::Removed{wait,..}|WaitChange::Resolved{wait,..}=>wait};
            wait.task_id==*task_id&&valid_id(tx.limits,&wait.wait_id)
                &&workflow::record_owner_revision(tx,&event.subject)?.revision>=event.subject_revision
        },
        (RecordRef::Question{question_id},ActivityDetail::Question(change))=>{
            let question=super::collaboration_questions::get(tx,question_id,policy(tx)?.record_bytes)?;
            if question.revision<event.subject_revision{return Err(Fault::Store("workflow event exceeds question revision".into()));}
            match change {
                QuestionChange::Posed{linked_record,recipient,..}=>question.posed.record==*linked_record&&question.posed.recipient==*recipient,
                QuestionChange::Closed{resolution}=>{
                    validate_question_resolution(tx,event,&question,resolution)?;
                    expected_routing=Some(ResolutionRouting{requested_by:question.poser,linked_record:question.posed.record});
                    match resolution {
                        QuestionResolution::Answered{question_id:id,answer,..}=>id==question_id&&answer.question_id==*question_id,
                        QuestionResolution::Cancelled{question_id:id,..}|QuestionResolution::Superseded{question_id:id,..}=>id==question_id,
                    }
                },
                QuestionChange::AnswerReplaced{previous,current,..}=>previous.question_id==*question_id&&current.question_id==*question_id,
                QuestionChange::AnswerWithdrawn{answer,..}|QuestionChange::MarkedMoot{answer,..}=>answer.question_id==*question_id,
            }
        },
        (RecordRef::Document{document_id},ActivityDetail::Document(change))=>{
            if workflow::record_owner_revision(tx,&event.subject)?.revision<event.subject_revision{return Err(Fault::Store("workflow event exceeds document revision".into()));}
            match change {
                DocumentChange::Created{version,..}=>version.document_id==*document_id,
                DocumentChange::VersionAdded{previous,version,..}=>previous.document_id==*document_id&&version.document_id==*document_id,
                DocumentChange::OwnerChanged{previous,current}=>{workflow::validate_direct_principal(tx,previous)?;workflow::validate_direct_principal(tx,current)?;true},
                DocumentChange::CommentAdded{..}=>unreachable!("comment subjects are validated above"),
            }
        },
        (RecordRef::Review{review_id},ActivityDetail::Review(change))=>{
            let q=workflow::QueryContext{tx,actor:&event.actor,now_ms:0,limits:policy(tx)?};
            let review=super::collaboration_reviews::get_review(&q,review_id)?;
            if review.revision<event.subject_revision{return Err(Fault::Store("workflow event exceeds review revision".into()));}
            match change {
                ReviewChange::Requested{target,reviewer,..}=>review.target==*target&&review.reviewer==*reviewer,
                ReviewChange::Closed{resolution}=>{
                    validate_review_resolution(tx,event,&review,resolution)?;
                    expected_routing=Some(ResolutionRouting{requested_by:review.requested_by,linked_record:event.subject.clone()});
                    match resolution {
                        ReviewResolution::Decided{review_id:id,target,..}|ReviewResolution::Withdrawn{review_id:id,target,..}=>id==review_id&&*target==review.target,
                    }
                },
            }
        },
        _=>false,
    };
    if !same_subject{return Err(Fault::Store("workflow event detail differs from canonical subject".into()));}
    // Absence is historical, not an invented routing request. A present route
    // must preserve the original terminal occurrence's immutable attribution.
    if let Some(routing)=&event.routing {
        workflow::validate_recorded_actor(tx,&routing.requested_by)?;
        let _=workflow::record_owner(tx,&routing.linked_record)?;
        if expected_routing.as_ref()!=Some(routing){return Err(Fault::Store("workflow resolution routing differs from canonical record".into()));}
    }
    Ok(())
}
/// Root calls this read-only audit before startup reconciliation or migration
/// writes. The root establishes that the schema already has workflow_events.
pub(crate) fn validate_stored(tx:&Connection)->Result<()> {
    let Some(p)=tx.limits.workflow.as_ref() else {
        let any:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM workflow_events)",[],|r|r.get(0))?;
        if any{return Err(Fault::Store("stored workflow events lack selected workflow policy".into()));}
        return Ok(());
    };
    let committed=super::collaboration_workflow::product_revision(tx)?;
    let mut q=tx.prepare(&format!("SELECT product_revision,ordinal,CAST(substr(CAST(body AS BLOB),1,{probe}) AS TEXT),CAST(substr(CAST(targets AS BLOB),1,{probe}) AS TEXT) FROM workflow_events ORDER BY product_revision,ordinal LIMIT ?1",probe=p.event_bytes.saturating_add(1)))?;
    let mut rows=q.query([integer(p.revisions.saturating_add(1) as u64)?])?;
    let mut count=0usize;let mut revision=None;let mut ordinal=0u64;
    let mut task_owners=std::collections::BTreeMap::new();
    while let Some(row)=rows.next()? {
        count+=1;if count>p.revisions{return Err(Fault::Store("stored workflow events exceed workflow.revisions".into()));}
        let raw_body:String=row.get(2)?;let raw_targets:String=row.get(3)?;
        let (event,targets)=read_event(tx,row.get(0)?,row.get(1)?,&raw_body,&raw_targets)?;
        if event.id.product_revision>committed{return Err(Fault::Store("workflow event exceeds committed product revision".into()));}
        if revision!=Some(event.id.product_revision){revision=Some(event.id.product_revision);ordinal=0;}
        if u64::from(event.id.ordinal)!=ordinal||ordinal>=p.events_per_mutation as u64{return Err(Fault::Store("workflow event ordinals are not a bounded contiguous revision".into()));}
        ordinal+=1;
        if encode(&event)?!=raw_body||encode(&targets)?!=raw_targets{return Err(Fault::Store("workflow event body or target envelope is not canonical".into()));}
        validate_event_record(tx,&event,&mut task_owners)?;
        for target in targets.concrete(){let s=super::sessions::get(tx,&target.session_id)?;if scope(tx,&s)?!=*target{return Err(Fault::Store("workflow concrete destination differs from retained session".into()));}}
        if let Some(claim)=targets.claim().filter(|c|!c.acknowledged) {
            let task=super::sessions::historical_task(tx,&claim.scope.task_id,&claim.scope.session_id)?;
            let accepted=task.input.resolved_limits.as_ref().ok_or_else(||Fault::Store("workflow claim has no accepted metadata".into()))?;
            let view=tx.with_limits(&accepted.global_metadata);
            let session=super::sessions::get(&view,&claim.scope.session_id)?;
            let pending=state(&view,&session)?.0.pending.ok_or_else(||Fault::Store("unacknowledged route claim has no retained session batch".into()))?;
            if pending.batch_id!=claim.batch_id||pending.delivery.as_ref()!=Some(&claim.intent)||!input_events(&pending).contains(&event.id) {
                return Err(Fault::Store("unacknowledged route claim differs from retained session input".into()));
            }
        }
    }
    for (task_id,(owner,_)) in task_owners {
        let head=super::collaboration_workflow::record_owner_revision(tx,&RecordRef::Task{task_id})?;
        if head.owner!=owner{return Err(Fault::Store("task coordination owner differs from retained owner-change chain".into()));}
    }
    Ok(())
}
fn admitted_pending(tx:&Connection,s:&Session,caller:&crate::mcp::Caller)->Result<Option<WorkflowContinuation>> {
    let p=policy(tx)?;
    let Some(c)=super::collaboration_work::admitted_continuation(tx,&s.task_id,&caller.attempt_id,p)? else{return Ok(None)};
    if c.task_id!=s.task_id || c.session_id!=s.id || c.resolved_waits.is_empty() {return Err(Fault::Store("admitted workflow continuation scope invalid".into()));}
    let mut acknowledged=0;
    for r in &c.resolved_waits {
        exists(tx,&r.event_id)?;
        if super::collaboration_work::acknowledged_event(tx,&s.task_id,&s.id,&r.event_id,p)? {acknowledged+=1;}
    }
    if acknowledged==c.resolved_waits.len(){return Ok(None)}
    if acknowledged!=0{return Err(Fault::Store("continuation input ACK is not atomic".into()));}
    Ok(Some(c))
}
/// Admission may reuse known preparation, never an unresolved append intent.
pub(super) fn delivery_blocks_admission(tx:&Connection,task:&Task)->Result<bool> {
    let present:bool=tx.query_row("SELECT workflow_delivery IS NOT NULL FROM sessions WHERE id=?1",[&task.session_id],|r|r.get(0))?;
    if !present{return Ok(false)}
    let accepted=task.input.resolved_limits.as_ref().ok_or_else(||Fault::Store("delivery has no accepted limits".into()))?;
    let view=tx.with_limits(&accepted.global_metadata);
    let session=super::sessions::get(&view,&task.session_id)?;
    Ok(state(&view,&session)?.0.pending.is_some_and(|b|b.delivery.is_some()))
}
pub(super) fn terminal_read_closed(tx:&Connection,task:&Task)->Result<bool> {
    let present:bool=tx.query_row("SELECT workflow_delivery IS NOT NULL FROM sessions WHERE id=?1",[&task.session_id],|r|r.get(0))?;
    if !present {return Ok(true)}
    let accepted=task.input.resolved_limits.as_ref().ok_or_else(||Fault::Store("terminal delivery lacks accepted limits".into()))?;
    let view=tx.with_limits(&accepted.global_metadata);
    let session=super::sessions::get(&view,&task.session_id)?;
    Ok(state(&view,&session)?.0.pending.is_none())
}
pub(super) fn apply(tx:&Connection,op:&Operation,cfg:&ControllerConfig,now:u64)->Result<(Data,bool)> {
    let caller=match op {Operation::ObserveCollaborationDelivery{caller,..}|Operation::PrepareCollaborationDelivery{caller,..}|Operation::BeginCollaborationDelivery{caller,..}|Operation::AcknowledgeCollaborationDelivery{caller,..}=>caller,_=>return Err(Fault::Store("wrong workflow delivery route".into()))};
    let task=super::collaboration_calls::caller(tx,caller,now)?;
    if !matches!(task.task.input.definition.execution_profile,Some(ExecutionProfile::CollaborationV2{..})) {
        return refuse("not_configured","accepted profile does not permit workflow delivery");
    }
    let accepted=task.task.input.resolved_limits.as_ref().ok_or_else(||Fault::Store("workflow caller has no accepted limits".into()))?;
    let accepted_tx=tx.with_limits(&accepted.global_metadata);
    let tx=&accepted_tx;
    policy(tx)?;
    let session=super::sessions::get(tx,&task.task.session_id)?;
    let (mut st,raw)=state(tx,&session)?;
    let continuation=admitted_pending(tx,&session,caller)?;
    let result=match op {
        Operation::ObserveCollaborationDelivery{limit:n,..}=>{
            limit(tx,*n)?;
            let available=if st.pending.as_ref().is_none_or(|b|b.delivery.is_none() && &b.prepared_by!=caller){match head(tx)?{Some(end)=>select_page(tx,&st,&end,*n,continuation.as_ref(),now)?.map(|s|s.page),None=>None}}else{None};
            (observed(tx,st,available)?,false)
        },
        Operation::PrepareCollaborationDelivery{operation_id,expected_cursor,expected_routing_cursor,through,limit:n,..}=>{
            limit(tx,*n)?;
            if expected_cursor!=&st.acknowledged_cursor{return refuse("stale_revision","workflow cursor changed or scope differs");}
            if expected_routing_cursor!=&st.routing_cursor{return refuse("stale_revision","workflow routing cursor changed");}
            if st.pending.as_ref().is_some_and(|b|&b.prepared_by!=caller && b.delivery.is_some()) {
                return refuse("delivery_pending","original workflow append intent remains unresolved");
            }
            if st.pending.as_ref().is_none_or(|b|&b.prepared_by!=caller && b.delivery.is_none()) {
                let selected=select_page(tx,&st,through,*n,continuation.as_ref(),now)?.ok_or_else(||Fault::Refused("no_changes","workflow window is already acknowledged".into()))?;
                retain_routes(tx,&st,&selected,operation_id,now)?;
    // Traceability scope: batch identity is the original receipt operation; replay uses the existing controller receipt owner.
            //= spec/impl/v2/implementation.md#committed-resolutions-reach-their-consumers
            //# A committed resolution delivery MUST carry an identity that deduplicates its delivery retries.
            st.pending=Some(DeliveryBatch{batch_id:operation_id.clone(),prepared_by:caller.clone(),delivery:None,page:selected.page});st.superseded=None;save(tx,&session,&st,&raw)?;
                (observed(tx,st,None)?,true)
            }else{(observed(tx,st,None)?,false)}
        },
        Operation::BeginCollaborationDelivery{operation_id,batch_id,context_binding,..}=>{
            if !valid_id(tx.limits,context_binding){return refuse("invalid_input","invalid workflow context binding");}
            let b=st.pending.as_ref().ok_or_else(||Fault::Refused("not_found","no pending workflow batch".into()))?.clone();
            if &b.batch_id!=batch_id{return refuse("batch_mismatch","workflow intent differs from pending batch");}
            if !b.page.has_input(){return refuse("invalid_input","empty prefix requires no executor append");}
            if b.page.continuation!=continuation {return refuse("delivery_pending","prepared input differs from current admitted continuation");}
            if let Some(intent)=&b.delivery {
                if intent.operation_id!=*operation_id || intent.caller!=*caller || intent.context_binding!=*context_binding {
                    return refuse("delivery_pending","original workflow append intent remains unresolved");
                }
                (observed(tx,st,None)?,false)
            } else {
                let intent=DeliveryIntent{operation_id:operation_id.clone(),caller:caller.clone(),context_binding:context_binding.clone()};
                if !begin_claims(tx,&b,&intent,now)? {
                    let route=b.page.routing.as_ref().ok_or_else(||Fault::Store("only route input may be superseded".into()))?;
                    // This receipt proves that no Begin was installed. Advance
                    // only the independent scan, never any input ACK or event.
                    st.routing_cursor=route.to_cursor.clone();st.routing_turn=false;
                    st.pending=None;st.superseded=Some(DeliverySupersession{batch_id:b.batch_id,operation_id:operation_id.clone(),caller:caller.clone(),context_binding:context_binding.clone()});
                    save(tx,&session,&st,&raw)?;
                    (observed(tx,st,None)?,true)
                } else {
                st.pending.as_mut().unwrap().delivery=Some(intent);
                save(tx,&session,&st,&raw)?;(observed(tx,st,None)?,true)
                }
            }
        },
        Operation::AcknowledgeCollaborationDelivery{batch_id,context_binding,..}=>{
            if !valid_id(tx.limits,context_binding){return refuse("invalid_input","invalid workflow context binding");}
            let b=st.pending.as_ref().ok_or_else(||Fault::Refused("not_found","no pending workflow batch".into()))?;
            if &b.batch_id!=batch_id{return refuse("batch_mismatch","workflow ACK differs from pending batch");}
            if b.page.has_input() && b.delivery.as_ref().is_none_or(|intent| &intent.caller!=caller || &intent.context_binding!=context_binding) {
                return refuse("delivery_pending","workflow ACK lacks exact original append intent");
            }
            if let Some(c)=&b.page.continuation {
                super::collaboration_work::acknowledge_continuation(tx,caller,context_binding,c,policy(tx)?)?;
            }
            acknowledge_routes(tx,b)?;
            if let Some(route)=&b.page.routing{st.routing_cursor=route.to_cursor.clone();st.routing_turn=false;}else{st.routing_turn=true;}
            st.acknowledged_cursor=b.page.to_cursor.clone();st.pending=None;st.last_ack_context=Some(context_binding.clone());save(tx,&session,&st,&raw)?;
            (observed(tx,st,None)?,true)
        },
        _=>unreachable!(),
    };
    let after=(cfg.clock)();if after<now{return refuse("clock_unavailable","clock moved backwards during workflow delivery");}
    super::collaboration_calls::caller(tx,caller,after)?;
    Ok(result)
}
/// Opaque public cursor is the canonical EventId JSON. Browser never parses its
/// numeric values; projected identities are decimal strings at its route boundary.
pub(super) fn activity(tx:&Connection,cursor:&Option<String>,n:usize)->Result<WorkflowActivityPage> {
    let p=policy(tx)?;limit(tx,n)?;
    let from=cursor.as_ref().map(|raw|{
        if raw.len()>tx.limits.controller.collaboration_cursor_bytes{return refuse("invalid_input","workflow activity cursor exceeds bound");}
        let id:EventId=serde_json::from_str(raw).map_err(|_|Fault::Refused("invalid_input","invalid workflow activity cursor".into()))?;
        if encode(&id)?!=*raw{return refuse("invalid_input","noncanonical workflow activity cursor");}exists(tx,&id)?;Ok(id)
    }).transpose()?;
    let end=head(tx)?;
    let Some(end_value)=&end else {return Ok(WorkflowActivityPage{items:vec![],through:None,next_cursor:None})};
    let (r,o)=from.as_ref().map(|x|(x.product_revision,i64::from(x.ordinal))).unwrap_or((0,-1));
    let mut q=tx.prepare(&format!("SELECT product_revision,ordinal,CAST(substr(CAST(body AS BLOB),1,{probe}) AS TEXT),CAST(substr(CAST(targets AS BLOB),1,{probe}) AS TEXT) FROM workflow_events WHERE (product_revision>?1 OR (product_revision=?1 AND ordinal>?2)) AND (product_revision<?3 OR (product_revision=?3 AND ordinal<=?4)) ORDER BY product_revision,ordinal LIMIT ?5",probe=p.event_bytes.saturating_add(1)))?;
    let mut rows=q.query(params![integer(r)?,o,integer(end_value.product_revision)?,i64::from(end_value.ordinal),integer(n.saturating_add(1) as u64)?])?;
    let mut result=WorkflowActivityPage{items:vec![],through:end.clone(),next_cursor:None};let mut last=from;
    while let Some(row)=rows.next()? {
        if result.items.len()==n {result.next_cursor=last.as_ref().map(encode).transpose()?;break;}
        let (event,_)=read_event(tx,row.get(0)?,row.get(1)?,&row.get::<_,String>(2)?,&row.get::<_,String>(3)?)?;
        result.items.push(event.clone());
        if bounded_json_size(&result,p.page_bytes).is_err(){result.items.pop();if result.items.is_empty(){return refuse("response_bound","workflow activity cannot fit first event");}result.next_cursor=last.as_ref().map(encode).transpose()?;break;}
        last=Some(event.id);
    }
    if bounded_json_size(&result,p.page_bytes).is_err(){return refuse("response_bound","workflow activity envelope exceeds bound");}
    Ok(result)
}


/// Read projection for the completion owner, not another delivery queue or
/// acknowledgement. Original event provenance stays intact for its reset rule.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct CompletionInput {
    pub event: WorkflowActivity,
    pub consumed: bool,
}
#[derive(Debug, Clone, PartialEq)]
pub(super) struct CompletionInputs {
    pub items: Vec<CompletionInput>,
    /// Even an advisory-only append intent remains an unknown input effect.
    pub pending_begin: bool,
}

/// Complete finite inventory at an active, already-required executor boundary.
/// No cursor filter, writes, receipt, clock read or recipient-policy override.
/// The completion owner supplies its selected scan bound and controller time.
/// A stopped caller must use its separately retained gate proof and the stopped
/// projection below; it cannot treat dynamic ineligibility as an empty proof.
pub(super) fn completion_inputs(
    tx: &Connection<'_>, task: &Task, now_ms: u64, scan_rows: usize,
) -> Result<CompletionInputs> {
    completion_inputs_for(tx, task, now_ms, scan_rows, CompletionScope::Active)
}

/// Read-only recheck for the same canonical stopped, nonuncertain attempt.
/// The completion owner supplies its original enforcing decision and owns final
/// Result or queued-resume admission checks. This grants no caller/input rights.
pub(super) fn completion_stopped_inputs(
    tx: &Connection<'_>, task: &Task, now_ms: u64, scan_rows: usize,
) -> Result<CompletionInputs> {
    completion_inputs_for(tx, task, now_ms, scan_rows, CompletionScope::Stopped)
}

#[derive(Clone, Copy)]
enum CompletionScope { Active, Stopped }

fn completion_inputs_for(
    tx: &Connection<'_>, task: &Task, now_ms: u64, scan_rows: usize, mode: CompletionScope,
) -> Result<CompletionInputs> {
    if scan_rows == 0 || scan_rows > isize::MAX as usize {
        return refuse("invalid_input", "completion workflow scan bound is invalid");
    }
    if !matches!(task.input.definition.execution_profile, Some(ExecutionProfile::CollaborationV2 { .. })) {
        return refuse("not_configured", "completion input projection requires accepted workflow delivery");
    }
    let attempt = task.attempt.as_ref().ok_or_else(|| Fault::Refused("stale_owner", "completion input projection lacks current attempt".into()))?;
    match mode {
        CompletionScope::Active if task.status != TaskStatus::Active || attempt.phase != AttemptPhase::Running || attempt.stopped || attempt.uncertain => {
            return refuse("reconciliation_required", "completion input projection requires active Running scope");
        }
        CompletionScope::Stopped if !matches!(task.status, TaskStatus::Active | TaskStatus::Queued)
            || attempt.phase != AttemptPhase::Stopped || !attempt.stopped || attempt.uncertain => {
            return refuse("reconciliation_required", "completion input projection requires the current stopped nonuncertain scope");
        }
        _ => {}
    }
    let caller = crate::mcp::Caller {
        task_id: task.id.clone(), attempt_id: attempt.id.clone(), generation: attempt.generation,
        worker_id: attempt.worker_id.clone(), incarnation: attempt.worker_incarnation.clone(),
    };
    let current = match mode {
        CompletionScope::Active => super::collaboration_calls::caller(tx, &caller, now_ms)?,
        CompletionScope::Stopped => load(tx, &task.id)?,
    };
    if current.task != *task { return refuse("stale_revision", "completion input task differs from canonical current scope"); }
    let accepted = task.input.resolved_limits.as_ref().ok_or_else(|| Fault::Store("completion input projection has no accepted metadata".into()))?;
    let view = tx.with_limits(&accepted.global_metadata);
    let tx = &view;
    let p = policy(tx)?;
    let session = super::sessions::get(tx, &task.session_id)?;
    let target = scope(tx, &session)?;
    let (state, _) = state(tx, &session)?;
    let pending_begin = state.pending.as_ref().is_some_and(|b| b.delivery.is_some());
    let begun_ids = state.pending.as_ref().filter(|b| b.delivery.is_some()).map(input_events).unwrap_or_default();
    // This helper also verifies that acknowledgement of an admitted continuation
    // is atomic. It must not treat admission or Prepare as consumption.
    let continuation = admitted_pending(tx, &session, &caller)?;
    let mut unseen_required = continuation.iter().flat_map(|c| c.resolved_waits.iter().map(|r| r.event_id.clone()))
        .collect::<std::collections::BTreeSet<_>>();
    let required = unseen_required.clone();
    let bound = scan_rows.min(p.revisions);
    let through = head(tx)?;
    let mut result = CompletionInputs { items: vec![], pending_begin };
    let Some(through) = through else {
        if !unseen_required.is_empty() || !begun_ids.is_empty() {
            return Err(Fault::Store("completion input references lack canonical event inventory".into()));
        }
        return Ok(result);
    };
    let committed = super::collaboration_workflow::product_revision(tx)?;
    if through.product_revision > committed {
        return Err(Fault::Store("completion input head exceeds committed workflow revision".into()));
    }
    let mut query = tx.prepare(&format!(
        "SELECT product_revision,ordinal,CAST(substr(CAST(body AS BLOB),1,{probe}) AS TEXT),CAST(substr(CAST(targets AS BLOB),1,{probe}) AS TEXT) FROM workflow_events WHERE product_revision<?1 OR (product_revision=?1 AND ordinal<=?2) ORDER BY product_revision,ordinal LIMIT ?3",
        probe = p.event_bytes.saturating_add(1)))?;
    let mut rows = query.query(params![integer(through.product_revision)?, i64::from(through.ordinal), integer(bound.saturating_add(1) as u64)?])?;
    let mut count = 0usize;
    while let Some(row) = rows.next()? {
        count += 1;
        if count > bound { return refuse("capacity", "completion workflow inventory exceeds selected scan bound"); }
        let (event, targets) = read_event(tx, row.get(0)?, row.get(1)?, &row.get::<_, String>(2)?, &row.get::<_, String>(3)?)?;
        let terminal = matches!(&event.detail,
            ActivityDetail::Task(TaskChange::Closed { .. }) |
            ActivityDetail::Question(QuestionChange::Closed { .. }) |
            ActivityDetail::Review(ReviewChange::Closed { .. }));
        let admitted = required.contains(&event.id);
        if admitted && !terminal { return Err(Fault::Store("required completion input is not a terminal resolver occurrence".into())); }
        unseen_required.remove(&event.id);
        // Advisory activity and WaitResolved audit siblings are not new required
        // outcomes. Their unknown Begin still survives in pending_begin above.
        if !terminal { continue; }
        let concrete = targets.concrete().contains(&target);
        let own_claim = targets.claim().filter(|claim| claim.scope == target);
        let route_acked = targets.acknowledged_for(&target);
        let own_begin = begun_ids.contains(&event.id);
        if let Some(claim) = own_claim.filter(|claim| !claim.acknowledged) {
            if state.pending.as_ref().is_none_or(|b| b.batch_id != claim.batch_id
                || b.delivery.as_ref() != Some(&claim.intent) || !input_events(b).contains(&event.id)) {
                return Err(Fault::Store("completion route Begin differs from original pending input".into()));
            }
        }
        // Final Result/queued-resume proof cannot select a new route for a
        // stopped scope. Existing concrete, admitted, claimed and ACKed inputs
        // above remain exact obligations even when route eligibility changed.
        let currently_owed = matches!(mode, CompletionScope::Active)
            && targets.owed() && route_eligible(tx, &event, &target, now_ms)?;
        if !(concrete || admitted || own_claim.is_some() || route_acked || own_begin || currently_owed) { continue; }
        // Only terminal events reach this point; owner-change chronology is
        // validated by the full stored-event audit.
        validate_event_record(tx, &event, &mut std::collections::BTreeMap::new())?;
        let actual_work_ack = retained_input(tx, &target, &event.id)?.is_some();
        // An ordinary cursor proves input retention only for immutable concrete
        // targets, which could not be skipped except by their exact ACK proof.
        // Routing scan progress never proves consumption.
        let ordinary_acked = concrete && state.acknowledged_cursor.position.as_ref().is_some_and(|id| id >= &event.id);
        let consumed = !own_begin && (actual_work_ack || route_acked || ordinary_acked);
        result.items.push(CompletionInput { event, consumed });
    }
    if !unseen_required.is_empty() {
        return Err(Fault::Store("required completion input is absent from canonical inventory".into()));
    }
    Ok(result)
}

#[cfg(test)]
#[path = "collaboration_notifications_completion_tests.rs"]
mod completion_tests;
