//! Public outer-turn carriage only. No model request charge, hidden response ID,
//! implicit source update, cancellation release or new execution authority.
use super::*;
use serde_json::Value;

pub(super) fn require_schema(tx:&StoreContext<'_>)->Result<()> {
    let version:i64=tx.query_row("PRAGMA user_version",[],|r|r.get(0))?;
    if version<super::super::application_schema::SUPPLIED_TURN_VERSION{return Err(corrupt("supplied public turn records require schema22"));}
    Ok(())
}
fn empty(boundary: CharterSuppliedBoundary) -> CharterSuppliedTurn {
    CharterSuppliedTurn { boundary, acknowledgment: None, reply: None, terminal: None }
}
fn context(tx: &StoreContext<'_>, b: &CharterSuppliedBoundary, l: &CharterLimits) -> Result<CharterSuppliedContext> {
    let c = supplied::query(tx, &b.delivery_id, l)?;
    if c.session_id != b.session_id || c.caller != b.caller || c.effective != b.effective
        || c.acknowledgment.as_ref().map(|a|a.thread_id.as_str()) != Some(b.thread_id.as_str()) {
        return refuse("source_mismatch", "outer turn differs from acknowledged supplied context");
    }
    Ok(c)
}
pub(super) fn validate_intent(tx: &StoreContext<'_>, b: &CharterSuppliedBoundary, l: &CharterLimits) -> Result<()> {
    require_schema(tx)?;
    identity(tx, &b.thread_id)?;identity(tx, &b.delivery_id)?;
    if b.outer_index == 0 || b.request_id == 0 || b.output_schema != supplied_result_schema() {
        return Err(corrupt("supplied turn schema or public occurrence differs"));
    }
    let task = historical(tx, &b.caller, &b.session_id)?;
    if task.input.definition.executor != ExecutorKind::Coding {return Err(corrupt("supplied turn is not Coding"));}
    context(tx,b,l)?;
    bounded(b,l.record_bytes)?;
    bounded(&b.output_schema,task.input.execution_limits().map_err(|e|corrupt(&e))?.coding.frame_bytes)
}
pub(super) fn begin(ctx: &Context<'_, '_>, caller: &Caller, delivery_id: &str, outer_index: u32,
    request_id: u64, thread_id: &str, output_schema: &Value) -> Result<CharterOutcome> {
    if outer_index==0||request_id==0||output_schema!=&supplied_result_schema(){return refuse("invalid_input","supplied public occurrence or output schema differs");}
    let c = supplied::query(ctx.tx,delivery_id,ctx.limits)?;
    let current = super::super::collaboration_calls::caller(ctx.tx,caller,ctx.now_ms)?.task;
    if caller != &c.caller || current.session_id != c.session_id {return refuse("caller_mismatch","supplied turn caller differs");}
    if current.input.definition.executor != ExecutorKind::Coding {return refuse("invalid_input","supplied turn requires Coding");}
    let observed: (Option<String>,bool) = ctx.tx.query_row("SELECT thread_id,executor_outcome IS NOT NULL FROM attempts WHERE id=?1",[&caller.attempt_id],|r|Ok((r.get(0)?,r.get(1)?)))?;
    if observed.1 || observed.0.as_deref().is_some_and(|v|v!=thread_id) {
        return refuse("invalid_transition","supplied turn follows outcome or differs from owned thread");
    }
    let previous: i64 = ctx.tx.query_row("SELECT coalesce(max(request_index),0) FROM charter_requests WHERE attempt_id=?1",[&caller.attempt_id],|r|r.get(0))?;
    if unsigned(previous)?.checked_add(1) != Some(u64::from(outer_index)) {
        return refuse("reconciliation_required","supplied turn is not the next original outer occurrence");
    }
    if previous>0 {
        let prior=request_at(ctx.tx,caller,u32::try_from(previous).map_err(|_|corrupt("outer index overflow"))?,ctx.limits)?;
        let value=query(ctx.tx,prior.id(),ctx.limits)?;
        if value.terminal.as_ref().is_none_or(|v|v.status!="completed") {
            return refuse("reconciliation_required","original supplied turn must finish before another start");
        }
    }
    let b=CharterSuppliedBoundary{turn_id:ctx.operation_id.into(),delivery_id:delivery_id.into(),session_id:c.session_id,
        caller:caller.clone(),outer_index,effective:c.effective,request_id,thread_id:thread_id.into(),output_schema:output_schema.clone()};
    validate_intent(ctx.tx,&b,ctx.limits)?;
    // No SetPending adoption or native reservation. The initial delivery remains
    // authoritative until a separately supported explicit update is implemented.
    insert_request(ctx,&RequestRecord::SuppliedTurn(b.clone()))?;
    Ok(CharterOutcome::SuppliedTurn(empty(b)))
}
fn boundary(tx: &StoreContext<'_>, id: &str,l: &CharterLimits)->Result<CharterSuppliedBoundary>{
    match request(tx,id,l)?{RequestRecord::SuppliedTurn(b)=>Ok(b),_=>refuse("invalid_input","not a supplied outer turn")}
}
fn check_ack(tx:&StoreContext<'_>,b:&CharterSuppliedBoundary,a:&CharterSuppliedTurnAck,l:&CharterLimits)->Result<()> {
    identity(tx,&a.operation_id)?;identity(tx,&a.turn_id)?;
    let task=historical(tx,&b.caller,&b.session_id)?;
    bounded(&a.response,task.input.execution_limits().map_err(|e|corrupt(&e))?.coding.frame_bytes)?;bounded(a,l.record_bytes)?;
    if a.request_id!=b.request_id || a.response.get("turn").and_then(|v|v.get("id")).and_then(Value::as_str)!=Some(a.turn_id.as_str()) {
        return refuse("source_mismatch","supplied turn acknowledgment differs from original public request");
    }
    Ok(())
}
pub(super) fn acknowledged(tx:&StoreContext<'_>,id:&str,l:&CharterLimits)->Result<CharterSuppliedTurn>{
    let b=boundary(tx,id,l)?;let mut value=empty(b);
    if let Some(raw)=raw_body(tx,"SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM charter_closures WHERE request_id=?1",id,l.record_bytes)?{
        let ack:CharterSuppliedTurnAck=stored(&raw,l.record_bytes)?;check_ack(tx,&value.boundary,&ack,l)?;value.acknowledgment=Some(ack);
    }Ok(value)
}
pub(super) fn ack(ctx:&Context<'_, '_>,caller:&Caller,id:&str,request_id:u64,response:&Value)->Result<CharterOutcome>{
    let mut value=acknowledged(ctx.tx,id,ctx.limits)?;let b=&value.boundary;
    historical(ctx.tx,caller,&b.session_id)?;
    if caller!=&b.caller{return refuse("caller_mismatch","supplied turn ACK caller differs");}
    validate_request_receipt(ctx.tx,&request(ctx.tx,id,ctx.limits)?)?;
    if value.acknowledgment.is_some(){return refuse("operation_conflict","supplied turn already acknowledged");}
    let turn_id=response.get("turn").and_then(|v|v.get("id")).and_then(Value::as_str).ok_or_else(||Fault::Refused("invalid_input","actual public turn ID missing".into()))?;
    let ack=CharterSuppliedTurnAck{operation_id:ctx.operation_id.into(),request_id,turn_id:turn_id.into(),response:response.clone()};
    check_ack(ctx.tx,b,&ack,ctx.limits)?;
    value.acknowledgment=Some(ack.clone());bounded(&value,ctx.limits.record_bytes)?;capacity(ctx.tx,ctx.limits,1)?;
    ctx.tx.execute("INSERT INTO charter_closures(request_id,body) VALUES(?1,?2)",params![id,encode(&ack)?])?;
    Ok(CharterOutcome::SuppliedTurn(value))
}
pub(super) fn validate_ack_receipt(tx:&StoreContext<'_>,id:&str,l:&CharterLimits)->Result<()> {
    let value=acknowledged(tx,id,l)?;let a=value.acknowledgment.as_ref().ok_or_else(||corrupt("supplied turn ACK missing"))?;
    let original=receipt(tx,&a.operation_id)?;let r=request(tx,id,l)?;validate_request_receipt(tx,&r)?;
    if original.commit.controller_revision<=receipt(tx,id)?.commit.controller_revision || !matches!((&original.request,charter_outcome(&original)?),
        (Operation::ObserveCharter{caller,observation:CharterObservation::AckSuppliedTurn{turn_id,request_id,response},..},CharterOutcome::SuppliedTurn(v))
        if caller==&value.boundary.caller&&turn_id==id&&*request_id==a.request_id&&response==&a.response&&v==&value){return Err(corrupt("supplied turn ACK original receipt differs"));}Ok(())
}
pub(super) fn validate_terminal(tx:&StoreContext<'_>,id:&str,t:&CharterSuppliedTerminal,l:&CharterLimits)->Result<()> {
    let value=acknowledged(tx,id,l)?;
    if value.acknowledgment.is_none(){return refuse("reconciliation_required","supplied result lacks its original start acknowledgment");}
    validate_ack_receipt(tx,id,l)?;
    if t.thread_id!=value.boundary.thread_id || value.acknowledgment.as_ref().map(|a|a.turn_id.as_str())!=Some(t.turn_id.as_str())
        || !matches!(t.status.as_str(),"completed"|"failed"|"interrupted") {
        return refuse("source_mismatch","supplied result does not belong to acknowledged public turn");
    }Ok(())
}
pub(super) fn result(ctx:&Context<'_, '_>,caller:&Caller,id:&str,terminal:&CharterSuppliedTerminal,value:&CharterReplyObservation)->Result<CharterOutcome>{
    let r=request(ctx.tx,id,ctx.limits)?;
    historical(ctx.tx,caller,r.session())?;
    if caller!=r.caller(){return refuse("caller_mismatch","supplied result caller differs");}
    validate_terminal(ctx.tx,id,terminal,ctx.limits)?;
    if value.response_id.is_some()||!value.tool_call_ids.is_empty(){return refuse("invalid_input","outer result must not fabricate hidden model response or tool IDs");}
    Ok(CharterOutcome::Reply(insert_reply(ctx,&r,value)?))
}
pub(super) fn query(tx:&StoreContext<'_>,id:&str,l:&CharterLimits)->Result<CharterSuppliedTurn>{
    let mut value=acknowledged(tx,id,l)?;let r=request(tx,id,l)?;validate_request_receipt(tx,&r)?;
    if value.acknowledgment.is_some(){validate_ack_receipt(tx,id,l)?;}
    if let Some(v)=reply(tx,&r,l)?{
        validate_reply_receipt(tx,&r,&v,l)?;
        let original=receipt(tx,&v.reply_id)?;
        let Operation::ObserveCharter{observation:CharterObservation::SuppliedReply{terminal,..},..}=original.request else{return Err(corrupt("supplied result original kind differs"));};
        let ack=value.acknowledgment.as_ref().ok_or_else(||corrupt("supplied result precedes ACK"))?;
        if original.commit.controller_revision<=receipt(tx,&ack.operation_id)?.commit.controller_revision{return Err(corrupt("supplied result precedes original ACK"));}
        value.reply=Some(v);value.terminal=Some(terminal);
    }
    bounded(&value,l.record_bytes)?;Ok(value)
}
