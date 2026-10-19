//! Pinned public turn/start outputSchema and completed-item/turn events only.
//! None-phase is unknown. No hidden model IDs, costs or reference synthesis.
use super::*;
use crate::charter::*;
use crate::protocol::{Data,Operation, bounded_json_size};

pub(super) struct Capture {
    boundary: CharterSuppliedBoundary,
    turn_id: String,
    items: Vec<Value>,
    frame_bytes: usize,
}
fn context_id(scope:&InitialCharterScope)->String {format!("{}-charter-supplied-initial",scope.caller.attempt_id)}
pub(super) async fn start(session:&mut Session,scope:&InitialCharterScope,state:&Path,index:u32,
    thread_id:&str,mut params:Value,control:&Control,credentials:&[String])->std::result::Result<(Value,Capture),String>{
    control.check()?;
    if !super::super::handoff::admits_supplied_turn_records(&scope.worker_state,&control.runtime_limits.worker)? {
        return Err("supplied outer records require owned worker17 admission".into());
    }
    let id=format!("{}-charter-outer-{index}",scope.caller.attempt_id);
    let dir=state.join("supplied-charter").join(format!("outer-{index}"));process::private_directory(&dir)?;
    let marker=dir.join("dispatch.json");
    // Presence (including malformed state) forbids a second executor send. The
    // existing original-operation journal resolves controller ACK uncertainty.
    if marker.try_exists().map_err(|_|"supplied turn dispatch cannot be inspected")? {
        return Err("original supplied turn requires reconciliation; no second turn/start".into());
    }
    let client=crate::client::ApiClient::new(&scope.controller_url,control.runtime_limits.clone())?;
    let delivery=match crate::worker::controller_call(&client,Operation::QueryCharter{query:CharterQuery::SuppliedContext{delivery_id:context_id(scope)}},control).await?{
        Data::CharterQuery(v)=>match *v {
            CharterQueryResult::SuppliedContext(v)=>v,_=>return Err("original supplied context absent".into()),
        },_=>return Err("original supplied context absent".into()),
    };
    if delivery.caller!=scope.caller||delivery.session_id!=scope.session_id
        ||delivery.acknowledgment.as_ref().map(|a|a.thread_id.as_str())!=Some(thread_id){return Err("supplied outer context differs".into());}
    params["outputSchema"]=supplied_result_schema();
    params.get_mut("input").and_then(Value::as_array_mut).ok_or("supplied turn input missing")?
        .push(json!({"type":"text","text":supplied_result_instructions()}));
    let request_id=session.next_id;
    if request_id==0||request_id==u64::MAX{return Err("supplied public request ID exhausted".into());}
    bounded_json_size(&json!({"id":request_id,"method":"turn/start","params":params}),session.limits.frame_bytes)?;
    if super::super::value_contains_credential(&params,credentials,&control.runtime_limits.worker)?{return Err("supplied turn contains configured credential".into());}
    let expected=CharterSuppliedBoundary{turn_id:id.clone(),delivery_id:delivery.delivery_id.clone(),session_id:scope.session_id.clone(),
        caller:scope.caller.clone(),outer_index:index,effective:delivery.effective,request_id,thread_id:thread_id.into(),output_schema:supplied_result_schema()};
    let operation=Operation::ObserveCharter{operation_id:id.clone(),caller:scope.caller.clone(),observation:CharterObservation::BeginSuppliedTurn{
        delivery_id:delivery.delivery_id,outer_index:index,request_id,thread_id:thread_id.into(),output_schema:supplied_result_schema()}};
    let mut begun=charter_delivery::mutation_outcome(&client,&dir.join("intent.json"),operation,control,credentials).await?;
    let CharterOutcome::SuppliedTurn(begun_turn)=begun.as_ref() else{return Err("supplied turn intent response differs".into())};
    if begun_turn.boundary!=expected||begun_turn.acknowledgment.is_some()||begun_turn.reply.is_some()||begun_turn.terminal.is_some(){return Err("supplied turn original intent differs".into());}
    control.check()?;
    // Marker and original request are ordered without an intervening await.
    process::atomic_json(&marker,&json!({"turn_id":id,"request_id":request_id,"thread_id":thread_id}))?;
    let response=session.request("turn/start",params,control).await.map_err(|e|format!("supplied turn outcome uncertain; no resend: {e}"))?;
    let turn_id=response["turn"]["id"].as_str().ok_or("supplied start lacks actual turn ID; no resend")?.to_owned();
    check_association_ids(thread_id,Some(&turn_id),credentials)?;
    let ack_id=format!("{id}-ack");
    let result=charter_delivery::mutation_outcome(&client,&dir.join("ack.json"),Operation::ObserveCharter{
        operation_id:ack_id.clone(),caller:scope.caller.clone(),observation:CharterObservation::AckSuppliedTurn{
            turn_id:id,request_id,response:response.clone()}},control,credentials).await?;
    let CharterOutcome::SuppliedTurn(begun_turn)=begun.as_mut() else{return Err("supplied turn intent response differs".into())};
    begun_turn.acknowledgment=Some(CharterSuppliedTurnAck{operation_id:ack_id,request_id,turn_id:turn_id.clone(),response:response.clone()});
    if result!=begun{return Err("supplied turn original ACK differs; no resend".into());}
    Ok((response,Capture{boundary:expected,turn_id,items:vec![],frame_bytes:session.limits.frame_bytes}))
}
impl Capture {
    pub(super) fn item(&mut self,message:&Value,control:&Control)->std::result::Result<(),String>{
        if message["method"]!="item/completed"||message["params"]["threadId"]!=self.boundary.thread_id
            ||message["params"]["turnId"]!=self.turn_id||message["params"]["item"]["type"]!="agentMessage"{return Ok(());}
        let id=message["params"]["item"]["id"].as_str().ok_or("public completed item lacks its actual ID")?;
        if let Some(old)=self.items.iter().find(|m|m["params"]["item"]["id"]==id){
            return if old==message{Ok(())}else{Err("original supplied completed item changed".into())};
        }
        let policy=control.runtime_limits.charter.as_ref().ok_or("supplied result policy absent")?;
        if self.items.len()>=policy.reply_items{return Err("supplied public items exceed accepted charter.reply_items".into());}
        bounded_json_size(message,self.frame_bytes.checked_mul(25).ok_or("public item canonical bound overflow")?)?;
        self.items.push(message.clone());Ok(())
    }
    fn projection(&self,terminal:&Value)->(Option<Value>,Vec<String>,Option<String>,Vec<String>){
        // Completed notifications are immutable original observations. A full
        // terminal view adds persisted items; even an empty full view cannot
        // erase an already observed final. Other views add no final evidence.
        let turn=&terminal["params"]["turn"];
        let mut items:Vec<&Value>=self.items.iter().map(|v|&v["params"]["item"]).collect();
        let mut conflicts=Vec::new();
        if turn.get("itemsView").is_none_or(|v|v=="full") {
            for item in turn["items"].as_array().into_iter().flatten().filter(|v|v["type"]=="agentMessage") {
                if let Some(id)=item["id"].as_str() {
                    let same:Vec<_>=items.iter().copied().filter(|v|v["id"]==id).collect();
                    if same.iter().any(|old|*old==item){continue;}
                    if !same.is_empty()&&!conflicts.iter().any(|v|v==id){conflicts.push(id.to_owned());}
                }
                items.push(item);
            }
        }
        let finals:Vec<_>=items.iter().copied().filter(|v|v["phase"]=="final_answer").collect();
        let mut ids=Vec::new();for id in items.iter().filter_map(|v|v["id"].as_str()){if !ids.iter().any(|v|v==id){ids.push(id.to_owned());}}
        let parsed:Vec<Value>=finals.iter().filter_map(|v|v["text"].as_str().and_then(|s|serde_json::from_str(s).ok())).collect();
        let declarations:Vec<Value>=parsed.iter().filter_map(|v|v.get("charter_references").cloned()).collect();
        let declaration=if finals.len()==1 {declarations.into_iter().next()}else if declarations.is_empty(){None}else{Some(Value::Array(declarations))};
        // Malformed/missing charter metadata never discards the genuine product
        // final text. Only a valid wrapper result is unwrapped.
        let result=if finals.len()==1&&conflicts.is_empty(){parsed.first().and_then(|v|v.get("result")).and_then(Value::as_str)
            .or_else(||finals[0]["text"].as_str()).map(str::to_owned)}else{None};
        (declaration,ids,result,conflicts)
    }
    pub(super) async fn finish(self,scope:&InitialCharterScope,terminal:&Value,control:&Control)->std::result::Result<Option<String>,String>{
        let status=terminal["params"]["turn"]["status"].as_str().ok_or("supplied terminal status missing")?;
        if terminal["method"]!="turn/completed"||terminal["params"]["threadId"]!=self.boundary.thread_id
            ||terminal["params"]["turn"]["id"]!=self.turn_id||!matches!(status,"completed"|"failed"|"interrupted"){
            return Err("supplied final event differs from actual outer occurrence".into());
        }
        bounded_json_size(terminal,self.frame_bytes.checked_mul(25).ok_or("public terminal canonical bound overflow")?)?;
        let (declaration,output_item_ids,result,conflicts)=self.projection(terminal);
        let metadata=CharterSuppliedTerminal{thread_id:self.boundary.thread_id,turn_id:self.turn_id,status:status.into(),error:terminal["params"]["turn"].get("error").cloned()};
        let id=self.boundary.turn_id;
        let reply=CharterReplyObservation{output:Some(json!({"items":self.items,"terminal":terminal})),output_reference:None,
            response_id:None,output_item_ids,tool_call_ids:vec![],declaration};
        // Same immutable output/journal recovery as native observations. Storage
        // failure is explicit uncertainty; audit findings themselves never gate.
        let client=crate::client::ApiClient::new(&scope.controller_url,control.runtime_limits.clone())?;
        super::super::charter::record_supplied_original(&client,&scope.worker_state,&control.runtime_limits,self.frame_bytes,
            scope.caller.clone(),format!("{id}-reply"),CharterObservation::SuppliedReply{turn_id:id,terminal:metadata,reply}).await?;
        if !conflicts.is_empty(){return Err("original supplied completed item identity conflicts; originals retained, no result inferred".into());}
        Ok(result)
    }
}

#[cfg(test)]
#[path="codex_charter_turns_tests.rs"]
mod tests;
