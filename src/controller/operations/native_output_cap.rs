//! A fully received, validated output-cap terminal. This receipt grants no tool
//! effect or final result; it accounts for the original charged request only.
use super::*;
use crate::{mcp::Caller,native_loop_contract as contract};
pub(crate) struct Proof {pub operation_id:String,pub controller_revision:u64}
fn caller(r:&Report)->Caller {Caller {task_id:r.task_id.clone(),attempt_id:r.attempt_id.clone(),generation:r.generation,worker_id:r.worker_id.clone(),incarnation:r.incarnation.clone()}}
fn body(task:&Task,r:&Report)->Result<(u32,u32)> {
    let Observation::NativeOutputCapped {request_index,undispatched_calls,request_binding,output_binding}=&r.observation else {return refuse("source_mismatch","output cap kind differs")};
    let s=task.input.resolved_limits.as_ref().ok_or_else(||Fault::Refused("source_mismatch","output cap accepted policy absent".into()))?;
    s.validate_for(&task.input).map_err(Fault::Store)?;
    let p=&task.input.execution_limits().map_err(Fault::Store)?.native;
    request_binding.validate(p.request_bytes).map_err(Fault::Store)?;
    output_binding.validate(task.input.limits.output_bytes.max(p.context_bytes)).map_err(Fault::Store)?;
    if task.id!=r.task_id || task.input.definition.executor!=ExecutorKind::Native || task.input.definition.execution_profile.is_none()
        || s.global_metadata.native_retry.is_none() || *request_index==0 || *undispatched_calls as usize>p.function_calls_per_response
        || r.report_id!=contract::output_cap_id(&r.attempt_id,*request_index)
        || !task.attempt.as_ref().is_some_and(|a|a.id==r.attempt_id&&a.generation==r.generation&&a.worker_id==r.worker_id&&a.worker_incarnation==r.incarnation) {
        return refuse("source_mismatch","output cap original owner/policy/identity differs");
    }
    Ok((*request_index,*undispatched_calls))
}
fn original(tx:&Connection<'_>,task:&Task,r:&Report,index:u32,calls:u32)->Result<u64> {
    let c=caller(r);let charge=super::native_retry::reservation(tx,&c,index)?;
    for id in [contract::rejection_id(&c.attempt_id,index),contract::format_failure_id(&c.attempt_id,index)] {
        if super::super::metadata::read(tx,&id)?.is_some() {return refuse("source_mismatch","output cap conflicts with original outcome");}
    }
    // Only actual bounded call occurrences are checked. No configured-budget scan.
    for ordinal in 1..=calls {
        if super::super::metadata::read_retained(tx,&contract::call_operation_id(&c.attempt_id,index,ordinal))?.is_some() {
            return refuse("source_mismatch","output cap call was already dispatched");
        }
    }
    if task.input.definition.charter.is_some() {
        let (v,revision)=super::super::charters::recorded_native_reply(tx,&c,index)?;
        // The reply reader has already verified every referenced original byte.
        let (original_bytes,digest)=if let Some(reference)=&v.observation.output_reference {
            (reference.bytes,reference.sha256.clone())
        } else {
            let bytes=serde_json::to_vec(v.observation.output.as_ref().ok_or_else(||Fault::Refused("source_mismatch","output cap original items absent".into()))?).map_err(|e|Fault::Store(e.to_string()))?;
            let digest:String=aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256,&bytes).as_ref().iter().map(|b|format!("{b:02x}")).collect();
            (bytes.len() as u64,digest)
        };
        let Observation::NativeOutputCapped {output_binding,..}=&r.observation else {unreachable!()};
        if v.observation.tool_call_ids.len()!=calls as usize || output_binding.bytes!=original_bytes || output_binding.sha256!=digest || revision<=charge {
            return refuse("source_mismatch","output cap differs from original charter output");
        }
        Ok(revision)
    } else {Ok(charge)}
}
pub(crate) fn validate_report(tx:&Connection<'_>,task:&Task,r:&Report)->Result<()> {
    let (index,calls)=body(task,r)?;original(tx,task,r,index,calls)?;
    let charged:u32=tx.query_row("SELECT model_requests FROM attempts WHERE id=?1",[&r.attempt_id],|r|r.get(0))?;
    if charged!=index {return refuse("source_mismatch","output cap must settle latest original charge");}Ok(())
}
pub(crate) fn confirmed(tx:&Connection<'_>,c:&Caller,index:u32)->Result<Option<Proof>> {
    let id=contract::output_cap_id(&c.attempt_id,index);let Some(v)=super::super::metadata::read(tx,&id)? else {return Ok(None)};
    let (Operation::ObserveWorker {report:r},Outcome::Ok {data:Data::Task(task)})=(&v.request,&v.outcome) else {return refuse("source_mismatch","output cap is not original successful receipt")};
    let (actual,calls)=body(task,r)?;
    if caller(r)!=*c || actual!=index {return refuse("source_mismatch","output cap original subject differs");}
    let accepted=tx.with_limits(&task.input.resolved_limits.as_ref().unwrap().global_metadata);
    if original(&accepted,task,r,index,calls)? >= v.commit.controller_revision {return refuse("source_mismatch","output cap receipt precedes original response");}
    Ok(Some(Proof {operation_id:id,controller_revision:v.commit.controller_revision}))
}
