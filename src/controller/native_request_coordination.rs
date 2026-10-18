//! Host-only coordination in the existing controller transaction and receipt owner.
//! Grants precede Usage; a replayed grant is historical evidence, not a second POST.
use super::{database::*, ControllerConfig};
use super::database::StoreContext as Connection;
use crate::{limits::{NativeRetryLimits, RuntimeLimits}, native_retry::*, protocol::*};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag="kind", rename_all="snake_case", deny_unknown_fields)]
enum AuthPhase { Ready, Cooldown { until_ms:u64 }, Probe { permit:NativeRequestPermit } }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Authentication { key:NativeAuthenticationKey, generation:u64, phase:AuthPhase }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RequestSlot { permit:NativeRequestPermit, settlement:Option<NativeRequestSettlement> }

fn selected(cfg:&ControllerConfig)->Result<&NativeRetryLimits> {
    cfg.limits.native_retry.as_ref().ok_or_else(||Fault::Refused("not_configured","native coordination storage policy absent".into()))
}
fn read<T:serde::de::DeserializeOwned+Serialize>(tx:&Connection,kind:&str,key:&str,p:&NativeRetryLimits)->Result<Option<T>> {
    let raw:Option<String>=tx.query_row("SELECT CAST(substr(CAST(body AS BLOB),1,?3) AS TEXT) FROM native_request_coordination WHERE kind=?1 AND key=?2",
        params![kind,key,sql_bound(p.coordination_record_bytes.saturating_add(1))?],|r|r.get(0)).optional()?;
    raw.map(|raw|{
        if raw.len()>p.coordination_record_bytes{return refuse("capacity","native_retry.coordination_record_bytes exceeded")}
        decode(&raw)
    }).transpose()
}
fn write<T:Serialize>(tx:&Connection,kind:&str,key:&str,value:&T,p:&NativeRetryLimits,capacity:usize)->Result<()> {
    let body=encode(value)?;
    if body.len()>p.coordination_record_bytes||key.len()>p.coordination_record_bytes {
        return refuse("capacity","native_retry.coordination_record_bytes exceeded");
    }
    let exists:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM native_request_coordination WHERE kind=?1 AND key=?2)",params![kind,key],|r|r.get(0))?;
    if !exists {
        let count:i64=tx.query_row("SELECT count(*) FROM (SELECT 1 FROM native_request_coordination WHERE kind=?1 LIMIT ?2)",
            params![kind,sql_bound(capacity.saturating_add(1))?],|r|r.get(0))?;
        let count=usize::try_from(count).map_err(|_|Fault::Store("native coordination row count invalid".into()))?;
        super::native_retry_state::validate_new_entry_count(count,capacity).map_err(|e|Fault::Refused("capacity",e))?;
    }
    tx.execute("INSERT INTO native_request_coordination(kind,key,body) VALUES(?1,?2,?3) ON CONFLICT(kind,key) DO UPDATE SET body=excluded.body",params![kind,key,body])?;
    Ok(())
}
fn request_key(permit:&NativeRequestPermit)->Result<String> { encode(&(&permit.circuit.caller.attempt_id,permit.request_index)) }
// Traceability scope: Authentication state is keyed by endpoint and credential configuration across workers; settle persists the accepted metadata cooldown and original history. The retry block consumer retains the reason-bearing alert.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A provider authentication failure MUST be coordinated across workers for the same dependency and credential-configuration identity with metadata-controlled backoff and a retained alert.
fn auth_key(key:&CircuitKey,credential_configuration:&str)->NativeAuthenticationKey {
    NativeAuthenticationKey{endpoint:key.endpoint.clone(),credential_configuration:credential_configuration.into()}
}
fn accepted<'a>(task:&'a Task)->Result<&'a NativeRetryLimits> {
    accepted_runtime(task)?.native_retry.as_ref()
        .ok_or_else(||Fault::Refused("not_configured","accepted native retry policy absent".into()))
}
fn accepted_runtime(task:&Task)->Result<&RuntimeLimits> {
    let snapshot=task.input.resolved_limits.as_ref().ok_or_else(||Fault::Store("native task policy absent".into()))?;
    snapshot.validate_for(&task.input).map_err(Fault::Store)?;
    Ok(&snapshot.global_metadata)
}
fn circuit_policy(p:&NativeRetryLimits)->CircuitPolicy {
    CircuitPolicy{failure_threshold:p.circuit.failure_threshold,open_ms:p.circuit.open_ms}
}
fn validate_route(task:&Task,key:&CircuitKey,credential:&str)->Result<()> {
    let p=task.input.execution_limits().map_err(|e|Fault::Refused("invalid_limits",e))?;
    key.validate().map_err(|e|Fault::Refused("source_mismatch",e))?;
    if task.input.definition.executor!=ExecutorKind::Native||key.endpoint.len()>p.native.endpoint_bytes
        ||task.input.definition.required_inference().map_err(|e|Fault::Refused("source_mismatch",e.into()))?.model!=key.model
        ||!crate::mcp::valid_identity(credential,accepted_runtime(task)?.transport.identity_bytes)||credential.len()>p.native.api_key_env_bytes {
        return refuse("source_mismatch","native request route or credential configuration differs");
    }
    let url=reqwest::Url::parse(&key.endpoint).map_err(|_|Fault::Refused("source_mismatch","invalid native endpoint".into()))?;
    let loopback=url.host_str().and_then(|h|h.trim_matches(['[',']']).parse::<std::net::IpAddr>().ok()).is_some_and(|ip|ip.is_loopback());
    if url.scheme()!="https"&&!(url.scheme()=="http"&&loopback){return refuse("source_mismatch","native endpoint requires HTTPS")}
    Ok(())
}
fn validate_permit(tx:&Connection,permit:&NativeRequestPermit)->Result<Task> {
    let caller=&permit.circuit.caller;
    let session_id:String=tx.query_row("SELECT json_extract(body,'$.session_id') FROM attempts WHERE id=?1 AND task_id=?2",params![caller.attempt_id,caller.task_id],|r|r.get(0))?;
    let task=super::sessions::historical_task(tx,&caller.task_id,&session_id)?;
    let p=accepted(&task)?;
    validate_route(&task,&permit.circuit.key,&permit.credential_configuration)?;
    let runtime=accepted_runtime(&task)?;
    permit.circuit.caller.validate(&runtime.transport).map_err(Fault::Store)?;
    if !valid_id(runtime,&permit.circuit.operation_id){return Err(Fault::Store("native acquire identity invalid".into()))}
    if permit.request_index==0||permit.circuit.policy!=circuit_policy(p)||permit.auth_cooldown_ms!=p.auth_cooldown_ms {
        return Err(Fault::Store("native permit differs from its accepted policy".into()));
    }
    let c=&permit.circuit.caller;
    let raw:String=tx.query_row("SELECT CAST(substr(CAST(body AS BLOB),1,?3) AS TEXT) FROM attempts WHERE id=?1 AND task_id=?2",
        params![c.attempt_id,c.task_id,sql_bound(runtime.transport.body_bytes.saturating_add(1))?],|r|r.get(0))?;
    if raw.len()>runtime.transport.body_bytes{return Err(Fault::Store("native retained attempt exceeds accepted bound".into()))}
    let a:Attempt=decode(&raw)?;
    if a.id!=c.attempt_id||a.task_id!=c.task_id||a.session_id!=task.session_id||a.generation!=c.generation||a.worker_id!=c.worker_id||a.worker_incarnation!=c.incarnation {
        return Err(Fault::Store("native retained attempt differs from original caller".into()));
    }
    Ok(task)
}
fn original_permit(tx:&Connection,id:&str)->Result<NativeRequestPermit> {
    let receipt=super::metadata::read(tx,id)?.ok_or_else(||Fault::Refused("not_found","original native acquire receipt absent".into()))?;
    let Operation::AcquireNativeRequest{operation_id,caller,request_index,key,credential_configuration}=&receipt.request else {
        return refuse("source_mismatch","original native acquire operation differs");
    };
    let Outcome::Ok{data:Data::NativeRequestAdmission(NativeRequestAdmission::Granted{permit})}=&receipt.outcome else {
        return refuse("source_mismatch","native acquire did not grant a request");
    };
    if operation_id!=id||permit.circuit.operation_id!=id||&permit.circuit.caller!=caller||permit.request_index!=*request_index
        ||&permit.circuit.key!=key||&permit.credential_configuration!=credential_configuration {
        return refuse("source_mismatch","native acquire receipt binding differs");
    }
    validate_permit(tx,permit)?;
    Ok(permit.clone())
}
fn event<T:Serialize>(tx:&Connection,task:&Task,kind:&str,value:&T,p:&NativeRetryLimits)->Result<()> {
    if kind.len()>tx.limits.controller.history_kind_bytes
        ||super::bounded_json_size(value,p.coordination_record_bytes).is_err() {
        return refuse("capacity","native coordination event exceeds configured bound");
    }
    history(tx,task,kind,value)
}
fn current_caller(tx:&Connection,caller:&crate::mcp::Caller,now:u64)->Result<StoredTask> {
    caller.validate(&tx.limits.transport).map_err(|e|Fault::Refused("invalid_input",e))?;
    let value=super::operations::owned(tx,&caller.task_id,&caller.attempt_id,caller.generation,true)?;
    let a=value.task.attempt.as_ref().ok_or_else(||Fault::Store("native caller attempt absent".into()))?;
    if a.worker_id!=caller.worker_id||a.worker_incarnation!=caller.incarnation {
        return refuse("stale_owner","native caller worker binding differs");
    }
    let raw:Option<String>=tx.query_row("SELECT body FROM workers WHERE id=?1",[&caller.worker_id],|r|r.get(0)).optional()?;
    if raw.as_deref().map(decode::<Worker>).transpose()?.is_none_or(|w|w.incarnation!=caller.incarnation) {
        return refuse("stale_owner","native caller worker incarnation is no longer current");
    }
    if value.task.status!=TaskStatus::Active||a.phase!=AttemptPhase::Running||a.uncertain
        ||value.unresolved_execution||value.task.waiting_child_id.is_some() {
        return refuse("reconciliation_required","native caller requires its active certain running attempt");
    }
    if now>=a.deadline_unix_ms||now>=value.task.input.limits.deadline_unix_ms {
        return refuse("deadline","native caller deadline expired");
    }
    Ok(value)
}
fn fenced(tx:&Connection,permit:&NativeRequestPermit,now:u64)->Result<bool> {
    let task=load(tx,&permit.circuit.caller.task_id)?.task;
    let c=&permit.circuit.caller;
    let Some(a)=&task.attempt else{return Ok(true)};
    if a.id!=c.attempt_id||a.generation!=c.generation||a.worker_id!=c.worker_id||a.worker_incarnation!=c.incarnation
        ||a.stopped||a.deadline_unix_ms<=now||task.input.limits.deadline_unix_ms<=now{return Ok(true)}
    let worker:Option<String>=tx.query_row("SELECT body FROM workers WHERE id=?1",[&c.worker_id],|r|r.get(0)).optional()?;
    Ok(worker.as_deref().map(decode::<Worker>).transpose()?.is_none_or(|w|w.incarnation!=c.incarnation))
}
fn settle(
    tx:&Connection,operation_id:&str,permit:&NativeRequestPermit,outcome:NativeRequestOutcome,
    source:NativeSettlementSource,cfg:&ControllerConfig,now:u64,
)->Result<(NativeRequestSettlement,bool)> {
    let storage=selected(cfg)?;
    let task=validate_permit(tx,permit)?;
    let slot_key=request_key(permit)?;
    let mut slot:RequestSlot=read(tx,"request",&slot_key,storage)?.ok_or_else(||Fault::Store("native acquire slot absent".into()))?;
    if slot.permit!=*permit{return Err(Fault::Store("native acquire slot differs from original receipt".into()))}
    if let Some(old)=slot.settlement {
        if old.outcome!=outcome{return refuse("operation_conflict","native request already has a different immutable settlement")}
        return Ok((old,false));
    }
    let key=encode(&permit.circuit.key)?;
    let circuit:CircuitState=read(tx,"circuit",&key,storage)?.ok_or_else(||Fault::Store("native circuit state absent".into()))?;
    let health=match &outcome {
        NativeRequestOutcome::Healthy=>CircuitOutcome::Healthy,
        NativeRequestOutcome::Rejected{rejection} if rejection.kind.same_input_retryable()=>CircuitOutcome::TransientFailure,
        // Neutral circuit health, not proof that a known-rejected POST never
        // started. The immutable slot retains its actual charged rejection.
        NativeRequestOutcome::Rejected{..}|NativeRequestOutcome::AuthenticationFailed|NativeRequestOutcome::NotStarted=>CircuitOutcome::NotStarted,
        NativeRequestOutcome::Unknown=>CircuitOutcome::Unknown,
    };
    let completion=super::native_retry_state::complete(&circuit,&permit.circuit,health,now).map_err(|e|Fault::Refused("source_mismatch",e))?;
    let akey=auth_key(&permit.circuit.key,&permit.credential_configuration);
    let akey_raw=encode(&akey)?;
    let mut auth:Authentication=read(tx,"authentication",&akey_raw,storage)?.ok_or_else(||Fault::Store("native authentication state absent".into()))?;
    if auth.key!=akey||permit.auth_generation>auth.generation{return Err(Fault::Store("native authentication generation differs".into()))}
    if permit.auth_generation==auth.generation {
        match &auth.phase {
            AuthPhase::Ready if !permit.auth_probe=>{},
            AuthPhase::Probe{permit:original} if permit.auth_probe&&original==permit=>{},
            _=>return Err(Fault::Store("native authentication permit does not own its phase".into())),
        }
        if matches!(&outcome,NativeRequestOutcome::AuthenticationFailed)
            ||matches!(&outcome,NativeRequestOutcome::Rejected{rejection} if rejection.kind==crate::native::RejectionKind::AuthenticationFailed)
            ||permit.auth_probe&&matches!(outcome,NativeRequestOutcome::Unknown|NativeRequestOutcome::NotStarted) {
            let until_ms=now.checked_add(permit.auth_cooldown_ms).ok_or_else(||Fault::Refused("capacity","native auth cooldown clock exhausted".into()))?;
            auth.generation=auth.generation.checked_add(1).ok_or_else(||Fault::Refused("capacity","native auth generation exhausted".into()))?;
            auth.phase=AuthPhase::Cooldown{until_ms};
        } else if permit.auth_probe {
            auth.generation=auth.generation.checked_add(1).ok_or_else(||Fault::Refused("capacity","native auth generation exhausted".into()))?;
            auth.phase=AuthPhase::Ready;
        }
    }
    let result=NativeRequestSettlement{operation_id:operation_id.into(),permit:permit.clone(),outcome,source,
        circuit:completion.disposition,
        circuit_retry_at_ms:match &completion.state.phase{CircuitPhase::Open{until_ms}=>Some(*until_ms),_=>None},
        auth_retry_at_ms:match &auth.phase{AuthPhase::Cooldown{until_ms}=>Some(*until_ms),_=>None}};
    slot.settlement=Some(result.clone());
    write(tx,"circuit",&key,&completion.state,storage,storage.circuit.max_entries)?;
    write(tx,"authentication",&akey_raw,&auth,storage,storage.auth_max_entries)?;
    write(tx,"request",&slot_key,&slot,storage,cfg.limits.controller.max_receipts)?;
    event(tx,&task,if result.source==NativeSettlementSource::FencedOwner{"native_request_owner_lost"}else{"native_request_settled"},&result,storage)?;
    Ok((result,true))
}

/// Reconcile only a precise held probe after its canonical owner is fenced.
/// Unknown remains immutable in the original request slot; no effect is replayed.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A new circuit-breaker probe after a prior unknown outcome MUST NOT replay the original uncertain request.
fn reconcile_probe(tx:&Connection,id:&str,by:&str,cfg:&ControllerConfig,now:u64)->Result<bool> {
    let permit=original_permit(tx,id)?;
    if !fenced(tx,&permit,now)?{return Ok(false)}
    let storage=selected(cfg)?;
    let slot:RequestSlot=read(tx,"request",&request_key(&permit)?,storage)?.ok_or_else(||Fault::Store("native probe slot absent".into()))?;
    if slot.settlement.is_some(){return Err(Fault::Store("settled native probe still owns health state".into()))}
    Ok(settle(tx,by,&permit,NativeRequestOutcome::Unknown,NativeSettlementSource::FencedOwner,cfg,now)?.1)
}

// Readiness is observation of the original blocker, never reconciliation,
// provider replay authority, or a new request-slot claim.
fn observe(tx:&Connection,caller:&crate::mcp::Caller,id:&str,cfg:&ControllerConfig,now:u64)->Result<Data> {
    let current=current_caller(tx,caller,now)?;
    accepted(&current.task)?;
    let runtime=accepted_runtime(&current.task)?;
    if !valid_id(runtime,id){return refuse("invalid_input","native readiness acquire identity invalid")}
    let accepted_tx=tx.with_limits(runtime);
    let receipt=super::metadata::read(&accepted_tx,id)?
        .ok_or_else(||Fault::Refused("not_found","original native wait receipt absent".into()))?;
    let Operation::AcquireNativeRequest{operation_id,caller:original,request_index,key,credential_configuration}=&receipt.request else {
        return refuse("source_mismatch","native readiness source is not an acquire");
    };
    if operation_id!=id||original!=caller||*request_index==0 {
        return refuse("source_mismatch","native readiness original caller or request differs");
    }
    validate_route(&current.task,key,credential_configuration)?;
    let Outcome::Ok{data:Data::NativeRequestAdmission(NativeRequestAdmission::Wait{reason,retry_at_ms,generation})}=&receipt.outcome else {
        return refuse("source_mismatch","native readiness source did not commit a wait");
    };
    let charged:u32=tx.query_row("SELECT model_requests FROM attempts WHERE id=?1",[&caller.attempt_id],|r|r.get(0))?;
    if charged.checked_add(1)!=Some(*request_index) {
        return refuse("source_mismatch","native readiness no longer precedes its original request");
    }
    let storage=selected(cfg)?;
    let changed=match reason {
        NativeWaitReason::CircuitOpen|NativeWaitReason::CircuitProbePending=>{
            let state:CircuitState=read(tx,"circuit",&encode(key)?,storage)?
                .ok_or_else(||Fault::Store("native readiness circuit absent".into()))?;
            super::native_retry_state::validate_state(&state).map_err(Fault::Store)?;
            if state.key!=*key||state.generation<*generation {
                return Err(Fault::Store("native readiness circuit identity or generation differs".into()));
            }
            match reason {
                NativeWaitReason::CircuitOpen=>{
                    let due=retry_at_ms.filter(|due|*due>0)
                        .ok_or_else(||Fault::Store("native circuit wait lacks its original due time".into()))?;
                    if state.generation!=*generation {true} else {match &state.phase {
                        CircuitPhase::Open{until_ms}=>{
                            if *until_ms!=due{return Err(Fault::Store("native circuit wait due time differs".into()))}
                            now>=due
                        },
                        _=>true,
                    }}
                },
                NativeWaitReason::CircuitProbePending=>{
                    if retry_at_ms.is_some(){return Err(Fault::Store("native pending probe has a fabricated due time".into()))}
                    if state.generation!=*generation {true} else {match &state.phase {
                        CircuitPhase::HalfOpen{probe}=>{
                            let permit=original_permit(tx,&probe.operation_id)?;
                            if permit.circuit!=*probe{return Err(Fault::Store("native readiness held circuit probe differs".into()))}
                            probe_fenced(tx,&permit,storage,now)?
                        },
                        _=>true,
                    }}
                },
                _=>unreachable!(),
            }
        },
        NativeWaitReason::AuthenticationCooldown|NativeWaitReason::AuthenticationProbePending=>{
            let key=auth_key(key,credential_configuration);
            let state:Authentication=read(tx,"authentication",&encode(&key)?,storage)?
                .ok_or_else(||Fault::Store("native readiness authentication state absent".into()))?;
            if state.key!=key||state.generation<*generation {
                return Err(Fault::Store("native readiness authentication identity or generation differs".into()));
            }
            match reason {
                NativeWaitReason::AuthenticationCooldown=>{
                    let due=retry_at_ms.filter(|due|*due>0)
                        .ok_or_else(||Fault::Store("native authentication wait lacks its original due time".into()))?;
                    if state.generation!=*generation {true} else {match &state.phase {
                        AuthPhase::Cooldown{until_ms}=>{
                            if *until_ms!=due{return Err(Fault::Store("native authentication wait due time differs".into()))}
                            now>=due
                        },
                        _=>true,
                    }}
                },
                NativeWaitReason::AuthenticationProbePending=>{
                    if retry_at_ms.is_some(){return Err(Fault::Store("native pending auth probe has a fabricated due time".into()))}
                    if state.generation!=*generation {true} else {match &state.phase {
                        AuthPhase::Probe{permit}=>{
                            if !permit.auth_probe||permit.auth_generation!=state.generation
                                ||auth_key(&permit.circuit.key,&permit.credential_configuration)!=state.key
                                ||original_permit(tx,&permit.circuit.operation_id)?!=*permit {
                                return Err(Fault::Store("native readiness held authentication probe differs".into()));
                            }
                            probe_fenced(tx,permit,storage,now)?
                        },
                        _=>true,
                    }}
                },
                _=>unreachable!(),
            }
        },
    };
    Ok(Data::NativeRequestReadiness{acquire_operation_id:id.into(),changed})
}

fn probe_fenced(tx:&Connection,permit:&NativeRequestPermit,storage:&NativeRetryLimits,now:u64)->Result<bool> {
    let slot:RequestSlot=read(tx,"request",&request_key(permit)?,storage)?
        .ok_or_else(||Fault::Store("native readiness probe slot absent".into()))?;
    if slot.permit!=*permit||slot.settlement.is_some() {
        return Err(Fault::Store("native readiness held probe slot differs or is settled".into()));
    }
    fenced(tx,permit,now)
}

//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# Retry timing, jitter and circuit-breaker bounds MUST come from versioned metadata.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# The platform MUST maintain one coordinated circuit breaker for each configured model and endpoint.
fn acquire(tx:&Connection,op:&Operation,cfg:&ControllerConfig,now:u64)->Result<(Data,bool)> {
    let Operation::AcquireNativeRequest{operation_id,caller,request_index,key,credential_configuration}=op else{unreachable!()};
    let current=current_caller(tx,caller,now)?;
    let p=accepted(&current.task)?;
    validate_route(&current.task,key,credential_configuration)?;
    if *request_index==0{return refuse("invalid_input","native request index must be positive")}
    let storage=selected(cfg)?;
    let skey=encode(&(&caller.attempt_id,*request_index))?;
    if let Some(slot)=read::<RequestSlot>(tx,"request",&skey,storage)? {
        if slot.permit.circuit.caller!=*caller||slot.permit.circuit.key!=*key||slot.permit.credential_configuration!=*credential_configuration {
            return refuse("operation_conflict","native request index already binds a different grant");
        }
        if original_permit(tx,&slot.permit.circuit.operation_id)?!=slot.permit{return Err(Fault::Store("native slot differs from original acquire".into()))}
        return Ok((Data::NativeRequestAdmission(NativeRequestAdmission::Granted{permit:slot.permit}),false));
    }
    let charged:u32=tx.query_row("SELECT model_requests FROM attempts WHERE id=?1",[&caller.attempt_id],|r|r.get(0))?;
    if charged.checked_add(1)!=Some(*request_index){return refuse("source_mismatch","native acquire must precede the next Usage reservation")}
    if super::operations::used_model_requests(tx,&caller.task_id)?>=u64::from(current.task.input.limits.model_requests) {
        return refuse("capacity","native request budget exhausted");
    }
    if charged>0 {
        let previous:RequestSlot=read(tx,"request",&encode(&(&caller.attempt_id,charged))?,storage)?
            .ok_or_else(||Fault::Refused("reconciliation_required","previous charged native request lacks coordination".into()))?;
        if previous.settlement.as_ref().is_none_or(|s|matches!(s.outcome,NativeRequestOutcome::Unknown)) {
            return refuse("reconciliation_required","previous native request remains unsettled or unknown");
        }
        // A known rejection is immutable evidence, not another POST grant.
        // Fatal classes end this attempt's request chain even after shared
        // health cooldown has elapsed or the credential configuration changes.
        if previous.settlement.as_ref().is_some_and(|s|match &s.outcome {
            NativeRequestOutcome::Rejected{rejection}=>!rejection.kind.same_input_retryable(),
            NativeRequestOutcome::AuthenticationFailed=>true,
            _=>false,
        }) {
            return refuse("reconciliation_required","previous native rejection does not permit another request in this attempt");
        }
    }
    let ckey=encode(key)?;let akey=auth_key(key,credential_configuration);let akey_raw=encode(&akey)?;
    let mut circuit:Option<CircuitState>=read(tx,"circuit",&ckey,storage)?;
    let mut auth:Authentication=read(tx,"authentication",&akey_raw,storage)?.unwrap_or(Authentication{key:akey.clone(),generation:0,phase:AuthPhase::Ready});
    if auth.key!=akey{return Err(Fault::Store("native authentication key differs".into()))}
    let mut probes=std::collections::BTreeSet::new();
    if let Some(state)=&circuit {
        super::native_retry_state::validate_state(state).map_err(Fault::Store)?;
        if state.key!=*key{return Err(Fault::Store("native circuit state key differs".into()))}
        if let CircuitPhase::HalfOpen{probe}=&state.phase {
            if original_permit(tx,&probe.operation_id)?.circuit!=*probe{return Err(Fault::Store("native held circuit probe differs from original grant".into()))}
            probes.insert(probe.operation_id.clone());
        }
    }
    if let AuthPhase::Probe{permit}=&auth.phase {
        if !permit.auth_probe||permit.auth_generation!=auth.generation||auth_key(&permit.circuit.key,&permit.credential_configuration)!=auth.key
            ||original_permit(tx,&permit.circuit.operation_id)?!=*permit {
            return Err(Fault::Store("native held authentication probe differs from original grant".into()));
        }
        probes.insert(permit.circuit.operation_id.clone());
    }
    let mut reconciled=false;
    for id in probes{reconciled|=reconcile_probe(tx,&id,operation_id,cfg,now)?;}
    if reconciled {
        circuit=read(tx,"circuit",&ckey,storage)?;
        // The held circuit probe may belong to another credential configuration.
        // Reconciliation changes that auth key, not an as-yet unused current key.
        auth=read(tx,"authentication",&akey_raw,storage)?.unwrap_or(Authentication{key:akey.clone(),generation:0,phase:AuthPhase::Ready});
    }
    let auth_probe=match &auth.phase {
        AuthPhase::Cooldown{until_ms} if now<*until_ms=>return Ok((Data::NativeRequestAdmission(NativeRequestAdmission::Wait{reason:NativeWaitReason::AuthenticationCooldown,retry_at_ms:Some(*until_ms),generation:auth.generation}),reconciled)),
        AuthPhase::Probe{..}=>return Ok((Data::NativeRequestAdmission(NativeRequestAdmission::Wait{reason:NativeWaitReason::AuthenticationProbePending,retry_at_ms:None,generation:auth.generation}),reconciled)),
        AuthPhase::Cooldown{..}=>true,AuthPhase::Ready=>false,
    };
    let planned=super::native_retry_state::acquire(circuit.as_ref(),key,caller,operation_id,&circuit_policy(p),now).map_err(|e|Fault::Refused("capacity",e))?;
    let circuit_permit=match planned.admission {
        CircuitAdmission::Admitted{permit}=>permit,
        CircuitAdmission::Open{retry_at_ms}=>return Ok((Data::NativeRequestAdmission(NativeRequestAdmission::Wait{reason:NativeWaitReason::CircuitOpen,retry_at_ms:Some(retry_at_ms),generation:planned.state.generation}),reconciled)),
        CircuitAdmission::ProbePending{..}=>return Ok((Data::NativeRequestAdmission(NativeRequestAdmission::Wait{reason:NativeWaitReason::CircuitProbePending,retry_at_ms:None,generation:planned.state.generation}),reconciled)),
    };
    let permit=NativeRequestPermit{circuit:circuit_permit,request_index:*request_index,credential_configuration:credential_configuration.clone(),
        auth_generation:auth.generation,auth_probe,auth_cooldown_ms:p.auth_cooldown_ms};
    if auth_probe{auth.phase=AuthPhase::Probe{permit:permit.clone()};}
    write(tx,"circuit",&ckey,&planned.state,storage,storage.circuit.max_entries)?;
    write(tx,"authentication",&akey_raw,&auth,storage,storage.auth_max_entries)?;
    write(tx,"request",&skey,&RequestSlot{permit:permit.clone(),settlement:None},storage,cfg.limits.controller.max_receipts)?;
    event(tx,&current.task,"native_request_granted",&permit,storage)?;
    Ok((Data::NativeRequestAdmission(NativeRequestAdmission::Granted{permit}),true))
}

// The compatibility AuthenticationFailed outcome is not an evidence shortcut.
// Every actual HTTP authentication failure still needs the original charged,
// canonical rejection report; new workers retain Rejected with its exact kind.
fn rejection_proof(tx:&Connection,permit:&NativeRequestPermit,outcome:&NativeRequestOutcome)->Result<Option<u64>> {
    let expected=match outcome {
        NativeRequestOutcome::Rejected{rejection}=>Some(rejection),
        NativeRequestOutcome::AuthenticationFailed=>None,
        _=>return Ok(None),
    };
    let proof=super::operations::native_retry::confirmed_rejection(tx,&permit.circuit.caller,permit.request_index)?
        .ok_or_else(||Fault::Refused("source_mismatch","native rejection settlement lacks original report".into()))?;
    if proof.endpoint!=permit.circuit.key.endpoint||proof.model!=permit.circuit.key.model
        ||expected.is_some_and(|rejection|proof.rejection!=*rejection)
        ||expected.is_none()&&proof.rejection.kind!=crate::native::RejectionKind::AuthenticationFailed {
        return refuse("source_mismatch","native rejection settlement differs from original report");
    }
    Ok(Some(proof.controller_revision))
}

pub(super) fn apply(tx:&Connection,op:&Operation,cfg:&ControllerConfig,now:u64)->Result<(Data,bool)> {
    match op {
        Operation::ObserveNativeRequest{caller,acquire_operation_id}=>{
            let data=observe(tx,caller,acquire_operation_id,cfg,now)?;
            let after=(cfg.clock)();
            if after<now{return refuse("clock_unavailable","native readiness clock moved backwards")}
            current_caller(tx,caller,after)?;
            Ok((data,false))
        },
        Operation::AcquireNativeRequest{caller,..}=>{
            let result=acquire(tx,op,cfg,now)?;
            let after=(cfg.clock)();
            if after<now{return refuse("clock_unavailable","native acquisition clock moved backwards")}
            current_caller(tx,caller,after)?;
            Ok(result)
        },
        Operation::SettleNativeRequest{operation_id,caller,acquire_operation_id,outcome}=>{
            caller.validate(&tx.limits.transport).map_err(|e|Fault::Refused("invalid_input",e))?;
            let permit=original_permit(tx,acquire_operation_id)?;
            if permit.circuit.caller!=*caller{return refuse("stale_owner","native settlement caller differs from original permit")}
            let current=super::operations::owned(tx,&caller.task_id,&caller.attempt_id,caller.generation,false)?;
            let a=current.task.attempt.as_ref().ok_or_else(||Fault::Refused("stale_owner","native settlement attempt absent".into()))?;
            if a.worker_id!=caller.worker_id||a.worker_incarnation!=caller.incarnation{return refuse("stale_owner","native settlement worker differs")}
            let slot:RequestSlot=read(tx,"request",&request_key(&permit)?,selected(cfg)?)?
                .ok_or_else(||Fault::Store("native settlement slot absent".into()))?;
            if slot.permit!=permit{return Err(Fault::Store("native settlement slot differs from original grant".into()))}
            if slot.settlement.as_ref().is_some_and(|old|old.outcome!=*outcome) {
                return refuse("operation_conflict","native request already has a different immutable settlement");
            }
            if !matches!(outcome,NativeRequestOutcome::NotStarted) {
                super::operations::native_retry::reservation(tx,caller,permit.request_index)?;
            }
            rejection_proof(tx,&permit,outcome)?;
            let (result,changed)=settle(tx,operation_id,&permit,outcome.clone(),NativeSettlementSource::Reported,cfg,now)?;
            Ok((Data::NativeRequestSettlement(result),changed))
        },
        _=>Err(Fault::Store("wrong native coordination operation route".into())),
    }
}

fn validate_settlement(tx:&Connection,slot:&RequestSlot,p:&NativeRetryLimits)->Result<()> {
    let Some(value)=&slot.settlement else{return Ok(())};
    if value.permit!=slot.permit{return Err(Fault::Store("native settlement permit differs from its slot".into()))}
    let original=super::metadata::read(tx,&value.operation_id)?
        .ok_or_else(||Fault::Store("native settlement original receipt absent".into()))?;
    let acquisition=super::metadata::read(tx,&slot.permit.circuit.operation_id)?
        .ok_or_else(||Fault::Store("native settlement original acquire absent".into()))?;
    if acquisition.commit.controller_revision>=original.commit.controller_revision {
        return Err(Fault::Store("native settlement does not follow its original acquire".into()));
    }
    match value.source {
        NativeSettlementSource::Reported=>{
            if !matches!(&original.request,Operation::SettleNativeRequest{operation_id,caller,acquire_operation_id,outcome}
                if operation_id==&value.operation_id&&caller==&slot.permit.circuit.caller
                    &&acquire_operation_id==&slot.permit.circuit.operation_id&&outcome==&value.outcome)
                ||!matches!(&original.outcome,Outcome::Ok{data:Data::NativeRequestSettlement(result)} if result==value) {
                return Err(Fault::Store("native settlement differs from its original receipt".into()));
            }
            if !matches!(&value.outcome,NativeRequestOutcome::NotStarted) {
                let reservation=super::operations::native_retry::reservation(tx,&slot.permit.circuit.caller,slot.permit.request_index)?;
                if reservation<=acquisition.commit.controller_revision||reservation>=original.commit.controller_revision {
                    return Err(Fault::Store("native settlement does not follow its original charge".into()));
                }
            }
            if let Some(revision)=rejection_proof(tx,&slot.permit,&value.outcome)? {
                if revision>=original.commit.controller_revision {
                    return Err(Fault::Store("native rejected settlement differs from original report".into()));
                }
            }
        },
        NativeSettlementSource::FencedOwner=>{
            if value.outcome!=NativeRequestOutcome::Unknown
                ||!matches!(&original.request,Operation::AcquireNativeRequest{operation_id,..} if operation_id==&value.operation_id)
                ||!matches!(&original.outcome,Outcome::Ok{data:Data::NativeRequestAdmission(_)}) {
                return Err(Fault::Store("native owner-loss receipt does not preserve Unknown".into()));
            }
            // The triggering acquire receipt and exact immutable history event
            // jointly retain the old permit; no provider result is manufactured.
            let mut query=tx.prepare("SELECT CAST(substr(CAST(body AS BLOB),1,?4) AS TEXT) FROM history WHERE task_id=?1 AND kind='native_request_owner_lost' AND json_extract(body,'$.operation_id')=?2 AND json_extract(body,'$.permit.circuit.operation_id')=?3 LIMIT 2")?;
            let values=query.query_map(params![slot.permit.circuit.caller.task_id,value.operation_id,slot.permit.circuit.operation_id,
                sql_bound(p.coordination_record_bytes.saturating_add(1))?],|r|r.get::<_,String>(0))?
                .collect::<std::result::Result<Vec<_>,_>>()?;
            if values.len()!=1||values[0].len()>p.coordination_record_bytes
                ||decode::<NativeRequestSettlement>(&values[0])?!=*value {
                return Err(Fault::Store("native owner-loss settlement differs from original event".into()));
            }
        },
    }
    Ok(())
}

#[cfg(test)]
mod key_tests {
    use super::*;

    #[test]
    fn authentication_dependency_omits_model_but_keeps_exact_endpoint_and_configuration() {
        // Pure dependency-key contract, not an attempt to admit an unsupported
        // model through the current selected-model definition policy.
        let first=CircuitKey{endpoint:"https://example.invalid/v1/responses".into(),model:"one".into()};
        let mut second=first.clone();second.model="two".into();
        assert_eq!(auth_key(&first,"configuration-1"),auth_key(&second,"configuration-1"));
        assert_ne!(auth_key(&first,"configuration-1"),auth_key(&second,"configuration-2"));
        second.endpoint="https://other.invalid/v1/responses".into();
        assert_ne!(auth_key(&first,"configuration-1"),auth_key(&second,"configuration-1"));
    }
}

/// Read-only startup validation. Root calls this only when the tagged table
/// exists, before migration/reconciliation writes. No missing row is recreated.
pub(crate) fn validate_stored(tx:&Connection,cfg:&ControllerConfig)->Result<()> {
    let any:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM native_request_coordination)",[],|r|r.get(0))?;
    if !any{return Ok(())}
    let p=selected(cfg)?;
    p.validate().map_err(Fault::Store)?;
    let maximum=p.circuit.max_entries.checked_add(p.auth_max_entries)
        .and_then(|n|n.checked_add(cfg.limits.controller.max_receipts))
        .and_then(|n|n.checked_add(1)).ok_or_else(||Fault::Store("native coordination inventory bound overflow".into()))?;
    let count:i64=tx.query_row("SELECT count(*) FROM (SELECT 1 FROM native_request_coordination LIMIT ?1)",
        [sql_bound(maximum)?],|r|r.get(0))?;
    if unsigned(count)?>=(maximum as u64){return Err(Fault::Store("native coordination inventory exceeds selected capacity".into()))}
    let unsupported:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM native_request_coordination WHERE kind NOT IN ('circuit','authentication','request'))",[],|r|r.get(0))?;
    if unsupported{return Err(Fault::Store("native coordination row kind unsupported".into()))}
    let mut circuits=std::collections::BTreeMap::<String,CircuitState>::new();
    let mut authentications=std::collections::BTreeMap::<String,Authentication>::new();
    let mut slots=std::collections::BTreeMap::<String,RequestSlot>::new();
    for (kind,maximum) in [("circuit",p.circuit.max_entries),("authentication",p.auth_max_entries),("request",cfg.limits.controller.max_receipts)] {
        let mut query=tx.prepare("SELECT CAST(substr(CAST(key AS BLOB),1,?2) AS TEXT),CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM native_request_coordination WHERE kind=?1 ORDER BY key LIMIT ?3")?;
        let mut rows=query.query(params![kind,sql_bound(p.coordination_record_bytes.saturating_add(1))?,sql_bound(maximum.saturating_add(1))?])?;
        let mut count=0usize;
        while let Some(row)=rows.next()? {
            count=count.checked_add(1).ok_or_else(||Fault::Store("native coordination inventory overflow".into()))?;
            if count>maximum{return Err(Fault::Store("native coordination inventory exceeds selected capacity".into()))}
            let key:String=row.get(0)?;let body:String=row.get(1)?;
            if key.len()>p.coordination_record_bytes||body.len()>p.coordination_record_bytes {
                return Err(Fault::Store("native coordination record exceeds selected byte bound".into()));
            }
            match kind {
                "circuit"=>{
                    let value:CircuitState=decode(&body)?;
                    super::native_retry_state::validate_state(&value).map_err(Fault::Store)?;
                    if encode(&value.key)?!=key{return Err(Fault::Store("native circuit row key differs".into()))}
                    circuits.insert(key,value);
                },
                "authentication"=>{
                    let value:Authentication=decode(&body)?;
                    if encode(&value.key)?!=key{return Err(Fault::Store("native authentication row key differs".into()))}
                    match &value.phase {
                        AuthPhase::Cooldown{until_ms} if *until_ms==0=>return Err(Fault::Store("native authentication cooldown invalid".into())),
                        AuthPhase::Probe{permit} if !permit.auth_probe||permit.auth_generation!=value.generation
                            ||auth_key(&permit.circuit.key,&permit.credential_configuration)!=value.key=>
                            return Err(Fault::Store("native authentication probe differs from phase".into())),
                        _=>{},
                    }
                    authentications.insert(key,value);
                },
                "request"=>{
                    let value:RequestSlot=decode(&body)?;
                    if request_key(&value.permit)?!=key||original_permit(tx,&value.permit.circuit.operation_id)?!=value.permit {
                        return Err(Fault::Store("native request slot differs from original acquire".into()));
                    }
                    validate_settlement(tx,&value,p)?;
                    slots.insert(key,value);
                },
                _=>unreachable!(),
            }
        }
    }
    let mut referenced_circuits=std::collections::BTreeSet::new();
    let mut referenced_auth=std::collections::BTreeSet::new();
    for slot in slots.values() {
        let permit=&slot.permit;
        let ckey=encode(&permit.circuit.key)?;
        let akey=encode(&auth_key(&permit.circuit.key,&permit.credential_configuration))?;
        let circuit=circuits.get(&ckey).ok_or_else(||Fault::Store("native request circuit absent".into()))?;
        let auth=authentications.get(&akey).ok_or_else(||Fault::Store("native request authentication state absent".into()))?;
        if permit.circuit.generation>circuit.generation||permit.auth_generation>auth.generation {
            return Err(Fault::Store("native request generation exceeds retained health state".into()));
        }
        referenced_circuits.insert(ckey);referenced_auth.insert(akey);
    }
    if referenced_circuits.len()!=circuits.len()||referenced_auth.len()!=authentications.len() {
        return Err(Fault::Store("native health state lacks an original request slot".into()));
    }
    for state in circuits.values() {
        if let CircuitPhase::HalfOpen{probe}=&state.phase {
            // Request index belongs to the canonical acquire receipt, never to
            // an inferred operation-ID layout or the current attempt counter.
            let permit=original_permit(tx,&probe.operation_id)?;
            let slot=slots.get(&request_key(&permit)?).ok_or_else(||Fault::Store("native circuit probe slot absent".into()))?;
            if permit.circuit!=*probe||slot.permit!=permit||slot.settlement.is_some() {
                return Err(Fault::Store("native circuit probe differs from unsettled original grant".into()));
            }
        }
    }
    for state in authentications.values() {
        if let AuthPhase::Probe{permit}=&state.phase {
            let slot=slots.get(&request_key(permit)?).ok_or_else(||Fault::Store("native authentication probe slot absent".into()))?;
            if slot.permit!=*permit||slot.settlement.is_some() {
                return Err(Fault::Store("native authentication probe differs from unsettled original grant".into()));
            }
        }
    }
    Ok(())
}
