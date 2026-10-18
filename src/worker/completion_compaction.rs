//! Actual compaction request retry orchestration shared by original Call and
//! Final carriers; every charged request retains its distinct canonical proof.
use super::*;

#[derive(Clone)]
pub(super) enum CompactionCarrier {
    Call {call:native::NativeLoopCall,output:String},
    Final(native::NativeLoopFinal),
}
impl CompactionCarrier {
    fn compaction_input(&self) -> Result<serde_json::Value,String> {
        match self {Self::Call {call,output}=>call.compaction_input(output),Self::Final(value)=>value.compaction_input()}
    }
    fn after_rejected_request(&self,index:u32) -> Result<Self,String> {
        Ok(match self {
            Self::Call {call,output}=>Self::Call {call:call.after_rejected_request(index)?,output:output.clone()},
            Self::Final(value)=>Self::Final(value.after_rejected_request(index)?),
        })
    }
    fn after_compaction(&self,index:u32,summary:Option<&str>) -> Result<Self,String> {
        Ok(match self {
            Self::Call {call,output}=>Self::Call {call:call.after_compaction(index,summary)?,output:output.clone()},
            Self::Final(value)=>Self::Final(value.after_compaction(index,summary)?),
        })
    }
}

/// A committed block is distinct from no due work. The caller must propagate
/// it as AttemptExit::Blocked and cannot continue to a model/tool/result action.
pub(super) enum CompactionProgress {
    NotDue,
    Completed { carrier: CompactionCarrier, requests: Vec<contract::NativeRequestEvidence> },
    Blocked,
}

fn with_secondary<T>(primary: Result<T,String>, secondary: Result<(),String>, label:&str) -> Result<T,String> {
    match (primary,secondary) {
        (value,Ok(()))=>value,
        (Ok(_),Err(error))=>Err(format!("{label}: {error}")),
        (Err(original),Err(error))=>Err(format!("{original}; {label}: {error}")),
    }
}

fn append_evidence(requests:&mut Vec<contract::NativeRequestEvidence>, entry:contract::NativeRequestEvidence,
    input:&TaskInput)->Result<(),String> {
    requests.push(entry);
    bounded_json_size(requests,input.limits.result_bytes).map_err(|_|"native compaction request evidence exceeds task_result_bytes".to_string())?;
    Ok(())
}

/// The immutable input context and first actual serialized body binding survive
/// retries. Each physical POST has its own acquire, debit and original charter
/// run; a known rejection is reconciled without manufacturing a reply/summary.
pub(super) async fn compact_if_due(
    reporter: &Reporter, input: &TaskInput, state: &Path, control: &Control,
    cfg: &native::NativeConfig, host: &native::ValidationHost, usage: &mut Usage,
    previous: &CompactionCarrier, first_index: u32,
) -> Result<CompactionProgress, String> {
    use crate::charter::{CharterObservation, CharterOutcome, CompactionDisposition};
    use crate::native_retry::NativeRequestOutcome;
    use super::super::native_retry::{self, RetryAction};
    if first_index >= input.limits.model_requests { return Ok(CompactionProgress::NotDue); }
    // Keep substantial child futures out of this poll frame as well as the
    // native loop frame; an outer box alone does not bound nested polling.
    let Some(mut binding) = boxed_execution(|| charter::session(reporter,input,control)).await? else { return Ok(CompactionProgress::NotDue); };
    if binding.session.compaction_due.is_none() { return Ok(CompactionProgress::NotDue); }
    if binding.session.compaction_in_flight.is_some() {
        return Err("charter compaction outcome unresolved; no automatic resend".into());
    }
    let context = previous.compaction_input()?;
    let mut carrier = previous.clone();
    let mut requests = Vec::new();
    let mut original_source = None;
    let mut retry_binding = None;
    let mut rejected_posts = 0u32;
    let mut index = first_index;
    loop {
        // A successful summary still needs an ordinary continuation slot. The
        // prior committed charge is the actual blocking source, not a new debit.
        if index >= input.limits.model_requests {
            let previous_index=index.checked_sub(1).ok_or("native request underflow")?;
            boxed_execution(|| native_retry::request_exhausted(reporter,control,state,previous_index)).await?;
            return Ok(CompactionProgress::Blocked);
        }
        if let Some(source) = &original_source {
            let target=binding.session.pending.as_ref().unwrap_or(&binding.session.effective);
            if target != source {
                return Err("compaction retry source changed; original rejection retained and next POST withheld".into());
            }
        }
        let mut request_diagnostic=NativeDiagnosticRequest::preflight(reporter,input,cfg,index)?;
        match boxed_execution(|| native_retry::acquire(reporter,input,control,state,cfg,index)).await? {
            RetryAction::Retry=>{},
            RetryAction::Blocked=>return Ok(CompactionProgress::Blocked),
        }
        let operation_id=format!("{}-charter-compaction-{index}",reporter.attempt.id);
        let prepared=boxed_execution(|| async {
            boxed_execution(|| reserve(reporter,input,control,usage,index,&mut request_diagnostic)).await?;
            let original=match boxed_execution(|| charter::mutate(reporter,state,control,operation_id.clone(),
                CharterObservation::BeginCompaction {session_id:reporter.attempt.session_id.clone(),
                    expected_revision:binding.session.revision,request_index:index})).await? {
                CharterOutcome::Compaction(value) if value.caller==charter::caller(&reporter.attempt)
                    && value.session_id==reporter.attempt.session_id && value.request_index==index
                    && value.run_id==operation_id && value.reply.is_none()=>value,
                _=>return Err("charter compaction admission differs".to_string()),
            };
            if original_source.as_ref().is_some_and(|source|source!=&original.effective.reference) {
                return Err("compaction retry admitted another source; POST withheld".into());
            }
            let exchange=charter::exchange(&original.effective,true,input)?;
            Ok((original,exchange))
        }).await;
        let (original,exchange)=match prepared {
            Ok(value)=>value,
            Err(error)=>{
                let retained=request_diagnostic.unstarted(reporter);
                let primary=with_secondary(Err(error),boxed_execution(|| native_retry::settle(reporter,input,state,index,NativeRequestOutcome::NotStarted)).await,
                    "native compaction NotStarted settlement failed");
                return with_secondary(primary,boxed_execution(|| acknowledge_native_diagnostic(reporter,control,retained)).await,
                    "native terminal diagnostic failed");
            },
        };
        if original_source.is_none() {original_source=Some(original.effective.reference.clone());}
        let request_host=host.for_request(retry_binding.clone()).with_charter(exchange.clone());
        let mut stopped=control.clone();
        let observed=boxed_execution(|| native::execute_charter_compaction(input,cfg,&context,&request_host,
            async {boxed_execution(|| current(reporter,input,control)).await.map(|_|())},stopped.stopped())).await;
        // Record uncertain exposure before any fallible receipt/usage operation.
        if observed.provider_outcome==native::ProviderOutcome::Unknown {
            reporter.provider_uncertain.store(true,Ordering::Relaxed);
        }
        let retained=request_diagnostic.retain(reporter,&request_host,observed.provider_outcome,observed.diagnostic.as_ref());
        let usage_result=boxed_execution(|| observed_usage(reporter,usage,index,observed.usage,observed.provider_outcome)).await;
        if let native::ProviderOutcome::Rejected(rejection)=observed.provider_outcome {
            let diagnostic=observed.result.as_ref().err().cloned().unwrap_or_else(||"rejected compaction unexpectedly returned a result".into());
            let actual_binding=match request_host.request_binding().and_then(|value|
                value.ok_or_else(||"rejected compaction original body binding absent".to_string())) {
                Ok(value)=>value,
                Err(error)=>{
                    // A known rejected POST still settles health even if the
                    // independently captured exact input binding is unavailable.
                    // Without it there is no rejection receipt or D reconciliation.
                    let primary=Err(format!("{diagnostic}; original compaction input binding failed: {error}"));
                    let primary=with_secondary(primary,usage_result,"native compaction usage recording failed");
                    return with_secondary(primary,boxed_execution(|| native_retry::settle(reporter,input,state,index,
                        NativeRequestOutcome::Rejected {rejection})).await,"native compaction rejected health settlement failed");
                },
            };
            let diagnostic_ack=boxed_execution(|| acknowledge_native_diagnostic(reporter,control,retained)).await;
            if let Err(error)=diagnostic_ack {
                let primary=with_secondary(Err(error),usage_result,"native compaction usage recording failed");
                return with_secondary(primary,boxed_execution(|| native_retry::settle(reporter,input,state,index,
                    NativeRequestOutcome::Rejected {rejection})).await,"native compaction rejected health settlement failed");
            }
            rejected_posts=rejected_posts.checked_add(1).ok_or("compaction rejected POST count overflow")?;
            // C persists the exact known rejection and settles original health,
            // including fatal outcomes; its Retry result alone is insufficient.
            let action=boxed_execution(|| native_retry::rejected(reporter,input,control,state,cfg,index,rejection,
                &actual_binding,&diagnostic,rejected_posts)).await
                .map_err(|error|format!("{diagnostic}; native compaction retry handling failed: {error}"));
            // Even a stopped/fatal caller may retain this exact original no-reply
            // resolution. If C did not commit proof, D refuses without repair.
            let report_id=contract::rejection_id(&reporter.attempt.id,index);
            let reconciliation=boxed_execution(|| async {
                match boxed_execution(|| charter::mutate(reporter,state,control,format!("{operation_id}-rejected"),
                    CharterObservation::CompactionRejected {run_id:operation_id.clone(),report_id:report_id.clone()})).await? {
                    CharterOutcome::Compaction(value) if value.run_id==original.run_id
                        && value.caller==original.caller && value.effective==original.effective
                        && value.reply.is_none() && value.summary.is_none() && value.disposition.is_none()=>Ok(()),
                    _=>Err("compaction rejection reconciliation differs".into()),
                }
            }).await;
            let action=with_secondary(action,reconciliation,"original compaction rejection reconciliation failed");
            let action=with_secondary(action,usage_result,"native compaction usage recording failed");
            let capture_result=match (exchange.take_failure(),charter::capture(&exchange)) {
                (Ok(None),Ok(None)) if observed.result.is_err()=>Ok(()),
                (Ok(Some(error)),_)=>Err(error),
                (Err(error),_)|(_,Err(error))=>Err(error),
                _=>Err("known rejected compaction supplied reply data; retry withheld".into()),
            };
            let action=with_secondary(action,capture_result,"rejected compaction capture differs")?;
            if matches!(action,RetryAction::Blocked) {return Ok(CompactionProgress::Blocked);}
            if !rejection.kind.same_input_retryable() {return Err("fatal rejection cannot authorize compaction retry".into());}
            append_evidence(&mut requests,contract::NativeRequestEvidence::ConfirmedRejection {request_index:index,operation_id:report_id},input)?;
            carrier=carrier.after_rejected_request(index)?;
            retry_binding=Some(actual_binding);
            index=index.checked_add(1).ok_or("native request index overflow")?;
            binding=boxed_execution(|| charter::session(reporter,input,control)).await?.ok_or("compaction retry original session missing")?;
            if binding.session.compaction_in_flight.is_some()||binding.session.compaction_due.is_none() {
                return Err("compaction rejection did not restore bounded due work".into());
            }
            continue;
        }
        let health=match observed.provider_outcome {
            native::ProviderOutcome::Completed|native::ProviderOutcome::OutputCapped=>NativeRequestOutcome::Healthy,
            native::ProviderOutcome::Unknown=>NativeRequestOutcome::Unknown,
            native::ProviderOutcome::NotStarted=>NativeRequestOutcome::NotStarted,
            native::ProviderOutcome::Rejected(_)=>unreachable!("handled above"),
        };
        let settlement=boxed_execution(|| native_retry::settle(reporter,input,state,index,health)).await;
        // Retain a genuine captured reply independently of current status or an
        // earlier usage/health failure. Audit is passive and confers no authority.
        let captured=boxed_execution(|| async {
            let capture_failure=exchange.take_failure()?;
            let mut accepted=None;
            if let Some(reply)=charter::capture(&exchange)? {
                let summary=observed.result.as_ref().ok().map(|value|value.summary.clone());
                match boxed_execution(|| charter::record_original(reporter,input,state,control,format!("{operation_id}-reply"),
                    CharterObservation::CompactionReply {run_id:operation_id.clone(),reply,summary})).await? {
                    CharterOutcome::Compaction(value) if value.run_id==original.run_id
                        && value.caller==original.caller && value.effective==original.effective=>{
                        if value.disposition==Some(CompactionDisposition::CurrentSourceSummary) {accepted=value.summary;}
                    },
                    _=>return Err("charter compaction result differs".into()),
                }
            } else if observed.result.is_ok() {return Err("successful compaction has no actual captured reply".into());}
            if let Some(error)=capture_failure {return Err(error);}
            Ok(accepted)
        }).await;
        let result=with_secondary(observed.result,usage_result,"native compaction usage recording failed");
        let result=with_secondary(result,boxed_execution(|| acknowledge_native_diagnostic(reporter,control,retained)).await,"native terminal diagnostic failed");
        let result=with_secondary(result,settlement,"native compaction health settlement failed");
        let accepted=match (result,captured) {
            (Ok(_),Ok(value))=>value,
            (Err(original),Ok(_))=>return Err(original),
            (Ok(_),Err(error))=>return Err(error),
            (Err(original),Err(error))=>return Err(format!("{original}; native compaction audit failed: {error}")),
        };
        if reporter.provider_uncertain.load(Ordering::Relaxed) {return Err("native compaction outcome remains unknown".into());}
        let next=carrier.after_compaction(index,accepted.as_deref())?;
        append_evidence(&mut requests,contract::NativeRequestEvidence::Compaction {request_index:index,operation_id},input)?;
        return Ok(CompactionProgress::Completed {carrier:next,requests});
    }
}
