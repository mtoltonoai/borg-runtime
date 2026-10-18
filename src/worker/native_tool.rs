//! Exactly one configured read-only native tool between two guarded requests.
//! No phase is resumed after worker loss: the ordinary native launch record
//! conservatively requires uncertainty reconciliation instead of repeating POST.
use super::*;
use crate::{
    native::{ProviderOutcome, UsageObservation},
    task_tool::{self as contract, ToolObservation},
};

#[derive(Default)]
struct Usage {
    input: Option<u64>,
    output: Option<u64>,
    known: u32,
    unknown: u32,
}
impl Usage {
    fn add(&mut self, usage: UsageObservation) -> Result<bool, String> {
        if let UsageObservation::Reported(value) = usage {
            let input = self
                .input
                .unwrap_or(0)
                .checked_add(value.input_tokens)
                .filter(|v| *v <= i64::MAX as u64)
                .ok_or("native cumulative input usage overflow")?;
            let output = self
                .output
                .unwrap_or(0)
                .checked_add(value.output_tokens)
                .filter(|v| *v <= i64::MAX as u64)
                .ok_or("native cumulative output usage overflow")?;
            self.input = Some(input);
            self.output = Some(output);
            self.known += 1;
            Ok(true)
        } else {
            self.unknown += 1;
            Ok(false)
        }
    }
    fn diagnostic(&self) -> String {
        if self.unknown == 0 {
            "native token usage: complete reported total for two requests".into()
        } else {
            format!(
                "native token usage: known reported subtotal for {} of two requests; remaining usage unknown",
                self.known
            )
        }
    }
}
async fn read_current(
    reporter: &Reporter,
    control: &Control,
) -> Result<contract::CurrentTaskProjection, String> {
    control.check()?;
    let operation = Operation::GetTask {
        task_id: reporter.attempt.task_id.clone(),
    };
    let mut stopped = control.clone();
    let reply = tokio::select! {biased;
        _=stopped.stopped()=>return Err("native task tool cancelled or expired".into()),
        value=reporter.client.call(operation)=>value?,
    };
    control.check()?;
    if !matches!(&reply.outcome, Outcome::Ok { data: Data::Task(task) }
        if task.session_id == reporter.attempt.session_id
            && task.assigned_agent_id == task.input.definition.id
            && task.attempt.as_ref().is_some_and(|a| a.session_id == reporter.attempt.session_id))
    {
        return Err("native task tool session binding differs".into());
    }
    contract::project(&reply, &reporter.attempt)
}
async fn reserve(
    reporter: &Reporter,
    control: &Control,
    usage: &Usage,
    requests: u32,
    diagnostic: &mut NativeDiagnosticRequest,
) -> Result<(), String> {
    control.check()?;
    if diagnostic.index != requests {return Err("native diagnostic request index differs".into());}
    diagnostic.reserve(reporter,control,usage.input,usage.output).await?;
    control.check()?;
    read_current(reporter, control).await?;
    Ok(())
}
async fn observed<T>(
    reporter: &Reporter,
    usage: &mut Usage,
    request: u32,
    result: Result<T, String>,
    tokens: UsageObservation,
    outcome: ProviderOutcome,
) -> Result<T, String> {
    if outcome == ProviderOutcome::Unknown {
        reporter.provider_uncertain.store(true, Ordering::Relaxed);
    }
    let publication=async {
      if usage.add(tokens)? {
        if !reporter
            .send_identified(
                format!("{}-native-usage-{request}", reporter.attempt.id),
                Observation::Usage {
                    native_diagnostic_reserved: false,
                    model_requests: request,
                    input_tokens: usage.input,
                    output_tokens: usage.output,
                },
            )
            .await?
        {
            return Err("native usage observation unacknowledged; later request withheld".into());
        }
      }
      Ok::<(),String>(())
    }.await;
    if let Err(publication)=publication {
        return Err(match result {
            Err(original)=>format!("{original}; native usage recording failed: {publication}"),
            Ok(_)=>publication,
        });
    }
    if reporter.provider_uncertain.load(Ordering::Relaxed) && result.is_ok() {
        return Err("native provider outcome remains unknown".into());
    }
    result
}

pub(super) async fn execute(
    config: &WorkerConfig,
    input: &TaskInput,
    control: &Control,
    reporter: &Reporter,
    continuation: Option<&ChildOutcome>,
    state: &Path,
    owner_lock: Arc<std::fs::File>,
) -> Result<AttemptExit, String> {
    let inference = input.definition.required_inference()?;
    input
        .definition
        .execution_profile
        .as_ref()
        .ok_or("template_profile_unbound")?;
    control.check()?;
    if continuation.is_some() || !contract::enabled(input) {
        return Err(
            "native task inspection requires two remaining requests and no child continuation"
                .into(),
        );
    }
    if input.limits.model_requests < 2 {
        return Err("native task inspection request budget: task_model_requests".into());
    }
    track_native_launch(state, &reporter.attempt, &control.runtime_limits)?;
    let first_read = read_current(reporter, control).await?;
    // Admit the prospective report framing before any provider request. This
    // value is size-only; it is never published as a real tool observation.
    let mut largest = first_read;
    largest.observed_controller_revision = i64::MAX as u64;
    contract::encode(
        &ToolObservation {
            call_id: "\\".repeat(input.execution_limits()?.native.call_id_bytes),
            task: largest,
        },
        input.limits.output_bytes,
        &input.execution_limits()?.native,
    )?;
    let host = crate::native::ValidationHost::with_owner_lock(
        config.validation_executable.clone(),
        native_registry(state, &reporter.attempt),
        owner_lock,
        Arc::new(input.execution_limits()?.native.clone()),
        control.validation_runtime.clone(),
    )
    .with_request_attempt(&reporter.attempt);
    let native_config = crate::native::NativeConfig {
        endpoint: config.native_endpoint.clone(),
        api_key_env: "OPENAI_API_KEY".into(),
        model: inference.model.clone(),
    };
    let mut usage = Usage::default();
    let mut first_diagnostic=NativeDiagnosticRequest::preflight(reporter,input,&native_config,1)?;
    let prepared=async {
        reserve(reporter,control,&usage,1,&mut first_diagnostic).await?;
        charter::begin(reporter,input,state,control,1).await
    }.await;
    let first_turn=match prepared {
        Ok(value)=>value,
        Err(error)=>{
            let retained=first_diagnostic.unstarted(reporter);
            return charter::with_secondary(Err(error),acknowledge_native_diagnostic(reporter,control,retained).await,
                "native terminal diagnostic failed");
        },
    };
    let mut stopped = control.clone();
    let first_host=host.for_request(None);
    let first_host = match &first_turn { Some(turn) => first_host.with_charter(turn.exchange.clone()), None => first_host };
    let first = crate::native::request_current_task(
        input,
        &native_config,
        &first_host,
        async { read_current(reporter, control).await.map(|_| ()) },
        stopped.stopped(),
    )
    .await;
    let retained=first_diagnostic.retain(reporter,&first_host,first.provider_outcome,first.diagnostic.as_ref());
    let call = observed(
        reporter,
        &mut usage,
        1,
        first.result,
        first.usage,
        first.provider_outcome,
    )
    .await;
    let recorded = charter::record(reporter, input, state, control, first_turn.as_ref()).await;
    let diagnostic_ack=acknowledge_native_diagnostic(reporter,control,retained).await;
    let call=charter::with_secondary(call,recorded,"native charter recording failed");
    let call=charter::with_secondary(call,diagnostic_ack,"native terminal diagnostic failed")?;
    control.check()?;
    //= spec/impl/v2/implementation.md#the-native-executor-is-bounded
    //# A native tool invocation MUST be restricted to the task's configured tool interface.
    // The model supplied no selector. This is the real typed controller read
    // for the exact worker-owned task/attempt; no caller-provided URL or ID.
    let projection = read_current(reporter, control).await?;
    let observation = ToolObservation {
        call_id: call.call_id().into(),
        task: projection,
    };
    let encoded = contract::encode(
        &observation,
        input.limits.output_bytes,
        &input.execution_limits()?.native,
    )?;
    let credentials = configured_credentials(&control.runtime_limits)?;
    if contains_credential(&encoded, &credentials) {
        return Err("native tool observation contains a configured credential".into());
    }
    if !reporter
        .send_identified(
            contract::report_id(&reporter.attempt.id),
            Observation::Progress {
                text: contract::PROGRESS_TEXT.into(),
                output: encoded,
                truncated: false,
            },
        )
        .await?
    {
        return Err("native tool observation unacknowledged; no continuation request sent".into());
    }
    control.check()?;
    read_current(reporter, control).await?;
    charter::close(reporter, state, control, first_turn.as_ref(), vec![crate::charter::CharterResultReceipt::CurrentTask {
        call_id: call.call_id().into(), report_id: contract::report_id(&reporter.attempt.id),
    }]).await?;
    let mut final_diagnostic=NativeDiagnosticRequest::preflight(reporter,input,&native_config,2)?;
    let prepared=async {
        reserve(reporter,control,&usage,2,&mut final_diagnostic).await?;
        charter::begin(reporter,input,state,control,2).await
    }.await;
    let final_turn=match prepared {
        Ok(value)=>value,
        Err(error)=>{
            let retained=final_diagnostic.unstarted(reporter);
            return charter::with_secondary(Err(error),acknowledge_native_diagnostic(reporter,control,retained).await,
                "native terminal diagnostic failed");
        },
    };
    let final_host=host.for_request(None);
    let final_host = match &final_turn { Some(turn) => final_host.with_charter(turn.exchange.clone()), None => final_host };
    //= spec/impl/v2/implementation.md#the-native-executor-is-bounded
    //# A native model continuation MUST consume the actual result of its preceding tool invocation.
    // Forward the exact call/context and acknowledged read projection; another
    // currentness read guards use but does not substitute a newer tool result.
    let mut stopped = control.clone();
    let final_value = crate::native::continue_current_task(
        input,
        &native_config,
        &call,
        &observation,
        &final_host,
        async { read_current(reporter, control).await.map(|_| ()) },
        stopped.stopped(),
    )
    .await;
    let retained=final_diagnostic.retain(reporter,&final_host,final_value.provider_outcome,final_value.diagnostic.as_ref());
    let result = observed(
        reporter,
        &mut usage,
        2,
        final_value.result,
        final_value.usage,
        final_value.provider_outcome,
    )
    .await;
    let recorded = charter::record(reporter, input, state, control, final_turn.as_ref()).await;
    let diagnostic_ack=acknowledge_native_diagnostic(reporter,control,retained).await;
    let result=charter::with_secondary(result,recorded,"native charter recording failed");
    let result=charter::with_secondary(result,diagnostic_ack,"native terminal diagnostic failed")?;
    charter::close(reporter, state, control, final_turn.as_ref(), Vec::new()).await?;
    control.check()?;
    read_current(reporter, control).await?;
    reporter
        .send(Observation::ExecutorOutcome {
            success: true,
            summary: scrub(
                result.summary.clone(),
                input.execution_limits()?.native.summary_bytes,
                &control.runtime_limits,
            ),
        })
        .await?;
    reporter.send(Observation::ProcessTreeStopped).await?;
    reporter
        .send(Observation::ResultCandidate {
            checks_passed: true,
            summary: scrub(
                result.summary,
                input.execution_limits()?.native.summary_bytes,
                &control.runtime_limits,
            ),
            checkpoint: None,
            validation: vec![
                "native structured output schema: pass".into(),
                contract::evidence(&reporter.attempt.id),
                usage.diagnostic(),
            ],
            output: Some(result.output),
        })
        .await?;
    Ok(AttemptExit::Finished)
}
