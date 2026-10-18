//! One bounded terminal read batch, driven by the existing cleanup scheduler.
use super::*;
use crate::read_retention::{TerminalReadRelease, ReleaseResult};

#[derive(Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct Progress {
    format_revision:u32,
    command_id:String,
    policy:WorkerRetentionLimits,
    policy_revision:String,
    after:Option<String>,
    pending:Option<TerminalReadRelease>,
}

async fn canonical_task(client:&ApiClient,command:&WorkerCommand,state:&Path,policy:&WorkerRetentionLimits)->Result<Task,String> {
    let WorkerCommand::RunAttempt{task,attempt,..}=command else {return Err("read cleanup requires RunAttempt".into())};
    if task.definition.executor==ExecutorKind::Coding {
        return canonical(client,command,state,policy.operation_timeout_ms).await.map_err(|_|"Coding original closure unavailable".into());
    }
    let reply=client.call(Operation::GetTask{task_id:attempt.task_id.clone()}).await?;
    let Outcome::Ok{data:Data::Task(current)}=reply.outcome else {return Err("native canonical task unavailable".into())};
    terminal(&current,task,attempt).map_err(|_|"native original closure incomplete")?;
    Ok(*current)
}

pub(super) async fn attempt_at(config:&WorkerConfig,state:&Path,client:&ApiClient,active:&BTreeMap<String,Active>,
    runtime:&Arc<crate::native::ValidationRuntime>,policy:&WorkerRetentionLimits,revision:&str,path:&Path,now:u64)->Result<Option<u64>,String> {
    let retry=add_ms(now,policy.retry_delay_ms)?;
    match tokio::time::timeout(Duration::from_millis(policy.operation_timeout_ms),
        step(config,state,client,active,runtime,policy,revision,path,now)).await {
        Ok(Ok(value))=>Ok(value),
        Ok(Err(error))=>{eprintln!("BORG_CLEANUP retained original-read: {error}");Ok(Some(retry))},
        Err(_)=>Ok(Some(retry)),
    }
}

async fn step(config:&WorkerConfig,state:&Path,client:&ApiClient,active:&BTreeMap<String,Active>,
    runtime:&Arc<crate::native::ValidationRuntime>,policy:&WorkerRetentionLimits,revision:&str,path:&Path,now:u64)->Result<Option<u64>,String> {
    if !handoff::admits_read_references(state,&config.limits.worker)? {return Err("read release requires worker20".into())}
    let receipt:Receipt=read_record(path,config.limits.worker.command_record_bytes)?.ok_or("read cleanup command absent")?;
    let WorkerCommand::RunAttempt{command_id,attempt,task,..}=&receipt.command else{return Ok(None)};
    if path!=state.join("commands").join(format!("{}.json",component(command_id))) || !receipt.completed
        || attempt.worker_id!=config.id || active.contains_key(&attempt.id) || attempt_outbox(state,attempt,&config.limits)?
        || process_obligation(&state.join("processes").join(component(&attempt.id)))? {return Err("original local attempt not closed".into())}
    if task.definition.executor==ExecutorKind::Coding && !local_ready(config,state,&receipt,active)? {return Err("Coding closure remains".into())}
    if task.definition.executor==ExecutorKind::Native && handoff::path(state,attempt).exists() {return Err("native handoff retains body consumer".into())}
    let current=canonical_task(client,&receipt.command,state,policy).await?;
    let directory=state.join("cleanup-reads");process::private_directory(&directory)?;
    let progress_path=directory.join(format!("{}.json",component(&attempt.id)));
    let mut progress:Progress=read_record(&progress_path,policy.record_bytes)?.unwrap_or(Progress{format_revision:20,command_id:command_id.clone(),
        policy:policy.clone(),policy_revision:revision.into(),after:None,pending:None});
    if progress.format_revision!=20||progress.command_id!=*command_id||progress.policy!=*policy||progress.policy_revision!=revision {return Err("read cleanup original policy differs".into())}
    let (_sender,cancel)=watch::channel(false);
    let control=Control{deadline_ms:add_ms(process::now_ms(),policy.operation_timeout_ms)?,cancel,worker_limits:Arc::new(config.limits.worker.clone()),
        runtime_limits:config.limits.clone(),validation_runtime:runtime.clone()};
    let (reads,custody)=if task.definition.executor==ExecutorKind::Coding {
        let cfg=codex_mcp::Config{instruction_source:task.instruction_source.clone(),execution_profile:task.definition.execution_profile.as_ref().filter(|p|matches!(p,ExecutionProfile::CollaborationV2{..})).cloned(),
            controller_url:config.controller_url.clone(),caller:collaboration_caller(attempt),deadline_unix_ms:attempt.deadline_unix_ms.min(task.limits.deadline_unix_ms),
            state:codex::state_path(&state.join("codex"),&attempt.id),runtime_limits:(*task.resolved_limits.as_ref().ok_or("accepted metadata absent")?.global_metadata).clone(),
            limits:task.execution_limits()?.collaboration.clone(),worker_limits:config.limits.worker.clone()};
        (codex_mcp::terminal_reads(&cfg,progress.after.as_deref(),policy.batch_entries)?,plugins::read_custody(state,attempt,policy,revision,&control).await?)
    } else {
        super::super::read_retention::native_reads(state,task,attempt,policy,progress.after.as_deref())?
    };
    if reads.is_empty() {return Ok(None)}
    if !custody.valid() {return Err("verified actual transcript absent".into())}
    let plan=match &progress.pending {
        Some(plan)=>{
            if plan.reads!=reads||plan.custody!=custody||plan.terminal_revision!=current.revision {return Err("original release evidence changed; no new batch sent".into())}
            plan.clone()
        }
        None=>{
            let id=mutations::fresh_id("read-custody")?;
            let plan=TerminalReadRelease{operation_id:id.clone(),caller:collaboration_caller(attempt),command_id:command_id.clone(),session_id:attempt.session_id.clone(),
                terminal_revision:current.revision,first_safe_operation_id:None,policy_revision:revision.into(),policy:policy.clone(),custody,custody_operation_id:id,reads};
            progress.pending=Some(plan.clone());bounded_json_size(&progress,policy.record_bytes)?;atomic_json(&progress_path,&progress)?;plan
        }
    };
    // Each exact intent is stored before lookup/send. Interrupted phases recover
    // their original receipt; a committed refusal never creates a fresh identity.
    let custody_request=Operation::ObserveWorker{report:Report{report_id:plan.custody_operation_id.clone(),worker_id:attempt.worker_id.clone(),incarnation:attempt.worker_incarnation.clone(),
        task_id:attempt.task_id.clone(),attempt_id:attempt.id.clone(),generation:attempt.generation,observation:Observation::ReadTranscriptCustody{proof:Box::new(plan.clone())}}};
    let custody_path=directory.join(format!("custody-{}.json",component(&plan.custody_operation_id)));
    let Some(Outcome::Ok{data:Data::Task(acknowledged)})=original(client,&custody_path,custody_request).await? else{return Err("original custody not acknowledged".into())};
    if *acknowledged!=current {return Err("custody original terminal task differs".into())}
    let mut observe=plan.clone();observe.operation_id=format!("{}-safe",plan.custody_operation_id);
    let safe_path=directory.join(format!("safe-{}.json",component(&plan.custody_operation_id)));
    let Some(Outcome::Ok{data:Data::TerminalReadRelease(first)})=original(client,&safe_path,Operation::ReleaseTerminalReads(observe.clone())).await? else{return Err("original first-safe observation unresolved".into())};
    if first.first_safe_operation_id!=observe.operation_id||!first.released.is_empty() {return Err("first-safe result differs".into())}
    if now<first.eligible_at_ms {return Ok(Some(first.eligible_at_ms))}
    control.check()?;
    if canonical_task(client,&receipt.command,state,policy).await?!=current||attempt_outbox(state,attempt,&config.limits)? {return Err("terminal closure changed before release".into())}
    let mut release=plan.clone();release.operation_id=format!("{}-release",plan.custody_operation_id);release.first_safe_operation_id=Some(observe.operation_id);
    let release_path=directory.join(format!("release-{}.json",component(&plan.custody_operation_id)));
    let Some(Outcome::Ok{data:Data::TerminalReadRelease(ReleaseResult{first_safe_ms,eligible_at_ms,..})})=original(client,&release_path,Operation::ReleaseTerminalReads(release)).await? else{return Err("original terminal release unresolved".into())};
    if first_safe_ms!=first.first_safe_ms||eligible_at_ms!=first.eligible_at_ms {return Err("release first-safe binding differs".into())}
    progress.after=plan.reads.last().map(|v|v.operation_id.clone());progress.pending=None;
    bounded_json_size(&progress,policy.record_bytes)?;atomic_json(&progress_path,&progress)?;
    // Yield to ordinary commands before selecting another batch.
    Ok(Some(add_ms(now,policy.retry_delay_ms)?))
}

async fn original(client:&ApiClient,path:&Path,request:Operation)->Result<Option<Outcome>,String> {
    let mut journal=match mutations::read(path,client.limits())? {
        Some(journal)=>{if serde_json::to_value(&journal.request).map_err(|_|"read cleanup request encoding")?!=serde_json::to_value(&request).map_err(|_|"read cleanup request encoding")? {return Err("original read cleanup intent differs".into())}journal},
        None=>mutations::create(path,request,0,client.limits())?,
    };
    let result=mutations::resolve(client,path,&mut journal).await?;
    if result.is_some() {mutations::consume(path,&mut journal)?;}
    Ok(result)
}
