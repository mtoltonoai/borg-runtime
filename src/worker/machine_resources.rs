//! An independent host service. It never changes an AwaitCommands journal,
//! registers capacity, acquires a task/model permit or creates report receipts.
use super::*;
use crate::machine_resources::{Profile,Producer,Role,WorkerBinding};

pub(super) fn start(config:&WorkerConfig,profile:Arc<Profile>)->Result<Producer,String> {
    profile.validate()?;
    if profile.local().role!=Role::Worker || profile.local().worker_id.as_deref()!=Some(&config.id) {
        return Err("worker host observation profile differs from configured worker".into());
    }
    let epoch=crate::machine_resources::producer_epoch(profile.bounds.kernel_read_bytes)?;
    let worker=Some(WorkerBinding {worker_id:config.id.clone(),incarnation:config.incarnation.clone()});
    let client=ApiClient::new(&config.controller_url,config.limits.clone())?;
    Producer::start(profile,worker,epoch,move|sample| {
        let client=client.clone();
        async move {
            // A failed/unknown observation is dropped. A later sequence may
            // replace it; no uncertain task effect is being retried here.
            let _=client.call(Operation::ReportMachineResources {sample}).await;
        }
    })
}

/// Native mutation journals use their original accepted command and policy.
/// Historical data is read through the same typed reader as recovery.
pub(super) fn admit(state:&Path,limits:&RuntimeLimits)->Result<(),String> {
    if handoff::admits_machine_resources(state,&limits.worker)? {return Ok(());}
    handoff::admit_machine_outcomes(state,limits)?;
    for (_,receipt) in command_receipts(state,limits)? {
        let WorkerCommand::RunAttempt {attempt,task,..}=&receipt.command else {continue;};
        if task.definition.executor!=ExecutorKind::Native || !crate::execution_profile::collaboration(task) {continue;}
        let directory=state.join("collaboration").join(component(&attempt.id));
        let entries=match std::fs::read_dir(&directory) {
            Ok(entries)=>entries,Err(error) if error.kind()==std::io::ErrorKind::NotFound=>continue,
            Err(_)=>return Err("native collaboration journal inventory unavailable".into()),
        };
        let snapshot=task.resolved_limits.as_ref().ok_or("execution limits unbound")?;
        let bound=(task.limits.model_requests as usize).checked_mul(task.execution_limits()?.native.function_calls_per_response)
            .ok_or("collaboration journal allowance overflow")?;
        for (index,entry) in entries.enumerate() {
            if index>=bound {return Err("collaboration journal count exceeds accepted allowance".into());}
            let path=entry.map_err(|_|"native collaboration journal inventory unreadable")?.path();
            if path.extension().and_then(|v|v.to_str())!=Some("json") {return Err("unexpected collaboration journal entry".into());}
            let journal=mutations::read(&path,&snapshot.global_metadata)?.ok_or("collaboration journal disappeared")?;
            if !matches!(&journal.request,Operation::CallCollaboration{call} if call.caller==collaboration_caller(attempt)) {
                return Err("collaboration journal caller or operation changed".into());
            }
            if requires(&journal.request,journal.outcome.as_ref()) {
                return Err("machine resource vocabulary in retained native journal requires worker19".into());
            }
        }
    }
    if mutations::read(&state.join("mutations/await.json"),limits)?.is_some_and(|j|requires(&j.request,j.outcome.as_ref())) {
        return Err("machine resource vocabulary in committed await requires worker19".into());
    }
    Ok(())
}
pub(super) fn requires(request:&Operation,outcome:Option<&Outcome>)->bool {
    matches!(request,Operation::ReportMachineResources{..}
        |Operation::QueryCollaboration{query:crate::collaboration::Query::Machines{..}|crate::collaboration::Query::Machine{..}})
        || matches!(request,Operation::CallCollaboration{call} if matches!(call.tool.as_str(),"collab_machines_list"|"collab_machine_get"))
        || outcome.is_some_and(crate::machine_resources::outcome_requires_telemetry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn command(metadata:&RuntimeLimits,coding:bool)->WorkerCommand {
        let mut task=TaskInput{instruction_source:None,pull_request:None,resolved_limits:None,repository_read:None,
            instruction:"Observe host resources without mutation".into(),definition:Definition{charter:None,limit_overrides:None,id:"machine-reader".into(),revision:"r1".into(),executor:if coding{ExecutorKind::Coding}else{ExecutorKind::Native},inference:Some(InferenceSettings::selected()),execution_profile:Some(ExecutionProfile::CollaborationV1{web_search:None})},
            repository:None,acceptance_commands:vec![],output_schema:None,limits:metadata.task_defaults.at(process::now_ms()).unwrap(),dependencies:vec![],native_workflow:None};
        task.resolved_limits=Some(metadata.resolve_task(&task).unwrap());
        let attempt=Attempt{id:"machine-attempt".into(),session_id:"machine-session".into(),task_id:"machine-task".into(),generation:1,worker_id:"worker".into(),worker_incarnation:"one".into(),phase:AttemptPhase::Assigned,deadline_unix_ms:task.limits.deadline_unix_ms,stopped:false,uncertain:false};
        WorkerCommand::RunAttempt{command_id:"machine-command".into(),session:Session{id:attempt.session_id.clone(),agent_id:task.definition.id.clone(),definition:task.definition.clone(),task_id:attempt.task_id.clone()},attempt,task,checkpoint:None,continuation:None,workflow_continuation:None,completion_continuation:None}
    }
    #[test]
    fn host_resources_prior_worker_carrier_refusal() {
        let metadata=test_limits();
        for coding in [false,true] {
            let state=std::env::temp_dir().join(format!("borg-machine-vocabulary-{}-{}-{coding}",std::process::id(),mutations::fresh_id("test").unwrap()));
            process::private_directory(&state).unwrap();
            atomic_json(&state.join("format.json"),&json!({"format_revision":18})).unwrap();
            let cmd=command(&metadata,coding);let WorkerCommand::RunAttempt{attempt,task,..}=&cmd else{unreachable!()};
            record_launch(&state.join("commands/original.json"),&cmd,&metadata).unwrap();
            let cfg=WorkerConfig{limits:metadata.clone(),resource_profile:None,controller_url:"http://127.0.0.1:9".into(),id:attempt.worker_id.clone(),incarnation:attempt.worker_incarnation.clone(),workspace_root:state.clone(),codex_path:"/must-not-launch".into(),coding_capacity:metadata.worker.coding_capacity,native_capacity:metadata.worker.native_capacity,native_endpoint:"http://127.0.0.1:9/no-provider".into(),validation_executable:"/must-not-launch".into()};
            let call=crate::mcp::Invocation{operation_id:"machine-original".into(),logical_call_id:"machine-logical".into(),caller:collaboration_caller(attempt),tool:"collab_machines_list".into(),arguments:json!({"cursor":null,"limit":1})};
            let dir=if coding{codex::state_path(&state.join("codex"),&attempt.id).join("collaboration-codex")}else{state.join("collaboration").join(component(&attempt.id))};
            process::private_directory(&dir).unwrap();let path=dir.join(if coding{"calls.json"}else{"0000000001.json"});
            let outcome=Outcome::Ok{data:Data::Collaboration(crate::collaboration::QueryResult::Machines(crate::collaboration::Page{items:vec![],next_cursor:None}))};
            let context=CommitContext{commit:MutationCommit{operation_id:call.operation_id.clone(),controller_revision:1},observed_controller_revision:1};
            if coding {
                let result=crate::mcp::result(&call,Reply{request_id:"machine-read".into(),outcome,commit:Some(context.commit),observed_controller_revision:Some(1)}).unwrap();
                atomic_json(&path,&json!({"instruction_source":null,"runtime_limits":&*metadata,"limits":task.execution_limits().unwrap().collaboration,"worker_limits":metadata.worker,"revision":2,"caller":call.caller,"deadline_unix_ms":attempt.deadline_unix_ms,"controller_url":cfg.controller_url,"entries":[{"thread_id":"original-thread","session_id":"original-session","invocation":call,"outcome":result,"delivered":true}]})).unwrap();
            }else{
                let mut journal=mutations::prepare(Operation::CallCollaboration{call},0,&metadata).unwrap();journal.outcome=Some(outcome);journal.outcome_context=Some(context);journal.consumed=true;mutations::persist(&path,&journal).unwrap();
            }
            let original=std::fs::read(&path).unwrap();let marker=std::fs::read(state.join("format.json")).unwrap();
            let reason=if coding{coding_collaboration_closed(&cfg,task,attempt,&state).unwrap_err()}else{admit(&state,&metadata).unwrap_err()};
            assert!(reason.contains("worker19"),"{reason}");assert_eq!(std::fs::read(&path).unwrap(),original);assert_eq!(std::fs::read(state.join("format.json")).unwrap(),marker);
            // Controlled marker fixture only: the current reader admits these
            // exact new typed bytes, without replay or a provider call.
            atomic_json(&state.join("format.json"),&json!({"format_revision":19})).unwrap();
            if coding{assert!(coding_collaboration_closed(&cfg,task,attempt,&state).unwrap());}else{admit(&state,&metadata).unwrap();}
            assert_eq!(std::fs::read(&path).unwrap(),original);
            std::fs::remove_dir_all(&state).unwrap();
        }
        let state=std::env::temp_dir().join(mutations::fresh_id("machine-upgrade").unwrap());process::private_directory(&state).unwrap();
        atomic_json(&state.join("format.json"),&json!({"format_revision":18})).unwrap();atomic_json(&state.join("limits.json"),&*metadata).unwrap();
        let before=std::fs::read(state.join("limits.json")).unwrap();let lease=process::WorkerLease::claim(&state.join("worker-owner.json"),&metadata.worker).unwrap();
        handoff::finish_state_upgrade(&state,&metadata,&lease).unwrap();assert!(handoff::admits_machine_resources(&state,&metadata.worker).unwrap());assert_eq!(std::fs::read(state.join("limits.json")).unwrap(),before);
        drop(lease);std::fs::remove_dir_all(&state).unwrap();
    }
}
