//! Original typed carriers are inspected before replay and outer-marker upgrade.
use super::*;
pub(super) fn admit(state:&Path,limits:&RuntimeLimits)->Result<(),String> {
    if handoff::admits_read_pagination(state,&limits.worker)? {return Ok(());}
    handoff::admit_outcomes(state,limits,crate::protocol::outcome_requires_read_pagination)?;
    for (_,receipt) in command_receipts(state,limits)? {
        let WorkerCommand::RunAttempt {attempt,task,..}=&receipt.command else {continue;};
        if task.definition.executor==ExecutorKind::Coding {
            let cfg=codex_mcp::Config {
                instruction_source:task.instruction_source.clone(),
                execution_profile:task.definition.execution_profile.as_ref().filter(|p|matches!(p,ExecutionProfile::CollaborationV2{..})).cloned(),
                controller_url:String::new(),caller:collaboration_caller(attempt),
                deadline_unix_ms:attempt.deadline_unix_ms.min(task.limits.deadline_unix_ms),
                state:codex::state_path(&state.join("codex"),&attempt.id),
                runtime_limits:(*task.resolved_limits.as_ref().ok_or("accepted limits absent")?.global_metadata).clone(),
                limits:task.execution_limits()?.collaboration.clone(),worker_limits:limits.worker.clone(),
            };
            codex_mcp::admit_read_pagination_from_retained_config(cfg)?;
        }
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
                return Err("read pagination vocabulary in retained native journal requires worker20".into());
            }
        }
    }
    if mutations::read(&state.join("mutations/await.json"),limits)?.is_some_and(|j|requires(&j.request,j.outcome.as_ref())) {
        return Err("read pagination vocabulary in committed await requires worker20".into());
    }
    Ok(())
}

fn requires(request:&Operation,outcome:Option<&Outcome>)->bool {
    crate::protocol::requires_read_pagination(request)||outcome.is_some_and(crate::protocol::outcome_requires_read_pagination)
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
        WorkerCommand::RunAttempt{command_id:"machine-command".into(),session:(Session{id:attempt.session_id.clone(),agent_id:task.definition.id.clone(),definition:task.definition.clone(),task_id:attempt.task_id.clone()}).into(),attempt,task: (task).into(),checkpoint:None,continuation:None,workflow_continuation:None,completion_continuation:None}
    }

    fn page()->Outcome {
        use crate::read_page::*;
        Outcome::Ok{data:Data::ReadPage(Box::new(ReadPage {subject:Selection::Task {task_id:"machine-task".into(),session_id:Some("machine-session".into()),part:TaskPart::Instruction},identity:Identity {source:Source::Task{task_id:"machine-task".into(),session_id:"machine-session".into(),part:TaskPart::Instruction,revision:"machine-session".into()},digest:digest(b"x")},summary:Summary::Task {status:TaskStatus::Active,parts:vec![TaskPart::Instruction]},encoding:Encoding::Utf8,offset:0,returned_bytes:1,total_bytes:1,content:"x".into(),complete:true,next_cursor:None,retained_truncation:None}))}
    }
    #[test]
    fn shared_read_pagination_previous_worker_typed_carriers_refuse() {
        let metadata=test_limits();
        for coding in [false,true] {for carrier in ["request","page","nested"] {
            let state=std::env::temp_dir().join(mutations::fresh_id("pagination-old-worker").unwrap());process::private_directory(&state).unwrap();
            atomic_json(&state.join("format.json"),&json!({"format_revision":19})).unwrap();process::private_directory(&state.join("commands")).unwrap();
            let command=command(&metadata,coding);let WorkerCommand::RunAttempt{attempt,task,..}=&command else{unreachable!()};record_launch(&state.join("commands/original.json"),&command,&metadata).unwrap();
            let cfg=WorkerConfig{limits:metadata.clone(),resource_profile:None,controller_url:"http://127.0.0.1:9".into(),id:attempt.worker_id.clone(),incarnation:attempt.worker_incarnation.clone(),workspace_root:state.clone(),codex_path:"/must-not-launch".into(),coding_capacity:metadata.worker.coding_capacity,native_capacity:metadata.worker.native_capacity,native_endpoint:"http://127.0.0.1:9/no-provider".into(),validation_executable:"/must-not-launch".into()};
            let call=crate::mcp::Invocation{operation_id:"pagination-original".into(),logical_call_id:"pagination-logical".into(),caller:collaboration_caller(attempt),tool:if carrier=="request"{"collab_read_continue"}else{"collab_memory_get"}.into(),arguments:if carrier=="request"{json!({"cursor":"original-cursor"})}else{json!({"key":"ordinary"})}};
            let outcome=if carrier=="request" {Outcome::Refused{code:"invalid_input".into(),message:"original refusal is still new typed request vocabulary".into()}} else if carrier=="page" {page()} else {
                let nested=crate::mcp::Invocation{operation_id:"nested-original".into(),logical_call_id:"nested-logical".into(),caller:call.caller.clone(),tool:"collab_task_get".into(),arguments:json!({"task_id":attempt.task_id,"part":"instruction"})};
                let result=crate::mcp::result(&nested,Reply{request_id:"nested".into(),outcome:page(),commit:Some(MutationCommit{operation_id:nested.operation_id.clone(),controller_revision:1}),observed_controller_revision:Some(1)}).unwrap();
                Outcome::Ok{data:Data::Collaboration(crate::collaboration::QueryResult::Call(Box::new(crate::collaboration::CallRecord{invocation:nested,result})))}
            };
            let context=CommitContext{commit:MutationCommit{operation_id:call.operation_id.clone(),controller_revision:2},observed_controller_revision:2};
            let directory=if coding{codex::state_path(&state.join("codex"),&attempt.id).join("collaboration-codex")}else{state.join("collaboration").join(component(&attempt.id))};process::private_directory(&directory).unwrap();
            let path=directory.join(if coding{"calls.json"}else{"0000000001.json"});
            if coding {
                let result=crate::mcp::result(&call,Reply{request_id:"original".into(),outcome,commit:Some(context.commit),observed_controller_revision:Some(2)}).unwrap();
                atomic_json(&path,&json!({"instruction_source":null,"runtime_limits":&*metadata,"limits":task.execution_limits().unwrap().collaboration,"worker_limits":metadata.worker,"revision":2,"caller":call.caller,"deadline_unix_ms":attempt.deadline_unix_ms,"controller_url":cfg.controller_url,"entries":[{"thread_id":"original-thread","session_id":"original-session","invocation":call,"outcome":result,"delivered":true}]})).unwrap();
            } else {
                let mut journal=mutations::prepare(Operation::CallCollaboration{call},0,&metadata).unwrap();journal.outcome=Some(outcome);journal.outcome_context=Some(context);journal.consumed=true;mutations::persist(&path,&journal).unwrap();
            }
            let bytes=std::fs::read(&path).unwrap();let marker=std::fs::read(state.join("format.json")).unwrap();
            let reason=if coding{coding_collaboration_closed(&cfg,task,attempt,&state).unwrap_err()}else{admit(&state,&metadata).unwrap_err()};
            assert!(reason.contains("worker20"),"{reason}");assert_eq!(std::fs::read(&path).unwrap(),bytes);assert_eq!(std::fs::read(state.join("format.json")).unwrap(),marker);
            atomic_json(&state.join("format.json"),&json!({"format_revision":20})).unwrap();if coding{assert!(coding_collaboration_closed(&cfg,task,attempt,&state).unwrap());}else{admit(&state,&metadata).unwrap();}
            assert_eq!(std::fs::read(&path).unwrap(),bytes);std::fs::remove_dir_all(state).unwrap();
        }}
        // Opaque user content containing identical vocabulary is not a typed carrier.
        assert!(!crate::protocol::outcome_requires_read_pagination(&Outcome::Ok{data:Data::Memory(Some(MemoryValue{agent_id:"a".into(),key:"k".into(),revision:1,value:json!({"kind":"read_page","cursor":"x","ReadPage":"ordinary"})}))}));
    }
}
