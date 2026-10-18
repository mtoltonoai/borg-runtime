//! Test-only retained-copy reader. No worker::run, API client request or cleanup.
use super::*;
use std::{collections::BTreeMap, fs};

#[test]
#[ignore = "requires exact private worker copy and canonical task manifest"]
fn decode_historical_state9_worker_copy() {
    let m: serde_json::Value=serde_json::from_slice(&fs::read(
        std::env::var_os("BORG_RETAINED_WORKER_MANIFEST").expect("explicit manifest")).unwrap()).unwrap();
    assert_eq!(m["format"],"borg-retained-worker-decode/1");
    let root=fs::canonicalize(m["copy_root"].as_str().unwrap()).unwrap();
    assert!(!root.starts_with("/var/lib"));
    assert_eq!(root.file_name().unwrap(),"typed-worker-copy");
    let files=m["expected_files"].as_object().unwrap();
    let memory=m["retained_memory"].as_object();
    let memory_files=memory.map(|v|v["files"].as_object().expect("explicit retained memory files"));
    let path=|name:&str| {
        let rel=Path::new(name);
        assert!(!rel.is_absolute() && rel.components().all(|p|matches!(p,std::path::Component::Normal(_))));
        let value=root.join(rel);let mut parent=value.as_path();
        while parent!=root { assert!(!fs::symlink_metadata(parent).unwrap().file_type().is_symlink());parent=parent.parent().unwrap(); }
        value
    };
    let mut before=BTreeMap::new();
    let byte_bound=m["byte_bound"].as_u64().filter(|v|*v>0).unwrap();
    let mut total=0u64;
    for (name,expected) in files {
        assert!(name=="limits.json" || name.starts_with("commands/") || name.starts_with("mutations/")
            || name.starts_with("collaboration/") || name.starts_with("cleanup/") || name.starts_with("publications/") || name.ends_with("/borg-collaboration-config.json")
            || name.ends_with("/collaboration-codex/calls.json")
            || memory_files.is_some_and(|v|v.contains_key(name)));
        let p=path(name);total=total.checked_add(fs::metadata(&p).unwrap().len()).unwrap();assert!(total<=byte_bound);
        let bytes=fs::read(p).unwrap();assert!(std::str::from_utf8(&bytes).unwrap()==expected.as_str().unwrap(), "copied retained record differs");
        before.insert(name.clone(),bytes);
    }
    let policy_path=path("limits.json");
    let limits=crate::limits::RuntimeLimits::load(&policy_path).unwrap();
    assert_eq!(serde_json::to_value(&limits).unwrap(),serde_json::from_slice::<serde_json::Value>(&before["limits.json"]).unwrap());
    let selected=crate::limits::RuntimeLimits::load(Path::new(m["selected_limits_path"].as_str().expect("explicit selected worker startup policy"))).unwrap();
    assert!(limits.retention.is_none(), "retained execution metadata must remain historical");
    let retention=selected.retention.as_ref().expect("selected cleanup startup policy");
    assert_eq!(serde_json::to_value(&retention.worker).unwrap(),m["selected_retention"]);
    assert_eq!(selected.revision,m["selected_revision"].as_str().unwrap());
    assert!(same_execution_policy(&limits,&selected), "startup retention must not replace accepted execution policy");
    let mut unrelated=selected.clone();unrelated.worker.native_capacity+=1;
    assert!(!same_execution_policy(&limits,&unrelated), "unrelated worker policy change must refuse compatibility");
    if let Some(binding)=memory {
        let task:Task=serde_json::from_value(binding["canonical_task"].clone()).unwrap();
        let attempt=task.attempt.as_ref().unwrap();
        assert_eq!(attempt.worker_id,m["worker_id"].as_str().unwrap());
        let scope=binding["scope_path"].as_str().unwrap();
        let selected=memory_files.unwrap();
        let expected= ["delivery.json","prepare.json","acknowledge.json"].map(|name|format!("{scope}/{name}"));
        assert_eq!(selected.keys().cloned().collect::<std::collections::BTreeSet<_>>(),expected.iter().cloned().collect());
        for name in &expected {
            assert_eq!(selected[name],serde_json::Value::String(std::str::from_utf8(&before[name]).unwrap().into()));
        }
        let mirror=memory_notice::validate_retained_mirror_copy(&before[&expected[0]],&task,&limits,Path::new(scope),&path(&expected[0])).unwrap();
        let mut original=Vec::new();
        for (index,key) in [(1,"prepare_reply"),(2,"acknowledge_reply")] {
            let journal=mutations::read(&path(&expected[index]),&limits).unwrap().unwrap();
            assert_eq!(serde_json::to_value(&journal).unwrap(),serde_json::from_slice::<serde_json::Value>(&before[&expected[index]]).unwrap());
            let reply:Reply=serde_json::from_value(binding[key].clone()).unwrap();
            let id=journal.request.mutation_id().unwrap().to_owned();
            reply.validate_for(&Operation::GetOperation{operation_id:id.clone()}).unwrap();
            let Outcome::Ok{data:Data::Operation(record)}=&reply.outcome else {panic!("original memory receipt missing")};
            assert_eq!(record.id,id);
            assert_eq!(record.intended,serde_json::to_value(&journal.request).unwrap());
            assert_eq!(record.outcome,Some(serde_json::to_value(journal.outcome.as_ref().unwrap()).unwrap()));
            assert_eq!(reply.commit.as_ref(),Some(&journal.outcome_context.as_ref().unwrap().commit));
            assert_eq!(journal.consumed,index==1);
            match &journal.request {
                Operation::PrepareAgentMemory{caller,..} if index==1 => assert_eq!(caller,&collaboration_caller(attempt)),
                Operation::AcknowledgeAgentMemory{caller,batch_id,..} if index==2 => {
                    assert_eq!(caller,&collaboration_caller(attempt));
                    assert_eq!(Some(batch_id.as_str()),mirror["pending"]["batch"]["batch_id"].as_str());
                }
                _=>panic!("retained memory mutation kind differs"),
            }
            original.push(journal.outcome.unwrap());
        }
        let Outcome::Ok{data:Data::AgentMemory(prepared)}=&original[0] else {panic!("retained prepare was not committed successfully")};
        assert!(matches!(&original[1],Outcome::Refused{..}));
        let pending=prepared.pending.as_ref().unwrap();
        assert!(pending.page.notices.is_empty() && prepared.available.is_none());
        assert!(mirror["pending"]["delivery"].is_null());
        assert_eq!(mirror["pending"]["batch"],serde_json::to_value(pending).unwrap());
        assert_eq!(mirror["acknowledged_cursor"],serde_json::to_value(&prepared.acknowledged_cursor).unwrap());
        assert_eq!(mirror["start_sequence"],serde_json::json!(prepared.start_sequence));
    }
    let mut retained=None;
    // Every expected raw file remains exact. Only these separately bound,
    // canonically stopped StopAttempt instructions may remain incomplete.
    let stops=m["retained_stops"].as_object().expect("explicit retained StopAttempt map");
    let worker_id=m["worker_id"].as_str().expect("exact worker identity");
    let mut seen_stops=std::collections::BTreeSet::new();
    for name in files.keys() {
        if name.starts_with("commands/") {
            let receipt=read_receipt(&path(name),&limits).unwrap();
            assert!(serde_json::to_value(&receipt).unwrap()==serde_json::from_slice::<serde_json::Value>(&before[name]).unwrap(), "command codec would ignore or add data");
            if let Some(expected)=stops.get(name) {
                let expected_command:WorkerCommand=serde_json::from_value(expected["command"].clone()).unwrap();
                let task:Task=serde_json::from_value(expected["canonical_task"].clone()).unwrap();
                assert_eq!(receipt.command,expected_command);
                assert!(!receipt.completed && !receipt.effects_started);
                let WorkerCommand::StopAttempt{command_id,task_id,attempt_id,generation,incarnation}=&receipt.command else {panic!("retained stop is not StopAttempt")};
                assert_eq!(name,&format!("commands/{}.json",component(command_id)));
                assert!(matches!(task.status,TaskStatus::Failed|TaskStatus::Cancelled));
                let a=task.attempt.as_ref().expect("canonical stopped attempt");
                assert!(a.stopped && a.phase==AttemptPhase::Stopped);
                assert_eq!(&task.id,task_id);assert_eq!(&a.task_id,task_id);
                assert_eq!(&a.id,attempt_id);assert_eq!(a.generation,*generation);
                assert_eq!(&a.worker_incarnation,incarnation);assert_eq!(a.worker_id,worker_id);
                assert_eq!(a.session_id,task.session_id);
                assert!(seen_stops.insert(name.clone()));
            } else if name==m["retained_command_path"].as_str().unwrap_or("") { assert!(retained.replace(receipt).is_none()); }
            else { assert!(receipt.completed); }
        } else if (name.starts_with("mutations/") && !name.starts_with("mutations/settlements/")) || name.starts_with("collaboration/") {
            assert!(mutations::read(&path(name),&limits).unwrap().is_some());
        }
    }
    assert_eq!(seen_stops,stops.keys().cloned().collect());
    for name in files.keys().filter(|n| n.ends_with("/borg-collaboration-config.json")) {
        if Some(name.as_str())==m["retained_config_path"].as_str() {continue;}
        let mut cfg:codex_mcp::Config=serde_json::from_slice(&before[name]).unwrap();
        cfg.state=path(name).parent().unwrap().to_owned();
        assert_ne!(codex_mcp::outcome_disposition(&cfg).unwrap(),codex_mcp::OutcomeDisposition::Unresolved);
    }
    if let Some(receipt)=retained {
        let task:Task=serde_json::from_value(m["canonical_task"].clone()).unwrap();
        let WorkerCommand::RunAttempt{attempt,task:input,..}=&receipt.command else {panic!("retained command not RunAttempt")};
        assert!(stopped_uncertain_coding_binding(&task,input,attempt));
        let was_completed=m["retained_completed_before"].as_bool().expect("exact retained completion flag");
        assert_eq!(receipt.completed,was_completed);
        assert!(was_completed,"both retained uncertain commands are already completed");
        let expected_task=match m["worker_id"].as_str().unwrap() {
            "agent-lab-4"=>"task-f2dc0f0ed413f702c9fe9516f4ef47ef-00000000000000000123",
            "agent-lab-3"=>"task-f2dc0f0ed413f702c9fe9516f4ef47ef-00000000000000000124",
            _=>panic!("unknown retained uncertain task host"),
        };
        assert_eq!(task.id,expected_task);
        let cfg_path=m["retained_config_path"].as_str().unwrap();
        let mut cfg:codex_mcp::Config=serde_json::from_slice(&before[cfg_path]).unwrap();
        assert_eq!(cfg.caller,collaboration_caller(attempt));
        assert_eq!(cfg.worker_limits,limits.worker);
        assert_eq!(cfg.runtime_limits,*input.resolved_limits.as_ref().unwrap().global_metadata);
        assert_eq!(cfg.limits,input.execution_limits().unwrap().collaboration);
        assert_eq!(cfg.deadline_unix_ms,attempt.deadline_unix_ms.min(input.limits.deadline_unix_ms));
        // Only the copy's local read location changes; the retained Config and
        // journal bytes remain exact, with original URL/caller/deadline/policies.
        cfg.state=path(cfg_path).parent().unwrap().to_owned();
        assert_eq!(codex_mcp::outcome_disposition(&cfg).unwrap(),codex_mcp::OutcomeDisposition::Unresolved);
        assert!(m["completed_output"].is_null(),"no completion rewrite is permitted");
    } else { assert!(m["canonical_task"].is_null()); }
    // Every consumed settlement uses its original RunAttempt's accepted policy.
    // The production readers validate the full saved Journal and Phase; no
    // client, resolution, consumption, persistence or scheduler is invoked.
    let settlements=m["settlement_records"].as_object().expect("explicit original settlement map");
    assert_eq!(files.keys().filter(|n|n.starts_with("mutations/settlements/")).cloned().collect::<std::collections::BTreeSet<_>>(),settlements.keys().cloned().collect());
    let commands=command_receipts(&root,&limits).unwrap();
    let mut seen_phases=std::collections::BTreeSet::new();
    for (name,binding) in settlements {
        let original:serde_json::Value=serde_json::from_slice(&before[name]).unwrap();
        let request:Operation=serde_json::from_value(original["request"].clone()).unwrap();
        let Operation::SettleOperation{settlement_id,operation_id,task_id,attempt_id,generation,..}=&request else {panic!("unexpected settlement operation")};
        let mut matches=commands.iter().filter(|(_,receipt)|matches!(&receipt.command,WorkerCommand::RunAttempt{attempt,..} if &attempt.task_id==task_id && &attempt.id==attempt_id && &attempt.generation==generation));
        let (command_file,receipt)=matches.next().expect("original settlement command missing");
        assert!(matches.next().is_none(),"ambiguous original settlement command");
        let WorkerCommand::RunAttempt{command_id,task:input,attempt,..}=&receipt.command else {unreachable!()};
        assert_eq!(command_file,&path(binding["command_path"].as_str().unwrap()));
        assert_eq!(command_file,&root.join("commands").join(format!("{}.json",component(command_id))));
        assert_eq!(attempt.worker_id,worker_id);assert!(!attempt.worker_incarnation.is_empty());
        let task:Task=serde_json::from_value(binding["canonical_task"].clone()).unwrap();
        assert!(coding_recovery_binding(&task,input,attempt));
        input.execution_limits().unwrap();
        let accepted=input.resolved_limits.as_ref().unwrap().global_metadata.as_ref();
        let journal=mutations::read(&path(name),accepted).unwrap().unwrap();
        assert_eq!(serde_json::to_value(&journal).unwrap(),original,"journal codec would ignore or add data");
        assert_eq!(serde_json::to_value(&journal.request).unwrap(),serde_json::to_value(&request).unwrap());
        assert!(journal.consumed,"unconsumed settlement is outside this read-only proof");
        assert!(!operation_id.is_empty() && !settlement_id.is_empty() && operation_id!=settlement_id);
        assert!(path(name)==mutations::settlement_path(&root,operation_id)
            || path(name)==root.join("mutations/settlements").join(format!("settled-{}.json",component(settlement_id))));
        publication::validate_saved_settlement(&root,&receipt.command,&journal.request).unwrap();
        let phase_name=binding["phase_path"].as_str().expect("exact original publication path");
        assert_eq!(path(phase_name),publication::path(&root,attempt));
        let phase=publication::read(&path(phase_name),&receipt.command).unwrap().unwrap();
        assert_eq!(serde_json::to_value(&phase).unwrap(),serde_json::from_slice::<serde_json::Value>(&before[phase_name]).unwrap());
        seen_phases.insert(phase_name.to_owned());
    }
    assert_eq!(seen_phases,files.keys().filter(|n|n.starts_with("publications/")).cloned().collect());
    let ledgers=m["cleanup_records"].as_object().expect("explicit copied cleanup map");
    assert_eq!(files.keys().filter(|n|n.starts_with("cleanup/")).cloned().collect::<std::collections::BTreeSet<_>>(),ledgers.keys().cloned().collect());
    for (name,binding) in ledgers {
        let command_name=binding["command_path"].as_str().unwrap();
        assert!(files.contains_key(command_name));
        let receipt=read_receipt(&path(command_name),&limits).unwrap();
        let WorkerCommand::RunAttempt{command_id,task:input,attempt,..}=&receipt.command else {panic!("cleanup is not a coding RunAttempt")};
        assert_eq!(input.definition.executor,ExecutorKind::Coding);
        assert_eq!(attempt.worker_id,worker_id);
        let task:Task=serde_json::from_value(binding["canonical_task"].clone()).unwrap();
        assert!(coding_recovery_binding(&task,input,attempt));
        let current=task.attempt.as_ref().unwrap();
        assert_eq!(serde_json::to_value(&task.input).unwrap(),serde_json::to_value(input).unwrap());
        assert_eq!(current.id,attempt.id);assert_eq!(current.task_id,attempt.task_id);
        assert_eq!(current.session_id,attempt.session_id);assert_eq!(current.worker_id,worker_id);
        assert_eq!(current.worker_incarnation,attempt.worker_incarnation);assert_eq!(current.generation,attempt.generation);
        assert_eq!(name,&format!("cleanup/{}.json",component(&attempt.id)));
        assert_eq!(command_name,format!("commands/{}.json",component(command_id)));
        let value=cleanup::validate_retained_cleanup_copy(&path(name),&retention.worker,&selected.revision,command_id,&attempt.id).unwrap();
        assert_eq!(value,serde_json::from_slice::<serde_json::Value>(&before[name]).unwrap(),"cleanup codec would ignore or add data");
        if !value["status"].is_null(){assert_eq!(value["status"],serde_json::to_value(&task.status).unwrap());}
        if !value["checkpoint"].is_null(){assert_eq!(value["checkpoint"],serde_json::to_value(&task.selected_checkpoint).unwrap());}
        if current.uncertain {
            assert_eq!(value["disposition"],"retained");
            assert_eq!(value["reason"],"local_recovery");
            assert!(value["first_safe_ms"].is_null());
            assert!(!value["cache_started"].as_bool().unwrap());
            assert!(!value["cache_done"].as_bool().unwrap());
            for slot in value["worktrees"].as_array().unwrap(){
                assert!(!slot["started"].as_bool().unwrap() && !slot["done"].as_bool().unwrap());
            }
        }
    }
    for (name,bytes) in before {assert_eq!(fs::read(path(&name)).unwrap(),bytes);}
}
