//! Child of the existing governance_completion fixture; all successful records
//! use its actual Controller producers. No provider or worker process is run.
use super::*;
use borg_runtime::native::{NativeRetryBlock, RejectionKind, RetryBlockReason, RetryAfter};
use std::os::unix::fs::MetadataExt;

fn files(root: &std::path::Path) -> BTreeMap<PathBuf, (u64,u64,u32,Vec<u8>)> {
    fn visit(root:&std::path::Path,at:&std::path::Path,out:&mut BTreeMap<PathBuf,(u64,u64,u32,Vec<u8>)>) {
        for entry in std::fs::read_dir(at).unwrap() {
            let path=entry.unwrap().path();let meta=std::fs::symlink_metadata(&path).unwrap();
            assert!(!meta.file_type().is_symlink());
            if meta.is_dir() { visit(root,&path,out); }
            else {
                assert!(meta.is_file());assert!(out.len()<64);assert!(meta.len()<=64*1024*1024);
                out.insert(path.strip_prefix(root).unwrap().to_owned(),
                    (meta.dev(),meta.ino(),meta.mode(),std::fs::read(path).unwrap()));
            }
        }
    }
    let mut out=BTreeMap::new();visit(root,root,&mut out);out
}
struct Held {
    path:PathBuf, config:ControllerConfig, task:Task, hold:CompletionHold,
    actor:Caller, worker:Worker, originals:Vec<(Operation,Reply)>, block_report:Operation,
}
// Current-protocol setup for this retained-state fixture. The parent helpers
// intentionally retain their historical unmarked Usage vocabulary.
async fn current_active(c:&Controller,label:&str)->(Task,Attempt,Caller,Worker) {
    let mut input=native(&id(label));
    input.definition.execution_profile=Some(ExecutionProfile::CollaborationV2 { publication: None,web_search:None});
    let queued=Box::pin(submit(c,&id("submit"),input)).await;
    let w=worker(&id("worker"),"inc",1,0);
    Box::pin(register(c,&w)).await;
    let a=Box::pin(commands(c,&w,vec![])).await.into_iter().find_map(|command|match command {
        WorkerCommand::RunAttempt {attempt,..} if attempt.task_id==queued.id=>Some(attempt),_=>None,
    }).expect("ordinary admission of this exact task");
    task(Box::pin(report(c,&a,&id("started"),Observation::Started {thread_id:None,turn_id:None})).await);
    Box::pin(super::super::governance::completed_native_request(c,&a,Some(4),Some(2))).await;
    (Box::pin(get(c,&queued.id)).await,a.clone(),caller(&a),w)
}
async fn current_rejected_charge(c:&Controller,a:&Attempt,index:u32)->(Operation,Reply) {
    use borg_runtime::native::{CategoryDiagnostic,DiagnosticBodyState,HttpDiagnostic,HttpStatusDiagnostic,
        NumericDiagnostic,ProviderErrorCategory,ProviderFailureClass,ProviderFailureDiagnostic,
        QuotaDiagnostic,RetryHeaderDiagnostic,SchedulerRetryDiagnostic};
    task(Box::pin(report(c,a,&borg_runtime::native_loop_contract::reservation_id(&a.id,index),
        Observation::Usage {native_diagnostic_reserved:true,model_requests:index,
            input_tokens:Some(u64::from(index)*4),output_tokens:Some(u64::from(index)*2)})).await);
    let accepted=Box::pin(get(c,&a.task_id)).await;
    let operation=completion_report(a,borg_runtime::native_loop_contract::diagnostic_id(&a.id,index),
        Observation::NativeRequestDiagnostic {request_index:index,
            endpoint:"https://api.openai.com/v1/responses".into(),
            model:accepted.input.definition.required_inference().unwrap().model.clone(),
            request_binding:Some(completion_binding()),
            observed_outcome:borg_runtime::native_loop_contract::NativeRequestObservedOutcome::KnownRejected {
                rejection_kind:RejectionKind::ContextLimit},
            // Controlled complete HTTP facts matching the rejection below;
            // no provider request or implicit successful reply is fabricated.
            failure:Some(ProviderFailureDiagnostic::http(ProviderFailureClass::HttpRefused,HttpDiagnostic {
                status:HttpStatusDiagnostic::Value(400),body:DiagnosticBodyState::Complete,
                category:CategoryDiagnostic::Value(ProviderErrorCategory::ContextLimit),
                scheduler_retry_after:SchedulerRetryDiagnostic::Value(RetryAfter::Absent),
                retry_after:RetryHeaderDiagnostic::Absent,retry_after_ms:NumericDiagnostic::Absent,
                requests_limit:NumericDiagnostic::Absent,requests_remaining:NumericDiagnostic::Absent,
                tokens_limit:NumericDiagnostic::Absent,tokens_remaining:NumericDiagnostic::Absent,
                requests_reset_ms:NumericDiagnostic::Absent,tokens_reset_ms:NumericDiagnostic::Absent,
                quota:QuotaDiagnostic::Absent}))});
    let original=Box::pin(reply(c,operation.clone())).await;
    assert!(matches!(original.outcome,Outcome::Ok {data:Data::Acknowledged}),"{original:?}");
    assert!(original.commit.is_some());
    (operation,original)
}
async fn held() -> Held {
    let p=path();let mut cfg=governance_config();
    Arc::make_mut(&mut cfg.limits).native_retry=Some(borg_runtime::limits::NativeRetryLimits {
        initial_backoff_ms:10,backoff_multiplier:2,max_backoff_ms:80,jitter_ms:7,
        circuit:borg_runtime::limits::NativeCircuitLimits{failure_threshold:2,open_ms:50,max_entries:16},
        auth_cooldown_ms:150,auth_max_entries:16,coordination_record_bytes:32_768,
    });
    cfg.limits.validate().unwrap();
    let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    Box::pin(promote(&c,"startup-completion-native-holds",true)).await;
    let (accepted,a,actor,w)=Box::pin(current_active(&c,"startup-both-holds")).await;
    let question=Box::pin(pose(&c,&actor)).await;
    Box::pin(close_question(&c,&question,None)).await;
    let (decision,original,state)=Box::pin(evaluated(&c,&actor,1,true)).await;
    assert_eq!(state.consecutive_count,1);
    let diagnostic=Box::pin(current_rejected_charge(&c,&a,2)).await;
    let rejected=Box::pin(rejected_completion_request(&c,&a,2,
        RejectionKind::ContextLimit,RetryAfter::Absent)).await;
    let rejection=Box::pin(reply(&c,Operation::GetOperation{operation_id:rejected.operation_id.clone()})).await;
    let Outcome::Ok{data:Data::Operation(record)}=&rejection.outcome else {panic!("original rejection expected")};
    let original_rejection:Operation=serde_json::from_value(record.intended.clone()).unwrap();
    let rejection_reply=Box::pin(reply(&c,original_rejection.clone())).await;
    let block=NativeRetryBlock{request_index:2,source_operation_id:rejected.operation_id,
        reason:RetryBlockReason::ContextLimit};
    let block_report=completion_report(&a,id("startup-native-block"),
        Observation::NativeRetryBlocked{block:block.clone()});
    let blocked=Box::pin(reply(&c,block_report.clone())).await;
    assert_eq!(task(blocked.outcome.clone()).native_retry_block,Some(block.clone()));
    let stop=completion_report(&a,id("startup-stopped"),Observation::ProcessTreeStopped);
    let stopped=Box::pin(reply(&c,stop.clone())).await;
    let retained=task(stopped.outcome.clone());
    assert_eq!(retained.status,TaskStatus::Blocked);assert_eq!(retained.input,accepted.input);
    assert_eq!(retained.native_retry_block,Some(block));
    assert!(retained.attempt.as_ref().unwrap().stopped);
    drop(c);controller_owner_released(&p).await;
    let hold=stored_hold(&p,&retained.id).unwrap();assert_eq!(hold.state,state);
    assert_eq!(hold.decision,decision_ref(&decision));
    Held{path:p,config:cfg,task:retained,hold,actor,worker:w,
        originals:vec![(decision,original),diagnostic,(original_rejection,rejection_reply),
            (block_report.clone(),blocked),(stop,stopped)],block_report}
}
async fn verify_valid_restart(h:&Held,cfg:&ControllerConfig) {
    let c=Controller::open_with_config(&h.path,cfg.clone()).unwrap();
    assert_eq!(Box::pin(get(&c,&h.task.id)).await,h.task);
    for (op,r) in &h.originals {same_original(&Box::pin(reply(&c,op.clone())).await,r);}
    Box::pin(no_completion_resume(&c,&h.worker,&h.task.id,&h.actor.attempt_id)).await;
    assert_eq!(Box::pin(get(&c,&h.task.id)).await,h.task);
    drop(c);controller_owner_released(&h.path).await;
    assert_eq!(stored_hold(&h.path,&h.task.id),Some(h.hold.clone()));
}

#[tokio::test]
async fn schema16_restart_preserves_both_holds_under_different_current_retry_policy() {
    let h=Box::pin(held()).await;
    // Current retry policy absence cannot erase the original accepted block or
    // reset completion. The accepted snapshot remains the validation context.
    let mut changed=h.config.clone();let p=Arc::make_mut(&mut changed.limits);
    p.revision=format!("{}-current-retry-disabled",p.revision);p.native_retry=None;p.validate().unwrap();
    for cfg in [&h.config,&changed] {Box::pin(verify_valid_restart(&h,cfg)).await;}
    let c=Controller::open_with_config(&h.path,changed).unwrap();
    let retry=Operation::RetryTask{operation_id:id("startup-fatal-retry"),
        task_id:h.task.id.clone(),expected_revision:h.task.revision};
    let denied=Box::pin(reply(&c,retry.clone())).await;
    assert_eq!(code(denied.outcome.clone()),"retry_correction_required");
    same_original(&Box::pin(reply(&c,retry)).await,&denied);
    assert_eq!(Box::pin(get(&c,&h.task.id)).await,h.task);
    drop(c);controller_owner_released(&h.path).await;
    assert_eq!(stored_hold(&h.path,&h.task.id),Some(h.hold));
}

#[tokio::test]
async fn current_schema_query_only_startup_refuses_independent_hold_provenance_corruption() {
    for defect in ["completion-count","retry-source","retry-history"] {
        let h=Box::pin(held()).await;
        // A valid original reopen is a prerequisite for every negative variant.
        Box::pin(verify_valid_restart(&h,&h.config)).await;
        let db=rusqlite::Connection::open_with_flags(&h.path,rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE).unwrap();
        assert_eq!(db.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),24);
        let original_receipts:Vec<(String,String,String)>=db.prepare(
            "SELECT id,request,outcome FROM receipts ORDER BY id LIMIT 129").unwrap()
            .query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap()
            .collect::<Result<Vec<_>,_>>().unwrap();assert!(original_receipts.len()<=128);
        if defect=="retry-history" {
            let Operation::ObserveWorker{report}=&h.block_report else{unreachable!()};
            let body=serde_json::to_string(&report.observation).unwrap();
            assert_eq!(db.execute("DELETE FROM history WHERE task_id=?1 AND kind='worker_observation' AND body=?2",
                rusqlite::params![h.task.id,body]).unwrap(),1);
        } else {
            let body:String=db.query_row("SELECT body FROM tasks WHERE id=?1",[&h.task.id],|r|r.get(0)).unwrap();
            let (old,new)=if defect=="completion-count" {
                let mut changed=h.hold.clone();changed.state.consecutive_count+=1;
                // Both counts are within policy; only the original receipt join
                // distinguishes the canonical corruption from a valid hold.
                assert!(changed.state.consecutive_count<=h.config.limits.governance.as_ref().unwrap().completion_continuations);
                (serde_json::to_string(&h.hold).unwrap(),serde_json::to_string(&changed).unwrap())
            } else {
                let old=h.task.native_retry_block.as_ref().unwrap();let mut changed=old.clone();
                // This is a real original reservation, not a missing/fake ID.
                // It cannot prove the ContextLimit rejection attributed here.
                changed.source_operation_id=borg_runtime::native_loop_contract::reservation_id(&h.actor.attempt_id,2);
                (serde_json::to_string(old).unwrap(),serde_json::to_string(&changed).unwrap())
            };
            assert_eq!(body.matches(&old).count(),1);
            // Preserve StoredTask field order and every unrelated canonical byte.
            let changed=body.replacen(&old,&new,1);
            assert_eq!(db.execute("UPDATE tasks SET body=?2 WHERE id=?1",rusqlite::params![h.task.id,changed]).unwrap(),1);
        }
        let after_receipts:Vec<(String,String,String)>=db.prepare(
            "SELECT id,request,outcome FROM receipts ORDER BY id LIMIT 129").unwrap()
            .query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap()
            .collect::<Result<Vec<_>,_>>().unwrap();assert_eq!(after_receipts,original_receipts);
        drop(db);let before=files(h.path.parent().unwrap());
        for _ in 0..2 {
            let error=Controller::open_with_config(&h.path,h.config.clone()).err().expect("corrupt retained hold must refuse startup");
            let expected=if defect=="completion-count" {"invalid retained completion hold"} else {"invalid retained native retry block"};
            assert!(error.contains(expected),"wrong refusal for {defect}: {error}");
            controller_owner_released(&h.path).await;
            assert_eq!(files(h.path.parent().unwrap()),before,"{defect}: failed startup rewrote retained files");
        }
    }
}

// No task is created: these original decisions must remain readable even when
// there is no task-lifetime snapshot from which to recover old global defaults.
struct TaskFree {
    path:PathBuf,config:ControllerConfig,configuration:ConfigurationVersion,
    originals:Vec<(Operation,Reply)>,
}
async fn task_free() -> TaskFree {
    let p=path();let cfg=governance_config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let query=Box::pin(reply(&c,Operation::QueryGovernance{query:GovernanceQuery::Configuration{revision:cfg.limits.revision.clone()}})).await;
    let Outcome::Ok{data:Data::Governance(result)}=query.outcome else{panic!("bootstrap required")};
    let GovernanceResult::Configuration(mut configuration)=*result else{panic!("configuration required")};
    let rule=RuleVersion{reference:RuleRef{id:id("retained-rule"),revision:"1".into()},body:RuleBody::Completion{allow_continuation:true}};
    let operation=Operation::MutateGovernance{operation_id:id("retained-register-rule"),mutation:GovernanceMutation::RegisterRule{rule:rule.clone()}};
    let original=Box::pin(reply(&c,operation.clone())).await;assert!(matches!(original.outcome,Outcome::Ok{..}));assert!(original.commit.is_some());
    configuration.revision=id("retained-configuration");configuration.completion=rule.reference;
    let register=Operation::MutateGovernance{operation_id:id("retained-register-configuration"),mutation:GovernanceMutation::RegisterConfiguration{configuration:configuration.clone()}};
    let registered=Box::pin(reply(&c,register.clone())).await;assert!(matches!(registered.outcome,Outcome::Ok{..}));assert!(registered.commit.is_some());
    drop(c);controller_owner_released(&p).await;
    let db=rusqlite::Connection::open_with_flags(&p,rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    assert_eq!(db.query_row("SELECT count(*) FROM tasks",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    assert_eq!(db.query_row("SELECT count(*) FROM sessions",[],|r|r.get::<_,i64>(0)).unwrap(),0);drop(db);
    TaskFree{path:p,config:cfg,configuration,originals:vec![(operation,original),(register,registered)]}
}
fn changed_global(original:&ControllerConfig)->ControllerConfig {
    let mut cfg=original.clone();let metadata=Arc::make_mut(&mut cfg.limits);
    metadata.revision=format!("{}-replacement",metadata.revision);
    metadata.execution.native.context_bytes/=2;
    let policy=metadata.definition_updates.as_mut().unwrap();policy.revision=format!("{}-replacement",policy.revision);
    metadata.validate().unwrap();cfg
}
async fn assert_retained_registration(c:&Controller,f:&TaskFree) {
    for (operation,original) in &f.originals {
        same_original(&Box::pin(reply(c,operation.clone())).await,original);
        let lookup=Box::pin(reply(c,Operation::QueryGovernance{query:GovernanceQuery::Decision{decision:DecisionRef{
            operation_id:operation.mutation_id().unwrap().into(),point:DecisionPoint::RuleAdministration}}})).await;
        let Outcome::Ok{data:Data::Governance(value)}=lookup.outcome else{panic!("retained audit required")};
        let GovernanceResult::Decision(record)=*value else{panic!("decision required")};
        assert_eq!(record.configuration_metadata.as_deref(),Some(f.config.limits.as_ref()));
        assert_eq!(record.configuration.global_revision,f.config.limits.revision);
    }
    let original=Box::pin(reply(c,Operation::QueryGovernance{query:GovernanceQuery::Configuration{revision:f.configuration.revision.clone()}})).await;
    assert_eq!(original.outcome,Outcome::Ok{data:Data::Governance(Box::new(GovernanceResult::Configuration(f.configuration.clone())))});
}
#[tokio::test]
async fn task_free_governance_metadata_survives_new_defaults_and_keeps_fresh_admission_strict() {
    let f=Box::pin(task_free()).await;let changed=changed_global(&f.config);
    for _ in 0..2 {
        let c=Controller::open_with_config(&f.path,changed.clone()).unwrap();
        Box::pin(assert_retained_registration(&c,&f)).await;
        drop(c);controller_owner_released(&f.path).await;
    }
    let c=Controller::open_with_config(&f.path,changed.clone()).unwrap();
    let missing=Operation::QueryGovernance{query:GovernanceQuery::Configuration{revision:id("missing-configuration")}};
    for _ in 0..2 {
        let absent=Box::pin(reply(&c,missing.clone())).await;
        assert!(absent.commit.is_none());
        assert_eq!(code(absent.outcome),"not_found");
    }
    Box::pin(assert_retained_registration(&c,&f)).await;
    let mut stale=f.configuration.clone();stale.revision=id("new-id-old-metadata");
    let denied=Box::pin(reply(&c,Operation::MutateGovernance{operation_id:id("stale-register"),mutation:GovernanceMutation::RegisterConfiguration{configuration:stale}})).await;
    assert_eq!(code(denied.outcome),"configuration_conflict");
    // Exact current metadata may admit a new immutable version; old snapshots
    // remain independently bound, even with different effective defaults/policy.
    let mut fresh=f.configuration.clone();fresh.revision=id("fresh-configuration");
    fresh.global_revision=changed.limits.revision.clone();fresh.definition_update_policy_revision=changed.limits.definition_updates.as_ref().map(|p|p.revision.clone());
    let added=Box::pin(reply(&c,Operation::MutateGovernance{operation_id:id("fresh-register"),mutation:GovernanceMutation::RegisterConfiguration{configuration:fresh}})).await;
    assert!(matches!(added.outcome,Outcome::Ok{..}));assert!(added.commit.is_some());
    drop(c);controller_owner_released(&f.path).await;
    let c=Controller::open_with_config(&f.path,changed).unwrap();Box::pin(assert_retained_registration(&c,&f)).await;
    drop(c);controller_owner_released(&f.path).await;
    let db=rusqlite::Connection::open_with_flags(&f.path,rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    assert_eq!(db.query_row("SELECT count(*) FROM tasks",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    assert_eq!(db.query_row("SELECT count(*) FROM sessions",[],|r|r.get::<_,i64>(0)).unwrap(),0);
}
#[tokio::test]
async fn contradictory_governance_metadata_snapshot_refuses_without_rewriting_originals() {
    let f=Box::pin(task_free()).await;let changed=changed_global(&f.config);
    let c=Controller::open_with_config(&f.path,changed.clone()).unwrap();Box::pin(assert_retained_registration(&c,&f)).await;
    drop(c);controller_owner_released(&f.path).await;
    {
        let db=rusqlite::Connection::open_with_flags(&f.path,rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE).unwrap();
        let id=f.originals[0].0.mutation_id().unwrap();
        let raw:String=db.query_row("SELECT governance FROM receipts WHERE id=?1",[id],|r|r.get(0)).unwrap();
        let mut records:Vec<DecisionRecord>=serde_json::from_str(&raw).unwrap();
        let metadata=records[0].configuration_metadata.as_mut().unwrap();metadata.execution.native.context_bytes/=2;metadata.validate().unwrap();
        assert_eq!(metadata.revision,f.config.limits.revision); // same identity, contradictory valid values
        assert_eq!(db.execute("UPDATE receipts SET governance=?2 WHERE id=?1",rusqlite::params![id,serde_json::to_string(&records).unwrap()]).unwrap(),1);
    }
    let before=files(f.path.parent().unwrap());
    for _ in 0..2 {
        let error=Controller::open_with_config(&f.path,changed.clone()).err().expect("contradictory immutable metadata must refuse");
        assert!(error.contains("invalid retained governance"),"{error}");controller_owner_released(&f.path).await;
        assert_eq!(files(f.path.parent().unwrap()),before);
    }
}
#[tokio::test]
async fn legacy_governance_without_original_metadata_refuses_new_defaults_without_inventing_context() {
    let f=Box::pin(task_free()).await;
    // Representation compatibility fixture, not a claimed historical producer:
    // remove only the optional new field from actual original typed audits.
    {
        let db=rusqlite::Connection::open_with_flags(&f.path,rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE).unwrap();
        for (op,_) in &f.originals {
            let id=op.mutation_id().unwrap();let raw:String=db.query_row("SELECT governance FROM receipts WHERE id=?1",[id],|r|r.get(0)).unwrap();
            let mut records:Vec<DecisionRecord>=serde_json::from_str(&raw).unwrap();
            for record in &mut records {record.configuration_metadata=None;}
            let legacy=serde_json::to_string(&records).unwrap();assert!(!legacy.contains("configuration_metadata"));
            assert_eq!(serde_json::to_string(&serde_json::from_str::<Vec<DecisionRecord>>(&legacy).unwrap()).unwrap(),legacy);
            assert_eq!(db.execute("UPDATE receipts SET governance=?2 WHERE id=?1",rusqlite::params![id,legacy]).unwrap(),1);
        }
    }
    let c=Controller::open_with_config(&f.path,f.config.clone()).unwrap();
    for (operation,original) in &f.originals {same_original(&Box::pin(reply(&c,operation.clone())).await,original);}
    drop(c);controller_owner_released(&f.path).await;
    let before=files(f.path.parent().unwrap());let changed=changed_global(&f.config);
    for _ in 0..2 {
        let error=Controller::open_with_config(&f.path,changed.clone()).err().expect("unknown original metadata must refuse");
        assert!(error.contains("invalid retained governance"),"{error}");controller_owner_released(&f.path).await;
        assert_eq!(files(f.path.parent().unwrap()),before);
    }
}
