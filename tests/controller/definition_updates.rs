// Actual controller transactions using controlled callers, not live models.
// The historical targets remain ordinary V1 definitions. Only the separate,
// explicitly selected manager supplies the current bound update authority.
fn definition_update_config() -> ControllerConfig {
    let mut cfg = config();
    Arc::make_mut(&mut cfg.limits).identity_administration = Some(borg_runtime::limits::IdentityAdministrationPolicy {
        version: 1, revision: "definition-update-manager-policy".into(), enabled: true,
        manager_agent_id: "charter".into(), complete_definitions: true,
    });
    cfg.limits.validate().unwrap();
    cfg
}
async fn definition_update_fixture(c: &Controller, label: &str) -> (Task, Attempt, Caller) {
    let mut manager = native("charter");
    manager.definition.execution_profile = Some(ExecutionProfile::CollaborationV2 { web_search: None });
    let mut collaboration = c.limits().execution.collaboration.clone();
    collaboration.capabilities = Some(borg_runtime::limits::CollaborationCapabilities {
        version: 1, task_creation: false, identity_administration: true,
    });
    let mut native_limits = c.limits().execution.native.clone();
    native_limits.context_bytes = 524_288;
    native_limits.schema_bytes = 262_144;
    native_limits.json_nodes = 65_536;
    native_limits.function_count = 74;
    manager.definition.limit_overrides = Some(Box::new(borg_runtime::limits::ExecutionLimitOverrides {
        native: Some(native_limits), collaboration: Some(collaboration), ..Default::default()
    }));
    let manager = submit(c, &format!("{label}-manager-submit"), manager).await;
    let w = worker(&format!("{label}-manager-worker"), "inc", 1, 0);
    register(c, &w).await;
    let a = run(&commands(c, &w, vec![]).await);
    assert_eq!(a.task_id, manager.id);
    task(report(c, &a, &format!("{label}-manager-start"), Observation::Started { thread_id: None, turn_id: None }).await);
    let caller = Caller { task_id: a.task_id, attempt_id: a.id, generation: a.generation,
        worker_id: a.worker_id, incarnation: a.worker_incarnation };
    let (target, attempt, _) = fixture(c, label).await;
    assert_ne!(target.id, caller.task_id);
    assert_eq!(target.input.definition.execution_profile, Some(ExecutionProfile::CollaborationV1 { web_search: None }));
    (target, attempt, caller)
}

fn definition_change_args(c: &Controller, definition: &Definition, changes: serde_json::Value) -> serde_json::Value {
    json!({"agent_id":definition.id, "expected_revision":definition.revision,
        "expected_global_revision":c.limits().revision,
        "expected_policy_revision":c.limits().definition_updates.as_ref().map(|v|v.revision.as_str()).unwrap_or("v2-definition-updates-1"),
        "changes":changes})
}
fn definition_change(reply: &Reply) -> &DefinitionUpdateOutcome {
    match &reply.outcome {
        Outcome::Ok { data:Data::DefinitionUpdate(value) } => value,
        other => panic!("expected actual definition application, got {other:?}"),
    }
}
async fn selected_definition(c: &Controller, id: &str) -> Definition {
    match call_fn(c, Operation::GetAgent { agent_id:id.into() }).await {
        Outcome::Ok { data:Data::Agent(value) } => value.definition,
        other => panic!("expected selected agent, got {other:?}"),
    }
}
// Traceability scope: Real store applies a permitted patch and resolves later
// task input from the selected head; no live model/provider claim.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# The shared agent interface MUST expose supported definition-limit changes through the canonical controller operation layer.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# A permitted definition-limit change MUST atomically commit its immutable definition, selected head and durable operation outcome.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# A definition-limit change MUST preserve the executor, inference settings and tool capabilities of its predecessor.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# A configuration change outcome MUST distinguish future-task selection from settings retained by already accepted tasks.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# Submitting an explicitly pinned definition MUST NOT change an existing agent's selected definition revision.
#[tokio::test]
async fn definition_updates_apply_future_settings_keep_old_pins_and_assignment() {
    let c=Controller::open_with_config(path(),definition_update_config()).unwrap();
    let (old, _, actor)=definition_update_fixture(&c,"definition-apply").await;
    let before=old.input.definition.clone();
    let accepted=serde_json::to_vec(&get(&c,&old.id).await.input).unwrap();
    let old_pending=submit(&c,"old-pending",profiled("definition-apply")).await;
    let effective=c.limits().resolve_definition(&before).unwrap();
    let next_context=effective.native.context_bytes*2;
    let call=invocation(&actor,"definition-apply-one","collab_definition_update",
        definition_change_args(&c,&before,json!({"native.context_bytes":next_context})));
    let reply=invoke(&c,&call).await;
    let result=definition_change(&reply);
    assert!(reply.commit.is_some());
    assert_eq!(result.classification,ConfigurationChangeClass::Widening);
    assert_eq!(result.effective_for,ConfigurationEffect::FutureSubmissions);
    assert_eq!(result.previous_revision,before.revision);
    assert_eq!(result.global_revision,c.limits().revision);
    let selected=selected_definition(&c,&before.id).await;
    assert_eq!(selected,result.selected_definition);
    assert_ne!(selected.revision,before.revision);
    assert_eq!(selected.executor,before.executor);
    assert_eq!(selected.inference,before.inference);
    assert_eq!(selected.execution_profile,before.execution_profile);
    let mut expected=effective.clone(); expected.native.context_bytes=next_context;
    assert_eq!(c.limits().resolve_definition(&selected).unwrap(),expected);
    let overrides=selected.limit_overrides.as_ref().unwrap();
    assert_eq!(overrides.native.as_ref(),Some(&expected.native));
    assert!(overrides.coding.is_none() && overrides.collaboration.is_none() && overrides.memory_notice.is_none());
    assert_eq!(serde_json::to_vec(&get(&c,&old.id).await.input).unwrap(),accepted);
    // Explicit old submission is still legal and does not undo the applied head.
    let old_late=submit(&c,"old-explicit-after-apply",profiled("definition-apply")).await;
    assert_eq!(old_late.input.definition,before);
    assert_eq!(selected_definition(&c,&before.id).await,selected);
    let assigned=task(call_fn(&c,Operation::AssignTask{operation_id:"assign-retained-revision".into(),task_id:old_pending.id.clone(),expected_revision:old_pending.revision,agent_id:before.id.clone()}).await);
    assert_eq!(assigned.input.definition,before);
    let mut new_input=profiled("definition-apply");new_input.definition=selected.clone();
    let new_task=submit(&c,"new-selected-task",new_input).await;
    assert_eq!(new_task.input.resolved_limits.as_ref().unwrap().values,expected);
    assert_eq!(new_task.input.definition,selected);
    assert_eq!(new_task.input.limits.model_requests,old.input.limits.model_requests);
    let lookup_reply=lookup(&c,&call).await;
    let recovered=mcp::recovered(&call,lookup_reply).unwrap();
    assert_eq!(recovered.reply.outcome,reply.outcome);
    assert_eq!(recovered.reply.commit,reply.commit);
}
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# A definition-limit change MUST compare the expected selected definition revision before applying a new immutable revision.
#[tokio::test]
async fn definition_updates_competing_cas_has_one_head_and_original_refusal() {
    let c=Controller::open_with_config(path(),definition_update_config()).unwrap();
    let (task,_,actor)=definition_update_fixture(&c,"definition-cas").await;
    let n=c.limits().execution.native.context_bytes;
    let a=invocation(&actor,"cas-a","collab_definition_update",definition_change_args(&c,&task.input.definition,json!({"native.context_bytes":n+1})));
    let b=invocation(&actor,"cas-b","collab_definition_update",definition_change_args(&c,&task.input.definition,json!({"native.context_bytes":n+2})));
    let (ar,br)=tokio::join!(invoke(&c,&a),invoke(&c,&b));
    let (winner,loser,loser_call)=if matches!(ar.outcome,Outcome::Ok{..}) {(&ar,&br,&b)} else {(&br,&ar,&a)};
    assert_eq!(code(loser.outcome.clone()),"configuration_conflict");
    assert_eq!(selected_definition(&c,&task.assigned_agent_id).await,definition_change(winner).selected_definition);
    assert_eq!(invoke(&c,loser_call).await.outcome,loser.outcome);
}
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# A definition-limit change MUST be restricted to the fields allowed by the configured change policy.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# A definition-limit change MUST validate the complete resulting effective configuration before commit.
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# A definition-limit change MUST NOT grant host administration, credentials or protected release authority.
#[tokio::test]
async fn definition_updates_refuse_unknown_disallowed_zero_crossfield_and_authority() {
    let mut cfg=definition_update_config();
    Arc::make_mut(&mut cfg.limits).definition_updates.as_mut().unwrap().allowed_fields=vec![borg_runtime::limits::DefinitionLimitField::NativeContextBytes];
    let c=Controller::open_with_config(path(),cfg).unwrap();
    let (task,_,actor)=definition_update_fixture(&c,"definition-invalid").await;
    for (id,changes,expected) in [
        ("unknown",json!({"native.model_requests":1}),"invalid_input"),
        ("disallowed",json!({"coding.summary_bytes":10}),"configuration_policy"),
        ("zero",json!({"native.context_bytes":0}),"invalid_input"),
        ("empty",json!({}),"invalid_input"),
        ("inconsistent",json!({"native.context_bytes":1}),"invalid_limits"),
        ("representation",json!({"native.context_bytes":u64::from(u32::MAX)+1}),"invalid_limits"),
        ("fraction",json!({"native.context_bytes":1.5}),"invalid_input"),
        ("host",json!({"host_admin":1}),"invalid_input"),
        ("policy",json!({"definition_updates.allowed_fields":1}),"invalid_input"),
    ] {
        let req=invocation(&actor,&format!("update-{id}"),"collab_definition_update",definition_change_args(&c,&task.input.definition,changes));
        let r=invoke(&c,&req).await;
        assert_eq!(code(r.outcome),expected,"{id}");
        assert_eq!(selected_definition(&c,&task.assigned_agent_id).await,task.input.definition);
    }
    for field in ["executor","inference","execution_profile","credentials","protected_release","limits"] {
        let mut args=definition_change_args(&c,&task.input.definition,json!({"native.context_bytes":c.limits().execution.native.context_bytes+1}));
        args[field]=json!({"value":"not-authority"});
        let req=invocation(&actor,&format!("forbidden-{field}"),"collab_definition_update",args);
        assert_eq!(code(invoke(&c,&req).await.outcome),"invalid_input");
    }
}
//= spec/impl/v2/implementation.md#operational-feedback-applies-supported-configuration
//= type=test
//# A definition-limit change MUST compare the expected active global metadata and change-policy revisions before application.
#[tokio::test]
async fn definition_updates_use_active_policy_not_callers_old_metadata() {
    let p=path();let mut cfg=definition_update_config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let (task,_,actor)=definition_update_fixture(&c,"definition-current-policy").await;
    let old=definition_change_args(&c,&task.input.definition,json!({"native.context_bytes":c.limits().execution.native.context_bytes*2}));
    let pinned=serde_json::to_vec(&get(&c,&task.id).await.input).unwrap();
    drop(c);controller_owner_released(&p).await;
    let current=Arc::make_mut(&mut cfg.limits);current.revision="current-global".into();
    current.definition_updates.as_mut().unwrap().revision="current-policy".into();
    current.execution.native.output_bytes-=1;current.validate().unwrap();
    let c=Controller::open_with_config(&p,cfg).unwrap();
    let old_req=invocation(&actor,"old-global-update","collab_definition_update",old.clone());
    assert_eq!(code(invoke(&c,&old_req).await.outcome),"configuration_conflict");
    let mut old_policy=old;old_policy["expected_global_revision"]=json!(c.limits().revision);
    let req=invocation(&actor,"old-policy-update","collab_definition_update",old_policy);
    assert_eq!(code(invoke(&c,&req).await.outcome),"configuration_conflict");
    let req=invocation(&actor,"active-policy-update","collab_definition_update",definition_change_args(&c,&task.input.definition,json!({"native.context_bytes":c.limits().execution.native.context_bytes*2})));
    let reply=invoke(&c,&req).await;
    let result=definition_change(&reply);
    assert_eq!(result.policy.revision,"current-policy");
    assert_eq!(result.global_revision,"current-global");
    assert_eq!(result.selected_definition.limit_overrides.as_ref().unwrap().native.as_ref().unwrap().output_bytes,c.limits().execution.native.output_bytes);
    assert_eq!(serde_json::to_vec(&get(&c,&task.id).await.input).unwrap(),pinned);
}
#[tokio::test]
async fn definition_updates_absent_policy_disables_new_calls_and_preserves_old_bytes() {
    let mut cfg=definition_update_config();Arc::make_mut(&mut cfg.limits).definition_updates=None;
    let old=serde_json::to_vec(cfg.limits.as_ref()).unwrap();
    assert!(!serde_json::from_slice::<serde_json::Value>(&old).unwrap().as_object().unwrap().contains_key("definition_updates"));
    let recovered:borg_runtime::limits::RuntimeLimits=serde_json::from_slice(&old).unwrap();
    assert_eq!(serde_json::to_vec(&recovered).unwrap(),old);recovered.validate().unwrap();
    let c=Controller::open_with_config(path(),cfg).unwrap();
    let (task,_,actor)=definition_update_fixture(&c,"definition-disabled").await;
    let req=invocation(&actor,"disabled-update","collab_definition_update",definition_change_args(&c,&task.input.definition,json!({"native.context_bytes":c.limits().execution.native.context_bytes+1})));
    assert_eq!(code(invoke(&c,&req).await.outcome),"configuration_disabled");
    assert_eq!(selected_definition(&c,&task.assigned_agent_id).await,task.input.definition);
}
#[tokio::test]
async fn definition_updates_replay_after_cancel_restart_and_conflict_without_new_effect() {
    let p=path();let mut cfg=definition_update_config();let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let (task,_,actor)=definition_update_fixture(&c,"definition-replay").await;
    let req=invocation(&actor,"definition-exact-original","collab_definition_update",definition_change_args(&c,&task.input.definition,json!({"native.context_bytes":c.limits().execution.native.context_bytes+1})));
    let first=invoke(&c,&req).await;let expected=definition_change(&first).selected_definition.clone();
    let mut changed=req.clone();changed.arguments["changes"]["native.context_bytes"]=json!(c.limits().execution.native.context_bytes+2);
    assert_eq!(code(invoke(&c,&changed).await.outcome),"operation_conflict");
    let current=get(&c,&actor.task_id).await;
    assert_eq!(current.assigned_agent_id, "charter");
    task_fn_cancel(&c,&current,"cancel-definition-caller").await;
    drop(c);controller_owner_released(&p).await;
    Arc::make_mut(&mut cfg.limits).definition_updates=None;
    let c=Controller::open_with_config(&p,cfg).unwrap();
    let again=invoke(&c,&req).await;
    assert_eq!(first.outcome,again.outcome);assert_eq!(first.commit,again.commit);
    assert_eq!(selected_definition(&c,&task.assigned_agent_id).await,expected);
    let mut next=req.clone();next.operation_id="new-after-cancel".into();next.logical_call_id="new-after-cancel".into();
    assert!(matches!(invoke(&c,&next).await.outcome,Outcome::Refused{..}));
}
async fn task_fn_cancel(c:&Controller,t:&Task,id:&str) {
    assert!(matches!(call_fn(c,Operation::CancelTask{operation_id:id.into(),task_id:t.id.clone(),expected_revision:t.revision}).await,Outcome::Ok{data:Data::Task(_)}));
}
#[cfg(debug_assertions)]
#[tokio::test]
async fn definition_updates_actual_commit_failure_leaves_no_revision_head_or_receipt() {
    let p=path();let mut cfg=definition_update_config();
    let fail=Arc::new(std::sync::atomic::AtomicBool::new(false));cfg.qualification_fail_next_commit=Some(fail.clone());
    let c=Controller::open_with_config(&p,cfg).unwrap();
    let (task,_,actor)=definition_update_fixture(&c,"definition-rollback").await;
    let req=invocation(&actor,"definition-commit-failed","collab_definition_update",definition_change_args(&c,&task.input.definition,json!({"native.context_bytes":c.limits().execution.native.context_bytes+1})));
    fail.store(true,Ordering::SeqCst);let failed=invoke(&c,&req).await;
    assert!(!fail.load(Ordering::SeqCst));assert_eq!(code(failed.outcome),"store_unavailable");assert!(failed.commit.is_none());
    assert_eq!(selected_definition(&c,&task.assigned_agent_id).await,task.input.definition);
    assert_eq!(code(lookup(&c,&req).await.outcome),"not_found");
    drop(c);controller_owner_released(&p).await;
    let db=rusqlite::Connection::open(&p).unwrap();
    assert_eq!(db.query_row("SELECT count(*) FROM definitions WHERE id=?1",[&task.assigned_agent_id],|r|r.get::<_,i64>(0)).unwrap(),1);
    drop(db);
    let c=Controller::open_with_config(&p,definition_update_config()).unwrap();
    let recovered=invoke(&c,&req).await;assert_eq!(definition_change(&recovered).selected_definition.revision,"definition-1");
}
#[tokio::test]
async fn definition_updates_classify_effective_values_and_skip_existing_revision_spelling() {
    let c=Controller::open_with_config(path(),definition_update_config()).unwrap();
    let (task,_,actor)=definition_update_fixture(&c,"definition-classification").await;
    let mut collision=profiled("definition-classification");collision.definition.revision="definition-1".into();
    submit(&c,"existing-generated-spelling",collision).await;
    let n=c.limits().execution.native.context_bytes;
    let neutral=invocation(&actor,"neutral-definition","collab_definition_update",definition_change_args(&c,&task.input.definition,json!({"native.context_bytes":n})));
    let r=invoke(&c,&neutral).await;let head=definition_change(&r);
    assert_eq!(head.classification,ConfigurationChangeClass::Neutral);
    assert_eq!(head.selected_definition.revision,"definition-2");
    let narrow=invocation(&actor,"narrow-definition","collab_definition_update",definition_change_args(&c,&head.selected_definition,json!({"native.context_bytes":n-1})));
    let r=invoke(&c,&narrow).await;assert_eq!(definition_change(&r).classification,ConfigurationChangeClass::Narrowing);
    let mut bad=c.limits().clone();let p=bad.definition_updates.as_mut().unwrap();p.allowed_fields.push(p.allowed_fields[0]);
    assert!(bad.validate().unwrap_err().contains("definition_updates.allowed_fields"));
}

// Frozen old declarations test the exact two changed decoder boundaries. Their
// unchanged nested types are reused; this is not an old-binary startup test.
mod pre_update_readers {
    use borg_runtime::{limits::*, protocol::*};
    use serde::{Deserialize,Serialize};
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct LegacyRuntimeLimits {
    pub runtime: RuntimeThreadLimits,
    pub constraints: RepresentationConstraints,
    pub schema_version: u32,
    pub revision: String,
    pub controller: ControllerLimits,
    pub transport: TransportLimits,
    pub worker: WorkerLimits,
    pub product: ProductLimits,
    pub repository: RepositoryLimits,
    pub interface: InterfaceLimits,
    pub execution: ExecutionLimits,
    pub task_defaults: TaskDefaults,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum LegacyData {
    RepositoryRegistration(borg_runtime::repository_catalogue::Entry),
    RepositoryRegistrations(borg_runtime::collaboration::Page<borg_runtime::repository_catalogue::Entry>),
    AgentMemory(borg_runtime::collaboration::AgentMemoryObservation),
    Repository(Box<borg_runtime::repository_view::Output>),
    Collaboration(borg_runtime::collaboration::QueryResult),
    CollaborationPage(borg_runtime::mcp::ListPage),
    ProtectedRelease(Box<borg_runtime::protected_release::StageView>),
    Release(Box<ReleaseCandidate>),
    Releases(Vec<ReleaseCandidate>),
    Agent(AgentSummary),
    Agents(Vec<AgentSummary>),
    Task(Box<Task>),
    Tasks(Vec<Task>),
    Worker(Worker),
    Commands(Vec<WorkerCommand>),
    Operation(OperationRecord),
    Memory(Option<MemoryValue>),
    Acknowledged,
}
    #[test]
    fn definition_updates_legacy_readers_accept_old_bytes_and_refuse_new_records() {
        let mut old=RuntimeLimits::seed().unwrap();
        // This frozen pre-update decoder has none of the later optional blocks.
        // Preserve its exact boundary; only definition_updates differs below.
        old.definition_updates=None;old.retention=None;old.workflow=None;
        old.governance=None;old.charter=None;old.native_retry=None;
        let old_bytes=serde_json::to_vec(&old).unwrap();
        let old_reader:LegacyRuntimeLimits=serde_json::from_slice(&old_bytes).unwrap();
        assert_eq!(serde_json::to_vec(&old_reader).unwrap(),old_bytes);
        let new_reader:RuntimeLimits=serde_json::from_slice(&old_bytes).unwrap();
        assert_eq!(serde_json::to_vec(&new_reader).unwrap(),old_bytes);
        let mut new=old.clone();new.definition_updates=RuntimeLimits::seed().unwrap().definition_updates;assert!(new.definition_updates.is_some());
        let new_bytes=serde_json::to_vec(&new).unwrap();
        let error=serde_json::from_slice::<LegacyRuntimeLimits>(&new_bytes).unwrap_err();
        assert!(error.to_string().contains("definition_updates"));
        let definition=Definition{charter: None, limit_overrides:None,execution_profile:Some(ExecutionProfile::CollaborationV1{web_search:None}),inference:Some(InferenceSettings::selected()),id:"compat-agent".into(),revision:"compat-1".into(),executor:ExecutorKind::Native};
        let old_data=Data::Agent(AgentSummary{id:definition.id.clone(),definition:definition.clone(),retired:false});
        let old_data_bytes=serde_json::to_vec(&old_data).unwrap();
        let decoded:LegacyData=serde_json::from_slice(&old_data_bytes).unwrap();
        assert_eq!(serde_json::to_vec(&decoded).unwrap(),old_data_bytes);
        let new_data=Data::DefinitionUpdate(Box::new(DefinitionUpdateOutcome{
            previous_revision:"compat-0".into(),selected_definition:definition,
            global_revision:new.revision,policy:new.definition_updates.unwrap(),
            changed_fields:vec![DefinitionLimitField::NativeContextBytes],
            classification:ConfigurationChangeClass::Widening,effective_for:ConfigurationEffect::FutureSubmissions,
        }));
        let bytes=serde_json::to_vec(&new_data).unwrap();
        assert!(serde_json::from_slice::<LegacyData>(&bytes).unwrap_err().to_string().contains("definition_update"));
        assert_eq!(serde_json::from_slice::<Data>(&bytes).unwrap(),new_data);
    }
}
#[tokio::test]
async fn definition_updates_old_wire_is_refused_before_receipt_or_head_mutation() {
    assert_eq!(WIRE_REVISION,25);
    let c=Controller::open_with_config(path(),definition_update_config()).unwrap();
    let (task,_,actor)=definition_update_fixture(&c,"definition-wire").await;
    let req=invocation(&actor,"wire-bound-update","collab_definition_update",definition_change_args(&c,&task.input.definition,json!({"native.context_bytes":c.limits().execution.native.context_bytes+1})));
    for version in [8,9,10,11,12,13,14,15,16,17,18,24] {
        let old=c.handle(Request{protocol_revision:version,request_id:format!("old-wire-request-{version}"),command:req.operation()}).await;
        assert_eq!(code(old.outcome),"unsupported_version");assert!(old.commit.is_none());
    }
    assert_eq!(code(lookup(&c,&req).await.outcome),"not_found");
    assert_eq!(selected_definition(&c,&task.assigned_agent_id).await,task.input.definition);
    let accepted=invoke(&c,&req).await;definition_change(&accepted);
}


// Exact coupled context update: no generic whole-definition mutation, no
// accepted-task rewrite and no model/executor/capability change.
// This is a controlled controller qualification, not a full-window model load.
#[tokio::test]
async fn maximum_context_update_is_atomic_and_keeps_unspecified_history() {
    let c=Controller::open_with_config(path(),definition_update_config()).unwrap();
    // Establish a real current caller before another pending task enters the queue.
    let (updater,updater_attempt,actor)=definition_update_fixture(&c,"context-updater").await;
    assert_eq!(updater_attempt.task_id,updater.id);
    assert_ne!(actor.task_id,updater.id);
    assert_eq!(get(&c,&actor.task_id).await.assigned_agent_id,"charter");
    let mut old_input=profiled("context-maximum");
    let mut native=c.limits().execution.native.clone();
    native.context_bytes=65_536;native.request_bytes=1_048_576;
    native.helper_value_bytes=1_048_832;native.response_wire_bytes=6_358_528;
    let mut coding=c.limits().execution.coding.clone();coding.model_context_window=None;
    old_input.definition.limit_overrides=Some(Box::new(borg_runtime::limits::ExecutionLimitOverrides{
        native:Some(native),coding:Some(coding),..Default::default()
    }));
    let old=submit(&c,"context-old-pending",old_input).await;
    let accepted=serde_json::to_vec(&old.input).unwrap();
    assert!(old.attempt.is_none());
    let before=old.input.definition.clone();
    let invalid=invocation(&actor,"context-incomplete","collab_definition_update",definition_change_args(&c,&before,json!({"native.context_bytes":u32::MAX})));
    let refusal=invoke(&c,&invalid).await;
    assert!(matches!(refusal.outcome,Outcome::Refused{ref code,..} if code=="invalid_limits"), "{refusal:?}");
    assert_eq!(selected_definition(&c,&before.id).await,before);
    let full=invocation(&actor,"context-complete","collab_definition_update",definition_change_args(&c,&before,json!({
        "native.context_bytes":u32::MAX,"native.request_bytes":u32::MAX,
        "native.helper_value_bytes":u32::MAX,"native.response_wire_bytes":u32::MAX,
        "coding.model_context_window":1_050_000
    })));
    let first=invoke(&c,&full).await;let outcome=definition_change(&first);
    assert!(first.commit.is_some());
    assert_eq!(outcome.classification,ConfigurationChangeClass::PreviousUnspecified);
    assert_eq!(outcome.changed_fields.len(),5);
    assert_eq!(outcome.selected_definition.executor,before.executor);
    assert_eq!(outcome.selected_definition.inference,before.inference);
    assert_eq!(outcome.selected_definition.execution_profile,before.execution_profile);
    let next=c.limits().resolve_definition(&outcome.selected_definition).unwrap();
    assert_eq!([next.native.context_bytes,next.native.request_bytes,next.native.helper_value_bytes,next.native.response_wire_bytes],[u32::MAX as usize;4]);
    assert_eq!(next.coding.model_context_window,Some(1_050_000));
    let repeat=invoke(&c,&full).await;assert_eq!(first,repeat);
    assert_eq!(serde_json::to_vec(&get(&c,&old.id).await.input).unwrap(),accepted);
    assert_eq!(before.limit_overrides.as_ref().unwrap().coding.as_ref().unwrap().model_context_window,None);
    let mut future=old.input.clone();future.definition=outcome.selected_definition.clone();future.resolved_limits=None;
    let future=submit(&c,"context-future-pinned",future).await;
    assert_eq!(future.input.execution_limits().unwrap().native.context_bytes,u32::MAX as usize);
    assert_eq!(future.input.execution_limits().unwrap().coding.model_context_window,Some(1_050_000));

    let stale=invocation(&actor,"context-stale","collab_definition_update",definition_change_args(&c,&before,json!({"coding.model_context_window":1_050_000})));
    assert!(matches!(invoke(&c,&stale).await.outcome,Outcome::Refused{ref code,..} if code=="configuration_conflict"));
    assert_eq!(selected_definition(&c,&before.id).await,outcome.selected_definition);
}

#[tokio::test]
async fn definition_updates_ordinary_caller_cannot_acquire_selected_manager_authority() {
    let c = Controller::open_with_config(path(), definition_update_config()).unwrap();
    let (old, _, ordinary) = fixture(&c, "ordinary-update-denied").await;
    let request = invocation(&ordinary, "ordinary-update-denied-operation", "collab_definition_update",
        definition_change_args(&c, &old.input.definition, json!({"native.context_bytes":c.limits().execution.native.context_bytes + 1})));
    assert_eq!(code(invoke(&c, &request).await.outcome), "identity_manager_required");
    assert_eq!(selected_definition(&c, &old.assigned_agent_id).await, old.input.definition);
    assert_eq!(get(&c, &old.id).await.input, old.input);
}
