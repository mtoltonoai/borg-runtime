//! Actual controller/SQLite receipts with controlled provider observations. These
//! tests do not contact Git or GitHub, or assert that the provider performed work.
use super::{Controller, ControllerConfig};
use crate::{protocol::*, pull_request as pr, repository_catalogue::Registration};
use serde_json::{Value, json};
use std::{path::PathBuf, sync::{Arc, atomic::{AtomicU64, Ordering}}};

static NEXT: AtomicU64 = AtomicU64::new(1);
fn serial() -> u64 { NEXT.fetch_add(1, Ordering::Relaxed) }
fn config() -> ControllerConfig {
    let mut limits = crate::limits::RuntimeLimits::seed().unwrap();
    limits.controller.command_wait_ms = 20;
    ControllerConfig { limits: Arc::new(limits), clock: Arc::new(|| 100),
        ..ControllerConfig::default() }
}
fn path() -> PathBuf {
    loop {
        let p = std::env::temp_dir().join(format!("borg-pr-controller-{}-{}", std::process::id(), serial()));
        match std::fs::create_dir(&p) {
            Ok(()) => return p.join("state.sqlite"),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => panic!("fixture directory: {e}"),
        }
    }
}
fn target() -> pr::Target {
    pr::Target { revision: "target-r1".into(), api_base: "https://api.github.com".into(),
        owner: "fixture-owner".into(), repository: "fixture-repo".into(), repository_id: 17,
        head_remote: "https://github.com/fixture-owner/fixture-repo.git".into(),
        base_branch: "main".into(), credential_name: "GITHUB_TOKEN".into() }
}
fn registration(configured: bool) -> Registration {
    Registration { repository_id: "pr-source".into(), registration_revision: "r1".into(),
        display_name: "Controlled publication source".into(),
        source_endpoint: "/fixture/source.git".into(), checkpoint_endpoint: "/fixture/checkpoint.git".into(),
        source_store_key: "source.git".into(), checkpoint_store_key: "checkpoint.git".into(),
        pull_request_target: configured.then(target) }
}
fn input(requested: bool) -> TaskInput {
    TaskInput { instruction_source: None, pull_request: requested.then(|| pr::Request { title: "Controlled change".into(), body: "Original request".into() }),
        resolved_limits: None, repository_read: None, instruction: "controlled coding fixture".into(),
        definition: Definition { id: "pr-agent".into(), revision: "definition-1".into(),
            executor: ExecutorKind::Coding, inference: Some(InferenceSettings::selected()),
            execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
            charter: None, limit_overrides: None },
        repository: Some(Repository { registration: Some(registration(true).binding()),
            url: String::new(), checkpoint_remote: String::new(), start_commit: "a".repeat(40),
            pull_request_target: None }), acceptance_commands: vec![vec!["controlled-check".into()]],
        output_schema: None, limits: Limits { deadline_unix_ms: 10_000, model_requests: 2,
            output_bytes: 4096, result_bytes: config().limits.task_defaults.result_bytes,
            max_delegation_depth: 1 }, dependencies: vec![], native_workflow: None }
}
async fn call(c: &Controller, command: Operation) -> Reply {
    Box::pin(c.handle(Request { protocol_revision: WIRE_REVISION, request_id: format!("request-{}", serial()), command })).await
}
fn data(reply: Reply) -> Data {
    match reply.outcome { Outcome::Ok { data } => data, other => panic!("expected committed response: {other:?}") }
}
fn task(reply: Reply) -> Task {
    match data(reply) { Data::Task(t) => *t, other => panic!("expected task: {other:?}") }
}
fn record(reply: Reply) -> OperationRecord {
    match data(reply) { Data::Operation(r) => r, other => panic!("expected original operation: {other:?}") }
}
fn refused(reply: Reply, expected: &str) {
    match reply.outcome { Outcome::Refused { code, .. } => assert_eq!(code, expected),
        other => panic!("expected refusal {expected}: {other:?}") }
}
async fn get(c: &Controller, id: &str) -> Task { task(call(c, Operation::GetTask { task_id: id.into() }).await) }
async fn report(c: &Controller, a: &Attempt, observation: Observation) -> Reply {
    call(c, Operation::ObserveWorker { report: Report { report_id: format!("report-{}", serial()),
        worker_id: a.worker_id.clone(), incarnation: a.worker_incarnation.clone(), task_id: a.task_id.clone(),
        attempt_id: a.id.clone(), generation: a.generation, observation } }).await
}
fn prepare(r: &OperationRecord) -> Operation {
    Operation::PrepareOperation { operation_id: r.id.clone(), task_id: r.task_id.clone(),
        attempt_id: r.attempt_id.clone(), generation: r.generation, kind: r.kind.clone(), intended: r.intended.clone() }
}
fn settle(r: &OperationRecord, outcome: Value) -> Operation {
    Operation::SettleOperation { settlement_id: format!("settlement-{}", serial()), operation_id: r.id.clone(),
        task_id: r.task_id.clone(), attempt_id: r.attempt_id.clone(), generation: r.generation,
        outcome, checkpoint: None }
}
fn head_proof(h: &pr::HeadIntent) -> pr::HeadProof {
    pr::HeadProof { operation_id: h.head_operation_id.clone(), target_revision: h.target.revision.clone(),
        remote: h.target.head_remote.clone(), remote_ref: h.head_ref.clone(), commit: h.binding.checkpoint.commit.clone() }
}
fn proof(i: &pr::Intent) -> pr::Proof {
    let t = &i.head.target;
    pr::Proof { operation_id: i.head.binding.operation_id.clone(), provider_id: 111, number: 23,
        url: format!("https://github.com/{}/{}/pull/23", t.owner, t.repository),
        head: pr::BranchProof { repository_id: t.repository_id, owner: t.owner.clone(), repository: t.repository.clone(),
            branch: i.head.head_ref.strip_prefix("refs/heads/").unwrap().into(), commit: i.head.binding.checkpoint.commit.clone() },
        // The base can advance independently; this valid observed SHA is evidence.
        base: pr::BranchProof { repository_id: t.repository_id, owner: t.owner.clone(), repository: t.repository.clone(),
            branch: t.base_branch.clone(), commit: "b".repeat(40) },
        title: i.title.clone(), body: i.body.clone(), marker: i.marker.clone() }
}
struct Fixture { c: Controller, path: PathBuf, t: Task, a: Attempt, cp: Checkpoint }
impl Fixture {
    async fn new(requested: bool) -> Self {
        let path = path(); let c = Controller::open_with_config(&path, config()).unwrap();
        data(call(&c, Operation::RegisterRepository { operation_id: "register-repository".into(), registration: registration(true) }).await);
        let t = task(call(&c, Operation::SubmitTask { operation_id: "submit".into(), input: input(requested) }).await);
        data(call(&c, Operation::RegisterWorker { operation_id: "register-worker".into(), worker: Worker {
            id: "worker".into(), incarnation: "incarnation".into(), coding_capacity: 1, native_capacity: 0 } }).await);
        let Data::Commands(commands) = data(call(&c, Operation::AwaitCommands { operation_id: "await".into(),
            worker_id: "worker".into(), incarnation: "incarnation".into(), acknowledged: vec![] }).await) else { panic!("commands") };
        let a = commands.into_iter().find_map(|c| match c { WorkerCommand::RunAttempt { attempt, .. } => Some(attempt), _ => None }).unwrap();
        task(report(&c, &a, Observation::Started { thread_id: Some("thread".into()), turn_id: Some("turn".into()) }).await);
        let cp = crate::worker::git::checkpoint(t.input.repository.as_ref().unwrap(), &t.id, &a.id, "c".repeat(40)).unwrap();
        record(call(&c, Operation::PrepareOperation { operation_id: cp.operation_id.clone(), task_id: t.id.clone(),
            attempt_id: a.id.clone(), generation: a.generation, kind: "checkpoint".into(),
            intended: json!({"registration":cp.registration,"repository":cp.repository,"commit":cp.commit,"remote_ref":cp.remote_ref}) }).await);
        record(call(&c, Operation::SettleOperation { settlement_id: "checkpoint-settlement".into(),
            operation_id: cp.operation_id.clone(), task_id: t.id.clone(), attempt_id: a.id.clone(), generation: a.generation,
            outcome: json!({"acknowledged":true}), checkpoint: Some(cp.clone()) }).await);
        let t = get(&c, &t.id).await;
        Self { c, path, t, a, cp }
    }
    fn bounds(&self) -> pr::Bounds { pr::Bounds::from_limits(&self.t.input.resolved_limits.as_ref().unwrap().global_metadata) }
    fn head(&self, id: &str) -> pr::HeadIntent {
        pr::prepare_head(self.t.input.repository.as_ref().unwrap().pull_request_target.as_ref().unwrap(),
            self.t.input.repository.as_ref().unwrap(), &self.a, &self.cp, id, &self.bounds()).unwrap()
    }
    async fn executor_ok(&self) {
        task(report(&self.c, &self.a, Observation::ExecutorOutcome { success: true, summary: "controlled executor success".into() }).await);
    }
    async fn candidate(&self, success: bool) -> Reply {
        report(&self.c, &self.a, Observation::ResultCandidate { checks_passed: success,
            summary: "controlled result".into(), checkpoint: Some(self.cp.clone()), validation: vec!["controlled check".into()],
            output: Some(json!({"pull_request":"untrusted output is not the typed reference"})) }).await
    }
}

#[tokio::test]
async fn publication_request_requires_coding_registered_target_and_rejects_endpoint_override() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    data(call(&c, Operation::RegisterRepository { operation_id: "register".into(), registration: registration(false) }).await);
    refused(call(&c, Operation::SubmitTask { operation_id: "unconfigured".into(), input: input(true) }).await, "pull_request_invalid");
    let mut forged = input(true);
    forged.repository.as_mut().unwrap().pull_request_target = Some(target());
    refused(call(&c, Operation::SubmitTask { operation_id: "forged-target".into(), input: forged }).await, "invalid_input");
    let mut native = input(true); native.definition.executor = ExecutorKind::Native;
    native.repository = None; native.acceptance_commands.clear(); native.output_schema = Some(json!({"type":"object"}));
    refused(call(&c, Operation::SubmitTask { operation_id: "noncoding".into(), input: native }).await, "invalid_input");
    let Data::Tasks(tasks) = data(call(&c, Operation::ListTasks { after: None, limit: 10 }).await) else { panic!("tasks") };
    assert!(tasks.is_empty(), "refused requests cannot create tasks or external effects");
}

#[tokio::test]
async fn requested_success_joins_exact_checkpoint_head_create_and_original_replay_after_restart() {
    let f = Fixture::new(true).await; let b = f.bounds(); f.executor_ok().await;
    let before = get(&f.c, &f.t.id).await;
    refused(f.candidate(true).await, "pull_request_unsettled");
    assert_eq!(get(&f.c, &f.t.id).await, before, "zero pending effects is not requested-result proof");
    let h = f.head("pr-create"); let hr = pr::head_record(&h, &b).unwrap();
    record(call(&f.c, prepare(&hr)).await);
    let mut forged_settled_head = hr.clone(); forged_settled_head.state = "settled".into();
    forged_settled_head.outcome = Some(json!(pr::HeadSettlement { acknowledged: true, proof: head_proof(&h) }));
    let i = pr::prepare_pull_request(&h, &forged_settled_head, f.t.input.pull_request.as_ref().unwrap(), &b).unwrap();
    let cr = pr::intent_record(&i, &b).unwrap();
    refused(call(&f.c, settle(&hr, json!({"acknowledged":true}))).await, "pull_request_invalid");
    let mut wrong_head = head_proof(&h); wrong_head.commit = "d".repeat(40);
    refused(call(&f.c, settle(&hr, json!(pr::HeadSettlement { acknowledged: true, proof: wrong_head }))).await, "pull_request_invalid");
    let actual_head = record(call(&f.c, settle(&hr, json!(pr::HeadSettlement { acknowledged: true, proof: head_proof(&h) }))).await);
    assert_eq!(actual_head, forged_settled_head);
    let second = pr::head_record(&f.head("another-pr-create"), &b).unwrap();
    refused(call(&f.c, prepare(&second)).await, "operation_conflict");
    // A catalogue revision cannot retarget this accepted command.
    let mut newer = registration(true); newer.registration_revision = "r2".into();
    newer.pull_request_target.as_mut().unwrap().base_branch = "other".into();
    data(call(&f.c, Operation::ReviseRepository { operation_id: "revise".into(), expected_generation: 1, registration: newer }).await);
    assert_eq!(get(&f.c, &f.t.id).await.input.repository, f.t.input.repository);
    let prepared_create = call(&f.c, prepare(&cr)).await; record(prepared_create.clone());
    refused(call(&f.c, settle(&cr, json!({"acknowledged":true}))).await, "pull_request_invalid");
    let p = proof(&i);
    for bad in [
        { let mut p = p.clone(); p.head.commit = "d".repeat(40); p },
        { let mut p = p.clone(); p.base.branch = "other".into(); p },
        { let mut p = p.clone(); p.marker.push('x'); p },
        { let mut p = p.clone(); p.base.repository_id += 1; p },
    ] {
        refused(call(&f.c, settle(&cr, json!(pr::Settlement { acknowledged: true, proof: bad }))).await, "pull_request_invalid");
        assert_eq!(record(call(&f.c, Operation::GetOperation { operation_id: cr.id.clone() }).await), cr);
    }
    let settlement = settle(&cr, json!(pr::Settlement { acknowledged: true, proof: p.clone() }));
    let settled = call(&f.c, settlement.clone()).await; record(settled.clone());
    task(report(&f.c, &f.a, Observation::ProcessTreeStopped).await);
    let accepted = task(f.candidate(true).await);
    let reference = pr::validate_proof(&i, &p, &b).unwrap();
    assert_eq!(accepted.result.as_ref().unwrap().pull_request, Some(reference));
    assert_eq!(accepted.result.as_ref().unwrap().checkpoint.as_ref(), Some(&f.cp));
    assert_eq!(accepted.status, TaskStatus::Succeeded);
    let path = f.path.clone(); let id = f.t.id.clone(); drop(f);
    let c = Controller::open_with_config(path, config()).unwrap();
    let replay = call(&c, settlement).await;
    assert_eq!(replay.outcome, settled.outcome); assert_eq!(replay.commit, settled.commit);
    assert_eq!(get(&c, &id).await, accepted, "original replay must not mutate accepted result");
    let prepared_replay = call(&c, prepare(&cr)).await;
    assert_eq!(prepared_replay.outcome, prepared_create.outcome);
    assert_eq!(prepared_replay.commit, prepared_create.commit);
}

#[tokio::test]
async fn not_applied_closes_uncertainty_but_cannot_satisfy_requested_success() {
    let f = Fixture::new(true).await; let b = f.bounds(); f.executor_ok().await;
    let h = f.head("create-never-entered"); let hr = pr::head_record(&h, &b).unwrap();
    record(call(&f.c, prepare(&hr)).await);
    let head = record(call(&f.c, settle(&hr, json!(pr::HeadSettlement { acknowledged: true, proof: head_proof(&h) }))).await);
    let i = pr::prepare_pull_request(&h, &head, f.t.input.pull_request.as_ref().unwrap(), &b).unwrap();
    let cr = pr::intent_record(&i, &b).unwrap(); record(call(&f.c, prepare(&cr)).await);
    record(call(&f.c, settle(&cr, json!({"acknowledged":false,"unknown":true}))).await);
    refused(f.candidate(true).await, "reconciliation_required");
    let ended = record(call(&f.c, settle(&cr, json!({"acknowledged":true,"not_applied":true}))).await);
    assert_eq!(ended.state, "settled");
    refused(f.candidate(true).await, "pull_request_invalid");
    task(report(&f.c, &f.a, Observation::ProcessTreeStopped).await);
    let failed = task(f.candidate(false).await);
    assert_eq!(failed.status, TaskStatus::Failed); assert!(failed.result.is_none());
    let retried = task(call(&f.c, Operation::RetryTask { operation_id: "retry".into(), task_id: failed.id.clone(), expected_revision: failed.revision }).await);
    let before = get(&f.c, &retried.id).await;
    // Exact historical replay returns the original fact without touching the
    // retried task; changed evidence under that old identity still refuses.
    let replayed = record(call(&f.c, settle(&hr, json!(pr::HeadSettlement { acknowledged: true, proof: head_proof(&h) }))).await);
    assert_eq!(replayed, head);
    let mut changed = head_proof(&h); changed.commit = "d".repeat(40);
    refused(call(&f.c, settle(&hr, json!(pr::HeadSettlement { acknowledged: true, proof: changed }))).await, "operation_conflict");
    assert_eq!(get(&f.c, &retried.id).await, before);
}

#[tokio::test]
async fn caller_supplied_head_proof_cannot_replace_canonical_pending_head() {
    let f = Fixture::new(true).await; let b = f.bounds();
    let h = f.head("create-before-head-settled"); let hr = pr::head_record(&h, &b).unwrap();
    record(call(&f.c, prepare(&hr)).await);
    let mut supplied = hr.clone(); supplied.state = "settled".into();
    supplied.outcome = Some(json!(pr::HeadSettlement { acknowledged: true, proof: head_proof(&h) }));
    let i = pr::prepare_pull_request(&h, &supplied, f.t.input.pull_request.as_ref().unwrap(), &b).unwrap();
    let cr = pr::intent_record(&i, &b).unwrap();
    let before = get(&f.c, &f.t.id).await;
    refused(call(&f.c, prepare(&cr)).await, "pull_request_invalid");
    assert_eq!(record(call(&f.c, Operation::GetOperation { operation_id: hr.id.clone() }).await), hr);
    assert_eq!(get(&f.c, &f.t.id).await, before);
    // A different attempt/generation cannot settle the original effect.
    let mut wrong_owner = settle(&hr, json!(pr::HeadSettlement { acknowledged: true, proof: head_proof(&h) }));
    if let Operation::SettleOperation { generation, .. } = &mut wrong_owner { *generation += 1; }
    refused(call(&f.c, wrong_owner).await, "operation_conflict");
    assert_eq!(record(call(&f.c, Operation::GetOperation { operation_id: hr.id.clone() }).await), hr);
}

// Direct query-only use of the same new startup validator. This deliberately
// does not claim physical controller/helper recovery or a historical producer.
fn retained(path: &std::path::Path, version: i64) -> super::database::Result<()> {
    let sql = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    sql.pragma_update(None, "query_only", true).unwrap();
    let cfg = config();
    super::pull_request::validate_existing(&super::database::StoreContext::new(&sql, &cfg.limits), &cfg, version)
}
fn assert_retained_refusal_unchanged(path: &std::path::Path, version: i64) {
    let before = std::fs::read(path).unwrap();
    assert!(retained(path, version).is_err());
    assert_eq!(std::fs::read(path).unwrap(), before, "query-only refusal preserves the original database bytes");
}

#[tokio::test]
async fn startup_reverse_prepare_membership_refuses_deleted_pending_head_without_repair() {
    let f = Fixture::new(true).await;
    let head = pr::head_record(&f.head("retained-create"), &f.bounds()).unwrap();
    record(call(&f.c, prepare(&head)).await);
    let path = f.path.clone(); drop(f);
    retained(&path, 20).unwrap();
    let sql = rusqlite::Connection::open(&path).unwrap();
    assert_eq!(sql.execute("DELETE FROM external_operations WHERE id=?1", [&head.id]).unwrap(), 1);
    drop(sql);
    assert_retained_refusal_unchanged(&path, 20);
}

#[tokio::test]
async fn startup_original_selection_refuses_erased_request_and_old_schema_typed_fields() {
    let f = Fixture::new(true).await;
    let path = f.path.clone(); let id = f.t.id.clone(); drop(f);
    retained(&path, 20).unwrap();
    assert_retained_refusal_unchanged(&path, 19);
    let sql = rusqlite::Connection::open(&path).unwrap();
    let raw: String = sql.query_row("SELECT body FROM tasks WHERE id=?1", [&id], |r| r.get(0)).unwrap();
    let mut stored: super::database::StoredTask = super::database::decode(&raw).unwrap();
    stored.task.input.pull_request = None;
    let changed = super::database::encode(&stored).unwrap();
    sql.execute("UPDATE tasks SET body=?2 WHERE id=?1", rusqlite::params![id, changed]).unwrap();
    drop(sql);
    assert_retained_refusal_unchanged(&path, 20);
}

#[tokio::test]
async fn startup_canonical_result_rejects_unknown_nested_reference_and_missing_result_receipt() {
    let f = Fixture::new(true).await; f.executor_ok().await;
    let head = f.head("result-create"); let b = f.bounds();
    let hr = pr::head_record(&head, &b).unwrap(); record(call(&f.c, prepare(&hr)).await);
    let hr = record(call(&f.c, settle(&hr, json!(pr::HeadSettlement { acknowledged: true, proof: head_proof(&head) }))).await);
    let intent = pr::prepare_pull_request(&head, &hr, f.t.input.pull_request.as_ref().unwrap(), &b).unwrap();
    let cr = pr::intent_record(&intent, &b).unwrap(); record(call(&f.c, prepare(&cr)).await);
    record(call(&f.c, settle(&cr, json!(pr::Settlement { acknowledged: true, proof: proof(&intent) }))).await);
    task(report(&f.c, &f.a, Observation::ProcessTreeStopped).await);
    let final_reply = f.candidate(true).await;
    let final_id = final_reply.commit.as_ref().unwrap().operation_id.clone(); task(final_reply);
    let path = f.path.clone(); let id = f.t.id.clone(); drop(f);
    retained(&path, 20).unwrap();
    let sql = rusqlite::Connection::open(&path).unwrap();
    let original: String = sql.query_row("SELECT body FROM tasks WHERE id=?1", [&id], |r| r.get(0)).unwrap();
    let changed = original.replacen("\"provider_id\":111", "\"provider_id\":111,\"unsupported\":true", 1);
    assert_ne!(changed, original, "fixture must alter the retained typed reference");
    sql.execute("UPDATE tasks SET body=?2 WHERE id=?1", rusqlite::params![id, changed]).unwrap(); drop(sql);
    assert_retained_refusal_unchanged(&path, 20);
    let sql = rusqlite::Connection::open(&path).unwrap();
    sql.execute("UPDATE tasks SET body=?2 WHERE id=?1", rusqlite::params![id, original]).unwrap();
    assert_eq!(sql.execute("DELETE FROM receipts WHERE id=?1", [&final_id]).unwrap(), 1); drop(sql);
    assert_retained_refusal_unchanged(&path, 20);
}

#[tokio::test]
async fn historical_generic_settlement_requires_original_successful_prepare_and_absent_request() {
    let f = Fixture::new(false).await;
    // A controlled historical-shape fixture, not a claim of an old producer run.
    let sql = rusqlite::Connection::open_in_memory().unwrap();
    sql.execute_batch("CREATE TABLE external_operations(id TEXT PRIMARY KEY,body TEXT,prepare TEXT,settlement TEXT); CREATE TABLE receipts(id TEXT PRIMARY KEY,request TEXT,outcome TEXT,controller_revision INTEGER); CREATE TABLE counters(kind TEXT PRIMARY KEY,value INTEGER); INSERT INTO counters VALUES('controller_revision',1);").unwrap();
    let record = OperationRecord { id: "legacy-pr".into(), task_id: f.t.id.clone(), attempt_id: f.a.id.clone(), generation: f.a.generation,
        kind: "pull_request".into(), intended: json!({"historical_note":"opaque original intent"}), state: "pending".into(), outcome: None };
    let request = prepare(&record);
    let outcome = Outcome::Ok { data: Data::Operation(record.clone()) };
    sql.execute("INSERT INTO external_operations VALUES(?1,?2,?3,NULL)", rusqlite::params![record.id, super::database::encode(&record).unwrap(), super::database::encode(&request).unwrap()]).unwrap();
    sql.execute("INSERT INTO receipts VALUES(?1,?2,?3,1)", rusqlite::params![record.id, super::database::encode(&request).unwrap(), super::database::encode(&outcome).unwrap()]).unwrap();
    let cfg = config(); let tx = super::database::StoreContext::new(&sql, &cfg.limits);
    super::pull_request::settle(&tx, &f.t, &record, &json!({"acknowledged":true})).unwrap();
    assert!(super::pull_request::prepare(&tx, &f.t, &record).is_err(), "new generic Prepare stays forbidden");
    let mut requested = f.t.clone(); requested.input.pull_request = input(true).pull_request;
    assert!(super::pull_request::settle(&tx, &requested, &record, &json!({"acknowledged":true})).is_err());
    sql.execute("DELETE FROM receipts WHERE id=?1", [&record.id]).unwrap();
    assert!(super::pull_request::settle(&tx, &f.t, &record, &json!({"acknowledged":true})).is_err(), "missing original provenance never opens legacy settlement");
}

fn collaboration_call(a:&Attempt,id:&str,tool:&str,arguments:Value)->Operation {
    crate::mcp::Invocation { operation_id:id.into(),logical_call_id:format!("logical-{id}"),
        caller:crate::mcp::Caller{task_id:a.task_id.clone(),attempt_id:a.id.clone(),generation:a.generation,worker_id:a.worker_id.clone(),incarnation:a.worker_incarnation.clone()},
        tool:tool.into(),arguments }.operation()
}

#[tokio::test]
async fn startup_accepts_original_public_task_receipt_without_reinterpreting_redacted_target() {
    let f=Fixture::new(true).await;
    let query=collaboration_call(&f.a,"public-original-task","collab_task_get",json!({"task_id":f.t.id}));
    let original=call(&f.c,query.clone()).await;let projected=task(original.clone());
    assert!(projected.input.repository.as_ref().unwrap().pull_request_target.is_none());
    assert_eq!(projected.input.pull_request,f.t.input.pull_request);
    assert!(get(&f.c,&f.t.id).await.input.repository.unwrap().pull_request_target.is_some());
    let path=f.path.clone();drop(f);
    retained(&path,20).unwrap();
    let c=Controller::open_with_config(&path,config()).unwrap();
    let replay=call(&c,query).await;assert_eq!(replay.outcome,original.outcome);assert_eq!(replay.commit,original.commit);
}

#[tokio::test]
async fn startup_accepts_actual_projected_configured_coding_child_admission_and_wait() {
    let mut cfg=config();
    cfg.validation_executable=PathBuf::from(std::env::var_os("BORG_TEST_RUNTIME").expect("explicit same-build native schema helper for configured parent admission"));
    assert!(cfg.validation_executable.is_absolute() && cfg.validation_executable.is_file());
    let path=path();let c=Controller::open_with_config(&path,cfg.clone()).unwrap();
    data(call(&c,Operation::RegisterRepository{operation_id:"configured-register".into(),registration:registration(true)}).await);
    let child=input(true);
    let mut parent=input(false);parent.repository=None;parent.definition.id="configured-parent".into();parent.definition.executor=ExecutorKind::Native;
    parent.acceptance_commands.clear();parent.output_schema=Some(json!({"type":"object"}));parent.limits.model_requests=4;
    parent.native_workflow=Some(Box::new(NativeWorkflow::CodingThenContinue{child:CodingTaskTemplate { instruction_source: None,
        pull_request:child.pull_request,instruction_prefix:"Configured prefix".into(),definition:child.definition,repository:child.repository.unwrap(),
        acceptance_commands:child.acceptance_commands,dependencies:child.dependencies,limits:child.limits }}));
    let parent=task(call(&c,Operation::SubmitTask{operation_id:"configured-submit".into(),input:parent}).await);
    data(call(&c,Operation::RegisterWorker{operation_id:"configured-worker".into(),worker:Worker{id:"configured-worker".into(),incarnation:"one".into(),coding_capacity:0,native_capacity:1}}).await);
    let Data::Commands(commands)=data(call(&c,Operation::AwaitCommands{operation_id:"configured-launch".into(),worker_id:"configured-worker".into(),incarnation:"one".into(),acknowledged:vec![]}).await) else {panic!("commands")};
    let a=commands.into_iter().find_map(|c|match c{WorkerCommand::RunAttempt{attempt,..}=>Some(attempt),_=>None}).unwrap();
    task(report(&c,&a,Observation::Started{thread_id:None,turn_id:None}).await);
    let request=collaboration_call(&a,"configured-delegation","collab_task_delegate",json!({"instruction":"Implement the configured change"}));
    let original=call(&c,request.clone()).await;let public_child=task(original.clone());
    assert_eq!(public_child.parent_task_id,Some(parent.id.clone()));
    assert!(public_child.input.repository.as_ref().unwrap().pull_request_target.is_none());
    let canonical=get(&c,&public_child.id).await;
    assert_eq!(canonical.input.repository.as_ref().unwrap().pull_request_target,Some(target()));assert!(canonical.input.pull_request.is_some());
    assert_eq!(public_child.input,crate::pull_request_view::input(&canonical.input));
    assert!(original.commit.is_some());
    let lookup=Operation::GetOperation{operation_id:"configured-delegation".into()};
    let saved=record(call(&c,lookup.clone()).await);
    assert_eq!(saved.outcome,Some(serde_json::to_value(&original.outcome).unwrap()),"the original durable receipt must contain the public projection");
    let replay=call(&c,request.clone()).await;assert_eq!(replay.outcome,original.outcome);assert_eq!(replay.commit,original.commit);
    assert_eq!(get(&c,&parent.id).await.waiting_child_id,Some(canonical.id.clone()));
    drop(c);retained(&path,20).unwrap();
    let c=Controller::open_with_config(&path,cfg).unwrap();
    let replay=call(&c,request).await;assert_eq!(replay.outcome,original.outcome);assert_eq!(replay.commit,original.commit);
    assert!(task(replay).input.repository.unwrap().pull_request_target.is_none());
    assert_eq!(record(call(&c,lookup).await),saved);
    assert_eq!(get(&c,&canonical.id).await,canonical);
}

#[tokio::test]
async fn startup_requires_latest_original_uncertain_observation_not_an_earlier_receipt_or_erasure() {
    let f=Fixture::new(true).await;let hr=pr::head_record(&f.head("uncertain-create"),&f.bounds()).unwrap();
    record(call(&f.c,prepare(&hr)).await);
    let earlier=record(call(&f.c,settle(&hr,json!({"acknowledged":false,"observation":"first unknown"}))).await);
    let latest=record(call(&f.c,settle(&hr,json!({"acknowledged":false,"observation":"later unknown"}))).await);
    let path=f.path.clone();drop(f);retained(&path,20).unwrap();
    for outcome in [earlier.outcome,None] {
        let mut corrupted=latest.clone();corrupted.outcome=outcome;
        let sql=rusqlite::Connection::open(&path).unwrap();
        sql.execute("UPDATE external_operations SET body=?2 WHERE id=?1",rusqlite::params![latest.id,super::database::encode(&corrupted).unwrap()]).unwrap();drop(sql);
        assert_retained_refusal_unchanged(&path,20);
    }
}

#[tokio::test]
async fn absent_request_keeps_checkpoint_only_result_and_forbids_optional_side_effect() {
    let f = Fixture::new(false).await; let h = f.head("unsolicited");
    let hr = pr::head_record(&h, &f.bounds()).unwrap();
    refused(call(&f.c, prepare(&hr)).await, "pull_request_invalid");
    f.executor_ok().await; task(report(&f.c, &f.a, Observation::ProcessTreeStopped).await);
    let accepted = task(f.candidate(true).await);
    assert_eq!(accepted.status, TaskStatus::Succeeded);
    assert!(accepted.result.as_ref().unwrap().pull_request.is_none());
    assert_eq!(accepted.result.unwrap().checkpoint, Some(f.cp));
}
