//! Actual Controller receipts and cleanup accounting. Reports are controlled
//! trusted-host facts; no provider request or worker process is executed here.
use borg_runtime::{
    controller::{Controller, ControllerConfig}, limits::RuntimeLimits, mcp::Caller,
    native::{CategoryDiagnostic, DiagnosticBodyState, HttpDiagnostic, HttpStatusDiagnostic,
        NumericDiagnostic, ProviderErrorCategory, ProviderFailureClass, ProviderFailureDiagnostic,
        ProviderRejection, ProviderRequestBinding, QuotaDiagnostic, RejectionKind, RetryAfter,
        RetryHeaderDiagnostic, SchedulerRetryDiagnostic},
    native_loop_contract::{self as contract, NativeRequestObservedOutcome as Observed}, protocol::*,
};
use rusqlite::{Connection, OpenFlags};
use serde_json::json;
use std::{path::{Path, PathBuf}, sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}},
    time::{Duration, SystemTime, UNIX_EPOCH}};

static NEXT: AtomicU64 = AtomicU64::new(1);
const ENDPOINT: &str = "https://example.invalid/v1/responses";
fn unique(label: &str) -> String { format!("{label}-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)) }
struct Lab { controller: Option<Controller>, root: PathBuf, config: ControllerConfig,
    clock: Arc<AtomicU64>, fail_commit: Arc<AtomicBool> }
impl Lab {
    fn new(mut limits: RuntimeLimits) -> Self {
        limits.controller.command_wait_ms = 5;
        limits.validate().unwrap();
        let base = PathBuf::from(std::env::var_os("BORG_TEST_ARTIFACT").expect("owned fixture directory"));
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let root = base.join(format!("{}-{}", unique("native-diagnostic"), stamp.as_nanos()));
        std::fs::create_dir(&root).unwrap();
        let clock = Arc::new(AtomicU64::new(stamp.as_millis().try_into().unwrap()));
        let observed = clock.clone();
        let fail_commit = Arc::new(AtomicBool::new(false));
        let config = ControllerConfig { limits: Arc::new(limits),
            validation_executable: PathBuf::from(std::env::var_os("BORG_TEST_RUNTIME").expect("same helper ABI runtime")),
            clock: Arc::new(move || observed.load(Ordering::SeqCst)),
            qualification_fail_next_commit: Some(fail_commit.clone()), ..ControllerConfig::default() };
        let controller = Some(Controller::open_with_config(root.join("tasks.db"), config.clone()).unwrap());
        Self { controller, root, config, clock, fail_commit }
    }
    fn controller(&self) -> &Controller { self.controller.as_ref().unwrap() }
    async fn call(&self, command: Operation) -> Reply {
        let reply = self.controller().handle(Request { protocol_revision: WIRE_REVISION,
            request_id: unique("transport"), command: command.clone() }).await;
        reply.validate_for(&command).unwrap(); reply
    }
    async fn close(&mut self) { self.controller.take(); released(&self.root.join("tasks.db")).await; }
    fn open(&mut self) { self.controller = Some(Controller::open_with_config(self.root.join("tasks.db"), self.config.clone()).unwrap()); }
    fn database(&self) -> Connection {
        assert!(self.controller.is_none(), "read closed source only");
        Connection::open_with_flags(self.root.join("tasks.db"), OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap()
    }
    async fn actor(&self) -> Caller {
        self.actor_with_profile(Some(ExecutionProfile::CollaborationV1 { web_search: None })).await
    }
    async fn actor_with_profile(&self, profile: Option<ExecutionProfile>) -> Caller {
        let input = TaskInput { instruction_source: None, pull_request: None,  instruction: "Return one bounded integer".into(),
            definition: Definition { id: unique("agent"), revision: "diagnostic-fixture".into(),
                executor: ExecutorKind::Native, inference: Some(InferenceSettings::selected()),
                execution_profile: profile,
                limit_overrides: None, charter: None }, repository: None, repository_read: None,
            acceptance_commands: vec![], output_schema: Some(json!({"type":"integer"})),
            limits: Limits { deadline_unix_ms: self.clock.load(Ordering::SeqCst) + 600_000,
                model_requests: 4, output_bytes: 16_384, result_bytes: self.config.limits.task_defaults.result_bytes,
                max_delegation_depth: 4 }, dependencies: vec![], native_workflow: None, resolved_limits: None };
        let submitted = task(self.call(Operation::SubmitTask { operation_id: unique("submit"), input: (input).into() }).await);
        let worker = Worker { id: unique("worker"), incarnation: "original".into(), coding_capacity: 0, native_capacity: 1 };
        assert!(matches!(self.call(Operation::RegisterWorker { operation_id: unique("register"), worker: worker.clone() }).await.outcome,
            Outcome::Ok { data: Data::Worker(_) }));
        let reply = self.call(Operation::AwaitCommands { operation_id: unique("await"), worker_id: worker.id,
            incarnation: worker.incarnation, acknowledged: vec![] }).await;
        let Outcome::Ok { data: Data::Commands(commands) } = reply.outcome else { panic!("{reply:?}") };
        let attempt = commands.into_iter().find_map(|c| match c { WorkerCommand::RunAttempt { attempt, .. }
            if attempt.task_id == submitted.id => Some(attempt), _ => None }).expect("original attempt");
        let caller = Caller { task_id: attempt.task_id, attempt_id: attempt.id, generation: attempt.generation,
            worker_id: attempt.worker_id, incarnation: attempt.worker_incarnation };
        task(self.call(report(&caller, unique("started"), Observation::Started { thread_id: None, turn_id: None })).await);
        caller
    }
}
async fn released(path: &Path) {
    let mut lock = path.as_os_str().to_owned(); lock.push(".controller-lock");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let file = std::fs::OpenOptions::new().read(true).write(true).open(PathBuf::from(&lock)).unwrap();
            if file.try_lock().is_ok() { break; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.expect("original owner released");
}
fn task(reply: Reply) -> Task { match reply.outcome { Outcome::Ok { data: Data::Task(task) } => *task,
    other => panic!("expected task: {other:?}") } }
fn report(c: &Caller, id: String, observation: Observation) -> Operation { Operation::ObserveWorker { report: Report {
    report_id: id, task_id: c.task_id.clone(), attempt_id: c.attempt_id.clone(), generation: c.generation,
    worker_id: c.worker_id.clone(), incarnation: c.incarnation.clone(), observation } } }
fn reserve(c: &Caller, index: u32) -> Operation { report(c, contract::reservation_id(&c.attempt_id, index),
    Observation::Usage { model_requests: index, input_tokens: None, output_tokens: None, native_diagnostic_reserved: true }) }
fn binding() -> ProviderRequestBinding { ProviderRequestBinding { bytes: 2,
    sha256: "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a".into() } }
fn facts() -> ProviderFailureDiagnostic { ProviderFailureDiagnostic::http(ProviderFailureClass::HttpRefused, HttpDiagnostic {
    status: HttpStatusDiagnostic::Value(429), body: DiagnosticBodyState::Complete,
    category: CategoryDiagnostic::Value(ProviderErrorCategory::RateLimited),
    scheduler_retry_after: SchedulerRetryDiagnostic::Value(RetryAfter::Absent),
    retry_after: RetryHeaderDiagnostic::Absent, retry_after_ms: NumericDiagnostic::Absent,
    requests_limit: NumericDiagnostic::Value(100), requests_remaining: NumericDiagnostic::Value(0),
    tokens_limit: NumericDiagnostic::Absent, tokens_remaining: NumericDiagnostic::Absent,
    requests_reset_ms: NumericDiagnostic::Value(1000), tokens_reset_ms: NumericDiagnostic::Absent,
    quota: QuotaDiagnostic::Absent }) }
fn diagnostic(c: &Caller, index: u32, observed_outcome: Observed) -> Operation {
    let failure = match observed_outcome {
        Observed::KnownRejected { .. } => Some(facts()),
        Observed::NotStarted => Some(ProviderFailureDiagnostic::new(ProviderFailureClass::LocalAdmission)),
        _ => None,
    };
    report(c, contract::diagnostic_id(&c.attempt_id, index), Observation::NativeRequestDiagnostic { request_index: index,
        endpoint: ENDPOINT.into(), model: SELECTED_MODEL.into(), request_binding: Some(binding()), observed_outcome, failure })
}
fn rejection(c: &Caller, index: u32) -> Operation { report(c, contract::rejection_id(&c.attempt_id, index),
    Observation::NativeRequestRejected { request_index: index, endpoint: ENDPOINT.into(), model: SELECTED_MODEL.into(),
        request_binding: binding(), rejection: ProviderRejection { kind: RejectionKind::RateLimited, retry_after: RetryAfter::Absent },
        diagnostic: "bounded rate limit".into() }) }
fn completion(c: &Caller) -> Operation { report(c, contract::completion_id(&c.attempt_id), Observation::NativeLoopCompleted {
    model_requests: 1, calls: vec![], requests: vec![], final_response_index: None }) }
fn ack(reply: &Reply) { assert!(matches!(reply.outcome, Outcome::Ok { data: Data::Acknowledged }), "{reply:?}"); assert!(reply.commit.is_some()); }
fn refused(reply: &Reply) { assert!(matches!(reply.outcome, Outcome::Refused { .. }), "{reply:?}"); }
fn ledger(db: &Connection, c: &Caller) -> (i64, i64, i64) {
    let (rows, consumed) = db.query_row("SELECT count(*),coalesce(sum(consumed),0) FROM native_request_diagnostic_credits WHERE attempt_id=?1",
        [&c.attempt_id], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
    let receipts = db.query_row("SELECT count(*) FROM receipts WHERE id=?1 AND cleanup=1", [contract::diagnostic_id(&c.attempt_id, 1)], |r| r.get(0)).unwrap();
    (rows, consumed, receipts)
}

//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A failed platform-owned provider request MUST retain bounded diagnostics sufficient to identify the observed failure class and available provider retry or quota information.
#[tokio::test]
async fn reserved_terminal_works_at_full_ordinary_capacity_and_replays_once_after_restart() {
    let mut limits = RuntimeLimits::seed().unwrap(); limits.controller.max_receipts = 8;
    let mut lab = Lab::new(limits); let c = lab.actor().await;
    task(lab.call(reserve(&c, 1)).await);
    // Repeated distinct normal progress fills actual ordinary receipt slots.
    for _ in 0..8 { let reply = lab.call(report(&c, unique("progress"), Observation::Progress {
        text: String::new(), output: String::new(), truncated: false })).await;
        if matches!(reply.outcome, Outcome::Refused { .. }) { break; }
    }
    let op = diagnostic(&c, 1, Observed::Completed);
    let first = lab.call(op.clone()).await; ack(&first);
    let duplicate = lab.call(op.clone()).await; ack(&duplicate);
    assert_eq!(duplicate.outcome, first.outcome); assert_eq!(duplicate.commit, first.commit);
    lab.close().await;
    { let db = lab.database(); assert_eq!(ledger(&db, &c), (1, 1, 1));
      assert_eq!(db.query_row("SELECT count(*) FROM receipts WHERE cleanup=0", [], |r| r.get::<_,i64>(0)).unwrap(), 8); }
    lab.open(); let replay = lab.call(op).await; ack(&replay); assert_eq!(replay.commit, first.commit);
    lab.close().await; assert_eq!(ledger(&lab.database(), &c), (1, 1, 1));
}

#[tokio::test]
async fn reservation_and_terminal_commit_failures_roll_back_credit_and_usage_together() {
    let mut lab = Lab::new(RuntimeLimits::seed().unwrap()); let c = lab.actor().await;
    let usage = reserve(&c, 1); lab.fail_commit.store(true, Ordering::SeqCst);
    refused(&lab.call(usage.clone()).await);
    lab.close().await;
    { let db = lab.database(); assert_eq!(ledger(&db, &c), (0, 0, 0));
      assert_eq!(db.query_row("SELECT model_requests FROM attempts WHERE id=?1", [&c.attempt_id], |r| r.get::<_,u32>(0)).unwrap(), 0); }
    lab.open(); task(lab.call(usage).await);
    let terminal = diagnostic(&c, 1, Observed::Completed); lab.fail_commit.store(true, Ordering::SeqCst);
    refused(&lab.call(terminal.clone()).await);
    lab.close().await; assert_eq!(ledger(&lab.database(), &c), (1, 0, 0));
    lab.open(); ack(&lab.call(terminal).await); lab.close().await;
    assert_eq!(ledger(&lab.database(), &c), (1, 1, 1));
}

#[tokio::test]
async fn no_diagnostic_credit_means_no_new_native_charge() {
    let mut limits = RuntimeLimits::seed().unwrap(); limits.controller.max_cleanup_receipts = 8;
    let mut lab = Lab::new(limits); let c = lab.actor().await;
    refused(&lab.call(reserve(&c, 1)).await);
    lab.close().await; let db = lab.database(); assert_eq!(ledger(&db, &c), (0, 0, 0));
    assert_eq!(db.query_row("SELECT model_requests FROM attempts WHERE id=?1", [&c.attempt_id], |r| r.get::<_,u32>(0)).unwrap(), 0);
}

//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A failed platform-owned provider request MUST retain bounded diagnostics sufficient to identify the observed failure class and available provider retry or quota information.
#[tokio::test]
async fn late_terminal_after_cancel_stop_and_replacement_does_not_change_current_task() {
    let mut lab = Lab::new(RuntimeLimits::seed().unwrap()); let c = lab.actor().await; task(lab.call(reserve(&c, 1)).await);
    let current = task(lab.call(Operation::GetTask { task_id: c.task_id.clone() }).await);
    task(lab.call(Operation::CancelTask { operation_id: unique("cancel"), task_id: c.task_id.clone(), expected_revision: current.revision }).await);
    task(lab.call(report(&c, unique("stopped"), Observation::ProcessTreeStopped)).await);
    assert!(matches!(lab.call(Operation::RegisterWorker { operation_id: unique("replace"), worker: Worker {
        id: c.worker_id.clone(), incarnation: "replacement".into(), coding_capacity: 0, native_capacity: 1 } }).await.outcome,
        Outcome::Ok { data: Data::Worker(_) }));
    let before = task(lab.call(Operation::GetTask { task_id: c.task_id.clone() }).await);
    ack(&lab.call(diagnostic(&c, 1, Observed::Completed)).await);
    assert_eq!(task(lab.call(Operation::GetTask { task_id: c.task_id.clone() }).await), before);
    lab.close().await; lab.open();
    assert_eq!(task(lab.call(Operation::GetTask { task_id: c.task_id.clone() }).await), before);
    lab.close().await;
}

//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A failed platform-owned provider request MUST retain bounded diagnostics sufficient to identify the observed failure class and available provider retry or quota information.
#[tokio::test]
async fn both_rejection_arrival_orders_require_exact_original_facts() {
    for diagnostic_first in [false, true] {
        let mut lab = Lab::new(RuntimeLimits::seed().unwrap()); let c = lab.actor().await; task(lab.call(reserve(&c, 1)).await);
        let terminal = diagnostic(&c, 1, Observed::KnownRejected { rejection_kind: RejectionKind::RateLimited });
        if diagnostic_first { ack(&lab.call(terminal.clone()).await); }
        task(lab.call(rejection(&c, 1)).await);
        if !diagnostic_first { ack(&lab.call(terminal).await); }
        lab.close().await; lab.open(); lab.close().await;
        assert_eq!(ledger(&lab.database(), &c), (1, 1, 1));
    }
    for diagnostic_first in [false, true] {
        let mut lab = Lab::new(RuntimeLimits::seed().unwrap()); let c = lab.actor().await; task(lab.call(reserve(&c, 1)).await);
        if diagnostic_first {
            ack(&lab.call(diagnostic(&c, 1, Observed::Completed)).await);
            refused(&lab.call(rejection(&c, 1)).await);
        } else {
            task(lab.call(rejection(&c, 1)).await);
            refused(&lab.call(diagnostic(&c, 1, Observed::Completed)).await);
        }
        lab.close().await; lab.open(); lab.close().await;
        assert_eq!(ledger(&lab.database(), &c), if diagnostic_first { (1,1,1) } else { (1,0,0) });
    }
}

#[tokio::test]
async fn completion_requires_marked_terminal_and_matching_success_kind() {
    for kind in [None, Some(Observed::NotStarted), Some(Observed::Completed)] {
        let mut lab = Lab::new(RuntimeLimits::seed().unwrap()); let c = lab.actor().await; task(lab.call(reserve(&c, 1)).await);
        if let Some(value) = &kind { ack(&lab.call(diagnostic(&c, 1, value.clone())).await); }
        let reply = lab.call(completion(&c)).await;
        if matches!(kind, Some(Observed::Completed)) { task(reply); } else { refused(&reply); }
        lab.close().await;
    }
}

#[tokio::test]
async fn native_result_candidate_cannot_bypass_its_marked_obligation() {
    let mut lab = Lab::new(RuntimeLimits::seed().unwrap());
    let c = lab.actor().await;
    task(lab.call(reserve(&c, 1)).await);
    task(lab.call(report(&c, unique("executor"), Observation::ExecutorOutcome { success: true, summary: "7".into() })).await);
    task(lab.call(report(&c, unique("stopped"), Observation::ProcessTreeStopped)).await);
    let reply = lab.call(report(&c, unique("candidate"), Observation::ResultCandidate {
        checks_passed: true, summary: "7".into(), checkpoint: None, validation: vec![], output: Some(json!(7)),
    })).await;
    assert!(matches!(&reply.outcome, Outcome::Refused { code, message }
        if code == "source_mismatch" && message.contains("diagnostic obligation remains outstanding")), "{reply:?}");
    let retained = task(lab.call(Operation::GetTask { task_id: c.task_id.clone() }).await);
    assert_ne!(retained.status, TaskStatus::Succeeded);
    lab.close().await;
    assert_eq!(ledger(&lab.database(), &c), (1, 0, 0));
}

#[tokio::test]
async fn malformed_terminal_and_token_only_updates_do_not_spend_or_mint_credit() {
    let mut lab = Lab::new(RuntimeLimits::seed().unwrap()); let c = lab.actor().await; task(lab.call(reserve(&c, 1)).await);
    let mut malformed = diagnostic(&c, 1, Observed::Completed);
    if let Operation::ObserveWorker { report } = &mut malformed { report.incarnation = "foreign".into(); }
    refused(&lab.call(malformed).await);
    task(lab.call(report(&c, unique("tokens"), Observation::Usage { model_requests: 1, input_tokens: Some(2),
        output_tokens: Some(3), native_diagnostic_reserved: false })).await);
    refused(&lab.call(report(&c, unique("double-credit"), Observation::Usage { model_requests: 1, input_tokens: Some(2),
        output_tokens: Some(3), native_diagnostic_reserved: true })).await);
    lab.close().await; assert_eq!(ledger(&lab.database(), &c), (1, 0, 0));
}

#[tokio::test]
async fn missing_or_inconsistent_diagnostic_rows_refuse_startup_without_repair() {
    for corruption in ["DELETE FROM native_request_diagnostic_credits",
        "UPDATE native_request_diagnostic_credits SET consumed=0",
        "DELETE FROM receipts WHERE id LIKE '%-native-diagnostic-%'",
        "DROP TABLE native_request_diagnostic_credits"] {
        let mut lab = Lab::new(RuntimeLimits::seed().unwrap()); let c = lab.actor().await; task(lab.call(reserve(&c, 1)).await);
        ack(&lab.call(diagnostic(&c, 1, Observed::Completed)).await); lab.close().await;
        let path = lab.root.join("tasks.db");
        { let db = Connection::open(&path).unwrap(); db.execute_batch(corruption).unwrap(); }
        let before = std::fs::read(&path).unwrap();
        assert!(Controller::open_with_config(&path, lab.config.clone()).is_err(), "{corruption}");
        assert_eq!(std::fs::read(&path).unwrap(), before, "startup must not reconstruct credit or terminal bytes");
    }
}
