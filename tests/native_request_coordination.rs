//! Actual Controller transactions for native request coordination.
//! No worker, provider, browser or remote process is launched by this fixture.
//! Permit/effect observations are explicit synthetic host reports; they do not
//! establish that a provider request ran or that an unknown effect was resolved.
use borg_runtime::{
    collaboration::{AttemptView, Query, QueryResult},
    controller::{Controller, ControllerConfig},
    limits::{NativeCircuitLimits, NativeRetryLimits, RuntimeLimits},
    mcp::Caller,
    native::{ProviderRejection, ProviderRequestBinding, RejectionKind, RetryAfter},
    native_retry::*,
    protocol::*,
};
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, atomic::{AtomicU64, Ordering}},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

static NEXT: AtomicU64 = AtomicU64::new(1);
const ENDPOINT: &str = "https://example.invalid/v1/responses";
const CREDENTIAL: &str = "fixture-credential-configuration-1";
const OPEN_MS: u64 = 50;
const AUTH_COOLDOWN_MS: u64 = 150;

fn unique(label: &str) -> String {
    format!("{label}-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed))
}
fn metadata() -> RuntimeLimits {
    let mut value = RuntimeLimits::seed().unwrap();
    value.execution.native.context_bytes = 65_536;
    value.controller.command_wait_ms = 5;
    // Explicit fixture values, never production defaults.
    value.native_retry = Some(NativeRetryLimits {
        initial_backoff_ms: 10,
        backoff_multiplier: 2,
        max_backoff_ms: 80,
        jitter_ms: 7,
        circuit: NativeCircuitLimits {
            failure_threshold: 2,
            open_ms: OPEN_MS,
            max_entries: 16,
        },
        auth_cooldown_ms: AUTH_COOLDOWN_MS,
        auth_max_entries: 16,
        coordination_record_bytes: 32_768,
    });
    value.validate().unwrap();
    value
}
fn key(endpoint: &str) -> CircuitKey {
    CircuitKey::new(&reqwest::Url::parse(endpoint).unwrap(), SELECTED_MODEL).unwrap()
}
fn transient() -> NativeRequestOutcome {
    NativeRequestOutcome::Rejected {
        rejection: ProviderRejection {
            kind: RejectionKind::RateLimited,
            retry_after: RetryAfter::Absent,
        },
    }
}
struct Lab {
    root: PathBuf,
    controller: Controller,
    config: ControllerConfig,
    clock: Arc<AtomicU64>,
}
impl Lab {
    fn new() -> Self {
        Self::with_limits(metadata())
    }
    fn with_limits(limits: RuntimeLimits) -> Self {
        limits.validate().unwrap();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let root = std::env::temp_dir().join(format!("{}-{now}", unique("native-coordination")));
        std::fs::create_dir(&root).unwrap();
        let clock = Arc::new(AtomicU64::new(now));
        let observed = clock.clone();
        let config = ControllerConfig {
            limits: Arc::new(limits),
            validation_executable: std::env::var_os("BORG_TEST_RUNTIME")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_borg-runtime"))),
            clock: Arc::new(move || observed.load(Ordering::SeqCst)),
            ..ControllerConfig::default()
        };
        let controller = Controller::open_with_config(root.join("tasks.sqlite"), config.clone()).unwrap();
        Self { root, controller, config, clock }
    }
    fn now(&self) -> u64 { self.clock.load(Ordering::SeqCst) }
    fn advance(&self, milliseconds: u64) {
        let next = self.now().checked_add(milliseconds).unwrap();
        self.clock.store(next, Ordering::SeqCst);
    }
    async fn restart(self) -> Self {
        let Self { root, controller, config, clock } = self;
        drop(controller);
        released(&root.join("tasks.sqlite")).await;
        let controller = Controller::open_with_config(root.join("tasks.sqlite"), config.clone()).unwrap();
        Self { root, controller, config, clock }
    }
    async fn actor(&self) -> Caller {
        let input = TaskInput { instruction_source: None,
            pull_request: None,
            instruction: "Return one bounded structured answer".into(),
            definition: Definition {
                id: unique("agent"),
                revision: "native-coordination-fixture-v1".into(),
                executor: ExecutorKind::Native,
                inference: Some(InferenceSettings::selected()),
                execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
                limit_overrides: None,
                charter: None,
            },
            repository: None,
            repository_read: None,
            acceptance_commands: vec![],
            output_schema: Some(json!({"type":"integer"})),
            limits: Limits {
                deadline_unix_ms: self.now().checked_add(600_000).unwrap(),
                model_requests: 16,
                output_bytes: 16_384,
                result_bytes: self.config.limits.task_defaults.result_bytes,
                max_delegation_depth: 4,
            },
            dependencies: vec![],
            native_workflow: None,
            resolved_limits: None,
        };
        let submitted = task(call(&self.controller, Operation::SubmitTask {
            operation_id: unique("submit"), input,
        }).await);
        let worker = Worker {
            id: unique("worker"), incarnation: "incarnation-1".into(),
            coding_capacity: 0, native_capacity: 1,
        };
        assert!(matches!(call(&self.controller, Operation::RegisterWorker {
            operation_id: unique("register"), worker: worker.clone(),
        }).await.outcome, Outcome::Ok { data: Data::Worker(_) }));
        let reply = call(&self.controller, Operation::AwaitCommands {
            operation_id: unique("await"),
            worker_id: worker.id,
            incarnation: worker.incarnation,
            acknowledged: vec![],
        }).await;
        let Outcome::Ok { data: Data::Commands(commands) } = reply.outcome else {
            panic!("ordinary admitted commands: {reply:?}")
        };
        let attempt = commands.into_iter().find_map(|command| match command {
            WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == submitted.id => Some(attempt),
            _ => None,
        }).expect("one ordinary current attempt");
        let caller = Caller {
            task_id: attempt.task_id, attempt_id: attempt.id, generation: attempt.generation,
            worker_id: attempt.worker_id, incarnation: attempt.worker_incarnation,
        };
        report(&self.controller, &caller, Observation::Started { thread_id: None, turn_id: None }).await;
        caller
    }
}
async fn released(path: &Path) {
    let mut lock = path.as_os_str().to_owned();
    lock.push(".controller-lock");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let file = std::fs::OpenOptions::new().read(true).write(true).open(PathBuf::from(&lock)).unwrap();
            if file.try_lock().is_ok() { break; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.expect("previous controller released its owner lock");
}
async fn call(controller: &Controller, command: Operation) -> Reply {
    let reply = controller.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: unique("request"),
        command: command.clone(),
    }).await;
    reply.validate_for(&command).unwrap();
    reply
}
fn task(reply: Reply) -> Task {
    match reply.outcome {
        Outcome::Ok { data: Data::Task(value) } => *value,
        other => panic!("expected task: {other:?}"),
    }
}
fn refused(reply: &Reply, expected: &str) {
    assert!(matches!(&reply.outcome, Outcome::Refused { code, .. } if code == expected), "{reply:?}");
}
fn same_receipt(first: &Reply, replay: &Reply) {
    assert_eq!(first.outcome, replay.outcome);
    assert_eq!(first.commit, replay.commit);
    assert!(first.commit.is_some(), "original mutation needs a committed receipt");
}
async fn report(controller: &Controller, caller: &Caller, observation: Observation) -> Task {
    task(call(controller, Operation::ObserveWorker { report: Report {
        report_id: unique("report"), worker_id: caller.worker_id.clone(),
        incarnation: caller.incarnation.clone(), task_id: caller.task_id.clone(),
        attempt_id: caller.attempt_id.clone(), generation: caller.generation, observation,
    }}).await)
}
async fn attempt_view(controller: &Controller, caller: &Caller) -> AttemptView {
    let reply = call(controller, Operation::QueryCollaboration { query: Query::TaskAttempts {
        task_id: caller.task_id.clone(), after: None, limit: 16,
    }}).await;
    let Outcome::Ok { data: Data::Collaboration(QueryResult::TaskAttempts(page)) } = reply.outcome else {
        panic!("attempt projection: {reply:?}")
    };
    assert!(page.next_cursor.is_none());
    page.items.into_iter().find(|entry| entry.attempt.id == caller.attempt_id)
        .expect("original attempt remains observable")
}
fn acquire(caller: &Caller, request_index: u32, endpoint: &str, credential: &str) -> Operation {
    Operation::AcquireNativeRequest {
        operation_id: unique("acquire"), caller: caller.clone(), request_index,
        key: key(endpoint), credential_configuration: credential.into(),
    }
}
fn settle(caller: &Caller, acquisition: &Operation, outcome: NativeRequestOutcome) -> Operation {
    Operation::SettleNativeRequest {
        operation_id: unique("settle"), caller: caller.clone(),
        acquire_operation_id: acquisition.mutation_id().unwrap().into(), outcome,
    }
}

fn admission(reply: &Reply) -> NativeRequestAdmission {
    match &reply.outcome {
        Outcome::Ok { data: Data::NativeRequestAdmission(value) } => value.clone(),
        other => panic!("expected native admission: {other:?}"),
    }
}
fn granted(reply: &Reply) -> NativeRequestPermit {
    match admission(reply) {
        NativeRequestAdmission::Granted { permit } => permit,
        other => panic!("expected native permit: {other:?}"),
    }
}
fn waiting(reply: &Reply, expected: NativeWaitReason) -> (Option<u64>, u64) {
    match admission(reply) {
        NativeRequestAdmission::Wait { reason, retry_at_ms, generation } => {
            assert_eq!(reason, expected);
            (retry_at_ms, generation)
        }
        other => panic!("expected bounded native wait: {other:?}"),
    }
}
fn settlement(reply: &Reply) -> NativeRequestSettlement {
    match &reply.outcome {
        Outcome::Ok { data: Data::NativeRequestSettlement(value) } => value.clone(),
        other => panic!("expected native settlement: {other:?}"),
    }
}
async fn reserve(controller: &Controller, caller: &Caller, index: u32) -> Reply {
    let command = Operation::ObserveWorker { report: Report {
        report_id: borg_runtime::native_loop_contract::reservation_id(&caller.attempt_id, index),
        worker_id: caller.worker_id.clone(), incarnation: caller.incarnation.clone(),
        task_id: caller.task_id.clone(), attempt_id: caller.attempt_id.clone(),
        generation: caller.generation,
        observation: Observation::Usage {
            native_diagnostic_reserved: false,
            model_requests: index, input_tokens: None, output_tokens: None,
        },
    }};
    let reply = call(controller, command).await;
    assert!(matches!(reply.outcome, Outcome::Ok { data: Data::Task(_) }), "{reply:?}");
    assert!(reply.commit.is_some());
    reply
}
async fn record_rejection(
    controller: &Controller, caller: &Caller, acquisition: &Operation, kind: RejectionKind,
) -> ProviderRejection {
    let Operation::AcquireNativeRequest { request_index, key, .. } = acquisition else { unreachable!() };
    let rejection = ProviderRejection { kind, retry_after: RetryAfter::Absent };
    let command = Operation::ObserveWorker { report: Report {
        report_id: borg_runtime::native_loop_contract::rejection_id(&caller.attempt_id, *request_index),
        worker_id: caller.worker_id.clone(), incarnation: caller.incarnation.clone(),
        task_id: caller.task_id.clone(), attempt_id: caller.attempt_id.clone(),
        generation: caller.generation,
        observation: Observation::NativeRequestRejected {
            request_index: *request_index, endpoint: key.endpoint.clone(), model: key.model.clone(),
            rejection, diagnostic: "Explicit synthetic fixture rejection observation".into(),
            // Trusted fixture observation of the bounded synthetic bytes `{}`;
            // this is not a claim that a provider request was sent.
            request_binding: ProviderRequestBinding {
                bytes: 2,
                sha256: "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a".into(),
            },
        },
    }};
    let original = call(controller, command.clone()).await;
    assert!(matches!(original.outcome, Outcome::Ok { data: Data::Task(_) }), "{original:?}");
    same_receipt(&original, &call(controller, command).await);
    rejection
}
async fn assert_charge(controller: &Controller, caller: &Caller, expected: u32) {
    let view = attempt_view(controller, caller).await;
    assert_eq!(view.reported_model_requests, expected);
    assert_eq!(view.input_tokens, None);
    assert_eq!(view.output_tokens, None);
}
async fn open_with_unknown_observations(lab: &Lab) -> NativeRequestSettlement {
    let mut last = None;
    for _ in 0..2 {
        let caller = lab.actor().await;
        let acquisition = acquire(&caller, 1, ENDPOINT, CREDENTIAL);
        let permit = granted(&call(&lab.controller, acquisition.clone()).await);
        assert!(!permit.circuit.probe);
        reserve(&lab.controller, &caller, 1).await;
        let result = settlement(&call(&lab.controller,
            settle(&caller, &acquisition, NativeRequestOutcome::Unknown)).await);
        assert_eq!(result.outcome, NativeRequestOutcome::Unknown);
        assert_eq!(result.source, NativeSettlementSource::Reported);
        assert_eq!(result.permit, permit);
        let duplicate = settlement(&call(&lab.controller,
            settle(&caller, &acquisition, NativeRequestOutcome::Unknown)).await);
        assert_eq!(duplicate, result, "another settlement identity cannot count the same health failure twice");
        assert_charge(&lab.controller, &caller, 1).await;
        let unsafe_next = call(&lab.controller, acquire(&caller, 2, ENDPOINT, CREDENTIAL)).await;
        refused(&unsafe_next, "reconciliation_required");
        assert_charge(&lab.controller, &caller, 1).await;
        last = Some(result);
    }
    let result = last.unwrap();
    assert_eq!(result.circuit_retry_at_ms, Some(lab.now() + OPEN_MS));
    result
}

#[tokio::test]
async fn acquisition_replays_original_permit_without_usage_and_conflicting_identity_refuses() {
    let lab = Lab::new();
    let caller = lab.actor().await;
    let acquisition = acquire(&caller, 1, ENDPOINT, CREDENTIAL);
    let original = call(&lab.controller, acquisition.clone()).await;
    let permit = granted(&original);
    assert_eq!(permit.circuit.operation_id, acquisition.mutation_id().unwrap());
    assert_eq!(permit.circuit.caller, caller);
    assert_eq!(permit.circuit.key, key(ENDPOINT));
    assert_eq!(permit.request_index, 1);
    assert_eq!(permit.credential_configuration, CREDENTIAL);
    assert!(!permit.circuit.probe && !permit.auth_probe);
    assert_charge(&lab.controller, &caller, 0).await;

    let replay = call(&lab.controller, acquisition.clone()).await;
    same_receipt(&original, &replay);
    let same_slot = call(&lab.controller, acquire(&caller, 1, ENDPOINT, CREDENTIAL)).await;
    assert_eq!(granted(&same_slot), permit, "a new mutation identity cannot replace an existing request slot");
    refused(&call(&lab.controller,
        acquire(&caller, 1, ENDPOINT, "fixture-changed-credential")).await, "operation_conflict");
    assert_charge(&lab.controller, &caller, 0).await;
    let mut changed = acquisition.clone();
    let Operation::AcquireNativeRequest { request_index, .. } = &mut changed else { unreachable!() };
    *request_index = 2;
    refused(&call(&lab.controller, changed).await, "operation_conflict");

    let lab = lab.restart().await;
    same_receipt(&original, &call(&lab.controller, acquisition.clone()).await);
    let result = settlement(&call(&lab.controller,
        settle(&caller, &acquisition, NativeRequestOutcome::NotStarted)).await);
    assert_eq!(result.outcome, NativeRequestOutcome::NotStarted);
    assert_eq!(result.permit, permit);
    assert_charge(&lab.controller, &caller, 0).await;
}

#[tokio::test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A circuit-breaker probe outcome MUST be settled against its exact original request identity and probe generation.
async fn settlement_requires_original_charge_and_deduplicates_each_permit_across_operation_ids() {
    let lab = Lab::new();
    let caller = lab.actor().await;
    let acquisition = acquire(&caller, 1, ENDPOINT, CREDENTIAL);
    let permit = granted(&call(&lab.controller, acquisition.clone()).await);
    let premature = settle(&caller, &acquisition, NativeRequestOutcome::Healthy);
    let missing = call(&lab.controller, premature.clone()).await;
    refused(&missing, "source_mismatch");
    assert_charge(&lab.controller, &caller, 0).await;
    reserve(&lab.controller, &caller, 1).await;
    // A committed refusal does not become success when its prerequisites change.
    same_receipt(&missing, &call(&lab.controller, premature).await);

    let completion = settle(&caller, &acquisition, NativeRequestOutcome::Healthy);
    let original = call(&lab.controller, completion.clone()).await;
    let value = settlement(&original);
    assert_eq!(value.permit, permit);
    assert_eq!(value.outcome, NativeRequestOutcome::Healthy);
    assert_eq!(value.source, NativeSettlementSource::Reported);
    assert_eq!(value.circuit, CircuitCompletionDisposition::Applied);
    same_receipt(&original, &call(&lab.controller, completion.clone()).await);

    let equivalent = call(&lab.controller,
        settle(&caller, &acquisition, NativeRequestOutcome::Healthy)).await;
    assert_eq!(settlement(&equivalent), value, "new transport/mutation identity cannot count one permit twice");
    let conflict = call(&lab.controller,
        settle(&caller, &acquisition, NativeRequestOutcome::NotStarted)).await;
    refused(&conflict, "operation_conflict");
    let mut changed = completion;
    let Operation::SettleNativeRequest { outcome, .. } = &mut changed else { unreachable!() };
    *outcome = NativeRequestOutcome::Unknown;
    refused(&call(&lab.controller, changed).await, "operation_conflict");
    assert_charge(&lab.controller, &caller, 1).await;
}

#[tokio::test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# The platform MUST maintain one coordinated circuit breaker for each configured model and endpoint.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A new circuit-breaker probe after a prior unknown outcome MUST NOT replay the original uncertain request.
async fn shared_circuit_wait_is_uncharged_and_restart_keeps_one_cross_worker_probe() {
    let lab = Lab::new();
    let opened = open_with_unknown_observations(&lab).await;
    let caller_a = lab.actor().await;
    let caller_b = lab.actor().await;
    let early = acquire(&caller_a, 1, ENDPOINT, CREDENTIAL);
    let early_reply = call(&lab.controller, early.clone()).await;
    let (retry_at, generation) = waiting(&early_reply, NativeWaitReason::CircuitOpen);
    assert_eq!(retry_at, opened.circuit_retry_at_ms);
    assert_charge(&lab.controller, &caller_a, 0).await;

    let isolated = lab.actor().await;
    let other_endpoint = "https://other.invalid/v1/responses";
    let independent = granted(&call(&lab.controller,
        acquire(&isolated, 1, other_endpoint, CREDENTIAL)).await);
    assert!(!independent.circuit.probe);
    assert_eq!(independent.circuit.key, key(other_endpoint));
    assert_charge(&lab.controller, &isolated, 0).await;

    let lab = lab.restart().await;
    same_receipt(&early_reply, &call(&lab.controller, early).await);
    lab.advance(OPEN_MS);
    let attempt_a = acquire(&caller_a, 1, ENDPOINT, CREDENTIAL);
    let attempt_b = acquire(&caller_b, 1, ENDPOINT, CREDENTIAL);
    let (reply_a, reply_b) = tokio::join!(
        call(&lab.controller, attempt_a.clone()),
        call(&lab.controller, attempt_b.clone()),
    );
    let entries = [(&caller_a, &attempt_a, &reply_a), (&caller_b, &attempt_b, &reply_b)];
    let mut winner = None;
    let mut waits = 0;
    for (caller, operation, reply) in entries {
        match admission(reply) {
            NativeRequestAdmission::Granted { permit } => {
                assert!(permit.circuit.probe);
                assert_eq!(permit.circuit.generation, generation);
                assert!(winner.replace((caller, operation, permit)).is_none());
            }
            NativeRequestAdmission::Wait { reason, retry_at_ms, generation: seen } => {
                assert_eq!(reason, NativeWaitReason::CircuitProbePending);
                assert_eq!(retry_at_ms, None);
                assert_eq!(seen, generation);
                waits += 1;
            }
        }
        assert_charge(&lab.controller, caller, 0).await;
    }
    assert_eq!(waits, 1);
    let (caller, operation, permit) = winner.expect("one probe admitted");
    reserve(&lab.controller, caller, 1).await;
    let closed = settlement(&call(&lab.controller,
        settle(caller, operation, NativeRequestOutcome::Healthy)).await);
    assert_eq!(closed.permit, permit);
    let next = lab.actor().await;
    let after = granted(&call(&lab.controller, acquire(&next, 1, ENDPOINT, CREDENTIAL)).await);
    assert!(!after.circuit.probe);
    assert!(after.circuit.generation > generation);
}

#[tokio::test]
async fn authentication_cooldown_is_shared_across_workers_and_isolated_by_configuration_and_endpoint() {
    let lab = Lab::new();
    let first = lab.actor().await;
    let failed = acquire(&first, 1, ENDPOINT, CREDENTIAL);
    granted(&call(&lab.controller, failed.clone()).await);
    reserve(&lab.controller, &first, 1).await;
    let rejection = record_rejection(&lab.controller, &first, &failed, RejectionKind::AuthenticationFailed).await;
    let result = settlement(&call(&lab.controller,
        settle(&first, &failed, NativeRequestOutcome::Rejected { rejection })).await);
    assert_eq!(result.outcome, NativeRequestOutcome::Rejected { rejection });
    assert_eq!(result.circuit_retry_at_ms, None);
    assert_eq!(result.auth_retry_at_ms, Some(lab.now() + AUTH_COOLDOWN_MS));
    assert_charge(&lab.controller, &first, 1).await;

    let second = lab.actor().await;
    let third = lab.actor().await;
    let deferred = call(&lab.controller, acquire(&second, 1, ENDPOINT, CREDENTIAL)).await;
    let (until, generation) = waiting(&deferred, NativeWaitReason::AuthenticationCooldown);
    assert_eq!(until, result.auth_retry_at_ms);
    assert_charge(&lab.controller, &second, 0).await;

    for (endpoint, credential) in [
        (ENDPOINT, "fixture-credential-configuration-2"),
        ("https://auth-other.invalid/v1/responses", CREDENTIAL),
    ] {
        let independent = lab.actor().await;
        let permit = granted(&call(&lab.controller, acquire(&independent, 1, endpoint, credential)).await);
        assert!(!permit.auth_probe);
        assert_charge(&lab.controller, &independent, 0).await;
    }

    let lab = lab.restart().await;
    lab.advance(AUTH_COOLDOWN_MS);
    let one = acquire(&second, 1, ENDPOINT, CREDENTIAL);
    let two = acquire(&third, 1, ENDPOINT, CREDENTIAL);
    let (left, right) = tokio::join!(call(&lab.controller, one), call(&lab.controller, two));
    let admissions = [admission(&left), admission(&right)];
    assert_eq!(admissions.iter().filter(|value| matches!(value,
        NativeRequestAdmission::Granted { permit } if permit.auth_probe && permit.auth_generation == generation)).count(), 1);
    assert_eq!(admissions.iter().filter(|value| matches!(value,
        NativeRequestAdmission::Wait { reason: NativeWaitReason::AuthenticationProbePending, retry_at_ms: None, generation: seen }
            if *seen == generation)).count(), 1);
    assert_charge(&lab.controller, &second, 0).await;
    assert_charge(&lab.controller, &third, 0).await;
}

#[tokio::test]
async fn elapsed_time_does_not_release_a_live_unreported_probe() {
    let lab = Lab::new();
    open_with_unknown_observations(&lab).await;
    lab.advance(OPEN_MS);
    let owner = lab.actor().await;
    let acquisition = acquire(&owner, 1, ENDPOINT, CREDENTIAL);
    let original = call(&lab.controller, acquisition.clone()).await;
    let permit = granted(&original);
    assert!(permit.circuit.probe);
    reserve(&lab.controller, &owner, 1).await;
    let lab = lab.restart().await;
    lab.advance(OPEN_MS * 100);
    let other = lab.actor().await;
    let waiting_reply = call(&lab.controller, acquire(&other, 1, ENDPOINT, CREDENTIAL)).await;
    let (retry_at, generation) = waiting(&waiting_reply, NativeWaitReason::CircuitProbePending);
    assert_eq!(retry_at, None);
    assert_eq!(generation, permit.circuit.generation);
    same_receipt(&original, &call(&lab.controller, acquisition).await);
    assert_charge(&lab.controller, &other, 0).await;
    assert_charge(&lab.controller, &owner, 1).await;
    let retained = attempt_view(&lab.controller, &owner).await;
    assert!(!retained.attempt.stopped && !retained.attempt.uncertain);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A new circuit-breaker probe after a prior unknown outcome MUST NOT replay the original uncertain request.
async fn fenced_probe_owner_enters_health_cooldown_without_settling_the_original_effect() {
    let lab = Lab::new();
    let recovering_credential = "fixture-previously-unseen-credential";
    open_with_unknown_observations(&lab).await;
    lab.advance(OPEN_MS);
    let owner = lab.actor().await;
    let acquisition = acquire(&owner, 1, ENDPOINT, CREDENTIAL);
    let original = call(&lab.controller, acquisition.clone()).await;
    let permit = granted(&original);
    assert!(permit.circuit.probe);
    reserve(&lab.controller, &owner, 1).await;
    report(&lab.controller, &owner, Observation::ExecutionUncertain {
        message: "Synthetic provider effect remains unknown".into(),
    }).await;
    report(&lab.controller, &owner, Observation::ProcessTreeStopped).await;
    let before = attempt_view(&lab.controller, &owner).await;
    assert!(before.attempt.stopped && before.attempt.uncertain);

    let next = lab.actor().await;
    let wait_reply = call(&lab.controller, acquire(&next, 1, ENDPOINT, recovering_credential)).await;
    let (retry_at, generation) = waiting(&wait_reply, NativeWaitReason::CircuitOpen);
    assert_eq!(retry_at, Some(lab.now() + OPEN_MS));
    assert!(generation > permit.circuit.generation);
    assert_charge(&lab.controller, &next, 0).await;
    let lab = lab.restart().await;
    same_receipt(&original, &call(&lab.controller, acquisition.clone()).await);
    lab.advance(OPEN_MS);
    let independent = granted(&call(&lab.controller, acquire(&next, 1, ENDPOINT, recovering_credential)).await);
    assert!(independent.circuit.probe);
    assert_eq!(independent.credential_configuration, recovering_credential);
    assert_eq!(independent.circuit.generation, generation);
    assert_ne!(independent.circuit.operation_id, permit.circuit.operation_id);
    let retained = attempt_view(&lab.controller, &owner).await;
    assert_eq!(retained.reported_model_requests, 1);
    assert!(retained.attempt.stopped && retained.attempt.uncertain);

    let reconciled = settlement(&call(&lab.controller,
        settle(&owner, &acquisition, NativeRequestOutcome::Unknown)).await);
    assert_eq!(reconciled.source, NativeSettlementSource::FencedOwner);
    assert_eq!(reconciled.outcome, NativeRequestOutcome::Unknown);
    assert_eq!(reconciled.permit, permit);
    assert_eq!(reconciled.circuit_retry_at_ms, retry_at);

    let late = call(&lab.controller, settle(&owner, &acquisition, NativeRequestOutcome::Healthy)).await;
    refused(&late, "operation_conflict");
    let contender = lab.actor().await;
    let still_held = call(&lab.controller, acquire(&contender, 1, ENDPOINT, CREDENTIAL)).await;
    assert_eq!(waiting(&still_held, NativeWaitReason::CircuitProbePending), (None, generation));
}

#[tokio::test]
async fn rejection_health_requires_the_exact_prior_rejection_receipt() {
    let lab = Lab::new();
    let caller = lab.actor().await;
    let acquisition = acquire(&caller, 1, ENDPOINT, CREDENTIAL);
    granted(&call(&lab.controller, acquisition.clone()).await);
    reserve(&lab.controller, &caller, 1).await;
    let report_missing = settle(&caller, &acquisition, transient());
    let refused_reply = call(&lab.controller, report_missing.clone()).await;
    refused(&refused_reply, "source_mismatch");
    same_receipt(&refused_reply, &call(&lab.controller, report_missing).await);
    assert_charge(&lab.controller, &caller, 1).await;
}

#[tokio::test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A provider quota exhaustion MUST be distinguished from transient throttling.
async fn all_nontransient_rejections_preserve_circuit_health_and_actual_request_charges() {
    let lab = Lab::new();
    assert!(RejectionKind::RateLimited.same_input_retryable());
    // One real reservation with an explicitly unknown synthetic observation
    // leaves the threshold-two circuit one failure short of opening.
    let first = lab.actor().await;
    let acquisition = acquire(&first, 1, ENDPOINT, CREDENTIAL);
    let initial = granted(&call(&lab.controller, acquisition.clone()).await);
    reserve(&lab.controller, &first, 1).await;
    let prior = settlement(&call(&lab.controller,
        settle(&first, &acquisition, NativeRequestOutcome::Unknown)).await);
    assert_eq!(prior.circuit_retry_at_ms, None);
    assert_charge(&lab.controller, &first, 1).await;

    for kind in [RejectionKind::InvalidRequest, RejectionKind::ContextLimit,
        RejectionKind::PolicyRefused, RejectionKind::QuotaExhausted, RejectionKind::AuthenticationFailed] {
        assert!(!kind.same_input_retryable());
        let caller = lab.actor().await;
        // Auth cooldown remains credential-specific; every case shares the
        // same admitted model and endpoint circuit.
        let credential = unique("nontransient-credential");
        let acquisition = acquire(&caller, 1, ENDPOINT, &credential);
        let permit = granted(&call(&lab.controller, acquisition.clone()).await);
        assert!(!permit.circuit.probe);
        assert_eq!(permit.circuit.generation, initial.circuit.generation);
        reserve(&lab.controller, &caller, 1).await;
        let rejection = record_rejection(&lab.controller, &caller, &acquisition, kind).await;
        let result = settlement(&call(&lab.controller,
            settle(&caller, &acquisition, NativeRequestOutcome::Rejected { rejection })).await);
        assert_eq!(result.outcome, NativeRequestOutcome::Rejected { rejection });
        assert_eq!(result.source, NativeSettlementSource::Reported);
        assert_eq!(result.circuit, CircuitCompletionDisposition::Applied);
        assert_eq!(result.circuit_retry_at_ms, None, "known {kind:?} must not open the health circuit");
        assert_eq!(result.auth_retry_at_ms,
            (kind == RejectionKind::AuthenticationFailed).then_some(lab.now() + AUTH_COOLDOWN_MS));
        assert_charge(&lab.controller, &caller, 1).await;
    }

    // Neutral rejections neither increment nor erase the preceding failure.
    let next = lab.actor().await;
    let acquisition = acquire(&next, 1, ENDPOINT, CREDENTIAL);
    let permit = granted(&call(&lab.controller, acquisition.clone()).await);
    assert!(!permit.circuit.probe);
    assert_eq!(permit.circuit.generation, initial.circuit.generation);
    reserve(&lab.controller, &next, 1).await;
    let opened = settlement(&call(&lab.controller,
        settle(&next, &acquisition, NativeRequestOutcome::Unknown)).await);
    assert_eq!(opened.circuit_retry_at_ms, Some(lab.now() + OPEN_MS));
    assert_charge(&lab.controller, &next, 1).await;
}

#[tokio::test]
async fn authentication_settlement_requires_its_exact_canonical_rejection_kind() {
    let lab = Lab::new();
    for prior_kind in [None, Some(RejectionKind::QuotaExhausted)] {
        let caller = lab.actor().await;
        let credential = unique("missing-auth-proof-credential");
        let acquisition = acquire(&caller, 1, ENDPOINT, &credential);
        granted(&call(&lab.controller, acquisition.clone()).await);
        reserve(&lab.controller, &caller, 1).await;
        if let Some(kind) = prior_kind {
            record_rejection(&lab.controller, &caller, &acquisition, kind).await;
        }
        for outcome in [NativeRequestOutcome::AuthenticationFailed,
            NativeRequestOutcome::Rejected { rejection: ProviderRejection {
                kind: RejectionKind::AuthenticationFailed, retry_after: RetryAfter::Absent,
            } }] {
            let command = settle(&caller, &acquisition, outcome);
            let original = call(&lab.controller, command.clone()).await;
            refused(&original, "source_mismatch");
            same_receipt(&original, &call(&lab.controller, command).await);
            assert_charge(&lab.controller, &caller, 1).await;
        }
        let independent = lab.actor().await;
        let permit = granted(&call(&lab.controller,
            acquire(&independent, 1, ENDPOINT, &credential)).await);
        assert!(!permit.circuit.probe && !permit.auth_probe, "refused auth settlement cannot install a cooldown");
        assert_charge(&lab.controller, &independent, 0).await;
    }
}

#[tokio::test]
async fn compatibility_authentication_outcome_accepts_only_the_original_authentication_receipt() {
    let lab = Lab::new();
    let caller = lab.actor().await;
    let acquisition = acquire(&caller, 1, ENDPOINT, CREDENTIAL);
    granted(&call(&lab.controller, acquisition.clone()).await);
    reserve(&lab.controller, &caller, 1).await;
    let premature = settle(&caller, &acquisition, NativeRequestOutcome::AuthenticationFailed);
    let missing = call(&lab.controller, premature.clone()).await;
    refused(&missing, "source_mismatch");
    record_rejection(&lab.controller, &caller, &acquisition, RejectionKind::AuthenticationFailed).await;
    // The old refusal remains immutable even after its missing proof arrives.
    same_receipt(&missing, &call(&lab.controller, premature).await);
    let original = call(&lab.controller,
        settle(&caller, &acquisition, NativeRequestOutcome::AuthenticationFailed)).await;
    let result = settlement(&original);
    assert_eq!(result.outcome, NativeRequestOutcome::AuthenticationFailed);
    assert_eq!(result.circuit_retry_at_ms, None);
    assert_eq!(result.auth_retry_at_ms, Some(lab.now() + AUTH_COOLDOWN_MS));
    assert_charge(&lab.controller, &caller, 1).await;
    let lab = lab.restart().await;
    let equivalent = call(&lab.controller,
        settle(&caller, &acquisition, NativeRequestOutcome::AuthenticationFailed)).await;
    assert_eq!(settlement(&equivalent), result, "startup and settlement replay retain the canonical auth proof");
}

#[tokio::test]
async fn circuit_key_capacity_refuses_a_new_endpoint_without_charge_or_losing_original_permit() {
    let mut limits = metadata();
    limits.native_retry.as_mut().unwrap().circuit.max_entries = 1;
    let lab = Lab::with_limits(limits);
    let original_caller = lab.actor().await;
    let acquisition = acquire(&original_caller, 1, ENDPOINT, CREDENTIAL);
    let original = call(&lab.controller, acquisition.clone()).await;
    let permit = granted(&original);
    assert_charge(&lab.controller, &original_caller, 0).await;

    let rejected_caller = lab.actor().await;
    let other = acquire(&rejected_caller, 1, "https://capacity-other.invalid/v1/responses", CREDENTIAL);
    let full = call(&lab.controller, other.clone()).await;
    refused(&full, "capacity");
    assert_charge(&lab.controller, &rejected_caller, 0).await;
    same_receipt(&full, &call(&lab.controller, other).await);
    same_receipt(&original, &call(&lab.controller, acquisition.clone()).await);

    let lab = lab.restart().await;
    same_receipt(&original, &call(&lab.controller, acquisition.clone()).await);
    assert_charge(&lab.controller, &original_caller, 0).await;
    assert_charge(&lab.controller, &rejected_caller, 0).await;
    let retained = settlement(&call(&lab.controller,
        settle(&original_caller, &acquisition, NativeRequestOutcome::NotStarted)).await);
    assert_eq!(retained.permit, permit);
    assert_eq!(retained.outcome, NativeRequestOutcome::NotStarted);
    // Capacity is retained-key capacity: settling a request does not silently
    // delete the original model/endpoint state to admit another key.
    refused(&call(&lab.controller, acquire(&rejected_caller, 1,
        "https://capacity-other.invalid/v1/responses", CREDENTIAL)).await, "capacity");
    assert_charge(&lab.controller, &rejected_caller, 0).await;
}

// Closed-fixture inspection only. The production worker has no SQLite access.
fn coordination_rows(path: &Path) -> Vec<(String, String, String)> {
    let db = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut statement = db.prepare(
        "SELECT kind,key,body FROM native_request_coordination ORDER BY kind,key LIMIT 65",
    ).unwrap();
    let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    assert!(rows.len() <= 64, "bounded fixture coordination inventory");
    rows
}
fn receipt_bytes(path: &Path, operation_id: &str) -> (String, String) {
    let db = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    db.query_row("SELECT request,outcome FROM receipts WHERE id=?1", [operation_id],
        |row| Ok((row.get(0)?, row.get(1)?))).unwrap()
}

#[tokio::test]
async fn startup_preserves_canonical_probe_then_refuses_a_corrupt_original_acquire_binding() {
    let lab = Lab::new();
    let earlier = open_with_unknown_observations(&lab).await;
    lab.advance(OPEN_MS);
    let owner = lab.actor().await;
    let acquisition = acquire(&owner, 1, ENDPOINT, CREDENTIAL);
    let original = call(&lab.controller, acquisition.clone()).await;
    let permit = granted(&original);
    assert!(permit.circuit.probe);
    reserve(&lab.controller, &owner, 1).await;

    let Lab { root, controller, config, clock: _ } = lab;
    let path = root.join("tasks.sqlite");
    drop(controller);
    released(&path).await;
    let before = coordination_rows(&path);
    let original_receipt = receipt_bytes(&path, acquisition.mutation_id().unwrap());

    // Establish the positive startup path before changing the closed test DB.
    let controller = Controller::open_with_config(&path, config.clone()).unwrap();
    same_receipt(&original, &call(&controller, acquisition.clone()).await);
    assert_charge(&controller, &owner, 1).await;
    drop(controller);
    released(&path).await;
    assert_eq!(coordination_rows(&path), before);
    assert_eq!(receipt_bytes(&path, acquisition.mutation_id().unwrap()), original_receipt);

    // Same serialization shape as the private stored request slot; retain all
    // valid bytes semantically except the exact original-acquire association.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct StoredRequestSlot {
        permit: NativeRequestPermit,
        settlement: Option<NativeRequestSettlement>,
    }
    let request_key = serde_json::to_string(&(&owner.attempt_id, 1u32)).unwrap();
    let db = rusqlite::Connection::open(&path).unwrap();
    let body: String = db.query_row(
        "SELECT body FROM native_request_coordination WHERE kind='request' AND key=?1",
        [&request_key], |row| row.get(0),
    ).unwrap();
    let mut slot: StoredRequestSlot = serde_json::from_str(&body).unwrap();
    assert_eq!(slot.permit, permit);
    assert!(slot.settlement.is_none());
    assert_eq!(serde_json::to_string(&slot).unwrap(), body, "fixture slot starts in canonical encoding");
    let other_id = earlier.permit.circuit.operation_id;
    assert_ne!(other_id, slot.permit.circuit.operation_id);
    let other_exists: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM receipts WHERE id=?1)", [&other_id], |row| row.get(0),
    ).unwrap();
    assert!(other_exists, "corruption points at a real foreign acquire receipt");
    slot.permit.circuit.operation_id = other_id;
    let corrupt = serde_json::to_string(&slot).unwrap();
    assert_eq!(db.execute(
        "UPDATE native_request_coordination SET body=?2 WHERE kind='request' AND key=?1",
        rusqlite::params![request_key, corrupt],
    ).unwrap(), 1);
    drop(db);
    let corrupted = coordination_rows(&path);

    let error = Controller::open_with_config(&path, config).err()
        .expect("startup must refuse a held-probe slot whose original receipt binding differs");
    assert!(error.contains("native"), "unexpected startup failure: {error}");
    released(&path).await;
    assert_eq!(coordination_rows(&path), corrupted, "startup refusal preserves corrupt evidence");
    assert_eq!(receipt_bytes(&path, acquisition.mutation_id().unwrap()), original_receipt);
}




#[tokio::test]
async fn fatal_rejection_fences_the_original_attempt_after_cooldown_and_restart() {
    let mut lab = Lab::new();
    for (kind, compatibility) in [
        (RejectionKind::InvalidRequest, false),
        (RejectionKind::ContextLimit, false),
        (RejectionKind::PolicyRefused, false),
        (RejectionKind::QuotaExhausted, false),
        (RejectionKind::AuthenticationFailed, false),
        (RejectionKind::AuthenticationFailed, true),
    ] {
        let caller = lab.actor().await;
        let credential = unique("fatal-rejection-credential");
        let acquisition = acquire(&caller, 1, ENDPOINT, &credential);
        let original_grant = call(&lab.controller, acquisition.clone()).await;
        let permit = granted(&original_grant);
        reserve(&lab.controller, &caller, 1).await;
        let rejection = record_rejection(&lab.controller, &caller, &acquisition, kind).await;
        let outcome = if compatibility {
            NativeRequestOutcome::AuthenticationFailed
        } else {
            NativeRequestOutcome::Rejected { rejection }
        };
        let completion = settle(&caller, &acquisition, outcome.clone());
        let original_settlement = call(&lab.controller, completion.clone()).await;
        assert_eq!(settlement(&original_settlement).outcome, outcome);

        let next = acquire(&caller, 2, ENDPOINT, &credential);
        let refusal = call(&lab.controller, next.clone()).await;
        refused(&refusal, "reconciliation_required");
        assert!(matches!(&refusal.outcome, Outcome::Refused { message, .. }
            if message == "previous native rejection does not permit another request in this attempt"));
        assert_charge(&lab.controller, &caller, 1).await;

        lab = lab.restart().await;
        lab.advance(AUTH_COOLDOWN_MS + OPEN_MS);
        same_receipt(&original_grant, &call(&lab.controller, acquisition.clone()).await);
        same_receipt(&original_settlement, &call(&lab.controller, completion).await);
        same_receipt(&refusal, &call(&lab.controller, next).await);
        refused(&call(&lab.controller, acquire(&caller, 2, ENDPOINT, &credential)).await,
            "reconciliation_required");
        // A different credential handle cannot erase an attempt's fatal result.
        refused(&call(&lab.controller, acquire(&caller, 2, ENDPOINT,
            "fixture-replacement-credential")).await, "reconciliation_required");
        assert_charge(&lab.controller, &caller, 1).await;
        assert_eq!(granted(&call(&lab.controller,
            acquire(&caller, 1, ENDPOINT, &credential)).await), permit,
            "the old slot remains the same historical grant, never a second slot");

        let independent = lab.actor().await;
        let fresh = acquire(&independent, 1, ENDPOINT, &credential);
        let independent_permit = granted(&call(&lab.controller, fresh.clone()).await);
        assert_eq!(independent_permit.auth_probe, kind == RejectionKind::AuthenticationFailed);
        assert!(!independent_permit.circuit.probe);
        assert_ne!(independent_permit.circuit.caller, caller);
        assert_charge(&lab.controller, &independent, 0).await;
        let not_started = settlement(&call(&lab.controller,
            settle(&independent, &fresh, NativeRequestOutcome::NotStarted)).await);
        assert_eq!(not_started.permit, independent_permit);
        assert_charge(&lab.controller, &caller, 1).await;
        assert_charge(&lab.controller, &independent, 0).await;
    }
}

#[tokio::test]
async fn canonical_retryable_rejection_still_allows_the_next_uncharged_grant() {
    let lab = Lab::new();
    let caller = lab.actor().await;
    let acquisition = acquire(&caller, 1, ENDPOINT, CREDENTIAL);
    granted(&call(&lab.controller, acquisition.clone()).await);
    reserve(&lab.controller, &caller, 1).await;
    let rejection = record_rejection(&lab.controller, &caller, &acquisition,
        RejectionKind::RateLimited).await;
    let completion = settle(&caller, &acquisition, NativeRequestOutcome::Rejected { rejection });
    let original = call(&lab.controller, completion.clone()).await;
    assert_eq!(settlement(&original).circuit_retry_at_ms, None);
    let lab = lab.restart().await;
    same_receipt(&original, &call(&lab.controller, completion).await);
    let next = acquire(&caller, 2, ENDPOINT, CREDENTIAL);
    let second = granted(&call(&lab.controller, next.clone()).await);
    assert_eq!(second.request_index, 2);
    assert_eq!(second.circuit.caller, caller);
    assert_charge(&lab.controller, &caller, 1).await;
    reserve(&lab.controller, &caller, 2).await;
    let result = settlement(&call(&lab.controller,
        settle(&caller, &next, NativeRequestOutcome::Healthy)).await);
    assert_eq!(result.permit, second);
    assert_charge(&lab.controller, &caller, 2).await;
}

// Receipt inventory is inspected only after the controller releases its lock.
async fn restart_with_receipt_count(lab: Lab) -> (Lab, u64) {
    let Lab { root, controller, config, clock } = lab;
    drop(controller);
    let path = root.join("tasks.sqlite");
    released(&path).await;
    let db = rusqlite::Connection::open_with_flags(&path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let count: i64 = db.query_row("SELECT count(*) FROM receipts", [], |row| row.get(0)).unwrap();
    drop(db);
    let controller = Controller::open_with_config(&path, config.clone()).unwrap();
    (Lab { root, controller, config, clock }, u64::try_from(count).unwrap())
}
async fn observe_wait(controller: &Controller, caller: &Caller, wait: &Operation) -> Reply {
    call(controller, Operation::ObserveNativeRequest {
        caller: caller.clone(), acquire_operation_id: wait.mutation_id().unwrap().into(),
    }).await
}
fn readiness(reply: &Reply, wait: &Operation) -> bool {
    assert!(reply.commit.is_none(), "readiness must not create a mutation receipt");
    match &reply.outcome {
        Outcome::Ok { data: Data::NativeRequestReadiness { acquire_operation_id, changed } } => {
            assert_eq!(acquire_operation_id, wait.mutation_id().unwrap());
            *changed
        },
        other => panic!("expected original-wait readiness: {other:?}"),
    }
}

#[tokio::test]
async fn readiness_reads_preserve_receipts_and_detect_due_time_and_probe_settlement() {
    let lab = Lab::new();
    open_with_unknown_observations(&lab).await;
    let owner = lab.actor().await;
    let early = acquire(&owner, 1, ENDPOINT, CREDENTIAL);
    let early_reply = call(&lab.controller, early.clone()).await;
    waiting(&early_reply, NativeWaitReason::CircuitOpen);
    let (lab, before) = restart_with_receipt_count(lab).await;
    for _ in 0..16 {
        let reply = observe_wait(&lab.controller, &owner, &early).await;
        assert!(!readiness(&reply, &early));
        assert_eq!(reply.observed_controller_revision, early_reply.observed_controller_revision);
    }
    let (lab, after) = restart_with_receipt_count(lab).await;
    assert_eq!(after, before, "unchanged wait reads do not consume receipt capacity");
    lab.advance(OPEN_MS);
    let due = observe_wait(&lab.controller, &owner, &early).await;
    assert!(readiness(&due, &early));
    assert_eq!(due.observed_controller_revision, early_reply.observed_controller_revision);
    assert_charge(&lab.controller, &owner, 0).await;

    let probe = acquire(&owner, 1, ENDPOINT, CREDENTIAL);
    assert!(granted(&call(&lab.controller, probe.clone()).await).circuit.probe);
    let waiter = lab.actor().await;
    let pending = acquire(&waiter, 1, ENDPOINT, CREDENTIAL);
    let pending_reply = call(&lab.controller, pending.clone()).await;
    waiting(&pending_reply, NativeWaitReason::CircuitProbePending);
    let (lab, before) = restart_with_receipt_count(lab).await;
    for _ in 0..16 {
        let reply = observe_wait(&lab.controller, &waiter, &pending).await;
        assert!(!readiness(&reply, &pending));
        assert_eq!(reply.observed_controller_revision, pending_reply.observed_controller_revision);
    }
    let (lab, after) = restart_with_receipt_count(lab).await;
    assert_eq!(after, before, "live-probe reads do not create archive-producing receipts");
    refused(&observe_wait(&lab.controller, &owner, &pending).await, "source_mismatch");
    refused(&observe_wait(&lab.controller, &owner, &probe).await, "source_mismatch");
    reserve(&lab.controller, &owner, 1).await;
    let settled = call(&lab.controller, settle(&owner, &probe, NativeRequestOutcome::Healthy)).await;
    settlement(&settled);
    let ready = observe_wait(&lab.controller, &waiter, &pending).await;
    assert!(readiness(&ready, &pending));
    assert_eq!(ready.observed_controller_revision, settled.observed_controller_revision);
    assert_charge(&lab.controller, &waiter, 0).await;
    same_receipt(&pending_reply, &call(&lab.controller, pending).await);
}

#[tokio::test]
async fn authentication_readiness_observes_fenced_probe_without_reconciling_it() {
    let lab = Lab::new();
    let rejected = lab.actor().await;
    let owner = lab.actor().await;
    let first = acquire(&rejected, 1, ENDPOINT, CREDENTIAL);
    granted(&call(&lab.controller, first.clone()).await);
    reserve(&lab.controller, &rejected, 1).await;
    let rejection = record_rejection(&lab.controller, &rejected, &first,
        RejectionKind::AuthenticationFailed).await;
    settlement(&call(&lab.controller,
        settle(&rejected, &first, NativeRequestOutcome::Rejected { rejection })).await);
    let early = acquire(&owner, 1, ENDPOINT, CREDENTIAL);
    let early_reply = call(&lab.controller, early.clone()).await;
    waiting(&early_reply, NativeWaitReason::AuthenticationCooldown);
    let unchanged = observe_wait(&lab.controller, &owner, &early).await;
    assert!(!readiness(&unchanged, &early));
    assert_eq!(unchanged.observed_controller_revision, early_reply.observed_controller_revision);
    lab.advance(AUTH_COOLDOWN_MS);
    let due = observe_wait(&lab.controller, &owner, &early).await;
    assert!(readiness(&due, &early));
    assert_eq!(due.observed_controller_revision, early_reply.observed_controller_revision);
    let probe = acquire(&owner, 1, ENDPOINT, CREDENTIAL);
    let original_probe = call(&lab.controller, probe.clone()).await;
    assert!(granted(&original_probe).auth_probe);
    reserve(&lab.controller, &owner, 1).await;

    // This independent caller's unchanged deadline is later than the held probe's.
    lab.advance(1);
    let waiter = lab.actor().await;
    let pending = acquire(&waiter, 1, ENDPOINT, CREDENTIAL);
    let pending_reply = call(&lab.controller, pending.clone()).await;
    waiting(&pending_reply, NativeWaitReason::AuthenticationProbePending);
    let (lab, before) = restart_with_receipt_count(lab).await;
    for _ in 0..16 {
        let reply = observe_wait(&lab.controller, &waiter, &pending).await;
        assert!(!readiness(&reply, &pending));
        assert_eq!(reply.observed_controller_revision, pending_reply.observed_controller_revision);
    }
    let (lab, after) = restart_with_receipt_count(lab).await;
    assert_eq!(after, before);
    let original_owner = task(call(&lab.controller, Operation::GetTask { task_id: owner.task_id.clone() }).await);
    lab.advance(original_owner.input.limits.deadline_unix_ms.checked_sub(lab.now()).unwrap());
    let ready = observe_wait(&lab.controller, &waiter, &pending).await;
    assert!(readiness(&ready, &pending));
    // The controller runs ordinary expiry before the read. This owner has no
    // canonical rejection, so maintenance fails it before readiness observes
    // the exact fence; readiness itself must not settle the held probe.
    assert!(ready.observed_controller_revision.unwrap() > pending_reply.observed_controller_revision.unwrap());
    assert_charge(&lab.controller, &waiter, 0).await;
    let retained = task(call(&lab.controller, Operation::GetTask { task_id: owner.task_id.clone() }).await);
    assert_eq!(retained.status, TaskStatus::Failed);
    assert_eq!(retained.blocked_reason.as_deref(), Some("execution_deadline"));
    assert!(retained.native_retry_block.is_none());
    assert_eq!(retained.input.limits.deadline_unix_ms, original_owner.input.limits.deadline_unix_ms);
    assert_eq!(retained.attempt.as_ref().unwrap().phase, AttemptPhase::StopRequested);
    for _ in 0..3 {
        let observed = observe_wait(&lab.controller, &waiter, &pending).await;
        assert!(readiness(&observed, &pending));
        assert_eq!(observed.observed_controller_revision, ready.observed_controller_revision);
    }
    assert_eq!(task(call(&lab.controller, Operation::GetTask { task_id: owner.task_id.clone() }).await),
        retained, "readiness leaves the already-expired owner unchanged");
    same_receipt(&original_probe, &call(&lab.controller, probe.clone()).await);

    // Only a new mutation may reconcile the exact fenced probe as health-Unknown.
    let fresh = acquire(&waiter, 1, ENDPOINT, CREDENTIAL);
    let cooldown = call(&lab.controller, fresh).await;
    let (retry_at, _) = waiting(&cooldown, NativeWaitReason::AuthenticationCooldown);
    assert_eq!(retry_at, Some(lab.now() + AUTH_COOLDOWN_MS));
    let reconciled = settlement(&call(&lab.controller,
        settle(&owner, &probe, NativeRequestOutcome::Unknown)).await);
    assert_eq!(reconciled.source, NativeSettlementSource::FencedOwner);
    assert_eq!(reconciled.outcome, NativeRequestOutcome::Unknown);
    assert_charge(&lab.controller, &owner, 1).await;
    assert_charge(&lab.controller, &waiter, 0).await;
}

#[tokio::test]
async fn readiness_watch_wakes_on_exact_probe_settlement_and_full_waiters_return_current_read() {
    let mut limits = metadata();
    limits.controller.command_wait_ms = 30_000;
    limits.controller.worker_waiters = 1;
    let lab = Lab::with_limits(limits);
    open_with_unknown_observations(&lab).await;
    lab.advance(OPEN_MS);
    let owner = lab.actor().await;
    let probe = acquire(&owner, 1, ENDPOINT, CREDENTIAL);
    let permit = granted(&call(&lab.controller, probe.clone()).await);
    assert!(permit.circuit.probe);
    reserve(&lab.controller, &owner, 1).await;
    let waiter = lab.actor().await;
    let pending = acquire(&waiter, 1, ENDPOINT, CREDENTIAL);
    let pending_reply = call(&lab.controller, pending.clone()).await;
    waiting(&pending_reply, NativeWaitReason::CircuitProbePending);

    let watched = observe_wait(&lab.controller, &waiter, &pending);
    tokio::pin!(watched);
    // Poll the actual handle future into its wait. An immediate false response
    // would fail this discriminator; the selected command wait is thirty seconds.
    assert!(tokio::time::timeout(Duration::from_millis(100), &mut watched).await.is_err(),
        "an unchanged readiness request must retain the existing changes watch");
    let overflow = tokio::time::timeout(Duration::from_secs(1),
        observe_wait(&lab.controller, &waiter, &pending)).await
        .expect("a full waiter pool returns the current read instead of waiting or refusing");
    assert!(!readiness(&overflow, &pending));
    assert_eq!(overflow.observed_controller_revision, pending_reply.observed_controller_revision);

    let completed = call(&lab.controller, settle(&owner, &probe, NativeRequestOutcome::Healthy)).await;
    assert_eq!(settlement(&completed).permit, permit);
    let awakened = tokio::time::timeout(Duration::from_secs(1), &mut watched).await
        .expect("the exact probe settlement wakes readiness before command_wait_ms");
    assert!(readiness(&awakened, &pending));
    assert_eq!(awakened.observed_controller_revision, completed.observed_controller_revision);
    assert_charge(&lab.controller, &owner, 1).await;
    assert_charge(&lab.controller, &waiter, 0).await;
    same_receipt(&pending_reply, &call(&lab.controller, pending.clone()).await);
}

// Inspect only the original bounded expiry events after closing the controller.
// The public history projection may omit an event's opaque detail; this fixture
// checks the durable typed occurrence directly without changing it.
async fn restart_with_deadline_events(lab: Lab, caller: &Caller)
    -> (Lab, Vec<(u64, String, serde_json::Value)>) {
    let Lab { root, controller, config, clock } = lab;
    drop(controller);
    let path = root.join("tasks.sqlite");
    released(&path).await;
    let db = rusqlite::Connection::open_with_flags(&path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let rows = {
        let mut query = db.prepare("SELECT revision,kind,body FROM history WHERE task_id=?1 AND kind IN ('deadline','native_retry_deadline') ORDER BY seq LIMIT 3").unwrap();
        let values = query.query_map([&caller.task_id], |row| Ok((row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?, row.get::<_, String>(2)?))).unwrap()
            .collect::<Result<Vec<_>, _>>().unwrap();
        values
    };
    assert!(rows.len() <= 2, "bounded fixture expiry-event inventory");
    let rows = rows.into_iter().map(|(revision, kind, body)| {
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();
        (u64::try_from(revision).unwrap(), kind, value)
    }).collect();
    drop(db);
    let controller = Controller::open_with_config(&path, config.clone()).unwrap();
    (Lab { root, controller, config, clock }, rows)
}

#[tokio::test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# Exhaustion of the retry allowance or deadline MUST leave an explicit reason-bearing blocked state without another provider dispatch.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# Every transition into a retry-related blocked state MUST publish its reason as a durable event.
async fn restarted_deadline_expiry_keeps_latest_known_rejection_as_typed_blocked_source() {
    let lab = Lab::new();
    let caller = lab.actor().await;
    // Two original rejection receipts discriminate latest charged evidence from
    // an earlier known rejection. These are explicit synthetic host observations.
    for index in 1..=2 {
        let acquisition = acquire(&caller, index, ENDPOINT, CREDENTIAL);
        granted(&call(&lab.controller, acquisition.clone()).await);
        reserve(&lab.controller, &caller, index).await;
        let rejection = record_rejection(&lab.controller, &caller, &acquisition,
            RejectionKind::RateLimited).await;
        settlement(&call(&lab.controller,
            settle(&caller, &acquisition, NativeRequestOutcome::Rejected { rejection })).await);
    }
    let prior = task(call(&lab.controller, Operation::GetTask { task_id: caller.task_id.clone() }).await);
    assert_eq!(prior.status, TaskStatus::Active);
    assert!(prior.native_retry_block.is_none());
    let deadline = prior.input.limits.deadline_unix_ms;
    let source = borg_runtime::native_loop_contract::rejection_id(&caller.attempt_id, 2);
    let lookup = Operation::GetOperation { operation_id: source.clone() };
    let original = call(&lab.controller, lookup.clone()).await;
    let Outcome::Ok { data: Data::Operation(record) } = &original.outcome else {
        panic!("original canonical rejection receipt: {original:?}")
    };
    let Operation::ObserveWorker { report: original_report } = serde_json::from_value(record.intended.clone()).unwrap() else {
        panic!("rejection source must be the actual original host report")
    };
    assert_eq!(original_report.report_id, source);
    assert_eq!(original_report.attempt_id, caller.attempt_id);
    assert!(matches!(original_report.observation, Observation::NativeRequestRejected { request_index: 2, .. }));
    assert_eq!(original.commit.as_ref().unwrap().operation_id, source);

    lab.advance(deadline.checked_sub(lab.now()).unwrap());
    let lab = lab.restart().await;
    let expired = task(call(&lab.controller, Operation::GetTask { task_id: caller.task_id.clone() }).await);
    assert_eq!(expired.status, TaskStatus::Blocked, "known rejection expiry must not become ordinary Failed");
    assert_eq!(expired.blocked_reason.as_deref(), Some("native_retry_deadline"));
    assert!(expired.result.is_none());
    assert_eq!(expired.input, prior.input, "expiry preserves the accepted policy, budget and deadline");
    let block = expired.native_retry_block.as_ref().expect("typed expiry occurrence");
    assert_eq!(block.request_index, 2);
    assert_eq!(block.reason, borg_runtime::native::RetryBlockReason::Deadline);
    assert_eq!(block.source_operation_id, source);
    let attempt = expired.attempt.as_ref().unwrap();
    assert_eq!(attempt.id, caller.attempt_id);
    assert_eq!(attempt.generation, caller.generation);
    assert_eq!(attempt.deadline_unix_ms, deadline);
    assert_eq!(attempt.phase, AttemptPhase::StopRequested);
    assert!(!attempt.stopped && !attempt.uncertain);
    assert_charge(&lab.controller, &caller, 2).await;
    refused(&call(&lab.controller, Operation::GetOperation {
        operation_id: borg_runtime::native_loop_contract::reservation_id(&caller.attempt_id, 3),
    }).await, "not_found");
    same_receipt(&original, &call(&lab.controller, lookup.clone()).await);

    let commands = call(&lab.controller, Operation::AwaitCommands {
        operation_id: unique("expiry-stop"), worker_id: caller.worker_id.clone(),
        incarnation: caller.incarnation.clone(), acknowledged: vec![],
    }).await;
    let Outcome::Ok { data: Data::Commands(commands) } = commands.outcome else {
        panic!("expiry must issue the existing stop command")
    };
    assert!(commands.iter().any(|command| matches!(command,
        WorkerCommand::StopAttempt { task_id, attempt_id, generation, .. }
            if task_id == &caller.task_id && attempt_id == &caller.attempt_id && *generation == caller.generation)));
    assert!(!commands.iter().any(|command| matches!(command, WorkerCommand::RunAttempt { .. })));
    let expected_event = serde_json::to_value(block).unwrap();
    let expected_revision = expired.revision;
    let stopped = report(&lab.controller, &caller, Observation::ProcessTreeStopped).await;
    assert_eq!(stopped.status, TaskStatus::Blocked);
    assert_eq!(stopped.native_retry_block, expired.native_retry_block);
    assert!(stopped.attempt.as_ref().unwrap().stopped);
    assert!(!stopped.attempt.as_ref().unwrap().uncertain);

    let (lab, events) = restart_with_deadline_events(lab, &caller).await;
    assert_eq!(events, vec![(expected_revision, "native_retry_deadline".into(), expected_event)]);
    assert_eq!(task(call(&lab.controller, Operation::GetTask { task_id: caller.task_id.clone() }).await), stopped);
    same_receipt(&original, &call(&lab.controller, lookup).await);
    assert_charge(&lab.controller, &caller, 2).await;
    refused(&call(&lab.controller, Operation::GetOperation {
        operation_id: borg_runtime::native_loop_contract::reservation_id(&caller.attempt_id, 3),
    }).await, "not_found");
}

#[tokio::test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A local transport close or process cleanup MUST NOT establish that an ambiguous provider request had no effect.
async fn expiry_without_rejection_keeps_original_failure_and_unknown_effect_evidence() {
    let lab = Lab::new();
    let caller = lab.actor().await;
    let acquisition = acquire(&caller, 1, ENDPOINT, CREDENTIAL);
    granted(&call(&lab.controller, acquisition.clone()).await);
    reserve(&lab.controller, &caller, 1).await;
    let completion = settle(&caller, &acquisition, NativeRequestOutcome::Unknown);
    let original_unknown = call(&lab.controller, completion.clone()).await;
    assert_eq!(settlement(&original_unknown).outcome, NativeRequestOutcome::Unknown);
    let prior = task(call(&lab.controller, Operation::GetTask { task_id: caller.task_id.clone() }).await);
    assert_eq!(prior.status, TaskStatus::Active,
        "a health settlement alone does not report execution uncertainty or stop the attempt");
    refused(&call(&lab.controller, Operation::GetOperation {
        operation_id: borg_runtime::native_loop_contract::rejection_id(&caller.attempt_id, 1),
    }).await, "not_found");
    lab.advance(prior.input.limits.deadline_unix_ms.checked_sub(lab.now()).unwrap());
    let lab = lab.restart().await;
    let expired = task(call(&lab.controller, Operation::GetTask { task_id: caller.task_id.clone() }).await);
    assert_eq!(expired.status, TaskStatus::Failed);
    assert_eq!(expired.blocked_reason.as_deref(), Some("execution_deadline"));
    assert!(expired.native_retry_block.is_none());
    assert_eq!(expired.input, prior.input);
    assert_eq!(expired.attempt.as_ref().unwrap().phase, AttemptPhase::StopRequested);
    same_receipt(&original_unknown, &call(&lab.controller, completion.clone()).await);
    // The separate actual host uncertainty/stop reports remain required; expiry
    // cannot invent known absence from a health Unknown or clear this evidence.
    report(&lab.controller, &caller, Observation::ExecutionUncertain {
        message: "Synthetic original provider outcome remains unknown after expiry".into(),
    }).await;
    let stopped = report(&lab.controller, &caller, Observation::ProcessTreeStopped).await;
    assert_eq!(stopped.status, TaskStatus::Failed);
    assert!(stopped.native_retry_block.is_none());
    assert!(stopped.attempt.as_ref().unwrap().stopped && stopped.attempt.as_ref().unwrap().uncertain);
    let (lab, events) = restart_with_deadline_events(lab, &caller).await;
    assert_eq!(events, vec![(expired.revision, "deadline".into(), serde_json::Value::Null)]);
    assert_eq!(task(call(&lab.controller, Operation::GetTask { task_id: caller.task_id.clone() }).await), stopped);
    same_receipt(&original_unknown, &call(&lab.controller, completion).await);
    assert_charge(&lab.controller, &caller, 1).await;
    refused(&call(&lab.controller, Operation::GetOperation {
        operation_id: borg_runtime::native_loop_contract::reservation_id(&caller.attempt_id, 2),
    }).await, "not_found");
}


// Root-selected boundary: an explicit operation is not evidence that an
// immutable fatal request was corrected. These are synthetic host observations,
// followed by actual Controller receipt, hold, stop, cancellation and reopen.
#[tokio::test]
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A platform-owned model request rejected as invalid MUST NOT be retried.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A context-window rejection MUST NOT be retried with unchanged input.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A provider policy refusal MUST be reported as a policy block.
//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# Every retry-related blocked state MUST have an explicit canonical exit transition available through the supported adapter or control interface.
async fn fatal_typed_holds_refuse_unchanged_retry_and_preserve_cancel_across_restart() {
    use borg_runtime::native::{NativeRetryBlock, RetryBlockReason};
    for (kind, reason) in [
        (RejectionKind::InvalidRequest, RetryBlockReason::InvalidRequest),
        (RejectionKind::ContextLimit, RetryBlockReason::ContextLimit),
        (RejectionKind::PolicyRefused, RetryBlockReason::PolicyRefused),
        (RejectionKind::QuotaExhausted, RetryBlockReason::QuotaExhausted),
        (RejectionKind::AuthenticationFailed, RetryBlockReason::AuthenticationFailed),
    ] {
        let lab=Lab::new();
        let caller=lab.actor().await;
        let acquisition=acquire(&caller,1,ENDPOINT,CREDENTIAL);
        granted(&call(&lab.controller,acquisition.clone()).await);
        reserve(&lab.controller,&caller,1).await;
        let rejection=record_rejection(&lab.controller,&caller,&acquisition,kind).await;
        settlement(&call(&lab.controller,settle(&caller,&acquisition,
            NativeRequestOutcome::Rejected {rejection})).await);
        let block=NativeRetryBlock {request_index:1,
            source_operation_id:borg_runtime::native_loop_contract::rejection_id(&caller.attempt_id,1),
            reason};
        let blocked=report(&lab.controller,&caller,Observation::NativeRetryBlocked {block:block.clone()}).await;
        assert_eq!(blocked.status,TaskStatus::Blocked);
        assert_eq!(blocked.native_retry_block,Some(block.clone()));
        let stopped=report(&lab.controller,&caller,Observation::ProcessTreeStopped).await;
        assert_eq!(stopped.status,TaskStatus::Blocked);
        assert!(stopped.attempt.as_ref().unwrap().stopped);
        let lab=lab.restart().await;
        assert_eq!(task(call(&lab.controller,Operation::GetTask {task_id:caller.task_id.clone()}).await),stopped);
        let retry=Operation::RetryTask {operation_id:unique("fatal-explicit-retry"),
            task_id:caller.task_id.clone(),expected_revision:stopped.revision};
        let refusal=call(&lab.controller,retry.clone()).await;
        refused(&refusal,"retry_correction_required");
        same_receipt(&refusal,&call(&lab.controller,retry).await);
        assert_eq!(task(call(&lab.controller,Operation::GetTask {task_id:caller.task_id.clone()}).await),stopped);
        assert_charge(&lab.controller,&caller,1).await;
        let cancelled=task(call(&lab.controller,Operation::CancelTask {
            operation_id:unique("cancel-fatal-hold"),task_id:caller.task_id.clone(),
            expected_revision:stopped.revision,
        }).await);
        assert_eq!(cancelled.status,TaskStatus::Cancelled);
        assert_eq!(cancelled.native_retry_block,Some(block));
        assert_eq!(cancelled.input,stopped.input);
        assert_eq!(cancelled.attempt,stopped.attempt);
        let lab=lab.restart().await;
        assert_eq!(task(call(&lab.controller,Operation::GetTask {task_id:caller.task_id.clone()}).await),cancelled);
        assert_charge(&lab.controller,&caller,1).await;
    }
}

